//! The wake-procedure contracts. specs.md Section 9, steps 1-5. M5
//! wires the recall step (Sections 9.1-9.5): the recall result carries
//! the planned injections of the wake.
//!
//! The gate and reply implementations live in the tamako-agent crate
//! (Phase 1, M4); the M5 recall worker follows them. tamako-core
//! defines the contracts so the actor can drive the wake
//! procedure without a dependency on the agent crate (no dependency
//! cycles, AGENT.md Section 4). Same pattern as `digest.rs`.

use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::actor::CoreError;
use crate::context::{
    escape_xml_text, ContextMessage, MEDIA_TAG_CLOSE, MEDIA_TAG_OPEN_PREFIX, MSG_TAG_CLOSE,
    MSG_TAG_OPEN_PREFIX, SUMMARY_TAG_CLOSE, SUMMARY_TAG_OPEN_PREFIX, YOU_TAG_CLOSE,
    YOU_TAG_OPEN_PREFIX,
};

/// One message presented to the participation gate (Section 9.6 input).
/// `content` is the rendered XML item form of
/// [`crate::context::render_human_content`] — the same render helper as
/// the live context, so the gate input stays consistent with what the
/// reply model sees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateMessage {
    /// The raw-log row id of this message.
    pub row_id: i64,
    /// The platform-side message id.
    pub platform_msg_id: String,
    /// The rendered XML item content (specs.md Section 7.3).
    pub content: String,
    /// The sender id of the message (recall entry resolution,
    /// proposed-graph-database-specs.md Section 8.1 step 1).
    pub sender_id: String,
    /// The platform id of the reply target, when the message is a reply.
    pub reply_to_platform_msg_id: Option<String>,
    /// The raw message text (recall term tokenization). `content`
    /// stays the rendered XML item form of specs.md Section 7.3.
    pub text: String,
}

/// One planned recall injection (Section 9.4): the rendered injection
/// assistant message plus the edge ids of the memories it carries
/// (Section 9.3 dedup).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedInjection {
    /// The dedup keys of the injected edges (the natural-key
    /// string of tamako-memory's NeighborEdge::edge_id).
    pub edge_ids: Vec<String>,
    /// The rendered injection text: the new `<memory>` shape
    /// ([`render_injection_content`]); legacy rows carry the old
    /// "I remember: ..." shape and render as-is.
    pub content: String,
}

/// The text prefix of the legacy recall-injection messages (Section 9.4):
/// the rendered injection was `"{INJECTION_TEXT_PREFIX}{edge texts}"`.
/// Kept as the old-shape filter anchor ([`filter_reply_parrot_lines`]);
/// the rows persisted before the XML transition still carry this shape
/// and render as-is. The NEW shape is the `<memory>` tag pair
/// ([`INJECTION_TAG_OPEN`]/[`INJECTION_TAG_CLOSE`], rendered by
/// [`render_injection_content`]).
pub const INJECTION_TEXT_PREFIX: &str = "I remember: ";

/// The opening tag of a rendered recall injection (decision 59, new
/// shape). This constant and [`render_injection_content`] are the SINGLE
/// source shared by the tamako-agent recall renderer and the parrot
/// filter, so the injection format and the filter can never drift apart.
pub const INJECTION_TAG_OPEN: &str = "<memory>";

/// The closing tag of a rendered recall injection (decision 59, new
/// shape).
pub const INJECTION_TAG_CLOSE: &str = "</memory>";

/// Renders the body of one recall injection into the new `<memory>`
/// shape: `"<memory>{escaped body}</memory>"`. The body is the
/// already-joined edge text; it is XML-text-escaped through the
/// context.rs helper so a hostile edge text cannot break out of the tag.
pub fn render_injection_content(body: &str) -> String {
    format!(
        "{INJECTION_TAG_OPEN}{}{INJECTION_TAG_CLOSE}",
        escape_xml_text(body)
    )
}

/// The outcome of the reply parrot filter
/// ([`filter_reply_parrot_lines`], decision 59 F1): the text the bot
/// sends, plus whether the filter stripped one or more lines. The
/// flag keeps the call site honest: a stripped wake emits the
/// decision-59 WARN line with the chat id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplyFilterOutcome {
    /// The trimmed reply text with every parrot line removed.
    pub text: String,
    /// True when at least one parrot line was removed, or a residual
    /// fence token was dropped or rewritten (decision 93 layer 3).
    pub stripped_parrot: bool,
}

/// True when the trimmed START of one line is the legacy injection
/// prefix (decision 59, F1, old shape). Matches the ASCII colon of
/// [`INJECTION_TEXT_PREFIX`] and the full-width colon `：` of
/// Chinese-context model output. Line-start anchored only: mid-line
/// text is never matched (false-positive control).
fn is_parrot_line(line: &str) -> bool {
    let start = line.trim_start();
    if start.starts_with(INJECTION_TEXT_PREFIX) {
        return true;
    }
    // The full-width-colon variant: same words, `：` for `:`. Strip
    // the trailing space AND the ASCII colon of the prefix; the
    // remainder of the line must then open with the full-width colon.
    start
        .strip_prefix(INJECTION_TEXT_PREFIX.trim_end().trim_end_matches(':'))
        .is_some_and(|rest| rest.starts_with('：'))
}

/// One strip region of the outbound parrot filter: an opener prefix and
/// the closer that ends the region. All shapes share the exact same
/// block semantics (decision 59 F1).
#[derive(Clone, Copy)]
struct StripRegion {
    /// The opener prefix of the region: the open tag minus its trailing
    /// `>` (`"<memory"` / `"<summary"`), or the open tag plus its
    /// trailing space (`"<msg "` / `"<you "` / `"<media "`, before the
    /// attributes). Line-start anchored only.
    open_line_prefix: &'static str,
    /// The closer tag; the first line containing it ends the region.
    closer: &'static str,
}

/// The outbound parrot filter (decision 59, F1). Removes every line
/// whose trimmed start matches a recall-injection or context-structure
/// shape, then trims the remainder. Five shapes:
///
/// - OLD: the legacy injection prefix (ASCII or full-width colon, see
///   [`is_parrot_line`]).
/// - NEW `<memory>`: a trimmed line starting with `"<memory"` opens a
///   strip region: that line and every following line up to and
///   including the first line containing `"</memory>"` are stripped.
///   A single line carrying both tags strips as one. A trimmed line
///   starting with `"</memory>"` alone (a bare closer) strips. An
///   unterminated region strips to the end of the text.
/// - `<summary>` (segmented summarization): the SAME block semantics
///   over [`SUMMARY_TAG_OPEN_PREFIX`]/[`SUMMARY_TAG_CLOSE`]. The
///   summary block is a model-visible format the reply model can
///   imitate; the extension is cheap and symmetric, so it shares the
///   region machinery instead of growing a second filter.
/// - `<msg>` / `<you>` (the XML context items of
///   [`crate::context::render_human_content`]/
///   [`crate::context::render_bot_content`]): the SAME block semantics
///   over [`MSG_TAG_OPEN_PREFIX`]/[`MSG_TAG_CLOSE`] and
///   [`YOU_TAG_OPEN_PREFIX`]/[`YOU_TAG_CLOSE`]. The reply model sees
///   these elements on every wake (they ARE its context) and imitated
///   them live (the 2026-08-14 soak incident); a confabulated `<msg>`
///   or `<you>` block must never reach the group.
/// - `<media>` (media captioning at intake, decision 82): the SAME
///   block semantics over
///   [`MEDIA_TAG_OPEN_PREFIX`]/[`MEDIA_TAG_CLOSE`]. Media elements are
///   model-visible inside `<msg>` text, so the reply model can imitate
///   them like the other context structure; the reply model must never
///   emit media blocks.
///
/// The reply model can imitate the injection format (the injections
/// enter its context as assistant-role messages, Sections 9.3-9.5) and
/// speak a confabulated "I remember: ..." or `<memory>...</memory>`
/// block; such a line is hallucinated speech, not a recalled memory,
/// and it must never reach the raw log (Rule P1) or the group. The
/// same holds for the imitated context structure: an echoed `<msg>` or
/// `<you>` element is not the bot's speech and must not be sent.
///
/// Decision 93 layer 3 (specs.md Section 9.8): residual `<reply>`
/// fence tokens are hygiene, not speech. [`fence_token_hygiene`] runs
/// per line BEFORE the shape checks: a tag-only line drops, an inline
/// pair unwraps to its content, an edge token strips, and a mid-line
/// single token survives (quotation protection — the group discusses
/// AI glitch output). The UNWRAP direction is the inverse of the
/// strip regions above: the fence body is the genuine reply, never
/// confabulated context.
///
/// The filter runs BEFORE the outbound raw-log row persists (Rule B1):
/// the log and the group see the same filtered text. It runs on EVERY
/// reply text by construction: the actor applies it to every
/// `ReplyGenerator` output, and tamako-agent's live generator applies
/// the same function at its own validation seam. Always on; no
/// configuration key. Pure function, no I/O.
pub fn filter_reply_parrot_lines(text: &str, fence: &ReplyFence) -> ReplyFilterOutcome {
    // Single-source discipline (decisions 59/61): the memory opener is
    // derived from the tag constant; the summary and message shapes use
    // the context.rs constants shared with the renderers.
    let regions: [StripRegion; 5] = [
        StripRegion {
            open_line_prefix: SUMMARY_TAG_OPEN_PREFIX,
            closer: SUMMARY_TAG_CLOSE,
        },
        StripRegion {
            open_line_prefix: INJECTION_TAG_OPEN.trim_end_matches('>'),
            closer: INJECTION_TAG_CLOSE,
        },
        StripRegion {
            open_line_prefix: MSG_TAG_OPEN_PREFIX,
            closer: MSG_TAG_CLOSE,
        },
        // LEGACY (pre-decision-95): the fixed `<you>` bot-speech tag.
        // Nothing renders it since decision 95 (the unified pet tag
        // took over; its residual tokens are fence hygiene, not a strip
        // region), but stored summaries and memory texts persisted
        // before decision 95 can still quote the old shape.
        StripRegion {
            open_line_prefix: YOU_TAG_OPEN_PREFIX,
            closer: YOU_TAG_CLOSE,
        },
        StripRegion {
            open_line_prefix: MEDIA_TAG_OPEN_PREFIX,
            closer: MEDIA_TAG_CLOSE,
        },
    ];
    let mut stripped_parrot = false;
    let mut active: Option<StripRegion> = None;
    let mut kept: Vec<Cow<'_, str>> = Vec::new();
    for line in text.lines() {
        // Decision 93 layer 3: fence-token hygiene runs first, so an
        // inline-unwrapped region opener still matches the shape
        // checks below.
        let line = match fence_token_hygiene(line, fence) {
            Some(hygiened) => {
                if hygiened.as_ref() != line {
                    stripped_parrot = true;
                }
                hygiened
            }
            None => {
                stripped_parrot = true;
                continue;
            }
        };
        let start = line.trim_start();
        if let Some(region) = active {
            // Inside a strip region: every line is stripped up to and
            // including the first line containing the closer.
            stripped_parrot = true;
            if start.contains(region.closer) {
                active = None;
            }
            continue;
        }
        if let Some(region) = regions
            .iter()
            .find(|region| start.starts_with(region.open_line_prefix))
        {
            // The line opens a strip region (line-start anchored only).
            // A single line carrying the closer too strips as one line.
            stripped_parrot = true;
            if !start.contains(region.closer) {
                active = Some(*region);
            }
            continue;
        }
        if regions
            .iter()
            .any(|region| start.starts_with(region.closer))
        {
            // A bare closer line strips (line-start anchored only).
            stripped_parrot = true;
            continue;
        }
        if is_parrot_line(&line) {
            stripped_parrot = true;
            continue;
        }
        kept.push(line);
    }
    ReplyFilterOutcome {
        text: kept.join("\n").trim().to_string(),
        stripped_parrot,
    }
}

/// The reply fence token pair of decision 93, derived from the unified
/// speech tag of decision 95 (`tamako_persona::pet_tag_for_name`,
/// threaded by the caller): opener prefix `"<{tag}"` (no closing `>`:
/// an attribute-carrying opener like `<tamako at="09:05" id="3734">`
/// is an expected imitation variant — the model sees the tag on every
/// history turn, attributes included, and the 2026-09-05 replay
/// measured such imitation in up to 6/10 outputs) and closer
/// `"</{tag}>"`. Shared by [`extract_reply_fence`] and
/// [`fence_token_hygiene`] (decisions 59/61 single-source discipline,
/// dynamic form: one derivation, threaded value).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplyFence {
    /// The opener token prefix (`"<tamako"`, no closing `>`).
    open: String,
    /// The closer tag (`"</tamako>"`).
    close: String,
}

impl ReplyFence {
    /// Builds the fence token pair of one speech tag.
    pub fn for_pet_tag(pet_tag: &str) -> Self {
        Self {
            open: format!("<{pet_tag}"),
            close: format!("</{pet_tag}>"),
        }
    }

    /// The opener token prefix (`"<tamako"`, no closing `>`) — for
    /// log attribution at the call seams.
    pub fn open(&self) -> &str {
        &self.open
    }
}

/// The outcome of [`extract_reply_fence`] (decision 93, specs.md
/// Section 9.8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplyFenceOutcome {
    /// The reply text: the fence body when a complete fence exists,
    /// the input unchanged otherwise (the fail-open fallback).
    pub text: String,
    /// True when a complete fence was found and extracted.
    pub fenced: bool,
    /// The non-whitespace byte count of the content dropped OUTSIDE
    /// the fence (the tags themselves never count — a compliant reply
    /// with surrounding whitespace raises no WARN). Always zero when
    /// `fenced` is false.
    pub dropped_bytes: usize,
}

/// Extracts the body of the reply fence (decision 93 layer 2, specs.md
/// Section 9.8). The ephemeral reply instruction requires the whole
/// reply in exactly one `<reply>...</reply>` element; the FIRST
/// complete pair yields the reply text and everything outside it drops
/// (the allowlist complement of the reasoning-markup blocklist: an
/// unknown future reasoning marker outside the fence drops with no
/// code change).
///
/// FAIL-OPEN by design: an absent, unclosed, or malformed fence
/// returns the input unchanged (`fenced: false`), and the residual
/// hygiene of the parrot filter still cleans up fence debris. A
/// non-compliant model degrades to the pre-contract hygiene level
/// instead of dropping every reply — the deployment runs several
/// endpoints and models, and strict fence-or-drop would turn a model
/// swap into silence.
///
/// Runs at the reply validation seam only (`tamako-agent`,
/// `trimmed_reply_or_error`): the warmup instruction carries no fence
/// sentence, and the actor-side seams stay fence-agnostic. Pure
/// function, no I/O.
pub fn extract_reply_fence(text: &str, fence: &ReplyFence) -> ReplyFenceOutcome {
    let no_fence = || ReplyFenceOutcome {
        text: text.to_string(),
        fenced: false,
        dropped_bytes: 0,
    };
    let Some(tag_start) = text.find(&fence.open) else {
        return no_fence();
    };
    // The character after the opener prefix must be `>` or whitespace —
    // otherwise this is a different tag (`<replies>`, ...).
    let after = &text[tag_start + fence.open.len()..];
    if !after.starts_with('>') && !after.starts_with(char::is_whitespace) {
        return no_fence();
    }
    // The opener tag ends at its first `>`. An attribute value
    // carrying `>` ends the tag early — hygiene, not parsing.
    let Some(tag_end) = text[tag_start..].find('>').map(|p| tag_start + p) else {
        return no_fence();
    };
    let body_start = tag_end + 1;
    let Some(close) = text[body_start..]
        .find(&fence.close)
        .map(|p| body_start + p)
    else {
        // An unclosed fence (e.g. truncation at max_tokens): no
        // extraction; the fallback path salvages the body through the
        // residual-token hygiene.
        return no_fence();
    };
    let body = &text[body_start..close];
    let dropped_bytes =
        text[..tag_start].trim().len() + text[close + fence.close.len()..].trim().len();
    ReplyFenceOutcome {
        text: body.to_string(),
        fenced: true,
        dropped_bytes,
    }
}

/// Removes residual `<reply>`-fence tokens from one line (decision 93
/// layer 3, specs.md Section 9.8). Returns `None` when the line is
/// nothing but a fence tag (possibly after surgery). Rules:
///
/// 1. A tag-only line drops: the bare closer, or the bare opener with
///    or without attributes.
/// 2. An inline `<reply>...</reply>` pair unwraps to its content — a
///    pair is never a quotation (quoting writes a single token).
/// 3. An edge token strips: a leading fence tag or closer, a trailing
///    fence tag or closer.
///
/// A mid-line single token survives untouched: quotation protection
/// for a group that discusses AI glitch output.
fn fence_token_hygiene<'a>(line: &'a str, fence: &ReplyFence) -> Option<Cow<'a, str>> {
    if !line.contains(&fence.open) && !line.contains(&fence.close) {
        return Some(Cow::Borrowed(line));
    }
    let trimmed = line.trim();
    if trimmed == fence.close {
        return None;
    }
    if let Some(after) = trimmed.strip_prefix(&fence.open) {
        let tag_only = if let Some(rest) = after.strip_prefix('>') {
            rest.is_empty()
        } else if after.starts_with(char::is_whitespace) {
            // An attribute-carrying opener is tag-only only when the
            // tag ENDS the line. The first `>` closes the tag; text
            // past it is content (an inline pair such as
            // `<tamako at="09:05" id="4">come eat</tamako>` unwraps in
            // rule 2 below — pre-fix, the `trimmed.ends_with('>')` test
            // misread the pair's closer as the tag end and DROPPED the
            // whole line, content included: decision-95 C1 fix).
            after
                .find('>')
                .is_some_and(|p| after[p + 1..].trim().is_empty())
        } else {
            false
        };
        if tag_only {
            return None;
        }
    }
    let mut current = line.to_owned();
    // Rule 2: inline pairs unwrap.
    while let Some(open) = current.find(&fence.open) {
        let after = &current[open + fence.open.len()..];
        if !(after.starts_with('>') || after.starts_with(char::is_whitespace)) {
            break;
        }
        let Some(tag_rel) = after.find('>') else {
            break;
        };
        let body_start = open + fence.open.len() + tag_rel + 1;
        let Some(close_rel) = current[body_start..].find(&fence.close) else {
            break;
        };
        let close = body_start + close_rel;
        let next = format!(
            "{}{}{}",
            &current[..open],
            &current[body_start..close],
            &current[close + fence.close.len()..]
        );
        current = next;
    }
    // Rule 3a: a leading fence token strips.
    {
        let t = current.trim_start();
        let tag_len = if t.starts_with(&fence.close) {
            Some(fence.close.len())
        } else if let Some(after) = t.strip_prefix(&fence.open) {
            if after.starts_with('>') {
                Some(fence.open.len() + 1)
            } else if after.starts_with(char::is_whitespace) {
                after.find('>').map(|p| fence.open.len() + p + 1)
            } else {
                None
            }
        } else {
            None
        };
        if let Some(n) = tag_len {
            let ws = current.len() - t.len();
            current = format!("{}{}", &current[..ws], &current[ws + n..]);
        }
    }
    // Rule 3b: a trailing fence token strips.
    {
        let t = current.trim_end();
        let cut = if t.ends_with(&fence.close) {
            Some(fence.close.len())
        } else if t.ends_with('>') {
            t.rfind('<').and_then(|p| {
                let candidate = &t[p..];
                let after = candidate.strip_prefix(&fence.open)?;
                (after.starts_with('>') || after.starts_with(char::is_whitespace))
                    .then_some(candidate.len())
            })
        } else {
            None
        };
        if let Some(n) = cut {
            let trailing_ws = current.len() - t.len();
            let keep = current.len() - trailing_ws - n;
            current = format!(
                "{}{}",
                &current[..keep],
                &current[current.len() - trailing_ws..]
            );
        }
    }
    // A line reduced to whitespace (e.g. an inline pair wrapping
    // nothing) is a tag-only line by another route.
    if current.trim().is_empty() {
        return None;
    }
    Some(if current == line {
        Cow::Borrowed(line)
    } else {
        Cow::Owned(current)
    })
}

/// The recall result of one wake (Section 9 step 2). M5 produces
/// at most one injection per wake; the Vec keeps the seam open.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecallOutcome {
    pub injections: Vec<PlannedInjection>,
}

/// The gate input (Section 9.6): the new messages of this wake plus the
/// injected memories of the recall step (Section 9.6: the recall result
/// is gate input on purpose).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateInput {
    /// The new messages of this wake: the raw-log rows above
    /// `wake_last_row_id`, rendered as gate messages.
    pub new_messages: Vec<GateMessage>,
    /// The rendered injection texts of the recall step ("<memory>...</memory>";
    /// legacy rows keep their "I remember: ..." text). Empty means no
    /// injection (Section 9.2: an empty
    /// injection is forbidden — nothing is injected).
    pub injections: Vec<String>,
    /// True for a forced wake (mention/reply, Section 8.1).
    pub forced: bool,
}

/// The gate output (Section 9.6): a binary decision with the target
/// message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateDecision {
    /// True when the bot participates in this wake.
    pub participate: bool,
    /// The raw-log row id of the message the reply targets. `None` when
    /// `participate` is false.
    pub target_row_id: Option<i64>,
    /// The gate's own reason string (telemetry only; it lands on the
    /// curated wake log line). `None` for the scripted and forced paths
    /// that never consulted the gate model.
    pub reason: Option<String>,
}

/// The recall seam (Section 9 step 2; Sections 9.1-9.5). The wake
/// calls recall before the gate. The injections of the result enter the
/// gate input (Section 9.6) and the context (Section 9.4, Rule C2).
/// An empty `injections` vec of the `RecallOutcome` means no injection
/// (Section 9.2: an empty injection is forbidden — nothing is
/// injected).
pub trait RecallProvider: Send + Sync {
    fn recall<'a>(
        &'a self,
        chat_id: &'a str,
        new_messages: &'a [GateMessage],
    ) -> Pin<Box<dyn Future<Output = Result<RecallOutcome, CoreError>> + Send + 'a>>;

    /// The decision-72 entry point (specs.md Sections 9.2 and 9.6):
    /// `recall` plus the shared context view. `Some(view)` carries the
    /// `LiveContext::gate_context_view` bytes the actor renders once
    /// per wake (bound: the pre-advance marker); `None` is the pre-72
    /// delta-only input (the `gate_context` kill switch).
    ///
    /// The DEFAULT ignores the view and delegates to `recall`, so
    /// existing implementations stay valid unchanged; the live
    /// `ShallowRecall` (tamako-agent) overrides it to forward the view
    /// to the relevance gate.
    fn recall_with_context<'a>(
        &'a self,
        chat_id: &'a str,
        new_messages: &'a [GateMessage],
        context_view: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<RecallOutcome, CoreError>> + Send + 'a>> {
        let _ = context_view;
        self.recall(chat_id, new_messages)
    }
}

/// The no-op recall: no injections. The binary wires it when no LLM
/// key is configured.
pub struct NoopRecall;

impl RecallProvider for NoopRecall {
    fn recall<'a>(
        &'a self,
        _chat_id: &'a str,
        _new_messages: &'a [GateMessage],
    ) -> Pin<Box<dyn Future<Output = Result<RecallOutcome, CoreError>> + Send + 'a>> {
        Box::pin(async { Ok(RecallOutcome::default()) })
    }
}

/// The participation decision (Section 9.6). The live rig implementation
/// and the scripted test implementation live in tamako-agent (same
/// pattern as M1's KnowledgeExtractor). Structured output via JSON
/// schema, over the cheap `gate_model`.
///
/// Forced wakes (mention/reply, Section 8.1) BYPASS this gate: the actor
/// never calls `decide` for a forced wake.
pub trait ParticipationGate: Send + Sync {
    fn decide<'a>(
        &'a self,
        input: &'a GateInput,
    ) -> Pin<Box<dyn Future<Output = Result<GateDecision, CoreError>> + Send + 'a>>;

    /// The decision-72 entry point (specs.md Section 9.6): `decide`
    /// plus the shared context view. `Some(view)` renders the view
    /// AHEAD of the per-call sections (the prefix-extension cache
    /// property: consecutive gate calls share a growing byte prefix);
    /// `None` renders the pre-72 delta-only prompt byte-identically
    /// (the `gate_context` kill switch). The view is reference
    /// material only: the targetable set stays the wake's new
    /// messages, stated by the prompt's tail instruction and enforced
    /// by the post-validation of the presented set.
    ///
    /// The DEFAULT ignores the view and delegates to `decide`, so
    /// existing implementations stay valid unchanged; the live
    /// `RigGate` (tamako-agent) overrides it.
    fn decide_with_context<'a>(
        &'a self,
        input: &'a GateInput,
        context_view: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<GateDecision, CoreError>> + Send + 'a>> {
        let _ = context_view;
        self.decide(input)
    }
}

/// The reply request: the live context as LLM-facing messages (preamble
/// first, from `LiveContext::messages_for_llm`) plus the chosen target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplyRequest {
    /// Preamble first; tamako-agent converts these to rig completion
    /// messages (the M2 seam).
    pub messages: Vec<ContextMessage>,
    /// The target message of the reply (the gate's choice).
    pub target: GateMessage,
}

/// Reply generation with the main model (`reply_model`), Section 9
/// step 4.
pub trait ReplyGenerator: Send + Sync {
    fn generate<'a>(
        &'a self,
        request: &'a ReplyRequest,
    ) -> Pin<Box<dyn Future<Output = Result<String, CoreError>> + Send + 'a>>;
}

/// The bundle the actor needs to run the wake procedure. `None` in
/// `GroupActorParams` keeps the stub behavior (a later subtask wires
/// this).
pub struct WakeServices {
    /// The recall seam (Section 9 step 2). The binary wires
    /// `NoopRecall` when no LLM key is configured.
    pub recall: Arc<dyn RecallProvider>,
    /// The participation gate (Section 9.6). Runs only for unforced
    /// wakes (Section 8.1).
    pub gate: Arc<dyn ParticipationGate>,
    /// The reply generator (Section 9 step 4).
    pub reply: Arc<dyn ReplyGenerator>,
}

#[cfg(test)]
mod tests {
    use super::*;
    /// The fence built from the legacy tag name `reply`: opener
    /// `<reply` and closer `</reply>` — keeps every pre-95 fence
    /// fixture byte-identical while the production tag comes from the
    /// persona name (decision 95).
    fn test_fence() -> ReplyFence {
        ReplyFence::for_pet_tag("reply")
    }

    #[test]
    fn the_unified_pet_tag_fence_extracts_and_hygienes() {
        // Decision 95: the fence tokens derive from the speech tag.
        let fence = ReplyFence::for_pet_tag("tamako");
        let outcome = extract_reply_fence("<tamako>\nnya\n</tamako>", &fence);
        assert!(outcome.fenced);
        assert_eq!(outcome.text, "\nnya\n");
        // The measured R1 shape: an attribute-carrying opener imitated
        // from the history items (up to 6/10 of outputs in the replay).
        let outcome = extract_reply_fence("<tamako at=\"09:05\" id=\"4\">nya</tamako>", &fence);
        assert!(outcome.fenced);
        assert_eq!(outcome.text, "nya");
        // A foreign-tag fence is no fence.
        let outcome = extract_reply_fence("<reply>nya</reply>", &fence);
        assert!(!outcome.fenced);
        assert_eq!(outcome.text, "<reply>nya</reply>");
    }

    #[test]
    fn an_attribute_carrying_inline_pair_unwraps_instead_of_dropping() {
        // Decision-95 C1 fix. Pre-fix, hygiene rule 1 misread the
        // pair's closing `>` as the tag end (`trimmed.ends_with('>')`),
        // classified the line as tag-only and DROPPED the content —
        // on the warmup path (no fence extraction upstream) that was
        // the whole output: an empty-warmup error.
        let fence = ReplyFence::for_pet_tag("tamako");
        let filtered =
            filter_reply_parrot_lines("<tamako at=\"09:05\" id=\"4\">come eat</tamako>", &fence);
        assert_eq!(filtered.text, "come eat");
        assert!(filtered.stripped_parrot);
        // A genuine tag-only line with attributes still drops.
        let filtered = filter_reply_parrot_lines("<tamako at=\"09:05\" id=\"4\">", &fence);
        assert_eq!(filtered.text, "");
        assert!(filtered.stripped_parrot);
    }

    fn sample_gate_message() -> GateMessage {
        GateMessage {
            row_id: 7,
            platform_msg_id: "m7".to_string(),
            content: r#"<msg from="Alice" at="13:07" id="7">hello</msg>"#.to_string(),
            sender_id: "u1".to_string(),
            reply_to_platform_msg_id: None,
            text: "hello".to_string(),
        }
    }

    #[tokio::test]
    async fn the_noop_recall_returns_no_injections() {
        // Section 9.2: an empty injection is forbidden — nothing is
        // injected. The no-op always returns the empty outcome.
        let recall = NoopRecall;
        let messages = vec![sample_gate_message()];
        let outcome = recall
            .recall("chat", &messages)
            .await
            .expect("the no-op recall never fails");
        assert_eq!(outcome, RecallOutcome::default());
    }

    #[test]
    fn the_traits_are_object_safe() {
        // The actor holds Arc<dyn ...> of each trait (see WakeServices).
        // This assertion keeps the traits object-safe.
        fn assert_object_safe(
            _: Option<Arc<dyn RecallProvider>>,
            _: Option<Arc<dyn ParticipationGate>>,
            _: Option<Arc<dyn ReplyGenerator>>,
        ) {
        }
        assert_object_safe(None, None, None);
    }

    #[test]
    fn the_parrot_filter_strips_a_leading_block() {
        // Decision 59, F1: the model echoed the injection format before
        // its real speech (the live-soak failure shape).
        let filtered = filter_reply_parrot_lines(
            "I remember: Alice likes tea.\nI remember: Bob runs.\nthe cafe on main street",
            &test_fence(),
        );
        assert_eq!(filtered.text, "the cafe on main street");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_a_mid_text_line() {
        let filtered =
            filter_reply_parrot_lines("one\nI remember: Alice likes tea.\ntwo", &test_fence());
        assert_eq!(filtered.text, "one\ntwo");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_the_full_width_colon_variant() {
        // Chinese-context model output uses the full-width colon.
        let filtered = filter_reply_parrot_lines("I remember：小明喜欢吃辣。\n在的", &test_fence());
        assert_eq!(filtered.text, "在的");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_a_single_line_memory_block() {
        // Decision 59, F1, new shape: one line carrying both tags
        // strips as one.
        let filtered = filter_reply_parrot_lines(
            "<memory>Alice likes tea</memory>\nthe cafe on main street",
            &test_fence(),
        );
        assert_eq!(filtered.text, "the cafe on main street");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_a_multi_line_memory_block() {
        let filtered = filter_reply_parrot_lines(
            "<memory>Alice likes tea\nBob runs</memory>\nthe cafe on main street",
            &test_fence(),
        );
        assert_eq!(filtered.text, "the cafe on main street");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_a_bare_memory_closer_line() {
        let filtered = filter_reply_parrot_lines("one\n</memory>\ntwo", &test_fence());
        assert_eq!(filtered.text, "one\ntwo");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_an_unterminated_memory_block_to_the_end() {
        // No closer appears: the region strips to the end of the text.
        let filtered =
            filter_reply_parrot_lines("one\n<memory>never closed\nrest of the text", &test_fence());
        assert_eq!(filtered.text, "one");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_both_shapes_in_one_text() {
        let filtered = filter_reply_parrot_lines(
            "<memory>Alice likes tea</memory>\nI remember: Bob runs.\nthe cafe",
            &test_fence(),
        );
        assert_eq!(filtered.text, "the cafe");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_a_single_line_summary_block() {
        // The `<summary>` shape shares the `<memory>` block semantics:
        // one line carrying both tags strips as one.
        let filtered = filter_reply_parrot_lines(
            "<summary range=\"1-3\">they argued about dinner</summary>\nthe cafe on main street",
            &test_fence(),
        );
        assert_eq!(filtered.text, "the cafe on main street");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_a_multi_line_summary_block() {
        let filtered = filter_reply_parrot_lines("<summary range=\"1-3\">they argued about dinner\nand made up</summary>\nthe cafe on main street", &test_fence());
        assert_eq!(filtered.text, "the cafe on main street");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_a_bare_summary_closer_line() {
        let filtered = filter_reply_parrot_lines("one\n</summary>\ntwo", &test_fence());
        assert_eq!(filtered.text, "one\ntwo");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_an_unterminated_summary_block_to_the_end() {
        // No closer appears: the region strips to the end of the text.
        let filtered = filter_reply_parrot_lines(
            "one\n<summary range=\"1-3\">never closed\nrest of the text",
            &test_fence(),
        );
        assert_eq!(filtered.text, "one");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_memory_and_summary_shapes_in_one_text() {
        let filtered = filter_reply_parrot_lines("<summary range=\"1-3\">digested chunk</summary>\n<memory>Alice likes tea</memory>\nthe cafe", &test_fence());
        assert_eq!(filtered.text, "the cafe");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_a_single_line_msg_block() {
        // The `<msg>` shape shares the `<memory>` block semantics (the
        // 2026-08-14 soak incident: the reply model imitated the XML
        // context structure live).
        let filtered = filter_reply_parrot_lines(
            "<msg from=\"Alice\" at=\"13:07\" id=\"1\">hello</msg>\nthe cafe on main street",
            &test_fence(),
        );
        assert_eq!(filtered.text, "the cafe on main street");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_a_multi_line_msg_block() {
        let filtered = filter_reply_parrot_lines(
            "<msg from=\"Alice\" at=\"13:07\" id=\"1\">hello\nthere</msg>\nthe cafe on main street",
            &test_fence(),
        );
        assert_eq!(filtered.text, "the cafe on main street");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_a_bare_msg_closer_line() {
        let filtered = filter_reply_parrot_lines("one\n</msg>\ntwo", &test_fence());
        assert_eq!(filtered.text, "one\ntwo");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_an_unterminated_msg_block_to_the_end() {
        // No closer appears: the region strips to the end of the text.
        let filtered = filter_reply_parrot_lines(
            "one\n<msg from=\"Alice\" at=\"13:07\" id=\"1\">never closed\nrest of the text",
            &test_fence(),
        );
        assert_eq!(filtered.text, "one");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_a_single_line_you_block() {
        let filtered = filter_reply_parrot_lines(
            "<you at=\"13:07\" id=\"2\">hi there</you>\nthe cafe on main street",
            &test_fence(),
        );
        assert_eq!(filtered.text, "the cafe on main street");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_a_multi_line_you_block() {
        let filtered = filter_reply_parrot_lines(
            "<you at=\"13:07\" id=\"2\">hi\nthere</you>\nthe cafe on main street",
            &test_fence(),
        );
        assert_eq!(filtered.text, "the cafe on main street");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_a_bare_you_closer_line() {
        let filtered = filter_reply_parrot_lines("one\n</you>\ntwo", &test_fence());
        assert_eq!(filtered.text, "one\ntwo");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_an_unterminated_you_block_to_the_end() {
        // No closer appears: the region strips to the end of the text.
        let filtered = filter_reply_parrot_lines(
            "one\n<you at=\"13:07\" id=\"2\">never closed\nrest of the text",
            &test_fence(),
        );
        assert_eq!(filtered.text, "one");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_a_single_line_media_block() {
        // The `<media>` shape (decision 82) shares the `<memory>` block
        // semantics: one line carrying both tags strips as one. The
        // reply model must never emit media blocks.
        let filtered = filter_reply_parrot_lines(
            "<media type=\"image\">confabulated</media>\nthe cafe on main street",
            &test_fence(),
        );
        assert_eq!(filtered.text, "the cafe on main street");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_a_multi_line_media_block() {
        let filtered = filter_reply_parrot_lines(
            "<media type=\"image\">confabulated\ncaption</media>\nthe cafe on main street",
            &test_fence(),
        );
        assert_eq!(filtered.text, "the cafe on main street");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_a_bare_media_closer_line() {
        // The surrounding legitimate text is preserved.
        let filtered = filter_reply_parrot_lines("one\n</media>\ntwo", &test_fence());
        assert_eq!(filtered.text, "one\ntwo");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_an_unterminated_media_block_to_the_end() {
        // No closer appears: the region strips to the end of the text.
        let filtered = filter_reply_parrot_lines(
            "one\n<media type=\"image\">never closed\nrest of the text",
            &test_fence(),
        );
        assert_eq!(filtered.text, "one");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_the_old_and_xml_shapes_in_one_text() {
        // The injection shapes and the imitated context structure strip
        // together; only real speech remains.
        let filtered = filter_reply_parrot_lines("I remember: Bob runs.\n<msg from=\"Alice\" at=\"13:07\" id=\"1\">hello</msg>\n<you at=\"13:08\" id=\"2\">hi</you>\nthe cafe", &test_fence());
        assert_eq!(filtered.text, "the cafe");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_exactly_what_the_message_renderers_produce() {
        // Single-source discipline (decisions 59/61): the `<msg>`/`<you>`
        // renderers and the filter share the tag constants, so they can
        // never drift apart.
        let human = crate::context::render_human_content(
            1,
            "Alice",
            None,
            time::macros::datetime!(2026-08-07 13:07 UTC),
            false,
            false,
            crate::context::ReplyRender::None,
            "hello",
        );
        let filtered = filter_reply_parrot_lines(&human, &test_fence());
        assert_eq!(filtered.text, "");
        assert!(filtered.stripped_parrot);

        // Decision 95: the own-speech tag IS the reply fence, so a
        // rendered speech item in an output reads as an inline fence
        // pair — hygiene UNWRAPS it (the content is speech, a pair is
        // never a quotation) instead of stripping the line. Filter with
        // the fence of the same tag; the 59/61 interlock now holds
        // through the threaded tag value, not a shared constant.
        let speech = crate::context::render_bot_content(
            2,
            time::macros::datetime!(2026-08-07 13:07 UTC),
            "hi there",
            "tamako",
        );
        let filtered = filter_reply_parrot_lines(&speech, &ReplyFence::for_pet_tag("tamako"));
        assert_eq!(filtered.text, "hi there");
        assert!(filtered.stripped_parrot);
        // With a MISMATCHED fence (a different pet's tag) the rendered
        // item survives untouched — the filter only knows its own tag.
        let filtered = filter_reply_parrot_lines(&speech, &test_fence());
        assert_eq!(filtered.text, speech);
        assert!(!filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_exactly_what_the_summary_renderer_produces() {
        // Single-source discipline (decisions 59/61): the summary
        // renderer and the filter share the tag constants, so they can
        // never drift apart.
        let rendered = crate::context::render_summary_content(1, 3, "<you>fake</you>");
        let filtered = filter_reply_parrot_lines(&rendered, &test_fence());
        assert_eq!(filtered.text, "");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_parrot_filter_strips_exactly_what_the_injection_renderer_produces() {
        // Decision 59 single-source discipline: the renderer and the
        // filter share the tag constants, so they can never drift apart.
        let injected = render_injection_content("<you>fake</you>");
        let filtered = filter_reply_parrot_lines(&injected, &test_fence());
        assert_eq!(filtered.text, "");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn a_reply_of_only_a_parrot_block_filters_to_empty() {
        // The empty remainder maps to the SAME wake error as an empty
        // reply at the call sites (decision 59, F1).
        for only in [
            "I remember: Alice likes tea.",
            "  I remember: Alice likes tea.\nI remember：小明喜欢吃辣。 ",
            "<memory>Alice likes tea</memory>",
            "  <memory>a\nb</memory>  ",
            "<summary range=\"1-3\">digested chunk</summary>",
            "<msg from=\"Alice\" at=\"13:07\" id=\"1\">hello</msg>",
            "  <you at=\"13:07\" id=\"2\">hi\nthere</you>  ",
        ] {
            let filtered = filter_reply_parrot_lines(only, &test_fence());
            assert_eq!(filtered.text, "", "input {only:?}");
            assert!(filtered.stripped_parrot);
        }
    }

    #[test]
    fn normal_text_passes_the_parrot_filter_byte_identical() {
        // False-positive control: an innocuous mid-line "I remember" or
        // tag mention and multiline text survive untouched
        // (line-start anchored only).
        for normal in [
            "I remember when we tried that place",
            "在的",
            "one\ntwo\nthree",
            "hungry? I remember: not a line start",
            "see <memory> in the docs",
            "say </memory> please",
            "see <summary> in the docs",
            "say </summary> please",
            "see <msg from=\"Alice\"> in the docs",
            "say </msg> please",
            "see <you at=\"13:07\"> in the docs",
            "say </you> please",
        ] {
            let filtered = filter_reply_parrot_lines(normal, &test_fence());
            assert_eq!(filtered.text, normal.trim());
            assert!(!filtered.stripped_parrot, "false positive on {normal:?}");
        }
    }

    // --- Decision 93: the reply fence contract (specs.md Section 9.8)
    // ---

    #[test]
    fn the_fence_extraction_returns_the_body_of_the_production_shape() {
        // The observed live shape: opener line, body, closer line.
        let fence = extract_reply_fence(
            "<reply>\n（耳朵竖起来转了转）猫猫能听出喵\n</reply>",
            &test_fence(),
        );
        assert!(fence.fenced);
        assert_eq!(fence.text, "\n（耳朵竖起来转了转）猫猫能听出喵\n");
        // Only whitespace sits outside the fence: no WARN.
        assert_eq!(fence.dropped_bytes, 0);
    }

    #[test]
    fn the_fence_extraction_drops_outside_content() {
        // The point of the contract: reasoning tails, prefaces, and
        // trailing chatter outside the fence never reach the group.
        let fence = extract_reply_fence(
            "preface chatter\n<reply>nya</reply>\ntrailing",
            &test_fence(),
        );
        assert!(fence.fenced);
        assert_eq!(fence.text, "nya");
        assert_eq!(
            fence.dropped_bytes,
            "preface chatter".len() + "trailing".len()
        );
    }

    #[test]
    fn the_fence_extraction_accepts_an_attribute_opener() {
        // The reply instruction names a message id; an imitated
        // attribute is an expected variant.
        let fence = extract_reply_fence("<reply to=\"44\">nya</reply>", &test_fence());
        assert!(fence.fenced);
        assert_eq!(fence.text, "nya");
    }

    #[test]
    fn an_unclosed_fence_falls_back_to_the_whole_text() {
        // Truncation at max_tokens: no extraction; the residual
        // hygiene salvages the body on the fallback path.
        let fence = extract_reply_fence("<reply>truncated body", &test_fence());
        assert!(!fence.fenced);
        assert_eq!(fence.text, "<reply>truncated body");
        assert_eq!(fence.dropped_bytes, 0);
    }

    #[test]
    fn a_text_without_a_fence_passes_through_unchanged() {
        let fence = extract_reply_fence("plain reply", &test_fence());
        assert!(!fence.fenced);
        assert_eq!(fence.text, "plain reply");
        assert_eq!(fence.dropped_bytes, 0);
    }

    #[test]
    fn a_lookalike_tag_is_not_a_fence() {
        let fence = extract_reply_fence("<replies>nya</replies>", &test_fence());
        assert!(!fence.fenced);
        assert_eq!(fence.text, "<replies>nya</replies>");
    }

    #[test]
    fn the_first_of_two_fences_wins_and_the_second_is_dropped() {
        let fence = extract_reply_fence("<reply>one</reply><reply>two</reply>", &test_fence());
        assert!(fence.fenced);
        assert_eq!(fence.text, "one");
        assert_eq!(fence.dropped_bytes, "<reply>two</reply>".len());
    }

    #[test]
    fn the_filter_drops_bare_fence_tag_lines_and_keeps_the_content() {
        // Layer 3 on the fallback path: an unextracted fence degrades
        // to token hygiene, never to content loss.
        let filtered =
            filter_reply_parrot_lines("<reply>\nthe cafe on main street\n</reply>", &test_fence());
        assert_eq!(filtered.text, "the cafe on main street");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_filter_unwraps_an_inline_fence_pair() {
        let filtered = filter_reply_parrot_lines("<reply>the cafe</reply>", &test_fence());
        assert_eq!(filtered.text, "the cafe");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_filter_strips_fence_edge_tokens() {
        for (raw, kept) in [
            ("<reply>the cafe", "the cafe"),
            ("the cafe</reply>", "the cafe"),
            ("<reply to=\"44\">the cafe", "the cafe"),
            ("</reply>the cafe", "the cafe"),
            ("the cafe <reply>", "the cafe"),
        ] {
            let filtered = filter_reply_parrot_lines(raw, &test_fence());
            assert_eq!(filtered.text, kept, "input {raw:?}");
            assert!(filtered.stripped_parrot, "input {raw:?}");
        }
    }

    #[test]
    fn a_mid_line_fence_token_survives() {
        // Quotation protection: the group discusses AI glitch output.
        // A single mid-line token is speech about tags, not a fence.
        for quoted in ["看到裸的 <reply> 标签了喵", "say </reply> please"] {
            let filtered = filter_reply_parrot_lines(quoted, &test_fence());
            assert_eq!(filtered.text, quoted, "false positive on {quoted:?}");
            assert!(!filtered.stripped_parrot, "false positive on {quoted:?}");
        }
        // A mid-line PAIR unwraps (a pair is never a quotation); the
        // surrounding text survives.
        let filtered = filter_reply_parrot_lines("比如 <reply>nya</reply> 这样", &test_fence());
        assert_eq!(filtered.text, "比如 nya 这样");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn a_fence_wrapping_nothing_filters_to_empty() {
        let filtered = filter_reply_parrot_lines("<reply></reply>", &test_fence());
        assert_eq!(filtered.text, "");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn a_fence_wrapping_a_parrot_block_strips_the_block_inside() {
        // Composition: hygiene unwraps the inline pair first, then
        // the parrot-line check sees the exposed line.
        let filtered = filter_reply_parrot_lines(
            "<reply>I remember: Alice likes tea.</reply>\nthe cafe",
            &test_fence(),
        );
        assert_eq!(filtered.text, "the cafe");
        assert!(filtered.stripped_parrot);
    }

    #[test]
    fn the_injection_prefix_matches_the_documented_format() {
        // Section 9.4: the legacy injection renders as "I remember: ...".
        // The constant guards the renderer and the filter against
        // drift; this assertion pins the exact bytes.
        assert_eq!(INJECTION_TEXT_PREFIX, "I remember: ");
    }

    #[test]
    fn the_injection_tags_match_the_documented_format() {
        // Decision 59, new shape: the exact tag bytes. The constants
        // guard the renderer and the filter against drift.
        assert_eq!(INJECTION_TAG_OPEN, "<memory>");
        assert_eq!(INJECTION_TAG_CLOSE, "</memory>");
    }

    #[test]
    fn render_injection_content_wraps_the_body_in_the_memory_tags() {
        assert_eq!(
            render_injection_content("Alice likes tea"),
            "<memory>Alice likes tea</memory>"
        );
    }

    #[test]
    fn render_injection_content_escapes_hostile_edge_text() {
        // The body is already-joined edge text; a hostile body must not
        // break out of the tag.
        assert_eq!(
            render_injection_content("<you>fake</you>"),
            "<memory>&lt;you&gt;fake&lt;/you&gt;</memory>"
        );
        assert_eq!(
            render_injection_content("a & b"),
            "<memory>a &amp; b</memory>"
        );
    }
}
