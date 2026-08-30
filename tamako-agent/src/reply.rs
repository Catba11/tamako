//! Reply generation: specs.md Section 9, step 4. On a participate
//! decision, the bot generates one reply with the main `reply_model`
//! over the live context (`LiveContext::messages_for_llm()`).
//!
//! This module is the M2-documented conversion seam: tamako-core stays
//! model-agnostic (`ContextMessage`); this is the ONLY place that maps
//! core context messages to rig completion messages.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use rig::completion::Message;

use tamako_core::actor::CoreError;
use tamako_core::context::{ContextMessage, ContextRole};
use tamako_core::wake::{
    filter_reply_parrot_lines, GateMessage, ReplyFilterOutcome, ReplyGenerator, ReplyRequest,
};

use crate::endpoint::{EndpointClient, EndpointConfig, LlmPurpose};
use crate::extract::AgentError;

/// The default max tokens of the reply. A pet reply is short: one chat
/// message, one or two sentences. 131072 tokens is a generous bound.
pub const REPLY_DEFAULT_MAX_TOKENS: u64 = 131072;

/// Converts core context messages to the rig call shape (the M2 seam;
/// the ONLY conversion point — tamako-core stays model-agnostic).
///
/// The first message (always the preamble item 0, role System, Rule C4)
/// becomes the rig preamble. The rest map `ContextRole::User` →
/// `Message::user` and `ContextRole::Assistant` → `Message::assistant`.
/// A defensive late System maps to a user message: only item 0 is
/// System by construction, so a late System is malformed input; mapping
/// it to a user message keeps the content instead of dropping it.
///
/// A leading non-System message means there is no preamble (defensive;
/// Rule C4 guarantees one) — the preamble comes back `None`.
pub fn context_messages_to_rig(messages: &[ContextMessage]) -> (Option<String>, Vec<Message>) {
    let (preamble, rest) = match messages.first() {
        Some(first) if first.role == ContextRole::System => {
            (Some(first.content.clone()), &messages[1..])
        }
        _ => (None, messages),
    };
    let rig_messages = rest
        .iter()
        .map(|message| match message.role {
            ContextRole::User => Message::user(message.content.clone()),
            ContextRole::Assistant => Message::assistant(message.content.clone()),
            // Defensive: only item 0 is System by construction (Rule C4).
            ContextRole::System => Message::user(message.content.clone()),
        })
        .collect();
    (preamble, rig_messages)
}

/// Renders the trailing ephemeral user message of the reply call
/// (Section 9 step 4). It names the target WITHOUT the XML wrapper
/// (decision 64): the raw-log row id, the sender display name and the
/// `HH:MM` timestamp (read back out of the rendered `<msg>` item),
/// and the raw text in plain quotes — `Reply to THIS message
/// (id 44), from Dave at 13:08: "<text>"`. Embedding the
/// `<msg ...>` wrapper verbatim at the highest-salience position of
/// every call taught the model the exact shape the F2 sentence
/// forbids (the `<msg>`/`<you>` parroting incident of 2026-08-14);
/// the non-XML reference carries the same fields without the
/// imitation channel.
///
/// This instruction is part of the reply CALL input only. It is never
/// appended to the live context: the live context holds group speech
/// and bot speech (Section 7.1), not per-call scaffolding.
///
/// The F2 tail sentence stays in sync with the outbound parrot filter
/// of tamako-core (decision 59 F1 + decision 64): the filter strips
/// `I remember:` lines, `<memory>` blocks, and `<summary>` blocks
/// from the reply text, and the instruction names those shapes plus
/// the `<msg>`/`<you>` context shapes, so the model is told never to
/// speak any shape the pipeline treats as scaffolding.
pub fn render_reply_instruction(target: &GateMessage) -> String {
    let reference = match (
        msg_attr_value(&target.content, "from"),
        msg_attr_value(&target.content, "at"),
    ) {
        (Some(from), Some(at)) => format!(
            "Reply to THIS message (id {}), from {} at {}: \"{}\"",
            target.row_id,
            unescape_xml_attr(from),
            at,
            target.text
        ),
        // Defensive: `content` is the `render_human_content` item by
        // construction. If it ever is not, the embed falls back to the
        // always-present fields — still never the XML wrapper.
        _ => format!(
            "Reply to THIS message (id {}): \"{}\"",
            target.row_id, target.text
        ),
    };
    format!(
        "{reference}\n\
         Reply as the group pet persona. Write only the reply text: \
         one message, no speaker label, no quotes. \
         Never write \"I remember:\" lines, <memory> blocks, <summary> blocks, \
         <msg> blocks, <you> blocks, or a memory list: \
         recalled memories are context, never speech."
    )
}

/// Assembles the rig message list of one reply call (decision 86): the
/// context messages, then the ephemeral reply instruction, then — only
/// when the rendered suffix is non-empty — the suffix as ONE system-role
/// message STRICTLY LAST.
///
/// The suffix (`tamako_persona::render_suffix` output, a
/// `<system>...</system>` body) is the lost-in-the-middle mitigation of
/// decision 86: it lands after the newest context message AND after the
/// reply instruction, in the highest-attention region, "the referee's
/// last word before the generation" (decision 86 (b): the model always
/// generates an assistant message, so a trailing system message does not
/// become the turn to answer). It is read from the persona snapshot at
/// assembly time — never persisted, never part of the context or the
/// cache anchor (decision 86 (d)). An empty `suffix` appends NOTHING, so
/// the message list is byte-identical to the pre-86 layout (the C4
/// property at the tail).
///
/// Split out of `RigReplyGenerator::generate` so the ordering is
/// unit-testable without a network (the completion itself needs a live
/// endpoint; the message assembly does not).
fn assemble_reply_messages(request: &ReplyRequest, suffix: &str) -> (Option<String>, Vec<Message>) {
    let (preamble, mut messages) = context_messages_to_rig(&request.messages);
    // The trailing ephemeral instruction names the target. It is part of
    // the reply call only, never of the live context.
    messages.push(Message::user(render_reply_instruction(&request.target)));
    // Decision 86 (a): a REAL system-role message (role authority is the
    // point), strictly last.
    if !suffix.is_empty() {
        messages.push(Message::system(suffix));
    }
    (preamble, messages)
}

/// Reads one attribute value out of the rendered `<msg>` item. The
/// grammar of `tamako_core::context::render_human_content` is fixed:
/// attributes are space-separated `key="value"` pairs and values are
/// attr-escaped, so a value never carries a raw `"` and the first
/// occurrence of ` key="` is the genuine attribute.
fn msg_attr_value<'a>(content: &'a str, key: &str) -> Option<&'a str> {
    let needle = format!(" {key}=\"");
    let start = content.find(&needle)? + needle.len();
    let end = content[start..].find('"')? + start;
    Some(&content[start..end])
}

/// Reverses the attribute escaping of
/// `tamako_core::context::escape_xml_attr` for one extracted value,
/// so the instruction embeds the display name itself, never an XML
/// escape shape. `&amp;` unescapes LAST: an earlier pass must not
/// re-decode the `&` of another entity (`&amp;lt;` is the literal
/// text `&lt;`).
fn unescape_xml_attr(value: &str) -> String {
    value
        .replace("&quot;", "\"")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

/// Trims the model output and applies the parrot filter
/// (`tamako_core::wake::filter_reply_parrot_lines`, decision 59 F1):
/// every line whose trimmed start matches the recall-injection prefix
/// (ASCII or full-width colon) is removed. The filter runs at the
/// reply-text validation seam, BEFORE the outbound raw-log row
/// persists (Rule B1): the log and the group see the same filtered
/// text, and the raw log never carries hallucinated speech (Rule P1).
/// The actor applies the same filter to every generator output, so no
/// reply text can bypass it.
///
/// An empty or whitespace-only remainder — including a reply that was
/// ONLY a parrot block — is the SAME wake error as an empty reply
/// today (Section 9 step 4: log, skip this wake, no crash). Pure
/// function, no I/O.
pub fn trimmed_reply_or_error(text: &str) -> Result<ReplyFilterOutcome, CoreError> {
    let filtered = filter_reply_parrot_lines(text);
    if filtered.text.is_empty() {
        Err(CoreError::Wake(
            "the reply model returned an empty reply".to_string(),
        ))
    } else {
        Ok(filtered)
    }
}

/// The live reply generator: one completion call on the main
/// `reply_model` endpoint (specs.md Section 9 step 4 and Section 13).
/// Plain text output; no output schema.
pub struct RigReplyGenerator {
    client: EndpointClient,
    max_tokens: u64,
    /// The rendered decision-86 suffix body (the `<system>...</system>`
    /// string of `tamako_persona::render_suffix`), behind a shared lock
    /// so the decision-80 persona hot reload swaps it in place. EMPTY
    /// string means no suffix: no trailing system message is appended
    /// (byte-identical pre-86 behavior). Never persisted — it is read at
    /// request-assembly time and appended STRICTLY LAST, past the cached
    /// prefix, so an edit invalidates nothing (decision 86 (d)).
    suffix: Arc<RwLock<String>>,
}

// The rig model handles do not implement Debug. A manual impl keeps
// RigReplyGenerator printable in test failures and logs (same pattern
// as RigExtractor).
impl std::fmt::Debug for RigReplyGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RigReplyGenerator")
            .field("client", &self.client)
            .field("max_tokens", &self.max_tokens)
            .finish_non_exhaustive()
    }
}

impl RigReplyGenerator {
    /// Builds the generator from an endpoint client. The suffix slot
    /// starts EMPTY (no suffix message) until [`with_suffix_slot`] wires
    /// the shared persona-snapshot lock.
    pub fn new(client: EndpointClient, max_tokens: u64) -> Self {
        RigReplyGenerator {
            client,
            max_tokens,
            suffix: Arc::new(RwLock::new(String::new())),
        }
    }

    /// Builds the generator for one resolved endpoint (the `reply`
    /// purpose, specs.md Section 13). Returns
    /// `AgentError::ProviderConfig` when the family API key is missing.
    pub fn from_endpoint(endpoint: &EndpointConfig) -> Result<Self, AgentError> {
        Ok(RigReplyGenerator::new(
            EndpointClient::build(endpoint)?.with_purpose(LlmPurpose::Reply.as_str()),
            REPLY_DEFAULT_MAX_TOKENS,
        ))
    }

    /// Wires the shared decision-86 suffix slot (the binary's persona
    /// snapshot; the live persona watcher rewrites it on each accepted
    /// reload, decision 80). The generator reads the current rendered
    /// body at every `generate` — an edit takes effect on the next call
    /// with no context/anchor change. The default empty slot means tests
    /// and suffix-less deployments behave byte-identically to pre-86.
    pub fn with_suffix_slot(mut self, suffix: Arc<RwLock<String>>) -> Self {
        self.suffix = suffix;
        self
    }
}

impl ReplyGenerator for RigReplyGenerator {
    fn generate<'a>(
        &'a self,
        request: &'a ReplyRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, CoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            let suffix = self
                .suffix
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            let (preamble, messages) = assemble_reply_messages(request, &suffix);
            let text = self
                .client
                .complete(
                    preamble,
                    messages,
                    // No schema: the reply is plain text.
                    None,
                    self.max_tokens,
                )
                .await
                // A reply failure skips this wake; the next wake is the
                // natural retry (CoreError::Wake docs).
                .map_err(|error| CoreError::Wake(error.to_string()))?;
            // The parrot filter runs here too, at the reply-text
            // validation seam of the live generator (decision 59, F1).
            // The strip is silent at the generator: the WARN needs the
            // chat id, which only the actor owns — the actor filters
            // every generator output again and emits the WARN there.
            Ok(trimmed_reply_or_error(&text)?.text)
        })
    }
}

/// The response mode of `ScriptedReplyGenerator`.
enum ScriptedReplyMode {
    /// Pops the next reply per call (FIFO). An exhausted queue fails
    /// with `CoreError::Wake`.
    Replies(VecDeque<String>),
    /// Every call fails with `CoreError::Wake`.
    Failing(String),
}

/// A scripted reply generator for tests (same pattern as
/// `ScriptedExtractor`). Two modes:
///
/// - `ScriptedReplyGenerator::with_replies(vec_of_replies)`: pops the
///   next reply per call (FIFO; when exhausted, every call fails with
///   `CoreError::Wake("scripted replies exhausted")`);
/// - `ScriptedReplyGenerator::failing(message)`: every call fails with
///   `CoreError::Wake`.
///
/// Every `ReplyRequest` is recorded for assertions (`requests()`).
pub struct ScriptedReplyGenerator {
    mode: Mutex<ScriptedReplyMode>,
    requests: Mutex<Vec<ReplyRequest>>,
}

impl ScriptedReplyGenerator {
    /// A scripted generator that answers with the given replies in
    /// order.
    pub fn with_replies(replies: Vec<String>) -> Self {
        ScriptedReplyGenerator {
            mode: Mutex::new(ScriptedReplyMode::Replies(replies.into())),
            requests: Mutex::new(Vec::new()),
        }
    }

    /// A scripted generator whose every call fails.
    pub fn failing(message: impl Into<String>) -> Self {
        ScriptedReplyGenerator {
            mode: Mutex::new(ScriptedReplyMode::Failing(message.into())),
            requests: Mutex::new(Vec::new()),
        }
    }

    /// Every request the generator received, in call order.
    pub fn requests(&self) -> Vec<ReplyRequest> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl ReplyGenerator for ScriptedReplyGenerator {
    fn generate<'a>(
        &'a self,
        request: &'a ReplyRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, CoreError>> + Send + 'a>>
    {
        // Lock, record, and decide synchronously; the future only
        // carries the result. A poisoned mutex is recovered; the
        // recorded requests stay valid (same policy as
        // ScriptedExtractor).
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(request.clone());
        let result = {
            let mut mode = self.mode.lock().unwrap_or_else(PoisonError::into_inner);
            match &mut *mode {
                ScriptedReplyMode::Replies(replies) => replies
                    .pop_front()
                    .ok_or_else(|| CoreError::Wake("scripted replies exhausted".to_string())),
                ScriptedReplyMode::Failing(message) => Err(CoreError::Wake(message.clone())),
            }
        };
        Box::pin(async move { result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tamako_persona::render_suffix;

    fn context_message(role: ContextRole, content: &str) -> ContextMessage {
        ContextMessage {
            role,
            content: content.to_string(),
        }
    }

    fn sample_target() -> GateMessage {
        GateMessage {
            row_id: 42,
            platform_msg_id: "m42".to_string(),
            // The XML <msg> shape of Section 7.2 step 4; the reply path
            // reads `content` and `platform_msg_id` only, so the M5
            // recall fields carry plausible stand-ins.
            content: r#"<msg from="Bob" at="13:02" id="42">what should we eat?</msg>"#.to_string(),
            sender_id: "u2".to_string(),
            reply_to_platform_msg_id: None,
            text: "what should we eat?".to_string(),
        }
    }

    fn sample_request() -> ReplyRequest {
        ReplyRequest {
            messages: vec![
                context_message(ContextRole::System, "You are the group pet."),
                context_message(ContextRole::User, "[Alice 13:01] hungry"),
                context_message(ContextRole::Assistant, "the cafe on main street"),
                context_message(ContextRole::User, "[Bob 13:02] what should we eat?"),
            ],
            target: sample_target(),
        }
    }

    #[test]
    fn the_conversion_extracts_the_item_0_preamble() {
        // Rule C4: item 0 is the system preamble.
        let (preamble, messages) = context_messages_to_rig(&sample_request().messages);
        assert_eq!(preamble.as_deref(), Some("You are the group pet."));
        assert_eq!(messages.len(), 3);
    }

    /// The (role, text) view of one rig message for assertions. rig's
    /// `Message` carries `OneOrMany` content items; the conversion
    /// produces single text items only.
    fn role_and_text(message: &Message) -> (&'static str, String) {
        use rig::completion::message::{AssistantContent, UserContent};
        match message {
            Message::User { content } => match content.first_ref() {
                UserContent::Text(text) => ("user", text.text.clone()),
                other => panic!("expected user text, got {other:?}"),
            },
            Message::Assistant { content, .. } => match content.first_ref() {
                AssistantContent::Text(text) => ("assistant", text.text.clone()),
                other => panic!("expected assistant text, got {other:?}"),
            },
            Message::System { content } => ("system", content.clone()),
        }
    }

    #[test]
    fn the_conversion_maps_user_and_assistant_roles() {
        let (_, messages) = context_messages_to_rig(&sample_request().messages);
        let rendered: Vec<_> = messages.iter().map(role_and_text).collect();
        assert_eq!(
            rendered,
            vec![
                ("user", "[Alice 13:01] hungry".to_string()),
                ("assistant", "the cafe on main street".to_string()),
                ("user", "[Bob 13:02] what should we eat?".to_string()),
            ]
        );
    }

    #[test]
    fn a_late_system_maps_to_a_user_message() {
        // Defensive: only item 0 is System by construction (Rule C4).
        let messages = vec![
            context_message(ContextRole::System, "preamble"),
            context_message(ContextRole::User, "first"),
            context_message(ContextRole::System, "malformed late system"),
        ];
        let (preamble, rig_messages) = context_messages_to_rig(&messages);
        assert_eq!(preamble.as_deref(), Some("preamble"));
        let rendered: Vec<_> = rig_messages.iter().map(role_and_text).collect();
        assert_eq!(
            rendered,
            vec![
                ("user", "first".to_string()),
                ("user", "malformed late system".to_string()),
            ]
        );
    }

    #[test]
    fn a_leading_non_system_message_means_no_preamble() {
        // Defensive: Rule C4 guarantees a preamble, but the conversion
        // must not eat a user message as the preamble.
        let messages = vec![context_message(ContextRole::User, "orphan")];
        let (preamble, rig_messages) = context_messages_to_rig(&messages);
        assert_eq!(preamble, None);
        assert_eq!(rig_messages.len(), 1);
    }

    #[test]
    fn the_suffix_is_the_strictly_last_system_message() {
        // Decision 86 (a)/(b): ONE system-role message, STRICTLY LAST —
        // after the newest context message AND the reply instruction.
        let suffix = render_suffix(&["rule one".to_owned(), "rule two".to_owned()]);
        let (preamble, messages) = assemble_reply_messages(&sample_request(), &suffix);
        // The preamble still extracts as item 0.
        assert_eq!(preamble.as_deref(), Some("You are the group pet."));
        let last = messages.last().expect("a last message");
        // The last message is system-role and carries the verbatim body.
        match last {
            Message::System { content } => {
                assert!(content.starts_with("<system>"));
                assert!(content.contains("<rule1>\nrule one\n</rule1>"));
                assert!(content.contains("<rule2>\nrule two\n</rule2>"));
            }
            other => panic!("the suffix must be a system message, got {other:?}"),
        }
        // The newest context message and the reply instruction precede it:
        // the second-to-last is the user-role reply instruction, and the
        // newest context <msg> ("[Bob 13:02] ...") comes before that.
        let roles: Vec<&str> = messages.iter().map(|m| role_and_text(m).0).collect();
        assert_eq!(roles.last(), Some(&"system"));
        assert_eq!(
            roles[roles.len() - 2],
            "user",
            "the reply instruction is second-to-last"
        );
        let rendered: Vec<(&str, String)> = messages.iter().map(role_and_text).collect();
        let newest_context = rendered
            .iter()
            .position(|(_, text)| text.contains("[Bob 13:02]"))
            .expect("the newest context message");
        assert!(
            newest_context < messages.len() - 1,
            "the newest context message precedes the suffix"
        );
    }

    #[test]
    fn an_empty_suffix_appends_no_message() {
        // Decision 86 (f): an absent/empty suffix appends NO message — the
        // message list is byte-identical to the pre-86 layout (the C4
        // property at the tail). The last message is the reply instruction.
        let (_, with_empty) = assemble_reply_messages(&sample_request(), "");
        let rendered: Vec<(&str, String)> = with_empty.iter().map(role_and_text).collect();
        // 3 context messages + the reply instruction; NO trailing system.
        assert_eq!(rendered.len(), 4);
        assert_eq!(rendered.last().map(|(role, _)| *role), Some("user"));
        assert!(!rendered.iter().any(|(role, _)| *role == "system"));
    }

    #[test]
    fn the_reply_instruction_names_the_target_verbatim() {
        let instruction = render_reply_instruction(&sample_target());
        assert!(instruction.contains("id 42"));
        // Decision 64: the target embed is the non-XML form — the same
        // fields (id, sender, time, text) without the <msg> wrapper the
        // F2 sentence forbids.
        assert!(instruction.contains("from Bob at 13:02: \"what should we eat?\""));
        // The F2 sentence names the shapes by tag, so assert against
        // the full WRAPPER forms only (the embed must not teach them).
        assert!(!instruction.contains("<msg from="));
        assert!(instruction.contains("no speaker label"));
        assert!(instruction.contains("no quotes"));
    }

    #[test]
    fn the_reply_instruction_target_embed_never_carries_the_xml_wrapper() {
        // Decision 64: even when the target content is not the expected
        // rendered item (the defensive fallback arm), the embed stays
        // free of the XML shape.
        let mut target = sample_target();
        target.content = "not a rendered item".to_string();
        let instruction = render_reply_instruction(&target);
        assert!(instruction.contains("Reply to THIS message (id 42): \"what should we eat?\""));
        assert!(!instruction.contains("<msg from="));
        assert!(!instruction.contains("<you at="));
    }

    #[test]
    fn the_reply_instruction_forbids_the_injection_format() {
        // Decision 59, F2: the tail instruction hardening; decision 64
        // extends the forbidden list with the context shapes. The
        // sentence is part of the ephemeral call-only instruction; the
        // preamble (Rule C4 cache anchor) is untouched.
        let instruction = render_reply_instruction(&sample_target());
        assert!(instruction
            .contains("Never write \"I remember:\" lines, <memory> blocks, <summary> blocks, <msg> blocks, <you> blocks, or a memory list"));
        assert!(instruction.contains("recalled memories are context, never speech"));
    }

    #[test]
    fn the_reply_instruction_states_the_f2_sentence_and_the_target_verbatim() {
        // The full ephemeral instruction, byte-exact: the non-XML
        // target reference (decision 64) plus the F2 sentence naming
        // every shape the outbound parrot filter strips.
        let expected = concat!(
            "Reply to THIS message (id 42), from Bob at 13:02: \"what should we eat?\"",
            "\n",
            "Reply as the group pet persona. Write only the reply text: ",
            "one message, no speaker label, no quotes. ",
            "Never write \"I remember:\" lines, <memory> blocks, <summary> blocks, <msg> blocks, <you> blocks, or a memory list: ",
            "recalled memories are context, never speech.",
        );
        assert_eq!(render_reply_instruction(&sample_target()), expected);
    }

    #[test]
    fn an_empty_or_whitespace_reply_is_a_wake_error() {
        for empty in ["", "   ", "\n\t "] {
            match trimmed_reply_or_error(empty) {
                Err(CoreError::Wake(message)) => {
                    assert_eq!(message, "the reply model returned an empty reply")
                }
                other => panic!("expected Wake error for {empty:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_reply_is_trimmed() {
        let reply = trimmed_reply_or_error("  the cafe on main street \n").expect("reply");
        assert_eq!(reply.text, "the cafe on main street");
        assert!(!reply.stripped_parrot);
    }

    #[test]
    fn a_leading_parrot_block_is_stripped() {
        // Decision 59, F1: the model echoed the injection format before
        // its real speech (the live-soak failure shape).
        let reply = trimmed_reply_or_error(
            "I remember: Alice likes tea.\nI remember: Bob runs.\nthe cafe on main street",
        )
        .expect("reply");
        assert_eq!(reply.text, "the cafe on main street");
        assert!(reply.stripped_parrot);
    }

    #[test]
    fn a_mid_text_parrot_line_is_stripped() {
        let reply =
            trimmed_reply_or_error("one\nI remember: Alice likes tea.\ntwo").expect("reply");
        assert_eq!(reply.text, "one\ntwo");
        assert!(reply.stripped_parrot);
    }

    #[test]
    fn the_full_width_colon_variant_is_stripped() {
        // Chinese-context model output uses the full-width colon.
        let reply = trimmed_reply_or_error("I remember：小明喜欢吃辣。\n在的").expect("reply");
        assert_eq!(reply.text, "在的");
        assert!(reply.stripped_parrot);
    }

    #[test]
    fn a_reply_of_only_a_parrot_block_is_the_empty_reply_error() {
        // Decision 59, F1: nothing remains after the strip, so the wake
        // follows the EXACT path of an empty reply today — the same
        // CoreError::Wake, nothing persisted, nothing sent.
        for only in [
            "I remember: Alice likes tea.",
            "  I remember: Alice likes tea.\nI remember：小明喜欢吃辣。 ",
        ] {
            match trimmed_reply_or_error(only) {
                Err(CoreError::Wake(message)) => {
                    assert_eq!(message, "the reply model returned an empty reply")
                }
                other => panic!("expected Wake error for {only:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn normal_text_passes_byte_identical() {
        // False-positive control: an innocuous mid-line "I remember"
        // mention and multiline text survive untouched (line-start
        // anchored only).
        for normal in [
            "I remember when we tried that place",
            "在的",
            "one\ntwo\nthree",
            "hungry? I remember: not a line start",
        ] {
            let reply = trimmed_reply_or_error(normal).expect("reply");
            assert_eq!(reply.text, normal.trim());
            assert!(!reply.stripped_parrot, "false positive on {normal:?}");
        }
    }

    #[tokio::test]
    async fn scripted_replies_pop_fifo_then_fail() {
        let generator = ScriptedReplyGenerator::with_replies(vec!["pizza".to_string()]);
        let first = generator.generate(&sample_request()).await.expect("first");
        assert_eq!(first, "pizza");
        match generator.generate(&sample_request()).await {
            Err(CoreError::Wake(message)) => assert_eq!(message, "scripted replies exhausted"),
            other => panic!("expected Wake error, got {other:?}"),
        }
        assert_eq!(generator.requests().len(), 2);
        assert_eq!(generator.requests()[0], sample_request());
    }

    #[tokio::test]
    async fn scripted_failing_mode_fails_every_call_and_records_requests() {
        let generator = ScriptedReplyGenerator::failing("boom");
        for _ in 0..2 {
            match generator.generate(&sample_request()).await {
                Err(CoreError::Wake(message)) => assert_eq!(message, "boom"),
                other => panic!("expected Wake error, got {other:?}"),
            }
        }
        assert_eq!(generator.requests().len(), 2);
    }

    #[test]
    fn from_endpoint_without_an_api_key_is_a_provider_config_error() {
        // The test environment must not carry a key for this assertion.
        use crate::endpoint::env_lock::ENV_LOCK;
        let _lock = ENV_LOCK.lock().unwrap();
        let saved_key = std::env::var(crate::endpoint::ANTHROPIC_API_KEY_ENV_VAR).ok();
        let saved_family = std::env::var(crate::endpoint::LLM_API_ENV_VAR).ok();
        std::env::remove_var(crate::endpoint::ANTHROPIC_API_KEY_ENV_VAR);
        std::env::remove_var(crate::endpoint::LLM_API_ENV_VAR);
        let endpoint = EndpointConfig {
            api: crate::endpoint::LlmApi::AnthropicCompatible,
            base_url: None,
            model: "claude-sonnet-4-5".to_string(),
            structured_output: crate::endpoint::StructuredOutputMode::Schema,
            session_id: crate::endpoint::DEFAULT_SESSION_ID.to_string(),
        };
        let result = RigReplyGenerator::from_endpoint(&endpoint);
        if let Some(key) = saved_key {
            std::env::set_var(crate::endpoint::ANTHROPIC_API_KEY_ENV_VAR, key);
        }
        if let Some(value) = saved_family {
            std::env::set_var(crate::endpoint::LLM_API_ENV_VAR, value);
        }
        match result {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig error, got {other:?}"),
        }
    }
}
