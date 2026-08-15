//! The live context of one group: the ordered item list of specs.md
//! Section 7. The live context is a materialized view of the raw message
//! log (Rule P1). The actor rebuilds it after a restart from the
//! `messages` table, the `injected_memories` table, and the session state.
//!
//! Structural argument (Rules C1 and C2): the public API permits appends
//! at the tail ONLY. There is deliberately no method that inserts into,
//! removes from, or mutates the middle of the history: no insert-at-index,
//! no remove-at-index, no mutable iterator, no public item vector. The
//! only destructive operations are `remove_at_or_below` (the Rule C3
//! digest-time removal, which drops a closed range at or below a boundary),
//! `upsert_summaries` (the summary-block replacement, the only mutation
//! path of Summary items — it runs only inside the actor-serialized
//! digest-completion window and at rebuild), and `reload_preamble` (the
//! Rule C4 item-0 replacement).
//!
//! This crate is model-agnostic. `ContextMessage` is the LLM-facing view;
//! tamako-agent converts it to rig completion messages in M4.

use std::collections::{BTreeMap, HashMap};

use time::macros::format_description;
use time::{OffsetDateTime, UtcOffset};

use tamako_store::{
    ContextSummaryRow, Direction, EventType, InjectedMemoryRow, MessageRow, ReplyTargetRow,
};

/// The UTC HH:MM format of the timestamp attributes (specs.md
/// Section 7.3).
const HHMM_FORMAT: &[time::format_description::FormatItem<'_>] =
    format_description!("[hour]:[minute]");

/// The UTC HH:MM of one message row, with the "??:??" fallback on a
/// format failure (the pattern of tamako-agent/src/pipeline.rs).
fn hhmm_of(timestamp: OffsetDateTime) -> String {
    timestamp
        .to_offset(UtcOffset::UTC)
        .format(HHMM_FORMAT)
        .unwrap_or_else(|_| "??:??".to_string())
}

/// Escapes text content for a text node: `&` first, then `<` and `>`.
/// Shared by the item renderers of this module and by
/// `wake::render_injection_content` (decision 59 single-source
/// discipline).
pub(crate) fn escape_xml_text(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Escapes an attribute value: the text set plus `"` → `&quot;`.
pub(crate) fn escape_xml_attr(text: &str) -> String {
    escape_xml_text(text).replace('"', "&quot;")
}

/// How one human message renders its reply attribute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyRender {
    /// Not a reply.
    None,
    /// A reply addressed to the bot (Rules A3/B1). Renders
    /// `reply="bot"` with NO target name/id: outbound rows carry
    /// synthetic `bot-out:{nanos}` ids, so the real platform id of the
    /// bot's message can never resolve to a stored row. Do not fake a
    /// resolution.
    ToBot,
    /// `reply_to_platform_msg_id` was set. `target` is `Some` when the
    /// raw log resolves it (`Store::find_reply_target`), `None` when
    /// the target is absent from the log (e.g. predates the bot) —
    /// both deterministic. `None` renders `reply="user"` without the
    /// target attributes.
    ToUser { target: Option<ReplyTargetRow> },
}

/// Role of an item for model input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContextRole {
    System,
    User,
    Assistant,
}

/// Kind of a context item. specs.md Section 7.1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContextItemKind {
    /// Item 0: the persona preamble. The provider cache anchor (Rule C4).
    Preamble,
    HumanMessage,
    BotSpeech,
    /// The kind exists now; only M5 (shallow recall) produces these items.
    RecallInjection,
    /// Reserved. No producer exists in Phase 1.
    ToolOutput,
    /// A summary of a digested chunk (segmented summarization, Rule C3
    /// keep-two retention). Role User on purpose: a summary is compressed
    /// HISTORY — reference data like human messages, not the bot's own
    /// recollection. The User role also shrinks the self-imitation
    /// parrot channel (an Assistant-role summary would train the reply
    /// model to speak `<summary>` blocks).
    Summary,
}

/// The message-id range tag of specs.md Section 7.1. Raw-log row ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeTag {
    pub first_msg_id: i64,
    pub last_msg_id: i64,
}

impl RangeTag {
    /// A tag that covers exactly one message.
    pub fn single(msg_id: i64) -> Self {
        Self {
            first_msg_id: msg_id,
            last_msg_id: msg_id,
        }
    }

    /// The canonical `range_tag` string form `"{first}-{last}"`, as stored
    /// in the `injected_memories` rows of M5.
    pub fn as_string(&self) -> String {
        format!("{}-{}", self.first_msg_id, self.last_msg_id)
    }
}

/// One item of the live context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextItem {
    pub kind: ContextItemKind,
    pub role: ContextRole,
    pub content: String,
    /// `None` only for the preamble (item 0).
    pub range_tag: Option<RangeTag>,
}

/// The LLM-facing message view. tamako-agent converts these to rig
/// completion messages in M4; tamako-core stays model-agnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextMessage {
    pub role: ContextRole,
    pub content: String,
}

/// Lightweight stats for future metrics (specs.md Section 12).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextStats {
    pub item_count: usize,
    pub estimated_bytes: usize,
}

/// The live context: the ordered item list of specs.md Section 7.1.
/// Item 0 is always the system preamble. Then comes the previous digested
/// chunk (the overlap buffer). Then comes the current tail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveContext {
    items: Vec<ContextItem>,
}

impl LiveContext {
    /// Creates a context that holds only the preamble. Rule C4: item 0 is
    /// the provider cache anchor; the prefix never changes between
    /// digests.
    pub fn new(preamble: String) -> Self {
        Self {
            items: vec![ContextItem {
                kind: ContextItemKind::Preamble,
                role: ContextRole::System,
                content: preamble,
                range_tag: None,
            }],
        }
    }

    /// The preamble text (item 0).
    pub fn preamble(&self) -> &str {
        // `items` is never empty: `new` inserts the preamble, and no
        // public method removes item 0.
        &self.items[0].content
    }

    /// The ordered item list, read-only.
    pub fn items(&self) -> &[ContextItem] {
        &self.items
    }

    /// Appends one human message at the tail (Rule C1). Role User, kind
    /// HumanMessage. The content is the XML item rendering of
    /// [`render_human_content`].
    #[allow(clippy::too_many_arguments)]
    pub fn append_human_message(
        &mut self,
        msg_id: i64,
        display_name: &str,
        username: Option<&str>,
        timestamp: OffsetDateTime,
        is_edit: bool,
        mentions_bot: bool,
        reply: ReplyRender,
        text: &str,
    ) {
        self.items.push(ContextItem {
            kind: ContextItemKind::HumanMessage,
            role: ContextRole::User,
            content: render_human_content(
                msg_id,
                display_name,
                username,
                timestamp,
                is_edit,
                mentions_bot,
                reply,
                text,
            ),
            range_tag: Some(RangeTag::single(msg_id)),
        });
    }

    /// Appends one message of the bot at the tail (Rule C1). Rule B1: the
    /// bot's own speech is part of the raw log. The model sees its own
    /// speech as plain assistant text; the `<you>` element distinguishes
    /// group members, not the bot. This is the M4 append API for the
    /// bot's own sent messages.
    pub fn append_bot_speech(&mut self, msg_id: i64, timestamp: OffsetDateTime, text: &str) {
        self.items.push(ContextItem {
            kind: ContextItemKind::BotSpeech,
            role: ContextRole::Assistant,
            content: render_bot_content(msg_id, timestamp, text),
            range_tag: Some(RangeTag::single(msg_id)),
        });
    }

    /// Appends one recall injection at the tail. Rule C2: an injection is
    /// appended at the TAIL, directly after the messages that triggered
    /// it; `position_msg_id` is the id of the log row the injection
    /// directly follows (the tail at append time). This is the M5 append
    /// API; the content arrives already rendered.
    pub fn append_recall_injection(&mut self, position_msg_id: i64, content: String) {
        self.items.push(ContextItem {
            kind: ContextItemKind::RecallInjection,
            role: ContextRole::Assistant,
            content,
            range_tag: Some(RangeTag::single(position_msg_id)),
        });
    }

    /// Replaces the summary block wholesale (the keep-two summary API).
    /// Removes EVERY existing Summary item, then inserts the given rows
    /// (callers pass them OLDEST FIRST) as Summary items immediately
    /// after the preamble (item 0), before every other item, each
    /// rendered via [`render_summary_content`].
    ///
    /// This is the ONLY mutation path for Summary items (Rules C1/C2:
    /// no general mid-insert). The keep-two retention is enforced by
    /// the CALLER, which passes at most the two newest persisted rows;
    /// this API itself only replaces — it never counts. It runs only
    /// inside the actor-serialized digest-completion window and at
    /// rebuild, so summary items never race the tail appends.
    ///
    /// The wholesale replace is what makes the live placement and the
    /// rebuild placement trivially identical (Rule P1): both paths call
    /// this same method with the same store query result.
    pub fn upsert_summaries(&mut self, summaries: &[ContextSummaryRow]) {
        self.items
            .retain(|item| item.kind != ContextItemKind::Summary);
        let rendered = summaries
            .iter()
            .map(|summary| ContextItem {
                kind: ContextItemKind::Summary,
                role: ContextRole::User,
                content: render_summary_content(
                    summary.first_msg_id,
                    summary.last_msg_id,
                    &summary.content,
                ),
                range_tag: Some(RangeTag {
                    first_msg_id: summary.first_msg_id,
                    last_msg_id: summary.last_msg_id,
                }),
            })
            .collect::<Vec<_>>();
        // Item 0 is always the preamble; the summary block lands right
        // after it, before the raw items and the injections.
        self.items.splice(1..1, rendered);
    }

    /// Rule C3: removes every item with a range tag at or below
    /// `boundary_msg_id`. The preamble is never removed.
    ///
    /// One-chunk-lag semantics (specs.md Section 7.1): the actor calls
    /// this with the PREVIOUS boundary B_old when the digest boundary
    /// advances B_old -> B_new. The chunk just digested, (B_old, B_new],
    /// stays one more chunk as the overlap buffer.
    ///
    /// C3 amendment (segmented summarization): Summary items are
    /// EXEMPT from this removal. Summary retention is count-based
    /// keep-two, driven by `upsert_summaries`, never by the C3 cutoff —
    /// a summary whose `last_msg_id` is at or below the boundary would
    /// otherwise be killed early, before its successor digest lands.
    pub fn remove_at_or_below(&mut self, boundary_msg_id: i64) {
        self.items.retain(|item| match &item.range_tag {
            None => true,
            // C3 amendment: the summary block survives every cutoff.
            Some(_) if item.kind == ContextItemKind::Summary => true,
            Some(tag) => tag.last_msg_id > boundary_msg_id,
        });
    }

    /// Rule C4: replaces item 0 with the new preamble. Every item after
    /// item 0 is preserved unchanged. A preamble change is a deliberate
    /// full context invalidation event: the provider prefix cache for this
    /// group is void from this point. Preamble edits are deliberate
    /// events, never runtime side effects (the strict persona startup
    /// policy is M6).
    pub fn reload_preamble(&mut self, new_preamble: String) {
        self.items[0].content = new_preamble;
    }

    /// The ordered item list for model input, preamble first. tamako-agent
    /// converts these to rig message types in M4.
    pub fn messages_for_llm(&self) -> Vec<ContextMessage> {
        self.items
            .iter()
            .map(|item| ContextMessage {
                role: item.role.clone(),
                content: item.content.clone(),
            })
            .collect()
    }

    /// Item count and the sum of the content lengths in UTF-8 bytes.
    pub fn stats(&self) -> ContextStats {
        ContextStats {
            item_count: self.items.len(),
            estimated_bytes: self.items.iter().map(|item| item.content.len()).sum(),
        }
    }

    /// Rule P1 restart rebuild. The result is bit-identical to the
    /// pre-restart in-memory context (same items, same order, same tags).
    ///
    /// Precondition (the caller filters): `rows` are the raw-log rows with
    /// id above the removal cutoff, ordered by id; `injections` are the
    /// dedup rows with injection_position above the cutoff, ordered by id;
    /// `summaries` are the kept summary rows (the two newest persisted
    /// rows, OLDEST FIRST — the caller passes the same store query the
    /// live digest-completion window consumes).
    ///
    /// Bit-identity contract (Rule P1): the caller builds `reply_targets`
    /// with the same store function used at intake
    /// (`Store::find_reply_target` over the same `chat_id`), so rebuild
    /// renders every row exactly like the incremental append did. The
    /// summaries land through the SAME `upsert_summaries` call as the
    /// live digest-completion window, so live placement and rebuild
    /// placement are trivially identical.
    ///
    /// Edit rows render `kind="edit"` from `row.event_type` (this
    /// amends the earlier "edits render identically" behavior
    /// deliberately). Injections land directly after the row whose id
    /// equals `injection_position` (Rule C2), content verbatim from the
    /// persisted row. Leftover injections (position matches no row id,
    /// e.g. a position beyond the current tail) are appended at the tail
    /// in injection-row order.
    ///
    /// Multi-edge collapse (decision 65): `injected_memories` persists
    /// ONE ROW PER EDGE (Section 9.3 dedup), while the live wake path
    /// appends ONE RecallInjection item per PLANNED injection — the N
    /// edges of one multi-edge injection persist N consecutive rows
    /// with the same (injection_position, content) but produce a single
    /// live item. The rebuild therefore collapses every RUN of
    /// consecutive rows sharing (injection_position, content) into one
    /// item, keeping the rebuilt context bit-identical to the live one
    /// (Rule P1). Non-consecutive duplicates and rows with different
    /// content or position never collapse: they are genuinely distinct
    /// injections. NOTE: this changes the rebuilt context bytes of
    /// groups with persisted multi-edge injections (fewer duplicate
    /// `<memory>` items) — one deliberate invalidation, deployed
    /// together with decision 64.
    pub fn rebuild(
        preamble: String,
        rows: &[MessageRow],
        injections: &[InjectedMemoryRow],
        reply_targets: &HashMap<String, ReplyTargetRow>,
        summaries: &[ContextSummaryRow],
    ) -> Self {
        // BTreeMap: placement is deterministic (positions in id order,
        // injections at one position in injection-row order).
        let mut injections_by_position: BTreeMap<i64, Vec<&InjectedMemoryRow>> = BTreeMap::new();
        for injection in injections {
            injections_by_position
                .entry(injection.injection_position)
                .or_default()
                .push(injection);
        }

        let mut context = Self::new(preamble);
        // Rule P1: the summary block lands through the same upsert as
        // the live digest-completion window (bit-identity), directly
        // after the preamble, before every raw item.
        context.upsert_summaries(summaries);
        for row in rows {
            match row.direction {
                Direction::Inbound => {
                    // Rules A3/B1: a reply to the bot renders `reply="bot"`
                    // with no target name/id — outbound rows carry
                    // synthetic `bot-out:{nanos}` ids, so the real
                    // platform id of the bot's message can never resolve
                    // to a stored row. Do not fake a resolution.
                    let reply = if row.is_reply_to_bot {
                        ReplyRender::ToBot
                    } else {
                        match &row.reply_to_platform_msg_id {
                            Some(pid) => ReplyRender::ToUser {
                                target: reply_targets.get(pid).cloned(),
                            },
                            None => ReplyRender::None,
                        }
                    };
                    context.append_human_message(
                        row.id,
                        &row.sender_display_name,
                        row.sender_username.as_deref(),
                        row.timestamp,
                        row.event_type == EventType::Edit,
                        row.mentions_bot,
                        reply,
                        &row.text,
                    );
                }
                Direction::Outbound => context.append_bot_speech(row.id, row.timestamp, &row.text),
            }
            if let Some(here) = injections_by_position.remove(&row.id) {
                append_injections_collapsed(&mut context, &here);
            }
        }
        // Leftover injections: the position matches no row id. Append them
        // at the tail in injection-row order.
        for (_, here) in injections_by_position {
            append_injections_collapsed(&mut context, &here);
        }
        context
    }
}

/// Appends one RecallInjection item per RUN of consecutive rows that
/// share (injection_position, content) — the decision 65 multi-edge
/// collapse of [`LiveContext::rebuild`]. The store persists one
/// `injected_memories` row per EDGE of a planned injection, and the
/// per-edge insert loop of the wake completion handler lands those rows
/// consecutively (row-id order) with identical position and content,
/// while the live context received ONE item for the whole injection.
/// Collapsing exactly the identical-content runs reproduces the live
/// view; rows with different content or position are distinct
/// injections and always append.
fn append_injections_collapsed(context: &mut LiveContext, injections: &[&InjectedMemoryRow]) {
    let mut previous: Option<(i64, &str)> = None;
    for injection in injections {
        let key = (injection.injection_position, injection.content.as_str());
        if previous == Some(key) {
            continue;
        }
        context.append_recall_injection(injection.injection_position, injection.content.clone());
        previous = Some(key);
    }
}

/// The opening-tag prefix of a rendered human-message item: `"<msg "`
/// (with the trailing space before the attributes). This constant and
/// [`render_human_content`] are the SINGLE source shared by the
/// renderer and the outbound parrot filter
/// ([`crate::wake::filter_reply_parrot_lines`]): the `<msg>` element is
/// a model-visible format the reply model can imitate (the live-soak
/// `<msg>`/`<you>` parroting incident of 2026-08-14), and a
/// confabulated `<msg>` block must never reach the group. Sharing the
/// constant keeps the renderer and the filter from drifting apart
/// (decisions 59/61 single-source discipline).
pub const MSG_TAG_OPEN_PREFIX: &str = "<msg ";

/// The closing tag of a rendered human-message item: `"</msg>"`. The
/// closer of the [`MSG_TAG_OPEN_PREFIX`] strip region in the parrot
/// filter.
pub const MSG_TAG_CLOSE: &str = "</msg>";

/// The opening-tag prefix of a rendered bot-speech item: `"<you "`
/// (with the trailing space before the attributes). Same single-source
/// discipline as [`MSG_TAG_OPEN_PREFIX`]: shared by
/// [`render_bot_content`] and the outbound parrot filter
/// ([`crate::wake::filter_reply_parrot_lines`]).
pub const YOU_TAG_OPEN_PREFIX: &str = "<you ";

/// The closing tag of a rendered bot-speech item: `"</you>"`. The
/// closer of the [`YOU_TAG_OPEN_PREFIX`] strip region in the parrot
/// filter.
pub const YOU_TAG_CLOSE: &str = "</you>";

/// Renders one human message as the XML item of specs.md Section 7.2
/// step 4 (the approved XML context rendering). This one helper serves
/// `append_human_message`, `rebuild`, and the M4 gate input
/// (`wake::GateMessage::content`): one render helper keeps the gate
/// input consistent with the live context, and makes the rebuild
/// bit-identical.
///
/// Output grammar (attribute order fixed; `at` = UTC `[hour]:[minute]`
/// with the "??:??" fallback; attribute values attr-escaped; text
/// text-escaped):
///
/// ```text
/// <msg from="{display_name}"[ user="{username}"] at="{HH:MM}" id="{msg_id}"
///      [ kind="edit"][ reply="bot"][ reply="user"[ reply_to_name="{name}"
///      reply_to_id="{row_id}"]][ mention="bot"]>{text}</msg>
/// ```
///
/// Flag precedence when several apply: `kind`, then `reply`, then
/// `mention` (a message can be both a reply and a mention — both
/// render).
#[allow(clippy::too_many_arguments)]
pub fn render_human_content(
    msg_id: i64,
    display_name: &str,
    username: Option<&str>,
    timestamp: OffsetDateTime,
    is_edit: bool,
    mentions_bot: bool,
    reply: ReplyRender,
    text: &str,
) -> String {
    let mut out = String::new();
    out.push_str(MSG_TAG_OPEN_PREFIX);
    out.push_str("from=\"");
    out.push_str(&escape_xml_attr(display_name));
    out.push('"');
    if let Some(username) = username {
        out.push_str(" user=\"");
        out.push_str(&escape_xml_attr(username));
        out.push('"');
    }
    out.push_str(" at=\"");
    out.push_str(&hhmm_of(timestamp));
    out.push('"');
    out.push_str(" id=\"");
    out.push_str(&msg_id.to_string());
    out.push('"');
    if is_edit {
        out.push_str(" kind=\"edit\"");
    }
    match &reply {
        ReplyRender::None => {}
        ReplyRender::ToBot => out.push_str(" reply=\"bot\""),
        ReplyRender::ToUser {
            target: Some(target),
        } => {
            out.push_str(" reply=\"user\"");
            out.push_str(" reply_to_name=\"");
            out.push_str(&escape_xml_attr(&target.display_name));
            out.push('"');
            out.push_str(" reply_to_id=\"");
            out.push_str(&target.row_id.to_string());
            out.push('"');
        }
        ReplyRender::ToUser { target: None } => out.push_str(" reply=\"user\""),
    }
    if mentions_bot {
        out.push_str(" mention=\"bot\"");
    }
    out.push('>');
    out.push_str(&escape_xml_text(text));
    out.push_str(MSG_TAG_CLOSE);
    out
}

/// Renders one message of the bot: the `<you>` element of the approved
/// XML context rendering. The model sees its own speech as plain
/// assistant text; `<you>` marks the bot's rows (Rule B1: the bot's own
/// speech is part of the raw log).
pub fn render_bot_content(msg_id: i64, timestamp: OffsetDateTime, text: &str) -> String {
    format!(
        "{YOU_TAG_OPEN_PREFIX}at=\"{}\" id=\"{msg_id}\">{}{YOU_TAG_CLOSE}",
        hhmm_of(timestamp),
        escape_xml_text(text)
    )
}

/// The opening-tag prefix of a rendered summary item: `"<summary"`.
/// This is the filter anchor of the outbound parrot filter
/// ([`crate::wake::filter_reply_parrot_lines`]): a summary block is a
/// model-visible format the reply model can imitate, and a confabulated
/// `<summary>` block must never reach the group. This constant and
/// [`render_summary_content`] are the SINGLE source shared by the
/// renderer and the filter, so the summary format and the filter can
/// never drift apart (decisions 59/61 single-source discipline).
pub const SUMMARY_TAG_OPEN_PREFIX: &str = "<summary";

/// The closing tag of a rendered summary item: `"</summary>"`. The
/// closer of the [`SUMMARY_TAG_OPEN_PREFIX`] strip region in the
/// parrot filter.
pub const SUMMARY_TAG_CLOSE: &str = "</summary>";

/// Renders one context summary as the `<summary>` item:
/// `<summary range="{first}-{last}">{text}</summary>`. The range
/// attribute is the canonical `{first}-{last}` string form of
/// [`RangeTag`]; attribute values are plain integers.
///
/// The summary compresses group messages → it is the Section 9.4
/// indirect-injection channel AGAIN: `text` is XML-text-escaped
/// (specs.md Section 9.4), so a hostile raw-log text can never break
/// out of the tag.
pub fn render_summary_content(first_msg_id: i64, last_msg_id: i64, text: &str) -> String {
    format!(
        "{SUMMARY_TAG_OPEN_PREFIX} range=\"{first_msg_id}-{last_msg_id}\">{}{SUMMARY_TAG_CLOSE}",
        escape_xml_text(text)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tamako_store::EventType;
    use time::macros::datetime;

    fn at_1307() -> OffsetDateTime {
        datetime!(2026-08-07 13:07 UTC)
    }

    fn at_utc(hour: u8, minute: u8) -> OffsetDateTime {
        let date =
            time::Date::from_calendar_date(2026, time::Month::August, 7).expect("a valid date");
        let time = time::Time::from_hms(hour, minute, 0).expect("a valid time");
        date.with_time(time).assume_utc()
    }

    fn tag_of(item: &ContextItem) -> RangeTag {
        item.range_tag.clone().expect("a range tag")
    }

    fn target(row_id: i64, name: &str) -> ReplyTargetRow {
        ReplyTargetRow {
            row_id,
            display_name: name.to_string(),
        }
    }

    #[test]
    fn new_context_has_the_preamble_as_item_zero() {
        let context = LiveContext::new("You are Tamako.".to_string());
        assert_eq!(context.items().len(), 1);
        let item = &context.items()[0];
        assert_eq!(item.kind, ContextItemKind::Preamble);
        assert_eq!(item.role, ContextRole::System);
        assert_eq!(item.range_tag, None);
        assert_eq!(context.preamble(), "You are Tamako.");
    }

    #[test]
    fn appends_land_at_the_tail_in_order() {
        let mut context = LiveContext::new("P".to_string());
        context.append_human_message(
            1,
            "Alice",
            None,
            at_1307(),
            false,
            false,
            ReplyRender::None,
            "hello",
        );
        context.append_bot_speech(2, at_1307(), "hi there");
        context.append_recall_injection(2, "I remember: Alice likes tea.".to_string());

        assert_eq!(context.items().len(), 4);
        let human = &context.items()[1];
        assert_eq!(human.kind, ContextItemKind::HumanMessage);
        assert_eq!(human.role, ContextRole::User);
        assert_eq!(
            human.content,
            r#"<msg from="Alice" at="13:07" id="1">hello</msg>"#
        );
        assert_eq!(tag_of(human), RangeTag::single(1));

        let speech = &context.items()[2];
        assert_eq!(speech.kind, ContextItemKind::BotSpeech);
        assert_eq!(speech.role, ContextRole::Assistant);
        assert_eq!(speech.content, r#"<you at="13:07" id="2">hi there</you>"#);
        assert_eq!(tag_of(speech), RangeTag::single(2));

        let injection = &context.items()[3];
        assert_eq!(injection.kind, ContextItemKind::RecallInjection);
        assert_eq!(injection.role, ContextRole::Assistant);
        assert_eq!(injection.content, "I remember: Alice likes tea.");
        assert_eq!(tag_of(injection), RangeTag::single(2));
    }

    #[test]
    fn the_message_renderers_compose_through_the_tag_constants() {
        // Single-source discipline (decisions 59/61): the renderers and
        // the parrot filter share the tag constants, so they can never
        // drift apart. The exact bytes stay pinned by the existing
        // renderer tests; this one pins the composition.
        let human = render_human_content(
            1,
            "Alice",
            None,
            at_1307(),
            false,
            false,
            ReplyRender::None,
            "hello",
        );
        assert!(human.starts_with(MSG_TAG_OPEN_PREFIX));
        assert!(human.ends_with(MSG_TAG_CLOSE));

        let speech = render_bot_content(2, at_1307(), "hi there");
        assert!(speech.starts_with(YOU_TAG_OPEN_PREFIX));
        assert!(speech.ends_with(YOU_TAG_CLOSE));
    }

    #[test]
    fn lag_one_semantics_across_two_digests() {
        let mut context = LiveContext::new("P".to_string());
        for msg_id in 1..=3 {
            context.append_human_message(
                msg_id,
                "Alice",
                None,
                at_1307(),
                false,
                false,
                ReplyRender::None,
                "chunk one",
            );
        }
        // Digest 1: boundary advances 0 -> 3. The actor removes at or
        // below the PREVIOUS boundary (0). Nothing is removed.
        context.remove_at_or_below(0);
        assert_eq!(context.items().len(), 4);

        for msg_id in 4..=6 {
            context.append_human_message(
                msg_id,
                "Alice",
                None,
                at_1307(),
                false,
                false,
                ReplyRender::None,
                "chunk two",
            );
        }
        // Digest 2: boundary advances 3 -> 6. Removal at or below the
        // previous boundary (3) drops chunk one; chunk two stays as the
        // overlap buffer.
        context.remove_at_or_below(3);

        assert_eq!(context.items().len(), 4);
        assert_eq!(context.items()[0].kind, ContextItemKind::Preamble);
        let tags: Vec<RangeTag> = context.items()[1..].iter().map(tag_of).collect();
        assert_eq!(
            tags,
            vec![
                RangeTag::single(4),
                RangeTag::single(5),
                RangeTag::single(6)
            ]
        );
    }

    #[test]
    fn removal_at_or_below_covers_every_kind() {
        let mut context = LiveContext::new("P".to_string());
        context.append_human_message(
            1,
            "Alice",
            None,
            at_1307(),
            false,
            false,
            ReplyRender::None,
            "hi",
        );
        context.append_bot_speech(2, at_1307(), "hello");
        context.append_recall_injection(2, "memory at 2".to_string());
        context.append_recall_injection(3, "memory at 3".to_string());

        context.remove_at_or_below(2);

        assert_eq!(context.items().len(), 2);
        assert_eq!(context.items()[0].kind, ContextItemKind::Preamble);
        let survivor = &context.items()[1];
        assert_eq!(survivor.kind, ContextItemKind::RecallInjection);
        assert_eq!(survivor.content, "memory at 3");
        assert_eq!(tag_of(survivor), RangeTag::single(3));
    }

    #[test]
    fn removal_boundary_is_exact() {
        let mut context = LiveContext::new("P".to_string());
        context.append_human_message(
            5,
            "Alice",
            None,
            at_1307(),
            false,
            false,
            ReplyRender::None,
            "at five",
        );
        context.append_human_message(
            6,
            "Alice",
            None,
            at_1307(),
            false,
            false,
            ReplyRender::None,
            "at six",
        );

        context.remove_at_or_below(5);

        // Tag equality means removed; a tag above the boundary is kept.
        assert_eq!(context.items().len(), 2);
        assert_eq!(tag_of(&context.items()[1]), RangeTag::single(6));
    }

    #[test]
    fn reload_preamble_replaces_item_zero_only() {
        let mut context = LiveContext::new("old preamble".to_string());
        context.append_human_message(
            1,
            "Alice",
            None,
            at_1307(),
            false,
            false,
            ReplyRender::None,
            "hello",
        );
        context.append_bot_speech(2, at_1307(), "hi");
        let tail_before: Vec<ContextItem> = context.items()[1..].to_vec();

        context.reload_preamble("new preamble".to_string());

        assert_eq!(context.preamble(), "new preamble");
        assert_eq!(context.items().len(), 3);
        // Same tail items, same order, same tags.
        assert_eq!(&context.items()[1..], tail_before.as_slice());
    }

    fn row(
        id: i64,
        direction: Direction,
        event_type: EventType,
        name: &str,
        username: Option<&str>,
        text: &str,
    ) -> MessageRow {
        MessageRow {
            id,
            platform_msg_id: format!("p{id}"),
            direction,
            event_type,
            timestamp: at_1307(),
            sender_id: format!("u{id}"),
            sender_display_name: name.to_string(),
            sender_username: username.map(str::to_string),
            text: text.to_string(),
            reply_to_platform_msg_id: None,
            mentions_bot: false,
            is_reply_to_bot: false,
        }
    }

    fn injection(id: i64, position: i64, content: &str) -> InjectedMemoryRow {
        InjectedMemoryRow {
            id,
            edge_id: format!("e{id}"),
            injection_position: position,
            range_tag: RangeTag::single(position).as_string(),
            content: content.to_string(),
        }
    }

    fn summary_row(
        id: i64,
        first_msg_id: i64,
        last_msg_id: i64,
        content: &str,
    ) -> ContextSummaryRow {
        ContextSummaryRow {
            id,
            first_msg_id,
            last_msg_id,
            content: content.to_string(),
        }
    }

    #[test]
    fn rebuild_places_items_and_injections_in_order() {
        let rows = vec![
            row(
                1,
                Direction::Inbound,
                EventType::Message,
                "Alice",
                Some("alice_tg"),
                "one",
            ),
            row(
                2,
                Direction::Outbound,
                EventType::Message,
                "Tamako",
                None,
                "two",
            ),
            row(
                3,
                Direction::Inbound,
                EventType::Edit,
                "Alice",
                Some("alice_tg"),
                "three (edited)",
            ),
        ];
        let injections = vec![
            injection(10, 2, "memory A at 2"),
            injection(11, 2, "memory B at 2"),
            injection(12, 1, "memory at 1"),
            injection(13, 99, "memory beyond the tail"),
        ];

        let context =
            LiveContext::rebuild("P".to_string(), &rows, &injections, &HashMap::new(), &[]);

        let rendered: Vec<(ContextItemKind, String)> = context
            .items()
            .iter()
            .map(|item| (item.kind.clone(), item.content.clone()))
            .collect();
        assert_eq!(
            rendered,
            vec![
                (ContextItemKind::Preamble, "P".to_string()),
                (
                    ContextItemKind::HumanMessage,
                    r#"<msg from="Alice" user="alice_tg" at="13:07" id="1">one</msg>"#
                        .to_string()
                ),
                // Injection order at one position is stable (row order).
                (ContextItemKind::RecallInjection, "memory at 1".to_string()),
                (
                    ContextItemKind::BotSpeech,
                    r#"<you at="13:07" id="2">two</you>"#.to_string()
                ),
                (
                    ContextItemKind::RecallInjection,
                    "memory A at 2".to_string()
                ),
                (
                    ContextItemKind::RecallInjection,
                    "memory B at 2".to_string()
                ),
                // An edit row renders kind="edit".
                (
                    ContextItemKind::HumanMessage,
                    r#"<msg from="Alice" user="alice_tg" at="13:07" id="3" kind="edit">three (edited)</msg>"#
                        .to_string()
                ),
                // A position beyond the tail lands at the tail.
                (
                    ContextItemKind::RecallInjection,
                    "memory beyond the tail".to_string()
                ),
            ]
        );
        let tags: Vec<Option<RangeTag>> = context
            .items()
            .iter()
            .map(|item| item.range_tag.clone())
            .collect();
        assert_eq!(
            tags,
            vec![
                None,
                Some(RangeTag::single(1)),
                Some(RangeTag::single(1)),
                Some(RangeTag::single(2)),
                Some(RangeTag::single(2)),
                Some(RangeTag::single(2)),
                Some(RangeTag::single(3)),
                Some(RangeTag::single(99)),
            ]
        );
    }

    #[test]
    fn rebuild_equals_the_incremental_build() {
        // The bit-identical property, in-process form: the same data
        // appended incrementally and rebuilt from persisted rows produce
        // equal contexts. The caller builds `reply_targets` with the same
        // store function used at intake (Rule P1), so rebuild ==
        // incremental. The rows carry a username, a resolved reply
        // target, and an edit row.
        let reply_targets = HashMap::from([("p1".to_string(), target(1, "Alice"))]);

        let mut live = LiveContext::new("P".to_string());
        live.append_human_message(
            1,
            "Alice",
            Some("alice_tg"),
            at_1307(),
            false,
            false,
            ReplyRender::None,
            "one",
        );
        live.append_recall_injection(1, "memory at 1".to_string());
        live.append_bot_speech(2, at_1307(), "two");
        live.append_recall_injection(2, "memory at 2".to_string());
        live.append_human_message(
            3,
            "Bob",
            None,
            at_1307(),
            false,
            false,
            ReplyRender::ToUser {
                target: Some(target(1, "Alice")),
            },
            "reply to one",
        );
        live.append_human_message(
            4,
            "Alice",
            Some("alice_tg"),
            at_1307(),
            true,
            false,
            ReplyRender::None,
            "edited",
        );

        let mut reply_row = row(
            3,
            Direction::Inbound,
            EventType::Message,
            "Bob",
            None,
            "reply to one",
        );
        reply_row.reply_to_platform_msg_id = Some("p1".to_string());
        let rows = vec![
            row(
                1,
                Direction::Inbound,
                EventType::Message,
                "Alice",
                Some("alice_tg"),
                "one",
            ),
            row(
                2,
                Direction::Outbound,
                EventType::Message,
                "Tamako",
                None,
                "two",
            ),
            reply_row,
            row(
                4,
                Direction::Inbound,
                EventType::Edit,
                "Alice",
                Some("alice_tg"),
                "edited",
            ),
        ];
        let injections = vec![
            injection(10, 1, "memory at 1"),
            injection(11, 2, "memory at 2"),
        ];
        let rebuilt =
            LiveContext::rebuild("P".to_string(), &rows, &injections, &reply_targets, &[]);

        assert_eq!(rebuilt, live);
    }

    #[test]
    fn rebuild_collapses_multi_edge_rows_into_the_one_live_item() {
        // Decision 65 bit-identity fix: the live wake path appends ONE
        // RecallInjection item per PlannedInjection, while
        // `injected_memories` persists ONE ROW PER EDGE — the N edges of
        // one multi-edge injection persist N consecutive rows with the
        // same (injection_position, content). The rebuild must collapse
        // that run into one item, or the post-restart context carries
        // duplicate `<memory>` items (the Rule P1 violation this test
        // would have caught).
        let mut live = LiveContext::new("P".to_string());
        live.append_human_message(
            1,
            "Alice",
            None,
            at_1307(),
            false,
            false,
            ReplyRender::None,
            "tea?",
        );
        live.append_bot_speech(2, at_1307(), "always");
        // One multi-edge injection: ONE item in the live context.
        live.append_recall_injection(2, "<memory>Alice likes tea</memory>".to_string());

        let rows = vec![
            row(
                1,
                Direction::Inbound,
                EventType::Message,
                "Alice",
                None,
                "tea?",
            ),
            row(
                2,
                Direction::Outbound,
                EventType::Message,
                "Tamako",
                None,
                "always",
            ),
        ];
        // The persisted form: one row PER EDGE of the injection, the
        // same position and content, consecutive row ids (the per-edge
        // insert loop of the wake completion handler).
        let injections = vec![
            injection(10, 2, "<memory>Alice likes tea</memory>"),
            injection(11, 2, "<memory>Alice likes tea</memory>"),
            injection(12, 2, "<memory>Alice likes tea</memory>"),
        ];
        let rebuilt =
            LiveContext::rebuild("P".to_string(), &rows, &injections, &HashMap::new(), &[]);

        assert_eq!(rebuilt, live);
    }

    #[test]
    fn rebuild_collapses_a_multi_edge_run_beyond_the_tail() {
        // The leftover path (the position matches no row id) collapses
        // the same way: the live append would have pushed one item at
        // the tail.
        let rows = vec![row(
            1,
            Direction::Inbound,
            EventType::Message,
            "Alice",
            None,
            "one",
        )];
        let injections = vec![
            injection(10, 99, "leftover memory"),
            injection(11, 99, "leftover memory"),
        ];
        let context =
            LiveContext::rebuild("P".to_string(), &rows, &injections, &HashMap::new(), &[]);

        let kinds: Vec<(ContextItemKind, String)> = context
            .items()
            .iter()
            .map(|item| (item.kind.clone(), item.content.clone()))
            .collect();
        assert_eq!(
            kinds,
            vec![
                (ContextItemKind::Preamble, "P".to_string()),
                (
                    ContextItemKind::HumanMessage,
                    r#"<msg from="Alice" at="13:07" id="1">one</msg>"#.to_string()
                ),
                (
                    ContextItemKind::RecallInjection,
                    "leftover memory".to_string()
                ),
            ]
        );
    }

    #[test]
    fn rebuild_does_not_collapse_distinct_or_non_consecutive_injections() {
        // Only true runs collapse. Same position but different content,
        // same content but not consecutive, and same content at a
        // different position are genuinely distinct injections: every
        // one renders its own item.
        let rows = vec![
            row(
                1,
                Direction::Inbound,
                EventType::Message,
                "Alice",
                None,
                "one",
            ),
            row(
                2,
                Direction::Outbound,
                EventType::Message,
                "Tamako",
                None,
                "two",
            ),
            row(
                3,
                Direction::Inbound,
                EventType::Message,
                "Alice",
                None,
                "three",
            ),
        ];
        let injections = vec![
            injection(10, 2, "memory X"),
            // Same position, different content: no collapse.
            injection(11, 2, "memory Y"),
            // Same content as row 10 but NOT consecutive: no collapse.
            injection(12, 2, "memory X"),
            // Same content, different position: no collapse.
            injection(13, 3, "memory X"),
        ];
        let context =
            LiveContext::rebuild("P".to_string(), &rows, &injections, &HashMap::new(), &[]);

        let rendered: Vec<(ContextItemKind, String, Option<RangeTag>)> = context
            .items()
            .iter()
            .map(|item| {
                (
                    item.kind.clone(),
                    item.content.clone(),
                    item.range_tag.clone(),
                )
            })
            .collect();
        assert_eq!(
            rendered,
            vec![
                (ContextItemKind::Preamble, "P".to_string(), None),
                (
                    ContextItemKind::HumanMessage,
                    r#"<msg from="Alice" at="13:07" id="1">one</msg>"#.to_string(),
                    Some(RangeTag::single(1))
                ),
                (
                    ContextItemKind::BotSpeech,
                    r#"<you at="13:07" id="2">two</you>"#.to_string(),
                    Some(RangeTag::single(2))
                ),
                (
                    ContextItemKind::RecallInjection,
                    "memory X".to_string(),
                    Some(RangeTag::single(2))
                ),
                (
                    ContextItemKind::RecallInjection,
                    "memory Y".to_string(),
                    Some(RangeTag::single(2))
                ),
                (
                    ContextItemKind::RecallInjection,
                    "memory X".to_string(),
                    Some(RangeTag::single(2))
                ),
                (
                    ContextItemKind::HumanMessage,
                    r#"<msg from="Alice" at="13:07" id="3">three</msg>"#.to_string(),
                    Some(RangeTag::single(3))
                ),
                (
                    ContextItemKind::RecallInjection,
                    "memory X".to_string(),
                    Some(RangeTag::single(3))
                ),
            ]
        );
    }

    #[test]
    fn rebuild_keeps_a_single_edge_injection_unchanged() {
        // The legacy one-row-per-injection data is unaffected: one row
        // still renders exactly one item (decision 65 changes nothing
        // for single-edge groups).
        let rows = vec![row(
            1,
            Direction::Inbound,
            EventType::Message,
            "Alice",
            None,
            "one",
        )];
        let injections = vec![injection(10, 1, "I remember: Alice likes tea.")];
        let context =
            LiveContext::rebuild("P".to_string(), &rows, &injections, &HashMap::new(), &[]);

        assert_eq!(context.items().len(), 3);
        let injection = &context.items()[2];
        assert_eq!(injection.kind, ContextItemKind::RecallInjection);
        assert_eq!(injection.content, "I remember: Alice likes tea.");
        assert_eq!(injection.range_tag, Some(RangeTag::single(1)));
    }

    #[test]
    fn rebuild_resolves_reply_targets_username_and_edit_kind() {
        // The rebuild renders the resolved reply target, the username,
        // and the edit kind from the persisted rows.
        let mut reply_row = row(
            2,
            Direction::Inbound,
            EventType::Message,
            "Bob",
            Some("bob_tg"),
            "hotpot?",
        );
        reply_row.reply_to_platform_msg_id = Some("p1".to_string());
        let rows = vec![
            row(
                1,
                Direction::Inbound,
                EventType::Message,
                "Alice",
                None,
                "any plans?",
            ),
            reply_row,
            row(
                3,
                Direction::Inbound,
                EventType::Edit,
                "Carol",
                None,
                "edited text",
            ),
        ];
        let reply_targets = HashMap::from([("p1".to_string(), target(1, "Alice"))]);

        let context = LiveContext::rebuild("P".to_string(), &rows, &[], &reply_targets, &[]);

        assert_eq!(
            context.items()[1].content,
            r#"<msg from="Alice" at="13:07" id="1">any plans?</msg>"#
        );
        assert_eq!(
            context.items()[2].content,
            r#"<msg from="Bob" user="bob_tg" at="13:07" id="2" reply="user" reply_to_name="Alice" reply_to_id="1">hotpot?</msg>"#
        );
        assert_eq!(
            context.items()[3].content,
            r#"<msg from="Carol" at="13:07" id="3" kind="edit">edited text</msg>"#
        );
    }

    #[test]
    fn rebuild_renders_unresolved_and_bot_replies_deterministically() {
        // An unresolved user reply renders `reply="user"` with no target
        // attributes; a reply to the bot renders `reply="bot"` with no
        // target attributes (Rules A3/B1: the synthetic outbound id can
        // never resolve). Both deterministic.
        let mut unresolved = row(
            1,
            Direction::Inbound,
            EventType::Message,
            "Dave",
            None,
            "old stuff",
        );
        unresolved.reply_to_platform_msg_id = Some("p-missing".to_string());
        let mut to_bot = row(
            2,
            Direction::Inbound,
            EventType::Message,
            "Carol",
            None,
            "you pick!",
        );
        to_bot.is_reply_to_bot = true;
        let rows = vec![unresolved, to_bot];

        let context = LiveContext::rebuild("P".to_string(), &rows, &[], &HashMap::new(), &[]);

        assert_eq!(
            context.items()[1].content,
            r#"<msg from="Dave" at="13:07" id="1" reply="user">old stuff</msg>"#
        );
        assert_eq!(
            context.items()[2].content,
            r#"<msg from="Carol" at="13:07" id="2" reply="bot">you pick!</msg>"#
        );
    }

    #[test]
    fn render_human_content_matches_the_contract_examples_byte_exactly() {
        // The approved XML grammar, byte-exact. Attribute order fixed.
        assert_eq!(
            render_human_content(
                41,
                "Alice",
                None,
                at_utc(13, 5),
                false,
                false,
                ReplyRender::None,
                "any plans for dinner?",
            ),
            r#"<msg from="Alice" at="13:05" id="41">any plans for dinner?</msg>"#
        );
        assert_eq!(
            render_human_content(
                42,
                "Bob",
                Some("bob_tg"),
                at_utc(13, 6),
                false,
                false,
                ReplyRender::ToUser {
                    target: Some(target(41, "Alice")),
                },
                "hotpot?",
            ),
            r#"<msg from="Bob" user="bob_tg" at="13:06" id="42" reply="user" reply_to_name="Alice" reply_to_id="41">hotpot?</msg>"#
        );
        assert_eq!(
            render_human_content(
                43,
                "Carol",
                None,
                at_utc(13, 7),
                false,
                false,
                ReplyRender::ToBot,
                "you pick!",
            ),
            r#"<msg from="Carol" at="13:07" id="43" reply="bot">you pick!</msg>"#
        );
        assert_eq!(
            render_human_content(
                44,
                "Dave",
                None,
                at_utc(13, 8),
                false,
                false,
                ReplyRender::ToUser { target: None },
                "old stuff",
            ),
            r#"<msg from="Dave" at="13:08" id="44" reply="user">old stuff</msg>"#
        );
        assert_eq!(
            render_human_content(
                45,
                "Dave",
                None,
                at_utc(13, 8),
                false,
                true,
                ReplyRender::None,
                "@tamako hi",
            ),
            r#"<msg from="Dave" at="13:08" id="45" mention="bot">@tamako hi</msg>"#
        );
        assert_eq!(
            render_human_content(
                46,
                "Alice",
                None,
                at_utc(13, 9),
                true,
                false,
                ReplyRender::None,
                "dinner at 7 (edited)",
            ),
            r#"<msg from="Alice" at="13:09" id="46" kind="edit">dinner at 7 (edited)</msg>"#
        );
        // Escaping: the display name is attr-escaped, the text is
        // text-escaped.
        assert_eq!(
            render_human_content(
                47,
                r#"Ann "Annie" & Co"#,
                None,
                at_utc(13, 10),
                false,
                false,
                ReplyRender::None,
                r#"a < b & "c""#,
            ),
            r#"<msg from="Ann &quot;Annie&quot; &amp; Co" at="13:10" id="47">a &lt; b &amp; "c"</msg>"#
        );
    }

    #[test]
    fn render_human_content_flag_precedence_is_kind_then_reply_then_mention() {
        // A message can be an edit, a reply, and a mention at once: all
        // three attributes render, in that fixed order.
        assert_eq!(
            render_human_content(
                48,
                "Eve",
                None,
                at_utc(13, 11),
                true,
                true,
                ReplyRender::ToBot,
                "edited reply mention",
            ),
            r#"<msg from="Eve" at="13:11" id="48" kind="edit" reply="bot" mention="bot">edited reply mention</msg>"#
        );
    }

    #[test]
    fn render_bot_content_wraps_in_the_you_element_with_escaping() {
        assert_eq!(
            render_bot_content(49, at_utc(13, 12), r#"hi & <bye>"#),
            r#"<you at="13:12" id="49">hi &amp; &lt;bye&gt;</you>"#
        );
        assert_eq!(
            render_bot_content(50, at_utc(9, 5), "plain"),
            r#"<you at="09:05" id="50">plain</you>"#
        );
    }

    #[test]
    fn messages_for_llm_maps_roles_and_order() {
        let mut context = LiveContext::new("P".to_string());
        context.append_human_message(
            1,
            "Alice",
            None,
            at_1307(),
            false,
            false,
            ReplyRender::None,
            "hello",
        );
        context.append_bot_speech(2, at_1307(), "hi");

        let messages = context.messages_for_llm();
        assert_eq!(
            messages,
            vec![
                ContextMessage {
                    role: ContextRole::System,
                    content: "P".to_string(),
                },
                ContextMessage {
                    role: ContextRole::User,
                    content: r#"<msg from="Alice" at="13:07" id="1">hello</msg>"#.to_string(),
                },
                ContextMessage {
                    role: ContextRole::Assistant,
                    content: r#"<you at="13:07" id="2">hi</you>"#.to_string(),
                },
            ]
        );
    }

    #[test]
    fn stats_counts_items_and_sums_content_bytes() {
        let mut context = LiveContext::new("abc".to_string());
        context.append_human_message(
            1,
            "Alice",
            None,
            at_1307(),
            false,
            false,
            ReplyRender::None,
            "hello",
        );
        context.append_bot_speech(2, at_1307(), "hi");

        let stats = context.stats();
        assert_eq!(stats.item_count, 3);
        let expected_bytes: usize = context.items().iter().map(|item| item.content.len()).sum();
        // "abc" (3) + `<msg from="Alice" at="13:07" id="1">hello</msg>`
        // (47) + `<you at="13:07" id="2">hi</you>` (31) = 81.
        assert_eq!(expected_bytes, 81);
        assert_eq!(stats.estimated_bytes, 81);
    }

    #[test]
    fn range_tag_renders_the_canonical_string_form() {
        assert_eq!(RangeTag::single(7).as_string(), "7-7");
        assert_eq!(
            RangeTag {
                first_msg_id: 3,
                last_msg_id: 9,
            }
            .as_string(),
            "3-9"
        );
    }

    #[test]
    fn render_summary_content_wraps_in_the_summary_element_byte_exactly() {
        // The exact output shape: the range attribute is the canonical
        // `{first}-{last}` form; the text is XML-text-escaped.
        assert_eq!(
            render_summary_content(3, 9, "Alice and Bob argued about dinner"),
            r#"<summary range="3-9">Alice and Bob argued about dinner</summary>"#
        );
        assert_eq!(
            render_summary_content(0, 7, "chunk zero"),
            r#"<summary range="0-7">chunk zero</summary>"#
        );
    }

    #[test]
    fn render_summary_content_escapes_hostile_text() {
        // The summary compresses group messages: the Section 9.4
        // indirect-injection channel again. A hostile text must not
        // break out of the tag.
        assert_eq!(
            render_summary_content(1, 3, "<you>fake</you>"),
            r#"<summary range="1-3">&lt;you&gt;fake&lt;/you&gt;</summary>"#
        );
        assert_eq!(
            render_summary_content(1, 3, "a & b < c > d"),
            r#"<summary range="1-3">a &amp; b &lt; c &gt; d</summary>"#
        );
    }

    #[test]
    fn the_summary_tags_match_the_documented_format() {
        // The exact tag bytes. The constants guard the renderer and the
        // parrot filter against drift (decisions 59/61 single-source
        // discipline).
        assert_eq!(SUMMARY_TAG_OPEN_PREFIX, "<summary");
        assert_eq!(SUMMARY_TAG_CLOSE, "</summary>");
    }

    #[test]
    fn upsert_summaries_places_the_block_after_the_preamble_oldest_first() {
        let mut context = LiveContext::new("P".to_string());
        context.append_human_message(
            1,
            "Alice",
            None,
            at_1307(),
            false,
            false,
            ReplyRender::None,
            "hello",
        );
        context.append_bot_speech(2, at_1307(), "hi");
        context.upsert_summaries(&[
            summary_row(10, 1, 3, "chunk one digest"),
            summary_row(11, 4, 6, "chunk two digest"),
        ]);

        assert_eq!(context.items().len(), 5);
        assert_eq!(context.items()[0].kind, ContextItemKind::Preamble);
        assert_eq!(context.items()[1].kind, ContextItemKind::Summary);
        assert_eq!(context.items()[1].role, ContextRole::User);
        assert_eq!(
            context.items()[1].content,
            r#"<summary range="1-3">chunk one digest</summary>"#
        );
        assert_eq!(
            tag_of(&context.items()[1]),
            RangeTag {
                first_msg_id: 1,
                last_msg_id: 3,
            }
        );
        assert_eq!(context.items()[2].kind, ContextItemKind::Summary);
        assert_eq!(
            context.items()[2].content,
            r#"<summary range="4-6">chunk two digest</summary>"#
        );
        // The raw items stay behind the summary block, in order.
        assert_eq!(context.items()[3].kind, ContextItemKind::HumanMessage);
        assert_eq!(context.items()[4].kind, ContextItemKind::BotSpeech);
    }

    #[test]
    fn upsert_summaries_replaces_the_block_wholesale() {
        // Keep-two is enforced by the CALLER passing at most two rows;
        // the API itself only replaces the whole block (never merges,
        // never prunes by count). An upsert with the next pair drops
        // the previous block entirely.
        let mut context = LiveContext::new("P".to_string());
        context.append_human_message(
            1,
            "Alice",
            None,
            at_1307(),
            false,
            false,
            ReplyRender::None,
            "hello",
        );
        context.upsert_summaries(&[
            summary_row(10, 1, 3, "old A"),
            summary_row(11, 4, 6, "old B"),
        ]);
        context.upsert_summaries(&[
            summary_row(11, 4, 6, "old B"),
            summary_row(12, 7, 9, "new C"),
        ]);

        assert_eq!(context.items().len(), 4);
        let kinds: Vec<ContextItemKind> = context
            .items()
            .iter()
            .map(|item| item.kind.clone())
            .collect();
        assert_eq!(
            kinds,
            vec![
                ContextItemKind::Preamble,
                ContextItemKind::Summary,
                ContextItemKind::Summary,
                ContextItemKind::HumanMessage,
            ]
        );
        assert_eq!(
            context.items()[1].content,
            r#"<summary range="4-6">old B</summary>"#
        );
        assert_eq!(
            context.items()[2].content,
            r#"<summary range="7-9">new C</summary>"#
        );

        // An empty upsert clears the block; the raw items stay.
        context.upsert_summaries(&[]);
        assert_eq!(context.items().len(), 2);
        assert_eq!(context.items()[0].kind, ContextItemKind::Preamble);
        assert_eq!(context.items()[1].kind, ContextItemKind::HumanMessage);
    }

    #[test]
    fn summary_items_are_exempt_from_remove_at_or_below() {
        // C3 amendment: summary retention is count-based keep-two,
        // driven by upsert_summaries, never by the C3 cutoff. A summary
        // whose last_msg_id is at or below the boundary SURVIVES while
        // the neighboring raw items are removed.
        let mut context = LiveContext::new("P".to_string());
        context.upsert_summaries(&[summary_row(10, 1, 3, "digested chunk")]);
        context.append_human_message(
            1,
            "Alice",
            None,
            at_1307(),
            false,
            false,
            ReplyRender::None,
            "one",
        );
        context.append_bot_speech(2, at_1307(), "two");
        context.append_human_message(
            4,
            "Bob",
            None,
            at_1307(),
            false,
            false,
            ReplyRender::None,
            "four",
        );

        context.remove_at_or_below(3);

        assert_eq!(context.items().len(), 3);
        assert_eq!(context.items()[0].kind, ContextItemKind::Preamble);
        // The summary has last_msg_id 3 <= 3 and survives the cutoff.
        assert_eq!(context.items()[1].kind, ContextItemKind::Summary);
        assert_eq!(
            context.items()[1].content,
            r#"<summary range="1-3">digested chunk</summary>"#
        );
        // The raw items at or below 3 are gone; the row above stays.
        assert_eq!(tag_of(&context.items()[2]), RangeTag::single(4));
    }

    #[test]
    fn rebuild_with_summaries_equals_the_incremental_build() {
        // The bit-identity property with the summary block (Rule P1):
        // the live path (upsert at digest completion, then appends) and
        // the rebuild path (upsert first, then the same rows) consume
        // the same store rows and produce equal contexts.
        let summaries = [
            summary_row(10, 1, 3, "chunk one"),
            summary_row(11, 4, 6, "chunk two"),
        ];
        let rows = vec![
            row(
                7,
                Direction::Inbound,
                EventType::Message,
                "Alice",
                Some("alice_tg"),
                "seven",
            ),
            row(
                8,
                Direction::Outbound,
                EventType::Message,
                "Tamako",
                None,
                "eight",
            ),
        ];
        let injections = vec![injection(20, 7, "memory at 7")];

        let mut live = LiveContext::new("P".to_string());
        live.upsert_summaries(&summaries);
        live.append_human_message(
            7,
            "Alice",
            Some("alice_tg"),
            at_1307(),
            false,
            false,
            ReplyRender::None,
            "seven",
        );
        live.append_recall_injection(7, "memory at 7".to_string());
        live.append_bot_speech(8, at_1307(), "eight");

        let rebuilt = LiveContext::rebuild(
            "P".to_string(),
            &rows,
            &injections,
            &HashMap::new(),
            &summaries,
        );

        assert_eq!(rebuilt, live);
        // Placement, explicitly: preamble, the two summaries (oldest
        // first), then the raw items and the injection.
        let kinds: Vec<ContextItemKind> = rebuilt
            .items()
            .iter()
            .map(|item| item.kind.clone())
            .collect();
        assert_eq!(
            kinds,
            vec![
                ContextItemKind::Preamble,
                ContextItemKind::Summary,
                ContextItemKind::Summary,
                ContextItemKind::HumanMessage,
                ContextItemKind::RecallInjection,
                ContextItemKind::BotSpeech,
            ]
        );
    }

    #[test]
    fn messages_for_llm_maps_summary_items_to_the_user_role() {
        // A summary is compressed history: reference data, User role
        // (never Assistant — the reply model must not learn to speak
        // `<summary>` blocks).
        let mut context = LiveContext::new("P".to_string());
        context.upsert_summaries(&[summary_row(10, 1, 3, "digested")]);
        context.append_bot_speech(4, at_1307(), "hi");

        let messages = context.messages_for_llm();
        assert_eq!(
            messages,
            vec![
                ContextMessage {
                    role: ContextRole::System,
                    content: "P".to_string(),
                },
                ContextMessage {
                    role: ContextRole::User,
                    content: r#"<summary range="1-3">digested</summary>"#.to_string(),
                },
                ContextMessage {
                    role: ContextRole::Assistant,
                    content: r#"<you at="13:07" id="4">hi</you>"#.to_string(),
                },
            ]
        );
    }
}
