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
//! digest-time removal, which drops a closed range at or below a boundary)
//! and `reload_preamble` (the Rule C4 item-0 replacement).
//!
//! This crate is model-agnostic. `ContextMessage` is the LLM-facing view;
//! tamako-agent converts it to rig completion messages in M4.

use std::collections::BTreeMap;

use time::macros::format_description;
use time::{OffsetDateTime, UtcOffset};

use tamako_store::{Direction, InjectedMemoryRow, MessageRow};

/// The UTC HH:MM format of the speaker label (specs.md Section 7.2 step 4).
const HHMM_FORMAT: &[time::format_description::FormatItem<'_>] =
    format_description!("[hour]:[minute]");

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
    /// HumanMessage. The content carries the speaker label of specs.md
    /// Section 7.2 step 4: `[{display_name} {HH:MM}] {text}`, HH:MM in UTC.
    pub fn append_human_message(
        &mut self,
        msg_id: i64,
        display_name: &str,
        timestamp: OffsetDateTime,
        text: &str,
    ) {
        self.items.push(ContextItem {
            kind: ContextItemKind::HumanMessage,
            role: ContextRole::User,
            content: render_human_content(display_name, timestamp, text),
            range_tag: Some(RangeTag::single(msg_id)),
        });
    }

    /// Appends one message of the bot at the tail (Rule C1). Rule B1: the
    /// bot's own speech is part of the raw log. The model sees its own
    /// speech as plain assistant text; the speaker label distinguishes
    /// group members, not the bot. This is the M4 append API for the
    /// bot's own sent messages.
    pub fn append_bot_speech(&mut self, msg_id: i64, text: &str) {
        self.items.push(ContextItem {
            kind: ContextItemKind::BotSpeech,
            role: ContextRole::Assistant,
            content: text.to_string(),
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

    /// Rule C3: removes every item with a range tag at or below
    /// `boundary_msg_id`. The preamble is never removed.
    ///
    /// One-chunk-lag semantics (specs.md Section 7.1): the actor calls
    /// this with the PREVIOUS boundary B_old when the digest boundary
    /// advances B_old -> B_new. The chunk just digested, (B_old, B_new],
    /// stays one more chunk as the overlap buffer.
    pub fn remove_at_or_below(&mut self, boundary_msg_id: i64) {
        self.items.retain(|item| match &item.range_tag {
            None => true,
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
    /// dedup rows with injection_position above the cutoff, ordered by id.
    ///
    /// Both event types Message and Edit render identically: an edit is
    /// just a new log row (specs.md Section 15, open item 4). Injections
    /// land directly after the row whose id equals `injection_position`
    /// (Rule C2), content verbatim from the persisted row. Leftover
    /// injections (position matches no row id, e.g. a position beyond the
    /// current tail) are appended at the tail in injection-row order.
    pub fn rebuild(
        preamble: String,
        rows: &[MessageRow],
        injections: &[InjectedMemoryRow],
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
        for row in rows {
            match row.direction {
                Direction::Inbound => context.append_human_message(
                    row.id,
                    &row.sender_display_name,
                    row.timestamp,
                    &row.text,
                ),
                Direction::Outbound => context.append_bot_speech(row.id, &row.text),
            }
            if let Some(here) = injections_by_position.remove(&row.id) {
                for injection in here {
                    context.append_recall_injection(
                        injection.injection_position,
                        injection.content.clone(),
                    );
                }
            }
        }
        // Leftover injections: the position matches no row id. Append them
        // at the tail in injection-row order.
        for (_, here) in injections_by_position {
            for injection in here {
                context.append_recall_injection(
                    injection.injection_position,
                    injection.content.clone(),
                );
            }
        }
        context
    }
}

/// Renders the speaker label of specs.md Section 7.2 step 4. This one
/// helper serves `append_human_message`, `rebuild`, and the M4 gate
/// input (`wake::GateMessage::content`): one render helper keeps the
/// gate input consistent with the live context, and makes the rebuild
/// bit-identical. On a format failure the HH:MM part falls back to
/// "??:??" (the pattern of tamako-agent/src/pipeline.rs).
pub fn render_human_content(display_name: &str, timestamp: OffsetDateTime, text: &str) -> String {
    let hhmm = timestamp
        .to_offset(UtcOffset::UTC)
        .format(HHMM_FORMAT)
        .unwrap_or_else(|_| "??:??".to_string());
    format!("[{display_name} {hhmm}] {text}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tamako_store::EventType;
    use time::macros::datetime;

    fn at_1307() -> OffsetDateTime {
        datetime!(2026-08-07 13:07 UTC)
    }

    fn tag_of(item: &ContextItem) -> RangeTag {
        item.range_tag.clone().expect("a range tag")
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
        context.append_human_message(1, "Alice", at_1307(), "hello");
        context.append_bot_speech(2, "hi there");
        context.append_recall_injection(2, "I remember: Alice likes tea.".to_string());

        assert_eq!(context.items().len(), 4);
        let human = &context.items()[1];
        assert_eq!(human.kind, ContextItemKind::HumanMessage);
        assert_eq!(human.role, ContextRole::User);
        assert_eq!(human.content, "[Alice 13:07] hello");
        assert_eq!(tag_of(human), RangeTag::single(1));

        let speech = &context.items()[2];
        assert_eq!(speech.kind, ContextItemKind::BotSpeech);
        assert_eq!(speech.role, ContextRole::Assistant);
        assert_eq!(speech.content, "hi there");
        assert_eq!(tag_of(speech), RangeTag::single(2));

        let injection = &context.items()[3];
        assert_eq!(injection.kind, ContextItemKind::RecallInjection);
        assert_eq!(injection.role, ContextRole::Assistant);
        assert_eq!(injection.content, "I remember: Alice likes tea.");
        assert_eq!(tag_of(injection), RangeTag::single(2));
    }

    #[test]
    fn lag_one_semantics_across_two_digests() {
        let mut context = LiveContext::new("P".to_string());
        for msg_id in 1..=3 {
            context.append_human_message(msg_id, "Alice", at_1307(), "chunk one");
        }
        // Digest 1: boundary advances 0 -> 3. The actor removes at or
        // below the PREVIOUS boundary (0). Nothing is removed.
        context.remove_at_or_below(0);
        assert_eq!(context.items().len(), 4);

        for msg_id in 4..=6 {
            context.append_human_message(msg_id, "Alice", at_1307(), "chunk two");
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
        context.append_human_message(1, "Alice", at_1307(), "hi");
        context.append_bot_speech(2, "hello");
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
        context.append_human_message(5, "Alice", at_1307(), "at five");
        context.append_human_message(6, "Alice", at_1307(), "at six");

        context.remove_at_or_below(5);

        // Tag equality means removed; a tag above the boundary is kept.
        assert_eq!(context.items().len(), 2);
        assert_eq!(tag_of(&context.items()[1]), RangeTag::single(6));
    }

    #[test]
    fn reload_preamble_replaces_item_zero_only() {
        let mut context = LiveContext::new("old preamble".to_string());
        context.append_human_message(1, "Alice", at_1307(), "hello");
        context.append_bot_speech(2, "hi");
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

    #[test]
    fn rebuild_places_items_and_injections_in_order() {
        let rows = vec![
            row(1, Direction::Inbound, EventType::Message, "Alice", "one"),
            row(2, Direction::Outbound, EventType::Message, "Tamako", "two"),
            row(
                3,
                Direction::Inbound,
                EventType::Edit,
                "Alice",
                "three (edited)",
            ),
        ];
        let injections = vec![
            injection(10, 2, "memory A at 2"),
            injection(11, 2, "memory B at 2"),
            injection(12, 1, "memory at 1"),
            injection(13, 99, "memory beyond the tail"),
        ];

        let context = LiveContext::rebuild("P".to_string(), &rows, &injections);

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
                    "[Alice 13:07] one".to_string()
                ),
                // Injection order at one position is stable (row order).
                (ContextItemKind::RecallInjection, "memory at 1".to_string()),
                (ContextItemKind::BotSpeech, "two".to_string()),
                (
                    ContextItemKind::RecallInjection,
                    "memory A at 2".to_string()
                ),
                (
                    ContextItemKind::RecallInjection,
                    "memory B at 2".to_string()
                ),
                // An edit row renders identically to a message row.
                (
                    ContextItemKind::HumanMessage,
                    "[Alice 13:07] three (edited)".to_string()
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
        // equal contexts.
        let mut live = LiveContext::new("P".to_string());
        live.append_human_message(1, "Alice", at_1307(), "one");
        live.append_recall_injection(1, "memory at 1".to_string());
        live.append_bot_speech(2, "two");
        live.append_recall_injection(2, "memory at 2".to_string());
        live.append_human_message(3, "Alice", at_1307(), "three");

        let rows = vec![
            row(1, Direction::Inbound, EventType::Message, "Alice", "one"),
            row(2, Direction::Outbound, EventType::Message, "Tamako", "two"),
            row(3, Direction::Inbound, EventType::Message, "Alice", "three"),
        ];
        let injections = vec![
            injection(10, 1, "memory at 1"),
            injection(11, 2, "memory at 2"),
        ];
        let rebuilt = LiveContext::rebuild("P".to_string(), &rows, &injections);

        assert_eq!(rebuilt, live);
    }

    #[test]
    fn messages_for_llm_maps_roles_and_order() {
        let mut context = LiveContext::new("P".to_string());
        context.append_human_message(1, "Alice", at_1307(), "hello");
        context.append_bot_speech(2, "hi");

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
                    content: "[Alice 13:07] hello".to_string(),
                },
                ContextMessage {
                    role: ContextRole::Assistant,
                    content: "hi".to_string(),
                },
            ]
        );
    }

    #[test]
    fn stats_counts_items_and_sums_content_bytes() {
        let mut context = LiveContext::new("abc".to_string());
        context.append_human_message(1, "Alice", at_1307(), "hello");
        context.append_bot_speech(2, "hi");

        let stats = context.stats();
        assert_eq!(stats.item_count, 3);
        let expected_bytes: usize = context.items().iter().map(|item| item.content.len()).sum();
        // "abc" (3) + "[Alice 13:07] hello" (19) + "hi" (2) = 24.
        assert_eq!(expected_bytes, 24);
        assert_eq!(stats.estimated_bytes, 24);
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
}
