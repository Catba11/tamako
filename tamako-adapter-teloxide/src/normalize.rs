//! Pure normalization from teloxide types to `tamako_core::event` types.
//!
//! Rule A1: no teloxide type leaves this module. Every function here is
//! pure: no async, no I/O. The live adapter (a later milestone) calls these
//! functions and wraps the results in `InboundEvent` values.
//!
//! Normalization decisions (specs.md Section 4.2):
//!
//! - Display-name fallback chain: "First Last" -> "@username" -> the
//!   numeric id.
//! - The `username` field of the normalized message carries
//!   `User.username` directly. It is `None` when the sender has no
//!   username, and always `None` for anonymous chat senders.
//! - Anonymous group admins send as the chat (`from: null`,
//!   `sender_chat` set). They get the synthetic sender id `chat:{id}` and
//!   the chat title as the display name. The same synthetic form is used
//!   for anonymous reaction actors.
//! - Emoji mapping: `ReactionType::Emoji` carries the emoji string.
//!   `ReactionType::CustomEmoji` carries its `custom_emoji_id` string.
//!   `ReactionType::Paid` carries no identity and is skipped.
//! - Entity offsets: Telegram uses UTF-16 code units. `Message::parse_entities`
//!   converts them to UTF-8 byte offsets (verified in the teloxide-core
//!   0.13.0 source, `message_entity.rs`). We use that helper, never raw
//!   offsets.
//! - Timestamps: teloxide uses chrono `DateTime<Utc>`. We convert through
//!   the unix timestamp to `time::OffsetDateTime` (the project standard,
//!   AGENT.md: the project uses the time crate, not chrono). Edited
//!   messages carry their EDIT date: the adapter routes them through
//!   `normalize_edited_message`, not `normalize_message`. See the cutover
//!   note on that function.

use tamako_core::event::{InboundEvent, MemberEvent, NormalizedMessage, ReactionEvent};
use teloxide::types::{
    Chat, Me, Message, MessageEntityKind, MessageReactionCountUpdated, MessageReactionUpdated,
    ReactionType, User,
};
use time::OffsetDateTime;

/// The bot's own identity, from `get_me` at startup. Needed for mention
/// and reply resolution at intake (specs.md Section 4.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BotIdentity {
    pub id: u64,
    pub username: String,
}

impl BotIdentity {
    /// Builds the identity from the `get_me` response. `Me::username`
    /// panics when the bot has no username; bots always have one, so this
    /// is expect-valid.
    pub fn from_me(me: &Me) -> Self {
        Self {
            id: me.user.id.0,
            username: me.username().to_string(),
        }
    }
}

/// Display-name fallback chain: "First Last" -> "@username" -> the numeric
/// id. A first name alone does not count: many users share a first name, so
/// it is too weak as an identity hint (specs.md Section 4.2).
pub fn display_name(user: &User) -> String {
    if user.last_name.is_some() {
        user.full_name()
    } else if let Some(username) = &user.username {
        format!("@{username}")
    } else {
        user.id.0.to_string()
    }
}

/// The string form of a chat id. Telegram group ids are negative; the minus
/// sign is kept.
pub fn chat_id_string(chat: &Chat) -> String {
    chat.id.0.to_string()
}

/// A text message -> `NormalizedMessage`. `None` for messages with no text
/// (photos, stickers, ...) and for service messages (`normalize_service`
/// handles those).
///
/// Sender resolution: `msg.from` gives the user id and the display name.
/// When `from` is `None` and the message was sent as the chat
/// (`sender_chat`, anonymous group admins), the sender id is the synthetic
/// `chat:{id}` form and the display name is the chat title (fallback: the
/// chat username, then the id). This is a documented, deliberate mapping:
/// the platform gives no user identity for these messages.
///
/// Note: edited messages do NOT use this function. The adapter routes
/// `UpdateKind::EditedMessage` through `normalize_edited_message`, which
/// stamps the edit date into the timestamp slot.
pub fn normalize_message(msg: &Message, bot: &BotIdentity) -> Option<NormalizedMessage> {
    let text = msg.text()?;

    let (sender_id, sender_display_name, username) = resolve_sender(msg);
    let reply = msg.reply_to_message();

    Some(NormalizedMessage {
        platform_msg_id: msg.id.0.to_string(),
        timestamp: unix_to_offset(msg.date.timestamp()),
        sender_id,
        sender_display_name,
        username,
        text: text.to_string(),
        reply_to_platform_msg_id: reply.map(|m| m.id.0.to_string()),
        mentions_bot: mentions_bot(msg, bot),
        is_reply_to_bot: reply
            .and_then(|m| m.from.as_ref())
            .is_some_and(|author| author.id.0 == bot.id),
    })
}

/// An edited text message -> `NormalizedMessage`. Identical to
/// `normalize_message`, except the timestamp is the EDIT date
/// (`Message::edit_date`), not the original send date. teloxide 0.17
/// (teloxide-core 0.13.0) types `edit_date` as
/// `Option<DateTime<Utc>>`; the Bot API always sets it on an
/// edited-message update, so the fallback to `msg.date` is defensive
/// only.
///
/// Rule A1: pure, like every function in this module. The adapter wraps
/// the result in `InboundEvent::EditedMessage`.
///
/// Timestamp cutover (specs.md Section 4.2 backfill note): edit rows
/// persisted before this entry point existed carry the ORIGINAL send
/// date in their timestamp slot. The raw log is append-only (Rule P1);
/// no cleanup migration rewrites them. Readers of the log see mixed
/// edit-timestamp semantics across the cutover.
pub fn normalize_edited_message(msg: &Message, bot: &BotIdentity) -> Option<NormalizedMessage> {
    let mut normalized = normalize_message(msg, bot)?;
    if let Some(edit_date) = msg.edit_date() {
        normalized.timestamp = unix_to_offset(edit_date.timestamp());
    }
    Some(normalized)
}

/// Service messages -> `MemberJoin` / `MemberLeave`. One event per user in
/// `new_chat_members` (it is a list). An empty vec for non-service messages.
///
/// Note: the bot itself can appear in `new_chat_members`. The event is
/// emitted anyway; the core decides what to do with it.
pub fn normalize_service(msg: &Message) -> Vec<InboundEvent> {
    let timestamp = unix_to_offset(msg.date.timestamp());

    if let Some(users) = msg.new_chat_members() {
        return users
            .iter()
            .map(|user| {
                InboundEvent::MemberJoin(MemberEvent {
                    timestamp,
                    user_id: user.id.0.to_string(),
                    display_name: display_name(user),
                })
            })
            .collect();
    }

    if let Some(user) = msg.left_chat_member() {
        return vec![InboundEvent::MemberLeave(MemberEvent {
            timestamp,
            user_id: user.id.0.to_string(),
            display_name: display_name(user),
        })];
    }

    Vec::new()
}

/// `MessageReactionUpdated` -> `ReactionEvent`. A named user reactor gives
/// `reactor_id: Some(user id)` and `anonymous: false`. An anonymous actor
/// (`actor_chat`) gives the synthetic `chat:{id}` reactor id and
/// `anonymous: true`. The two are mutually exclusive per the Bot API
/// (teloxide models this as `MaybeAnonymousUser`). `aggregated: false`.
///
/// Note: in teloxide 0.17 the update has no direct `user`/`actor_chat`
/// fields. It has `actor: MaybeAnonymousUser` with the `user()` and
/// `chat()` accessors used here.
pub fn normalize_reaction(update: &MessageReactionUpdated) -> ReactionEvent {
    let (reactor_id, anonymous) = if let Some(user) = update.user() {
        (Some(user.id.0.to_string()), false)
    } else if let Some(chat) = update.chat() {
        (Some(format!("chat:{}", chat.id.0)), true)
    } else {
        // Unreachable per the Bot API: one of the two is always present.
        (None, false)
    };

    ReactionEvent {
        platform_msg_id: update.message_id.0.to_string(),
        timestamp: unix_to_offset(update.date.timestamp()),
        reactor_id,
        anonymous,
        aggregated: false,
        old_emojis: emojis_of(&update.old_reaction),
        new_emojis: emojis_of(&update.new_reaction),
    }
}

/// `MessageReactionCountUpdated` -> `ReactionEvent` with `reactor_id: None`
/// and `aggregated: true`. `old_emojis` is empty: the count update carries
/// only the current totals, no previous state (documented limitation of the
/// Bot API; anonymous contexts yield only these delayed updates).
pub fn normalize_reaction_count(update: &MessageReactionCountUpdated) -> ReactionEvent {
    ReactionEvent {
        platform_msg_id: update.message_id.0.to_string(),
        timestamp: unix_to_offset(update.date.timestamp()),
        reactor_id: None,
        anonymous: false,
        aggregated: true,
        old_emojis: Vec::new(),
        new_emojis: update
            .reactions
            .iter()
            .filter_map(|r| emoji_of(&r.r#type))
            .collect(),
    }
}

/// Sender resolution for `normalize_message`. See its doc comment for the
/// anonymous-admin mapping. The third tuple member is the sender's
/// username (`User.username`); `None` for senders without a username and
/// for anonymous chat senders.
fn resolve_sender(msg: &Message) -> (String, String, Option<String>) {
    if let Some(user) = &msg.from {
        return (
            user.id.0.to_string(),
            display_name(user),
            user.username.clone(),
        );
    }
    // Sent as a chat (anonymous group admin, or a channel post).
    let chat = msg.sender_chat.as_ref().unwrap_or(&msg.chat);
    let name = chat
        .title()
        .map(str::to_string)
        .or_else(|| chat.username().map(|u| format!("@{u}")))
        .unwrap_or_else(|| chat.id.0.to_string());
    (format!("chat:{}", chat.id.0), name, None)
}

/// True when an entity mentions the bot: a `mention` entity whose text is
/// `@<bot username>` (case-insensitive), or a `text_mention` entity that
/// carries the bot's user id. `Message::parse_entities` converts the UTF-16
/// entity offsets to UTF-8; do not slice the text with raw offsets.
fn mentions_bot(msg: &Message, bot: &BotIdentity) -> bool {
    let Some(entities) = msg.parse_entities() else {
        return false;
    };
    entities.iter().any(|entity| match entity.kind() {
        MessageEntityKind::Mention => entity
            .text()
            .strip_prefix('@')
            .is_some_and(|name| name.eq_ignore_ascii_case(&bot.username)),
        MessageEntityKind::TextMention { user } => user.id.0 == bot.id,
        _ => false,
    })
}

/// Maps a reaction list to emoji strings. See the module docs for the
/// mapping. `Paid` reactions are skipped.
fn emojis_of(reactions: &[ReactionType]) -> Vec<String> {
    reactions.iter().filter_map(emoji_of).collect()
}

/// Maps one reaction type to its string form. `None` for `Paid`.
fn emoji_of(reaction: &ReactionType) -> Option<String> {
    match reaction {
        ReactionType::Emoji { emoji } => Some(emoji.clone()),
        ReactionType::CustomEmoji { custom_emoji_id } => Some(custom_emoji_id.0.clone()),
        ReactionType::Paid => None,
    }
}

/// chrono `DateTime<Utc>` -> `OffsetDateTime` via the unix timestamp.
/// Expect-valid: Telegram dates are well within range.
fn unix_to_offset(secs: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(secs).expect("a valid unix timestamp")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const BOT_ID: u64 = 777_000;
    const BOT_USERNAME: &str = "tamako_bot";
    const GROUP_ID: i64 = -1_001_234_567_890;
    const DATE: i64 = 1_700_000_000;

    fn bot() -> BotIdentity {
        BotIdentity {
            id: BOT_ID,
            username: BOT_USERNAME.to_string(),
        }
    }

    fn bot_user_json() -> serde_json::Value {
        json!({
            "id": BOT_ID,
            "is_bot": true,
            "first_name": "Tamako",
            "username": BOT_USERNAME,
        })
    }

    fn group_json() -> serde_json::Value {
        json!({
            "id": GROUP_ID,
            "type": "supergroup",
            "title": "Test Group",
        })
    }

    fn user_json(
        id: u64,
        first: &str,
        last: Option<&str>,
        username: Option<&str>,
    ) -> serde_json::Value {
        let mut value = json!({ "id": id, "is_bot": false, "first_name": first });
        if let Some(last) = last {
            value["last_name"] = json!(last);
        }
        if let Some(username) = username {
            value["username"] = json!(username);
        }
        value
    }

    fn message(extra: serde_json::Value) -> Message {
        let mut base = json!({
            "message_id": 1,
            "from": user_json(42, "Alice", Some("Smith"), Some("alice")),
            "chat": group_json(),
            "date": DATE,
        });
        base.as_object_mut()
            .expect("an object")
            .extend(extra.as_object().expect("an object").clone());
        serde_json::from_value(base).expect("a valid Message")
    }

    fn text_message(text: &str, entities: serde_json::Value) -> Message {
        message(json!({ "text": text, "entities": entities }))
    }

    #[test]
    fn bot_identity_from_me() {
        let me: Me = serde_json::from_value(json!({
            "id": BOT_ID,
            "is_bot": true,
            "first_name": "Tamako",
            "username": BOT_USERNAME,
            "can_join_groups": true,
            "can_read_all_group_messages": true,
            "supports_inline_queries": false,
            "has_main_web_app": false,
        }))
        .expect("a valid Me");
        let identity = BotIdentity::from_me(&me);
        assert_eq!(identity, bot());
    }

    #[test]
    fn display_name_prefers_first_and_last_name() {
        let user: User =
            serde_json::from_value(user_json(1, "Alice", Some("Smith"), Some("alice")))
                .expect("a valid User");
        assert_eq!(display_name(&user), "Alice Smith");
    }

    #[test]
    fn display_name_falls_back_to_username() {
        let user: User = serde_json::from_value(user_json(1, "Alice", None, Some("alice")))
            .expect("a valid User");
        assert_eq!(display_name(&user), "@alice");
    }

    #[test]
    fn display_name_falls_back_to_numeric_id() {
        let user: User =
            serde_json::from_value(user_json(1, "Alice", None, None)).expect("a valid User");
        assert_eq!(display_name(&user), "1");
    }

    #[test]
    fn chat_id_string_keeps_the_minus_sign() {
        let chat: Chat = serde_json::from_value(group_json()).expect("a valid Chat");
        assert_eq!(chat_id_string(&chat), "-1001234567890");
    }

    #[test]
    fn normalizes_a_plain_text_message() {
        let msg = text_message("hello", json!([]));
        let normalized = normalize_message(&msg, &bot()).expect("a text message");
        assert_eq!(normalized.platform_msg_id, "1");
        assert_eq!(normalized.sender_id, "42");
        assert_eq!(normalized.sender_display_name, "Alice Smith");
        assert_eq!(normalized.username, Some("alice".to_string()));
        assert_eq!(normalized.text, "hello");
        assert_eq!(normalized.timestamp, unix_to_offset(DATE));
        assert_eq!(normalized.reply_to_platform_msg_id, None);
        assert!(!normalized.mentions_bot);
        assert!(!normalized.is_reply_to_bot);
    }

    #[test]
    fn a_sender_without_a_username_normalizes_to_none_username() {
        // The display-name fallback chain is untouched (decision 25): a
        // user without a username still gets a display name. The username
        // field alone is None.
        let msg = message(json!({
            "from": user_json(43, "Bob", None, None),
            "text": "no username here",
        }));
        let normalized = normalize_message(&msg, &bot()).expect("a text message");
        assert_eq!(normalized.sender_id, "43");
        assert_eq!(normalized.sender_display_name, "43");
        assert_eq!(normalized.username, None);
    }

    #[test]
    fn mention_entity_of_the_bot_username_sets_mentions_bot() {
        // "🔥 hi @tamako_bot": the fire emoji is two UTF-16 code units, so
        // the mention starts at UTF-16 offset 6. This test proves the
        // UTF-16 handling in parse_entities is used.
        let msg = text_message(
            "\u{1F525} hi @tamako_bot",
            json!([{ "type": "mention", "offset": 6, "length": 11 }]),
        );
        assert!(
            normalize_message(&msg, &bot())
                .expect("a text message")
                .mentions_bot
        );
    }

    #[test]
    fn mention_entity_matching_is_case_insensitive() {
        let msg = text_message(
            "hi @Tamako_Bot",
            json!([{ "type": "mention", "offset": 3, "length": 11 }]),
        );
        assert!(
            normalize_message(&msg, &bot())
                .expect("a text message")
                .mentions_bot
        );
    }

    #[test]
    fn text_mention_of_the_bot_id_sets_mentions_bot() {
        let msg = text_message(
            "hi Tamako",
            json!([{ "type": "text_mention", "offset": 3, "length": 6, "user": bot_user_json() }]),
        );
        assert!(
            normalize_message(&msg, &bot())
                .expect("a text message")
                .mentions_bot
        );
    }

    #[test]
    fn mention_of_another_user_is_not_a_bot_mention() {
        let msg = text_message(
            "hi @someone_else",
            json!([{ "type": "mention", "offset": 3, "length": 13 }]),
        );
        assert!(
            !normalize_message(&msg, &bot())
                .expect("a text message")
                .mentions_bot
        );
    }

    fn replying_message(replied_from: serde_json::Value) -> Message {
        message(json!({
            "text": "a reply",
            "reply_to_message": {
                "message_id": 99,
                "from": replied_from,
                "chat": group_json(),
                "date": DATE - 10,
                "text": "the original",
            },
        }))
    }

    #[test]
    fn reply_to_a_bot_message_sets_is_reply_to_bot() {
        let msg = replying_message(bot_user_json());
        let normalized = normalize_message(&msg, &bot()).expect("a text message");
        assert!(normalized.is_reply_to_bot);
        assert_eq!(normalized.reply_to_platform_msg_id, Some("99".to_string()));
    }

    #[test]
    fn reply_to_a_user_message_is_not_a_reply_to_bot() {
        let msg = replying_message(user_json(7, "Bob", None, Some("bob")));
        let normalized = normalize_message(&msg, &bot()).expect("a text message");
        assert!(!normalized.is_reply_to_bot);
        assert_eq!(normalized.reply_to_platform_msg_id, Some("99".to_string()));
    }

    #[test]
    fn normalize_message_ignores_the_edit_date() {
        // normalize_message stays pure and always stamps the SEND date.
        // Only the adapter's edited-message path (normalize_edited_message)
        // stamps the edit date. This test pins the separation.
        let msg = message(json!({ "text": "edited text", "edit_date": DATE + 5 }));
        let normalized = normalize_message(&msg, &bot()).expect("a text message");
        assert_eq!(normalized.timestamp, unix_to_offset(DATE));
    }

    #[test]
    fn edited_message_carries_the_edit_date() {
        // K3 fix: an edit row's timestamp is the EDIT date, not the
        // original send date. The other fields normalize identically
        // (rule A4 fields are the same).
        let msg = message(json!({ "text": "edited text", "edit_date": DATE + 5 }));
        let normalized = normalize_edited_message(&msg, &bot()).expect("a text message");
        assert_eq!(normalized.text, "edited text");
        assert_eq!(normalized.username, Some("alice".to_string()));
        assert_eq!(normalized.timestamp, unix_to_offset(DATE + 5));
    }

    #[test]
    fn edited_message_falls_back_to_the_send_date_without_an_edit_date() {
        // The Bot API always sets edit_date on an edited-message update;
        // the fallback to msg.date is defensive only.
        let msg = message(json!({ "text": "edited text" }));
        let normalized = normalize_edited_message(&msg, &bot()).expect("a text message");
        assert_eq!(normalized.timestamp, unix_to_offset(DATE));
    }

    #[test]
    fn new_chat_members_give_one_join_event_per_user() {
        let msg = message(json!({
            "new_chat_members": [
                user_json(10, "Carol", Some("Jones"), None),
                user_json(11, "Dave", None, Some("dave")),
            ],
        }));
        let events = normalize_service(&msg);
        assert_eq!(events.len(), 2);
        match &events[0] {
            InboundEvent::MemberJoin(member) => {
                assert_eq!(member.user_id, "10");
                assert_eq!(member.display_name, "Carol Jones");
                assert_eq!(member.timestamp, unix_to_offset(DATE));
            }
            other => panic!("expected MemberJoin, got {other:?}"),
        }
        match &events[1] {
            InboundEvent::MemberJoin(member) => {
                assert_eq!(member.user_id, "11");
                assert_eq!(member.display_name, "@dave");
            }
            other => panic!("expected MemberJoin, got {other:?}"),
        }
    }

    #[test]
    fn left_chat_member_gives_a_leave_event() {
        let msg = message(json!({
            "left_chat_member": user_json(12, "Eve", None, None),
        }));
        let events = normalize_service(&msg);
        assert_eq!(events.len(), 1);
        match &events[0] {
            InboundEvent::MemberLeave(member) => {
                assert_eq!(member.user_id, "12");
                assert_eq!(member.display_name, "12");
            }
            other => panic!("expected MemberLeave, got {other:?}"),
        }
    }

    #[test]
    fn a_non_service_message_gives_no_service_events() {
        let msg = text_message("hello", json!([]));
        assert!(normalize_service(&msg).is_empty());
    }

    #[test]
    fn a_service_message_is_not_a_text_message() {
        let msg = message(json!({
            "new_chat_members": [user_json(10, "Carol", None, None)],
        }));
        assert!(normalize_message(&msg, &bot()).is_none());
    }

    #[test]
    fn a_photo_message_normalizes_to_none() {
        let msg = message(json!({
            "photo": [{
                "file_id": "file-id",
                "file_unique_id": "unique-id",
                "width": 100,
                "height": 100,
            }],
        }));
        assert!(normalize_message(&msg, &bot()).is_none());
    }

    #[test]
    fn an_anonymous_admin_gets_the_synthetic_chat_sender() {
        let msg = message(json!({
            "from": null,
            "sender_chat": group_json(),
            "text": "anonymous admin text",
        }));
        let normalized = normalize_message(&msg, &bot()).expect("a text message");
        assert_eq!(normalized.sender_id, format!("chat:{GROUP_ID}"));
        assert_eq!(normalized.sender_display_name, "Test Group");
        // Anonymous actors carry no user identity, so no username either.
        assert_eq!(normalized.username, None);
    }

    fn reaction_update(actor: serde_json::Value) -> MessageReactionUpdated {
        let mut base = json!({
            "chat": group_json(),
            "message_id": 35,
            "date": DATE,
            "old_reaction": [{ "type": "emoji", "emoji": "👍" }],
            "new_reaction": [
                { "type": "emoji", "emoji": "❤" },
                { "type": "custom_emoji", "custom_emoji_id": "custom-1" },
            ],
        });
        base.as_object_mut()
            .expect("an object")
            .extend(actor.as_object().expect("an object").clone());
        serde_json::from_value(base).expect("a valid MessageReactionUpdated")
    }

    #[test]
    fn a_named_reaction_carries_the_user_id() {
        let update =
            reaction_update(json!({ "user": user_json(42, "Alice", Some("Smith"), None) }));
        let event = normalize_reaction(&update);
        assert_eq!(event.platform_msg_id, "35");
        assert_eq!(event.reactor_id, Some("42".to_string()));
        assert!(!event.anonymous);
        assert!(!event.aggregated);
        assert_eq!(event.old_emojis, vec!["👍".to_string()]);
        assert_eq!(
            event.new_emojis,
            vec!["❤".to_string(), "custom-1".to_string()]
        );
        assert_eq!(event.timestamp, unix_to_offset(DATE));
    }

    #[test]
    fn an_anonymous_reaction_uses_the_synthetic_chat_reactor() {
        let update = reaction_update(json!({ "actor_chat": group_json() }));
        let event = normalize_reaction(&update);
        assert_eq!(event.reactor_id, Some(format!("chat:{GROUP_ID}")));
        assert!(event.anonymous);
        assert!(!event.aggregated);
    }

    #[test]
    fn a_paid_reaction_is_skipped() {
        let update: MessageReactionUpdated = serde_json::from_value(json!({
            "chat": group_json(),
            "message_id": 35,
            "user": user_json(42, "Alice", None, None),
            "date": DATE,
            "old_reaction": [],
            "new_reaction": [{ "type": "paid" }],
        }))
        .expect("a valid MessageReactionUpdated");
        assert!(normalize_reaction(&update).new_emojis.is_empty());
    }

    #[test]
    fn a_count_update_is_aggregated_and_has_no_reactor() {
        let update: MessageReactionCountUpdated = serde_json::from_value(json!({
            "chat": group_json(),
            "message_id": 36,
            "date": DATE,
            "reactions": [
                { "type": { "type": "emoji", "emoji": "🗿" }, "total_count": 2 },
                { "type": { "type": "emoji", "emoji": "🌭" }, "total_count": 1 },
            ],
        }))
        .expect("a valid MessageReactionCountUpdated");
        let event = normalize_reaction_count(&update);
        assert_eq!(event.platform_msg_id, "36");
        assert_eq!(event.reactor_id, None);
        assert!(!event.anonymous);
        assert!(event.aggregated);
        // The count update carries no previous state.
        assert!(event.old_emojis.is_empty());
        assert_eq!(event.new_emojis, vec!["🗿".to_string(), "🌭".to_string()]);
        assert_eq!(event.timestamp, unix_to_offset(DATE));
    }
}
