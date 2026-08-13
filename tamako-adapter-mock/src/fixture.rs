//! The replay fixture format.
//!
//! A replay fixture is a recorded chat log for one group. It is JSON test
//! data. Justification for JSON: the fixture is test data, and JSON needs
//! no hand-written parser. serde_json does the work.
//!
//! Message and edited-message entries MAY carry an optional `username`
//! field: the sender's platform username (Rule A1: the normalized
//! `NormalizedMessage.username`). Entries without the field are valid and
//! deserialize with `username: None` — the no-username path.
//!
//! Rule A5: a second platform adapter must be possible without changes to
//! the actor. This fixture proves that the normalized contract of
//! tamako-core is sufficient to drive the system.

use std::path::Path;

use tamako_core::event::{InboundEvent, MemberEvent, NormalizedMessage, ReactionEvent};

/// A recorded chat log for one group.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReplayFixture {
    /// The group chat id that produced this recording.
    pub chat_id: String,
    /// The recorded events, in time order.
    pub events: Vec<FixtureEvent>,
}

/// One recorded event.
///
/// The serde tag keeps the JSON legible:
/// `{"type": "message", ...fields}`, `{"type": "edited_message", ...}`,
/// `{"type": "reaction", ...}`, `{"type": "member_join", ...}`,
/// `{"type": "member_leave", ...}`.
///
/// `Message` and `EditedMessage` entries carry the full
/// [`NormalizedMessage`]. The `username` field inside them is OPTIONAL
/// (serde `Option`): entries without it deserialize with
/// `username: None`.
///
/// Rule A2: the adapter exposes these five inbound event kinds. The
/// variants carry the same normalized structs as [`InboundEvent`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FixtureEvent {
    Message(NormalizedMessage),
    EditedMessage(NormalizedMessage),
    Reaction(ReactionEvent),
    MemberJoin(MemberEvent),
    MemberLeave(MemberEvent),
}

impl From<FixtureEvent> for InboundEvent {
    fn from(event: FixtureEvent) -> Self {
        match event {
            FixtureEvent::Message(m) => InboundEvent::Message(m),
            FixtureEvent::EditedMessage(m) => InboundEvent::EditedMessage(m),
            FixtureEvent::Reaction(r) => InboundEvent::Reaction(r),
            FixtureEvent::MemberJoin(m) => InboundEvent::MemberJoin(m),
            FixtureEvent::MemberLeave(m) => InboundEvent::MemberLeave(m),
        }
    }
}

/// Errors of fixture loading.
#[derive(Debug, thiserror::Error)]
pub enum FixtureError {
    /// The fixture file cannot be read.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// The fixture file is not valid JSON or does not match the format.
    #[error("fixture parse error: {0}")]
    Parse(#[from] serde_json::Error),
}

/// Loads a replay fixture from a JSON file.
pub fn load_fixture(path: &Path) -> Result<ReplayFixture, FixtureError> {
    let bytes = std::fs::read(path)?;
    let fixture = serde_json::from_slice(&bytes)?;
    Ok(fixture)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tamako_core::event::{InboundEvent, MemberEvent, NormalizedMessage, ReactionEvent};

    /// Path of the shipped demo fixture.
    fn shipped_fixture_path() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/replay_chat.json")
    }

    #[test]
    fn shipped_fixture_parses_and_yields_14_events_in_order() {
        let fixture = load_fixture(&shipped_fixture_path()).expect("fixture must parse");

        assert_eq!(fixture.chat_id, "-1001234567890");
        assert_eq!(fixture.events.len(), 14);

        // The events must be in time order: no timestamp goes back in time.
        let timestamps: Vec<_> = fixture
            .events
            .iter()
            .map(|event| match event {
                FixtureEvent::Message(m) | FixtureEvent::EditedMessage(m) => m.timestamp,
                FixtureEvent::Reaction(r) => r.timestamp,
                FixtureEvent::MemberJoin(m) | FixtureEvent::MemberLeave(m) => m.timestamp,
            })
            .collect();
        for pair in timestamps.windows(2) {
            assert!(pair[0] <= pair[1], "events must be in time order");
        }

        // The event kinds must appear in the recorded order.
        let kinds: Vec<&str> = fixture
            .events
            .iter()
            .map(|event| match event {
                FixtureEvent::MemberJoin(_) => "member_join",
                FixtureEvent::Message(_) => "message",
                FixtureEvent::EditedMessage(_) => "edited_message",
                FixtureEvent::Reaction(_) => "reaction",
                FixtureEvent::MemberLeave(_) => "member_leave",
            })
            .collect();
        assert_eq!(
            kinds,
            [
                "member_join",
                "message",
                "message",
                "message",
                "message",
                "message",
                "message",
                "message",
                "message",
                "message",
                "message",
                "reaction",
                "edited_message",
                "member_leave",
            ]
        );
    }

    #[test]
    fn conversion_preserves_every_field() {
        // Test data comes from JSON. The time crate is not a dependency of
        // this crate, so timestamps enter through the serde format.
        let message: NormalizedMessage = serde_json::from_value(serde_json::json!({
            "platform_msg_id": "7",
            "timestamp": "2026-08-01T13:00:00Z",
            "sender_id": "100001",
            "sender_display_name": "Alice",
            "username": "alice",
            "text": "hello",
            "reply_to_platform_msg_id": "6",
            "mentions_bot": true,
            "is_reply_to_bot": false,
        }))
        .expect("message must deserialize");

        let converted = InboundEvent::from(FixtureEvent::Message(message.clone()));
        let InboundEvent::Message(got) = converted else {
            panic!("variant must be preserved");
        };
        assert_eq!(got.platform_msg_id, message.platform_msg_id);
        assert_eq!(got.timestamp, message.timestamp);
        assert_eq!(got.sender_id, message.sender_id);
        assert_eq!(got.sender_display_name, message.sender_display_name);
        assert_eq!(got.username, message.username);
        assert_eq!(got.text, message.text);
        assert_eq!(
            got.reply_to_platform_msg_id,
            message.reply_to_platform_msg_id
        );
        assert_eq!(got.mentions_bot, message.mentions_bot);
        assert_eq!(got.is_reply_to_bot, message.is_reply_to_bot);

        let reaction: ReactionEvent = serde_json::from_value(serde_json::json!({
            "platform_msg_id": "7",
            "timestamp": "2026-08-01T13:00:00Z",
            "reactor_id": "100002",
            "anonymous": false,
            "aggregated": false,
            "old_emojis": [],
            "new_emojis": ["👍"],
        }))
        .expect("reaction must deserialize");
        let converted = InboundEvent::from(FixtureEvent::Reaction(reaction.clone()));
        let InboundEvent::Reaction(got) = converted else {
            panic!("variant must be preserved");
        };
        assert_eq!(got.platform_msg_id, reaction.platform_msg_id);
        assert_eq!(got.timestamp, reaction.timestamp);
        assert_eq!(got.reactor_id, reaction.reactor_id);
        assert_eq!(got.anonymous, reaction.anonymous);
        assert_eq!(got.aggregated, reaction.aggregated);
        assert_eq!(got.old_emojis, reaction.old_emojis);
        assert_eq!(got.new_emojis, reaction.new_emojis);

        let member: MemberEvent = serde_json::from_value(serde_json::json!({
            "timestamp": "2026-08-01T13:00:00Z",
            "user_id": "100003",
            "display_name": "Carol",
        }))
        .expect("member event must deserialize");
        let converted = InboundEvent::from(FixtureEvent::MemberJoin(member.clone()));
        let InboundEvent::MemberJoin(got) = converted else {
            panic!("variant must be preserved");
        };
        assert_eq!(got.timestamp, member.timestamp);
        assert_eq!(got.user_id, member.user_id);
        assert_eq!(got.display_name, member.display_name);

        let converted = InboundEvent::from(FixtureEvent::MemberLeave(member));
        assert!(matches!(converted, InboundEvent::MemberLeave(_)));

        let converted = InboundEvent::from(FixtureEvent::EditedMessage(message));
        assert!(matches!(converted, InboundEvent::EditedMessage(_)));
    }

    /// A message entry JSON with the optional `username` field present.
    fn message_json_with_username(username: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "platform_msg_id": "1",
            "timestamp": "2026-08-01T13:00:00Z",
            "sender_id": "100001",
            "sender_display_name": "Alice",
            "username": username,
            "text": "hello",
            "reply_to_platform_msg_id": null,
            "mentions_bot": false,
            "is_reply_to_bot": false,
        })
    }

    #[test]
    fn a_message_entry_with_username_parses_and_populates_the_field() {
        let message: NormalizedMessage =
            serde_json::from_value(message_json_with_username(serde_json::json!("alice")))
                .expect("message must deserialize");
        assert_eq!(message.username, Some("alice".to_string()));
    }

    #[test]
    fn a_message_entry_without_username_parses_with_none() {
        // `username` is an optional field of the fixture format: entries
        // without it must keep working.
        let message: NormalizedMessage =
            serde_json::from_value(message_json_with_username(serde_json::Value::Null))
                .expect("message must deserialize");
        assert_eq!(message.username, None);
    }

    #[test]
    fn a_message_entry_with_a_missing_username_key_parses_with_none() {
        let json = message_json_with_username(serde_json::json!("alice"));
        let mut without_key = json;
        without_key
            .as_object_mut()
            .expect("an object")
            .remove("username");
        let message: NormalizedMessage =
            serde_json::from_value(without_key).expect("message must deserialize");
        assert_eq!(message.username, None);
    }

    #[test]
    fn malformed_fixture_returns_parse_error() {
        let path = std::env::temp_dir().join(format!(
            "tamako_mock_bad_fixture_{}.json",
            std::process::id()
        ));
        std::fs::write(&path, b"{ this is not json").expect("write must succeed");

        let result = load_fixture(&path);
        assert!(
            matches!(result, Err(FixtureError::Parse(_))),
            "malformed JSON must give FixtureError::Parse, got: {result:?}"
        );

        // Well-formed JSON with a wrong shape is also a parse error.
        std::fs::write(&path, br#"{"chat_id": 42, "events": []}"#).expect("write must succeed");
        let result = load_fixture(&path);
        assert!(
            matches!(result, Err(FixtureError::Parse(_))),
            "wrong shape must give FixtureError::Parse, got: {result:?}"
        );

        let _ = std::fs::remove_file(&path);
    }
}
