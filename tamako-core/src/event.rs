//! Normalized platform types. Refer to specs.md Section 4.
//! Rule A1: these types carry no platform-specific data.

use time::OffsetDateTime;

/// Rule A4: a normalized message carries these fields and no platform types.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NormalizedMessage {
    pub platform_msg_id: String,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
    pub sender_id: String,
    pub sender_display_name: String,
    pub text: String,
    pub reply_to_platform_msg_id: Option<String>,
    pub mentions_bot: bool,
    pub is_reply_to_bot: bool,
}

/// A reaction on a message. Refer to specs.md Section 4.1, rule A2, and
/// Section 5.2 for the reaction table.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReactionEvent {
    pub platform_msg_id: String,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
    /// The reactor. `None` when the platform gives no reactor identity
    /// (aggregated count updates).
    pub reactor_id: Option<String>,
    /// True when the reactor acted anonymously (sent as a chat; the
    /// reactor_id then carries the synthetic `chat:{id}` form).
    pub anonymous: bool,
    /// True for aggregated count updates (weak signal, no per-user data).
    pub aggregated: bool,
    /// The emoji sets before and after the event (specs.md Section 5.2).
    pub old_emojis: Vec<String>,
    pub new_emojis: Vec<String>,
}

/// A member join or leave event. Refer to specs.md Section 4.1, rule A2.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MemberEvent {
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
    pub user_id: String,
    pub display_name: String,
}

/// Rule A2: inbound events.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum InboundEvent {
    Message(NormalizedMessage),
    EditedMessage(NormalizedMessage),
    Reaction(ReactionEvent),
    MemberJoin(MemberEvent),
    MemberLeave(MemberEvent),
}

impl InboundEvent {
    /// Returns the timestamp of the event.
    pub fn timestamp(&self) -> OffsetDateTime {
        match self {
            Self::Message(message) | Self::EditedMessage(message) => message.timestamp,
            Self::Reaction(reaction) => reaction.timestamp,
            Self::MemberJoin(member) | Self::MemberLeave(member) => member.timestamp,
        }
    }
}

/// Rule A3: outbound actions.
///
/// Every variant carries its target `chat_id` as the first field. The live
/// adapter serves several groups from one update stream, so the action must
/// carry its target chat. Rule P5: nothing crosses groups.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OutboundAction {
    SendText {
        chat_id: String,
        text: String,
        reply_to_platform_msg_id: Option<String>,
    },
    SendMedia {
        chat_id: String,
        media_ref: String,
        caption: Option<String>,
        reply_to_platform_msg_id: Option<String>,
    },
    React {
        chat_id: String,
        platform_msg_id: String,
        emoji: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_message() -> NormalizedMessage {
        NormalizedMessage {
            platform_msg_id: "m1".to_string(),
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_000)
                .expect("a valid unix timestamp"),
            sender_id: "u1".to_string(),
            sender_display_name: "Alice".to_string(),
            text: "hello".to_string(),
            reply_to_platform_msg_id: None,
            mentions_bot: false,
            is_reply_to_bot: false,
        }
    }

    #[test]
    fn timestamp_returns_the_event_timestamp() {
        let message = sample_message();
        let event = InboundEvent::Message(message.clone());
        assert_eq!(event.timestamp(), message.timestamp);
    }

    #[test]
    fn message_serde_round_trip() {
        let message = sample_message();
        let text = toml::to_string(&message).expect("serialization succeeds");
        let parsed: NormalizedMessage = toml::from_str(&text).expect("deserialization succeeds");
        assert_eq!(message, parsed);
    }
}
