//! The mock platform adapter.
//!
//! The adapter replays fixture events in order. It records every outbound
//! action for test assertions. It never fails: a recording is a reliable
//! source.
//!
//! Rule A1: only normalized types cross the adapter boundary.
//! Rule A3: the outbound actions are `SendText`, `SendMedia`, and `React`.
//! The adapter records them all.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use tamako_core::adapter::{AdapterError, PlatformAdapter};
use tamako_core::event::{InboundEvent, OutboundAction};

use crate::fixture::{self, FixtureError, ReplayFixture};

/// Mock platform adapter. Replays fixture events in order. Records every
/// outbound action for test assertions.
pub struct MockAdapter {
    chat_id: String,
    events: VecDeque<InboundEvent>,
    recorded: Mutex<Vec<OutboundAction>>,
}

impl MockAdapter {
    /// Builds an adapter from a parsed fixture.
    pub fn from_fixture(fixture: ReplayFixture) -> Self {
        Self {
            chat_id: fixture.chat_id,
            events: fixture.events.into_iter().map(InboundEvent::from).collect(),
            recorded: Mutex::new(Vec::new()),
        }
    }

    /// Loads a fixture file and builds an adapter from it.
    pub fn from_fixture_path(path: &Path) -> Result<Self, FixtureError> {
        Ok(Self::from_fixture(fixture::load_fixture(path)?))
    }

    /// Returns the group chat id of the replayed recording.
    pub fn chat_id(&self) -> &str {
        &self.chat_id
    }

    /// Returns a copy of the recorded outbound actions, in record order.
    pub fn recorded_actions(&self) -> Vec<OutboundAction> {
        self.recorded_lock().clone()
    }

    /// Returns the number of events that are not yet replayed.
    pub fn remaining(&self) -> usize {
        self.events.len()
    }

    /// Locks the recording. If a panic poisoned the mutex, this method
    /// recovers the recording. The recorded data is still valid after a
    /// panic in another caller.
    fn recorded_lock(&self) -> MutexGuard<'_, Vec<OutboundAction>> {
        self.recorded.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl PlatformAdapter for MockAdapter {
    /// Pops the next event from the recording. Returns `None` at the end
    /// of the recording.
    async fn next_event(&mut self) -> Result<Option<InboundEvent>, AdapterError> {
        let event = self.events.pop_front();
        if event.is_none() {
            tracing::debug!(chat_id = %self.chat_id, "replay finished");
        }
        Ok(event)
    }

    /// Records the outbound action. Always succeeds.
    async fn execute(&self, action: OutboundAction) -> Result<(), AdapterError> {
        tracing::debug!(chat_id = %self.chat_id, "recorded outbound action");
        self.recorded_lock().push(action);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_fixture() -> ReplayFixture {
        // Test data comes from JSON. The time crate is not a dependency of
        // this crate, so timestamps enter through the serde format.
        serde_json::from_value(serde_json::json!({
            "chat_id": "-1001",
            "events": [
                {
                    "type": "message",
                    "platform_msg_id": "1",
                    "timestamp": "2026-08-01T13:00:00Z",
                    "sender_id": "100001",
                    "sender_display_name": "Alice",
                    "username": "alice",
                    "text": "first",
                    "reply_to_platform_msg_id": null,
                    "mentions_bot": false,
                    "is_reply_to_bot": false,
                },
                {
                    "type": "reaction",
                    "platform_msg_id": "1",
                    "timestamp": "2026-08-01T13:01:00Z",
                    "reactor_id": "100002",
                    "anonymous": false,
                    "aggregated": false,
                    "old_emojis": [],
                    "new_emojis": ["👍"],
                },
                {
                    "type": "member_join",
                    "timestamp": "2026-08-01T13:02:00Z",
                    "user_id": "100003",
                    "display_name": "Carol",
                },
            ],
        }))
        .expect("fixture must deserialize")
    }

    #[tokio::test]
    async fn next_event_replays_full_sequence_then_none() {
        let mut adapter = MockAdapter::from_fixture(test_fixture());
        assert_eq!(adapter.remaining(), 3);
        assert_eq!(adapter.chat_id(), "-1001");

        let first = adapter.next_event().await.expect("no source error");
        let Some(InboundEvent::Message(m)) = first else {
            panic!("first event must be a message");
        };
        assert_eq!(m.text, "first");
        assert_eq!(m.username, Some("alice".to_string()));

        let second = adapter.next_event().await.expect("no source error");
        assert!(matches!(second, Some(InboundEvent::Reaction(_))));

        let third = adapter.next_event().await.expect("no source error");
        assert!(matches!(third, Some(InboundEvent::MemberJoin(_))));

        assert_eq!(adapter.remaining(), 0);
        // The end of the recording is stable: None, again and again.
        assert!(adapter
            .next_event()
            .await
            .expect("no source error")
            .is_none());
        assert!(adapter
            .next_event()
            .await
            .expect("no source error")
            .is_none());
    }

    #[tokio::test]
    async fn execute_records_actions_in_order() {
        let adapter = MockAdapter::from_fixture(test_fixture());

        adapter
            .execute(OutboundAction::SendText {
                chat_id: "-1001".to_string(),
                text: "hello".to_string(),
                reply_to_platform_msg_id: None,
            })
            .await
            .expect("execute must succeed");
        adapter
            .execute(OutboundAction::React {
                chat_id: "-1001".to_string(),
                platform_msg_id: "1".to_string(),
                emoji: "❤️".to_string(),
            })
            .await
            .expect("execute must succeed");
        adapter
            .execute(OutboundAction::SendMedia {
                chat_id: "-1001".to_string(),
                media_ref: "photo:cat".to_string(),
                caption: Some("a cat".to_string()),
                reply_to_platform_msg_id: Some("1".to_string()),
            })
            .await
            .expect("execute must succeed");

        let recorded = adapter.recorded_actions();
        assert_eq!(recorded.len(), 3);
        assert!(matches!(&recorded[0], OutboundAction::SendText { text, .. } if text == "hello"));
        assert!(matches!(&recorded[1], OutboundAction::React { emoji, .. } if emoji == "❤️"));
        assert!(
            matches!(&recorded[2], OutboundAction::SendMedia { media_ref, .. } if media_ref == "photo:cat")
        );
    }

    #[test]
    fn from_fixture_path_loads_the_shipped_fixture() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/replay_chat.json");
        let adapter = MockAdapter::from_fixture_path(&path).expect("fixture must load");
        assert_eq!(adapter.chat_id(), "-1001234567890");
        assert_eq!(adapter.remaining(), 14);
    }
}
