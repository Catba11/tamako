//! The live teloxide adapter: polling intake plus outbound actions.
//!
//! Rule A1: no teloxide type crosses the boundary. Only `GroupEvent`,
//! `TeloxideAdapter`, and the re-exported `BotIdentity` leave this module;
//! they carry only `tamako_core::event` types and strings.
//!
//! Stream-consumption design (verified in the teloxide 0.17.0 source,
//! `update_listeners/polling.rs` and `update_listeners.rs`):
//! `Polling::as_stream` returns a `PollingStream<'a, Bot>` that borrows
//! `&'a mut Polling<Bot>`. The stream and the listener therefore cannot live
//! in the same struct (that would be self-referential), and teloxide 0.17
//! has no owned-stream API. A background task owns both and forwards the
//! stream items over a channel. No Dispatcher, no dptree handler. The
//! adapter awaits the next item from the channel directly.

use std::collections::VecDeque;
use std::time::Duration;

use futures::StreamExt as _;
use tamako_core::adapter::{AdapterError, PlatformAdapter};
use tamako_core::event::{InboundEvent, OutboundAction};
use teloxide::payloads::{SendMessageSetters as _, SetMessageReactionSetters as _};
use teloxide::requests::Requester as _;
use teloxide::types::{
    AllowedUpdate, ChatId, MessageId, ReactionType, ReplyParameters, Update, UpdateKind, UserId,
};
use teloxide::update_listeners::{AsUpdateStream as _, Polling};
use teloxide::{Bot, RequestError};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::capability::{self, BotChatStatus};
use crate::normalize::{self, BotIdentity};

/// One normalized inbound event with its source chat (rule A2). The live
/// binary serves several groups from one update stream and routes by this
/// id. Rule P5: nothing crosses groups; the chat id makes the boundary
/// explicit.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupEvent {
    pub chat_id: String,
    pub event: InboundEvent,
}

/// The Telegram platform adapter (specs.md Section 4.2).
///
/// Owns the `Bot` (rule A1: it stays inside), the bot identity resolved at
/// startup, and the receiving end of the polling update channel.
pub struct TeloxideAdapter {
    bot: Bot,
    identity: BotIdentity,
    /// Events of one update that wait for delivery. One update can give
    /// several events (one MemberJoin per new chat member).
    pending: VecDeque<GroupEvent>,
    /// The inbound update channel. The sender lives in the polling task;
    /// see the module docs for why a task owns the listener stream.
    updates: mpsc::Receiver<Result<Update, RequestError>>,
}

impl TeloxideAdapter {
    /// Builds the adapter: resolves the bot identity through `get_me` and
    /// starts the polling task.
    ///
    /// specs.md Section 4.2: mention and reply metadata is resolved at
    /// intake, so the bot identity is needed before the first event.
    pub async fn new(token: &str) -> Result<Self, AdapterError> {
        let bot = Bot::new(token);
        // Verified in teloxide-core 0.13.0 `bot.rs`: only `Bot::from_env`
        // honors the TELOXIDE_API_URL env variable; `Bot::new` does not.
        // A custom Bot API server is a supported deployment, so set it here.
        let bot = match std::env::var("TELOXIDE_API_URL") {
            Ok(raw) => {
                // teloxide does not re-export the Url type (reqwest::Url).
                // Its type is inferred from the `set_api_url` parameter.
                let api_url = raw.trim_end_matches('/').parse().map_err(|err| {
                    AdapterError::Source(format!("invalid TELOXIDE_API_URL {raw:?}: {err}"))
                })?;
                bot.set_api_url(api_url)
            }
            Err(_) => bot,
        };
        let me = bot
            .get_me()
            .await
            .map_err(|err| AdapterError::Source(format!("get_me failed: {err}")))?;
        let identity = BotIdentity::from_me(&me);
        Ok(Self {
            bot: bot.clone(),
            identity,
            pending: VecDeque::new(),
            updates: spawn_polling_task(bot),
        })
    }

    /// The bot identity from `get_me` at startup.
    pub fn bot_identity(&self) -> &BotIdentity {
        &self.identity
    }

    /// The bot's membership status in one chat, through `get_chat_member`.
    ///
    /// Reaction updates arrive only for administrators (specs.md Section
    /// 4.2). A non-admin bot works normally, except reaction collection is
    /// absent. Requesting reaction updates without admin status causes no
    /// error; the updates simply never arrive.
    ///
    /// A chat id parse failure gives `Unknown` with a debug log. Any call
    /// failure (e.g. the bot is not in the chat) gives `Unknown` with a
    /// debug log; the caller continues normally.
    pub async fn bot_chat_status(&self, chat_id: &str) -> BotChatStatus {
        let chat = match chat_id.parse::<i64>() {
            Ok(id) => ChatId(id),
            Err(err) => {
                debug!(chat_id = %chat_id, error = %err, "invalid chat id; status unknown");
                return BotChatStatus::Unknown;
            }
        };
        match self
            .bot
            .get_chat_member(chat, UserId(self.identity.id))
            .await
        {
            Ok(member) => capability::classify_member_kind(&member.kind),
            Err(err) => {
                debug!(chat_id = %chat_id, error = %err, "get_chat_member failed; status unknown");
                BotChatStatus::Unknown
            }
        }
    }

    /// Returns the next normalized group event, with its chat id.
    ///
    /// One update can give zero events (a non-text message, an unhandled
    /// update kind: skipped with a debug log) or several (delivered in
    /// order, each with the same chat id). Stream errors are logged at warn
    /// level and skipped, not fatal. Returns `Ok(None)` only when the
    /// update stream has ended.
    pub async fn next_group_event(&mut self) -> Result<Option<GroupEvent>, AdapterError> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(Some(event));
            }
            let update = match self.updates.recv().await {
                // The channel closed: the polling task ended.
                None => return Ok(None),
                Some(Ok(update)) => update,
                Some(Err(err)) => {
                    warn!(error = %err, "update stream error; skipping");
                    continue;
                }
            };
            let events = events_of_update(&update, &self.identity);
            if events.is_empty() {
                debug!(update_id = update.id.0, "update gave no events; skipping");
                continue;
            }
            self.pending = events.into();
        }
    }
}

impl PlatformAdapter for TeloxideAdapter {
    /// The trait view is the rule A5 substitutability proof and serves
    /// single-group consumers. The live binary serves several groups and
    /// routes by chat id through `next_group_event`.
    async fn next_event(&mut self) -> Result<Option<InboundEvent>, AdapterError> {
        Ok(self.next_group_event().await?.map(|group| group.event))
    }

    /// Rule A3: outbound actions.
    async fn execute(&self, action: OutboundAction) -> Result<(), AdapterError> {
        match action {
            OutboundAction::SendText {
                chat_id,
                text,
                reply_to_platform_msg_id,
            } => {
                let mut request = self.bot.send_message(parse_chat_id(&chat_id)?, text);
                if let Some(reply_to) = reply_to_platform_msg_id {
                    // teloxide 0.17 replaced `reply_to_message_id` with
                    // `reply_parameters` (verified in teloxide-core 0.13.0
                    // `payloads/send_message.rs`).
                    request = request
                        .reply_parameters(ReplyParameters::new(parse_message_id(&reply_to)?));
                }
                // specs.md Section 4.2: a denied outbound action is
                // tolerated and logged, never fatal.
                outbound_result(request.await, &chat_id, "send_message")
            }
            OutboundAction::React {
                chat_id,
                platform_msg_id,
                emoji,
            } => {
                // Reactions set by bots never generate updates, so this
                // never loops back into intake (Bot API docs, mirrored in
                // teloxide-core 0.13.0 `types/update.rs`).
                // specs.md Section 4.2: a denied outbound action is
                // tolerated and logged, never fatal.
                outbound_result(
                    self.bot
                        .set_message_reaction(
                            parse_chat_id(&chat_id)?,
                            parse_message_id(&platform_msg_id)?,
                        )
                        .reaction(vec![ReactionType::Emoji { emoji }])
                        .await,
                    &chat_id,
                    "set_message_reaction",
                )
            }
            OutboundAction::SendMedia { .. } => Err(AdapterError::Unsupported(
                "SendMedia is Phase 3 territory; the pet's replies are text".to_string(),
            )),
        }
    }
}

/// Starts the polling task and returns the receiving end of the update
/// channel. The task ends when the listener stream ends or when the
/// receiver is dropped (the adapter was dropped).
fn spawn_polling_task(bot: Bot) -> mpsc::Receiver<Result<Update, RequestError>> {
    // Capacity 100: one full get_updates batch (the Bot API maximum). A
    // full channel applies backpressure to the polling task.
    let (tx, rx) = mpsc::channel(100);
    tokio::spawn(async move {
        // `Polling::new` does not exist in teloxide 0.17; the builder is the
        // entry point (verified in `update_listeners/polling.rs`).
        let mut listener = Polling::builder(bot)
            // Must stay below the default reqwest client timeout of 17 s
            // (teloxide-core 0.13.0 `net.rs`). 10 s matches the teloxide
            // `polling_default` listener.
            .timeout(Duration::from_secs(10))
            // A pending webhook makes get_updates fail with HTTP 409.
            .delete_webhook()
            .await
            // Reaction updates also require the bot to be a chat
            // administrator. Bots never see other bots' messages. Reactions
            // set by bots never generate updates.
            .allowed_updates(vec![
                AllowedUpdate::Message,
                AllowedUpdate::EditedMessage,
                AllowedUpdate::MessageReaction,
                AllowedUpdate::MessageReactionCount,
                AllowedUpdate::ChatMember,
            ])
            .build();
        // PollingStream is !Unpin (pin_project over tokio Sleep), so pin it
        // on the stack before StreamExt::next.
        let mut stream = std::pin::pin!(listener.as_stream());
        while let Some(item) = stream.next().await {
            if tx.send(item).await.is_err() {
                // The adapter was dropped.
                break;
            }
        }
    });
    rx
}

/// Maps one update to zero or more group events. Pure: no I/O, so the unit
/// tests exercise the full intake dispatch without network. Every event
/// carries its chat id (rule P5 boundary).
fn events_of_update(update: &Update, bot: &BotIdentity) -> Vec<GroupEvent> {
    match &update.kind {
        UpdateKind::Message(msg) => {
            let chat_id = normalize::chat_id_string(&msg.chat);
            if let Some(message) = normalize::normalize_message(msg, bot) {
                vec![GroupEvent {
                    chat_id,
                    event: InboundEvent::Message(message),
                }]
            } else {
                // No text: a service message (member join/leave) or content
                // the pet does not read (photo, sticker, ...).
                normalize::normalize_service(msg)
                    .into_iter()
                    .map(|event| GroupEvent {
                        chat_id: chat_id.clone(),
                        event,
                    })
                    .collect()
            }
        }
        UpdateKind::EditedMessage(msg) => {
            // K3 fix: edit rows carry the EDIT date in the timestamp slot
            // (specs.md Section 4.2), so the edited path has its own entry
            // point. normalize_message would stamp the original send date.
            let Some(message) = normalize::normalize_edited_message(msg, bot) else {
                debug!("edited message without text; skipping");
                return Vec::new();
            };
            vec![GroupEvent {
                chat_id: normalize::chat_id_string(&msg.chat),
                event: InboundEvent::EditedMessage(message),
            }]
        }
        UpdateKind::MessageReaction(reaction) => vec![GroupEvent {
            chat_id: normalize::chat_id_string(&reaction.chat),
            event: InboundEvent::Reaction(normalize::normalize_reaction(reaction)),
        }],
        UpdateKind::MessageReactionCount(reaction) => vec![GroupEvent {
            chat_id: normalize::chat_id_string(&reaction.chat),
            event: InboundEvent::Reaction(normalize::normalize_reaction_count(reaction)),
        }],
        // Received (it is in allowed_updates) but ignored: no consumer yet.
        // Consistent with the actor's debug-only member handling.
        UpdateKind::ChatMember(_) => {
            debug!("chat member update ignored (no consumer yet)");
            Vec::new()
        }
        _ => {
            debug!("unhandled update kind; skipping");
            Vec::new()
        }
    }
}

/// Parses a string chat id (i64). Group ids are negative; the minus sign is
/// part of the string. A parse failure is `AdapterError::Sink`.
fn parse_chat_id(raw: &str) -> Result<ChatId, AdapterError> {
    raw.parse::<i64>()
        .map(ChatId)
        .map_err(|err| AdapterError::Sink(format!("invalid chat id {raw:?}: {err}")))
}

/// Parses a string message id (i32). A parse failure is `AdapterError::Sink`.
fn parse_message_id(raw: &str) -> Result<MessageId, AdapterError> {
    raw.parse::<i32>()
        .map(MessageId)
        .map_err(|err| AdapterError::Sink(format!("invalid message id {raw:?}: {err}")))
}

/// Maps an outbound request result to the adapter result.
///
/// specs.md Section 4.2: a denied outbound action (the bot lacks the
/// permission) is tolerated and logged, never fatal. The warn log carries
/// the chat id. Any other request error keeps the `AdapterError::Sink`
/// mapping.
fn outbound_result<T>(
    result: Result<T, RequestError>,
    chat_id: &str,
    action: &str,
) -> Result<(), AdapterError> {
    match result {
        Ok(_) => Ok(()),
        Err(err) if capability::is_permission_error(&err) => {
            warn!(chat_id = %chat_id, error = %err, "outbound action denied by Telegram (missing permission); continuing");
            Ok(())
        }
        Err(err) => Err(AdapterError::Sink(format!("{action} failed: {err}"))),
    }
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

    fn user_json(id: u64, first: &str) -> serde_json::Value {
        json!({ "id": id, "is_bot": false, "first_name": first })
    }

    fn group_json() -> serde_json::Value {
        json!({ "id": GROUP_ID, "type": "supergroup", "title": "Test Group" })
    }

    fn message_json(extra: serde_json::Value) -> serde_json::Value {
        let mut base = json!({
            "message_id": 1,
            "from": user_json(42, "Alice"),
            "chat": group_json(),
            "date": DATE,
        });
        base.as_object_mut()
            .expect("an object")
            .extend(extra.as_object().expect("an object").clone());
        base
    }

    fn update(extra: serde_json::Value) -> Update {
        let mut base = json!({ "update_id": 9 });
        base.as_object_mut()
            .expect("an object")
            .extend(extra.as_object().expect("an object").clone());
        // Note: `serde_json::from_value` does NOT work here. teloxide-core
        // 0.13.0 `types/update.rs` implements a custom Deserialize Visitor
        // for UpdateKind over `deserialize_any`; combined with the
        // `#[serde(flatten)]` on `Update::kind`, the Value deserializer
        // loses the keys and the kind becomes `UpdateKind::Error`. The
        // string round trip uses the real JSON deserializer, which is also
        // what the live path parses.
        let text = serde_json::to_string(&base).expect("json");
        serde_json::from_str(&text).expect("a valid Update")
    }

    #[test]
    fn parses_valid_ids() {
        assert_eq!(
            parse_chat_id("-1001234567890").expect("a valid chat id"),
            ChatId(GROUP_ID)
        );
        assert_eq!(
            parse_message_id("35").expect("a valid message id"),
            MessageId(35)
        );
    }

    #[test]
    fn an_invalid_chat_id_is_a_sink_error() {
        let err = parse_chat_id("not-a-chat").expect_err("a parse failure");
        assert!(matches!(err, AdapterError::Sink(_)));
        assert!(err.to_string().contains("not-a-chat"));
    }

    #[test]
    fn an_invalid_message_id_is_a_sink_error() {
        let err = parse_message_id("1.5").expect_err("a parse failure");
        assert!(matches!(err, AdapterError::Sink(_)));
    }

    #[test]
    fn a_text_message_gives_one_message_event_with_the_chat_id() {
        let upd = update(json!({ "message": message_json(json!({ "text": "hello" })) }));
        let events = events_of_update(&upd, &bot());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].chat_id, GROUP_ID.to_string());
        assert!(matches!(events[0].event, InboundEvent::Message(_)));
    }

    #[test]
    fn an_edited_message_gives_an_edited_message_event() {
        let upd = update(json!({
            "edited_message": message_json(json!({ "text": "edited", "edit_date": DATE + 5 })),
        }));
        let events = events_of_update(&upd, &bot());
        assert_eq!(events.len(), 1);
        match &events[0].event {
            // The edit update routes through normalize_edited_message: the
            // timestamp is the edit date, not the original send date.
            InboundEvent::EditedMessage(message) => {
                assert_eq!(
                    message.timestamp,
                    time::OffsetDateTime::from_unix_timestamp(DATE + 5).expect("valid timestamp")
                );
            }
            other => panic!("expected EditedMessage, got {other:?}"),
        }
    }

    #[test]
    fn a_photo_message_gives_no_events() {
        let upd = update(json!({
            "message": message_json(json!({
                "photo": [{
                    "file_id": "file-id",
                    "file_unique_id": "unique-id",
                    "width": 100,
                    "height": 100,
                }],
            })),
        }));
        assert!(events_of_update(&upd, &bot()).is_empty());
    }

    #[test]
    fn new_chat_members_give_ordered_events_with_the_same_chat_id() {
        let upd = update(json!({
            "message": message_json(json!({
                "new_chat_members": [user_json(10, "Carol"), user_json(11, "Dave")],
            })),
        }));
        let events = events_of_update(&upd, &bot());
        assert_eq!(events.len(), 2);
        for event in &events {
            assert_eq!(event.chat_id, GROUP_ID.to_string());
            assert!(matches!(event.event, InboundEvent::MemberJoin(_)));
        }
        let InboundEvent::MemberJoin(first) = &events[0].event else {
            panic!("a join")
        };
        let InboundEvent::MemberJoin(second) = &events[1].event else {
            panic!("a join")
        };
        assert_eq!(first.user_id, "10");
        assert_eq!(second.user_id, "11");
    }

    #[test]
    fn a_reaction_update_gives_a_reaction_event_with_the_chat_id() {
        let upd = update(json!({
            "message_reaction": {
                "chat": group_json(),
                "message_id": 35,
                "user": user_json(42, "Alice"),
                "date": DATE,
                "old_reaction": [],
                "new_reaction": [{ "type": "emoji", "emoji": "👍" }],
            },
        }));
        let events = events_of_update(&upd, &bot());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].chat_id, GROUP_ID.to_string());
        let InboundEvent::Reaction(reaction) = &events[0].event else {
            panic!("a reaction")
        };
        assert!(!reaction.aggregated);
    }

    #[test]
    fn a_reaction_count_update_gives_an_aggregated_reaction_event() {
        let upd = update(json!({
            "message_reaction_count": {
                "chat": group_json(),
                "message_id": 36,
                "date": DATE,
                "reactions": [{ "type": { "type": "emoji", "emoji": "🗿" }, "total_count": 2 }],
            },
        }));
        let events = events_of_update(&upd, &bot());
        assert_eq!(events.len(), 1);
        let InboundEvent::Reaction(reaction) = &events[0].event else {
            panic!("a reaction")
        };
        assert!(reaction.aggregated);
    }

    #[test]
    fn a_chat_member_update_is_ignored() {
        let upd = update(json!({
            "chat_member": {
                "chat": group_json(),
                "from": user_json(42, "Alice"),
                "date": DATE,
                "old_chat_member": { "status": "left", "user": user_json(10, "Carol") },
                "new_chat_member": { "status": "member", "user": user_json(10, "Carol") },
            },
        }));
        assert!(events_of_update(&upd, &bot()).is_empty());
    }

    /// An adapter with a dummy bot for offline `execute` tests. The dummy
    /// bot builds requests without network (verified in teloxide-core
    /// 0.13.0 `bot.rs`: `Bot::new` only builds a reqwest client).
    fn dummy_adapter() -> (TeloxideAdapter, mpsc::Sender<Result<Update, RequestError>>) {
        let (tx, rx) = mpsc::channel(8);
        let adapter = TeloxideAdapter {
            bot: Bot::new("dummy-token"),
            identity: bot(),
            pending: VecDeque::new(),
            updates: rx,
        };
        (adapter, tx)
    }

    #[tokio::test]
    async fn send_media_is_unsupported() {
        let (adapter, _tx) = dummy_adapter();
        let err = adapter
            .execute(OutboundAction::SendMedia {
                chat_id: GROUP_ID.to_string(),
                media_ref: "photo.png".to_string(),
                caption: None,
                reply_to_platform_msg_id: None,
            })
            .await
            .expect_err("SendMedia is unsupported");
        assert!(matches!(err, AdapterError::Unsupported(_)));
    }

    #[tokio::test]
    async fn send_text_with_an_invalid_chat_id_is_a_sink_error() {
        let (adapter, _tx) = dummy_adapter();
        let err = adapter
            .execute(OutboundAction::SendText {
                chat_id: "not-a-chat".to_string(),
                text: "hello".to_string(),
                reply_to_platform_msg_id: None,
            })
            .await
            .expect_err("a parse failure");
        assert!(matches!(err, AdapterError::Sink(_)));
    }

    #[tokio::test]
    async fn send_text_with_an_invalid_reply_id_is_a_sink_error() {
        let (adapter, _tx) = dummy_adapter();
        let err = adapter
            .execute(OutboundAction::SendText {
                chat_id: GROUP_ID.to_string(),
                text: "hello".to_string(),
                reply_to_platform_msg_id: Some("not-a-message".to_string()),
            })
            .await
            .expect_err("a parse failure");
        assert!(matches!(err, AdapterError::Sink(_)));
    }

    #[tokio::test]
    async fn react_with_an_invalid_message_id_is_a_sink_error() {
        let (adapter, _tx) = dummy_adapter();
        let err = adapter
            .execute(OutboundAction::React {
                chat_id: GROUP_ID.to_string(),
                platform_msg_id: "not-a-message".to_string(),
                emoji: "👍".to_string(),
            })
            .await
            .expect_err("a parse failure");
        assert!(matches!(err, AdapterError::Sink(_)));
    }

    #[tokio::test]
    async fn next_group_event_skips_empty_updates_buffers_and_ends() {
        let (mut adapter, tx) = dummy_adapter();
        // A stream error: logged and skipped.
        tx.send(Err(RequestError::Api(teloxide::ApiError::Unknown(
            "boom".to_string(),
        ))))
        .await
        .expect("the channel is open");
        // An update with no events: skipped.
        tx.send(Ok(update(json!({
            "message": message_json(json!({
                "photo": [{
                    "file_id": "file-id",
                    "file_unique_id": "unique-id",
                    "width": 100,
                    "height": 100,
                }],
            })),
        }))))
        .await
        .expect("the channel is open");
        // One update with two events.
        tx.send(Ok(update(json!({
            "message": message_json(json!({
                "new_chat_members": [user_json(10, "Carol"), user_json(11, "Dave")],
            })),
        }))))
        .await
        .expect("the channel is open");
        // Close the channel: the stream ended.
        drop(tx);

        let first = adapter
            .next_group_event()
            .await
            .expect("no error")
            .expect("an event");
        let second = adapter
            .next_group_event()
            .await
            .expect("no error")
            .expect("an event");
        assert!(matches!(first.event, InboundEvent::MemberJoin(_)));
        assert!(matches!(second.event, InboundEvent::MemberJoin(_)));
        assert_eq!(first.chat_id, second.chat_id);
        // The buffered events came from one update, in order.
        let InboundEvent::MemberJoin(first_join) = first.event else {
            panic!("a join")
        };
        let InboundEvent::MemberJoin(second_join) = second.event else {
            panic!("a join")
        };
        assert_eq!(first_join.user_id, "10");
        assert_eq!(second_join.user_id, "11");
        // The channel is closed: Ok(None).
        assert!(adapter
            .next_group_event()
            .await
            .expect("no error")
            .is_none());
    }

    #[tokio::test]
    async fn the_trait_view_returns_the_event_part() {
        let (mut adapter, tx) = dummy_adapter();
        tx.send(Ok(update(
            json!({ "message": message_json(json!({ "text": "hello" })) }),
        )))
        .await
        .expect("the channel is open");
        let event = adapter
            .next_event()
            .await
            .expect("no error")
            .expect("an event");
        assert!(matches!(event, InboundEvent::Message(_)));
    }
}
