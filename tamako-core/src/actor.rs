//! The per-group actor. Refer to specs.md Section 6.
//!
//! Phase 0 scope (dev-roadmap.md Section 2): inbox, trigger state,
//! persistence and rebuild of the session state. Message intake appends to
//! the raw log and updates the counters — no LLM call, no context
//! materialization, no wake or digest procedure. Those enter in Phase 1;
//! the trigger evaluation here only logs a stub message.

use std::sync::Arc;

use rand::rngs::StdRng;
use rand::SeedableRng;
use tamako_memory::{MemoryBackend, MemoryError};
use tamako_store::{Direction, EventType, InsertOutcome, NewMessage, Store, StoreError};
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::config::TriggerConfig;
use crate::event::{InboundEvent, NormalizedMessage};
use crate::session::{round_to_millis, SessionState};
use crate::trigger::WakeScheduler;

/// Errors of tamako-core.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("memory error: {0}")]
    Memory(#[from] MemoryError),
    #[error("actor task join error: {0}")]
    Join(String),
    #[error("actor inbox closed")]
    InboxClosed,
}

/// The default inbox capacity when the caller has no preference.
pub const DEFAULT_INBOX_CAPACITY: usize = 256;

/// Commands of the per-group actor inbox. specs.md Section 6.1, rule 1:
/// all trigger events enter one FIFO inbox.
pub enum ActorCommand {
    Inbound(InboundEvent),
    /// Evaluates the wake timer at the given instant. The Phase 0 demo and
    /// tests drive time explicitly; a real timer driver is Phase 1.
    Tick(OffsetDateTime),
    /// Returns a snapshot of the session state (tests, restart checks).
    Snapshot(oneshot::Sender<SessionState>),
    Shutdown,
}

/// The handle of a running group actor.
pub struct GroupActorHandle {
    chat_id: String,
    inbox: mpsc::Sender<ActorCommand>,
    join: JoinHandle<Result<(), CoreError>>,
}

impl GroupActorHandle {
    /// The group this actor serves.
    pub fn chat_id(&self) -> &str {
        &self.chat_id
    }

    /// Sends one command to the FIFO inbox.
    pub async fn send(&self, cmd: ActorCommand) -> Result<(), CoreError> {
        self.inbox
            .send(cmd)
            .await
            .map_err(|_| CoreError::InboxClosed)
    }

    /// Sends one inbound event to the FIFO inbox.
    pub async fn send_event(&self, ev: InboundEvent) -> Result<(), CoreError> {
        self.send(ActorCommand::Inbound(ev)).await
    }

    /// Returns a snapshot of the current session state.
    pub async fn snapshot(&self) -> Result<SessionState, CoreError> {
        let (tx, rx) = oneshot::channel();
        self.send(ActorCommand::Snapshot(tx)).await?;
        rx.await.map_err(|_| CoreError::InboxClosed)
    }

    /// Sends Shutdown and awaits the task. Graceful.
    ///
    /// A startup failure aborts the actor task before the loop runs. Such a
    /// failure surfaces here through the JoinHandle: the send fails (the
    /// receiver is gone) and the join returns the startup error.
    pub async fn shutdown(self) -> Result<(), CoreError> {
        // A failed send means the task already ended; the join below still
        // reports the outcome.
        let _ = self.inbox.send(ActorCommand::Shutdown).await;
        match self.join.await {
            Ok(result) => result,
            Err(error) => Err(CoreError::Join(error.to_string())),
        }
    }
}

/// Parameters of `spawn_group_actor`.
pub struct GroupActorParams<M: MemoryBackend> {
    pub chat_id: String,
    pub store: Arc<Store>,
    pub memory: Arc<M>,
    pub config: TriggerConfig,
    /// Start time of the process run. Used for fresh jitter and as the
    /// fallback "now" for a brand-new group.
    pub started_at: OffsetDateTime,
    /// Inbox capacity. Refer to `DEFAULT_INBOX_CAPACITY`.
    pub inbox_capacity: usize,
}

/// Spawns the actor task and returns the handle immediately.
///
/// Design note (one of the two options of the task spec): the startup
/// sequence runs INSIDE the spawned task. `spawn_group_actor` is
/// synchronous and never blocks the caller. A startup failure aborts the
/// task with the error; `GroupActorHandle::shutdown` surfaces it through
/// the JoinHandle. Callers that must know the startup outcome await one
/// `snapshot()` (FIFO order guarantees startup finished first) or call
/// `shutdown()`.
///
/// Startup sequence:
/// 1. `store.open_group` — creates `{data_root}/{chat_id}/store.db`
///    (Rule P5). Runs in `spawn_blocking` (AGENT.md Section 6.2).
/// 2. `memory.ensure_schema(chat_id)` — one graph file per group
///    (specs.md Section 5.1).
/// 3. `store.load_all_state` → `SessionState::decode` — the rebuild of
///    specs.md Section 6.1, rule 4.
///
/// The `MemoryBackend` trait promises `Send` futures, so the actor task
/// runs under any tokio runtime flavor.
pub fn spawn_group_actor<M: MemoryBackend + 'static>(
    params: GroupActorParams<M>,
) -> GroupActorHandle {
    let (tx, rx) = mpsc::channel(params.inbox_capacity);
    let chat_id = params.chat_id.clone();
    let join = tokio::spawn(run_actor(params, rx));
    GroupActorHandle {
        chat_id,
        inbox: tx,
        join,
    }
}

/// Maps a `JoinError` of a blocking store call into `CoreError`.
fn join_error(error: tokio::task::JoinError) -> CoreError {
    CoreError::Join(error.to_string())
}

/// Runs one blocking store call. AGENT.md Section 6.2: synchronous storage
/// calls never block the async runtime.
async fn blocking_store<T, F>(store: &Arc<Store>, call: F) -> Result<T, CoreError>
where
    T: Send + 'static,
    F: FnOnce(Arc<Store>) -> Result<T, StoreError> + Send + 'static,
{
    let store = Arc::clone(store);
    let outcome = tokio::task::spawn_blocking(move || call(store))
        .await
        .map_err(join_error)?;
    Ok(outcome?)
}

/// Builds the raw-log row for one normalized inbound message.
/// Rule A4: the normalized message carries every field the row needs.
fn to_new_message(msg: &NormalizedMessage, event_type: EventType) -> NewMessage {
    NewMessage {
        platform_msg_id: msg.platform_msg_id.clone(),
        direction: Direction::Inbound,
        event_type,
        timestamp: msg.timestamp,
        sender_id: msg.sender_id.clone(),
        sender_display_name: msg.sender_display_name.clone(),
        text: msg.text.clone(),
        reply_to_platform_msg_id: msg.reply_to_platform_msg_id.clone(),
        mentions_bot: msg.mentions_bot,
        is_reply_to_bot: msg.is_reply_to_bot,
    }
}

/// Persists the session state. specs.md Section 6.1, rule 4: the actor
/// persists the session state after every mutation.
async fn persist_session(
    store: &Arc<Store>,
    chat_id: &str,
    session: &SessionState,
) -> Result<(), CoreError> {
    let pairs = session.encode();
    let chat_id = chat_id.to_string();
    blocking_store(store, move |store| store.set_state_many(&chat_id, &pairs)).await
}

/// The actor task. Owns the session state and the live wake scheduler.
/// specs.md Section 6.1, rule 2: session-state mutations are strictly
/// serialized inside this loop.
async fn run_actor<M: MemoryBackend>(
    params: GroupActorParams<M>,
    mut inbox: mpsc::Receiver<ActorCommand>,
) -> Result<(), CoreError> {
    let GroupActorParams {
        chat_id,
        store,
        memory,
        config,
        started_at,
        ..
    } = params;

    // --- Startup (see the docstring of spawn_group_actor) ---
    let startup_chat_id = chat_id.clone();
    blocking_store(&store, move |store| store.open_group(&startup_chat_id)).await?;
    memory.ensure_schema(&chat_id).await?;
    let persisted_chat_id = chat_id.clone();
    let persisted = blocking_store(&store, move |store| {
        store.load_all_state(&persisted_chat_id)
    })
    .await?;

    // One RNG per actor task, seeded from entropy. Tests need deterministic
    // STATE, not deterministic jitter; encode/decode gives them the state.
    let mut rng = StdRng::from_rng(&mut rand::rng());
    let mut session = SessionState::decode(&persisted, &config, started_at, &mut rng);
    let mut wake = WakeScheduler::from_state(session.wake.clone());

    // --- Inbox loop ---
    while let Some(command) = inbox.recv().await {
        match command {
            ActorCommand::Inbound(InboundEvent::Message(msg)) => {
                handle_message(
                    &store,
                    &chat_id,
                    &config,
                    &mut session,
                    &mut wake,
                    &mut rng,
                    msg,
                )
                .await?;
            }
            ActorCommand::Inbound(InboundEvent::EditedMessage(msg)) => {
                // specs.md Section 15, open item 4: an edit is appended as a
                // new log row with event_type Edit. It is never a
                // retraction. No other processing in Phase 0.
                let row = to_new_message(&msg, EventType::Edit);
                let edit_chat_id = chat_id.clone();
                blocking_store(&store, move |store| {
                    store.insert_message(&edit_chat_id, &row)
                })
                .await?;
            }
            // Phase 0 stores message rows only. Reaction processing is
            // Phase 2 (warmup backoff, specs.md Section 8.5). Member events
            // have no Phase 0 consumer.
            ActorCommand::Inbound(
                event @ (InboundEvent::Reaction(_)
                | InboundEvent::MemberJoin(_)
                | InboundEvent::MemberLeave(_)),
            ) => {
                debug!(chat_id = %chat_id, event = ?event, "inbound event ignored in phase 0");
            }
            ActorCommand::Tick(now) => {
                if wake.should_fire(now, &config) {
                    info!(chat_id = %chat_id, "wake timer fired (stub)");
                    reset_wake(&config, &mut session, &mut wake, &mut rng, now);
                    persist_session(&store, &chat_id, &session).await?;
                }
            }
            ActorCommand::Snapshot(reply) => {
                // A dropped receiver means the caller went away. That is not
                // an actor failure.
                let _ = reply.send(session.clone());
            }
            ActorCommand::Shutdown => break,
        }
    }
    Ok(())
}

/// Message intake. specs.md Section 8.1.
async fn handle_message(
    store: &Arc<Store>,
    chat_id: &str,
    config: &TriggerConfig,
    session: &mut SessionState,
    wake: &mut WakeScheduler,
    rng: &mut StdRng,
    msg: NormalizedMessage,
) -> Result<(), CoreError> {
    // Rule P1 (specs.md Section 8.1): FIRST persist to the raw log. The
    // insert completes before anything else runs.
    let row = to_new_message(&msg, EventType::Message);
    let intake_chat_id = chat_id.to_string();
    let outcome = blocking_store(store, move |store| {
        store.insert_message(&intake_chat_id, &row)
    })
    .await?;
    if outcome == InsertOutcome::Duplicate {
        // Idempotent intake (AGENT.md Section 6.2): a replay after a crash
        // can re-see a message. The row exists already; the counters below
        // still update, so the wake counter can count one delivery twice.
        // The log row — the source of truth — is not duplicated.
        debug!(chat_id = %chat_id, platform_msg_id = %msg.platform_msg_id, "duplicate delivery");
    }

    // Update the session in memory.
    session.record_human_message();
    wake.record_message();
    session.wake = wake.snapshot();
    persist_session(store, chat_id, session).await?;

    // Trigger evaluation. The procedures are Phase 1 stubs; only the
    // scheduling runs here. `msg.timestamp` is `now`: deterministic replay.
    let now = msg.timestamp;
    if msg.mentions_bot || msg.is_reply_to_bot {
        // specs.md Section 8.1: the bot must respond when addressed
        // directly. The muted state does not suppress a forced wake.
        info!(chat_id = %chat_id, "forced wake requested (stub)");
    } else if wake.should_fire(now, config) {
        info!(chat_id = %chat_id, "wake trigger fired (stub)");
        reset_wake(config, session, wake, rng, now);
        persist_session(store, chat_id, session).await?;
    }
    Ok(())
}

/// Resets the wake scheduler and mirrors the result into the session.
/// The fresh interval is normalized to whole milliseconds so the persisted
/// encoding stays lossless (refer to `round_to_millis`).
fn reset_wake(
    config: &TriggerConfig,
    session: &mut SessionState,
    wake: &mut WakeScheduler,
    rng: &mut StdRng,
    now: OffsetDateTime,
) {
    wake.reset(now, config, rng);
    let mut snapshot = wake.snapshot();
    snapshot.current_interval = round_to_millis(snapshot.current_interval);
    *wake = WakeScheduler::from_state(snapshot.clone());
    session.wake = snapshot;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use tamako_memory::MemoryBatch;
    use tempfile::TempDir;

    /// A memory backend double. All calls succeed; `ensure_schema` records
    /// the chat_id values it receives.
    struct NoopMemory {
        ensured: Mutex<Vec<String>>,
    }

    impl NoopMemory {
        fn new() -> Self {
            Self {
                ensured: Mutex::new(Vec::new()),
            }
        }

        fn ensured_chat_ids(&self) -> Vec<String> {
            self.ensured
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl MemoryBackend for NoopMemory {
        async fn ensure_schema(&self, chat_id: &str) -> tamako_memory::Result<()> {
            self.ensured
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(chat_id.to_string());
            Ok(())
        }

        async fn upsert_batch(
            &self,
            _chat_id: &str,
            _batch: &MemoryBatch,
        ) -> tamako_memory::Result<()> {
            Ok(())
        }

        async fn checkpoint(&self, _chat_id: &str) -> tamako_memory::Result<()> {
            Ok(())
        }

        async fn close(&self, _chat_id: &str) -> tamako_memory::Result<()> {
            Ok(())
        }
    }

    const CHAT_ID: &str = "-1001234567890";

    /// Fixed base time for deterministic replay.
    fn t0() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("a valid unix timestamp")
    }

    fn message(id: &str, seconds_after_t0: i64, mentions_bot: bool) -> NormalizedMessage {
        NormalizedMessage {
            platform_msg_id: id.to_string(),
            timestamp: t0() + time::Duration::seconds(seconds_after_t0),
            sender_id: "u1".to_string(),
            sender_display_name: "Alice".to_string(),
            text: format!("text of {id}"),
            reply_to_platform_msg_id: None,
            mentions_bot,
            is_reply_to_bot: false,
        }
    }

    struct Fixture {
        // The TempDir must outlive the store.
        _dir: TempDir,
        store: Arc<Store>,
        memory: Arc<NoopMemory>,
    }

    fn make_fixture() -> Fixture {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let store = Arc::new(Store::new(dir.path().join("data")));
        let memory = Arc::new(NoopMemory::new());
        Fixture {
            _dir: dir,
            store,
            memory,
        }
    }

    fn spawn_on(fixture: &Fixture, config: TriggerConfig) -> GroupActorHandle {
        spawn_group_actor(GroupActorParams {
            chat_id: CHAT_ID.to_string(),
            store: Arc::clone(&fixture.store),
            memory: Arc::clone(&fixture.memory),
            config,
            started_at: t0(),
            inbox_capacity: DEFAULT_INBOX_CAPACITY,
        })
    }

    fn spawn_fixture(config: TriggerConfig) -> (Fixture, GroupActorHandle) {
        let fixture = make_fixture();
        let handle = spawn_on(&fixture, config);
        (fixture, handle)
    }

    /// Lists the raw log through a blocking call, like the actor does.
    async fn list_messages(store: &Arc<Store>) -> Vec<tamako_store::MessageRow> {
        let store = Arc::clone(store);
        tokio::task::spawn_blocking(move || store.list_messages(CHAT_ID))
            .await
            .expect("the blocking task joins")
            .expect("list_messages succeeds")
    }

    #[tokio::test]
    async fn intake_persists_the_message_before_anything_else() {
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        handle
            .send_event(InboundEvent::Message(message("m2", 2, false)))
            .await
            .expect("send succeeds");
        // A snapshot is a FIFO barrier: when it returns, both messages are
        // processed.
        handle.snapshot().await.expect("snapshot succeeds");

        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].platform_msg_id, "m1");
        assert_eq!(rows[1].platform_msg_id, "m2");
        for row in &rows {
            assert_eq!(row.direction, Direction::Inbound);
            assert_eq!(row.event_type, EventType::Message);
        }
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn duplicate_delivery_does_not_double_count() {
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        for _ in 0..2 {
            handle
                .send_event(InboundEvent::Message(message("m1", 1, false)))
                .await
                .expect("send succeeds");
        }
        let session = handle.snapshot().await.expect("snapshot succeeds");

        // Idempotent intake: the raw log holds ONE row.
        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 1);
        // Documented choice: the wake counter reflects both deliveries. The
        // counter is a scheduling hint; the log row is the source of truth.
        assert_eq!(session.wake.msgs_since_wake, 2);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn edit_appends_a_new_row() {
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        handle
            .send_event(InboundEvent::Message(message("m1", 1, false)))
            .await
            .expect("send succeeds");
        let mut edited = message("m1", 1, false);
        edited.text = "edited text".to_string();
        handle
            .send_event(InboundEvent::EditedMessage(edited))
            .await
            .expect("send succeeds");
        handle.snapshot().await.expect("snapshot succeeds");

        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].event_type, EventType::Message);
        assert_eq!(rows[1].event_type, EventType::Edit);
        assert_eq!(rows[1].text, "edited text");
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn restart_rebuilds_identical_state() {
        // The Phase 0 exit criterion (dev-roadmap.md Section 2).
        let config = TriggerConfig::default();
        let (fixture, handle) = spawn_fixture(config.clone());
        for index in 1..=3 {
            handle
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
        let first = handle.snapshot().await.expect("snapshot succeeds");
        handle.shutdown().await.expect("shutdown succeeds");

        // A NEW actor on the same store and the same started_at.
        let restarted = spawn_group_actor(GroupActorParams {
            chat_id: CHAT_ID.to_string(),
            store: Arc::clone(&fixture.store),
            memory: Arc::new(NoopMemory::new()),
            config,
            started_at: t0(),
            inbox_capacity: DEFAULT_INBOX_CAPACITY,
        });
        let rebuilt = restarted.snapshot().await.expect("snapshot succeeds");
        assert_eq!(first, rebuilt);

        // The rebuilt actor keeps working on the same log.
        for index in 4..=5 {
            restarted
                .send_event(InboundEvent::Message(message(
                    &format!("m{index}"),
                    index,
                    false,
                )))
                .await
                .expect("send succeeds");
        }
        let advanced = restarted.snapshot().await.expect("snapshot succeeds");
        let rows = list_messages(&fixture.store).await;
        assert_eq!(rows.len(), 5);
        assert!(advanced.wake.msgs_since_wake > rebuilt.wake.msgs_since_wake);
        restarted.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn forced_wake_does_not_panic_when_muted() {
        let config = TriggerConfig {
            monologue_limit: 2,
            ..TriggerConfig::default()
        };
        let fixture = make_fixture();

        // Craft the muted state: monologue_limit consecutive bot messages
        // engage the monologue lock (specs.md Section 8.5). Seed the store
        // BEFORE the actor spawns: a concurrent open_group of the same
        // store.db races the WAL pragma of the actor startup.
        let mut rng = StdRng::seed_from_u64(23);
        let mut seeded = SessionState::new(&config, t0(), &mut rng);
        seeded.record_bot_message(&config);
        seeded.record_bot_message(&config);
        assert!(seeded.muted);
        let pairs = seeded.encode();
        let store = Arc::clone(&fixture.store);
        tokio::task::spawn_blocking(move || {
            store.open_group(CHAT_ID)?;
            store.set_state_many(CHAT_ID, &pairs)
        })
        .await
        .expect("the blocking task joins")
        .expect("seeding succeeds");

        // The actor rebuilds the seeded (muted) state at startup.
        let handle = spawn_on(&fixture, config.clone());

        // A direct mention requests a forced wake. specs.md Section 8.1:
        // the muted state does not suppress a forced wake.
        handle
            .send_event(InboundEvent::Message(message("m1", 1, true)))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");

        // The human message clears the monologue lock (Section 8.5), and
        // the session is persisted.
        assert!(!session.muted);
        let persisted = {
            let store = Arc::clone(&fixture.store);
            tokio::task::spawn_blocking(move || store.load_all_state(CHAT_ID))
                .await
                .expect("the blocking task joins")
                .expect("load_all_state succeeds")
        };
        let persisted_map: std::collections::HashMap<String, String> =
            session.encode().into_iter().collect();
        assert_eq!(persisted, persisted_map);
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn tick_fires_the_timer() {
        let config = TriggerConfig {
            wake_interval: std::time::Duration::from_millis(10),
            wake_floor: std::time::Duration::ZERO,
            ..TriggerConfig::default()
        };
        let (_fixture, handle) = spawn_fixture(config);
        handle
            .send(ActorCommand::Tick(t0() + time::Duration::hours(1)))
            .await
            .expect("send succeeds");
        let session = handle.snapshot().await.expect("snapshot succeeds");

        assert_eq!(session.wake.msgs_since_wake, 0);
        assert_eq!(session.wake.last_wake_at, t0() + time::Duration::hours(1));
        handle.shutdown().await.expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn startup_ensures_the_memory_schema_for_the_group() {
        let (fixture, handle) = spawn_fixture(TriggerConfig::default());
        // The snapshot is a FIFO barrier: startup completed before it.
        handle.snapshot().await.expect("snapshot succeeds");
        assert_eq!(fixture.memory.ensured_chat_ids(), vec![CHAT_ID.to_string()]);
        handle.shutdown().await.expect("shutdown succeeds");
    }
}
