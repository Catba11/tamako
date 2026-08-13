//! End-to-end tests of the M4 wake procedure (specs.md Section 9)
//! against the REAL SQLite store, the real actor (`spawn_group_actor`),
//! and scripted gate/reply doubles (tamako-agent). The outbound channel
//! of the binary is reproduced as a forwarder task that drains the
//! channel into an `Arc<MockAdapter>` sink (`execute` takes `&self`).
//! Env-hermetic: scripted doubles only; no LLM keys, no network.
//!
//! Timer note: every scenario drives the actor with explicit events and
//! explicit `Tick` commands. The built-in 1-second ticker of the actor
//! (the zero-cadence guard) is kept inert by configuration: scenarios
//! use either a near-infinite `wake_interval` (the interval path never
//! fires) or message timestamps in the FUTURE relative to the real
//! clock (a tick's elapsed time is then negative and the floor check of
//! specs.md Section 8.3 blocks the fire).

use std::sync::Arc;
use std::time::Duration;

use tamako_adapter_mock::MockAdapter;
use tamako_agent::{ScriptedGate, ScriptedReplyGenerator};
use tamako_core::actor::{
    spawn_group_actor, ActorCommand, GroupActorHandle, GroupActorParams, DEFAULT_INBOX_CAPACITY,
};
use tamako_core::adapter::PlatformAdapter;
use tamako_core::config::TriggerConfig;
use tamako_core::context::{render_bot_content, ContextItemKind};
use tamako_core::event::{InboundEvent, NormalizedMessage, OutboundAction};
use tamako_core::session::SessionState;
use tamako_core::wake::{GateDecision, NoopRecall, WakeServices};
use tamako_memory::{AliasTarget, MemoryBackend, MemoryBatch};
use tamako_store::{Direction, MessageRow, Store};
use time::OffsetDateTime;
use tokio::sync::mpsc;

const CHAT_ID: &str = "-1001234567890";

/// The bounded waits of this suite. The scripted doubles return
/// immediately, so a wake round trip is milliseconds; five seconds is
/// generous enough for a loaded CI machine.
const WAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// The poll interval of every bounded wait.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// A near-infinite wake interval: the interval path of the trigger
/// never fires, so only the message count and explicit ticks drive
/// wakes. `wake_interval * 1.3` (the jitter upper bound) still fits.
const HUGE_INTERVAL: Duration = Duration::from_secs(u64::MAX / 4);

/// The replay fixture of the mock adapter crate (14 events, one group).
fn replay_fixture_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../tamako-adapter-mock/fixtures/replay_chat.json")
}

/// A memory backend double. All calls succeed; nothing is recorded
/// (the wake procedure never touches the graph in M4).
struct NoopMemory;

impl MemoryBackend for NoopMemory {
    async fn ensure_schema(&self, _chat_id: &str) -> tamako_memory::Result<()> {
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

    async fn alias_targets(
        &self,
        _chat_id: &str,
        _alias_node_id: &str,
    ) -> tamako_memory::Result<Vec<AliasTarget>> {
        Ok(vec![])
    }

    async fn neighbors(
        &self,
        _chat_id: &str,
        _node_id: &str,
    ) -> tamako_memory::Result<Vec<tamako_memory::NeighborEdge>> {
        Ok(vec![])
    }

    async fn close(&self, _chat_id: &str) -> tamako_memory::Result<()> {
        Ok(())
    }
}

struct Fixture {
    // The TempDir must outlive the store.
    _dir: tempfile::TempDir,
    store: Arc<Store>,
    memory: Arc<NoopMemory>,
}

fn make_fixture() -> Fixture {
    let dir = tempfile::tempdir().expect("a temporary data root");
    Fixture {
        store: Arc::new(Store::new(dir.path().to_path_buf())),
        memory: Arc::new(NoopMemory),
        _dir: dir,
    }
}

/// The test rig: the real actor plus the outbound plumbing of the
/// binary (one channel, one pump into the platform adapter).
struct Harness {
    handle: GroupActorHandle,
    sink: Arc<MockAdapter>,
    gate: Arc<ScriptedGate>,
    reply: Arc<ScriptedReplyGenerator>,
    outbound_tx: mpsc::Sender<OutboundAction>,
    forwarder: tokio::task::JoinHandle<()>,
}

/// Spawns the real actor with scripted wake doubles and wires the
/// outbound channel into a fresh mock-adapter sink (empty event queue;
/// it only records).
fn spawn_on(
    fixture: &Fixture,
    config: TriggerConfig,
    started_at: OffsetDateTime,
    gate: Arc<ScriptedGate>,
    reply: Arc<ScriptedReplyGenerator>,
) -> Harness {
    let (outbound_tx, outbound_rx) = mpsc::channel::<OutboundAction>(100);
    // Open the group store BEFORE the actor spawns: a concurrent
    // `open_group` of the same store.db races the WAL pragma of the
    // actor startup. With the database already in WAL mode, the actor's
    // own `open_group` pragma is a no-op.
    fixture
        .store
        .open_group(CHAT_ID)
        .expect("open_group succeeds");
    // The sink shares the fixture chat id; its event queue is empty
    // (the actor is fed directly or through a second source adapter).
    let sink = Arc::new(MockAdapter::from_fixture(
        tamako_adapter_mock::ReplayFixture {
            chat_id: CHAT_ID.to_string(),
            events: vec![],
        },
    ));
    let forwarder = {
        let sink = Arc::clone(&sink);
        tokio::spawn(async move {
            let mut outbound_rx = outbound_rx;
            while let Some(action) = outbound_rx.recv().await {
                sink.execute(action)
                    .await
                    .expect("the mock adapter never fails");
            }
        })
    };
    let handle = spawn_group_actor(GroupActorParams {
        chat_id: CHAT_ID.to_string(),
        store: Arc::clone(&fixture.store),
        memory: Arc::clone(&fixture.memory),
        config,
        started_at,
        inbox_capacity: DEFAULT_INBOX_CAPACITY,
        preamble: "test preamble".to_string(),
        digest: None,
        post_digest_hook: None,
        wake: Some(WakeServices {
            recall: Arc::new(NoopRecall),
            gate: Arc::clone(&gate) as Arc<dyn tamako_core::wake::ParticipationGate>,
            reply: Arc::clone(&reply) as Arc<dyn tamako_core::wake::ReplyGenerator>,
        }),
        summary_provider: None,
        outbound: Some(outbound_tx.clone()),
        bot_name: Some("Tamako".to_string()),
    });
    Harness {
        handle,
        sink,
        gate,
        reply,
        outbound_tx,
        forwarder,
    }
}

/// Graceful shutdown of every spawned task of the rig.
async fn shutdown(harness: Harness) {
    harness
        .handle
        .shutdown()
        .await
        .expect("the actor reports no error");
    // Closing the last sender ends the forwarder loop.
    drop(harness.outbound_tx);
    harness.forwarder.await.expect("the forwarder joins");
}

/// Polls the sink until at least `min` outbound actions are recorded or
/// the timeout elapses. The forwarder records an action only after the
/// actor's send path persisted the outbound raw-log row (Rule B1), so
/// this wait also orders the raw-log assertions.
async fn wait_for_actions(sink: &Arc<MockAdapter>, min: usize) -> Vec<OutboundAction> {
    let deadline = std::time::Instant::now() + WAKE_TIMEOUT;
    loop {
        let actions = sink.recorded_actions();
        if actions.len() >= min {
            return actions;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {WAKE_TIMEOUT:?} waiting for {min} outbound actions (got {})",
            actions.len()
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Polls a state-table counter until it reaches `expected` or the
/// timeout elapses. The wake counters are best effort, but on this
/// harness every increment succeeds.
async fn wait_for_counter(store: &Arc<Store>, key: &str, expected: &str) {
    let deadline = std::time::Instant::now() + WAKE_TIMEOUT;
    loop {
        let value = counter(store, key).await;
        if value.as_deref() == Some(expected) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {WAKE_TIMEOUT:?} waiting for counter {key} = {expected} (got {value:?})"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Polls the session snapshot until `predicate` holds or the timeout
/// elapses. Every snapshot is a FIFO barrier over the caller's own
/// commands.
async fn wait_for_session(
    handle: &GroupActorHandle,
    predicate: impl Fn(&SessionState) -> bool,
    what: &str,
) -> SessionState {
    let deadline = std::time::Instant::now() + WAKE_TIMEOUT;
    loop {
        let session = handle.snapshot().await.expect("the snapshot succeeds");
        if predicate(&session) {
            return session;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {WAKE_TIMEOUT:?} waiting for session condition: {what}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn counter(store: &Arc<Store>, key: &str) -> Option<String> {
    let store = Arc::clone(store);
    let key = key.to_string();
    tokio::task::spawn_blocking(move || store.get_state(CHAT_ID, &key))
        .await
        .expect("the blocking task joins")
        .expect("get_state succeeds")
}

/// All raw-log rows of one direction.
async fn rows_of_direction(store: &Arc<Store>, direction: Direction) -> Vec<MessageRow> {
    let store = Arc::clone(store);
    tokio::task::spawn_blocking(move || store.list_messages(CHAT_ID))
        .await
        .expect("the blocking task joins")
        .expect("list_messages succeeds")
        .into_iter()
        .filter(|row| row.direction == direction)
        .collect()
}

/// The outbound actions of the sink as `SendText` triples.
fn send_texts(actions: &[OutboundAction]) -> Vec<(String, String, Option<String>)> {
    actions
        .iter()
        .map(|action| match action {
            OutboundAction::SendText {
                chat_id,
                text,
                reply_to_platform_msg_id,
            } => (
                chat_id.clone(),
                text.clone(),
                reply_to_platform_msg_id.clone(),
            ),
            other => panic!("expected SendText, got {other:?}"),
        })
        .collect()
}

/// A plain or forcing message with a fixed sender and timestamp.
fn message(id: &str, at: OffsetDateTime, mention: bool) -> NormalizedMessage {
    NormalizedMessage {
        platform_msg_id: id.to_string(),
        timestamp: at,
        sender_id: "u1".to_string(),
        sender_display_name: "Alice".to_string(),
        username: None,
        text: format!("text of {id}"),
        reply_to_platform_msg_id: None,
        mentions_bot: mention,
        is_reply_to_bot: false,
    }
}

/// Scenario A: threshold wake over the replay fixture, end to end
/// (specs.md Sections 8.1, 8.3, 9). The pacing waits for each send
/// before feeding more events; that makes the raw-log row ids
/// deterministic:
///
/// - rows 1-4: messages 41, 43, 44, 45 (45 is a mention, forced wake);
/// - row 5: the outbound row of reply r1 (Rule B1);
/// - rows 6-8: messages 46, 47, 48 (48 is a reply to the bot, forced);
/// - row 9: the outbound row of reply r2;
/// - rows 10-12: messages 49, 50, 51; message 51 completes the count
///   threshold (wake_msg_count = 3) and the gate targets row 12;
/// - row 13: the trailing edit.
///
/// The unforced count fire at message 44 stays blocked: `started_at`
/// is one day in the future, so the elapsed time of the Section 8.3
/// floor check is negative until the first forced wake resets the
/// scheduler onto the replay clock. The same trick keeps the built-in
/// 1-second ticker inert before the first wake; afterwards the count
/// is always below the threshold between paced segments.
#[tokio::test]
async fn threshold_wake_over_the_replay_fixture_end_to_end() {
    let fixture = make_fixture();
    let config = TriggerConfig {
        wake_msg_count: 3,
        wake_floor: Duration::ZERO,
        wake_interval: HUGE_INTERVAL,
        ..TriggerConfig::default()
    };
    let gate = Arc::new(ScriptedGate::with_decisions(vec![GateDecision {
        participate: true,
        target_row_id: Some(12),
        reason: Some("a question the pet can answer".to_string()),
    }]));
    let reply = Arc::new(ScriptedReplyGenerator::with_replies(vec![
        "r1".to_string(),
        "r2".to_string(),
        "r3".to_string(),
    ]));
    let started_at = OffsetDateTime::now_utc() + Duration::from_secs(24 * 60 * 60);
    let harness = spawn_on(&fixture, config, started_at, gate, reply);

    // Feed the fixture through a second mock adapter as the source.
    let mut source =
        MockAdapter::from_fixture_path(&replay_fixture_path()).expect("the shipped fixture loads");
    let mut expected_actions = 0_usize;
    while let Some(event) = source.next_event().await.expect("the replay source works") {
        // The forcing messages and the threshold message each complete
        // one wake with one send; pace the feed on those.
        let wake_send = matches!(
            &event,
            InboundEvent::Message(msg)
                if matches!(msg.platform_msg_id.as_str(), "45" | "48" | "51")
        );
        if wake_send {
            expected_actions += 1;
        }
        harness
            .handle
            .send_event(event)
            .await
            .expect("the actor inbox is open");
        if wake_send {
            wait_for_actions(&harness.sink, expected_actions).await;
        }
    }
    // FIFO barrier: the reaction, the edit, and the leave are processed.
    harness
        .handle
        .snapshot()
        .await
        .expect("the snapshot succeeds");
    wait_for_counter(&fixture.store, "wakes_total", "3").await;
    wait_for_counter(&fixture.store, "participations_total", "3").await;

    // Three SendText actions in order, each a reply to its target.
    let sends = send_texts(&harness.sink.recorded_actions());
    assert_eq!(
        sends,
        vec![
            (
                CHAT_ID.to_string(),
                "r1".to_string(),
                Some("45".to_string())
            ),
            (
                CHAT_ID.to_string(),
                "r2".to_string(),
                Some("48".to_string())
            ),
            (
                CHAT_ID.to_string(),
                "r3".to_string(),
                Some("51".to_string())
            ),
        ]
    );
    // Rule B1: three outbound rows in the raw log, texts r1/r2/r3.
    let outbound = rows_of_direction(&fixture.store, Direction::Outbound).await;
    assert_eq!(outbound.len(), 3);
    for (row, (text, reply_to)) in outbound
        .iter()
        .zip(["r1", "r2", "r3"].iter().zip(["45", "48", "51"].iter()))
    {
        assert_eq!(&row.text, text);
        assert_eq!(row.reply_to_platform_msg_id.as_deref(), Some(*reply_to));
        assert_eq!(row.sender_display_name, "Tamako");
    }
    // Rule C1: the context contains the three bot speeches.
    let context = harness
        .handle
        .context_snapshot()
        .await
        .expect("the context snapshot succeeds");
    let bot_speeches: Vec<&str> = context
        .iter()
        .filter(|item| item.kind == ContextItemKind::BotSpeech)
        .map(|item| item.content.as_str())
        .collect();
    let expected: Vec<String> = outbound
        .iter()
        .map(|row| render_bot_content(row.id, row.timestamp, &row.text))
        .collect();
    assert_eq!(bot_speeches, expected);
    // The gate ran exactly once (the two forced wakes bypass it,
    // Section 8.1), over the new messages of the threshold wake.
    let inputs = harness.gate.inputs();
    assert_eq!(inputs.len(), 1);
    assert_eq!(
        inputs[0]
            .new_messages
            .iter()
            .map(|msg| msg.row_id)
            .collect::<Vec<_>>(),
        vec![10, 11, 12]
    );
    assert!(!inputs[0].forced);
    shutdown(harness).await;
}

/// Scenario B: the gate says no — nothing is sent and the participation
/// counter is unchanged (specs.md Section 9.6).
#[tokio::test]
async fn gate_no_sends_nothing() {
    let fixture = make_fixture();
    let t0 = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("a valid timestamp");
    let config = TriggerConfig {
        wake_msg_count: 3,
        wake_floor: Duration::ZERO,
        wake_interval: HUGE_INTERVAL,
        ..TriggerConfig::default()
    };
    let gate = Arc::new(ScriptedGate::with_decisions(vec![GateDecision {
        participate: false,
        target_row_id: None,
        reason: Some("nothing to add".to_string()),
    }]));
    let reply = Arc::new(ScriptedReplyGenerator::failing(
        "the reply model must not be called on a no",
    ));
    let harness = spawn_on(&fixture, config, t0, gate, reply);
    for (index, id) in ["b1", "b2", "b3"].iter().enumerate() {
        harness
            .handle
            .send_event(InboundEvent::Message(message(
                id,
                t0 + time::Duration::seconds(index as i64 + 1),
                false,
            )))
            .await
            .expect("the actor inbox is open");
    }
    wait_for_counter(&fixture.store, "wakes_total", "1").await;
    // The gate ran and said no; the barrier orders the completion
    // handler (which does nothing on a no) before the assertions.
    assert_eq!(harness.gate.inputs().len(), 1);
    harness
        .handle
        .snapshot()
        .await
        .expect("the snapshot succeeds");
    assert!(
        harness.sink.recorded_actions().is_empty(),
        "a gate-no wake sends nothing"
    );
    assert!(
        rows_of_direction(&fixture.store, Direction::Outbound)
            .await
            .is_empty(),
        "a gate-no wake writes no outbound row"
    );
    assert_eq!(counter(&fixture.store, "participations_total").await, None);
    shutdown(harness).await;
}

/// Scenario C: a forced wake bypasses the gate even when the group is
/// muted (specs.md Sections 8.1 and 8.5). The muted session state is
/// seeded BEFORE the actor spawns (a concurrent `open_group` of the
/// same store.db would race the WAL pragma of the actor startup).
#[tokio::test]
async fn forced_wake_bypasses_the_gate_when_muted() {
    let fixture = make_fixture();
    let t0 = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("a valid timestamp");
    {
        let store = Arc::clone(&fixture.store);
        tokio::task::spawn_blocking(move || {
            store.open_group(CHAT_ID)?;
            store.set_state_many(
                CHAT_ID,
                &[
                    ("muted_flag".to_string(), "1".to_string()),
                    ("consecutive_bot_msgs".to_string(), "2".to_string()),
                ],
            )
        })
        .await
        .expect("the blocking task joins")
        .expect("seeding succeeds");
    }
    let config = TriggerConfig {
        wake_interval: HUGE_INTERVAL,
        ..TriggerConfig::default()
    };
    // If the gate is ever called, the wake fails and no reply arrives:
    // the scripted failure is a strong signal for the bypass.
    let gate = Arc::new(ScriptedGate::failing(
        "the gate must not be called for a forced wake",
    ));
    let reply = Arc::new(ScriptedReplyGenerator::with_replies(vec!["r1".to_string()]));
    let harness = spawn_on(&fixture, config, t0, gate, reply);
    harness
        .handle
        .send_event(InboundEvent::Message(message(
            "c1",
            t0 + time::Duration::seconds(1),
            true,
        )))
        .await
        .expect("the actor inbox is open");
    let actions = wait_for_actions(&harness.sink, 1).await;
    let sends = send_texts(&actions);
    assert_eq!(
        sends,
        vec![(
            CHAT_ID.to_string(),
            "r1".to_string(),
            Some("c1".to_string())
        )]
    );
    assert!(
        harness.gate.inputs().is_empty(),
        "the forced wake bypassed the gate (Section 8.1)"
    );
    wait_for_counter(&fixture.store, "wakes_total", "1").await;
    wait_for_counter(&fixture.store, "participations_total", "1").await;
    // The mention intake cleared the monologue lock (Section 8.5: any
    // human message clears it).
    let session = harness
        .handle
        .snapshot()
        .await
        .expect("the snapshot succeeds");
    assert!(!session.muted);
    shutdown(harness).await;
}

/// Scenario D: the monologue lock in live operation (specs.md
/// Section 8.5). Two back-to-back mentions produce two consecutive bot
/// speeches with no human message in between (the second mention is
/// intaken while the first wake is in flight, so it queues as the
/// forced wake of Section 6.2), and the lock engages. The suppressed
/// wake must be tick-driven: any human message clears the lock at
/// intake, so a message-driven wake can never observe `muted`.
///
/// All timestamps are one day in the future: the built-in 1-second
/// ticker then always sees a negative elapsed time and stays inert;
/// only the explicit `Tick` drives the suppressed wake.
#[tokio::test]
async fn monologue_lock_suppresses_unforced_wakes_until_a_human_message() {
    let fixture = make_fixture();
    let t0 = OffsetDateTime::now_utc() + Duration::from_secs(24 * 60 * 60);
    let config = TriggerConfig {
        wake_msg_count: 3,
        wake_floor: Duration::ZERO,
        wake_interval: Duration::from_secs(60),
        // A fixed jitter makes the current interval exactly 60 s.
        wake_jitter_min: 1.0,
        wake_jitter_max: 1.0,
        ..TriggerConfig::default()
    };
    // Rows: m1=1, m2=2 (both intaken before the first completion),
    // r1=3, r2=4 (outbound rows, Rule B1), m3=5, m4=6, m5=7. The final
    // wake sees the new messages 5, 6, 7 and targets row 7.
    let gate = Arc::new(ScriptedGate::with_decisions(vec![GateDecision {
        participate: true,
        target_row_id: Some(7),
        reason: None,
    }]));
    let reply = Arc::new(ScriptedReplyGenerator::with_replies(vec![
        "r1".to_string(),
        "r2".to_string(),
        "r3".to_string(),
    ]));
    let harness = spawn_on(&fixture, config, t0, gate, reply);
    // Back-to-back mentions: two forced wakes, two consecutive bot
    // speeches, no human message in between.
    for (id, seconds) in [("d1", 1), ("d2", 2)] {
        harness
            .handle
            .send_event(InboundEvent::Message(message(
                id,
                t0 + time::Duration::seconds(seconds),
                true,
            )))
            .await
            .expect("the actor inbox is open");
    }
    wait_for_actions(&harness.sink, 2).await;
    wait_for_session(
        &harness.handle,
        |session| session.muted,
        "muted after two bot speeches",
    )
    .await;

    // The muted unforced wake must be tick-driven. The interval (60 s)
    // has elapsed at t0 + 120 s; the count is zero after the last wake.
    harness
        .handle
        .send(ActorCommand::Tick(t0 + time::Duration::seconds(120)))
        .await
        .expect("the actor inbox is open");
    let session = wait_for_session(
        &harness.handle,
        |_| true,
        "the tick is processed (FIFO barrier)",
    )
    .await;
    assert!(session.muted, "the suppressed wake does not clear the lock");
    assert_eq!(
        harness.sink.recorded_actions().len(),
        2,
        "the muted wake sent nothing"
    );
    assert!(
        harness.gate.inputs().is_empty(),
        "the suppressed wake never reaches the gate (forced wakes bypass it)"
    );
    wait_for_counter(&fixture.store, "wakes_total", "3").await;

    // A human message unlocks; the next threshold wake runs.
    harness
        .handle
        .send_event(InboundEvent::Message(message(
            "d3",
            t0 + time::Duration::seconds(121),
            false,
        )))
        .await
        .expect("the actor inbox is open");
    let session = wait_for_session(&harness.handle, |session| !session.muted, "unlocked").await;
    assert!(!session.muted);
    for (id, seconds) in [("d4", 122), ("d5", 123)] {
        harness
            .handle
            .send_event(InboundEvent::Message(message(
                id,
                t0 + time::Duration::seconds(seconds),
                false,
            )))
            .await
            .expect("the actor inbox is open");
    }
    let actions = wait_for_actions(&harness.sink, 3).await;
    let sends = send_texts(&actions);
    assert_eq!(
        sends,
        vec![
            (
                CHAT_ID.to_string(),
                "r1".to_string(),
                Some("d1".to_string())
            ),
            (
                CHAT_ID.to_string(),
                "r2".to_string(),
                Some("d2".to_string())
            ),
            (
                CHAT_ID.to_string(),
                "r3".to_string(),
                Some("d5".to_string())
            ),
        ]
    );
    wait_for_counter(&fixture.store, "wakes_total", "4").await;
    wait_for_counter(&fixture.store, "participations_total", "3").await;
    assert_eq!(harness.gate.inputs().len(), 1);
    shutdown(harness).await;
}

/// Scenario E: the recency re-check of specs.md Section 6.2. The gate
/// targets the first of three new messages; with
/// `reply_staleness_threshold = 0` the two newer human messages make the
/// generated reply stale and the completion handler DISCARDS it
/// (documented decision: discard, not regenerate). A trailing forced
/// wake is the deterministic proof that the discard completed: its send
/// implies the first wake's completion handler ran (the forced wake
/// starts at the earliest inside that handler), so exactly one outbound
/// row (the forced reply) proves the first reply left no trace.
#[tokio::test]
async fn stale_reply_is_discarded() {
    let fixture = make_fixture();
    let t0 = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("a valid timestamp");
    let config = TriggerConfig {
        wake_msg_count: 3,
        wake_floor: Duration::ZERO,
        wake_interval: HUGE_INTERVAL,
        reply_staleness_threshold: 0,
        ..TriggerConfig::default()
    };
    let gate = Arc::new(ScriptedGate::with_decisions(vec![GateDecision {
        participate: true,
        target_row_id: Some(1),
        reason: None,
    }]));
    let reply = Arc::new(ScriptedReplyGenerator::with_replies(vec![
        "r1-stale".to_string(),
        "r2".to_string(),
    ]));
    let harness = spawn_on(&fixture, config, t0, gate, reply);
    for (index, id) in ["e1", "e2", "e3"].iter().enumerate() {
        harness
            .handle
            .send_event(InboundEvent::Message(message(
                id,
                t0 + time::Duration::seconds(index as i64 + 1),
                false,
            )))
            .await
            .expect("the actor inbox is open");
    }
    // The reply model ran (the reply is generated BEFORE the recency
    // re-check, Section 9 step 4), so the wake is past the gate.
    let deadline = std::time::Instant::now() + WAKE_TIMEOUT;
    while harness.reply.requests().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {WAKE_TIMEOUT:?} waiting for the reply generation"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    // The deterministic barrier: the forced wake starts at the earliest
    // inside the first wake's completion handler (Section 6.2 queueing),
    // so its send proves the discard already ran.
    harness
        .handle
        .send_event(InboundEvent::Message(message(
            "e4",
            t0 + time::Duration::seconds(4),
            true,
        )))
        .await
        .expect("the actor inbox is open");
    let actions = wait_for_actions(&harness.sink, 1).await;
    let sends = send_texts(&actions);
    assert_eq!(
        sends,
        vec![(
            CHAT_ID.to_string(),
            "r2".to_string(),
            Some("e4".to_string())
        )],
        "the stale reply was discarded; only the forced reply was sent"
    );
    let outbound = rows_of_direction(&fixture.store, Direction::Outbound).await;
    assert_eq!(outbound.len(), 1);
    assert_eq!(outbound[0].text, "r2");
    wait_for_counter(&fixture.store, "wakes_total", "2").await;
    wait_for_counter(&fixture.store, "participations_total", "1").await;
    shutdown(harness).await;
}

/// Scenario F: the parrot filter end to end (decision 59, F1). Two
/// forced wakes (mentions), two scripted replies:
///
/// - reply 1 is ONLY a confabulated parrot block (ASCII and full-width
///   colon variants): the filter leaves nothing, the wake follows the
///   exact empty-reply path — nothing persisted, nothing sent, no
///   participation counted;
/// - reply 2 is a parrot block followed by real text: the group and
///   the raw log see the SAME stripped remainder (Rule B1: the log is
///   the truth).
///
/// The second wake's send is the deterministic barrier that the first
/// wake's failure completed (Section 6.2: a queued forced wake starts
/// at the earliest inside the previous wake's completion handler),
/// exactly as in `stale_reply_is_discarded`.
#[tokio::test]
async fn parroting_replies_are_filtered_before_log_and_send() {
    let fixture = make_fixture();
    let t0 = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("a valid timestamp");
    let config = TriggerConfig {
        wake_msg_count: 3,
        wake_floor: Duration::ZERO,
        wake_interval: HUGE_INTERVAL,
        ..TriggerConfig::default()
    };
    // Forced wakes bypass the gate; an exhausted scripted gate fails
    // the test if one is ever consulted.
    let gate = Arc::new(ScriptedGate::with_decisions(vec![]));
    let reply = Arc::new(ScriptedReplyGenerator::with_replies(vec![
        "I remember: Alice likes tea.\nI remember：小明喜欢吃辣。".to_string(),
        "I remember: Alice likes tea.\n在的".to_string(),
    ]));
    let harness = spawn_on(&fixture, config, t0, gate, reply);
    harness
        .handle
        .send_event(InboundEvent::Message(message(
            "f1",
            t0 + time::Duration::seconds(1),
            true,
        )))
        .await
        .expect("the actor inbox is open");
    // The reply model ran for the first wake.
    let deadline = std::time::Instant::now() + WAKE_TIMEOUT;
    while harness.reply.requests().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {WAKE_TIMEOUT:?} waiting for the reply generation"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    harness
        .handle
        .send_event(InboundEvent::Message(message(
            "f2",
            t0 + time::Duration::seconds(2),
            true,
        )))
        .await
        .expect("the actor inbox is open");
    // Exactly one send: the stripped remainder of reply 2. Its arrival
    // proves the parrot-only wake left no trace behind it.
    let actions = wait_for_actions(&harness.sink, 1).await;
    let sends = send_texts(&actions);
    assert_eq!(
        sends,
        vec![(
            CHAT_ID.to_string(),
            "在的".to_string(),
            Some("f2".to_string())
        )],
        "the parrot block never reaches the group; only the remainder is sent"
    );
    // Rule B1: the one outbound raw-log row carries the SAME filtered
    // text the group saw — the log is the truth.
    let outbound = rows_of_direction(&fixture.store, Direction::Outbound).await;
    assert_eq!(outbound.len(), 1);
    assert_eq!(outbound[0].text, "在的");
    assert_eq!(outbound[0].reply_to_platform_msg_id.as_deref(), Some("f2"));
    // Rule C1: the context bot speech is the filtered text too, wrapped
    // by the Section 7.2 step 4 bot-speech renderer.
    let context = harness
        .handle
        .context_snapshot()
        .await
        .expect("the context snapshot succeeds");
    let bot_speeches: Vec<&str> = context
        .iter()
        .filter(|item| item.kind == ContextItemKind::BotSpeech)
        .map(|item| item.content.as_str())
        .collect();
    let expected_speech =
        render_bot_content(outbound[0].id, outbound[0].timestamp, &outbound[0].text);
    assert_eq!(bot_speeches, vec![expected_speech.as_str()]);
    wait_for_counter(&fixture.store, "wakes_total", "2").await;
    // Only the stripped-remainder send counts as a participation; the
    // parrot-only wake failed like an empty reply.
    wait_for_counter(&fixture.store, "participations_total", "1").await;
    shutdown(harness).await;
}
