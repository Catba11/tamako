//! The monologue lock across a restart (specs.md Section 8.5), end to
//! end against the REAL SQLite store, the real actor
//! (`spawn_group_actor`), and scripted gate/reply doubles
//! (tamako-agent). The outbound channel of the binary is reproduced as
//! a forwarder task that drains the channel into an `Arc<MockAdapter>`
//! sink (`execute` takes `&self`). Env-hermetic: scripted doubles only;
//! no LLM keys, no network.
//!
//! Timer note: the scenario drives the actor with explicit events and
//! one explicit `Tick` command. All timestamps sit one day in the
//! FUTURE relative to the real clock. The built-in 1-second ticker of
//! the actor then always sees a negative elapsed time and stays inert
//! (the floor check of specs.md Section 8.3 blocks the fire).

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tamako_adapter_mock::MockAdapter;
use tamako_agent::{ScriptedGate, ScriptedReplyGenerator};
use tamako_core::actor::{
    spawn_group_actor, ActorCommand, GroupActorHandle, GroupActorParams, DEFAULT_INBOX_CAPACITY,
};
use tamako_core::adapter::PlatformAdapter;
use tamako_core::config::TriggerConfig;
use tamako_core::event::{InboundEvent, NormalizedMessage, OutboundAction};
use tamako_core::session::SessionState;
use tamako_core::wake::{GateDecision, NoopRecall, WakeServices};
use tamako_memory::{AliasTarget, MemoryBackend, MemoryBatch};
use tamako_store::Store;
use time::OffsetDateTime;
use tokio::sync::mpsc;

const CHAT_ID: &str = "-1001234567890";

/// The bounded waits of this suite. The scripted doubles return
/// immediately, so a wake round trip is milliseconds; five seconds is
/// generous enough for a loaded CI machine.
const WAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// The poll interval of every bounded wait.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

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

/// The storage handles of one run. The TempDir of the data root stays
/// with the test, so a second fixture can reopen the same root after
/// the first one drops (the restart).
struct Fixture {
    store: Arc<Store>,
    memory: Arc<NoopMemory>,
}

fn make_fixture(data_root: &Path) -> Fixture {
    Fixture {
        store: Arc::new(Store::new(data_root.to_path_buf())),
        memory: Arc::new(NoopMemory),
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
        warmup: None,
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

/// Scenario: the monologue lock of specs.md Section 8.5 survives a
/// restart. Phase 1 drives a live-shaped sequence — human messages,
/// two back-to-back bot speeches (two forced wakes from mentions),
/// then silence — and the lock engages. Phase 2 respawns the actor on
/// the SAME data root and asserts:
///
/// - the restarted session is still muted (the persisted keys
///   `muted_flag` and `consecutive_bot_msgs` of specs.md Section 5.2
///   rebuild the lock);
/// - a tick-driven unforced wake stays suppressed after the restart
///   (Section 9 step 1): nothing is sent, the gate never runs, and
///   `wakes_total` still advances (a suppressed wake counts as a
///   wake);
/// - one human message clears the lock at intake (Section 8.5) and the
///   next threshold wake speaks again.
///
/// The suppressed wake must be tick-driven in both phases: any human
/// message clears the lock at intake, so a message-driven wake can
/// never observe `muted`.
///
/// Raw-log rows: h1=1, h2=2 (plain), m1=3, m2=4 (mentions), r1=5,
/// r2=6 (outbound rows, Rule B1). After the restart: u1=7, u2=8, u3=9.
/// The final wake sees the new messages 7, 8, 9 and targets row 9 (the
/// suppressed wake of Section 9 step 1 does not advance
/// `wake_last_row_id`).
#[tokio::test]
async fn monologue_lock_survives_a_restart_and_unlocks_on_a_human_message() {
    let dir = tempfile::tempdir().expect("a temporary data root");
    let data_root = dir.path().to_path_buf();
    // All timestamps one day in the future: the built-in 1-second
    // ticker then always sees a negative elapsed time and stays inert;
    // only the explicit `Tick` drives the suppressed wake.
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

    // --- Phase 1: engage the lock on a fresh data root. ---
    let fixture = make_fixture(&data_root);
    // The forced wakes bypass the gate (Section 8.1), so a failing
    // gate is a loud signal: any gate call in this phase fails the
    // wake.
    let gate = Arc::new(ScriptedGate::failing(
        "the gate must not be called for a forced wake",
    ));
    let reply = Arc::new(ScriptedReplyGenerator::with_replies(vec![
        "r1".to_string(),
        "r2".to_string(),
    ]));
    let harness = spawn_on(&fixture, config.clone(), t0, gate, reply);
    // Human messages, then two back-to-back mentions, then silence.
    for (id, seconds, mention) in [
        ("h1", 1, false),
        ("h2", 2, false),
        ("m1", 3, true),
        ("m2", 4, true),
    ] {
        harness
            .handle
            .send_event(InboundEvent::Message(message(
                id,
                t0 + time::Duration::seconds(seconds),
                mention,
            )))
            .await
            .expect("the actor inbox is open");
    }
    // Two forced wakes, two consecutive bot speeches, no human message
    // in between: the lock engages (Section 8.5).
    let actions = wait_for_actions(&harness.sink, 2).await;
    let sends = send_texts(&actions);
    assert_eq!(
        sends,
        vec![
            (
                CHAT_ID.to_string(),
                "r1".to_string(),
                Some("m1".to_string())
            ),
            (
                CHAT_ID.to_string(),
                "r2".to_string(),
                Some("m2".to_string())
            ),
        ]
    );
    let session = wait_for_session(
        &harness.handle,
        |session| session.muted,
        "muted after two bot speeches",
    )
    .await;
    assert!(session.muted);
    assert_eq!(session.consecutive_bot_msgs, 2);
    assert!(
        harness.gate.inputs().is_empty(),
        "the forced wakes bypassed the gate (Section 8.1)"
    );
    assert_eq!(harness.reply.requests().len(), 2);
    wait_for_counter(&fixture.store, "wakes_total", "2").await;
    shutdown(harness).await;
    // A real restart drops every handle and opens the files again.
    drop(fixture);

    // --- Phase 2: the restart on the SAME data root. ---
    let fixture = make_fixture(&data_root);
    // A fresh working gate and reply model for the unlock wake of this
    // phase. The muted-phase assertions below prove the gate never ran
    // while the lock held.
    let gate = Arc::new(ScriptedGate::with_decisions(vec![GateDecision {
        participate: true,
        target_row_id: Some(9),
        reason: None,
    }]));
    let reply = Arc::new(ScriptedReplyGenerator::with_replies(vec!["r3".to_string()]));
    let harness = spawn_on(&fixture, config, t0, gate, reply);

    // The core assertion: the restarted actor rebuilt the lock from
    // the persisted state (specs.md Section 6.1, rule 4).
    let session = harness
        .handle
        .snapshot()
        .await
        .expect("the snapshot succeeds");
    assert!(session.muted, "the lock survived the restart");
    assert_eq!(session.consecutive_bot_msgs, 2);

    // The interval (60 s) has elapsed at t0 + 120 s. The muted
    // unforced wake is suppressed (Section 9 step 1).
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
    assert!(
        harness.sink.recorded_actions().is_empty(),
        "the muted wake sent nothing"
    );
    assert!(
        harness.gate.inputs().is_empty(),
        "the suppressed wake never reaches the gate"
    );
    // The suppressed wake still counts as a wake: 2 forced wakes of
    // phase 1 plus this one.
    wait_for_counter(&fixture.store, "wakes_total", "3").await;

    // A human message clears the lock at intake (Section 8.5).
    harness
        .handle
        .send_event(InboundEvent::Message(message(
            "u1",
            t0 + time::Duration::seconds(121),
            false,
        )))
        .await
        .expect("the actor inbox is open");
    let session = wait_for_session(&harness.handle, |session| !session.muted, "unlocked").await;
    assert!(!session.muted);
    assert_eq!(session.consecutive_bot_msgs, 0);

    // The remaining messages complete the count threshold; the wake
    // runs and the bot speaks again.
    for (id, seconds) in [("u2", 122), ("u3", 123)] {
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
    let actions = wait_for_actions(&harness.sink, 1).await;
    let sends = send_texts(&actions);
    assert_eq!(
        sends,
        // The unforced unlock wake targets u3, the LATEST message: zero
        // newer human messages, so decision 70 (specs.md Section 6.2)
        // sends r3 as a plain standalone message with no quote.
        vec![(CHAT_ID.to_string(), "r3".to_string(), None)],
        "the lock disengaged: the threshold wake spoke after the restart"
    );
    wait_for_counter(&fixture.store, "wakes_total", "4").await;
    wait_for_counter(&fixture.store, "participations_total", "3").await;
    // The gate ran exactly once, over the new messages of the unlock
    // wake (Section 9.6).
    let inputs = harness.gate.inputs();
    assert_eq!(inputs.len(), 1);
    assert!(!inputs[0].forced);
    assert_eq!(
        inputs[0]
            .new_messages
            .iter()
            .map(|msg| msg.row_id)
            .collect::<Vec<_>>(),
        vec![7, 8, 9]
    );
    shutdown(harness).await;
}
