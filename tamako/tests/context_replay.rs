//! End-to-end tests of the M2 context lifecycle (specs.md Section 7,
//! rules C1-C4) against the REAL SQLite store and the REAL LadybugDB
//! backend, driven through the REAL actor with a scripted extractor.
//!
//! Coverage:
//! - Rule C1 / Section 7.3: intake appends User items in the XML
//!   `<msg>` rendering (UTC);
//! - Rule C4: item 0 is the preamble;
//! - Rule C3 / Section 7.1: a completed digest removes only the items at
//!   or below the PREVIOUS boundary (the one-chunk lag);
//! - Section 10.2 step 4: the same cutoff prunes `injected_memories`;
//! - Rule P1 / Section 6.1 rule 4: a restart rebuilds the context
//!   bit-identically from the raw log, `injected_memories`, and the
//!   session state.
//!
//! Determinism note for test 1. The digest trigger fires at the fourth
//! log row, and the pipeline reads the tail at execution time (specs.md
//! Section 10.1), so the first boundary B1 can be any row id in 4..=11.
//! When B1 <= 7, the tail above B1 still holds four or more rows and the
//! post-digest trigger re-evaluation of the actor starts a SECOND digest
//! immediately. A poll-based observation of the post-digest-1 state
//! (`prev == None`, nothing removed) would race that cascade. The
//! `GateHook` below parks the actor loop inside the FIRST digest
//! completion handler — after the session persistence, before the
//! re-evaluation — and the test queues `Shutdown` before it opens the
//! gate. The FIFO inbox then guarantees that no later digest result is
//! ever processed. A restart exposes the frozen post-digest-1 state
//! through the public snapshot APIs (the Rule P1 rebuild is
//! bit-identical). The `snapshot()` barrier of the other suites is
//! subsumed here: the queued `Shutdown` drains the inbox, so the join
//! proves every intake landed.

use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tamako_adapter_mock::fixture::load_fixture;
use tamako_adapter_mock::{FixtureEvent, MockAdapter, ReplayFixture};
use tamako_agent::{
    AgentDigestPipeline, ExtractedNode, ExtractedNodeType, KnowledgeGraph, PipelineConfig,
    ScriptedExtractor,
};
use tamako_core::actor::{
    spawn_group_actor, ActorCommand, GroupActorHandle, GroupActorParams, DEFAULT_INBOX_CAPACITY,
};
use tamako_core::adapter::PlatformAdapter;
use tamako_core::config::TriggerConfig;
use tamako_core::context::{ContextItem, ContextItemKind, ContextRole, LiveContext, RangeTag};
use tamako_core::digest::{DigestOutcome, PostDigestHook};
use tamako_core::event::{InboundEvent, NormalizedMessage};
use tamako_memory::LbugBackend;
use tamako_store::{EventType, MessageRow, Store, StoreError};
use time::macros::{datetime, format_description};
use time::{OffsetDateTime, UtcOffset};

const CHAT_ID: &str = "-1001234567890";

/// The preamble of every actor of this suite.
const TEST_PREAMBLE: &str = "test preamble";

/// The shipped demo fixture.
const FIXTURE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../tamako-adapter-mock/fixtures/replay_chat.json"
);

/// The bounded waits of this suite. The scripted extractor returns
/// immediately, so a digest round trip is milliseconds; five seconds is
/// generous enough for a loaded CI machine.
const DIGEST_TIMEOUT: Duration = Duration::from_secs(5);
/// The poll interval of `wait_for_boundary` and `wait_for_first_outcome`.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// The UTC HH:MM format of the timestamp attributes (specs.md
/// Section 7.3). The test re-renders the label independently of the
/// implementation renderer.
const HHMM_FORMAT: &[time::format_description::FormatItem<'_>] =
    format_description!("[hour]:[minute]");

/// Fixed base time for the hand-crafted messages of test 2 (the pattern
/// of tamako/tests/digest_replay.rs).
fn t0() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("a valid unix timestamp")
}

/// One fixed start time for test 1. It sits AFTER every fixture
/// timestamp, so the wake floor blocks every timer fire (the pattern of
/// tamako/tests/replay_restart.rs).
fn started_after_fixture() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_800_000_000).expect("a valid unix timestamp")
}

/// A config whose digest trigger fires every `max_messages` tail rows
/// (specs.md Section 8.2, the messages threshold).
fn digest_config(max_messages: u32) -> TriggerConfig {
    TriggerConfig {
        digest_max_messages: max_messages,
        ..TriggerConfig::default()
    }
}

/// A hand-crafted message of test 2 (the pattern of
/// tamako/tests/digest_replay.rs).
fn message(id: &str, seconds: i64, sender_id: &str, name: &str, text: &str) -> NormalizedMessage {
    NormalizedMessage {
        platform_msg_id: id.to_string(),
        timestamp: t0() + time::Duration::seconds(seconds),
        sender_id: sender_id.to_string(),
        sender_display_name: name.to_string(),
        username: None,
        text: text.to_string(),
        reply_to_platform_msg_id: None,
        mentions_bot: false,
        is_reply_to_bot: false,
    }
}

/// A hand-crafted message of test 1. The timestamps continue the
/// fixture: the last fixture event is at 13:15 UTC.
fn tail_message(
    id: &str,
    seconds: i64,
    sender_id: &str,
    name: &str,
    text: &str,
) -> NormalizedMessage {
    NormalizedMessage {
        platform_msg_id: id.to_string(),
        timestamp: datetime!(2026-08-01 13:16 UTC) + time::Duration::seconds(seconds),
        sender_id: sender_id.to_string(),
        sender_display_name: name.to_string(),
        username: None,
        text: text.to_string(),
        reply_to_platform_msg_id: None,
        mentions_bot: false,
        is_reply_to_bot: false,
    }
}

/// The trivial scripted graph: one Person node, no edges. The graph
/// content is irrelevant to the context assertions; "Alice" appears in
/// every batch of this suite, so the mention map always binds her
/// (specs.md Section 10.1).
fn trivial_graph() -> KnowledgeGraph {
    KnowledgeGraph {
        nodes: vec![ExtractedNode {
            name: "Alice".to_string(),
            node_type: ExtractedNodeType::Person,
            description: "A group member.".to_string(),
        }],
        edges: vec![],
    }
}

struct Fixture {
    // The TempDir must outlive the store and the backend.
    _dir: tempfile::TempDir,
    store: Arc<Store>,
    memory: Arc<LbugBackend>,
}

fn make_fixture() -> Fixture {
    let dir = tempfile::tempdir().expect("a temporary data root");
    let store = Arc::new(Store::new(dir.path().to_path_buf()));
    let memory = Arc::new(LbugBackend::new(dir.path().to_path_buf()));
    Fixture {
        _dir: dir,
        store,
        memory,
    }
}

/// Spawns the real actor with the real pipeline over a scripted
/// extractor.
fn spawn_actor(
    fixture: &Fixture,
    config: TriggerConfig,
    extractor: Arc<ScriptedExtractor>,
    post_digest_hook: Option<Arc<dyn PostDigestHook>>,
    started_at: OffsetDateTime,
) -> GroupActorHandle {
    let digest = Arc::new(AgentDigestPipeline::new(
        Arc::clone(&fixture.store),
        Arc::clone(&fixture.memory),
        extractor,
        PipelineConfig::default(),
    ));
    spawn_group_actor(GroupActorParams {
        chat_id: CHAT_ID.to_string(),
        store: Arc::clone(&fixture.store),
        memory: Arc::clone(&fixture.memory),
        config,
        started_at,
        inbox_capacity: DEFAULT_INBOX_CAPACITY,
        preamble: TEST_PREAMBLE.to_string(),
        digest: Some(digest),
        post_digest_hook,
        // The M4 wake wiring enters in a later subtask.
        wake: None,
        summary_provider: None,
        outbound: None,
        bot_name: None,
    })
}

/// Replays the events through a MockAdapter into the actor (the pattern
/// of tamako/tests/replay_restart.rs).
async fn replay(handle: &GroupActorHandle, chat_id: &str, events: Vec<FixtureEvent>) {
    let mut adapter = MockAdapter::from_fixture(ReplayFixture {
        chat_id: chat_id.to_string(),
        events,
    });
    while let Some(event) = adapter
        .next_event()
        .await
        .expect("the mock adapter never fails")
    {
        handle
            .send_event(event)
            .await
            .expect("the actor inbox is open");
    }
}

/// Polls `snapshot()` until the digest boundary reaches `min` or the
/// timeout elapses (the pattern of tamako/tests/digest_replay.rs). The
/// snapshot is a FIFO barrier, and the spawned digest task reports
/// through the inbox: a snapshot that shows the boundary proves the
/// whole completion handler (Rule C3 removal, prune, persistence)
/// already ran. Deterministic.
async fn wait_for_boundary(handle: &GroupActorHandle, min: i64, timeout: Duration) -> i64 {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let session = handle.snapshot().await.expect("the snapshot succeeds");
        if session.last_digest_boundary_msg_id >= min {
            return session.last_digest_boundary_msg_id;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {timeout:?} waiting for the digest boundary to reach {min} \
             (current boundary: {})",
            session.last_digest_boundary_msg_id
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Runs one blocking store call, like the actor does (AGENT.md
/// Section 6.2).
async fn blocking_store_call<T, F>(store: &Arc<Store>, call: F) -> T
where
    T: Send + 'static,
    F: FnOnce(Arc<Store>) -> Result<T, StoreError> + Send + 'static,
{
    let store = Arc::clone(store);
    tokio::task::spawn_blocking(move || call(store))
        .await
        .expect("the blocking task joins")
        .expect("the store call succeeds")
}

/// Reads the raw-log rows above `after_id` through a blocking call.
async fn store_rows_after(fixture: &Fixture, after_id: i64) -> Vec<MessageRow> {
    blocking_store_call(&fixture.store, move |store| {
        store.list_messages_after(CHAT_ID, after_id)
    })
    .await
}

/// The XML speaker label of specs.md Section 7.3, rendered by the
/// test from a persisted raw-log row. The fixture rows carry no username
/// and no resolvable user-reply targets, so only the edit, bot-reply,
/// and mention attributes can appear; the texts have no special
/// characters, so the unescaped form is exact here.
fn expected_label(row: &MessageRow) -> String {
    let hhmm = row
        .timestamp
        .to_offset(UtcOffset::UTC)
        .format(HHMM_FORMAT)
        .expect("the replay timestamps format");
    let kind = if row.event_type == EventType::Edit {
        " kind=\"edit\""
    } else {
        ""
    };
    let reply = if row.is_reply_to_bot {
        " reply=\"bot\""
    } else {
        ""
    };
    let mention = if row.mentions_bot {
        " mention=\"bot\""
    } else {
        ""
    };
    format!(
        "<msg from=\"{}\" at=\"{}\" id=\"{}\"{}{}{}>{}</msg>",
        row.sender_display_name, hhmm, row.id, kind, reply, mention, row.text
    )
}

/// Asserts the Rule C4 item 0.
fn assert_preamble(item: &ContextItem) {
    assert_eq!(item.kind, ContextItemKind::Preamble);
    assert_eq!(item.role, ContextRole::System);
    assert_eq!(item.content, TEST_PREAMBLE);
    assert_eq!(item.range_tag, None);
}

/// Asserts that one context item is the User item of one raw-log row:
/// kind, role, the exact speaker label, and the range tag.
fn assert_human_row_item(item: &ContextItem, row: &MessageRow) {
    assert_eq!(item.kind, ContextItemKind::HumanMessage);
    assert_eq!(item.role, ContextRole::User);
    assert_eq!(item.content, expected_label(row));
    assert_eq!(item.range_tag, Some(RangeTag::single(row.id)));
}

/// A post-digest hook that parks the actor loop inside the FIRST digest
/// completion handler. Refer to the module header: this freezes the
/// exact post-digest-1 state (after the session persistence, before the
/// trigger re-evaluation) so the test does not race a cascade digest.
/// The test holds the gate guard from before the spawn and drops it
/// only after `Shutdown` is queued. Every outcome is recorded.
struct GateHook {
    outcomes: Mutex<Vec<DigestOutcome>>,
    gate: Arc<tokio::sync::Mutex<()>>,
    park_once: AtomicBool,
}

impl GateHook {
    fn new(gate: Arc<tokio::sync::Mutex<()>>) -> Self {
        GateHook {
            outcomes: Mutex::new(Vec::new()),
            gate,
            park_once: AtomicBool::new(true),
        }
    }

    fn outcomes(&self) -> Vec<DigestOutcome> {
        self.outcomes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl PostDigestHook for GateHook {
    fn after_digest<'a>(
        &'a self,
        _chat_id: &'a str,
        outcome: &'a DigestOutcome,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        let outcome = outcome.clone();
        Box::pin(async move {
            self.outcomes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(outcome);
            if self.park_once.swap(false, Ordering::SeqCst) {
                // The actor loop parks here until the test drops the
                // gate guard.
                let _guard = self.gate.lock().await;
            }
        })
    }
}

/// Polls the hook until the first digest outcome is recorded or the
/// timeout elapses. The hook records the outcome BEFORE it parks, so
/// this wait never needs the actor loop.
async fn wait_for_first_outcome(hook: &GateHook, timeout: Duration) -> DigestOutcome {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Some(outcome) = hook.outcomes().into_iter().next() {
            return outcome;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {timeout:?} waiting for the first digest outcome"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[tokio::test]
async fn context_grows_on_intake_and_lags_out_after_two_digests() {
    let recording = load_fixture(Path::new(FIXTURE_PATH)).expect("the shipped fixture loads");
    let chat_id = recording.chat_id.clone();
    assert_eq!(chat_id, CHAT_ID);
    // The fixture composition: 10 messages, 1 edit, 1 reaction, 1
    // member_join, 1 member_leave (14 events). The messages and the edit
    // become raw-log rows 1..=11 in replay order; the reaction and the
    // member events have no Phase 1 consumer.
    assert_eq!(recording.events.len(), 14);
    let log_event_count = recording
        .events
        .iter()
        .filter(|event| {
            matches!(
                event,
                FixtureEvent::Message(_) | FixtureEvent::EditedMessage(_)
            )
        })
        .count();
    assert_eq!(log_event_count, 11);

    let fixture = make_fixture();
    // The test holds the gate from before the spawn: the first digest
    // completion parks inside the actor loop (refer to GateHook and the
    // module header).
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    let gate_guard = gate.lock().await;
    let hook = Arc::new(GateHook::new(Arc::clone(&gate)));
    // Three graphs: digest 1, the cascade digest 2 that can start after
    // the gate opens (its result is dropped: `Shutdown` is already
    // queued), and margin. The graph content is irrelevant here.
    let extractor = Arc::new(ScriptedExtractor::with_graphs(vec![
        trivial_graph(),
        trivial_graph(),
        trivial_graph(),
    ]));
    let handle = spawn_actor(
        &fixture,
        digest_config(4),
        extractor,
        Some(hook.clone() as Arc<dyn PostDigestHook>),
        started_after_fixture(),
    );

    replay(&handle, &chat_id, recording.events).await;

    // Digest 1. The pipeline read the tail at execution time (specs.md
    // Section 10.1): B1 is the id of some log row in 4..=11.
    let first = wait_for_first_outcome(&hook, DIGEST_TIMEOUT).await;
    let b1 = first.new_boundary();
    assert!(
        matches!(first, DigestOutcome::Extracted { .. }),
        "the fixture prose is extracted, got {first:?}"
    );
    assert!(
        (4..=11).contains(&b1),
        "the first batch covers log rows 1..=b1, got {b1}"
    );

    // Queue Shutdown BEFORE the gate opens. FIFO: the actor drains the
    // intakes and the Shutdown before any later digest result can
    // arrive; the join proves every intake landed.
    handle
        .send(ActorCommand::Shutdown)
        .await
        .expect("the actor inbox is open");
    drop(gate_guard);
    handle.shutdown().await.expect("the actor reports no error");

    // --- The frozen post-digest-1 state, observed through a restart. ---
    // Rule P1: the rebuild is bit-identical to the pre-shutdown context.
    // The removal cutoff is `prev.unwrap_or(0)` = 0, so every row above
    // id 0 is live: the first digest removed nothing (Rule C3 — the
    // previous chunk does not exist yet).
    let restarted = spawn_actor(
        &fixture,
        digest_config(4),
        Arc::new(ScriptedExtractor::with_graphs(vec![
            trivial_graph(),
            trivial_graph(),
        ])),
        None,
        started_after_fixture(),
    );
    let session = restarted.snapshot().await.expect("the snapshot succeeds");
    assert_eq!(
        session.prev_digest_boundary_msg_id, None,
        "the first digest has no previous chunk"
    );
    assert_eq!(session.last_digest_boundary_msg_id, b1);

    let rows = store_rows_after(&fixture, 0).await;
    assert_eq!(rows.len(), 11, "10 messages and 1 edit");
    assert!(
        rows.iter().any(|row| row.event_type == EventType::Edit),
        "the edit is a raw-log row too"
    );
    let items = restarted
        .context_snapshot()
        .await
        .expect("the context snapshot succeeds");
    assert_eq!(items.len(), 12, "the preamble and every log row above id 0");
    assert_preamble(&items[0]);
    // Every log row above id 0 appears as a User item with the exact
    // speaker label — the edit row included (an edit is just a new row,
    // specs.md Section 15 open item 4).
    for (index, row) in rows.iter().enumerate() {
        assert_human_row_item(&items[index + 1], row);
    }

    // --- Digest 2 over the tail (b1, b2]. ---
    // Four more messages (rows 12..=15). The trigger fires when the tail
    // above b1 reaches four rows — at row 12 when b1 <= 8, at row b1+4
    // otherwise — and the pipeline reads at least up to the fire point:
    // b2 >= max(12, b1+4). The tail above b2 then holds at most three
    // rows, so NO third digest can follow and the observations below are
    // stable.
    for msg in [
        tail_message(
            "x1",
            0,
            "100001",
            "Alice",
            "the hike was great and the cat loved it",
        ),
        tail_message("x2", 60, "100002", "Bob", "saturday worked out in the end"),
        tail_message(
            "x3",
            120,
            "100003",
            "Carol",
            "she wore the tiny backpack all day",
        ),
        tail_message("x4", 180, "100001", "Alice", "next trip we bring two cats"),
    ] {
        restarted
            .send_event(InboundEvent::Message(msg))
            .await
            .expect("the actor inbox is open");
    }
    let b2 = wait_for_boundary(&restarted, b1 + 1, DIGEST_TIMEOUT).await;
    assert!(
        (12..=15).contains(&b2) && b2 >= b1 + 4,
        "the second batch covers (b1, b2] with b2 >= max(12, b1+4), got b1={b1} b2={b2}"
    );

    let rows = store_rows_after(&fixture, 0).await;
    assert_eq!(rows.len(), 15);
    let items = restarted
        .context_snapshot()
        .await
        .expect("the context snapshot succeeds");
    // Rule C3 with the one-chunk lag: every item at or below the
    // PREVIOUS boundary b1 is gone. Everything above b1 stays: the chunk
    // just digested, (b1, b2], is the new overlap buffer, and the rows
    // above b2 are the live tail.
    let live_rows: Vec<&MessageRow> = rows.iter().filter(|row| row.id > b1).collect();
    assert_eq!(items.len(), live_rows.len() + 1);
    assert_preamble(&items[0]);
    for (index, row) in live_rows.iter().enumerate() {
        assert_human_row_item(&items[index + 1], row);
    }
    assert!(
        live_rows.iter().any(|row| row.id == b2),
        "the chunk (b1, b2] is still present as the overlap buffer"
    );
    assert!(
        items[1..]
            .iter()
            .all(|item| item.range_tag.as_ref().expect("a range tag").last_msg_id > b1),
        "every context item with a tag <= b1 is gone"
    );
    let session = restarted.snapshot().await.expect("the snapshot succeeds");
    assert_eq!(session.prev_digest_boundary_msg_id, Some(b1));
    assert_eq!(session.last_digest_boundary_msg_id, b2);
    restarted
        .shutdown()
        .await
        .expect("the actor reports no error");
}

#[tokio::test]
async fn restart_rebuilds_a_bit_identical_context_with_injections() {
    let fixture = make_fixture();
    // Digest 1 only: the tail never reaches three rows again before the
    // restart (m4 and m5 stay below the threshold).
    let extractor = Arc::new(ScriptedExtractor::with_graphs(vec![trivial_graph()]));
    let handle = spawn_actor(&fixture, digest_config(3), extractor, None, t0());

    for msg in [
        message("m1", 1, "u1", "Alice", "morning all"),
        message("m2", 2, "u1", "Alice", "GRPO looks unstable"),
        message("m3", 3, "u2", "Bob", "really? ours converged fine"),
    ] {
        handle
            .send_event(InboundEvent::Message(msg))
            .await
            .expect("the actor inbox is open");
    }
    handle.snapshot().await.expect("the snapshot succeeds");
    // Only three rows exist, so the boundary is exactly 3.
    let boundary = wait_for_boundary(&handle, 3, DIGEST_TIMEOUT).await;
    assert_eq!(boundary, 3);
    let session = handle.snapshot().await.expect("the snapshot succeeds");
    assert_eq!(
        session.prev_digest_boundary_msg_id, None,
        "the first digest has no previous chunk"
    );

    // The tail (m4, m5) stays below the threshold: no second digest.
    for msg in [
        message("m4", 4, "u1", "Alice", "lucky. my run diverged twice"),
        message("m5", 5, "u2", "Bob", "what learning rate?"),
    ] {
        handle
            .send_event(InboundEvent::Message(msg))
            .await
            .expect("the actor inbox is open");
    }
    let session = handle.snapshot().await.expect("the snapshot succeeds");
    assert_eq!(session.last_digest_boundary_msg_id, 3);

    // The injection rows go directly into the store (the M5 producer
    // path does not exist yet). They do NOT enter the live context of
    // the running actor; the test asserts the REBUILD path.
    blocking_store_call(&fixture.store, move |store| {
        store.insert_injected_memory(
            CHAT_ID,
            "edge-1",
            2,
            "1-2",
            "I remember: Alice likes GRPO",
        )?;
        store.insert_injected_memory(CHAT_ID, "edge-2", 4, "4-4", "I remember: Bob runs 3e-6")?;
        Ok(())
    })
    .await;
    handle.shutdown().await.expect("the actor reports no error");

    // --- First restart: the rebuild with injections. ---
    let restarted = spawn_actor(
        &fixture,
        digest_config(3),
        Arc::new(ScriptedExtractor::with_graphs(vec![trivial_graph()])),
        None,
        t0(),
    );
    let session = restarted.snapshot().await.expect("the snapshot succeeds");
    assert_eq!(session.prev_digest_boundary_msg_id, None);
    assert_eq!(session.last_digest_boundary_msg_id, 3);

    // The hand-computed rebuild over the same persisted rows (Rule P1).
    let (rows, injections) = blocking_store_call(&fixture.store, move |store| {
        let rows = store.list_messages_after(CHAT_ID, 0)?;
        let injections = store.list_injected_memories(CHAT_ID)?;
        Ok((rows, injections))
    })
    .await;
    assert_eq!(rows.len(), 5);
    assert_eq!(
        injections.len(),
        2,
        "the injection rows survive: nothing is pruned before a second digest"
    );
    let expected = LiveContext::rebuild(
        TEST_PREAMBLE.to_string(),
        &rows,
        &injections,
        &HashMap::new(),
    );
    let rebuilt = restarted
        .context_snapshot()
        .await
        .expect("the context snapshot succeeds");
    assert_eq!(
        rebuilt,
        expected.items(),
        "the rebuild is bit-identical to LiveContext::rebuild over the persisted rows"
    );

    // Rule C2 placement, explicitly: the injection "edge-1" sits
    // directly after the m2 item, "edge-2" directly after the m4 item.
    // All five messages are present: prev is None, so the cutoff is 0
    // and nothing is pruned.
    assert_eq!(rebuilt.len(), 8);
    assert_preamble(&rebuilt[0]);
    assert_eq!(rebuilt[1].range_tag, Some(RangeTag::single(1)));
    assert_eq!(rebuilt[2].range_tag, Some(RangeTag::single(2)));
    assert_eq!(rebuilt[3].kind, ContextItemKind::RecallInjection);
    assert_eq!(rebuilt[3].role, ContextRole::Assistant);
    assert_eq!(rebuilt[3].content, "I remember: Alice likes GRPO");
    assert_eq!(rebuilt[3].range_tag, Some(RangeTag::single(2)));
    assert_eq!(rebuilt[4].range_tag, Some(RangeTag::single(3)));
    assert_eq!(rebuilt[5].range_tag, Some(RangeTag::single(4)));
    assert_eq!(rebuilt[6].kind, ContextItemKind::RecallInjection);
    assert_eq!(rebuilt[6].content, "I remember: Bob runs 3e-6");
    assert_eq!(rebuilt[6].range_tag, Some(RangeTag::single(4)));
    assert_eq!(rebuilt[7].range_tag, Some(RangeTag::single(5)));

    // --- Digest 2 over the tail (3, b2]. ---
    // The tail above 3 already holds m4 and m5, so the trigger fires at
    // the m6 intake; the pipeline reads the tail at execution time:
    // b2 is 6, 7, or 8. The tail above b2 then holds at most two rows,
    // so no third digest can follow.
    for msg in [
        message("m6", 6, "u1", "Alice", "3e-6 with warmup"),
        message("m7", 7, "u2", "Bob", "nice. ours needed cosine decay"),
        message("m8", 8, "u1", "Alice", "let us compare runs on friday"),
    ] {
        restarted
            .send_event(InboundEvent::Message(msg))
            .await
            .expect("the actor inbox is open");
    }
    let b2 = wait_for_boundary(&restarted, 6, DIGEST_TIMEOUT).await;
    assert!(
        (6..=8).contains(&b2),
        "the second batch covers (3, b2], got {b2}"
    );
    let session = restarted.snapshot().await.expect("the snapshot succeeds");
    assert_eq!(session.prev_digest_boundary_msg_id, Some(3));
    assert_eq!(session.last_digest_boundary_msg_id, b2);

    // The lagged view equals the rebuild over the rows above the
    // PREVIOUS boundary and the surviving injection rows.
    let (rows, injections) = blocking_store_call(&fixture.store, move |store| {
        let rows = store.list_messages_after(CHAT_ID, 3)?;
        let injections = store.list_injected_memories(CHAT_ID)?;
        Ok((rows, injections))
    })
    .await;
    let expected = LiveContext::rebuild(
        TEST_PREAMBLE.to_string(),
        &rows,
        &injections,
        &HashMap::new(),
    );
    let before_shutdown = restarted
        .context_snapshot()
        .await
        .expect("the context snapshot succeeds");
    assert_eq!(
        before_shutdown,
        expected.items(),
        "the lagged view equals the rebuild above the previous boundary"
    );

    // Rule C3 explicitly: m1..m3 AND the injection "edge-1" at position
    // 2 are gone (tags <= prev = 3); the chunk (3, b2] stays.
    assert!(
        before_shutdown[1..].iter().all(|item| item
            .range_tag
            .as_ref()
            .expect("a range tag")
            .last_msg_id
            > 3),
        "every item with a tag <= 3 is gone"
    );
    assert!(
        !before_shutdown
            .iter()
            .any(|item| item.content.contains("Alice likes GRPO")),
        "the injection at position 2 lagged out"
    );
    assert_eq!(before_shutdown[1].range_tag, Some(RangeTag::single(4)));
    // The injection "edge-2" at position 4 survives, directly after m4.
    assert_eq!(before_shutdown[2].kind, ContextItemKind::RecallInjection);
    assert_eq!(before_shutdown[2].content, "I remember: Bob runs 3e-6");
    assert_eq!(before_shutdown[2].range_tag, Some(RangeTag::single(4)));

    // Section 10.2 step 4: the dedup set is pruned at the same cutoff.
    assert_eq!(injections.len(), 1);
    assert_eq!(injections[0].edge_id, "edge-2");

    // --- Second restart: the lagged view survives restarts. ---
    restarted
        .shutdown()
        .await
        .expect("the actor reports no error");
    // No scripted graphs: nothing can digest after this restart. The
    // tail above b2 holds at most two rows and no intake follows.
    let again = spawn_actor(
        &fixture,
        digest_config(3),
        Arc::new(ScriptedExtractor::with_graphs(vec![])),
        None,
        t0(),
    );
    let session = again.snapshot().await.expect("the snapshot succeeds");
    assert_eq!(session.prev_digest_boundary_msg_id, Some(3));
    assert_eq!(session.last_digest_boundary_msg_id, b2);
    let rebuilt = again
        .context_snapshot()
        .await
        .expect("the context snapshot succeeds");
    assert_eq!(
        rebuilt, before_shutdown,
        "the restart rebuilds the lagged view bit-identically"
    );
    again.shutdown().await.expect("the actor reports no error");
}
