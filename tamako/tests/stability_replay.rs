//! The M6 stability replay loop (specs.md Sections 6, 8, 9, 10). One
//! long run against the REAL SQLite store, the REAL LadybugDB backend,
//! the REAL digest pipeline (scripted extractor), and the REAL shallow
//! recall worker (scripted relevance gate), driven through the REAL
//! actor. Ten iterations of mixed traffic — plain messages, a mention
//! (forced wake, Section 8.1), a threshold wake, a tick-driven interval
//! wake (Section 8.3), and one digest crossing per iteration (Section
//! 8.2) — with a FULL restart inside every iteration.
//!
//! Assertions of the run:
//! - Rule P1 / Section 6.1 rule 4: every restart rebuilds the context
//!   bit-identically, and the session snapshot survives the restart;
//! - specs.md Section 10.3: no dead letters;
//! - Rule B1: the raw log holds exactly the fed inbound rows plus the
//!   bot's own outbound rows;
//! - the digest boundary never regresses (one digest per iteration);
//! - `injection_wakes_total` grows (Section 12 metric);
//! - the counters stay consistent (`participations_total <= wakes_total`).
//!
//! Scripted-double exhaustion semantics (tamako-agent): the relevance
//! gate and the gate degrade to "nothing" / silence when exhausted, the
//! extractor degrades to an empty graph, but the reply generator FAILS
//! when exhausted. The reply supply therefore covers exactly the 30
//! participations of the run; the other supplies carry margin.
//!
//! Timer note (the wake_replay.rs pattern): every timestamp sits one
//! day in the FUTURE, so the built-in 1-second ticker always sees a
//! negative elapsed time and stays inert. Only the explicit `Tick`
//! commands drive timer fires. `digest_timeout` is near-infinite: a
//! future message timestamp would otherwise fire the Section 8.2
//! timeout fallback against the wall-clock `last_digest_at` on every
//! intake with a non-empty tail.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tamako_adapter_mock::MockAdapter;
use tamako_agent::{
    AgentDigestPipeline, AgentError, ExtractedNode, ExtractedNodeType, KnowledgeGraph,
    PipelineConfig, RelevanceGate, RelevanceInput, ScriptedExtractor, ScriptedGate,
    ScriptedRelevanceGate, ScriptedReplyGenerator, ShallowRecall,
};
use tamako_core::actor::{
    spawn_group_actor, ActorCommand, GroupActorHandle, GroupActorParams, DEFAULT_INBOX_CAPACITY,
};
use tamako_core::adapter::PlatformAdapter;
use tamako_core::config::TriggerConfig;
use tamako_core::context::ContextItemKind;
use tamako_core::digest::DigestPipeline;
use tamako_core::event::{InboundEvent, NormalizedMessage, OutboundAction};
use tamako_core::summary::{ScriptedSummary, SummaryProvider};
use tamako_core::wake::{GateDecision, ParticipationGate, ReplyGenerator, WakeServices};
use tamako_memory::identifiers::{alias_id, concept_id, person_id};
use tamako_memory::{LbugBackend, MemoryBackend, MemoryBatch, MemoryEdge, MemoryNode, NodeType};
use tamako_store::{Direction, MessageRow, Store};
use time::OffsetDateTime;
use tokio::sync::mpsc;

const CHAT_ID: &str = "-1001234567890";

/// The preamble of every actor of this suite.
const TEST_PREAMBLE: &str = "test preamble";

/// The bounded waits of this suite. The scripted doubles return
/// immediately, so a wake or digest round trip is milliseconds; five
/// seconds is generous enough for a loaded CI machine.
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);
/// The poll interval of every bounded wait.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// The regression tripwire of the run. The loop finishes in a few
/// seconds; the bound stays generous for a loaded CI machine.
const RUNTIME_BUDGET: Duration = Duration::from_secs(60);

/// The iteration count of the loop.
const ITERATIONS: u32 = 10;

/// A near-infinite digest timeout: the future clock of the replay must
/// never fire the Section 8.2 timeout fallback (module header).
const HUGE_DIGEST_TIMEOUT: Duration = Duration::from_secs(u64::MAX / 4);

/// The fixed base time of the replay: one day in the future, so the
/// built-in ticker stays inert (module header).
fn t0() -> OffsetDateTime {
    OffsetDateTime::now_utc() + Duration::from_secs(24 * 60 * 60)
}

/// The stability configuration: count-driven wakes of three messages,
/// a 60 s interval with a fixed jitter (the explicit ticks fire the
/// interval path), no floor, and a digest every eight tail rows.
fn stability_config() -> TriggerConfig {
    TriggerConfig {
        wake_msg_count: 3,
        wake_interval: Duration::from_secs(60),
        // A fixed jitter makes the current interval exactly 60 s.
        wake_jitter_min: 1.0,
        wake_jitter_max: 1.0,
        wake_floor: Duration::ZERO,
        digest_max_messages: 8,
        digest_timeout: HUGE_DIGEST_TIMEOUT,
        ..TriggerConfig::default()
    }
}

struct Fixture {
    // The TempDir must outlive the store and the backend.
    _dir: tempfile::TempDir,
    store: Arc<Store>,
    memory: Arc<LbugBackend>,
}

async fn make_fixture() -> Fixture {
    let dir = tempfile::tempdir().expect("a temporary data root");
    let store = Arc::new(Store::new(dir.path().to_path_buf()));
    let memory = Arc::new(LbugBackend::new(dir.path().to_path_buf()));
    memory.ensure_schema(CHAT_ID).await.expect("the schema");
    Fixture {
        _dir: dir,
        store,
        memory,
    }
}

fn person_node(user_id: &str, name: &str, at: OffsetDateTime) -> MemoryNode {
    MemoryNode {
        id: person_id(user_id),
        name: name.to_string(),
        node_type: NodeType::Person,
        created_at: at,
        updated_at: at,
        properties: None,
    }
}

fn concept_node(name: &str, at: OffsetDateTime) -> MemoryNode {
    MemoryNode {
        id: concept_id(name),
        name: name.to_string(),
        node_type: NodeType::Concept,
        created_at: at,
        updated_at: at,
        properties: None,
    }
}

fn fact_edge(
    source_id: &str,
    target_id: &str,
    relationship: &str,
    text: &str,
    at: OffsetDateTime,
) -> MemoryEdge {
    MemoryEdge {
        source_id: source_id.to_string(),
        target_id: target_id.to_string(),
        relationship_name: relationship.to_string(),
        valid_at: at,
        invalid_at: None,
        edge_text: text.to_string(),
        created_at: at,
        updated_at: at,
        properties: None,
    }
}

/// Seeds the recall graph directly (the seeding style of recall.rs):
/// Alice (u1) with one fact edge to the GRPO concept, plus the surface
/// form "grpo" of the concept (a known_as edge, Section 7.4 step 2).
/// A wake then finds candidates through the sender person (Section 8.1
/// step 1 of the database spec) AND through the alias term "grpo" of
/// the message texts (step 2).
async fn seed_graph(fixture: &Fixture, at: OffsetDateTime) {
    let alice = person_node("u1", "Alice", at);
    let grpo = concept_node("GRPO", at);
    let alias = MemoryNode {
        id: alias_id("grpo"),
        name: "grpo".to_string(),
        node_type: NodeType::Alias,
        created_at: at,
        updated_at: at,
        properties: None,
    };
    let batch = MemoryBatch {
        batch_id: "seed".to_string(),
        nodes: vec![alice.clone(), grpo.clone(), alias.clone()],
        edges: vec![
            fact_edge(&alice.id, &grpo.id, "related_to", "Alice likes GRPO.", at),
            fact_edge(
                &grpo.id,
                &alias.id,
                "known_as",
                "grpo is a surface form of GRPO.",
                at,
            ),
        ],
    };
    fixture
        .memory
        .upsert_batch(CHAT_ID, &batch)
        .await
        .expect("the seed upsert");
}

/// The scripted doubles of the whole run. The doubles are shared across
/// every respawn: their FIFO queues are the supply of the FULL run.
struct WakeDoubles {
    relevance: Arc<ScriptedRelevanceGate>,
    gate: Arc<ScriptedGate>,
    reply: Arc<ScriptedReplyGenerator>,
}

/// An `Arc` adapter for the scripted relevance gate (the pattern of
/// tamako/tests/recall_replay.rs). `ShallowRecall` takes its gate by
/// value; the test keeps an `Arc` handle for the call assertions.
struct SharedRelevanceGate(Arc<ScriptedRelevanceGate>);

impl RelevanceGate for SharedRelevanceGate {
    fn select<'a>(
        &'a self,
        input: &'a RelevanceInput,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<usize>, AgentError>> + Send + 'a>> {
        self.0.select(input)
    }
}

/// The trivial scripted graph: one Person node, no edges. "Alice" is a
/// sender of every iteration, so the mention map always binds her
/// (specs.md Section 10.1) and the MERGEs stay idempotent (Section
/// 10.3).
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

/// Spawns the real actor on the shared data root with the real pipeline
/// over the scripted extractor, the real shallow recall over the
/// scripted relevance gate, and the scripted gate/reply doubles.
fn spawn_on(
    fixture: &Fixture,
    config: &TriggerConfig,
    started_at: OffsetDateTime,
    doubles: &WakeDoubles,
    extractor: Arc<ScriptedExtractor>,
    summary: Arc<ScriptedSummary>,
    outbound: mpsc::Sender<OutboundAction>,
) -> GroupActorHandle {
    // Open the group store BEFORE the actor spawns: a concurrent
    // `open_group` of the same store.db races the WAL pragma of the
    // actor startup (the note of tamako/tests/wake_replay.rs).
    fixture
        .store
        .open_group(CHAT_ID)
        .expect("open_group succeeds");
    let digest: Arc<dyn DigestPipeline> = Arc::new(AgentDigestPipeline::new(
        Arc::clone(&fixture.store),
        Arc::clone(&fixture.memory),
        extractor,
        PipelineConfig::default(),
    ));
    let recall = ShallowRecall::new(
        Arc::clone(&fixture.store),
        Arc::clone(&fixture.memory),
        SharedRelevanceGate(Arc::clone(&doubles.relevance)),
        config.recall_injection_cap,
    );
    spawn_group_actor(GroupActorParams {
        chat_id: CHAT_ID.to_string(),
        store: Arc::clone(&fixture.store),
        memory: Arc::clone(&fixture.memory),
        config: config.clone(),
        started_at,
        inbox_capacity: DEFAULT_INBOX_CAPACITY,
        preamble: TEST_PREAMBLE.to_string(),
        digest: Some(digest),
        post_digest_hook: None,
        wake: Some(WakeServices {
            recall: Arc::new(recall),
            gate: Arc::clone(&doubles.gate) as Arc<dyn ParticipationGate>,
            reply: Arc::clone(&doubles.reply) as Arc<dyn ReplyGenerator>,
        }),
        warmup: None,
        summary_provider: Some(summary as Arc<dyn SummaryProvider>),
        outbound: Some(outbound),
        bot_name: Some("Tamako".to_string()),
    })
}

/// The outbound plumbing of the binary (the wake_replay.rs pattern):
/// one channel, one forwarder task into a mock-adapter sink. ONE pump
/// serves the whole run: every respawn receives a fresh clone of the
/// sender, so the recorded actions accumulate across the restarts.
struct OutboundPump {
    tx: mpsc::Sender<OutboundAction>,
    sink: Arc<MockAdapter>,
    forwarder: tokio::task::JoinHandle<()>,
}

fn outbound_pump() -> OutboundPump {
    let (tx, mut rx) = mpsc::channel::<OutboundAction>(100);
    let sink = Arc::new(MockAdapter::from_fixture(
        tamako_adapter_mock::ReplayFixture {
            chat_id: CHAT_ID.to_string(),
            events: vec![],
        },
    ));
    let forwarder = {
        let sink = Arc::clone(&sink);
        tokio::spawn(async move {
            while let Some(action) = rx.recv().await {
                sink.execute(action)
                    .await
                    .expect("the mock adapter never fails");
            }
        })
    };
    OutboundPump {
        tx,
        sink,
        forwarder,
    }
}

/// Graceful shutdown of the pump.
async fn shutdown_pump(pump: OutboundPump) {
    drop(pump.tx);
    pump.forwarder.await.expect("the forwarder joins");
}

/// Polls the sink until at least `min` outbound actions are recorded
/// or the timeout elapses (the pattern of tamako/tests/wake_replay.rs).
/// The forwarder records an action only after the actor's send path
/// persisted the outbound raw-log row (Rule B1), so this wait orders
/// the wake completion handler of every iteration.
async fn wait_for_actions(sink: &Arc<MockAdapter>, min: usize) {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    loop {
        let actions = sink.recorded_actions();
        if actions.len() >= min {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out after {WAIT_TIMEOUT:?} waiting for {min} outbound actions (got {})",
            actions.len()
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Polls `snapshot()` until the digest boundary reaches `min` or the
/// timeout elapses (the pattern of tamako/tests/digest_replay.rs). A
/// snapshot that shows the boundary proves the whole digest completion
/// handler (Rule C3 removal, the Section 10.2 step 4 prune, the session
/// persistence, the re-evaluation) already ran. Deterministic.
async fn wait_for_boundary(handle: &GroupActorHandle, min: i64) -> i64 {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    loop {
        let session = handle.snapshot().await.expect("the snapshot succeeds");
        if session.last_digest_boundary_msg_id >= min {
            return session.last_digest_boundary_msg_id;
        }
        assert!(
            Instant::now() < deadline,
            "timed out after {WAIT_TIMEOUT:?} waiting for the digest boundary to reach {min} \
             (current boundary: {})",
            session.last_digest_boundary_msg_id
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

/// The parsed value of a counter key; a missing key reads as zero.
async fn counter_value(store: &Arc<Store>, key: &str) -> u64 {
    counter(store, key)
        .await
        .map(|value| value.parse().expect("a counter is a number"))
        .unwrap_or(0)
}

/// Sends one inbound message event.
async fn send_message(handle: &GroupActorHandle, message: NormalizedMessage) {
    handle
        .send_event(InboundEvent::Message(message))
        .await
        .expect("the actor inbox is open");
}

/// A plain inbound message of the loop.
fn message(
    id: &str,
    at: OffsetDateTime,
    sender_id: &str,
    name: &str,
    text: &str,
) -> NormalizedMessage {
    NormalizedMessage {
        platform_msg_id: id.to_string(),
        timestamp: at,
        sender_id: sender_id.to_string(),
        sender_display_name: name.to_string(),
        username: None,
        text: text.to_string(),
        reply_to_platform_msg_id: None,
        mentions_bot: false,
        is_reply_to_bot: false,
    }
}

/// A mention of the bot (a forced wake, specs.md Section 8.1).
fn mention(id: &str, at: OffsetDateTime, text: &str) -> NormalizedMessage {
    let mut message = message(id, at, "u1", "Alice", text);
    message.mentions_bot = true;
    message
}

/// The stability loop. Iteration i (0-based) adds exactly nine raw-log
/// rows (rows 9i+1..=9i+9), paced so the row ids stay deterministic:
///
/// - rows 9i+1..=9i+3: three plain messages; the third reaches
///   wake_msg_count and starts the threshold wake; row 9i+4 is the
///   outbound row of its reply (Rule B1);
/// - row 9i+5: a mention; the forced wake bypasses the gate (Section
///   8.1); row 9i+6 is the second reply row;
/// - rows 9i+7..=9i+8: two plain messages; an explicit `Tick` 63 s
///   after the mention fires the interval path (Section 8.3; the fixed
///   jitter makes the current interval exactly 60 s); row 9i+9 is the
///   third reply row;
/// - the tail crosses digest_max_messages = 8 at the row 9i+7 or 9i+8
///   intake: exactly one digest per iteration (the tail above the new
///   boundary holds at most one row, so no cascade follows — the
///   re-evaluation is inert and the actor is quiescent at capture
///   time).
///
/// The human messages between the wakes keep `consecutive_bot_msgs`
/// below the monologue limit (Section 8.5): the lock never engages.
///
/// Every iteration ends with a FULL restart: capture the context and
/// session snapshots, shut the actor down, respawn on the same data
/// root with the same config and the same `started_at`, and assert the
/// Rule P1 bit-identical rebuild and the session match. The respawned
/// actor continues the loop.
#[tokio::test]
async fn stability_loop_digests_wakes_injections_and_restarts() {
    let started = Instant::now();
    let started_at = t0();
    let fixture = make_fixture().await;
    seed_graph(&fixture, started_at).await;
    let config = stability_config();

    // The scripted supply of the FULL run (the exhaustion semantics of
    // the module header):
    // - 20 gate decisions: two unforced wakes per iteration, each
    //   participating at its deterministic tail row;
    // - 30 replies: exactly the 30 participations of the run;
    // - 40 relevance selections "the first candidate": every wake with
    //   candidates injects; an exhausted queue would only select
    //   nothing, so the margin is free;
    // - 12 extraction graphs: one digest per iteration plus margin.
    let decisions: Vec<GateDecision> = (0..ITERATIONS)
        .flat_map(|i| {
            let base = 9 * i64::from(i);
            [
                GateDecision {
                    participate: true,
                    target_row_id: Some(base + 3),
                    reason: None,
                },
                GateDecision {
                    participate: true,
                    target_row_id: Some(base + 8),
                    reason: None,
                },
            ]
        })
        .collect();
    let replies: Vec<String> = (1..=3 * ITERATIONS).map(|n| format!("r{n}")).collect();
    let doubles = WakeDoubles {
        relevance: Arc::new(ScriptedRelevanceGate::with_selections(
            (0..40).map(|_| vec![0]).collect(),
        )),
        gate: Arc::new(ScriptedGate::with_decisions(decisions)),
        reply: Arc::new(ScriptedReplyGenerator::with_replies(replies)),
    };
    let extractor = Arc::new(ScriptedExtractor::with_graphs(
        (0..12).map(|_| trivial_graph()).collect(),
    ));
    // The summarizer supply (decision 62): one summary per digest from
    // the second digest on, plus margin. An exhausted queue would fail
    // the call and defer the removal, so the margin is free.
    let summaries = Arc::new(ScriptedSummary::with_summaries(
        (1..=ITERATIONS + 2)
            .map(|n| format!("summary of chunk {n}"))
            .collect(),
    ));

    let pump = outbound_pump();
    let mut handle = spawn_on(
        &fixture,
        &config,
        started_at,
        &doubles,
        Arc::clone(&extractor),
        Arc::clone(&summaries),
        pump.tx.clone(),
    );

    let mut expected_actions = 0_usize;
    let mut fed_inbound = 0_u64;
    let mut prev_boundary = 0_i64;
    let mut last_injections = 0_u64;
    let mut first_iteration_injections = None;
    for i in 0..ITERATIONS {
        let base = started_at + time::Duration::seconds(120 * i64::from(i));
        let id = |k: u32| format!("i{i}m{k}");

        // The threshold wake (rows 9i+1..=9i+3, reply at 9i+4).
        send_message(
            &handle,
            message(
                &id(1),
                base,
                "u1",
                "Alice",
                "the grpo run diverged twice last night",
            ),
        )
        .await;
        send_message(
            &handle,
            message(
                &id(2),
                base + time::Duration::seconds(1),
                "u2",
                "Bob",
                "ours converged fine",
            ),
        )
        .await;
        send_message(
            &handle,
            message(
                &id(3),
                base + time::Duration::seconds(2),
                "u1",
                "Alice",
                "lucky. which learning rate?",
            ),
        )
        .await;
        fed_inbound += 3;
        expected_actions += 1;
        wait_for_actions(&pump.sink, expected_actions).await;

        // The forced wake (row 9i+5, reply at 9i+6).
        send_message(
            &handle,
            mention(
                &id(4),
                base + time::Duration::seconds(3),
                "Tamako, any grpo tips?",
            ),
        )
        .await;
        fed_inbound += 1;
        expected_actions += 1;
        wait_for_actions(&pump.sink, expected_actions).await;

        // The digest crossing: rows 9i+7..=9i+8 push the tail past
        // digest_max_messages = 8. Wait for the boundary BEFORE the
        // tick: this keeps the test deterministic (the interval wake's
        // recall never overlaps an in-flight digest). Concurrent
        // read+write on one group's graph is safe in production: the
        // M6 fix serializes ALL per-group operations inside
        // `LbugBackend::with_conn` (adr-0001 addendum 2026-08-08;
        // lbug 0.18 `Send + Sync` does not imply read-during-write
        // safety). The regression test is
        // tamako-memory/tests/lbug_concurrent_access.rs.
        send_message(
            &handle,
            message(
                &id(5),
                base + time::Duration::seconds(4),
                "u2",
                "Bob",
                "3e-6 with warmup worked for us",
            ),
        )
        .await;
        send_message(
            &handle,
            message(
                &id(6),
                base + time::Duration::seconds(5),
                "u1",
                "Alice",
                "we compare the grpo runs on friday",
            ),
        )
        .await;
        fed_inbound += 2;
        // Exactly one digest per iteration: the boundary advances by at
        // least seven rows and never regresses.
        let boundary = wait_for_boundary(&handle, prev_boundary + 7).await;
        assert!(
            boundary > prev_boundary,
            "iteration {i}: the digest boundary regressed ({prev_boundary} -> {boundary})"
        );
        prev_boundary = boundary;

        // The interval wake (reply at row 9i+9). The tick arrives 63 s
        // after the mention wake reset the scheduler; the digest is
        // done, so no wake and no digest overlap.
        handle
            .send(ActorCommand::Tick(base + time::Duration::seconds(66)))
            .await
            .expect("the actor inbox is open");
        expected_actions += 1;
        wait_for_actions(&pump.sink, expected_actions).await;

        // Section 12: the injection-rate metric never decreases; the
        // run asserts the overall growth after the loop.
        let injections = counter_value(&fixture.store, "injection_wakes_total").await;
        assert!(
            injections >= last_injections,
            "iteration {i}: injection_wakes_total decreased ({last_injections} -> {injections})"
        );
        if i == 0 {
            first_iteration_injections = Some(injections);
        }
        last_injections = injections;

        // The full restart of this iteration. The actor is quiescent:
        // every wake of the iteration is paced through the sink, and
        // the digest completion handler ran (the boundary wait).
        let context_before = handle
            .context_snapshot()
            .await
            .expect("the context snapshot succeeds");
        let session_before = handle.snapshot().await.expect("the snapshot succeeds");
        handle.shutdown().await.expect("the actor reports no error");
        handle = spawn_on(
            &fixture,
            &config,
            started_at,
            &doubles,
            Arc::clone(&extractor),
            Arc::clone(&summaries),
            pump.tx.clone(),
        );
        let context_after = handle
            .context_snapshot()
            .await
            .expect("the context snapshot succeeds");
        assert_eq!(
            context_after, context_before,
            "iteration {i}: the restart rebuilds the context bit-identically (Rule P1)"
        );
        let session_after = handle.snapshot().await.expect("the snapshot succeeds");
        assert_eq!(
            session_after, session_before,
            "iteration {i}: the session survives the restart"
        );

        // Decision 62 invariants across the restart: at most two
        // summary items, each backed by a persisted context_summaries
        // row of the same range (the keep-two window is a VIEW of the
        // table, Rule P1).
        let summary_ranges: Vec<String> = context_after
            .iter()
            .filter(|item| item.kind == ContextItemKind::Summary)
            .map(|item| item.content.clone())
            .collect();
        assert!(
            summary_ranges.len() <= 2,
            "iteration {i}: more than two summary items: {summary_ranges:?}"
        );
        for content in &summary_ranges {
            let tag_end = content.find('>').expect("the summary tag closes");
            let range = content
                .strip_prefix(r#"<summary range=""#)
                .and_then(|rest| rest[..tag_end - r#"<summary range=""#.len()].strip_suffix('"'))
                .expect("the summary item carries a range attribute");
            let (first, last) = range
                .split_once('-')
                .expect("the range attribute is first-last");
            let first: i64 = first.parse().expect("a numeric range bound");
            let last: i64 = last.parse().expect("a numeric range bound");
            let store = Arc::clone(&fixture.store);
            let row = tokio::task::spawn_blocking(move || {
                store.find_context_summary(CHAT_ID, first, last)
            })
            .await
            .expect("the blocking task joins")
            .expect("find_context_summary succeeds");
            assert!(
                row.is_some(),
                "iteration {i}: the summary item ({first}, {last}] has no persisted row"
            );
        }
    }

    // --- The final state. ---
    // Decision 62: one summary per digest from the second digest on —
    // ITERATIONS digests minus the first = ITERATIONS - 1 summary rows,
    // and the summarizer was never exhausted.
    {
        let store = Arc::clone(&fixture.store);
        let summary_rows =
            tokio::task::spawn_blocking(move || store.list_newest_context_summaries(CHAT_ID, 1000))
                .await
                .expect("the blocking task joins")
                .expect("list_newest_context_summaries succeeds");
        assert_eq!(
            summary_rows.len(),
            ITERATIONS as usize - 1,
            "one summary per removed chunk: {summary_rows:?}"
        );
        assert_eq!(summaries.inputs().len(), ITERATIONS as usize - 1);
    }

    // specs.md Section 10.3: no dead letters. The scripted extractor
    // never fails, so the table stays empty.
    {
        let store = Arc::clone(&fixture.store);
        let dead_letters = tokio::task::spawn_blocking(move || store.list_dead_letters(CHAT_ID))
            .await
            .expect("the blocking task joins")
            .expect("list_dead_letters succeeds");
        assert!(dead_letters.is_empty(), "no dead letters: {dead_letters:?}");
    }

    // Rule B1: the raw log holds exactly the fed inbound rows plus the
    // bot's own outbound rows.
    let rows: Vec<MessageRow> = {
        let store = Arc::clone(&fixture.store);
        tokio::task::spawn_blocking(move || store.list_messages(CHAT_ID))
            .await
            .expect("the blocking task joins")
            .expect("list_messages succeeds")
    };
    let outbound: Vec<&MessageRow> = rows
        .iter()
        .filter(|row| row.direction == Direction::Outbound)
        .collect();
    assert_eq!(outbound.len(), expected_actions);
    assert!(
        outbound
            .iter()
            .all(|row| row.sender_display_name == "Tamako"),
        "every outbound row is a bot row"
    );
    assert_eq!(
        rows.len() as u64,
        fed_inbound + expected_actions as u64,
        "the raw log holds the fed inbound rows plus the outbound rows (Rule B1)"
    );

    // The counters (specs.md Section 12). The loop paced every wake to
    // completion, so the values are exact.
    assert_eq!(
        counter(&fixture.store, "wakes_total").await.as_deref(),
        Some("30"),
        "three wakes per iteration (threshold, forced, interval)"
    );
    assert_eq!(
        counter(&fixture.store, "participations_total")
            .await
            .as_deref(),
        Some("30"),
        "every wake participates"
    );
    let injections = counter_value(&fixture.store, "injection_wakes_total").await;
    assert!(
        injections > first_iteration_injections.expect("iteration 0 ran"),
        "injection_wakes_total grows over the run (after iteration 0: {:?}, final: {injections})",
        first_iteration_injections
    );
    assert!(
        injections <= 30,
        "an injection wake is a wake: {injections} <= wakes_total"
    );

    // The scripted doubles saw exactly the planned calls: two gate
    // calls per iteration, three reply generations per iteration.
    assert_eq!(doubles.gate.inputs().len(), 2 * ITERATIONS as usize);
    assert_eq!(doubles.reply.requests().len(), 3 * ITERATIONS as usize);
    // Every extraction graph of the supply was enough: the extractor
    // never degraded to the empty graph.
    assert_eq!(extractor.inputs().len(), ITERATIONS as usize);

    handle.shutdown().await.expect("the actor reports no error");
    shutdown_pump(pump).await;

    let elapsed = started.elapsed();
    println!("stability loop: {ITERATIONS} iterations in {elapsed:.2?}");
    assert!(
        elapsed < RUNTIME_BUDGET,
        "the stability loop exceeded the {RUNTIME_BUDGET:?} budget: {elapsed:.2?}"
    );
}
