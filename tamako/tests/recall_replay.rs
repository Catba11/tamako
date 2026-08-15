//! End-to-end tests of the M5 shallow recall and the full injection
//! protocol (specs.md Sections 9.1-9.6 and 10.2 step 4) against the
//! REAL SQLite store, the REAL LadybugDB backend, and the real actor
//! (`spawn_group_actor`), with scripted relevance/participation/reply
//! doubles (tamako-agent). The harness mirrors tamako/tests/wake_replay.rs
//! (wake path) and tamako/tests/context_replay.rs (restart and digest
//! path). Env-hermetic: scripted doubles only; no LLM keys, no network.
//!
//! Coverage:
//! - Section 9.4 / Rule C2: a wake with a graph hit appends exactly one
//!   "I remember: ..." — since the XML context rendering round, a
//!   "<memory>...</memory>" RecallInjection assistant item at the tail row
//!   of the wake, writes one `injected_memories` row per edge id, and
//!   bumps `injection_wakes_total`;
//! - Section 9.6: the injection rides the gate input (the "Injected
//!   memories" section) and the reply-model snapshot;
//! - Section 9.3: the same edge is never injected twice in one chunk;
//! - Sections 9.1/9.2: zero candidates means no injection and NO
//!   relevance-gate call;
//! - Rule P1: a restart rebuilds the injection bit-identically;
//! - Section 10.2 step 4 / Rule C3: the second digest prunes the
//!   injection rows at the one-chunk-lag boundary.
//!
//! Timer note: same policy as wake_replay.rs — `wake_interval` is
//! near-infinite and the wake floor is zero, so only the message count
//! drives wakes and the built-in 1-second ticker stays inert.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tamako_adapter_mock::MockAdapter;
use tamako_agent::{
    AgentDigestPipeline, AgentError, ExtractedNode, ExtractedNodeType, KnowledgeGraph,
    PipelineConfig, RelevanceGate, RelevanceInput, ScriptedExtractor, ScriptedGate,
    ScriptedRelevanceGate, ScriptedReplyGenerator, ShallowRecall,
};
use tamako_core::actor::{
    spawn_group_actor, GroupActorHandle, GroupActorParams, DEFAULT_INBOX_CAPACITY,
};
use tamako_core::adapter::PlatformAdapter;
use tamako_core::config::TriggerConfig;
use tamako_core::context::{ContextItem, ContextItemKind, ContextRole, RangeTag};
use tamako_core::digest::DigestPipeline;
use tamako_core::event::{InboundEvent, NormalizedMessage, OutboundAction};
use tamako_core::wake::{GateDecision, ParticipationGate, ReplyGenerator, WakeServices};
use tamako_memory::identifiers::{alias_id, concept_id, person_id};
use tamako_memory::{LbugBackend, MemoryBackend, MemoryBatch, MemoryEdge, MemoryNode, NodeType};
use tamako_store::{InjectedMemoryRow, Store};
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

/// A near-infinite wake interval: the interval path of the trigger
/// never fires, so only the message count drives wakes (the pattern of
/// tamako/tests/wake_replay.rs).
const HUGE_INTERVAL: Duration = Duration::from_secs(u64::MAX / 4);

/// The fixed base time of every hand-crafted message of this suite
/// (the pattern of tamako/tests/digest_replay.rs).
fn t0() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("a valid unix timestamp")
}

/// The wake configuration of this suite: count-driven wakes of three
/// messages, no floor, no interval fires.
fn wake_config() -> TriggerConfig {
    TriggerConfig {
        wake_msg_count: 3,
        wake_floor: Duration::ZERO,
        wake_interval: HUGE_INTERVAL,
        ..TriggerConfig::default()
    }
}

/// A hand-crafted inbound message.
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

/// A stopword-only message text: the recall tokenizer drops every
/// token, so the sender Person entry is the ONLY recall entry. The
/// candidate set of the wake is then exactly the neighbor set of the
/// seeded person.
const STOPWORD_TEXT: &str = "ok ok thanks";

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

fn person_node(user_id: &str, name: &str) -> MemoryNode {
    MemoryNode {
        id: person_id(user_id),
        name: name.to_string(),
        node_type: NodeType::Person,
        created_at: t0(),
        updated_at: t0(),
        properties: None,
    }
}

fn concept_node(name: &str) -> MemoryNode {
    MemoryNode {
        id: concept_id(name),
        name: name.to_string(),
        node_type: NodeType::Concept,
        created_at: t0(),
        updated_at: t0(),
        properties: None,
    }
}

fn fact_edge(
    source_id: &str,
    target_id: &str,
    text: &str,
    created_at: OffsetDateTime,
) -> MemoryEdge {
    MemoryEdge {
        source_id: source_id.to_string(),
        target_id: target_id.to_string(),
        relationship_name: "related_to".to_string(),
        valid_at: t0(),
        invalid_at: None,
        edge_text: text.to_string(),
        created_at,
        updated_at: created_at,
        properties: None,
    }
}

/// Seeds the graph directly (the seeding style of recall.rs): one
/// Person node for `user_id` plus one fact edge per (concept, text,
/// created_at) triple.
async fn seed_person_facts(
    fixture: &Fixture,
    user_id: &str,
    name: &str,
    facts: &[(&str, &str, i64)],
) {
    let person = person_node(user_id, name);
    let mut nodes = vec![person.clone()];
    let mut edges = Vec::new();
    for (concept, text, created_seconds) in facts {
        let concept = concept_node(concept);
        edges.push(fact_edge(
            &person.id,
            &concept.id,
            text,
            t0() + time::Duration::seconds(*created_seconds),
        ));
        nodes.push(concept);
    }
    let batch = MemoryBatch {
        batch_id: "seed".to_string(),
        nodes,
        edges,
    };
    fixture
        .memory
        .upsert_batch(CHAT_ID, &batch)
        .await
        .expect("the seed upsert");
}

/// An Alias node of the graph (the `alias_node_seeded` style of
/// recall.rs). `alias_id` normalizes the surface form internally
/// (Section 7.1 of the database spec).
fn alias_node(surface_form: &str) -> MemoryNode {
    MemoryNode {
        id: alias_id(surface_form),
        name: surface_form.to_string(),
        node_type: NodeType::Alias,
        created_at: t0(),
        updated_at: t0(),
        properties: None,
    }
}

/// Seeds the Section 7.4 step 2 write-path shape directly against the
/// graph (the seeding style of recall.rs `seed_person_alias`): one
/// Person node, one Alias node for the surface form, the `known_as`
/// edge person -> alias, plus one fact edge person -> Concept. The
/// fact edge is the NEWER edge, so it comes first in the neighbor
/// fetch (created_at descending, Section 8.2 of the database spec).
async fn seed_person_with_alias_and_fact(
    fixture: &Fixture,
    user_id: &str,
    name: &str,
    surface: &str,
    fact: (&str, &str),
) {
    let person = person_node(user_id, name);
    let alias = alias_node(surface);
    let known_as = MemoryEdge {
        source_id: person.id.clone(),
        target_id: alias.id.clone(),
        relationship_name: "known_as".to_string(),
        valid_at: t0(),
        invalid_at: None,
        edge_text: format!("{surface} is a surface form of {name}."),
        created_at: t0(),
        updated_at: t0(),
        properties: None,
    };
    let concept = concept_node(fact.0);
    let fact = fact_edge(
        &person.id,
        &concept.id,
        fact.1,
        t0() + time::Duration::seconds(1),
    );
    let batch = MemoryBatch {
        batch_id: "seed".to_string(),
        nodes: vec![person, alias, concept],
        edges: vec![known_as, fact],
    };
    fixture
        .memory
        .upsert_batch(CHAT_ID, &batch)
        .await
        .expect("the seed upsert");
}

/// The valid neighbor edge ids of one node, in fetch order (created_at
/// descending, Section 8.2 of the database spec).
async fn neighbor_edge_ids(fixture: &Fixture, user_id: &str) -> Vec<String> {
    fixture
        .memory
        .neighbors(CHAT_ID, &person_id(user_id))
        .await
        .expect("the neighbor fetch")
        .iter()
        .map(|edge| edge.edge_id())
        .collect()
}

/// The scripted doubles of one wake.
struct WakeDoubles {
    relevance: Arc<ScriptedRelevanceGate>,
    gate: Arc<ScriptedGate>,
    reply: Arc<ScriptedReplyGenerator>,
}

/// An `Arc` adapter for the scripted relevance gate. `ShallowRecall`
/// takes its gate by value; the test keeps an `Arc` handle for the
/// `inputs()` / `call_count()` assertions.
struct SharedRelevanceGate(Arc<ScriptedRelevanceGate>);

impl RelevanceGate for SharedRelevanceGate {
    fn select<'a>(
        &'a self,
        input: &'a RelevanceInput,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<usize>, AgentError>> + Send + 'a>> {
        self.0.select(input)
    }
}

/// Spawns the real actor with the REAL shallow recall worker over the
/// shared store and graph, plus the scripted wake doubles.
fn spawn_with_wake(
    fixture: &Fixture,
    config: TriggerConfig,
    started_at: OffsetDateTime,
    doubles: &WakeDoubles,
    digest: Option<Arc<dyn DigestPipeline>>,
    outbound: Option<mpsc::Sender<OutboundAction>>,
) -> GroupActorHandle {
    // Open the group store BEFORE the actor spawns (the WAL pragma race
    // note of tamako/tests/wake_replay.rs).
    fixture
        .store
        .open_group(CHAT_ID)
        .expect("open_group succeeds");
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
        config,
        started_at,
        inbox_capacity: DEFAULT_INBOX_CAPACITY,
        preamble: TEST_PREAMBLE.to_string(),
        digest,
        post_digest_hook: None,
        wake: Some(WakeServices {
            recall: Arc::new(recall),
            gate: Arc::clone(&doubles.gate) as Arc<dyn ParticipationGate>,
            reply: Arc::clone(&doubles.reply) as Arc<dyn ReplyGenerator>,
        }),
        summary_provider: None,
        outbound,
        bot_name: Some("Tamako".to_string()),
    })
}

/// Sends one inbound message event.
async fn send_message(handle: &GroupActorHandle, message: NormalizedMessage) {
    handle
        .send_event(InboundEvent::Message(message))
        .await
        .expect("the actor inbox is open");
}

/// The outbound plumbing of the binary (the wake_replay.rs pattern):
/// one channel, one forwarder task into a mock-adapter sink. The
/// forwarder records an action only after the actor's send path
/// persisted the outbound raw-log row (Rule B1), so a recorded action
/// orders every assertion on the wake completion handler.
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
async fn wait_for_actions(sink: &Arc<MockAdapter>, min: usize) {
    let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let actions = sink.recorded_actions();
        if actions.len() >= min {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {WAIT_TIMEOUT:?} waiting for {min} outbound actions (got {})",
            actions.len()
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Polls until the participation gate recorded `min` inputs. The gate
/// call happens AFTER the recall call inside the wake task, so this
/// wait also proves the recall of that wake completed.
async fn wait_for_gate_inputs(gate: &Arc<ScriptedGate>, min: usize) {
    let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let count = gate.inputs().len();
        if count >= min {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {WAIT_TIMEOUT:?} waiting for {min} gate inputs (got {count})"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Polls until the relevance gate recorded `min` calls.
async fn wait_for_relevance_calls(gate: &Arc<ScriptedRelevanceGate>, min: usize) {
    let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let count = gate.call_count();
        if count >= min {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {WAIT_TIMEOUT:?} waiting for {min} relevance-gate calls (got {count})"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Polls a state-table counter until it reaches `expected` or the
/// timeout elapses (the pattern of tamako/tests/wake_replay.rs).
async fn wait_for_counter(store: &Arc<Store>, key: &str, expected: &str) {
    let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let value = counter(store, key).await;
        if value.as_deref() == Some(expected) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {WAIT_TIMEOUT:?} waiting for counter {key} = {expected} (got {value:?})"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Polls `snapshot()` until the digest boundary reaches `min` or the
/// timeout elapses (the pattern of tamako/tests/context_replay.rs). A
/// snapshot that shows the boundary proves the whole digest completion
/// handler (Rule C3 removal, Section 10.2 step 4 prune, persistence)
/// already ran.
async fn wait_for_boundary(handle: &GroupActorHandle, min: i64) -> i64 {
    let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let session = handle.snapshot().await.expect("the snapshot succeeds");
        if session.last_digest_boundary_msg_id >= min {
            return session.last_digest_boundary_msg_id;
        }
        assert!(
            std::time::Instant::now() < deadline,
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

/// Every `injected_memories` row of the group.
async fn injected_rows(store: &Arc<Store>) -> Vec<InjectedMemoryRow> {
    let store = Arc::clone(store);
    tokio::task::spawn_blocking(move || store.list_injected_memories(CHAT_ID))
        .await
        .expect("the blocking task joins")
        .expect("list_injected_memories succeeds")
}

/// The RecallInjection items of one context snapshot.
fn recall_injections(items: &[ContextItem]) -> Vec<&ContextItem> {
    items
        .iter()
        .filter(|item| item.kind == ContextItemKind::RecallInjection)
        .collect()
}

/// Scenario 1: a seeded graph, one threshold wake, the full injection
/// protocol end to end (specs.md Section 9). Rows: m1=1, m2=2, m3=3;
/// the wake fires at m3, so the Rule C2 injection position is the tail
/// row 3. The gate participates and targets row 3; the reply sends.
#[tokio::test]
async fn injection_flows_end_to_end_over_a_seeded_graph() {
    let fixture = make_fixture().await;
    seed_person_facts(&fixture, "u1", "Alice", &[("tea", "Alice likes tea.", 0)]).await;
    let tea_edge_id = neighbor_edge_ids(&fixture, "u1").await.remove(0);

    let doubles = WakeDoubles {
        relevance: Arc::new(ScriptedRelevanceGate::with_selections(vec![vec![0]])),
        gate: Arc::new(ScriptedGate::with_decisions(vec![GateDecision {
            participate: true,
            target_row_id: Some(3),
            reason: None,
        }])),
        reply: Arc::new(ScriptedReplyGenerator::with_replies(vec!["r1".to_string()])),
    };
    let config = wake_config();
    let pump = outbound_pump();
    let handle = spawn_with_wake(
        &fixture,
        config,
        t0(),
        &doubles,
        None,
        Some(pump.tx.clone()),
    );

    for (index, id) in ["m1", "m2", "m3"].iter().enumerate() {
        send_message(
            &handle,
            message(id, index as i64 + 1, "u1", "Alice", STOPWORD_TEXT),
        )
        .await;
    }
    // The send rides AFTER the injection application in the wake
    // completion handler, so the recorded action is the barrier for
    // both (Section 9: injections are applied first).
    wait_for_actions(&pump.sink, 1).await;
    wait_for_counter(&fixture.store, "injection_wakes_total", "1").await;

    // (a) Rule C2 / Section 9.4: exactly one RecallInjection assistant
    // item, content "<memory>Alice likes tea.</memory>", at the tail row 3.
    let context = handle
        .context_snapshot()
        .await
        .expect("the context snapshot");
    let injections = recall_injections(&context);
    assert_eq!(injections.len(), 1);
    let injection = injections[0];
    assert_eq!(injection.role, ContextRole::Assistant);
    assert_eq!(injection.content, "<memory>Alice likes tea.</memory>");
    assert_eq!(injection.range_tag, Some(RangeTag::single(3)));

    // (b) Section 9.3 / specs.md Section 5.2: one injected_memories row
    // per edge id, with the position, the range tag, and the content.
    let rows = injected_rows(&fixture.store).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].edge_id, tea_edge_id);
    assert_eq!(rows[0].injection_position, 3);
    assert_eq!(rows[0].range_tag, "3-3");
    assert_eq!(rows[0].content, "<memory>Alice likes tea.</memory>");

    // (c) Section 9.6: the injection rides the gate input (the
    // "Injected memories" section of the gate prompt).
    let gate_inputs = doubles.gate.inputs();
    assert_eq!(gate_inputs.len(), 1);
    assert_eq!(
        gate_inputs[0].injections,
        vec!["<memory>Alice likes tea.</memory>".to_string()]
    );
    assert!(!gate_inputs[0].forced);
    // ... and the reply-model snapshot (Section 9 step 4, Rule C2 tail
    // position): the reply model sees the injection too.
    let requests = doubles.reply.requests();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0].messages.iter().any(|message| {
            message.role == ContextRole::Assistant
                && message.content == "<memory>Alice likes tea.</memory>"
        }),
        "the reply-model snapshot carries the injection"
    );

    // (d) Section 12: the injection-rate metric.
    assert_eq!(
        counter(&fixture.store, "injection_wakes_total")
            .await
            .as_deref(),
        Some("1")
    );
    // The relevance gate saw exactly the seeded candidate.
    let relevance_inputs = doubles.relevance.inputs();
    assert_eq!(relevance_inputs.len(), 1);
    assert_eq!(relevance_inputs[0].candidates.len(), 1);
    assert_eq!(relevance_inputs[0].candidates[0].edge_id, tea_edge_id);
    assert_eq!(
        relevance_inputs[0].candidates[0].edge_text,
        "Alice likes tea."
    );

    handle.shutdown().await.expect("the actor reports no error");
    shutdown_pump(pump).await;
}

/// Scenario 2: the Section 9.3 dedup across two wakes in the same
/// chunk. The seeded person has TWO fact edges; the first wake injects
/// the first presented candidate (the NEWER edge comes first in the
/// neighbor fetch, Section 8.2 of the database spec). The second wake
/// resolves the same person: the already-injected edge is dropped
/// BEFORE the relevance gate, so the gate sees only the surviving edge
/// and its exhausted selection queue injects nothing.
#[tokio::test]
async fn an_injected_edge_is_not_reinjected_in_the_same_chunk() {
    let fixture = make_fixture().await;
    // created_at descending: "tea" (newer) is presented first.
    seed_person_facts(
        &fixture,
        "u1",
        "Alice",
        &[("go", "Alice plays go.", 0), ("tea", "Alice likes tea.", 1)],
    )
    .await;
    let edge_ids = neighbor_edge_ids(&fixture, "u1").await;
    let tea_edge_id = edge_ids[0].clone();
    let go_edge_id = edge_ids[1].clone();

    let doubles = WakeDoubles {
        // One selection only: the queue is exhausted at the second
        // call, which then selects nothing.
        relevance: Arc::new(ScriptedRelevanceGate::with_selections(vec![vec![0]])),
        gate: Arc::new(ScriptedGate::with_decisions(vec![
            GateDecision {
                participate: false,
                target_row_id: None,
                reason: None,
            },
            GateDecision {
                participate: false,
                target_row_id: None,
                reason: None,
            },
        ])),
        reply: Arc::new(ScriptedReplyGenerator::failing(
            "a gate-no wake never reaches the reply model",
        )),
    };
    let handle = spawn_with_wake(&fixture, wake_config(), t0(), &doubles, None, None);

    // Wake 1 (rows 1-3): injects the tea edge at position 3.
    for (index, id) in ["m1", "m2", "m3"].iter().enumerate() {
        send_message(
            &handle,
            message(id, index as i64 + 1, "u1", "Alice", STOPWORD_TEXT),
        )
        .await;
    }
    // The barrier BEFORE the second batch: the dedup row must exist
    // when the recall of wake 2 reads it.
    wait_for_counter(&fixture.store, "injection_wakes_total", "1").await;

    // Wake 2 (rows 4-6), same sender: the same candidate resolves.
    for (index, id) in ["m4", "m5", "m6"].iter().enumerate() {
        send_message(
            &handle,
            message(id, index as i64 + 4, "u1", "Alice", STOPWORD_TEXT),
        )
        .await;
    }
    // The second relevance-gate call proves the recall of wake 2 ran
    // (`wakes_total` bumps at wake START, so it cannot order this).
    wait_for_relevance_calls(&doubles.relevance, 2).await;
    handle.snapshot().await.expect("the snapshot succeeds");

    // Section 9.3: the second relevance-gate call does NOT contain the
    // already-injected edge id; only the surviving edge is presented.
    let relevance_inputs = doubles.relevance.inputs();
    assert_eq!(relevance_inputs.len(), 2);
    let first_call = &relevance_inputs[0].candidates;
    assert_eq!(first_call.len(), 2);
    assert_eq!(first_call[0].edge_id, tea_edge_id);
    assert_eq!(first_call[0].edge_text, "Alice likes tea.");
    assert_eq!(first_call[1].edge_id, go_edge_id);
    let second_call = &relevance_inputs[1].candidates;
    assert_eq!(second_call.len(), 1);
    assert_eq!(second_call[0].edge_id, go_edge_id);
    assert!(
        second_call
            .iter()
            .all(|candidate| candidate.edge_id != tea_edge_id),
        "the already-injected edge is never presented again"
    );

    // No second injection: one RecallInjection item, one dedup row,
    // the metric stays 1.
    let context = handle
        .context_snapshot()
        .await
        .expect("the context snapshot");
    assert_eq!(recall_injections(&context).len(), 1);
    let rows = injected_rows(&fixture.store).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].edge_id, tea_edge_id);
    assert_eq!(
        counter(&fixture.store, "injection_wakes_total")
            .await
            .as_deref(),
        Some("1")
    );

    handle.shutdown().await.expect("the actor reports no error");
}

/// Scenario 3: no candidates — no injection and NO relevance-gate call
/// (Sections 9.1/9.2: the fixed cost applies to wakes with candidates
/// only). The graph holds an unrelated person; the wake sender is
/// unknown and the stopword-only texts yield no alias terms.
#[tokio::test]
async fn no_candidates_means_no_injection_and_no_relevance_call() {
    let fixture = make_fixture().await;
    seed_person_facts(&fixture, "u1", "Alice", &[("tea", "Alice likes tea.", 0)]).await;

    let doubles = WakeDoubles {
        relevance: Arc::new(ScriptedRelevanceGate::with_selections(vec![vec![0]])),
        gate: Arc::new(ScriptedGate::with_decisions(vec![GateDecision {
            participate: false,
            target_row_id: None,
            reason: None,
        }])),
        reply: Arc::new(ScriptedReplyGenerator::failing(
            "a gate-no wake never reaches the reply model",
        )),
    };
    let handle = spawn_with_wake(&fixture, wake_config(), t0(), &doubles, None, None);

    for (index, id) in ["m1", "m2", "m3"].iter().enumerate() {
        send_message(
            &handle,
            message(id, index as i64 + 1, "u2", "Bob", STOPWORD_TEXT),
        )
        .await;
    }
    // The gate call proves the recall of this wake completed (recall
    // runs BEFORE the gate inside the wake task; `wakes_total` bumps
    // at wake start and cannot order this).
    wait_for_gate_inputs(&doubles.gate, 1).await;
    handle.snapshot().await.expect("the snapshot succeeds");

    // Section 9.1: zero candidates — the relevance gate is NEVER
    // called.
    assert_eq!(doubles.relevance.call_count(), 0);
    // The participation gate ran (and saw an empty injection list).
    let gate_inputs = doubles.gate.inputs();
    assert_eq!(gate_inputs.len(), 1);
    assert!(gate_inputs[0].injections.is_empty());
    // No context item, no dedup row, no metric.
    let context = handle
        .context_snapshot()
        .await
        .expect("the context snapshot");
    assert!(recall_injections(&context).is_empty());
    assert!(injected_rows(&fixture.store).await.is_empty());
    assert_eq!(counter(&fixture.store, "injection_wakes_total").await, None);

    handle.shutdown().await.expect("the actor reports no error");
}

/// Scenario 4: Rule P1 (the M2 rebuild path) — a restart rebuilds the
/// injected assistant message bit-identically from the raw log and the
/// `injected_memories` rows (the restart pattern of
/// tamako/tests/context_replay.rs).
#[tokio::test]
async fn a_restart_rebuilds_the_injection_bit_identically() {
    let fixture = make_fixture().await;
    seed_person_facts(&fixture, "u1", "Alice", &[("tea", "Alice likes tea.", 0)]).await;

    let doubles = WakeDoubles {
        relevance: Arc::new(ScriptedRelevanceGate::with_selections(vec![vec![0]])),
        gate: Arc::new(ScriptedGate::with_decisions(vec![GateDecision {
            participate: false,
            target_row_id: None,
            reason: None,
        }])),
        reply: Arc::new(ScriptedReplyGenerator::failing(
            "a gate-no wake never reaches the reply model",
        )),
    };
    let handle = spawn_with_wake(&fixture, wake_config(), t0(), &doubles, None, None);
    for (index, id) in ["m1", "m2", "m3"].iter().enumerate() {
        send_message(
            &handle,
            message(id, index as i64 + 1, "u1", "Alice", STOPWORD_TEXT),
        )
        .await;
    }
    wait_for_counter(&fixture.store, "injection_wakes_total", "1").await;
    let before = handle
        .context_snapshot()
        .await
        .expect("the context snapshot");
    assert_eq!(recall_injections(&before).len(), 1);
    handle.shutdown().await.expect("the actor reports no error");

    // The restart: a fresh actor over the same data root. No wake
    // services are needed for the rebuild assertion.
    fixture
        .store
        .open_group(CHAT_ID)
        .expect("open_group succeeds");
    let restarted = spawn_group_actor(GroupActorParams {
        chat_id: CHAT_ID.to_string(),
        store: Arc::clone(&fixture.store),
        memory: Arc::clone(&fixture.memory),
        config: wake_config(),
        started_at: t0(),
        inbox_capacity: DEFAULT_INBOX_CAPACITY,
        preamble: TEST_PREAMBLE.to_string(),
        digest: None,
        post_digest_hook: None,
        wake: None,
        summary_provider: None,
        outbound: None,
        bot_name: None,
    });
    let after = restarted
        .context_snapshot()
        .await
        .expect("the context snapshot");
    assert_eq!(
        after, before,
        "the restart rebuilds the context bit-identically (Rule P1)"
    );
    // Explicitly: kind, role, content, and position of the rebuilt
    // injection.
    let injections = recall_injections(&after);
    assert_eq!(injections.len(), 1);
    assert_eq!(injections[0].kind, ContextItemKind::RecallInjection);
    assert_eq!(injections[0].role, ContextRole::Assistant);
    assert_eq!(injections[0].content, "<memory>Alice likes tea.</memory>");
    assert_eq!(injections[0].range_tag, Some(RangeTag::single(3)));

    restarted
        .shutdown()
        .await
        .expect("the actor reports no error");
}

/// Scenario 4b: the MULTI-EDGE form of the restart bit-identity
/// (decision 65). One wake injects TWO edges of the same person as one
/// PlannedInjection: the live context gets ONE RecallInjection item,
/// while `injected_memories` persists ONE ROW PER EDGE (Section 9.3) —
/// two consecutive rows with the same position and content. The restart
/// rebuild must collapse that run back into the one live item (Rule
/// P1). The absence of this test hid the M5-era violation where the
/// rebuild appended one item per ROW.
#[tokio::test]
async fn a_restart_rebuilds_a_multi_edge_injection_bit_identically() {
    let fixture = make_fixture().await;
    // created_at descending: "tea" (newer) is presented first, "go"
    // second; the scripted gate selects both.
    seed_person_facts(
        &fixture,
        "u1",
        "Alice",
        &[("go", "Alice plays go.", 0), ("tea", "Alice likes tea.", 1)],
    )
    .await;

    let doubles = WakeDoubles {
        relevance: Arc::new(ScriptedRelevanceGate::with_selections(vec![vec![0, 1]])),
        gate: Arc::new(ScriptedGate::with_decisions(vec![GateDecision {
            participate: false,
            target_row_id: None,
            reason: None,
        }])),
        reply: Arc::new(ScriptedReplyGenerator::failing(
            "a gate-no wake never reaches the reply model",
        )),
    };
    let handle = spawn_with_wake(&fixture, wake_config(), t0(), &doubles, None, None);
    for (index, id) in ["m1", "m2", "m3"].iter().enumerate() {
        send_message(
            &handle,
            message(id, index as i64 + 1, "u1", "Alice", STOPWORD_TEXT),
        )
        .await;
    }
    wait_for_counter(&fixture.store, "injection_wakes_total", "1").await;
    let before = handle
        .context_snapshot()
        .await
        .expect("the context snapshot");
    // The live view: ONE injection item carrying both edge texts.
    assert_eq!(recall_injections(&before).len(), 1);
    handle.shutdown().await.expect("the actor reports no error");

    // The persisted form: TWO rows (one per edge), the same position
    // and content, consecutive row ids — the exact shape the decision
    // 65 collapse consumes.
    let rows = injected_rows(&fixture.store).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].injection_position, 3);
    assert_eq!(rows[1].injection_position, 3);
    assert_eq!(rows[0].content, rows[1].content);
    assert_eq!(
        rows[0].content,
        "<memory>Alice likes tea. Alice plays go.</memory>"
    );
    assert_ne!(rows[0].edge_id, rows[1].edge_id);

    // The restart: a fresh actor over the same data root.
    fixture
        .store
        .open_group(CHAT_ID)
        .expect("open_group succeeds");
    let restarted = spawn_group_actor(GroupActorParams {
        chat_id: CHAT_ID.to_string(),
        store: Arc::clone(&fixture.store),
        memory: Arc::clone(&fixture.memory),
        config: wake_config(),
        started_at: t0(),
        inbox_capacity: DEFAULT_INBOX_CAPACITY,
        preamble: TEST_PREAMBLE.to_string(),
        digest: None,
        post_digest_hook: None,
        wake: None,
        summary_provider: None,
        outbound: None,
        bot_name: None,
    });
    let after = restarted
        .context_snapshot()
        .await
        .expect("the context snapshot");
    assert_eq!(
        after, before,
        "the restart rebuilds the multi-edge injection bit-identically (Rule P1)"
    );
    // Explicitly: the two persisted rows collapsed back into ONE item.
    let injections = recall_injections(&after);
    assert_eq!(injections.len(), 1);
    assert_eq!(injections[0].kind, ContextItemKind::RecallInjection);
    assert_eq!(
        injections[0].content,
        "<memory>Alice likes tea. Alice plays go.</memory>"
    );
    assert_eq!(injections[0].range_tag, Some(RangeTag::single(3)));

    restarted
        .shutdown()
        .await
        .expect("the actor reports no error");
}

/// The trivial scripted graph of the digest tests (the pattern of
/// tamako/tests/context_replay.rs). The extracted person never binds
/// (no "Alice" message exists here), so the digest writes no neighbor
/// of the seeded wake sender.
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

/// Scenario 5: the digest prunes the injection rows at the boundary
/// (Section 10.2 step 4 + Rule C3, the one-chunk lag). Forced wakes
/// (mentions) drive the injections, so NO threshold wake can race a
/// digest completion; the raw-log layout stays exact:
///
/// - row 1: m1 (a mention) — forced wake 1 injects the tea edge at
///   position 1; row 2 is the outbound row of the reply (Rule B1);
/// - rows 3-4: m2, m3 complete the first digest tail; digest 1 covers
///   rows 1..=4 (b1 = 4, prev = None — nothing is pruned);
/// - rows 5-7: m4, m5, m6; digest 2 covers rows 5..=7 (b2 = 7,
///   prev = 4) and prunes at the PREVIOUS boundary: the injection at
///   position 1 (<= 4) is removed, context item included (Rule C3);
/// - row 8: m7 (a mention) — forced wake 2 resolves the same edge
///   again (the dedup row is gone) and injects it at position 8,
///   ABOVE the new cutoff; row 9 is the second reply row;
/// - rows 10-11: m8, m9 complete the third digest tail (prev = 7);
///   the fresh injection at position 8 survives.
///
/// Every digest boundary is exact: each digest task reads the tail at
/// execution time and the test paces the feed on the sink actions and
/// the boundary waits, so a fixed number of rows exists at every fire
/// point.
#[tokio::test]
async fn the_digest_prunes_injection_rows_at_the_boundary() {
    let fixture = make_fixture().await;
    seed_person_facts(&fixture, "u9", "Zed", &[("tea", "Zed likes tea.", 0)]).await;
    let tea_edge_id = neighbor_edge_ids(&fixture, "u9").await.remove(0);

    let doubles = WakeDoubles {
        relevance: Arc::new(ScriptedRelevanceGate::with_selections(vec![
            vec![0],
            vec![0],
        ])),
        // Forced wakes bypass the participation gate (Section 8.1).
        gate: Arc::new(ScriptedGate::failing(
            "the gate must not be called for a forced wake",
        )),
        reply: Arc::new(ScriptedReplyGenerator::with_replies(vec![
            "r1".to_string(),
            "r2".to_string(),
        ])),
    };
    let extractor = Arc::new(ScriptedExtractor::with_graphs(vec![
        trivial_graph(),
        trivial_graph(),
        trivial_graph(),
        trivial_graph(),
    ]));
    let digest: Arc<dyn DigestPipeline> = Arc::new(AgentDigestPipeline::new(
        Arc::clone(&fixture.store),
        Arc::clone(&fixture.memory),
        extractor,
        PipelineConfig::default(),
    ));
    let config = TriggerConfig {
        digest_max_messages: 3,
        // Mentions alone drive the wakes: the count threshold is far
        // above every batch of this scenario.
        wake_msg_count: 100,
        ..wake_config()
    };
    let pump = outbound_pump();
    let handle = spawn_with_wake(
        &fixture,
        config,
        t0(),
        &doubles,
        Some(digest),
        Some(pump.tx.clone()),
    );

    /// A mention of the bot by the seeded sender (a forced wake,
    /// Section 8.1).
    fn mention(id: &str, seconds: i64) -> NormalizedMessage {
        let mut message = message(id, seconds, "u9", "Zed", STOPWORD_TEXT);
        message.mentions_bot = true;
        message
    }

    // Row 1: the mention; row 2: the reply. The recorded action is the
    // barrier: the injection was applied BEFORE the send (Section 9).
    send_message(&handle, mention("m1", 1)).await;
    wait_for_actions(&pump.sink, 1).await;
    assert_eq!(
        counter(&fixture.store, "injection_wakes_total")
            .await
            .as_deref(),
        Some("1")
    );

    // Rows 3-4: digest 1 covers rows 1..=4.
    for (index, id) in ["m2", "m3"].iter().enumerate() {
        send_message(
            &handle,
            message(id, index as i64 + 2, "u9", "Zed", STOPWORD_TEXT),
        )
        .await;
    }
    let b1 = wait_for_boundary(&handle, 4).await;
    assert_eq!(b1, 4, "the first batch covers rows 1..=4");
    let session = handle.snapshot().await.expect("the snapshot succeeds");
    assert_eq!(session.prev_digest_boundary_msg_id, None);
    let rows = injected_rows(&fixture.store).await;
    assert_eq!(
        rows.len(),
        1,
        "the first digest prunes nothing (prev is None)"
    );
    assert_eq!(rows[0].injection_position, 1);

    // Rows 5-7: digest 2 sets prev = 4 and prunes the injection.
    for (index, id) in ["m4", "m5", "m6"].iter().enumerate() {
        send_message(
            &handle,
            message(id, index as i64 + 4, "u9", "Zed", STOPWORD_TEXT),
        )
        .await;
    }
    let b2 = wait_for_boundary(&handle, 7).await;
    assert_eq!(b2, 7, "the second batch covers rows 5..=7");
    let session = handle.snapshot().await.expect("the snapshot succeeds");
    assert_eq!(session.prev_digest_boundary_msg_id, Some(4));

    // Section 10.2 step 4 / Rule C3: the injection row at position 1
    // (<= prev = 4) is pruned; the RecallInjection item is gone from
    // the live context.
    assert!(
        injected_rows(&fixture.store).await.is_empty(),
        "the second digest pruned the injection row"
    );
    let context = handle
        .context_snapshot()
        .await
        .expect("the context snapshot");
    assert!(
        recall_injections(&context).is_empty(),
        "the injection item lagged out at the one-chunk boundary"
    );

    // Row 8: the second mention re-injects the same edge ABOVE the new
    // cutoff (the dedup row was pruned); row 9 is the reply.
    send_message(&handle, mention("m7", 8)).await;
    wait_for_actions(&pump.sink, 2).await;
    assert_eq!(
        counter(&fixture.store, "injection_wakes_total")
            .await
            .as_deref(),
        Some("2")
    );

    // Rows 10-11: digest 3 sets prev = 7; the fresh injection at
    // position 8 survives.
    for (index, id) in ["m8", "m9"].iter().enumerate() {
        send_message(
            &handle,
            message(id, index as i64 + 9, "u9", "Zed", STOPWORD_TEXT),
        )
        .await;
    }
    let b3 = wait_for_boundary(&handle, b2 + 1).await;
    let session = handle.snapshot().await.expect("the snapshot succeeds");
    assert_eq!(session.prev_digest_boundary_msg_id, Some(7));
    assert!(b3 >= 10, "the third batch covers at least rows 8..=10");

    let rows = injected_rows(&fixture.store).await;
    assert_eq!(
        rows.len(),
        1,
        "the fresh injection survives above the cutoff"
    );
    assert_eq!(rows[0].edge_id, tea_edge_id);
    assert_eq!(rows[0].injection_position, 8);
    assert_eq!(rows[0].range_tag, "8-8");
    assert_eq!(rows[0].content, "<memory>Zed likes tea.</memory>");
    let context = handle
        .context_snapshot()
        .await
        .expect("the context snapshot");
    let injections = recall_injections(&context);
    assert_eq!(injections.len(), 1);
    assert_eq!(injections[0].content, "<memory>Zed likes tea.</memory>");
    assert_eq!(injections[0].range_tag, Some(RangeTag::single(8)));
    // The relevance gate saw the re-resolved candidate (call 2).
    assert_eq!(doubles.relevance.call_count(), 2);
    assert_eq!(
        doubles.relevance.inputs()[1].candidates[0].edge_id,
        tea_edge_id
    );

    handle.shutdown().await.expect("the actor reports no error");
    shutdown_pump(pump).await;
}

/// Scenario 6: a Chinese wake message whose CJK n-gram exactly matches
/// a stored Alias flows end to end (decision 58 on top of the
/// tokenizer of decision 44; specs.md Sections 9.1-9.4). The wake text
/// "明哥今天来吗" is one maximal CJK run; its bigram "明哥" matches
/// the stored Alias. The pre-n-gram tokenizer produced only the
/// whole-run token "明哥今天来吗", which matches nothing — this test
/// is the end-to-end proof that the n-gram path finds the alias.
///
/// The sender "u9" is UNSEEDED: the sender Person entry resolves to an
/// unknown node with zero neighbors, so the ONLY path to the candidate
/// is the alias n-gram (Section 8.1 steps 1 and 2 of the database
/// spec). The alias has exactly one target, so the entry is the Person
/// node 小明 and BOTH of its edges are candidates (one hop, Section
/// 8.2): the fact edge first (the newer edge), then the known_as edge.
///
/// Rows: m1=1, m2=2, m3=3; the wake fires at m3, so the Rule C2
/// injection position is the tail row 3. The gate participates and
/// targets row 3; the reply sends.
#[tokio::test]
async fn a_chinese_ngram_matching_an_alias_flows_end_to_end() {
    let fixture = make_fixture().await;
    seed_person_with_alias_and_fact(&fixture, "u7", "小明", "明哥", ("辣味", "小明喜欢吃辣。"))
        .await;
    // The natural key of the fact edge, through the same read path as
    // the other scenarios (Section 8.2 of the database spec).
    let fact_edge_id = fixture
        .memory
        .neighbors(CHAT_ID, &person_id("u7"))
        .await
        .expect("the neighbor fetch")
        .into_iter()
        .find(|edge| edge.relationship_name == "related_to")
        .expect("the fact edge")
        .edge_id();

    let doubles = WakeDoubles {
        // Index 0 is the fact edge: it is the newer edge of the target
        // node, so it leads the presented set (verified by the
        // candidate assertions below).
        relevance: Arc::new(ScriptedRelevanceGate::with_selections(vec![vec![0]])),
        gate: Arc::new(ScriptedGate::with_decisions(vec![GateDecision {
            participate: true,
            target_row_id: Some(3),
            reason: None,
        }])),
        reply: Arc::new(ScriptedReplyGenerator::with_replies(vec!["r1".to_string()])),
    };
    let config = wake_config();
    let pump = outbound_pump();
    let handle = spawn_with_wake(
        &fixture,
        config,
        t0(),
        &doubles,
        None,
        Some(pump.tx.clone()),
    );

    // Three Chinese texts from the unseeded sender. Only m2 carries the
    // alias, as a substring of a longer CJK run: the n-gram set of the
    // run "明哥今天来吗" contains the bigram "明哥". The texts are
    // short enough that the n-gram pool stays below MAX_NGRAM_TERMS, so
    // no term is truncated.
    let texts = ["天气不错", "明哥今天来吗", "一起吃饭吧"];
    for (index, id) in ["m1", "m2", "m3"].iter().enumerate() {
        send_message(
            &handle,
            message(id, index as i64 + 1, "u9", "阿杰", texts[index]),
        )
        .await;
    }
    // The send rides AFTER the injection application in the wake
    // completion handler, so the recorded action is the barrier for
    // both (Section 9: injections are applied first).
    wait_for_actions(&pump.sink, 1).await;
    wait_for_counter(&fixture.store, "injection_wakes_total", "1").await;

    // (a) Sections 9.1/9.2: the relevance gate was called EXACTLY once.
    // The presented set is exactly the two edges of the alias target
    // (verified order): the fact edge first, then the known_as edge.
    // The candidate source is the seeded person "u7", reached ONLY
    // through the alias n-gram — the sender "u9" is unseeded.
    let relevance_inputs = doubles.relevance.inputs();
    assert_eq!(relevance_inputs.len(), 1);
    assert!(
        relevance_inputs[0]
            .new_messages
            .iter()
            .all(|message| message.sender_id == "u9"),
        "every wake message comes from the unseeded sender"
    );
    let candidates = &relevance_inputs[0].candidates;
    assert_eq!(candidates.len(), 2);
    assert_eq!(candidates[0].edge_id, fact_edge_id);
    assert_eq!(candidates[0].edge_text, "小明喜欢吃辣。");
    assert_eq!(candidates[0].source_id, person_id("u7"));
    assert_eq!(candidates[0].relationship_name, "related_to");
    assert_eq!(candidates[1].edge_text, "明哥 is a surface form of 小明.");
    assert_eq!(candidates[1].relationship_name, "known_as");

    // (b) Rule C2 / Section 9.4: exactly one RecallInjection assistant
    // item, content "<memory>小明喜欢吃辣。</memory>", at the tail row 3.
    let context = handle
        .context_snapshot()
        .await
        .expect("the context snapshot");
    let injections = recall_injections(&context);
    assert_eq!(injections.len(), 1);
    let injection = injections[0];
    assert_eq!(injection.role, ContextRole::Assistant);
    assert_eq!(injection.content, "<memory>小明喜欢吃辣。</memory>");
    assert_eq!(injection.range_tag, Some(RangeTag::single(3)));

    // (c) Section 9.3 / specs.md Section 5.2: exactly one
    // injected_memories row, keyed on the natural key of the fact edge.
    let rows = injected_rows(&fixture.store).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].edge_id, fact_edge_id);
    assert_eq!(rows[0].injection_position, 3);
    assert_eq!(rows[0].range_tag, "3-3");
    assert_eq!(rows[0].content, "<memory>小明喜欢吃辣。</memory>");

    // (d) Section 9.6: the injection rides the gate input and the
    // reply-model snapshot.
    let gate_inputs = doubles.gate.inputs();
    assert_eq!(gate_inputs.len(), 1);
    assert_eq!(
        gate_inputs[0].injections,
        vec!["<memory>小明喜欢吃辣。</memory>".to_string()]
    );
    let requests = doubles.reply.requests();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0].messages.iter().any(|message| {
            message.role == ContextRole::Assistant
                && message.content == "<memory>小明喜欢吃辣。</memory>"
        }),
        "the reply-model snapshot carries the injection"
    );

    // (e) Section 12: the injection-rate metric.
    assert_eq!(
        counter(&fixture.store, "injection_wakes_total")
            .await
            .as_deref(),
        Some("1")
    );

    handle.shutdown().await.expect("the actor reports no error");
    shutdown_pump(pump).await;
}
