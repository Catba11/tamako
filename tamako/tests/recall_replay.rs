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
//! Decision 76 (deep recall): the DEEP scenarios at the bottom of this
//! file run the same real actor with `ShallowRecall::with_deep_recall`
//! wired the way main.rs wires it — a DEDICATED one-group Store for the
//! recall (the KNN / edge_texts single-open-group contract) and a
//! scripted `EmbeddingProvider`, so the deep behavior stays
//! deterministic and network-free. The scenarios ABOVE stay shallow
//! (no DeepRecallConfig): their assertions are the byte-identical
//! pre-76 behavior and are semantically unchanged.
//!
//! Timer note: same policy as wake_replay.rs — `wake_interval` is
//! near-infinite and the wake floor is zero, so only the message count
//! drives wakes and the built-in 1-second ticker stays inert.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tamako_adapter_mock::MockAdapter;
use tamako_agent::endpoint::EMBEDDING_DIMS;
use tamako_agent::recall::DeepRecallConfig;
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
use tamako_core::context::{
    render_human_content, ContextItem, ContextItemKind, ContextRole, RangeTag, ReplyRender,
};
use tamako_core::digest::DigestPipeline;
use tamako_core::embedding::{
    reconcile_group, EmbeddingError, EmbeddingProvider, GroupEmbeddingTarget,
};
use tamako_core::event::{InboundEvent, NormalizedMessage, OutboundAction};
use tamako_core::wake::{GateDecision, ParticipationGate, ReplyGenerator, WakeServices};
use tamako_memory::identifiers::{alias_id, concept_id, person_id};
use tamako_memory::{
    EdgeId, LbugBackend, MemoryBackend, MemoryBatch, MemoryEdge, MemoryNode, NodeType,
};
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

impl Fixture {
    /// The data root of the fixture (the `GroupEmbeddingTarget::open`
    /// argument of the reconciliation scenario).
    fn data_root(&self) -> &std::path::Path {
        self._dir.path()
    }

    /// A DEDICATED one-group Store over the same data root — the
    /// deep-recall store shape of main.rs (`GroupEmbeddingTarget::open`
    /// — the same per-group store.db file, so every write of the
    /// fixture store and of the actor's shared store is visible here).
    /// The decision-76 store sources (KNN over `node_embeddings`, LIKE
    /// over `edge_texts`) are chat_id-less helpers that reject a Store
    /// with 2+ open groups (`StoreError::AmbiguousGroup`).
    fn dedicated_store(&self) -> Arc<Store> {
        let store = Arc::new(Store::new(self._dir.path().to_path_buf()));
        store.open_group(CHAT_ID).expect("open_group succeeds");
        store
    }
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

    // Decision 72: forward the shared context view — the trait's
    // DEFAULT `select_with_context` drops it, which would record a
    // false `None` (the kill-switch shape) on every call of this suite.
    fn select_with_context<'a>(
        &'a self,
        input: &'a RelevanceInput,
        context_view: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<usize>, AgentError>> + Send + 'a>> {
        self.0.select_with_context(input, context_view)
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

    // Decision 72: within a single wake the participation gate and the
    // recall relevance gate received the SAME context-view bytes (the
    // actor renders the view once per wake and passes it to both).
    // This first wake's marker is 0 over an empty context, so the
    // shared view is the empty string.
    assert_eq!(doubles.gate.context_views(), vec![Some(String::new())]);
    assert_eq!(
        doubles.relevance.context_views(),
        doubles.gate.context_views(),
        "one wake, one prefix: both gates received the same view bytes"
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

    // Decision 72, byte-exact: the second wake's context view is the
    // first wake's view EXTENDED by wake 1's new-messages item bytes
    // AND wake 1's injection (Rule C2: the RecallInjection item rides
    // at the tail row it follows — position 3 is AT the bound, so the
    // injection joins the view). The `injection_wakes_total` barrier
    // above ordered the completion-handler append before wake 2
    // rendered its view.
    let rendered = |row: i64, seconds: i64| {
        render_human_content(
            row,
            "Alice",
            None,
            t0() + time::Duration::seconds(seconds),
            false,
            false,
            ReplyRender::None,
            STOPWORD_TEXT,
        )
    };
    let expected_second_view = [
        rendered(1, 1),
        rendered(2, 2),
        rendered(3, 3),
        "<memory>Alice likes tea.</memory>".to_string(),
    ]
    .join("\n");
    let relevance_views = doubles.relevance.context_views();
    assert_eq!(relevance_views.len(), 2);
    assert_eq!(relevance_views[0].as_deref(), Some(""));
    assert_eq!(
        relevance_views[1].as_deref(),
        Some(expected_second_view.as_str()),
        "view(2) == view(1) extended by wake 1's new items and its injection"
    );
    // Both gates of the wake received the SAME view bytes.
    assert_eq!(doubles.gate.context_views(), relevance_views);

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

// ---- Decision 76: the deep-recall scenarios. -------------------------
//
// The recall is wired the way main.rs wires a `deep_recall = true`
// group with a provider: a DEDICATED one-group store (the
// single-open-group contract of the KNN / edge_texts reads) plus a
// scripted embedding provider, so the deep behavior stays deterministic
// and network-free. The scenarios ABOVE keep the pre-76 shallow wiring
// (no DeepRecallConfig) and are semantically unchanged.

/// A scripted core embedding provider (the decision-76 vector-entry
/// seam): pops one batch result per `embed_texts` call (FIFO) and
/// records every call's texts — the same double shape as the recall.rs
/// ScriptedEmbedder, kept local so the suite stays env-hermetic. An
/// exhausted queue fails the call; the recall degrades the vector
/// entry to empty on ANY provider error (decision 76), so a surprise
/// call surfaces through the `call_count` / `calls` assertions.
struct ScriptedEmbedder {
    batches: Mutex<VecDeque<Vec<Vec<f32>>>>,
    calls: Mutex<Vec<Vec<String>>>,
}

impl ScriptedEmbedder {
    /// Every `embed_texts` call answers with the next batch (in order).
    fn with_batches(batches: Vec<Vec<Vec<f32>>>) -> Self {
        ScriptedEmbedder {
            batches: Mutex::new(batches.into()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn call_count(&self) -> usize {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    fn calls(&self) -> Vec<Vec<String>> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl EmbeddingProvider for ScriptedEmbedder {
    fn embed<'a>(
        &'a self,
        text: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<f32>, EmbeddingError>> + Send + 'a>> {
        Box::pin(async move {
            let texts = [text.to_string()];
            let mut batches = self.embed_texts(&texts).await?;
            Ok(batches.remove(0))
        })
    }

    fn embed_texts<'a>(
        &'a self,
        texts: &'a [String],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Vec<f32>>, EmbeddingError>> + Send + 'a>> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(texts.to_vec());
        let result = self
            .batches
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
            .ok_or_else(|| {
                EmbeddingError::Provider("scripted embedder: exhausted queue".to_string())
            });
        Box::pin(async move { result })
    }
}

/// The unit basis vector of one dimension (EMBEDDING_DIMS wide — the
/// pinned sidecar dimension of decision 66).
fn unit_vector(dim: usize) -> Vec<f32> {
    let mut vector = vec![0.0; EMBEDDING_DIMS];
    vector[dim] = 1.0;
    vector
}

/// A fact edge stamped at the wall clock (plus `offset_seconds`): the
/// decision-76 two-hop expansion applies the 90-day recall window
/// against the real `now_utc()`, so the deep scenarios cannot use the
/// fixed t0() of the shallow scenarios above.
fn recent_edge(
    source_id: &str,
    target_id: &str,
    relationship: &str,
    text: &str,
    offset_seconds: i64,
) -> MemoryEdge {
    let at = OffsetDateTime::now_utc() + time::Duration::seconds(offset_seconds);
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

/// Seeds nodes and edges directly against the graph (the seeding style
/// of `seed_person_facts`).
async fn seed_graph(fixture: &Fixture, nodes: Vec<MemoryNode>, edges: Vec<MemoryEdge>) {
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

/// The opaque `EdgeId` JSON of one seeded edge — the key shape the
/// `edge_texts` sidecar stores (the store never parses it).
fn edge_json_id(edge: &MemoryEdge) -> String {
    EdgeId {
        source_id: edge.source_id.clone(),
        relationship_name: edge.relationship_name.clone(),
        target_id: edge.target_id.clone(),
        valid_at: edge.valid_at,
    }
    .encode()
}

/// Writes one `edge_texts` row on the fixture store (the post-commit
/// digest harvest shape of decision 76c). Runs BEFORE the actor spawns.
async fn seed_edge_text(fixture: &Fixture, edge_id: &str, edge_text: &str) {
    let store = Arc::clone(&fixture.store);
    let edge_id = edge_id.to_string();
    let edge_text = edge_text.to_string();
    tokio::task::spawn_blocking(move || {
        store.open_group(CHAT_ID)?;
        store.upsert_edge_text(&edge_id, &edge_text)
    })
    .await
    .expect("the blocking task joins")
    .expect("upsert_edge_text succeeds");
}

/// Spawns the real actor with the recall wired the way main.rs wires a
/// `deep_recall = true` group with a provider (decision 76): a
/// DEDICATED one-group store for the recall plus the scripted
/// embedding provider and the group's thresholds. The actor itself
/// keeps the fixture's shared store.
fn spawn_with_deep_wake(
    fixture: &Fixture,
    config: TriggerConfig,
    started_at: OffsetDateTime,
    doubles: &WakeDoubles,
    embedder: &Arc<ScriptedEmbedder>,
) -> GroupActorHandle {
    // Open the group store BEFORE the actor spawns (the WAL pragma race
    // note of tamako/tests/wake_replay.rs).
    fixture
        .store
        .open_group(CHAT_ID)
        .expect("open_group succeeds");
    let recall = ShallowRecall::new(
        fixture.dedicated_store(),
        Arc::clone(&fixture.memory),
        SharedRelevanceGate(Arc::clone(&doubles.relevance)),
        config.recall_injection_cap,
    )
    .with_deep_recall(DeepRecallConfig {
        provider: Arc::clone(embedder) as Arc<dyn EmbeddingProvider>,
        vector_candidate_threshold: config.vector_candidate_threshold,
        candidate_cap: config.recall_candidate_cap,
    });
    spawn_group_actor(GroupActorParams {
        chat_id: CHAT_ID.to_string(),
        store: Arc::clone(&fixture.store),
        memory: Arc::clone(&fixture.memory),
        config,
        started_at,
        inbox_capacity: DEFAULT_INBOX_CAPACITY,
        preamble: TEST_PREAMBLE.to_string(),
        digest: None,
        post_digest_hook: None,
        wake: Some(WakeServices {
            recall: Arc::new(recall),
            gate: Arc::clone(&doubles.gate) as Arc<dyn ParticipationGate>,
            reply: Arc::clone(&doubles.reply) as Arc<dyn ReplyGenerator>,
        }),
        summary_provider: None,
        outbound: None,
        bot_name: Some("Tamako".to_string()),
    })
}

/// The scripted doubles of a deep scenario whose wake injects nothing:
/// the relevance gate records its input and selects empty; the
/// participation gate declines; the reply model is never reached.
fn silent_doubles() -> WakeDoubles {
    WakeDoubles {
        relevance: Arc::new(ScriptedRelevanceGate::with_selections(vec![vec![]])),
        gate: Arc::new(ScriptedGate::with_decisions(vec![GateDecision {
            participate: false,
            target_row_id: None,
            reason: None,
        }])),
        reply: Arc::new(ScriptedReplyGenerator::failing(
            "a gate-no wake never reaches the reply model",
        )),
    }
}

/// The edge texts of the candidates the relevance gate recorded on its
/// first (and only) call.
fn presented_texts(doubles: &WakeDoubles) -> Vec<String> {
    let inputs = doubles.relevance.inputs();
    assert_eq!(inputs.len(), 1, "exactly one relevance-gate call");
    inputs[0]
        .candidates
        .iter()
        .map(|candidate| candidate.edge_text.clone())
        .collect()
}

/// Deep scenario 1 (decision 76 (a)): a two-hop fact surfaces. The
/// fact lives at Alice -> espresso -> cake; the wake's only entry is
/// the sender's Person (stopword texts, so NO candidate terms and the
/// embed provider is never called — decision 76 (f)). The hop-2 edge
/// espresso -> cake appears among the candidates presented to the
/// relevance gate, asserted through the gate's recorded input.
#[tokio::test]
async fn deep_recall_surfaces_a_two_hop_fact_end_to_end() {
    let fixture = make_fixture().await;
    let alice = person_node("u1", "Alice");
    let espresso = concept_node("espresso");
    let cake = concept_node("cake");
    let hop_one = recent_edge(
        &alice.id,
        &espresso.id,
        "related_to",
        "Alice likes espresso.",
        0,
    );
    let hop_two = recent_edge(
        &espresso.id,
        &cake.id,
        "related_to",
        "Espresso pairs with cake.",
        0,
    );
    seed_graph(
        &fixture,
        vec![alice, espresso, cake],
        vec![hop_one, hop_two],
    )
    .await;

    let doubles = silent_doubles();
    let embedder = Arc::new(ScriptedEmbedder::with_batches(Vec::new()));
    let handle = spawn_with_deep_wake(&fixture, wake_config(), t0(), &doubles, &embedder);

    for (index, id) in ["m1", "m2", "m3"].iter().enumerate() {
        send_message(
            &handle,
            message(id, index as i64 + 1, "u1", "Alice", STOPWORD_TEXT),
        )
        .await;
    }
    wait_for_relevance_calls(&doubles.relevance, 1).await;
    handle.snapshot().await.expect("the snapshot succeeds");

    // Decision 76 (f): stopword-only texts yield no candidate terms,
    // so the vector entry never calls the provider.
    assert_eq!(embedder.call_count(), 0);
    // The hop-1 edge exactly once (the shallow source won the
    // first-wins dedup), then the hop-2 edge of the expansion.
    assert_eq!(
        presented_texts(&doubles),
        vec![
            "Alice likes espresso.".to_string(),
            "Espresso pairs with cake.".to_string(),
        ]
    );
    // The empty scripted selection injects nothing (Section 9.2).
    assert!(injected_rows(&fixture.store).await.is_empty());

    handle.shutdown().await.expect("the actor reports no error");
}

/// Deep scenario 2 (decision 76 (c)): an FTS-only hit. The term
/// appears ONLY in an edge DESCRIPTION — no entry node name carries it
/// (the detached edge is unreachable from every shallow entry) — and
/// the term is the two-character CJK word 咖啡 (the FTS5-trigram hole
/// of decision 76 (c): the sidecar is a plain parameterized LIKE).
#[tokio::test]
async fn deep_recall_fts_surfaces_an_edge_description_only_cjk_term() {
    let fixture = make_fixture().await;
    let debate = concept_node("price debate");
    let market = concept_node("market");
    let edge = recent_edge(
        &debate.id,
        &market.id,
        "related_to",
        "The group debated 咖啡 prices.",
        0,
    );
    seed_graph(&fixture, vec![debate, market], vec![edge.clone()]).await;
    seed_edge_text(&fixture, &edge_json_id(&edge), &edge.edge_text).await;

    let doubles = silent_doubles();
    // One candidate term (咖啡): ONE batched embeddings call, answered
    // with one query vector; the empty node_embeddings sidecar accepts
    // nothing, so only the FTS source contributes.
    let embedder = Arc::new(ScriptedEmbedder::with_batches(vec![vec![unit_vector(0)]]));
    let handle = spawn_with_deep_wake(&fixture, wake_config(), t0(), &doubles, &embedder);

    // The sender is UNSEEDED and m2's text is exactly the 2-char term:
    // no person entry, no alias entry — the sidecar hit is the ONLY
    // path to the candidate.
    let texts = ["ok ok thanks", "咖啡", "ok ok thanks"];
    for (index, id) in ["m1", "m2", "m3"].iter().enumerate() {
        send_message(
            &handle,
            message(id, index as i64 + 1, "u9", "阿杰", texts[index]),
        )
        .await;
    }
    wait_for_relevance_calls(&doubles.relevance, 1).await;
    handle.snapshot().await.expect("the snapshot succeeds");

    assert_eq!(embedder.call_count(), 1);
    assert_eq!(embedder.calls(), vec![vec!["咖啡".to_string()]]);
    assert_eq!(
        presented_texts(&doubles),
        vec!["The group debated 咖啡 prices.".to_string()]
    );
    assert!(injected_rows(&fixture.store).await.is_empty());

    handle.shutdown().await.expect("the actor reports no error");
}

/// Deep scenario 3 (decision 76 (b)): the also_known_as bridge. The
/// sender's Person node links to her English name twin through
/// also_known_as — the cross-language bridge of decision 74 — and the
/// TWIN carries the fact. The expansion TRAVERSES also_known_as (the
/// whitelist drops only contains and known_as), so the twin's fact
/// surfaces at hop 2.
#[tokio::test]
async fn deep_recall_bridges_a_cross_language_fact_through_also_known_as() {
    let fixture = make_fixture().await;
    let ming = person_node("u7", "小明");
    let twin = person_node("u7en", "Xiao Ming");
    let ceremony = concept_node("tea ceremony");
    let bridge = recent_edge(
        &ming.id,
        &twin.id,
        "also_known_as",
        "小明 is also known as Xiao Ming.",
        1,
    );
    let fact = recent_edge(
        &twin.id,
        &ceremony.id,
        "related_to",
        "Xiao Ming hosts the tea ceremony.",
        0,
    );
    seed_graph(&fixture, vec![ming, twin, ceremony], vec![bridge, fact]).await;

    let doubles = silent_doubles();
    let embedder = Arc::new(ScriptedEmbedder::with_batches(Vec::new()));
    let handle = spawn_with_deep_wake(&fixture, wake_config(), t0(), &doubles, &embedder);

    for (index, id) in ["m1", "m2", "m3"].iter().enumerate() {
        send_message(
            &handle,
            message(id, index as i64 + 1, "u7", "小明", STOPWORD_TEXT),
        )
        .await;
    }
    wait_for_relevance_calls(&doubles.relevance, 1).await;
    handle.snapshot().await.expect("the snapshot succeeds");

    assert_eq!(embedder.call_count(), 0);
    // The bridge edge itself (the shallow neighbor, one candidate after
    // the first-wins dedup against the expansion's hop 1), then the
    // twin's fact at hop 2.
    assert_eq!(
        presented_texts(&doubles),
        vec![
            "小明 is also known as Xiao Ming.".to_string(),
            "Xiao Ming hosts the tea ceremony.".to_string(),
        ]
    );

    handle.shutdown().await.expect("the actor reports no error");
}

/// Deep scenario 4 (decision 76 (b)): contains and known_as are never
/// TRAVERSED. Decoy facts sit BEHIND a contains edge and a known_as
/// edge of the entry node; the expansion's whitelist drops both
/// relationships, so the decoys never reach the gate. The known_as
/// edge of the ENTRY itself still appears as a shallow candidate (the
/// pre-76 behavior scenario 6 pins above) — it is the traversal
/// THROUGH it that adds nothing.
#[tokio::test]
async fn deep_recall_never_traverses_contains_or_known_as() {
    let fixture = make_fixture().await;
    let alice = person_node("u1", "Alice");
    let tea = concept_node("tea");
    let alias = alias_node("Al");
    let sealed = concept_node("sealed");
    let hidden_one = concept_node("hidden one");
    let hidden_two = concept_node("hidden two");
    let fact = recent_edge(&alice.id, &tea.id, "related_to", "Alice likes tea.", 3);
    let known_as = recent_edge(
        &alice.id,
        &alias.id,
        "known_as",
        "Al is a surface form of Alice.",
        2,
    );
    let contains = recent_edge(
        &alice.id,
        &sealed.id,
        "contains",
        "The contains decoy edge.",
        1,
    );
    let behind_known_as = recent_edge(
        &alias.id,
        &hidden_one.id,
        "related_to",
        "The known_as decoy fact.",
        0,
    );
    let behind_contains = recent_edge(
        &sealed.id,
        &hidden_two.id,
        "related_to",
        "The contains decoy fact.",
        0,
    );
    seed_graph(
        &fixture,
        vec![alice, tea, alias, sealed, hidden_one, hidden_two],
        vec![fact, known_as, contains, behind_known_as, behind_contains],
    )
    .await;

    let doubles = silent_doubles();
    let embedder = Arc::new(ScriptedEmbedder::with_batches(Vec::new()));
    let handle = spawn_with_deep_wake(&fixture, wake_config(), t0(), &doubles, &embedder);

    for (index, id) in ["m1", "m2", "m3"].iter().enumerate() {
        send_message(
            &handle,
            message(id, index as i64 + 1, "u1", "Alice", STOPWORD_TEXT),
        )
        .await;
    }
    wait_for_relevance_calls(&doubles.relevance, 1).await;
    handle.snapshot().await.expect("the snapshot succeeds");

    // Exactly the valid fact and the entry's own known_as edge (the
    // shallow set); neither decoy surfaces through any source.
    let texts = presented_texts(&doubles);
    assert_eq!(texts.len(), 2, "no decoy reached the gate: {texts:?}");
    assert!(texts.contains(&"Alice likes tea.".to_string()));
    assert!(texts.contains(&"Al is a surface form of Alice.".to_string()));
    assert!(!texts.contains(&"The contains decoy edge.".to_string()));
    assert!(!texts.contains(&"The known_as decoy fact.".to_string()));
    assert!(!texts.contains(&"The contains decoy fact.".to_string()));

    handle.shutdown().await.expect("the actor reports no error");
}

/// Deep scenario 5 (decision 76, the Section 8.2 validity filter): an
/// invalidated fact surfaces through NO source. The stale edge KEEPS
/// its `edge_texts` sidecar row (the digest harvest ran before the
/// invalidation — the sidecar lags until the next reconciliation), so
/// the FTS source HITS the row; the hydration (`edges_by_ids`, valid
/// only) drops it. The shallow neighbor fetch and the two-hop
/// expansion filter `invalid_at` the same way.
#[tokio::test]
async fn deep_recall_never_surfaces_an_invalidated_fact() {
    let fixture = make_fixture().await;
    let alice = person_node("u1", "Alice");
    let tea = concept_node("tea");
    let boycott = concept_node("boycott");
    let valid = recent_edge(&alice.id, &tea.id, "related_to", "Alice likes tea.", 0);
    let stale = recent_edge(
        &alice.id,
        &boycott.id,
        "related_to",
        "Alice boycotts 龙井 after the scandal.",
        0,
    );
    seed_graph(
        &fixture,
        vec![alice, tea, boycott],
        vec![valid, stale.clone()],
    )
    .await;
    // The sidecar row of the stale edge (harvested pre-invalidation),
    // then the memory invalidate path (decision 75 (d)).
    seed_edge_text(&fixture, &edge_json_id(&stale), &stale.edge_text).await;
    let invalidated = fixture
        .memory
        .invalidate_edge(CHAT_ID, &edge_json_id(&stale), OffsetDateTime::now_utc())
        .await
        .expect("the invalidate succeeds");
    assert!(
        invalidated,
        "the seeded edge was valid before the invalidation"
    );

    let doubles = silent_doubles();
    // One candidate term (龙井): the FTS row of the INVALID edge is the
    // only sidecar hit.
    let embedder = Arc::new(ScriptedEmbedder::with_batches(vec![vec![unit_vector(0)]]));
    let handle = spawn_with_deep_wake(&fixture, wake_config(), t0(), &doubles, &embedder);

    let texts = ["ok ok thanks", "龙井", "ok ok thanks"];
    for (index, id) in ["m1", "m2", "m3"].iter().enumerate() {
        send_message(
            &handle,
            message(id, index as i64 + 1, "u1", "Alice", texts[index]),
        )
        .await;
    }
    wait_for_relevance_calls(&doubles.relevance, 1).await;
    handle.snapshot().await.expect("the snapshot succeeds");

    assert_eq!(embedder.calls(), vec![vec!["龙井".to_string()]]);
    // Only the valid fact is presented; the invalidated edge surfaced
    // through neither the FTS hit nor the graph reads.
    assert_eq!(
        presented_texts(&doubles),
        vec!["Alice likes tea.".to_string()]
    );

    handle.shutdown().await.expect("the actor reports no error");
}

/// Deep scenario 6 (decision 76 (c), Section 7.6 step 6): the startup
/// reconciliation repairs a seeded FTS gap and drops an orphan, END TO
/// END — a graph edge with NO sidecar row (the harvest loss) becomes
/// searchable after `reconcile_group`, and a wake then surfaces it
/// through the FTS source. The set-difference mechanics are covered at
/// core level (embedding.rs `reconciliation_repairs_the_edge_texts_
/// sidecar`); this test pins the repair-to-recall hand-off.
#[tokio::test]
async fn reconciliation_repairs_the_fts_gap_and_a_deep_wake_surfaces_the_edge() {
    let fixture = make_fixture().await;
    // A DETACHED fact: no shallow entry reaches it, so only the
    // repaired sidecar row can surface it.
    let deploy = concept_node("the deploy");
    let fix = concept_node("the fix");
    let edge = recent_edge(
        &deploy.id,
        &fix.id,
        "related_to",
        "Alice deploys the fix tonight.",
        0,
    );
    seed_graph(&fixture, vec![deploy, fix], vec![edge.clone()]).await;
    // An orphan row: its natural key matches no graph edge.
    let orphan_id = EdgeId {
        source_id: concept_id("gone"),
        relationship_name: "related_to".to_string(),
        target_id: concept_id("stale"),
        valid_at: OffsetDateTime::now_utc(),
    }
    .encode();
    seed_edge_text(&fixture, &orphan_id, "a tombstoned edge").await;

    // The startup reconciliation of the group (the embedding worker's
    // pass, decisions 66/76c) over a dedicated one-group target.
    let target = GroupEmbeddingTarget::open(fixture.data_root(), CHAT_ID)
        .expect("the reconciliation target opens");
    let report = reconcile_group(fixture.memory.as_ref(), &target).await;
    assert_eq!(
        report.edge_texts_upserted, 1,
        "the harvest-loss row is re-written"
    );
    assert_eq!(report.edge_texts_pruned, 1, "the orphan row is pruned");
    // The sidecar now holds exactly the graph's edge.
    let store = Arc::clone(&target.store);
    let ids = tokio::task::spawn_blocking(move || store.list_edge_text_ids())
        .await
        .expect("the blocking task joins")
        .expect("list_edge_text_ids succeeds");
    assert_eq!(ids, vec![edge_json_id(&edge)]);

    // ... and a deep wake surfaces the repaired row through FTS.
    let doubles = silent_doubles();
    let embedder = Arc::new(ScriptedEmbedder::with_batches(vec![vec![unit_vector(0)]]));
    let handle = spawn_with_deep_wake(&fixture, wake_config(), t0(), &doubles, &embedder);

    let texts = ["ok ok thanks", "deploys", "ok ok thanks"];
    for (index, id) in ["m1", "m2", "m3"].iter().enumerate() {
        send_message(
            &handle,
            message(id, index as i64 + 1, "u9", "阿杰", texts[index]),
        )
        .await;
    }
    wait_for_relevance_calls(&doubles.relevance, 1).await;
    handle.snapshot().await.expect("the snapshot succeeds");

    assert_eq!(embedder.calls(), vec![vec!["deploys".to_string()]]);
    assert_eq!(
        presented_texts(&doubles),
        vec!["Alice deploys the fix tonight.".to_string()]
    );

    handle.shutdown().await.expect("the actor reports no error");
}
