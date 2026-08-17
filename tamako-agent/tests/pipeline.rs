//! Integration tests of the digest pipeline: real `Store` and real
//! `LbugBackend` over a tempfile, scripted extractors. specs.md
//! Section 10.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tamako_agent::endpoint::EMBEDDING_DIMS;
use tamako_agent::resolve::{
    ConfirmationAnswer, ResolutionConfirmer, ScriptedConfirmer, VectorResolutionConfig,
};
use tamako_agent::{
    AgentDigestPipeline, BindingSource, ExtractedEdge, ExtractedNode, ExtractedNodeType,
    KnowledgeGraph, PipelineConfig, ScriptedExtractor,
};
use tamako_core::digest::{DigestOutcome, DigestPipeline};
use tamako_core::embedding::{EmbeddingError, EmbeddingProvider};
use tamako_memory::{identifiers, LbugBackend, MemoryBackend, MemoryBatch, MemoryNode, NodeType};
use tamako_store::{Direction, EventType, NewMessage, Store};
use tempfile::TempDir;
use time::OffsetDateTime;

const CHAT: &str = "pipeline_test";

fn fixtures() -> (TempDir, Arc<Store>, Arc<LbugBackend>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(Store::new(dir.path()));
    let memory = Arc::new(LbugBackend::new(dir.path()));
    (dir, store, memory)
}

fn message(
    platform_id: &str,
    direction: Direction,
    sender_id: &str,
    display_name: &str,
    text: &str,
    timestamp_secs: i64,
    reply_to: Option<&str>,
) -> NewMessage {
    NewMessage {
        platform_msg_id: platform_id.to_string(),
        direction,
        event_type: EventType::Message,
        timestamp: OffsetDateTime::from_unix_timestamp(timestamp_secs).expect("valid timestamp"),
        sender_id: sender_id.to_string(),
        sender_display_name: display_name.to_string(),
        sender_username: None,
        text: text.to_string(),
        reply_to_platform_msg_id: reply_to.map(str::to_string),
        mentions_bot: false,
        is_reply_to_bot: false,
    }
}

fn insert_all(store: &Store, messages: &[NewMessage]) {
    for msg in messages {
        store.insert_message(CHAT, msg).expect("insert");
    }
}

fn test_config() -> PipelineConfig {
    PipelineConfig {
        max_retries: 3,
        retry_base_delay: Duration::from_millis(1),
    }
}

fn pipeline<M: Into<Arc<ScriptedExtractor>>>(
    store: &Arc<Store>,
    memory: &Arc<LbugBackend>,
    extractor: M,
) -> Arc<AgentDigestPipeline<LbugBackend>> {
    Arc::new(AgentDigestPipeline::new(
        store.clone(),
        memory.clone(),
        extractor.into(),
        test_config(),
    ))
}

async fn count(memory: &LbugBackend, cypher: &str) -> i64 {
    memory
        .query_rows(CHAT, cypher)
        .await
        .expect("query")
        .first()
        .and_then(|row| row.first())
        .and_then(|value| value.parse::<i64>().ok())
        .expect("count")
}

fn alice_likes_deploy_graph() -> KnowledgeGraph {
    KnowledgeGraph {
        nodes: vec![
            ExtractedNode {
                name: "Alice".to_string(),
                node_type: ExtractedNodeType::Person,
                description: "A group member who deploys.".to_string(),
            },
            ExtractedNode {
                name: "the deploy".to_string(),
                node_type: ExtractedNodeType::Concept,
                description: "The nightly deploy.".to_string(),
            },
        ],
        edges: vec![ExtractedEdge {
            source: "Alice".to_string(),
            target: "the deploy".to_string(),
            relationship_name: "works_on".to_string(),
            description: "Alice deploys the fix tonight.".to_string(),
        }],
    }
}

#[tokio::test]
async fn a_skeleton_batch_stores_the_skeleton_without_calling_the_extractor() {
    // Section 7.2 rule 5 of the database spec.
    let (_dir, store, memory) = fixtures();
    insert_all(
        &store,
        &[
            message(
                "m1",
                Direction::Inbound,
                "1001",
                "Alice",
                "hi",
                1_700_000_000,
                None,
            ),
            message(
                "m2",
                Direction::Inbound,
                "2002",
                "Bob",
                "🎉🎉",
                1_700_000_060,
                None,
            ),
            message(
                "m3",
                Direction::Inbound,
                "1001",
                "Alice",
                "thanks",
                1_700_000_120,
                None,
            ),
        ],
    );
    let extractor = Arc::new(ScriptedExtractor::with_graphs(vec![
        alice_likes_deploy_graph(),
    ]));
    let pipeline = pipeline(&store, &memory, extractor.clone());

    let outcome = pipeline
        .run_digest(CHAT, 0)
        .await
        .expect("digest")
        .expect("non-empty tail");

    let expected_batch_id = identifiers::batch_id(1, 3);
    assert_eq!(
        outcome,
        DigestOutcome::Skeleton {
            batch_id: expected_batch_id.clone(),
            new_boundary: 3,
        }
    );
    // The extractor was never called.
    assert!(extractor.inputs().is_empty());
    // The graph holds the MessageBatch skeleton only.
    assert_eq!(
        count(
            &memory,
            "MATCH (n:Node) WHERE n.type = 'MessageBatch' RETURN count(n)"
        )
        .await,
        1
    );
    assert_eq!(count(&memory, "MATCH (n:Node) RETURN count(n)").await, 1);
    assert_eq!(
        count(&memory, "MATCH ()-[r:EDGE]->() RETURN count(r)").await,
        0
    );
    // The skeleton carries the real properties (Section 6.2).
    let rows = memory
        .query_rows(
            CHAT,
            &format!("MATCH (n:Node {{id: '{expected_batch_id}'}}) RETURN n.properties"),
        )
        .await
        .expect("query properties");
    let properties = rows[0][0].as_str();
    assert!(properties.contains("\"first_msg_id\":1"));
    assert!(properties.contains("\"last_msg_id\":3"));
    assert!(properties.contains("\"msg_count\":3"));
}

#[tokio::test]
async fn the_happy_path_extracts_validates_resolves_and_writes() {
    // specs.md Section 10.2. Rule B1: the bot's own message is digest
    // input.
    let (_dir, store, memory) = fixtures();
    insert_all(
        &store,
        &[
            message(
                "m1",
                Direction::Inbound,
                "1001",
                "Alice",
                "I will deploy the fix tonight",
                1_700_000_000,
                None,
            ),
            message(
                "m2",
                Direction::Inbound,
                "2002",
                "Bob",
                "the staging deploy is already done",
                1_700_000_060,
                Some("m1"),
            ),
            // Rule B1: the bot's own speech is part of the digest input.
            message(
                "m3",
                Direction::Outbound,
                "9000",
                "Tamako",
                "deploy sounds important, nya",
                1_700_000_120,
                Some("m2"),
            ),
        ],
    );
    let extractor = Arc::new(ScriptedExtractor::with_graphs(vec![
        alice_likes_deploy_graph(),
    ]));
    let pipeline = pipeline(&store, &memory, extractor.clone());

    let outcome = pipeline
        .run_digest(CHAT, 0)
        .await
        .expect("digest")
        .expect("non-empty tail");

    assert_eq!(
        outcome,
        DigestOutcome::Extracted {
            batch_id: identifiers::batch_id(1, 3),
            new_boundary: 3,
            // Person + Concept + 2 Alias + MessageBatch.
            node_count: 5,
            // known_as + also_known_as + works_on + 2 contains.
            edge_count: 5,
        }
    );

    // The extraction input carries the labeled messages (Section 7.2
    // step 4) and the mention/reply map (specs.md Section 10.1).
    let inputs = extractor.inputs();
    assert_eq!(inputs.len(), 1);
    let input = &inputs[0];
    assert_eq!(input.messages.len(), 3);
    assert_eq!(input.messages[0].display_name, "Alice");
    assert_eq!(input.messages[0].time_hhmm, "22:13"); // 1_700_000_000 UTC
    assert_eq!(input.messages[2].display_name, "Tamako");
    // Mention map: Alice (sender), Bob (sender; the reply-target dedupe
    // of m3 loses to the sender binding of m2), Tamako (sender).
    let bindings: Vec<(&str, &str, BindingSource)> = input
        .mention_map
        .iter()
        .map(|b| (b.display_name.as_str(), b.tg_user_id.as_str(), b.source))
        .collect();
    assert_eq!(
        bindings,
        vec![
            ("Alice", "1001", BindingSource::Sender),
            ("Bob", "2002", BindingSource::Sender),
            ("Tamako", "9000", BindingSource::Sender),
        ]
    );

    // The graph state. Section 7.4 step 1: Alice bound to person_id.
    assert_eq!(
        count(
            &memory,
            &format!(
                "MATCH (n:Node {{id: '{}'}}) RETURN count(n)",
                identifiers::person_id("1001")
            )
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &memory,
            "MATCH ()-[r:EDGE]->() WHERE r.relationship_name = 'works_on' RETURN count(r)"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &memory,
            "MATCH ()-[r:EDGE]->() WHERE r.relationship_name = 'contains' RETURN count(r)"
        )
        .await,
        2
    );
    // No dead letters, no failure counters.
    assert!(store.list_dead_letters(CHAT).expect("letters").is_empty());
    assert_eq!(
        store
            .get_state(CHAT, "digest_failures_total")
            .expect("state"),
        None
    );
}

#[tokio::test]
async fn a_failing_extractor_dead_letters_after_all_retries() {
    // specs.md Section 10.3: max_retries TOTAL attempts, then the
    // dead-letter table, the metric, and the skip. The failure must NOT
    // escape as Err; it ends as Ok(Some(DeadLettered)).
    let (_dir, store, memory) = fixtures();
    insert_all(
        &store,
        &[message(
            "m1",
            Direction::Inbound,
            "1001",
            "Alice",
            "the migration broke the staging database",
            1_700_000_000,
            None,
        )],
    );
    let extractor = Arc::new(ScriptedExtractor::failing("provider boom"));
    let pipeline = pipeline(&store, &memory, extractor.clone());

    let outcome = pipeline
        .run_digest(CHAT, 0)
        .await
        .expect("a dead-lettered batch is Ok")
        .expect("non-empty tail");

    let expected_batch_id = identifiers::batch_id(1, 1);
    match outcome {
        DigestOutcome::DeadLettered {
            batch_id,
            new_boundary,
            error,
        } => {
            assert_eq!(batch_id, expected_batch_id);
            assert_eq!(new_boundary, 1);
            assert!(error.contains("provider boom"));
        }
        other => panic!("expected DeadLettered, got {other:?}"),
    }

    // Exactly max_retries attempts (3).
    assert_eq!(extractor.inputs().len(), 3);
    // The dead-letter row: skeleton JSON plus the error.
    let letters = store.list_dead_letters(CHAT).expect("letters");
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].batch_id, expected_batch_id);
    assert!(letters[0].batch_skeleton.contains("\"first_msg_id\":1"));
    assert!(letters[0].batch_skeleton.contains("\"msg_count\":1"));
    assert!(letters[0].error.contains("provider boom"));
    // The counters of specs.md Section 12.
    assert_eq!(
        store
            .get_state(CHAT, "digest_failures_total")
            .expect("state"),
        Some("3".to_string())
    );
    assert_eq!(
        store.get_state(CHAT, "dead_letters_total").expect("state"),
        Some("1".to_string())
    );
    // The skipped range stays in the raw log (Section 10.3 item 3).
    assert_eq!(store.list_messages(CHAT).expect("log").len(), 1);
    // Nothing reached the graph.
    assert_eq!(count(&memory, "MATCH (n:Node) RETURN count(n)").await, 0);
}

#[tokio::test]
async fn running_the_same_range_twice_is_idempotent() {
    // specs.md Section 10.3 item 1: retries use the same batch id; the
    // MERGE operations are idempotent (anti-defer list, dev-roadmap.md
    // Section 7).
    let (_dir, store, memory) = fixtures();
    insert_all(
        &store,
        &[
            message(
                "m1",
                Direction::Inbound,
                "1001",
                "Alice",
                "I will deploy the fix tonight",
                1_700_000_000,
                None,
            ),
            message(
                "m2",
                Direction::Inbound,
                "2002",
                "Bob",
                "the staging deploy is already done",
                1_700_000_060,
                Some("m1"),
            ),
        ],
    );
    let extractor = Arc::new(ScriptedExtractor::with_graphs(vec![
        alice_likes_deploy_graph(),
        alice_likes_deploy_graph(),
    ]));
    let pipeline = pipeline(&store, &memory, extractor);

    for run in 1..=2 {
        let outcome = pipeline
            .run_digest(CHAT, 0)
            .await
            .expect("digest")
            .expect("non-empty tail");
        assert!(
            matches!(outcome, DigestOutcome::Extracted { .. }),
            "run {run} must extract"
        );
    }

    assert_eq!(count(&memory, "MATCH (n:Node) RETURN count(n)").await, 5);
    assert_eq!(
        count(&memory, "MATCH ()-[r:EDGE]->() RETURN count(r)").await,
        5
    );
    assert_eq!(
        count(
            &memory,
            "MATCH (n:Node) WHERE n.type = 'MessageBatch' RETURN count(n)"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn an_empty_tail_returns_none() {
    // The DigestPipeline contract: Ok(None) when there is nothing to
    // digest.
    let (_dir, store, memory) = fixtures();
    insert_all(
        &store,
        &[message(
            "m1",
            Direction::Inbound,
            "1001",
            "Alice",
            "a substantive message about the release plan",
            1_700_000_000,
            None,
        )],
    );
    let extractor = Arc::new(ScriptedExtractor::with_graphs(vec![]));
    let pipeline = pipeline(&store, &memory, extractor.clone());

    let outcome = pipeline.run_digest(CHAT, 1).await.expect("digest");
    assert_eq!(outcome, None);
    assert!(extractor.inputs().is_empty());
}

// ---- Decision 73: the vector pre-screen end to end. Real tempdir
// Stores + real LbugBackend, scripted extractor/provider/confirmer
// doubles (the same shape as the pipeline.rs inline tests). The replay
// binary wires a None provider (deterministic, network-free), so these
// tests construct the pipeline directly with BOTH builders:
// `with_embedding_store` (the decision-66 dedicated one-group store the
// KNN read needs) and `with_vector_prescreen`. ----

/// A scripted core embedding provider for the pre-screen tests (the
/// same pattern as `ScriptedExtractor`): one queued batch per
/// `embed_texts` call, every call recorded for assertions.
struct ScriptedEmbedder {
    batches: Mutex<VecDeque<Vec<Vec<f32>>>>,
    calls: Mutex<Vec<Vec<String>>>,
}

impl ScriptedEmbedder {
    fn with_batches(batches: Vec<Vec<Vec<f32>>>) -> Self {
        ScriptedEmbedder {
            batches: Mutex::new(batches.into()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.lock().expect("calls lock").len()
    }
}

impl EmbeddingProvider for ScriptedEmbedder {
    fn embed<'a>(
        &'a self,
        text: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<f32>, EmbeddingError>> + Send + 'a>> {
        Box::pin(async move {
            let mut batch = self.embed_texts(&[text.to_string()]).await?;
            Ok(batch.remove(0))
        })
    }

    fn embed_texts<'a>(
        &'a self,
        texts: &'a [String],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Vec<f32>>, EmbeddingError>> + Send + 'a>> {
        self.calls.lock().expect("calls lock").push(texts.to_vec());
        let batch = self
            .batches
            .lock()
            .expect("batches lock")
            .pop_front()
            .expect("scripted embedder: exhausted queue");
        Box::pin(async move { Ok(batch) })
    }
}

/// A 4096-dimensional unit basis vector (the pinned sidecar dimension
/// of decision 66); identical query/candidate vectors give cosine
/// similarity 1.0, above the 0.92 match threshold.
fn unit_vector(dim: usize) -> Vec<f32> {
    let mut vector = vec![0.0; EMBEDDING_DIMS];
    vector[dim] = 1.0;
    vector
}

/// A unit vector whose cosine similarity with `unit_vector(0)` is
/// exactly `cosine` (up to f32 precision).
fn tilted_vector(cosine: f32, dim: usize) -> Vec<f32> {
    let mut vector = vec![0.0; EMBEDDING_DIMS];
    vector[0] = cosine;
    vector[dim] = (1.0 - cosine * cosine).sqrt();
    vector
}

/// The group's dedicated one-group embedding Store of decision 66: the
/// chat_id-less sidecar helpers (`upsert_node_embedding`, the KNN read)
/// require exactly one open group per Store instance.
fn dedicated_embedding_store(dir: &TempDir) -> Arc<Store> {
    let store = Arc::new(Store::new(dir.path()));
    store.open_group(CHAT).expect("open group");
    store
}

/// Seeds one bare Person node (a pre-screen candidate; no alias edges,
/// so step 2 never fires for the entity under test).
async fn seed_person_node(memory: &LbugBackend, id: &str, name: &str, description: Option<&str>) {
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
    memory.ensure_schema(CHAT).await.expect("schema");
    let batch = MemoryBatch {
        batch_id: format!("seed-{id}"),
        nodes: vec![MemoryNode {
            id: id.to_string(),
            name: name.to_string(),
            node_type: NodeType::Person,
            created_at: now,
            updated_at: now,
            properties: description
                .map(|text| serde_json::json!({ "description": text }).to_string()),
        }],
        edges: vec![],
    };
    memory.upsert_batch(CHAT, &batch).await.expect("seed");
}

/// The decision-73 digest batch: the extracted person "Al" and the
/// concept "GRPO" match NO mention binding (the senders are Alice/Bob)
/// and no alias on the first run, so both reach step 3.
fn al_likes_grpo_graph() -> KnowledgeGraph {
    KnowledgeGraph {
        nodes: vec![
            ExtractedNode {
                name: "Al".to_string(),
                node_type: ExtractedNodeType::Person,
                description: "Alice, the group member who deploys.".to_string(),
            },
            ExtractedNode {
                name: "GRPO".to_string(),
                node_type: ExtractedNodeType::Concept,
                description: "A training method Alice finds unstable.".to_string(),
            },
        ],
        edges: vec![ExtractedEdge {
            source: "Al".to_string(),
            target: "GRPO".to_string(),
            relationship_name: "likes".to_string(),
            description: "Al likes GRPO despite its instability.".to_string(),
        }],
    }
}

/// The two-message raw log of the pre-screen tests: the senders are
/// Alice and Bob, so neither "Al"/"Alicia" nor "GRPO" binds at step 1.
fn insert_prescreen_messages(store: &Store) {
    insert_all(
        store,
        &[
            message(
                "m1",
                Direction::Inbound,
                "1001",
                "Alice",
                "I will deploy the fix tonight",
                1_700_000_000,
                None,
            ),
            message(
                "m2",
                Direction::Inbound,
                "2002",
                "Bob",
                "the staging deploy is already done",
                1_700_000_060,
                Some("m1"),
            ),
        ],
    );
}

/// Builds the pipeline the way live mode does (decision 73): the shared
/// store for the digest reads, the dedicated one-group embedding store
/// for the enqueue and the KNN read, and the scripted provider/confirmer
/// of the step-3 pre-screen.
fn prescreen_pipeline(
    store: &Arc<Store>,
    memory: &Arc<LbugBackend>,
    extractor: ScriptedExtractor,
    embedding_store: &Arc<Store>,
    provider: Arc<ScriptedEmbedder>,
    confirmer: Arc<ScriptedConfirmer>,
) -> AgentDigestPipeline<LbugBackend> {
    AgentDigestPipeline::new(
        Arc::clone(store),
        Arc::clone(memory),
        Arc::new(extractor),
        test_config(),
    )
    .with_embedding_store(Arc::clone(embedding_store))
    .with_vector_prescreen(
        provider as Arc<dyn EmbeddingProvider>,
        confirmer as Arc<dyn ResolutionConfirmer>,
        VectorResolutionConfig::default(),
    )
}

/// The sorted node ids of the whole graph (the convergence witness of
/// the idempotency proof).
async fn sorted_node_ids(memory: &LbugBackend) -> Vec<String> {
    let mut ids: Vec<String> = memory
        .query_rows(CHAT, "MATCH (n:Node) RETURN n.id")
        .await
        .expect("node ids")
        .into_iter()
        .map(|mut row| row.remove(0))
        .collect();
    ids.sort();
    ids
}

fn vector_counter(store: &Store, key: &str) -> Option<String> {
    store.get_state(CHAT, key).expect("state")
}

#[tokio::test]
async fn a_replayed_batch_converges_through_the_prescreen_without_duplicate_nodes() {
    // The decision-73 acceptance test: ONE scripted batch through the
    // full pipeline TWICE (a replayed/retried batch, specs.md Section
    // 10.3). Run 1: "Al" auto-matches the pre-seeded person node
    // (similarity 1.0 >= 0.92, the matched counter increments); "GRPO"
    // finds no compatible hit and creates its deterministic concept
    // node. Between the runs the decision-66 WORKER is emulated by
    // upserting the run-1 concept node's embedding into the sidecar
    // index (the digest itself only ENQUEUES the pair; the worker fills
    // the index asynchronously). Run 2 must bind the SAME node ids and
    // create no second node for either entity.
    let (dir, store, memory) = fixtures();
    insert_prescreen_messages(&store);
    let alice_id = identifiers::person_id("1001");
    let grpo_id = identifiers::concept_id("GRPO");
    seed_person_node(
        &memory,
        &alice_id,
        "Alice",
        Some("A group member who deploys."),
    )
    .await;
    let embedding_store = dedicated_embedding_store(&dir);
    embedding_store
        .upsert_node_embedding(&alice_id, &unit_vector(0))
        .expect("seed embedding");

    // ONE batched embeddings call covers both unresolved entities, in
    // graph order: "Al" identical to the seeded candidate, "GRPO"
    // orthogonal to every indexed vector. Run 2 needs no second batch:
    // the run-1 alias edges bind both entities deterministically.
    let provider = Arc::new(ScriptedEmbedder::with_batches(vec![vec![
        unit_vector(0),
        unit_vector(1),
    ]]));
    let confirmer = Arc::new(ScriptedConfirmer::with_answers(vec![]));
    let pipeline = prescreen_pipeline(
        &store,
        &memory,
        ScriptedExtractor::with_graphs(vec![al_likes_grpo_graph(), al_likes_grpo_graph()]),
        &embedding_store,
        Arc::clone(&provider),
        Arc::clone(&confirmer),
    );

    // Run 1.
    let outcome = pipeline
        .run_digest(CHAT, 0)
        .await
        .expect("digest")
        .expect("non-empty tail");
    match outcome {
        // Person (the seeded candidate) + Alias "Al" + Concept GRPO +
        // Alias "GRPO" + MessageBatch.
        DigestOutcome::Extracted {
            node_count,
            edge_count,
            ..
        } => {
            assert_eq!(node_count, 5);
            // known_as + also_known_as + likes + 2 contains.
            assert_eq!(edge_count, 5);
        }
        other => panic!("expected Extracted, got {other:?}"),
    }
    // The auto-match bound "Al" to the seeded node; no confirmation ran.
    assert_eq!(
        vector_counter(&store, "vector_resolution_matched_total"),
        Some("1".to_string())
    );
    assert_eq!(
        vector_counter(&store, "vector_resolution_confirmed_total"),
        None
    );
    assert_eq!(provider.call_count(), 1);
    assert!(confirmer.calls().is_empty());

    // The worker emulation: the run-1 concept node's embedding lands in
    // the sidecar index before the replay.
    embedding_store
        .upsert_node_embedding(&grpo_id, &unit_vector(1))
        .expect("the emulated worker fill");

    let ids_before = sorted_node_ids(&memory).await;
    let nodes_before = count(&memory, "MATCH (n:Node) RETURN count(n)").await;
    let edges_before = count(&memory, "MATCH ()-[r:EDGE]->() RETURN count(r)").await;

    // Run 2: the SAME range again (the retry of specs.md Section 10.3,
    // stable batch id included).
    let outcome = pipeline
        .run_digest(CHAT, 0)
        .await
        .expect("digest")
        .expect("non-empty tail");
    assert!(
        matches!(outcome, DigestOutcome::Extracted { .. }),
        "the replay extracts, got {outcome:?}"
    );

    // (a) The replay binds the SAME node ids and adds nothing.
    assert_eq!(
        ids_before,
        sorted_node_ids(&memory).await,
        "the replay binds the same node ids"
    );
    assert_eq!(
        nodes_before,
        count(&memory, "MATCH (n:Node) RETURN count(n)").await,
        "the replay adds no node"
    );
    assert_eq!(
        edges_before,
        count(&memory, "MATCH ()-[r:EDGE]->() RETURN count(r)").await,
        "the replay adds no edge"
    );
    // (b) Exactly ONE node per re-run entity: no duplicate of the
    // run-1 concept, no second person next to the seeded candidate.
    assert_eq!(
        count(
            &memory,
            &format!("MATCH (n:Node {{id: '{grpo_id}'}}) RETURN count(n)")
        )
        .await,
        1,
        "one concept node across the replay"
    );
    assert_eq!(
        count(
            &memory,
            "MATCH (n:Node) WHERE n.type = 'Person' RETURN count(n)"
        )
        .await,
        1,
        "one person node across the replay"
    );
    // (c) The run-1 alias edges made step 2 deterministic: the replay
    // did not even call the provider, and the matched counter did not
    // double.
    assert_eq!(provider.call_count(), 1);
    assert_eq!(
        vector_counter(&store, "vector_resolution_matched_total"),
        Some("1".to_string())
    );
}

#[tokio::test]
async fn the_middle_band_confirmation_binds_the_candidate_without_a_duplicate() {
    // Decision 73 middle band end to end: "Alicia" reaches step 3, the
    // seeded person node scores in the confirmation band (similarity
    // 0.85 in [0.80, 0.92)), the scripted confirmer accepts, and the
    // entity binds to the candidate: the confirmed counter increments
    // and NO second person node appears.
    let (dir, store, memory) = fixtures();
    insert_prescreen_messages(&store);
    let alice_id = identifiers::person_id("1001");
    seed_person_node(
        &memory,
        &alice_id,
        "Alice",
        Some("A group member who deploys."),
    )
    .await;
    let embedding_store = dedicated_embedding_store(&dir);
    embedding_store
        .upsert_node_embedding(&alice_id, &tilted_vector(0.85, 1))
        .expect("seed embedding");

    let provider = Arc::new(ScriptedEmbedder::with_batches(vec![vec![unit_vector(0)]]));
    let confirmer = Arc::new(ScriptedConfirmer::with_answers(vec![ConfirmationAnswer {
        same: true,
        reason: "scripted: the same person".to_string(),
    }]));
    let alicia_graph = KnowledgeGraph {
        nodes: vec![ExtractedNode {
            name: "Alicia".to_string(),
            node_type: ExtractedNodeType::Person,
            description: "Alice, the group member who deploys.".to_string(),
        }],
        edges: vec![],
    };
    let pipeline = prescreen_pipeline(
        &store,
        &memory,
        ScriptedExtractor::with_graphs(vec![alicia_graph]),
        &embedding_store,
        Arc::clone(&provider),
        Arc::clone(&confirmer),
    );

    let outcome = pipeline
        .run_digest(CHAT, 0)
        .await
        .expect("digest")
        .expect("non-empty tail");
    match outcome {
        // Person (the accepted candidate) + Alias "Alicia" + MessageBatch.
        DigestOutcome::Extracted {
            node_count,
            edge_count,
            ..
        } => {
            assert_eq!(node_count, 3);
            // known_as + contains.
            assert_eq!(edge_count, 2);
        }
        other => panic!("expected Extracted, got {other:?}"),
    }

    assert_eq!(
        vector_counter(&store, "vector_resolution_confirmed_total"),
        Some("1".to_string())
    );
    assert_eq!(
        vector_counter(&store, "vector_resolution_matched_total"),
        None
    );
    assert_eq!(
        vector_counter(&store, "vector_resolution_rejected_total"),
        None
    );
    // The confirmation call presented the extracted entity and the
    // STORED candidate content.
    let calls = confirmer.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0.name, "Alicia");
    assert_eq!(calls[0].1.name, "Alice");
    // No duplicate: exactly one Person node, the seeded candidate.
    assert_eq!(
        count(
            &memory,
            "MATCH (n:Node) WHERE n.type = 'Person' RETURN count(n)"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &memory,
            &format!("MATCH (n:Node {{id: '{alice_id}'}}) RETURN count(n)")
        )
        .await,
        1
    );
}
