//! End-to-end tests of the M1 digest pipeline (specs.md Section 10)
//! against the REAL SQLite store and the REAL LadybugDB backend. The
//! actor drives the pipeline through a scripted extractor; the raw log
//! is built through the actor from `NormalizedMessage` events (no JSON
//! fixture changes). Tests c and d drive the pipeline directly (specs.md
//! Section 10.3 concerns the pipeline, not the actor).

use std::sync::Arc;
use std::time::Duration;

use tamako_agent::{
    AgentDigestPipeline, ExtractedEdge, ExtractedNode, ExtractedNodeType, KnowledgeGraph,
    PipelineConfig, ScriptedExtractor,
};
use tamako_core::actor::{
    spawn_group_actor, GroupActorHandle, GroupActorParams, DEFAULT_INBOX_CAPACITY,
};
use tamako_core::config::TriggerConfig;
use tamako_core::digest::{DigestOutcome, DigestPipeline};
use tamako_core::event::{InboundEvent, NormalizedMessage};
use tamako_memory::identifiers::{concept_id, person_id};
use tamako_memory::LbugBackend;
use tamako_store::{DeadLetterRow, Direction, EventType, NewMessage, Store, StoreError};
use time::OffsetDateTime;

const CHAT_ID: &str = "-1001234567890";

/// The bounded waits of this suite. The scripted extractor returns
/// immediately, so a digest round trip is milliseconds; five seconds is
/// generous enough for a loaded CI machine.
const DIGEST_TIMEOUT: Duration = Duration::from_secs(5);
/// The poll interval of `wait_for_boundary`.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Fixed base time for deterministic replay.
fn t0() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("a valid unix timestamp")
}

/// A config whose digest trigger fires every `max_messages` tail rows
/// (specs.md Section 8.2, the messages threshold).
fn digest_config(max_messages: u32) -> TriggerConfig {
    TriggerConfig {
        digest_max_messages: max_messages,
        ..TriggerConfig::default()
    }
}

fn message(id: &str, seconds: i64, sender_id: &str, name: &str, text: &str) -> NormalizedMessage {
    NormalizedMessage {
        platform_msg_id: id.to_string(),
        timestamp: t0() + time::Duration::seconds(seconds),
        sender_id: sender_id.to_string(),
        sender_display_name: name.to_string(),
        text: text.to_string(),
        reply_to_platform_msg_id: None,
        mentions_bot: false,
        is_reply_to_bot: false,
    }
}

/// The scripted extraction of the Alice/GRPO batch: Alice (a group
/// member, bound to her sender id by the mention map, Section 7.4
/// step 1) likes GRPO (a concept).
fn alice_grpo_graph() -> KnowledgeGraph {
    KnowledgeGraph {
        nodes: vec![
            ExtractedNode {
                name: "Alice".to_string(),
                node_type: ExtractedNodeType::Person,
                description: "Alice talked about GRPO.".to_string(),
            },
            ExtractedNode {
                name: "GRPO".to_string(),
                node_type: ExtractedNodeType::Concept,
                description: "A training method Alice finds unstable.".to_string(),
            },
        ],
        edges: vec![ExtractedEdge {
            source: "Alice".to_string(),
            target: "GRPO".to_string(),
            relationship_name: "likes".to_string(),
            description: "Alice likes GRPO despite its instability.".to_string(),
        }],
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

fn scripted_pipeline(
    fixture: &Fixture,
    extractor: Arc<ScriptedExtractor>,
    config: PipelineConfig,
) -> Arc<AgentDigestPipeline<LbugBackend>> {
    Arc::new(AgentDigestPipeline::new(
        Arc::clone(&fixture.store),
        Arc::clone(&fixture.memory),
        extractor,
        config,
    ))
}

/// Spawns the real actor with the real pipeline over a scripted
/// extractor.
fn spawn_with_digest(
    fixture: &Fixture,
    config: TriggerConfig,
    extractor: Arc<ScriptedExtractor>,
) -> GroupActorHandle {
    let digest = scripted_pipeline(fixture, extractor, PipelineConfig::default());
    spawn_group_actor(GroupActorParams {
        chat_id: CHAT_ID.to_string(),
        store: Arc::clone(&fixture.store),
        memory: Arc::clone(&fixture.memory),
        config,
        started_at: t0(),
        inbox_capacity: DEFAULT_INBOX_CAPACITY,
        digest: Some(digest),
        post_digest_hook: None,
    })
}

/// Polls `snapshot()` until the digest boundary reaches `min` or the
/// timeout elapses. The snapshot is a FIFO barrier, and the spawned
/// digest task reports through the inbox: a snapshot that shows the
/// boundary proves the whole completion handler (boundary update,
/// persistence, hook, re-evaluation) already ran. Deterministic.
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

/// Runs one read-only Cypher query through the real backend.
async fn query(fixture: &Fixture, cypher: &str) -> Vec<Vec<String>> {
    fixture
        .memory
        .query_rows(CHAT_ID, cypher)
        .await
        .expect("the query succeeds")
}

/// Runs a `RETURN count(...)` query and parses the single cell.
async fn count(fixture: &Fixture, cypher: &str) -> i64 {
    let rows = query(fixture, cypher).await;
    assert_eq!(rows.len(), 1, "a count query returns one row: {cypher}");
    assert_eq!(rows[0].len(), 1, "a count query returns one cell: {cypher}");
    rows[0][0].parse().expect("a count cell is a number")
}

/// Inserts rows directly into the raw log (no actor). Tests c and d
/// drive the pipeline directly; the log rows are their input. `offset`
/// is the index of the first row (platform ids and timestamps derive
/// from it).
async fn insert_messages(fixture: &Fixture, offset: usize, texts: &[(&str, &str, &str)]) {
    let store = Arc::clone(&fixture.store);
    let rows: Vec<NewMessage> = texts
        .iter()
        .enumerate()
        .map(|(index, (sender_id, name, text))| NewMessage {
            platform_msg_id: format!("m{}", offset + index + 1),
            direction: Direction::Inbound,
            event_type: EventType::Message,
            timestamp: t0() + time::Duration::seconds((offset + index) as i64),
            sender_id: sender_id.to_string(),
            sender_display_name: name.to_string(),
            text: text.to_string(),
            reply_to_platform_msg_id: None,
            mentions_bot: false,
            is_reply_to_bot: false,
        })
        .collect();
    tokio::task::spawn_blocking(move || {
        store.open_group(CHAT_ID)?;
        for row in &rows {
            store.insert_message(CHAT_ID, row)?;
        }
        Ok::<_, StoreError>(())
    })
    .await
    .expect("the blocking task joins")
    .expect("the inserts succeed");
}

async fn list_dead_letters(fixture: &Fixture) -> Vec<DeadLetterRow> {
    let store = Arc::clone(&fixture.store);
    tokio::task::spawn_blocking(move || store.list_dead_letters(CHAT_ID))
        .await
        .expect("the blocking task joins")
        .expect("list_dead_letters succeeds")
}

async fn counter(fixture: &Fixture, key: &str) -> Option<String> {
    let store = Arc::clone(&fixture.store);
    let key = key.to_string();
    tokio::task::spawn_blocking(move || store.get_state(CHAT_ID, &key))
        .await
        .expect("the blocking task joins")
        .expect("get_state succeeds")
}

#[tokio::test]
async fn digest_runs_end_to_end_against_the_real_backend() {
    // Five messages, digest_max_messages = 3: the trigger fires at
    // message 3 and exactly one batch is written (the remaining tail
    // stays below the threshold). The batch is the range
    // `(boundary, current_tail]` of specs.md Section 10.1 — the tail at
    // EXECUTION time — so the batch can also cover message 4 or 5 when
    // the spawned digest task reads the log after their intake. The
    // final boundary is therefore in 3..=5; everything else below is
    // exact.
    let extractor = Arc::new(ScriptedExtractor::with_graphs(vec![alice_grpo_graph()]));
    let fixture = make_fixture();
    let handle = spawn_with_digest(&fixture, digest_config(3), Arc::clone(&extractor));
    let messages = [
        message("m1", 1, "u1", "Alice", "morning all"),
        message("m2", 2, "u1", "Alice", "GRPO looks unstable"),
        message("m3", 3, "u2", "Bob", "really? ours converged fine"),
        message("m4", 4, "u1", "Alice", "lucky. my run diverged twice"),
        message("m5", 5, "u2", "Bob", "what learning rate?"),
    ];
    for msg in messages {
        handle
            .send_event(InboundEvent::Message(msg))
            .await
            .expect("the actor inbox is open");
    }
    // FIFO barrier: all five intakes are processed.
    handle.snapshot().await.expect("the snapshot succeeds");
    let boundary = wait_for_boundary(&handle, 3, DIGEST_TIMEOUT).await;
    assert!(
        (3..=5).contains(&boundary),
        "the batch covers log rows 1..=boundary, got {boundary}"
    );

    // The scripted extractor saw exactly one batch: the batch covers
    // log rows 1..=boundary.
    let inputs = extractor.inputs();
    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0].messages.len(), boundary as usize);

    // The graph holds the batch and the extracted entities.
    assert_eq!(
        count(
            &fixture,
            "MATCH (n:Node) WHERE n.type = 'MessageBatch' RETURN count(n)"
        )
        .await,
        1
    );
    // Alice was the SENDER of every u1 row, so the mention-map binding
    // (Section 7.4 step 1) resolves her to the deterministic person id.
    let alice_id = person_id("u1");
    let grpo_id = concept_id("GRPO");
    assert_eq!(
        count(
            &fixture,
            &format!(
                "MATCH (n:Node) WHERE n.id = '{alice_id}' AND n.type = 'Person' RETURN count(n)"
            )
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture,
            &format!("MATCH (n:Node) WHERE n.id = '{grpo_id}' RETURN count(n)")
        )
        .await,
        1
    );
    // The fact edge, in the Phase 1 multi-value form (Section 7.5:
    // valid_at set, invalid_at NULL).
    assert_eq!(
        count(
            &fixture,
            &format!(
                "MATCH (s:Node {{id: '{alice_id}'}})-[r:EDGE]->(t:Node {{id: '{grpo_id}'}}) \
                 WHERE r.relationship_name = 'likes' AND r.invalid_at IS NULL RETURN count(r)"
            )
        )
        .await,
        1
    );
    // Provenance (Section 6.3): the batch contains both entities.
    for target_id in [&alice_id, &grpo_id] {
        assert_eq!(
            count(
                &fixture,
                &format!(
                    "MATCH (s:Node)-[r:EDGE]->(t:Node {{id: '{target_id}'}}) \
                     WHERE s.type = 'MessageBatch' AND r.relationship_name = 'contains' \
                     RETURN count(r)"
                )
            )
            .await,
            1
        );
    }
    // The surface-form alias edge of a Person (Section 6.3).
    assert_eq!(
        count(
            &fixture,
            &format!(
                "MATCH (s:Node {{id: '{alice_id}'}})-[r:EDGE]->(t:Node) \
                 WHERE r.relationship_name = 'known_as' RETURN count(r)"
            )
        )
        .await,
        1
    );
    handle.shutdown().await.expect("the actor reports no error");
}

#[tokio::test]
async fn digest_is_idempotent_under_retry() {
    // Same setup as the end-to-end test: one batch, boundary 3.
    let fixture = make_fixture();
    let extractor = Arc::new(ScriptedExtractor::with_graphs(vec![alice_grpo_graph()]));
    let handle = spawn_with_digest(&fixture, digest_config(3), extractor);
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
    let boundary = wait_for_boundary(&handle, 3, DIGEST_TIMEOUT).await;
    assert_eq!(boundary, 3);
    handle.shutdown().await.expect("the actor reports no error");

    let nodes_before = count(&fixture, "MATCH (n:Node) RETURN count(n)").await;
    let edges_before = count(&fixture, "MATCH ()-[r:EDGE]->() RETURN count(r)").await;

    // Simulated retry with a stable batch id (specs.md Section 10.3):
    // push the SAME range (0, 3] through a pipeline on the same store
    // and backend. The deterministic identifiers of Section 7.1 make
    // the MERGEs idempotent.
    let retry_extractor = Arc::new(ScriptedExtractor::with_graphs(vec![alice_grpo_graph()]));
    let retry = scripted_pipeline(&fixture, retry_extractor, PipelineConfig::default());
    let outcome = retry
        .run_digest(CHAT_ID, 0)
        .await
        .expect("the retry run succeeds")
        .expect("the tail is non-empty");
    assert!(
        matches!(
            outcome,
            DigestOutcome::Extracted {
                new_boundary: 3,
                ..
            }
        ),
        "the retry re-extracts the same batch, got {outcome:?}"
    );

    let nodes_after = count(&fixture, "MATCH (n:Node) RETURN count(n)").await;
    let edges_after = count(&fixture, "MATCH ()-[r:EDGE]->() RETURN count(r)").await;
    assert_eq!(nodes_before, nodes_after, "a retry adds no node");
    assert_eq!(edges_before, edges_after, "a retry adds no edge");
}

#[tokio::test]
async fn failed_batch_dead_letters_and_later_batches_still_process() {
    // specs.md Section 10.3: a failed batch never blocks later batches.
    // The actor is out of this test: the pipelines run directly.
    let fixture = make_fixture();
    // Batch 1 covers rows 1..=3. Insert them FIRST: the pipeline reads
    // the tail at execution time (Section 10.1), so the later rows
    // must not exist yet.
    insert_messages(
        &fixture,
        0,
        &[
            ("u1", "Alice", "morning all"),
            ("u1", "Alice", "GRPO looks unstable"),
            ("u2", "Bob", "really? ours converged fine"),
        ],
    )
    .await;

    // Batch 1: every attempt fails; max_retries = 2 total attempts.
    let failing = Arc::new(ScriptedExtractor::failing("boom"));
    let failing_pipeline = scripted_pipeline(
        &fixture,
        failing,
        PipelineConfig {
            max_retries: 2,
            // One millisecond: the backoff is real but the test stays
            // fast (bounded, deterministic).
            retry_base_delay: Duration::from_millis(1),
        },
    );
    let outcome = failing_pipeline
        .run_digest(CHAT_ID, 0)
        .await
        .expect("the run succeeds at the pipeline level")
        .expect("the tail is non-empty");
    match outcome {
        DigestOutcome::DeadLettered {
            new_boundary,
            error,
            ..
        } => {
            // The boundary advances: the failed batch is SKIPPED.
            assert_eq!(new_boundary, 3);
            assert!(error.contains("boom"), "the error is recorded: {error}");
        }
        other => panic!("expected DeadLettered, got {other:?}"),
    }
    // The dead_letter table holds the batch skeleton (Section 10.3
    // item 2); the skipped range stays in the raw log (item 3).
    let dead_letters = list_dead_letters(&fixture).await;
    assert_eq!(dead_letters.len(), 1);
    assert!(dead_letters[0]
        .batch_skeleton
        .contains("\"first_msg_id\":1"));
    assert!(dead_letters[0].batch_skeleton.contains("\"last_msg_id\":3"));
    assert!(dead_letters[0].error.contains("boom"));
    // The failure metric of specs.md Section 12: two attempts failed.
    assert_eq!(
        counter(&fixture, "digest_failures_total").await.as_deref(),
        Some("2")
    );

    // Batch 2 over the SAME store and backend still processes. The
    // later rows enter the raw log now; the skipped range stays (specs
    // Section 10.3 item 3).
    insert_messages(
        &fixture,
        3,
        &[
            ("u1", "Alice", "lucky. my run diverged twice"),
            ("u2", "Bob", "what learning rate?"),
            ("u1", "Alice", "3e-6 with warmup"),
        ],
    )
    .await;
    let good = Arc::new(ScriptedExtractor::with_graphs(vec![alice_grpo_graph()]));
    let good_pipeline = scripted_pipeline(&fixture, good, PipelineConfig::default());
    let outcome = good_pipeline
        .run_digest(CHAT_ID, 3)
        .await
        .expect("the run succeeds")
        .expect("the tail is non-empty");
    assert!(
        matches!(
            outcome,
            DigestOutcome::Extracted {
                new_boundary: 6,
                ..
            }
        ),
        "the later batch extracts, got {outcome:?}"
    );
    assert_eq!(
        count(
            &fixture,
            "MATCH (n:Node) WHERE n.type = 'MessageBatch' RETURN count(n)"
        )
        .await,
        1,
        "only the successful batch wrote a MessageBatch node"
    );
}

#[tokio::test]
async fn skeleton_batch_stores_only_the_skeleton() {
    // Section 7.2 rule 5 of the database spec: a batch of only emoji or
    // greetings skips the extraction call. The actor is out of this
    // test; the pipeline runs directly.
    let fixture = make_fixture();
    insert_messages(
        &fixture,
        0,
        &[
            ("u1", "Alice", "\u{1F44D}"),
            ("u2", "Bob", "hi"),
            ("u1", "Alice", "哈哈"),
        ],
    )
    .await;

    let extractor = Arc::new(ScriptedExtractor::with_graphs(vec![alice_grpo_graph()]));
    let pipeline = scripted_pipeline(&fixture, Arc::clone(&extractor), PipelineConfig::default());
    let outcome = pipeline
        .run_digest(CHAT_ID, 0)
        .await
        .expect("the run succeeds")
        .expect("the tail is non-empty");
    assert!(
        matches!(
            outcome,
            DigestOutcome::Skeleton {
                new_boundary: 3,
                ..
            }
        ),
        "the batch is a skeleton, got {outcome:?}"
    );

    assert_eq!(
        count(
            &fixture,
            "MATCH (n:Node) WHERE n.type = 'MessageBatch' RETURN count(n)"
        )
        .await,
        1
    );
    assert_eq!(
        count(&fixture, "MATCH (n:Node) RETURN count(n)").await,
        1,
        "no Person/Concept/Alias nodes are written for a skeleton batch"
    );
    assert!(
        extractor.inputs().is_empty(),
        "the extractor is never called for a skeleton batch"
    );
}

#[tokio::test]
async fn restart_keeps_the_digest_boundary() {
    // Phase 1 exit criterion: a restart loses no message and no digest
    // boundary.
    let fixture = make_fixture();
    let extractor = Arc::new(ScriptedExtractor::with_graphs(vec![alice_grpo_graph()]));
    let handle = spawn_with_digest(&fixture, digest_config(3), extractor);
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
    let boundary = wait_for_boundary(&handle, 3, DIGEST_TIMEOUT).await;
    assert_eq!(boundary, 3);
    let before = handle.snapshot().await.expect("the snapshot succeeds");
    assert!(
        before.last_digest_at.is_some(),
        "a completed digest records its wall-clock time"
    );
    handle.shutdown().await.expect("the actor reports no error");

    // A new actor on the same data root rebuilds the persisted state
    // (specs.md Section 6.1, rule 4). No scripted graphs: nothing should
    // digest after the restart (the tail is empty).
    let restarted = spawn_with_digest(
        &fixture,
        digest_config(3),
        Arc::new(ScriptedExtractor::with_graphs(vec![])),
    );
    let after = restarted.snapshot().await.expect("the snapshot succeeds");
    assert_eq!(
        after.last_digest_boundary_msg_id, before.last_digest_boundary_msg_id,
        "the digest boundary survives the restart"
    );
    assert_eq!(
        after.last_digest_at, before.last_digest_at,
        "the digest time survives the restart"
    );
    restarted
        .shutdown()
        .await
        .expect("the actor reports no error");
}
