//! Integration tests of the digest pipeline: real `Store` and real
//! `LbugBackend` over a tempfile, scripted extractors. specs.md
//! Section 10.

use std::sync::Arc;
use std::time::Duration;

use tamako_agent::{
    AgentDigestPipeline, BindingSource, ExtractedEdge, ExtractedNode, ExtractedNodeType,
    KnowledgeGraph, PipelineConfig, ScriptedExtractor,
};
use tamako_core::digest::{DigestOutcome, DigestPipeline};
use tamako_memory::{identifiers, LbugBackend};
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
