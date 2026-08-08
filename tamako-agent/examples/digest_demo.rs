//! The offline digest demo (Phase 1, M1). Runs without an API key:
//!
//! ```text
//! cargo run -p tamako-agent --example digest_demo
//! ```
//!
//! The demo replays the shipped chat log (`replay_chat.json`) through
//! the real actor with the real digest pipeline over a scripted
//! extractor (no LLM call), then prints the resulting graph through the
//! real LadybugDB backend. The data directory stays behind under the
//! system temp dir for inspection.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tamako_adapter_mock::MockAdapter;
use tamako_agent::{
    AgentDigestPipeline, ExtractedEdge, ExtractedNode, ExtractedNodeType, KnowledgeGraph,
    PipelineConfig, ScriptedExtractor,
};
use tamako_core::actor::{
    spawn_group_actor, GroupActorHandle, GroupActorParams, DEFAULT_INBOX_CAPACITY,
};
use tamako_core::adapter::PlatformAdapter;
use tamako_core::config::TriggerConfig;
use tamako_memory::LbugBackend;
use tamako_store::Store;

/// The shipped replay fixture (14 events: 10 messages plus joins,
/// leaves, a reaction, and an edit).
const FIXTURE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../tamako-adapter-mock/fixtures/replay_chat.json"
);

/// The poll interval of the digest wait loop.
const POLL_INTERVAL: Duration = Duration::from_millis(20);
/// The bounded wait for the digest chain. The scripted extractor returns
/// immediately; ten seconds is generous.
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);
/// The quiet period that ends the wait: the boundary did not move for
/// this long, so the digest chain drained (specs.md Section 6.2: one
/// digest at a time per group; each completion re-evaluates the tail).
const QUIET_PERIOD: Duration = Duration::from_millis(200);

/// One plausible extraction of the fixture chat: Alice, Bob, and Carol
/// plan a Saturday hike; Carol wants to bring her cat.
fn fixture_graph() -> KnowledgeGraph {
    KnowledgeGraph {
        nodes: vec![
            ExtractedNode {
                name: "Alice".to_string(),
                node_type: ExtractedNodeType::Person,
                description: "Alice proposed a hike this weekend.".to_string(),
            },
            ExtractedNode {
                name: "Bob".to_string(),
                node_type: ExtractedNodeType::Person,
                description: "Bob joined the hike and checked the forecast.".to_string(),
            },
            ExtractedNode {
                name: "Carol".to_string(),
                node_type: ExtractedNodeType::Person,
                description: "Carol wants to bring her adventure cat.".to_string(),
            },
            ExtractedNode {
                name: "Saturday hike".to_string(),
                node_type: ExtractedNodeType::Concept,
                description: "The group settled on Saturday, 9 am, cat included.".to_string(),
            },
        ],
        edges: vec![
            ExtractedEdge {
                source: "Alice".to_string(),
                target: "Saturday hike".to_string(),
                relationship_name: "proposed".to_string(),
                description: "Alice proposed the hike.".to_string(),
            },
            ExtractedEdge {
                source: "Bob".to_string(),
                target: "Saturday hike".to_string(),
                relationship_name: "joined".to_string(),
                description: "Bob said he is in.".to_string(),
            },
            ExtractedEdge {
                source: "Carol".to_string(),
                target: "Saturday hike".to_string(),
                relationship_name: "joined".to_string(),
                description: "Carol joined and brings her cat.".to_string(),
            },
        ],
    }
}

/// Waits until the digest chain drained: the boundary stops moving for
/// QUIET_PERIOD. Bounded by WAIT_TIMEOUT.
async fn wait_for_digests(handle: &GroupActorHandle) -> i64 {
    let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
    let mut last_boundary = -1_i64;
    let mut last_change = std::time::Instant::now();
    loop {
        let session = handle.snapshot().await.expect("the snapshot succeeds");
        let boundary = session.last_digest_boundary_msg_id;
        if boundary != last_boundary {
            last_boundary = boundary;
            last_change = std::time::Instant::now();
        } else if boundary > 0 && last_change.elapsed() >= QUIET_PERIOD {
            return boundary;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {WAIT_TIMEOUT:?} waiting for a digest (boundary: {boundary})"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[tokio::main]
async fn main() {
    // A fixed data directory under the system temp dir. Cleaned at the
    // start so every run replays from scratch; left behind at the end
    // so the graph files can be inspected.
    let data_root: PathBuf = std::env::temp_dir().join("tamako-digest-demo");
    if data_root.exists() {
        std::fs::remove_dir_all(&data_root).expect("the old demo directory is removable");
    }
    std::fs::create_dir_all(&data_root).expect("the demo directory is creatable");
    println!("data directory: {}", data_root.display());

    let mut adapter =
        MockAdapter::from_fixture_path(std::path::Path::new(FIXTURE_PATH)).expect("fixture loads");
    let chat_id = adapter.chat_id().to_string();
    println!("replaying {FIXTURE_PATH} into chat {chat_id}");

    let store = Arc::new(Store::new(data_root.clone()));
    let memory = Arc::new(LbugBackend::new(data_root.clone()));
    let extractor = Arc::new(ScriptedExtractor::with_graphs(vec![fixture_graph()]));
    let pipeline = Arc::new(AgentDigestPipeline::new(
        Arc::clone(&store),
        Arc::clone(&memory),
        extractor,
        PipelineConfig::default(),
    ));
    // A low message threshold so the trigger fires during the demo
    // (specs.md Section 8.2).
    let config = TriggerConfig {
        digest_max_messages: 4,
        ..TriggerConfig::default()
    };
    let handle = spawn_group_actor(GroupActorParams {
        chat_id: chat_id.clone(),
        store: Arc::clone(&store),
        memory: Arc::clone(&memory),
        config,
        started_at: time::OffsetDateTime::now_utc(),
        inbox_capacity: DEFAULT_INBOX_CAPACITY,
        // A small static preamble; the demo has no persona file. Rule C4:
        // it seeds item 0 of the live context.
        preamble: "You are Tamako, a test pet.\n".to_string(),
        digest: Some(pipeline),
        post_digest_hook: None,
    });

    let mut events = 0_usize;
    while let Some(event) = adapter
        .next_event()
        .await
        .expect("the mock adapter never fails")
    {
        handle
            .send_event(event)
            .await
            .expect("the actor inbox is open");
        events += 1;
    }
    // The snapshot is a FIFO barrier: every event is processed.
    handle.snapshot().await.expect("the snapshot succeeds");
    let boundary = wait_for_digests(&handle).await;
    handle.shutdown().await.expect("the actor reports no error");

    // The summary, read back through the real backend.
    let dead_letters = store
        .list_dead_letters(&chat_id)
        .expect("list_dead_letters succeeds");
    let node_counts = memory
        .query_rows(
            &chat_id,
            "MATCH (n:Node) RETURN n.type, count(n) ORDER BY n.type",
        )
        .await
        .expect("the node count query succeeds");
    let edges = memory
        .query_rows(
            &chat_id,
            "MATCH (s:Node)-[r:EDGE]->(t:Node) \
             RETURN s.name, r.relationship_name, t.name ORDER BY r.relationship_name",
        )
        .await
        .expect("the edge query succeeds");

    println!("\nDigest summary");
    println!("  events replayed:        {events}");
    println!("  digest boundary msg id: {boundary}");
    println!("  dead letters:           {}", dead_letters.len());
    println!("  nodes by type:");
    for row in &node_counts {
        println!("    {:<12} {}", row[0], row[1]);
    }
    println!("  edges:");
    for row in &edges {
        println!("    {} -[{}]-> {}", row[0], row[1], row[2]);
    }
    println!(
        "\ninspect the graph at {}",
        data_root.join(&chat_id).display()
    );
}
