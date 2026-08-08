//! Regression test: concurrent read and write on one group database.
//!
//! lbug 0.18 `Send + Sync` does not make a read concurrent with a write
//! safe (crash signature: SIGSEGV in the storage layer). `LbugBackend`
//! serializes all per-group operations, reads and CHECKPOINT included
//! (proposed-graph-database-specs.md Section 6.1 rule 3,
//! docs/adr-0001-ladybugdb-binding.md addendum 2026-08-08). This test
//! runs a growing write loop against a read loop on the fixed backend
//! and asserts a clean completion. The backend-internal serialization
//! makes the pass deterministic.

use std::sync::Arc;
use std::time::Duration;

use tamako_memory::{
    identifiers, LbugBackend, MemoryBackend, MemoryBatch, MemoryEdge, MemoryNode, NodeType,
};
use time::macros::datetime;

const RUN_DURATION: Duration = Duration::from_secs(10);

/// One growing batch: the hub node plus fresh concept nodes and edges.
/// Each edge has a distinct `valid_at`, so every batch appends and
/// allocates pages. Same shape as the race repro of 2026-08-08.
fn growing_batch(iteration: u64) -> (MemoryBatch, String) {
    let base = datetime!(2026-08-07 10:00 UTC);
    let hub = MemoryNode {
        id: identifiers::concept_id("ConcurrentHub"),
        name: "ConcurrentHub".to_string(),
        node_type: NodeType::Concept,
        created_at: base,
        updated_at: base,
        properties: None,
    };
    let mut nodes = vec![hub.clone()];
    let mut edges = Vec::new();
    for i in 0..20u64 {
        let seq = iteration * 20 + i;
        let target = MemoryNode {
            id: identifiers::concept_id(&format!("ConcurrentTarget{seq:06}")),
            name: format!("ConcurrentTarget{seq:06}"),
            node_type: NodeType::Concept,
            created_at: base,
            updated_at: base,
            properties: None,
        };
        let valid_at = base + time::Duration::seconds(seq as i64);
        edges.push(MemoryEdge {
            source_id: hub.id.clone(),
            target_id: target.id.clone(),
            relationship_name: "mentions".to_string(),
            valid_at,
            invalid_at: None,
            edge_text: format!("ConcurrentHub mentions ConcurrentTarget{seq:06}"),
            created_at: valid_at,
            updated_at: valid_at,
            properties: None,
        });
        nodes.push(target);
    }
    let batch = MemoryBatch {
        batch_id: identifiers::batch_id(8000, iteration as i64),
        nodes,
        edges,
    };
    (batch, hub.id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_read_and_write_on_one_group_is_safe() {
    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(LbugBackend::new(dir.path()));
    let start = std::time::Instant::now();

    let (seed_batch, hub_id) = growing_batch(0);
    backend
        .upsert_batch("concurrent_chat", &seed_batch)
        .await
        .unwrap();
    let alias_id = identifiers::alias_id("concurrent_alias");

    let writer = {
        let backend = backend.clone();
        tokio::spawn(async move {
            let mut iteration = 1u64;
            let mut writes = 0u64;
            while start.elapsed() < RUN_DURATION {
                let (batch, _) = growing_batch(iteration);
                iteration += 1;
                backend
                    .upsert_batch("concurrent_chat", &batch)
                    .await
                    .unwrap();
                writes += 1;
            }
            writes
        })
    };

    let reader = {
        let backend = backend.clone();
        tokio::spawn(async move {
            let mut iteration = 0u64;
            let mut reads = 0u64;
            while start.elapsed() < RUN_DURATION {
                iteration += 1;
                if iteration.is_multiple_of(16) {
                    let _ = backend
                        .alias_targets("concurrent_chat", &alias_id)
                        .await
                        .unwrap();
                } else {
                    let _ = backend.neighbors("concurrent_chat", &hub_id).await.unwrap();
                }
                reads += 1;
            }
            reads
        })
    };

    let writes = writer.await.unwrap();
    let reads = reader.await.unwrap();
    assert!(writes > 0, "the write loop must complete batches");
    assert!(reads > 0, "the read loop must complete queries");
}
