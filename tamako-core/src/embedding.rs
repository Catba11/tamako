//! The embedding worker (current-state.md decision 66): ONE
//! process-wide background task that drains every group's
//! `pending_embeddings` queue, embeds the stored node content through
//! the embedding endpoint, and upserts the vec rows.
//!
//! Two mechanisms, one task:
//!
//! - **Startup reconciliation** ([`reconcile_group`]): runs once per
//!   group BEFORE the first drain tick. It diffs the graph's stored
//!   node contents against the store-side done-journal: backfill (a
//!   node never embedded), steady-state repair (the stored content
//!   drifted from the journaled hash), and tombstone cleanup (vec/queue
//!   rows whose node left the graph) are ONE pass.
//! - **The drain tick** ([`drain_group`]): every
//!   [`EMBEDDING_WORKER_INTERVAL`], at most [`EMBEDDING_BATCH_PER_GROUP`]
//!   rows per group, embedded SEQUENTIALLY (the rate limit at our
//!   scale).
//!
//! Why not a post-digest hook: the hook runs inline on the actor loop
//! and a 300-second endpoint timeout would stall the inbox (specs.md
//! Section 6.1 rule 3). The queue + interval task keeps an
//! embeddings-API outage away from the digest path entirely: the queue
//! grows, nothing blocks.
//!
//! The task is deliberately unsupervised: a panic kills only this
//! detached tokio task (tokio isolates task panics), the interval
//! simply stops, and every queue row stays inspectable store-side. No
//! supervisor machinery by design.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tamako_memory::{MemoryBackend, NodeContent};
use tamako_store::{Store, StoreError};
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::digest::embedding_content_hash;

/// The drain cadence of the worker (decision 66). One pass per group
/// per interval.
pub const EMBEDDING_WORKER_INTERVAL: Duration = Duration::from_secs(30);

/// The per-group claim limit of one drain tick (decision 66). Embed
/// calls run sequentially within the batch; the limit paces the
/// endpoint.
pub const EMBEDDING_BATCH_PER_GROUP: usize = 8;

/// Errors of the embedding worker. Row-level and group-level failures
/// are logged at WARN and skipped inside the pass functions; this type
/// is the carrier between the store/memory seams and those handlers.
#[derive(Debug, thiserror::Error)]
pub enum EmbeddingError {
    /// The embedding provider call failed (transport, provider, or a
    /// dimension mismatch at the seam).
    #[error("embedding provider failed: {0}")]
    Provider(String),
    /// A store call of the embedding sidecar failed.
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    /// A blocking store task failed to join.
    #[error("blocking store task failed to join: {0}")]
    Join(String),
}

/// The embedding seam of the worker. tamako-core cannot depend on
/// tamako-agent (AGENT.md Section 4: no dependency cycles), so — the
/// same contract-in-core pattern as [`crate::digest::DigestPipeline`] —
/// the worker defines its own seam here and the binary adapts
/// `tamako_agent::RigEmbeddingProvider` onto it. Object-safe (the
/// `Pin<Box>` convention of `DigestPipeline`).
pub trait EmbeddingProvider: Send + Sync {
    /// Embeds one text. The text layout is [`embedded_text`].
    fn embed<'a>(
        &'a self,
        text: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<f32>, EmbeddingError>> + Send + 'a>>;
}

/// The embedded text layout (decision 66): the node name, one newline,
/// then the description — `name ++ "\n" ++ description`, the SAME byte
/// layout that [`embedding_content_hash`] hashes, so the vector and its
/// change detector always describe the same content. The embed call is
/// made even when the description is empty: the name alone is
/// meaningful.
pub fn embedded_text(name: &str, description: &str) -> String {
    format!("{name}\n{description}")
}

/// Decision 66 embeds only Person/Alias/Concept nodes; MessageBatch
/// skeletons are never embedded. A node-id PREFIX filter cannot
/// implement this: identifiers.rs makes every node id an opaque UUID5
/// hash — the natural keys (`tg_user:…`, `alias:…`, `concept:…`,
/// `batch:…`) are the hash INPUT, so no prefix survives on the stored
/// id. The discriminator that DOES survive on the read path:
/// MessageBatch nodes carry their own id as the display name
/// (tamako-agent resolve.rs `message_batch_node` sets `name: batch_id`
/// on both the skeleton and the full-resolution write), while
/// Person/Alias/Concept names are human display names.
fn is_embedded_kind(node_id: &str, content: &NodeContent) -> bool {
    content.name != node_id
}

/// One group's embedding sidecar: the chat id plus the group's OWN
/// Store instance. The chat_id-less embedding helpers of [`Store`]
/// require exactly one open group per instance
/// ([`StoreError::AmbiguousGroup`] otherwise), so the worker holds one
/// dedicated Store per group.
pub struct GroupEmbeddingTarget {
    pub chat_id: String,
    pub store: Arc<Store>,
}

impl GroupEmbeddingTarget {
    /// Opens (creating when needed, Rule P5) the group store under
    /// `data_root` and wraps it as the group's embedding target.
    pub fn open(data_root: &Path, chat_id: &str) -> Result<Self, EmbeddingError> {
        let store = Arc::new(Store::new(data_root.to_path_buf()));
        store.open_group(chat_id)?;
        Ok(GroupEmbeddingTarget {
            chat_id: chat_id.to_string(),
            store,
        })
    }
}

/// The outcome of one [`reconcile_group`] pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Queue rows actually inserted (post-dedup).
    pub enqueued: usize,
    /// Orphan node ids tombstoned (vec + queue rows deleted).
    pub pruned_orphans: usize,
}

/// The outcome of one [`drain_group`] tick.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DrainReport {
    /// Queue rows claimed this tick.
    pub claimed: usize,
    /// Rows embedded and journaled.
    pub embedded: usize,
    /// Claimed rows whose node no longer exists (tombstoned, no embed
    /// call).
    pub gone: usize,
    /// Rows whose embed attempt failed (the attempt/failure discipline
    /// is store-side: `mark_embedding_attempt_failed`).
    pub failed: usize,
}

/// Runs one synchronous store call off the async runtime (AGENT.md
/// Section 6.2).
async fn store_call<T>(
    store: &Arc<Store>,
    call: impl FnOnce(&Store) -> Result<T, StoreError> + Send + 'static,
) -> Result<T, EmbeddingError>
where
    T: Send + 'static,
{
    let store = Arc::clone(store);
    tokio::task::spawn_blocking(move || call(&store))
        .await
        .map_err(|error| EmbeddingError::Join(error.to_string()))?
        .map_err(EmbeddingError::from)
}

/// The startup reconciliation of one group (decision 66): backfill,
/// steady-state repair, and tombstone cleanup in ONE pass.
///
/// - Every embedded-kind graph node whose current stored content hash
///   is not the node's latest journaled done hash is (re-)enqueued —
///   this covers the first-startup backfill AND the repair of a node
///   whose stored content drifted (alias merge, manual edit).
/// - Every node id known to the sidecar (vec rows UNION queue rows)
///   that the graph no longer holds is tombstoned.
///
/// Failures are WARN + skip: a group whose memory read fails keeps its
/// queue untouched and retries on the next process start. Logs one INFO
/// summary line per group.
pub async fn reconcile_group<M: MemoryBackend>(
    memory: &M,
    target: &GroupEmbeddingTarget,
) -> ReconcileReport {
    let mut report = ReconcileReport::default();
    let contents = match memory.list_node_contents(&target.chat_id).await {
        Ok(contents) => contents,
        Err(error) => {
            warn!(chat_id = %target.chat_id, %error, "embedding reconciliation: graph listing failed; skipping the group");
            return report;
        }
    };
    let graph_ids: HashSet<&str> = contents.iter().map(|(id, _)| id.as_str()).collect();
    let done: HashMap<String, String> = match store_call(
        &target.store,
        Store::done_embedding_hashes,
    )
    .await
    {
        Ok(done) => done.into_iter().collect(),
        Err(error) => {
            warn!(chat_id = %target.chat_id, %error, "embedding reconciliation: done-journal read failed; skipping the group");
            return report;
        }
    };
    let to_enqueue: Vec<(String, String)> = contents
        .iter()
        .filter(|(node_id, content)| is_embedded_kind(node_id, content))
        .map(|(node_id, content)| {
            (
                node_id.clone(),
                embedding_content_hash(&content.name, &content.description),
            )
        })
        .filter(|(node_id, hash)| done.get(node_id) != Some(hash))
        .collect();
    if !to_enqueue.is_empty() {
        let count = to_enqueue.len();
        match store_call(&target.store, move |store| {
            store.enqueue_embeddings(&to_enqueue)
        })
        .await
        {
            Ok(inserted) => report.enqueued = inserted,
            Err(error) => {
                warn!(chat_id = %target.chat_id, %error, count, "embedding reconciliation: enqueue failed")
            }
        }
    }
    // Orphan tombstones: sidecar node ids the graph no longer holds.
    let known = match store_call(&target.store, Store::all_embedding_node_ids).await {
        Ok(known) => known,
        Err(error) => {
            warn!(chat_id = %target.chat_id, %error, "embedding reconciliation: orphan scan failed; tombstones skipped");
            info!(chat_id = %target.chat_id, enqueued = report.enqueued, pruned_orphans = 0, "embedding reconciliation complete");
            return report;
        }
    };
    for node_id in known
        .into_iter()
        .filter(|node_id| !graph_ids.contains(node_id.as_str()))
    {
        let id = node_id.clone();
        match store_call(&target.store, move |store| {
            store.delete_node_embedding_rows(&id)
        })
        .await
        {
            Ok(()) => report.pruned_orphans += 1,
            Err(error) => {
                warn!(chat_id = %target.chat_id, node_id = %node_id, %error, "embedding reconciliation: orphan tombstone failed")
            }
        }
    }
    info!(chat_id = %target.chat_id, enqueued = report.enqueued, pruned_orphans = report.pruned_orphans, "embedding reconciliation complete");
    report
}

/// One drain tick of one group (decision 66): claims the oldest pending
/// rows (at most [`EMBEDDING_BATCH_PER_GROUP`]) and embeds them
/// SEQUENTIALLY.
///
/// Per row: the STORED node content is authoritative (pipeline-known
/// candidate values lose to the MERGE coalesce under alias drift, Rule
/// R4). A node that no longer exists is tombstoned (vec + queue rows)
/// and the claimed row is closed WITHOUT an embed call. A successful
/// embed upserts the vec row, closes the claimed row, and journals the
/// hash of the STORED content ([`Store::record_node_embedded`]) — the
/// journal records what was actually embedded, not the candidate hash
/// the row was claimed with. A failed embed records the attempt
/// store-side (the attempts cap and the 'failed' flip live in
/// [`Store::mark_embedding_attempt_failed`]) and the pass moves on.
///
/// Every store/memory failure is WARN + continue with the next row; a
/// claim failure skips the group for this tick. The pass never
/// propagates an error.
pub async fn drain_group<M: MemoryBackend>(
    provider: &dyn EmbeddingProvider,
    memory: &M,
    target: &GroupEmbeddingTarget,
) -> DrainReport {
    let mut report = DrainReport::default();
    let batch = match store_call(&target.store, |store| {
        store.claim_embedding_batch(EMBEDDING_BATCH_PER_GROUP)
    })
    .await
    {
        Ok(batch) => batch,
        Err(error) => {
            warn!(chat_id = %target.chat_id, %error, "embedding drain: claim failed; skipping the group this tick");
            return report;
        }
    };
    report.claimed = batch.len();
    for row in batch {
        let content = match memory.node_content(&target.chat_id, &row.node_id).await {
            Ok(content) => content,
            Err(error) => {
                warn!(chat_id = %target.chat_id, node_id = %row.node_id, %error, "embedding drain: node read failed; the row stays pending");
                continue;
            }
        };
        let Some(content) = content else {
            // The node is gone (merged away, deleted): tombstone every
            // trace of it, then close the claimed row. The tombstone
            // already removed the queue row itself, so the mark-done is
            // the belt-and-braces close for a same-tick re-enqueue.
            let node_id = row.node_id.clone();
            if let Err(error) = store_call(&target.store, move |store| {
                store.delete_node_embedding_rows(&node_id)
            })
            .await
            {
                warn!(chat_id = %target.chat_id, node_id = %row.node_id, %error, "embedding drain: tombstone of a gone node failed");
                continue;
            }
            if let Err(error) = store_call(&target.store, move |store| {
                store.mark_embedding_done(row.id)
            })
            .await
            {
                warn!(chat_id = %target.chat_id, node_id = %row.node_id, %error, "embedding drain: closing the row of a gone node failed");
            }
            report.gone += 1;
            continue;
        };
        // The documented text layout; embedded even when the
        // description is empty — the name alone is meaningful.
        let text = embedded_text(&content.name, &content.description);
        match provider.embed(&text).await {
            Ok(vector) => {
                let stored_hash = embedding_content_hash(&content.name, &content.description);
                let node_id = row.node_id.clone();
                let upsert = {
                    let node_id = node_id.clone();
                    store_call(&target.store, move |store| {
                        store.upsert_node_embedding(&node_id, &vector)
                    })
                    .await
                };
                if let Err(error) = upsert {
                    warn!(chat_id = %target.chat_id, node_id = %row.node_id, %error, "embedding drain: vec upsert failed; the row stays pending");
                    continue;
                }
                if let Err(error) = store_call(&target.store, move |store| {
                    store.mark_embedding_done(row.id)
                })
                .await
                {
                    warn!(chat_id = %target.chat_id, node_id = %row.node_id, %error, "embedding drain: mark-done failed after a successful embed");
                }
                let journal = {
                    let node_id = node_id.clone();
                    store_call(&target.store, move |store| {
                        store.record_node_embedded(&node_id, &stored_hash)
                    })
                    .await
                };
                if let Err(error) = journal {
                    warn!(chat_id = %target.chat_id, node_id = %row.node_id, %error, "embedding drain: done-journal write failed; the next reconciliation re-enqueues the node");
                }
                report.embedded += 1;
            }
            Err(error) => {
                warn!(chat_id = %target.chat_id, node_id = %row.node_id, attempt = row.attempts + 1, %error, "embedding attempt failed");
                if let Err(error) = store_call(&target.store, move |store| {
                    store.mark_embedding_attempt_failed(row.id)
                })
                .await
                {
                    warn!(chat_id = %target.chat_id, node_id = %row.node_id, %error, "embedding drain: attempt-failure record failed");
                }
                report.failed += 1;
            }
        }
    }
    report
}

/// The process-wide embedding worker (decision 66). Construct with the
/// resolved provider (or `None` when embeddings are degraded off), the
/// graph memory backend, and one [`GroupEmbeddingTarget`] per group;
/// [`EmbeddingWorker::spawn`] runs the task.
pub struct EmbeddingWorker<M: MemoryBackend> {
    provider: Option<Arc<dyn EmbeddingProvider>>,
    memory: Arc<M>,
    targets: Vec<GroupEmbeddingTarget>,
}

impl<M: MemoryBackend + 'static> EmbeddingWorker<M> {
    pub fn new(
        provider: Option<Arc<dyn EmbeddingProvider>>,
        memory: Arc<M>,
        targets: Vec<GroupEmbeddingTarget>,
    ) -> Self {
        EmbeddingWorker {
            provider,
            memory,
            targets,
        }
    }

    /// Spawns the worker task. A `None` provider (the degrade path: no
    /// API key) logs ONE info line and spawns NO task — a no-op task
    /// ticking forever would only pretend the sidecar is alive. The
    /// returned handle is detached: dropping it never cancels the task.
    ///
    /// The task body runs the per-group startup reconciliation ONCE,
    /// then ticks at [`EMBEDDING_WORKER_INTERVAL`] with
    /// `MissedTickBehavior::Delay` (a delayed tick loses at most
    /// cadence, the house idiom). A panic kills only this task; there
    /// is no supervisor machinery by design (module docs).
    pub fn spawn(self) -> Option<tokio::task::JoinHandle<()>> {
        let Some(provider) = self.provider else {
            info!("embeddings disabled: no embedding provider; the embedding worker will not run");
            return None;
        };
        let memory = self.memory;
        let targets = self.targets;
        Some(tokio::spawn(async move {
            // Startup reconciliation BEFORE the first drain tick
            // (decision 66): backfill, steady-state repair, and
            // tombstone cleanup are one mechanism.
            for target in &targets {
                reconcile_group(&*memory, target).await;
            }
            let mut ticker = tokio::time::interval(EMBEDDING_WORKER_INTERVAL);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            // The first interval tick fires immediately; consume it so
            // the first drain runs one full interval after the
            // reconciliation pass.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                for target in &targets {
                    drain_group(&*provider, &*memory, target).await;
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use tamako_memory::{AliasTarget, MemoryBatch, NeighborEdge, Result as MemoryResult};
    use tamako_store::EMBEDDING_DIM;

    use super::*;

    /// The scripted provider: records every embedded text and serves
    /// the queued results, falling back to `default` when the queue is
    /// empty.
    struct ScriptedProvider {
        calls: Mutex<Vec<String>>,
        results: Mutex<VecDeque<Result<Vec<f32>, EmbeddingError>>>,
        fail_by_default: bool,
    }

    impl ScriptedProvider {
        fn succeeding() -> Self {
            ScriptedProvider {
                calls: Mutex::new(Vec::new()),
                results: Mutex::new(VecDeque::new()),
                fail_by_default: false,
            }
        }

        fn failing() -> Self {
            ScriptedProvider {
                fail_by_default: true,
                ..Self::succeeding()
            }
        }

        fn texts(&self) -> Vec<String> {
            self.calls.lock().expect("calls lock").clone()
        }

        fn fallback(&self) -> Result<Vec<f32>, EmbeddingError> {
            if self.fail_by_default {
                Err(EmbeddingError::Provider("boom".to_string()))
            } else {
                Ok(vec![1.0; EMBEDDING_DIM])
            }
        }
    }

    impl EmbeddingProvider for ScriptedProvider {
        fn embed<'a>(
            &'a self,
            text: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<f32>, EmbeddingError>> + Send + 'a>> {
            self.calls
                .lock()
                .expect("calls lock")
                .push(text.to_string());
            let result = self
                .results
                .lock()
                .expect("results lock")
                .pop_front()
                .unwrap_or_else(|| self.fallback());
            Box::pin(async move { result })
        }
    }

    /// The scripted memory backend: serves seeded node contents; every
    /// write path is a no-op. (NoopMemory's defaults return empty
    /// listings; the drain and reconciliation tests need seeded reads.)
    #[derive(Default)]
    struct ScriptedMemory {
        /// chat_id -> node_id -> stored content.
        contents: Mutex<HashMap<String, HashMap<String, NodeContent>>>,
    }

    impl ScriptedMemory {
        fn with_group(chat_id: &str, nodes: &[(&str, NodeContent)]) -> Self {
            let nodes: HashMap<String, NodeContent> = nodes
                .iter()
                .map(|(id, content)| ((*id).to_string(), content.clone()))
                .collect();
            ScriptedMemory {
                contents: Mutex::new(HashMap::from([(chat_id.to_string(), nodes)])),
            }
        }
    }

    impl MemoryBackend for ScriptedMemory {
        async fn ensure_schema(&self, _chat_id: &str) -> MemoryResult<()> {
            Ok(())
        }

        async fn upsert_batch(&self, _chat_id: &str, _batch: &MemoryBatch) -> MemoryResult<()> {
            Ok(())
        }

        async fn checkpoint(&self, _chat_id: &str) -> MemoryResult<()> {
            Ok(())
        }

        async fn alias_targets(
            &self,
            _chat_id: &str,
            _alias_node_id: &str,
        ) -> MemoryResult<Vec<AliasTarget>> {
            Ok(Vec::new())
        }

        async fn neighbors(
            &self,
            _chat_id: &str,
            _node_id: &str,
        ) -> MemoryResult<Vec<NeighborEdge>> {
            Ok(Vec::new())
        }

        async fn node_content(
            &self,
            chat_id: &str,
            node_id: &str,
        ) -> MemoryResult<Option<NodeContent>> {
            Ok(self
                .contents
                .lock()
                .expect("contents lock")
                .get(chat_id)
                .and_then(|group| group.get(node_id))
                .cloned())
        }

        async fn list_node_contents(
            &self,
            chat_id: &str,
        ) -> MemoryResult<Vec<(String, NodeContent)>> {
            let mut contents: Vec<(String, NodeContent)> = self
                .contents
                .lock()
                .expect("contents lock")
                .get(chat_id)
                .map(|group| {
                    group
                        .iter()
                        .map(|(id, content)| (id.clone(), content.clone()))
                        .collect()
                })
                .unwrap_or_default();
            contents.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(contents)
        }

        async fn close(&self, _chat_id: &str) -> MemoryResult<()> {
            Ok(())
        }
    }

    fn content(name: &str, description: &str) -> NodeContent {
        NodeContent {
            name: name.to_string(),
            description: description.to_string(),
        }
    }

    /// Opens group "chat_a" of a fresh tempdir as the embedding target.
    fn test_target() -> (tempfile::TempDir, GroupEmbeddingTarget) {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = GroupEmbeddingTarget::open(dir.path(), "chat_a").expect("open group");
        (dir, target)
    }

    #[test]
    fn the_embedded_text_layout_is_name_newline_description() {
        // The layout is the SAME byte sequence embedding_content_hash
        // hashes; the hash's pinned-vector test in digest.rs locks the
        // pair together.
        assert_eq!(embedded_text("Tama", "a cat"), "Tama\na cat");
        // An empty description still embeds: the name alone is
        // meaningful.
        assert_eq!(embedded_text("Tama", ""), "Tama\n");
    }

    #[test]
    fn the_message_batch_filter_uses_the_name_equals_id_invariant() {
        // resolve.rs `message_batch_node` writes name == id for
        // MessageBatch nodes (on both write paths); Person/Alias/
        // Concept names are human display names. The node ids are
        // opaque UUID5 hashes, so no prefix filter can exist.
        let batch = content("9b2f…", "");
        assert!(!is_embedded_kind("9b2f…", &batch));
        let person = content("Tama", "a cat");
        assert!(is_embedded_kind("9b2f…", &person));
    }

    #[tokio::test]
    async fn the_drain_embeds_claimed_rows_and_journals_the_stored_hash() {
        let (_dir, target) = test_target();
        let memory = ScriptedMemory::with_group("chat_a", &[("n1", content("Tama", "a cat"))]);
        // The claimed row carries a pipeline-known CANDIDATE hash that
        // has drifted from the stored content (alias merge).
        target
            .store
            .enqueue_embeddings(&[("n1".to_string(), "candidate-hash".to_string())])
            .expect("enqueue");
        let provider = ScriptedProvider::succeeding();

        let report = drain_group(&provider, &memory, &target).await;

        assert_eq!(
            report,
            DrainReport {
                claimed: 1,
                embedded: 1,
                gone: 0,
                failed: 0,
            }
        );
        // The embed call saw the documented text layout.
        assert_eq!(provider.texts(), vec!["Tama\na cat".to_string()]);
        // The vec row was upserted: KNN returns it at distance ~0.
        let hits = target
            .store
            .knn_node_embeddings(&vec![1.0; EMBEDDING_DIM], 1)
            .expect("knn");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, "n1");
        assert!(hits[0].1.abs() < 1e-6);
        // The claimed row is done and leaves the claim set.
        assert!(target
            .store
            .claim_embedding_batch(EMBEDDING_BATCH_PER_GROUP)
            .expect("claim")
            .is_empty());
        // The done-journal carries the STORED hash, not the candidate
        // hash the row was claimed with.
        let stored = embedding_content_hash("Tama", "a cat");
        assert_ne!(stored, "candidate-hash");
        assert_eq!(
            target.store.done_embedding_hashes().expect("done hashes"),
            vec![("n1".to_string(), stored)]
        );
    }

    #[tokio::test]
    async fn the_drain_tombstones_rows_whose_node_is_gone() {
        let (_dir, target) = test_target();
        // The graph holds no "ghost" node: ScriptedMemory serves None.
        let memory = ScriptedMemory::default();
        target
            .store
            .enqueue_embeddings(&[("ghost".to_string(), "h".to_string())])
            .expect("enqueue");
        let provider = ScriptedProvider::succeeding();

        let report = drain_group(&provider, &memory, &target).await;

        assert_eq!(report.gone, 1);
        assert_eq!(report.embedded, 0);
        assert!(provider.texts().is_empty(), "no embed call for a gone node");
        // Vec + queue rows are deleted; the claim set is empty.
        assert!(target
            .store
            .all_embedding_node_ids()
            .expect("node ids")
            .is_empty());
        assert!(target
            .store
            .claim_embedding_batch(EMBEDDING_BATCH_PER_GROUP)
            .expect("claim")
            .is_empty());
    }

    #[tokio::test]
    async fn three_failed_attempts_flip_the_row_to_failed_and_stop_claims() {
        let (_dir, target) = test_target();
        let memory = ScriptedMemory::with_group("chat_a", &[("n1", content("Tama", "a cat"))]);
        target
            .store
            .enqueue_embeddings(&[("n1".to_string(), "h".to_string())])
            .expect("enqueue");
        let provider = ScriptedProvider::failing();

        for round in 1..=3 {
            let report = drain_group(&provider, &memory, &target).await;
            assert_eq!(report.failed, 1, "round {round}");
        }

        assert_eq!(provider.texts().len(), 3, "one embed call per attempt");
        // The row flipped to 'failed' at the cap and left the claim
        // set, but stays inspectable (the id is still known to the
        // sidecar through the queue row).
        assert!(target
            .store
            .claim_embedding_batch(EMBEDDING_BATCH_PER_GROUP)
            .expect("claim")
            .is_empty());
        assert_eq!(
            target.store.all_embedding_node_ids().expect("node ids"),
            vec!["n1".to_string()]
        );
        // Nothing was embedded or journaled.
        assert!(target
            .store
            .done_embedding_hashes()
            .expect("done hashes")
            .is_empty());
    }

    #[tokio::test]
    async fn reconciliation_backfills_nodes_missing_from_the_done_journal() {
        let (_dir, target) = test_target();
        let memory = ScriptedMemory::with_group(
            "chat_a",
            &[
                ("p1", content("Tama", "a cat")),
                ("c1", content("Rust", "a language")),
                // The MessageBatch skeleton: name == id (the resolve.rs
                // invariant the filter relies on).
                ("b1", content("b1", "")),
            ],
        );

        let report = reconcile_group(&memory, &target).await;

        assert_eq!(report.enqueued, 2);
        assert_eq!(report.pruned_orphans, 0);
        let queued: HashSet<(String, String)> = target
            .store
            .claim_embedding_batch(100)
            .expect("claim")
            .into_iter()
            .map(|row| (row.node_id, row.content_hash))
            .collect();
        assert_eq!(
            queued,
            HashSet::from([
                ("p1".to_string(), embedding_content_hash("Tama", "a cat")),
                (
                    "c1".to_string(),
                    embedding_content_hash("Rust", "a language")
                ),
            ]),
            "the MessageBatch skeleton is never enqueued"
        );
    }

    #[tokio::test]
    async fn reconciliation_reenqueues_a_node_whose_stored_content_drifted() {
        let (_dir, target) = test_target();
        let memory = ScriptedMemory::with_group(
            "chat_a",
            &[
                ("p1", content("Tama", "a cat")),
                ("c1", content("Rust", "a language")),
            ],
        );
        // c1's journal is current; p1's journal holds a STALE hash
        // (the stored content drifted since the embed).
        target
            .store
            .record_node_embedded("c1", &embedding_content_hash("Rust", "a language"))
            .expect("journal c1");
        target
            .store
            .record_node_embedded("p1", "stale-hash")
            .expect("journal p1");

        let report = reconcile_group(&memory, &target).await;

        assert_eq!(report.enqueued, 1);
        let claimed = target.store.claim_embedding_batch(100).expect("claim");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].node_id, "p1");
        assert_eq!(
            claimed[0].content_hash,
            embedding_content_hash("Tama", "a cat"),
            "re-enqueued at the CURRENT stored hash"
        );
    }

    #[tokio::test]
    async fn reconciliation_tombstones_orphans_and_keeps_current_nodes_queued_out() {
        let (_dir, target) = test_target();
        let memory = ScriptedMemory::with_group(
            "chat_a",
            &[("p1", content("Tama", "a cat")), ("b1", content("b1", ""))],
        );
        // An orphan: vec + queue rows for a node the graph no longer
        // holds.
        target
            .store
            .upsert_node_embedding("orphan", &vec![1.0; EMBEDDING_DIM])
            .expect("upsert orphan");
        target
            .store
            .enqueue_embeddings(&[("orphan".to_string(), "h".to_string())])
            .expect("enqueue orphan");
        // p1's journal is current: no re-enqueue.
        target
            .store
            .record_node_embedded("p1", &embedding_content_hash("Tama", "a cat"))
            .expect("journal p1");

        let report = reconcile_group(&memory, &target).await;

        assert_eq!(report.enqueued, 0);
        assert_eq!(report.pruned_orphans, 1);
        assert_eq!(
            target.store.all_embedding_node_ids().expect("node ids"),
            vec!["p1".to_string()],
            "the orphan's vec and queue rows are gone; p1's journal row remains"
        );
    }

    #[tokio::test]
    async fn spawn_without_a_provider_logs_and_spawns_nothing() {
        let worker: EmbeddingWorker<ScriptedMemory> =
            EmbeddingWorker::new(None, Arc::new(ScriptedMemory::default()), Vec::new());
        assert!(worker.spawn().is_none());
    }
}
