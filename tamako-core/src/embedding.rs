//! The embedding worker (current-state.md decision 66): ONE
//! process-wide background task that drains every group's
//! `pending_embeddings` queue, embeds the stored node content through
//! the embedding endpoint, and upserts the vec rows.
//!
//! Two mechanisms, one task:
//!
//! - **Reconciliation** ([`reconcile_group`]): runs once per group
//!   BEFORE the first drain tick, then periodically every
//!   [`RECONCILE_EVERY_N_TICKS`] ticks (decision 77, M4). It diffs the
//!   graph's stored node contents against the store-side done-journal:
//!   backfill (a node never embedded), steady-state repair (the stored
//!   content drifted from the journaled hash), and tombstone cleanup
//!   (vec/queue rows whose node left the graph) are ONE pass. Decision
//!   76 extends the same pass to the `edge_texts` sidecar as a pure set
//!   difference against the graph's edges (no journal on that side).
//!   Reconciliation needs NO provider — the edge_texts repair is a pure
//!   store/graph diff — so it runs even when embeddings are disabled.
//! - **The drain tick** ([`drain_group`]): every
//!   [`EMBEDDING_WORKER_INTERVAL`], at most [`EMBEDDING_BATCH_PER_GROUP`]
//!   rows per group (or [`EMBEDDING_BURST_BATCH`] while the backlog
//!   exceeds [`EMBEDDING_BURST_THRESHOLD`], decision 77 M13), embedded
//!   with the bounded concurrency of [`EmbeddingWorker::new`]'s
//!   `embedding_concurrency` (decision 113: concurrent single-text
//!   POSTs, never array input). A `None` provider skips the drain loop
//!   only; reconciliation still runs.
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

use tamako_memory::{MemoryBackend, NodeType};
use tamako_store::{Store, StoreError};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::digest::embedding_content_hash;

/// The drain cadence of the worker (decision 66). One pass per group
/// per interval.
pub const EMBEDDING_WORKER_INTERVAL: Duration = Duration::from_secs(30);

/// The per-group claim limit of one drain tick (decision 66). Embed
/// calls within the batch run with the configured bounded concurrency
/// (decision 113, [`embed_texts_bounded`]); the limit paces the
/// endpoint.
pub const EMBEDDING_BATCH_PER_GROUP: usize = 8;

/// The periodic reconciliation cadence (decision 77, M4): every Nth
/// tick re-runs [`reconcile_group`] — 40 × the 30 s
/// [`EMBEDDING_WORKER_INTERVAL`] ≈ 20 min. The edge_texts sidecar
/// repair is a pure store/graph diff, so this cadence runs even with
/// NO provider (the drain loop alone is provider-gated).
pub const RECONCILE_EVERY_N_TICKS: u32 = 40;

/// The backlog-burst threshold (decision 77, M13): when a group's
/// pending embedding queue EXCEEDS this many rows, a tick claims
/// [`EMBEDDING_BURST_BATCH`] instead of [`EMBEDDING_BATCH_PER_GROUP`]
/// until the backlog drains.
pub const EMBEDDING_BURST_THRESHOLD: usize = 256;

/// The per-group claim limit of a burst tick (decision 77, M13). Refer
/// to [`EMBEDDING_BURST_THRESHOLD`].
pub const EMBEDDING_BURST_BATCH: usize = 64;

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

    /// Embeds a batch of texts, one vector per input in INPUT ORDER.
    /// Decision 113: batching means BOUNDED-CONCURRENT SINGLE-TEXT
    /// posts, never array input (the decision-81 addendum: the ZDR
    /// route serves single-text only, array input 404s) — the binary's
    /// adapter overrides this with [`embed_texts_bounded`]; the
    /// DEFAULT implementation loops [`embed`] sequentially, the
    /// correct fallback for any provider (the existing worker and its
    /// test doubles stay source-compatible). The dimension pin (3072,
    /// decision 81) applies per element — the `embed` implementations
    /// enforce it, and the default loop inherits that enforcement.
    #[allow(clippy::type_complexity)]
    fn embed_texts<'a>(
        &'a self,
        texts: &'a [String],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Vec<f32>>, EmbeddingError>> + Send + 'a>> {
        Box::pin(async move {
            let mut vectors = Vec::with_capacity(texts.len());
            for text in texts {
                vectors.push(self.embed(text).await?);
            }
            Ok(vectors)
        })
    }
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
/// skeletons are never embedded. Decision 77 (M10) discriminates on the
/// STORED node `type` (the closed set of graph-spec Section 6.2, read
/// batched through [`MemoryBackend::node_resolution_infos`]) — the old
/// discriminator, the MessageBatch name-equals-id invariant, was only a
/// PROXY and misclassified a real node whose display name collides with
/// its id. A node MISSING from the resolution read (a stored type
/// string outside the closed set — the read paths skip such rows) is
/// embedded: the closed set carries exactly one non-embedded kind, and
/// the drain re-reads the stored content anyway.
fn is_embedded_kind(kind: Option<NodeType>) -> bool {
    kind != Some(NodeType::MessageBatch)
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
    /// edge_texts rows (re-)written for graph edges missing from the
    /// sidecar (decision 76, Section 7.6 step 6).
    pub edge_texts_upserted: usize,
    /// Orphan edge_texts rows deleted (sidecar edge ids the graph no
    /// longer holds — incl. merge-tombstoned nodes' edges and the
    /// pre-repoint edge ids a merge leaves behind).
    pub edge_texts_pruned: usize,
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

/// The reconciliation pass of one group (decision 66 startup, decision
/// 77 M4 periodic): backfill, steady-state repair, and tombstone
/// cleanup in ONE pass.
///
/// - Every embedded-kind graph node whose current stored content hash
///   is not the node's latest journaled done hash is (re-)enqueued —
///   this covers the first-startup backfill AND the repair of a node
///   whose stored content drifted (alias merge, manual edit). The
///   embedded-kind filter reads the stored TYPE column (decision 77,
///   M10): one batched [`MemoryBackend::node_resolution_infos`] call
///   over the listed ids. Without a provider the enqueued rows simply
///   wait store-side for a later drain — the pass is harmless and runs
///   anyway, so a provider appearing later finds the queue primed.
/// - Every node id known to the sidecar (vec rows UNION queue rows)
///   that the graph no longer holds is tombstoned.
/// - Decision 76 (Section 7.6 step 6): the `edge_texts` sidecar rides
///   the same mechanism as a pure SET DIFFERENCE (no done-journal on
///   this side): the graph truth is `list_all_edges` (ALL edges, valid
///   and invalid); a graph edge missing from the sidecar is (re-)
///   written, and a sidecar id the graph no longer holds is pruned
///   (orphans, incl. merge-tombstoned nodes' edges). The journal-free
///   diff is what restores a merge ROLLBACK's recreated edges
///   automatically: the rollback re-creates the loser and its edges,
///   and their rows reappear here regardless of any node journal.
///
/// Failures are WARN + skip: a group whose memory read fails keeps its
/// queue untouched and retries on the next pass. Logs one DEBUG
/// summary line per group (decision 77, S2: at the M4 periodic cadence
/// the line is RECURRING, and the curated INFO surface of decision 53
/// is frozen to the one-line-per-wake/digest set — a periodic line
/// outside that set would extend it, so the summary is DEBUG).
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
    // Decision 77 (M10): the embedded-kind filter reads the stored type
    // column — ONE batched resolution-info call over the listed ids.
    let node_ids: Vec<String> = contents.iter().map(|(id, _)| id.clone()).collect();
    let kinds: HashMap<String, NodeType> = match memory
        .node_resolution_infos(&target.chat_id, &node_ids)
        .await
    {
        Ok(infos) => infos
            .into_iter()
            .map(|(id, info)| (id, info.kind))
            .collect(),
        Err(error) => {
            warn!(chat_id = %target.chat_id, %error, "embedding reconciliation: kind read failed; skipping the group");
            return report;
        }
    };
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
        .filter(|(node_id, _)| is_embedded_kind(kinds.get(node_id).copied()))
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
            debug!(chat_id = %target.chat_id, enqueued = report.enqueued, pruned_orphans = 0, edge_texts_upserted = 0, edge_texts_pruned = 0, "embedding reconciliation complete");
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
    // Decision 76 (Section 7.6 step 6): the edge_texts sidecar diff —
    // the same set-difference mechanism as the node pass above, minus
    // the done-journal (see the doc comment). Every failure is WARN +
    // skip, like the node passes.
    let graph_edges = match memory.list_all_edges(&target.chat_id).await {
        Ok(edges) => edges,
        Err(error) => {
            warn!(chat_id = %target.chat_id, %error, "embedding reconciliation: graph edge listing failed; edge_texts repair skipped");
            debug!(chat_id = %target.chat_id, enqueued = report.enqueued, pruned_orphans = report.pruned_orphans, edge_texts_upserted = 0, edge_texts_pruned = 0, "embedding reconciliation complete");
            return report;
        }
    };
    let sidecar_texts: HashMap<String, String> = match store_call(
        &target.store,
        Store::list_edge_texts,
    )
    .await
    {
        Ok(pairs) => pairs.into_iter().collect(),
        Err(error) => {
            warn!(chat_id = %target.chat_id, %error, "embedding reconciliation: edge_texts scan failed; edge repair skipped");
            debug!(chat_id = %target.chat_id, enqueued = report.enqueued, pruned_orphans = report.pruned_orphans, edge_texts_upserted = 0, edge_texts_pruned = 0, "embedding reconciliation complete");
            return report;
        }
    };
    let graph_edge_ids: HashSet<&str> = graph_edges
        .iter()
        .map(|(edge_id, _)| edge_id.as_str())
        .collect();
    for (edge_id, edge_text) in &graph_edges {
        // Decision 77 (S6-F6): the diff is BY CONTENT — upsert when the
        // sidecar row is missing OR its text DRIFTED from the graph's
        // edge_text (a re-extracted edge with a new description repairs
        // the sidecar here; the id-presence-only diff could not). The
        // digest harvest skips empty descriptions (nothing to search);
        // the diff mirrors that skip so an empty-text edge never flaps
        // between the two writers.
        if edge_text.is_empty()
            || sidecar_texts
                .get(edge_id)
                .is_some_and(|text| text == edge_text)
        {
            continue;
        }
        let id = edge_id.clone();
        let text = edge_text.clone();
        match store_call(&target.store, move |store| {
            store.upsert_edge_text(&id, &text)
        })
        .await
        {
            Ok(()) => report.edge_texts_upserted += 1,
            Err(error) => {
                warn!(chat_id = %target.chat_id, edge_id = %edge_id, %error, "embedding reconciliation: edge_texts upsert failed")
            }
        }
    }
    let orphans: Vec<String> = sidecar_texts
        .into_keys()
        .filter(|edge_id| !graph_edge_ids.contains(edge_id.as_str()))
        .collect();
    if !orphans.is_empty() {
        let count = orphans.len();
        match store_call(&target.store, move |store| {
            store.delete_edge_texts(&orphans)
        })
        .await
        {
            Ok(deleted) => report.edge_texts_pruned = deleted,
            Err(error) => {
                warn!(chat_id = %target.chat_id, %error, count, "embedding reconciliation: edge_texts orphan prune failed")
            }
        }
    }
    debug!(chat_id = %target.chat_id, enqueued = report.enqueued, pruned_orphans = report.pruned_orphans, edge_texts_upserted = report.edge_texts_upserted, edge_texts_pruned = report.edge_texts_pruned, "embedding reconciliation complete");
    report
}

/// Embeds every text with at most `concurrency` calls in flight
/// (decision 113): concurrent SINGLE-TEXT posts — never array input,
/// so the decision-81 ZDR route discipline is unchanged. Returns one
/// entry per input text, in input order; every text is attempted even
/// after a failure (the per-item [`Result`] carries the error), so a
/// caller keeps exact per-item attribution. `concurrency = 0` behaves
/// as 1; `concurrency = 1` is the decision-66 sequential behavior.
#[allow(clippy::type_complexity)]
pub fn embed_texts_bounded<'a>(
    provider: &'a dyn EmbeddingProvider,
    texts: &'a [String],
    concurrency: usize,
) -> Pin<Box<dyn Future<Output = Vec<Result<Vec<f32>, EmbeddingError>>> + Send + 'a>> {
    // The explicit `+ Send` box is deliberate: as an `async fn` the
    // Send proof of the boxed per-text futures leaks into every
    // caller's generic future and rustc's higher-ranked check fails
    // ("Send is not general enough") at the worker's tokio::spawn.
    // Boxing here pins the proof to this definition site.
    Box::pin(async move {
        use futures::StreamExt;
        if texts.is_empty() {
            return Vec::new();
        }
        // The explicit loop is deliberate: a `map` closure returning
        // the boxed per-text future makes rustc demand an
        // inexpressible higher-ranked closure signature ("FnOnce is
        // not general enough"); the loop keeps every lifetime
        // concrete at 'a.
        let mut calls = Vec::with_capacity(texts.len());
        for text in texts {
            calls.push(provider.embed(text));
        }
        futures::stream::iter(calls)
            .buffered(concurrency.max(1))
            .collect()
            .await
    })
}

/// One drain tick of one group (decision 66): claims the oldest pending
/// rows (at most [`EMBEDDING_BATCH_PER_GROUP`], or
/// [`EMBEDDING_BURST_BATCH`] while the backlog exceeds
/// [`EMBEDDING_BURST_THRESHOLD`] — decision 77, M13) and embeds them
/// with the bounded concurrency of `embedding_concurrency` (decision
/// 113; 1 is the decision-66 sequential behavior).
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
    embedding_concurrency: usize,
) -> DrainReport {
    let mut report = DrainReport::default();
    // Backlog-burst probe (decision 77, M13): `claim_embedding_batch`
    // is a pure SELECT — the store keeps no claim state — so ONE probe
    // claim of `EMBEDDING_BURST_THRESHOLD + 1` rows doubles as the
    // pending-count read. A FULL probe means the pending backlog exceeds
    // the threshold and the tick drains EMBEDDING_BURST_BATCH rows; a
    // short probe drains the normal EMBEDDING_BATCH_PER_GROUP. The probe
    // rows past the batch stay 'pending' and are re-read next tick.
    let probe = match store_call(&target.store, |store| {
        store.claim_embedding_batch(EMBEDDING_BURST_THRESHOLD + 1)
    })
    .await
    {
        Ok(probe) => probe,
        Err(error) => {
            warn!(chat_id = %target.chat_id, %error, "embedding drain: claim failed; skipping the group this tick");
            return report;
        }
    };
    let batch_size = if probe.len() > EMBEDDING_BURST_THRESHOLD {
        EMBEDDING_BURST_BATCH
    } else {
        EMBEDDING_BATCH_PER_GROUP
    };
    let batch: Vec<_> = probe.into_iter().take(batch_size).collect();
    report.claimed = batch.len();
    // Phase A (decision 113): read every claimed row's content
    // sequentially. A read failure leaves the row pending; a gone
    // node is tombstoned and closed exactly as the decision-66 loop
    // did. Survivors carry their content into the embed phase.
    let mut survivors = Vec::new();
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
        survivors.push((row, content));
    }
    // Phase B: one bounded-concurrent pass over the survivors' texts
    // (decision 113). Concurrent single-text POSTs — never array
    // input, so the decision-81 ZDR route discipline is unchanged.
    // Every text is attempted even after a failure, so phase C keeps
    // exact per-row attribution.
    let texts: Vec<String> = survivors
        .iter()
        .map(|(_, content)| embedded_text(&content.name, &content.description))
        .collect();
    let vectors = embed_texts_bounded(provider, &texts, embedding_concurrency).await;
    debug_assert_eq!(
        vectors.len(),
        survivors.len(),
        "embed_texts_bounded returns one entry per input text"
    );
    // Phase C: the per-row store writes stay sequential, exactly the
    // decision-66 order (upsert → mark-done → journal).
    for ((row, content), result) in survivors.into_iter().zip(vectors) {
        match result {
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
    /// The drain tick's embed-phase bound (decision 113,
    /// [`embed_texts_bounded`]); 1 is the decision-66 sequential
    /// behavior.
    embedding_concurrency: usize,
}

impl<M: MemoryBackend + 'static> EmbeddingWorker<M> {
    pub fn new(
        provider: Option<Arc<dyn EmbeddingProvider>>,
        memory: Arc<M>,
        targets: Vec<GroupEmbeddingTarget>,
        embedding_concurrency: usize,
    ) -> Self {
        EmbeddingWorker {
            provider,
            memory,
            targets,
            embedding_concurrency,
        }
    }

    /// Spawns the worker task. The task ALWAYS spawns: a `None`
    /// provider (the degrade path: no API key, or
    /// `embedding_enabled = false`) logs ONE info line and skips only
    /// the DRAIN loop — reconciliation needs no embeddings (the
    /// edge_texts repair is a pure store/graph diff) and still runs at
    /// startup and on the [`RECONCILE_EVERY_N_TICKS`] cadence
    /// (decision 77, M4). The `Option` return is kept for the
    /// decision-66 call-site shape; it is now always `Some`. The
    /// returned handle is detached: dropping it never cancels the task.
    ///
    /// The task body runs the per-group reconciliation ONCE at startup,
    /// then ticks at [`EMBEDDING_WORKER_INTERVAL`] with
    /// `MissedTickBehavior::Delay` (a delayed tick loses at most
    /// cadence, the house idiom). Every [`RECONCILE_EVERY_N_TICKS`]th
    /// tick re-runs the reconciliation. A panic kills only this task;
    /// there is no supervisor machinery by design (module docs).
    pub fn spawn(self) -> Option<tokio::task::JoinHandle<()>> {
        let provider = self.provider;
        if provider.is_none() {
            info!("embeddings disabled: no embedding provider; the drain loop is off, but the reconciliation pass still runs (startup + periodic)");
        }
        let memory = self.memory;
        let targets = self.targets;
        let embedding_concurrency = self.embedding_concurrency;
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
            // Ticks since the startup reconciliation; every Nth tick
            // re-runs it (decision 77, M4).
            let mut ticks: u32 = 0;
            loop {
                ticker.tick().await;
                ticks += 1;
                if let Some(provider) = &provider {
                    for target in &targets {
                        drain_group(&**provider, &*memory, target, embedding_concurrency).await;
                    }
                }
                if ticks.is_multiple_of(RECONCILE_EVERY_N_TICKS) {
                    for target in &targets {
                        reconcile_group(&*memory, target).await;
                    }
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use tamako_memory::{
        AliasTarget, MemoryBatch, NeighborEdge, NodeContent, NodeResolutionInfo,
        Result as MemoryResult,
    };
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
        /// chat_id -> node_id -> stored kind — the type-column truth of
        /// the decision-77 (M10) embedded-kind filter, served through
        /// `node_resolution_infos`.
        kinds: Mutex<HashMap<String, HashMap<String, NodeType>>>,
        /// chat_id -> (edge_id, edge_text) of the group's edges — the
        /// graph truth of the decision-76 edge_texts diff.
        edges: Mutex<HashMap<String, Vec<(String, String)>>>,
    }

    impl ScriptedMemory {
        fn with_group(chat_id: &str, nodes: &[(&str, NodeContent)]) -> Self {
            let nodes: HashMap<String, NodeContent> = nodes
                .iter()
                .map(|(id, content)| ((*id).to_string(), content.clone()))
                .collect();
            ScriptedMemory {
                contents: Mutex::new(HashMap::from([(chat_id.to_string(), nodes)])),
                ..ScriptedMemory::default()
            }
        }

        /// Seeds the group's stored kinds (decision 77, M10 tests).
        fn with_kinds(self, chat_id: &str, kinds: &[(&str, NodeType)]) -> Self {
            self.kinds.lock().expect("kinds lock").insert(
                chat_id.to_string(),
                kinds
                    .iter()
                    .map(|(id, kind)| ((*id).to_string(), *kind))
                    .collect(),
            );
            self
        }

        /// Adds (or replaces) one node post-construction — the paused-
        /// time periodic-reconciliation test seeds a node AFTER the
        /// startup pass.
        fn add_node(&self, chat_id: &str, node_id: &str, content: NodeContent, kind: NodeType) {
            self.contents
                .lock()
                .expect("contents lock")
                .entry(chat_id.to_string())
                .or_default()
                .insert(node_id.to_string(), content);
            self.kinds
                .lock()
                .expect("kinds lock")
                .entry(chat_id.to_string())
                .or_default()
                .insert(node_id.to_string(), kind);
        }

        /// Seeds the group's edge listing (decision 76 tests).
        fn with_edges(self, chat_id: &str, edges: &[(&str, &str)]) -> Self {
            self.edges.lock().expect("edges lock").insert(
                chat_id.to_string(),
                edges
                    .iter()
                    .map(|(id, text)| ((*id).to_string(), (*text).to_string()))
                    .collect(),
            );
            self
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

        async fn list_all_edges(&self, chat_id: &str) -> MemoryResult<Vec<(String, String)>> {
            Ok(self
                .edges
                .lock()
                .expect("edges lock")
                .get(chat_id)
                .cloned()
                .unwrap_or_default())
        }

        async fn node_resolution_infos(
            &self,
            chat_id: &str,
            node_ids: &[String],
        ) -> MemoryResult<Vec<(String, NodeResolutionInfo)>> {
            let kinds = self.kinds.lock().expect("kinds lock");
            let Some(group) = kinds.get(chat_id) else {
                return Ok(Vec::new());
            };
            Ok(node_ids
                .iter()
                .filter_map(|id| {
                    group.get(id).map(|kind| {
                        (
                            id.clone(),
                            NodeResolutionInfo {
                                kind: *kind,
                                alias_target: None,
                                alias_target_count: 0,
                            },
                        )
                    })
                })
                .collect())
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
    fn the_embedded_kind_filter_discriminates_on_the_stored_type() {
        // Decision 77 (M10): the discriminator is the stored `type`
        // column, not the name-equals-id proxy. Only MessageBatch is
        // excluded.
        assert!(!is_embedded_kind(Some(NodeType::MessageBatch)));
        assert!(is_embedded_kind(Some(NodeType::Person)));
        assert!(is_embedded_kind(Some(NodeType::Alias)));
        assert!(is_embedded_kind(Some(NodeType::Concept)));
        // A node missing from the resolution read (a stored type
        // outside the closed set) is embedded: the closed set carries
        // exactly one non-embedded kind.
        assert!(is_embedded_kind(None));
    }

    #[tokio::test]
    async fn the_default_batch_impl_composes_the_single_embed_calls() {
        let provider = ScriptedProvider::succeeding();
        let texts: Vec<String> = ["a", "b", "c"].iter().map(|s| (*s).to_string()).collect();

        let vectors = provider.embed_texts(&texts).await.expect("batch");

        // N sequential single calls compose the batch result, in input
        // order; the scripted fallback vector carries through.
        assert_eq!(provider.texts(), texts);
        assert_eq!(vectors, vec![vec![1.0; EMBEDDING_DIM]; 3]);
    }

    #[tokio::test]
    async fn the_default_batch_impl_propagates_the_first_single_failure() {
        let provider = ScriptedProvider::failing();
        let texts: Vec<String> = ["a", "b"].iter().map(|s| (*s).to_string()).collect();

        match provider.embed_texts(&texts).await {
            Err(EmbeddingError::Provider(message)) => assert_eq!(message, "boom"),
            other => panic!("expected a Provider error, got {other:?}"),
        }
        // The loop short-circuits: the first failure stops the batch.
        assert_eq!(provider.texts().len(), 1);
    }

    #[tokio::test]
    async fn the_default_batch_impl_returns_empty_for_empty_input() {
        let provider = ScriptedProvider::succeeding();
        let vectors = provider.embed_texts(&[]).await.expect("empty batch");
        assert!(vectors.is_empty());
        assert!(
            provider.texts().is_empty(),
            "no single calls for an empty batch"
        );
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

        let report = drain_group(&provider, &memory, &target, 4).await;

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

        let report = drain_group(&provider, &memory, &target, 4).await;

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
            let report = drain_group(&provider, &memory, &target, 4).await;
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
    async fn the_drain_attributes_per_row_results_across_survivors() {
        // Decision 113: three survivors, the mid one's embed fails —
        // the phase-split zips results back to rows in input order, so
        // exactly n2 records the failed attempt while n1/n3 embed and
        // journal (exercises the survivors↔vectors alignment no
        // single-row test can reach).
        let (_dir, target) = test_target();
        let memory = ScriptedMemory::with_group(
            "chat_a",
            &[
                ("n1", content("Alpha", "first")),
                ("n2", content("Beta", "second")),
                ("n3", content("Gamma", "third")),
            ],
        );
        target
            .store
            .enqueue_embeddings(&[
                ("n1".to_string(), "h1".to_string()),
                ("n2".to_string(), "h2".to_string()),
                ("n3".to_string(), "h3".to_string()),
            ])
            .expect("enqueue");
        let provider = ScriptedProvider {
            results: Mutex::new(VecDeque::from([
                Ok(vec![1.0; EMBEDDING_DIM]),
                Err(EmbeddingError::Provider("boom".to_string())),
                Ok(vec![1.0; EMBEDDING_DIM]),
            ])),
            ..ScriptedProvider::succeeding()
        };

        let report = drain_group(&provider, &memory, &target, 4).await;

        assert_eq!(
            report,
            DrainReport {
                claimed: 3,
                embedded: 2,
                gone: 0,
                failed: 1,
            }
        );
        // n1 and n3 journaled with their STORED hashes; n2 is not.
        assert_eq!(
            target.store.done_embedding_hashes().expect("done hashes"),
            vec![
                ("n1".to_string(), embedding_content_hash("Alpha", "first")),
                ("n3".to_string(), embedding_content_hash("Gamma", "third")),
            ]
        );
        // n2's row stays claimable with one attempt recorded; the
        // other two rows closed.
        let pending = target
            .store
            .claim_embedding_batch(EMBEDDING_BATCH_PER_GROUP)
            .expect("claim");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].node_id, "n2");
        assert_eq!(pending[0].attempts, 1);
    }

    #[tokio::test]
    async fn reconciliation_backfills_nodes_missing_from_the_done_journal() {
        let (_dir, target) = test_target();
        let memory = ScriptedMemory::with_group(
            "chat_a",
            &[
                ("p1", content("Tama", "a cat")),
                ("c1", content("Rust", "a language")),
                // The MessageBatch skeleton: excluded by the stored
                // type column (decision 77, M10).
                ("b1", content("b1", "")),
            ],
        )
        .with_kinds(
            "chat_a",
            &[
                ("p1", NodeType::Person),
                ("c1", NodeType::Concept),
                ("b1", NodeType::MessageBatch),
            ],
        );

        let report = reconcile_group(&memory, &target).await;

        assert_eq!(report.enqueued, 2);
        assert_eq!(report.pruned_orphans, 0);
        assert_eq!(report.edge_texts_upserted, 0);
        assert_eq!(report.edge_texts_pruned, 0);
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
        )
        .with_kinds(
            "chat_a",
            &[("p1", NodeType::Person), ("c1", NodeType::Concept)],
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
        )
        .with_kinds(
            "chat_a",
            &[("p1", NodeType::Person), ("b1", NodeType::MessageBatch)],
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
    async fn reconciliation_repairs_the_edge_texts_sidecar() {
        // Decision 76/77: the reconcile pass diffs edge_texts against
        // the graph's edges BY CONTENT — a graph edge missing from the
        // sidecar is upserted (id + the graph's edge_text), a sidecar
        // row the graph no longer holds (a merge tombstone) is pruned,
        // and an empty-text edge is never written (the digest harvest
        // skip, mirrored so the two writers never flap).
        let (_dir, target) = test_target();
        let memory = ScriptedMemory::with_group("chat_a", &[]).with_edges(
            "chat_a",
            &[("e1", "Alice discussed coffee with Bob"), ("e2", "")],
        );
        // A stale orphan row: its edge left the graph.
        target
            .store
            .upsert_edge_text("e-orphan", "a tombstoned edge")
            .expect("seed orphan");

        let report = reconcile_group(&memory, &target).await;

        assert_eq!(report.edge_texts_upserted, 1);
        assert_eq!(report.edge_texts_pruned, 1);
        // The node side of the report is untouched by the edge pass.
        assert_eq!(report.enqueued, 0);
        assert_eq!(report.pruned_orphans, 0);
        assert_eq!(
            target.store.list_edge_text_ids().expect("ids"),
            vec!["e1".to_string()],
            "e1 written, the orphan pruned; the empty-text edge e2 is never written"
        );
        assert_eq!(
            target.store.search_edge_texts("coffee").expect("search"),
            vec!["e1".to_string()],
            "the upserted row carries the graph's edge_text"
        );
    }

    #[tokio::test]
    async fn reconciliation_leaves_a_converged_edge_texts_sidecar_alone() {
        // Steady state: sidecar == graph. A second pass writes and
        // prunes nothing (the diff is empty both ways).
        let (_dir, target) = test_target();
        let memory = ScriptedMemory::with_group("chat_a", &[])
            .with_edges("chat_a", &[("e1", "Alice discussed coffee with Bob")]);
        target
            .store
            .upsert_edge_text("e1", "Alice discussed coffee with Bob")
            .expect("seed row");

        let report = reconcile_group(&memory, &target).await;

        assert_eq!(report.edge_texts_upserted, 0);
        assert_eq!(report.edge_texts_pruned, 0);
        assert_eq!(
            target.store.list_edge_text_ids().expect("ids"),
            vec!["e1".to_string()]
        );
    }

    #[tokio::test]
    async fn reconciliation_repairs_a_drifted_edge_text_row() {
        // Decision 77 (S6-F6): a sidecar row whose text DRIFTED from
        // the graph's edge_text (the edge was re-extracted with a new
        // description) is corrected by the content diff — the
        // id-presence-only diff would have missed it, because the id
        // was already present.
        let (_dir, target) = test_target();
        let memory = ScriptedMemory::with_group("chat_a", &[])
            .with_edges("chat_a", &[("e1", "Alice discussed tea with Bob")]);
        target
            .store
            .upsert_edge_text("e1", "Alice discussed coffee with Bob")
            .expect("seed stale row");

        let report = reconcile_group(&memory, &target).await;

        assert_eq!(report.edge_texts_upserted, 1);
        assert_eq!(report.edge_texts_pruned, 0);
        assert_eq!(
            target.store.list_edge_texts().expect("pairs"),
            vec![("e1".to_string(), "Alice discussed tea with Bob".to_string())],
            "the stale text is replaced with the graph's edge_text"
        );
        assert!(
            target
                .store
                .search_edge_texts("coffee")
                .expect("search")
                .is_empty(),
            "the drifted text no longer matches"
        );
    }

    #[tokio::test]
    async fn reconciliation_embeds_a_person_whose_name_equals_its_id() {
        // Decision 77 (M10): the filter reads the stored TYPE, so a
        // Person whose display name collides with its node id IS
        // enqueued (the old name==id proxy would have dropped it),
        // while a MessageBatch skeleton is still excluded.
        let (_dir, target) = test_target();
        let memory = ScriptedMemory::with_group(
            "chat_a",
            &[
                ("n1", content("n1", "a person named like its own id")),
                ("b1", content("b1", "")),
            ],
        )
        .with_kinds(
            "chat_a",
            &[("n1", NodeType::Person), ("b1", NodeType::MessageBatch)],
        );

        let report = reconcile_group(&memory, &target).await;

        assert_eq!(report.enqueued, 1);
        let claimed = target.store.claim_embedding_batch(100).expect("claim");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].node_id, "n1");
        assert_eq!(
            claimed[0].content_hash,
            embedding_content_hash("n1", "a person named like its own id")
        );
    }

    #[tokio::test]
    async fn a_backlog_above_the_burst_threshold_drains_the_burst_batch() {
        // Decision 77 (M13): 300 pending rows exceed
        // EMBEDDING_BURST_THRESHOLD (256), so the tick claims
        // EMBEDDING_BURST_BATCH (64) instead of EMBEDDING_BATCH_PER_GROUP.
        let (_dir, target) = test_target();
        // The graph holds none of the seeded nodes: every claimed row
        // takes the gone-node tombstone path, so the test exercises the
        // claim limit without any embed call.
        let memory = ScriptedMemory::default();
        let rows: Vec<(String, String)> = (0..300)
            .map(|i| (format!("n{i}"), format!("h{i}")))
            .collect();
        target.store.enqueue_embeddings(&rows).expect("enqueue");
        let provider = ScriptedProvider::succeeding();

        let report = drain_group(&provider, &memory, &target, 4).await;

        assert_eq!(report.claimed, EMBEDDING_BURST_BATCH);
        assert!(provider.texts().is_empty(), "gone nodes make no embed call");
    }

    #[tokio::test]
    async fn a_backlog_below_the_burst_threshold_drains_the_normal_batch() {
        // Decision 77 (M13): 10 pending rows stay under the threshold,
        // so the tick claims the normal EMBEDDING_BATCH_PER_GROUP (8).
        let (_dir, target) = test_target();
        let memory = ScriptedMemory::default();
        let rows: Vec<(String, String)> = (0..10)
            .map(|i| (format!("n{i}"), format!("h{i}")))
            .collect();
        target.store.enqueue_embeddings(&rows).expect("enqueue");
        let provider = ScriptedProvider::succeeding();

        let report = drain_group(&provider, &memory, &target, 4).await;

        assert_eq!(report.claimed, EMBEDDING_BATCH_PER_GROUP);
    }

    #[tokio::test]
    async fn spawn_without_a_provider_still_runs_the_startup_reconciliation() {
        // Decision 77 (M4): the worker task spawns even with NO
        // provider — reconciliation (the edge_texts repair is a pure
        // store/graph diff) needs no embeddings; only the drain loop is
        // skipped.
        let (_dir, target) = test_target();
        let memory = ScriptedMemory::with_group("chat_a", &[("p1", content("Tama", "a cat"))])
            .with_kinds("chat_a", &[("p1", NodeType::Person)]);
        let store = Arc::clone(&target.store);
        let worker = EmbeddingWorker::new(None, Arc::new(memory), vec![target], 1);

        let handle = worker
            .spawn()
            .expect("the task spawns even without a provider");

        // The startup reconciliation enqueues p1 without any provider.
        let mut enqueued = false;
        for _ in 0..200 {
            if !store
                .claim_embedding_batch(EMBEDDING_BATCH_PER_GROUP)
                .expect("claim")
                .is_empty()
            {
                enqueued = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        handle.abort();
        assert!(enqueued, "the startup reconciliation ran provider-less");
    }

    #[tokio::test(start_paused = true)]
    async fn the_periodic_reconciliation_runs_every_n_ticks_without_a_provider() {
        // Decision 77 (M4): every RECONCILE_EVERY_N_TICKSth tick re-runs
        // the reconciliation, provider or not. Paused time drives the
        // 30 s interval instantly.
        let (_dir, target) = test_target();
        // Two markers bracket the STARTUP pass: p0's enqueue is its
        // first mutation, the prune of the seeded e-orphan sidecar row
        // its LAST. Waiting for both guarantees the worker finished the
        // pass and parks on the ticker BEFORE the test advances the
        // clock — ticks of a not-yet-created ticker would be lost.
        let memory = Arc::new(
            ScriptedMemory::with_group("chat_a", &[("p0", content("Mochi", "a cat"))])
                .with_kinds("chat_a", &[("p0", NodeType::Person)]),
        );
        let store = Arc::clone(&target.store);
        store
            .upsert_edge_text("e-orphan", "gone")
            .expect("seed orphan");
        let worker = EmbeddingWorker::new(None, Arc::clone(&memory), vec![target], 1);
        let handle = worker.spawn().expect("spawn");
        let claimed = || {
            store
                .claim_embedding_batch(100)
                .expect("claim")
                .into_iter()
                .map(|row| row.node_id)
                .collect::<HashSet<_>>()
        };
        for _ in 0..10_000 {
            if claimed().contains("p0") && store.list_edge_text_ids().expect("ids").is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            claimed().contains("p0") && store.list_edge_text_ids().expect("ids").is_empty(),
            "the startup pass ran to completion"
        );
        // Let the worker consume the immediate first tick and park on
        // the ticker (synchronous code only from here).
        for _ in 0..1_000 {
            tokio::task::yield_now().await;
        }

        // A node that appears AFTER the startup pass: only the periodic
        // reconciliation can pick it up (no provider → no drain).
        memory.add_node("chat_a", "p1", content("Tama", "a cat"), NodeType::Person);

        // Ticks 1..N-1: no reconciliation.
        for _ in 0..RECONCILE_EVERY_N_TICKS - 1 {
            tokio::time::advance(EMBEDDING_WORKER_INTERVAL).await;
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
        }
        assert!(
            !claimed().contains("p1"),
            "no periodic reconciliation before the Nth tick"
        );

        // The Nth tick reconciles and enqueues the new node.
        tokio::time::advance(EMBEDDING_WORKER_INTERVAL).await;
        for _ in 0..10_000 {
            if claimed().contains("p1") {
                break;
            }
            tokio::task::yield_now().await;
        }
        handle.abort();
        assert!(
            claimed().contains("p1"),
            "the Nth tick re-ran the reconciliation"
        );
    }

    /// The bounded-helper probe (decision 113): records every text at
    /// call time, tracks the in-flight overlap, optionally delays each
    /// text "tN" by (5-N)×10 ms (later inputs resolve earlier), and
    /// optionally fails one text.
    struct BoundedProbe {
        calls: Mutex<Vec<String>>,
        in_flight: std::sync::atomic::AtomicUsize,
        peak: std::sync::atomic::AtomicUsize,
        descending_delays: bool,
        fail_text: Option<String>,
    }

    impl BoundedProbe {
        fn new() -> Self {
            BoundedProbe {
                calls: Mutex::new(Vec::new()),
                in_flight: std::sync::atomic::AtomicUsize::new(0),
                peak: std::sync::atomic::AtomicUsize::new(0),
                descending_delays: false,
                fail_text: None,
            }
        }

        fn peak(&self) -> usize {
            self.peak.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl EmbeddingProvider for BoundedProbe {
        fn embed<'a>(
            &'a self,
            text: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<f32>, EmbeddingError>> + Send + 'a>> {
            use std::sync::atomic::Ordering;
            self.calls.lock().expect("calls").push(text.to_string());
            let text = text.to_string();
            Box::pin(async move {
                let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(now, Ordering::SeqCst);
                if self.descending_delays {
                    let index = text
                        .strip_prefix('t')
                        .expect("shape")
                        .parse::<usize>()
                        .expect("index");
                    tokio::time::sleep(Duration::from_millis((5 - index) as u64 * 10)).await;
                } else {
                    tokio::task::yield_now().await;
                }
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                if Some(text.as_str()) == self.fail_text.as_deref() {
                    Err(EmbeddingError::Provider("boom".to_string()))
                } else {
                    let marker = text
                        .strip_prefix('t')
                        .and_then(|rest| rest.parse::<usize>().ok())
                        .map(|index| index as f32)
                        .unwrap_or(0.0);
                    Ok(vec![marker])
                }
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_bounded_helper_preserves_input_order_under_concurrency() {
        // Later inputs resolve EARLIER (descending sleep): the result
        // vector must still align with the input order.
        let provider = BoundedProbe {
            descending_delays: true,
            ..BoundedProbe::new()
        };
        let texts: Vec<String> = (0..5).map(|i| format!("t{i}")).collect();
        let results = embed_texts_bounded(&provider, &texts, 3).await;
        let markers: Vec<f32> = results
            .into_iter()
            .map(|result| result.expect("all succeed")[0])
            .collect();
        assert_eq!(markers, vec![0.0, 1.0, 2.0, 3.0, 4.0]);
    }

    #[tokio::test]
    async fn the_bounded_helper_respects_the_bound_and_overlaps() {
        let provider = BoundedProbe::new();
        let texts: Vec<String> = (0..6).map(|i| format!("t{i}")).collect();
        let results = embed_texts_bounded(&provider, &texts, 2).await;
        assert!(results.iter().all(|result| result.is_ok()));
        assert_eq!(provider.peak(), 2, "the bound is the ceiling AND reached");
    }

    #[tokio::test]
    async fn the_bounded_helper_attempts_every_text_after_a_failure() {
        let provider = BoundedProbe {
            fail_text: Some("bad".to_string()),
            ..BoundedProbe::new()
        };
        let texts: Vec<String> = vec!["t0".to_string(), "bad".to_string(), "t2".to_string()];
        let results = embed_texts_bounded(&provider, &texts, 3).await;
        assert_eq!(
            provider.calls.lock().expect("calls").len(),
            3,
            "every text attempted"
        );
        assert!(results[0].is_ok());
        assert!(results[1].is_err(), "the failure lands at ITS position");
        assert!(results[2].is_ok());
    }

    #[tokio::test]
    async fn the_bounded_helper_with_concurrency_one_never_overlaps() {
        let provider = BoundedProbe::new();
        let texts: Vec<String> = (0..4).map(|i| format!("t{i}")).collect();
        let results = embed_texts_bounded(&provider, &texts, 1).await;
        assert!(results.iter().all(|result| result.is_ok()));
        assert_eq!(
            provider.peak(),
            1,
            "concurrency 1 is the sequential behavior"
        );
    }

    #[tokio::test]
    async fn the_bounded_helper_embeds_nothing_for_empty_input() {
        let provider = BoundedProbe::new();
        let results = embed_texts_bounded(&provider, &[], 4).await;
        assert!(results.is_empty());
        assert!(provider.calls.lock().expect("calls").is_empty());
    }
}
