//! The M1 digest pipeline. specs.md Section 10: input (10.1),
//! extraction and write (10.2), failure handling (10.3). The pipeline
//! implements `tamako_core::digest::DigestPipeline`; the actor drives it
//! through `Arc<dyn DigestPipeline>`.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tamako_core::actor::CoreError;
use tamako_core::digest::{embedding_content_hash, DigestOutcome, DigestPipeline};
use tamako_core::embedding::EmbeddingProvider as CoreEmbeddingProvider;
use tamako_memory::identifiers::{batch_id as message_batch_id, normalize};
use tamako_memory::{EdgeId, MemoryBackend, MemoryBatch, NodeType};
use tamako_store::{MessageRow, Store, StoreError};
use time::macros::format_description;
use time::{OffsetDateTime, UtcOffset};

use crate::extract::{
    AgentError, BatchMessage, BindingSource, ExtractionInput, KnowledgeExtractor, MentionBinding,
};
use crate::graph::KnowledgeGraph;
use crate::resolve::{
    message_batch_node, resolve_batch, ResolutionConfirmer, VectorPrescreen,
    VectorResolutionConfig, VectorResolutionStats,
};
use crate::skeleton::is_skeleton_batch;
use crate::validate::validate_relationship_name;

/// The UTC HH:MM rendering of the speaker label. Section 7.2 step 4 of
/// the database spec.
const HHMM_FORMAT: &[time::format_description::FormatItem<'_>] =
    format_description!("[hour]:[minute]");

/// The cap of the exponential backoff. specs.md Section 10.3.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);

/// Retry/dead-letter configuration. specs.md Section 10.3.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// The TOTAL number of attempts of one batch, including the first
    /// one (specs.md Section 13: default 5). After this many failures
    /// the batch is dead-lettered.
    pub max_retries: u32,
    /// Base of the exponential backoff. The delay before retry n is
    /// base * 2^(n-1), capped at 60 s. Tests set this to ~1 ms.
    pub retry_base_delay: Duration,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        PipelineConfig {
            // specs.md Section 13.
            max_retries: 5,
            retry_base_delay: Duration::from_secs(2),
        }
    }
}

/// The M1 digest pipeline. Implements tamako_core::digest::DigestPipeline.
/// Construct with `Arc::new(...)` and hand to the actor as
/// `Arc<dyn DigestPipeline>`.
pub struct AgentDigestPipeline<M: MemoryBackend> {
    store: Arc<Store>,
    /// The group's DEDICATED one-group Store of the decision-66
    /// embedding enqueue. The chat_id-less embedding helpers require
    /// exactly one open group per Store instance
    /// (`StoreError::AmbiguousGroup` otherwise), while the shared
    /// `store` above opens one group per served chat — in live mode
    /// with 2+ groups an enqueue through the shared store always
    /// fails. `None` disables the enqueue (a construction degrade);
    /// the worker's startup reconciliation heals the loss.
    embedding_store: Option<Arc<Store>>,
    memory: Arc<M>,
    extractor: Arc<dyn KnowledgeExtractor>,
    /// The decision-73 step-3 pre-screen parts (provider, confirmer,
    /// thresholds). `None` keeps the byte-identical Phase 1 behavior:
    /// the pipeline was built without the vector sidecar (replay mode,
    /// or a missing embedding provider key).
    vector: Option<VectorParts>,
    /// The decision-75 single-value fact predicates of the resolved
    /// group (graph-spec Section 7.5). They ride the graph commit so a
    /// new edge with a registered predicate invalidates the older valid
    /// (subject, predicate) siblings. EMPTY preserves the byte-identical
    /// Phase 1 write behavior: a pipeline built without
    /// [`Self::with_single_value_predicates`] never invalidates and
    /// never touches the `facts_invalidated_total` counter.
    single_value_predicates: Vec<String>,
    /// Decision 77 (S3-F10): one-time latch of the construction WARN —
    /// `vector_resolution` enabled but the dedicated one-group
    /// embedding store never wired (`with_vector_prescreen` without
    /// `with_embedding_store`). The builders run in either order, so
    /// the missing dependency can only be detected at first USE; the
    /// latch keeps it ONE construction WARN, not one per digest
    /// attempt.
    prescreen_store_warned: std::sync::atomic::AtomicBool,
    config: PipelineConfig,
}

/// The held parts of the decision-73 vector pre-screen. Assembled into
/// a [`VectorPrescreen`] per digest call (the KNN also needs the
/// one-group embedding store, which the pipeline holds separately).
struct VectorParts {
    /// The shared embedding provider (the same `Arc` the decision-66
    /// worker uses; the binary adapts the rig provider onto the core
    /// seam).
    provider: Arc<dyn CoreEmbeddingProvider>,
    /// The middle-band confirmation seam over the digest endpoint.
    confirmer: Arc<dyn ResolutionConfirmer>,
    /// The resolved per-group thresholds and the `vector_resolution`
    /// toggle.
    config: VectorResolutionConfig,
}

impl<M: MemoryBackend> AgentDigestPipeline<M> {
    pub fn new(
        store: Arc<Store>,
        memory: Arc<M>,
        extractor: Arc<dyn KnowledgeExtractor>,
        config: PipelineConfig,
    ) -> Self {
        AgentDigestPipeline {
            store,
            embedding_store: None,
            memory,
            extractor,
            vector: None,
            single_value_predicates: Vec::new(),
            prescreen_store_warned: std::sync::atomic::AtomicBool::new(false),
            config,
        }
    }

    /// Wires the group's dedicated one-group embedding Store (decision
    /// 66). Builder-style so the plain `new` keeps working for callers
    /// without the embedding sidecar; a pipeline built without this
    /// store still digests, with the enqueue disabled.
    pub fn with_embedding_store(mut self, store: Arc<Store>) -> Self {
        self.embedding_store = Some(store);
        self
    }

    /// Wires the decision-73 vector pre-screen (Section 7.4 step 3).
    /// Builder-style, like [`Self::with_embedding_store`]: a pipeline
    /// built without it keeps the byte-identical Phase 1 resolution.
    /// The pre-screen also needs the dedicated embedding store (the KNN
    /// read), so BOTH builders must run for step 3 to activate; the
    /// `vector_resolution` toggle lives in `config`.
    pub fn with_vector_prescreen(
        mut self,
        provider: Arc<dyn CoreEmbeddingProvider>,
        confirmer: Arc<dyn ResolutionConfirmer>,
        config: VectorResolutionConfig,
    ) -> Self {
        self.vector = Some(VectorParts {
            provider,
            confirmer,
            config,
        });
        self
    }

    /// Wires the decision-75 single-value fact registry (Section 7.5).
    /// Builder-style, like [`Self::with_embedding_store`]: the resolved
    /// per-group `single_value_predicates` of the group this pipeline
    /// serves. A pipeline built without it keeps the byte-identical
    /// Phase 1 write behavior: the commit never invalidates and never
    /// touches the `facts_invalidated_total` counter.
    pub fn with_single_value_predicates(mut self, predicates: Vec<String>) -> Self {
        self.single_value_predicates = predicates;
        self
    }

    /// Assembles the per-call step-3 bundle. `None` when the pre-screen
    /// parts are not wired, the toggle is off, or the dedicated
    /// embedding store is missing (the decision-66 construction
    /// degrade) — every `None` is the byte-identical Phase 1 behavior.
    fn vector_prescreen(&self) -> Option<VectorPrescreen<'_>> {
        let parts = self.vector.as_ref()?;
        if !parts.config.enabled {
            return None;
        }
        let Some(embedding_store) = self.embedding_store.as_ref() else {
            // Decision 77 (S3-F10): the toggle is ON but the one-group
            // embedding store was never wired — a construction-time
            // misconfiguration that silently disabled step 3 before.
            // ONE WARN (latched): the builders run in either order, so
            // first use is the earliest point the missing dependency is
            // knowable.
            if !self
                .prescreen_store_warned
                .swap(true, std::sync::atomic::Ordering::Relaxed)
            {
                tracing::warn!(
                    "vector_resolution is enabled but the dedicated one-group embedding store \
                     is not wired (with_vector_prescreen without with_embedding_store); the \
                     step-3 vector pre-screen is disabled"
                );
            }
            return None;
        };
        Some(VectorPrescreen {
            provider: &*parts.provider,
            embedding_store,
            confirmer: &*parts.confirmer,
            config: &parts.config,
        })
    }

    /// Runs one store call against `store` inside
    /// `tokio::task::spawn_blocking` (AGENT.md Section 6.2 — the same
    /// pattern the actor uses).
    async fn run_on_store<T>(
        store: Arc<Store>,
        f: impl FnOnce(&Store) -> Result<T, StoreError> + Send + 'static,
    ) -> Result<T, AgentError>
    where
        T: Send + 'static,
    {
        tokio::task::spawn_blocking(move || f(&store))
            .await
            .map_err(|error| AgentError::Join(error.to_string()))?
            .map_err(AgentError::Store)
    }

    /// Runs one store call against the shared multi-group store (the
    /// digest reads and counters — correct with any open-group count,
    /// the chat id is always explicit).
    async fn run_store<T>(
        &self,
        f: impl FnOnce(&Store) -> Result<T, StoreError> + Send + 'static,
    ) -> Result<T, AgentError>
    where
        T: Send + 'static,
    {
        Self::run_on_store(self.store.clone(), f).await
    }

    /// The backoff delay after failed attempt n (1-based): base *
    /// 2^(n-1), capped at 60 s (specs.md Section 10.3).
    fn retry_delay(&self, attempt: u32) -> Duration {
        // The shift cap keeps 2^shift small; the duration cap applies
        // anyway.
        let shift = (attempt.saturating_sub(1)).min(10);
        self.config
            .retry_base_delay
            .saturating_mul(2_u32.saturating_pow(shift))
            .min(MAX_RETRY_DELAY)
    }

    /// The counter increment of specs.md Section 12. Best effort: a
    /// counter failure is logged, never propagated. The batch outcome
    /// does not depend on its metrics.
    async fn bump_counter(&self, chat_id: &str, key: &str) {
        self.bump_counter_by(chat_id, key, 1).await;
    }

    /// The delta form of [`Self::bump_counter`]. A non-positive delta
    /// skips the store call entirely.
    async fn bump_counter_by(&self, chat_id: &str, key: &str, delta: i64) {
        if delta <= 0 {
            return;
        }
        let chat_id = chat_id.to_string();
        let key = key.to_string();
        if let Err(error) = self
            .run_store(move |store| store.increment_counter(&chat_id, &key, delta))
            .await
        {
            tracing::warn!(error = %error, "failed to increment a digest counter");
        }
    }

    /// The decision-73 step-3 counters (specs.md Section 12 naming
    /// discipline: Prometheus-compatible, `_total` suffix). Best
    /// effort, like every counter of the pipeline.
    async fn bump_vector_counters(&self, chat_id: &str, stats: VectorResolutionStats) {
        self.bump_counter_by(
            chat_id,
            "vector_resolution_matched_total",
            i64::from(stats.auto_matched),
        )
        .await;
        self.bump_counter_by(
            chat_id,
            "vector_resolution_confirmed_total",
            i64::from(stats.confirmed),
        )
        .await;
        self.bump_counter_by(
            chat_id,
            "vector_resolution_rejected_total",
            i64::from(stats.rejected),
        )
        .await;
    }

    /// One batch attempt: extraction, post-validation, resolution, and
    /// the graph write. specs.md Section 10.2. Returns the node/edge
    /// counts plus the step-3 outcome tallies of THIS attempt; the
    /// caller (the retry loop) bumps the `vector_resolution_*` counters
    /// ONCE, post-commit, from the FINAL attempt's stats (decision 77:
    /// a retried attempt must not double-count the resolution stats).
    async fn extract_and_write(
        &self,
        chat_id: &str,
        input: &ExtractionInput,
        frame: &BatchFrame,
    ) -> Result<(usize, usize, VectorResolutionStats), AgentError> {
        let graph = self.extractor.extract(input).await?;
        // Section 6.3: post-validation in plain Rust. Never trust the
        // prompt.
        let validated_names: Vec<_> = graph
            .edges
            .iter()
            .map(|edge| validate_relationship_name(&edge.relationship_name))
            .collect();
        let resolved = resolve_batch(
            &*self.memory,
            chat_id,
            &graph,
            &validated_names,
            &input.mention_map,
            &frame.batch_id,
            frame.first_msg_id,
            frame.last_msg_id,
            frame.batch_end,
            frame.msg_count,
            frame.started_at,
            // Decision 73: the step-3 vector pre-screen (`None` = the
            // byte-identical Phase 1 behavior).
            self.vector_prescreen().as_ref(),
        )
        .await?;
        let vector_stats = resolved.vector_stats;
        let batch = resolved.batch;
        let node_count = batch.nodes.len();
        let edge_count = batch.edges.len();
        // Decision 75 (Section 7.5): the graph commit rides the resolved
        // single-value registry, so a new edge with a registered
        // predicate invalidates the older valid (subject, predicate)
        // siblings inside the same transaction. An EMPTY registry is the
        // byte-identical Phase 1 write path (no invalidation, zero
        // returned). The count feeds the `facts_invalidated_total`
        // counter of decision 75 (e), best effort like every counter of
        // the pipeline (`bump_counter_by` skips zero).
        let outcome = self
            .memory
            .upsert_batch_with_registry(chat_id, &batch, &self.single_value_predicates)
            .await?;
        self.bump_counter_by(
            chat_id,
            "facts_invalidated_total",
            i64::from(outcome.invalidated),
        )
        .await;
        // Decision 66: immediately AFTER the graph commit, enqueue the
        // (node_id, content-hash) pairs into the per-group
        // pending_embeddings queue. The sidecar vector write of Section
        // 7.6 step 5 is the embedding worker's job, not the digest's.
        // The enqueue goes through the DEDICATED one-group embedding
        // store (`with_embedding_store`): the chat_id-less embedding
        // helpers reject a Store with 2+ open groups
        // (`StoreError::AmbiguousGroup`), so the shared multi-group
        // digest store cannot carry this call in live mode. A `None`
        // embedding store skips the enqueue entirely (a construction
        // degrade). Cross-store atomicity is impossible (graph =
        // LadybugDB, queue = SQLite), so the enqueue is BEST EFFORT: a
        // failure logs a WARN and never fails the digest; the startup
        // reconciliation repairs the enqueue loss. An empty item list
        // is a no-op (and skips the store call entirely).
        let items = embedding_enqueue_items(&graph, &batch);
        if !items.is_empty() {
            if let Some(embedding_store) = &self.embedding_store {
                let result = Self::run_on_store(Arc::clone(embedding_store), move |store| {
                    store.enqueue_embeddings(&items)
                })
                .await;
                if let Err(error) = result {
                    tracing::warn!(
                        chat_id,
                        batch_id = %frame.batch_id,
                        error = %error,
                        "embedding enqueue failed after the graph commit; \
                         the startup reconciliation will repair the loss"
                    );
                }
            }
        }
        // Decision 76 (Section 7.6 step 6): immediately AFTER the graph
        // commit, upsert the batch's edge descriptions into the
        // `edge_texts` sidecar (the LIKE-search mirror of the graph
        // edges' descriptions). A LOCAL write, no queue — the same
        // best-effort discipline as the enqueue above, through the same
        // dedicated one-group embedding store: a failure logs a WARN
        // and never fails the digest, and the startup reconciliation
        // (extended to edges, decision 76c) repairs the loss. The
        // upsert is idempotent and replay-convergent: INSERT OR REPLACE
        // keyed by the deterministic edge id, so a replayed batch
        // re-encodes the same ids and replaces the same rows in place.
        // Decision 77 (S4-F3): ONE batched `upsert_edge_texts` call
        // (one transaction for the whole batch) replaces the per-row
        // loop. An empty item list is a no-op (and skips the store
        // call entirely; the helper itself also early-returns).
        let edge_texts = edge_text_upsert_items(&batch);
        if !edge_texts.is_empty() {
            if let Some(embedding_store) = &self.embedding_store {
                let result = Self::run_on_store(Arc::clone(embedding_store), move |store| {
                    store.upsert_edge_texts(&edge_texts).map(|_| ())
                })
                .await;
                if let Err(error) = result {
                    tracing::warn!(
                        chat_id,
                        batch_id = %frame.batch_id,
                        error = %error,
                        "edge_texts harvest failed after the graph commit; \
                         the startup reconciliation will repair the loss"
                    );
                }
            }
        }
        Ok((node_count, edge_count, vector_stats))
    }

    /// One skeleton attempt (Section 7.2 rule 5): the MessageBatch node
    /// only, no extraction call.
    async fn store_skeleton(&self, chat_id: &str, frame: &BatchFrame) -> Result<(), AgentError> {
        let now = OffsetDateTime::now_utc();
        let batch = MemoryBatch {
            batch_id: frame.batch_id.clone(),
            nodes: vec![message_batch_node(
                &frame.batch_id,
                frame.first_msg_id,
                frame.last_msg_id,
                frame.msg_count,
                frame.started_at,
                frame.batch_end,
                now,
            )],
            edges: vec![],
        };
        self.memory.upsert_batch(chat_id, &batch).await?;
        // PHASE 2 HOOK: skeleton batches carry no entity nodes, so there
        // are no embeddings to write here. The embedding write of
        // Section 7.6 step 5 belongs to the extraction path.
        Ok(())
    }

    /// The digest run over the range `(boundary, tail]`. Refer to the
    /// `DigestPipeline` contract of tamako-core.
    async fn run(
        &self,
        chat_id: &str,
        last_digest_boundary_msg_id: i64,
    ) -> Result<Option<DigestOutcome>, CoreError> {
        // specs.md Section 10.1: the raw log range (boundary, tail].
        let chat_id_owned = chat_id.to_string();
        let rows = self
            .run_store(move |store| {
                store.list_messages_after(&chat_id_owned, last_digest_boundary_msg_id)
            })
            .await
            .map_err(core_error)?;
        if rows.is_empty() {
            return Ok(None);
        }

        // Batch assembly.
        let frame = BatchFrame::of_rows(&rows);
        let input = assemble_extraction_input(&frame.batch_id, &rows);
        let texts: Vec<&str> = rows.iter().map(|row| row.text.as_str()).collect();
        // Section 7.2 rule 5: the skeleton check. The extractor is never
        // called for a skeleton batch.
        let skeleton = is_skeleton_batch(&texts);

        // The retry/dead-letter loop of specs.md Section 10.3. One
        // iteration is one batch attempt cycle (extract -> validate ->
        // resolve -> upsert, or skeleton -> upsert). Every failure class
        // goes through the same discipline: a storage failure is also a
        // batch failure. `max_retries` is the TOTAL number of attempts.
        let mut attempt = 0_u32;
        let last_error: AgentError;
        loop {
            attempt += 1;
            let result: Result<AttemptSuccess, AgentError> = if skeleton {
                self.store_skeleton(chat_id, &frame)
                    .await
                    .map(|()| AttemptSuccess::Skeleton)
            } else {
                self.extract_and_write(chat_id, &input, &frame).await.map(
                    |(node_count, edge_count, vector_stats)| AttemptSuccess::Extracted {
                        node_count,
                        edge_count,
                        vector_stats,
                    },
                )
            };
            match result {
                Ok(AttemptSuccess::Skeleton) => {
                    return Ok(Some(DigestOutcome::Skeleton {
                        batch_id: frame.batch_id,
                        new_boundary: frame.last_msg_id,
                    }));
                }
                Ok(AttemptSuccess::Extracted {
                    node_count,
                    edge_count,
                    vector_stats,
                }) => {
                    // The step-3 outcome counters (Section 12). Best
                    // effort, bumped ONCE — post-commit, from the FINAL
                    // attempt's stats only (decision 77): a retried
                    // attempt re-resolves, and per-attempt counting
                    // would double-count the resolution stats.
                    self.bump_vector_counters(chat_id, vector_stats).await;
                    // The actor's curated `digest` line supersedes this
                    // one; the extraction detail stays at debug.
                    tracing::debug!(
                        chat_id,
                        batch_id = %frame.batch_id,
                        nodes = node_count,
                        edges = edge_count,
                        new_boundary = frame.last_msg_id,
                        "digest batch extracted"
                    );
                    return Ok(Some(DigestOutcome::Extracted {
                        batch_id: frame.batch_id,
                        new_boundary: frame.last_msg_id,
                        node_count,
                        edge_count,
                    }));
                }
                Err(error) => {
                    // The metric of specs.md Section 12, reachable here.
                    self.bump_counter(chat_id, "digest_failures_total").await;
                    if attempt >= self.config.max_retries {
                        last_error = error;
                        break;
                    }
                    let delay = self.retry_delay(attempt);
                    tracing::warn!(
                        chat_id,
                        batch_id = %frame.batch_id,
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        error = %error,
                        "digest attempt failed; retry scheduled"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }

        // Dead-letter (specs.md Section 10.3 item 2). The skipped range
        // stays in the raw log (item 3); nothing is deleted.
        let error_string = last_error.to_string();
        let batch_skeleton = serde_json::json!({
            "batch_id": frame.batch_id,
            "first_msg_id": frame.first_msg_id,
            "last_msg_id": frame.last_msg_id,
            "msg_count": frame.msg_count,
            "started_at": frame.started_at,
            "ended_at": frame.batch_end,
        })
        .to_string();
        {
            let chat_id_owned = chat_id.to_string();
            let batch_id_owned = frame.batch_id.clone();
            let error_owned = error_string.clone();
            self.run_store(move |store| {
                store.insert_dead_letter(
                    &chat_id_owned,
                    &batch_id_owned,
                    &batch_skeleton,
                    &error_owned,
                )
            })
            .await
            .map_err(core_error)?;
        }
        self.bump_counter(chat_id, "dead_letters_total").await;
        tracing::error!(
            chat_id,
            batch_id = %frame.batch_id,
            attempts = attempt,
            error = %error_string,
            "digest batch dead-lettered after all retries"
        );
        Ok(Some(DigestOutcome::DeadLettered {
            batch_id: frame.batch_id,
            new_boundary: frame.last_msg_id,
            error: error_string,
        }))
    }
}

/// The success of one batch attempt, internal to the retry loop.
enum AttemptSuccess {
    Skeleton,
    Extracted {
        node_count: usize,
        edge_count: usize,
        /// The step-3 tallies of the final attempt, bumped post-commit.
        vector_stats: VectorResolutionStats,
    },
}

/// The deterministic frame of one batch: the id range, the stable batch
/// identifier, and the timestamps. Shared by every attempt of the retry
/// loop (specs.md Section 10.3: the batch identifier is stable across
/// retries).
struct BatchFrame {
    batch_id: String,
    first_msg_id: i64,
    last_msg_id: i64,
    msg_count: u32,
    started_at: OffsetDateTime,
    batch_end: OffsetDateTime,
}

impl BatchFrame {
    /// Builds the frame of the range `(boundary, tail]`. Section 7.2
    /// adapted: the actor fires the trigger (product thresholds of
    /// specs.md Section 8.2); the batch here is exactly the returned
    /// range.
    fn of_rows(rows: &[MessageRow]) -> Self {
        let first = rows.first();
        let last = rows.last();
        let first_msg_id = first.map(|row| row.id).unwrap_or_default();
        let last_msg_id = last.map(|row| row.id).unwrap_or_default();
        BatchFrame {
            // The batch id is STABLE across retries (anti-defer list,
            // dev-roadmap.md Section 7).
            batch_id: message_batch_id(first_msg_id, last_msg_id),
            first_msg_id,
            last_msg_id,
            msg_count: rows.len() as u32,
            started_at: first
                .map(|row| row.timestamp)
                .unwrap_or_else(OffsetDateTime::now_utc),
            batch_end: last
                .map(|row| row.timestamp)
                .unwrap_or_else(OffsetDateTime::now_utc),
        }
    }
}

/// Builds the (node_id, content_hash) enqueue pairs of one committed
/// batch (decision 66). One pair per extracted entity: the post-resolve
/// node id plus the hash of the pipeline-known candidate content (the
/// extracted name and description). The surface-form Alias nodes and
/// the MessageBatch node carry no embedding. An entity with an empty
/// candidate name is skipped. The embedding worker re-reads the stored
/// content and rehashes it, so alias drift needs no handling here.
fn embedding_enqueue_items(graph: &KnowledgeGraph, batch: &MemoryBatch) -> Vec<(String, String)> {
    let mut items = Vec::new();
    for extracted in &graph.nodes {
        if extracted.name.is_empty() {
            continue;
        }
        // The resolved entity node carries the extracted name. Prefer
        // the Person/Concept node over the surface-form Alias node of
        // the same name; the Section 7.4 step-4 fallback has no entity
        // node at all — its Alias node doubles as the entity node.
        let entity = batch
            .nodes
            .iter()
            .filter(|node| node.name == extracted.name)
            .find(|node| matches!(node.node_type, NodeType::Person | NodeType::Concept))
            .or_else(|| {
                batch
                    .nodes
                    .iter()
                    .find(|node| node.name == extracted.name && node.node_type == NodeType::Alias)
            });
        let Some(node) = entity else {
            continue;
        };
        let hash = embedding_content_hash(&extracted.name, &extracted.description);
        items.push((node.id.clone(), hash));
    }
    items
}

/// Builds the (edge_id, edge_text) upsert pairs of one committed batch
/// (decision 76, Section 7.6 step 6). One pair per batch edge with a
/// non-empty description: the edge id is the [`EdgeId::encode`] of the
/// resolved natural key (`source_id` / `relationship_name` /
/// `target_id` / `valid_at`) the graph commit MERGEd, so the pairs
/// name the sidecar rows of exactly the edges the graph now holds.
/// Every edge kind is harvested — the recall LIKE scan decides what to
/// match; the sidecar must mirror the graph whole, or the startup
/// reconciliation's set difference would prune the harvested rows. An
/// edge with an empty `edge_text` is skipped: nothing to search.
fn edge_text_upsert_items(batch: &MemoryBatch) -> Vec<(String, String)> {
    batch
        .edges
        .iter()
        .filter(|edge| !edge.edge_text.is_empty())
        .map(|edge| {
            let edge_id = EdgeId {
                source_id: edge.source_id.clone(),
                relationship_name: edge.relationship_name.clone(),
                target_id: edge.target_id.clone(),
                valid_at: edge.valid_at,
            }
            .encode();
            (edge_id, edge.edge_text.clone())
        })
        .collect()
}

/// Maps the crate error to the core error (the `DigestPipeline`
/// contract). Store/Memory/Join map to their CoreError counterparts;
/// Extraction/ProviderConfig become CoreError::Digest.
fn core_error(error: AgentError) -> CoreError {
    match error {
        AgentError::Store(error) => CoreError::Store(error),
        AgentError::Memory(error) => CoreError::Memory(error),
        AgentError::Join(error) => CoreError::Join(error),
        AgentError::Extraction(error) | AgentError::ProviderConfig(error) => {
            CoreError::Digest(error)
        }
    }
}

impl<M: MemoryBackend> DigestPipeline for AgentDigestPipeline<M> {
    fn run_digest<'a>(
        &'a self,
        chat_id: &'a str,
        last_digest_boundary_msg_id: i64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<DigestOutcome>, CoreError>> + Send + 'a>> {
        Box::pin(async move { self.run(chat_id, last_digest_boundary_msg_id).await })
    }
}

/// Batch assembly (Section 7.2): the labeled messages and the
/// mention/reply map of the batch.
fn assemble_extraction_input(batch_id: &str, rows: &[MessageRow]) -> ExtractionInput {
    let messages = rows
        .iter()
        .map(|row| BatchMessage {
            display_name: row.sender_display_name.clone(),
            // HH:MM in UTC from the log-row timestamp.
            time_hhmm: row
                .timestamp
                .to_offset(UtcOffset::UTC)
                .format(HHMM_FORMAT)
                .unwrap_or_else(|_| "??:??".to_string()),
            text: row.text.clone(),
        })
        .collect();
    ExtractionInput {
        batch_id: batch_id.to_string(),
        messages,
        mention_map: assemble_mention_map(rows),
    }
}

/// The mention/reply map of specs.md Section 10.1: every row
/// contributes a `Sender` binding (display name -> sender id); a row
/// whose reply target is an earlier row of the batch contributes a
/// `ReplyTarget` binding (the REPLIED-TO sender). Deduped by
/// (normalized display name, tg_user_id); the first source wins.
fn assemble_mention_map(rows: &[MessageRow]) -> Vec<MentionBinding> {
    let mut bindings: Vec<MentionBinding> = Vec::new();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    // Reply targets resolve against earlier rows of the batch only.
    let mut by_platform_id: HashMap<&str, &MessageRow> = HashMap::new();
    for row in rows {
        push_binding(
            &mut bindings,
            &mut seen,
            &row.sender_display_name,
            &row.sender_id,
            BindingSource::Sender,
        );
        if let Some(reply_to) = &row.reply_to_platform_msg_id {
            if let Some(target) = by_platform_id.get(reply_to.as_str()) {
                push_binding(
                    &mut bindings,
                    &mut seen,
                    &target.sender_display_name,
                    &target.sender_id,
                    BindingSource::ReplyTarget,
                );
            }
        }
        by_platform_id.insert(row.platform_msg_id.as_str(), row);
    }
    bindings
}

/// Adds one binding when its (normalized display name, tg_user_id) pair
/// is new. Section 7.1 normalization on the display-name side.
fn push_binding(
    bindings: &mut Vec<MentionBinding>,
    seen: &mut HashSet<(String, String)>,
    display_name: &str,
    tg_user_id: &str,
    source: BindingSource,
) {
    if seen.insert((normalize(display_name), tg_user_id.to_string())) {
        bindings.push(MentionBinding {
            display_name: display_name.to_string(),
            tg_user_id: tg_user_id.to_string(),
            source,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{ExtractedEdge, ExtractedNode, ExtractedNodeType};
    use std::collections::VecDeque;
    use tamako_memory::identifiers::{concept_id, person_id};
    use tamako_memory::{LbugBackend, MemoryNode};
    use tamako_store::{Direction, EventType, NewMessage};

    #[test]
    fn the_retry_delay_doubles_and_caps_at_60_seconds() {
        // specs.md Section 10.3: the delay before retry n is
        // base * 2^(n-1), capped at 60 s.
        let config = PipelineConfig {
            max_retries: 30,
            retry_base_delay: Duration::from_secs(1),
        };
        let store = Store::new(std::path::PathBuf::from("/nonexistent"));
        let memory = tamako_memory::LbugBackend::new(std::path::PathBuf::from("/nonexistent"));
        let pipeline = AgentDigestPipeline::new(
            Arc::new(store),
            Arc::new(memory),
            Arc::new(crate::ScriptedExtractor::failing("boom")),
            config,
        );
        assert_eq!(pipeline.retry_delay(1), Duration::from_secs(1));
        assert_eq!(pipeline.retry_delay(2), Duration::from_secs(2));
        assert_eq!(pipeline.retry_delay(3), Duration::from_secs(4));
        assert_eq!(pipeline.retry_delay(7), Duration::from_secs(60));
        assert_eq!(pipeline.retry_delay(20), Duration::from_secs(60));
    }

    #[test]
    fn the_default_config_matches_specs_section_13() {
        let config = PipelineConfig::default();
        assert_eq!(config.max_retries, 5);
    }

    #[test]
    fn the_pipeline_is_a_digest_pipeline_object() {
        // The actor holds Arc<dyn DigestPipeline>. This assertion keeps
        // the impl object-safe-compatible.
        fn assert_pipeline(_: Option<Arc<dyn DigestPipeline>>) {}
        assert_pipeline(None);
    }

    // ---- Decision 66: enqueue into pending_embeddings after the graph
    // commit. Real temp Store + real LbugBackend, scripted extractor
    // (the pattern of tamako-agent/tests/pipeline.rs). ----

    const ENQUEUE_CHAT: &str = "enqueue_test";

    fn enqueue_fixtures() -> (tempfile::TempDir, Arc<Store>, Arc<LbugBackend>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(Store::new(dir.path()));
        let memory = Arc::new(LbugBackend::new(dir.path()));
        (dir, store, memory)
    }

    fn inbound(platform_id: &str, sender_id: &str, display_name: &str, text: &str) -> NewMessage {
        NewMessage {
            platform_msg_id: platform_id.to_string(),
            direction: Direction::Inbound,
            event_type: EventType::Message,
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp"),
            sender_id: sender_id.to_string(),
            sender_display_name: display_name.to_string(),
            sender_username: None,
            text: text.to_string(),
            reply_to_platform_msg_id: None,
            mentions_bot: false,
            is_reply_to_bot: false,
        }
    }

    fn alice_deploy_graph() -> KnowledgeGraph {
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

    /// The group's dedicated one-group embedding Store of decision 66
    /// (the same shape as the worker's `GroupEmbeddingTarget::open`):
    /// a separate Store instance with exactly one open group.
    fn dedicated_embedding_store(dir: &tempfile::TempDir) -> Arc<Store> {
        let store = Arc::new(Store::new(dir.path()));
        store.open_group(ENQUEUE_CHAT).expect("open group");
        store
    }

    /// Builds the pipeline the way live mode does: the shared
    /// multi-group store for the digest reads plus, when given, the
    /// dedicated one-group embedding store for the enqueue. `None` is
    /// the construction degrade (enqueue disabled).
    fn enqueue_pipeline(
        store: &Arc<Store>,
        memory: &Arc<LbugBackend>,
        extractor: crate::ScriptedExtractor,
        embedding_store: Option<Arc<Store>>,
    ) -> AgentDigestPipeline<LbugBackend> {
        let pipeline = AgentDigestPipeline::new(
            Arc::clone(store),
            Arc::clone(memory),
            Arc::new(extractor),
            PipelineConfig {
                max_retries: 2,
                retry_base_delay: Duration::from_millis(1),
            },
        );
        match embedding_store {
            Some(store) => pipeline.with_embedding_store(store),
            None => pipeline,
        }
    }

    fn enqueue_pairs(store: &Store) -> Vec<(String, String)> {
        let mut pairs: Vec<(String, String)> = store
            .claim_embedding_batch(100)
            .expect("claim")
            .into_iter()
            .map(|row| (row.node_id, row.content_hash))
            .collect();
        pairs.sort();
        pairs
    }

    #[tokio::test]
    async fn the_graph_commit_enqueues_the_entity_embedding_pairs() {
        // Decision 66: after upsert_batch commits, one queue row per
        // extracted entity carries the post-resolve node id and the hash
        // of the pipeline-known candidate content. The Alias nodes and
        // the MessageBatch node are NOT queued.
        let (dir, store, memory) = enqueue_fixtures();
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m1", "1001", "Alice", "I will deploy the fix tonight"),
            )
            .expect("insert");
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m2", "2002", "Bob", "the staging deploy is already done"),
            )
            .expect("insert");
        let embedding_store = dedicated_embedding_store(&dir);
        let pipeline = enqueue_pipeline(
            &store,
            &memory,
            crate::ScriptedExtractor::with_graphs(vec![alice_deploy_graph()]),
            Some(Arc::clone(&embedding_store)),
        );

        let outcome = pipeline
            .run_digest(ENQUEUE_CHAT, 0)
            .await
            .expect("digest")
            .expect("non-empty tail");
        assert!(matches!(outcome, DigestOutcome::Extracted { .. }));

        // Section 7.4 step 1: Alice binds to person_id("1001") through
        // the sender mention binding; the concept takes the
        // deterministic id.
        assert_eq!(
            enqueue_pairs(&embedding_store),
            vec![
                (
                    concept_id("the deploy"),
                    embedding_content_hash("the deploy", "The nightly deploy.")
                ),
                (
                    person_id("1001"),
                    embedding_content_hash("Alice", "A group member who deploys.")
                ),
            ]
        );
    }

    #[tokio::test]
    async fn a_failed_enqueue_never_fails_the_digest() {
        // Decision 66 best-effort discipline: an embedding Store rigged
        // to fail the enqueue (two groups open -> the chat_id-less
        // embedding helper returns StoreError::AmbiguousGroup) still
        // yields a successful digest, and the failure never enters the
        // Phase 1 digest error taxonomy.
        let (dir, store, memory) = enqueue_fixtures();
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m1", "1001", "Alice", "I will deploy the fix tonight"),
            )
            .expect("insert");
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m2", "2002", "Bob", "the staging deploy is already done"),
            )
            .expect("insert");
        // The rig: the embedding helpers require EXACTLY ONE open group
        // per Store instance.
        let embedding_store = dedicated_embedding_store(&dir);
        embedding_store
            .open_group("enqueue_test_other")
            .expect("open");
        let pipeline = enqueue_pipeline(
            &store,
            &memory,
            crate::ScriptedExtractor::with_graphs(vec![alice_deploy_graph()]),
            Some(embedding_store),
        );

        let outcome = pipeline
            .run_digest(ENQUEUE_CHAT, 0)
            .await
            .expect("the digest is Ok despite the enqueue failure")
            .expect("non-empty tail");
        match outcome {
            // Person + Concept + 2 Alias + MessageBatch.
            DigestOutcome::Extracted { node_count, .. } => assert_eq!(node_count, 5),
            other => panic!("expected Extracted, got {other:?}"),
        }
        assert_eq!(
            store
                .get_state(ENQUEUE_CHAT, "digest_failures_total")
                .expect("state"),
            None
        );
    }

    #[tokio::test]
    async fn repeating_the_digest_dedups_the_enqueue_rows() {
        // INSERT OR IGNORE on UNIQUE(node_id, content_hash): the same
        // digest twice queues the same rows, no duplicates.
        let (dir, store, memory) = enqueue_fixtures();
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m1", "1001", "Alice", "I will deploy the fix tonight"),
            )
            .expect("insert");
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m2", "2002", "Bob", "the staging deploy is already done"),
            )
            .expect("insert");
        let embedding_store = dedicated_embedding_store(&dir);
        let pipeline = enqueue_pipeline(
            &store,
            &memory,
            crate::ScriptedExtractor::with_graphs(vec![alice_deploy_graph(), alice_deploy_graph()]),
            Some(Arc::clone(&embedding_store)),
        );

        for run in 1..=2 {
            let outcome = pipeline
                .run_digest(ENQUEUE_CHAT, 0)
                .await
                .expect("digest")
                .expect("non-empty tail");
            assert!(
                matches!(outcome, DigestOutcome::Extracted { .. }),
                "run {run} must extract"
            );
        }

        assert_eq!(
            enqueue_pairs(&embedding_store),
            vec![
                (
                    concept_id("the deploy"),
                    embedding_content_hash("the deploy", "The nightly deploy.")
                ),
                (
                    person_id("1001"),
                    embedding_content_hash("Alice", "A group member who deploys.")
                ),
            ]
        );
    }

    #[tokio::test]
    async fn the_multi_group_topology_enqueues_through_the_dedicated_store() {
        // The LIVE-mode topology (the S5 defect): the shared digest
        // store opens one group per served chat, so with 2+ groups an
        // enqueue through it would AmbiguousGroup-fail on every digest.
        // The pipeline must instead enqueue through its dedicated
        // one-group embedding store while the shared store keeps
        // serving the multi-group digest reads.
        let (dir, store, memory) = enqueue_fixtures();
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m1", "1001", "Alice", "I will deploy the fix tonight"),
            )
            .expect("insert");
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m2", "2002", "Bob", "the staging deploy is already done"),
            )
            .expect("insert");
        // The second served group on the SHARED store (the second live
        // actor): the enqueue helpers can no longer resolve the group
        // on this instance.
        store
            .insert_message(
                "enqueue_chat_other",
                &inbound("x1", "9009", "Carol", "an unrelated chat"),
            )
            .expect("insert");
        let embedding_store = dedicated_embedding_store(&dir);
        let pipeline = enqueue_pipeline(
            &store,
            &memory,
            crate::ScriptedExtractor::with_graphs(vec![alice_deploy_graph()]),
            Some(Arc::clone(&embedding_store)),
        );

        let outcome = pipeline
            .run_digest(ENQUEUE_CHAT, 0)
            .await
            .expect("digest")
            .expect("non-empty tail");
        assert!(matches!(outcome, DigestOutcome::Extracted { .. }));

        // The queue rows land in the group's own store, readable
        // through the dedicated one-group handle despite the two open
        // groups on the shared store.
        assert_eq!(
            enqueue_pairs(&embedding_store),
            vec![
                (
                    concept_id("the deploy"),
                    embedding_content_hash("the deploy", "The nightly deploy.")
                ),
                (
                    person_id("1001"),
                    embedding_content_hash("Alice", "A group member who deploys.")
                ),
            ]
        );
    }

    #[tokio::test]
    async fn the_digest_runs_with_the_embedding_enqueue_disabled() {
        // The construction degrade of main.rs (WARN + proceed with a
        // None embedding store): the digest still runs; the enqueue is
        // skipped and the startup reconciliation heals the loss.
        let (dir, store, memory) = enqueue_fixtures();
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m1", "1001", "Alice", "I will deploy the fix tonight"),
            )
            .expect("insert");
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m2", "2002", "Bob", "the staging deploy is already done"),
            )
            .expect("insert");
        let pipeline = enqueue_pipeline(
            &store,
            &memory,
            crate::ScriptedExtractor::with_graphs(vec![alice_deploy_graph()]),
            None,
        );

        let outcome = pipeline
            .run_digest(ENQUEUE_CHAT, 0)
            .await
            .expect("the digest is Ok with the enqueue disabled")
            .expect("non-empty tail");
        match outcome {
            // Person + Concept + 2 Alias + MessageBatch.
            DigestOutcome::Extracted { node_count, .. } => assert_eq!(node_count, 5),
            other => panic!("expected Extracted, got {other:?}"),
        }

        // Nothing was queued.
        let reader = dedicated_embedding_store(&dir);
        assert!(enqueue_pairs(&reader).is_empty());
    }

    #[test]
    fn the_enqueue_items_cover_entities_and_the_alias_fallback() {
        // The pure pairing rule: the entity node wins over the
        // surface-form Alias of the same name; the Section 7.4 step-4
        // fallback Alias doubles as the entity node; empty candidate
        // names and non-entity nodes are skipped.
        let now = OffsetDateTime::now_utc();
        let node = |id: &str, name: &str, node_type: NodeType| MemoryNode {
            id: id.to_string(),
            name: name.to_string(),
            node_type,
            created_at: now,
            updated_at: now,
            properties: None,
        };
        let graph = KnowledgeGraph {
            nodes: vec![
                ExtractedNode {
                    name: "Alice".to_string(),
                    node_type: ExtractedNodeType::Person,
                    description: "deploys.".to_string(),
                },
                ExtractedNode {
                    name: "tama".to_string(),
                    node_type: ExtractedNodeType::Person,
                    description: "a cat.".to_string(),
                },
                ExtractedNode {
                    name: String::new(),
                    node_type: ExtractedNodeType::Concept,
                    description: "no name.".to_string(),
                },
            ],
            edges: vec![],
        };
        let batch = MemoryBatch {
            batch_id: "b1".to_string(),
            nodes: vec![
                node("alias-alice", "Alice", NodeType::Alias),
                node("person-alice", "Alice", NodeType::Person),
                // The step-4 fallback: the Alias node IS the entity node.
                node("alias-tama", "tama", NodeType::Alias),
                node("b1", "b1", NodeType::MessageBatch),
            ],
            edges: vec![],
        };

        assert_eq!(
            embedding_enqueue_items(&graph, &batch),
            vec![
                (
                    "person-alice".to_string(),
                    embedding_content_hash("Alice", "deploys.")
                ),
                (
                    "alias-tama".to_string(),
                    embedding_content_hash("tama", "a cat.")
                ),
            ]
        );
    }

    // ---- Decision 73: the vector pre-screen wired end to end. Real
    // tempdir Stores + real LbugBackend, scripted extractor/provider/
    // confirmer doubles. ----

    /// A scripted core embedding provider for the pipeline-level
    /// pre-screen tests (the same shape as the resolve.rs double).
    struct ScriptedEmbedder {
        batches: std::sync::Mutex<VecDeque<Result<Vec<Vec<f32>>, String>>>,
        calls: std::sync::Mutex<Vec<Vec<String>>>,
    }

    impl ScriptedEmbedder {
        fn with_batches(batches: Vec<Vec<Vec<f32>>>) -> Self {
            ScriptedEmbedder {
                batches: std::sync::Mutex::new(
                    batches.into_iter().map(Ok).collect::<VecDeque<_>>(),
                ),
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn call_count(&self) -> usize {
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
        }
    }

    impl CoreEmbeddingProvider for ScriptedEmbedder {
        fn embed<'a>(
            &'a self,
            text: &'a str,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<Vec<f32>, tamako_core::embedding::EmbeddingError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                let texts = [text.to_string()];
                let mut batches = self.embed_texts(&texts).await?;
                Ok(batches.remove(0))
            })
        }

        fn embed_texts<'a>(
            &'a self,
            texts: &'a [String],
        ) -> Pin<
            Box<
                dyn Future<Output = Result<Vec<Vec<f32>>, tamako_core::embedding::EmbeddingError>>
                    + Send
                    + 'a,
            >,
        > {
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(texts.to_vec());
            let result = self
                .batches
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .unwrap_or_else(|| Err("scripted embedder: exhausted queue".to_string()));
            Box::pin(
                async move { result.map_err(tamako_core::embedding::EmbeddingError::Provider) },
            )
        }
    }

    /// A 4096-dimensional unit basis vector (the pinned sidecar
    /// dimension); identical query/candidate vectors give cosine
    /// similarity 1.0, above the 0.92 match threshold.
    fn prescreen_unit_vector(dim: usize) -> Vec<f32> {
        let mut vector = vec![0.0; crate::endpoint::EMBEDDING_DIMS];
        vector[dim] = 1.0;
        vector
    }

    /// Seeds one bare Person node (a pre-screen candidate; no alias
    /// edges, so step 2 never fires for the entity under test).
    async fn seed_person_node(memory: &LbugBackend, id: &str, name: &str) {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
        memory.ensure_schema(ENQUEUE_CHAT).await.expect("schema");
        let batch = MemoryBatch {
            batch_id: format!("seed-{id}"),
            nodes: vec![MemoryNode {
                id: id.to_string(),
                name: name.to_string(),
                node_type: NodeType::Person,
                created_at: now,
                updated_at: now,
                properties: None,
            }],
            edges: vec![],
        };
        memory
            .upsert_batch(ENQUEUE_CHAT, &batch)
            .await
            .expect("seed");
    }

    /// The decision-73 digest graph: the extracted person "Al" matches
    /// NO mention binding (the senders are Alice/Bob) and no alias, so
    /// it reaches step 3.
    fn al_graph() -> KnowledgeGraph {
        KnowledgeGraph {
            nodes: vec![ExtractedNode {
                name: "Al".to_string(),
                node_type: ExtractedNodeType::Person,
                description: "Alice, the group member who deploys.".to_string(),
            }],
            edges: vec![],
        }
    }

    fn al_messages(store: &Store) {
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m1", "1001", "Alice", "I will deploy the fix tonight"),
            )
            .expect("insert");
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m2", "2002", "Bob", "the staging deploy is already done"),
            )
            .expect("insert");
    }

    #[tokio::test]
    async fn the_person_top_band_confirms_and_increments_the_confirmed_counter() {
        // Decision 79 (b) end to end: "Al" reaches step 3, the seeded
        // person node is a top-band hit (sim 1.0 >= 0.92) — a
        // PERSON-kind binding, so the budget-capped confirmation call
        // runs (the same call the middle band makes); the accept binds
        // the reused id and the `confirmed` state-table counter
        // increments (the house counter mechanism of specs.md Section
        // 12). No auto-match happened: `matched` stays absent.
        let (dir, store, memory) = enqueue_fixtures();
        al_messages(&store);
        seed_person_node(&memory, &person_id("1001"), "Alice").await;
        let embedding_store = dedicated_embedding_store(&dir);
        embedding_store
            .upsert_node_embedding(&person_id("1001"), &prescreen_unit_vector(0))
            .expect("seed embedding");

        let provider = Arc::new(ScriptedEmbedder::with_batches(vec![vec![
            prescreen_unit_vector(0),
        ]]));
        let confirmer = Arc::new(crate::resolve::ScriptedConfirmer::with_answers(vec![
            crate::resolve::ConfirmationAnswer {
                same: true,
                reason: "scripted".to_string(),
            },
        ]));
        let pipeline = enqueue_pipeline(
            &store,
            &memory,
            crate::ScriptedExtractor::with_graphs(vec![al_graph()]),
            Some(Arc::clone(&embedding_store)),
        )
        .with_vector_prescreen(
            Arc::clone(&provider) as Arc<dyn CoreEmbeddingProvider>,
            Arc::clone(&confirmer) as Arc<dyn crate::resolve::ResolutionConfirmer>,
            VectorResolutionConfig::default(),
        );

        let outcome = pipeline
            .run_digest(ENQUEUE_CHAT, 0)
            .await
            .expect("digest")
            .expect("non-empty tail");
        assert!(matches!(outcome, DigestOutcome::Extracted { .. }));

        // The entity bound to the existing node: its embedding enqueue
        // pair carries the REUSED id.
        assert_eq!(
            enqueue_pairs(&embedding_store),
            vec![(
                person_id("1001"),
                embedding_content_hash("Al", "Alice, the group member who deploys.")
            )]
        );
        assert_eq!(
            store
                .get_state(ENQUEUE_CHAT, "vector_resolution_confirmed_total")
                .expect("state"),
            Some("1".to_string())
        );
        // A top-band Person confirm is NEVER an auto-match (decision
        // 79 (b)): the matched counter does not move.
        assert_eq!(
            store
                .get_state(ENQUEUE_CHAT, "vector_resolution_matched_total")
                .expect("state"),
            None
        );
        assert_eq!(confirmer.calls().len(), 1);
        assert_eq!(provider.call_count(), 1);
    }

    #[tokio::test]
    async fn the_disabled_vector_toggle_never_calls_the_provider() {
        // `vector_resolution` false: step 3 is skipped entirely. The
        // provider is never called, no counters move, and the outcome
        // is the byte-identical Phase 1 behavior (the entity attaches
        // to its fallback Alias node).
        let (dir, store, memory) = enqueue_fixtures();
        al_messages(&store);
        seed_person_node(&memory, &person_id("1001"), "Alice").await;
        let embedding_store = dedicated_embedding_store(&dir);
        embedding_store
            .upsert_node_embedding(&person_id("1001"), &prescreen_unit_vector(0))
            .expect("seed embedding");

        let provider = Arc::new(ScriptedEmbedder::with_batches(vec![vec![
            prescreen_unit_vector(0),
        ]]));
        let confirmer = Arc::new(crate::resolve::ScriptedConfirmer::with_answers(vec![]));
        let pipeline = enqueue_pipeline(
            &store,
            &memory,
            crate::ScriptedExtractor::with_graphs(vec![al_graph()]),
            Some(Arc::clone(&embedding_store)),
        )
        .with_vector_prescreen(
            Arc::clone(&provider) as Arc<dyn CoreEmbeddingProvider>,
            Arc::clone(&confirmer) as Arc<dyn crate::resolve::ResolutionConfirmer>,
            VectorResolutionConfig {
                enabled: false,
                ..VectorResolutionConfig::default()
            },
        );

        let outcome = pipeline
            .run_digest(ENQUEUE_CHAT, 0)
            .await
            .expect("digest")
            .expect("non-empty tail");
        match outcome {
            // Phase 1: the fallback Alias node doubles as the entity
            // node; plus the MessageBatch node. No person node.
            DigestOutcome::Extracted { node_count, .. } => assert_eq!(node_count, 2),
            other => panic!("expected Extracted, got {other:?}"),
        }
        assert_eq!(provider.call_count(), 0);
        assert_eq!(confirmer.calls().len(), 0);
        assert_eq!(
            store
                .get_state(ENQUEUE_CHAT, "vector_resolution_matched_total")
                .expect("state"),
            None
        );
    }

    // ---- Decision 75: the single-value registry rides the graph
    // commit. Real tempdir Store + real LbugBackend, scripted extractor
    // (the pattern of the decision-66/73 tests above). ----

    /// One batch carrying a single-value CHANGE ("quit A, now at B"):
    /// two `works_at` edges out of the same sender-bound person, in
    /// batch order. The last write must win.
    fn works_at_change_graph() -> KnowledgeGraph {
        KnowledgeGraph {
            nodes: vec![
                ExtractedNode {
                    name: "Alice".to_string(),
                    node_type: ExtractedNodeType::Person,
                    description: "A group member changing jobs.".to_string(),
                },
                ExtractedNode {
                    name: "AcmeCorp".to_string(),
                    node_type: ExtractedNodeType::Concept,
                    description: "The old employer.".to_string(),
                },
                ExtractedNode {
                    name: "NewCorp".to_string(),
                    node_type: ExtractedNodeType::Concept,
                    description: "The new employer.".to_string(),
                },
            ],
            edges: vec![
                ExtractedEdge {
                    source: "Alice".to_string(),
                    target: "AcmeCorp".to_string(),
                    relationship_name: "works_at".to_string(),
                    description: "Alice works at AcmeCorp.".to_string(),
                },
                ExtractedEdge {
                    source: "Alice".to_string(),
                    target: "NewCorp".to_string(),
                    relationship_name: "works_at".to_string(),
                    description: "Alice now works at NewCorp.".to_string(),
                },
            ],
        }
    }

    /// The valid/invalid `works_at` rows of the sender-bound Alice, as
    /// (target name, invalid_at rendering) pairs.
    async fn works_at_rows(memory: &LbugBackend) -> Vec<(String, String)> {
        memory
            .query_rows(
                ENQUEUE_CHAT,
                &format!(
                    "MATCH (s:Node {{id: '{}'}})-[r:EDGE]->(t:Node) \
                     WHERE r.relationship_name = 'works_at' \
                     RETURN t.name, r.invalid_at",
                    person_id("1001")
                ),
            )
            .await
            .expect("query")
            .into_iter()
            .map(|row| (row[0].clone(), row[1].clone()))
            .collect()
    }

    #[tokio::test]
    async fn a_registered_predicate_change_invalidates_the_old_edge_and_bumps_the_counter() {
        // Decision 75 (b)/(e): one batch carries the change, the last
        // write wins, the older valid sibling is invalidated inside the
        // same transaction, and the count feeds facts_invalidated_total.
        let (_dir, store, memory) = enqueue_fixtures();
        al_messages(&store);
        let pipeline = enqueue_pipeline(
            &store,
            &memory,
            crate::ScriptedExtractor::with_graphs(vec![works_at_change_graph()]),
            None,
        )
        .with_single_value_predicates(vec!["works_at".to_string()]);

        let outcome = pipeline
            .run_digest(ENQUEUE_CHAT, 0)
            .await
            .expect("digest")
            .expect("non-empty tail");
        assert!(matches!(outcome, DigestOutcome::Extracted { .. }));

        // The counter moved by exactly the invalidated count.
        assert_eq!(
            store
                .get_state(ENQUEUE_CHAT, "facts_invalidated_total")
                .expect("state"),
            Some("1".to_string())
        );
        // Exactly one valid works_at edge on Alice (the last write);
        // the older one carries an invalid_at.
        let rows = works_at_rows(&memory).await;
        assert_eq!(rows.len(), 2);
        let valid: Vec<_> = rows
            .iter()
            .filter(|(_, invalid_at)| invalid_at == "NULL")
            .collect();
        assert_eq!(valid.len(), 1, "exactly one valid works_at edge");
        assert_eq!(valid[0].0, "NewCorp", "the last write wins");
    }

    #[tokio::test]
    async fn no_registry_keeps_the_byte_identical_phase_1_write_behavior() {
        // A pipeline built WITHOUT with_single_value_predicates keeps
        // the decision-74 write path: every predicate stays multi-value
        // (both edges valid) and no facts_invalidated_total counter row
        // appears (bump_counter_by skips a zero delta).
        let (_dir, store, memory) = enqueue_fixtures();
        al_messages(&store);
        let pipeline = enqueue_pipeline(
            &store,
            &memory,
            crate::ScriptedExtractor::with_graphs(vec![works_at_change_graph()]),
            None,
        );

        let outcome = pipeline
            .run_digest(ENQUEUE_CHAT, 0)
            .await
            .expect("digest")
            .expect("non-empty tail");
        assert!(matches!(outcome, DigestOutcome::Extracted { .. }));

        // No counter row at all.
        assert_eq!(
            store
                .get_state(ENQUEUE_CHAT, "facts_invalidated_total")
                .expect("state"),
            None
        );
        // Both edges stay valid (multi-value Phase 1).
        let rows = works_at_rows(&memory).await;
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter().all(|(_, invalid_at)| invalid_at == "NULL"),
            "no invalidation without a registry"
        );
    }

    // ---- Decision 76: the edge_texts harvest after the graph commit.
    // Real temp Store + real LbugBackend, scripted extractor (the
    // pattern of the decision-66 tests above). ----

    /// The fact-edge id of the `alice_deploy_graph` digest: the
    /// [`EdgeId::encode`] of the resolved natural key (the sender-bound
    /// Alice person, the deterministic concept id; valid_at = batch_end
    /// = the last message's timestamp, resolve.rs Section 7.5).
    fn works_on_edge_id() -> String {
        EdgeId {
            source_id: person_id("1001"),
            relationship_name: "works_on".to_string(),
            target_id: concept_id("the deploy"),
            valid_at: OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp"),
        }
        .encode()
    }

    /// The sorted edge ids of the graph (the reconciliation truth of
    /// decision 76c).
    async fn graph_edge_ids(memory: &LbugBackend) -> Vec<String> {
        let mut ids: Vec<String> = memory
            .list_all_edges(ENQUEUE_CHAT)
            .await
            .expect("edges")
            .into_iter()
            .map(|(edge_id, _)| edge_id)
            .collect();
        ids.sort();
        ids
    }

    #[tokio::test]
    async fn the_graph_commit_harvests_the_edge_descriptions() {
        // Decision 76 (Section 7.6 step 6): after the graph commit the
        // batch's edge descriptions land in the edge_texts sidecar,
        // keyed by the encoded natural key. Every edge kind is
        // harvested (fact, surface-form, contains provenance), so the
        // sidecar mirrors the graph whole.
        let (dir, store, memory) = enqueue_fixtures();
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m1", "1001", "Alice", "I will deploy the fix tonight"),
            )
            .expect("insert");
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m2", "2002", "Bob", "the staging deploy is already done"),
            )
            .expect("insert");
        let embedding_store = dedicated_embedding_store(&dir);
        let pipeline = enqueue_pipeline(
            &store,
            &memory,
            crate::ScriptedExtractor::with_graphs(vec![alice_deploy_graph()]),
            Some(Arc::clone(&embedding_store)),
        );

        let outcome = pipeline
            .run_digest(ENQUEUE_CHAT, 0)
            .await
            .expect("digest")
            .expect("non-empty tail");
        assert!(matches!(outcome, DigestOutcome::Extracted { .. }));

        // The sidecar mirrors the graph's edges exactly.
        assert_eq!(
            embedding_store.list_edge_text_ids().expect("ids"),
            graph_edge_ids(&memory).await
        );
        // The fact edge's row carries the extracted description under
        // the resolved natural-key id.
        assert_eq!(
            embedding_store
                .search_edge_texts("deploys the fix tonight")
                .expect("search"),
            vec![works_on_edge_id()]
        );
    }

    #[tokio::test]
    async fn repeating_the_digest_converges_the_edge_text_rows() {
        // The upsert is idempotent and replay-convergent: the same
        // digest twice re-encodes the same deterministic edge ids and
        // replaces the same rows — same rows, no error.
        let (dir, store, memory) = enqueue_fixtures();
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m1", "1001", "Alice", "I will deploy the fix tonight"),
            )
            .expect("insert");
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m2", "2002", "Bob", "the staging deploy is already done"),
            )
            .expect("insert");
        let embedding_store = dedicated_embedding_store(&dir);
        let pipeline = enqueue_pipeline(
            &store,
            &memory,
            crate::ScriptedExtractor::with_graphs(vec![alice_deploy_graph(), alice_deploy_graph()]),
            Some(Arc::clone(&embedding_store)),
        );

        for run in 1..=2 {
            let outcome = pipeline
                .run_digest(ENQUEUE_CHAT, 0)
                .await
                .expect("digest")
                .expect("non-empty tail");
            assert!(
                matches!(outcome, DigestOutcome::Extracted { .. }),
                "run {run} must extract"
            );
        }

        assert_eq!(
            embedding_store.list_edge_text_ids().expect("ids"),
            graph_edge_ids(&memory).await,
            "one row per graph edge, no duplicates"
        );
    }

    #[tokio::test]
    async fn an_edge_with_an_empty_description_is_not_harvested() {
        // Nothing to search: an edge with an empty edge_text never gets
        // a sidecar row. (The reconciliation's missing-from-sidecar
        // branch mirrors the skip, so the row never flaps.)
        let (dir, store, memory) = enqueue_fixtures();
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m1", "1001", "Alice", "I will deploy the fix tonight"),
            )
            .expect("insert");
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m2", "2002", "Bob", "the staging deploy is already done"),
            )
            .expect("insert");
        let embedding_store = dedicated_embedding_store(&dir);
        let mut graph = alice_deploy_graph();
        graph.edges[0].description = String::new();
        let pipeline = enqueue_pipeline(
            &store,
            &memory,
            crate::ScriptedExtractor::with_graphs(vec![graph]),
            Some(Arc::clone(&embedding_store)),
        );

        let outcome = pipeline
            .run_digest(ENQUEUE_CHAT, 0)
            .await
            .expect("digest")
            .expect("non-empty tail");
        assert!(matches!(outcome, DigestOutcome::Extracted { .. }));

        let ids = embedding_store.list_edge_text_ids().expect("ids");
        // The graph DOES hold the empty-text edge…
        assert!(graph_edge_ids(&memory).await.contains(&works_on_edge_id()));
        // …but the sidecar has no row for it, while every other graph
        // edge is mirrored.
        assert!(!ids.contains(&works_on_edge_id()));
        assert_eq!(ids.len(), graph_edge_ids(&memory).await.len() - 1);
    }

    #[tokio::test]
    async fn a_failed_edge_text_harvest_never_fails_the_digest() {
        // Decision 76 best-effort discipline, the same rig as the
        // decision-66 enqueue failure test: an embedding Store with two
        // open groups fails every single-group helper
        // (StoreError::AmbiguousGroup); the digest still succeeds and
        // the failure never enters the Phase 1 digest error taxonomy.
        let (dir, store, memory) = enqueue_fixtures();
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m1", "1001", "Alice", "I will deploy the fix tonight"),
            )
            .expect("insert");
        store
            .insert_message(
                ENQUEUE_CHAT,
                &inbound("m2", "2002", "Bob", "the staging deploy is already done"),
            )
            .expect("insert");
        let embedding_store = dedicated_embedding_store(&dir);
        embedding_store
            .open_group("edge_text_test_other")
            .expect("open");
        let pipeline = enqueue_pipeline(
            &store,
            &memory,
            crate::ScriptedExtractor::with_graphs(vec![alice_deploy_graph()]),
            Some(embedding_store),
        );

        let outcome = pipeline
            .run_digest(ENQUEUE_CHAT, 0)
            .await
            .expect("the digest is Ok despite the harvest failure")
            .expect("non-empty tail");
        match outcome {
            // Person + Concept + 2 Alias + MessageBatch.
            DigestOutcome::Extracted { node_count, .. } => assert_eq!(node_count, 5),
            other => panic!("expected Extracted, got {other:?}"),
        }
        assert_eq!(
            store
                .get_state(ENQUEUE_CHAT, "digest_failures_total")
                .expect("state"),
            None
        );
        // Nothing was written (a fresh one-group reader on the same
        // path — the pattern of the disabled-enqueue test).
        let reader = dedicated_embedding_store(&dir);
        assert!(reader.list_edge_text_ids().expect("ids").is_empty());
    }

    // ---- Decision 77: the `vector_resolution_*` counters bump ONCE,
    // post-commit, from the FINAL attempt's stats. ----

    /// A MemoryBackend wrapper that fails the FIRST
    /// `upsert_batch_with_registry` call and delegates everything else
    /// to the real backend (the double-count fixture: attempt 1
    /// resolves and then fails the graph commit, attempt 2 re-resolves
    /// and commits).
    struct FailOnceUpsert {
        inner: Arc<LbugBackend>,
        failures_left: std::sync::atomic::AtomicU32,
    }

    impl MemoryBackend for FailOnceUpsert {
        async fn ensure_schema(&self, chat_id: &str) -> tamako_memory::Result<()> {
            self.inner.ensure_schema(chat_id).await
        }

        async fn upsert_batch(
            &self,
            chat_id: &str,
            batch: &MemoryBatch,
        ) -> tamako_memory::Result<()> {
            self.inner.upsert_batch(chat_id, batch).await
        }

        async fn upsert_batch_with_registry(
            &self,
            chat_id: &str,
            batch: &MemoryBatch,
            single_value_predicates: &[String],
        ) -> tamako_memory::Result<tamako_memory::UpsertOutcome> {
            if self
                .failures_left
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed)
                > 0
            {
                return Err(tamako_memory::MemoryError::Backend(
                    "rigged commit failure".to_string(),
                ));
            }
            self.inner
                .upsert_batch_with_registry(chat_id, batch, single_value_predicates)
                .await
        }

        async fn checkpoint(&self, chat_id: &str) -> tamako_memory::Result<()> {
            self.inner.checkpoint(chat_id).await
        }

        async fn alias_targets(
            &self,
            chat_id: &str,
            alias_node_id: &str,
        ) -> tamako_memory::Result<Vec<tamako_memory::AliasTarget>> {
            self.inner.alias_targets(chat_id, alias_node_id).await
        }

        async fn neighbors(
            &self,
            chat_id: &str,
            node_id: &str,
        ) -> tamako_memory::Result<Vec<tamako_memory::NeighborEdge>> {
            self.inner.neighbors(chat_id, node_id).await
        }

        async fn node_content(
            &self,
            chat_id: &str,
            node_id: &str,
        ) -> tamako_memory::Result<Option<tamako_memory::NodeContent>> {
            self.inner.node_content(chat_id, node_id).await
        }

        async fn node_resolution_infos(
            &self,
            chat_id: &str,
            node_ids: &[String],
        ) -> tamako_memory::Result<Vec<(String, tamako_memory::NodeResolutionInfo)>> {
            self.inner.node_resolution_infos(chat_id, node_ids).await
        }

        async fn close(&self, chat_id: &str) -> tamako_memory::Result<()> {
            self.inner.close(chat_id).await
        }
    }

    #[tokio::test]
    async fn a_retried_attempt_bumps_the_vector_counters_once() {
        // Decision 77: attempt 1 resolves "Al" through the pre-screen
        // (a top-band Person binding — decision 79 (b) confirms it) and
        // then fails the graph commit; attempt 2 re-resolves (another
        // confirmation accept) and commits. The counters reflect ONE
        // resolution pass — the FINAL attempt's stats — not the
        // per-attempt sum.
        let (dir, store, memory) = enqueue_fixtures();
        al_messages(&store);
        seed_person_node(&memory, &person_id("1001"), "Alice").await;
        let embedding_store = dedicated_embedding_store(&dir);
        embedding_store
            .upsert_node_embedding(&person_id("1001"), &prescreen_unit_vector(0))
            .expect("seed embedding");

        // Two extraction answers (one per attempt); the provider
        // answers the batched pre-screen call twice, and each attempt's
        // Person top band makes ONE confirmation call.
        let provider = Arc::new(ScriptedEmbedder::with_batches(vec![
            vec![prescreen_unit_vector(0)],
            vec![prescreen_unit_vector(0)],
        ]));
        let accept = || crate::resolve::ConfirmationAnswer {
            same: true,
            reason: "scripted".to_string(),
        };
        let confirmer = Arc::new(crate::resolve::ScriptedConfirmer::with_answers(vec![
            accept(),
            accept(),
        ]));
        let failing_memory = FailOnceUpsert {
            inner: Arc::clone(&memory),
            failures_left: std::sync::atomic::AtomicU32::new(1),
        };
        let pipeline = AgentDigestPipeline::new(
            Arc::clone(&store),
            Arc::new(failing_memory),
            Arc::new(crate::ScriptedExtractor::with_graphs(vec![
                al_graph(),
                al_graph(),
            ])),
            PipelineConfig {
                max_retries: 3,
                retry_base_delay: Duration::from_millis(1),
            },
        )
        .with_embedding_store(Arc::clone(&embedding_store))
        .with_vector_prescreen(
            Arc::clone(&provider) as Arc<dyn CoreEmbeddingProvider>,
            Arc::clone(&confirmer) as Arc<dyn crate::resolve::ResolutionConfirmer>,
            VectorResolutionConfig::default(),
        );

        let outcome = pipeline
            .run_digest(ENQUEUE_CHAT, 0)
            .await
            .expect("the digest succeeds on the second attempt")
            .expect("non-empty tail");
        assert!(matches!(outcome, DigestOutcome::Extracted { .. }));

        // ONE resolution pass counted: the retried attempt's
        // confirmation accept did not double-count.
        assert_eq!(
            store
                .get_state(ENQUEUE_CHAT, "vector_resolution_confirmed_total")
                .expect("state"),
            Some("1".to_string())
        );
        // A top-band Person confirm is never an auto-match (decision
        // 79 (b)): the matched counter does not move.
        assert_eq!(
            store
                .get_state(ENQUEUE_CHAT, "vector_resolution_matched_total")
                .expect("state"),
            None
        );
        // The failed attempt itself is still counted.
        assert_eq!(
            store
                .get_state(ENQUEUE_CHAT, "digest_failures_total")
                .expect("state"),
            Some("1".to_string())
        );
        assert_eq!(provider.call_count(), 2);
    }
}
