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
use tamako_memory::{MemoryBackend, MemoryBatch, NodeType};
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
        let embedding_store = self.embedding_store.as_ref()?;
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
    /// the graph write. specs.md Section 10.2.
    async fn extract_and_write(
        &self,
        chat_id: &str,
        input: &ExtractionInput,
        frame: &BatchFrame,
    ) -> Result<(usize, usize), AgentError> {
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
        // The step-3 outcome counters (Section 12). Best effort, after
        // the resolution they describe; the retry loop may re-resolve,
        // the same per-attempt counting as digest_failures_total.
        self.bump_vector_counters(chat_id, resolved.vector_stats)
            .await;
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
        Ok((node_count, edge_count))
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
                    |(node_count, edge_count)| AttemptSuccess::Extracted {
                        node_count,
                        edge_count,
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
                }) => {
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
    async fn the_auto_match_binds_and_increments_the_matched_counter() {
        // Decision 73 end to end: "Al" reaches step 3, the seeded
        // person node is a top-band hit (sim 1.0 >= 0.92), the entity
        // reuses the node id, no confirmation call runs, and the
        // state-table counter increments (the house counter mechanism
        // of specs.md Section 12).
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
                .get_state(ENQUEUE_CHAT, "vector_resolution_matched_total")
                .expect("state"),
            Some("1".to_string())
        );
        assert_eq!(
            store
                .get_state(ENQUEUE_CHAT, "vector_resolution_confirmed_total")
                .expect("state"),
            None
        );
        assert_eq!(confirmer.calls().len(), 0);
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
}
