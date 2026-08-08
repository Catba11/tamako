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
use tamako_core::digest::{DigestOutcome, DigestPipeline};
use tamako_memory::identifiers::{batch_id as message_batch_id, normalize};
use tamako_memory::{MemoryBackend, MemoryBatch};
use tamako_store::{MessageRow, Store, StoreError};
use time::macros::format_description;
use time::{OffsetDateTime, UtcOffset};

use crate::extract::{
    AgentError, BatchMessage, BindingSource, ExtractionInput, KnowledgeExtractor, MentionBinding,
};
use crate::resolve::{message_batch_node, resolve_batch};
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
    memory: Arc<M>,
    extractor: Arc<dyn KnowledgeExtractor>,
    config: PipelineConfig,
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
            memory,
            extractor,
            config,
        }
    }

    /// Runs one store call inside `tokio::task::spawn_blocking`
    /// (AGENT.md Section 6.2 — the same pattern the actor uses).
    async fn run_store<T>(
        &self,
        f: impl FnOnce(&Store) -> Result<T, StoreError> + Send + 'static,
    ) -> Result<T, AgentError>
    where
        T: Send + 'static,
    {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || f(&store))
            .await
            .map_err(|error| AgentError::Join(error.to_string()))?
            .map_err(AgentError::Store)
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
        let chat_id = chat_id.to_string();
        let key = key.to_string();
        if let Err(error) = self
            .run_store(move |store| store.increment_counter(&chat_id, &key, 1))
            .await
        {
            tracing::warn!(error = %error, "failed to increment a digest counter");
        }
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
        let batch = resolve_batch(
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
        )
        .await?;
        let node_count = batch.nodes.len();
        let edge_count = batch.edges.len();
        self.memory.upsert_batch(chat_id, &batch).await?;
        // PHASE 2 HOOK: write name/description embeddings to the sidecar
        // vector index here (Section 7.6 step 5).
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
                    tracing::info!(
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
                        "digest attempt failed; retrying with the same batch id"
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
}
