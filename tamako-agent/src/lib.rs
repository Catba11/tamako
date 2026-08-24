//! tamako-agent: all LLM concerns of the Tamako bot, plus the
//! deterministic stages of the digest pipeline. Refer to specs.md
//! Sections 9, 10, and 13 and proposed-graph-database-specs.md
//! Section 7.
//!
//! The LLM concerns:
//!
//! - Extraction (`extract`, `prompt`, `rig_impl`): the digest pipeline
//!   turns a range of the raw message log into graph writes
//!   (specs.md Section 10; see the stage list below).
//! - The participation gate (`gate`): the binary participate/silent
//!   decision of the wake procedure (specs.md Section 9.6) over the
//!   cheap `gate_model`, structured output via JSON schema. Forced
//!   wakes (mention/reply, Section 8.1) bypass the gate. The live
//!   implementation is `RigGate`; tests use `ScriptedGate`.
//! - The relevance gate of the recall worker (`recall`): the shallow
//!   recall of the wake procedure (specs.md Section 9 step 2, Sections
//!   9.1-9.4). Deterministic candidate extraction, exact alias match,
//!   a cheap-model relevance decision, post-validated in plain Rust.
//!   The live implementation is `RigRelevanceGate`; tests use
//!   `ScriptedRelevanceGate`.
//! - Reply generation (`reply`): on participate, the reply text with
//!   the main `reply_model` over the live context (specs.md Section 9
//!   step 4). The live implementation is `RigReplyGenerator`; tests use
//!   `ScriptedReplyGenerator`.
//! - Warmup generation (`warmup`): the Phase 2 warmup trigger generates
//!   one casual opener with the REPLY purpose over the live context
//!   plus the sampled topic (specs.md Section 9.7, decision 78). The
//!   live implementation is `RigWarmupGenerator`; tests use
//!   `ScriptedWarmupGenerator`.
//! - The Rule C3 segmented summarizer (`summary`): when a digest
//!   removes a chunk from the live context, the actor summarizes the
//!   chunk and keeps the two newest summaries (specs.md Section 10,
//!   keep-two retention). The live implementation is `RigSummary`
//!   over the cheap `summary_model` endpoint, with the digest
//!   flat-label dialect as input (decision 61 divergence: the
//!   summarizer does not read the XML dialogue dialect).
//! - Media captioning (`caption`): media attachments are captioned at
//!   intake by the vision model of decision 82 (c) over the
//!   openai-compatible caption endpoint. The live implementation is
//!   `RigCaptionProvider`, wrapped in the `RetryCaptionProvider`
//!   retry/backoff decorator of decision 82 (d); the adapter drives
//!   the `tamako_core::caption::CaptionProvider` contract.
//!
//! Gate and reply failures are `CoreError::Wake`: log, skip this wake,
//! no crash (the next wake is the natural retry).
//!
//! ## The rig conversion seam
//!
//! tamako-core is model-agnostic: the wake contracts speak
//! `ContextMessage`. `reply::context_messages_to_rig` is the ONLY place
//! that converts core context messages to rig completion messages (the
//! M2-documented seam): item 0 (the Rule C4 preamble) becomes the rig
//! preamble, the rest map user/assistant.
//!
//! ## The digest pipeline stages
//!
//! The digest pipeline turns a range of the raw message log into graph
//! writes. The stages, in order:
//!
//! 1. Batch assembly (`pipeline`). The pipeline reads the raw log range
//!    `(last_digest_boundary_msg_id, tail]` from the store, builds the
//!    labeled messages of Section 7.2 step 4 of the database spec
//!    (`[{display_name} {HH:MM}] {text}`, UTC), and builds the mention and
//!    reply map of specs.md Section 10.1. The batch identifier
//!    (`tamako_memory::identifiers::batch_id`) is stable across retries
//!    (specs.md Section 10.3).
//! 2. Skeleton detection (`skeleton`). Section 7.2 rule 5: a batch of only
//!    emoji or greetings stores the MessageBatch skeleton without an
//!    extraction call.
//! 3. Extraction (`extract`, `prompt`, `rig_impl`). Section 7.3: the LLM
//!    returns a structured `KnowledgeGraph` object. The live
//!    implementation sends a rig completion request with an output schema
//!    (Anthropic native structured output). Tests use `ScriptedExtractor`.
//! 4. Post-validation (`validate`). Section 6.3: relationship names are
//!    checked in plain Rust. The prompt is never trusted.
//! 5. Entity resolution (`resolve`). Section 7.4 steps 1, 2, and 4 only
//!    (Phase 1 scope, dev-roadmap.md Section 3): mention binding, exact
//!    alias match, ambiguity fallback to the Alias node. Facts are written
//!    in the Phase 1 multi-value form of Section 7.5: `valid_at` set,
//!    `invalid_at` NULL.
//! 6. Write (`pipeline` through `tamako_memory::MemoryBackend`). One
//!    transaction per group, `CHECKPOINT` at the end (Section 7.6).
//! 7. Retry and dead-letter (`pipeline`). specs.md Section 10.3:
//!    exponential backoff with a stable batch identifier, dead-letter
//!    after `max_retries` attempts. A failed batch never blocks later
//!    batches.
//!
//! ## Provider configuration
//!
//! LLM access is endpoint-portable (specs.md Section 13). Every LLM call
//! uses one of two API families: `anthropic-compatible` or
//! `openai-compatible`. "Compatible" describes the wire format only,
//! never the vendor. The `endpoint` module implements this: it maps
//! config values (`LlmConfigValues`) and environment overrides onto one
//! resolved endpoint per purpose (`digest`, `gate`, `reply`,
//! `summary`), and builds rig clients for them.
//!
//! Config keys: `llm_api`, `llm_base_url`, `digest_model`, `gate_model`,
//! `reply_model`, `summary_model`, plus per-purpose overrides of
//! `llm_api` and `llm_base_url` (`digest_llm_api` etc. — mixed
//! deployments are legal). Environment overrides: `TAMAKO_LLM_API`,
//! `TAMAKO_LLM_BASE_URL`, `TAMAKO_DIGEST_MODEL`, `TAMAKO_GATE_MODEL`,
//! `TAMAKO_REPLY_MODEL`, `TAMAKO_SUMMARY_MODEL`; the summary purpose
//! additionally overrides the family and the base URL with
//! `TAMAKO_SUMMARY_LLM_API` and `TAMAKO_SUMMARY_LLM_BASE_URL`. The
//! environment wins. The default family is `anthropic-compatible`; the
//! default models are `claude-haiku-4-5` (digest, gate, summary) and
//! `claude-sonnet-4-5` (reply).
//!
//! API keys come from the environment only, never from a config file:
//! `ANTHROPIC_API_KEY` for anthropic-compatible endpoints,
//! `OPENAI_API_KEY` for openai-compatible endpoints. `tamako-agent`
//! never stores the key. Note: rig's own `ANTHROPIC_BASE_URL` /
//! `OPENAI_BASE_URL` env vars do NOT apply; `TAMAKO_LLM_BASE_URL` is the
//! Tamako-level override (see the `endpoint` module docs).
//!
//! ## Phase 2 hook points
//!
//! Embeddings and the sidecar vector index are Phase 2 (AGENT.md
//! Section 3). The hook point is marked in the pipeline at the place
//! where a batch was just upserted (`PHASE 2 HOOK` comment, Section 7.6
//! step 5).

pub mod caption;
pub mod endpoint;
pub mod extract;
pub mod gate;
pub mod graph;
pub mod merge_confirm;
pub mod pipeline;
pub mod prompt;
pub mod recall;
pub mod reply;
pub mod resolve;
pub mod rig_impl;
pub mod skeleton;
pub mod summary;
pub mod validate;
pub mod warmup;

pub use caption::{RetryCaptionProvider, RigCaptionProvider, CAPTION_PROMPT};
pub use endpoint::{
    CaptionEndpoint, EndpointClient, EndpointConfig, LlmApi, LlmConfigValues, LlmEndpoints,
    LlmPurpose, StructuredOutputMode,
};
pub use extract::{
    AgentError, BatchMessage, BindingSource, ExtractionInput, KnowledgeExtractor, MentionBinding,
    ScriptedExtractor,
};
pub use gate::{GateOutput, RigGate, ScriptedGate};
pub use graph::{ExtractedEdge, ExtractedNode, ExtractedNodeType, KnowledgeGraph};
pub use pipeline::{AgentDigestPipeline, PipelineConfig};
pub use prompt::{render_extraction_prompt, EXTRACTION_PREAMBLE};
pub use recall::{
    candidate_terms, render_recall_prompt, RecallCandidate, RecallSelection, RelevanceGate,
    RelevanceInput, RigRelevanceGate, ScriptedRelevanceGate, ShallowRecall, RECALL_PREAMBLE,
};
pub use reply::{context_messages_to_rig, RigReplyGenerator, ScriptedReplyGenerator};
pub use resolve::resolve_batch;
pub use rig_impl::{ExtractorConfig, RigExtractor};
pub use skeleton::is_skeleton_batch;
pub use summary::{render_summary_prompt, RigSummary, SummaryOutput, SUMMARY_PREAMBLE};
pub use validate::{
    is_snake_case_identifier, validate_relationship_name, RelationshipName,
    FALLBACK_RELATIONSHIP_NAME, RESERVED_RELATIONSHIP_NAMES,
};
pub use warmup::{render_warmup_instruction, RigWarmupGenerator, ScriptedWarmupGenerator};

/// Runs one inline LLM-seam call to completion, containing a panic
/// (decision 77, M3): a panicking embedding provider or resolution
/// confirmer degrades to its existing failure path (WARN + treat as a
/// failed call) instead of unwinding into the digest/wake task that
/// awaited it inline. The panic becomes the same failure class as a
/// provider error; the returned message carries the `task panicked`
/// marker, so the logs distinguish a panic from a provider failure.
///
/// This MIRRORS tamako-core's `contain_task_panic` (decision 65):
/// that helper is private to `tamako_core::actor` and tamako-core is a
/// sibling crate, so the std-only pattern is reproduced here rather
/// than reused. As there: `futures::FutureExt::catch_unwind` is the
/// textbook shape, but `futures` is not a dependency of the workspace
/// and the std covers the same ground (`poll_fn` +
/// `std::panic::catch_unwind`, no new dependency). The
/// `AssertUnwindSafe` is sound: after a caught panic the inner future
/// is NEVER polled again — the wrapper resolves with the error and
/// drops it — which is exactly the contract `futures`' own
/// `CatchUnwind` relies on.
pub(crate) async fn contain_task_panic<F, T>(body: F) -> Result<T, String>
where
    F: std::future::Future<Output = T>,
{
    let mut body = Box::pin(body);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body.as_mut().poll(cx))) {
            Ok(std::task::Poll::Ready(output)) => std::task::Poll::Ready(Ok(output)),
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Err(payload) => std::task::Poll::Ready(Err(format!(
                "task panicked: {}",
                panic_payload_text(payload.as_ref())
            ))),
        }
    })
    .await
}

/// Renders the payload of a caught panic: the `&str` or `String` of a
/// `panic!` message; anything else (a `panic_any` payload) reports its
/// kind, since the payload is opaque. (The same helper as
/// tamako-core's, kept private to the mirror above.)
fn panic_payload_text(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "a non-string panic payload".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::contain_task_panic;

    #[tokio::test]
    async fn contain_task_panic_converts_a_panic_into_a_marked_failure() {
        let outcome: Result<(), String> =
            contain_task_panic(async { panic!("the provider exploded") }).await;
        let message = outcome.expect_err("the panic becomes an Err");
        assert!(message.contains("task panicked"));
        assert!(message.contains("the provider exploded"));
    }

    #[tokio::test]
    async fn contain_task_panic_renders_a_non_string_payload_by_kind() {
        let outcome: Result<(), String> =
            contain_task_panic(async { std::panic::panic_any(42) }).await;
        let message = outcome.expect_err("the panic becomes an Err");
        assert_eq!(message, "task panicked: a non-string panic payload");
    }

    #[tokio::test]
    async fn contain_task_panic_passes_a_normal_result_through() {
        let outcome: Result<u32, String> = contain_task_panic(async { 41 + 1 }).await;
        assert_eq!(outcome, Ok(42));
    }
}
