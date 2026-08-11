//! tamako-agent: all LLM concerns of the Tamako bot, plus the
//! deterministic stages of the digest pipeline. Refer to specs.md
//! Sections 9, 10, and 13 and proposed-graph-database-specs.md
//! Section 7.
//!
//! The LLM concerns are three calls:
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
//! resolved endpoint per purpose (`digest`, `gate`, `reply`), and builds
//! rig clients for them.
//!
//! Config keys: `llm_api`, `llm_base_url`, `digest_model`, `gate_model`,
//! `reply_model`, plus per-purpose overrides of `llm_api` and
//! `llm_base_url` (`digest_llm_api` etc. — mixed deployments are legal).
//! Environment overrides: `TAMAKO_LLM_API`, `TAMAKO_LLM_BASE_URL`,
//! `TAMAKO_DIGEST_MODEL`, `TAMAKO_GATE_MODEL`, `TAMAKO_REPLY_MODEL`. The
//! environment wins. The default family is `anthropic-compatible`; the
//! default models are `claude-haiku-4-5` (digest, gate) and
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

pub mod endpoint;
pub mod extract;
pub mod gate;
pub mod graph;
pub mod pipeline;
pub mod prompt;
pub mod recall;
pub mod reply;
pub mod resolve;
pub mod rig_impl;
pub mod skeleton;
pub mod validate;

pub use endpoint::{
    EndpointClient, EndpointConfig, LlmApi, LlmConfigValues, LlmEndpoints, LlmPurpose,
    StructuredOutputMode,
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
    RelevanceInput, RigRelevanceGate, ScriptedRelevanceGate, ShallowRecall, INJECTION_TEXT_PREFIX,
    RECALL_PREAMBLE,
};
pub use reply::{context_messages_to_rig, RigReplyGenerator, ScriptedReplyGenerator};
pub use resolve::resolve_batch;
pub use rig_impl::{ExtractorConfig, RigExtractor};
pub use skeleton::is_skeleton_batch;
pub use validate::{
    is_snake_case_identifier, validate_relationship_name, RelationshipName,
    FALLBACK_RELATIONSHIP_NAME, RESERVED_RELATIONSHIP_NAMES,
};
