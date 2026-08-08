//! tamako-agent: all LLM concerns of the Tamako bot (Phase 1, M1), plus
//! the deterministic stages of the digest pipeline. Refer to specs.md
//! Section 10 and proposed-graph-database-specs.md Section 7.
//!
//! The crate implements the digest pipeline that turns a range of the raw
//! message log into graph writes. The stages, in order:
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
//! Extraction runs on Anthropic Claude through rig. The model defaults to
//! `claude-haiku-4-5` (cheap tier, constant
//! `rig::providers::anthropic::completion::CLAUDE_HAIKU_4_5`). Override
//! order (`ExtractorConfig::resolve`): the environment variable
//! `TAMAKO_DIGEST_MODEL` first, then the config-file key `digest_model`
//! (specs.md Section 13), then the default. The environment wins.
//!
//! The API key comes from `ANTHROPIC_API_KEY`: rig's `Client::from_env()`
//! reads it. `ANTHROPIC_BASE_URL` is respected by rig. `tamako-agent`
//! never reads the key itself.
//!
//! ## Phase 2 hook points
//!
//! Embeddings and the sidecar vector index are Phase 2 (AGENT.md
//! Section 3). The hook point is marked in the pipeline at the place
//! where a batch was just upserted (`PHASE 2 HOOK` comment, Section 7.6
//! step 5).

pub mod extract;
pub mod graph;
pub mod pipeline;
pub mod prompt;
pub mod resolve;
pub mod rig_impl;
pub mod skeleton;
pub mod validate;

pub use extract::{
    AgentError, BatchMessage, BindingSource, ExtractionInput, KnowledgeExtractor, MentionBinding,
    ScriptedExtractor,
};
pub use graph::{ExtractedEdge, ExtractedNode, ExtractedNodeType, KnowledgeGraph};
pub use pipeline::{AgentDigestPipeline, PipelineConfig};
pub use prompt::{render_extraction_prompt, EXTRACTION_PREAMBLE};
pub use resolve::resolve_batch;
pub use rig_impl::{ExtractorConfig, RigExtractor};
pub use skeleton::is_skeleton_batch;
pub use validate::{
    is_snake_case_identifier, validate_relationship_name, RelationshipName,
    FALLBACK_RELATIONSHIP_NAME, RESERVED_RELATIONSHIP_NAMES,
};
