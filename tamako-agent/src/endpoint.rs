//! LLM endpoint portability (specs.md Section 13).
//!
//! specs.md Section 13: "LLM access is endpoint-portable. Every LLM call
//! uses one of two API families: `anthropic-compatible` or
//! `openai-compatible`. 'Compatible' describes the wire format only, never
//! the vendor." The base URL and the model names are configuration items.
//! A purpose (`digest`, `gate`, `reply`, `summary`) may override
//! `llm_api` and `llm_base_url` individually; mixed deployments are
//! legal.
//!
//! Resolution inputs are the plain data struct [`LlmConfigValues`]. This
//! keeps resolution decoupled from tamako-core's TriggerConfig; a later
//! step maps TriggerConfig fields onto [`LlmConfigValues`].
//!
//! specs.md Section 13: "API keys come from the environment only, never
//! from a config file: `ANTHROPIC_API_KEY` for anthropic-compatible
//! endpoints, `OPENAI_API_KEY` for openai-compatible endpoints. These
//! variable names are the convention for the format, for third-party
//! endpoints as well."
//!
//! ## What rig 0.41 can and cannot do
//!
//! Verified against the rig-core 0.41.0 sources:
//!
//! - CAN: custom base URLs on every provider. All rig providers are type
//!   aliases over one generic `client::Client` with one shared builder:
//!   `.builder().api_key(key).base_url(url).build()`.
//! - CAN: custom model names. Model names are free-form strings. rig never
//!   validates them.
//! - CAN: Anthropic native structured output. `output_schema` maps to
//!   `output_config.format` without a beta header. `max_tokens` is
//!   mandatory on Anthropic; this layer always sets it.
//! - CAN: OpenAI structured output. `output_schema` maps to
//!   `response_format: { type: "json_schema", ... }`. LIMIT: rig hardcodes
//!   `strict: true`. An openai-compatible endpoint that does not support
//!   strict JSON schema may reject the request.
//! - CAN: OpenAI JSON-object mode. On the chat-completions path,
//!   `CompletionRequestBuilder::additional_params(serde_json::Value)` is
//!   merged and `#[serde(flatten)]`ed into the request body, so
//!   `response_format: { type: "json_object" }` is expressible without a
//!   schema. The `json_object` structured-output mode uses exactly this.
//! - CAN: custom default headers on every request of a client.
//!   `ClientBuilder::http_headers(HeaderMap)` replaces the client's
//!   default headers; `Client::post`/`get`/`post_sse` merge them into
//!   every request, and `build()` inserts the API-key auth header only
//!   when the map does not already carry it. The session-affinity
//!   headers of the resolved `session_id` use exactly this (decision
//!   84 (a): every request carries BOTH `x-opencode-session`, the
//!   Opencode Go gateway's session-affinity key, AND `x-session-id`,
//!   OpenRouter's sticky-routing key; dual-send is harmless — each
//!   gateway reads its own key).
//! - CANNOT: Anthropic JSON-object mode. The Anthropic Messages API has
//!   no json_object response format. On the anthropic-compatible family
//!   the `json_object` mode cannot be expressed; it degrades to
//!   prompt-only behavior (no `response_format` at all). The preambles
//!   state the exact output shape, so the degradation stays usable.
//!
//! ## Structured-output modes and the repair retry
//!
//! Live extraction against an openai-compatible endpoint that ignores or
//! mishandles `json_schema` fails intermittently ("missing field").
//! Two mitigation layers live here:
//!
//! 1. [`StructuredOutputMode`]: a per-purpose config of how the schema
//!    reaches the endpoint — `schema` (rig `output_schema`, the
//!    default), `json_object` (OpenAI-only `response_format` via
//!    `additional_params`; degrades to prompt-only on Anthropic), and
//!    `prompt_only` (no wire-level enforcement at all).
//! 2. The repair retry of [`EndpointClient::complete_structured`]: when
//!    the response parses as JSON but fails the typed parse (schema
//!    validation), ONE repair completion runs on the same endpoint with
//!    the broken JSON, the validation error, and the schema. A failed
//!    repair returns the ORIGINAL error, so the caller's backoff and
//!    dead-letter discipline applies unchanged.
//! - CHOICE: the default `openai::Client` speaks the Responses API
//!   (`POST {base}/responses`), which is first-party only in practice. For
//!   openai-compatible endpoints (vLLM, OpenRouter, self-hosted) this
//!   layer uses `openai::CompletionsClient`, which speaks
//!   `POST {base}/chat/completions`.
//! - QUIRK: base URL handling differs per family. Anthropic normalizes
//!   the base URL (strips a trailing `/`, `/v1/messages`, `/messages`, or
//!   `v1` suffix) and posts to `{base}/v1/messages`. OpenAI uses the base
//!   URL verbatim; the canonical default includes `/v1`.
//!
//! ## Deliberate decision: base URL env vars of rig do not apply
//!
//! This layer builds clients with the explicit builder, not with
//! `Client::from_env()`. Therefore rig's `ANTHROPIC_BASE_URL` and
//! `OPENAI_BASE_URL` env vars do NOT apply to Tamako-configured
//! endpoints. `TAMAKO_LLM_BASE_URL` is the Tamako-level override. This
//! keeps the base URL under one configuration scheme (specs.md
//! Section 13).
//!
//! ## Reasoning-markup sanitation (decision 93)
//!
//! Every completion's extracted text passes [`strip_reasoning_markup`]
//! before any purpose sees it (reply, gates, summary, extraction — and
//! captions via `caption.rs`). A serving stack that splits reasoning
//! from content with a NAIVE first-`</think>` string match breaks when
//! the reasoning itself mentions the tag — observed live: the group
//! discussed a glitchy AI output, the reasoning quoted `</think>`, and
//! the content field carried the reasoning tail, the stray closer, and
//! the answer. The stripper is fail-closed: balanced
//! `<think>...</think>` regions strip; an orphan `</think>` drops
//! everything up to and including it; an unclosed `<think>` voids the
//! remainder, so an all-reasoning response maps to the same
//! `AgentError::Extraction` as a text-less response and every caller's
//! backoff discipline applies unchanged. One WARN per non-trivial
//! strip. Providers that return reasoning on a separate response field
//! (`reasoning_content` / `reasoning`) were already safe: rig maps
//! them to `AssistantContent::Reasoning`, which the text extraction
//! never selects.
//!
//! ## Per-attempt timeout (H4b)
//!
//! Every completion attempt is bounded by [`ENDPOINT_TIMEOUT`] (a code
//! constant, deliberately no config key). The bound is PER ATTEMPT:
//! the first call and the one repair retry of
//! [`EndpointClient::complete_structured`] each get their own window.
//! A stalled completion maps to `AgentError::Extraction` with an
//! `endpoint timeout after` prefix, so every caller's existing failure
//! semantics apply unchanged (digest backoff/dead-letter, wake skip,
//! summary deferral).

use std::time::Duration;

use rig::client::{CompletionClient, EmbeddingsClient as _};
use rig::completion::{AssistantContent, Message};
use rig::embeddings::EmbeddingModel as _;
use rig::providers::{anthropic, openai};

use crate::extract::AgentError;

/// Environment override of the API family (specs.md Section 13).
pub const LLM_API_ENV_VAR: &str = "TAMAKO_LLM_API";

/// Environment override of the base URL (specs.md Section 13).
pub const LLM_BASE_URL_ENV_VAR: &str = "TAMAKO_LLM_BASE_URL";

/// Environment override of the digest model (specs.md Section 13).
pub const DIGEST_MODEL_ENV_VAR: &str = "TAMAKO_DIGEST_MODEL";

/// Environment override of the gate model (specs.md Section 13).
pub const GATE_MODEL_ENV_VAR: &str = "TAMAKO_GATE_MODEL";

/// Environment override of the reply model (specs.md Section 13).
pub const REPLY_MODEL_ENV_VAR: &str = "TAMAKO_REPLY_MODEL";

/// Environment override of the summary model (specs.md Section 13).
pub const SUMMARY_MODEL_ENV_VAR: &str = "TAMAKO_SUMMARY_MODEL";

/// Per-purpose API-key override of the digest purpose (decision 87,
/// ENVIRONMENT-ONLY — no TOML key: secrets never enter the config file,
/// the specs.md Section 13 invariant "API keys come from the environment
/// only" is preserved and extended). Resolution chain: this var → the
/// family var (`ANTHROPIC_API_KEY` / `OPENAI_API_KEY`) → a missing-key
/// `ProviderConfig` error. An empty string counts as unset and falls
/// through to the family key (the `env_value` discipline). The use case:
/// pointing one purpose at a DIFFERENT provider of the SAME API family.
pub const DIGEST_LLM_API_KEY_ENV_VAR: &str = "TAMAKO_DIGEST_LLM_API_KEY";

/// Per-purpose API-key override of the gate purpose (decision 87). Refer
/// to [`DIGEST_LLM_API_KEY_ENV_VAR`].
pub const GATE_LLM_API_KEY_ENV_VAR: &str = "TAMAKO_GATE_LLM_API_KEY";

/// Per-purpose API-key override of the reply purpose (decision 87).
/// Refer to [`DIGEST_LLM_API_KEY_ENV_VAR`].
pub const REPLY_LLM_API_KEY_ENV_VAR: &str = "TAMAKO_REPLY_LLM_API_KEY";

/// Per-purpose API-key override of the summary purpose (decision 87).
/// Refer to [`DIGEST_LLM_API_KEY_ENV_VAR`].
pub const SUMMARY_LLM_API_KEY_ENV_VAR: &str = "TAMAKO_SUMMARY_LLM_API_KEY";

/// Environment override of the summary API family (specs.md Section
/// 13). The summary
/// purpose alone has per-purpose api/base URL env vars (the S3
/// deployment contract); the other purposes keep the global
/// `TAMAKO_LLM_API` / `TAMAKO_LLM_BASE_URL` overrides only.
pub const SUMMARY_LLM_API_ENV_VAR: &str = "TAMAKO_SUMMARY_LLM_API";

/// Environment override of the summary base URL (specs.md Section
/// 13). Refer to
/// [`SUMMARY_LLM_API_ENV_VAR`].
pub const SUMMARY_LLM_BASE_URL_ENV_VAR: &str = "TAMAKO_SUMMARY_LLM_BASE_URL";

/// Environment override of the session-id PREFIX (decision 84 (d)):
/// `llm_session_id` stays global-only as the operator-chosen prefix.
/// The full header value is `{prefix}-{suffix}`, where the suffix is
/// the persisted per-(group, purpose) mint (decision 84 (b); refer to
/// [`EndpointConfig::with_session_suffix`]). BOTH affinity headers
/// carry the full value: `x-opencode-session` (Opencode Go gateway
/// session affinity) and `x-session-id` (OpenRouter sticky routing)
/// — decision 84 (a).
pub const LLM_SESSION_ID_ENV_VAR: &str = "TAMAKO_LLM_SESSION_ID";

/// The default session-id PREFIX (specs.md Section 13's
/// `llm_session_id`). Decision 84 (b)/(d): one deployment, one
/// operator-chosen prefix; the per-(group, purpose) disambiguation
/// suffix is machine-generated and persisted.
pub const DEFAULT_SESSION_ID: &str = "tamako";

/// The session-affinity purpose string of the caption endpoint
/// (decision 84 (b)): the six purposes are the four completion
/// purposes ([`LlmPurpose::as_str`]) plus `caption` and `embedding`.
/// The binary keys the store's `get_or_insert_session_suffix` with
/// this value; do NOT hardcode the string there.
pub const CAPTION_SESSION_PURPOSE: &str = "caption";

/// The session-affinity purpose string of the embedding endpoint
/// (decision 84 (b)); refer to [`CAPTION_SESSION_PURPOSE`].
pub const EMBEDDING_SESSION_PURPOSE: &str = "embedding";

/// Global environment override of the structured-output mode
/// (`schema` | `json_object` | `prompt_only`). The per-purpose env vars
/// take precedence; refer to [`StructuredOutputMode`].
pub const STRUCTURED_OUTPUT_ENV_VAR: &str = "TAMAKO_STRUCTURED_OUTPUT";

/// Environment override of the digest structured-output mode.
pub const DIGEST_STRUCTURED_OUTPUT_ENV_VAR: &str = "TAMAKO_DIGEST_STRUCTURED_OUTPUT";

/// Environment override of the gate structured-output mode.
pub const GATE_STRUCTURED_OUTPUT_ENV_VAR: &str = "TAMAKO_GATE_STRUCTURED_OUTPUT";

/// Environment override of the reply structured-output mode.
pub const REPLY_STRUCTURED_OUTPUT_ENV_VAR: &str = "TAMAKO_REPLY_STRUCTURED_OUTPUT";

/// Environment override of the summary structured-output mode.
pub const SUMMARY_STRUCTURED_OUTPUT_ENV_VAR: &str = "TAMAKO_SUMMARY_STRUCTURED_OUTPUT";

/// Environment override of the embedding model (current-state.md
/// decision 66). Global-only: one embedding endpoint per deployment,
/// no per-purpose or per-group variant.
pub const EMBEDDING_MODEL_ENV_VAR: &str = "TAMAKO_EMBEDDING_MODEL";

/// Environment override of the embedding base URL (decision 66).
/// Global-only; refer to [`EMBEDDING_MODEL_ENV_VAR`].
pub const EMBEDDING_BASE_URL_ENV_VAR: &str = "TAMAKO_EMBEDDING_BASE_URL";

/// Environment override of the caption model (current-state.md
/// decision 82 (c)). Global-only: one caption endpoint per deployment
/// (the same standing as the embedding endpoint, decision 66).
pub const CAPTION_MODEL_ENV_VAR: &str = "TAMAKO_CAPTION_MODEL";

/// Environment override of the caption base URL (decision 82 (c)).
/// Global-only; refer to [`CAPTION_MODEL_ENV_VAR`].
pub const CAPTION_BASE_URL_ENV_VAR: &str = "TAMAKO_CAPTION_BASE_URL";

/// API key env var of the anthropic-compatible family (specs.md
/// Section 13: API keys come from the environment only).
pub const ANTHROPIC_API_KEY_ENV_VAR: &str = "ANTHROPIC_API_KEY";

/// API key env var of the openai-compatible family (specs.md Section 13).
pub const OPENAI_API_KEY_ENV_VAR: &str = "OPENAI_API_KEY";

/// The default digest model (specs.md Section 13: `claude-haiku-4-5`).
pub const DEFAULT_DIGEST_MODEL: &str = anthropic::completion::CLAUDE_HAIKU_4_5;

/// The default gate model (specs.md Section 13: `claude-haiku-4-5`).
pub const DEFAULT_GATE_MODEL: &str = anthropic::completion::CLAUDE_HAIKU_4_5;

/// The default reply model (specs.md Section 13: `claude-sonnet-4-5`).
/// rig-core 0.41.0 has no `CLAUDE_SONNET_4_5` constant (only
/// `CLAUDE_SONNET_4_6`), so this is a string literal.
pub const DEFAULT_REPLY_MODEL: &str = "claude-sonnet-4-5";

/// The default summary model (cheap tier: the segmented summarizer of
/// the Rule C3 removed chunk, specs.md Section 10). The same cheap
/// model as the gate.
pub const DEFAULT_SUMMARY_MODEL: &str = anthropic::completion::CLAUDE_HAIKU_4_5;

/// The default embedding model (current-state.md decision 81).
/// Decision 66 first pinned `qwen/qwen3-embedding-8b`; decision 81
/// switches to `google/gemini-embedding-2` (the OpenRouter id, served
/// by google-vertex with ZDR).
pub const DEFAULT_EMBEDDING_MODEL: &str = "google/gemini-embedding-2";

/// The default embedding base URL (decision 66; unchanged by decision
/// 81 — the gemini-embedding-2 id is served via OpenRouter):
/// OpenRouter's openai-compatible endpoint. rig uses the base URL
/// verbatim, so the request lands on `{base}/embeddings`.
pub const DEFAULT_EMBEDDING_BASE_URL: &str = "https://openrouter.ai/api/v1";

/// The default caption model (current-state.md decision 82 (c)):
/// MiniMax M3, the vision model that captions media at intake, served
/// via OpenRouter (the first-party MiniMax endpoint is NOT ZDR; the
/// account's ZDR-only policy routes to a third-party ZDR provider).
pub const DEFAULT_CAPTION_MODEL: &str = "minimax/minimax-m3";

/// The default caption base URL (decision 82 (c)): OpenRouter's
/// openai-compatible endpoint — the SAME default as
/// [`DEFAULT_EMBEDDING_BASE_URL`]. rig uses the base URL verbatim, so
/// the caption request lands on `{base}/chat/completions`.
pub const DEFAULT_CAPTION_BASE_URL: &str = "https://openrouter.ai/api/v1";

/// The pinned embedding dimension (decision 81). Decision 66 first
/// pinned 4096 (qwen3-embedding-8b); decision 81 re-pins to 3072,
/// google/gemini-embedding-2's NATIVE dimension (the top of the
/// Matryoshka ladder — the response is 3072 whether or not the
/// `dimensions` request parameter is honored, so the hard length pin
/// is the guard). rig sends it as the openai-compatible `dimensions`
/// request field (`embedding_model_with_ndims`); a response vector of
/// any other length is a hard error (the store schema pins the
/// dimension).
pub const EMBEDDING_DIMS: usize = 3072;

/// The per-attempt completion timeout (H4b). One bound around every
/// endpoint completion attempt: the first call AND the one repair
/// retry of `complete_structured` each get their own window (both go
/// through `EndpointClient::complete`).
///
/// 900 s is generous on purpose. Live digests complete in ~20 s, but
/// reasoning models burn reasoning tokens before any content, and with
/// `max_tokens` up to 102400 a slow-but-progressing reasoning response
/// legitimately runs for minutes; at peak hours a loaded endpoint
/// stretches this further. The timeout only guards against a STALLED
/// completion (no response at all); it cannot false-positive on a
/// healthy slow endpoint, while a hung connection is bounded per
/// attempt.
///
/// Raised 300 s -> 900 s (operator ruling, 2026-08-23): at peak hours a
/// large digest batch against a loaded endpoint can legitimately exceed
/// 5 minutes, and a false-positive timeout wastes the whole batch
/// attempt. The bound is shared by every purpose (digest, gate, reply,
/// summary, embedding, caption): a hung wake/reply call now also takes
/// up to 15 minutes to be declared dead — accepted, since the wake
/// failure discipline requeues and a reply that late was already
/// worthless.
///
/// There is deliberately no config key: the bound is a code constant.
/// Tests inject a smaller value through `EndpointClient::with_timeout`.
pub const ENDPOINT_TIMEOUT: Duration = Duration::from_secs(900);

/// The API family of an endpoint (specs.md Section 13). The family
/// selects the wire format only, not the vendor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LlmApi {
    /// The Anthropic Messages API wire format.
    AnthropicCompatible,
    /// The OpenAI chat completions wire format.
    OpenAiCompatible,
}

impl LlmApi {
    /// The spec string of the family.
    pub fn as_str(&self) -> &'static str {
        match self {
            LlmApi::AnthropicCompatible => "anthropic-compatible",
            LlmApi::OpenAiCompatible => "openai-compatible",
        }
    }

    /// The env var that carries the API key of the family (specs.md
    /// Section 13: API keys come from the environment only).
    fn api_key_env_var(&self) -> &'static str {
        match self {
            LlmApi::AnthropicCompatible => ANTHROPIC_API_KEY_ENV_VAR,
            LlmApi::OpenAiCompatible => OPENAI_API_KEY_ENV_VAR,
        }
    }
}

/// Parses a family string. Accepts exactly the spec strings
/// `anthropic-compatible` and `openai-compatible`. An unknown family is
/// a configuration error, never a silent default.
impl std::str::FromStr for LlmApi {
    type Err = AgentError;

    fn from_str(value: &str) -> Result<Self, AgentError> {
        match value {
            "anthropic-compatible" => Ok(LlmApi::AnthropicCompatible),
            "openai-compatible" => Ok(LlmApi::OpenAiCompatible),
            other => Err(AgentError::ProviderConfig(format!(
                "unknown llm_api family {other:?}: expected \"anthropic-compatible\" or \"openai-compatible\""
            ))),
        }
    }
}

impl std::fmt::Display for LlmApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How a structured call enforces its output shape on the wire (module
/// docs). Endpoints that ignore or mishandle `json_schema` need a
/// weaker mode; the repair retry of `complete_structured` covers the
/// remaining failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StructuredOutputMode {
    /// rig `output_schema`: Anthropic native structured output, OpenAI
    /// `json_schema` response format (rig hardcodes `strict: true`).
    /// The default.
    #[default]
    Schema,
    /// OpenAI only: `response_format: { type: "json_object" }` via
    /// `additional_params`. No schema on the wire. Attached PER CALL,
    /// only when the call carries a schema (M1: a schema-less
    /// plain-text call sends no response_format). On the Anthropic
    /// family this mode cannot be expressed and degrades to
    /// prompt-only behavior (module docs).
    JsonObject,
    /// No wire-level enforcement: the preamble alone carries the
    /// output shape.
    PromptOnly,
}

impl StructuredOutputMode {
    /// The config string of the mode.
    pub fn as_str(&self) -> &'static str {
        match self {
            StructuredOutputMode::Schema => "schema",
            StructuredOutputMode::JsonObject => "json_object",
            StructuredOutputMode::PromptOnly => "prompt_only",
        }
    }
}

/// Parses a mode string. Accepts exactly `schema`, `json_object`, and
/// `prompt_only`. An unknown mode is a configuration error, never a
/// silent default (the same discipline as `LlmApi`).
impl std::str::FromStr for StructuredOutputMode {
    type Err = AgentError;

    fn from_str(value: &str) -> Result<Self, AgentError> {
        match value {
            "schema" => Ok(StructuredOutputMode::Schema),
            "json_object" => Ok(StructuredOutputMode::JsonObject),
            "prompt_only" => Ok(StructuredOutputMode::PromptOnly),
            other => Err(AgentError::ProviderConfig(format!(
                "unknown structured_output mode {other:?}: expected \"schema\", \"json_object\", or \"prompt_only\""
            ))),
        }
    }
}

impl std::fmt::Display for StructuredOutputMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The LLM purposes of the bot (specs.md Section 13). Each purpose may
/// override `llm_api` and `llm_base_url` individually.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LlmPurpose {
    /// Digest extraction (specs.md Section 10).
    Digest,
    /// Participation decision (specs.md Section 9.6).
    Gate,
    /// Reply generation (specs.md Section 9, step 4).
    Reply,
    /// Segmented context summarization of the Rule C3 removed chunk
    /// (specs.md Section 10, keep-two retention). Cheap tier.
    Summary,
}

impl LlmPurpose {
    /// The lowercase name of the purpose.
    pub fn as_str(&self) -> &'static str {
        match self {
            LlmPurpose::Digest => "digest",
            LlmPurpose::Gate => "gate",
            LlmPurpose::Reply => "reply",
            LlmPurpose::Summary => "summary",
        }
    }

    /// The env var that overrides the model of the purpose.
    fn model_env_var(&self) -> &'static str {
        match self {
            LlmPurpose::Digest => DIGEST_MODEL_ENV_VAR,
            LlmPurpose::Gate => GATE_MODEL_ENV_VAR,
            LlmPurpose::Reply => REPLY_MODEL_ENV_VAR,
            LlmPurpose::Summary => SUMMARY_MODEL_ENV_VAR,
        }
    }

    /// The default model of the purpose (specs.md Section 13).
    fn default_model(&self) -> &'static str {
        match self {
            LlmPurpose::Digest => DEFAULT_DIGEST_MODEL,
            LlmPurpose::Gate => DEFAULT_GATE_MODEL,
            LlmPurpose::Reply => DEFAULT_REPLY_MODEL,
            LlmPurpose::Summary => DEFAULT_SUMMARY_MODEL,
        }
    }

    /// The env var that overrides the structured-output mode of the
    /// purpose.
    fn structured_output_env_var(&self) -> &'static str {
        match self {
            LlmPurpose::Digest => DIGEST_STRUCTURED_OUTPUT_ENV_VAR,
            LlmPurpose::Gate => GATE_STRUCTURED_OUTPUT_ENV_VAR,
            LlmPurpose::Reply => REPLY_STRUCTURED_OUTPUT_ENV_VAR,
            LlmPurpose::Summary => SUMMARY_STRUCTURED_OUTPUT_ENV_VAR,
        }
    }

    /// The per-purpose API-key env var of decision 87 (ENVIRONMENT-ONLY;
    /// no TOML key). Resolution chain in [`EndpointClient::build`]: this
    /// var → the family var ([`LlmApi::api_key_env_var`]) → a missing-key
    /// `ProviderConfig` error naming both. Empty counts as unset.
    fn api_key_env_var(&self) -> &'static str {
        match self {
            LlmPurpose::Digest => DIGEST_LLM_API_KEY_ENV_VAR,
            LlmPurpose::Gate => GATE_LLM_API_KEY_ENV_VAR,
            LlmPurpose::Reply => REPLY_LLM_API_KEY_ENV_VAR,
            LlmPurpose::Summary => SUMMARY_LLM_API_KEY_ENV_VAR,
        }
    }

    /// The env var that overrides the API family of the purpose.
    /// `None` for digest, gate, and reply: those purposes keep the
    /// global `TAMAKO_LLM_API` override only (refer to
    /// [`SUMMARY_LLM_API_ENV_VAR`]).
    fn llm_api_env_var(&self) -> Option<&'static str> {
        match self {
            LlmPurpose::Summary => Some(SUMMARY_LLM_API_ENV_VAR),
            _ => None,
        }
    }

    /// The env var that overrides the base URL of the purpose. `None`
    /// for digest, gate, and reply: those purposes keep the global
    /// `TAMAKO_LLM_BASE_URL` override only (refer to
    /// [`SUMMARY_LLM_BASE_URL_ENV_VAR`]).
    fn llm_base_url_env_var(&self) -> Option<&'static str> {
        match self {
            LlmPurpose::Summary => Some(SUMMARY_LLM_BASE_URL_ENV_VAR),
            _ => None,
        }
    }
}

/// The resolved endpoint of one purpose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointConfig {
    /// The API family (wire format).
    pub api: LlmApi,
    /// The base URL. `None` selects the canonical default of the family.
    pub base_url: Option<String>,
    /// The model name.
    pub model: String,
    /// The structured-output mode of the purpose
    /// ([`StructuredOutputMode`], module docs). Default `schema`.
    pub structured_output: StructuredOutputMode,
    /// The resolved session id, sent as BOTH the `x-opencode-session`
    /// and the `x-session-id` header on every request of the client
    /// (decision 84 (a)). [`LlmEndpoints::resolve`] yields the
    /// global-only PREFIX; [`EndpointConfig::with_session_suffix`]
    /// then joins the per-(group, purpose) suffix to
    /// `{prefix}-{suffix}` (decision 84 (b)). Never empty: resolution
    /// falls back to [`DEFAULT_SESSION_ID`].
    pub session_id: String,
}

impl EndpointConfig {
    /// Applies the per-(group, purpose) affinity suffix (decision
    /// 84 (b)): the session id becomes `{prefix}-{suffix}`. Called by
    /// the binary at the per-group service-build site after
    /// `resolve` (which is group-agnostic and yields the PREFIX).
    /// Group-less callers (`--status`, resolve-only paths) never call
    /// this; their session id stays the bare prefix.
    ///
    /// The suffix is the persisted per-(group, purpose) mint of
    /// decision 84 (b); the caller obtains it from the store (this
    /// layer stays store-agnostic).
    pub fn with_session_suffix(mut self, suffix: &str) -> Self {
        self.session_id = format!("{}-{suffix}", self.session_id);
        self
    }
}

/// Plain config-file values for endpoint resolution (specs.md
/// Section 13). All fields optional. This struct decouples resolution
/// from tamako-core's TriggerConfig; a later step maps TriggerConfig
/// fields onto these values.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LlmConfigValues {
    /// Global API family (`llm_api`).
    pub llm_api: Option<String>,
    /// Global base URL (`llm_base_url`).
    pub llm_base_url: Option<String>,
    /// Global session-id PREFIX (`llm_session_id`; decision 84 (d):
    /// global-only, no per-purpose or per-group variant).
    pub llm_session_id: Option<String>,
    /// Digest model (`digest_model`).
    pub digest_model: Option<String>,
    /// Gate model (`gate_model`).
    pub gate_model: Option<String>,
    /// Reply model (`reply_model`).
    pub reply_model: Option<String>,
    /// Summary model (`summary_model`; specs.md Section 13).
    pub summary_model: Option<String>,
    /// Digest-specific API family (`digest_llm_api`).
    pub digest_llm_api: Option<String>,
    /// Digest-specific base URL (`digest_llm_base_url`).
    pub digest_llm_base_url: Option<String>,
    /// Gate-specific API family (`gate_llm_api`).
    pub gate_llm_api: Option<String>,
    /// Gate-specific base URL (`gate_llm_base_url`).
    pub gate_llm_base_url: Option<String>,
    /// Reply-specific API family (`reply_llm_api`).
    pub reply_llm_api: Option<String>,
    /// Reply-specific base URL (`reply_llm_base_url`).
    pub reply_llm_base_url: Option<String>,
    /// Summary-specific API family (`summary_llm_api`, reported for
    /// spec backfill).
    pub summary_llm_api: Option<String>,
    /// Summary-specific base URL (`summary_llm_base_url`, reported for
    /// spec backfill).
    pub summary_llm_base_url: Option<String>,
    /// Global structured-output mode (`structured_output`).
    pub structured_output: Option<String>,
    /// Digest-specific structured-output mode
    /// (`digest_structured_output`).
    pub digest_structured_output: Option<String>,
    /// Gate-specific structured-output mode (`gate_structured_output`).
    pub gate_structured_output: Option<String>,
    /// Reply-specific structured-output mode
    /// (`reply_structured_output`).
    pub reply_structured_output: Option<String>,
    /// Summary-specific structured-output mode
    /// (`summary_structured_output`; specs.md Section 13).
    pub summary_structured_output: Option<String>,
    /// The embedding model (`embedding_model`; decision 66,
    /// global-only). The binary always maps the resolved config value
    /// here; `None` selects [`DEFAULT_EMBEDDING_MODEL`].
    pub embedding_model: Option<String>,
    /// The embedding base URL (`embedding_llm_base_url`; decision 66,
    /// global-only). `None` selects [`DEFAULT_EMBEDDING_BASE_URL`].
    pub embedding_llm_base_url: Option<String>,
    /// The caption model (`caption_model`; decision 82 (c),
    /// global-only). `None` selects [`DEFAULT_CAPTION_MODEL`].
    pub caption_model: Option<String>,
    /// The caption base URL (`caption_llm_base_url`; decision 82 (c),
    /// global-only). `None` selects [`DEFAULT_CAPTION_BASE_URL`].
    pub caption_llm_base_url: Option<String>,
}

impl LlmConfigValues {
    /// The purpose-specific API family override, when set.
    fn purpose_llm_api(&self, purpose: LlmPurpose) -> Option<&str> {
        let value = match purpose {
            LlmPurpose::Digest => self.digest_llm_api.as_deref(),
            LlmPurpose::Gate => self.gate_llm_api.as_deref(),
            LlmPurpose::Reply => self.reply_llm_api.as_deref(),
            LlmPurpose::Summary => self.summary_llm_api.as_deref(),
        };
        value.filter(|value| !value.is_empty())
    }

    /// The purpose-specific base URL override, when set.
    fn purpose_llm_base_url(&self, purpose: LlmPurpose) -> Option<&str> {
        let value = match purpose {
            LlmPurpose::Digest => self.digest_llm_base_url.as_deref(),
            LlmPurpose::Gate => self.gate_llm_base_url.as_deref(),
            LlmPurpose::Reply => self.reply_llm_base_url.as_deref(),
            LlmPurpose::Summary => self.summary_llm_base_url.as_deref(),
        };
        value.filter(|value| !value.is_empty())
    }

    /// The model of the purpose, when set.
    fn purpose_model(&self, purpose: LlmPurpose) -> Option<&str> {
        let value = match purpose {
            LlmPurpose::Digest => self.digest_model.as_deref(),
            LlmPurpose::Gate => self.gate_model.as_deref(),
            LlmPurpose::Reply => self.reply_model.as_deref(),
            LlmPurpose::Summary => self.summary_model.as_deref(),
        };
        value.filter(|value| !value.is_empty())
    }

    /// The purpose-specific structured-output mode override, when set.
    fn purpose_structured_output(&self, purpose: LlmPurpose) -> Option<&str> {
        let value = match purpose {
            LlmPurpose::Digest => self.digest_structured_output.as_deref(),
            LlmPurpose::Gate => self.gate_structured_output.as_deref(),
            LlmPurpose::Reply => self.reply_structured_output.as_deref(),
            LlmPurpose::Summary => self.summary_structured_output.as_deref(),
        };
        value.filter(|value| !value.is_empty())
    }
}

/// Reads an env var. The value is ASCII-trimmed; an empty or
/// whitespace-only value counts as unset (decision 77, S5-L3).
/// `pub(crate)` for the caption provider (`crate::caption`), which
/// reads `OPENAI_API_KEY` through the same idiom.
pub(crate) fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim_ascii().to_string())
        .filter(|value| !value.is_empty())
}

/// The resolved endpoints of the four purposes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmEndpoints {
    /// The digest extraction endpoint.
    pub digest: EndpointConfig,
    /// The participation gate endpoint.
    pub gate: EndpointConfig,
    /// The reply generation endpoint.
    pub reply: EndpointConfig,
    /// The segmented summarizer endpoint (the Rule C3 removed chunk).
    pub summary: EndpointConfig,
}

impl LlmEndpoints {
    /// Resolves the four endpoints from the config values and the
    /// environment (specs.md Section 13). Precedence per purpose:
    ///
    /// - API family: env `TAMAKO_LLM_API` → purpose config
    ///   (`digest_llm_api` etc.) → global config `llm_api` → default
    ///   `anthropic-compatible`. The summary purpose adds one higher
    ///   step: `TAMAKO_SUMMARY_LLM_API` wins over `TAMAKO_LLM_API`.
    ///   An unparsable string (env or config) is
    ///   `AgentError::ProviderConfig`, never a silent default.
    /// - Base URL: env `TAMAKO_LLM_BASE_URL` → purpose config → global
    ///   config `llm_base_url` → `None` (the canonical default of the
    ///   family). The summary purpose adds one higher step:
    ///   `TAMAKO_SUMMARY_LLM_BASE_URL` wins over `TAMAKO_LLM_BASE_URL`.
    /// - Model: purpose env (`TAMAKO_DIGEST_MODEL` etc.) → purpose
    ///   config (`digest_model` etc.) → the purpose default.
    /// - Structured-output mode: purpose env
    ///   (`TAMAKO_DIGEST_STRUCTURED_OUTPUT` etc.) → global env
    ///   `TAMAKO_STRUCTURED_OUTPUT` → purpose config
    ///   (`digest_structured_output` etc.) → global config
    ///   `structured_output` → default `schema`. An unparsable string
    ///   (env or config) is `AgentError::ProviderConfig`, never a
    ///   silent default.
    /// - Session id: the global-only affinity PREFIX of decision
    ///   84 (d): env `TAMAKO_LLM_SESSION_ID` → global config
    ///   `llm_session_id` → default [`DEFAULT_SESSION_ID`].
    ///   Resolution is group-agnostic and yields the PREFIX; the
    ///   per-(group, purpose) suffix of decision 84 (b) is applied
    ///   afterward via [`EndpointConfig::with_session_suffix`] at the
    ///   per-group build site (decision 84 (c)).
    ///
    /// Empty strings count as unset, in env and config alike.
    pub fn resolve(values: &LlmConfigValues) -> Result<Self, AgentError> {
        Ok(LlmEndpoints {
            digest: Self::resolve_purpose(LlmPurpose::Digest, values)?,
            gate: Self::resolve_purpose(LlmPurpose::Gate, values)?,
            reply: Self::resolve_purpose(LlmPurpose::Reply, values)?,
            summary: Self::resolve_purpose(LlmPurpose::Summary, values)?,
        })
    }

    fn resolve_purpose(
        purpose: LlmPurpose,
        values: &LlmConfigValues,
    ) -> Result<EndpointConfig, AgentError> {
        let api_string = purpose
            .llm_api_env_var()
            .and_then(env_value)
            .or_else(|| env_value(LLM_API_ENV_VAR))
            .or_else(|| values.purpose_llm_api(purpose).map(str::to_string))
            .or_else(|| {
                values
                    .llm_api
                    .as_deref()
                    .filter(|v| !v.is_empty())
                    .map(str::to_string)
            });
        let api = match api_string {
            Some(value) => value.parse::<LlmApi>()?,
            None => LlmApi::AnthropicCompatible,
        };
        let base_url = purpose
            .llm_base_url_env_var()
            .and_then(env_value)
            .or_else(|| env_value(LLM_BASE_URL_ENV_VAR))
            .or_else(|| values.purpose_llm_base_url(purpose).map(str::to_string))
            .or_else(|| {
                values
                    .llm_base_url
                    .as_deref()
                    .filter(|v| !v.is_empty())
                    .map(str::to_string)
            });
        let model = env_value(purpose.model_env_var())
            .or_else(|| values.purpose_model(purpose).map(str::to_string))
            .unwrap_or_else(|| purpose.default_model().to_string());
        let mode_string = env_value(purpose.structured_output_env_var())
            .or_else(|| env_value(STRUCTURED_OUTPUT_ENV_VAR))
            .or_else(|| {
                values
                    .purpose_structured_output(purpose)
                    .map(str::to_string)
            })
            .or_else(|| {
                values
                    .structured_output
                    .as_deref()
                    .filter(|v| !v.is_empty())
                    .map(str::to_string)
            });
        let structured_output = match mode_string {
            Some(value) => value.parse::<StructuredOutputMode>()?,
            None => StructuredOutputMode::Schema,
        };
        Ok(EndpointConfig {
            api,
            base_url,
            model,
            structured_output,
            session_id: resolve_session_id(values),
        })
    }
}

/// Resolves the global session-id PREFIX (module docs, decision
/// 84 (d)): env `TAMAKO_LLM_SESSION_ID` → global config
/// `llm_session_id` → [`DEFAULT_SESSION_ID`]. Empty strings count as
/// unset, so the resolved value is never empty. The per-(group,
/// purpose) suffix of decision 84 (b) is NOT this function's concern:
/// the caller applies it after `resolve` via `with_session_suffix`.
fn resolve_session_id(values: &LlmConfigValues) -> String {
    env_value(LLM_SESSION_ID_ENV_VAR)
        .or_else(|| {
            values
                .llm_session_id
                .as_deref()
                .filter(|v| !v.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| DEFAULT_SESSION_ID.to_string())
}

/// Builds the default-header map carrying the gateway
/// session-affinity headers (module docs, decision 84 (a)): BOTH
/// `x-opencode-session` (the Opencode Go gateway's session-affinity
/// key) AND `x-session-id` (OpenRouter's sticky-routing key), each
/// with the same resolved session id. Dual-send is harmless — each
/// gateway reads its own key — and avoids fragile base-url sniffing.
/// The map REPLACES the client's default headers; the client
/// `build()` inserts the API-key auth header when the map does not
/// carry it, so the two never clash. Shared by
/// [`EndpointClient::build`] (completions) and
/// [`RigEmbeddingProvider::from_endpoint`] (embeddings) and
/// [`crate::caption::RigCaptionProvider::from_endpoint`] (captions):
/// the session affinity carries over to all three (decision 84 (b):
/// "the endpoint layer's session plumbing already carries to all
/// three client kinds"). A session id that is not a valid header
/// value is `AgentError::ProviderConfig`.
pub(crate) fn session_header_map(
    session_id: &str,
) -> Result<rig::http_client::HeaderMap, AgentError> {
    let mut headers = rig::http_client::HeaderMap::new();
    for key in ["x-opencode-session", "x-session-id"] {
        headers.insert(
            key,
            rig::http_client::HeaderValue::from_str(session_id).map_err(|error| {
                AgentError::ProviderConfig(format!(
                    "invalid llm_session_id {session_id:?}: {error}"
                ))
            })?,
        );
    }
    Ok(headers)
}

/// The purpose value of an [`EndpointClient`] that no purpose-named
/// wrapper stamped (module docs, decision 53). `EndpointClient::build`
/// has no purpose signal — [`EndpointConfig`] is purpose-agnostic — so
/// the field starts here and the purpose-named `from_endpoint`
/// wrappers (extractor, gate, reply, summary, recall, warmup, and the
/// two confirmers) stamp it through
/// [`EndpointClient::build_for_purpose`]. Direct `build` callers (tests)
/// keep this value; their usage lines stay honest rather than guessed.
const UNSCOPED_PURPOSE: &str = "unscoped";

/// Resolves the API key of one completion endpoint (decision 87).
///
/// The chain, when a `purpose` is known (the purpose-named `from_endpoint`
/// wrappers): the purpose env var ([`LlmPurpose::api_key_env_var`]) → the
/// family env var ([`LlmApi::api_key_env_var`]: `ANTHROPIC_API_KEY` /
/// `OPENAI_API_KEY`) → a missing-key `ProviderConfig` naming BOTH vars
/// tried. When `purpose` is `None` (the purpose-agnostic
/// [`EndpointClient::build`] path, e.g. tests) the family var alone
/// applies. An empty string counts as unset and falls through (the
/// [`env_value`] discipline). ENVIRONMENT-ONLY: no TOML key exists
/// (specs.md Section 13 — secrets never enter the config file).
///
/// Embedding and caption providers do NOT use this chain — they keep the
/// family key (process-wide, the decision-84 M5 follow-up).
fn resolve_api_key(
    endpoint: &EndpointConfig,
    purpose: Option<LlmPurpose>,
) -> Result<String, AgentError> {
    let family_var = endpoint.api.api_key_env_var();
    if let Some(purpose) = purpose {
        let purpose_var = purpose.api_key_env_var();
        if let Some(key) = env_value(purpose_var) {
            return Ok(key);
        }
        return env_value(family_var).ok_or_else(|| {
            AgentError::ProviderConfig(format!(
                "missing API key: set {purpose_var} or {family_var} for {} endpoints (purpose: {})",
                endpoint.api,
                purpose.as_str()
            ))
        });
    }
    env_value(family_var).ok_or_else(|| {
        AgentError::ProviderConfig(format!(
            "missing API key: set {family_var} for {} endpoints",
            endpoint.api
        ))
    })
}

/// The rig completion model handle of one family. rig 0.41 has two
/// distinct model types; the enum hides the split.
enum EndpointModel {
    Anthropic(anthropic::completion::CompletionModel),
    OpenAi(openai::completion::CompletionModel),
}

/// A completion client for one endpoint (specs.md Section 13). Hides the
/// per-family rig model types behind one async `complete` call.
pub struct EndpointClient {
    model: EndpointModel,
    /// The LLM purpose the client serves ([`LlmPurpose::as_str`]:
    /// "digest"/"gate"/"reply"/"summary"). Carried into the curated
    /// per-call usage INFO line (decision 53). `build` sets
    /// [`UNSCOPED_PURPOSE`]; the purpose-named wrappers stamp the real
    /// purpose through [`EndpointClient::build_for_purpose`].
    purpose: &'static str,
    /// The resolved model name (a copy of [`EndpointConfig::model`]),
    /// carried into the per-call usage INFO line so the operator can
    /// attribute provider cache behavior to a model.
    model_name: String,
    /// The resolved structured-output mode of the endpoint (module
    /// docs). `complete` interprets its `output_schema` per this mode.
    structured_output: StructuredOutputMode,
    /// The per-attempt completion timeout (H4b, [`ENDPOINT_TIMEOUT`]).
    /// Production clients always use the constant; tests override it
    /// through `with_timeout`.
    timeout: Duration,
}

// The rig model handles do not implement Debug. A manual impl keeps
// EndpointClient printable in test failures and logs (same pattern as
// RigExtractor).
impl std::fmt::Debug for EndpointClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let family = match &self.model {
            EndpointModel::Anthropic(_) => LlmApi::AnthropicCompatible,
            EndpointModel::OpenAi(_) => LlmApi::OpenAiCompatible,
        };
        f.debug_struct("EndpointClient")
            .field("family", &family)
            .field("purpose", &self.purpose)
            .field("model_name", &self.model_name)
            .field("structured_output", &self.structured_output)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl EndpointClient {
    /// Builds a client for the endpoint. Reads the family API key from
    /// the environment (specs.md Section 13: "API keys come from the
    /// environment only, never from a config file"). A missing or empty
    /// key is `AgentError::ProviderConfig`.
    ///
    /// Uses the explicit builder, not `Client::from_env()`, so the
    /// `llm_base_url` config is honored. rig's `ANTHROPIC_BASE_URL` and
    /// `OPENAI_BASE_URL` env vars therefore do NOT apply; see the module
    /// docs.
    ///
    /// The resolved `session_id` becomes the `x-opencode-session` and
    /// `x-session-id` default headers of the client (module docs,
    /// decision 84 (a)): both are sent on every request of both API
    /// families. A session id that is not a valid header value is
    /// `AgentError::ProviderConfig`.
    pub fn build(endpoint: &EndpointConfig) -> Result<Self, AgentError> {
        // The purpose-agnostic path (direct `build` callers, tests): no
        // per-purpose override applies, so the key is the family key and
        // the client starts UNSCOPED. The purpose-aware construction is
        // [`EndpointClient::build_for_purpose`].
        Self::build_with_key(endpoint, None)
    }

    /// Builds a client for one PURPOSE's endpoint (decision 87): the API
    /// key resolves purpose env var → family env var → missing-key
    /// `ProviderConfig`, and the client is stamped with the purpose (so
    /// the per-call usage INFO line is honest). This is the construction
    /// the purpose-named `from_endpoint` wrappers use; it subsumes
    /// [`EndpointClient::build`] plus the purpose stamping.
    pub fn build_for_purpose(
        endpoint: &EndpointConfig,
        purpose: LlmPurpose,
    ) -> Result<Self, AgentError> {
        Self::build_with_key(endpoint, Some(purpose))
    }

    /// The shared construction. `purpose` selects the decision-87 key
    /// chain (purpose env → family env); `None` resolves the family key
    /// directly (the purpose-agnostic `build` path). The returned client
    /// carries the purpose's `as_str()` stamp, or [`UNSCOPED_PURPOSE`]
    /// when purpose-less.
    fn build_with_key(
        endpoint: &EndpointConfig,
        purpose: Option<LlmPurpose>,
    ) -> Result<Self, AgentError> {
        let api_key = resolve_api_key(endpoint, purpose)?;
        // The gateway session-affinity header (module docs); refer to
        // `session_header_map`.
        let headers = session_header_map(&endpoint.session_id)?;
        let purpose_str = purpose.map_or(UNSCOPED_PURPOSE, |p| p.as_str());
        match endpoint.api {
            LlmApi::AnthropicCompatible => {
                let mut builder = anthropic::Client::builder()
                    .api_key(api_key)
                    .http_headers(headers.clone());
                if let Some(base_url) = &endpoint.base_url {
                    builder = builder.base_url(base_url);
                }
                let client = builder
                    .build()
                    .map_err(|error| AgentError::ProviderConfig(error.to_string()))?;
                Ok(EndpointClient {
                    model: EndpointModel::Anthropic(client.completion_model(&endpoint.model)),
                    purpose: purpose_str,
                    model_name: endpoint.model.clone(),
                    structured_output: endpoint.structured_output,
                    timeout: ENDPOINT_TIMEOUT,
                })
            }
            LlmApi::OpenAiCompatible => {
                // The default openai::Client speaks the Responses API
                // (first-party only in practice). CompletionsClient is
                // the openai-compatible path: POST {base}/chat/completions.
                let mut builder = openai::CompletionsClient::builder()
                    .api_key(api_key)
                    .http_headers(headers);
                if let Some(base_url) = &endpoint.base_url {
                    builder = builder.base_url(base_url);
                }
                let client = builder
                    .build()
                    .map_err(|error| AgentError::ProviderConfig(error.to_string()))?;
                Ok(EndpointClient {
                    model: EndpointModel::OpenAi(client.completion_model(&endpoint.model)),
                    purpose: purpose_str,
                    model_name: endpoint.model.clone(),
                    structured_output: endpoint.structured_output,
                    timeout: ENDPOINT_TIMEOUT,
                })
            }
        }
    }

    /// Test-only override of the per-attempt timeout (H4b). Production
    /// clients always use [`ENDPOINT_TIMEOUT`]; there is deliberately
    /// no config key for it.
    #[cfg(test)]
    pub(crate) fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// The purpose stamp of the client (decision 96, C4): labels the
    /// validation-seam WARNs so the decision-93 per-model telemetry
    /// goal is reachable from the log stream.
    pub fn purpose(&self) -> &'static str {
        self.purpose
    }

    /// The resolved model name of the client (decision 96, C4):
    /// labels the validation-seam WARNs alongside the purpose.
    pub fn model_name(&self) -> &str {
        &self.model_name
    }

    /// Sends one completion request and returns the first text content.
    ///
    /// Prompt convention: the last message is the prompt; the preceding
    /// messages go to the chat history. An empty `messages` sends one
    /// empty user message as the prompt. `preamble` becomes the system
    /// message. `output_schema` is interpreted per the resolved
    /// structured-output mode (module docs): `Schema` passes it to rig;
    /// `JsonObject` drops it and — only when the call CARRIES a schema
    /// (M1: a schema-less call, e.g. the plain-text reply path, sends
    /// NO response_format) — adds
    /// `response_format: { type: "json_object" }` on the OpenAI family
    /// only (Anthropic degrades to prompt-only); `PromptOnly` drops it
    /// unconditionally. `max_tokens` is always set (Anthropic requires
    /// it). The attempt is bounded by the per-attempt timeout (H4b,
    /// [`ENDPOINT_TIMEOUT`]).
    ///
    /// Errors: provider errors, a response without text content, and a
    /// stalled attempt (no response within the timeout; the message
    /// starts with `endpoint timeout after`) are
    /// `AgentError::Extraction`.
    pub async fn complete(
        &self,
        preamble: Option<String>,
        messages: Vec<Message>,
        output_schema: Option<schemars::Schema>,
        max_tokens: u64,
    ) -> Result<String, AgentError> {
        // The mode decides whether the schema reaches the wire and
        // whether the OpenAI json_object response_format applies.
        // M1: the json_object response_format is PER CALL — only a
        // call that carries a schema gets it. A schema-less call (the
        // plain-text reply path) on a json_object endpoint sends NO
        // response_format; otherwise the endpoint would force JSON
        // output on a plain-text reply.
        let (output_schema, json_object) = match self.structured_output {
            StructuredOutputMode::Schema => (output_schema, false),
            StructuredOutputMode::JsonObject => {
                let attach =
                    output_schema.is_some() && matches!(self.model, EndpointModel::OpenAi(_));
                (None, attach)
            }
            StructuredOutputMode::PromptOnly => (None, false),
        };
        match &self.model {
            EndpointModel::Anthropic(model) => {
                complete_with(
                    model,
                    self.purpose,
                    &self.model_name,
                    preamble,
                    messages,
                    output_schema,
                    max_tokens,
                    self.timeout,
                    false,
                )
                .await
            }
            EndpointModel::OpenAi(model) => {
                complete_with(
                    model,
                    self.purpose,
                    &self.model_name,
                    preamble,
                    messages,
                    output_schema,
                    max_tokens,
                    self.timeout,
                    json_object,
                )
                .await
            }
        }
    }

    /// One structured completion with the repair retry of the module
    /// docs: the shared flow of extraction, gate, recall, and the
    /// segmented summarizer.
    ///
    /// The first call passes `schema` to `complete` (the resolved mode
    /// decides how it reaches the wire) and parses the text into `T`.
    /// A parse failure becomes the ORIGINAL error
    /// `AgentError::Extraction("{error_label}: {error}")`. When the raw
    /// text parses as a `serde_json::Value` — it IS JSON but failed
    /// schema validation — ONE repair completion runs on the same
    /// endpoint: the broken JSON, the validation error, and the schema,
    /// with [`REPAIR_PREAMBLE`]. Non-JSON text returns the original
    /// error immediately (no repair).
    ///
    /// A repaired text that parses as `T` is returned (with a warn
    /// log). A failed repair call OR an unparsable repaired text
    /// returns the ORIGINAL error, so the caller's backoff and
    /// dead-letter discipline applies unchanged.
    pub async fn complete_structured<T>(
        &self,
        preamble: Option<String>,
        messages: Vec<Message>,
        schema: schemars::Schema,
        max_tokens: u64,
        error_label: &str,
    ) -> Result<T, AgentError>
    where
        T: serde::de::DeserializeOwned,
    {
        self.complete_structured_cancellable(
            preamble,
            messages,
            schema,
            max_tokens,
            error_label,
            None,
        )
        .await
    }

    /// The decision-114 cancellable variant of [`Self::complete_structured`]:
    /// the drain token polls BEFORE the initial call and BETWEEN the
    /// initial call and the one repair retry, so a drain stop never
    /// waits out more than one in-flight call window. A fired token
    /// answers [`AgentError::Cancelled`] — never an extraction failure,
    /// so no attempt is consumed and no repair fires.
    pub async fn complete_structured_cancellable<T>(
        &self,
        preamble: Option<String>,
        messages: Vec<Message>,
        schema: schemars::Schema,
        max_tokens: u64,
        error_label: &str,
        cancel: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<T, AgentError>
    where
        T: serde::de::DeserializeOwned,
    {
        complete_structured_with(
            |call| async move {
                self.complete(
                    call.preamble,
                    call.messages,
                    Some(call.schema),
                    call.max_tokens,
                )
                .await
            },
            preamble,
            messages,
            schema,
            max_tokens,
            error_label,
            cancel,
        )
        .await
    }
}

/// The resolved embedding endpoint (current-state.md decision 66).
/// Global-only: one embedding endpoint per deployment, no per-purpose
/// or per-group machinery (the same standing as the session-id
/// PREFIX of decision 84 (d)).
/// Embeddings always use the openai-compatible family
/// (`POST {base}/embeddings`), so — unlike [`EndpointConfig`] — the
/// family is fixed and the base URL is concrete (decision 66 pins the
/// default).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingEndpoint {
    /// The base URL of the openai-compatible embedding endpoint.
    /// Never empty: resolution falls back to
    /// [`DEFAULT_EMBEDDING_BASE_URL`].
    pub base_url: String,
    /// The embedding model name. Never empty: resolution falls back
    /// to [`DEFAULT_EMBEDDING_MODEL`].
    pub model: String,
    /// The resolved session id, sent as BOTH the `x-opencode-session`
    /// and the `x-session-id` header on every embedding request
    /// (decision 84 (a)). [`EmbeddingEndpoint::resolve`] yields the
    /// global-only PREFIX;
    /// [`EmbeddingEndpoint::with_session_suffix`] then joins the
    /// per-(group, purpose) suffix to `{prefix}-{suffix}` (decision
    /// 84 (b); refer to [`EndpointConfig::session_id`]).
    pub session_id: String,
}

impl EmbeddingEndpoint {
    /// Resolves the embedding endpoint from the config values and the
    /// environment (the same env-wins idiom as
    /// [`LlmEndpoints::resolve`]):
    ///
    /// - Model: env `TAMAKO_EMBEDDING_MODEL` → config
    ///   `embedding_model` → [`DEFAULT_EMBEDDING_MODEL`].
    /// - Base URL: env `TAMAKO_EMBEDDING_BASE_URL` → config
    ///   `embedding_llm_base_url` → [`DEFAULT_EMBEDDING_BASE_URL`].
    /// - Session id: the global PREFIX chain of
    ///   [`LlmEndpoints::resolve`] (env `TAMAKO_LLM_SESSION_ID` →
    ///   config `llm_session_id` → [`DEFAULT_SESSION_ID`]); the
    ///   per-(group, purpose) suffix of decision 84 (b) is applied
    ///   afterward via [`EmbeddingEndpoint::with_session_suffix`].
    ///
    /// Empty strings count as unset, in env and config alike. The
    /// values are free-form strings (rig never validates model
    /// names), so resolution is infallible.
    pub fn resolve(values: &LlmConfigValues) -> Self {
        let model = env_value(EMBEDDING_MODEL_ENV_VAR)
            .or_else(|| {
                values
                    .embedding_model
                    .as_deref()
                    .filter(|v| !v.is_empty())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| DEFAULT_EMBEDDING_MODEL.to_string());
        let base_url = env_value(EMBEDDING_BASE_URL_ENV_VAR)
            .or_else(|| {
                values
                    .embedding_llm_base_url
                    .as_deref()
                    .filter(|v| !v.is_empty())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| DEFAULT_EMBEDDING_BASE_URL.to_string());
        EmbeddingEndpoint {
            base_url,
            model,
            session_id: resolve_session_id(values),
        }
    }

    /// Applies the per-(group, purpose) affinity suffix (decision
    /// 84 (b)): the session id becomes `{prefix}-{suffix}`. The same
    /// contract as [`EndpointConfig::with_session_suffix`]: called by
    /// the binary at the per-group service-build site after
    /// `resolve`; the suffix is the persisted store mint (this layer
    /// stays store-agnostic); group-less callers never call it.
    pub fn with_session_suffix(mut self, suffix: &str) -> Self {
        self.session_id = format!("{}-{suffix}", self.session_id);
        self
    }
}

/// The resolved caption endpoint (current-state.md decision 82 (c)).
/// Global-only: one caption endpoint per deployment, no per-purpose
/// or per-group machinery (the same standing as
/// [`EmbeddingEndpoint`]). Captions always use the openai-compatible
/// family (`POST {base}/chat/completions` with an image-bearing
/// message), so — unlike [`EndpointConfig`] — the family is fixed and
/// the base URL is concrete (decision 82 (c) pins the default).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptionEndpoint {
    /// The base URL of the openai-compatible caption endpoint.
    /// Never empty: resolution falls back to
    /// [`DEFAULT_CAPTION_BASE_URL`].
    pub base_url: String,
    /// The caption model name. Never empty: resolution falls back
    /// to [`DEFAULT_CAPTION_MODEL`].
    pub model: String,
    /// The resolved session id, sent as BOTH the `x-opencode-session`
    /// and the `x-session-id` header on every caption request
    /// (decision 84 (a)). [`CaptionEndpoint::resolve`] yields the
    /// global-only PREFIX; [`CaptionEndpoint::with_session_suffix`]
    /// then joins the per-(group, purpose) suffix to
    /// `{prefix}-{suffix}` (decision 84 (b); refer to
    /// [`EndpointConfig::session_id`]).
    pub session_id: String,
}

impl CaptionEndpoint {
    /// Resolves the caption endpoint from the config values and the
    /// environment (the same env-wins idiom as
    /// [`EmbeddingEndpoint::resolve`]):
    ///
    /// - Model: env `TAMAKO_CAPTION_MODEL` → config
    ///   `caption_model` → [`DEFAULT_CAPTION_MODEL`].
    /// - Base URL: env `TAMAKO_CAPTION_BASE_URL` → config
    ///   `caption_llm_base_url` → [`DEFAULT_CAPTION_BASE_URL`].
    /// - Session id: the global PREFIX chain of
    ///   [`LlmEndpoints::resolve`] (env `TAMAKO_LLM_SESSION_ID` →
    ///   config `llm_session_id` → [`DEFAULT_SESSION_ID`]); the
    ///   per-(group, purpose) suffix of decision 84 (b) is applied
    ///   afterward via [`CaptionEndpoint::with_session_suffix`].
    ///
    /// Empty strings count as unset, in env and config alike. The
    /// values are free-form strings (rig never validates model
    /// names), so resolution is infallible.
    pub fn resolve(values: &LlmConfigValues) -> Self {
        let model = env_value(CAPTION_MODEL_ENV_VAR)
            .or_else(|| {
                values
                    .caption_model
                    .as_deref()
                    .filter(|v| !v.is_empty())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| DEFAULT_CAPTION_MODEL.to_string());
        let base_url = env_value(CAPTION_BASE_URL_ENV_VAR)
            .or_else(|| {
                values
                    .caption_llm_base_url
                    .as_deref()
                    .filter(|v| !v.is_empty())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| DEFAULT_CAPTION_BASE_URL.to_string());
        CaptionEndpoint {
            base_url,
            model,
            session_id: resolve_session_id(values),
        }
    }

    /// Applies the per-(group, purpose) affinity suffix (decision
    /// 84 (b)): the session id becomes `{prefix}-{suffix}`. The same
    /// contract as [`EndpointConfig::with_session_suffix`]: called by
    /// the binary at the per-group service-build site after
    /// `resolve`; the suffix is the persisted store mint (this layer
    /// stays store-agnostic); group-less callers never call it.
    pub fn with_session_suffix(mut self, suffix: &str) -> Self {
        self.session_id = format!("{}-{suffix}", self.session_id);
        self
    }
}

/// The embedding seam of the Phase 2 sidecar (decision 66). One text
/// in, one [`EMBEDDING_DIMS`]-dimensional vector out. Object-safe
/// (the same `Pin<Box>` convention as [`crate::KnowledgeExtractor`]).
///
/// The seam exists so the KNOWN WATCH ITEM of the rig 0.41 embedding
/// surface stays contained: rig's openai-compatible embedding
/// response type REQUIRES a `usage` object and fails with a
/// `MissingUsage`-class error when the provider (OpenRouter) omits
/// it. If that bites in production, the swap to a direct reqwest
/// `POST {base}/embeddings` call replaces the body of
/// [`RigEmbeddingProvider::embed`] only; the trait, the error class,
/// the dimension pin, and every caller stay unchanged.
pub trait EmbeddingProvider: Send + Sync {
    /// Embeds one text. Errors: provider/transport failures and a
    /// response vector whose length is not [`EMBEDDING_DIMS`] (the
    /// dimension is pinned) are `AgentError::Extraction`; a stalled
    /// attempt (no response within [`ENDPOINT_TIMEOUT`], the H4b
    /// bound mirrored from completions) is an `AgentError::Extraction`
    /// whose message starts with `endpoint timeout after`.
    fn embed<'a>(
        &'a self,
        text: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<f32>, AgentError>> + Send + 'a>,
    >;

    /// Embeds a batch of texts, one vector per input in INPUT ORDER
    /// (decision 73: the entity resolver makes ONE batched embeddings
    /// call per digest batch). The same error classes as [`embed`],
    /// applied per element. The DEFAULT implementation loops
    /// [`EmbeddingProvider::embed`] sequentially — the correct
    /// fallback for any provider; a provider with a native batch
    /// surface overrides it with ONE provider call (the per-attempt
    /// [`ENDPOINT_TIMEOUT`] bound then covers the whole batch call).
    #[allow(clippy::type_complexity)]
    fn embed_texts<'a>(
        &'a self,
        texts: &'a [String],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<Vec<f32>>, AgentError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let mut vectors = Vec::with_capacity(texts.len());
            for text in texts {
                vectors.push(self.embed(text).await?);
            }
            Ok(vectors)
        })
    }
}

/// The live [`EmbeddingProvider`] over rig's openai-compatible
/// embedding surface (decision 66): the SAME
/// [`openai::CompletionsClient`] family the endpoint layer builds for
/// openai-compatible completions, extended with
/// `embedding_model_with_ndims(model, EMBEDDING_DIMS)`. The
/// `x-opencode-session` and `x-session-id` default headers carry over
/// via [`session_header_map`] (decision 84 (a)). Refer to
/// [`EmbeddingProvider`] for the
/// MissingUsage watch item this type contains.
pub struct RigEmbeddingProvider {
    model: openai::GenericEmbeddingModel<openai::OpenAICompletionsExt>,
}

// The rig model handle does not implement Debug. A manual impl keeps
// RigEmbeddingProvider printable in test failures and logs (the same
// pattern as EndpointClient).
impl std::fmt::Debug for RigEmbeddingProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RigEmbeddingProvider")
            .field("model", &self.model.model)
            .field("ndims", &EMBEDDING_DIMS)
            .finish_non_exhaustive()
    }
}

impl RigEmbeddingProvider {
    /// Builds the provider for the resolved embedding endpoint. Reads
    /// `OPENAI_API_KEY` from the environment (specs.md Section 13:
    /// API keys come from the environment only; the embedding endpoint
    /// is openai-compatible). Every build failure is
    /// `AgentError::ProviderConfig` (the same shape as
    /// [`EndpointClient::build`]): a missing or empty key, an invalid
    /// session-id header value, or a rig client-build error.
    pub fn from_endpoint(endpoint: &EmbeddingEndpoint) -> Result<Self, AgentError> {
        let api_key = env_value(OPENAI_API_KEY_ENV_VAR).ok_or_else(|| {
            AgentError::ProviderConfig(format!(
                "missing API key: set {OPENAI_API_KEY_ENV_VAR} for openai-compatible endpoints"
            ))
        })?;
        // The same builder + session-affinity header as
        // EndpointClient::build's openai-compatible branch.
        let client = openai::CompletionsClient::builder()
            .api_key(api_key)
            .base_url(&endpoint.base_url)
            .http_headers(session_header_map(&endpoint.session_id)?)
            .build()
            .map_err(|error| AgentError::ProviderConfig(error.to_string()))?;
        Ok(RigEmbeddingProvider {
            model: client.embedding_model_with_ndims(&endpoint.model, EMBEDDING_DIMS),
        })
    }

    /// The degrade seam (decision 66): `None` means embeddings are
    /// disabled for this run. Mirrors the per-purpose degrade of the
    /// binary (a missing family key degrades with ONE startup warning,
    /// never a hard error — the `build_digest_pipeline` /
    /// `build_wake_services` shape in tamako/src/main.rs): every build
    /// failure is `AgentError::ProviderConfig`, logged once at WARN,
    /// and the caller wires `None`.
    pub fn build(endpoint: &EmbeddingEndpoint) -> Option<Self> {
        match Self::from_endpoint(endpoint) {
            Ok(provider) => Some(provider),
            Err(error) => {
                tracing::warn!(%error, "embedding provider disabled: no provider configuration; embeddings will not run");
                None
            }
        }
    }
}

impl EmbeddingProvider for RigEmbeddingProvider {
    fn embed<'a>(
        &'a self,
        text: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<f32>, AgentError>> + Send + 'a>,
    > {
        Box::pin(async move {
            // The per-attempt bound of H4b, mirrored from completions:
            // a stalled embedding is bounded, a slow-but-progressing
            // response is untouched.
            let embedding = tokio::time::timeout(ENDPOINT_TIMEOUT, self.model.embed_text(text))
                .await
                .map_err(|_| {
                    AgentError::Extraction(format!(
                        "endpoint timeout after {ENDPOINT_TIMEOUT:?}: no embedding response from the endpoint"
                    ))
                })?
                .map_err(|error| AgentError::Extraction(format!("embedding failed: {error}")))?;
            checked_embedding_vector(embedding.vec)
        })
    }

    // embed_texts is NOT overridden: decision 81 addendum — the
    // default OpenRouter route for google/gemini-embedding-2 serves
    // ARRAY input only from google-ai-studio (excluded under the
    // account's ZDR-only policy → the embeddings call 404s), while
    // single-text input is served by the ZDR google-vertex endpoints.
    // The trait's default sequential embed loop is the correct surface
    // for this provider: one POST per text, the same rate-limit and
    // timeout discipline as the rest of the embeddings path, and every
    // call takes the ZDR route. (Verified empirically 2026-08-22:
    // single-text 200, array 404, repeatedly.)
}

/// The dimension pin of decision 81 (decision 66 first pinned 4096)
/// and the f64→f32 narrowing (the store schema holds f32). A vector of
/// any length other than [`EMBEDDING_DIMS`] is a HARD error: a
/// wrong-dimension vector must never reach the store. Pure, so the pin
/// and the narrowing are unit-testable without a network.
fn checked_embedding_vector(vec: Vec<f64>) -> Result<Vec<f32>, AgentError> {
    if vec.len() != EMBEDDING_DIMS {
        return Err(AgentError::Extraction(format!(
            "embedding dimension mismatch: expected {EMBEDDING_DIMS} (decision 81 pins the dimension), got {}",
            vec.len()
        )));
    }
    // The f64→f32 narrowing is the deliberate store-schema
    // conversion (embeddings are unit-magnitude; f32 is the stored
    // precision of the sidecar index).
    #[allow(clippy::cast_possible_truncation)]
    Ok(vec.into_iter().map(|value| value as f32).collect())
}

/// The batch flavor of [`checked_embedding_vector`] (decision 73): the
/// dimension pin and the f64→f32 narrowing applied per element, the
/// input order carried through. Pure, so the batched shape logic is
/// unit-testable without a network. Test-only since the decision-81
/// addendum (the live batch surface is the trait's default sequential
/// loop; see `RigEmbeddingProvider`).
#[cfg(test)]
fn checked_batch_vectors(vecs: Vec<Vec<f64>>) -> Result<Vec<Vec<f32>>, AgentError> {
    vecs.into_iter().map(checked_embedding_vector).collect()
}

/// The outcome of [`strip_reasoning_markup`]: the text with reasoning
/// markup removed, plus the removed byte count for the WARN (the
/// operator's per-model endpoint-behavior gauge, decision 93).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReasoningStripOutcome {
    /// The text with every reasoning-markup region removed. NOT
    /// trimmed — the caller owns trimming.
    pub(crate) text: String,
    /// The removed byte count; zero means the input passed through
    /// untouched.
    pub(crate) stripped_bytes: usize,
}

/// Removes reasoning markup from one completion's text (decision 93,
/// specs.md Section 9.8). Exact-match lowercase `<think>` only — the
/// machine templates emit one spelling. The scan:
///
/// - a balanced `<think>...</think>` region strips (first closer
///   wins), and the scan resumes after it;
/// - an orphan `</think>` (no unmatched opener before it) drops
///   everything up to and including it — the observed shape of a
///   provider-side reasoning parser that split at a literal `</think>`
///   the reasoning itself mentioned;
/// - an unclosed `<think>` voids the remainder: everything from the
///   opener on is reasoning without an answer (fail closed).
///
/// Deliberate tradeoff: a genuine reply QUOTING a raw `</think>` loses
/// its head up to the quote. Accepted over the alternative (leaking
/// reasoning): the leak is the recurring live failure, the quote is
/// not, and every strip emits a WARN with the byte count.
pub(crate) fn strip_reasoning_markup(text: &str) -> ReasoningStripOutcome {
    const OPEN: &str = "<think>";
    const CLOSE: &str = "</think>";
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    let mut stripped_bytes = 0;
    loop {
        // Decision 96 (C3): with NO opener in the remainder every
        // remaining closer is an orphan, and the survivor is provably
        // the tail past the LAST one — one rfind jump replaces the
        // per-closer loop, whose opener rescan made closer-dense input
        // quadratic (measured 400 KB -> 9.4 s, synchronously inside
        // the async completion task). Output and stripped_bytes are
        // unchanged.
        let Some(o) = rest.find(OPEN) else {
            if let Some(last) = rest.rfind(CLOSE) {
                stripped_bytes += last + CLOSE.len();
                rest = &rest[last + CLOSE.len()..];
            }
            out.push_str(rest);
            break;
        };
        // An orphan closer ahead of the opener: a reasoning tail.
        if let Some(c) = rest.find(CLOSE) {
            if c < o {
                stripped_bytes += c + CLOSE.len();
                rest = &rest[c + CLOSE.len()..];
                continue;
            }
        }
        out.push_str(&rest[..o]);
        match rest[o + OPEN.len()..].find(CLOSE) {
            Some(rel) => {
                stripped_bytes += OPEN.len() + rel + CLOSE.len();
                rest = &rest[o + OPEN.len() + rel + CLOSE.len()..];
            }
            None => {
                stripped_bytes += rest.len() - o;
                rest = "";
            }
        }
    }
    ReasoningStripOutcome {
        text: out,
        stripped_bytes,
    }
}

/// The shared completion flow of both families. The request shape is
/// identical; only the rig model type differs. `purpose` and
/// `model_name` label the curated per-call usage INFO line (decision
/// 53); the caller (`EndpointClient::complete`) passes its stamped
/// purpose and resolved model name. `json_object` adds the
/// OpenAI `json_object` response format via `additional_params`
/// (expressible on the chat-completions path only; the caller sets it
/// for the OpenAI family only). `timeout` bounds the send: a stalled
/// completion (no response within the window) is an
/// `AgentError::Extraction` whose message starts with
/// `endpoint timeout after` (H4b).
// The request shape mirrors `EndpointClient::complete` plus the two
// usage-line labels; bundling them into an args struct would only
// shuffle the same fields (the codebase's standing allow idiom).
#[allow(clippy::too_many_arguments)]
async fn complete_with<M>(
    model: &M,
    purpose: &str,
    model_name: &str,
    preamble: Option<String>,
    messages: Vec<Message>,
    output_schema: Option<schemars::Schema>,
    max_tokens: u64,
    timeout: Duration,
    json_object: bool,
) -> Result<String, AgentError>
where
    M: rig::completion::CompletionModel,
{
    let (history, prompt) = match messages.split_last() {
        Some((prompt, history)) => (history.to_vec(), prompt.clone()),
        None => (Vec::new(), Message::user(String::new())),
    };
    let mut request = model.completion_request(prompt);
    if !history.is_empty() {
        request = request.messages(history);
    }
    if let Some(preamble) = preamble {
        // The preamble becomes the system message.
        request = request.preamble(preamble);
    }
    if let Some(schema) = output_schema {
        // Anthropic: native structured output. OpenAI: json_schema
        // response_format (rig hardcodes strict: true; see module docs).
        request = request.output_schema(schema);
    }
    if json_object {
        // OpenAI chat completions only: additional_params is
        // serde-flattened into the request body (module docs).
        request = request.additional_params(serde_json::json!({
            "response_format": { "type": "json_object" }
        }));
    }
    // Always set max_tokens: Anthropic requires it. The per-attempt
    // timeout (H4b, ENDPOINT_TIMEOUT) guards against a STALLED
    // completion only; a slow-but-progressing response is untouched.
    // The first call and the one repair retry each get their own
    // window (both go through EndpointClient::complete).
    let response = tokio::time::timeout(timeout, request.max_tokens(max_tokens).send())
        .await
        .map_err(|_| {
            AgentError::Extraction(format!(
                "endpoint timeout after {timeout:?}: no completion response from the endpoint"
            ))
        })?
        .map_err(|error| AgentError::Extraction(error.to_string()))?;
    // The curated per-call usage line (decision 53), at INFO so the
    // operator can observe the provider prompt-cache behavior of the
    // session-affinity header (decision 84) without recompiling log
    // filters. One line per completion call: purpose + model + the
    // cached/written/input/output token counts.
    tracing::info!(
        purpose,
        model = model_name,
        input_tokens = response.usage.input_tokens,
        cached_input_tokens = response.usage.cached_input_tokens,
        cache_creation_input_tokens = response.usage.cache_creation_input_tokens,
        output_tokens = response.usage.output_tokens,
        "llm completion usage"
    );
    let text = response
        .choice
        .iter()
        .find_map(|content| match content {
            AssistantContent::Text(text) => Some(text.text.clone()),
            _ => None,
        })
        .ok_or_else(|| AgentError::Extraction("no text content in the response".to_string()))?;
    // Decision 93: reasoning-markup sanitation at the one seam every
    // purpose flows through (module docs, specs.md Section 9.8).
    let stripped = strip_reasoning_markup(&text);
    if stripped.stripped_bytes > 0 {
        tracing::warn!(
            purpose,
            model = model_name,
            stripped_bytes = stripped.stripped_bytes,
            "stripped reasoning markup from the completion text"
        );
    }
    // Fail closed: an all-reasoning response is the same Extraction
    // class as a text-less response, so every caller's backoff and
    // dead-letter discipline applies unchanged.
    if stripped.text.trim().is_empty() {
        return Err(AgentError::Extraction(
            "the response text is entirely reasoning markup".to_string(),
        ));
    }
    Ok(stripped.text)
}

/// The system preamble of the repair completion (module docs). The
/// repair fixes structure and field names only; every value stays.
pub const REPAIR_PREAMBLE: &str = "\
You repair JSON. Fix the JSON to match the schema. Change nothing else: keep every value, only repair the structure and field names. Output only the repaired JSON.";

/// Renders the user message of the repair completion: the broken JSON,
/// the validation error, and the target schema as JSON.
fn render_repair_prompt(
    broken_json: &str,
    validation_error: &str,
    schema: &schemars::Schema,
) -> String {
    let schema_json =
        serde_json::to_string_pretty(schema).unwrap_or_else(|_| format!("{schema:?}"));
    format!(
        "The following JSON fails validation:\n\n{broken_json}\n\nValidation error:\n{validation_error}\n\nTarget schema:\n{schema_json}\n\nFix the JSON to match the schema. Change nothing else. Output only the repaired JSON."
    )
}

/// The arguments of one completion call of the structured flow (the
/// first attempt or the repair). Carried through the completion
/// closure of `complete_structured_with` so tests can capture and
/// script the calls without a network.
#[derive(Debug)]
pub(crate) struct CompletionCall {
    /// The system preamble.
    pub(crate) preamble: Option<String>,
    /// The chat messages; the last is the prompt.
    pub(crate) messages: Vec<Message>,
    /// The output schema. How it reaches the wire is the mode's
    /// decision inside `EndpointClient::complete`.
    pub(crate) schema: schemars::Schema,
    /// The max-tokens bound.
    pub(crate) max_tokens: u64,
}

/// The two-call flow of `EndpointClient::complete_structured`,
/// generic over the completion closure so the flow is unit-testable
/// without a network. `EndpointClient::complete_structured` delegates
/// with a closure over `EndpointClient::complete`; tests (of this
/// module and of the single-purpose providers such as
/// `crate::summary`) script the closure. Refer to
pub(crate) async fn complete_structured_with<T, F, Fut>(
    complete: F,
    preamble: Option<String>,
    messages: Vec<Message>,
    schema: schemars::Schema,
    max_tokens: u64,
    error_label: &str,
    cancel: Option<tokio_util::sync::CancellationToken>,
) -> Result<T, AgentError>
where
    T: serde::de::DeserializeOwned,
    F: Fn(CompletionCall) -> Fut,
    Fut: std::future::Future<Output = Result<String, AgentError>>,
{
    // Decision 114: a fired drain token answers before either call
    // starts. The initial call and the repair retry each keep their own
    // call window; the stop budget is sized to one.
    if cancel.as_ref().is_some_and(|token| token.is_cancelled()) {
        return Err(AgentError::Cancelled);
    }
    let first = complete(CompletionCall {
        preamble,
        messages,
        schema: schema.clone(),
        max_tokens,
    })
    .await?;
    let validation_error = match serde_json::from_str::<T>(&first) {
        Ok(value) => return Ok(value),
        Err(error) => error,
    };
    // The ORIGINAL error. A failed repair returns this, never the
    // repair's own error: the caller's backoff and dead-letter
    // discipline applies unchanged.
    let original = AgentError::Extraction(format!("{error_label}: {validation_error}"));
    // Repair trigger: the text IS JSON but failed the typed parse
    // (schema validation). Non-JSON text is a different failure class;
    // no repair attempt.
    if serde_json::from_str::<serde_json::Value>(&first).is_err() {
        return Err(original);
    }
    // Decision 114: the drain token's second poll — BETWEEN the initial
    // call and the repair retry. A fired token skips the repair and
    // answers Cancelled, so the stop budget waits out at most the one
    // in-flight window.
    if cancel.as_ref().is_some_and(|token| token.is_cancelled()) {
        return Err(AgentError::Cancelled);
    }
    let repaired = complete(CompletionCall {
        preamble: Some(REPAIR_PREAMBLE.to_string()),
        messages: vec![Message::user(render_repair_prompt(
            &first,
            &validation_error.to_string(),
            &schema,
        ))],
        schema,
        max_tokens,
    })
    .await;
    match repaired {
        Ok(text) => match serde_json::from_str::<T>(&text) {
            Ok(value) => {
                tracing::warn!(
                    error_label,
                    "structured output needed a repair completion; the endpoint does not follow the schema reliably"
                );
                Ok(value)
            }
            Err(_) => Err(original),
        },
        // The repair call itself failed: the original error stands.
        Err(_) => Err(original),
    }
}

#[cfg(test)]
pub(crate) mod env_lock {
    /// Serializes tests that mutate process env vars. Env mutation is
    /// process-global; the lock keeps the tests hermetic against each
    /// other.
    pub(crate) static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
}

#[cfg(test)]
mod tests {
    use super::*;

    /// All env vars of the endpoint layer plus the API keys. Tests
    /// remove them up front and restore them on drop.
    const ALL_ENV_VARS: &[&str] = &[
        LLM_API_ENV_VAR,
        LLM_BASE_URL_ENV_VAR,
        LLM_SESSION_ID_ENV_VAR,
        DIGEST_MODEL_ENV_VAR,
        GATE_MODEL_ENV_VAR,
        REPLY_MODEL_ENV_VAR,
        SUMMARY_MODEL_ENV_VAR,
        SUMMARY_LLM_API_ENV_VAR,
        SUMMARY_LLM_BASE_URL_ENV_VAR,
        STRUCTURED_OUTPUT_ENV_VAR,
        DIGEST_STRUCTURED_OUTPUT_ENV_VAR,
        GATE_STRUCTURED_OUTPUT_ENV_VAR,
        REPLY_STRUCTURED_OUTPUT_ENV_VAR,
        SUMMARY_STRUCTURED_OUTPUT_ENV_VAR,
        EMBEDDING_MODEL_ENV_VAR,
        EMBEDDING_BASE_URL_ENV_VAR,
        CAPTION_MODEL_ENV_VAR,
        CAPTION_BASE_URL_ENV_VAR,
        ANTHROPIC_API_KEY_ENV_VAR,
        OPENAI_API_KEY_ENV_VAR,
        // Decision 87: the per-purpose API-key overrides.
        DIGEST_LLM_API_KEY_ENV_VAR,
        GATE_LLM_API_KEY_ENV_VAR,
        REPLY_LLM_API_KEY_ENV_VAR,
        SUMMARY_LLM_API_KEY_ENV_VAR,
    ];

    /// Saves, clears, and restores env vars. Hermetic env handling.
    struct EnvGuard {
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl EnvGuard {
        fn cleared() -> (std::sync::MutexGuard<'static, ()>, Self) {
            let lock = env_lock::ENV_LOCK.lock().unwrap();
            let guard = EnvGuard {
                saved: ALL_ENV_VARS
                    .iter()
                    .map(|name| (*name, std::env::var(name).ok()))
                    .collect(),
            };
            for name in ALL_ENV_VARS {
                std::env::remove_var(name);
            }
            (lock, guard)
        }

        fn set(&self, name: &str, value: &str) {
            std::env::set_var(name, value);
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (name, value) in &self.saved {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }

    #[test]
    fn family_parse_accepts_the_two_spec_strings() {
        use std::str::FromStr as _;
        assert_eq!(
            LlmApi::from_str("anthropic-compatible").unwrap(),
            LlmApi::AnthropicCompatible
        );
        assert_eq!(
            LlmApi::from_str("openai-compatible").unwrap(),
            LlmApi::OpenAiCompatible
        );
        assert_eq!(LlmApi::AnthropicCompatible.as_str(), "anthropic-compatible");
        assert_eq!(LlmApi::OpenAiCompatible.as_str(), "openai-compatible");
        assert_eq!(
            LlmApi::AnthropicCompatible.to_string(),
            "anthropic-compatible"
        );
    }

    #[test]
    fn family_parse_rejects_unknown_strings() {
        use std::str::FromStr as _;
        for bad in ["anthropic", "openai", "", "ANTHROPIC-COMPATIBLE", "claude"] {
            match LlmApi::from_str(bad) {
                Err(AgentError::ProviderConfig(_)) => {}
                other => panic!("expected ProviderConfig for {bad:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn purpose_names_are_the_spec_strings() {
        assert_eq!(LlmPurpose::Digest.as_str(), "digest");
        assert_eq!(LlmPurpose::Gate.as_str(), "gate");
        assert_eq!(LlmPurpose::Reply.as_str(), "reply");
        assert_eq!(LlmPurpose::Summary.as_str(), "summary");
    }

    #[test]
    fn all_defaults_resolve_to_anthropic_with_spec_models() {
        let (_lock, _env) = EnvGuard::cleared();
        let endpoints = LlmEndpoints::resolve(&LlmConfigValues::default()).unwrap();
        assert_eq!(
            endpoints.digest,
            EndpointConfig {
                api: LlmApi::AnthropicCompatible,
                base_url: None,
                model: "claude-haiku-4-5".to_string(),
                structured_output: StructuredOutputMode::Schema,
                session_id: DEFAULT_SESSION_ID.to_string(),
            }
        );
        assert_eq!(
            endpoints.gate,
            EndpointConfig {
                api: LlmApi::AnthropicCompatible,
                base_url: None,
                model: "claude-haiku-4-5".to_string(),
                structured_output: StructuredOutputMode::Schema,
                session_id: DEFAULT_SESSION_ID.to_string(),
            }
        );
        assert_eq!(
            endpoints.reply,
            EndpointConfig {
                api: LlmApi::AnthropicCompatible,
                base_url: None,
                model: "claude-sonnet-4-5".to_string(),
                structured_output: StructuredOutputMode::Schema,
                session_id: DEFAULT_SESSION_ID.to_string(),
            }
        );
        // The summary purpose defaults to the cheap tier (the same
        // model as the gate).
        assert_eq!(
            endpoints.summary,
            EndpointConfig {
                api: LlmApi::AnthropicCompatible,
                base_url: None,
                model: "claude-haiku-4-5".to_string(),
                structured_output: StructuredOutputMode::Schema,
                session_id: DEFAULT_SESSION_ID.to_string(),
            }
        );
    }

    #[test]
    fn global_config_applies_to_all_purposes() {
        let (_lock, _env) = EnvGuard::cleared();
        let values = LlmConfigValues {
            llm_api: Some("openai-compatible".to_string()),
            llm_base_url: Some("http://localhost:8000/v1".to_string()),
            digest_model: Some("qwen2.5-7b".to_string()),
            gate_model: Some("qwen2.5-3b".to_string()),
            reply_model: Some("qwen2.5-32b".to_string()),
            summary_model: Some("qwen2.5-1b".to_string()),
            ..LlmConfigValues::default()
        };
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        assert_eq!(endpoints.digest.api, LlmApi::OpenAiCompatible);
        assert_eq!(
            endpoints.digest.base_url.as_deref(),
            Some("http://localhost:8000/v1")
        );
        assert_eq!(endpoints.digest.model, "qwen2.5-7b");
        assert_eq!(endpoints.gate.model, "qwen2.5-3b");
        assert_eq!(endpoints.reply.model, "qwen2.5-32b");
        assert_eq!(endpoints.summary.model, "qwen2.5-1b");
        assert_eq!(endpoints.summary.api, LlmApi::OpenAiCompatible);
        assert_eq!(
            endpoints.summary.base_url.as_deref(),
            Some("http://localhost:8000/v1")
        );
    }

    #[test]
    fn purpose_config_overrides_global_config_per_purpose() {
        let (_lock, _env) = EnvGuard::cleared();
        // A mixed deployment (specs.md Section 13): a cheap self-hosted
        // openai-compatible endpoint for extraction, first-party
        // Anthropic for the rest.
        let values = LlmConfigValues {
            llm_api: Some("anthropic-compatible".to_string()),
            llm_base_url: Some("https://api.anthropic.com".to_string()),
            digest_llm_api: Some("openai-compatible".to_string()),
            digest_llm_base_url: Some("http://localhost:8000/v1".to_string()),
            ..LlmConfigValues::default()
        };
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        assert_eq!(endpoints.digest.api, LlmApi::OpenAiCompatible);
        assert_eq!(
            endpoints.digest.base_url.as_deref(),
            Some("http://localhost:8000/v1")
        );
        // Gate and reply keep the global family and base URL.
        assert_eq!(endpoints.gate.api, LlmApi::AnthropicCompatible);
        assert_eq!(
            endpoints.gate.base_url.as_deref(),
            Some("https://api.anthropic.com")
        );
        assert_eq!(endpoints.reply.api, LlmApi::AnthropicCompatible);
        assert_eq!(
            endpoints.reply.base_url.as_deref(),
            Some("https://api.anthropic.com")
        );
    }

    #[test]
    fn env_wins_over_purpose_config_for_family_and_base_url() {
        let (_lock, env) = EnvGuard::cleared();
        env.set(LLM_API_ENV_VAR, "anthropic-compatible");
        env.set(LLM_BASE_URL_ENV_VAR, "https://proxy.example.com");
        let values = LlmConfigValues {
            llm_api: Some("openai-compatible".to_string()),
            llm_base_url: Some("http://localhost:8000/v1".to_string()),
            digest_llm_api: Some("openai-compatible".to_string()),
            digest_llm_base_url: Some("http://digest.internal/v1".to_string()),
            gate_llm_api: Some("openai-compatible".to_string()),
            gate_llm_base_url: Some("http://gate.internal/v1".to_string()),
            reply_llm_api: Some("openai-compatible".to_string()),
            reply_llm_base_url: Some("http://reply.internal/v1".to_string()),
            summary_llm_api: Some("openai-compatible".to_string()),
            summary_llm_base_url: Some("http://summary.internal/v1".to_string()),
            ..LlmConfigValues::default()
        };
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        for endpoint in [
            &endpoints.digest,
            &endpoints.gate,
            &endpoints.reply,
            &endpoints.summary,
        ] {
            assert_eq!(endpoint.api, LlmApi::AnthropicCompatible);
            assert_eq!(
                endpoint.base_url.as_deref(),
                Some("https://proxy.example.com")
            );
        }
    }

    #[test]
    fn purpose_env_wins_over_purpose_config_for_the_model() {
        let (_lock, env) = EnvGuard::cleared();
        env.set(DIGEST_MODEL_ENV_VAR, "env-digest-model");
        env.set(GATE_MODEL_ENV_VAR, "env-gate-model");
        env.set(REPLY_MODEL_ENV_VAR, "env-reply-model");
        env.set(SUMMARY_MODEL_ENV_VAR, "env-summary-model");
        let values = LlmConfigValues {
            digest_model: Some("config-digest-model".to_string()),
            gate_model: Some("config-gate-model".to_string()),
            reply_model: Some("config-reply-model".to_string()),
            summary_model: Some("config-summary-model".to_string()),
            ..LlmConfigValues::default()
        };
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        assert_eq!(endpoints.digest.model, "env-digest-model");
        assert_eq!(endpoints.gate.model, "env-gate-model");
        assert_eq!(endpoints.reply.model, "env-reply-model");
        assert_eq!(endpoints.summary.model, "env-summary-model");
    }

    #[test]
    fn the_summary_purpose_resolves_its_own_keys() {
        let (_lock, env) = EnvGuard::cleared();
        // Per-group config: the summary purpose overrides family, base
        // URL, and model individually (mixed deployments, specs.md
        // Section 13).
        let values = LlmConfigValues {
            llm_api: Some("anthropic-compatible".to_string()),
            summary_llm_api: Some("openai-compatible".to_string()),
            summary_llm_base_url: Some("http://summary.internal/v1".to_string()),
            summary_model: Some("local-summary-model".to_string()),
            summary_structured_output: Some("prompt_only".to_string()),
            ..LlmConfigValues::default()
        };
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        assert_eq!(endpoints.summary.api, LlmApi::OpenAiCompatible);
        assert_eq!(
            endpoints.summary.base_url.as_deref(),
            Some("http://summary.internal/v1")
        );
        assert_eq!(endpoints.summary.model, "local-summary-model");
        assert_eq!(
            endpoints.summary.structured_output,
            StructuredOutputMode::PromptOnly
        );
        // The other purposes keep the global family; their models keep
        // the defaults.
        assert_eq!(endpoints.gate.api, LlmApi::AnthropicCompatible);
        assert_eq!(endpoints.gate.model, "claude-haiku-4-5");
        assert_eq!(
            endpoints.gate.structured_output,
            StructuredOutputMode::Schema
        );

        // The summary purpose env vars win over every config source.
        env.set(SUMMARY_LLM_API_ENV_VAR, "anthropic-compatible");
        env.set(
            SUMMARY_LLM_BASE_URL_ENV_VAR,
            "https://summary.env.example.com",
        );
        env.set(SUMMARY_MODEL_ENV_VAR, "env-summary-model");
        env.set(SUMMARY_STRUCTURED_OUTPUT_ENV_VAR, "json_object");
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        assert_eq!(endpoints.summary.api, LlmApi::AnthropicCompatible);
        assert_eq!(
            endpoints.summary.base_url.as_deref(),
            Some("https://summary.env.example.com")
        );
        assert_eq!(endpoints.summary.model, "env-summary-model");
        assert_eq!(
            endpoints.summary.structured_output,
            StructuredOutputMode::JsonObject
        );
        // The global env overrides still feed the other purposes.
        assert_eq!(endpoints.gate.api, LlmApi::AnthropicCompatible);
    }

    #[test]
    fn empty_env_values_count_as_unset() {
        let (_lock, env) = EnvGuard::cleared();
        env.set(LLM_API_ENV_VAR, "");
        env.set(LLM_BASE_URL_ENV_VAR, "");
        env.set(DIGEST_MODEL_ENV_VAR, "");
        let values = LlmConfigValues {
            llm_api: Some("openai-compatible".to_string()),
            llm_base_url: Some("http://localhost:8000/v1".to_string()),
            digest_model: Some("config-model".to_string()),
            ..LlmConfigValues::default()
        };
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        assert_eq!(endpoints.digest.api, LlmApi::OpenAiCompatible);
        assert_eq!(
            endpoints.digest.base_url.as_deref(),
            Some("http://localhost:8000/v1")
        );
        assert_eq!(endpoints.digest.model, "config-model");
    }

    #[test]
    fn whitespace_env_values_are_trimmed_and_whitespace_only_is_unset() {
        // Decision 77 (S5-L3): env_value trims ASCII whitespace; a
        // whitespace-only value counts as unset and the config value
        // wins.
        let (_lock, env) = EnvGuard::cleared();
        env.set(LLM_API_ENV_VAR, " \t ");
        env.set(DIGEST_MODEL_ENV_VAR, " env-model ");
        let values = LlmConfigValues {
            llm_api: Some("openai-compatible".to_string()),
            digest_model: Some("config-model".to_string()),
            ..LlmConfigValues::default()
        };
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        // Whitespace-only: unset, the config value wins.
        assert_eq!(endpoints.digest.api, LlmApi::OpenAiCompatible);
        // Padded: trimmed, the env value wins.
        assert_eq!(endpoints.digest.model, "env-model");
    }

    #[test]
    fn an_unknown_family_string_is_a_provider_config_error() {
        let (_lock, env) = EnvGuard::cleared();
        // From the config file.
        let values = LlmConfigValues {
            llm_api: Some("azure-compatible".to_string()),
            ..LlmConfigValues::default()
        };
        match LlmEndpoints::resolve(&values) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
        // From a purpose override.
        let values = LlmConfigValues {
            reply_llm_api: Some("bogus".to_string()),
            ..LlmConfigValues::default()
        };
        match LlmEndpoints::resolve(&values) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
        // From the summary purpose override.
        let values = LlmConfigValues {
            summary_llm_api: Some("bogus".to_string()),
            ..LlmConfigValues::default()
        };
        match LlmEndpoints::resolve(&values) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
        // From the environment.
        env.set(LLM_API_ENV_VAR, "bogus");
        match LlmEndpoints::resolve(&LlmConfigValues::default()) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
        // From the summary purpose env (wins over every other source).
        env.set(LLM_API_ENV_VAR, "anthropic-compatible");
        env.set(SUMMARY_LLM_API_ENV_VAR, "bogus");
        match LlmEndpoints::resolve(&LlmConfigValues::default()) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
    }

    #[test]
    fn build_without_an_api_key_is_a_provider_config_error() {
        let (_lock, _env) = EnvGuard::cleared();
        for api in [LlmApi::AnthropicCompatible, LlmApi::OpenAiCompatible] {
            let endpoint = EndpointConfig {
                api,
                base_url: None,
                model: "any-model".to_string(),
                structured_output: StructuredOutputMode::Schema,
                session_id: DEFAULT_SESSION_ID.to_string(),
            };
            match EndpointClient::build(&endpoint) {
                Err(AgentError::ProviderConfig(_)) => {}
                other => panic!("expected ProviderConfig for {api}, got {other:?}"),
            }
        }
    }

    #[test]
    fn build_constructs_clients_for_both_families() {
        // Client construction performs no I/O; no network call here.
        let (_lock, env) = EnvGuard::cleared();
        env.set(ANTHROPIC_API_KEY_ENV_VAR, "test-anthropic-key");
        env.set(OPENAI_API_KEY_ENV_VAR, "test-openai-key");
        let anthropic_endpoint = EndpointConfig {
            api: LlmApi::AnthropicCompatible,
            base_url: Some("http://localhost:9999".to_string()),
            model: "claude-haiku-4-5".to_string(),
            structured_output: StructuredOutputMode::Schema,
            session_id: "test-session-anthropic".to_string(),
        };
        let openai_endpoint = EndpointConfig {
            api: LlmApi::OpenAiCompatible,
            base_url: Some("http://localhost:9998/v1".to_string()),
            model: "local-model".to_string(),
            structured_output: StructuredOutputMode::JsonObject,
            session_id: "test-session-openai".to_string(),
        };
        EndpointClient::build(&anthropic_endpoint).expect("anthropic client");
        EndpointClient::build(&openai_endpoint).expect("openai client");
    }

    #[test]
    fn an_empty_api_key_counts_as_missing() {
        let (_lock, env) = EnvGuard::cleared();
        env.set(ANTHROPIC_API_KEY_ENV_VAR, "");
        let endpoint = EndpointConfig {
            api: LlmApi::AnthropicCompatible,
            base_url: None,
            model: "claude-haiku-4-5".to_string(),
            structured_output: StructuredOutputMode::Schema,
            session_id: DEFAULT_SESSION_ID.to_string(),
        };
        match EndpointClient::build(&endpoint) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
    }

    /// An openai-compatible endpoint fixture for the decision-87 key
    /// resolution tests (no network: construction performs no I/O).
    fn openai_endpoint() -> EndpointConfig {
        EndpointConfig {
            api: LlmApi::OpenAiCompatible,
            base_url: Some("http://localhost:9998/v1".to_string()),
            model: "local-model".to_string(),
            structured_output: StructuredOutputMode::Schema,
            session_id: "test-session".to_string(),
        }
    }

    #[test]
    fn the_purpose_key_wins_over_the_family_key() {
        // Decision 87: the purpose env var takes precedence over the
        // family var. Both set → construction succeeds using the purpose
        // key. We cannot read the key back off the rig client, so we pin
        // the SELECTION by setting ONLY the purpose key to a valid value
        // and the family key to a value that would also work — then prove
        // below (the fallback test) that the family key alone is used
        // when the purpose key is absent. The winning proof: with ONLY
        // the purpose key set (family unset), construction must succeed.
        let (_lock, env) = EnvGuard::cleared();
        env.set(REPLY_LLM_API_KEY_ENV_VAR, "reply-purpose-key");
        env.set(OPENAI_API_KEY_ENV_VAR, "family-key");
        EndpointClient::build_for_purpose(&openai_endpoint(), LlmPurpose::Reply)
            .expect("purpose key + family key present: construction succeeds");
    }

    #[test]
    fn the_purpose_key_alone_suffices_without_the_family_key() {
        // Decision 87: the purpose env var is tried FIRST — with the
        // family var UNSET, a present purpose key still builds (this is
        // the case that proves the purpose var is consulted at all).
        let (_lock, env) = EnvGuard::cleared();
        env.set(GATE_LLM_API_KEY_ENV_VAR, "gate-purpose-key");
        EndpointClient::build_for_purpose(&openai_endpoint(), LlmPurpose::Gate)
            .expect("the purpose key alone builds the client");
    }

    #[test]
    fn the_family_key_is_the_fallback_when_the_purpose_key_is_absent() {
        // Decision 87: no purpose key → the family key is used.
        let (_lock, env) = EnvGuard::cleared();
        env.set(OPENAI_API_KEY_ENV_VAR, "family-key");
        EndpointClient::build_for_purpose(&openai_endpoint(), LlmPurpose::Digest)
            .expect("the family key is the fallback");
    }

    #[test]
    fn the_client_stamps_purpose_and_model_for_the_seam_warns() {
        // Decision 96 (C4): the seam WARNs label themselves with the
        // client's purpose stamp and resolved model name.
        let (_lock, env) = EnvGuard::cleared();
        env.set(OPENAI_API_KEY_ENV_VAR, "family-key");
        let endpoint = openai_endpoint();
        let client = EndpointClient::build_for_purpose(&endpoint, LlmPurpose::Reply)
            .expect("the family key builds the client");
        assert_eq!(client.purpose(), "reply");
        assert_eq!(client.model_name(), endpoint.model);
    }

    #[test]
    fn an_empty_purpose_key_falls_through_to_the_family_key() {
        // Decision 87: an empty purpose key counts as unset (the
        // `env_value` discipline) and falls through to the family key.
        let (_lock, env) = EnvGuard::cleared();
        env.set(SUMMARY_LLM_API_KEY_ENV_VAR, "");
        env.set(OPENAI_API_KEY_ENV_VAR, "family-key");
        EndpointClient::build_for_purpose(&openai_endpoint(), LlmPurpose::Summary)
            .expect("empty purpose key falls through to the family key");
    }

    #[test]
    fn a_missing_purpose_and_family_key_errors_naming_both_vars() {
        // Decision 87: neither set → ProviderConfig naming BOTH the
        // purpose var and the family var that were tried.
        let (_lock, _env) = EnvGuard::cleared();
        match EndpointClient::build_for_purpose(&openai_endpoint(), LlmPurpose::Reply) {
            Err(AgentError::ProviderConfig(message)) => {
                assert!(
                    message.contains(REPLY_LLM_API_KEY_ENV_VAR),
                    "the error names the purpose var {REPLY_LLM_API_KEY_ENV_VAR}: {message}"
                );
                assert!(
                    message.contains(OPENAI_API_KEY_ENV_VAR),
                    "the error names the family var {OPENAI_API_KEY_ENV_VAR}: {message}"
                );
            }
            other => panic!("expected ProviderConfig naming both vars, got {other:?}"),
        }
    }

    #[test]
    fn each_purpose_resolves_its_own_key_var() {
        // Decision 87: each of the four purposes consults its OWN env var.
        // Set only one purpose's key at a time (family unset); construction
        // for that purpose must succeed, and the helper maps each purpose
        // to the right var name.
        for (purpose, var) in [
            (LlmPurpose::Digest, DIGEST_LLM_API_KEY_ENV_VAR),
            (LlmPurpose::Gate, GATE_LLM_API_KEY_ENV_VAR),
            (LlmPurpose::Reply, REPLY_LLM_API_KEY_ENV_VAR),
            (LlmPurpose::Summary, SUMMARY_LLM_API_KEY_ENV_VAR),
        ] {
            let (_lock, env) = EnvGuard::cleared();
            env.set(var, "the-purpose-key");
            EndpointClient::build_for_purpose(&openai_endpoint(), purpose).unwrap_or_else(
                |error| {
                    panic!(
                        "purpose {} should build from {var}: {error}",
                        purpose.as_str()
                    )
                },
            );
            // The helper maps the purpose to exactly this var.
            assert_eq!(purpose.api_key_env_var(), var);
        }
    }

    #[test]
    fn the_purpose_agnostic_build_uses_the_family_key_only() {
        // Decision 87, the unchanged `build` path (direct callers, tests):
        // a purpose key set WITHOUT the family key does NOT reach the
        // purpose-agnostic `build` — it still wants the family key.
        let (_lock, env) = EnvGuard::cleared();
        env.set(REPLY_LLM_API_KEY_ENV_VAR, "reply-purpose-key");
        match EndpointClient::build(&openai_endpoint()) {
            Err(AgentError::ProviderConfig(message)) => {
                assert!(message.contains(OPENAI_API_KEY_ENV_VAR));
            }
            other => panic!(
                "build without a family key must fail even with a purpose key set, got {other:?}"
            ),
        }
        // With the family key present, `build` succeeds.
        env.set(OPENAI_API_KEY_ENV_VAR, "family-key");
        EndpointClient::build(&openai_endpoint())
            .expect("family key builds the purpose-agnostic client");
    }

    #[test]
    fn mode_parse_accepts_the_three_spec_strings() {
        use std::str::FromStr as _;
        assert_eq!(
            StructuredOutputMode::from_str("schema").unwrap(),
            StructuredOutputMode::Schema
        );
        assert_eq!(
            StructuredOutputMode::from_str("json_object").unwrap(),
            StructuredOutputMode::JsonObject
        );
        assert_eq!(
            StructuredOutputMode::from_str("prompt_only").unwrap(),
            StructuredOutputMode::PromptOnly
        );
        assert_eq!(StructuredOutputMode::Schema.as_str(), "schema");
        assert_eq!(StructuredOutputMode::JsonObject.as_str(), "json_object");
        assert_eq!(StructuredOutputMode::PromptOnly.as_str(), "prompt_only");
        assert_eq!(StructuredOutputMode::JsonObject.to_string(), "json_object");
        assert_eq!(
            StructuredOutputMode::default(),
            StructuredOutputMode::Schema
        );
    }

    #[test]
    fn mode_parse_rejects_unknown_strings() {
        use std::str::FromStr as _;
        for bad in ["json", "strict", "", "SCHEMA", "json-object", "none"] {
            match StructuredOutputMode::from_str(bad) {
                Err(AgentError::ProviderConfig(_)) => {}
                other => panic!("expected ProviderConfig for {bad:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn structured_output_resolution_follows_the_precedence_chain() {
        let (_lock, env) = EnvGuard::cleared();
        // Default: schema.
        let endpoints = LlmEndpoints::resolve(&LlmConfigValues::default()).unwrap();
        assert_eq!(
            endpoints.digest.structured_output,
            StructuredOutputMode::Schema
        );
        // Global config.
        let values = LlmConfigValues {
            structured_output: Some("prompt_only".to_string()),
            ..LlmConfigValues::default()
        };
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        assert_eq!(
            endpoints.gate.structured_output,
            StructuredOutputMode::PromptOnly
        );
        // Purpose config wins over global config.
        let values = LlmConfigValues {
            structured_output: Some("prompt_only".to_string()),
            digest_structured_output: Some("json_object".to_string()),
            ..LlmConfigValues::default()
        };
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        assert_eq!(
            endpoints.digest.structured_output,
            StructuredOutputMode::JsonObject
        );
        assert_eq!(
            endpoints.gate.structured_output,
            StructuredOutputMode::PromptOnly
        );
        // Global env wins over purpose config.
        env.set(STRUCTURED_OUTPUT_ENV_VAR, "schema");
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        assert_eq!(
            endpoints.digest.structured_output,
            StructuredOutputMode::Schema
        );
        // Purpose env wins over global env.
        env.set(DIGEST_STRUCTURED_OUTPUT_ENV_VAR, "json_object");
        env.set(GATE_STRUCTURED_OUTPUT_ENV_VAR, "prompt_only");
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        assert_eq!(
            endpoints.digest.structured_output,
            StructuredOutputMode::JsonObject
        );
        assert_eq!(
            endpoints.gate.structured_output,
            StructuredOutputMode::PromptOnly
        );
        // The reply purpose env applies to the reply endpoint only.
        env.set(REPLY_STRUCTURED_OUTPUT_ENV_VAR, "json_object");
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        assert_eq!(
            endpoints.reply.structured_output,
            StructuredOutputMode::JsonObject
        );
        // The summary purpose env applies to the summary endpoint only.
        env.set(SUMMARY_STRUCTURED_OUTPUT_ENV_VAR, "prompt_only");
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        assert_eq!(
            endpoints.summary.structured_output,
            StructuredOutputMode::PromptOnly
        );
        assert_eq!(
            endpoints.reply.structured_output,
            StructuredOutputMode::JsonObject
        );
    }

    #[test]
    fn empty_structured_output_values_count_as_unset() {
        let (_lock, env) = EnvGuard::cleared();
        env.set(DIGEST_STRUCTURED_OUTPUT_ENV_VAR, "");
        env.set(STRUCTURED_OUTPUT_ENV_VAR, "");
        let values = LlmConfigValues {
            structured_output: Some("json_object".to_string()),
            digest_structured_output: Some(String::new()),
            ..LlmConfigValues::default()
        };
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        // The empty purpose env and purpose config fall through to the
        // global config.
        assert_eq!(
            endpoints.digest.structured_output,
            StructuredOutputMode::JsonObject
        );
    }

    #[test]
    fn an_unknown_structured_output_string_is_a_provider_config_error() {
        let (_lock, env) = EnvGuard::cleared();
        // From the global config.
        let values = LlmConfigValues {
            structured_output: Some("yaml".to_string()),
            ..LlmConfigValues::default()
        };
        match LlmEndpoints::resolve(&values) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
        // From a purpose config.
        let values = LlmConfigValues {
            gate_structured_output: Some("bogus".to_string()),
            ..LlmConfigValues::default()
        };
        match LlmEndpoints::resolve(&values) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
        // From the summary purpose config.
        let values = LlmConfigValues {
            summary_structured_output: Some("bogus".to_string()),
            ..LlmConfigValues::default()
        };
        match LlmEndpoints::resolve(&values) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
        // From the global env.
        env.set(STRUCTURED_OUTPUT_ENV_VAR, "bogus");
        match LlmEndpoints::resolve(&LlmConfigValues::default()) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
        // From a purpose env (wins over every other source).
        env.set(STRUCTURED_OUTPUT_ENV_VAR, "schema");
        env.set(REPLY_STRUCTURED_OUTPUT_ENV_VAR, "bogus");
        match LlmEndpoints::resolve(&LlmConfigValues::default()) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
        // From the summary purpose env.
        env.set(REPLY_STRUCTURED_OUTPUT_ENV_VAR, "schema");
        env.set(SUMMARY_STRUCTURED_OUTPUT_ENV_VAR, "bogus");
        match LlmEndpoints::resolve(&LlmConfigValues::default()) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
    }

    #[test]
    fn the_client_stores_and_reports_the_resolved_mode() {
        // Client construction performs no I/O; no network call here.
        let (_lock, env) = EnvGuard::cleared();
        env.set(OPENAI_API_KEY_ENV_VAR, "test-openai-key");
        let endpoint = EndpointConfig {
            api: LlmApi::OpenAiCompatible,
            base_url: Some("http://localhost:9998/v1".to_string()),
            model: "local-model".to_string(),
            structured_output: StructuredOutputMode::JsonObject,
            session_id: DEFAULT_SESSION_ID.to_string(),
        };
        let client = EndpointClient::build(&endpoint).expect("openai client");
        let debug = format!("{client:?}");
        // Family and mode print with their derived Debug names.
        assert!(debug.contains("OpenAiCompatible"));
        assert!(debug.contains("JsonObject"));
    }

    #[test]
    fn the_default_timeout_is_the_endpoint_constant() {
        // Client construction performs no I/O; no network call here.
        let (_lock, env) = EnvGuard::cleared();
        env.set(OPENAI_API_KEY_ENV_VAR, "test-openai-key");
        let endpoint = EndpointConfig {
            api: LlmApi::OpenAiCompatible,
            base_url: Some("http://localhost:9998/v1".to_string()),
            model: "local-model".to_string(),
            structured_output: StructuredOutputMode::Schema,
            session_id: DEFAULT_SESSION_ID.to_string(),
        };
        let client = EndpointClient::build(&endpoint).expect("openai client");
        // H4b: no config key; production always uses the constant.
        assert_eq!(client.timeout, ENDPOINT_TIMEOUT);
    }

    #[test]
    fn the_client_carries_the_model_name_and_a_stamped_purpose() {
        // The curated per-call usage INFO line (decision 53) reads its
        // purpose and model fields off the client. Client construction
        // performs no I/O; no network call here.
        let (_lock, env) = EnvGuard::cleared();
        env.set(OPENAI_API_KEY_ENV_VAR, "test-openai-key");
        let endpoint = EndpointConfig {
            api: LlmApi::OpenAiCompatible,
            base_url: Some("http://localhost:9998/v1".to_string()),
            model: "local-model".to_string(),
            structured_output: StructuredOutputMode::Schema,
            session_id: DEFAULT_SESSION_ID.to_string(),
        };
        let client = EndpointClient::build(&endpoint).expect("openai client");
        // `build` copies the resolved model name and, lacking a purpose
        // signal on EndpointConfig, starts unscoped.
        assert_eq!(client.model_name, "local-model");
        assert_eq!(client.purpose, UNSCOPED_PURPOSE);
        // The purpose-named construction (decision 87) stamps the purpose
        // AND resolves the key — the production path of the wrappers.
        let client = EndpointClient::build_for_purpose(&endpoint, LlmPurpose::Reply)
            .expect("build_for_purpose");
        assert_eq!(client.purpose, "reply");
        assert_eq!(client.model_name, "local-model");
        // Both dims are printable in test failures and logs.
        let debug = format!("{client:?}");
        assert!(debug.contains("local-model"));
        assert!(debug.contains("reply"));
    }

    // --- The embedding endpoint (decision 66) ---

    #[test]
    fn embedding_resolution_defaults_to_the_decision_66_values() {
        let (_lock, _env) = EnvGuard::cleared();
        let endpoint = EmbeddingEndpoint::resolve(&LlmConfigValues::default());
        assert_eq!(
            endpoint,
            EmbeddingEndpoint {
                base_url: DEFAULT_EMBEDDING_BASE_URL.to_string(),
                model: DEFAULT_EMBEDDING_MODEL.to_string(),
                session_id: DEFAULT_SESSION_ID.to_string(),
            }
        );
    }

    #[test]
    fn embedding_resolution_env_wins_over_config() {
        let (_lock, env) = EnvGuard::cleared();
        let values = LlmConfigValues {
            embedding_model: Some("config-embedding-model".to_string()),
            embedding_llm_base_url: Some("https://config.example/v1".to_string()),
            llm_session_id: Some("config-session".to_string()),
            ..LlmConfigValues::default()
        };
        // Config wins over the defaults.
        let endpoint = EmbeddingEndpoint::resolve(&values);
        assert_eq!(endpoint.model, "config-embedding-model");
        assert_eq!(endpoint.base_url, "https://config.example/v1");
        // The session id shares the global chain of the purposes.
        assert_eq!(endpoint.session_id, "config-session");
        // The env wins over the config.
        env.set(EMBEDDING_MODEL_ENV_VAR, "env-embedding-model");
        env.set(EMBEDDING_BASE_URL_ENV_VAR, "https://env.example/v1");
        let endpoint = EmbeddingEndpoint::resolve(&values);
        assert_eq!(endpoint.model, "env-embedding-model");
        assert_eq!(endpoint.base_url, "https://env.example/v1");
    }

    #[test]
    fn empty_embedding_values_count_as_unset() {
        let (_lock, env) = EnvGuard::cleared();
        env.set(EMBEDDING_MODEL_ENV_VAR, "");
        env.set(EMBEDDING_BASE_URL_ENV_VAR, "");
        let values = LlmConfigValues {
            embedding_model: Some(String::new()),
            embedding_llm_base_url: Some(String::new()),
            ..LlmConfigValues::default()
        };
        let endpoint = EmbeddingEndpoint::resolve(&values);
        assert_eq!(endpoint.model, DEFAULT_EMBEDDING_MODEL);
        assert_eq!(endpoint.base_url, DEFAULT_EMBEDDING_BASE_URL);
        // An empty env falls through to the config.
        let values = LlmConfigValues {
            embedding_model: Some("config-embedding-model".to_string()),
            ..LlmConfigValues::default()
        };
        let endpoint = EmbeddingEndpoint::resolve(&values);
        assert_eq!(endpoint.model, "config-embedding-model");
    }

    // --- The caption endpoint (decision 82 (c)) ---

    #[test]
    fn caption_resolution_defaults_to_the_decision_82_values() {
        let (_lock, _env) = EnvGuard::cleared();
        let endpoint = CaptionEndpoint::resolve(&LlmConfigValues::default());
        assert_eq!(
            endpoint,
            CaptionEndpoint {
                base_url: DEFAULT_CAPTION_BASE_URL.to_string(),
                model: DEFAULT_CAPTION_MODEL.to_string(),
                session_id: DEFAULT_SESSION_ID.to_string(),
            }
        );
    }

    #[test]
    fn caption_resolution_env_wins_over_config() {
        let (_lock, env) = EnvGuard::cleared();
        let values = LlmConfigValues {
            caption_model: Some("config-caption-model".to_string()),
            caption_llm_base_url: Some("https://config.example/v1".to_string()),
            llm_session_id: Some("config-session".to_string()),
            ..LlmConfigValues::default()
        };
        // Config wins over the defaults.
        let endpoint = CaptionEndpoint::resolve(&values);
        assert_eq!(endpoint.model, "config-caption-model");
        assert_eq!(endpoint.base_url, "https://config.example/v1");
        // The session id shares the global chain of the purposes.
        assert_eq!(endpoint.session_id, "config-session");
        // The env wins over the config.
        env.set(CAPTION_MODEL_ENV_VAR, "env-caption-model");
        env.set(CAPTION_BASE_URL_ENV_VAR, "https://env.example/v1");
        let endpoint = CaptionEndpoint::resolve(&values);
        assert_eq!(endpoint.model, "env-caption-model");
        assert_eq!(endpoint.base_url, "https://env.example/v1");
    }

    #[test]
    fn empty_caption_values_count_as_unset() {
        let (_lock, env) = EnvGuard::cleared();
        env.set(CAPTION_MODEL_ENV_VAR, "");
        env.set(CAPTION_BASE_URL_ENV_VAR, "");
        let values = LlmConfigValues {
            caption_model: Some(String::new()),
            caption_llm_base_url: Some(String::new()),
            ..LlmConfigValues::default()
        };
        let endpoint = CaptionEndpoint::resolve(&values);
        assert_eq!(endpoint.model, DEFAULT_CAPTION_MODEL);
        assert_eq!(endpoint.base_url, DEFAULT_CAPTION_BASE_URL);
        // An empty env falls through to the config.
        let values = LlmConfigValues {
            caption_model: Some("config-caption-model".to_string()),
            ..LlmConfigValues::default()
        };
        let endpoint = CaptionEndpoint::resolve(&values);
        assert_eq!(endpoint.model, "config-caption-model");
    }

    #[test]
    fn embedding_build_without_an_api_key_degrades_to_none() {
        // The degrade seam: a missing OPENAI_API_KEY is a WARN + None,
        // never a hard error (the per-purpose degrade shape of the
        // binary).
        let (_lock, env) = EnvGuard::cleared();
        let endpoint = EmbeddingEndpoint::resolve(&LlmConfigValues::default());
        match RigEmbeddingProvider::from_endpoint(&endpoint) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
        assert!(RigEmbeddingProvider::build(&endpoint).is_none());
        // An empty key counts as missing.
        env.set(OPENAI_API_KEY_ENV_VAR, "");
        assert!(RigEmbeddingProvider::build(&endpoint).is_none());
    }

    #[test]
    fn embedding_build_constructs_a_client_with_a_key() {
        // Client construction performs no I/O; no network call here.
        let (_lock, env) = EnvGuard::cleared();
        env.set(OPENAI_API_KEY_ENV_VAR, "test-openai-key");
        let endpoint = EmbeddingEndpoint {
            base_url: "http://localhost:9998/v1".to_string(),
            model: "local-embedding-model".to_string(),
            session_id: "test-session-embedding".to_string(),
        };
        let provider = RigEmbeddingProvider::build(&endpoint).expect("the provider builds");
        let debug = format!("{provider:?}");
        assert!(debug.contains("local-embedding-model"));
        assert!(debug.contains(&EMBEDDING_DIMS.to_string()));
    }

    #[test]
    fn embedding_build_with_an_invalid_session_id_degrades_to_none() {
        // The same ProviderConfig class as EndpointClient::build: a
        // newline is never a valid header value.
        let (_lock, env) = EnvGuard::cleared();
        env.set(OPENAI_API_KEY_ENV_VAR, "test-openai-key");
        let endpoint = EmbeddingEndpoint {
            base_url: DEFAULT_EMBEDDING_BASE_URL.to_string(),
            model: DEFAULT_EMBEDDING_MODEL.to_string(),
            session_id: "bad\nsession".to_string(),
        };
        match RigEmbeddingProvider::from_endpoint(&endpoint) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
        assert!(RigEmbeddingProvider::build(&endpoint).is_none());
    }

    #[test]
    fn the_dimension_pin_accepts_exactly_3072_and_narrows_to_f32() {
        let vec: Vec<f64> = (0..EMBEDDING_DIMS).map(|i| i as f64 * 0.5).collect();
        let narrowed = checked_embedding_vector(vec).expect("3072 passes the pin");
        assert_eq!(narrowed.len(), EMBEDDING_DIMS);
        assert_eq!(narrowed[3], 1.5_f32);
    }

    #[test]
    fn the_dimension_pin_rejects_any_other_length() {
        for wrong in [
            vec![0.0; EMBEDDING_DIMS - 1],
            vec![0.0; EMBEDDING_DIMS + 1],
            Vec::new(),
        ] {
            match checked_embedding_vector(wrong) {
                Err(AgentError::Extraction(message)) => {
                    assert!(
                        message.starts_with("embedding dimension mismatch"),
                        "message: {message}"
                    );
                }
                other => panic!("expected an Extraction error, got {other:?}"),
            }
        }
    }

    #[test]
    fn the_batch_dimension_pin_accepts_n_vectors_of_3072_in_input_order() {
        let vecs: Vec<Vec<f64>> = (0..3)
            .map(|row| (0..EMBEDDING_DIMS).map(|i| (row + i) as f64).collect())
            .collect();

        let narrowed = checked_batch_vectors(vecs).expect("three 3072-vectors pass the pin");

        assert_eq!(narrowed.len(), 3);
        // Input order carried through; every element pinned + narrowed.
        for (row, vector) in narrowed.iter().enumerate() {
            assert_eq!(vector.len(), EMBEDDING_DIMS);
            assert_eq!(vector[1], (row + 1) as f32);
        }
    }

    #[test]
    fn the_batch_dimension_pin_rejects_a_batch_with_one_wrong_length() {
        let mut vecs = vec![vec![0.0; EMBEDDING_DIMS]; 2];
        vecs.insert(1, vec![0.0; EMBEDDING_DIMS - 1]);

        match checked_batch_vectors(vecs) {
            Err(AgentError::Extraction(message)) => {
                assert!(
                    message.starts_with("embedding dimension mismatch"),
                    "message: {message}"
                );
            }
            other => panic!("expected an Extraction error, got {other:?}"),
        }
    }

    #[test]
    fn the_batch_dimension_pin_passes_an_empty_batch_through() {
        let narrowed = checked_batch_vectors(Vec::new()).expect("empty batch");
        assert!(narrowed.is_empty());
    }

    #[tokio::test]
    async fn an_empty_batch_never_reaches_the_endpoint() {
        // The provider points at an unreachable port: any provider
        // call would fail fast with a transport error. The empty-input
        // guard returns before the call, so this succeeds offline.
        // The env guards are scoped so no MutexGuard is held across
        // the await.
        let provider = {
            let (_lock, env) = EnvGuard::cleared();
            env.set(OPENAI_API_KEY_ENV_VAR, "test-openai-key");
            let endpoint = EmbeddingEndpoint {
                base_url: "http://localhost:9998/v1".to_string(),
                model: "local-embedding-model".to_string(),
                session_id: "test-session-embedding".to_string(),
            };
            RigEmbeddingProvider::build(&endpoint).expect("the provider builds")
        };

        let vectors = EmbeddingProvider::embed_texts(&provider, &[])
            .await
            .expect("the empty-input guard returns before any provider call");
        assert!(vectors.is_empty());
    }

    // --- Wire fakes (local TcpListener, no external network) ---

    /// The canned OpenAI chat-completion body of the wire fakes.
    const CANNED_COMPLETION: &str = concat!(
        r#"{"id":"x","object":"chat.completion","model":"test-model","#,
        r#""choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"#,
        r#""usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#
    );

    /// Reads one HTTP request (head + the body per Content-Length) off
    /// the stream, so the socket closes without unread data (the same
    /// discipline as the_session_id_header_reaches_the_wire). Returns
    /// (head, body).
    fn read_request(stream: &mut std::net::TcpStream) -> (String, String) {
        use std::io::Read as _;
        let mut raw = Vec::new();
        let mut buffer = [0_u8; 4096];
        let head_end = loop {
            let read = stream.read(&mut buffer).expect("a readable request");
            raw.extend_from_slice(&buffer[..read]);
            if let Some(position) = raw
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|position| position + 4)
            {
                break position;
            }
        };
        let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
        let content_length: usize = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.trim()
                    .eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().ok())?
            })
            .expect("a content-length header");
        while raw.len() < head_end + content_length {
            let read = stream.read(&mut buffer).expect("a readable body");
            raw.extend_from_slice(&buffer[..read]);
        }
        let body = String::from_utf8_lossy(&raw[head_end..head_end + content_length]).to_string();
        (head, body)
    }

    /// Writes the canned OpenAI chat-completion response. The
    /// `connection: close` header makes every request its own
    /// connection (no keep-alive ambiguity in the fake).
    fn write_canned_response(stream: &mut std::net::TcpStream) {
        use std::io::Write as _;
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            CANNED_COMPLETION.len(),
            CANNED_COMPLETION
        )
        .expect("the response is writable");
    }

    /// A local HTTP fake: accepts `requests` connections in order,
    /// captures each request body, and answers the canned OpenAI
    /// chat-completion response. No external network.
    fn spawn_capture_server(
        requests: usize,
    ) -> (
        u16,
        std::sync::mpsc::Receiver<String>,
        std::thread::JoinHandle<()>,
    ) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("the listener binds");
        let port = listener.local_addr().expect("a local address").port();
        let (body_tx, body_rx) = std::sync::mpsc::channel::<String>();
        let server = std::thread::spawn(move || {
            for _ in 0..requests {
                let (mut stream, _) = listener.accept().expect("one connection");
                let (_head, body) = read_request(&mut stream);
                body_tx.send(body).expect("the body reaches the test");
                write_canned_response(&mut stream);
            }
        });
        (port, body_rx, server)
    }

    /// Builds an OpenAI-family client against a local fake (the API key
    /// is read at build time only, so the env guard drops BEFORE any
    /// await: no lock is held across it).
    fn local_openai_client(port: u16, mode: StructuredOutputMode) -> EndpointClient {
        let endpoint = EndpointConfig {
            api: LlmApi::OpenAiCompatible,
            base_url: Some(format!("http://127.0.0.1:{port}/v1")),
            model: "test-model".to_string(),
            structured_output: mode,
            session_id: DEFAULT_SESSION_ID.to_string(),
        };
        let (_lock, env) = EnvGuard::cleared();
        env.set(OPENAI_API_KEY_ENV_VAR, "test-openai-key");
        EndpointClient::build(&endpoint).expect("openai client")
    }

    // --- The session id (`x-opencode-session` + `x-session-id`
    // headers, decision 84 (a)) ---

    #[test]
    fn session_id_resolution_follows_the_precedence_chain() {
        let (_lock, env) = EnvGuard::cleared();
        // Default: "tamako", the same PREFIX in every purpose
        // (global-only, decision 84 (d)); resolve is group-agnostic,
        // so no suffix is joined here.
        let endpoints = LlmEndpoints::resolve(&LlmConfigValues::default()).unwrap();
        for endpoint in [
            &endpoints.digest,
            &endpoints.gate,
            &endpoints.reply,
            &endpoints.summary,
        ] {
            assert_eq!(endpoint.session_id, DEFAULT_SESSION_ID);
        }
        // Global config wins over the default.
        let values = LlmConfigValues {
            llm_session_id: Some("config-session".to_string()),
            ..LlmConfigValues::default()
        };
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        assert_eq!(endpoints.digest.session_id, "config-session");
        // The env wins over the config.
        env.set(LLM_SESSION_ID_ENV_VAR, "env-session");
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        assert_eq!(endpoints.gate.session_id, "env-session");
    }

    #[test]
    fn empty_session_id_values_count_as_unset() {
        let (_lock, env) = EnvGuard::cleared();
        env.set(LLM_SESSION_ID_ENV_VAR, "");
        let values = LlmConfigValues {
            llm_session_id: Some(String::new()),
            ..LlmConfigValues::default()
        };
        // Empty env and empty config fall through to the default, so
        // the resolved value is never empty (an empty header is never
        // emitted).
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        assert_eq!(endpoints.reply.session_id, DEFAULT_SESSION_ID);
        // An empty env falls through to the config.
        let values = LlmConfigValues {
            llm_session_id: Some("config-session".to_string()),
            ..LlmConfigValues::default()
        };
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        assert_eq!(endpoints.digest.session_id, "config-session");
    }

    #[test]
    fn an_invalid_session_id_header_value_is_a_provider_config_error() {
        let (_lock, env) = EnvGuard::cleared();
        env.set(OPENAI_API_KEY_ENV_VAR, "test-openai-key");
        let endpoint = EndpointConfig {
            api: LlmApi::OpenAiCompatible,
            base_url: None,
            model: "any-model".to_string(),
            structured_output: StructuredOutputMode::Schema,
            // A newline is never a valid header value.
            session_id: "bad\nsession".to_string(),
        };
        match EndpointClient::build(&endpoint) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
    }

    #[test]
    fn session_header_map_carries_both_affinity_headers_with_the_same_value() {
        // Decision 84 (a): Opencode Go reads x-opencode-session,
        // OpenRouter reads x-session-id; both carry the same id.
        let headers = session_header_map("test-session-xyz").expect("a valid header value");
        assert_eq!(
            headers
                .get("x-opencode-session")
                .expect("the x-opencode-session header"),
            "test-session-xyz"
        );
        assert_eq!(
            headers
                .get("x-session-id")
                .expect("the x-session-id header"),
            "test-session-xyz"
        );
    }

    #[test]
    fn an_invalid_session_id_is_rejected_for_the_header_map() {
        // The same validation gates BOTH affinity headers: the map
        // build fails before either is emitted.
        match session_header_map("bad\nsession") {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
    }

    #[test]
    fn with_session_suffix_joins_prefix_and_suffix_on_all_three_endpoint_kinds() {
        // Decision 84 (b): resolve yields the bare prefix;
        // with_session_suffix joins `{prefix}-{suffix}`.
        let endpoint = EndpointConfig {
            api: LlmApi::OpenAiCompatible,
            base_url: None,
            model: "any-model".to_string(),
            structured_output: StructuredOutputMode::Schema,
            session_id: "tamako".to_string(),
        }
        .with_session_suffix("abc123");
        assert_eq!(endpoint.session_id, "tamako-abc123");
        let embedding = EmbeddingEndpoint {
            base_url: "http://localhost:9998/v1".to_string(),
            model: "any-embedding-model".to_string(),
            session_id: "tamako".to_string(),
        }
        .with_session_suffix("abc123");
        assert_eq!(embedding.session_id, "tamako-abc123");
        let caption = CaptionEndpoint {
            base_url: "http://localhost:9997/v1".to_string(),
            model: "any-caption-model".to_string(),
            session_id: "tamako".to_string(),
        }
        .with_session_suffix("abc123");
        assert_eq!(caption.session_id, "tamako-abc123");
    }

    #[test]
    fn the_session_affinity_purpose_strings_match_the_spec() {
        // Decision 84 (b): the binary keys the store's
        // get_or_insert_session_suffix with these six strings; this
        // pin keeps the store keys aligned with the spec's purposes.
        assert_eq!(LlmPurpose::Digest.as_str(), "digest");
        assert_eq!(LlmPurpose::Gate.as_str(), "gate");
        assert_eq!(LlmPurpose::Reply.as_str(), "reply");
        assert_eq!(LlmPurpose::Summary.as_str(), "summary");
        assert_eq!(CAPTION_SESSION_PURPOSE, "caption");
        assert_eq!(EMBEDDING_SESSION_PURPOSE, "embedding");
    }

    /// Wire-level proof: the session id of the endpoint config reaches
    /// the server as BOTH the `x-opencode-session` and the
    /// `x-session-id` header (decision 84 (a)). A local
    /// TcpListener accepts ONE connection, captures the request head,
    /// and answers a minimal OpenAI chat-completion response; no
    /// external network.
    #[tokio::test]
    async fn the_session_id_header_reaches_the_wire() {
        use std::io::{Read as _, Write as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("the listener binds");
        let port = listener.local_addr().expect("a local address").port();
        let (head_tx, head_rx) = std::sync::mpsc::channel::<String>();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("one connection");
            // Read the head, then the body per Content-Length, so the
            // socket closes without unread data.
            let mut raw = Vec::new();
            let mut buffer = [0_u8; 4096];
            let head_end = loop {
                let read = stream.read(&mut buffer).expect("a readable request");
                raw.extend_from_slice(&buffer[..read]);
                if let Some(position) = raw
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| position + 4)
                {
                    break position;
                }
            };
            let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
            let content_length: usize = head
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.trim()
                        .eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().ok())?
                })
                .expect("a content-length header");
            while raw.len() < head_end + content_length {
                let read = stream.read(&mut buffer).expect("a readable body");
                raw.extend_from_slice(&buffer[..read]);
            }
            head_tx.send(head).expect("the head reaches the test");
            let body = concat!(
                r#"{"id":"x","object":"chat.completion","model":"test-model","#,
                r#""choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"#,
                r#""usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#
            );
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .expect("the response is writable");
        });
        let endpoint = EndpointConfig {
            api: LlmApi::OpenAiCompatible,
            base_url: Some(format!("http://127.0.0.1:{port}/v1")),
            model: "test-model".to_string(),
            structured_output: StructuredOutputMode::Schema,
            session_id: "test-session-xyz".to_string(),
        };
        // The API key is read at build time only, so the env guard
        // drops BEFORE the await (no lock is held across it).
        let client = {
            let (_lock, env) = EnvGuard::cleared();
            env.set(OPENAI_API_KEY_ENV_VAR, "test-openai-key");
            EndpointClient::build(&endpoint).expect("openai client")
        };
        let text = client
            .complete(
                Some("p".to_string()),
                vec![Message::user("hi".to_string())],
                None,
                64,
            )
            .await
            .expect("the local endpoint completes");
        assert_eq!(text, "ok");
        let head = head_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the server captured the request head");
        assert!(
            head.lines()
                .any(|line| { line.eq_ignore_ascii_case("x-opencode-session: test-session-xyz") }),
            "the request carried x-opencode-session: test-session-xyz, head:\n{head}"
        );
        assert!(
            head.lines()
                .any(|line| { line.eq_ignore_ascii_case("x-session-id: test-session-xyz") }),
            "the request carried x-session-id: test-session-xyz, head:\n{head}"
        );
        server.join().expect("the server thread joins");
    }

    // --- Decision 93: reasoning-markup sanitation ---

    #[test]
    fn strip_reasoning_markup_passes_clean_text_through_untouched() {
        let outcome = strip_reasoning_markup("the cafe on main street");
        assert_eq!(outcome.text, "the cafe on main street");
        assert_eq!(outcome.stripped_bytes, 0);
    }

    #[test]
    fn strip_reasoning_markup_strips_a_balanced_region() {
        let outcome = strip_reasoning_markup("<think>let me think</think>the answer");
        assert_eq!(outcome.text, "the answer");
        assert_eq!(outcome.stripped_bytes, "<think>let me think</think>".len());
    }

    #[test]
    fn strip_reasoning_markup_strips_multiple_regions() {
        let outcome = strip_reasoning_markup("<think>a</think>one<think>b</think>two");
        assert_eq!(outcome.text, "onetwo");
    }

    #[test]
    fn strip_reasoning_markup_drops_a_reasoning_tail_before_an_orphan_closer() {
        // The observed production shape: a provider-side reasoning
        // parser split at a literal </think> the reasoning itself
        // mentioned, so the content field carried the reasoning tail,
        // the real closer, and the answer.
        let outcome = strip_reasoning_markup(
            "` tags and \"zz\" patterns, joking about the glitch. Good.</think>mrrp……那才不是故障喵",
        );
        assert_eq!(outcome.text, "mrrp……那才不是故障喵");
    }

    #[test]
    fn strip_reasoning_markup_iterates_over_multiple_orphan_closers() {
        let outcome = strip_reasoning_markup("tail one</think>tail two</think>the answer");
        assert_eq!(outcome.text, "the answer");
    }

    #[test]
    fn strip_reasoning_markup_voids_an_unclosed_region() {
        let outcome = strip_reasoning_markup("the start<think>reasoning without end");
        assert_eq!(outcome.text, "the start");
        let outcome = strip_reasoning_markup("<think>only reasoning");
        assert_eq!(outcome.text, "");
    }

    #[test]
    fn strip_reasoning_markup_jumps_a_closer_dense_tail_in_one_pass() {
        // Decision 96 (C3): with no opener in the remainder, every
        // closer is an orphan and the survivor is the tail past the
        // LAST one — one rfind, not a per-closer rescan (quadratic
        // pre-fix: 400 KB of closers measured at 9.4 s). The size
        // makes a regression audible in the suite wall time.
        let mut input = String::with_capacity(420_000);
        for _ in 0..50_000 {
            input.push_str("</think>");
        }
        input.push_str("the answer");
        let outcome = strip_reasoning_markup(&input);
        assert_eq!(outcome.text, "the answer");
        assert_eq!(outcome.stripped_bytes, 50_000 * "</think>".len());
        // The mixed shape: content between the closers is part of the
        // reasoning tail and drops with it.
        let outcome =
            strip_reasoning_markup("tail one</think>tail two</think>tail three</think>the answer");
        assert_eq!(outcome.text, "the answer");
        assert_eq!(
            outcome.stripped_bytes,
            "tail one</think>tail two</think>tail three</think>".len()
        );
    }

    /// Serves one fixed completion body: reads the request (head +
    /// body per Content-Length, so the socket closes without unread
    /// data), answers, closes. The decision-93 wire tests script the
    /// content field through it. No external network.
    fn spawn_fixed_body_server(body: &'static str) -> (u16, std::thread::JoinHandle<()>) {
        use std::io::{Read as _, Write as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("the listener binds");
        let port = listener.local_addr().expect("a local address").port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("one connection");
            let mut raw = Vec::new();
            let mut buffer = [0_u8; 4096];
            let head_end = loop {
                let read = stream.read(&mut buffer).expect("a readable request");
                raw.extend_from_slice(&buffer[..read]);
                if let Some(position) = raw
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| position + 4)
                {
                    break position;
                }
            };
            let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
            let content_length: usize = head
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.trim()
                        .eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().ok())?
                })
                .expect("a content-length header");
            while raw.len() < head_end + content_length {
                let read = stream.read(&mut buffer).expect("a readable body");
                raw.extend_from_slice(&buffer[..read]);
            }
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .expect("the response is writable");
        });
        (port, server)
    }

    /// One `EndpointClient::complete` against a local fixed-body
    /// server. The API key is read at build time only, so the env
    /// guard drops BEFORE the await (no lock is held across it).
    async fn complete_against(port: u16) -> Result<String, AgentError> {
        let endpoint = EndpointConfig {
            api: LlmApi::OpenAiCompatible,
            base_url: Some(format!("http://127.0.0.1:{port}/v1")),
            model: "test-model".to_string(),
            structured_output: StructuredOutputMode::Schema,
            session_id: "test-session".to_string(),
        };
        let client = {
            let (_lock, env) = EnvGuard::cleared();
            env.set(OPENAI_API_KEY_ENV_VAR, "test-openai-key");
            EndpointClient::build(&endpoint).expect("openai client")
        };
        client
            .complete(
                Some("p".to_string()),
                vec![Message::user("hi".to_string())],
                None,
                64,
            )
            .await
    }

    /// Decision 93: a completion whose content carries a reasoning
    /// tail plus a stray closer (the botched provider-side split)
    /// yields the answer only — the full `EndpointClient::complete`
    /// path against a local server.
    #[tokio::test]
    async fn a_reasoning_tail_in_the_content_is_stripped() {
        let body = concat!(
            r#"{"id":"x","object":"chat.completion","model":"test-model","#,
            r#""choices":[{"index":0,"message":{"role":"assistant","content":"reasoning tail about glitchy tags</think>the answer"},"finish_reason":"stop"}],"#,
            r#""usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#
        );
        let (port, server) = spawn_fixed_body_server(body);
        let text = complete_against(port)
            .await
            .expect("the completion succeeds");
        assert_eq!(text, "the answer");
        server.join().expect("the server thread joins");
    }

    /// An all-reasoning response fails closed: the same Extraction
    /// class as a text-less response, so every caller's backoff and
    /// dead-letter discipline applies unchanged.
    #[tokio::test]
    async fn an_all_reasoning_response_is_an_extraction_error() {
        let body = concat!(
            r#"{"id":"x","object":"chat.completion","model":"test-model","#,
            r#""choices":[{"index":0,"message":{"role":"assistant","content":"<think>only reasoning, no answer"},"finish_reason":"stop"}],"#,
            r#""usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#
        );
        let (port, server) = spawn_fixed_body_server(body);
        match complete_against(port).await {
            Err(AgentError::Extraction(message)) => {
                assert_eq!(message, "the response text is entirely reasoning markup");
            }
            other => panic!("expected the all-reasoning Extraction error, got {other:?}"),
        }
        server.join().expect("the server thread joins");
    }

    // --- H4b: the per-attempt timeout ---

    /// A stalled completion fails with the timeout error class
    /// (`AgentError::Extraction` with the `endpoint timeout after`
    /// prefix). The seam is the full `EndpointClient::complete` path
    /// against a local server that reads the request and then NEVER
    /// responds until the test releases it — hermetic (localhost only),
    /// and rig's client needs no special stall hook because the fake
    /// server IS the stall. The tiny timeout is injected through the
    /// test-only `with_timeout` override (no config key).
    #[tokio::test]
    async fn a_stalled_completion_fails_with_a_timeout_error() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("the listener binds");
        let port = listener.local_addr().expect("a local address").port();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("one connection");
            // Read the full request so the client is not blocked on
            // write, then hold the connection open without answering.
            let _ = read_request(&mut stream);
            // Block until the test releases the server; the client has
            // long timed out by then. The stream drops unanswered.
            let _ = release_rx.recv();
        });
        let client = local_openai_client(port, StructuredOutputMode::Schema)
            .with_timeout(Duration::from_millis(100));
        match client
            .complete(None, vec![Message::user("hi".to_string())], None, 64)
            .await
        {
            Err(AgentError::Extraction(message)) => {
                assert!(
                    message.starts_with("endpoint timeout after"),
                    "expected the timeout error class, got {message:?}"
                );
            }
            other => panic!("expected a timeout Extraction error, got {other:?}"),
        }
        drop(release_tx);
        server.join().expect("the server thread joins");
    }

    // --- M1: json_object response_format is per call ---

    /// M1 regression: on a json_object-configured OpenAI endpoint the
    /// `response_format` parameter is PER CALL. A structured call
    /// (schema present) carries `response_format: {"type":
    /// "json_object"}`; a plain-text call (no schema — the reply
    /// generator path) carries NO response_format. Before the fix the
    /// mode attached response_format to every call of the client and
    /// broke plain-text replies on json_object endpoints.
    #[tokio::test]
    async fn json_object_response_format_is_attached_only_with_a_schema() {
        let (port, bodies, server) = spawn_capture_server(2);
        let client = local_openai_client(port, StructuredOutputMode::JsonObject);
        // Plain-text call (the reply path): NO response_format.
        client
            .complete(None, vec![Message::user("hi".to_string())], None, 64)
            .await
            .expect("the plain-text call completes");
        let plain_body = bodies
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the plain-text request body");
        let plain: serde_json::Value =
            serde_json::from_str(&plain_body).expect("the plain-text body is JSON");
        assert!(
            plain.get("response_format").is_none(),
            "a schema-less call must not carry response_format: {plain}"
        );
        // Structured call (schema present): the json_object
        // response_format reaches the wire.
        client
            .complete(
                None,
                vec![Message::user("hi".to_string())],
                Some(schemars::schema_for!(RepairTarget)),
                64,
            )
            .await
            .expect("the structured call completes");
        let structured_body = bodies
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the structured request body");
        let structured: serde_json::Value =
            serde_json::from_str(&structured_body).expect("the structured body is JSON");
        assert_eq!(
            structured.get("response_format"),
            Some(&serde_json::json!({ "type": "json_object" })),
            "a schema-carrying call must carry the json_object response_format: {structured}"
        );
        server.join().expect("the server thread joins");
    }

    // --- The repair retry flow (scripted completion closure, no
    // network) ---

    /// The parse target of the repair-flow tests.
    #[derive(Debug, PartialEq, serde::Deserialize, schemars::JsonSchema)]
    struct RepairTarget {
        value: u32,
    }

    /// The text of the last (prompt) message of a call.
    fn prompt_text(call: &CompletionCall) -> String {
        let Some(Message::User { content }) = call.messages.last() else {
            return String::new();
        };
        content
            .iter()
            .find_map(|content| match content {
                rig::completion::message::UserContent::Text(text) => Some(text.text.clone()),
                _ => None,
            })
            .unwrap_or_default()
    }

    /// One recorded call: the preamble, the prompt text, and the schema
    /// as JSON (for equality assertions).
    type RecordedCall = (Option<String>, String, serde_json::Value);

    /// A scripted completion closure: pops the next response per call
    /// and records every call. `RefCell` suffices: `#[tokio::test]` is
    /// single-threaded by default.
    struct ScriptedFlow {
        calls: std::cell::RefCell<Vec<RecordedCall>>,
        responses: std::cell::RefCell<std::collections::VecDeque<Result<String, AgentError>>>,
    }

    impl ScriptedFlow {
        fn new(responses: Vec<Result<String, AgentError>>) -> Self {
            ScriptedFlow {
                calls: std::cell::RefCell::new(Vec::new()),
                responses: std::cell::RefCell::new(responses.into()),
            }
        }

        async fn run(&self, label: &str) -> Result<RepairTarget, AgentError> {
            let schema = schemars::schema_for!(RepairTarget);
            complete_structured_with(
                |call| {
                    self.calls.borrow_mut().push((
                        call.preamble.clone(),
                        prompt_text(&call),
                        serde_json::to_value(&call.schema).expect("schema to json"),
                    ));
                    let next = self
                        .responses
                        .borrow_mut()
                        .pop_front()
                        .expect("a scripted response per call");
                    async move { next }
                },
                Some("test preamble".to_string()),
                vec![Message::user("test prompt".to_string())],
                schema,
                1024,
                label,
                None,
            )
            .await
        }

        fn calls(&self) -> Vec<RecordedCall> {
            self.calls.borrow().clone()
        }
    }

    #[tokio::test]
    async fn repair_succeeds_after_a_schema_validation_failure() {
        let flow = ScriptedFlow::new(vec![
            // Valid JSON, invalid shape: the repair trigger.
            Ok(r#"{"valu":7}"#.to_string()),
            Ok(r#"{"value":7}"#.to_string()),
        ]);
        let result = flow.run("invalid test JSON").await.expect("repaired");
        assert_eq!(result, RepairTarget { value: 7 });
        let calls = flow.calls();
        assert_eq!(calls.len(), 2);
        // The repair call carries the repair preamble and a prompt with
        // the broken JSON, the validation error, and the schema.
        assert_eq!(calls[1].0.as_deref(), Some(REPAIR_PREAMBLE));
        assert!(calls[1].1.contains(r#"{"valu":7}"#));
        assert!(calls[1].1.contains("missing field"));
        assert!(calls[1].1.contains(r#""value""#));
        // Both calls carry the same schema; the mode decides on the
        // wire inside EndpointClient::complete.
        assert_eq!(calls[0].2, calls[1].2);
    }

    #[tokio::test]
    async fn a_failed_repair_returns_the_original_error() {
        let flow = ScriptedFlow::new(vec![
            Ok(r#"{"valu":7}"#.to_string()),
            Ok(r#"{"still":"broken"}"#.to_string()),
        ]);
        match flow.run("invalid test JSON").await {
            Err(AgentError::Extraction(message)) => {
                assert!(message.starts_with("invalid test JSON: missing field"));
            }
            other => panic!("expected the original Extraction error, got {other:?}"),
        }
        assert_eq!(flow.calls().len(), 2);
    }

    #[tokio::test]
    async fn a_valid_first_response_needs_no_repair() {
        let flow = ScriptedFlow::new(vec![Ok(r#"{"value":42}"#.to_string())]);
        let result = flow.run("invalid test JSON").await.expect("first try");
        assert_eq!(result, RepairTarget { value: 42 });
        assert_eq!(flow.calls().len(), 1);
    }

    #[tokio::test]
    async fn non_json_text_returns_the_original_error_without_a_repair() {
        let flow = ScriptedFlow::new(vec![Ok("I cannot help with that.".to_string())]);
        match flow.run("invalid test JSON").await {
            Err(AgentError::Extraction(message)) => {
                assert!(message.starts_with("invalid test JSON:"));
            }
            other => panic!("expected the original Extraction error, got {other:?}"),
        }
        // Non-JSON text is a different failure class: no repair call.
        assert_eq!(flow.calls().len(), 1);
    }

    #[tokio::test]
    async fn a_failing_repair_call_returns_the_original_error() {
        let flow = ScriptedFlow::new(vec![
            Ok(r#"{"valu":7}"#.to_string()),
            Err(AgentError::Extraction("the provider is down".to_string())),
        ]);
        match flow.run("invalid test JSON").await {
            Err(AgentError::Extraction(message)) => {
                // The ORIGINAL parse error, never the repair call's own
                // error: the caller's backoff discipline applies.
                assert!(message.starts_with("invalid test JSON: missing field"));
            }
            other => panic!("expected the original Extraction error, got {other:?}"),
        }
        assert_eq!(flow.calls().len(), 2);
    }
}
