//! LLM endpoint portability (specs.md Section 13).
//!
//! specs.md Section 13: "LLM access is endpoint-portable. Every LLM call
//! uses one of two API families: `anthropic-compatible` or
//! `openai-compatible`. 'Compatible' describes the wire format only, never
//! the vendor." The base URL and the model names are configuration items.
//! A purpose (`digest`, `gate`, `reply`) may override `llm_api` and
//! `llm_base_url` individually; mixed deployments are legal.
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
//!   when the map does not already carry it. The `x-opencode-session`
//!   header of the resolved `session_id` uses exactly this (the
//!   Opencode Go gateway's session-affinity key).
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

use rig::client::CompletionClient;
use rig::completion::{AssistantContent, Message};
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

/// Environment override of the session id of the endpoint, sent as the
/// `x-opencode-session` header (Opencode Go gateway session affinity;
/// provider prompt-cache affinity). Global-only.
pub const LLM_SESSION_ID_ENV_VAR: &str = "TAMAKO_LLM_SESSION_ID";

/// The default session id (reported for spec backfill with
/// `llm_session_id`). One sticky id per deployment.
pub const DEFAULT_SESSION_ID: &str = "tamako";

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
    /// `additional_params`. No schema on the wire. On the Anthropic
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
}

impl LlmPurpose {
    /// The lowercase name of the purpose.
    pub fn as_str(&self) -> &'static str {
        match self {
            LlmPurpose::Digest => "digest",
            LlmPurpose::Gate => "gate",
            LlmPurpose::Reply => "reply",
        }
    }

    /// The env var that overrides the model of the purpose.
    fn model_env_var(&self) -> &'static str {
        match self {
            LlmPurpose::Digest => DIGEST_MODEL_ENV_VAR,
            LlmPurpose::Gate => GATE_MODEL_ENV_VAR,
            LlmPurpose::Reply => REPLY_MODEL_ENV_VAR,
        }
    }

    /// The default model of the purpose (specs.md Section 13).
    fn default_model(&self) -> &'static str {
        match self {
            LlmPurpose::Digest => DEFAULT_DIGEST_MODEL,
            LlmPurpose::Gate => DEFAULT_GATE_MODEL,
            LlmPurpose::Reply => DEFAULT_REPLY_MODEL,
        }
    }

    /// The env var that overrides the structured-output mode of the
    /// purpose.
    fn structured_output_env_var(&self) -> &'static str {
        match self {
            LlmPurpose::Digest => DIGEST_STRUCTURED_OUTPUT_ENV_VAR,
            LlmPurpose::Gate => GATE_STRUCTURED_OUTPUT_ENV_VAR,
            LlmPurpose::Reply => REPLY_STRUCTURED_OUTPUT_ENV_VAR,
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
    /// The resolved session id, sent as the `x-opencode-session`
    /// header on every request of the client (global-only; the same
    /// value in all three purposes). Never empty: resolution falls
    /// back to [`DEFAULT_SESSION_ID`].
    pub session_id: String,
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
    /// Global session id (`llm_session_id`; global-only, no
    /// per-purpose variant).
    pub llm_session_id: Option<String>,
    /// Digest model (`digest_model`).
    pub digest_model: Option<String>,
    /// Gate model (`gate_model`).
    pub gate_model: Option<String>,
    /// Reply model (`reply_model`).
    pub reply_model: Option<String>,
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
}

impl LlmConfigValues {
    /// The purpose-specific API family override, when set.
    fn purpose_llm_api(&self, purpose: LlmPurpose) -> Option<&str> {
        let value = match purpose {
            LlmPurpose::Digest => self.digest_llm_api.as_deref(),
            LlmPurpose::Gate => self.gate_llm_api.as_deref(),
            LlmPurpose::Reply => self.reply_llm_api.as_deref(),
        };
        value.filter(|value| !value.is_empty())
    }

    /// The purpose-specific base URL override, when set.
    fn purpose_llm_base_url(&self, purpose: LlmPurpose) -> Option<&str> {
        let value = match purpose {
            LlmPurpose::Digest => self.digest_llm_base_url.as_deref(),
            LlmPurpose::Gate => self.gate_llm_base_url.as_deref(),
            LlmPurpose::Reply => self.reply_llm_base_url.as_deref(),
        };
        value.filter(|value| !value.is_empty())
    }

    /// The model of the purpose, when set.
    fn purpose_model(&self, purpose: LlmPurpose) -> Option<&str> {
        let value = match purpose {
            LlmPurpose::Digest => self.digest_model.as_deref(),
            LlmPurpose::Gate => self.gate_model.as_deref(),
            LlmPurpose::Reply => self.reply_model.as_deref(),
        };
        value.filter(|value| !value.is_empty())
    }

    /// The purpose-specific structured-output mode override, when set.
    fn purpose_structured_output(&self, purpose: LlmPurpose) -> Option<&str> {
        let value = match purpose {
            LlmPurpose::Digest => self.digest_structured_output.as_deref(),
            LlmPurpose::Gate => self.gate_structured_output.as_deref(),
            LlmPurpose::Reply => self.reply_structured_output.as_deref(),
        };
        value.filter(|value| !value.is_empty())
    }
}

/// Reads an env var. An empty value counts as unset.
fn env_value(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// The resolved endpoints of the three purposes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmEndpoints {
    /// The digest extraction endpoint.
    pub digest: EndpointConfig,
    /// The participation gate endpoint.
    pub gate: EndpointConfig,
    /// The reply generation endpoint.
    pub reply: EndpointConfig,
}

impl LlmEndpoints {
    /// Resolves the three endpoints from the config values and the
    /// environment (specs.md Section 13). Precedence per purpose:
    ///
    /// - API family: env `TAMAKO_LLM_API` → purpose config
    ///   (`digest_llm_api` etc.) → global config `llm_api` → default
    ///   `anthropic-compatible`. An unparsable string (env or config) is
    ///   `AgentError::ProviderConfig`, never a silent default.
    /// - Base URL: env `TAMAKO_LLM_BASE_URL` → purpose config → global
    ///   config `llm_base_url` → `None` (the canonical default of the
    ///   family).
    /// - Model: purpose env (`TAMAKO_DIGEST_MODEL` etc.) → purpose
    ///   config (`digest_model` etc.) → the purpose default.
    /// - Structured-output mode: purpose env
    ///   (`TAMAKO_DIGEST_STRUCTURED_OUTPUT` etc.) → global env
    ///   `TAMAKO_STRUCTURED_OUTPUT` → purpose config
    ///   (`digest_structured_output` etc.) → global config
    ///   `structured_output` → default `schema`. An unparsable string
    ///   (env or config) is `AgentError::ProviderConfig`, never a
    ///   silent default.
    /// - Session id (global-only, the same value in all three
    ///   purposes): env `TAMAKO_LLM_SESSION_ID` → global config
    ///   `llm_session_id` → default [`DEFAULT_SESSION_ID`].
    ///
    /// Empty strings count as unset, in env and config alike.
    pub fn resolve(values: &LlmConfigValues) -> Result<Self, AgentError> {
        Ok(LlmEndpoints {
            digest: Self::resolve_purpose(LlmPurpose::Digest, values)?,
            gate: Self::resolve_purpose(LlmPurpose::Gate, values)?,
            reply: Self::resolve_purpose(LlmPurpose::Reply, values)?,
        })
    }

    fn resolve_purpose(
        purpose: LlmPurpose,
        values: &LlmConfigValues,
    ) -> Result<EndpointConfig, AgentError> {
        let api_string = env_value(LLM_API_ENV_VAR)
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
        let base_url = env_value(LLM_BASE_URL_ENV_VAR)
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

/// Resolves the global session id (module docs): env
/// `TAMAKO_LLM_SESSION_ID` → global config `llm_session_id` →
/// [`DEFAULT_SESSION_ID`]. Empty strings count as unset, so the
/// resolved value is never empty.
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
    /// The resolved structured-output mode of the endpoint (module
    /// docs). `complete` interprets its `output_schema` per this mode.
    structured_output: StructuredOutputMode,
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
            .field("structured_output", &self.structured_output)
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
    /// The resolved `session_id` becomes the `x-opencode-session`
    /// default header of the client (module docs): it is sent on every
    /// request of both API families. A session id that is not a valid
    /// header value is `AgentError::ProviderConfig`.
    pub fn build(endpoint: &EndpointConfig) -> Result<Self, AgentError> {
        let key_var = endpoint.api.api_key_env_var();
        let api_key = env_value(key_var).ok_or_else(|| {
            AgentError::ProviderConfig(format!(
                "missing API key: set {key_var} for {} endpoints",
                endpoint.api
            ))
        })?;
        // The gateway session-affinity header (module docs). The map
        // replaces the client's default headers; `build()` inserts the
        // API-key auth header when the map does not carry it, so the
        // two never clash.
        let mut headers = rig::http_client::HeaderMap::new();
        headers.insert(
            "x-opencode-session",
            rig::http_client::HeaderValue::from_str(&endpoint.session_id).map_err(|error| {
                AgentError::ProviderConfig(format!(
                    "invalid llm_session_id {:?}: {error}",
                    endpoint.session_id
                ))
            })?,
        );
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
                    structured_output: endpoint.structured_output,
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
                    structured_output: endpoint.structured_output,
                })
            }
        }
    }

    /// Sends one completion request and returns the first text content.
    ///
    /// Prompt convention: the last message is the prompt; the preceding
    /// messages go to the chat history. An empty `messages` sends one
    /// empty user message as the prompt. `preamble` becomes the system
    /// message. `output_schema` is interpreted per the resolved
    /// structured-output mode (module docs): `Schema` passes it to rig;
    /// `JsonObject` drops it and adds
    /// `response_format: { type: "json_object" }` on the OpenAI family
    /// only (Anthropic degrades to prompt-only); `PromptOnly` drops it
    /// unconditionally. `max_tokens` is always set (Anthropic requires
    /// it).
    ///
    /// Errors: provider errors and a response without text content are
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
        let (output_schema, json_object) = match self.structured_output {
            StructuredOutputMode::Schema => (output_schema, false),
            StructuredOutputMode::JsonObject => {
                (None, matches!(self.model, EndpointModel::OpenAi(_)))
            }
            StructuredOutputMode::PromptOnly => (None, false),
        };
        match &self.model {
            EndpointModel::Anthropic(model) => {
                complete_with(model, preamble, messages, output_schema, max_tokens, false).await
            }
            EndpointModel::OpenAi(model) => {
                complete_with(
                    model,
                    preamble,
                    messages,
                    output_schema,
                    max_tokens,
                    json_object,
                )
                .await
            }
        }
    }

    /// One structured completion with the repair retry of the module
    /// docs: the shared flow of extraction, gate, and recall.
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
        )
        .await
    }
}

/// The shared completion flow of both families. The request shape is
/// identical; only the rig model type differs. `json_object` adds the
/// OpenAI `json_object` response format via `additional_params`
/// (expressible on the chat-completions path only; the caller sets it
/// for the OpenAI family only).
async fn complete_with<M>(
    model: &M,
    preamble: Option<String>,
    messages: Vec<Message>,
    output_schema: Option<schemars::Schema>,
    max_tokens: u64,
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
    // Always set max_tokens: Anthropic requires it.
    let response = request
        .max_tokens(max_tokens)
        .send()
        .await
        .map_err(|error| AgentError::Extraction(error.to_string()))?;
    // Per-call usage at DEBUG (decision 57): the cache fields expose
    // the provider prompt-cache behavior of the session-affinity
    // header. The curated INFO lines (decision 53) stay untouched.
    tracing::debug!(
        input_tokens = response.usage.input_tokens,
        cached_input_tokens = response.usage.cached_input_tokens,
        cache_creation_input_tokens = response.usage.cache_creation_input_tokens,
        output_tokens = response.usage.output_tokens,
        "llm completion usage"
    );
    response
        .choice
        .iter()
        .find_map(|content| match content {
            AssistantContent::Text(text) => Some(text.text.clone()),
            _ => None,
        })
        .ok_or_else(|| AgentError::Extraction("no text content in the response".to_string()))
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
/// with a closure over `EndpointClient::complete`; tests script the
/// closure. Refer to `complete_structured` for the retry semantics.
async fn complete_structured_with<T, F, Fut>(
    complete: F,
    preamble: Option<String>,
    messages: Vec<Message>,
    schema: schemars::Schema,
    max_tokens: u64,
    error_label: &str,
) -> Result<T, AgentError>
where
    T: serde::de::DeserializeOwned,
    F: Fn(CompletionCall) -> Fut,
    Fut: std::future::Future<Output = Result<String, AgentError>>,
{
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
        STRUCTURED_OUTPUT_ENV_VAR,
        DIGEST_STRUCTURED_OUTPUT_ENV_VAR,
        GATE_STRUCTURED_OUTPUT_ENV_VAR,
        REPLY_STRUCTURED_OUTPUT_ENV_VAR,
        ANTHROPIC_API_KEY_ENV_VAR,
        OPENAI_API_KEY_ENV_VAR,
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
            ..LlmConfigValues::default()
        };
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        for endpoint in [&endpoints.digest, &endpoints.gate, &endpoints.reply] {
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
        let values = LlmConfigValues {
            digest_model: Some("config-digest-model".to_string()),
            gate_model: Some("config-gate-model".to_string()),
            reply_model: Some("config-reply-model".to_string()),
            ..LlmConfigValues::default()
        };
        let endpoints = LlmEndpoints::resolve(&values).unwrap();
        assert_eq!(endpoints.digest.model, "env-digest-model");
        assert_eq!(endpoints.gate.model, "env-gate-model");
        assert_eq!(endpoints.reply.model, "env-reply-model");
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
        // From the environment.
        env.set(LLM_API_ENV_VAR, "bogus");
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

    // --- The session id (`x-opencode-session` header) ---

    #[test]
    fn session_id_resolution_follows_the_precedence_chain() {
        let (_lock, env) = EnvGuard::cleared();
        // Default: "tamako", the same value in all three purposes
        // (global-only).
        let endpoints = LlmEndpoints::resolve(&LlmConfigValues::default()).unwrap();
        for endpoint in [&endpoints.digest, &endpoints.gate, &endpoints.reply] {
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

    /// Wire-level proof: the session id of the endpoint config reaches
    /// the server as the `x-opencode-session` header. A local
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
