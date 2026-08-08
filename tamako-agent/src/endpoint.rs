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
        Ok(EndpointConfig {
            api,
            base_url,
            model,
        })
    }
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
    pub fn build(endpoint: &EndpointConfig) -> Result<Self, AgentError> {
        let key_var = endpoint.api.api_key_env_var();
        let api_key = env_value(key_var).ok_or_else(|| {
            AgentError::ProviderConfig(format!(
                "missing API key: set {key_var} for {} endpoints",
                endpoint.api
            ))
        })?;
        match endpoint.api {
            LlmApi::AnthropicCompatible => {
                let mut builder = anthropic::Client::builder().api_key(api_key);
                if let Some(base_url) = &endpoint.base_url {
                    builder = builder.base_url(base_url);
                }
                let client = builder
                    .build()
                    .map_err(|error| AgentError::ProviderConfig(error.to_string()))?;
                Ok(EndpointClient {
                    model: EndpointModel::Anthropic(client.completion_model(&endpoint.model)),
                })
            }
            LlmApi::OpenAiCompatible => {
                // The default openai::Client speaks the Responses API
                // (first-party only in practice). CompletionsClient is
                // the openai-compatible path: POST {base}/chat/completions.
                let mut builder = openai::CompletionsClient::builder().api_key(api_key);
                if let Some(base_url) = &endpoint.base_url {
                    builder = builder.base_url(base_url);
                }
                let client = builder
                    .build()
                    .map_err(|error| AgentError::ProviderConfig(error.to_string()))?;
                Ok(EndpointClient {
                    model: EndpointModel::OpenAi(client.completion_model(&endpoint.model)),
                })
            }
        }
    }

    /// Sends one completion request and returns the first text content.
    ///
    /// Prompt convention: the last message is the prompt; the preceding
    /// messages go to the chat history. An empty `messages` sends one
    /// empty user message as the prompt. `preamble` becomes the system
    /// message. `output_schema` applies only when `Some`. `max_tokens`
    /// is always set (Anthropic requires it).
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
        match &self.model {
            EndpointModel::Anthropic(model) => {
                complete_with(model, preamble, messages, output_schema, max_tokens).await
            }
            EndpointModel::OpenAi(model) => {
                complete_with(model, preamble, messages, output_schema, max_tokens).await
            }
        }
    }
}

/// The shared completion flow of both families. The request shape is
/// identical; only the rig model type differs.
async fn complete_with<M>(
    model: &M,
    preamble: Option<String>,
    messages: Vec<Message>,
    output_schema: Option<schemars::Schema>,
    max_tokens: u64,
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
    // Always set max_tokens: Anthropic requires it.
    let response = request
        .max_tokens(max_tokens)
        .send()
        .await
        .map_err(|error| AgentError::Extraction(error.to_string()))?;
    response
        .choice
        .iter()
        .find_map(|content| match content {
            AssistantContent::Text(text) => Some(text.text.clone()),
            _ => None,
        })
        .ok_or_else(|| AgentError::Extraction("no text content in the response".to_string()))
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
        DIGEST_MODEL_ENV_VAR,
        GATE_MODEL_ENV_VAR,
        REPLY_MODEL_ENV_VAR,
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
            }
        );
        assert_eq!(
            endpoints.gate,
            EndpointConfig {
                api: LlmApi::AnthropicCompatible,
                base_url: None,
                model: "claude-haiku-4-5".to_string(),
            }
        );
        assert_eq!(
            endpoints.reply,
            EndpointConfig {
                api: LlmApi::AnthropicCompatible,
                base_url: None,
                model: "claude-sonnet-4-5".to_string(),
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
        };
        let openai_endpoint = EndpointConfig {
            api: LlmApi::OpenAiCompatible,
            base_url: Some("http://localhost:9998/v1".to_string()),
            model: "local-model".to_string(),
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
        };
        match EndpointClient::build(&endpoint) {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig, got {other:?}"),
        }
    }
}
