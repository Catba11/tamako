//! The live extractor: one completion call on the endpoint layer
//! (`endpoint`, specs.md Section 13) with the `KnowledgeGraph` output
//! schema. rig-core 0.41 has no Extractor type; on Anthropic the output
//! schema maps to native structured output (OutputFormat::JsonSchema),
//! on openai-compatible endpoints to the json_schema response format.
//!
//! The Anthropic default path is unchanged for existing env-only users:
//! `ANTHROPIC_API_KEY` plus all defaults selects the
//! anthropic-compatible family, `claude-haiku-4-5`, and the canonical
//! base URL. The env vars of this module (`DIGEST_MODEL_ENV_VAR`) keep
//! working as thin delegates over the endpoint layer.

use rig::completion::Message;

use crate::endpoint::{EndpointClient, EndpointConfig, LlmConfigValues, LlmEndpoints, LlmPurpose};
use crate::extract::{AgentError, ExtractionInput, KnowledgeExtractor};
use crate::graph::KnowledgeGraph;
use crate::prompt::{render_extraction_prompt, EXTRACTION_PREAMBLE};

pub use crate::endpoint::DIGEST_MODEL_ENV_VAR;

/// The default extraction model: the cheap-tier Anthropic Claude.
pub const DEFAULT_EXTRACTION_MODEL: &str = crate::endpoint::DEFAULT_DIGEST_MODEL;

/// The default max tokens of the extraction response.
pub const DEFAULT_MAX_TOKENS: u64 = 262144;

/// Extractor configuration. Provider: the endpoint layer (specs.md
/// Section 13), Anthropic family by default.
#[derive(Debug, Clone)]
pub struct ExtractorConfig {
    /// The extraction model. Default: claude-haiku-4-5 (cheap tier).
    pub model: String,
    /// Max tokens of the extraction response. Default 262144.
    pub max_tokens: u64,
}

impl Default for ExtractorConfig {
    fn default() -> Self {
        ExtractorConfig {
            model: DEFAULT_EXTRACTION_MODEL.to_string(),
            max_tokens: DEFAULT_MAX_TOKENS,
        }
    }
}

impl ExtractorConfig {
    /// Resolution order: `TAMAKO_DIGEST_MODEL` env var, then the
    /// config-file value (`digest_model`, specs.md Section 13), then the
    /// default. The API key is NOT here: the endpoint layer reads the
    /// family key (`ANTHROPIC_API_KEY` by default) from the environment
    /// in `EndpointClient::build()`.
    pub fn resolve(config_file_model: Option<&str>) -> Self {
        let model = std::env::var(DIGEST_MODEL_ENV_VAR)
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| config_file_model.map(str::to_string));
        ExtractorConfig {
            model: model.unwrap_or_else(|| DEFAULT_EXTRACTION_MODEL.to_string()),
            ..ExtractorConfig::default()
        }
    }
}

/// The live extractor: a rig completion request with the KnowledgeGraph
/// output schema (rig-core 0.41 native structured output on Anthropic).
pub struct RigExtractor {
    client: EndpointClient,
    max_tokens: u64,
}

// The rig model handle does not implement Debug. A manual impl keeps
// RigExtractor printable in test failures and logs.
impl std::fmt::Debug for RigExtractor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RigExtractor")
            .field("client", &self.client)
            .field("max_tokens", &self.max_tokens)
            .finish_non_exhaustive()
    }
}

impl RigExtractor {
    /// Builds the extractor from an endpoint client.
    pub fn new(client: EndpointClient, max_tokens: u64) -> Self {
        RigExtractor { client, max_tokens }
    }

    /// Builds the extractor for one resolved endpoint (specs.md
    /// Section 13). Returns `AgentError::ProviderConfig` when the family
    /// API key is missing.
    pub fn from_endpoint(endpoint: &EndpointConfig) -> Result<Self, AgentError> {
        Ok(RigExtractor::new(
            EndpointClient::build(endpoint)?.with_purpose(LlmPurpose::Digest.as_str()),
            DEFAULT_MAX_TOKENS,
        ))
    }

    /// Builds the extractor from the environment with all-default
    /// endpoint values: the `config.model` acts as the digest model
    /// config value, so `TAMAKO_DIGEST_MODEL` still wins over it. The
    /// default path selects the anthropic-compatible family, the
    /// canonical base URL, and `ANTHROPIC_API_KEY` from the environment
    /// (specs.md Section 13: API keys come from the environment only).
    /// Returns `AgentError::ProviderConfig` when the key is missing.
    pub fn from_env(config: ExtractorConfig) -> Result<Self, AgentError> {
        let values = LlmConfigValues {
            digest_model: Some(config.model),
            ..LlmConfigValues::default()
        };
        let endpoints = LlmEndpoints::resolve(&values)?;
        let extractor = RigExtractor::from_endpoint(&endpoints.digest)?;
        Ok(RigExtractor {
            max_tokens: config.max_tokens,
            ..extractor
        })
    }
}

impl KnowledgeExtractor for RigExtractor {
    fn extract<'a>(
        &'a self,
        input: &'a ExtractionInput,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<KnowledgeGraph, AgentError>> + Send + 'a>,
    > {
        Box::pin(async move {
            // The shared structured flow of the endpoint layer: one
            // completion with the schema (the resolved mode decides
            // how it reaches the wire) plus the one-shot repair retry
            // on a schema validation failure.
            self.client
                .complete_structured::<KnowledgeGraph>(
                    // The preamble becomes the system message.
                    Some(EXTRACTION_PREAMBLE.to_string()),
                    vec![Message::user(render_extraction_prompt(input))],
                    schemars::schema_for!(KnowledgeGraph),
                    self.max_tokens,
                    "invalid graph JSON",
                )
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::{env_lock::ENV_LOCK, LlmApi};

    #[test]
    fn the_default_model_is_the_cheap_tier() {
        let config = ExtractorConfig::default();
        assert_eq!(config.model, "claude-haiku-4-5");
        assert_eq!(config.max_tokens, 262144);
    }

    #[test]
    fn resolve_prefers_the_env_var_then_the_config_file() {
        // The tests must not depend on the operator environment.
        let _lock = ENV_LOCK.lock().unwrap();
        let saved = std::env::var(DIGEST_MODEL_ENV_VAR).ok();
        std::env::remove_var(DIGEST_MODEL_ENV_VAR);

        let config = ExtractorConfig::resolve(Some("claude-sonnet-4-6"));
        assert_eq!(config.model, "claude-sonnet-4-6");

        std::env::set_var(DIGEST_MODEL_ENV_VAR, "claude-opus-4-8");
        let config = ExtractorConfig::resolve(Some("claude-sonnet-4-6"));
        assert_eq!(config.model, "claude-opus-4-8");
        std::env::remove_var(DIGEST_MODEL_ENV_VAR);

        let config = ExtractorConfig::resolve(None);
        assert_eq!(config.model, DEFAULT_EXTRACTION_MODEL);

        if let Some(value) = saved {
            std::env::set_var(DIGEST_MODEL_ENV_VAR, value);
        }
    }

    #[test]
    fn from_env_without_an_api_key_is_a_provider_config_error() {
        // The test environment must not carry a key for this assertion.
        let _lock = ENV_LOCK.lock().unwrap();
        let saved = std::env::var("ANTHROPIC_API_KEY").ok();
        let saved_family = std::env::var(crate::endpoint::LLM_API_ENV_VAR).ok();
        std::env::remove_var("ANTHROPIC_API_KEY");
        std::env::remove_var(crate::endpoint::LLM_API_ENV_VAR);
        let result = RigExtractor::from_env(ExtractorConfig::default());
        if let Some(key) = saved {
            std::env::set_var("ANTHROPIC_API_KEY", key);
        }
        if let Some(value) = saved_family {
            std::env::set_var(crate::endpoint::LLM_API_ENV_VAR, value);
        }
        match result {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig error, got {other:?}"),
        }
    }

    #[test]
    fn the_default_digest_endpoint_is_anthropic_haiku_canonical_url() {
        let _lock = ENV_LOCK.lock().unwrap();
        let saved: Vec<(&str, Option<String>)> = [
            crate::endpoint::LLM_API_ENV_VAR,
            crate::endpoint::LLM_BASE_URL_ENV_VAR,
            DIGEST_MODEL_ENV_VAR,
        ]
        .iter()
        .map(|name| (*name, std::env::var(name).ok()))
        .collect();
        for (name, _) in &saved {
            std::env::remove_var(name);
        }

        let endpoints = LlmEndpoints::resolve(&LlmConfigValues::default())
            .expect("default resolution succeeds");
        assert_eq!(endpoints.digest.api, LlmApi::AnthropicCompatible);
        assert_eq!(endpoints.digest.model, "claude-haiku-4-5");
        assert_eq!(endpoints.digest.base_url, None);

        for (name, value) in saved {
            if let Some(value) = value {
                std::env::set_var(name, value);
            }
        }
    }
}
