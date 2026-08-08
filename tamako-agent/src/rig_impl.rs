//! The live extractor: a rig completion request with the
//! `KnowledgeGraph` output schema. rig-core 0.41 has no Extractor type;
//! on Anthropic the output schema maps to native structured output
//! (OutputFormat::JsonSchema).

use rig::client::{CompletionClient, ProviderClient};
use rig::completion::{AssistantContent, CompletionModel as _, Message};
use rig::providers::anthropic;

use crate::extract::{AgentError, ExtractionInput, KnowledgeExtractor};
use crate::graph::KnowledgeGraph;
use crate::prompt::{render_extraction_prompt, EXTRACTION_PREAMBLE};

/// The default extraction model: the cheap-tier Anthropic Claude.
pub const DEFAULT_EXTRACTION_MODEL: &str = anthropic::completion::CLAUDE_HAIKU_4_5;

/// The default max tokens of the extraction response.
pub const DEFAULT_MAX_TOKENS: u64 = 8192;

/// The environment variable that overrides the extraction model.
pub const DIGEST_MODEL_ENV_VAR: &str = "TAMAKO_DIGEST_MODEL";

/// Extractor configuration. Provider: Anthropic through rig.
#[derive(Debug, Clone)]
pub struct ExtractorConfig {
    /// The extraction model. Default: claude-haiku-4-5 (cheap tier).
    pub model: String,
    /// Max tokens of the extraction response. Default 8192.
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
    /// default. The API key is NOT here: rig reads `ANTHROPIC_API_KEY`
    /// from the environment in `Client::from_env()`.
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
    model: anthropic::completion::CompletionModel,
    max_tokens: u64,
}

// The rig model handle does not implement Debug. A manual impl keeps
// RigExtractor printable in test failures and logs.
impl std::fmt::Debug for RigExtractor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RigExtractor")
            .field("max_tokens", &self.max_tokens)
            .finish_non_exhaustive()
    }
}

impl RigExtractor {
    /// Builds the extractor from the environment (`ANTHROPIC_API_KEY`;
    /// `ANTHROPIC_BASE_URL` is respected by rig). Returns
    /// `AgentError::ProviderConfig` when the key is missing.
    pub fn from_env(config: ExtractorConfig) -> Result<Self, AgentError> {
        let client = anthropic::Client::from_env()
            .map_err(|error| AgentError::ProviderConfig(error.to_string()))?;
        Ok(RigExtractor {
            model: client.completion_model(config.model),
            max_tokens: config.max_tokens,
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
            let response = self
                .model
                .completion_request(Message::user(render_extraction_prompt(input)))
                // The preamble becomes the system message.
                .preamble(EXTRACTION_PREAMBLE.to_string())
                // Anthropic maps the schema to native structured output.
                .output_schema(schemars::schema_for!(KnowledgeGraph))
                .max_tokens(self.max_tokens)
                .send()
                .await
                .map_err(|error| AgentError::Extraction(error.to_string()))?;
            let text = response
                .choice
                .iter()
                .find_map(|content| match content {
                    AssistantContent::Text(text) => Some(text.text.as_str()),
                    _ => None,
                })
                .ok_or_else(|| {
                    AgentError::Extraction("no text content in the response".to_string())
                })?;
            serde_json::from_str::<KnowledgeGraph>(text)
                .map_err(|error| AgentError::Extraction(format!("invalid graph JSON: {error}")))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_model_is_the_cheap_tier() {
        let config = ExtractorConfig::default();
        assert_eq!(config.model, "claude-haiku-4-5");
        assert_eq!(config.max_tokens, 8192);
    }

    #[test]
    fn resolve_prefers_the_env_var_then_the_config_file() {
        // The tests must not depend on the operator environment.
        std::env::remove_var(DIGEST_MODEL_ENV_VAR);

        let config = ExtractorConfig::resolve(Some("claude-sonnet-4-6"));
        assert_eq!(config.model, "claude-sonnet-4-6");

        std::env::set_var(DIGEST_MODEL_ENV_VAR, "claude-opus-4-8");
        let config = ExtractorConfig::resolve(Some("claude-sonnet-4-6"));
        assert_eq!(config.model, "claude-opus-4-8");
        std::env::remove_var(DIGEST_MODEL_ENV_VAR);

        let config = ExtractorConfig::resolve(None);
        assert_eq!(config.model, DEFAULT_EXTRACTION_MODEL);
    }

    #[test]
    fn from_env_without_an_api_key_is_a_provider_config_error() {
        // The test environment must not carry a key for this assertion.
        let saved = std::env::var("ANTHROPIC_API_KEY").ok();
        std::env::remove_var("ANTHROPIC_API_KEY");
        let result = RigExtractor::from_env(ExtractorConfig::default());
        if let Some(key) = saved {
            std::env::set_var("ANTHROPIC_API_KEY", key);
        }
        match result {
            Err(AgentError::ProviderConfig(_)) => {}
            other => panic!("expected ProviderConfig error, got {other:?}"),
        }
    }
}
