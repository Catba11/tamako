//! Typed configuration. The global defaults come from specs.md Section 13.
//! Rule of AGENT.md Section 6.3: every key is overridable per group.
//!
//! TOML file format: a `[global]` section plus one `[groups.<chat_id>]`
//! table per group override. Durations are expressed in seconds. Example:
//!
//! ```toml
//! [global]
//! wake_msg_count = 5
//! wake_interval_secs = 3600
//!
//! [groups."-1001234567890"]
//! wake_msg_count = 3
//! ```

use std::collections::HashMap;
use std::time::Duration;

/// Trigger and pipeline thresholds. Refer to specs.md Sections 8 and 10.
#[derive(Debug, Clone, PartialEq)]
pub struct TriggerConfig {
    /// specs.md Section 8.3. Default 5.
    pub wake_msg_count: u32,
    /// specs.md Section 8.3. Default 1 h.
    pub wake_interval: Duration,
    /// specs.md Section 8.3. Lower bound of the jitter factor. Default 0.7.
    pub wake_jitter_min: f64,
    /// specs.md Section 8.3. Upper bound of the jitter factor. Default 1.3.
    pub wake_jitter_max: f64,
    /// specs.md Section 8.3. Minimum time between two wakes. Default 5 min.
    pub wake_floor: Duration,
    /// specs.md Section 8.2. Default 5000.
    pub digest_max_chars_cjk: usize,
    /// specs.md Section 8.2. Default 100.
    pub digest_max_messages: u32,
    /// specs.md Section 8.2. Default 2500.
    pub digest_max_words: u32,
    /// specs.md Section 8.2. Default 20 kB.
    pub digest_max_bytes: usize,
    /// specs.md Section 8.2. Timeout fallback. Default 6 h.
    pub digest_timeout: Duration,
    /// specs.md Section 10.3. Default 5.
    pub digest_max_retries: u32,
    /// Extraction model override for the digest pipeline. `None` (the
    /// default) means the agent crate's built-in default model. The env
    /// var TAMAKO_DIGEST_MODEL takes precedence at wiring time.
    /// Deviation: specs.md Section 13 has no LLM keys; this key is
    /// reported as a deviation of Phase 1 M1.
    pub digest_model: Option<String>,
    /// specs.md Section 13: the endpoint family, `anthropic-compatible`
    /// or `openai-compatible`. `None` (the default): the agent layer
    /// resolves the default family.
    pub llm_api: Option<String>,
    /// specs.md Section 13: the base URL of the endpoint.
    pub llm_base_url: Option<String>,
    /// The session id of the LLM endpoint, sent as the
    /// `x-opencode-session` header on every request (Opencode Go
    /// gateway session affinity; provider prompt-cache affinity).
    /// Global-only (one session id per deployment, no per-purpose or
    /// per-group variant). `None` (the default): the agent layer
    /// resolves the default `"tamako"`; the env var
    /// TAMAKO_LLM_SESSION_ID wins. Deviation: specs.md Section 13 has
    /// no such key; reported for spec backfill.
    pub llm_session_id: Option<String>,
    /// specs.md Section 13: a purpose (`digest`, `gate`, `reply`) may
    /// override `llm_api` and `llm_base_url` individually. This permits
    /// mixed deployments. The digest-purpose overrides.
    pub digest_llm_api: Option<String>,
    /// The digest-purpose override of `llm_base_url`. Refer to
    /// `digest_llm_api`.
    pub digest_llm_base_url: Option<String>,
    /// The gate-purpose override of `llm_api`. Refer to `digest_llm_api`.
    pub gate_llm_api: Option<String>,
    /// The gate-purpose override of `llm_base_url`. Refer to
    /// `digest_llm_api`.
    pub gate_llm_base_url: Option<String>,
    /// The reply-purpose override of `llm_api`. Refer to `digest_llm_api`.
    pub reply_llm_api: Option<String>,
    /// The reply-purpose override of `llm_base_url`. Refer to
    /// `digest_llm_api`.
    pub reply_llm_base_url: Option<String>,
    /// The summary-purpose override of `llm_api`. Refer to
    /// `digest_llm_api`. Reported for spec backfill with the
    /// `summary_*` keys.
    pub summary_llm_api: Option<String>,
    /// The summary-purpose override of `llm_base_url`. Refer to
    /// `digest_llm_api`. Reported for spec backfill with the
    /// `summary_*` keys.
    pub summary_llm_base_url: Option<String>,
    /// specs.md Section 13: the model of the participation gate
    /// (Section 9.6). `None` (the default) means the agent crate's
    /// built-in cheap model.
    pub gate_model: Option<String>,
    /// specs.md Section 13: the model of the reply generation (Section 9
    /// step 4). `None` (the default) means the agent crate's built-in
    /// main model.
    pub reply_model: Option<String>,
    /// The model of the Rule C3 segmented summarizer (specs.md
    /// Section 10, keep-two retention). `None` (the default) means the
    /// agent crate's built-in cheap model. Deviation: specs.md
    /// Section 13 has no such key; reported for spec backfill.
    pub summary_model: Option<String>,
    /// How the structured calls enforce their output shape on the wire:
    /// `schema` (the default), `json_object`, or `prompt_only`. `None`
    /// means the agent layer resolves the default. Deviation: specs.md
    /// Section 13 has no such key; reported for spec backfill.
    pub structured_output: Option<String>,
    /// The digest-purpose override of `structured_output`. Refer to
    /// `structured_output` (reported for spec backfill).
    pub digest_structured_output: Option<String>,
    /// The gate-purpose override of `structured_output`. Refer to
    /// `structured_output` (reported for spec backfill).
    pub gate_structured_output: Option<String>,
    /// The reply-purpose override of `structured_output`. Refer to
    /// `structured_output` (reported for spec backfill).
    pub reply_structured_output: Option<String>,
    /// The summary-purpose override of `structured_output`. Refer to
    /// `structured_output` (reported for spec backfill).
    pub summary_structured_output: Option<String>,
    /// specs.md Section 6.2 recency re-check: when more than this many
    /// newer human messages arrived after the target message, the
    /// generated reply is DISCARDED, not regenerated. Default 20.
    /// Deviation of Phase 1 M4: this key is not yet in specs.md
    /// Section 13; it is reported for spec backfill.
    pub reply_staleness_threshold: u32,
    /// specs.md Section 6.2 (decision 70): a NON-forced wake reply
    /// quotes (replies-to) its target message only when MORE than this
    /// many newer human messages arrived after the target; a recent
    /// target gets a plain standalone message (a Telegram reply
    /// notifies the author, and a recent target needs no context
    /// anchor). A forced wake always quotes. Default 10.
    pub reply_quote_threshold: u32,
    /// The hard cap of injected memories per wake (Section 9.2
    /// conservative default). Default 5. Deviation: specs.md Section 13
    /// has no such key; reported for spec backfill (Phase 1 M5).
    pub recall_injection_cap: u32,
    /// specs.md Section 8.4. Lower bound of the daily quota. Default 1.
    pub warmup_quota_min: u32,
    /// specs.md Section 8.4. Upper bound of the daily quota. Default 3.
    pub warmup_quota_max: u32,
    /// specs.md Section 8.4. Default 4 h.
    pub warmup_silence: Duration,
    /// specs.md Section 8.5. Default 2.
    pub monologue_limit: u32,
}

impl Default for TriggerConfig {
    /// The values of specs.md Section 13.
    fn default() -> Self {
        Self {
            wake_msg_count: 5,
            wake_interval: Duration::from_secs(60 * 60),
            wake_jitter_min: 0.7,
            wake_jitter_max: 1.3,
            wake_floor: Duration::from_secs(5 * 60),
            digest_max_chars_cjk: 5000,
            digest_max_messages: 100,
            digest_max_words: 2500,
            digest_max_bytes: 20 * 1024,
            digest_timeout: Duration::from_secs(6 * 60 * 60),
            digest_max_retries: 5,
            digest_model: None,
            llm_api: None,
            llm_base_url: None,
            llm_session_id: None,
            digest_llm_api: None,
            digest_llm_base_url: None,
            gate_llm_api: None,
            gate_llm_base_url: None,
            reply_llm_api: None,
            reply_llm_base_url: None,
            summary_llm_api: None,
            summary_llm_base_url: None,
            gate_model: None,
            reply_model: None,
            summary_model: None,
            structured_output: None,
            digest_structured_output: None,
            gate_structured_output: None,
            reply_structured_output: None,
            summary_structured_output: None,
            reply_staleness_threshold: 20,
            reply_quote_threshold: 10,
            recall_injection_cap: 5,
            warmup_quota_min: 1,
            warmup_quota_max: 3,
            warmup_silence: Duration::from_secs(4 * 60 * 60),
            monologue_limit: 2,
        }
    }
}

/// Serde overlay for one TOML table. Durations are expressed in seconds.
/// A `None` field keeps the value of the base configuration.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TriggerConfigToml {
    pub wake_msg_count: Option<u32>,
    pub wake_interval_secs: Option<u64>,
    pub wake_jitter_min: Option<f64>,
    pub wake_jitter_max: Option<f64>,
    pub wake_floor_secs: Option<u64>,
    pub digest_max_chars_cjk: Option<usize>,
    pub digest_max_messages: Option<u32>,
    pub digest_max_words: Option<u32>,
    pub digest_max_bytes: Option<usize>,
    pub digest_timeout_secs: Option<u64>,
    pub digest_max_retries: Option<u32>,
    /// Extraction model override. Refer to `TriggerConfig::digest_model`.
    pub digest_model: Option<String>,
    /// The endpoint family override. Refer to `TriggerConfig::llm_api`.
    pub llm_api: Option<String>,
    /// The endpoint base URL. Refer to `TriggerConfig::llm_base_url`.
    pub llm_base_url: Option<String>,
    /// The endpoint session id (global-only). Refer to
    /// `TriggerConfig::llm_session_id`.
    pub llm_session_id: Option<String>,
    /// The digest-purpose endpoint family. Refer to
    /// `TriggerConfig::digest_llm_api`.
    pub digest_llm_api: Option<String>,
    /// The digest-purpose base URL. Refer to
    /// `TriggerConfig::digest_llm_base_url`.
    pub digest_llm_base_url: Option<String>,
    /// The gate-purpose endpoint family. Refer to
    /// `TriggerConfig::gate_llm_api`.
    pub gate_llm_api: Option<String>,
    /// The gate-purpose base URL. Refer to
    /// `TriggerConfig::gate_llm_base_url`.
    pub gate_llm_base_url: Option<String>,
    /// The reply-purpose endpoint family. Refer to
    /// `TriggerConfig::reply_llm_api`.
    pub reply_llm_api: Option<String>,
    /// The reply-purpose base URL. Refer to
    /// `TriggerConfig::reply_llm_base_url`.
    pub reply_llm_base_url: Option<String>,
    /// The summary-purpose endpoint family (reported for spec
    /// backfill). Refer to `TriggerConfig::reply_llm_api`.
    pub summary_llm_api: Option<String>,
    /// The summary-purpose base URL (reported for spec backfill).
    /// Refer to `TriggerConfig::reply_llm_base_url`.
    pub summary_llm_base_url: Option<String>,
    /// The gate model override. Refer to `TriggerConfig::gate_model`.
    pub gate_model: Option<String>,
    /// The reply model override. Refer to `TriggerConfig::reply_model`.
    pub reply_model: Option<String>,
    /// The summary model override (reported for spec backfill). Refer
    /// to `TriggerConfig::summary_model`.
    pub summary_model: Option<String>,
    /// The structured-output mode. Refer to
    /// `TriggerConfig::structured_output`.
    pub structured_output: Option<String>,
    /// The digest-purpose structured-output mode. Refer to
    /// `TriggerConfig::digest_structured_output`.
    pub digest_structured_output: Option<String>,
    /// The gate-purpose structured-output mode. Refer to
    /// `TriggerConfig::gate_structured_output`.
    pub gate_structured_output: Option<String>,
    /// The reply-purpose structured-output mode. Refer to
    /// `TriggerConfig::reply_structured_output`.
    pub reply_structured_output: Option<String>,
    /// The summary-purpose structured-output mode (reported for spec
    /// backfill). Refer to `TriggerConfig::summary_structured_output`.
    pub summary_structured_output: Option<String>,
    /// The recency re-check threshold. Refer to
    /// `TriggerConfig::reply_staleness_threshold`.
    pub reply_staleness_threshold: Option<u32>,
    /// The quote threshold. Refer to `TriggerConfig::reply_quote_threshold`.
    pub reply_quote_threshold: Option<u32>,
    /// The injection cap of one wake. Refer to
    /// `TriggerConfig::recall_injection_cap`.
    pub recall_injection_cap: Option<u32>,
    pub warmup_quota_min: Option<u32>,
    pub warmup_quota_max: Option<u32>,
    pub warmup_silence_secs: Option<u64>,
    pub monologue_limit: Option<u32>,
}

impl TriggerConfigToml {
    /// Applies the set fields over `base`. A `None` field changes nothing.
    pub fn apply(&self, base: &mut TriggerConfig) {
        if let Some(value) = self.wake_msg_count {
            base.wake_msg_count = value;
        }
        if let Some(value) = self.wake_interval_secs {
            base.wake_interval = Duration::from_secs(value);
        }
        if let Some(value) = self.wake_jitter_min {
            base.wake_jitter_min = value;
        }
        if let Some(value) = self.wake_jitter_max {
            base.wake_jitter_max = value;
        }
        if let Some(value) = self.wake_floor_secs {
            base.wake_floor = Duration::from_secs(value);
        }
        if let Some(value) = self.digest_max_chars_cjk {
            base.digest_max_chars_cjk = value;
        }
        if let Some(value) = self.digest_max_messages {
            base.digest_max_messages = value;
        }
        if let Some(value) = self.digest_max_words {
            base.digest_max_words = value;
        }
        if let Some(value) = self.digest_max_bytes {
            base.digest_max_bytes = value;
        }
        if let Some(value) = self.digest_timeout_secs {
            base.digest_timeout = Duration::from_secs(value);
        }
        if let Some(value) = self.digest_max_retries {
            base.digest_max_retries = value;
        }
        if let Some(value) = &self.digest_model {
            base.digest_model = Some(value.clone());
        }
        if let Some(value) = &self.llm_api {
            base.llm_api = Some(value.clone());
        }
        if let Some(value) = &self.llm_base_url {
            base.llm_base_url = Some(value.clone());
        }
        if let Some(value) = &self.llm_session_id {
            base.llm_session_id = Some(value.clone());
        }
        if let Some(value) = &self.digest_llm_api {
            base.digest_llm_api = Some(value.clone());
        }
        if let Some(value) = &self.digest_llm_base_url {
            base.digest_llm_base_url = Some(value.clone());
        }
        if let Some(value) = &self.gate_llm_api {
            base.gate_llm_api = Some(value.clone());
        }
        if let Some(value) = &self.gate_llm_base_url {
            base.gate_llm_base_url = Some(value.clone());
        }
        if let Some(value) = &self.reply_llm_api {
            base.reply_llm_api = Some(value.clone());
        }
        if let Some(value) = &self.reply_llm_base_url {
            base.reply_llm_base_url = Some(value.clone());
        }
        if let Some(value) = &self.summary_llm_api {
            base.summary_llm_api = Some(value.clone());
        }
        if let Some(value) = &self.summary_llm_base_url {
            base.summary_llm_base_url = Some(value.clone());
        }
        if let Some(value) = &self.gate_model {
            base.gate_model = Some(value.clone());
        }
        if let Some(value) = &self.reply_model {
            base.reply_model = Some(value.clone());
        }
        if let Some(value) = &self.summary_model {
            base.summary_model = Some(value.clone());
        }
        if let Some(value) = &self.structured_output {
            base.structured_output = Some(value.clone());
        }
        if let Some(value) = &self.digest_structured_output {
            base.digest_structured_output = Some(value.clone());
        }
        if let Some(value) = &self.gate_structured_output {
            base.gate_structured_output = Some(value.clone());
        }
        if let Some(value) = &self.reply_structured_output {
            base.reply_structured_output = Some(value.clone());
        }
        if let Some(value) = &self.summary_structured_output {
            base.summary_structured_output = Some(value.clone());
        }
        if let Some(value) = self.reply_staleness_threshold {
            base.reply_staleness_threshold = value;
        }
        if let Some(value) = self.reply_quote_threshold {
            base.reply_quote_threshold = value;
        }
        if let Some(value) = self.recall_injection_cap {
            base.recall_injection_cap = value;
        }
        if let Some(value) = self.warmup_quota_min {
            base.warmup_quota_min = value;
        }
        if let Some(value) = self.warmup_quota_max {
            base.warmup_quota_max = value;
        }
        if let Some(value) = self.warmup_silence_secs {
            base.warmup_silence = Duration::from_secs(value);
        }
        if let Some(value) = self.monologue_limit {
            base.monologue_limit = value;
        }
    }
}

/// Errors of the configuration loader.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The TOML text is not valid.
    #[error("failed to parse the configuration file: {0}")]
    Parse(#[from] toml::de::Error),
}

/// The root configuration of the bot.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BotConfig {
    /// The global defaults after the `[global]` overlay.
    pub global: TriggerConfig,
    /// One overlay per group, keyed by `chat_id`.
    pub overrides: HashMap<String, TriggerConfigToml>,
}

/// The serde shape of the TOML file.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct BotConfigToml {
    global: Option<TriggerConfigToml>,
    #[serde(default)]
    groups: HashMap<String, TriggerConfigToml>,
}

impl BotConfig {
    /// Returns the effective configuration for one group. It starts from the
    /// global configuration and applies the group override when it exists.
    /// Rule of AGENT.md Section 6.3: every key is overridable per group.
    pub fn for_group(&self, chat_id: &str) -> TriggerConfig {
        let mut config = self.global.clone();
        if let Some(overlay) = self.overrides.get(chat_id) {
            overlay.apply(&mut config);
        }
        config
    }

    /// Parses the TOML configuration file. The file has a `[global]`
    /// section and one `[groups.<chat_id>]` table per group override.
    pub fn from_toml_str(text: &str) -> Result<BotConfig, ConfigError> {
        let parsed: BotConfigToml = toml::from_str(text)?;
        let mut global = TriggerConfig::default();
        if let Some(overlay) = &parsed.global {
            overlay.apply(&mut global);
        }
        Ok(BotConfig {
            global,
            overrides: parsed.groups,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE_TOML: &str = r#"
[global]
wake_msg_count = 10
wake_interval_secs = 7200

[groups."-100123"]
wake_msg_count = 2
wake_floor_secs = 60
"#;

    #[test]
    fn defaults_match_section_13() {
        let config = TriggerConfig::default();
        assert_eq!(config.wake_msg_count, 5);
        assert_eq!(config.wake_interval, Duration::from_secs(60 * 60));
        assert_eq!(config.wake_jitter_min, 0.7);
        assert_eq!(config.wake_jitter_max, 1.3);
        assert_eq!(config.wake_floor, Duration::from_secs(5 * 60));
        assert_eq!(config.digest_max_chars_cjk, 5000);
        assert_eq!(config.digest_max_messages, 100);
        assert_eq!(config.digest_max_words, 2500);
        assert_eq!(config.digest_max_bytes, 20 * 1024);
        assert_eq!(config.digest_timeout, Duration::from_secs(6 * 60 * 60));
        assert_eq!(config.digest_max_retries, 5);
        assert_eq!(config.warmup_quota_min, 1);
        assert_eq!(config.warmup_quota_max, 3);
        assert_eq!(config.warmup_silence, Duration::from_secs(4 * 60 * 60));
        assert_eq!(config.monologue_limit, 2);
        // The M4 keys: every Option is None by default; the staleness
        // threshold is 20 (M4 deviation, reported for spec backfill).
        assert_eq!(config.llm_api, None);
        assert_eq!(config.llm_base_url, None);
        // The session-id key (global-only; reported for spec backfill):
        // None by default; the agent layer resolves "tamako".
        assert_eq!(config.llm_session_id, None);
        assert_eq!(config.digest_llm_api, None);
        assert_eq!(config.digest_llm_base_url, None);
        assert_eq!(config.gate_llm_api, None);
        assert_eq!(config.gate_llm_base_url, None);
        assert_eq!(config.reply_llm_api, None);
        assert_eq!(config.reply_llm_base_url, None);
        assert_eq!(config.gate_model, None);
        assert_eq!(config.reply_model, None);
        assert_eq!(config.reply_staleness_threshold, 20);
        // Decision 70 (specs.md Sections 6.2/13): the quote threshold
        // defaults to 10 newer human messages.
        assert_eq!(config.reply_quote_threshold, 10);
        // The structured-output mode keys (reported for spec backfill):
        // every Option is None by default; the agent layer resolves
        // the default mode.
        assert_eq!(config.structured_output, None);
        assert_eq!(config.digest_structured_output, None);
        assert_eq!(config.gate_structured_output, None);
        assert_eq!(config.reply_structured_output, None);
        // The summary keys (reported for spec backfill): every Option
        // is None by default; the agent layer resolves the cheap model
        // and the default mode.
        assert_eq!(config.summary_model, None);
        assert_eq!(config.summary_llm_api, None);
        assert_eq!(config.summary_llm_base_url, None);
        assert_eq!(config.summary_structured_output, None);
        // The M5 key: the injection cap defaults to 5 (Section 9.2
        // conservative default; deviation reported for spec backfill).
        assert_eq!(config.recall_injection_cap, 5);
    }

    #[test]
    fn toml_override_sets_recall_injection_cap() {
        // The M5 deviation key follows the same per-key overlay pattern
        // as every other key (AGENT.md Section 6.3: overridable per
        // group).
        let text = r#"
[global]
recall_injection_cap = 9

[groups."-100777"]
recall_injection_cap = 2
"#;
        let config = BotConfig::from_toml_str(text).expect("the M5 TOML loads");
        assert_eq!(config.global.recall_injection_cap, 9);
        // A group override applies over the global value.
        assert_eq!(config.for_group("-100777").recall_injection_cap, 2);
        // A group without an override receives the global value.
        assert_eq!(config.for_group("-100999").recall_injection_cap, 9);
        // A key the TOML does not set keeps the default.
        let plain = BotConfig::from_toml_str("[global]\n").expect("an empty overlay loads");
        assert_eq!(plain.global.recall_injection_cap, 5);
    }

    #[test]
    fn toml_override_sets_reply_quote_threshold() {
        // Decision 70: the quote threshold follows the same per-key
        // overlay pattern as every other trigger key (AGENT.md Section
        // 6.3: overridable per group).
        let text = r#"
[global]
reply_quote_threshold = 15

[groups."-100777"]
reply_quote_threshold = 0
"#;
        let config = BotConfig::from_toml_str(text).expect("the TOML loads");
        assert_eq!(config.global.reply_quote_threshold, 15);
        // A group override applies over the global value (0 wins: every
        // non-forced reply quotes, the pre-decision-70 behavior).
        assert_eq!(config.for_group("-100777").reply_quote_threshold, 0);
        // A group without an override receives the global value.
        assert_eq!(config.for_group("-100999").reply_quote_threshold, 15);
        // A key the TOML does not set keeps the default.
        let plain = BotConfig::from_toml_str("[global]\n").expect("an empty overlay loads");
        assert_eq!(plain.global.reply_quote_threshold, 10);
    }

    #[test]
    fn toml_parse_and_apply_covers_the_m4_keys() {
        // specs.md Section 13: llm_api / llm_base_url plus the per-purpose
        // overrides and the gate/reply models. reply_staleness_threshold
        // is the M4 deviation key.
        let text = r#"
[global]
llm_api = "anthropic-compatible"
llm_base_url = "https://llm.example/v1"
llm_session_id = "my-deployment"
digest_llm_api = "openai-compatible"
digest_llm_base_url = "https://digest.example/v1"
gate_llm_api = "openai-compatible"
gate_llm_base_url = "https://gate.example/v1"
reply_llm_api = "anthropic-compatible"
reply_llm_base_url = "https://reply.example/v1"
gate_model = "cheap-model"
reply_model = "main-model"
reply_staleness_threshold = 7

[groups."-100777"]
reply_staleness_threshold = 3
"#;
        let config = BotConfig::from_toml_str(text).expect("the M4 TOML loads");
        let global = &config.global;
        assert_eq!(global.llm_api.as_deref(), Some("anthropic-compatible"));
        assert_eq!(
            global.llm_base_url.as_deref(),
            Some("https://llm.example/v1")
        );
        // The global-only session-id key parses and applies.
        assert_eq!(global.llm_session_id.as_deref(), Some("my-deployment"));
        assert_eq!(global.digest_llm_api.as_deref(), Some("openai-compatible"));
        assert_eq!(
            global.digest_llm_base_url.as_deref(),
            Some("https://digest.example/v1")
        );
        assert_eq!(global.gate_llm_api.as_deref(), Some("openai-compatible"));
        assert_eq!(
            global.gate_llm_base_url.as_deref(),
            Some("https://gate.example/v1")
        );
        assert_eq!(
            global.reply_llm_api.as_deref(),
            Some("anthropic-compatible")
        );
        assert_eq!(
            global.reply_llm_base_url.as_deref(),
            Some("https://reply.example/v1")
        );
        assert_eq!(global.gate_model.as_deref(), Some("cheap-model"));
        assert_eq!(global.reply_model.as_deref(), Some("main-model"));
        assert_eq!(global.reply_staleness_threshold, 7);

        // A group override applies the M4 keys the same way.
        let overridden = config.for_group("-100777");
        assert_eq!(overridden.reply_staleness_threshold, 3);
        // Keys that the override does not set keep the global values.
        assert_eq!(overridden.gate_model.as_deref(), Some("cheap-model"));
    }

    #[test]
    fn toml_override_sets_structured_output_modes() {
        // The structured-output keys follow the same per-key overlay
        // pattern as every other key (AGENT.md Section 6.3: overridable
        // per group).
        let text = r#"
[global]
structured_output = "json_object"
digest_structured_output = "prompt_only"

[groups."-100777"]
gate_structured_output = "prompt_only"
reply_structured_output = "json_object"
"#;
        let config = BotConfig::from_toml_str(text).expect("the TOML loads");
        let global = &config.global;
        assert_eq!(global.structured_output.as_deref(), Some("json_object"));
        assert_eq!(
            global.digest_structured_output.as_deref(),
            Some("prompt_only")
        );
        assert_eq!(global.gate_structured_output, None);
        assert_eq!(global.reply_structured_output, None);
        // A group override applies the keys the same way; unset keys
        // keep the global values.
        let overridden = config.for_group("-100777");
        assert_eq!(
            overridden.gate_structured_output.as_deref(),
            Some("prompt_only")
        );
        assert_eq!(
            overridden.reply_structured_output.as_deref(),
            Some("json_object")
        );
        assert_eq!(overridden.structured_output.as_deref(), Some("json_object"));
        assert_eq!(
            overridden.digest_structured_output.as_deref(),
            Some("prompt_only")
        );
        // A group without an override receives the global values.
        let plain = config.for_group("-100999");
        assert_eq!(plain.gate_structured_output, None);
        // Keys that the TOML does not set keep the default (None).
        let empty = BotConfig::from_toml_str("[global]\n").expect("an empty overlay loads");
        assert_eq!(empty.global.structured_output, None);
        assert_eq!(empty.global.digest_structured_output, None);
    }

    #[test]
    fn toml_override_sets_the_summary_keys() {
        // The summary keys (reported for spec backfill) follow the
        // same per-key overlay pattern as every other key (AGENT.md
        // Section 6.3: overridable per group).
        let text = r#"
[global]
summary_model = "global-summary-model"
summary_llm_api = "openai-compatible"
summary_llm_base_url = "https://summary.example/v1"
summary_structured_output = "prompt_only"

[groups."-100777"]
summary_model = "group-summary-model"
"#;
        let config = BotConfig::from_toml_str(text).expect("the summary TOML loads");
        let global = &config.global;
        assert_eq!(
            global.summary_model.as_deref(),
            Some("global-summary-model")
        );
        assert_eq!(global.summary_llm_api.as_deref(), Some("openai-compatible"));
        assert_eq!(
            global.summary_llm_base_url.as_deref(),
            Some("https://summary.example/v1")
        );
        assert_eq!(
            global.summary_structured_output.as_deref(),
            Some("prompt_only")
        );
        // A group override applies the keys the same way; unset keys
        // keep the global values.
        let overridden = config.for_group("-100777");
        assert_eq!(
            overridden.summary_model.as_deref(),
            Some("group-summary-model")
        );
        assert_eq!(
            overridden.summary_llm_api.as_deref(),
            Some("openai-compatible")
        );
        assert_eq!(
            overridden.summary_structured_output.as_deref(),
            Some("prompt_only")
        );
        // A group without an override receives the global values.
        let plain = config.for_group("-100999");
        assert_eq!(plain.summary_model.as_deref(), Some("global-summary-model"));
        // Keys that the TOML does not set keep the default (None).
        let empty = BotConfig::from_toml_str("[global]\n").expect("an empty overlay loads");
        assert_eq!(empty.global.summary_model, None);
        assert_eq!(empty.global.summary_structured_output, None);
    }

    #[test]
    fn toml_round_trip_with_global_and_group_override() {
        let parsed: BotConfigToml = toml::from_str(EXAMPLE_TOML).expect("the example TOML parses");
        let serialized = toml::to_string(&parsed).expect("the config serializes");
        let reparsed: BotConfigToml =
            toml::from_str(&serialized).expect("the serialized config parses again");
        assert_eq!(parsed, reparsed);

        let config = BotConfig::from_toml_str(EXAMPLE_TOML).expect("the example TOML loads");
        assert_eq!(config.global.wake_msg_count, 10);
        assert_eq!(config.global.wake_interval, Duration::from_secs(7200));
        // Fields that the TOML does not set keep the Section 13 defaults.
        assert_eq!(config.global.wake_floor, Duration::from_secs(5 * 60));
        let override_ = config
            .overrides
            .get("-100123")
            .expect("the override exists");
        assert_eq!(override_.wake_msg_count, Some(2));
        assert_eq!(override_.wake_floor_secs, Some(60));
    }

    #[test]
    fn for_group_applies_the_override_only_for_that_group() {
        let config = BotConfig::from_toml_str(EXAMPLE_TOML).expect("the example TOML loads");

        let overridden = config.for_group("-100123");
        assert_eq!(overridden.wake_msg_count, 2);
        assert_eq!(overridden.wake_floor, Duration::from_secs(60));
        // Keys that the override does not set keep the global values.
        assert_eq!(overridden.wake_interval, Duration::from_secs(7200));

        // A group without an override receives the global configuration.
        let plain = config.for_group("-100999");
        assert_eq!(plain, config.global);
    }

    #[test]
    fn from_toml_str_rejects_invalid_toml() {
        assert!(BotConfig::from_toml_str("[global\n").is_err());
    }
}
