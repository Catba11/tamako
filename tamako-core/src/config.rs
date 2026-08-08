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
