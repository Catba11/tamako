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

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

pub use tamako_persona::SuffixMode;

/// The shared default base URL of the openai-compatible OpenRouter
/// endpoints (embedding, decision 66; captioning, decision 82 (c)).
const DEFAULT_OPENROUTER_BASE_URL: &str = "https://openrouter.ai/api/v1";

/// The default media captioning model of decision 82 (c).
const DEFAULT_CAPTION_MODEL: &str = "minimax/minimax-m3";

/// The `warmup_active_hours` window of specs.md Section 8.4, parsed
/// ("HH:MM-HH:MM", host-local; overnight ranges unsupported).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveHours {
    /// Minutes after host-local midnight, window start (inclusive).
    pub start_minutes: u16,
    /// Minutes after host-local midnight, window end (exclusive).
    pub end_minutes: u16,
}

impl ActiveHours {
    /// The default window of specs.md Section 13: "08:00-23:00" host-local.
    pub const DEFAULT: ActiveHours = ActiveHours {
        start_minutes: 8 * 60,
        end_minutes: 23 * 60,
    };

    /// Parses "HH:MM-HH:MM". Err(reason) on: bad shape, HH > 23,
    /// MM > 59, or `end <= start` (overnight and empty windows are
    /// unsupported, specs.md Section 8.4 — a LOUD config error).
    pub fn parse(text: &str) -> Result<ActiveHours, String> {
        fn hm(part: &str) -> Result<u16, String> {
            let (h, m) = part
                .split_once(':')
                .ok_or_else(|| format!("expected the shape HH:MM, got {part:?}"))?;
            let hours: u16 = h
                .parse()
                .map_err(|_| format!("expected the shape HH:MM, got {part:?}"))?;
            let minutes: u16 = m
                .parse()
                .map_err(|_| format!("expected the shape HH:MM, got {part:?}"))?;
            if hours > 23 || minutes > 59 {
                return Err(format!("time out of range in {part:?} (HH 0-23, MM 0-59)"));
            }
            Ok(hours * 60 + minutes)
        }
        let (start, end) = text
            .split_once('-')
            .ok_or_else(|| "expected the shape HH:MM-HH:MM".to_string())?;
        let start_minutes = hm(start)?;
        let end_minutes = hm(end)?;
        if end_minutes <= start_minutes {
            return Err(format!(
                "end {end:?} is not after start {start:?} — same-day windows only, overnight and empty ranges are unsupported (specs.md Section 8.4)"
            ));
        }
        Ok(ActiveHours {
            start_minutes,
            end_minutes,
        })
    }

    /// The window length in seconds.
    pub fn span_seconds(&self) -> u64 {
        u64::from(self.end_minutes - self.start_minutes) * 60
    }
}

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
    /// The embedding model of the Phase 2 sidecar (current-state.md
    /// decision 66). GLOBAL-ONLY and flat: no per-purpose and no
    /// per-group machinery (the same standing as `llm_session_id`).
    /// Unlike the purpose model keys this is a concrete value, not an
    /// Option: decision 66 introduced the key (pinning the
    /// `qwen/qwen3-embedding-8b` default); decision 81 re-pins the
    /// default to `google/gemini-embedding-2` (native 3072 dims).
    /// The env var TAMAKO_EMBEDDING_MODEL wins at wiring time.
    /// Deviation: specs.md Section 13 has no
    /// embedding keys; reported for spec backfill (Phase 2).
    pub embedding_model: String,
    /// The base URL of the openai-compatible embedding endpoint
    /// (decision 66). Global-only and flat; refer to
    /// `embedding_model`. Default `https://openrouter.ai/api/v1`.
    /// The env var TAMAKO_EMBEDDING_BASE_URL wins at wiring time.
    pub embedding_llm_base_url: String,
    /// The embedding kill switch (decision 77, M6a). GLOBAL-ONLY and
    /// flat, the same standing as `embedding_model`: `false` disables
    /// the embedding provider build at wiring time (the binary reads
    /// this key; the provider-less worker then runs reconciliation
    /// only, no drains). Default true. Deviation: specs.md Section 13
    /// has no such key; reported for spec backfill.
    pub embedding_enabled: bool,
    /// The media captioning model of decision 82 (c): photos/stickers
    /// are captioned at intake by this vision model. Flat and concrete
    /// like `embedding_model` (the default is materialized here, not
    /// resolved by the agent layer), but UNLIKE the embedding keys
    /// this key follows the ordinary per-group-override pattern of the
    /// other model keys (`digest_model`, `gate_model`, ...) — a group
    /// may pin a different caption model. Deviation: specs.md
    /// Section 13 has no caption keys; reported for spec backfill.
    pub caption_model: String,
    /// The base URL of the openai-compatible caption endpoint
    /// (decision 82 (c)). Default `https://openrouter.ai/api/v1` (the
    /// same OpenRouter default as the embedding endpoint).
    /// Per-group-overridable like `caption_model`; refer to it.
    pub caption_llm_base_url: String,
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
    /// specs.md Section 9.6 (decision 72): the participation gate and
    /// the recall relevance gate receive the SHARED CONTEXT VIEW (the
    /// same rendered bytes the reply model sees, up to the wake's
    /// marker) ahead of their per-call sections, so consecutive gate
    /// calls share a growing byte prefix (provider prompt cache).
    /// `false` restores the delta-only input. Default true.
    pub gate_context: bool,
    /// proposed-graph-database-specs.md Section 7.4 step 3 (decision
    /// 73): the vector pre-screen in entity resolution. `false` skips
    /// step 3 entirely (byte-identical Phase 1 behavior path, not even
    /// the embeddings call). Default true.
    pub vector_resolution: bool,
    /// Decision 73: the cosine SIMILARITY at or above which the best
    /// compatible sidecar hit binds without a confirmation call.
    /// Default 0.92.
    pub vector_match_threshold: f64,
    /// Decision 73: the lower bound of the LLM confirmation band;
    /// below it the entity creates a new node. Default 0.80.
    pub vector_candidate_threshold: f64,
    /// Decision 73: the cap of LLM confirmation calls per digest
    /// batch; an exhausted budget treats middle-band entities as
    /// below-threshold. Default 5.
    pub resolution_confirm_budget: u32,
    /// Decision 74 (proposed-graph-database-specs.md Section 7.7 step
    /// 1): the cosine SIMILARITY at or above which an embedded
    /// Person/Concept pair becomes a merge candidate of the offline
    /// merge tool. Deliberately narrower than the write-path band
    /// (decision 74 point 5). Default 0.85 (provisional).
    pub merge_candidate_threshold: f64,
    /// Decision 75 (proposed-graph-database-specs.md Section 7.5,
    /// specs.md Section 13): the single-value fact predicates. When a
    /// NEW edge with a registered predicate is written, the older
    /// valid edges with the same (subject, predicate) are invalidated
    /// — one valid fact per subject per predicate. A predicate absent
    /// from the list is multi-value. Default the four of Section 13.
    pub single_value_predicates: Vec<String>,
    /// The hard cap of injected memories per wake (Section 9.2
    /// conservative default). Default 5. Deviation: specs.md Section 13
    /// has no such key; reported for spec backfill (Phase 1 M5).
    pub recall_injection_cap: u32,
    /// specs.md Sections 9.1/13 (decision 76): the deep-recall switch.
    /// `false` restores the Phase 1 shallow candidate form (direct
    /// neighbors only, one hop, entry by mentions/replies and exact
    /// alias match) byte-identically — no embeddings call, no two-hop
    /// expansion, no edge_texts full-text source. Default true.
    pub deep_recall: bool,
    /// specs.md Sections 9.1/13 (decision 76 (d)): the TOTAL candidate
    /// cap before the relevance gate — every candidate source (shallow,
    /// vector entry, two-hop expansion, edge_texts full-text) merged
    /// and deduped by edge id, then truncated. Default 40.
    pub recall_candidate_cap: u32,
    /// specs.md Sections 8.4/13 (decision 78 (e)): the warmup master
    /// switch. Default TRUE — the Phase 2 exit criterion needs live
    /// measurement.
    pub warmup: bool,
    /// specs.md Sections 8.4/13 (decision 78 (e)): the daily warmup
    /// quota. Default 1, valid range 1..=3 — a value outside is a LOUD
    /// startup error, never a silent clamp.
    pub warmup_quota: u32,
    /// specs.md Sections 8.4/13 (decision 78 (e)): the host-local
    /// window the quota spreads over. Default "08:00-23:00".
    pub warmup_active_hours: ActiveHours,
    /// specs.md Section 8.4 (decision 78 (e)). Default 4 h.
    pub warmup_silence: Duration,
    /// specs.md Section 8.5 (decision 78 (e)): the engagement watch
    /// window of a sent warmup. Default 30 min.
    pub warmup_reaction_window: Duration,
    /// specs.md Section 9.7 step 2 (decision 78 (e)): the per-topic
    /// cooldown. Default 3 days.
    pub warmup_topic_cooldown_days: u32,
    /// specs.md Section 8.5. Default 2.
    pub monologue_limit: u32,
    /// specs.md Sections 8.1/13 (decision 79 (c)): the forced-wake
    /// cooldown. A forced wake that produced a reply suppresses new
    /// forcings for this long (the mention/reply is still logged and
    /// lands in the next wake's presented set; no wake fires). Default
    /// 10 s; 0 disables. The TOML key is `u64` seconds, which cannot be
    /// negative — the type IS the validation.
    pub forced_wake_cooldown: Duration,
    /// The placement mode of the decision-86 suffix in reply requests:
    /// `system` (the Decision 86 default: separate system message strictly last)
    /// or `append` (appended into the final user instruction with an authoritative
    /// preamble contract).
    pub suffix_mode: SuffixMode,
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
            embedding_model: "google/gemini-embedding-2".to_string(),
            embedding_llm_base_url: DEFAULT_OPENROUTER_BASE_URL.to_string(),
            embedding_enabled: true,
            caption_model: DEFAULT_CAPTION_MODEL.to_string(),
            caption_llm_base_url: DEFAULT_OPENROUTER_BASE_URL.to_string(),
            structured_output: None,
            digest_structured_output: None,
            gate_structured_output: None,
            reply_structured_output: None,
            summary_structured_output: None,
            reply_staleness_threshold: 20,
            reply_quote_threshold: 10,
            gate_context: true,
            vector_resolution: true,
            vector_match_threshold: 0.92,
            vector_candidate_threshold: 0.80,
            resolution_confirm_budget: 5,
            merge_candidate_threshold: 0.85,
            single_value_predicates: vec![
                "currently_playing".to_string(),
                "works_at".to_string(),
                "lives_in".to_string(),
                "dating".to_string(),
            ],
            recall_injection_cap: 5,
            deep_recall: true,
            recall_candidate_cap: 40,
            warmup: true,
            warmup_quota: 1,
            warmup_active_hours: ActiveHours::DEFAULT,
            warmup_silence: Duration::from_secs(4 * 60 * 60),
            warmup_reaction_window: Duration::from_secs(30 * 60),
            warmup_topic_cooldown_days: 3,
            monologue_limit: 2,
            forced_wake_cooldown: Duration::from_secs(10),
            suffix_mode: SuffixMode::System,
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
    /// The embedding model (decision 66; global-only, no per-group
    /// machinery). Refer to `TriggerConfig::embedding_model`.
    pub embedding_model: Option<String>,
    /// The embedding base URL (decision 66; global-only). Refer to
    /// `TriggerConfig::embedding_llm_base_url`.
    pub embedding_llm_base_url: Option<String>,
    /// The embedding kill switch (decision 77, M6a; global-only).
    /// Refer to `TriggerConfig::embedding_enabled`.
    pub embedding_enabled: Option<bool>,
    /// The media captioning model (decision 82 (c); per-group
    /// overridable). Refer to `TriggerConfig::caption_model`.
    pub caption_model: Option<String>,
    /// The caption endpoint base URL (decision 82 (c); per-group
    /// overridable). Refer to `TriggerConfig::caption_llm_base_url`.
    pub caption_llm_base_url: Option<String>,
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
    /// The gate context-view switch (decision 72). Refer to
    /// `TriggerConfig::gate_context`.
    pub gate_context: Option<bool>,
    /// The vector pre-screen switch (decision 73). Refer to
    /// `TriggerConfig::vector_resolution`.
    pub vector_resolution: Option<bool>,
    /// The auto-match similarity. Refer to
    /// `TriggerConfig::vector_match_threshold`.
    pub vector_match_threshold: Option<f64>,
    /// The confirmation-band lower bound. Refer to
    /// `TriggerConfig::vector_candidate_threshold`.
    pub vector_candidate_threshold: Option<f64>,
    /// The per-batch confirmation budget. Refer to
    /// `TriggerConfig::resolution_confirm_budget`.
    pub resolution_confirm_budget: Option<u32>,
    /// The merge-candidate similarity (decision 74). Refer to
    /// `TriggerConfig::merge_candidate_threshold`.
    pub merge_candidate_threshold: Option<f64>,
    /// The single-value fact predicates (decision 75). Refer to
    /// `TriggerConfig::single_value_predicates`. A SET overlay
    /// REPLACES the whole list — a per-group override replaces
    /// wholesale, no merging with the base list.
    pub single_value_predicates: Option<Vec<String>>,
    /// The injection cap of one wake. Refer to
    /// `TriggerConfig::recall_injection_cap`.
    pub recall_injection_cap: Option<u32>,
    /// The deep-recall switch (decision 76). Refer to
    /// `TriggerConfig::deep_recall`.
    pub deep_recall: Option<bool>,
    /// The total candidate cap before the relevance gate (decision
    /// 76). Refer to `TriggerConfig::recall_candidate_cap`.
    pub recall_candidate_cap: Option<u32>,
    /// The warmup master switch (decision 78 (e)). Refer to
    /// `TriggerConfig::warmup`.
    pub warmup: Option<bool>,
    /// The daily warmup quota (decision 78 (e)). Refer to
    /// `TriggerConfig::warmup_quota`.
    pub warmup_quota: Option<u32>,
    /// The active-hours window, "HH:MM-HH:MM" host-local (decision 78
    /// (e)). Refer to `TriggerConfig::warmup_active_hours`.
    pub warmup_active_hours: Option<String>,
    pub warmup_silence_secs: Option<u64>,
    /// The engagement watch window (decision 78 (e)). Refer to
    /// `TriggerConfig::warmup_reaction_window`.
    pub warmup_reaction_window_secs: Option<u64>,
    /// The per-topic cooldown in days (decision 78 (e)). Refer to
    /// `TriggerConfig::warmup_topic_cooldown_days`.
    pub warmup_topic_cooldown_days: Option<u32>,
    pub monologue_limit: Option<u32>,
    /// The forced-wake cooldown in seconds (decision 79 (c)); 0
    /// disables. Refer to `TriggerConfig::forced_wake_cooldown`.
    pub forced_wake_cooldown_secs: Option<u64>,
    /// The placement mode of the decision-86 suffix. Refer to
    /// `TriggerConfig::suffix_mode`.
    #[serde(alias = "suffix-mode")]
    pub suffix_mode: Option<SuffixMode>,
    /// Unknown keys land here (decision 84 (e)) and are WARNed about at
    /// load, never applied. `flatten` keeps forward compatibility: a
    /// newer config's keys don't hard-fail an older binary (no
    /// `deny_unknown_fields`). Private: construction inside the crate
    /// uses `..Default::default()`, and no external crate builds this
    /// struct literally. `BTreeMap` keeps the WARN order deterministic.
    #[serde(flatten)]
    unknown: BTreeMap<String, toml::Value>,
}

impl TriggerConfigToml {
    /// The name of the first GLOBAL-ONLY key this table sets, if any
    /// (decision 77, S6-F7). The global-only set, verified against
    /// specs.md Section 13 and the `TriggerConfig` key docs:
    /// `llm_session_id` ("one session id per deployment... do not"
    /// override per group), `embedding_model`, `embedding_llm_base_url`
    /// (both "global only"), and `embedding_enabled` (decision 77,
    /// M6a). `llm_api`/`llm_base_url` are NOT global-only — Section 13
    /// resolves LLM access from the per-group effective configuration —
    /// and neither are the per-purpose `*_llm_*` / model /
    /// structured-output keys. The decision-82 (c) caption keys are
    /// NOT global-only either: unlike the embedding keys (one vector
    /// space deployment-wide), a group may legitimately pin a
    /// different caption model or endpoint.
    fn global_only_key(&self) -> Option<&'static str> {
        if self.llm_session_id.is_some() {
            return Some("llm_session_id");
        }
        if self.embedding_model.is_some() {
            return Some("embedding_model");
        }
        if self.embedding_llm_base_url.is_some() {
            return Some("embedding_llm_base_url");
        }
        if self.embedding_enabled.is_some() {
            return Some("embedding_enabled");
        }
        None
    }

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
        if let Some(value) = &self.embedding_model {
            base.embedding_model = value.clone();
        }
        if let Some(value) = &self.embedding_llm_base_url {
            base.embedding_llm_base_url = value.clone();
        }
        if let Some(value) = self.embedding_enabled {
            base.embedding_enabled = value;
        }
        if let Some(value) = &self.caption_model {
            base.caption_model = value.clone();
        }
        if let Some(value) = &self.caption_llm_base_url {
            base.caption_llm_base_url = value.clone();
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
        if let Some(value) = self.gate_context {
            base.gate_context = value;
        }
        if let Some(value) = self.vector_resolution {
            base.vector_resolution = value;
        }
        if let Some(value) = self.vector_match_threshold {
            base.vector_match_threshold = value;
        }
        if let Some(value) = self.vector_candidate_threshold {
            base.vector_candidate_threshold = value;
        }
        if let Some(value) = self.resolution_confirm_budget {
            base.resolution_confirm_budget = value;
        }
        if let Some(value) = self.merge_candidate_threshold {
            base.merge_candidate_threshold = value;
        }
        if let Some(value) = &self.single_value_predicates {
            // A set overlay REPLACES the whole list (no merging), so a
            // group can also declare zero single-value predicates.
            base.single_value_predicates = value.clone();
        }
        if let Some(value) = self.recall_injection_cap {
            base.recall_injection_cap = value;
        }
        if let Some(value) = self.deep_recall {
            base.deep_recall = value;
        }
        if let Some(value) = self.recall_candidate_cap {
            base.recall_candidate_cap = value;
        }
        if let Some(value) = self.warmup {
            base.warmup = value;
        }
        if let Some(value) = self.warmup_quota {
            base.warmup_quota = value;
        }
        if let Some(value) = &self.warmup_active_hours {
            // `BotConfig::from_toml_str` validates the raw overlay
            // string BEFORE apply runs (decision 78 (e): a loud error,
            // never a silent skip), so a malformed value never reaches
            // this branch.
            if let Ok(hours) = ActiveHours::parse(value) {
                base.warmup_active_hours = hours;
            }
        }
        if let Some(value) = self.warmup_silence_secs {
            base.warmup_silence = Duration::from_secs(value);
        }
        if let Some(value) = self.warmup_reaction_window_secs {
            base.warmup_reaction_window = Duration::from_secs(value);
        }
        if let Some(value) = self.warmup_topic_cooldown_days {
            base.warmup_topic_cooldown_days = value;
        }
        if let Some(value) = self.monologue_limit {
            base.monologue_limit = value;
        }
        if let Some(value) = self.forced_wake_cooldown_secs {
            base.forced_wake_cooldown = Duration::from_secs(value);
        }
        if let Some(value) = self.suffix_mode {
            base.suffix_mode = value;
        }
    }
}

/// Errors of the configuration loader.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The TOML text is not valid.
    #[error("failed to parse the configuration file: {0}")]
    Parse(#[from] toml::de::Error),
    /// A global-only key appeared under a `[groups.<chat_id>]` table
    /// (decision 77, S6-F7). Global-only keys live under `[global]`
    /// only; a group-table occurrence would otherwise apply silently.
    #[error(
        "the key '{key}' is global-only and cannot appear under [groups.{group}]; move it to [global]"
    )]
    GlobalOnlyKey {
        /// The offending key name.
        key: &'static str,
        /// The group table the key appeared under.
        group: String,
    },
    /// A warmup key carries an out-of-range or malformed value
    /// (decision 78 (e)): loud at startup, never a silent clamp.
    #[error("invalid value for '{key}'{group_context}: {value:?} — {reason}")]
    InvalidValue {
        /// The offending key name.
        key: &'static str,
        /// The group context: " under [groups.-1001]", or "" under
        /// [global].
        group_context: String,
        /// The offending value.
        value: String,
        /// Why the value is invalid.
        reason: String,
    },
}

/// The decision-78 (e) warmup validation of one raw TOML overlay:
/// `warmup_quota` in 1..=3 and `warmup_active_hours` well-formed and
/// same-day. Runs on the raw overlay values BEFORE apply, so a bad
/// value never applies silently. `group_context` is "" under [global]
/// or " under [groups.<chat_id>]".
fn validate_warmup_overlay(
    overlay: &TriggerConfigToml,
    group_context: &str,
) -> Result<(), ConfigError> {
    if let Some(value) = overlay.warmup_quota {
        if !(1..=3).contains(&value) {
            return Err(ConfigError::InvalidValue {
                key: "warmup_quota",
                group_context: group_context.to_string(),
                value: value.to_string(),
                reason: "the valid range is 1..=3 (specs.md Section 8.4)".to_string(),
            });
        }
    }
    if let Some(value) = &overlay.warmup_active_hours {
        if let Err(reason) = ActiveHours::parse(value) {
            return Err(ConfigError::InvalidValue {
                key: "warmup_active_hours",
                group_context: group_context.to_string(),
                value: value.clone(),
                reason,
            });
        }
    }
    Ok(())
}

/// The decision-84 (e) collection of the unknown keys of a parsed
/// configuration as `(table, key)` pairs — `"global"` for the global
/// table, `"groups.<chat_id>"` for a group table. This is the pure,
/// directly assertable seam under the `warn!` emission of
/// `BotConfig::from_toml_str`: serde's `flatten` catch-all computes
/// the unknown set (no hand-maintained key list to drift), the
/// `BTreeMap` catch-all plus the sorted group ids keep the output
/// order deterministic.
fn unknown_keys(parsed: &BotConfigToml) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if let Some(overlay) = &parsed.global {
        out.extend(
            overlay
                .unknown
                .keys()
                .map(|key| ("global".to_string(), key.clone())),
        );
    }
    // Sorted group ids keep the reported order deterministic.
    let mut groups: Vec<&String> = parsed.groups.keys().collect();
    groups.sort();
    for group in groups {
        out.extend(
            parsed.groups[group]
                .unknown
                .keys()
                .map(|key| (format!("groups.{group}"), key.clone())),
        );
    }
    out
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
    /// A global-only key under a group table is a LOUD error
    /// (decision 77, S6-F7) naming the key and the group. An unknown
    /// key is a curated WARN, never an error (decision 84 (e)).
    pub fn from_toml_str(text: &str) -> Result<BotConfig, ConfigError> {
        let parsed: BotConfigToml = toml::from_str(text)?;
        // Decision 84 (e): one curated WARN per unknown key, global
        // table and per-group tables alike — the key applies NOTHING
        // (the motivating typo keys `digest_max_chars_words` /
        // `digest_max_chars_bytes` were silently ignored). Warn-only,
        // never deny_unknown_fields: forward compatibility means a
        // newer config's keys must not hard-fail an older binary.
        for (table, key) in unknown_keys(&parsed) {
            tracing::warn!(key = %key, table = %table, "unknown config key, ignored");
        }
        // Sorted group ids keep the reported error deterministic.
        let mut groups: Vec<&String> = parsed.groups.keys().collect();
        groups.sort();
        for &group in &groups {
            if let Some(key) = parsed.groups[group].global_only_key() {
                return Err(ConfigError::GlobalOnlyKey {
                    key,
                    group: group.clone(),
                });
            }
        }
        // Decision 78 (e): the warmup keys validate LOUDLY on the raw
        // overlay values, BEFORE apply runs — a bad value never applies
        // silently and never clamps. The effective values are covered
        // by the same checks: the defaults are in range, and every
        // overlay value that could replace them is checked here.
        if let Some(overlay) = &parsed.global {
            validate_warmup_overlay(overlay, "")?;
        }
        for &group in &groups {
            let group_context = format!(" under [groups.{group}]");
            validate_warmup_overlay(&parsed.groups[group], &group_context)?;
        }
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
        // Decision 78 (e) (specs.md Sections 8.4/8.5/9.7/13): the six
        // warmup keys.
        assert!(config.warmup);
        assert_eq!(config.warmup_quota, 1);
        assert_eq!(config.warmup_active_hours, ActiveHours::DEFAULT);
        assert_eq!(config.warmup_active_hours.span_seconds(), 15 * 60 * 60);
        assert_eq!(config.warmup_silence, Duration::from_secs(4 * 60 * 60));
        assert_eq!(config.warmup_reaction_window, Duration::from_secs(30 * 60));
        assert_eq!(config.warmup_topic_cooldown_days, 3);
        assert_eq!(config.monologue_limit, 2);
        // Decision 79 (c) (specs.md Sections 8.1/13): the forced-wake
        // cooldown defaults to 10 s.
        assert_eq!(config.forced_wake_cooldown, Duration::from_secs(10));
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
        // Decision 72 (specs.md Sections 9.2/9.6/13): the gates receive
        // the shared context view by default.
        assert!(config.gate_context);
        // Decision 73 (proposed-graph-database-specs.md Section 7.4
        // step 3, specs.md Section 13): the vector pre-screen defaults.
        assert!(config.vector_resolution);
        assert_eq!(config.vector_match_threshold, 0.92);
        assert_eq!(config.vector_candidate_threshold, 0.80);
        assert_eq!(config.resolution_confirm_budget, 5);
        // Decision 74 (proposed-graph-database-specs.md Section 7.7
        // step 1): the merge-candidate threshold defaults to the
        // provisional 0.85, narrower than the write-path band.
        assert_eq!(config.merge_candidate_threshold, 0.85);
        // Decision 75 (proposed-graph-database-specs.md Section 7.5,
        // specs.md Section 13): the single-value predicate registry
        // defaults to the four predicates of Section 13.
        assert_eq!(
            config.single_value_predicates,
            vec![
                "currently_playing".to_string(),
                "works_at".to_string(),
                "lives_in".to_string(),
                "dating".to_string(),
            ]
        );
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
        // Decision 76 (specs.md Sections 9.1/13): deep recall defaults
        // ON; the total candidate cap defaults to 40.
        assert!(config.deep_recall);
        assert_eq!(config.recall_candidate_cap, 40);
    }

    #[test]
    fn toml_override_sets_decision_76_keys() {
        // Decision 76: the deep-recall keys follow the same per-key
        // overlay pattern as every other trigger key (AGENT.md Section
        // 6.3: overridable per group).
        let text = r#"
[global]
deep_recall = false
recall_candidate_cap = 60

[groups."-100777"]
deep_recall = true
recall_candidate_cap = 10
"#;
        let config = BotConfig::from_toml_str(text).expect("the TOML loads");
        assert!(!config.global.deep_recall);
        assert_eq!(config.global.recall_candidate_cap, 60);
        // A group override applies over the global value (true wins:
        // deep recall returns for this group only).
        assert!(config.for_group("-100777").deep_recall);
        assert_eq!(config.for_group("-100777").recall_candidate_cap, 10);
        // A group without an override receives the global value.
        assert!(!config.for_group("-100999").deep_recall);
        assert_eq!(config.for_group("-100999").recall_candidate_cap, 60);
        // Keys the TOML does not set keep the decision-76 defaults.
        let plain = BotConfig::from_toml_str("[global]\n").expect("an empty overlay loads");
        assert!(plain.global.deep_recall);
        assert_eq!(plain.global.recall_candidate_cap, 40);
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
    fn toml_override_sets_gate_context() {
        // Decision 72: the context-view kill switch follows the same
        // per-key overlay pattern as every other trigger key (AGENT.md
        // Section 6.3: overridable per group).
        let text = r#"
[global]
gate_context = false

[groups."-100777"]
gate_context = true
"#;
        let config = BotConfig::from_toml_str(text).expect("the TOML loads");
        assert!(!config.global.gate_context);
        // A group override applies over the global value (true wins:
        // the view returns for this group only).
        assert!(config.for_group("-100777").gate_context);
        // A group without an override receives the global value.
        assert!(!config.for_group("-100999").gate_context);
        // A key the TOML does not set keeps the default (true).
        let plain = BotConfig::from_toml_str("[global]\n").expect("an empty overlay loads");
        assert!(plain.global.gate_context);
    }

    #[test]
    fn toml_override_sets_decision_73_keys() {
        // Decision 73: the vector pre-screen keys follow the same
        // per-key overlay pattern as every other trigger key (AGENT.md
        // Section 6.3: overridable per group).
        let text = r#"
[global]
vector_resolution = false
vector_match_threshold = 0.95
vector_candidate_threshold = 0.75
resolution_confirm_budget = 2

[groups."-100777"]
vector_resolution = true
vector_match_threshold = 0.90
"#;
        let config = BotConfig::from_toml_str(text).expect("the TOML loads");
        assert!(!config.global.vector_resolution);
        assert_eq!(config.global.vector_match_threshold, 0.95);
        assert_eq!(config.global.vector_candidate_threshold, 0.75);
        assert_eq!(config.global.resolution_confirm_budget, 2);
        // A group override applies over the global value.
        assert!(config.for_group("-100777").vector_resolution);
        assert_eq!(config.for_group("-100777").vector_match_threshold, 0.90);
        // Keys the group does not override inherit the global values.
        assert_eq!(config.for_group("-100777").vector_candidate_threshold, 0.75);
        assert_eq!(config.for_group("-100777").resolution_confirm_budget, 2);
        // A group without an override receives the global values.
        assert!(!config.for_group("-100999").vector_resolution);
        assert_eq!(config.for_group("-100999").vector_match_threshold, 0.95);
        // Keys the TOML does not set keep the decision-73 defaults.
        let plain = BotConfig::from_toml_str("[global]\n").expect("an empty overlay loads");
        assert!(plain.global.vector_resolution);
        assert_eq!(plain.global.vector_match_threshold, 0.92);
        assert_eq!(plain.global.vector_candidate_threshold, 0.80);
        assert_eq!(plain.global.resolution_confirm_budget, 5);
    }

    #[test]
    fn toml_override_sets_merge_candidate_threshold() {
        // Decision 74 point 5: an independent threshold key, per-group
        // overridable like every other trigger key (AGENT.md Section
        // 6.3).
        let text = r#"
[global]
merge_candidate_threshold = 0.90

[groups."-100777"]
merge_candidate_threshold = 0.88
"#;
        let config = BotConfig::from_toml_str(text).expect("the TOML loads");
        // Global set.
        assert_eq!(config.global.merge_candidate_threshold, 0.90);
        // A group override wins over the global value.
        assert_eq!(config.for_group("-100777").merge_candidate_threshold, 0.88);
        // A group without an override inherits the global value.
        assert_eq!(config.for_group("-100999").merge_candidate_threshold, 0.90);
        // Keys the TOML does not set keep the decision-74 default.
        let plain = BotConfig::from_toml_str("[global]\n").expect("an empty overlay loads");
        assert_eq!(plain.global.merge_candidate_threshold, 0.85);
    }

    #[test]
    fn toml_override_sets_single_value_predicates() {
        // Decision 75: the single-value predicate registry follows the
        // per-key overlay pattern (AGENT.md Section 6.3), with one
        // twist: a set overlay REPLACES the whole list — no merging,
        // so a group override wins wholesale.
        let text = r#"
[global]
single_value_predicates = ["works_at", "lives_in"]

[groups."-100777"]
single_value_predicates = ["favorite_food"]
"#;
        let config = BotConfig::from_toml_str(text).expect("the TOML loads");
        // The global set parses (a TOML array of strings) and applies.
        assert_eq!(
            config.global.single_value_predicates,
            vec!["works_at".to_string(), "lives_in".to_string()]
        );
        // A group override REPLACES the list wholesale: the global
        // entries do NOT carry over into this group.
        assert_eq!(
            config.for_group("-100777").single_value_predicates,
            vec!["favorite_food".to_string()]
        );
        // A group without an override inherits the global list.
        assert_eq!(
            config.for_group("-100999").single_value_predicates,
            vec!["works_at".to_string(), "lives_in".to_string()]
        );
        // An EMPTY array parses: a group can declare zero single-value
        // predicates (every predicate is then multi-value for it).
        let empty_text = r#"
[groups."-100888"]
single_value_predicates = []
"#;
        let empty_config = BotConfig::from_toml_str(empty_text).expect("the TOML loads");
        assert!(empty_config
            .for_group("-100888")
            .single_value_predicates
            .is_empty());
        // The key the TOML does not set keeps the Section 13 default.
        let plain = BotConfig::from_toml_str("[global]\n").expect("an empty overlay loads");
        assert_eq!(
            plain.global.single_value_predicates,
            vec![
                "currently_playing".to_string(),
                "works_at".to_string(),
                "lives_in".to_string(),
                "dating".to_string(),
            ]
        );
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
    fn toml_parse_and_apply_covers_the_embedding_keys() {
        // Decision 66 introduced the embedding keys: flat, GLOBAL-ONLY,
        // and concrete (no Option); decision 81 re-pins the default.
        let defaults = TriggerConfig::default();
        assert_eq!(defaults.embedding_model, "google/gemini-embedding-2");
        assert_eq!(
            defaults.embedding_llm_base_url,
            "https://openrouter.ai/api/v1"
        );

        // Explicit values in [global] parse and apply.
        let text = r#"
[global]
embedding_model = "text-embedding-3-large"
embedding_llm_base_url = "https://embeddings.example/v1"
"#;
        let config = BotConfig::from_toml_str(text).expect("the embedding TOML loads");
        assert_eq!(config.global.embedding_model, "text-embedding-3-large");
        assert_eq!(
            config.global.embedding_llm_base_url,
            "https://embeddings.example/v1"
        );

        // Keys the TOML does not set keep the decision-81 default.
        let plain = BotConfig::from_toml_str("[global]\n").expect("an empty overlay loads");
        assert_eq!(plain.global.embedding_model, "google/gemini-embedding-2");
        assert_eq!(
            plain.global.embedding_llm_base_url,
            "https://openrouter.ai/api/v1"
        );
    }

    #[test]
    fn active_hours_parses_the_shape_and_rejects_bad_windows() {
        // specs.md Section 8.4: "HH:MM-HH:MM", host-local, same-day.
        let hours = ActiveHours::parse("08:00-23:00").expect("the default window parses");
        assert_eq!(hours, ActiveHours::DEFAULT);
        assert_eq!(hours.span_seconds(), 15 * 60 * 60);
        assert_eq!(
            ActiveHours::parse("00:30-01:45").expect("a short window parses"),
            ActiveHours {
                start_minutes: 30,
                end_minutes: 105,
            }
        );
        // Bad shape, out-of-range times, and overnight/empty windows
        // all error with a reason.
        for text in ["8-23", "08:00/23:00", "25:00-26:00", "08:60-23:00"] {
            assert!(ActiveHours::parse(text).is_err(), "{text} must fail");
        }
        let overnight = ActiveHours::parse("23:00-08:00").expect_err("overnight fails");
        assert!(
            overnight.contains("same-day"),
            "the overnight reason names the same-day rule: {overnight}"
        );
        let empty = ActiveHours::parse("08:00-08:00").expect_err("an empty window fails");
        assert!(empty.contains("same-day"), "{empty}");
    }

    #[test]
    fn toml_override_sets_decision_78_warmup_keys() {
        // Decision 78 (e): the six warmup keys follow the same per-key
        // overlay pattern as every other trigger key (AGENT.md Section
        // 6.3: overridable per group).
        let text = r#"
[global]
warmup = false
warmup_quota = 2
warmup_active_hours = "09:30-22:00"
warmup_silence_secs = 7200
warmup_reaction_window_secs = 900
warmup_topic_cooldown_days = 5

[groups."-100777"]
warmup = true
warmup_quota = 3
"#;
        let config = BotConfig::from_toml_str(text).expect("the warmup TOML loads");
        let global = &config.global;
        assert!(!global.warmup);
        assert_eq!(global.warmup_quota, 2);
        assert_eq!(
            global.warmup_active_hours,
            ActiveHours {
                start_minutes: 9 * 60 + 30,
                end_minutes: 22 * 60,
            }
        );
        assert_eq!(global.warmup_silence, Duration::from_secs(7200));
        assert_eq!(global.warmup_reaction_window, Duration::from_secs(900));
        assert_eq!(global.warmup_topic_cooldown_days, 5);
        // A group override wins over the global value; keys the group
        // does not set inherit the global values.
        let overridden = config.for_group("-100777");
        assert!(overridden.warmup);
        assert_eq!(overridden.warmup_quota, 3);
        assert_eq!(overridden.warmup_active_hours, global.warmup_active_hours);
        assert_eq!(overridden.warmup_silence, global.warmup_silence);
        assert_eq!(
            overridden.warmup_reaction_window,
            global.warmup_reaction_window
        );
        assert_eq!(
            overridden.warmup_topic_cooldown_days,
            global.warmup_topic_cooldown_days
        );
        // A group without an override receives the global values.
        assert!(!config.for_group("-100999").warmup);
        assert_eq!(config.for_group("-100999").warmup_quota, 2);
        // Keys the TOML does not set keep the decision-78 defaults.
        let plain = BotConfig::from_toml_str("[global]\n").expect("an empty overlay loads");
        assert!(plain.global.warmup);
        assert_eq!(plain.global.warmup_quota, 1);
        assert_eq!(plain.global.warmup_active_hours, ActiveHours::DEFAULT);
    }

    #[test]
    fn toml_override_sets_forced_wake_cooldown() {
        // Decision 79 (c) (specs.md Sections 8.1/13): the forced-wake
        // cooldown follows the same per-key overlay pattern as every
        // other trigger key (AGENT.md Section 6.3: overridable per
        // group).
        let text = r#"
[global]
forced_wake_cooldown_secs = 30

[groups."-100777"]
forced_wake_cooldown_secs = 5
"#;
        let config = BotConfig::from_toml_str(text).expect("the TOML loads");
        assert_eq!(config.global.forced_wake_cooldown, Duration::from_secs(30));
        // A group override wins over the global value.
        assert_eq!(
            config.for_group("-100777").forced_wake_cooldown,
            Duration::from_secs(5)
        );
        // A group without an override inherits the global value.
        assert_eq!(
            config.for_group("-100999").forced_wake_cooldown,
            Duration::from_secs(30)
        );
        // Keys the TOML does not set keep the decision-79 default.
        let plain = BotConfig::from_toml_str("[global]\n").expect("an empty overlay loads");
        assert_eq!(plain.global.forced_wake_cooldown, Duration::from_secs(10));
        // 0 parses and applies: the suppression is disabled (decision
        // 79 (c)).
        let disabled = BotConfig::from_toml_str("[global]\nforced_wake_cooldown_secs = 0\n")
            .expect("a zero cooldown loads");
        assert_eq!(disabled.global.forced_wake_cooldown, Duration::ZERO);
    }

    #[test]
    fn warmup_quota_out_of_range_is_a_loud_error() {
        // Decision 78 (e): quota outside 1..=3 fails at startup naming
        // the key and the value — never a silent clamp.
        for value in [0, 4] {
            let text = format!("[global]\nwarmup_quota = {value}\n");
            let error = BotConfig::from_toml_str(&text)
                .expect_err("an out-of-range quota must fail loudly");
            let message = error.to_string();
            assert!(
                message.contains("warmup_quota"),
                "the error names the key: {message}"
            );
            assert!(
                message.contains(&value.to_string()),
                "the error names the value {value}: {message}"
            );
        }
    }

    #[test]
    fn malformed_warmup_active_hours_is_a_loud_error() {
        // Decision 78 (e): a malformed window string fails at startup
        // naming the key and the value — apply never sees it.
        for value in ["8-23", "08:00/23:00", "25:00-26:00"] {
            let text = format!("[global]\nwarmup_active_hours = \"{value}\"\n");
            let error =
                BotConfig::from_toml_str(&text).expect_err("a malformed window must fail loudly");
            let message = error.to_string();
            assert!(
                message.contains("warmup_active_hours"),
                "the error names the key: {message}"
            );
            assert!(
                message.contains(value),
                "the error names the value {value}: {message}"
            );
        }
        // An overnight window fails with a reason naming the same-day
        // rule (specs.md Section 8.4).
        let text = "[global]\nwarmup_active_hours = \"23:00-08:00\"\n";
        let error = BotConfig::from_toml_str(text).expect_err("an overnight window must fail");
        let message = error.to_string();
        assert!(message.contains("warmup_active_hours"), "{message}");
        assert!(message.contains("23:00-08:00"), "{message}");
        assert!(
            message.contains("same-day"),
            "the reason names the same-day rule: {message}"
        );
    }

    #[test]
    fn an_invalid_warmup_value_under_a_group_names_the_group() {
        // Decision 78 (e): the validation runs on every group overlay
        // too, and the error names the group.
        let text = r#"
[global]
warmup_active_hours = "09:00-21:00"

[groups."-1001"]
warmup_quota = 4
"#;
        let error = BotConfig::from_toml_str(text).expect_err("the group quota must fail loudly");
        let message = error.to_string();
        assert!(message.contains("warmup_quota"), "{message}");
        assert!(message.contains('4'), "{message}");
        assert!(
            message.contains("-1001"),
            "the error names the group: {message}"
        );
        // A malformed window under a group also fails, naming the group
        // — even though apply would have skipped it.
        let text = r#"
[groups."-1001"]
warmup_active_hours = "8-23"
"#;
        let error = BotConfig::from_toml_str(text).expect_err("the group window must fail loudly");
        let message = error.to_string();
        assert!(message.contains("warmup_active_hours"), "{message}");
        assert!(message.contains("-1001"), "{message}");
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

    #[test]
    fn embedding_enabled_defaults_to_true_and_parses_under_global() {
        // Decision 77 (M6a): the kill switch is flat, GLOBAL-ONLY, and
        // concrete — true by default, `false` under [global] applies.
        let defaults = TriggerConfig::default();
        assert!(defaults.embedding_enabled);

        let text = r#"
[global]
embedding_enabled = false
"#;
        let config = BotConfig::from_toml_str(text).expect("the TOML loads");
        assert!(!config.global.embedding_enabled);
        // No group override exists (global-only): every group resolves
        // the global value.
        assert!(!config.for_group("-100999").embedding_enabled);

        // A key the TOML does not set keeps the default.
        let plain = BotConfig::from_toml_str("[global]\n").expect("an empty overlay loads");
        assert!(plain.global.embedding_enabled);
    }

    #[test]
    fn global_only_keys_under_a_group_are_a_loud_parse_error() {
        // Decision 77 (S6-F7): the verified global-only set is
        // llm_session_id, embedding_model, embedding_llm_base_url, and
        // embedding_enabled. Each one under [groups.*] is a ConfigError
        // naming the key AND the group.
        for (key, value) in [
            ("llm_session_id", "\"my-deployment\""),
            ("embedding_model", "\"text-embedding-3-large\""),
            (
                "embedding_llm_base_url",
                "\"https://embeddings.example/v1\"",
            ),
            ("embedding_enabled", "false"),
        ] {
            let text = format!("[groups.\"-100777\"]\n{key} = {value}\n");
            let error = BotConfig::from_toml_str(&text)
                .expect_err(&format!("{key} under a group must fail loudly"));
            let message = error.to_string();
            assert!(
                message.contains(key),
                "the error names the key {key}: {message}"
            );
            assert!(
                message.contains("-100777"),
                "the error names the group: {message}"
            );
        }
        // Per-group keys (llm_api/llm_base_url are NOT global-only)
        // still parse under a group.
        let per_group = r#"
[groups."-100777"]
llm_api = "openai-compatible"
llm_base_url = "https://group.example/v1"
"#;
        assert!(BotConfig::from_toml_str(per_group).is_ok());
    }

    #[test]
    fn global_only_keys_under_global_still_parse() {
        // The same four keys under [global] parse and apply.
        let text = r#"
[global]
llm_session_id = "my-deployment"
embedding_model = "text-embedding-3-large"
embedding_llm_base_url = "https://embeddings.example/v1"
embedding_enabled = false
"#;
        let config = BotConfig::from_toml_str(text).expect("the global-only TOML loads");
        assert_eq!(
            config.global.llm_session_id.as_deref(),
            Some("my-deployment")
        );
        assert_eq!(config.global.embedding_model, "text-embedding-3-large");
        assert_eq!(
            config.global.embedding_llm_base_url,
            "https://embeddings.example/v1"
        );
        assert!(!config.global.embedding_enabled);
    }

    #[test]
    fn toml_parse_and_apply_covers_the_caption_keys() {
        // Decision 82 (c): the caption keys are flat and concrete like
        // the embedding keys, but PER-GROUP overridable (unlike them).
        let defaults = TriggerConfig::default();
        assert_eq!(defaults.caption_model, "minimax/minimax-m3");
        assert_eq!(
            defaults.caption_llm_base_url,
            "https://openrouter.ai/api/v1"
        );

        // Explicit values in [global] parse and apply.
        let text = r#"
[global]
caption_model = "acme/vision-1"
caption_llm_base_url = "https://captions.example/v1"
"#;
        let config = BotConfig::from_toml_str(text).expect("the caption TOML loads");
        assert_eq!(config.global.caption_model, "acme/vision-1");
        assert_eq!(
            config.global.caption_llm_base_url,
            "https://captions.example/v1"
        );

        // Keys the TOML does not set keep the decision-82 defaults.
        let plain = BotConfig::from_toml_str("[global]\n").expect("an empty overlay loads");
        assert_eq!(plain.global.caption_model, "minimax/minimax-m3");
        assert_eq!(
            plain.global.caption_llm_base_url,
            "https://openrouter.ai/api/v1"
        );
    }

    #[test]
    fn caption_keys_are_per_group_overridable() {
        // Decision 82 (c): UNLIKE the embedding keys, the caption keys
        // are NOT in the global-only set — a `[groups.*]` table
        // carrying them is NOT rejected by the global-only check, and
        // the override applies over the default.
        let text = r#"
[groups."-100"]
caption_model = "acme/other"
caption_llm_base_url = "https://group-captions.example/v1"
"#;
        let config = BotConfig::from_toml_str(text)
            .expect("caption keys under a group are not global-only: the TOML loads");
        let overridden = config.for_group("-100");
        assert_eq!(overridden.caption_model, "acme/other");
        assert_eq!(
            overridden.caption_llm_base_url,
            "https://group-captions.example/v1"
        );
        // A group without an override receives the decision-82
        // defaults (no [global] overlay set them).
        let plain = config.for_group("-200");
        assert_eq!(plain.caption_model, "minimax/minimax-m3");
        assert_eq!(plain.caption_llm_base_url, "https://openrouter.ai/api/v1");
    }

    #[test]
    fn unknown_keys_load_warn_collect_and_apply_nothing() {
        // Decision 84 (e): a typo key under [global] LOADS fine (warn-
        // only, no deny_unknown_fields — forward compatibility), lands
        // in the unknown collection, and applies NOTHING: the real
        // field keeps its default.
        let text = r#"
[global]
wake_msg_count = 10
digest_max_chars_words = 1000

[groups."-100"]
wake_floor_secs = 60
digest_max_chars_bytes = 4096
"#;
        let config = BotConfig::from_toml_str(text).expect("a typo key must load, warn-only");
        // The known keys apply; the typo keys change nothing.
        assert_eq!(config.global.wake_msg_count, 10);
        assert_eq!(config.global.digest_max_words, 2500);
        assert_eq!(config.for_group("-100").wake_floor, Duration::from_secs(60));
        assert_eq!(config.for_group("-100").digest_max_bytes, 20 * 1024);
        // The collection names the table and the key, one pair each,
        // deterministically ordered.
        let parsed: BotConfigToml = toml::from_str(text).expect("the TOML parses");
        assert_eq!(
            unknown_keys(&parsed),
            vec![
                ("global".to_string(), "digest_max_chars_words".to_string()),
                (
                    "groups.-100".to_string(),
                    "digest_max_chars_bytes".to_string()
                ),
            ]
        );
        // The same unknown set sits in the group overlay that
        // `BotConfig` stores.
        assert!(config.overrides["-100"]
            .unknown
            .contains_key("digest_max_chars_bytes"));
        assert!(!config.overrides["-100"]
            .unknown
            .contains_key("wake_floor_secs"));
    }

    #[test]
    fn known_keys_never_land_in_the_unknown_collection() {
        // Every key of the motivating TOML is known: the collection is
        // empty, so no WARN fires (known keys never warn).
        for text in [EXAMPLE_TOML, "[global]\n", "\n"] {
            let parsed: BotConfigToml = toml::from_str(text).expect("the TOML parses");
            assert!(
                unknown_keys(&parsed).is_empty(),
                "no unknown keys in {text:?}"
            );
        }
        // A config setting several known keys across global and groups.
        let text = r#"
[global]
warmup = false
deep_recall = false
vector_match_threshold = 0.95
single_value_predicates = ["works_at"]

[groups."-100777"]
gate_context = true
forced_wake_cooldown_secs = 5
"#;
        let parsed: BotConfigToml = toml::from_str(text).expect("the TOML parses");
        assert!(unknown_keys(&parsed).is_empty());
        assert!(BotConfig::from_toml_str(text).is_ok());
    }

    #[test]
    fn the_motivating_typo_keys_are_flagged_and_ignored() {
        // Decision 84 (e) regression: the live tamako.toml carried
        // `digest_max_chars_words`/`digest_max_chars_bytes`, SILENTLY
        // ignored (the real keys are `digest_max_words`/
        // `digest_max_bytes`). Both typo keys now surface in the
        // unknown collection (one curated WARN each at load) while the
        // real fields keep their Section 8.2 defaults.
        let text = r#"
[global]
digest_max_chars_words = 1000
digest_max_chars_bytes = 4096
"#;
        let config = BotConfig::from_toml_str(text).expect("the typo config loads");
        assert_eq!(config.global.digest_max_words, 2500);
        assert_eq!(config.global.digest_max_bytes, 20 * 1024);
        let parsed: BotConfigToml = toml::from_str(text).expect("the TOML parses");
        assert_eq!(
            unknown_keys(&parsed),
            vec![
                ("global".to_string(), "digest_max_chars_bytes".to_string()),
                ("global".to_string(), "digest_max_chars_words".to_string()),
            ]
        );
    }

    #[test]
    fn unknown_keys_do_not_break_serialization() {
        // The flatten catch-all must not break the Serialize derive: a
        // config without unknown keys serializes exactly as before (the
        // catch-all emits nothing), and a config WITH unknown keys
        // round-trips them through the unknown map.
        let parsed: BotConfigToml = toml::from_str(EXAMPLE_TOML).expect("the example TOML parses");
        let serialized = toml::to_string(&parsed).expect("the config serializes");
        assert!(
            !serialized.contains("unknown"),
            "the catch-all emits nothing when empty: {serialized}"
        );
        let with_typo: BotConfigToml = toml::from_str("[global]\nfuture_key = 1\n")
            .expect("a future key parses (forward compatibility)");
        assert_eq!(
            unknown_keys(&with_typo),
            vec![("global".to_string(), "future_key".to_string())]
        );
    }

    #[test]
    fn suffix_mode_defaults_to_system() {
        let config = TriggerConfig::default();
        assert_eq!(config.suffix_mode, SuffixMode::System);
    }

    #[test]
    fn suffix_mode_parses_from_toml_and_overrides_per_group() {
        let toml = r#"
[global]
suffix_mode = "append"

[groups."-100123"]
suffix-mode = "system"

[groups."-100456"]
# Uses global
"#;
        let config = BotConfig::from_toml_str(toml).expect("config parses");
        assert_eq!(config.global.suffix_mode, SuffixMode::Append);
        assert_eq!(config.for_group("-100123").suffix_mode, SuffixMode::System);
        assert_eq!(config.for_group("-100456").suffix_mode, SuffixMode::Append);
    }
}
