//! Session state of one group and its store.db encoding.
//! Refer to specs.md Sections 5.2 and 6.1.
//!
//! The actor persists the session after every mutation (specs.md
//! Section 6.1, rule 4) and rebuilds it from the state table on restart.

use std::collections::HashMap;
use std::time::Duration;

use rand::Rng;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::config::TriggerConfig;
use crate::trigger::{WakeScheduler, WakeSchedulerState};

// State-table keys of specs.md Section 5.2. The store crate treats the
// state table as a generic key-value store; these keys are the contract
// between encode/decode here and the rows in store.db.
const KEY_DIGEST_BOUNDARY: &str = "last_digest_boundary_msg_id";
const KEY_LAST_DIGEST_AT: &str = "last_digest_at";
const KEY_MUTED: &str = "muted_flag";
const KEY_CONSECUTIVE_BOT: &str = "consecutive_bot_msgs";
const KEY_WAKE_MSGS: &str = "wake_msgs_since_wake";
const KEY_WAKE_LAST_AT: &str = "wake_last_wake_at";
const KEY_WAKE_INTERVAL_MS: &str = "wake_current_interval_ms";

/// Session state of one group. The actor persists it after every mutation
/// (specs.md Section 6.1, rule 4) and rebuilds it on restart.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionState {
    /// specs.md Section 7.1: splits the previous digested chunk from the tail.
    pub last_digest_boundary_msg_id: i64,
    /// The time of the last successful digest. `None` means the tail was
    /// never digested (specs.md Section 8.2, timeout fallback).
    pub last_digest_at: Option<OffsetDateTime>,
    /// specs.md Section 8.5: the monologue lock state.
    pub muted: bool,
    pub consecutive_bot_msgs: u32,
    pub wake: WakeSchedulerState,
}

/// Truncates a duration to whole milliseconds.
///
/// The persisted encoding of `wake_current_interval_ms` is decimal
/// milliseconds. Normalizing the live interval to whole milliseconds keeps
/// the encode/decode round-trip lossless: a restarted actor rebuilds a
/// bitwise-identical state (Phase 0 exit criterion, dev-roadmap.md
/// Section 2). The jittered interval of specs.md Section 8.3 loses at most
/// one millisecond, which is immaterial at a one-hour scale.
pub(crate) fn round_to_millis(duration: Duration) -> Duration {
    // as_millis fits u64 for every duration this system produces. The
    // saturating fallback covers the theoretical overflow.
    let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
    Duration::from_millis(millis)
}

impl SessionState {
    /// Creates a fresh state for a group that has no persisted state.
    /// The wake scheduler rolls its first jittered interval from `now`.
    pub fn new(config: &TriggerConfig, now: OffsetDateTime, rng: &mut impl Rng) -> Self {
        let mut wake = WakeScheduler::new(config, now, rng).snapshot();
        wake.current_interval = round_to_millis(wake.current_interval);
        Self {
            last_digest_boundary_msg_id: 0,
            last_digest_at: None,
            muted: false,
            consecutive_bot_msgs: 0,
            wake,
        }
    }

    /// Encodes the state as state-table key-value pairs.
    ///
    /// Keys (specs.md Section 5.2):
    /// `last_digest_boundary_msg_id` (decimal), `last_digest_at` (RFC 3339;
    /// `None` encodes as the empty string), `muted_flag` ("0"/"1"),
    /// `consecutive_bot_msgs` (decimal), `wake_msgs_since_wake` (decimal),
    /// `wake_last_wake_at` (RFC 3339), `wake_current_interval_ms` (decimal
    /// milliseconds).
    pub fn encode(&self) -> Vec<(String, String)> {
        // Rfc3339 formatting fails only for years outside 0..=9999.
        let last_wake_at = self
            .wake
            .last_wake_at
            .format(&Rfc3339)
            .unwrap_or_else(|_| String::new());
        let last_digest_at = self
            .last_digest_at
            .map(|at| at.format(&Rfc3339).unwrap_or_else(|_| String::new()))
            .unwrap_or_default();
        vec![
            (
                KEY_DIGEST_BOUNDARY.to_string(),
                self.last_digest_boundary_msg_id.to_string(),
            ),
            (KEY_LAST_DIGEST_AT.to_string(), last_digest_at),
            (
                KEY_MUTED.to_string(),
                if self.muted { "1" } else { "0" }.to_string(),
            ),
            (
                KEY_CONSECUTIVE_BOT.to_string(),
                self.consecutive_bot_msgs.to_string(),
            ),
            (
                KEY_WAKE_MSGS.to_string(),
                self.wake.msgs_since_wake.to_string(),
            ),
            (KEY_WAKE_LAST_AT.to_string(), last_wake_at),
            (
                KEY_WAKE_INTERVAL_MS.to_string(),
                self.wake.current_interval.as_millis().to_string(),
            ),
        ]
    }

    /// Rebuilds the state from the persisted key-value rows.
    ///
    /// The function is total: a missing key (first start) falls back to the
    /// fresh default, and a malformed value falls back to the fresh default
    /// of THAT field. Well-formed input round-trips losslessly through
    /// `encode`. Nothing is logged here; the caller sees the result.
    pub fn decode(
        map: &HashMap<String, String>,
        config: &TriggerConfig,
        now: OffsetDateTime,
        rng: &mut impl Rng,
    ) -> Self {
        let fresh = Self::new(config, now, rng);
        let muted = match map.get(KEY_MUTED).map(String::as_str) {
            Some("0") => false,
            Some("1") => true,
            _ => fresh.muted,
        };
        Self {
            last_digest_boundary_msg_id: map
                .get(KEY_DIGEST_BOUNDARY)
                .and_then(|value| value.parse().ok())
                .unwrap_or(fresh.last_digest_boundary_msg_id),
            // A missing, empty, or malformed value means the tail was
            // never digested: None is the fresh default.
            last_digest_at: map
                .get(KEY_LAST_DIGEST_AT)
                .filter(|value| !value.is_empty())
                .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok()),
            muted,
            consecutive_bot_msgs: map
                .get(KEY_CONSECUTIVE_BOT)
                .and_then(|value| value.parse().ok())
                .unwrap_or(fresh.consecutive_bot_msgs),
            wake: WakeSchedulerState {
                msgs_since_wake: map
                    .get(KEY_WAKE_MSGS)
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(fresh.wake.msgs_since_wake),
                last_wake_at: map
                    .get(KEY_WAKE_LAST_AT)
                    .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
                    .unwrap_or(fresh.wake.last_wake_at),
                current_interval: map
                    .get(KEY_WAKE_INTERVAL_MS)
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(Duration::from_millis)
                    .unwrap_or(fresh.wake.current_interval),
            },
        }
    }

    /// specs.md Section 8.5: any human message clears the monologue lock.
    pub fn record_human_message(&mut self) {
        self.muted = false;
        self.consecutive_bot_msgs = 0;
    }

    /// Records one message of the bot. Rule B1 (specs.md Section 10.4): the
    /// bot's own speech is part of the raw log. At `monologue_limit`
    /// consecutive bot messages the monologue lock engages (specs.md
    /// Section 8.5). The mechanism exists now; the live call sites enter in
    /// Phase 1 (anti-defer list, dev-roadmap.md Section 7).
    pub fn record_bot_message(&mut self, config: &TriggerConfig) {
        self.consecutive_bot_msgs = self.consecutive_bot_msgs.saturating_add(1);
        if self.consecutive_bot_msgs >= config.monologue_limit {
            self.muted = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    fn fixed_now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("a valid unix timestamp")
    }

    #[test]
    fn encode_decode_round_trip_is_identical() {
        let config = TriggerConfig::default();
        let mut rng = StdRng::seed_from_u64(42);
        let mut state = SessionState::new(&config, fixed_now(), &mut rng);
        state.last_digest_boundary_msg_id = 137;
        state.last_digest_at = Some(fixed_now());
        state.wake.msgs_since_wake = 3;

        let encoded = state.encode();
        let map: HashMap<String, String> = encoded.into_iter().collect();
        let decoded = SessionState::decode(&map, &config, fixed_now(), &mut rng);

        assert_eq!(decoded, state);
    }

    #[test]
    fn decode_of_missing_empty_or_malformed_last_digest_at_is_none() {
        let config = TriggerConfig::default();
        let mut rng = StdRng::seed_from_u64(42);
        let state = SessionState::new(&config, fixed_now(), &mut rng);
        let map: HashMap<String, String> = state.encode().into_iter().collect();

        // Missing key.
        let decoded = SessionState::decode(
            &HashMap::new(),
            &config,
            fixed_now(),
            &mut StdRng::seed_from_u64(42),
        );
        assert_eq!(decoded.last_digest_at, None);

        // Empty value (the encoding of None).
        assert_eq!(map.get(KEY_LAST_DIGEST_AT), Some(&String::new()));
        let decoded = SessionState::decode(&map, &config, fixed_now(), &mut rng);
        assert_eq!(decoded.last_digest_at, None);

        // Malformed value falls back to the fresh default (None).
        let mut corrupt = map;
        corrupt.insert(KEY_LAST_DIGEST_AT.to_string(), "not-a-date".to_string());
        let decoded = SessionState::decode(&corrupt, &config, fixed_now(), &mut rng);
        assert_eq!(decoded.last_digest_at, None);
    }

    #[test]
    fn decode_of_an_empty_map_equals_fresh_state() {
        let config = TriggerConfig::default();
        // Both constructions use the same seed and the same `now`, so the
        // fresh fallback inside decode rolls the same jittered interval.
        let fresh = SessionState::new(&config, fixed_now(), &mut StdRng::seed_from_u64(7));
        let decoded = SessionState::decode(
            &HashMap::new(),
            &config,
            fixed_now(),
            &mut StdRng::seed_from_u64(7),
        );
        assert_eq!(decoded, fresh);
    }

    #[test]
    fn decode_falls_back_per_field_on_malformed_values() {
        let config = TriggerConfig::default();
        let mut rng = StdRng::seed_from_u64(9);
        let state = SessionState::new(&config, fixed_now(), &mut rng);
        let mut map: HashMap<String, String> = state.encode().into_iter().collect();
        // Corrupt one field. The other fields must still decode.
        map.insert(KEY_WAKE_MSGS.to_string(), "not-a-number".to_string());
        map.insert(KEY_MUTED.to_string(), "yes".to_string());

        let decoded =
            SessionState::decode(&map, &config, fixed_now(), &mut StdRng::seed_from_u64(9));
        assert_eq!(decoded.wake.msgs_since_wake, 0);
        assert!(!decoded.muted);
        // Untouched fields keep their persisted values.
        assert_eq!(
            decoded.last_digest_boundary_msg_id,
            state.last_digest_boundary_msg_id
        );
        assert_eq!(decoded.wake.current_interval, state.wake.current_interval);
    }

    #[test]
    fn human_message_clears_the_monologue_lock() {
        let config = TriggerConfig::default();
        let mut rng = StdRng::seed_from_u64(13);
        let mut state = SessionState::new(&config, fixed_now(), &mut rng);
        for _ in 0..config.monologue_limit {
            state.record_bot_message(&config);
        }
        assert!(state.muted);

        state.record_human_message();
        assert!(!state.muted);
        assert_eq!(state.consecutive_bot_msgs, 0);
    }

    #[test]
    fn bot_messages_at_the_limit_set_muted() {
        let config = TriggerConfig {
            monologue_limit: 2,
            ..TriggerConfig::default()
        };
        let mut rng = StdRng::seed_from_u64(17);
        let mut state = SessionState::new(&config, fixed_now(), &mut rng);

        state.record_bot_message(&config);
        assert!(!state.muted);
        assert_eq!(state.consecutive_bot_msgs, 1);

        state.record_bot_message(&config);
        assert!(state.muted);
        assert_eq!(state.consecutive_bot_msgs, 2);
    }
}
