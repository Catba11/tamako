//! Pure trigger scheduling logic. No tokio, no I/O.
//! Wake trigger: specs.md Section 8.3. Digest trigger: specs.md Section 8.2.
//! The wake and digest procedures are Phase 1 stubs. This module implements
//! only the scheduling.

use rand::Rng;
use std::time::Duration;
use time::OffsetDateTime;

use crate::config::TriggerConfig;

/// Serializable snapshot of the wake scheduler. The actor persists this
/// state in store.db and rebuilds the scheduler from it on restart.
/// Refer to specs.md Section 6.1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeSchedulerState {
    /// Messages received since the last wake. Refer to specs.md Section 8.1.
    pub msgs_since_wake: u32,
    /// The time of the last wake.
    pub last_wake_at: OffsetDateTime,
    /// The jittered interval now in force.
    pub current_interval: Duration,
}

/// The wake trigger state machine. Pure logic; the actor drives it.
#[derive(Debug)]
pub struct WakeScheduler {
    msgs_since_wake: u32,
    last_wake_at: OffsetDateTime,
    current_interval: Duration,
}

/// Which condition of the wake trigger fired (specs.md Section 8.3).
/// Telemetry only: `fire_reason` reports the reason of a fire; it never
/// changes the scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FireReason {
    /// `msgs_since_wake >= wake_msg_count`.
    MessageCount,
    /// `now - last_wake_at >= current_interval`.
    Interval,
}

impl FireReason {
    /// The telemetry spelling of the reason (the curated wake log line).
    pub fn as_str(&self) -> &'static str {
        match self {
            FireReason::MessageCount => "message_count",
            FireReason::Interval => "interval",
        }
    }
}

/// Converts a `std::time::Duration` to a `time::Duration`. A `std` duration
/// is never negative, so the conversion can only fail on overflow. The
/// saturating fallback covers that case.
fn to_time_duration(duration: Duration) -> time::Duration {
    time::Duration::try_from(duration).unwrap_or(time::Duration::MAX)
}

/// Rolls a fresh jittered interval:
/// `current_interval = wake_interval * U(wake_jitter_min, wake_jitter_max)`.
/// specs.md Section 8.3: the expectation stays stable.
fn roll_interval(config: &TriggerConfig, rng: &mut impl Rng) -> Duration {
    // A hand-edited configuration file can invert the range. Swap the bounds
    // instead of panicking inside `random_range`.
    let (min, max) = if config.wake_jitter_min <= config.wake_jitter_max {
        (config.wake_jitter_min, config.wake_jitter_max)
    } else {
        (config.wake_jitter_max, config.wake_jitter_min)
    };
    // A negative factor has no meaning. Clamp it instead of panicking
    // inside `mul_f64`.
    let factor = rng.random_range(min..=max).max(0.0);
    config.wake_interval.mul_f64(factor)
}

impl WakeScheduler {
    /// Creates a scheduler and rolls the first jittered interval.
    pub fn new(config: &TriggerConfig, now: OffsetDateTime, rng: &mut impl Rng) -> Self {
        Self {
            msgs_since_wake: 0,
            last_wake_at: now,
            current_interval: roll_interval(config, rng),
        }
    }

    /// Rebuilds a scheduler from a persisted snapshot. No jitter re-roll:
    /// the persisted interval stays in force.
    pub fn from_state(state: WakeSchedulerState) -> Self {
        Self {
            msgs_since_wake: state.msgs_since_wake,
            last_wake_at: state.last_wake_at,
            current_interval: state.current_interval,
        }
    }

    /// Returns a serializable snapshot of the state.
    pub fn snapshot(&self) -> WakeSchedulerState {
        WakeSchedulerState {
            msgs_since_wake: self.msgs_since_wake,
            last_wake_at: self.last_wake_at,
            current_interval: self.current_interval,
        }
    }

    /// specs.md Section 8.1: every inbound message increments the wake
    /// counter.
    pub fn record_message(&mut self) {
        // A counter overflow must never panic.
        self.msgs_since_wake = self.msgs_since_wake.saturating_add(1);
    }

    /// specs.md Section 8.3: fire when the FIRST of these is true —
    /// `msgs_since_wake >= wake_msg_count`, or
    /// `now - last_wake_at >= current_interval` — AND the floor holds:
    /// `now - last_wake_at >= wake_floor`.
    pub fn should_fire(&self, now: OffsetDateTime, config: &TriggerConfig) -> bool {
        self.fire_reason(now, config).is_some()
    }

    /// Like `should_fire`, but reports WHICH condition fired (`None`
    /// when the trigger does not fire). The count wins when both hold
    /// (it is checked first). `should_fire` delegates to this method,
    /// so the two can never diverge.
    pub fn fire_reason(&self, now: OffsetDateTime, config: &TriggerConfig) -> Option<FireReason> {
        let elapsed = now - self.last_wake_at;
        // The floor caps the cost in very active groups where
        // `wake_msg_count` messages can arrive in seconds.
        if elapsed < to_time_duration(config.wake_floor) {
            return None;
        }
        if self.msgs_since_wake >= config.wake_msg_count {
            Some(FireReason::MessageCount)
        } else if elapsed >= to_time_duration(self.current_interval) {
            Some(FireReason::Interval)
        } else {
            None
        }
    }

    /// Resets the counter and the timer. Rolls a fresh jitter factor so the
    /// expectation of the wake interval stays stable (specs.md Section 8.3).
    pub fn reset(&mut self, now: OffsetDateTime, config: &TriggerConfig, rng: &mut impl Rng) {
        self.msgs_since_wake = 0;
        self.last_wake_at = now;
        self.current_interval = roll_interval(config, rng);
    }
}

/// Statistics of the undigested tail. The actor maintains these counters
/// from the raw log.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TailStats {
    /// CJK characters in the tail.
    pub chars_cjk: usize,
    /// Messages in the tail.
    pub messages: u32,
    /// Words in the tail.
    pub words: u32,
    /// Bytes in the tail.
    pub bytes: usize,
    /// The time of the last successful digest. `None` means the tail was
    /// never digested.
    pub last_digest_at: Option<OffsetDateTime>,
}

impl TailStats {
    /// Returns true when the tail carries no content.
    fn is_empty(&self) -> bool {
        self.chars_cjk == 0 && self.messages == 0 && self.words == 0 && self.bytes == 0
    }
}

/// True for a CJK ideograph (U+4E00..=U+9FFF and extension A
/// U+3400..=U+4DBF), a kana character (U+3040..=U+30FF), or a hangul
/// syllable (U+AC00..=U+D7AF).
fn is_cjk(ch: char) -> bool {
    matches!(
        ch,
        '\u{4E00}'..='\u{9FFF}' | '\u{3400}'..='\u{4DBF}' | '\u{3040}'..='\u{30FF}' | '\u{AC00}'..='\u{D7AF}'
    )
}

/// Computes the tail statistics of specs.md Section 8.2 from the raw-log
/// rows of the tail (rows with id > last_digest_boundary_msg_id) and the
/// time of the last successful digest.
///
/// Counting rules:
/// - chars_cjk: count of CJK ideographs and kana/hangul syllables in all
///   texts;
/// - words: whitespace-separated tokens (CJK text counts through
///   chars_cjk);
/// - messages: row count; bytes: sum of text UTF-8 lengths;
/// - last_digest_at: passed through from the session.
pub fn tail_stats(
    rows: &[tamako_store::MessageRow],
    last_digest_at: Option<OffsetDateTime>,
) -> TailStats {
    let mut stats = TailStats {
        last_digest_at,
        ..TailStats::default()
    };
    for row in rows {
        // Counters saturate; a counter overflow must never panic.
        stats.messages = stats.messages.saturating_add(1);
        stats.bytes = stats.bytes.saturating_add(row.text.len());
        stats.chars_cjk = stats
            .chars_cjk
            .saturating_add(row.text.chars().filter(|ch| is_cjk(*ch)).count());
        let words = u32::try_from(row.text.split_whitespace().count()).unwrap_or(u32::MAX);
        stats.words = stats.words.saturating_add(words);
    }
    stats
}

/// specs.md Section 8.2: fire when the FIRST threshold is reached, or
/// (fallback) the tail is non-empty and the last digest is older than
/// `digest_timeout`.
///
/// A tail that was never digested (`last_digest_at` is `None`) does not
/// fire the timeout fallback. Only the size thresholds apply to it. This
/// prevents a digest of a tiny tail right after the bot joins a group.
pub fn digest_should_fire(tail: &TailStats, now: OffsetDateTime, config: &TriggerConfig) -> bool {
    if tail.chars_cjk >= config.digest_max_chars_cjk
        || tail.messages >= config.digest_max_messages
        || tail.words >= config.digest_max_words
        || tail.bytes >= config.digest_max_bytes
    {
        return true;
    }
    if tail.is_empty() {
        return false;
    }
    match tail.last_digest_at {
        Some(last_digest_at) => now - last_digest_at >= to_time_duration(config.digest_timeout),
        None => false,
    }
}

/// The tick cadence of the M4 timer driver. Choice: one eighth of the
/// wake interval, clamped below by 1 s (never a hot loop) and above by
/// min(wake_floor, 5 min) (the floor already bounds wake spacing; the
/// digest timeout fallback of Section 8.2 is far coarser than any
/// cadence this produces). At the defaults (1 h interval, 5 min floor)
/// the cadence is 5 min: a wake fires at most one cadence period late.
pub fn timer_cadence(config: &TriggerConfig) -> Duration {
    let upper = config.wake_floor.min(Duration::from_secs(5 * 60));
    // A wake_floor below 1 s must not panic: the lower bound never
    // exceeds the upper bound.
    let lower = Duration::from_secs(1).min(upper);
    (config.wake_interval / 8).clamp(lower, upper)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use tamako_store::{Direction, EventType, MessageRow};

    fn now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("a valid unix timestamp")
    }

    /// A raw-log row with the given id and text. Only `id` and `text`
    /// matter for tail_stats.
    fn row(id: i64, text: &str) -> MessageRow {
        MessageRow {
            id,
            platform_msg_id: format!("m{id}"),
            direction: Direction::Inbound,
            event_type: EventType::Message,
            timestamp: now(),
            sender_id: "u1".to_string(),
            sender_display_name: "Alice".to_string(),
            sender_username: None,
            text: text.to_string(),
            reply_to_platform_msg_id: None,
            mentions_bot: false,
            is_reply_to_bot: false,
            forward: None,
        }
    }

    #[test]
    fn timer_cadence_of_the_default_config_is_the_five_minute_cap() {
        // 1 h / 8 = 7.5 min, clamped above by min(5 min floor, 5 min).
        let config = TriggerConfig::default();
        assert_eq!(timer_cadence(&config), Duration::from_secs(5 * 60));
    }

    #[test]
    fn timer_cadence_is_never_a_hot_loop() {
        // 10 ms / 8 = 1.25 ms, clamped below by 1 s.
        let config = TriggerConfig {
            wake_interval: Duration::from_millis(10),
            ..TriggerConfig::default()
        };
        assert_eq!(timer_cadence(&config), Duration::from_secs(1));
    }

    #[test]
    fn timer_cadence_follows_a_sub_second_wake_floor() {
        // A wake_floor below 1 s becomes the upper bound; the lower bound
        // stays at or below it (no panic).
        let config = TriggerConfig {
            wake_floor: Duration::from_millis(500),
            ..TriggerConfig::default()
        };
        assert_eq!(timer_cadence(&config), Duration::from_millis(500));
    }

    #[test]
    fn timer_cadence_honors_a_wake_floor_below_the_cap() {
        // 1 h / 8 = 7.5 min, upper bound min(2 min floor, 5 min) = 2 min.
        let config = TriggerConfig {
            wake_floor: Duration::from_secs(2 * 60),
            ..TriggerConfig::default()
        };
        assert_eq!(timer_cadence(&config), Duration::from_secs(2 * 60));
    }

    #[test]
    fn timer_cadence_never_panics_on_tiny_values() {
        // Very small everything: the cadence stays a valid duration and
        // never exceeds the floor.
        let config = TriggerConfig {
            wake_interval: Duration::from_nanos(1),
            wake_floor: Duration::from_nanos(1),
            ..TriggerConfig::default()
        };
        assert_eq!(timer_cadence(&config), Duration::from_nanos(1));
    }

    #[test]
    fn tail_stats_of_empty_rows_is_empty() {
        let stats = tail_stats(&[], Some(now()));
        assert_eq!(
            stats,
            TailStats {
                last_digest_at: Some(now()),
                ..TailStats::default()
            }
        );
    }

    #[test]
    fn tail_stats_counts_mixed_english_and_cjk_text() {
        let rows = vec![
            row(1, "hello world"), // 2 words, 0 CJK, 11 bytes
            row(2, "你好世界"),    // 1 token, 4 CJK, 12 bytes
            row(3, "猫 cat ねこ"), // 3 tokens, 3 CJK, 14 bytes
        ];
        let stats = tail_stats(&rows, None);
        assert_eq!(stats.messages, 3);
        assert_eq!(stats.words, 6);
        assert_eq!(stats.chars_cjk, 7);
        assert_eq!(stats.bytes, 11 + 12 + 14);
        assert_eq!(stats.last_digest_at, None);
    }

    #[test]
    fn tail_stats_counts_kana_and_hangul_as_cjk() {
        let rows = vec![row(1, "ひらがな カタカナ"), row(2, "한국어")];
        let stats = tail_stats(&rows, Some(now()));
        assert_eq!(stats.chars_cjk, 4 + 4 + 3);
        assert_eq!(stats.words, 2 + 1);
    }

    #[test]
    fn tail_stats_passes_last_digest_at_through() {
        let at = now();
        assert_eq!(tail_stats(&[], Some(at)).last_digest_at, Some(at));
        assert_eq!(tail_stats(&[], None).last_digest_at, None);
    }

    #[test]
    fn jitter_stays_within_bounds_and_the_expectation_is_stable() {
        let config = TriggerConfig::default();
        let mut rng = StdRng::seed_from_u64(42);
        let start = now();
        let mut scheduler = WakeScheduler::new(&config, start, &mut rng);

        let lower = config.wake_interval.mul_f64(config.wake_jitter_min);
        let upper = config.wake_interval.mul_f64(config.wake_jitter_max);

        let samples = 10_000_u32;
        let mut total = Duration::ZERO;
        for _ in 0..samples {
            scheduler.reset(start, &config, &mut rng);
            let interval = scheduler.snapshot().current_interval;
            assert!(
                (lower..=upper).contains(&interval),
                "interval {interval:?} outside [{lower:?}, {upper:?}]"
            );
            total += interval;
        }

        // The jitter range is symmetric around 1.0, so the expectation of
        // the interval equals wake_interval. With 10 000 samples the
        // standard error is far below 2 percent.
        let average = total / samples;
        let band_low = config.wake_interval.mul_f64(0.98);
        let band_high = config.wake_interval.mul_f64(1.02);
        assert!(
            (band_low..=band_high).contains(&average),
            "average {average:?} outside +/-2% of {:?}",
            config.wake_interval
        );
    }

    #[test]
    fn from_state_restores_the_state_without_a_reroll() {
        let config = TriggerConfig::default();
        let mut rng = StdRng::seed_from_u64(7);
        let start = now();
        let mut scheduler = WakeScheduler::new(&config, start, &mut rng);
        scheduler.record_message();
        scheduler.record_message();

        let snapshot = scheduler.snapshot();
        let restored = WakeScheduler::from_state(snapshot.clone());
        assert_eq!(restored.snapshot(), snapshot);

        // The restored scheduler behaves like the original.
        let later = start + time::Duration::hours(2);
        assert_eq!(
            restored.should_fire(later, &config),
            scheduler.should_fire(later, &config)
        );
    }

    #[test]
    fn floor_blocks_fire_before_the_floor_elapses() {
        let config = TriggerConfig::default();
        let mut rng = StdRng::seed_from_u64(11);
        let start = now();
        let mut scheduler = WakeScheduler::new(&config, start, &mut rng);
        for _ in 0..config.wake_msg_count {
            scheduler.record_message();
        }
        // The count threshold is reached, but only one minute elapsed.
        assert!(!scheduler.should_fire(start + time::Duration::minutes(1), &config));
    }

    #[test]
    fn floor_allows_fire_after_the_floor_elapses() {
        let config = TriggerConfig::default();
        let mut rng = StdRng::seed_from_u64(13);
        let start = now();
        let mut scheduler = WakeScheduler::new(&config, start, &mut rng);
        for _ in 0..config.wake_msg_count {
            scheduler.record_message();
        }
        assert!(scheduler.should_fire(start + config.wake_floor, &config));
    }

    #[test]
    fn fires_on_count_before_the_interval_elapses() {
        // A large interval: only the count can fire inside this test.
        let config = TriggerConfig {
            wake_interval: Duration::from_secs(24 * 60 * 60),
            ..TriggerConfig::default()
        };
        let mut rng = StdRng::seed_from_u64(17);
        let start = now();
        let mut scheduler = WakeScheduler::new(&config, start, &mut rng);

        let after_floor = start + config.wake_floor;
        // One message below the count: no fire.
        for _ in 0..config.wake_msg_count - 1 {
            scheduler.record_message();
        }
        assert!(!scheduler.should_fire(after_floor, &config));
        // The count threshold is reached: fire, hours before the interval.
        scheduler.record_message();
        assert!(scheduler.should_fire(after_floor, &config));
    }

    #[test]
    fn fires_on_interval_before_the_count_is_reached() {
        // A short interval: current_interval is at most 1.3 * 10 minutes.
        let config = TriggerConfig {
            wake_interval: Duration::from_secs(10 * 60),
            ..TriggerConfig::default()
        };
        let mut rng = StdRng::seed_from_u64(19);
        let start = now();
        let mut scheduler = WakeScheduler::new(&config, start, &mut rng);

        // No messages at all. After 20 minutes the interval has elapsed
        // for every possible jitter roll.
        scheduler.record_message(); // one message, far below the count
        assert!(scheduler.should_fire(start + time::Duration::minutes(20), &config));
    }

    #[test]
    fn fire_reason_reports_the_count_when_the_count_fires() {
        // A large interval: only the count can fire inside this test.
        let config = TriggerConfig {
            wake_interval: Duration::from_secs(24 * 60 * 60),
            ..TriggerConfig::default()
        };
        let mut rng = StdRng::seed_from_u64(29);
        let start = now();
        let mut scheduler = WakeScheduler::new(&config, start, &mut rng);
        for _ in 0..config.wake_msg_count {
            scheduler.record_message();
        }
        let at = start + config.wake_floor;
        assert_eq!(
            scheduler.fire_reason(at, &config),
            Some(FireReason::MessageCount)
        );
        // One message below the count: no fire, no reason.
        let mut rng = StdRng::seed_from_u64(29);
        let mut scheduler = WakeScheduler::new(&config, start, &mut rng);
        for _ in 0..config.wake_msg_count - 1 {
            scheduler.record_message();
        }
        assert_eq!(scheduler.fire_reason(at, &config), None);
    }

    #[test]
    fn fire_reason_reports_the_interval_when_the_interval_fires() {
        // A short interval and a count that stays out of reach.
        let config = TriggerConfig {
            wake_interval: Duration::from_secs(10 * 60),
            ..TriggerConfig::default()
        };
        let mut rng = StdRng::seed_from_u64(31);
        let start = now();
        let mut scheduler = WakeScheduler::new(&config, start, &mut rng);
        scheduler.record_message(); // one message, far below the count
                                    // After 20 minutes the interval has elapsed for every jitter roll.
        let at = start + time::Duration::minutes(20);
        assert_eq!(
            scheduler.fire_reason(at, &config),
            Some(FireReason::Interval)
        );
        // Inside the interval: no fire, no reason.
        assert_eq!(
            scheduler.fire_reason(start + config.wake_floor, &config),
            None
        );
    }

    #[test]
    fn fire_reason_is_none_before_the_floor_elapses() {
        let config = TriggerConfig::default();
        let mut rng = StdRng::seed_from_u64(37);
        let start = now();
        let mut scheduler = WakeScheduler::new(&config, start, &mut rng);
        for _ in 0..config.wake_msg_count {
            scheduler.record_message();
        }
        // The count threshold is reached, but only one minute elapsed:
        // the floor suppresses the fire and the reason.
        assert_eq!(
            scheduler.fire_reason(start + time::Duration::minutes(1), &config),
            None
        );
    }

    #[test]
    fn fire_reason_prefers_the_count_when_both_conditions_hold() {
        // A short interval: after 20 minutes both the count and the
        // interval hold; the count is checked first, so it wins.
        let config = TriggerConfig {
            wake_interval: Duration::from_secs(10 * 60),
            ..TriggerConfig::default()
        };
        let mut rng = StdRng::seed_from_u64(41);
        let start = now();
        let mut scheduler = WakeScheduler::new(&config, start, &mut rng);
        for _ in 0..config.wake_msg_count {
            scheduler.record_message();
        }
        let at = start + time::Duration::minutes(20);
        assert!(scheduler.should_fire(at, &config));
        assert_eq!(
            scheduler.fire_reason(at, &config),
            Some(FireReason::MessageCount)
        );
    }

    #[test]
    fn fire_reason_spellings_are_stable() {
        // The telemetry spellings of the curated wake log line.
        assert_eq!(FireReason::MessageCount.as_str(), "message_count");
        assert_eq!(FireReason::Interval.as_str(), "interval");
    }

    #[test]
    fn digest_fires_on_each_size_threshold_individually() {
        let config = TriggerConfig::default();
        let now = now();
        let base = TailStats {
            last_digest_at: Some(now),
            ..TailStats::default()
        };

        assert!(digest_should_fire(
            &TailStats {
                chars_cjk: config.digest_max_chars_cjk,
                ..base
            },
            now,
            &config
        ));
        assert!(digest_should_fire(
            &TailStats {
                messages: config.digest_max_messages,
                ..base
            },
            now,
            &config
        ));
        assert!(digest_should_fire(
            &TailStats {
                words: config.digest_max_words,
                ..base
            },
            now,
            &config
        ));
        assert!(digest_should_fire(
            &TailStats {
                bytes: config.digest_max_bytes,
                ..base
            },
            now,
            &config
        ));

        // Control: a tail below every threshold with a fresh digest does
        // not fire.
        let small = TailStats {
            chars_cjk: 10,
            messages: 1,
            words: 5,
            bytes: 100,
            last_digest_at: Some(now),
        };
        assert!(!digest_should_fire(&small, now, &config));
    }

    #[test]
    fn digest_timeout_fallback_fires_only_when_the_tail_is_non_empty() {
        let config = TriggerConfig::default();
        let now = now();
        let old_digest = now - time::Duration::hours(7);

        // An empty tail never fires the timeout fallback.
        let empty = TailStats {
            last_digest_at: Some(old_digest),
            ..TailStats::default()
        };
        assert!(!digest_should_fire(&empty, now, &config));

        // A non-empty tail with a digest older than the timeout fires.
        let stale = TailStats {
            messages: 1,
            bytes: 10,
            last_digest_at: Some(old_digest),
            ..TailStats::default()
        };
        assert!(digest_should_fire(&stale, now, &config));

        // A non-empty tail with a recent digest does not fire.
        let fresh = TailStats {
            last_digest_at: Some(now - time::Duration::hours(1)),
            ..stale
        };
        assert!(!digest_should_fire(&fresh, now, &config));

        // A tail that was never digested does not fire the fallback.
        let never_digested = TailStats {
            messages: 1,
            bytes: 10,
            ..TailStats::default()
        };
        assert!(!digest_should_fire(&never_digested, now, &config));
    }
}
