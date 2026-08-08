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
        let elapsed = now - self.last_wake_at;
        // The floor caps the cost in very active groups where
        // `wake_msg_count` messages can arrive in seconds.
        if elapsed < to_time_duration(config.wake_floor) {
            return false;
        }
        self.msgs_since_wake >= config.wake_msg_count
            || elapsed >= to_time_duration(self.current_interval)
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

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    fn now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("a valid unix timestamp")
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
