//! The warmup trigger: contracts and pure logic. specs.md Sections
//! 8.4/8.5/9.7 (decision 78). No tokio, no I/O; the actor drives these
//! functions from its timer tick, and the generation implementation
//! lives in tamako-agent (the same seam pattern as `wake.rs`).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rand::Rng;
use time::format_description::FormatItem;
use time::macros::format_description;
use time::{Date, Duration as TimeDuration, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

use tamako_memory::identifiers::normalize;
use tamako_memory::TopicCandidate;

use crate::actor::CoreError;
use crate::config::{ActiveHours, TriggerConfig};
use crate::context::ContextMessage;

/// The warmup generation request (specs.md Section 9.7 step 3):
/// the live context as LLM-facing messages (preamble first, from
/// `LiveContext::messages_for_llm`) plus the chosen topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmupRequest {
    pub messages: Vec<ContextMessage>,
    /// The display name of the sampled topic (the warmup instruction names it).
    pub topic: String,
}

/// Warmup generation with the reply purpose (specs.md Section 9.7 step
/// 3; decision 78 (a)). The live implementation (tamako-agent) reuses
/// the resolved `reply` endpoint. A sibling seam of `ReplyGenerator`
/// (NOT a method on it): warmup has no reply target, and the decision-59
/// reply path stays byte-identical.
pub trait WarmupGenerator: Send + Sync {
    fn generate_warmup<'a>(
        &'a self,
        request: &'a WarmupRequest,
    ) -> Pin<Box<dyn Future<Output = Result<String, CoreError>> + Send + 'a>>;
}

/// The bundle the actor needs to run the warmup procedure. `None` in
/// `GroupActorParams` disables the trigger entirely (replay mode, or no
/// provider key — the same discipline as `wake: Option<WakeServices>`).
pub struct WarmupServices {
    pub generator: Arc<dyn WarmupGenerator>,
}

/// The memory-read cap of the topic sampling (specs.md Section 9.7
/// step 2): the top 100 Concepts of the group by edge degree.
pub const TOPIC_SAMPLE_LIMIT: u32 = 100;

/// The raw-log tail of the topic exclusion (specs.md Section 9.7 step
/// 2): a topic whose normalized name appears in the 50 newest raw-log
/// rows (any direction) is excluded — never restart the conversation
/// that just went quiet.
pub const TAIL_EXCLUSION_ROWS: u32 = 50;

/// The half-life of the recency decay of [`topic_weight`] (decision 78
/// (c)): a topic whose last activity was 7 days ago weighs half of one
/// active right now.
pub const RECENCY_HALF_LIFE_DAYS: f64 = 7.0;

/// The documented default age of a topic with `last_activity_at ==
/// None` (decision 78 (c)): it decays as if last active 30 days ago. A
/// zero-edge topic has weight 0 anyway through the edge-count factor.
pub const STALE_TOPIC_DAYS: f64 = 30.0;

/// The day-iteration bound of [`schedule_next`]: the host-local day of
/// `now` through that many days ahead. A pathological backoff
/// multiplier can push every window of the range below the lower bound;
/// the caller retries on a later tick.
pub const MAX_SCHEDULE_DAYS_AHEAD: u32 = 30;

/// The local-date format "YYYY-MM-DD" of [`local_date_string`] and the
/// persisted cooldown dates (decision 78 (c)).
const DATE_FORMAT: &[FormatItem<'_>] = format_description!("[year]-[month]-[day]");

/// The effective daily quota under the Section 8.5 soft backoff:
/// max(0, `warmup_quota` − `backoff_factor`). Zero means no warmup
/// until a successful engagement resets the factor.
pub fn effective_quota(warmup_quota: u32, backoff_factor: u32) -> u32 {
    warmup_quota.saturating_sub(backoff_factor)
}

/// The Section 8.5 interval multiplier 2^`backoff_factor`. Saturates
/// at `u64::MAX` for a factor of 64 or more (a stale factor grown by a
/// long-ignored group never overflows the scheduling math).
pub fn interval_multiplier(backoff_factor: u32) -> u64 {
    1u64.checked_shl(backoff_factor).unwrap_or(u64::MAX)
}

/// The host-local "YYYY-MM-DD" of `now` under `offset`. The formatting
/// fallback mirrors the codebase's `unwrap_or_else` style (context.rs
/// `hhmm_of`); a valid date under a valid offset never fails.
pub fn local_date_string(now: OffsetDateTime, offset: UtcOffset) -> String {
    now.to_offset(offset)
        .format(DATE_FORMAT)
        .unwrap_or_else(|_| "????-??-??".to_string())
}

/// The UTC instants of the active-hours window of one host-local day:
/// `date` 00:00 host-local + start/end minutes (the pair, start
/// inclusive / end exclusive). `PrimitiveDateTime::new(date,
/// Time::MIDNIGHT).assume_offset(offset)` per host-local minute.
pub fn day_active_window(
    date: Date,
    hours: ActiveHours,
    offset: UtcOffset,
) -> (OffsetDateTime, OffsetDateTime) {
    let midnight = PrimitiveDateTime::new(date, Time::MIDNIGHT).assume_offset(offset);
    (
        midnight + TimeDuration::minutes(i64::from(hours.start_minutes)),
        midnight + TimeDuration::minutes(i64::from(hours.end_minutes)),
    )
}

/// The P1 scheduling function (specs.md Sections 8.4/8.5): the next
/// `warmup_next_at` after `prev`, or `None`.
///
/// Method (the landed, documented slot method): the active-hours
/// window of one host-local day is divided into
/// `effective_quota(quota, factor)` equal windows and ONE uniform
/// random point is drawn per window (the "divide into quota-sized
/// windows, one random point per window" method). Only ONE point is
/// ever committed — the returned `warmup_next_at` persists (Rule P1:
/// a restart never reshuffles); unchosen candidate points evaporate.
///
/// `None` when the effective quota is 0 (the Section 8.5 backoff maxed
/// out — no warmup until the factor resets).
///
/// Lower bound: the point must be ≥ `now`, and — when `prev` is `Some`
/// (the slot being replaced, i.e. the slot that just fired or was
/// gates-skipped) — ≥ `prev + min_gap` where `min_gap = window_secs ×
/// interval_multiplier(factor)` (Section 8.5: the backoff spaces
/// activations out by 2^factor). `window_secs` is the length of ONE
/// quota-sized window (the span divided by the effective quota). With
/// factor 0 the min gap equals the window length, which the
/// one-point-per-window construction already satisfies — the
/// multiplier only bites under backoff.
///
/// Day iteration: today (the host-local day of `now` under `offset`)
/// through today + [`MAX_SCHEDULE_DAYS_AHEAD`]; the first day
/// contributing a qualifying point wins. Draw points lazily per day (a
/// day whose entire active window lies below the lower bound consumes
/// NO rng draws). Saturating `time::Duration` math everywhere (window
/// × multiplier must never overflow).
///
/// `None` when no day qualifies within the bound (a pathological
/// multiplier); the caller retries on a later tick.
pub fn schedule_next(
    config: &TriggerConfig,
    backoff_factor: u32,
    prev: Option<OffsetDateTime>,
    now: OffsetDateTime,
    offset: UtcOffset,
    rng: &mut impl Rng,
) -> Option<OffsetDateTime> {
    let quota = effective_quota(config.warmup_quota, backoff_factor);
    if quota == 0 {
        return None;
    }
    let span_secs = config.warmup_active_hours.span_seconds();
    // One quota-sized window; a zero-length span (a configuration the
    // ActiveHours parse rejects) yields no schedule rather than a
    // random_range panic (the pattern of trigger.rs).
    let window_secs = span_secs / u64::from(quota);
    if window_secs == 0 {
        return None;
    }
    let mut lower_bound = now;
    if let Some(prev) = prev {
        let min_gap = TimeDuration::seconds(
            window_secs
                .saturating_mul(interval_multiplier(backoff_factor))
                .min(i64::MAX as u64) as i64,
        );
        lower_bound = lower_bound.max(prev.saturating_add(min_gap));
    }
    let today = now.to_offset(offset).date();
    for day_index in 0..=MAX_SCHEDULE_DAYS_AHEAD {
        let Some(date) = today.checked_add(TimeDuration::days(i64::from(day_index))) else {
            break;
        };
        let (day_start, day_end) = day_active_window(date, config.warmup_active_hours, offset);
        if day_end <= lower_bound {
            // The whole day lies below the lower bound: no rng draws.
            continue;
        }
        if day_end <= lower_bound {
            // The whole day lies below the lower bound: no rng draws.
            continue;
        }
        // The first qualifying point of the first qualifying day wins;
        // the unchosen candidate points of the day evaporate.
        for point in day_slots(day_start, window_secs, quota, rng) {
            if point >= lower_bound {
                return Some(point);
            }
        }
    }
    None
}

/// One uniform random point per quota-sized window of one day (the
/// "divide into quota-sized windows, one random point per window"
/// method of [`schedule_next`]). Saturating second math; the caller
/// guarantees `window_secs > 0`.
fn day_slots(
    day_start: OffsetDateTime,
    window_secs: u64,
    quota: u32,
    rng: &mut impl Rng,
) -> Vec<OffsetDateTime> {
    (0..u64::from(quota))
        .map(|slot| {
            let offset_secs = slot
                .saturating_mul(window_secs)
                .saturating_add(rng.random_range(0..window_secs))
                .min(i64::MAX as u64) as i64;
            day_start.saturating_add(TimeDuration::seconds(offset_secs))
        })
        .collect()
}

/// weight = edge_count × 0.5^(days_since_activity / RECENCY_HALF_LIFE_DAYS)
/// (decision 78 (c): edge count × recency decay; the documented formula).
/// `None` activity decays as [`STALE_TOPIC_DAYS`]. A future timestamp
/// (clock skew) decays as 0 days, never negative.
pub fn topic_weight(
    edge_count: u64,
    last_activity_at: Option<OffsetDateTime>,
    now: OffsetDateTime,
) -> f64 {
    let days = match last_activity_at {
        Some(at) => (now - at).whole_seconds().max(0) as f64 / 86_400.0,
        None => STALE_TOPIC_DAYS,
    };
    edge_count as f64 * 0.5_f64.powf(days / RECENCY_HALF_LIFE_DAYS)
}

/// The cooldown-age check of decision 78 (c): the stored date is FEWER
/// than `cooldown_days` whole days before `today_local`. `None` when
/// either date is malformed (a malformed stored date does NOT exclude;
/// the state self-heals at the next write and [`prune_cooldowns`]
/// drops it).
fn cooldown_active(stored: &str, today: Date, cooldown_days: u32) -> bool {
    match Date::parse(stored, DATE_FORMAT) {
        Ok(stored) => (today - stored).whole_days() < i64::from(cooldown_days),
        Err(_) => false,
    }
}

/// specs.md Section 9.7 step 2: the weighted pick over the sampled
/// candidates, minus the exclusions. `tail_texts` are the texts of the
/// [`TAIL_EXCLUSION_ROWS`] newest raw-log rows (any direction).
///
/// Exclusion 1 (cooldown, decision 78 (c)): the candidate's NORMALIZED
/// name ([`normalize`]) is a key of `cooldowns` whose stored date is
/// FEWER than `cooldown_days` whole days before `today_local`.
///
/// Exclusion 2 (tail): the candidate's normalized name appears as a
/// SUBSTRING of any normalized tail text (the landed reading of
/// "appears in the 50-row raw-log tail"; an empty normalized name
/// never matches).
///
/// The pick is weighted-uniform over the survivors
/// (`rng.random_range(0.0..total)`, cumulative scan; the f64 total is
/// finite by construction). All-survivors-weight-zero or empty
/// survivors → `None` (a group with no eligible topic stays silent —
/// forced small talk is worse than silence, Section 9.7 step 2).
pub fn pick_topic(
    candidates: &[TopicCandidate],
    cooldowns: &HashMap<String, String>,
    tail_texts: &[String],
    today_local: &str,
    cooldown_days: u32,
    now: OffsetDateTime,
    rng: &mut impl Rng,
) -> Option<TopicCandidate> {
    let today = Date::parse(today_local, DATE_FORMAT).ok();
    let normalized_tails: Vec<String> = tail_texts.iter().map(|text| normalize(text)).collect();
    let mut weighted: Vec<(&TopicCandidate, f64)> = Vec::new();
    for candidate in candidates {
        let name = normalize(&candidate.name);
        if let (Some(today), Some(stored)) = (today, cooldowns.get(name.as_str())) {
            if cooldown_active(stored, today, cooldown_days) {
                continue;
            }
        }
        if !name.is_empty()
            && normalized_tails
                .iter()
                .any(|tail| tail.contains(name.as_str()))
        {
            continue;
        }
        weighted.push((
            candidate,
            topic_weight(candidate.edge_count, candidate.last_activity_at, now),
        ));
    }
    let total: f64 = weighted.iter().map(|(_, weight)| weight).sum();
    // The weights are finite and non-negative by construction.
    if total <= 0.0 {
        return None;
    }
    let mut draw = rng.random_range(0.0..total);
    for (candidate, weight) in &weighted {
        draw -= weight;
        if draw < 0.0 {
            return Some((*candidate).clone());
        }
    }
    // f64 rounding can leave the draw at or above the last cumulative
    // weight; the last survivor takes the remainder.
    weighted.last().map(|(candidate, _)| (*candidate).clone())
}

/// Prunes cooldown entries older than `cooldown_days` (and malformed
/// dates) from the map; the actor persists the pruned map. Keeps the
/// persisted JSON bounded over years of operation (derived hygiene).
/// An entry exactly `cooldown_days` old drops, in step with the
/// exclusion rule (it no longer excludes). A malformed `today_local`
/// (a path the `local_date_string` fallback alone can produce) prunes
/// everything — the state self-heals at the next write.
pub fn prune_cooldowns(
    cooldowns: &mut HashMap<String, String>,
    today_local: &str,
    cooldown_days: u32,
) {
    let today = Date::parse(today_local, DATE_FORMAT).ok();
    cooldowns.retain(|_, stored| match today {
        Some(today) => match Date::parse(stored, DATE_FORMAT) {
            Ok(stored) => (today - stored).whole_days() < i64::from(cooldown_days),
            Err(_) => false,
        },
        None => false,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use time::macros::datetime;

    fn hours(text: &str) -> ActiveHours {
        ActiveHours::parse(text).expect("the test window parses")
    }

    fn config(quota: u32, active_hours: &str) -> TriggerConfig {
        TriggerConfig {
            warmup_quota: quota,
            warmup_active_hours: hours(active_hours),
            ..TriggerConfig::default()
        }
    }

    fn candidate(
        name: &str,
        edge_count: u64,
        last_activity_at: Option<OffsetDateTime>,
    ) -> TopicCandidate {
        TopicCandidate {
            node_id: format!("id-{name}"),
            name: name.to_string(),
            edge_count,
            last_activity_at,
        }
    }

    /// True when `point` lands inside the active window of the
    /// host-local day `days_ahead` after `base_date` under `offset`.
    fn in_window_of_day(
        point: OffsetDateTime,
        base_date: Date,
        days_ahead: i64,
        active_hours: ActiveHours,
        offset: UtcOffset,
    ) -> bool {
        let date = base_date + TimeDuration::days(days_ahead);
        let (start, end) = day_active_window(date, active_hours, offset);
        point >= start && point < end
    }

    #[test]
    fn the_warmup_generator_trait_is_object_safe() {
        // The actor holds Arc<dyn WarmupGenerator> (WarmupServices).
        fn assert_object_safe(_: Option<Arc<dyn WarmupGenerator>>) {}
        assert_object_safe(None);
    }

    #[test]
    fn effective_quota_saturates_at_zero() {
        // specs.md Section 8.5: max(0, quota − factor).
        assert_eq!(effective_quota(3, 0), 3);
        assert_eq!(effective_quota(3, 1), 2);
        assert_eq!(effective_quota(1, 1), 0);
        assert_eq!(effective_quota(2, 10), 0);
    }

    #[test]
    fn interval_multiplier_saturates_at_u64_max() {
        assert_eq!(interval_multiplier(0), 1);
        assert_eq!(interval_multiplier(1), 2);
        assert_eq!(interval_multiplier(5), 32);
        assert_eq!(interval_multiplier(63), 1u64 << 63);
        assert_eq!(interval_multiplier(64), u64::MAX);
        assert_eq!(interval_multiplier(u32::MAX), u64::MAX);
    }

    #[test]
    fn local_date_string_follows_the_offset() {
        // 2026-08-21 20:00 UTC is 2026-08-22 04:00 at UTC+8 and
        // 2026-08-21 12:00 at UTC-8.
        let now = datetime!(2026-08-21 20:00 UTC);
        assert_eq!(local_date_string(now, UtcOffset::UTC), "2026-08-21");
        assert_eq!(
            local_date_string(now, UtcOffset::from_hms(8, 0, 0).unwrap()),
            "2026-08-22"
        );
        assert_eq!(
            local_date_string(now, UtcOffset::from_hms(-8, 0, 0).unwrap()),
            "2026-08-21"
        );
    }

    #[test]
    fn day_active_window_converts_the_host_local_minutes() {
        // "08:00-23:00" at UTC+8 is 00:00-15:00 UTC of the same civil
        // date.
        let plus_eight = UtcOffset::from_hms(8, 0, 0).unwrap();
        let date = datetime!(2026-08-22 00:00 UTC).date();
        let (start, end) = day_active_window(date, hours("08:00-23:00"), plus_eight);
        assert_eq!(start, datetime!(2026-08-22 00:00 UTC));
        assert_eq!(end, datetime!(2026-08-22 15:00 UTC));
        // UTC-8: the same civil window is 16:00 UTC of the civil date
        // through 07:00 UTC of the next day.
        let minus_eight = UtcOffset::from_hms(-8, 0, 0).unwrap();
        let (start, end) = day_active_window(date, hours("08:00-23:00"), minus_eight);
        assert_eq!(start, datetime!(2026-08-22 16:00 UTC));
        assert_eq!(end, datetime!(2026-08-23 07:00 UTC));
    }

    #[test]
    fn schedule_next_returns_none_when_the_backoff_zeroes_the_quota() {
        // Section 8.5: quota 1 with factor 1 has effective quota 0.
        let config = config(1, "08:00-23:00");
        let mut rng = StdRng::seed_from_u64(1);
        let scheduled = schedule_next(
            &config,
            1,
            None,
            datetime!(2026-08-21 10:00 UTC),
            UtcOffset::UTC,
            &mut rng,
        );
        assert_eq!(scheduled, None);
    }

    #[test]
    fn schedule_next_points_stay_in_a_window_and_respect_the_lower_bound() {
        // Property test over many seeds: quota 1, a small active
        // window, `now` inside it. The single window is the whole
        // span, so the drawn point may lie below `now`; the result is
        // then None today and the next day's point. Every returned
        // point lies inside SOME day's active window and ≥ now.
        let config = config(1, "12:00-13:00");
        let now = datetime!(2026-08-21 12:30 UTC);
        let base_date = now.date();
        for seed in 0..100 {
            let mut rng = StdRng::seed_from_u64(seed);
            let Some(point) = schedule_next(&config, 0, None, now, UtcOffset::UTC, &mut rng) else {
                panic!("a 30-day iteration always finds a window (seed {seed})");
            };
            assert!(point >= now, "seed {seed}: the point respects now");
            assert!(
                (0..=i64::from(MAX_SCHEDULE_DAYS_AHEAD)).any(|day| in_window_of_day(
                    point,
                    base_date,
                    day,
                    config.warmup_active_hours,
                    UtcOffset::UTC
                )),
                "seed {seed}: the point lies inside some day's active window"
            );
        }
    }

    #[test]
    fn the_day_slots_land_one_per_quota_window() {
        // Quota 3 over "08:00-20:00": the 12 h span divides into three
        // 4 h windows and each drawn point lands inside its own third.
        let window_secs = hours("08:00-20:00").span_seconds() / 3;
        assert_eq!(window_secs, 4 * 60 * 60);
        let day_start = datetime!(2026-08-22 08:00 UTC);
        for seed in 0..100 {
            let mut rng = StdRng::seed_from_u64(seed);
            let slots = day_slots(day_start, window_secs, 3, &mut rng);
            assert_eq!(slots.len(), 3);
            for (index, slot) in slots.iter().enumerate() {
                let window_start = day_start + TimeDuration::hours(4 * index as i64);
                let window_end = window_start + TimeDuration::hours(4);
                assert!(
                    *slot >= window_start && *slot < window_end,
                    "seed {seed}: slot {index} ({slot}) lies in [{window_start}, {window_end})"
                );
            }
        }
    }

    #[test]
    fn schedule_next_spaces_points_by_the_backoff_multiplier() {
        // Factor 1 (Section 8.5): the min gap is 2 × the window
        // length. Quota 2 with factor 1 has effective quota 1, so
        // "00:00-23:59" is one window of 86340 s and the next point
        // sits ≥ prev + 2 × 86340 s.
        let config = config(2, "00:00-23:59");
        let prev = datetime!(2026-08-21 12:00 UTC);
        let window_secs = config.warmup_active_hours.span_seconds();
        for seed in 0..20 {
            let mut rng = StdRng::seed_from_u64(seed);
            let point = schedule_next(&config, 1, Some(prev), prev, UtcOffset::UTC, &mut rng)
                .expect("the 30-day iteration finds a window");
            assert!(
                point >= prev + TimeDuration::seconds(2 * window_secs as i64),
                "seed {seed}: the point respects prev + 2 windows"
            );
        }
    }

    #[test]
    fn schedule_next_pushes_to_the_next_day_when_the_window_passed() {
        // Now is past today's "08:00-09:00" window: the point lands in
        // tomorrow's window.
        let config = config(1, "08:00-09:00");
        let now = datetime!(2026-08-21 10:00 UTC);
        let mut rng = StdRng::seed_from_u64(3);
        let point = schedule_next(&config, 0, None, now, UtcOffset::UTC, &mut rng)
            .expect("tomorrow qualifies");
        let (start, end) = day_active_window(
            datetime!(2026-08-22 00:00 UTC).date(),
            config.warmup_active_hours,
            UtcOffset::UTC,
        );
        assert!(point >= start && point < end);
    }

    #[test]
    fn schedule_next_is_deterministic_per_seed() {
        // Rule P1 encoded as a test: same seed + same inputs → the
        // same committed point. A restart that re-derives from the
        // same persisted state never reshuffles.
        let config = config(3, "08:00-23:00");
        let now = datetime!(2026-08-21 06:00 UTC);
        let mut first = StdRng::seed_from_u64(42);
        let mut second = StdRng::seed_from_u64(42);
        assert_eq!(
            schedule_next(&config, 0, None, now, UtcOffset::UTC, &mut first),
            schedule_next(&config, 0, None, now, UtcOffset::UTC, &mut second)
        );
    }

    #[test]
    fn schedule_next_draws_nothing_for_a_day_below_the_lower_bound() {
        // A huge backoff factor pushes the lower bound past every day
        // of the range: no point qualifies and None comes back (the
        // caller retries on a later tick) — and the rng state never
        // moves, proving the lazy per-day draws.
        let config = config(1, "08:00-09:00");
        let prev = datetime!(2026-08-21 08:30 UTC);
        let mut rng = StdRng::seed_from_u64(9);
        let scheduled = schedule_next(&config, 40, Some(prev), prev, UtcOffset::UTC, &mut rng);
        assert_eq!(scheduled, None);
        let mut fresh = StdRng::seed_from_u64(9);
        assert_eq!(rng.random_range(0..1000u32), fresh.random_range(0..1000u32));
    }

    #[test]
    fn topic_weight_decays_by_the_documented_formula() {
        let now = datetime!(2026-08-21 12:00 UTC);
        // A zero-edge topic weighs 0 (decision 78 (c): the edge-count
        // factor).
        assert_eq!(topic_weight(0, Some(now), now), 0.0);
        // Active right now: the full edge count.
        assert_eq!(topic_weight(10, Some(now), now), 10.0);
        // One half-life (7 days): exactly half.
        let half_life_ago = now - TimeDuration::days(7);
        assert_eq!(topic_weight(10, Some(half_life_ago), now), 5.0);
        // None activity decays as 30 days (the documented default).
        let expected = 10.0 * 0.5_f64.powf(STALE_TOPIC_DAYS / RECENCY_HALF_LIFE_DAYS);
        assert_eq!(topic_weight(10, None, now), expected);
        // Clock skew: a future timestamp decays as 0 days, never negative.
        let future = now + TimeDuration::days(2);
        assert_eq!(topic_weight(10, Some(future), now), 10.0);
    }

    fn cooldown_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn pick_topic_excludes_a_topic_inside_its_cooldown() {
        // Today is 2026-08-21; cooldown 3 days. Stored 2026-08-19 is 2
        // days before → excluded; stored 2026-08-18 is exactly 3 days
        // before → the cooldown expired.
        let now = datetime!(2026-08-21 12:00 UTC);
        let cooling = candidate("Rust", 10, Some(now));
        let expired = candidate("Go", 1, Some(now));
        let candidates = vec![cooling.clone(), expired.clone()];
        let mut rng = StdRng::seed_from_u64(1);
        let cooldowns = cooldown_map(&[("rust", "2026-08-19")]);
        for _ in 0..100 {
            let picked = pick_topic(&candidates, &cooldowns, &[], "2026-08-21", 3, now, &mut rng)
                .expect("go survives");
            assert_eq!(picked.name, "Go");
        }
        // Exactly at the boundary the topic picks again.
        let cooldowns = cooldown_map(&[("rust", "2026-08-18")]);
        let mut saw_rust = false;
        for _ in 0..100 {
            if pick_topic(&candidates, &cooldowns, &[], "2026-08-21", 3, now, &mut rng)
                .expect("a survivor")
                .name
                == "Rust"
            {
                saw_rust = true;
                break;
            }
        }
        assert!(saw_rust, "the expired cooldown no longer excludes");
    }

    #[test]
    fn pick_topic_ignores_a_malformed_cooldown_date() {
        // A malformed stored date does NOT exclude (the state
        // self-heals at the next write; prune_cooldowns drops it).
        let now = datetime!(2026-08-21 12:00 UTC);
        let only = candidate("Rust", 10, Some(now));
        let cooldowns = cooldown_map(&[("rust", "not-a-date")]);
        let mut rng = StdRng::seed_from_u64(1);
        let picked = pick_topic(&[only], &cooldowns, &[], "2026-08-21", 3, now, &mut rng)
            .expect("the malformed date does not exclude the only candidate");
        assert_eq!(picked.name, "Rust");
    }

    #[test]
    fn pick_topic_excludes_a_name_in_the_normalized_tail() {
        // The substring rule (the landed reading of Section 9.7 step
        // 2), normalized on both sides: case and whitespace fold.
        let now = datetime!(2026-08-21 12:00 UTC);
        let graph = candidate("Graph Database", 10, Some(now));
        let quiet = candidate("Hiking", 1, Some(now));
        let tail = vec!["we discussed GRAPH   DATABASE all evening".to_string()];
        let mut rng = StdRng::seed_from_u64(2);
        for _ in 0..100 {
            let picked = pick_topic(
                &[graph.clone(), quiet.clone()],
                &HashMap::new(),
                &tail,
                "2026-08-21",
                3,
                now,
                &mut rng,
            )
            .expect("hiking survives");
            assert_eq!(picked.name, "Hiking");
        }
    }

    #[test]
    fn pick_topic_tail_matching_folds_nfkc_and_never_matches_an_empty_name() {
        let now = datetime!(2026-08-21 12:00 UTC);
        // Full-width NFKC folding: "Ｔａｍａｋｏ" matches the tail's "tamako".
        let full_width = candidate("Ｔａｍａｋｏ", 10, Some(now));
        let tail = vec!["tamako news".to_string()];
        let mut rng = StdRng::seed_from_u64(3);
        assert_eq!(
            pick_topic(
                &[full_width],
                &HashMap::new(),
                &tail,
                "2026-08-21",
                3,
                now,
                &mut rng
            ),
            None
        );
        // An empty normalized name never matches any tail.
        let nameless = candidate("   ", 10, Some(now));
        let picked = pick_topic(
            &[nameless],
            &HashMap::new(),
            &["anything at all".to_string()],
            "2026-08-21",
            3,
            now,
            &mut rng,
        )
        .expect("the empty name is not tail-excluded");
        assert_eq!(picked.name, "   ");
    }

    #[test]
    fn pick_topic_stays_silent_when_every_survivor_has_zero_weight() {
        // Section 9.7 step 2: forced small talk is worse than silence.
        let now = datetime!(2026-08-21 12:00 UTC);
        let zero = candidate("Dust", 0, Some(now));
        let mut rng = StdRng::seed_from_u64(4);
        assert_eq!(
            pick_topic(
                &[zero],
                &HashMap::new(),
                &[],
                "2026-08-21",
                3,
                now,
                &mut rng
            ),
            None
        );
        assert_eq!(
            pick_topic(&[], &HashMap::new(), &[], "2026-08-21", 3, now, &mut rng),
            None
        );
    }

    #[test]
    fn pick_topic_always_picks_a_single_survivor() {
        let now = datetime!(2026-08-21 12:00 UTC);
        let only = candidate("Rust", 4, Some(now));
        let mut rng = StdRng::seed_from_u64(5);
        for _ in 0..100 {
            let picked = pick_topic(
                std::slice::from_ref(&only),
                &HashMap::new(),
                &[],
                "2026-08-21",
                3,
                now,
                &mut rng,
            );
            assert_eq!(picked, Some(only.clone()));
        }
    }

    #[test]
    fn pick_topic_never_picks_an_excluded_topic_over_many_draws() {
        // A heavyweight excluded candidate and a featherweight
        // survivor: 1000 seeded draws never return the excluded one.
        let now = datetime!(2026-08-21 12:00 UTC);
        let heavy = candidate("Politics", 10_000, Some(now));
        let light = candidate("Tea", 1, Some(now));
        let cooldowns = cooldown_map(&[("politics", "2026-08-21")]);
        let tail = vec!["enough about politics".to_string()];
        let empty_cooldowns = HashMap::new();
        let empty_tail: Vec<String> = Vec::new();
        let mut rng = StdRng::seed_from_u64(6);
        for draw in 0..1000 {
            let (cooldowns, tail) = if draw % 2 == 0 {
                // Excluded by the cooldown half of the draws…
                (&cooldowns, &empty_tail)
            } else {
                // …and by the raw-log tail the other half.
                (&empty_cooldowns, &tail)
            };
            let picked = pick_topic(
                &[heavy.clone(), light.clone()],
                cooldowns,
                tail,
                "2026-08-21",
                3,
                now,
                &mut rng,
            )
            .expect("tea always survives");
            assert_eq!(picked.name, "Tea", "draw {draw}");
        }
    }

    #[test]
    fn prune_cooldowns_drops_old_and_malformed_entries() {
        let mut cooldowns = cooldown_map(&[
            ("fresh", "2026-08-20"),
            ("boundary", "2026-08-18"),
            ("stale", "2020-01-01"),
            ("malformed", "not-a-date"),
        ]);
        prune_cooldowns(&mut cooldowns, "2026-08-21", 3);
        // Fresh (1 day before) stays; exactly 3 days before drops in
        // step with the exclusion rule; stale and malformed drop.
        assert_eq!(cooldowns, cooldown_map(&[("fresh", "2026-08-20")]));
    }
}
