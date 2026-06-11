// SQLMesh-style interval tracking for duckOrch.
//
// Core concepts (mirroring sqlmesh/core/snapshot/definition.py):
//
//   Interval     = (start_ts, end_ts) in epoch seconds, half-open [start, end)
//   IntervalUnit = granularity: Daily | Hourly | Minutes(N)
//
// The three key functions:
//
//   expand_range(start, end, unit)     → all interval boundary timestamps
//   merge_intervals(vec)               → minimal covering set (sorted, non-overlapping)
//   compute_missing(unit, stored, ...) → gaps between stored and expected
//
// Unlike the old `on_cron` / `on_interval(secs)` approach that compares a
// single `own_last_materialized_at` timestamp against now, this module tracks
// *which intervals have been processed*, enabling:
//   - Correct backfill (multiple missed days processed in order)
//   - No duplicate-trigger risk (interval row = consumed)
//   - Natural idempotency (insert interval row → already covered)

use serde::{Deserialize, Serialize};

/// Half-open time interval [start_ts, end_ts) in epoch seconds.
pub type Interval = (i64, i64);

// ---------------------------------------------------------------------------
// IntervalUnit
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntervalUnit {
    /// UTC calendar-day boundaries. step = 86 400 s.
    Daily,
    /// UTC hour boundaries. step = 3 600 s.
    Hourly,
    /// Fixed-width N-minute windows aligned to epoch. step = N * 60 s.
    Minutes(u32),
}

impl IntervalUnit {
    /// Parse from human string.  Accepted forms:
    ///   "daily" | "1d"
    ///   "hourly" | "1h"
    ///   "Nmin" | "Nm" | "Nh" | "Nd"  (N > 0)
    pub fn from_str(s: &str) -> Option<Self> {
        let lower = s.trim().to_ascii_lowercase();
        match lower.as_str() {
            "daily" | "1d" => return Some(IntervalUnit::Daily),
            "hourly" | "1h" => return Some(IntervalUnit::Hourly),
            _ => {}
        }
        // Try "Nunit" form.
        let split = lower
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(lower.len());
        let num: u32 = lower[..split].parse().ok()?;
        if num == 0 {
            return None;
        }
        let unit = lower[split..].trim();
        match unit {
            "d" | "day" | "days" => {
                if num == 1 {
                    Some(IntervalUnit::Daily)
                } else {
                    // N-day windows: represent as Minutes(N * 1440).
                    Some(IntervalUnit::Minutes(num * 1440))
                }
            }
            "h" | "hour" | "hours" => {
                if num == 1 {
                    Some(IntervalUnit::Hourly)
                } else {
                    Some(IntervalUnit::Minutes(num * 60))
                }
            }
            "m" | "min" | "mins" | "minute" | "minutes" => Some(IntervalUnit::Minutes(num)),
            _ => None,
        }
    }

    /// Canonical serialisation for DSL round-trip.
    pub fn to_dsl_str(self) -> String {
        match self {
            IntervalUnit::Daily => "daily".into(),
            IntervalUnit::Hourly => "hourly".into(),
            IntervalUnit::Minutes(n) if n % 1440 == 0 => format!("{}d", n / 1440),
            IntervalUnit::Minutes(n) if n % 60 == 0 => format!("{}h", n / 60),
            IntervalUnit::Minutes(n) => format!("{}min", n),
        }
    }

    /// Width of one interval in seconds.
    pub fn step_secs(self) -> i64 {
        match self {
            IntervalUnit::Daily => 86_400,
            IntervalUnit::Hourly => 3_600,
            IntervalUnit::Minutes(n) => (n as i64) * 60,
        }
    }

    /// Snap `ts` down to the start of the interval that contains it.
    pub fn floor(self, ts: i64) -> i64 {
        let step = self.step_secs();
        // Integer floor division (handles negative timestamps correctly).
        ts - ts.rem_euclid(step)
    }
}

// ---------------------------------------------------------------------------
// parse_interval_start
// ---------------------------------------------------------------------------

/// Parse an `@interval_start` header value into epoch seconds (UTC).
/// Accepts `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, ISO `T` separator, or a raw
/// epoch integer.
pub fn parse_interval_start(s: &str) -> Option<i64> {
    let v = s.trim();
    if v.is_empty() {
        return None;
    }
    if let Ok(epoch) = v.parse::<i64>() {
        return Some(epoch);
    }
    if let Ok(d) = chrono::NaiveDate::parse_from_str(v, "%Y-%m-%d") {
        return Some(d.and_hms_opt(0, 0, 0)?.and_utc().timestamp());
    }
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"] {
        if let Ok(t) = chrono::NaiveDateTime::parse_from_str(v, fmt) {
            return Some(t.and_utc().timestamp());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// expand_range
// ---------------------------------------------------------------------------

/// Generate all interval-boundary timestamps in [start_ts, end_ts).
///
/// Returns a sorted list `[t0, t1, t2, ...]` where each consecutive pair
/// `(ti, ti+1)` represents one interval.  The first element is the
/// interval-floor of `start_ts`; the last is the last boundary <= `end_ts`.
///
/// Mirrors SQLMesh's `expand_range` in snapshot/definition.py.
pub fn expand_range(start_ts: i64, end_ts: i64, unit: IntervalUnit) -> Vec<i64> {
    if start_ts >= end_ts {
        return vec![];
    }
    let step = unit.step_secs();
    let t0 = unit.floor(start_ts);
    let mut timestamps = Vec::new();
    let mut t = t0;
    while t < end_ts {
        timestamps.push(t);
        t += step;
    }
    // Append exclusive end so callers can zip(ts, ts[1..]). Clip at end_ts
    // (mirrors SQLMesh): an unaligned end produces a *partial* final
    // interval rather than a full-width one, so allow_partials callers never
    // record time they have not actually processed.
    timestamps.push(t.min(end_ts));
    timestamps
}

// ---------------------------------------------------------------------------
// merge_intervals
// ---------------------------------------------------------------------------

/// Merge overlapping or contiguous intervals into the minimal covering set.
///
/// Mirrors SQLMesh's `merge_intervals` in snapshot/definition.py.
pub fn merge_intervals(mut intervals: Vec<Interval>) -> Vec<Interval> {
    if intervals.is_empty() {
        return vec![];
    }
    intervals.sort_by_key(|&(s, _)| s);
    let mut merged: Vec<Interval> = Vec::with_capacity(intervals.len());
    for (s, e) in intervals {
        match merged.last_mut() {
            Some(last) if s <= last.1 => {
                // Overlapping or touching: extend end.
                last.1 = last.1.max(e);
            }
            _ => merged.push((s, e)),
        }
    }
    merged
}

// ---------------------------------------------------------------------------
// compute_missing
// ---------------------------------------------------------------------------

/// Return all intervals in [start_ts, end_ts) not covered by `stored`.
///
/// `stored` must already be merged (non-overlapping, sorted).  Pass it
/// through `merge_intervals` first if you are not sure.
///
/// `lookback`: re-process trailing intervals near a gap — interval `i` is
/// also considered missing when interval `i + lookback` is missing. Handles
/// late-arriving data (mirrors SQLMesh's model `lookback`). `0` disables.
///
/// Mirrors SQLMesh's `compute_missing_intervals` in snapshot/definition.py.
pub fn compute_missing(
    unit: IntervalUnit,
    stored: &[Interval],
    start_ts: i64,
    end_ts: i64,
    lookback: u32,
) -> Vec<Interval> {
    let boundaries = expand_range(start_ts, end_ts, unit);
    if boundaries.len() < 2 {
        return vec![];
    }
    let mut missing: Vec<Interval> = Vec::new();
    'outer: for (&cur, &nxt) in boundaries.iter().zip(boundaries[1..].iter()) {
        for &(low, high) in stored {
            if cur >= low && nxt <= high {
                // Fully covered.
                continue 'outer;
            }
            if low >= nxt {
                // Stored intervals are sorted; no point scanning further.
                break;
            }
        }
        missing.push((cur, nxt));
    }
    if !missing.is_empty() && lookback > 0 {
        // SQLMesh semantics: interval i is missing if interval i+lookback is
        // missing. Walk boundary pairs; when the pair `lookback` steps ahead
        // is in the missing set, add this one too.
        let lb = lookback as usize;
        let missing_set: std::collections::BTreeSet<Interval> = missing.iter().copied().collect();
        let mut extended = missing_set.clone();
        for (i, (&cur, &nxt)) in boundaries.iter().zip(boundaries[1..].iter()).enumerate() {
            let parent = boundaries.get(i + lb).zip(boundaries.get(i + lb + 1));
            match parent {
                // Parent beyond the range end, or parent itself missing →
                // this interval must be re-processed too.
                None => {
                    extended.insert((cur, nxt));
                }
                Some((&ps, &pe)) if missing_set.contains(&(ps, pe)) => {
                    extended.insert((cur, nxt));
                }
                _ => {}
            }
        }
        return extended.into_iter().collect();
    }
    missing
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------- IntervalUnit::from_str --------------------

    #[test]
    fn parses_daily() {
        assert_eq!(IntervalUnit::from_str("daily"), Some(IntervalUnit::Daily));
        assert_eq!(IntervalUnit::from_str("1d"), Some(IntervalUnit::Daily));
        assert_eq!(IntervalUnit::from_str("Daily"), Some(IntervalUnit::Daily));
    }

    #[test]
    fn parses_hourly() {
        assert_eq!(IntervalUnit::from_str("hourly"), Some(IntervalUnit::Hourly));
        assert_eq!(IntervalUnit::from_str("1h"), Some(IntervalUnit::Hourly));
    }

    #[test]
    fn parses_minutes() {
        assert_eq!(
            IntervalUnit::from_str("5min"),
            Some(IntervalUnit::Minutes(5))
        );
        assert_eq!(
            IntervalUnit::from_str("30m"),
            Some(IntervalUnit::Minutes(30))
        );
        assert_eq!(
            IntervalUnit::from_str("2h"),
            Some(IntervalUnit::Minutes(120))
        );
    }

    #[test]
    fn rejects_zero_and_garbage() {
        assert_eq!(IntervalUnit::from_str("0d"), None);
        assert_eq!(IntervalUnit::from_str(""), None);
        assert_eq!(IntervalUnit::from_str("foobar"), None);
    }

    // -------------------- floor --------------------

    #[test]
    fn floor_daily_at_midnight() {
        // 2026-06-10 00:00:00 UTC = 1749513600
        let midnight: i64 = 1_749_513_600;
        assert_eq!(IntervalUnit::Daily.floor(midnight), midnight);
        assert_eq!(IntervalUnit::Daily.floor(midnight + 3600), midnight);
        assert_eq!(IntervalUnit::Daily.floor(midnight + 86399), midnight);
    }

    #[test]
    fn floor_hourly() {
        let hour_start: i64 = 1_749_513_600; // already on hour
        assert_eq!(IntervalUnit::Hourly.floor(hour_start + 1800), hour_start);
    }

    // -------------------- expand_range --------------------

    #[test]
    fn expand_range_daily_three_days() {
        // 3 days starting at 2026-06-10 00:00 UTC
        let start: i64 = 1_749_513_600;
        let end = start + 3 * 86_400;
        let ts = expand_range(start, end, IntervalUnit::Daily);
        // expect [t0, t1, t2, t3] — 4 boundaries for 3 intervals
        assert_eq!(ts.len(), 4);
        assert_eq!(ts[0], start);
        assert_eq!(ts[1], start + 86_400);
        assert_eq!(ts[3], end);
    }

    #[test]
    fn expand_range_empty_when_start_ge_end() {
        assert!(expand_range(100, 100, IntervalUnit::Daily).is_empty());
        assert!(expand_range(200, 100, IntervalUnit::Daily).is_empty());
    }

    #[test]
    fn expand_range_minutes() {
        let start: i64 = 0;
        let end = 600; // 10 minutes
        let ts = expand_range(start, end, IntervalUnit::Minutes(5));
        assert_eq!(ts, vec![0, 300, 600]);
    }

    // -------------------- merge_intervals --------------------

    #[test]
    fn merge_disjoint() {
        let merged = merge_intervals(vec![(0, 10), (20, 30)]);
        assert_eq!(merged, vec![(0, 10), (20, 30)]);
    }

    #[test]
    fn merge_overlapping() {
        let merged = merge_intervals(vec![(0, 15), (10, 30)]);
        assert_eq!(merged, vec![(0, 30)]);
    }

    #[test]
    fn merge_contiguous() {
        let merged = merge_intervals(vec![(0, 10), (10, 20)]);
        assert_eq!(merged, vec![(0, 20)]);
    }

    #[test]
    fn merge_unsorted_input() {
        let merged = merge_intervals(vec![(20, 30), (0, 10), (10, 20)]);
        assert_eq!(merged, vec![(0, 30)]);
    }

    #[test]
    fn merge_empty() {
        assert!(merge_intervals(vec![]).is_empty());
    }

    // -------------------- compute_missing --------------------

    #[test]
    fn no_missing_when_fully_covered() {
        let start: i64 = 0;
        let end = 3 * 86_400;
        let stored = vec![(0, end)];
        let missing = compute_missing(IntervalUnit::Daily, &stored, start, end, 0);
        assert!(missing.is_empty(), "fully covered: {:?}", missing);
    }

    #[test]
    fn all_missing_when_nothing_stored() {
        let start: i64 = 1_781_049_600; // 2026-06-10
        let end = start + 3 * 86_400; // 2026-06-13
        let missing = compute_missing(IntervalUnit::Daily, &[], start, end, 0);
        assert_eq!(missing.len(), 3);
        assert_eq!(missing[0], (start, start + 86_400));
        assert_eq!(missing[2], (start + 2 * 86_400, end));
    }

    #[test]
    fn middle_day_missing() {
        let d0: i64 = 1_781_049_600;
        let d1 = d0 + 86_400;
        let d2 = d0 + 2 * 86_400;
        let d3 = d0 + 3 * 86_400;
        // Day 0 and day 2 stored; day 1 missing.
        let stored = merge_intervals(vec![(d0, d1), (d2, d3)]);
        let missing = compute_missing(IntervalUnit::Daily, &stored, d0, d3, 0);
        assert_eq!(missing, vec![(d1, d2)]);
    }

    #[test]
    fn no_missing_future_stored_ignored() {
        let start: i64 = 0;
        let end = 2 * 86_400;
        // Stored covers exactly [0, 2d) — no missing.
        let stored = vec![(0, 2 * 86_400)];
        let missing = compute_missing(IntervalUnit::Daily, &stored, start, end, 0);
        assert!(missing.is_empty());
    }

    // -------------------- lookback --------------------

    #[test]
    fn lookback_zero_is_noop() {
        let d0: i64 = 0;
        let stored = vec![(0, 86_400)];
        let missing = compute_missing(IntervalUnit::Daily, &stored, d0, 2 * 86_400, 0);
        assert_eq!(missing, vec![(86_400, 2 * 86_400)]);
    }

    #[test]
    fn lookback_pulls_in_preceding_interval() {
        // Days 0-2 stored, day 3 missing. lookback=1 → day 2 re-processed
        // because day 3 (its parent) is missing.
        let day = 86_400;
        let stored = vec![(0, 3 * day)];
        let missing = compute_missing(IntervalUnit::Daily, &stored, 0, 4 * day, 1);
        assert_eq!(missing, vec![(2 * day, 3 * day), (3 * day, 4 * day)]);
    }

    #[test]
    fn lookback_no_missing_means_no_recompute() {
        // Everything stored → lookback must not invent work.
        let day = 86_400;
        let stored = vec![(0, 4 * day)];
        let missing = compute_missing(IntervalUnit::Daily, &stored, 0, 4 * day, 2);
        assert!(missing.is_empty(), "{:?}", missing);
    }

    #[test]
    fn lookback_tail_intervals_added_when_gap_exists() {
        // Day 1 missing in the middle; lookback=1 also pulls in day 0
        // (parent day 1 missing) and the final day 3 (parent beyond range).
        let day = 86_400;
        let stored = merge_intervals(vec![(0, day), (2 * day, 4 * day)]);
        let missing = compute_missing(IntervalUnit::Daily, &stored, 0, 4 * day, 1);
        assert_eq!(
            missing,
            vec![(0, day), (day, 2 * day), (3 * day, 4 * day)],
            "day0 (parent=day1 missing), day1 (gap), day3 (parent beyond end)"
        );
    }

    // -------------------- dsl round-trip --------------------

    #[test]
    fn dsl_roundtrip() {
        for (input, expected) in [
            ("daily", "daily"),
            ("hourly", "hourly"),
            ("5min", "5min"),
            ("2h", "2h"),
            ("3d", "3d"),
        ] {
            let unit = IntervalUnit::from_str(input).unwrap();
            assert_eq!(unit.to_dsl_str(), expected, "input={}", input);
            // re-parse canonical form
            let unit2 = IntervalUnit::from_str(&unit.to_dsl_str()).unwrap();
            assert_eq!(unit, unit2);
        }
    }
}
