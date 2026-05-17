// Phase 14: calendar-style ASCII renderer for `duck-orch asset partitions`.
//
// Renders the registered + materialized state of a partitioned Asset. Pure
// function over (asset_name, partition_def, partition_rows) → String so that
// the CLI can hand it bytes from a JSON-mode SELECT and we can unit-test it
// in Rust without a DuckDB roundtrip.
//
// Status legend:
//   ✅ success     — last materialization succeeded
//   🟡 in_progress — registered + last status is in_progress
//   ❌ failed      — last materialization failed
//   ⚪ missing     — registered but never materialized
//
// For DailyPartition we render the last 4 weeks (or full range when smaller)
// as a 3-column grid. Static renders as a bulleted list. Multi is grouped
// by the first dimension and recurses (one bucket per outer key).

use orch_common::PartitionDef;
use std::collections::BTreeMap;

/// One partition's runtime state, fed in from the C++ pragma JSON output.
#[derive(Debug, Clone)]
pub struct PartitionStatus {
    pub key: String,
    /// Last known materialization status, or `None` if never materialized.
    /// Expected values: "success" | "failed" | "in_progress".
    pub status: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellKind {
    Success,
    InProgress,
    Failed,
    Missing,
}

impl CellKind {
    fn from_status(status: Option<&str>) -> Self {
        match status {
            Some("success") => CellKind::Success,
            Some("in_progress") => CellKind::InProgress,
            Some("failed") => CellKind::Failed,
            _ => CellKind::Missing,
        }
    }

    pub fn glyph(self) -> &'static str {
        match self {
            CellKind::Success => "✅",
            CellKind::InProgress => "🟡",
            CellKind::Failed => "❌",
            CellKind::Missing => "⚪",
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Tally {
    success: usize,
    in_progress: usize,
    failed: usize,
    missing: usize,
}

impl Tally {
    fn count(&mut self, cell: CellKind) {
        match cell {
            CellKind::Success => self.success += 1,
            CellKind::InProgress => self.in_progress += 1,
            CellKind::Failed => self.failed += 1,
            CellKind::Missing => self.missing += 1,
        }
    }
    fn line(&self) -> String {
        format!(
            "Status: {} success, {} in_progress, {} failed, {} missing",
            self.success, self.in_progress, self.failed, self.missing
        )
    }
}

/// Render the calendar string. `rows` should contain all known partitions
/// for the asset (registered or materialized); duplicates are deduped by
/// `key`, last write wins.
pub fn render_calendar(
    asset_name: &str,
    def: &PartitionDef,
    rows: &[PartitionStatus],
) -> String {
    // De-dupe + sort keys deterministically.
    let mut by_key: BTreeMap<String, Option<String>> = BTreeMap::new();
    for r in rows {
        by_key.insert(r.key.clone(), r.status.clone());
    }

    let mut out = String::new();
    out.push_str(&format!("{}  ({})\n", asset_name, header_kind(def)));

    let mut tally = Tally::default();
    match def {
        PartitionDef::Daily { .. } => {
            render_daily(&by_key, &mut out, &mut tally);
        }
        PartitionDef::Static(_) => {
            render_static(&by_key, &mut out, &mut tally);
        }
        PartitionDef::Multi(dims) => {
            render_multi(dims, &by_key, &mut out, &mut tally);
        }
    }

    out.push('\n');
    out.push_str(&tally.line());
    out.push('\n');
    out
}

fn header_kind(def: &PartitionDef) -> String {
    match def {
        PartitionDef::Daily { start, end } => match end {
            Some(e) => format!("DailyPartition, start={}, end={}", start, e),
            None => format!("DailyPartition, start={}", start),
        },
        PartitionDef::Static(vals) => format!("StaticPartition, {} values", vals.len()),
        PartitionDef::Multi(dims) => {
            let names: Vec<&str> = dims.keys().map(|s| s.as_str()).collect();
            format!("MultiPartition, dimensions=[{}]", names.join(", "))
        }
    }
}

fn render_daily(
    by_key: &BTreeMap<String, Option<String>>,
    out: &mut String,
    tally: &mut Tally,
) {
    // Show the last 4 weeks (~28 entries) for readability when there are
    // many partitions; otherwise render the full range.
    let total = by_key.len();
    let take = total.min(28);
    let start = total.saturating_sub(take);
    let mut col = 0;
    for (i, (key, status)) in by_key.iter().enumerate() {
        let cell = CellKind::from_status(status.as_deref());
        tally.count(cell);
        if i < start {
            // Past the visible window; still tallied so the summary is
            // accurate.
            continue;
        }
        if col == 0 {
            out.push_str("  ");
        } else {
            out.push_str("   ");
        }
        out.push_str(&format!("{} {}", key, cell.glyph()));
        col += 1;
        if col == 3 {
            out.push('\n');
            col = 0;
        }
    }
    if col != 0 {
        out.push('\n');
    }
}

fn render_static(
    by_key: &BTreeMap<String, Option<String>>,
    out: &mut String,
    tally: &mut Tally,
) {
    for (key, status) in by_key {
        let cell = CellKind::from_status(status.as_deref());
        tally.count(cell);
        out.push_str(&format!("  {} {}\n", cell.glyph(), key));
    }
}

fn render_multi(
    dims: &BTreeMap<String, PartitionDef>,
    by_key: &BTreeMap<String, Option<String>>,
    out: &mut String,
    tally: &mut Tally,
) {
    // Group by first dimension (alphabetical first key). Multi keys are
    // joined with `|`.
    let first_dim = dims.keys().next().cloned().unwrap_or_default();
    let mut buckets: BTreeMap<String, Vec<(String, Option<String>)>> = BTreeMap::new();
    for (k, status) in by_key {
        let head = k.split('|').next().unwrap_or(k).to_string();
        buckets.entry(head).or_default().push((k.clone(), status.clone()));
    }
    for (head, entries) in &buckets {
        out.push_str(&format!("  {}={}\n", first_dim, head));
        for (key, status) in entries {
            let cell = CellKind::from_status(status.as_deref());
            tally.count(cell);
            out.push_str(&format!("    {} {}\n", cell.glyph(), key));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn ps(k: &str, s: Option<&str>) -> PartitionStatus {
        PartitionStatus {
            key: k.into(),
            status: s.map(|s| s.into()),
        }
    }

    #[test]
    fn daily_calendar_three_per_row() {
        let def = PartitionDef::Daily {
            start: NaiveDate::from_ymd_opt(2026, 5, 13).unwrap(),
            end: None,
        };
        let rows = vec![
            ps("2026-05-13", Some("success")),
            ps("2026-05-14", Some("success")),
            ps("2026-05-15", Some("success")),
            ps("2026-05-16", Some("in_progress")),
            ps("2026-05-17", None),
            ps("2026-05-18", None),
        ];
        let s = render_calendar("analytics.daily_orders", &def, &rows);
        assert!(s.contains("analytics.daily_orders"));
        assert!(s.contains("DailyPartition, start=2026-05-13"));
        assert!(s.contains("2026-05-13 ✅"));
        assert!(s.contains("2026-05-16 🟡"));
        assert!(s.contains("2026-05-17 ⚪"));
        assert!(s.contains("Status: 3 success, 1 in_progress, 0 failed, 2 missing"));
    }

    #[test]
    fn static_calendar_lists_each() {
        let def = PartitionDef::Static(vec!["jp".into(), "us".into(), "eu".into()]);
        let rows = vec![
            ps("jp", Some("success")),
            ps("us", Some("failed")),
            ps("eu", None),
        ];
        let s = render_calendar("regions", &def, &rows);
        assert!(s.contains("regions  (StaticPartition, 3 values)"));
        assert!(s.contains("✅ jp"));
        assert!(s.contains("❌ us"));
        assert!(s.contains("⚪ eu"));
        assert!(s.contains("Status: 1 success, 0 in_progress, 1 failed, 1 missing"));
    }

    #[test]
    fn multi_calendar_groups_by_first_dim() {
        let def = orch_common::parse_partition_decl(
            "multi(date=daily(start=2026-05-15,end=2026-05-16),region=static(jp,us))",
        )
        .unwrap();
        let rows = vec![
            ps("2026-05-15|jp", Some("success")),
            ps("2026-05-15|us", Some("failed")),
            ps("2026-05-16|jp", None),
            ps("2026-05-16|us", None),
        ];
        let s = render_calendar("regional.orders", &def, &rows);
        assert!(s.contains("regional.orders"));
        assert!(s.contains("MultiPartition, dimensions=[date, region]"));
        assert!(s.contains("date=2026-05-15"));
        assert!(s.contains("✅ 2026-05-15|jp"));
        assert!(s.contains("❌ 2026-05-15|us"));
        assert!(s.contains("Status: 1 success, 0 in_progress, 1 failed, 2 missing"));
    }

    #[test]
    fn missing_when_never_materialized() {
        let def = PartitionDef::Static(vec!["a".into()]);
        let rows = vec![ps("a", None)];
        let s = render_calendar("x", &def, &rows);
        assert!(s.contains("⚪ a"));
        assert!(s.contains("0 success"));
    }
}
