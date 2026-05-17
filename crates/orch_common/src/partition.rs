// Phase 14: Partition definition for first-class Assets.
//
// Three partition flavors, mirroring Dagster terminology:
//   * Daily   — `daily(start=2026-01-01[, end=2026-12-31])`
//                keys are `YYYY-MM-DD` strings.
//   * Static  — `static(jp,us,eu)`
//                keys are the literal value strings themselves.
//   * Multi   — `multi(date=daily(start=2026-01-01), region=static(jp,us))`
//                cartesian product, keys joined by `|`. Dimension order is
//                preserved (BTreeMap insertion order → sorted by key, which
//                is deterministic and what callers want for stable keys).
//
// Keys are surfaced to SQL as the string passed to `$partition_key` (or as
// split components for Multi). Storage in `__orch__.asset_partitions` keeps
// the full key + a JSON `dimension_values` map for Multi-dim introspection.
//
// Parser is intentionally hand-rolled (no Jinja / regex engines) per the
// project memory `feedback_no_jinja_duckorch`: DuckDB-native primitives +
// simple substitution only.

use chrono::{Duration, NaiveDate};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionDef {
    /// Daily partition with a UTC start date and optional end date (inclusive).
    Daily {
        start: NaiveDate,
        end: Option<NaiveDate>,
    },
    /// Static enum-style partition: each value becomes a partition key.
    Static(Vec<String>),
    /// Multi-dimensional partition: cartesian product of named sub-partitions.
    /// Dimension order is the BTreeMap iteration order (alphabetical) — keep
    /// this deterministic so partition keys are stable across re-registers.
    Multi(BTreeMap<String, PartitionDef>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionParseError {
    Empty,
    UnknownKind(String),
    MissingParen(String),
    BadField { key: String, value: String },
    MissingField { kind: String, field: String },
    BadDate(String),
    EmptyList,
}

impl std::fmt::Display for PartitionParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartitionParseError::Empty => write!(f, "@partitions_by: empty declaration"),
            PartitionParseError::UnknownKind(k) => {
                write!(f, "@partitions_by: unknown kind `{}` (expected daily|static|multi)", k)
            }
            PartitionParseError::MissingParen(s) => {
                write!(f, "@partitions_by: missing parentheses in `{}`", s)
            }
            PartitionParseError::BadField { key, value } => {
                write!(f, "@partitions_by: bad field `{}={}`", key, value)
            }
            PartitionParseError::MissingField { kind, field } => {
                write!(f, "@partitions_by {}: missing required `{}`", kind, field)
            }
            PartitionParseError::BadDate(s) => {
                write!(f, "@partitions_by: invalid date `{}` (expected YYYY-MM-DD)", s)
            }
            PartitionParseError::EmptyList => write!(f, "@partitions_by: empty value list"),
        }
    }
}

impl std::error::Error for PartitionParseError {}

/// Parse a `@partitions_by` declaration like:
///   * `daily(start=2026-01-01)`
///   * `daily(start=2026-01-01,end=2026-12-31)`
///   * `static(jp,us,eu)`
///   * `multi(date=daily(start=2026-01-01),region=static(jp,us))`
pub fn parse_partition_decl(s: &str) -> Result<PartitionDef, PartitionParseError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(PartitionParseError::Empty);
    }
    let (kind, body) = split_kind_body(s)?;
    match kind {
        "daily" => parse_daily_body(body),
        "static" => parse_static_body(body),
        "multi" => parse_multi_body(body),
        other => Err(PartitionParseError::UnknownKind(other.to_string())),
    }
}

fn split_kind_body(s: &str) -> Result<(&str, &str), PartitionParseError> {
    let open = s
        .find('(')
        .ok_or_else(|| PartitionParseError::MissingParen(s.to_string()))?;
    if !s.ends_with(')') {
        return Err(PartitionParseError::MissingParen(s.to_string()));
    }
    let kind = s[..open].trim();
    let body = &s[open + 1..s.len() - 1];
    Ok((kind, body))
}

fn parse_daily_body(body: &str) -> Result<PartitionDef, PartitionParseError> {
    let mut start: Option<NaiveDate> = None;
    let mut end: Option<NaiveDate> = None;
    for kv in split_top_level_commas(body) {
        let kv = kv.trim();
        if kv.is_empty() {
            continue;
        }
        let (k, v) = kv.split_once('=').ok_or_else(|| PartitionParseError::BadField {
            key: kv.to_string(),
            value: String::new(),
        })?;
        let k = k.trim();
        let v = v.trim();
        match k {
            "start" => start = Some(parse_iso_date(v)?),
            "end" => end = Some(parse_iso_date(v)?),
            _ => {
                return Err(PartitionParseError::BadField {
                    key: k.to_string(),
                    value: v.to_string(),
                });
            }
        }
    }
    let start = start.ok_or_else(|| PartitionParseError::MissingField {
        kind: "daily".into(),
        field: "start".into(),
    })?;
    Ok(PartitionDef::Daily { start, end })
}

fn parse_static_body(body: &str) -> Result<PartitionDef, PartitionParseError> {
    let vals: Vec<String> = body
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if vals.is_empty() {
        return Err(PartitionParseError::EmptyList);
    }
    Ok(PartitionDef::Static(vals))
}

fn parse_multi_body(body: &str) -> Result<PartitionDef, PartitionParseError> {
    let mut dims: BTreeMap<String, PartitionDef> = BTreeMap::new();
    for chunk in split_top_level_commas(body) {
        let chunk = chunk.trim();
        if chunk.is_empty() {
            continue;
        }
        let eq = chunk.find('=').ok_or_else(|| PartitionParseError::BadField {
            key: chunk.to_string(),
            value: String::new(),
        })?;
        let name = chunk[..eq].trim().to_string();
        let rest = chunk[eq + 1..].trim();
        if name.is_empty() {
            return Err(PartitionParseError::BadField {
                key: chunk.to_string(),
                value: String::new(),
            });
        }
        let sub = parse_partition_decl(rest)?;
        // Multi inside Multi is rejected by the surface grammar — guard here.
        if matches!(sub, PartitionDef::Multi(_)) {
            return Err(PartitionParseError::BadField {
                key: name,
                value: "nested multi not supported".into(),
            });
        }
        dims.insert(name, sub);
    }
    if dims.is_empty() {
        return Err(PartitionParseError::EmptyList);
    }
    Ok(PartitionDef::Multi(dims))
}

fn parse_iso_date(s: &str) -> Result<NaiveDate, PartitionParseError> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| PartitionParseError::BadDate(s.to_string()))
}

/// Comma split that respects nested parentheses. Needed for `multi(...)`
/// bodies where commas inside `daily(start=...)` must not split.
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth: i32 = 0;
    let mut start = 0usize;
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 0 => {
                out.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&s[start..]);
    out
}

impl PartitionDef {
    /// Expand to the concrete partition keys this definition produces.
    ///
    /// * `now` is used by `Daily` to compute its upper bound when no explicit
    ///   end date is set. Callers typically pass today's date.
    /// * `range` lets the caller narrow Daily expansion to a `(from, to)`
    ///   pair (inclusive). Ignored for `Static` (always returns the literal
    ///   list).
    ///
    /// For `Multi`, the cartesian product of every dimension's keys is
    /// returned, joined with `|`. Dimension order is alphabetical (BTreeMap),
    /// matching how `dimension_values` JSON is stored.
    pub fn expand_keys(
        &self,
        now: NaiveDate,
        range: Option<(String, String)>,
    ) -> Vec<String> {
        match self {
            PartitionDef::Daily { start, end } => {
                let (lo, hi) = effective_daily_range(*start, *end, now, range.as_ref());
                if hi < lo {
                    return Vec::new();
                }
                let mut out = Vec::new();
                let mut d = lo;
                while d <= hi {
                    out.push(d.format("%Y-%m-%d").to_string());
                    d += Duration::days(1);
                }
                out
            }
            PartitionDef::Static(vals) => vals.clone(),
            PartitionDef::Multi(dims) => {
                // Compute per-dimension keys, then cartesian product.
                let per_dim: Vec<(String, Vec<String>)> = dims
                    .iter()
                    .map(|(name, def)| (name.clone(), def.expand_keys(now, range.clone())))
                    .collect();
                let mut acc: Vec<Vec<String>> = vec![Vec::new()];
                for (_name, keys) in &per_dim {
                    let mut next: Vec<Vec<String>> = Vec::with_capacity(acc.len() * keys.len());
                    for existing in &acc {
                        for k in keys {
                            let mut row = existing.clone();
                            row.push(k.clone());
                            next.push(row);
                        }
                    }
                    acc = next;
                }
                acc.into_iter().map(|row| row.join("|")).collect()
            }
        }
    }

    /// For Multi: dimension names in deterministic order (alphabetical).
    /// For other variants: a single synthetic dimension `"partition_key"`.
    pub fn dimension_names(&self) -> Vec<String> {
        match self {
            PartitionDef::Multi(dims) => dims.keys().cloned().collect(),
            _ => vec!["partition_key".into()],
        }
    }

    /// Split a partition key string into per-dimension values, paired with
    /// dimension names. For non-Multi definitions this returns a single pair
    /// `("partition_key", key)`. For Multi the key is split on `|` and
    /// zipped with `dimension_names()`.
    pub fn split_key(&self, key: &str) -> Vec<(String, String)> {
        match self {
            PartitionDef::Multi(_) => {
                let names = self.dimension_names();
                let parts: Vec<&str> = key.split('|').collect();
                names
                    .into_iter()
                    .zip(parts.into_iter().chain(std::iter::repeat("")))
                    .map(|(n, v)| (n, v.to_string()))
                    .collect()
            }
            _ => vec![("partition_key".into(), key.to_string())],
        }
    }
}

fn effective_daily_range(
    start: NaiveDate,
    end: Option<NaiveDate>,
    now: NaiveDate,
    range: Option<&(String, String)>,
) -> (NaiveDate, NaiveDate) {
    // Default upper bound: explicit end, else today + 1 (so today's partition
    // is always registered but we don't pre-register far-future keys).
    let default_hi = end.unwrap_or_else(|| now + Duration::days(1));
    let (lo, hi) = match range {
        Some((from, to)) => {
            let f = NaiveDate::parse_from_str(from, "%Y-%m-%d").unwrap_or(start);
            let t = NaiveDate::parse_from_str(to, "%Y-%m-%d").unwrap_or(default_hi);
            (f.max(start), t.min(default_hi))
        }
        None => (start, default_hi),
    };
    (lo, hi)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    #[test]
    fn parses_daily_start_only() {
        let p = parse_partition_decl("daily(start=2026-01-01)").unwrap();
        assert_eq!(p, PartitionDef::Daily { start: d("2026-01-01"), end: None });
    }

    #[test]
    fn parses_daily_start_end() {
        let p = parse_partition_decl("daily(start=2026-01-01, end=2026-12-31)").unwrap();
        assert_eq!(
            p,
            PartitionDef::Daily {
                start: d("2026-01-01"),
                end: Some(d("2026-12-31")),
            }
        );
    }

    #[test]
    fn parses_static() {
        let p = parse_partition_decl("static(jp,us,eu)").unwrap();
        assert_eq!(p, PartitionDef::Static(vec!["jp".into(), "us".into(), "eu".into()]));
    }

    #[test]
    fn parses_static_with_spaces() {
        let p = parse_partition_decl("static( a, b , c )").unwrap();
        assert_eq!(p, PartitionDef::Static(vec!["a".into(), "b".into(), "c".into()]));
    }

    #[test]
    fn parses_multi() {
        let p = parse_partition_decl(
            "multi(date=daily(start=2026-01-01),region=static(jp,us))",
        )
        .unwrap();
        match p {
            PartitionDef::Multi(dims) => {
                assert_eq!(dims.len(), 2);
                assert!(matches!(dims.get("date"), Some(PartitionDef::Daily { .. })));
                assert!(matches!(dims.get("region"), Some(PartitionDef::Static(_))));
            }
            _ => panic!("expected Multi"),
        }
    }

    #[test]
    fn rejects_unknown_kind() {
        assert!(matches!(
            parse_partition_decl("yearly(start=2026)"),
            Err(PartitionParseError::UnknownKind(_))
        ));
    }

    #[test]
    fn rejects_missing_paren() {
        assert!(matches!(
            parse_partition_decl("daily start=2026-01-01"),
            Err(PartitionParseError::MissingParen(_))
        ));
    }

    #[test]
    fn rejects_daily_missing_start() {
        assert!(matches!(
            parse_partition_decl("daily(end=2026-12-31)"),
            Err(PartitionParseError::MissingField { .. })
        ));
    }

    #[test]
    fn rejects_bad_date() {
        assert!(matches!(
            parse_partition_decl("daily(start=2026/01/01)"),
            Err(PartitionParseError::BadDate(_))
        ));
    }

    #[test]
    fn rejects_empty_static() {
        assert!(matches!(
            parse_partition_decl("static()"),
            Err(PartitionParseError::EmptyList)
        ));
    }

    #[test]
    fn rejects_nested_multi() {
        let err = parse_partition_decl(
            "multi(a=multi(b=daily(start=2026-01-01)))",
        )
        .unwrap_err();
        assert!(matches!(err, PartitionParseError::BadField { .. }));
    }

    #[test]
    fn expand_keys_daily_explicit_end() {
        let p = PartitionDef::Daily {
            start: d("2026-05-13"),
            end: Some(d("2026-05-15")),
        };
        let keys = p.expand_keys(d("2026-05-17"), None);
        assert_eq!(keys, vec!["2026-05-13", "2026-05-14", "2026-05-15"]);
    }

    #[test]
    fn expand_keys_daily_open_end_uses_now_plus_one() {
        let p = PartitionDef::Daily {
            start: d("2026-05-15"),
            end: None,
        };
        let keys = p.expand_keys(d("2026-05-17"), None);
        // start..=now+1 → 5/15, 5/16, 5/17, 5/18
        assert_eq!(keys, vec!["2026-05-15", "2026-05-16", "2026-05-17", "2026-05-18"]);
    }

    #[test]
    fn expand_keys_daily_range_clamps_to_start() {
        let p = PartitionDef::Daily {
            start: d("2026-05-15"),
            end: None,
        };
        let keys = p.expand_keys(
            d("2026-05-20"),
            Some(("2026-05-10".into(), "2026-05-16".into())),
        );
        // from clamped to start, to clamped to default_hi (= now+1 = 5/21)
        assert_eq!(keys, vec!["2026-05-15", "2026-05-16"]);
    }

    #[test]
    fn expand_keys_static_returns_literals() {
        let p = PartitionDef::Static(vec!["jp".into(), "us".into(), "eu".into()]);
        let keys = p.expand_keys(d("2026-05-17"), None);
        assert_eq!(keys, vec!["jp", "us", "eu"]);
    }

    #[test]
    fn expand_keys_multi_cartesian() {
        let p = parse_partition_decl(
            "multi(date=daily(start=2026-05-15,end=2026-05-16),region=static(jp,us))",
        )
        .unwrap();
        let keys = p.expand_keys(d("2026-05-17"), None);
        // Alphabetical: date first (2 keys), region (2 keys) → 4 combinations.
        // Outer loop is date, inner is region.
        assert_eq!(
            keys,
            vec![
                "2026-05-15|jp",
                "2026-05-15|us",
                "2026-05-16|jp",
                "2026-05-16|us",
            ]
        );
    }

    #[test]
    fn split_key_multi_pairs_names_and_values() {
        let p = parse_partition_decl(
            "multi(date=daily(start=2026-01-01),region=static(jp,us))",
        )
        .unwrap();
        let parts = p.split_key("2026-05-17|jp");
        assert_eq!(
            parts,
            vec![("date".into(), "2026-05-17".into()), ("region".into(), "jp".into())]
        );
    }

    #[test]
    fn split_key_non_multi_yields_partition_key() {
        let p = PartitionDef::Static(vec!["jp".into()]);
        assert_eq!(
            p.split_key("jp"),
            vec![("partition_key".into(), "jp".into())]
        );
    }
}
