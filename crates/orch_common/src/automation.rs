// Phase 15: AutomationCondition AST + parser + evaluator + `@target_lag`
// duration helper.
//
// Dagster-style automation conditions are stored on each Asset and evaluated
// periodically by the sensor loop. The DSL is intentionally tiny:
//
//   condition := atom
//              | condition AND condition          // & also accepted
//              | condition OR condition           // | also accepted
//              | NOT condition                    // ! also accepted
//              | ( condition )
//
//   atom      := eager()                      // also bare `eager`
//              | on_cron(<cron-expr>)          // wall-clock cron tick
//              | on_interval(<interval-unit>)  // SQLMesh-style interval tracking
//              | on_missing()
//              | freshness_violated()
//              | in_progress()
//
// `on_interval` vs `on_cron`:
//   on_cron("0 6 * * *")  fires when a scheduled wall-clock tick falls between
//                          own_last_materialized_at and now.
//   on_interval("1d")     fires when now - own_last_materialized_at >= 1 day,
//                          regardless of what time of day it is.  This is the
//                          SQLMesh / Snowflake TARGET_LAG style: data-driven
//                          recency check, not wall-clock alignment.
//
// Precedence (highest to lowest): NOT > AND > OR. So
// `eager AND NOT in_progress` parses as `eager AND (NOT in_progress())`.
//
// Hand-rolled lexer + recursive-descent parser. No Jinja, no regex.
// `serialize_dsl` round-trips an AST back to a canonical string for storage
// on `__orch__.assets.automation_condition`.
//
// Evaluator:
//   `evaluate(cond, ctx)`           → (bool, reason)   — "should we run?"
//   `missing_intervals_for(cond, ctx)` → Vec<Interval> — for on_interval: which
//                                        intervals to process (may be >1 for backfill)

use chrono::{Duration as ChronoDuration, NaiveDateTime, TimeZone, Utc};
use cron::Schedule;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::interval::{compute_missing, merge_intervals, Interval, IntervalUnit};

// ---------------------------------------------------------------------------
// AST
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AutomationCondition {
    Eager,
    OnCron(String),
    /// SQLMesh-style interval tracking. Fires when one or more intervals of the
    /// given unit are missing from `EvalContext::stored_intervals`.
    /// Unlike `on_cron`, this is purely data-driven: "which intervals haven't
    /// been computed yet?" rather than "did a wall-clock tick happen?".
    OnInterval(IntervalUnit),
    OnMissing,
    FreshnessViolated,
    InProgress,
    And(Box<AutomationCondition>, Box<AutomationCondition>),
    Or(Box<AutomationCondition>, Box<AutomationCondition>),
    Not(Box<AutomationCondition>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutomationParseError {
    Empty,
    Unexpected { at: usize, got: String },
    UnknownAtom(String),
    UnterminatedString,
    MissingArg(&'static str),
    BadDuration(String),
    BadCron(String),
}

impl std::fmt::Display for AutomationParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AutomationParseError::Empty => write!(f, "@automation: empty expression"),
            AutomationParseError::Unexpected { at, got } => {
                write!(f, "@automation: unexpected `{}` at position {}", got, at)
            }
            AutomationParseError::UnknownAtom(s) => {
                write!(
                    f,
                    "@automation: unknown atom `{}` (expected eager|on_cron|on_interval|on_missing|freshness_violated|in_progress)",
                    s
                )
            }
            AutomationParseError::UnterminatedString => {
                write!(f, "@automation: unterminated string literal")
            }
            AutomationParseError::MissingArg(name) => {
                write!(f, "@automation: `{}` requires an argument", name)
            }
            AutomationParseError::BadDuration(s) => {
                write!(f, "@target_lag: invalid duration `{}`", s)
            }
            AutomationParseError::BadCron(s) => {
                write!(f, "@automation on_cron: invalid cron expression `{}`", s)
            }
        }
    }
}

impl std::error::Error for AutomationParseError {}

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    LParen,
    RParen,
    And,
    Or,
    Not,
    Ident(String),
    Str(String),
    Eof,
}

struct Lexer<'a> {
    src: &'a str,
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(src: &'a str) -> Self {
        Self { src, pos: 0 }
    }

    fn peek_char(&self) -> Option<char> {
        self.src[self.pos..].chars().next()
    }

    fn bump_char(&mut self) -> Option<char> {
        let c = self.peek_char()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek_char() {
            if c.is_whitespace() {
                self.bump_char();
            } else {
                break;
            }
        }
    }

    fn next(&mut self) -> Result<(usize, Tok), AutomationParseError> {
        self.skip_ws();
        let start = self.pos;
        let c = match self.peek_char() {
            Some(c) => c,
            None => return Ok((start, Tok::Eof)),
        };
        match c {
            '(' => {
                self.bump_char();
                Ok((start, Tok::LParen))
            }
            ')' => {
                self.bump_char();
                Ok((start, Tok::RParen))
            }
            '&' => {
                self.bump_char();
                // accept `&&`
                if self.peek_char() == Some('&') {
                    self.bump_char();
                }
                Ok((start, Tok::And))
            }
            '|' => {
                self.bump_char();
                if self.peek_char() == Some('|') {
                    self.bump_char();
                }
                Ok((start, Tok::Or))
            }
            '!' => {
                self.bump_char();
                Ok((start, Tok::Not))
            }
            '"' | '\'' => {
                let quote = c;
                self.bump_char();
                let s_start = self.pos;
                while let Some(ch) = self.peek_char() {
                    if ch == quote {
                        let s = self.src[s_start..self.pos].to_string();
                        self.bump_char();
                        return Ok((start, Tok::Str(s)));
                    }
                    self.bump_char();
                }
                Err(AutomationParseError::UnterminatedString)
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let id_start = self.pos;
                while let Some(ch) = self.peek_char() {
                    if ch.is_ascii_alphanumeric() || ch == '_' {
                        self.bump_char();
                    } else {
                        break;
                    }
                }
                let ident = &self.src[id_start..self.pos];
                let tok = match ident.to_ascii_lowercase().as_str() {
                    "and" => Tok::And,
                    "or" => Tok::Or,
                    "not" => Tok::Not,
                    _ => Tok::Ident(ident.to_string()),
                };
                Ok((start, tok))
            }
            other => Err(AutomationParseError::Unexpected {
                at: start,
                got: other.to_string(),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Parser (recursive descent: or > and > not > primary)
// ---------------------------------------------------------------------------

struct Parser<'a> {
    lx: Lexer<'a>,
    peeked: Option<(usize, Tok)>,
}

impl<'a> Parser<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            lx: Lexer::new(src),
            peeked: None,
        }
    }

    fn peek(&mut self) -> Result<&(usize, Tok), AutomationParseError> {
        if self.peeked.is_none() {
            self.peeked = Some(self.lx.next()?);
        }
        Ok(self.peeked.as_ref().unwrap())
    }

    fn bump(&mut self) -> Result<(usize, Tok), AutomationParseError> {
        if let Some(t) = self.peeked.take() {
            Ok(t)
        } else {
            self.lx.next()
        }
    }

    fn parse_or(&mut self) -> Result<AutomationCondition, AutomationParseError> {
        let mut lhs = self.parse_and()?;
        loop {
            match self.peek()? {
                (_, Tok::Or) => {
                    self.bump()?;
                    let rhs = self.parse_and()?;
                    lhs = AutomationCondition::Or(Box::new(lhs), Box::new(rhs));
                }
                _ => break,
            }
        }
        Ok(lhs)
    }

    fn parse_and(&mut self) -> Result<AutomationCondition, AutomationParseError> {
        let mut lhs = self.parse_not()?;
        loop {
            match self.peek()? {
                (_, Tok::And) => {
                    self.bump()?;
                    let rhs = self.parse_not()?;
                    lhs = AutomationCondition::And(Box::new(lhs), Box::new(rhs));
                }
                _ => break,
            }
        }
        Ok(lhs)
    }

    fn parse_not(&mut self) -> Result<AutomationCondition, AutomationParseError> {
        match self.peek()? {
            (_, Tok::Not) => {
                self.bump()?;
                let inner = self.parse_not()?;
                Ok(AutomationCondition::Not(Box::new(inner)))
            }
            _ => self.parse_primary(),
        }
    }

    fn parse_primary(&mut self) -> Result<AutomationCondition, AutomationParseError> {
        let (pos, tok) = self.bump()?;
        match tok {
            Tok::LParen => {
                let inner = self.parse_or()?;
                let (_, close) = self.bump()?;
                if close != Tok::RParen {
                    return Err(AutomationParseError::Unexpected {
                        at: pos,
                        got: format!("{:?}", close),
                    });
                }
                Ok(inner)
            }
            Tok::Ident(name) => self.parse_atom_after_ident(&name),
            other => Err(AutomationParseError::Unexpected {
                at: pos,
                got: format!("{:?}", other),
            }),
        }
    }

    fn parse_atom_after_ident(
        &mut self,
        name: &str,
    ) -> Result<AutomationCondition, AutomationParseError> {
        // Atom names are normalized lowercase. `eager` and the others may
        // appear bare (no parens) — the bare shortcut applies to all atoms
        // that take no argument; on_cron always requires an arg.
        let lower = name.to_ascii_lowercase();
        let has_paren = matches!(self.peek()?, (_, Tok::LParen));
        if has_paren {
            self.bump()?; // consume `(`
            let arg: Option<String> = match self.peek()? {
                (_, Tok::RParen) => None,
                (_, Tok::Str(_)) => {
                    if let (_, Tok::Str(s)) = self.bump()? {
                        Some(s)
                    } else {
                        unreachable!()
                    }
                }
                (pos, t) => {
                    return Err(AutomationParseError::Unexpected {
                        at: *pos,
                        got: format!("{:?}", t),
                    });
                }
            };
            let (pos, close) = self.bump()?;
            if close != Tok::RParen {
                return Err(AutomationParseError::Unexpected {
                    at: pos,
                    got: format!("{:?}", close),
                });
            }
            build_atom(&lower, arg)
        } else {
            // Bare atom shortcut.
            build_atom(&lower, None)
        }
    }
}

fn build_atom(
    name: &str,
    arg: Option<String>,
) -> Result<AutomationCondition, AutomationParseError> {
    match name {
        "eager" => Ok(AutomationCondition::Eager),
        "on_missing" => Ok(AutomationCondition::OnMissing),
        "freshness_violated" => Ok(AutomationCondition::FreshnessViolated),
        "in_progress" => Ok(AutomationCondition::InProgress),
        "on_cron" => {
            let a = arg.ok_or(AutomationParseError::MissingArg("on_cron"))?;
            if a.is_empty() {
                return Err(AutomationParseError::MissingArg("on_cron"));
            }
            // Validate cron eagerly so authoring errors surface at parse time.
            let _ = Schedule::from_str(&normalize_cron(&a))
                .map_err(|_| AutomationParseError::BadCron(a.clone()))?;
            Ok(AutomationCondition::OnCron(a))
        }
        "on_interval" => {
            let a = arg.ok_or(AutomationParseError::MissingArg("on_interval"))?;
            if a.is_empty() {
                return Err(AutomationParseError::MissingArg("on_interval"));
            }
            let unit = IntervalUnit::from_str(&a)
                .ok_or_else(|| AutomationParseError::BadDuration(a.clone()))?;
            Ok(AutomationCondition::OnInterval(unit))
        }
        other => Err(AutomationParseError::UnknownAtom(other.to_string())),
    }
}

/// `cron` 0.12 expects 6+ fields. If a 5-field expression is supplied,
/// prepend `0 ` for the seconds slot. Mirrors `orch_cli::normalize_cron`.
fn normalize_cron(expr: &str) -> String {
    let n = expr.split_whitespace().count();
    if n == 5 {
        format!("0 {}", expr)
    } else {
        expr.to_string()
    }
}

/// Parse an `@automation <expr>` declaration into an AST. Accepts the bare
/// `eager` shortcut (no parens) and any well-formed combination of atoms,
/// `AND`/`&`, `OR`/`|`, `NOT`/`!`, and parentheses.
pub fn parse_automation(s: &str) -> Result<AutomationCondition, AutomationParseError> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(AutomationParseError::Empty);
    }
    let mut p = Parser::new(trimmed);
    let cond = p.parse_or()?;
    // Make sure nothing trails.
    match p.peek()? {
        (_, Tok::Eof) => Ok(cond),
        (pos, t) => Err(AutomationParseError::Unexpected {
            at: *pos,
            got: format!("{:?}", t),
        }),
    }
}

// ---------------------------------------------------------------------------
// DSL serialization (canonical round-trip)
// ---------------------------------------------------------------------------

impl AutomationCondition {
    /// Walk the AST and return the first `OnInterval` unit, if any. Used at
    /// parse time to populate `Task::interval_unit` so the C++ registration
    /// path can write `__orch__.assets.interval_unit` without re-parsing.
    pub fn first_interval_unit(&self) -> Option<IntervalUnit> {
        match self {
            AutomationCondition::OnInterval(unit) => Some(*unit),
            AutomationCondition::And(l, r) | AutomationCondition::Or(l, r) => {
                l.first_interval_unit().or_else(|| r.first_interval_unit())
            }
            AutomationCondition::Not(inner) => inner.first_interval_unit(),
            _ => None,
        }
    }

    /// Canonical DSL form. Used to store the condition string on
    /// `__orch__.assets.automation_condition`. Always wraps atoms in `()` so
    /// re-parsing is unambiguous; precedence-required parentheses are
    /// inserted around `OR` children of `AND` and around any `NOT` operand
    /// that is itself a compound.
    pub fn serialize_dsl(&self) -> String {
        match self {
            AutomationCondition::Eager => "eager()".into(),
            AutomationCondition::OnCron(expr) => format!("on_cron(\"{}\")", expr),
            AutomationCondition::OnInterval(unit) => {
                format!("on_interval(\"{}\")", unit.to_dsl_str())
            }
            AutomationCondition::OnMissing => "on_missing()".into(),
            AutomationCondition::FreshnessViolated => "freshness_violated()".into(),
            AutomationCondition::InProgress => "in_progress()".into(),
            AutomationCondition::Not(inner) => match inner.as_ref() {
                AutomationCondition::And(_, _) | AutomationCondition::Or(_, _) => {
                    format!("NOT ({})", inner.serialize_dsl())
                }
                _ => format!("NOT {}", inner.serialize_dsl()),
            },
            AutomationCondition::And(l, r) => {
                let ls = wrap_if_or(l);
                let rs = wrap_if_or(r);
                format!("{} AND {}", ls, rs)
            }
            AutomationCondition::Or(l, r) => {
                format!("{} OR {}", l.serialize_dsl(), r.serialize_dsl())
            }
        }
    }
}

fn wrap_if_or(c: &AutomationCondition) -> String {
    match c {
        AutomationCondition::Or(_, _) => format!("({})", c.serialize_dsl()),
        _ => c.serialize_dsl(),
    }
}

// ---------------------------------------------------------------------------
// @target_lag duration helper
// ---------------------------------------------------------------------------

/// Parse a `@target_lag` duration. Accepts:
///   * `Ns` / `Nsec` / `Nsecs` / `Nsecond[s]`     → seconds
///   * `Nm` / `Nmin` / `Nminute[s]`               → minutes
///   * `Nh` / `Nhour[s]`                          → hours
///   * `Nd` / `Nday[s]`                           → days
///   * `Nms` / `Nmillisecond[s]`                  → milliseconds (rounded up)
///   * plain integer → seconds
///
/// Internally `@target_lag 5min` is wired to `automation = eager() throttle 5min`
/// — for this phase the value is stored in `__orch__.assets.target_lag_seconds`
/// and the throttle semantics live in `evaluate`.
pub fn parse_target_lag(s: &str) -> Result<u64, AutomationParseError> {
    let raw = s.trim();
    if raw.is_empty() {
        return Err(AutomationParseError::BadDuration(s.to_string()));
    }
    // Split into numeric prefix + unit suffix.
    let split_at = raw
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(raw.len());
    let num_part = &raw[..split_at];
    let unit_part = raw[split_at..].trim().to_ascii_lowercase();
    if num_part.is_empty() {
        return Err(AutomationParseError::BadDuration(s.to_string()));
    }
    let n: f64 = num_part
        .parse()
        .map_err(|_| AutomationParseError::BadDuration(s.to_string()))?;
    let secs: f64 = match unit_part.as_str() {
        "" => n,
        "ms" | "millisecond" | "milliseconds" => (n / 1000.0).ceil(),
        "s" | "sec" | "secs" | "second" | "seconds" => n,
        "m" | "min" | "minute" | "minutes" => n * 60.0,
        "h" | "hour" | "hours" => n * 3600.0,
        "d" | "day" | "days" => n * 86_400.0,
        _ => return Err(AutomationParseError::BadDuration(s.to_string())),
    };
    if secs < 0.0 || !secs.is_finite() {
        return Err(AutomationParseError::BadDuration(s.to_string()));
    }
    // Round half-up; min 1 second when caller asked for any positive duration.
    let rounded = secs.round() as u64;
    Ok(if rounded == 0 && n > 0.0 { 1 } else { rounded })
}

// ---------------------------------------------------------------------------
// Evaluator
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct EvalContext {
    /// Max materialized_at across all upstream Assets, considering both
    /// success and failed rows (`Eager` only cares that something changed).
    pub upstream_max_materialized_at: Option<NaiveDateTime>,
    /// Max materialized_at on this Asset where status='success'. Used as the
    /// "last good run" anchor by `Eager`, `OnCron` and the throttle.
    pub own_last_materialized_at: Option<NaiveDateTime>,
    /// Number of registered partitions without any 'success' materialization.
    pub missing_partition_count: u64,
    /// Sensor tick timestamp. Pass `Utc::now().naive_utc()` in production.
    pub now: NaiveDateTime,
    /// Freshness policy lag in seconds (Phase 16 wires this from the
    /// `@freshness max_lag=...` header). `None` means "no policy", which
    /// makes `freshness_violated()` always return false.
    pub freshness_lag_seconds: Option<u64>,
    /// Whether any materialization for this asset is currently in flight.
    pub in_progress: bool,
    /// `@target_lag` from the asset. None = no throttle.
    pub target_lag_seconds: Option<u64>,
    /// Last automation evaluation timestamp (for throttle bookkeeping). Not
    /// used by the current evaluator — the throttle is anchored on the last
    /// *successful run* instead, matching Dagster semantics.
    pub last_evaluated_at: Option<NaiveDateTime>,

    // --- SQLMesh-style interval tracking (used by on_interval) ---
    /// Already-computed intervals for this asset, loaded from
    /// `__orch__.asset_intervals`. Must be merged (non-overlapping, sorted)
    /// before being placed here; pass through `merge_intervals` first.
    pub stored_intervals: Vec<Interval>,
    /// The earliest timestamp from which intervals should be tracked for this
    /// asset (equivalent to SQLMesh's model `start` date). Epoch seconds UTC.
    /// `None` means "use own_last_materialized_at as anchor" (legacy fallback).
    pub interval_start_ts: Option<i64>,
    /// Re-process the last N intervals whenever a newer interval is missing
    /// (late-arriving data; SQLMesh model `lookback`). `0` disables.
    pub lookback: u32,
    /// Include the current, incomplete interval when computing gaps
    /// (SQLMesh `allow_partials`). Default false: only complete intervals.
    pub allow_partials: bool,
}

impl EvalContext {
    pub fn at(now: NaiveDateTime) -> Self {
        Self {
            now,
            ..Self::default()
        }
    }
}

/// Evaluate an automation condition. Returns `(condition_met, reason)`.
///
/// Semantics:
///   * `Eager`              → upstream_max_materialized_at > own_last_materialized_at
///                            (NULL own ⇒ true if upstream exists)
///   * `OnCron(expr)`       → some cron tick falls in
///                            `(own_last_materialized_at, now]`
///   * `OnMissing`          → `missing_partition_count > 0`
///   * `FreshnessViolated`  → `now - own_last_materialized_at > freshness_lag_seconds`
///                            (only if policy is set)
///   * `InProgress`         → `ctx.in_progress`
///   * AND/OR/NOT recurse, short-circuiting AND/OR.
///
/// `target_lag_seconds`: if a successful run is within the lag window of
/// `now`, the evaluator short-circuits and returns
/// `(false, "throttled: last run < target_lag (Ns)")` *without* descending
/// into the AST. This wraps the condition tree like a Dagster-style throttle.
pub fn evaluate(cond: &AutomationCondition, ctx: &EvalContext) -> (bool, String) {
    if let (Some(lag), Some(last)) = (ctx.target_lag_seconds, ctx.own_last_materialized_at) {
        let elapsed = (ctx.now - last).num_seconds();
        if elapsed >= 0 && (elapsed as u64) < lag {
            return (
                false,
                format!("throttled: last run {}s ago < target_lag {}s", elapsed, lag),
            );
        }
    }
    evaluate_inner(cond, ctx)
}

fn evaluate_inner(cond: &AutomationCondition, ctx: &EvalContext) -> (bool, String) {
    match cond {
        AutomationCondition::Eager => match (
            ctx.upstream_max_materialized_at,
            ctx.own_last_materialized_at,
        ) {
            (None, _) => (false, "no upstream materialization".into()),
            (Some(up), None) => (
                true,
                format!("upstream updated {} (own never materialized)", up),
            ),
            (Some(up), Some(own)) => {
                if up > own {
                    (true, format!("upstream updated {} > own last {}", up, own))
                } else {
                    (false, format!("upstream {} <= own last {}", up, own))
                }
            }
        },
        AutomationCondition::OnCron(expr) => {
            let sched = match Schedule::from_str(&normalize_cron(expr)) {
                Ok(s) => s,
                Err(_) => return (false, format!("invalid cron `{}`", expr)),
            };
            let anchor = ctx
                .own_last_materialized_at
                .unwrap_or(ctx.now - ChronoDuration::days(36500));
            let anchor_utc = Utc.from_utc_datetime(&anchor);
            let now_utc = Utc.from_utc_datetime(&ctx.now);
            let next = sched.after(&anchor_utc).next();
            match next {
                Some(t) if t <= now_utc => (
                    true,
                    format!(
                        "cron `{}` ticked at {} (anchor {})",
                        expr,
                        t.naive_utc(),
                        anchor
                    ),
                ),
                Some(t) => (
                    false,
                    format!(
                        "cron `{}` next tick at {} > now {}",
                        expr,
                        t.naive_utc(),
                        ctx.now
                    ),
                ),
                None => (false, format!("cron `{}` produced no upcoming tick", expr)),
            }
        }
        AutomationCondition::OnInterval(unit) => {
            let missing = interval_gaps(unit, ctx);
            if missing.is_empty() {
                (
                    false,
                    format!(
                        "on_interval({}): all intervals up to date",
                        unit.to_dsl_str()
                    ),
                )
            } else {
                (
                    true,
                    format!(
                        "on_interval({}): {} interval(s) missing, next [{}, {})",
                        unit.to_dsl_str(),
                        missing.len(),
                        missing[0].0,
                        missing[0].1,
                    ),
                )
            }
        }
        AutomationCondition::OnMissing => {
            if ctx.missing_partition_count > 0 {
                (
                    true,
                    format!("{} partition(s) missing", ctx.missing_partition_count),
                )
            } else {
                (false, "no missing partitions".into())
            }
        }
        AutomationCondition::FreshnessViolated => {
            match (ctx.freshness_lag_seconds, ctx.own_last_materialized_at) {
                (None, _) => (false, "no freshness policy".into()),
                (Some(_), None) => (true, "never materialized (freshness violated)".into()),
                (Some(lag), Some(last)) => {
                    let elapsed = (ctx.now - last).num_seconds();
                    if elapsed >= 0 && (elapsed as u64) > lag {
                        (
                            true,
                            format!("stale: {}s > freshness lag {}s", elapsed, lag),
                        )
                    } else {
                        (
                            false,
                            format!("fresh: {}s <= freshness lag {}s", elapsed.max(0), lag),
                        )
                    }
                }
            }
        }
        AutomationCondition::InProgress => {
            if ctx.in_progress {
                (true, "in_progress".into())
            } else {
                (false, "not in_progress".into())
            }
        }
        AutomationCondition::And(l, r) => {
            let (lv, lr) = evaluate_inner(l, ctx);
            if !lv {
                return (false, format!("AND short-circuit: lhs false ({})", lr));
            }
            let (rv, rr) = evaluate_inner(r, ctx);
            if rv {
                (true, format!("AND: {} AND {}", lr, rr))
            } else {
                (false, format!("AND: rhs false ({})", rr))
            }
        }
        AutomationCondition::Or(l, r) => {
            let (lv, lr) = evaluate_inner(l, ctx);
            if lv {
                return (true, format!("OR short-circuit: lhs true ({})", lr));
            }
            let (rv, rr) = evaluate_inner(r, ctx);
            if rv {
                (true, format!("OR: rhs true ({})", rr))
            } else {
                (false, format!("OR: both false ({} / {})", lr, rr))
            }
        }
        AutomationCondition::Not(inner) => {
            let (v, r) = evaluate_inner(inner, ctx);
            (!v, format!("NOT ({})", r))
        }
    }
}

// ---------------------------------------------------------------------------
// Interval gap helper
// ---------------------------------------------------------------------------

/// Compute missing intervals for an `OnInterval` condition from the context.
///
/// Uses `ctx.stored_intervals` as the "already done" set and `ctx.now` as
/// the exclusive end.  The inclusive start is `ctx.interval_start_ts` when
/// set, otherwise falls back to the floor of `ctx.own_last_materialized_at`
/// (legacy: treat last run as "everything before this was done").
fn interval_gaps(unit: &IntervalUnit, ctx: &EvalContext) -> Vec<Interval> {
    let now_ts = ctx.now.and_utc().timestamp();
    let start_ts = match ctx.interval_start_ts {
        Some(ts) => ts,
        None => match ctx.own_last_materialized_at {
            // Legacy fallback: last materialization = all prior intervals done.
            Some(last) => {
                let last_ts = last.and_utc().timestamp();
                unit.floor(last_ts) + unit.step_secs()
            }
            // Never run and no start configured → track from floor(now) - 1 step
            // so at least the most recent complete interval is caught.
            None => unit.floor(now_ts) - unit.step_secs(),
        },
    };
    // Exclude the currently-in-progress interval unless allow_partials —
    // then the partial interval up to `now` is also processed (SQLMesh
    // `allow_partials`; expand_range appends the unaligned end boundary).
    let end_ts = if ctx.allow_partials {
        now_ts
    } else {
        unit.floor(now_ts)
    };
    if start_ts >= end_ts {
        return vec![];
    }
    let stored = merge_intervals(ctx.stored_intervals.clone());
    compute_missing(*unit, &stored, start_ts, end_ts, ctx.lookback)
}

/// For `OnInterval` conditions: return the list of intervals that should be
/// processed (in chronological order).  The sensor loop can iterate this list
/// and enqueue one task execution per interval.
///
/// Returns an empty vec for non-interval conditions (use `evaluate` instead).
pub fn missing_intervals_for(cond: &AutomationCondition, ctx: &EvalContext) -> Vec<Interval> {
    match cond {
        AutomationCondition::OnInterval(unit) => interval_gaps(unit, ctx),
        AutomationCondition::And(l, r) => {
            // Intersect: only return intervals where both sides agree.
            // Non-interval side acts as a gate (if it's false, no intervals).
            let (lv, _) = evaluate(l, ctx);
            let (rv, _) = evaluate(r, ctx);
            match (l.as_ref(), r.as_ref()) {
                (AutomationCondition::OnInterval(_), _) if rv => missing_intervals_for(l, ctx),
                (_, AutomationCondition::OnInterval(_)) if lv => missing_intervals_for(r, ctx),
                _ => vec![],
            }
        }
        AutomationCondition::Or(l, r) => {
            // Union of missing intervals from both sides.
            let mut combined = missing_intervals_for(l, ctx);
            combined.extend(missing_intervals_for(r, ctx));
            merge_intervals(combined)
        }
        _ => vec![],
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn ts(s: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap()
    }

    // -------------------- parser --------------------

    #[test]
    fn parses_bare_eager_shortcut() {
        assert_eq!(
            parse_automation("eager").unwrap(),
            AutomationCondition::Eager
        );
    }

    #[test]
    fn parses_eager_parens() {
        assert_eq!(
            parse_automation("eager()").unwrap(),
            AutomationCondition::Eager
        );
    }

    #[test]
    fn parses_on_missing() {
        assert_eq!(
            parse_automation("on_missing()").unwrap(),
            AutomationCondition::OnMissing
        );
    }

    #[test]
    fn parses_freshness_violated() {
        assert_eq!(
            parse_automation("freshness_violated()").unwrap(),
            AutomationCondition::FreshnessViolated
        );
    }

    #[test]
    fn parses_in_progress() {
        assert_eq!(
            parse_automation("in_progress()").unwrap(),
            AutomationCondition::InProgress
        );
    }

    #[test]
    fn parses_on_cron_quoted() {
        let c = parse_automation("on_cron(\"0 6 * * *\")").unwrap();
        assert_eq!(c, AutomationCondition::OnCron("0 6 * * *".into()));
    }

    #[test]
    fn parses_on_cron_single_quoted() {
        // Allow single-quoted cron string in addition to double-quoted.
        let c = parse_automation("on_cron('0 6 * * *')").unwrap();
        assert_eq!(c, AutomationCondition::OnCron("0 6 * * *".into()));
    }

    #[test]
    fn parses_and_two_atoms() {
        let c = parse_automation("eager AND in_progress()").unwrap();
        match c {
            AutomationCondition::And(l, r) => {
                assert_eq!(*l, AutomationCondition::Eager);
                assert_eq!(*r, AutomationCondition::InProgress);
            }
            other => panic!("expected And, got {:?}", other),
        }
    }

    #[test]
    fn parses_or_with_symbol_and_keyword() {
        let c = parse_automation("eager | on_missing()").unwrap();
        match c {
            AutomationCondition::Or(l, r) => {
                assert_eq!(*l, AutomationCondition::Eager);
                assert_eq!(*r, AutomationCondition::OnMissing);
            }
            other => panic!("expected Or, got {:?}", other),
        }
    }

    #[test]
    fn precedence_not_binds_tighter_than_and() {
        // eager AND NOT in_progress  ⇒  And(Eager, Not(InProgress))
        let c = parse_automation("eager AND NOT in_progress()").unwrap();
        match c {
            AutomationCondition::And(l, r) => {
                assert_eq!(*l, AutomationCondition::Eager);
                assert_eq!(
                    *r,
                    AutomationCondition::Not(Box::new(AutomationCondition::InProgress))
                );
            }
            other => panic!("expected And, got {:?}", other),
        }
    }

    #[test]
    fn precedence_and_binds_tighter_than_or() {
        // a OR b AND c  ⇒  Or(a, And(b, c))
        let c = parse_automation("eager OR on_missing() AND in_progress()").unwrap();
        match c {
            AutomationCondition::Or(l, r) => {
                assert_eq!(*l, AutomationCondition::Eager);
                match *r {
                    AutomationCondition::And(la, ra) => {
                        assert_eq!(*la, AutomationCondition::OnMissing);
                        assert_eq!(*ra, AutomationCondition::InProgress);
                    }
                    other => panic!("expected And inner, got {:?}", other),
                }
            }
            other => panic!("expected Or, got {:?}", other),
        }
    }

    #[test]
    fn parentheses_override_precedence() {
        // (a OR b) AND c
        let c = parse_automation("(eager OR on_missing()) AND in_progress()").unwrap();
        match c {
            AutomationCondition::And(l, r) => {
                assert!(matches!(*l, AutomationCondition::Or(_, _)));
                assert_eq!(*r, AutomationCondition::InProgress);
            }
            other => panic!("expected And, got {:?}", other),
        }
    }

    #[test]
    fn double_negation() {
        let c = parse_automation("NOT NOT eager").unwrap();
        match c {
            AutomationCondition::Not(inner) => match *inner {
                AutomationCondition::Not(inner2) => {
                    assert_eq!(*inner2, AutomationCondition::Eager);
                }
                other => panic!("expected Not inner, got {:?}", other),
            },
            other => panic!("expected Not, got {:?}", other),
        }
    }

    #[test]
    fn rejects_empty() {
        assert!(matches!(
            parse_automation("   "),
            Err(AutomationParseError::Empty)
        ));
    }

    #[test]
    fn rejects_unknown_atom() {
        let err = parse_automation("magic()").unwrap_err();
        assert!(
            matches!(err, AutomationParseError::UnknownAtom(_)),
            "{:?}",
            err
        );
    }

    #[test]
    fn rejects_on_cron_without_arg() {
        let err = parse_automation("on_cron()").unwrap_err();
        assert!(
            matches!(err, AutomationParseError::MissingArg("on_cron")),
            "{:?}",
            err
        );
    }

    #[test]
    fn rejects_bad_cron() {
        let err = parse_automation("on_cron(\"not-a-cron\")").unwrap_err();
        assert!(matches!(err, AutomationParseError::BadCron(_)), "{:?}", err);
    }

    #[test]
    fn rejects_dangling_operator() {
        assert!(parse_automation("eager AND").is_err());
        assert!(parse_automation("AND eager").is_err());
    }

    #[test]
    fn rejects_unmatched_paren() {
        assert!(parse_automation("(eager").is_err());
    }

    // -------------------- DSL round-trip --------------------

    #[test]
    fn dsl_roundtrip_atoms() {
        for src in [
            "eager()",
            "on_missing()",
            "freshness_violated()",
            "in_progress()",
        ] {
            let c = parse_automation(src).unwrap();
            assert_eq!(c.serialize_dsl(), src);
            let c2 = parse_automation(&c.serialize_dsl()).unwrap();
            assert_eq!(c, c2);
        }
    }

    #[test]
    fn dsl_roundtrip_combinations() {
        let cases = [
            "eager AND NOT in_progress()",
            "eager OR on_missing()",
            "(eager OR on_missing()) AND NOT in_progress()",
            "on_cron(\"0 6 * * *\")",
        ];
        for src in cases {
            let c = parse_automation(src).unwrap();
            let s = c.serialize_dsl();
            let c2 = parse_automation(&s).unwrap();
            assert_eq!(c, c2, "round trip mismatch for `{}` -> `{}`", src, s);
        }
    }

    // -------------------- target_lag --------------------

    #[test]
    fn target_lag_seconds() {
        assert_eq!(parse_target_lag("30s").unwrap(), 30);
        assert_eq!(parse_target_lag("30sec").unwrap(), 30);
        assert_eq!(parse_target_lag("30seconds").unwrap(), 30);
    }

    #[test]
    fn target_lag_minutes() {
        assert_eq!(parse_target_lag("5m").unwrap(), 300);
        assert_eq!(parse_target_lag("5min").unwrap(), 300);
        assert_eq!(parse_target_lag("5minutes").unwrap(), 300);
    }

    #[test]
    fn target_lag_hours_days() {
        assert_eq!(parse_target_lag("1h").unwrap(), 3600);
        assert_eq!(parse_target_lag("1d").unwrap(), 86_400);
    }

    #[test]
    fn target_lag_ms_rounds_up() {
        assert_eq!(parse_target_lag("1500ms").unwrap(), 2);
        assert_eq!(parse_target_lag("500ms").unwrap(), 1);
    }

    #[test]
    fn target_lag_plain_integer_is_seconds() {
        assert_eq!(parse_target_lag("45").unwrap(), 45);
    }

    #[test]
    fn target_lag_rejects_garbage() {
        assert!(parse_target_lag("five minutes").is_err());
        assert!(parse_target_lag("").is_err());
        assert!(parse_target_lag("10weeks").is_err());
    }

    // -------------------- evaluator --------------------

    fn ctx_at(now: &str) -> EvalContext {
        EvalContext::at(ts(&format!("{} 12:00:00", now)))
    }

    #[test]
    fn eager_true_when_upstream_newer() {
        let mut ctx = ctx_at("2026-05-17");
        ctx.upstream_max_materialized_at = Some(ts("2026-05-17 10:00:00"));
        ctx.own_last_materialized_at = Some(ts("2026-05-17 09:00:00"));
        let (met, _) = evaluate(&AutomationCondition::Eager, &ctx);
        assert!(met);
    }

    #[test]
    fn eager_false_when_own_newer() {
        let mut ctx = ctx_at("2026-05-17");
        ctx.upstream_max_materialized_at = Some(ts("2026-05-17 09:00:00"));
        ctx.own_last_materialized_at = Some(ts("2026-05-17 10:00:00"));
        let (met, _) = evaluate(&AutomationCondition::Eager, &ctx);
        assert!(!met);
    }

    #[test]
    fn eager_true_when_own_never() {
        let mut ctx = ctx_at("2026-05-17");
        ctx.upstream_max_materialized_at = Some(ts("2026-05-17 10:00:00"));
        let (met, _) = evaluate(&AutomationCondition::Eager, &ctx);
        assert!(met);
    }

    #[test]
    fn on_missing_atom() {
        let mut ctx = ctx_at("2026-05-17");
        ctx.missing_partition_count = 3;
        let (met, _) = evaluate(&AutomationCondition::OnMissing, &ctx);
        assert!(met);
        ctx.missing_partition_count = 0;
        let (met, _) = evaluate(&AutomationCondition::OnMissing, &ctx);
        assert!(!met);
    }

    #[test]
    fn freshness_violated_requires_policy() {
        let ctx = ctx_at("2026-05-17");
        let (met, _) = evaluate(&AutomationCondition::FreshnessViolated, &ctx);
        assert!(!met, "no policy => never violated");
    }

    #[test]
    fn freshness_violated_after_lag() {
        let mut ctx = ctx_at("2026-05-17");
        ctx.freshness_lag_seconds = Some(60);
        ctx.own_last_materialized_at = Some(ts("2026-05-17 10:00:00"));
        // 2h elapsed, 60s lag => violated.
        let (met, _) = evaluate(&AutomationCondition::FreshnessViolated, &ctx);
        assert!(met);
    }

    #[test]
    fn in_progress_atom() {
        let mut ctx = ctx_at("2026-05-17");
        ctx.in_progress = true;
        let (met, _) = evaluate(&AutomationCondition::InProgress, &ctx);
        assert!(met);
    }

    #[test]
    fn and_or_not_combinations() {
        let mut ctx = ctx_at("2026-05-17");
        ctx.upstream_max_materialized_at = Some(ts("2026-05-17 10:00:00"));
        ctx.own_last_materialized_at = Some(ts("2026-05-17 09:00:00"));
        ctx.in_progress = false;
        // eager AND NOT in_progress => true
        let cond = parse_automation("eager AND NOT in_progress()").unwrap();
        let (met, _) = evaluate(&cond, &ctx);
        assert!(met);
        // flip in_progress to true => false
        ctx.in_progress = true;
        let (met, _) = evaluate(&cond, &ctx);
        assert!(!met);
    }

    #[test]
    fn or_short_circuits() {
        let mut ctx = ctx_at("2026-05-17");
        ctx.missing_partition_count = 1;
        // First branch true => no need to check second.
        let cond = parse_automation("on_missing() OR in_progress()").unwrap();
        let (met, _) = evaluate(&cond, &ctx);
        assert!(met);
    }

    #[test]
    fn throttle_short_circuits_before_condition() {
        let mut ctx = ctx_at("2026-05-17");
        ctx.upstream_max_materialized_at = Some(ts("2026-05-17 11:30:00"));
        ctx.own_last_materialized_at = Some(ts("2026-05-17 11:50:00"));
        ctx.target_lag_seconds = Some(3600); // 1h throttle; 10min since last
                                             // eager would otherwise return true; throttle should block.
        let cond = AutomationCondition::Eager;
        let (met, reason) = evaluate(&cond, &ctx);
        assert!(!met);
        assert!(reason.contains("throttled"), "reason: {}", reason);
    }

    #[test]
    fn throttle_passes_after_lag_expires() {
        let mut ctx = ctx_at("2026-05-17");
        ctx.upstream_max_materialized_at = Some(ts("2026-05-17 11:50:00"));
        ctx.own_last_materialized_at = Some(ts("2026-05-17 10:00:00"));
        ctx.target_lag_seconds = Some(60);
        let cond = AutomationCondition::Eager;
        let (met, reason) = evaluate(&cond, &ctx);
        assert!(met, "expected pass, reason: {}", reason);
    }

    // -------------------- on_interval --------------------

    #[test]
    fn parses_on_interval_minutes() {
        let c = parse_automation("on_interval(\"5min\")").unwrap();
        assert_eq!(c, AutomationCondition::OnInterval(IntervalUnit::Minutes(5)));
    }

    #[test]
    fn parses_on_interval_days() {
        let c = parse_automation("on_interval(\"1d\")").unwrap();
        assert_eq!(c, AutomationCondition::OnInterval(IntervalUnit::Daily));
    }

    #[test]
    fn rejects_on_interval_unknown_unit() {
        assert!(parse_automation("on_interval(\"0s\")").is_err());
    }

    #[test]
    fn rejects_on_interval_no_arg() {
        assert!(parse_automation("on_interval()").is_err());
    }

    #[test]
    fn on_interval_fires_when_never_materialized() {
        let mut ctx = ctx_at("2026-05-17");
        // No stored intervals, no start configured — falls back to floor(now)-1 step,
        // so the most recent complete daily interval is missing.
        ctx.interval_start_ts = Some(
            ts("2026-05-16 00:00:00").and_utc().timestamp(),
        );
        let cond = parse_automation("on_interval(\"1d\")").unwrap();
        let (met, reason) = evaluate(&cond, &ctx);
        assert!(met, "never materialized should fire: {}", reason);
    }

    #[test]
    fn on_interval_fires_when_interval_elapsed() {
        let mut ctx = ctx_at("2026-05-18");
        // last materialization was on 2026-05-16; interval_start_ts anchors from
        // 2026-05-16, so the 2026-05-17 daily slot is missing => fires.
        ctx.own_last_materialized_at = Some(ts("2026-05-16 11:00:00"));
        ctx.interval_start_ts = Some(ts("2026-05-16 00:00:00").and_utc().timestamp());
        let cond = parse_automation("on_interval(\"1d\")").unwrap();
        let (met, reason) = evaluate(&cond, &ctx);
        assert!(met, "daily interval elapsed should fire: {}", reason);
    }

    #[test]
    fn on_interval_does_not_fire_when_up_to_date() {
        use crate::interval::Interval;
        let mut ctx = ctx_at("2026-05-17");
        // The only complete daily interval before now (2026-05-17 12:00) is
        // 2026-05-16. Mark it as stored => nothing missing.
        let day_start = ts("2026-05-16 00:00:00").and_utc().timestamp();
        let day_end = ts("2026-05-17 00:00:00").and_utc().timestamp();
        ctx.stored_intervals = vec![(day_start, day_end)];
        ctx.interval_start_ts = Some(day_start);
        let cond = parse_automation("on_interval(\"1d\")").unwrap();
        let (met, reason) = evaluate(&cond, &ctx);
        assert!(!met, "all intervals up to date should NOT fire: {}", reason);
    }

    #[test]
    fn on_interval_dsl_roundtrip() {
        let src = "on_interval(\"daily\")";
        let c = parse_automation(src).unwrap();
        let s = c.serialize_dsl();
        let c2 = parse_automation(&s).unwrap();
        assert_eq!(c, c2);
    }

    #[test]
    fn on_interval_not_wall_clock_aligned() {
        // on_interval is data-driven (which intervals are missing?), not
        // wall-clock aligned. Firing at 3am vs noon does not matter.
        let mut ctx = EvalContext::at(ts("2026-05-17 03:00:00"));
        // Anchor from 2026-05-15; 2026-05-16 daily slot is complete and missing.
        ctx.interval_start_ts = Some(ts("2026-05-15 00:00:00").and_utc().timestamp());
        let cond = parse_automation("on_interval(\"1d\")").unwrap();
        let (met, reason) = evaluate(&cond, &ctx);
        assert!(met, "daily interval should fire regardless of wall-clock hour: {}", reason);
    }

    #[test]
    fn on_cron_ticks_when_tick_in_window() {
        // Every minute. anchor=11:00, now=12:00 => has ticked.
        let mut ctx = ctx_at("2026-05-17");
        ctx.own_last_materialized_at = Some(ts("2026-05-17 11:00:00"));
        let cond = parse_automation("on_cron(\"* * * * *\")").unwrap();
        let (met, _) = evaluate(&cond, &ctx);
        assert!(met);
    }

    #[test]
    fn on_cron_no_tick_yet() {
        // 6am daily; anchor today after 6am, now also after 6am, no new tick.
        let now = ts("2026-05-17 12:00:00");
        let mut ctx = EvalContext::at(now);
        ctx.own_last_materialized_at = Some(ts("2026-05-17 06:30:00"));
        let cond = parse_automation("on_cron(\"0 6 * * *\")").unwrap();
        let (met, _) = evaluate(&cond, &ctx);
        assert!(!met);
    }

    // Sanity check that NaiveDate parsing helper works (and so the test
    // module actually links against chrono::NaiveDate without warnings).
    #[test]
    fn naive_date_helper_lives() {
        let _ = NaiveDate::from_ymd_opt(2026, 5, 17).unwrap();
    }
}
