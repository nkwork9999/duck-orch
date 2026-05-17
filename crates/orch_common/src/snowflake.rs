// Phase 17: Snowflake-style `CREATE DYNAMIC TABLE / ASSET` parser.
//
// duckOrch's primary surface for dynamic-asset creation is the pragma
// `orch_create_dynamic_asset(name, target_lag, sql)`. This module turns a
// Snowflake-style multi-statement SQL file into a list of `(name, target_lag,
// sql)` tuples so the CLI can `migrate-from-snowflake <dump>` by invoking
// the pragma per parsed block.
//
// Grammar (case-insensitive, whitespace/newlines tolerant):
//   CREATE [OR REPLACE] DYNAMIC (TABLE | ASSET) <qualified_name>
//     [TARGET_LAG = '<duration>']
//     [LAG = '<duration>']                    -- accepted alias
//     [WAREHOUSE = '<name>']                  -- ignored (Snowflake-only)
//     [REFRESH_MODE = '...']                  -- ignored (Snowflake-only)
//     [INITIALIZE = '...']                    -- ignored (Snowflake-only)
//     [COMMENT = '...']                       -- captured but not stored
//   AS
//     <SELECT-or-CTE body up to `;` at top level>;
//
// The parser is deliberately tolerant — it skips over Snowflake-isms we
// don't model and just picks out the three things we need. Strings,
// `/* ... */` comments and `-- ...` line comments are respected when
// scanning for the terminating `;`.

use serde::{Deserialize, Serialize};

/// One parsed Snowflake-style dynamic-asset block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnowflakeDynamicBlock {
    /// Fully-qualified asset name as written, e.g. `analytics.user_stats`.
    pub name: String,
    /// `TARGET_LAG` / `LAG` value as written (e.g. `5 minutes`). `None`
    /// when the user omitted it — caller decides the default.
    pub target_lag: Option<String>,
    /// The `AS <body>` body, trimmed and with the trailing `;` removed.
    pub sql_body: String,
}

/// Parse all dynamic-asset blocks from a single SQL dump string.
///
/// Unknown statements (regular DDL, comments, blank lines) are silently
/// skipped — the caller is migrating a real Snowflake dump and only wants
/// the dynamic tables.
pub fn parse_snowflake_dump(src: &str) -> Vec<SnowflakeDynamicBlock> {
    let mut out = Vec::new();
    let bytes = src.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Skip leading whitespace + comments.
        i = skip_ws_comments(bytes, i);
        if i >= bytes.len() {
            break;
        }
        // Locate the next `;` at top level — that's the end of this statement.
        let stmt_end = find_stmt_end(bytes, i);
        let stmt = &src[i..stmt_end];
        if let Some(block) = parse_one_create_dynamic(stmt) {
            out.push(block);
        }
        i = if stmt_end < bytes.len() {
            stmt_end + 1
        } else {
            stmt_end
        };
    }
    out
}

/// Parse exactly one `CREATE DYNAMIC TABLE / ASSET ... AS ...` statement.
/// Returns `None` for anything that doesn't look like one. Public so the
/// pragma layer can use it on a single-statement input too.
pub fn parse_one_create_dynamic(stmt: &str) -> Option<SnowflakeDynamicBlock> {
    let trimmed = stmt.trim_start();
    // CREATE [OR REPLACE] DYNAMIC (TABLE | ASSET) <name> ...
    let after_create = strip_keyword_ci(trimmed, "CREATE")?;
    let after_or_replace = match strip_keyword_ci(after_create.trim_start(), "OR") {
        Some(rest) => strip_keyword_ci(rest.trim_start(), "REPLACE")?,
        None => after_create,
    };
    let after_dynamic = strip_keyword_ci(after_or_replace.trim_start(), "DYNAMIC")?;
    let after_kind = strip_keyword_ci(after_dynamic.trim_start(), "TABLE")
        .or_else(|| strip_keyword_ci(after_dynamic.trim_start(), "ASSET"))?;
    let after_kind = after_kind.trim_start();
    if after_kind.is_empty() {
        return None;
    }

    // Asset name: read until whitespace.
    let (name, after_name) = read_identifier(after_kind)?;
    if name.is_empty() {
        return None;
    }

    // Walk the options soup until we hit a top-level `AS`. Capture
    // `TARGET_LAG` / `LAG` values along the way.
    let mut target_lag: Option<String> = None;
    let rest_after_options = match scan_options(after_name, &mut target_lag) {
        Some(s) => s,
        None => return None,
    };

    let body = rest_after_options.trim();
    if body.is_empty() {
        return None;
    }
    let body = body.trim_end_matches(';').trim().to_string();
    Some(SnowflakeDynamicBlock {
        name: name.to_string(),
        target_lag,
        sql_body: body,
    })
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn skip_ws_comments(bytes: &[u8], mut i: usize) -> usize {
    loop {
        while i < bytes.len() && (bytes[i] as char).is_ascii_whitespace() {
            i += 1;
        }
        if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 < bytes.len() {
                i += 2;
            }
            continue;
        }
        return i;
    }
}

fn find_stmt_end(bytes: &[u8], start: usize) -> usize {
    let mut i = start;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if c == b'"' {
            i += 1;
            while i < bytes.len() && bytes[i] != b'"' {
                i += 1;
            }
            if i < bytes.len() {
                i += 1;
            }
            continue;
        }
        if i + 1 < bytes.len() && c == b'-' && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if i + 1 < bytes.len() && c == b'/' && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 < bytes.len() {
                i += 2;
            }
            continue;
        }
        if c == b';' {
            return i;
        }
        i += 1;
    }
    bytes.len()
}

fn strip_keyword_ci<'a>(src: &'a str, kw: &str) -> Option<&'a str> {
    if src.len() < kw.len() {
        return None;
    }
    let head = &src[..kw.len()];
    if !head.eq_ignore_ascii_case(kw) {
        return None;
    }
    let after = &src[kw.len()..];
    // Keyword must be followed by whitespace, EOF, `(` or `=`.
    match after.chars().next() {
        None => Some(after),
        Some(c) if c.is_ascii_whitespace() || c == '(' || c == '=' => Some(after),
        _ => None,
    }
}

fn read_identifier(src: &str) -> Option<(&str, &str)> {
    let s = src.trim_start();
    if s.is_empty() {
        return None;
    }
    let mut end = 0;
    for (i, c) in s.char_indices() {
        if c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '"' {
            end = i + c.len_utf8();
        } else {
            break;
        }
    }
    if end == 0 {
        return None;
    }
    let name = s[..end].trim_matches('"');
    Some((name, &s[end..]))
}

/// Scan the options between `<name>` and the body-introducing `AS`. Captures
/// `TARGET_LAG` / `LAG` if present. Returns the slice after `AS`.
fn scan_options<'a>(src: &'a str, target_lag: &mut Option<String>) -> Option<&'a str> {
    let mut s = src.trim_start();
    loop {
        s = s.trim_start();
        // End of options?
        if let Some(after) = strip_keyword_ci(s, "AS") {
            return Some(after.trim_start());
        }
        if s.is_empty() {
            return None;
        }
        // TARGET_LAG = '...'
        if let Some(after_kw) = strip_keyword_ci(s, "TARGET_LAG").or_else(|| strip_keyword_ci(s, "LAG"))
        {
            let after_eq = consume_eq(after_kw)?;
            let (val, rest) = read_quoted_or_word(after_eq.trim_start())?;
            if target_lag.is_none() {
                *target_lag = Some(val);
            }
            s = rest;
            continue;
        }
        // Other Snowflake options we ignore: WAREHOUSE / REFRESH_MODE /
        // INITIALIZE / COMMENT / CLUSTER BY / etc. They follow either
        // `<KW> = <value>` or `<KW> <value>` shape. Best-effort skip.
        if let Some(after_id) = skip_unknown_option(s) {
            s = after_id;
            continue;
        }
        // Couldn't make progress — bail.
        return None;
    }
}

fn consume_eq(src: &str) -> Option<&str> {
    let s = src.trim_start();
    if let Some(rest) = s.strip_prefix('=') {
        return Some(rest);
    }
    Some(s) // Some Snowflake variants omit `=`.
}

fn read_quoted_or_word(src: &str) -> Option<(String, &str)> {
    let s = src.trim_start();
    if let Some(rest) = s.strip_prefix('\'') {
        let mut out = String::new();
        let mut idx = 0;
        let bytes = rest.as_bytes();
        while idx < bytes.len() {
            let b = bytes[idx];
            if b == b'\'' {
                if idx + 1 < bytes.len() && bytes[idx + 1] == b'\'' {
                    out.push('\'');
                    idx += 2;
                    continue;
                }
                return Some((out, &rest[idx + 1..]));
            }
            out.push(b as char);
            idx += 1;
        }
        return None;
    }
    // Bare word (e.g. `5min`).
    let mut end = 0;
    for (i, c) in s.char_indices() {
        if c.is_ascii_whitespace() || c == ',' || c == ';' {
            break;
        }
        end = i + c.len_utf8();
    }
    if end == 0 {
        return None;
    }
    Some((s[..end].to_string(), &s[end..]))
}

fn skip_unknown_option(src: &str) -> Option<&str> {
    let s = src.trim_start();
    // Read the keyword.
    let mut end = 0;
    for (i, c) in s.char_indices() {
        if c.is_ascii_alphanumeric() || c == '_' {
            end = i + c.len_utf8();
        } else {
            break;
        }
    }
    if end == 0 {
        return None;
    }
    let after = s[end..].trim_start();
    // `= <value>` or single value.
    if let Some(rest) = after.strip_prefix('=') {
        let (_, after_val) = read_quoted_or_word(rest.trim_start())?;
        return Some(after_val);
    }
    // Otherwise treat next token as the value (best-effort).
    if !after.is_empty() && !after.starts_with(|c: char| c.is_ascii_whitespace()) {
        let (_, after_val) = read_quoted_or_word(after)?;
        return Some(after_val);
    }
    Some(after)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_dynamic_table() {
        let src = "CREATE DYNAMIC TABLE analytics.daily_total \
                   TARGET_LAG = '5 minutes' \
                   AS \
                   SELECT date, SUM(amount) AS total FROM raw.events GROUP BY date;";
        let blocks = parse_snowflake_dump(src);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].name, "analytics.daily_total");
        assert_eq!(blocks[0].target_lag.as_deref(), Some("5 minutes"));
        assert!(blocks[0].sql_body.starts_with("SELECT"));
        assert!(!blocks[0].sql_body.ends_with(';'));
    }

    #[test]
    fn parses_dynamic_asset_keyword() {
        let src = "CREATE DYNAMIC ASSET sales.totals AS SELECT 1;";
        let blocks = parse_snowflake_dump(src);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].name, "sales.totals");
        assert!(blocks[0].target_lag.is_none());
    }

    #[test]
    fn parses_or_replace_and_multiple_blocks() {
        let src = "
            CREATE OR REPLACE DYNAMIC TABLE a.b
              TARGET_LAG = '1 hour'
              AS SELECT * FROM raw;

            -- a stray DDL we should skip
            CREATE TABLE noise(x INT);

            CREATE DYNAMIC TABLE c.d
              TARGET_LAG = '30s'
              AS SELECT 1;
        ";
        let blocks = parse_snowflake_dump(src);
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].name, "a.b");
        assert_eq!(blocks[0].target_lag.as_deref(), Some("1 hour"));
        assert_eq!(blocks[1].name, "c.d");
        assert_eq!(blocks[1].target_lag.as_deref(), Some("30s"));
    }

    #[test]
    fn ignores_warehouse_and_comment_options() {
        let src = "CREATE DYNAMIC TABLE x.y \
                   TARGET_LAG = '1h' \
                   WAREHOUSE = 'wh1' \
                   REFRESH_MODE = 'AUTO' \
                   AS SELECT 1;";
        let blocks = parse_snowflake_dump(src);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].name, "x.y");
        assert_eq!(blocks[0].target_lag.as_deref(), Some("1h"));
    }

    #[test]
    fn handles_quoted_strings_in_body() {
        let src = "CREATE DYNAMIC TABLE x AS SELECT 'a;b' AS s FROM raw;";
        let blocks = parse_snowflake_dump(src);
        assert_eq!(blocks.len(), 1);
        assert!(blocks[0].sql_body.contains("'a;b'"));
    }

    #[test]
    fn handles_line_comments_in_body() {
        let src = "CREATE DYNAMIC TABLE x AS \n\
                   -- this; semicolon is in a comment\n\
                   SELECT 1;";
        let blocks = parse_snowflake_dump(src);
        assert_eq!(blocks.len(), 1);
        assert!(blocks[0].sql_body.contains("SELECT 1"));
    }

    #[test]
    fn case_insensitive_keywords() {
        let src = "create dynamic table z target_lag = '2m' as select 1;";
        let blocks = parse_snowflake_dump(src);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].name, "z");
        assert_eq!(blocks[0].target_lag.as_deref(), Some("2m"));
    }

    #[test]
    fn fixture_snowflake_dump_three_blocks() {
        // The shipped fixture `tests/fixtures/snowflake_dump.sql` is what the
        // CLI's `duck-orch dynamic create-from-sql` chews on. Lock in the
        // expected (name, target_lag) tuples so a future tweak to the parser
        // can't silently regress it.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/snowflake_dump.sql");
        let src = std::fs::read_to_string(&path).expect("read fixture");
        let blocks = parse_snowflake_dump(&src);
        assert_eq!(blocks.len(), 3, "expected 3 dynamic-asset blocks");
        assert_eq!(blocks[0].name, "analytics.daily_total");
        assert_eq!(blocks[0].target_lag.as_deref(), Some("5 minutes"));
        assert!(blocks[0].sql_body.starts_with("SELECT"));
        assert_eq!(blocks[1].name, "analytics.region_sum");
        assert_eq!(blocks[1].target_lag.as_deref(), Some("1 hour"));
        assert_eq!(blocks[2].name, "analytics.hourly_users");
        assert_eq!(blocks[2].target_lag.as_deref(), Some("15m"));
    }
}
