// HTTP source (P2).
//
// Fetching is deliberately the thinnest layer that still survives real APIs:
// four pagination shapes, retry with backoff, and a hard page ceiling. Each
// page is written to its own `part-NNNN.jsonl` before anything is parsed
// further, so a failed run can be inspected — and so the normalization path
// downstream is the same one file sources take.
//
// Credentials never appear here as configuration: the caller resolves them
// (from a DuckDB secret) and passes finished header values in.

use std::fs;
use std::io::Write;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

fn default_max_pages() -> usize {
    100
}
fn default_timeout() -> u64 {
    60
}
fn default_retries() -> usize {
    3
}

#[derive(Debug, Clone, Deserialize)]
pub struct FetchSpec {
    pub url: String,
    pub out_dir: String,
    /// `[[name, value], ...]` — already-resolved request headers.
    #[serde(default)]
    pub headers: Vec<Vec<String>>,
    /// none | page | offset | cursor | link
    #[serde(default)]
    pub paginate: String,
    /// Dotted path to the array of records, e.g. `data` or `result.items`.
    /// Empty means the body itself is the array.
    #[serde(default)]
    pub records_path: String,
    /// Dotted path to the next cursor, for `paginate = cursor`.
    #[serde(default)]
    pub cursor_path: String,
    /// Query parameter the cursor is sent back in.
    #[serde(default)]
    pub cursor_param: String,
    /// Query parameter carrying the page number or row offset.
    #[serde(default)]
    pub page_param: String,
    #[serde(default)]
    pub start_page: i64,
    /// Cursor stored by the previous run, resumed on this one.
    #[serde(default)]
    pub cursor_in: String,
    #[serde(default = "default_max_pages")]
    pub max_pages: usize,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    #[serde(default = "default_retries")]
    pub retries: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct PageInfo {
    pub page: usize,
    pub url: String,
    pub status: u16,
    pub bytes: usize,
    pub records: usize,
    pub file: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FetchOut {
    pub files: Vec<String>,
    pub pages: Vec<PageInfo>,
    pub records: usize,
    pub cursor_out: String,
    /// True when the page ceiling stopped the run before the source was
    /// exhausted — the caller must say so rather than imply completeness.
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    None,
    Page,
    Offset,
    Cursor,
    Link,
}

fn parse_mode(s: &str) -> Result<Mode, String> {
    match s.trim().to_ascii_lowercase().as_str() {
        "" | "none" => Ok(Mode::None),
        "page" => Ok(Mode::Page),
        "offset" => Ok(Mode::Offset),
        "cursor" => Ok(Mode::Cursor),
        "link" => Ok(Mode::Link),
        other => Err(format!(
            "unknown pagination '{}' (expected none, page, offset, cursor or link)",
            other
        )),
    }
}

pub fn fetch(spec: &FetchSpec) -> Result<FetchOut, String> {
    let mode = parse_mode(&spec.paginate)?;
    validate(spec, mode)?;

    fs::create_dir_all(&spec.out_dir)
        .map_err(|e| format!("cannot create {}: {}", spec.out_dir, e))?;

    let mut files = Vec::new();
    let mut pages = Vec::new();
    let mut total = 0usize;
    let mut cursor = spec.cursor_in.clone();
    let mut page_no = if spec.start_page != 0 { spec.start_page } else { 1 };
    let mut offset = 0i64;
    let mut next_url: Option<String> = None;
    let mut truncated = false;

    for idx in 0..spec.max_pages {
        let url = match (mode, &next_url) {
            (Mode::Link, Some(u)) => u.clone(),
            (Mode::Page, _) if idx > 0 || spec.start_page != 0 => {
                add_param(&spec.url, &spec.page_param, &page_no.to_string())
            }
            (Mode::Offset, _) if idx > 0 => {
                add_param(&spec.url, &spec.page_param, &offset.to_string())
            }
            (Mode::Cursor, _) if !cursor.is_empty() => {
                add_param(&spec.url, &spec.cursor_param, &cursor)
            }
            _ => spec.url.clone(),
        };

        let (status, body, link_header) = request(&url, spec)?;
        let value: Value = serde_json::from_str(&body)
            .map_err(|e| format!("{} returned a body that is not JSON: {}", url, e))?;
        let records = extract_records(&value, &spec.records_path)?;

        let file = PathBuf::from(&spec.out_dir).join(format!("part-{:04}.jsonl", idx));
        write_jsonl(&file, &records)?;
        let file_str = file.to_string_lossy().to_string();

        pages.push(PageInfo {
            page: idx,
            url: url.clone(),
            status,
            bytes: body.len(),
            records: records.len(),
            file: file_str.clone(),
        });
        files.push(file_str);
        total += records.len();

        // Decide whether there is another page.
        match mode {
            Mode::None => break,
            Mode::Page => {
                if records.is_empty() {
                    break;
                }
                page_no += 1;
            }
            Mode::Offset => {
                if records.is_empty() {
                    break;
                }
                offset += records.len() as i64;
            }
            Mode::Cursor => {
                let next = dig(&value, &spec.cursor_path)
                    .and_then(scalar_to_string)
                    .unwrap_or_default();
                if next.is_empty() || next == cursor {
                    cursor = next;
                    break;
                }
                cursor = next;
            }
            Mode::Link => match parse_link_next(link_header.as_deref()) {
                Some(u) => next_url = Some(u),
                None => break,
            },
        }

        if idx + 1 == spec.max_pages {
            truncated = true;
        }
    }

    Ok(FetchOut {
        files,
        pages,
        records: total,
        cursor_out: cursor,
        truncated,
    })
}

fn validate(spec: &FetchSpec, mode: Mode) -> Result<(), String> {
    match mode {
        Mode::Cursor => {
            if spec.cursor_path.is_empty() || spec.cursor_param.is_empty() {
                return Err(
                    "cursor pagination needs both cursor_path and cursor_param".to_string()
                );
            }
        }
        Mode::Page | Mode::Offset => {
            if spec.page_param.is_empty() {
                return Err("page/offset pagination needs page_param".to_string());
            }
        }
        _ => {}
    }
    if spec.max_pages == 0 {
        return Err("max_pages must be at least 1".to_string());
    }
    Ok(())
}

#[cfg(not(test))]
fn request(url: &str, spec: &FetchSpec) -> Result<(u16, String, Option<String>), String> {
    let mut last_err = String::new();
    for attempt in 0..=spec.retries {
        if attempt > 0 {
            std::thread::sleep(std::time::Duration::from_millis(250 * (1 << (attempt - 1)) as u64));
        }
        let agent = ureq::AgentBuilder::new()
            .timeout(std::time::Duration::from_secs(spec.timeout_secs))
            .build();
        let mut req = agent.get(url);
        for h in &spec.headers {
            if h.len() == 2 {
                req = req.set(&h[0], &h[1]);
            }
        }
        match req.call() {
            Ok(resp) => {
                let status = resp.status();
                let link = resp.header("link").map(|s| s.to_string());
                let body = resp
                    .into_string()
                    .map_err(|e| format!("{}: cannot read body: {}", url, e))?;
                return Ok((status, body, link));
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                // 4xx is the caller's fault and will not improve on retry.
                if code < 500 {
                    return Err(format!("{} returned HTTP {}: {}", url, code, truncate(&body)));
                }
                last_err = format!("{} returned HTTP {}: {}", url, code, truncate(&body));
            }
            Err(e) => {
                last_err = format!("{} failed: {}", url, e);
            }
        }
    }
    Err(last_err)
}

// Tests must not reach the network; they drive the paging logic through a
// scripted responder instead.
#[cfg(test)]
fn request(url: &str, _spec: &FetchSpec) -> Result<(u16, String, Option<String>), String> {
    tests::scripted(url)
}

fn truncate(s: &str) -> String {
    if s.chars().count() > 200 {
        let head: String = s.chars().take(200).collect();
        format!("{}…", head)
    } else {
        s.to_string()
    }
}

fn write_jsonl(path: &PathBuf, records: &[Value]) -> Result<(), String> {
    let mut f = fs::File::create(path).map_err(|e| format!("cannot write {:?}: {}", path, e))?;
    for r in records {
        let line = serde_json::to_string(r).map_err(|e| e.to_string())?;
        writeln!(f, "{}", line).map_err(|e| format!("cannot write {:?}: {}", path, e))?;
    }
    Ok(())
}

/// Pull the record array out of a response body.
fn extract_records(value: &Value, path: &str) -> Result<Vec<Value>, String> {
    let target = if path.trim().is_empty() {
        Some(value)
    } else {
        dig(value, path)
    };
    match target {
        Some(Value::Array(a)) => Ok(a.clone()),
        Some(Value::Null) | None => {
            if path.trim().is_empty() {
                Ok(Vec::new())
            } else {
                Err(format!("records_path '{}' is not present in the response", path))
            }
        }
        // A single object is a one-record page; common for detail endpoints.
        Some(other) => Ok(vec![other.clone()]),
    }
}

/// Walk a dotted path (`meta.next`) through a JSON object.
fn dig<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let cleaned = path.trim().trim_start_matches("$.").trim_start_matches('$');
    if cleaned.is_empty() {
        return Some(value);
    }
    let mut cur = value;
    for part in cleaned.split('.') {
        if part.is_empty() {
            continue;
        }
        cur = cur.get(part)?;
    }
    Some(cur)
}

fn scalar_to_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Null => Some(String::new()),
        _ => None,
    }
}

/// `<https://api/x?page=2>; rel="next", <...>; rel="last"` → the next URL.
fn parse_link_next(header: Option<&str>) -> Option<String> {
    let h = header?;
    for part in h.split(',') {
        if !part.to_ascii_lowercase().contains("rel=\"next\"")
            && !part.to_ascii_lowercase().contains("rel=next")
        {
            continue;
        }
        let start = part.find('<')?;
        let end = part.find('>')?;
        if end > start + 1 {
            return Some(part[start + 1..end].to_string());
        }
    }
    None
}

/// Append a query parameter, respecting an existing query string.
pub fn add_param(url: &str, key: &str, value: &str) -> String {
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{}{}{}={}", url, sep, encode(key), encode(value))
}

fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        let c = *b as char;
        if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~') {
            out.push(c);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::cell::RefCell;

    thread_local! {
        static SCRIPT: RefCell<Vec<(String, String, Option<String>)>> =
            const { RefCell::new(Vec::new()) };
        static SEEN: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    }

    /// Queue up the responses a test expects to be requested, in order.
    fn script(responses: Vec<(&str, Option<&str>)>) {
        SCRIPT.with(|s| {
            *s.borrow_mut() = responses
                .into_iter()
                .map(|(body, link)| {
                    (String::new(), body.to_string(), link.map(|l| l.to_string()))
                })
                .collect()
        });
        SEEN.with(|s| s.borrow_mut().clear());
    }

    pub(crate) fn scripted(url: &str) -> Result<(u16, String, Option<String>), String> {
        SEEN.with(|s| s.borrow_mut().push(url.to_string()));
        SCRIPT.with(|s| {
            let mut q = s.borrow_mut();
            if q.is_empty() {
                return Err(format!("no scripted response left for {}", url));
            }
            let (_, body, link) = q.remove(0);
            Ok((200, body, link))
        })
    }

    fn urls() -> Vec<String> {
        SEEN.with(|s| s.borrow().clone())
    }

    fn spec(dir: &str) -> FetchSpec {
        FetchSpec {
            url: "https://api.example.com/orders".into(),
            out_dir: dir.into(),
            headers: vec![],
            paginate: String::new(),
            records_path: String::new(),
            cursor_path: String::new(),
            cursor_param: String::new(),
            page_param: String::new(),
            start_page: 0,
            cursor_in: String::new(),
            max_pages: 10,
            timeout_secs: 5,
            retries: 0,
        }
    }

    fn tmpdir(name: &str) -> String {
        let mut p = std::env::temp_dir();
        p.push(format!("orch_ingest_test_{}", name));
        let _ = fs::remove_dir_all(&p);
        p.to_string_lossy().to_string()
    }

    #[test]
    fn single_page_array_body() {
        let dir = tmpdir("single");
        script(vec![(r#"[{"id":1},{"id":2}]"#, None)]);
        let out = fetch(&spec(&dir)).unwrap();
        assert_eq!(out.records, 2);
        assert_eq!(out.files.len(), 1);
        let text = fs::read_to_string(&out.files[0]).unwrap();
        assert_eq!(text.lines().count(), 2);
        assert!(!out.truncated);
    }

    #[test]
    fn records_path_pulls_the_array_out() {
        let dir = tmpdir("path");
        script(vec![(r#"{"data":{"items":[{"id":1}]}}"#, None)]);
        let mut s = spec(&dir);
        s.records_path = "data.items".into();
        let out = fetch(&s).unwrap();
        assert_eq!(out.records, 1);
    }

    #[test]
    fn missing_records_path_is_an_error() {
        let dir = tmpdir("missingpath");
        script(vec![(r#"{"data":[]}"#, None)]);
        let mut s = spec(&dir);
        s.records_path = "nope".into();
        assert!(fetch(&s).unwrap_err().contains("records_path"));
    }

    #[test]
    fn page_mode_stops_on_the_first_empty_page() {
        let dir = tmpdir("page");
        script(vec![
            (r#"[{"id":1}]"#, None),
            (r#"[{"id":2}]"#, None),
            ("[]", None),
        ]);
        let mut s = spec(&dir);
        s.paginate = "page".into();
        s.page_param = "page".into();
        let out = fetch(&s).unwrap();
        assert_eq!(out.records, 2);
        assert_eq!(out.pages.len(), 3);
        assert!(urls()[1].contains("page=2"));
        assert!(urls()[2].contains("page=3"));
    }

    #[test]
    fn offset_mode_advances_by_record_count() {
        let dir = tmpdir("offset");
        script(vec![(r#"[{"id":1},{"id":2}]"#, None), ("[]", None)]);
        let mut s = spec(&dir);
        s.paginate = "offset".into();
        s.page_param = "offset".into();
        let out = fetch(&s).unwrap();
        assert_eq!(out.records, 2);
        assert!(urls()[1].contains("offset=2"));
    }

    #[test]
    fn cursor_mode_follows_and_reports_the_last_cursor() {
        let dir = tmpdir("cursor");
        script(vec![
            (r#"{"data":[{"id":1}],"meta":{"next":"c2"}}"#, None),
            (r#"{"data":[{"id":2}],"meta":{"next":null}}"#, None),
        ]);
        let mut s = spec(&dir);
        s.paginate = "cursor".into();
        s.records_path = "data".into();
        s.cursor_path = "meta.next".into();
        s.cursor_param = "cursor".into();
        let out = fetch(&s).unwrap();
        assert_eq!(out.records, 2);
        assert!(urls()[1].contains("cursor=c2"));
        assert_eq!(out.cursor_out, "");
    }

    #[test]
    fn cursor_mode_resumes_from_a_stored_cursor() {
        let dir = tmpdir("resume");
        script(vec![(r#"{"data":[],"meta":{}}"#, None)]);
        let mut s = spec(&dir);
        s.paginate = "cursor".into();
        s.records_path = "data".into();
        s.cursor_path = "meta.next".into();
        s.cursor_param = "cursor".into();
        s.cursor_in = "saved".into();
        fetch(&s).unwrap();
        assert!(urls()[0].contains("cursor=saved"));
    }

    #[test]
    fn a_repeated_cursor_stops_the_loop() {
        let dir = tmpdir("samecursor");
        script(vec![
            (r#"{"data":[{"id":1}],"meta":{"next":"c1"}}"#, None),
            (r#"{"data":[{"id":2}],"meta":{"next":"c1"}}"#, None),
        ]);
        let mut s = spec(&dir);
        s.paginate = "cursor".into();
        s.records_path = "data".into();
        s.cursor_path = "meta.next".into();
        s.cursor_param = "cursor".into();
        s.cursor_in = "c1".into();
        let out = fetch(&s).unwrap();
        assert_eq!(out.pages.len(), 1);
    }

    #[test]
    fn link_mode_follows_the_next_relation() {
        let dir = tmpdir("link");
        script(vec![
            (
                r#"[{"id":1}]"#,
                Some("<https://api.example.com/orders?page=2>; rel=\"next\""),
            ),
            (r#"[{"id":2}]"#, None),
        ]);
        let mut s = spec(&dir);
        s.paginate = "link".into();
        let out = fetch(&s).unwrap();
        assert_eq!(out.records, 2);
        assert_eq!(urls()[1], "https://api.example.com/orders?page=2");
    }

    #[test]
    fn the_page_ceiling_is_reported_not_hidden() {
        let dir = tmpdir("ceiling");
        script(vec![(r#"[{"id":1}]"#, None), (r#"[{"id":2}]"#, None)]);
        let mut s = spec(&dir);
        s.paginate = "page".into();
        s.page_param = "page".into();
        s.max_pages = 2;
        let out = fetch(&s).unwrap();
        assert!(out.truncated);
        assert_eq!(out.pages.len(), 2);
    }

    #[test]
    fn cursor_mode_needs_its_configuration() {
        let dir = tmpdir("badcursor");
        let mut s = spec(&dir);
        s.paginate = "cursor".into();
        assert!(fetch(&s).unwrap_err().contains("cursor_path"));
    }

    #[test]
    fn unknown_pagination_is_refused() {
        let dir = tmpdir("badmode");
        let mut s = spec(&dir);
        s.paginate = "spiral".into();
        assert!(fetch(&s).unwrap_err().contains("unknown pagination"));
    }

    #[test]
    fn query_parameters_respect_an_existing_query_string() {
        assert_eq!(add_param("http://a/b", "p", "1"), "http://a/b?p=1");
        assert_eq!(add_param("http://a/b?x=1", "p", "2"), "http://a/b?x=1&p=2");
        assert_eq!(add_param("http://a/b", "p", "a b&c"), "http://a/b?p=a%20b%26c");
    }

    #[test]
    fn link_header_parsing() {
        assert_eq!(
            parse_link_next(Some("<http://a?p=2>; rel=\"next\", <http://a?p=9>; rel=\"last\"")),
            Some("http://a?p=2".to_string())
        );
        assert_eq!(parse_link_next(Some("<http://a?p=9>; rel=\"last\"")), None);
        assert_eq!(parse_link_next(None), None);
    }

    #[test]
    fn a_single_object_body_is_one_record() {
        let dir = tmpdir("single_obj");
        script(vec![(r#"{"id":1}"#, None)]);
        let out = fetch(&spec(&dir)).unwrap();
        assert_eq!(out.records, 1, "a detail endpoint returns one record");
        assert_eq!(out.pages.len(), 1);
    }
}
