// duckorch_core — FFI facade.
//
// All `extern "C"` symbols live here. The actual logic is delegated to
// orch_common / orch_dag / orch_lineage / orch_runtime / orch_ol sub-crates.
// Keeping the FFI surface in one crate gives the C++ side a single static
// library to link against.

use std::panic::{catch_unwind, AssertUnwindSafe};

use orch_common::{
    ffi::{free_vec, leak_vec, read_str},
    Task,
};

fn write_out(s: String, out_ptr: *mut *mut u8, out_len: *mut usize) -> i32 {
    let bytes = s.into_bytes();
    let (p, l) = leak_vec(bytes);
    unsafe {
        *out_ptr = p;
        *out_len = l;
    }
    0
}

fn err_to_buf(msg: &str, out_ptr: *mut *mut u8, out_len: *mut usize) -> i32 {
    let bytes = orch_common::ffi::error_json(msg);
    let s = String::from_utf8(bytes).unwrap_or_default();
    write_out(s, out_ptr, out_len);
    -1
}

// ---------------------------------------------------------------------------
// Memory management
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn orch_string_free(ptr: *mut u8, len: usize) {
    unsafe { free_vec(ptr, len) }
}

// ---------------------------------------------------------------------------
// Hello world (Phase 0 sentinel)
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "C" fn orch_hello(
    name_ptr: *const u8,
    name_len: usize,
    out_buf: *mut u8,
    out_cap: usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let name = unsafe { read_str(name_ptr, name_len) };
        let g = format!("hello {} from duckorch_core", name);
        let bytes = g.as_bytes();
        let n = bytes.len().min(out_cap);
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf, n) };
        n as i32
    }))
    .unwrap_or(-1)
}

// ---------------------------------------------------------------------------
// Task parsing & directory loading (orch_runtime)
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "C" fn orch_parse_task(
    sql_ptr: *const u8,
    sql_len: usize,
    file_path_ptr: *const u8,
    file_path_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let sql = unsafe { read_str(sql_ptr, sql_len) };
        let fp = unsafe { read_str(file_path_ptr, file_path_len) };
        let fp_opt = if fp.is_empty() { None } else { Some(fp) };
        match orch_runtime::parse_sql_file(sql, fp_opt) {
            Ok(t) => {
                let json = serde_json::to_string(&t).unwrap_or_default();
                write_out(json, out_ptr, out_len)
            }
            Err(e) => err_to_buf(&e.to_string(), out_ptr, out_len),
        }
    }))
    .unwrap_or(-1)
}

#[unsafe(no_mangle)]
pub extern "C" fn orch_load_directory(
    path_ptr: *const u8,
    path_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let path = unsafe { read_str(path_ptr, path_len) };
        let mut tasks: Vec<Task> = Vec::new();
        let mut errors: Vec<String> = Vec::new();
        for entry in walkdir::WalkDir::new(path)
            .follow_links(true)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if !entry.file_type().is_file() {
                continue;
            }
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) != Some("sql") {
                continue;
            }
            let content = match std::fs::read_to_string(p) {
                Ok(c) => c,
                Err(e) => {
                    errors.push(format!("read {}: {}", p.display(), e));
                    continue;
                }
            };
            let path_str = p.display().to_string();
            match orch_runtime::parse_sql_file(&content, Some(&path_str)) {
                Ok(t) => tasks.push(t),
                Err(e) => errors.push(format!("{}: {}", path_str, e)),
            }
        }
        let result = serde_json::json!({
            "tasks": tasks,
            "errors": errors,
        });
        write_out(result.to_string(), out_ptr, out_len)
    }))
    .unwrap_or(-1)
}

// ---------------------------------------------------------------------------
// Lineage extraction (orch_lineage)
// ---------------------------------------------------------------------------

/// Extract column-level lineage. `schema_json` is an optional JSON object
/// mapping table_name -> [column_names], used to resolve `SELECT *`.
#[unsafe(no_mangle)]
pub extern "C" fn orch_extract_column_lineage(
    sql_ptr: *const u8,
    sql_len: usize,
    schema_json_ptr: *const u8,
    schema_json_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let sql = unsafe { read_str(sql_ptr, sql_len) };
        let schema_json = unsafe { read_str(schema_json_ptr, schema_json_len) };
        let schema: orch_lineage::column::SchemaMap = if schema_json.is_empty() {
            std::collections::HashMap::new()
        } else {
            serde_json::from_str(schema_json).unwrap_or_default()
        };
        let result = orch_lineage::column::extract_column_lineage(sql, &schema);
        write_out(
            serde_json::to_string(&result).unwrap_or_default(),
            out_ptr,
            out_len,
        )
    }))
    .unwrap_or(-1)
}

#[unsafe(no_mangle)]
pub extern "C" fn orch_extract_io(
    sql_ptr: *const u8,
    sql_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let sql = unsafe { read_str(sql_ptr, sql_len) };
        let (inputs, outputs) = orch_lineage::extract_io(sql);
        let result = serde_json::json!({"inputs": inputs, "outputs": outputs});
        write_out(result.to_string(), out_ptr, out_len)
    }))
    .unwrap_or(-1)
}

// ---------------------------------------------------------------------------
// DAG (orch_dag)
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "C" fn orch_build_dag(
    tasks_json_ptr: *const u8,
    tasks_json_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let json = unsafe { read_str(tasks_json_ptr, tasks_json_len) };
        let tasks: Vec<Task> = match serde_json::from_str(json) {
            Ok(t) => t,
            Err(e) => return err_to_buf(&format!("invalid tasks json: {}", e), out_ptr, out_len),
        };
        match orch_dag::build_dag(&tasks) {
            Ok(r) => write_out(
                serde_json::to_string(&r).unwrap_or_default(),
                out_ptr,
                out_len,
            ),
            Err(e) => err_to_buf(&e.message, out_ptr, out_len),
        }
    }))
    .unwrap_or(-1)
}

#[unsafe(no_mangle)]
pub extern "C" fn orch_topo_layers(
    tasks_json_ptr: *const u8,
    tasks_json_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let json = unsafe { read_str(tasks_json_ptr, tasks_json_len) };
        let tasks: Vec<Task> = match serde_json::from_str(json) {
            Ok(t) => t,
            Err(e) => return err_to_buf(&format!("invalid tasks json: {}", e), out_ptr, out_len),
        };
        match orch_dag::topo_layers(&tasks) {
            Ok(l) => write_out(
                serde_json::to_string(&l).unwrap_or_default(),
                out_ptr,
                out_len,
            ),
            Err(e) => err_to_buf(&e.message, out_ptr, out_len),
        }
    }))
    .unwrap_or(-1)
}

#[unsafe(no_mangle)]
pub extern "C" fn orch_downstream_of(
    tasks_json_ptr: *const u8,
    tasks_json_len: usize,
    failed_ptr: *const u8,
    failed_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let json = unsafe { read_str(tasks_json_ptr, tasks_json_len) };
        let failed = unsafe { read_str(failed_ptr, failed_len) };
        let tasks: Vec<Task> = match serde_json::from_str(json) {
            Ok(t) => t,
            Err(e) => return err_to_buf(&format!("invalid tasks json: {}", e), out_ptr, out_len),
        };
        let down = orch_dag::downstream_of(&tasks, failed);
        write_out(
            serde_json::to_string(&down).unwrap_or_default(),
            out_ptr,
            out_len,
        )
    }))
    .unwrap_or(-1)
}

#[unsafe(no_mangle)]
pub extern "C" fn orch_render_mermaid(
    dag_json_ptr: *const u8,
    dag_json_len: usize,
    mode: i32,
    statuses_json_ptr: *const u8,
    statuses_json_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let dag_json = unsafe { read_str(dag_json_ptr, dag_json_len) };
        let dag: orch_dag::DagResult = match serde_json::from_str(dag_json) {
            Ok(d) => d,
            Err(e) => return err_to_buf(&format!("invalid dag json: {}", e), out_ptr, out_len),
        };
        let statuses_str = unsafe { read_str(statuses_json_ptr, statuses_json_len) };
        let statuses: Vec<(String, String)> = if statuses_str.is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(statuses_str).unwrap_or_default()
        };
        let m = match mode {
            0 => orch_dag::mermaid::Mode::Lineage,
            1 => orch_dag::mermaid::Mode::Dag,
            _ => orch_dag::mermaid::Mode::Combined,
        };
        let s = orch_dag::mermaid::render(&dag, m, &statuses);
        write_out(s, out_ptr, out_len)
    }))
    .unwrap_or(-1)
}

// ---------------------------------------------------------------------------
// Runtime templating (orch_runtime)
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "C" fn orch_substitute_vars(
    sql_ptr: *const u8,
    sql_len: usize,
    vars_json_ptr: *const u8,
    vars_json_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let sql = unsafe { read_str(sql_ptr, sql_len) };
        let vars_json = unsafe { read_str(vars_json_ptr, vars_json_len) };
        let vars: std::collections::HashMap<String, String> = if vars_json.is_empty() {
            std::collections::HashMap::new()
        } else {
            serde_json::from_str(vars_json).unwrap_or_default()
        };
        let s = orch_runtime::substitute(sql, &vars);
        write_out(s, out_ptr, out_len)
    }))
    .unwrap_or(-1)
}

// ---------------------------------------------------------------------------
// Phase 13: Asset code_version (sql hash) helper
// ---------------------------------------------------------------------------

/// Compute the canonical `code_version` string (FNV-1a 64-bit hex of the
/// trimmed SQL body) for a task. Returned via the same leaked-buffer FFI
/// convention so the C++ Asset auto-population path can stash it in
/// `__orch__.assets.code_version`.
#[unsafe(no_mangle)]
pub extern "C" fn orch_sql_code_version(
    sql_ptr: *const u8,
    sql_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let sql = unsafe { read_str(sql_ptr, sql_len) };
        let v = orch_common::sql_code_version(sql);
        write_out(v, out_ptr, out_len)
    }))
    .unwrap_or(-1)
}

// ---------------------------------------------------------------------------
// Phase 13 m2: Asset-level Mermaid renderer
// ---------------------------------------------------------------------------

/// Render a Mermaid graph centered on `focal_asset`. `edges_json` is a JSON
/// array of `{upstream_asset, downstream_asset, via_task, edge_type}` rows
/// that the C++ caller has already pulled from `__orch__.asset_edges`
/// (typically upstream-of + downstream-of the focal asset). No transitive
/// closure is performed Rust-side.
#[unsafe(no_mangle)]
pub extern "C" fn orch_render_asset_lineage(
    focal_ptr: *const u8,
    focal_len: usize,
    edges_json_ptr: *const u8,
    edges_json_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let focal = unsafe { read_str(focal_ptr, focal_len) };
        let edges_json = unsafe { read_str(edges_json_ptr, edges_json_len) };
        let edges: Vec<orch_dag::mermaid::AssetEdge> = if edges_json.is_empty() {
            Vec::new()
        } else {
            match serde_json::from_str(edges_json) {
                Ok(v) => v,
                Err(e) => {
                    return err_to_buf(&format!("invalid edges json: {}", e), out_ptr, out_len);
                }
            }
        };
        let s = orch_dag::mermaid::render_asset_lineage(focal, &edges);
        write_out(s, out_ptr, out_len)
    }))
    .unwrap_or(-1)
}

// ---------------------------------------------------------------------------
// OpenLineage (orch_ol)
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "C" fn orch_ol_set_url(ptr: *const u8, len: usize) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let url = unsafe { read_str(ptr, len) };
        orch_ol::set_url(url);
        0
    }))
    .unwrap_or(-1)
}

#[unsafe(no_mangle)]
pub extern "C" fn orch_ol_set_api_key(ptr: *const u8, len: usize) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let key = unsafe { read_str(ptr, len) };
        orch_ol::set_api_key(key);
        0
    }))
    .unwrap_or(-1)
}

#[unsafe(no_mangle)]
pub extern "C" fn orch_ol_set_debug(d: i32) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        orch_ol::set_debug(d != 0);
        0
    }))
    .unwrap_or(-1)
}

#[unsafe(no_mangle)]
pub extern "C" fn orch_ol_emit(ptr: *const u8, len: usize) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let event = unsafe { read_str(ptr, len) };
        if orch_ol::enqueue(event) {
            0
        } else {
            1
        }
    }))
    .unwrap_or(-1)
}

// ---------------------------------------------------------------------------
// Phase 14: Partition expansion + calendar rendering
// ---------------------------------------------------------------------------

/// Expand a partition definition into a JSON array of `{key, dimension_values}`
/// rows. `def_json` is the serde-serialized `PartitionDef`. `range_json` is
/// optional: when present, `{"from":"YYYY-MM-DD","to":"YYYY-MM-DD"}` narrows
/// Daily expansion (ignored by Static; applied recursively in Multi).
///
/// `dimension_values` is a JSON object string; for non-Multi partitions it
/// contains a single `partition_key` field, for Multi it carries each named
/// dimension's value (e.g. `{"date":"2026-05-17","region":"jp"}`).
#[unsafe(no_mangle)]
pub extern "C" fn orch_partition_expand(
    def_json_ptr: *const u8,
    def_json_len: usize,
    range_json_ptr: *const u8,
    range_json_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let def_json = unsafe { read_str(def_json_ptr, def_json_len) };
        let range_json = unsafe { read_str(range_json_ptr, range_json_len) };
        let def: orch_common::PartitionDef = match serde_json::from_str(def_json) {
            Ok(d) => d,
            Err(e) => {
                return err_to_buf(&format!("invalid def json: {}", e), out_ptr, out_len);
            }
        };
        #[derive(serde::Deserialize, Default)]
        struct Range {
            from: Option<String>,
            to: Option<String>,
        }
        let range: Range = if range_json.is_empty() {
            Range::default()
        } else {
            serde_json::from_str(range_json).unwrap_or_default()
        };
        let r = match (range.from, range.to) {
            (Some(a), Some(b)) => Some((a, b)),
            _ => None,
        };
        let today = chrono::Utc::now().date_naive();
        let keys = def.expand_keys(today, r);
        let mut rows = Vec::with_capacity(keys.len());
        for k in keys {
            let dims = def.split_key(&k);
            let mut obj = serde_json::Map::new();
            for (n, v) in dims {
                obj.insert(n, serde_json::Value::String(v));
            }
            rows.push(serde_json::json!({
                "key": k,
                "dimension_values": serde_json::Value::Object(obj),
            }));
        }
        write_out(serde_json::Value::Array(rows).to_string(), out_ptr, out_len)
    }))
    .unwrap_or(-1)
}

/// Split a partition key into per-dimension `(name, value)` pairs. Returns
/// a JSON array of `{"name":..., "value":...}` objects. For non-Multi
/// definitions the array has a single element with `name="partition_key"`.
#[unsafe(no_mangle)]
pub extern "C" fn orch_partition_split_key(
    def_json_ptr: *const u8,
    def_json_len: usize,
    key_ptr: *const u8,
    key_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let def_json = unsafe { read_str(def_json_ptr, def_json_len) };
        let key = unsafe { read_str(key_ptr, key_len) };
        let def: orch_common::PartitionDef = match serde_json::from_str(def_json) {
            Ok(d) => d,
            Err(e) => {
                return err_to_buf(&format!("invalid def json: {}", e), out_ptr, out_len);
            }
        };
        let parts = def.split_key(key);
        let arr: Vec<serde_json::Value> = parts
            .into_iter()
            .map(|(n, v)| serde_json::json!({"name": n, "value": v}))
            .collect();
        write_out(serde_json::Value::Array(arr).to_string(), out_ptr, out_len)
    }))
    .unwrap_or(-1)
}

/// Render the calendar-style ASCII for an Asset's partitions.
///
/// `def_json` is the serde-serialized `PartitionDef`.
/// `rows_json` is a JSON array of `{key, status}` rows; `status` may be
/// null for never-materialized partitions.
#[unsafe(no_mangle)]
pub extern "C" fn orch_render_partition_calendar(
    asset_ptr: *const u8,
    asset_len: usize,
    def_json_ptr: *const u8,
    def_json_len: usize,
    rows_json_ptr: *const u8,
    rows_json_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let asset = unsafe { read_str(asset_ptr, asset_len) };
        let def_json = unsafe { read_str(def_json_ptr, def_json_len) };
        let rows_json = unsafe { read_str(rows_json_ptr, rows_json_len) };
        let def: orch_common::PartitionDef = match serde_json::from_str(def_json) {
            Ok(d) => d,
            Err(e) => {
                return err_to_buf(&format!("invalid def json: {}", e), out_ptr, out_len);
            }
        };
        #[derive(serde::Deserialize)]
        struct Row {
            key: String,
            status: Option<String>,
        }
        let raw: Vec<Row> = if rows_json.is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(rows_json).unwrap_or_default()
        };
        let rows: Vec<orch_runtime::PartitionStatus> = raw
            .into_iter()
            .map(|r| orch_runtime::PartitionStatus {
                key: r.key,
                status: r.status,
            })
            .collect();
        let s = orch_runtime::render_calendar(asset, &def, &rows);
        write_out(s, out_ptr, out_len)
    }))
    .unwrap_or(-1)
}

// ---------------------------------------------------------------------------
// Phase 15: AutomationCondition FFI surface.
// ---------------------------------------------------------------------------

/// Parse an `@automation` DSL string into a canonical AST + DSL string.
/// Used by the asset upsert path to re-validate stored conditions on load
/// and by the simulate pragma to display the parsed condition. Output:
/// `{"dsl": "...", "ast": <serde-json>}` on success, `{"error":"..."}` on
/// failure (return -1).
#[unsafe(no_mangle)]
pub extern "C" fn orch_automation_parse(
    src_ptr: *const u8,
    src_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let src = unsafe { read_str(src_ptr, src_len) };
        match orch_common::parse_automation(src) {
            Ok(cond) => {
                let dsl = cond.serialize_dsl();
                let v = serde_json::json!({
                    "dsl": dsl,
                    "ast": cond,
                });
                write_out(v.to_string(), out_ptr, out_len)
            }
            Err(e) => err_to_buf(&e.to_string(), out_ptr, out_len),
        }
    }))
    .unwrap_or(-1)
}

// ---------------------------------------------------------------------------
// Shared JSON → EvalContext helper (used by both FFI functions and tests)
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize, Default)]
#[serde(default)]
struct RawCtx {
    upstream_max_materialized_at: Option<String>,
    own_last_materialized_at: Option<String>,
    missing_partition_count: u64,
    now: Option<String>,
    freshness_lag_seconds: Option<u64>,
    in_progress: bool,
    target_lag_seconds: Option<u64>,
    last_evaluated_at: Option<String>,
    /// SQLMesh-style: already-computed intervals [[start_ts, end_ts], ...]
    stored_intervals: Vec<[i64; 2]>,
    /// Epoch seconds: earliest timestamp to track intervals from.
    interval_start_ts: Option<i64>,
    /// Re-process the last N intervals when a newer one is missing.
    lookback: u32,
    /// Include the current incomplete interval when computing gaps.
    allow_partials: bool,
}

fn parse_ts(s: &str) -> Option<chrono::NaiveDateTime> {
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        if let Ok(t) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Some(t);
        }
    }
    None
}

fn eval_ctx_from_json(ctx_json: &str) -> Result<orch_common::EvalContext, String> {
    let raw: RawCtx = if ctx_json.is_empty() {
        RawCtx::default()
    } else {
        serde_json::from_str(ctx_json).map_err(|e| format!("invalid eval context json: {}", e))?
    };
    let opt_ts = |s: Option<String>| s.as_deref().and_then(parse_ts);
    let stored = raw.stored_intervals.iter().map(|p| (p[0], p[1])).collect();
    let now = raw
        .now
        .as_deref()
        .and_then(parse_ts)
        .unwrap_or_else(|| chrono::Utc::now().naive_utc());
    Ok(orch_common::EvalContext {
        upstream_max_materialized_at: opt_ts(raw.upstream_max_materialized_at),
        own_last_materialized_at: opt_ts(raw.own_last_materialized_at),
        missing_partition_count: raw.missing_partition_count,
        now,
        freshness_lag_seconds: raw.freshness_lag_seconds,
        in_progress: raw.in_progress,
        target_lag_seconds: raw.target_lag_seconds,
        last_evaluated_at: opt_ts(raw.last_evaluated_at),
        stored_intervals: stored,
        interval_start_ts: raw.interval_start_ts,
        lookback: raw.lookback,
        allow_partials: raw.allow_partials,
    })
}

/// Evaluate an automation condition against a context snapshot.
///
/// `cond_dsl`: the DSL string stored on `__orch__.assets.automation_condition`.
/// `ctx_json`: a JSON object with snake_case fields matching `EvalContext`.
/// Timestamps are ISO `YYYY-MM-DD HH:MM:SS[.fff]` strings; missing or null
/// fields fall back to defaults (`None`, `0`, `false`).
///
/// Returns `{"condition_met": bool, "reason": "...", "dsl": "...canonical..."}`.
#[unsafe(no_mangle)]
pub extern "C" fn orch_automation_evaluate(
    cond_dsl_ptr: *const u8,
    cond_dsl_len: usize,
    ctx_json_ptr: *const u8,
    ctx_json_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let dsl = unsafe { read_str(cond_dsl_ptr, cond_dsl_len) };
        let ctx_json = unsafe { read_str(ctx_json_ptr, ctx_json_len) };
        let cond = match orch_common::parse_automation(dsl) {
            Ok(c) => c,
            Err(e) => {
                return err_to_buf(&format!("automation parse error: {}", e), out_ptr, out_len);
            }
        };

        let ctx = match eval_ctx_from_json(ctx_json) {
            Ok(c) => c,
            Err(e) => return err_to_buf(&e, out_ptr, out_len),
        };
        let (met, reason) = orch_common::evaluate_automation(&cond, &ctx);
        let v = serde_json::json!({
            "condition_met": met,
            "reason": reason,
            "dsl": cond.serialize_dsl(),
        });
        write_out(v.to_string(), out_ptr, out_len)
    }))
    .unwrap_or(-1)
}

/// Return the list of missing intervals for an `on_interval()` condition.
///
/// Same inputs as `orch_automation_evaluate`. Output is a JSON array of
/// `[start_ts, end_ts]` pairs (epoch seconds, half-open) in chronological
/// order.  Returns `[]` for non-interval conditions.
///
/// The sensor loop calls this when `orch_automation_evaluate` returns
/// `condition_met=true` and the condition contains `on_interval`, then
/// enqueues one task run per returned interval and INSERTs the interval
/// into `__orch__.asset_intervals` on success.
#[unsafe(no_mangle)]
pub extern "C" fn orch_missing_intervals_for(
    cond_dsl_ptr: *const u8,
    cond_dsl_len: usize,
    ctx_json_ptr: *const u8,
    ctx_json_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let dsl = unsafe { read_str(cond_dsl_ptr, cond_dsl_len) };
        let ctx_json = unsafe { read_str(ctx_json_ptr, ctx_json_len) };
        let cond = match orch_common::parse_automation(dsl) {
            Ok(c) => c,
            Err(e) => {
                return err_to_buf(&format!("automation parse error: {}", e), out_ptr, out_len);
            }
        };

        let ctx = match eval_ctx_from_json(ctx_json) {
            Ok(c) => c,
            Err(e) => return err_to_buf(&e, out_ptr, out_len),
        };
        let missing = orch_common::missing_intervals_for(&cond, &ctx);
        let arr: Vec<[i64; 2]> = missing.iter().map(|&(s, e)| [s, e]).collect();
        write_out(
            serde_json::to_string(&arr).unwrap_or_default(),
            out_ptr,
            out_len,
        )
    }))
    .unwrap_or(-1)
}

// ---------------------------------------------------------------------------
// Tests for eval_ctx_from_json + FFI contract
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use orch_common::{evaluate_automation, missing_intervals_for, parse_automation};

    // Helper: build a minimal context JSON string.
    fn ctx(fields: &str) -> String {
        format!("{{\"now\":\"2026-06-10 12:00:00\",{}}}", fields)
    }

    // -----------------------------------------------------------------------
    // eval_ctx_from_json — JSON contract
    // -----------------------------------------------------------------------

    #[test]
    fn empty_json_gives_defaults() {
        let ec = eval_ctx_from_json("").unwrap();
        assert!(ec.stored_intervals.is_empty());
        assert!(ec.interval_start_ts.is_none());
        assert!(ec.own_last_materialized_at.is_none());
    }

    #[test]
    fn parses_stored_intervals() {
        let json = ctx("\"stored_intervals\":[[0,86400],[172800,259200]]");
        let ec = eval_ctx_from_json(&json).unwrap();
        assert_eq!(ec.stored_intervals, vec![(0, 86_400), (172_800, 259_200)]);
    }

    #[test]
    fn parses_interval_start_ts() {
        let json = ctx("\"interval_start_ts\":1781049600");
        let ec = eval_ctx_from_json(&json).unwrap();
        assert_eq!(ec.interval_start_ts, Some(1_781_049_600));
    }

    #[test]
    fn parses_lookback_and_allow_partials() {
        let json = ctx("\"lookback\":2,\"allow_partials\":true");
        let ec = eval_ctx_from_json(&json).unwrap();
        assert_eq!(ec.lookback, 2);
        assert!(ec.allow_partials);
        // Defaults when absent.
        let ec2 = eval_ctx_from_json(&ctx("\"stored_intervals\":[]")).unwrap();
        assert_eq!(ec2.lookback, 0);
        assert!(!ec2.allow_partials);
    }

    #[test]
    fn lookback_repulls_trailing_interval() {
        // Days 8,9 stored; now = 06-10 12:00 → no plain gap. But with
        // lookback=1 the trailing day must be re-processed only when a
        // *newer* interval is missing — fully stored ⇒ still empty.
        let start: i64 = 1_780_876_800; // 2026-06-08 00:00 UTC
        let json = format!(
            "{{\"now\":\"2026-06-10 12:00:00\",\
              \"stored_intervals\":[[{},{}]],\
              \"interval_start_ts\":{},\"lookback\":1}}",
            start,
            start + 2 * 86_400,
            start
        );
        let cond = parse_automation("on_interval(\"daily\")").unwrap();
        let ctx = eval_ctx_from_json(&json).unwrap();
        let missing = missing_intervals_for(&cond, &ctx);
        assert!(missing.is_empty(), "fully stored + lookback: {:?}", missing);
    }

    #[test]
    fn allow_partials_includes_current_interval() {
        // Day 8 and 9 stored; now = 06-10 12:00. Without allow_partials the
        // in-progress 06-10 day is excluded → empty. With it, the partial
        // [06-10 00:00, 06-10 12:00) interval is missing.
        let start: i64 = 1_780_876_800; // 2026-06-08 00:00 UTC
        let day10 = start + 2 * 86_400; // 2026-06-10 00:00 UTC
        let json = format!(
            "{{\"now\":\"2026-06-10 12:00:00\",\
              \"stored_intervals\":[[{},{}]],\
              \"interval_start_ts\":{},\"allow_partials\":true}}",
            start, day10, start
        );
        let cond = parse_automation("on_interval(\"daily\")").unwrap();
        let ctx = eval_ctx_from_json(&json).unwrap();
        let missing = missing_intervals_for(&cond, &ctx);
        assert_eq!(
            missing,
            vec![(day10, day10 + 12 * 3_600)],
            "partial day expected"
        );
    }

    #[test]
    fn parses_iso_timestamps() {
        let json = ctx("\"own_last_materialized_at\":\"2026-06-09 00:00:00\"");
        let ec = eval_ctx_from_json(&json).unwrap();
        assert!(ec.own_last_materialized_at.is_some());
    }

    #[test]
    fn rejects_bad_json() {
        assert!(eval_ctx_from_json("{not valid json}").is_err());
    }

    // -----------------------------------------------------------------------
    // on_interval evaluate — via eval_ctx_from_json
    // -----------------------------------------------------------------------

    #[test]
    fn on_interval_false_when_fully_stored() {
        // 2026-06-10 00:00 UTC epoch = 1781049600; now = 12:00 same day.
        // Stored covers the whole day → no missing interval → condition_met false.
        let start: i64 = 1_781_049_600; // 2026-06-10 00:00 UTC
        let end = start + 86_400;
        let json = format!(
            "{{\"now\":\"2026-06-10 12:00:00\",\
              \"stored_intervals\":[[{},{}]],\
              \"interval_start_ts\":{}}}",
            start, end, start
        );
        let cond = parse_automation("on_interval(\"daily\")").unwrap();
        let ctx = eval_ctx_from_json(&json).unwrap();
        let (met, _) = evaluate_automation(&cond, &ctx);
        assert!(!met, "fully stored day should not fire");
    }

    #[test]
    fn on_interval_true_when_day_missing() {
        // interval_start_ts = 2026-06-08 00:00; now = 2026-06-10 12:00.
        // Nothing stored → 2 missing days → condition_met true.
        let start: i64 = 1_780_876_800; // 2026-06-08 00:00 UTC
        let json = format!(
            "{{\"now\":\"2026-06-10 12:00:00\",\
              \"stored_intervals\":[],\
              \"interval_start_ts\":{}}}",
            start
        );
        let cond = parse_automation("on_interval(\"daily\")").unwrap();
        let ctx = eval_ctx_from_json(&json).unwrap();
        let (met, _) = evaluate_automation(&cond, &ctx);
        assert!(met, "2 missing days should fire");
    }

    // -----------------------------------------------------------------------
    // missing_intervals_for — via eval_ctx_from_json
    // -----------------------------------------------------------------------

    #[test]
    fn missing_returns_empty_when_fully_covered() {
        let start: i64 = 1_781_049_600; // 2026-06-10 00:00 UTC
        let end = start + 86_400;
        let json = format!(
            "{{\"now\":\"2026-06-10 12:00:00\",\
              \"stored_intervals\":[[{},{}]],\
              \"interval_start_ts\":{}}}",
            start, end, start
        );
        let cond = parse_automation("on_interval(\"daily\")").unwrap();
        let ctx = eval_ctx_from_json(&json).unwrap();
        let missing = missing_intervals_for(&cond, &ctx);
        assert!(missing.is_empty(), "fully covered: {:?}", missing);
    }

    #[test]
    fn missing_returns_two_days_when_both_absent() {
        let start: i64 = 1_780_876_800; // 2026-06-08 00:00 UTC
        let json = format!(
            "{{\"now\":\"2026-06-10 12:00:00\",\
              \"stored_intervals\":[],\
              \"interval_start_ts\":{}}}",
            start
        );
        let cond = parse_automation("on_interval(\"daily\")").unwrap();
        let ctx = eval_ctx_from_json(&json).unwrap();
        let missing = missing_intervals_for(&cond, &ctx);
        assert_eq!(missing.len(), 2, "expected 2 missing days: {:?}", missing);
        assert_eq!(missing[0].0, start);
        assert_eq!(missing[1].0, start + 86_400);
    }

    #[test]
    fn missing_returns_gap_in_middle() {
        // Day 0 and day 2 stored; day 1 missing.
        let d0: i64 = 1_780_876_800; // 2026-06-08 00:00 UTC
        let d1 = d0 + 86_400;
        let d2 = d0 + 2 * 86_400;
        let d3 = d0 + 3 * 86_400;
        let json = format!(
            "{{\"now\":\"2026-06-11 12:00:00\",\
              \"stored_intervals\":[[{d0},{d1}],[{d2},{d3}]],\
              \"interval_start_ts\":{d0}}}"
        );
        let cond = parse_automation("on_interval(\"daily\")").unwrap();
        let ctx = eval_ctx_from_json(&json).unwrap();
        let missing = missing_intervals_for(&cond, &ctx);
        assert_eq!(missing, vec![(d1, d2)], "only middle day should be missing");
    }

    #[test]
    fn missing_empty_for_eager_condition() {
        let json = ctx("\"stored_intervals\":[]");
        let cond = parse_automation("eager").unwrap();
        let ctx = eval_ctx_from_json(&json).unwrap();
        let missing = missing_intervals_for(&cond, &ctx);
        assert!(
            missing.is_empty(),
            "non-interval condition returns no intervals"
        );
    }

    #[test]
    fn on_interval_and_not_in_progress_gate() {
        // on_interval fires but in_progress is true → AND gate blocks it.
        let start: i64 = 1_780_876_800; // 2026-06-08 00:00 UTC
        let json = format!(
            "{{\"now\":\"2026-06-10 12:00:00\",\
              \"stored_intervals\":[],\
              \"interval_start_ts\":{},\
              \"in_progress\":true}}",
            start
        );
        let cond = parse_automation("on_interval(\"daily\") AND NOT in_progress()").unwrap();
        let ctx = eval_ctx_from_json(&json).unwrap();
        let (met, reason) = evaluate_automation(&cond, &ctx);
        assert!(!met, "in_progress gate should block: {}", reason);
    }

    #[test]
    fn hourly_missing_intervals() {
        // 3 hours of tracking, nothing stored → 3 missing 1h intervals.
        // now = epoch + 3h + 30min; interval_start_ts = epoch
        let start: i64 = 0;
        let now_ts = 3 * 3600 + 1800; // 3.5 hours after epoch
                                      // Use a fixed ISO timestamp: 1970-01-01 03:30:00
        let json = format!(
            "{{\"now\":\"1970-01-01 03:30:00\",\
              \"stored_intervals\":[],\
              \"interval_start_ts\":{}}}",
            start
        );
        let cond = parse_automation("on_interval(\"hourly\")").unwrap();
        let ctx = eval_ctx_from_json(&json).unwrap();
        let missing = missing_intervals_for(&cond, &ctx);
        // floor(3.5h) = 3h → intervals [0,1h), [1h,2h), [2h,3h) = 3 intervals
        let _ = now_ts; // used above for clarity
        assert_eq!(missing.len(), 3, "3 missing hours: {:?}", missing);
    }
}

/// Parse a `@target_lag` duration string and return its value in seconds
/// as a JSON `{"seconds": N}` object.
#[unsafe(no_mangle)]
pub extern "C" fn orch_target_lag_parse(
    src_ptr: *const u8,
    src_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let src = unsafe { read_str(src_ptr, src_len) };
        match orch_common::parse_target_lag(src) {
            Ok(s) => {
                let v = serde_json::json!({ "seconds": s });
                write_out(v.to_string(), out_ptr, out_len)
            }
            Err(e) => err_to_buf(&e.to_string(), out_ptr, out_len),
        }
    }))
    .unwrap_or(-1)
}

// ---------------------------------------------------------------------------
// Ingestion (orch_ingest) — Phase 19 P0
// ---------------------------------------------------------------------------

/// Build a normalization plan. Input is a `PlanSpec` JSON carrying the target
/// table, the source relation and the column types DuckDB inferred; output is
/// a `Plan` JSON with one entry per table, each holding ready-to-run DDL and
/// INSERT text.
#[unsafe(no_mangle)]
pub extern "C" fn orch_ingest_plan(
    spec_ptr: *const u8,
    spec_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let json = unsafe { read_str(spec_ptr, spec_len) };
        let spec: orch_ingest::PlanSpec = match serde_json::from_str(json) {
            Ok(s) => s,
            Err(e) => {
                return err_to_buf(&format!("invalid ingest plan spec: {}", e), out_ptr, out_len)
            }
        };
        let plan = orch_ingest::build_plan(&spec);
        write_out(
            serde_json::to_string(&plan).unwrap_or_default(),
            out_ptr,
            out_len,
        )
    }))
    .unwrap_or(-1)
}

/// Compare a table's current columns against the shape the incoming data
/// wants. Returns the changes plus the DDL that absorbs them, and an error
/// list for changes P0 refuses to apply.
#[unsafe(no_mangle)]
pub extern "C" fn orch_ingest_schema_diff(
    spec_ptr: *const u8,
    spec_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let json = unsafe { read_str(spec_ptr, spec_len) };
        let spec: orch_ingest::DiffSpec = match serde_json::from_str(json) {
            Ok(s) => s,
            Err(e) => {
                return err_to_buf(&format!("invalid ingest diff spec: {}", e), out_ptr, out_len)
            }
        };
        let out = orch_ingest::diff(&spec);
        write_out(
            serde_json::to_string(&out).unwrap_or_default(),
            out_ptr,
            out_len,
        )
    }))
    .unwrap_or(-1)
}

/// Statements that enforce a write disposition after the rows have been
/// appended. Input is a `PruneSpec` JSON; output `{statements, disposition,
/// errors}`.
#[unsafe(no_mangle)]
pub extern "C" fn orch_ingest_prune(
    spec_ptr: *const u8,
    spec_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let json = unsafe { read_str(spec_ptr, spec_len) };
        let spec: orch_ingest::PruneSpec = match serde_json::from_str(json) {
            Ok(s) => s,
            Err(e) => {
                return err_to_buf(&format!("invalid ingest prune spec: {}", e), out_ptr, out_len)
            }
        };
        let out = orch_ingest::prune(&spec);
        write_out(
            serde_json::to_string(&out).unwrap_or_default(),
            out_ptr,
            out_len,
        )
    }))
    .unwrap_or(-1)
}

/// Fetch a paginated HTTP source into JSONL part files. Input is a
/// `FetchSpec` JSON with the request already carrying resolved headers;
/// output `{files, pages, records, cursor_out, truncated}`.
#[unsafe(no_mangle)]
pub extern "C" fn orch_ingest_fetch(
    spec_ptr: *const u8,
    spec_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let json = unsafe { read_str(spec_ptr, spec_len) };
        let spec: orch_ingest::FetchSpec = match serde_json::from_str(json) {
            Ok(s) => s,
            Err(e) => {
                return err_to_buf(&format!("invalid ingest fetch spec: {}", e), out_ptr, out_len)
            }
        };
        match orch_ingest::fetch(&spec) {
            Ok(out) => write_out(
                serde_json::to_string(&out).unwrap_or_default(),
                out_ptr,
                out_len,
            ),
            Err(e) => err_to_buf(&e, out_ptr, out_len),
        }
    }))
    .unwrap_or(-1)
}
