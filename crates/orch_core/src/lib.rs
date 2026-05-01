// duckorch_core: orchestration logic exposed via C FFI.
//
// All FFI functions follow this convention:
//   - Inputs are (ptr, len) UTF-8 byte slices.
//   - Outputs are written into out-params (*mut *mut u8, *mut usize) holding
//     a heap-allocated buffer; caller must free via `orch_string_free`.
//   - Return value: 0 on success, negative on error.
//
// All entry points wrap their body in `catch_unwind` to ensure no Rust panic
// crosses the FFI boundary.

use std::panic::{catch_unwind, AssertUnwindSafe};

mod dag;
mod ffi;
mod lineage;
mod mermaid;
mod openlineage;
mod task_parser;
mod templating;

use ffi::{leak_vec, read_str};

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
    let s = format!("{{\"error\":{}}}", serde_json::to_string(msg).unwrap_or("\"\"".into()));
    write_out(s, out_ptr, out_len);
    -1
}

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

/// Parse a single SQL file content into a Task JSON.
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
        match task_parser::parse_sql_file(sql, fp_opt) {
            Ok(t) => {
                let json = serde_json::to_string(&t).unwrap_or_default();
                write_out(json, out_ptr, out_len)
            }
            Err(e) => err_to_buf(&e.to_string(), out_ptr, out_len),
        }
    }))
    .unwrap_or(-1)
}

/// Walk a directory recursively, parse all .sql files, return a JSON array of Tasks.
#[unsafe(no_mangle)]
pub extern "C" fn orch_load_directory(
    path_ptr: *const u8,
    path_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let path = unsafe { read_str(path_ptr, path_len) };
        let mut tasks = Vec::new();
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
            match task_parser::parse_sql_file(&content, Some(&path_str)) {
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

/// Extract inputs/outputs from a SQL string.
#[unsafe(no_mangle)]
pub extern "C" fn orch_extract_io(
    sql_ptr: *const u8,
    sql_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let sql = unsafe { read_str(sql_ptr, sql_len) };
        let (inputs, outputs) = lineage::extract_io(sql);
        let result = serde_json::json!({"inputs": inputs, "outputs": outputs});
        write_out(result.to_string(), out_ptr, out_len)
    }))
    .unwrap_or(-1)
}

/// Build topological layers (mutually independent tasks). Returns JSON: [[t1,t2],[t3],...].
#[unsafe(no_mangle)]
pub extern "C" fn orch_topo_layers(
    tasks_json_ptr: *const u8,
    tasks_json_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let json = unsafe { read_str(tasks_json_ptr, tasks_json_len) };
        let tasks: Vec<task_parser::Task> = match serde_json::from_str(json) {
            Ok(t) => t,
            Err(e) => return err_to_buf(&format!("invalid tasks json: {}", e), out_ptr, out_len),
        };
        match dag::topo_layers(&tasks) {
            Ok(l) => write_out(serde_json::to_string(&l).unwrap_or_default(), out_ptr, out_len),
            Err(e) => err_to_buf(&e.message, out_ptr, out_len),
        }
    }))
    .unwrap_or(-1)
}

/// Build a DAG from a JSON array of Tasks. Returns ordered task names + edges.
#[unsafe(no_mangle)]
pub extern "C" fn orch_build_dag(
    tasks_json_ptr: *const u8,
    tasks_json_len: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let json = unsafe { read_str(tasks_json_ptr, tasks_json_len) };
        let tasks: Vec<task_parser::Task> = match serde_json::from_str(json) {
            Ok(t) => t,
            Err(e) => return err_to_buf(&format!("invalid tasks json: {}", e), out_ptr, out_len),
        };
        match dag::build_dag(&tasks) {
            Ok(r) => write_out(serde_json::to_string(&r).unwrap_or_default(), out_ptr, out_len),
            Err(e) => err_to_buf(&e.message, out_ptr, out_len),
        }
    }))
    .unwrap_or(-1)
}

/// Render Mermaid diagram from a DagResult JSON.
/// `mode`: 0=lineage, 1=dag, 2=combined
/// `statuses_json`: optional JSON array of [name, status] pairs (empty array OK).
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
        let dag: dag::DagResult = match serde_json::from_str(dag_json) {
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
            0 => mermaid::Mode::Lineage,
            1 => mermaid::Mode::Dag,
            _ => mermaid::Mode::Combined,
        };
        let s = mermaid::render(&dag, m, &statuses);
        write_out(s, out_ptr, out_len)
    }))
    .unwrap_or(-1)
}

/// Set OpenLineage backend URL. Empty string = disabled.
#[unsafe(no_mangle)]
pub extern "C" fn orch_ol_set_url(ptr: *const u8, len: usize) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let url = unsafe { read_str(ptr, len) };
        openlineage::set_url(url);
        0
    }))
    .unwrap_or(-1)
}

#[unsafe(no_mangle)]
pub extern "C" fn orch_ol_set_api_key(ptr: *const u8, len: usize) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let key = unsafe { read_str(ptr, len) };
        openlineage::set_api_key(key);
        0
    }))
    .unwrap_or(-1)
}

#[unsafe(no_mangle)]
pub extern "C" fn orch_ol_set_debug(d: i32) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        openlineage::set_debug(d != 0);
        0
    }))
    .unwrap_or(-1)
}

/// Enqueue an OpenLineage event for async POST. Returns 0 if queued, 1 if URL not set, -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn orch_ol_emit(ptr: *const u8, len: usize) -> i32 {
    catch_unwind(AssertUnwindSafe(|| {
        let event = unsafe { read_str(ptr, len) };
        if openlineage::enqueue(event) {
            0
        } else {
            1
        }
    }))
    .unwrap_or(-1)
}

/// Substitute Jinja-style {{ key }} placeholders. `vars_json` is a JSON object {key: value}.
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
        let s = templating::substitute(sql, &vars);
        write_out(s, out_ptr, out_len)
    }))
    .unwrap_or(-1)
}

/// Compute downstream task names of a failed task (for skip propagation).
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
        let tasks: Vec<task_parser::Task> = match serde_json::from_str(json) {
            Ok(t) => t,
            Err(e) => return err_to_buf(&format!("invalid tasks json: {}", e), out_ptr, out_len),
        };
        let down = dag::downstream_of(&tasks, failed);
        write_out(serde_json::to_string(&down).unwrap_or_default(), out_ptr, out_len)
    }))
    .unwrap_or(-1)
}
