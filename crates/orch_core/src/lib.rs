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
            Ok(r) => write_out(serde_json::to_string(&r).unwrap_or_default(), out_ptr, out_len),
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
            Ok(l) => write_out(serde_json::to_string(&l).unwrap_or_default(), out_ptr, out_len),
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
        write_out(serde_json::to_string(&down).unwrap_or_default(), out_ptr, out_len)
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
