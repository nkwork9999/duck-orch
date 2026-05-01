// FFI helpers for returning heap-allocated strings to C++.
//
// Convention: Rust functions write a JSON or text result into a heap buffer,
// expose pointer + length via out-params, and rely on the C++ caller to call
// `orch_string_free` to release the memory.

use std::os::raw::c_char;

/// Take a Vec<u8> and return ptr+len that the caller is responsible for freeing.
pub fn leak_vec(v: Vec<u8>) -> (*mut u8, usize) {
    let mut boxed = v.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    let len = boxed.len();
    std::mem::forget(boxed);
    (ptr, len)
}

/// Free a buffer previously returned via `leak_vec`.
///
/// # Safety
/// `ptr` must be a pointer previously returned by `leak_vec`, and `len` must
/// match. After this call the pointer is invalid.
pub unsafe fn free_vec(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        let _ = unsafe { Vec::from_raw_parts(ptr, len, len) };
    }
}

pub unsafe fn read_str<'a>(ptr: *const u8, len: usize) -> &'a str {
    if ptr.is_null() || len == 0 {
        return "";
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    std::str::from_utf8(bytes).unwrap_or("")
}

#[allow(dead_code)]
pub unsafe fn read_cstr<'a>(ptr: *const c_char) -> &'a str {
    if ptr.is_null() {
        return "";
    }
    let cstr = unsafe { std::ffi::CStr::from_ptr(ptr) };
    cstr.to_str().unwrap_or("")
}

/// Build a small JSON {"error": "..."} payload as bytes.
pub fn error_json(msg: &str) -> Vec<u8> {
    format!(
        "{{\"error\":{}}}",
        serde_json::to_string(msg).unwrap_or_else(|_| "\"\"".into())
    )
    .into_bytes()
}
