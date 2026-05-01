// duckorch_core: orchestration logic written in Rust, called from C++ via FFI.
//
// Phase 0: hello world FFI only. Real modules will be added in later phases.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::slice;
use std::str;

/// Hello-world FFI. Writes a fixed greeting into the output buffer.
///
/// # Safety
/// `name_ptr` must point to `name_len` bytes of valid UTF-8.
/// `out_buf` must point to at least `out_cap` writable bytes.
/// Returns the number of bytes written, or -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn orch_hello(
    name_ptr: *const u8,
    name_len: usize,
    out_buf: *mut u8,
    out_cap: usize,
) -> i32 {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let name = unsafe {
            let bytes = slice::from_raw_parts(name_ptr, name_len);
            str::from_utf8(bytes).unwrap_or("?")
        };
        let greeting = format!("hello {} from duckorch_core", name);
        let bytes = greeting.as_bytes();
        let n = bytes.len().min(out_cap);
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf, n);
        }
        n as i32
    }));
    result.unwrap_or(-1)
}
