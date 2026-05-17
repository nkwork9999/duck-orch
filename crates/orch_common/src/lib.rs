// Shared primitives used by every duckOrch sub-crate.
//
// Exposes:
//   * Task            — the canonical task definition struct
//   * TaskTest        — assertion attached to a Task
//   * FFI helpers     — heap buffer leak/free + safe pointer-to-str

pub mod ffi;
pub mod hash;
pub mod task;

pub use hash::{fnv1a_64, sql_code_version};
pub use task::{ParamSpec, ParamType, Task, TaskTest};
