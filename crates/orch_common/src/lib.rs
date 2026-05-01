// Shared primitives used by every duckOrch sub-crate.
//
// Exposes:
//   * Task            — the canonical task definition struct
//   * TaskTest        — assertion attached to a Task
//   * FFI helpers     — heap buffer leak/free + safe pointer-to-str

pub mod ffi;
pub mod task;

pub use task::{Task, TaskTest};
