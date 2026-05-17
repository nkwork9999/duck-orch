// Shared primitives used by every duckOrch sub-crate.
//
// Exposes:
//   * Task            — the canonical task definition struct
//   * TaskTest        — assertion attached to a Task
//   * FFI helpers     — heap buffer leak/free + safe pointer-to-str

pub mod automation;
pub mod ffi;
pub mod hash;
pub mod partition;
pub mod snowflake;
pub mod task;

pub use automation::{
    evaluate as evaluate_automation, parse_automation, parse_target_lag, AutomationCondition,
    AutomationParseError, EvalContext,
};
pub use hash::{fnv1a_64, sql_code_version};
pub use partition::{parse_partition_decl, PartitionDef, PartitionParseError};
pub use snowflake::{parse_one_create_dynamic, parse_snowflake_dump, SnowflakeDynamicBlock};
pub use task::{ParamSpec, ParamType, Task, TaskTest};
