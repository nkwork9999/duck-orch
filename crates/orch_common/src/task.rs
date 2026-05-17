// Canonical Task definition. Shared by parser, dag, executor, lineage.

use crate::automation::AutomationCondition;
use crate::partition::PartitionDef;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Task {
    pub name: String,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub sql: String,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
    pub depends_on: Vec<String>,
    pub schedule: Option<String>,
    pub retries: u32,
    pub timeout_seconds: Option<u64>,
    pub incremental_by: Option<String>,
    pub tags: Vec<String>,
    pub tests: Vec<TaskTest>,
    pub file_path: Option<String>,
    /// DuckDB-native `$name` parameter declarations from `-- @param name:TYPE`
    /// headers. Foundation for Phase 13/14 typed parameter binding; not yet
    /// wired to SQL execution.
    pub params: Vec<ParamSpec>,
    /// Phase 13: explicit asset name from `-- @asset name=...`. When set,
    /// promotes this task to a first-class Asset producer. If absent but
    /// `outputs` is non-empty, each output is auto-registered as an Asset
    /// for backward compatibility.
    pub asset_name: Option<String>,
    /// Asset kind: 'table' | 'view' | 'external' | 'file' | 'model'.
    /// Defaults to 'table' when auto-derived from `@outputs`.
    pub asset_kind: Option<String>,
    /// Logical grouping for UI/CLI (e.g. 'sales', 'analytics').
    pub asset_group: Option<String>,
    /// Asset-level owner. Falls back to `task.owner` if unset.
    pub asset_owner: Option<String>,
    /// Asset-level description. Falls back to `task.description` if unset.
    pub asset_description: Option<String>,
    /// Asset tags for filtering/grouping in `asset list`.
    pub asset_tags: Vec<String>,
    /// Phase 14: partition definition from `-- @partitions_by <expr>`.
    /// `None` means the asset is unpartitioned (everything goes under the
    /// `'__default__'` partition_key, preserving Phase 13 behaviour).
    pub partitions: Option<PartitionDef>,
    /// Phase 15: parsed AutomationCondition AST from `-- @automation <expr>`.
    /// `None` means the asset is *not* automation-driven; the sensor loop
    /// will skip it entirely.
    pub automation: Option<AutomationCondition>,
    /// Phase 15: canonical DSL string for the automation condition, populated
    /// in lock-step with `automation`. Stored separately so the C++ asset
    /// upsert path can write the string directly to
    /// `__orch__.assets.automation_condition` without needing an extra
    /// Rust-side serializer call.
    pub automation_dsl: Option<String>,
    /// Phase 15: `@target_lag` value in seconds. Conceptually
    /// `automation = eager() throttle <N>` — the throttle is applied by the
    /// evaluator (see `automation::evaluate`).
    pub target_lag_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskTest {
    pub query: String,
    pub assertion: String,
}

/// Declared DuckDB-native parameter on a task: `$name` with a SQL type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParamSpec {
    pub name: String,
    pub ty: ParamType,
}

/// Supported SQL types for `-- @param` declarations. Mapped to DuckDB
/// `bind_*` calls at execution time (Phase 13+).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParamType {
    Varchar,
    Integer,
    BigInt,
    Date,
    Timestamp,
    Boolean,
    Double,
}
