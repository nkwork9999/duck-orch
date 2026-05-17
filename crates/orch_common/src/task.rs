// Canonical Task definition. Shared by parser, dag, executor, lineage.

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
