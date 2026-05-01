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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskTest {
    pub query: String,
    pub assertion: String,
}
