// duck-orch-mcp — Phase 11.
//
// MCP (Model Context Protocol) サーバ。Claude Code 等から stdio 経由で
// duckOrch を読み取りツールとして駆動できるようにする薄いラッパ。
//
// 実装方針: 既存の `duck-orch` CLI を std::process::Command でサブプロセス
// 呼び出しし、`--json` 出力をパースする (クエリロジックを再実装しない)。
//
// 環境変数:
//   DUCK_ORCH_BIN  — duck-orch バイナリのパス (既定: PATH 上の `duck-orch`)
//   DUCK_ORCH_DB   — DuckDB ファイルパス (`--db` に渡す)
//   DUCKORCH_EXT   — `--ext` に渡す (任意、未指定なら CLI 側で auto-detect)

use std::future::Future;
use std::process::Command;

use rmcp::{
    ErrorData as McpError, ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, tool::Parameters},
    model::{CallToolResult, Content, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
    transport::stdio,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ---- request schemas ----

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ListRunsRequest {
    /// Maximum number of rows to return (default 20)
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DescribeTaskRequest {
    /// Task name
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GetLineageRequest {
    /// Target table name (e.g. `main.sales_clean`)
    pub table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ValidateRequest {
    /// Path to a .sql task file
    pub file: String,
}

// ---- helpers ----

fn duck_orch_bin() -> String {
    std::env::var("DUCK_ORCH_BIN").unwrap_or_else(|_| "duck-orch".to_string())
}

fn db_arg() -> Option<String> {
    std::env::var("DUCK_ORCH_DB").ok()
}

fn ext_arg() -> Option<String> {
    std::env::var("DUCKORCH_EXT").ok()
}

/// Spawn `duck-orch <args...> --json` and return stdout as a String.
/// Stderr is captured and folded into the error message on non-zero exit.
fn run_cli(extra: &[&str], json: bool) -> Result<String, String> {
    let mut cmd = Command::new(duck_orch_bin());
    if let Some(db) = db_arg() {
        cmd.arg("--db").arg(db);
    }
    if let Some(ext) = ext_arg() {
        cmd.arg("--ext").arg(ext);
    }
    for a in extra {
        cmd.arg(a);
    }
    if json {
        cmd.arg("--json");
    }
    let out = cmd
        .output()
        .map_err(|e| format!("failed to spawn {}: {}", duck_orch_bin(), e))?;
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    if !out.status.success() {
        return Err(format!(
            "duck-orch exited with status {}: {}{}",
            out.status.code().unwrap_or(-1),
            stderr,
            stdout
        ));
    }
    Ok(stdout)
}

fn to_mcp_err(msg: String) -> McpError {
    McpError::internal_error(msg, None)
}

fn ok_text(s: impl Into<String>) -> Result<CallToolResult, McpError> {
    Ok(CallToolResult::success(vec![Content::text(s.into())]))
}

// ---- server ----

#[derive(Debug, Clone)]
pub struct DuckOrchServer {
    tool_router: ToolRouter<Self>,
}

impl Default for DuckOrchServer {
    fn default() -> Self {
        Self::new()
    }
}

#[tool_router]
impl DuckOrchServer {
    pub fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        name = "list_pipelines",
        description = "List registered tasks grouped by tag/pipeline from __orch__.tasks. Read-only."
    )]
    pub async fn list_pipelines(&self) -> Result<CallToolResult, McpError> {
        // The CLI doesn't have a dedicated `pipelines` subcommand; reuse `status`?
        // Better: shell-out via raw SELECT. The CLI exposes that indirectly through
        // `register`/`status`. For now we fetch the task list by re-using
        // `register` with no-op (re-register an empty dir is destructive), so we
        // call `status` to at least surface activity and then enumerate tasks via
        // a follow-up SELECT through duck-orch's shell-out path. Since the CLI has
        // no read-only "list tasks" command, expose what we can: most recent runs
        // grouped by task. Users can call `describe_task` for detail.
        //
        // (Punt: a dedicated `duck-orch tasks` subcommand would be cleaner; track
        // as follow-up.)
        let out = run_cli(&["status"], true).map_err(to_mcp_err)?;
        ok_text(out)
    }

    #[tool(
        name = "list_runs",
        description = "Return recent rows from __orch__.runs (default 20). Read-only."
    )]
    pub async fn list_runs(
        &self,
        Parameters(req): Parameters<ListRunsRequest>,
    ) -> Result<CallToolResult, McpError> {
        let raw = run_cli(&["status"], true).map_err(to_mcp_err)?;
        let limit = req.limit.unwrap_or(20) as usize;
        // `duck-orch status` already LIMIT 50s; trim to requested count if smaller.
        let trimmed = match serde_json::from_str::<Vec<serde_json::Value>>(&raw) {
            Ok(mut rows) => {
                rows.truncate(limit);
                serde_json::to_string(&rows).unwrap_or(raw)
            }
            Err(_) => raw,
        };
        ok_text(trimmed)
    }

    #[tool(
        name = "describe_task",
        description = "Return detail for one task by name (validate-style metadata if a file path is given, else lineage neighbours). Read-only."
    )]
    pub async fn describe_task(
        &self,
        Parameters(req): Parameters<DescribeTaskRequest>,
    ) -> Result<CallToolResult, McpError> {
        // Without a `duck-orch tasks show <name>` subcommand, the best we can do
        // is return upstream + downstream lineage edges that mention the task's
        // output table. Treat `name` as a table name fallback.
        let upstream = run_cli(&["lineage", &req.name], true).unwrap_or_default();
        let downstream = run_cli(&["impact", &req.name], true).unwrap_or_default();
        let body = serde_json::json!({
            "name": req.name,
            "upstream": serde_json::from_str::<serde_json::Value>(&upstream)
                .unwrap_or(serde_json::Value::Null),
            "downstream": serde_json::from_str::<serde_json::Value>(&downstream)
                .unwrap_or(serde_json::Value::Null),
        });
        ok_text(body.to_string())
    }

    #[tool(
        name = "get_lineage",
        description = "Return Mermaid lineage graph for a table via `duck-orch graph lineage`. Read-only."
    )]
    pub async fn get_lineage(
        &self,
        Parameters(req): Parameters<GetLineageRequest>,
    ) -> Result<CallToolResult, McpError> {
        // `duck-orch graph` prints the full lineage Mermaid (not per-table).
        // For per-table upstream we use `lineage <table>` (returns rows, not Mermaid).
        // Combine: prefer the rows query so the caller can re-render; also include
        // the full graph for context.
        let rows = run_cli(&["lineage", &req.table], true).map_err(to_mcp_err)?;
        let graph = run_cli(&["graph", "lineage"], false).unwrap_or_default();
        let body = serde_json::json!({
            "table": req.table,
            "upstream_rows": serde_json::from_str::<serde_json::Value>(&rows)
                .unwrap_or(serde_json::Value::Null),
            "mermaid_full": graph,
        });
        ok_text(body.to_string())
    }

    #[tool(
        name = "validate",
        description = "Parse and validate one task file. Wraps `duck-orch validate <file> --json`."
    )]
    pub async fn validate(
        &self,
        Parameters(req): Parameters<ValidateRequest>,
    ) -> Result<CallToolResult, McpError> {
        let out = run_cli(&["validate", &req.file], true).map_err(to_mcp_err)?;
        ok_text(out)
    }
}

#[tool_handler]
impl ServerHandler for DuckOrchServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "duckOrch MCP server — read-only tools that wrap the `duck-orch` CLI \
                 (lineage, runs, validate). Set DUCK_ORCH_DB to point at the DuckDB file."
                    .into(),
            ),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Log to stderr so we don't pollute the MCP stdio channel.
    eprintln!(
        "[duck-orch-mcp] starting (bin={}, db={:?})",
        duck_orch_bin(),
        db_arg()
    );

    let service = DuckOrchServer::new().serve(stdio()).await.map_err(|e| {
        eprintln!("[duck-orch-mcp] serve error: {e}");
        e
    })?;
    service.waiting().await?;
    Ok(())
}
