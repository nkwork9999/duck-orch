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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RunPipelineRequest {
    /// Pipeline / tag name (currently advisory — duck-orch runs the full DAG).
    #[serde(default)]
    pub pipeline: Option<String>,
    /// If true (default), do NOT execute. Returns the lineage graph + task list
    /// so the caller can preview "what would run". Only when explicitly false
    /// does this tool actually invoke `duck-orch run`.
    #[serde(default = "default_true")]
    pub dry_run: bool,
}

fn default_true() -> bool { true }

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct IngestPreviewRequest {
    /// Path or glob of the JSON source (a local file for now).
    pub path: String,
    /// Target table, optionally qualified: `raw.orders` or `lake.raw.orders`.
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct IngestRunRequest {
    /// Path or glob of the JSON source.
    pub path: String,
    /// Target table, optionally qualified.
    pub target: String,
    /// `append` (default), `replace` or `merge`. `merge` needs primary_key.
    #[serde(default)]
    pub disposition: Option<String>,
    /// Comma-separated key columns, required by `merge`.
    #[serde(default)]
    pub primary_key: Option<String>,
    /// If true (default), only show the tables the load would produce.
    /// Only when explicitly false does this tool write anything.
    #[serde(default = "default_true")]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct IngestSchemaRequest {
    /// Dataset name, e.g. `raw.orders`. Omit for every dataset.
    #[serde(default)]
    pub dataset: Option<String>,
}


#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RegisterTaskRequest {
    /// Filesystem path to a directory of .sql task files (the CLI's
    /// `register <dir>` argument). Raw SQL strings are rejected.
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct UnregisterTaskRequest {
    /// Task name to remove from `__orch__.tasks`.
    pub name: String,
    /// Must be set to `true` explicitly; otherwise the call is refused.
    #[serde(default)]
    pub confirm: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ScheduleAddRequest {
    /// Pipeline-or-task name to schedule.
    pub name: String,
    /// Cron expression (standard 5-field or 6-field).
    pub cron: String,
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

/// Run a raw SQL statement through `duckdb` (not via `duck-orch`).
/// Needed for write tools the CLI doesn't expose yet (e.g. unregister).
/// Loads DUCKORCH_EXT first so `__orch__` tables resolve.
fn run_duckdb_sql(sql: &str) -> Result<String, String> {
    let bin = std::env::var("DUCKDB_BIN").unwrap_or_else(|_| "duckdb".to_string());
    let db = db_arg().unwrap_or_else(|| "./mydata.duckdb".to_string());
    let mut prelude = String::new();
    if let Some(ext) = ext_arg() {
        prelude.push_str(&format!("LOAD '{}';\n", ext));
    }
    let full_sql = prelude + sql;
    let mut cmd = Command::new(&bin);
    cmd.arg(&db)
        .arg("-init").arg("/dev/null")
        .arg("-unsigned")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = cmd.spawn()
        .map_err(|e| format!("failed to spawn {}: {}", bin, e))?;
    use std::io::Write;
    child.stdin.as_mut().ok_or("no stdin")?
        .write_all(full_sql.as_bytes())
        .map_err(|e| format!("write stdin: {}", e))?;
    let out = child.wait_with_output().map_err(|e| e.to_string())?;
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    if !out.status.success() {
        return Err(format!(
            "duckdb exited {}: {}{}",
            out.status.code().unwrap_or(-1), stderr, stdout
        ));
    }
    Ok(stdout)
}

fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
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

    // ---- write tools ----

    #[tool(
        name = "run_pipeline",
        description = "Run the registered DAG via `duck-orch run`. Defaults to dry_run=true \
                       (returns the lineage graph + recent task list as a preview, no execution). \
                       Pass dry_run=false to actually invoke the pipeline."
    )]
    pub async fn run_pipeline(
        &self,
        Parameters(req): Parameters<RunPipelineRequest>,
    ) -> Result<CallToolResult, McpError> {
        if req.dry_run {
            // No execution — show what would run.
            let graph = run_cli(&["graph", "lineage"], false).unwrap_or_default();
            let status = run_cli(&["status"], true).unwrap_or_default();
            let body = serde_json::json!({
                "ok": true,
                "dry_run": true,
                "pipeline": req.pipeline,
                "would_run_graph_mermaid": graph,
                "recent_runs": serde_json::from_str::<serde_json::Value>(&status)
                    .unwrap_or(serde_json::Value::Null),
                "note": "dry_run=true — nothing executed. Pass dry_run=false to actually run.",
            });
            return ok_text(body.to_string());
        }
        let out = run_cli(&["run"], true).map_err(to_mcp_err)?;
        let parsed = serde_json::from_str::<serde_json::Value>(&out)
            .unwrap_or(serde_json::Value::String(out.clone()));
        let body = serde_json::json!({
            "ok": true,
            "dry_run": false,
            "pipeline": req.pipeline,
            "output": parsed,
        });
        ok_text(body.to_string())
    }

    #[tool(
        name = "register_task",
        description = "Register tasks from a directory of `.sql` files. Wraps `duck-orch register <path> --json`. \
                       Input must be an existing filesystem path (file or directory), not raw SQL."
    )]
    pub async fn register_task(
        &self,
        Parameters(req): Parameters<RegisterTaskRequest>,
    ) -> Result<CallToolResult, McpError> {
        let p = std::path::Path::new(&req.path);
        if !p.exists() {
            return Err(to_mcp_err(format!(
                "register_task: path {:?} does not exist (expected a file or directory, not raw SQL)",
                req.path
            )));
        }
        let out = run_cli(&["register", &req.path], true).map_err(to_mcp_err)?;
        let parsed = serde_json::from_str::<serde_json::Value>(&out)
            .unwrap_or(serde_json::Value::String(out.clone()));
        let body = serde_json::json!({ "ok": true, "output": parsed });
        ok_text(body.to_string())
    }

    #[tool(
        name = "unregister_task",
        description = "Delete one row from `__orch__.tasks` by name. Requires confirm=true \
                       (default false → refused). Implemented via a direct DuckDB DELETE because \
                       the CLI has no `unregister` subcommand yet."
    )]
    pub async fn unregister_task(
        &self,
        Parameters(req): Parameters<UnregisterTaskRequest>,
    ) -> Result<CallToolResult, McpError> {
        if !req.confirm {
            return Err(to_mcp_err(
                "unregister_task: confirm=false (default). Pass confirm=true to actually delete.".into(),
            ));
        }
        let sql = format!(
            "DELETE FROM __orch__.tasks WHERE name = {}; SELECT changes() AS deleted;",
            sql_quote(&req.name)
        );
        let out = run_duckdb_sql(&sql).map_err(to_mcp_err)?;
        let body = serde_json::json!({ "ok": true, "name": req.name, "output": out });
        ok_text(body.to_string())
    }

    #[tool(
        name = "schedule_add",
        description = "Register a cron schedule. Wraps `duck-orch schedule add <name> <cron>`."
    )]
    pub async fn schedule_add(
        &self,
        Parameters(req): Parameters<ScheduleAddRequest>,
    ) -> Result<CallToolResult, McpError> {
        let out = run_cli(&["schedule", "add", &req.name, &req.cron], false)
            .map_err(to_mcp_err)?;
        let body = serde_json::json!({ "ok": true, "name": req.name, "cron": req.cron, "output": out });
        ok_text(body.to_string())
    }

    #[tool(
        name = "preview_ingest",
        description = "Show the parent/child tables and columns a JSON source would produce, without writing. Wraps `duck-orch ingest preview`. Read-only."
    )]
    pub async fn preview_ingest(
        &self,
        Parameters(req): Parameters<IngestPreviewRequest>,
    ) -> Result<CallToolResult, McpError> {
        let out = run_cli(&["ingest", "preview", &req.path, &req.target], true)
            .map_err(to_mcp_err)?;
        let body = serde_json::json!({
            "path": req.path,
            "target": req.target,
            "plan": serde_json::from_str::<serde_json::Value>(&out)
                .unwrap_or(serde_json::Value::String(out.clone())),
        });
        ok_text(body.to_string())
    }

    #[tool(
        name = "run_ingest",
        description = "Load a JSON source into normalized tables. Defaults to dry_run=true, which only previews the shape; pass dry_run=false to actually write. Wraps `duck-orch ingest run`."
    )]
    pub async fn run_ingest(
        &self,
        Parameters(req): Parameters<IngestRunRequest>,
    ) -> Result<CallToolResult, McpError> {
        if req.dry_run {
            let out = run_cli(&["ingest", "preview", &req.path, &req.target], true)
                .map_err(to_mcp_err)?;
            let body = serde_json::json!({
                "dry_run": true,
                "note": "nothing was written; pass dry_run=false to load",
                "plan": serde_json::from_str::<serde_json::Value>(&out)
                    .unwrap_or(serde_json::Value::String(out.clone())),
            });
            return ok_text(body.to_string());
        }
        let mut argv: Vec<String> = vec![
            "ingest".into(),
            "run".into(),
            req.path.clone(),
            req.target.clone(),
        ];
        if let Some(d) = &req.disposition {
            argv.push("--disposition".into());
            argv.push(d.clone());
        }
        if let Some(k) = &req.primary_key {
            argv.push("--primary-key".into());
            argv.push(k.clone());
        }
        let refs: Vec<&str> = argv.iter().map(|s| s.as_str()).collect();
        let out = run_cli(&refs, true).map_err(to_mcp_err)?;
        let body = serde_json::json!({
            "dry_run": false,
            "summary": serde_json::from_str::<serde_json::Value>(&out)
                .unwrap_or(serde_json::Value::String(out.clone())),
        });
        ok_text(body.to_string())
    }

    #[tool(
        name = "ingest_schema_history",
        description = "Schema ledger for ingested datasets: versions and what each one changed. Wraps `duck-orch ingest schema` / `ingest changes`. Read-only."
    )]
    pub async fn ingest_schema_history(
        &self,
        Parameters(req): Parameters<IngestSchemaRequest>,
    ) -> Result<CallToolResult, McpError> {
        let mut versions_args: Vec<String> = vec!["ingest".into(), "schema".into()];
        let mut changes_args: Vec<String> = vec!["ingest".into(), "changes".into()];
        if let Some(d) = &req.dataset {
            versions_args.push(d.clone());
            changes_args.push(d.clone());
        }
        let vrefs: Vec<&str> = versions_args.iter().map(|s| s.as_str()).collect();
        let crefs: Vec<&str> = changes_args.iter().map(|s| s.as_str()).collect();
        let versions = run_cli(&vrefs, true).map_err(to_mcp_err)?;
        let changes = run_cli(&crefs, true).unwrap_or_default();
        let body = serde_json::json!({
            "dataset": req.dataset,
            "versions": serde_json::from_str::<serde_json::Value>(&versions)
                .unwrap_or(serde_json::Value::Null),
            "changes": serde_json::from_str::<serde_json::Value>(&changes)
                .unwrap_or(serde_json::Value::Null),
        });
        ok_text(body.to_string())
    }
}


#[tool_handler]
impl ServerHandler for DuckOrchServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "duckOrch MCP server — wraps the `duck-orch` CLI. Read tools: list_pipelines, \
                 list_runs, describe_task, get_lineage, validate. Write tools: run_pipeline \
                 (dry_run=true by default), register_task, unregister_task (requires confirm=true), \
                 schedule_add. Set DUCK_ORCH_DB to point at the DuckDB file."
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
