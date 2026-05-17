# orch_mcp — duckOrch MCP server

A thin [MCP (Model Context Protocol)](https://modelcontextprotocol.io) server
that wraps the existing `duck-orch` CLI so Claude Code (or any MCP client) can
drive duckOrch over **stdio**.

Built on the official Rust SDK [`rmcp`](https://crates.io/crates/rmcp) v0.3
with features `server`, `macros`, `transport-io`.

## What it exposes

### Read-only

| Tool             | Wraps                              | Notes |
|------------------|------------------------------------|-------|
| `list_pipelines` | `duck-orch status --json`          | (Punt: a dedicated `tasks` subcommand on the CLI would be cleaner.) |
| `list_runs`      | `duck-orch status --json`          | `limit` argument (default 20) truncates client-side. |
| `describe_task`  | `duck-orch lineage` + `impact`     | Returns upstream + downstream edges for the task's output table. |
| `get_lineage`    | `duck-orch lineage` + `graph`      | Returns rows + full Mermaid graph. |
| `validate`       | `duck-orch validate <file> --json` | Parse + validate one `.sql` task file. |

### Write (safety-defaulted)

| Tool               | Wraps                                                | Safety default |
|--------------------|------------------------------------------------------|----------------|
| `run_pipeline`     | `duck-orch run --json`                               | **`dry_run=true`** — returns the lineage Mermaid + recent runs as a preview, no execution. Pass `dry_run=false` to actually run. |
| `register_task`    | `duck-orch register <path> --json`                   | Rejects unless `path` is an existing file/directory (no raw SQL). |
| `unregister_task`  | `DELETE FROM __orch__.tasks` via direct `duckdb`     | **Requires `confirm=true`** (default `false` → refused). The CLI has no `unregister` subcommand yet, so this tool talks to `duckdb` directly using `DUCK_ORCH_DB` / `DUCKORCH_EXT`. |
| `schedule_add`     | `duck-orch schedule add <name> <cron>`               | Cron is validated by the CLI. |

`schedule_remove` is not exposed (the CLI has no matching subcommand yet — punted).

## Build

```bash
cargo build -p orch_mcp --release
# binary: target/release/duck-orch-mcp
```

Requires the `duck-orch` binary (crate `duckorch_cli`) to be on `$PATH`,
**and** the duckOrch DuckDB extension already built (see top-level README).

## Configure via env

| Var              | Default                 | Purpose |
|------------------|-------------------------|---------|
| `DUCK_ORCH_BIN`  | `duck-orch` (on PATH)   | Path to the `duck-orch` CLI binary. |
| `DUCK_ORCH_DB`   | (CLI default `./mydata.duckdb`) | Passed as `--db <path>` to every subcommand. |
| `DUCKORCH_EXT`   | (CLI auto-detect)       | Passed as `--ext <path>` if set. |

The MCP server itself writes a single boot line to **stderr** then speaks
JSON-RPC over stdio. Do not write anything to stdout from hooks.

## Wire into Claude Code

Add an `mcpServers` entry to `~/.claude.json`:

```jsonc
{
  "mcpServers": {
    "duckorch": {
      "command": "/absolute/path/to/target/release/duck-orch-mcp",
      "args": [],
      "env": {
        "DUCK_ORCH_BIN": "/absolute/path/to/target/release/duck-orch",
        "DUCK_ORCH_DB":  "/absolute/path/to/your/project.duckdb",
        "DUCKORCH_EXT":  "/absolute/path/to/build/release/extension/duckorch/duckorch.duckdb_extension"
      }
    }
  }
}
```

Restart Claude Code and the `duckorch` server should appear under `/mcp`.
Tools will be namespaced as `mcp__duckorch__list_runs`, etc.

## Smoke-test outside Claude

```bash
# Should print a JSON-RPC initialize response and then wait on stdin.
DUCK_ORCH_DB=./mydata.duckdb ./target/release/duck-orch-mcp <<'EOF'
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}
EOF
```
