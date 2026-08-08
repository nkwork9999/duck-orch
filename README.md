# duckOrch

**Asset-centric data orchestration with lineage, partitions, and a sensor
loop**, packaged as a single DuckDB extension. Define tasks → automatic DAG +
asset graph, partition-aware execution, Dagster-style declarative automation,
Snowflake-compatible `CREATE DYNAMIC ASSET` syntax, MCP server for Claude Code,
Mermaid visualization, and OpenLineage emission — all in one `LOAD`.

```sql
LOAD duckorch;
PRAGMA orch_register('./tasks/');                            -- load *.sql files
PRAGMA orch_backfill('analytics.daily', '2026-01-01', '2026-12-31');
PRAGMA orch_sensor_start;                                    -- auto-run when upstream updates
PRAGMA orch_create_dynamic_asset(
  'analytics.region_total', '5 minutes',
  'SELECT region, SUM(total) FROM analytics.daily GROUP BY region');
```

```bash
duck-orch register ./tasks/
duck-orch asset partitions analytics.daily      # ✅⚪ calendar view
duck-orch backfill analytics.daily --from 2026-01-01 --to 2026-12-31
duck-orch automation simulate analytics.region_total
duck-orch dynamic migrate-from-snowflake snowflake_dump.sql
```

---

## What it does

### Core (Phase 0–9)

| | |
|---|---|
| **Task definition** | One SQL file = one task. Metadata in `-- @key value` comment headers (SQLMesh-style). |
| **Dependency resolution** | DAG built from `inputs`/`outputs`. `sqlparser-rs` infers them from the SQL if you don't. |
| **Execution** | Topological order, parallel within a layer (`SET orch_max_parallel = 4`). |
| **Failure handling** | Exponential backoff retry, then skip downstream tasks. |
| **Incremental** | `@incremental_by ts` + `{{ last_processed_at }}` for delta processing. |
| **Tests / lineage / visualization / OpenLineage / scheduling** | See `DESIGN.md` for the originally-shipped Phase 0–9 surface. |

### Asset + Partition + Sensor (Phase 11–17)

| | |
|---|---|
| **MCP server** | `duck-orch-mcp` stdio server (`rmcp 0.3`) exposes 9 tools (`list_pipelines`, `list_assets`, `run_pipeline` with `dry_run` default, etc.) for direct Claude Code integration. |
| **Asset as first-class** | `@asset name=...` (or auto-derived from `@outputs`) promotes a task's output to `__orch__.assets`. Per-Asset materialization history, code_version hash, declared edges, owner, group, description, tags. |
| **Partitions** | `@partitions_by daily(start=2026-01-01)` / `static(jp,us,eu)` / `multi(date=..., region=...)`. `$partition_key` is bound via DuckDB `PREPARE` (multi-statement aware) so the task SQL runs once per partition. |
| **Backfill** | `duck-orch backfill <asset> --from D --to D \| --partition K \| --missing` with calendar-style ✅🟡❌⚪ ASCII output. |
| **DSL Automation** | `@automation eager \| on_cron(...) \| on_interval(...) \| on_missing \| freshness_violated \| in_progress` plus `&` / `\|` / `!` operators. Stateless evaluator with `target_lag_seconds` throttle wrapper. |
| **Interval tracking** | SQLMesh-style: `@automation on_interval("daily")` + `@interval_start 2026-01-01` tracks *which* time intervals have been processed (`__orch__.asset_intervals`) instead of comparing wall-clock timestamps. Gaps are computed set-difference style, contiguous gaps run as one batch, and `${interval_start}` / `${interval_end}` (ISO UTC) + `${interval_start_ts}` / `${interval_end_ts}` (epoch) are substituted into the task SQL. `@lookback N` re-processes trailing intervals for late data; `@allow_partials` includes the in-progress interval. `PRAGMA orch_restate('asset', '2026-06-01', '2026-06-03')` deletes stored intervals so the next tick recomputes them. |
| **Sensor loop** | Background `std::thread` polls every N seconds (`PRAGMA orch_sensor_set_interval`), evaluates eligible Assets via the Rust evaluator, logs to `__orch__.automation_evaluations`, and fires `RunSingleTask` when `condition_met`. |
| **Freshness + Asset Check** | `@freshness max_lag=60min` ties into `FreshnessViolated`. `@check name=N "<SQL>" expect <op> <value>` runs at end of every successful task; `severity=error` failures block downstream. `${asset}` substituted at execution. |
| **Snowflake `CREATE DYNAMIC ASSET`** | `PRAGMA orch_create_dynamic_asset(name, target_lag, sql)` synthesizes a task + Asset + `automation_condition='eager()'` so the sensor picks it up. `duck-orch dynamic migrate-from-snowflake <dump>` parses a Snowflake dump and registers every block (skipping `WAREHOUSE`/`REFRESH_MODE`/etc.). |
| **3-route surface** | Every feature is reachable from CLI (`duck-orch ...`), SQL (`PRAGMA orch_*`), and MCP (Claude Code tools). |

The primary positioning: **"DuckDB-native, offline, single-file Asset
orchestrator with Dagster's Declarative Automation + Snowflake's
`TARGET_LAG` semantics."** Designed to run on a laptop (or in a plane)
without a cloud control plane.

---

## Quick start

### 1. Build

```bash
git clone --recurse-submodules https://github.com/nkwork9999/duck-orch.git
cd duck-orch
make                                    # full DuckDB-bundled build (~10–30 min)
cargo build -p duckorch_cli --release  # CLI binary
```

Outputs:
- `build/release/duckdb` — DuckDB CLI with duckorch pre-loaded
- `build/release/extension/duckorch/duckorch.duckdb_extension` — extension binary
- `target/release/duck-orch` — standalone CLI

### 2. Define tasks

```sql
-- tasks/clean_users.sql
-- @task name=clean_users
-- @outputs analytics.clean_users
-- @retries 2

CREATE OR REPLACE TABLE analytics.clean_users AS
SELECT id, name, country FROM raw.users WHERE deleted_at IS NULL;
```

```sql
-- tasks/user_stats.sql
-- @task name=user_stats
-- @outputs analytics.user_stats
-- @test "SELECT COUNT(*) FROM analytics.user_stats WHERE users < 0" expect 0

CREATE OR REPLACE TABLE analytics.user_stats AS
SELECT country, COUNT(*) AS users
FROM analytics.clean_users
GROUP BY country;
```

`inputs` is auto-extracted from the SQL, so you don't have to write it.

### 3. Run

```bash
duck-orch register ./tasks/
duck-orch run
duck-orch graph
```

Or from inside DuckDB:

```sql
LOAD duckorch;
PRAGMA orch_register('./tasks/');
PRAGMA orch_run;
SELECT task_name, status FROM __orch__.runs ORDER BY started_at;
```

---

## Task file format

```sql
-- Core (Phase 0–9)
-- @task name=user_stats               required (or `-- @name user_stats`)
-- @description Active users per country
-- @owner data-team@example.com
-- @inputs analytics.clean_users        optional (auto-extracted)
-- @outputs analytics.user_stats         required (or auto-extracted)
-- @depends_on clean_users               optional (inferred from inputs)
-- @schedule "0 6 * * *"                 5-field cron
-- @retries 3
-- @timeout 300                          seconds
-- @incremental_by updated_at            incremental column
-- @tags daily, analytics
-- @test "SELECT COUNT(*) FROM x WHERE y < 0" expect 0

-- Asset / Partition (Phase 13–14)
-- @asset name=analytics.user_stats     promotes to first-class Asset
-- @asset_kind table                    | view | external | file | model
-- @asset_group sales
-- @asset_owner data-team@example.com
-- @asset_description "Active users per country"
-- @asset_tags daily, sales
-- @partitions_by daily(start=2026-01-01)
-- @param partition_key:DATE             declares $partition_key for PREPARE

-- Automation / Freshness / Check (Phase 15–16)
-- @automation eager AND NOT in_progress()
-- @target_lag 5min                      Snowflake-style declarative freshness
-- @freshness max_lag=60min              wires into freshness_violated()
-- @check name=positive "SELECT MIN(rev) FROM ${asset}" expect gt 0
-- @check_severity error                 | warn

-- Interval tracking (Phase 18, SQLMesh-style)
-- @automation on_interval("daily")      | "hourly" | "5min" | "2h" | "3d"
-- @interval_start 2026-01-01            track gaps from this UTC date
-- @lookback 2                           re-process last N intervals (late data)
-- @allow_partials                       include the in-progress interval

<SQL body referencing $partition_key etc.>
```

### Variable substitution

| Style | Use | Mechanism |
|---|---|---|
| `$name` | new code (Phase 12+): partition keys, typed params | DuckDB native `PREPARE` + named bind, multi-statement aware |
| `${asset}` | identifier interpolation in `@check` SQL | plain string substitution |
| `${interval_start}` / `${interval_end}` | Phase 18 `on_interval()` runs: quoted ISO UTC timestamp literal (`WHERE ts >= ${interval_start}` works as-is); `${interval_start_ts}` / `${interval_end_ts}` give bare epoch seconds | plain string substitution |
| `{{ var }}` | legacy `@incremental_by` (Phase 7) | self-contained 33-line substitution (no Jinja crate); kept for back-compat, **not used in new features** |

Supported `{{}}` variables: `{{ last_processed_at }}`, `{{ now }}`, `{{ run_id }}`.

### Snowflake-style declaration

If you'd rather skip the headers entirely:

```sql
-- snowflake_dump.sql
CREATE DYNAMIC TABLE analytics.region_total
  TARGET_LAG = '5 minutes'
  AS
  SELECT region, SUM(total) AS rt
  FROM analytics.daily_orders
  GROUP BY region;
```

```bash
duck-orch dynamic migrate-from-snowflake snowflake_dump.sql
```

…registers each block as an Asset with `automation_condition='eager()'` and
`target_lag_seconds=300`. The sensor materializes it within `target_lag` of
upstream changes.

---

## CLI

```
duck-orch [--db <path>] [--ext <path>] <subcommand> [--json]

# Core (Phase 0–9)
  register <dir>                       Load tasks from a directory of .sql files
  run [--partition <key>]              Execute the DAG (or one partition)
  status                               Recent run history
  graph [lineage|dag|combined]         Mermaid output
  test                                 Run @test assertions
  validate <file>                      Validate one file (returns structured JSON)
  impact <table>                       What breaks if I change <table>?
  lineage <table>                      Upstream lineage of <table>
  schedule add <name> <cron>           Register a cron schedule
  schedule daemon                      Long-running poll loop (run-due every 30s)

# Asset / Partition (Phase 13–14)
  asset list [--group <name>]          All registered Assets
  asset show <name>                    Single Asset details
  asset lineage <name>                 Mermaid (Asset-level)
  asset materializations <name>        Per-partition history
  asset partitions <name>              Calendar-style ✅⚪ view
  asset health                         Freshness + 24h run stats
  backfill <asset> --from D --to D     Range
  backfill <asset> --partition K       Single partition
  backfill <asset> --missing           Only unmaterialized

# Automation / Sensor (Phase 15)
  automation status                    Per-Asset condition_met + reason
  automation simulate <asset>          Dry-run (no log, no fire)
  sensor start|stop|status             Toggle the background loop
  sensor set-interval <seconds>        Polling cadence (default 30s)

# Asset Check (Phase 16)
  check run <asset>                    Execute every check declared on <asset>
  check history <asset> [--limit N]    Recent results

# Dynamic Asset / Snowflake migration (Phase 17)
  dynamic list                         All Assets with automation_condition
  dynamic refresh <asset>              Force-run, bypassing target_lag throttle
  dynamic create <name> --target-lag <dur> --sql <inline>
  dynamic create-from-sql <file>       Parse Snowflake-style file, register each block
  dynamic migrate-from-snowflake <file>      Alias of create-from-sql

# JSON ingestion (Phase 19)
  ingest preview <path> <target>       Tables the source would produce; writes nothing
  ingest run <path> <target> [--disposition append|replace|merge]
                                       [--primary-key <cols>] [--max-nesting N]
  ingest http <url> <target> [--secret <name> | --token <t>] [--paginate ...]
  ingest schema [<dataset>]            Schema ledger versions
  ingest changes [<dataset>]           What each version changed
  ingest loads [--limit N]             Load history, failures included
  ingest state / ingest reset <source> <resource>
```

Pass `--json` to any subcommand for Claude / agent-parseable output.

### MCP server (Phase 11)

`duck-orch-mcp` exposes the CLI surface over stdio MCP for direct
Claude Code integration:

```jsonc
// ~/.claude.json
{
  "mcpServers": {
    "duckorch": {
      "command": "/abs/path/to/target/release/duck-orch-mcp",
      "env": {
        "DUCKDB_BIN": "/abs/path/to/build/release/duckdb",
        "DUCKORCH_EXT": "/abs/path/to/build/release/extension/duckorch/duckorch.duckdb_extension",
        "DUCK_ORCH_DB": "/abs/path/to/state.duckdb"
      }
    }
  }
}
```

Read-only tools (always safe): `list_pipelines`, `list_assets`, `list_runs`,
`describe_task`, `get_lineage`, `impact`, `validate`.
Write tools (defaulted to safe modes): `run_pipeline` (`dry_run=true`),
`register_task` (path-required), `unregister_task` (`confirm=true`),
`schedule_add`.

---

## SQL API

```sql
LOAD duckorch;

-- Configuration
SET orch_max_parallel = 4;
SET orch_openlineage_url = 'http://marquez:5000/api/v1/lineage';
SET orch_openlineage_debug = true;
SET orch_namespace = 'my-warehouse';        -- OL job namespace
SET orch_capture_interactive = true;        -- capture ad-hoc CTAS/INSERT

-- Core (Phase 0–9)
PRAGMA orch_init;                       -- create __orch__ schema
PRAGMA orch_register('./tasks/');       -- load directory
PRAGMA orch_run;                        -- execute the DAG
PRAGMA orch_test;                       -- run @test assertions
PRAGMA orch_visualize('lineage');       -- Mermaid

-- Asset / Partition (Phase 13–14)
PRAGMA orch_asset_list;                                  -- registered Assets
PRAGMA orch_asset_show('analytics.user_stats');
PRAGMA orch_asset_lineage('analytics.user_stats');
PRAGMA orch_asset_materializations('analytics.user_stats', 50);
PRAGMA orch_asset_partitions('analytics.daily');
PRAGMA orch_asset_partitions_calendar('analytics.daily');
PRAGMA orch_asset_health;
PRAGMA orch_backfill('analytics.daily', '2026-01-01', '2026-12-31');
PRAGMA orch_backfill_missing('analytics.daily');
PRAGMA orch_run_partition('analytics.daily', '2026-05-17');

-- Automation / Sensor (Phase 15)
PRAGMA orch_automation_status;                  -- evaluation snapshot
PRAGMA orch_automation_simulate('analytics.x'); -- dry-run
PRAGMA orch_sensor_start;
PRAGMA orch_sensor_stop;
PRAGMA orch_sensor_status;
PRAGMA orch_sensor_set_interval(30);

-- Asset Check (Phase 16)
PRAGMA orch_check_run('analytics.x');
PRAGMA orch_check_history('analytics.x', 100);

-- Dynamic Asset (Phase 17, Snowflake-compatible)
PRAGMA orch_create_dynamic_asset(
  'analytics.region_total', '5 minutes',
  'SELECT region, SUM(total) FROM analytics.daily GROUP BY region');
PRAGMA orch_dynamic_list;
PRAGMA orch_dynamic_refresh('analytics.region_total');

-- Pure scalar functions
SELECT orch_extract_io('INSERT INTO x SELECT * FROM y');
SELECT orch_extract_column_lineage(
  'CREATE TABLE z AS SELECT a, UPPER(b) AS B_UP FROM t', '');

-- State tables (Phase 0–17)
SELECT * FROM __orch__.tasks;
SELECT * FROM __orch__.runs WHERE status = 'failed';
SELECT * FROM __orch__.lineage_edges;
SELECT * FROM __orch__.column_lineage WHERE via_task = 'my_task';
SELECT * FROM __orch__.assets;                       -- Phase 13
SELECT * FROM __orch__.asset_materializations;       -- Phase 13/14
SELECT * FROM __orch__.asset_edges;                  -- Phase 13/15
SELECT * FROM __orch__.asset_partitions;             -- Phase 14
SELECT * FROM __orch__.automation_evaluations;       -- Phase 15
SELECT * FROM __orch__.asset_checks;                 -- Phase 16
SELECT * FROM __orch__.asset_check_results;          -- Phase 16
SELECT * FROM __orch__.ingest_schemas;               -- Phase 19
SELECT * FROM __orch__.ingest_schema_changes;        -- Phase 19
SELECT * FROM __orch__.ingest_loads;                 -- Phase 19
SELECT * FROM __orch__.ingest_state;                 -- Phase 19
SELECT * FROM __orch__.ingest_fetches;               -- Phase 19
```

### JSON ingestion (Phase 19)

Nested JSON becomes flat parent/child tables, and a schema ledger absorbs
columns that appear later. Type inference is DuckDB's own: the source is read
through `read_json` and the resulting types drive the normalization.

```sql
-- see the shape first; nothing is written
PRAGMA orch_ingest_preview('orders.jsonl', 'raw.orders');

-- load a file source
PRAGMA orch_ingest_run('orders/*.jsonl', 'raw.orders');
PRAGMA orch_ingest_run('orders.jsonl', 'raw.orders',
                       disposition = 'merge', primary_key = 'id');

-- load an API, resuming from the cursor the previous run stored
CREATE SECRET orders_api (TYPE http, BEARER_TOKEN '...');
PRAGMA orch_ingest_http('https://api.example.com/orders', 'raw.orders',
                        secret = 'orders_api', paginate = 'cursor',
                        records_path = 'data', cursor_path = 'meta.next',
                        cursor_param = 'cursor', resource = 'orders');

PRAGMA orch_ingest_state;
PRAGMA orch_ingest_reset('https://api.example.com/orders', 'orders');
```

| | |
|---|---|
| **Normalization** | Structs flatten into the parent (`customer__name`); arrays become child tables (`orders__items`). Control columns `_orch_id` / `_orch_parent_id` / `_orch_index` / `_orch_load_id` link and order the rows. `max_nesting` (default 3) caps how deep child tables go; anything deeper stays a JSON string column. |
| **Schema ledger** | `ingest_schemas` gains a version only when the shape actually moves. Added columns and widening types (`INTEGER → BIGINT`, anything → `VARCHAR`) are absorbed with `ALTER`; an incompatible change fails the load with the column named. Columns that vanish stay and take NULLs. |
| **Dispositions** | `append` (default), `replace` (keep only this load), `merge` (needs `primary_key`; replaces earlier rows whose key came back, children included). |
| **HTTP** | `none` / `page` / `offset` / `cursor` / `link` pagination, bearer token from a DuckDB secret, resume cursor in `ingest_state`, per-page log in `ingest_fetches`. A `max_pages` ceiling is reported as `truncated`, never silently. |
| **Destination** | A three-part target (`lake.raw.orders`) loads into an attached catalog, so a DuckLake lakehouse is just a different target. |
| **Lineage** | Loaded tables register as Assets with a materialization per load, parent→child asset edges, and a `file://` / `http://` upstream in `lineage_edges`. |

---

## Architecture

A "**thin C++ shim + Rust core**" sandwich.

```
┌─ C++ extension (~3000 lines) ────────────────────────┐
│  Registers PRAGMA / scalar functions                 │
│  Executes SQL via per-thread Connection              │
│  std::thread parallel dispatch                       │
│  PREPARE + named bind for $param (multi-stmt aware)  │
│  Sensor std::thread (Phase 15) — evaluates           │
│    AutomationConditions every N seconds              │
│  OptimizerExtension hook for ad-hoc column lineage   │
│  Catalog API for DuckLake namespace                  │
└──────────────┬───────────────────────────────────────┘
               ↕ extern "C" FFI
┌─ Rust workspace ─────────────────────────────────────┐
│  orch_common   Task, PartitionDef, ParamSpec,        │
│                AutomationCondition + evaluator,      │
│                AssetCheck, Snowflake dump parser,    │
│                FNV-1a code_version                   │
│  orch_dag      DAG, topo layers, Mermaid             │
│  orch_lineage  sqlparser-rs + column module          │
│  orch_runtime  Header parser ($param/binding,        │
│                @asset / @partitions_by /             │
│                @automation / @target_lag /           │
│                @freshness / @check), legacy {{}}     │
│  orch_ol       OpenLineage HTTP worker               │
│  orch_core     extern "C" facade (FFI shims)         │
│  orch_cli      duck-orch binary (3-route CLI)        │
│  orch_mcp      duck-orch-mcp (rmcp stdio, Phase 11)  │
└──────────────────────────────────────────────────────┘
```

Why the C++ layer? DuckDB's stable C extension API doesn't yet expose
optimizer / parser hooks, so a pure-Rust extension can't observe queries.
The C++ shim handles DuckDB-internal calls; all logic lives in Rust.
Same pattern as `ducksmiles`.

See [DESIGN.md](DESIGN.md) for the full design.

---

## Status

All ROADMAP phases (0–17) shipped. See [ROADMAP.md](ROADMAP.md) for the design
of Phase 11–17 (Asset-centric, sensor-driven evolution).

| Phase | Topic | Status |
|---|---|---|
| 0 | Project skeleton | ✅ |
| 1 | Parser + DAG + execution | ✅ |
| 2 | Optimizer hook (auto-interception) | ✅ (opt-in, `orch_capture_interactive`) |
| 3 | Mermaid visualization | ✅ |
| 4 | Retry + downstream skip | ✅ |
| 5 | Parallel execution | ✅ |
| 6 | CLI | ✅ |
| 7 | Incremental + tests | ✅ |
| 8 | Scheduler | ✅ |
| 9 | OpenLineage events | ✅ |
| 11 | MCP server (`duck-orch-mcp`, stdio, 9 tools) | ✅ |
| 12 | DuckDB-native `$param` binding | ✅ |
| 13 | Asset as first-class entity (schema + read API + edge projection) | ✅ |
| 14 | Partition (Daily/Static/Multi, backfill, calendar) | ✅ |
| 15 | AutomationCondition DSL + `@target_lag` + sensor | ✅ |
| 16 | Freshness + Asset Check + SLA surfaces | ✅ |
| 17 | `CREATE DYNAMIC ASSET` + Snowflake dump migration | ✅ |
| + | Column-level lineage with subtype taxonomy | ✅ |
| + | DuckLake-aware OL namespace resolution | ✅ |
| + | OpenLineage `columnLineage` facet emission | ✅ |

---

## Development

```bash
cargo test --workspace --release    # Rust unit tests
make                                # full DuckDB-bundled build
make debug                          # debug build

# Iterate quickly on Rust only
cargo build -p duckorch_core --release
cargo build -p duckorch_cli --release
```

DuckDB and `extension-ci-tools` are pinned at `v1.5.1` (submodules).
When upgrading, bump both together.

---

## License

MIT. See [LICENSE](LICENSE).

## Credits

All code in duckOrch is original. Design ideas (only — no code copied)
were drawn from:

- [ducksmiles](https://github.com/duckdb/community-extensions/tree/main/extensions/ducksmiles) — C++ + Rust hybrid build layout
- [SQLMesh](https://sqlmesh.readthedocs.io) — comment-header task file format
- [dbt](https://docs.getdbt.com) — testing, downstream skip, `--full-refresh`
- [OpenLineage](https://openlineage.io) — event spec (Apache-2.0; compatibility comes from following a public spec, not from any specific implementation)
- [Dagster](https://docs.dagster.io) (Phase 13–16) — Software-Defined Assets concept, `AutomationCondition`, partition + backfill UX
- [Snowflake Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about) (Phase 17) — `TARGET_LAG` declarative freshness; `CREATE DYNAMIC ASSET` syntax is intentionally close to `CREATE DYNAMIC TABLE` so dumps migrate without rewriting
- [Anthropic Model Context Protocol](https://github.com/modelcontextprotocol) (Phase 11) — `rmcp` Rust SDK powers the stdio MCP server

duckOrch's positioning is "**laptop-sized Asset orchestrator with cloud-grade
semantics**." It is not trying to replace Airflow / Dagster / Snowflake at
enterprise scale; it is trying to deliver their best ideas in a single
`LOAD duckorch;` you can run on a plane.
