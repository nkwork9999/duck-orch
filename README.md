# duckOrch

**DAG orchestration with lineage as a side product**, packaged as a single DuckDB extension.
Define tasks → automatic dependency resolution, parallel execution, Mermaid visualization,
and OpenLineage emission, all in one place.

```sql
LOAD duckorch;
PRAGMA orch_register('./tasks/');   -- load *.sql files
PRAGMA orch_run;                    -- execute in DAG order
PRAGMA orch_visualize('lineage');   -- get a Mermaid diagram
```

```bash
duck-orch register ./tasks/
duck-orch run --json
duck-orch graph > pipeline.md       # renders inline on GitHub PRs
```

---

## What it does

| | |
|---|---|
| **Task definition** | One SQL file = one task. Metadata lives in `-- @key value` comment headers (SQLMesh-style). |
| **Dependency resolution** | DAG built from `inputs`/`outputs`. If you don't write them, `sqlparser-rs` infers from the SQL. |
| **Execution** | Topological order, parallel within a layer (`SET orch_max_parallel = 4`). |
| **Failure handling** | Exponential backoff retry, then skip downstream tasks. |
| **Incremental** | `@incremental_by ts` + `{{ last_processed_at }}` for delta processing. |
| **Tests** | `@test "SQL" expect 0` runs assertions after task completion. |
| **Visualization** | Mermaid (`lineage` / `dag` / `combined` modes) via `PRAGMA orch_visualize`. |
| **Observability** | OpenLineage-compatible events POSTed to Marquez / DataHub / etc. |
| **Scheduling** | `duck-orch schedule add NAME "0 6 * * *"` + `daemon` for a polling loop. |
| **Agent integration** | `--json` on every CLI subcommand, structured `validate`, `impact` analysis. |

The lineage table comes for free as a derivative of each task's `inputs`/`outputs`.
The primary feature is **DAG orchestration**.

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

<SQL body>
```

### Jinja-style placeholders (incremental tasks)

```sql
INSERT INTO log
SELECT * FROM raw.events
WHERE event_time > {{ last_processed_at }}
  AND event_time <= {{ now }};
```

Supported variables: `{{ last_processed_at }}`, `{{ now }}`, `{{ run_id }}`.

---

## CLI

```
duck-orch [--db <path>] [--ext <path>] <subcommand> [--json]

  register <dir>           Load tasks from a directory of .sql files
  run                      Execute the DAG
  status                   Recent run history
  graph [lineage|dag|combined]   Mermaid output
  test                     Run @test assertions
  validate <file>          Validate one file (returns structured JSON)
  impact <table>           What breaks if I change <table>?
  lineage <table>          Upstream lineage of <table>
  schedule add <name> <cron>     Register a cron schedule
  schedule daemon          Long-running poll loop (run-due every 30s)
```

Pass `--json` to any subcommand for Claude / agent-parseable output.

---

## SQL API

```sql
LOAD duckorch;

-- Configuration
SET orch_max_parallel = 4;
SET orch_openlineage_url = 'http://marquez:5000/api/v1/lineage';
SET orch_openlineage_debug = true;

-- Operations
PRAGMA orch_init;                       -- create __orch__ schema
PRAGMA orch_register('./tasks/');       -- load directory
PRAGMA orch_run;                        -- execute
PRAGMA orch_test;                       -- run @test assertions
PRAGMA orch_visualize('lineage');       -- Mermaid

-- Pure scalar functions
SELECT orch_extract_io('INSERT INTO x SELECT * FROM y');
-- {"inputs":["y"],"outputs":["x"]}
SELECT orch_render_mermaid(dag_json, 0, '[]');

-- State tables
SELECT * FROM __orch__.tasks;
SELECT * FROM __orch__.runs WHERE status = 'failed';
SELECT * FROM __orch__.lineage_edges;
```

---

## Architecture

A "**thin C++ shim + Rust core**" sandwich.

```
┌─ C++ extension (~700 lines) ─────────────┐
│  Registers PRAGMA / scalar functions     │
│  Executes SQL via per-thread Connection  │
│  std::thread parallel dispatch            │
└──────────────┬───────────────────────────┘
               ↕ extern "C" FFI
┌─ Rust workspace ─────────────────────────┐
│  orch_common   Task type, FFI helpers    │
│  orch_dag      DAG, topo layers, Mermaid │
│  orch_lineage  sqlparser-rs              │
│  orch_runtime  Parser, templating        │
│  orch_ol       OpenLineage HTTP worker   │
│  orch_core     extern "C" facade         │
│  orch_cli      duck-orch binary          │
└──────────────────────────────────────────┘
```

Why the C++ layer? DuckDB's stable C extension API does not yet expose
optimizer / parser hooks, so a pure-Rust extension cannot intercept
queries. The C++ shim handles DuckDB-internal calls; all logic lives
in Rust. Same pattern as `ducksmiles`.

See [DESIGN.md](DESIGN.md) for the full design.

---

## Status

Phases 1 through 9 are complete. Phase 2 (optimizer-hook based query
auto-interception) is deferred — explicit task registration plus
`sqlparser-rs` auto-extraction covers the common case.

| Phase | Topic | Status |
|---|---|---|
| 0 | Project skeleton | ✅ |
| 1 | Parser + DAG + execution | ✅ |
| 2 | Optimizer hook | ⏸ deferred |
| 3 | Mermaid visualization | ✅ |
| 4 | Retry + downstream skip | ✅ |
| 5 | Parallel execution | ✅ |
| 6 | CLI | ✅ |
| 7 | Incremental + tests | ✅ |
| 8 | Scheduler | ✅ |
| 9 | OpenLineage | ✅ |

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
- [duck_lineage](https://github.com/ilum-cloud/duck_lineage) — emitting OpenLineage from DuckDB
- [SQLMesh](https://sqlmesh.readthedocs.io) — comment-header task file format
- [dbt](https://docs.getdbt.com) — testing, downstream skip, `--full-refresh`
- [OpenLineage](https://openlineage.io) — event spec (Apache-2.0; compatibility comes from following a public spec, not from any specific implementation)

duckOrch's positioning is "task execution first, lineage as a derivative
of each task's inputs/outputs". This makes it complementary to
duck_lineage (which observes arbitrary queries via optimizer hooks).
You can load both extensions together.
