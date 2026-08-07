// duck-orch CLI — Phase 6.
//
// Shells out to the duckdb binary (must be in PATH or DUCKDB_BIN env).
// Loads the duckorch extension and dispatches subcommands to PRAGMA / SELECT.
//
// Every subcommand supports --json for Claude / agent-friendly output.

use std::process::{Command, Stdio};

const HELP: &str = "duck-orch — DuckDB orchestration CLI

USAGE:
    duck-orch [--db <path>] [--ext <path>] <subcommand> [args] [--json]

SUBCOMMANDS:
    register <dir>           Load tasks from a directory of .sql files
    run                      Execute the DAG
    status                   Show recent task runs
    graph [mode]             Print Mermaid (mode = lineage|dag|combined, default lineage)
    test                     Run @test assertions
    validate <file>          Parse and validate one task file (writes JSON to stdout)
    impact <table>           Show downstream tasks/tables affected by changing <table>
    lineage <table>          Show upstream lineage of <table>
    asset list [--group <g>]                    List Assets (optional group filter)
    asset show <name>                            Show one Asset row
    asset lineage <name>                         Mermaid for upstream+downstream of an Asset
    asset materializations <name> [--limit N]    Recent materialization history
    asset health                                  Per-Asset health summary (last status + 24h counts)
    asset partitions <name> [--json]             Calendar-style ASCII (or JSON rows)
    run --partition <key> <asset>                Run one partition of an Asset
    backfill <asset> --from <date> --to <date>   Re-run all partitions in [from, to]
    backfill <asset> --partition <key>           Re-run one partition
    backfill <asset> --missing                   Re-run only never-succeeded partitions
    schedule add <name> <cron>   Register a cron schedule
    schedule list                List schedules
    schedule run-due             Run pipelines whose next trigger is due
    schedule daemon              Long-running loop polling schedules every 30s
    automation status [--json]                  Per-asset automation condition + last eval
    automation simulate <asset> [--json]        Dry-run eval one asset (no logging, no run)
    sensor start                                Start the automation sensor thread
    sensor stop                                 Stop the automation sensor thread
    sensor status                               Show running flag + last tick stats
    sensor set-interval <seconds>               Change the sensor polling interval
    check run <asset> [--json]                  Execute all declared checks for <asset>
    check history <asset> [--limit N] [--json]  Recent asset_check_results rows
    dynamic list [--json]                       List dynamic assets (Snowflake-style)
    dynamic refresh <asset> [--json]            Force-run the defining task immediately
    dynamic create <name> --target-lag <dur> --sql <inline>
                                                Register one dynamic asset from inline SQL
    dynamic create-from-sql <file>              Parse Snowflake-style file, register each block
    dynamic migrate-from-snowflake <file>       Alias of create-from-sql
    ingest preview <path> <target>              Show the tables a JSON source would produce
    ingest run <path> <target> [--disposition append|replace|merge]
                                                [--primary-key <cols>] [--max-nesting N]
    ingest http <url> <target> [--secret <name> | --token <t>]
                                                [--paginate none|page|offset|cursor|link]
                                                [--records-path <p>] [--cursor-path <p>]
                                                [--cursor-param <q>] [--page-param <q>]
                                                [--resource <r>] [--max-pages N]
                                                [--disposition <d>] [--primary-key <cols>]
    ingest schema [<dataset>]                   Schema ledger versions
    ingest changes [<dataset>]                  What each version changed
    ingest loads [--limit N]                    Load history (including failures)
    ingest state                                Resume cursors per source/resource
    ingest reset <source> <resource>            Forget a cursor
    help                     Show this help

GLOBAL FLAGS:
    --db <path>              DuckDB file path (default: ./mydata.duckdb)
    --ext <path>             duckorch.duckdb_extension path (auto-detect if omitted)
    --json                   Emit JSON to stdout (machine-readable)

ENVIRONMENT:
    DUCKDB_BIN               Path to duckdb binary (default: \"duckdb\" in PATH)
    DUCKORCH_EXT             Default --ext value
    DUCKORCH_DB              Default --db value
";

struct Args {
    db: String,
    ext: Option<String>,
    json: bool,
    subcommand: String,
    rest: Vec<String>,
}

fn parse_args() -> Result<Args, String> {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut db = std::env::var("DUCKORCH_DB").unwrap_or_else(|_| "./mydata.duckdb".to_string());
    let mut ext = std::env::var("DUCKORCH_EXT").ok();
    let mut json = false;
    let mut sub: Option<String> = None;
    let mut rest = Vec::new();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--db" => {
                i += 1;
                db = raw.get(i).cloned().ok_or("--db needs a value")?;
            }
            "--ext" => {
                i += 1;
                ext = Some(raw.get(i).cloned().ok_or("--ext needs a value")?);
            }
            "--json" => json = true,
            "-h" | "--help" | "help" if sub.is_none() => sub = Some("help".to_string()),
            other if sub.is_none() => sub = Some(other.to_string()),
            _ => rest.push(raw[i].clone()),
        }
        i += 1;
    }
    Ok(Args {
        db,
        ext,
        json,
        subcommand: sub.unwrap_or_else(|| "help".to_string()),
        rest,
    })
}

fn auto_detect_ext() -> Option<String> {
    let candidates = [
        "./build/release/extension/duckorch/duckorch.duckdb_extension",
        "../build/release/extension/duckorch/duckorch.duckdb_extension",
    ];
    for c in &candidates {
        if std::path::Path::new(c).exists() {
            return Some(c.to_string());
        }
    }
    None
}

fn duckdb_bin() -> String {
    std::env::var("DUCKDB_BIN").unwrap_or_else(|_| "duckdb".to_string())
}

// Run a SQL script through duckdb. Returns (stdout, stderr, exit_code).
fn run_sql(args: &Args, sql: &str, json_mode: bool) -> Result<(String, String, i32), String> {
    let ext_path = args
        .ext
        .clone()
        .or_else(auto_detect_ext)
        .ok_or("could not find duckorch extension; pass --ext or set DUCKORCH_EXT")?;

    let mut prelude = String::new();
    if json_mode {
        prelude.push_str(".mode json\n");
    }
    prelude.push_str(&format!("LOAD '{}';\n", ext_path));
    let full_sql = prelude + sql;

    let mut cmd = Command::new(duckdb_bin());
    cmd.arg(&args.db)
        .arg("-init").arg("/dev/null")
        .arg("-unsigned")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn duckdb: {} (DUCKDB_BIN={})", e, duckdb_bin()))?;

    use std::io::Write;
    child
        .stdin
        .as_mut()
        .ok_or("no stdin")?
        .write_all(full_sql.as_bytes())
        .map_err(|e| format!("write stdin: {}", e))?;

    let out = child.wait_with_output().map_err(|e| format!("{}", e))?;
    Ok((
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
        out.status.code().unwrap_or(-1),
    ))
}

fn sql_escape(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn cmd_register(args: &Args) -> i32 {
    if args.rest.is_empty() {
        eprintln!("register: missing <dir> argument");
        return 2;
    }
    let dir = &args.rest[0];
    let sql = format!("PRAGMA orch_register({});", sql_escape(dir));
    let (out, err, code) = match run_sql(args, &sql, false) {
        Ok(r) => r,
        Err(e) => { eprintln!("{}", e); return 2; }
    };
    if !err.is_empty() { eprintln!("{}", err); }
    if code != 0 { return code; }

    if args.json {
        // Re-run a SELECT to fetch tasks
        let (jout, _, _) = run_sql(args, "SELECT name, inputs, outputs, depends_on FROM __orch__.tasks ORDER BY name;", true).unwrap_or_default();
        print!("{}", jout);
    } else {
        let (sout, _, _) = run_sql(args, "SELECT name, inputs, outputs FROM __orch__.tasks ORDER BY name;", false).unwrap_or_default();
        print!("{}", sout);
    }
    0
}

fn cmd_run(args: &Args) -> i32 {
    // Phase 14: `duck-orch run <asset> --partition <key>` runs a single
    // partition of one Asset (via PRAGMA orch_run_partition). When no
    // --partition flag is given we keep the Phase 5 full-DAG behaviour.
    let mut rest: Vec<String> = args.rest.clone();
    let partition = extract_flag_value(&mut rest, "--partition");
    if let Some(key) = partition {
        if rest.is_empty() {
            eprintln!("run --partition: missing <asset_name>");
            return 2;
        }
        let asset = &rest[0];
        let sql = format!(
            "PRAGMA orch_run_partition({}, {});",
            sql_escape(asset),
            sql_escape(&key)
        );
        let (_, err, code) = match run_sql(args, &sql, false) {
            Ok(r) => r,
            Err(e) => { eprintln!("{}", e); return 2; }
        };
        if !err.is_empty() { eprintln!("{}", err); }
        return code;
    }
    let (_, err, code) = match run_sql(args, "PRAGMA orch_run;", false) {
        Ok(r) => r,
        Err(e) => { eprintln!("{}", e); return 2; }
    };
    if !err.is_empty() { eprintln!("{}", err); }
    if code != 0 { return code; }

    let q = "SELECT task_name, status, retry_count, error_message FROM __orch__.runs WHERE pipeline_run_id = (SELECT pipeline_run_id FROM __orch__.runs ORDER BY started_at DESC LIMIT 1) ORDER BY started_at;";
    if args.json {
        let (out, _, _) = run_sql(args, q, true).unwrap_or_default();
        print!("{}", out);
    } else {
        let (out, _, _) = run_sql(args, q, false).unwrap_or_default();
        print!("{}", out);
    }
    0
}

// Phase 14: `duck-orch backfill <asset> [--from FROM --to TO | --partition K |
// --missing]`. Sequential per-partition execution on the C++ side
// (`PRAGMA orch_backfill*`).
//
// `--parallel N` is recognized but currently delegated to the existing
// `orch_max_parallel` global rather than spinning local threads — full
// per-partition fan-out is punted to a follow-up.
fn cmd_backfill(args: &Args) -> i32 {
    if args.rest.is_empty() {
        eprintln!("backfill: missing <asset> argument");
        return 2;
    }
    let mut rest: Vec<String> = args.rest.clone();
    let from = extract_flag_value(&mut rest, "--from");
    let to = extract_flag_value(&mut rest, "--to");
    let partition = extract_flag_value(&mut rest, "--partition");
    let _parallel = extract_flag_value(&mut rest, "--parallel");
    let missing = rest.iter().any(|s| s == "--missing");
    let rest_clean: Vec<&String> = rest.iter().filter(|s| s.as_str() != "--missing").collect();
    if rest_clean.is_empty() {
        eprintln!("backfill: missing <asset> argument");
        return 2;
    }
    let asset = rest_clean[0];
    let sql = if let Some(k) = partition {
        format!(
            "PRAGMA orch_run_partition({}, {});",
            sql_escape(asset),
            sql_escape(&k)
        )
    } else if missing {
        format!("PRAGMA orch_backfill_missing({});", sql_escape(asset))
    } else {
        let from_v = from.map(|s| format!("'{}'", s.replace('\'', "''"))).unwrap_or("NULL".into());
        let to_v = to.map(|s| format!("'{}'", s.replace('\'', "''"))).unwrap_or("NULL".into());
        format!(
            "PRAGMA orch_backfill({}, {}, {});",
            sql_escape(asset),
            from_v,
            to_v
        )
    };
    let (_, err, code) = match run_sql(args, &sql, false) {
        Ok(r) => r,
        Err(e) => { eprintln!("{}", e); return 2; }
    };
    if !err.is_empty() { eprintln!("{}", err); }
    if code != 0 { return code; }
    // Echo the resulting per-partition status for the asset.
    let report_sql = format!(
        "SELECT partition_key, last_status, last_materialized_at \
         FROM (PRAGMA orch_asset_partitions({})) ORDER BY partition_key;",
        sql_escape(asset)
    );
    let (out, _, _) = run_sql(args, &report_sql, args.json).unwrap_or_default();
    print!("{}", out);
    0
}

fn cmd_status(args: &Args) -> i32 {
    let q = "SELECT task_name, status, started_at, retry_count FROM __orch__.runs ORDER BY started_at DESC LIMIT 50;";
    let (out, err, code) = match run_sql(args, q, args.json) {
        Ok(r) => r,
        Err(e) => { eprintln!("{}", e); return 2; }
    };
    if !err.is_empty() { eprintln!("{}", err); }
    print!("{}", out);
    code
}

fn cmd_graph(args: &Args) -> i32 {
    let mode = args.rest.first().map(|s| s.as_str()).unwrap_or("lineage");
    let sql = format!("PRAGMA orch_visualize('{}');", mode);
    let (out, err, code) = match run_sql(args, &sql, args.json) {
        Ok(r) => r,
        Err(e) => { eprintln!("{}", e); return 2; }
    };
    if !err.is_empty() { eprintln!("{}", err); }
    if args.json {
        print!("{}", out);
    } else {
        // Strip the duckdb table chrome and print just the mermaid string with newlines unescaped.
        for line in out.lines() {
            if line.contains("graph LR") || line.contains("classDef") || line.contains("-->") || line.contains("class ") {
                println!("{}", line.replace("\\n", "\n").trim_matches('│').trim());
            }
        }
    }
    code
}

fn cmd_test(args: &Args) -> i32 {
    let (out, err, code) = match run_sql(args, "PRAGMA orch_test;", false) {
        Ok(r) => r,
        Err(e) => { eprintln!("{}", e); return 2; }
    };
    print!("{}{}", err, out);
    code
}

fn cmd_validate(args: &Args) -> i32 {
    if args.rest.is_empty() {
        eprintln!("validate: missing <file> argument");
        return 2;
    }
    let file = &args.rest[0];
    let content = match std::fs::read_to_string(file) {
        Ok(c) => c,
        Err(e) => { eprintln!("read {}: {}", file, e); return 2; }
    };
    // Use the parser via SQL-roundtrip-free path
    let sql = format!(
        "SELECT orch_parse_task({}, {}) AS task;",
        sql_escape(&content),
        sql_escape(file)
    );
    let (out, _, code) = match run_sql(args, &sql, true) {
        Ok(r) => r,
        Err(e) => { eprintln!("{}", e); return 2; }
    };
    print!("{}", out);
    code
}

fn cmd_impact(args: &Args) -> i32 {
    if args.rest.is_empty() {
        eprintln!("impact: missing <table> argument");
        return 2;
    }
    let table = &args.rest[0];
    let q = format!(
        "WITH RECURSIVE down AS ( \
           SELECT dst_dataset AS table_name, via_task FROM __orch__.lineage_edges WHERE src_dataset = {} \
           UNION ALL \
           SELECT le.dst_dataset, le.via_task FROM __orch__.lineage_edges le JOIN down d ON le.src_dataset = d.table_name \
         ) \
         SELECT DISTINCT table_name, via_task FROM down ORDER BY table_name;",
        sql_escape(table)
    );
    let (out, err, code) = match run_sql(args, &q, args.json) {
        Ok(r) => r,
        Err(e) => { eprintln!("{}", e); return 2; }
    };
    if !err.is_empty() { eprintln!("{}", err); }
    print!("{}", out);
    code
}

fn cmd_lineage(args: &Args) -> i32 {
    if args.rest.is_empty() {
        eprintln!("lineage: missing <table> argument");
        return 2;
    }
    let table = &args.rest[0];
    let q = format!(
        "WITH RECURSIVE up AS ( \
           SELECT src_dataset AS table_name, via_task FROM __orch__.lineage_edges WHERE dst_dataset = {} \
           UNION ALL \
           SELECT le.src_dataset, le.via_task FROM __orch__.lineage_edges le JOIN up u ON le.dst_dataset = u.table_name \
         ) \
         SELECT DISTINCT table_name, via_task FROM up ORDER BY table_name;",
        sql_escape(table)
    );
    let (out, err, code) = match run_sql(args, &q, args.json) {
        Ok(r) => r,
        Err(e) => { eprintln!("{}", e); return 2; }
    };
    if !err.is_empty() { eprintln!("{}", err); }
    print!("{}", out);
    code
}

// =============================================================================
// Phase 13 m2: `duck-orch asset ...` subcommands.
//
// Each leaf calls the corresponding `PRAGMA orch_asset_*` and pipes stdout
// through. `--json` flips duckdb to `.mode json` (handled by run_sql) so the
// caller gets a JSON envelope per row.
// =============================================================================

fn extract_flag_value(rest: &mut Vec<String>, flag: &str) -> Option<String> {
    let mut i = 0;
    while i < rest.len() {
        if rest[i] == flag {
            if i + 1 < rest.len() {
                let v = rest.remove(i + 1);
                rest.remove(i);
                return Some(v);
            }
            // Trailing --flag with no value → drop it and stop.
            rest.remove(i);
            return None;
        }
        i += 1;
    }
    None
}

fn cmd_asset(args: &Args) -> i32 {
    let sub = args.rest.first().map(|s| s.as_str()).unwrap_or("");
    match sub {
        "list" => cmd_asset_list(args),
        "show" => cmd_asset_show(args),
        "lineage" => cmd_asset_lineage(args),
        "materializations" => cmd_asset_materializations(args),
        "health" => cmd_asset_health(args),
        "partitions" => cmd_asset_partitions(args),
        "" => {
            eprintln!("asset: missing subcommand (list|show|lineage|materializations|health|partitions)");
            2
        }
        other => {
            eprintln!("unknown asset subcommand: {}", other);
            2
        }
    }
}

fn cmd_asset_list(args: &Args) -> i32 {
    // Pull optional --group flag out of the rest after the "list" verb.
    let mut tail: Vec<String> = args.rest.iter().skip(1).cloned().collect();
    let group = extract_flag_value(&mut tail, "--group");
    let sql = match group {
        Some(g) => format!("PRAGMA orch_asset_list_group({});", sql_escape(&g)),
        None => "PRAGMA orch_asset_list;".to_string(),
    };
    let (out, err, code) = match run_sql(args, &sql, args.json) {
        Ok(r) => r,
        Err(e) => { eprintln!("{}", e); return 2; }
    };
    if !err.is_empty() { eprintln!("{}", err); }
    print!("{}", out);
    code
}

fn cmd_asset_show(args: &Args) -> i32 {
    if args.rest.len() < 2 {
        eprintln!("asset show: missing <name>");
        return 2;
    }
    let name = &args.rest[1];
    let sql = format!("PRAGMA orch_asset_show({});", sql_escape(name));
    let (out, err, code) = match run_sql(args, &sql, args.json) {
        Ok(r) => r,
        Err(e) => { eprintln!("{}", e); return 2; }
    };
    if !err.is_empty() { eprintln!("{}", err); }
    print!("{}", out);
    code
}

fn cmd_asset_lineage(args: &Args) -> i32 {
    if args.rest.len() < 2 {
        eprintln!("asset lineage: missing <name>");
        return 2;
    }
    let name = &args.rest[1];
    let sql = format!("PRAGMA orch_asset_lineage({});", sql_escape(name));
    let (out, err, code) = match run_sql(args, &sql, args.json) {
        Ok(r) => r,
        Err(e) => { eprintln!("{}", e); return 2; }
    };
    if !err.is_empty() { eprintln!("{}", err); }
    if args.json {
        print!("{}", out);
    } else {
        // Strip the duckdb table chrome and unescape \n inside the mermaid
        // string (mirrors the `graph` subcommand behaviour).
        for line in out.lines() {
            if line.contains("graph LR")
                || line.contains("classDef")
                || line.contains("-->")
                || line.contains("class ")
            {
                println!("{}", line.replace("\\n", "\n").trim_matches('│').trim());
            }
        }
    }
    code
}

fn cmd_asset_materializations(args: &Args) -> i32 {
    if args.rest.len() < 2 {
        eprintln!("asset materializations: missing <name>");
        return 2;
    }
    let name = &args.rest[1];
    let mut tail: Vec<String> = args.rest.iter().skip(2).cloned().collect();
    let limit: i64 = extract_flag_value(&mut tail, "--limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    let sql = format!(
        "PRAGMA orch_asset_materializations({}, {});",
        sql_escape(name),
        limit
    );
    let (out, err, code) = match run_sql(args, &sql, args.json) {
        Ok(r) => r,
        Err(e) => { eprintln!("{}", e); return 2; }
    };
    if !err.is_empty() { eprintln!("{}", err); }
    print!("{}", out);
    code
}

fn cmd_asset_health(args: &Args) -> i32 {
    let sql = "PRAGMA orch_asset_health;";
    let (out, err, code) = match run_sql(args, sql, args.json) {
        Ok(r) => r,
        Err(e) => { eprintln!("{}", e); return 2; }
    };
    if !err.is_empty() { eprintln!("{}", err); }
    print!("{}", out);
    code
}

// =============================================================================
// Phase 14: `duck-orch asset partitions <asset> [--json]`.
//
// Default (text mode): calls `PRAGMA orch_asset_partitions_calendar` to get
// the rendered ASCII calendar (single VARCHAR cell) and strips duckdb's
// table chrome before printing — same trick as `asset lineage`.
//
// --json: emits the raw partition rows via `PRAGMA orch_asset_partitions`.
// =============================================================================
fn cmd_asset_partitions(args: &Args) -> i32 {
    if args.rest.len() < 2 {
        eprintln!("asset partitions: missing <name>");
        return 2;
    }
    let name = &args.rest[1];
    if args.json {
        let sql = format!("PRAGMA orch_asset_partitions({});", sql_escape(name));
        let (out, err, code) = match run_sql(args, &sql, true) {
            Ok(r) => r,
            Err(e) => { eprintln!("{}", e); return 2; }
        };
        if !err.is_empty() { eprintln!("{}", err); }
        print!("{}", out);
        return code;
    }
    let sql = format!(
        "PRAGMA orch_asset_partitions_calendar({});",
        sql_escape(name)
    );
    let (out, err, code) = match run_sql(args, &sql, false) {
        Ok(r) => r,
        Err(e) => { eprintln!("{}", e); return 2; }
    };
    if !err.is_empty() { eprintln!("{}", err); }
    // Strip duckdb table chrome and unescape \n inside the rendered cell.
    // The single column 'calendar' carries a multi-line string; duckdb
    // surrounds it with │ borders and renders newlines as `\n` literals.
    for line in out.lines() {
        if line.starts_with('│') {
            let body = line.trim_matches('│').trim();
            if body.is_empty() || body == "calendar" || body.starts_with("varchar") {
                continue;
            }
            for sub in body.split("\\n") {
                println!("{}", sub);
            }
        }
    }
    code
}

// cron 0.12 wants 6+ fields. If user passes a standard 5-field cron,
// prepend "0 " for seconds.
fn normalize_cron(expr: &str) -> String {
    let n = expr.split_whitespace().count();
    if n == 5 {
        format!("0 {}", expr)
    } else {
        expr.to_string()
    }
}

fn cmd_schedule(args: &Args) -> i32 {
    let sub = args.rest.first().map(|s| s.as_str()).unwrap_or("list");
    match sub {
        "add" => {
            if args.rest.len() < 3 {
                eprintln!("schedule add: usage: schedule add <name> <cron>");
                return 2;
            }
            let name = &args.rest[1];
            let cron = &args.rest[2];
            // Validate cron expression
            use std::str::FromStr;
            if let Err(e) = cron::Schedule::from_str(&normalize_cron(cron)) {
                eprintln!("invalid cron: {}", e);
                return 2;
            }
            let sql = format!(
                "INSERT OR REPLACE INTO __orch__.schedules (pipeline_or_task, cron_expr, enabled, next_trigger_at) \
                 VALUES ({}, {}, true, current_timestamp);",
                sql_escape(name), sql_escape(cron)
            );
            // Need to ensure schedules table exists
            let setup = "CREATE TABLE IF NOT EXISTS __orch__.schedules (\
                pipeline_or_task VARCHAR PRIMARY KEY, cron_expr VARCHAR, timezone VARCHAR DEFAULT 'UTC', \
                enabled BOOLEAN DEFAULT true, last_triggered_at TIMESTAMP, next_trigger_at TIMESTAMP);";
            let (_, err, code) = run_sql(args, &format!("CREATE SCHEMA IF NOT EXISTS __orch__; {} {}", setup, sql), false)
                .unwrap_or_default();
            if !err.is_empty() { eprintln!("{}", err); }
            code
        }
        "list" => {
            let q = "SELECT pipeline_or_task, cron_expr, enabled, next_trigger_at FROM __orch__.schedules ORDER BY pipeline_or_task;";
            let (out, _, code) = run_sql(args, q, args.json).unwrap_or_default();
            print!("{}", out);
            code
        }
        "run-due" => run_schedule_due(args),
        "daemon" => {
            eprintln!("[duck-orch] schedule daemon started (poll = 30s)");
            loop {
                let _ = run_schedule_due(args);
                std::thread::sleep(std::time::Duration::from_secs(30));
            }
        }
        other => { eprintln!("unknown schedule subcommand: {}", other); 2 }
    }
}

fn run_schedule_due(args: &Args) -> i32 {
    use std::str::FromStr;
    use chrono::Utc;
    let q = "SELECT pipeline_or_task, cron_expr, \
             (COALESCE(next_trigger_at, current_timestamp)::TIMESTAMP)::VARCHAR AS next_trigger_at \
             FROM __orch__.schedules WHERE enabled = true;";
    let (_, _, _) = run_sql(args, q, false).unwrap_or_default();
    // Re-query in JSON to parse
    let (jout, _, _) = run_sql(args, q, true).unwrap_or_default();
    let rows: Vec<serde_json::Value> = match serde_json::from_str(&jout) {
        Ok(v) => v,
        Err(_) => return 0,
    };
    let now = Utc::now();
    for r in rows {
        let name = r.get("pipeline_or_task").and_then(|v| v.as_str()).unwrap_or("");
        let cron_expr = r.get("cron_expr").and_then(|v| v.as_str()).unwrap_or("");
        let next_str = r.get("next_trigger_at").and_then(|v| v.as_str()).unwrap_or("");
        // Parse next time. If <= now → run.
        // DuckDB timestamps come back like "2026-05-01 18:31:36.518224" (space, no Z).
        // Try every plausible format; only fall back to `now` if everything fails.
        let next_time = chrono::DateTime::parse_from_rfc3339(next_str)
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(next_str, "%Y-%m-%dT%H:%M:%S%.f")
                .map(|t| t.and_utc().fixed_offset()))
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(next_str, "%Y-%m-%d %H:%M:%S%.f")
                .map(|t| t.and_utc().fixed_offset()))
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(next_str, "%Y-%m-%d %H:%M:%S")
                .map(|t| t.and_utc().fixed_offset()))
            .unwrap_or_else(|_| {
                eprintln!("[duck-orch] WARN: could not parse next_trigger_at='{}', firing now", next_str);
                now.into()
            });
        if next_time > now {
            continue;
        }
        eprintln!("[duck-orch] running schedule {} ({})", name, cron_expr);
        let _ = run_sql(args, "PRAGMA orch_run;", false);
        // Compute next trigger and update
        let sched = match cron::Schedule::from_str(&normalize_cron(cron_expr)) {
            Ok(s) => s,
            Err(e) => { eprintln!("invalid cron for {}: {}", name, e); continue; }
        };
        let next = sched.upcoming(Utc).next();
        if let Some(nt) = next {
            let upd = format!(
                "UPDATE __orch__.schedules SET last_triggered_at = current_timestamp, \
                 next_trigger_at = '{}' WHERE pipeline_or_task = {};",
                nt.format("%Y-%m-%d %H:%M:%S"),
                sql_escape(name)
            );
            let _ = run_sql(args, &upd, false);
        }
    }
    0
}

// =============================================================================
// Phase 15: `duck-orch automation ...` + `duck-orch sensor ...` subcommands.
// =============================================================================

fn cmd_automation(args: &Args) -> i32 {
    let sub = args.rest.first().map(|s| s.as_str()).unwrap_or("");
    match sub {
        "status" => {
            let (out, err, code) =
                match run_sql(args, "PRAGMA orch_automation_status;", args.json) {
                    Ok(r) => r,
                    Err(e) => { eprintln!("{}", e); return 2; }
                };
            if !err.is_empty() { eprintln!("{}", err); }
            print!("{}", out);
            code
        }
        "simulate" => {
            if args.rest.len() < 2 {
                eprintln!("automation simulate: missing <asset>");
                return 2;
            }
            let asset = &args.rest[1];
            let sql = format!("PRAGMA orch_automation_simulate({});", sql_escape(asset));
            let (out, err, code) = match run_sql(args, &sql, args.json) {
                Ok(r) => r,
                Err(e) => { eprintln!("{}", e); return 2; }
            };
            if !err.is_empty() { eprintln!("{}", err); }
            print!("{}", out);
            code
        }
        "" => {
            eprintln!("automation: missing subcommand (status|simulate)");
            2
        }
        other => {
            eprintln!("unknown automation subcommand: {}", other);
            2
        }
    }
}

fn cmd_sensor(args: &Args) -> i32 {
    let sub = args.rest.first().map(|s| s.as_str()).unwrap_or("");
    match sub {
        "start" | "enable" => {
            let (_, err, code) = match run_sql(args, "PRAGMA orch_sensor_start;", false) {
                Ok(r) => r,
                Err(e) => { eprintln!("{}", e); return 2; }
            };
            if !err.is_empty() { eprintln!("{}", err); }
            code
        }
        "stop" | "disable" => {
            let (_, err, code) = match run_sql(args, "PRAGMA orch_sensor_stop;", false) {
                Ok(r) => r,
                Err(e) => { eprintln!("{}", e); return 2; }
            };
            if !err.is_empty() { eprintln!("{}", err); }
            code
        }
        "status" => {
            let (out, err, code) = match run_sql(args, "PRAGMA orch_sensor_status;", args.json) {
                Ok(r) => r,
                Err(e) => { eprintln!("{}", e); return 2; }
            };
            if !err.is_empty() { eprintln!("{}", err); }
            print!("{}", out);
            code
        }
        "set-interval" => {
            if args.rest.len() < 2 {
                eprintln!("sensor set-interval: missing <seconds>");
                return 2;
            }
            let n: i64 = match args.rest[1].parse() {
                Ok(v) => v,
                Err(_) => { eprintln!("sensor set-interval: invalid integer"); return 2; }
            };
            let sql = format!("PRAGMA orch_sensor_set_interval({});", n);
            let (_, err, code) = match run_sql(args, &sql, false) {
                Ok(r) => r,
                Err(e) => { eprintln!("{}", e); return 2; }
            };
            if !err.is_empty() { eprintln!("{}", err); }
            code
        }
        "" => {
            eprintln!("sensor: missing subcommand (start|stop|status|set-interval)");
            2
        }
        other => {
            eprintln!("unknown sensor subcommand: {}", other);
            2
        }
    }
}

// =============================================================================
// Phase 16: `duck-orch check ...` subcommands (run | history).
// =============================================================================

fn cmd_check(args: &Args) -> i32 {
    let sub = args.rest.first().map(|s| s.as_str()).unwrap_or("");
    match sub {
        "run" => {
            if args.rest.len() < 2 {
                eprintln!("check run: missing <asset>");
                return 2;
            }
            let asset = &args.rest[1];
            let sql = format!("PRAGMA orch_check_run({});", sql_escape(asset));
            let (out, err, code) = match run_sql(args, &sql, args.json) {
                Ok(r) => r,
                Err(e) => { eprintln!("{}", e); return 2; }
            };
            if !err.is_empty() { eprintln!("{}", err); }
            print!("{}", out);
            code
        }
        "history" => {
            if args.rest.len() < 2 {
                eprintln!("check history: missing <asset>");
                return 2;
            }
            let asset = args.rest[1].clone();
            let mut tail: Vec<String> = args.rest.iter().skip(2).cloned().collect();
            let limit: i64 = extract_flag_value(&mut tail, "--limit")
                .and_then(|s| s.parse().ok())
                .unwrap_or(50);
            let sql = format!(
                "PRAGMA orch_check_history({}, {});",
                sql_escape(&asset),
                limit
            );
            let (out, err, code) = match run_sql(args, &sql, args.json) {
                Ok(r) => r,
                Err(e) => { eprintln!("{}", e); return 2; }
            };
            if !err.is_empty() { eprintln!("{}", err); }
            print!("{}", out);
            code
        }
        "" => {
            eprintln!("check: missing subcommand (run|history)");
            2
        }
        other => {
            eprintln!("unknown check subcommand: {}", other);
            2
        }
    }
}

// =============================================================================
// Phase 17: `duck-orch dynamic ...` — Snowflake-compatible Dynamic Asset
// surface. All subcommands shell into the corresponding `PRAGMA orch_*`
// implementations; `create-from-sql` / `migrate-from-snowflake` additionally
// pre-parse the source file via `orch_common::snowflake::parse_snowflake_dump`.
// =============================================================================


// ---------------------------------------------------------------------------
// ingest (Phase 19)
// ---------------------------------------------------------------------------

/// Render an optional named pragma argument, e.g. `, disposition='merge'`.
fn named_arg(name: &str, value: &Option<String>) -> String {
    match value {
        Some(v) if !v.is_empty() => format!(", {} = {}", name, sql_escape(v)),
        _ => String::new(),
    }
}

fn emit(args: &Args, sql: &str) -> i32 {
    let (out, err, code) = match run_sql(args, sql, args.json) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{}", e);
            return 2;
        }
    };
    if !err.is_empty() {
        eprintln!("{}", err);
    }
    print!("{}", out);
    code
}

fn cmd_ingest(args: &Args) -> i32 {
    let sub = args.rest.first().map(|s| s.as_str()).unwrap_or("");
    let mut tail: Vec<String> = args.rest.iter().skip(1).cloned().collect();

    match sub {
        "preview" | "run" | "http" => {
            let max_nesting = extract_flag_value(&mut tail, "--max-nesting");
            let disposition = extract_flag_value(&mut tail, "--disposition");
            let primary_key = extract_flag_value(&mut tail, "--primary-key");
            let paginate = extract_flag_value(&mut tail, "--paginate");
            let records_path = extract_flag_value(&mut tail, "--records-path");
            let cursor_path = extract_flag_value(&mut tail, "--cursor-path");
            let cursor_param = extract_flag_value(&mut tail, "--cursor-param");
            let page_param = extract_flag_value(&mut tail, "--page-param");
            let resource = extract_flag_value(&mut tail, "--resource");
            let secret = extract_flag_value(&mut tail, "--secret");
            let token = extract_flag_value(&mut tail, "--token");
            let max_pages = extract_flag_value(&mut tail, "--max-pages");

            if tail.len() < 2 {
                eprintln!("ingest {}: needs <source> <target>", sub);
                return 2;
            }
            let source = &tail[0];
            let target = &tail[1];

            let nesting = match &max_nesting {
                Some(v) if !v.is_empty() => format!(", max_nesting = {}", v),
                _ => String::new(),
            };

            let sql = match sub {
                "preview" => format!(
                    "PRAGMA orch_ingest_preview({}, {}{});",
                    sql_escape(source),
                    sql_escape(target),
                    nesting
                ),
                "run" => format!(
                    "PRAGMA orch_ingest_run({}, {}{}{}{});",
                    sql_escape(source),
                    sql_escape(target),
                    named_arg("disposition", &disposition),
                    named_arg("primary_key", &primary_key),
                    nesting
                ),
                _ => {
                    let pages = match &max_pages {
                        Some(v) if !v.is_empty() => format!(", max_pages = {}", v),
                        _ => String::new(),
                    };
                    format!(
                        "PRAGMA orch_ingest_http({}, {}{}{}{}{}{}{}{}{}{}{}{});",
                        sql_escape(source),
                        sql_escape(target),
                        named_arg("disposition", &disposition),
                        named_arg("primary_key", &primary_key),
                        named_arg("paginate", &paginate),
                        named_arg("records_path", &records_path),
                        named_arg("cursor_path", &cursor_path),
                        named_arg("cursor_param", &cursor_param),
                        named_arg("page_param", &page_param),
                        named_arg("resource", &resource),
                        named_arg("secret", &secret),
                        named_arg("token", &token),
                        format!("{}{}", nesting, pages)
                    )
                }
            };
            emit(args, &sql)
        }
        "schema" => {
            let filter = tail
                .first()
                .map(|d| format!(" WHERE dataset = {}", sql_escape(d)))
                .unwrap_or_default();
            emit(
                args,
                &format!(
                    "SELECT dataset, version, parent_dataset, schema_hash, created_at \
                     FROM __orch__.ingest_schemas{} ORDER BY dataset, version;",
                    filter
                ),
            )
        }
        "changes" => {
            let filter = tail
                .first()
                .map(|d| format!(" WHERE dataset = {}", sql_escape(d)))
                .unwrap_or_default();
            emit(
                args,
                &format!(
                    "SELECT dataset, version, change_kind, column_name, from_type, to_type, \
                     applied_at FROM __orch__.ingest_schema_changes{} ORDER BY applied_at;",
                    filter
                ),
            )
        }
        "loads" => {
            let limit: i64 = extract_flag_value(&mut tail, "--limit")
                .and_then(|s| s.parse().ok())
                .unwrap_or(50);
            emit(
                args,
                &format!(
                    "SELECT load_id, source_kind, source, dataset, write_disposition, status, \
                     tables_written, rows_inserted, rows_deleted, truncated, started_at, \
                     error_message FROM __orch__.ingest_loads ORDER BY started_at DESC LIMIT {};",
                    limit
                ),
            )
        }
        "state" => emit(args, "PRAGMA orch_ingest_state;"),
        "reset" => {
            if tail.len() < 2 {
                eprintln!("ingest reset: needs <source> <resource>");
                return 2;
            }
            emit(
                args,
                &format!(
                    "PRAGMA orch_ingest_reset({}, {});",
                    sql_escape(&tail[0]),
                    sql_escape(&tail[1])
                ),
            )
        }
        "" => {
            eprintln!("ingest: missing subcommand (preview|run|http|schema|changes|loads|state|reset)");
            2
        }
        other => {
            eprintln!("ingest: unknown subcommand '{}'", other);
            2
        }
    }
}

fn cmd_dynamic(args: &Args) -> i32 {
    let sub = args.rest.first().map(|s| s.as_str()).unwrap_or("");
    match sub {
        "list" => {
            let (out, err, code) = match run_sql(args, "PRAGMA orch_dynamic_list;", args.json) {
                Ok(r) => r,
                Err(e) => { eprintln!("{}", e); return 2; }
            };
            if !err.is_empty() { eprintln!("{}", err); }
            print!("{}", out);
            code
        }
        "refresh" => {
            if args.rest.len() < 2 {
                eprintln!("dynamic refresh: missing <asset>");
                return 2;
            }
            let asset = &args.rest[1];
            let sql = format!("PRAGMA orch_dynamic_refresh({});", sql_escape(asset));
            let (out, err, code) = match run_sql(args, &sql, args.json) {
                Ok(r) => r,
                Err(e) => { eprintln!("{}", e); return 2; }
            };
            if !err.is_empty() { eprintln!("{}", err); }
            print!("{}", out);
            code
        }
        "create" => cmd_dynamic_create(args),
        "create-from-sql" | "migrate-from-snowflake" => cmd_dynamic_create_from_sql(args),
        "" => {
            eprintln!(
                "dynamic: missing subcommand (list|refresh|create|create-from-sql|migrate-from-snowflake)"
            );
            2
        }
        other => {
            eprintln!("unknown dynamic subcommand: {}", other);
            2
        }
    }
}

fn cmd_dynamic_create(args: &Args) -> i32 {
    // `duck-orch dynamic create <name> --target-lag <dur> --sql <inline>`
    let mut rest: Vec<String> = args.rest.iter().skip(1).cloned().collect();
    let target_lag = match extract_flag_value(&mut rest, "--target-lag") {
        Some(v) => v,
        None => { eprintln!("dynamic create: missing --target-lag"); return 2; }
    };
    let inline_sql = match extract_flag_value(&mut rest, "--sql") {
        Some(v) => v,
        None => { eprintln!("dynamic create: missing --sql"); return 2; }
    };
    if rest.is_empty() {
        eprintln!("dynamic create: missing <name>");
        return 2;
    }
    let name = &rest[0];
    let sql = format!(
        "PRAGMA orch_create_dynamic_asset({}, {}, {});",
        sql_escape(name),
        sql_escape(&target_lag),
        sql_escape(&inline_sql)
    );
    let (out, err, code) = match run_sql(args, &sql, args.json) {
        Ok(r) => r,
        Err(e) => { eprintln!("{}", e); return 2; }
    };
    if !err.is_empty() { eprintln!("{}", err); }
    print!("{}", out);
    code
}

fn cmd_dynamic_create_from_sql(args: &Args) -> i32 {
    if args.rest.len() < 2 {
        eprintln!("dynamic create-from-sql: missing <file>");
        return 2;
    }
    let path = &args.rest[1];
    let src = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("dynamic create-from-sql: cannot read {}: {}", path, e);
            return 2;
        }
    };
    let blocks = orch_common::parse_snowflake_dump(&src);
    if blocks.is_empty() {
        if args.json {
            println!("{{\"created\":0,\"skipped\":0,\"errors\":[],\"results\":[]}}");
        } else {
            println!("no CREATE DYNAMIC TABLE/ASSET blocks found in {}", path);
        }
        return 0;
    }

    let mut created = 0usize;
    let mut skipped = 0usize;
    let mut errors: Vec<String> = Vec::new();
    let mut results: Vec<serde_json::Value> = Vec::new();
    for blk in &blocks {
        // Default target_lag when omitted: 5 minutes (Snowflake-ish default).
        let lag = blk.target_lag.clone().unwrap_or_else(|| "5min".to_string());
        if blk.sql_body.is_empty() {
            skipped += 1;
            continue;
        }
        let sql = format!(
            "PRAGMA orch_create_dynamic_asset({}, {}, {});",
            sql_escape(&blk.name),
            sql_escape(&lag),
            sql_escape(&blk.sql_body)
        );
        match run_sql(args, &sql, false) {
            Ok((stdout, stderr, code)) => {
                if code == 0 {
                    created += 1;
                    if args.json {
                        results.push(serde_json::json!({
                            "name": blk.name,
                            "target_lag": lag,
                            "status": "created",
                        }));
                    } else {
                        println!("created  {}  target_lag={}", blk.name, lag);
                    }
                    if !stderr.is_empty() {
                        eprintln!("{}", stderr);
                    }
                    let _ = stdout;
                } else {
                    skipped += 1;
                    let msg = format!("{} failed: {}", blk.name, stderr.trim());
                    errors.push(msg.clone());
                    if !args.json {
                        eprintln!("{}", msg);
                    }
                }
            }
            Err(e) => {
                skipped += 1;
                errors.push(format!("{} spawn-error: {}", blk.name, e));
            }
        }
    }
    if args.json {
        let body = serde_json::json!({
            "created": created,
            "skipped": skipped,
            "errors": errors,
            "results": results,
        });
        println!("{}", body);
    } else {
        println!("done: {} created, {} skipped, {} errors", created, skipped, errors.len());
    }
    if errors.is_empty() { 0 } else { 1 }
}

fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => { eprintln!("{}", e); std::process::exit(2); }
    };
    let code = match args.subcommand.as_str() {
        "register" => cmd_register(&args),
        "run" => cmd_run(&args),
        "status" => cmd_status(&args),
        "graph" => cmd_graph(&args),
        "test" => cmd_test(&args),
        "validate" => cmd_validate(&args),
        "impact" => cmd_impact(&args),
        "lineage" => cmd_lineage(&args),
        "asset" => cmd_asset(&args),
        "backfill" => cmd_backfill(&args),
        "schedule" => cmd_schedule(&args),
        "automation" => cmd_automation(&args),
        "sensor" => cmd_sensor(&args),
        "check" => cmd_check(&args),
        "dynamic" => cmd_dynamic(&args),
        "ingest" => cmd_ingest(&args),
        "help" | "" => { print!("{}", HELP); 0 }
        other => { eprintln!("unknown subcommand: {}", other); print!("{}", HELP); 2 }
    };
    std::process::exit(code);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_flag_value_basic() {
        let mut v = vec!["a".to_string(), "--group".to_string(), "sales".to_string()];
        assert_eq!(extract_flag_value(&mut v, "--group"), Some("sales".into()));
        assert_eq!(v, vec!["a"]);
    }

    #[test]
    fn extract_flag_value_missing() {
        let mut v = vec!["a".to_string(), "b".to_string()];
        assert_eq!(extract_flag_value(&mut v, "--group"), None);
        assert_eq!(v, vec!["a", "b"]);
    }

    #[test]
    fn extract_flag_value_trailing() {
        // `--group` with no following arg → dropped, returns None.
        let mut v = vec!["a".to_string(), "--group".to_string()];
        assert_eq!(extract_flag_value(&mut v, "--group"), None);
        assert_eq!(v, vec!["a"]);
    }

    #[test]
    fn extract_flag_value_preserves_order() {
        let mut v = vec![
            "a".to_string(),
            "--limit".to_string(),
            "20".to_string(),
            "b".to_string(),
        ];
        assert_eq!(extract_flag_value(&mut v, "--limit"), Some("20".into()));
        assert_eq!(v, vec!["a", "b"]);
    }

    #[test]
    fn sql_escape_quotes() {
        assert_eq!(sql_escape("o'brien"), "'o''brien'");
        assert_eq!(sql_escape("plain"), "'plain'");
    }

    // Phase 17: regression test for the snowflake re-export through the CLI.
    // The CLI calls `orch_common::parse_snowflake_dump` in `cmd_dynamic_create_from_sql`
    // — keep the dependency wired by exercising the symbol here too.
    #[test]
    fn dynamic_create_from_sql_reexport_works() {
        let src = "CREATE DYNAMIC TABLE a TARGET_LAG = '5m' AS SELECT 1;";
        let blocks = orch_common::parse_snowflake_dump(src);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].name, "a");
    }
}
