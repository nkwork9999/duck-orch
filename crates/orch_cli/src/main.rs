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
        "help" | "" => { print!("{}", HELP); 0 }
        other => { eprintln!("unknown subcommand: {}", other); print!("{}", HELP); 2 }
    };
    std::process::exit(code);
}
