// Parses SQL files with `-- @key value` header comments into Task structs.

use crate::binding::parse_param_decl;
use orch_common::{parse_automation, parse_partition_decl, parse_target_lag, Task, TaskTest};
use std::path::Path;

#[derive(Debug)]
pub struct ParseError {
    pub message: String,
    pub line: Option<usize>,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.line {
            Some(l) => write!(f, "line {}: {}", l, self.message),
            None => write!(f, "{}", self.message),
        }
    }
}

impl std::error::Error for ParseError {}

pub fn parse_sql_file(content: &str, file_path: Option<&str>) -> Result<Task, ParseError> {
    let mut task = Task::default();
    task.file_path = file_path.map(|s| s.to_string());

    let mut sql_lines: Vec<&str> = Vec::new();
    let mut in_header = true;

    for (i, line) in content.lines().enumerate() {
        let trimmed = line.trim();

        if in_header {
            if trimmed.is_empty() {
                continue;
            }
            if let Some(rest) = parse_header_line(trimmed) {
                apply_header(&mut task, rest, i + 1)?;
                continue;
            }
            in_header = false;
        }
        sql_lines.push(line);
    }

    task.sql = sql_lines.join("\n").trim().to_string();

    if task.name.is_empty() {
        if let Some(p) = file_path {
            if let Some(stem) = Path::new(p).file_stem().and_then(|s| s.to_str()) {
                task.name = stem.to_string();
            }
        }
    }

    if task.name.is_empty() {
        return Err(ParseError {
            message: "task has no name (use `-- @task name=...` or a named .sql file)".into(),
            line: None,
        });
    }

    if task.outputs.is_empty() {
        let (inputs_auto, outputs_auto) = orch_lineage::extract_io(&task.sql);
        if task.inputs.is_empty() {
            task.inputs = inputs_auto;
        }
        if task.outputs.is_empty() {
            task.outputs = outputs_auto;
        }
    } else if task.inputs.is_empty() {
        let (inputs_auto, _) = orch_lineage::extract_io(&task.sql);
        task.inputs = inputs_auto;
    }

    Ok(task)
}

fn parse_header_line(line: &str) -> Option<&str> {
    let s = line.strip_prefix("--")?.trim_start();
    if s.starts_with('@') {
        Some(&s[1..])
    } else {
        None
    }
}

fn apply_header(task: &mut Task, content: &str, line: usize) -> Result<(), ParseError> {
    let (key, rest) = split_first_word(content);

    match key {
        "task" => {
            for kv in parse_inline_kv(rest) {
                if kv.0 == "name" {
                    task.name = kv.1.to_string();
                }
            }
        }
        "name" => task.name = rest.trim().to_string(),
        "description" => task.description = Some(rest.trim().to_string()),
        "owner" => task.owner = Some(rest.trim().to_string()),
        "inputs" => task.inputs = split_csv(rest),
        "outputs" => task.outputs = split_csv(rest),
        "depends_on" => task.depends_on = split_csv(rest),
        "schedule" => task.schedule = Some(rest.trim().trim_matches('"').to_string()),
        "retries" => {
            task.retries = rest.trim().parse().map_err(|_| ParseError {
                message: format!("invalid retries: {}", rest),
                line: Some(line),
            })?;
        }
        "timeout" => {
            task.timeout_seconds = Some(rest.trim().parse().map_err(|_| ParseError {
                message: format!("invalid timeout: {}", rest),
                line: Some(line),
            })?);
        }
        "incremental_by" => task.incremental_by = Some(rest.trim().to_string()),
        "tags" => task.tags = split_csv(rest),
        "test" => task.tests.push(parse_test(rest, line)?),
        "param" => {
            let spec = parse_param_decl(rest).map_err(|e| ParseError {
                message: e.to_string(),
                line: Some(line),
            })?;
            task.params.push(spec);
        }
        // Phase 13: Asset 一級化 headers. `@asset` mirrors `@task` and parses
        // `name=value`; the rest are scalar/CSV style like other headers.
        "asset" => {
            let mut got = false;
            for (k, v) in parse_inline_kv(rest) {
                if k == "name" {
                    task.asset_name = Some(v.to_string());
                    got = true;
                }
            }
            if !got {
                let bare = rest.trim().trim_matches('"');
                if !bare.is_empty() {
                    task.asset_name = Some(bare.to_string());
                }
            }
        }
        "asset_kind" => task.asset_kind = Some(rest.trim().to_string()),
        "asset_group" => task.asset_group = Some(rest.trim().to_string()),
        "asset_owner" => task.asset_owner = Some(rest.trim().to_string()),
        "asset_description" => task.asset_description = Some(rest.trim().to_string()),
        "asset_tags" => task.asset_tags = split_csv(rest),
        // Phase 14: partition declaration. Stored on the task; expanded into
        // concrete partition keys at registration time on the C++ side, and
        // surfaced via `$partition_key` bindings at execution time.
        "partitions_by" => {
            let def = parse_partition_decl(rest).map_err(|e| ParseError {
                message: e.to_string(),
                line: Some(line),
            })?;
            task.partitions = Some(def);
        }
        // Phase 15: AutomationCondition / TARGET_LAG headers.
        //
        // `@automation <expr>` — Dagster-style condition AST. The parsed AST
        // is stored on `task.automation`; we *also* pre-compute the canonical
        // DSL string (`task.automation_dsl`) so the C++ asset upsert path can
        // write `automation_condition` straight to the DB without a separate
        // FFI round-trip just to serialize the enum.
        "automation" => {
            let cond = parse_automation(rest).map_err(|e| ParseError {
                message: e.to_string(),
                line: Some(line),
            })?;
            task.automation_dsl = Some(cond.serialize_dsl());
            task.automation = Some(cond);
        }
        // `@target_lag <duration>` — Snowflake Dynamic Tables-style throttle.
        // Internally `@target_lag 5min` is equivalent to
        // `@automation eager() throttle 5min`; the throttle column is stored
        // separately so the evaluator can wrap *any* condition tree with it.
        // If no explicit `@automation` is set, we synthesize a default
        // `eager()` condition so the asset is sensor-eligible out of the box.
        "target_lag" => {
            let secs = parse_target_lag(rest).map_err(|e| ParseError {
                message: e.to_string(),
                line: Some(line),
            })?;
            task.target_lag_seconds = Some(secs);
            if task.automation.is_none() {
                let cond = orch_common::AutomationCondition::Eager;
                task.automation_dsl = Some(cond.serialize_dsl());
                task.automation = Some(cond);
            }
        }
        _ => {}
    }

    Ok(())
}

fn split_first_word(s: &str) -> (&str, &str) {
    let s = s.trim_start();
    match s.find(|c: char| c.is_whitespace() || c == '=') {
        Some(i) => {
            let key = &s[..i];
            let rest = s[i..].trim_start_matches(|c: char| c.is_whitespace() || c == '=');
            (key, rest)
        }
        None => (s, ""),
    }
}

fn parse_inline_kv(s: &str) -> Vec<(&str, &str)> {
    let mut out = Vec::new();
    for tok in s.split_whitespace() {
        if let Some((k, v)) = tok.split_once('=') {
            out.push((k.trim(), v.trim().trim_matches('"')));
        }
    }
    out
}

fn split_csv(s: &str) -> Vec<String> {
    s.split(',')
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

fn parse_test(rest: &str, line: usize) -> Result<TaskTest, ParseError> {
    let s = rest.trim_start();
    if !s.starts_with('"') {
        return Err(ParseError {
            message: format!("@test expects quoted SQL, got: {}", rest),
            line: Some(line),
        });
    }
    let after_open = &s[1..];
    let close = after_open.find('"').ok_or_else(|| ParseError {
        message: "unterminated @test SQL string".into(),
        line: Some(line),
    })?;
    let query = after_open[..close].to_string();
    let assertion = after_open[close + 1..].trim().to_string();
    Ok(TaskTest { query, assertion })
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch_common::ParamType;

    #[test]
    fn parses_param_header() {
        let sql = "-- @name my_task\n-- @param partition_key:DATE\n-- @param count:INT\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        assert_eq!(task.name, "my_task");
        assert_eq!(task.params.len(), 2);
        assert_eq!(task.params[0].name, "partition_key");
        assert_eq!(task.params[0].ty, ParamType::Date);
        assert_eq!(task.params[1].name, "count");
        assert_eq!(task.params[1].ty, ParamType::Integer);
    }

    #[test]
    fn rejects_bad_param_header() {
        let sql = "-- @name bad\n-- @param oops_no_colon\nSELECT 1;\n";
        let err = parse_sql_file(sql, None).expect_err("should fail");
        assert!(err.message.contains("@param"), "got: {}", err.message);
        assert_eq!(err.line, Some(2));
    }

    #[test]
    fn task_without_param_header_has_empty_params() {
        let sql = "-- @name plain\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        assert!(task.params.is_empty());
    }

    // ------------------------------------------------------------------
    // Phase 13: Asset header parsing
    // ------------------------------------------------------------------

    #[test]
    fn parses_asset_name_kv() {
        let sql = "-- @name t\n-- @asset name=analytics.user_stats\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        assert_eq!(task.asset_name.as_deref(), Some("analytics.user_stats"));
    }

    #[test]
    fn parses_asset_name_bare() {
        // Tolerate `-- @asset analytics.user_stats` without name= prefix
        let sql = "-- @name t\n-- @asset analytics.user_stats\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        assert_eq!(task.asset_name.as_deref(), Some("analytics.user_stats"));
    }

    #[test]
    fn parses_asset_kind() {
        let sql = "-- @name t\n-- @asset_kind view\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        assert_eq!(task.asset_kind.as_deref(), Some("view"));
    }

    #[test]
    fn parses_asset_group() {
        let sql = "-- @name t\n-- @asset_group sales\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        assert_eq!(task.asset_group.as_deref(), Some("sales"));
    }

    #[test]
    fn parses_asset_owner() {
        let sql = "-- @name t\n-- @asset_owner data@example.com\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        assert_eq!(task.asset_owner.as_deref(), Some("data@example.com"));
    }

    #[test]
    fn parses_asset_description() {
        let sql = "-- @name t\n-- @asset_description Active users by country\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        assert_eq!(
            task.asset_description.as_deref(),
            Some("Active users by country")
        );
    }

    #[test]
    fn parses_asset_tags_csv() {
        let sql = "-- @name t\n-- @asset_tags daily, sales, kpi\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        assert_eq!(task.asset_tags, vec!["daily", "sales", "kpi"]);
    }

    #[test]
    fn task_without_asset_headers_is_empty() {
        let sql = "-- @name t\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        assert!(task.asset_name.is_none());
        assert!(task.asset_kind.is_none());
        assert!(task.asset_group.is_none());
        assert!(task.asset_owner.is_none());
        assert!(task.asset_description.is_none());
        assert!(task.asset_tags.is_empty());
    }

    // ------------------------------------------------------------------
    // Phase 14: @partitions_by header parsing
    // ------------------------------------------------------------------

    #[test]
    fn parses_partitions_by_daily() {
        let sql = "-- @name t\n-- @partitions_by daily(start=2026-01-01)\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        match task.partitions {
            Some(orch_common::PartitionDef::Daily { ref start, end }) => {
                assert_eq!(start.to_string(), "2026-01-01");
                assert!(end.is_none());
            }
            other => panic!("expected Daily, got {:?}", other),
        }
    }

    #[test]
    fn parses_partitions_by_static() {
        let sql = "-- @name t\n-- @partitions_by static(jp,us,eu)\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        match task.partitions {
            Some(orch_common::PartitionDef::Static(ref v)) => {
                assert_eq!(v, &vec!["jp".to_string(), "us".to_string(), "eu".to_string()]);
            }
            other => panic!("expected Static, got {:?}", other),
        }
    }

    #[test]
    fn parses_partitions_by_multi() {
        let sql = "-- @name t\n-- @partitions_by multi(date=daily(start=2026-01-01),region=static(jp,us))\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        match task.partitions {
            Some(orch_common::PartitionDef::Multi(ref dims)) => {
                assert!(dims.contains_key("date"));
                assert!(dims.contains_key("region"));
            }
            other => panic!("expected Multi, got {:?}", other),
        }
    }

    #[test]
    fn rejects_bad_partitions_by() {
        let sql = "-- @name t\n-- @partitions_by hourly(start=2026-01-01)\nSELECT 1;\n";
        let err = parse_sql_file(sql, None).expect_err("should fail");
        assert!(err.message.contains("@partitions_by"), "got: {}", err.message);
        assert_eq!(err.line, Some(2));
    }

    #[test]
    fn task_without_partitions_by_has_none() {
        let sql = "-- @name t\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        assert!(task.partitions.is_none());
    }

    // ------------------------------------------------------------------
    // Phase 15: @automation / @target_lag header parsing
    // ------------------------------------------------------------------

    #[test]
    fn parses_automation_eager_shortcut() {
        let sql = "-- @name t\n-- @automation eager\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        assert_eq!(
            task.automation,
            Some(orch_common::AutomationCondition::Eager)
        );
        assert_eq!(task.automation_dsl.as_deref(), Some("eager()"));
    }

    #[test]
    fn parses_automation_combined() {
        let sql = "-- @name t\n-- @automation eager AND NOT in_progress()\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        assert!(matches!(
            task.automation,
            Some(orch_common::AutomationCondition::And(_, _))
        ));
        assert_eq!(
            task.automation_dsl.as_deref(),
            Some("eager() AND NOT in_progress()")
        );
    }

    #[test]
    fn rejects_bad_automation() {
        let sql = "-- @name t\n-- @automation magic_atom()\nSELECT 1;\n";
        let err = parse_sql_file(sql, None).expect_err("should fail");
        assert!(err.message.contains("@automation"), "got: {}", err.message);
        assert_eq!(err.line, Some(2));
    }

    #[test]
    fn parses_target_lag_minutes() {
        let sql = "-- @name t\n-- @target_lag 5min\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        assert_eq!(task.target_lag_seconds, Some(300));
        // target_lag without explicit @automation auto-synthesizes eager().
        assert_eq!(
            task.automation,
            Some(orch_common::AutomationCondition::Eager)
        );
        assert_eq!(task.automation_dsl.as_deref(), Some("eager()"));
    }

    #[test]
    fn target_lag_does_not_clobber_explicit_automation() {
        let sql = "-- @name t\n-- @automation on_missing()\n-- @target_lag 30s\nSELECT 1;\n";
        let task = parse_sql_file(sql, None).expect("parse ok");
        assert_eq!(
            task.automation,
            Some(orch_common::AutomationCondition::OnMissing)
        );
        assert_eq!(task.target_lag_seconds, Some(30));
    }

    #[test]
    fn rejects_bad_target_lag() {
        let sql = "-- @name t\n-- @target_lag five-minutes\nSELECT 1;\n";
        let err = parse_sql_file(sql, None).expect_err("should fail");
        assert!(err.message.contains("@target_lag"), "got: {}", err.message);
        assert_eq!(err.line, Some(2));
    }

    #[test]
    fn parses_full_asset_header_bundle() {
        let sql = "\
-- @name user_stats
-- @asset name=analytics.user_stats
-- @asset_kind table
-- @asset_group sales
-- @asset_owner data-team@example.com
-- @asset_description Active users by country
-- @asset_tags daily,kpi
SELECT 1;
";
        let task = parse_sql_file(sql, None).expect("parse ok");
        assert_eq!(task.name, "user_stats");
        assert_eq!(task.asset_name.as_deref(), Some("analytics.user_stats"));
        assert_eq!(task.asset_kind.as_deref(), Some("table"));
        assert_eq!(task.asset_group.as_deref(), Some("sales"));
        assert_eq!(task.asset_owner.as_deref(), Some("data-team@example.com"));
        assert_eq!(
            task.asset_description.as_deref(),
            Some("Active users by country")
        );
        assert_eq!(task.asset_tags, vec!["daily", "kpi"]);
    }
}
