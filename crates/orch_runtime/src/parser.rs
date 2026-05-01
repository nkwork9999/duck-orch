// Parses SQL files with `-- @key value` header comments into Task structs.

use orch_common::{Task, TaskTest};
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
