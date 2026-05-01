// Render a DagResult as a Mermaid graph definition.

use crate::DagResult;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Serialize, Deserialize)]
pub enum Mode {
    Lineage,
    Dag,
    Combined,
}

pub fn render(dag: &DagResult, mode: Mode, statuses: &[(String, String)]) -> String {
    let mut s = String::from("graph LR\n");
    match mode {
        Mode::Lineage => render_lineage(dag, &mut s),
        Mode::Dag => render_dag(dag, &mut s),
        Mode::Combined => {
            render_lineage(dag, &mut s);
            render_dag(dag, &mut s);
        }
    }
    if !statuses.is_empty() {
        render_status_classes(statuses, &mut s);
    }
    s
}

fn render_lineage(dag: &DagResult, out: &mut String) {
    let mut seen = BTreeSet::new();
    for e in &dag.lineage_edges {
        let from = sanitize(&e.src_dataset);
        let to = sanitize(&e.dst_dataset);
        if seen.insert((from.clone(), to.clone())) {
            out.push_str(&format!(
                "    {}[({})] --> {}[({})]\n",
                from, e.src_dataset, to, e.dst_dataset
            ));
        }
    }
}

fn render_dag(dag: &DagResult, out: &mut String) {
    for e in &dag.task_edges {
        out.push_str(&format!(
            "    {}_task --> {}_task\n",
            sanitize(&e.from),
            sanitize(&e.to)
        ));
    }
}

fn render_status_classes(statuses: &[(String, String)], out: &mut String) {
    out.push_str("    classDef success fill:#cfc,stroke:#393\n");
    out.push_str("    classDef failed  fill:#fcc,stroke:#933\n");
    out.push_str("    classDef running fill:#ffc,stroke:#993\n");
    out.push_str("    classDef skipped fill:#eee,stroke:#999\n");
    for (name, status) in statuses {
        let cls = match status.as_str() {
            "success" => "success",
            "failed" | "test_failed" => "failed",
            "running" => "running",
            "skipped" => "skipped",
            _ => continue,
        };
        out.push_str(&format!("    class {} {}\n", sanitize(name), cls));
    }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}
