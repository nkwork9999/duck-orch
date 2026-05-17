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

// ---------------------------------------------------------------------------
// Phase 13: Asset-level Mermaid renderer.
//
// Given the focal asset name plus the raw asset_edges rows (already filtered
// to upstream + downstream of `focal` by the caller), produce a Mermaid
// `graph LR` rendering. No transitive closure here — caller decides what to
// pass in. The focal node is highlighted via a `focal` classDef.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetEdge {
    pub upstream_asset: String,
    pub downstream_asset: String,
    #[serde(default)]
    pub via_task: String,
    #[serde(default)]
    pub edge_type: String,
}

pub fn render_asset_lineage(focal: &str, edges: &[AssetEdge]) -> String {
    let mut s = String::from("graph LR\n");
    let mut seen = BTreeSet::new();
    let mut nodes = BTreeSet::new();
    nodes.insert(focal.to_string());
    for e in edges {
        let from = sanitize(&e.upstream_asset);
        let to = sanitize(&e.downstream_asset);
        nodes.insert(e.upstream_asset.clone());
        nodes.insert(e.downstream_asset.clone());
        if seen.insert((from.clone(), to.clone())) {
            let label = if e.via_task.is_empty() {
                String::new()
            } else {
                format!("|{}|", e.via_task)
            };
            s.push_str(&format!(
                "    {}[({})] -->{} {}[({})]\n",
                from, e.upstream_asset, label, to, e.downstream_asset
            ));
        }
    }
    // Make sure isolated focal still shows up.
    if edges.is_empty() {
        s.push_str(&format!("    {}[({})]\n", sanitize(focal), focal));
    }
    s.push_str("    classDef focal fill:#ffd,stroke:#990,stroke-width:2px\n");
    s.push_str(&format!("    class {} focal\n", sanitize(focal)));
    s
}

#[cfg(test)]
mod asset_mermaid_tests {
    use super::*;

    fn e(up: &str, down: &str, via: &str) -> AssetEdge {
        AssetEdge {
            upstream_asset: up.into(),
            downstream_asset: down.into(),
            via_task: via.into(),
            edge_type: "direct".into(),
        }
    }

    #[test]
    fn isolated_focal_renders() {
        let s = render_asset_lineage("lonely", &[]);
        assert!(s.contains("lonely[(lonely)]"));
        assert!(s.contains("class lonely focal"));
    }

    #[test]
    fn upstream_and_downstream() {
        let edges = vec![e("raw", "focal", "build_focal"), e("focal", "mart", "build_mart")];
        let s = render_asset_lineage("focal", &edges);
        assert!(s.contains("raw[(raw)] -->|build_focal| focal"));
        assert!(s.contains("focal[(focal)] -->|build_mart| mart"));
        assert!(s.contains("class focal focal"));
    }

    #[test]
    fn dedupes_repeated_edges() {
        let edges = vec![e("a", "b", "t1"), e("a", "b", "t1")];
        let s = render_asset_lineage("a", &edges);
        let count = s.matches("a[(a)] -->|t1| b[(b)]").count();
        assert_eq!(count, 1);
    }

    #[test]
    fn no_via_label_when_empty() {
        let edges = vec![e("a", "b", "")];
        let s = render_asset_lineage("a", &edges);
        assert!(s.contains("a[(a)] --> b[(b)]"));
    }
}
