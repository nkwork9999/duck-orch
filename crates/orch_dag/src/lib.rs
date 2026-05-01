// DAG construction, topological ordering, downstream propagation, Mermaid rendering.

pub mod mermaid;

use orch_common::Task;
use petgraph::algo::{is_cyclic_directed, toposort};
use petgraph::graph::{DiGraph, NodeIndex};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct DagBuildError {
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskEdge {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LineageEdge {
    pub src_dataset: String,
    pub dst_dataset: String,
    pub via_task: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DagResult {
    pub order: Vec<String>,
    pub task_edges: Vec<TaskEdge>,
    pub lineage_edges: Vec<LineageEdge>,
}

pub fn build_dag(tasks: &[Task]) -> Result<DagResult, DagBuildError> {
    let mut graph: DiGraph<String, ()> = DiGraph::new();
    let mut idx: HashMap<String, NodeIndex> = HashMap::new();

    for t in tasks {
        let n = graph.add_node(t.name.clone());
        idx.insert(t.name.clone(), n);
    }

    let mut producer: HashMap<String, String> = HashMap::new();
    for t in tasks {
        for o in &t.outputs {
            producer.insert(o.clone(), t.name.clone());
        }
    }

    let mut task_edges = Vec::new();
    let mut lineage_edges = Vec::new();

    for t in tasks {
        for dep in &t.depends_on {
            if let (Some(&from), Some(&to)) = (idx.get(dep), idx.get(&t.name)) {
                graph.update_edge(from, to, ());
                task_edges.push(TaskEdge {
                    from: dep.clone(),
                    to: t.name.clone(),
                });
            }
        }
        for inp in &t.inputs {
            if let Some(prod) = producer.get(inp) {
                if prod != &t.name {
                    if let (Some(&from), Some(&to)) = (idx.get(prod), idx.get(&t.name)) {
                        graph.update_edge(from, to, ());
                        task_edges.push(TaskEdge {
                            from: prod.clone(),
                            to: t.name.clone(),
                        });
                    }
                }
            }
            for out in &t.outputs {
                lineage_edges.push(LineageEdge {
                    src_dataset: inp.clone(),
                    dst_dataset: out.clone(),
                    via_task: t.name.clone(),
                });
            }
        }
    }

    if is_cyclic_directed(&graph) {
        return Err(DagBuildError {
            message: "DAG contains a cycle".into(),
        });
    }

    let topo = toposort(&graph, None).map_err(|e| DagBuildError {
        message: format!("toposort failed at node {:?}", e.node_id()),
    })?;
    let order: Vec<String> = topo.into_iter().map(|n| graph[n].clone()).collect();

    task_edges.sort_by(|a, b| (a.from.clone(), a.to.clone()).cmp(&(b.from.clone(), b.to.clone())));
    task_edges.dedup_by(|a, b| a.from == b.from && a.to == b.to);

    Ok(DagResult {
        order,
        task_edges,
        lineage_edges,
    })
}

/// Group tasks into "layers" of mutually independent tasks.
pub fn topo_layers(tasks: &[Task]) -> Result<Vec<Vec<String>>, DagBuildError> {
    let dag = build_dag(tasks)?;
    let mut indeg: HashMap<String, usize> = HashMap::new();
    for t in tasks {
        indeg.insert(t.name.clone(), 0);
    }
    for e in &dag.task_edges {
        *indeg.entry(e.to.clone()).or_insert(0) += 1;
    }
    let mut adj: HashMap<String, Vec<String>> = HashMap::new();
    for e in &dag.task_edges {
        adj.entry(e.from.clone()).or_default().push(e.to.clone());
    }

    let mut layers: Vec<Vec<String>> = Vec::new();
    let mut current: Vec<String> = indeg
        .iter()
        .filter(|(_, &d)| d == 0)
        .map(|(k, _)| k.clone())
        .collect();
    current.sort();

    while !current.is_empty() {
        let mut next: Vec<String> = Vec::new();
        for n in &current {
            if let Some(children) = adj.get(n) {
                for c in children {
                    if let Some(d) = indeg.get_mut(c) {
                        *d -= 1;
                        if *d == 0 {
                            next.push(c.clone());
                        }
                    }
                }
            }
        }
        layers.push(current);
        next.sort();
        current = next;
    }
    Ok(layers)
}

pub fn downstream_of(tasks: &[Task], failed: &str) -> Vec<String> {
    let dag = match build_dag(tasks) {
        Ok(d) => d,
        Err(_) => return Vec::new(),
    };
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for e in &dag.task_edges {
        adj.entry(&e.from).or_default().push(&e.to);
    }
    let mut visited = std::collections::BTreeSet::new();
    let mut stack = vec![failed];
    while let Some(n) = stack.pop() {
        if let Some(children) = adj.get(n) {
            for c in children {
                if visited.insert((*c).to_string()) {
                    stack.push(*c);
                }
            }
        }
    }
    visited.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(name: &str, ins: &[&str], outs: &[&str]) -> Task {
        Task {
            name: name.to_string(),
            inputs: ins.iter().map(|s| s.to_string()).collect(),
            outputs: outs.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn linear_chain() {
        let tasks = vec![t("a", &[], &["X"]), t("b", &["X"], &["Y"]), t("c", &["Y"], &["Z"])];
        let r = build_dag(&tasks).unwrap();
        assert_eq!(r.order, vec!["a", "b", "c"]);
    }

    #[test]
    fn cycle_detected() {
        let tasks = vec![t("a", &["Z"], &["X"]), t("b", &["X"], &["Z"])];
        assert!(build_dag(&tasks).is_err());
    }
}
