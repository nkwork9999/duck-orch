// Write dispositions (P1).
//
// All three dispositions share one shape: the load always appends first, then
// prunes what the disposition says should no longer be there. Pruning after
// the fact rather than deleting first means the incoming rows are already in
// the table when the comparison runs, so nothing has to re-read the source or
// stage a copy.
//
//   append   nothing to prune
//   replace  drop every row that did not come from this load
//   merge    drop rows from earlier loads whose primary key arrived again
//
// Child rows are found by prefix: a root id never contains `/`, and every
// descendant id is `<root id>/<index>/...`, so `split_part(_orch_id, '/', 1)`
// is the root a row belongs to at any depth.

use serde::{Deserialize, Serialize};

use crate::shape::{ID_COL, LOAD_COL};
use crate::typ::{quote_ident, sql_lit};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Disposition {
    Append,
    Replace,
    Merge,
}

impl Disposition {
    pub fn parse(s: &str) -> Result<Disposition, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "append" => Ok(Disposition::Append),
            "replace" => Ok(Disposition::Replace),
            "merge" => Ok(Disposition::Merge),
            other => Err(format!(
                "unknown write disposition '{}' (expected append, replace or merge)",
                other
            )),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Disposition::Append => "append",
            Disposition::Replace => "replace",
            Disposition::Merge => "merge",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PruneTable {
    /// Qualified, quoted table name.
    pub qualified: String,
    pub depth: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PruneSpec {
    pub disposition: String,
    pub load_id: String,
    /// Root table, qualified and quoted.
    pub root: String,
    /// Every table in the plan, root included.
    pub tables: Vec<PruneTable>,
    /// Primary-key columns on the root table. Required for merge.
    #[serde(default)]
    pub primary_key: Vec<String>,
    /// Columns the root table actually has, used to reject a bad key early.
    #[serde(default)]
    pub root_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PruneOut {
    pub statements: Vec<String>,
    pub disposition: String,
    pub errors: Vec<String>,
}

pub fn prune(spec: &PruneSpec) -> PruneOut {
    let disposition = match Disposition::parse(&spec.disposition) {
        Ok(d) => d,
        Err(e) => {
            return PruneOut {
                statements: Vec::new(),
                disposition: spec.disposition.clone(),
                errors: vec![e],
            }
        }
    };

    let load = sql_lit(&spec.load_id);
    let id = quote_ident(ID_COL);
    let load_col = quote_ident(LOAD_COL);
    let mut statements = Vec::new();
    let mut errors = Vec::new();

    match disposition {
        Disposition::Append => {}

        Disposition::Replace => {
            // Deepest first, so a child never briefly outlives its parent.
            let mut tables = spec.tables.clone();
            tables.sort_by(|a, b| b.depth.cmp(&a.depth));
            for t in &tables {
                statements.push(format!(
                    "DELETE FROM {} WHERE {} <> {}",
                    t.qualified, load_col, load
                ));
            }
        }

        Disposition::Merge => {
            if spec.primary_key.is_empty() {
                errors.push(
                    "merge needs primary_key — a source without a stable key can only be appended"
                        .to_string(),
                );
                return PruneOut {
                    statements: Vec::new(),
                    disposition: disposition.as_str().to_string(),
                    errors,
                };
            }
            for k in &spec.primary_key {
                if !spec.root_columns.is_empty()
                    && !spec
                        .root_columns
                        .iter()
                        .any(|c| c.eq_ignore_ascii_case(k))
                {
                    errors.push(format!(
                        "primary_key column '{}' is not a column of {} (available: {})",
                        k,
                        spec.root,
                        spec.root_columns.join(", ")
                    ));
                }
            }
            if !errors.is_empty() {
                return PruneOut {
                    statements: Vec::new(),
                    disposition: disposition.as_str().to_string(),
                    errors,
                };
            }

            // NULLs compare equal here on purpose: a key that is NULL in both
            // the stored row and the incoming row is the same business key.
            let key_match: Vec<String> = spec
                .primary_key
                .iter()
                .map(|k| {
                    let c = quote_ident(k);
                    format!("n.{c} IS NOT DISTINCT FROM t.{c}", c = c)
                })
                .collect();

            // Inlined rather than staged in a temp table: a temp table lives
            // in the `temp` catalog, and DuckDB refuses to let one transaction
            // write to two catalogs — which would break loading into an
            // attached lakehouse. The subquery only reads the root table, and
            // the root rows are deleted last, so every child sees the same set.
            let stale = format!(
                "SELECT t.{id} FROM {root} t WHERE t.{load_col} <> {load} \
                 AND EXISTS (SELECT 1 FROM {root} n WHERE n.{load_col} = {load} AND {keys})",
                id = id,
                root = spec.root,
                load_col = load_col,
                load = load,
                keys = key_match.join(" AND ")
            );

            let mut children: Vec<&PruneTable> =
                spec.tables.iter().filter(|t| t.depth > 0).collect();
            children.sort_by(|a, b| b.depth.cmp(&a.depth));
            for t in children {
                statements.push(format!(
                    "DELETE FROM {tbl} WHERE split_part({id}, '/', 1) IN ({stale})",
                    tbl = t.qualified,
                    id = id,
                    stale = stale
                ));
            }
            statements.push(format!(
                "DELETE FROM {root} WHERE {id} IN ({stale})",
                root = spec.root,
                id = id,
                stale = stale
            ));
        }
    }

    PruneOut {
        statements,
        disposition: disposition.as_str().to_string(),
        errors,
    }
}

/// Split a `primary_key` argument (`"id"` or `"a, b"`) into column names.
pub fn split_key(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(disposition: &str, pk: Vec<&str>) -> PruneSpec {
        PruneSpec {
            disposition: disposition.into(),
            load_id: "L2".into(),
            root: "\"raw\".\"orders\"".into(),
            tables: vec![
                PruneTable {
                    qualified: "\"raw\".\"orders\"".into(),
                    depth: 0,
                },
                PruneTable {
                    qualified: "\"raw\".\"orders__items\"".into(),
                    depth: 1,
                },
                PruneTable {
                    qualified: "\"raw\".\"orders__items__tags\"".into(),
                    depth: 2,
                },
            ],
            primary_key: pk.iter().map(|s| s.to_string()).collect(),
            root_columns: vec!["id".into(), "code".into()],
        }
    }

    #[test]
    fn append_prunes_nothing() {
        let out = prune(&spec("append", vec![]));
        assert!(out.statements.is_empty());
        assert!(out.errors.is_empty());
    }

    #[test]
    fn empty_disposition_means_append() {
        assert!(prune(&spec("", vec![])).statements.is_empty());
    }

    #[test]
    fn replace_deletes_every_older_row_deepest_first() {
        let out = prune(&spec("replace", vec![]));
        assert_eq!(out.statements.len(), 3);
        assert!(out.statements[0].contains("orders__items__tags"));
        assert!(out.statements[1].contains("orders__items\""));
        assert!(out.statements[2].contains("orders\""));
        for s in &out.statements {
            assert!(s.contains("\"_orch_load_id\" <> 'L2'"));
        }
    }

    #[test]
    fn merge_deletes_children_first_then_the_roots() {
        let out = prune(&spec("merge", vec!["id"]));
        assert_eq!(out.statements.len(), 3);
        assert!(out.statements[0].contains("orders__items__tags"));
        assert!(out.statements[1].contains("orders__items\""));
        assert!(out.statements[2].starts_with("DELETE FROM \"raw\".\"orders\""));
        for s in &out.statements {
            assert!(s.contains("n.\"id\" IS NOT DISTINCT FROM t.\"id\""));
        }
        assert!(out.errors.is_empty());
    }

    #[test]
    fn merge_stages_nothing_so_it_can_cross_catalogs() {
        let out = prune(&spec("merge", vec!["id"]));
        for s in &out.statements {
            assert!(!s.contains("TEMP TABLE"), "no temp staging: {}", s);
        }
    }

    #[test]
    fn merge_uses_prefix_matching_for_descendants() {
        let out = prune(&spec("merge", vec!["id"]));
        assert!(out.statements[0].contains("split_part(\"_orch_id\", '/', 1)"));
    }

    #[test]
    fn merge_with_composite_key() {
        let out = prune(&spec("merge", vec!["id", "code"]));
        assert!(out.statements[0].contains("n.\"id\" IS NOT DISTINCT FROM t.\"id\""));
        assert!(out.statements[0].contains("n.\"code\" IS NOT DISTINCT FROM t.\"code\""));
    }

    #[test]
    fn merge_without_a_key_is_refused() {
        let out = prune(&spec("merge", vec![]));
        assert!(out.statements.is_empty());
        assert_eq!(out.errors.len(), 1);
        assert!(out.errors[0].contains("primary_key"));
    }

    #[test]
    fn merge_with_an_unknown_key_column_is_refused() {
        let out = prune(&spec("merge", vec!["nope"]));
        assert!(out.statements.is_empty());
        assert!(out.errors[0].contains("not a column"));
    }

    #[test]
    fn unknown_disposition_is_refused() {
        let out = prune(&spec("upsert", vec![]));
        assert!(out.errors[0].contains("unknown write disposition"));
    }

    #[test]
    fn key_splitting() {
        assert_eq!(split_key("id"), vec!["id"]);
        assert_eq!(split_key(" a , b "), vec!["a", "b"]);
        assert!(split_key("").is_empty());
    }
}
