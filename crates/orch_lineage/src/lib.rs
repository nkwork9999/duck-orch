// Extract input/output tables from a SQL string using sqlparser-rs.
// Best-effort: covers INSERT/UPDATE/CREATE TABLE AS / SELECT.

use sqlparser::ast::{Insert, ObjectName, Query, SetExpr, Statement, TableFactor, TableWithJoins};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;
use std::collections::BTreeSet;

pub fn extract_io(sql: &str) -> (Vec<String>, Vec<String>) {
    let dialect = DuckDbDialect {};
    let stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return (Vec::new(), Vec::new()),
    };

    let mut inputs = BTreeSet::new();
    let mut outputs = BTreeSet::new();

    for stmt in &stmts {
        walk_stmt(stmt, &mut inputs, &mut outputs);
    }

    (inputs.into_iter().collect(), outputs.into_iter().collect())
}

fn walk_stmt(stmt: &Statement, ins: &mut BTreeSet<String>, outs: &mut BTreeSet<String>) {
    match stmt {
        Statement::Insert(Insert {
            table_name, source, ..
        }) => {
            outs.insert(name_to_string(table_name));
            if let Some(q) = source {
                walk_query(q, ins);
            }
        }
        Statement::Update { table, .. } => {
            if let TableFactor::Table { name, .. } = &table.relation {
                outs.insert(name_to_string(name));
            }
            walk_table_with_joins(table, ins);
        }
        Statement::CreateTable(ct) => {
            outs.insert(name_to_string(&ct.name));
            if let Some(q) = &ct.query {
                walk_query(q, ins);
            }
        }
        Statement::CreateView { name, query, .. } => {
            outs.insert(name_to_string(name));
            walk_query(query, ins);
        }
        Statement::Query(q) => {
            walk_query(q, ins);
        }
        _ => {}
    }
}

fn walk_query(q: &Query, ins: &mut BTreeSet<String>) {
    if let Some(with) = &q.with {
        let cte_names: BTreeSet<String> = with
            .cte_tables
            .iter()
            .map(|c| c.alias.name.value.to_string())
            .collect();
        for cte in &with.cte_tables {
            walk_query(&cte.query, ins);
        }
        let mut tmp = BTreeSet::new();
        walk_set_expr(&q.body, &mut tmp);
        for n in tmp {
            if !cte_names.contains(&n) {
                ins.insert(n);
            }
        }
    } else {
        walk_set_expr(&q.body, ins);
    }
}

fn walk_set_expr(s: &SetExpr, ins: &mut BTreeSet<String>) {
    match s {
        SetExpr::Select(sel) => {
            for twj in &sel.from {
                walk_table_with_joins(twj, ins);
            }
        }
        SetExpr::Query(q) => walk_query(q, ins),
        SetExpr::SetOperation { left, right, .. } => {
            walk_set_expr(left, ins);
            walk_set_expr(right, ins);
        }
        SetExpr::Values(_) => {}
        SetExpr::Insert(_) => {}
        SetExpr::Update(_) => {}
        SetExpr::Table(t) => {
            if let Some(name) = &t.table_name {
                ins.insert(name.clone());
            }
        }
    }
}

fn walk_table_with_joins(twj: &TableWithJoins, ins: &mut BTreeSet<String>) {
    walk_table_factor(&twj.relation, ins);
    for j in &twj.joins {
        walk_table_factor(&j.relation, ins);
    }
}

fn walk_table_factor(tf: &TableFactor, ins: &mut BTreeSet<String>) {
    match tf {
        TableFactor::Table { name, .. } => {
            ins.insert(name_to_string(name));
        }
        TableFactor::Derived { subquery, .. } => walk_query(subquery, ins),
        TableFactor::TableFunction { .. } => {}
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => walk_table_with_joins(table_with_joins, ins),
        _ => {}
    }
}

fn name_to_string(n: &ObjectName) -> String {
    n.0.iter().map(|id| id.value.clone()).collect::<Vec<_>>().join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_simple_insert() {
        let (i, o) = extract_io("INSERT INTO clean SELECT * FROM raw WHERE deleted = false");
        assert_eq!(o, vec!["clean"]);
        assert_eq!(i, vec!["raw"]);
    }

    #[test]
    fn extract_create_table_as() {
        let (i, o) = extract_io(
            "CREATE OR REPLACE TABLE analytics.user_stats AS \
             SELECT country, COUNT(*) FROM analytics.clean_users GROUP BY 1",
        );
        assert_eq!(o, vec!["analytics.user_stats"]);
        assert_eq!(i, vec!["analytics.clean_users"]);
    }

    #[test]
    fn extract_join() {
        let (i, _) = extract_io("INSERT INTO x SELECT * FROM a JOIN b ON a.id = b.id");
        assert_eq!(i, vec!["a", "b"]);
    }
}
