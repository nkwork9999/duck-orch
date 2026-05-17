// Column-level lineage extractor using sqlparser-rs AST.
//
// Approach (independent implementation; algorithm idea drawn from sqlglot's
// Scope-based traversal — no source copied):
//
//  1. Parse SQL → AST.
//  2. For each top-level statement, identify the output target:
//     - INSERT INTO t SELECT ...   → t is the output dataset
//     - CREATE [OR REPLACE] TABLE t AS SELECT ...   → t is the output
//     - bare SELECT → no target dataset; per-column lineage is still emitted
//       against an anonymous output.
//  3. Build a Scope from the FROM clause (alias → real name) and CTEs.
//  4. For each SELECT projection item, walk the Expression tree and collect
//     contributing source columns + transformation subtype.
//  5. Wildcard `*` / `t.*` resolution requires a schema map (table → columns).
//     Caller-provided; if missing, those projection items are emitted with
//     `unresolved: true` so the caller can do a second pass via DuckDB
//     `Connection::Prepare()`.
//
// Subtype taxonomy follows the OpenLineage `transformations` facet:
//   IDENTITY        — bare reference, no expression
//   TRANSFORMATION  — single-column expression / cast / function / arithmetic
//   AGGREGATION     — inside SUM/COUNT/AVG/MIN/MAX/etc.
//   WINDOW          — inside an OVER(...) clause
//   FILTER          — sourced via WHERE clause
//   JOIN_KEY        — sourced via JOIN ... ON
//   GROUP_BY        — referenced in GROUP BY
//   SORT            — referenced in ORDER BY

use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    BinaryOperator, CastKind, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Insert,
    JoinConstraint, JoinOperator, ObjectName, Query, Select, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins, UnaryOperator, Value,
};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;
use std::collections::{BTreeMap, HashMap, HashSet};

// =============================================================================
// Public types — mirror the OpenLineage `columnLineage` facet shape
// =============================================================================

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransformationKind {
    #[serde(rename = "DIRECT")]
    Direct,
    #[serde(rename = "INDIRECT")]
    Indirect,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Subtype {
    #[serde(rename = "IDENTITY")]
    Identity,
    #[serde(rename = "TRANSFORMATION")]
    Transformation,
    #[serde(rename = "AGGREGATION")]
    Aggregation,
    #[serde(rename = "WINDOW")]
    Window,
    #[serde(rename = "FILTER")]
    Filter,
    #[serde(rename = "JOIN_KEY")]
    JoinKey,
    #[serde(rename = "GROUP_BY")]
    GroupBy,
    #[serde(rename = "SORT")]
    Sort,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transformation {
    #[serde(rename = "type")]
    pub kind: TransformationKind,
    pub subtype: Subtype,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputField {
    pub dataset: String,
    pub field: String,
    pub transformations: Vec<Transformation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnLineage {
    pub output_field: String,
    pub inputs: Vec<InputField>,
    /// True if any wildcards or unresolved references prevented full extraction.
    /// Caller may rerun with a schema map to fully resolve.
    #[serde(default, skip_serializing_if = "is_false")]
    pub unresolved: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractResult {
    /// catalog.schema.table when known, otherwise empty.
    pub output_dataset: String,
    pub columns: Vec<ColumnLineage>,
    /// Tables referenced via wildcards that need schema resolution.
    pub unresolved_wildcards: Vec<String>,
}

/// Optional schema info: dataset_name -> ordered column list.
pub type SchemaMap = HashMap<String, Vec<String>>;

// =============================================================================
// Public entry point
// =============================================================================

pub fn extract_column_lineage(sql: &str, schema: &SchemaMap) -> Vec<ExtractResult> {
    let dialect = DuckDbDialect {};
    let stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    let mut results = Vec::new();
    for stmt in &stmts {
        if let Some(r) = extract_one(stmt, schema) {
            results.push(r);
        }
    }
    results
}

// =============================================================================
// Per-statement entry
// =============================================================================

fn extract_one(stmt: &Statement, schema: &SchemaMap) -> Option<ExtractResult> {
    match stmt {
        Statement::Insert(Insert {
            table_name, source, ..
        }) => {
            let target = name_to_string(table_name);
            let q = source.as_ref()?;
            let (cols, unresolved) = extract_query(q, schema);
            Some(ExtractResult {
                output_dataset: target,
                columns: cols,
                unresolved_wildcards: unresolved,
            })
        }
        Statement::CreateTable(ct) => {
            let target = name_to_string(&ct.name);
            let q = ct.query.as_ref()?;
            let (cols, unresolved) = extract_query(q, schema);
            Some(ExtractResult {
                output_dataset: target,
                columns: cols,
                unresolved_wildcards: unresolved,
            })
        }
        Statement::CreateView { name, query, .. } => {
            let target = name_to_string(name);
            let (cols, unresolved) = extract_query(query, schema);
            Some(ExtractResult {
                output_dataset: target,
                columns: cols,
                unresolved_wildcards: unresolved,
            })
        }
        Statement::Query(q) => {
            let (cols, unresolved) = extract_query(q, schema);
            Some(ExtractResult {
                output_dataset: String::new(),
                columns: cols,
                unresolved_wildcards: unresolved,
            })
        }
        _ => None,
    }
}

fn extract_query(q: &Query, schema: &SchemaMap) -> (Vec<ColumnLineage>, Vec<String>) {
    let mut cte_scope: HashMap<String, Vec<String>> = HashMap::new();
    if let Some(with) = &q.with {
        for cte in &with.cte_tables {
            // Best-effort: record CTE name and (later) its output columns.
            // We don't recursively extract CTE column lineage in MVP.
            cte_scope.insert(cte.alias.name.value.clone(), Vec::new());
        }
    }
    extract_setexpr(&q.body, schema, &cte_scope, q)
}

fn extract_setexpr(
    s: &SetExpr,
    schema: &SchemaMap,
    cte_scope: &HashMap<String, Vec<String>>,
    parent_query: &Query,
) -> (Vec<ColumnLineage>, Vec<String>) {
    match s {
        SetExpr::Select(sel) => extract_select(sel, schema, cte_scope, parent_query),
        SetExpr::Query(q) => extract_query(q, schema),
        SetExpr::SetOperation { left, right, .. } => {
            // UNION/INTERSECT/EXCEPT: outputs are positional. Combine by index.
            let (lc, lu) = extract_setexpr(left, schema, cte_scope, parent_query);
            let (rc, ru) = extract_setexpr(right, schema, cte_scope, parent_query);
            let mut merged: Vec<ColumnLineage> = Vec::new();
            let n = lc.len().max(rc.len());
            for i in 0..n {
                let l = lc.get(i);
                let r = rc.get(i);
                let name = l
                    .map(|x| x.output_field.clone())
                    .or_else(|| r.map(|x| x.output_field.clone()))
                    .unwrap_or_else(|| format!("col_{}", i));
                let mut inputs: Vec<InputField> = Vec::new();
                if let Some(x) = l {
                    inputs.extend(x.inputs.clone());
                }
                if let Some(x) = r {
                    inputs.extend(x.inputs.clone());
                }
                let unresolved = l.map(|x| x.unresolved).unwrap_or(false)
                    || r.map(|x| x.unresolved).unwrap_or(false);
                merged.push(ColumnLineage {
                    output_field: name,
                    inputs,
                    unresolved,
                });
            }
            let mut unresolved_total = lu;
            unresolved_total.extend(ru);
            (merged, unresolved_total)
        }
        _ => (Vec::new(), Vec::new()),
    }
}

// =============================================================================
// SELECT body — where most of the work happens
// =============================================================================

#[derive(Debug, Default)]
struct SelectScope {
    /// alias → real table name
    aliases: HashMap<String, String>,
    /// real_name (or alias if no alias) — preserves declaration order for `*` resolution
    tables_in_order: Vec<String>,
}

fn extract_select(
    sel: &Select,
    schema: &SchemaMap,
    _cte_scope: &HashMap<String, Vec<String>>,
    _parent_query: &Query,
) -> (Vec<ColumnLineage>, Vec<String>) {
    let mut scope = SelectScope::default();
    for twj in &sel.from {
        collect_tables(twj, &mut scope);
    }

    // INDIRECT contributors: WHERE / JOIN ON / GROUP BY / ORDER BY all force lineage.
    let mut indirect_inputs: Vec<InputField> = Vec::new();

    // JOIN ON conditions
    for twj in &sel.from {
        for j in &twj.joins {
            let constraint_opt = match &j.join_operator {
                JoinOperator::Inner(c)
                | JoinOperator::LeftOuter(c)
                | JoinOperator::RightOuter(c)
                | JoinOperator::FullOuter(c)
                | JoinOperator::LeftSemi(c)
                | JoinOperator::RightSemi(c)
                | JoinOperator::LeftAnti(c)
                | JoinOperator::RightAnti(c) => Some(c),
                _ => None,
            };
            if let Some(JoinConstraint::On(expr)) = constraint_opt {
                let refs = collect_refs(expr, &scope);
                for r in refs {
                    indirect_inputs.push(InputField {
                        dataset: r.0,
                        field: r.1,
                        transformations: vec![Transformation {
                            kind: TransformationKind::Indirect,
                            subtype: Subtype::JoinKey,
                            description: None,
                        }],
                    });
                }
            }
        }
    }
    // WHERE
    if let Some(where_expr) = &sel.selection {
        let refs = collect_refs(where_expr, &scope);
        for r in refs {
            indirect_inputs.push(InputField {
                dataset: r.0,
                field: r.1,
                transformations: vec![Transformation {
                    kind: TransformationKind::Indirect,
                    subtype: Subtype::Filter,
                    description: None,
                }],
            });
        }
    }
    // GROUP BY
    let group_exprs = match &sel.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => exprs.clone(),
        _ => Vec::new(),
    };
    for ge in &group_exprs {
        let refs = collect_refs(ge, &scope);
        for r in refs {
            indirect_inputs.push(InputField {
                dataset: r.0,
                field: r.1,
                transformations: vec![Transformation {
                    kind: TransformationKind::Indirect,
                    subtype: Subtype::GroupBy,
                    description: None,
                }],
            });
        }
    }

    // Per-projection lineage
    let mut columns: Vec<ColumnLineage> = Vec::new();
    let mut unresolved_wildcards: Vec<String> = Vec::new();

    for (idx, item) in sel.projection.iter().enumerate() {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let name = derive_output_name(expr).unwrap_or_else(|| format!("col_{}", idx));
                let inputs = lineage_for_expr(expr, &scope);
                columns.push(ColumnLineage {
                    output_field: name,
                    inputs: with_indirect(inputs, &indirect_inputs),
                    unresolved: false,
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let inputs = lineage_for_expr(expr, &scope);
                columns.push(ColumnLineage {
                    output_field: alias.value.clone(),
                    inputs: with_indirect(inputs, &indirect_inputs),
                    unresolved: false,
                });
            }
            SelectItem::Wildcard(_) => {
                // SELECT * — needs schema. If we have schemas for all tables, expand.
                let mut handled = true;
                for tbl in &scope.tables_in_order {
                    let cols = match schema.get(tbl) {
                        Some(c) => c,
                        None => {
                            unresolved_wildcards.push(tbl.clone());
                            handled = false;
                            continue;
                        }
                    };
                    for c in cols {
                        columns.push(ColumnLineage {
                            output_field: c.clone(),
                            inputs: with_indirect(
                                vec![InputField {
                                    dataset: tbl.clone(),
                                    field: c.clone(),
                                    transformations: vec![Transformation {
                                        kind: TransformationKind::Direct,
                                        subtype: Subtype::Identity,
                                        description: None,
                                    }],
                                }],
                                &indirect_inputs,
                            ),
                            unresolved: false,
                        });
                    }
                }
                if !handled {
                    columns.push(ColumnLineage {
                        output_field: format!("*_pos{}", idx),
                        inputs: Vec::new(),
                        unresolved: true,
                    });
                }
            }
            SelectItem::QualifiedWildcard(name, _) => {
                let table_alias = name_to_string(&simple_object_name(name));
                let real = scope.aliases.get(&table_alias).cloned().unwrap_or(table_alias.clone());
                match schema.get(&real) {
                    Some(cols) => {
                        for c in cols {
                            columns.push(ColumnLineage {
                                output_field: c.clone(),
                                inputs: with_indirect(
                                    vec![InputField {
                                        dataset: real.clone(),
                                        field: c.clone(),
                                        transformations: vec![Transformation {
                                            kind: TransformationKind::Direct,
                                            subtype: Subtype::Identity,
                                            description: None,
                                        }],
                                    }],
                                    &indirect_inputs,
                                ),
                                unresolved: false,
                            });
                        }
                    }
                    None => {
                        unresolved_wildcards.push(real);
                        columns.push(ColumnLineage {
                            output_field: format!("{}.*_pos{}", table_alias, idx),
                            inputs: Vec::new(),
                            unresolved: true,
                        });
                    }
                }
            }
        }
    }

    (columns, unresolved_wildcards)
}

fn with_indirect(mut direct: Vec<InputField>, indirect: &[InputField]) -> Vec<InputField> {
    let existing: HashSet<(String, String)> = direct
        .iter()
        .map(|f| (f.dataset.clone(), f.field.clone()))
        .collect();
    for ind in indirect {
        if !existing.contains(&(ind.dataset.clone(), ind.field.clone())) {
            direct.push(ind.clone());
        }
    }
    direct
}

// =============================================================================
// Expression analysis
// =============================================================================

/// Compute lineage entries for a projection expression. Returns DIRECT-only
/// inputs (INDIRECT contributors are added separately at the SELECT level).
fn lineage_for_expr(expr: &Expr, scope: &SelectScope) -> Vec<InputField> {
    let mut bag: BTreeMap<(String, String), Vec<Transformation>> = BTreeMap::new();
    walk_expr_into(expr, scope, false, false, false, &mut bag);
    bag.into_iter()
        .map(|((dataset, field), transformations)| InputField {
            dataset,
            field,
            transformations,
        })
        .collect()
}

/// Helper used by SELECT-level INDIRECT collection (WHERE / JOIN ON / GROUP BY).
/// Returns (dataset, field) pairs without transformation tagging.
fn collect_refs(expr: &Expr, scope: &SelectScope) -> Vec<(String, String)> {
    let mut bag: BTreeMap<(String, String), Vec<Transformation>> = BTreeMap::new();
    walk_expr_into(expr, scope, false, false, false, &mut bag);
    bag.into_keys().collect()
}

/// Recursive walker. Flags accumulate via parent → child.
fn walk_expr_into(
    expr: &Expr,
    scope: &SelectScope,
    in_aggregate: bool,
    in_window: bool,
    in_transformation: bool,
    bag: &mut BTreeMap<(String, String), Vec<Transformation>>,
) {
    match expr {
        Expr::Identifier(id) => {
            // Bare identifier: try to resolve against the single table in scope.
            let dataset = if scope.tables_in_order.len() == 1 {
                scope.tables_in_order[0].clone()
            } else {
                String::new()
            };
            push(bag, dataset, id.value.clone(), in_aggregate, in_window, in_transformation);
        }
        Expr::CompoundIdentifier(parts) => {
            if parts.len() >= 2 {
                let table_alias = parts[parts.len() - 2].value.clone();
                let column = parts[parts.len() - 1].value.clone();
                let dataset = scope.aliases.get(&table_alias).cloned().unwrap_or(table_alias);
                push(bag, dataset, column, in_aggregate, in_window, in_transformation);
            }
        }
        Expr::Cast { expr: inner, .. } => {
            walk_expr_into(inner, scope, in_aggregate, in_window, true, bag);
        }
        Expr::BinaryOp { left, right, .. } => {
            walk_expr_into(left, scope, in_aggregate, in_window, true, bag);
            walk_expr_into(right, scope, in_aggregate, in_window, true, bag);
        }
        Expr::UnaryOp { expr: inner, .. } => {
            walk_expr_into(inner, scope, in_aggregate, in_window, true, bag);
        }
        Expr::Function(func) => {
            let fname = name_to_string(&func.name).to_uppercase();
            let agg_now = in_aggregate || is_aggregate(&fname);
            let win_now = in_window || func.over.is_some();
            // Recurse into args
            if let FunctionArguments::List(arglist) = &func.args {
                for a in &arglist.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = a {
                        walk_expr_into(e, scope, agg_now, win_now, true, bag);
                    } else if let FunctionArg::Named { arg: FunctionArgExpr::Expr(e), .. } = a {
                        walk_expr_into(e, scope, agg_now, win_now, true, bag);
                    }
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                walk_expr_into(op, scope, in_aggregate, in_window, true, bag);
            }
            for c in conditions {
                walk_expr_into(c, scope, in_aggregate, in_window, true, bag);
            }
            for r in results {
                walk_expr_into(r, scope, in_aggregate, in_window, true, bag);
            }
            if let Some(er) = else_result {
                walk_expr_into(er, scope, in_aggregate, in_window, true, bag);
            }
        }
        Expr::Nested(inner) => {
            walk_expr_into(inner, scope, in_aggregate, in_window, in_transformation, bag);
        }
        Expr::Value(_) => {}
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            walk_expr_into(inner, scope, in_aggregate, in_window, true, bag);
        }
        // Other variants: best-effort no-op (literal-like)
        _ => {}
    }
}

fn push(
    bag: &mut BTreeMap<(String, String), Vec<Transformation>>,
    dataset: String,
    field: String,
    in_aggregate: bool,
    in_window: bool,
    in_transformation: bool,
) {
    let (kind, subtype) = if in_aggregate {
        (TransformationKind::Indirect, Subtype::Aggregation)
    } else if in_window {
        (TransformationKind::Indirect, Subtype::Window)
    } else if in_transformation {
        (TransformationKind::Direct, Subtype::Transformation)
    } else {
        (TransformationKind::Direct, Subtype::Identity)
    };
    let t = Transformation {
        kind,
        subtype,
        description: None,
    };
    let entry = bag.entry((dataset, field)).or_insert_with(Vec::new);
    if !entry.iter().any(|x| x.kind == t.kind && x.subtype == t.subtype) {
        entry.push(t);
    }
}

// =============================================================================
// Helpers — table scope, name resolution, output naming
// =============================================================================

fn collect_tables(twj: &TableWithJoins, scope: &mut SelectScope) {
    collect_table_factor(&twj.relation, scope);
    for j in &twj.joins {
        collect_table_factor(&j.relation, scope);
    }
}

fn collect_table_factor(tf: &TableFactor, scope: &mut SelectScope) {
    if let TableFactor::Table { name, alias, .. } = tf {
        let real = name_to_string(name);
        let alias_s = alias.as_ref().map(|a| a.name.value.clone()).unwrap_or_else(|| real.clone());
        scope.aliases.insert(alias_s.clone(), real.clone());
        // For `*` resolution we use alias (or real if no alias)
        scope.tables_in_order.push(real);
    }
}

fn name_to_string(n: &ObjectName) -> String {
    n.0.iter().map(|id| id.value.clone()).collect::<Vec<_>>().join(".")
}

fn simple_object_name(n: &ObjectName) -> ObjectName {
    n.clone()
}

fn derive_output_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(id) => Some(id.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.clone()),
        _ => None,
    }
}

fn is_aggregate(name: &str) -> bool {
    matches!(
        name,
        "SUM" | "COUNT" | "AVG" | "MIN" | "MAX"
            | "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP"
            | "VARIANCE" | "VAR_POP" | "VAR_SAMP"
            | "MEDIAN" | "MODE" | "ARRAY_AGG" | "STRING_AGG"
            | "LIST" | "FIRST" | "LAST" | "ANY_VALUE" | "BIT_AND" | "BIT_OR"
    )
}

// Tame compiler complaints about unused imports under no-test builds
#[allow(dead_code)]
fn _silence(_x: BinaryOperator, _y: UnaryOperator, _z: CastKind, _v: Value) {}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_schema() -> SchemaMap {
        HashMap::new()
    }

    #[test]
    fn simple_alias() {
        let sql = "CREATE TABLE out AS SELECT id, UPPER(name) AS upname FROM users";
        let r = extract_column_lineage(sql, &empty_schema());
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].output_dataset, "out");
        let cols = &r[0].columns;
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].output_field, "id");
        assert_eq!(cols[0].inputs.len(), 1);
        assert_eq!(cols[0].inputs[0].dataset, "users");
        assert_eq!(cols[0].inputs[0].field, "id");
        assert_eq!(cols[0].inputs[0].transformations[0].subtype, Subtype::Identity);

        assert_eq!(cols[1].output_field, "upname");
        assert_eq!(cols[1].inputs[0].field, "name");
        assert_eq!(cols[1].inputs[0].transformations[0].subtype, Subtype::Transformation);
    }

    #[test]
    fn aggregate_indirect() {
        let sql = "CREATE TABLE stats AS SELECT country, COUNT(*) AS users FROM clean_users GROUP BY country";
        let r = extract_column_lineage(sql, &empty_schema());
        let cols = &r[0].columns;
        // country: direct from clean_users.country, but also INDIRECT GROUP_BY
        let country = cols.iter().find(|c| c.output_field == "country").unwrap();
        assert!(country.inputs.iter().any(|i| i.field == "country"));
    }

    #[test]
    fn join_indirect_join_key() {
        let sql = "CREATE TABLE rev AS SELECT u.country, o.amount FROM users u JOIN orders o ON u.id = o.user_id";
        let r = extract_column_lineage(sql, &empty_schema());
        let cols = &r[0].columns;
        // o.amount column should have at least 1 INDIRECT JOIN_KEY entry
        let amount = cols.iter().find(|c| c.output_field == "amount").unwrap();
        assert!(amount
            .inputs
            .iter()
            .any(|i| i.transformations.iter().any(|t| t.subtype == Subtype::JoinKey)));
    }

    #[test]
    fn wildcard_unresolved_when_no_schema() {
        let sql = "CREATE TABLE copy AS SELECT * FROM users";
        let r = extract_column_lineage(sql, &empty_schema());
        assert!(!r[0].unresolved_wildcards.is_empty());
    }

    #[test]
    fn wildcard_resolved_with_schema() {
        let sql = "CREATE TABLE copy AS SELECT * FROM users";
        let mut s = HashMap::new();
        s.insert("users".to_string(), vec!["id".to_string(), "name".to_string()]);
        let r = extract_column_lineage(sql, &s);
        let cols = &r[0].columns;
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].output_field, "id");
        assert_eq!(cols[1].output_field, "name");
    }
}
