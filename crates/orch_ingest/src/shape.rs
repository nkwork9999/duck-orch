// Normalization plan: nested type tree → flat parent/child tables + SQL.
//
// Rules (P0):
//   scalar          → one column
//   struct          → flattened into the parent, names joined with `__`
//   list of struct  → child table, one row per element
//   list of scalar  → child table with a single `value` column
//   list of list    → child table whose element becomes another child
//   map             → collapsed to a JSON string column
//   beyond max_nesting → collapsed to a JSON string column
//
// Control columns:
//   _orch_id        `<load_id>#<row number>` on the root, then `<parent>/<index>`
//   _orch_parent_id link to the parent row (child tables only)
//   _orch_index     0-based position in the source array (child tables only)
//   _orch_load_id   which load wrote the row
//
// The identifiers are derived from the load id and the source row number, so
// they are stable once written. P0 is append-only, so nothing ever needs to
// re-derive an id for a row that already exists — which is exactly why the
// row-content hashing idea was dropped: adding a column would have changed
// every id.

use serde::{Deserialize, Serialize};

use crate::typ::{parse, quote_ident, sql_lit, Ty};

pub const ID_COL: &str = "_orch_id";
pub const PARENT_COL: &str = "_orch_parent_id";
pub const INDEX_COL: &str = "_orch_index";
pub const LOAD_COL: &str = "_orch_load_id";

fn default_max_nesting() -> usize {
    3
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlanSpec {
    /// Target table, optionally schema-qualified (`raw.orders`).
    pub target: String,
    /// Relation the INSERTs read from (a temp view over `read_json`).
    pub source_relation: String,
    pub load_id: String,
    /// Column in `source_relation` carrying the source row number.
    pub row_key: String,
    #[serde(default = "default_max_nesting")]
    pub max_nesting: usize,
    pub columns: Vec<ColSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanCol {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TablePlan {
    /// Unqualified table name, e.g. `orders__items`.
    pub name: String,
    /// Ready-to-use qualified and quoted name, e.g. `"raw"."orders__items"`.
    pub qualified: String,
    pub parent: Option<String>,
    pub depth: usize,
    pub columns: Vec<PlanCol>,
    pub create_sql: String,
    pub insert_sql: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Plan {
    /// Attached catalog, when the target names one (`lake.raw.orders`).
    /// This is what lets a load land in DuckLake instead of the local file.
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub tables: Vec<TablePlan>,
    pub warnings: Vec<String>,
}

/// A list column found while flattening — becomes a child table unless it sits
/// deeper than `max_nesting`.
struct ChildDef {
    /// Flattened name of the list column relative to the current table.
    name: String,
    /// SQL expression for the list, already bound to the current alias.
    expr: String,
    elem: Ty,
}

pub fn build_plan(spec: &PlanSpec) -> Plan {
    let (catalog, schema, base) = split_target(&spec.target);
    let mut warnings = Vec::new();
    let mut tables = Vec::new();

    let load_lit = sql_lit(&spec.load_id);
    let root_from = format!(
        "(SELECT {load} || '#' || CAST({rk} AS VARCHAR) AS {id}, * FROM {src}) AS l0",
        load = load_lit,
        rk = quote_ident(&spec.row_key),
        id = quote_ident(ID_COL),
        src = spec.source_relation
    );

    // ---- root table -------------------------------------------------------
    let mut cols = vec![
        PlanCol {
            name: ID_COL.into(),
            ty: "VARCHAR".into(),
        },
        PlanCol {
            name: LOAD_COL.into(),
            ty: "VARCHAR".into(),
        },
    ];
    let mut exprs = vec![
        format!("l0.{}", quote_ident(ID_COL)),
        load_lit.clone(),
    ];
    let mut children = Vec::new();
    let mut used: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();

    for c in &spec.columns {
        if c.name == spec.row_key {
            continue;
        }
        let ty = parse(&c.ty);
        let expr = format!("l0.{}", quote_ident(&c.name));
        collect(
            &normalize(&c.name),
            &expr,
            &ty,
            &mut cols,
            &mut exprs,
            &mut children,
            &mut used,
            &mut warnings,
        );
    }

    finish_children(
        &mut cols,
        &mut exprs,
        &mut used,
        &children,
        1,
        spec.max_nesting,
        &mut warnings,
    );

    let root_qualified = qualify(&catalog, &schema, &base);
    tables.push(TablePlan {
        name: base.clone(),
        qualified: root_qualified.clone(),
        parent: None,
        depth: 0,
        create_sql: create_sql(&root_qualified, &cols),
        insert_sql: insert_sql(&root_qualified, &cols, &exprs, &root_from),
        columns: cols,
    });

    // ---- child tables, breadth-first -------------------------------------
    let mut queue: Vec<(String, String, String, Vec<ChildDef>, usize)> = vec![(
        base.clone(),
        root_qualified,
        root_from,
        children,
        1usize,
    )];

    while let Some((parent_name, _parent_qualified, parent_from, defs, depth)) = pop_front(&mut queue)
    {
        if depth > spec.max_nesting {
            continue;
        }
        for def in defs {
            let alias = format!("l{}", depth);
            let parent_alias = format!("l{}", depth - 1);
            let parent_id_expr = if depth == 1 {
                format!("{}.{}", parent_alias, quote_ident(ID_COL))
            } else {
                format!(
                    "({p}.{parent} || '/' || CAST({p}.{idx} AS VARCHAR))",
                    p = parent_alias,
                    parent = quote_ident(PARENT_COL),
                    idx = quote_ident(INDEX_COL)
                )
            };
            let from = format!(
                "(SELECT {pid} AS {parent}, generate_subscripts({e}, 1) - 1 AS {idx}, \
                 unnest({e}) AS v FROM {pf}) AS {alias}",
                pid = parent_id_expr,
                parent = quote_ident(PARENT_COL),
                idx = quote_ident(INDEX_COL),
                e = def.expr,
                pf = parent_from,
                alias = alias
            );

            let table_name = format!("{}__{}", parent_name, def.name);
            let qualified = qualify(&catalog, &schema, &table_name);

            let mut ccols = vec![
                PlanCol {
                    name: ID_COL.into(),
                    ty: "VARCHAR".into(),
                },
                PlanCol {
                    name: PARENT_COL.into(),
                    ty: "VARCHAR".into(),
                },
                PlanCol {
                    name: INDEX_COL.into(),
                    ty: "BIGINT".into(),
                },
                PlanCol {
                    name: LOAD_COL.into(),
                    ty: "VARCHAR".into(),
                },
            ];
            let mut cexprs = vec![
                format!(
                    "({a}.{parent} || '/' || CAST({a}.{idx} AS VARCHAR))",
                    a = alias,
                    parent = quote_ident(PARENT_COL),
                    idx = quote_ident(INDEX_COL)
                ),
                format!("{}.{}", alias, quote_ident(PARENT_COL)),
                format!("{}.{}", alias, quote_ident(INDEX_COL)),
                load_lit.clone(),
            ];
            let mut cused: Vec<String> = ccols.iter().map(|c| c.name.clone()).collect();
            let mut cchildren = Vec::new();

            // A struct element contributes its fields directly (`sku`), not
            // under a `value__` prefix — only scalar elements need the
            // synthetic `value` name.
            let elem_expr = format!("{}.v", alias);
            match &def.elem {
                Ty::Struct(fields) if !fields.is_empty() => {
                    for (fname, fty) in fields {
                        let fexpr =
                            format!("struct_extract({}, {})", elem_expr, sql_lit(fname));
                        collect(
                            &normalize(fname),
                            &fexpr,
                            fty,
                            &mut ccols,
                            &mut cexprs,
                            &mut cchildren,
                            &mut cused,
                            &mut warnings,
                        );
                    }
                }
                other => collect(
                    "value",
                    &elem_expr,
                    other,
                    &mut ccols,
                    &mut cexprs,
                    &mut cchildren,
                    &mut cused,
                    &mut warnings,
                ),
            }
            finish_children(
                &mut ccols,
                &mut cexprs,
                &mut cused,
                &cchildren,
                depth + 1,
                spec.max_nesting,
                &mut warnings,
            );

            tables.push(TablePlan {
                name: table_name.clone(),
                qualified: qualified.clone(),
                parent: Some(parent_name.clone()),
                depth,
                create_sql: create_sql(&qualified, &ccols),
                insert_sql: insert_sql(&qualified, &ccols, &cexprs, &from),
                columns: ccols,
            });

            if !cchildren.is_empty() {
                queue.push((table_name, qualified, from, cchildren, depth + 1));
            }
        }
    }

    Plan {
        catalog,
        schema,
        tables,
        warnings,
    }
}

fn pop_front<T>(v: &mut Vec<T>) -> Option<T> {
    if v.is_empty() {
        None
    } else {
        Some(v.remove(0))
    }
}

/// Walk one node of the type tree, appending columns to the current table and
/// recording any list columns for later child-table creation.
#[allow(clippy::too_many_arguments)]
fn collect(
    name: &str,
    expr: &str,
    ty: &Ty,
    cols: &mut Vec<PlanCol>,
    exprs: &mut Vec<String>,
    children: &mut Vec<ChildDef>,
    used: &mut Vec<String>,
    warnings: &mut Vec<String>,
) {
    match ty {
        Ty::Scalar(s) => {
            push_col(name, s, expr, cols, exprs, used, warnings);
        }
        Ty::Struct(fields) => {
            if fields.is_empty() {
                push_col(name, "VARCHAR", &to_json(expr), cols, exprs, used, warnings);
                return;
            }
            for (fname, fty) in fields {
                let child_name = format!("{}__{}", name, normalize(fname));
                let child_expr = format!("struct_extract({}, {})", expr, sql_lit(fname));
                collect(
                    &child_name,
                    &child_expr,
                    fty,
                    cols,
                    exprs,
                    children,
                    used,
                    warnings,
                );
            }
        }
        Ty::List(elem) => {
            children.push(ChildDef {
                name: name.to_string(),
                expr: expr.to_string(),
                elem: (**elem).clone(),
            });
        }
        Ty::Map(_, _) => {
            warnings.push(format!("{}: MAP kept as a JSON string column", name));
            push_col(name, "VARCHAR", &to_json(expr), cols, exprs, used, warnings);
        }
    }
}

/// Turn list columns that sit deeper than `max_nesting` into JSON columns on
/// the table that owns them.
fn finish_children(
    cols: &mut Vec<PlanCol>,
    exprs: &mut Vec<String>,
    used: &mut Vec<String>,
    children: &[ChildDef],
    child_depth: usize,
    max_nesting: usize,
    warnings: &mut Vec<String>,
) {
    if child_depth <= max_nesting {
        return;
    }
    for def in children {
        warnings.push(format!(
            "{}: nesting depth {} exceeds max_nesting {} — kept as a JSON string column",
            def.name, child_depth, max_nesting
        ));
        push_col(
            &def.name,
            "VARCHAR",
            &to_json(&def.expr),
            cols,
            exprs,
            used,
            warnings,
        );
    }
}

fn to_json(expr: &str) -> String {
    format!("CAST(to_json({}) AS VARCHAR)", expr)
}

fn push_col(
    name: &str,
    ty: &str,
    expr: &str,
    cols: &mut Vec<PlanCol>,
    exprs: &mut Vec<String>,
    used: &mut Vec<String>,
    warnings: &mut Vec<String>,
) {
    let final_name = dedupe(name, used, warnings);
    used.push(final_name.clone());
    cols.push(PlanCol {
        name: final_name,
        ty: ty.to_string(),
    });
    exprs.push(expr.to_string());
}

/// DuckDB identifiers are case-insensitive, so two source keys that differ only
/// in case would collide. Suffix the loser instead of silently dropping it.
fn dedupe(name: &str, used: &[String], warnings: &mut Vec<String>) -> String {
    let lower = name.to_ascii_lowercase();
    if !used.iter().any(|u| u.to_ascii_lowercase() == lower) {
        return name.to_string();
    }
    let mut n = 2;
    loop {
        let cand = format!("{}_{}", name, n);
        let cl = cand.to_ascii_lowercase();
        if !used.iter().any(|u| u.to_ascii_lowercase() == cl) {
            warnings.push(format!("column name collision: {} → {}", name, cand));
            return cand;
        }
        n += 1;
    }
}

pub fn normalize(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if c == '_' {
            out.push('_');
        } else if c.is_alphanumeric() {
            // Non-ASCII letters (e.g. Japanese keys) are legal identifiers in
            // DuckDB once quoted, so keep them rather than mangling to `_`.
            out.push(c);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push_str("col");
    }
    if out.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) {
        out.insert(0, '_');
    }
    if out.chars().count() > 100 {
        let head: String = out.chars().take(92).collect();
        out = format!("{}_{:08x}", head, fnv1a(&out));
    }
    out
}

fn fnv1a(s: &str) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for b in s.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h
}

/// `orders` / `raw.orders` / `lake.raw.orders` → (catalog, schema, table).
/// A three-part name is what points a load at an attached catalog such as a
/// DuckLake lakehouse.
fn split_target(target: &str) -> (Option<String>, Option<String>, String) {
    let t = target.trim().replace('"', "");
    let parts: Vec<&str> = t.split('.').filter(|p| !p.is_empty()).collect();
    match parts.len() {
        0 => (None, None, t),
        1 => (None, None, parts[0].to_string()),
        2 => (None, Some(parts[0].to_string()), parts[1].to_string()),
        _ => (
            Some(parts[0].to_string()),
            Some(parts[1].to_string()),
            parts[2..].join("."),
        ),
    }
}

fn qualify(catalog: &Option<String>, schema: &Option<String>, table: &str) -> String {
    match (catalog, schema) {
        (Some(c), Some(s)) => format!(
            "{}.{}.{}",
            quote_ident(c),
            quote_ident(s),
            quote_ident(table)
        ),
        (None, Some(s)) => format!("{}.{}", quote_ident(s), quote_ident(table)),
        _ => quote_ident(table),
    }
}

fn create_sql(qualified: &str, cols: &[PlanCol]) -> String {
    let defs: Vec<String> = cols
        .iter()
        .map(|c| format!("{} {}", quote_ident(&c.name), c.ty))
        .collect();
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        qualified,
        defs.join(", ")
    )
}

fn insert_sql(qualified: &str, cols: &[PlanCol], exprs: &[String], from: &str) -> String {
    let names: Vec<String> = cols.iter().map(|c| quote_ident(&c.name)).collect();
    let selects: Vec<String> = exprs
        .iter()
        .zip(cols.iter())
        .map(|(e, c)| format!("{} AS {}", e, quote_ident(&c.name)))
        .collect();
    format!(
        "INSERT INTO {} ({}) SELECT {} FROM {}",
        qualified,
        names.join(", "),
        selects.join(", "),
        from
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(columns: Vec<(&str, &str)>) -> PlanSpec {
        PlanSpec {
            target: "raw.orders".into(),
            source_relation: "__orch_ingest_src".into(),
            load_id: "L1".into(),
            row_key: "__orch_rn".into(),
            max_nesting: 3,
            columns: columns
                .into_iter()
                .map(|(n, t)| ColSpec {
                    name: n.into(),
                    ty: t.into(),
                })
                .collect(),
        }
    }

    #[test]
    fn flat_source_makes_one_table() {
        let p = build_plan(&spec(vec![("id", "BIGINT"), ("name", "VARCHAR")]));
        assert_eq!(p.tables.len(), 1);
        let names: Vec<&str> = p.tables[0].columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["_orch_id", "_orch_load_id", "id", "name"]);
        assert_eq!(p.tables[0].qualified, "\"raw\".\"orders\"");
    }

    #[test]
    fn struct_is_flattened_into_the_parent() {
        let p = build_plan(&spec(vec![(
            "customer",
            "STRUCT(\"name\" VARCHAR, city VARCHAR)",
        )]));
        assert_eq!(p.tables.len(), 1);
        let names: Vec<&str> = p.tables[0].columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"customer__name"));
        assert!(names.contains(&"customer__city"));
        assert!(p.tables[0].insert_sql.contains("struct_extract"));
    }

    #[test]
    fn list_of_struct_becomes_a_child_table() {
        let p = build_plan(&spec(vec![
            ("id", "BIGINT"),
            ("items", "STRUCT(sku VARCHAR, qty BIGINT)[]"),
        ]));
        assert_eq!(p.tables.len(), 2);
        let child = &p.tables[1];
        assert_eq!(child.name, "orders__items");
        assert_eq!(child.parent.as_deref(), Some("orders"));
        assert_eq!(child.depth, 1);
        let names: Vec<&str> = child.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "_orch_id",
                "_orch_parent_id",
                "_orch_index",
                "_orch_load_id",
                "sku",
                "qty"
            ]
        );
        assert!(child.insert_sql.contains("generate_subscripts"));
        assert!(child.insert_sql.contains("unnest"));
    }

    #[test]
    fn list_of_scalar_gets_a_value_column() {
        let p = build_plan(&spec(vec![("tags", "VARCHAR[]")]));
        assert_eq!(p.tables.len(), 2);
        let names: Vec<&str> = p.tables[1].columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"value"));
    }

    #[test]
    fn nesting_is_capped() {
        let mut s = spec(vec![("items", "STRUCT(tags VARCHAR[])[]")]);
        s.max_nesting = 1;
        let p = build_plan(&s);
        // items becomes a table; tags is past the cap so it stays JSON text.
        assert_eq!(p.tables.len(), 2);
        let child = &p.tables[1];
        let tags = child
            .columns
            .iter()
            .find(|c| c.name == "tags")
            .expect("tags column kept on the child table");
        assert_eq!(tags.ty, "VARCHAR");
        assert!(child.insert_sql.contains("to_json"));
        assert!(p.warnings.iter().any(|w| w.contains("max_nesting")));
    }

    #[test]
    fn deep_nesting_within_the_cap_makes_a_grandchild() {
        let p = build_plan(&spec(vec![("items", "STRUCT(tags VARCHAR[])[]")]));
        let names: Vec<&str> = p.tables.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["orders", "orders__items", "orders__items__tags"]
        );
        assert_eq!(p.tables[2].depth, 2);
    }

    #[test]
    fn unqualified_target_has_no_schema() {
        let mut s = spec(vec![("id", "BIGINT")]);
        s.target = "orders".into();
        let p = build_plan(&s);
        assert_eq!(p.schema, None);
        assert_eq!(p.catalog, None);
        assert_eq!(p.tables[0].qualified, "\"orders\"");
    }

    #[test]
    fn a_three_part_target_names_a_catalog() {
        let mut s = spec(vec![("id", "BIGINT"), ("tags", "VARCHAR[]")]);
        s.target = "lake.raw.orders".into();
        let p = build_plan(&s);
        assert_eq!(p.catalog.as_deref(), Some("lake"));
        assert_eq!(p.schema.as_deref(), Some("raw"));
        assert_eq!(p.tables[0].qualified, "\"lake\".\"raw\".\"orders\"");
        assert_eq!(p.tables[1].qualified, "\"lake\".\"raw\".\"orders__tags\"");
    }

    #[test]
    fn case_only_collisions_are_suffixed() {
        let p = build_plan(&spec(vec![("Id", "BIGINT"), ("id", "VARCHAR")]));
        let names: Vec<&str> = p.tables[0].columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"id_2"));
        assert!(p.warnings.iter().any(|w| w.contains("collision")));
    }

    #[test]
    fn quotes_in_identifiers_are_escaped() {
        let p = build_plan(&spec(vec![("we\"ird", "BIGINT")]));
        // Normalization turns the quote into `_`, so nothing can break out.
        assert!(p.tables[0].create_sql.contains("we_ird"));
        assert!(!p.tables[0].create_sql.contains("we\"ird"));
    }

    #[test]
    fn row_key_is_not_ingested_as_data() {
        let p = build_plan(&spec(vec![("id", "BIGINT"), ("__orch_rn", "BIGINT")]));
        let names: Vec<&str> = p.tables[0].columns.iter().map(|c| c.name.as_str()).collect();
        assert!(!names.contains(&"__orch_rn"));
    }
}
