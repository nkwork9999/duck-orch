// Schema diff: what changed between the table as it exists and the shape the
// incoming data wants, plus the DDL that absorbs the difference.
//
// P0 contract is deliberately two-valued: absorb it, or stop. Added columns and
// widening type changes are absorbed; anything else fails the load with a
// message naming the column.

use serde::{Deserialize, Serialize};

use crate::shape::PlanCol;
use crate::typ::quote_ident;

#[derive(Debug, Clone, Deserialize)]
pub struct DiffSpec {
    /// Qualified, quoted table name — used verbatim in the generated DDL.
    pub table: String,
    pub old: Vec<PlanCol>,
    pub new: Vec<PlanCol>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Change {
    pub kind: String,
    pub column: String,
    pub from: String,
    pub to: String,
    pub ddl: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiffOut {
    pub changes: Vec<Change>,
    pub errors: Vec<String>,
}

pub fn diff(spec: &DiffSpec) -> DiffOut {
    let mut changes = Vec::new();
    let mut errors = Vec::new();

    for n in &spec.new {
        match find(&spec.old, &n.name) {
            None => changes.push(Change {
                kind: "column_added".into(),
                column: n.name.clone(),
                from: String::new(),
                to: n.ty.clone(),
                ddl: format!(
                    "ALTER TABLE {} ADD COLUMN {} {}",
                    spec.table,
                    quote_ident(&n.name),
                    n.ty
                ),
            }),
            Some(o) => match widen(&o.ty, &n.ty) {
                Widen::Same | Widen::AlreadyWider => {}
                Widen::To(t) => changes.push(Change {
                    kind: "type_widened".into(),
                    column: n.name.clone(),
                    from: o.ty.clone(),
                    to: t.clone(),
                    ddl: format!(
                        "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                        spec.table,
                        quote_ident(&n.name),
                        t
                    ),
                }),
                Widen::Incompatible => errors.push(format!(
                    "{}.{}: incompatible type change {} → {}",
                    spec.table, n.name, o.ty, n.ty
                )),
            },
        }
    }

    // Columns that vanished from the source stay in the table and take NULLs.
    for o in &spec.old {
        if find(&spec.new, &o.name).is_none() {
            changes.push(Change {
                kind: "column_removed".into(),
                column: o.name.clone(),
                from: o.ty.clone(),
                to: String::new(),
                ddl: String::new(),
            });
        }
    }

    DiffOut { changes, errors }
}

fn find<'a>(cols: &'a [PlanCol], name: &str) -> Option<&'a PlanCol> {
    cols.iter()
        .find(|c| c.name.eq_ignore_ascii_case(name))
}

enum Widen {
    Same,
    AlreadyWider,
    To(String),
    Incompatible,
}

fn widen(old: &str, new: &str) -> Widen {
    let o = old.trim().to_ascii_uppercase();
    let n = new.trim().to_ascii_uppercase();
    if o == n {
        return Widen::Same;
    }
    if o == "VARCHAR" {
        // Text already holds anything.
        return Widen::AlreadyWider;
    }
    if n == "VARCHAR" {
        return Widen::To("VARCHAR".into());
    }
    match (rank(&o), rank(&n)) {
        (Some(a), Some(b)) if b > a => Widen::To(n),
        (Some(a), Some(b)) if b <= a => Widen::AlreadyWider,
        _ => Widen::Incompatible,
    }
}

fn rank(t: &str) -> Option<u8> {
    if t.starts_with("DECIMAL") {
        return Some(7);
    }
    Some(match t {
        "BOOLEAN" => 1,
        "TINYINT" | "UTINYINT" => 2,
        "SMALLINT" | "USMALLINT" => 3,
        "INTEGER" | "UINTEGER" => 4,
        "BIGINT" | "UBIGINT" => 5,
        "HUGEINT" | "UHUGEINT" => 6,
        "FLOAT" => 8,
        "DOUBLE" => 9,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(n: &str, t: &str) -> PlanCol {
        PlanCol {
            name: n.into(),
            ty: t.into(),
        }
    }

    fn run(old: Vec<PlanCol>, new: Vec<PlanCol>) -> DiffOut {
        diff(&DiffSpec {
            table: "\"raw\".\"orders\"".into(),
            old,
            new,
        })
    }

    #[test]
    fn identical_schemas_produce_nothing() {
        let d = run(vec![col("id", "BIGINT")], vec![col("id", "BIGINT")]);
        assert!(d.changes.is_empty());
        assert!(d.errors.is_empty());
    }

    #[test]
    fn new_column_is_absorbed() {
        let d = run(
            vec![col("id", "BIGINT")],
            vec![col("id", "BIGINT"), col("tier", "VARCHAR")],
        );
        assert_eq!(d.changes.len(), 1);
        assert_eq!(d.changes[0].kind, "column_added");
        assert_eq!(
            d.changes[0].ddl,
            "ALTER TABLE \"raw\".\"orders\" ADD COLUMN \"tier\" VARCHAR"
        );
        assert!(d.errors.is_empty());
    }

    #[test]
    fn integer_widens_to_bigint() {
        let d = run(vec![col("n", "INTEGER")], vec![col("n", "BIGINT")]);
        assert_eq!(d.changes[0].kind, "type_widened");
        assert!(d.changes[0].ddl.contains("ALTER COLUMN \"n\" TYPE BIGINT"));
    }

    #[test]
    fn narrowing_is_left_alone() {
        let d = run(vec![col("n", "BIGINT")], vec![col("n", "INTEGER")]);
        assert!(d.changes.is_empty());
        assert!(d.errors.is_empty());
    }

    #[test]
    fn anything_may_widen_to_varchar() {
        let d = run(vec![col("n", "BIGINT")], vec![col("n", "VARCHAR")]);
        assert_eq!(d.changes[0].to, "VARCHAR");
    }

    #[test]
    fn incompatible_change_is_an_error() {
        let d = run(vec![col("t", "DATE")], vec![col("t", "BIGINT")]);
        assert!(d.changes.is_empty());
        assert_eq!(d.errors.len(), 1);
        assert!(d.errors[0].contains("incompatible"));
    }

    #[test]
    fn missing_column_is_recorded_but_not_dropped() {
        let d = run(
            vec![col("id", "BIGINT"), col("gone", "VARCHAR")],
            vec![col("id", "BIGINT")],
        );
        assert_eq!(d.changes.len(), 1);
        assert_eq!(d.changes[0].kind, "column_removed");
        assert!(d.changes[0].ddl.is_empty());
    }

    #[test]
    fn column_lookup_ignores_case() {
        let d = run(vec![col("Id", "BIGINT")], vec![col("id", "BIGINT")]);
        assert!(d.changes.is_empty());
    }
}
