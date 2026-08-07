// DuckDB type-string parsing.
//
// P0 ingestion never inspects the raw JSON itself — it asks DuckDB to infer
// the shape (`read_json` + `DESCRIBE`) and then works purely off the type
// strings that come back. That keeps type inference in one place (DuckDB's)
// and reduces the Rust side to a string-to-tree parse plus SQL generation.
//
// Grammar handled here:
//   STRUCT("a" BIGINT, b VARCHAR)   nested struct
//   <type>[]                        list
//   <type>[3]                       fixed-size array (treated as list)
//   MAP(VARCHAR, JSON)              map
//   anything else                   scalar, kept verbatim (upper-cased)

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ty {
    Scalar(String),
    Struct(Vec<(String, Ty)>),
    List(Box<Ty>),
    Map(Box<Ty>, Box<Ty>),
}

impl Ty {
    /// Render back to a DuckDB type string. Used for DDL and for the schema
    /// ledger, so it must round-trip with `parse`.
    pub fn render(&self) -> String {
        match self {
            Ty::Scalar(s) => s.clone(),
            Ty::List(inner) => format!("{}[]", inner.render()),
            Ty::Map(k, v) => format!("MAP({}, {})", k.render(), v.render()),
            Ty::Struct(fields) => {
                let inner: Vec<String> = fields
                    .iter()
                    .map(|(n, t)| format!("{} {}", quote_ident(n), t.render()))
                    .collect();
                format!("STRUCT({})", inner.join(", "))
            }
        }
    }
}

pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

pub fn sql_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

pub fn parse(s: &str) -> Ty {
    let t = s.trim();
    if t.is_empty() {
        return Ty::Scalar("VARCHAR".to_string());
    }

    // Trailing [] / [N] — only when the bracket sits at the very end and the
    // prefix is balanced, so `STRUCT(a VARCHAR[])` is not mistaken for a list.
    if t.ends_with(']') {
        if let Some(open) = matching_bracket_open(t) {
            let inner = &t[..open];
            if is_balanced(inner) {
                return Ty::List(Box::new(parse(inner)));
            }
        }
    }

    let upper = t.to_ascii_uppercase();
    if upper.starts_with("STRUCT(") && t.ends_with(')') {
        let body = &t[7..t.len() - 1];
        let mut fields = Vec::new();
        for part in split_top_level(body) {
            if let Some((name, ty)) = split_field(&part) {
                fields.push((name, parse(&ty)));
            }
        }
        return Ty::Struct(fields);
    }
    if upper.starts_with("MAP(") && t.ends_with(')') {
        let body = &t[4..t.len() - 1];
        let parts = split_top_level(body);
        if parts.len() == 2 {
            return Ty::Map(Box::new(parse(&parts[0])), Box::new(parse(&parts[1])));
        }
    }

    Ty::Scalar(upper)
}

/// Index of the `[` matching a trailing `]`, if the bracket pair is the last
/// thing in the string.
fn matching_bracket_open(t: &str) -> Option<usize> {
    let bytes = t.as_bytes();
    let mut i = bytes.len() - 1; // on ']'
    let mut depth = 0usize;
    loop {
        match bytes[i] {
            b']' => depth += 1,
            b'[' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        if i == 0 {
            return None;
        }
        i -= 1;
    }
}

fn is_balanced(s: &str) -> bool {
    let mut paren = 0i32;
    let mut bracket = 0i32;
    let mut in_quote = false;
    for c in s.chars() {
        match c {
            '"' => in_quote = !in_quote,
            '(' if !in_quote => paren += 1,
            ')' if !in_quote => paren -= 1,
            '[' if !in_quote => bracket += 1,
            ']' if !in_quote => bracket -= 1,
            _ => {}
        }
        if paren < 0 || bracket < 0 {
            return false;
        }
    }
    paren == 0 && bracket == 0 && !in_quote
}

/// Split on commas that sit outside quotes, parens and brackets.
fn split_top_level(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut paren = 0i32;
    let mut bracket = 0i32;
    let mut in_quote = false;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                // "" inside a quoted identifier is an escaped quote.
                if in_quote && chars.peek() == Some(&'"') {
                    cur.push('"');
                    cur.push('"');
                    chars.next();
                    continue;
                }
                in_quote = !in_quote;
                cur.push(c);
            }
            '(' if !in_quote => {
                paren += 1;
                cur.push(c);
            }
            ')' if !in_quote => {
                paren -= 1;
                cur.push(c);
            }
            '[' if !in_quote => {
                bracket += 1;
                cur.push(c);
            }
            ']' if !in_quote => {
                bracket -= 1;
                cur.push(c);
            }
            ',' if !in_quote && paren == 0 && bracket == 0 => {
                out.push(cur.trim().to_string());
                cur = String::new();
            }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

/// `"a b" VARCHAR` / `sku VARCHAR` → (name, type).
fn split_field(s: &str) -> Option<(String, String)> {
    let t = s.trim();
    if t.is_empty() {
        return None;
    }
    if let Some(rest) = t.strip_prefix('"') {
        let mut name = String::new();
        let mut chars = rest.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    name.push('"');
                    chars.next();
                    continue;
                }
                let remainder: String = chars.collect();
                return Some((name, remainder.trim().to_string()));
            }
            name.push(c);
        }
        return None;
    }
    let idx = t.find(char::is_whitespace)?;
    Some((t[..idx].to_string(), t[idx..].trim().to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar() {
        assert_eq!(parse("BIGINT"), Ty::Scalar("BIGINT".into()));
        assert_eq!(parse("varchar"), Ty::Scalar("VARCHAR".into()));
    }

    #[test]
    fn list_of_scalar() {
        assert_eq!(
            parse("VARCHAR[]"),
            Ty::List(Box::new(Ty::Scalar("VARCHAR".into())))
        );
    }

    #[test]
    fn struct_with_quoted_field() {
        let t = parse("STRUCT(\"name\" VARCHAR, city VARCHAR)");
        match t {
            Ty::Struct(f) => {
                assert_eq!(f.len(), 2);
                assert_eq!(f[0].0, "name");
                assert_eq!(f[1].0, "city");
            }
            other => panic!("expected struct, got {:?}", other),
        }
    }

    #[test]
    fn list_of_struct_with_inner_list() {
        // The exact shape DESCRIBE returns for the nested-orders fixture.
        let t = parse("STRUCT(sku VARCHAR, qty BIGINT, tags VARCHAR[])[]");
        let inner = match t {
            Ty::List(i) => *i,
            other => panic!("expected list, got {:?}", other),
        };
        match inner {
            Ty::Struct(f) => {
                assert_eq!(f.len(), 3);
                assert_eq!(f[2].1, Ty::List(Box::new(Ty::Scalar("VARCHAR".into()))));
            }
            other => panic!("expected struct, got {:?}", other),
        }
    }

    #[test]
    fn struct_field_holding_a_list_is_not_read_as_a_list() {
        // The trailing `]` belongs to the inner field, not to the struct.
        assert!(matches!(parse("STRUCT(a VARCHAR[], b BIGINT)"), Ty::Struct(_)));
    }

    #[test]
    fn map_type() {
        assert_eq!(
            parse("MAP(VARCHAR, BIGINT)"),
            Ty::Map(
                Box::new(Ty::Scalar("VARCHAR".into())),
                Box::new(Ty::Scalar("BIGINT".into()))
            )
        );
    }

    #[test]
    fn render_round_trips() {
        let src = "STRUCT(\"name\" VARCHAR, tags VARCHAR[])[]";
        assert_eq!(parse(&parse(src).render()), parse(src));
    }

    #[test]
    fn fixed_size_array_is_a_list() {
        assert_eq!(
            parse("INTEGER[3]"),
            Ty::List(Box::new(Ty::Scalar("INTEGER".into())))
        );
    }
}
