// DuckDB-native `$name` parameter binding support.
//
// Phase 12 foundation: parses `-- @param name:TYPE` header declarations into
// `ParamSpec` records and provides a `BoundParams` container that pairs each
// spec with a string-form value. Actual `bind_*` wiring to DuckDB statements
// is deferred to Phase 13/14.
//
// NOTE: This module deliberately does NOT extend the legacy Jinja-style
// `{{ var }}` substitution in `templating.rs`. All new variable plumbing
// flows through `$name` parameters that DuckDB itself binds.

pub use orch_common::{ParamSpec, ParamType};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingError {
    /// Header value was missing the `:` separator between name and type.
    MissingColon(String),
    /// Parameter name part was empty.
    EmptyName(String),
    /// Type token did not match any supported `ParamType`.
    UnknownType { name: String, ty: String },
}

impl std::fmt::Display for BindingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BindingError::MissingColon(s) => {
                write!(f, "@param expects `name:TYPE`, got: {}", s)
            }
            BindingError::EmptyName(s) => {
                write!(f, "@param has empty name: {}", s)
            }
            BindingError::UnknownType { name, ty } => {
                write!(f, "@param {}: unknown type `{}`", name, ty)
            }
        }
    }
}

impl std::error::Error for BindingError {}

/// Parse a single `-- @param` header value such as `partition_key:DATE`
/// into a `ParamSpec`. Whitespace around name and type is tolerated; the
/// type token is case-insensitive.
pub fn parse_param_decl(s: &str) -> Result<ParamSpec, BindingError> {
    let trimmed = s.trim();
    let (raw_name, raw_ty) = trimmed
        .split_once(':')
        .ok_or_else(|| BindingError::MissingColon(trimmed.to_string()))?;

    let name = raw_name.trim().to_string();
    if name.is_empty() {
        return Err(BindingError::EmptyName(trimmed.to_string()));
    }

    let ty_token = raw_ty.trim();
    let ty = match ty_token.to_ascii_uppercase().as_str() {
        "VARCHAR" | "STRING" | "TEXT" => ParamType::Varchar,
        "INT" | "INTEGER" | "INT4" => ParamType::Integer,
        "BIGINT" | "INT8" | "LONG" => ParamType::BigInt,
        "DATE" => ParamType::Date,
        "TIMESTAMP" | "DATETIME" => ParamType::Timestamp,
        "BOOL" | "BOOLEAN" => ParamType::Boolean,
        "DOUBLE" | "FLOAT" | "FLOAT8" => ParamType::Double,
        _ => {
            return Err(BindingError::UnknownType {
                name,
                ty: ty_token.to_string(),
            });
        }
    };

    Ok(ParamSpec { name, ty })
}

/// Pairs each declared `ParamSpec` with its caller-supplied value (kept as a
/// string; type conversion is performed at bind time in Phase 13/14).
#[derive(Debug, Clone, Default)]
pub struct BoundParams {
    pub entries: Vec<(ParamSpec, String)>,
}

impl BoundParams {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, spec: ParamSpec, value: impl Into<String>) {
        self.entries.push((spec, value.into()));
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_date_decl() {
        let spec = parse_param_decl("partition_key:DATE").unwrap();
        assert_eq!(spec.name, "partition_key");
        assert_eq!(spec.ty, ParamType::Date);
    }

    #[test]
    fn parses_int_alias_case_insensitive() {
        let spec = parse_param_decl("  count : int  ").unwrap();
        assert_eq!(spec.name, "count");
        assert_eq!(spec.ty, ParamType::Integer);
    }

    #[test]
    fn parses_all_supported_types() {
        let cases = [
            ("a:VARCHAR", ParamType::Varchar),
            ("a:STRING", ParamType::Varchar),
            ("a:INTEGER", ParamType::Integer),
            ("a:BIGINT", ParamType::BigInt),
            ("a:DATE", ParamType::Date),
            ("a:TIMESTAMP", ParamType::Timestamp),
            ("a:BOOLEAN", ParamType::Boolean),
            ("a:DOUBLE", ParamType::Double),
        ];
        for (input, expected) in cases {
            let spec = parse_param_decl(input).unwrap();
            assert_eq!(spec.ty, expected, "input was {}", input);
        }
    }

    #[test]
    fn rejects_missing_colon() {
        let err = parse_param_decl("partition_key DATE").unwrap_err();
        assert!(matches!(err, BindingError::MissingColon(_)));
    }

    #[test]
    fn rejects_unknown_type() {
        let err = parse_param_decl("k:UUID").unwrap_err();
        match err {
            BindingError::UnknownType { name, ty } => {
                assert_eq!(name, "k");
                assert_eq!(ty, "UUID");
            }
            other => panic!("expected UnknownType, got {:?}", other),
        }
    }

    #[test]
    fn rejects_empty_name() {
        let err = parse_param_decl(":DATE").unwrap_err();
        assert!(matches!(err, BindingError::EmptyName(_)));
    }

    #[test]
    fn bound_params_push_and_len() {
        let spec = parse_param_decl("d:DATE").unwrap();
        let mut bp = BoundParams::new();
        assert!(bp.is_empty());
        bp.push(spec.clone(), "2026-01-01");
        assert_eq!(bp.len(), 1);
        assert_eq!(bp.entries[0].0, spec);
        assert_eq!(bp.entries[0].1, "2026-01-01");
    }
}
