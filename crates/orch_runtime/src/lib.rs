// Task file parsing + Jinja-style placeholder substitution + DuckDB-native
// `$name` parameter binding (Phase 12 foundation).

pub mod binding;
pub mod calendar;
pub mod parser;
pub mod templating;

pub use binding::{parse_param_decl, BindingError, BoundParams};
pub use calendar::{render_calendar, CellKind, PartitionStatus};
pub use parser::{parse_sql_file, ParseError};
pub use templating::substitute;
