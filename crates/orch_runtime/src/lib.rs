// Task file parsing + Jinja-style placeholder substitution.

pub mod parser;
pub mod templating;

pub use parser::{parse_sql_file, ParseError};
pub use templating::substitute;
