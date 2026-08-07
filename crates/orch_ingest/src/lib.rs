// orch_ingest — JSON ingestion (Phase 19).
//
// Layers:
//   typ     DuckDB type strings → a type tree
//   shape   type tree → parent/child tables and the SQL that fills them
//   ledger  what changed between the stored table and the incoming shape
//   write   append / replace / merge, expressed as post-load pruning
//   source  HTTP fetching with pagination, resume cursors and a page ceiling
//
// Everything except `source` is pure: it takes the column types DuckDB
// inferred and returns SQL text plus a description of what changed. Every
// statement is executed by the C++ side, which owns the connection.

pub mod ledger;
pub mod shape;
pub mod source;
pub mod typ;
pub mod write;

pub use ledger::{diff, DiffOut, DiffSpec};
pub use shape::{build_plan, Plan, PlanSpec};
pub use source::{fetch, FetchOut, FetchSpec};
pub use write::{prune, split_key, Disposition, PruneOut, PruneSpec};
