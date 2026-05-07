//! Query frontends.
//!
//! `parse_sql` is the supported entrypoint. ZenithQL is a tiny extension —
//! `text_match(col, "...")` is recognized as an FTS predicate, and JSON path
//! comparisons like `metadata.foo = 'bar'` are mapped to JsonPathEq.

pub mod sql;

pub use sql::parse_sql;

use zen_common::ZenError;
use zen_query::LogicalPlan;

/// Parse a query string. Detects ZenithQL syntactic sugars and falls through
/// to the SQL parser.
pub fn parse(query: &str, tenant_id: u64) -> Result<LogicalPlan, ZenError> {
    parse_sql(query, tenant_id)
}
