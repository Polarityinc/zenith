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

#[cfg(test)]
mod parse_proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// The parser must NEVER panic on arbitrary input. Random ASCII
        /// strings should produce either `Ok(plan)` or `Err(ZenError)`
        /// — never trigger a `panic!` or `unwrap` on `None`. Catches
        /// reachable panics in `unwrap_or_else`, integer parsing, slice
        /// indexing, etc.
        #[test]
        fn random_input_never_panics(s in r#"[ -~\t\n]{0,200}"#) {
            let _ = parse(&s, 0);
        }

        /// Well-formed `SELECT a FROM spans WHERE x = N` queries with
        /// random column names + literals must always parse.
        #[test]
        fn select_eq_int_always_parses(
            col in "[a-z][a-z0-9_]{0,12}",
            val in 0i64..1_000_000,
        ) {
            let q = format!("SELECT {col} FROM spans WHERE {col} = {val}");
            prop_assert!(parse(&q, 1).is_ok(), "failed: {q}");
        }
    }
}
