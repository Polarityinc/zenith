//! Query engine: parses logical plans, optimizes, and executes against a
//! catalog + storage stack with late materialization.

pub mod expr;
pub mod logical;
pub mod physical;
pub mod executor;
pub mod cache;
pub mod row;
pub mod segment_cache;
pub mod segment_list_cache;

pub use expr::{Expr, Literal};
pub use logical::{LogicalPlan, Predicate, Projection};
pub use physical::PhysicalPlan;
pub use executor::{execute, execute_with_cache, execute_full};
pub use cache::ResultCache;
pub use row::{ResultRow, ResultSet};
pub use segment_cache::SegmentCache;
pub use segment_list_cache::SegmentListCache;
