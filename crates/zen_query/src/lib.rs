//! Query engine: parses logical plans, optimizes, and executes against a
//! catalog + storage stack with late materialization.

pub mod cache;
pub mod executor;
pub mod expr;
pub mod logical;
pub mod physical;
pub mod row;
pub mod segment_cache;
pub mod segment_list_cache;

pub use cache::ResultCache;
pub use executor::{execute, execute_full, execute_with_cache};
pub use expr::{Expr, Literal};
pub use logical::{LogicalPlan, Predicate, Projection};
pub use physical::PhysicalPlan;
pub use row::{ResultRow, ResultSet};
pub use segment_cache::SegmentCache;
pub use segment_list_cache::SegmentListCache;
