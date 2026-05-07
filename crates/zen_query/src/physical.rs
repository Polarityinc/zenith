//! Physical plan operators. The current set covers:
//!  - SegmentScan with late materialization
//!  - WalScan (memtable + WAL files)
//!  - RoaringIntersect (filter combination)
//!  - FtsSearch (Tantivy hits → row mask)
//!  - JsonPathFilter (indexed posting lookup or scan fallback)
//!  - Limit / OrderBy / Project
//!  - Aggregate (count / sum / avg / min / max / percentile)
//!
//! Operators are not pull-based iterators here; the executor drives them
//! sequentially with materialized intermediate results. That's simpler and
//! plenty fast for the cardinalities we hit on the AI-trace workload.

use crate::logical::{AggregateFn, LogicalPlan};

pub struct PhysicalPlan {
    pub logical: LogicalPlan,
}

impl PhysicalPlan {
    pub fn from_logical(logical: LogicalPlan) -> Self {
        Self { logical }
    }
}

#[derive(Clone, Debug)]
pub enum AggResult {
    Int(i64),
    Float(f64),
    String(String),
    Null,
}

pub fn aggregate_label(name: &str, agg: &AggregateFn) -> String {
    match agg {
        AggregateFn::Count => "count".into(),
        AggregateFn::Sum(c) => format!("sum_{c}"),
        AggregateFn::Avg(c) => format!("avg_{c}"),
        AggregateFn::Min(c) => format!("min_{c}"),
        AggregateFn::Max(c) => format!("max_{c}"),
        AggregateFn::Percentile { column: c, q } => {
            let _ = name;
            format!("p{}_{}", (*q * 100.0).round() as u32, c)
        }
    }
}
