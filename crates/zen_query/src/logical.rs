//! Logical plan and helper structs.

use serde::{Deserialize, Serialize};

use crate::expr::Expr;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Projection {
    /// `None` means SELECT *.
    pub columns: Option<Vec<String>>,
}

impl Projection {
    pub fn star() -> Self {
        Self { columns: None }
    }
    pub fn list<I: IntoIterator<Item = String>>(it: I) -> Self {
        Self {
            columns: Some(it.into_iter().collect()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Predicate {
    pub expr: Expr,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AggregateFn {
    Count,
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
    Percentile { column: String, q: f64 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogicalPlan {
    /// Tenant id we're querying for. Set by the gateway, not by the user.
    pub tenant_id: u64,
    pub partition_ids: Vec<u32>,

    /// SELECT projection.
    pub projection: Projection,

    /// WHERE predicate. None means no filter.
    pub predicate: Option<Predicate>,

    /// Time range filter — `[time_min_ms, time_max_ms]`. Always present.
    pub time_min_ms: i64,
    pub time_max_ms: i64,

    /// Optional ORDER BY column + ascending flag.
    pub order_by: Option<(String, bool)>,
    /// LIMIT.
    pub limit: Option<u32>,

    /// GROUP BY columns. Empty means no grouping.
    pub group_by: Vec<String>,

    /// Aggregations to compute. Empty means no aggregation.
    pub aggregates: Vec<(String, AggregateFn)>,
}

impl Default for LogicalPlan {
    fn default() -> Self {
        Self {
            tenant_id: 0,
            partition_ids: vec![0],
            projection: Projection::star(),
            predicate: None,
            time_min_ms: i64::MIN,
            time_max_ms: i64::MAX,
            order_by: None,
            limit: None,
            group_by: vec![],
            aggregates: vec![],
        }
    }
}
