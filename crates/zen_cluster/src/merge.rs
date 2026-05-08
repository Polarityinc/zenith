//! Combine partial `ResultSet`s from remote workers into one final result.
//!
//! v1 is intentionally simple: concatenate rows up to `limit`, sum the
//! stats. This is correct for tenant-scoped queries (the common case),
//! since each tenant lives on one primary replica and so there's only
//! one partial. For cross-shard fan-out with `GROUP BY`, the planner
//! emits *partial* aggregations on workers and a final aggregation on
//! the coordinator — that path lives in the planner, not here.

use zen_query::row::ResultStats;
use zen_query::ResultSet;

pub fn merge_result_sets(parts: Vec<ResultSet>, limit: Option<usize>) -> ResultSet {
    if parts.is_empty() {
        return ResultSet::default();
    }
    let mut iter = parts.into_iter();
    let mut out = iter.next().unwrap();
    for r in iter {
        out.rows.extend(r.rows);
        // Union of columns; preserve first-seen order, append new ones.
        for c in r.columns {
            if !out.columns.contains(&c) {
                out.columns.push(c);
            }
        }
        // Sum stats.
        out.stats = ResultStats {
            segments_scanned: out.stats.segments_scanned + r.stats.segments_scanned,
            row_groups_pruned: out.stats.row_groups_pruned + r.stats.row_groups_pruned,
            row_groups_scanned: out.stats.row_groups_scanned + r.stats.row_groups_scanned,
            rows_returned: out.stats.rows_returned + r.stats.rows_returned,
            elapsed_ms: out.stats.elapsed_ms.max(r.stats.elapsed_ms),
            bytes_decoded_wide: out.stats.bytes_decoded_wide + r.stats.bytes_decoded_wide,
        };
    }
    if let Some(n) = limit {
        if out.rows.len() > n {
            out.rows.truncate(n);
            out.stats.rows_returned = n as u32;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn rs(rows: usize, scanned: u32) -> ResultSet {
        let mut out = ResultSet {
            columns: vec!["x".into()],
            rows: (0..rows)
                .map(|i| {
                    let mut f = BTreeMap::new();
                    f.insert("x".into(), serde_json::json!(i));
                    zen_query::ResultRow { fields: f }
                })
                .collect(),
            ..Default::default()
        };
        out.stats.rows_returned = rows as u32;
        out.stats.segments_scanned = scanned;
        out
    }

    #[test]
    fn merge_concats_and_sums() {
        let merged = merge_result_sets(vec![rs(3, 1), rs(2, 2), rs(5, 4)], None);
        assert_eq!(merged.rows.len(), 10);
        assert_eq!(merged.stats.segments_scanned, 7);
        assert_eq!(merged.stats.rows_returned, 10);
    }

    #[test]
    fn merge_respects_limit() {
        let merged = merge_result_sets(vec![rs(3, 1), rs(5, 1)], Some(4));
        assert_eq!(merged.rows.len(), 4);
        assert_eq!(merged.stats.rows_returned, 4);
    }

    #[test]
    fn merge_empty_input() {
        let merged = merge_result_sets(Vec::new(), Some(10));
        assert_eq!(merged.rows.len(), 0);
    }
}
