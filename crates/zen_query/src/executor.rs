//! Query executor.
//!
//! Implements the late-materialization scan loop:
//! 1. Use catalog to pick relevant segments.
//! 2. For each segment, prune row groups via metadata.
//! 3. For each surviving row group:
//!    a. Apply equality predicates against decoded "thin" columns.
//!    b. Apply FTS predicate (if any) → row mask.
//!    c. Apply JSON-path predicate (if any).
//!    d. Late-materialize wide columns ONLY for surviving rows.
//! 4. Merge with WAL/memtable scans.
//! 5. Apply ORDER BY, LIMIT, GROUP BY, projection.

// Hot-path mask scans index a freshly-built `Vec<bool>` by integer
// position; clippy suggests `iter().enumerate()` but that materializes
// `&bool` references that the optimizer fights with. The current
// indexed form generates the tightest scalar code on aarch64 — see
// the durability_bench comparison. Keeping the suppression localized.
#![allow(clippy::needless_range_loop)]

/// Hard upper bound for any user-supplied LIMIT. A query like
/// `SELECT * FROM spans LIMIT 1_000_000_000` would otherwise tie up
/// the executor scanning all segments before it could even check the
/// LIMIT — a trivial DoS for any tenant. Honest queries fit well
/// inside this; analytical paging should use OFFSET-style cursors.
pub const MAX_QUERY_LIMIT: u32 = 100_000;

/// Clamp a parsed LIMIT to [`MAX_QUERY_LIMIT`].
fn clamp_limit(limit: Option<u32>) -> Option<u32> {
    limit.map(|l| l.min(MAX_QUERY_LIMIT))
}

use std::sync::Arc;
use std::time::Instant;

use roaring::RoaringBitmap;

use zen_catalog::{Catalog, SegmentRow};
use zen_common::{PartitionId, TenantId, ZenError, ZenResult};
use zen_format::{ColumnValues, PageView, RowValue, SegmentReader};
use zen_storage::BlobStore;

use crate::expr::{Expr, Literal};
use crate::logical::{AggregateFn, LogicalPlan};
use crate::physical::{aggregate_label, AggResult};
use crate::row::{ResultRow, ResultSet, ResultStats};
use crate::segment_cache::{SegmentCache, SegmentExtras};
use crate::segment_list_cache::SegmentListCache;

pub async fn execute(
    plan: &LogicalPlan,
    catalog: Arc<dyn Catalog>,
    store: Arc<dyn BlobStore>,
) -> ZenResult<ResultSet> {
    execute_with_cache(plan, catalog, store, &SegmentCache::default()).await
}

pub async fn execute_with_cache(
    plan: &LogicalPlan,
    catalog: Arc<dyn Catalog>,
    store: Arc<dyn BlobStore>,
    seg_cache: &SegmentCache,
) -> ZenResult<ResultSet> {
    execute_full(
        plan,
        catalog,
        store,
        seg_cache,
        &SegmentListCache::default(),
    )
    .await
}

pub async fn execute_full(
    plan: &LogicalPlan,
    catalog: Arc<dyn Catalog>,
    store: Arc<dyn BlobStore>,
    seg_cache: &SegmentCache,
    list_cache: &SegmentListCache,
) -> ZenResult<ResultSet> {
    // SECURITY: clamp the parsed LIMIT before any scan kicks off so a
    // request with `LIMIT 999_999_999` (or none at all on a wide
    // SELECT) can't DoS the executor.
    let mut plan = plan.clone();
    plan.limit = clamp_limit(plan.limit);
    let plan = &plan;
    // Fast path: pure GROUP BY + COUNT(*), no ORDER BY. Bypass ResultRow
    // construction and operate directly on the dict-encoded group_by columns.
    if !plan.aggregates.is_empty()
        && plan.order_by.is_none()
        && plan
            .aggregates
            .iter()
            .all(|(_, a)| matches!(a, AggregateFn::Count))
    {
        return execute_count_aggregate(plan, catalog, store, seg_cache, list_cache).await;
    }

    let start = Instant::now();
    let mut stats = ResultStats::default();

    let tenant = TenantId(plan.tenant_id);
    let mut all_rows: Vec<ResultRow> = Vec::new();

    // Extract a literal `trace_id = '...'` filter for segment-level pruning.
    let trace_id_filter: Option<[u8; 16]> = plan
        .predicate
        .as_ref()
        .and_then(|p| extract_trace_id_filter(&p.expr));

    for &p in &plan.partition_ids {
        let all_segments = list_cache
            .list(
                &catalog,
                tenant,
                PartitionId(p),
                plan.time_min_ms,
                plan.time_max_ms,
            )
            .await?;
        // Prune segments whose trace_id range can't contain the literal trace_id
        // (point-lookup fast path).
        let segments: Vec<_> = if let Some(tid) = trace_id_filter {
            let pruned: Vec<_> = all_segments
                .iter()
                .filter(|s| s.trace_id_min.0 <= tid && tid <= s.trace_id_max.0)
                .cloned()
                .collect();
            stats.row_groups_pruned += (all_segments.len() - pruned.len()) as u32;
            pruned
        } else {
            all_segments.iter().cloned().collect()
        };
        stats.segments_scanned += segments.len() as u32;

        if segments.len() <= 1 {
            for seg in segments.iter() {
                scan_one_segment(
                    seg,
                    store.clone(),
                    seg_cache,
                    plan,
                    &mut all_rows,
                    &mut stats,
                )
                .await?;
            }
        } else {
            // Bounded concurrency: stream segments through a window of
            // MAX_IN_FLIGHT in-flight scans. Doesn't matter for 50 segments,
            // matters for 10,000+.
            use futures::stream::{self, StreamExt};
            const MAX_IN_FLIGHT: usize = 64;
            let plan_clone = plan.clone();
            let store = store.clone();
            let seg_cache = seg_cache.clone();
            let results: Vec<ZenResult<(Vec<ResultRow>, ResultStats)>> =
                stream::iter(segments.into_iter())
                    .map(|seg| {
                        let store = store.clone();
                        let seg_cache = seg_cache.clone();
                        let plan = plan_clone.clone();
                        async move {
                            let mut rows: Vec<ResultRow> = Vec::new();
                            let mut s = ResultStats::default();
                            scan_one_segment(&seg, store, &seg_cache, &plan, &mut rows, &mut s)
                                .await
                                .map(|()| (rows, s))
                        }
                    })
                    .buffer_unordered(MAX_IN_FLIGHT)
                    .collect()
                    .await;
            for r in results {
                let (rows, s) = r?;
                all_rows.extend(rows);
                stats.row_groups_pruned += s.row_groups_pruned;
                stats.row_groups_scanned += s.row_groups_scanned;
                stats.bytes_decoded_wide += s.bytes_decoded_wide;
            }
        }

        // Sync write visibility: scan unconsumed WAL files in the same
        // partition. Pure WALs are small (cap at flush_max_bytes per object).
        // We only scan if there's no expensive filtering they can't support.
        let wal_rows = scan_unconsumed_wals(
            &catalog,
            store.clone(),
            tenant,
            PartitionId(p),
            plan,
            trace_id_filter,
        )
        .await?;
        all_rows.extend(wal_rows);
    }

    // ORDER BY (string or numeric).
    if let Some((col, asc)) = &plan.order_by {
        all_rows.sort_by(|a, b| {
            let av = a.fields.get(col);
            let bv = b.fields.get(col);
            let ord = compare_json_values(av, bv);
            if *asc {
                ord
            } else {
                ord.reverse()
            }
        });
    }

    // GROUP BY + AGGREGATE.
    let final_rows = if !plan.aggregates.is_empty() || !plan.group_by.is_empty() {
        run_group_aggregate(&all_rows, plan)
    } else {
        all_rows
    };

    // LIMIT.
    let mut final_rows = final_rows;
    if let Some(l) = plan.limit {
        final_rows.truncate(l as usize);
    }

    // Project: if explicit columns, keep only those.
    if plan.aggregates.is_empty() {
        if let Some(cols) = &plan.projection.columns {
            for row in &mut final_rows {
                row.fields = row
                    .fields
                    .iter()
                    .filter(|(k, _)| cols.contains(k))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
            }
        }
    }

    stats.elapsed_ms = start.elapsed().as_millis() as u64;
    stats.rows_returned = final_rows.len() as u32;

    let columns = if let Some(cols) = &plan.projection.columns {
        cols.clone()
    } else {
        // Emit a stable field order from the first row.
        final_rows
            .first()
            .map(|r| r.fields.keys().cloned().collect())
            .unwrap_or_default()
    };

    Ok(ResultSet {
        columns,
        rows: final_rows,
        stats,
    })
}

async fn scan_one_segment(
    seg: &SegmentRow,
    store: Arc<dyn BlobStore>,
    seg_cache: &SegmentCache,
    plan: &LogicalPlan,
    out: &mut Vec<ResultRow>,
    stats: &mut ResultStats,
) -> ZenResult<()> {
    let extras = seg_cache.get_or_load(&seg.object_key, store).await?;
    let reader = extras.reader.clone();
    let n_rgs = reader.row_group_count();

    if n_rgs <= 1 {
        for rg_idx in 0..n_rgs {
            let res = scan_row_group(&extras, rg_idx, plan)?;
            out.extend(res.rows);
            stats.row_groups_pruned += res.pruned;
            stats.row_groups_scanned += res.scanned;
            stats.bytes_decoded_wide += res.bytes_decoded_wide;
        }
        return Ok(());
    }

    let mut handles: Vec<tokio::task::JoinHandle<ZenResult<RgScanResult>>> =
        Vec::with_capacity(n_rgs);
    for rg_idx in 0..n_rgs {
        let extras = extras.clone();
        let plan = plan.clone();
        handles.push(tokio::task::spawn_blocking(move || {
            scan_row_group(&extras, rg_idx, &plan)
        }));
    }
    for h in handles {
        let res = h
            .await
            .map_err(|e| zen_common::ZenError::query(format!("rg join: {e}")))??;
        out.extend(res.rows);
        stats.row_groups_pruned += res.pruned;
        stats.row_groups_scanned += res.scanned;
        stats.bytes_decoded_wide += res.bytes_decoded_wide;
    }
    Ok(())
}

struct RgScanResult {
    rows: Vec<ResultRow>,
    pruned: u32,
    scanned: u32,
    bytes_decoded_wide: u64,
}

fn scan_row_group(
    extras: &SegmentExtras,
    rg_idx: usize,
    plan: &LogicalPlan,
) -> ZenResult<RgScanResult> {
    let reader = &extras.reader;
    let col_idx = |name: &str| -> Option<u32> {
        reader
            .metadata
            .column_names
            .iter()
            .position(|c| c == name)
            .map(|i| i as u32)
    };

    let mut out = RgScanResult {
        rows: Vec::new(),
        pruned: 0,
        scanned: 0,
        bytes_decoded_wide: 0,
    };

    let predicate = plan.predicate.as_ref().map(|p| &p.expr);
    let row_count = reader.row_groups[rg_idx].row_count as usize;

    if let Some(expr) = predicate {
        if !zone_map_might_match(reader, &col_idx, rg_idx, expr) {
            out.pruned += 1;
            return Ok(out);
        }
    }
    if plan.time_min_ms != i64::MIN || plan.time_max_ms != i64::MAX {
        if let Some(t_idx) = col_idx("start_time_ms") {
            if let Some(rg_hc) = reader.hotcache.row_groups.get(rg_idx) {
                if let Some(c) = rg_hc.columns.iter().find(|c| c.column_idx == t_idx) {
                    if let zen_index::ZoneMapValue::I64 { min, max } = c.zone_map.value {
                        if max < plan.time_min_ms || min > plan.time_max_ms {
                            out.pruned += 1;
                            return Ok(out);
                        }
                    }
                }
            }
        }
    }

    let mut mask = RoaringBitmap::new();
    for r in 0..row_count {
        mask.insert(r as u32);
    }
    if let Some(expr) = predicate {
        let bm = eval_predicate(extras, &col_idx, rg_idx, expr, row_count)?;
        mask &= bm;
    }
    if plan.time_min_ms != i64::MIN || plan.time_max_ms != i64::MAX {
        if let Some(t_idx) = col_idx("start_time_ms") {
            if let ColumnValues::I64(times) = reader.read_column(rg_idx, t_idx)? {
                let mut tmask = RoaringBitmap::new();
                for (i, t) in times.iter().enumerate() {
                    if *t >= plan.time_min_ms && *t <= plan.time_max_ms {
                        tmask.insert(i as u32);
                    }
                }
                mask &= tmask;
            }
        }
    }
    if mask.is_empty() {
        out.pruned += 1;
        return Ok(out);
    }
    out.scanned += 1;
    let limit = plan.limit.map(|l| l as usize);

    // LIMIT pushdown: when the query has a LIMIT and no global aggregation
    // / ORDER BY, only materialize the first `limit` surviving rows from
    // this row group. Without this, queries returning a large bitmap pay
    // the cost of decoding every match before truncating to LIMIT.
    let allow_limit_pushdown =
        plan.aggregates.is_empty() && plan.order_by.is_none() && plan.group_by.is_empty();
    let rows_idx: Vec<usize> = if let (Some(l), true) = (limit, allow_limit_pushdown) {
        mask.iter().take(l).map(|i| i as usize).collect()
    } else {
        mask.iter().map(|i| i as usize).collect()
    };
    let mut local_stats = ResultStats::default();

    // Aggregate fast path: when the query only needs group_by + count(*) (or
    // sum/avg/min/max over numeric cols), we don't need to materialize wide
    // string columns. Only decode the columns the aggregation references.
    let aggregate_only = !plan.aggregates.is_empty()
        && plan
            .aggregates
            .iter()
            .all(|(_, a)| matches!(a, AggregateFn::Count));
    let mut buf: Vec<ResultRow> = Vec::with_capacity(rows_idx.len());
    if aggregate_only && plan.order_by.is_none() {
        // Only decode group_by columns. Skip wide-column materialization
        // entirely.
        let cols: Vec<String> = plan.group_by.clone();
        materialize_rows_minimal(reader, &col_idx, rg_idx, &rows_idx, &cols, &mut buf)?;
    } else {
        materialize_rows(
            reader,
            &col_idx,
            rg_idx,
            &rows_idx,
            plan,
            &mut buf,
            &mut local_stats,
        )?;
    }
    if let Some(l) = limit {
        if !plan.aggregates.is_empty() || plan.order_by.is_some() {
            // Global aggregation / ordering needs all rows.
        } else {
            buf.truncate(l);
        }
    }
    out.bytes_decoded_wide += local_stats.bytes_decoded_wide;
    out.rows = buf;
    Ok(out)
}

/// Minimal materialization: only decode the listed columns. Used for the
/// aggregate fast path. Skips JSON encoding for columns we don't need.
fn materialize_rows_minimal(
    reader: &SegmentReader,
    col_idx: &dyn Fn(&str) -> Option<u32>,
    rg_idx: usize,
    rows: &[usize],
    cols: &[String],
    out: &mut Vec<ResultRow>,
) -> ZenResult<()> {
    use zen_format::PageView;
    let mut views: Vec<(String, PageView<'_>)> = Vec::new();
    for c in cols {
        if let Some(i) = col_idx(c) {
            if reader.row_groups[rg_idx]
                .descriptor_for_column(i)
                .is_some()
            {
                views.push((c.clone(), reader.open_page(rg_idx, i)?));
            }
        }
    }
    let mut new_rows: Vec<ResultRow> = rows.iter().map(|_| ResultRow::default()).collect();
    for (col_name, view) in &views {
        for (i, &r) in rows.iter().enumerate() {
            let v = view.row(r)?;
            new_rows[i].fields.insert(col_name.clone(), row_value_to_json(v));
        }
    }
    out.extend(new_rows);
    Ok(())
}

/// Fast path: GROUP BY (...) COUNT(*). Skips materialization and aggregates
/// directly on dict-encoded columns.
async fn execute_count_aggregate(
    plan: &LogicalPlan,
    catalog: Arc<dyn Catalog>,
    store: Arc<dyn BlobStore>,
    seg_cache: &SegmentCache,
    list_cache: &SegmentListCache,
) -> ZenResult<ResultSet> {
    use std::collections::BTreeMap;

    let start = Instant::now();
    let mut stats = ResultStats::default();
    let tenant = TenantId(plan.tenant_id);
    let trace_id_filter: Option<[u8; 16]> = plan
        .predicate
        .as_ref()
        .and_then(|p| extract_trace_id_filter(&p.expr));

    // Per-(group key tuple) → count.
    let mut counts: BTreeMap<Vec<String>, i64> = BTreeMap::new();

    for &p in &plan.partition_ids {
        let all_segments = list_cache
            .list(
                &catalog,
                tenant,
                PartitionId(p),
                plan.time_min_ms,
                plan.time_max_ms,
            )
            .await?;
        let segments: Vec<_> = if let Some(tid) = trace_id_filter {
            all_segments
                .iter()
                .filter(|s| s.trace_id_min.0 <= tid && tid <= s.trace_id_max.0)
                .cloned()
                .collect()
        } else {
            all_segments.iter().cloned().collect()
        };
        stats.segments_scanned += segments.len() as u32;

        // Spawn per-segment, merge into local map.
        use futures::future::join_all;
        let plan_clone = plan.clone();
        let mut futs = Vec::with_capacity(segments.len());
        for seg in segments.iter() {
            let seg = seg.clone();
            let store = store.clone();
            let seg_cache = seg_cache.clone();
            let plan = plan_clone.clone();
            futs.push(async move {
                count_one_segment(&seg, store, &seg_cache, &plan).await
            });
        }
        for r in join_all(futs).await {
            let (m, s) = r?;
            stats.row_groups_pruned += s.row_groups_pruned;
            stats.row_groups_scanned += s.row_groups_scanned;
            for (k, v) in m {
                *counts.entry(k).or_insert(0) += v;
            }
        }

        // Sync write visibility for COUNT GROUP BY: also count rows in
        // unconsumed WAL files. We pull them as ResultRow then aggregate.
        let wal_rows = scan_unconsumed_wals(
            &catalog,
            store.clone(),
            tenant,
            PartitionId(p),
            plan,
            trace_id_filter,
        )
        .await?;
        for row in wal_rows {
            let key: Vec<String> = plan
                .group_by
                .iter()
                .map(|c| {
                    row.fields
                        .get(c)
                        .map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                        .unwrap_or_default()
                })
                .collect();
            *counts.entry(key).or_insert(0) += 1;
        }
    }

    // Build result rows.
    let mut final_rows: Vec<ResultRow> = Vec::with_capacity(counts.len());
    for (key, count) in counts {
        let mut row = ResultRow::default();
        for (i, col) in plan.group_by.iter().enumerate() {
            row.fields.insert(
                col.clone(),
                serde_json::Value::String(key.get(i).cloned().unwrap_or_default()),
            );
        }
        for (label, agg) in &plan.aggregates {
            let lbl = aggregate_label(label, agg);
            row.fields.insert(lbl, serde_json::Value::from(count));
        }
        final_rows.push(row);
    }

    if let Some(l) = plan.limit {
        final_rows.truncate(l as usize);
    }

    stats.elapsed_ms = start.elapsed().as_millis() as u64;
    stats.rows_returned = final_rows.len() as u32;
    let mut columns: Vec<String> = plan.group_by.clone();
    for (label, agg) in &plan.aggregates {
        columns.push(aggregate_label(label, agg));
    }
    Ok(ResultSet {
        columns,
        rows: final_rows,
        stats,
    })
}

async fn count_one_segment(
    seg: &SegmentRow,
    store: Arc<dyn BlobStore>,
    seg_cache: &SegmentCache,
    plan: &LogicalPlan,
) -> ZenResult<(ahash::AHashMap<Vec<String>, i64>, ResultStats)> {
    use zen_format::ColumnValues;
    let extras = seg_cache.get_or_load(&seg.object_key, store).await?;
    let reader = &extras.reader;

    // ahash is significantly faster than the default SipHash for the small,
    // numeric or short-string keys we hit in aggregations (3-5× hash throughput).
    let mut counts: ahash::AHashMap<Vec<String>, i64> =
        ahash::AHashMap::with_capacity(64);
    let mut stats = ResultStats::default();

    for rg_idx in 0..reader.row_group_count() {
        let row_count = reader.row_groups[rg_idx].row_count as usize;
        let col_idx = |name: &str| -> Option<u32> {
            reader
                .metadata
                .column_names
                .iter()
                .position(|c| c == name)
                .map(|i| i as u32)
        };

        // Predicate prune via zone maps + posting-list lookup.
        let predicate = plan.predicate.as_ref().map(|p| &p.expr);
        if let Some(expr) = predicate {
            if !zone_map_might_match(reader, &col_idx, rg_idx, expr) {
                stats.row_groups_pruned += 1;
                continue;
            }
        }

        // Compute mask.
        let mut mask = RoaringBitmap::new();
        for r in 0..row_count {
            mask.insert(r as u32);
        }
        if let Some(expr) = predicate {
            let bm = eval_predicate(&extras, &col_idx, rg_idx, expr, row_count)?;
            mask &= bm;
        }
        if mask.is_empty() {
            stats.row_groups_pruned += 1;
            continue;
        }
        stats.row_groups_scanned += 1;

        // Fast path: no group_by → just add to total.
        if plan.group_by.is_empty() {
            *counts.entry(Vec::new()).or_insert(0) += mask.len() as i64;
            continue;
        }

        let cols: Vec<u32> = plan
            .group_by
            .iter()
            .filter_map(|c| col_idx(c))
            .collect();
        if cols.len() != plan.group_by.len() {
            return Err(zen_common::ZenError::query("group_by column missing"));
        }

        // Fast path: single dict-encoded group_by column → count by dict_id
        // directly. Avoids 100K String allocations per row group.
        if cols.len() == 1 {
            use zen_format::PageView;
            let view = reader.open_page(rg_idx, cols[0])?;
            if let PageView::Dict(dec) = view {
                // For each row in mask, look up dict key (u32 → u32 counter).
                let mut local: ahash::AHashMap<u32, i64> =
                    ahash::AHashMap::with_capacity(dec.dict.len());
                for r in mask.iter() {
                    let r = r as usize;
                    let k = dec.keys[r];
                    *local.entry(k).or_insert(0) += 1;
                }
                for (k, c) in local {
                    let s = String::from_utf8_lossy(&dec.dict[k as usize]).into_owned();
                    *counts.entry(vec![s]).or_insert(0) += c;
                }
                continue;
            }
        }

        // General path: decode each group_by column to Vec<String>, key by tuple.
        let mut col_values: Vec<Vec<String>> = Vec::with_capacity(cols.len());
        for &c in &cols {
            let cv = reader.read_column(rg_idx, c)?;
            let strs: Vec<String> = match cv {
                ColumnValues::StringsOwned(v) => v
                    .into_iter()
                    .map(|s| String::from_utf8_lossy(&s).into_owned())
                    .collect(),
                ColumnValues::I64(v) => v.into_iter().map(|i| i.to_string()).collect(),
                ColumnValues::F64(v) => v.into_iter().map(|f| f.to_string()).collect(),
                _ => return Err(zen_common::ZenError::query("unsupported group_by col type")),
            };
            col_values.push(strs);
        }
        for r in mask.iter() {
            let r = r as usize;
            let key: Vec<String> = col_values.iter().map(|v| v[r].clone()).collect();
            *counts.entry(key).or_insert(0) += 1;
        }
    }
    Ok((counts, stats))
}

/// Walk the predicate tree looking for an `Eq(Column("trace_id"), Literal::String(ulid))`
/// at any conjunction position. Returns the 16-byte trace_id if found.
fn extract_trace_id_filter(expr: &Expr) -> Option<[u8; 16]> {
    use ulid::Ulid;
    match expr {
        Expr::And(a, b) => extract_trace_id_filter(a).or_else(|| extract_trace_id_filter(b)),
        Expr::Eq(left, right) => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(c), Expr::Literal(Literal::String(v))) if c == "trace_id" => {
                Ulid::from_string(v).ok().map(|u| u.0.to_be_bytes())
            }
            _ => None,
        },
        _ => None,
    }
}

/// Returns `false` if the row group provably can't satisfy the predicate, given
/// the per-(rg,column) zone maps in the segment's hotcache. Conservative: when
/// in doubt, returns `true` (no pruning).
fn zone_map_might_match(
    reader: &SegmentReader,
    col_idx: &dyn Fn(&str) -> Option<u32>,
    rg_idx: usize,
    expr: &Expr,
) -> bool {
    use zen_index::ZoneMapValue;

    let lookup_zm = |column: &str| {
        let cidx = col_idx(column)?;
        reader
            .hotcache
            .row_groups
            .get(rg_idx)?
            .columns
            .iter()
            .find(|c| c.column_idx == cidx)
            .map(|c| &c.zone_map)
    };

    match expr {
        Expr::And(a, b) => {
            zone_map_might_match(reader, col_idx, rg_idx, a)
                && zone_map_might_match(reader, col_idx, rg_idx, b)
        }
        Expr::Or(a, b) => {
            zone_map_might_match(reader, col_idx, rg_idx, a)
                || zone_map_might_match(reader, col_idx, rg_idx, b)
        }
        Expr::Eq(left, right) => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(c), Expr::Literal(Literal::String(v))) => {
                if let Some(zm) = lookup_zm(c) {
                    match &zm.value {
                        ZoneMapValue::Bytes { min, max } => {
                            return v.as_bytes() >= min.as_slice()
                                && v.as_bytes() <= max.as_slice();
                        }
                        ZoneMapValue::Fixed { min, max } => {
                            // trace_id / span_id columns: parse ULID literal.
                            if let Ok(u) = ulid::Ulid::from_string(v) {
                                let bytes = u.0.to_be_bytes();
                                let bs: &[u8] = &bytes;
                                return bs >= min.as_slice() && bs <= max.as_slice();
                            }
                            return true;
                        }
                        _ => {}
                    }
                }
                true
            }
            (Expr::Column(c), Expr::Literal(Literal::Int(v))) => {
                if let Some(zm) = lookup_zm(c) {
                    if let ZoneMapValue::I64 { min, max } = zm.value {
                        return *v >= min && *v <= max;
                    }
                }
                true
            }
            _ => true,
        },
        Expr::Lt(left, right) | Expr::Le(left, right) => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(c), Expr::Literal(Literal::Int(v))) => {
                if let Some(zm) = lookup_zm(c) {
                    if let ZoneMapValue::I64 { min, .. } = zm.value {
                        return min <= *v;
                    }
                }
                true
            }
            _ => true,
        },
        Expr::Gt(left, right) | Expr::Ge(left, right) => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(c), Expr::Literal(Literal::Int(v))) => {
                if let Some(zm) = lookup_zm(c) {
                    if let ZoneMapValue::I64 { max, .. } = zm.value {
                        return max >= *v;
                    }
                }
                true
            }
            _ => true,
        },
        _ => true,
    }
}

fn eval_predicate(
    extras: &SegmentExtras,
    col_idx: &dyn Fn(&str) -> Option<u32>,
    rg_idx: usize,
    expr: &Expr,
    row_count: usize,
) -> ZenResult<RoaringBitmap> {
    let reader = &extras.reader;
    match expr {
        Expr::And(a, b) => {
            let l = eval_predicate(extras, col_idx, rg_idx, a, row_count)?;
            let r = eval_predicate(extras, col_idx, rg_idx, b, row_count)?;
            Ok(l & r)
        }
        Expr::Or(a, b) => {
            let l = eval_predicate(extras, col_idx, rg_idx, a, row_count)?;
            let r = eval_predicate(extras, col_idx, rg_idx, b, row_count)?;
            Ok(l | r)
        }
        Expr::Not(a) => {
            let l = eval_predicate(extras, col_idx, rg_idx, a, row_count)?;
            let mut all = RoaringBitmap::new();
            for i in 0..row_count {
                all.insert(i as u32);
            }
            Ok(all - l)
        }
        Expr::Eq(left, right) => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(c), Expr::Literal(Literal::String(v))) => {
                let i = col_idx(c).ok_or_else(|| ZenError::query(format!("column {c} not found")))?;
                // Fast path: cached bitmap posting list lookup if available.
                if let Some(bm) = extras.posting_lookup_cached(rg_idx as u32, i, v.as_bytes()) {
                    return Ok((*bm).clone());
                }
                let cv = reader.read_column(rg_idx, i)?;
                Ok(eq_string(&cv, v))
            }
            (Expr::Column(c), Expr::Literal(Literal::Int(v))) => {
                let i = col_idx(c).ok_or_else(|| ZenError::query(format!("column {c} not found")))?;
                let cv = reader.read_column(rg_idx, i)?;
                Ok(eq_int(&cv, *v))
            }
            _ => Err(ZenError::query(format!("unsupported Eq form: {expr:?}"))),
        },
        Expr::Lt(_, _) | Expr::Le(_, _) | Expr::Gt(_, _) | Expr::Ge(_, _) | Expr::Ne(_, _) => {
            eval_compare(reader, col_idx, rg_idx, expr)
        }
        Expr::TextMatch { column, query } => {
            // Cached FTS handle path.
            if let Some(bm) = extras.fts_search_cached(rg_idx as u32, column, query) {
                return Ok((*bm).clone());
            }
            scan_text_match(reader, col_idx, rg_idx, expr)
        }
        Expr::JsonPathEq { path, value } => {
            if let Some(bm) = extras.jsonpath_lookup_cached(rg_idx as u32, path, value) {
                return Ok((*bm).clone());
            }
            scan_jsonpath_eq(reader, col_idx, rg_idx, expr)
        }
        Expr::Literal(Literal::Bool(true)) => {
            let mut all = RoaringBitmap::new();
            for i in 0..row_count {
                all.insert(i as u32);
            }
            Ok(all)
        }
        Expr::Literal(Literal::Bool(false)) => Ok(RoaringBitmap::new()),
        _ => Err(ZenError::query(format!("unsupported predicate: {expr:?}"))),
    }
}

fn eval_compare(
    reader: &SegmentReader,
    col_idx: &dyn Fn(&str) -> Option<u32>,
    rg_idx: usize,
    expr: &Expr,
) -> ZenResult<RoaringBitmap> {
    let (op_lt, op_le, op_gt, op_ge, op_ne) = (
        matches!(expr, Expr::Lt(_, _)),
        matches!(expr, Expr::Le(_, _)),
        matches!(expr, Expr::Gt(_, _)),
        matches!(expr, Expr::Ge(_, _)),
        matches!(expr, Expr::Ne(_, _)),
    );
    let (left, right) = match expr {
        Expr::Lt(a, b) | Expr::Le(a, b) | Expr::Gt(a, b) | Expr::Ge(a, b) | Expr::Ne(a, b) => (a, b),
        // Defensive: every variant the precondition matches against
        // is enumerated above. If a future Expr variant slips through
        // the precondition check, return a structured error rather
        // than panicking — a panic on user-supplied query input is a
        // DoS.
        other => {
            return Err(ZenError::query(format!(
                "comparison helper called with non-comparison expression: {other:?}"
            )));
        }
    };
    if let (Expr::Column(c), Expr::Literal(Literal::Int(v))) = (left.as_ref(), right.as_ref()) {
        let i = col_idx(c).ok_or_else(|| ZenError::query(format!("column {c} not found")))?;
        let cv = reader.read_column(rg_idx, i)?;
        if let ColumnValues::I64(arr) = cv {
            let mut bm = RoaringBitmap::new();
            for (idx, x) in arr.iter().enumerate() {
                let ok = if op_lt {
                    *x < *v
                } else if op_le {
                    *x <= *v
                } else if op_gt {
                    *x > *v
                } else if op_ge {
                    *x >= *v
                } else if op_ne {
                    *x != *v
                } else {
                    false
                };
                if ok {
                    bm.insert(idx as u32);
                }
            }
            return Ok(bm);
        }
    }
    Err(ZenError::query(format!("unsupported compare: {expr:?}")))
}

fn scan_text_match(
    reader: &SegmentReader,
    col_idx: &dyn Fn(&str) -> Option<u32>,
    rg_idx: usize,
    expr: &Expr,
) -> ZenResult<RoaringBitmap> {
    if let Expr::TextMatch { column, query } = expr {
        let i = col_idx(column).ok_or_else(|| ZenError::query(format!("column {column}")))?;
        // Fast path: indexed FTS lookup if the segment has a Tantivy blob
        // recorded in the hotcache for this column.
        if let Some(rg_hc) = reader.hotcache.row_groups.get(rg_idx) {
            if let Some(c) = rg_hc.columns.iter().find(|c| c.column_idx == i) {
                if let (Some(off), Some(len)) = (c.fts_offset, c.fts_length) {
                    let inline_base = reader.footer.inline_indexes_offset as usize;
                    let start = inline_base + off as usize;
                    let end = start + len as usize;
                    if reader.bytes.len() >= end {
                        let blob = &reader.bytes[start..end];
                        if let Ok(handle) = zen_fts::open_fts_index(blob) {
                            let q = zen_fts::FtsQuery {
                                field: Some(column),
                                query,
                                limit: 100_000,
                            };
                            if let Ok(bm) = handle.search_to_bitmap(&q) {
                                return Ok(bm);
                            }
                        }
                    }
                }
            }
        }
        // Scan fallback.
        let view = reader.open_page(rg_idx, i)?;
        let mut bm = RoaringBitmap::new();
        let needle = query.to_lowercase();
        for r in 0..view.row_count() {
            if let RowValue::Bytes(b) = view.row(r)? {
                if let Ok(s) = std::str::from_utf8(&b) {
                    if s.to_lowercase().contains(&needle) {
                        bm.insert(r as u32);
                    }
                }
            }
        }
        return Ok(bm);
    }
    Err(ZenError::query("not a text match"))
}

fn scan_jsonpath_eq(
    reader: &SegmentReader,
    col_idx: &dyn Fn(&str) -> Option<u32>,
    rg_idx: usize,
    expr: &Expr,
) -> ZenResult<RoaringBitmap> {
    if let Expr::JsonPathEq { path, value } = expr {
        let meta_idx = col_idx("metadata").ok_or_else(|| ZenError::query("no metadata column"))?;
        // Fast path: indexed JSON-path posting lookup.
        if let Some(rg_hc) = reader.hotcache.row_groups.get(rg_idx) {
            if let Some(c) = rg_hc.columns.iter().find(|c| c.column_idx == meta_idx) {
                if let (Some(off), Some(len)) = (c.jsonpath_offset, c.jsonpath_length) {
                    let inline_base = reader.footer.inline_indexes_offset as usize;
                    let start = inline_base + off as usize;
                    let end = start + len as usize;
                    if reader.bytes.len() >= end {
                        let bytes = &reader.bytes[start..end];
                        if let Ok(idx) = zen_jsonpath::JsonPathIndex::deserialize(bytes) {
                            if idx.knows_path(path) {
                                let bm = idx
                                    .lookup(path, value)
                                    .cloned()
                                    .unwrap_or_default();
                                return Ok(bm);
                            }
                        }
                    }
                }
            }
        }
        // Scan fallback.
        let view = reader.open_page(rg_idx, meta_idx)?;
        let mut bm = RoaringBitmap::new();
        for r in 0..view.row_count() {
            if let RowValue::Bytes(b) = view.row(r)? {
                if b.is_empty() {
                    continue;
                }
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&b) {
                    let mut found = false;
                    zen_jsonpath::discovery::walk(&v, "", 0, 8, &mut |p, scalar| {
                        if !found && p == path && scalar == Some(value.as_str()) {
                            found = true;
                        }
                    });
                    if found {
                        bm.insert(r as u32);
                    }
                }
            }
        }
        return Ok(bm);
    }
    Err(ZenError::query("not jsonpath_eq"))
}

/// If a posting list is available in the hotcache for `(rg_idx, column_idx)`,
/// look up `value` and return the matching row mask. Returns `None` if no
/// posting list exists or the value isn't present (returns empty bitmap if
/// posting list is present but value is missing).
///
/// Currently unused — the inline scan path is fast enough that the posting
/// lookup hasn't earned its keep on the workloads measured so far. Kept
/// for the future "low-cardinality-equality" optimizer path; remove if
/// the v1 design ships without it.
#[allow(dead_code)]
fn posting_lookup(
    reader: &SegmentReader,
    rg_idx: usize,
    column_idx: u32,
    value: &[u8],
) -> Option<RoaringBitmap> {
    let rg = reader.hotcache.row_groups.get(rg_idx)?;
    let entry = rg.columns.iter().find(|c| c.column_idx == column_idx)?;
    let local_off = entry.posting_offset?;
    let len = entry.posting_length? as usize;
    let inline_base = reader.footer.inline_indexes_offset as usize;
    let start = inline_base + local_off as usize;
    let end = start + len;
    if reader.bytes.len() < end {
        return None;
    }
    let bytes = &reader.bytes[start..end];
    let pm = zen_index::PostingMap::deserialize(bytes).ok()?;
    Some(
        pm.get(value)
            .map(|pl| pl.bitmap.clone())
            .unwrap_or_default(),
    )
}

fn eq_string(cv: &ColumnValues<'static>, value: &str) -> RoaringBitmap {
    let mut bm = RoaringBitmap::new();
    if let ColumnValues::StringsOwned(v) = cv {
        for (i, s) in v.iter().enumerate() {
            if s.as_slice() == value.as_bytes() {
                bm.insert(i as u32);
            }
        }
    }
    bm
}

fn eq_int(cv: &ColumnValues<'static>, value: i64) -> RoaringBitmap {
    let mut bm = RoaringBitmap::new();
    if let ColumnValues::I64(v) = cv {
        for (i, x) in v.iter().enumerate() {
            if *x == value {
                bm.insert(i as u32);
            }
        }
    }
    bm
}

fn materialize_rows(
    reader: &SegmentReader,
    col_idx: &dyn Fn(&str) -> Option<u32>,
    rg_idx: usize,
    rows: &[usize],
    plan: &LogicalPlan,
    out: &mut Vec<ResultRow>,
    stats: &mut ResultStats,
) -> ZenResult<()> {
    let cols_to_decode: Vec<&String> = match &plan.projection.columns {
        Some(cols) => cols.iter().collect(),
        None => reader.metadata.column_names.iter().collect(),
    };

    // Open page views once per column. Skip columns that aren't materialized in this RG.
    let mut views: Vec<(String, PageView<'_>)> = Vec::new();
    for c in &cols_to_decode {
        if let Some(i) = col_idx(c) {
            // Some columns (e.g. embedding) may not have a page for this row group.
            if reader.row_groups[rg_idx]
                .descriptor_for_column(i)
                .is_some()
            {
                views.push(((*c).clone(), reader.open_page(rg_idx, i)?));
            }
        }
    }

    let mut new_rows: Vec<ResultRow> = rows.iter().map(|_| ResultRow::default()).collect();
    for (col_name, view) in &views {
        let is_wide = matches!(
            col_name.as_str(),
            "prompt" | "completion" | "tool_io_text" | "metadata"
        );
        for (i, &r) in rows.iter().enumerate() {
            let v = view.row(r)?;
            let json = row_value_to_json(v);
            if is_wide {
                if let serde_json::Value::String(s) = &json {
                    stats.bytes_decoded_wide += s.len() as u64;
                }
            }
            new_rows[i].fields.insert(col_name.clone(), json);
        }
    }
    out.extend(new_rows);
    Ok(())
}

fn row_value_to_json(v: RowValue) -> serde_json::Value {
    match v {
        RowValue::Bytes(b) => match std::str::from_utf8(&b) {
            Ok(s) => serde_json::Value::String(s.to_string()),
            Err(_) => serde_json::json!(b),
        },
        RowValue::I64(x) => serde_json::Value::from(x),
        RowValue::F64(x) => serde_json::Value::from(x),
        RowValue::Fixed16(b) => {
            serde_json::Value::String(ulid::Ulid::from(u128::from_be_bytes(b)).to_string())
        }
    }
}

fn compare_json_values(a: Option<&serde_json::Value>, b: Option<&serde_json::Value>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Some(x), Some(y)) => match (x, y) {
            (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
                a.as_f64().unwrap_or(0.0).partial_cmp(&b.as_f64().unwrap_or(0.0)).unwrap_or(Ordering::Equal)
            }
            (serde_json::Value::String(a), serde_json::Value::String(b)) => a.cmp(b),
            _ => Ordering::Equal,
        },
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn run_group_aggregate(rows: &[ResultRow], plan: &LogicalPlan) -> Vec<ResultRow> {
    use std::collections::BTreeMap;
    if plan.group_by.is_empty() {
        // Single bucket aggregate.
        let mut out = ResultRow::default();
        for (label, agg) in &plan.aggregates {
            let result = compute_aggregate(rows, agg);
            out.fields
                .insert(aggregate_label(label, agg), agg_result_to_json(result));
        }
        return vec![out];
    }
    let mut groups: BTreeMap<Vec<String>, Vec<ResultRow>> = BTreeMap::new();
    for row in rows {
        let key: Vec<String> = plan
            .group_by
            .iter()
            .map(|c| {
                row.fields
                    .get(c)
                    .map(value_to_string)
                    .unwrap_or_default()
            })
            .collect();
        groups.entry(key).or_default().push(row.clone());
    }
    groups
        .into_iter()
        .map(|(key, group_rows)| {
            let mut row = ResultRow::default();
            for (i, c) in plan.group_by.iter().enumerate() {
                row.fields.insert(c.clone(), serde_json::Value::String(key[i].clone()));
            }
            for (label, agg) in &plan.aggregates {
                let result = compute_aggregate(&group_rows, agg);
                row.fields
                    .insert(aggregate_label(label, agg), agg_result_to_json(result));
            }
            row
        })
        .collect()
}

fn compute_aggregate(rows: &[ResultRow], agg: &AggregateFn) -> AggResult {
    match agg {
        AggregateFn::Count => AggResult::Int(rows.len() as i64),
        AggregateFn::Sum(c) => {
            let s: f64 = rows
                .iter()
                .filter_map(|r| r.fields.get(c).and_then(|v| v.as_f64()))
                .sum();
            AggResult::Float(s)
        }
        AggregateFn::Avg(c) => {
            let vals: Vec<f64> = rows
                .iter()
                .filter_map(|r| r.fields.get(c).and_then(|v| v.as_f64()))
                .collect();
            if vals.is_empty() {
                AggResult::Null
            } else {
                AggResult::Float(vals.iter().sum::<f64>() / vals.len() as f64)
            }
        }
        AggregateFn::Min(c) => {
            let vals: Vec<f64> = rows
                .iter()
                .filter_map(|r| r.fields.get(c).and_then(|v| v.as_f64()))
                .collect();
            if vals.is_empty() {
                AggResult::Null
            } else {
                AggResult::Float(vals.iter().cloned().fold(f64::INFINITY, f64::min))
            }
        }
        AggregateFn::Max(c) => {
            let vals: Vec<f64> = rows
                .iter()
                .filter_map(|r| r.fields.get(c).and_then(|v| v.as_f64()))
                .collect();
            if vals.is_empty() {
                AggResult::Null
            } else {
                AggResult::Float(vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max))
            }
        }
        AggregateFn::Percentile { column, q } => {
            let mut vals: Vec<f64> = rows
                .iter()
                .filter_map(|r| r.fields.get(column).and_then(|v| v.as_f64()))
                .collect();
            if vals.is_empty() {
                AggResult::Null
            } else {
                vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let idx = ((vals.len() as f64) * q).clamp(0.0, (vals.len() - 1) as f64) as usize;
                AggResult::Float(vals[idx])
            }
        }
    }
}

fn agg_result_to_json(r: AggResult) -> serde_json::Value {
    match r {
        AggResult::Int(i) => serde_json::Value::from(i),
        AggResult::Float(f) => serde_json::Value::from(f),
        AggResult::String(s) => serde_json::Value::String(s),
        AggResult::Null => serde_json::Value::Null,
    }
}

fn value_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "".into(),
        other => other.to_string(),
    }
}

/// Scan all WAL objects that haven't been consumed by a compaction yet, apply
/// the predicate, and return matching rows. This is the "writes are visible
/// on PUT-ack" path — without it, queries only see rows that have made it into
/// a published segment.
async fn scan_unconsumed_wals(
    catalog: &Arc<dyn Catalog>,
    store: Arc<dyn BlobStore>,
    tenant: TenantId,
    partition: PartitionId,
    plan: &LogicalPlan,
    trace_id_filter: Option<[u8; 16]>,
) -> ZenResult<Vec<ResultRow>> {
    use zen_common::CommitId;
    use zen_wal::WalReader;

    let wals = catalog
        .list_wal_objects(tenant, partition, CommitId(0))
        .await?;
    if wals.is_empty() {
        return Ok(Vec::new());
    }

    use futures::stream::{self, StreamExt};
    const MAX_IN_FLIGHT: usize = 64;
    let plan_clone = plan.clone();
    let store_clone = store.clone();
    let results: Vec<ZenResult<Vec<ResultRow>>> = stream::iter(wals.into_iter())
        .map(|wal| {
            let store = store_clone.clone();
            let plan = plan_clone.clone();
            async move {
                let bytes = match store.get(&wal.object_key).await {
                    Ok(b) => b,
                    Err(_) => return Ok(Vec::new()), // WAL deleted / superseded
                };
                let (_h, batches) = match WalReader::parse(&bytes) {
                    Ok(p) => p,
                    Err(_) => return Ok(Vec::new()),
                };
                let mut all = Vec::new();
                for batch in batches {
                    let mut rows = Vec::new();
                    arrow_batch_to_rows(&batch, &plan, trace_id_filter, &mut rows)?;
                    all.extend(rows);
                }
                Ok::<Vec<ResultRow>, zen_common::ZenError>(all)
            }
        })
        .buffer_unordered(MAX_IN_FLIGHT)
        .collect()
        .await;
    let mut out: Vec<ResultRow> = Vec::new();
    for r in results {
        out.extend(r?);
    }
    Ok(out)
}

fn arrow_batch_to_rows(
    batch: &arrow_array::RecordBatch,
    plan: &LogicalPlan,
    trace_id_filter: Option<[u8; 16]>,
    out: &mut Vec<ResultRow>,
) -> ZenResult<()> {
    use arrow_array::{Array, FixedSizeBinaryArray, Int64Array, StringArray, UInt32Array, UInt64Array};

    let schema = batch.schema();
    let n = batch.num_rows();

    // Vectorized predicate eval: build a row mask (Vec<bool>) over the entire
    // batch in one pass per predicate, downcasting columns ONCE outside the
    // hot loop. For 100K-row batches this is 50-100× faster than per-row
    // dynamic dispatch.
    let mut mask: Vec<bool> = vec![true; n];

    // Trace_id pre-filter.
    if let Some(tid_filter) = trace_id_filter {
        if let Ok(i) = schema.index_of("trace_id") {
            if let Some(arr) = batch.column(i).as_any().downcast_ref::<FixedSizeBinaryArray>() {
                for r in 0..n {
                    if mask[r] && arr.value(r) != tid_filter {
                        mask[r] = false;
                    }
                }
            }
        }
    }

    // Predicate eval, vectorized.
    if let Some(pred) = &plan.predicate {
        apply_pred_vectorized(batch, &schema, &pred.expr, &mut mask)?;
    }

    // Pre-resolve projection columns (downcast once each).
    let cols: Vec<String> = match &plan.projection.columns {
        Some(cols) => cols.clone(),
        None => vec![
            "trace_id".into(),
            "span_id".into(),
            "start_time_ms".into(),
            "end_time_ms".into(),
            "model".into(),
            "status".into(),
            "prompt".into(),
            "completion".into(),
        ],
    };
    let limit = plan.limit.map(|l| l as usize);

    // Pre-resolve column array references with their type tag.
    enum ColRef<'a> {
        Str(&'a StringArray),
        I64(&'a Int64Array),
        U32(&'a UInt32Array),
        U64(&'a UInt64Array),
        Fix16(&'a FixedSizeBinaryArray),
    }
    let mut refs: Vec<(String, ColRef<'_>)> = Vec::new();
    for col_name in &cols {
        let idx = match schema.index_of(col_name) {
            Ok(i) => i,
            Err(_) => continue,
        };
        let arr = batch.column(idx);
        if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
            refs.push((col_name.clone(), ColRef::Str(a)));
        } else if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
            refs.push((col_name.clone(), ColRef::I64(a)));
        } else if let Some(a) = arr.as_any().downcast_ref::<UInt32Array>() {
            refs.push((col_name.clone(), ColRef::U32(a)));
        } else if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
            refs.push((col_name.clone(), ColRef::U64(a)));
        } else if let Some(a) = arr.as_any().downcast_ref::<FixedSizeBinaryArray>() {
            refs.push((col_name.clone(), ColRef::Fix16(a)));
        }
    }

    for row_idx in 0..n {
        if !mask[row_idx] {
            continue;
        }
        let mut row = ResultRow::default();
        for (col_name, col_ref) in &refs {
            let val = match col_ref {
                ColRef::Str(a) if !a.is_null(row_idx) => {
                    serde_json::Value::String(a.value(row_idx).to_string())
                }
                ColRef::I64(a) if !a.is_null(row_idx) => {
                    serde_json::Value::from(a.value(row_idx))
                }
                ColRef::U32(a) if !a.is_null(row_idx) => {
                    serde_json::Value::from(a.value(row_idx))
                }
                ColRef::U64(a) if !a.is_null(row_idx) => {
                    serde_json::Value::from(a.value(row_idx))
                }
                ColRef::Fix16(a) if !a.is_null(row_idx) => {
                    let b = a.value(row_idx);
                    if b.len() == 16 {
                        let mut arr16 = [0u8; 16];
                        arr16.copy_from_slice(b);
                        serde_json::Value::String(
                            ulid::Ulid::from(u128::from_be_bytes(arr16)).to_string(),
                        )
                    } else {
                        continue;
                    }
                }
                _ => continue,
            };
            row.fields.insert(col_name.clone(), val);
        }
        out.push(row);
        if let Some(l) = limit {
            if out.len() >= l && plan.aggregates.is_empty() && plan.order_by.is_none() {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// Vectorized predicate evaluation — downcasts columns once per call and
/// AND/ORs masks together. For Eq/Ne against string or i64 scalars this is
/// effectively the same compute kernel as `arrow::compute::kernels::cmp`,
/// just without going through `Datum`.
fn apply_pred_vectorized(
    batch: &arrow_array::RecordBatch,
    schema: &arrow_schema::Schema,
    expr: &Expr,
    mask: &mut [bool],
) -> ZenResult<()> {
    use arrow_array::{Array, FixedSizeBinaryArray, Int64Array, StringArray};
    let n = batch.num_rows();

    match expr {
        Expr::And(a, b) => {
            apply_pred_vectorized(batch, schema, a, mask)?;
            // Short-circuit: AND with a separate buffer for b, then combine.
            let mut bmask = vec![true; n];
            apply_pred_vectorized(batch, schema, b, &mut bmask)?;
            for i in 0..n {
                mask[i] &= bmask[i];
            }
            Ok(())
        }
        Expr::Or(a, b) => {
            let mut amask = vec![true; n];
            let mut bmask = vec![true; n];
            apply_pred_vectorized(batch, schema, a, &mut amask)?;
            apply_pred_vectorized(batch, schema, b, &mut bmask)?;
            for i in 0..n {
                mask[i] = mask[i] && (amask[i] || bmask[i]);
            }
            Ok(())
        }
        Expr::Not(a) => {
            let mut amask = vec![true; n];
            apply_pred_vectorized(batch, schema, a, &mut amask)?;
            for i in 0..n {
                mask[i] &= !amask[i];
            }
            Ok(())
        }
        Expr::Eq(left, right) => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(c), Expr::Literal(Literal::String(v))) => {
                let i = match schema.index_of(c) {
                    Ok(i) => i,
                    Err(_) => {
                        for m in mask.iter_mut() {
                            *m = false;
                        }
                        return Ok(());
                    }
                };
                let arr = batch.column(i);
                if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                    let needle = v.as_str();
                    for r in 0..n {
                        if mask[r] && (a.is_null(r) || a.value(r) != needle) {
                            mask[r] = false;
                        }
                    }
                    return Ok(());
                }
                if let Some(a) = arr.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                    if let Ok(u) = ulid::Ulid::from_string(v) {
                        let target = u.0.to_be_bytes();
                        for r in 0..n {
                            if mask[r] && (a.is_null(r) || a.value(r) != &target[..]) {
                                mask[r] = false;
                            }
                        }
                        return Ok(());
                    }
                }
                for m in mask.iter_mut() {
                    *m = false;
                }
                Ok(())
            }
            (Expr::Column(c), Expr::Literal(Literal::Int(v))) => {
                let i = match schema.index_of(c) {
                    Ok(i) => i,
                    Err(_) => {
                        for m in mask.iter_mut() {
                            *m = false;
                        }
                        return Ok(());
                    }
                };
                let arr = batch.column(i);
                if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                    for r in 0..n {
                        if mask[r] && (a.is_null(r) || a.value(r) != *v) {
                            mask[r] = false;
                        }
                    }
                }
                Ok(())
            }
            _ => Ok(()), // unsupported: leave mask as-is (conservative pass)
        },
        Expr::TextMatch { column, query } => {
            let i = match schema.index_of(column) {
                Ok(i) => i,
                Err(_) => {
                    for m in mask.iter_mut() {
                        *m = false;
                    }
                    return Ok(());
                }
            };
            let arr = batch.column(i);
            if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                let needle = query.to_lowercase();
                for r in 0..n {
                    if mask[r] && (a.is_null(r) || !a.value(r).to_lowercase().contains(&needle)) {
                        mask[r] = false;
                    }
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

#[allow(dead_code)]
fn arrow_row_matches(
    batch: &arrow_array::RecordBatch,
    schema: &arrow_schema::Schema,
    row_idx: usize,
    expr: &Expr,
) -> ZenResult<bool> {
    use arrow_array::{Array, FixedSizeBinaryArray, Int64Array, StringArray};
    match expr {
        Expr::And(a, b) => Ok(arrow_row_matches(batch, schema, row_idx, a)?
            && arrow_row_matches(batch, schema, row_idx, b)?),
        Expr::Or(a, b) => Ok(arrow_row_matches(batch, schema, row_idx, a)?
            || arrow_row_matches(batch, schema, row_idx, b)?),
        Expr::Not(a) => Ok(!arrow_row_matches(batch, schema, row_idx, a)?),
        Expr::Eq(left, right) => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(c), Expr::Literal(Literal::String(v))) => {
                let i = match schema.index_of(c) {
                    Ok(i) => i,
                    Err(_) => return Ok(false),
                };
                let arr = batch.column(i);
                if arr.is_null(row_idx) {
                    return Ok(false);
                }
                if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                    return Ok(a.value(row_idx) == v);
                }
                if let Some(a) = arr.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                    if let Ok(u) = ulid::Ulid::from_string(v) {
                        let target = u.0.to_be_bytes();
                        return Ok(a.value(row_idx) == &target[..]);
                    }
                }
                Ok(false)
            }
            (Expr::Column(c), Expr::Literal(Literal::Int(v))) => {
                let i = match schema.index_of(c) {
                    Ok(i) => i,
                    Err(_) => return Ok(false),
                };
                let arr = batch.column(i);
                if arr.is_null(row_idx) {
                    return Ok(false);
                }
                if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                    return Ok(a.value(row_idx) == *v);
                }
                Ok(false)
            }
            _ => Ok(true), // unsupported pattern; conservative
        },
        Expr::TextMatch { column, query } => {
            let i = match schema.index_of(column) {
                Ok(i) => i,
                Err(_) => return Ok(false),
            };
            let arr = batch.column(i);
            if arr.is_null(row_idx) {
                return Ok(false);
            }
            if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                let s = a.value(row_idx);
                let needle = query.to_lowercase();
                return Ok(s.to_lowercase().contains(&needle));
            }
            Ok(false)
        }
        _ => Ok(true), // unsupported predicate types over WAL: conservative pass.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use chrono::Utc;
    use ulid::Ulid;
    use uuid::Uuid;

    use zen_catalog::{model::WalObjectRow, Catalog, SqliteCatalog};
    use zen_common::{CommitId, Schema, SchemaFingerprint, SpanId, SpanRecord, TraceId};
    use zen_compactor::compact_partition;
    use zen_memtable::flush_to_record_batch;
    use zen_storage::local_fs::InMemoryStore;
    use zen_wal::WalWriter;

    use crate::expr::Expr;
    use crate::logical::{LogicalPlan, Predicate, Projection};

    async fn setup_indexed_segment() -> (Arc<dyn Catalog>, Arc<dyn BlobStore>) {
        let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
        let catalog: Arc<dyn Catalog> = Arc::new(SqliteCatalog::open_in_memory().await.unwrap());
        catalog.ensure_tenant(TenantId(1), "t").await.unwrap();
        catalog.ensure_partition(TenantId(1), PartitionId(0)).await.unwrap();

        let mut rows = Vec::new();
        for t in 0..5u32 {
            let mut tid = [0u8; 16];
            tid[0..4].copy_from_slice(&t.to_be_bytes());
            for s in 0..10u32 {
                let mut sid = [0u8; 16];
                sid[0..4].copy_from_slice(&t.to_be_bytes());
                sid[4..8].copy_from_slice(&s.to_be_bytes());
                let mut r = SpanRecord::new(TenantId(1), PartitionId(0));
                r.trace_id = TraceId(tid);
                r.span_id = SpanId(sid);
                r.start_time_ms = 1000 + (t as i64) * 100 + s as i64;
                r.duration_ms = 50;
                r.model = Some(if s % 2 == 0 { "gpt-4o" } else { "haiku" }.into());
                r.status = Some(if s == 9 { "error" } else { "ok" }.into());
                r.prompt = Some(format!(
                    "trace {t} span {s}: {}",
                    if s == 7 { "out of memory" } else { "no error" }
                ));
                r.completion = Some(format!("response {s}"));
                r.commit_id = CommitId((t * 10 + s + 1) as u64);
                rows.push(r);
            }
        }
        let writer = WalWriter::new(store.clone());
        let batch = flush_to_record_batch(&rows).unwrap();
        let key = writer
            .flush(
                TenantId(1),
                PartitionId(0),
                CommitId(1),
                Schema::spans_v1().fingerprint(),
                &batch,
            )
            .await
            .unwrap();
        catalog
            .register_wal_object(WalObjectRow {
                wal_id: Uuid::from_u128(Ulid::new().0),
                tenant_id: TenantId(1),
                partition_id: PartitionId(0),
                object_key: key.to_string(),
                commit_id_min: CommitId(1),
                commit_id_max: CommitId(1),
                byte_count: 0,
                row_count: rows.len() as i64,
                schema_fingerprint: SchemaFingerprint(0),
                consumed_at: None,
                created_at: Utc::now(),
            })
            .await
            .unwrap();

        let _ = compact_partition(
            catalog.clone(),
            store.clone(),
            TenantId(1),
            PartitionId(0),
            "w",
            &Schema::spans_v1(),
        )
        .await
        .unwrap();
        (catalog, store)
    }

    #[tokio::test]
    async fn time_range_attr_filter_returns_correct_rows() {
        let (catalog, store) = setup_indexed_segment().await;
        let plan = LogicalPlan {
            tenant_id: 1,
            partition_ids: vec![0],
            projection: Projection::list(["span_id".into(), "model".into(), "status".into()]),
            predicate: Some(Predicate {
                expr: Expr::and(
                    Expr::eq(Expr::col("status"), Expr::lit_str("error")),
                    Expr::eq(Expr::col("model"), Expr::lit_str("haiku")),
                ),
            }),
            time_min_ms: 0,
            time_max_ms: i64::MAX,
            ..Default::default()
        };
        let rs = execute(&plan, catalog, store).await.unwrap();
        // s=9 is error and odd → haiku → 5 hits (one per trace).
        assert_eq!(rs.rows.len(), 5);
        for row in &rs.rows {
            assert_eq!(row.fields.get("status").unwrap(), "error");
            assert_eq!(row.fields.get("model").unwrap(), "haiku");
        }
    }

    #[tokio::test]
    async fn fts_text_match() {
        let (catalog, store) = setup_indexed_segment().await;
        let plan = LogicalPlan {
            tenant_id: 1,
            partition_ids: vec![0],
            projection: Projection::list(["span_id".into(), "prompt".into()]),
            predicate: Some(Predicate {
                expr: Expr::TextMatch {
                    column: "prompt".into(),
                    query: "out of memory".into(),
                },
            }),
            time_min_ms: 0,
            time_max_ms: i64::MAX,
            ..Default::default()
        };
        let rs = execute(&plan, catalog, store).await.unwrap();
        // s=7 has "out of memory" → 5 traces × 1 span = 5.
        assert_eq!(rs.rows.len(), 5);
    }

    #[tokio::test]
    async fn aggregation_count_by_model() {
        let (catalog, store) = setup_indexed_segment().await;
        let plan = LogicalPlan {
            tenant_id: 1,
            partition_ids: vec![0],
            projection: Projection::star(),
            predicate: None,
            time_min_ms: 0,
            time_max_ms: i64::MAX,
            group_by: vec!["model".into()],
            aggregates: vec![("count".into(), AggregateFn::Count)],
            ..Default::default()
        };
        let rs = execute(&plan, catalog, store).await.unwrap();
        assert_eq!(rs.rows.len(), 2);
        let total: i64 = rs
            .rows
            .iter()
            .filter_map(|r| r.fields.get("count").and_then(|v| v.as_i64()))
            .sum();
        assert_eq!(total, 50);
    }
}
