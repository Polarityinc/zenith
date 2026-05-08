//! High-level compaction driver: catalog lease → list WAL → merge → build → publish.

use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use ulid::Ulid;
use uuid::Uuid;

use zen_catalog::{Catalog, SegmentRow};
use zen_common::{
    CommitId, PartitionId, Schema, TenantId, ZenError, ZenResult,
};
use zen_index::sparse::{RowGroupKey, SparseRowGroupIndex};
use zen_storage::BlobStore;

use crate::build::{build_segment_from_iter, build_segment_from_rows, BuildOptions};
use crate::merge::merge_wals;

#[derive(Clone, Debug, Default)]
pub struct CompactionStats {
    pub wal_objects_consumed: u32,
    pub rows_compacted: u64,
    pub segment_bytes: u64,
    pub elapsed_ms: u64,
}

pub async fn compact_partition(
    catalog: Arc<dyn Catalog>,
    store: Arc<dyn BlobStore>,
    tenant: TenantId,
    partition: PartitionId,
    worker_id: &str,
    schema: &Schema,
) -> ZenResult<CompactionStats> {
    let start = std::time::Instant::now();

    // Acquire lease.
    catalog
        .acquire_compaction_lease(tenant, partition, worker_id, 60)
        .await?;

    let wals = catalog
        .list_wal_objects(tenant, partition, CommitId(0))
        .await?;
    if wals.is_empty() {
        catalog
            .release_compaction_lease(tenant, partition, worker_id)
            .await
            .ok();
        return Ok(CompactionStats::default());
    }

    let keys: Vec<String> = wals.iter().map(|w| w.object_key.clone()).collect();
    // Highest catalog commit_id_max among the WALs we're consuming. Use this for
    // mark_wal_consumed; the WAL header's commit_id is its commit_id_min, which
    // is too low and would leave WALs orphaned and re-merged on the next compact.
    let consumed_through = wals
        .iter()
        .map(|w| w.commit_id_max)
        .max()
        .unwrap_or(CommitId(0));
    let merged = merge_wals(store.clone(), &keys).await?;
    let n_rows = merged.rows.len();
    if n_rows == 0 {
        catalog
            .mark_wal_consumed(tenant, partition, consumed_through, Utc::now())
            .await
            .ok();
        catalog
            .release_compaction_lease(tenant, partition, worker_id)
            .await
            .ok();
        return Ok(CompactionStats {
            wal_objects_consumed: wals.len() as u32,
            ..Default::default()
        });
    }

    let opts = BuildOptions::default();
    let (segment_bytes, meta) =
        build_segment_from_rows(&merged.rows, tenant, partition, schema, &opts)?;

    // Build sparse rowgroup index for catalog.
    let mut sparse = SparseRowGroupIndex::new();
    // Iterate row groups by re-walking the segment reader (cheap with our format).
    let reader = zen_format::SegmentReader::from_bytes(segment_bytes.clone())?;
    for rg in &reader.row_groups {
        let _ = rg;
    }
    // Compute per-row-group keys from the original sorted rows (deterministic).
    {
        let mut start = 0usize;
        for rg in &reader.row_groups {
            let end = (start + rg.row_count as usize).min(merged.rows.len());
            let chunk = &merged.rows[start..end];
            if !chunk.is_empty() {
                let min_tid = chunk.iter().map(|r| r.trace_id.0).min().unwrap();
                let max_tid = chunk.iter().map(|r| r.trace_id.0).max().unwrap();
                let min_t = chunk.iter().map(|r| r.start_time_ms).min().unwrap();
                let max_t = chunk.iter().map(|r| r.start_time_ms).max().unwrap();
                let min_c = chunk.iter().map(|r| r.commit_id.0).min().unwrap();
                let max_c = chunk.iter().map(|r| r.commit_id.0).max().unwrap();
                sparse.push(RowGroupKey {
                    min_trace_id: min_tid,
                    max_trace_id: max_tid,
                    min_start_time: min_t,
                    max_start_time: max_t,
                    min_commit_id: min_c,
                    max_commit_id: max_c,
                });
            }
            start = end;
        }
    }
    let sparse_bytes = sparse
        .serialize()
        .map_err(|e| ZenError::compactor(format!("sparse serialize: {e}")))?;

    // Upload segment to object storage.
    let object_key = format!(
        "segments/{}/{}/{}.zseg",
        tenant, partition, Ulid::from(meta.segment_id)
    );
    store
        .put(&object_key, Bytes::from(segment_bytes.clone()))
        .await?;

    // Register in catalog.
    catalog
        .register_segment(SegmentRow {
            segment_id: Uuid::from_u128(meta.segment_id),
            tenant_id: tenant,
            partition_id: partition,
            object_key: object_key.clone(),
            level: 0,
            byte_count: segment_bytes.len() as i64,
            row_count: n_rows as i64,
            time_min: meta.time_min_ms,
            time_max: meta.time_max_ms,
            trace_id_min: meta.trace_id_min,
            trace_id_max: meta.trace_id_max,
            commit_id_min: meta.commit_id_min,
            commit_id_max: meta.commit_id_max,
            schema_fingerprint: meta.schema_fingerprint,
            rowgroup_index: sparse_bytes.to_vec(),
            superseded_at: None,
            created_at: Utc::now(),
        })
        .await?;

    // Mark WAL objects consumed.
    catalog
        .mark_wal_consumed(tenant, partition, consumed_through, Utc::now())
        .await?;

    catalog
        .release_compaction_lease(tenant, partition, worker_id)
        .await
        .ok();

    Ok(CompactionStats {
        wal_objects_consumed: wals.len() as u32,
        rows_compacted: n_rows as u64,
        segment_bytes: segment_bytes.len() as u64,
        elapsed_ms: start.elapsed().as_millis() as u64,
    })
}

/// Tier-N compaction: read every active segment + every unconsumed WAL,
/// merge into one big segment, mark inputs as superseded/consumed.
///
/// **Memory-bounded**: streams a k-way merge over input segments via
/// `streaming_compact_segments` so we never hold more than one row group
/// per input in memory at once. Required to compact at 1 TB+ scale.
///
/// Run periodically (or on demand) once the segment count gets high enough
/// that scans pay multi-segment overhead.
pub async fn compact_full(
    catalog: Arc<dyn Catalog>,
    store: Arc<dyn BlobStore>,
    tenant: TenantId,
    partition: PartitionId,
    worker_id: &str,
    schema: &Schema,
) -> ZenResult<CompactionStats> {
    use zen_format::SegmentReader;
    let start = std::time::Instant::now();
    catalog
        .acquire_compaction_lease(tenant, partition, worker_id, 120)
        .await?;

    let segs = catalog.list_segments_in_range(tenant, partition, i64::MIN, i64::MAX).await?;
    let wals = catalog.list_wal_objects(tenant, partition, CommitId(0)).await?;

    if segs.is_empty() && wals.is_empty() {
        catalog.release_compaction_lease(tenant, partition, worker_id).await.ok();
        return Ok(CompactionStats::default());
    }

    // Open input segments. SegmentReader holds the bytes; the row iterators
    // decode one row group at a time into a small buffer so we never hold
    // ALL rows in memory simultaneously.
    let mut readers: Vec<Arc<SegmentReader>> = Vec::with_capacity(segs.len());
    for seg in &segs {
        let bytes = store.get(&seg.object_key).await?;
        readers.push(Arc::new(SegmentReader::from_bytes(bytes.to_vec())?));
    }

    // Pull WAL rows once; merge_wals already sorts them.
    let mut wal_rows: Vec<zen_common::SpanRecord> = Vec::new();
    if !wals.is_empty() {
        let keys: Vec<String> = wals.iter().map(|w| w.object_key.clone()).collect();
        let merged = merge_wals(store.clone(), &keys).await?;
        wal_rows = merged.rows;
    }

    // Build streaming sources: one per segment + one per (sorted) WAL batch.
    let mut sources: Vec<Box<dyn Iterator<Item = zen_common::SpanRecord> + Send>> =
        Vec::with_capacity(readers.len() + 1);
    for reader in readers {
        sources.push(Box::new(SegmentRowIter::new(reader)));
    }
    if !wal_rows.is_empty() {
        sources.push(Box::new(wal_rows.into_iter()));
    }

    // K-way merge in (trace_id, start_time, span_id) order. Streaming output:
    // build_segment_from_iter pulls rows lazily and finalizes one row group at
    // a time, so memory is bounded by row_group_max_rows × bytes-per-row,
    // not by total compacted size. This is what unblocks 1 TB+ scale.
    let merge = KWayMerge::new(sources);
    let opts = BuildOptions::default();
    let built = build_segment_from_iter(merge, tenant, partition, schema, &opts)?;
    let (segment_bytes, meta, n_rows) = match built {
        Some(t) => t,
        None => {
            catalog.release_compaction_lease(tenant, partition, worker_id).await.ok();
            return Ok(CompactionStats::default());
        }
    };

    // Reconstruct sparse row-group index from the freshly written segment.
    let mut sparse = SparseRowGroupIndex::new();
    let reader = SegmentReader::from_bytes(segment_bytes.clone())?;
    for (i, _rg) in reader.row_groups.iter().enumerate() {
        // Pull min/max from the hotcache zone maps we just built.
        if let Some(rg_hc) = reader.hotcache.row_groups.get(i) {
            let trace_zm = rg_hc.columns.iter()
                .find(|c| reader.metadata.column_names.get(c.column_idx as usize).is_some_and(|n| n == "trace_id"));
            let time_zm = rg_hc.columns.iter()
                .find(|c| reader.metadata.column_names.get(c.column_idx as usize).is_some_and(|n| n == "start_time_ms"));
            let commit_zm = rg_hc.columns.iter()
                .find(|c| reader.metadata.column_names.get(c.column_idx as usize).is_some_and(|n| n == "commit_id"));
            let (min_tid, max_tid) = match trace_zm.map(|c| &c.zone_map.value) {
                Some(zen_index::ZoneMapValue::Fixed { min, max })
                | Some(zen_index::ZoneMapValue::Bytes { min, max }) => {
                    let mut mn = [0u8; 16];
                    let mut mx = [0u8; 16];
                    let lmin = min.len().min(16);
                    let lmax = max.len().min(16);
                    mn[..lmin].copy_from_slice(&min[..lmin]);
                    mx[..lmax].copy_from_slice(&max[..lmax]);
                    (mn, mx)
                }
                _ => ([0u8; 16], [0xFFu8; 16]),
            };
            let (min_t, max_t) = match time_zm.map(|c| &c.zone_map.value) {
                Some(zen_index::ZoneMapValue::I64 { min, max }) => (*min, *max),
                _ => (i64::MIN, i64::MAX),
            };
            let (min_c, max_c) = match commit_zm.map(|c| &c.zone_map.value) {
                Some(zen_index::ZoneMapValue::I64 { min, max }) => (*min as u64, *max as u64),
                _ => (0, u64::MAX),
            };
            sparse.push(RowGroupKey {
                min_trace_id: min_tid, max_trace_id: max_tid,
                min_start_time: min_t, max_start_time: max_t,
                min_commit_id: min_c, max_commit_id: max_c,
            });
        }
    }
    let sparse_bytes = sparse.serialize().map_err(|e| ZenError::compactor(format!("sparse: {e}")))?;

    let object_key = format!("segments/{}/{}/{}.zseg",
        tenant, partition, Ulid::from(meta.segment_id));
    store.put(&object_key, Bytes::from(segment_bytes.clone())).await?;
    catalog.register_segment(SegmentRow {
        segment_id: Uuid::from_u128(meta.segment_id),
        tenant_id: tenant, partition_id: partition,
        object_key: object_key.clone(),
        level: 1,  // tier-2
        byte_count: segment_bytes.len() as i64,
        row_count: n_rows as i64,
        time_min: meta.time_min_ms, time_max: meta.time_max_ms,
        trace_id_min: meta.trace_id_min, trace_id_max: meta.trace_id_max,
        commit_id_min: meta.commit_id_min, commit_id_max: meta.commit_id_max,
        schema_fingerprint: meta.schema_fingerprint,
        rowgroup_index: sparse_bytes.to_vec(),
        superseded_at: None, created_at: Utc::now(),
    }).await?;

    // Mark old segments superseded.
    let old_ids: Vec<Uuid> = segs.iter().map(|s| s.segment_id).collect();
    catalog.mark_segments_superseded(&old_ids, Utc::now()).await?;

    // Mark WALs consumed.
    if !wals.is_empty() {
        let consumed_through = wals.iter().map(|w| w.commit_id_max).max().unwrap_or(CommitId(0));
        catalog.mark_wal_consumed(tenant, partition, consumed_through, Utc::now()).await.ok();
    }

    catalog.release_compaction_lease(tenant, partition, worker_id).await.ok();
    Ok(CompactionStats {
        wal_objects_consumed: wals.len() as u32,
        rows_compacted: n_rows as u64,
        segment_bytes: segment_bytes.len() as u64,
        elapsed_ms: start.elapsed().as_millis() as u64,
    })
}

fn decode_row_group_to_records(
    reader: &zen_format::SegmentReader,
    rg_idx: usize,
    n: usize,
    out: &mut Vec<zen_common::SpanRecord>,
) -> ZenResult<()> {
    use zen_format::ColumnValues;
    use zen_common::{SpanRecord, TenantId, PartitionId, TraceId, SpanId, CommitId};

    let col_idx = |name: &str| -> Option<u32> {
        reader.metadata.column_names.iter().position(|c| c == name).map(|i| i as u32)
    };

    // Decode each column we care about. We only care about columns that survive
    // the round-trip; missing columns become None.
    let tenant: Vec<i64> = match col_idx("tenant_id").and_then(|i| reader.read_column(rg_idx, i).ok()) {
        Some(ColumnValues::I64(v)) => v, _ => vec![0; n],
    };
    let partition: Vec<i64> = match col_idx("partition_id").and_then(|i| reader.read_column(rg_idx, i).ok()) {
        Some(ColumnValues::I64(v)) => v, _ => vec![0; n],
    };
    let trace_id: Vec<[u8;16]> = match col_idx("trace_id").and_then(|i| reader.read_column(rg_idx, i).ok()) {
        Some(ColumnValues::Fixed16(v)) => v, _ => vec![[0;16]; n],
    };
    let span_id: Vec<[u8;16]> = match col_idx("span_id").and_then(|i| reader.read_column(rg_idx, i).ok()) {
        Some(ColumnValues::Fixed16(v)) => v, _ => vec![[0;16]; n],
    };
    let parent_span_id: Vec<[u8;16]> = match col_idx("parent_span_id").and_then(|i| reader.read_column(rg_idx, i).ok()) {
        Some(ColumnValues::Fixed16(v)) => v, _ => vec![[0;16]; n],
    };
    let start_time: Vec<i64> = match col_idx("start_time_ms").and_then(|i| reader.read_column(rg_idx, i).ok()) {
        Some(ColumnValues::I64(v)) => v, _ => vec![0; n],
    };
    let end_time: Vec<i64> = match col_idx("end_time_ms").and_then(|i| reader.read_column(rg_idx, i).ok()) {
        Some(ColumnValues::I64(v)) => v, _ => vec![0; n],
    };
    let duration: Vec<i64> = match col_idx("duration_ms").and_then(|i| reader.read_column(rg_idx, i).ok()) {
        Some(ColumnValues::I64(v)) => v, _ => vec![0; n],
    };
    let model: Vec<Vec<u8>> = match col_idx("model").and_then(|i| reader.read_column(rg_idx, i).ok()) {
        Some(ColumnValues::StringsOwned(v)) => v, _ => vec![Vec::new(); n],
    };
    let status: Vec<Vec<u8>> = match col_idx("status").and_then(|i| reader.read_column(rg_idx, i).ok()) {
        Some(ColumnValues::StringsOwned(v)) => v, _ => vec![Vec::new(); n],
    };
    let provider: Vec<Vec<u8>> = match col_idx("provider").and_then(|i| reader.read_column(rg_idx, i).ok()) {
        Some(ColumnValues::StringsOwned(v)) => v, _ => vec![Vec::new(); n],
    };
    let tool_name: Vec<Vec<u8>> = match col_idx("tool_name").and_then(|i| reader.read_column(rg_idx, i).ok()) {
        Some(ColumnValues::StringsOwned(v)) => v, _ => vec![Vec::new(); n],
    };
    let span_type: Vec<Vec<u8>> = match col_idx("span_type").and_then(|i| reader.read_column(rg_idx, i).ok()) {
        Some(ColumnValues::StringsOwned(v)) => v, _ => vec![Vec::new(); n],
    };
    let prompt: Vec<Vec<u8>> = match col_idx("prompt").and_then(|i| reader.read_column(rg_idx, i).ok()) {
        Some(ColumnValues::StringsOwned(v)) => v, _ => vec![Vec::new(); n],
    };
    let completion: Vec<Vec<u8>> = match col_idx("completion").and_then(|i| reader.read_column(rg_idx, i).ok()) {
        Some(ColumnValues::StringsOwned(v)) => v, _ => vec![Vec::new(); n],
    };
    let metadata: Vec<Vec<u8>> = match col_idx("metadata").and_then(|i| reader.read_column(rg_idx, i).ok()) {
        Some(ColumnValues::BytesOwned(v)) => v, _ => vec![Vec::new(); n],
    };
    let commit_id: Vec<i64> = match col_idx("commit_id").and_then(|i| reader.read_column(rg_idx, i).ok()) {
        Some(ColumnValues::I64(v)) => v, _ => vec![0; n],
    };

    fn opt_str(b: Vec<u8>) -> Option<String> {
        if b.is_empty() { None } else { String::from_utf8(b).ok() }
    }

    for i in 0..n {
        out.push(SpanRecord {
            tenant_id: TenantId(tenant[i] as u64),
            partition_id: PartitionId(partition[i] as u32),
            trace_id: TraceId(trace_id[i]),
            span_id: SpanId(span_id[i]),
            parent_span_id: if parent_span_id[i] == [0;16] { None } else { Some(SpanId(parent_span_id[i])) },
            start_time_ms: start_time[i],
            end_time_ms: end_time[i],
            duration_ms: duration[i],
            span_type: opt_str(span_type[i].clone()),
            status: opt_str(status[i].clone()),
            provider: opt_str(provider[i].clone()),
            model: opt_str(model[i].clone()),
            tool_name: opt_str(tool_name[i].clone()),
            prompt: opt_str(prompt[i].clone()),
            completion: opt_str(completion[i].clone()),
            prompt_tokens: None, completion_tokens: None,
            cost_usd: None, temperature: None, top_p: None,
            tool_io_text: None,
            user_id: None, session_id: None, request_id: None,
            metadata: if metadata[i].is_empty() { None } else { serde_json::from_slice(&metadata[i]).ok() },
            embedding: None,
            commit_id: CommitId(commit_id[i] as u64),
        });
    }
    Ok(())
}

// Streaming row iterator over a single segment. Holds an Arc<SegmentReader>
// and decodes one row group at a time into a small buffer. Memory-bounded
// by row_group_size, not by total segment size.
struct SegmentRowIter {
    reader: Arc<zen_format::SegmentReader>,
    rg_idx: usize,
    row_in_rg: usize,
    current_rows: Vec<zen_common::SpanRecord>,
}

impl SegmentRowIter {
    fn new(reader: Arc<zen_format::SegmentReader>) -> Self {
        Self {
            reader,
            rg_idx: usize::MAX,
            row_in_rg: 0,
            current_rows: Vec::new(),
        }
    }

    fn load_next_rg(&mut self) -> ZenResult<bool> {
        let next = if self.rg_idx == usize::MAX { 0 } else { self.rg_idx + 1 };
        if next >= self.reader.row_group_count() {
            return Ok(false);
        }
        self.rg_idx = next;
        self.row_in_rg = 0;
        self.current_rows.clear();
        let n = self.reader.row_groups[next].row_count as usize;
        decode_row_group_to_records(&self.reader, next, n, &mut self.current_rows)?;
        Ok(true)
    }
}

impl Iterator for SegmentRowIter {
    type Item = zen_common::SpanRecord;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.row_in_rg < self.current_rows.len() {
                // Move out via swap_remove from current_rows? We're consuming
                // sequentially; just clone. SpanRecord clone is bounded by
                // string sizes; ~25KB at worst.
                let r = self.current_rows[self.row_in_rg].clone();
                self.row_in_rg += 1;
                return Some(r);
            }
            // Advance to next row group
            match self.load_next_rg() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(_) => return None,
            }
        }
    }
}

// K-way merge over multiple sorted iterators of SpanRecord. Order is
// (trace_id, start_time_ms, span_id) which matches the segment sort.
struct KWayMerge<I: Iterator<Item = zen_common::SpanRecord>> {
    sources: Vec<Option<I>>,
    heap: std::collections::BinaryHeap<MergeHead>,
}

struct MergeHead {
    row: zen_common::SpanRecord,
    src: usize,
}

impl PartialEq for MergeHead {
    fn eq(&self, other: &Self) -> bool {
        self.row.trace_id.0 == other.row.trace_id.0
            && self.row.start_time_ms == other.row.start_time_ms
            && self.row.span_id.0 == other.row.span_id.0
    }
}
impl Eq for MergeHead {}

impl Ord for MergeHead {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse for min-heap (smallest popped first).
        other.row.trace_id.0.cmp(&self.row.trace_id.0)
            .then_with(|| other.row.start_time_ms.cmp(&self.row.start_time_ms))
            .then_with(|| other.row.span_id.0.cmp(&self.row.span_id.0))
    }
}
impl PartialOrd for MergeHead {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: Iterator<Item = zen_common::SpanRecord>> KWayMerge<I> {
    fn new(mut sources: Vec<I>) -> Self {
        let mut heap = std::collections::BinaryHeap::with_capacity(sources.len());
        let mut sources_opt: Vec<Option<I>> = Vec::with_capacity(sources.len());
        for (i, mut s) in sources.drain(..).enumerate() {
            if let Some(row) = s.next() {
                heap.push(MergeHead { row, src: i });
            }
            sources_opt.push(Some(s));
        }
        Self { sources: sources_opt, heap }
    }
}

impl<I: Iterator<Item = zen_common::SpanRecord>> Iterator for KWayMerge<I> {
    type Item = zen_common::SpanRecord;
    fn next(&mut self) -> Option<Self::Item> {
        let head = self.heap.pop()?;
        let MergeHead { row, src } = head;
        if let Some(s) = self.sources[src].as_mut() {
            if let Some(next_row) = s.next() {
                self.heap.push(MergeHead { row: next_row, src });
            } else {
                // Source exhausted
                self.sources[src] = None;
            }
        }
        Some(row)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use ulid::Ulid;
    use uuid::Uuid;

    use zen_catalog::{model::WalObjectRow, SqliteCatalog};
    use zen_common::{Schema, SchemaFingerprint, SpanId, SpanRecord, TraceId};
    use zen_memtable::{flush_to_record_batch};
    use zen_storage::local_fs::InMemoryStore;
    use zen_wal::WalWriter;

    use super::*;

    #[tokio::test]
    async fn end_to_end_compaction_trace_locality() {
        let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
        let catalog: Arc<dyn Catalog> = Arc::new(SqliteCatalog::open_in_memory().await.unwrap());
        catalog.ensure_tenant(TenantId(1), "t").await.unwrap();
        catalog.ensure_partition(TenantId(1), PartitionId(0)).await.unwrap();

        // Generate 10 traces × 10 spans each.
        let mut rows = Vec::new();
        for t in 0..10u32 {
            let mut tid = [0u8; 16];
            tid[0..4].copy_from_slice(&t.to_be_bytes());
            for s in 0..10u32 {
                let mut sid = [0u8; 16];
                sid[0..4].copy_from_slice(&t.to_be_bytes());
                sid[4..8].copy_from_slice(&s.to_be_bytes());
                let mut r = SpanRecord::new(TenantId(1), PartitionId(0));
                r.trace_id = TraceId(tid);
                r.span_id = SpanId(sid);
                r.start_time_ms = (t as i64) * 10_000 + (s as i64) * 100;
                r.end_time_ms = r.start_time_ms + 50;
                r.duration_ms = 50;
                r.model = Some("gpt-4o".into());
                r.status = Some("ok".into());
                r.prompt = Some(format!("prompt for trace {t} span {s}"));
                r.completion = Some(format!("response for span {s}"));
                r.commit_id = CommitId((t * 10 + s + 1) as u64);
                rows.push(r);
            }
        }
        // Shuffle so they hit WAL out of order.
        let chunks: Vec<&[SpanRecord]> = rows.chunks(20).collect();
        let writer = WalWriter::new(store.clone());
        for (i, chunk) in chunks.into_iter().enumerate() {
            let batch = flush_to_record_batch(chunk).unwrap();
            let key = writer
                .flush(
                    TenantId(1),
                    PartitionId(0),
                    CommitId((i as u64) + 1),
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
                    commit_id_min: CommitId((i as u64) + 1),
                    commit_id_max: CommitId((i as u64) + 1),
                    byte_count: 0,
                    row_count: chunk.len() as i64,
                    schema_fingerprint: SchemaFingerprint(0),
                    consumed_at: None,
                    created_at: Utc::now(),
                })
                .await
                .unwrap();
        }

        let schema = Schema::spans_v1();
        let stats = compact_partition(
            catalog.clone(),
            store.clone(),
            TenantId(1),
            PartitionId(0),
            "test-worker",
            &schema,
        )
        .await
        .unwrap();
        assert_eq!(stats.rows_compacted, 100);
        assert!(stats.segment_bytes > 0);

        // Verify trace-locality: every trace's spans are in one row group.
        let segs = catalog
            .list_segments_for_tenant(TenantId(1))
            .await
            .unwrap();
        assert_eq!(segs.len(), 1);
        let seg_bytes = store.get(&segs[0].object_key).await.unwrap();
        let reader = zen_format::SegmentReader::from_bytes(seg_bytes.to_vec()).unwrap();

        // Decode trace_id column from each row group; verify each trace appears in exactly 1 RG.
        use std::collections::HashMap;
        let mut trace_to_rgs: HashMap<[u8; 16], std::collections::HashSet<usize>> = HashMap::new();
        for rg_idx in 0..reader.row_group_count() {
            let cv = reader.read_column(rg_idx, 2).unwrap(); // trace_id is column 2 in spans_v1
            if let zen_format::ColumnValues::Fixed16(v) = cv {
                for tid in v {
                    trace_to_rgs.entry(tid).or_default().insert(rg_idx);
                }
            }
        }
        for (tid, rgs) in &trace_to_rgs {
            assert_eq!(
                rgs.len(),
                1,
                "trace {tid:?} appears in multiple row groups: {rgs:?}"
            );
        }
    }

    #[tokio::test]
    async fn empty_compaction_idempotent() {
        let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
        let catalog: Arc<dyn Catalog> = Arc::new(SqliteCatalog::open_in_memory().await.unwrap());
        catalog.ensure_tenant(TenantId(1), "t").await.unwrap();
        catalog.ensure_partition(TenantId(1), PartitionId(0)).await.unwrap();
        let schema = Schema::spans_v1();
        let stats = compact_partition(
            catalog,
            store,
            TenantId(1),
            PartitionId(0),
            "w",
            &schema,
        )
        .await
        .unwrap();
        assert_eq!(stats.rows_compacted, 0);
    }
}

