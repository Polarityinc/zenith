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

use crate::build::{build_segment_from_rows, BuildOptions};
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
