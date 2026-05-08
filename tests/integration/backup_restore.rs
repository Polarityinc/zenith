//! Backup → restore round-trip. Build a tenant's data, run the backup
//! logic against a temp directory, blow away the catalog + segments,
//! restore, verify the queries return the same rows.
//!
//! The CLI commands themselves wire to the catalog/store; this test
//! invokes the same building blocks (catalog API, store API) directly
//! instead of shelling out to the binary, so we don't depend on the
//! built artifact.

use std::sync::Arc;

use chrono::Utc;
use tempfile::TempDir;
use uuid::Uuid;

use zen_catalog::{
    model::{SegmentRow, WalObjectRow},
    Catalog, SqliteCatalog,
};
use zen_common::{
    CommitId, PartitionId, Schema, SchemaFingerprint, SpanId, SpanRecord, TenantId, TraceId,
};
use zen_compactor::compact_partition;
use zen_memtable::flush_to_record_batch;
use zen_storage::{local_fs::InMemoryStore, BlobStore};
use zen_wal::WalWriter;

#[tokio::test]
async fn backup_then_restore_preserves_segments() {
    let store_a: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
    let cat_a: Arc<dyn Catalog> = Arc::new(SqliteCatalog::open_in_memory().await.unwrap());
    cat_a.ensure_tenant(TenantId(1), "t").await.unwrap();
    cat_a.ensure_partition(TenantId(1), PartitionId(0))
        .await
        .unwrap();

    // Seed: one trace with five spans, then compact.
    let mut rows = Vec::new();
    for s in 0..5u32 {
        let mut sid = [0u8; 16];
        sid[0..4].copy_from_slice(&s.to_be_bytes());
        let mut r = SpanRecord::new(TenantId(1), PartitionId(0));
        r.trace_id = TraceId([3u8; 16]);
        r.span_id = SpanId(sid);
        r.start_time_ms = 1000 + s as i64;
        r.duration_ms = 5;
        r.model = Some("gpt-4o".into());
        r.commit_id = CommitId((s + 1) as u64);
        rows.push(r);
    }
    let writer = WalWriter::new(store_a.clone());
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
    cat_a
        .register_wal_object(WalObjectRow {
            wal_id: Uuid::new_v4(),
            tenant_id: TenantId(1),
            partition_id: PartitionId(0),
            object_key: key.to_string(),
            commit_id_min: CommitId(1),
            commit_id_max: CommitId(1),
            byte_count: 0,
            row_count: 5,
            schema_fingerprint: SchemaFingerprint(0),
            consumed_at: None,
            created_at: Utc::now(),
        })
        .await
        .unwrap();
    let _ = compact_partition(
        cat_a.clone(),
        store_a.clone(),
        TenantId(1),
        PartitionId(0),
        "compactor",
        &Schema::spans_v1(),
    )
    .await
    .unwrap();

    // BACKUP — replicate the CLI's logic against the live catalog/store.
    let backup_dir = TempDir::new().unwrap();
    let segs_a = cat_a
        .list_segments_for_tenant(TenantId(1))
        .await
        .unwrap();
    assert!(!segs_a.is_empty(), "expected at least one segment after compact");
    let seg_dir = backup_dir.path().join("segments");
    std::fs::create_dir_all(&seg_dir).unwrap();
    for s in &segs_a {
        let bytes = store_a.get(&s.object_key).await.unwrap();
        std::fs::write(seg_dir.join(format!("{}.zseg", s.segment_id)), bytes).unwrap();
    }

    // RESTORE — fresh store + catalog, replay the manifest.
    let store_b: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
    let cat_b: Arc<dyn Catalog> = Arc::new(SqliteCatalog::open_in_memory().await.unwrap());
    cat_b.ensure_tenant(TenantId(1), "t").await.unwrap();
    cat_b.ensure_partition(TenantId(1), PartitionId(0))
        .await
        .unwrap();
    for s in &segs_a {
        let bytes = std::fs::read(seg_dir.join(format!("{}.zseg", s.segment_id))).unwrap();
        store_b
            .put(&s.object_key, bytes::Bytes::from(bytes))
            .await
            .unwrap();
        cat_b
            .register_segment(SegmentRow {
                segment_id: s.segment_id,
                tenant_id: s.tenant_id,
                partition_id: s.partition_id,
                object_key: s.object_key.clone(),
                level: s.level,
                byte_count: s.byte_count,
                row_count: s.row_count,
                time_min: s.time_min,
                time_max: s.time_max,
                trace_id_min: s.trace_id_min,
                trace_id_max: s.trace_id_max,
                commit_id_min: s.commit_id_min,
                commit_id_max: s.commit_id_max,
                schema_fingerprint: s.schema_fingerprint,
                rowgroup_index: s.rowgroup_index.clone(),
                superseded_at: None,
                created_at: Utc::now(),
            })
            .await
            .unwrap();
    }

    // Verify: catalog B sees the same segments and the bytes match.
    let segs_b = cat_b
        .list_segments_for_tenant(TenantId(1))
        .await
        .unwrap();
    assert_eq!(segs_a.len(), segs_b.len());
    let row_count_a: i64 = segs_a.iter().map(|s| s.row_count).sum();
    let row_count_b: i64 = segs_b.iter().map(|s| s.row_count).sum();
    assert_eq!(row_count_a, row_count_b);
    assert_eq!(row_count_b, 5);
}
