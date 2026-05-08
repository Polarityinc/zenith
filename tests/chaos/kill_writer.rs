//! Chaos: simulate a writer crash mid-flush. The WAL is durable
//! (Phase A fixed this); on next compactor run the rows must reappear
//! in the active segments.

use std::sync::Arc;

use chrono::Utc;
use uuid::Uuid;

use zen_catalog::{model::WalObjectRow, Catalog, MockCatalog};
use zen_common::{
    CommitId, PartitionId, Schema, SchemaFingerprint, SpanId, SpanRecord, TenantId, TraceId,
};
use zen_compactor::compact_partition;
use zen_memtable::flush_to_record_batch;
use zen_storage::{local_fs::InMemoryStore, BlobStore};
use zen_wal::WalWriter;

#[tokio::test]
async fn wal_durable_then_compact_recovers_rows() {
    let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
    let catalog: Arc<dyn Catalog> = Arc::new(MockCatalog::new());
    catalog.ensure_tenant(TenantId(1), "t").await.unwrap();
    catalog
        .ensure_partition(TenantId(1), PartitionId(0))
        .await
        .unwrap();

    // Write 3 batches to WAL — pretending each is a separate "process
    // lifetime" terminated by a crash before compaction.
    for batch_idx in 0..3u32 {
        let mut rows = Vec::new();
        for s in 0..4u32 {
            let mut sid = [0u8; 16];
            sid[0..4].copy_from_slice(&batch_idx.to_be_bytes());
            sid[4..8].copy_from_slice(&s.to_be_bytes());
            let mut r = SpanRecord::new(TenantId(1), PartitionId(0));
            r.trace_id = TraceId([0xab; 16]);
            r.span_id = SpanId(sid);
            r.start_time_ms = 1_000 + (batch_idx as i64) * 100 + s as i64;
            r.duration_ms = 5;
            r.commit_id = CommitId((batch_idx * 100 + s + 1) as u64);
            rows.push(r);
        }
        let writer = WalWriter::new(store.clone());
        let batch = flush_to_record_batch(&rows).unwrap();
        let cid = CommitId((batch_idx * 100 + 1) as u64);
        let key = writer
            .flush(
                TenantId(1),
                PartitionId(0),
                cid,
                Schema::spans_v1().fingerprint(),
                &batch,
            )
            .await
            .unwrap();
        catalog
            .register_wal_object(WalObjectRow {
                wal_id: Uuid::new_v4(),
                tenant_id: TenantId(1),
                partition_id: PartitionId(0),
                object_key: key.to_string(),
                commit_id_min: cid,
                commit_id_max: CommitId(cid.0 + 3),
                byte_count: 0,
                row_count: 4,
                schema_fingerprint: SchemaFingerprint(0),
                consumed_at: None,
                created_at: Utc::now(),
            })
            .await
            .unwrap();
        // "Crash": no flush, no compact. Just drop the writer and move on.
    }

    // Recovery path: a new compactor instance picks up all unconsumed
    // WAL objects and produces segments containing every row.
    let stats = compact_partition(
        catalog.clone(),
        store.clone(),
        TenantId(1),
        PartitionId(0),
        "recovery-worker",
        &Schema::spans_v1(),
    )
    .await
    .unwrap();
    assert_eq!(stats.rows_compacted, 12, "expected all 12 rows recovered");
    let segs = catalog.list_segments_for_tenant(TenantId(1)).await.unwrap();
    assert!(!segs.is_empty(), "compactor should have produced a segment");
    let total_rows: i64 = segs.iter().map(|s| s.row_count).sum();
    assert_eq!(total_rows, 12);
}
