//! End-to-end integration test: ingest → compact → query against an in-process server.

use std::sync::Arc;

use chrono::Utc;
use tokio::net::TcpListener;
use uuid::Uuid;

use zen_catalog::{model::WalObjectRow, Catalog, MockCatalog};
use zen_common::{
    CommitId, Config, PartitionId, Schema, SchemaFingerprint, SpanId, SpanRecord, TenantId, TraceId,
};
use zen_compactor::compact_partition;
use zen_memtable::flush_to_record_batch;
use zen_server::{http::router, ServerState};
use zen_storage::{local_fs::InMemoryStore, BlobStore};
use zen_wal::WalWriter;

#[tokio::test]
async fn ingest_compact_query_end_to_end() {
    let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
    let catalog: Arc<dyn Catalog> = Arc::new(MockCatalog::new());
    catalog.ensure_tenant(TenantId(1), "t").await.unwrap();
    catalog
        .ensure_partition(TenantId(1), PartitionId(0))
        .await
        .unwrap();

    // Ingest 200 spans across 20 traces.
    let mut rows = Vec::new();
    for t in 0..20u32 {
        let mut tid = [0u8; 16];
        tid[0..4].copy_from_slice(&t.to_be_bytes());
        for s in 0..10u32 {
            let mut sid = [0u8; 16];
            sid[0..4].copy_from_slice(&t.to_be_bytes());
            sid[4..8].copy_from_slice(&s.to_be_bytes());
            let mut r = SpanRecord::new(TenantId(1), PartitionId(0));
            r.trace_id = TraceId(tid);
            r.span_id = SpanId(sid);
            r.start_time_ms = 1_000 + (t as i64) * 100 + s as i64;
            r.duration_ms = 50;
            r.model = Some(if s % 2 == 0 { "gpt-4o" } else { "haiku" }.into());
            r.status = Some("ok".into());
            r.prompt = Some(format!("trace {t} span {s}"));
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
            wal_id: Uuid::new_v4(),
            tenant_id: TenantId(1),
            partition_id: PartitionId(0),
            object_key: key.to_string(),
            commit_id_min: CommitId(1),
            commit_id_max: CommitId(1),
            byte_count: 0,
            row_count: rows.len() as i64,
            time_min: 0,
            time_max: 0,
            trace_id_min: TraceId([0u8; 16]),
            trace_id_max: TraceId([0xff; 16]),
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
        "test-worker",
        &Schema::spans_v1(),
    )
    .await
    .unwrap();

    // Spin up the server.
    let cfg = Config::default();
    let state = ServerState::new(cfg, catalog.clone(), store.clone());
    let app = router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    // Allow server to bind.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/query", addr);
    let body = serde_json::json!({
        "tenant_id": 1,
        "query": "SELECT model, count(*) FROM spans GROUP BY model"
    });
    let r: serde_json::Value = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let rows = r["result"]["rows"].as_array().unwrap();
    let mut total = 0i64;
    for row in rows {
        let count = row["fields"]["count"].as_i64().unwrap();
        total += count;
    }
    assert_eq!(total, 200);
    assert_eq!(rows.len(), 2); // gpt-4o + haiku
}
