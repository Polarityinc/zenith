//! Smoke test: verify /v1/metrics renders after a query.

use std::sync::Arc;

use chrono::Utc;
use tokio::net::TcpListener;
use uuid::Uuid;

use zen_catalog::{model::WalObjectRow, Catalog, SqliteCatalog};
use zen_common::{
    CommitId, Config, PartitionId, Schema, SchemaFingerprint, SpanId, SpanRecord, TenantId, TraceId,
};
use zen_compactor::compact_partition;
use zen_memtable::flush_to_record_batch;
use zen_server::{http::router, ServerState};
use zen_storage::{local_fs::InMemoryStore, BlobStore};
use zen_wal::WalWriter;

#[tokio::test]
async fn metrics_endpoint_renders_query_observations() {
    zen_server::metrics::init();

    let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
    let catalog: Arc<dyn Catalog> = Arc::new(SqliteCatalog::open_in_memory().await.unwrap());
    catalog.ensure_tenant(TenantId(1), "t").await.unwrap();
    catalog
        .ensure_partition(TenantId(1), PartitionId(0))
        .await
        .unwrap();

    // One trace, one span — minimal corpus.
    let mut r = SpanRecord::new(TenantId(1), PartitionId(0));
    r.trace_id = TraceId([1u8; 16]);
    r.span_id = SpanId([2u8; 16]);
    r.start_time_ms = 1000;
    r.duration_ms = 5;
    r.model = Some("gpt-4o".into());
    r.commit_id = CommitId(1);

    let writer = WalWriter::new(store.clone());
    let batch = flush_to_record_batch(&[r]).unwrap();
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
            row_count: 1,
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
        "compactor",
        &Schema::spans_v1(),
    )
    .await
    .unwrap();

    let state = ServerState::new(Config::default(), catalog, store);
    let app = router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    // Issue a query so the histogram has at least one observation.
    let _ = client
        .post(format!("http://{addr}/v1/query"))
        .json(&serde_json::json!({
            "tenant_id": 1,
            "query": "SELECT count(*) FROM spans"
        }))
        .send()
        .await
        .unwrap()
        .text()
        .await;

    // Scrape /v1/metrics and assert our histogram name appears.
    let body = client
        .get(format!("http://{addr}/v1/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        body.contains("zen_query_duration_seconds"),
        "expected histogram in /v1/metrics, got:\n{body}"
    );
    assert!(
        body.contains("zen_queries_total"),
        "expected counter in /v1/metrics, got:\n{body}"
    );
}
