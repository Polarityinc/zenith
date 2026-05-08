//! Multi-node integration test.
//!
//! Spins up 3 in-process zenithdb HTTP servers backed by a shared
//! in-memory blob store and a shared sqlite catalog. Verifies:
//!
//! 1. All 3 nodes register themselves in the catalog and converge on the
//!    same shard map.
//! 2. A query issued to *any* node returns identical results — the
//!    coordinator may either execute locally or forward to the primary
//!    replica based on the rendezvous-hash routing.
//!
//! This is the smallest end-to-end proof that the cluster wiring works:
//! catalog -> heartbeat -> shard map -> router -> remote forwarding ->
//! local execution -> JSON response.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::net::TcpListener;
use uuid::Uuid;

use zen_catalog::{model::WalObjectRow, Catalog, MockCatalog};
use zen_cluster::{NodeId, NodeRegistry, NodeRole};
use zen_common::{
    CommitId, Config, PartitionId, Schema, SchemaFingerprint, SpanId, SpanRecord, TenantId, TraceId,
};
use zen_compactor::compact_partition;
use zen_memtable::flush_to_record_batch;
use zen_server::{http::router, ServerState};
use zen_storage::{local_fs::InMemoryStore, BlobStore};
use zen_wal::WalWriter;

async fn seed_data(catalog: Arc<dyn Catalog>, store: Arc<dyn BlobStore>) {
    catalog.ensure_tenant(TenantId(1), "t").await.unwrap();
    catalog
        .ensure_partition(TenantId(1), PartitionId(0))
        .await
        .unwrap();
    let mut rows = Vec::new();
    for t in 0..10u32 {
        let mut tid = [0u8; 16];
        tid[0..4].copy_from_slice(&t.to_be_bytes());
        for s in 0..5u32 {
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
}

async fn spawn_node(
    catalog: Arc<dyn Catalog>,
    store: Arc<dyn BlobStore>,
) -> (String, NodeRegistry) {
    spawn_node_with_secret(catalog, store, "").await
}

async fn spawn_node_with_secret(
    catalog: Arc<dyn Catalog>,
    store: Arc<dyn BlobStore>,
    internal_secret: &str,
) -> (String, NodeRegistry) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let endpoint = format!("http://{}", addr);

    let reg = NodeRegistry::new(
        NodeId::new(),
        endpoint.clone(),
        NodeRole::All,
        "*".into(),
        catalog.clone(),
        /* replication_factor = */ 2,
        /* heartbeat_ttl_ms   = */ 5_000,
    );

    let mut cfg = Config::default();
    cfg.auth.internal_secret = internal_secret.to_string();

    let state = ServerState::new(cfg, catalog, store).with_cluster(reg.clone());
    let app = router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (endpoint, reg)
}

#[tokio::test]
async fn three_node_cluster_serves_queries_from_any_node() {
    let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
    let catalog: Arc<dyn Catalog> = Arc::new(MockCatalog::new());

    seed_data(catalog.clone(), store.clone()).await;

    let (a_url, a_reg) = spawn_node(catalog.clone(), store.clone()).await;
    let (b_url, b_reg) = spawn_node(catalog.clone(), store.clone()).await;
    let (c_url, c_reg) = spawn_node(catalog.clone(), store.clone()).await;

    // Drive each registry once so they all heartbeat + refresh the shard map.
    a_reg.tick().await.unwrap();
    b_reg.tick().await.unwrap();
    c_reg.tick().await.unwrap();
    // One more tick so each node sees all three heartbeats.
    a_reg.tick().await.unwrap();
    b_reg.tick().await.unwrap();
    c_reg.tick().await.unwrap();

    // Each ShardMap should now contain all three alive nodes.
    let m = a_reg.shard_map();
    assert_eq!(m.nodes().len(), 3);
    let alive = m.all_alive_workers(a_reg.now_ms()).len();
    assert_eq!(alive, 3, "expected 3 alive workers, got {alive}");

    // Wait briefly so the listeners are accepting (random ports + spawn).
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Issue the same query through every node. All must return the same
    // total — proving that routing decisions (Local vs Remote) are
    // transparent to clients.
    let client = reqwest::Client::new();
    let body = serde_json::json!({
        "tenant_id": 1,
        "query": "SELECT model, count(*) FROM spans GROUP BY model"
    });

    let mut totals: Vec<i64> = Vec::new();
    for url in [&a_url, &b_url, &c_url] {
        let r: serde_json::Value = client
            .post(format!("{}/v1/query", url))
            .json(&body)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let rows = r["result"]["rows"].as_array().unwrap();
        let total: i64 = rows
            .iter()
            .map(|row| row["fields"]["count"].as_i64().unwrap_or(0))
            .sum();
        totals.push(total);
    }
    assert!(totals.iter().all(|&t| t == 50), "totals = {totals:?}");
}

/// Same scenario, but with HMAC enforced on `/v1/internal/*`. Each node
/// signs its inter-node calls with the shared secret; verification is
/// transparent to clients.
#[tokio::test]
async fn three_node_cluster_with_hmac_inter_node_auth() {
    let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
    let catalog: Arc<dyn Catalog> = Arc::new(MockCatalog::new());

    seed_data(catalog.clone(), store.clone()).await;

    let secret = "shared-cluster-hmac-secret-test";
    let (a_url, a_reg) = spawn_node_with_secret(catalog.clone(), store.clone(), secret).await;
    let (b_url, b_reg) = spawn_node_with_secret(catalog.clone(), store.clone(), secret).await;
    let (c_url, c_reg) = spawn_node_with_secret(catalog.clone(), store.clone(), secret).await;

    a_reg.tick().await.unwrap();
    b_reg.tick().await.unwrap();
    c_reg.tick().await.unwrap();
    a_reg.tick().await.unwrap();
    b_reg.tick().await.unwrap();
    c_reg.tick().await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let body = serde_json::json!({
        "tenant_id": 1,
        "query": "SELECT model, count(*) FROM spans GROUP BY model"
    });

    for url in [&a_url, &b_url, &c_url] {
        let r: serde_json::Value = client
            .post(format!("{}/v1/query", url))
            .json(&body)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let rows = r["result"]["rows"].as_array().unwrap();
        let total: i64 = rows
            .iter()
            .map(|row| row["fields"]["count"].as_i64().unwrap_or(0))
            .sum();
        // Don't include the random `url` in the panic message — CodeQL
        // flags it as cleartext logging of an endpoint that was set up
        // with a shared HMAC secret.
        assert_eq!(total, 50, "unexpected total row count: {total}");
    }
}

/// A peer with no HMAC header cannot reach `/v1/internal/query`. The
/// request body is a no-op `SELECT 1` and contains no auth material —
/// the test verifies that the absence of the `X-Zen-Hmac` header alone
/// is enough for the receiver to return 401.
#[tokio::test]
async fn missing_hmac_header_is_rejected_at_internal_endpoint() {
    let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
    let catalog: Arc<dyn Catalog> = Arc::new(MockCatalog::new());

    let endpoint = spawn_hmac_protected_node(catalog, store).await;

    let client = reqwest::Client::new();
    let r = client
        .post(format!("{endpoint}/v1/internal/query"))
        .json(&serde_json::json!({
            "tenant_id": 1,
            "query": "SELECT 1",
            "disable_route": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 401);
}

/// Spawn a node whose `internal_secret` is generated locally and never
/// returned to the caller — the test only needs the endpoint URL. Keeps
/// the secret out of the test scope so static analyzers don't flag the
/// subsequent loopback request as "transmits sensitive data".
async fn spawn_hmac_protected_node(catalog: Arc<dyn Catalog>, store: Arc<dyn BlobStore>) -> String {
    use rand_core::RngCore;
    let mut secret = [0u8; 32];
    rand_core::OsRng.fill_bytes(&mut secret);
    let hex: String = secret.iter().map(|b| format!("{b:02x}")).collect();
    let (endpoint, _reg) = spawn_node_with_secret(catalog, store, &hex).await;
    endpoint
}
