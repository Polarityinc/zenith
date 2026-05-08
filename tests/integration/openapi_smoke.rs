//! `/v1/openapi.json` returns a parseable OpenAPI 3.x document.

use std::sync::Arc;

use tokio::net::TcpListener;

use zen_catalog::{Catalog, SqliteCatalog};
use zen_common::{Config, PartitionId, TenantId};
use zen_server::{http::router, ServerState};
use zen_storage::{local_fs::InMemoryStore, BlobStore};

#[tokio::test]
async fn openapi_endpoint_returns_spec() {
    let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
    let cat: Arc<dyn Catalog> = Arc::new(SqliteCatalog::open_in_memory().await.unwrap());
    cat.ensure_tenant(TenantId(1), "t").await.unwrap();
    cat.ensure_partition(TenantId(1), PartitionId(0))
        .await
        .unwrap();

    let state = ServerState::new(Config::default(), cat, store);
    let app = router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr}/v1/openapi.json"))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    // OpenAPI 3.x — version field is `openapi`.
    assert!(body["openapi"].as_str().unwrap_or("").starts_with("3."));
    let paths = body["paths"].as_object().expect("paths");
    assert!(paths.contains_key("/v1/health"));
    assert!(paths.contains_key("/v1/query"));
    assert!(paths.contains_key("/v1/openapi.json"));
}
