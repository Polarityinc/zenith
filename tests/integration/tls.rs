//! TLS smoke test: launch the server with a self-signed cert and verify
//! that an HTTPS client can hit `/v1/health` over rustls.

use std::sync::Arc;

use tempfile::TempDir;

use zen_catalog::{Catalog, SqliteCatalog};
use zen_common::{Config, PartitionId, TenantId};
use zen_server::{http::serve, ServerState};
use zen_storage::{local_fs::InMemoryStore, BlobStore};

/// PEM-encoded self-signed cert + key for `localhost`. Generated once
/// and committed; this is test material only, never used in production.
const CERT_PEM: &str = include_str!("test_data/localhost-cert.pem");
const KEY_PEM: &str = include_str!("test_data/localhost-key.pem");

#[tokio::test]
async fn https_listener_serves_health() {
    let dir = TempDir::new().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, CERT_PEM).unwrap();
    std::fs::write(&key_path, KEY_PEM).unwrap();

    let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
    let cat: Arc<dyn Catalog> = Arc::new(SqliteCatalog::open_in_memory().await.unwrap());
    cat.ensure_tenant(TenantId(1), "t").await.unwrap();
    cat.ensure_partition(TenantId(1), PartitionId(0))
        .await
        .unwrap();

    let mut cfg = Config::default();
    cfg.server.tls.cert_path = cert_path.to_string_lossy().to_string();
    cfg.server.tls.key_path = key_path.to_string_lossy().to_string();

    // Pick a free port via temporary bind.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);
    let addr_str = addr.to_string();

    let state = ServerState::new(cfg, cat, store);
    tokio::spawn(async move {
        serve(state, &addr_str).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Build a reqwest client that trusts our self-signed CA. Hostname
    // verification stays ENABLED — the cert SAN includes both
    // `DNS:localhost` and `IP:127.0.0.1`, so we can validate strictly.
    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(CERT_PEM.as_bytes()).unwrap())
        .build()
        .unwrap();

    // Use the SAN-matching `localhost` hostname. The listener is bound
    // to 127.0.0.1 which `localhost` resolves to on every supported
    // platform, so this connects to the same socket while exercising
    // strict hostname verification.
    let url = format!("https://localhost:{}/v1/health", addr.port());
    let r = client.get(&url).send().await.unwrap();
    assert!(r.status().is_success(), "got {}", r.status());
}
