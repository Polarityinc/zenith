//! Auth integration: prove that with `auth.jwks_url` set the customer
//! routes require a valid JWT.

use std::sync::Arc;

use jsonwebtoken::{encode, EncodingKey, Header};
use tokio::net::TcpListener;

use zen_auth::{ClaimsCache, JwtVerifier};
use zen_catalog::{Catalog, SqliteCatalog};
use zen_common::{Config, PartitionId, TenantId};
use zen_server::middleware::auth::AuthState;
use zen_server::{http::router, ServerState};
use zen_storage::{local_fs::InMemoryStore, BlobStore};

const PEM: &[u8] = include_bytes!("../../crates/zen_auth/src/test_data/rsa_pkcs8.pem");
const JWKS: &str = include_str!("../../crates/zen_auth/src/test_data/jwks.json");

#[derive(serde::Serialize)]
struct TestClaims {
    sub: String,
    tenant_id: u64,
    exp: i64,
    scope: String,
}

fn mint_token(tenant: u64, exp: i64) -> String {
    let mut h = Header::new(jsonwebtoken::Algorithm::RS256);
    h.kid = Some("test-1".into());
    let enc = EncodingKey::from_rsa_pem(PEM).unwrap();
    encode(
        &h,
        &TestClaims {
            sub: "alice".into(),
            tenant_id: tenant,
            exp,
            scope: "ingest read admin".into(),
        },
        &enc,
    )
    .unwrap()
}

async fn spawn_server_auth_on() -> String {
    let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
    let cat: Arc<dyn Catalog> = Arc::new(SqliteCatalog::open_in_memory().await.unwrap());
    cat.ensure_tenant(TenantId(1), "t").await.unwrap();
    cat.ensure_partition(TenantId(1), PartitionId(0))
        .await
        .unwrap();

    let cfg = Config::default();
    let mut state = ServerState::new(cfg, cat, store);

    // Override auth to use our test JWKS instead of the empty default.
    let jwks: jsonwebtoken::jwk::JwkSet = serde_json::from_str(JWKS).unwrap();
    state.auth = AuthState {
        jwt: Some(JwtVerifier::from_jwks(jwks, ClaimsCache::default())),
        hmac: None,
    };

    let app = router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    format!("http://{addr}")
}

#[tokio::test]
async fn anonymous_request_to_customer_route_is_401() {
    let url = spawn_server_auth_on().await;
    let client = reqwest::Client::new();
    let r = client
        .post(format!("{url}/v1/query"))
        .json(&serde_json::json!({"tenant_id": 1, "query": "SELECT 1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 401);
}

#[tokio::test]
async fn valid_token_passes() {
    let url = spawn_server_auth_on().await;
    let client = reqwest::Client::new();
    let token = mint_token(1, chrono::Utc::now().timestamp() + 60);
    let r = client
        .post(format!("{url}/v1/query"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"tenant_id": 1, "query": "SELECT count(*) FROM spans"}))
        .send()
        .await
        .unwrap();
    // 200 OK on parsed query against empty corpus.
    assert!(
        r.status().is_success(),
        "expected 2xx, got {}: {}",
        r.status(),
        r.text().await.unwrap_or_default()
    );
}

#[tokio::test]
async fn expired_token_rejected() {
    let url = spawn_server_auth_on().await;
    let client = reqwest::Client::new();
    let token = mint_token(1, chrono::Utc::now().timestamp() - 600);
    let r = client
        .post(format!("{url}/v1/query"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"tenant_id": 1, "query": "SELECT 1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 401);
}

#[tokio::test]
async fn metrics_remains_public() {
    let url = spawn_server_auth_on().await;
    let client = reqwest::Client::new();
    // Anonymous, no auth header — should still work since /v1/metrics
    // is in the public router.
    let r = client.get(format!("{url}/v1/metrics")).send().await.unwrap();
    assert!(r.status().is_success(), "metrics 2xx, got {}", r.status());
}
