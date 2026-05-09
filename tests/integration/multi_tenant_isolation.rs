//! End-to-end multi-tenant isolation. Two tenants ingest to the same
//! server; each tenant's queries must see ONLY their own rows.
//!
//! This is the "the security audit caught it" regression test made real
//! — without the JWT-tenant-claim enforcement (Phase B + the security
//! audit commit), a token for tenant A could read tenant B's data by
//! changing `tenant_id` in the request body. With the enforcement on,
//! the cross-tenant query is 403'd.
//!
//! We exercise both modes:
//!   * **auth off (anonymous)** — body `tenant_id` is trusted (single-
//!     tenant dev mode). Cross-body queries hit different catalogs;
//!     ingest into tenant 1 must NOT show up in queries against tenant 2.
//!   * **auth on (JWT enforced)** — cross-body `tenant_id` is rejected
//!     with 403 before the query even runs.

use std::sync::Arc;

use jsonwebtoken::{encode, EncodingKey, Header};
use tokio::net::TcpListener;

use zen_auth::{ClaimsCache, JwtVerifier};
use zen_catalog::{Catalog, MockCatalog};
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

fn mint(tenant: u64, scope: &str) -> String {
    let mut h = Header::new(jsonwebtoken::Algorithm::RS256);
    h.kid = Some("test-1".into());
    let enc = EncodingKey::from_rsa_pem(PEM).unwrap();
    encode(
        &h,
        &TestClaims {
            sub: format!("u-tenant{tenant}"),
            tenant_id: tenant,
            exp: chrono::Utc::now().timestamp() + 60,
            scope: scope.into(),
        },
        &enc,
    )
    .unwrap()
}

async fn spawn(state: ServerState) -> String {
    let app = router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    format!("http://{addr}")
}

async fn ingest(
    client: &reqwest::Client,
    url: &str,
    bearer: Option<&str>,
    tenant_id: u64,
    n: usize,
    model: &str,
) -> u16 {
    let spans: Vec<serde_json::Value> = (0..n)
        .map(|i| {
            serde_json::json!({
                "start_time_ms": 1000 + i,
                "end_time_ms":   1001 + i,
                "duration_ms":   1,
                "model":         model,
                "status":        "ok",
                "prompt":        format!("hello tenant{tenant_id}"),
            })
        })
        .collect();
    let mut req = client
        .post(format!("{url}/v1/ingest"))
        .json(&serde_json::json!({ "tenant_id": tenant_id, "spans": spans }));
    if let Some(t) = bearer {
        req = req.header("Authorization", format!("Bearer {t}"));
    }
    req.send().await.unwrap().status().as_u16()
}

async fn count_rows(
    client: &reqwest::Client,
    url: &str,
    bearer: Option<&str>,
    tenant_id: u64,
) -> i64 {
    let mut req = client
        .post(format!("{url}/v1/query"))
        .json(&serde_json::json!({
            "tenant_id": tenant_id,
            "query": "SELECT count(*) FROM spans",
        }));
    if let Some(t) = bearer {
        req = req.header("Authorization", format!("Bearer {t}"));
    }
    let r: serde_json::Value = req.send().await.unwrap().json().await.unwrap();
    r["result"]["rows"]
        .as_array()
        .and_then(|a| a.first())
        .and_then(|f| f["fields"]["count"].as_i64())
        .unwrap_or(0)
}

#[tokio::test]
async fn auth_off_two_tenants_keep_their_rows_separate() {
    // No JWT layer (auth.jwks_url = ""). Anonymous claim allows any
    // body tenant_id through. We're testing the storage / catalog
    // tenant scoping, not the auth claim enforcement.
    let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
    let catalog: Arc<dyn Catalog> = Arc::new(MockCatalog::new());
    catalog.ensure_tenant(TenantId(1), "alpha").await.unwrap();
    catalog.ensure_partition(TenantId(1), PartitionId(0)).await.unwrap();
    catalog.ensure_tenant(TenantId(2), "beta").await.unwrap();
    catalog.ensure_partition(TenantId(2), PartitionId(0)).await.unwrap();

    let state = ServerState::new(Config::default(), catalog, store);
    let url = spawn(state).await;
    let client = reqwest::Client::new();

    assert_eq!(ingest(&client, &url, None, 1, 17, "gpt-4o").await, 200);
    assert_eq!(ingest(&client, &url, None, 2, 5, "claude-opus-4-7").await, 200);

    // Tenant 1 sees its own 17 rows; tenant 2 sees its 5.
    assert_eq!(count_rows(&client, &url, None, 1).await, 17, "tenant 1 row count");
    assert_eq!(count_rows(&client, &url, None, 2).await, 5,  "tenant 2 row count");
}

#[tokio::test]
async fn auth_on_cross_tenant_query_is_403() {
    // Auth on. JwtVerifier preloaded with the test JWKS.
    let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
    let catalog: Arc<dyn Catalog> = Arc::new(MockCatalog::new());
    catalog.ensure_tenant(TenantId(1), "alpha").await.unwrap();
    catalog.ensure_partition(TenantId(1), PartitionId(0)).await.unwrap();
    catalog.ensure_tenant(TenantId(2), "beta").await.unwrap();
    catalog.ensure_partition(TenantId(2), PartitionId(0)).await.unwrap();

    let mut state = ServerState::new(Config::default(), catalog, store);
    let jwks: jsonwebtoken::jwk::JwkSet = serde_json::from_str(JWKS).unwrap();
    state.auth = AuthState {
        jwt: Some(JwtVerifier::from_jwks(jwks, ClaimsCache::default())),
        hmac: None,
    };
    let url = spawn(state).await;
    let client = reqwest::Client::new();

    let token1 = mint(1, "ingest read admin");

    // Legitimate ingest into tenant 1 with a tenant-1 token: 200.
    let s = ingest(&client, &url, Some(&token1), 1, 7, "gpt-4o").await;
    assert_eq!(s, 200, "tenant-1 token + body=1 must succeed");

    // Cross-tenant ingest attempt: tenant-1 token, body claims tenant 2 → 403.
    let s = ingest(&client, &url, Some(&token1), 2, 99, "gpt-4o").await;
    assert_eq!(s, 403, "cross-tenant ingest must be 403");

    // Cross-tenant query attempt: tenant-1 token reading tenant 2 → 403.
    let r = client
        .post(format!("{url}/v1/query"))
        .header("Authorization", format!("Bearer {token1}"))
        .json(&serde_json::json!({
            "tenant_id": 2,
            "query": "SELECT count(*) FROM spans"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 403);

    // Tenant 2's data must remain at 0 (the cross-tenant ingest was blocked).
    let token2 = mint(2, "ingest read admin");
    assert_eq!(count_rows(&client, &url, Some(&token2), 2).await, 0);
}

#[tokio::test]
async fn auth_on_segments_endpoint_is_tenant_scoped() {
    let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
    let catalog: Arc<dyn Catalog> = Arc::new(MockCatalog::new());
    catalog.ensure_tenant(TenantId(1), "alpha").await.unwrap();
    catalog.ensure_partition(TenantId(1), PartitionId(0)).await.unwrap();
    catalog.ensure_tenant(TenantId(9), "victim").await.unwrap();

    let mut state = ServerState::new(Config::default(), catalog, store);
    let jwks: jsonwebtoken::jwk::JwkSet = serde_json::from_str(JWKS).unwrap();
    state.auth = AuthState {
        jwt: Some(JwtVerifier::from_jwks(jwks, ClaimsCache::default())),
        hmac: None,
    };
    let url = spawn(state).await;
    let client = reqwest::Client::new();
    let token1 = mint(1, "ingest read admin");

    // tenant-1 token can list its own segments.
    let r = client
        .get(format!("{url}/v1/segments?tenant_id=1"))
        .header("Authorization", format!("Bearer {token1}"))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());

    // tenant-1 token CANNOT list tenant 9's segments.
    let r = client
        .get(format!("{url}/v1/segments?tenant_id=9"))
        .header("Authorization", format!("Bearer {token1}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 403);
}

#[tokio::test]
async fn auth_on_compact_requires_admin_scope() {
    let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
    let catalog: Arc<dyn Catalog> = Arc::new(MockCatalog::new());
    catalog.ensure_tenant(TenantId(1), "alpha").await.unwrap();
    catalog.ensure_partition(TenantId(1), PartitionId(0)).await.unwrap();

    let mut state = ServerState::new(Config::default(), catalog, store);
    let jwks: jsonwebtoken::jwk::JwkSet = serde_json::from_str(JWKS).unwrap();
    state.auth = AuthState {
        jwt: Some(JwtVerifier::from_jwks(jwks, ClaimsCache::default())),
        hmac: None,
    };
    let url = spawn(state).await;
    let client = reqwest::Client::new();
    let read_only = mint(1, "read");
    let admin = mint(1, "ingest read admin");

    let r = client
        .post(format!("{url}/v1/compact"))
        .header("Authorization", format!("Bearer {read_only}"))
        .json(&serde_json::json!({"tenant_id": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 403, "read scope must be rejected on compact");

    let r = client
        .post(format!("{url}/v1/compact"))
        .header("Authorization", format!("Bearer {admin}"))
        .json(&serde_json::json!({"tenant_id": 1}))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "admin scope must succeed on compact");
}
