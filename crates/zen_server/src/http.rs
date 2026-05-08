//! Wire HTTP routes onto axum.
//!
//! The router is split into three trust zones, each with its own
//! middleware stack:
//!
//! - **public**: `/v1/health` (liveness probe), `/v1/metrics` (Prometheus
//!   scraper). No auth.
//! - **customer**: `/v1/{ingest,traces,query,segments,compact,…}`. Behind
//!   the JWT layer — every request must carry a verified Bearer token.
//! - **internal**: `/v1/internal/*`. Behind the HMAC layer — node-to-node
//!   only.
//!
//! When the operator hasn't configured `auth.jwks_url` /
//! `auth.internal_secret`, the corresponding middleware is a no-op (auth
//! off). This is the dev / single-node path; the boot-time validator
//! warns on it.

use axum::extract::DefaultBodyLimit;
use axum::middleware::from_fn_with_state;
use axum::routing::{get, post};
use axum::Router;
use tower::limit::ConcurrencyLimitLayer;

use crate::admin::{
    handle_compact, handle_compact_full, handle_health, handle_healthz, handle_readyz,
    handle_segments,
};
use crate::ingest::handle_ingest;
use crate::internal_query::handle_internal_query;
use crate::metrics::handle_metrics;
use crate::middleware::auth::{hmac_layer, jwt_layer};
use crate::middleware::limits::{rate_limit_layer, RateLimits};
use crate::openapi::handle_openapi;
use crate::otlp::handle_otlp_traces;
use crate::query_handler::handle_query;
use crate::state::ServerState;

/// 256 MiB — large enough for the Brainstore-style 100 × 100 KB ingest plus
/// future Arrow-IPC bulk writes.
const MAX_BODY_BYTES: usize = 256 * 1024 * 1024;

pub fn router(state: ServerState) -> Router {
    let max_concurrent = state.config.query.max_concurrent_queries.max(1) as usize;
    let limits = RateLimits::new(
        state.config.query.tenant_qps_limit,
        state.config.query.tenant_burst,
    );

    let public = Router::new()
        .route("/v1/health", get(handle_health))
        .route("/v1/healthz", get(handle_healthz))
        .route("/v1/readyz", get(handle_readyz))
        .route("/v1/metrics", get(handle_metrics))
        .route("/v1/openapi.json", get(handle_openapi));

    let customer = Router::new()
        .route("/v1/ingest", post(handle_ingest))
        .route("/v1/traces", post(handle_otlp_traces))
        .route("/v1/query", post(handle_query))
        .route("/v1/compact", post(handle_compact))
        .route("/v1/compact-full", post(handle_compact_full))
        .route("/v1/segments", get(handle_segments))
        // JWT first so the rate-limiter can read `Claims.tenant_id`.
        // Order matters: layers in axum apply *outside-in*, so we add
        // the rate-limit *first* (which makes it inner-most) and JWT
        // last (outer-most).
        .layer(from_fn_with_state(limits, rate_limit_layer))
        .layer(from_fn_with_state(state.clone(), jwt_layer))
        // Global concurrency cap — runs before everything else so we
        // shed load early when overloaded.
        .layer(ConcurrencyLimitLayer::new(max_concurrent));

    let internal = Router::new()
        .route("/v1/internal/query", post(handle_internal_query))
        .layer(from_fn_with_state(state.clone(), hmac_layer));

    public
        .merge(customer)
        .merge(internal)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

pub async fn serve(
    state: ServerState,
    addr: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let tls_cert = state.config.server.tls.cert_path.clone();
    let tls_key = state.config.server.tls.key_path.clone();
    let app = router(state);
    if !tls_cert.is_empty() && !tls_key.is_empty() {
        // Install the AWS-LC-RS provider once. Idempotent — subsequent
        // calls are no-ops thanks to the `OnceLock` inside rustls.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let tls_config = load_rustls_config(&tls_cert, &tls_key)?;
        let socket: std::net::SocketAddr = addr.parse()?;
        tracing::info!(%addr, %tls_cert, "zenithdb https listening (rustls + aws-lc-rs)");
        axum_server::bind_rustls(
            socket,
            axum_server::tls_rustls::RustlsConfig::from_config(std::sync::Arc::new(tls_config)),
        )
        .serve(app.into_make_service())
        .await?;
    } else {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!(%addr, "zenithdb http listening (plaintext — TLS not configured)");
        axum::serve(listener, app).await?;
    }
    Ok(())
}

fn load_rustls_config(
    cert_path: &str,
    key_path: &str,
) -> Result<rustls::ServerConfig, Box<dyn std::error::Error + Send + Sync>> {
    use std::fs::File;
    use std::io::BufReader;
    let cert_file = File::open(cert_path)?;
    let key_file = File::open(key_path)?;
    let certs: Vec<_> =
        rustls_pemfile::certs(&mut BufReader::new(cert_file)).collect::<Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))?
        .ok_or("no private key found in key_path")?;
    let cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    Ok(cfg)
}
