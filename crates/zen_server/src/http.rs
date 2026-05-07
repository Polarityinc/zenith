//! Wire HTTP routes onto axum.

use axum::routing::{get, post};
use axum::Router;

use crate::admin::{handle_compact, handle_health, handle_segments};
use crate::ingest::handle_ingest;
use crate::otlp::handle_otlp_traces;
use crate::query_handler::handle_query;
use crate::state::ServerState;

pub fn router(state: ServerState) -> Router {
    Router::new()
        .route("/v1/health", get(handle_health))
        .route("/v1/ingest", post(handle_ingest))
        .route("/v1/traces", post(handle_otlp_traces))
        .route("/v1/query", post(handle_query))
        .route("/v1/compact", post(handle_compact))
        .route("/v1/segments", get(handle_segments))
        .with_state(state)
}

pub async fn serve(state: ServerState, addr: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "zenithdb http listening");
    axum::serve(listener, app).await?;
    Ok(())
}
