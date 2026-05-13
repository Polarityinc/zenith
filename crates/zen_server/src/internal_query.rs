//! `POST /v1/internal/query` — node-to-node query endpoint.
//!
//! Identical semantics to `/v1/query` except it bypasses the
//! `QueryRouter` and always executes locally. Used when a coordinator
//! has decided this node is the right replica for a shard and wants the
//! local executor to do the work.

use axum::{extract::State, http::StatusCode, Json};

use zen_cluster::remote::{InternalQueryRequest, InternalQueryResponse};
use zen_query::execute_full;

use crate::state::ServerState;

pub async fn handle_internal_query(
    State(state): State<ServerState>,
    Json(req): Json<InternalQueryRequest>,
) -> Result<Json<InternalQueryResponse>, (StatusCode, String)> {
    let plan = state
        .parse_query(&req.query, req.tenant_id)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("parse error: {e}")))?;
    let result = execute_full(
        &plan,
        state.catalog.clone(),
        state.store.clone(),
        &state.seg_cache,
        &state.list_cache,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
    Ok(Json(InternalQueryResponse { result }))
}
