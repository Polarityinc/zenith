//! Query endpoint.

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};

use zen_query::{execute_full, ResultSet};

use crate::state::ServerState;

#[derive(Clone, Debug, Deserialize)]
pub struct QueryRequest {
    pub tenant_id: u64,
    pub query: String,
    /// "sql" (default) or "zql".
    #[serde(default)]
    pub dialect: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct QueryResponse {
    pub result: ResultSet,
}

pub async fn handle_query(
    State(state): State<ServerState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, String)> {
    let plan = zen_ql::parse(&req.query, req.tenant_id).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("parse error: {e}"),
        )
    })?;
    let result = execute_full(
        &plan,
        state.catalog,
        state.store,
        &state.seg_cache,
        &state.list_cache,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
    Ok(Json(QueryResponse { result }))
}
