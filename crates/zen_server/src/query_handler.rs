//! Public query endpoint. When clustered, routes via the
//! `zen_cluster::QueryRouter` so single-tenant queries land on the
//! tenant's primary replica.

use std::time::Instant;

use axum::{extract::State, http::StatusCode, Extension, Json};
use serde::{Deserialize, Serialize};

use zen_auth::Claims;
use zen_cluster::remote::InternalQueryRequest;
use zen_cluster::{QueryRouter, RouteDecision, ShardKey};
use zen_query::{execute_full, ResultSet};

use crate::metrics::names;
use crate::middleware::tenant_check::ANONYMOUS_SUB;
use crate::state::ServerState;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    pub tenant_id: u64,
    pub query: String,
    /// "sql" (default) or "zql".
    #[serde(default)]
    pub dialect: Option<String>,
    /// Force local execution; skip the router. Used in tests and as an
    /// operator escape hatch.
    #[serde(default)]
    pub disable_route: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct QueryResponse {
    pub result: ResultSet,
}

pub async fn handle_query(
    State(state): State<ServerState>,
    claims: Extension<Claims>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, String)> {
    // CRITICAL: enforce that the JWT-verified tenant matches the
    // request body. Without this, a tenant with a valid token could
    // submit `{"tenant_id": <other>}` and read/write any other
    // tenant's data — the worst class of multi-tenant bug.
    if claims.sub != ANONYMOUS_SUB && claims.tenant_id != req.tenant_id {
        return Err((
            StatusCode::FORBIDDEN,
            format!(
                "tenant mismatch: token authorizes tenant {}, request claims tenant {}",
                claims.tenant_id, req.tenant_id
            ),
        ));
    }
    let started = Instant::now();
    let tenant_label = req.tenant_id.to_string();
    let result = handle_query_inner(state, req).await;
    let elapsed = started.elapsed().as_secs_f64();
    let status = if result.is_ok() { "ok" } else { "error" };
    metrics::histogram!(names::QUERY_DURATION, "tenant" => tenant_label.clone()).record(elapsed);
    metrics::counter!(names::QUERIES_TOTAL, "tenant" => tenant_label, "status" => status)
        .increment(1);
    result
}

async fn handle_query_inner(
    state: ServerState,
    req: QueryRequest,
) -> Result<Json<QueryResponse>, (StatusCode, String)> {
    if !req.disable_route {
        if let Some(reg) = state.cluster.clone() {
            let map = reg.shard_map();
            let router = QueryRouter::new(reg.local_id(), map);
            let key = ShardKey::new(req.tenant_id, 0);
            match router.route_tenant(key, reg.now_ms()) {
                RouteDecision::Local => {}
                RouteDecision::Remote(targets) => {
                    let internal = InternalQueryRequest {
                        tenant_id: req.tenant_id,
                        query: req.query.clone(),
                        dialect: req.dialect.clone(),
                        disable_route: true,
                    };
                    match state.remote.forward(&targets, &internal).await {
                        Ok(result) => return Ok(Json(QueryResponse { result })),
                        Err(e) => {
                            tracing::warn!(error=%e, "all remote replicas failed, falling back to local");
                        }
                    }
                }
                RouteDecision::FanOut {
                    targets,
                    include_local,
                } => {
                    let internal = InternalQueryRequest {
                        tenant_id: req.tenant_id,
                        query: req.query.clone(),
                        dialect: req.dialect.clone(),
                        disable_route: true,
                    };
                    let mut parts = state.remote.fan_out(&targets, &internal).await;
                    if include_local {
                        if let Ok(plan) = state.parse_query(&req.query, req.tenant_id) {
                            if let Ok(local) = execute_full(
                                &plan,
                                state.catalog.clone(),
                                state.store.clone(),
                                &state.seg_cache,
                                &state.list_cache,
                            )
                            .await
                            {
                                parts.push(local);
                            }
                        }
                    }
                    return Ok(Json(QueryResponse {
                        result: zen_cluster::merge_result_sets(parts, None),
                    }));
                }
            }
        }
    }

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
    Ok(Json(QueryResponse { result }))
}
