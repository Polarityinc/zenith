//! Admin endpoints: compact, list segments, health.

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};

use zen_common::{PartitionId, Schema, TenantId};
use zen_compactor::{compact_full, compact_partition};

use crate::state::ServerState;

#[derive(Clone, Debug, Deserialize)]
pub struct CompactRequest {
    pub tenant_id: u64,
    #[serde(default)]
    pub partition_id: u32,
}

#[derive(Clone, Debug, Serialize)]
pub struct CompactResponse {
    pub wal_objects_consumed: u32,
    pub rows_compacted: u64,
    pub segment_bytes: u64,
    pub elapsed_ms: u64,
}

pub async fn handle_compact(
    State(state): State<ServerState>,
    Json(req): Json<CompactRequest>,
) -> Result<Json<CompactResponse>, (StatusCode, String)> {
    let tenant = TenantId(req.tenant_id);
    let partition = PartitionId(req.partition_id);
    let stats = compact_partition(
        state.catalog.clone(),
        state.store.clone(),
        tenant,
        partition,
        "http-admin",
        &Schema::spans_v1(),
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
    Ok(Json(CompactResponse {
        wal_objects_consumed: stats.wal_objects_consumed,
        rows_compacted: stats.rows_compacted,
        segment_bytes: stats.segment_bytes,
        elapsed_ms: stats.elapsed_ms,
    }))
}

pub async fn handle_compact_full(
    State(state): State<ServerState>,
    Json(req): Json<CompactRequest>,
) -> Result<Json<CompactResponse>, (StatusCode, String)> {
    let tenant = TenantId(req.tenant_id);
    let partition = PartitionId(req.partition_id);
    let stats = compact_full(
        state.catalog.clone(),
        state.store.clone(),
        tenant,
        partition,
        "http-admin-full",
        &Schema::spans_v1(),
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
    Ok(Json(CompactResponse {
        wal_objects_consumed: stats.wal_objects_consumed,
        rows_compacted: stats.rows_compacted,
        segment_bytes: stats.segment_bytes,
        elapsed_ms: stats.elapsed_ms,
    }))
}

pub async fn handle_health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

/// Liveness probe — process is alive and responsive. Cheap; just confirms
/// the axum task is reachable and we're not deadlocked. Kubernetes uses
/// this to decide whether to kill+restart.
pub async fn handle_healthz() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok", "kind": "liveness"}))
}

/// Readiness probe — should we receive customer traffic? Checks:
///
/// - Catalog reachable (tenant 0 lookup succeeds; ~1 ms on sqlite,
///   <50 ms on a healthy Postgres).
///
/// Add more checks here as they become available — segment-cache warm-up,
/// WAL flush age, etc. Returns 503 on any failure so Kubernetes pulls
/// the pod from the load-balancer endpoint set.
pub async fn handle_readyz(
    State(state): State<ServerState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    state
        .catalog
        .ensure_tenant(TenantId(0), "default")
        .await
        .map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("catalog probe failed: {e}"),
            )
        })?;
    Ok(Json(serde_json::json!({
        "status": "ready",
        "kind": "readiness",
        "checks": {
            "catalog": "ok",
        }
    })))
}

pub async fn handle_segments(
    State(state): State<ServerState>,
    axum::extract::Query(q): axum::extract::Query<SegmentsQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let segs = state
        .catalog
        .list_segments_for_tenant(TenantId(q.tenant_id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
    Ok(Json(serde_json::json!({
        "count": segs.len(),
        "segments": segs.iter().map(|s| serde_json::json!({
            "segment_id": s.segment_id.to_string(),
            "object_key": s.object_key,
            "row_count": s.row_count,
            "byte_count": s.byte_count,
            "time_min": s.time_min,
            "time_max": s.time_max,
        })).collect::<Vec<_>>(),
    })))
}

#[derive(Deserialize)]
pub struct SegmentsQuery {
    pub tenant_id: u64,
}
