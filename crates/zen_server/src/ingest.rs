//! Ingest endpoint.

use std::time::Instant;

use axum::{extract::State, http::StatusCode, Extension, Json};
use serde::{Deserialize, Serialize};

use zen_auth::Claims;
use zen_catalog::model::{WalObjectBounds, WalObjectRow};
use zen_common::{CommitId, PartitionId, Schema, SpanId, SpanRecord, TenantId, TraceId};
use zen_wal::WalWriter;

use crate::metrics::names;
use crate::middleware::tenant_check::ANONYMOUS_SUB;
use crate::state::ServerState;

/// Per-request span cap. Defends against allocate-millions-of-rows DoS
/// when a malicious actor with a valid token sends one giant batch.
const MAX_SPANS_PER_REQUEST: usize = 100_000;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestRequest {
    pub tenant_id: u64,
    #[serde(default)]
    pub partition_id: u32,
    pub spans: Vec<SpanIn>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SpanIn {
    #[serde(default)]
    pub trace_id: Option<String>,
    #[serde(default)]
    pub span_id: Option<String>,
    #[serde(default)]
    pub parent_span_id: Option<String>,
    pub start_time_ms: i64,
    pub end_time_ms: i64,
    #[serde(default)]
    pub duration_ms: Option<i64>,
    #[serde(default)]
    pub span_type: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub completion: Option<String>,
    #[serde(default)]
    pub prompt_tokens: Option<u32>,
    #[serde(default)]
    pub completion_tokens: Option<u32>,
    #[serde(default)]
    pub cost_usd: Option<f64>,
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub top_p: Option<f64>,
    #[serde(default)]
    pub tool_io_text: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub embedding: Option<Vec<f32>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct IngestResponse {
    pub spans_accepted: u32,
    pub wal_object_key: String,
}

pub async fn handle_ingest(
    State(state): State<ServerState>,
    claims: Extension<Claims>,
    body: axum::body::Bytes,
) -> Result<Json<IngestResponse>, (StatusCode, String)> {
    let started = Instant::now();
    let result = handle_ingest_inner(state, claims.0, body).await;
    let elapsed = started.elapsed().as_secs_f64();
    let (status, rows, tenant_label) = match &result {
        Ok(r) => ("ok", r.spans_accepted as u64, String::new()),
        Err(_) => ("error", 0, String::new()),
    };
    metrics::histogram!(names::INGEST_DURATION, "tenant" => tenant_label.clone()).record(elapsed);
    if rows > 0 {
        metrics::counter!(names::INGEST_ROWS_TOTAL, "tenant" => tenant_label.clone(), "status" => status)
            .increment(rows);
    }
    result
}

async fn handle_ingest_inner(
    state: ServerState,
    claims: Claims,
    body: axum::body::Bytes,
) -> Result<Json<IngestResponse>, (StatusCode, String)> {
    // simd-json is 3-4× faster than serde_json for large bodies. The ingest
    // path is dominated by JSON parse cost on 10+ MB write batches; this
    // alone roughly doubles write throughput for the Brainstore-style
    // 100×100 KB workload.
    //
    // simd-json mutates its input buffer, so we own a Vec.
    let mut buf = body.to_vec();
    let req: IngestRequest = simd_json::from_slice(&mut buf)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("ingest body parse: {e}")))?;
    // CRITICAL: enforce JWT-tenant matches request tenant.
    if claims.sub != ANONYMOUS_SUB && claims.tenant_id != req.tenant_id {
        return Err((
            StatusCode::FORBIDDEN,
            format!(
                "tenant mismatch: token authorizes tenant {}, request claims tenant {}",
                claims.tenant_id, req.tenant_id
            ),
        ));
    }
    if req.spans.is_empty() {
        return Ok(Json(IngestResponse {
            spans_accepted: 0,
            wal_object_key: String::new(),
        }));
    }
    if req.spans.len() > MAX_SPANS_PER_REQUEST {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "ingest batch too large: {} spans > {} max per request",
                req.spans.len(),
                MAX_SPANS_PER_REQUEST
            ),
        ));
    }
    let tenant = TenantId(req.tenant_id);
    let partition = PartitionId(req.partition_id);
    state
        .catalog
        .ensure_tenant(tenant, "")
        .await
        .map_err(http_err)?;
    state
        .catalog
        .ensure_partition(tenant, partition)
        .await
        .map_err(http_err)?;

    let n = req.spans.len();

    // Convert to SpanRecord and assign commit_ids.
    let commit_id = state
        .catalog
        .next_commit_range(tenant, partition, n as u64)
        .await
        .map_err(http_err)?;

    let records: Vec<SpanRecord> = req
        .spans
        .into_iter()
        .enumerate()
        .map(|(i, s)| {
            let trace_id = s
                .trace_id
                .as_deref()
                .and_then(|s| s.parse::<TraceId>().ok())
                .unwrap_or_else(TraceId::new_random);
            let span_id = s
                .span_id
                .as_deref()
                .and_then(|s| s.parse::<SpanId>().ok())
                .unwrap_or_else(SpanId::new_random);
            let parent_span_id = s
                .parent_span_id
                .as_deref()
                .and_then(|s| s.parse::<SpanId>().ok());
            SpanRecord {
                tenant_id: tenant,
                partition_id: partition,
                trace_id,
                span_id,
                parent_span_id,
                start_time_ms: s.start_time_ms,
                end_time_ms: s.end_time_ms,
                duration_ms: s
                    .duration_ms
                    .unwrap_or(s.end_time_ms.saturating_sub(s.start_time_ms)),
                span_type: s.span_type,
                status: s.status,
                provider: s.provider,
                model: s.model,
                tool_name: s.tool_name,
                prompt: s.prompt,
                completion: s.completion,
                prompt_tokens: s.prompt_tokens,
                completion_tokens: s.completion_tokens,
                cost_usd: s.cost_usd,
                temperature: s.temperature,
                top_p: s.top_p,
                tool_io_text: s.tool_io_text,
                user_id: s.user_id,
                session_id: s.session_id,
                request_id: s.request_id,
                metadata: s.metadata,
                embedding: s.embedding,
                commit_id: CommitId(commit_id.0 + i as u64),
            }
        })
        .collect();

    // Append to memtable.
    let wal_bounds = WalObjectBounds::from_span_records(&records);
    let mt = state.memtable_for(tenant, partition);
    mt.append_many(records);

    // Synchronously flush to WAL.
    let batch = mt.flush().map_err(http_err)?;
    let writer = WalWriter::new(state.store.clone());
    let (key, wal_bytes) = writer
        .flush_with_size(
            tenant,
            partition,
            commit_id,
            Schema::spans_v1().fingerprint(),
            &batch,
        )
        .await
        .map_err(http_err)?;

    state
        .catalog
        .register_wal_object(WalObjectRow {
            wal_id: uuid::Uuid::new_v4(),
            tenant_id: tenant,
            partition_id: partition,
            object_key: key.to_string(),
            commit_id_min: commit_id,
            commit_id_max: CommitId(commit_id.0 + n as u64 - 1),
            byte_count: wal_bytes as i64,
            row_count: n as i64,
            time_min: wal_bounds.time_min,
            time_max: wal_bounds.time_max,
            trace_id_min: wal_bounds.trace_id_min,
            trace_id_max: wal_bounds.trace_id_max,
            schema_fingerprint: Schema::spans_v1().fingerprint(),
            consumed_at: None,
            created_at: chrono::Utc::now(),
        })
        .await
        .map_err(http_err)?;

    Ok(Json(IngestResponse {
        spans_accepted: n as u32,
        wal_object_key: key.to_string(),
    }))
}

fn http_err<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}"))
}
