//! gRPC server: implements `IngestService` and `QueryService` from `zen_proto`.

use chrono::Utc;
use tonic::{Request, Response, Status};

use zen_catalog::model::WalObjectRow;
use zen_common::{CommitId, PartitionId, Schema, SpanId, SpanRecord, TenantId, TraceId};
use zen_proto::v1::{
    ingest_service_server::{IngestService, IngestServiceServer},
    query_service_server::{QueryService, QueryServiceServer},
    IngestRequest, IngestResponse, QueryRequest, QueryResponse,
};
use zen_query::execute;
use zen_wal::WalWriter;

use crate::state::ServerState;

pub struct GrpcIngest {
    pub state: ServerState,
}

#[tonic::async_trait]
impl IngestService for GrpcIngest {
    async fn ingest(
        &self,
        req: Request<IngestRequest>,
    ) -> Result<Response<IngestResponse>, Status> {
        let req = req.into_inner();
        if req.spans.is_empty() {
            return Ok(Response::new(IngestResponse {
                spans_accepted: 0,
                wal_object_key: String::new(),
            }));
        }
        // Tenant + partition come from the first span; in production this should be
        // header-based and validated against auth. Same convention as the HTTP path.
        let tenant = TenantId(req.spans[0].tenant_id);
        let partition = PartitionId(req.spans[0].partition_id);

        self.state
            .catalog
            .ensure_tenant(tenant, "")
            .await
            .map_err(|e| Status::internal(format!("{e}")))?;
        self.state
            .catalog
            .ensure_partition(tenant, partition)
            .await
            .map_err(|e| Status::internal(format!("{e}")))?;

        let n = req.spans.len();
        let commit_id = self
            .state
            .catalog
            .next_commit_id(tenant, partition)
            .await
            .map_err(|e| Status::internal(format!("{e}")))?;

        let records: Vec<SpanRecord> = req
            .spans
            .into_iter()
            .enumerate()
            .map(|(i, s)| SpanRecord {
                tenant_id: tenant,
                partition_id: partition,
                trace_id: bytes_to_trace_id(&s.trace_id),
                span_id: bytes_to_span_id(&s.span_id),
                parent_span_id: if s.parent_span_id.is_empty() {
                    None
                } else {
                    Some(bytes_to_span_id(&s.parent_span_id))
                },
                start_time_ms: s.start_time_ms,
                end_time_ms: s.end_time_ms,
                duration_ms: s.duration_ms,
                span_type: opt(&s.span_type),
                status: opt(&s.status),
                provider: opt(&s.provider),
                model: opt(&s.model),
                tool_name: opt(&s.tool_name),
                prompt: opt(&s.prompt),
                completion: opt(&s.completion),
                prompt_tokens: if s.prompt_tokens == 0 { None } else { Some(s.prompt_tokens) },
                completion_tokens: if s.completion_tokens == 0 { None } else { Some(s.completion_tokens) },
                cost_usd: if s.cost_usd == 0.0 { None } else { Some(s.cost_usd) },
                temperature: if s.temperature == 0.0 { None } else { Some(s.temperature) },
                top_p: if s.top_p == 0.0 { None } else { Some(s.top_p) },
                tool_io_text: opt(&s.tool_io_text),
                user_id: opt(&s.user_id),
                session_id: opt(&s.session_id),
                request_id: opt(&s.request_id),
                metadata: if s.metadata_json.is_empty() {
                    None
                } else {
                    serde_json::from_slice(&s.metadata_json).ok()
                },
                embedding: if s.embedding.is_empty() {
                    None
                } else {
                    Some(s.embedding)
                },
                commit_id: CommitId(commit_id.0 + i as u64),
            })
            .collect();

        let mt = self.state.memtable_for(tenant, partition);
        mt.append_many(records);
        let batch = mt
            .flush()
            .map_err(|e| Status::internal(format!("{e}")))?;
        let writer = WalWriter::new(self.state.store.clone());
        let key = writer
            .flush(
                tenant,
                partition,
                commit_id,
                Schema::spans_v1().fingerprint(),
                &batch,
            )
            .await
            .map_err(|e| Status::internal(format!("{e}")))?;
        self.state
            .catalog
            .register_wal_object(WalObjectRow {
                wal_id: uuid::Uuid::new_v4(),
                tenant_id: tenant,
                partition_id: partition,
                object_key: key.to_string(),
                commit_id_min: commit_id,
                commit_id_max: CommitId(commit_id.0 + n as u64 - 1),
                byte_count: 0,
                row_count: n as i64,
                schema_fingerprint: Schema::spans_v1().fingerprint(),
                consumed_at: None,
                created_at: Utc::now(),
            })
            .await
            .map_err(|e| Status::internal(format!("{e}")))?;

        Ok(Response::new(IngestResponse {
            spans_accepted: n as u64,
            wal_object_key: key.to_string(),
        }))
    }
}

pub struct GrpcQuery {
    pub state: ServerState,
}

#[tonic::async_trait]
impl QueryService for GrpcQuery {
    async fn query(
        &self,
        req: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let req = req.into_inner();
        let plan = zen_ql::parse(&req.query, req.tenant_id)
            .map_err(|e| Status::invalid_argument(format!("parse: {e}")))?;
        let result = execute(&plan, self.state.catalog.clone(), self.state.store.clone())
            .await
            .map_err(|e| Status::internal(format!("{e}")))?;
        let rows_json: Vec<String> = result
            .rows
            .iter()
            .map(|r| serde_json::to_string(&r.fields).unwrap_or_default())
            .collect();
        Ok(Response::new(QueryResponse {
            rows_json,
            segments_scanned: result.stats.segments_scanned,
            row_groups_scanned: result.stats.row_groups_scanned,
            row_groups_pruned: result.stats.row_groups_pruned,
            elapsed_ms: result.stats.elapsed_ms,
        }))
    }
}

pub async fn serve(
    state: ServerState,
    addr: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: std::net::SocketAddr = addr.parse()?;
    let ingest = GrpcIngest { state: state.clone() };
    let query = GrpcQuery { state };
    tracing::info!(%addr, "zenithdb grpc listening");
    tonic::transport::Server::builder()
        .add_service(IngestServiceServer::new(ingest))
        .add_service(QueryServiceServer::new(query))
        .serve(addr)
        .await?;
    Ok(())
}

fn bytes_to_trace_id(b: &[u8]) -> TraceId {
    let mut a = [0u8; 16];
    let n = b.len().min(16);
    a[..n].copy_from_slice(&b[..n]);
    TraceId(a)
}

fn bytes_to_span_id(b: &[u8]) -> SpanId {
    let mut a = [0u8; 16];
    let n = b.len().min(16);
    a[..n].copy_from_slice(&b[..n]);
    SpanId(a)
}

fn opt(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}
