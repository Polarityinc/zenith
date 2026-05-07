//! OTLP ingest endpoint.
//!
//! Accepts OTLP/JSON `ExportTraceServiceRequest` payloads and maps each span
//! onto a `SpanRecord`. The mapping follows the OpenTelemetry GenAI semantic
//! convention attributes:
//!
//!   gen_ai.system               → provider
//!   gen_ai.request.model        → model
//!   gen_ai.usage.prompt_tokens  → prompt_tokens
//!   gen_ai.usage.completion_tokens → completion_tokens
//!   gen_ai.request.temperature  → temperature
//!   gen_ai.request.top_p        → top_p
//!   gen_ai.prompt               → prompt
//!   gen_ai.completion           → completion
//!
//! Plus the standard OTel span fields (trace_id, span_id, name, status, time).

use axum::{extract::State, http::StatusCode, Json};
use serde::Deserialize;
use serde_json::Value;

use zen_catalog::model::WalObjectRow;
use zen_common::{CommitId, PartitionId, Schema, SpanId, SpanRecord, TenantId, TraceId};
use zen_memtable::flush_to_record_batch;
use zen_wal::WalWriter;

use crate::state::ServerState;

#[derive(Debug, Deserialize)]
pub struct OtlpExportTraceRequest {
    #[serde(rename = "resourceSpans", default)]
    pub resource_spans: Vec<ResourceSpans>,
}

#[derive(Debug, Deserialize)]
pub struct ResourceSpans {
    #[serde(default)]
    pub resource: Option<Resource>,
    #[serde(rename = "scopeSpans", default)]
    pub scope_spans: Vec<ScopeSpans>,
}

#[derive(Debug, Deserialize)]
pub struct Resource {
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
}

#[derive(Debug, Deserialize)]
pub struct ScopeSpans {
    #[serde(rename = "spans", default)]
    pub spans: Vec<OtlpSpan>,
}

#[derive(Debug, Deserialize)]
pub struct OtlpSpan {
    #[serde(rename = "traceId", default)]
    pub trace_id: String,
    #[serde(rename = "spanId", default)]
    pub span_id: String,
    #[serde(rename = "parentSpanId", default)]
    pub parent_span_id: String,
    #[serde(default)]
    pub name: String,
    #[serde(rename = "startTimeUnixNano", default)]
    pub start_time_unix_nano: String,
    #[serde(rename = "endTimeUnixNano", default)]
    pub end_time_unix_nano: String,
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
    #[serde(default)]
    pub status: Option<Status>,
}

#[derive(Debug, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: Value,
}

#[derive(Debug, Deserialize)]
pub struct Status {
    #[serde(default)]
    pub code: u32, // 0 unset, 1 ok, 2 error
    #[serde(default)]
    pub message: String,
}

pub async fn handle_otlp_traces(
    State(state): State<ServerState>,
    Json(req): Json<OtlpExportTraceRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let tenant = TenantId(0); // OTLP doesn't carry tenant; assume default. Header-driven routing in real deploys.
    let partition = PartitionId(0);

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

    let mut records: Vec<SpanRecord> = Vec::new();
    for rs in req.resource_spans {
        let resource_provider = rs
            .resource
            .as_ref()
            .and_then(|r| attr_str(&r.attributes, "service.name"));
        for ss in rs.scope_spans {
            for s in ss.spans {
                if let Some(rec) = otlp_to_span(s, tenant, partition, resource_provider.as_deref()) {
                    records.push(rec);
                }
            }
        }
    }
    if records.is_empty() {
        return Ok(Json(serde_json::json!({"spans_accepted": 0})));
    }

    // Allocate commit_ids and flush to WAL.
    let n = records.len();
    let commit_id = state
        .catalog
        .next_commit_id(tenant, partition)
        .await
        .map_err(http_err)?;
    for (i, r) in records.iter_mut().enumerate() {
        r.commit_id = CommitId(commit_id.0 + i as u64);
    }
    let mt = state.memtable_for(tenant, partition);
    mt.append_many(records);

    let batch = mt.flush().map_err(http_err)?;
    let writer = WalWriter::new(state.store.clone());
    let key = writer
        .flush(
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
            byte_count: 0,
            row_count: n as i64,
            schema_fingerprint: Schema::spans_v1().fingerprint(),
            consumed_at: None,
            created_at: chrono::Utc::now(),
        })
        .await
        .map_err(http_err)?;

    Ok(Json(serde_json::json!({
        "spans_accepted": n,
        "wal_object_key": key.to_string(),
    })))
}

fn otlp_to_span(
    s: OtlpSpan,
    tenant: TenantId,
    partition: PartitionId,
    fallback_provider: Option<&str>,
) -> Option<SpanRecord> {
    let trace_id = parse_hex_id(&s.trace_id);
    let span_id_bytes = parse_hex_id_8(&s.span_id);
    let parent_span_id = if s.parent_span_id.is_empty() {
        None
    } else {
        Some(SpanId(parse_hex_id_8(&s.parent_span_id)))
    };
    let start_ms = parse_unix_nano_to_ms(&s.start_time_unix_nano);
    let end_ms = parse_unix_nano_to_ms(&s.end_time_unix_nano);
    let duration_ms = end_ms.saturating_sub(start_ms);

    let provider = attr_str(&s.attributes, "gen_ai.system")
        .or_else(|| fallback_provider.map(|s| s.to_string()));
    let model = attr_str(&s.attributes, "gen_ai.request.model")
        .or_else(|| attr_str(&s.attributes, "gen_ai.response.model"));
    let prompt_tokens = attr_int(&s.attributes, "gen_ai.usage.prompt_tokens")
        .or_else(|| attr_int(&s.attributes, "gen_ai.usage.input_tokens"))
        .map(|x| x as u32);
    let completion_tokens = attr_int(&s.attributes, "gen_ai.usage.completion_tokens")
        .or_else(|| attr_int(&s.attributes, "gen_ai.usage.output_tokens"))
        .map(|x| x as u32);
    let temperature = attr_float(&s.attributes, "gen_ai.request.temperature");
    let top_p = attr_float(&s.attributes, "gen_ai.request.top_p");
    let prompt = attr_str(&s.attributes, "gen_ai.prompt");
    let completion = attr_str(&s.attributes, "gen_ai.completion");
    let user_id = attr_str(&s.attributes, "user.id");
    let session_id = attr_str(&s.attributes, "session.id");
    let request_id = attr_str(&s.attributes, "http.request.id")
        .or_else(|| attr_str(&s.attributes, "request.id"));
    let tool_name = attr_str(&s.attributes, "gen_ai.tool.name");

    let span_type = if model.is_some() {
        Some("llm_call".to_string())
    } else if tool_name.is_some() {
        Some("tool_call".to_string())
    } else {
        Some("agent_step".to_string())
    };

    let status = match s.status.as_ref().map(|s| s.code).unwrap_or(0) {
        2 => Some("error".to_string()),
        1 => Some("ok".to_string()),
        _ => None,
    };

    Some(SpanRecord {
        tenant_id: tenant,
        partition_id: partition,
        trace_id: TraceId(trace_id),
        span_id: SpanId(span_id_bytes),
        parent_span_id,
        start_time_ms: start_ms,
        end_time_ms: end_ms,
        duration_ms,
        span_type,
        status,
        provider,
        model,
        tool_name,
        prompt,
        completion,
        prompt_tokens,
        completion_tokens,
        cost_usd: None,
        temperature,
        top_p,
        tool_io_text: None,
        user_id,
        session_id,
        request_id,
        metadata: Some(serde_json::json!({
            "otel_span_name": s.name,
            "attributes": attr_to_json(&s.attributes),
        })),
        embedding: None,
        commit_id: CommitId(0),
    })
}

fn attr_str(attrs: &[KeyValue], key: &str) -> Option<String> {
    for kv in attrs {
        if kv.key == key {
            if let Some(s) = kv
                .value
                .get("stringValue")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
            {
                return Some(s);
            }
        }
    }
    None
}

fn attr_int(attrs: &[KeyValue], key: &str) -> Option<i64> {
    for kv in attrs {
        if kv.key == key {
            if let Some(v) = kv.value.get("intValue") {
                if let Some(i) = v.as_i64() {
                    return Some(i);
                }
                if let Some(s) = v.as_str() {
                    if let Ok(i) = s.parse::<i64>() {
                        return Some(i);
                    }
                }
            }
        }
    }
    None
}

fn attr_float(attrs: &[KeyValue], key: &str) -> Option<f64> {
    for kv in attrs {
        if kv.key == key {
            if let Some(f) = kv.value.get("doubleValue").and_then(|v| v.as_f64()) {
                return Some(f);
            }
        }
    }
    None
}

fn attr_to_json(attrs: &[KeyValue]) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for kv in attrs {
        let v = kv
            .value
            .get("stringValue")
            .cloned()
            .or_else(|| kv.value.get("intValue").cloned())
            .or_else(|| kv.value.get("doubleValue").cloned())
            .or_else(|| kv.value.get("boolValue").cloned())
            .unwrap_or(serde_json::Value::Null);
        map.insert(kv.key.clone(), v);
    }
    serde_json::Value::Object(map)
}

fn parse_hex_id(s: &str) -> [u8; 16] {
    let mut out = [0u8; 16];
    let bytes = hex_decode(s);
    let n = bytes.len().min(16);
    out[16 - n..].copy_from_slice(&bytes[..n]);
    out
}

fn parse_hex_id_8(s: &str) -> [u8; 16] {
    // OTLP span IDs are 8 bytes; we left-pad with zeros into our 16-byte SpanId.
    let mut out = [0u8; 16];
    let bytes = hex_decode(s);
    let n = bytes.len().min(8);
    out[16 - n..].copy_from_slice(&bytes[..n]);
    out
}

fn hex_decode(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i + 1 < bytes.len() {
        let h = hex_nibble(bytes[i]);
        let l = hex_nibble(bytes[i + 1]);
        out.push((h << 4) | l);
        i += 2;
    }
    out
}

fn hex_nibble(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => 0,
    }
}

fn parse_unix_nano_to_ms(s: &str) -> i64 {
    s.parse::<i64>().map(|n| n / 1_000_000).unwrap_or(0)
}

fn http_err<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_genai_attrs() {
        let raw = r#"{
            "traceId": "0123456789abcdef0123456789abcdef",
            "spanId": "0123456789abcdef",
            "parentSpanId": "",
            "name": "openai.chat",
            "startTimeUnixNano": "1700000000000000000",
            "endTimeUnixNano": "1700000001500000000",
            "attributes": [
                {"key":"gen_ai.system","value":{"stringValue":"openai"}},
                {"key":"gen_ai.request.model","value":{"stringValue":"gpt-4o"}},
                {"key":"gen_ai.usage.prompt_tokens","value":{"intValue":"42"}},
                {"key":"gen_ai.usage.completion_tokens","value":{"intValue":"100"}},
                {"key":"gen_ai.request.temperature","value":{"doubleValue":0.7}}
            ],
            "status": {"code":1, "message":""}
        }"#;
        let s: OtlpSpan = serde_json::from_str(raw).unwrap();
        let rec = otlp_to_span(s, TenantId(0), PartitionId(0), None).unwrap();
        assert_eq!(rec.provider.as_deref(), Some("openai"));
        assert_eq!(rec.model.as_deref(), Some("gpt-4o"));
        assert_eq!(rec.prompt_tokens, Some(42));
        assert_eq!(rec.completion_tokens, Some(100));
        assert_eq!(rec.temperature, Some(0.7));
        assert_eq!(rec.span_type.as_deref(), Some("llm_call"));
        assert_eq!(rec.status.as_deref(), Some("ok"));
        assert_eq!(rec.duration_ms, 1500);
    }

    #[test]
    fn parses_hex_ids() {
        let id = parse_hex_id("0123456789abcdef0123456789abcdef");
        assert_eq!(
            id,
            [
                0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
                0xcd, 0xef
            ]
        );
        let s = parse_hex_id_8("0123456789abcdef");
        // Span ID is 8 bytes, left-padded into 16-byte slot.
        assert_eq!(&s[8..], &[0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]);
    }
}
