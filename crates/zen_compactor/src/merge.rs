//! Read WAL objects, parse to `SpanRecord` rows, sort by
//! `(trace_id, start_time, span_id)`, apply tombstones (later commit_id wins).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    Array, FixedSizeBinaryArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt32Array,
    UInt64Array,
};

use zen_common::{
    CommitId, PartitionId, SpanId, SpanRecord, TenantId, TraceId, ZenResult,
};
use zen_storage::BlobStore;
use zen_wal::WalReader;

pub struct MergedRows {
    pub rows: Vec<SpanRecord>,
    pub commit_id_max: CommitId,
}

/// Merge WAL objects from `keys` into a single sorted, tombstone-resolved list.
pub async fn merge_wals(
    store: Arc<dyn BlobStore>,
    wal_keys: &[String],
) -> ZenResult<MergedRows> {
    if wal_keys.is_empty() {
        return Ok(MergedRows {
            rows: Vec::new(),
            commit_id_max: CommitId(0),
        });
    }
    let mut all: Vec<SpanRecord> = Vec::new();
    let mut commit_id_max = CommitId(0);
    for k in wal_keys {
        let bytes = store.get(k).await?;
        let (header, batches) = WalReader::parse(&bytes)?;
        if header.commit_id.0 > commit_id_max.0 {
            commit_id_max = header.commit_id;
        }
        for b in batches {
            arrow_to_records(&b, &mut all)?;
        }
    }

    // Sort globally for trace-locality.
    all.sort_by(|a, b| {
        a.trace_id
            .0
            .cmp(&b.trace_id.0)
            .then_with(|| a.start_time_ms.cmp(&b.start_time_ms))
            .then_with(|| a.span_id.0.cmp(&b.span_id.0))
            .then_with(|| b.commit_id.0.cmp(&a.commit_id.0)) // higher commit first
    });

    // Tombstone resolution: keep first occurrence of each (tenant, span_id) — which is
    // the highest commit_id by sort tiebreaker.
    let mut seen: HashMap<(u64, [u8; 16]), bool> = HashMap::new();
    let mut deduped: Vec<SpanRecord> = Vec::with_capacity(all.len());
    for r in all {
        let key = (r.tenant_id.0, r.span_id.0);
        if seen.insert(key, true).is_none() {
            deduped.push(r);
        }
    }
    Ok(MergedRows {
        rows: deduped,
        commit_id_max,
    })
}

fn arrow_to_records(batch: &RecordBatch, out: &mut Vec<SpanRecord>) -> ZenResult<()> {
    let schema = batch.schema();
    let n = batch.num_rows();
    let col = |name: &str| -> Option<&dyn Array> {
        schema.index_of(name).ok().map(|i| batch.column(i).as_ref())
    };

    let tenant = col("tenant_id");
    let partition = col("partition_id");
    let trace_id = col("trace_id");
    let span_id = col("span_id");
    let parent_span_id = col("parent_span_id");
    let start_time = col("start_time_ms");
    let end_time = col("end_time_ms");
    let duration = col("duration_ms");
    let span_type = col("span_type");
    let status = col("status");
    let provider = col("provider");
    let model = col("model");
    let tool_name = col("tool_name");
    let prompt = col("prompt");
    let completion = col("completion");
    let prompt_tokens = col("prompt_tokens");
    let completion_tokens = col("completion_tokens");
    let cost_usd = col("cost_usd");
    let temperature = col("temperature");
    let top_p = col("top_p");
    let tool_io_text = col("tool_io_text");
    let user_id = col("user_id");
    let session_id = col("session_id");
    let request_id = col("request_id");
    let metadata_json = col("metadata_json");
    let commit_id = col("commit_id");

    for i in 0..n {
        let mut r = SpanRecord::default();
        if let Some(a) = tenant.and_then(|a| a.as_any().downcast_ref::<UInt64Array>()) {
            r.tenant_id = TenantId(a.value(i));
        }
        if let Some(a) = partition.and_then(|a| a.as_any().downcast_ref::<UInt32Array>()) {
            r.partition_id = PartitionId(a.value(i));
        }
        if let Some(a) = trace_id.and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>()) {
            let mut tid = [0u8; 16];
            tid.copy_from_slice(a.value(i));
            r.trace_id = TraceId(tid);
        }
        if let Some(a) = span_id.and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>()) {
            let mut sid = [0u8; 16];
            sid.copy_from_slice(a.value(i));
            r.span_id = SpanId(sid);
        }
        if let Some(a) =
            parent_span_id.and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>())
        {
            if !a.is_null(i) {
                let mut p = [0u8; 16];
                p.copy_from_slice(a.value(i));
                r.parent_span_id = Some(SpanId(p));
            }
        }
        if let Some(a) = start_time.and_then(|a| a.as_any().downcast_ref::<Int64Array>()) {
            r.start_time_ms = a.value(i);
        }
        if let Some(a) = end_time.and_then(|a| a.as_any().downcast_ref::<Int64Array>()) {
            r.end_time_ms = a.value(i);
        }
        if let Some(a) = duration.and_then(|a| a.as_any().downcast_ref::<Int64Array>()) {
            r.duration_ms = a.value(i);
        }
        if let Some(a) = span_type.and_then(|a| a.as_any().downcast_ref::<StringArray>()) {
            if !a.is_null(i) {
                r.span_type = Some(a.value(i).to_string());
            }
        }
        if let Some(a) = status.and_then(|a| a.as_any().downcast_ref::<StringArray>()) {
            if !a.is_null(i) {
                r.status = Some(a.value(i).to_string());
            }
        }
        if let Some(a) = provider.and_then(|a| a.as_any().downcast_ref::<StringArray>()) {
            if !a.is_null(i) {
                r.provider = Some(a.value(i).to_string());
            }
        }
        if let Some(a) = model.and_then(|a| a.as_any().downcast_ref::<StringArray>()) {
            if !a.is_null(i) {
                r.model = Some(a.value(i).to_string());
            }
        }
        if let Some(a) = tool_name.and_then(|a| a.as_any().downcast_ref::<StringArray>()) {
            if !a.is_null(i) {
                r.tool_name = Some(a.value(i).to_string());
            }
        }
        if let Some(a) = prompt.and_then(|a| a.as_any().downcast_ref::<StringArray>()) {
            if !a.is_null(i) {
                r.prompt = Some(a.value(i).to_string());
            }
        }
        if let Some(a) = completion.and_then(|a| a.as_any().downcast_ref::<StringArray>()) {
            if !a.is_null(i) {
                r.completion = Some(a.value(i).to_string());
            }
        }
        if let Some(a) = prompt_tokens.and_then(|a| a.as_any().downcast_ref::<UInt32Array>()) {
            if !a.is_null(i) {
                r.prompt_tokens = Some(a.value(i));
            }
        }
        if let Some(a) = completion_tokens.and_then(|a| a.as_any().downcast_ref::<UInt32Array>()) {
            if !a.is_null(i) {
                r.completion_tokens = Some(a.value(i));
            }
        }
        if let Some(a) = cost_usd.and_then(|a| a.as_any().downcast_ref::<Float64Array>()) {
            if !a.is_null(i) {
                r.cost_usd = Some(a.value(i));
            }
        }
        if let Some(a) = temperature.and_then(|a| a.as_any().downcast_ref::<Float64Array>()) {
            if !a.is_null(i) {
                r.temperature = Some(a.value(i));
            }
        }
        if let Some(a) = top_p.and_then(|a| a.as_any().downcast_ref::<Float64Array>()) {
            if !a.is_null(i) {
                r.top_p = Some(a.value(i));
            }
        }
        if let Some(a) = tool_io_text.and_then(|a| a.as_any().downcast_ref::<StringArray>()) {
            if !a.is_null(i) {
                r.tool_io_text = Some(a.value(i).to_string());
            }
        }
        if let Some(a) = user_id.and_then(|a| a.as_any().downcast_ref::<StringArray>()) {
            if !a.is_null(i) {
                r.user_id = Some(a.value(i).to_string());
            }
        }
        if let Some(a) = session_id.and_then(|a| a.as_any().downcast_ref::<StringArray>()) {
            if !a.is_null(i) {
                r.session_id = Some(a.value(i).to_string());
            }
        }
        if let Some(a) = request_id.and_then(|a| a.as_any().downcast_ref::<StringArray>()) {
            if !a.is_null(i) {
                r.request_id = Some(a.value(i).to_string());
            }
        }
        if let Some(a) = metadata_json.and_then(|a| a.as_any().downcast_ref::<StringArray>()) {
            if !a.is_null(i) {
                let s = a.value(i);
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
                    r.metadata = Some(v);
                }
            }
        }
        if let Some(a) = commit_id.and_then(|a| a.as_any().downcast_ref::<UInt64Array>()) {
            r.commit_id = CommitId(a.value(i));
        }
        out.push(r);
    }
    Ok(())
}
