//! Convert sorted `SpanRecord`s to an Arrow `RecordBatch`.

use std::sync::Arc;

use arrow_array::builder::{
    FixedSizeBinaryBuilder, Float64Builder, Int64Builder, StringBuilder, UInt32Builder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};

use zen_common::{SpanRecord, ZenError, ZenResult};

use crate::arrow_schema::spans_arrow_schema;

pub fn flush_to_record_batch(rows: &[SpanRecord]) -> ZenResult<RecordBatch> {
    let n = rows.len();
    let mut tenant = UInt64Builder::with_capacity(n);
    let mut partition = UInt32Builder::with_capacity(n);
    let mut trace_id = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut span_id = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut parent_span_id = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut start_time = Int64Builder::with_capacity(n);
    let mut end_time = Int64Builder::with_capacity(n);
    let mut duration = Int64Builder::with_capacity(n);
    let mut span_type = StringBuilder::with_capacity(n, 64);
    let mut status = StringBuilder::with_capacity(n, 64);
    let mut provider = StringBuilder::with_capacity(n, 64);
    let mut model = StringBuilder::with_capacity(n, 64);
    let mut tool_name = StringBuilder::with_capacity(n, 64);
    let mut prompt = StringBuilder::with_capacity(n, 1024);
    let mut completion = StringBuilder::with_capacity(n, 1024);
    let mut prompt_tokens = UInt32Builder::with_capacity(n);
    let mut completion_tokens = UInt32Builder::with_capacity(n);
    let mut cost_usd = Float64Builder::with_capacity(n);
    let mut temperature = Float64Builder::with_capacity(n);
    let mut top_p = Float64Builder::with_capacity(n);
    let mut tool_io_text = StringBuilder::with_capacity(n, 256);
    let mut user_id = StringBuilder::with_capacity(n, 64);
    let mut session_id = StringBuilder::with_capacity(n, 64);
    let mut request_id = StringBuilder::with_capacity(n, 64);
    let mut metadata_json = StringBuilder::with_capacity(n, 256);
    let mut commit_id = UInt64Builder::with_capacity(n);

    for r in rows {
        tenant.append_value(r.tenant_id.0);
        partition.append_value(r.partition_id.0);
        trace_id
            .append_value(r.trace_id.as_bytes())
            .map_err(|e| ZenError::format(format!("trace_id append: {e}")))?;
        span_id
            .append_value(r.span_id.as_bytes())
            .map_err(|e| ZenError::format(format!("span_id append: {e}")))?;
        match r.parent_span_id {
            Some(p) => parent_span_id
                .append_value(p.as_bytes())
                .map_err(|e| ZenError::format(format!("parent_span_id append: {e}")))?,
            None => parent_span_id.append_null(),
        }
        start_time.append_value(r.start_time_ms);
        end_time.append_value(r.end_time_ms);
        duration.append_value(r.duration_ms);
        push_opt_str(&mut span_type, r.span_type.as_deref());
        push_opt_str(&mut status, r.status.as_deref());
        push_opt_str(&mut provider, r.provider.as_deref());
        push_opt_str(&mut model, r.model.as_deref());
        push_opt_str(&mut tool_name, r.tool_name.as_deref());
        push_opt_str(&mut prompt, r.prompt.as_deref());
        push_opt_str(&mut completion, r.completion.as_deref());
        push_opt_u32(&mut prompt_tokens, r.prompt_tokens);
        push_opt_u32(&mut completion_tokens, r.completion_tokens);
        push_opt_f64(&mut cost_usd, r.cost_usd);
        push_opt_f64(&mut temperature, r.temperature);
        push_opt_f64(&mut top_p, r.top_p);
        push_opt_str(&mut tool_io_text, r.tool_io_text.as_deref());
        push_opt_str(&mut user_id, r.user_id.as_deref());
        push_opt_str(&mut session_id, r.session_id.as_deref());
        push_opt_str(&mut request_id, r.request_id.as_deref());
        match &r.metadata {
            Some(v) => metadata_json.append_value(serde_json::to_string(v).unwrap_or_default()),
            None => metadata_json.append_null(),
        }
        commit_id.append_value(r.commit_id.0);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(tenant.finish()),
        Arc::new(partition.finish()),
        Arc::new(trace_id.finish()),
        Arc::new(span_id.finish()),
        Arc::new(parent_span_id.finish()),
        Arc::new(start_time.finish()),
        Arc::new(end_time.finish()),
        Arc::new(duration.finish()),
        Arc::new(span_type.finish()),
        Arc::new(status.finish()),
        Arc::new(provider.finish()),
        Arc::new(model.finish()),
        Arc::new(tool_name.finish()),
        Arc::new(prompt.finish()),
        Arc::new(completion.finish()),
        Arc::new(prompt_tokens.finish()),
        Arc::new(completion_tokens.finish()),
        Arc::new(cost_usd.finish()),
        Arc::new(temperature.finish()),
        Arc::new(top_p.finish()),
        Arc::new(tool_io_text.finish()),
        Arc::new(user_id.finish()),
        Arc::new(session_id.finish()),
        Arc::new(request_id.finish()),
        Arc::new(metadata_json.finish()),
        Arc::new(commit_id.finish()),
    ];
    let schema = spans_arrow_schema();
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| ZenError::format(format!("record batch: {e}")))
}

fn push_opt_str(b: &mut arrow_array::builder::StringBuilder, v: Option<&str>) {
    match v {
        Some(s) => b.append_value(s),
        None => b.append_null(),
    }
}

fn push_opt_u32(b: &mut arrow_array::builder::UInt32Builder, v: Option<u32>) {
    match v {
        Some(x) => b.append_value(x),
        None => b.append_null(),
    }
}

fn push_opt_f64(b: &mut arrow_array::builder::Float64Builder, v: Option<f64>) {
    match v {
        Some(x) => b.append_value(x),
        None => b.append_null(),
    }
}
