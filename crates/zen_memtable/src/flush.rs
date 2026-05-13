//! Convert sorted `SpanRecord`s to an Arrow `RecordBatch`.

use std::sync::Arc;

use arrow_array::builder::{
    FixedSizeBinaryBuilder, Float64Builder, Int64Builder, StringBuilder, UInt32Builder,
    UInt64Builder,
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
    RecordBatch::try_new(schema, arrays).map_err(|e| ZenError::format(format!("record batch: {e}")))
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Cursor;

    use arrow_ipc::reader::StreamReader;
    use arrow_ipc::writer::StreamWriter;
    use proptest::prelude::*;

    use zen_common::{CommitId, PartitionId, SpanId, TenantId, TraceId};

    fn record_with_seed(seed: u64) -> SpanRecord {
        let mut r = SpanRecord::new(TenantId(1), PartitionId(0));
        // Deterministic identifiers so tests don't depend on system clock /
        // ulid randomness.
        r.trace_id = TraceId([(seed & 0xff) as u8; 16]);
        r.span_id = SpanId([((seed >> 8) & 0xff) as u8; 16]);
        r.parent_span_id = if seed % 2 == 0 {
            None
        } else {
            Some(SpanId([((seed >> 16) & 0xff) as u8; 16]))
        };
        r.start_time_ms = (seed % 1_000_000) as i64;
        r.end_time_ms = r.start_time_ms + 100;
        r.duration_ms = 100;
        r.span_type = Some("llm_call".into());
        r.status = Some("ok".into());
        r.provider = Some("openai".into());
        r.model = Some("gpt-4o".into());
        r.tool_name = None;
        r.prompt = Some(format!("prompt-{seed}"));
        r.completion = Some(format!("completion-{seed}"));
        r.prompt_tokens = Some(((seed % 1000) as u32) + 1);
        r.completion_tokens = Some(((seed % 500) as u32) + 1);
        r.cost_usd = Some(0.01_f64 * (seed as f64));
        r.temperature = Some(0.7);
        r.top_p = Some(0.95);
        r.tool_io_text = None;
        r.user_id = Some(format!("user-{}", seed % 7));
        r.session_id = Some(format!("sess-{}", seed % 13));
        r.request_id = Some(format!("req-{seed}"));
        r.metadata = Some(serde_json::json!({ "k": seed }));
        r.commit_id = CommitId(seed);
        r
    }

    #[test]
    fn flush_to_record_batch_100_rows_has_expected_schema() {
        let rows: Vec<SpanRecord> = (0..100u64).map(record_with_seed).collect();
        let batch = flush_to_record_batch(&rows).unwrap();
        assert_eq!(batch.num_rows(), 100, "100 rows expected");
        // Match the canonical Arrow schema bit-for-bit.
        let want = spans_arrow_schema();
        assert_eq!(batch.schema().fields(), want.fields());
        assert_eq!(batch.num_columns(), want.fields().len());
    }

    #[test]
    fn flush_to_record_batch_empty_returns_zero_row_batch() {
        // The implementation does not flag empty as an error; it returns a
        // valid 0-row RecordBatch with the canonical schema. We pin that.
        let batch = flush_to_record_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        let want = spans_arrow_schema();
        assert_eq!(batch.schema().fields(), want.fields());
    }

    #[test]
    fn flush_to_record_batch_handles_all_optional_nulls() {
        // Append a row with every Option field as None — must not panic and
        // must produce nullable columns at index N=0.
        let r = SpanRecord::new(TenantId(2), PartitionId(1));
        let batch = flush_to_record_batch(&[r]).unwrap();
        assert_eq!(batch.num_rows(), 1);
        // Column "parent_span_id" at index 4 should be all-null.
        let parent = batch.column(4);
        assert_eq!(parent.null_count(), 1);
    }

    fn arb_record() -> impl Strategy<Value = SpanRecord> {
        // A small but representative shape: vary identity + payload sizes,
        // include a mixture of Some/None across the optional columns.
        (
            any::<u64>(), // tenant
            any::<u32>(), // partition
            any::<[u8; 16]>(),
            any::<[u8; 16]>(),
            proptest::option::of(any::<[u8; 16]>()),
            any::<i64>(),
            any::<i64>(),
            proptest::option::of(".*"),
            proptest::option::of(any::<u32>()),
            proptest::option::of(any::<f64>()),
        )
            .prop_map(
                |(tenant, partition, t, sp, parent, st, et, prompt, ptok, cost)| {
                    let mut r = SpanRecord::new(TenantId(tenant), PartitionId(partition));
                    r.trace_id = TraceId(t);
                    r.span_id = SpanId(sp);
                    r.parent_span_id = parent.map(SpanId);
                    r.start_time_ms = st;
                    r.end_time_ms = et;
                    r.duration_ms = et.wrapping_sub(st);
                    r.prompt = prompt;
                    r.prompt_tokens = ptok;
                    // f64::NaN through Arrow IPC is a valid bit-pattern; only
                    // disallow signaling NaNs and infinities to keep the
                    // round-trip equality check sane.
                    r.cost_usd = cost.filter(|x| x.is_finite());
                    r
                },
            )
    }

    proptest! {
        /// Encode a random batch of records into an Arrow RecordBatch, push
        /// it through the Arrow IPC stream codec, and assert that we can
        /// read it back with the same row + column counts. Catches schema
        /// drift, builder/array length mismatches, and IPC framing bugs in
        /// downstream consumers (the WAL writer uses the same code path).
        #[test]
        fn roundtrip_arrow_ipc(records in proptest::collection::vec(arb_record(), 1..200)) {
            let batch = flush_to_record_batch(&records).unwrap();
            prop_assert_eq!(batch.num_rows(), records.len());

            let mut buf: Vec<u8> = Vec::new();
            {
                let mut w = StreamWriter::try_new(Cursor::new(&mut buf), batch.schema().as_ref())
                    .expect("stream writer");
                w.write(&batch).expect("write");
                w.finish().expect("finish");
            }

            let r = StreamReader::try_new(Cursor::new(&buf), None).expect("reader");
            let mut rows = 0usize;
            let mut got_cols = 0usize;
            for b in r {
                let b = b.expect("batch");
                rows += b.num_rows();
                got_cols = b.num_columns();
            }
            prop_assert_eq!(rows, records.len());
            prop_assert_eq!(got_cols, batch.num_columns());
        }
    }
}
