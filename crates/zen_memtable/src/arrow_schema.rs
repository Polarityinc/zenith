//! Conversion from `zen_common::Schema` to an Arrow schema.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema as ArrowSchema};

pub fn spans_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("tenant_id", DataType::UInt64, false),
        Field::new("partition_id", DataType::UInt32, false),
        Field::new(
            "trace_id",
            DataType::FixedSizeBinary(16),
            false,
        ),
        Field::new("span_id", DataType::FixedSizeBinary(16), false),
        Field::new("parent_span_id", DataType::FixedSizeBinary(16), true),
        Field::new("start_time_ms", DataType::Int64, false),
        Field::new("end_time_ms", DataType::Int64, false),
        Field::new("duration_ms", DataType::Int64, false),
        Field::new("span_type", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("provider", DataType::Utf8, true),
        Field::new("model", DataType::Utf8, true),
        Field::new("tool_name", DataType::Utf8, true),
        Field::new("prompt", DataType::Utf8, true),
        Field::new("completion", DataType::Utf8, true),
        Field::new("prompt_tokens", DataType::UInt32, true),
        Field::new("completion_tokens", DataType::UInt32, true),
        Field::new("cost_usd", DataType::Float64, true),
        Field::new("temperature", DataType::Float64, true),
        Field::new("top_p", DataType::Float64, true),
        Field::new("tool_io_text", DataType::Utf8, true),
        Field::new("user_id", DataType::Utf8, true),
        Field::new("session_id", DataType::Utf8, true),
        Field::new("request_id", DataType::Utf8, true),
        Field::new("metadata_json", DataType::Utf8, true),
        Field::new("commit_id", DataType::UInt64, false),
    ]))
}
