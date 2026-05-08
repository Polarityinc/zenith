//! Logical schema for spans.
//!
//! The schema is the contract between the writer and the reader. It is identified
//! by a 128-bit fingerprint computed from a canonical encoding of the column list,
//! so two writers using the same schema produce bit-compatible segments.

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_128;

use crate::types::SchemaFingerprint;

/// Logical column type. Maps directly onto Arrow types in zen_format.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    Bool,
    Int32,
    Int64,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Binary,
    /// Stored as 16-byte fixed.
    TraceId,
    /// Stored as 16-byte fixed.
    SpanId,
    /// Generic JSON; encoded as length-prefixed binary at the storage layer.
    Json,
    /// Fixed-width float vector. The dimension travels with the column spec.
    FloatVector(u32),
    /// Wall-clock millis since epoch, stored as i64 with delta-of-delta encoding.
    TimestampMillis,
}

/// Hint to the indexing layer; influences segment-build behavior.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexHint {
    /// No additional index built. (Zone maps are always built; they are not optional.)
    None,
    /// Roaring bitmap posting list keyed on `(value_hash)`. Best for low-medium cardinality.
    Bitmap,
    /// Tantivy FTS index over this text column.
    Fts,
    /// JSON-path discovery + indexing. Implies `ColumnType::Json`.
    JsonPath,
    /// HNSW vector index. Implies `ColumnType::FloatVector(_)`.
    Hnsw,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnSpec {
    pub name: String,
    pub ty: ColumnType,
    pub nullable: bool,
    pub index: IndexHint,
    /// Sort-key priority. 0 = not part of sort key; positive = priority order.
    /// The standard span schema uses (1: trace_id, 2: start_time_ms, 3: span_id).
    #[serde(default)]
    pub sort_priority: u8,
}

impl ColumnSpec {
    pub fn new<S: Into<String>>(name: S, ty: ColumnType) -> Self {
        Self {
            name: name.into(),
            ty,
            nullable: true,
            index: IndexHint::None,
            sort_priority: 0,
        }
    }
    pub fn required(mut self) -> Self {
        self.nullable = false;
        self
    }
    pub fn with_index(mut self, h: IndexHint) -> Self {
        self.index = h;
        self
    }
    pub fn with_sort_priority(mut self, p: u8) -> Self {
        self.sort_priority = p;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<ColumnSpec>,
}

impl Schema {
    pub fn new(columns: Vec<ColumnSpec>) -> Self {
        Self { columns }
    }

    /// Stable 128-bit fingerprint over the canonical encoding of the schema.
    pub fn fingerprint(&self) -> SchemaFingerprint {
        let mut buf = String::with_capacity(64 * self.columns.len());
        for c in &self.columns {
            buf.push_str(&c.name);
            buf.push('|');
            buf.push_str(&format!("{:?}", c.ty));
            buf.push('|');
            buf.push(if c.nullable { 'n' } else { 'r' });
            buf.push('|');
            buf.push_str(&format!("{:?}", c.index));
            buf.push('|');
            buf.push_str(&c.sort_priority.to_string());
            buf.push('\n');
        }
        SchemaFingerprint(xxh3_128(buf.as_bytes()))
    }

    /// Index of column by name, or None.
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// The canonical "spans" schema that corresponds to `SpanRecord`.
    pub fn spans_v1() -> Self {
        Self::new(vec![
            ColumnSpec::new("tenant_id", ColumnType::UInt64).required(),
            ColumnSpec::new("partition_id", ColumnType::UInt32).required(),
            ColumnSpec::new("trace_id", ColumnType::TraceId)
                .required()
                .with_sort_priority(1),
            ColumnSpec::new("span_id", ColumnType::SpanId)
                .required()
                .with_sort_priority(3),
            ColumnSpec::new("parent_span_id", ColumnType::SpanId),
            ColumnSpec::new("start_time_ms", ColumnType::TimestampMillis)
                .required()
                .with_sort_priority(2),
            ColumnSpec::new("end_time_ms", ColumnType::TimestampMillis).required(),
            ColumnSpec::new("duration_ms", ColumnType::Int64).required(),
            ColumnSpec::new("span_type", ColumnType::Utf8).with_index(IndexHint::Bitmap),
            ColumnSpec::new("status", ColumnType::Utf8).with_index(IndexHint::Bitmap),
            ColumnSpec::new("provider", ColumnType::Utf8).with_index(IndexHint::Bitmap),
            ColumnSpec::new("model", ColumnType::Utf8).with_index(IndexHint::Bitmap),
            ColumnSpec::new("tool_name", ColumnType::Utf8).with_index(IndexHint::Bitmap),
            ColumnSpec::new("prompt", ColumnType::Utf8).with_index(IndexHint::Fts),
            ColumnSpec::new("completion", ColumnType::Utf8).with_index(IndexHint::Fts),
            ColumnSpec::new("prompt_tokens", ColumnType::UInt32),
            ColumnSpec::new("completion_tokens", ColumnType::UInt32),
            ColumnSpec::new("cost_usd", ColumnType::Float64),
            ColumnSpec::new("temperature", ColumnType::Float64),
            ColumnSpec::new("top_p", ColumnType::Float64),
            ColumnSpec::new("tool_io_text", ColumnType::Utf8).with_index(IndexHint::Fts),
            ColumnSpec::new("user_id", ColumnType::Utf8),
            ColumnSpec::new("session_id", ColumnType::Utf8),
            ColumnSpec::new("request_id", ColumnType::Utf8),
            ColumnSpec::new("metadata", ColumnType::Json).with_index(IndexHint::JsonPath),
            ColumnSpec::new("embedding", ColumnType::FloatVector(1536)).with_index(IndexHint::Hnsw),
            ColumnSpec::new("commit_id", ColumnType::UInt64).required(),
        ])
    }

    /// Indices of sort-key columns in priority order.
    pub fn sort_key_columns(&self) -> Vec<usize> {
        let mut v: Vec<(u8, usize)> = self
            .columns
            .iter()
            .enumerate()
            .filter_map(|(i, c)| (c.sort_priority > 0).then_some((c.sort_priority, i)))
            .collect();
        v.sort_by_key(|(p, _)| *p);
        v.into_iter().map(|(_, i)| i).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_is_stable() {
        let a = Schema::spans_v1().fingerprint();
        let b = Schema::spans_v1().fingerprint();
        assert_eq!(a, b);
    }

    #[test]
    fn fingerprint_changes_with_schema() {
        let a = Schema::spans_v1().fingerprint();
        let mut s2 = Schema::spans_v1();
        s2.columns.push(ColumnSpec::new("extra", ColumnType::Int32));
        assert_ne!(a, s2.fingerprint());
    }

    #[test]
    fn sort_keys_are_priority_ordered() {
        let s = Schema::spans_v1();
        let keys = s.sort_key_columns();
        let names: Vec<&str> = keys.iter().map(|i| s.columns[*i].name.as_str()).collect();
        assert_eq!(names, vec!["trace_id", "start_time_ms", "span_id"]);
    }

    #[test]
    fn index_of_finds_columns() {
        let s = Schema::spans_v1();
        assert_eq!(s.index_of("model"), Some(11));
        assert_eq!(s.index_of("nope"), None);
    }
}
