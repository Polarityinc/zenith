//! In-memory write buffer.
//!
//! Holds raw `SpanRecord`s grouped by `(tenant, partition)`. On flush, sorts by
//! `(trace_id, start_time, span_id)`, encodes to an Arrow `RecordBatch`, and
//! hands off to the WAL writer.

pub mod builder;
pub mod arrow_schema;
pub mod flush;

pub use builder::MemTable;
pub use arrow_schema::spans_arrow_schema;
pub use flush::flush_to_record_batch;
