// zen_common — types, errors, config

pub mod types;
pub mod errors;
pub mod config;
pub mod time;
pub mod schema;

pub use errors::{ZenError, ZenResult};
pub use types::{TenantId, PartitionId, CommitId, TraceId, SpanId, SchemaFingerprint, SpanRecord, SpanType, SpanStatus};
pub use config::Config;
pub use schema::{Schema, ColumnSpec, ColumnType, IndexHint};
pub use time::{now_millis, MilliInstant};
