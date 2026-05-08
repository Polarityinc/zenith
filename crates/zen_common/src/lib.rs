// zen_common — types, errors, config

pub mod config;
pub mod errors;
pub mod schema;
pub mod time;
pub mod types;

pub use config::Config;
pub use errors::{ZenError, ZenResult};
pub use schema::{ColumnSpec, ColumnType, IndexHint, Schema};
pub use time::{now_millis, MilliInstant};
pub use types::{
    CommitId, PartitionId, SchemaFingerprint, SpanId, SpanRecord, SpanStatus, SpanType, TenantId,
    TraceId,
};
