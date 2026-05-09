//! Catalog row types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use zen_common::{CommitId, PartitionId, SchemaFingerprint, SpanRecord, TenantId, TraceId};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct WalObjectBounds {
    pub time_min: i64,
    pub time_max: i64,
    pub trace_id_min: TraceId,
    pub trace_id_max: TraceId,
}

impl WalObjectBounds {
    pub fn from_span_records(records: &[SpanRecord]) -> Self {
        let mut time_min = i64::MAX;
        let mut time_max = i64::MIN;
        let mut trace_id_min = TraceId([0xff; 16]);
        let mut trace_id_max = TraceId([0; 16]);

        for r in records {
            time_min = time_min.min(r.start_time_ms);
            time_max = time_max.max(r.start_time_ms);
            if r.trace_id.0 < trace_id_min.0 {
                trace_id_min = r.trace_id;
            }
            if r.trace_id.0 > trace_id_max.0 {
                trace_id_max = r.trace_id;
            }
        }

        if records.is_empty() {
            return Self {
                time_min: i64::MIN,
                time_max: i64::MAX,
                trace_id_min: TraceId([0; 16]),
                trace_id_max: TraceId([0xff; 16]),
            };
        }

        Self {
            time_min,
            time_max,
            trace_id_min,
            trace_id_max,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalObjectRow {
    pub wal_id: uuid::Uuid,
    pub tenant_id: TenantId,
    pub partition_id: PartitionId,
    pub object_key: String,
    pub commit_id_min: CommitId,
    pub commit_id_max: CommitId,
    pub byte_count: i64,
    pub row_count: i64,
    pub time_min: i64,
    pub time_max: i64,
    pub trace_id_min: TraceId,
    pub trace_id_max: TraceId,
    pub schema_fingerprint: SchemaFingerprint,
    pub consumed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SegmentRow {
    pub segment_id: uuid::Uuid,
    pub tenant_id: TenantId,
    pub partition_id: PartitionId,
    pub object_key: String,
    pub level: i16,
    pub byte_count: i64,
    pub row_count: i64,
    pub time_min: i64,
    pub time_max: i64,
    pub trace_id_min: TraceId,
    pub trace_id_max: TraceId,
    pub commit_id_min: CommitId,
    pub commit_id_max: CommitId,
    pub schema_fingerprint: SchemaFingerprint,
    /// Serialized `SparseRowGroupIndex` (zen_index::sparse).
    pub rowgroup_index: Vec<u8>,
    pub superseded_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Cluster node row. Used by `zen_cluster::NodeRegistry`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeRow {
    pub node_id: uuid::Uuid,
    pub endpoint: String,
    pub role: String,
    pub shards: String,
    pub last_heartbeat_ms: i64,
}
