//! Catalog row types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use zen_common::{CommitId, PartitionId, SchemaFingerprint, TenantId, TraceId};

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
