//! Segment-level metadata: identity, schema fingerprint, time/commit ranges.
//!
//! Serialized as bincode at the head of the segment so that any reader can
//! decide whether the segment is relevant to a query just from its first
//! few KB.

use serde::{Deserialize, Serialize};

use zen_common::{CommitId, PartitionId, SchemaFingerprint, SpanId, TenantId, TraceId};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SegmentMetadata {
    pub segment_id: u128,
    pub tenant_id: TenantId,
    pub partition_id: PartitionId,
    pub schema_fingerprint: SchemaFingerprint,

    pub time_min_ms: i64,
    pub time_max_ms: i64,

    pub commit_id_min: CommitId,
    pub commit_id_max: CommitId,

    pub row_count: u64,

    pub trace_id_min: TraceId,
    pub trace_id_max: TraceId,
    pub span_id_min: SpanId,
    pub span_id_max: SpanId,

    /// Names of columns that drive the sort, in priority order.
    pub sort_keys: Vec<String>,
    /// Names of columns in the schema, in stored order.
    pub column_names: Vec<String>,
}

impl SegmentMetadata {
    pub fn new(
        segment_id: u128,
        tenant_id: TenantId,
        partition_id: PartitionId,
        schema_fingerprint: SchemaFingerprint,
        column_names: Vec<String>,
        sort_keys: Vec<String>,
    ) -> Self {
        Self {
            segment_id,
            tenant_id,
            partition_id,
            schema_fingerprint,
            time_min_ms: i64::MAX,
            time_max_ms: i64::MIN,
            commit_id_min: CommitId(u64::MAX),
            commit_id_max: CommitId(0),
            row_count: 0,
            trace_id_min: TraceId([0xFF; 16]),
            trace_id_max: TraceId([0; 16]),
            span_id_min: SpanId([0xFF; 16]),
            span_id_max: SpanId([0; 16]),
            sort_keys,
            column_names,
        }
    }

    pub fn observe_time(&mut self, t: i64) {
        if t < self.time_min_ms {
            self.time_min_ms = t;
        }
        if t > self.time_max_ms {
            self.time_max_ms = t;
        }
    }

    pub fn observe_commit(&mut self, c: CommitId) {
        if c.0 < self.commit_id_min.0 {
            self.commit_id_min = c;
        }
        if c.0 > self.commit_id_max.0 {
            self.commit_id_max = c;
        }
    }

    pub fn observe_trace_id(&mut self, t: TraceId) {
        if t.0 < self.trace_id_min.0 {
            self.trace_id_min = t;
        }
        if t.0 > self.trace_id_max.0 {
            self.trace_id_max = t;
        }
    }

    pub fn observe_span_id(&mut self, s: SpanId) {
        if s.0 < self.span_id_min.0 {
            self.span_id_min = s;
        }
        if s.0 > self.span_id_max.0 {
            self.span_id_max = s;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observe_min_max() {
        let fp = SchemaFingerprint(0);
        let mut m = SegmentMetadata::new(
            0, TenantId(0), PartitionId(0), fp,
            vec!["a".to_string()], vec!["a".to_string()],
        );
        m.observe_time(1000);
        m.observe_time(50);
        m.observe_time(2000);
        assert_eq!(m.time_min_ms, 50);
        assert_eq!(m.time_max_ms, 2000);

        m.observe_commit(CommitId(7));
        m.observe_commit(CommitId(3));
        m.observe_commit(CommitId(11));
        assert_eq!(m.commit_id_min, CommitId(3));
        assert_eq!(m.commit_id_max, CommitId(11));
    }

    #[test]
    fn bincode_roundtrip() {
        let m = SegmentMetadata::new(
            42, TenantId(1), PartitionId(2), SchemaFingerprint(99),
            vec!["x".into(), "y".into()],
            vec!["x".into()],
        );
        let bytes = bincode::serialize(&m).unwrap();
        let m2: SegmentMetadata = bincode::deserialize(&bytes).unwrap();
        assert_eq!(m, m2);
    }
}
