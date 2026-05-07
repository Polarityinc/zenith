//! Foundational identifier and record types.
//!
//! These types are deliberately small and `Copy`-friendly. They are the building blocks
//! for every other crate; keep their layout stable across schema versions.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Tenant identifier (an opaque 64-bit handle assigned by the catalog).
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct TenantId(pub u64);

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "t{:016x}", self.0)
    }
}

/// Partition identifier within a tenant.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct PartitionId(pub u32);

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "p{:08x}", self.0)
    }
}

/// Monotonic commit identifier per `(tenant, partition)`.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct CommitId(pub u64);

impl fmt::Display for CommitId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "c{:020}", self.0)
    }
}

impl CommitId {
    pub const ZERO: CommitId = CommitId(0);
    #[inline]
    pub const fn new(v: u64) -> Self {
        Self(v)
    }
    #[inline]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// 128-bit trace identifier (ULID layout works fine here).
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Serialize, Deserialize,
)]
pub struct TraceId(pub [u8; 16]);

impl TraceId {
    #[inline]
    pub fn from_bytes(b: [u8; 16]) -> Self {
        Self(b)
    }
    #[inline]
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
    #[inline]
    pub fn to_u128(&self) -> u128 {
        u128::from_be_bytes(self.0)
    }
    #[inline]
    pub fn from_u128(x: u128) -> Self {
        Self(x.to_be_bytes())
    }
    pub fn new_random() -> Self {
        Self::from_u128(Ulid::new().0)
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", Ulid(self.to_u128()))
    }
}

impl FromStr for TraceId {
    type Err = ulid::DecodeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_u128(Ulid::from_string(s)?.0))
    }
}

/// 128-bit span identifier within a trace.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Serialize, Deserialize,
)]
pub struct SpanId(pub [u8; 16]);

impl SpanId {
    #[inline]
    pub fn from_bytes(b: [u8; 16]) -> Self {
        Self(b)
    }
    #[inline]
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
    #[inline]
    pub fn to_u128(&self) -> u128 {
        u128::from_be_bytes(self.0)
    }
    #[inline]
    pub fn from_u128(x: u128) -> Self {
        Self(x.to_be_bytes())
    }
    pub fn new_random() -> Self {
        Self::from_u128(Ulid::new().0)
    }
}

impl fmt::Display for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", Ulid(self.to_u128()))
    }
}

impl FromStr for SpanId {
    type Err = ulid::DecodeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_u128(Ulid::from_string(s)?.0))
    }
}

/// 128-bit schema fingerprint. Fingerprint is computed from the canonical column list +
/// types; two schemas with the same fingerprint are bit-compatible at the segment-format level.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct SchemaFingerprint(pub u128);

impl fmt::Display for SchemaFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sf:{:032x}", self.0)
    }
}

/// Span coarse classification used by indexing and aggregation.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanType {
    AgentStep,
    LlmCall,
    ToolCall,
    Retrieval,
    EvalScore,
    Other,
}

impl SpanType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AgentStep => "agent_step",
            Self::LlmCall => "llm_call",
            Self::ToolCall => "tool_call",
            Self::Retrieval => "retrieval",
            Self::EvalScore => "eval_score",
            Self::Other => "other",
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanStatus {
    Ok,
    Error,
    Timeout,
    Cancelled,
}

impl SpanStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
        }
    }
}

/// A single span. This is the shape that flows from clients into the memtable; it is
/// converted into Arrow columns when batches are flushed.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SpanRecord {
    // Identity
    pub tenant_id: TenantId,
    pub partition_id: PartitionId,
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub parent_span_id: Option<SpanId>,

    // Time
    pub start_time_ms: i64,
    pub end_time_ms: i64,
    pub duration_ms: i64,

    // Coarse classification
    pub span_type: Option<String>,
    pub status: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub tool_name: Option<String>,

    // LLM-specific
    pub prompt: Option<String>,
    pub completion: Option<String>,
    pub prompt_tokens: Option<u32>,
    pub completion_tokens: Option<u32>,
    pub cost_usd: Option<f64>,
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,

    // Tool / agent
    pub tool_io_text: Option<String>,

    // Identity tracking
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub request_id: Option<String>,

    // Free-form
    pub metadata: Option<serde_json::Value>,

    // Optional embedding
    pub embedding: Option<Vec<f32>>,

    // MVCC: monotonically increasing per (tenant, partition); set by writer.
    pub commit_id: CommitId,
}

impl SpanRecord {
    pub fn new(tenant_id: TenantId, partition_id: PartitionId) -> Self {
        Self {
            tenant_id,
            partition_id,
            trace_id: TraceId::new_random(),
            span_id: SpanId::new_random(),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_id_roundtrip() {
        let id = TraceId::new_random();
        let s = id.to_string();
        let parsed: TraceId = s.parse().unwrap();
        assert_eq!(id, parsed);
        let bytes = *id.as_bytes();
        assert_eq!(TraceId::from_bytes(bytes), id);
        assert_eq!(TraceId::from_u128(id.to_u128()), id);
    }

    #[test]
    fn span_id_roundtrip() {
        let id = SpanId::new_random();
        let s = id.to_string();
        let parsed: SpanId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn commit_id_next_is_monotonic() {
        let a = CommitId::new(7);
        let b = a.next();
        assert!(b.0 > a.0);
        assert_eq!(b.0, 8);
    }

    #[test]
    fn span_type_status_strings_stable() {
        assert_eq!(SpanType::LlmCall.as_str(), "llm_call");
        assert_eq!(SpanStatus::Ok.as_str(), "ok");
    }

    #[test]
    fn span_record_default_construction() {
        let s = SpanRecord::new(TenantId(1), PartitionId(0));
        assert_eq!(s.tenant_id, TenantId(1));
        assert_eq!(s.partition_id, PartitionId(0));
        assert_ne!(s.trace_id, TraceId::default());
    }

    #[test]
    fn ids_serialize_compactly() {
        let id = TraceId::new_random();
        let json = serde_json::to_string(&id).unwrap();
        assert!(json.starts_with('['));
    }
}
