//! Node identity and roles.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub Uuid);

impl NodeId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_bytes(b: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(b))
    }

    pub fn as_bytes(&self) -> [u8; 16] {
        *self.0.as_bytes()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Accepts client traffic; routes/fan-outs queries; can also run scans.
    Coordinator,
    /// Executes scans only. Receives sub-plans from coordinators.
    Worker,
    /// Runs compaction in the background. Doesn't serve client traffic.
    Compactor,
    /// Does everything. Used for single-node and small-cluster deployments.
    All,
}

impl NodeRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeRole::Coordinator => "coordinator",
            NodeRole::Worker => "worker",
            NodeRole::Compactor => "compactor",
            NodeRole::All => "all",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "coordinator" => Some(NodeRole::Coordinator),
            "worker" => Some(NodeRole::Worker),
            "compactor" => Some(NodeRole::Compactor),
            "all" => Some(NodeRole::All),
            _ => None,
        }
    }

    /// True if this role serves /v1/internal/query (i.e. runs scans).
    pub fn runs_scans(&self) -> bool {
        matches!(self, NodeRole::Worker | NodeRole::All)
    }

    /// True if this role accepts client traffic.
    pub fn accepts_client_queries(&self) -> bool {
        matches!(self, NodeRole::Coordinator | NodeRole::All)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: NodeId,
    pub endpoint: String,
    pub role: NodeRole,
    /// Shard expression. `*` means any shard. Otherwise comma-separated
    /// `tenant=N,M` or `tenant=N..M` ranges. Parsed lazily by `ShardMap`.
    pub shards: String,
    pub last_heartbeat_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeStatus {
    Alive,
    Stale,
}

impl NodeInfo {
    /// A node is alive if its last heartbeat is within `ttl_ms`.
    pub fn status(&self, now_ms: i64, ttl_ms: i64) -> NodeStatus {
        if now_ms - self.last_heartbeat_ms <= ttl_ms {
            NodeStatus::Alive
        } else {
            NodeStatus::Stale
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_parse_roundtrip() {
        for r in [
            NodeRole::Coordinator,
            NodeRole::Worker,
            NodeRole::Compactor,
            NodeRole::All,
        ] {
            assert_eq!(NodeRole::parse(r.as_str()), Some(r));
        }
        assert!(NodeRole::parse("frobnicator").is_none());
    }

    #[test]
    fn node_status_within_ttl() {
        let n = NodeInfo {
            node_id: NodeId::new(),
            endpoint: "http://a:8080".into(),
            role: NodeRole::All,
            shards: "*".into(),
            last_heartbeat_ms: 1000,
        };
        assert_eq!(n.status(1500, 1000), NodeStatus::Alive);
        assert_eq!(n.status(2500, 1000), NodeStatus::Stale);
    }

    #[test]
    fn role_capabilities() {
        assert!(NodeRole::Worker.runs_scans());
        assert!(NodeRole::All.runs_scans());
        assert!(!NodeRole::Coordinator.runs_scans());
        assert!(!NodeRole::Compactor.runs_scans());

        assert!(NodeRole::Coordinator.accepts_client_queries());
        assert!(NodeRole::All.accepts_client_queries());
        assert!(!NodeRole::Worker.accepts_client_queries());
    }
}
