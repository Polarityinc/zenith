//! Decide whether a query runs locally or fans out.
//!
//! Most agent-trace queries are tenant-scoped: the parsed plan carries
//! `tenant_id` from the auth header. For these, the router picks the
//! primary replica for the tenant's partition and either:
//!
//! - returns `Local` if that primary is *us*, OR
//! - returns `Remote([primary, … fallback replicas])` to be tried in order.
//!
//! When the workload spans tenants (admin queries, cross-tenant
//! aggregates), the router fans out to *all* alive workers and merges via
//! `merge::merge_result_sets`.

use crate::node::{NodeId, NodeInfo};
use crate::shard::{ShardKey, ShardMap};

#[derive(Clone, Debug)]
pub enum RouteDecision {
    /// Run the plan in this process.
    Local,
    /// Forward to a remote node — try each in order on connection failure.
    Remote(Vec<NodeInfo>),
    /// Run on every alive worker and merge. Includes self if local is a worker.
    FanOut {
        targets: Vec<NodeInfo>,
        include_local: bool,
    },
}

pub struct QueryRouter {
    pub local_id: NodeId,
    pub map: ShardMap,
}

impl QueryRouter {
    pub fn new(local_id: NodeId, map: ShardMap) -> Self {
        Self { local_id, map }
    }

    /// Single-tenant route. Common path; cheap.
    pub fn route_tenant(&self, key: ShardKey, now_ms: i64) -> RouteDecision {
        let replicas = self.map.replicas_for(key, now_ms);
        if replicas.is_empty() {
            // No alive worker eligible; fall back to local (best-effort).
            return RouteDecision::Local;
        }
        if replicas[0].node_id == self.local_id {
            return RouteDecision::Local;
        }
        // We're not the primary. Forward to the ranked replicas in order;
        // if all fail, the caller can fall back to running locally since
        // every node can read object_store directly.
        RouteDecision::Remote(replicas)
    }

    /// Cross-tenant / cross-shard fan-out.
    pub fn route_all(&self, now_ms: i64) -> RouteDecision {
        let mut all = self.map.all_alive_workers(now_ms);
        let include_local = all.iter().any(|n| n.node_id == self.local_id);
        if include_local {
            all.retain(|n| n.node_id != self.local_id);
        }
        RouteDecision::FanOut {
            targets: all,
            include_local,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{NodeId, NodeRole};

    fn n(id: u8, role: NodeRole, shards: &str, hb: i64) -> NodeInfo {
        let mut bytes = [0u8; 16];
        bytes[0] = id;
        NodeInfo {
            node_id: NodeId::from_bytes(bytes),
            endpoint: format!("http://node{id}:8080"),
            role,
            shards: shards.into(),
            last_heartbeat_ms: hb,
        }
    }

    #[test]
    fn route_tenant_local_when_primary() {
        // Build a map where node 1 is the only candidate; route from node 1.
        let local = NodeId::from_bytes([1u8; 16]);
        let nodes = vec![NodeInfo {
            node_id: local,
            endpoint: "http://1:8080".into(),
            role: NodeRole::All,
            shards: "*".into(),
            last_heartbeat_ms: 1000,
        }];
        let map = ShardMap::new(nodes, 1, 500);
        let router = QueryRouter::new(local, map);
        let key = ShardKey::new(1, 0);
        match router.route_tenant(key, 1000) {
            RouteDecision::Local => {}
            other => panic!("expected Local, got {other:?}"),
        }
    }

    #[test]
    fn route_tenant_remote_when_not_primary() {
        // Multiple alive nodes; we may or may not be the primary depending
        // on the HRW outcome. We drive it by setting our id such that we
        // are *not* the primary for the chosen key.
        let n1 = n(1, NodeRole::All, "*", 1000);
        let n2 = n(2, NodeRole::All, "*", 1000);
        let n3 = n(3, NodeRole::All, "*", 1000);
        let nodes = vec![n1.clone(), n2.clone(), n3.clone()];
        let map = ShardMap::new(nodes, 2, 500);
        // Try a few keys until we find one where local (n3) isn't primary.
        let local = n3.node_id;
        let router = QueryRouter::new(local, map);
        let mut found_remote = false;
        for t in 0..50 {
            let key = ShardKey::new(t as u64, 0);
            if let RouteDecision::Remote(_) = router.route_tenant(key, 1000) {
                found_remote = true;
                break;
            }
        }
        assert!(found_remote, "expected at least one Remote routing");
    }

    #[test]
    fn route_all_separates_local_from_remotes() {
        let n1 = n(1, NodeRole::All, "*", 1000);
        let n2 = n(2, NodeRole::All, "*", 1000);
        let n3 = n(3, NodeRole::All, "*", 1000);
        let nodes = vec![n1.clone(), n2.clone(), n3.clone()];
        let map = ShardMap::new(nodes, 1, 500);
        let local = n2.node_id;
        let router = QueryRouter::new(local, map);
        match router.route_all(1000) {
            RouteDecision::FanOut {
                targets,
                include_local,
            } => {
                assert!(include_local);
                assert_eq!(targets.len(), 2);
                assert!(!targets.iter().any(|n| n.node_id == local));
            }
            other => panic!("expected FanOut, got {other:?}"),
        }
    }
}
