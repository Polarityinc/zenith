//! Shard map: which node(s) own which (tenant, partition).
//!
//! Uses Rendezvous (HRW — Highest Random Weight) hashing. Properties:
//!
//! 1. Adding/removing one node only re-routes 1/N of keys (vs full rehash).
//! 2. Deterministic for the same (key, node-set) — every node computes the
//!    same routing without coordination.
//! 3. No need for virtual nodes / consistent-hash ring data structures.
//!
//! For replication factor R, return the top-R nodes ranked by hash. The
//! coordinator usually targets rank 0 (primary); if it's down, falls back
//! to the next.
//!
//! `shards` filter expressions on each NodeInfo restrict candidates further:
//! a node with `shards="tenant=1,2"` is only eligible for tenant 1 or 2.

use crate::node::{NodeId, NodeInfo, NodeStatus};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ShardKey {
    pub tenant_id: u64,
    pub partition_id: u32,
}

impl ShardKey {
    pub fn new(tenant_id: u64, partition_id: u32) -> Self {
        Self {
            tenant_id,
            partition_id,
        }
    }
}

#[derive(Clone)]
pub struct ShardMap {
    /// All known nodes (alive + stale). The router filters by liveness.
    nodes: Vec<NodeInfo>,
    /// Replication factor: how many replicas to return per shard.
    pub replication_factor: usize,
    /// Heartbeat TTL in ms — nodes that haven't beat within this are skipped.
    pub heartbeat_ttl_ms: i64,
}

impl ShardMap {
    pub fn new(nodes: Vec<NodeInfo>, replication_factor: usize, heartbeat_ttl_ms: i64) -> Self {
        Self {
            nodes,
            replication_factor: replication_factor.max(1),
            heartbeat_ttl_ms,
        }
    }

    pub fn nodes(&self) -> &[NodeInfo] {
        &self.nodes
    }

    /// Resolve `(tenant, partition)` to the ranked replica list, only
    /// including alive + role-eligible + shard-filter-matching nodes.
    pub fn replicas_for(&self, key: ShardKey, now_ms: i64) -> Vec<NodeInfo> {
        let mut candidates: Vec<(u64, &NodeInfo)> = self
            .nodes
            .iter()
            .filter(|n| n.status(now_ms, self.heartbeat_ttl_ms) == NodeStatus::Alive)
            .filter(|n| n.role.runs_scans())
            .filter(|n| shard_filter_matches(&n.shards, key))
            .map(|n| (hrw_score(n.node_id, key), n))
            .collect();

        // Highest score first — Rendezvous hashing.
        candidates.sort_by(|a, b| b.0.cmp(&a.0));

        candidates
            .into_iter()
            .take(self.replication_factor)
            .map(|(_, n)| n.clone())
            .collect()
    }

    /// All shards that this local node owns at any rank up to `replication_factor`.
    /// Used by writer-routing: which (tenant, partition) writes land here?
    pub fn shards_owned_by(
        &self,
        local: NodeId,
        candidate_keys: &[ShardKey],
        now_ms: i64,
    ) -> Vec<ShardKey> {
        candidate_keys
            .iter()
            .filter(|k| {
                self.replicas_for(**k, now_ms)
                    .iter()
                    .any(|n| n.node_id == local)
            })
            .copied()
            .collect()
    }

    /// All distinct alive nodes that run scans. Used when a query has no
    /// shard predicate and needs to fan out everywhere.
    pub fn all_alive_workers(&self, now_ms: i64) -> Vec<NodeInfo> {
        self.nodes
            .iter()
            .filter(|n| n.status(now_ms, self.heartbeat_ttl_ms) == NodeStatus::Alive)
            .filter(|n| n.role.runs_scans())
            .cloned()
            .collect()
    }
}

/// Rendezvous-hash score for (node, shard_key). Mixes the node bytes with
/// the shard key bytes via xxh3-64. Higher is better.
fn hrw_score(node: NodeId, key: ShardKey) -> u64 {
    let mut buf = [0u8; 16 + 8 + 4];
    buf[0..16].copy_from_slice(&node.as_bytes());
    buf[16..24].copy_from_slice(&key.tenant_id.to_le_bytes());
    buf[24..28].copy_from_slice(&key.partition_id.to_le_bytes());
    xxhash_rust::xxh3::xxh3_64(&buf)
}

/// Parse a NodeInfo `shards` expression and check whether the given key
/// matches. Supported forms:
///
/// - `*`                      → everything
/// - `tenant=1,2,3`           → exact tenant ids
/// - `tenant=10..20`          → tenant id range, half-open
///
/// Anything unparseable falls back to `false` (fail closed).
fn shard_filter_matches(expr: &str, key: ShardKey) -> bool {
    let expr = expr.trim();
    if expr == "*" || expr.is_empty() {
        return true;
    }
    if let Some(rest) = expr.strip_prefix("tenant=") {
        for part in rest.split(',') {
            let part = part.trim();
            if let Some((lo, hi)) = part.split_once("..") {
                if let (Ok(lo), Ok(hi)) = (lo.parse::<u64>(), hi.parse::<u64>()) {
                    if key.tenant_id >= lo && key.tenant_id < hi {
                        return true;
                    }
                }
            } else if let Ok(t) = part.parse::<u64>() {
                if key.tenant_id == t {
                    return true;
                }
            }
        }
        return false;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{NodeId, NodeInfo, NodeRole};

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
    fn replicas_returns_top_k_alive_workers() {
        let nodes = vec![
            n(1, NodeRole::All, "*", 1000),
            n(2, NodeRole::All, "*", 1000),
            n(3, NodeRole::All, "*", 1000),
            n(4, NodeRole::All, "*", 1000),
            n(5, NodeRole::Compactor, "*", 1000), // not a scan worker
            n(6, NodeRole::All, "*", 0),          // stale
        ];
        let m = ShardMap::new(nodes, 2, 500);
        let r = m.replicas_for(ShardKey::new(1, 0), 1000);
        assert_eq!(r.len(), 2);
        // No compactor or stale node returned.
        for nn in &r {
            assert!(nn.role.runs_scans());
            assert!(nn.last_heartbeat_ms >= 500);
        }
    }

    #[test]
    fn shard_filter_tenant_list() {
        let key_t1 = ShardKey::new(1, 0);
        let key_t9 = ShardKey::new(9, 0);
        assert!(shard_filter_matches("*", key_t1));
        assert!(shard_filter_matches("tenant=1,2,3", key_t1));
        assert!(!shard_filter_matches("tenant=1,2,3", key_t9));
        assert!(shard_filter_matches("tenant=5..15", key_t9));
        assert!(!shard_filter_matches("tenant=5..15", key_t1));
        assert!(!shard_filter_matches("garbage", key_t1));
    }

    #[test]
    fn shard_filter_restricts_replicas() {
        let nodes = vec![
            n(1, NodeRole::All, "tenant=100", 1000),
            n(2, NodeRole::All, "tenant=200", 1000),
            n(3, NodeRole::All, "*", 1000),
        ];
        let m = ShardMap::new(nodes, 3, 500);
        let r100 = m.replicas_for(ShardKey::new(100, 0), 1000);
        // Eligible: node1 (matches tenant=100) and node3 (wildcard). node2 excluded.
        let ids: Vec<u8> = r100.iter().map(|n| n.node_id.as_bytes()[0]).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&3));
        assert!(!ids.contains(&2));
    }

    #[test]
    fn hrw_is_deterministic() {
        let a = hrw_score(NodeId::from_bytes([7u8; 16]), ShardKey::new(1, 0));
        let b = hrw_score(NodeId::from_bytes([7u8; 16]), ShardKey::new(1, 0));
        assert_eq!(a, b);
        // Different shards yield different scores (with overwhelming probability).
        let c = hrw_score(NodeId::from_bytes([7u8; 16]), ShardKey::new(2, 0));
        assert_ne!(a, c);
    }

    #[test]
    fn rendezvous_minimal_remap_when_node_added() {
        // Add one node; expect ~1/N of keys to remap.
        let base: Vec<NodeInfo> = (1u8..=10).map(|i| n(i, NodeRole::All, "*", 1000)).collect();
        let mut extended = base.clone();
        extended.push(n(11, NodeRole::All, "*", 1000));
        let m1 = ShardMap::new(base, 1, 500);
        let m2 = ShardMap::new(extended, 1, 500);

        let mut moved = 0;
        let total = 1000;
        for t in 0..total {
            let k = ShardKey::new(t as u64, 0);
            let r1 = m1.replicas_for(k, 1000);
            let r2 = m2.replicas_for(k, 1000);
            if r1[0].node_id != r2[0].node_id {
                moved += 1;
            }
        }
        // 1/11 ≈ 9 % expected; allow generous slack for randomness.
        let pct = moved as f64 / total as f64;
        assert!(
            pct > 0.03 && pct < 0.20,
            "expected ~9% remap, got {pct:.3}"
        );
    }
}
