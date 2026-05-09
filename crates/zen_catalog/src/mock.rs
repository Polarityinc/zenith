//! In-memory `Catalog` implementation for tests and benches.
//!
//! No SQL, no external dependencies, no on-disk state — every operation
//! is a `parking_lot::RwLock` guard around a `HashMap`. Honors all the
//! semantic contracts of the trait: monotonic commit-id allocation,
//! tenant-scoped segment listing, lease TTLs, etc.
//!
//! Production deployments use [`PostgresCatalog`](crate::postgres) — the
//! mock exists only so tests can run without spinning up a Postgres
//! container.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;

use zen_common::{CommitId, PartitionId, TenantId, ZenError, ZenResult};

use crate::model::{NodeRow, SegmentRow, WalObjectRow};
use crate::Catalog;

#[derive(Default)]
pub struct MockCatalog {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    tenants: RwLock<HashMap<TenantId, String>>,
    partitions: RwLock<HashMap<(TenantId, PartitionId), ()>>,
    /// Per-(tenant, partition) commit-id counter. AtomicU64 so concurrent
    /// `next_commit_id` calls don't serialize.
    commit_seq: RwLock<HashMap<(TenantId, PartitionId), Arc<AtomicU64>>>,
    wals: RwLock<Vec<WalObjectRow>>,
    segments: RwLock<Vec<SegmentRow>>,
    leases: RwLock<HashMap<(TenantId, PartitionId), Lease>>,
    nodes: RwLock<HashMap<uuid::Uuid, NodeRow>>,
}

#[derive(Clone)]
struct Lease {
    worker_id: String,
    expires_at: DateTime<Utc>,
}

impl MockCatalog {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Catalog for MockCatalog {
    async fn ensure_tenant(&self, tenant: TenantId, name: &str) -> ZenResult<()> {
        self.inner
            .tenants
            .write()
            .entry(tenant)
            .or_insert_with(|| name.to_string());
        Ok(())
    }

    async fn ensure_partition(&self, tenant: TenantId, partition: PartitionId) -> ZenResult<()> {
        self.inner
            .partitions
            .write()
            .entry((tenant, partition))
            .or_insert(());
        // Initialize commit counter on first partition creation.
        self.inner
            .commit_seq
            .write()
            .entry((tenant, partition))
            .or_insert_with(|| Arc::new(AtomicU64::new(1)));
        Ok(())
    }

    async fn next_commit_id(
        &self,
        tenant: TenantId,
        partition: PartitionId,
    ) -> ZenResult<CommitId> {
        // Fast path: counter exists.
        {
            if let Some(c) = self.inner.commit_seq.read().get(&(tenant, partition)) {
                return Ok(CommitId(c.fetch_add(1, Ordering::SeqCst)));
            }
        }
        // Slow path: create.
        let mut g = self.inner.commit_seq.write();
        let c = g
            .entry((tenant, partition))
            .or_insert_with(|| Arc::new(AtomicU64::new(1)));
        Ok(CommitId(c.fetch_add(1, Ordering::SeqCst)))
    }

    async fn register_wal_object(&self, w: WalObjectRow) -> ZenResult<()> {
        self.inner.wals.write().push(w);
        Ok(())
    }

    async fn list_wal_objects(
        &self,
        tenant: TenantId,
        partition: PartitionId,
        since_commit_id: CommitId,
    ) -> ZenResult<Vec<WalObjectRow>> {
        let g = self.inner.wals.read();
        let mut out: Vec<_> = g
            .iter()
            .filter(|w| {
                w.tenant_id == tenant
                    && w.partition_id == partition
                    && w.consumed_at.is_none()
                    && w.commit_id_min >= since_commit_id
            })
            .cloned()
            .collect();
        out.sort_by_key(|w| w.commit_id_min);
        Ok(out)
    }

    async fn register_segment(&self, s: SegmentRow) -> ZenResult<()> {
        self.inner.segments.write().push(s);
        Ok(())
    }

    async fn list_segments_in_range(
        &self,
        tenant: TenantId,
        partition: PartitionId,
        time_min: i64,
        time_max: i64,
    ) -> ZenResult<Vec<SegmentRow>> {
        let g = self.inner.segments.read();
        let out: Vec<_> = g
            .iter()
            .filter(|s| {
                s.tenant_id == tenant
                    && s.partition_id == partition
                    && s.superseded_at.is_none()
                    // Standard half-open overlap: [time_min..time_max]
                    // intersects [s.time_min..s.time_max].
                    && s.time_min <= time_max
                    && s.time_max >= time_min
            })
            .cloned()
            .collect();
        Ok(out)
    }

    async fn mark_wal_consumed(
        &self,
        tenant: TenantId,
        partition: PartitionId,
        consumed_through: CommitId,
        at: DateTime<Utc>,
    ) -> ZenResult<u64> {
        let mut g = self.inner.wals.write();
        let mut n = 0u64;
        for w in g.iter_mut() {
            if w.tenant_id == tenant
                && w.partition_id == partition
                && w.consumed_at.is_none()
                && w.commit_id_max <= consumed_through
            {
                w.consumed_at = Some(at);
                n += 1;
            }
        }
        Ok(n)
    }

    async fn acquire_compaction_lease(
        &self,
        tenant: TenantId,
        partition: PartitionId,
        worker_id: &str,
        ttl_seconds: u64,
    ) -> ZenResult<()> {
        let now = Utc::now();
        let mut g = self.inner.leases.write();
        let take = match g.get(&(tenant, partition)) {
            None => true,
            Some(l) => l.expires_at <= now || l.worker_id == worker_id,
        };
        if !take {
            return Err(ZenError::catalog(format!(
                "compaction lease for ({tenant:?}, {partition:?}) is held by {}",
                g.get(&(tenant, partition))
                    .map(|l| l.worker_id.as_str())
                    .unwrap_or("?")
            )));
        }
        g.insert(
            (tenant, partition),
            Lease {
                worker_id: worker_id.to_string(),
                expires_at: now + chrono::Duration::seconds(ttl_seconds as i64),
            },
        );
        Ok(())
    }

    async fn release_compaction_lease(
        &self,
        tenant: TenantId,
        partition: PartitionId,
        worker_id: &str,
    ) -> ZenResult<()> {
        let mut g = self.inner.leases.write();
        if let Some(l) = g.get(&(tenant, partition)) {
            if l.worker_id == worker_id {
                g.remove(&(tenant, partition));
            }
        }
        Ok(())
    }

    async fn list_segments_for_tenant(&self, tenant: TenantId) -> ZenResult<Vec<SegmentRow>> {
        let g = self.inner.segments.read();
        Ok(g.iter()
            .filter(|s| s.tenant_id == tenant && s.superseded_at.is_none())
            .cloned()
            .collect())
    }

    async fn mark_segments_superseded(
        &self,
        tenant: TenantId,
        segment_ids: &[uuid::Uuid],
        at: DateTime<Utc>,
    ) -> ZenResult<u64> {
        let mut g = self.inner.segments.write();
        let mut n = 0u64;
        for s in g.iter_mut() {
            // Tenant-scoped supersede — same security guarantee as the
            // production Postgres impl.
            if s.tenant_id == tenant
                && s.superseded_at.is_none()
                && segment_ids.contains(&s.segment_id)
            {
                s.superseded_at = Some(at);
                n += 1;
            }
        }
        Ok(n)
    }

    async fn upsert_node(&self, row: NodeRow) -> ZenResult<()> {
        self.inner.nodes.write().insert(row.node_id, row);
        Ok(())
    }

    async fn list_nodes(&self) -> ZenResult<Vec<NodeRow>> {
        Ok(self.inner.nodes.read().values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zen_common::SchemaFingerprint;

    #[tokio::test]
    async fn ensure_tenant_idempotent() {
        let c = MockCatalog::new();
        c.ensure_tenant(TenantId(1), "a").await.unwrap();
        c.ensure_tenant(TenantId(1), "a").await.unwrap();
    }

    #[tokio::test]
    async fn next_commit_id_monotonic_concurrent() {
        let c = Arc::new(MockCatalog::new());
        c.ensure_partition(TenantId(1), PartitionId(0))
            .await
            .unwrap();
        let mut handles = Vec::new();
        for _ in 0..16 {
            let c2 = c.clone();
            handles.push(tokio::spawn(async move {
                let mut out = Vec::new();
                for _ in 0..32 {
                    out.push(
                        c2.next_commit_id(TenantId(1), PartitionId(0))
                            .await
                            .unwrap()
                            .0,
                    );
                }
                out
            }));
        }
        let mut all: Vec<u64> = Vec::new();
        for h in handles {
            all.extend(h.await.unwrap());
        }
        all.sort();
        for w in all.windows(2) {
            assert!(w[0] != w[1], "duplicate commit_id {}", w[0]);
        }
    }

    #[tokio::test]
    async fn segment_register_list_in_range() {
        let c = MockCatalog::new();
        c.ensure_tenant(TenantId(1), "x").await.unwrap();
        c.ensure_partition(TenantId(1), PartitionId(0))
            .await
            .unwrap();
        c.register_segment(SegmentRow {
            segment_id: uuid::Uuid::new_v4(),
            tenant_id: TenantId(1),
            partition_id: PartitionId(0),
            object_key: "k".into(),
            level: 0,
            byte_count: 1,
            row_count: 1,
            time_min: 1000,
            time_max: 2000,
            trace_id_min: zen_common::TraceId([0u8; 16]),
            trace_id_max: zen_common::TraceId([0xff; 16]),
            commit_id_min: CommitId(1),
            commit_id_max: CommitId(1),
            schema_fingerprint: SchemaFingerprint(0),
            rowgroup_index: vec![],
            superseded_at: None,
            created_at: Utc::now(),
        })
        .await
        .unwrap();
        let in_range = c
            .list_segments_in_range(TenantId(1), PartitionId(0), 500, 1500)
            .await
            .unwrap();
        assert_eq!(in_range.len(), 1);
        let out_of_range = c
            .list_segments_in_range(TenantId(1), PartitionId(0), 3000, 4000)
            .await
            .unwrap();
        assert_eq!(out_of_range.len(), 0);
    }

    #[tokio::test]
    async fn lease_lifecycle() {
        let c = MockCatalog::new();
        c.acquire_compaction_lease(TenantId(1), PartitionId(0), "w1", 60)
            .await
            .unwrap();
        // Different worker can't take it while held.
        assert!(c
            .acquire_compaction_lease(TenantId(1), PartitionId(0), "w2", 60)
            .await
            .is_err());
        // Same worker can refresh.
        c.acquire_compaction_lease(TenantId(1), PartitionId(0), "w1", 60)
            .await
            .unwrap();
        // Release lets w2 acquire.
        c.release_compaction_lease(TenantId(1), PartitionId(0), "w1")
            .await
            .unwrap();
        c.acquire_compaction_lease(TenantId(1), PartitionId(0), "w2", 60)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn supersede_is_tenant_scoped() {
        let c = MockCatalog::new();
        let id = uuid::Uuid::new_v4();
        c.register_segment(SegmentRow {
            segment_id: id,
            tenant_id: TenantId(1),
            partition_id: PartitionId(0),
            object_key: "k".into(),
            level: 0,
            byte_count: 1,
            row_count: 1,
            time_min: 1,
            time_max: 2,
            trace_id_min: zen_common::TraceId([0u8; 16]),
            trace_id_max: zen_common::TraceId([0xff; 16]),
            commit_id_min: CommitId(1),
            commit_id_max: CommitId(1),
            schema_fingerprint: SchemaFingerprint(0),
            rowgroup_index: vec![],
            superseded_at: None,
            created_at: Utc::now(),
        })
        .await
        .unwrap();
        // Tenant 2 tries to supersede tenant 1's segment by guessed UUID.
        let n = c
            .mark_segments_superseded(TenantId(2), &[id], Utc::now())
            .await
            .unwrap();
        assert_eq!(n, 0, "cross-tenant supersede must not succeed");
        // Tenant 1 succeeds.
        let n = c
            .mark_segments_superseded(TenantId(1), &[id], Utc::now())
            .await
            .unwrap();
        assert_eq!(n, 1);
    }
}
