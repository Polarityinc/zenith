//! Catalog: tiny metadata store. Tracks segments, WAL objects, commit IDs,
//! compaction leases. Lives outside the segment data — segments hold the
//! truth, the catalog is a fast index.
//!
//! Default backend is sqlite (zero-install). Postgres available behind a
//! feature flag for prod-like deployments.

pub mod sqlite;
pub mod model;
#[cfg(feature = "catalog-postgres")]
pub mod postgres;

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use zen_common::{CommitId, Config, PartitionId, TenantId, ZenError, ZenResult};

pub use model::*;
pub use sqlite::SqliteCatalog;

/// All operations the engine needs from a catalog.
#[async_trait]
pub trait Catalog: Send + Sync + 'static {
    /// Initialize tenant rows + reserve commit-seq starting state.
    async fn ensure_tenant(&self, tenant: TenantId, name: &str) -> ZenResult<()>;
    async fn ensure_partition(&self, tenant: TenantId, partition: PartitionId) -> ZenResult<()>;

    /// Allocate the next commit_id for `(tenant, partition)`. Strongly monotonic.
    async fn next_commit_id(&self, tenant: TenantId, partition: PartitionId) -> ZenResult<CommitId>;

    /// Register a freshly-flushed WAL object.
    async fn register_wal_object(&self, w: WalObjectRow) -> ZenResult<()>;

    /// List unconsumed WAL objects for `(tenant, partition)` since `since_commit_id`.
    async fn list_wal_objects(
        &self,
        tenant: TenantId,
        partition: PartitionId,
        since_commit_id: CommitId,
    ) -> ZenResult<Vec<WalObjectRow>>;

    /// Register a freshly-published segment.
    async fn register_segment(&self, s: SegmentRow) -> ZenResult<()>;

    /// List active segments overlapping `[time_min, time_max]` for `(tenant, partition)`.
    async fn list_segments_in_range(
        &self,
        tenant: TenantId,
        partition: PartitionId,
        time_min: i64,
        time_max: i64,
    ) -> ZenResult<Vec<SegmentRow>>;

    /// Mark WAL objects up to `consumed_through` as consumed by a compaction.
    async fn mark_wal_consumed(
        &self,
        tenant: TenantId,
        partition: PartitionId,
        consumed_through: CommitId,
        at: DateTime<Utc>,
    ) -> ZenResult<u64>;

    /// Acquire a compaction lease. Returns Ok(()) if acquired, error if held.
    async fn acquire_compaction_lease(
        &self,
        tenant: TenantId,
        partition: PartitionId,
        worker_id: &str,
        ttl_seconds: u64,
    ) -> ZenResult<()>;

    /// Release a compaction lease.
    async fn release_compaction_lease(
        &self,
        tenant: TenantId,
        partition: PartitionId,
        worker_id: &str,
    ) -> ZenResult<()>;

    /// Used by query planners that hold per-tenant info: list segments by tenant.
    async fn list_segments_for_tenant(&self, tenant: TenantId) -> ZenResult<Vec<SegmentRow>>;

    /// Mark a set of segments as superseded by a tier-2 / tier-N compaction.
    /// Superseded segments are no longer returned by `list_segments_*`.
    async fn mark_segments_superseded(
        &self,
        segment_ids: &[uuid::Uuid],
        at: DateTime<Utc>,
    ) -> ZenResult<u64>;

    /// Upsert this node's heartbeat row. Called by every node on a 500 ms
    /// tick (`zen_cluster::NodeRegistry`). Used to drive the cluster's
    /// shard map.
    async fn upsert_node(&self, row: NodeRow) -> ZenResult<()>;

    /// List all known node rows, including stale ones — the cluster layer
    /// filters by heartbeat TTL when computing the shard map.
    async fn list_nodes(&self) -> ZenResult<Vec<NodeRow>>;
}

pub async fn open_catalog(cfg: &Config) -> ZenResult<Arc<dyn Catalog>> {
    match cfg.catalog.backend.as_str() {
        "sqlite" => {
            let cat = SqliteCatalog::open(&cfg.catalog.sqlite_path).await?;
            Ok(Arc::new(cat))
        }
        #[cfg(feature = "catalog-postgres")]
        "postgres" => {
            let url = cfg
                .catalog
                .postgres_url
                .as_deref()
                .ok_or_else(|| ZenError::catalog("postgres_url missing"))?;
            let cat = postgres::PostgresCatalog::open(url).await?;
            Ok(Arc::new(cat))
        }
        #[cfg(not(feature = "catalog-postgres"))]
        "postgres" => Err(ZenError::catalog(
            "postgres backend requires `catalog-postgres` feature",
        )),
        other => Err(ZenError::catalog(format!("unknown catalog backend: {other}"))),
    }
}
