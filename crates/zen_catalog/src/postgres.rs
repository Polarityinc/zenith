//! Postgres-backed catalog. The production backend.
//!
//! Connects via sqlx, applies migrations from `migrations/postgres/` on
//! open, and implements every method on the [`Catalog`] trait. Indexes
//! and primary keys mirror the SQLite layout one-for-one with
//! Postgres-native types (BYTEA, BIGINT, TIMESTAMPTZ).
//!
//! Concurrency model:
//!
//! - `next_commit_id`: `INSERT … ON CONFLICT DO UPDATE … RETURNING`
//!   inside a single transaction so concurrent writers serialize
//!   through the row lock and never see a duplicate id.
//! - Compaction leases: read-then-write inside a transaction with
//!   `SELECT … FOR UPDATE` so two workers can't both think they hold
//!   the lease.
//! - Segment / WAL inserts: ordinary INSERTs.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{PgPool, Row};

use zen_common::{
    CommitId, PartitionId, SchemaFingerprint, TenantId, TraceId, ZenError, ZenResult,
};

use crate::model::{NodeRow, SegmentRow, WalObjectRow};
use crate::Catalog;

pub struct PostgresCatalog {
    pub pool: PgPool,
}

impl PostgresCatalog {
    pub async fn open(url: &str) -> ZenResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(url)
            .await
            .map_err(|e| ZenError::catalog(format!("postgres connect: {e}")))?;
        sqlx::migrate!("./migrations/postgres")
            .run(&pool)
            .await
            .map_err(|e| ZenError::catalog(format!("postgres migrate: {e}")))?;
        Ok(Self { pool })
    }
}

fn fp_to_bytes(fp: SchemaFingerprint) -> Vec<u8> {
    fp.0.to_le_bytes().to_vec()
}
fn fp_from_bytes(b: &[u8]) -> SchemaFingerprint {
    let mut a = [0u8; 16];
    a.copy_from_slice(&b[..16]);
    SchemaFingerprint(u128::from_le_bytes(a))
}

fn trace_from_bytes(b: &[u8]) -> ZenResult<TraceId> {
    if b.len() != 16 {
        return Err(ZenError::catalog("trace_id length != 16"));
    }
    let mut a = [0u8; 16];
    a.copy_from_slice(b);
    Ok(TraceId(a))
}

fn segment_from_row(r: &PgRow) -> ZenResult<SegmentRow> {
    let segment_id_bytes: Vec<u8> = r.try_get("segment_id").map_err(catalog_err)?;
    let segment_id = uuid::Uuid::from_slice(&segment_id_bytes)
        .map_err(|e| ZenError::catalog(format!("segment_id parse: {e}")))?;
    Ok(SegmentRow {
        segment_id,
        tenant_id: TenantId(r.try_get::<i64, _>("tenant_id").map_err(catalog_err)? as u64),
        partition_id: PartitionId(
            r.try_get::<i64, _>("partition_id").map_err(catalog_err)? as u32,
        ),
        object_key: r.try_get("object_key").map_err(catalog_err)?,
        level: r.try_get::<i16, _>("level").map_err(catalog_err)?,
        byte_count: r.try_get("byte_count").map_err(catalog_err)?,
        row_count: r.try_get("row_count").map_err(catalog_err)?,
        time_min: r.try_get("time_min").map_err(catalog_err)?,
        time_max: r.try_get("time_max").map_err(catalog_err)?,
        trace_id_min: trace_from_bytes(&r.try_get::<Vec<u8>, _>("trace_id_min").map_err(catalog_err)?)?,
        trace_id_max: trace_from_bytes(&r.try_get::<Vec<u8>, _>("trace_id_max").map_err(catalog_err)?)?,
        commit_id_min: CommitId(r.try_get::<i64, _>("commit_id_min").map_err(catalog_err)? as u64),
        commit_id_max: CommitId(r.try_get::<i64, _>("commit_id_max").map_err(catalog_err)? as u64),
        schema_fingerprint: fp_from_bytes(
            &r.try_get::<Vec<u8>, _>("schema_fingerprint").map_err(catalog_err)?,
        ),
        rowgroup_index: r.try_get("rowgroup_index").map_err(catalog_err)?,
        superseded_at: r.try_get("superseded_at").map_err(catalog_err)?,
        created_at: r.try_get("created_at").map_err(catalog_err)?,
    })
}

fn catalog_err<E: std::fmt::Display>(e: E) -> ZenError {
    ZenError::catalog(format!("{e}"))
}

#[async_trait]
impl Catalog for PostgresCatalog {
    async fn ensure_tenant(&self, tenant: TenantId, name: &str) -> ZenResult<()> {
        sqlx::query("INSERT INTO tenants (tenant_id, name) VALUES ($1, $2) ON CONFLICT (tenant_id) DO NOTHING")
            .bind(tenant.0 as i64)
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| ZenError::catalog(format!("ensure_tenant: {e}")))?;
        Ok(())
    }

    async fn ensure_partition(&self, tenant: TenantId, partition: PartitionId) -> ZenResult<()> {
        sqlx::query(
            "INSERT INTO partitions (tenant_id, partition_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        )
        .bind(tenant.0 as i64)
        .bind(partition.0 as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| ZenError::catalog(format!("ensure_partition: {e}")))?;
        Ok(())
    }

    async fn next_commit_id(
        &self,
        tenant: TenantId,
        partition: PartitionId,
    ) -> ZenResult<CommitId> {
        // Atomic increment via UPSERT + RETURNING. The row lock
        // serializes concurrent writers without deadlock risk.
        let row: (i64,) = sqlx::query_as(
            "INSERT INTO commit_seq_state (tenant_id, partition_id, next_commit_id)
             VALUES ($1, $2, 2)
             ON CONFLICT (tenant_id, partition_id) DO UPDATE
                SET next_commit_id = commit_seq_state.next_commit_id + 1
             RETURNING next_commit_id - 1",
        )
        .bind(tenant.0 as i64)
        .bind(partition.0 as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| ZenError::catalog(format!("next_commit_id: {e}")))?;
        Ok(CommitId(row.0 as u64))
    }

    async fn register_wal_object(&self, w: WalObjectRow) -> ZenResult<()> {
        sqlx::query(
            "INSERT INTO wal_objects (
               wal_id, tenant_id, partition_id, object_key,
               commit_id_min, commit_id_max, byte_count, row_count,
               schema_fingerprint, consumed_at, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
        )
        .bind(w.wal_id.as_bytes().to_vec())
        .bind(w.tenant_id.0 as i64)
        .bind(w.partition_id.0 as i64)
        .bind(&w.object_key)
        .bind(w.commit_id_min.0 as i64)
        .bind(w.commit_id_max.0 as i64)
        .bind(w.byte_count)
        .bind(w.row_count)
        .bind(fp_to_bytes(w.schema_fingerprint))
        .bind(w.consumed_at)
        .bind(w.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| ZenError::catalog(format!("register_wal_object: {e}")))?;
        Ok(())
    }

    async fn list_wal_objects(
        &self,
        tenant: TenantId,
        partition: PartitionId,
        since_commit_id: CommitId,
    ) -> ZenResult<Vec<WalObjectRow>> {
        let rows = sqlx::query(
            "SELECT wal_id, tenant_id, partition_id, object_key,
                    commit_id_min, commit_id_max, byte_count, row_count,
                    schema_fingerprint, consumed_at, created_at
             FROM wal_objects
             WHERE tenant_id=$1 AND partition_id=$2
               AND commit_id_min >= $3 AND consumed_at IS NULL
             ORDER BY commit_id_min ASC",
        )
        .bind(tenant.0 as i64)
        .bind(partition.0 as i64)
        .bind(since_commit_id.0 as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ZenError::catalog(format!("list_wal_objects: {e}")))?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let wal_id_bytes: Vec<u8> = r.try_get("wal_id").map_err(catalog_err)?;
            out.push(WalObjectRow {
                wal_id: uuid::Uuid::from_slice(&wal_id_bytes)
                    .map_err(|e| ZenError::catalog(format!("wal_id parse: {e}")))?,
                tenant_id: TenantId(r.try_get::<i64, _>("tenant_id").map_err(catalog_err)? as u64),
                partition_id: PartitionId(
                    r.try_get::<i64, _>("partition_id").map_err(catalog_err)? as u32,
                ),
                object_key: r.try_get("object_key").map_err(catalog_err)?,
                commit_id_min: CommitId(
                    r.try_get::<i64, _>("commit_id_min").map_err(catalog_err)? as u64,
                ),
                commit_id_max: CommitId(
                    r.try_get::<i64, _>("commit_id_max").map_err(catalog_err)? as u64,
                ),
                byte_count: r.try_get("byte_count").map_err(catalog_err)?,
                row_count: r.try_get("row_count").map_err(catalog_err)?,
                schema_fingerprint: fp_from_bytes(
                    &r.try_get::<Vec<u8>, _>("schema_fingerprint").map_err(catalog_err)?,
                ),
                consumed_at: r.try_get("consumed_at").map_err(catalog_err)?,
                created_at: r.try_get("created_at").map_err(catalog_err)?,
            });
        }
        Ok(out)
    }

    async fn register_segment(&self, s: SegmentRow) -> ZenResult<()> {
        sqlx::query(
            "INSERT INTO segments (
               segment_id, tenant_id, partition_id, object_key, level,
               byte_count, row_count, time_min, time_max,
               trace_id_min, trace_id_max,
               commit_id_min, commit_id_max,
               schema_fingerprint, rowgroup_index, superseded_at, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)",
        )
        .bind(s.segment_id.as_bytes().to_vec())
        .bind(s.tenant_id.0 as i64)
        .bind(s.partition_id.0 as i64)
        .bind(&s.object_key)
        .bind(s.level)
        .bind(s.byte_count)
        .bind(s.row_count)
        .bind(s.time_min)
        .bind(s.time_max)
        .bind(s.trace_id_min.0.to_vec())
        .bind(s.trace_id_max.0.to_vec())
        .bind(s.commit_id_min.0 as i64)
        .bind(s.commit_id_max.0 as i64)
        .bind(fp_to_bytes(s.schema_fingerprint))
        .bind(&s.rowgroup_index)
        .bind(s.superseded_at)
        .bind(s.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| ZenError::catalog(format!("register_segment: {e}")))?;
        Ok(())
    }

    async fn list_segments_in_range(
        &self,
        tenant: TenantId,
        partition: PartitionId,
        time_min: i64,
        time_max: i64,
    ) -> ZenResult<Vec<SegmentRow>> {
        let rows = sqlx::query(
            "SELECT segment_id, tenant_id, partition_id, object_key, level,
                    byte_count, row_count, time_min, time_max,
                    trace_id_min, trace_id_max,
                    commit_id_min, commit_id_max,
                    schema_fingerprint, rowgroup_index, superseded_at, created_at
             FROM segments
             WHERE tenant_id=$1 AND partition_id=$2 AND superseded_at IS NULL
               AND time_max >= $3 AND time_min <= $4
             ORDER BY time_min ASC",
        )
        .bind(tenant.0 as i64)
        .bind(partition.0 as i64)
        .bind(time_min)
        .bind(time_max)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ZenError::catalog(format!("list_segments_in_range: {e}")))?;
        rows.iter().map(segment_from_row).collect()
    }

    async fn mark_wal_consumed(
        &self,
        tenant: TenantId,
        partition: PartitionId,
        consumed_through: CommitId,
        at: DateTime<Utc>,
    ) -> ZenResult<u64> {
        let r = sqlx::query(
            "UPDATE wal_objects SET consumed_at=$1
             WHERE tenant_id=$2 AND partition_id=$3
               AND commit_id_max <= $4 AND consumed_at IS NULL",
        )
        .bind(at)
        .bind(tenant.0 as i64)
        .bind(partition.0 as i64)
        .bind(consumed_through.0 as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| ZenError::catalog(format!("mark_wal_consumed: {e}")))?;
        Ok(r.rows_affected())
    }

    async fn acquire_compaction_lease(
        &self,
        tenant: TenantId,
        partition: PartitionId,
        worker_id: &str,
        ttl_seconds: u64,
    ) -> ZenResult<()> {
        let now = Utc::now();
        let exp = now + chrono::Duration::seconds(ttl_seconds as i64);
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| ZenError::catalog(format!("lease tx: {e}")))?;
        let row: Option<(String, DateTime<Utc>)> = sqlx::query_as(
            "SELECT worker_id, expires_at FROM compaction_leases
             WHERE tenant_id=$1 AND partition_id=$2 FOR UPDATE",
        )
        .bind(tenant.0 as i64)
        .bind(partition.0 as i64)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| ZenError::catalog(format!("lease read: {e}")))?;
        let take = match row {
            None => true,
            Some((cur_worker, cur_exp)) => cur_exp <= now || cur_worker == worker_id,
        };
        if !take {
            tx.rollback().await.ok();
            return Err(ZenError::conflict(format!(
                "compaction lease for ({tenant:?},{partition:?}) is held"
            )));
        }
        sqlx::query(
            "INSERT INTO compaction_leases (tenant_id, partition_id, worker_id, expires_at)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (tenant_id, partition_id)
                DO UPDATE SET worker_id = EXCLUDED.worker_id,
                              expires_at = EXCLUDED.expires_at",
        )
        .bind(tenant.0 as i64)
        .bind(partition.0 as i64)
        .bind(worker_id)
        .bind(exp)
        .execute(&mut *tx)
        .await
        .map_err(|e| ZenError::catalog(format!("lease upsert: {e}")))?;
        tx.commit()
            .await
            .map_err(|e| ZenError::catalog(format!("lease commit: {e}")))?;
        Ok(())
    }

    async fn release_compaction_lease(
        &self,
        tenant: TenantId,
        partition: PartitionId,
        worker_id: &str,
    ) -> ZenResult<()> {
        sqlx::query(
            "DELETE FROM compaction_leases
             WHERE tenant_id=$1 AND partition_id=$2 AND worker_id=$3",
        )
        .bind(tenant.0 as i64)
        .bind(partition.0 as i64)
        .bind(worker_id)
        .execute(&self.pool)
        .await
        .map_err(|e| ZenError::catalog(format!("release_compaction_lease: {e}")))?;
        Ok(())
    }

    async fn list_segments_for_tenant(&self, tenant: TenantId) -> ZenResult<Vec<SegmentRow>> {
        let rows = sqlx::query(
            "SELECT segment_id, tenant_id, partition_id, object_key, level,
                    byte_count, row_count, time_min, time_max,
                    trace_id_min, trace_id_max,
                    commit_id_min, commit_id_max,
                    schema_fingerprint, rowgroup_index, superseded_at, created_at
             FROM segments
             WHERE tenant_id=$1 AND superseded_at IS NULL
             ORDER BY time_min ASC",
        )
        .bind(tenant.0 as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ZenError::catalog(format!("list_segments_for_tenant: {e}")))?;
        rows.iter().map(segment_from_row).collect()
    }

    async fn mark_segments_superseded(
        &self,
        tenant: TenantId,
        segment_ids: &[uuid::Uuid],
        at: DateTime<Utc>,
    ) -> ZenResult<u64> {
        if segment_ids.is_empty() {
            return Ok(0);
        }
        let mut total: u64 = 0;
        for id in segment_ids {
            let r = sqlx::query(
                "UPDATE segments SET superseded_at=$1
                 WHERE segment_id=$2 AND tenant_id=$3
                   AND superseded_at IS NULL",
            )
            .bind(at)
            .bind(id.as_bytes().to_vec())
            .bind(tenant.0 as i64)
            .execute(&self.pool)
            .await
            .map_err(|e| ZenError::catalog(format!("mark_segments_superseded: {e}")))?;
            total += r.rows_affected();
        }
        Ok(total)
    }

    async fn upsert_node(&self, row: NodeRow) -> ZenResult<()> {
        sqlx::query(
            "INSERT INTO nodes (node_id, endpoint, role, shards, last_heartbeat_ms)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (node_id) DO UPDATE
                SET endpoint = EXCLUDED.endpoint,
                    role = EXCLUDED.role,
                    shards = EXCLUDED.shards,
                    last_heartbeat_ms = EXCLUDED.last_heartbeat_ms",
        )
        .bind(row.node_id.as_bytes().to_vec())
        .bind(row.endpoint)
        .bind(row.role)
        .bind(row.shards)
        .bind(row.last_heartbeat_ms)
        .execute(&self.pool)
        .await
        .map_err(|e| ZenError::catalog(format!("upsert_node: {e}")))?;
        Ok(())
    }

    async fn list_nodes(&self) -> ZenResult<Vec<NodeRow>> {
        let rows = sqlx::query(
            "SELECT node_id, endpoint, role, shards, last_heartbeat_ms FROM nodes",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ZenError::catalog(format!("list_nodes: {e}")))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let node_id_bytes: Vec<u8> = r.try_get("node_id").map_err(catalog_err)?;
            if node_id_bytes.len() != 16 {
                return Err(ZenError::catalog("nodes.node_id length != 16"));
            }
            let mut a = [0u8; 16];
            a.copy_from_slice(&node_id_bytes);
            out.push(NodeRow {
                node_id: uuid::Uuid::from_bytes(a),
                endpoint: r.try_get("endpoint").map_err(catalog_err)?,
                role: r.try_get("role").map_err(catalog_err)?,
                shards: r.try_get("shards").map_err(catalog_err)?,
                last_heartbeat_ms: r.try_get("last_heartbeat_ms").map_err(catalog_err)?,
            });
        }
        Ok(out)
    }
}
