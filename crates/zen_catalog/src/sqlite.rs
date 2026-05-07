//! Sqlite-backed catalog. Embedded; zero-install for dev.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::str::FromStr;

use zen_common::{CommitId, PartitionId, SchemaFingerprint, TenantId, TraceId, ZenError, ZenResult};

use crate::model::{SegmentRow, WalObjectRow};
use crate::Catalog;

pub struct SqliteCatalog {
    pub pool: SqlitePool,
}

impl SqliteCatalog {
    pub async fn open(path: &str) -> ZenResult<Self> {
        // Ensure parent dir exists for file-based sqlite.
        if !path.starts_with(":memory:") {
            if let Some(parent) = std::path::Path::new(path).parent() {
                if !parent.as_os_str().is_empty() {
                    let _ = tokio::fs::create_dir_all(parent).await;
                }
            }
        }
        let opts = SqliteConnectOptions::from_str(&format!("sqlite://{path}?mode=rwc"))
            .map_err(|e| ZenError::catalog(format!("sqlite opts: {e}")))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .busy_timeout(std::time::Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(opts)
            .await
            .map_err(|e| ZenError::catalog(format!("sqlite connect: {e}")))?;
        Self::run_migrations(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn open_in_memory() -> ZenResult<Self> {
        Self::open(":memory:").await
    }

    async fn run_migrations(pool: &SqlitePool) -> ZenResult<()> {
        let stmts = [
            r#"CREATE TABLE IF NOT EXISTS tenants (
                tenant_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )"#,
            r#"CREATE TABLE IF NOT EXISTS partitions (
                tenant_id INTEGER NOT NULL,
                partition_id INTEGER NOT NULL,
                PRIMARY KEY (tenant_id, partition_id)
            )"#,
            r#"CREATE TABLE IF NOT EXISTS commit_seq_state (
                tenant_id INTEGER NOT NULL,
                partition_id INTEGER NOT NULL,
                next_commit_id INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (tenant_id, partition_id)
            )"#,
            r#"CREATE TABLE IF NOT EXISTS wal_objects (
                wal_id BLOB PRIMARY KEY,
                tenant_id INTEGER NOT NULL,
                partition_id INTEGER NOT NULL,
                object_key TEXT NOT NULL,
                commit_id_min INTEGER NOT NULL,
                commit_id_max INTEGER NOT NULL,
                byte_count INTEGER NOT NULL,
                row_count INTEGER NOT NULL,
                schema_fingerprint BLOB NOT NULL,
                consumed_at TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )"#,
            r#"CREATE INDEX IF NOT EXISTS wal_objects_unconsumed
               ON wal_objects (tenant_id, partition_id, commit_id_min)
               WHERE consumed_at IS NULL"#,
            r#"CREATE TABLE IF NOT EXISTS segments (
                segment_id BLOB PRIMARY KEY,
                tenant_id INTEGER NOT NULL,
                partition_id INTEGER NOT NULL,
                object_key TEXT NOT NULL,
                level INTEGER NOT NULL DEFAULT 0,
                byte_count INTEGER NOT NULL,
                row_count INTEGER NOT NULL,
                time_min INTEGER NOT NULL,
                time_max INTEGER NOT NULL,
                trace_id_min BLOB NOT NULL,
                trace_id_max BLOB NOT NULL,
                commit_id_min INTEGER NOT NULL,
                commit_id_max INTEGER NOT NULL,
                schema_fingerprint BLOB NOT NULL,
                rowgroup_index BLOB NOT NULL,
                superseded_at TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )"#,
            r#"CREATE INDEX IF NOT EXISTS segments_active
               ON segments (tenant_id, partition_id, time_min, time_max)
               WHERE superseded_at IS NULL"#,
            r#"CREATE TABLE IF NOT EXISTS compaction_leases (
                tenant_id INTEGER NOT NULL,
                partition_id INTEGER NOT NULL,
                worker_id TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                PRIMARY KEY (tenant_id, partition_id)
            )"#,
        ];
        for s in stmts {
            sqlx::query(s)
                .execute(pool)
                .await
                .map_err(|e| ZenError::catalog(format!("migration: {e}")))?;
        }
        Ok(())
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

#[async_trait]
impl Catalog for SqliteCatalog {
    async fn ensure_tenant(&self, tenant: TenantId, name: &str) -> ZenResult<()> {
        sqlx::query("INSERT OR IGNORE INTO tenants (tenant_id, name) VALUES (?1, ?2)")
            .bind(tenant.0 as i64)
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| ZenError::catalog(format!("ensure_tenant: {e}")))?;
        Ok(())
    }

    async fn ensure_partition(&self, tenant: TenantId, partition: PartitionId) -> ZenResult<()> {
        sqlx::query(
            "INSERT OR IGNORE INTO partitions (tenant_id, partition_id) VALUES (?1, ?2)",
        )
        .bind(tenant.0 as i64)
        .bind(partition.0 as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| ZenError::catalog(format!("ensure_partition: {e}")))?;
        sqlx::query(
            "INSERT OR IGNORE INTO commit_seq_state (tenant_id, partition_id, next_commit_id)
             VALUES (?1, ?2, 1)",
        )
        .bind(tenant.0 as i64)
        .bind(partition.0 as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| ZenError::catalog(format!("init commit seq: {e}")))?;
        Ok(())
    }

    async fn next_commit_id(&self, tenant: TenantId, partition: PartitionId) -> ZenResult<CommitId> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| ZenError::catalog(format!("tx begin: {e}")))?;

        // BEGIN IMMEDIATE so we hold the lock through the read+write.
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *tx)
            .await
            .ok();

        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT next_commit_id FROM commit_seq_state WHERE tenant_id=?1 AND partition_id=?2",
        )
        .bind(tenant.0 as i64)
        .bind(partition.0 as i64)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| ZenError::catalog(format!("read commit seq: {e}")))?;

        let next: i64 = match row {
            Some((v,)) => v,
            None => {
                sqlx::query(
                    "INSERT OR IGNORE INTO partitions (tenant_id, partition_id) VALUES (?1, ?2)",
                )
                .bind(tenant.0 as i64)
                .bind(partition.0 as i64)
                .execute(&mut *tx)
                .await
                .map_err(|e| ZenError::catalog(format!("ensure partition during next: {e}")))?;
                sqlx::query(
                    "INSERT INTO commit_seq_state (tenant_id, partition_id, next_commit_id) VALUES (?1, ?2, 1)",
                )
                .bind(tenant.0 as i64)
                .bind(partition.0 as i64)
                .execute(&mut *tx)
                .await
                .map_err(|e| ZenError::catalog(format!("init commit seq: {e}")))?;
                1
            }
        };

        sqlx::query(
            "UPDATE commit_seq_state SET next_commit_id = ?3
             WHERE tenant_id=?1 AND partition_id=?2",
        )
        .bind(tenant.0 as i64)
        .bind(partition.0 as i64)
        .bind(next + 1)
        .execute(&mut *tx)
        .await
        .map_err(|e| ZenError::catalog(format!("bump commit seq: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| ZenError::catalog(format!("tx commit: {e}")))?;

        Ok(CommitId(next as u64))
    }

    async fn register_wal_object(&self, w: WalObjectRow) -> ZenResult<()> {
        sqlx::query(
            "INSERT INTO wal_objects (
               wal_id, tenant_id, partition_id, object_key,
               commit_id_min, commit_id_max, byte_count, row_count, schema_fingerprint, consumed_at, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
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
        .bind(w.consumed_at.map(|d| d.to_rfc3339()))
        .bind(w.created_at.to_rfc3339())
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
        let rows: Vec<(
            Vec<u8>, i64, i64, String, i64, i64, i64, i64, Vec<u8>, Option<String>, String,
        )> = sqlx::query_as(
            "SELECT wal_id, tenant_id, partition_id, object_key,
                    commit_id_min, commit_id_max, byte_count, row_count, schema_fingerprint,
                    consumed_at, created_at
             FROM wal_objects
             WHERE tenant_id=?1 AND partition_id=?2 AND commit_id_min >= ?3 AND consumed_at IS NULL
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
            out.push(WalObjectRow {
                wal_id: uuid::Uuid::from_slice(&r.0)
                    .map_err(|e| ZenError::catalog(format!("wal_id parse: {e}")))?,
                tenant_id: TenantId(r.1 as u64),
                partition_id: PartitionId(r.2 as u32),
                object_key: r.3,
                commit_id_min: CommitId(r.4 as u64),
                commit_id_max: CommitId(r.5 as u64),
                byte_count: r.6,
                row_count: r.7,
                schema_fingerprint: fp_from_bytes(&r.8),
                consumed_at: r.9.and_then(|s| DateTime::parse_from_rfc3339(&s).ok().map(|d| d.with_timezone(&Utc))),
                created_at: DateTime::parse_from_rfc3339(&r.10)
                    .map_err(|e| ZenError::catalog(format!("created_at parse: {e}")))?
                    .with_timezone(&Utc),
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
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
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
        .bind(s.superseded_at.map(|d| d.to_rfc3339()))
        .bind(s.created_at.to_rfc3339())
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
        let rows: Vec<SegmentRowSql> = sqlx::query_as::<_, SegmentRowSql>(
            "SELECT segment_id, tenant_id, partition_id, object_key, level,
                    byte_count, row_count, time_min, time_max,
                    trace_id_min, trace_id_max,
                    commit_id_min, commit_id_max,
                    schema_fingerprint, rowgroup_index, superseded_at, created_at
             FROM segments
             WHERE tenant_id=?1 AND partition_id=?2 AND superseded_at IS NULL
               AND time_max >= ?3 AND time_min <= ?4
             ORDER BY time_min ASC",
        )
        .bind(tenant.0 as i64)
        .bind(partition.0 as i64)
        .bind(time_min)
        .bind(time_max)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ZenError::catalog(format!("list_segments_in_range: {e}")))?;

        rows.into_iter().map(|r| r.into_row()).collect()
    }

    async fn mark_wal_consumed(
        &self,
        tenant: TenantId,
        partition: PartitionId,
        consumed_through: CommitId,
        at: DateTime<Utc>,
    ) -> ZenResult<u64> {
        let r = sqlx::query(
            "UPDATE wal_objects SET consumed_at=?1
             WHERE tenant_id=?2 AND partition_id=?3 AND commit_id_max <= ?4 AND consumed_at IS NULL",
        )
        .bind(at.to_rfc3339())
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
        // Read current.
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT worker_id, expires_at FROM compaction_leases
             WHERE tenant_id=?1 AND partition_id=?2",
        )
        .bind(tenant.0 as i64)
        .bind(partition.0 as i64)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| ZenError::catalog(format!("lease read: {e}")))?;
        let take = match row {
            None => true,
            Some((cur_worker, exp_str)) => {
                let cur_exp = DateTime::parse_from_rfc3339(&exp_str)
                    .map_err(|e| ZenError::catalog(format!("lease exp parse: {e}")))?
                    .with_timezone(&Utc);
                if cur_exp <= now {
                    true // expired
                } else if cur_worker == worker_id {
                    true // refresh
                } else {
                    false
                }
            }
        };
        if !take {
            tx.rollback().await.ok();
            return Err(ZenError::conflict(format!(
                "compaction lease for ({tenant:?},{partition:?}) is held"
            )));
        }
        sqlx::query(
            "INSERT INTO compaction_leases (tenant_id, partition_id, worker_id, expires_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(tenant_id, partition_id) DO UPDATE SET worker_id=excluded.worker_id, expires_at=excluded.expires_at",
        )
        .bind(tenant.0 as i64)
        .bind(partition.0 as i64)
        .bind(worker_id)
        .bind(exp.to_rfc3339())
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
             WHERE tenant_id=?1 AND partition_id=?2 AND worker_id=?3",
        )
        .bind(tenant.0 as i64)
        .bind(partition.0 as i64)
        .bind(worker_id)
        .execute(&self.pool)
        .await
        .map_err(|e| ZenError::catalog(format!("release lease: {e}")))?;
        Ok(())
    }

    async fn list_segments_for_tenant(&self, tenant: TenantId) -> ZenResult<Vec<SegmentRow>> {
        let rows: Vec<SegmentRowSql> = sqlx::query_as::<_, SegmentRowSql>(
            "SELECT segment_id, tenant_id, partition_id, object_key, level,
                    byte_count, row_count, time_min, time_max,
                    trace_id_min, trace_id_max,
                    commit_id_min, commit_id_max,
                    schema_fingerprint, rowgroup_index, superseded_at, created_at
             FROM segments
             WHERE tenant_id=?1 AND superseded_at IS NULL
             ORDER BY time_min ASC",
        )
        .bind(tenant.0 as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ZenError::catalog(format!("list_segments_for_tenant: {e}")))?;
        rows.into_iter().map(|r| r.into_row()).collect()
    }

    async fn mark_segments_superseded(
        &self,
        segment_ids: &[uuid::Uuid],
        at: DateTime<Utc>,
    ) -> ZenResult<u64> {
        if segment_ids.is_empty() {
            return Ok(0);
        }
        let mut total: u64 = 0;
        for id in segment_ids {
            let r = sqlx::query(
                "UPDATE segments SET superseded_at=?1 WHERE segment_id=?2 AND superseded_at IS NULL",
            )
            .bind(at.to_rfc3339())
            .bind(id.as_bytes().to_vec())
            .execute(&self.pool)
            .await
            .map_err(|e| ZenError::catalog(format!("mark superseded: {e}")))?;
            total += r.rows_affected();
        }
        Ok(total)
    }
}

#[derive(sqlx::FromRow)]
struct SegmentRowSql {
    segment_id: Vec<u8>,
    tenant_id: i64,
    partition_id: i64,
    object_key: String,
    level: i64,
    byte_count: i64,
    row_count: i64,
    time_min: i64,
    time_max: i64,
    trace_id_min: Vec<u8>,
    trace_id_max: Vec<u8>,
    commit_id_min: i64,
    commit_id_max: i64,
    schema_fingerprint: Vec<u8>,
    rowgroup_index: Vec<u8>,
    superseded_at: Option<String>,
    created_at: String,
}

impl SegmentRowSql {
    fn into_row(self) -> ZenResult<SegmentRow> {
        let mut tmin = [0u8; 16];
        let mut tmax = [0u8; 16];
        if self.trace_id_min.len() != 16 || self.trace_id_max.len() != 16 {
            return Err(ZenError::catalog("trace_id width != 16"));
        }
        tmin.copy_from_slice(&self.trace_id_min);
        tmax.copy_from_slice(&self.trace_id_max);
        Ok(SegmentRow {
            segment_id: uuid::Uuid::from_slice(&self.segment_id)
                .map_err(|e| ZenError::catalog(format!("segment_id parse: {e}")))?,
            tenant_id: TenantId(self.tenant_id as u64),
            partition_id: PartitionId(self.partition_id as u32),
            object_key: self.object_key,
            level: self.level as i16,
            byte_count: self.byte_count,
            row_count: self.row_count,
            time_min: self.time_min,
            time_max: self.time_max,
            trace_id_min: TraceId(tmin),
            trace_id_max: TraceId(tmax),
            commit_id_min: CommitId(self.commit_id_min as u64),
            commit_id_max: CommitId(self.commit_id_max as u64),
            schema_fingerprint: fp_from_bytes(&self.schema_fingerprint),
            rowgroup_index: self.rowgroup_index,
            superseded_at: self
                .superseded_at
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok().map(|d| d.with_timezone(&Utc))),
            created_at: DateTime::parse_from_rfc3339(&self.created_at)
                .map_err(|e| ZenError::catalog(format!("created_at parse: {e}")))?
                .with_timezone(&Utc),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn migrations_run_idempotent() {
        let cat = SqliteCatalog::open_in_memory().await.unwrap();
        // Run again — should not fail.
        SqliteCatalog::run_migrations(&cat.pool).await.unwrap();
    }

    #[tokio::test]
    async fn next_commit_id_monotonic_concurrent() {
        let cat = SqliteCatalog::open_in_memory().await.unwrap();
        cat.ensure_tenant(TenantId(1), "t").await.unwrap();
        cat.ensure_partition(TenantId(1), PartitionId(0)).await.unwrap();

        let n = 50;
        let cat = std::sync::Arc::new(cat);
        let mut handles = Vec::new();
        for _ in 0..n {
            let c = cat.clone();
            handles.push(tokio::spawn(async move {
                c.next_commit_id(TenantId(1), PartitionId(0)).await.unwrap()
            }));
        }
        let mut got: Vec<u64> = Vec::with_capacity(n);
        for h in handles {
            got.push(h.await.unwrap().0);
        }
        got.sort_unstable();
        // All distinct.
        for i in 1..got.len() {
            assert_ne!(got[i], got[i - 1], "duplicate commit id observed");
        }
    }

    #[tokio::test]
    async fn lease_lifecycle() {
        let cat = SqliteCatalog::open_in_memory().await.unwrap();
        cat.acquire_compaction_lease(TenantId(1), PartitionId(0), "worker-A", 60)
            .await
            .unwrap();
        // Same worker can re-acquire (refresh).
        cat.acquire_compaction_lease(TenantId(1), PartitionId(0), "worker-A", 60)
            .await
            .unwrap();
        // Different worker cannot.
        let r = cat
            .acquire_compaction_lease(TenantId(1), PartitionId(0), "worker-B", 60)
            .await;
        assert!(r.is_err());
        // Release.
        cat.release_compaction_lease(TenantId(1), PartitionId(0), "worker-A")
            .await
            .unwrap();
        // Now B can take it.
        cat.acquire_compaction_lease(TenantId(1), PartitionId(0), "worker-B", 60)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn wal_register_list_consume() {
        let cat = SqliteCatalog::open_in_memory().await.unwrap();
        cat.ensure_tenant(TenantId(1), "t").await.unwrap();
        cat.ensure_partition(TenantId(1), PartitionId(0)).await.unwrap();

        for i in 1..=5 {
            cat.register_wal_object(WalObjectRow {
                wal_id: uuid::Uuid::new_v4(),
                tenant_id: TenantId(1),
                partition_id: PartitionId(0),
                object_key: format!("wal/x/{i}.wal"),
                commit_id_min: CommitId(i),
                commit_id_max: CommitId(i),
                byte_count: 1024,
                row_count: 100,
                schema_fingerprint: SchemaFingerprint(0xfeed),
                consumed_at: None,
                created_at: Utc::now(),
            })
            .await
            .unwrap();
        }
        let list = cat
            .list_wal_objects(TenantId(1), PartitionId(0), CommitId(1))
            .await
            .unwrap();
        assert_eq!(list.len(), 5);

        let consumed = cat
            .mark_wal_consumed(TenantId(1), PartitionId(0), CommitId(3), Utc::now())
            .await
            .unwrap();
        assert_eq!(consumed, 3);

        let list = cat
            .list_wal_objects(TenantId(1), PartitionId(0), CommitId(1))
            .await
            .unwrap();
        assert_eq!(list.len(), 2);
    }

    #[tokio::test]
    async fn segment_register_list_in_range() {
        let cat = SqliteCatalog::open_in_memory().await.unwrap();
        cat.ensure_tenant(TenantId(1), "t").await.unwrap();
        cat.ensure_partition(TenantId(1), PartitionId(0)).await.unwrap();
        for i in 0..3 {
            cat.register_segment(SegmentRow {
                segment_id: uuid::Uuid::new_v4(),
                tenant_id: TenantId(1),
                partition_id: PartitionId(0),
                object_key: format!("seg/{i}.zseg"),
                level: 0,
                byte_count: 1024,
                row_count: 100,
                time_min: i * 1000,
                time_max: i * 1000 + 999,
                trace_id_min: TraceId([0; 16]),
                trace_id_max: TraceId([0xFF; 16]),
                commit_id_min: CommitId(1),
                commit_id_max: CommitId(10),
                schema_fingerprint: SchemaFingerprint(0xbabe),
                rowgroup_index: vec![],
                superseded_at: None,
                created_at: Utc::now(),
            })
            .await
            .unwrap();
        }
        let list = cat
            .list_segments_in_range(TenantId(1), PartitionId(0), 1500, 1500)
            .await
            .unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].time_min, 1000);
    }
}
