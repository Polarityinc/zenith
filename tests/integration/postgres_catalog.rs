//! `PostgresCatalog` integration test. Runs ONLY when `ZEN_PG_TEST_URL`
//! points at a reachable Postgres instance:
//!
//!   createdb zenith_pgtest
//!   ZEN_PG_TEST_URL='postgres://USER@localhost:5432/zenith_pgtest' \
//!     cargo test -p zen_integration_tests --test postgres_catalog
//!
//! Without the env var the test is a no-op so `cargo test` on a
//! developer laptop without Postgres still passes.
//!
//! All assertions live inside ONE `#[tokio::test]` because the sqlx
//! `Pool` is tied to the runtime that created it — splitting into many
//! `#[tokio::test]` functions would re-open the pool per test (and
//! re-race the migration runner).

use std::sync::Arc;

use chrono::Utc;
use uuid::Uuid;

use zen_catalog::{model::*, Catalog, PostgresCatalog};
use zen_common::{CommitId, PartitionId, SchemaFingerprint, TenantId, TraceId};

fn pg_url() -> Option<String> {
    std::env::var("ZEN_PG_TEST_URL").ok().filter(|s| !s.is_empty())
}

fn seg_row(tenant: u64, partition: u32, time_min: i64, time_max: i64) -> SegmentRow {
    SegmentRow {
        segment_id: Uuid::new_v4(),
        tenant_id: TenantId(tenant),
        partition_id: PartitionId(partition),
        object_key: format!("seg/{tenant}/{partition}/{}", Uuid::new_v4()),
        level: 0,
        byte_count: 1024,
        row_count: 100,
        time_min,
        time_max,
        trace_id_min: TraceId([0u8; 16]),
        trace_id_max: TraceId([0xff; 16]),
        commit_id_min: CommitId(1),
        commit_id_max: CommitId(100),
        schema_fingerprint: SchemaFingerprint(0xabcd),
        rowgroup_index: vec![0u8; 8],
        superseded_at: None,
        created_at: Utc::now(),
    }
}

async fn truncate(cat: &PostgresCatalog) {
    sqlx::query(
        "TRUNCATE TABLE segments, wal_objects, compaction_leases,
                       commit_seq_state, partitions, tenants, nodes
         RESTART IDENTITY CASCADE",
    )
    .execute(&cat.pool)
    .await
    .expect("truncate");
}

#[tokio::test]
async fn postgres_catalog_full_contract() {
    let Some(url) = pg_url() else {
        eprintln!("ZEN_PG_TEST_URL not set; skipping postgres catalog test");
        return;
    };

    // Open + apply migrations once. Concrete handle for the truncate
    // helper; trait object for the actual sub-tests.
    let pg = PostgresCatalog::open(&url).await.expect("open postgres");
    let cat: Arc<dyn Catalog> = Arc::new(PostgresCatalog::open(&url).await.expect("open postgres again"));

    // Re-open should be a no-op (migrations idempotent).
    {
        let cat2 = PostgresCatalog::open(&url).await.expect("reopen postgres");
        let row: (i64,) = sqlx::query_as("SELECT count(*) FROM _sqlx_migrations")
            .fetch_one(&cat2.pool)
            .await
            .unwrap();
        assert!(
            row.0 >= 2,
            "expected at least 2 applied migrations, got {}",
            row.0
        );
    }

    // ─── 1: next_commit_id is monotonic under 16-way concurrency.
    truncate(&pg).await;
    cat.ensure_tenant(TenantId(1), "t").await.unwrap();
    cat.ensure_partition(TenantId(1), PartitionId(0))
        .await
        .unwrap();
    let mut handles = Vec::new();
    for _ in 0..16 {
        let cat = cat.clone();
        handles.push(tokio::spawn(async move {
            let mut out = Vec::new();
            for _ in 0..32 {
                out.push(
                    cat.next_commit_id(TenantId(1), PartitionId(0))
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
        assert_ne!(
            w[0], w[1],
            "Postgres row-lock must serialize next_commit_id"
        );
    }
    assert_eq!(all.len(), 16 * 32);
    eprintln!("  ✓ next_commit_id monotonic under 16-way × 32 fan-out");

    // ─── 2: segment register + range query.
    truncate(&pg).await;
    cat.ensure_tenant(TenantId(1), "t").await.unwrap();
    cat.ensure_partition(TenantId(1), PartitionId(0))
        .await
        .unwrap();
    cat.register_segment(seg_row(1, 0, 1000, 2000))
        .await
        .unwrap();
    cat.register_segment(seg_row(1, 0, 5000, 6000))
        .await
        .unwrap();
    let in_range = cat
        .list_segments_in_range(TenantId(1), PartitionId(0), 500, 1500)
        .await
        .unwrap();
    assert_eq!(in_range.len(), 1);
    let both = cat
        .list_segments_in_range(TenantId(1), PartitionId(0), 0, 10_000)
        .await
        .unwrap();
    assert_eq!(both.len(), 2);
    eprintln!("  ✓ segment register + list_in_range");

    // ─── 3: cross-tenant supersede is BLOCKED.
    truncate(&pg).await;
    let s1 = seg_row(1, 0, 1, 2);
    let id = s1.segment_id;
    cat.register_segment(s1).await.unwrap();
    let n_cross = cat
        .mark_segments_superseded(TenantId(2), &[id], Utc::now())
        .await
        .unwrap();
    assert_eq!(n_cross, 0, "cross-tenant supersede must affect 0 rows");
    assert_eq!(
        cat.list_segments_for_tenant(TenantId(1))
            .await
            .unwrap()
            .len(),
        1
    );
    let n_real = cat
        .mark_segments_superseded(TenantId(1), &[id], Utc::now())
        .await
        .unwrap();
    assert_eq!(n_real, 1);
    assert_eq!(
        cat.list_segments_for_tenant(TenantId(1))
            .await
            .unwrap()
            .len(),
        0
    );
    eprintln!("  ✓ cross-tenant supersede blocked, owner allowed");

    // ─── 4: lease lifecycle.
    truncate(&pg).await;
    cat.acquire_compaction_lease(TenantId(1), PartitionId(0), "w1", 60)
        .await
        .unwrap();
    assert!(
        cat.acquire_compaction_lease(TenantId(1), PartitionId(0), "w2", 60)
            .await
            .is_err(),
        "different worker must be blocked"
    );
    cat.acquire_compaction_lease(TenantId(1), PartitionId(0), "w1", 60)
        .await
        .unwrap();
    cat.release_compaction_lease(TenantId(1), PartitionId(0), "w1")
        .await
        .unwrap();
    cat.acquire_compaction_lease(TenantId(1), PartitionId(0), "w2", 60)
        .await
        .unwrap();
    eprintln!("  ✓ compaction lease acquire / refresh / release");

    // ─── 5: nodes upsert + list.
    truncate(&pg).await;
    let id_a = Uuid::new_v4();
    let id_b = Uuid::new_v4();
    cat.upsert_node(NodeRow {
        node_id: id_a,
        endpoint: "http://a:8080".into(),
        role: "all".into(),
        shards: "*".into(),
        last_heartbeat_ms: 100,
    })
    .await
    .unwrap();
    cat.upsert_node(NodeRow {
        node_id: id_b,
        endpoint: "http://b:8080".into(),
        role: "worker".into(),
        shards: "tenant=1,2".into(),
        last_heartbeat_ms: 200,
    })
    .await
    .unwrap();
    cat.upsert_node(NodeRow {
        node_id: id_a,
        endpoint: "http://a:8080".into(),
        role: "all".into(),
        shards: "*".into(),
        last_heartbeat_ms: 999,
    })
    .await
    .unwrap();
    let rows = cat.list_nodes().await.unwrap();
    assert_eq!(rows.len(), 2);
    let row_a = rows.iter().find(|r| r.node_id == id_a).unwrap();
    assert_eq!(
        row_a.last_heartbeat_ms, 999,
        "upsert must update, not duplicate"
    );
    eprintln!("  ✓ nodes upsert + list");

    // Final cleanup so a follow-up run starts fresh.
    truncate(&pg).await;
}
