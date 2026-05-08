//! Micro-benchmark for the Phase-A durability change.
//!
//! Compares write_flush latency between `LocalFsStore::new` (default,
//! durable=true with fsync) and `LocalFsStore::new_unsafe_fast`
//! (durable=false). Run with `cargo test -p zen_integration_tests --test
//! durability_bench --release -- --nocapture --ignored`. Marked
//! `#[ignore]` so it doesn't run on every `cargo test` — it takes ~30 s.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tokio::net::TcpListener;

use zen_catalog::{Catalog, SqliteCatalog};
use zen_common::{Config, PartitionId, TenantId};
use zen_server::{http::router, ServerState};
use zen_storage::{local_fs::LocalFsStore, BlobStore};

const N_INGEST: usize = 100;
/// 100 × 100 KB matches the Brainstore reference write_flush workload.
const SPANS_PER_BATCH: usize = 100;
const PROMPT_BYTES: usize = 100_000;

async fn spawn_server_with(store: Arc<dyn BlobStore>) -> String {
    let cat: Arc<dyn Catalog> = Arc::new(SqliteCatalog::open_in_memory().await.unwrap());
    cat.ensure_tenant(TenantId(1), "t").await.unwrap();
    cat.ensure_partition(TenantId(1), PartitionId(0))
        .await
        .unwrap();
    let mut cfg = Config::default();
    cfg.ingest.flush_max_bytes = 256 * 1024 * 1024;
    let state = ServerState::new(cfg, cat, store);
    let app = router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    format!("http://{addr}")
}

fn prompt_string() -> String {
    "x".repeat(PROMPT_BYTES)
}

fn build_batch_body(tenant_id: u64) -> serde_json::Value {
    let prompt = prompt_string();
    let spans: Vec<_> = (0..SPANS_PER_BATCH)
        .map(|i| {
            serde_json::json!({
                "start_time_ms": 1_000 + i as i64,
                "end_time_ms": 1_005 + i as i64,
                "duration_ms": 5,
                "model": "gpt-4o",
                "status": "ok",
                "prompt": prompt,
            })
        })
        .collect();
    serde_json::json!({
        "tenant_id": tenant_id,
        "partition_id": 0,
        "spans": spans,
    })
}

async fn run_one_pass(store_factory: impl Fn(PathBuf) -> Arc<dyn BlobStore>) -> Vec<f64> {
    let dir = TempDir::new().unwrap();
    let store = store_factory(dir.path().to_path_buf());
    let url = spawn_server_with(store).await;

    let client = reqwest::Client::new();
    let mut latencies_ms = Vec::with_capacity(N_INGEST);
    // Warm-up.
    let body = build_batch_body(1);
    for _ in 0..3 {
        let _ = client
            .post(format!("{url}/v1/ingest"))
            .json(&body)
            .send()
            .await
            .unwrap();
    }

    for _ in 0..N_INGEST {
        let body = build_batch_body(1);
        let started = Instant::now();
        let r = client
            .post(format!("{url}/v1/ingest"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(r.status().is_success(), "ingest failed: {}", r.status());
        let _ = r.text().await;
        latencies_ms.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    latencies_ms
}

fn p_quantile(samples: &mut [f64], q: f64) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((samples.len() - 1) as f64 * q).round() as usize;
    samples[idx]
}

#[tokio::test]
#[ignore]
async fn durability_vs_unsafe_fast_write_flush() {
    let mut durable = run_one_pass(|root| Arc::new(LocalFsStore::new(root).unwrap())).await;
    let mut fast = run_one_pass(|root| Arc::new(LocalFsStore::new_unsafe_fast(root).unwrap())).await;

    let durable_p50 = p_quantile(&mut durable, 0.50);
    let durable_p95 = p_quantile(&mut durable, 0.95);
    let fast_p50 = p_quantile(&mut fast, 0.50);
    let fast_p95 = p_quantile(&mut fast, 0.95);

    println!(
        "\n write_flush 100×100KB sequential (n={N_INGEST})\n  durable=true   p50={durable_p50:.1} ms  p95={durable_p95:.1} ms"
    );
    println!(
        "  durable=false  p50={fast_p50:.1} ms  p95={fast_p95:.1} ms\n  ratio          p50={:.2}×  p95={:.2}×",
        durable_p50 / fast_p50,
        durable_p95 / fast_p95
    );
}

/// Measure group-commit's gain: 16 concurrent writers each flushing
/// their own 100 KB batch. With group commit, only ~1 fsync runs per
/// coalesce window, recovering most of the durable-mode tax.
#[tokio::test]
#[ignore]
async fn group_commit_concurrent_write_flush() {
    use std::time::Instant;

    async fn one_concurrent_pass(
        store_factory: impl Fn(PathBuf) -> Arc<dyn BlobStore>,
    ) -> Vec<f64> {
        let dir = tempfile::TempDir::new().unwrap();
        let store = store_factory(dir.path().to_path_buf());
        let url = spawn_server_with(store).await;

        // Warm up.
        let client = reqwest::Client::new();
        let warmup_body = build_batch_body(1);
        for _ in 0..3 {
            let _ = client
                .post(format!("{url}/v1/ingest"))
                .json(&warmup_body)
                .send()
                .await
                .unwrap();
        }

        const CONCURRENCY: usize = 16;
        const ROUNDS: usize = 6;
        let mut latencies_ms = Vec::with_capacity(CONCURRENCY * ROUNDS);

        for _ in 0..ROUNDS {
            let mut handles = Vec::with_capacity(CONCURRENCY);
            for _ in 0..CONCURRENCY {
                let url = url.clone();
                let client = client.clone();
                let body = build_batch_body(1);
                handles.push(tokio::spawn(async move {
                    let started = Instant::now();
                    let _ = client
                        .post(format!("{url}/v1/ingest"))
                        .json(&body)
                        .send()
                        .await
                        .unwrap();
                    started.elapsed().as_secs_f64() * 1000.0
                }));
            }
            for h in handles {
                latencies_ms.push(h.await.unwrap());
            }
        }
        latencies_ms
    }

    let mut durable =
        one_concurrent_pass(|root| Arc::new(LocalFsStore::new(root).unwrap())).await;
    let mut fast =
        one_concurrent_pass(|root| Arc::new(LocalFsStore::new_unsafe_fast(root).unwrap())).await;

    let dp50 = p_quantile(&mut durable, 0.50);
    let dp95 = p_quantile(&mut durable, 0.95);
    let fp50 = p_quantile(&mut fast, 0.50);
    let fp95 = p_quantile(&mut fast, 0.95);

    println!(
        "\n write_flush 100×100KB × 16 concurrent\n  durable=true (group-commit)  p50={dp50:.1} ms  p95={dp95:.1} ms"
    );
    println!(
        "  durable=false                p50={fp50:.1} ms  p95={fp95:.1} ms\n  ratio                        p50={:.2}×  p95={:.2}×",
        dp50 / fp50,
        dp95 / fp95
    );
}
