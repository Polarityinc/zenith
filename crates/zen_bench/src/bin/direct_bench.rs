//! Direct in-process benchmark — calls `execute_full()` without HTTP overhead.
//! This is the apples-to-apples comparison vs DuckDB (also in-process).
//!
//! Run after data is loaded:
//!   ./target/release/zen_direct_bench
//!
//! Env:
//!   ZEN_ITERS=200
//!   ZEN_MODE=hot|cold (default hot)

use std::sync::Arc;
use std::time::{Duration, Instant};

use zen_common::Config;
use zen_query::{SegmentCache, SegmentListCache};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::load_from_path("examples/zenithdb.dev.toml")?;
    let store = zen_storage::open_blob_store(&cfg).await?;
    let catalog = zen_catalog::open_catalog(&cfg).await?;

    let mode = std::env::var("ZEN_MODE").unwrap_or_else(|_| "hot".into());
    let iters: usize = std::env::var("ZEN_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);

    let segs = catalog
        .list_segments_for_tenant(zen_common::TenantId(0))
        .await?;
    let total_rows: i64 = segs.iter().map(|s| s.row_count).sum();
    let total_bytes: i64 = segs.iter().map(|s| s.byte_count).sum();
    println!(
        "# segments: {}, total rows: {}, total bytes: {:.1} MB",
        segs.len(),
        total_rows,
        total_bytes as f64 / 1_048_576.0,
    );

    // Look up a real trace_id so trace_load is a hit.
    let first = segs
        .first()
        .ok_or_else(|| anyhow::anyhow!("no segments — run setup_multi_segment.sh first"))?;
    let bytes = store.get(&first.object_key).await?;
    let reader = zen_format::SegmentReader::from_bytes(bytes.to_vec())?;
    let trace_id_str = {
        let cv = reader.read_column(0, 2)?;
        if let zen_format::ColumnValues::Fixed16(v) = cv {
            ulid::Ulid::from(u128::from_be_bytes(v[v.len() / 2])).to_string()
        } else {
            return Err(anyhow::anyhow!("trace_id col not Fixed16"));
        }
    };

    let queries: Vec<(&str, String)> = vec![
        (
            "B1_trace_load",
            format!("SELECT span_id, model FROM spans WHERE trace_id = '{trace_id_str}'"),
        ),
        (
            "B2_attr_filter",
            "SELECT span_id, model, duration_ms FROM spans WHERE model = 'gpt-4o' AND status = 'error' LIMIT 100".into(),
        ),
        (
            "B3_fts_memory",
            "SELECT span_id, prompt FROM spans WHERE text_match(prompt, 'memory') LIMIT 100".into(),
        ),
        (
            "B6_jsonpath",
            "SELECT span_id FROM spans WHERE metadata.tier = 'primary' LIMIT 100".into(),
        ),
        (
            "B8_group_by_model",
            "SELECT model, count(*) FROM spans GROUP BY model".into(),
        ),
    ];

    println!("\n## Mode: {mode} | iters: {iters}\n");
    println!(
        "{:24}  {:>10}  {:>10}  {:>10}",
        "query", "p50 µs", "p95 µs", "p99 µs"
    );
    println!("{}", "-".repeat(60));
    for (name, sql) in queries {
        let plan = zen_ql::parse(&sql, 0)?;
        let mut times = Vec::with_capacity(iters);

        let seg_cache = Arc::new(SegmentCache::new(128));
        let list_cache = Arc::new(if mode == "cold" {
            SegmentListCache::new(Duration::from_millis(0), 1024)
        } else {
            SegmentListCache::default()
        });

        for _ in 0..3 {
            zen_query::execute_full(
                &plan,
                catalog.clone(),
                store.clone(),
                seg_cache.as_ref(),
                list_cache.as_ref(),
            )
            .await?;
        }
        for _ in 0..iters {
            if mode == "cold" {
                let sc = SegmentCache::new(128);
                let lc = SegmentListCache::new(Duration::from_millis(0), 1024);
                let t0 = Instant::now();
                let _ = zen_query::execute_full(&plan, catalog.clone(), store.clone(), &sc, &lc)
                    .await?;
                times.push(t0.elapsed().as_micros() as f64);
            } else {
                let t0 = Instant::now();
                let _ = zen_query::execute_full(
                    &plan,
                    catalog.clone(),
                    store.clone(),
                    seg_cache.as_ref(),
                    list_cache.as_ref(),
                )
                .await?;
                times.push(t0.elapsed().as_micros() as f64);
            }
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = times[times.len() / 2];
        let p95 = times[(times.len() * 95) / 100];
        let p99 = times[(times.len() * 99) / 100];
        println!("{name:24}  {p50:>10.0}  {p95:>10.0}  {p99:>10.0}");
    }
    Ok(())
}
