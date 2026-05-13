//! Deterministic in-process query benchmark.
//!
//! This builds synthetic agent-trace segments directly in an in-memory
//! catalog/blob store, then runs the query executor without HTTP overhead.
//! It is intended for before/after engine work:
//!
//! ```bash
//! cargo run --release -p zen_bench --bin local_query_bench -- \
//!   --rows 200000 --segment-rows 50000 --iters 30 --mode both
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use chrono::Utc;
use zen_catalog::{
    model::{SegmentRow, WalObjectBounds, WalObjectRow},
    Catalog, MockCatalog,
};
use zen_common::{CommitId, PartitionId, Schema, SpanId, SpanRecord, TenantId, TraceId};
use zen_compactor::{build_segment_from_rows, BuildOptions};
use zen_index::sparse::{RowGroupKey, SparseRowGroupIndex};
use zen_memtable::flush_to_record_batch;
use zen_query::{SegmentCache, SegmentListCache};
use zen_storage::local_fs::InMemoryStore;
use zen_storage::BlobStore;
use zen_wal::WalWriter;

#[derive(Clone, Debug)]
struct Args {
    rows: usize,
    segment_rows: usize,
    row_group_rows: u32,
    wal_objects: usize,
    wal_rows: usize,
    iters: usize,
    mode: String,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            rows: 100_000,
            segment_rows: 50_000,
            row_group_rows: 16_384,
            wal_objects: 0,
            wal_rows: 1_000,
            iters: 30,
            mode: "hot".into(),
        }
    }
}

#[derive(Clone)]
struct BuiltCorpus {
    catalog: Arc<dyn Catalog>,
    store: Arc<dyn BlobStore>,
    trace_id: String,
    time_lo: i64,
    time_hi: i64,
    wal_time_lo: Option<i64>,
    wal_time_hi: Option<i64>,
    segments: usize,
    wal_objects: usize,
    bytes: usize,
    wal_bytes: usize,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let args = parse_args()?;
    let build_start = Instant::now();
    let corpus = build_corpus(&args).await?;

    println!(
        "# rows={} segments={} bytes_mb={:.1} wal_objects={} wal_mb={:.1} build_ms={} row_group_rows={} segment_rows={}",
        args.rows,
        corpus.segments,
        corpus.bytes as f64 / 1_048_576.0,
        corpus.wal_objects,
        corpus.wal_bytes as f64 / 1_048_576.0,
        build_start.elapsed().as_millis(),
        args.row_group_rows,
        args.segment_rows,
    );

    let mut modes = Vec::new();
    match args.mode.as_str() {
        "hot" | "cold" => modes.push(args.mode.as_str()),
        "both" => {
            modes.push("hot");
            modes.push("cold");
        }
        other => anyhow::bail!("unsupported --mode {other}; expected hot, cold, or both"),
    }

    for mode in modes {
        run_suite(&args, &corpus, mode).await?;
    }
    Ok(())
}

fn parse_args() -> anyhow::Result<Args> {
    let mut args = Args::default();
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let value = it
            .next()
            .ok_or_else(|| anyhow::anyhow!("{flag} needs a value"))?;
        match flag.as_str() {
            "--rows" => args.rows = value.parse()?,
            "--segment-rows" => args.segment_rows = value.parse()?,
            "--row-group-rows" => args.row_group_rows = value.parse()?,
            "--wal-objects" => args.wal_objects = value.parse()?,
            "--wal-rows" => args.wal_rows = value.parse()?,
            "--iters" => args.iters = value.parse()?,
            "--mode" => args.mode = value,
            other => anyhow::bail!("unknown argument {other}"),
        }
    }
    if args.rows == 0 {
        anyhow::bail!("--rows must be > 0");
    }
    if args.segment_rows == 0 {
        anyhow::bail!("--segment-rows must be > 0");
    }
    if args.row_group_rows == 0 {
        anyhow::bail!("--row-group-rows must be > 0");
    }
    if args.wal_objects > 0 && args.wal_rows == 0 {
        anyhow::bail!("--wal-rows must be > 0 when --wal-objects is set");
    }
    Ok(args)
}

async fn build_corpus(args: &Args) -> anyhow::Result<BuiltCorpus> {
    let tenant = TenantId(0);
    let partition = PartitionId(0);
    let schema = Schema::spans_v1();
    let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
    let catalog: Arc<dyn Catalog> = Arc::new(Ok::<_, zen_common::ZenError>(MockCatalog::new())?);
    catalog.ensure_tenant(tenant, "bench").await?;
    catalog.ensure_partition(tenant, partition).await?;

    let rows = generate_rows(args.rows);
    let trace_id = rows[rows.len() / 2].trace_id.to_string();
    let time_lo = rows[rows.len() / 2].start_time_ms;
    let time_hi = time_lo + 10_000;
    let build_opts = BuildOptions {
        row_group_max_rows: args.row_group_rows,
        row_group_max_bytes: 64 * 1024 * 1024,
    };

    let mut total_bytes = 0usize;
    let mut segments = 0usize;
    for (idx, chunk) in rows.chunks(args.segment_rows).enumerate() {
        let (bytes, _) = build_segment_from_rows(chunk, tenant, partition, &schema, &build_opts)?;
        let reader = zen_format::SegmentReader::from_bytes(bytes.clone())?;
        let meta = reader.metadata.clone();
        let rowgroup_index = sparse_rowgroup_index(&reader)?;
        let key = format!("bench/segment-{idx:06}.zseg");
        let segment_byte_count = bytes.len();
        total_bytes += segment_byte_count;
        store.put(&key, Bytes::from(bytes)).await?;
        catalog
            .register_segment(SegmentRow {
                segment_id: uuid::Uuid::from_u128(meta.segment_id),
                tenant_id: tenant,
                partition_id: partition,
                object_key: key,
                level: 1,
                byte_count: segment_byte_count as i64,
                row_count: meta.row_count as i64,
                time_min: meta.time_min_ms,
                time_max: meta.time_max_ms,
                trace_id_min: meta.trace_id_min,
                trace_id_max: meta.trace_id_max,
                commit_id_min: meta.commit_id_min,
                commit_id_max: meta.commit_id_max,
                schema_fingerprint: meta.schema_fingerprint,
                rowgroup_index,
                superseded_at: None,
                created_at: Utc::now(),
            })
            .await?;
        segments += 1;
    }

    let mut wal_bytes = 0usize;
    let mut wal_time_lo = None;
    let mut wal_time_hi = None;
    if args.wal_objects > 0 {
        let writer = WalWriter::new(store.clone());
        let wal_rows_total = args.wal_objects.saturating_mul(args.wal_rows);
        let wal_rows = generate_rows_from(
            wal_rows_total,
            args.rows as u128 + 1,
            1_800_000_000_000,
            args.rows as u64 + 1,
        );
        if !wal_rows.is_empty() {
            wal_time_lo = Some(wal_rows[wal_rows.len() / 2].start_time_ms);
            wal_time_hi = wal_time_lo.map(|lo| lo + 10_000);
        }
        for chunk in wal_rows.chunks(args.wal_rows) {
            let batch = flush_to_record_batch(chunk)?;
            let commit_id = chunk.first().map(|r| r.commit_id).unwrap_or(CommitId(0));
            let bounds = WalObjectBounds::from_span_records(chunk);
            let key = writer
                .flush(tenant, partition, commit_id, schema.fingerprint(), &batch)
                .await?;
            let object_bytes = store.get(&key.to_string()).await?.len();
            wal_bytes += object_bytes;
            catalog
                .register_wal_object(WalObjectRow {
                    wal_id: uuid::Uuid::from_u128(ulid::Ulid::new().0),
                    tenant_id: tenant,
                    partition_id: partition,
                    object_key: key.to_string(),
                    commit_id_min: chunk.first().map(|r| r.commit_id).unwrap_or(commit_id),
                    commit_id_max: chunk.last().map(|r| r.commit_id).unwrap_or(commit_id),
                    byte_count: object_bytes as i64,
                    row_count: chunk.len() as i64,
                    time_min: bounds.time_min,
                    time_max: bounds.time_max,
                    trace_id_min: bounds.trace_id_min,
                    trace_id_max: bounds.trace_id_max,
                    schema_fingerprint: schema.fingerprint(),
                    consumed_at: None,
                    created_at: Utc::now(),
                })
                .await?;
        }
    }

    Ok(BuiltCorpus {
        catalog,
        store,
        trace_id,
        time_lo,
        time_hi,
        wal_time_lo,
        wal_time_hi,
        segments,
        wal_objects: args.wal_objects,
        bytes: total_bytes,
        wal_bytes,
    })
}

fn sparse_rowgroup_index(reader: &zen_format::SegmentReader) -> anyhow::Result<Vec<u8>> {
    let mut sparse = SparseRowGroupIndex::new();
    for rg_hc in &reader.hotcache.row_groups {
        let trace_zm = rg_hc.columns.iter().find(|c| {
            reader
                .metadata
                .column_names
                .get(c.column_idx as usize)
                .is_some_and(|name| name == "trace_id")
        });
        let time_zm = rg_hc.columns.iter().find(|c| {
            reader
                .metadata
                .column_names
                .get(c.column_idx as usize)
                .is_some_and(|name| name == "start_time_ms")
        });
        let commit_zm = rg_hc.columns.iter().find(|c| {
            reader
                .metadata
                .column_names
                .get(c.column_idx as usize)
                .is_some_and(|name| name == "commit_id")
        });

        let (min_tid, max_tid) = match trace_zm.map(|c| &c.zone_map.value) {
            Some(zen_index::ZoneMapValue::Fixed { min, max })
            | Some(zen_index::ZoneMapValue::Bytes { min, max }) => {
                let mut mn = [0u8; 16];
                let mut mx = [0u8; 16];
                let lmin = min.len().min(16);
                let lmax = max.len().min(16);
                mn[..lmin].copy_from_slice(&min[..lmin]);
                mx[..lmax].copy_from_slice(&max[..lmax]);
                (mn, mx)
            }
            _ => ([0u8; 16], [0xffu8; 16]),
        };
        let (min_t, max_t) = match time_zm.map(|c| &c.zone_map.value) {
            Some(zen_index::ZoneMapValue::I64 { min, max }) => (*min, *max),
            _ => (i64::MIN, i64::MAX),
        };
        let (min_c, max_c) = match commit_zm.map(|c| &c.zone_map.value) {
            Some(zen_index::ZoneMapValue::I64 { min, max }) => (*min as u64, *max as u64),
            _ => (0, u64::MAX),
        };
        sparse.push(RowGroupKey {
            min_trace_id: min_tid,
            max_trace_id: max_tid,
            min_start_time: min_t,
            max_start_time: max_t,
            min_commit_id: min_c,
            max_commit_id: max_c,
        });
    }
    Ok(sparse.serialize()?.to_vec())
}

fn generate_rows(target_rows: usize) -> Vec<SpanRecord> {
    generate_rows_from(target_rows, 1, 1_700_000_000_000, 1)
}

fn generate_rows_from(
    target_rows: usize,
    first_trace_no: u128,
    base_time: i64,
    first_commit_id: u64,
) -> Vec<SpanRecord> {
    const MODELS: [&str; 8] = [
        "gpt-4o",
        "claude-sonnet-4-7",
        "gpt-5-mini",
        "haiku-4-5",
        "o4-mini",
        "gemini-pro",
        "llama-3-70b",
        "mistral-large",
    ];
    const PROMPTS: [&str; 8] = [
        "Summarize the following conversation in 2-3 sentences",
        "What is the time complexity of this algorithm?",
        "Generate a SQL query that selects the top 10 customers by revenue",
        "Out of memory error in retrieval cache during compaction",
        "Rate limit exceeded for tier free; please upgrade your plan",
        "Explain the difference between mutexes and rwlocks in Rust",
        "Search for recent papers about retrieval-augmented generation",
        "Analyze the user behaviour log and identify churn signals",
    ];
    const COMPLETIONS: [&str; 5] = [
        "The request can be answered by checking the relevant trace fields.",
        "The complexity is O(n log n) due to the sort operation.",
        "SELECT customer_id, SUM(revenue) FROM orders GROUP BY customer_id",
        "The worker exhausted its memory limit while building the segment.",
        "A mutex grants exclusive access while a rwlock allows multiple readers.",
    ];

    let mut rows = Vec::with_capacity(target_rows);
    let mut trace_no = first_trace_no;
    while rows.len() < target_rows {
        let trace_id = TraceId::from_u128(trace_no);
        let spans_in_trace = 8 + (trace_no as usize % 9);
        for span_no in 0..spans_in_trace {
            if rows.len() >= target_rows {
                break;
            }
            let row_no = rows.len();
            let span_id = SpanId::from_u128(((trace_no + 1) << 32) | span_no as u128);
            let mut row = SpanRecord::new(TenantId(0), PartitionId(0));
            row.trace_id = trace_id;
            row.span_id = span_id;
            row.parent_span_id = if span_no == 0 {
                None
            } else {
                Some(SpanId::from_u128(
                    ((trace_no + 1) << 32) | (span_no - 1) as u128,
                ))
            };
            row.start_time_ms = base_time + trace_no as i64 * 1_000 + span_no as i64 * 100;
            row.duration_ms = 25 + (row_no % 2_000) as i64;
            row.end_time_ms = row.start_time_ms + row.duration_ms;
            row.model = Some(weighted_model(row_no, &MODELS).to_string());
            row.status = Some(if row_no % 25 == 0 { "error" } else { "ok" }.to_string());
            row.provider = Some(
                if row_no % 11 == 0 {
                    "anthropic"
                } else {
                    "openai"
                }
                .to_string(),
            );
            row.span_type = Some(
                match row_no % 10 {
                    0 => "agent_step",
                    1..=4 => "llm_call",
                    5..=8 => "tool_call",
                    _ => "retrieval",
                }
                .to_string(),
            );
            row.tool_name = Some(format!("tool-{:02}", row_no % 50));
            row.prompt = Some(PROMPTS[row_no % PROMPTS.len()].to_string());
            row.completion = Some(COMPLETIONS[row_no % COMPLETIONS.len()].to_string());
            row.prompt_tokens = Some(20 + (row_no % 2_000) as u32);
            row.completion_tokens = Some(15 + (row_no % 1_500) as u32);
            row.cost_usd = Some((row_no % 500) as f64 / 100_000.0);
            row.temperature = Some(((row_no % 100) as f64) / 100.0);
            row.top_p = Some(0.7 + ((row_no % 30) as f64) / 100.0);
            row.user_id = Some(format!("u-{}", row_no % 1_000));
            row.session_id = Some(format!("s-{}", trace_no % 10_000));
            row.request_id = Some(format!("r-{row_no}"));
            row.tool_io_text = if row_no % 7 == 0 {
                Some(format!("tool output row {row_no}"))
            } else {
                None
            };
            row.metadata = Some(serde_json::json!({
                "tier": if row_no % 10 == 0 { "secondary" } else { "primary" },
                "user_id": format!("u-{}", row_no % 1000),
                "request_id": format!("r-{row_no}"),
                "output": {
                    "steps": [
                        {"name": if row_no % 2 == 0 { "router" } else { "summarize" }}
                    ]
                }
            }));
            row.commit_id = CommitId(first_commit_id + row_no as u64);
            rows.push(row);
        }
        trace_no += 1;
    }
    rows.sort_by(|a, b| {
        a.trace_id
            .0
            .cmp(&b.trace_id.0)
            .then_with(|| a.start_time_ms.cmp(&b.start_time_ms))
            .then_with(|| a.span_id.0.cmp(&b.span_id.0))
    });
    rows
}

fn weighted_model<'a>(row_no: usize, models: &'a [&str]) -> &'a str {
    match row_no % 100 {
        0..=49 => models[0],
        50..=69 => models[1],
        70..=79 => models[2],
        80..=87 => models[3],
        88..=92 => models[4],
        93..=96 => models[5],
        97..=98 => models[6],
        _ => models[7],
    }
}

async fn run_suite(args: &Args, corpus: &BuiltCorpus, mode: &str) -> anyhow::Result<()> {
    let mut queries: Vec<(&str, String)> = vec![
        (
            "B1_trace_load",
            format!(
                "SELECT span_id, model FROM spans WHERE trace_id = '{}'",
                corpus.trace_id
            ),
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
            "B4_time_slice",
            format!(
                "SELECT span_id, start_time_ms FROM spans WHERE start_time_ms >= {} AND start_time_ms < {} LIMIT 100",
                corpus.time_lo, corpus.time_hi
            ),
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
    if let (Some(lo), Some(hi)) = (corpus.wal_time_lo, corpus.wal_time_hi) {
        queries.push((
            "B5_wal_time_slice",
            format!(
                "SELECT span_id, start_time_ms FROM spans WHERE start_time_ms >= {lo} AND start_time_ms < {hi} LIMIT 100"
            ),
        ));
    }

    println!("\n## mode={mode} iters={}\n", args.iters);
    println!(
        "{:22} {:>10} {:>10} {:>10} {:>10}",
        "query", "p50_us", "p95_us", "p99_us", "rows"
    );
    println!("{}", "-".repeat(70));
    for (name, sql) in queries {
        let plan = zen_ql::parse(&sql, 0)?;
        let seg_cache = SegmentCache::new(512);
        let list_cache = SegmentListCache::default();
        for _ in 0..3 {
            let _ = zen_query::execute_full(
                &plan,
                corpus.catalog.clone(),
                corpus.store.clone(),
                &seg_cache,
                &list_cache,
            )
            .await?;
        }

        let mut times = Vec::with_capacity(args.iters);
        let mut row_count = 0usize;
        for _ in 0..args.iters {
            let t0 = Instant::now();
            let rs = if mode == "cold" {
                let sc = SegmentCache::new(512);
                let lc = SegmentListCache::new(Duration::from_millis(0), 1024);
                zen_query::execute_full(
                    &plan,
                    corpus.catalog.clone(),
                    corpus.store.clone(),
                    &sc,
                    &lc,
                )
                .await?
            } else {
                zen_query::execute_full(
                    &plan,
                    corpus.catalog.clone(),
                    corpus.store.clone(),
                    &seg_cache,
                    &list_cache,
                )
                .await?
            };
            times.push(t0.elapsed().as_micros() as f64);
            row_count = rs.rows.len();
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = percentile(&times, 50);
        let p95 = percentile(&times, 95);
        let p99 = percentile(&times, 99);
        println!("{name:22} {p50:10.0} {p95:10.0} {p99:10.0} {row_count:10}");
    }
    Ok(())
}

fn percentile(sorted: &[f64], pct: usize) -> f64 {
    let idx = ((sorted.len() * pct) / 100).min(sorted.len().saturating_sub(1));
    sorted[idx]
}
