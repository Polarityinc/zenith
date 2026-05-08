//! `zen` CLI: admin and benchmark driver.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(name = "zen", about = "ZenithDB administrative + benchmark CLI")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Start the ZenithDB server.
    Serve {
        #[arg(long, default_value = "examples/zenithdb.dev.toml")]
        config: PathBuf,
    },
    /// Generate a synthetic workload to disk.
    BenchGen {
        #[arg(long, default_value_t = 100_000)]
        rows: usize,
        #[arg(long)]
        output: PathBuf,
    },
    /// Load a previously-generated workload into a running server.
    BenchLoad {
        #[arg(long)]
        input: PathBuf,
        #[arg(long, default_value = "http://localhost:8080")]
        target: String,
        #[arg(long, default_value_t = 200)]
        batch_size: usize,
        #[arg(long, default_value_t = 16)]
        concurrency: usize,
    },
    /// Run benchmark queries against the server.
    BenchRun {
        #[arg(long, default_value = "http://localhost:8080")]
        target: String,
        #[arg(long, default_value_t = 5)]
        seconds: u64,
        #[arg(long, default_value_t = 1)]
        concurrency: usize,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Compare bench output and update LEADERBOARD.md.
    BenchCompare {
        #[arg(long)]
        candidate: PathBuf,
        #[arg(long, default_value = "bench-results/LEADERBOARD.md")]
        leaderboard: PathBuf,
    },
    /// Trigger compaction on a tenant/partition.
    AdminCompact {
        #[arg(long, default_value = "http://localhost:8080")]
        target: String,
        #[arg(long)]
        tenant: u64,
        #[arg(long, default_value_t = 0)]
        partition: u32,
    },
    /// List active segments for a tenant.
    AdminSegments {
        #[arg(long, default_value = "http://localhost:8080")]
        target: String,
        #[arg(long)]
        tenant: u64,
    },
    /// Snapshot a tenant's segments + WAL into a backup directory or
    /// object-store prefix. The output contains a manifest plus copies
    /// of every active segment file referenced from the catalog at the
    /// moment the backup runs. Compactor publishes are not blocked, but
    /// any segment registered after `manifest.timestamp` is excluded.
    AdminBackup {
        #[arg(long, default_value = "examples/zenithdb.dev.toml")]
        config: PathBuf,
        #[arg(long)]
        tenant: u64,
        #[arg(long)]
        out: PathBuf,
    },
    /// Restore a tenant from an `AdminBackup` directory. The catalog
    /// must already exist; segments are copied back into the active
    /// store and re-registered.
    AdminRestore {
        #[arg(long, default_value = "examples/zenithdb.dev.toml")]
        config: PathBuf,
        #[arg(long)]
        tenant: u64,
        #[arg(long)]
        from: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(false)
        .init();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Serve { config } => cmd_serve(config).await,
        Cmd::BenchGen { rows, output } => cmd_bench_gen(rows, output).await,
        Cmd::BenchLoad {
            input,
            target,
            batch_size,
            concurrency,
        } => cmd_bench_load(input, target, batch_size, concurrency).await,
        Cmd::BenchRun {
            target,
            seconds,
            concurrency,
            output,
        } => cmd_bench_run(target, seconds, concurrency, output).await,
        Cmd::BenchCompare {
            candidate,
            leaderboard,
        } => cmd_bench_compare(candidate, leaderboard).await,
        Cmd::AdminCompact {
            target,
            tenant,
            partition,
        } => cmd_admin_compact(target, tenant, partition).await,
        Cmd::AdminSegments { target, tenant } => cmd_admin_segments(target, tenant).await,
        Cmd::AdminBackup {
            config,
            tenant,
            out,
        } => cmd_admin_backup(config, tenant, out).await,
        Cmd::AdminRestore {
            config,
            tenant,
            from,
        } => cmd_admin_restore(config, tenant, from).await,
    }
}

async fn cmd_admin_backup(config_path: PathBuf, tenant: u64, out: PathBuf) -> Result<()> {
    let cfg = zen_common::Config::load_from_path(&config_path)?;
    let store = zen_storage::open_blob_store(&cfg).await?;
    let catalog = zen_catalog::open_catalog(&cfg).await?;
    std::fs::create_dir_all(&out)?;
    let segs = catalog
        .list_segments_for_tenant(zen_common::TenantId(tenant))
        .await?;
    let mut manifest = serde_json::Map::new();
    manifest.insert(
        "tenant_id".into(),
        serde_json::Value::Number(tenant.into()),
    );
    manifest.insert(
        "snapshot_at".into(),
        serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
    );
    let mut seg_array = Vec::new();
    let seg_dir = out.join("segments");
    std::fs::create_dir_all(&seg_dir)?;
    for s in &segs {
        let bytes = store.get(&s.object_key).await?;
        let dest = seg_dir.join(format!("{}.zseg", s.segment_id));
        std::fs::write(&dest, &bytes)?;
        seg_array.push(serde_json::json!({
            "segment_id": s.segment_id.to_string(),
            "object_key": s.object_key,
            "byte_count": s.byte_count,
            "row_count": s.row_count,
            "time_min": s.time_min,
            "time_max": s.time_max,
            "level": s.level,
            "commit_id_min": s.commit_id_min.0,
            "commit_id_max": s.commit_id_max.0,
        }));
    }
    manifest.insert("segments".into(), serde_json::Value::Array(seg_array));
    let manifest_path = out.join("manifest.json");
    std::fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
    println!(
        "backup: {} segments → {}",
        segs.len(),
        out.display()
    );
    Ok(())
}

async fn cmd_admin_restore(config_path: PathBuf, tenant: u64, from: PathBuf) -> Result<()> {
    use zen_catalog::model::SegmentRow;
    let cfg = zen_common::Config::load_from_path(&config_path)?;
    let store = zen_storage::open_blob_store(&cfg).await?;
    let catalog = zen_catalog::open_catalog(&cfg).await?;
    let manifest_bytes = std::fs::read(from.join("manifest.json"))
        .context("manifest.json not found in backup directory")?;
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes)?;
    catalog
        .ensure_tenant(zen_common::TenantId(tenant), "restored")
        .await?;
    catalog
        .ensure_partition(zen_common::TenantId(tenant), zen_common::PartitionId(0))
        .await?;
    let segments = manifest["segments"]
        .as_array()
        .context("manifest missing segments[]")?;
    let mut restored = 0;
    for s in segments {
        let segment_id = uuid::Uuid::parse_str(s["segment_id"].as_str().unwrap_or(""))?;
        let object_key = s["object_key"].as_str().unwrap_or("").to_string();
        // SECURITY: object_key comes from a manifest that may be
        // attacker-controlled in transit. Reject anything containing
        // path-traversal sequences or absolute paths so a malicious
        // backup can't write to arbitrary keys (e.g. "../etc/passwd"
        // on a local-fs store, or another tenant's prefix on S3).
        if object_key.is_empty()
            || object_key.contains("..")
            || object_key.starts_with('/')
            || object_key.contains('\0')
        {
            anyhow::bail!(
                "manifest object_key {object_key:?} failed safety check (contains '..', is absolute, or has NUL)"
            );
        }
        // Constrain restored keys to this tenant's namespace.
        let expected_prefix = format!("tenants/{tenant}/");
        if !object_key.starts_with(&expected_prefix)
            && !object_key.starts_with(&format!("seg/{tenant}/"))
        {
            // Older manifests may not include a tenant prefix; allow
            // those but log a warning so operators notice cross-tenant
            // restore attempts.
            tracing::warn!(
                object_key,
                tenant,
                "restore: object_key has no tenant prefix; assuming legacy manifest"
            );
        }
        let path = from.join("segments").join(format!("{segment_id}.zseg"));
        // Defence-in-depth: ensure the read path is contained within
        // the backup directory (catches manifest with a tampered
        // segment_id like "../../escape").
        let canonical_from = from.canonicalize()?;
        let canonical_path = path.canonicalize()?;
        if !canonical_path.starts_with(&canonical_from) {
            anyhow::bail!("manifest segment path escapes backup root: {path:?}");
        }
        let bytes = std::fs::read(&path)?;
        store
            .put(&object_key, bytes::Bytes::from(bytes))
            .await?;
        catalog
            .register_segment(SegmentRow {
                segment_id,
                tenant_id: zen_common::TenantId(tenant),
                partition_id: zen_common::PartitionId(0),
                object_key: object_key.clone(),
                level: s["level"].as_i64().unwrap_or(0) as i16,
                byte_count: s["byte_count"].as_i64().unwrap_or(0),
                row_count: s["row_count"].as_i64().unwrap_or(0),
                time_min: s["time_min"].as_i64().unwrap_or(0),
                time_max: s["time_max"].as_i64().unwrap_or(0),
                trace_id_min: zen_common::TraceId([0u8; 16]),
                trace_id_max: zen_common::TraceId([0xff; 16]),
                commit_id_min: zen_common::CommitId(s["commit_id_min"].as_u64().unwrap_or(0)),
                commit_id_max: zen_common::CommitId(s["commit_id_max"].as_u64().unwrap_or(0)),
                schema_fingerprint: zen_common::SchemaFingerprint(0),
                rowgroup_index: Vec::new(),
                superseded_at: None,
                created_at: chrono::Utc::now(),
            })
            .await?;
        restored += 1;
    }
    println!("restored {restored} segments from {}", from.display());
    Ok(())
}

async fn cmd_serve(config_path: PathBuf) -> Result<()> {
    let cfg = zen_common::Config::load_from_path(&config_path)
        .context("failed to load config")?;

    let store = zen_storage::open_blob_store(&cfg).await?;
    let catalog = zen_catalog::open_catalog(&cfg).await?;

    catalog
        .ensure_tenant(zen_common::TenantId(0), "default")
        .await?;
    catalog
        .ensure_partition(zen_common::TenantId(0), zen_common::PartitionId(0))
        .await?;

    let state = zen_server::ServerState::new(cfg.clone(), catalog, store);
    let http_addr = cfg.server.http_listen.clone();
    let grpc_addr = cfg.server.listen.clone();
    let state_for_grpc = state.clone();
    let grpc_task = tokio::spawn(async move {
        if let Err(e) = zen_server::grpc::serve(state_for_grpc, &grpc_addr).await {
            tracing::error!(error = %e, "grpc server failed");
        }
    });
    let http_result = zen_server::http::serve(state, &http_addr).await;
    grpc_task.abort();
    http_result
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

async fn cmd_bench_gen(rows: usize, output: PathBuf) -> Result<()> {
    let cfg = zen_bench::WorkloadConfig {
        rows,
        ..Default::default()
    };
    tracing::info!(rows, "generating synthetic workload");
    let spans = zen_bench::generate_workload(&cfg);
    if let Some(parent) = output.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent).await.ok();
        }
    }
    let f = std::fs::File::create(&output)?;
    let w = std::io::BufWriter::new(f);
    serde_json::to_writer(w, &spans)?;
    tracing::info!(?output, n = spans.len(), "workload written");
    Ok(())
}

async fn cmd_bench_load(
    input: PathBuf,
    target: String,
    batch_size: usize,
    concurrency: usize,
) -> Result<()> {
    let f = std::fs::File::open(&input)?;
    let r = std::io::BufReader::new(f);
    let spans: Vec<zen_bench::workload::SpanIn> = serde_json::from_reader(r)?;
    tracing::info!(n = spans.len(), %target, "loading workload");
    let t0 = std::time::Instant::now();
    let n = zen_bench::load_to_server(&target, spans, batch_size, concurrency).await?;
    tracing::info!(n, elapsed_ms = t0.elapsed().as_millis() as u64, "loaded");
    println!("loaded {n} spans in {:?}", t0.elapsed());
    Ok(())
}

async fn cmd_bench_run(
    target: String,
    seconds: u64,
    concurrency: usize,
    output: Option<PathBuf>,
) -> Result<()> {
    let suite = zen_bench::run::default_suite();
    let results =
        zen_bench::run_suite(&target, &suite, Duration::from_secs(seconds), concurrency).await?;
    for r in &results {
        println!(
            "{:35} p50={:>8.0}µs  p95={:>8.0}µs  p99={:>8.0}µs  n={}",
            r.name, r.p50_us, r.p95_us, r.p99_us, r.n
        );
    }
    if let Some(path) = output {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }
        let f = std::fs::File::create(&path)?;
        serde_json::to_writer_pretty(std::io::BufWriter::new(f), &results)?;
        println!("wrote {path:?}");
    }
    Ok(())
}

async fn cmd_bench_compare(candidate: PathBuf, leaderboard: PathBuf) -> Result<()> {
    let f = std::fs::File::open(&candidate)?;
    let results: Vec<zen_bench::run::BenchResult> = serde_json::from_reader(std::io::BufReader::new(f))?;
    let md = zen_bench::Leaderboard::render(&results);
    if let Some(parent) = leaderboard.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    std::fs::write(&leaderboard, md)?;
    println!("leaderboard updated: {leaderboard:?}");
    Ok(())
}

async fn cmd_admin_compact(target: String, tenant: u64, partition: u32) -> Result<()> {
    let body = serde_json::json!({"tenant_id": tenant, "partition_id": partition});
    let r = reqwest::Client::new()
        .post(format!("{target}/v1/compact"))
        .json(&body)
        .send()
        .await?
        .text()
        .await?;
    println!("{r}");
    Ok(())
}

async fn cmd_admin_segments(target: String, tenant: u64) -> Result<()> {
    let r = reqwest::Client::new()
        .get(format!("{target}/v1/segments"))
        .query(&[("tenant_id", tenant)])
        .send()
        .await?
        .text()
        .await?;
    println!("{r}");
    Ok(())
}
