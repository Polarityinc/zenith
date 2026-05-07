//! `zen` CLI: admin and benchmark driver.

use std::path::PathBuf;
use std::sync::Arc;
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
    }
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
