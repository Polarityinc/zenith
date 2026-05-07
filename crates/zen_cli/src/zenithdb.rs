//! `zenithdb` server entrypoint. Equivalent to `zen serve`.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(name = "zenithdb", about = "ZenithDB server")]
struct Args {
    #[arg(long, default_value = "examples/zenithdb.dev.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(false)
        .init();
    let args = Args::parse();
    let cfg = zen_common::Config::load_from_path(&args.config)
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
    http_result.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}
