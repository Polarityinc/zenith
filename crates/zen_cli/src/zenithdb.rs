//! `zenithdb` server entrypoint. Equivalent to `zen serve`.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(name = "zenithdb", about = "ZenithDB server")]
struct Args {
    #[arg(long, default_value = "examples/zenithdb.dev.toml")]
    config: PathBuf,
}

/// Build the global tracing subscriber. Always installs the `fmt` layer
/// (so logs go to stdout); additionally installs the OTLP layer when
/// `cfg.telemetry.otlp_endpoint` is non-empty. The OTLP exporter uses
/// the batching async pipeline so it never blocks request handlers.
fn install_tracing(cfg: &zen_common::Config) -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(cfg.telemetry.log_level.clone()));
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);

    if cfg.telemetry.otlp_endpoint.is_empty() {
        // No OTLP — install fmt only.
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();
        return Ok(());
    }

    // OTLP enabled — initialize a span exporter that ships to the
    // configured endpoint. Use batching so individual handler spans
    // don't pay per-span network cost.
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig;

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(cfg.telemetry.otlp_endpoint.clone())
        .build()
        .context("build OTLP span exporter")?;
    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_resource(opentelemetry_sdk::Resource::new(vec![
            opentelemetry::KeyValue::new("service.name", "zenithdb"),
        ]))
        .build();
    let tracer = provider.tracer("zenithdb");
    opentelemetry::global::set_tracer_provider(provider);
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_layer)
        .init();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cfg = zen_common::Config::load_from_path(&args.config)
        .context("failed to load config")?;
    install_tracing(&cfg)?;
    // Initialize the global Prometheus recorder so metric calls in handlers
    // have somewhere to record. Idempotent — safe across reload paths.
    let _ = zen_server::metrics::init();

    let store = zen_storage::open_blob_store(&cfg).await?;
    let catalog = zen_catalog::open_catalog(&cfg).await?;

    catalog
        .ensure_tenant(zen_common::TenantId(0), "default")
        .await?;
    catalog
        .ensure_partition(zen_common::TenantId(0), zen_common::PartitionId(0))
        .await?;

    let state = zen_server::ServerState::new(cfg.clone(), catalog, store);
    if cfg.auth.jwks_url.is_empty() {
        tracing::warn!("auth.jwks_url is empty — JWT verification is DISABLED. Do not run this configuration in production.");
    }
    if cfg.server.tls.cert_path.is_empty() {
        tracing::warn!("server.tls.cert_path is empty — listening over plain HTTP. Run behind a TLS-terminating load balancer in production.");
    }

    let http_addr = cfg.server.http_listen.clone();
    let grpc_addr = cfg.server.listen.clone();
    let state_for_grpc = state.clone();
    let grpc_task = tokio::spawn(async move {
        if let Err(e) = zen_server::grpc::serve(state_for_grpc, &grpc_addr).await {
            tracing::error!(error = %e, "grpc server failed");
        }
    });

    // Run the HTTP serve loop concurrent with a SIGTERM/SIGINT watcher.
    // On shutdown signal, flush memtables and stop accepting requests so
    // the cluster's load balancer pulls us out of rotation cleanly.
    let serve_fut = zen_server::http::serve(state.clone(), &http_addr);
    let shutdown_fut = shutdown_signal();
    let http_result = tokio::select! {
        r = serve_fut => r,
        () = shutdown_fut => {
            tracing::info!("shutdown signal received; flushing memtables and exiting");
            flush_all_memtables(&state).await;
            Ok(())
        }
    };
    grpc_task.abort();
    http_result.map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

/// Wait for either Ctrl-C or SIGTERM. Used to drive graceful shutdown.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let sigterm = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = sigterm => {},
    }
}

/// Best-effort flush of all in-memory memtables to WAL on shutdown. Each
/// memtable's contents become a normal WAL object so a subsequent boot
/// recovers the data via the existing WAL-replay path.
async fn flush_all_memtables(state: &zen_server::ServerState) {
    let snapshot: Vec<_> = state
        .memtables
        .read()
        .iter()
        .map(|((t, p), m)| (*t, *p, m.clone()))
        .collect();
    for (tenant, partition, mt) in snapshot {
        match mt.flush() {
            Ok(batch) if batch.num_rows() > 0 => {
                let writer = zen_wal::WalWriter::new(state.store.clone());
                let commit = match state.catalog.next_commit_id(tenant, partition).await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(error=%e, ?tenant, ?partition, "shutdown: next_commit_id failed");
                        continue;
                    }
                };
                if let Err(e) = writer
                    .flush(
                        tenant,
                        partition,
                        commit,
                        zen_common::Schema::spans_v1().fingerprint(),
                        &batch,
                    )
                    .await
                {
                    tracing::warn!(error=%e, ?tenant, ?partition, "shutdown: WAL flush failed");
                }
            }
            Ok(_) => {} // empty memtable
            Err(e) => {
                tracing::warn!(error=%e, ?tenant, ?partition, "shutdown: memtable flush failed");
            }
        }
    }
}
