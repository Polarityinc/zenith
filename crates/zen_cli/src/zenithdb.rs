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
    let cfg = zen_common::Config::load_from_path(&args.config).context("failed to load config")?;
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
                let Some(metadata) = wal_metadata_from_batch(&batch) else {
                    tracing::warn!(
                        ?tenant,
                        ?partition,
                        "shutdown: WAL metadata extraction failed"
                    );
                    continue;
                };
                let (key, wal_bytes) = match writer
                    .flush_with_size(
                        tenant,
                        partition,
                        metadata.commit_id_min,
                        zen_common::Schema::spans_v1().fingerprint(),
                        &batch,
                    )
                    .await
                {
                    Ok(pair) => pair,
                    Err(e) => {
                        tracing::warn!(error=%e, ?tenant, ?partition, "shutdown: WAL flush failed");
                        continue;
                    }
                };
                let row = zen_catalog::model::WalObjectRow {
                    wal_id: uuid::Uuid::new_v4(),
                    tenant_id: tenant,
                    partition_id: partition,
                    object_key: key.to_string(),
                    commit_id_min: metadata.commit_id_min,
                    commit_id_max: metadata.commit_id_max,
                    byte_count: wal_bytes as i64,
                    row_count: batch.num_rows() as i64,
                    time_min: metadata.time_min,
                    time_max: metadata.time_max,
                    trace_id_min: metadata.trace_id_min,
                    trace_id_max: metadata.trace_id_max,
                    schema_fingerprint: zen_common::Schema::spans_v1().fingerprint(),
                    consumed_at: None,
                    created_at: chrono::Utc::now(),
                };
                if let Err(e) = state.catalog.register_wal_object(row).await {
                    tracing::warn!(error=%e, ?tenant, ?partition, "shutdown: WAL catalog register failed");
                }
            }
            Ok(_) => {} // empty memtable
            Err(e) => {
                tracing::warn!(error=%e, ?tenant, ?partition, "shutdown: memtable flush failed");
            }
        }
    }
}

struct WalBatchMetadata {
    commit_id_min: zen_common::CommitId,
    commit_id_max: zen_common::CommitId,
    time_min: i64,
    time_max: i64,
    trace_id_min: zen_common::TraceId,
    trace_id_max: zen_common::TraceId,
}

fn wal_metadata_from_batch(batch: &arrow_array::RecordBatch) -> Option<WalBatchMetadata> {
    use arrow_array::{Array, FixedSizeBinaryArray, Int64Array, UInt64Array};

    let schema = batch.schema();
    let commit_idx = schema.index_of("commit_id").ok()?;
    let commit = batch
        .column(commit_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()?;
    let mut commit_min = u64::MAX;
    let mut commit_max = 0u64;
    for i in 0..batch.num_rows() {
        if commit.is_null(i) {
            continue;
        }
        let c = commit.value(i);
        commit_min = commit_min.min(c);
        commit_max = commit_max.max(c);
    }
    if commit_min == u64::MAX {
        return None;
    }

    let mut time_min = i64::MIN;
    let mut time_max = i64::MAX;
    if let Ok(time_idx) = schema.index_of("start_time_ms") {
        if let Some(times) = batch.column(time_idx).as_any().downcast_ref::<Int64Array>() {
            let mut min_seen = i64::MAX;
            let mut max_seen = i64::MIN;
            for i in 0..batch.num_rows() {
                if times.is_null(i) {
                    continue;
                }
                let t = times.value(i);
                min_seen = min_seen.min(t);
                max_seen = max_seen.max(t);
            }
            if min_seen != i64::MAX {
                time_min = min_seen;
                time_max = max_seen;
            }
        }
    }

    let mut trace_id_min = zen_common::TraceId([0; 16]);
    let mut trace_id_max = zen_common::TraceId([0xff; 16]);
    if let Ok(trace_idx) = schema.index_of("trace_id") {
        if let Some(trace_ids) = batch
            .column(trace_idx)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
        {
            let mut min_seen = [0xffu8; 16];
            let mut max_seen = [0u8; 16];
            let mut saw_trace = false;
            for i in 0..batch.num_rows() {
                if trace_ids.is_null(i) {
                    continue;
                }
                let value = trace_ids.value(i);
                if value.len() != 16 {
                    continue;
                }
                let mut tid = [0u8; 16];
                tid.copy_from_slice(value);
                min_seen = min_seen.min(tid);
                max_seen = max_seen.max(tid);
                saw_trace = true;
            }
            if saw_trace {
                trace_id_min = zen_common::TraceId(min_seen);
                trace_id_max = zen_common::TraceId(max_seen);
            }
        }
    }

    Some(WalBatchMetadata {
        commit_id_min: zen_common::CommitId(commit_min),
        commit_id_max: zen_common::CommitId(commit_max),
        time_min,
        time_max,
        trace_id_min,
        trace_id_max,
    })
}
