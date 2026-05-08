//! Prometheus `/v1/metrics` endpoint and metric-name registry.
//!
//! Uses the `metrics` crate (lock-free atomics, ~50 ns per observation)
//! rather than the older `prometheus` crate so the hot path doesn't pay
//! a mutex acquisition. The exporter renders text on demand when the
//! Prometheus scraper hits `/v1/metrics`.

use std::sync::OnceLock;

use axum::{extract::State, http::StatusCode};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use crate::state::ServerState;

/// Metric name constants — keep in one place so dashboards and code stay
/// in sync. All durations are in **seconds** per Prometheus convention.
pub mod names {
    pub const QUERY_DURATION: &str = "zen_query_duration_seconds";
    pub const INGEST_DURATION: &str = "zen_ingest_duration_seconds";
    pub const WAL_FLUSH_DURATION: &str = "zen_wal_flush_duration_seconds";
    pub const COMPACTION_DURATION: &str = "zen_compaction_duration_seconds";
    pub const QUERIES_TOTAL: &str = "zen_queries_total";
    pub const INGEST_ROWS_TOTAL: &str = "zen_ingest_rows_total";
    pub const SEGMENTS_ACTIVE: &str = "zen_segments_active";
    pub const WAL_LAG_BYTES: &str = "zen_wal_lag_bytes";
    pub const FSYNCS_TOTAL: &str = "zen_fsyncs_total";
}

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Initialize the global Prometheus recorder. Call this exactly once at
/// server startup, before any metric is emitted. Idempotent — subsequent
/// calls return the existing handle.
pub fn init() -> &'static PrometheusHandle {
    HANDLE.get_or_init(|| {
        let builder = PrometheusBuilder::new()
            // Buckets in seconds, tuned to our observed query/ingest range
            // (sub-millisecond to a few seconds). Coarser buckets keep the
            // exporter render cheap at scale.
            .set_buckets_for_metric(
                metrics_exporter_prometheus::Matcher::Suffix(
                    "_duration_seconds".to_string(),
                ),
                &[
                    0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25,
                    0.5, 1.0, 2.5, 5.0, 10.0,
                ],
            )
            .unwrap_or_else(|_| PrometheusBuilder::new());
        // install_recorder both registers the recorder and returns the
        // handle the /v1/metrics endpoint uses to render text. We don't
        // use the optional HTTP listener feature — our axum router serves
        // the endpoint directly.
        builder
            .install_recorder()
            .expect("failed to install prometheus recorder")
    })
}

/// `GET /v1/metrics` — Prometheus text exposition format.
///
/// Lazily initializes the recorder on first scrape so the binary that
/// hosts the router doesn't have to remember to call [`init`] up-front
/// (the CLI does call it, but in-process integration tests forget).
pub async fn handle_metrics(State(_state): State<ServerState>) -> Result<String, (StatusCode, String)> {
    Ok(init().render())
}
