# Changelog

All notable changes to ZenithDB. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
versioning: [SemVer](https://semver.org/).

## [Unreleased]

### Production-readiness sprint (2026-05-07)

#### Added

- **Auth (`zen_auth` crate)**: JWT (RS256 + JWKS) for customer traffic;
  HMAC-SHA256 for inter-node `/v1/internal/*`. Verified `Claims` injected
  into request extensions; per-token claims cache (16 K entries, 5 min TTL).
- **TLS termination**: `axum-server` + `rustls` + `aws-lc-rs` for AES-NI.
  Optional — falls back to plaintext when `server.tls.cert_path` is empty.
- **Observability**: `/v1/metrics` Prometheus endpoint with histograms
  (`zen_query_duration_seconds`, `zen_ingest_duration_seconds`, etc.) and
  counters (`zen_queries_total`, `zen_ingest_rows_total`). OTLP tracing
  exporter wired from `telemetry.otlp_endpoint` config.
- **Rate limiting**: per-tenant token bucket (default 100 QPS / 1000 burst);
  global concurrency cap (default 256).
- **Health probes**: `/v1/healthz` (liveness — process up) split from
  `/v1/readyz` (readiness — catalog reachable).
- **Graceful shutdown**: SIGTERM / SIGINT trigger memtable flush to WAL
  before exit.
- **Encryption (`zen_crypto` crate)**: AES-256-GCM envelope encryption
  with pluggable root key (static / KMS).
- **Backup / restore**: `zen admin-backup` and `zen admin-restore` CLIs
  serialize a tenant's segments + manifest to a directory and replay them
  against a fresh catalog.
- **CI/CD**: GitHub Actions for `fmt`, `clippy --D warnings`, `test`,
  `audit`, `deny`. Release pipeline builds + signs multi-arch Docker
  images on tag.
- **Helm hardening**: PodDisruptionBudget, HPA, NetworkPolicy,
  ServiceAccount with IRSA hooks, Ingress template.
- **Terraform hardening**: KMS key, SSE-KMS bucket, Secrets Manager for
  catalog DB credentials, multi-AZ RDS with deletion protection, IAM
  least-privilege policy.

#### Changed

- **WAL durability is ON by default** (was off via `ZEN_FS_DURABLE=1`).
  Opt out with `ZEN_UNSAFE_FAST=1` or `LocalFsStore::new_unsafe_fast` —
  reproducible-data scenarios only. Measured cost: +21% p95 on
  write_flush 100×100 KB (17.5 ms → 21.2 ms), still 5–15× faster than
  Postgres / DuckDB at the same workload.
- **`put` now actually fsyncs** when `durable=true` (previous bug: only
  `put_if_absent` honored the flag, so segments written by the compactor
  were never durabilized).
- Auth-off mode now logs a loud warning at boot.

#### Fixed

- 83 → 0 clippy warnings; CI now enforces `-D warnings`.

## [0.1.0] — 2026-05-04

Initial build:

- 18-crate Rust workspace (~13 K LOC).
- PAX columnar segments with FSST + ZSTD + Gorilla XOR + FoR + RLE + dict.
- Tantivy-as-library FTS, embedded inline in segments.
- HNSW vectors, JSON-path indexing, roaring bitmap posting lists.
- Tokio-only async runtime.
- HTTP / gRPC / OTLP endpoints.
- ZenithQL + SQL frontends.
- 3-node clustering via rendezvous-hash sharding (`zen_cluster`).
