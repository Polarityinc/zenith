# Zenith / ZenithDB

A custom AI-agent-trace database in Rust, purpose-built for the AI agent observability workload.

The engine name is `zenithdb`. CLI: `zen`. Crate prefix: `zen_`.

## Why

AI agent traces have a unique shape: long, sparse, high-cardinality JSON payloads with rich text, late-arriving annotations, and bursty ingest. Existing observability backends are built for short, structured spans and pay a 10-100x cost on this workload. Zenith's storage engine is built around five non-negotiable design choices that make the AI-trace workload cheap:

1. PAX segment format sorted by `(trace_id, start_time, span_id)`, with per-row offset directories on wide string columns.
2. The compactor enforces row-group-level trace-locality: every span of a trace lands in one row group.
3. Late materialization in the scan operator: never decode a wide column for a row that didn't survive every other filter.
4. Tantivy as a library, embedded inline in segments — not a separate index.
5. WAL on object storage with conditional PUT, queryable on PUT-ack and merged with compacted segments at query time.

Everything else is supporting infrastructure.

## Quick start (Mac)

Prerequisites:
- Rust 1.87+ (stable)
- protoc 3.21+ (`brew install protobuf`)

```bash
# Build
cargo build --release

# Test (all crates)
cargo test --workspace

# Lint
cargo clippy --workspace --all-targets -- -D warnings

# Run server with default dev config (sqlite catalog, local-fs object store)
cargo run --release -p zen_cli -- serve --config examples/zenithdb.dev.toml

# Generate a 4M-span synthetic corpus
cargo run --release -p zen_cli -- bench gen \
    --rows 4000000 \
    --output /tmp/zen-corpus.bin

# Load corpus into a running server
cargo run --release -p zen_cli -- bench load \
    --input /tmp/zen-corpus.bin \
    --target http://localhost:8080

# Run benchmark suite
cargo run --release -p zen_cli -- bench run \
    --suite all \
    --warmup 30s \
    --duration 60s \
    --output bench-results/$(date +%Y%m%d-%H%M%S).json

# Update leaderboard
cargo run --release -p zen_cli -- bench compare \
    --baseline bench-results/baseline.json \
    --candidate bench-results/$(date +%Y%m%d-%H%M%S).json \
    --update-leaderboard
```

## Production readiness

Zenith ships with the operational primitives needed to run on real
infrastructure. See [`docs/RUNBOOK.md`](docs/RUNBOOK.md) for the
operator's guide and [`CHANGELOG.md`](CHANGELOG.md) for what landed
when.

- **Auth**: JWT (RS256 + JWKS) on customer routes, HMAC on inter-node.
- **TLS**: optional `rustls` + `aws-lc-rs` termination, or run behind a
  TLS-terminating LB.
- **Observability**: `/v1/metrics` Prometheus endpoint + OTLP tracing.
- **Reliability**: WAL fsync ON by default, graceful shutdown,
  PodDisruptionBudget, multi-AZ catalog, snapshot/restore CLI.
- **Encryption at rest**: `zen_crypto` envelope encryption; pluggable
  KMS root key.
- **Rate limits**: per-tenant token bucket + global concurrency cap.
- **Multi-node**: rendezvous-hash sharded, transparent query routing,
  3-node integration test in CI.

## Architecture

```
Clients (SDKs, OTLP, REST)
            │
            ▼
       Gateway (axum/tonic)
        │           │
   ingest         queries
        │           │
   Writer       Querier
   (memtable)   (planner+exec)
        │           │
        ▼           ▼
   Catalog (sqlite/Postgres)
        │
        ▼
  Object Storage (local-fs / S3 / MinIO)
   - WAL (.wal)
   - Segments (.zseg)
```

## Default configuration profile

The default profile runs entirely on your laptop with no external services:
- **Catalog**: sqlite at `./data/zenith.db`.
- **Object store**: local filesystem at `./data/blobs/`.
- **NVMe page cache**: in-process, default 4 GB.

For "prod-like" testing, opt into Docker Compose:

```bash
docker compose -f deploy/docker/docker-compose.dev.yml up -d
```

This brings up Postgres 14 and MinIO. Set `ZEN_PROFILE=prod-like` and the server uses them.

## Repository layout

See `crates/` for the 18 crates that make up the engine. The five "moat" crates are `zen_format`, `zen_compactor`, `zen_query`, `zen_fts`, `zen_wal`. Everything else is supporting infrastructure.

## License

Apache 2.0.
