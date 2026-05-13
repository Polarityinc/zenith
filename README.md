<p align="center">
  <a href="https://zenith.polarity.so">
    <img src=".github/assets/zenith.png" alt="ZenithDB" width="100%" />
  </a>
</p>

<p align="center">
  <strong>The columnar database purpose-built for AI agent traces.</strong>
</p>

<p align="center">
  <a href="https://github.com/Polarityinc/zenith/actions/workflows/ci.yml"><img src="https://github.com/Polarityinc/zenith/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License: Apache 2.0" /></a>
  <a href="https://github.com/Polarityinc/zenith/blob/main/rust-toolchain.toml"><img src="https://img.shields.io/badge/rust-1.87%2B-orange.svg" alt="Rust 1.87+" /></a>
  <a href="CHANGELOG.md"><img src="https://img.shields.io/badge/status-alpha-yellow.svg" alt="Status: alpha" /></a>
  <a href="https://github.com/Polarityinc/zenith/issues"><img src="https://img.shields.io/github/issues/Polarityinc/zenith.svg" alt="Issues" /></a>
</p>

<p align="center">
  <a href="#quickstart">Quickstart</a> ·
  <a href="#architecture">Architecture</a> ·
  <a href="docs/RUNBOOK.md">Runbook</a> ·
  <a href="CONTRIBUTING.md">Contributing</a> ·
  <a href="CHANGELOG.md">Changelog</a>
</p>

---

## What is ZenithDB?

**ZenithDB** is an open source, columnar database engine purpose-built for the AI agent observability workload — long, sparse, high-cardinality JSON traces with rich text fields, late-arriving annotations, and bursty ingest. It is written in Rust, exposes HTTP / gRPC / OTLP endpoints, and speaks both SQL and ZenithQL.

Existing observability backends are built for short, structured spans and pay a 10–100× cost on this workload. ZenithDB's storage engine is built around five non-negotiable design choices that make AI traces cheap to store and fast to query:

1. **PAX segment format** sorted by `(trace_id, start_time, span_id)`, with per-row offset directories on wide string columns.
2. **Trace-locality at compaction time** — every span of a trace lands in one row group.
3. **Late materialization** in the scan operator — wide columns are never decoded for rows that didn't survive every other filter.
4. **Tantivy embedded inline** in segments, not a separate index.
5. **WAL on object storage** with conditional PUT, queryable on PUT-ack and merged with compacted segments at query time.

Everything else is supporting infrastructure.

## Quickstart

### Install (30-second start)

One line on macOS or Linux — the installer detects your OS + arch and drops the `zen` binary in `~/.local/bin`:

```bash
# macOS (Apple Silicon or Intel) and Linux (arm64 or amd64)
curl -fsSL https://raw.githubusercontent.com/Polarityinc/zenith/main/install.sh | sh

# pin a version, or change the install dir
curl -fsSL https://raw.githubusercontent.com/Polarityinc/zenith/main/install.sh \
  | VERSION=v0.1.0 INSTALL_DIR=/usr/local/bin sh
```

Then start a local server in seconds (in-memory catalog + local-FS object store, no Docker, no Postgres):

```bash
zen serve --config examples/zenithdb.dev.toml
# HTTP :8080  |  gRPC :50051  |  data under ./data/
```

### Build from source

If you'd rather build it yourself (or `install.sh` has no release for your platform yet):

- Rust **1.87+** (stable)
- `protoc` 3.21+ (`brew install protobuf` on macOS, `apt-get install protobuf-compiler` on Debian/Ubuntu)

```bash
git clone https://github.com/Polarityinc/zenith.git
cd zenith
cargo build --release

# Default profile: in-memory `MockCatalog` + local-FS object store. No Docker required.
cargo run --release -p zen_cli -- serve --config examples/zenithdb.dev.toml
```

The server listens on `:8080` (HTTP) and `:50051` (gRPC). Data lives under `./data/`.

### Ingest and query

```bash
# Ingest a trace
curl -s localhost:8080/v1/ingest -H 'content-type: application/json' -d '{
  "trace_id": "01HZ...",
  "spans": [{
    "span_id": "01HZ...A",
    "name": "agent.run",
    "start_time": "2026-05-07T12:00:00Z",
    "attributes": {"model": "claude-opus-4-7", "tokens": 4321}
  }]
}'

# Query with SQL
curl -s 'localhost:8080/v1/query' -H 'content-type: application/json' -d '{
  "sql": "SELECT model, count(*) FROM spans WHERE start_time > now() - 1h GROUP BY model"
}'
```

### Production-like local stack (Postgres + MinIO)

```bash
docker compose -f deploy/docker/docker-compose.dev.yml up -d
ZEN_PROFILE=prod-like cargo run --release -p zen_cli -- serve
```

## Architecture

```
Clients (SDKs, OTLP, REST, gRPC)
            │
            ▼
       Gateway (axum + tonic)
        │           │
   ingest         queries
        │           │
   Writer       Querier
   (memtable)   (planner + exec)
        │           │
        ▼           ▼
   Catalog (Postgres)
        │
        ▼
  Object storage (local-fs / S3 / GCS / Azure)
   - WAL    (.wal)
   - Segments (.zseg)
```

The workspace is 18 Rust crates under [`crates/`](crates). The five "moat" crates that contain the engine's defining work:

| Crate | What it does |
|-------|--------------|
| [`zen_format`](crates/zen_format)       | PAX segment encoder/decoder, FSST/ZSTD/Gorilla/FoR/RLE/dict codecs, footer & offset directory layout. |
| [`zen_compactor`](crates/zen_compactor) | Streaming k-way merge compactor that enforces trace-locality. |
| [`zen_query`](crates/zen_query)         | Vectorized scan operator, late materialization, predicate pushdown. |
| [`zen_fts`](crates/zen_fts)             | Tantivy-as-a-library embedded inline in segments. |
| [`zen_wal`](crates/zen_wal)             | Object-storage WAL with conditional PUT, queryable on ack. |

The remaining crates (`zen_storage`, `zen_memtable`, `zen_catalog`, `zen_index`, `zen_jsonpath`, `zen_vector`, `zen_compress`, `zen_server`, `zen_cli`, `zen_cluster`, `zen_auth`, `zen_crypto`, `zen_proto`, `zen_ql`, `zen_bench`, `zen_common`) are supporting infrastructure.

## Configuration

The default profile runs entirely on your laptop with no external services:

- **Catalog** — Postgres (production) or in-memory `MockCatalog` (dev/test, set in config)
- **Object store** — local filesystem at `./data/blobs/`
- **NVMe page cache** — in-process, default 4 GiB

See [`examples/zenithdb.dev.toml`](examples/zenithdb.dev.toml) for the full config surface, and [`docs/RUNBOOK.md`](docs/RUNBOOK.md) for production tuning.

## Console (web dashboard)

ZenithDB ships with a minimal Next.js console under [`web/`](web) that
talks to the engine's HTTP API. It exposes live segments, queries,
compactions, WAL metrics, and a query runner — no fixtures, all real
data.

```bash
cd web
bun install
bun dev   # http://localhost:3000
```

Configure the upstream via `ZENITH_URL` (default `http://localhost:8080`).

## Documentation

- **[Architecture](docs/ARCHITECTURE.md)** — deep dive on the five moat crates and the design choices.
- **[Runbook](docs/RUNBOOK.md)** — operator's guide.
- **[Examples](examples)** — `zenithdb.dev.toml` config + Python and TypeScript quickstarts.
- **[Changelog](CHANGELOG.md)** — what landed when.
- **[Contributing](CONTRIBUTING.md)** — dev setup, conventions, PR workflow.
- **[Security](SECURITY.md)** — vulnerability disclosure.
- **[Code of Conduct](CODE_OF_CONDUCT.md)** — community standards.

## Status

ZenithDB is **alpha**. The core engine is feature-complete and runs the full benchmark suite, but on-disk format and wire protocols may still change before `1.0`. Track the [CHANGELOG](CHANGELOG.md) for breaking changes.

## Community

- **Issues & feature requests** — [GitHub Issues](https://github.com/Polarityinc/zenith/issues)
- **Discussions** — [GitHub Discussions](https://github.com/Polarityinc/zenith/discussions)
- **Security disclosures** — see [SECURITY.md](SECURITY.md)
- **Contact** — [support@polarity.so](mailto:support@polarity.so)

## Contributing

We welcome contributions of all sizes — bug reports, docs, tests, and code. Start with [CONTRIBUTING.md](CONTRIBUTING.md). The codebase has an opinionated structure (see [Architecture](#architecture)); for non-trivial changes, open an issue first so we can align before you write code.

## License

ZenithDB is licensed under the [Apache License 2.0](LICENSE). Third-party notices in [NOTICE](NOTICE).

Copyright © 2026 [Polarity, Inc.](https://polarity.so)
