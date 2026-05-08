# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

ZenithDB — a Rust columnar database engine purpose-built for AI agent traces. 18-crate workspace, one `cargo build`. Engine name `zenithdb`, CLI binary `zen`, all crates prefixed `zen_`.

## Toolchain

- Rust **1.87 stable**, pinned via `rust-toolchain.toml` (rustup will pick it up automatically).
- `protoc` 3.21+ is required to build (`brew install protobuf` / `apt-get install protobuf-compiler`). Several crates run `prost-build` / `tonic-build` in their `build.rs`.

## Common commands

CI runs four gates and rejects on any of them. Match them locally before pushing:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings   # -D warnings is enforced
cargo test --workspace --all-features
cargo deny check                                        # licenses + advisories
```

`cargo audit --deny warnings` runs in CI as a fifth gate.

### Running a single test

```bash
cargo test -p <crate> <test_name>                       # unit/integration test in a crate
cargo test -p zen_format encoder::tests::roundtrip      # by full path
cargo test -p integration auth::                        # the integration suite lives in tests/integration
cargo test -p integration -- --nocapture                # see println! / tracing output
```

The integration / perf / chaos suites are **separate crates** under `tests/` (not `cargo test` default targets — they're listed in the workspace `members`). Always pass `-p integration` / `-p perf` / `-p chaos` to target them.

### Run the server

```bash
# Default dev profile — SQLite catalog + local-FS object store under ./data/. Zero external services.
cargo run --release -p zen_cli -- serve --config examples/zenithdb.dev.toml
```

`zen_cli` ships **two binaries**: `zen` (the user-facing CLI with `serve`, `bench`, `admin-backup`, `admin-restore`, etc.) and `zenithdb` (server-only entrypoint). Both live in `crates/zen_cli/src/`.

For a Postgres + MinIO stack:

```bash
docker compose -f deploy/docker/docker-compose.dev.yml up -d
ZEN_PROFILE=prod-like cargo run --release -p zen_cli -- serve
```

### Benchmarks

The bench suite is first-class. If you touch storage, query, or compaction, run a before/after.

```bash
cargo run --release -p zen_cli -- bench gen --rows 4000000 --output /tmp/corpus.bin
cargo run --release -p zen_cli -- bench load --input /tmp/corpus.bin --target http://localhost:8080
cargo run --release -p zen_cli -- bench run --suite all --warmup 30s --duration 60s \
  --output bench-results/$(date +%Y%m%d-%H%M%S).json
cargo run --release -p zen_cli -- bench compare \
  --baseline bench-results/baseline.json \
  --candidate bench-results/<your-run>.json
```

`bench-results/baseline.json` is committed and is the reference. Only update it as a deliberate, separate change.

### Fuzzing

Targets in `fuzz/fuzz_targets/`: `fuzz_segment_parse`, `fuzz_wal_parse`, `fuzz_zenithql_parse`. Run with `cargo +nightly fuzz run <target>` from `fuzz/`. Re-run the relevant target whenever you change a parser, codec, or wire format.

## Architecture (the big picture)

### Request flow

```
Clients (REST / gRPC / OTLP)
        │
        ▼
   Gateway (axum + tonic)        — crates/zen_server
   ├── ingest  → Writer (memtable + WAL)        — zen_memtable, zen_wal
   └── query   → Querier (planner + executor)   — zen_query, zen_ql
                       │
                       ▼
              Catalog (sqlite / postgres)        — zen_catalog
                       │
                       ▼
              Object storage (fs / S3 / GCS / Azure) — zen_storage
              ├── WAL (.wal)
              └── Segments (.zseg)               — zen_format
```

### The five "moat" crates

These contain the engine's defining work. Changes here need close review and usually a CHANGELOG entry:

| Crate            | Responsibility |
|------------------|----------------|
| `zen_format`     | PAX segment encoder/decoder; FSST/ZSTD/Gorilla/FoR/RLE/dict codecs; segment footer + offset directory layout. **On-disk format = stability surface.** |
| `zen_compactor`  | Streaming k-way merge compactor. Enforces row-group-level **trace-locality**: every span of a trace lands in one row group. |
| `zen_query`      | Vectorized scan operator with **late materialization** — wide columns are never decoded for rows that didn't survive every other filter. Predicate pushdown lives here. |
| `zen_fts`        | Tantivy embedded **inline in segments** (not a sidecar index). |
| `zen_wal`        | WAL on object storage with conditional PUT, queryable on PUT-ack and merged with compacted segments at query time. |

### Supporting crates

`zen_storage` (object_store wrapper + NVMe page cache), `zen_memtable` (write path), `zen_catalog` (sqlx, sqlite default), `zen_index` (roaring bitmap posting lists), `zen_jsonpath` (sampled JSON-path indexing), `zen_vector` (HNSW), `zen_compress` (codec wrappers), `zen_server` (HTTP+gRPC+OTLP), `zen_cli` (`zen` + `zenithdb` binaries), `zen_cluster` (rendezvous-hash sharding), `zen_auth` (JWT customer + HMAC inter-node), `zen_crypto` (AES-256-GCM envelope encryption), `zen_proto` (protobuf), `zen_ql` (ZenithQL parser, chumsky-based), `zen_bench` (load gen + suite runner), `zen_common` (shared types).

### Design invariants worth knowing before editing

- **Segments are sorted by `(trace_id, start_time, span_id)`**. The compactor enforces this; the query path assumes it. Don't break this assumption when changing encoding, merging, or row-group construction.
- **Single async runtime**: tokio only. Don't introduce `async-std`, `smol`, or ad-hoc threadpools. Blocking work goes through `tokio::task::spawn_blocking`.
- **WAL fsync is ON by default**. `ZEN_UNSAFE_FAST=1` (or `LocalFsStore::new_unsafe_fast`) opts out — only valid for reproducible-data scenarios. Don't change the default.
- **Both `put` and `put_if_absent` must honor `durable=true`**. There was a bug where only `put_if_absent` did, so compactor-written segments weren't durabilized. Watch for regressions if you touch `LocalFsStore`.
- **The query planner can read the WAL directly** to give sync write visibility. Anything that affects WAL layout has to keep this readable.
- **Auth-off mode logs a loud warning at boot**, by design. Don't silence it.

## Workspace conventions

- **Workspace deps live in the root `Cargo.toml`** under `[workspace.dependencies]`. Leaf crates reference them as `foo = { workspace = true }`. When adding or upgrading a dep, edit the root, not the leaf.
- The Arrow ecosystem is pinned to a **single major version** across the workspace (currently `53`). Don't drift.
- **Release profile** uses `lto = "fat"`, `codegen-units = 1`, `panic = "abort"`, `strip = "symbols"`. Release builds are slow (5–10 min from scratch) — that's expected.
- **`#[allow(...)]` lint suppressions need a one-line comment** explaining why. CI runs `clippy --D warnings`; don't add new warnings.
- **Public API in any crate is part of the stability surface.** Anything `pub` at a crate boundary should have doc-comments for non-obvious behavior.
- **`unsafe` blocks require a `// SAFETY:` comment** spelling out the invariants.

## Tests, fuzz, perf

- **Unit tests** live next to the code they test (`#[cfg(test)] mod tests`).
- **Cross-crate integration tests** live in the `tests/integration` crate (`tests/integration/src/...`, registered in workspace members).
- **Perf microbenchmarks** in `tests/perf` — `criterion`-based.
- **Chaos tests** in `tests/chaos` — fault-injection / multi-node failure scenarios.
- **Fuzzing**: `fuzz/fuzz_targets/` — `cargo-fuzz` targets for parsers and on-disk formats.
- **End-to-end coverage** in `tests/integration/end_to_end.rs`; multi-node coverage in `multi_node.rs`; durability + auth + TLS each have their own files.

## Deploy / ops

- Helm chart: `deploy/helm/` (PDB, HPA, NetworkPolicy, ServiceAccount with IRSA hooks).
- Terraform: `deploy/terraform/` (KMS, SSE-KMS bucket, multi-AZ RDS, Secrets Manager, IAM least-privilege).
- Dockerfile: `deploy/docker/Dockerfile.zenithdb` (multi-arch, signed via cosign keyless OIDC on tag push).
- Operator's guide: `docs/RUNBOOK.md`. Sizing notes: `docs/SCALING_1TB_1PB.md`. Update these when changing operator-facing behavior.

## Things that change with care

When editing these, expect required CODEOWNER review (see `.github/CODEOWNERS`):

- `crates/zen_format` / `zen_wal` / `zen_compactor` — storage team. Changes to these often imply on-disk format migrations.
- `crates/zen_auth` / `.github/` — security team.
- `deploy/` — SRE team.

CHANGELOG entry under `## [Unreleased]` is expected for any user- or operator-visible change.
