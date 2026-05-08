# Contributing to ZenithDB

Thanks for your interest in ZenithDB. This document covers how to set up a dev environment, the conventions we follow, and how to land a change.

For non-trivial work (new features, format changes, anything in the five "moat" crates listed below), please open an issue first so we can align on direction before you write code.

## Table of contents

- [Ground rules](#ground-rules)
- [Development setup](#development-setup)
- [Build, test, lint](#build-test-lint)
- [Where things live](#where-things-live)
- [Coding conventions](#coding-conventions)
- [Commit messages](#commit-messages)
- [Pull requests](#pull-requests)
- [Sign-off (DCO)](#sign-off-dco)
- [Reporting issues](#reporting-issues)
- [Security](#security)
- [License](#license)

## Ground rules

- Be excellent to each other. We expect respectful, professional interactions.
- Small, focused PRs land faster than large, sprawling ones.
- Discuss design *before* writing the code for non-trivial changes.
- Tests come with the change, not in a follow-up.
- Public APIs and on-disk formats are stability surfaces — treat changes to them with care and call them out explicitly in the PR.

## Development setup

### Prerequisites

- **Rust 1.87+** (stable). The toolchain is pinned in [`rust-toolchain.toml`](rust-toolchain.toml); `rustup` will pick it up.
- **`protoc`** 3.21+
  - macOS: `brew install protobuf`
  - Debian/Ubuntu: `sudo apt-get install -y protobuf-compiler`
- **Optional**: Docker + `docker compose` for the prod-like stack (Postgres + MinIO).
- **Optional**: `cargo-audit`, `cargo-deny`, `cargo-nextest` for parity with CI.

```bash
cargo install --locked cargo-audit cargo-deny cargo-nextest
```

### Clone and bootstrap

```bash
git clone https://github.com/polarity-cc/zenithdb.git
cd zenithdb
cargo build --workspace
```

The first build pulls and compiles a lot — expect 5–10 minutes on a clean machine. Subsequent builds are incremental.

### Run a dev server

```bash
cargo run --release -p zen_cli -- serve --config examples/zenithdb.dev.toml
```

Default profile uses SQLite catalog + local-FS object store under `./data/`. No external services required.

For a prod-like stack with Postgres + MinIO:

```bash
docker compose -f deploy/docker/docker-compose.dev.yml up -d
ZEN_PROFILE=prod-like cargo run --release -p zen_cli -- serve
```

## Build, test, lint

These four commands are what CI runs. Match them locally before pushing:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-features
cargo deny check
```

`cargo nextest run --workspace` is a faster substitute for `cargo test` if you have it installed.

### Benchmarking

The benchmark suite is a first-class part of the project. If your change touches the storage engine, query path, or compactor, please include a before/after run:

```bash
# Generate a synthetic corpus and load it.
cargo run --release -p zen_cli -- bench gen --rows 4000000 --output /tmp/corpus.bin
cargo run --release -p zen_cli -- bench load --input /tmp/corpus.bin --target http://localhost:8080

# Run the suite.
cargo run --release -p zen_cli -- bench run --suite all --warmup 30s --duration 60s \
  --output bench-results/$(date +%Y%m%d-%H%M%S).json

# Compare against the committed baseline.
cargo run --release -p zen_cli -- bench compare \
  --baseline bench-results/baseline.json \
  --candidate bench-results/$(date +%Y%m%d-%H%M%S).json
```

Don't update `bench-results/baseline.json` in your PR unless that's the explicit goal of the change — baseline updates are a separate, deliberate step.

### Fuzzing

The fuzz targets live in [`fuzz/`](fuzz). They use `cargo-fuzz`. If you change a parser, codec, or wire format, run the relevant target for a few minutes locally:

```bash
cd fuzz
cargo +nightly fuzz run <target> -- -max_total_time=300
```

## Where things live

The workspace is **18 crates** under [`crates/`](crates). Five contain the engine's defining work; everything else is supporting infrastructure.

**Moat crates** — change with care, expect close review:

| Crate            | Responsibility |
|------------------|----------------|
| `zen_format`     | PAX segment encoder/decoder; FSST/ZSTD/Gorilla/FoR/RLE/dict codecs; footer & offset directory layout. |
| `zen_compactor`  | Streaming k-way merge compactor; enforces row-group-level trace-locality. |
| `zen_query`      | Vectorized scan operator, late materialization, predicate pushdown. |
| `zen_fts`        | Tantivy embedded inline in segments. |
| `zen_wal`        | Object-storage WAL with conditional PUT, queryable on PUT-ack. |

**Supporting crates**: `zen_storage`, `zen_memtable`, `zen_catalog`, `zen_index`, `zen_jsonpath`, `zen_vector`, `zen_compress`, `zen_server`, `zen_cli`, `zen_cluster`, `zen_auth`, `zen_crypto`, `zen_proto`, `zen_ql`, `zen_bench`, `zen_common`.

**Other top-level directories**:

- `tests/` — `integration`, `perf`, `chaos` test suites.
- `fuzz/` — `cargo-fuzz` targets.
- `examples/` — runnable sample configs.
- `deploy/` — Docker, Helm, Terraform.
- `docs/` — operator-facing documentation.
- `proto/` — protobuf definitions.

## Coding conventions

- **Formatting**: `rustfmt` defaults. Run `cargo fmt --all` before committing.
- **Lints**: `cargo clippy --workspace --all-targets -- -D warnings` must pass. CI rejects new warnings.
- **Errors**: prefer `thiserror` for library crates, `anyhow` for binaries / tests. Don't `unwrap()` outside tests; use `expect("…")` with a real message if you must.
- **`unsafe`**: rare and well-justified. Every `unsafe` block needs a `// SAFETY:` comment explaining the invariants.
- **Public API**: anything `pub` in a library crate is part of the API surface. Add `#[doc]` for non-obvious behavior.
- **Comments**: explain *why*, not *what*. Don't leave commented-out code behind.
- **Dependencies**: don't add a new top-level dep without a clear reason; prefer features on existing deps. Update [`Cargo.toml`](Cargo.toml) workspace deps, not the leaf crate's `Cargo.toml`.
- **Async**: tokio only — no `async-std` / `smol` / threadpool drift.

## Commit messages

We follow a loose [Conventional Commits](https://www.conventionalcommits.org/) style. The first line is `<type>: <imperative summary>`, under 72 chars. Common types:

- `feat:` — new feature
- `fix:` — bug fix
- `perf:` — performance improvement (include numbers in the body)
- `refactor:` — non-behavior-changing restructure
- `docs:` — docs only
- `test:` — tests only
- `ci:` — CI/build only
- `chore:` — dep bumps, tooling

Examples from this repo's history:

```
perf: vectorized WAL predicate eval + simd-json ingest body parse
perf: streaming k-way merge compactor + ahash hot paths
prod: hardening sprint (auth, TLS, observability, durability, encryption, backup, CI, multi-node)
```

Body should explain *why* and any tradeoffs. Reference issues with `Refs #123` or `Fixes #123`.

## Pull requests

1. **Branch off `main`**. Branch names are loose but `<type>/<short-desc>` is encouraged (e.g. `perf/posting-fts-jsonpath-agg`, `fix/wal-fsync-put`).
2. **Keep it focused**. One logical change per PR. If review uncovers a second issue, file it as a follow-up rather than expanding the PR.
3. **Pass CI locally first** — `fmt`, `clippy`, `test`, `deny`. Catching these locally is much faster than the CI loop.
4. **Update `CHANGELOG.md`** under `## [Unreleased]` for any user-visible change.
5. **Update docs** — at minimum, public API doc-comments. Update `docs/RUNBOOK.md` if you change operator-facing behavior.
6. **Fill out the PR template**.
7. **Get a green review from a [CODEOWNER](.github/CODEOWNERS)** for the touched paths. The storage / format / WAL / compactor / auth crates have additional required reviewers.

We squash-merge by default. Keep your PR title in good shape because that's what ends up in `git log`.

### Pre-commit hooks

We don't ship a pre-commit hook, but if you want one, the simplest version is:

```bash
cat > .git/hooks/pre-commit <<'EOF'
#!/bin/sh
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
EOF
chmod +x .git/hooks/pre-commit
```

## Sign-off (DCO)

By contributing to ZenithDB you certify the [Developer Certificate of Origin](https://developercertificate.org/). Add a `Signed-off-by:` trailer to every commit:

```bash
git commit -s -m "fix: …"
```

This adds:

```
Signed-off-by: Your Name <you@example.com>
```

Use a real name and the email on your GitHub account.

## Reporting issues

For bugs and feature requests, please use [GitHub Issues](https://github.com/polarity-cc/zenithdb/issues) with the appropriate template. A good bug report includes:

- ZenithDB version (`zen --version`) and git SHA if building from source.
- OS / arch.
- Config (with secrets redacted).
- Minimal reproduction.
- Logs at `RUST_LOG=zen=debug` if relevant.
- What you expected vs. what happened.

For questions and design discussion, use [GitHub Discussions](https://github.com/polarity-cc/zenithdb/discussions).

## Security

**Do not file security issues as public GitHub issues.** See [SECURITY.md](SECURITY.md) for the disclosure process.

## License

By contributing to ZenithDB, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
