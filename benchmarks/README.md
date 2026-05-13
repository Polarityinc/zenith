# benchmarks/

Reproducible benchmark harnesses for ZenithDB.

| Script | Workload | Runtime |
|--------|----------|---------|
| [`bench_1b.sh`](bench_1b.sh) | 1,000,000,000 spans, chunked load + periodic compaction, then the canonical query suite (B2/B3/B6/B8). | 12-48h on a single host. |

## bench_1b.sh — 1 billion rows

The canonical large-scale benchmark. Generates synthetic AI-trace data in
chunks, loads each chunk via HTTP ingest, compacts every N chunks, and
finishes with the standard `zen bench-run` suite.

### Pre-flight

```bash
# Build the CLI
cargo build --release -p zen_cli

# Start a server in another shell
./target/release/zen serve --config examples/zenithdb.dev.toml
```

### Run

```bash
./benchmarks/bench_1b.sh
```

Defaults are tuned for a workstation with NVMe + ≥32 GiB RAM. Override
anything via env vars:

```bash
TOTAL_ROWS=1000000000 \
CHUNK_ROWS=10000000 \
CONCURRENCY=64 \
BATCH_SIZE=1000 \
COMPACT_EVERY=10 \
SUITE_SECONDS=120 \
./benchmarks/bench_1b.sh
```

### Output

Latencies land in `bench-results/1b-<timestamp>.json`:

```json
[
  {"name":"B2_time_range_attr_filter","p50_us":...,"p95_us":...,"p99_us":...,"n":...},
  {"name":"B3_fts_common_term",       "p50_us":...,"p95_us":...,"p99_us":...,"n":...},
  {"name":"B6_jsonpath_indexed",      "p50_us":...,"p95_us":...,"p99_us":...,"n":...},
  {"name":"B8_aggregation_by_model",  "p50_us":...,"p95_us":...,"p99_us":...,"n":...}
]
```

Compare against the committed baseline:

```bash
./target/release/zen bench-compare \
  --candidate bench-results/1b-<timestamp>.json \
  --leaderboard bench-results/LEADERBOARD.md
```

### What's actually being measured

- **Ingest throughput** (rows/sec, printed live during the load phase).
- **Compaction stability** — the script triggers `POST /v1/compact` every
  `COMPACT_EVERY` chunks; failures are logged but non-fatal.
- **Query latency at scale** — `bench-run` reports p50/p95/p99 for each
  query in the canonical suite. With 1B rows and trace-locality
  enforced by the compactor, latencies should stay close to small-corpus
  numbers — that's the point of the row-group pruning + late
  materialization design.

### Tuning notes

| If you see ... | Try ... |
|----------------|---------|
| Ingest is the bottleneck (low rows/sec) | Bump `CONCURRENCY` (e.g. 64) and/or `BATCH_SIZE` (e.g. 1000). |
| Disk fills up during the load phase | Lower `CHUNK_ROWS` (chunks are deleted after load, but peak = 1 chunk file). |
| Long pauses between chunks | Compaction is running synchronously when triggered — raise `COMPACT_EVERY`. |
| Memory pressure during gen | Lower `CHUNK_ROWS` (the generator builds the full chunk in memory before writing). |

### Cleanup

```bash
rm -rf ./bench-1b-work          # scratch chunk dir (auto-cleaned per chunk, but the dir remains)
rm -rf ./data                   # the engine's segments + WAL
```
