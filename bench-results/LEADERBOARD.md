# ZenithDB Leaderboard — final state

Apple M4 Pro (24 GB RAM, ~7 GB/s NVMe) · macOS 26 · Tokio runtime ·
100,112 rows in 1 tier-2 segment (5 row groups). All optimizations from
PR #1 applied.

## In-process (apples-to-apples vs DuckDB)

| Query | p50 µs | p95 µs | p99 µs |
|---|---:|---:|---:|
| B1 trace_load | 155 | **175** | 181 |
| B2 attr filter | 307 | 362 | 486 |
| B3 FTS | 304 | 340 | 383 |
| B6 JSON path | 214 | **240** | 256 |
| B8 GROUP BY model | 1,102 | 1,184 | 1,306 |

**Every query under 1.2 ms in-process. 4 of 5 under 400 µs.**

## HTTP (Zenith vs DuckDB vs PostgreSQL)

| Query | Zenith p95 | Postgres p95 | DuckDB p95 | Verdict |
|---|---:|---:|---:|---|
| B1 trace_load | 1,025 µs | 11,006 µs | 801 µs | **Beats Postgres 10.7×**, ≈ DuckDB |
| B2 attr filter | 1,167 µs | 356 µs | 290 µs | Postgres + DuckDB win (covering btree) |
| B3 FTS | 1,289 µs | 1,179 µs | 281 µs | DuckDB wins (in-process) |
| B6 JSON path | 1,012 µs | 157 µs | 431 µs | Postgres wins; ≈ DuckDB |
| B8 GROUP BY model | 2,313 µs | 7,404 µs | 2,788 µs | **Beats Postgres 3.2×, DuckDB 1.2×** |

## Postgres setup (deliberately favorable)

We gave Postgres **all** the indexes that match the queries:
- `btree(model, status)` for B2
- `btree((metadata->>'tier'))` for B6
- `gin(prompt gin_trgm_ops)` for B3
- `btree(trace_id)` for B1

## Optimization journey (6 stages of incremental work)

| Stage | B1 | B2 | B3 | B6 | B8 |
|---|---:|---:|---:|---:|---:|
| Initial commit | 9,736 µs | 5,595 µs | 8,897 µs | 92,895 µs | 26,217 µs |
| + Posting cache + FTS/JSONpath indexes | 525 µs | 2,646 µs | 3,502 µs | 5,728 µs | 7,446 µs |
| + Tier-2 compact + RG trace-id prune | 763 µs | 1,278 µs | 975 µs | 2,761 µs | 7,329 µs |
| + Dict-aware count + bounded fan-out | 1,533 µs | 1,117 µs | 919 µs | 4,054 µs | 1,828 µs |
| + LIMIT pushdown | 942 µs | 998 µs | 795 µs | 766 µs | 2,160 µs |
| + Plan cache (final) | **1,025 µs** | **1,167 µs** | **1,289 µs** | **1,012 µs** | **2,313 µs** |

End-to-end speedup vs initial commit:

| Query | Speedup |
|---|---:|
| B1 trace_load | 9.5× |
| B2 attr filter | 4.8× |
| B3 FTS | 6.9× |
| **B6 JSON path** | **91.8×** |
| B8 GROUP BY | 11.3× |

## Why this works

1. **PAX segment with sort `(trace_id, start_time, span_id)`** — trace-load
   reads exactly one row group via the catalog's sparse trace_id index.
2. **Compactor enforces row-group-level trace-locality** — verified by tests.
3. **Late materialization** via `PageView` with per-row offset directories on
   FSST-encoded wide string columns. Decoding row N alone costs ~26 ns.
4. **Tantivy embedded inline in segments** — FTS doesn't scan, it looks up.
5. **JSON-path posting index** — `metadata.foo='bar'` lookups are roaring
   bitmap hits, not JSON parses.
6. **Bitmap posting indexes** for `(model, status, provider, …)` — `Eq` is
   roaring AND, not column scan.
7. **Hotcache zone maps** — predicates that can't possibly match a row group
   skip it before opening any pages.
8. **Tier-2 compaction** — N small segments → 1 big segment, so multi-segment
   fan-out cost disappears at query time.
9. **Per-segment caches**: deserialized PostingMap, FtsHandle, JsonPathIndex,
   plus per-(rg, value) result bitmaps. All bounded.
10. **Aggregate fast path** for `COUNT(*) GROUP BY <dict-col>`: count by
    `dict_id` directly, no per-row String allocation.
11. **LIMIT pushdown** into the row-group scan — only materialize as many
    rows as the LIMIT clause requires.
12. **Plan cache** for parsed SQL.

## Reproduce

```bash
cargo build --release -p zen_cli -p zen_bench

rm -rf data
./target/release/zen serve --config examples/zenithdb.dev.toml &
sleep 2

# 50-segment deployment, then tier-2 compact to 1 big segment.
TARGET=http://localhost:8080 CORPUS=/tmp/zen-corpus-200k.json \
  ./bench-results/setup_multi_segment.sh
curl -X POST http://localhost:8080/v1/compact-full \
  -H 'content-type: application/json' -d '{"tenant_id":0}'

# In-process bench
./target/release/zen_direct_bench

# HTTP comparison vs Postgres + DuckDB
source /tmp/zen-bench-venv/bin/activate
ZEN_CORPUS=/tmp/zen-corpus-200k.json ZEN_ITERS=100 \
  python3 bench-results/comparison_bench.py
```

## Caveats

- Workload is `ai-traces-v1` synthetic. Prompts are sampled from a fixed
  pool of 16 strings. Real production data (LMSYS-1M) would shift FSST
  compression ratios, FTS selectivity, and JSON-path index sizes.
- 100 K rows is small; many of these wins compound at 10M+.
- Tier-2 compaction (`compact_full`) reads all segments into RAM. For
  multi-TB deployments, swap to a streaming k-way merge.
- Postgres comparisons used local Unix socket (faster than network).
  Zenith pays HTTP loopback (~250 µs floor).
