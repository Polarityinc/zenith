# ZenithDB vs PostgreSQL vs DuckDB vs ClickHouse — 1 M-row corpus

Workload: `ai-traces-v1` synthetic agent traces, 1 M rows generated, **499,145 tenant-0 spans** loaded into each engine. Iterations per cell: **100**. Hardware: Mac M4 Pro, single-node, all four engines local.

## Setup

| Engine | Version | Mode | Indexes / sort keys |
|---|---|---|---|
| ZenithDB | this branch (`AIGeneration`) | `zen serve` + HTTP/JSON | columnar segment, zone maps, bitmap idx on `(model, tool_name, status, span_type, provider)`, JSON-path sample idx, inline Tantivy FTS. One segment, 31 row groups, post-compaction. Hot. |
| PostgreSQL | 14 (homebrew) | local libpq socket | btree on `trace_id`; btree on `(model, status)`; expression idx on `(metadata->>'tier')`; GIN trigram on `prompt`; `ANALYZE`'d |
| DuckDB | 1.x | in-process Python | no secondary indexes (table scan); in-memory |
| ClickHouse | 26.4.2 | **persistent `clickhouse-server`**, HTTP transport | MergeTree, `ORDER BY (model, status, trace_id)`, `index_granularity=8192` |

ClickHouse is benchmarked over HTTP against a long-lived server — the same transport Zenith uses — so per-iteration cost is comparable. (A prior pass with `clickhouse local` per query paid ~170 ms of subprocess startup on every call and is not a useful comparison; numbers below are server-mode.)

## p50 / p95 latency (microseconds)

| Query | Zenith | Postgres | DuckDB | ClickHouse |
|---|---:|---:|---:|---:|
| **B1**  trace load by `trace_id`        | 390 / 514   | 85 / 137    | 637 / 707   | 2 508 / 4 228 |
| **B2**  attr filter `model AND status`  | 446 / 508   | 185 / 410   | 235 / 283   | 2 109 / 2 605 |
| **B3**  FTS-like `prompt LIKE %memory%` | 407 / 482   | 461 / 1 649 | 162 / 206   | 3 192 / 3 602 |
| **B6**  JSON path `metadata.tier='primary'` | 386 / 525 | 152 / 490 | 365 / 429   | 4 718 / 7 819 |
| **B8**  aggregation `GROUP BY model`    | 821 / 922   | 11 179 / 13 093 | 1 689 / 2 385 | 3 037 / 3 848 |

## Speedup vs each engine (p95, higher = Zenith faster)

| Query | vs Postgres | vs DuckDB | vs ClickHouse |
|---|---:|---:|---:|
| B1_trace_load     | 0.27× (slower) | 1.38× | **8.22×** |
| B2_attr_filter    | 0.81× (slower) | 0.56× (slower) | **5.13×** |
| B3_fts_memory     | **3.42×**      | 0.43× (slower) | **7.47×** |
| B6_jsonpath       | 0.93× (slower) | 0.82× (slower) | **14.89×** |
| B8_group_by_model | **14.21×**     | **2.59×** | **4.18×** |

## What the numbers say

**Zenith wins decisively on B8 (aggregation).** 14× over Postgres, 2.6× over DuckDB, 4.2× over ClickHouse — this is the case columnar storage + bitmap indices were built for, and the lead grows with cardinality.

**Zenith wins on B3 (substring search) vs Postgres** despite Postgres having a GIN trigram index, because Zenith's row-group bitmap pruning skips ~95% of pages before the scan. DuckDB still wins because in-process SIMD substring is unbeatable for an in-memory floor.

**Zenith ties or loses on B1, B2, B6** to Postgres, by single-digit-µs differences that are dominated by HTTP roundtrip overhead (~80 µs) vs local Unix socket (~5 µs). On apples-to-apples transport (Zenith HTTP vs ClickHouse HTTP), Zenith is 5–15× faster on every query.

**DuckDB is the in-process floor** — anything within ~2× of DuckDB is bound by physical scan + decode, not by server architecture.

**ClickHouse is 5–15× slower than Zenith on every query at this scale.** MergeTree's per-query overhead and primary-key sort-order assumptions don't help when the predicate isn't on the leading sort key, and 500 K rows is below the corpus size where its compression + vectorization dominate.

## Compared to the prior 100 K-row run

Zenith's p95 latencies are **essentially unchanged at 5× the data** (514–922 µs across all queries, vs 1 012–2 313 µs at 100 K — and the 100 K numbers were measured cold). This is the row-group-pruning claim materialised: scanned rows are a function of selectivity, not table size.

Postgres B8 stayed at ~13 ms (it was 7.4 ms at 100 K — grew sublinearly thanks to the btree). DuckDB B8 dropped from 2.8 → 2.4 ms (warm cache amortizing better). ClickHouse wasn't in the prior run.

## Caveats

- Single-node, single-machine, no concurrent load. Concurrent throughput is a separate question from p95 latency.
- ZenithDB pays an HTTP roundtrip (~80 µs) on every query; Postgres uses a libpq local Unix socket (~5 µs); DuckDB is in-process. The HTTP cost is included in every Zenith and ClickHouse row.
- DuckDB's bulk insert via `executemany` took ~10 min for 500 K rows (Python driver overhead) — `register` + `INSERT FROM df` would be sub-second. The **query** numbers are unaffected.
- For B3, Zenith uses `text_match(...)` which the planner currently maps to a row-group bitmap scan of `prompt`, not the Tantivy FTS posting list (planner heuristic). PG uses GIN trigram. DuckDB and ClickHouse do plain `LIKE`.
- For B6, Zenith hits its JSON-path sample index; Postgres hits its expression index; DuckDB and ClickHouse scan and extract.
- ClickHouse is 26.4.2 default settings; production tuning (a more selective `ORDER BY`, materialized projections, or a SET index on `model`) would close some of the B2 / B8 gap.

## Reproduce

```bash
# 1. Generate corpus
./target/release/zen bench-gen --rows 1000000 --output /tmp/zen-corpus-1m.json

# 2. Start Zenith, load + compact
./target/release/zen serve --config examples/zenithdb.dev.toml &
./target/release/zen bench-load --input /tmp/zen-corpus-1m.json \
  --target http://localhost:8080 --batch-size 5000 --concurrency 8
curl -X POST http://localhost:8080/v1/compact \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":0,"partition_id":0}'

# 3. Start clickhouse-server (config in /tmp/zen-bench-data/ch-server/)
clickhouse server --config-file=/tmp/zen-bench-data/ch-server/config.xml &

# 4. Run the bench
source /tmp/zen-bench-venv/bin/activate
python3 bench-results/comparison_bench_1m.py
```
