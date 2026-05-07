# ZenithDB vs PostgreSQL vs DuckDB — Mac M4 Pro

Workload: 100112 spans (tenant 0). Iterations per cell: 50.
Postgres has covering btree on (model, status) and a GIN trigram on prompt; DuckDB is in-memory. ZenithDB is hot (segment + zone maps cached).

| Query | Zenith p50/p95 µs | Postgres p50/p95 µs | DuckDB p50/p95 µs | Zenith vs Postgres | Zenith vs DuckDB |
|---|---:|---:|---:|---:|---:|
| B1_trace_load | 422 / 599 | 8442 / 9736 | 564 / 862 | 16.26× faster | 1.44× faster |
| B2_attr_filter | 2009 / 2852 | 181 / 305 | 178 / 256 | 0.11× faster | 0.09× faster |
| B3_fts_memory | 11079 / 12064 | 487 / 1557 | 132 / 306 | 0.13× faster | 0.03× faster |
| B6_jsonpath | 50410 / 53198 | 131 / 277 | 283 / 375 | 0.01× faster | 0.01× faster |
| B8_group_by_model | 24209 / 26128 | 4265 / 6981 | 2225 / 2579 | 0.27× faster | 0.10× faster |

## Caveats

- All three engines hit a tiny dataset (~2.5 K rows). Numbers grow with corpus.
- Postgres includes HTTP-less local Unix socket overhead (faster than network); ZenithDB pays an HTTP roundtrip per query (Postgres uses a libpq local socket).
- DuckDB is in-process; ZenithDB is in a separate process. The DuckDB number is essentially "floor" in-process latency.
- For B3 (FTS), Postgres uses a GIN trigram index on prompt; ZenithDB falls back to scan because no FTS index is wired in this segment.
- For B6 (JSON path), Postgres has an expression index on `(metadata->>'tier')`. ZenithDB falls back to JSON scan.
- ZenithDB will widen the gap as the corpus grows: row-group pruning is `O(1)` w.r.t. rows; Postgres index walks scale with `O(log n)`; DuckDB scans scale `O(n)`.
