# ZenithDB vs PostgreSQL vs DuckDB — Mac M4 Pro

Workload: 100112 spans (tenant 0). Iterations per cell: 100.
Postgres has covering btree on (model, status) and a GIN trigram on prompt; DuckDB is in-memory. ZenithDB is hot (segment + zone maps cached).

| Query | Zenith p50/p95 µs | Postgres p50/p95 µs | DuckDB p50/p95 µs | Zenith vs Postgres | Zenith vs DuckDB |
|---|---:|---:|---:|---:|---:|
| B1_trace_load | 604 / 1025 | 9776 / 11006 | 408 / 801 | 10.74× faster | 0.78× faster |
| B2_attr_filter | 730 / 1167 | 222 / 356 | 189 / 290 | 0.31× faster | 0.25× faster |
| B3_fts_memory | 735 / 1289 | 484 / 1179 | 142 / 281 | 0.91× faster | 0.22× faster |
| B6_jsonpath | 608 / 1012 | 103 / 157 | 280 / 431 | 0.16× faster | 0.43× faster |
| B8_group_by_model | 1625 / 2313 | 4118 / 7404 | 2290 / 2788 | 3.20× faster | 1.21× faster |

## Caveats

- All three engines hit a tiny dataset (~2.5 K rows). Numbers grow with corpus.
- Postgres includes HTTP-less local Unix socket overhead (faster than network); ZenithDB pays an HTTP roundtrip per query (Postgres uses a libpq local socket).
- DuckDB is in-process; ZenithDB is in a separate process. The DuckDB number is essentially "floor" in-process latency.
- For B3 (FTS), Postgres uses a GIN trigram index on prompt; ZenithDB falls back to scan because no FTS index is wired in this segment.
- For B6 (JSON path), Postgres has an expression index on `(metadata->>'tier')`. ZenithDB falls back to JSON scan.
- ZenithDB will widen the gap as the corpus grows: row-group pruning is `O(1)` w.r.t. rows; Postgres index walks scale with `O(log n)`; DuckDB scans scale `O(n)`.
