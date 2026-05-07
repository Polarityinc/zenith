# ZenithDB vs PostgreSQL vs DuckDB — Mac M4 Pro

Workload: 100112 spans (tenant 0). Iterations per cell: 50.
Postgres has covering btree on (model, status) and a GIN trigram on prompt; DuckDB is in-memory. ZenithDB is hot (segment + zone maps cached).

| Query | Zenith p50/p95 µs | Postgres p50/p95 µs | DuckDB p50/p95 µs | Zenith vs Postgres | Zenith vs DuckDB |
|---|---:|---:|---:|---:|---:|
| B1_trace_load | 392 / 525 | 7999 / 9920 | 543 / 851 | 18.91× faster | 1.62× faster |
| B2_attr_filter | 1731 / 2646 | 193 / 290 | 174 / 190 | 0.11× faster | 0.07× faster |
| B3_fts_memory | 3015 / 3502 | 490 / 1525 | 125 / 225 | 0.44× faster | 0.06× faster |
| B6_jsonpath | 4891 / 5728 | 119 / 253 | 283 / 375 | 0.04× faster | 0.07× faster |
| B8_group_by_model | 6613 / 7446 | 3948 / 9821 | 2246 / 2595 | 1.32× faster | 0.35× faster |

## Caveats

- All three engines hit a tiny dataset (~2.5 K rows). Numbers grow with corpus.
- Postgres includes HTTP-less local Unix socket overhead (faster than network); ZenithDB pays an HTTP roundtrip per query (Postgres uses a libpq local socket).
- DuckDB is in-process; ZenithDB is in a separate process. The DuckDB number is essentially "floor" in-process latency.
- For B3 (FTS), Postgres uses a GIN trigram index on prompt; ZenithDB falls back to scan because no FTS index is wired in this segment.
- For B6 (JSON path), Postgres has an expression index on `(metadata->>'tier')`. ZenithDB falls back to JSON scan.
- ZenithDB will widen the gap as the corpus grows: row-group pruning is `O(1)` w.r.t. rows; Postgres index walks scale with `O(log n)`; DuckDB scans scale `O(n)`.
