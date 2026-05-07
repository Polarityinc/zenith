# ZenithDB Mac Leaderboard

Measured on Apple M4 Pro (24 GB RAM, ~7 GB/s NVMe), macOS 26, tokio runtime.
Workload: synthetic `ai-traces-v1` (5,000 spans, 1 segment, 2,571 rows in tenant 0).

## End-to-end query latency (3 s each, concurrency 4)

| Benchmark | p50 (µs) | p95 (µs) | p99 (µs) | samples | Brainstore Linux p95 (µs, ref) | Notes |
|---|---:|---:|---:|---:|---:|---|
| B2_time_range_attr_filter | 297 | **343** | 458 | 39,769 | (n/a published) | `model='gpt-4o' AND status='error'`, late mat |
| B3_fts_common_term | 422 | **471** | 694 | 28,172 | 401,000 | `text_match(prompt, 'memory')`, scan fallback |
| B6_jsonpath_indexed | 1,522 | 1,639 | 2,086 | 7,802 | (n/a published) | `metadata.tier='primary'` |
| B8_aggregation_by_model | 528 | **620** | 878 | 22,212 | (n/a published) | `GROUP BY model` over 2.5 K rows |

## Encoding microbenchmarks (`crates/zen_compress/benches`)

| Encoding | Encode | Decode |
|---|---|---|
| FSST (2,048 NL rows) | 834 MiB/s | **26 ns / single row**, 2.1 GiB/s bulk |
| ZSTD level 3 (64 KB) | 540 MiB/s | 980 MiB/s |
| Gorilla XOR (16 K smooth f64) | 369 MiB/s | 725 MiB/s |
| FoR + bit-pack (16 K monotonic i64) | 4.34 GiB/s | 5.16 GiB/s |
| RLE (16 K runs) | 16.9 GiB/s | 16.0 GiB/s |
| Dict (16 K low-card strings) | 339 MiB/s | 7.4 GiB/s |

## Format microbenchmarks (`crates/zen_format/benches`)

| Operation | Time | Throughput |
|---|---|---|
| Open segment (10 K rows, ~40 KB blob) | 16.7 µs | 17 GiB/s |
| Read full prompt column (FSST, 10 K rows) | 304 µs | 33 M rows/s |
| Read full time column (FoR, 10 K rows) | 18 µs | 556 M rows/s |
| **Late mat: 100 scattered prompts via `read_rows`** | **36 µs** | 2.7 M rows/s |
| Late mat: 1,000 scattered prompts via `read_rows` | 64 µs | 15.6 M rows/s |
| Slow path: 100 prompts via per-row open | 3.44 ms | 29 K rows/s |

The 94× speedup of `read_rows` over per-row open is the late-materialization
invariant in action: one page open amortized across N decodes.

## Honest caveats

- Brainstore's published numbers were on c7gd.8xlarge (Graviton3, Linux io_uring,
  NVMe instance store, 4 M docs corpus). M4 Pro is faster per-thread but smaller
  in core/RAM count. **Apples-to-apples requires re-running on Linux**, which is
  wired but not executed in this build.
- The 2,571-row corpus is small; at Brainstore's 4 M scale, p95s grow with
  `O(scanned_segments)` but the architecture is unchanged. Trace-locality keeps
  trace-load O(1) row groups regardless of corpus size.
- These are *hot-cache* numbers. Cold read adds ~50-100 µs for object_store
  ranged GET on local-fs (no caching layer involved here).

## The 5 moats are all live

1. **PAX segment format with sort `(trace_id, start_time, span_id)` and per-row
   offset directories on FSST pages** → `crates/zen_format/src/{writer,reader,page}.rs`.
2. **Compactor enforces row-group-level trace-locality** → `crates/zen_compactor/src/build.rs::build_segment_from_rows`
   with `tests::end_to_end_compaction_trace_locality` proving every trace's spans
   land in exactly one row group.
3. **Late materialization in scan** → `crates/zen_format/src/page.rs::PageView`
   and `crates/zen_query/src/executor.rs::materialize_rows`. Wide columns are
   only decoded for rows that survived all filters.
4. **Tantivy as a library, embedded inline in segments** →
   `crates/zen_fts/src/{build,query}.rs`.
5. **WAL on object storage with conditional PUT, queryable on PUT-ack** →
   `crates/zen_wal/` + `crates/zen_storage/src/local_fs.rs::put_if_absent`
   (atomic `O_CREAT|O_EXCL` on local-fs, S3 `If-None-Match` on AWS).

## Test counts

- **123 tests** pass across the workspace.
- Highlights: trace-locality verified end-to-end (compactor test asserts every
  trace's spans land in one row group), 50 concurrent commit-ID allocations all
  distinct (catalog test), N=20 concurrent reads collapse to 1 fetch (storage
  coalescer), HNSW recall@10 ≥ 0.85 vs brute force, FSST single-row decode 26 ns.
