# Architecture

This document is for engineers who want to understand *why* ZenithDB
makes the choices it does, before they read code. For the operator
view, see [RUNBOOK.md](RUNBOOK.md).

## The workload

AI agent traces look unlike anything traditional observability backends
were built for:

- **Long, sparse JSON payloads**: prompts, completions, tool I/O.
  Single fields can be 100 KB+.
- **High cardinality** on every attribute that matters: model versions,
  tool names, run IDs.
- **Late-arriving annotations**: rubric scores, human labels, replays —
  attached to traces minutes or hours after the original spans.
- **Bursty ingest**, then minutes of silence.
- **Trace-locality**: a query almost always reads spans by `trace_id`,
  often the full tree.

Treating this as "spans = rows in a wide table" the way Honeycomb or
Tempo do works, but the per-byte cost is 10–100× too high. The five
"moat" crates exist because of this shape.

## The five non-negotiables

### 1. PAX segments sorted by `(trace_id, start_time, span_id)`

Crate: [`zen_format`](../crates/zen_format).

Segments use a **PAX** layout (Partition Attributes Across) — rows are
grouped into row groups (~64 K rows each), columns are stored together
inside each group. This gives column scans the locality of pure
columnar formats *and* row-fetch locality when you do need the whole
span.

The sort key matters: trace lookups become a binary search inside one
row group, not a scan of N segments. Wide string columns (prompt,
completion, tool I/O) get **per-row offset directories** so we can skip
into the right byte range without decoding the whole column.

Codecs are picked per column:

- **FSST + ZSTD** for strings (FSST first — it's a static dictionary
  compressor that lets ZSTD do its best work).
- **Gorilla XOR** for floats (timestamps, durations).
- **Frame-of-Reference + RLE** for low-card integers.
- **Dictionary** for enums (model, status, span_type).

### 2. Trace-locality at compaction time

Crate: [`zen_compactor`](../crates/zen_compactor).

The compactor enforces, at row-group construction, that every span of
a trace lands in **one row group**. This is the single most important
property of the engine: a "give me all spans for trace X" query reads
one row group, full stop.

The compactor is a streaming k-way merge — it never materializes all
segments in memory, just the heads of the input streams. Tier 2
compaction merges already-trace-local row groups into larger segments
without re-sorting.

### 3. Late materialization in the scan operator

Crate: [`zen_query`](../crates/zen_query).

Conventional column stores decode every projected column for every
row, then filter. We do the opposite: we run all cheap predicates
first, build a row mask, and *then* decode the wide columns — but
**only for surviving rows**.

Concretely: a query like `WHERE model = 'claude-opus-4-7' AND status =
'ok' SELECT prompt, completion` does the model + status dictionary
checks on the bitmap-encoded columns first, drops ~95% of rows, and
then dereferences only the prompts and completions for the survivors
via the offset directory. The wide columns are *never* fully decoded.

### 4. Tantivy embedded inline

Crate: [`zen_fts`](../crates/zen_fts).

Full-text search lives **inside** the segment, not in a sidecar
index. Each segment carries its own Tantivy index over the configured
text fields (`prompt`, `completion`, `tool_io_text` by default). This
means:

- One file to compact, one file to back up, one file to evict.
- The query path reads index + data with one IO planner.
- FTS can be combined with structured predicates in the same scan
  operator (FTS bitmap → AND → other bitmaps → late materialization).

The cost is segment size — a Tantivy index is ~20% of text size on top
of the compressed strings. We think it's worth it.

### 5. WAL on object storage with conditional PUT

Crate: [`zen_wal`](../crates/zen_wal).

The WAL doesn't live on local disk. It lives in the same object store
the segments do, with each batch keyed by a monotonic sequence number
and written with **conditional PUT** (`If-None-Match: *`) so two
writers can't both believe they own the same sequence.

The querier reads the WAL directly — so writes are visible the moment
the object-store PUT acknowledges, with no fsync, no replication
round-trip, no commit log replay. WAL segments are then merged with
compacted segments at query time and eventually rolled up into proper
`.zseg` files.

This is what lets ZenithDB run on **just** S3 + Postgres (catalog) and
still give you single-digit-millisecond read-after-write.

## Request flow

```
        ┌────────────────────────────────────────────┐
        │           Clients (REST / gRPC / OTLP)     │
        └─────────────────────┬──────────────────────┘
                              ▼
                  ┌───────────────────────┐
                  │ Gateway (axum, tonic) │   ← auth, rate limit, tracing
                  └───────┬───────────────┘
              ingest      │      query
                          │
            ┌─────────────┴──────────────┐
            ▼                            ▼
      ┌──────────┐                ┌────────────┐
      │  Writer  │                │  Querier   │
      │ memtable │                │ planner +  │
      │   WAL    │                │ executor   │
      └─────┬────┘                └──────┬─────┘
            │                            │
            ▼                            ▼
        ┌──────────────────────────────────┐
        │      Catalog (Postgres)          │   ← tenant, segment, schema
        └──────────────────────────────────┘
            │                            │
            ▼                            ▼
        ┌──────────────────────────────────┐
        │  Object storage (fs / S3 / …)    │
        │  ├── WAL (.wal)                  │
        │  └── Segments (.zseg)            │
        └──────────────────────────────────┘
```

## Why Postgres for the catalog

We considered etcd, FoundationDB, Raft-of-the-week. We chose Postgres
for three reasons:

1. **Operators already run it.** No new system to provision, monitor,
   backup, or page on.
2. **SERIALIZABLE transactions** make segment-list mutations trivial.
3. **Logical replication + multi-AZ** is a solved problem.

SQLite was the original dev backend; we removed it because it doesn't
survive cluster scale-up (single writer, no replication, no failover)
and it confused users about what to deploy in production. For tests
and local dev there's an in-memory `MockCatalog` that has the same
interface.

## Why tokio-only

All async lives on a single tokio runtime. We do not mix `async-std`,
`smol`, or ad-hoc threadpools. Blocking work goes through
`tokio::task::spawn_blocking`.

Reason: cross-runtime panics, double-tokio deadlocks, and "which
executor am I on right now?" debugging cost more than the marginal
ergonomics of any one library that wanted its own runtime.

## What's *not* in here on purpose

- **No background JIT.** The query path is hand-vectorized. JITting a
  per-query plan is on the roadmap for sub-millisecond OLAP but isn't
  the first 80% of speedups.
- **No distributed transactions.** Writes are single-tenant, single
  segment. Queries fan out via the rendezvous-hash router but never
  hold cross-shard locks.
- **No row-level updates.** All mutation goes through ingest +
  compaction. Soft-deletes are tombstoned during compaction.

## Reading order

If you're trying to ramp on the engine, read the crates in this order:

1. `zen_format` — the on-disk layout. Everything else is downstream of this.
2. `zen_wal` — the durability story.
3. `zen_query` — how the scan operator and late materialization work.
4. `zen_compactor` — how trace-locality is built and maintained.
5. `zen_fts` — the embedded FTS index.

Everything else (`zen_server`, `zen_catalog`, `zen_cluster`,
`zen_auth`, `zen_crypto`, `zen_index`, `zen_jsonpath`, `zen_vector`,
`zen_memtable`, `zen_compress`, `zen_proto`, `zen_ql`, `zen_bench`,
`zen_common`, `zen_cli`) is supporting infrastructure — large in
volume, low in conceptual surprise.
