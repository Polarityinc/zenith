# Zenith — Scaling to 1 TB and 1 PB

This document covers two questions:

1. **At 1 TB** of agent-trace data (~40 M docs × 25 KB), how does Zenith
   compare to PostgreSQL and DuckDB on the four Brainstore-style
   benchmarks plus three other common analytics shapes?
2. **At 1 PB** (40 B docs × 25 KB, ~300 TB compressed), what is the
   multi-node architecture that lets Zenith run as a horizontally-scalable
   cluster — like ClickHouse, but with shared object-store-backed
   data so we don't shuffle bytes between nodes?

The 1 TB numbers below are extrapolated from measured 1.2 GB latencies
plus the asymptotic per-segment cost model. They are estimates, not
production-validated runs at 1 TB. The 1 PB design ships with code
(crate `zen_cluster`) and a 3-node integration test that proves the
routing + heartbeat plumbing works end-to-end.

---

## Test workload (canonical)

- 40 M LLM-trace docs × 25 KB each ≈ 1 TB raw
- ~300 GB after FSST + ZSTD + dictionary encoding
- After tier-2 compaction: ~1,000 segments × 300 MB each
- Each segment: ~50 row groups × ~64 MB
- Hardware reference: c7gd.8xlarge (32 vCPU, 64 GB RAM, NVMe instance
  store, 12.5 Gbps network) for cluster nodes; segments live on S3.

## Query suite

| ID | Query | Selectivity | Has LIMIT? |
|---|---|---|---|
| B1 | `WHERE trace_id = ? LIMIT 100` | 1 trace / 40M | yes |
| B2 | `WHERE model='gpt-4o' AND status='error' LIMIT 100` | ~2 % | yes |
| B3 | `WHERE prompt MATCH 'memory' LIMIT 100` (FTS) | ~10 % | yes |
| B6 | `WHERE metadata.tier='primary' LIMIT 100` (JSON path) | ~50 % | yes |
| B8 | `SELECT model, count(*) GROUP BY model` | 100 % (no LIMIT) | no |
| W1 | flush 100 × 100 KB rows | n/a | n/a |
| W2 | read-after-flush visible latency | n/a | n/a |

---

## 1 TB head-to-head: Zenith vs Postgres vs DuckDB

p95 latencies, warm cache, single host (32 vCPU, 64 GB), anchored to
**measured 1.2 GB numbers** and scaled out by the asymptotic per-shape
cost model. Lower is better. Bold = best in row.

**Anchor row** — what was actually measured at 1.2 GB (from the most
recent benchmark run, included for honesty):

| Test | Zenith @ 1.2 GB | Postgres @ 1.2 GB | DuckDB @ 1.2 GB |
|---|---:|---:|---:|
| B1 span_load p95 | 1–3 ms | 1–22 ms | 0.4–2.7 ms |
| B3 FTS "memory" p95 | 0.9 ms | 9.5–10.9 ms | 1–1.2 ms |
| W1 flush 100 × 100 KB p95 | 24–41 ms | 240–280 ms | 95–150 ms |
| W2 write visible p95 | 0 ms | 240–280 ms | 95–150 ms |

**Estimated row** — extrapolation to 1 TB (≈ 833× the corpus):

| Test | **Zenith (1 node)** | PostgreSQL 16 (+pgvector) | DuckDB 1.0 |
|---|---:|---:|---:|
| B1 trace_load | **5–30 ms** | 30–200 ms | 200 ms – 2 s |
| B2 attr filter | **5–20 ms** | 100 ms – 1 s | 500 ms – 5 s |
| B3 FTS "memory" | **5–20 ms** | **1 – 10 s** (GIN can't push LIMIT) | 500 ms – 8 s |
| B6 JSON path | **5–15 ms** | 500 ms – 5 s | 5 – 60 s (per-row JSON eval) |
| B8 GROUP BY (full scan) | **30–300 ms** | **1 – 5 min** (heap scan + TOAST) | 5 – 60 s (parallel column scan) |
| W1 flush 100 × 100 KB | **25–50 ms** | 280 ms – 1 s | 100 ms – 800 ms |
| W2 write visible | **0–10 ms** | 280 ms – 1 s (= write flush) | 1 – 30 s (next CHECKPOINT) |

### How the 1 TB numbers come from the 1.2 GB measurements

The naive question is "if 1.2 GB → 1 TB is 833×, why isn't every
latency 833× worse?" The answer is that different shapes scale
differently. There are three classes:

1. **O(1) in corpus size** — point lookups via index, write commits
   that don't touch existing data, sync visibility paths. These get
   slower only by cache-miss factor + log-depth growth.
2. **O(matches) in selectivity** — FTS, range filters that don't have
   LIMIT pushdown. These scale linearly with the *number of matching
   rows*, which is corpus_size × selectivity.
3. **O(N) in corpus size** — full-table aggregates without index
   coverage. These are the ones that go from "annoying" to
   "unusable" as the corpus grows.

#### Postgres B1 span_load: 1–22 ms → 30–200 ms (10× worse)

- btree depth grows by ~2 levels (log_branching_factor of 833× ≈ 2).
- Working set no longer fits in `shared_buffers` → 2-5 cold page
  fetches at ~1 ms each.
- TOAST detoast of the 25 KB row: same cost (~5-10 ms).
- Net: ~10× degradation. Still very usable.

#### Postgres B3 FTS: 9–11 ms → **1–10 s** (≈ 200–800× worse)

This is the big one and the place I underestimated. Postgres GIN
posting lists scale **linearly with the number of matching documents**,
not with the LIMIT. The query optimizer cannot push `LIMIT 100` into
the GIN scan: it must build the full bitmap of matches first, then
apply LIMIT at the heap-fetch stage.

- 1.2 GB: "memory" matches ~10 % × 50 K = 5,000 docs. GIN posting list
  fits in one page; bitmap construction is microseconds.
- 1 TB: "memory" matches ~10 % × 40 M = 4 M docs. GIN posting list is
  30–100 MB; bitmap construction is hundreds of ms; sorting and
  ranking 4 M entries before taking 100 dominates the wall clock.

Zenith stays at 5–20 ms because Tantivy *does* push LIMIT 100 into
the index search (top-K early termination), and per-segment Tantivy
handles are cached.

#### Postgres B8 GROUP BY full scan: 1–5 min (scales linearly)

This is the most painful comparison. With no covering index that
includes `model` *and* zero TOAST detoast (which Postgres can't avoid
because the heap row is needed to be counted), B8 reduces to a
sequential heap scan of 1 TB.

- Effective sequential read on local NVMe: ~500 MB/s after the heap
  walks TOAST and indexes touch the row visibility map.
- 1 TB ÷ 500 MB/s = 2,000 sec single-thread = 33 min.
- Parallel query with 8 workers (default `max_parallel_workers_per_gather=2`,
  bumped) → ~4 min wall.
- A *covering index* on `(tenant_id, model)` cuts this dramatically
  (~10–30 sec) but doubles your storage cost; in practice covering
  indexes are not maintained on agent traces because `model` is one
  of dozens of columns operators want to group by.

I wrote "8–30 s" earlier — that was assuming a covering index. Without
one, the truthful answer is **1–5 minutes**.

#### Postgres W1 / W2: 240–280 ms → 280 ms – 1 s

Writes are *almost* O(1) in corpus size:

- WAL fsync: same 1-5 ms regardless of corpus.
- TOAST chunking of 10 MB body: same ~50-100 ms.
- Index updates: 5 indexes × log₂(40M) = trivial extra cost.
- *But*: lock contention with concurrent vacuum + autovacuum CPU at
  1 TB chews up roughly 2-3× of the 1.2 GB latency under load.

The "write visible" column is interesting: in Postgres, write-visible
*is* write-flush (transactional). Zenith decouples them by scanning
unconsumed WALs in the executor — write returns when the WAL object is
durable, but a query that arrives 1 ms later sees the row immediately.
That's why Zenith's "write visible" stays at 0 ms regardless of corpus.

#### DuckDB at 1 TB

DuckDB scales much better than Postgres on full scans (vectorized
columnar) but worse on point lookups (no traditional indexes — relies
solely on zone-map pruning on min/max stats):

- **B1**: With trace-sorted ingest, zone maps prune to ~1 row group →
  100-300 ms. With realistic interleaved ingest (which is the actual
  agent-trace pattern), zone maps prune to 5–50 row groups → 200 ms – 2 s.
  Zenith's compactor enforces trace-locality at compaction time, which
  is the architectural difference that makes B1 cheap regardless of
  ingest order.
- **B3 FTS**: DuckDB's `fts` extension stores BM25 posting lists in a
  side table, joined back to the main table. At 1 TB, 4 M matches
  drive the join cost similarly to Postgres GIN. 500 ms – 8 s.
- **B8**: DuckDB's strongest case. The model column ZSTD-compressed
  is ~30-80 MB; with dictionary stats DuckDB can answer count(*) by
  group without decoding the column itself. 5 sec is realistic; up to
  60 sec when the dictionary spans many segments and the parallel scan
  has to coordinate.
- **W1 / W2**: writes are batched; DuckDB only makes them visible
  after `CHECKPOINT`. At 1 TB, checkpoints involve compacting the WAL
  + index rebuild, which can take 1-30 sec. Zenith's WAL-scan executor
  removes that latency entirely — writes are visible the instant the
  WAL object PUT acks.

### The architectural reasons Zenith stays flat where the others scale up

The 1.2 GB measurements look close because at small scales every
engine's working set fits in RAM and the constant factors dominate.
At 1 TB the picture diverges because Zenith's design pushes back
against three specific failure modes that hit Postgres and DuckDB:

1. **LIMIT-aware index search.** Tantivy returns top-K with early
   termination; GIN cannot. This single architectural choice is why
   Zenith B3 stays at 5–20 ms while Postgres B3 goes to 1–10 sec.
2. **Cached per-segment indexes** (posting lists, FTS handles, JSON
   path indexes). After the first cold open, the second query against
   the same segment costs ~5 µs to look up the bitmap. Postgres GIN
   has to descend the same btree pages on every query.
3. **Trace-locality enforced at compaction**, not derived from ingest
   order. DuckDB's zone maps prune well *if* you happened to ingest
   in trace order; in agent-trace workloads that is never true.

### Hard cases / honest caveats

- **DuckDB on cold cache**: B1 + B2 + B3 all degrade to 200 ms - 5 s
  if the relevant Parquet files aren't already mmap'd. Zenith pays the
  same cold cost — it's a property of object storage, not the engine.
- **Postgres after VACUUM ANALYZE**: a freshly vacuumed Postgres with
  warm cache sometimes hits 5-15 ms on B1 — competitive with Zenith.
  But only Zenith holds that latency past the 4 M-row mark.
- **Cross-tenant aggregates** (e.g. "total spans across all tenants
  yesterday"): Postgres wins under 10 GB because it has a single
  index. Zenith wins past 100 GB because it pushes the count into
  per-segment partials.
- **No published Brainstore numbers exist for this workload at 1 TB.**
  Brainstore's published table is 4 M docs (~100 GB) on c7gd.8xlarge.
  Extrapolating linearly is unsafe; based on architectural similarity
  to Postgres, we'd guess Brainstore at 1 TB looks roughly 2-4× their
  100 GB numbers — so 1.0–2.5 s on span_load, 0.5–1.5 s on FTS,
  3–7 s on write flush. Zenith would still beat each by 30-100×.

---

## 1 PB and beyond: multi-node architecture

ClickHouse scales to PB by sharding tables across nodes and replicating
each shard for HA. We do **almost the same thing**, with one big
simplification: the data plane is a shared object store (S3 / GCS / fs)
rather than per-node local disks. That separation of compute and storage
lets us:

- Add or remove nodes without rebalancing data on the wire.
- Run the compactor as a separate fleet from query coordinators.
- Spin up ephemeral query-only nodes for burst capacity.

### Layered architecture (top to bottom)

```
┌──────────────────────────────────────────────────────────┐
│ Clients (HTTP/gRPC/OTLP)                                  │
└───────────────┬──────────────────────────────────────────┘
                │
┌───────────────▼──────────────────────────────────────────┐
│ Coordinator pool (any node with role=Coordinator|All)     │
│  - parses query, looks up shard in `ShardMap`             │
│  - routes via `QueryRouter`:                              │
│      · Local        — execute here                        │
│      · Remote(N)    — POST /v1/internal/query → primary   │
│      · FanOut       — parallel POST + merge               │
└───────────────┬──────────────────────────────────────────┘
                │
┌───────────────▼──────────────────────────────────────────┐
│ Worker pool (role=Worker|All)                             │
│  - runs scans against object store + segment cache        │
│  - returns ResultSet partials                             │
└───────────────┬──────────────────────────────────────────┘
                │
┌───────────────▼──────────────────────────────────────────┐
│ Compactor pool (role=Compactor) — write-side only         │
│  - reads WALs, builds segments, registers in catalog      │
│  - acquires per-(tenant,partition) lease                  │
└───────────────┬──────────────────────────────────────────┘
                │
┌───────────────▼──────────────────────────────────────────┐
│ Catalog (Postgres at PB scale; MockCatalog (in-memory; not for production))        │
│  - tenants, partitions, segments, wal_objects, leases     │
│  - nodes (with last_heartbeat_ms)                         │
└───────────────┬──────────────────────────────────────────┘
                │
┌───────────────▼──────────────────────────────────────────┐
│ Object store (S3 / GCS / Azure / local FS)                │
│  - immutable .zseg segments                               │
│  - immutable .zwal WAL objects                            │
│  - shared-readable from every node                        │
└──────────────────────────────────────────────────────────┘
```

### Sharding by rendezvous hash

We use **HRW (Highest Random Weight)** hashing on `(tenant_id,
partition_id)`. For each shard key, every node computes the same
deterministic score against every other node and ranks them. The top-R
become the replicas (R = `replication_factor`, typically 2 or 3).

Properties (vs ClickHouse's manual `Distributed` shard map):

- **No coordinator**: every node computes the same routing
  independently. No leader election needed.
- **Minimal remap on resize**: adding 1 node to N moves only ~1/N of
  shards. A 100→101 cluster scale-up moves ~1% of shards. The
  `rendezvous_minimal_remap_when_node_added` test in `zen_cluster`
  empirically validates this at ~9% on a 10→11 cluster.
- **Filter expressions per node**: each NodeInfo carries a `shards`
  expression like `tenant=1,2,3` or `tenant=10..20` so operators can
  pin large tenants to dedicated hardware.

### Replication and consistency

- **Storage replication**: provided by S3 (11 9s for free) or by the
  underlying GCS/Azure equivalent. Local-FS deployments need a
  separate replication scheme; we recommend `zfs send` snapshots or
  rsync to a sibling.
- **Read consistency**: snapshot-isolation per query. The query plan
  captures `as_of_commit_id` at parse time and reads only segments +
  WAL objects with `commit_id ≤ as_of`. Concurrent ingest doesn't
  affect in-flight queries.
- **Write coordination**: a single tenant's `next_commit_id` is
  allocated through one row in `commit_seq_state`. With Postgres,
  this is a `SELECT … FOR UPDATE` + `UPDATE … RETURNING`.
  `put_if_absent` on the WAL object catches the rare race where two
  nodes pick the same commit_id; the loser bumps and retries.
- **HA writer**: any node accepting an ingest can serve as the writer
  for that batch. There's no primary-of-the-moment to fail over.

### What's wired up in the repo today

- ✅ `zen_cluster` crate with `NodeId`, `NodeRole`, `NodeInfo`,
  `ShardMap` (HRW), `QueryRouter`, `RemoteClient`, `merge_result_sets`.
  14 unit tests.
- ✅ `nodes` table in `zen_catalog`; `upsert_node` / `list_nodes`
  trait methods on `Catalog`. Postgres production impl + `MockCatalog` test impl
  implementation (the latter behind `catalog-postgres`).
- ✅ `ServerState::with_cluster(NodeRegistry)` — cluster mode is opt-in.
- ✅ `POST /v1/internal/query` — node-to-node endpoint that bypasses
  the router and always executes locally.
- ✅ Query handler in `zen_server` consults the router on every
  `/v1/query` and forwards to remote replicas when this node is not
  the primary, with graceful fallback to local exec on full-replica
  outage (every node can read shared object_store).
- ✅ 3-node integration test (`tests/integration/multi_node.rs`)
  proving heartbeat + shard map convergence + routing transparency.

### What's queued behind a feature flag (next sprint)

- Cross-shard `GROUP BY` partial aggregation merge in the planner.
  Today the merger concatenates rows + sums stats; for cross-tenant
  aggregates we need the planner to emit `partial → final` and merge
  group-by hash maps on the coordinator. ~300 LOC.
- Worker-only / compactor-only deployment mode. The roles are typed
  in `NodeRole` already; we just need binaries for `zen serve
  --role=worker` and `zen compact --daemon`.
- TLS for inter-node traffic. The `RemoteClient` is HTTP-only today
  because we assumed VPC-internal trust; behind a flag, switch to
  rustls + mTLS using the catalog as the cert store.

### Capacity ladder

Rough sizing for the cluster, derived from the per-segment cost model.

| Corpus | Nodes | RAM / node | Segment cache hit | Notes |
|---|---:|---:|---:|---|
| 1 GB | 1 | 8 GB | 100 % | Postgres (cloud-managed) is the production catalog at every scale |
| 100 GB | 1 | 32 GB | 100 % | Still single-node; one Postgres instance with PITR |
| 1 TB | 3 | 64 GB | ~95 % | Postgres catalog, replication=2, S3 storage |
| 10 TB | 8–12 | 64 GB | ~80 % | Coordinator + worker split optional |
| 100 TB | 32–64 | 64 GB | ~50 % | Coordinator/worker/compactor pool split |
| 1 PB | 256 | 64 GB | <30 % | Tenant pinning required; HLL approximations on B8 |

At 1 PB, hot working set rarely exceeds 10 TB (the last 7 days of
ingest plus any retroactive deep-dive). Cache hit rates fall but
latency stays bounded because the cold path is a parallel ranged GET
with `buffer_unordered(64)` — bandwidth-limited, not request-limited.

---

## Summary

- **At 1 TB on a single host**: Zenith beats Postgres on every query
  type and beats DuckDB on every query type *except* possibly the very
  cleanest in-memory full-scan aggregate (B8). The latter is one
  precomputed-rollup feature away from also being a Zenith win.
- **At 1 PB across a cluster**: the architecture is a coordinator /
  worker / compactor split sharded by HRW over tenant+partition,
  reading shared object storage. The wire protocol, node registry,
  router, and remote forwarder are implemented and tested today
  (commit on this branch).
- **What's not yet validated**: actual measured throughput numbers at
  1 TB / 1 PB — those need a real S3 + multi-host cluster and a
  budget the local-Mac dev environment can't deliver.

The path from "passing 3-node test on a Mac" to "actual PB cluster on
AWS" is operational, not architectural: spin up a Postgres catalog
replicated across an AZ, point N nodes at it, run them with
`role=All` (or split roles for big deployments), and let HRW + S3 do
the rest.
