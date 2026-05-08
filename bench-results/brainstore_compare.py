#!/usr/bin/env python3
"""
Brainstore-style benchmark: same four scenarios, three engines.

  1. Span load (UI trace inspect): SELECT * WHERE trace_id = X
  2. Full-text search ("out of memory")
  3. Write latency (flush to server): 100 × 100 KB docs ingest ack time
  4. Write visibility (read-after-flush): time from flush-ack to query hit

Brainstore workload: 3,925,153 docs × 25 KB on c7gd.8xlarge.
Local equivalent: scaled-down to fit a Mac (default 100K × 25 KB ≈ 2.5 GB).
Scale via env var: ZEN_DOCS=200000 to push it.

Run:
  source /tmp/zen-bench-venv/bin/activate
  python3 bench-results/brainstore_compare.py

Pre-req: ZenithDB running on localhost:8080 (clean / empty data dir).
"""

import json
import os
import random
import statistics
import string
import subprocess
import time
import uuid
from pathlib import Path

import duckdb
import psycopg2
import requests
from psycopg2.extras import execute_values

USER = os.environ.get("USER", "postgres")
PG_DSN = f"host=localhost dbname=postgres user={USER}"
ZEN_URL = "http://localhost:8080"
N_DOCS = int(os.environ.get("ZEN_DOCS", "100000"))
DOC_SIZE_BYTES = int(os.environ.get("ZEN_DOC_SIZE", "25000"))
N_TRACES = N_DOCS // 12  # ~12 spans per trace
N_QUERY_ITERS = int(os.environ.get("ZEN_QUERY_ITERS", "50"))
WRITE_BATCH_DOCS = 100
WRITE_BATCH_DOC_SIZE = 100_000


def lorem_pool():
    """A natural-language pool that includes 'memory' for FTS."""
    return [
        "out of memory error in retrieval cache during compaction phase 3",
        "the model encountered a rate limit and is retrying with backoff",
        "summarize the conversation into three bullet points and identify the main intent",
        "compose a polite reply to the customer email about late shipment",
        "generate a SQL query to find the top 10 customers by lifetime revenue",
        "decode the base64 string and return the JSON payload",
        "write a one-paragraph executive summary of the attached PDF",
        "explain how transformers handle long context windows in plain language",
        "translate this paragraph from English to German",
        "find the bug in the python function and propose a fix",
        "explore the dataset and identify three churn signals",
        "search for recent papers about retrieval-augmented generation",
        "analyze the user behavior log and surface anomalies",
        "rewrite this React component to use server components",
        "the worker exhausted its memory limit while building the segment",
        "reached the request quota for the free tier; upgrading to pro now",
    ]


def make_doc(rng, target_size):
    """Produce a single span doc with ~target_size bytes of prompt+completion."""
    pool = lorem_pool()
    prompt_chunks = []
    completion_chunks = []
    bytes_used = 0
    while bytes_used < target_size:
        p = rng.choice(pool)
        prompt_chunks.append(p)
        c = rng.choice(pool)
        completion_chunks.append(c)
        bytes_used += len(p) + len(c) + 2
    return " ".join(prompt_chunks), " ".join(completion_chunks)


def generate_corpus():
    print(f"Generating {N_DOCS:,} docs × ~{DOC_SIZE_BYTES // 1000} KB ≈ {N_DOCS * DOC_SIZE_BYTES / 1024 / 1024 / 1024:.1f} GB raw...")
    rng = random.Random(42)
    docs = []
    models = ["gpt-4o", "claude-sonnet-4-7", "gpt-5-mini", "haiku-4-5"]
    statuses = ["ok"] * 95 + ["error"] * 4 + ["timeout"]
    spans_per_trace = 12
    n_traces = N_DOCS // spans_per_trace
    for t in range(n_traces):
        trace_id = f"01H{t:026X}"  # ULID-ish
        for s in range(spans_per_trace):
            span_id = f"01H{(t * 100 + s):026X}"
            prompt, completion = make_doc(rng, DOC_SIZE_BYTES // 2)
            docs.append({
                "tenant_id": 0,
                "partition_id": 0,
                "trace_id": trace_id,
                "span_id": span_id,
                "parent_span_id": None,
                "start_time_ms": 1_700_000_000_000 + t * 1000 + s,
                "end_time_ms": 1_700_000_000_000 + t * 1000 + s + 100,
                "duration_ms": 100,
                "span_type": "llm_call",
                "status": rng.choice(statuses),
                "provider": "openai",
                "model": rng.choice(models),
                "tool_name": None,
                "prompt": prompt,
                "completion": completion,
                "prompt_tokens": rng.randint(100, 5000),
                "completion_tokens": rng.randint(100, 5000),
                "cost_usd": rng.uniform(0.0001, 0.05),
                "temperature": 0.7,
                "top_p": 0.9,
                "tool_io_text": None,
                "user_id": f"u-{rng.randint(0, 1000)}",
                "session_id": f"s-{rng.randint(0, 100)}",
                "request_id": f"r-{rng.randint(0, 100000)}",
                "metadata": {"tier": "primary" if t % 2 == 0 else "secondary"},
                "embedding": None,
            })
    return docs, n_traces


def load_zen(docs):
    """Bulk load via /v1/ingest (split into batches)."""
    print("Loading ZenithDB...")
    t0 = time.perf_counter()
    sess = requests.Session()
    BATCH = 50  # 50 docs × 25 KB = 1.25 MB per request
    sent = 0
    for i in range(0, len(docs), BATCH):
        chunk = docs[i:i + BATCH]
        body = {"tenant_id": 0, "partition_id": 0, "spans": chunk}
        r = sess.post(f"{ZEN_URL}/v1/ingest", json=body, timeout=120)
        r.raise_for_status()
        sent += len(chunk)
    elapsed = time.perf_counter() - t0
    print(f"  → {sent:,} docs in {elapsed:.1f}s ({sent/elapsed:.0f} docs/s)")
    print("  Compacting → tier-2 segment...")
    t0 = time.perf_counter()
    r = sess.post(f"{ZEN_URL}/v1/compact", json={"tenant_id": 0, "partition_id": 0}, timeout=600)
    r.raise_for_status()
    print(f"  compact: {r.json()}, {time.perf_counter()-t0:.1f}s")
    r = sess.post(f"{ZEN_URL}/v1/compact-full", json={"tenant_id": 0, "partition_id": 0}, timeout=600)
    r.raise_for_status()
    print(f"  compact-full: {r.json()}, total {time.perf_counter()-t0:.1f}s")


def setup_pg(docs):
    print("Loading Postgres...")
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS spans CASCADE")
    cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    cur.execute("""
        CREATE TABLE spans (
          tenant_id BIGINT, partition_id INT, trace_id TEXT, span_id TEXT,
          parent_span_id TEXT, start_time_ms BIGINT, end_time_ms BIGINT,
          duration_ms BIGINT, span_type TEXT, status TEXT, provider TEXT,
          model TEXT, tool_name TEXT, prompt TEXT, completion TEXT,
          prompt_tokens INT, completion_tokens INT, cost_usd DOUBLE PRECISION,
          temperature DOUBLE PRECISION, top_p DOUBLE PRECISION,
          metadata JSONB
        )
    """)
    rows = []
    for d in docs:
        rows.append((
            d["tenant_id"], d["partition_id"], d["trace_id"], d["span_id"],
            d.get("parent_span_id"), d["start_time_ms"], d["end_time_ms"],
            d["duration_ms"], d["span_type"], d["status"], d["provider"],
            d["model"], d.get("tool_name"), d["prompt"], d["completion"],
            d["prompt_tokens"], d["completion_tokens"], d["cost_usd"],
            d["temperature"], d["top_p"],
            json.dumps(d["metadata"]) if d["metadata"] else None,
        ))
    print("  inserting…")
    t0 = time.perf_counter()
    execute_values(
        cur,
        """INSERT INTO spans (tenant_id, partition_id, trace_id, span_id, parent_span_id,
            start_time_ms, end_time_ms, duration_ms, span_type, status, provider, model,
            tool_name, prompt, completion, prompt_tokens, completion_tokens, cost_usd,
            temperature, top_p, metadata) VALUES %s""",
        rows,
        page_size=500,
    )
    print(f"  insert: {time.perf_counter()-t0:.1f}s")
    print("  building indexes…")
    cur.execute("CREATE INDEX idx_spans_trace_id ON spans(trace_id)")
    cur.execute("CREATE INDEX idx_spans_prompt_trgm ON spans USING gin (prompt gin_trgm_ops)")
    cur.execute("ANALYZE spans")
    cur.close()
    conn.close()
    return PG_DSN


def setup_duckdb(docs):
    print("Loading DuckDB...")
    db = duckdb.connect(":memory:")
    db.execute("""
        CREATE TABLE spans (
          tenant_id BIGINT, partition_id INTEGER, trace_id VARCHAR, span_id VARCHAR,
          parent_span_id VARCHAR, start_time_ms BIGINT, end_time_ms BIGINT,
          duration_ms BIGINT, span_type VARCHAR, status VARCHAR, provider VARCHAR,
          model VARCHAR, tool_name VARCHAR, prompt VARCHAR, completion VARCHAR,
          prompt_tokens INTEGER, completion_tokens INTEGER, cost_usd DOUBLE,
          temperature DOUBLE, top_p DOUBLE, metadata VARCHAR
        )
    """)
    rows = []
    for d in docs:
        rows.append((
            d["tenant_id"], d["partition_id"], d["trace_id"], d["span_id"],
            d.get("parent_span_id"), d["start_time_ms"], d["end_time_ms"],
            d["duration_ms"], d["span_type"], d["status"], d["provider"],
            d["model"], d.get("tool_name"), d["prompt"], d["completion"],
            d["prompt_tokens"], d["completion_tokens"], d["cost_usd"],
            d["temperature"], d["top_p"],
            json.dumps(d["metadata"]) if d["metadata"] else None,
        ))
    t0 = time.perf_counter()
    BATCH = 5000
    for i in range(0, len(rows), BATCH):
        db.executemany(
            "INSERT INTO spans VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows[i:i + BATCH],
        )
    print(f"  insert: {time.perf_counter()-t0:.1f}s")
    return db


def percentiles(times_ms):
    if not times_ms:
        return {"p50": 0, "p95": 0, "p99": 0, "n": 0}
    s = sorted(times_ms)
    n = len(s)
    return {
        "p50": s[n // 2],
        "p95": s[min(int(n * 0.95), n - 1)],
        "p99": s[min(int(n * 0.99), n - 1)],
        "n": n,
    }


# ───────── Span load ────────────────────────────────────────────────────────────

def bench_span_load(target, sample_trace_ids):
    """For each engine, time span_load against the SAME sample of trace_ids."""
    res = {}
    # Zen
    sess = requests.Session()
    times = []
    for tid in sample_trace_ids:
        body = {"tenant_id": 0, "query": f"SELECT span_id, start_time_ms FROM spans WHERE trace_id = '{tid}'"}
        t0 = time.perf_counter()
        sess.post(f"{ZEN_URL}/v1/query", json=body, timeout=30).raise_for_status()
        times.append((time.perf_counter() - t0) * 1000)
    res["Zenith"] = percentiles(times)
    # Postgres
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()
    times = []
    for tid in sample_trace_ids:
        t0 = time.perf_counter()
        cur.execute("SELECT span_id, start_time_ms FROM spans WHERE trace_id = %s", (tid,))
        cur.fetchall()
        times.append((time.perf_counter() - t0) * 1000)
    res["Postgres"] = percentiles(times)
    cur.close()
    conn.close()
    # DuckDB
    times = []
    for tid in sample_trace_ids:
        t0 = time.perf_counter()
        target.execute(f"SELECT span_id, start_time_ms FROM spans WHERE trace_id = '{tid}'").fetchall()
        times.append((time.perf_counter() - t0) * 1000)
    res["DuckDB"] = percentiles(times)
    return res


def bench_fts(duck_db, query="memory"):
    res = {}
    sess = requests.Session()
    body = {"tenant_id": 0, "query": f"SELECT span_id FROM spans WHERE text_match(prompt, '{query}') LIMIT 100"}
    times = []
    for _ in range(N_QUERY_ITERS):
        t0 = time.perf_counter()
        r = sess.post(f"{ZEN_URL}/v1/query", json=body, timeout=120)
        r.raise_for_status()
        times.append((time.perf_counter() - t0) * 1000)
    res["Zenith"] = percentiles(times)
    # Postgres trigram
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()
    times = []
    for _ in range(N_QUERY_ITERS):
        t0 = time.perf_counter()
        cur.execute("SELECT span_id FROM spans WHERE prompt LIKE %s LIMIT 100", (f"%{query}%",))
        cur.fetchall()
        times.append((time.perf_counter() - t0) * 1000)
    res["Postgres"] = percentiles(times)
    cur.close()
    conn.close()
    # DuckDB
    times = []
    for _ in range(N_QUERY_ITERS):
        t0 = time.perf_counter()
        duck_db.execute(f"SELECT span_id FROM spans WHERE prompt LIKE '%{query}%' LIMIT 100").fetchall()
        times.append((time.perf_counter() - t0) * 1000)
    res["DuckDB"] = percentiles(times)
    return res


# ───────── Write latency ────────────────────────────────────────────────────────

def make_write_batch(rng):
    """100 docs × 100 KB each."""
    big = "out of memory " * (WRITE_BATCH_DOC_SIZE // 14)
    big = big[:WRITE_BATCH_DOC_SIZE]
    docs = []
    base_t = int(time.time() * 1000)
    new_trace = f"WRITE-TRACE-{rng.randint(0, 10**9):x}"
    for i in range(WRITE_BATCH_DOCS):
        docs.append({
            "tenant_id": 0,
            "partition_id": 0,
            "trace_id": new_trace,
            "span_id": f"WRITE-SPAN-{rng.randint(0, 10**9):x}-{i}",
            "start_time_ms": base_t + i,
            "end_time_ms": base_t + i + 1,
            "duration_ms": 1,
            "span_type": "llm_call",
            "status": "ok",
            "provider": "openai",
            "model": "gpt-4o",
            "prompt": big,
            "completion": "",
            "metadata": {"writelat_marker": "yes"},
        })
    return docs, new_trace


def bench_write_flush_zen(rng, n=10):
    """Time POST /v1/ingest for 100 × 100 KB docs."""
    times = []
    new_traces = []
    sess = requests.Session()
    for _ in range(n):
        docs, tid = make_write_batch(rng)
        body = {"tenant_id": 0, "partition_id": 0, "spans": docs}
        t0 = time.perf_counter()
        r = sess.post(f"{ZEN_URL}/v1/ingest", json=body, timeout=60)
        r.raise_for_status()
        times.append((time.perf_counter() - t0) * 1000)
        new_traces.append(tid)
    return percentiles(times), new_traces


def bench_write_visible_zen(new_traces, max_wait_s=30):
    """Each new write_flush already returned an ack. Now query for the row;
    Zenith's executor scans unconsumed WAL files for sync visibility, so the
    first SELECT should return immediately. NO COMPACTION FIRST."""
    times = []
    sess = requests.Session()
    for tid in new_traces:
        body = {"tenant_id": 0, "query": f"SELECT span_id FROM spans WHERE trace_id = '{tid}' LIMIT 1"}
        t0 = time.perf_counter()
        deadline = t0 + max_wait_s
        while time.perf_counter() < deadline:
            r = sess.post(f"{ZEN_URL}/v1/query", json=body, timeout=30)
            r.raise_for_status()
            rows = r.json().get("result", {}).get("rows", [])
            if rows:
                times.append((time.perf_counter() - t0) * 1000)
                break
            time.sleep(0.01)
    return percentiles(times)


def bench_write_pg(rng, n=10):
    """Same workload against Postgres."""
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    times = []
    new_traces = []
    for _ in range(n):
        docs, tid = make_write_batch(rng)
        rows = []
        for d in docs:
            rows.append((
                0, 0, d["trace_id"], d["span_id"], None, d["start_time_ms"],
                d["end_time_ms"], 1, "llm_call", "ok", "openai", "gpt-4o",
                None, d["prompt"], "", None, None, None, None, None,
                json.dumps(d["metadata"]),
            ))
        t0 = time.perf_counter()
        execute_values(
            cur,
            """INSERT INTO spans VALUES %s""",
            rows,
            page_size=500,
        )
        times.append((time.perf_counter() - t0) * 1000)
        new_traces.append(tid)
    cur.close()
    conn.close()
    return percentiles(times), new_traces


def bench_write_duck(duck_db, rng, n=10):
    times = []
    new_traces = []
    for _ in range(n):
        docs, tid = make_write_batch(rng)
        rows = []
        for d in docs:
            rows.append((
                0, 0, d["trace_id"], d["span_id"], None, d["start_time_ms"],
                d["end_time_ms"], 1, "llm_call", "ok", "openai", "gpt-4o",
                None, d["prompt"], "", None, None, None, None, None,
                json.dumps(d["metadata"]),
            ))
        t0 = time.perf_counter()
        duck_db.executemany(
            "INSERT INTO spans VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        times.append((time.perf_counter() - t0) * 1000)
        new_traces.append(tid)
    return percentiles(times), new_traces


# ───────── Main ────────────────────────────────────────────────────────────────

def main():
    docs, n_traces = generate_corpus()
    sample_traces = [docs[i * 12]["trace_id"] for i in range(0, n_traces, max(1, n_traces // N_QUERY_ITERS))][:N_QUERY_ITERS]

    load_zen(docs)
    pg_dsn = setup_pg(docs)
    duck_db = setup_duckdb(docs)

    # 1. Span load
    print("\n=== 1. Span load (UI trace inspect) ===")
    span_load = bench_span_load(duck_db, sample_traces)
    for k, v in span_load.items():
        print(f"  {k:10}  p50={v['p50']:.1f} ms  p95={v['p95']:.1f} ms  p99={v['p99']:.1f} ms")

    # 2. FTS
    print("\n=== 2. Full-text search (\"memory\") ===")
    fts = bench_fts(duck_db, "memory")
    for k, v in fts.items():
        print(f"  {k:10}  p50={v['p50']:.1f} ms  p95={v['p95']:.1f} ms  p99={v['p99']:.1f} ms")

    # 3. Write latency
    print("\n=== 3. Write latency (100 × 100 KB docs) ===")
    rng = random.Random(7)
    zen_flush, zen_traces = bench_write_flush_zen(rng, n=10)
    print(f"  Zenith    flush p50={zen_flush['p50']:.1f} ms  p95={zen_flush['p95']:.1f} ms")
    pg_flush, pg_traces = bench_write_pg(rng, n=10)
    print(f"  Postgres  flush p50={pg_flush['p50']:.1f} ms  p95={pg_flush['p95']:.1f} ms")
    duck_flush, duck_traces = bench_write_duck(duck_db, rng, n=10)
    print(f"  DuckDB    flush p50={duck_flush['p50']:.1f} ms  p95={duck_flush['p95']:.1f} ms")

    # 4. Write visibility
    print("\n=== 4. Write visibility (read-after-flush) ===")
    zen_vis = bench_write_visible_zen(zen_traces, max_wait_s=30)
    print(f"  Zenith    visible p50={zen_vis['p50']:.1f} ms  p95={zen_vis['p95']:.1f} ms")
    print("  Postgres  visible == flush (sync writes)")
    print("  DuckDB    visible == flush (sync writes)")

    # Markdown out
    out = []
    out.append("# Brainstore-style benchmark: ZenithDB vs PostgreSQL vs DuckDB\n")
    out.append(f"Corpus: {N_DOCS:,} docs × ~{DOC_SIZE_BYTES // 1000} KB ≈ {N_DOCS * DOC_SIZE_BYTES / 1024 / 1024 / 1024:.1f} GB raw "
               f"(scaled down from Brainstore's 3.9 M × 25 KB ≈ 100 GB).\n")
    out.append("Apple M4 Pro (24 GB RAM) · macOS 26 · Tokio runtime.\n")
    out.append("| Test | Zenith p95 | Postgres p95 | DuckDB p95 |")
    out.append("|---|---:|---:|---:|")
    out.append(f"| Span load (trace inspect) | {span_load['Zenith']['p95']:.1f} ms | {span_load['Postgres']['p95']:.1f} ms | {span_load['DuckDB']['p95']:.1f} ms |")
    out.append(f"| Full-text search 'memory' | {fts['Zenith']['p95']:.1f} ms | {fts['Postgres']['p95']:.1f} ms | {fts['DuckDB']['p95']:.1f} ms |")
    out.append(f"| Write flush (100 × 100 KB) | {zen_flush['p95']:.1f} ms | {pg_flush['p95']:.1f} ms | {duck_flush['p95']:.1f} ms |")
    out.append(f"| Write visible (read-after-flush) | {zen_vis['p95']:.1f} ms | {pg_flush['p95']:.1f} ms (sync) | {duck_flush['p95']:.1f} ms (sync) |")
    out.append("\n## Brainstore reference numbers (their March 2025 post)")
    out.append("\n| Test | Brainstore | 'Popular DW' | Competitor |")
    out.append("|---|---:|---:|---:|")
    out.append("| Span load | 549 ms | 679 ms | 1,160 ms |")
    out.append("| FTS 'memory' | 240 ms | 78,963 ms | 20,789 ms |")
    out.append("| Write flush | 1,780 ms | 331 ms | 4,176 ms |")
    out.append("| Write visible | 1,780 ms | 2,678 ms | 10,412 ms |")
    out.append("\n*Brainstore numbers are at 3.9 M × 25 KB on c7gd.8xlarge. Our numbers are scaled down; see corpus size above.*")
    Path("bench-results/brainstore_compare.md").write_text("\n".join(out) + "\n")
    print("\nwrote bench-results/brainstore_compare.md")


if __name__ == "__main__":
    main()
