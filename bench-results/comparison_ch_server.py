#!/usr/bin/env python3
"""ClickHouse-server (persistent daemon) leg of the 1M-row comparison.
Loads tenant-0 spans into a MergeTree, then benchmarks B1/B2/B3/B6/B8 over
HTTP — the same transport Zenith uses — so the comparison is fair."""

import json, os, time, sys
from pathlib import Path
import requests

CORPUS = Path("/tmp/zen-corpus-1m.json")
CH_URL = "http://localhost:8123"
ITERATIONS = 100
ZEN_URL = "http://localhost:8080"


def ch(sql, body=None):
    if body is not None:
        r = requests.post(CH_URL, params={"query": sql}, data=body, timeout=600)
    else:
        r = requests.post(CH_URL, data=sql, timeout=600)
    r.raise_for_status()
    return r.text


def percentiles(times_us):
    t = sorted(times_us); n = len(t)
    return {"p50_us": t[int(n*0.5)], "p95_us": t[int(n*0.95)],
            "p99_us": t[min(int(n*0.99), n-1)], "n": n}


def setup():
    ch("DROP TABLE IF EXISTS spans")
    ch("""
        CREATE TABLE spans (
          tenant_id UInt64,
          partition_id UInt32,
          trace_id String,
          span_id String,
          parent_span_id String,
          start_time_ms Int64,
          end_time_ms Int64,
          duration_ms Int64,
          span_type String,
          status String,
          provider String,
          model String,
          tool_name String,
          prompt String,
          completion String,
          prompt_tokens UInt32,
          completion_tokens UInt32,
          cost_usd Float64,
          temperature Float64,
          top_p Float64,
          metadata String
        ) ENGINE = MergeTree()
        ORDER BY (model, status, trace_id)
        SETTINGS index_granularity = 8192
    """)
    print("loading corpus ...", flush=True)
    with open(CORPUS) as f:
        spans = json.load(f)
    spans = [s for s in spans if s["tenant_id"] == 0]
    print(f"  -> {len(spans):,} tenant-0 spans", flush=True)

    # Bulk insert as JSONEachRow over HTTP
    payload = []
    for s in spans:
        payload.append(json.dumps({
            "tenant_id": s["tenant_id"],
            "partition_id": s["partition_id"],
            "trace_id": s.get("trace_id") or "",
            "span_id": s.get("span_id") or "",
            "parent_span_id": s.get("parent_span_id") or "",
            "start_time_ms": s["start_time_ms"],
            "end_time_ms": s["end_time_ms"],
            "duration_ms": s.get("duration_ms") or 0,
            "span_type": s.get("span_type") or "",
            "status": s.get("status") or "",
            "provider": s.get("provider") or "",
            "model": s.get("model") or "",
            "tool_name": s.get("tool_name") or "",
            "prompt": s.get("prompt") or "",
            "completion": s.get("completion") or "",
            "prompt_tokens": s.get("prompt_tokens") or 0,
            "completion_tokens": s.get("completion_tokens") or 0,
            "cost_usd": s.get("cost_usd") or 0.0,
            "temperature": s.get("temperature") or 0.0,
            "top_p": s.get("top_p") or 0.0,
            "metadata": json.dumps(s.get("metadata")) if s.get("metadata") is not None else "",
        }))
    body = ("\n".join(payload) + "\n").encode()
    print(f"  payload: {len(body)/1024/1024:.0f} MB", flush=True)
    t0 = time.perf_counter()
    ch("INSERT INTO spans FORMAT JSONEachRow", body=body)
    print(f"  inserted in {time.perf_counter()-t0:.1f}s", flush=True)
    # Quick sanity
    cnt = ch("SELECT count() FROM spans").strip()
    print(f"  count() = {cnt}", flush=True)


def get_trace_id():
    r = requests.post(f"{ZEN_URL}/v1/query",
                      json={"tenant_id": 0, "query": "SELECT trace_id FROM spans LIMIT 1"},
                      timeout=10)
    r.raise_for_status()
    rows = r.json().get("result", {}).get("rows", [])
    return rows[0]["fields"].get("trace_id") if rows else None


def bench(sql):
    s = requests.Session()
    for _ in range(5):
        s.post(CH_URL, data=sql, timeout=30).raise_for_status()
    times = []
    for _ in range(ITERATIONS):
        t0 = time.perf_counter()
        r = s.post(CH_URL, data=sql, timeout=30); r.raise_for_status(); r.text
        times.append((time.perf_counter() - t0) * 1_000_000)
    return percentiles(times)


def main():
    setup()
    tid = get_trace_id()
    queries = [
        ("B1_trace_load", f"SELECT span_id, model, start_time_ms FROM spans WHERE trace_id = '{tid}' FORMAT TabSeparated"),
        ("B2_attr_filter", "SELECT span_id, model, duration_ms FROM spans WHERE model = 'gpt-4o' AND status = 'error' LIMIT 100 FORMAT TabSeparated"),
        ("B3_fts_memory", "SELECT span_id, prompt FROM spans WHERE prompt LIKE '%memory%' LIMIT 100 FORMAT TabSeparated"),
        ("B6_jsonpath", "SELECT span_id FROM spans WHERE JSONExtractString(metadata, 'tier') = 'primary' LIMIT 100 FORMAT TabSeparated"),
        ("B8_group_by_model", "SELECT model, count() FROM spans GROUP BY model FORMAT TabSeparated"),
    ]
    print(f"\nRunning {ITERATIONS} iters per query against clickhouse-server (HTTP)...\n", flush=True)
    out = {}
    for name, sql in queries:
        p = bench(sql)
        out[name] = p
        print(f"  {name:22}  p50={p['p50_us']:>7.0f}µs  p95={p['p95_us']:>7.0f}µs  p99={p['p99_us']:>7.0f}µs", flush=True)
    with open("/tmp/zen-bench-data/ch-server-results.json", "w") as f:
        json.dump(out, f, indent=2)
    print("wrote /tmp/zen-bench-data/ch-server-results.json", flush=True)


if __name__ == "__main__":
    main()
