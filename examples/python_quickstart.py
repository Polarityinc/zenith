#!/usr/bin/env python3
"""
ZenithDB Python quickstart.

Hits a running ZenithDB at http://localhost:8080 — ingests one trace
with two spans, then queries it back via SQL.

Run:
    pip install requests
    python examples/python_quickstart.py
"""

from __future__ import annotations

import json
import sys
import time
import uuid

import requests

BASE = "http://localhost:8080"
TENANT_ID = 1


def ingest_one_trace() -> None:
    trace_id = uuid.uuid4().hex
    parent = uuid.uuid4().hex
    child = uuid.uuid4().hex
    now = int(time.time() * 1000)

    payload = {
        "tenant_id": TENANT_ID,
        "spans": [
            {
                "trace_id": trace_id,
                "span_id": parent,
                "parent_span_id": None,
                "name": "agent.run",
                "start_time_ms": now,
                "end_time_ms": now + 320,
                "attributes": {
                    "model": "claude-opus-4-7",
                    "tokens": 4321,
                    "status": "ok",
                },
            },
            {
                "trace_id": trace_id,
                "span_id": child,
                "parent_span_id": parent,
                "name": "tool.web_search",
                "start_time_ms": now + 90,
                "end_time_ms": now + 210,
                "attributes": {"tool": "web_search", "results": 8},
            },
        ],
    }

    r = requests.post(f"{BASE}/v1/ingest", json=payload, timeout=5)
    r.raise_for_status()
    print(f"ingested trace_id={trace_id}  status={r.status_code}")


def query_count() -> None:
    body = {
        "tenant_id": TENANT_ID,
        "query": "SELECT model, count(*) AS n "
                 "FROM spans WHERE model IS NOT NULL GROUP BY model",
        "dialect": "sql",
    }
    r = requests.post(f"{BASE}/v1/query", json=body, timeout=5)
    r.raise_for_status()
    result = r.json()
    print(json.dumps(result, indent=2))


def main() -> int:
    try:
        ingest_one_trace()
        query_count()
        return 0
    except requests.RequestException as e:
        print(f"error talking to {BASE}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
