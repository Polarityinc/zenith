// ZenithDB TypeScript quickstart.
//
// Hits a running ZenithDB at http://localhost:8080 — ingests one trace
// with two spans, then queries it back via SQL.
//
// Run with Bun (fastest path, no compile step):
//     bun examples/typescript_quickstart.ts
//
// Or Node 20+ with `tsx`:
//     npx tsx examples/typescript_quickstart.ts

const BASE = "http://localhost:8080";
const TENANT_ID = 1;

async function ingestOneTrace(): Promise<void> {
  const traceId = crypto.randomUUID().replace(/-/g, "");
  const parent = crypto.randomUUID().replace(/-/g, "");
  const child = crypto.randomUUID().replace(/-/g, "");
  const now = Date.now();

  const r = await fetch(`${BASE}/v1/ingest`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      tenant_id: TENANT_ID,
      spans: [
        {
          trace_id: traceId,
          span_id: parent,
          parent_span_id: null,
          name: "agent.run",
          start_time_ms: now,
          end_time_ms: now + 320,
          attributes: {
            model: "claude-opus-4-7",
            tokens: 4321,
            status: "ok",
          },
        },
        {
          trace_id: traceId,
          span_id: child,
          parent_span_id: parent,
          name: "tool.web_search",
          start_time_ms: now + 90,
          end_time_ms: now + 210,
          attributes: { tool: "web_search", results: 8 },
        },
      ],
    }),
  });
  if (!r.ok) throw new Error(`ingest failed: ${r.status} ${await r.text()}`);
  console.log(`ingested trace_id=${traceId}  status=${r.status}`);
}

async function queryCount(): Promise<void> {
  const r = await fetch(`${BASE}/v1/query`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      tenant_id: TENANT_ID,
      query:
        "SELECT model, count(*) AS n FROM spans WHERE model IS NOT NULL GROUP BY model",
      dialect: "sql",
    }),
  });
  if (!r.ok) throw new Error(`query failed: ${r.status} ${await r.text()}`);
  console.log(JSON.stringify(await r.json(), null, 2));
}

try {
  await ingestOneTrace();
  await queryCount();
} catch (e) {
  console.error(`error talking to ${BASE}:`, (e as Error).message);
  process.exit(1);
}
