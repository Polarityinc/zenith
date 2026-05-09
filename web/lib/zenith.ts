// Real client for the ZenithDB HTTP API.
//
// Endpoints (from crates/zen_server/src/http.rs):
//   GET  /v1/healthz                 — liveness
//   GET  /v1/readyz                  — readiness (catalog reachable)
//   GET  /v1/metrics                 — Prometheus text exposition
//   GET  /v1/segments?tenant_id=N    — segment list for a tenant
//   POST /v1/query                   — SQL / ZQL
//
// Configure via ZENITH_URL env var; defaults to http://localhost:8080.

const BASE = process.env.ZENITH_URL ?? "http://localhost:8080";
const TIMEOUT_MS = 1500;

async function safe<T>(f: () => Promise<T>): Promise<T | null> {
  try {
    return await f();
  } catch {
    return null;
  }
}

async function get(path: string, init?: RequestInit): Promise<Response> {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), TIMEOUT_MS);
  try {
    return await fetch(`${BASE}${path}`, {
      ...init,
      signal: ctl.signal,
      cache: "no-store",
    });
  } finally {
    clearTimeout(t);
  }
}

export type Health = { ok: boolean };
export type Ready = { ok: boolean; checks: Record<string, string> };

export async function getHealth(): Promise<Health | null> {
  return safe(async () => {
    const r = await get("/v1/healthz");
    if (!r.ok) return { ok: false };
    const j = (await r.json()) as { status?: string };
    return { ok: j.status === "ok" };
  });
}

export async function getReady(): Promise<Ready | null> {
  return safe(async () => {
    const r = await get("/v1/readyz");
    const j = (await r.json()) as { status?: string; checks?: Record<string, string> };
    return { ok: r.ok && j.status === "ok", checks: j.checks ?? {} };
  });
}

// ─── Prometheus parser ────────────────────────────────────────────────────

export type Metric = {
  name: string;
  labels: Record<string, string>;
  value: number;
};

export function parseProm(text: string): Metric[] {
  const out: Metric[] = [];
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    if (!line || line.startsWith("#")) continue;
    // name{label="v",...} value
    const m = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{([^}]*)\})?\s+(\S+)/);
    if (!m) continue;
    const name = m[1];
    const labelsRaw = m[3] ?? "";
    const value = Number(m[4]);
    if (!Number.isFinite(value)) continue;
    const labels: Record<string, string> = {};
    if (labelsRaw) {
      for (const kv of labelsRaw.split(/,(?=(?:[^"]*"[^"]*")*[^"]*$)/)) {
        const eq = kv.indexOf("=");
        if (eq < 0) continue;
        const k = kv.slice(0, eq).trim();
        const v = kv
          .slice(eq + 1)
          .trim()
          .replace(/^"(.*)"$/, "$1");
        labels[k] = v;
      }
    }
    out.push({ name, labels, value });
  }
  return out;
}

export async function getMetrics(): Promise<Metric[] | null> {
  return safe(async () => {
    const r = await get("/v1/metrics");
    if (!r.ok) throw new Error("metrics not ok");
    const text = await r.text();
    return parseProm(text);
  });
}

// ─── Rate cache (process-local) ───────────────────────────────────────────
// Counters are monotonic; rate = Δvalue / Δt.

type Snap = { ts: number; values: Map<string, number> };
const snaps: Map<string, Snap> = new Map();

function key(name: string, labels: Record<string, string>): string {
  return name + "|" + JSON.stringify(labels);
}

export function rate(metrics: Metric[], scope: string): Map<string, number> {
  const now = Date.now();
  const cur = new Map<string, number>();
  for (const m of metrics) cur.set(key(m.name, m.labels), m.value);
  const prev = snaps.get(scope);
  snaps.set(scope, { ts: now, values: cur });
  const out = new Map<string, number>();
  if (!prev) return out;
  const dt = (now - prev.ts) / 1000;
  if (dt < 0.5 || dt > 60) return out;
  for (const [k, v] of cur) {
    const p = prev.values.get(k);
    if (p === undefined) continue;
    out.set(k, Math.max(0, (v - p) / dt));
  }
  return out;
}

// ─── Histogram quantile (linear-interp on bucket boundaries) ─────────────

export function histogramQuantile(
  metrics: Metric[],
  metricName: string,
  q: number,
  filter: (labels: Record<string, string>) => boolean = () => true,
): number | null {
  // Aggregate across all matching label sets that are NOT the `le` label.
  type Bucket = { le: number; count: number };
  const groups: Map<string, Bucket[]> = new Map();
  for (const m of metrics) {
    if (m.name !== `${metricName}_bucket`) continue;
    if (!filter(m.labels)) continue;
    const le = m.labels["le"] === "+Inf" ? Infinity : Number(m.labels["le"]);
    if (!Number.isFinite(le) && le !== Infinity) continue;
    const groupLabels = { ...m.labels };
    delete groupLabels["le"];
    const k = JSON.stringify(groupLabels);
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k)!.push({ le, count: m.value });
  }
  // Sum the buckets across groups (filter-respected aggregate).
  const merged: Map<number, number> = new Map();
  for (const buckets of groups.values()) {
    for (const b of buckets) {
      merged.set(b.le, (merged.get(b.le) ?? 0) + b.count);
    }
  }
  if (merged.size < 2) return null;
  const sorted = [...merged.entries()].sort((a, b) => a[0] - b[0]);
  const total = sorted[sorted.length - 1][1];
  if (total <= 0) return null;
  const target = q * total;
  let prevLe = 0;
  let prevCount = 0;
  for (const [le, count] of sorted) {
    if (count >= target) {
      if (le === Infinity) return prevLe;
      const frac =
        count === prevCount ? 1 : (target - prevCount) / (count - prevCount);
      return prevLe + (le - prevLe) * frac;
    }
    prevLe = le;
    prevCount = count;
  }
  return null;
}

// ─── Segments ─────────────────────────────────────────────────────────────

export type Segment = {
  segment_id: string;
  object_key: string;
  row_count: number;
  byte_count: number;
  time_min: string;
  time_max: string;
};

export async function getSegments(tenantId: string): Promise<Segment[] | null> {
  return safe(async () => {
    const r = await get(`/v1/segments?tenant_id=${encodeURIComponent(tenantId)}`);
    if (!r.ok) throw new Error("segments not ok");
    const j = (await r.json()) as { segments?: Segment[] };
    return j.segments ?? [];
  });
}

// ─── Tenant discovery from metrics ─────────────────────────────────────────

export function tenantsFromMetrics(metrics: Metric[]): string[] {
  const set = new Set<string>();
  for (const m of metrics) {
    const t = m.labels["tenant"];
    if (t && t !== "" && t !== "0") set.add(t);
  }
  return [...set].sort();
}

export const ZENITH_BASE = BASE;
