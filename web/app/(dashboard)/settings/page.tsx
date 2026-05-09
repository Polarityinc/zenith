import { ZENITH_BASE, getHealth, getReady } from "@/lib/zenith";
import { PageHeader, SectionHeader } from "../../components/dashboard";
import { StatusDot } from "../../components/format";

export const dynamic = "force-dynamic";

export default async function Page() {
  const [health, ready] = await Promise.all([getHealth(), getReady()]);

  const rows: { k: string; v: React.ReactNode }[] = [
    {
      k: "ZENITH_URL",
      v: <span className="text-white/80">{ZENITH_BASE}</span>,
    },
    {
      k: "Liveness",
      v: (
        <span className="inline-flex items-center gap-1.5">
          <StatusDot kind={health?.ok ? "ok" : "err"} />
          {health?.ok ? "ok" : "down"} · GET /v1/healthz
        </span>
      ),
    },
    {
      k: "Readiness",
      v: (
        <span className="inline-flex items-center gap-1.5">
          <StatusDot kind={ready?.ok ? "ok" : "warn"} />
          {ready?.ok ? "ok" : "down"} · GET /v1/readyz
        </span>
      ),
    },
    {
      k: "Catalog",
      v: ready?.checks.catalog ?? "—",
    },
    {
      k: "OpenAPI",
      v: (
        <a
          href={`${ZENITH_BASE}/v1/openapi.json`}
          className="text-white/80 underline underline-offset-2 hover:text-white"
        >
          {ZENITH_BASE}/v1/openapi.json
        </a>
      ),
    },
    {
      k: "Dashboard build",
      v: <span className="text-white/60">Next.js 15 · Geist Sans</span>,
    },
  ];

  return (
    <div className="flex flex-col gap-5">
      <PageHeader
        title="Settings"
        subtitle="Read-only view of how the dashboard is wired. Set ZENITH_URL in your environment to point at a different engine."
      />

      <section className="border border-border">
        <SectionHeader title="Connection" />
        <div>
          {rows.map((r, i, arr) => (
            <div
              key={r.k}
              className={`grid grid-cols-[200px_1fr] items-center px-4 py-3 text-xs ${
                i !== arr.length - 1 ? "border-b border-border" : ""
              }`}
            >
              <span className="text-2xs uppercase tracking-wider text-white/40">
                {r.k}
              </span>
              <span className="text-white/70">{r.v}</span>
            </div>
          ))}
        </div>
      </section>

      <section className="border border-border">
        <SectionHeader title="Endpoints in use" />
        <div className="px-4 py-3 text-xs text-white/70 leading-relaxed">
          <ul className="space-y-1.5">
            <li>
              <span className="text-white/80">GET /v1/healthz</span> — liveness
              probe
            </li>
            <li>
              <span className="text-white/80">GET /v1/readyz</span> — readiness
              (catalog reachable)
            </li>
            <li>
              <span className="text-white/80">GET /v1/metrics</span> — Prometheus
              text exposition; parsed in <code>lib/zenith.ts</code>
            </li>
            <li>
              <span className="text-white/80">
                GET /v1/segments?tenant_id=…
              </span>{" "}
              — segment list (object_key, row_count, byte_count, time_min,
              time_max)
            </li>
            <li>
              <span className="text-white/80">POST /v1/query</span> — proxied via{" "}
              <code>/api/query</code> from the Queries page
            </li>
            <li>
              <span className="text-white/80">POST /v1/compact</span> — proxied
              via <code>/api/compact</code> from the Compactions page
            </li>
          </ul>
        </div>
      </section>
    </div>
  );
}
