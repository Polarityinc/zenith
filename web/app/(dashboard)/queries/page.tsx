import {
  getMetrics,
  histogramQuantile,
  rate,
  tenantsFromMetrics,
  type Metric,
} from "@/lib/zenith";
import { fmtMs, fmtNum } from "../../components/format";
import {
  PageHeader,
  SectionHeader,
  Stat,
  Empty,
} from "../../components/dashboard";
import { QueryRunner } from "./runner";

export const dynamic = "force-dynamic";

function sumMetric(
  metrics: Metric[],
  name: string,
  filter: (l: Record<string, string>) => boolean = () => true,
): number {
  return metrics
    .filter((m) => m.name === name && filter(m.labels))
    .reduce((a, b) => a + b.value, 0);
}

export default async function Page() {
  const metrics = await getMetrics();
  const m = metrics ?? [];
  const tenantIds = tenantsFromMetrics(m);
  const queryRates = rate(m, "queries-page");

  const total = sumMetric(m, "zen_queries_total");
  const errors = sumMetric(
    m,
    "zen_queries_total",
    (l) => l.status === "error",
  );
  const p50 = histogramQuantile(m, "zen_query_duration_seconds", 0.5);
  const p95 = histogramQuantile(m, "zen_query_duration_seconds", 0.95);
  const p99 = histogramQuantile(m, "zen_query_duration_seconds", 0.99);

  return (
    <div className="flex flex-col gap-5">
      <PageHeader
        title="Queries"
        subtitle="Counters from /v1/metrics, latency from histograms. Run a query against /v1/query directly below."
      />

      <section className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-px bg-border">
        <Stat label="QUERIES" value={fmtNum(total)} unit="total" />
        <Stat
          label="ERROR RATE"
          value={total > 0 ? `${((errors / total) * 100).toFixed(2)}%` : "—"}
          sub={`${fmtNum(errors)} errors`}
        />
        <Stat label="P50" value={fmtMs(p50)} sub={`p99 ${fmtMs(p99)}`} />
        <Stat label="P95" value={fmtMs(p95)} />
      </section>

      <section className="border border-border p-4">
        <div className="text-xs mb-3">Run a query</div>
        <QueryRunner defaultTenantId={tenantIds[0]} />
      </section>

      <section className="border border-border">
        <SectionHeader title="By tenant" count={tenantIds.length} />
        {tenantIds.length === 0 ? (
          <Empty msg="No tenant labels found in /v1/metrics yet." />
        ) : (
          <>
            <div className="grid grid-cols-[200px_120px_120px_120px_120px] text-2xs text-white/40 px-4 py-2 border-b border-border">
              <span>TENANT_ID</span>
              <span className="text-right">QUERIES</span>
              <span className="text-right">QPS</span>
              <span className="text-right">ERRORS</span>
              <span className="text-right">ERR %</span>
            </div>
            {tenantIds.map((tid, i, arr) => {
              const tQ = sumMetric(
                m,
                "zen_queries_total",
                (l) => l.tenant === tid,
              );
              const tErr = sumMetric(
                m,
                "zen_queries_total",
                (l) => l.tenant === tid && l.status === "error",
              );
              const qps = [...queryRates.entries()]
                .filter(
                  ([k]) =>
                    k.startsWith("zen_queries_total|") &&
                    k.includes(`"${tid}"`),
                )
                .reduce((a, [, v]) => a + v, 0);
              return (
                <div
                  key={tid}
                  className={`grid grid-cols-[200px_120px_120px_120px_120px] items-center px-4 py-2 text-xs hover:bg-white/[0.02] ${
                    i !== arr.length - 1 ? "border-b border-border" : ""
                  }`}
                >
                  <span className="text-white/80">{tid}</span>
                  <span className="text-right text-white/70">{fmtNum(tQ)}</span>
                  <span className="text-right text-white/70">
                    {qps > 0 ? qps.toFixed(1) : "—"}
                  </span>
                  <span className="text-right text-white/70">{fmtNum(tErr)}</span>
                  <span className="text-right text-white/70">
                    {tQ > 0 ? `${((tErr / tQ) * 100).toFixed(2)}%` : "—"}
                  </span>
                </div>
              );
            })}
          </>
        )}
      </section>
    </div>
  );
}
