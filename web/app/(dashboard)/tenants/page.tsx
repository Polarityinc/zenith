import {
  getMetrics,
  getSegments,
  rate,
  tenantsFromMetrics,
  type Metric,
} from "@/lib/zenith";
import { fmtBytes, fmtNum } from "../../components/format";
import { PageHeader, SectionHeader, Empty } from "../../components/dashboard";

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
  const queryRates = rate(m, "queries-tenants-page");

  const lists = await Promise.all(
    tenantIds.map((id) => getSegments(id).then((s) => [id, s ?? []] as const)),
  );

  return (
    <div className="flex flex-col gap-5">
      <PageHeader
        title="Tenants"
        subtitle="Discovered from tenant labels on /v1/metrics; storage from /v1/segments per tenant."
      />

      <section className="border border-border">
        <SectionHeader title="All tenants" count={tenantIds.length} />
        {tenantIds.length === 0 ? (
          <Empty msg="No tenant labels found yet. Issue a query or ingest." />
        ) : (
          <>
            <div className="grid grid-cols-[200px_120px_100px_120px_120px_140px_1fr] text-2xs text-white/40 px-4 py-2 border-b border-border">
              <span>TENANT_ID</span>
              <span className="text-right">QUERIES</span>
              <span className="text-right">QPS</span>
              <span className="text-right">SEGMENTS</span>
              <span className="text-right">ROWS</span>
              <span className="text-right">STORAGE</span>
              <span>OLDEST → NEWEST</span>
            </div>
            {lists.map(([tid, segs], i, arr) => {
              const queries = sumMetric(
                m,
                "zen_queries_total",
                (l) => l.tenant === tid,
              );
              const qps = [...queryRates.entries()]
                .filter(
                  ([k]) =>
                    k.startsWith("zen_queries_total|") &&
                    k.includes(`"${tid}"`),
                )
                .reduce((a, [, v]) => a + v, 0);
              const sBytes = segs.reduce((a, b) => a + (b.byte_count ?? 0), 0);
              const sRows = segs.reduce((a, b) => a + (b.row_count ?? 0), 0);
              const sortedByTime = [...segs].sort((a, b) =>
                (a.time_min ?? "").localeCompare(b.time_min ?? ""),
              );
              const oldest = sortedByTime[0]?.time_min ?? "—";
              const newest =
                sortedByTime[sortedByTime.length - 1]?.time_max ?? "—";
              return (
                <div
                  key={tid}
                  className={`grid grid-cols-[200px_120px_100px_120px_120px_140px_1fr] items-center px-4 py-2.5 text-xs hover:bg-white/[0.02] ${
                    i !== arr.length - 1 ? "border-b border-border" : ""
                  }`}
                >
                  <span className="text-white/80">{tid}</span>
                  <span className="text-right text-white/70">
                    {fmtNum(queries)}
                  </span>
                  <span className="text-right text-white/70">
                    {qps > 0 ? qps.toFixed(1) : "—"}
                  </span>
                  <span className="text-right text-white/70">{segs.length}</span>
                  <span className="text-right text-white/70">{fmtNum(sRows)}</span>
                  <span className="text-right text-white/70">
                    {fmtBytes(sBytes)}
                  </span>
                  <span className="text-2xs text-white/50">
                    {oldest.slice(0, 19)} → {newest.slice(0, 19)}
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
