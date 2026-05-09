import {
  getMetrics,
  getSegments,
  histogramQuantile,
  rate,
  tenantsFromMetrics,
  type Metric,
} from "@/lib/zenith";
import { fmtBytes, fmtMs, fmtNum, fmtTime } from "../components/format";
import {
  PageHeader,
  SectionHeader,
  Stat,
  Empty,
} from "../components/dashboard";

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

  const queriesTotal = sumMetric(m, "zen_queries_total");
  const ingestRowsTotal = sumMetric(m, "zen_ingest_rows_total");
  const queryRates = rate(m, "queries");
  const ingestRates = rate(m, "ingest");
  const queriesPerSec = [...queryRates.entries()]
    .filter(([k]) => k.startsWith("zen_queries_total|"))
    .reduce((a, [, v]) => a + v, 0);
  const rowsPerSec = [...ingestRates.entries()]
    .filter(([k]) => k.startsWith("zen_ingest_rows_total|"))
    .reduce((a, [, v]) => a + v, 0);

  const queryP50 = histogramQuantile(m, "zen_query_duration_seconds", 0.5);
  const queryP95 = histogramQuantile(m, "zen_query_duration_seconds", 0.95);
  const ingestP95 = histogramQuantile(m, "zen_ingest_duration_seconds", 0.95);

  const tenantIds = tenantsFromMetrics(m);
  const tenantSegmentLists = await Promise.all(
    tenantIds
      .slice(0, 8)
      .map((id) => getSegments(id).then((s) => [id, s ?? []] as const)),
  );

  const totalSegments = tenantSegmentLists.reduce(
    (a, [, list]) => a + list.length,
    0,
  );
  const totalBytes = tenantSegmentLists.reduce(
    (a, [, list]) => a + list.reduce((aa, s) => aa + (s.byte_count ?? 0), 0),
    0,
  );
  const totalRows = tenantSegmentLists.reduce(
    (a, [, list]) => a + list.reduce((aa, s) => aa + (s.row_count ?? 0), 0),
    0,
  );

  const allSegments = tenantSegmentLists
    .flatMap(([tenant, list]) => list.map((seg) => ({ tenant, seg })))
    .sort((a, b) =>
      (b.seg.time_max ?? "").localeCompare(a.seg.time_max ?? ""),
    )
    .slice(0, 10);

  return (
    <div className="flex flex-col gap-5">
      <PageHeader title="Overview" subtitle="Live state of the engine." />

      <section className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-px bg-border">
        <Stat
          label="QUERIES"
          value={fmtNum(queriesTotal)}
          unit="total"
          sub={
            queriesPerSec > 0
              ? `${queriesPerSec.toFixed(1)} per second`
              : "warming up rate…"
          }
        />
        <Stat
          label="QUERY P95"
          value={fmtMs(queryP95)}
          sub={`p50 ${fmtMs(queryP50)}`}
        />
        <Stat
          label="INGEST ROWS"
          value={fmtNum(ingestRowsTotal)}
          unit="total"
          sub={
            rowsPerSec > 0
              ? `${fmtNum(rowsPerSec)} rows / s`
              : `ingest p95 ${fmtMs(ingestP95)}`
          }
        />
        <Stat
          label="STORAGE"
          value={fmtBytes(totalBytes)}
          sub={`${fmtNum(totalRows)} rows · ${totalSegments} segments`}
        />
      </section>

      <section className="border border-border">
        <SectionHeader title="Tenants" count={tenantIds.length} href="/tenants" />
        {tenantIds.length === 0 ? (
          <Empty msg="No tenant labels found in /v1/metrics. Issue a query or ingest to populate." />
        ) : (
          <>
            <div className="grid grid-cols-[160px_100px_90px_100px_100px_120px] text-2xs text-white/40 px-4 py-2 border-b border-border">
              <span>TENANT_ID</span>
              <span className="text-right">QUERIES</span>
              <span className="text-right">QPS</span>
              <span className="text-right">SEGMENTS</span>
              <span className="text-right">ROWS</span>
              <span className="text-right">STORAGE</span>
            </div>
            {tenantSegmentLists.map(([tid, segs], i, arr) => {
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
              return (
                <div
                  key={tid}
                  className={`grid grid-cols-[160px_100px_90px_100px_100px_120px] items-center px-4 py-2 text-xs hover:bg-white/[0.02] ${
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
                </div>
              );
            })}
          </>
        )}
      </section>

      <section className="border border-border">
        <SectionHeader
          title="Recent segments"
          count={allSegments.length}
          href="/segments"
        />
        {allSegments.length === 0 ? (
          <Empty msg="No segments yet. After ingest + a flush, compacted segments show up here." />
        ) : (
          <>
            <div className="grid grid-cols-[1fr_140px_100px_100px_160px_160px] text-2xs text-white/40 px-4 py-2 border-b border-border">
              <span>OBJECT_KEY</span>
              <span>TENANT</span>
              <span className="text-right">ROWS</span>
              <span className="text-right">SIZE</span>
              <span>TIME_MIN</span>
              <span>TIME_MAX</span>
            </div>
            {allSegments.map(({ tenant, seg }, i, arr) => (
              <div
                key={seg.segment_id}
                className={`grid grid-cols-[1fr_140px_100px_100px_160px_160px] items-center px-4 py-2 text-xs hover:bg-white/[0.02] ${
                  i !== arr.length - 1 ? "border-b border-border" : ""
                }`}
              >
                <span className="text-white/80 truncate">{seg.object_key}</span>
                <span className="text-white/60">{tenant}</span>
                <span className="text-right text-white/70">
                  {fmtNum(seg.row_count)}
                </span>
                <span className="text-right text-white/70">
                  {fmtBytes(seg.byte_count)}
                </span>
                <span className="text-white/50 text-2xs">
                  {fmtTime(seg.time_min)}
                </span>
                <span className="text-white/50 text-2xs">
                  {fmtTime(seg.time_max)}
                </span>
              </div>
            ))}
          </>
        )}
      </section>
    </div>
  );
}

