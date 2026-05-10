import {
  getMetrics,
  getSegments,
  tenantsFromMetrics,
  type Metric,
} from "@/lib/zenith";
import { fmtBytes, fmtNum, fmtTime } from "../../components/format";
import {
  PageHeader,
  SectionHeader,
  Empty,
  Stat,
} from "../../components/dashboard";
import { CompactionTrigger } from "./trigger";

export const dynamic = "force-dynamic";

function sumMetric(metrics: Metric[], name: string): number {
  return metrics.filter((m) => m.name === name).reduce((a, b) => a + b.value, 0);
}

export default async function Page() {
  const metrics = await getMetrics();
  const m = metrics ?? [];
  const tenantIds = tenantsFromMetrics(m);

  const lists = await Promise.all(
    tenantIds.map((id) => getSegments(id).then((s) => [id, s ?? []] as const)),
  );

  const allSegs = lists.flatMap(([tenant, list]) =>
    list.map((seg) => ({ tenant, seg })),
  );

  // Tiny segments are compaction candidates. Sort by size ascending,
  // group by tenant.
  const candidatesByTenant = new Map<
    string,
    { count: number; bytes: number; rows: number }
  >();
  for (const { tenant, seg } of allSegs) {
    if ((seg.byte_count ?? 0) < 64 * 1024 * 1024) {
      const cur = candidatesByTenant.get(tenant) ?? {
        count: 0,
        bytes: 0,
        rows: 0,
      };
      cur.count++;
      cur.bytes += seg.byte_count ?? 0;
      cur.rows += seg.row_count ?? 0;
      candidatesByTenant.set(tenant, cur);
    }
  }

  const compactionsTotal = sumMetric(m, "zen_compactions_total");
  const compactionBytes = sumMetric(m, "zen_compaction_bytes_total");

  return (
    <div className="flex flex-col gap-5">
      <PageHeader
        title="Compactions"
        subtitle="Trigger a manual compaction via /v1/compact. The background compactor also runs on size + age thresholds."
      />

      <section className="grid grid-cols-1 md:grid-cols-3 gap-px bg-border">
        <Stat
          label="COMPACTIONS"
          value={fmtNum(compactionsTotal)}
          sub="/v1/metrics counter"
        />
        <Stat
          label="MERGED"
          value={fmtBytes(compactionBytes)}
          sub="bytes through compactor"
        />
        <Stat
          label="CANDIDATES"
          value={String(
            [...candidatesByTenant.values()].reduce((a, b) => a + b.count, 0),
          )}
          sub="< 64 MiB segments"
        />
      </section>

      <section className="border border-border p-4">
        <div className="text-xs mb-3">Trigger compaction</div>
        <CompactionTrigger defaultTenantId={tenantIds[0]} />
      </section>

      <section className="border border-border">
        <SectionHeader
          title="Compaction candidates"
          count={candidatesByTenant.size}
        />
        {candidatesByTenant.size === 0 ? (
          <Empty msg="No segments under 64 MiB. Either nothing has been ingested yet, or the background compactor is keeping up." />
        ) : (
          <>
            <div className="grid grid-cols-[200px_120px_140px_140px] text-2xs text-white/40 px-4 py-2 border-b border-border">
              <span>TENANT_ID</span>
              <span className="text-right">SEGMENTS</span>
              <span className="text-right">ROWS</span>
              <span className="text-right">BYTES</span>
            </div>
            {[...candidatesByTenant.entries()].map(([tid, c], i, arr) => (
              <div
                key={tid}
                className={`grid grid-cols-[200px_120px_140px_140px] items-center px-4 py-2 text-xs hover:bg-white/[0.02] ${
                  i !== arr.length - 1 ? "border-b border-border" : ""
                }`}
              >
                <span className="text-white/80">{tid}</span>
                <span className="text-right text-white/70">{c.count}</span>
                <span className="text-right text-white/70">
                  {fmtNum(c.rows)}
                </span>
                <span className="text-right text-white/70">
                  {fmtBytes(c.bytes)}
                </span>
              </div>
            ))}
          </>
        )}
      </section>

      <section className="border border-border">
        <SectionHeader title="All segments" count={allSegs.length} />
        {allSegs.length === 0 ? (
          <Empty msg="No segments yet." />
        ) : (
          <>
            <div className="grid grid-cols-[1fr_140px_100px_100px_180px] text-2xs text-white/40 px-4 py-2 border-b border-border">
              <span>OBJECT_KEY</span>
              <span>TENANT</span>
              <span className="text-right">ROWS</span>
              <span className="text-right">SIZE</span>
              <span>WRITTEN</span>
            </div>
            {allSegs
              .sort((a, b) =>
                (b.seg.time_max ?? "").localeCompare(a.seg.time_max ?? ""),
              )
              .slice(0, 30)
              .map(({ tenant, seg }, i, arr) => (
                <div
                  key={seg.segment_id}
                  className={`grid grid-cols-[1fr_140px_100px_100px_180px] items-center px-4 py-2 text-xs hover:bg-white/[0.02] ${
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

