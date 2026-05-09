import {
  getMetrics,
  getSegments,
  tenantsFromMetrics,
} from "@/lib/zenith";
import { fmtBytes, fmtNum, fmtTime } from "../../components/format";
import { PageHeader, SectionHeader, Empty } from "../../components/dashboard";

export const dynamic = "force-dynamic";

export default async function Page() {
  const metrics = await getMetrics();
  const tenantIds = tenantsFromMetrics(metrics ?? []);

  const lists = await Promise.all(
    tenantIds.map((id) => getSegments(id).then((s) => [id, s ?? []] as const)),
  );

  const all = lists
    .flatMap(([tenant, list]) => list.map((seg) => ({ tenant, seg })))
    .sort((a, b) =>
      (b.seg.time_max ?? "").localeCompare(a.seg.time_max ?? ""),
    );

  const totalRows = all.reduce((a, b) => a + (b.seg.row_count ?? 0), 0);
  const totalBytes = all.reduce((a, b) => a + (b.seg.byte_count ?? 0), 0);

  return (
    <div className="flex flex-col gap-5">
      <PageHeader
        title="Segments"
        subtitle={`${all.length} segments · ${fmtNum(totalRows)} rows · ${fmtBytes(totalBytes)} across ${tenantIds.length} tenant${tenantIds.length === 1 ? "" : "s"}.`}
      />

      <section className="border border-border">
        <SectionHeader title="All segments" count={all.length} />
        {all.length === 0 ? (
          <Empty msg="No segments yet. After ingest + a flush, compacted segments show up here." />
        ) : (
          <>
            <div className="grid grid-cols-[1fr_140px_100px_100px_180px_180px_140px] text-2xs text-white/40 px-4 py-2 border-b border-border">
              <span>OBJECT_KEY</span>
              <span>TENANT</span>
              <span className="text-right">ROWS</span>
              <span className="text-right">SIZE</span>
              <span>TIME_MIN</span>
              <span>TIME_MAX</span>
              <span>SEGMENT_ID</span>
            </div>
            {all.map(({ tenant, seg }, i, arr) => (
              <div
                key={seg.segment_id}
                className={`grid grid-cols-[1fr_140px_100px_100px_180px_180px_140px] items-center px-4 py-2 text-xs hover:bg-white/[0.02] ${
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
                <span className="text-white/40 text-2xs truncate">
                  {seg.segment_id}
                </span>
              </div>
            ))}
          </>
        )}
      </section>
    </div>
  );
}
