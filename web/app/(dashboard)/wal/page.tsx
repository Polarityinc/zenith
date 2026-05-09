import { getMetrics, type Metric } from "@/lib/zenith";
import { fmtBytes, fmtNum } from "../../components/format";
import { PageHeader, SectionHeader, Empty } from "../../components/dashboard";

export const dynamic = "force-dynamic";

function relevant(m: Metric): boolean {
  return /^zen_(wal|memtable|ingest)_/.test(m.name);
}

export default async function Page() {
  const metrics = await getMetrics();
  const m = (metrics ?? []).filter(relevant);

  // Group by metric name → sum value, collect labels.
  type Group = {
    name: string;
    total: number;
    series: { labels: Record<string, string>; value: number }[];
  };
  const groups: Map<string, Group> = new Map();
  for (const x of m) {
    const g = groups.get(x.name) ?? { name: x.name, total: 0, series: [] };
    g.total += x.value;
    g.series.push({ labels: x.labels, value: x.value });
    groups.set(x.name, g);
  }
  const list = [...groups.values()].sort((a, b) => a.name.localeCompare(b.name));

  return (
    <div className="flex flex-col gap-5">
      <PageHeader
        title="WAL"
        subtitle="Write-ahead log lives on object storage with conditional PUT. Counters and histograms here are filtered to zen_wal_/zen_memtable_/zen_ingest_."
      />

      <section className="border border-border">
        <SectionHeader title="WAL & ingest metrics" count={list.length} />
        {list.length === 0 ? (
          <Empty msg="No WAL/ingest metrics yet. Issue an ingest to populate." />
        ) : (
          <>
            <div className="grid grid-cols-[1fr_120px_1fr] text-2xs text-white/40 px-4 py-2 border-b border-border">
              <span>METRIC</span>
              <span className="text-right">TOTAL</span>
              <span>SERIES</span>
            </div>
            {list.map((g, i, arr) => (
              <div
                key={g.name}
                className={`grid grid-cols-[1fr_120px_1fr] items-start px-4 py-2 text-xs hover:bg-white/[0.02] ${
                  i !== arr.length - 1 ? "border-b border-border" : ""
                }`}
              >
                <span className="text-white/80">{g.name}</span>
                <span className="text-right text-white/70">
                  {g.name.endsWith("_bytes") || g.name.endsWith("_bytes_total")
                    ? fmtBytes(g.total)
                    : fmtNum(g.total)}
                </span>
                <span className="text-2xs text-white/50">
                  {g.series.length} series
                  {g.series.length <= 4 && (
                    <ul className="mt-1 space-y-0.5 text-white/40">
                      {g.series.map((s, j) => (
                        <li key={j}>
                          {Object.keys(s.labels).length === 0
                            ? "{}"
                            : `{${Object.entries(s.labels)
                                .map(([k, v]) => `${k}="${v}"`)
                                .join(", ")}}`}{" "}
                          = {fmtNum(s.value)}
                        </li>
                      ))}
                    </ul>
                  )}
                </span>
              </div>
            ))}
          </>
        )}
      </section>
    </div>
  );
}
