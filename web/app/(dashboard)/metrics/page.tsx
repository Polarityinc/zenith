import { getMetrics, type Metric } from "@/lib/zenith";
import { fmtNum } from "../../components/format";
import { PageHeader, SectionHeader, Empty } from "../../components/dashboard";

export const dynamic = "force-dynamic";

export default async function Page() {
  const metrics = await getMetrics();
  const m = metrics ?? [];

  // Group by metric name; show summary value (sum) + series count.
  type Group = { name: string; sum: number; count: number };
  const groups: Map<string, Group> = new Map();
  for (const x of m) {
    const g = groups.get(x.name) ?? { name: x.name, sum: 0, count: 0 };
    g.sum += x.value;
    g.count++;
    groups.set(x.name, g);
  }
  const list = [...groups.values()].sort((a, b) => a.name.localeCompare(b.name));

  return (
    <div className="flex flex-col gap-5">
      <PageHeader
        title="Metrics"
        subtitle={`Raw Prometheus exposition from /v1/metrics, parsed client-side. ${list.length} metrics, ${m.length} time series.`}
      />

      <section className="border border-border">
        <SectionHeader title="All metrics" count={list.length} />
        {list.length === 0 ? (
          <Empty msg="No metrics returned by /v1/metrics." />
        ) : (
          <>
            <div className="grid grid-cols-[1fr_140px_140px] text-2xs text-white/40 px-4 py-2 border-b border-border">
              <span>NAME</span>
              <span className="text-right">SERIES</span>
              <span className="text-right">SUM</span>
            </div>
            {list.map((g, i, arr) => (
              <div
                key={g.name}
                className={`grid grid-cols-[1fr_140px_140px] items-center px-4 py-2 text-xs hover:bg-white/[0.02] ${
                  i !== arr.length - 1 ? "border-b border-border" : ""
                }`}
              >
                <span className="text-white/80">{g.name}</span>
                <span className="text-right text-white/70">{g.count}</span>
                <span className="text-right text-white/70">{fmtNum(g.sum)}</span>
              </div>
            ))}
          </>
        )}
      </section>

      <section className="border border-border">
        <SectionHeader
          title="Raw series"
          count={m.length}
          rhs={
            <span className="text-2xs text-white/40">first 200 of {m.length}</span>
          }
        />
        {m.length === 0 ? (
          <Empty msg="No series." />
        ) : (
          <pre className="p-4 text-2xs text-white/70 overflow-auto leading-relaxed max-h-[600px] whitespace-pre-wrap">
            {m
              .slice(0, 200)
              .map(
                (x: Metric) =>
                  `${x.name}${
                    Object.keys(x.labels).length
                      ? `{${Object.entries(x.labels)
                          .map(([k, v]) => `${k}="${v}"`)
                          .join(",")}}`
                      : ""
                  } ${x.value}`,
              )
              .join("\n")}
          </pre>
        )}
      </section>
    </div>
  );
}
