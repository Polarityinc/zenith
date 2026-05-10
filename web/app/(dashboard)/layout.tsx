import {
  getHealth,
  getReady,
  getMetrics,
  getSegments,
  tenantsFromMetrics,
  ZENITH_BASE,
} from "@/lib/zenith";
import { PrimaryButton, GhostButton } from "../components/ui";
import { Refresher } from "../components/refresher";
import { NavLink } from "../components/nav";
import { StatusDot } from "../components/format";
import { fmtNum } from "../components/format";
import {
  Activity,
  Database,
  Layers,
  Code,
  Box,
  ScrollText,
  BarChart,
  Settings,
  Search,
} from "../components/icons";

export const dynamic = "force-dynamic";

export default async function DashboardLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const [health, ready, metrics] = await Promise.all([
    getHealth(),
    getReady(),
    getMetrics(),
  ]);
  const live = !!health?.ok && metrics !== null;

  // Pre-compute counts for sidebar badges. We don't await segments
  // for every nav render because that's expensive — instead we count
  // tenants from /v1/metrics, which is one cheap call.
  const tenantIds = tenantsFromMetrics(metrics ?? []);
  // Pull segments only for the first tenant so sidebar can show *some*
  // segment count without N round trips per request.
  const firstTenantSegs = tenantIds[0]
    ? (await getSegments(tenantIds[0]))?.length ?? 0
    : 0;

  return (
    <div className="min-h-screen flex tabular-nums">
      <aside className="w-[220px] shrink-0 border-r border-border flex flex-col">
        <div className="px-4 h-12 flex items-center gap-2 border-b border-border">
          <span className="size-2.5 bg-white" />
          <span className="text-sm">ZenithDB</span>
          <span className="ml-auto text-2xs text-white/40">0.1.0</span>
        </div>

        <nav className="px-2 py-3 flex flex-col gap-0.5">
          <div className="px-3 py-1 text-2xs uppercase tracking-wider text-white/30">
            General
          </div>
          <NavLink href="/" icon={<Activity />} label="Overview" />
          <NavLink
            href="/tenants"
            icon={<Database />}
            label="Tenants"
            badge={tenantIds.length ? String(tenantIds.length) : undefined}
          />
          <NavLink
            href="/segments"
            icon={<Layers />}
            label="Segments"
            badge={firstTenantSegs ? `${fmtNum(firstTenantSegs)}+` : undefined}
          />
          <NavLink href="/queries" icon={<Code />} label="Queries" />

          <div className="px-3 py-1 mt-3 text-2xs uppercase tracking-wider text-white/30">
            Storage
          </div>
          <NavLink href="/compactions" icon={<Box />} label="Compactions" />
          <NavLink href="/wal" icon={<ScrollText />} label="WAL" />
          <NavLink href="/metrics" icon={<BarChart />} label="Metrics" />

          <div className="px-3 py-1 mt-3 text-2xs uppercase tracking-wider text-white/30">
            Admin
          </div>
          <NavLink href="/settings" icon={<Settings />} label="Settings" />
        </nav>

        <div className="mt-auto px-4 py-3 border-t border-border flex items-center gap-2 text-2xs text-white/40">
          <StatusDot kind={live ? "ok" : "err"} />
          {live ? "connected" : "offline"}
        </div>
      </aside>

      <main className="flex-1 min-w-0 flex flex-col">
        <header className="h-12 border-b border-border flex items-center px-5 gap-3">
          <span className="text-sm text-white/80">ZenithDB</span>
          <span className="text-white/30">/</span>
          <span className="text-xs text-white/50">{ZENITH_BASE}</span>

          <div className="ml-auto flex items-center gap-2">
            <Refresher intervalMs={5000} />
            <div className="hidden md:flex items-center gap-1.5 px-2 h-7 bg-white/[0.04] text-2xs text-white/50 w-[260px]">
              <Search />
              <input
                placeholder="Search segments, queries, traces…"
                className="bg-transparent outline-none flex-1 placeholder:text-white/30"
              />
              <span className="text-white/30">⌘K</span>
            </div>
            <PrimaryButton href={`${ZENITH_BASE}/v1/openapi.json`}>
              OPEN API
            </PrimaryButton>
            <GhostButton href="https://github.com/Polarityinc/zenith">
              DOCS
            </GhostButton>
          </div>
        </header>

        <div className="border-b border-border px-5 py-3 flex flex-wrap items-center gap-2.5 text-2xs text-white/50">
          <span>Connected to</span>
          <span aria-hidden>/</span>
          <span className="text-white/70">{ZENITH_BASE}</span>
          <span aria-hidden>/</span>
          <span className="inline-flex items-center gap-1.5 text-white/70">
            <StatusDot kind={health?.ok ? "ok" : "err"} />
            liveness {health?.ok ? "ok" : "down"}
          </span>
          <span aria-hidden>/</span>
          <span className="inline-flex items-center gap-1.5 text-white/70">
            <StatusDot kind={ready?.ok ? "ok" : "warn"} />
            readiness {ready?.ok ? "ok" : "down"}
          </span>
          <span aria-hidden>/</span>
          <span className="inline-flex items-center gap-1.5 text-white/70">
            <StatusDot kind={metrics ? "ok" : "off"} />
            metrics {metrics ? "scraping" : "offline"}
          </span>
        </div>

        {!live && (
          <div className="border-b border-border bg-red-500/10 text-red-300 px-5 py-3 text-xs flex items-center gap-3">
            <StatusDot kind="err" />
            <span>
              Could not reach{" "}
              <span className="text-red-200">{ZENITH_BASE}</span>. Start it with{" "}
              <span className="text-red-200">
                cargo run -p zen_cli -- serve --config examples/zenithdb.dev.toml
              </span>
              .
            </span>
          </div>
        )}

        <div className="flex-1 p-5 overflow-auto">{children}</div>
      </main>
    </div>
  );
}
