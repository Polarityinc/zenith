"use client";

import { useState } from "react";

type Result = {
  ok: boolean;
  status: number;
  ms: number;
  body: unknown;
};

export function QueryRunner({ defaultTenantId }: { defaultTenantId?: string }) {
  const [tenantId, setTenantId] = useState(defaultTenantId ?? "1");
  const [dialect, setDialect] = useState<"sql" | "zql">("sql");
  const [query, setQuery] = useState(
    "SELECT count(*) AS n FROM spans LIMIT 10",
  );
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<Result | null>(null);

  async function run(e: React.FormEvent) {
    e.preventDefault();
    setRunning(true);
    setResult(null);
    const started = performance.now();
    try {
      const r = await fetch("/api/query", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          tenant_id: Number(tenantId),
          query,
          dialect,
        }),
      });
      const text = await r.text();
      let body: unknown;
      try {
        body = JSON.parse(text);
      } catch {
        body = text;
      }
      setResult({
        ok: r.ok,
        status: r.status,
        ms: performance.now() - started,
        body,
      });
    } catch (e: unknown) {
      setResult({
        ok: false,
        status: 0,
        ms: performance.now() - started,
        body: e instanceof Error ? e.message : String(e),
      });
    } finally {
      setRunning(false);
    }
  }

  return (
    <form onSubmit={run} className="flex flex-col gap-3">
      <div className="flex items-center gap-2 text-2xs text-white/50">
        <label>tenant_id</label>
        <input
          value={tenantId}
          onChange={(e) => setTenantId(e.target.value)}
          inputMode="numeric"
          className="bg-white/[0.04] outline-none px-2 h-7 text-xs text-white w-[120px]"
        />
        <label className="ml-2">dialect</label>
        <select
          value={dialect}
          onChange={(e) => setDialect(e.target.value as "sql" | "zql")}
          className="bg-white/[0.04] outline-none px-2 h-7 text-xs text-white"
        >
          <option value="sql">sql</option>
          <option value="zql">zql</option>
        </select>
        <button
          type="submit"
          disabled={running}
          className="ml-auto group inline-flex items-center justify-center gap-1 whitespace-nowrap uppercase bg-white text-black h-7 px-2 text-2xs disabled:opacity-50"
        >
          {running ? "RUNNING…" : "RUN QUERY"}
        </button>
      </div>
      <textarea
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        rows={6}
        spellCheck={false}
        className="bg-white/[0.04] outline-none p-3 text-xs text-white resize-y leading-relaxed"
      />

      {result && (
        <div className="border border-border">
          <div className="px-4 h-9 border-b border-border flex items-center justify-between">
            <div className="flex items-center gap-2 text-2xs">
              <span
                className={`size-1.5 rounded-full ${
                  result.ok ? "bg-green-400" : "bg-red-400"
                }`}
              />
              <span className="text-white/70">
                {result.ok ? "ok" : "error"} · status {result.status}
              </span>
            </div>
            <span className="text-2xs text-white/40">
              {result.ms.toFixed(0)} ms
            </span>
          </div>
          <pre className="p-4 text-2xs text-white/80 overflow-auto leading-relaxed max-h-[480px] whitespace-pre-wrap">
            {typeof result.body === "string"
              ? result.body
              : JSON.stringify(result.body, null, 2)}
          </pre>
        </div>
      )}
    </form>
  );
}
