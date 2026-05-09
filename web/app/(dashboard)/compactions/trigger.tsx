"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";

type Result = { ok: boolean; status: number; body: unknown };

export function CompactionTrigger({
  defaultTenantId,
}: {
  defaultTenantId?: string;
}) {
  const router = useRouter();
  const [tenantId, setTenantId] = useState(defaultTenantId ?? "1");
  const [full, setFull] = useState(false);
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<Result | null>(null);

  async function run(e: React.FormEvent) {
    e.preventDefault();
    setRunning(true);
    setResult(null);
    try {
      const r = await fetch("/api/compact", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          tenant_id: Number(tenantId),
          full,
        }),
      });
      const text = await r.text();
      let body: unknown;
      try {
        body = JSON.parse(text);
      } catch {
        body = text;
      }
      setResult({ ok: r.ok, status: r.status, body });
      router.refresh();
    } catch (e: unknown) {
      setResult({
        ok: false,
        status: 0,
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
        <label className="ml-3 inline-flex items-center gap-1.5">
          <input
            type="checkbox"
            checked={full}
            onChange={(e) => setFull(e.target.checked)}
            className="accent-white"
          />
          full (merge all segments to one)
        </label>
        <button
          type="submit"
          disabled={running}
          className="ml-auto group inline-flex items-center justify-center gap-1 whitespace-nowrap uppercase bg-white text-black h-7 px-2 text-2xs disabled:opacity-50"
        >
          {running ? "TRIGGERING…" : "TRIGGER COMPACTION"}
        </button>
      </div>

      {result && (
        <div className="border border-border">
          <div className="px-4 h-9 border-b border-border flex items-center gap-2 text-2xs">
            <span
              className={`size-1.5 rounded-full ${
                result.ok ? "bg-green-400" : "bg-red-400"
              }`}
            />
            <span className="text-white/70">
              {result.ok ? "ok" : "error"} · status {result.status}
            </span>
          </div>
          <pre className="p-4 text-2xs text-white/80 overflow-auto leading-relaxed max-h-[300px] whitespace-pre-wrap">
            {typeof result.body === "string"
              ? result.body
              : JSON.stringify(result.body, null, 2)}
          </pre>
        </div>
      )}
    </form>
  );
}
