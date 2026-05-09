export function fmtBytes(n: number): string {
  if (!Number.isFinite(n)) return "—";
  if (n === 0) return "0 B";
  const u = ["B", "KiB", "MiB", "GiB", "TiB"];
  let i = 0;
  let v = n;
  while (v >= 1024 && i < u.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v < 10 && i > 0 ? 1 : 0)} ${u[i]}`;
}

export function fmtNum(n: number): string {
  if (!Number.isFinite(n)) return "—";
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)}B`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}k`;
  return n.toFixed(0);
}

export function fmtMs(seconds: number | null): string {
  if (seconds === null || !Number.isFinite(seconds)) return "—";
  const ms = seconds * 1000;
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)} s`;
  if (ms >= 10) return `${ms.toFixed(0)} ms`;
  return `${ms.toFixed(1)} ms`;
}

export function fmtTime(s: string | undefined): string {
  if (!s) return "—";
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  return d.toISOString().replace("T", " ").slice(0, 19);
}

export function StatusDot({
  kind,
}: {
  kind: "ok" | "warn" | "err" | "off";
}) {
  const c = {
    ok: "bg-green-400",
    warn: "bg-amber-400",
    err: "bg-red-400",
    off: "bg-white/30",
  }[kind];
  return (
    <span aria-hidden className={`inline-block w-1.5 h-1.5 rounded-full ${c}`} />
  );
}
