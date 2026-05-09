import Link from "next/link";
import { ChevronRight } from "./icons";

export function PageHeader({
  title,
  subtitle,
}: {
  title: string;
  subtitle?: string;
}) {
  return (
    <div className="flex items-end justify-between">
      <div>
        <h1 className="text-xl text-white">{title}</h1>
        {subtitle && (
          <p className="mt-1 text-xs text-white/50 leading-normal">{subtitle}</p>
        )}
      </div>
    </div>
  );
}

export function SectionHeader({
  title,
  count,
  href,
  rhs,
}: {
  title: string;
  count?: number;
  href?: string;
  rhs?: React.ReactNode;
}) {
  return (
    <div className="flex items-center justify-between px-4 h-9 border-b border-border">
      <div className="flex items-center gap-2">
        <span className="text-xs">{title}</span>
        {count !== undefined && (
          <span className="bg-white/10 text-2xs text-white/50 leading-none px-1 py-0.5">
            {count}
          </span>
        )}
      </div>
      {rhs ??
        (href && (
          <Link
            href={href}
            className="group flex items-center gap-1 text-2xs text-white/50 hover:text-white"
          >
            View all <ChevronRight />
          </Link>
        ))}
    </div>
  );
}

export function Stat({
  label,
  value,
  unit,
  sub,
}: {
  label: string;
  value: string;
  unit?: string;
  sub?: string;
}) {
  return (
    <div className="bg-bg p-4 flex flex-col gap-3">
      <span className="text-2xs uppercase tracking-wider text-white/40">
        {label}
      </span>
      <div className="flex items-baseline gap-1.5">
        <span className="text-2xl text-white">{value}</span>
        {unit && <span className="text-2xs text-white/40">{unit}</span>}
      </div>
      {sub && <span className="text-2xs text-white/50">{sub}</span>}
    </div>
  );
}

export function Empty({ msg }: { msg: string }) {
  return (
    <div className="px-4 py-8 text-2xs text-white/40 text-center">{msg}</div>
  );
}
