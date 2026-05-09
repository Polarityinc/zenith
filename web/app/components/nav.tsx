"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

export function NavLink({
  href,
  icon,
  label,
  badge,
}: {
  href: string;
  icon: React.ReactNode;
  label: string;
  badge?: string;
}) {
  const pathname = usePathname();
  const active = pathname === href;
  return (
    <Link
      href={href}
      className={`flex items-center gap-2 px-3 py-1.5 text-xs transition-colors ${
        active
          ? "bg-white/[0.06] text-white"
          : "text-white/50 hover:text-white hover:bg-white/[0.03]"
      }`}
    >
      <span className="w-3.5 h-3.5 flex items-center justify-center">{icon}</span>
      <span className="flex-1">{label}</span>
      {badge && <span className="text-2xs text-white/40">{badge}</span>}
    </Link>
  );
}
