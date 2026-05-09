type IconProps = { className?: string };

const base = "w-3.5 h-3.5";

export const ChevronRight = ({ className }: IconProps) => (
  <svg width="12" height="12" viewBox="0 0 12 12" fill="none" className={className ?? "w-3 h-3"}>
    <path d="M4.75 9.125L7.875 6L4.75 2.875" stroke="currentColor" strokeLinecap="square" />
  </svg>
);

export const Search = ({ className }: IconProps) => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" className={className ?? base}>
    <circle cx="6" cy="6" r="4" stroke="currentColor" />
    <path d="M9 9L12 12" stroke="currentColor" strokeLinecap="square" />
  </svg>
);

export const Plus = ({ className }: IconProps) => (
  <svg width="10" height="10" viewBox="0 0 10 10" fill="none" className={className ?? "w-2.5 h-2.5"}>
    <path d="M5 1V9M1 5H9" stroke="currentColor" strokeLinecap="square" />
  </svg>
);

export const StatusDot = ({ className }: IconProps) => (
  <span
    aria-hidden="true"
    className={`inline-block w-1.5 h-1.5 rounded-full bg-current ${className ?? ""}`}
  />
);

export const Square = ({ className }: IconProps) => (
  <svg width="8" height="8" viewBox="0 0 8 8" fill="none" className={className ?? "w-2 h-2"}>
    <rect x="1" y="1" width="6" height="6" stroke="currentColor" />
  </svg>
);

export const Database = ({ className }: IconProps) => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" className={className ?? base}>
    <ellipse cx="7" cy="3" rx="5" ry="1.5" stroke="currentColor" />
    <path d="M2 3v8c0 .8 2.2 1.5 5 1.5s5-.7 5-1.5V3" stroke="currentColor" />
    <path d="M2 7c0 .8 2.2 1.5 5 1.5s5-.7 5-1.5" stroke="currentColor" />
  </svg>
);

export const Activity = ({ className }: IconProps) => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" className={className ?? base}>
    <path d="M1 7h3l2-5 2 10 2-5h3" stroke="currentColor" strokeLinecap="square" />
  </svg>
);

export const Box = ({ className }: IconProps) => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" className={className ?? base}>
    <path d="M7 1L1.5 4v6L7 13l5.5-3V4L7 1z" stroke="currentColor" />
    <path d="M1.5 4L7 7l5.5-3M7 7v6" stroke="currentColor" />
  </svg>
);

export const Code = ({ className }: IconProps) => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" className={className ?? base}>
    <path d="M5 4L2 7l3 3M9 4l3 3-3 3" stroke="currentColor" strokeLinecap="square" />
  </svg>
);

export const Layers = ({ className }: IconProps) => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" className={className ?? base}>
    <path d="M7 1L1 4l6 3 6-3-6-3z" stroke="currentColor" strokeLinejoin="miter" />
    <path d="M1 7l6 3 6-3M1 10l6 3 6-3" stroke="currentColor" strokeLinejoin="miter" />
  </svg>
);

export const ScrollText = ({ className }: IconProps) => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" className={className ?? base}>
    <path d="M2 2h8v10H4a2 2 0 01-2-2V2z" stroke="currentColor" />
    <path d="M5 5h4M5 7h4M5 9h2" stroke="currentColor" strokeLinecap="square" />
  </svg>
);

export const BarChart = ({ className }: IconProps) => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" className={className ?? base}>
    <path d="M2 12V8M6 12V4M10 12V6" stroke="currentColor" strokeLinecap="square" />
    <path d="M1 13h12" stroke="currentColor" strokeLinecap="square" />
  </svg>
);

export const Settings = ({ className }: IconProps) => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" className={className ?? base}>
    <circle cx="7" cy="7" r="2" stroke="currentColor" />
    <path
      d="M7 1v2M7 11v2M1 7h2M11 7h2M2.8 2.8l1.4 1.4M9.8 9.8l1.4 1.4M2.8 11.2l1.4-1.4M9.8 4.2l1.4-1.4"
      stroke="currentColor"
      strokeLinecap="square"
    />
  </svg>
);
