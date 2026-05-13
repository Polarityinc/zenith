import { ChevronRight } from "./icons";

// The arrow that slides in on hover (Prime-Intellect style).
function SlideArrow() {
  return (
    <div className="w-3 h-3 overflow-hidden relative">
      <div className="flex -translate-x-full transition-transform duration-300 ease-in-out group-hover:translate-x-0">
        <ChevronRight className="w-3 h-3 shrink-0" />
        <ChevronRight className="w-3 h-3 shrink-0" />
      </div>
    </div>
  );
}

export function PrimaryButton({
  href,
  children,
}: {
  href: string;
  children: React.ReactNode;
}) {
  return (
    <a
      href={href}
      className="group inline-flex w-fit shrink-0 items-center justify-center gap-1 whitespace-nowrap uppercase transition-colors hover:cursor-pointer focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-white/60 bg-white text-black min-h-7 px-2 py-2 text-xs leading-none"
    >
      {children}
      <SlideArrow />
    </a>
  );
}

export function GhostButton({
  href,
  children,
}: {
  href: string;
  children: React.ReactNode;
}) {
  return (
    <a
      href={href}
      className="group inline-flex w-fit shrink-0 items-center justify-center gap-1 whitespace-nowrap uppercase transition-colors hover:cursor-pointer focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-white/60 bg-white/[0.04] text-white/50 hover:bg-white/[0.08] hover:text-white min-h-7 px-2 py-2 text-xs leading-none"
    >
      {children}
      <SlideArrow />
    </a>
  );
}
