"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";

export function Refresher({ intervalMs = 5000 }: { intervalMs?: number }) {
  const router = useRouter();
  const [age, setAge] = useState(0);

  useEffect(() => {
    const tick = setInterval(() => setAge((a) => a + 1), 1000);
    const refresh = setInterval(() => {
      router.refresh();
      setAge(0);
    }, intervalMs);
    return () => {
      clearInterval(tick);
      clearInterval(refresh);
    };
  }, [intervalMs, router]);

  return (
    <span className="font-mono text-2xs text-white/40">
      live · {age}s
    </span>
  );
}
