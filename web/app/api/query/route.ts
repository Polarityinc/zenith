import { ZENITH_BASE } from "@/lib/zenith";

export const dynamic = "force-dynamic";

export async function POST(req: Request) {
  const body = await req.text();
  try {
    const r = await fetch(`${ZENITH_BASE}/v1/query`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body,
      cache: "no-store",
    });
    const text = await r.text();
    return new Response(text, {
      status: r.status,
      headers: {
        "content-type": r.headers.get("content-type") ?? "application/json",
      },
    });
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    return new Response(JSON.stringify({ error: msg, base: ZENITH_BASE }), {
      status: 502,
      headers: { "content-type": "application/json" },
    });
  }
}
