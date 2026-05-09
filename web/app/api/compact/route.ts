import { ZENITH_BASE } from "@/lib/zenith";

export const dynamic = "force-dynamic";

export async function POST(req: Request) {
  // Body: {"tenant_id": <num>, "full": bool}
  // Routes to /v1/compact (incremental) or /v1/compact-full.
  let json: { tenant_id?: number; full?: boolean };
  try {
    json = await req.json();
  } catch {
    return new Response(JSON.stringify({ error: "invalid JSON body" }), {
      status: 400,
      headers: { "content-type": "application/json" },
    });
  }
  const path = json.full ? "/v1/compact-full" : "/v1/compact";
  const upstream = JSON.stringify({ tenant_id: json.tenant_id });
  try {
    const r = await fetch(`${ZENITH_BASE}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: upstream,
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
