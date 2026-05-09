# ZenithDB Dashboard

Minimal Next.js 15 console for a running ZenithDB. **Real data, no fixtures** — every number is fetched live from the engine's HTTP API.

## Run

```bash
# In one terminal — start ZenithDB.
cargo run --release -p zen_cli -- serve --config examples/zenithdb.dev.toml

# In another — start the dashboard.
cd web
bun install
bun dev
```

Open [http://localhost:3000](http://localhost:3000).

## Configuration

| Var          | Default                    | Description                           |
|--------------|----------------------------|---------------------------------------|
| `ZENITH_URL` | `http://localhost:8080`    | Where to scrape live data from.       |

## Where the numbers come from

| UI element         | Endpoint                       |
|--------------------|--------------------------------|
| liveness pill      | `GET /v1/healthz`              |
| readiness pill     | `GET /v1/readyz`               |
| QUERIES, p95, INGEST, RATES | `GET /v1/metrics` (Prometheus parsed in `lib/zenith.ts`) |
| Tenant rows        | tenant labels in `/v1/metrics` + `GET /v1/segments?tenant_id=…` |
| Recent segments    | `GET /v1/segments?tenant_id=…` for each tenant |

Counters → rates are computed by diffing two snapshots in process memory; the page auto-revalidates every 5s via a `router.refresh()` ticker. p95 / p50 are linear-interpolated from the `_bucket` series of `zen_query_duration_seconds` and `zen_ingest_duration_seconds`.

## Layout

- `app/page.tsx` — Server Component; fetches all data and renders the page.
- `app/components/refresher.tsx` — `"use client"` ticker that calls `router.refresh()`.
- `app/components/ui.tsx` — `PrimaryButton` / `GhostButton` with the slide-arrow hover.
- `app/components/icons.tsx` — hand-rolled SVGs.
- `lib/zenith.ts` — fetcher, Prometheus parser, histogram-quantile, rate cache.
- `tailwind.config.ts` — `bg`, `surface`, `surface-card`, `border`; `2xs` (10px) and `h1-title` (56px) sizes.

When the engine is unreachable the page renders an offline banner with the URL it tried, so you don't waste time wondering whether the data is stale or absent.
