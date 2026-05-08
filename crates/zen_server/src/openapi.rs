//! Hand-rolled OpenAPI 3.1 spec for `/v1/*`.
//!
//! We don't `#[derive(ToSchema)]` on every handler request/response type
//! because doing so would propagate `utoipa` derives into request DTOs
//! defined in `zen_query` (the response shape) and force a workspace-wide
//! dep change. Instead, we publish a static `OpenApi` constructed in code
//! that mirrors the actual handlers. Tracking is straightforward: any
//! new route in `http.rs` should also land here.
//!
//! `GET /v1/openapi.json` returns the spec; SDK generators (typescript,
//! python, go, rust) can point at it directly.

use axum::{extract::State, response::IntoResponse, Json};
use utoipa::openapi::{
    path::OperationBuilder, ContactBuilder, HttpMethod, InfoBuilder, OpenApiBuilder, PathItem,
    PathsBuilder,
};

use crate::state::ServerState;

/// Build the static OpenAPI document. Cached as the bytes of a JSON
/// rendering on first request via `OnceLock` — see [`handle_openapi`].
fn build_spec() -> utoipa::openapi::OpenApi {
    let info = InfoBuilder::new()
        .title("ZenithDB HTTP API")
        .version(env!("CARGO_PKG_VERSION"))
        .description(Some(
            "Public HTTP API for ZenithDB. Customer-facing routes \
             (`/v1/ingest`, `/v1/query`, `/v1/traces`, …) require a JWT \
             Bearer token when `auth.jwks_url` is configured. \
             `/v1/internal/*` is the cluster-internal route, \
             authenticated by HMAC and not intended for customer use.",
        ))
        .contact(Some(
            ContactBuilder::new()
                .name(Some("Polarity"))
                .email(Some("support@polarity.so"))
                .build(),
        ))
        .build();

    let paths = PathsBuilder::new()
        .path(
            "/v1/health",
            PathItem::new(
                HttpMethod::Get,
                OperationBuilder::new()
                    .summary(Some("Liveness probe (legacy alias)"))
                    .description(Some("Returns 200 OK if the process is up. Use `/v1/healthz` for new integrations."))
                    .build(),
            ),
        )
        .path(
            "/v1/healthz",
            PathItem::new(
                HttpMethod::Get,
                OperationBuilder::new()
                    .summary(Some("Liveness probe"))
                    .description(Some("Cheap, dependency-free probe — returns 200 if the axum task is reachable. Used by Kubernetes liveness probes."))
                    .build(),
            ),
        )
        .path(
            "/v1/readyz",
            PathItem::new(
                HttpMethod::Get,
                OperationBuilder::new()
                    .summary(Some("Readiness probe"))
                    .description(Some("Returns 200 only when the process is ready to accept customer traffic. Verifies the catalog is reachable; returns 503 on dependency failure."))
                    .build(),
            ),
        )
        .path(
            "/v1/metrics",
            PathItem::new(
                HttpMethod::Get,
                OperationBuilder::new()
                    .summary(Some("Prometheus metrics"))
                    .description(Some("Standard Prometheus text exposition format. Scrape on a 15-30 s interval. Public — no auth, since metric names + tenant labels are not sensitive in this product."))
                    .build(),
            ),
        )
        .path(
            "/v1/ingest",
            PathItem::new(
                HttpMethod::Post,
                OperationBuilder::new()
                    .summary(Some("Ingest spans"))
                    .description(Some("Accepts a JSON `IngestRequest` (see `ingest.rs::IngestRequest`). Returns the WAL object key + accepted span count. Body limit is 256 MiB. Auth: JWT, scope `ingest`."))
                    .build(),
            ),
        )
        .path(
            "/v1/traces",
            PathItem::new(
                HttpMethod::Post,
                OperationBuilder::new()
                    .summary(Some("OTLP/HTTP trace ingest"))
                    .description(Some("OpenTelemetry OTLP/protobuf payloads (`gen_ai.*` semantic conventions). Auth: JWT, scope `ingest`."))
                    .build(),
            ),
        )
        .path(
            "/v1/query",
            PathItem::new(
                HttpMethod::Post,
                OperationBuilder::new()
                    .summary(Some("Run a query"))
                    .description(Some("Body: `{ tenant_id, query, dialect }`. SQL or ZenithQL. Returns columns + rows + scan stats. Auth: JWT."))
                    .build(),
            ),
        )
        .path(
            "/v1/segments",
            PathItem::new(
                HttpMethod::Get,
                OperationBuilder::new()
                    .summary(Some("List active segments for a tenant"))
                    .description(Some("Used by debugging tooling. Auth: JWT, scope `admin`."))
                    .build(),
            ),
        )
        .path(
            "/v1/compact",
            PathItem::new(
                HttpMethod::Post,
                OperationBuilder::new()
                    .summary(Some("Trigger a partition compaction"))
                    .description(Some("Manual compaction handle. Returns when compaction completes. Auth: JWT, scope `admin`."))
                    .build(),
            ),
        )
        .path(
            "/v1/compact-full",
            PathItem::new(
                HttpMethod::Post,
                OperationBuilder::new()
                    .summary(Some("Trigger a full-tenant compaction"))
                    .description(Some("Tier-N merge across the tenant. Long-running. Auth: JWT, scope `admin`."))
                    .build(),
            ),
        )
        .path(
            "/v1/openapi.json",
            PathItem::new(
                HttpMethod::Get,
                OperationBuilder::new()
                    .summary(Some("This document"))
                    .description(Some("Returns the OpenAPI 3.1 schema for the public API. SDK generators (openapi-generator, etc.) can point at this URL."))
                    .build(),
            ),
        )
        .build();

    OpenApiBuilder::new().info(info).paths(paths).build()
}

use std::sync::OnceLock;
static SPEC: OnceLock<utoipa::openapi::OpenApi> = OnceLock::new();

/// `GET /v1/openapi.json`. Returns the OpenAPI 3.1 document. Cached on
/// first request; no per-call cost beyond a hashmap lookup.
pub async fn handle_openapi(State(_state): State<ServerState>) -> impl IntoResponse {
    Json(SPEC.get_or_init(build_spec).clone())
}
