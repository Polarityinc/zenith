//! Axum middleware: JWT (customer-facing) + HMAC (inter-node).
//!
//! Both middlewares live as `from_fn_with_state` closures so they
//! integrate with our existing `ServerState`. They inject a `Claims`
//! value into request extensions on success; handlers downstream pull
//! it via `axum::Extension<Claims>`.

use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::Next,
    response::Response,
};
use http_body_util::BodyExt;

use zen_auth::{Claims, JwtVerifier};

use crate::state::ServerState;

/// JWT middleware. Requires `Authorization: Bearer <jwt>`. On success,
/// injects the verified `Claims` into the request extensions for
/// downstream handlers.
///
/// When `state.auth.jwt` is `None`, this middleware passes through
/// unauthenticated and injects an "anonymous" claim with `tenant_id`
/// set to whatever the request carries — matches the pre-auth behavior
/// for dev / single-node deployments. The boot-time validator logs a
/// warning when this is the case.
pub async fn jwt_layer(
    State(state): State<ServerState>,
    mut req: Request,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    let verifier = match state.auth.jwt.as_ref() {
        Some(v) => v.clone(),
        None => {
            // Auth disabled (single-node / dev). Insert a marker claim
            // so handlers don't crash when extracting. The `anonymous`
            // sub flag tells `enforce_tenant` to allow any request body
            // tenant_id (otherwise dev would be unusable). Scope is
            // intentionally permissive ONLY in this off-mode; real
            // tokens carry whatever scope the IdP minted.
            req.extensions_mut().insert(Claims {
                sub: "anonymous".into(),
                tenant_id: 0,
                exp: 0,
                scope: "ingest read admin".into(),
            });
            return Ok(next.run(req).await);
        }
    };

    let token = extract_bearer(&req).ok_or((
        StatusCode::UNAUTHORIZED,
        "missing or malformed Authorization header".to_string(),
    ))?;

    let claims = verifier
        .verify(&token)
        .await
        .map_err(|e| (StatusCode::UNAUTHORIZED, format!("auth: {e}")))?;
    req.extensions_mut().insert(claims);
    Ok(next.run(req).await)
}

/// HMAC middleware for `/v1/internal/*`. Verifies `X-Zen-Hmac` and
/// `X-Zen-Timestamp` against the configured shared secret. Auth is
/// disabled when `state.auth.hmac` is `None` (single-node deployments).
pub async fn hmac_layer(
    State(state): State<ServerState>,
    req: Request,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    let verifier = match state.auth.hmac.as_ref() {
        Some(v) => v.clone(),
        None => return Ok(next.run(req).await),
    };
    let sig = req
        .headers()
        .get("x-zen-hmac")
        .and_then(|v| v.to_str().ok())
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "missing X-Zen-Hmac header".to_string(),
        ))?
        .to_string();
    let ts: i64 = req
        .headers()
        .get("x-zen-timestamp")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "missing/invalid X-Zen-Timestamp header".to_string(),
        ))?;
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    // Buffer the body so we can hash it (HMAC needs the bytes) then
    // forward the same bytes to the next handler.
    let (parts, body) = req.into_parts();
    let bytes = body
        .collect()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("body collect: {e}")))?
        .to_bytes();
    verifier
        .verify(method.as_str(), &path, &bytes, ts, &sig)
        .map_err(|e| (StatusCode::UNAUTHORIZED, format!("hmac: {e}")))?;
    let req = Request::from_parts(parts, Body::from(bytes));
    Ok(next.run(req).await)
}

fn extract_bearer(req: &Request) -> Option<String> {
    let header_val = req.headers().get(header::AUTHORIZATION)?;
    let s = header_val.to_str().ok()?;
    let rest = s.strip_prefix("Bearer ")?;
    Some(rest.trim().to_string())
}

/// Container for verifier instances. Held in `ServerState` so handlers
/// can reach them without re-loading the JWKS or re-deriving HMAC keys.
#[derive(Clone, Default)]
pub struct AuthState {
    pub jwt: Option<JwtVerifier>,
    pub hmac: Option<zen_auth::HmacVerifier>,
}

impl AuthState {
    /// Build from config. Empty auth fields mean auth is OFF.
    pub fn from_config(cfg: &zen_common::Config) -> Self {
        let jwt = if cfg.auth.jwks_url.is_empty() {
            tracing::warn!(
                "SECURITY: auth.jwks_url is empty — JWT verification is DISABLED. \
                 Anonymous traffic gets ingest+read+admin scope. \
                 Do not run this configuration in production."
            );
            None
        } else {
            Some(JwtVerifier::new(
                cfg.auth.jwks_url.clone(),
                zen_auth::ClaimsCache::default(),
            ))
        };
        let hmac = if cfg.auth.internal_secret.is_empty() {
            tracing::warn!(
                "SECURITY: auth.internal_secret is empty — inter-node \
                 /v1/internal/* is OPEN. Set a 32+ byte secret on every \
                 cluster node before exposing the port externally."
            );
            None
        } else {
            match zen_auth::HmacVerifier::new(cfg.auth.internal_secret.as_bytes().to_vec()) {
                Ok(v) => Some(v),
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "auth.internal_secret rejected: HMAC layer disabled. \
                         Provide a 32+ byte secret to enable."
                    );
                    None
                }
            }
        };
        Self { jwt, hmac }
    }
}
