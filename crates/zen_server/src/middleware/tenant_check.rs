//! Cross-tenant authorization helper.
//!
//! Every customer-facing handler that takes a `tenant_id` in its body or
//! query string MUST call [`enforce_tenant`] to verify it matches the
//! verified JWT claim. Without this check, a tenant with a valid token
//! could supply an arbitrary `tenant_id` in the request body and read or
//! mutate another tenant's data — the most severe class of multi-tenant
//! bug.
//!
//! When auth is disabled (dev / single-tenant deployments where
//! `cfg.auth.jwks_url` is empty), the JWT layer injects a permissive
//! `Claims { tenant_id: 0, scope: "" }`. In that mode we still want to
//! allow any `req.tenant_id` to pass through — that's the whole point of
//! "auth off" — but a real verified claim with `tenant_id != 0` MUST
//! match. Enforcement therefore skips when `claims.sub == "anonymous"`,
//! which is the marker the auth middleware uses for the off path.

use axum::{
    extract::Request,
    http::{Extensions, StatusCode},
};

use zen_auth::Claims;

/// Marker subject the auth middleware injects when JWT is disabled.
pub const ANONYMOUS_SUB: &str = "anonymous";

/// Returns Ok if the request's `claimed_tenant_id` is allowed for the
/// caller; Err with a 403 otherwise. Pulls `Claims` from the request
/// extensions (set by `jwt_layer`).
pub fn enforce_tenant(
    extensions: &Extensions,
    claimed_tenant_id: u64,
) -> Result<(), (StatusCode, String)> {
    match extensions.get::<Claims>() {
        Some(c) if c.sub == ANONYMOUS_SUB => Ok(()),
        Some(c) if c.tenant_id == claimed_tenant_id => Ok(()),
        Some(c) => Err((
            StatusCode::FORBIDDEN,
            format!(
                "tenant mismatch: token authorizes tenant {}, request claims tenant {}",
                c.tenant_id, claimed_tenant_id
            ),
        )),
        None => Err((
            StatusCode::UNAUTHORIZED,
            "no auth claims present (jwt_layer must run before this handler)".into(),
        )),
    }
}

/// Convenience: extract the verified Claims (or 401).
pub fn require_claims(req: &Request) -> Result<&Claims, (StatusCode, String)> {
    req.extensions().get::<Claims>().ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            "no auth claims present".to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ext_with(claims: Claims) -> Extensions {
        let mut e = Extensions::new();
        e.insert(claims);
        e
    }

    #[test]
    fn allows_matching_tenant() {
        let e = ext_with(Claims {
            sub: "alice".into(),
            tenant_id: 42,
            exp: 0,
            scope: String::new(),
        });
        enforce_tenant(&e, 42).unwrap();
    }

    #[test]
    fn rejects_mismatched_tenant() {
        let e = ext_with(Claims {
            sub: "alice".into(),
            tenant_id: 42,
            exp: 0,
            scope: String::new(),
        });
        let r = enforce_tenant(&e, 99).unwrap_err();
        assert_eq!(r.0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn allows_any_tenant_when_anonymous() {
        let e = ext_with(Claims {
            sub: ANONYMOUS_SUB.into(),
            tenant_id: 0,
            exp: 0,
            scope: "ingest read admin".into(),
        });
        enforce_tenant(&e, 99).unwrap();
    }

    #[test]
    fn rejects_when_claims_missing() {
        let e = Extensions::new();
        let r = enforce_tenant(&e, 1).unwrap_err();
        assert_eq!(r.0, StatusCode::UNAUTHORIZED);
    }
}
