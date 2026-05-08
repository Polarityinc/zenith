//! Authentication primitives for ZenithDB.
//!
//! Two flavors:
//!
//! - **JWT (RS256)** for client traffic. The customer ships a token minted
//!   by their own IdP — Auth0, Cognito, Supabase GoTrue, etc. ZenithDB
//!   verifies the signature against a configured JWKS URL and pulls
//!   `tenant_id` and `scope` from the claims. Public-key crypto is the
//!   Supabase-style approach: rotation is the IdP's responsibility, not
//!   ZenithDB's.
//! - **HMAC-SHA256** for inter-node `/v1/internal/*` traffic. A shared
//!   secret is configured at every node; the request body is signed and
//!   verified with `subtle::ConstantTimeEq` to defend against timing
//!   attacks. ~3 µs per request — an order of magnitude cheaper than
//!   JWT verify, which is why we use it on the hot inter-node path.
//!
//! Both verify paths are wrapped in a [`ClaimsCache`] keyed on a fast
//! xxh3 of the token bytes. Verified tokens stay in the cache until
//! their `exp` claim, capped at 16 K entries.

pub mod claims;
pub mod hmac_auth;
pub mod jwt;

pub use claims::{Claims, ClaimsCache};
pub use hmac_auth::{HmacError, HmacVerifier};
pub use jwt::{JwtError, JwtVerifier};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("missing Authorization header")]
    MissingAuth,
    #[error("malformed Authorization header")]
    Malformed,
    #[error("token expired")]
    Expired,
    #[error("invalid signature")]
    BadSignature,
    #[error("missing claim: {0}")]
    MissingClaim(&'static str),
    #[error("internal: {0}")]
    Internal(String),
}
