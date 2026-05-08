//! HMAC-SHA256 inter-node auth.
//!
//! Used by `/v1/internal/*` requests between cluster nodes. Each node
//! configured with the same shared secret. The sender computes an HMAC
//! over `METHOD\nPATH\nBODY_HASH\nTIMESTAMP` and ships it in the
//! `X-Zen-Hmac` header along with `X-Zen-Timestamp`. The receiver:
//!
//! 1. Rejects requests with a timestamp more than 5 minutes off — replay
//!    protection.
//! 2. Recomputes the expected HMAC and `subtle::ConstantTimeEq`s it
//!    against the received one — defends against timing attacks on the
//!    comparison itself.
//!
//! We deliberately *don't* use JWT for inter-node traffic: HMAC is ~10×
//! cheaper and the only thing we need to prove is "this request came
//! from a peer that knows the shared secret". No claims, no rotation
//! beyond a config reload, no JWKS fetch.

use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::AuthError;

#[derive(Debug, Error)]
pub enum HmacError {
    #[error("missing X-Zen-Hmac or X-Zen-Timestamp header")]
    MissingHeader,
    #[error("malformed header")]
    Malformed,
    #[error("timestamp skew too large")]
    Skew,
    #[error("signature mismatch")]
    BadSignature,
}

impl From<HmacError> for AuthError {
    fn from(e: HmacError) -> AuthError {
        match e {
            HmacError::MissingHeader => AuthError::MissingAuth,
            HmacError::Malformed => AuthError::Malformed,
            HmacError::Skew | HmacError::BadSignature => AuthError::BadSignature,
        }
    }
}

/// Maximum clock skew allowed between sender and receiver.
const MAX_SKEW_SECONDS: i64 = 300;

#[derive(Clone)]
pub struct HmacVerifier {
    secret: Vec<u8>,
}

impl HmacVerifier {
    pub fn new(secret: impl Into<Vec<u8>>) -> Self {
        Self {
            secret: secret.into(),
        }
    }

    /// Compute the canonical HMAC for a request. Used by both signer and
    /// verifier; expose so callers can audit the canonical form.
    pub fn sign(&self, method: &str, path: &str, body: &[u8], timestamp: i64) -> String {
        let body_hash = Sha256::digest(body);
        let body_hex = hex_encode(&body_hash);
        let canonical = format!("{method}\n{path}\n{body_hex}\n{timestamp}");
        let mut mac = <Hmac<Sha256>>::new_from_slice(&self.secret)
            .expect("hmac accepts any key length");
        mac.update(canonical.as_bytes());
        let tag = mac.finalize().into_bytes();
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(tag)
    }

    pub fn verify(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
        timestamp: i64,
        signature: &str,
    ) -> Result<(), HmacError> {
        let now = chrono::Utc::now().timestamp();
        if (now - timestamp).abs() > MAX_SKEW_SECONDS {
            return Err(HmacError::Skew);
        }
        let expected = self.sign(method, path, body, timestamp);
        // Constant-time compare to defend against timing attacks.
        if expected.as_bytes().ct_eq(signature.as_bytes()).into() {
            Ok(())
        } else {
            Err(HmacError::BadSignature)
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX_CHARS[(b >> 4) as usize] as char);
        s.push(HEX_CHARS[(b & 0xf) as usize] as char);
    }
    s
}
const HEX_CHARS: &[u8] = b"0123456789abcdef";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_verify_roundtrip() {
        let v = HmacVerifier::new(b"hunter2".to_vec());
        let now = chrono::Utc::now().timestamp();
        let sig = v.sign("POST", "/v1/internal/query", b"{}", now);
        v.verify("POST", "/v1/internal/query", b"{}", now, &sig).unwrap();
    }

    #[test]
    fn body_tamper_rejected() {
        let v = HmacVerifier::new(b"hunter2".to_vec());
        let now = chrono::Utc::now().timestamp();
        let sig = v.sign("POST", "/v1/internal/query", b"{}", now);
        let r = v.verify("POST", "/v1/internal/query", b"{tampered}", now, &sig);
        assert!(matches!(r, Err(HmacError::BadSignature)));
    }

    #[test]
    fn stale_timestamp_rejected() {
        let v = HmacVerifier::new(b"hunter2".to_vec());
        let stale = chrono::Utc::now().timestamp() - (MAX_SKEW_SECONDS + 60);
        let sig = v.sign("POST", "/p", b"{}", stale);
        let r = v.verify("POST", "/p", b"{}", stale, &sig);
        assert!(matches!(r, Err(HmacError::Skew)));
    }
}
