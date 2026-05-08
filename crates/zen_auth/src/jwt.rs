//! JWT (RS256) verification with a JWKS fetcher and claims cache.
//!
//! Standard pattern: customer's IdP publishes a JWKS at a known URL,
//! we fetch + cache the keyset, verify each incoming token's signature
//! against the matching `kid`. We re-fetch JWKS lazily when a `kid`
//! isn't found in our cache (handles rotation without restart).
//!
//! Performance: `jsonwebtoken` does RSA verify with `ring`/`rustcrypto`,
//! ~30–100 µs per verify. We cache the final `Claims` for the token's
//! TTL (capped at 5 minutes) so that a hot client pays ~50 ns after
//! the first request.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use jsonwebtoken::{decode, decode_header, jwk::JwkSet, DecodingKey, Validation};
use parking_lot::RwLock;
use serde::Deserialize;
use thiserror::Error;

use crate::claims::{token_hash, Claims, ClaimsCache};
use crate::AuthError;

#[derive(Debug, Error)]
pub enum JwtError {
    #[error("malformed token")]
    Malformed,
    #[error("unknown signing key (kid={0:?})")]
    UnknownKid(Option<String>),
    #[error("signature verification failed")]
    BadSignature,
    #[error("token expired")]
    Expired,
    #[error("missing claim: {0}")]
    MissingClaim(&'static str),
    #[error("jwks fetch: {0}")]
    JwksFetch(String),
    #[error("jwks parse: {0}")]
    JwksParse(String),
}

impl From<JwtError> for AuthError {
    fn from(e: JwtError) -> AuthError {
        match e {
            JwtError::Malformed => AuthError::Malformed,
            JwtError::Expired => AuthError::Expired,
            JwtError::BadSignature | JwtError::UnknownKid(_) => AuthError::BadSignature,
            JwtError::MissingClaim(c) => AuthError::MissingClaim(c),
            JwtError::JwksFetch(s) | JwtError::JwksParse(s) => AuthError::Internal(s),
        }
    }
}

/// JWKS cache + verification entry point.
///
/// Construct with [`JwtVerifier::new`] (lazy fetch on first verify) or
/// [`JwtVerifier::from_jwks`] (preload, used by tests).
#[derive(Clone)]
pub struct JwtVerifier {
    inner: Arc<Inner>,
}

struct Inner {
    jwks_url: Option<String>,
    keys: RwLock<KeysetSnapshot>,
    claims_cache: ClaimsCache,
    /// Validation rules — same for every token. Required claims, allowed
    /// algorithms, leeway. We require `exp` and validate it.
    validation: Validation,
    /// HTTP client for JWKS refresh.
    http: reqwest::Client,
    /// JWKS refresh interval. Stale-while-revalidate semantics.
    jwks_ttl: Duration,
}

#[derive(Default)]
struct KeysetSnapshot {
    by_kid: HashMap<String, DecodingKey>,
    fetched_at: Option<Instant>,
}

impl JwtVerifier {
    /// Build a verifier that will fetch JWKS lazily from `jwks_url` on
    /// first use, then re-fetch when `jwks_ttl` elapses.
    pub fn new(jwks_url: String, claims_cache: ClaimsCache) -> Self {
        let mut validation = Validation::new(jsonwebtoken::Algorithm::RS256);
        validation.set_required_spec_claims(&["exp"]);
        // Default leeway is 60 s. We want tighter — on a clock-synced
        // cluster, 5 s is plenty and reduces the replay window.
        validation.leeway = 5;
        Self {
            inner: Arc::new(Inner {
                jwks_url: Some(jwks_url),
                keys: RwLock::new(KeysetSnapshot::default()),
                claims_cache,
                validation,
                http: reqwest::Client::new(),
                jwks_ttl: Duration::from_secs(300),
            }),
        }
    }

    /// Test/dev variant: build a verifier with a preloaded JWKS, no
    /// network fetch. Used in unit tests.
    pub fn from_jwks(jwks: JwkSet, claims_cache: ClaimsCache) -> Self {
        let mut validation = Validation::new(jsonwebtoken::Algorithm::RS256);
        validation.set_required_spec_claims(&["exp"]);
        // Default leeway is 60 s. We want tighter — on a clock-synced
        // cluster, 5 s is plenty and reduces the replay window.
        validation.leeway = 5;
        let mut by_kid = HashMap::new();
        for jwk in jwks.keys.iter() {
            if let (Some(kid), Ok(key)) = (
                jwk.common.key_id.as_ref(),
                jsonwebtoken::DecodingKey::from_jwk(jwk),
            ) {
                by_kid.insert(kid.clone(), key);
            }
        }
        Self {
            inner: Arc::new(Inner {
                jwks_url: None,
                keys: RwLock::new(KeysetSnapshot {
                    by_kid,
                    fetched_at: Some(Instant::now()),
                }),
                claims_cache,
                validation,
                http: reqwest::Client::new(),
                jwks_ttl: Duration::from_secs(300),
            }),
        }
    }

    /// Verify a token. Returns parsed `Claims` on success. Hot path is
    /// the cache-hit branch, ~50 ns; cache-miss verifies signature, then
    /// caches the result for the token's remaining lifetime.
    pub async fn verify(&self, token: &str) -> Result<Claims, JwtError> {
        let h = token_hash(token.as_bytes());
        if let Some(c) = self.inner.claims_cache.get(h) {
            // Defense in depth: re-check expiry even on cache hit.
            let now = chrono::Utc::now().timestamp();
            if c.exp > now {
                return Ok(c);
            }
        }

        let header = decode_header(token).map_err(|_| JwtError::Malformed)?;
        let kid = header.kid.clone().ok_or(JwtError::UnknownKid(None))?;

        let key = self.lookup_key(&kid).await?;

        #[derive(Deserialize)]
        struct RawClaims {
            sub: Option<String>,
            // Tenant claim — accept either snake_case or camelCase, since
            // different IdPs differ. Required.
            #[serde(default, alias = "tenantId")]
            tenant_id: Option<u64>,
            exp: i64,
            #[serde(default)]
            scope: Option<String>,
        }

        let data = decode::<RawClaims>(token, &key, &self.inner.validation).map_err(|e| {
            match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => JwtError::Expired,
                jsonwebtoken::errors::ErrorKind::InvalidSignature => JwtError::BadSignature,
                _ => JwtError::Malformed,
            }
        })?;
        let raw = data.claims;
        let tenant = raw.tenant_id.ok_or(JwtError::MissingClaim("tenant_id"))?;
        let claims = Claims {
            sub: raw.sub.unwrap_or_default(),
            tenant_id: tenant,
            exp: raw.exp,
            scope: raw.scope.unwrap_or_default(),
        };
        self.inner.claims_cache.insert(h, claims.clone());
        Ok(claims)
    }

    async fn lookup_key(&self, kid: &str) -> Result<DecodingKey, JwtError> {
        // Fast path: read lock, check cache.
        {
            let g = self.inner.keys.read();
            if let Some(k) = g.by_kid.get(kid) {
                if let Some(t) = g.fetched_at {
                    if t.elapsed() < self.inner.jwks_ttl {
                        return Ok(k.clone());
                    }
                }
            }
        }
        // Slow path: refresh JWKS.
        if let Some(url) = self.inner.jwks_url.clone() {
            self.refresh_jwks(&url).await?;
        }
        let g = self.inner.keys.read();
        g.by_kid
            .get(kid)
            .cloned()
            .ok_or_else(|| JwtError::UnknownKid(Some(kid.to_string())))
    }

    async fn refresh_jwks(&self, url: &str) -> Result<(), JwtError> {
        // SECURITY: JWKS must come over HTTPS — over plain HTTP a MITM
        // can swap in attacker-controlled keys and forge any JWT. The
        // only exception is `http://localhost` for in-process tests.
        if !url.starts_with("https://") && !url.starts_with("http://localhost") && !url.starts_with("http://127.0.0.1") {
            return Err(JwtError::JwksFetch(format!(
                "JWKS URL must use https:// (got: {url})"
            )));
        }
        let resp = self
            .inner
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| JwtError::JwksFetch(format!("{e}")))?;
        // SECURITY: bound the JWKS response so a malicious or
        // compromised IdP can't OOM us with a multi-GB document.
        // Real JWKS docs are ≤ a few KB; 1 MiB is generous.
        const MAX_JWKS_BYTES: usize = 1024 * 1024;
        if let Some(len) = resp.content_length() {
            if len as usize > MAX_JWKS_BYTES {
                return Err(JwtError::JwksFetch(format!(
                    "JWKS response too large: {len} bytes > {MAX_JWKS_BYTES} max"
                )));
            }
        }
        // Even if Content-Length is absent or lying, cap the in-flight
        // accumulation. We collect chunks until we exceed the cap.
        use futures::StreamExt;
        let mut stream = resp.bytes_stream();
        let mut buf: Vec<u8> = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| JwtError::JwksFetch(format!("stream: {e}")))?;
            if buf.len() + chunk.len() > MAX_JWKS_BYTES {
                return Err(JwtError::JwksFetch(format!(
                    "JWKS exceeded {MAX_JWKS_BYTES} byte cap mid-stream"
                )));
            }
            buf.extend_from_slice(&chunk);
        }
        let jwks: JwkSet =
            serde_json::from_slice(&buf).map_err(|e| JwtError::JwksParse(format!("{e}")))?;
        let mut by_kid = HashMap::new();
        for jwk in jwks.keys.iter() {
            if let (Some(kid), Ok(key)) = (
                jwk.common.key_id.as_ref(),
                DecodingKey::from_jwk(jwk),
            ) {
                by_kid.insert(kid.clone(), key);
            }
        }
        let mut g = self.inner.keys.write();
        *g = KeysetSnapshot {
            by_kid,
            fetched_at: Some(Instant::now()),
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};

    fn rsa_keypair() -> (EncodingKey, JwkSet) {
        // Test key — RFC 7517 example RSA-2048 keypair from
        // jsonwebtoken's own test fixtures (PKCS#8 PEM + matching JWK).
        // The token kid is "test-1".
        let pem = include_bytes!("test_data/rsa_pkcs8.pem");
        let jwks_json = include_str!("test_data/jwks.json");
        let jwks: JwkSet = serde_json::from_str(jwks_json).unwrap();
        let enc = EncodingKey::from_rsa_pem(pem).unwrap();
        (enc, jwks)
    }

    #[derive(Debug, serde::Serialize)]
    struct TestClaims {
        sub: String,
        tenant_id: u64,
        exp: i64,
        scope: String,
    }

    fn mint(enc: &EncodingKey, exp: i64, tenant_id: u64) -> String {
        let mut h = Header::new(jsonwebtoken::Algorithm::RS256);
        h.kid = Some("test-1".into());
        encode(
            &h,
            &TestClaims {
                sub: "alice".into(),
                tenant_id,
                exp,
                scope: "ingest read".into(),
            },
            enc,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn verify_valid_token() {
        let (enc, jwks) = rsa_keypair();
        let v = JwtVerifier::from_jwks(jwks, ClaimsCache::default());
        let exp = chrono::Utc::now().timestamp() + 60;
        let token = mint(&enc, exp, 42);
        let claims = v.verify(&token).await.unwrap();
        assert_eq!(claims.tenant_id, 42);
        assert!(claims.has_scope("ingest"));
    }

    #[tokio::test]
    async fn expired_token_rejected() {
        let (enc, jwks) = rsa_keypair();
        let v = JwtVerifier::from_jwks(jwks, ClaimsCache::default());
        // Expire well outside the 5 s leeway window.
        let token = mint(&enc, chrono::Utc::now().timestamp() - 600, 42);
        let r = v.verify(&token).await;
        assert!(
            matches!(r, Err(JwtError::Expired)),
            "expected JwtError::Expired, got {r:?}"
        );
    }

    #[tokio::test]
    async fn cache_hit_avoids_reverification() {
        let (enc, jwks) = rsa_keypair();
        let cache = ClaimsCache::default();
        let v = JwtVerifier::from_jwks(jwks, cache.clone());
        let token = mint(&enc, chrono::Utc::now().timestamp() + 60, 7);
        v.verify(&token).await.unwrap();
        assert!(cache.get(token_hash(token.as_bytes())).is_some());
    }
}
