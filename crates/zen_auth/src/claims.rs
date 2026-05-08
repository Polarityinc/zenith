//! Verified-token claims + the cache that holds them.

use std::sync::Arc;
use std::time::Duration;

use moka::sync::Cache;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claims {
    /// Subject — usually a user or service-account identifier. Kept for
    /// audit logs; we don't use it for authorization.
    pub sub: String,
    /// The tenant this token may operate against. The HTTP handler
    /// compares this against the request's `tenant_id` parameter; mismatch
    /// is a 403.
    pub tenant_id: u64,
    /// Token expiry as a unix timestamp (seconds since epoch).
    pub exp: i64,
    /// Optional scope claim, space-separated permissions like
    /// `"ingest read admin"`. Empty string = read-only.
    #[serde(default)]
    pub scope: String,
}

impl Claims {
    pub fn has_scope(&self, needed: &str) -> bool {
        if self.scope.is_empty() {
            // Empty scope == read-only. `read` (or no requirement) passes.
            return needed == "read";
        }
        self.scope.split_whitespace().any(|s| s == needed)
    }
}

/// Cache of verified claims keyed on a fast hash of the token. The
/// expensive RS256 / HS256 verification runs once; subsequent requests
/// in the next ~minutes cost a hashmap lookup (~50 ns).
#[derive(Clone)]
pub struct ClaimsCache {
    inner: Arc<Cache<u64, Claims>>,
}

impl ClaimsCache {
    pub fn new(capacity: u64, ttl: Duration) -> Self {
        let cache = Cache::builder()
            .max_capacity(capacity)
            .time_to_live(ttl)
            .build();
        Self {
            inner: Arc::new(cache),
        }
    }

    pub fn get(&self, token_hash: u64) -> Option<Claims> {
        self.inner.get(&token_hash)
    }

    pub fn insert(&self, token_hash: u64, claims: Claims) {
        self.inner.insert(token_hash, claims);
    }
}

impl Default for ClaimsCache {
    fn default() -> Self {
        Self::new(16_384, Duration::from_secs(300))
    }
}

/// Hash a token byte string into a 64-bit cache key. xxh3 is ~10 GB/s on
/// modern hardware and is the same primitive we use for the plan cache,
/// so the work is well-amortized.
pub fn token_hash(bytes: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_roundtrip() {
        let cache = ClaimsCache::new(16, Duration::from_secs(60));
        let h = token_hash(b"abc");
        assert!(cache.get(h).is_none());
        cache.insert(
            h,
            Claims {
                sub: "u1".into(),
                tenant_id: 7,
                exp: 9_999_999_999,
                scope: "ingest read".into(),
            },
        );
        let c = cache.get(h).unwrap();
        assert_eq!(c.tenant_id, 7);
    }

    #[test]
    fn scope_matching() {
        let c = Claims {
            sub: "u".into(),
            tenant_id: 1,
            exp: 0,
            scope: "ingest read".into(),
        };
        assert!(c.has_scope("ingest"));
        assert!(c.has_scope("read"));
        assert!(!c.has_scope("admin"));

        let read_only = Claims {
            sub: "u".into(),
            tenant_id: 1,
            exp: 0,
            scope: "".into(),
        };
        assert!(read_only.has_scope("read"));
        assert!(!read_only.has_scope("ingest"));
    }
}
