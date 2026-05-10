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

    fn mk_claims(sub: &str, tenant: u64, exp: i64, scope: &str) -> Claims {
        Claims {
            sub: sub.into(),
            tenant_id: tenant,
            exp,
            scope: scope.into(),
        }
    }

    /// `moka::sync::Cache` evicts by W-TinyLFU at the configured max
    /// capacity. Internally it batches admit/evict via a write buffer; the
    /// `run_pending_tasks` hook drains that buffer synchronously so we can
    /// observe the post-eviction state without sleeping. We then assert
    /// that the cache size is bounded by `cap`.
    #[test]
    fn cache_evicts_when_capacity_exceeded() {
        let cap: u64 = 8;
        let cache = ClaimsCache::new(cap, Duration::from_secs(60));
        // Insert 4× capacity.
        for i in 0..(cap * 4) {
            cache.insert(i, mk_claims("u", i, 0, "read"));
        }
        // Drain pending admit/evict work.
        cache.inner.run_pending_tasks();
        let size = cache.inner.entry_count();
        assert!(
            size <= cap,
            "size {size} must be ≤ capacity {cap} after pending tasks drain"
        );
        assert!(size > 0, "cache should still hold some entries");
    }

    /// Cache does not enforce token expiry — verifiers do, before insertion.
    /// Pin that contract: `get` happily returns claims with `exp` long past.
    #[test]
    fn get_returns_expired_claims_unchanged() {
        let cache = ClaimsCache::new(16, Duration::from_secs(60));
        let h = token_hash(b"already-expired");
        let expired = mk_claims("u1", 7, 1, "ingest"); // exp = 1 (epoch second 1)
        cache.insert(h, expired.clone());
        let got = cache.get(h).expect("present");
        // Cache returns the row; it doesn't filter on exp.
        assert_eq!(got.exp, 1);
        assert_eq!(got.tenant_id, 7);
        assert_eq!(got.scope, "ingest");
        // Sanity: epoch second 1 is, in fact, in the past.
        assert!(got.exp < chrono::Utc::now().timestamp());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_insert_from_many_tasks_is_safe() {
        let cache = ClaimsCache::new(8_192, Duration::from_secs(60));
        let mut handles = Vec::new();
        let n_tasks = 32u64;
        let per_task = 200u64;
        for t in 0..n_tasks {
            let cache = cache.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..per_task {
                    let h = (t << 16) | i;
                    cache.insert(h, mk_claims("u", t, 9_999_999_999, "read"));
                    // Read-back from inside the same task; with TTL=60s and
                    // capacity 8192 ≫ 32×200=6400, the entry must still be
                    // present immediately.
                    let got = cache.get(h).expect("just inserted");
                    assert_eq!(got.tenant_id, t);
                }
            }));
        }
        for h in handles {
            h.await.expect("task ok");
        }
        cache.inner.run_pending_tasks();
        let count = cache.inner.entry_count();
        assert!(count > 0);
    }

    #[test]
    fn has_scope_handles_empty_single_and_multi() {
        // Empty scope: only `read` passes (the doc'd policy).
        let empty = mk_claims("u", 1, 0, "");
        assert!(empty.has_scope("read"));
        assert!(!empty.has_scope("ingest"));
        assert!(!empty.has_scope("admin"));
        assert!(!empty.has_scope(""));

        // Single scope.
        let single = mk_claims("u", 1, 0, "admin");
        assert!(single.has_scope("admin"));
        assert!(!single.has_scope("read"));
        assert!(!single.has_scope("ingest"));

        // Multi scope, irregular whitespace (tabs + double space).
        let multi = mk_claims("u", 1, 0, "ingest \t read  admin");
        assert!(multi.has_scope("ingest"));
        assert!(multi.has_scope("read"));
        assert!(multi.has_scope("admin"));
        assert!(!multi.has_scope("compactor"));
        // Scope match must be exact: "ad" must not match "admin".
        assert!(!multi.has_scope("ad"));
        // And it must not be a substring match: "in" must not match "ingest".
        assert!(!multi.has_scope("in"));
    }
}
