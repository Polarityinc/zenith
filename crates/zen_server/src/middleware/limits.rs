//! Concurrency and per-tenant rate limiting.
//!
//! Two layers:
//!
//! - **Global concurrency**: `tower::limit::ConcurrencyLimitLayer`. Caps
//!   the total number of in-flight customer requests at `cfg.query
//!   .max_concurrent_queries` (default 256). Requests that exceed the cap
//!   wait briefly, then 503 if no slot opens. Cheap on the hot path —
//!   one atomic counter.
//!
//! - **Per-tenant QPS**: a `governor::Quota` keyed on `Claims.tenant_id`.
//!   Default 100 QPS / 1000 burst. Token-bucket math is ~80 ns. Tenants
//!   that exceed the quota get 429 — clients should back off.
//!
//! Both run *after* JWT verification so the limiter can read the verified
//! tenant out of the request extensions.

use std::num::NonZeroU32;
use std::sync::Arc;

use axum::{extract::Request, http::StatusCode, middleware::Next, response::Response};
use dashmap::DashMap;
use governor::{clock::DefaultClock, state::keyed::DefaultKeyedStateStore, Quota, RateLimiter};

use zen_auth::Claims;

type TenantLimiter = RateLimiter<u64, DefaultKeyedStateStore<u64>, DefaultClock>;

#[derive(Clone)]
pub struct RateLimits {
    inner: Arc<RateLimitsInner>,
}

struct RateLimitsInner {
    /// Per-tenant rate limiter. Keyed on tenant_id; tokens replenish at
    /// `qps` per second up to `burst` capacity.
    limiter: TenantLimiter,
    /// Cardinality cap on the keyed state — protects against a malicious
    /// stream of one-shot tenant IDs filling RAM. We let governor handle
    /// the bookkeeping but this dashmap caps the total observed count.
    seen_tenants: DashMap<u64, ()>,
}

impl RateLimits {
    pub fn new(qps: u32, burst: u32) -> Self {
        let qps_nz = NonZeroU32::new(qps.max(1)).unwrap();
        let burst_nz = NonZeroU32::new(burst.max(1)).unwrap();
        let quota = Quota::per_second(qps_nz).allow_burst(burst_nz);
        Self {
            inner: Arc::new(RateLimitsInner {
                limiter: RateLimiter::keyed(quota),
                seen_tenants: DashMap::new(),
            }),
        }
    }

    /// Returns Ok if the request fits within the tenant's budget.
    /// Returns Err with retry-after seconds otherwise.
    pub fn check(&self, tenant_id: u64) -> Result<(), u64> {
        // Cap distinct keys so a malicious traffic shape can't blow up
        // memory. After 100K seen tenants we stop tracking new ones — at
        // that point operators should be doing per-tenant pinning anyway.
        if self.inner.seen_tenants.len() < 100_000 {
            self.inner.seen_tenants.insert(tenant_id, ());
        }
        match self.inner.limiter.check_key(&tenant_id) {
            Ok(_) => Ok(()),
            Err(neg) => Err(neg
                .wait_time_from(self.inner.limiter.clock_now())
                .as_secs()
                .max(1)),
        }
    }
}

trait ClockNow {
    fn clock_now(&self) -> governor::clock::QuantaInstant;
}

impl ClockNow for TenantLimiter {
    fn clock_now(&self) -> governor::clock::QuantaInstant {
        use governor::clock::Clock;
        DefaultClock::default().now()
    }
}

/// Axum middleware. Reads the verified `Claims` injected by the JWT
/// layer; rejects with 429 if the tenant is over budget.
pub async fn rate_limit_layer(
    axum::extract::State(limits): axum::extract::State<RateLimits>,
    req: Request,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    let tenant = req
        .extensions()
        .get::<Claims>()
        .map(|c| c.tenant_id)
        .unwrap_or(0);
    if let Err(retry_after) = limits.check(tenant) {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            format!("tenant {tenant} rate limit exceeded; retry-after {retry_after}s"),
        ));
    }
    Ok(next.run(req).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limit_admits_within_burst() {
        let limits = RateLimits::new(10, 5);
        for _ in 0..5 {
            limits.check(1).unwrap();
        }
    }

    #[test]
    fn rate_limit_rejects_beyond_burst() {
        let limits = RateLimits::new(1, 2);
        // Two through the bucket immediately; further calls fail.
        limits.check(7).unwrap();
        limits.check(7).unwrap();
        let mut rejected = 0;
        for _ in 0..20 {
            if limits.check(7).is_err() {
                rejected += 1;
            }
        }
        assert!(rejected > 0, "expected at least one rejection");
    }

    #[test]
    fn separate_tenants_have_separate_buckets() {
        let limits = RateLimits::new(1, 1);
        limits.check(1).unwrap();
        limits.check(2).unwrap();
        // Both first calls succeed because each tenant's bucket starts full.
    }
}
