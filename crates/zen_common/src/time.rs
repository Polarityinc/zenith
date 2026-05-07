//! Wall-clock helpers in milliseconds since epoch.
//!
//! All timestamps in segments and WAL records are i64 milliseconds since UNIX epoch.
//! Sub-millisecond resolution is not needed for the AI-trace workload — span durations
//! cluster around 100ms-30s — and i64 ms keeps storage cheap (delta-of-delta + bit-pack).

use std::time::{SystemTime, UNIX_EPOCH};

/// Lightweight wrapper around a UNIX millisecond timestamp.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct MilliInstant(pub i64);

impl MilliInstant {
    pub const ZERO: MilliInstant = MilliInstant(0);

    pub fn now() -> Self {
        Self(now_millis())
    }

    pub fn ms(&self) -> i64 {
        self.0
    }

    pub fn elapsed_since(&self, earlier: Self) -> i64 {
        self.0 - earlier.0
    }
}

#[inline]
pub fn now_millis() -> i64 {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    d.as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_millis_is_recent() {
        let t = now_millis();
        // 2026-05-06 epoch ms is roughly 1.778e12; sanity check.
        assert!(t > 1_700_000_000_000);
        assert!(t < 4_000_000_000_000);
    }

    #[test]
    fn milli_instant_elapsed_monotonic() {
        let a = MilliInstant::now();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = MilliInstant::now();
        assert!(b.elapsed_since(a) >= 1);
    }
}
