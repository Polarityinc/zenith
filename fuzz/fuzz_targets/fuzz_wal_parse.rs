#![no_main]
//! Fuzz the WAL object parser. Random byte input must never panic; the
//! parser must always return a structured `Result`.
//!
//!   cargo +nightly fuzz run fuzz_wal_parse -- -max_total_time=300

use libfuzzer_sys::fuzz_target;
use zen_wal::format::parse_wal_object;

fuzz_target!(|data: &[u8]| {
    let _ = parse_wal_object(data);
});
