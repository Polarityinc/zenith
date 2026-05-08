#![no_main]
//! Fuzz the ZenithQL/SQL parser. Any UTF-8 string must produce
//! `Ok(plan)` or `Err(ZenError)` — never a panic.
//!
//!   cargo +nightly fuzz run fuzz_zenithql_parse -- -max_total_time=300

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = zen_ql::parse(s, 0);
    }
});
