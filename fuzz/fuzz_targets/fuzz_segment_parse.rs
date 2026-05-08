#![no_main]
//! Fuzz the segment-format parser. Random byte input must never panic.
//!
//!   cargo +nightly fuzz run fuzz_segment_parse -- -max_total_time=300

use libfuzzer_sys::fuzz_target;
use zen_format::reader::SegmentReader;

fuzz_target!(|data: &[u8]| {
    let _ = SegmentReader::from_bytes(data.to_vec());
});
