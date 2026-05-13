//! Chaos test: corrupt a segment file's bytes and verify the reader rejects it.

use zen_common::{CommitId, PartitionId, SchemaFingerprint, SpanId, TenantId, TraceId};
use zen_format::{
    encode_page, ColumnValues, PageEncoding, RowGroupBuilder, SegmentMetadata, SegmentReader,
    SegmentWriter,
};

fn build_simple_segment_bytes() -> Vec<u8> {
    let fp = SchemaFingerprint(0x1234);
    let mut meta = SegmentMetadata::new(
        1,
        TenantId(7),
        PartitionId(0),
        fp,
        vec!["trace_id".into(), "start_time_ms".into()],
        vec!["trace_id".into()],
    );
    meta.observe_time(1000);
    meta.observe_time(2000);
    meta.observe_commit(CommitId(1));
    meta.observe_commit(CommitId(99));
    meta.observe_trace_id(TraceId([0x10; 16]));
    meta.observe_trace_id(TraceId([0x20; 16]));
    meta.observe_span_id(SpanId([0x00; 16]));
    meta.observe_span_id(SpanId([0xFF; 16]));

    let mut writer = SegmentWriter::new(meta);
    let mut rgb = RowGroupBuilder::new(2);
    let trace_ids: Vec<[u8; 16]> = vec![[0x10; 16], [0x20; 16]];
    let (e, b) = encode_page(ColumnValues::Fixed16(trace_ids), PageEncoding::FixedRaw).unwrap();
    rgb.add_page(0, e, b.to_vec(), 32);
    let times: Vec<i64> = vec![1000, 2000];
    let (e, b) = encode_page(ColumnValues::I64(times), PageEncoding::For).unwrap();
    rgb.add_page(1, e, b.to_vec(), 16);
    let (payload, header) = rgb.finish();
    writer.add_row_group(header, payload);
    writer.finish().unwrap().to_vec()
}

#[test]
fn corrupt_magic_is_rejected() {
    let mut bytes = build_simple_segment_bytes();
    bytes[0] ^= 0xFF;
    assert!(SegmentReader::from_bytes(bytes).is_err());
}

#[test]
fn corrupt_trailer_is_rejected() {
    let mut bytes = build_simple_segment_bytes();
    let n = bytes.len();
    bytes[n - 1] ^= 0xFF;
    assert!(SegmentReader::from_bytes(bytes).is_err());
}

#[test]
fn happy_path_still_opens() {
    let bytes = build_simple_segment_bytes();
    SegmentReader::from_bytes(bytes).unwrap();
}
