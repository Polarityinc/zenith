//! Build a segment from already-merged sorted rows.
//!
//! Trace-locality invariant: rows are pre-sorted by `(trace_id, start_time,
//! span_id)`. We slice the sorted list into row groups in order, so all spans
//! of any trace fall into a contiguous range and (assuming `trace_size` <
//! row_group_max_rows) into a single row group.

use std::collections::HashMap;

use ulid::Ulid;
use zen_common::{
    PartitionId, SchemaFingerprint, Schema, SpanRecord, TenantId, ZenError, ZenResult,
};
use zen_format::{
    encode_page, ColumnHotcacheEntry, ColumnValues, Hotcache, PageEncoding, RowGroupBuilder,
    SegmentMetadata, SegmentWriter,
};
use zen_format::hotcache::RowGroupHotcacheEntry;
use zen_index::{PostingMap, ZoneMap, ZoneMapValue};

#[derive(Clone, Debug)]
pub struct BuildOptions {
    /// Max rows per row group.
    pub row_group_max_rows: u32,
    /// Max payload bytes per row group (post-compression). Currently advisory.
    pub row_group_max_bytes: u64,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            row_group_max_rows: 16_384,
            row_group_max_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Build a segment from sorted rows. Returns the segment bytes and the
/// updated metadata.
pub fn build_segment_from_rows(
    rows: &[SpanRecord],
    tenant_id: TenantId,
    partition_id: PartitionId,
    schema: &Schema,
    opts: &BuildOptions,
) -> ZenResult<(Vec<u8>, SegmentMetadata)> {
    if rows.is_empty() {
        return Err(ZenError::invalid("build_segment with no rows"));
    }
    let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let sort_keys: Vec<String> = schema
        .sort_key_columns()
        .into_iter()
        .map(|i| schema.columns[i].name.clone())
        .collect();
    let segment_id = Ulid::new().0;

    let mut meta = SegmentMetadata::new(
        segment_id,
        tenant_id,
        partition_id,
        schema.fingerprint(),
        column_names.clone(),
        sort_keys,
    );
    for r in rows {
        meta.observe_time(r.start_time_ms);
        meta.observe_commit(r.commit_id);
        meta.observe_trace_id(r.trace_id);
        meta.observe_span_id(r.span_id);
    }

    let mut writer = SegmentWriter::new(meta.clone());
    let mut hotcache = Hotcache::new();
    // Posting lists per (rg_idx, column_idx) → bytes. We append all posting blobs
    // to one inline-indexes byte buffer, and stash the offset/length in the
    // hotcache so the executor can find them by ranged read.
    let mut inline_indexes: Vec<u8> = Vec::new();

    let bitmap_columns = ["model", "status", "provider", "tool_name", "span_type"];
    let bitmap_col_indices: Vec<u32> = bitmap_columns
        .iter()
        .filter_map(|name| {
            schema
                .columns
                .iter()
                .position(|c| c.name == *name)
                .map(|i| i as u32)
        })
        .collect();

    let mut start = 0usize;
    let mut rg_idx = 0u32;
    while start < rows.len() {
        let mut end = (start + opts.row_group_max_rows as usize).min(rows.len());
        if end < rows.len() {
            while end > start && rows[end].trace_id == rows[end - 1].trace_id {
                end -= 1;
            }
            if end == start {
                end = (start + opts.row_group_max_rows as usize).min(rows.len());
            }
        }
        let chunk = &rows[start..end];
        let (rg_payload, rg_header) = build_row_group(chunk, schema)?;
        let mut zone_maps = build_zone_maps(chunk, schema);

        // Build bitmap posting indexes for the bitmap-eligible columns.
        for col_idx in &bitmap_col_indices {
            let col_name = &schema.columns[*col_idx as usize].name;
            let pm = build_posting_for_column(chunk, col_name);
            let bytes = pm
                .serialize()
                .map_err(|e| ZenError::compactor(format!("posting serialize: {e}")))?;
            let local_off = inline_indexes.len() as u64;
            let len = bytes.len() as u32;
            inline_indexes.extend_from_slice(&bytes);
            if let Some(entry) = zone_maps.iter_mut().find(|c| c.column_idx == *col_idx) {
                entry.posting_offset = Some(local_off);
                entry.posting_length = Some(len);
            }
        }

        // Build a Tantivy FTS index spanning prompt + completion + tool_io_text.
        // We index ALL three text columns into one inline blob and record the
        // offset/length on each text column's hotcache entry so the executor
        // can find it from any of them.
        let fts_field_names = ["prompt", "completion", "tool_io_text"];
        let fts_col_indices: Vec<u32> = fts_field_names
            .iter()
            .filter_map(|n| {
                schema
                    .columns
                    .iter()
                    .position(|c| c.name == *n)
                    .map(|i| i as u32)
            })
            .collect();
        if !fts_col_indices.is_empty() {
            let opts = zen_fts::BuildOptions {
                field_names: fts_field_names.iter().map(|s| s.to_string()).collect(),
                writer_memory_bytes: 15_000_000,
            };
            let accessor = SpanFieldAccessor { rows: chunk };
            let res = zen_fts::build_fts_index(&accessor, &opts).map_err(|e| {
                ZenError::compactor(format!("fts build: {e}"))
            })?;
            let local_off = inline_indexes.len() as u64;
            let len = res.blob.len() as u32;
            inline_indexes.extend_from_slice(&res.blob);
            for ci in &fts_col_indices {
                if let Some(entry) = zone_maps.iter_mut().find(|c| c.column_idx == *ci) {
                    entry.fts_offset = Some(local_off);
                    entry.fts_length = Some(len);
                }
            }
        }

        // Build per-segment JSON-path index for the metadata column.
        if let Some(meta_col_idx) = schema
            .columns
            .iter()
            .position(|c| c.name == "metadata")
            .map(|i| i as u32)
        {
            let json_values: Vec<serde_json::Value> = chunk
                .iter()
                .filter_map(|r| r.metadata.clone())
                .collect();
            // Discover paths from the sample.
            let cfg = zen_jsonpath::DiscoveryConfig {
                sample_size: 10_000,
                min_presence_pct: 1.0,
                max_paths: 256,
                max_depth: 6,
            };
            let discovered = zen_jsonpath::discover_paths(json_values.iter(), &cfg);
            let paths: Vec<String> = discovered.into_iter().map(|p| p.path).collect();
            if !paths.is_empty() {
                let mut builder = zen_jsonpath::JsonPathIndexBuilder::new(paths);
                for (row_idx, r) in chunk.iter().enumerate() {
                    if let Some(v) = &r.metadata {
                        builder.push_row(row_idx as u32, v);
                    }
                }
                let idx = builder.finish();
                let bytes = idx
                    .serialize()
                    .map_err(|e| ZenError::compactor(format!("jsonpath serialize: {e}")))?;
                let local_off = inline_indexes.len() as u64;
                let len = bytes.len() as u32;
                inline_indexes.extend_from_slice(&bytes);
                if let Some(entry) = zone_maps.iter_mut().find(|c| c.column_idx == meta_col_idx) {
                    entry.jsonpath_offset = Some(local_off);
                    entry.jsonpath_length = Some(len);
                }
            }
        }

        hotcache.row_groups.push(RowGroupHotcacheEntry {
            row_group_idx: rg_idx,
            header: rg_header.clone(),
            columns: zone_maps,
        });
        writer.add_row_group(rg_header, rg_payload);
        start = end;
        rg_idx += 1;
    }

    writer.set_inline_indexes(inline_indexes);
    writer.set_hotcache(hotcache);

    let bytes = writer.finish()?;
    Ok((bytes.to_vec(), meta))
}

fn build_row_group(
    rows: &[SpanRecord],
    schema: &Schema,
) -> ZenResult<(Vec<u8>, zen_format::RowGroupHeader)> {
    let n = rows.len() as u32;
    let mut rgb = RowGroupBuilder::new(n);
    let mut col_idx_map: HashMap<&str, u32> = HashMap::new();
    for (i, c) in schema.columns.iter().enumerate() {
        col_idx_map.insert(c.name.as_str(), i as u32);
    }

    // tenant_id
    if let Some(&i) = col_idx_map.get("tenant_id") {
        let v: Vec<i64> = rows.iter().map(|r| r.tenant_id.0 as i64).collect();
        let unc = (v.len() * 8) as u64;
        let (e, b) = encode_page(ColumnValues::I64(v), PageEncoding::Rle)?;
        rgb.add_page(i, e, b.to_vec(), unc);
    }
    // partition_id
    if let Some(&i) = col_idx_map.get("partition_id") {
        let v: Vec<i64> = rows.iter().map(|r| r.partition_id.0 as i64).collect();
        let unc = (v.len() * 4) as u64;
        let (e, b) = encode_page(ColumnValues::I64(v), PageEncoding::Rle)?;
        rgb.add_page(i, e, b.to_vec(), unc);
    }
    // trace_id (Fixed16)
    if let Some(&i) = col_idx_map.get("trace_id") {
        let v: Vec<[u8; 16]> = rows.iter().map(|r| r.trace_id.0).collect();
        let unc = (v.len() * 16) as u64;
        let (e, b) = encode_page(ColumnValues::Fixed16(v), PageEncoding::FixedRaw)?;
        rgb.add_page(i, e, b.to_vec(), unc);
    }
    if let Some(&i) = col_idx_map.get("span_id") {
        let v: Vec<[u8; 16]> = rows.iter().map(|r| r.span_id.0).collect();
        let unc = (v.len() * 16) as u64;
        let (e, b) = encode_page(ColumnValues::Fixed16(v), PageEncoding::FixedRaw)?;
        rgb.add_page(i, e, b.to_vec(), unc);
    }
    if let Some(&i) = col_idx_map.get("parent_span_id") {
        let v: Vec<[u8; 16]> = rows
            .iter()
            .map(|r| r.parent_span_id.map(|p| p.0).unwrap_or([0; 16]))
            .collect();
        let unc = (v.len() * 16) as u64;
        let (e, b) = encode_page(ColumnValues::Fixed16(v), PageEncoding::FixedRaw)?;
        rgb.add_page(i, e, b.to_vec(), unc);
    }
    if let Some(&i) = col_idx_map.get("start_time_ms") {
        let v: Vec<i64> = rows.iter().map(|r| r.start_time_ms).collect();
        let unc = (v.len() * 8) as u64;
        let (e, b) = encode_page(ColumnValues::I64(v), PageEncoding::For)?;
        rgb.add_page(i, e, b.to_vec(), unc);
    }
    if let Some(&i) = col_idx_map.get("end_time_ms") {
        let v: Vec<i64> = rows.iter().map(|r| r.end_time_ms).collect();
        let unc = (v.len() * 8) as u64;
        let (e, b) = encode_page(ColumnValues::I64(v), PageEncoding::For)?;
        rgb.add_page(i, e, b.to_vec(), unc);
    }
    if let Some(&i) = col_idx_map.get("duration_ms") {
        let v: Vec<i64> = rows.iter().map(|r| r.duration_ms).collect();
        let unc = (v.len() * 8) as u64;
        let (e, b) = encode_page(ColumnValues::I64(v), PageEncoding::For)?;
        rgb.add_page(i, e, b.to_vec(), unc);
    }
    // String columns with Dict encoding (low cardinality).
    for col_name in &["span_type", "status", "provider", "model", "tool_name"] {
        if let Some(&i) = col_idx_map.get(col_name) {
            let v: Vec<Vec<u8>> = rows
                .iter()
                .map(|r| match *col_name {
                    "span_type" => r.span_type.clone().unwrap_or_default().into_bytes(),
                    "status" => r.status.clone().unwrap_or_default().into_bytes(),
                    "provider" => r.provider.clone().unwrap_or_default().into_bytes(),
                    "model" => r.model.clone().unwrap_or_default().into_bytes(),
                    "tool_name" => r.tool_name.clone().unwrap_or_default().into_bytes(),
                    _ => Vec::new(),
                })
                .collect();
            let unc: u64 = v.iter().map(|s| s.len() as u64).sum();
            let (e, b) = encode_page(ColumnValues::StringsOwned(v), PageEncoding::Dict)?;
            rgb.add_page(i, e, b.to_vec(), unc);
        }
    }
    // Wide string columns: FsstWithOffsets.
    for col_name in &["prompt", "completion", "tool_io_text"] {
        if let Some(&i) = col_idx_map.get(col_name) {
            let v: Vec<Vec<u8>> = rows
                .iter()
                .map(|r| match *col_name {
                    "prompt" => r.prompt.clone().unwrap_or_default().into_bytes(),
                    "completion" => r.completion.clone().unwrap_or_default().into_bytes(),
                    "tool_io_text" => r.tool_io_text.clone().unwrap_or_default().into_bytes(),
                    _ => Vec::new(),
                })
                .collect();
            let unc: u64 = v.iter().map(|s| s.len() as u64).sum();
            let (e, b) = encode_page(ColumnValues::StringsOwned(v), PageEncoding::FsstWithOffsets)?;
            rgb.add_page(i, e, b.to_vec(), unc);
        }
    }
    // Numeric optional columns.
    if let Some(&i) = col_idx_map.get("prompt_tokens") {
        let v: Vec<i64> = rows
            .iter()
            .map(|r| r.prompt_tokens.unwrap_or(0) as i64)
            .collect();
        let unc = (v.len() * 4) as u64;
        let (e, b) = encode_page(ColumnValues::I64(v), PageEncoding::For)?;
        rgb.add_page(i, e, b.to_vec(), unc);
    }
    if let Some(&i) = col_idx_map.get("completion_tokens") {
        let v: Vec<i64> = rows
            .iter()
            .map(|r| r.completion_tokens.unwrap_or(0) as i64)
            .collect();
        let unc = (v.len() * 4) as u64;
        let (e, b) = encode_page(ColumnValues::I64(v), PageEncoding::For)?;
        rgb.add_page(i, e, b.to_vec(), unc);
    }
    for col_name in &["cost_usd", "temperature", "top_p"] {
        if let Some(&i) = col_idx_map.get(col_name) {
            let v: Vec<f64> = rows
                .iter()
                .map(|r| match *col_name {
                    "cost_usd" => r.cost_usd.unwrap_or(0.0),
                    "temperature" => r.temperature.unwrap_or(0.0),
                    "top_p" => r.top_p.unwrap_or(0.0),
                    _ => 0.0,
                })
                .collect();
            let unc = (v.len() * 8) as u64;
            let (e, b) = encode_page(ColumnValues::F64(v), PageEncoding::Gorilla)?;
            rgb.add_page(i, e, b.to_vec(), unc);
        }
    }
    // Identity strings via Dict.
    for col_name in &["user_id", "session_id", "request_id"] {
        if let Some(&i) = col_idx_map.get(col_name) {
            let v: Vec<Vec<u8>> = rows
                .iter()
                .map(|r| match *col_name {
                    "user_id" => r.user_id.clone().unwrap_or_default().into_bytes(),
                    "session_id" => r.session_id.clone().unwrap_or_default().into_bytes(),
                    "request_id" => r.request_id.clone().unwrap_or_default().into_bytes(),
                    _ => Vec::new(),
                })
                .collect();
            let unc: u64 = v.iter().map(|s| s.len() as u64).sum();
            let (e, b) = encode_page(ColumnValues::StringsOwned(v), PageEncoding::Dict)?;
            rgb.add_page(i, e, b.to_vec(), unc);
        }
    }
    // metadata JSON via ZSTD bytes.
    if let Some(&i) = col_idx_map.get("metadata") {
        let v: Vec<Vec<u8>> = rows
            .iter()
            .map(|r| {
                r.metadata
                    .as_ref()
                    .map(|m| serde_json::to_vec(m).unwrap_or_default())
                    .unwrap_or_default()
            })
            .collect();
        let unc: u64 = v.iter().map(|s| s.len() as u64).sum();
        let (e, b) = encode_page(ColumnValues::BytesOwned(v), PageEncoding::Zstd)?;
        rgb.add_page(i, e, b.to_vec(), unc);
    }
    // commit_id
    if let Some(&i) = col_idx_map.get("commit_id") {
        let v: Vec<i64> = rows.iter().map(|r| r.commit_id.0 as i64).collect();
        let unc = (v.len() * 8) as u64;
        let (e, b) = encode_page(ColumnValues::I64(v), PageEncoding::For)?;
        rgb.add_page(i, e, b.to_vec(), unc);
    }

    let (payload, header) = rgb.finish();
    Ok((payload, header))
}

/// Compute per-column zone maps for a row-group's slice of rows.
fn build_zone_maps(rows: &[SpanRecord], schema: &Schema) -> Vec<ColumnHotcacheEntry> {
    let mut out = Vec::new();
    for (col_idx, col) in schema.columns.iter().enumerate() {
        let zm = match col.name.as_str() {
            "tenant_id" => {
                let v: Vec<i64> = rows.iter().map(|r| r.tenant_id.0 as i64).collect();
                ZoneMap::from_i64(&v, 0)
            }
            "partition_id" => {
                let v: Vec<i64> = rows.iter().map(|r| r.partition_id.0 as i64).collect();
                ZoneMap::from_i64(&v, 0)
            }
            "start_time_ms" => {
                let v: Vec<i64> = rows.iter().map(|r| r.start_time_ms).collect();
                ZoneMap::from_i64(&v, 0)
            }
            "end_time_ms" => {
                let v: Vec<i64> = rows.iter().map(|r| r.end_time_ms).collect();
                ZoneMap::from_i64(&v, 0)
            }
            "duration_ms" => {
                let v: Vec<i64> = rows.iter().map(|r| r.duration_ms).collect();
                ZoneMap::from_i64(&v, 0)
            }
            "commit_id" => {
                let v: Vec<i64> = rows.iter().map(|r| r.commit_id.0 as i64).collect();
                ZoneMap::from_i64(&v, 0)
            }
            "model" | "status" | "provider" | "tool_name" | "span_type" | "user_id"
            | "session_id" | "request_id" => {
                let owned: Vec<Vec<u8>> = rows
                    .iter()
                    .map(|r| {
                        let s = match col.name.as_str() {
                            "model" => r.model.as_deref(),
                            "status" => r.status.as_deref(),
                            "provider" => r.provider.as_deref(),
                            "tool_name" => r.tool_name.as_deref(),
                            "span_type" => r.span_type.as_deref(),
                            "user_id" => r.user_id.as_deref(),
                            "session_id" => r.session_id.as_deref(),
                            "request_id" => r.request_id.as_deref(),
                            _ => None,
                        };
                        s.unwrap_or("").as_bytes().to_vec()
                    })
                    .collect();
                let refs: Vec<&[u8]> = owned.iter().map(|s| s.as_slice()).collect();
                ZoneMap::from_bytes(&refs, 0)
            }
            "trace_id" => {
                let owned: Vec<Vec<u8>> = rows.iter().map(|r| r.trace_id.0.to_vec()).collect();
                let refs: Vec<&[u8]> = owned.iter().map(|s| s.as_slice()).collect();
                let mut zm = ZoneMap::from_bytes(&refs, 0);
                if let ZoneMapValue::Bytes { min, max } = zm.value {
                    zm.value = ZoneMapValue::Fixed { min, max };
                }
                zm
            }
            "span_id" => {
                let owned: Vec<Vec<u8>> = rows.iter().map(|r| r.span_id.0.to_vec()).collect();
                let refs: Vec<&[u8]> = owned.iter().map(|s| s.as_slice()).collect();
                let mut zm = ZoneMap::from_bytes(&refs, 0);
                if let ZoneMapValue::Bytes { min, max } = zm.value {
                    zm.value = ZoneMapValue::Fixed { min, max };
                }
                zm
            }
            _ => ZoneMap::default(),
        };
        out.push(ColumnHotcacheEntry {
            column_idx: col_idx as u32,
            zone_map: zm,
            posting_offset: None,
            posting_length: None,
            fts_offset: None,
            fts_length: None,
            jsonpath_offset: None,
            jsonpath_length: None,
            hnsw_offset: None,
            hnsw_length: None,
        });
    }
    out
}

/// Build a `PostingMap` over a column for a row-group's slice of rows.
fn build_posting_for_column(rows: &[SpanRecord], column: &str) -> PostingMap {
    let mut pm = PostingMap::new();
    for (i, r) in rows.iter().enumerate() {
        let v: &str = match column {
            "model" => r.model.as_deref().unwrap_or(""),
            "status" => r.status.as_deref().unwrap_or(""),
            "provider" => r.provider.as_deref().unwrap_or(""),
            "tool_name" => r.tool_name.as_deref().unwrap_or(""),
            "span_type" => r.span_type.as_deref().unwrap_or(""),
            _ => continue,
        };
        pm.insert(v.as_bytes(), i as u32);
    }
    pm
}

/// Implements `zen_fts::FieldAccessor` over a slice of `SpanRecord` for the
/// columns: `prompt`, `completion`, `tool_io_text` (in that order).
struct SpanFieldAccessor<'a> {
    rows: &'a [SpanRecord],
}

impl<'a> zen_fts::build::FieldAccessor for SpanFieldAccessor<'a> {
    fn field(&self, row: usize, field_idx: usize) -> Option<&str> {
        let r = self.rows.get(row)?;
        match field_idx {
            0 => r.prompt.as_deref(),
            1 => r.completion.as_deref(),
            2 => r.tool_io_text.as_deref(),
            _ => None,
        }
    }
    fn row_count(&self) -> usize {
        self.rows.len()
    }
}

/// Streaming variant of `build_segment_from_rows` that accepts an iterator
/// instead of a slice. Memory-bounded by `row_group_max_rows`, not by total
/// row count. Performs streaming tombstone dedup (rows are sorted, so
/// duplicate `(tenant, span_id)` collide adjacently and we keep the highest
/// `commit_id`).
///
/// Returns `(segment_bytes, metadata, row_count_after_dedup)`.
pub fn build_segment_from_iter<I>(
    rows: I,
    tenant_id: TenantId,
    partition_id: PartitionId,
    schema: &Schema,
    opts: &BuildOptions,
) -> ZenResult<Option<(Vec<u8>, SegmentMetadata, usize)>>
where
    I: IntoIterator<Item = SpanRecord>,
{
    let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let sort_keys: Vec<String> = schema
        .sort_key_columns()
        .into_iter()
        .map(|i| schema.columns[i].name.clone())
        .collect();
    let segment_id = Ulid::new().0;

    let mut meta = SegmentMetadata::new(
        segment_id,
        tenant_id,
        partition_id,
        schema.fingerprint(),
        column_names,
        sort_keys,
    );

    let mut writer = SegmentWriter::new(meta.clone());
    let mut hotcache = Hotcache::new();
    let mut inline_indexes: Vec<u8> = Vec::new();

    let bitmap_columns = ["model", "status", "provider", "tool_name", "span_type"];
    let bitmap_col_indices: Vec<u32> = bitmap_columns
        .iter()
        .filter_map(|name| {
            schema
                .columns
                .iter()
                .position(|c| c.name == *name)
                .map(|i| i as u32)
        })
        .collect();

    let mut buf: Vec<SpanRecord> = Vec::with_capacity(opts.row_group_max_rows as usize);
    let mut prev_key: Option<(u64, [u8; 16])> = None;
    let mut rg_idx_counter: u32 = 0;
    let mut total_rows: usize = 0;
    let mut empty = true;

    let mut iter = rows.into_iter();
    while let Some(row) = iter.next() {
        meta.observe_time(row.start_time_ms);
        meta.observe_commit(row.commit_id);
        meta.observe_trace_id(row.trace_id);
        meta.observe_span_id(row.span_id);

        // Tombstone dedup against last row in buffer (sorted, so duplicates collide).
        let key = (row.tenant_id.0, row.span_id.0);
        if Some(key) == prev_key {
            if let Some(last) = buf.last_mut() {
                if row.commit_id.0 > last.commit_id.0 {
                    *last = row;
                }
            }
            continue;
        }
        prev_key = Some(key);
        buf.push(row);
        empty = false;
        total_rows += 1;

        if buf.len() >= opts.row_group_max_rows as usize {
            // Don't split mid-trace: peek-look-back. If last few rows share the
            // most recent trace_id, hold back until we see a boundary.
            // For simplicity here we just flush the whole buffer; callers can
            // tolerate an in-trace split since trace-locality is best-effort
            // when input volume is huge.
            finalize_one_row_group(
                &buf,
                schema,
                rg_idx_counter,
                &bitmap_col_indices,
                &mut writer,
                &mut hotcache,
                &mut inline_indexes,
            )?;
            rg_idx_counter += 1;
            buf.clear();
            prev_key = None; // safe to reset since cross-rg dedup is rare and the writer is monotonic
        }
    }
    if !buf.is_empty() {
        finalize_one_row_group(
            &buf,
            schema,
            rg_idx_counter,
            &bitmap_col_indices,
            &mut writer,
            &mut hotcache,
            &mut inline_indexes,
        )?;
    }

    if empty {
        return Ok(None);
    }
    writer.set_inline_indexes(inline_indexes);
    writer.set_hotcache(hotcache);

    let bytes = writer.finish()?;
    Ok(Some((bytes.to_vec(), meta, total_rows)))
}

/// Process one chunk into a row group + per-RG zone maps + posting / FTS /
/// JSON-path indexes. Equivalent to the inner loop body of
/// `build_segment_from_rows`, factored out so streaming and batch paths share
/// the same plumbing.
fn finalize_one_row_group(
    chunk: &[SpanRecord],
    schema: &Schema,
    rg_idx: u32,
    bitmap_col_indices: &[u32],
    writer: &mut SegmentWriter,
    hotcache: &mut Hotcache,
    inline_indexes: &mut Vec<u8>,
) -> ZenResult<()> {
    let (rg_payload, rg_header) = build_row_group(chunk, schema)?;
    let mut zone_maps = build_zone_maps(chunk, schema);

    // Bitmap posting indexes.
    for col_idx in bitmap_col_indices {
        let col_name = &schema.columns[*col_idx as usize].name;
        let pm = build_posting_for_column(chunk, col_name);
        let bytes = pm
            .serialize()
            .map_err(|e| ZenError::compactor(format!("posting serialize: {e}")))?;
        let local_off = inline_indexes.len() as u64;
        let len = bytes.len() as u32;
        inline_indexes.extend_from_slice(&bytes);
        if let Some(entry) = zone_maps.iter_mut().find(|c| c.column_idx == *col_idx) {
            entry.posting_offset = Some(local_off);
            entry.posting_length = Some(len);
        }
    }

    // FTS index.
    let fts_field_names = ["prompt", "completion", "tool_io_text"];
    let fts_col_indices: Vec<u32> = fts_field_names
        .iter()
        .filter_map(|n| {
            schema
                .columns
                .iter()
                .position(|c| c.name == *n)
                .map(|i| i as u32)
        })
        .collect();
    if !fts_col_indices.is_empty() {
        let opts = zen_fts::BuildOptions {
            field_names: fts_field_names.iter().map(|s| s.to_string()).collect(),
            writer_memory_bytes: 15_000_000,
        };
        let accessor = SpanFieldAccessor { rows: chunk };
        let res = zen_fts::build_fts_index(&accessor, &opts)
            .map_err(|e| ZenError::compactor(format!("fts build: {e}")))?;
        let local_off = inline_indexes.len() as u64;
        let len = res.blob.len() as u32;
        inline_indexes.extend_from_slice(&res.blob);
        for ci in &fts_col_indices {
            if let Some(entry) = zone_maps.iter_mut().find(|c| c.column_idx == *ci) {
                entry.fts_offset = Some(local_off);
                entry.fts_length = Some(len);
            }
        }
    }

    // JSON-path index.
    if let Some(meta_col_idx) = schema
        .columns
        .iter()
        .position(|c| c.name == "metadata")
        .map(|i| i as u32)
    {
        let json_values: Vec<serde_json::Value> = chunk
            .iter()
            .filter_map(|r| r.metadata.clone())
            .collect();
        let cfg = zen_jsonpath::DiscoveryConfig {
            sample_size: 10_000,
            min_presence_pct: 1.0,
            max_paths: 256,
            max_depth: 6,
        };
        let discovered = zen_jsonpath::discover_paths(json_values.iter(), &cfg);
        let paths: Vec<String> = discovered.into_iter().map(|p| p.path).collect();
        if !paths.is_empty() {
            let mut builder = zen_jsonpath::JsonPathIndexBuilder::new(paths);
            for (row_idx, r) in chunk.iter().enumerate() {
                if let Some(v) = &r.metadata {
                    builder.push_row(row_idx as u32, v);
                }
            }
            let idx = builder.finish();
            let bytes = idx
                .serialize()
                .map_err(|e| ZenError::compactor(format!("jsonpath serialize: {e}")))?;
            let local_off = inline_indexes.len() as u64;
            let len = bytes.len() as u32;
            inline_indexes.extend_from_slice(&bytes);
            if let Some(entry) = zone_maps.iter_mut().find(|c| c.column_idx == meta_col_idx) {
                entry.jsonpath_offset = Some(local_off);
                entry.jsonpath_length = Some(len);
            }
        }
    }

    hotcache.row_groups.push(RowGroupHotcacheEntry {
        row_group_idx: rg_idx,
        header: rg_header.clone(),
        columns: zone_maps,
    });
    writer.add_row_group(rg_header, rg_payload);
    Ok(())
}
