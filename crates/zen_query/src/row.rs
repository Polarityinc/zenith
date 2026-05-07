//! Result-row representation. We don't return Arrow batches across the
//! network just yet — a JSON-friendly shape is easier for the HTTP API. The
//! gRPC path will swap in Arrow Flight in a follow-up.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResultRow {
    pub fields: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<ResultRow>,
    pub stats: ResultStats,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResultStats {
    pub segments_scanned: u32,
    pub row_groups_pruned: u32,
    pub row_groups_scanned: u32,
    pub rows_returned: u32,
    pub elapsed_ms: u64,
    pub bytes_decoded_wide: u64,
}
