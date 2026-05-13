//! Write-ahead log on object storage.
//!
//! WAL objects use Arrow IPC + ZSTD so the querier can read unsealed WALs
//! directly without compaction. Each WAL object is named
//! `wal/{tenant}/{partition}/{commit_id}-{ulid}.wal`, and is uploaded with
//! `put_if_absent` so that two writers racing on the same key never both
//! succeed (the loser increments commit-id and retries).

pub mod format;
pub mod reader;
pub mod writer;

pub use format::{WalHeader, WalObjectKey, FORMAT_VERSION, MAGIC};
pub use reader::WalReader;
pub use writer::WalWriter;
