//! Segment file format for ZenithDB.
//!
//! A segment is one immutable object on object storage. Layout:
//!
//! ```text
//! [Magic header][Metadata len + bytes][Row groups][Inline indexes][Hotcache][Footer][Magic trailer]
//! ```
//!
//! See `magic.rs`, `meta.rs`, `row_group.rs`, `page.rs`, `hotcache.rs`,
//! `footer.rs`, `writer.rs`, `reader.rs` for details.

pub mod footer;
pub mod hotcache;
pub mod magic;
pub mod meta;
pub mod page;
pub mod reader;
pub mod row_group;
pub mod writer;

pub use footer::Footer;
pub use hotcache::{ColumnHotcacheEntry, Hotcache};
pub use magic::{FORMAT_VERSION, MAGIC_HEADER, MAGIC_TRAILER};
pub use meta::SegmentMetadata;
pub use page::{
    decode_one_row, decode_page, encode_page, ColumnValues, PageEncoding, PageView, RowValue,
};
pub use reader::SegmentReader;
pub use row_group::{ColumnPageDescriptor, RowGroupBuilder, RowGroupHeader, RowGroupReader};
pub use writer::SegmentWriter;
