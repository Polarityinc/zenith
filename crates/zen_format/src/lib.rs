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

pub mod magic;
pub mod meta;
pub mod row_group;
pub mod page;
pub mod hotcache;
pub mod footer;
pub mod writer;
pub mod reader;

pub use magic::{MAGIC_HEADER, MAGIC_TRAILER, FORMAT_VERSION};
pub use meta::SegmentMetadata;
pub use row_group::{RowGroupBuilder, RowGroupReader, RowGroupHeader, ColumnPageDescriptor};
pub use page::{ColumnValues, PageEncoding, PageView, RowValue, decode_one_row, decode_page, encode_page};
pub use hotcache::{Hotcache, ColumnHotcacheEntry};
pub use footer::Footer;
pub use writer::SegmentWriter;
pub use reader::SegmentReader;
