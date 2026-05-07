//! Magic constants identifying ZenithDB segment files.

pub const MAGIC_HEADER: &[u8; 8] = b"ZENSEGV1";
pub const MAGIC_TRAILER: &[u8; 8] = b"1VGESNEZ";
pub const FORMAT_VERSION: u32 = 1;
