//! Encoding primitives for the segment format.
//!
//! This crate exposes byte-in / byte-out encoders for every page type that
//! `zen_format` produces. Each encoder is independently testable and has a
//! property test for round-trip correctness.

pub mod dict;
pub mod for_bitpack;
pub mod fsst;
pub mod gorilla;
pub mod rle;
pub mod zstd_page;

pub use dict::{DictBuilder, DictDecoder};
pub use for_bitpack::{for_decompress, for_encode};
pub use fsst::{FsstCompressor, FsstHeader};
pub use gorilla::{gorilla_decompress, gorilla_encode};
pub use rle::{rle_decompress, rle_encode};
pub use zstd_page::{zstd_compress, zstd_decompress};

/// Encoding tag stored in every page header so the reader knows how to decode.
#[repr(u8)]
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Encoding {
    /// Raw bytes (e.g. fixed-width binary like trace_id).
    Raw = 0,
    /// FSST + ZSTD wrap for wide string columns.
    FsstZstd = 1,
    /// Pure ZSTD (no FSST training data).
    Zstd = 2,
    /// Gorilla XOR for f64 series.
    Gorilla = 3,
    /// Frame-of-Reference + bit-packed varint for ints.
    For = 4,
    /// Run-length encoding.
    Rle = 5,
    /// Dictionary keys + LZ4 wrap.
    Dict = 6,
}

impl Encoding {
    pub fn as_u8(self) -> u8 {
        self as u8
    }
    pub fn try_from_u8(x: u8) -> Option<Self> {
        Some(match x {
            0 => Self::Raw,
            1 => Self::FsstZstd,
            2 => Self::Zstd,
            3 => Self::Gorilla,
            4 => Self::For,
            5 => Self::Rle,
            6 => Self::Dict,
            _ => return None,
        })
    }
}
