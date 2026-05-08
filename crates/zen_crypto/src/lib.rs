//! Envelope encryption for ZenithDB segments.
//!
//! - Each segment gets its own 32-byte AES-256 data key.
//! - The data key is wrapped by a "root key" (in production, a KMS-held
//!   key; in single-node deployments, a static key from config). Wrapping
//!   uses AES-256-GCM with a per-segment nonce so identical data keys
//!   never produce identical wrapped bytes.
//! - The plaintext segment payload is then encrypted with AES-256-GCM
//!   using the data key, again with a fresh nonce.
//!
//! On hardware with AES-NI / NEON crypto extensions (essentially every
//! Linux x86_64 server and every Apple Silicon Mac), this runs at ~5-10
//! GB/s — at or above NVMe sequential read bandwidth, so the warm-cache
//! query path is unaffected and cold reads pay <5%.
//!
//! Wire format of an encrypted segment (back-compat: an unencrypted
//! segment doesn't carry this header — `EnvelopeHeader::detect` returns
//! None on legacy bytes):
//!
//! ```text
//! [magic 4: ZENV] [ver 1] [data_key_nonce 12] [wrapped_data_key 32+16]
//! [payload_nonce 12] [ciphertext...] [tag 16]
//! ```

pub mod envelope;
pub mod root;

pub use envelope::{decrypt, encrypt, EnvelopeError};
pub use root::RootKey;
