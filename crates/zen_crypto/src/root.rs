//! Root key abstraction. Wraps and unwraps per-segment data keys.
//!
//! In production this should be backed by AWS KMS / GCP KMS / HashiCorp
//! Vault — anything that holds a master key the application never sees
//! in plaintext. For self-host single-node we ship a `StaticRootKey`
//! that reads a 32-byte secret from disk; rotating it requires a
//! re-encrypt of all segments (offline). The `RootKey` trait lets
//! either back-end plug in.

use thiserror::Error;
use zeroize::{Zeroize, ZeroizeOnDrop};

#[derive(Debug, Error)]
pub enum RootKeyError {
    #[error("invalid root key length: expected 32 bytes")]
    InvalidLength,
    #[error("kms: {0}")]
    Kms(String),
}

/// 32-byte symmetric key. Zeroized on drop to limit residence in memory
/// after the segment is decrypted and the data key is no longer needed.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct DataKey(pub [u8; 32]);

pub trait RootKey: Send + Sync {
    /// Encrypt a fresh data key. Returns (nonce, wrapped_bytes).
    fn wrap(&self, data_key: &DataKey) -> Result<(Vec<u8>, Vec<u8>), RootKeyError>;
    /// Decrypt a wrapped data key.
    fn unwrap(&self, nonce: &[u8], wrapped: &[u8]) -> Result<DataKey, RootKeyError>;
}

/// On-disk static root key. Read from `cfg.crypto.root_key_path` at boot.
/// Suitable for single-node and small clusters; not suitable for
/// regulated workloads where the master key must be HSM-backed.
pub struct StaticRootKey {
    inner: [u8; 32],
}

impl StaticRootKey {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self { inner: bytes }
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self, RootKeyError> {
        if bytes.len() != 32 {
            return Err(RootKeyError::InvalidLength);
        }
        let mut a = [0u8; 32];
        a.copy_from_slice(bytes);
        Ok(Self::new(a))
    }
}

impl RootKey for StaticRootKey {
    fn wrap(&self, data_key: &DataKey) -> Result<(Vec<u8>, Vec<u8>), RootKeyError> {
        use aes_gcm::aead::{Aead, KeyInit, OsRng};
        use aes_gcm::{Aes256Gcm, Nonce};
        let cipher = Aes256Gcm::new_from_slice(&self.inner)
            .map_err(|e| RootKeyError::Kms(format!("aead init: {e}")))?;
        let mut nonce_bytes = [0u8; 12];
        rand_core::RngCore::fill_bytes(&mut OsRng, &mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ct = cipher
            .encrypt(nonce, data_key.0.as_ref())
            .map_err(|e| RootKeyError::Kms(format!("wrap: {e}")))?;
        Ok((nonce_bytes.to_vec(), ct))
    }

    fn unwrap(&self, nonce: &[u8], wrapped: &[u8]) -> Result<DataKey, RootKeyError> {
        use aes_gcm::aead::{Aead, KeyInit};
        use aes_gcm::{Aes256Gcm, Nonce};
        if nonce.len() != 12 {
            return Err(RootKeyError::Kms("invalid nonce length".into()));
        }
        let cipher = Aes256Gcm::new_from_slice(&self.inner)
            .map_err(|e| RootKeyError::Kms(format!("aead init: {e}")))?;
        let pt = cipher
            .decrypt(Nonce::from_slice(nonce), wrapped)
            .map_err(|e| RootKeyError::Kms(format!("unwrap: {e}")))?;
        if pt.len() != 32 {
            return Err(RootKeyError::Kms("wrapped key not 32 bytes".into()));
        }
        let mut a = [0u8; 32];
        a.copy_from_slice(&pt);
        Ok(DataKey(a))
    }
}
