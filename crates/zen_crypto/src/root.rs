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
///
/// `Zeroize` + `ZeroizeOnDrop` ensure the master key bytes are wiped
/// from RAM when the value is dropped — defends core dumps and
/// memory-snapshot tools from leaking the key after server shutdown.
#[derive(Zeroize, ZeroizeOnDrop)]
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a root key from OS randomness. We avoid hard-coded key bytes so
    /// the tests don't trigger CodeQL's "hard-coded cryptographic value" rule
    /// and so each test gets independent isolation.
    fn random_root_key() -> StaticRootKey {
        use aes_gcm::aead::OsRng;
        let mut bytes = [0u8; 32];
        rand_core::RngCore::fill_bytes(&mut OsRng, &mut bytes);
        StaticRootKey::new(bytes)
    }

    fn random_data_key() -> DataKey {
        use aes_gcm::aead::OsRng;
        let mut bytes = [0u8; 32];
        rand_core::RngCore::fill_bytes(&mut OsRng, &mut bytes);
        DataKey(bytes)
    }

    /// Compile-time check: `DataKey` and `StaticRootKey` must be `Zeroize` +
    /// `ZeroizeOnDrop`. If this stops compiling, the wipe-on-drop guarantee
    /// has been silently dropped from the type.
    #[allow(dead_code)]
    fn assert_zeroize_traits_present() {
        fn assert_zeroize<T: Zeroize + ZeroizeOnDrop>() {}
        assert_zeroize::<DataKey>();
        assert_zeroize::<StaticRootKey>();
    }

    #[test]
    fn wrap_unwrap_roundtrip() {
        let root = random_root_key();
        let dk = random_data_key();
        let original_bytes = dk.0;

        let (nonce, wrapped) = root.wrap(&dk).expect("wrap");
        assert_eq!(nonce.len(), 12);
        // AES-GCM wrap of 32 bytes plaintext: 32 ciphertext + 16 tag = 48.
        assert_eq!(wrapped.len(), 48);

        let recovered = root.unwrap(&nonce, &wrapped).expect("unwrap");
        assert_eq!(recovered.0, original_bytes);
    }

    #[test]
    fn from_slice_round_trips_for_32_bytes() {
        let mut bytes = [0u8; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(7).wrapping_add(13);
        }
        let key = StaticRootKey::from_slice(&bytes).expect("from_slice");
        // Wrap/unwrap a fresh data key as a smoke test.
        let dk = random_data_key();
        let (nonce, wrapped) = key.wrap(&dk).expect("wrap");
        let recovered = key.unwrap(&nonce, &wrapped).expect("unwrap");
        assert_eq!(recovered.0, dk.0);

        // Reject lengths != 32.
        let too_short = StaticRootKey::from_slice(&bytes[..16]);
        assert!(matches!(too_short, Err(RootKeyError::InvalidLength)));
        let too_long = StaticRootKey::from_slice(&[0u8; 64]);
        assert!(matches!(too_long, Err(RootKeyError::InvalidLength)));
    }

    #[test]
    fn wrong_root_key_fails_to_unwrap_with_kms_error() {
        let real = random_root_key();
        let imposter = random_root_key();
        let dk = random_data_key();

        let (nonce, wrapped) = real.wrap(&dk).expect("wrap");
        let r = imposter.unwrap(&nonce, &wrapped);
        assert!(
            matches!(r, Err(RootKeyError::Kms(_))),
            "expected RootKeyError::Kms, got {:?}",
            r.as_ref().err()
        );
    }

    #[test]
    fn wrap_nonces_are_non_deterministic() {
        let root = random_root_key();
        let dk = random_data_key();

        // Wrap the same data key 16 times. Every nonce should be distinct,
        // and every ciphertext should also be distinct (AES-GCM is
        // deterministic given fixed key+nonce, so a different nonce implies
        // a different ciphertext).
        let mut nonces = std::collections::HashSet::new();
        let mut wraps = std::collections::HashSet::new();
        for _ in 0..16 {
            let (nonce, wrapped) = root.wrap(&dk).expect("wrap");
            assert!(
                nonces.insert(nonce.clone()),
                "duplicate wrap nonce — RNG broken"
            );
            assert!(
                wraps.insert(wrapped),
                "duplicate ciphertext — wrap is deterministic across calls"
            );
        }
        assert_eq!(nonces.len(), 16);
    }

    #[test]
    fn tampered_ciphertext_fails_to_unwrap() {
        let root = random_root_key();
        let dk = random_data_key();
        let (nonce, mut wrapped) = root.wrap(&dk).expect("wrap");

        // Flip the last byte of the AEAD tag — must not decrypt to a valid
        // data key.
        let last = wrapped.len() - 1;
        wrapped[last] = wrapped[last].wrapping_add(7) | 1;
        let r = root.unwrap(&nonce, &wrapped);
        assert!(
            matches!(r, Err(RootKeyError::Kms(_))),
            "tampered ciphertext should be rejected, got {:?}",
            r.as_ref().err()
        );

        // And tampering somewhere in the middle (the body) must also fail.
        let (nonce2, mut wrapped2) = root.wrap(&dk).expect("wrap");
        wrapped2[5] = wrapped2[5].wrapping_add(3) | 1;
        let r2 = root.unwrap(&nonce2, &wrapped2);
        assert!(matches!(r2, Err(RootKeyError::Kms(_))));
    }

    #[test]
    fn wrong_nonce_length_fails_with_kms_error() {
        let root = random_root_key();
        let dk = random_data_key();
        let (_nonce, wrapped) = root.wrap(&dk).expect("wrap");

        // 11 bytes != 12: must be rejected up front.
        let bad_nonce = vec![0u8; 11];
        let r = root.unwrap(&bad_nonce, &wrapped);
        assert!(matches!(r, Err(RootKeyError::Kms(_))));
    }

    #[test]
    fn data_key_clone_is_independent_and_drops_safely() {
        // DataKey: Clone — verify cloning gives independent storage and
        // dropping each clone runs the zeroize cleanup without panicking.
        let dk = random_data_key();
        let original = dk.0;
        {
            let cloned = dk.clone();
            assert_eq!(cloned.0, original, "clone must preserve bytes");
            // `cloned` drops here — Zeroize trait wipes its inner array.
        }
        // Original still readable after clone drops.
        assert_eq!(dk.0, original);
    }
}
