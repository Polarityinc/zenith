//! Per-segment envelope encryption.

use thiserror::Error;

use crate::root::{DataKey, RootKey};

const MAGIC: [u8; 4] = *b"ZENV";
const VERSION: u8 = 1;
const KEY_NONCE_LEN: usize = 12;
const PAYLOAD_NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
/// 32-byte data key + 16-byte AEAD tag.
const WRAPPED_KEY_LEN: usize = 32 + TAG_LEN;
const HEADER_LEN: usize = 4 + 1 + KEY_NONCE_LEN + WRAPPED_KEY_LEN + PAYLOAD_NONCE_LEN;

#[derive(Debug, Error)]
pub enum EnvelopeError {
    #[error("ciphertext too short")]
    TooShort,
    #[error("bad magic")]
    BadMagic,
    #[error("unsupported version: {0}")]
    UnsupportedVersion(u8),
    #[error("aead: {0}")]
    Aead(String),
    #[error("root key: {0}")]
    Root(String),
}

/// Encrypt `plaintext` for at-rest storage. Generates a fresh data key,
/// wraps it with the root key, and writes a self-describing header
/// followed by the AES-256-GCM ciphertext + tag.
pub fn encrypt(
    plaintext: &[u8],
    root: &dyn RootKey,
) -> Result<Vec<u8>, EnvelopeError> {
    use aes_gcm::aead::{Aead, KeyInit, OsRng};
    use aes_gcm::{Aes256Gcm, Nonce};

    // Fresh data key.
    let mut dk_bytes = [0u8; 32];
    rand_core::RngCore::fill_bytes(&mut OsRng, &mut dk_bytes);
    let data_key = DataKey(dk_bytes);

    // Wrap data key.
    let (key_nonce, wrapped) = root
        .wrap(&data_key)
        .map_err(|e| EnvelopeError::Root(format!("{e}")))?;
    if wrapped.len() != WRAPPED_KEY_LEN {
        return Err(EnvelopeError::Root(format!(
            "wrapped key len {} != {WRAPPED_KEY_LEN}",
            wrapped.len()
        )));
    }
    if key_nonce.len() != KEY_NONCE_LEN {
        return Err(EnvelopeError::Root(format!(
            "key nonce len {} != {KEY_NONCE_LEN}",
            key_nonce.len()
        )));
    }

    // Encrypt payload.
    let cipher = Aes256Gcm::new_from_slice(&data_key.0)
        .map_err(|e| EnvelopeError::Aead(format!("init: {e}")))?;
    let mut payload_nonce = [0u8; PAYLOAD_NONCE_LEN];
    rand_core::RngCore::fill_bytes(&mut OsRng, &mut payload_nonce);
    let ct = cipher
        .encrypt(Nonce::from_slice(&payload_nonce), plaintext)
        .map_err(|e| EnvelopeError::Aead(format!("encrypt: {e}")))?;

    // Assemble.
    let mut out = Vec::with_capacity(HEADER_LEN + ct.len());
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    out.extend_from_slice(&key_nonce);
    out.extend_from_slice(&wrapped);
    out.extend_from_slice(&payload_nonce);
    out.extend_from_slice(&ct);
    Ok(out)
}

/// Inverse of [`encrypt`]. Verifies the magic, unwraps the data key, and
/// decrypts the payload.
pub fn decrypt(blob: &[u8], root: &dyn RootKey) -> Result<Vec<u8>, EnvelopeError> {
    use aes_gcm::aead::{Aead, KeyInit};
    use aes_gcm::{Aes256Gcm, Nonce};

    if blob.len() < HEADER_LEN {
        return Err(EnvelopeError::TooShort);
    }
    if blob[..4] != MAGIC {
        return Err(EnvelopeError::BadMagic);
    }
    let ver = blob[4];
    if ver != VERSION {
        return Err(EnvelopeError::UnsupportedVersion(ver));
    }
    let key_nonce = &blob[5..5 + KEY_NONCE_LEN];
    let wrapped = &blob[5 + KEY_NONCE_LEN..5 + KEY_NONCE_LEN + WRAPPED_KEY_LEN];
    let payload_nonce_off = 5 + KEY_NONCE_LEN + WRAPPED_KEY_LEN;
    let payload_nonce = &blob[payload_nonce_off..payload_nonce_off + PAYLOAD_NONCE_LEN];
    let ct = &blob[payload_nonce_off + PAYLOAD_NONCE_LEN..];

    let dk = root
        .unwrap(key_nonce, wrapped)
        .map_err(|e| EnvelopeError::Root(format!("{e}")))?;
    let cipher = Aes256Gcm::new_from_slice(&dk.0)
        .map_err(|e| EnvelopeError::Aead(format!("init: {e}")))?;
    let pt = cipher
        .decrypt(Nonce::from_slice(payload_nonce), ct)
        .map_err(|e| EnvelopeError::Aead(format!("decrypt: {e}")))?;
    Ok(pt)
}

/// True when the bytes start with the envelope magic. Useful for
/// back-compat readers that need to handle both legacy unencrypted
/// segments and new envelope-encrypted ones.
pub fn is_encrypted(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes[..4] == MAGIC
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::root::StaticRootKey;

    fn root() -> StaticRootKey {
        StaticRootKey::new([0x42; 32])
    }

    #[test]
    fn roundtrip_short() {
        let ct = encrypt(b"hello", &root()).unwrap();
        assert!(is_encrypted(&ct));
        let pt = decrypt(&ct, &root()).unwrap();
        assert_eq!(pt, b"hello");
    }

    #[test]
    fn roundtrip_large() {
        let plain = vec![0xab; 2 * 1024 * 1024];
        let ct = encrypt(&plain, &root()).unwrap();
        let pt = decrypt(&ct, &root()).unwrap();
        assert_eq!(pt, plain);
    }

    #[test]
    fn tamper_rejected() {
        let mut ct = encrypt(b"important", &root()).unwrap();
        let last = ct.len() - 1;
        ct[last] ^= 0xff;
        let r = decrypt(&ct, &root());
        assert!(matches!(r, Err(EnvelopeError::Aead(_))));
    }

    #[test]
    fn wrong_root_key_rejected() {
        let ct = encrypt(b"important", &root()).unwrap();
        let other = StaticRootKey::new([0x99; 32]);
        let r = decrypt(&ct, &other);
        assert!(matches!(r, Err(EnvelopeError::Root(_))));
    }

    #[test]
    fn legacy_bytes_not_recognized() {
        assert!(!is_encrypted(b"ZENSEGV1"));
    }
}
