//! WAL on-disk format.
//!
//! ```text
//! [magic 8B "ZENWALV1"] [version u32 le] [commit_id u64 le]
//! [schema_fingerprint u128 le] [arrow_payload_len u32 le]
//! [zstd-compressed Arrow IPC stream]
//! [crc32 u32 le] (over magic..end-of-payload)
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32fast::Hasher;

use zen_common::{CommitId, PartitionId, SchemaFingerprint, TenantId, ZenError, ZenResult};

pub const MAGIC: &[u8; 8] = b"ZENWALV1";
pub const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq)]
pub struct WalHeader {
    pub commit_id: CommitId,
    pub schema_fingerprint: SchemaFingerprint,
}

#[derive(Clone, Debug)]
pub struct WalObjectKey {
    pub tenant_id: TenantId,
    pub partition_id: PartitionId,
    pub commit_id: CommitId,
    pub ulid: String,
}

impl WalObjectKey {
    pub fn new(tenant_id: TenantId, partition_id: PartitionId, commit_id: CommitId) -> Self {
        Self {
            tenant_id,
            partition_id,
            commit_id,
            ulid: ulid::Ulid::new().to_string(),
        }
    }
}

impl std::fmt::Display for WalObjectKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "wal/{}/{}/{:020}-{}.wal",
            self.tenant_id, self.partition_id, self.commit_id.0, self.ulid
        )
    }
}

pub fn build_wal_object(header: WalHeader, arrow_payload_zstd: Bytes) -> ZenResult<Bytes> {
    let mut out = BytesMut::with_capacity(40 + arrow_payload_zstd.len());
    out.put_slice(MAGIC);
    out.put_u32_le(FORMAT_VERSION);
    out.put_u64_le(header.commit_id.0);
    out.put_u128_le(header.schema_fingerprint.0);
    out.put_u32_le(arrow_payload_zstd.len() as u32);
    out.put_slice(&arrow_payload_zstd);

    let mut h = Hasher::new();
    h.update(&out);
    let crc = h.finalize();
    out.put_u32_le(crc);
    Ok(out.freeze())
}

pub fn parse_wal_object(bytes: &[u8]) -> ZenResult<(WalHeader, Bytes)> {
    if bytes.len() < 8 + 4 + 8 + 16 + 4 + 4 {
        return Err(ZenError::format("wal object too small"));
    }
    if &bytes[..8] != MAGIC {
        return Err(ZenError::format("bad WAL magic"));
    }
    let mut p = &bytes[8..];
    let version = p.get_u32_le();
    if version != FORMAT_VERSION {
        return Err(ZenError::format(format!(
            "WAL version {version} unsupported"
        )));
    }
    let commit_id = CommitId(p.get_u64_le());
    let fp = SchemaFingerprint(p.get_u128_le());
    let payload_len = p.get_u32_le() as usize;
    let need = payload_len
        .checked_add(4)
        .ok_or_else(|| ZenError::format("WAL payload_len overflow"))?;
    if p.len() < need {
        return Err(ZenError::format("WAL payload truncated"));
    }
    let payload = p[..payload_len].to_vec();
    if bytes.len() < 4 {
        return Err(ZenError::format("WAL CRC field truncated"));
    }
    let crc_pos = bytes.len() - 4;
    let stored_crc = u32::from_le_bytes(
        bytes[crc_pos..crc_pos + 4]
            .try_into()
            .map_err(|_| ZenError::format("WAL CRC slice"))?,
    );
    let mut h = Hasher::new();
    h.update(&bytes[..crc_pos]);
    let actual = h.finalize();
    if actual != stored_crc {
        return Err(ZenError::format(format!(
            "WAL CRC32 mismatch: stored={stored_crc:08x} actual={actual:08x}"
        )));
    }
    Ok((
        WalHeader {
            commit_id,
            schema_fingerprint: fp,
        },
        Bytes::from(payload),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::any;

    #[test]
    fn key_format_stable() {
        let k = WalObjectKey {
            tenant_id: TenantId(1),
            partition_id: PartitionId(2),
            commit_id: CommitId(42),
            ulid: "01H".to_string(),
        };
        let s = k.to_string();
        assert!(s.starts_with("wal/t0000000000000001/p00000002/00000000000000000042-01H.wal"));
    }

    #[test]
    fn build_and_parse_roundtrip() {
        let h = WalHeader {
            commit_id: CommitId(7),
            schema_fingerprint: SchemaFingerprint(0xdead),
        };
        let payload = Bytes::from_static(b"some compressed arrow data here");
        let bytes = build_wal_object(h.clone(), payload.clone()).unwrap();
        let (h2, p2) = parse_wal_object(&bytes).unwrap();
        assert_eq!(h, h2);
        assert_eq!(payload, p2);
    }

    proptest::proptest! {
        /// Round-trip every header + payload shape through build → parse,
        /// asserting equality. Catches off-by-one in framing, length
        /// fields, magic. Random byte content rules out anything that
        /// happened to work on a single hand-written test.
        #[test]
        fn roundtrip_arbitrary_header_and_payload(
            commit in 0u64..u64::MAX,
            fp in 0u128..u128::MAX,
            payload in proptest::collection::vec(any::<u8>(), 0..2048),
        ) {
            let h = WalHeader {
                commit_id: CommitId(commit),
                schema_fingerprint: SchemaFingerprint(fp),
            };
            let bytes = build_wal_object(h.clone(), Bytes::from(payload.clone())).unwrap();
            let (h2, p2) = parse_wal_object(&bytes).unwrap();
            proptest::prop_assert_eq!(h, h2);
            proptest::prop_assert_eq!(p2.as_ref(), payload.as_slice());
        }

        /// Random byte mutation in the payload region must be caught by
        /// the CRC32. Header + magic positions are protected against
        /// (we never blindly mutate them since those have stricter
        /// validation paths).
        #[test]
        fn random_payload_corruption_caught(
            payload in proptest::collection::vec(any::<u8>(), 8..1024),
            mut_off in 40usize..1024,
            mut_xor in 1u8..=u8::MAX,
        ) {
            let h = WalHeader {
                commit_id: CommitId(0xc0ffee),
                schema_fingerprint: SchemaFingerprint(0xdead),
            };
            let bytes = build_wal_object(h, Bytes::from(payload)).unwrap();
            if mut_off >= bytes.len() - 4 {
                return Ok(());
            }
            let mut bad = bytes.to_vec();
            bad[mut_off] ^= mut_xor;
            proptest::prop_assert!(parse_wal_object(&bad).is_err());
        }
    }

    #[test]
    fn detects_corruption() {
        let h = WalHeader {
            commit_id: CommitId(7),
            schema_fingerprint: SchemaFingerprint(0xdead),
        };
        let payload = Bytes::from_static(b"some compressed arrow data here");
        let bytes = build_wal_object(h, payload).unwrap();
        let mut bad = bytes.to_vec();
        // Corrupt a byte in the payload region.
        bad[40] ^= 0xFF;
        assert!(parse_wal_object(&bad).is_err());
    }
}
