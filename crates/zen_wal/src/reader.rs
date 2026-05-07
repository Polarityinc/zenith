//! WAL reader: parse object → Arrow `RecordBatch`.

use std::io::Cursor;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use bytes::Bytes;

use zen_common::{ZenError, ZenResult};

use crate::format::{parse_wal_object, WalHeader};

pub struct WalReader;

impl WalReader {
    /// Parse a WAL object into its header + a vector of record batches.
    /// Most flushes produce exactly one batch, but Arrow IPC streams may have many.
    pub fn parse(bytes: &[u8]) -> ZenResult<(WalHeader, Vec<RecordBatch>)> {
        let (header, payload_zstd) = parse_wal_object(bytes)?;
        let arrow_bytes = zen_compress::zstd_decompress(&payload_zstd)?;
        Self::read_arrow_batches(&arrow_bytes).map(|b| (header, b))
    }

    fn read_arrow_batches(bytes: &[u8]) -> ZenResult<Vec<RecordBatch>> {
        let r = StreamReader::try_new(Cursor::new(bytes), None)
            .map_err(|e| ZenError::format(format!("arrow stream reader: {e}")))?;
        let mut out = Vec::new();
        for b in r {
            out.push(b.map_err(|e| ZenError::format(format!("arrow batch: {e}")))?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    use zen_common::{CommitId, PartitionId, SchemaFingerprint, TenantId};
    use zen_storage::{local_fs::InMemoryStore, BlobStore};

    use crate::writer::WalWriter;

    #[tokio::test]
    async fn writer_then_reader_roundtrip() {
        let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
        let w = WalWriter::new(store.clone());

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let id = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
        let name = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")]));
        let batch = RecordBatch::try_new(schema.clone(), vec![id, name]).unwrap();

        let key = w
            .flush(
                TenantId(7),
                PartitionId(3),
                CommitId(11),
                SchemaFingerprint(0x1234),
                &batch,
            )
            .await
            .unwrap();
        let bytes = store.get(&key.to_string()).await.unwrap();
        let (header, batches) = WalReader::parse(&bytes).unwrap();
        assert_eq!(header.commit_id, CommitId(11));
        assert_eq!(header.schema_fingerprint, SchemaFingerprint(0x1234));
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn concurrent_writers_do_not_overwrite() {
        let store: Arc<dyn BlobStore> = Arc::new(InMemoryStore::default());
        let schema = Arc::new(ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch_a = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )
        .unwrap();
        let batch_b = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![2i64]))],
        )
        .unwrap();

        // Two writers with the same (tenant, partition, commit_id) but different ulids.
        // They should not collide because ulid is regenerated.
        let w1 = WalWriter::new(store.clone());
        let w2 = WalWriter::new(store.clone());
        let k1 = w1
            .flush(
                TenantId(1),
                PartitionId(0),
                CommitId(5),
                SchemaFingerprint(0),
                &batch_a,
            )
            .await
            .unwrap();
        let k2 = w2
            .flush(
                TenantId(1),
                PartitionId(0),
                CommitId(5),
                SchemaFingerprint(0),
                &batch_b,
            )
            .await
            .unwrap();
        assert_ne!(k1.to_string(), k2.to_string());
        assert!(store.get(&k1.to_string()).await.is_ok());
        assert!(store.get(&k2.to_string()).await.is_ok());
    }
}
