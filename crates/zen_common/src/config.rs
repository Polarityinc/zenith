//! Top-level server configuration.
//!
//! Loads from a TOML file, with environment-variable overrides for hot-reloadable
//! settings. Storage and catalog backends are not hot-reloadable — server requires a
//! restart to switch them.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::errors::{ZenError, ZenResult};

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub catalog: CatalogConfig,
    #[serde(default)]
    pub ingest: IngestConfig,
    #[serde(default)]
    pub compact: CompactConfig,
    #[serde(default)]
    pub query: QueryConfig,
    #[serde(default)]
    pub fts: FtsConfig,
    #[serde(default)]
    pub bitmap_index: BitmapIndexConfig,
    #[serde(default)]
    pub jsonpath_index: JsonPathIndexConfig,
    #[serde(default)]
    pub vector: VectorConfig,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub crypto: CryptoConfig,
}

/// Encryption-at-rest settings. Default is **off** — segments and WALs
/// are stored unencrypted (back-compat with existing deployments). Turn
/// it on by setting `enabled=true` and pointing `root_key_path` at a
/// 32-byte file (or 64 hex chars) holding the symmetric root key.
///
/// In production, the root key should come from a KMS (AWS KMS, GCP
/// KMS, HashiCorp Vault). Today we ship a `StaticRootKey` from disk;
/// the `RootKey` trait in `zen_crypto` lets a follow-up plug a KMS
/// adapter in without touching the wire format.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CryptoConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Path to a file holding the 32-byte root key. Either raw bytes
    /// (file is exactly 32 bytes long) or 64 hex characters.
    #[serde(default)]
    pub root_key_path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    pub listen: String,
    pub http_listen: String,
    #[serde(default)]
    pub tls: TlsConfig,
}
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:50051".into(),
            http_listen: "0.0.0.0:8080".into(),
            tls: TlsConfig::default(),
        }
    }
}

/// TLS settings for the HTTP / gRPC listeners. Empty `cert_path` means
/// "serve plain HTTP" — sane for in-VPC deployments behind a TLS-
/// terminating load balancer (Kong / Envoy / ALB) and required for the
/// in-process integration tests. Set `cert_path` + `key_path` to enable
/// rustls termination directly inside the zenithdb process.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TlsConfig {
    #[serde(default)]
    pub cert_path: String,
    #[serde(default)]
    pub key_path: String,
    /// Optional path to a CA bundle used to verify client certificates
    /// when mTLS is required. Empty means client certs aren't required.
    #[serde(default)]
    pub client_ca_path: String,
}

/// Authentication settings. When all fields are empty, auth is **off** —
/// useful for the dev path and the integration tests but unsafe for any
/// production deployment. The boot-time validation logs a loud warning
/// when this happens.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AuthConfig {
    /// HTTPS URL of the issuing IdP's JWKS document. When set, public
    /// HTTP endpoints require a `Bearer` JWT signed by one of the keys
    /// in the document. When empty, JWT verification is **disabled**.
    #[serde(default)]
    pub jwks_url: String,
    /// Shared secret used to authenticate `/v1/internal/*` requests
    /// between cluster nodes. When empty, internal endpoints are open —
    /// only safe for single-node deployments. Prefer setting this for
    /// any cluster, even on a private network.
    #[serde(default)]
    pub internal_secret: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageConfig {
    pub backend: String, // "fs" | "s3" | "gcs" | "azure" | "memory"
    pub fs_root: String,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub nvme_cache_dir: String,
    pub nvme_cache_bytes: u64,
}
impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: "fs".into(),
            fs_root: "./data/blobs".into(),
            bucket: None,
            region: None,
            endpoint: None,
            access_key: None,
            secret_key: None,
            nvme_cache_dir: "./data/cache".into(),
            nvme_cache_bytes: 4 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatalogConfig {
    pub backend: String, // "sqlite" | "postgres"
    pub sqlite_path: String,
    pub postgres_url: Option<String>,
}
impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            backend: "sqlite".into(),
            sqlite_path: "./data/zenith.db".into(),
            postgres_url: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IngestConfig {
    pub flush_interval_ms: u64,
    pub flush_max_bytes: u64,
    pub writer_threads: u32,
}
impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            flush_interval_ms: 100,
            flush_max_bytes: 64 * 1024 * 1024,
            writer_threads: 4,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactConfig {
    pub trigger_wal_count: u32,
    pub trigger_wal_bytes: u64,
    pub trigger_age_seconds: u64,
    pub target_segment_bytes: u64,
    pub worker_threads: u32,
}
impl Default for CompactConfig {
    fn default() -> Self {
        Self {
            trigger_wal_count: 32,
            trigger_wal_bytes: 256 * 1024 * 1024,
            trigger_age_seconds: 300,
            target_segment_bytes: 512 * 1024 * 1024,
            worker_threads: 2,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryConfig {
    pub max_concurrent_queries: u32,
    pub result_cache_max_bytes: u64,
    /// Per-tenant request budget in QPS. 0 disables rate limiting.
    #[serde(default = "default_tenant_qps")]
    pub tenant_qps_limit: u32,
    /// Burst capacity per tenant — number of requests allowed back-to-back
    /// before the bucket starts throttling.
    #[serde(default = "default_tenant_burst")]
    pub tenant_burst: u32,
}
fn default_tenant_qps() -> u32 {
    100
}
fn default_tenant_burst() -> u32 {
    1000
}
impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_concurrent_queries: 256,
            result_cache_max_bytes: 1024 * 1024 * 1024,
            tenant_qps_limit: default_tenant_qps(),
            tenant_burst: default_tenant_burst(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FtsConfig {
    pub indexed_fields: Vec<String>,
}
impl Default for FtsConfig {
    fn default() -> Self {
        Self {
            indexed_fields: vec!["prompt".into(), "completion".into(), "tool_io_text".into()],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BitmapIndexConfig {
    pub indexed_columns: Vec<String>,
}
impl Default for BitmapIndexConfig {
    fn default() -> Self {
        Self {
            indexed_columns: vec![
                "model".into(),
                "tool_name".into(),
                "status".into(),
                "span_type".into(),
                "provider".into(),
            ],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonPathIndexConfig {
    pub sample_size: u32,
    pub min_presence_pct: f64,
    pub max_paths_per_segment: u32,
}
impl Default for JsonPathIndexConfig {
    fn default() -> Self {
        Self {
            sample_size: 10_000,
            min_presence_pct: 1.0,
            max_paths_per_segment: 256,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorConfig {
    pub enabled: bool,
    pub dimensions: u32,
    pub hnsw_m: u32,
    pub hnsw_ef_construction: u32,
    pub hnsw_ef_search: u32,
    pub quantize: bool,
}
impl Default for VectorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dimensions: 1536,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_search: 50,
            quantize: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TelemetryConfig {
    pub log_level: String,
    pub otlp_endpoint: String,
}
impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            log_level: "info".into(),
            otlp_endpoint: String::new(),
        }
    }
}

impl Config {
    /// Load from TOML on disk, applying env overrides on top.
    pub fn load_from_path<P: AsRef<Path>>(path: P) -> ZenResult<Self> {
        let s = std::fs::read_to_string(&path)?;
        let mut cfg: Config = toml::from_str(&s)?;
        cfg.apply_env_overrides();
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn from_toml_str(s: &str) -> ZenResult<Self> {
        let mut cfg: Config = toml::from_str(s)?;
        cfg.apply_env_overrides();
        cfg.validate()?;
        Ok(cfg)
    }

    /// Apply `ZEN_*` environment variables for the most operationally-important fields.
    pub fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("ZEN_LISTEN") {
            self.server.listen = v;
        }
        if let Ok(v) = std::env::var("ZEN_HTTP_LISTEN") {
            self.server.http_listen = v;
        }
        if let Ok(v) = std::env::var("ZEN_STORAGE_BACKEND") {
            self.storage.backend = v;
        }
        if let Ok(v) = std::env::var("ZEN_FS_ROOT") {
            self.storage.fs_root = v;
        }
        if let Ok(v) = std::env::var("ZEN_CATALOG_BACKEND") {
            self.catalog.backend = v;
        }
        if let Ok(v) = std::env::var("ZEN_SQLITE_PATH") {
            self.catalog.sqlite_path = v;
        }
        if let Ok(v) = std::env::var("ZEN_POSTGRES_URL") {
            self.catalog.postgres_url = Some(v);
        }
        if let Ok(v) = std::env::var("ZEN_LOG") {
            self.telemetry.log_level = v;
        }
    }

    pub fn validate(&self) -> ZenResult<()> {
        match self.storage.backend.as_str() {
            "fs" | "s3" | "gcs" | "azure" | "memory" => {}
            other => {
                return Err(ZenError::invalid(format!(
                    "unknown storage backend: {other}"
                )))
            }
        }
        match self.catalog.backend.as_str() {
            "sqlite" | "postgres" => {}
            other => {
                return Err(ZenError::invalid(format!(
                    "unknown catalog backend: {other}"
                )))
            }
        }
        if self.catalog.backend == "postgres" && self.catalog.postgres_url.is_none() {
            return Err(ZenError::invalid(
                "catalog.backend=postgres requires catalog.postgres_url",
            ));
        }
        if self.ingest.flush_max_bytes == 0 {
            return Err(ZenError::invalid("ingest.flush_max_bytes must be > 0"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        Config::default().validate().unwrap();
    }

    #[test]
    fn parses_dev_toml_minimal() {
        let s = r#"
            [server]
            listen = "127.0.0.1:50000"
            http_listen = "127.0.0.1:8000"

            [storage]
            backend = "fs"
            fs_root = "./tmp"
            nvme_cache_dir = "./tmp/cache"
            nvme_cache_bytes = 1024

            [catalog]
            backend = "sqlite"
            sqlite_path = "./tmp/zenith.db"
        "#;
        let cfg = Config::from_toml_str(s).unwrap();
        assert_eq!(cfg.server.listen, "127.0.0.1:50000");
        assert_eq!(cfg.storage.backend, "fs");
    }

    #[test]
    fn rejects_unknown_storage_backend() {
        let s = r#"
            [storage]
            backend = "wat"
            fs_root = "x"
            nvme_cache_dir = "x"
            nvme_cache_bytes = 1
            [catalog]
            backend = "sqlite"
            sqlite_path = "x"
        "#;
        assert!(Config::from_toml_str(s).is_err());
    }

    #[test]
    fn postgres_requires_url() {
        let s = r#"
            [catalog]
            backend = "postgres"
            sqlite_path = "x"
        "#;
        assert!(Config::from_toml_str(s).is_err());
    }
}
