//! Crate-wide error type. Every other crate should re-export `ZenResult` and use this
//! enum for fallible operations; keeping it centralized lets us match on a single error
//! type in higher layers (the server, the executor) without `Box<dyn Error>` everywhere.

use thiserror::Error;

pub type ZenResult<T> = Result<T, ZenError>;

#[derive(Debug, Error)]
pub enum ZenError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("toml decode: {0}")]
    TomlDecode(#[from] toml::de::Error),

    #[error("toml encode: {0}")]
    TomlEncode(#[from] toml::ser::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("ulid: {0}")]
    Ulid(#[from] ulid::DecodeError),

    #[error("invalid argument: {0}")]
    Invalid(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("format: {0}")]
    Format(String),

    #[error("compress: {0}")]
    Compress(String),

    #[error("storage: {0}")]
    Storage(String),

    #[error("catalog: {0}")]
    Catalog(String),

    #[error("query: {0}")]
    Query(String),

    #[error("compactor: {0}")]
    Compactor(String),

    #[error("internal: {0}")]
    Internal(String),
}

impl ZenError {
    pub fn invalid<S: Into<String>>(s: S) -> Self {
        ZenError::Invalid(s.into())
    }
    pub fn not_found<S: Into<String>>(s: S) -> Self {
        ZenError::NotFound(s.into())
    }
    pub fn format<S: Into<String>>(s: S) -> Self {
        ZenError::Format(s.into())
    }
    pub fn compress<S: Into<String>>(s: S) -> Self {
        ZenError::Compress(s.into())
    }
    pub fn storage<S: Into<String>>(s: S) -> Self {
        ZenError::Storage(s.into())
    }
    pub fn catalog<S: Into<String>>(s: S) -> Self {
        ZenError::Catalog(s.into())
    }
    pub fn query<S: Into<String>>(s: S) -> Self {
        ZenError::Query(s.into())
    }
    pub fn compactor<S: Into<String>>(s: S) -> Self {
        ZenError::Compactor(s.into())
    }
    pub fn internal<S: Into<String>>(s: S) -> Self {
        ZenError::Internal(s.into())
    }
    pub fn conflict<S: Into<String>>(s: S) -> Self {
        ZenError::Conflict(s.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helpers_construct() {
        let e = ZenError::format("bad magic");
        assert!(matches!(e, ZenError::Format(_)));
        assert_eq!(format!("{e}"), "format: bad magic");
    }

    #[test]
    fn io_error_converts() {
        let io: std::io::Error = std::io::ErrorKind::NotFound.into();
        let e: ZenError = io.into();
        assert!(matches!(e, ZenError::Io(_)));
    }
}
