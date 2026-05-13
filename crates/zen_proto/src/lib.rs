//! Protobuf definitions for the Zenith API. The build script compiles
//! `proto/zen/*.proto` via `prost-build`. Re-export the generated types here.

pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/zen.v1.rs"));
}

pub use v1::{IngestRequest, IngestResponse, QueryRequest, QueryResponse, SpanIngestRequest};
