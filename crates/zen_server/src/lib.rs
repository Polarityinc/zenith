//! ZenithDB HTTP + gRPC server.

pub mod state;
pub mod http;
pub mod ingest;
pub mod internal_query;
pub mod metrics;
pub mod middleware;
pub mod openapi;
pub mod query_handler;
pub mod admin;
pub mod otlp;
pub mod grpc;

pub use state::ServerState;
