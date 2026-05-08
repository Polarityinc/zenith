//! ZenithDB HTTP + gRPC server.

pub mod admin;
pub mod grpc;
pub mod http;
pub mod ingest;
pub mod internal_query;
pub mod metrics;
pub mod middleware;
pub mod openapi;
pub mod otlp;
pub mod query_handler;
pub mod state;

pub use state::ServerState;
