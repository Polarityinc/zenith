//! Multi-node coordination for ZenithDB.
//!
//! Architecture (ClickHouse-inspired, but storage-disaggregated like
//! Snowflake): every node can serve every read because object storage is
//! the shared data plane. Sharding decides *which* node owns the *write
//! routing and query coordination* for a given (tenant, partition), not
//! who can physically read the data. This is what lets us scale
//! horizontally to PB-class corpora without inter-node data shuffling.
//!
//! - `node`        : node identity + role + heartbeat status
//! - `shard`       : rendezvous-hash shard map → ranked replica list
//! - `registry`    : heartbeat loop against the catalog
//! - `router`      : "execute local or fan-out remote?" decision
//! - `remote`      : HTTP client for inter-node /v1/internal/query
//! - `merge`       : combine partial ResultSets from remote workers

pub mod merge;
pub mod node;
pub mod registry;
pub mod remote;
pub mod router;
pub mod shard;

pub use merge::merge_result_sets;
pub use node::{NodeId, NodeInfo, NodeRole, NodeStatus};
pub use registry::NodeRegistry;
pub use remote::RemoteClient;
pub use router::{QueryRouter, RouteDecision};
pub use shard::{ShardKey, ShardMap};
