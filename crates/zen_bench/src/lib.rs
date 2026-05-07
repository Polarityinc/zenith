//! Benchmark suite: synthetic workload generator + B1-B20 runner + leaderboard.

pub mod workload;
pub mod load;
pub mod run;
pub mod leaderboard;

pub use workload::{generate_workload, WorkloadConfig};
pub use load::load_to_server;
pub use run::{run_suite, BenchSuite};
pub use leaderboard::Leaderboard;
