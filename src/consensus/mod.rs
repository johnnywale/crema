//! Raft consensus layer.

pub mod flow_control;
pub mod health;
pub mod node;
pub mod state_machine;
pub mod storage;
pub mod transport;

#[cfg(feature = "rocksdb-storage")]
pub mod rocksdb_storage;

#[cfg(test)]
mod transport_tests;

pub use flow_control::{FlowControl, PressureLevel, RateLimiter};
pub use health::{HealthCheckConfig, HealthChecker, HealthReport, HealthStatus};
pub use node::RaftNode;
pub use state_machine::CacheStateMachine;
pub use storage::{MemStorage, RaftStorage};
pub use transport::{BackpressureCallback, BackpressureEvent, RaftTransport};

#[cfg(feature = "rocksdb-storage")]
pub use rocksdb_storage::{RocksDbStorage, RocksDbStorageConfig};
