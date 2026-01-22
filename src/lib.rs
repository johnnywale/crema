//! Strongly consistent distributed cache with Raft consensus.
//!
//! This crate provides an embedded distributed cache that uses:
//! - **Moka** for high-performance local caching with automatic TTL/eviction
//! - **raft-rs** for strong consistency via Raft consensus
//! - **Two-tier membership** for safe cluster management
//!
//! # Features
//!
//! - Strong consistency for writes via Raft consensus
//! - Fast local reads from Moka cache
//! - Automatic TTL and TinyLFU eviction
//! - Two-tier cluster membership (discovery + manual Raft control)
//! - Pre-validation to ensure state machine apply never fails
//!
//! # Example
//!
//! ```rust,no_run
//! use crema::{DistributedCache, CacheConfig};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create configuration
//!     let config = CacheConfig::new(1, "127.0.0.1:9000".parse()?)
//!         .with_max_capacity(100_000)
//!         .with_default_ttl(Duration::from_secs(3600));
//!
//!     // Create the distributed cache
//!     let cache = DistributedCache::new(config).await?;
//!
//!     // Write operations go through Raft consensus
//!     cache.put("user:123", "Alice").await?;
//!
//!     // Read operations are local (fast, but may be stale on followers)
//!     if let Some(value) = cache.get(b"user:123").await {
//!         println!("Found: {:?}", value);
//!     }
//!
//!     // Delete
//!     cache.delete("user:123").await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │            Application Layer                 │
//! └─────────────────────────────────────────────┘
//!                     │
//!                     ▼
//! ┌─────────────────────────────────────────────┐
//! │          DistributedCache API               │
//! │  • get(key) -> Option<Value>                │
//! │  • put(key, value) -> Result<()>            │
//! │  • delete(key) -> Result<()>                │
//! └─────────────────────────────────────────────┘
//!                     │
//!     ┌───────────────┼───────────────┐
//!     ▼               ▼               ▼
//! ┌─────────┐   ┌──────────┐   ┌─────────┐
//! │ Cluster │   │  Raft    │   │  Moka   │
//! │Membership│  │Consensus │   │ Cache   │
//! └─────────┘   └──────────┘   └─────────┘
//! ```
//!
//! # Consistency Model
//!
//! - **Writes**: Strongly consistent via Raft (linearizable)
//! - **Reads**: Locally consistent (may be stale on followers)
//! - **Leader reads**: Strongly consistent if reading from leader
//!
//! # Checkpointing
//!
//! The cache supports periodic checkpointing for fast recovery:
//!
//! ```rust,ignore
//! use distributed_cache::checkpoint::{CheckpointConfig, CheckpointManager};
//!
//! let config = CheckpointConfig::new("./checkpoints")
//!     .with_log_threshold(10_000)
//!     .with_compression(true);
//!
//! // Snapshots are automatically created based on configured triggers
//! ```

pub mod cache;
pub mod checkpoint;
pub mod cluster;
pub mod config;
pub mod consensus;
pub mod error;
pub mod metrics;
pub mod multiraft;
pub mod network;
pub mod partitioning;
// pub mod rebalancing;
pub mod testing;
pub mod types;

// Re-export main types for convenience
pub use cache::DistributedCache;
pub use config::{
    CacheConfig, MemberlistConfig, MembershipConfig, MembershipMode, MultiRaftCacheConfig,
    RaftConfig,
};
pub use error::{Error, Result};
pub use types::{CacheCommand, CacheStats, ClusterStatus, NodeId, PeerInfo};

// Re-export cluster types
pub use cluster::{ClusterMembership, MemberEvent};

// Re-export checkpoint types
pub use checkpoint::{
    CheckpointConfig, CheckpointManager, FormatError, RaftStateProvider, SnapshotEntry,
    SnapshotInfo, SnapshotMetadata, SnapshotReader, SnapshotWriter,
};

// Re-export partitioning types
pub use partitioning::{HashRing, KeyOwnership, OwnershipRole, OwnershipTracker, PendingTransfers};

// // Re-export rebalancing types
// pub use rebalancing::{
//     RebalanceConfig, RebalanceCoordinator, RebalanceError, RebalanceState, RebalanceType,
//     TransferBatch, TransferEntry, TransferProgress,
// };

// Re-export metrics types
pub use metrics::{
    CacheMetrics, Counter, FloatGauge, Gauge, Histogram, HistogramSnapshot, HistogramTimer,
    LabeledCounter, LabeledGauge, LabeledHistogram, MetricsSnapshot, CACHE_LATENCY_BUCKETS,
    RAFT_LATENCY_BUCKETS,
};

// Re-export testing types
pub use testing::{
    ChaosAction, ChaosConfig, ChaosController, ChaosRunner, ChaosScenario, ChaosStats,
    NetworkPartition, NodeCrash, TestAssertions, TestCluster, TestMetrics,
};

// Re-export Multi-Raft types
pub use multiraft::{
    BatchRouter, CoordinatorState, MultiRaftBuilder, MultiRaftConfig, MultiRaftCoordinator,
    MultiRaftStats, RouterConfig, RoutingDecision, Shard, ShardAssignment, ShardConfig, ShardId,
    ShardInfo, ShardLeaderBroadcaster, ShardLeaderTracker, ShardRange, ShardRouter, ShardState,
    // Shard placement and registry
    MovementType, PlacementConfig, ShardMovement, ShardPlacement,
    ShardLifecycleState, ShardMetadata, ShardRegistry,
    // Migration (use multiraft:: prefix to avoid conflicts with rebalancing)
    InMemoryMigrationStore, MigrationCheckpoint, MigrationConfig, MigrationPhase,
    MigrationProgress, MigrationStateStore, MigrationStats, ShardMigration,
    ShardMigrationCoordinator, MigrationMetrics, MigrationMetricsSnapshot, MigrationTimer,
};

// Re-export migration transfer types (from multiraft, not rebalancing)
pub use multiraft::TransferBatch as MigrationTransferBatch;
pub use multiraft::TransferEntry as MigrationTransferEntry;

// Re-export memberlist types for Multi-Raft integration
pub use cluster::memberlist_cluster::ShardLeaderInfo;
