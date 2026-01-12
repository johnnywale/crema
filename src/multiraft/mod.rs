//! Multi-Raft module for horizontal scaling.
//!
//! This module implements Multi-Raft, allowing the cache to scale writes
//! by partitioning the keyspace across multiple independent Raft groups.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    MultiRaftCoordinator                         │
//! │                                                                 │
//! │  ┌───────────────────────────────────────────────────────────┐ │
//! │  │                     ShardRouter                           │ │
//! │  │  - Hash-based key routing                                 │ │
//! │  │  - Shard registration                                     │ │
//! │  │  - Leader tracking                                        │ │
//! │  └───────────────────────────────────────────────────────────┘ │
//! │                             │                                   │
//! │            ┌────────────────┼────────────────┐                 │
//! │            ▼                ▼                ▼                 │
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
//! │  │   Shard 0    │  │   Shard 1    │  │   Shard N    │  ...   │
//! │  │  (Raft Grp)  │  │  (Raft Grp)  │  │  (Raft Grp)  │        │
//! │  │              │  │              │  │              │        │
//! │  │  Keys: 0%    │  │  Keys: 25%   │  │  Keys: 75%   │        │
//! │  │  to 25%      │  │  to 50%      │  │  to 100%     │        │
//! │  └──────────────┘  └──────────────┘  └──────────────┘        │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Why Multi-Raft?
//!
//! A single Raft group has a theoretical limit of ~50K writes/sec, but
//! practical limits are often ~10-20K writes/sec due to:
//! - Network latency for replication
//! - Disk fsync for durability
//! - Leader bottleneck (all writes go through leader)
//!
//! Multi-Raft solves this by:
//! - Partitioning keyspace into N shards (default: 16)
//! - Each shard has its own independent Raft group
//! - Different shards can have different leaders
//! - Write throughput scales linearly with shards
//!
//! # Example
//!
//! ```rust,ignore
//! use distributed_cache::multiraft::{MultiRaftBuilder, MultiRaftConfig};
//!
//! // Create a Multi-Raft coordinator with 16 shards
//! let coordinator = MultiRaftBuilder::new(node_id)
//!     .num_shards(16)
//!     .replica_factor(3)
//!     .shard_capacity(100_000)
//!     .build_and_init()
//!     .await?;
//!
//! // Use like a regular cache - routing is automatic
//! coordinator.put("user:123", "Alice").await?;
//! let value = coordinator.get(b"user:123").await?;
//!
//! // Check which shard owns a key
//! let shard_id = coordinator.shard_for_key(b"user:123");
//!
//! // Get statistics
//! let stats = coordinator.stats();
//! println!("Total entries: {}", stats.total_entries);
//! println!("Ops/sec: {:.2}", stats.operations_per_sec);
//! ```
//!
//! # Shard Assignment
//!
//! Keys are assigned to shards using consistent hashing:
//!
//! ```text
//! shard_id = hash(key) % num_shards
//! ```
//!
//! This ensures:
//! - Same key always goes to same shard
//! - Keys are evenly distributed across shards
//! - Adding/removing shards requires rebalancing
//!
//! # Performance
//!
//! With Multi-Raft, write throughput scales linearly:
//!
//! | Shards | Theoretical Max | Practical Max |
//! |--------|-----------------|---------------|
//! | 1      | 50K/sec         | 10-20K/sec    |
//! | 4      | 200K/sec        | 40-80K/sec    |
//! | 16     | 800K/sec        | 160-320K/sec  |
//! | 64     | 3.2M/sec        | 640K-1.3M/sec |
//!
//! Read throughput is not affected by Raft and remains high (~500K/sec per node).

mod coordinator;
mod router;
mod shard;

pub use coordinator::{
    CoordinatorState, MultiRaftBuilder, MultiRaftConfig, MultiRaftCoordinator, MultiRaftStats,
};
pub use router::{BatchRouter, RouterConfig, RoutingDecision, ShardRouter};
pub use shard::{Shard, ShardAssignment, ShardConfig, ShardId, ShardInfo, ShardRange, ShardState};
