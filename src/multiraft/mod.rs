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
//!
//! # Lock Hierarchy
//!
//! The multi-raft module uses multiple locks for thread safety. To prevent deadlocks,
//! locks must ALWAYS be acquired in the following order. Never acquire a higher-numbered
//! lock while holding a lower-numbered lock.
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────┐
//! │                          LOCK ACQUISITION ORDER                            │
//! │                                                                            │
//! │  1. MultiRaftCoordinator.state            (highest priority)              │
//! │  2. MultiRaftCoordinator.shard_configs                                    │
//! │  3. MigrationOrchestrator.active_orchestrations                           │
//! │  4. ShardMigrationCoordinator.active_migrations                           │
//! │  5. ShardPlacement.ring                                                   │
//! │  6. ShardPlacement.pending_ring                                           │
//! │  7. ShardRegistry internal locks          (lowest priority)               │
//! │                                                                            │
//! └────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Critical Rules
//!
//! 1. **Never hold multiple write locks simultaneously** if you can avoid it.
//!    Release one lock before acquiring another when possible.
//!
//! 2. **ring before pending_ring**: In `ShardPlacement`, always acquire the `ring`
//!    lock before `pending_ring`. The `commit()` method follows this pattern.
//!
//! 3. **Validate before acquiring**: For operations like `create_shard()`, acquire
//!    the lock first, then validate. Don't validate, release, then re-acquire
//!    (this creates TOCTOU races).
//!
//! 4. **Async boundaries**: Don't hold locks across `.await` points. Clone data
//!    out, drop the lock, then perform async work.
//!
//! 5. **Persistence order**: For crash consistency, persist to disk BEFORE
//!    updating in-memory state. This way, a crash between operations leaves
//!    the disk in a consistent state that can be recovered.
//!
//! ## Example: Safe Lock Pattern
//!
//! ```rust,ignore
//! // GOOD: Clone out, release lock, then do async work
//! let data = {
//!     let guard = self.data.read();
//!     guard.clone()
//! };
//! // Lock released, safe to await
//! do_async_work(data).await?;
//!
//! // BAD: Holding lock across await
//! let guard = self.data.read();
//! do_async_work(&*guard).await?; // DEADLOCK RISK!
//! ```
//!
//! ## Example: Atomic Read-Modify-Write
//!
//! ```rust,ignore
//! // GOOD: Single lock for atomic operation
//! let mut guard = self.state.write();
//! let old_value = guard.value;
//! guard.value = new_value;
//! drop(guard);
//!
//! // BAD: Read-release-write (TOCTOU race)
//! let old = self.state.read().value;
//! // Another thread could modify here!
//! self.state.write().value = new_value;
//! ```

mod coordinator;
mod migration_cleanup;
pub mod memberlist_integration;
mod migration;
mod migration_metrics;
mod migration_orchestrator;
mod migration_routing;
mod migration_transport;
mod persistent_migration_store;
mod raft_migration;
mod router;
mod shard;
mod shard_placement;
mod shard_recovery;
mod shard_registry;
mod shard_storage;

pub use coordinator::{
    CoordinatorState, MultiRaftBuilder, MultiRaftConfig, MultiRaftCoordinator, MultiRaftShardController,
    MultiRaftStats,
};
pub use memberlist_integration::{ShardLeaderBroadcaster, ShardLeaderTracker};
pub use migration::{
    InMemoryMigrationStore, MigrationCheckpoint, MigrationConfig, MigrationPhase,
    MigrationProgress, MigrationStateStore, MigrationStats, ShardMigration,
    ShardMigrationCoordinator, TransferBatch, TransferEntry,
};
pub use migration_metrics::{MigrationMetrics, MigrationMetricsSnapshot, MigrationTimer};
pub use migration_orchestrator::{
    DataTransporter, MigrationCommand, MigrationOrchestrator, MigrationRaftProposer,
    NoOpDataTransporter, NoOpRaftProposer, OrchestrationState,
};
pub use migration_transport::{
    RpcDataTransporter, RpcMigrationRaftProposer, ShardAccessor,
};
pub use migration_cleanup::{
    CleanupResource, CleanupResult, MigrationCleanupHandler, MigrationCleanupManager,
    NoOpCleanupHandler,
};
pub use migration_routing::{
    DualWriteResult, DualWriteTracker, FailedDualWrite, MigrationRouter, MigrationRoutingConfig,
    MigrationRoutingStrategy, ReadTargets, RoutingDecision as MigrationRoutingDecision, WriteTarget,
};
pub use persistent_migration_store::{
    FileMigrationStore, InMemoryRaftMigrationStore, MigrationCheckpointData,
    PersistentMigrationStore,
};
pub use raft_migration::{
    JanitorReport, NoOpShardRaftController, RaftChangeType, RaftMembershipChange,
    RaftMigrationConfig, RaftMigrationCoordinator, RaftMigrationPhase, RaftMigrationProgress,
    RaftMigrationStats, RaftShardMigration, ShardRaftController,
};
pub use router::{BatchRouter, RouterConfig, RoutingDecision, ShardRouter};
pub use shard::{Shard, ShardAssignment, ShardConfig, ShardId, ShardInfo, ShardRange, ShardState};
pub use shard_placement::{MovementType, PlacementConfig, ShardMovement, ShardPlacement};
pub use shard_recovery::{
    RecoveredShard, RecoveryCoordinatorBuilder, RecoveryStats, ShardRecoveryCoordinator,
    ShardRecoveryDetail,
};
pub use shard_registry::{ShardLifecycleState, ShardMetadata, ShardRegistry};
pub use shard_storage::{
    PersistedShardMetadata, PersistedShardRegistry, ShardSnapshotInfo, ShardStorageConfig,
    ShardStorageManager,
};
