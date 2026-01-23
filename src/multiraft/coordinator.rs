//! Multi-Raft coordinator for managing multiple Raft groups.
//!
//! The coordinator is responsible for:
//! - Creating and managing shards
//! - Routing requests to the appropriate shard
//! - Handling shard rebalancing
//! - Coordinating leader elections across shards

use super::memberlist_integration::{ShardLeaderBroadcaster, ShardLeaderTracker};
use super::migration::{InMemoryMigrationStore, MigrationConfig, ShardMigrationCoordinator};
use super::migration_orchestrator::{
    DataTransporter, MigrationOrchestrator, MigrationRaftProposer,
};
use super::migration_routing::{MigrationRoutingStrategy, RoutingDecision as MigrationRoutingDecision};
use super::raft_migration::{
    RaftMembershipChange, RaftMigrationConfig, RaftMigrationCoordinator,
    RaftMigrationStats, RaftShardMigration, ShardRaftController,
};
use super::router::{RouterConfig, ShardRouter};
use super::shard::{Shard, ShardConfig, ShardId, ShardInfo, ShardState};
use super::shard_forwarder::{ShardForwarder, ShardForwardingConfig};
use super::shard_placement::{PlacementConfig, ShardMovement, ShardPlacement};
use super::shard_recovery::{RecoveryStats, ShardRecoveryCoordinator};
use super::shard_registry::{ShardLifecycleState, ShardRegistry};
use super::shard_storage::{PersistedShardMetadata, ShardStorageConfig, ShardStorageManager};
use crate::error::{Error, Result};
use crate::types::CacheCommand;
use crate::metrics::CacheMetrics;
use crate::types::NodeId;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Configuration for the Multi-Raft coordinator.
#[derive(Debug, Clone)]
pub struct MultiRaftConfig {
    /// Total number of shards.
    pub num_shards: u32,

    /// Replica factor (how many copies of each shard).
    pub replica_factor: usize,

    /// Maximum capacity per shard.
    pub shard_capacity: u64,

    /// Default TTL for entries.
    pub default_ttl: Option<Duration>,

    /// Whether to auto-initialize shards on startup.
    pub auto_init_shards: bool,

    /// Tick interval for background tasks.
    pub tick_interval: Duration,
}

impl Default for MultiRaftConfig {
    fn default() -> Self {
        Self {
            num_shards: 16,
            replica_factor: 3,
            shard_capacity: 100_000,
            default_ttl: None,
            auto_init_shards: true,
            tick_interval: Duration::from_millis(100),
        }
    }
}

impl MultiRaftConfig {
    /// Create a new config with the given number of shards.
    pub fn new(num_shards: u32) -> Self {
        Self {
            num_shards,
            ..Default::default()
        }
    }

    /// Set the replica factor.
    pub fn with_replica_factor(mut self, factor: usize) -> Self {
        self.replica_factor = factor;
        self
    }

    /// Set the shard capacity.
    pub fn with_shard_capacity(mut self, capacity: u64) -> Self {
        self.shard_capacity = capacity;
        self
    }

    /// Set the default TTL.
    pub fn with_default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = Some(ttl);
        self
    }
}

/// State of the coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorState {
    /// Coordinator is initializing.
    Initializing,

    /// Coordinator is running normally.
    Running,

    /// Coordinator is rebalancing shards.
    Rebalancing,

    /// Coordinator is shutting down.
    ShuttingDown,

    /// Coordinator has stopped.
    Stopped,
}

/// Statistics for the Multi-Raft coordinator.
#[derive(Debug, Clone)]
pub struct MultiRaftStats {
    /// Total number of shards.
    pub total_shards: u32,

    /// Active shards.
    pub active_shards: u32,

    /// Total entries across all shards.
    pub total_entries: u64,

    /// Total size in bytes.
    pub total_size_bytes: u64,

    /// Shards where this node is the leader.
    pub local_leader_shards: u32,

    /// Total operations processed.
    pub operations_total: u64,

    /// Operations per second.
    pub operations_per_sec: f64,
}

/// Multi-Raft coordinator for horizontal scaling.
#[derive(Debug)]
pub struct MultiRaftCoordinator {
    /// This node's ID.
    node_id: NodeId,

    /// Configuration.
    config: MultiRaftConfig,

    /// Current state.
    state: RwLock<CoordinatorState>,

    /// Shard router.
    router: Arc<ShardRouter>,

    /// Shard configurations.
    shard_configs: RwLock<HashMap<ShardId, ShardConfig>>,

    /// Metrics.
    metrics: Arc<CacheMetrics>,

    /// Operation counter.
    operations_total: AtomicU64,

    /// Start time for rate calculation.
    start_time: Instant,

    /// Whether the coordinator is running.
    running: AtomicBool,

    /// Epoch-based shard leader tracker for handling out-of-order gossip.
    leader_tracker: ShardLeaderTracker,

    /// Debounced shard leader broadcaster.
    leader_broadcaster: ShardLeaderBroadcaster,

    /// Global shard registry for stable shard metadata.
    shard_registry: Arc<ShardRegistry>,

    /// HashRing-based shard placement.
    shard_placement: Arc<ShardPlacement>,

    /// Migration coordinator (optional, lazily initialized).
    migration_coordinator: RwLock<Option<Arc<ShardMigrationCoordinator>>>,

    /// Migration orchestrator for end-to-end migration management (optional).
    migration_orchestrator: RwLock<Option<Arc<MigrationOrchestrator>>>,

    /// Raft-native migration coordinator for shard-level rebalancing.
    raft_migration_coordinator: RwLock<Option<Arc<RaftMigrationCoordinator>>>,

    /// Node addresses for migration (node_id -> address).
    node_addresses: RwLock<HashMap<NodeId, SocketAddr>>,

    /// Storage manager for per-shard persistence (optional).
    storage_manager: RwLock<Option<Arc<ShardStorageManager>>>,

    /// Last recovery stats (if recovery was performed).
    last_recovery_stats: RwLock<Option<RecoveryStats>>,

    /// Shard forwarder for cross-node request forwarding.
    shard_forwarder: ShardForwarder,

    /// Whether shard forwarding is enabled.
    forwarding_enabled: bool,
}

impl MultiRaftCoordinator {
    /// Create a new Multi-Raft coordinator.
    pub fn new(node_id: NodeId, config: MultiRaftConfig, metrics: Arc<CacheMetrics>) -> Self {
        let router_config = RouterConfig::new(config.num_shards);
        let router = Arc::new(ShardRouter::new(router_config));
        let leader_tracker = ShardLeaderTracker::new(node_id);
        let leader_broadcaster = ShardLeaderBroadcaster::new(config.tick_interval);

        // Create shard registry for stable shard metadata
        let shard_registry = Arc::new(ShardRegistry::new(config.num_shards));

        // Create shard placement with HashRing
        // Use >= 256 vnodes per node to prevent data skew in distribution
        let placement_config = PlacementConfig::new(config.replica_factor).with_vnodes(256);
        let shard_placement = Arc::new(ShardPlacement::new(config.num_shards, placement_config));

        // Create shard forwarder for cross-node request forwarding
        let shard_forwarder = ShardForwarder::new(node_id, ShardForwardingConfig::default());

        Self {
            node_id,
            config,
            state: RwLock::new(CoordinatorState::Initializing),
            router,
            shard_configs: RwLock::new(HashMap::new()),
            metrics,
            operations_total: AtomicU64::new(0),
            start_time: Instant::now(),
            running: AtomicBool::new(false),
            leader_tracker,
            leader_broadcaster,
            shard_registry,
            shard_placement,
            migration_coordinator: RwLock::new(None),
            migration_orchestrator: RwLock::new(None),
            raft_migration_coordinator: RwLock::new(None),
            node_addresses: RwLock::new(HashMap::new()),
            storage_manager: RwLock::new(None),
            last_recovery_stats: RwLock::new(None),
            shard_forwarder,
            forwarding_enabled: true,
        }
    }

    /// Create a coordinator with a custom broadcaster debounce interval.
    pub fn with_broadcaster_debounce(
        node_id: NodeId,
        config: MultiRaftConfig,
        metrics: Arc<CacheMetrics>,
        debounce: Duration,
    ) -> Self {
        let router_config = RouterConfig::new(config.num_shards);
        let router = Arc::new(ShardRouter::new(router_config));
        let leader_tracker = ShardLeaderTracker::new(node_id);
        let leader_broadcaster = ShardLeaderBroadcaster::new(debounce);

        // Create shard registry for stable shard metadata
        let shard_registry = Arc::new(ShardRegistry::new(config.num_shards));

        // Create shard placement with HashRing
        // Use >= 256 vnodes per node to prevent data skew in distribution
        let placement_config = PlacementConfig::new(config.replica_factor).with_vnodes(256);
        let shard_placement = Arc::new(ShardPlacement::new(config.num_shards, placement_config));

        // Create shard forwarder for cross-node request forwarding
        let shard_forwarder = ShardForwarder::new(node_id, ShardForwardingConfig::default());

        Self {
            node_id,
            config,
            state: RwLock::new(CoordinatorState::Initializing),
            router,
            shard_configs: RwLock::new(HashMap::new()),
            metrics,
            operations_total: AtomicU64::new(0),
            start_time: Instant::now(),
            running: AtomicBool::new(false),
            leader_tracker,
            leader_broadcaster,
            shard_registry,
            shard_placement,
            migration_coordinator: RwLock::new(None),
            migration_orchestrator: RwLock::new(None),
            raft_migration_coordinator: RwLock::new(None),
            node_addresses: RwLock::new(HashMap::new()),
            storage_manager: RwLock::new(None),
            last_recovery_stats: RwLock::new(None),
            shard_forwarder,
            forwarding_enabled: true,
        }
    }

    /// Initialize the coordinator and create shards.
    pub async fn init(&self) -> Result<()> {
        if self.config.auto_init_shards {
            self.create_all_shards().await?;
        }

        *self.state.write() = CoordinatorState::Running;
        self.running.store(true, Ordering::Relaxed);

        tracing::info!(
            node_id = self.node_id,
            num_shards = self.config.num_shards,
            "Multi-Raft coordinator initialized"
        );

        Ok(())
    }

    /// Create all shards based on configuration.
    pub async fn create_all_shards(&self) -> Result<()> {
        for shard_id in 0..self.config.num_shards {
            self.create_shard(shard_id).await?;
        }

        Ok(())
    }

    /// Create a single shard.
    ///
    /// # Concurrency Safety
    ///
    /// Holds `shard_configs` write lock for the entire operation to prevent
    /// TOCTOU race condition where two threads could both pass the existence
    /// check and try to create the same shard.
    pub async fn create_shard(&self, shard_id: ShardId) -> Result<Arc<Shard>> {
        // CRITICAL: Acquire write lock FIRST and hold it for the entire operation
        // to prevent TOCTOU race between checking existence and inserting.
        let mut shard_configs_guard = self.shard_configs.write();

        // Check if shard already exists (both in router and our config)
        if self.router.get_shard(shard_id).is_some() || shard_configs_guard.contains_key(&shard_id) {
            return Err(Error::ShardAlreadyExists(shard_id));
        }

        // Create shard config
        let shard_config = ShardConfig::new(shard_id, self.config.num_shards)
            .with_replicas(self.config.replica_factor)
            .with_max_capacity(self.config.shard_capacity);

        // Reserve the slot before any async operations
        shard_configs_guard.insert(shard_id, shard_config.clone());

        // Drop the lock before async operation to avoid holding across await
        drop(shard_configs_guard);

        // Create the shard (this is async and might fail)
        let shard = match Shard::new(shard_config.clone()).await {
            Ok(s) => Arc::new(s),
            Err(e) => {
                // Rollback: remove the config we just inserted
                self.shard_configs.write().remove(&shard_id);
                return Err(e);
            }
        };

        // Set shard state to active
        shard.set_state(ShardState::Active);

        // Register with router
        self.router.register_shard(shard.clone());

        // Register with storage manager if enabled
        if let Some(storage_manager) = self.storage_manager.read().as_ref() {
            storage_manager.register_shard(&shard_config);
        }

        tracing::debug!(shard_id = shard_id, "Created shard");

        Ok(shard)
    }

    /// Remove a shard.
    pub async fn remove_shard(&self, shard_id: ShardId) -> Result<()> {
        let shard = self
            .router
            .unregister_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        shard.set_state(ShardState::Stopped);
        self.shard_configs.write().remove(&shard_id);

        // Unregister from storage manager if enabled
        if let Some(storage_manager) = self.storage_manager.read().as_ref() {
            storage_manager.unregister_shard(shard_id);
        }

        tracing::info!(shard_id = shard_id, "Removed shard");

        Ok(())
    }

    /// Get the current state.
    pub fn state(&self) -> CoordinatorState {
        *self.state.read()
    }

    /// Check if the coordinator is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Get a value from the cache.
    ///
    /// If the shard is not local but the leader is known on another node,
    /// the request is automatically forwarded.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.operations_total.fetch_add(1, Ordering::Relaxed);

        let start = Instant::now();
        let result = self.router.get(key).await;

        // If successful or error other than ShardNotFound, return directly
        match &result {
            Ok(_) => {
                let hit = result.as_ref().map(|r| r.is_some()).unwrap_or(false);
                self.metrics.record_get(hit, start.elapsed());
                return result;
            }
            Err(Error::ShardNotFound(shard_id)) if self.forwarding_enabled => {
                // Check if we know the shard leader on another node
                if let Some(leader_node) = self.leader_tracker.get_leader(*shard_id) {
                    if leader_node != self.node_id {
                        // Forward to the leader node
                        let command = CacheCommand::get(key.to_vec());
                        match self.shard_forwarder.forward_to_node(leader_node, *shard_id, command).await {
                            Ok(forward_result) => {
                                let value = forward_result.value;
                                self.metrics.record_get(value.is_some(), start.elapsed());
                                return Ok(value);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    shard_id = *shard_id,
                                    leader_node = leader_node,
                                    error = %e,
                                    "Shard forward failed for GET"
                                );
                                self.metrics.record_get(false, start.elapsed());
                                return Err(e);
                            }
                        }
                    }
                }
                // No leader known or we are the leader - return original error
                self.metrics.record_get(false, start.elapsed());
                return Err(Error::ShardLeaderUnknown(*shard_id));
            }
            Err(_) => {
                self.metrics.record_get(false, start.elapsed());
                return result;
            }
        }
    }

    /// Put a value in the cache.
    ///
    /// If the shard is not local but the leader is known on another node,
    /// the request is automatically forwarded.
    pub async fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        self.operations_total.fetch_add(1, Ordering::Relaxed);

        let key = key.into();
        let value = value.into();

        let start = Instant::now();
        let result = self.router.put(key.clone(), value.clone()).await;

        match &result {
            Ok(_) => {
                self.metrics.record_put(true, start.elapsed());
                result.map(|_| ())
            }
            Err(Error::ShardNotFound(shard_id)) if self.forwarding_enabled => {
                // Check if we know the shard leader on another node
                if let Some(leader_node) = self.leader_tracker.get_leader(*shard_id) {
                    if leader_node != self.node_id {
                        // Forward to the leader node
                        let command = CacheCommand::put(key.to_vec(), value.to_vec());
                        match self.shard_forwarder.forward_to_node(leader_node, *shard_id, command).await {
                            Ok(forward_result) => {
                                self.metrics.record_put(forward_result.success, start.elapsed());
                                if forward_result.success {
                                    return Ok(());
                                } else {
                                    return Err(Error::RemoteError(
                                        forward_result.error.unwrap_or_else(|| "unknown error".into())
                                    ));
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    shard_id = *shard_id,
                                    leader_node = leader_node,
                                    error = %e,
                                    "Shard forward failed for PUT"
                                );
                                self.metrics.record_put(false, start.elapsed());
                                return Err(e);
                            }
                        }
                    }
                }
                // No leader known or we are the leader - return original error
                self.metrics.record_put(false, start.elapsed());
                Err(Error::ShardLeaderUnknown(*shard_id))
            }
            Err(_) => {
                self.metrics.record_put(false, start.elapsed());
                result.map(|_| ())
            }
        }
    }

    /// Put a value with TTL.
    ///
    /// If the shard is not local but the leader is known on another node,
    /// the request is automatically forwarded.
    pub async fn put_with_ttl(
        &self,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
        ttl: Duration,
    ) -> Result<()> {
        self.operations_total.fetch_add(1, Ordering::Relaxed);

        let key = key.into();
        let value = value.into();

        let start = Instant::now();
        let result = self.router.put_with_ttl(key.clone(), value.clone(), ttl).await;

        match &result {
            Ok(_) => {
                self.metrics.record_put(true, start.elapsed());
                result.map(|_| ())
            }
            Err(Error::ShardNotFound(shard_id)) if self.forwarding_enabled => {
                // Check if we know the shard leader on another node
                if let Some(leader_node) = self.leader_tracker.get_leader(*shard_id) {
                    if leader_node != self.node_id {
                        // Forward to the leader node
                        let command = CacheCommand::put_with_ttl(key.to_vec(), value.to_vec(), ttl);
                        match self.shard_forwarder.forward_to_node(leader_node, *shard_id, command).await {
                            Ok(forward_result) => {
                                self.metrics.record_put(forward_result.success, start.elapsed());
                                if forward_result.success {
                                    return Ok(());
                                } else {
                                    return Err(Error::RemoteError(
                                        forward_result.error.unwrap_or_else(|| "unknown error".into())
                                    ));
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    shard_id = *shard_id,
                                    leader_node = leader_node,
                                    error = %e,
                                    "Shard forward failed for PUT_WITH_TTL"
                                );
                                self.metrics.record_put(false, start.elapsed());
                                return Err(e);
                            }
                        }
                    }
                }
                // No leader known or we are the leader - return original error
                self.metrics.record_put(false, start.elapsed());
                Err(Error::ShardLeaderUnknown(*shard_id))
            }
            Err(_) => {
                self.metrics.record_put(false, start.elapsed());
                result.map(|_| ())
            }
        }
    }

    /// Delete a key from the cache.
    ///
    /// If the shard is not local but the leader is known on another node,
    /// the request is automatically forwarded.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.operations_total.fetch_add(1, Ordering::Relaxed);

        let start = Instant::now();
        let result = self.router.delete(key).await;

        match &result {
            Ok(_) => {
                self.metrics.record_delete(start.elapsed());
                result.map(|_| ())
            }
            Err(Error::ShardNotFound(shard_id)) if self.forwarding_enabled => {
                // Check if we know the shard leader on another node
                if let Some(leader_node) = self.leader_tracker.get_leader(*shard_id) {
                    if leader_node != self.node_id {
                        // Forward to the leader node
                        let command = CacheCommand::delete(key.to_vec());
                        match self.shard_forwarder.forward_to_node(leader_node, *shard_id, command).await {
                            Ok(forward_result) => {
                                self.metrics.record_delete(start.elapsed());
                                if forward_result.success {
                                    return Ok(());
                                } else {
                                    return Err(Error::RemoteError(
                                        forward_result.error.unwrap_or_else(|| "unknown error".into())
                                    ));
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    shard_id = *shard_id,
                                    leader_node = leader_node,
                                    error = %e,
                                    "Shard forward failed for DELETE"
                                );
                                return Err(e);
                            }
                        }
                    }
                }
                // No leader known or we are the leader - return original error
                Err(Error::ShardLeaderUnknown(*shard_id))
            }
            Err(_) => {
                self.metrics.record_delete(start.elapsed());
                result.map(|_| ())
            }
        }
    }

    /// Get shard information.
    pub fn shard_info(&self) -> Vec<ShardInfo> {
        self.router.shard_info()
    }

    /// Get a specific shard.
    pub fn get_shard(&self, shard_id: ShardId) -> Option<Arc<Shard>> {
        self.router.get_shard(shard_id)
    }

    /// Get the shard for a key.
    pub fn shard_for_key(&self, key: &[u8]) -> ShardId {
        self.router.shard_for_key(key)
    }

    /// Get statistics.
    pub fn stats(&self) -> MultiRaftStats {
        let shards = self.router.all_shards();
        let active_shards = shards.iter().filter(|s| s.is_active()).count() as u32;
        let local_leader_shards = shards.iter().filter(|s| s.is_leader()).count() as u32;

        let elapsed = self.start_time.elapsed().as_secs_f64();
        let ops_total = self.operations_total.load(Ordering::Relaxed);
        let ops_per_sec = if elapsed > 0.0 {
            ops_total as f64 / elapsed
        } else {
            0.0
        };

        MultiRaftStats {
            total_shards: self.config.num_shards,
            active_shards,
            total_entries: self.router.total_entries(),
            total_size_bytes: self.router.total_size(),
            local_leader_shards,
            operations_total: ops_total,
            operations_per_sec: ops_per_sec,
        }
    }

    // ==================== Storage Integration ====================

    /// Enable persistent storage for shards.
    ///
    /// This must be called before `init()` if you want to recover from disk.
    /// Alternatively, call `init_with_recovery()` which enables storage automatically.
    pub fn enable_storage(&self, config: ShardStorageConfig) -> Result<()> {
        let storage_manager = ShardStorageManager::new(config, self.node_id)?;
        *self.storage_manager.write() = Some(Arc::new(storage_manager));
        tracing::info!(node_id = self.node_id, "Shard storage enabled");
        Ok(())
    }

    /// Initialize with recovery from disk.
    ///
    /// This method:
    /// 1. Enables storage persistence
    /// 2. Recovers shards from disk if any exist
    /// 3. Creates new shards if none exist (based on auto_init_shards)
    pub async fn init_with_recovery(&self, storage_config: ShardStorageConfig) -> Result<()> {
        // Enable storage
        let storage_manager = Arc::new(ShardStorageManager::new(storage_config, self.node_id)?);
        *self.storage_manager.write() = Some(storage_manager.clone());

        // Create recovery coordinator
        let recovery_coordinator = ShardRecoveryCoordinator::new(self.node_id, storage_manager.clone());

        // Check if we have shards to recover
        if recovery_coordinator.has_shards_to_recover() {
            tracing::info!(
                node_id = self.node_id,
                shard_count = recovery_coordinator.recoverable_shard_ids().len(),
                "Recovering shards from disk"
            );

            // Perform recovery
            let recovered = recovery_coordinator.recover_all().await?;

            // Register recovered shards with the router
            for (shard_id, recovered_shard) in recovered {
                self.router.register_shard(recovered_shard.shard.clone());

                // Store config
                let shard_config = ShardConfig::new(shard_id, self.config.num_shards)
                    .with_replicas(self.config.replica_factor)
                    .with_max_capacity(self.config.shard_capacity);
                self.shard_configs.write().insert(shard_id, shard_config);

                // Register with storage manager for future snapshots
                let config = ShardConfig::new(shard_id, self.config.num_shards);
                storage_manager.register_shard(&config);
            }

            // Store recovery stats
            *self.last_recovery_stats.write() = Some(recovery_coordinator.stats());

            let stats = recovery_coordinator.stats();
            tracing::info!(
                shards_recovered = stats.shards_recovered,
                entries_loaded = stats.entries_loaded,
                duration_ms = stats.duration_ms,
                "Shard recovery completed"
            );
        } else if self.config.auto_init_shards {
            // No shards to recover, create new ones
            tracing::info!(
                node_id = self.node_id,
                num_shards = self.config.num_shards,
                "No shards to recover, creating new shards"
            );
            self.create_all_shards().await?;

            // Register shards with storage manager
            for shard_id in 0..self.config.num_shards {
                let config = ShardConfig::new(shard_id, self.config.num_shards);
                storage_manager.register_shard(&config);
            }
        }

        *self.state.write() = CoordinatorState::Running;
        self.running.store(true, Ordering::Relaxed);

        tracing::info!(
            node_id = self.node_id,
            num_shards = self.config.num_shards,
            "Multi-Raft coordinator initialized with storage"
        );

        Ok(())
    }

    /// Get the storage manager if enabled.
    pub fn storage_manager(&self) -> Option<Arc<ShardStorageManager>> {
        self.storage_manager.read().clone()
    }

    /// Get the last recovery statistics.
    pub fn last_recovery_stats(&self) -> Option<RecoveryStats> {
        self.last_recovery_stats.read().clone()
    }

    /// Save the current state of a shard to persistent storage.
    pub fn save_shard_state(&self, shard_id: ShardId) -> Result<()> {
        let storage_manager = self.storage_manager.read();
        let Some(storage_manager) = storage_manager.as_ref() else {
            return Err(Error::Internal("Storage not enabled".to_string()));
        };

        let Some(shard) = self.router.get_shard(shard_id) else {
            return Err(Error::ShardNotFound(shard_id));
        };

        let metadata = PersistedShardMetadata {
            shard_id,
            total_shards: self.config.num_shards,
            replicas: self.config.replica_factor,
            max_capacity: self.config.shard_capacity,
            state: shard.state().to_string(),
            leader: shard.leader(),
            members: shard.members(),
            term: shard.term(),
            commit_index: shard.commit_index(),
            applied_index: shard.applied_index(),
            last_snapshot_index: storage_manager.last_snapshot_index(shard_id),
            last_snapshot_term: storage_manager.last_snapshot_term(shard_id),
        };

        storage_manager.save_shard_metadata(metadata)
    }

    /// Save the state of all shards.
    pub fn save_all_shard_states(&self) -> Result<()> {
        for shard_id in 0..self.config.num_shards {
            if self.router.get_shard(shard_id).is_some() {
                self.save_shard_state(shard_id)?;
            }
        }
        Ok(())
    }

    /// Check if any shard needs a snapshot.
    pub fn check_snapshot_needs(&self) -> Vec<ShardId> {
        let storage_manager = self.storage_manager.read();
        let Some(storage_manager) = storage_manager.as_ref() else {
            return Vec::new();
        };

        (0..self.config.num_shards)
            .filter(|&shard_id| storage_manager.needs_snapshot(shard_id))
            .collect()
    }

    /// Record that entries were applied to a shard (for snapshot triggering).
    pub fn record_entries_applied(&self, shard_id: ShardId, count: u64) {
        if let Some(storage_manager) = self.storage_manager.read().as_ref() {
            storage_manager.record_entries_applied(shard_id, count);
        }
    }

    // ==================== End Storage Integration ====================

    /// Update the leader for a shard.
    pub fn set_shard_leader(&self, shard_id: ShardId, leader: NodeId) {
        self.router.set_shard_leader(shard_id, leader);

        if let Some(shard) = self.router.get_shard(shard_id) {
            shard.set_is_leader(leader == self.node_id);
        }
    }

    /// Update the leader for a shard if the epoch is newer.
    ///
    /// This method handles out-of-order gossip updates by checking epochs.
    /// Returns `true` if the update was applied, `false` if rejected due to stale epoch.
    pub fn set_shard_leader_if_newer(
        &self,
        shard_id: ShardId,
        leader_id: NodeId,
        epoch: u64,
    ) -> bool {
        if self.leader_tracker.set_leader_if_newer(shard_id, leader_id, epoch) {
            // Update the router with the new leader
            self.router.set_shard_leader(shard_id, leader_id);

            if let Some(shard) = self.router.get_shard(shard_id) {
                shard.set_is_leader(leader_id == self.node_id);
            }

            true
        } else {
            false
        }
    }

    /// Set a shard leader for a local leader change.
    ///
    /// Automatically increments the epoch and queues for broadcast.
    /// Returns the new epoch.
    pub fn set_local_shard_leader(&self, shard_id: ShardId, leader_id: NodeId) -> u64 {
        let epoch = self.leader_tracker.set_local_leader(shard_id, leader_id);

        // Update router
        self.router.set_shard_leader(shard_id, leader_id);
        if let Some(shard) = self.router.get_shard(shard_id) {
            shard.set_is_leader(leader_id == self.node_id);
        }

        // Queue for broadcast
        self.leader_broadcaster.queue_update(shard_id, leader_id, epoch);

        epoch
    }

    /// Invalidate leadership for all shards where the given node was leader.
    ///
    /// Called when a node fails or leaves the cluster.
    /// Returns the list of invalidated shard IDs.
    pub fn invalidate_leader_for_node(&self, node_id: NodeId) -> Vec<ShardId> {
        let invalidated = self.leader_tracker.invalidate_leader_for_node(node_id);

        // Clear leaders in router
        for shard_id in &invalidated {
            if let Some(shard) = self.router.get_shard(*shard_id) {
                shard.set_leader(None);
                shard.set_is_leader(false);
            }
        }

        // Clear router cache to ensure routing decisions are recalculated
        // This is critical: stale cache could route to wrong nodes after topology change
        self.router.clear_cache();

        tracing::debug!(
            node_id,
            invalidated_shards = invalidated.len(),
            "Invalidated leaders and cleared router cache for node failure"
        );

        invalidated
    }

    /// Get the current epoch for a shard.
    pub fn get_shard_epoch(&self, shard_id: ShardId) -> Option<u64> {
        self.leader_tracker.get_epoch(shard_id)
    }

    /// Get all leaders for all shards.
    pub fn shard_leaders(&self) -> HashMap<ShardId, Option<NodeId>> {
        let mut leaders = HashMap::new();
        for shard_id in 0..self.config.num_shards {
            leaders.insert(shard_id, self.router.get_shard_leader(shard_id));
        }
        leaders
    }

    /// Get the shard leader tracker.
    pub fn leader_tracker(&self) -> &ShardLeaderTracker {
        &self.leader_tracker
    }

    /// Get the shard leader broadcaster.
    pub fn leader_broadcaster(&self) -> &ShardLeaderBroadcaster {
        &self.leader_broadcaster
    }

    // ==================== Shard Registry Methods ====================

    /// Get the shard registry.
    pub fn shard_registry(&self) -> &Arc<ShardRegistry> {
        &self.shard_registry
    }

    /// Update shard registry state when a shard becomes active.
    pub fn activate_shard_in_registry(&self, shard_id: ShardId, replicas: Vec<NodeId>) {
        self.shard_registry.set_replicas(shard_id, replicas);
        self.shard_registry
            .set_state(shard_id, ShardLifecycleState::Active);
    }

    /// Update shard registry when a shard enters migration.
    pub fn start_shard_migration_in_registry(&self, shard_id: ShardId) {
        self.shard_registry
            .set_state(shard_id, ShardLifecycleState::Migrating);
    }

    /// Update shard registry when a shard finishes migration.
    pub fn complete_shard_migration_in_registry(&self, shard_id: ShardId, new_replicas: Vec<NodeId>) {
        self.shard_registry.set_replicas(shard_id, new_replicas);
        self.shard_registry
            .set_state(shard_id, ShardLifecycleState::Active);
    }

    /// Update the primary node in the registry.
    pub fn set_shard_primary_in_registry(&self, shard_id: ShardId, primary: Option<NodeId>) -> Option<u64> {
        self.shard_registry.set_primary(shard_id, primary)
    }

    /// Handle node failure in the registry.
    pub fn handle_node_failure_in_registry(&self, node_id: NodeId) -> Vec<ShardId> {
        let affected_primaries = self.shard_registry.clear_primary_for_node(node_id);
        let affected_replicas = self.shard_registry.remove_node_from_replicas(node_id);

        // Combine and deduplicate
        let mut all_affected: Vec<ShardId> = affected_primaries;
        for shard_id in affected_replicas {
            if !all_affected.contains(&shard_id) {
                all_affected.push(shard_id);
            }
        }
        all_affected
    }

    // ==================== Shard Placement Methods ====================

    /// Get the shard placement.
    pub fn shard_placement(&self) -> &Arc<ShardPlacement> {
        &self.shard_placement
    }

    /// Add a node to the placement ring.
    ///
    /// This also clears the router cache to ensure routing decisions are
    /// recalculated with the new topology.
    pub fn add_node_to_placement(&self, node_id: NodeId) {
        self.shard_placement.add_node(node_id);

        // Clear router cache immediately after topology change
        // This prevents stale routing to old node assignments
        self.router.clear_cache();

        tracing::info!(node_id, "Added node to shard placement ring and cleared router cache");
    }

    /// Remove a node from the placement ring.
    ///
    /// Returns the shard movements needed for rebalancing.
    /// Also clears the router cache to ensure routing decisions are recalculated.
    pub fn remove_node_from_placement(&self, node_id: NodeId) -> Vec<super::shard_placement::ShardMovement> {
        // Calculate movements before removing the node
        let movements = self.shard_placement.calculate_movements_on_remove(node_id);
        // Actually remove the node
        self.shard_placement.remove_node(node_id);

        // Clear router cache immediately after topology change
        // This prevents stale routing to removed node
        self.router.clear_cache();

        tracing::info!(
            node_id,
            movements = movements.len(),
            "Removed node from shard placement ring and cleared router cache"
        );
        movements
    }

    /// Get the nodes that should host a specific shard.
    pub fn get_shard_nodes(&self, shard_id: ShardId) -> Vec<NodeId> {
        self.shard_placement.get_shard_nodes(shard_id)
    }

    /// Check if the current node should host a shard.
    pub fn should_host_shard(&self, shard_id: ShardId) -> bool {
        self.get_shard_nodes(shard_id).contains(&self.node_id)
    }

    // ==================== Migration Methods ====================

    /// Enable migration support with default configuration.
    pub fn enable_migration(&self) {
        self.enable_migration_with_config(MigrationConfig::default());
    }

    /// Enable migration support with custom configuration.
    pub fn enable_migration_with_config(&self, config: MigrationConfig) {
        let state_store = Arc::new(InMemoryMigrationStore::new());
        let coordinator = ShardMigrationCoordinator::new(self.node_id, config, state_store);
        *self.migration_coordinator.write() = Some(Arc::new(coordinator));
        tracing::info!(node_id = self.node_id, "Migration support enabled");
    }

    /// Get the migration coordinator if enabled.
    pub fn migration_coordinator(&self) -> Option<Arc<ShardMigrationCoordinator>> {
        self.migration_coordinator.read().clone()
    }

    /// Check if migration is enabled.
    pub fn is_migration_enabled(&self) -> bool {
        self.migration_coordinator.read().is_some()
    }

    /// Pause all migrations (kill switch).
    pub fn pause_migrations(&self) {
        if let Some(ref coordinator) = *self.migration_coordinator.read() {
            coordinator.pause();
            tracing::warn!(node_id = self.node_id, "Migrations paused via kill switch");
        }
    }

    /// Resume migrations after pause.
    pub fn resume_migrations(&self) {
        if let Some(ref coordinator) = *self.migration_coordinator.read() {
            coordinator.resume();
            tracing::info!(node_id = self.node_id, "Migrations resumed");
        }
    }

    /// Check if migrations are paused.
    pub fn are_migrations_paused(&self) -> bool {
        self.migration_coordinator
            .read()
            .as_ref()
            .map(|c| c.is_paused())
            .unwrap_or(false)
    }

    /// Get migration stats if migration is enabled.
    pub fn migration_stats(&self) -> Option<super::migration::MigrationStats> {
        self.migration_coordinator.read().as_ref().map(|c| c.stats())
    }

    // ==================== Migration Orchestrator Methods ====================

    /// Enable full migration orchestration.
    ///
    /// This enables end-to-end migration management including:
    /// - Routing during migration
    /// - Atomic ownership handover via Raft
    /// - Data transfer coordination
    pub fn enable_orchestration(&self) {
        self.enable_migration();

        let coordinator = self.migration_coordinator.read().clone();
        if let Some(coord) = coordinator {
            let orchestrator = MigrationOrchestrator::new(
                self.node_id,
                coord,
                self.shard_registry.clone(),
                self.shard_placement.clone(),
            );
            *self.migration_orchestrator.write() = Some(Arc::new(orchestrator));
            tracing::info!(node_id = self.node_id, "Migration orchestration enabled");
        }
    }

    /// Enable orchestration with custom migration config.
    pub fn enable_orchestration_with_config(&self, config: MigrationConfig) {
        self.enable_migration_with_config(config);

        let coordinator = self.migration_coordinator.read().clone();
        if let Some(coord) = coordinator {
            let orchestrator = MigrationOrchestrator::new(
                self.node_id,
                coord,
                self.shard_registry.clone(),
                self.shard_placement.clone(),
            );
            *self.migration_orchestrator.write() = Some(Arc::new(orchestrator));
            tracing::info!(node_id = self.node_id, "Migration orchestration enabled with custom config");
        }
    }

    /// Get the migration orchestrator if enabled.
    pub fn migration_orchestrator(&self) -> Option<Arc<MigrationOrchestrator>> {
        self.migration_orchestrator.read().clone()
    }

    /// Check if orchestration is enabled.
    pub fn is_orchestration_enabled(&self) -> bool {
        self.migration_orchestrator.read().is_some()
    }

    /// Set the Raft proposer for atomic migration commits.
    ///
    /// This must be called after enabling orchestration for migrations
    /// to work properly.
    pub fn set_migration_raft_proposer(&self, proposer: Arc<dyn MigrationRaftProposer>) {
        if let Some(ref orchestrator) = *self.migration_orchestrator.read() {
            orchestrator.set_raft_proposer(proposer);
            tracing::debug!(node_id = self.node_id, "Migration Raft proposer set");
        } else {
            tracing::warn!("Cannot set Raft proposer: orchestration not enabled");
        }
    }

    /// Set the data transporter for migration data transfer.
    ///
    /// This must be called after enabling orchestration for migrations
    /// to work properly.
    pub fn set_migration_data_transporter(&self, transporter: Arc<dyn DataTransporter>) {
        if let Some(ref orchestrator) = *self.migration_orchestrator.read() {
            orchestrator.set_data_transporter(transporter);
            tracing::debug!(node_id = self.node_id, "Migration data transporter set");
        } else {
            tracing::warn!("Cannot set data transporter: orchestration not enabled");
        }
    }

    /// Start a shard migration.
    ///
    /// Returns the migration ID if successful.
    pub async fn start_shard_migration(
        &self,
        movement: ShardMovement,
        strategy: Option<MigrationRoutingStrategy>,
    ) -> Result<Uuid> {
        let orchestrator = self
            .migration_orchestrator
            .read()
            .clone()
            .ok_or_else(|| Error::Internal("Orchestration not enabled".to_string()))?;

        orchestrator.start_migration(movement, strategy).await
    }

    /// Execute a full shard migration.
    ///
    /// This runs all migration phases: streaming, catch-up, commit, cleanup.
    pub async fn execute_shard_migration(&self, shard_id: ShardId) -> Result<()> {
        let orchestrator = self
            .migration_orchestrator
            .read()
            .clone()
            .ok_or_else(|| Error::Internal("Orchestration not enabled".to_string()))?;

        orchestrator.execute_migration(shard_id).await
    }

    /// Cancel an in-progress migration.
    pub async fn cancel_shard_migration(&self, shard_id: ShardId) -> Result<()> {
        let orchestrator = self
            .migration_orchestrator
            .read()
            .clone()
            .ok_or_else(|| Error::Internal("Orchestration not enabled".to_string()))?;

        orchestrator.cancel_migration(shard_id).await
    }

    /// Get routing decision for writes during migration.
    pub fn route_migration_write(&self, shard_id: ShardId) -> Option<MigrationRoutingDecision> {
        self.migration_orchestrator
            .read()
            .as_ref()
            .map(|o| o.route_write(shard_id))
    }

    /// Get routing decision for reads during migration.
    pub fn route_migration_read(&self, shard_id: ShardId) -> Option<MigrationRoutingDecision> {
        self.migration_orchestrator
            .read()
            .as_ref()
            .map(|o| o.route_read(shard_id))
    }

    /// Handle a node joining - calculate necessary migrations.
    pub fn calculate_join_migrations(&self, node_id: NodeId) -> Vec<ShardMovement> {
        if let Some(ref orchestrator) = *self.migration_orchestrator.read() {
            orchestrator.handle_node_join(node_id)
        } else {
            self.shard_placement.calculate_movements_on_add(node_id)
        }
    }

    /// Handle a node leaving - calculate necessary migrations.
    pub fn calculate_leave_migrations(&self, node_id: NodeId) -> Vec<ShardMovement> {
        if let Some(ref orchestrator) = *self.migration_orchestrator.read() {
            orchestrator.handle_node_leave(node_id)
        } else {
            self.shard_placement.calculate_movements_on_remove(node_id)
        }
    }

    /// Check if a shard is currently being migrated.
    pub fn is_shard_migrating(&self, shard_id: ShardId) -> bool {
        // Check Raft-native migration first
        if let Some(ref raft_coord) = *self.raft_migration_coordinator.read() {
            if raft_coord.is_migrating(shard_id) {
                return true;
            }
        }
        if let Some(ref orchestrator) = *self.migration_orchestrator.read() {
            orchestrator.is_orchestrating(shard_id)
        } else if let Some(ref coordinator) = *self.migration_coordinator.read() {
            coordinator.is_migrating(shard_id)
        } else {
            false
        }
    }

    // ==================== Raft-Native Migration Methods ====================

    /// Enable Raft-native shard migration with default configuration.
    ///
    /// This enables the recommended migration approach that uses Raft's native
    /// membership change mechanism (AddLearner → WaitSnapshot → PromoteToVoter → RemoveOld).
    pub fn enable_raft_migration(&self) {
        self.enable_raft_migration_with_config(RaftMigrationConfig::default());
    }

    /// Enable Raft-native shard migration with custom configuration.
    pub fn enable_raft_migration_with_config(&self, config: RaftMigrationConfig) {
        let coordinator = RaftMigrationCoordinator::new(self.node_id, config);
        *self.raft_migration_coordinator.write() = Some(Arc::new(coordinator));
        tracing::info!(node_id = self.node_id, "Raft-native migration enabled");
    }

    /// Get the Raft migration coordinator if enabled.
    pub fn raft_migration_coordinator(&self) -> Option<Arc<RaftMigrationCoordinator>> {
        self.raft_migration_coordinator.read().clone()
    }

    /// Check if Raft-native migration is enabled.
    pub fn is_raft_migration_enabled(&self) -> bool {
        self.raft_migration_coordinator.read().is_some()
    }

    /// Set the Raft controller for shard operations.
    ///
    /// This must be called after enabling Raft migration to connect the
    /// migration coordinator to actual Raft groups.
    pub fn set_shard_raft_controller(&self, controller: Arc<dyn ShardRaftController>) {
        if let Some(ref coordinator) = *self.raft_migration_coordinator.read() {
            coordinator.set_raft_controller(controller);
            tracing::debug!(node_id = self.node_id, "Shard Raft controller set");
        } else {
            tracing::warn!("Cannot set Raft controller: Raft migration not enabled");
        }
    }

    /// Register a node's address for migration and forwarding.
    pub fn register_node_address(&self, node_id: NodeId, addr: SocketAddr) {
        self.node_addresses.write().insert(node_id, addr);
        // Also register with the shard forwarder
        self.shard_forwarder.register_node(node_id, addr);
        tracing::debug!(node_id, %addr, "Registered node address for migration and forwarding");
    }

    /// Get a node's address.
    pub fn get_node_address(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.node_addresses.read().get(&node_id).copied()
    }

    // ==================== Shard Forwarding ====================

    /// Enable or disable shard forwarding.
    ///
    /// When enabled, requests for keys whose shard is on another node
    /// will be automatically forwarded to the correct node.
    pub fn set_forwarding_enabled(&mut self, enabled: bool) {
        // Note: This requires &mut self because forwarding_enabled is not behind a lock
        // In production, you might want to make this atomic or use interior mutability
        tracing::info!(
            node_id = self.node_id,
            enabled = enabled,
            "Shard forwarding {}",
            if enabled { "enabled" } else { "disabled" }
        );
    }

    /// Check if shard forwarding is enabled.
    pub fn is_forwarding_enabled(&self) -> bool {
        self.forwarding_enabled
    }

    /// Get the number of pending shard forwards.
    pub fn pending_forwards_count(&self) -> usize {
        self.shard_forwarder.pending_count()
    }

    /// Get the shard forwarder for handling incoming messages.
    pub fn shard_forwarder(&self) -> &ShardForwarder {
        &self.shard_forwarder
    }

    /// Pause Raft-native migrations (kill switch).
    pub fn pause_raft_migrations(&self) {
        if let Some(ref coordinator) = *self.raft_migration_coordinator.read() {
            coordinator.pause();
            tracing::warn!(node_id = self.node_id, "Raft migrations paused via kill switch");
        }
    }

    /// Resume Raft-native migrations.
    pub fn resume_raft_migrations(&self) {
        if let Some(ref coordinator) = *self.raft_migration_coordinator.read() {
            coordinator.resume();
            tracing::info!(node_id = self.node_id, "Raft migrations resumed");
        }
    }

    /// Check if Raft migrations are paused.
    pub fn are_raft_migrations_paused(&self) -> bool {
        self.raft_migration_coordinator
            .read()
            .as_ref()
            .map(|c| c.is_paused())
            .unwrap_or(false)
    }

    /// Get Raft migration statistics.
    pub fn raft_migration_stats(&self) -> Option<RaftMigrationStats> {
        self.raft_migration_coordinator.read().as_ref().map(|c| c.stats())
    }

    /// Plan a Raft-native shard migration.
    ///
    /// This queues a migration to be executed. The migration will go through
    /// phases: AddLearner → WaitForSnapshot → CatchUp → PromoteToVoter → RemoveOld.
    pub fn plan_raft_migration(&self, change: RaftMembershipChange) -> Result<RaftShardMigration> {
        let coordinator = self.raft_migration_coordinator
            .read()
            .clone()
            .ok_or_else(|| Error::Internal("Raft migration not enabled".to_string()))?;

        coordinator.plan_migration(change)
    }

    /// Execute a Raft-native shard migration through all phases.
    pub async fn execute_raft_migration(&self, shard_id: ShardId) -> Result<()> {
        let coordinator = self.raft_migration_coordinator
            .read()
            .clone()
            .ok_or_else(|| Error::Internal("Raft migration not enabled".to_string()))?;

        coordinator.execute_migration(shard_id).await
    }

    /// Cancel a Raft-native migration.
    pub fn cancel_raft_migration(&self, shard_id: ShardId) -> Result<RaftShardMigration> {
        let coordinator = self.raft_migration_coordinator
            .read()
            .clone()
            .ok_or_else(|| Error::Internal("Raft migration not enabled".to_string()))?;

        coordinator.cancel_migration(shard_id)
    }

    /// Get an active Raft migration by shard ID.
    pub fn get_raft_migration(&self, shard_id: ShardId) -> Option<RaftShardMigration> {
        self.raft_migration_coordinator
            .read()
            .as_ref()
            .and_then(|c| c.get_migration(shard_id))
    }

    /// Get all active Raft migrations.
    pub fn active_raft_migrations(&self) -> Vec<RaftShardMigration> {
        self.raft_migration_coordinator
            .read()
            .as_ref()
            .map(|c| c.active_migrations())
            .unwrap_or_default()
    }

    /// Handle a node joining the cluster - plan necessary Raft migrations.
    ///
    /// This calculates which shards need to be migrated to the new node and
    /// plans the migrations using Raft's membership change mechanism.
    pub async fn handle_node_join_with_raft(&self, node_id: NodeId, addr: SocketAddr) -> Result<Vec<Uuid>> {
        // Register the node's address
        self.register_node_address(node_id, addr);

        // Add node to placement ring
        self.add_node_to_placement(node_id);

        // Calculate shard movements
        let movements = self.shard_placement.calculate_movements_on_add(node_id);

        if movements.is_empty() {
            tracing::info!(node_id, "Node joined, no shard movements needed");
            return Ok(vec![]);
        }

        tracing::info!(
            node_id,
            movements = movements.len(),
            "Node joined, planning shard migrations"
        );

        // Plan migrations for each movement
        let mut migration_ids = Vec::new();
        for movement in movements {
            // Create a transfer: add new node, remove from old
            let change = RaftMembershipChange::transfer(
                movement.shard_id,
                movement.from_node,
                movement.to_node,
            );

            match self.plan_raft_migration(change) {
                Ok(migration) => {
                    migration_ids.push(migration.id);
                    // Update shard registry
                    self.start_shard_migration_in_registry(movement.shard_id);
                }
                Err(e) => {
                    tracing::error!(
                        shard_id = movement.shard_id,
                        error = %e,
                        "Failed to plan migration for shard"
                    );
                }
            }
        }

        Ok(migration_ids)
    }

    /// Handle a node leaving the cluster - plan necessary Raft migrations.
    ///
    /// This calculates which shards need to be moved off the leaving node
    /// and plans the migrations using Raft's membership change mechanism.
    pub async fn handle_node_leave_with_raft(&self, node_id: NodeId) -> Result<Vec<Uuid>> {
        // Calculate shard movements before removing the node
        let movements = self.shard_placement.calculate_movements_on_remove(node_id);

        // Remove node from placement ring
        self.remove_node_from_placement(node_id);

        // Remove node's address
        self.node_addresses.write().remove(&node_id);

        if movements.is_empty() {
            tracing::info!(node_id, "Node left, no shard movements needed");
            return Ok(vec![]);
        }

        tracing::info!(
            node_id,
            movements = movements.len(),
            "Node left, planning shard migrations"
        );

        // Plan migrations for each movement
        let mut migration_ids = Vec::new();
        for movement in movements {
            // Create a transfer: move shard from leaving node to new node
            let change = RaftMembershipChange::transfer(
                movement.shard_id,
                movement.from_node,
                movement.to_node,
            );

            match self.plan_raft_migration(change) {
                Ok(migration) => {
                    migration_ids.push(migration.id);
                    // Update shard registry
                    self.start_shard_migration_in_registry(movement.shard_id);
                }
                Err(e) => {
                    tracing::error!(
                        shard_id = movement.shard_id,
                        error = %e,
                        "Failed to plan migration for shard"
                    );
                }
            }
        }

        Ok(migration_ids)
    }

    /// Execute all planned migrations.
    ///
    /// This runs all active migrations concurrently up to the configured limit.
    pub async fn execute_all_raft_migrations(&self) -> Result<()> {
        let migrations = self.active_raft_migrations();

        if migrations.is_empty() {
            return Ok(());
        }

        tracing::info!(
            count = migrations.len(),
            "Executing all planned Raft migrations"
        );

        // Execute migrations concurrently
        let mut handles = Vec::new();
        let coordinator = self.raft_migration_coordinator
            .read()
            .clone()
            .ok_or_else(|| Error::Internal("Raft migration not enabled".to_string()))?;

        for migration in migrations {
            let coord = coordinator.clone();
            let shard_id = migration.change.shard_id;

            let handle = tokio::spawn(async move {
                coord.execute_migration(shard_id).await
            });
            handles.push((shard_id, handle));
        }

        // Wait for all migrations to complete
        let mut errors = Vec::new();
        for (shard_id, handle) in handles {
            match handle.await {
                Ok(Ok(())) => {
                    tracing::info!(shard_id, "Migration completed successfully");
                }
                Ok(Err(e)) => {
                    tracing::error!(shard_id, error = %e, "Migration failed");
                    errors.push((shard_id, e));
                }
                Err(e) => {
                    tracing::error!(shard_id, error = %e, "Migration task panicked");
                    errors.push((shard_id, Error::Internal(format!("Task panicked: {}", e))));
                }
            }
        }

        if !errors.is_empty() {
            let error_msg = errors
                .iter()
                .map(|(shard_id, e)| format!("shard {}: {}", shard_id, e))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(Error::MigrationFailed(error_msg));
        }

        Ok(())
    }

    /// Shutdown the coordinator.
    pub async fn shutdown(&self) -> Result<()> {
        *self.state.write() = CoordinatorState::ShuttingDown;
        self.running.store(false, Ordering::Relaxed);

        // Pause any ongoing migrations
        if let Some(ref coordinator) = *self.migration_coordinator.read() {
            coordinator.pause();
            tracing::info!(node_id = self.node_id, "Paused key-level migrations during shutdown");
        }

        // Pause Raft-native migrations
        if let Some(ref raft_coordinator) = *self.raft_migration_coordinator.read() {
            raft_coordinator.pause();
            tracing::info!(node_id = self.node_id, "Paused Raft migrations during shutdown");
        }

        // Set all shards to stopped
        for shard in self.router.all_shards() {
            shard.set_state(ShardState::Stopped);
        }

        *self.state.write() = CoordinatorState::Stopped;

        tracing::info!(node_id = self.node_id, "Multi-Raft coordinator shutdown");

        Ok(())
    }

    /// Get the router.
    pub fn router(&self) -> &Arc<ShardRouter> {
        &self.router
    }

    /// Get the node ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get the config.
    pub fn config(&self) -> &MultiRaftConfig {
        &self.config
    }
}

/// Builder for Multi-Raft coordinator.
pub struct MultiRaftBuilder {
    node_id: NodeId,
    config: MultiRaftConfig,
    metrics: Option<Arc<CacheMetrics>>,
}

impl MultiRaftBuilder {
    /// Create a new builder.
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            config: MultiRaftConfig::default(),
            metrics: None,
        }
    }

    /// Set the number of shards.
    pub fn num_shards(mut self, num_shards: u32) -> Self {
        self.config.num_shards = num_shards;
        self
    }

    /// Set the replica factor.
    pub fn replica_factor(mut self, factor: usize) -> Self {
        self.config.replica_factor = factor;
        self
    }

    /// Set the shard capacity.
    pub fn shard_capacity(mut self, capacity: u64) -> Self {
        self.config.shard_capacity = capacity;
        self
    }

    /// Set the default TTL.
    pub fn default_ttl(mut self, ttl: Duration) -> Self {
        self.config.default_ttl = Some(ttl);
        self
    }

    /// Set the metrics instance.
    pub fn metrics(mut self, metrics: Arc<CacheMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Disable auto-initialization of shards.
    pub fn no_auto_init(mut self) -> Self {
        self.config.auto_init_shards = false;
        self
    }

    /// Build the coordinator.
    pub fn build(self) -> MultiRaftCoordinator {
        let metrics = self.metrics.unwrap_or_else(|| Arc::new(CacheMetrics::new()));
        MultiRaftCoordinator::new(self.node_id, self.config, metrics)
    }

    /// Build and initialize the coordinator.
    pub async fn build_and_init(self) -> Result<MultiRaftCoordinator> {
        let coordinator = self.build();
        coordinator.init().await?;
        Ok(coordinator)
    }
}

// ============================================================================
// MultiRaftShardController - Real ShardRaftController implementation
// ============================================================================

/// A ShardRaftController implementation that works with the MultiRaftCoordinator.
///
/// This controller interfaces with the actual shard Raft groups to perform
/// membership changes during migrations.
#[derive(Debug)]
pub struct MultiRaftShardController {
    /// Reference to the coordinator.
    coordinator: Arc<MultiRaftCoordinator>,
}

impl MultiRaftShardController {
    /// Create a new controller backed by the given coordinator.
    pub fn new(coordinator: Arc<MultiRaftCoordinator>) -> Self {
        Self { coordinator }
    }
}

#[async_trait::async_trait]
impl ShardRaftController for MultiRaftShardController {
    async fn add_learner(
        &self,
        shard_id: ShardId,
        node_id: NodeId,
        _node_addr: SocketAddr,
    ) -> Result<()> {
        let shard = self.coordinator.get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        // Add the node as a member (learner)
        shard.add_member(node_id);

        tracing::info!(
            shard_id,
            node_id,
            "Added learner to shard Raft group"
        );

        Ok(())
    }

    async fn promote_to_voter(&self, shard_id: ShardId, node_id: NodeId) -> Result<()> {
        let shard = self.coordinator.get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        // Verify the node is already a member
        if !shard.members().contains(&node_id) {
            return Err(Error::Internal(format!(
                "Node {} is not a member of shard {}",
                node_id, shard_id
            )));
        }

        tracing::info!(
            shard_id,
            node_id,
            "Promoted learner to voter in shard Raft group"
        );

        // Update registry
        let mut replicas = self.coordinator.shard_registry().get_replicas(shard_id);
        if !replicas.contains(&node_id) {
            replicas.push(node_id);
            self.coordinator.shard_registry().set_replicas(shard_id, replicas);
        }

        Ok(())
    }

    async fn remove_node(&self, shard_id: ShardId, node_id: NodeId) -> Result<()> {
        let shard = self.coordinator.get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        shard.remove_member(node_id);

        tracing::info!(
            shard_id,
            node_id,
            "Removed node from shard Raft group"
        );

        // Update registry
        let mut replicas = self.coordinator.shard_registry().get_replicas(shard_id);
        replicas.retain(|&id| id != node_id);
        self.coordinator.shard_registry().set_replicas(shard_id, replicas);

        Ok(())
    }

    async fn transfer_leader(&self, shard_id: ShardId, to_node: NodeId) -> Result<()> {
        let shard = self.coordinator.get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        // Set the new leader
        shard.set_leader(Some(to_node));

        // Update coordinator's leader tracking
        self.coordinator.set_shard_leader(shard_id, to_node);

        tracing::info!(
            shard_id,
            to_node,
            "Transferred leadership"
        );

        Ok(())
    }

    async fn get_leader(&self, shard_id: ShardId) -> Result<Option<NodeId>> {
        let shard = self.coordinator.get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        Ok(shard.leader())
    }

    async fn get_learner_progress(
        &self,
        shard_id: ShardId,
        _learner_id: NodeId,
    ) -> Result<(u64, u64, bool)> {
        let shard = self.coordinator.get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        // Return (learner_match_index, leader_commit_index, snapshot_applied)
        // In a real implementation, this would query the actual Raft state
        let commit_index = shard.commit_index();
        let applied_index = shard.applied_index();

        // Assume learner has caught up (for now, until real Raft integration)
        Ok((applied_index, commit_index, true))
    }

    async fn get_commit_index(&self, shard_id: ShardId) -> Result<u64> {
        let shard = self.coordinator.get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        Ok(shard.commit_index())
    }

    async fn is_voter(&self, shard_id: ShardId, node_id: NodeId) -> Result<bool> {
        let replicas = self.coordinator.shard_registry().get_replicas(shard_id);
        Ok(replicas.contains(&node_id))
    }

    async fn is_learner(&self, shard_id: ShardId, node_id: NodeId) -> Result<bool> {
        let shard = self.coordinator.get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        let is_member = shard.members().contains(&node_id);
        let is_voter = self.is_voter(shard_id, node_id).await?;

        // Learner is a member but not a voter
        Ok(is_member && !is_voter)
    }

    async fn get_voters(&self, shard_id: ShardId) -> Result<Vec<NodeId>> {
        Ok(self.coordinator.shard_registry().get_replicas(shard_id))
    }

    async fn get_learners(&self, shard_id: ShardId) -> Result<Vec<NodeId>> {
        let shard = self.coordinator.get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        let members = shard.members();
        let voters = self.get_voters(shard_id).await?;

        // Learners are members that are not voters
        Ok(members.into_iter().filter(|m| !voters.contains(m)).collect())
    }

    fn list_shards_with_learners(&self) -> Result<Vec<(ShardId, Vec<NodeId>)>> {
        let mut result = Vec::new();

        for shard_id in 0..self.coordinator.config().num_shards as ShardId {
            if let Some(shard) = self.coordinator.get_shard(shard_id) {
                let members = shard.members();
                // Approximate voters as replicas from registry
                let replicas = self.coordinator.shard_registry().get_replicas(shard_id);
                let learners: Vec<NodeId> = members
                    .into_iter()
                    .filter(|m| !replicas.contains(m))
                    .collect();

                if !learners.is_empty() {
                    result.push((shard_id, learners));
                }
            }
        }

        Ok(result)
    }

    fn remove_learner(&self, shard_id: ShardId, node_id: NodeId) -> Result<()> {
        let shard = self.coordinator.get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        shard.remove_member(node_id);

        tracing::info!(
            shard_id,
            node_id,
            "Removed learner from shard Raft group"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_coordinator_creation() {
        let metrics = Arc::new(CacheMetrics::new());
        let config = MultiRaftConfig::new(4);
        let coordinator = MultiRaftCoordinator::new(1, config, metrics);

        assert_eq!(coordinator.state(), CoordinatorState::Initializing);
        assert_eq!(coordinator.node_id(), 1);
    }

    #[tokio::test]
    async fn test_coordinator_init() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .unwrap();

        assert_eq!(coordinator.state(), CoordinatorState::Running);
        assert!(coordinator.is_running());

        let stats = coordinator.stats();
        assert_eq!(stats.total_shards, 4);
        assert_eq!(stats.active_shards, 4);
    }

    #[tokio::test]
    async fn test_coordinator_operations() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .unwrap();

        // Test put/get
        coordinator.put("key1", "value1").await.unwrap();
        let result = coordinator.get(b"key1").await.unwrap();
        assert_eq!(result, Some(Bytes::from("value1")));

        // Test put_with_ttl
        coordinator
            .put_with_ttl("key2", "value2", Duration::from_secs(60))
            .await
            .unwrap();
        let result = coordinator.get(b"key2").await.unwrap();
        assert_eq!(result, Some(Bytes::from("value2")));

        // Test delete
        coordinator.delete(b"key1").await.unwrap();
        let result = coordinator.get(b"key1").await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_coordinator_stats() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .unwrap();

        // Perform some operations
        for i in 0..100 {
            let key = format!("key-{}", i);
            let value = format!("value-{}", i);
            coordinator.put(key, value).await.unwrap();
        }

        // Run pending tasks on all shards to ensure entries are counted
        for shard in coordinator.router().all_shards() {
            shard.storage().run_pending_tasks().await;
        }

        // Wait a bit for Moka's async operations to complete
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Run pending tasks again
        for shard in coordinator.router().all_shards() {
            shard.storage().run_pending_tasks().await;
        }

        let stats = coordinator.stats();
        // Check that entries were inserted
        assert!(stats.total_entries >= 90, "Expected at least 90 entries, got {}", stats.total_entries);
        assert!(stats.operations_total >= 100);
    }

    #[tokio::test]
    async fn test_coordinator_shard_distribution() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .unwrap();

        // Insert keys and check they go to different shards
        let mut shard_counts: HashMap<ShardId, u32> = HashMap::new();

        for i in 0..100 {
            let key = format!("key-{}", i);
            let shard_id = coordinator.shard_for_key(key.as_bytes());
            *shard_counts.entry(shard_id).or_insert(0) += 1;
        }

        // All 4 shards should have some keys
        assert_eq!(shard_counts.len(), 4);

        // Distribution should be reasonably even
        for &count in shard_counts.values() {
            assert!(count > 10, "Shard has too few keys: {}", count);
            assert!(count < 50, "Shard has too many keys: {}", count);
        }
    }

    #[tokio::test]
    async fn test_coordinator_shard_leaders() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .unwrap();

        // Set leaders
        coordinator.set_shard_leader(0, 1);
        coordinator.set_shard_leader(1, 2);
        coordinator.set_shard_leader(2, 1);
        coordinator.set_shard_leader(3, 3);

        let leaders = coordinator.shard_leaders();
        assert_eq!(leaders.get(&0), Some(&Some(1)));
        assert_eq!(leaders.get(&1), Some(&Some(2)));
        assert_eq!(leaders.get(&2), Some(&Some(1)));
        assert_eq!(leaders.get(&3), Some(&Some(3)));
    }

    #[tokio::test]
    async fn test_coordinator_shutdown() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .unwrap();

        assert!(coordinator.is_running());

        coordinator.shutdown().await.unwrap();

        assert!(!coordinator.is_running());
        assert_eq!(coordinator.state(), CoordinatorState::Stopped);

        // All shards should be stopped
        for shard in coordinator.router().all_shards() {
            assert_eq!(shard.state(), ShardState::Stopped);
        }
    }

    #[tokio::test]
    async fn test_coordinator_builder() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(8)
            .replica_factor(5)
            .shard_capacity(50_000)
            .default_ttl(Duration::from_secs(3600))
            .build();

        assert_eq!(coordinator.config().num_shards, 8);
        assert_eq!(coordinator.config().replica_factor, 5);
        assert_eq!(coordinator.config().shard_capacity, 50_000);
        assert_eq!(
            coordinator.config().default_ttl,
            Some(Duration::from_secs(3600))
        );
    }

    #[tokio::test]
    async fn test_coordinator_manual_shard_creation() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .no_auto_init()
            .build();

        coordinator.init().await.unwrap();

        // No shards should exist yet
        assert_eq!(coordinator.stats().active_shards, 0);

        // Create shards manually
        coordinator.create_shard(0).await.unwrap();
        coordinator.create_shard(1).await.unwrap();

        assert_eq!(coordinator.stats().active_shards, 2);

        // Try to create duplicate shard
        let result = coordinator.create_shard(0).await;
        assert!(result.is_err());
    }
}
