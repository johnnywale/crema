//! Multi-Raft coordinator for managing multiple Raft groups.
//!
//! The coordinator is responsible for:
//! - Creating and managing shards
//! - Routing requests to the appropriate shard
//! - Handling shard rebalancing
//! - Coordinating leader elections across shards

use super::memberlist_integration::{ShardLeaderBroadcaster, ShardLeaderTracker};
use super::router::{RouterConfig, ShardRouter};
use super::shard::{Shard, ShardConfig, ShardId, ShardInfo, ShardState};
use crate::error::{Error, Result};
use crate::metrics::CacheMetrics;
use crate::types::NodeId;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
}

impl MultiRaftCoordinator {
    /// Create a new Multi-Raft coordinator.
    pub fn new(node_id: NodeId, config: MultiRaftConfig, metrics: Arc<CacheMetrics>) -> Self {
        let router_config = RouterConfig::new(config.num_shards);
        let router = Arc::new(ShardRouter::new(router_config));
        let leader_tracker = ShardLeaderTracker::new(node_id);
        let leader_broadcaster = ShardLeaderBroadcaster::new(config.tick_interval);

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
    pub async fn create_shard(&self, shard_id: ShardId) -> Result<Arc<Shard>> {
        // Check if shard already exists
        if self.router.get_shard(shard_id).is_some() {
            return Err(Error::ShardAlreadyExists(shard_id));
        }

        // Create shard config
        let shard_config = ShardConfig::new(shard_id, self.config.num_shards)
            .with_replicas(self.config.replica_factor)
            .with_max_capacity(self.config.shard_capacity);

        // Create the shard
        let shard = Arc::new(Shard::new(shard_config.clone()).await?);

        // Set shard state to active
        shard.set_state(ShardState::Active);

        // Register with router
        self.router.register_shard(shard.clone());

        // Store config
        self.shard_configs.write().insert(shard_id, shard_config);

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
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.operations_total.fetch_add(1, Ordering::Relaxed);

        let start = Instant::now();
        let result = self.router.get(key).await;
        let hit = result.as_ref().map(|r| r.is_some()).unwrap_or(false);

        self.metrics.record_get(hit, start.elapsed());

        result
    }

    /// Put a value in the cache.
    pub async fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        self.operations_total.fetch_add(1, Ordering::Relaxed);

        let start = Instant::now();
        let result = self.router.put(key.into(), value.into()).await;
        let success = result.is_ok();

        self.metrics.record_put(success, start.elapsed());

        result.map(|_| ())
    }

    /// Put a value with TTL.
    pub async fn put_with_ttl(
        &self,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
        ttl: Duration,
    ) -> Result<()> {
        self.operations_total.fetch_add(1, Ordering::Relaxed);

        let start = Instant::now();
        let result = self.router.put_with_ttl(key.into(), value.into(), ttl).await;
        let success = result.is_ok();

        self.metrics.record_put(success, start.elapsed());

        result.map(|_| ())
    }

    /// Delete a key from the cache.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.operations_total.fetch_add(1, Ordering::Relaxed);

        let start = Instant::now();
        let result = self.router.delete(key).await;

        self.metrics.record_delete(start.elapsed());

        result.map(|_| ())
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

    /// Shutdown the coordinator.
    pub async fn shutdown(&self) -> Result<()> {
        *self.state.write() = CoordinatorState::ShuttingDown;
        self.running.store(false, Ordering::Relaxed);

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
