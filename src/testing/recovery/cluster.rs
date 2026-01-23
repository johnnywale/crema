//! Recovery test cluster for testing crash and recovery scenarios.
//!
//! This module provides a test cluster that supports:
//! - Starting/stopping nodes gracefully
//! - Simulating crashes (non-graceful shutdown)
//! - Recovery from persistent state
//! - Consistency verification

use crate::cache::DistributedCache;
use crate::checkpoint::CheckpointConfig;
use crate::config::{CacheConfig, MemberlistConfig, RaftConfig, RaftStorageType};
use crate::error::{Error, Result};
use crate::testing::recovery::checker::ConsistencyChecker;
use crate::testing::recovery::failpoint::FailpointRegistry;
use crate::testing::utils::allocate_os_ports_with_memberlist;
use crate::types::NodeId;

use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Configuration for a recovery test cluster.
#[derive(Debug, Clone)]
pub struct RecoveryTestConfig {
    /// Number of nodes in the cluster.
    pub node_count: usize,

    /// Whether to use persistent storage (RocksDB).
    pub persistent: bool,

    /// Base timeout for operations.
    pub timeout: Duration,

    /// Whether to enable checkpointing.
    pub checkpointing: bool,

    /// Log threshold for checkpointing.
    pub checkpoint_log_threshold: u64,

    /// Maximum snapshots to keep.
    pub checkpoint_max_snapshots: usize,
}

impl Default for RecoveryTestConfig {
    fn default() -> Self {
        Self {
            node_count: 3,
            persistent: false,
            timeout: Duration::from_secs(10),
            checkpointing: true,
            checkpoint_log_threshold: 100,
            checkpoint_max_snapshots: 3,
        }
    }
}

impl RecoveryTestConfig {
    /// Create a new config with the given node count.
    pub fn new(node_count: usize) -> Self {
        Self {
            node_count,
            ..Default::default()
        }
    }

    /// Enable persistent storage.
    pub fn with_persistent(mut self, persistent: bool) -> Self {
        self.persistent = persistent;
        self
    }

    /// Set the timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Enable/disable checkpointing.
    pub fn with_checkpointing(mut self, enabled: bool) -> Self {
        self.checkpointing = enabled;
        self
    }

    /// Set checkpoint log threshold.
    pub fn with_checkpoint_log_threshold(mut self, threshold: u64) -> Self {
        self.checkpoint_log_threshold = threshold;
        self
    }
}

/// State of a node in the test cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    /// Node is running normally.
    Running,

    /// Node was stopped gracefully.
    Stopped,

    /// Node crashed (simulated).
    Crashed,

    /// Node has not been started yet.
    NotStarted,
}

/// A node in the recovery test cluster.
struct RecoveryTestNode {
    /// The distributed cache instance (if running).
    cache: Option<Arc<DistributedCache>>,

    /// Node configuration.
    config: CacheConfig,

    /// Current state.
    state: NodeState,

    /// Data directory for persistent storage.
    data_dir: PathBuf,

    /// Checkpoint directory.
    checkpoint_dir: PathBuf,
}

/// A test cluster for recovery testing.
pub struct RecoveryTestCluster {
    /// Nodes in the cluster.
    nodes: RwLock<HashMap<NodeId, RecoveryTestNode>>,

    /// Temporary directories (owned to prevent cleanup).
    temp_dirs: Mutex<Vec<TempDir>>,

    /// Failpoint registry for injecting failures.
    pub failpoints: Arc<FailpointRegistry>,

    /// Consistency checker for verifying data.
    checker: RwLock<ConsistencyChecker>,

    /// Cluster configuration.
    config: RecoveryTestConfig,

    /// Port allocations.
    port_allocations: RwLock<HashMap<NodeId, (u16, u16)>>,

    /// Whether the cluster has been initialized.
    initialized: AtomicBool,

    /// Next proposal ID (for tracking writes).
    next_proposal_id: AtomicU64,
}

impl RecoveryTestCluster {
    /// Create a new recovery test cluster.
    pub async fn new(config: RecoveryTestConfig) -> Result<Self> {
        let cluster = Self {
            nodes: RwLock::new(HashMap::new()),
            temp_dirs: Mutex::new(Vec::new()),
            failpoints: Arc::new(FailpointRegistry::new()),
            checker: RwLock::new(ConsistencyChecker::new()),
            config,
            port_allocations: RwLock::new(HashMap::new()),
            initialized: AtomicBool::new(false),
            next_proposal_id: AtomicU64::new(1),
        };

        Ok(cluster)
    }

    /// Initialize the cluster by allocating ports and creating node configs.
    pub async fn initialize(&self) -> Result<()> {
        if self.initialized.load(Ordering::SeqCst) {
            return Ok(());
        }

        let node_ids: Vec<NodeId> = (1..=self.config.node_count as u64).collect();

        // Allocate both raft (TCP) and memberlist (UDP) ports for all nodes
        let port_configs = allocate_os_ports_with_memberlist(&node_ids).await;

        // Create temp directories and node configs
        let mut temp_dirs = self.temp_dirs.lock().await;
        let mut nodes = self.nodes.write();
        let mut port_allocs = self.port_allocations.write();

        for (node_id, raft_port, memberlist_port) in &port_configs {
            // Create temp directory for this node
            let temp_dir = tempfile::tempdir()
                .map_err(|e| Error::Internal(format!("Failed to create temp dir: {}", e)))?;

            let data_dir = temp_dir.path().join("data");
            let checkpoint_dir = temp_dir.path().join("checkpoints");

            std::fs::create_dir_all(&data_dir)
                .map_err(|e| Error::Internal(format!("Failed to create data dir: {}", e)))?;
            std::fs::create_dir_all(&checkpoint_dir)
                .map_err(|e| Error::Internal(format!("Failed to create checkpoint dir: {}", e)))?;

            // Build node configuration with OS-allocated memberlist port
            let config = self.build_node_config(
                *node_id,
                *raft_port,
                *memberlist_port,
                &data_dir,
                &checkpoint_dir,
                &port_configs,
            );

            nodes.insert(
                *node_id,
                RecoveryTestNode {
                    cache: None,
                    config,
                    state: NodeState::NotStarted,
                    data_dir,
                    checkpoint_dir,
                },
            );

            port_allocs.insert(*node_id, (*raft_port, *memberlist_port));
            temp_dirs.push(temp_dir);
        }

        self.initialized.store(true, Ordering::SeqCst);
        info!(node_count = self.config.node_count, "Recovery test cluster initialized");

        Ok(())
    }

    /// Build configuration for a single node.
    fn build_node_config(
        &self,
        node_id: NodeId,
        raft_port: u16,
        memberlist_port: u16,
        data_dir: &PathBuf,
        checkpoint_dir: &PathBuf,
        port_configs: &[(NodeId, u16, u16)], // (node_id, raft_port, memberlist_port)
    ) -> CacheConfig {
        let raft_addr: SocketAddr = format!("127.0.0.1:{}", raft_port).parse().unwrap();

        // Build seed nodes (all other nodes) - use raft ports
        let seed_nodes: Vec<(NodeId, SocketAddr)> = port_configs
            .iter()
            .filter(|(id, _, _)| *id != node_id)
            .map(|(id, raft_port, _)| {
                let addr: SocketAddr = format!("127.0.0.1:{}", raft_port).parse().unwrap();
                (*id, addr)
            })
            .collect();

        // Build memberlist seed addresses - use OS-allocated memberlist ports
        let memberlist_seeds: Vec<SocketAddr> = port_configs
            .iter()
            .filter(|(id, _, _)| *id != node_id)
            .map(|(_, _, ml_port)| {
                let addr: SocketAddr = format!("127.0.0.1:{}", ml_port).parse().unwrap();
                addr
            })
            .collect();

        // Raft configuration with fast timeouts for tests
        // Use staggered election ticks to reduce election contention
        let base_election_tick = 10;

        // Configure storage type based on persistent flag and feature availability
        #[cfg(feature = "rocksdb-storage")]
        let storage_type = if self.config.persistent {
            RaftStorageType::RocksDb(crate::config::RocksDbConfig::new(
                data_dir.join("raft").to_string_lossy().to_string(),
            ).with_sync_writes(true))
        } else {
            RaftStorageType::Memory
        };

        #[cfg(not(feature = "rocksdb-storage"))]
        let storage_type = {
            let _ = data_dir; // Suppress unused warning
            RaftStorageType::Memory
        };

        let raft_config = RaftConfig {
            tick_interval_ms: 50,
            election_tick: base_election_tick + (node_id as usize * 5),
            heartbeat_tick: 2,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 256,
            pre_vote: true,
            applied: 0,
            storage_type,
        };

        // Checkpoint configuration
        let checkpoint_config = CheckpointConfig::new(checkpoint_dir.clone())
            .with_log_threshold(self.config.checkpoint_log_threshold)
            .with_max_snapshots(self.config.checkpoint_max_snapshots)
            .with_compression(true);

        // Memberlist configuration
        let memberlist_addr: SocketAddr = format!("127.0.0.1:{}", memberlist_port).parse().unwrap();
        let memberlist_config = MemberlistConfig {
            enabled: true,
            bind_addr: Some(memberlist_addr),
            advertise_addr: None,
            node_name: None,
            seed_addrs: memberlist_seeds,
            auto_add_peers: true,
            auto_remove_peers: false,
            auto_add_voters: false,
            auto_remove_voters: false,
        };

        CacheConfig {
            node_id,
            raft_addr,
            seed_nodes,
            max_capacity: 100_000,
            default_ttl: Some(Duration::from_secs(3600)),
            default_tti: None,
            raft: raft_config,
            membership: Default::default(),
            memberlist: memberlist_config,
            checkpoint: checkpoint_config,
            forwarding: Default::default(),
            multiraft: Default::default(),
        }
    }

    /// Start all nodes in the cluster.
    pub async fn start_all(&self) -> Result<()> {
        self.initialize().await?;

        let node_ids: Vec<NodeId> = self.nodes.read().keys().copied().collect();
        for node_id in node_ids {
            self.start_node(node_id).await?;
        }

        Ok(())
    }

    /// Start a specific node.
    pub async fn start_node(&self, node_id: NodeId) -> Result<()> {
        let config = {
            let nodes = self.nodes.read();
            let node = nodes
                .get(&node_id)
                .ok_or_else(|| Error::Internal(format!("Node {} not found", node_id)))?;

            if node.state == NodeState::Running {
                return Ok(());
            }

            node.config.clone()
        };

        info!(node_id, "Starting node");

        let cache = DistributedCache::new(config).await?;
        let cache = Arc::new(cache);

        {
            let mut nodes = self.nodes.write();
            if let Some(node) = nodes.get_mut(&node_id) {
                node.cache = Some(cache);
                node.state = NodeState::Running;
            }
        }

        Ok(())
    }

    /// Stop a node gracefully.
    pub async fn stop_node(&self, node_id: NodeId) -> Result<()> {
        let cache = {
            let mut nodes = self.nodes.write();
            let node = nodes
                .get_mut(&node_id)
                .ok_or_else(|| Error::Internal(format!("Node {} not found", node_id)))?;

            if node.state != NodeState::Running {
                return Ok(());
            }

            node.state = NodeState::Stopped;
            node.cache.take()
        };

        if let Some(cache) = cache {
            info!(node_id, "Stopping node gracefully");
            // Give some time for pending operations
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(cache);
        }

        Ok(())
    }

    /// Simulate a crash (non-graceful shutdown).
    pub async fn crash_node(&self, node_id: NodeId) -> Result<()> {
        let cache = {
            let mut nodes = self.nodes.write();
            let node = nodes
                .get_mut(&node_id)
                .ok_or_else(|| Error::Internal(format!("Node {} not found", node_id)))?;

            if node.state != NodeState::Running {
                return Ok(());
            }

            node.state = NodeState::Crashed;
            node.cache.take()
        };

        if let Some(cache) = cache {
            info!(node_id, "Crashing node (non-graceful)");
            // Immediately drop without cleanup
            std::mem::drop(cache);
        }

        Ok(())
    }

    /// Recover a crashed or stopped node.
    pub async fn recover_node(&self, node_id: NodeId) -> Result<()> {
        {
            let nodes = self.nodes.read();
            let node = nodes
                .get(&node_id)
                .ok_or_else(|| Error::Internal(format!("Node {} not found", node_id)))?;

            if node.state == NodeState::Running {
                return Ok(());
            }
        }

        info!(node_id, "Recovering node from persistent state");

        // Wait for RocksDB lock file to be released after crash/stop
        // This is needed because RocksDB may take a moment to fully close
        tokio::time::sleep(Duration::from_millis(500)).await;

        self.start_node(node_id).await
    }

    /// Get the current leader (if any).
    pub async fn get_leader(&self) -> Option<NodeId> {
        let nodes = self.nodes.read();

        for (node_id, node) in nodes.iter() {
            if node.state != NodeState::Running {
                continue;
            }

            if let Some(cache) = &node.cache {
                if cache.is_leader() {
                    return Some(*node_id);
                }
            }
        }

        None
    }

    /// Wait for a leader to be elected.
    pub async fn wait_for_leader(&self, timeout_duration: Duration) -> Result<NodeId> {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            if let Some(leader) = self.get_leader().await {
                return Ok(leader);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Err(Error::Timeout)
    }

    /// Write a key-value pair to the cluster.
    pub async fn write(&self, key: &str, value: &str) -> Result<()> {
        self.write_with_ttl(key, value, None).await
    }

    /// Write a key-value pair with optional TTL.
    pub async fn write_with_ttl(
        &self,
        key: &str,
        value: &str,
        ttl: Option<Duration>,
    ) -> Result<()> {
        // Find the leader
        let leader_id = self.wait_for_leader(self.config.timeout).await?;

        let cache = {
            let nodes = self.nodes.read();
            let node = nodes
                .get(&leader_id)
                .ok_or_else(|| Error::Internal("Leader not found".to_string()))?;

            node.cache
                .clone()
                .ok_or_else(|| Error::Internal("Leader cache not running".to_string()))?
        };

        // Perform the write
        let key_bytes = Bytes::from(key.to_string());
        let value_bytes = Bytes::from(value.to_string());

        match ttl {
            Some(ttl) => cache.put_with_ttl(key_bytes, value_bytes, ttl).await?,
            None => cache.put(key_bytes, value_bytes).await?,
        }

        // Record the write for consistency checking
        self.checker.write().record_write(key, value);

        Ok(())
    }

    /// Read a value from a specific node.
    pub async fn read(&self, node_id: NodeId, key: &str) -> Result<Option<String>> {
        let cache = {
            let nodes = self.nodes.read();
            let node = nodes
                .get(&node_id)
                .ok_or_else(|| Error::Internal(format!("Node {} not found", node_id)))?;

            if node.state != NodeState::Running {
                return Err(Error::Internal(format!("Node {} is not running", node_id)));
            }

            node.cache
                .clone()
                .ok_or_else(|| Error::Internal(format!("Node {} cache not available", node_id)))?
        };

        let key_bytes = Bytes::from(key.to_string());
        let value = cache.get(&key_bytes).await;

        Ok(value.map(|v| String::from_utf8_lossy(&v).to_string()))
    }

    /// Read a value from the leader.
    pub async fn read_from_leader(&self, key: &str) -> Result<Option<String>> {
        let leader_id = self.wait_for_leader(self.config.timeout).await?;
        self.read(leader_id, key).await
    }

    /// Delete a key from the cluster.
    pub async fn delete(&self, key: &str) -> Result<()> {
        let leader_id = self.wait_for_leader(self.config.timeout).await?;

        let cache = {
            let nodes = self.nodes.read();
            let node = nodes
                .get(&leader_id)
                .ok_or_else(|| Error::Internal("Leader not found".to_string()))?;

            node.cache
                .clone()
                .ok_or_else(|| Error::Internal("Leader cache not running".to_string()))?
        };

        let key_bytes = Bytes::from(key.to_string());
        cache.delete(key_bytes).await?;

        self.checker.write().record_delete(key);

        Ok(())
    }

    /// Verify consistency across all running nodes.
    pub async fn verify_consistency(&self) -> Result<()> {
        let checker = self.checker.read();
        checker.verify_all_nodes(self).await
    }

    /// Verify a specific node's data matches expected state.
    pub async fn verify_node(&self, node_id: NodeId) -> Result<()> {
        let checker = self.checker.read();
        checker.verify_node(self, node_id).await
    }

    /// Get the state of a node.
    pub fn node_state(&self, node_id: NodeId) -> Option<NodeState> {
        self.nodes.read().get(&node_id).map(|n| n.state)
    }

    /// Get all node IDs.
    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.read().keys().copied().collect()
    }

    /// Get running node IDs.
    pub fn running_nodes(&self) -> Vec<NodeId> {
        self.nodes
            .read()
            .iter()
            .filter(|(_, n)| n.state == NodeState::Running)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get the cache instance for a node (if running).
    pub fn get_cache(&self, node_id: NodeId) -> Option<Arc<DistributedCache>> {
        self.nodes.read().get(&node_id).and_then(|n| n.cache.clone())
    }

    /// Force a snapshot on a specific node.
    pub async fn force_snapshot(&self, node_id: NodeId) -> Result<()> {
        let cache = self
            .get_cache(node_id)
            .ok_or_else(|| Error::Internal(format!("Node {} not running", node_id)))?;

        cache.force_checkpoint().await
    }

    /// Get the applied index for a node.
    pub fn get_applied_index(&self, node_id: NodeId) -> Option<u64> {
        self.get_cache(node_id).map(|c| c.applied_index())
    }

    /// Wait for all nodes to reach a minimum applied index.
    pub async fn wait_for_replication(&self, min_index: u64, timeout_duration: Duration) -> Result<()> {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout_duration {
            let mut all_caught_up = true;

            for node_id in self.running_nodes() {
                if let Some(index) = self.get_applied_index(node_id) {
                    if index < min_index {
                        all_caught_up = false;
                        break;
                    }
                } else {
                    all_caught_up = false;
                    break;
                }
            }

            if all_caught_up {
                return Ok(());
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Err(Error::Timeout)
    }

    /// Shutdown the entire cluster.
    pub async fn shutdown(&self) {
        let node_ids: Vec<NodeId> = self.nodes.read().keys().copied().collect();

        for node_id in node_ids {
            if let Err(e) = self.stop_node(node_id).await {
                warn!(node_id, error = %e, "Error stopping node during shutdown");
            }
        }

        info!("Recovery test cluster shutdown complete");
    }

    /// Clear the consistency checker (for new test scenarios).
    pub fn clear_expected_state(&self) {
        *self.checker.write() = ConsistencyChecker::new();
    }

    /// Get the checkpoint directory for a node.
    pub fn checkpoint_dir(&self, node_id: NodeId) -> Option<PathBuf> {
        self.nodes.read().get(&node_id).map(|n| n.checkpoint_dir.clone())
    }

    /// Get the data directory for a node.
    pub fn data_dir(&self, node_id: NodeId) -> Option<PathBuf> {
        self.nodes.read().get(&node_id).map(|n| n.data_dir.clone())
    }
}

impl Drop for RecoveryTestCluster {
    fn drop(&mut self) {
        // Synchronously drop all caches
        let mut nodes = self.nodes.write();
        for (_, node) in nodes.iter_mut() {
            node.cache = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_creation() {
        let config = RecoveryTestConfig::new(3);
        let cluster = RecoveryTestCluster::new(config).await.unwrap();

        cluster.initialize().await.unwrap();
        assert_eq!(cluster.node_ids().len(), 3);
    }

    #[tokio::test]
    async fn test_cluster_start_stop() {
        let config = RecoveryTestConfig::new(3);
        let cluster = RecoveryTestCluster::new(config).await.unwrap();

        cluster.start_all().await.unwrap();

        // Wait for leader
        let leader = cluster.wait_for_leader(Duration::from_secs(10)).await;
        assert!(leader.is_ok());

        cluster.shutdown().await;
    }
}
