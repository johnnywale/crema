//! Shard Raft Manager for Multi-Raft Phase 2.
//!
//! This module manages the lifecycle of all ShardRaftNodes, providing:
//! - Coordinated startup and shutdown
//! - Single tick loop for efficiency
//! - Message routing to correct shards

use crate::cache::storage::CacheStorage;
use crate::config::ShardRaftConfig;
use crate::error::{Error, Result};
use crate::types::NodeId;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::shard::ShardId;
use super::shard_raft_node::{ShardRaftNode, ShardRaftNodeBuilder};
use super::shard_transport::ShardTransportMultiplexer;

/// Statistics for the shard Raft manager.
#[derive(Debug, Clone)]
pub struct ShardRaftManagerStats {
    /// Total number of managed shards.
    pub total_shards: usize,

    /// Number of shards where this node is the leader.
    pub leader_shards: usize,

    /// Number of running shard nodes.
    pub running_shards: usize,
}

/// Manager for all ShardRaftNodes in a Multi-Raft setup.
///
/// The ShardRaftManager provides:
/// - Centralized lifecycle management for all shard Raft nodes
/// - Efficient single tick loop (optional) for all shards
/// - Coordinated startup and graceful shutdown
///
/// # Architecture
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────┐
/// │                     ShardRaftManager                            │
/// │  ┌───────────────────────────────────────────────────────────┐  │
/// │  │                 ShardTransportMultiplexer                 │  │
/// │  │              (shared across all shards)                   │  │
/// │  └───────────────────────────────────────────────────────────┘  │
/// │                              │                                   │
/// │           ┌──────────────────┼──────────────────┐               │
/// │           ▼                  ▼                  ▼               │
/// │    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
/// │    │ShardRaftNode │  │ShardRaftNode │  │ShardRaftNode │        │
/// │    │  (Shard 0)   │  │  (Shard 1)   │  │  (Shard N)   │        │
/// │    └──────────────┘  └──────────────┘  └──────────────┘        │
/// │                              │                                   │
/// │                    ┌─────────┴─────────┐                        │
/// │                    │  Unified Tick Loop │                       │
/// │                    │  (optional)        │                       │
/// │                    └───────────────────┘                        │
/// └─────────────────────────────────────────────────────────────────┘
/// ```
#[derive(Debug)]
pub struct ShardRaftManager {
    /// This node's ID.
    node_id: NodeId,

    /// This node's Raft address.
    local_addr: String,

    /// Shard Raft configuration.
    config: ShardRaftConfig,

    /// The shared transport multiplexer.
    transport: Arc<ShardTransportMultiplexer>,

    /// Map of shard ID to ShardRaftNode.
    /// Wrapped in Arc to allow sharing with the unified tick loop.
    shard_nodes: Arc<RwLock<HashMap<ShardId, Arc<ShardRaftNode>>>>,

    /// Shutdown senders for each shard's tick loop.
    shutdown_txs: RwLock<HashMap<ShardId, mpsc::Sender<()>>>,

    /// Task handles for each shard's tick loop.
    task_handles: RwLock<HashMap<ShardId, JoinHandle<()>>>,

    /// Whether the manager is running.
    running: AtomicBool,

    /// Shards currently being created (to prevent TOCTOU race in create_shard).
    pending_shards: RwLock<HashSet<ShardId>>,

    /// Whether to use unified tick loop.
    unified_tick_loop: bool,

    /// Handle for the unified tick loop task.
    unified_tick_handle: RwLock<Option<JoinHandle<()>>>,

    /// Shutdown sender for unified tick loop.
    unified_shutdown_tx: RwLock<Option<mpsc::Sender<()>>>,
}

impl ShardRaftManager {
    /// Create a new ShardRaftManager.
    pub fn new(
        node_id: NodeId,
        local_addr: String,
        config: ShardRaftConfig,
        transport: Arc<ShardTransportMultiplexer>,
    ) -> Self {
        Self {
            node_id,
            local_addr,
            config,
            transport,
            shard_nodes: Arc::new(RwLock::new(HashMap::new())),
            shutdown_txs: RwLock::new(HashMap::new()),
            task_handles: RwLock::new(HashMap::new()),
            running: AtomicBool::new(false),
            pending_shards: RwLock::new(HashSet::new()),
            unified_tick_loop: true, // Use unified loop by default for efficiency
            unified_tick_handle: RwLock::new(None),
            unified_shutdown_tx: RwLock::new(None),
        }
    }

    /// Create with unified tick loop disabled (each shard runs its own loop).
    pub fn with_per_shard_loops(mut self) -> Self {
        self.unified_tick_loop = false;
        self
    }

    /// Get this node's ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get the transport multiplexer.
    pub fn transport(&self) -> &Arc<ShardTransportMultiplexer> {
        &self.transport
    }

    /// Check if the manager is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Get the number of managed shards.
    pub fn shard_count(&self) -> usize {
        self.shard_nodes.read().len()
    }

    /// Get a shard node by ID.
    pub fn get_shard(&self, shard_id: ShardId) -> Option<Arc<ShardRaftNode>> {
        self.shard_nodes.read().get(&shard_id).cloned()
    }

    /// Get all shard IDs.
    pub fn shard_ids(&self) -> Vec<ShardId> {
        self.shard_nodes.read().keys().copied().collect()
    }

    /// Get all shard Raft nodes.
    pub fn all_shards(&self) -> Vec<Arc<ShardRaftNode>> {
        self.shard_nodes.read().values().cloned().collect()
    }

    /// Create and register a new shard Raft node.
    pub async fn create_shard(
        &self,
        shard_id: ShardId,
        peers: Vec<NodeId>,
        storage: Arc<CacheStorage>,
    ) -> Result<Arc<ShardRaftNode>> {
        // Use pending_shards to prevent TOCTOU race condition:
        // 1. Atomically check both maps and reserve the slot
        // 2. Create the shard (async)
        // 3. Insert into shard_nodes and remove from pending
        {
            let shard_nodes = self.shard_nodes.read();
            let mut pending = self.pending_shards.write();

            if shard_nodes.contains_key(&shard_id) || pending.contains(&shard_id) {
                return Err(Error::ShardAlreadyExists(shard_id));
            }

            // Reserve the slot
            pending.insert(shard_id);
        }

        // Create the shard Raft node (async operation)
        let shard_node = match ShardRaftNodeBuilder::new(shard_id, self.node_id)
            .with_peers(peers)
            .with_config(self.config.clone())
            .with_storage(storage)
            .with_transport(self.transport.clone())
            .with_local_addr(self.local_addr.clone())
            .build()
            .await
        {
            Ok(node) => node,
            Err(e) => {
                // Rollback: remove from pending
                self.pending_shards.write().remove(&shard_id);
                return Err(e);
            }
        };

        // Register the shard and remove from pending
        {
            self.shard_nodes
                .write()
                .insert(shard_id, shard_node.clone());
            self.pending_shards.write().remove(&shard_id);
        }

        tracing::info!(
            node_id = self.node_id,
            shard_id,
            "Created and registered ShardRaftNode"
        );

        // If not using unified loop and manager is running, start individual loop
        if !self.unified_tick_loop && self.running.load(Ordering::Relaxed) {
            self.start_shard_loop(shard_id, shard_node.clone()).await;
        }

        Ok(shard_node)
    }

    /// Remove a shard Raft node.
    pub async fn remove_shard(&self, shard_id: ShardId) -> Result<()> {
        // Stop the shard's tick loop if running individually
        let tx = self.shutdown_txs.write().remove(&shard_id);
        if let Some(tx) = tx {
            let _ = tx.send(()).await;
        }

        // Wait for the task to complete
        let handle = self.task_handles.write().remove(&shard_id);
        if let Some(handle) = handle {
            let _ = handle.await;
        }

        // Remove the shard node
        let shard_node = self
            .shard_nodes
            .write()
            .remove(&shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        // Shutdown the shard node
        shard_node.shutdown().await?;

        tracing::info!(node_id = self.node_id, shard_id, "Removed ShardRaftNode");

        Ok(())
    }

    /// Start the tick loops for all shards.
    pub async fn start(&self) -> Result<()> {
        self.running.store(true, Ordering::Relaxed);

        if self.unified_tick_loop {
            // Start the unified tick loop
            self.start_unified_loop().await;
        } else {
            // Start individual loops for each shard
            let shards: Vec<_> = self
                .shard_nodes
                .read()
                .iter()
                .map(|(id, node)| (*id, node.clone()))
                .collect();

            for (shard_id, shard_node) in shards {
                self.start_shard_loop(shard_id, shard_node).await;
            }
        }

        tracing::info!(
            node_id = self.node_id,
            shard_count = self.shard_count(),
            unified = self.unified_tick_loop,
            "ShardRaftManager started"
        );

        Ok(())
    }

    /// Start an individual shard's tick loop.
    async fn start_shard_loop(&self, shard_id: ShardId, shard_node: Arc<ShardRaftNode>) {
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let handle = tokio::spawn(async move {
            shard_node.run(shutdown_rx).await;
        });

        self.shutdown_txs.write().insert(shard_id, shutdown_tx);
        self.task_handles.write().insert(shard_id, handle);

        tracing::debug!(
            node_id = self.node_id,
            shard_id,
            "Started individual shard tick loop"
        );
    }

    /// Start the unified tick loop for all shards.
    async fn start_unified_loop(&self) {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        *self.unified_shutdown_tx.write() = Some(shutdown_tx);

        let shard_nodes = self.shard_nodes.clone();
        let tick_interval = Duration::from_millis(self.config.tick_interval_ms);
        let node_id = self.node_id;

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick_interval);
            let panic_count = AtomicU64::new(0);

            tracing::info!(node_id, "Unified shard tick loop started");

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Get all shard nodes
                        let nodes: Vec<Arc<ShardRaftNode>> = shard_nodes
                            .read()
                            .values()
                            .cloned()
                            .collect();

                        // Phase 1: Drain messages and tick (synchronous, fast)
                        // These operations must be sequential per shard but are CPU-bound
                        // Wrap in catch_unwind to prevent one shard's panic from killing the loop
                        for shard_node in &nodes {
                            let shard_id = shard_node.shard_id();

                            // Drain messages with panic protection
                            if let Err(e) = AssertUnwindSafe(shard_node.drain_messages())
                                .catch_unwind()
                                .await
                            {
                                let count = panic_count.fetch_add(1, Ordering::Relaxed);
                                tracing::error!(
                                    node_id,
                                    shard_id,
                                    panic_count = count + 1,
                                    "Shard drain_messages panicked: {:?}",
                                    e
                                );
                                continue; // Skip this shard for this tick
                            }

                            // Tick with panic protection
                            if std::panic::catch_unwind(AssertUnwindSafe(|| {
                                shard_node.tick();
                            }))
                            .is_err()
                            {
                                let count = panic_count.fetch_add(1, Ordering::Relaxed);
                                tracing::error!(
                                    node_id,
                                    shard_id,
                                    panic_count = count + 1,
                                    "Shard tick panicked"
                                );
                                continue;
                            }
                        }

                        // Phase 2: Process ready state concurrently using FuturesUnordered
                        // This is the I/O-heavy phase that can stall - run in parallel
                        // so a slow shard doesn't block healthy shards
                        let panic_count_ref = &panic_count;
                        let mut ready_futures: FuturesUnordered<_> = nodes
                            .iter()
                            .map(|node| {
                                let node = node.clone();
                                async move {
                                    let shard_id = node.shard_id();
                                    // Wrap process_ready in panic protection
                                    if let Err(_e) = AssertUnwindSafe(node.process_ready())
                                        .catch_unwind()
                                        .await
                                    {
                                        let count = panic_count_ref.fetch_add(1, Ordering::Relaxed);
                                        tracing::error!(
                                            node_id,
                                            shard_id,
                                            panic_count = count + 1,
                                            "Shard process_ready panicked"
                                        );
                                    }
                                }
                            })
                            .collect();

                        // Drive all futures to completion concurrently
                        while ready_futures.next().await.is_some() {}

                        // Yield to allow other tasks to run
                        tokio::task::yield_now().await;
                    }
                    _ = shutdown_rx.recv() => {
                        tracing::info!(node_id, "Unified shard tick loop shutdown signaled");
                        break;
                    }
                }
            }

            let total_panics = panic_count.load(Ordering::Relaxed);
            if total_panics > 0 {
                tracing::warn!(
                    node_id,
                    total_panics,
                    "Unified shard tick loop stopped with panics recorded"
                );
            } else {
                tracing::info!(node_id, "Unified shard tick loop stopped");
            }
        });

        *self.unified_tick_handle.write() = Some(handle);
    }

    /// Shutdown the manager and all shard nodes.
    pub async fn shutdown(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);

        // Stop unified loop if running (take from lock before awaiting)
        let unified_shutdown_tx = self.unified_shutdown_tx.write().take();
        if let Some(tx) = unified_shutdown_tx {
            let _ = tx.send(()).await;
        }

        let unified_tick_handle = self.unified_tick_handle.write().take();
        if let Some(handle) = unified_tick_handle {
            let _ = handle.await;
        }

        // Stop all individual shard loops (take from lock before awaiting)
        let shutdown_txs: Vec<_> = self.shutdown_txs.write().drain().collect();
        for (shard_id, tx) in shutdown_txs {
            tracing::debug!(shard_id, "Sending shutdown signal to shard loop");
            let _ = tx.send(()).await;
        }

        // Wait for all tasks to complete (take from lock before awaiting)
        // We use individual awaits with panic catching to ensure all tasks are awaited
        // even if some panic during shutdown
        let handles: Vec<_> = self.task_handles.write().drain().collect();
        for (shard_id, handle) in handles {
            tracing::debug!(shard_id, "Waiting for shard loop to complete");
            match handle.await {
                Ok(()) => {}
                Err(e) if e.is_panic() => {
                    tracing::error!(shard_id, "Shard loop task panicked during shutdown");
                }
                Err(e) => {
                    tracing::warn!(shard_id, error = %e, "Shard loop task cancelled");
                }
            }
        }

        // Shutdown all shard nodes
        let nodes: Vec<_> = self.shard_nodes.write().drain().collect();
        for (shard_id, node) in nodes {
            tracing::debug!(shard_id, "Shutting down shard node");
            if let Err(e) = node.shutdown().await {
                tracing::warn!(
                    shard_id,
                    error = %e,
                    "Error shutting down shard node"
                );
            }
        }

        // Shutdown transport
        self.transport.shutdown().await;

        tracing::info!(node_id = self.node_id, "ShardRaftManager shutdown complete");

        Ok(())
    }

    /// Get statistics about the managed shards.
    pub fn stats(&self) -> ShardRaftManagerStats {
        let nodes = self.shard_nodes.read();
        let total_shards = nodes.len();
        let leader_shards = nodes.values().filter(|n| n.is_leader()).count();
        let running_shards = nodes.values().filter(|n| n.is_running()).count();

        ShardRaftManagerStats {
            total_shards,
            leader_shards,
            running_shards,
        }
    }

    /// Add a peer to all shard Raft groups.
    pub async fn add_peer_to_all(&self, peer_id: NodeId, addr: SocketAddr) {
        // Add to transport
        self.transport.add_peer(peer_id, addr).await;

        // Collect nodes first, then drop lock before iterating
        // This avoids holding the read lock while calling add_peer on each node
        let nodes: Vec<_> = self.shard_nodes.read().values().cloned().collect();
        for node in nodes {
            node.add_peer(peer_id);
        }

        tracing::info!(
            node_id = self.node_id,
            peer_id,
            %addr,
            "Added peer to all shards"
        );
    }

    /// Remove a peer from all shard Raft groups.
    pub fn remove_peer_from_all(&self, peer_id: NodeId) {
        // Remove from transport
        self.transport.remove_peer(peer_id);

        // Collect nodes first, then drop lock before iterating
        // This avoids holding the read lock while calling remove_peer on each node
        let nodes: Vec<_> = self.shard_nodes.read().values().cloned().collect();
        for node in nodes {
            node.remove_peer(peer_id);
        }

        tracing::info!(
            node_id = self.node_id,
            peer_id,
            "Removed peer from all shards"
        );
    }
}

/// Builder for ShardRaftManager.
pub struct ShardRaftManagerBuilder {
    node_id: NodeId,
    local_addr: String,
    config: ShardRaftConfig,
    transport: Option<Arc<ShardTransportMultiplexer>>,
    unified_tick_loop: bool,
}

impl ShardRaftManagerBuilder {
    /// Create a new builder.
    pub fn new(node_id: NodeId, local_addr: impl Into<String>) -> Self {
        Self {
            node_id,
            local_addr: local_addr.into(),
            config: ShardRaftConfig::default(),
            transport: None,
            unified_tick_loop: true,
        }
    }

    /// Set the Raft configuration.
    pub fn with_config(mut self, config: ShardRaftConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the transport multiplexer.
    pub fn with_transport(mut self, transport: Arc<ShardTransportMultiplexer>) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Use per-shard tick loops instead of unified loop.
    pub fn with_per_shard_loops(mut self) -> Self {
        self.unified_tick_loop = false;
        self
    }

    /// Build the ShardRaftManager.
    pub fn build(self) -> Result<ShardRaftManager> {
        let transport = self
            .transport
            .ok_or_else(|| Error::Config("Transport is required".to_string()))?;

        let mut manager =
            ShardRaftManager::new(self.node_id, self.local_addr, self.config, transport);

        if !self.unified_tick_loop {
            manager = manager.with_per_shard_loops();
        }

        Ok(manager)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CacheConfig;
    use crate::consensus::transport::RaftTransport;

    #[tokio::test]
    async fn test_shard_raft_manager_creation() {
        let node_id = 1;
        let raft_transport = Arc::new(RaftTransport::new(node_id));
        let transport = Arc::new(ShardTransportMultiplexer::new(node_id, raft_transport));

        let manager = ShardRaftManagerBuilder::new(node_id, "127.0.0.1:9000")
            .with_transport(transport)
            .with_config(ShardRaftConfig::fast_for_tests())
            .build()
            .unwrap();

        assert_eq!(manager.node_id(), node_id);
        assert_eq!(manager.shard_count(), 0);
        assert!(!manager.is_running());
    }

    #[tokio::test]
    async fn test_shard_creation() {
        let node_id = 1;
        let raft_transport = Arc::new(RaftTransport::new(node_id));
        let transport = Arc::new(ShardTransportMultiplexer::new(node_id, raft_transport));

        let manager = ShardRaftManagerBuilder::new(node_id, "127.0.0.1:9000")
            .with_transport(transport)
            .with_config(ShardRaftConfig::fast_for_tests())
            .build()
            .unwrap();

        // Create storage
        let cache_config = CacheConfig::default();
        let storage = Arc::new(CacheStorage::new(&cache_config));

        // Create shard
        let shard_node = manager.create_shard(0, vec![1], storage).await.unwrap();

        assert_eq!(shard_node.shard_id(), 0);
        assert_eq!(manager.shard_count(), 1);
        assert!(manager.get_shard(0).is_some());
        assert!(manager.get_shard(1).is_none());
    }
}
