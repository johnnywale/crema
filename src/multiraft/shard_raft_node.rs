//! Per-shard RaftNode wrapper for Multi-Raft Phase 2.
//!
//! This module provides a shard-aware wrapper around RaftNode that enables
//! per-shard Raft consensus in a Multi-Raft setup.

use crate::cache::storage::CacheStorage;
use crate::config::{ShardRaftConfig, ShardReadMode};
use crate::consensus::state_machine::CacheStateMachine;
use crate::consensus::RaftNode;
use crate::error::{Error, RaftError, Result};
use crate::types::{CacheCommand, NodeId, ProposalResult};
use parking_lot::RwLock;
use raft::prelude::Message as RaftMessage;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use super::shard::ShardId;
use super::shard_transport::{ShardRaftTransport, ShardTransportMultiplexer};

/// A shard-aware RaftNode wrapper.
///
/// Wraps the existing RaftNode to provide per-shard Raft consensus.
/// Each ShardRaftNode is an independent Raft group managing a subset
/// of the keyspace.
///
/// # Architecture
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────┐
/// │                      ShardRaftNode                          │
/// │  ┌─────────────────────────────────────────────────────┐    │
/// │  │                     RaftNode                        │    │
/// │  │  (wrapped, handles consensus for this shard)        │    │
/// │  └─────────────────────────────────────────────────────┘    │
/// │  ┌─────────────────────────────────────────────────────┐    │
/// │  │                 CacheStateMachine                   │    │
/// │  │  (applies committed commands to local storage)      │    │
/// │  └─────────────────────────────────────────────────────┘    │
/// │  ┌─────────────────────────────────────────────────────┐    │
/// │  │              ShardTransportMultiplexer              │    │
/// │  │  (routes messages with shard_id prefix)             │    │
/// │  └─────────────────────────────────────────────────────┘    │
/// └─────────────────────────────────────────────────────────────┘
/// ```
#[derive(Debug)]
pub struct ShardRaftNode {
    /// The shard ID this node manages.
    shard_id: ShardId,

    /// This node's ID within the Raft cluster.
    node_id: NodeId,

    /// The underlying RaftNode.
    raft_node: Arc<RaftNode>,

    /// The shared transport multiplexer.
    transport: Arc<ShardTransportMultiplexer>,

    /// Receiver for incoming Raft messages.
    message_rx: RwLock<Option<mpsc::UnboundedReceiver<RaftMessage>>>,

    /// The local cache storage for this shard.
    storage: Arc<CacheStorage>,

    /// Read consistency mode.
    read_mode: ShardReadMode,

    /// Raft configuration.
    config: ShardRaftConfig,

    /// Whether the node is running.
    running: AtomicBool,

    /// Peers in this shard's Raft group.
    peers: RwLock<HashSet<NodeId>>,

    /// Current epoch for leader tracking.
    epoch: AtomicU64,
}

impl ShardRaftNode {
    /// Create a new ShardRaftNode.
    ///
    /// # Arguments
    ///
    /// * `shard_id` - The shard this node manages
    /// * `node_id` - This node's ID
    /// * `peers` - Initial peers in the Raft group
    /// * `config` - Raft configuration
    /// * `storage` - Local cache storage for this shard
    /// * `transport` - Shared transport multiplexer
    /// * `local_addr` - This node's local address
    pub async fn new(
        shard_id: ShardId,
        node_id: NodeId,
        peers: Vec<NodeId>,
        config: ShardRaftConfig,
        storage: Arc<CacheStorage>,
        transport: Arc<ShardTransportMultiplexer>,
        local_addr: String,
    ) -> Result<Arc<Self>> {
        // Create state machine for this shard
        let state_machine = Arc::new(CacheStateMachine::new(storage.clone()));

        // Create shard-aware transport adapter
        // This ensures Raft messages are tagged with the shard_id so they
        // are routed to the correct per-shard RaftNode on the receiving end.
        let shard_transport = Arc::new(ShardRaftTransport::new(shard_id, transport.clone()));

        // Convert to RaftConfig and create RaftNode with the shard transport
        let raft_config = config.to_raft_config();
        let raft_node = RaftNode::new_with_transport(
            node_id,
            peers.clone(),
            raft_config,
            state_machine,
            local_addr,
            shard_transport, // Inject shard-aware transport!
        )?;

        // Register with the transport multiplexer to receive messages
        let message_rx = transport.register_shard(shard_id);

        let peer_set: HashSet<NodeId> = peers.into_iter().collect();

        let shard_node = Arc::new(Self {
            shard_id,
            node_id,
            raft_node,
            transport,
            message_rx: RwLock::new(Some(message_rx)),
            storage,
            read_mode: config.read_mode,
            config,
            running: AtomicBool::new(false),
            peers: RwLock::new(peer_set),
            epoch: AtomicU64::new(0),
        });

        tracing::info!(shard_id, node_id, "Created ShardRaftNode");

        Ok(shard_node)
    }

    /// Get the shard ID.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Get this node's ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get the underlying RaftNode.
    pub fn raft_node(&self) -> &Arc<RaftNode> {
        &self.raft_node
    }

    /// Get the cache storage.
    pub fn storage(&self) -> &Arc<CacheStorage> {
        &self.storage
    }

    /// Check if this node is the leader.
    pub fn is_leader(&self) -> bool {
        self.raft_node.is_leader()
    }

    /// Get the current leader ID.
    pub fn leader_id(&self) -> Option<NodeId> {
        self.raft_node.leader_id()
    }

    /// Get the current term.
    pub fn term(&self) -> u64 {
        self.raft_node.term()
    }

    /// Get the commit index.
    pub fn commit_index(&self) -> u64 {
        self.raft_node.commit_index()
    }

    /// Get the applied index.
    pub fn applied_index(&self) -> u64 {
        self.raft_node.applied_index()
    }

    /// Get the current epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    /// Increment and return the new epoch.
    pub fn next_epoch(&self) -> u64 {
        self.epoch.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Get the read mode.
    pub fn read_mode(&self) -> ShardReadMode {
        self.read_mode
    }

    /// Check if the node is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Get the peers in this shard's Raft group.
    pub fn peers(&self) -> Vec<NodeId> {
        self.peers.read().iter().copied().collect()
    }

    /// Add a peer to this shard's Raft group.
    pub fn add_peer(&self, peer_id: NodeId) {
        self.peers.write().insert(peer_id);
    }

    /// Remove a peer from this shard's Raft group.
    pub fn remove_peer(&self, peer_id: NodeId) {
        self.peers.write().remove(&peer_id);
    }

    /// Propose a cache command to this shard's Raft group.
    ///
    /// This method proposes the command and waits for it to be committed.
    /// If this node is not the leader, returns NotLeader error with leader hint.
    pub async fn propose(&self, command: CacheCommand) -> Result<ProposalResult> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader: self.leader_id(),
            }
            .into());
        }

        // Propose via the underlying RaftNode
        self.raft_node.propose(command).await
    }

    /// Step a Raft message into this shard's RaftNode.
    pub fn step(&self, msg: RaftMessage) -> Result<()> {
        self.raft_node.step(msg)
    }

    /// Tick the Raft node (called periodically).
    pub fn tick(&self) {
        self.raft_node.tick();
    }

    /// Process ready state (called after tick or message handling).
    pub async fn process_ready(&self) {
        // Process the underlying RaftNode's ready state
        self.raft_node.process_ready().await;

        // Send any outgoing messages via the shard transport
        // Note: The RaftNode's transport already handles message sending,
        // but we need to wrap them with shard_id for proper routing.
        // This is handled by the tick loop in ShardRaftManager.
    }

    /// Run the shard Raft tick loop.
    ///
    /// This method runs until shutdown is signaled.
    pub async fn run(self: Arc<Self>, mut shutdown_rx: mpsc::Receiver<()>) {
        let tick_interval = Duration::from_millis(self.config.tick_interval_ms);
        let mut interval = tokio::time::interval(tick_interval);

        self.running.store(true, Ordering::Relaxed);

        // Take the message receiver
        // If already taken (run_tick_loop called twice), log error and continue without receiver
        let mut message_rx = self.message_rx.write().take();
        if message_rx.is_none() {
            tracing::error!(
                shard_id = self.shard_id,
                node_id = self.node_id,
                "run_tick_loop called but message_rx already taken - duplicate tick loop?"
            );
        }

        tracing::info!(
            shard_id = self.shard_id,
            node_id = self.node_id,
            "ShardRaftNode tick loop started"
        );

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.tick();
                    self.process_ready().await;
                }
                msg = async {
                    if let Some(ref mut rx) = message_rx {
                        rx.recv().await
                    } else {
                        std::future::pending().await
                    }
                } => {
                    if let Some(raft_msg) = msg {
                        if let Err(e) = self.step(raft_msg) {
                            tracing::warn!(
                                shard_id = self.shard_id,
                                error = %e,
                                "Failed to step Raft message"
                            );
                        }
                        self.process_ready().await;
                    }
                }
                _ = shutdown_rx.recv() => {
                    tracing::info!(
                        shard_id = self.shard_id,
                        node_id = self.node_id,
                        "ShardRaftNode shutdown signaled"
                    );
                    break;
                }
            }
        }

        self.running.store(false, Ordering::Relaxed);

        // Unregister from transport
        self.transport.unregister_shard(self.shard_id);

        tracing::info!(
            shard_id = self.shard_id,
            node_id = self.node_id,
            "ShardRaftNode tick loop stopped"
        );
    }

    /// Drain and process any pending Raft messages.
    ///
    /// This is used by the unified tick loop to process incoming messages
    /// without needing to run the full `run()` loop.
    ///
    /// Returns the number of messages processed.
    pub async fn drain_messages(&self) -> usize {
        let mut message_rx = self.message_rx.write();
        let Some(rx) = message_rx.as_mut() else {
            return 0;
        };

        let mut count = 0;
        // Drain all available messages without blocking
        loop {
            match rx.try_recv() {
                Ok(raft_msg) => {
                    if let Err(e) = self.step(raft_msg) {
                        tracing::warn!(
                            shard_id = self.shard_id,
                            error = %e,
                            "Failed to step Raft message in drain"
                        );
                    }
                    count += 1;
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    tracing::warn!(shard_id = self.shard_id, "Message channel disconnected");
                    break;
                }
            }
        }

        count
    }

    /// Gracefully shutdown the shard Raft node.
    pub async fn shutdown(self: Arc<Self>) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        self.transport.unregister_shard(self.shard_id);

        // Shutdown the underlying RaftNode
        self.raft_node.clone().shutdown().await?;

        tracing::info!(
            shard_id = self.shard_id,
            node_id = self.node_id,
            "ShardRaftNode shutdown complete"
        );

        Ok(())
    }
}

/// Builder for ShardRaftNode.
pub struct ShardRaftNodeBuilder {
    shard_id: ShardId,
    node_id: NodeId,
    peers: Vec<NodeId>,
    config: ShardRaftConfig,
    storage: Option<Arc<CacheStorage>>,
    transport: Option<Arc<ShardTransportMultiplexer>>,
    local_addr: String,
}

impl ShardRaftNodeBuilder {
    /// Create a new builder.
    pub fn new(shard_id: ShardId, node_id: NodeId) -> Self {
        Self {
            shard_id,
            node_id,
            peers: vec![node_id], // Self is always a peer
            config: ShardRaftConfig::default(),
            storage: None,
            transport: None,
            local_addr: "127.0.0.1:0".to_string(),
        }
    }

    /// Set the peers.
    pub fn with_peers(mut self, peers: Vec<NodeId>) -> Self {
        self.peers = peers;
        self
    }

    /// Set the Raft configuration.
    pub fn with_config(mut self, config: ShardRaftConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the storage.
    pub fn with_storage(mut self, storage: Arc<CacheStorage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Set the transport.
    pub fn with_transport(mut self, transport: Arc<ShardTransportMultiplexer>) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Set the local address.
    pub fn with_local_addr(mut self, addr: impl Into<String>) -> Self {
        self.local_addr = addr.into();
        self
    }

    /// Build the ShardRaftNode.
    pub async fn build(self) -> Result<Arc<ShardRaftNode>> {
        let storage = self
            .storage
            .ok_or_else(|| Error::Config("Storage is required".to_string()))?;

        let transport = self
            .transport
            .ok_or_else(|| Error::Config("Transport is required".to_string()))?;

        ShardRaftNode::new(
            self.shard_id,
            self.node_id,
            self.peers,
            self.config,
            storage,
            transport,
            self.local_addr,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CacheConfig;
    use crate::consensus::transport::RaftTransport;

    #[tokio::test]
    async fn test_shard_raft_node_creation() {
        let node_id = 1;
        let shard_id = 0;

        // Create storage
        let cache_config = CacheConfig::default();
        let storage = Arc::new(CacheStorage::new(&cache_config));

        // Create transport
        let raft_transport = Arc::new(RaftTransport::new(node_id));
        let transport = Arc::new(ShardTransportMultiplexer::new(node_id, raft_transport));

        // Create shard Raft node
        let shard_node = ShardRaftNodeBuilder::new(shard_id, node_id)
            .with_storage(storage)
            .with_transport(transport)
            .with_config(ShardRaftConfig::fast_for_tests())
            .build()
            .await
            .unwrap();

        assert_eq!(shard_node.shard_id(), shard_id);
        assert_eq!(shard_node.node_id(), node_id);
        assert!(!shard_node.is_running());
    }
}
