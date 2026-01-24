//! Cluster discovery trait and types.
//!
//! This module defines a generic trait for cluster discovery implementations,
//! allowing different gossip protocols to be used (memberlist, etcd, Consul, etc.).
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    DistributedCache                              │
//! │                                                                  │
//! │   discovery: Box<dyn ClusterDiscovery>                          │
//! │                                                                  │
//! └─────────────────────────────────────────────────────────────────┘
//!                               │
//!                               ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                  ClusterDiscovery Trait                          │
//! └─────────────────────────────────────────────────────────────────┘
//!           │                    │                    │
//!           ▼                    ▼                    ▼
//! ┌──────────────┐    ┌──────────────────┐    ┌──────────────────┐
//! │ Memberlist   │    │ NoOpCluster      │    │ Future: etcd,    │
//! │ Discovery    │    │ Discovery        │    │ Consul, etc.     │
//! └──────────────┘    └──────────────────┘    └──────────────────┘
//! ```

use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

use crate::types::NodeId;

/// Metadata for a node in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetadata {
    /// Node's unique identifier for Raft consensus.
    pub node_id: NodeId,
    /// Address for Raft RPC communication.
    pub raft_addr: SocketAddr,
    /// Node version for compatibility checking.
    pub version: String,
    /// Optional tags for node classification and metadata.
    pub tags: HashMap<String, String>,
}

impl NodeMetadata {
    /// Create new node metadata.
    pub fn new(node_id: NodeId, raft_addr: SocketAddr) -> Self {
        Self {
            node_id,
            raft_addr,
            version: env!("CARGO_PKG_VERSION").to_string(),
            tags: HashMap::new(),
        }
    }

    /// Add a tag to the metadata.
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// Get a tag value.
    pub fn get_tag(&self, key: &str) -> Option<&str> {
        self.tags.get(key).map(|s| s.as_str())
    }

    /// Set a tag value.
    pub fn set_tag(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.tags.insert(key.into(), value.into());
    }

    /// Remove a tag.
    pub fn remove_tag(&mut self, key: &str) -> Option<String> {
        self.tags.remove(key)
    }

    /// Serialize metadata to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_default()
    }

    /// Deserialize metadata from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }
}

/// Shard leader information with epoch for ordering out-of-order gossip updates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardLeaderInfo {
    /// The leader's node ID.
    pub leader_id: NodeId,
    /// Monotonically increasing epoch for version ordering.
    pub epoch: u64,
}

impl ShardLeaderInfo {
    /// Create new shard leader info.
    pub fn new(leader_id: NodeId, epoch: u64) -> Self {
        Self { leader_id, epoch }
    }
}

/// Extension trait for NodeMetadata to handle shard leader encoding.
pub trait ShardLeaderMetadata {
    /// Set shard leaders with epoch information.
    fn set_shard_leaders(&mut self, leaders: &HashMap<u32, ShardLeaderInfo>);

    /// Get shard leaders from metadata.
    fn get_shard_leaders(&self) -> HashMap<u32, ShardLeaderInfo>;

    /// Check if this node has shard leader information.
    fn has_shard_leaders(&self) -> bool;
}

impl ShardLeaderMetadata for NodeMetadata {
    fn set_shard_leaders(&mut self, leaders: &HashMap<u32, ShardLeaderInfo>) {
        if leaders.is_empty() {
            self.tags.remove("multiraft.shard_leaders");
            return;
        }

        let encoded = leaders
            .iter()
            .map(|(shard_id, info)| format!("{}:{}:{}", shard_id, info.leader_id, info.epoch))
            .collect::<Vec<_>>()
            .join(",");
        self.tags.insert("multiraft.shard_leaders".to_string(), encoded);
    }

    fn get_shard_leaders(&self) -> HashMap<u32, ShardLeaderInfo> {
        self.tags
            .get("multiraft.shard_leaders")
            .map(|s| parse_shard_leaders(s))
            .unwrap_or_default()
    }

    fn has_shard_leaders(&self) -> bool {
        self.tags.contains_key("multiraft.shard_leaders")
    }
}

/// Parse shard leaders from the encoded string format.
fn parse_shard_leaders(encoded: &str) -> HashMap<u32, ShardLeaderInfo> {
    let mut result = HashMap::new();

    if encoded.is_empty() {
        return result;
    }

    for entry in encoded.split(',') {
        let parts: Vec<&str> = entry.split(':').collect();
        if parts.len() != 3 {
            continue;
        }

        let shard_id = match parts[0].parse::<u32>() {
            Ok(id) => id,
            Err(_) => continue,
        };
        let leader_id = match parts[1].parse::<NodeId>() {
            Ok(id) => id,
            Err(_) => continue,
        };
        let epoch = match parts[2].parse::<u64>() {
            Ok(e) => e,
            Err(_) => continue,
        };

        result.insert(shard_id, ShardLeaderInfo::new(leader_id, epoch));
    }

    result
}

/// Events emitted by cluster discovery implementations.
#[derive(Debug, Clone)]
pub enum ClusterEvent {
    /// A node joined the cluster.
    NodeJoin {
        node_id: NodeId,
        raft_addr: SocketAddr,
        metadata: NodeMetadata,
    },
    /// A node left the cluster gracefully.
    NodeLeave { node_id: NodeId },
    /// A node was detected as failed.
    NodeFailed { node_id: NodeId },
    /// A node's metadata was updated.
    NodeUpdate {
        node_id: NodeId,
        metadata: NodeMetadata,
    },
}

/// Node state tracked by the registry.
#[derive(Debug, Clone)]
struct TrackedNode {
    metadata: NodeMetadata,
    first_seen: Instant,
    last_seen: Instant,
    is_healthy: bool,
}

impl TrackedNode {
    fn new(metadata: NodeMetadata) -> Self {
        let now = Instant::now();
        Self {
            metadata,
            first_seen: now,
            last_seen: now,
            is_healthy: true,
        }
    }
}

/// Registry mapping Node IDs to their network addresses and metadata.
/// This is shared infrastructure used by all cluster discovery implementations.
#[derive(Debug, Default)]
pub struct NodeRegistry {
    /// Maps NodeId → node state
    nodes: RwLock<HashMap<NodeId, TrackedNode>>,
}

impl NodeRegistry {
    /// Create a new node registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register or update a node.
    /// Returns true if this is a new node, false if existing.
    pub fn register(&self, metadata: NodeMetadata) -> bool {
        let node_id = metadata.node_id;
        let mut nodes = self.nodes.write();

        if let Some(node) = nodes.get_mut(&node_id) {
            node.metadata = metadata;
            node.last_seen = Instant::now();
            node.is_healthy = true;
            false
        } else {
            nodes.insert(node_id, TrackedNode::new(metadata));
            true
        }
    }

    /// Mark a node as unhealthy (failed).
    pub fn mark_failed(&self, node_id: NodeId) {
        if let Some(node) = self.nodes.write().get_mut(&node_id) {
            node.is_healthy = false;
        }
    }

    /// Mark a node as recovered (healthy again).
    pub fn mark_recovered(&self, node_id: NodeId) {
        if let Some(node) = self.nodes.write().get_mut(&node_id) {
            node.is_healthy = true;
            node.last_seen = Instant::now();
        }
    }

    /// Remove a node from the registry.
    pub fn unregister(&self, node_id: NodeId) -> Option<NodeMetadata> {
        self.nodes.write().remove(&node_id).map(|n| n.metadata)
    }

    /// Get the Raft address for a node.
    pub fn get_addr(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.nodes.read().get(&node_id).map(|n| n.metadata.raft_addr)
    }

    /// Get full metadata for a node.
    pub fn get_metadata(&self, node_id: NodeId) -> Option<NodeMetadata> {
        self.nodes.read().get(&node_id).map(|n| n.metadata.clone())
    }

    /// Check if a node is registered.
    pub fn contains(&self, node_id: NodeId) -> bool {
        self.nodes.read().contains_key(&node_id)
    }

    /// Check if a node is healthy.
    pub fn is_healthy(&self, node_id: NodeId) -> bool {
        self.nodes
            .read()
            .get(&node_id)
            .map(|n| n.is_healthy)
            .unwrap_or(false)
    }

    /// Get all registered node IDs.
    pub fn all_nodes(&self) -> Vec<NodeId> {
        self.nodes.read().keys().copied().collect()
    }

    /// Get all healthy node IDs.
    pub fn healthy_nodes(&self) -> Vec<NodeId> {
        self.nodes
            .read()
            .iter()
            .filter(|(_, n)| n.is_healthy)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get all node addresses.
    pub fn all_addresses(&self) -> HashMap<NodeId, SocketAddr> {
        self.nodes
            .read()
            .iter()
            .map(|(id, n)| (*id, n.metadata.raft_addr))
            .collect()
    }

    /// Get the number of registered nodes.
    pub fn len(&self) -> usize {
        self.nodes.read().len()
    }

    /// Check if registry is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.read().is_empty()
    }
}

/// Errors that can occur in cluster discovery operations.
#[derive(Debug, thiserror::Error)]
pub enum ClusterDiscoveryError {
    #[error("Failed to bind to address: {0}")]
    BindError(String),

    #[error("Failed to join cluster: {0}")]
    JoinError(String),

    #[error("Transport error: {0}")]
    TransportError(String),

    #[error("Cluster not initialized")]
    NotInitialized,

    #[error("Other error: {0}")]
    Other(String),
}

/// Trait for cluster discovery implementations.
///
/// Implementations provide node discovery, health monitoring, and metadata
/// broadcasting for distributed cluster membership.
///
/// # Implementations
///
/// - `MemberlistDiscovery` - Gossip-based discovery using SWIM protocol (requires `memberlist` feature)
/// - `NoOpClusterDiscovery` - No-op implementation for single-node or manual management
#[async_trait]
pub trait ClusterDiscovery: Send + Sync {
    /// Start the cluster discovery service.
    async fn start(&mut self) -> Result<(), ClusterDiscoveryError>;

    /// Gracefully leave the cluster.
    async fn leave(&mut self) -> Result<(), ClusterDiscoveryError>;

    /// Shutdown the discovery service.
    async fn shutdown(&mut self) -> Result<(), ClusterDiscoveryError>;

    /// Check if the service is initialized and running.
    fn is_initialized(&self) -> bool;

    /// Get this node's ID.
    fn node_id(&self) -> NodeId;

    /// Get the node registry for accessing discovered nodes.
    fn registry(&self) -> Arc<NodeRegistry>;

    /// Receive the next cluster event (blocking).
    async fn recv_event(&mut self) -> Option<ClusterEvent>;

    /// Try to receive an event without blocking.
    fn try_recv_event(&mut self) -> Option<ClusterEvent>;

    /// Get all known member node IDs.
    fn members(&self) -> Vec<NodeId>;

    /// Get all healthy member node IDs.
    fn healthy_members(&self) -> Vec<NodeId>;

    /// Get address for a specific node.
    fn get_node_addr(&self, node_id: NodeId) -> Option<SocketAddr>;

    /// Update local node metadata (triggers broadcast to cluster).
    async fn update_local_metadata(&self, metadata: NodeMetadata) -> Result<(), ClusterDiscoveryError>;

    /// Check if discovery is actually doing something (vs NoOp).
    fn is_active(&self) -> bool {
        true
    }
}

/// A no-op cluster discovery implementation for single-node deployments
/// or when discovery is disabled.
pub struct NoOpClusterDiscovery {
    node_id: NodeId,
    registry: Arc<NodeRegistry>,
    event_rx: mpsc::UnboundedReceiver<ClusterEvent>,
    event_tx: mpsc::UnboundedSender<ClusterEvent>,
    initialized: bool,
}

impl NoOpClusterDiscovery {
    /// Create a new no-op discovery instance.
    pub fn new(node_id: NodeId, raft_addr: SocketAddr) -> Self {
        let registry = Arc::new(NodeRegistry::new());
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        // Register self
        let metadata = NodeMetadata::new(node_id, raft_addr);
        registry.register(metadata);

        Self {
            node_id,
            registry,
            event_rx,
            event_tx,
            initialized: false,
        }
    }

    /// Simulate a node join event (for testing or manual management).
    pub fn simulate_join(&self, node_id: NodeId, raft_addr: SocketAddr) {
        let metadata = NodeMetadata::new(node_id, raft_addr);
        self.registry.register(metadata.clone());
        let _ = self.event_tx.send(ClusterEvent::NodeJoin {
            node_id,
            raft_addr,
            metadata,
        });
    }

    /// Simulate a node leave event (for testing or manual management).
    pub fn simulate_leave(&self, node_id: NodeId) {
        self.registry.unregister(node_id);
        let _ = self.event_tx.send(ClusterEvent::NodeLeave { node_id });
    }

    /// Simulate a node failure event (for testing or manual management).
    pub fn simulate_failure(&self, node_id: NodeId) {
        self.registry.mark_failed(node_id);
        let _ = self.event_tx.send(ClusterEvent::NodeFailed { node_id });
    }
}

#[async_trait]
impl ClusterDiscovery for NoOpClusterDiscovery {
    async fn start(&mut self) -> Result<(), ClusterDiscoveryError> {
        self.initialized = true;
        Ok(())
    }

    async fn leave(&mut self) -> Result<(), ClusterDiscoveryError> {
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), ClusterDiscoveryError> {
        self.initialized = false;
        Ok(())
    }

    fn is_initialized(&self) -> bool {
        self.initialized
    }

    fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn registry(&self) -> Arc<NodeRegistry> {
        self.registry.clone()
    }

    async fn recv_event(&mut self) -> Option<ClusterEvent> {
        self.event_rx.recv().await
    }

    fn try_recv_event(&mut self) -> Option<ClusterEvent> {
        self.event_rx.try_recv().ok()
    }

    fn members(&self) -> Vec<NodeId> {
        self.registry.all_nodes()
    }

    fn healthy_members(&self) -> Vec<NodeId> {
        self.registry.healthy_nodes()
    }

    fn get_node_addr(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.registry.get_addr(node_id)
    }

    async fn update_local_metadata(&self, metadata: NodeMetadata) -> Result<(), ClusterDiscoveryError> {
        self.registry.register(metadata);
        Ok(())
    }

    fn is_active(&self) -> bool {
        false // NoOp discovery is not actively discovering
    }
}

impl std::fmt::Debug for NoOpClusterDiscovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NoOpClusterDiscovery")
            .field("node_id", &self.node_id)
            .field("initialized", &self.initialized)
            .finish()
    }
}

/// Configuration for static cluster discovery.
#[derive(Debug, Clone)]
pub struct StaticDiscoveryConfig {
    /// List of peer nodes (node_id, raft_addr).
    pub peers: Vec<(NodeId, SocketAddr)>,
    /// Whether to perform periodic health checks via TCP.
    pub health_check_enabled: bool,
    /// Interval between health checks.
    pub health_check_interval: std::time::Duration,
    /// Timeout for TCP health check connection attempts.
    pub health_check_timeout: std::time::Duration,
}

impl Default for StaticDiscoveryConfig {
    fn default() -> Self {
        Self {
            peers: Vec::new(),
            health_check_enabled: true,
            health_check_interval: std::time::Duration::from_secs(5),
            health_check_timeout: std::time::Duration::from_secs(2),
        }
    }
}

impl StaticDiscoveryConfig {
    /// Create a new static discovery config with the given peers.
    pub fn new(peers: Vec<(NodeId, SocketAddr)>) -> Self {
        Self {
            peers,
            ..Default::default()
        }
    }

    /// Create config from a list of addresses with auto-assigned node IDs.
    /// Node IDs are assigned starting from 1.
    pub fn from_addrs(addrs: Vec<SocketAddr>) -> Self {
        let peers = addrs
            .into_iter()
            .enumerate()
            .map(|(i, addr)| ((i + 1) as NodeId, addr))
            .collect();
        Self::new(peers)
    }

    /// Disable health checks.
    pub fn without_health_checks(mut self) -> Self {
        self.health_check_enabled = false;
        self
    }

    /// Set health check interval.
    pub fn with_health_check_interval(mut self, interval: std::time::Duration) -> Self {
        self.health_check_interval = interval;
        self
    }

    /// Set health check timeout.
    pub fn with_health_check_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.health_check_timeout = timeout;
        self
    }

    /// Add a peer to the configuration.
    pub fn with_peer(mut self, node_id: NodeId, addr: SocketAddr) -> Self {
        self.peers.push((node_id, addr));
        self
    }
}

/// Static cluster discovery using a fixed list of node addresses.
///
/// Unlike `MemberlistDiscovery` which requires a separate gossip port, this implementation
/// uses the Raft service addresses directly. Health checks verify nodes are reachable
/// by attempting TCP connections to their Raft ports.
///
/// This implementation is useful for:
/// - Development and testing environments
/// - Simple deployments with known node addresses
/// - Kubernetes/container deployments where service discovery is handled externally
/// - Environments where you don't want additional gossip protocol overhead
///
/// # Addresses
///
/// The addresses configured should be the **Raft service ports** (same as `raft_addr`).
/// No separate discovery port is needed.
///
/// # Example
///
/// ```rust,ignore
/// use crema::cluster::{StaticClusterDiscovery, StaticDiscoveryConfig};
///
/// // All addresses are Raft service ports (e.g., 9000)
/// let config = StaticDiscoveryConfig::new(vec![
///     (1, "192.168.1.10:9000".parse().unwrap()),  // node 1 Raft addr
///     (2, "192.168.1.11:9000".parse().unwrap()),  // node 2 Raft addr
///     (3, "192.168.1.12:9000".parse().unwrap()),  // node 3 Raft addr
/// ]);
///
/// let mut discovery = StaticClusterDiscovery::new(
///     1,                                           // this node's ID
///     "192.168.1.10:9000".parse().unwrap(),       // this node's Raft addr
///     config,
/// );
/// discovery.start().await?;
/// ```
pub struct StaticClusterDiscovery {
    /// This node's ID.
    node_id: NodeId,
    /// This node's Raft address.
    raft_addr: SocketAddr,
    /// Configuration with peer list.
    config: StaticDiscoveryConfig,
    /// Node registry.
    registry: Arc<NodeRegistry>,
    /// Event channel receiver.
    event_rx: mpsc::UnboundedReceiver<ClusterEvent>,
    /// Event channel sender.
    event_tx: mpsc::UnboundedSender<ClusterEvent>,
    /// Whether the discovery is initialized.
    initialized: bool,
    /// Health check task handle.
    health_check_handle: Option<tokio::task::JoinHandle<()>>,
    /// Shutdown signal for health check task.
    health_check_shutdown: Option<mpsc::Sender<()>>,
}

impl StaticClusterDiscovery {
    /// Create a new static cluster discovery instance.
    pub fn new(node_id: NodeId, raft_addr: SocketAddr, config: StaticDiscoveryConfig) -> Self {
        let registry = Arc::new(NodeRegistry::new());
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        // Register self
        let metadata = NodeMetadata::new(node_id, raft_addr);
        registry.register(metadata);

        Self {
            node_id,
            raft_addr,
            config,
            registry,
            event_rx,
            event_tx,
            initialized: false,
            health_check_handle: None,
            health_check_shutdown: None,
        }
    }

    /// Create from a simple list of peer addresses.
    /// This node's ID and address must be included in the list.
    pub fn from_peers(
        node_id: NodeId,
        raft_addr: SocketAddr,
        peers: Vec<(NodeId, SocketAddr)>,
    ) -> Self {
        let config = StaticDiscoveryConfig::new(peers);
        Self::new(node_id, raft_addr, config)
    }

    /// Start health check background task.
    fn start_health_checks(&mut self) {
        if !self.config.health_check_enabled {
            return;
        }

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        let registry = self.registry.clone();
        let event_tx = self.event_tx.clone();
        let peers: Vec<_> = self
            .config
            .peers
            .iter()
            .filter(|(id, _)| *id != self.node_id)
            .cloned()
            .collect();
        let interval = self.config.health_check_interval;
        let timeout = self.config.health_check_timeout;

        let handle = tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        tracing::debug!("Static discovery health check shutting down");
                        break;
                    }
                    _ = interval_timer.tick() => {
                        for (peer_id, peer_addr) in &peers {
                            let was_healthy = registry.is_healthy(*peer_id);
                            let is_healthy = Self::check_health(*peer_addr, timeout).await;

                            if was_healthy && !is_healthy {
                                // Node became unhealthy
                                registry.mark_failed(*peer_id);
                                let _ = event_tx.send(ClusterEvent::NodeFailed { node_id: *peer_id });
                                tracing::warn!(
                                    node_id = *peer_id,
                                    addr = %peer_addr,
                                    "Static discovery: node health check failed"
                                );
                            } else if !was_healthy && is_healthy {
                                // Node recovered
                                registry.mark_recovered(*peer_id);
                                if let Some(metadata) = registry.get_metadata(*peer_id) {
                                    let _ = event_tx.send(ClusterEvent::NodeUpdate {
                                        node_id: *peer_id,
                                        metadata,
                                    });
                                }
                                tracing::info!(
                                    node_id = *peer_id,
                                    addr = %peer_addr,
                                    "Static discovery: node recovered"
                                );
                            }
                        }
                    }
                }
            }
        });

        self.health_check_handle = Some(handle);
        self.health_check_shutdown = Some(shutdown_tx);
    }

    /// Check health of a peer via TCP connection attempt.
    async fn check_health(addr: SocketAddr, timeout: std::time::Duration) -> bool {
        match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(addr)).await {
            Ok(Ok(_stream)) => true,
            Ok(Err(_)) | Err(_) => false,
        }
    }

    /// Add a peer dynamically after creation.
    pub fn add_peer(&self, node_id: NodeId, raft_addr: SocketAddr) {
        let metadata = NodeMetadata::new(node_id, raft_addr);
        let is_new = self.registry.register(metadata.clone());

        if is_new {
            let _ = self.event_tx.send(ClusterEvent::NodeJoin {
                node_id,
                raft_addr,
                metadata,
            });
        }
    }

    /// Remove a peer dynamically.
    pub fn remove_peer(&self, node_id: NodeId) {
        if self.registry.unregister(node_id).is_some() {
            let _ = self.event_tx.send(ClusterEvent::NodeLeave { node_id });
        }
    }

    /// Get the configuration.
    pub fn config(&self) -> &StaticDiscoveryConfig {
        &self.config
    }
}

#[async_trait]
impl ClusterDiscovery for StaticClusterDiscovery {
    async fn start(&mut self) -> Result<(), ClusterDiscoveryError> {
        if self.initialized {
            return Ok(());
        }

        tracing::info!(
            node_id = self.node_id,
            peer_count = self.config.peers.len(),
            "Starting static cluster discovery"
        );

        // Register all configured peers and emit join events
        for (peer_id, peer_addr) in &self.config.peers {
            if *peer_id == self.node_id {
                continue; // Skip self, already registered
            }

            let metadata = NodeMetadata::new(*peer_id, *peer_addr);
            self.registry.register(metadata.clone());

            // Emit join event for each peer
            let _ = self.event_tx.send(ClusterEvent::NodeJoin {
                node_id: *peer_id,
                raft_addr: *peer_addr,
                metadata,
            });

            tracing::debug!(
                node_id = *peer_id,
                addr = %peer_addr,
                "Static discovery: registered peer"
            );
        }

        // Start health check background task
        self.start_health_checks();

        self.initialized = true;

        tracing::info!(
            node_id = self.node_id,
            total_members = self.registry.len(),
            "Static cluster discovery started"
        );

        Ok(())
    }

    async fn leave(&mut self) -> Result<(), ClusterDiscoveryError> {
        tracing::info!(node_id = self.node_id, "Leaving static cluster");
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), ClusterDiscoveryError> {
        tracing::info!(node_id = self.node_id, "Shutting down static cluster discovery");

        // Stop health check task
        if let Some(tx) = self.health_check_shutdown.take() {
            let _ = tx.send(()).await;
        }
        if let Some(handle) = self.health_check_handle.take() {
            handle.abort();
        }

        self.initialized = false;
        Ok(())
    }

    fn is_initialized(&self) -> bool {
        self.initialized
    }

    fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn registry(&self) -> Arc<NodeRegistry> {
        self.registry.clone()
    }

    async fn recv_event(&mut self) -> Option<ClusterEvent> {
        self.event_rx.recv().await
    }

    fn try_recv_event(&mut self) -> Option<ClusterEvent> {
        self.event_rx.try_recv().ok()
    }

    fn members(&self) -> Vec<NodeId> {
        self.registry.all_nodes()
    }

    fn healthy_members(&self) -> Vec<NodeId> {
        self.registry.healthy_nodes()
    }

    fn get_node_addr(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.registry.get_addr(node_id)
    }

    async fn update_local_metadata(
        &self,
        metadata: NodeMetadata,
    ) -> Result<(), ClusterDiscoveryError> {
        self.registry.register(metadata);
        Ok(())
    }

    fn is_active(&self) -> bool {
        // Static discovery is considered "active" since it manages known peers
        // (unlike NoOp which does nothing)
        true
    }
}

impl std::fmt::Debug for StaticClusterDiscovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticClusterDiscovery")
            .field("node_id", &self.node_id)
            .field("raft_addr", &self.raft_addr)
            .field("peer_count", &self.config.peers.len())
            .field("initialized", &self.initialized)
            .field("health_check_enabled", &self.config.health_check_enabled)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_metadata_serialization() {
        let metadata = NodeMetadata::new(1, "127.0.0.1:9000".parse().unwrap())
            .with_tag("role", "leader");

        let bytes = metadata.to_bytes();
        let decoded = NodeMetadata::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.node_id, 1);
        assert_eq!(decoded.get_tag("role"), Some("leader"));
    }

    #[test]
    fn test_shard_leader_encoding() {
        let mut metadata = NodeMetadata::new(1, "127.0.0.1:9000".parse().unwrap());

        let mut leaders = HashMap::new();
        leaders.insert(0, ShardLeaderInfo::new(1, 10));
        leaders.insert(1, ShardLeaderInfo::new(2, 20));

        metadata.set_shard_leaders(&leaders);
        let decoded = metadata.get_shard_leaders();

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.get(&0).unwrap().leader_id, 1);
        assert_eq!(decoded.get(&1).unwrap().epoch, 20);
    }

    #[test]
    fn test_node_registry() {
        let registry = NodeRegistry::new();

        let metadata = NodeMetadata::new(1, "127.0.0.1:9000".parse().unwrap());
        assert!(registry.register(metadata.clone()));
        assert!(!registry.register(metadata)); // Second time returns false

        assert!(registry.contains(1));
        assert!(registry.is_healthy(1));

        registry.mark_failed(1);
        assert!(!registry.is_healthy(1));

        registry.mark_recovered(1);
        assert!(registry.is_healthy(1));

        registry.unregister(1);
        assert!(!registry.contains(1));
    }

    #[tokio::test]
    async fn test_noop_discovery() {
        let mut discovery = NoOpClusterDiscovery::new(1, "127.0.0.1:9000".parse().unwrap());

        assert!(!discovery.is_initialized());
        assert!(discovery.start().await.is_ok());
        assert!(discovery.is_initialized());
        assert_eq!(discovery.node_id(), 1);
        assert_eq!(discovery.members().len(), 1);
        assert!(!discovery.is_active()); // NoOp is not active
        assert!(discovery.shutdown().await.is_ok());
    }

    #[tokio::test]
    async fn test_noop_discovery_simulate_events() {
        let mut discovery = NoOpClusterDiscovery::new(1, "127.0.0.1:9000".parse().unwrap());
        discovery.start().await.unwrap();

        // Simulate node 2 joining
        discovery.simulate_join(2, "127.0.0.1:9001".parse().unwrap());

        // Check event
        let event = discovery.try_recv_event();
        assert!(matches!(event, Some(ClusterEvent::NodeJoin { node_id: 2, .. })));
        assert_eq!(discovery.members().len(), 2);

        // Simulate node 2 leaving
        discovery.simulate_leave(2);
        let event = discovery.try_recv_event();
        assert!(matches!(event, Some(ClusterEvent::NodeLeave { node_id: 2 })));
        assert_eq!(discovery.members().len(), 1);
    }

    // ==================== StaticClusterDiscovery Tests ====================

    #[test]
    fn test_static_discovery_config_default() {
        let config = StaticDiscoveryConfig::default();
        assert!(config.peers.is_empty());
        assert!(config.health_check_enabled);
        assert_eq!(config.health_check_interval, std::time::Duration::from_secs(5));
        assert_eq!(config.health_check_timeout, std::time::Duration::from_secs(2));
    }

    #[test]
    fn test_static_discovery_config_builder() {
        let config = StaticDiscoveryConfig::new(vec![
            (1, "127.0.0.1:9000".parse().unwrap()),
        ])
        .with_peer(2, "127.0.0.1:9001".parse().unwrap())
        .with_peer(3, "127.0.0.1:9002".parse().unwrap())
        .with_health_check_interval(std::time::Duration::from_secs(10))
        .with_health_check_timeout(std::time::Duration::from_secs(3))
        .without_health_checks();

        assert_eq!(config.peers.len(), 3);
        assert!(!config.health_check_enabled);
        assert_eq!(config.health_check_interval, std::time::Duration::from_secs(10));
        assert_eq!(config.health_check_timeout, std::time::Duration::from_secs(3));
    }

    #[test]
    fn test_static_discovery_config_from_addrs() {
        let config = StaticDiscoveryConfig::from_addrs(vec![
            "10.0.0.1:9000".parse().unwrap(),
            "10.0.0.2:9000".parse().unwrap(),
            "10.0.0.3:9000".parse().unwrap(),
        ]);

        assert_eq!(config.peers.len(), 3);
        // Auto-assigned IDs starting from 1
        assert_eq!(config.peers[0], (1, "10.0.0.1:9000".parse().unwrap()));
        assert_eq!(config.peers[1], (2, "10.0.0.2:9000".parse().unwrap()));
        assert_eq!(config.peers[2], (3, "10.0.0.3:9000".parse().unwrap()));
    }

    #[tokio::test]
    async fn test_static_discovery_basic() {
        let config = StaticDiscoveryConfig::new(vec![
            (1, "127.0.0.1:9000".parse().unwrap()),
            (2, "127.0.0.1:9001".parse().unwrap()),
        ])
        .without_health_checks();

        let mut discovery =
            StaticClusterDiscovery::new(1, "127.0.0.1:9000".parse().unwrap(), config);

        assert!(!discovery.is_initialized());
        assert_eq!(discovery.node_id(), 1);
        assert!(discovery.is_active());

        discovery.start().await.unwrap();

        assert!(discovery.is_initialized());
        assert_eq!(discovery.members().len(), 2);
        assert_eq!(discovery.healthy_members().len(), 2);

        // Check peer address
        assert_eq!(
            discovery.get_node_addr(2),
            Some("127.0.0.1:9001".parse().unwrap())
        );

        discovery.shutdown().await.unwrap();
        assert!(!discovery.is_initialized());
    }

    #[tokio::test]
    async fn test_static_discovery_emits_join_events() {
        let config = StaticDiscoveryConfig::new(vec![
            (1, "127.0.0.1:9000".parse().unwrap()),
            (2, "127.0.0.1:9001".parse().unwrap()),
            (3, "127.0.0.1:9002".parse().unwrap()),
        ])
        .without_health_checks();

        let mut discovery =
            StaticClusterDiscovery::new(1, "127.0.0.1:9000".parse().unwrap(), config);

        discovery.start().await.unwrap();

        // Should receive join events for peers 2 and 3 (not self)
        let mut join_ids = Vec::new();
        while let Some(event) = discovery.try_recv_event() {
            if let ClusterEvent::NodeJoin { node_id, .. } = event {
                join_ids.push(node_id);
            }
        }

        assert_eq!(join_ids.len(), 2);
        assert!(join_ids.contains(&2));
        assert!(join_ids.contains(&3));
    }

    #[tokio::test]
    async fn test_static_discovery_add_peer() {
        let config = StaticDiscoveryConfig::new(vec![
            (1, "127.0.0.1:9000".parse().unwrap()),
        ])
        .without_health_checks();

        let mut discovery =
            StaticClusterDiscovery::new(1, "127.0.0.1:9000".parse().unwrap(), config);

        discovery.start().await.unwrap();
        assert_eq!(discovery.members().len(), 1);

        // Add peer dynamically
        discovery.add_peer(2, "127.0.0.1:9001".parse().unwrap());

        assert_eq!(discovery.members().len(), 2);
        assert_eq!(
            discovery.get_node_addr(2),
            Some("127.0.0.1:9001".parse().unwrap())
        );

        // Check event was emitted
        let event = discovery.try_recv_event();
        assert!(matches!(event, Some(ClusterEvent::NodeJoin { node_id: 2, .. })));
    }

    #[tokio::test]
    async fn test_static_discovery_remove_peer() {
        let config = StaticDiscoveryConfig::new(vec![
            (1, "127.0.0.1:9000".parse().unwrap()),
            (2, "127.0.0.1:9001".parse().unwrap()),
        ])
        .without_health_checks();

        let mut discovery =
            StaticClusterDiscovery::new(1, "127.0.0.1:9000".parse().unwrap(), config);

        discovery.start().await.unwrap();
        // Drain join events
        while discovery.try_recv_event().is_some() {}

        assert_eq!(discovery.members().len(), 2);

        // Remove peer
        discovery.remove_peer(2);

        assert_eq!(discovery.members().len(), 1);
        assert_eq!(discovery.get_node_addr(2), None);

        // Check event was emitted
        let event = discovery.try_recv_event();
        assert!(matches!(event, Some(ClusterEvent::NodeLeave { node_id: 2 })));
    }

    #[tokio::test]
    async fn test_static_discovery_from_peers() {
        let peers = vec![
            (10, "10.0.0.10:9000".parse().unwrap()),
            (20, "10.0.0.20:9000".parse().unwrap()),
            (30, "10.0.0.30:9000".parse().unwrap()),
        ];

        let mut discovery =
            StaticClusterDiscovery::from_peers(10, "10.0.0.10:9000".parse().unwrap(), peers);

        discovery.start().await.unwrap();

        assert_eq!(discovery.members().len(), 3);
        assert_eq!(
            discovery.get_node_addr(20),
            Some("10.0.0.20:9000".parse().unwrap())
        );
        assert_eq!(
            discovery.get_node_addr(30),
            Some("10.0.0.30:9000".parse().unwrap())
        );

        discovery.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_static_discovery_update_metadata() {
        let config = StaticDiscoveryConfig::new(vec![
            (1, "127.0.0.1:9000".parse().unwrap()),
        ])
        .without_health_checks();

        let mut discovery =
            StaticClusterDiscovery::new(1, "127.0.0.1:9000".parse().unwrap(), config);

        discovery.start().await.unwrap();

        // Update local metadata
        let metadata = NodeMetadata::new(1, "127.0.0.1:9000".parse().unwrap())
            .with_tag("role", "leader");

        discovery.update_local_metadata(metadata).await.unwrap();

        // Verify metadata was updated
        let registry = discovery.registry();
        let stored = registry.get_metadata(1).unwrap();
        assert_eq!(stored.get_tag("role"), Some("leader"));
    }

    #[test]
    fn test_static_discovery_debug() {
        let config = StaticDiscoveryConfig::new(vec![
            (1, "127.0.0.1:9000".parse().unwrap()),
            (2, "127.0.0.1:9001".parse().unwrap()),
        ])
        .without_health_checks();

        let discovery =
            StaticClusterDiscovery::new(1, "127.0.0.1:9000".parse().unwrap(), config);

        let debug_str = format!("{:?}", discovery);
        assert!(debug_str.contains("StaticClusterDiscovery"));
        assert!(debug_str.contains("node_id: 1"));
        assert!(debug_str.contains("peer_count: 2"));
    }
}
