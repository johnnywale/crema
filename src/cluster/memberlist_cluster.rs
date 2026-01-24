//! Memberlist-based cluster discovery and membership management.
//!
//! This module integrates the `memberlist` crate for gossip-based node discovery
//! with the Raft consensus layer. It implements the two-tier membership architecture:
//! - Layer 1 (Memberlist): Automatic node discovery via SWIM gossip protocol
//! - Layer 2 (Raft): Manual voting membership changes
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    MemberlistCluster                            │
//! │  ┌───────────────────────────────────────────────────────────┐ │
//! │  │              Memberlist (Gossip Layer)                     │ │
//! │  │  - Node discovery via SWIM protocol                       │ │
//! │  │  - Health monitoring and failure detection                │ │
//! │  │  - Metadata broadcast (raft_id, raft_addr)                │ │
//! │  └───────────────────────────────────────────────────────────┘ │
//! │                             │                                   │
//! │                             ▼                                   │
//! │  ┌───────────────────────────────────────────────────────────┐ │
//! │  │              NodeRegistry                                  │ │
//! │  │  - Maps Raft NodeID → SocketAddr                          │ │
//! │  │  - Tracks node metadata and health                        │ │
//! │  └───────────────────────────────────────────────────────────┘ │
//! │                             │                                   │
//! │                             ▼                                   │
//! │  ┌───────────────────────────────────────────────────────────┐ │
//! │  │              Event System                                  │ │
//! │  │  - Join/Leave/Update events                               │ │
//! │  │  - Integration with ClusterMembership                     │ │
//! │  └───────────────────────────────────────────────────────────┘ │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use memberlist::delegate::{CompositeDelegate, EventDelegate, NodeDelegate};
use memberlist::net::resolver::socket_addr::SocketAddrResolver;
use memberlist::net::stream_layer::tcp::Tcp;
use memberlist::net::{NetTransport, NetTransportOptions};
use memberlist::proto::Meta;
use memberlist::tokio::TokioRuntime;
use memberlist::{Memberlist, Options};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::cluster::discovery::{
    ClusterDiscovery, ClusterDiscoveryError, ClusterEvent, NodeMetadata as GenericNodeMetadata,
};
use crate::types::NodeId;

/// Type alias for the transport layer
type Transport = NetTransport<
    SmolStr,
    SocketAddrResolver<TokioRuntime>,
    Tcp<TokioRuntime>,
    TokioRuntime,
>;

/// Type alias for the delegate
/// CompositeDelegate<I, Address, A, C, E, M, N, P>:
/// - A: Main delegate (VoidDelegate)
/// - C: Conflict delegate (VoidDelegate)
/// - E: Event delegate (MemberlistEventDelegate)
/// - M: Merge delegate (VoidDelegate)
/// - N: Node delegate (MemberlistNodeDelegate) - provides node metadata
/// - P: Ping delegate (VoidDelegate)
type MemberlistDelegate = CompositeDelegate<
    SmolStr,
    SocketAddr,
    memberlist::delegate::VoidDelegate<SmolStr, SocketAddr>, // A: Main delegate
    memberlist::delegate::VoidDelegate<SmolStr, SocketAddr>, // C: Conflict delegate
    MemberlistEventDelegate,                                  // E: Event delegate
    memberlist::delegate::VoidDelegate<SmolStr, SocketAddr>, // M: Merge delegate
    MemberlistNodeDelegate,                                   // N: Node delegate
    memberlist::delegate::VoidDelegate<SmolStr, SocketAddr>, // P: Ping delegate
>;

/// Metadata broadcast via memberlist gossip.
/// This is serialized and included in memberlist's node metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftNodeMetadata {
    /// Raft node ID (u64)
    pub raft_id: NodeId,
    /// Address for Raft RPC communication
    pub raft_addr: SocketAddr,
    /// Node version for compatibility checking
    pub version: String,
    /// Optional tags for node classification
    pub tags: HashMap<String, String>,
}

/// Shard leader information with epoch for ordering out-of-order gossip updates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardLeaderInfo {
    /// The leader's node ID.
    pub leader_id: NodeId,
    /// Monotonically increasing epoch for version ordering.
    /// Higher epochs supersede lower ones for the same shard.
    pub epoch: u64,
}

impl ShardLeaderInfo {
    /// Create a new shard leader info.
    pub fn new(leader_id: NodeId, epoch: u64) -> Self {
        Self { leader_id, epoch }
    }
}

impl RaftNodeMetadata {
    pub fn new(raft_id: NodeId, raft_addr: SocketAddr) -> Self {
        Self {
            raft_id,
            raft_addr,
            version: env!("CARGO_PKG_VERSION").to_string(),
            tags: HashMap::new(),
        }
    }

    pub fn with_tag(mut self, key: &str, value: &str) -> Self {
        self.tags.insert(key.to_string(), value.to_string());
        self
    }

    /// Serialize metadata to bytes for memberlist
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_default()
    }

    /// Deserialize metadata from bytes
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }

    /// Set shard leaders with epoch information.
    ///
    /// Encodes as: "shard_id:leader_id:epoch,..." under the key "multiraft.shard_leaders".
    /// This format is compact to fit within memberlist's 512-byte metadata limit.
    pub fn set_shard_leaders(&mut self, leaders: &HashMap<u32, ShardLeaderInfo>) {
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

    /// Get shard leaders from metadata.
    ///
    /// Decodes the "multiraft.shard_leaders" tag.
    /// Returns an empty map if the tag is missing or malformed.
    /// Tolerates individual malformed entries (skips them).
    pub fn get_shard_leaders(&self) -> HashMap<u32, ShardLeaderInfo> {
        self.tags
            .get("multiraft.shard_leaders")
            .map(|s| parse_shard_leaders(s))
            .unwrap_or_default()
    }

    /// Check if this node has Multi-Raft shard leader information.
    pub fn has_shard_leaders(&self) -> bool {
        self.tags.contains_key("multiraft.shard_leaders")
    }
}

/// Parse shard leaders from the encoded string format.
///
/// Format: "shard_id:leader_id:epoch,..."
/// Tolerates malformed entries by skipping them.
fn parse_shard_leaders(encoded: &str) -> HashMap<u32, ShardLeaderInfo> {
    let mut result = HashMap::new();

    if encoded.is_empty() {
        return result;
    }

    for entry in encoded.split(',') {
        let parts: Vec<&str> = entry.split(':').collect();
        if parts.len() != 3 {
            // Skip malformed entries
            tracing::debug!(entry = entry, "Skipping malformed shard leader entry");
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

/// Events emitted by the memberlist cluster
#[derive(Debug, Clone)]
pub enum MemberlistEvent {
    /// A node joined the cluster
    NodeJoin {
        raft_id: NodeId,
        raft_addr: SocketAddr,
        metadata: RaftNodeMetadata,
    },
    /// A node left the cluster gracefully
    NodeLeave { raft_id: NodeId },
    /// A node was detected as failed
    NodeFailed { raft_id: NodeId },
    /// A node's metadata was updated
    NodeUpdate {
        raft_id: NodeId,
        metadata: RaftNodeMetadata,
    },
}

/// Node state tracked by the registry
#[derive(Debug, Clone)]
struct TrackedNode {
    metadata: RaftNodeMetadata,
    memberlist_id: SmolStr,
    first_seen: Instant,
    last_seen: Instant,
    is_healthy: bool,
}

impl TrackedNode {
    fn new(metadata: RaftNodeMetadata, memberlist_id: SmolStr) -> Self {
        let now = Instant::now();
        Self {
            metadata,
            memberlist_id,
            first_seen: now,
            last_seen: now,
            is_healthy: true,
        }
    }
}

/// Registry mapping Raft NodeIDs to their network addresses and metadata.
/// This is the bridge between memberlist discovery and Raft transport.
#[derive(Debug, Default)]
pub struct NodeRegistry {
    /// Maps Raft NodeID → node state
    nodes: RwLock<HashMap<NodeId, TrackedNode>>,
    /// Maps memberlist ID → Raft NodeID for reverse lookup
    memberlist_to_raft: RwLock<HashMap<SmolStr, NodeId>>,
}

impl NodeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a node from memberlist discovery
    pub fn register(&self, metadata: RaftNodeMetadata, memberlist_id: SmolStr) -> bool {
        let raft_id = metadata.raft_id;
        let mut nodes = self.nodes.write();
        let mut ml_to_raft = self.memberlist_to_raft.write();

        if nodes.contains_key(&raft_id) {
            // Update existing node
            if let Some(node) = nodes.get_mut(&raft_id) {
                node.metadata = metadata;
                node.last_seen = Instant::now();
                node.is_healthy = true;
                node.memberlist_id = memberlist_id.clone();
            }
            ml_to_raft.insert(memberlist_id, raft_id);
            false // Not a new node
        } else {
            // New node
            ml_to_raft.insert(memberlist_id.clone(), raft_id);
            nodes.insert(raft_id, TrackedNode::new(metadata, memberlist_id));
            true // Is a new node
        }
    }

    /// Register a node by metadata only (for self-registration)
    pub fn register_self(&self, metadata: RaftNodeMetadata, memberlist_id: SmolStr) {
        let raft_id = metadata.raft_id;
        let mut nodes = self.nodes.write();
        let mut ml_to_raft = self.memberlist_to_raft.write();

        ml_to_raft.insert(memberlist_id.clone(), raft_id);
        nodes.insert(raft_id, TrackedNode::new(metadata, memberlist_id));
    }

    /// Get Raft ID from memberlist ID
    pub fn get_raft_id(&self, memberlist_id: &SmolStr) -> Option<NodeId> {
        self.memberlist_to_raft.read().get(memberlist_id).copied()
    }

    /// Mark a node as unhealthy (failed)
    pub fn mark_failed(&self, raft_id: NodeId) {
        if let Some(node) = self.nodes.write().get_mut(&raft_id) {
            node.is_healthy = false;
        }
    }

    /// Remove a node from the registry
    pub fn unregister(&self, raft_id: NodeId) -> Option<RaftNodeMetadata> {
        let mut nodes = self.nodes.write();
        let mut ml_to_raft = self.memberlist_to_raft.write();

        if let Some(node) = nodes.remove(&raft_id) {
            ml_to_raft.remove(&node.memberlist_id);
            Some(node.metadata)
        } else {
            None
        }
    }

    /// Remove a node by memberlist ID
    pub fn unregister_by_memberlist_id(&self, memberlist_id: &SmolStr) -> Option<RaftNodeMetadata> {
        let raft_id = self.memberlist_to_raft.read().get(memberlist_id).copied()?;
        self.unregister(raft_id)
    }

    /// Get the Raft address for a node
    pub fn get_addr(&self, raft_id: NodeId) -> Option<SocketAddr> {
        self.nodes.read().get(&raft_id).map(|n| n.metadata.raft_addr)
    }

    /// Get full metadata for a node
    pub fn get_metadata(&self, raft_id: NodeId) -> Option<RaftNodeMetadata> {
        self.nodes.read().get(&raft_id).map(|n| n.metadata.clone())
    }

    /// Check if a node is registered
    pub fn contains(&self, raft_id: NodeId) -> bool {
        self.nodes.read().contains_key(&raft_id)
    }

    /// Check if a node is healthy
    pub fn is_healthy(&self, raft_id: NodeId) -> bool {
        self.nodes
            .read()
            .get(&raft_id)
            .map(|n| n.is_healthy)
            .unwrap_or(false)
    }

    /// Get all registered node IDs
    pub fn all_nodes(&self) -> Vec<NodeId> {
        self.nodes.read().keys().copied().collect()
    }

    /// Get all healthy node IDs
    pub fn healthy_nodes(&self) -> Vec<NodeId> {
        self.nodes
            .read()
            .iter()
            .filter(|(_, n)| n.is_healthy)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get all node addresses (for Raft transport)
    pub fn all_addresses(&self) -> HashMap<NodeId, SocketAddr> {
        self.nodes
            .read()
            .iter()
            .map(|(id, n)| (*id, n.metadata.raft_addr))
            .collect()
    }

    /// Get the number of registered nodes
    pub fn len(&self) -> usize {
        self.nodes.read().len()
    }

    /// Check if registry is empty
    pub fn is_empty(&self) -> bool {
        self.nodes.read().is_empty()
    }
}

/// Configuration for memberlist cluster
#[derive(Debug, Clone)]
pub struct MemberlistClusterConfig {
    /// This node's Raft ID
    pub raft_id: NodeId,
    /// Address to bind for memberlist gossip
    pub bind_addr: SocketAddr,
    /// Address to advertise to other nodes
    pub advertise_addr: Option<SocketAddr>,
    /// Address for Raft RPC
    pub raft_addr: SocketAddr,
    /// Seed nodes to join initially
    pub seed_nodes: Vec<SocketAddr>,
    /// Node name (defaults to "node-{raft_id}")
    pub node_name: Option<String>,
}

impl MemberlistClusterConfig {
    pub fn new(raft_id: NodeId, bind_addr: SocketAddr, raft_addr: SocketAddr) -> Self {
        Self {
            raft_id,
            bind_addr,
            advertise_addr: None,
            raft_addr,
            seed_nodes: Vec::new(),
            node_name: None,
        }
    }

    pub fn with_seed_nodes(mut self, seeds: Vec<SocketAddr>) -> Self {
        self.seed_nodes = seeds;
        self
    }

    pub fn with_advertise_addr(mut self, addr: SocketAddr) -> Self {
        self.advertise_addr = Some(addr);
        self
    }

    pub fn with_node_name(mut self, name: String) -> Self {
        self.node_name = Some(name);
        self
    }

    pub fn node_name(&self) -> String {
        self.node_name
            .clone()
            .unwrap_or_else(|| format!("node-{}", self.raft_id))
    }
}

/// Delegate for handling memberlist events
pub struct MemberlistEventDelegate {
    this_node_id: NodeId,
    registry: Arc<NodeRegistry>,
    event_tx: mpsc::UnboundedSender<MemberlistEvent>,
}

impl MemberlistEventDelegate {
    fn new(
        this_node_id: NodeId,
        registry: Arc<NodeRegistry>,
        event_tx: mpsc::UnboundedSender<MemberlistEvent>,
    ) -> Self {
        Self {
            this_node_id,
            registry,
            event_tx,
        }
    }

    fn handle_node_join(&self, node_id: &SmolStr, meta: &[u8]) {
        if let Some(metadata) = RaftNodeMetadata::from_bytes(meta) {
            if metadata.raft_id == self.this_node_id {
                return; // Ignore self
            }

            let is_new = self.registry.register(metadata.clone(), node_id.clone());

            if is_new {
                info!(raft_id = metadata.raft_id, "Node joined via memberlist");
                let _ = self.event_tx.send(MemberlistEvent::NodeJoin {
                    raft_id: metadata.raft_id,
                    raft_addr: metadata.raft_addr,
                    metadata,
                });
            }
        }
    }

    fn handle_node_leave(&self, node_id: &SmolStr) {
        if let Some(raft_id) = self.registry.get_raft_id(node_id) {
            if raft_id == self.this_node_id {
                return;
            }

            self.registry.unregister(raft_id);
            info!(raft_id, "Node left via memberlist");
            let _ = self.event_tx.send(MemberlistEvent::NodeLeave { raft_id });
        }
    }

    fn handle_node_update(&self, node_id: &SmolStr, meta: &[u8]) {
        if let Some(metadata) = RaftNodeMetadata::from_bytes(meta) {
            if metadata.raft_id == self.this_node_id {
                return;
            }

            self.registry.register(metadata.clone(), node_id.clone());
            debug!(raft_id = metadata.raft_id, "Node updated via memberlist");
            let _ = self.event_tx.send(MemberlistEvent::NodeUpdate {
                raft_id: metadata.raft_id,
                metadata,
            });
        }
    }
}

// Implement the EventDelegate trait from memberlist
impl EventDelegate for MemberlistEventDelegate {
    type Id = SmolStr;
    type Address = SocketAddr;

    fn notify_join(
        &self,
        node: Arc<memberlist::proto::NodeState<Self::Id, Self::Address>>,
    ) -> impl Future<Output = ()> + Send {
        let node_id = node.id().clone();
        let meta = node.meta().to_vec();
        self.handle_node_join(&node_id, &meta);
        async {}
    }

    fn notify_leave(
        &self,
        node: Arc<memberlist::proto::NodeState<Self::Id, Self::Address>>,
    ) -> impl Future<Output = ()> + Send {
        let node_id = node.id().clone();
        self.handle_node_leave(&node_id);
        async {}
    }

    fn notify_update(
        &self,
        node: Arc<memberlist::proto::NodeState<Self::Id, Self::Address>>,
    ) -> impl Future<Output = ()> + Send {
        let node_id = node.id().clone();
        let meta = node.meta().to_vec();
        self.handle_node_update(&node_id, &meta);
        async {}
    }
}

/// Delegate for providing local node metadata to memberlist.
/// This metadata is broadcast to other nodes during gossip.
pub struct MemberlistNodeDelegate {
    /// The local node's metadata (serialized)
    local_meta: Meta,
}

impl MemberlistNodeDelegate {
    fn new(metadata: RaftNodeMetadata) -> Self {
        let bytes = metadata.to_bytes();
        // Meta has a max size of 512 bytes, our metadata should fit
        let meta = Meta::try_from(bytes).unwrap_or_else(|_| Meta::empty());
        Self { local_meta: meta }
    }
}

// Implement the NodeDelegate trait to provide node metadata
impl NodeDelegate for MemberlistNodeDelegate {
    fn node_meta(&self, _limit: usize) -> impl Future<Output = Meta> + Send {
        let meta = self.local_meta.clone();
        async move { meta }
    }

    fn notify_message(&self, _msg: Cow<'_, [u8]>) -> impl Future<Output = ()> + Send {
        async {}
    }

    fn broadcast_messages<F>(
        &self,
        _limit: usize,
        _encoded_len: F,
    ) -> impl Future<Output = impl Iterator<Item = bytes::Bytes> + Send> + Send
    where
        F: Fn(bytes::Bytes) -> (usize, bytes::Bytes) + Send + Sync + 'static,
    {
        async { std::iter::empty() }
    }

    fn local_state(&self, _join: bool) -> impl Future<Output = bytes::Bytes> + Send {
        async { bytes::Bytes::new() }
    }

    fn merge_remote_state(&self, _buf: &[u8], _join: bool) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// Memberlist-based cluster for node discovery and health monitoring
pub struct MemberlistCluster {
    config: MemberlistClusterConfig,
    registry: Arc<NodeRegistry>,
    event_rx: mpsc::UnboundedReceiver<MemberlistEvent>,
    event_tx: mpsc::UnboundedSender<MemberlistEvent>,
    memberlist: Option<Arc<Memberlist<Transport, MemberlistDelegate>>>,
}

impl MemberlistCluster {
    /// Create a new memberlist cluster (not yet started)
    pub fn new(config: MemberlistClusterConfig) -> Self {
        let registry = Arc::new(NodeRegistry::new());
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        // Register self
        let self_metadata = RaftNodeMetadata::new(config.raft_id, config.raft_addr);
        let self_id: SmolStr = config.node_name().into();
        registry.register_self(self_metadata, self_id);

        Self {
            config,
            registry,
            event_rx,
            event_tx,
            memberlist: None,
        }
    }

    /// Get the node registry for Raft transport integration
    pub fn registry(&self) -> Arc<NodeRegistry> {
        self.registry.clone()
    }

    /// Get this node's Raft ID
    pub fn raft_id(&self) -> NodeId {
        self.config.raft_id
    }

    /// Receive the next memberlist event
    pub async fn recv_event(&mut self) -> Option<MemberlistEvent> {
        self.event_rx.recv().await
    }

    /// Try to receive an event without blocking
    pub fn try_recv_event(&mut self) -> Option<MemberlistEvent> {
        self.event_rx.try_recv().ok()
    }

    /// Check if memberlist is initialized
    pub fn is_initialized(&self) -> bool {
        self.memberlist.is_some()
    }

    /// Start the memberlist cluster (joins the gossip network)
    pub async fn start(&mut self) -> Result<(), MemberlistError> {
        info!(
            raft_id = self.config.raft_id,
            bind_addr = %self.config.bind_addr,
            "Starting memberlist cluster"
        );

        // Create the node delegate for providing local metadata
        let local_metadata = RaftNodeMetadata::new(self.config.raft_id, self.config.raft_addr);
        let node_delegate = MemberlistNodeDelegate::new(local_metadata);

        // Create the event delegate for handling join/leave/update events
        let event_delegate = MemberlistEventDelegate::new(
            self.config.raft_id,
            self.registry.clone(),
            self.event_tx.clone(),
        );

        // Create composite delegate with node metadata and event handling
        let delegate: MemberlistDelegate = CompositeDelegate::new()
            .with_node_delegate(node_delegate)
            .with_event_delegate(event_delegate);

        // Configure network transport
        let node_name: SmolStr = self.config.node_name().into();
        let mut transport_opts = NetTransportOptions::<
            SmolStr,
            SocketAddrResolver<TokioRuntime>,
            Tcp<TokioRuntime>,
        >::new(node_name);
        transport_opts.add_bind_address(self.config.bind_addr.into());

        if let Some(advertise_addr) = self.config.advertise_addr {
            transport_opts = transport_opts.with_advertise_address(advertise_addr.into());
        }

        // Configure memberlist options
        let ml_opts = Options::local();

        // Create memberlist
        let memberlist = Memberlist::with_delegate(delegate, transport_opts, ml_opts)
            .await
            .map_err(|e| MemberlistError::BindError(e.to_string()))?;

        // Join seed nodes if provided
        if !self.config.seed_nodes.is_empty() {
            info!(
                seeds = ?self.config.seed_nodes,
                "Joining seed nodes"
            );
            for seed in &self.config.seed_nodes {
                let node = memberlist::transport::Node::new(
                    format!("seed-{}", seed).into(),
                    memberlist::proto::MaybeResolvedAddress::Resolved(*seed),
                );
                match memberlist.join(node).await {
                    Ok(_) => {
                        info!(seed = %seed, "Successfully joined seed node");
                    }
                    Err(e) => {
                        warn!(seed = %seed, error = %e, "Failed to join seed node");
                    }
                }
            }
        }

        self.memberlist = Some(Arc::new(memberlist));

        Ok(())
    }

    /// Gracefully leave the cluster
    pub async fn leave(&mut self) -> Result<(), MemberlistError> {
        info!(raft_id = self.config.raft_id, "Leaving memberlist cluster");

        if let Some(memberlist) = self.memberlist.take() {
            memberlist
                .leave(std::time::Duration::from_secs(5))
                .await
                .map_err(|e| MemberlistError::TransportError(e.to_string()))?;
        }

        Ok(())
    }

    /// Shutdown the memberlist completely
    pub async fn shutdown(&mut self) -> Result<(), MemberlistError> {
        info!(
            raft_id = self.config.raft_id,
            "Shutting down memberlist cluster"
        );

        if let Some(memberlist) = self.memberlist.take() {
            memberlist
                .shutdown()
                .await
                .map_err(|e| MemberlistError::TransportError(e.to_string()))?;
        }

        Ok(())
    }

    /// Get all known nodes
    pub fn members(&self) -> Vec<NodeId> {
        self.registry.all_nodes()
    }

    /// Get all healthy members
    pub fn healthy_members(&self) -> Vec<NodeId> {
        self.registry.healthy_nodes()
    }

    /// Get address for a specific node
    pub fn get_node_addr(&self, raft_id: NodeId) -> Option<SocketAddr> {
        self.registry.get_addr(raft_id)
    }

    /// Get the underlying memberlist instance
    pub fn memberlist(&self) -> Option<&Arc<Memberlist<Transport, MemberlistDelegate>>> {
        self.memberlist.as_ref()
    }

    /// Manually trigger a node join (for testing)
    pub fn simulate_join(&self, metadata: RaftNodeMetadata) {
        let memberlist_id: SmolStr = format!("node-{}", metadata.raft_id).into();
        let is_new = self.registry.register(metadata.clone(), memberlist_id);
        if is_new {
            let _ = self.event_tx.send(MemberlistEvent::NodeJoin {
                raft_id: metadata.raft_id,
                raft_addr: metadata.raft_addr,
                metadata,
            });
        }
    }

    /// Manually trigger a node leave (for testing)
    pub fn simulate_leave(&self, raft_id: NodeId) {
        self.registry.unregister(raft_id);
        let _ = self.event_tx.send(MemberlistEvent::NodeLeave { raft_id });
    }

    /// Convert a MemberlistEvent to a ClusterEvent
    fn to_cluster_event(event: MemberlistEvent) -> ClusterEvent {
        match event {
            MemberlistEvent::NodeJoin { raft_id, raft_addr, metadata } => {
                ClusterEvent::NodeJoin {
                    node_id: raft_id,
                    raft_addr,
                    metadata: GenericNodeMetadata {
                        node_id: metadata.raft_id,
                        raft_addr: metadata.raft_addr,
                        version: metadata.version,
                        tags: metadata.tags,
                    },
                }
            }
            MemberlistEvent::NodeLeave { raft_id } => {
                ClusterEvent::NodeLeave { node_id: raft_id }
            }
            MemberlistEvent::NodeFailed { raft_id } => {
                ClusterEvent::NodeFailed { node_id: raft_id }
            }
            MemberlistEvent::NodeUpdate { raft_id, metadata } => {
                ClusterEvent::NodeUpdate {
                    node_id: raft_id,
                    metadata: GenericNodeMetadata {
                        node_id: metadata.raft_id,
                        raft_addr: metadata.raft_addr,
                        version: metadata.version,
                        tags: metadata.tags,
                    },
                }
            }
        }
    }
}

/// Implement the ClusterDiscovery trait for MemberlistCluster
#[async_trait::async_trait]
impl ClusterDiscovery for MemberlistCluster {
    async fn start(&mut self) -> Result<(), ClusterDiscoveryError> {
        MemberlistCluster::start(self)
            .await
            .map_err(|e| ClusterDiscoveryError::Other(e.to_string()))
    }

    async fn leave(&mut self) -> Result<(), ClusterDiscoveryError> {
        MemberlistCluster::leave(self)
            .await
            .map_err(|e| ClusterDiscoveryError::Other(e.to_string()))
    }

    async fn shutdown(&mut self) -> Result<(), ClusterDiscoveryError> {
        MemberlistCluster::shutdown(self)
            .await
            .map_err(|e| ClusterDiscoveryError::Other(e.to_string()))
    }

    fn is_initialized(&self) -> bool {
        MemberlistCluster::is_initialized(self)
    }

    fn node_id(&self) -> NodeId {
        self.raft_id()
    }

    fn registry(&self) -> Arc<crate::cluster::discovery::NodeRegistry> {
        // Create a new generic NodeRegistry from our memberlist-specific one
        let generic_registry = Arc::new(crate::cluster::discovery::NodeRegistry::new());

        // Copy nodes from memberlist registry to generic registry
        for node_id in self.registry.all_nodes() {
            if let Some(metadata) = self.registry.get_metadata(node_id) {
                let generic_metadata = GenericNodeMetadata {
                    node_id: metadata.raft_id,
                    raft_addr: metadata.raft_addr,
                    version: metadata.version,
                    tags: metadata.tags,
                };
                generic_registry.register(generic_metadata);

                if !self.registry.is_healthy(node_id) {
                    generic_registry.mark_failed(node_id);
                }
            }
        }

        generic_registry
    }

    async fn recv_event(&mut self) -> Option<ClusterEvent> {
        self.event_rx.recv().await.map(Self::to_cluster_event)
    }

    fn try_recv_event(&mut self) -> Option<ClusterEvent> {
        self.event_rx.try_recv().ok().map(Self::to_cluster_event)
    }

    fn members(&self) -> Vec<NodeId> {
        MemberlistCluster::members(self)
    }

    fn healthy_members(&self) -> Vec<NodeId> {
        MemberlistCluster::healthy_members(self)
    }

    fn get_node_addr(&self, node_id: NodeId) -> Option<SocketAddr> {
        MemberlistCluster::get_node_addr(self, node_id)
    }

    async fn update_local_metadata(&self, _metadata: GenericNodeMetadata) -> Result<(), ClusterDiscoveryError> {
        // Memberlist doesn't support dynamic metadata updates after start
        // The metadata is set at initialization time
        Ok(())
    }
}

/// Errors that can occur in memberlist cluster operations
#[derive(Debug, thiserror::Error)]
pub enum MemberlistError {
    #[error("Failed to bind to address: {0}")]
    BindError(String),

    #[error("Failed to join cluster: {0}")]
    JoinError(String),

    #[error("Memberlist transport error: {0}")]
    TransportError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_node_metadata_serialization() {
        let meta = RaftNodeMetadata::new(1, "127.0.0.1:9001".parse().unwrap())
            .with_tag("role", "voter")
            .with_tag("region", "us-east");

        let bytes = meta.to_bytes();
        let decoded = RaftNodeMetadata::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.raft_id, 1);
        assert_eq!(decoded.raft_addr, "127.0.0.1:9001".parse().unwrap());
        assert_eq!(decoded.tags.get("role"), Some(&"voter".to_string()));
        assert_eq!(decoded.tags.get("region"), Some(&"us-east".to_string()));
    }

    #[test]
    fn test_node_registry_basic() {
        let registry = NodeRegistry::new();

        let meta1 = RaftNodeMetadata::new(1, "127.0.0.1:9001".parse().unwrap());
        let meta2 = RaftNodeMetadata::new(2, "127.0.0.1:9002".parse().unwrap());

        assert!(registry.register(meta1.clone(), "node-1".into()));
        assert!(registry.register(meta2.clone(), "node-2".into()));
        assert!(!registry.register(meta1, "node-1".into())); // Already exists

        assert_eq!(registry.len(), 2);
        assert!(registry.contains(1));
        assert!(registry.contains(2));
        assert!(!registry.contains(3));

        assert_eq!(
            registry.get_addr(1),
            Some("127.0.0.1:9001".parse().unwrap())
        );
        assert!(registry.is_healthy(1));

        registry.mark_failed(1);
        assert!(!registry.is_healthy(1));

        registry.unregister(1);
        assert!(!registry.contains(1));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_node_registry_healthy_nodes() {
        let registry = NodeRegistry::new();

        for i in 1..=5 {
            let meta = RaftNodeMetadata::new(i, format!("127.0.0.1:900{}", i).parse().unwrap());
            registry.register(meta, format!("node-{}", i).into());
        }

        assert_eq!(registry.healthy_nodes().len(), 5);

        registry.mark_failed(2);
        registry.mark_failed(4);

        let healthy = registry.healthy_nodes();
        assert_eq!(healthy.len(), 3);
        assert!(healthy.contains(&1));
        assert!(healthy.contains(&3));
        assert!(healthy.contains(&5));
    }

    #[test]
    fn test_node_registry_memberlist_id_lookup() {
        let registry = NodeRegistry::new();

        let meta = RaftNodeMetadata::new(42, "127.0.0.1:9042".parse().unwrap());
        registry.register(meta, "my-node".into());

        assert_eq!(registry.get_raft_id(&"my-node".into()), Some(42));
        assert_eq!(registry.get_raft_id(&"unknown".into()), None);
    }

    #[tokio::test]
    async fn test_memberlist_cluster_creation() {
        let config = MemberlistClusterConfig::new(
            1,
            "127.0.0.1:7001".parse().unwrap(),
            "127.0.0.1:9001".parse().unwrap(),
        );

        let cluster = MemberlistCluster::new(config);

        assert_eq!(cluster.raft_id(), 1);
        assert!(cluster.registry().contains(1)); // Self is registered
        assert!(!cluster.is_initialized());
    }

    #[tokio::test]
    async fn test_memberlist_cluster_simulate_events() {
        let config = MemberlistClusterConfig::new(
            1,
            "127.0.0.1:7001".parse().unwrap(),
            "127.0.0.1:9001".parse().unwrap(),
        );

        let mut cluster = MemberlistCluster::new(config);

        // Simulate node 2 joining
        let meta2 = RaftNodeMetadata::new(2, "127.0.0.1:9002".parse().unwrap());
        cluster.simulate_join(meta2);

        // Check event
        let event = cluster.try_recv_event();
        assert!(matches!(
            event,
            Some(MemberlistEvent::NodeJoin { raft_id: 2, .. })
        ));

        // Check registry
        assert!(cluster.registry().contains(2));
        assert_eq!(cluster.members().len(), 2);

        // Simulate node 2 leaving
        cluster.simulate_leave(2);

        let event = cluster.try_recv_event();
        assert!(matches!(
            event,
            Some(MemberlistEvent::NodeLeave { raft_id: 2 })
        ));
        assert!(!cluster.registry().contains(2));
    }

    #[tokio::test]
    async fn test_memberlist_cluster_config_builder() {
        let config = MemberlistClusterConfig::new(
            2,
            "127.0.0.1:7002".parse().unwrap(),
            "127.0.0.1:9002".parse().unwrap(),
        )
        .with_seed_nodes(vec!["127.0.0.1:7001".parse().unwrap()])
        .with_advertise_addr("192.168.1.100:7002".parse().unwrap())
        .with_node_name("my-custom-node".to_string());

        assert_eq!(config.raft_id, 2);
        assert_eq!(config.seed_nodes.len(), 1);
        assert_eq!(
            config.advertise_addr,
            Some("192.168.1.100:7002".parse().unwrap())
        );
        assert_eq!(config.node_name(), "my-custom-node");
    }

    #[test]
    fn test_shard_leader_info() {
        let info = ShardLeaderInfo::new(42, 5);
        assert_eq!(info.leader_id, 42);
        assert_eq!(info.epoch, 5);
    }

    #[test]
    fn test_shard_leader_metadata_encoding() {
        let mut meta = RaftNodeMetadata::new(1, "127.0.0.1:9001".parse().unwrap());

        // Set some shard leaders
        let mut leaders = std::collections::HashMap::new();
        leaders.insert(0, ShardLeaderInfo::new(1, 10));
        leaders.insert(1, ShardLeaderInfo::new(2, 20));
        leaders.insert(2, ShardLeaderInfo::new(1, 15));

        meta.set_shard_leaders(&leaders);

        // Verify encoding
        assert!(meta.has_shard_leaders());

        // Decode and verify
        let decoded = meta.get_shard_leaders();
        assert_eq!(decoded.len(), 3);

        let info0 = decoded.get(&0).unwrap();
        assert_eq!(info0.leader_id, 1);
        assert_eq!(info0.epoch, 10);

        let info1 = decoded.get(&1).unwrap();
        assert_eq!(info1.leader_id, 2);
        assert_eq!(info1.epoch, 20);

        let info2 = decoded.get(&2).unwrap();
        assert_eq!(info2.leader_id, 1);
        assert_eq!(info2.epoch, 15);
    }

    #[test]
    fn test_shard_leader_metadata_empty() {
        let meta = RaftNodeMetadata::new(1, "127.0.0.1:9001".parse().unwrap());

        // No shard leaders set
        assert!(!meta.has_shard_leaders());
        let decoded = meta.get_shard_leaders();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_shard_leader_metadata_clear() {
        let mut meta = RaftNodeMetadata::new(1, "127.0.0.1:9001".parse().unwrap());

        // Set some leaders
        let mut leaders = std::collections::HashMap::new();
        leaders.insert(0, ShardLeaderInfo::new(1, 10));
        meta.set_shard_leaders(&leaders);
        assert!(meta.has_shard_leaders());

        // Clear by setting empty map
        meta.set_shard_leaders(&std::collections::HashMap::new());
        assert!(!meta.has_shard_leaders());
    }

    #[test]
    fn test_shard_leader_parse_malformed_entries() {
        // Test that malformed entries are gracefully skipped
        let malformed_inputs = [
            "",                           // Empty
            "0:1",                         // Missing epoch
            "0:1:a",                       // Non-numeric epoch
            "a:1:10",                      // Non-numeric shard_id
            "0:b:10",                      // Non-numeric leader_id
            "0:1:10,invalid,1:2:20",       // One valid, one invalid, one valid
        ];

        for input in &malformed_inputs {
            let result = parse_shard_leaders(input);
            // Should not panic, just skip invalid entries
            if *input == "0:1:10,invalid,1:2:20" {
                // Should have parsed 2 valid entries
                assert_eq!(result.len(), 2);
                assert!(result.contains_key(&0));
                assert!(result.contains_key(&1));
            }
        }
    }

    #[test]
    fn test_shard_leader_serialization_roundtrip() {
        let mut meta = RaftNodeMetadata::new(1, "127.0.0.1:9001".parse().unwrap());

        // Set shard leaders
        let mut leaders = std::collections::HashMap::new();
        leaders.insert(0, ShardLeaderInfo::new(100, 1000));
        leaders.insert(63, ShardLeaderInfo::new(200, 2000)); // Max shard in Phase 1
        meta.set_shard_leaders(&leaders);

        // Serialize and deserialize the entire metadata
        let bytes = meta.to_bytes();
        let decoded_meta = RaftNodeMetadata::from_bytes(&bytes).unwrap();

        // Verify shard leaders survived the roundtrip
        let decoded_leaders = decoded_meta.get_shard_leaders();
        assert_eq!(decoded_leaders.len(), 2);

        let info0 = decoded_leaders.get(&0).unwrap();
        assert_eq!(info0.leader_id, 100);
        assert_eq!(info0.epoch, 1000);

        let info63 = decoded_leaders.get(&63).unwrap();
        assert_eq!(info63.leader_id, 200);
        assert_eq!(info63.epoch, 2000);
    }
}
