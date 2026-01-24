//! Memberlist-based cluster discovery implementation.
//!
//! This module provides `MemberlistDiscovery`, which implements the `ClusterDiscovery`
//! trait using the memberlist gossip protocol (SWIM).

use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};

use super::discovery::{
    ClusterDiscovery, ClusterDiscoveryError, ClusterEvent, NodeMetadata, NodeRegistry,
};
use super::memberlist_cluster::{
    MemberlistCluster, MemberlistClusterConfig, MemberlistEvent, RaftNodeMetadata,
};
use crate::config::MemberlistConfig;
use crate::types::NodeId;

/// Memberlist-based cluster discovery using SWIM gossip protocol.
///
/// This implementation wraps `MemberlistCluster` and implements the `ClusterDiscovery`
/// trait, providing gossip-based node discovery and health monitoring.
pub struct MemberlistDiscovery {
    /// The underlying memberlist cluster.
    cluster: MemberlistCluster,
    /// Generic node registry (synced from memberlist registry).
    registry: Arc<NodeRegistry>,
    /// Channel for converted events.
    event_rx: mpsc::UnboundedReceiver<ClusterEvent>,
    /// Channel sender for converted events.
    event_tx: mpsc::UnboundedSender<ClusterEvent>,
    /// This node's ID.
    node_id: NodeId,
    /// Whether the cluster has been started.
    initialized: bool,
}

impl MemberlistDiscovery {
    /// Create a new memberlist discovery instance from configuration.
    pub fn new(
        node_id: NodeId,
        raft_addr: SocketAddr,
        config: &MemberlistConfig,
        seed_nodes: &[(NodeId, SocketAddr)],
    ) -> Self {
        let bind_addr = config.get_bind_addr(raft_addr);

        // Build memberlist config
        let mut ml_config = MemberlistClusterConfig::new(node_id, bind_addr, raft_addr);

        // Add seed addresses from config
        if !config.seed_addrs.is_empty() {
            ml_config = ml_config.with_seed_nodes(config.seed_addrs.clone());
        } else {
            // Fall back to seed_nodes addresses if no memberlist-specific seeds
            let seed_addrs: Vec<_> = seed_nodes
                .iter()
                .filter_map(|(_, addr)| {
                    // Convert raft addr to memberlist addr (port + 1000)
                    addr.port()
                        .checked_add(1000)
                        .map(|ml_port| SocketAddr::new(addr.ip(), ml_port))
                })
                .collect();
            if !seed_addrs.is_empty() {
                ml_config = ml_config.with_seed_nodes(seed_addrs);
            }
        }

        if let Some(advertise) = config.advertise_addr {
            ml_config = ml_config.with_advertise_addr(advertise);
        }

        if let Some(ref name) = config.node_name {
            ml_config = ml_config.with_node_name(name.clone());
        }

        let cluster = MemberlistCluster::new(ml_config);
        let registry = Arc::new(NodeRegistry::new());
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        // Register self in the generic registry
        let self_metadata = NodeMetadata::new(node_id, raft_addr);
        registry.register(self_metadata);

        Self {
            cluster,
            registry,
            event_rx,
            event_tx,
            node_id,
            initialized: false,
        }
    }

    /// Convert a MemberlistEvent to a ClusterEvent.
    fn convert_event(event: MemberlistEvent) -> ClusterEvent {
        match event {
            MemberlistEvent::NodeJoin {
                raft_id,
                raft_addr,
                metadata,
            } => ClusterEvent::NodeJoin {
                node_id: raft_id,
                raft_addr,
                metadata: Self::convert_metadata(&metadata),
            },
            MemberlistEvent::NodeLeave { raft_id } => ClusterEvent::NodeLeave { node_id: raft_id },
            MemberlistEvent::NodeFailed { raft_id } => {
                ClusterEvent::NodeFailed { node_id: raft_id }
            }
            MemberlistEvent::NodeUpdate { raft_id, metadata } => ClusterEvent::NodeUpdate {
                node_id: raft_id,
                metadata: Self::convert_metadata(&metadata),
            },
        }
    }

    /// Convert RaftNodeMetadata to generic NodeMetadata.
    fn convert_metadata(raft_meta: &RaftNodeMetadata) -> NodeMetadata {
        NodeMetadata {
            node_id: raft_meta.raft_id,
            raft_addr: raft_meta.raft_addr,
            version: raft_meta.version.clone(),
            tags: raft_meta.tags.clone(),
        }
    }

    /// Sync the generic registry from the memberlist registry.
    fn sync_registry(&self) {
        let ml_registry = self.cluster.registry();

        for node_id in ml_registry.all_nodes() {
            if let Some(ml_metadata) = ml_registry.get_metadata(node_id) {
                let generic_metadata = Self::convert_metadata(&ml_metadata);
                self.registry.register(generic_metadata);

                if !ml_registry.is_healthy(node_id) {
                    self.registry.mark_failed(node_id);
                }
            }
        }
    }

    /// Get access to the underlying memberlist cluster (for advanced usage).
    pub fn inner(&self) -> &MemberlistCluster {
        &self.cluster
    }

    /// Get mutable access to the underlying memberlist cluster.
    pub fn inner_mut(&mut self) -> &mut MemberlistCluster {
        &mut self.cluster
    }
}

#[async_trait]
impl ClusterDiscovery for MemberlistDiscovery {
    async fn start(&mut self) -> Result<(), ClusterDiscoveryError> {
        info!(
            node_id = self.node_id,
            "Starting memberlist discovery"
        );

        self.cluster
            .start()
            .await
            .map_err(|e| ClusterDiscoveryError::Other(e.to_string()))?;

        self.initialized = true;

        // Sync registry after start
        self.sync_registry();

        info!(
            node_id = self.node_id,
            "Memberlist discovery started"
        );

        Ok(())
    }

    async fn leave(&mut self) -> Result<(), ClusterDiscoveryError> {
        info!(node_id = self.node_id, "Leaving memberlist cluster");

        self.cluster
            .leave()
            .await
            .map_err(|e| ClusterDiscoveryError::Other(e.to_string()))
    }

    async fn shutdown(&mut self) -> Result<(), ClusterDiscoveryError> {
        info!(node_id = self.node_id, "Shutting down memberlist discovery");

        self.initialized = false;

        self.cluster
            .shutdown()
            .await
            .map_err(|e| ClusterDiscoveryError::Other(e.to_string()))
    }

    fn is_initialized(&self) -> bool {
        self.initialized && self.cluster.is_initialized()
    }

    fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn registry(&self) -> Arc<NodeRegistry> {
        // Sync before returning
        self.sync_registry();
        self.registry.clone()
    }

    async fn recv_event(&mut self) -> Option<ClusterEvent> {
        // First check our converted event queue
        if let Ok(event) = self.event_rx.try_recv() {
            return Some(event);
        }

        // Then try to get from memberlist
        if let Some(ml_event) = self.cluster.recv_event().await {
            let event = Self::convert_event(ml_event.clone());

            // Update registry based on event
            match &ml_event {
                MemberlistEvent::NodeJoin { metadata, .. } => {
                    self.registry.register(Self::convert_metadata(metadata));
                }
                MemberlistEvent::NodeLeave { raft_id } => {
                    self.registry.unregister(*raft_id);
                }
                MemberlistEvent::NodeFailed { raft_id } => {
                    self.registry.mark_failed(*raft_id);
                }
                MemberlistEvent::NodeUpdate { metadata, .. } => {
                    self.registry.register(Self::convert_metadata(metadata));
                }
            }

            return Some(event);
        }

        None
    }

    fn try_recv_event(&mut self) -> Option<ClusterEvent> {
        // First check our converted event queue
        if let Ok(event) = self.event_rx.try_recv() {
            return Some(event);
        }

        // Then try to get from memberlist
        if let Some(ml_event) = self.cluster.try_recv_event() {
            let event = Self::convert_event(ml_event.clone());

            // Update registry based on event
            match &ml_event {
                MemberlistEvent::NodeJoin { metadata, .. } => {
                    self.registry.register(Self::convert_metadata(metadata));
                }
                MemberlistEvent::NodeLeave { raft_id } => {
                    self.registry.unregister(*raft_id);
                }
                MemberlistEvent::NodeFailed { raft_id } => {
                    self.registry.mark_failed(*raft_id);
                }
                MemberlistEvent::NodeUpdate { metadata, .. } => {
                    self.registry.register(Self::convert_metadata(metadata));
                }
            }

            return Some(event);
        }

        None
    }

    fn members(&self) -> Vec<NodeId> {
        self.cluster.members()
    }

    fn healthy_members(&self) -> Vec<NodeId> {
        self.cluster.healthy_members()
    }

    fn get_node_addr(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.cluster.get_node_addr(node_id)
    }

    async fn update_local_metadata(
        &self,
        metadata: NodeMetadata,
    ) -> Result<(), ClusterDiscoveryError> {
        // Update our generic registry
        self.registry.register(metadata);
        // Memberlist doesn't support dynamic metadata updates after start
        // The metadata is set at initialization time
        Ok(())
    }

    fn is_active(&self) -> bool {
        true // Memberlist is an active discovery mechanism
    }
}

impl std::fmt::Debug for MemberlistDiscovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemberlistDiscovery")
            .field("node_id", &self.node_id)
            .field("initialized", &self.initialized)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_metadata() {
        let raft_meta = RaftNodeMetadata::new(1, "127.0.0.1:9000".parse().unwrap())
            .with_tag("role", "leader");

        let generic_meta = MemberlistDiscovery::convert_metadata(&raft_meta);

        assert_eq!(generic_meta.node_id, 1);
        assert_eq!(generic_meta.raft_addr, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(generic_meta.get_tag("role"), Some("leader"));
    }

    #[test]
    fn test_convert_event() {
        let raft_meta = RaftNodeMetadata::new(2, "127.0.0.1:9001".parse().unwrap());
        let ml_event = MemberlistEvent::NodeJoin {
            raft_id: 2,
            raft_addr: "127.0.0.1:9001".parse().unwrap(),
            metadata: raft_meta,
        };

        let cluster_event = MemberlistDiscovery::convert_event(ml_event);

        match cluster_event {
            ClusterEvent::NodeJoin { node_id, raft_addr, .. } => {
                assert_eq!(node_id, 2);
                assert_eq!(raft_addr, "127.0.0.1:9001".parse().unwrap());
            }
            _ => panic!("Expected NodeJoin event"),
        }
    }
}
