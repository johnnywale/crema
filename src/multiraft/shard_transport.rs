//! Shard-aware transport multiplexer for Multi-Raft Phase 2.
//!
//! This module provides message routing infrastructure for per-shard Raft
//! in a Multi-Raft setup.
//!
//! # Architecture
//!
//! The shard transport system uses a two-layer architecture:
//!
//! 1. **NodeMessageRouter** (in `network/router.rs`): Owns all TCP connections
//!    and handles actual message I/O. This is node-scoped and shared.
//!
//! 2. **RaftShardMultiplexer** (this module): Pure routing logic that maps
//!    incoming ShardRaft messages to the correct shard's RaftNode. No I/O.
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────┐
//! │                     NodeMessageRouter                              │
//! │  (owns TCP connections, sends Message::Raft and Message::ShardRaft)│
//! └───────────────────────────────┬────────────────────────────────────┘
//!                                 │
//!          ┌──────────────────────┼──────────────────────┐
//!          │                      │                      │
//!          ▼                      ▼                      ▼
//!   ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
//!   │  Main Raft   │      │RaftShardMux  │      │ Application  │
//!   │  (Raft msg)  │      │(routes shard │      │  Messages    │
//!   └──────────────┘      │ msgs)        │      └──────────────┘
//!                         └───────┬──────┘
//!                                 │
//!            ┌────────────────────┼────────────────────┐
//!            ▼                    ▼                    ▼
//!     ┌──────────┐         ┌──────────┐         ┌──────────┐
//!     │ Shard 0  │         │ Shard 1  │         │ Shard N  │
//!     │ Handler  │         │ Handler  │         │ Handler  │
//!     └──────────┘         └──────────┘         └──────────┘
//! ```

use crate::consensus::transport::{RaftMessageSender, RaftTransport, TransportMetricsSnapshot};
use crate::error::{Error, NetworkError, Result};
use crate::network::router::NodeMessageRouter;
use crate::network::rpc::{Message, ShardRaftMessage};
use crate::types::NodeId;
use futures::future::BoxFuture;
use parking_lot::RwLock;
use raft::prelude::Message as RaftMessage;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;

use super::shard::ShardId;

/// Pure routing multiplexer for shard Raft messages.
///
/// This component only handles the routing of incoming ShardRaft messages
/// to the correct shard's handler. It does NOT own any network connections.
/// All actual I/O is delegated to NodeMessageRouter.
///
/// # Usage
///
/// 1. Create a RaftShardMultiplexer for the node
/// 2. Register shards to get receivers for incoming messages
/// 3. When a Message::ShardRaft is received, call `route_incoming()`
/// 4. The message is forwarded to the correct shard's handler
#[derive(Debug)]
pub struct RaftShardMultiplexer {
    /// This node's ID.
    node_id: NodeId,

    /// Message handlers for each shard (shard_id → sender).
    handlers: RwLock<HashMap<ShardId, mpsc::UnboundedSender<RaftMessage>>>,
}

impl RaftShardMultiplexer {
    /// Create a new shard multiplexer.
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            handlers: RwLock::new(HashMap::new()),
        }
    }

    /// Get this node's ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Register a handler for a shard's messages.
    ///
    /// Returns a receiver that will receive Raft messages for this shard.
    pub fn register_shard(&self, shard_id: ShardId) -> mpsc::UnboundedReceiver<RaftMessage> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.handlers.write().insert(shard_id, tx);
        tracing::debug!(
            node_id = self.node_id,
            shard_id,
            "Registered shard message handler"
        );
        rx
    }

    /// Unregister a shard's handler.
    pub fn unregister_shard(&self, shard_id: ShardId) {
        self.handlers.write().remove(&shard_id);
        tracing::debug!(
            node_id = self.node_id,
            shard_id,
            "Unregistered shard message handler"
        );
    }

    /// Route an incoming shard Raft message to the correct handler.
    ///
    /// This is called by the network layer when a Message::ShardRaft is received.
    pub fn route_incoming(&self, shard_msg: ShardRaftMessage) -> Result<()> {
        let shard_id = shard_msg.shard_id;

        // Decode the Raft message
        let raft_msg = shard_msg.to_raft_message().map_err(|e| {
            Error::Network(NetworkError::Deserialization(format!(
                "Failed to decode Raft message: {}",
                e
            )))
        })?;

        // Find the handler for this shard
        let handlers = self.handlers.read();
        if let Some(tx) = handlers.get(&shard_id) {
            if let Err(e) = tx.send(raft_msg) {
                tracing::warn!(
                    shard_id,
                    error = %e,
                    "Failed to deliver message to shard handler (channel closed)"
                );
                return Err(Error::Internal(format!(
                    "Shard {} handler channel closed",
                    shard_id
                )));
            }
        } else {
            tracing::warn!(
                node_id = self.node_id,
                shard_id,
                "No handler registered for shard"
            );
            return Err(Error::ShardNotFound(shard_id));
        }

        Ok(())
    }

    /// Check if a shard has a registered handler.
    pub fn has_shard(&self, shard_id: ShardId) -> bool {
        self.handlers.read().contains_key(&shard_id)
    }

    /// Get all registered shard IDs.
    pub fn shard_ids(&self) -> Vec<ShardId> {
        self.handlers.read().keys().copied().collect()
    }

    /// Clear all handlers (called on shutdown).
    pub fn clear(&self) {
        self.handlers.write().clear();
        tracing::debug!(node_id = self.node_id, "Cleared all shard handlers");
    }
}

// ============================================================================
// Legacy ShardTransportMultiplexer - Kept for backward compatibility
// ============================================================================

/// Transport multiplexer that routes Raft messages by shard ID.
///
/// **DEPRECATED**: This struct maintains backward compatibility with existing code
/// that expects a transport-owning multiplexer. New code should use:
/// - `RaftShardMultiplexer` for routing logic
/// - `NodeMessageRouter` (via `ShardRaftAdapter`) for transport
///
/// This wraps the existing RaftTransport and adds shard-aware routing.
/// TCP connections are shared across all shards to reduce resource usage.
#[derive(Debug)]
pub struct ShardTransportMultiplexer {
    /// This node's ID.
    node_id: NodeId,

    /// The underlying transport (shared across all shards).
    /// This is optional for the new architecture where NodeMessageRouter is used.
    transport: Option<Arc<RaftTransport>>,

    /// Optional NodeMessageRouter for the new architecture.
    router: Option<Arc<NodeMessageRouter>>,

    /// The pure routing multiplexer.
    multiplexer: RaftShardMultiplexer,

    /// Peer addresses (node_id -> address).
    peer_addresses: RwLock<HashMap<NodeId, SocketAddr>>,
}

impl ShardTransportMultiplexer {
    /// Create a new shard transport multiplexer with a RaftTransport (legacy).
    pub fn new(node_id: NodeId, transport: Arc<RaftTransport>) -> Self {
        Self {
            node_id,
            transport: Some(transport),
            router: None,
            multiplexer: RaftShardMultiplexer::new(node_id),
            peer_addresses: RwLock::new(HashMap::new()),
        }
    }

    /// Create with a NodeMessageRouter (new architecture).
    pub fn with_router(node_id: NodeId, router: Arc<NodeMessageRouter>) -> Self {
        Self {
            node_id,
            transport: None,
            router: Some(router),
            multiplexer: RaftShardMultiplexer::new(node_id),
            peer_addresses: RwLock::new(HashMap::new()),
        }
    }

    /// Create with a new RaftTransport (legacy).
    pub fn new_with_transport(node_id: NodeId) -> Self {
        let transport = Arc::new(RaftTransport::new(node_id));
        Self::new(node_id, transport)
    }

    /// Get the underlying transport (legacy).
    pub fn transport(&self) -> &Arc<RaftTransport> {
        self.transport
            .as_ref()
            .expect("No RaftTransport configured - use router() instead")
    }

    /// Get the NodeMessageRouter if configured.
    pub fn node_router(&self) -> Option<&Arc<NodeMessageRouter>> {
        self.router.as_ref()
    }

    /// Get the pure routing multiplexer.
    pub fn multiplexer(&self) -> &RaftShardMultiplexer {
        &self.multiplexer
    }

    /// Get this node's ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Register a handler for a shard's messages.
    ///
    /// Returns a receiver that will receive Raft messages for this shard.
    pub fn register_shard(&self, shard_id: ShardId) -> mpsc::UnboundedReceiver<RaftMessage> {
        self.multiplexer.register_shard(shard_id)
    }

    /// Unregister a shard's handler.
    pub fn unregister_shard(&self, shard_id: ShardId) {
        self.multiplexer.unregister_shard(shard_id)
    }

    /// Add a peer to the transport.
    pub async fn add_peer(&self, node_id: NodeId, addr: SocketAddr) {
        self.peer_addresses.write().insert(node_id, addr);

        if let Some(router) = &self.router {
            router.add_peer(node_id, addr).await;
        } else if let Some(transport) = &self.transport {
            transport.add_peer(node_id, addr).await;
        }

        tracing::debug!(
            self_node_id = self.node_id,
            peer_node_id = node_id,
            %addr,
            "Added peer to shard transport"
        );
    }

    /// Remove a peer from the transport.
    pub fn remove_peer(&self, node_id: NodeId) {
        self.peer_addresses.write().remove(&node_id);

        if let Some(router) = &self.router {
            router.remove_peer(node_id);
        } else if let Some(transport) = &self.transport {
            transport.remove_peer(node_id);
        }

        tracing::debug!(
            self_node_id = self.node_id,
            peer_node_id = node_id,
            "Removed peer from shard transport"
        );
    }

    /// Get a peer's address.
    pub fn get_peer(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.peer_addresses.read().get(&node_id).copied()
    }

    /// Send a Raft message for a specific shard.
    pub fn send_shard_message(&self, shard_id: ShardId, msg: RaftMessage) -> Result<()> {
        let to = msg.to;

        if let Some(router) = &self.router {
            // Use NodeMessageRouter - synchronous, non-blocking
            router.send_shard_raft_message(shard_id, msg)?;
        } else if let Some(transport) = &self.transport {
            // Legacy path using RaftTransport
            let shard_msg = ShardRaftMessage::from_raft_message(shard_id, &msg).map_err(|e| {
                Error::Network(NetworkError::Serialization(format!(
                    "Failed to serialize Raft message: {}",
                    e
                )))
            })?;

            let message = Message::ShardRaft(shard_msg);

            // Send via the shared transport
            let transport = transport.clone();
            tokio::spawn(async move {
                if let Err(e) = transport.send_message(to, message).await {
                    tracing::warn!(
                        to,
                        shard_id,
                        error = %e,
                        "Failed to send shard Raft message"
                    );
                }
            });
        } else {
            return Err(Error::Internal("No transport configured".to_string()));
        }

        Ok(())
    }

    /// Send multiple Raft messages for a specific shard.
    pub fn send_shard_messages(&self, shard_id: ShardId, msgs: Vec<RaftMessage>) {
        for msg in msgs {
            if let Err(e) = self.send_shard_message(shard_id, msg) {
                tracing::warn!(
                    shard_id,
                    error = %e,
                    "Failed to send shard Raft message"
                );
            }
        }
    }

    /// Handle an incoming shard Raft message.
    ///
    /// Routes the message to the correct shard's handler.
    pub fn handle_shard_message(&self, shard_msg: ShardRaftMessage) -> Result<()> {
        self.multiplexer.route_incoming(shard_msg)
    }

    /// Check if a shard has a registered handler.
    pub fn has_shard(&self, shard_id: ShardId) -> bool {
        self.multiplexer.has_shard(shard_id)
    }

    /// Get all registered shard IDs.
    pub fn shard_ids(&self) -> Vec<ShardId> {
        self.multiplexer.shard_ids()
    }

    /// Shutdown the transport.
    pub async fn shutdown(&self) {
        self.multiplexer.clear();

        // Only shutdown the legacy transport if we own it
        // NodeMessageRouter shutdown is handled by its owner
        if let Some(transport) = &self.transport {
            transport.shutdown().await;
        }

        tracing::info!(
            node_id = self.node_id,
            "Shard transport multiplexer shutdown"
        );
    }
}

/// Shard-aware message sender that wraps Raft messages with shard_id.
///
/// This struct implements `RaftMessageSender` to enable per-shard Raft replication.
/// When `RaftNode` calls `send_messages()`, this adapter routes the messages through
/// the `ShardTransportMultiplexer` with the correct shard ID prefix.
///
/// # Problem Solved
///
/// Without this adapter, `RaftNode` creates its own internal `RaftTransport` which
/// sends messages as `Message::Raft(...)` without any shard information. This causes
/// per-shard messages to be routed to the main Raft group instead of the correct shard.
///
/// # Solution
///
/// ```text
/// Before (Broken):
///   ShardRaftNode → RaftNode → RaftTransport → Message::Raft(msg) → LOST
///
/// After (Fixed):
///   ShardRaftNode → RaftNode → ShardRaftTransport → Message::ShardRaft(shard_id, msg) → OK
/// ```
#[derive(Debug)]
pub struct ShardRaftTransport {
    /// The shard ID this transport handles.
    shard_id: ShardId,

    /// The underlying multiplexer that routes shard messages.
    multiplexer: Arc<ShardTransportMultiplexer>,
}

impl ShardRaftTransport {
    /// Create a new shard-aware transport.
    ///
    /// # Arguments
    ///
    /// * `shard_id` - The shard ID to associate with outgoing messages
    /// * `multiplexer` - The shared transport multiplexer for routing
    pub fn new(shard_id: ShardId, multiplexer: Arc<ShardTransportMultiplexer>) -> Self {
        Self {
            shard_id,
            multiplexer,
        }
    }

    /// Get the shard ID.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Get the underlying multiplexer.
    pub fn multiplexer(&self) -> &Arc<ShardTransportMultiplexer> {
        &self.multiplexer
    }
}

impl RaftMessageSender for ShardRaftTransport {
    /// Send multiple Raft messages with shard ID prefix.
    ///
    /// This is the key method that enables per-shard replication. Each message
    /// is wrapped with the shard ID and routed through the multiplexer.
    fn send_messages(&self, msgs: Vec<RaftMessage>) {
        self.multiplexer.send_shard_messages(self.shard_id, msgs);
    }

    /// Send a single arbitrary message to a peer.
    ///
    /// Delegates to the underlying transport for non-Raft messages
    /// (like forwarded commands).
    fn send_message(&self, to: NodeId, msg: Message) -> BoxFuture<'_, Result<()>> {
        let multiplexer = self.multiplexer.clone();
        Box::pin(async move {
            if let Some(router) = multiplexer.node_router() {
                router.send_message(to, msg).await
            } else {
                multiplexer.transport().send_message(to, msg).await
            }
        })
    }

    /// Add a peer to the transport.
    fn add_peer(&self, id: NodeId, addr: SocketAddr) -> BoxFuture<'_, ()> {
        let multiplexer = self.multiplexer.clone();
        Box::pin(async move {
            multiplexer.add_peer(id, addr).await;
        })
    }

    /// Remove a peer from the transport.
    fn remove_peer(&self, id: NodeId) {
        self.multiplexer.remove_peer(id);
    }

    /// Get a peer's address.
    fn get_peer(&self, id: NodeId) -> Option<SocketAddr> {
        self.multiplexer.get_peer(id)
    }

    /// Get all registered peer IDs.
    fn peer_ids(&self) -> Vec<NodeId> {
        if let Some(router) = self.multiplexer.node_router() {
            router.peer_ids()
        } else {
            self.multiplexer.transport().peer_ids()
        }
    }

    /// Get transport metrics.
    ///
    /// Returns metrics from the underlying shared transport.
    fn metrics(&self) -> TransportMetricsSnapshot {
        if let Some(router) = self.multiplexer.node_router() {
            router.metrics()
        } else {
            self.multiplexer.transport().metrics()
        }
    }

    /// Shutdown the transport.
    ///
    /// Note: This is a no-op because `ShardRaftTransport` doesn't own the
    /// multiplexer. The multiplexer is shared across all shards and is
    /// shut down separately by the coordinator.
    fn shutdown(&self) -> BoxFuture<'_, ()> {
        // Shard transport doesn't own the multiplexer, so no-op
        Box::pin(async {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_raft_shard_multiplexer() {
        let multiplexer = RaftShardMultiplexer::new(1);

        // Register shards
        let _rx0 = multiplexer.register_shard(0);
        let _rx1 = multiplexer.register_shard(1);

        assert!(multiplexer.has_shard(0));
        assert!(multiplexer.has_shard(1));
        assert!(!multiplexer.has_shard(2));

        // Unregister
        multiplexer.unregister_shard(0);
        assert!(!multiplexer.has_shard(0));
        assert!(multiplexer.has_shard(1));
    }

    #[tokio::test]
    async fn test_shard_ids() {
        let multiplexer = RaftShardMultiplexer::new(1);

        let _rx0 = multiplexer.register_shard(0);
        let _rx1 = multiplexer.register_shard(1);
        let _rx3 = multiplexer.register_shard(3);

        let mut ids = multiplexer.shard_ids();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 3]);
    }

    #[tokio::test]
    async fn test_legacy_shard_registration() {
        let transport = Arc::new(RaftTransport::new(1));
        let multiplexer = ShardTransportMultiplexer::new(1, transport);

        // Register shards
        let _rx0 = multiplexer.register_shard(0);
        let _rx1 = multiplexer.register_shard(1);

        assert!(multiplexer.has_shard(0));
        assert!(multiplexer.has_shard(1));
        assert!(!multiplexer.has_shard(2));

        // Unregister
        multiplexer.unregister_shard(0);
        assert!(!multiplexer.has_shard(0));
        assert!(multiplexer.has_shard(1));
    }
}
