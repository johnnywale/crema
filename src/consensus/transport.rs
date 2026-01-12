//! Transport layer for sending Raft messages to peers.

use crate::error::{NetworkError, Result};
use crate::network::rpc::{encode_message, Message, RaftMessageWrapper};
use crate::types::NodeId;
use parking_lot::RwLock;
use raft::prelude::Message as RaftMessage;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, warn};

/// Transport for sending messages to other Raft nodes.
pub struct RaftTransport {
    /// This node's ID.
    node_id: NodeId,

    /// Mapping of node IDs to their addresses.
    /// Shared with sender_loop via Arc.
    peers: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,

    /// Channel for outgoing messages.
    outgoing_tx: mpsc::UnboundedSender<(NodeId, Message)>,

    /// Handle to the sender task.
    _sender_handle: tokio::task::JoinHandle<()>,
}

impl RaftTransport {
    /// Create a new transport.
    pub fn new(node_id: NodeId) -> Self {
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel();

        // Create shared peers map - IMPORTANT: sender_loop must use the same instance
        let peers = Arc::new(RwLock::new(HashMap::new()));
        let peers_clone = peers.clone();

        let sender_handle = tokio::spawn(async move {
            Self::sender_loop(peers_clone, outgoing_rx).await;
        });

        Self {
            node_id,
            peers,  // Use the shared Arc, not a new HashMap!
            outgoing_tx,
            _sender_handle: sender_handle,
        }
    }

    /// Add a peer to the transport.
    pub fn add_peer(&self, id: NodeId, addr: SocketAddr) {
        self.peers.write().insert(id, addr);
    }

    /// Remove a peer from the transport.
    pub fn remove_peer(&self, id: NodeId) {
        self.peers.write().remove(&id);
    }

    /// Get a peer's address.
    pub fn get_peer(&self, id: NodeId) -> Option<SocketAddr> {
        self.peers.read().get(&id).copied()
    }

    /// Get all peer IDs.
    pub fn peer_ids(&self) -> Vec<NodeId> {
        self.peers.read().keys().copied().collect()
    }

    /// Send a Raft message to a peer.
    pub fn send(&self, msg: RaftMessage) -> Result<()> {
        let to = msg.to;

        let wrapper = RaftMessageWrapper::from_raft_message(&msg)
            .map_err(|e| NetworkError::Serialization(e.to_string()))?;

        let message = Message::Raft(wrapper);

        self.outgoing_tx
            .send((to, message))
            .map_err(|_| NetworkError::SendFailed("channel closed".to_string()))?;

        Ok(())
    }

    /// Send multiple Raft messages.
    pub fn send_messages(&self, msgs: Vec<RaftMessage>) {
        if !msgs.is_empty() {
            eprintln!("TRANSPORT: Queuing {} messages from node {}", msgs.len(), self.node_id);
        }
        for msg in msgs {
            eprintln!("TRANSPORT: Queuing msg type={:?} from {} to {}", msg.msg_type, msg.from, msg.to);
            if let Err(e) = self.send(msg) {
                warn!(error = %e, "Failed to queue Raft message");
            }
        }
    }

    /// Background loop that sends queued messages.
    async fn sender_loop(
        peers: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
        mut rx: mpsc::UnboundedReceiver<(NodeId, Message)>,
    ) {
        eprintln!("SENDER_LOOP: Started");
        // Connection cache
        let mut connections: HashMap<NodeId, TcpStream> = HashMap::new();

        while let Some((to, msg)) = rx.recv().await {
            eprintln!("SENDER_LOOP: Got message for node {}", to);
            let addr = {
                let peers = peers.read();
                let peer_count = peers.len();
                eprintln!("SENDER_LOOP: Looking up node {} in {} peers", to, peer_count);
                peers.get(&to).copied()
            };

            let addr = match addr {
                Some(a) => {
                    eprintln!("SENDER_LOOP: Found addr {} for node {}", a, to);
                    a
                }
                None => {
                    eprintln!("SENDER_LOOP: Unknown peer {}, dropping message", to);
                    debug!(to, "Unknown peer, dropping message");
                    continue;
                }
            };

            // Try to send using cached connection or create new one
            let result = Self::send_to_peer(&mut connections, to, addr, &msg).await;

            if let Err(e) = result {
                eprintln!("SENDER_LOOP: Failed to send to node {}: {}", to, e);
                debug!(to, error = %e, "Failed to send message, will retry on next message");
                connections.remove(&to);
            } else {
                eprintln!("SENDER_LOOP: Successfully sent to node {}", to);
            }
        }
        eprintln!("SENDER_LOOP: Exiting");
    }

    async fn send_to_peer(
        connections: &mut HashMap<NodeId, TcpStream>,
        to: NodeId,
        addr: SocketAddr,
        msg: &Message,
    ) -> Result<()> {
        // Encode message with length prefix
        let data = encode_message(msg)?;
        let len = data.len() as u32;

        // Get or create connection
        let stream = if let Some(stream) = connections.get_mut(&to) {
            stream
        } else {
            let stream = TcpStream::connect(addr)
                .await
                .map_err(|e| NetworkError::ConnectionFailed {
                    addr: addr.to_string(),
                    reason: e.to_string(),
                })?;

            connections.insert(to, stream);
            connections.get_mut(&to).unwrap()
        };

        // Write length prefix + data
        stream.write_all(&len.to_be_bytes()).await.map_err(|e| {
            NetworkError::SendFailed(format!("failed to write length: {}", e))
        })?;

        stream
            .write_all(&data)
            .await
            .map_err(|e| NetworkError::SendFailed(format!("failed to write data: {}", e)))?;

        Ok(())
    }
}

impl std::fmt::Debug for RaftTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftTransport")
            .field("node_id", &self.node_id)
            .field("peer_count", &self.peers.read().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_remove_peer() {
        let transport = RaftTransport::new(1);

        transport.add_peer(2, "127.0.0.1:9002".parse().unwrap());
        transport.add_peer(3, "127.0.0.1:9003".parse().unwrap());

        assert_eq!(transport.peer_ids().len(), 2);

        transport.remove_peer(2);
        assert_eq!(transport.peer_ids().len(), 1);
        assert!(transport.get_peer(2).is_none());
        assert!(transport.get_peer(3).is_some());
    }
}
