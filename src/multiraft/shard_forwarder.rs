//! Cross-shard request forwarding for Multi-Raft mode.
//!
//! When a request arrives at a node that doesn't host the target shard's leader,
//! this module handles forwarding the request to the correct node.

use crate::error::{Error, Result};
use crate::network::rpc::{Message, ShardForwardedCommand, ShardForwardResponse, ShardId};
use crate::types::{CacheCommand, NodeId};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;

/// Configuration for shard forwarding.
#[derive(Debug, Clone)]
pub struct ShardForwardingConfig {
    /// Whether forwarding is enabled.
    pub enabled: bool,

    /// Timeout for forwarded requests.
    pub timeout: Duration,

    /// Maximum number of pending forwards (backpressure).
    pub max_pending_forwards: usize,

    /// Connection timeout for reaching other nodes.
    pub connect_timeout: Duration,
}

impl Default for ShardForwardingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            timeout: Duration::from_secs(5),
            max_pending_forwards: 5000,
            connect_timeout: Duration::from_secs(2),
        }
    }
}

impl ShardForwardingConfig {
    /// Create a new config with forwarding enabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Disable forwarding.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Set the timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the max pending forwards.
    pub fn with_max_pending(mut self, max: usize) -> Self {
        self.max_pending_forwards = max;
        self
    }
}

/// Response from a forwarded shard request.
#[derive(Debug)]
pub struct ForwardResult {
    /// Whether the operation succeeded.
    pub success: bool,
    /// Value for GET operations.
    pub value: Option<Bytes>,
    /// Error message if failed.
    pub error: Option<String>,
}

/// Handles cross-shard request forwarding.
#[derive(Debug)]
pub struct ShardForwarder {
    /// This node's ID.
    node_id: NodeId,

    /// Configuration.
    config: ShardForwardingConfig,

    /// Node addresses for forwarding.
    node_addresses: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,

    /// Pending forwarded requests awaiting response.
    pending_forwards: Arc<DashMap<u64, oneshot::Sender<ForwardResult>>>,

    /// Counter for generating unique request IDs.
    next_request_id: AtomicU64,
}

impl ShardForwarder {
    /// Create a new shard forwarder.
    pub fn new(node_id: NodeId, config: ShardForwardingConfig) -> Self {
        Self {
            node_id,
            config,
            node_addresses: Arc::new(RwLock::new(HashMap::new())),
            pending_forwards: Arc::new(DashMap::new()),
            next_request_id: AtomicU64::new(1),
        }
    }

    /// Create with default config.
    pub fn with_defaults(node_id: NodeId) -> Self {
        Self::new(node_id, ShardForwardingConfig::default())
    }

    /// Check if forwarding is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Register a node's address.
    pub fn register_node(&self, node_id: NodeId, addr: SocketAddr) {
        self.node_addresses.write().insert(node_id, addr);
    }

    /// Remove a node's address.
    pub fn unregister_node(&self, node_id: NodeId) {
        self.node_addresses.write().remove(&node_id);
    }

    /// Get a node's address.
    pub fn get_node_address(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.node_addresses.read().get(&node_id).copied()
    }

    /// Get the number of pending forwards.
    pub fn pending_count(&self) -> usize {
        self.pending_forwards.len()
    }

    /// Forward a command to a specific node for a shard.
    pub async fn forward_to_node(
        &self,
        target_node: NodeId,
        shard_id: ShardId,
        command: CacheCommand,
    ) -> Result<ForwardResult> {
        // Check if forwarding is enabled
        if !self.config.enabled {
            return Err(Error::ShardNotLocal {
                shard_id,
                target_node: Some(target_node),
            });
        }

        // Backpressure check
        if self.pending_forwards.len() >= self.config.max_pending_forwards {
            return Err(Error::ServerBusy {
                pending: self.pending_forwards.len(),
            });
        }

        // Get target node address
        let target_addr = self.get_node_address(target_node).ok_or_else(|| {
            tracing::warn!(
                node_id = self.node_id,
                target_node = target_node,
                shard_id = shard_id,
                "Forward failed: target node address unknown"
            );
            Error::ShardLeaderUnknown(shard_id)
        })?;

        // Generate request ID
        let request_id = self.next_request_id.fetch_add(1, Ordering::SeqCst);

        // Create completion channel
        let (tx, rx) = oneshot::channel();
        self.pending_forwards.insert(request_id, tx);

        // Create the forwarded command
        let forward_cmd = ShardForwardedCommand::new(request_id, self.node_id, shard_id, command);
        let msg = Message::ShardForwardedCommand(forward_cmd);

        tracing::debug!(
            node_id = self.node_id,
            target_node = target_node,
            shard_id = shard_id,
            request_id = request_id,
            "Forwarding shard request to target node"
        );

        // Send the message
        if let Err(e) = self.send_message(target_addr, &msg).await {
            self.pending_forwards.remove(&request_id);
            return Err(Error::ForwardFailed(format!(
                "Failed to send to node {}: {}",
                target_node, e
            )));
        }

        // Wait for response with timeout
        match tokio::time::timeout(self.config.timeout, rx).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => {
                // Channel closed
                self.pending_forwards.remove(&request_id);
                Err(Error::Internal("forward channel closed".into()))
            }
            Err(_) => {
                // Timeout
                self.pending_forwards.remove(&request_id);
                Err(Error::Timeout)
            }
        }
    }

    /// Handle an incoming ShardForwardResponse.
    ///
    /// This is called when we receive a response to a forwarded request.
    pub fn handle_response(&self, response: &ShardForwardResponse) {
        if let Some((_, tx)) = self.pending_forwards.remove(&response.request_id) {
            let result = ForwardResult {
                success: response.success,
                value: response.value.clone().map(Bytes::from),
                error: response.error.clone(),
            };
            let _ = tx.send(result);
        } else {
            tracing::warn!(
                request_id = response.request_id,
                "Received shard forward response for unknown request"
            );
        }
    }

    /// Send a message to a node.
    async fn send_message(&self, addr: SocketAddr, msg: &Message) -> Result<()> {
        // Connect with timeout
        let stream = tokio::time::timeout(self.config.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| {
                Error::ForwardFailed(format!("Connection timeout to {}", addr))
            })?
            .map_err(|e| Error::ForwardFailed(format!("Connection failed to {}: {}", addr, e)))?;

        let (mut reader, mut writer) = stream.into_split();

        // Serialize and frame the message
        let data =
            bincode::serialize(msg).map_err(|e| Error::ForwardFailed(format!("Serialize: {}", e)))?;

        let len = data.len() as u32;
        let mut framed = Vec::with_capacity(4 + data.len());
        framed.extend_from_slice(&len.to_be_bytes());
        framed.extend_from_slice(&data);

        // Send
        writer
            .write_all(&framed)
            .await
            .map_err(|e| Error::ForwardFailed(format!("Write failed: {}", e)))?;
        writer
            .flush()
            .await
            .map_err(|e| Error::ForwardFailed(format!("Flush failed: {}", e)))?;

        // Read response
        let mut len_buf = [0u8; 4];
        reader
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| Error::ForwardFailed(format!("Read length failed: {}", e)))?;

        let resp_len = u32::from_be_bytes(len_buf) as usize;
        if resp_len > 16 * 1024 * 1024 {
            return Err(Error::ForwardFailed("Response too large".into()));
        }

        let mut resp_buf = vec![0u8; resp_len];
        reader
            .read_exact(&mut resp_buf)
            .await
            .map_err(|e| Error::ForwardFailed(format!("Read response failed: {}", e)))?;

        // Deserialize response
        let resp_msg: Message = bincode::deserialize(&resp_buf)
            .map_err(|e| Error::ForwardFailed(format!("Deserialize response: {}", e)))?;

        // Handle the response
        if let Message::ShardForwardResponse(resp) = resp_msg {
            self.handle_response(&resp);
        }

        Ok(())
    }

    /// Get the shared pending forwards map for external access.
    pub fn pending_forwards(&self) -> &Arc<DashMap<u64, oneshot::Sender<ForwardResult>>> {
        &self.pending_forwards
    }
}

/// Statistics for shard forwarding.
#[derive(Debug, Clone, Default)]
pub struct ShardForwardingStats {
    /// Total forwards attempted.
    pub total_forwards: u64,
    /// Successful forwards.
    pub successful_forwards: u64,
    /// Failed forwards.
    pub failed_forwards: u64,
    /// Timed out forwards.
    pub timeout_forwards: u64,
    /// Current pending forwards.
    pub pending_forwards: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = ShardForwardingConfig::default();
        assert!(config.enabled);
        assert_eq!(config.timeout, Duration::from_secs(5));
        assert_eq!(config.max_pending_forwards, 5000);
    }

    #[test]
    fn test_config_disabled() {
        let config = ShardForwardingConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_forwarder_creation() {
        let forwarder = ShardForwarder::with_defaults(1);
        assert!(forwarder.is_enabled());
        assert_eq!(forwarder.pending_count(), 0);
    }

    #[test]
    fn test_node_registration() {
        let forwarder = ShardForwarder::with_defaults(1);

        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        forwarder.register_node(2, addr);

        assert_eq!(forwarder.get_node_address(2), Some(addr));
        assert_eq!(forwarder.get_node_address(3), None);

        forwarder.unregister_node(2);
        assert_eq!(forwarder.get_node_address(2), None);
    }
}
