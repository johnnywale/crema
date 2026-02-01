//! Cross-shard request forwarding for Multi-Raft mode.
//!
//! When a request arrives at a node that doesn't host the target shard's leader,
//! this module handles forwarding the request to the correct node.

use crate::consensus::transport::RaftMessageSender;
use crate::error::{Error, Result};
use crate::metrics::facade::{counter_inc, gauge_set, histogram_record};
use crate::network::rpc::{Message, ShardForwardResponse, ShardForwardedCommand, ShardId};
use crate::types::{CacheCommand, NodeId};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
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
}

impl Default for ShardForwardingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            timeout: Duration::from_secs(5),
            max_pending_forwards: 5000,
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
pub struct ShardForwarder {
    /// This node's ID.
    node_id: NodeId,

    /// Configuration.
    config: ShardForwardingConfig,

    /// Node addresses for forwarding.
    node_addresses: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,

    /// Pending forwarded requests awaiting response.
    /// Maps request_id -> (oneshot sender for the response, creation time for cleanup).
    pending_forwards: Arc<DashMap<u64, (oneshot::Sender<ForwardResult>, Instant)>>,

    /// Counter for generating unique request IDs.
    next_request_id: AtomicU64,

    /// Transport for sending messages (optional, set after initialization).
    transport: RwLock<Option<Arc<dyn RaftMessageSender>>>,
}

impl std::fmt::Debug for ShardForwarder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardForwarder")
            .field("node_id", &self.node_id)
            .field("config", &self.config)
            .field("pending_count", &self.pending_forwards.len())
            .field("transport_set", &self.transport.read().is_some())
            .finish()
    }
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
            transport: RwLock::new(None),
        }
    }

    /// Create with default config.
    pub fn with_defaults(node_id: NodeId) -> Self {
        Self::new(node_id, ShardForwardingConfig::default())
    }

    /// Set the transport for sending messages.
    pub fn set_transport(&self, transport: Arc<dyn RaftMessageSender>) {
        *self.transport.write() = Some(transport);
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
    #[allow(unused_variables)]
    pub async fn forward_to_node(
        &self,
        target_node: NodeId,
        shard_id: ShardId,
        command: CacheCommand,
    ) -> Result<ForwardResult> {
        let start_time = Instant::now();
        let node_id_str = self.node_id.to_string();
        let shard_id_str = shard_id.to_string();

        // Record forward attempt
        counter_inc!("crema_shard_forwards_total", "node_id" => node_id_str.clone(), "shard_id" => shard_id_str.clone(), "success" => "attempt");

        // Check if forwarding is enabled
        if !self.config.enabled {
            counter_inc!("crema_shard_forward_failures_total", "node_id" => node_id_str.clone(), "reason" => "disabled");
            return Err(Error::ShardNotLocal {
                shard_id,
                target_node: Some(target_node),
            });
        }

        // Backpressure check
        if self.pending_forwards.len() >= self.config.max_pending_forwards {
            counter_inc!("crema_shard_forward_failures_total", "node_id" => node_id_str.clone(), "reason" => "backpressure");
            return Err(Error::ServerBusy {
                pending: self.pending_forwards.len(),
            });
        }

        // Get transport (required for sending)
        let transport = self.transport.read().clone().ok_or_else(|| {
            tracing::error!(
                node_id = self.node_id,
                target_node = target_node,
                shard_id = shard_id,
                "Forward failed: transport not set"
            );
            Error::Internal("ShardForwarder transport not initialized".into())
        })?;

        // Generate request ID
        let request_id = self.next_request_id.fetch_add(1, Ordering::SeqCst);

        // Create completion channel
        let (tx, rx) = oneshot::channel();
        self.pending_forwards
            .insert(request_id, (tx, Instant::now()));

        // Create the forwarded command
        let forward_cmd = ShardForwardedCommand::new(request_id, self.node_id, shard_id, command);
        let msg = Message::ShardForwardedCommand(forward_cmd);

        tracing::debug!(
            node_id = self.node_id,
            target_node = target_node,
            shard_id = shard_id,
            request_id = request_id,
            "Forwarding shard request via transport"
        );

        // Send the message via transport (response comes back asynchronously through
        // NetworkServer -> CacheMessageHandler -> handle_response())
        if let Err(e) = transport.send_message(target_node, msg).await {
            self.pending_forwards.remove(&request_id);
            counter_inc!("crema_shard_forward_failures_total", "node_id" => node_id_str.clone(), "reason" => "send_failed");
            histogram_record!("crema_shard_forward_duration_seconds", start_time.elapsed().as_secs_f64(), "node_id" => node_id_str, "shard_id" => shard_id_str);
            return Err(Error::ForwardFailed(format!(
                "Failed to send to node {}: {}",
                target_node, e
            )));
        }

        // Update pending gauge
        gauge_set!("crema_shard_forward_pending", self.pending_forwards.len() as f64, "node_id" => node_id_str.clone());

        // Wait for response with timeout
        match tokio::time::timeout(self.config.timeout, rx).await {
            Ok(Ok(result)) => {
                counter_inc!("crema_shard_forwards_total", "node_id" => node_id_str.clone(), "shard_id" => shard_id_str.clone(), "success" => if result.success { "true" } else { "false" });
                histogram_record!("crema_shard_forward_duration_seconds", start_time.elapsed().as_secs_f64(), "node_id" => node_id_str, "shard_id" => shard_id_str);
                Ok(result)
            }
            Ok(Err(_)) => {
                // Channel closed
                self.pending_forwards.remove(&request_id);
                counter_inc!("crema_shard_forward_failures_total", "node_id" => node_id_str.clone(), "reason" => "channel_closed");
                histogram_record!("crema_shard_forward_duration_seconds", start_time.elapsed().as_secs_f64(), "node_id" => node_id_str, "shard_id" => shard_id_str);
                Err(Error::Internal("forward channel closed".into()))
            }
            Err(_) => {
                // Timeout
                self.pending_forwards.remove(&request_id);
                counter_inc!("crema_shard_forward_failures_total", "node_id" => node_id_str.clone(), "reason" => "timeout");
                histogram_record!("crema_shard_forward_duration_seconds", start_time.elapsed().as_secs_f64(), "node_id" => node_id_str, "shard_id" => shard_id_str);
                Err(Error::Timeout)
            }
        }
    }

    /// Handle an incoming ShardForwardResponse.
    ///
    /// This is called when we receive a response to a forwarded request.
    pub fn handle_response(&self, response: &ShardForwardResponse) {
        if let Some((_, (tx, _))) = self.pending_forwards.remove(&response.request_id) {
            let result = ForwardResult {
                success: response.success,
                value: response.value.clone().map(Bytes::from),
                error: response.error.clone(),
            };
            if tx.send(result).is_err() {
                tracing::debug!(
                    request_id = response.request_id,
                    "Forward response receiver dropped (requester likely timed out)"
                );
            }
        } else {
            tracing::warn!(
                request_id = response.request_id,
                "Received shard forward response for unknown request"
            );
        }
    }

    /// Get the shared pending forwards map for external access.
    pub fn pending_forwards(
        &self,
    ) -> &Arc<DashMap<u64, (oneshot::Sender<ForwardResult>, Instant)>> {
        &self.pending_forwards
    }

    /// Clean up stale pending forward entries.
    ///
    /// This should be called periodically to prevent memory leaks from orphaned requests
    /// (e.g., when a leader crashes or network partition occurs).
    pub fn cleanup_stale_entries(&self) -> u64 {
        let max_age = self.config.timeout + Duration::from_secs(5);
        let now = Instant::now();
        let mut removed = 0u64;
        self.pending_forwards.retain(|_id, (_tx, created_at)| {
            let is_stale = now.duration_since(*created_at) > max_age;
            if is_stale {
                removed = removed.saturating_add(1);
            }
            !is_stale
        });
        removed
    }

    /// Get the configured timeout for forwarded requests.
    pub fn timeout(&self) -> Duration {
        self.config.timeout
    }

    /// Send a raw message to a node without expecting a specific response format.
    ///
    /// This is used for broadcasts and other messages where we don't need to track
    /// the response in the pending_forwards map.
    pub async fn send_raw_message(
        &self,
        target_node: NodeId,
        _target_addr: SocketAddr,
        msg: Message,
    ) -> Result<()> {
        // Get transport (required for sending)
        let transport =
            self.transport.read().clone().ok_or_else(|| {
                Error::Internal("ShardForwarder transport not initialized".into())
            })?;

        // Send the message via transport
        transport.send_message(target_node, msg).await?;
        Ok(())
    }

    /// Send a raw message and wait for a response.
    ///
    /// This is used for request-response patterns like GetTopology where we need
    /// to wait for a specific response.
    pub async fn send_raw_message_with_response(
        &self,
        target_node: NodeId,
        target_addr: SocketAddr,
        msg: Message,
    ) -> Result<Message> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        // For request-response patterns, we need to establish a direct connection
        // and wait for the response, since the regular transport is fire-and-forget
        // for some message types.

        let mut stream = TcpStream::connect(target_addr).await.map_err(|e| {
            Error::Network(crate::error::NetworkError::ConnectionFailed {
                addr: target_addr.to_string(),
                reason: e.to_string(),
            })
        })?;

        // Serialize and send the message
        let framed = crate::network::rpc::frame_message(&msg).map_err(|e| {
            Error::Network(crate::error::NetworkError::Serialization(e.to_string()))
        })?;

        stream
            .write_all(&framed)
            .await
            .map_err(|e| Error::Network(crate::error::NetworkError::SendFailed(e.to_string())))?;

        // Read response with timeout
        let timeout_duration = self.config.timeout;
        let response = tokio::time::timeout(timeout_duration, async {
            // Read length prefix
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await?;
            let len = u32::from_be_bytes(len_buf) as usize;

            // Read message body
            let mut buf = vec![0u8; len];
            stream.read_exact(&mut buf).await?;

            // Deserialize
            crate::network::rpc::decode_message(&buf)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
        })
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|e| Error::Network(crate::error::NetworkError::ReceiveFailed(e.to_string())))?;

        tracing::debug!(
            node_id = self.node_id,
            target_node,
            "Received response to raw message"
        );

        Ok(response)
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
