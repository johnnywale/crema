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
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
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

    /// Interval for cleanup of stale pending entries.
    pub cleanup_interval: Duration,

    /// Maximum number of pooled connections per remote address.
    pub max_connections_per_host: usize,

    /// Maximum time a connection can remain idle in the pool.
    pub connection_idle_timeout: Duration,
}

impl Default for ShardForwardingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            timeout: Duration::from_secs(5),
            max_pending_forwards: 5000,
            cleanup_interval: Duration::from_secs(30),
            max_connections_per_host: 4,
            connection_idle_timeout: Duration::from_secs(60),
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

/// A pooled TCP connection with metadata.
struct PooledConnection {
    /// The TCP stream.
    stream: TcpStream,
    /// When this connection was last used.
    last_used: Instant,
}

/// Connection pool for a specific address.
struct ConnectionPool {
    /// Available (idle) connections.
    connections: VecDeque<PooledConnection>,
    /// Maximum connections to keep in pool.
    max_size: usize,
    /// Idle timeout for connections.
    idle_timeout: Duration,
}

impl ConnectionPool {
    fn new(max_size: usize, idle_timeout: Duration) -> Self {
        Self {
            connections: VecDeque::new(),
            max_size,
            idle_timeout,
        }
    }

    /// Try to get an idle connection from the pool.
    fn get(&mut self) -> Option<TcpStream> {
        let now = Instant::now();
        // Remove expired connections from front and find a valid one
        while let Some(conn) = self.connections.pop_front() {
            if now.duration_since(conn.last_used) < self.idle_timeout {
                return Some(conn.stream);
            }
            // Connection expired, drop it
        }
        None
    }

    /// Return a connection to the pool.
    fn put(&mut self, stream: TcpStream) {
        if self.connections.len() < self.max_size {
            self.connections.push_back(PooledConnection {
                stream,
                last_used: Instant::now(),
            });
        }
        // If pool is full, the stream is dropped
    }

    /// Remove expired connections from the pool.
    fn cleanup_expired(&mut self) {
        let now = Instant::now();
        self.connections
            .retain(|conn| now.duration_since(conn.last_used) < self.idle_timeout);
    }
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

    /// Flag to signal shutdown of the cleanup loop.
    shutdown_flag: Arc<AtomicBool>,

    /// Connection pool per remote address (for request-response patterns).
    connection_pools: Arc<Mutex<HashMap<SocketAddr, ConnectionPool>>>,
}

impl std::fmt::Debug for ShardForwarder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pool_count = self.connection_pools.lock().len();
        f.debug_struct("ShardForwarder")
            .field("node_id", &self.node_id)
            .field("config", &self.config)
            .field("pending_count", &self.pending_forwards.len())
            .field("transport_set", &self.transport.read().is_some())
            .field("shutdown", &self.shutdown_flag.load(Ordering::Relaxed))
            .field("connection_pools", &pool_count)
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
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            connection_pools: Arc::new(Mutex::new(HashMap::new())),
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

        // Check transport health before attempting to send
        if !transport.is_healthy() {
            counter_inc!("crema_shard_forward_failures_total", "node_id" => node_id_str.clone(), "reason" => "transport_unhealthy");
            return Err(Error::Internal(
                "Transport is unhealthy (dispatcher stopped)".into(),
            ));
        }

        // Check if target peer is registered
        if !transport.has_peer(target_node) {
            tracing::warn!(
                node_id = self.node_id,
                target_node,
                shard_id,
                "Target peer not registered in transport"
            );
            // Don't fail immediately - the transport may establish connection on send
        }

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

    /// Start a background cleanup loop that periodically removes stale pending entries
    /// and expired pooled connections.
    ///
    /// This spawns a background task that runs until shutdown() is called.
    /// Returns immediately after spawning the task.
    pub fn start_cleanup_loop(self: &Arc<Self>) {
        let forwarder = Arc::clone(self);
        let interval = forwarder.config.cleanup_interval;
        let node_id = forwarder.node_id;

        tokio::spawn(async move {
            tracing::debug!(node_id, "Starting shard forwarder cleanup loop");

            while !forwarder.shutdown_flag.load(Ordering::Relaxed) {
                tokio::time::sleep(interval).await;

                if forwarder.shutdown_flag.load(Ordering::Relaxed) {
                    break;
                }

                // Clean up stale pending forward entries
                let removed = forwarder.cleanup_stale_entries();
                if removed > 0 {
                    tracing::debug!(
                        node_id,
                        removed,
                        remaining = forwarder.pending_forwards.len(),
                        "Cleaned up stale forwarding entries"
                    );
                    counter_inc!("crema_shard_forward_stale_cleaned_total", "node_id" => node_id.to_string());
                }

                // Clean up expired pooled connections
                let connections_removed = forwarder.cleanup_connection_pools();
                if connections_removed > 0 {
                    tracing::debug!(
                        node_id,
                        connections_removed,
                        "Cleaned up expired pooled connections"
                    );
                    counter_inc!("crema_connection_pool_expired_total", "node_id" => node_id.to_string());
                }
            }

            tracing::debug!(node_id, "Shard forwarder cleanup loop stopped");
        });
    }

    /// Signal the cleanup loop to stop.
    pub fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::Relaxed);
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown_flag.load(Ordering::Relaxed)
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

    /// Get a connection from the pool or create a new one.
    async fn get_pooled_connection(&self, addr: SocketAddr) -> Result<TcpStream> {
        // First, try to get a pooled connection
        let pooled = {
            let mut pools = self.connection_pools.lock();
            pools.get_mut(&addr).and_then(|pool| pool.get())
        };

        if let Some(stream) = pooled {
            tracing::trace!(addr = %addr, "Reusing pooled connection");
            counter_inc!("crema_connection_pool_hits_total", "node_id" => self.node_id.to_string());
            return Ok(stream);
        }

        // No pooled connection available, create a new one
        tracing::trace!(addr = %addr, "Creating new connection");
        counter_inc!("crema_connection_pool_misses_total", "node_id" => self.node_id.to_string());

        TcpStream::connect(addr).await.map_err(|e| {
            Error::Network(crate::error::NetworkError::ConnectionFailed {
                addr: addr.to_string(),
                reason: e.to_string(),
            })
        })
    }

    /// Return a connection to the pool for reuse.
    fn return_connection_to_pool(&self, addr: SocketAddr, stream: TcpStream) {
        let mut pools = self.connection_pools.lock();
        let pool = pools.entry(addr).or_insert_with(|| {
            ConnectionPool::new(
                self.config.max_connections_per_host,
                self.config.connection_idle_timeout,
            )
        });
        pool.put(stream);
    }

    /// Clean up expired connections from all pools.
    pub fn cleanup_connection_pools(&self) -> usize {
        let mut pools = self.connection_pools.lock();
        let mut total_removed = 0;
        for pool in pools.values_mut() {
            let before = pool.connections.len();
            pool.cleanup_expired();
            total_removed += before - pool.connections.len();
        }
        // Remove empty pools
        pools.retain(|_, pool| !pool.connections.is_empty());
        total_removed
    }

    /// Send a raw message and wait for a response.
    ///
    /// This is used for request-response patterns like GetTopology where we need
    /// to wait for a specific response.
    ///
    /// Uses connection pooling to reduce connection churn.
    pub async fn send_raw_message_with_response(
        &self,
        target_node: NodeId,
        target_addr: SocketAddr,
        msg: Message,
    ) -> Result<Message> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Get a connection from pool or create new
        let mut stream = self.get_pooled_connection(target_addr).await?;

        // Serialize and send the message
        let framed = crate::network::rpc::frame_message(&msg).map_err(|e| {
            Error::Network(crate::error::NetworkError::Serialization(e.to_string()))
        })?;

        // Send with error handling - don't return connection on error
        if let Err(e) = stream.write_all(&framed).await {
            // Connection is broken, don't return to pool
            return Err(Error::Network(crate::error::NetworkError::SendFailed(
                e.to_string(),
            )));
        }

        // Read response with timeout
        let timeout_duration = self.config.timeout;
        let result = tokio::time::timeout(timeout_duration, async {
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
        .await;

        match result {
            Ok(Ok(response)) => {
                // Success - return connection to pool for reuse
                self.return_connection_to_pool(target_addr, stream);

                tracing::debug!(
                    node_id = self.node_id,
                    target_node,
                    "Received response to raw message"
                );

                Ok(response)
            }
            Ok(Err(e)) => {
                // IO error - connection may be broken, don't return to pool
                Err(Error::Network(crate::error::NetworkError::ReceiveFailed(
                    e.to_string(),
                )))
            }
            Err(_) => {
                // Timeout - connection may be in bad state, don't return to pool
                Err(Error::Timeout)
            }
        }
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
