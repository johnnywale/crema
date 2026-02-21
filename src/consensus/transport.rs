use crate::error::{NetworkError, Result};
use crate::network::rpc::{encode_message_into, Message, RaftMessageWrapper};
use crate::types::NodeId;
use crate::Error;
use crate::{counter_inc, gauge_set, histogram_record_duration};
use bytes::BytesMut;
use futures::future::BoxFuture;
use parking_lot::RwLock;
use raft::prelude::Message as RaftMessage;
use socket2::{SockRef, TcpKeepalive};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, error, info, trace, warn};

/// Trait for sending Raft messages.
///
/// This trait allows injection of custom message routing logic, enabling
/// shard-aware transport for Multi-Raft setups while maintaining backward
/// compatibility with the existing single-Raft implementation.
///
/// # Usage
///
/// The default `RaftTransport` implements this trait directly. For Multi-Raft,
/// a `ShardRaftTransport` adapter wraps messages with shard IDs before sending.
///
/// ```text
/// Single Raft:  RaftNode → RaftTransport (impl RaftMessageSender) → Network
/// Multi-Raft:   ShardRaftNode → ShardRaftTransport (impl RaftMessageSender)
///                            → ShardTransportMultiplexer → Network
/// ```
pub trait RaftMessageSender: Send + Sync + 'static {
    /// Send multiple Raft messages.
    ///
    /// This is the primary method for sending Raft protocol messages (heartbeats,
    /// vote requests, log appends, etc.). Messages are queued and sent asynchronously.
    fn send_messages(&self, msgs: Vec<RaftMessage>);

    /// Send a single arbitrary message to a peer.
    ///
    /// Unlike `send_messages()` which takes Raft protocol messages, this method
    /// sends our custom Message enum directly. Used for forwarded commands,
    /// forwarded responses, and other application-level messages.
    fn send_message(&self, to: NodeId, msg: Message) -> BoxFuture<'_, Result<()>>;

    /// Add a peer to the sender's peer list.
    ///
    /// The transport will establish connections to this peer as needed.
    fn add_peer(&self, id: NodeId, addr: SocketAddr) -> BoxFuture<'_, ()>;

    /// Remove a peer from the sender's peer list.
    ///
    /// Any pending messages to this peer may be dropped.
    fn remove_peer(&self, id: NodeId);

    /// Get a peer's address.
    ///
    /// Returns `Some(addr)` if the peer is registered, `None` otherwise.
    /// Useful for debugging and testing.
    fn get_peer(&self, id: NodeId) -> Option<SocketAddr>;

    /// Get all registered peer IDs.
    ///
    /// Returns a list of all peer node IDs that have been added to this sender.
    /// Useful for debugging and testing.
    fn peer_ids(&self) -> Vec<NodeId>;

    /// Get transport metrics snapshot.
    ///
    /// Returns current statistics about messages sent, failed, connections, etc.
    fn metrics(&self) -> TransportMetricsSnapshot;

    /// Check if the transport is healthy and ready to send messages.
    ///
    /// Returns true if the transport can accept and send messages.
    fn is_healthy(&self) -> bool;

    /// Check if a peer is registered in the transport.
    ///
    /// Returns true if the peer has been added via `add_peer()`.
    fn has_peer(&self, id: NodeId) -> bool;

    /// Shutdown the sender.
    ///
    /// Gracefully closes all connections and stops background tasks.
    fn shutdown(&self) -> BoxFuture<'_, ()>;
}

/// Message priority
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessagePriority {
    High,   // heartbeat, vote
    Normal, // log append, snapshot
}

/// Backpressure event type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureEvent {
    /// Queue is full, message was dropped
    QueueFull {
        peer_id: NodeId,
        priority: MessagePriority,
    },
    /// Queue is nearly full (reached 80% capacity)
    QueueHighWatermark {
        peer_id: NodeId,
        priority: MessagePriority,
        current_size: usize,
    },
    /// Queue returned to normal (below 50% capacity)
    QueueNormal {
        peer_id: NodeId,
        priority: MessagePriority,
    },
}

/// Backpressure callback function type
pub type BackpressureCallback = Arc<dyn Fn(BackpressureEvent) + Send + Sync>;

/// Pending message to be sent
#[derive(Debug)]
struct PendingMessage {
    #[allow(dead_code)]
    to: NodeId,
    msg: Message,
    #[allow(dead_code)]
    enqueued_at: Instant,
    /// Estimated size in bytes (cached for batch size tracking)
    estimated_size: usize,
}

impl PendingMessage {
    /// Create a new pending message with size estimation.
    fn new(to: NodeId, msg: Message) -> Self {
        let estimated_size = Self::estimate_message_size(&msg);
        Self {
            to,
            msg,
            enqueued_at: Instant::now(),
            estimated_size,
        }
    }

    /// Estimate the serialized size of a message.
    /// This is an approximation used for batch size limiting.
    fn estimate_message_size(msg: &Message) -> usize {
        // Base overhead for enum variant + framing
        const BASE_OVERHEAD: usize = 32;

        match msg {
            Message::Raft(wrapper) => BASE_OVERHEAD + wrapper.data.len(),
            Message::ClientRequest(req) => {
                BASE_OVERHEAD + Self::estimate_command_size(&req.command)
            }
            Message::ForwardedCommand(fwd) => {
                BASE_OVERHEAD + Self::estimate_command_size(&fwd.command)
            }
            Message::ShardForwardedCommand(fwd) => {
                BASE_OVERHEAD + Self::estimate_command_size(&fwd.command)
            }
            Message::ShardRaft(msg) => BASE_OVERHEAD + msg.raft_message.data.len(),
            Message::MigrationFetchResponse(resp) => {
                BASE_OVERHEAD
                    + resp
                        .entries
                        .iter()
                        .map(|e| e.key.len() + e.value.len() + 16)
                        .sum::<usize>()
            }
            Message::MigrationApplyRequest(req) => {
                BASE_OVERHEAD
                    + req
                        .entries
                        .iter()
                        .map(|e| e.key.len() + e.value.len() + 16)
                        .sum::<usize>()
            }
            Message::MigrationProposalForward(fwd) => BASE_OVERHEAD + fwd.command_bytes.len(),
            // For other message types, use a conservative estimate
            _ => BASE_OVERHEAD + 256,
        }
    }

    /// Estimate the size of a cache command.
    fn estimate_command_size(cmd: &crate::types::CacheCommand) -> usize {
        use crate::types::CacheCommand;
        match cmd {
            CacheCommand::Put { key, value, .. } => key.len() + value.len() + 32,
            CacheCommand::Delete { key } => key.len() + 16,
            CacheCommand::Get { key } => key.len() + 16,
            CacheCommand::Clear => 16,
        }
    }
}

/// RAII wrapper: connection + permit binding to prevent leaks
struct TiedConnection {
    stream: TcpStream,
    _permit: OwnedSemaphorePermit, // automatically released on Drop
}

impl TiedConnection {
    fn new(stream: TcpStream, permit: OwnedSemaphorePermit) -> Self {
        Self {
            stream,
            _permit: permit,
        }
    }
}

/// Transport configuration
#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub max_retries: usize,
    pub initial_retry_delay: Duration,
    pub max_retry_delay: Duration,
    pub connect_timeout: Duration,
    pub write_timeout: Duration,
    pub max_connections: usize,
    pub enable_tcp_nodelay: bool,
    pub tcp_keepalive_time: Duration,
    pub tcp_keepalive_interval: Duration,
    pub tcp_keepalive_retries: u32,
    pub per_peer_queue_size: usize,
    pub enable_connection_prewarming: bool,
    pub enable_retry_jitter: bool,
    /// Idle connection timeout. If no messages are sent/received within this duration,
    /// the connection is automatically closed to free resources.
    /// Set to None to disable idle timeout.
    pub idle_timeout: Option<Duration>,
    /// Message batching delay. When enabled, messages are collected for this duration
    /// before being sent together.
    /// Set to None to disable batching.
    pub batch_delay: Option<Duration>,
    /// Maximum number of messages per batch
    pub batch_max_messages: usize,
    /// Maximum batch size in bytes. Prevents OOM from large messages.
    /// When the batch size reaches this limit, it is sent immediately even if
    /// the message count has not reached batch_max_messages.
    pub batch_max_bytes: usize,
    /// Maximum retries during connection establishment
    pub max_connect_retries: usize,
    /// Initial delay for connection retries
    pub initial_connect_retry_delay: Duration,
    /// Maximum delay for connection retries
    pub max_connect_retry_delay: Duration,
    /// Pending retry message queue size per peer
    pub max_pending_retries: usize,
    /// Background reconnect interval after connection failure (for tc18 fix).
    /// Even without new messages, the worker will periodically attempt to re-establish the connection.
    pub background_reconnect_interval: Option<Duration>,
    /// Duration of the forced reconnect window after a connection failure.
    /// During this window, reconnection is attempted aggressively.
    pub force_reconnect_window: Duration,
    /// Worker shutdown timeout - how long to wait for each worker to flush during shutdown.
    /// Use a shorter value for tests (e.g., 50ms) vs production (1s).
    pub worker_shutdown_timeout: Duration,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_retry_delay: Duration::from_millis(100),
            max_retry_delay: Duration::from_secs(2),
            connect_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            max_connections: 1000,
            enable_tcp_nodelay: true,
            tcp_keepalive_time: Duration::from_secs(60),
            tcp_keepalive_interval: Duration::from_secs(10),
            tcp_keepalive_retries: 3,
            // TC22 fix: Increase queue size to prevent backpressure under high load
            per_peer_queue_size: 4096,
            enable_connection_prewarming: true,
            enable_retry_jitter: true,
            idle_timeout: Some(Duration::from_secs(300)), // 5 minutes default
            batch_delay: None,                            // Disabled by default
            batch_max_messages: 32,
            batch_max_bytes: 4 * 1024 * 1024, // 4MB default - prevents OOM from large messages
            max_connect_retries: 3,
            initial_connect_retry_delay: Duration::from_millis(50),
            max_connect_retry_delay: Duration::from_millis(500),
            max_pending_retries: 10,
            // Attempt reconnection every 500ms until successful
            background_reconnect_interval: Some(Duration::from_millis(500)),
            force_reconnect_window: Duration::from_secs(30),
            worker_shutdown_timeout: Duration::from_secs(1),
        }
    }
}

impl TransportConfig {
    /// Create a fast configuration suitable for tests.
    /// Uses shorter timeouts to speed up test execution.
    pub fn fast_for_tests() -> Self {
        Self {
            worker_shutdown_timeout: Duration::from_millis(50),
            connect_timeout: Duration::from_millis(500),
            write_timeout: Duration::from_millis(500),
            initial_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
            initial_connect_retry_delay: Duration::from_millis(10),
            max_connect_retry_delay: Duration::from_millis(100),
            idle_timeout: Some(Duration::from_secs(10)),
            background_reconnect_interval: Some(Duration::from_millis(100)),
            force_reconnect_window: Duration::from_secs(5),
            ..Default::default()
        }
    }
}

/// Worker control command
#[derive(Debug)]
enum WorkerCommand {
    /// Graceful stop (flush the queue first)
    Stop(oneshot::Sender<()>),
}

/// Transport layer control command
#[derive(Debug)]
enum TransportCommand {
    UpdatePeer {
        peer_id: NodeId,
        new_addr: SocketAddr,
    },
    RemovePeer {
        peer_id: NodeId,
    },
    Shutdown(oneshot::Sender<()>),
}

/// Detailed transport metrics
#[derive(Debug, Default)]
pub struct TransportMetrics {
    pub messages_sent: AtomicU64,
    pub messages_failed: AtomicU64,
    pub high_priority_sent: AtomicU64,
    pub normal_priority_sent: AtomicU64,
    pub connections_created: AtomicU64,
    pub connections_failed: AtomicU64,
    pub active_connections: AtomicUsize,
    /// Total send latency in microseconds, used to compute average latency
    pub total_send_latency_us: AtomicU64,
    pub send_count_for_latency: AtomicU64,
    /// Retry count during connection establishment
    pub connection_retries: AtomicU64,
    /// Messages dropped due to full queue
    pub messages_dropped_queue_full: AtomicU64,
    /// Current total number of pending retry messages
    pub pending_retries: AtomicUsize,
    /// Background reconnection attempt count
    pub background_reconnect_attempts: AtomicU64,
}

impl TransportMetrics {
    pub fn snapshot(&self) -> TransportMetricsSnapshot {
        let send_count = self.send_count_for_latency.load(Ordering::Relaxed);
        let avg_latency_us = if send_count > 0 {
            self.total_send_latency_us.load(Ordering::Relaxed) / send_count
        } else {
            0
        };

        TransportMetricsSnapshot {
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            messages_failed: self.messages_failed.load(Ordering::Relaxed),
            high_priority_sent: self.high_priority_sent.load(Ordering::Relaxed),
            normal_priority_sent: self.normal_priority_sent.load(Ordering::Relaxed),
            connections_created: self.connections_created.load(Ordering::Relaxed),
            connections_failed: self.connections_failed.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            average_send_latency_us: avg_latency_us,
            connection_retries: self.connection_retries.load(Ordering::Relaxed),
            messages_dropped_queue_full: self.messages_dropped_queue_full.load(Ordering::Relaxed),
            pending_retries: self.pending_retries.load(Ordering::Relaxed),
            background_reconnect_attempts: self
                .background_reconnect_attempts
                .load(Ordering::Relaxed),
        }
    }

    /// Record send latency for metrics tracking.
    pub fn record_send_latency(&self, latency: Duration) {
        self.total_send_latency_us
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
        self.send_count_for_latency.fetch_add(1, Ordering::Relaxed);

        // Also record to new metrics facade
        histogram_record_duration!(
            crate::metrics::descriptors::TRANSPORT_SEND_DURATION_SECONDS,
            latency
        );
    }

    /// Record a successful message send to the new metrics facade.
    pub fn record_message_sent(&self, priority: MessagePriority) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        match priority {
            MessagePriority::High => {
                self.high_priority_sent.fetch_add(1, Ordering::Relaxed);
                counter_inc!(
                    crate::metrics::descriptors::TRANSPORT_MESSAGES_SENT_TOTAL,
                    "priority" => "high"
                );
            }
            MessagePriority::Normal => {
                self.normal_priority_sent.fetch_add(1, Ordering::Relaxed);
                counter_inc!(
                    crate::metrics::descriptors::TRANSPORT_MESSAGES_SENT_TOTAL,
                    "priority" => "normal"
                );
            }
        }
    }

    /// Record a failed message send.
    #[allow(unused_variables)]
    pub fn record_message_failed(&self, reason: &str) {
        self.messages_failed.fetch_add(1, Ordering::Relaxed);
        let reason_owned = reason.to_string();
        counter_inc!(
            crate::metrics::descriptors::TRANSPORT_MESSAGES_FAILED_TOTAL,
            "reason" => reason_owned
        );
    }

    /// Record a connection created.
    pub fn record_connection_created(&self) {
        self.connections_created.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        counter_inc!(crate::metrics::descriptors::TRANSPORT_CONNECTIONS_CREATED_TOTAL);
        gauge_set!(
            crate::metrics::descriptors::TRANSPORT_CONNECTIONS_ACTIVE,
            self.active_connections.load(Ordering::Relaxed) as f64
        );
    }

    /// Record a connection closed.
    pub fn record_connection_closed(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
        gauge_set!(
            crate::metrics::descriptors::TRANSPORT_CONNECTIONS_ACTIVE,
            self.active_connections.load(Ordering::Relaxed) as f64
        );
    }

    /// Record a connection failure.
    #[allow(unused_variables)]
    pub fn record_connection_failed(&self, reason: &str) {
        self.connections_failed.fetch_add(1, Ordering::Relaxed);
        let reason_owned = reason.to_string();
        counter_inc!(
            crate::metrics::descriptors::TRANSPORT_CONNECTIONS_FAILED_TOTAL,
            "reason" => reason_owned
        );
    }

    /// Record a queue full event.
    #[allow(unused_variables)]
    pub fn record_queue_full(&self, priority: MessagePriority) {
        self.messages_dropped_queue_full
            .fetch_add(1, Ordering::Relaxed);
        let priority_str = match priority {
            MessagePriority::High => "high",
            MessagePriority::Normal => "normal",
        };
        counter_inc!(
            crate::metrics::descriptors::TRANSPORT_QUEUE_FULL_TOTAL,
            "priority" => priority_str
        );
    }
}

#[derive(Debug, Clone)]
pub struct TransportMetricsSnapshot {
    pub messages_sent: u64,
    pub messages_failed: u64,
    pub high_priority_sent: u64,
    pub normal_priority_sent: u64,
    pub connections_created: u64,
    pub connections_failed: u64,
    pub active_connections: usize,
    /// Average send latency in microseconds
    pub average_send_latency_us: u64,
    /// Retry count during connection establishment
    pub connection_retries: u64,
    /// Messages dropped due to full queue
    pub messages_dropped_queue_full: u64,
    /// Current total number of pending retry messages
    pub pending_retries: usize,
    /// Background reconnection attempt count
    pub background_reconnect_attempts: u64,
}

/// Per-peer worker state
struct PeerWorker {
    peer_id: NodeId,
    #[allow(dead_code)]
    addr: SocketAddr,
    high_priority_tx: mpsc::Sender<PendingMessage>,
    normal_priority_tx: mpsc::Sender<PendingMessage>,
    control_tx: mpsc::UnboundedSender<WorkerCommand>,
    handle: tokio::task::JoinHandle<()>,
}

/// Raft message transport layer
pub struct RaftTransport {
    node_id: NodeId,
    peers: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
    workers: Arc<RwLock<HashMap<NodeId, PeerWorker>>>,
    command_tx: mpsc::UnboundedSender<TransportCommand>,
    #[allow(dead_code)]
    dispatcher_handle: Arc<tokio::task::JoinHandle<()>>,
    config: TransportConfig,
    metrics: Arc<TransportMetrics>,
    connection_semaphore: Arc<Semaphore>,
    /// Backpressure callback to notify the Raft state machine to slow down sending
    backpressure_callback: Option<BackpressureCallback>,
    /// Pending message queue: buffers messages when a peer is known but its worker is not yet ready
    pending_messages: Arc<RwLock<HashMap<NodeId, VecDeque<PendingMessage>>>>,
}

impl RaftTransport {
    pub fn new(node_id: NodeId) -> Self {
        Self::with_config(node_id, TransportConfig::default())
    }

    pub fn with_config(node_id: NodeId, config: TransportConfig) -> Self {
        let peers = Arc::new(RwLock::new(HashMap::new()));
        let workers = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(TransportMetrics::default());
        let connection_semaphore = Arc::new(Semaphore::new(config.max_connections));
        let pending_messages = Arc::new(RwLock::new(HashMap::new()));

        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let dispatcher_handle = {
            let workers = workers.clone();
            let peers = peers.clone();
            let config = config.clone();
            let metrics = metrics.clone();
            let semaphore = connection_semaphore.clone();

            tokio::spawn(async move {
                Self::dispatcher_loop(
                    node_id, workers, peers, command_rx, config, metrics, semaphore,
                )
                .await;
            })
        };

        info!(
            node_id,
            "RaftTransport created with enhanced reconnection logic"
        );

        Self {
            node_id,
            peers,
            workers,
            command_tx,
            dispatcher_handle: Arc::new(dispatcher_handle),
            config,
            metrics,
            connection_semaphore,
            backpressure_callback: None,
            pending_messages,
        }
    }

    /// Set the backpressure callback.
    /// Called when the queue is full or reaches high watermark, to notify upper layers to slow down.
    pub fn set_backpressure_callback(&mut self, callback: BackpressureCallback) {
        self.backpressure_callback = Some(callback);
    }

    /// Create a Transport with a backpressure callback
    pub fn with_backpressure_callback(
        node_id: NodeId,
        config: TransportConfig,
        callback: BackpressureCallback,
    ) -> Self {
        let mut transport = Self::with_config(node_id, config);
        transport.backpressure_callback = Some(callback);
        transport
    }

    pub async fn add_peer(&self, id: NodeId, addr: SocketAddr) {
        self.peers.write().insert(id, addr);

        if self.config.enable_connection_prewarming {
            self.ensure_worker(id, addr).await;
        }

        debug!(node_id = self.node_id, peer_id = id, %addr, "Peer added");
    }

    pub fn update_peer(&self, id: NodeId, addr: SocketAddr) {
        let _ = self.command_tx.send(TransportCommand::UpdatePeer {
            peer_id: id,
            new_addr: addr,
        });
        debug!(node_id = self.node_id, peer_id = id, %addr, "Peer update queued");
    }

    pub fn remove_peer(&self, id: NodeId) {
        let _ = self
            .command_tx
            .send(TransportCommand::RemovePeer { peer_id: id });
        debug!(node_id = self.node_id, peer_id = id, "Peer removal queued");
    }

    pub fn get_peer(&self, id: NodeId) -> Option<SocketAddr> {
        self.peers.read().get(&id).copied()
    }

    pub fn peer_ids(&self) -> Vec<NodeId> {
        self.peers.read().keys().copied().collect()
    }

    pub fn peer_count(&self) -> usize {
        self.peers.read().len()
    }

    /// Check if the transport is healthy and ready to send messages.
    ///
    /// Returns true if:
    /// - The dispatcher task is still running
    /// - The command channel is not closed
    pub fn is_healthy(&self) -> bool {
        // Check if dispatcher task is still running
        !self.dispatcher_handle.is_finished() && !self.command_tx.is_closed()
    }

    /// Check if a specific peer is registered and has an active worker.
    pub fn is_peer_connected(&self, peer_id: NodeId) -> bool {
        self.peers.read().contains_key(&peer_id) && self.workers.read().contains_key(&peer_id)
    }

    /// Check if a peer is registered (may not have active worker yet).
    pub fn has_peer(&self, peer_id: NodeId) -> bool {
        self.peers.read().contains_key(&peer_id)
    }

    /// Get the count of active workers (peers with established connections).
    pub fn active_worker_count(&self) -> usize {
        self.workers.read().len()
    }

    pub fn metrics(&self) -> TransportMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Send a Raft message (with priority routing and backpressure feedback)
    pub fn send(&self, msg: RaftMessage) -> Result<()> {
        let to = msg.to;
        let msg_type = msg.msg_type;
        let priority = Self::determine_priority(&msg);

        let wrapper = RaftMessageWrapper::from_raft_message(&msg)
            .map_err(|e| NetworkError::Serialization(e.to_string()))?;

        let message = Message::Raft(wrapper);
        let pending = PendingMessage::new(to, message);

        // Select queue based on priority
        let worker = {
            let workers = self.workers.read();
            workers
                .get(&to)
                .map(|w| (w.high_priority_tx.clone(), w.normal_priority_tx.clone()))
        };

        if let Some((hp_tx, np_tx)) = worker {
            let tx = match priority {
                MessagePriority::High => &hp_tx,
                MessagePriority::Normal => &np_tx,
            };

            // Calculate queue utilization and trigger backpressure callback
            let capacity = tx.capacity();
            let max_capacity = tx.max_capacity();
            let current_size = max_capacity - capacity;
            let usage_percent = (current_size * 100) / max_capacity;

            // Check backpressure state
            if let Some(ref callback) = self.backpressure_callback {
                if usage_percent >= 80 {
                    callback(BackpressureEvent::QueueHighWatermark {
                        peer_id: to,
                        priority,
                        current_size,
                    });
                } else if usage_percent < 50 && current_size > 0 {
                    // Only notify recovery when queue has data but is below 50%
                    callback(BackpressureEvent::QueueNormal {
                        peer_id: to,
                        priority,
                    });
                }
            }

            match tx.try_send(pending) {
                Ok(_) => {}
                Err(_) => {
                    // Queue full, trigger backpressure callback
                    if let Some(ref callback) = self.backpressure_callback {
                        callback(BackpressureEvent::QueueFull {
                            peer_id: to,
                            priority,
                        });
                    }
                    return Err(Error::from(NetworkError::SendFailed(
                        "peer queue full".to_string(),
                    )));
                }
            }
        } else {
            // Worker does not exist, check if peer is known
            let peer_known = self.peers.read().contains_key(&to);
            if peer_known {
                // Peer is known but worker is not ready yet; queue the message for later
                let mut pending_map = self.pending_messages.write();
                let queue = pending_map.entry(to).or_default();

                // Limit pending queue size to prevent memory bloat
                if queue.len() < self.config.max_pending_retries {
                    queue.push_back(pending);
                    debug!(
                        node_id = self.node_id,
                        to,
                        queue_len = queue.len(),
                        "Message queued for pending peer"
                    );
                } else {
                    // Queue full, drop message
                    self.metrics
                        .messages_dropped_queue_full
                        .fetch_add(1, Ordering::Relaxed);
                    debug!(
                        node_id = self.node_id,
                        to, "Pending queue full, message dropped"
                    );
                    return Err(Error::from(NetworkError::SendFailed(
                        "pending queue full".to_string(),
                    )));
                }
            } else {
                debug!(node_id = self.node_id, to, "Unknown peer, message dropped");
                return Err(Error::from(NetworkError::SendFailed(
                    "unknown peer".to_string(),
                )));
            }
        }

        trace!(
            from = self.node_id,
            to = to,
            msg_type = ?msg_type,
            priority = ?priority,
            "Message queued"
        );

        Ok(())
    }

    pub fn send_messages(&self, msgs: Vec<RaftMessage>) {
        for msg in msgs {
            if let Err(e) = self.send(msg) {
                warn!(error = %e, "Failed to queue message");
            }
        }
    }

    /// Send an arbitrary Message to a peer (for forwarding, etc.).
    ///
    /// Unlike `send()` which takes RaftMessage, this method sends our custom
    /// Message enum directly. Used for ForwardedCommand/ForwardResponse.
    pub async fn send_message(&self, to: NodeId, msg: Message) -> Result<()> {
        let pending = PendingMessage::new(to, msg);

        // Try to send via worker (use high priority for forwarded commands)
        let worker = {
            let workers = self.workers.read();
            workers.get(&to).map(|w| w.high_priority_tx.clone())
        };

        if let Some(tx) = worker {
            match tx.try_send(pending) {
                Ok(_) => {
                    trace!(from = self.node_id, to, "Custom message queued");
                    Ok(())
                }
                Err(_) => Err(Error::from(NetworkError::SendFailed(
                    "peer queue full".to_string(),
                ))),
            }
        } else {
            // Check if peer is known but worker not ready
            let peer_known = self.peers.read().contains_key(&to);
            if peer_known {
                // Ensure worker exists
                if let Some(addr) = self.get_peer(to) {
                    self.ensure_worker(to, addr).await;
                    // Retry send
                    let worker = {
                        let workers = self.workers.read();
                        workers.get(&to).map(|w| w.high_priority_tx.clone())
                    };
                    if let Some(tx) = worker {
                        match tx.try_send(pending) {
                            Ok(_) => return Ok(()),
                            Err(_) => {
                                return Err(Error::from(NetworkError::SendFailed(
                                    "peer queue full after worker creation".to_string(),
                                )));
                            }
                        }
                    }
                }
            }
            Err(Error::from(NetworkError::SendFailed(
                "unknown peer".to_string(),
            )))
        }
    }

    pub async fn shutdown(&self) {
        info!(node_id = self.node_id, "Shutting down transport");

        let (tx, rx) = oneshot::channel();
        let _ = self.command_tx.send(TransportCommand::Shutdown(tx));

        match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(_)) => info!(node_id = self.node_id, "Transport shutdown complete"),
            Ok(Err(_)) => warn!(node_id = self.node_id, "Shutdown channel dropped"),
            Err(_) => warn!(node_id = self.node_id, "Transport shutdown timeout"),
        }
    }

    async fn ensure_worker(&self, peer_id: NodeId, addr: SocketAddr) {
        let hp_tx_clone;
        {
            let mut workers = self.workers.write();

            if workers.contains_key(&peer_id) {
                return;
            }

            let (hp_tx, hp_rx) = mpsc::channel(self.config.per_peer_queue_size);
            let (np_tx, np_rx) = mpsc::channel(self.config.per_peer_queue_size);
            let (control_tx, control_rx) = mpsc::unbounded_channel();

            hp_tx_clone = hp_tx.clone();

            let handle = {
                let config = self.config.clone();
                let metrics = self.metrics.clone();
                let semaphore = self.connection_semaphore.clone();
                let prewarm = self.config.enable_connection_prewarming;

                tokio::spawn(async move {
                    Self::peer_worker_loop(
                        peer_id, addr, hp_rx, np_rx, control_rx, config, metrics, semaphore,
                        prewarm,
                    )
                    .await;
                })
            };

            workers.insert(
                peer_id,
                PeerWorker {
                    peer_id,
                    addr,
                    high_priority_tx: hp_tx,
                    normal_priority_tx: np_tx,
                    control_tx,
                    handle,
                },
            );

            debug!(node_id = self.node_id, peer_id, "Peer worker created");
        }

        // After worker creation, immediately forward queued pending messages to the worker
        let pending_msgs: Vec<PendingMessage> = {
            let mut pending_map = self.pending_messages.write();
            pending_map
                .remove(&peer_id)
                .map(|q| q.into_iter().collect())
                .unwrap_or_default()
        };

        if !pending_msgs.is_empty() {
            let count = pending_msgs.len();
            for msg in pending_msgs {
                // Send pending messages to the high-priority queue (these are early critical messages)
                if hp_tx_clone.try_send(msg).is_err() {
                    debug!(
                        node_id = self.node_id,
                        peer_id, "Failed to forward pending message to worker"
                    );
                }
            }
            debug!(
                node_id = self.node_id,
                peer_id, count, "Forwarded pending messages to worker"
            );
        }
    }

    /// Dispatcher: processes control commands, supports graceful shutdown
    async fn dispatcher_loop(
        node_id: NodeId,
        workers: Arc<RwLock<HashMap<NodeId, PeerWorker>>>,
        peers: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
        mut command_rx: mpsc::UnboundedReceiver<TransportCommand>,
        config: TransportConfig,
        metrics: Arc<TransportMetrics>,
        semaphore: Arc<Semaphore>,
    ) {
        info!(node_id, "Dispatcher loop started");

        while let Some(cmd) = command_rx.recv().await {
            match cmd {
                TransportCommand::UpdatePeer { peer_id, new_addr } => {
                    // 1. Clean up old worker asynchronously without blocking the dispatcher loop
                    if let Some(worker) = workers.write().remove(&peer_id) {
                        tokio::spawn(async move {
                            let (tx, rx) = oneshot::channel();
                            // 1. Send stop signal
                            if worker.control_tx.send(WorkerCommand::Stop(tx)).is_ok() {
                                // 2. Allow a short grace period (e.g., 200ms; can be shorter in test environments)
                                if (tokio::time::timeout(Duration::from_millis(200), rx).await)
                                    .is_err()
                                {
                                    debug!(peer_id, "Worker graceful stop timeout, forcing abort");
                                }
                            }
                            // 3. Regardless of the outcome, abort the handle to release all resources (including the permit)
                            worker.handle.abort();
                        });
                    }

                    // 2. Update address immediately and create new worker
                    peers.write().insert(peer_id, new_addr);

                    let (hp_tx, hp_rx) = mpsc::channel(config.per_peer_queue_size);
                    let (np_tx, np_rx) = mpsc::channel(config.per_peer_queue_size);
                    let (control_tx, control_rx) = mpsc::unbounded_channel();

                    let worker_handle = {
                        let config = config.clone();
                        let metrics = metrics.clone();
                        let semaphore = semaphore.clone();
                        let prewarm = config.enable_connection_prewarming;
                        tokio::spawn(async move {
                            Self::peer_worker_loop(
                                peer_id, new_addr, hp_rx, np_rx, control_rx, config, metrics,
                                semaphore, prewarm,
                            )
                            .await;
                        })
                    };

                    workers.write().insert(
                        peer_id,
                        PeerWorker {
                            peer_id,
                            addr: new_addr,
                            high_priority_tx: hp_tx,
                            normal_priority_tx: np_tx,
                            control_tx,
                            handle: worker_handle,
                        },
                    );

                    info!(node_id, peer_id, %new_addr, "Peer worker replaced asynchronously");
                }

                TransportCommand::RemovePeer { peer_id } => {
                    peers.write().remove(&peer_id);
                    if let Some(worker) = workers.write().remove(&peer_id) {
                        // Also clean up asynchronously
                        tokio::spawn(async move {
                            let (tx, rx) = oneshot::channel();
                            let _ = worker.control_tx.send(WorkerCommand::Stop(tx));
                            let _ = tokio::time::timeout(Duration::from_secs(2), rx).await;
                            worker.handle.abort();
                        });
                    }
                }
                TransportCommand::Shutdown(ack) => {
                    info!(node_id, "Initiating global shutdown");

                    // Key point 1: Drain all workers immediately and release the lock to avoid contention
                    let worker_list: Vec<PeerWorker> = {
                        let mut current_workers = workers.write();
                        current_workers.drain().map(|(_, v)| v).collect()
                    };

                    let mut drain_futures = vec![];
                    let shutdown_timeout = config.worker_shutdown_timeout;
                    for worker in worker_list {
                        let (tx, rx) = oneshot::channel();
                        let _ = worker.control_tx.send(WorkerCommand::Stop(tx));

                        drain_futures.push(async move {
                            // Give each worker time to flush (configurable, default 1s, 50ms for tests)
                            if (tokio::time::timeout(shutdown_timeout, rx).await).is_err() {
                                debug!(peer_id = worker.peer_id, "Worker stop timeout, aborting");
                            }
                            // Key point 2: Explicitly abort the handle to ensure it terminates
                            worker.handle.abort();
                            let _ = worker.handle.await;
                        });
                    }

                    // Key point 3: Overall timeout to ensure the dispatcher can always exit
                    let _ = tokio::time::timeout(
                        Duration::from_secs(3),
                        futures::future::join_all(drain_futures),
                    )
                    .await;

                    let _ = ack.send(());
                    break;
                }
            }
        }
        info!(node_id, "Dispatcher loop exited");
    }

    /// Handle background reconnection attempt.
    ///
    /// For tc18 fix: proactively attempt reconnection even without new messages.
    #[allow(clippy::too_many_arguments)]
    async fn handle_background_reconnect(
        peer_id: NodeId,
        addr: SocketAddr,
        connection: &mut Option<TiedConnection>,
        pending_retry: &VecDeque<PendingMessage>,
        last_connection_failure: &mut Option<Instant>,
        config: &TransportConfig,
        metrics: &Arc<TransportMetrics>,
        semaphore: &Arc<Semaphore>,
    ) {
        debug!(peer_id, "Attempting background reconnection");
        metrics
            .background_reconnect_attempts
            .fetch_add(1, Ordering::Relaxed);

        *connection =
            Self::establish_connection_with_retry(peer_id, addr, config, metrics, semaphore).await;

        if connection.is_some() {
            info!(peer_id, "Background reconnection successful");
            *last_connection_failure = None;

            if !pending_retry.is_empty() {
                debug!(
                    peer_id,
                    pending_count = pending_retry.len(),
                    "Processing pending retry queue after reconnection"
                );
            }
        } else {
            debug!(peer_id, "Background reconnection failed, will retry");
            *last_connection_failure = Some(Instant::now());
        }
    }

    /// Process one message from the pending retry queue.
    #[allow(clippy::too_many_arguments)]
    async fn handle_retry_message(
        peer_id: NodeId,
        addr: SocketAddr,
        pending_retry: &mut VecDeque<PendingMessage>,
        last_activity: &mut Instant,
        connection: &mut Option<TiedConnection>,
        buffer: &mut BytesMut,
        last_connection_failure: &mut Option<Instant>,
        config: &TransportConfig,
        metrics: &Arc<TransportMetrics>,
        semaphore: &Arc<Semaphore>,
    ) {
        if let Some(msg) = pending_retry.pop_front() {
            metrics.pending_retries.fetch_sub(1, Ordering::Relaxed);
            *last_activity = Instant::now();
            Self::process_message(
                peer_id,
                addr,
                msg,
                connection,
                buffer,
                config,
                metrics,
                semaphore,
                MessagePriority::High, // retry messages treated as high priority
                last_connection_failure,
            )
            .await;
        }
    }

    /// Try to establish a connection if disconnected, queuing the message for retry on failure.
    ///
    /// Returns `true` if a connection is available (existing or newly established),
    /// `false` if connection failed and the message was queued/dropped.
    #[allow(clippy::too_many_arguments)]
    async fn ensure_connected_or_queue(
        peer_id: NodeId,
        addr: SocketAddr,
        msg: PendingMessage,
        connection: &mut Option<TiedConnection>,
        pending_retry: &mut VecDeque<PendingMessage>,
        last_connection_failure: &mut Option<Instant>,
        config: &TransportConfig,
        metrics: &Arc<TransportMetrics>,
        semaphore: &Arc<Semaphore>,
        label: &str,
    ) -> Option<PendingMessage> {
        if connection.is_some() {
            return Some(msg);
        }

        debug!(peer_id, "{} triggered connection attempt", label);
        *connection =
            Self::establish_connection_with_retry(peer_id, addr, config, metrics, semaphore).await;

        if connection.is_none() {
            *last_connection_failure = Some(Instant::now());
            if pending_retry.len() < config.max_pending_retries {
                pending_retry.push_back(msg);
                metrics.pending_retries.fetch_add(1, Ordering::Relaxed);
                debug!(
                    peer_id,
                    pending_count = pending_retry.len(),
                    "{} queued for retry",
                    label
                );
            } else {
                metrics
                    .messages_dropped_queue_full
                    .fetch_add(1, Ordering::Relaxed);
                metrics.messages_failed.fetch_add(1, Ordering::Relaxed);
                warn!(peer_id, "Pending retry queue full, {} dropped", label);
            }
            None
        } else {
            *last_connection_failure = None;
            Some(msg)
        }
    }

    /// Handle a normal-priority message: batch or send immediately depending on config.
    #[allow(clippy::too_many_arguments)]
    async fn handle_normal_message_send(
        peer_id: NodeId,
        addr: SocketAddr,
        msg: PendingMessage,
        batch_delay: Option<Duration>,
        batch_buffer: &mut Vec<PendingMessage>,
        batch_bytes: &mut usize,
        connection: &mut Option<TiedConnection>,
        buffer: &mut BytesMut,
        last_connection_failure: &mut Option<Instant>,
        config: &TransportConfig,
        metrics: &Arc<TransportMetrics>,
        semaphore: &Arc<Semaphore>,
    ) {
        if batch_delay.is_some() {
            // Batching enabled: collect messages
            *batch_bytes += msg.estimated_size;
            batch_buffer.push(msg);

            // If batch limit reached (message count or byte size), send immediately
            if batch_buffer.len() >= config.batch_max_messages
                || *batch_bytes >= config.batch_max_bytes
            {
                Self::process_batch(
                    peer_id,
                    addr,
                    batch_buffer,
                    batch_bytes,
                    connection,
                    buffer,
                    config,
                    metrics,
                    semaphore,
                    last_connection_failure,
                )
                .await;
            }
        } else {
            // Batching disabled: send immediately
            Self::process_message(
                peer_id,
                addr,
                msg,
                connection,
                buffer,
                config,
                metrics,
                semaphore,
                MessagePriority::Normal,
                last_connection_failure,
            )
            .await;
        }
    }

    /// Handle idle timeout: close the connection to free resources.
    async fn handle_idle_timeout(
        peer_id: NodeId,
        connection: &mut Option<TiedConnection>,
        metrics: &Arc<TransportMetrics>,
    ) {
        debug!(peer_id, "Connection idle timeout, closing");
        if let Some(mut conn) = connection.take() {
            let _ = conn.stream.shutdown().await;
            metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Handle graceful shutdown: flush batch buffer, retry queue, and remaining channel messages.
    #[allow(clippy::too_many_arguments)]
    async fn handle_graceful_shutdown(
        peer_id: NodeId,
        addr: SocketAddr,
        ack: oneshot::Sender<()>,
        high_priority_rx: &mut mpsc::Receiver<PendingMessage>,
        normal_priority_rx: &mut mpsc::Receiver<PendingMessage>,
        batch_buffer: &mut Vec<PendingMessage>,
        batch_bytes: &mut usize,
        pending_retry: &mut VecDeque<PendingMessage>,
        connection: &mut Option<TiedConnection>,
        buffer: &mut BytesMut,
        last_connection_failure: &mut Option<Instant>,
        config: &TransportConfig,
        metrics: &Arc<TransportMetrics>,
        semaphore: &Arc<Semaphore>,
    ) {
        debug!(peer_id, "Flushing remaining messages before stop");

        // First, send messages in the batch buffer
        if !batch_buffer.is_empty() {
            Self::process_batch(
                peer_id,
                addr,
                batch_buffer,
                batch_bytes,
                connection,
                buffer,
                config,
                metrics,
                semaphore,
                last_connection_failure,
            )
            .await;
        }

        // Process messages in the retry queue
        while let Some(msg) = pending_retry.pop_front() {
            metrics.pending_retries.fetch_sub(1, Ordering::Relaxed);
            Self::process_message(
                peer_id,
                addr,
                msg,
                connection,
                buffer,
                config,
                metrics,
                semaphore,
                MessagePriority::High,
                last_connection_failure,
            )
            .await;
        }

        // Flush all remaining messages (with timeout)
        let flush_timeout = tokio::time::sleep(Duration::from_secs(1));
        tokio::pin!(flush_timeout);

        loop {
            tokio::select! {
                Some(msg) = high_priority_rx.recv() => {
                    Self::process_message(
                        peer_id, addr, msg, connection, buffer,
                        config, metrics, semaphore, MessagePriority::High,
                        last_connection_failure,
                    ).await;
                }
                Some(msg) = normal_priority_rx.recv() => {
                    Self::process_message(
                        peer_id, addr, msg, connection, buffer,
                        config, metrics, semaphore, MessagePriority::Normal,
                        last_connection_failure,
                    ).await;
                }
                _ = &mut flush_timeout => {
                    warn!(peer_id, "Flush timeout, forcing stop");
                    break;
                }
                else => break,
            }
        }

        let _ = ack.send(());
    }

    /// Clean up connection and pending retry queue on worker exit.
    async fn cleanup_worker(
        peer_id: NodeId,
        connection: &mut Option<TiedConnection>,
        pending_retry: &VecDeque<PendingMessage>,
        metrics: &Arc<TransportMetrics>,
    ) {
        if let Some(mut conn) = connection.take() {
            let _ = conn.stream.shutdown().await;
            metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
        }

        let remaining = pending_retry.len();
        if remaining > 0 {
            metrics
                .pending_retries
                .fetch_sub(remaining, Ordering::Relaxed);
            metrics
                .messages_failed
                .fetch_add(remaining as u64, Ordering::Relaxed);
            debug!(
                peer_id,
                remaining, "Discarding pending retry messages on worker exit"
            );
        }

        debug!(peer_id, "Worker exited");
    }

    /// Create a sleep future that fires never (used as a disabled timer).
    fn never_fire_sleep() -> tokio::time::Sleep {
        tokio::time::sleep(Duration::from_secs(86400 * 365))
    }

    /// Per-peer worker: dual-queue priority scheduling + buffer reuse + idle timeout + batching + connection prewarming + message retry
    ///
    /// **Key improvements (for tc18_stale_leader_replacement):**
    /// 1. Added background reconnect mechanism: periodically attempts reconnection even without new messages
    /// 2. Force-marks reconnection needed after connection failure to avoid Pre-Vote deadloop
    /// 3. Ensures old connection is fully removed on failure so the next send triggers reconnection
    #[allow(clippy::too_many_arguments)]
    async fn peer_worker_loop(
        peer_id: NodeId,
        addr: SocketAddr,
        mut high_priority_rx: mpsc::Receiver<PendingMessage>,
        mut normal_priority_rx: mpsc::Receiver<PendingMessage>,
        mut control_rx: mpsc::UnboundedReceiver<WorkerCommand>,
        config: TransportConfig,
        metrics: Arc<TransportMetrics>,
        semaphore: Arc<Semaphore>,
        prewarm: bool,
    ) {
        debug!(peer_id, %addr, prewarm, "Peer worker started with enhanced reconnection");

        let mut connection: Option<TiedConnection> = None;
        let mut buffer = BytesMut::with_capacity(4096); // reused buffer
        let mut last_activity = Instant::now();
        let mut last_connection_failure: Option<Instant> = None;
        let mut pending_retry: VecDeque<PendingMessage> = VecDeque::new();

        // Connection prewarming
        if prewarm {
            debug!(peer_id, "Pre-warming connection");
            connection =
                Self::establish_connection_with_retry(peer_id, addr, &config, &metrics, &semaphore)
                    .await;
            if connection.is_some() {
                debug!(peer_id, "Connection pre-warmed successfully");
                last_connection_failure = None;
            } else {
                debug!(
                    peer_id,
                    "Connection pre-warm failed, will retry on first message"
                );
                last_connection_failure = Some(Instant::now());
            }
        }

        let mut batch_buffer: Vec<PendingMessage> = Vec::with_capacity(config.batch_max_messages);
        let mut batch_bytes: usize = 0;
        let batch_delay = config.batch_delay;

        loop {
            // Calculate timeout futures
            let idle_timeout_fut = match config.idle_timeout {
                Some(idle_timeout) => {
                    let elapsed = last_activity.elapsed();
                    if elapsed >= idle_timeout {
                        tokio::time::sleep(Duration::ZERO)
                    } else {
                        tokio::time::sleep(idle_timeout - elapsed)
                    }
                }
                None => Self::never_fire_sleep(),
            };

            let batch_timeout_fut = if !batch_buffer.is_empty() {
                batch_delay.map_or_else(|| tokio::time::sleep(Duration::ZERO), tokio::time::sleep)
            } else {
                Self::never_fire_sleep()
            };

            let retry_timeout_fut = if !pending_retry.is_empty() && connection.is_some() {
                tokio::time::sleep(Duration::from_millis(100))
            } else {
                Self::never_fire_sleep()
            };

            let background_reconnect_fut = if connection.is_none()
                && last_connection_failure
                    .is_some_and(|t| t.elapsed() < config.force_reconnect_window)
            {
                config
                    .background_reconnect_interval
                    .map_or_else(Self::never_fire_sleep, tokio::time::sleep)
            } else {
                Self::never_fire_sleep()
            };

            tokio::select! {
                biased; // use biased select to ensure priority ordering

                _ = background_reconnect_fut, if connection.is_none() && last_connection_failure.is_some() => {
                    Self::handle_background_reconnect(
                        peer_id, addr, &mut connection, &pending_retry,
                        &mut last_connection_failure, &config, &metrics, &semaphore,
                    ).await;
                }

                _ = retry_timeout_fut, if !pending_retry.is_empty() && connection.is_some() => {
                    Self::handle_retry_message(
                        peer_id, addr, &mut pending_retry, &mut last_activity,
                        &mut connection, &mut buffer, &mut last_connection_failure,
                        &config, &metrics, &semaphore,
                    ).await;
                }

                Some(msg) = high_priority_rx.recv() => {
                    last_activity = Instant::now();
                    let msg = Self::ensure_connected_or_queue(
                        peer_id, addr, msg, &mut connection, &mut pending_retry,
                        &mut last_connection_failure, &config, &metrics, &semaphore,
                        "High-priority message",
                    ).await;
                    if let Some(msg) = msg {
                        Self::process_message(
                            peer_id, addr, msg, &mut connection, &mut buffer,
                            &config, &metrics, &semaphore, MessagePriority::High,
                            &mut last_connection_failure,
                        ).await;
                    }
                }

                Some(msg) = normal_priority_rx.recv(), if high_priority_rx.is_empty() => {
                    last_activity = Instant::now();
                    let msg = Self::ensure_connected_or_queue(
                        peer_id, addr, msg, &mut connection, &mut pending_retry,
                        &mut last_connection_failure, &config, &metrics, &semaphore,
                        "Normal-priority message",
                    ).await;
                    if let Some(msg) = msg {
                        Self::handle_normal_message_send(
                            peer_id, addr, msg, batch_delay, &mut batch_buffer,
                            &mut batch_bytes, &mut connection, &mut buffer,
                            &mut last_connection_failure, &config, &metrics, &semaphore,
                        ).await;
                    }
                }

                _ = batch_timeout_fut, if !batch_buffer.is_empty() => {
                    Self::process_batch(
                        peer_id, addr, &mut batch_buffer, &mut batch_bytes,
                        &mut connection, &mut buffer, &config, &metrics, &semaphore,
                        &mut last_connection_failure,
                    ).await;
                }

                _ = idle_timeout_fut, if connection.is_some() => {
                    Self::handle_idle_timeout(peer_id, &mut connection, &metrics).await;
                }

                Some(WorkerCommand::Stop(ack)) = control_rx.recv() => {
                    Self::handle_graceful_shutdown(
                        peer_id, addr, ack, &mut high_priority_rx, &mut normal_priority_rx,
                        &mut batch_buffer, &mut batch_bytes, &mut pending_retry,
                        &mut connection, &mut buffer, &mut last_connection_failure,
                        &config, &metrics, &semaphore,
                    ).await;
                    break;
                }
            }
        }

        Self::cleanup_worker(peer_id, &mut connection, &pending_retry, &metrics).await;
    }

    /// Process messages in batch (zero-copy optimization)
    ///
    /// **Key improvement: added connection failure tracking and byte size limits**
    #[allow(clippy::too_many_arguments)]
    async fn process_batch(
        peer_id: NodeId,
        addr: SocketAddr,
        batch: &mut Vec<PendingMessage>,
        batch_bytes: &mut usize,
        connection: &mut Option<TiedConnection>,
        buffer: &mut BytesMut,
        config: &TransportConfig,
        metrics: &Arc<TransportMetrics>,
        semaphore: &Arc<Semaphore>,
        last_connection_failure: &mut Option<Instant>,
    ) {
        if batch.is_empty() {
            return;
        }

        trace!(
            peer_id,
            batch_size = batch.len(),
            batch_bytes = *batch_bytes,
            "Processing message batch"
        );

        // Ensure a connection exists (with retries)
        if connection.is_none() {
            *connection =
                Self::establish_connection_with_retry(peer_id, addr, config, metrics, semaphore)
                    .await;
            if connection.is_none() {
                *last_connection_failure = Some(Instant::now());
            } else {
                *last_connection_failure = None;
            }
        }

        if let Some(ref mut conn) = connection {
            buffer.clear();

            // Zero-copy: encode all messages directly into the same buffer
            for pending in batch.iter() {
                let send_start = Instant::now();
                match encode_message_into(&pending.msg, buffer) {
                    Ok(_) => {
                        metrics.record_send_latency(send_start.elapsed());
                    }
                    Err(e) => {
                        warn!(peer_id, error = %e, "Failed to encode message in batch");
                        metrics.messages_failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            // Send all data at once
            match tokio::time::timeout(config.write_timeout, conn.stream.write_all(buffer)).await {
                Ok(Ok(_)) => {
                    if let Err(e) = conn.stream.flush().await {
                        warn!(peer_id, error = %e, "Batch flush failed");
                        // **Critical: connection invalid, fully remove and mark failure**
                        if let Some(mut conn) = connection.take() {
                            let _ = conn.stream.shutdown().await;
                            metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                        }
                        *last_connection_failure = Some(Instant::now());
                        metrics
                            .messages_failed
                            .fetch_add(batch.len() as u64, Ordering::Relaxed);
                    } else {
                        metrics
                            .messages_sent
                            .fetch_add(batch.len() as u64, Ordering::Relaxed);
                        metrics
                            .normal_priority_sent
                            .fetch_add(batch.len() as u64, Ordering::Relaxed);
                        trace!(peer_id, count = batch.len(), "Batch sent successfully");
                        *last_connection_failure = None; // send succeeded, clear failure marker
                    }
                }
                Ok(Err(e)) => {
                    warn!(peer_id, error = %e, "Batch write failed");
                    // **Critical: connection invalid, fully remove and mark failure**
                    if let Some(mut conn) = connection.take() {
                        let _ = conn.stream.shutdown().await;
                        metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                    }
                    *last_connection_failure = Some(Instant::now());
                    metrics
                        .messages_failed
                        .fetch_add(batch.len() as u64, Ordering::Relaxed);
                }
                Err(_) => {
                    warn!(peer_id, "Batch write timeout");
                    // **Critical: connection invalid, fully remove and mark failure**
                    if let Some(mut conn) = connection.take() {
                        let _ = conn.stream.shutdown().await;
                        metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                    }
                    *last_connection_failure = Some(Instant::now());
                    metrics
                        .messages_failed
                        .fetch_add(batch.len() as u64, Ordering::Relaxed);
                }
            }
        } else {
            *last_connection_failure = Some(Instant::now());
            metrics
                .messages_failed
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
        }

        batch.clear();
        *batch_bytes = 0;
    }

    /// Process a single message (with exponential backoff + jitter)
    ///
    /// **Key improvement: added connection failure tracking parameter**
    #[allow(clippy::too_many_arguments)]
    async fn process_message(
        peer_id: NodeId,
        addr: SocketAddr,
        pending: PendingMessage,
        connection: &mut Option<TiedConnection>,
        buffer: &mut BytesMut,
        config: &TransportConfig,
        metrics: &Arc<TransportMetrics>,
        semaphore: &Arc<Semaphore>,
        priority: MessagePriority,
        last_connection_failure: &mut Option<Instant>,
    ) {
        let send_start = Instant::now();

        let mut retry_delay = config.initial_retry_delay;
        let mut attempts = 0;
        let mut success = false;

        while attempts < config.max_retries && !success {
            attempts += 1;

            // Ensure a connection exists (with retries)
            if connection.is_none() {
                *connection = Self::establish_connection_with_retry(
                    peer_id, addr, config, metrics, semaphore,
                )
                .await;
                if connection.is_none() {
                    *last_connection_failure = Some(Instant::now());
                } else {
                    *last_connection_failure = None;
                }
            }

            if let Some(ref mut conn) = connection {
                buffer.clear(); // reuse buffer

                match Self::send_message_to_stream(&mut conn.stream, &pending.msg, buffer, config)
                    .await
                {
                    Ok(_) => {
                        success = true;
                        metrics.messages_sent.fetch_add(1, Ordering::Relaxed);

                        match priority {
                            MessagePriority::High => {
                                metrics.high_priority_sent.fetch_add(1, Ordering::Relaxed);
                            }
                            MessagePriority::Normal => {
                                metrics.normal_priority_sent.fetch_add(1, Ordering::Relaxed);
                            }
                        }

                        metrics.record_send_latency(send_start.elapsed());
                        trace!(peer_id, priority = ?priority, "Message sent");
                        *last_connection_failure = None; // send succeeded, clear failure marker
                    }
                    Err(e) => {
                        warn!(peer_id, error = %e, attempt = attempts, "Send failed");

                        // **Key improvement: on connection failure, must fully remove old connection and mark failure**
                        if let Some(mut conn) = connection.take() {
                            let _ = conn.stream.shutdown().await;
                            metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                        }
                        *last_connection_failure = Some(Instant::now());

                        metrics.connections_failed.fetch_add(1, Ordering::Relaxed);

                        if attempts < config.max_retries {
                            // Exponential backoff + jitter
                            if config.enable_retry_jitter {
                                let jitter = Duration::from_millis(rand::random::<u64>() % 50);
                                retry_delay =
                                    (retry_delay * 2 + jitter).min(config.max_retry_delay);
                            } else {
                                retry_delay = (retry_delay * 2).min(config.max_retry_delay);
                            }

                            tokio::time::sleep(retry_delay).await;
                        }
                    }
                }
            }
        }

        if !success {
            error!(peer_id, attempts, "Message failed after all retries");
            metrics.messages_failed.fetch_add(1, Ordering::Relaxed);
            *last_connection_failure = Some(Instant::now());
        }
    }

    /// Establish a connection (returns TiedConnection with automatic permit management)
    #[allow(dead_code)]
    async fn establish_connection(
        peer_id: NodeId,
        addr: SocketAddr,
        config: &TransportConfig,
        metrics: &Arc<TransportMetrics>,
        semaphore: &Arc<Semaphore>,
    ) -> Option<TiedConnection> {
        // Acquire owned permit
        let permit = match semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                warn!(
                    peer_id,
                    "Connection limit reached, cannot acquire semaphore permit"
                );
                metrics.connections_failed.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };

        match tokio::time::timeout(config.connect_timeout, TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => {
                // TCP optimization
                if let Err(e) = Self::configure_tcp(&stream, config) {
                    warn!(peer_id, error = %e, "Failed to configure TCP");
                }

                metrics.connections_created.fetch_add(1, Ordering::Relaxed);
                metrics.active_connections.fetch_add(1, Ordering::Relaxed);
                debug!(peer_id, %addr, "Connection established");

                Some(TiedConnection::new(stream, permit))
            }
            Ok(Err(e)) => {
                warn!(peer_id, error = %e, "Connection failed");
                metrics.connections_failed.fetch_add(1, Ordering::Relaxed);
                None
            }
            Err(_) => {
                warn!(peer_id, "Connection timeout");
                metrics.connections_failed.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Establish a connection (with retry mechanism)
    ///
    /// Retries on transient network errors (e.g., ConnectionRefused)
    /// using exponential backoff + jitter strategy.
    async fn establish_connection_with_retry(
        peer_id: NodeId,
        addr: SocketAddr,
        config: &TransportConfig,
        metrics: &Arc<TransportMetrics>,
        semaphore: &Arc<Semaphore>,
    ) -> Option<TiedConnection> {
        let mut retry_count = 0;
        let mut retry_delay = config.initial_connect_retry_delay;

        while retry_count < config.max_connect_retries {
            retry_count += 1;

            // Acquire connection permit
            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    warn!(
                        peer_id,
                        "Connection limit reached, cannot acquire semaphore permit"
                    );
                    metrics.connections_failed.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            };

            // Attempt connection
            match tokio::time::timeout(config.connect_timeout, TcpStream::connect(addr)).await {
                Ok(Ok(stream)) => {
                    if let Err(e) = Self::configure_tcp(&stream, config) {
                        warn!(peer_id, error = %e, "Failed to configure TCP");
                    }
                    metrics.connections_created.fetch_add(1, Ordering::Relaxed);
                    metrics.active_connections.fetch_add(1, Ordering::Relaxed);
                    debug!(peer_id, %addr, retry_count, "Connection established");
                    return Some(TiedConnection::new(stream, permit));
                }
                Ok(Err(e)) => {
                    // Determine if the error is retryable
                    let should_retry = matches!(
                        e.kind(),
                        std::io::ErrorKind::ConnectionRefused
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::ConnectionAborted
                    );

                    if should_retry && retry_count < config.max_connect_retries {
                        debug!(
                            peer_id,
                            error = %e,
                            retry_count,
                            "Connection failed, will retry"
                        );
                        metrics.connection_retries.fetch_add(1, Ordering::Relaxed);

                        // Exponential backoff + jitter
                        if config.enable_retry_jitter {
                            let jitter = Duration::from_millis(rand::random::<u64>() % 20);
                            retry_delay =
                                (retry_delay * 2 + jitter).min(config.max_connect_retry_delay);
                        } else {
                            retry_delay = (retry_delay * 2).min(config.max_connect_retry_delay);
                        }

                        tokio::time::sleep(retry_delay).await;
                    } else {
                        warn!(peer_id, error = %e, retry_count, "Connection failed, giving up");
                        metrics.connections_failed.fetch_add(1, Ordering::Relaxed);
                        return None;
                    }
                }
                Err(_) => {
                    warn!(peer_id, retry_count, "Connection timeout");
                    metrics.connections_failed.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }
        }

        metrics.connections_failed.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Configure TCP parameters (enhanced Keep-Alive)
    fn configure_tcp(stream: &TcpStream, config: &TransportConfig) -> std::io::Result<()> {
        // Disable Nagle's algorithm
        if config.enable_tcp_nodelay {
            stream.set_nodelay(true)?;
        }

        // Set enhanced TCP Keep-Alive
        let socket_ref = SockRef::from(stream);
        let keepalive = TcpKeepalive::new()
            .with_time(config.tcp_keepalive_time)
            .with_interval(config.tcp_keepalive_interval);

        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let keepalive = keepalive.with_retries(config.tcp_keepalive_retries);

        socket_ref.set_tcp_keepalive(&keepalive)?;

        Ok(())
    }

    /// Send a message (zero-copy optimization)
    ///
    /// Uses encode_message_into to write directly into the reused BytesMut buffer,
    /// avoiding intermediate Vec<u8> allocation.
    async fn send_message_to_stream(
        stream: &mut TcpStream,
        msg: &Message,
        buffer: &mut BytesMut,
        config: &TransportConfig,
    ) -> Result<()> {
        // Zero-copy: encode directly into buffer, avoiding intermediate Vec allocation
        buffer.clear();
        encode_message_into(msg, buffer)?;

        tokio::time::timeout(config.write_timeout, stream.write_all(buffer))
            .await
            .map_err(|_| NetworkError::SendFailed("write timeout".to_string()))?
            .map_err(|e| NetworkError::SendFailed(format!("write failed: {}", e)))?;

        stream
            .flush()
            .await
            .map_err(|e| NetworkError::SendFailed(format!("flush failed: {}", e)))?;

        Ok(())
    }

    fn determine_priority(msg: &RaftMessage) -> MessagePriority {
        use raft::prelude::MessageType;

        match msg.get_msg_type() {
            MessageType::MsgHeartbeat
            | MessageType::MsgHeartbeatResponse
            | MessageType::MsgRequestVote
            | MessageType::MsgRequestVoteResponse => MessagePriority::High,
            _ => MessagePriority::Normal,
        }
    }
}

impl Drop for RaftTransport {
    fn drop(&mut self) {
        // Only attempt to send if command_tx is not yet closed; non-blocking
        let (tx, _) = oneshot::channel();
        let _ = self.command_tx.send(TransportCommand::Shutdown(tx));

        // Key point: do not await dispatcher_handle here.
        // Drop is synchronous, but the dispatcher is async.
        // Let the tokio runtime clean up the task when the handle is dropped.
    }
}

impl std::fmt::Debug for RaftTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftTransport")
            .field("node_id", &self.node_id)
            .field("peer_count", &self.peer_count())
            .field("metrics", &self.metrics())
            .finish()
    }
}

/// Implementation of RaftMessageSender for RaftTransport.
///
/// This allows RaftTransport to be used as the default message sender
/// for single-Raft setups while enabling injection of shard-aware
/// transport for Multi-Raft.
impl RaftMessageSender for RaftTransport {
    fn send_messages(&self, msgs: Vec<RaftMessage>) {
        RaftTransport::send_messages(self, msgs)
    }

    fn send_message(&self, to: NodeId, msg: Message) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { RaftTransport::send_message(self, to, msg).await })
    }

    fn add_peer(&self, id: NodeId, addr: SocketAddr) -> BoxFuture<'_, ()> {
        Box::pin(async move { RaftTransport::add_peer(self, id, addr).await })
    }

    fn remove_peer(&self, id: NodeId) {
        RaftTransport::remove_peer(self, id)
    }

    fn get_peer(&self, id: NodeId) -> Option<SocketAddr> {
        RaftTransport::get_peer(self, id)
    }

    fn peer_ids(&self) -> Vec<NodeId> {
        RaftTransport::peer_ids(self)
    }

    fn metrics(&self) -> TransportMetricsSnapshot {
        RaftTransport::metrics(self)
    }

    fn is_healthy(&self) -> bool {
        RaftTransport::is_healthy(self)
    }

    fn has_peer(&self, id: NodeId) -> bool {
        RaftTransport::has_peer(self, id)
    }

    fn shutdown(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move { RaftTransport::shutdown(self).await })
    }
}
