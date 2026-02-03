//! Node-scoped message router for Multi-Raft.
//!
//! This module provides a centralized router that owns all peer connections
//! for a node, enabling both main Raft and per-shard Raft to share the same
//! connection pool.
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────┐
//! │     NetworkServer      │   ← TCP listener (unchanged)
//! └───────────┬────────────┘
//!             │
//! ┌───────────▼────────────┐
//! │   NodeMessageRouter    │   ← Single, node-scoped
//! │  - node_id             │      Owns ALL peer connections
//! │  - connection pool     │      Unified backpressure
//! │  - backpressure        │      Centralized metrics
//! │  - metrics / tracing   │
//! └───────────┬────────────┘
//!             │
//! ┌───────────▼────────────┐
//! │  RaftShardMultiplexer  │   ← Pure routing logic (no I/O)
//! │  - shard_id → handler  │      Routes incoming messages
//! └───────────┬────────────┘
//!             │
//!     ┌───────┴───────┐
//!     │   Raft Core   │   ← Main Raft + Per-shard Raft
//!     └───────────────┘
//! ```

use crate::consensus::transport::{
    BackpressureCallback, BackpressureEvent, MessagePriority, RaftMessageSender, TransportConfig,
    TransportMetrics, TransportMetricsSnapshot,
};
use crate::error::{NetworkError, Result};
use crate::network::rpc::{encode_message_into, Message, RaftMessageWrapper, ShardRaftMessage};
use crate::types::NodeId;
use crate::Error;
use bytes::BytesMut;
use futures::future::BoxFuture;
use parking_lot::RwLock;
use raft::prelude::{Message as RaftMessage, MessageType};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, error, info, trace, warn};

use super::rpc::ShardId;

/// Pending message waiting to be sent.
#[derive(Debug)]
struct PendingMessage {
    #[allow(dead_code)]
    to: NodeId,
    msg: Message,
    #[allow(dead_code)]
    enqueued_at: Instant,
}

/// RAII wrapper: connection + permit, released on drop.
struct TiedConnection {
    stream: TcpStream,
    _permit: OwnedSemaphorePermit,
}

impl TiedConnection {
    fn new(stream: TcpStream, permit: OwnedSemaphorePermit) -> Self {
        Self {
            stream,
            _permit: permit,
        }
    }
}

/// Worker control command.
#[derive(Debug)]
enum WorkerCommand {
    Stop(oneshot::Sender<()>),
}

/// Transport control command.
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

/// Per-peer worker state.
struct PeerWorker {
    peer_id: NodeId,
    #[allow(dead_code)]
    addr: SocketAddr,
    high_priority_tx: mpsc::Sender<PendingMessage>,
    normal_priority_tx: mpsc::Sender<PendingMessage>,
    control_tx: mpsc::UnboundedSender<WorkerCommand>,
    handle: tokio::task::JoinHandle<()>,
}

/// Node-scoped message router that owns all peer connections.
///
/// This router provides a single connection pool that can be shared by both
/// the main Raft group and per-shard Raft groups in Multi-Raft mode.
///
/// # Benefits
///
/// - **Single connection pool**: One TCP connection per peer regardless of shard count
/// - **Unified backpressure**: Prevents memory issues from runaway shards
/// - **Centralized metrics**: One place for all transport stats
/// - **Clear separation**: I/O (router) vs routing logic (multiplexer)
pub struct NodeMessageRouter {
    node_id: NodeId,
    peers: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
    workers: Arc<RwLock<HashMap<NodeId, PeerWorker>>>,
    command_tx: mpsc::UnboundedSender<TransportCommand>,
    #[allow(dead_code)]
    dispatcher_handle: Arc<tokio::task::JoinHandle<()>>,
    config: TransportConfig,
    metrics: Arc<TransportMetrics>,
    connection_semaphore: Arc<Semaphore>,
    backpressure_callback: Option<BackpressureCallback>,
    pending_messages: Arc<RwLock<HashMap<NodeId, VecDeque<PendingMessage>>>>,
    shutdown: AtomicBool,
}

impl NodeMessageRouter {
    /// Create a new node message router.
    pub fn new(node_id: NodeId, config: TransportConfig) -> Self {
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

        info!(node_id, "NodeMessageRouter created");

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
            shutdown: AtomicBool::new(false),
        }
    }

    /// Create with default configuration.
    pub fn with_defaults(node_id: NodeId) -> Self {
        Self::new(node_id, TransportConfig::default())
    }

    /// Set the backpressure callback.
    pub fn set_backpressure_callback(&mut self, callback: BackpressureCallback) {
        self.backpressure_callback = Some(callback);
    }

    /// Get the node ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get transport metrics snapshot.
    pub fn metrics(&self) -> TransportMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Add a peer to the router.
    pub async fn add_peer(&self, id: NodeId, addr: SocketAddr) {
        self.peers.write().insert(id, addr);

        if self.config.enable_connection_prewarming {
            self.ensure_worker(id, addr).await;
        }

        debug!(node_id = self.node_id, peer_id = id, %addr, "Peer added to router");
    }

    /// Update a peer's address.
    pub fn update_peer(&self, id: NodeId, addr: SocketAddr) {
        let _ = self.command_tx.send(TransportCommand::UpdatePeer {
            peer_id: id,
            new_addr: addr,
        });
        debug!(node_id = self.node_id, peer_id = id, %addr, "Peer update queued");
    }

    /// Remove a peer from the router.
    pub fn remove_peer(&self, id: NodeId) {
        let _ = self
            .command_tx
            .send(TransportCommand::RemovePeer { peer_id: id });
        debug!(node_id = self.node_id, peer_id = id, "Peer removal queued");
    }

    /// Get a peer's address.
    pub fn get_peer(&self, id: NodeId) -> Option<SocketAddr> {
        self.peers.read().get(&id).copied()
    }

    /// Get all peer IDs.
    pub fn peer_ids(&self) -> Vec<NodeId> {
        self.peers.read().keys().copied().collect()
    }

    /// Get peer count.
    pub fn peer_count(&self) -> usize {
        self.peers.read().len()
    }

    /// Check if the router is healthy and ready to send messages.
    pub fn is_healthy(&self) -> bool {
        // Router is healthy if the command channel is not closed
        !self.command_tx.is_closed()
    }

    /// Check if a peer is registered in the router.
    pub fn has_peer(&self, id: NodeId) -> bool {
        self.peers.read().contains_key(&id)
    }

    /// Send a main Raft message (wrapped as Message::Raft).
    pub fn send_raft_message(&self, msg: RaftMessage) -> Result<()> {
        let to = msg.to;
        let msg_type = msg.msg_type;
        let priority = Self::determine_priority(&msg);

        let wrapper = RaftMessageWrapper::from_raft_message(&msg)
            .map_err(|e| NetworkError::Serialization(e.to_string()))?;

        let message = Message::Raft(wrapper);
        self.send_message_internal(to, message, priority, msg_type)
    }

    /// Send a shard Raft message (wrapped as Message::ShardRaft).
    pub fn send_shard_raft_message(&self, shard_id: ShardId, msg: RaftMessage) -> Result<()> {
        let to = msg.to;
        let msg_type = msg.msg_type;
        let priority = Self::determine_priority(&msg);

        let shard_msg = ShardRaftMessage::from_raft_message(shard_id, &msg)
            .map_err(|e| NetworkError::Serialization(e.to_string()))?;

        let message = Message::ShardRaft(shard_msg);
        self.send_message_internal(to, message, priority, msg_type)
    }

    /// Send an arbitrary message to a peer.
    pub async fn send_message(&self, to: NodeId, msg: Message) -> Result<()> {
        let pending = PendingMessage {
            to,
            msg,
            enqueued_at: Instant::now(),
        };

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

    /// Send multiple main Raft messages.
    pub fn send_raft_messages(&self, msgs: Vec<RaftMessage>) {
        for msg in msgs {
            if let Err(e) = self.send_raft_message(msg) {
                warn!(error = %e, "Failed to queue Raft message");
            }
        }
    }

    /// Send multiple shard Raft messages.
    pub fn send_shard_raft_messages(&self, shard_id: ShardId, msgs: Vec<RaftMessage>) {
        for msg in msgs {
            if let Err(e) = self.send_shard_raft_message(shard_id, msg) {
                warn!(shard_id, error = %e, "Failed to queue shard Raft message");
            }
        }
    }

    /// Shutdown the router.
    pub async fn shutdown(&self) {
        if self.shutdown.swap(true, Ordering::SeqCst) {
            return; // Already shutting down
        }

        info!(node_id = self.node_id, "Shutting down NodeMessageRouter");

        let (tx, rx) = oneshot::channel();
        let _ = self.command_tx.send(TransportCommand::Shutdown(tx));

        match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(_)) => info!(
                node_id = self.node_id,
                "NodeMessageRouter shutdown complete"
            ),
            Ok(Err(_)) => warn!(node_id = self.node_id, "Shutdown channel dropped"),
            Err(_) => warn!(node_id = self.node_id, "NodeMessageRouter shutdown timeout"),
        }
    }

    // Internal helper to send a message with priority
    fn send_message_internal(
        &self,
        to: NodeId,
        msg: Message,
        priority: MessagePriority,
        msg_type: MessageType,
    ) -> Result<()> {
        let pending = PendingMessage {
            to,
            msg,
            enqueued_at: Instant::now(),
        };

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

            // Check queue capacity for backpressure
            let capacity = tx.capacity();
            let max_capacity = tx.max_capacity();
            let current_size = max_capacity - capacity;
            let usage_percent = (current_size * 100) / max_capacity;

            if let Some(ref callback) = self.backpressure_callback {
                if usage_percent >= 80 {
                    callback(BackpressureEvent::QueueHighWatermark {
                        peer_id: to,
                        priority,
                        current_size,
                    });
                } else if usage_percent < 50 && current_size > 0 {
                    callback(BackpressureEvent::QueueNormal {
                        peer_id: to,
                        priority,
                    });
                }
            }

            match tx.try_send(pending) {
                Ok(_) => {}
                Err(_) => {
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
            // Worker doesn't exist, check if peer is known
            let peer_known = self.peers.read().contains_key(&to);
            if peer_known {
                let mut pending_map = self.pending_messages.write();
                let queue = pending_map.entry(to).or_default();

                if queue.len() < self.config.max_pending_retries {
                    queue.push_back(pending);
                    debug!(
                        node_id = self.node_id,
                        to,
                        queue_len = queue.len(),
                        "Message queued for pending peer"
                    );
                } else {
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

        // Forward pending messages to the new worker
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

    // The dispatcher and worker loops are largely copied from RaftTransport
    // to ensure consistent behavior.

    async fn dispatcher_loop(
        node_id: NodeId,
        workers: Arc<RwLock<HashMap<NodeId, PeerWorker>>>,
        peers: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
        mut command_rx: mpsc::UnboundedReceiver<TransportCommand>,
        config: TransportConfig,
        metrics: Arc<TransportMetrics>,
        semaphore: Arc<Semaphore>,
    ) {
        info!(node_id, "NodeMessageRouter dispatcher loop started");

        while let Some(cmd) = command_rx.recv().await {
            match cmd {
                TransportCommand::UpdatePeer { peer_id, new_addr } => {
                    if let Some(worker) = workers.write().remove(&peer_id) {
                        tokio::spawn(async move {
                            let (tx, rx) = oneshot::channel();
                            if worker.control_tx.send(WorkerCommand::Stop(tx)).is_ok()
                                && tokio::time::timeout(Duration::from_millis(200), rx)
                                    .await
                                    .is_err()
                            {
                                debug!(peer_id, "Worker graceful stop timeout, forcing abort");
                            }
                            worker.handle.abort();
                        });
                    }

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

                    info!(node_id, peer_id, %new_addr, "Peer worker replaced");
                }

                TransportCommand::RemovePeer { peer_id } => {
                    peers.write().remove(&peer_id);
                    if let Some(worker) = workers.write().remove(&peer_id) {
                        tokio::spawn(async move {
                            let (tx, rx) = oneshot::channel();
                            let _ = worker.control_tx.send(WorkerCommand::Stop(tx));
                            let _ = tokio::time::timeout(Duration::from_secs(2), rx).await;
                            worker.handle.abort();
                        });
                    }
                }

                TransportCommand::Shutdown(ack) => {
                    info!(node_id, "Initiating router shutdown");

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
                            if (tokio::time::timeout(shutdown_timeout, rx).await).is_err() {
                                debug!(peer_id = worker.peer_id, "Worker stop timeout, aborting");
                            }
                            worker.handle.abort();
                            let _ = worker.handle.await;
                        });
                    }

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
        info!(node_id, "NodeMessageRouter dispatcher loop exited");
    }

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
        debug!(peer_id, %addr, prewarm, "Peer worker started");

        let mut connection: Option<TiedConnection> = None;
        let mut buffer = BytesMut::with_capacity(4096);
        let mut last_activity = Instant::now();
        let mut last_connection_failure: Option<Instant> = None;
        let mut pending_retry: VecDeque<PendingMessage> = VecDeque::new();

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
        let batch_delay = config.batch_delay;

        loop {
            let idle_timeout_fut = if let Some(idle_timeout) = config.idle_timeout {
                let elapsed = last_activity.elapsed();
                if elapsed >= idle_timeout {
                    tokio::time::sleep(Duration::ZERO)
                } else {
                    tokio::time::sleep(idle_timeout - elapsed)
                }
            } else {
                tokio::time::sleep(Duration::from_secs(86400 * 365))
            };

            let batch_timeout_fut = if !batch_buffer.is_empty() {
                if let Some(delay) = batch_delay {
                    tokio::time::sleep(delay)
                } else {
                    tokio::time::sleep(Duration::ZERO)
                }
            } else {
                tokio::time::sleep(Duration::from_secs(86400 * 365))
            };

            let retry_timeout_fut = if !pending_retry.is_empty() && connection.is_some() {
                tokio::time::sleep(Duration::from_millis(100))
            } else {
                tokio::time::sleep(Duration::from_secs(86400 * 365))
            };

            let background_reconnect_fut = if connection.is_none()
                && last_connection_failure.is_some()
                && last_connection_failure.unwrap().elapsed() < config.force_reconnect_window
            {
                if let Some(interval) = config.background_reconnect_interval {
                    tokio::time::sleep(interval)
                } else {
                    tokio::time::sleep(Duration::from_secs(86400 * 365))
                }
            } else {
                tokio::time::sleep(Duration::from_secs(86400 * 365))
            };

            tokio::select! {
                biased;

                _ = background_reconnect_fut, if connection.is_none() && last_connection_failure.is_some() => {
                    debug!(peer_id, "Attempting background reconnection");
                    metrics.background_reconnect_attempts.fetch_add(1, Ordering::Relaxed);

                    connection = Self::establish_connection_with_retry(
                        peer_id, addr, &config, &metrics, &semaphore
                    ).await;

                    if connection.is_some() {
                        info!(peer_id, "Background reconnection successful");
                        last_connection_failure = None;
                    } else {
                        debug!(peer_id, "Background reconnection failed, will retry");
                        last_connection_failure = Some(Instant::now());
                    }
                }

                _ = retry_timeout_fut, if !pending_retry.is_empty() && connection.is_some() => {
                    if let Some(msg) = pending_retry.pop_front() {
                        metrics.pending_retries.fetch_sub(1, Ordering::Relaxed);
                        last_activity = Instant::now();
                        Self::process_message(
                            peer_id, addr, msg, &mut connection, &mut buffer,
                            &config, &metrics, &semaphore, MessagePriority::High,
                            &mut last_connection_failure,
                        ).await;
                    }
                }

                Some(msg) = high_priority_rx.recv() => {
                    last_activity = Instant::now();

                    if connection.is_none() {
                        debug!(peer_id, "High-priority message triggered connection attempt");
                        connection = Self::establish_connection_with_retry(
                            peer_id, addr, &config, &metrics, &semaphore
                        ).await;

                        if connection.is_none() {
                            last_connection_failure = Some(Instant::now());
                            if pending_retry.len() < config.max_pending_retries {
                                pending_retry.push_back(msg);
                                metrics.pending_retries.fetch_add(1, Ordering::Relaxed);
                                debug!(peer_id, pending_count = pending_retry.len(),
                                    "High-priority message queued for retry");
                            } else {
                                metrics.messages_dropped_queue_full.fetch_add(1, Ordering::Relaxed);
                                metrics.messages_failed.fetch_add(1, Ordering::Relaxed);
                                warn!(peer_id, "Pending retry queue full, high-priority message dropped");
                            }
                            continue;
                        } else {
                            last_connection_failure = None;
                        }
                    }

                    Self::process_message(
                        peer_id, addr, msg, &mut connection, &mut buffer,
                        &config, &metrics, &semaphore, MessagePriority::High,
                        &mut last_connection_failure,
                    ).await;
                }

                Some(msg) = normal_priority_rx.recv(), if high_priority_rx.is_empty() => {
                    last_activity = Instant::now();

                    if connection.is_none() {
                        connection = Self::establish_connection_with_retry(
                            peer_id, addr, &config, &metrics, &semaphore
                        ).await;

                        if connection.is_none() {
                            last_connection_failure = Some(Instant::now());
                            if pending_retry.len() < config.max_pending_retries {
                                pending_retry.push_back(msg);
                                metrics.pending_retries.fetch_add(1, Ordering::Relaxed);
                                debug!(peer_id, pending_count = pending_retry.len(),
                                    "Message queued for retry");
                            } else {
                                metrics.messages_dropped_queue_full.fetch_add(1, Ordering::Relaxed);
                                metrics.messages_failed.fetch_add(1, Ordering::Relaxed);
                                warn!(peer_id, "Pending retry queue full, message dropped");
                            }
                            continue;
                        } else {
                            last_connection_failure = None;
                        }
                    }

                    if batch_delay.is_some() {
                        batch_buffer.push(msg);
                        if batch_buffer.len() >= config.batch_max_messages {
                            Self::process_batch(
                                peer_id, addr, &mut batch_buffer, &mut connection, &mut buffer,
                                &config, &metrics, &semaphore, &mut last_connection_failure,
                            ).await;
                        }
                    } else {
                        Self::process_message(
                            peer_id, addr, msg, &mut connection, &mut buffer,
                            &config, &metrics, &semaphore, MessagePriority::Normal,
                            &mut last_connection_failure,
                        ).await;
                    }
                }

                _ = batch_timeout_fut, if !batch_buffer.is_empty() => {
                    Self::process_batch(
                        peer_id, addr, &mut batch_buffer, &mut connection, &mut buffer,
                        &config, &metrics, &semaphore, &mut last_connection_failure,
                    ).await;
                }

                _ = idle_timeout_fut, if connection.is_some() => {
                    debug!(peer_id, "Connection idle timeout, closing");
                    if let Some(mut conn) = connection.take() {
                        let _ = conn.stream.shutdown().await;
                        metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                    }
                }

                Some(WorkerCommand::Stop(ack)) = control_rx.recv() => {
                    debug!(peer_id, "Flushing remaining messages before stop");

                    if !batch_buffer.is_empty() {
                        Self::process_batch(
                            peer_id, addr, &mut batch_buffer, &mut connection, &mut buffer,
                            &config, &metrics, &semaphore, &mut last_connection_failure,
                        ).await;
                    }

                    while let Some(msg) = pending_retry.pop_front() {
                        metrics.pending_retries.fetch_sub(1, Ordering::Relaxed);
                        Self::process_message(
                            peer_id, addr, msg, &mut connection, &mut buffer,
                            &config, &metrics, &semaphore, MessagePriority::High,
                            &mut last_connection_failure,
                        ).await;
                    }

                    let flush_timeout = tokio::time::sleep(Duration::from_secs(1));
                    tokio::pin!(flush_timeout);

                    loop {
                        tokio::select! {
                            Some(msg) = high_priority_rx.recv() => {
                                Self::process_message(
                                    peer_id, addr, msg, &mut connection, &mut buffer,
                                    &config, &metrics, &semaphore, MessagePriority::High,
                                    &mut last_connection_failure,
                                ).await;
                            }
                            Some(msg) = normal_priority_rx.recv() => {
                                Self::process_message(
                                    peer_id, addr, msg, &mut connection, &mut buffer,
                                    &config, &metrics, &semaphore, MessagePriority::Normal,
                                    &mut last_connection_failure,
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
                    break;
                }
            }
        }

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

    #[allow(clippy::too_many_arguments)]
    async fn process_batch(
        peer_id: NodeId,
        addr: SocketAddr,
        batch: &mut Vec<PendingMessage>,
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
            "Processing message batch"
        );

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

            match tokio::time::timeout(config.write_timeout, conn.stream.write_all(buffer)).await {
                Ok(Ok(_)) => {
                    if let Err(e) = conn.stream.flush().await {
                        warn!(peer_id, error = %e, "Batch flush failed");
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
                        *last_connection_failure = None;
                    }
                }
                Ok(Err(e)) => {
                    warn!(peer_id, error = %e, "Batch write failed");
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
    }

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
                buffer.clear();

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
                        *last_connection_failure = None;
                    }
                    Err(e) => {
                        warn!(peer_id, error = %e, attempt = attempts, "Send failed");

                        if let Some(mut conn) = connection.take() {
                            let _ = conn.stream.shutdown().await;
                            metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                        }
                        *last_connection_failure = Some(Instant::now());

                        metrics.connections_failed.fetch_add(1, Ordering::Relaxed);

                        if attempts < config.max_retries {
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

    async fn establish_connection_with_retry(
        peer_id: NodeId,
        addr: SocketAddr,
        config: &TransportConfig,
        metrics: &Arc<TransportMetrics>,
        semaphore: &Arc<Semaphore>,
    ) -> Option<TiedConnection> {
        use socket2::{SockRef, TcpKeepalive};

        let mut retry_count = 0;
        let mut retry_delay = config.initial_connect_retry_delay;

        while retry_count < config.max_connect_retries {
            retry_count += 1;

            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    warn!(peer_id, "Connection limit reached");
                    metrics.connections_failed.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            };

            match tokio::time::timeout(config.connect_timeout, TcpStream::connect(addr)).await {
                Ok(Ok(stream)) => {
                    // Configure TCP
                    if config.enable_tcp_nodelay {
                        let _ = stream.set_nodelay(true);
                    }

                    let socket_ref = SockRef::from(&stream);
                    let keepalive = TcpKeepalive::new()
                        .with_time(config.tcp_keepalive_time)
                        .with_interval(config.tcp_keepalive_interval);

                    #[cfg(any(target_os = "linux", target_os = "macos"))]
                    let keepalive = keepalive.with_retries(config.tcp_keepalive_retries);

                    let _ = socket_ref.set_tcp_keepalive(&keepalive);

                    metrics.connections_created.fetch_add(1, Ordering::Relaxed);
                    metrics.active_connections.fetch_add(1, Ordering::Relaxed);
                    debug!(peer_id, %addr, retry_count, "Connection established");
                    return Some(TiedConnection::new(stream, permit));
                }
                Ok(Err(e)) => {
                    let should_retry = matches!(
                        e.kind(),
                        std::io::ErrorKind::ConnectionRefused
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::ConnectionAborted
                    );

                    if should_retry && retry_count < config.max_connect_retries {
                        debug!(peer_id, error = %e, retry_count, "Connection failed, will retry");
                        metrics.connection_retries.fetch_add(1, Ordering::Relaxed);

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

    async fn send_message_to_stream(
        stream: &mut TcpStream,
        msg: &Message,
        buffer: &mut BytesMut,
        config: &TransportConfig,
    ) -> Result<()> {
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
}

impl std::fmt::Debug for NodeMessageRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeMessageRouter")
            .field("node_id", &self.node_id)
            .field("peer_count", &self.peer_count())
            .field("metrics", &self.metrics())
            .finish()
    }
}

// ============================================================================
// Transport Adapters
// ============================================================================

/// Adapter for main Raft group that wraps messages as Message::Raft.
///
/// This implements `RaftMessageSender` to allow the main RaftNode to use
/// the shared NodeMessageRouter.
pub struct MainRaftAdapter {
    router: Arc<NodeMessageRouter>,
}

impl MainRaftAdapter {
    /// Create a new adapter for the main Raft group.
    pub fn new(router: Arc<NodeMessageRouter>) -> Self {
        Self { router }
    }

    /// Get the underlying router.
    pub fn router(&self) -> &Arc<NodeMessageRouter> {
        &self.router
    }
}

impl RaftMessageSender for MainRaftAdapter {
    fn send_messages(&self, msgs: Vec<RaftMessage>) {
        self.router.send_raft_messages(msgs);
    }

    fn send_message(&self, to: NodeId, msg: Message) -> BoxFuture<'_, Result<()>> {
        let router = self.router.clone();
        Box::pin(async move { router.send_message(to, msg).await })
    }

    fn add_peer(&self, id: NodeId, addr: SocketAddr) -> BoxFuture<'_, ()> {
        let router = self.router.clone();
        Box::pin(async move {
            router.add_peer(id, addr).await;
        })
    }

    fn remove_peer(&self, id: NodeId) {
        self.router.remove_peer(id);
    }

    fn get_peer(&self, id: NodeId) -> Option<SocketAddr> {
        self.router.get_peer(id)
    }

    fn peer_ids(&self) -> Vec<NodeId> {
        self.router.peer_ids()
    }

    fn metrics(&self) -> TransportMetricsSnapshot {
        self.router.metrics()
    }

    fn is_healthy(&self) -> bool {
        self.router.is_healthy()
    }

    fn has_peer(&self, id: NodeId) -> bool {
        self.router.has_peer(id)
    }

    fn shutdown(&self) -> BoxFuture<'_, ()> {
        // MainRaftAdapter doesn't own the router, so shutdown is a no-op.
        // The router is shut down by its owner (usually DistributedCache).
        Box::pin(async {})
    }
}

impl std::fmt::Debug for MainRaftAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MainRaftAdapter")
            .field("node_id", &self.router.node_id())
            .finish()
    }
}

/// Adapter for per-shard Raft groups that wraps messages as Message::ShardRaft.
///
/// This implements `RaftMessageSender` to allow ShardRaftNodes to use
/// the shared NodeMessageRouter with shard-aware message routing.
pub struct ShardRaftAdapter {
    shard_id: ShardId,
    router: Arc<NodeMessageRouter>,
}

impl ShardRaftAdapter {
    /// Create a new adapter for a specific shard.
    pub fn new(shard_id: ShardId, router: Arc<NodeMessageRouter>) -> Self {
        Self { shard_id, router }
    }

    /// Get the shard ID.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Get the underlying router.
    pub fn router(&self) -> &Arc<NodeMessageRouter> {
        &self.router
    }
}

impl RaftMessageSender for ShardRaftAdapter {
    fn send_messages(&self, msgs: Vec<RaftMessage>) {
        self.router.send_shard_raft_messages(self.shard_id, msgs);
    }

    fn send_message(&self, to: NodeId, msg: Message) -> BoxFuture<'_, Result<()>> {
        let router = self.router.clone();
        Box::pin(async move { router.send_message(to, msg).await })
    }

    fn add_peer(&self, id: NodeId, addr: SocketAddr) -> BoxFuture<'_, ()> {
        let router = self.router.clone();
        Box::pin(async move {
            router.add_peer(id, addr).await;
        })
    }

    fn remove_peer(&self, id: NodeId) {
        self.router.remove_peer(id);
    }

    fn get_peer(&self, id: NodeId) -> Option<SocketAddr> {
        self.router.get_peer(id)
    }

    fn peer_ids(&self) -> Vec<NodeId> {
        self.router.peer_ids()
    }

    fn metrics(&self) -> TransportMetricsSnapshot {
        self.router.metrics()
    }

    fn is_healthy(&self) -> bool {
        self.router.is_healthy()
    }

    fn has_peer(&self, id: NodeId) -> bool {
        self.router.has_peer(id)
    }

    fn shutdown(&self) -> BoxFuture<'_, ()> {
        // ShardRaftAdapter doesn't own the router, so shutdown is a no-op.
        Box::pin(async {})
    }
}

impl std::fmt::Debug for ShardRaftAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardRaftAdapter")
            .field("shard_id", &self.shard_id)
            .field("node_id", &self.router.node_id())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_router_creation() {
        let router = NodeMessageRouter::with_defaults(1);
        assert_eq!(router.node_id(), 1);
        assert_eq!(router.peer_count(), 0);
        router.shutdown().await;
    }

    #[tokio::test]
    async fn test_peer_management() {
        let router = NodeMessageRouter::with_defaults(1);

        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        router.add_peer(2, addr).await;

        assert_eq!(router.peer_count(), 1);
        assert_eq!(router.get_peer(2), Some(addr));
        assert!(router.peer_ids().contains(&2));

        router.remove_peer(2);
        // Note: removal is async, so peer might still be visible briefly
        router.shutdown().await;
    }

    #[tokio::test]
    async fn test_main_raft_adapter() {
        let router = Arc::new(NodeMessageRouter::with_defaults(1));
        let adapter = MainRaftAdapter::new(router.clone());

        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        adapter.add_peer(2, addr).await;

        assert_eq!(adapter.get_peer(2), Some(addr));
        assert!(adapter.peer_ids().contains(&2));

        router.shutdown().await;
    }

    #[tokio::test]
    async fn test_shard_raft_adapter() {
        let router = Arc::new(NodeMessageRouter::with_defaults(1));
        let adapter = ShardRaftAdapter::new(5, router.clone());

        assert_eq!(adapter.shard_id(), 5);

        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        adapter.add_peer(2, addr).await;

        assert_eq!(adapter.get_peer(2), Some(addr));

        router.shutdown().await;
    }
}
