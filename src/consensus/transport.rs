use crate::error::{NetworkError, Result};
use crate::network::rpc::{encode_message_into, Message, RaftMessageWrapper};
use crate::types::NodeId;
use crate::Error;
use bytes::BytesMut;
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

/// 消息优先级
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessagePriority {
    High,   // 心跳、投票
    Normal, // 日志追加、快照
}

/// 背压事件类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureEvent {
    /// 队列已满，消息被丢弃
    QueueFull { peer_id: NodeId, priority: MessagePriority },
    /// 队列接近满（达到 80% 容量）
    QueueHighWatermark { peer_id: NodeId, priority: MessagePriority, current_size: usize },
    /// 队列恢复正常（低于 50% 容量）
    QueueNormal { peer_id: NodeId, priority: MessagePriority },
}

/// 背压回调函数类型
pub type BackpressureCallback = Arc<dyn Fn(BackpressureEvent) + Send + Sync>;

/// 待发送的消息
#[derive(Debug)]
struct PendingMessage {
    to: NodeId,
    msg: Message,
    enqueued_at: Instant,
}

/// RAII 封装：连接 + Permit 绑定，防止泄露
struct TiedConnection {
    stream: TcpStream,
    _permit: OwnedSemaphorePermit, // Drop 时自动释放
}

impl TiedConnection {
    fn new(stream: TcpStream, permit: OwnedSemaphorePermit) -> Self {
        Self {
            stream,
            _permit: permit,
        }
    }
}

/// 传输配置
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
    /// 空闲连接超时时间。如果连接在此时间内没有消息往来，将自动断开以释放资源。
    /// 设置为 None 禁用空闲超时。
    pub idle_timeout: Option<Duration>,
    /// 消息批处理延迟。启用后，会等待此时间收集多条消息后一起发送。
    /// 设置为 None 禁用批处理。
    pub batch_delay: Option<Duration>,
    /// 批处理最大消息数量
    pub batch_max_messages: usize,
    /// 连接建立阶段的最大重试次数
    pub max_connect_retries: usize,
    /// 连接重试的初始延迟
    pub initial_connect_retry_delay: Duration,
    /// 连接重试的最大延迟
    pub max_connect_retry_delay: Duration,
    /// 每个 peer 的待重试消息队列大小
    pub max_pending_retries: usize,
    /// 新增：连接失败后的持续重连间隔（针对 tc18 问题）
    /// 即使没有新消息，worker 也会定期尝试重新建立连接
    pub background_reconnect_interval: Option<Duration>,
    /// 新增：连接失败标记持续时间，在此期间强制尝试重连
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
            max_connect_retries: 3,
            initial_connect_retry_delay: Duration::from_millis(50),
            max_connect_retry_delay: Duration::from_millis(500),
            max_pending_retries: 10,
            // 新增：每500ms尝试重连，直到成功
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

/// Worker 控制命令
#[derive(Debug)]
enum WorkerCommand {
    /// 优雅停止（刷完队列）
    Stop(oneshot::Sender<()>),
}

/// 传输层控制命令
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

/// 详细的传输指标
#[derive(Debug, Default)]
pub struct TransportMetrics {
    pub messages_sent: AtomicU64,
    pub messages_failed: AtomicU64,
    pub high_priority_sent: AtomicU64,
    pub normal_priority_sent: AtomicU64,
    pub connections_created: AtomicU64,
    pub connections_failed: AtomicU64,
    pub active_connections: AtomicUsize,
    /// 总发送延迟（微秒），用于计算平均延迟
    pub total_send_latency_us: AtomicU64,
    pub send_count_for_latency: AtomicU64,
    /// 连接建立阶段的重试次数
    pub connection_retries: AtomicU64,
    /// 因队列满而丢弃的消息数
    pub messages_dropped_queue_full: AtomicU64,
    /// 当前待重试的消息总数
    pub pending_retries: AtomicUsize,
    /// 新增：后台重连尝试次数
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
            background_reconnect_attempts: self.background_reconnect_attempts.load(Ordering::Relaxed),
        }
    }

    fn record_send_latency(&self, latency: Duration) {
        self.total_send_latency_us
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
        self.send_count_for_latency.fetch_add(1, Ordering::Relaxed);
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
    /// 平均发送延迟（微秒）
    pub average_send_latency_us: u64,
    /// 连接建立阶段的重试次数
    pub connection_retries: u64,
    /// 因队列满而丢弃的消息数
    pub messages_dropped_queue_full: u64,
    /// 当前待重试的消息总数
    pub pending_retries: usize,
    /// 新增：后台重连尝试次数
    pub background_reconnect_attempts: u64,
}

/// Per-Peer Worker 状态
struct PeerWorker {
    peer_id: NodeId,
    addr: SocketAddr,
    high_priority_tx: mpsc::Sender<PendingMessage>,
    normal_priority_tx: mpsc::Sender<PendingMessage>,
    control_tx: mpsc::UnboundedSender<WorkerCommand>,
    handle: tokio::task::JoinHandle<()>,
}

/// Raft 消息传输层
pub struct RaftTransport {
    node_id: NodeId,
    peers: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
    workers: Arc<RwLock<HashMap<NodeId, PeerWorker>>>,
    command_tx: mpsc::UnboundedSender<TransportCommand>,
    dispatcher_handle: Arc<tokio::task::JoinHandle<()>>,
    config: TransportConfig,
    metrics: Arc<TransportMetrics>,
    connection_semaphore: Arc<Semaphore>,
    /// 背压回调，用于通知 Raft 状态机减缓发送速度
    backpressure_callback: Option<BackpressureCallback>,
    /// 待发送消息队列：用于 peer 在 peers 中但 worker 尚未就绪时缓存消息
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

    /// 设置背压回调函数
    /// 当队列满或达到高水位时会调用此回调，用于通知上层减缓发送速度
    pub fn set_backpressure_callback(&mut self, callback: BackpressureCallback) {
        self.backpressure_callback = Some(callback);
    }

    /// 创建带背压回调的 Transport
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

    pub fn metrics(&self) -> TransportMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// 发送 Raft 消息（带优先级判断和背压反馈）
    pub fn send(&self, msg: RaftMessage) -> Result<()> {
        let to = msg.to;
        let msg_type = msg.msg_type;
        let priority = Self::determine_priority(&msg);

        let wrapper = RaftMessageWrapper::from_raft_message(&msg)
            .map_err(|e| NetworkError::Serialization(e.to_string()))?;

        let message = Message::Raft(wrapper);
        let pending = PendingMessage {
            to,
            msg: message,
            enqueued_at: Instant::now(),
        };

        // 根据优先级选择队列
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

            // 计算队列使用率并触发背压回调
            let capacity = tx.capacity();
            let max_capacity = tx.max_capacity();
            let current_size = max_capacity - capacity;
            let usage_percent = (current_size * 100) / max_capacity;

            // 检查背压状态
            if let Some(ref callback) = self.backpressure_callback {
                if usage_percent >= 80 {
                    callback(BackpressureEvent::QueueHighWatermark {
                        peer_id: to,
                        priority,
                        current_size,
                    });
                } else if usage_percent < 50 && current_size > 0 {
                    // 只在队列有数据但低于 50% 时通知恢复
                    callback(BackpressureEvent::QueueNormal {
                        peer_id: to,
                        priority,
                    });
                }
            }

            match tx.try_send(pending) {
                Ok(_) => {}
                Err(_) => {
                    // 队列满，触发背压回调
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
            // Worker 不存在，检查 peer 是否已知
            let peer_known = self.peers.read().contains_key(&to);
            if peer_known {
                // Peer 已知但 worker 尚未就绪，将消息放入待发送队列
                let mut pending_map = self.pending_messages.write();
                let queue = pending_map.entry(to).or_insert_with(VecDeque::new);

                // 限制待发送队列大小，防止内存膨胀
                if queue.len() < self.config.max_pending_retries {
                    queue.push_back(pending);
                    debug!(
                        node_id = self.node_id,
                        to,
                        queue_len = queue.len(),
                        "Message queued for pending peer"
                    );
                } else {
                    // 队列满，丢弃消息
                    self.metrics.messages_dropped_queue_full.fetch_add(1, Ordering::Relaxed);
                    debug!(node_id = self.node_id, to, "Pending queue full, message dropped");
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
        let pending = PendingMessage {
            to,
            msg,
            enqueued_at: Instant::now(),
        };

        // Try to send via worker (use high priority for forwarded commands)
        let worker = {
            let workers = self.workers.read();
            workers
                .get(&to)
                .map(|w| w.high_priority_tx.clone())
        };

        if let Some(tx) = worker {
            match tx.try_send(pending) {
                Ok(_) => {
                    trace!(from = self.node_id, to, "Custom message queued");
                    Ok(())
                }
                Err(_) => {
                    Err(Error::from(NetworkError::SendFailed(
                        "peer queue full".to_string(),
                    )))
                }
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
                        peer_id, addr, hp_rx, np_rx, control_rx, config, metrics, semaphore, prewarm,
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

        // Worker 创建后，立即将待发送队列中的消息转发给 worker
        let pending_msgs: Vec<PendingMessage> = {
            let mut pending_map = self.pending_messages.write();
            pending_map.remove(&peer_id).map(|q| q.into_iter().collect()).unwrap_or_default()
        };

        if !pending_msgs.is_empty() {
            let count = pending_msgs.len();
            for msg in pending_msgs {
                // 将待发送消息发送到高优先级队列（因为这些是早期关键消息）
                if hp_tx_clone.try_send(msg).is_err() {
                    debug!(node_id = self.node_id, peer_id, "Failed to forward pending message to worker");
                }
            }
            debug!(node_id = self.node_id, peer_id, count, "Forwarded pending messages to worker");
        }
    }

    /// Dispatcher：处理控制命令，支持优雅停机
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
                    // 1. 异步清理旧 Worker，不阻塞当前 Dispatcher 循环
                    if let Some(worker) = workers.write().remove(&peer_id) {
                        tokio::spawn(async move {
                            let (tx, rx) = oneshot::channel();
                            // 1. 发送停止信号
                            if worker.control_tx.send(WorkerCommand::Stop(tx)).is_ok() {
                                // 2. 给一段较短的优雅时间（例如 200ms，测试环境下可以更短）
                                if let Err(_) =
                                    tokio::time::timeout(Duration::from_millis(200), rx).await
                                {
                                    debug!(peer_id, "Worker graceful stop timeout, forcing abort");
                                }
                            }
                            // 3. 无论如何，最后确保 handle 被 abort 以释放所有资源（包括 Permit）
                            worker.handle.abort();
                        });
                    }

                    // 2. 立即更新地址并创建新 Worker
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
                        // 同样异步清理
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

                    // 关键点 1：立即取出所有 Worker 并释放锁，防止锁竞争
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
                            if let Err(_) = tokio::time::timeout(shutdown_timeout, rx).await {
                                debug!(peer_id = worker.peer_id, "Worker stop timeout, aborting");
                            }
                            // 关键点 2：显式调用 abort 确保 handle 结束
                            worker.handle.abort();
                            let _ = worker.handle.await;
                        });
                    }

                    // 关键点 3：总控超时，确保 Dispatcher 一定能退出
                    let _ = tokio::time::timeout(
                        Duration::from_secs(3),
                        futures::future::join_all(drain_futures)
                    ).await;

                    let _ = ack.send(());
                    break;
                }
            }
        }
        info!(node_id, "Dispatcher loop exited");
    }

    /// Per-Peer Worker：双队列优先级调度 + 缓冲区复用 + 空闲超时 + 批处理 + 连接预热 + 消息重试
    ///
    /// **关键改进（针对 tc18_stale_leader_replacement）：**
    /// 1. 增加后台重连机制：即使没有新消息，也会定期尝试重新建立连接
    /// 2. 连接失败后强制标记需要重连，避免 Pre-Vote 死循环
    /// 3. 确保连接失败时完全清除旧连接，下次发送时触发重连
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
        let mut buffer = BytesMut::with_capacity(4096); // 复用缓冲区
        let mut last_activity = Instant::now();

        // 新增：追踪最后一次连接失败的时间
        let mut last_connection_failure: Option<Instant> = None;

        // 新增：待重试消息队列
        let mut pending_retry: VecDeque<PendingMessage> = VecDeque::new();

        // 连接预热：在 worker 启动时立即尝试建立连接（使用带重试的版本）
        if prewarm {
            debug!(peer_id, "Pre-warming connection");
            connection = Self::establish_connection_with_retry(peer_id, addr, &config, &metrics, &semaphore).await;
            if connection.is_some() {
                debug!(peer_id, "Connection pre-warmed successfully");
                last_connection_failure = None; // 连接成功，清除失败标记
            } else {
                debug!(peer_id, "Connection pre-warm failed, will retry on first message");
                last_connection_failure = Some(Instant::now()); // 标记连接失败
            }
        }

        // 批处理缓冲区
        let mut batch_buffer: Vec<PendingMessage> = Vec::with_capacity(config.batch_max_messages);
        let batch_delay = config.batch_delay;

        loop {
            // 计算空闲超时
            let idle_timeout_fut = if let Some(idle_timeout) = config.idle_timeout {
                let elapsed = last_activity.elapsed();
                if elapsed >= idle_timeout {
                    // 已经超时，立即触发
                    tokio::time::sleep(Duration::ZERO)
                } else {
                    tokio::time::sleep(idle_timeout - elapsed)
                }
            } else {
                // 禁用空闲超时，使用一个永远不会触发的 future
                tokio::time::sleep(Duration::from_secs(86400 * 365)) // 1 year
            };

            // 计算批处理超时（如果有待处理的批次）
            let batch_timeout_fut = if !batch_buffer.is_empty() {
                if let Some(delay) = batch_delay {
                    tokio::time::sleep(delay)
                } else {
                    tokio::time::sleep(Duration::ZERO) // 立即发送
                }
            } else {
                tokio::time::sleep(Duration::from_secs(86400 * 365)) // 永不触发
            };

            // 计算重试队列处理超时（有连接且有待重试消息时触发）
            let retry_timeout_fut = if !pending_retry.is_empty() && connection.is_some() {
                tokio::time::sleep(Duration::from_millis(100))
            } else {
                tokio::time::sleep(Duration::from_secs(86400 * 365)) // 永不触发
            };

            // **关键新增：后台重连定时器**
            // 如果连接断开且在强制重连窗口内，定期尝试重连
            let background_reconnect_fut = if connection.is_none()
                && last_connection_failure.is_some()
                && last_connection_failure.unwrap().elapsed() < config.force_reconnect_window {
                if let Some(interval) = config.background_reconnect_interval {
                    tokio::time::sleep(interval)
                } else {
                    tokio::time::sleep(Duration::from_secs(86400 * 365))
                }
            } else {
                tokio::time::sleep(Duration::from_secs(86400 * 365))
            };

            tokio::select! {
                biased; // 使用有偏选择，确保优先级顺序

                // **新增：后台重连逻辑（最高优先级）**
                // 针对 tc18 问题：即使没有新消息，也主动尝试重连
                _ = background_reconnect_fut, if connection.is_none() && last_connection_failure.is_some() => {
                    debug!(peer_id, "Attempting background reconnection");
                    metrics.background_reconnect_attempts.fetch_add(1, Ordering::Relaxed);

                    connection = Self::establish_connection_with_retry(
                        peer_id, addr, &config, &metrics, &semaphore
                    ).await;

                    if connection.is_some() {
                        info!(peer_id, "Background reconnection successful");
                        last_connection_failure = None; // 连接成功，清除失败标记

                        // 连接恢复后，立即尝试处理待重试队列
                        if !pending_retry.is_empty() {
                            debug!(peer_id, pending_count = pending_retry.len(),
                                "Processing pending retry queue after reconnection");
                        }
                    } else {
                        debug!(peer_id, "Background reconnection failed, will retry");
                        last_connection_failure = Some(Instant::now());
                    }
                }

                // 0. 优先处理重试队列中的消息（当有连接时）
                _ = retry_timeout_fut, if !pending_retry.is_empty() && connection.is_some() => {
                    if let Some(msg) = pending_retry.pop_front() {
                        metrics.pending_retries.fetch_sub(1, Ordering::Relaxed);
                        last_activity = Instant::now();
                        Self::process_message(
                            peer_id,
                            addr,
                            msg,
                            &mut connection,
                            &mut buffer,
                            &config,
                            &metrics,
                            &semaphore,
                            MessagePriority::High, // 重试消息视为高优先级
                            &mut last_connection_failure,
                        ).await;
                    }
                }

                // 1. 优先处理高优先级消息（不参与批处理，立即发送）
                Some(msg) = high_priority_rx.recv() => {
                    last_activity = Instant::now();

                    // 如果没有连接，尝试建立（使用带重试的版本）
                    if connection.is_none() {
                        debug!(peer_id, "High-priority message triggered connection attempt");
                        connection = Self::establish_connection_with_retry(peer_id, addr, &config, &metrics, &semaphore).await;

                        // 连接仍然失败，将消息放入重试队列
                        if connection.is_none() {
                            last_connection_failure = Some(Instant::now()); // 标记连接失败
                            if pending_retry.len() < config.max_pending_retries {
                                pending_retry.push_back(msg);
                                metrics.pending_retries.fetch_add(1, Ordering::Relaxed);
                                debug!(peer_id, pending_count = pending_retry.len(), "High-priority message queued for retry");
                            } else {
                                // 队列满，丢弃消息
                                metrics.messages_dropped_queue_full.fetch_add(1, Ordering::Relaxed);
                                metrics.messages_failed.fetch_add(1, Ordering::Relaxed);
                                warn!(peer_id, "Pending retry queue full, high-priority message dropped");
                            }
                            continue;
                        } else {
                            last_connection_failure = None; // 连接成功，清除失败标记
                        }
                    }

                    // 立即发送（不批处理）
                    Self::process_message(
                        peer_id,
                        addr,
                        msg,
                        &mut connection,
                        &mut buffer,
                        &config,
                        &metrics,
                        &semaphore,
                        MessagePriority::High,
                        &mut last_connection_failure,
                    ).await;
                }

                // 2. 处理普通优先级消息（可批处理）
                Some(msg) = normal_priority_rx.recv(), if high_priority_rx.is_empty() => {
                    last_activity = Instant::now();

                    // 如果没有连接，尝试建立（使用带重试的版本）
                    if connection.is_none() {
                        connection = Self::establish_connection_with_retry(peer_id, addr, &config, &metrics, &semaphore).await;

                        // 连接仍然失败，将消息放入重试队列
                        if connection.is_none() {
                            last_connection_failure = Some(Instant::now()); // 标记连接失败
                            if pending_retry.len() < config.max_pending_retries {
                                pending_retry.push_back(msg);
                                metrics.pending_retries.fetch_add(1, Ordering::Relaxed);
                                debug!(peer_id, pending_count = pending_retry.len(), "Message queued for retry");
                            } else {
                                // 队列满，丢弃消息
                                metrics.messages_dropped_queue_full.fetch_add(1, Ordering::Relaxed);
                                metrics.messages_failed.fetch_add(1, Ordering::Relaxed);
                                warn!(peer_id, "Pending retry queue full, message dropped");
                            }
                            continue;
                        } else {
                            last_connection_failure = None; // 连接成功，清除失败标记
                        }
                    }

                    if batch_delay.is_some() {
                        // 启用批处理：收集消息
                        batch_buffer.push(msg);

                        // 如果达到批处理上限，立即发送
                        if batch_buffer.len() >= config.batch_max_messages {
                            Self::process_batch(
                                peer_id,
                                addr,
                                &mut batch_buffer,
                                &mut connection,
                                &mut buffer,
                                &config,
                                &metrics,
                                &semaphore,
                                &mut last_connection_failure,
                            ).await;
                        }
                    } else {
                        // 禁用批处理：立即发送
                        Self::process_message(
                            peer_id,
                            addr,
                            msg,
                            &mut connection,
                            &mut buffer,
                            &config,
                            &metrics,
                            &semaphore,
                            MessagePriority::Normal,
                            &mut last_connection_failure,
                        ).await;
                    }
                }

                // 3. 批处理超时：发送累积的消息
                _ = batch_timeout_fut, if !batch_buffer.is_empty() => {
                    Self::process_batch(
                        peer_id,
                        addr,
                        &mut batch_buffer,
                        &mut connection,
                        &mut buffer,
                        &config,
                        &metrics,
                        &semaphore,
                        &mut last_connection_failure,
                    ).await;
                }

                // 4. 空闲超时：关闭连接以释放资源
                _ = idle_timeout_fut, if connection.is_some() => {
                    debug!(peer_id, "Connection idle timeout, closing");
                    if let Some(mut conn) = connection.take() {
                        let _ = conn.stream.shutdown().await;
                        metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                    }
                    // 不退出 worker，等待新消息时重新连接
                }

                // 5. 优雅停机：刷完队列
                Some(WorkerCommand::Stop(ack)) = control_rx.recv() => {
                    debug!(peer_id, "Flushing remaining messages before stop");

                    // 先发送批处理缓冲区中的消息
                    if !batch_buffer.is_empty() {
                        Self::process_batch(
                            peer_id,
                            addr,
                            &mut batch_buffer,
                            &mut connection,
                            &mut buffer,
                            &config,
                            &metrics,
                            &semaphore,
                            &mut last_connection_failure,
                        ).await;
                    }

                    // 处理重试队列中的消息
                    while let Some(msg) = pending_retry.pop_front() {
                        metrics.pending_retries.fetch_sub(1, Ordering::Relaxed);
                        Self::process_message(
                            peer_id, addr, msg, &mut connection, &mut buffer,
                            &config, &metrics, &semaphore, MessagePriority::High,
                            &mut last_connection_failure,
                        ).await;
                    }

                    // 刷完所有消息（带超时）
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

        // 清理连接
        if let Some(mut conn) = connection.take() {
            let _ = conn.stream.shutdown().await;
            metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
        }

        // 清理待重试队列的指标
        let remaining = pending_retry.len();
        if remaining > 0 {
            metrics.pending_retries.fetch_sub(remaining, Ordering::Relaxed);
            metrics.messages_failed.fetch_add(remaining as u64, Ordering::Relaxed);
            debug!(peer_id, remaining, "Discarding pending retry messages on worker exit");
        }

        debug!(peer_id, "Worker exited");
    }

    /// 批量处理消息（零拷贝优化）
    ///
    /// **关键改进：增加连接失败追踪**
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

        trace!(peer_id, batch_size = batch.len(), "Processing message batch");

        // 确保有连接（使用带重试的版本）
        if connection.is_none() {
            *connection = Self::establish_connection_with_retry(peer_id, addr, config, metrics, semaphore).await;
            if connection.is_none() {
                *last_connection_failure = Some(Instant::now());
            } else {
                *last_connection_failure = None;
            }
        }

        if let Some(ref mut conn) = connection {
            buffer.clear();

            // 零拷贝：将所有消息直接编码到同一个缓冲区
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

            // 一次性发送所有数据
            match tokio::time::timeout(config.write_timeout, conn.stream.write_all(buffer)).await {
                Ok(Ok(_)) => {
                    if let Err(e) = conn.stream.flush().await {
                        warn!(peer_id, error = %e, "Batch flush failed");
                        // **关键：连接失效，完全移除并标记**
                        if let Some(mut conn) = connection.take() {
                            let _ = conn.stream.shutdown().await;
                            metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                        }
                        *last_connection_failure = Some(Instant::now());
                        metrics.messages_failed.fetch_add(batch.len() as u64, Ordering::Relaxed);
                    } else {
                        metrics.messages_sent.fetch_add(batch.len() as u64, Ordering::Relaxed);
                        metrics.normal_priority_sent.fetch_add(batch.len() as u64, Ordering::Relaxed);
                        trace!(peer_id, count = batch.len(), "Batch sent successfully");
                        *last_connection_failure = None; // 发送成功，清除失败标记
                    }
                }
                Ok(Err(e)) => {
                    warn!(peer_id, error = %e, "Batch write failed");
                    // **关键：连接失效，完全移除并标记**
                    if let Some(mut conn) = connection.take() {
                        let _ = conn.stream.shutdown().await;
                        metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                    }
                    *last_connection_failure = Some(Instant::now());
                    metrics.messages_failed.fetch_add(batch.len() as u64, Ordering::Relaxed);
                }
                Err(_) => {
                    warn!(peer_id, "Batch write timeout");
                    // **关键：连接失效，完全移除并标记**
                    if let Some(mut conn) = connection.take() {
                        let _ = conn.stream.shutdown().await;
                        metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                    }
                    *last_connection_failure = Some(Instant::now());
                    metrics.messages_failed.fetch_add(batch.len() as u64, Ordering::Relaxed);
                }
            }
        } else {
            *last_connection_failure = Some(Instant::now());
            metrics.messages_failed.fetch_add(batch.len() as u64, Ordering::Relaxed);
        }

        batch.clear();
    }

    /// 处理单条消息（带指数退避 + Jitter）
    ///
    /// **关键改进：增加连接失败追踪参数**
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

            // 确保有连接（使用带重试的版本）
            if connection.is_none() {
                *connection =
                    Self::establish_connection_with_retry(peer_id, addr, config, metrics, semaphore).await;
                if connection.is_none() {
                    *last_connection_failure = Some(Instant::now());
                } else {
                    *last_connection_failure = None;
                }
            }

            if let Some(ref mut conn) = connection {
                buffer.clear(); // 复用缓冲区

                match Self::send_message_to_stream(&mut conn.stream, &pending.msg, buffer, config).await {
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
                        *last_connection_failure = None; // 发送成功，清除失败标记
                    }
                    Err(e) => {
                        warn!(peer_id, error = %e, attempt = attempts, "Send failed");

                        // **关键改进：连接失效时，必须完全移除旧连接并标记失败**
                        if let Some(mut conn) = connection.take() {
                            let _ = conn.stream.shutdown().await;
                            metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                        }
                        *last_connection_failure = Some(Instant::now());

                        metrics.connections_failed.fetch_add(1, Ordering::Relaxed);

                        if attempts < config.max_retries {
                            // 指数退避 + Jitter
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

    /// 建立连接（返回 TiedConnection，自动管理 Permit）
    async fn establish_connection(
        peer_id: NodeId,
        addr: SocketAddr,
        config: &TransportConfig,
        metrics: &Arc<TransportMetrics>,
        semaphore: &Arc<Semaphore>,
    ) -> Option<TiedConnection> {
        // 获取 Owned Permit
        let permit = match semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                warn!(peer_id, "Connection limit reached");
                return None;
            }
        };

        match tokio::time::timeout(config.connect_timeout, TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => {
                // TCP 优化
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

    /// 建立连接（带重试机制）
    ///
    /// 针对瞬时网络错误（如 ConnectionRefused）进行重试，
    /// 使用指数退避 + Jitter 策略。
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

            // 获取连接许可
            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    warn!(peer_id, "Connection limit reached");
                    return None;
                }
            };

            // 尝试连接
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
                    // 判断是否为可重试的错误
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

                        // 指数退避 + Jitter
                        if config.enable_retry_jitter {
                            let jitter = Duration::from_millis(rand::random::<u64>() % 20);
                            retry_delay = (retry_delay * 2 + jitter).min(config.max_connect_retry_delay);
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

    /// 配置 TCP 参数（增强 Keep-Alive）
    fn configure_tcp(stream: &TcpStream, config: &TransportConfig) -> std::io::Result<()> {
        // 禁用 Nagle
        if config.enable_tcp_nodelay {
            stream.set_nodelay(true)?;
        }

        // 设置增强的 TCP Keep-Alive
        let socket_ref = SockRef::from(stream);
        let keepalive = TcpKeepalive::new()
            .with_time(config.tcp_keepalive_time)
            .with_interval(config.tcp_keepalive_interval);

        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let keepalive = keepalive.with_retries(config.tcp_keepalive_retries);

        socket_ref.set_tcp_keepalive(&keepalive)?;

        Ok(())
    }

    /// 发送消息（零拷贝优化）
    ///
    /// 使用 encode_message_into 直接写入复用的 BytesMut 缓冲区，
    /// 避免中间 Vec<u8> 分配。
    async fn send_message_to_stream(
        stream: &mut TcpStream,
        msg: &Message,
        buffer: &mut BytesMut,
        config: &TransportConfig,
    ) -> Result<()> {
        // 零拷贝：直接编码到 buffer，避免中间 Vec 分配
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
        // 仅在 command_tx 还没关闭时尝试发送，不阻塞
        let (tx, _) = oneshot::channel();
        let _ = self.command_tx.send(TransportCommand::Shutdown(tx));

        // 关键点：不要在这里 await dispatcher_handle，
        // Drop 是同步的，Dispatcher 是异步的。
        // 让 tokio runtime 在 handle 失去引用时自动清理任务。
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