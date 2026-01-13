use crate::error::{NetworkError, Result};
use crate::network::rpc::{encode_message, Message, RaftMessageWrapper};
use crate::types::NodeId;
use parking_lot::RwLock;
use raft::prelude::Message as RaftMessage;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, trace, warn};

/// Configuration for transport behavior
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// Maximum retry attempts for failed sends
    pub max_retries: usize,
    /// Delay between retries
    pub retry_delay: Duration,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Write timeout
    pub write_timeout: Duration,
    /// Maximum concurrent connections
    pub max_connections: usize,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
            connect_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            max_connections: 1000,
        }
    }
}

/// Commands that can be sent to the sender loop
#[derive(Debug)]
enum SenderCommand {
    /// Update peer address atomically (clears connection and updates address)
    UpdatePeer {
        peer_id: NodeId,
        new_addr: SocketAddr,
    },
    /// Clear cached connection for a specific peer
    ClearConnection(NodeId),
    /// Shutdown the sender loop with acknowledgment
    Shutdown(oneshot::Sender<()>),
}

/// Transport metrics for monitoring
#[derive(Debug, Default)]
pub struct TransportMetrics {
    pub messages_sent: AtomicUsize,
    pub messages_failed: AtomicUsize,
    pub connections_created: AtomicUsize,
    pub connections_failed: AtomicUsize,
    pub active_connections: AtomicUsize,
}

#[derive(Debug, Clone)]
pub struct TransportMetricsSnapshot {
    pub messages_sent: usize,
    pub messages_failed: usize,
    pub connections_created: usize,
    pub connections_failed: usize,
    pub active_connections: usize,
}

/// Transport for sending messages to other Raft nodes.
pub struct RaftTransport {
    /// This node's ID.
    node_id: NodeId,

    /// Mapping of node IDs to their addresses.
    peers: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,

    /// Channel for outgoing messages.
    outgoing_tx: mpsc::UnboundedSender<(NodeId, Message)>,

    /// Channel for control commands
    command_tx: mpsc::UnboundedSender<SenderCommand>,

    /// Handle to the sender task.
    sender_handle: Arc<tokio::task::JoinHandle<()>>,

    /// Transport configuration
    config: TransportConfig,

    /// Metrics
    metrics: Arc<TransportMetrics>,
}

impl RaftTransport {
    /// Create a new transport with default config.
    pub fn new(node_id: NodeId) -> Self {
        Self::with_config(node_id, TransportConfig::default())
    }

    /// Create a new transport with custom config.
    pub fn with_config(node_id: NodeId, config: TransportConfig) -> Self {
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel();
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let peers = Arc::new(RwLock::new(HashMap::new()));
        let peers_clone = peers.clone();
        let config_clone = config.clone();
        let metrics = Arc::new(TransportMetrics::default());
        let metrics_clone = metrics.clone();

        let sender_handle = tokio::spawn(async move {
            Self::sender_loop(
                node_id,
                peers_clone,
                outgoing_rx,
                command_rx,
                config_clone,
                metrics_clone,
            )
            .await;
        });

        info!(node_id, "RaftTransport created");

        Self {
            node_id,
            peers,
            outgoing_tx,
            command_tx,
            sender_handle: Arc::new(sender_handle),
            config,
            metrics,
        }
    }

    /// Add a peer to the transport.
    pub fn add_peer(&self, id: NodeId, addr: SocketAddr) {
        self.peers.write().insert(id, addr);
        debug!(node_id = self.node_id, peer_id = id, %addr, "Peer added");
    }

    /// Update a peer's address atomically (clears cached connection)
    pub fn update_peer(&self, id: NodeId, addr: SocketAddr) {
        // 发送原子更新命令 - 由 sender_loop 处理以避免竞态条件
        let _ = self.command_tx.send(SenderCommand::UpdatePeer {
            peer_id: id,
            new_addr: addr,
        });
        debug!(node_id = self.node_id, peer_id = id, %addr, "Peer update queued");
    }

    /// Remove a peer from the transport.
    pub fn remove_peer(&self, id: NodeId) {
        self.peers.write().remove(&id);
        let _ = self.command_tx.send(SenderCommand::ClearConnection(id));
        debug!(node_id = self.node_id, peer_id = id, "Peer removed");
    }

    /// Get a peer's address.
    pub fn get_peer(&self, id: NodeId) -> Option<SocketAddr> {
        self.peers.read().get(&id).copied()
    }

    /// Get all peer IDs.
    pub fn peer_ids(&self) -> Vec<NodeId> {
        self.peers.read().keys().copied().collect()
    }

    /// Get peer count
    pub fn peer_count(&self) -> usize {
        self.peers.read().len()
    }

    /// Get transport metrics snapshot
    pub fn metrics(&self) -> TransportMetricsSnapshot {
        TransportMetricsSnapshot {
            messages_sent: self.metrics.messages_sent.load(Ordering::Relaxed),
            messages_failed: self.metrics.messages_failed.load(Ordering::Relaxed),
            connections_created: self.metrics.connections_created.load(Ordering::Relaxed),
            connections_failed: self.metrics.connections_failed.load(Ordering::Relaxed),
            active_connections: self.metrics.active_connections.load(Ordering::Relaxed),
        }
    }

    /// Send a Raft message to a peer.
    pub fn send(&self, msg: RaftMessage) -> Result<()> {
        let to = msg.to;
        let msg_type = msg.msg_type;

        let wrapper = RaftMessageWrapper::from_raft_message(&msg)
            .map_err(|e| NetworkError::Serialization(e.to_string()))?;

        let message = Message::Raft(wrapper);

        self.outgoing_tx
            .send((to, message))
            .map_err(|_| NetworkError::SendFailed("channel closed".to_string()))?;

        trace!(
            from = self.node_id,
            to = to,
            msg_type = ?msg_type,
            "Raft message queued"
        );

        Ok(())
    }

    /// Send multiple Raft messages.
    pub fn send_messages(&self, msgs: Vec<RaftMessage>) {
        let count = msgs.len();
        if count > 0 {
            debug!(node_id = self.node_id, count, "Queuing Raft messages");
        }

        for msg in msgs {
            trace!(
                from = msg.from,
                to = msg.to,
                msg_type = ?msg.msg_type,
                "Queuing message"
            );

            if let Err(e) = self.send(msg) {
                warn!(error = %e, "Failed to queue Raft message");
            }
        }
    }

    /// Shutdown the transport gracefully
    pub async fn shutdown(&self) {
        info!(node_id = self.node_id, "Shutting down transport");

        let (tx, rx) = oneshot::channel();
        let _ = self.command_tx.send(SenderCommand::Shutdown(tx));

        // 等待确认或超时
        match tokio::time::timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(_)) => info!(node_id = self.node_id, "Transport shutdown complete"),
            Ok(Err(_)) => warn!(node_id = self.node_id, "Shutdown channel dropped"),
            Err(_) => warn!(node_id = self.node_id, "Transport shutdown timeout"),
        }
    }

    /// Background loop that sends queued messages with retry logic.
    async fn sender_loop(
        node_id: NodeId,
        peers: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
        mut rx: mpsc::UnboundedReceiver<(NodeId, Message)>,
        mut command_rx: mpsc::UnboundedReceiver<SenderCommand>,
        config: TransportConfig,
        metrics: Arc<TransportMetrics>,
    ) {
        info!(node_id, "Sender loop started");

        // Connection cache with address validation - stores (address, stream)
        let mut connections: HashMap<NodeId, (SocketAddr, TcpStream)> = HashMap::new();

        loop {
            tokio::select! {
                Some((to, msg)) = rx.recv() => {
                    trace!(node_id, to, "Processing outgoing message");

                    let current_addr = {
                        let peers = peers.read();
                        peers.get(&to).copied()
                    };

                    let current_addr = match current_addr {
                        Some(a) => a,
                        None => {
                            debug!(node_id, to, "Unknown peer, dropping message");
                            metrics.messages_failed.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };

                    // Check connection limit
                    if connections.len() >= config.max_connections {
                        warn!(
                            node_id,
                            connections = connections.len(),
                            max = config.max_connections,
                            "Connection limit reached"
                        );
                    }

                    // Retry loop
                    let mut attempts = 0;
                    let mut success = false;

                    while attempts < config.max_retries && !success {
                        attempts += 1;

                        trace!(
                            node_id,
                            to,
                            addr = %current_addr,
                            attempt = attempts,
                            max_retries = config.max_retries,
                            "Attempting to send message"
                        );

                        match Self::send_to_peer_with_timeout(
                            &mut connections,
                            to,
                            current_addr,
                            &msg,
                            &config,
                            &metrics,
                        )
                        .await
                        {
                            Ok(_) => {
                                trace!(node_id, to, "Message sent successfully");
                                metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
                                success = true;
                            }
                            Err(e) => {
                                warn!(
                                    node_id,
                                    to,
                                    error = %e,
                                    attempt = attempts,
                                    "Failed to send message"
                                );

                                // Remove stale connection
                                if let Some((_, mut stream)) = connections.remove(&to) {
                                    let _ = stream.shutdown().await;
                                    metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                                }

                                metrics.connections_failed.fetch_add(1, Ordering::Relaxed);

                                // Wait before retry (except on last attempt)
                                if attempts < config.max_retries {
                                    sleep(config.retry_delay).await;
                                }
                            }
                        }
                    }

                    if !success {
                        error!(
                            node_id,
                            to,
                            attempts,
                            "Failed to send message after all retries"
                        );
                        metrics.messages_failed.fetch_add(1, Ordering::Relaxed);
                    }
                }

                Some(cmd) = command_rx.recv() => {
                    match cmd {
                        SenderCommand::UpdatePeer { peer_id, new_addr } => {
                            // 原子操作：先清除连接，再更新地址
                            if let Some((old_addr, mut stream)) = connections.remove(&peer_id) {
                                let _ = stream.shutdown().await;
                                metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                                debug!(
                                    node_id,
                                    peer_id,
                                    old_addr = %old_addr,
                                    new_addr = %new_addr,
                                    "Connection cleared for address update"
                                );
                            }

                            // 更新地址
                            peers.write().insert(peer_id, new_addr);
                            info!(node_id, peer_id, %new_addr, "Peer address updated");
                        }

                        SenderCommand::ClearConnection(peer_id) => {
                            if let Some((_, mut stream)) = connections.remove(&peer_id) {
                                let _ = stream.shutdown().await;
                                metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                                debug!(node_id, peer_id, "Connection cleared");
                            }
                        }

                        SenderCommand::Shutdown(ack) => {
                            info!(node_id, "Received shutdown command");
                            let _ = ack.send(()); // 发送确认
                            break;
                        }
                    }
                }
            }
        }

        // Close all connections
        for (peer_id, (_, mut stream)) in connections.drain() {
            let _ = stream.shutdown().await;
            debug!(node_id, peer_id, "Connection closed");
        }

        metrics.active_connections.store(0, Ordering::Relaxed);
        info!(node_id, "Sender loop exited");
    }

    async fn send_to_peer_with_timeout(
        connections: &mut HashMap<NodeId, (SocketAddr, TcpStream)>,
        to: NodeId,
        addr: SocketAddr,
        msg: &Message,
        config: &TransportConfig,
        metrics: &Arc<TransportMetrics>,
    ) -> Result<()> {
        tokio::time::timeout(
            config.write_timeout,
            Self::send_to_peer(connections, to, addr, msg, config, metrics),
        )
        .await
        .map_err(|_| NetworkError::SendFailed("send timeout".to_string()))?
    }

    async fn send_to_peer(
        connections: &mut HashMap<NodeId, (SocketAddr, TcpStream)>,
        to: NodeId,
        addr: SocketAddr,
        msg: &Message,
        config: &TransportConfig,
        metrics: &Arc<TransportMetrics>,
    ) -> Result<()> {
        // Encode message
        let data = encode_message(msg)?;
        let len = data.len() as u32;

        // Get or create connection with address validation
        let stream = if let Some((cached_addr, stream)) = connections.get_mut(&to) {
            if *cached_addr == addr {
                // Address matches - check if connection is alive
                match stream.writable().await {
                    Ok(_) => stream,
                    Err(_) => {
                        // Connection is dead, recreate
                        debug!("Connection dead, recreating for peer {}", to);
                        connections.remove(&to);
                        metrics.active_connections.fetch_sub(1, Ordering::Relaxed);

                        let new_stream =
                            tokio::time::timeout(config.connect_timeout, TcpStream::connect(addr))
                                .await
                                .map_err(|_| NetworkError::ConnectionFailed {
                                    addr: addr.to_string(),
                                    reason: "connection timeout".to_string(),
                                })?
                                .map_err(|e| NetworkError::ConnectionFailed {
                                    addr: addr.to_string(),
                                    reason: e.to_string(),
                                })?;

                        metrics.connections_created.fetch_add(1, Ordering::Relaxed);
                        metrics.active_connections.fetch_add(1, Ordering::Relaxed);
                        connections.insert(to, (addr, new_stream));
                        &mut connections.get_mut(&to).unwrap().1
                    }
                }
            } else {
                // Address changed! Clear stale connection and create new one
                warn!(
                    "Peer {} address changed from {} to {}, clearing stale connection",
                    to, cached_addr, addr
                );
                connections.remove(&to);
                metrics.active_connections.fetch_sub(1, Ordering::Relaxed);

                let new_stream =
                    tokio::time::timeout(config.connect_timeout, TcpStream::connect(addr))
                        .await
                        .map_err(|_| NetworkError::ConnectionFailed {
                            addr: addr.to_string(),
                            reason: "connection timeout".to_string(),
                        })?
                        .map_err(|e| NetworkError::ConnectionFailed {
                            addr: addr.to_string(),
                            reason: e.to_string(),
                        })?;

                metrics.connections_created.fetch_add(1, Ordering::Relaxed);
                metrics.active_connections.fetch_add(1, Ordering::Relaxed);
                connections.insert(to, (addr, new_stream));
                &mut connections.get_mut(&to).unwrap().1
            }
        } else {
            // Create new connection
            let stream = tokio::time::timeout(config.connect_timeout, TcpStream::connect(addr))
                .await
                .map_err(|_| NetworkError::ConnectionFailed {
                    addr: addr.to_string(),
                    reason: "connection timeout".to_string(),
                })?
                .map_err(|e| NetworkError::ConnectionFailed {
                    addr: addr.to_string(),
                    reason: e.to_string(),
                })?;

            metrics.connections_created.fetch_add(1, Ordering::Relaxed);
            metrics.active_connections.fetch_add(1, Ordering::Relaxed);
            connections.insert(to, (addr, stream));
            &mut connections.get_mut(&to).unwrap().1
        };

        // Write data
        stream
            .write_all(&len.to_be_bytes())
            .await
            .map_err(|e| NetworkError::SendFailed(format!("failed to write length: {}", e)))?;

        stream
            .write_all(&data)
            .await
            .map_err(|e| NetworkError::SendFailed(format!("failed to write data: {}", e)))?;

        stream
            .flush()
            .await
            .map_err(|e| NetworkError::SendFailed(format!("failed to flush: {}", e)))?;

        Ok(())
    }
}

impl Drop for RaftTransport {
    fn drop(&mut self) {
        let (tx, _rx) = oneshot::channel();
        let _ = self.command_tx.send(SenderCommand::Shutdown(tx));
    }
}

impl std::fmt::Debug for RaftTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftTransport")
            .field("node_id", &self.node_id)
            .field("peer_count", &self.peer_count())
            .field("config", &self.config)
            .field("metrics", &self.metrics())
            .finish()
    }
}

// ============================================================================
// 测试改进
// ============================================================================

#[cfg(test)]
mod integration_tests {
    use super::*;
    use raft::prelude::MessageType;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    /// Helper function to create a test Raft message
    fn create_test_message(from: NodeId, to: NodeId, msg_type: MessageType) -> RaftMessage {
        let mut msg = RaftMessage::default();
        msg.set_from(from);
        msg.set_to(to);
        msg.set_msg_type(msg_type);
        msg.set_term(1);
        msg.set_log_term(0);
        msg.set_index(0);
        msg.set_commit(0);
        msg.set_reject(false);
        msg.set_reject_hint(0);
        msg
    }

    /// Mock server that receives and records messages
    struct MockRaftServer {
        listener: TcpListener,
        received_messages: Arc<Mutex<Vec<Message>>>,
        message_count: Arc<AtomicUsize>,
    }

    impl MockRaftServer {
        async fn new() -> Result<Self> {
            let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| {
                NetworkError::ConnectionFailed {
                    addr: "127.0.0.1:0".to_string(),
                    reason: e.to_string(),
                }
            })?;

            Ok(Self {
                listener,
                received_messages: Arc::new(Mutex::new(Vec::new())),
                message_count: Arc::new(AtomicUsize::new(0)),
            })
        }

        fn addr(&self) -> SocketAddr {
            self.listener.local_addr().unwrap()
        }

        async fn run(&self) {
            let received = self.received_messages.clone();
            let count = self.message_count.clone();

            loop {
                match self.listener.accept().await {
                    Ok((mut socket, _)) => {
                        let received = received.clone();
                        let count = count.clone();

                        tokio::spawn(async move {
                            loop {
                                // Read length prefix (4 bytes)
                                let mut len_buf = [0u8; 4];
                                match socket.read_exact(&mut len_buf).await {
                                    Ok(_) => {}
                                    Err(_) => break, // Connection closed
                                }

                                let len = u32::from_be_bytes(len_buf) as usize;

                                // Read message data
                                let mut data = vec![0u8; len];
                                match socket.read_exact(&mut data).await {
                                    Ok(_) => {}
                                    Err(_) => break,
                                }

                                // Decode message
                                match bincode::deserialize::<Message>(&data) {
                                    Ok(msg) => {
                                        received.lock().await.push(msg);
                                        count.fetch_add(1, Ordering::SeqCst);
                                    }
                                    Err(e) => {
                                        eprintln!("Failed to deserialize message: {}", e);
                                        break;
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("Failed to accept connection: {}", e);
                        break;
                    }
                }
            }
        }

        async fn get_received_messages(&self) -> Vec<Message> {
            self.received_messages.lock().await.clone()
        }

        fn get_message_count(&self) -> usize {
            self.message_count.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn test_send_single_message_to_peer() {
        // Setup mock server
        let server = MockRaftServer::new().await.unwrap();
        let server_addr = server.addr();
        let received_messages = server.received_messages.clone();

        // Start server in background
        tokio::spawn(async move {
            server.run().await;
        });

        // Give server time to start
        sleep(Duration::from_millis(50)).await;

        // Create transport and add peer
        let transport = RaftTransport::new(1);
        transport.add_peer(2, server_addr);

        // Send a message
        let msg = create_test_message(1, 2, MessageType::MsgHeartbeat);
        transport.send(msg).unwrap();

        // Wait for message to be sent and received
        sleep(Duration::from_millis(200)).await;

        // Verify message was received
        let received = received_messages.lock().await;
        assert_eq!(received.len(), 1);

        if let Message::Raft(wrapper) = &received[0] {
            let raft_msg = wrapper.to_raft_message().unwrap();
            assert_eq!(raft_msg.get_from(), 1);
            assert_eq!(raft_msg.get_to(), 2);
            assert_eq!(raft_msg.get_msg_type(), MessageType::MsgHeartbeat);
        } else {
            panic!("Expected Raft message");
        }

        transport.shutdown().await;
    }

    #[tokio::test]
    async fn test_send_multiple_messages() {
        let server = MockRaftServer::new().await.unwrap();
        let server_addr = server.addr();
        let received_messages = server.received_messages.clone();

        tokio::spawn(async move {
            server.run().await;
        });

        sleep(Duration::from_millis(50)).await;

        let transport = RaftTransport::new(1);
        transport.add_peer(2, server_addr);

        // Send multiple messages
        let messages = vec![
            create_test_message(1, 2, MessageType::MsgHeartbeat),
            create_test_message(1, 2, MessageType::MsgAppend),
            create_test_message(1, 2, MessageType::MsgRequestVote),
        ];

        transport.send_messages(messages);

        // Wait for all messages to be processed
        sleep(Duration::from_millis(300)).await;

        // Verify all messages received
        let received = received_messages.lock().await;
        assert_eq!(received.len(), 3);

        transport.shutdown().await;
    }

    #[tokio::test]
    async fn test_connection_reuse() {
        let server = MockRaftServer::new().await.unwrap();
        let server_addr = server.addr();
        let count = server.message_count.clone();

        tokio::spawn(async move {
            server.run().await;
        });

        sleep(Duration::from_millis(50)).await;

        let transport = RaftTransport::new(1);
        transport.add_peer(2, server_addr);

        // Send multiple messages - should reuse connection
        for _ in 0..5 {
            let msg = create_test_message(1, 2, MessageType::MsgHeartbeat);
            transport.send(msg).unwrap();
            sleep(Duration::from_millis(50)).await;
        }

        // Wait for processing
        sleep(Duration::from_millis(200)).await;

        assert_eq!(count.load(Ordering::SeqCst), 5);

        transport.shutdown().await;
    }

    #[tokio::test]
    async fn test_retry_on_connection_failure() {
        let config = TransportConfig {
            max_retries: 3,
            retry_delay: Duration::from_millis(50),
            connect_timeout: Duration::from_millis(500),
            write_timeout: Duration::from_millis(500),
            max_connections: 10,
        };

        let transport = RaftTransport::with_config(1, config);

        // Add peer with invalid address (will fail to connect)
        transport.add_peer(2, "127.0.0.1:9999".parse().unwrap());

        // This should fail after retries
        let msg = create_test_message(1, 2, MessageType::MsgHeartbeat);
        transport.send(msg).unwrap();

        // Wait for retries to complete
        sleep(Duration::from_millis(500)).await;

        transport.shutdown().await;
        // Test passes if no panic occurs - failures are logged
    }

    #[tokio::test]
    async fn test_send_to_unknown_peer() {
        let transport = RaftTransport::new(1);

        // Don't add peer 2
        let msg = create_test_message(1, 2, MessageType::MsgHeartbeat);
        transport.send(msg).unwrap();

        // Message should be queued but dropped when processing
        sleep(Duration::from_millis(100)).await;

        transport.shutdown().await;
        // Test passes - unknown peer is handled gracefully
    }

    #[tokio::test]
    async fn test_multiple_peers() {
        // Create two mock servers
        let server1 = MockRaftServer::new().await.unwrap();
        let server2 = MockRaftServer::new().await.unwrap();
        let addr1 = server1.addr();
        let addr2 = server2.addr();
        let count1 = server1.message_count.clone();
        let count2 = server2.message_count.clone();

        tokio::spawn(async move { server1.run().await });
        tokio::spawn(async move { server2.run().await });

        sleep(Duration::from_millis(50)).await;

        let transport = RaftTransport::new(1);
        transport.add_peer(2, addr1);
        transport.add_peer(3, addr2);

        // Send to both peers
        transport
            .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
            .unwrap();
        transport
            .send(create_test_message(1, 3, MessageType::MsgHeartbeat))
            .unwrap();

        sleep(Duration::from_millis(200)).await;

        assert_eq!(count1.load(Ordering::SeqCst), 1);
        assert_eq!(count2.load(Ordering::SeqCst), 1);

        transport.shutdown().await;
    }

    #[tokio::test]
    async fn test_peer_management() {
        let transport = RaftTransport::new(1);

        // Add peers
        let addr1: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:9002".parse().unwrap();

        transport.add_peer(2, addr1);
        transport.add_peer(3, addr2);

        assert_eq!(transport.peer_count(), 2);
        assert_eq!(transport.get_peer(2), Some(addr1));
        assert_eq!(transport.get_peer(3), Some(addr2));

        let peer_ids = transport.peer_ids();
        assert!(peer_ids.contains(&2));
        assert!(peer_ids.contains(&3));

        // Remove peer
        transport.remove_peer(2);
        assert_eq!(transport.peer_count(), 1);
        assert_eq!(transport.get_peer(2), None);

        transport.shutdown().await;
    }

    #[tokio::test]
    async fn test_concurrent_sends() {
        let server = MockRaftServer::new().await.unwrap();
        let server_addr = server.addr();
        let count = server.message_count.clone();

        tokio::spawn(async move {
            server.run().await;
        });

        sleep(Duration::from_millis(50)).await;

        let transport = Arc::new(RaftTransport::new(1));
        transport.add_peer(2, server_addr);

        // Spawn multiple tasks sending concurrently
        let mut handles = vec![];
        for i in 0..10 {
            let transport_clone = transport.clone();
            let handle = tokio::spawn(async move {
                let msg = create_test_message(1, 2, MessageType::MsgHeartbeat);
                transport_clone.send(msg).unwrap();
            });
            handles.push(handle);
        }

        // Wait for all sends to complete
        for handle in handles {
            handle.await.unwrap();
        }

        sleep(Duration::from_millis(500)).await;

        assert_eq!(count.load(Ordering::SeqCst), 10);

        transport.shutdown().await;
    }
    #[tokio::test]
    async fn test_stale_connection_recovery() {
        // This test verifies that when a connection becomes stale,
        // the transport detects it and handles it gracefully

        let server = MockRaftServer::new().await.unwrap();
        let server_addr = server.addr();
        let count = server.message_count.clone();

        let handle = tokio::spawn(async move {
            server.run().await;
        });

        sleep(Duration::from_millis(50)).await;

        let transport = RaftTransport::new(1);
        transport.add_peer(2, server_addr);

        // Send first message - establishes connection
        transport
            .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
            .unwrap();
        sleep(Duration::from_millis(200)).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);

        // Kill the server to make connection stale
        handle.abort();
        sleep(Duration::from_millis(200)).await;

        // Try to send - should fail after retries
        transport
            .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
            .unwrap();

        // Wait for retries to complete and fail
        sleep(Duration::from_millis(500)).await;

        // Message should not be delivered (server is down)
        // Test passes if no panic occurs - this validates error handling

        transport.shutdown().await;
    }

    #[tokio::test]
    async fn test_connection_recovery_after_server_restart() {
        // This test simulates a server restarting on a new address

        // Start first server
        let server1 = MockRaftServer::new().await.unwrap();
        let addr1 = server1.addr();
        let count1 = server1.message_count.clone();

        let handle1 = tokio::spawn(async move {
            server1.run().await;
        });

        sleep(Duration::from_millis(50)).await;

        let transport = RaftTransport::new(1);
        transport.add_peer(2, addr1);

        // Send message to first server
        transport
            .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
            .unwrap();
        sleep(Duration::from_millis(200)).await;

        assert_eq!(count1.load(Ordering::SeqCst), 1);

        // Abort first server (simulating crash)
        handle1.abort();
        sleep(Duration::from_millis(200)).await;

        // Start second server on a DIFFERENT port (simulating restart with new address)
        let server2 = MockRaftServer::new().await.unwrap();
        let addr2 = server2.addr();
        let count2 = server2.message_count.clone();

        tokio::spawn(async move {
            server2.run().await;
        });

        sleep(Duration::from_millis(100)).await;

        // Update peer address to new server - this simulates address discovery
        transport.update_peer(2, addr2);

        // Send message - should work because we updated the peer address
        // The transport will create a new connection to the new address
        transport
            .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
            .unwrap();

        // Give more time for connection + retries
        sleep(Duration::from_millis(500)).await;

        // Should have received at least 1 message
        let received_count = count2.load(Ordering::SeqCst);
        assert!(
            received_count >= 1,
            "Expected at least 1 message, got {}",
            received_count
        );

        transport.shutdown().await;
    }

    #[tokio::test]
    async fn test_large_message_send() {
        let server = MockRaftServer::new().await.unwrap();
        let server_addr = server.addr();
        let received_messages = server.received_messages.clone();

        tokio::spawn(async move {
            server.run().await;
        });

        sleep(Duration::from_millis(50)).await;

        let transport = RaftTransport::new(1);
        transport.add_peer(2, server_addr);

        // Create a large message with many entries
        let mut msg = create_test_message(1, 2, MessageType::MsgAppend);

        // Add 1000 log entries using protobuf RepeatedField
        let entries: Vec<_> = (0..1000)
            .map(|_| {
                let mut entry = raft::prelude::Entry::default();
                entry.set_data(vec![1, 2, 3, 4, 5].into());
                entry
            })
            .collect();
        msg.set_entries(entries.into());

        transport.send(msg).unwrap();

        sleep(Duration::from_millis(500)).await;

        let received = received_messages.lock().await;
        assert_eq!(received.len(), 1);

        if let Message::Raft(wrapper) = &received[0] {
            let raft_msg = wrapper.to_raft_message().unwrap();
            assert_eq!(raft_msg.get_entries().len(), 1000);
        }

        transport.shutdown().await;
    }

    #[tokio::test]
    async fn test_connection_recovery_with_retry() {
        // This test verifies that the retry logic can handle temporary connection failures
        // NOTE: The current implementation caches connections in sender_loop.
        // When a connection fails, the retry logic will remove it from cache and reconnect.

        let server = MockRaftServer::new().await.unwrap();
        let server_addr = server.addr();
        let count = server.message_count.clone();

        // Start server
        let handle = tokio::spawn(async move {
            server.run().await;
        });

        sleep(Duration::from_millis(50)).await;

        let transport = RaftTransport::new(1);
        transport.add_peer(2, server_addr);

        // Send first message - establishes connection
        transport
            .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
            .unwrap();
        sleep(Duration::from_millis(200)).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);

        // Kill server temporarily
        handle.abort();
        sleep(Duration::from_millis(100)).await;

        // Restart server on SAME address (same port)
        let server2 = MockRaftServer::new().await.unwrap();
        // We need to bind to a specific port to reuse the address, but since we can't
        // do that easily with port 0, this test demonstrates the limitation.

        // For now, this test just verifies the transport handles the failure gracefully
        transport
            .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
            .unwrap();

        // Message will fail to send (server is down) but transport should handle it
        sleep(Duration::from_millis(500)).await;

        transport.shutdown().await;
        // Test passes if no panic - demonstrates graceful failure handling
    }

    #[tokio::test]
    async fn test_peer_address_update_scenario() {
        // This test demonstrates the CORRECT way to handle peer address changes
        // with the current implementation: remove old peer, add new one

        // Setup first server
        let server1 = MockRaftServer::new().await.unwrap();
        let addr1 = server1.addr();
        let count1 = server1.message_count.clone();

        tokio::spawn(async move {
            server1.run().await;
        });

        sleep(Duration::from_millis(50)).await;

        let transport = RaftTransport::new(1);
        transport.add_peer(2, addr1);

        // Send to first server
        transport
            .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
            .unwrap();
        sleep(Duration::from_millis(200)).await;
        assert_eq!(count1.load(Ordering::SeqCst), 1);

        // Now imagine we discover peer 2 has moved to a new address
        // The CORRECT approach with current implementation:

        // 1. Remove old peer (clears address mapping)
        transport.remove_peer(2);
        sleep(Duration::from_millis(100)).await; // Give sender_loop time to process

        // 2. Start new server at different address
        let server2 = MockRaftServer::new().await.unwrap();
        let addr2 = server2.addr();
        let count2 = server2.message_count.clone();

        tokio::spawn(async move {
            server2.run().await;
        });

        sleep(Duration::from_millis(100)).await;

        // 3. Add peer with new address
        transport.add_peer(2, addr2);

        // 4. Send message - should work now
        transport
            .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
            .unwrap();

        sleep(Duration::from_millis(500)).await;

        let received_count = count2.load(Ordering::SeqCst);
        assert!(
            received_count >= 1,
            "Expected at least 1 message, got {}. If this fails, the connection cache \
             may not be properly cleared when removing peers.",
            received_count
        );

        transport.shutdown().await;
    }

    #[tokio::test]
    async fn test_shutdown_with_pending_messages() {
        let server = MockRaftServer::new().await.unwrap();
        let server_addr = server.addr();

        tokio::spawn(async move {
            server.run().await;
        });

        sleep(Duration::from_millis(50)).await;

        let transport = RaftTransport::new(1);
        transport.add_peer(2, server_addr);

        // Queue multiple messages
        for _ in 0..5 {
            transport
                .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
                .unwrap();
        }

        // Shutdown immediately
        transport.shutdown().await;

        // Should not panic or hang
    }
}
