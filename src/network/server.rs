//! TCP server for handling incoming Raft and client messages.

use crate::error::{NetworkError, Result};
use crate::metrics::facade::{counter_add, counter_inc, gauge_dec, gauge_inc};
use crate::network::rpc::{decode_message, Message, PongResponse};
use crate::types::NodeId;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Handler for incoming messages.
pub trait MessageHandler: Send + Sync + 'static {
    /// Handle an incoming message and optionally return a response.
    fn handle(&self, msg: Message) -> Option<Message>;
}

/// TCP server for Raft communication.
pub struct NetworkServer {
    /// Address to bind to.
    bind_addr: SocketAddr,

    /// This node's ID.
    node_id: NodeId,

    /// Message handler.
    handler: Arc<dyn MessageHandler>,

    /// Shutdown signal receiver.
    shutdown_rx: mpsc::Receiver<()>,

    /// Cancellation token for graceful shutdown of connection handlers.
    cancellation_token: CancellationToken,

    /// Counter for active connections (used for graceful shutdown).
    active_connections: Arc<AtomicUsize>,
}

impl NetworkServer {
    /// Create a new network server.
    pub fn new(
        bind_addr: SocketAddr,
        node_id: NodeId,
        handler: Arc<dyn MessageHandler>,
    ) -> (Self, mpsc::Sender<()>) {
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let cancellation_token = CancellationToken::new();
        let active_connections = Arc::new(AtomicUsize::new(0));

        let server = Self {
            bind_addr,
            node_id,
            handler,
            shutdown_rx,
            cancellation_token,
            active_connections,
        };

        (server, shutdown_tx)
    }

    /// Run the server.
    pub async fn run(mut self) -> Result<()> {
        let listener = TcpListener::bind(self.bind_addr)
            .await
            .map_err(NetworkError::Io)?;

        info!(addr = %self.bind_addr, "Network server listening");

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            debug!(peer = %peer_addr, "Accepted connection");
                            let handler = self.handler.clone();
                            let node_id = self.node_id;
                            let cancel_token = self.cancellation_token.clone();
                            let active_conns = self.active_connections.clone();

                            // Increment active connection count
                            active_conns.fetch_add(1, Ordering::SeqCst);

                            // Record metrics
                            counter_inc!("crema_network_connections_total", "node_id" => node_id.to_string());
                            gauge_inc!("crema_network_connections_active", "node_id" => node_id.to_string());

                            tokio::spawn(async move {
                                let result = Self::handle_connection(
                                    stream,
                                    handler,
                                    node_id,
                                    cancel_token,
                                ).await;

                                // Decrement active connection count
                                active_conns.fetch_sub(1, Ordering::SeqCst);
                                gauge_dec!("crema_network_connections_active", "node_id" => node_id.to_string());

                                if let Err(e) = result {
                                    debug!(error = %e, "Connection handler error");
                                    counter_inc!("crema_network_connection_errors_total", "node_id" => node_id.to_string(), "error_type" => "handler_error");
                                }
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to accept connection");
                            counter_inc!("crema_network_connection_errors_total", "node_id" => self.node_id.to_string(), "error_type" => "accept_failed");
                        }
                    }
                }
                _ = self.shutdown_rx.recv() => {
                    info!("Network server shutting down, cancelling {} active connections",
                          self.active_connections.load(Ordering::SeqCst));

                    // Cancel all connection handlers
                    self.cancellation_token.cancel();

                    // Wait for active connections to finish (with timeout)
                    let shutdown_timeout = Duration::from_millis(500);
                    let start = std::time::Instant::now();

                    while self.active_connections.load(Ordering::SeqCst) > 0 {
                        if start.elapsed() > shutdown_timeout {
                            warn!(
                                "Shutdown timeout: {} connections still active, forcing close",
                                self.active_connections.load(Ordering::SeqCst)
                            );
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }

                    info!("Network server shutdown complete");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_connection(
        mut stream: TcpStream,
        handler: Arc<dyn MessageHandler>,
        node_id: NodeId,
        cancel_token: CancellationToken,
    ) -> Result<()> {
        loop {
            // Read message length (4 bytes), with cancellation check
            let mut len_buf = [0u8; 4];

            tokio::select! {
                biased;

                // Check for cancellation first (higher priority)
                _ = cancel_token.cancelled() => {
                    debug!(node_id, "Connection handler cancelled during read");
                    return Ok(());
                }

                // Read from stream
                result = stream.read_exact(&mut len_buf) => {
                    match result {
                        Ok(_) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                            // Connection closed
                            return Ok(());
                        }
                        Err(e) => return Err(NetworkError::Io(e).into()),
                    }
                }
            }

            // Check cancellation again before processing
            if cancel_token.is_cancelled() {
                debug!("Connection handler cancelled before message processing");
                return Ok(());
            }

            let len = u32::from_be_bytes(len_buf) as usize;
            // Maximum message size to prevent memory exhaustion attacks.
            // 16MB is large enough for batch operations but limits DoS impact.
            // For production, consider per-connection rate limiting.
            const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
            if len > MAX_MESSAGE_SIZE {
                tracing::warn!(
                    message_len = len,
                    max_size = MAX_MESSAGE_SIZE,
                    "Rejecting oversized message"
                );
                counter_inc!("crema_network_protocol_errors_total", "node_id" => node_id.to_string(), "error_type" => "message_too_large");
                return Err(NetworkError::ReceiveFailed(format!(
                    "message too large: {} bytes (max {})",
                    len, MAX_MESSAGE_SIZE
                ))
                .into());
            }

            // Read message data
            let mut data = vec![0u8; len];
            stream
                .read_exact(&mut data)
                .await
                .map_err(NetworkError::Io)?;

            // Record bytes received (length prefix + message body)
            counter_add!("crema_network_bytes_received_total", (4 + len) as u64, "node_id" => node_id.to_string());

            // Check cancellation before handling message
            if cancel_token.is_cancelled() {
                debug!("Connection handler cancelled before message handling");
                return Ok(());
            }

            // Decode message
            let msg = match decode_message(&data) {
                Ok(m) => m,
                Err(e) => {
                    counter_inc!("crema_network_decode_errors_total", "node_id" => node_id.to_string());
                    return Err(e.into());
                }
            };

            // Record request
            let msg_type = get_message_type(&msg);
            counter_inc!("crema_network_requests_total", "node_id" => node_id.to_string(), "type" => msg_type.to_string());

            // Handle message and get optional response
            if let Some(response) = handler.handle(msg) {
                // Send response
                let response_data = crate::network::rpc::encode_message(&response)?;
                let response_len = response_data.len() as u32;

                stream
                    .write_all(&response_len.to_be_bytes())
                    .await
                    .map_err(NetworkError::Io)?;

                stream
                    .write_all(&response_data)
                    .await
                    .map_err(NetworkError::Io)?;

                // Record bytes sent (length prefix + response body)
                counter_add!("crema_network_bytes_sent_total", (4 + response_data.len()) as u64, "node_id" => node_id.to_string());
            }
        }
    }
}

/// Get a string representation of the message type for metrics.
fn get_message_type(msg: &Message) -> &'static str {
    match msg {
        Message::Raft(_) => "raft",
        Message::ClientRequest(_) => "client_request",
        Message::ClientResponse(_) => "client_response",
        Message::Ping(_) => "ping",
        Message::Pong(_) => "pong",
        Message::ForwardedCommand(_) => "forwarded_command",
        Message::ForwardResponse(_) => "forward_response",
        Message::ShardForwardedCommand(_) => "shard_forward",
        Message::ShardForwardResponse(_) => "shard_forward_response",
        Message::ShardRaft(_) => "shard_raft",
        Message::MigrationFetchRequest(_) => "migration_fetch",
        Message::MigrationFetchResponse(_) => "migration_fetch_response",
        Message::MigrationApplyRequest(_) => "migration_apply",
        Message::MigrationApplyResponse(_) => "migration_apply_response",
        Message::MigrationShardStatsRequest(_) => "migration_stats",
        Message::MigrationShardStatsResponse(_) => "migration_stats_response",
        Message::MigrationProposalForward(_) => "migration_proposal",
        Message::MigrationProposalForwardResponse(_) => "migration_proposal_response",
        // Shard coordination messages
        Message::ShardCreationBroadcast(_) => "shard_creation_broadcast",
        Message::ShardCreationAck(_) => "shard_creation_ack",
        Message::ShardRemovalBroadcast(_) => "shard_removal_broadcast",
        Message::ShardRemovalAck(_) => "shard_removal_ack",
        Message::GetTopology(_) => "get_topology",
        Message::TopologyResponse(_) => "topology_response",
        Message::GetShardInfo(_) => "get_shard_info",
        Message::ShardInfoResponse(_) => "shard_info_response",
        // Slot migration scan messages
        Message::SlotScanRequest(_) => "slot_scan_request",
        Message::SlotScanResponse(_) => "slot_scan_response",
    }
}

/// Simple ping handler for testing.
pub struct PingHandler {
    node_id: NodeId,
    raft_addr: String,
}

impl PingHandler {
    pub fn new(node_id: NodeId, raft_addr: String) -> Self {
        Self { node_id, raft_addr }
    }
}

impl MessageHandler for PingHandler {
    fn handle(&self, msg: Message) -> Option<Message> {
        match msg {
            Message::Ping(_) => Some(Message::Pong(PongResponse {
                node_id: self.node_id,
                raft_addr: self.raft_addr.clone(),
                leader_id: None,
            })),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EchoHandler;

    impl MessageHandler for EchoHandler {
        fn handle(&self, msg: Message) -> Option<Message> {
            Some(msg)
        }
    }

    #[tokio::test]
    async fn test_server_accepts_connections() {
        let handler = Arc::new(EchoHandler);
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (_server, _shutdown_tx) = NetworkServer::new(addr, 1, handler);

        // Get the actual bound address
        let listener = TcpListener::bind(addr).await.unwrap();
        let actual_addr = listener.local_addr().unwrap();
        drop(listener);

        // Start server in background
        let (server, shutdown_tx) = NetworkServer::new(actual_addr, 1, Arc::new(EchoHandler));

        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        // Give server time to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Try to connect
        let result = TcpStream::connect(actual_addr).await;
        assert!(result.is_ok());

        // Shutdown
        let _ = shutdown_tx.send(()).await;
        let _ = server_handle.await;
    }
}
