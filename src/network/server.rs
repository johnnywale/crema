//! TCP server for handling incoming Raft and client messages.

use crate::error::{NetworkError, Result};
use crate::network::rpc::{decode_message, Message, PongResponse};
use crate::types::NodeId;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info};

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
}

impl NetworkServer {
    /// Create a new network server.
    pub fn new(
        bind_addr: SocketAddr,
        node_id: NodeId,
        handler: Arc<dyn MessageHandler>,
    ) -> (Self, mpsc::Sender<()>) {
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let server = Self {
            bind_addr,
            node_id,
            handler,
            shutdown_rx,
        };

        (server, shutdown_tx)
    }

    /// Run the server.
    pub async fn run(mut self) -> Result<()> {
        let listener = TcpListener::bind(self.bind_addr)
            .await
            .map_err(|e| NetworkError::Io(e))?;

        info!(addr = %self.bind_addr, "Network server listening");

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            debug!(peer = %peer_addr, "Accepted connection");
                            let handler = self.handler.clone();
                            let node_id = self.node_id;
                            tokio::spawn(async move {
                                if let Err(e) = Self::handle_connection(stream, handler, node_id).await {
                                    debug!(error = %e, "Connection handler error");
                                }
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to accept connection");
                        }
                    }
                }
                _ = self.shutdown_rx.recv() => {
                    info!("Network server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_connection(
        mut stream: TcpStream,
        handler: Arc<dyn MessageHandler>,
        _node_id: NodeId,
    ) -> Result<()> {
        loop {
            // Read message length (4 bytes)
            let mut len_buf = [0u8; 4];
            match stream.read_exact(&mut len_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Connection closed
                    return Ok(());
                }
                Err(e) => return Err(NetworkError::Io(e).into()),
            }

            let len = u32::from_be_bytes(len_buf) as usize;
            if len > 16 * 1024 * 1024 {
                // 16MB max message size
                return Err(NetworkError::ReceiveFailed("message too large".to_string()).into());
            }

            // Read message data
            let mut data = vec![0u8; len];
            stream
                .read_exact(&mut data)
                .await
                .map_err(|e| NetworkError::Io(e))?;

            // Decode message
            let msg = decode_message(&data)?;

            // Handle message and get optional response
            if let Some(response) = handler.handle(msg) {
                // Send response
                let response_data = crate::network::rpc::encode_message(&response)?;
                let response_len = response_data.len() as u32;

                stream
                    .write_all(&response_len.to_be_bytes())
                    .await
                    .map_err(|e| NetworkError::Io(e))?;

                stream
                    .write_all(&response_data)
                    .await
                    .map_err(|e| NetworkError::Io(e))?;
            }
        }
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
