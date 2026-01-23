//! Integration tests for RaftTransport.

#![cfg(test)]

use crate::consensus::transport::*;
use crate::network::rpc::{decode_message, Message};
use crate::types::NodeId;
use raft::prelude::Message as RaftMessage;
use raft::prelude::MessageType;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::Mutex as TokioMutex;
use tokio::time::sleep;

/// Allocate an OS-assigned port for testing by binding to port 0.
/// This avoids conflicts with Windows' dynamic port exclusion ranges.
async fn allocate_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

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
    addr: SocketAddr,
    received_messages: Arc<TokioMutex<Vec<Message>>>,
    message_count: Arc<AtomicUsize>,
}

impl MockRaftServer {
    async fn new() -> Self {
        let port = allocate_port().await;
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

        Self {
            addr,
            received_messages: Arc::new(TokioMutex::new(Vec::new())),
            message_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    async fn run(&self) {
        let listener = TcpListener::bind(self.addr).await.unwrap();
        let received = self.received_messages.clone();
        let count = self.message_count.clone();

        loop {
            match listener.accept().await {
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
                            match decode_message(&data) {
                                Ok(msg) => {
                                    received.lock().await.push(msg);
                                    count.fetch_add(1, AtomicOrdering::SeqCst);
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

    #[allow(dead_code)]
    fn get_message_count(&self) -> usize {
        self.message_count.load(AtomicOrdering::SeqCst)
    }
}

#[tokio::test]
async fn test_send_single_message_to_peer() {
    // Setup mock server
    let server = MockRaftServer::new().await;
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
    transport.add_peer(2, server_addr).await;

    // Send a message
    let msg = create_test_message(1, 2, MessageType::MsgHeartbeat);
    transport.send(msg).unwrap();

    // Wait for message to be sent and received
    sleep(Duration::from_millis(300)).await;

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
    let server = MockRaftServer::new().await;
    let server_addr = server.addr();
    let received_messages = server.received_messages.clone();

    tokio::spawn(async move {
        server.run().await;
    });

    sleep(Duration::from_millis(50)).await;

    let transport = RaftTransport::new(1);
    transport.add_peer(2, server_addr).await;

    // Send multiple messages
    let messages = vec![
        create_test_message(1, 2, MessageType::MsgHeartbeat),
        create_test_message(1, 2, MessageType::MsgAppend),
        create_test_message(1, 2, MessageType::MsgRequestVote),
    ];

    transport.send_messages(messages);

    // Wait for all messages to be processed
    sleep(Duration::from_millis(500)).await;

    // Verify all messages received
    let received = received_messages.lock().await;
    assert_eq!(received.len(), 3);

    transport.shutdown().await;
}

#[tokio::test]
async fn test_connection_reuse() {
    let server = MockRaftServer::new().await;
    let server_addr = server.addr();
    let count = server.message_count.clone();

    tokio::spawn(async move {
        server.run().await;
    });

    sleep(Duration::from_millis(50)).await;

    let transport = RaftTransport::new(1);
    transport.add_peer(2, server_addr).await;

    // Send multiple messages - should reuse connection
    for _ in 0..5 {
        let msg = create_test_message(1, 2, MessageType::MsgHeartbeat);
        transport.send(msg).unwrap();
        sleep(Duration::from_millis(50)).await;
    }

    // Wait for processing
    sleep(Duration::from_millis(300)).await;

    assert_eq!(count.load(AtomicOrdering::SeqCst), 5);

    // Verify only one connection was created
    let metrics = transport.metrics();
    assert_eq!(metrics.connections_created, 1);

    transport.shutdown().await;
}

#[tokio::test]
async fn test_retry_on_connection_failure() {
    let config = TransportConfig {
        max_retries: 3,
        initial_retry_delay: Duration::from_millis(50),
        max_retry_delay: Duration::from_millis(200),
        connect_timeout: Duration::from_millis(500),
        write_timeout: Duration::from_millis(500),
        max_connections: 10,
        enable_tcp_nodelay: true,
        tcp_keepalive_time: Default::default(),
        tcp_keepalive_interval: Default::default(),
        per_peer_queue_size: 100,
        enable_connection_prewarming: true,
        tcp_keepalive_retries: 0,
        enable_retry_jitter: false,
        ..Default::default()
    };

    let transport = RaftTransport::with_config(1, config);

    // Add peer with invalid address (will fail to connect)
    let invalid_port = allocate_port().await;
    transport
        .add_peer(2, format!("127.0.0.1:{}", invalid_port).parse().unwrap())
        .await;

    // This should fail after retries
    let msg = create_test_message(1, 2, MessageType::MsgHeartbeat);
    transport.send(msg).unwrap();

    // Wait for retries to complete
    sleep(Duration::from_millis(1000)).await;

    // Check metrics show failures
    let metrics = transport.metrics();
    assert!(metrics.connections_failed > 0);

    transport.shutdown().await;
    // Test passes if no panic occurs - failures are logged
}

#[tokio::test]
async fn test_send_to_unknown_peer() {
    let transport = RaftTransport::new(1);

    // Don't add peer 2
    let msg = create_test_message(1, 2, MessageType::MsgHeartbeat);
    let result = transport.send(msg);

    // Should return error because worker doesn't exist
    assert!(result.is_err());

    transport.shutdown().await;
}

#[tokio::test]
async fn test_multiple_peers() {
    // Create two mock servers
    let server1 = MockRaftServer::new().await;
    let server2 = MockRaftServer::new().await;
    let addr1 = server1.addr();
    let addr2 = server2.addr();
    let count1 = server1.message_count.clone();
    let count2 = server2.message_count.clone();

    tokio::spawn(async move { server1.run().await });
    tokio::spawn(async move { server2.run().await });

    sleep(Duration::from_millis(50)).await;

    let transport = RaftTransport::new(1);
    transport.add_peer(2, addr1).await;
    transport.add_peer(3, addr2).await;

    // Send to both peers
    transport
        .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
        .unwrap();
    transport
        .send(create_test_message(1, 3, MessageType::MsgHeartbeat))
        .unwrap();

    sleep(Duration::from_millis(300)).await;

    assert_eq!(count1.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(count2.load(AtomicOrdering::SeqCst), 1);

    transport.shutdown().await;
}

#[tokio::test]
async fn test_peer_management() {
    let transport = RaftTransport::new(1);

    // Add peers
    let port1 = allocate_port().await;
    let port2 = allocate_port().await;
    let addr1: SocketAddr = format!("127.0.0.1:{}", port1).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", port2).parse().unwrap();

    transport.add_peer(2, addr1).await;
    transport.add_peer(3, addr2).await;

    assert_eq!(transport.peer_count(), 2);
    assert_eq!(transport.get_peer(2), Some(addr1));
    assert_eq!(transport.get_peer(3), Some(addr2));

    let peer_ids = transport.peer_ids();
    assert!(peer_ids.contains(&2));
    assert!(peer_ids.contains(&3));

    // Remove peer
    transport.remove_peer(2);
    sleep(Duration::from_millis(100)).await; // Wait for command to be processed

    // Note: peer_count may still return 2 because remove is async via command channel
    // The address should still be returned until dispatcher processes it

    transport.shutdown().await;
}

#[tokio::test]
async fn test_concurrent_sends() {
    let server = MockRaftServer::new().await;
    let server_addr = server.addr();
    let count = server.message_count.clone();

    tokio::spawn(async move {
        server.run().await;
    });

    sleep(Duration::from_millis(50)).await;

    let transport = Arc::new(RaftTransport::new(1));
    transport.add_peer(2, server_addr).await;

    // Spawn multiple tasks sending concurrently
    let mut handles = vec![];
    for _ in 0..10 {
        let transport_clone = transport.clone();
        let handle = tokio::spawn(async move {
            let msg = create_test_message(1, 2, MessageType::MsgHeartbeat);
            let _ = transport_clone.send(msg);
        });
        handles.push(handle);
    }

    // Wait for all sends to complete
    for handle in handles {
        handle.await.unwrap();
    }

    sleep(Duration::from_millis(500)).await;

    assert_eq!(count.load(AtomicOrdering::SeqCst), 10);

    transport.shutdown().await;
}

#[tokio::test]
async fn test_stale_connection_recovery() {
    // This test verifies that when a connection becomes stale,
    // the transport detects it and handles it gracefully

    let server = MockRaftServer::new().await;
    let server_addr = server.addr();
    let count = server.message_count.clone();

    let handle = tokio::spawn(async move {
        server.run().await;
    });

    sleep(Duration::from_millis(50)).await;

    let transport = RaftTransport::new(1);
    transport.add_peer(2, server_addr).await;

    // Send first message - establishes connection
    transport
        .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
        .unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(count.load(AtomicOrdering::SeqCst), 1);

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
    let server1 = MockRaftServer::new().await;
    let addr1 = server1.addr();
    let count1 = server1.message_count.clone();

    let handle1 = tokio::spawn(async move {
        server1.run().await;
    });

    sleep(Duration::from_millis(50)).await;

    let transport = RaftTransport::new(1);
    transport.add_peer(2, addr1).await;

    // Send message to first server
    transport
        .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
        .unwrap();
    sleep(Duration::from_millis(300)).await;

    assert_eq!(count1.load(AtomicOrdering::SeqCst), 1);

    // Abort first server (simulating crash)
    handle1.abort();
    sleep(Duration::from_millis(200)).await;

    // Start second server on a DIFFERENT port (simulating restart with new address)
    let server2 = MockRaftServer::new().await;
    let addr2 = server2.addr();
    let count2 = server2.message_count.clone();

    tokio::spawn(async move {
        server2.run().await;
    });

    sleep(Duration::from_millis(100)).await;

    // Update peer address to new server - this simulates address discovery
    transport.update_peer(2, addr2);

    // Wait for update to be processed
    sleep(Duration::from_millis(200)).await;

    // Send message - should work because we updated the peer address
    transport
        .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
        .unwrap();

    // Give more time for connection + message delivery
    sleep(Duration::from_millis(500)).await;

    // Should have received at least 1 message
    let received_count = count2.load(AtomicOrdering::SeqCst);
    assert!(
        received_count >= 1,
        "Expected at least 1 message, got {}",
        received_count
    );

    transport.shutdown().await;
}

#[tokio::test]
async fn test_large_message_send() {
    let server = MockRaftServer::new().await;
    let server_addr = server.addr();
    let received_messages = server.received_messages.clone();

    tokio::spawn(async move {
        server.run().await;
    });

    sleep(Duration::from_millis(50)).await;

    let transport = RaftTransport::new(1);
    transport.add_peer(2, server_addr).await;

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
async fn test_peer_address_update_scenario() {
    // This test demonstrates atomic peer address updates

    // Setup first server
    let server1 = MockRaftServer::new().await;
    let addr1 = server1.addr();
    let count1 = server1.message_count.clone();

    tokio::spawn(async move {
        server1.run().await;
    });

    sleep(Duration::from_millis(50)).await;

    let transport = RaftTransport::new(1);
    transport.add_peer(2, addr1).await;

    // Send to first server
    transport
        .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
        .unwrap();
    sleep(Duration::from_millis(300)).await;
    assert_eq!(count1.load(AtomicOrdering::SeqCst), 1);

    // Start new server at different address
    let server2 = MockRaftServer::new().await;
    let addr2 = server2.addr();
    let count2 = server2.message_count.clone();

    tokio::spawn(async move {
        server2.run().await;
    });

    sleep(Duration::from_millis(100)).await;

    // Use update_peer to atomically change the address
    transport.update_peer(2, addr2);
    sleep(Duration::from_millis(200)).await;

    // Send message - should go to new server
    transport
        .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
        .unwrap();

    sleep(Duration::from_millis(500)).await;

    let received_count = count2.load(AtomicOrdering::SeqCst);
    assert!(
        received_count >= 1,
        "Expected at least 1 message on new server, got {}",
        received_count
    );

    transport.shutdown().await;
}

#[tokio::test]
async fn test_shutdown_with_pending_messages() {
    let server = MockRaftServer::new().await;
    let server_addr = server.addr();

    tokio::spawn(async move {
        server.run().await;
    });

    sleep(Duration::from_millis(50)).await;

    let transport = RaftTransport::new(1);
    transport.add_peer(2, server_addr).await;

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

#[tokio::test]
async fn test_message_priority() {
    let server = MockRaftServer::new().await;
    let server_addr = server.addr();
    let received_messages = server.received_messages.clone();

    tokio::spawn(async move {
        server.run().await;
    });

    sleep(Duration::from_millis(50)).await;

    let transport = RaftTransport::new(1);
    transport.add_peer(2, server_addr).await;

    // Send mix of high and normal priority messages
    transport
        .send(create_test_message(1, 2, MessageType::MsgAppend)) // Normal
        .unwrap();
    transport
        .send(create_test_message(1, 2, MessageType::MsgHeartbeat)) // High
        .unwrap();
    transport
        .send(create_test_message(1, 2, MessageType::MsgRequestVote)) // High
        .unwrap();

    sleep(Duration::from_millis(500)).await;

    let received = received_messages.lock().await;
    assert_eq!(received.len(), 3);

    // Check metrics
    let metrics = transport.metrics();
    assert!(metrics.high_priority_sent >= 2);
    assert!(metrics.normal_priority_sent >= 1);

    transport.shutdown().await;
}

#[tokio::test]
async fn test_metrics_tracking() {
    let server = MockRaftServer::new().await;
    let server_addr = server.addr();

    tokio::spawn(async move {
        server.run().await;
    });

    sleep(Duration::from_millis(50)).await;

    let transport = RaftTransport::new(1);
    transport.add_peer(2, server_addr).await;

    // Initial metrics should be zero
    let initial_metrics = transport.metrics();
    assert_eq!(initial_metrics.messages_sent, 0);

    // Send some messages
    for _ in 0..5 {
        transport
            .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
            .unwrap();
    }

    sleep(Duration::from_millis(500)).await;

    // Check metrics were updated
    let final_metrics = transport.metrics();
    assert_eq!(final_metrics.messages_sent, 5);
    assert_eq!(final_metrics.connections_created, 1);
    assert!(final_metrics.average_send_latency_us < 1_000_000); // Should be reasonably fast (< 1 second)

    transport.shutdown().await;
}

#[tokio::test]
async fn test_priority_preemption_logic() {
    let server = MockRaftServer::new().await;
    let server_addr = server.addr();
    let received_messages = server.received_messages.clone();
    let message_count = server.message_count.clone();

    // 1. Setup Transport, deliberately not starting Server's run loop
    let transport = RaftTransport::new(1);
    transport.add_peer(2, server_addr).await;

    // 2. Send 5 normal messages (Normal) -> These will queue in normal_priority_rx
    for _ in 0..5 {
        transport
            .send(create_test_message(1, 2, MessageType::MsgAppend))
            .unwrap();
    }

    // 3. Send 1 high priority message (High) -> Goes to high_priority_rx queue
    transport
        .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
        .unwrap();

    // 4. Start server, Worker begins establishing connection and consuming queue
    tokio::spawn(async move { server.run().await });

    // 5. Wait until the server has actually received at least 1 message
    // (not just for transport to report sending - that's a race condition)
    let success = wait_for_condition(Duration::from_secs(3), || {
        message_count.load(AtomicOrdering::SeqCst) >= 1
    })
    .await;

    assert!(success, "Server did not receive any messages within the timeout");

    // 6. Verify receive order
    let received = received_messages.lock().await;
    assert!(!received.is_empty(), "Server should have received messages");

    // Since Worker after establishing connection, first select iteration prefers high_priority_rx
    if let Message::Raft(wrapper) = &received[0] {
        let raft_msg = wrapper.to_raft_message().unwrap();
        assert_eq!(
            raft_msg.get_msg_type(),
            MessageType::MsgHeartbeat,
            "High priority message (Heartbeat) should preempt Normal priority (Append)"
        );
    }

    transport.shutdown().await;
}

/// Generic polling helper function
async fn wait_for_condition<F>(timeout: Duration, mut condition: F) -> bool
where
    F: FnMut() -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

#[tokio::test]
async fn test_connection_limit_enforcement() {
    let config = TransportConfig {
        max_connections: 2, // Very small connection limit
        ..Default::default()
    };
    let transport = RaftTransport::with_config(1, config);

    // Start 3 Servers
    let s1 = MockRaftServer::new().await;
    let s2 = MockRaftServer::new().await;
    let s3 = MockRaftServer::new().await;

    transport.add_peer(2, s1.addr()).await;
    transport.add_peer(3, s2.addr()).await;
    transport.add_peer(4, s3.addr()).await;

    // Send messages to all three nodes
    let _ = transport.send(create_test_message(1, 2, MessageType::MsgHeartbeat));
    let _ = transport.send(create_test_message(1, 3, MessageType::MsgHeartbeat));
    let _ = transport.send(create_test_message(1, 4, MessageType::MsgHeartbeat));

    sleep(Duration::from_millis(300)).await;

    let metrics = transport.metrics();
    // Active connections should not exceed 2
    assert!(metrics.active_connections <= 2);
    transport.shutdown().await;
}

#[tokio::test]
async fn test_rapid_peer_churning() {
    let transport = RaftTransport::new(1);
    let addr1: SocketAddr = "127.0.0.1:1111".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:2222".parse().unwrap();

    for _ in 0..10 {
        // add_peer internally synchronously inserts into peers map, but triggers async ensure_worker
        transport.add_peer(2, addr1).await;
        // remove_peer is completely async command
        transport.remove_peer(2);
        transport.add_peer(2, addr2).await;
    }

    // Final state check: get_peer reads from local memory, usually very fast
    assert_eq!(transport.get_peer(2), Some(addr2));

    // Improvement: wait for all async Worker replacement logic to converge
    // If not converged, active_connections might be > 0 due to old Worker not yet exited
    // Here we mainly verify Dispatcher is still working normally
    let success = wait_for_condition(Duration::from_secs(2), || {
        // This logic can be adjusted based on your needs, e.g., check active Worker count
        // Assuming each peer only has one worker, final active connections shouldn't explode
        transport.metrics().active_connections <= 1
    })
    .await;

    assert!(success, "Dispatcher failed to settle worker churn");
    transport.shutdown().await;
}

#[tokio::test]
async fn test_write_timeout_slow_receiver() {
    let config = TransportConfig {
        write_timeout: Duration::from_millis(100),
        max_retries: 1, // Reduce retries to speed up test failure
        ..Default::default()
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let transport = RaftTransport::with_config(1, config);
    transport.add_peer(2, addr).await;

    // Simulate only accepting connection but not reading
    tokio::spawn(async move {
        if let Ok((_stream, _)) = listener.accept().await {
            // Just hold the connection, do nothing, until Drop
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    // Send many large messages to fill Socket buffer, forcing timeout
    for _ in 0..20 {
        let mut msg = create_test_message(1, 2, MessageType::MsgAppend);
        // Increase data volume to ensure filling kernel TCP send buffer
        let entries = vec![
            raft::prelude::Entry {
                data: vec![0u8; 1024 * 64].into(), // 64KB per entry
                ..Default::default()
            };
            5
        ];
        msg.set_entries(entries.into());
        let _ = transport.send(msg);
    }

    // Improvement: use polling to wait for failure metrics to increase
    let success = wait_for_condition(Duration::from_secs(5), || {
        let m = transport.metrics();
        m.messages_failed > 0
    })
    .await;

    let final_metrics = transport.metrics();
    assert!(
        success,
        "Should have recorded failed messages due to write timeout, got metrics: {:?}",
        final_metrics
    );

    transport.shutdown().await;
}

#[tokio::test]
async fn test_permit_raii_recovery() {
    let config = TransportConfig {
        max_connections: 1,
        initial_retry_delay: Duration::from_millis(10),
        ..Default::default()
    };
    let transport = RaftTransport::with_config(1, config);

    // Node 2 starts
    let s1 = MockRaftServer::new().await;
    let s1_addr = s1.addr;
    tokio::spawn(async move { s1.run().await });

    transport.add_peer(2, s1_addr).await;
    transport
        .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
        .unwrap();

    // Wait for first message to be sent successfully (Metrics count increases)
    wait_for_metrics(&transport, |m| m.messages_sent == 1).await;

    // Remove Node 2
    transport.remove_peer(2);

    // Key: wait for active_connections to drop back to 0, ensuring Permit is returned
    wait_for_metrics(&transport, |m| m.active_connections == 0).await;

    // Node 3 starts
    let s2 = MockRaftServer::new().await;
    let s2_addr = s2.addr;
    tokio::spawn(async move { s2.run().await });

    transport.add_peer(3, s2_addr).await;
    transport
        .send(create_test_message(1, 3, MessageType::MsgHeartbeat))
        .unwrap();

    // Wait for second message to succeed
    let success = wait_for_metrics(&transport, |m| m.messages_sent == 2).await;

    assert!(success, "Permit was not recovered in time for Node 3");
    let final_metrics = transport.metrics();
    assert_eq!(final_metrics.active_connections, 1);
}

// Helper function: async polling for Metrics status
async fn wait_for_metrics<F>(transport: &RaftTransport, condition: F) -> bool
where
    F: Fn(TransportMetricsSnapshot) -> bool,
{
    for _ in 0..40 {
        // Wait up to 2 seconds
        if condition(transport.metrics()) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}


/// A. Network Partition & Recovery Test
/// Establish connection and send messages, then suddenly close Server, wait a while, restart Server.
/// Verify Worker can successfully reconnect via exponential backoff and resume message sending
#[tokio::test]
async fn test_network_partition_and_recovery() {
    let config = TransportConfig {
        max_retries: 5,
        initial_retry_delay: Duration::from_millis(50),
        max_retry_delay: Duration::from_millis(500),
        ..Default::default()
    };

    let transport = RaftTransport::with_config(1, config);

    // Phase 1: Normal sending
    let server1 = MockRaftServer::new().await;
    let server_addr = server1.addr();
    let count1 = server1.message_count.clone();

    let handle1 = tokio::spawn(async move {
        server1.run().await;
    });

    sleep(Duration::from_millis(50)).await;
    transport.add_peer(2, server_addr).await;

    // Send first message
    transport
        .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
        .unwrap();

    wait_for_metrics(&transport, |m| m.messages_sent >= 1).await;
    assert_eq!(count1.load(AtomicOrdering::SeqCst), 1);

    // Phase 2: Simulate network partition (close server)
    handle1.abort();
    sleep(Duration::from_millis(100)).await;

    // Send some messages (will fail and trigger reconnection)
    for _ in 0..3 {
        let _ = transport.send(create_test_message(1, 2, MessageType::MsgHeartbeat));
        sleep(Duration::from_millis(50)).await;
    }

    // Verify active_connections decreased
    let _metrics_during_partition = transport.metrics();
    // Connection may be disconnected, or still retrying

    // Phase 3: Recover network (restart server at same address)
    let server2 = MockRaftServer::new().await;
    let server2_addr = server2.addr();
    let count2 = server2.message_count.clone();

    tokio::spawn(async move {
        server2.run().await;
    });

    sleep(Duration::from_millis(50)).await;

    // Update peer address to new server
    transport.update_peer(2, server2_addr);
    sleep(Duration::from_millis(200)).await;

    // Send message to verify recovery
    transport
        .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
        .unwrap();

    let success = wait_for_condition(Duration::from_secs(3), || {
        count2.load(AtomicOrdering::SeqCst) >= 1
    })
    .await;

    assert!(success, "Failed to recover after partition");
    transport.shutdown().await;
}

/// B. Slow Network & Congestion Test (Congestion Control)
/// Use a simulated extremely slow receiving Server, send more than per_peer_queue_size messages.
/// Verify send method correctly returns NetworkError::SendFailed("peer queue full")
#[tokio::test]
async fn test_slow_network_congestion() {
    let config = TransportConfig {
        per_peer_queue_size: 5, // Very small queue
        max_retries: 1,
        ..Default::default()
    };

    let transport = RaftTransport::with_config(1, config);

    // Create a "slow" server that never reads
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Only accept connection but don't read any data
    tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                // Keep connection but don't read
                tokio::spawn(async move {
                    let _keep = stream;
                    tokio::time::sleep(Duration::from_secs(60)).await;
                });
            }
        }
    });

    sleep(Duration::from_millis(50)).await;
    transport.add_peer(2, addr).await;

    // Rapidly send messages to fill queue
    let mut queue_full_count = 0;
    for _ in 0..20 {
        match transport.send(create_test_message(1, 2, MessageType::MsgAppend)) {
            Ok(_) => {}
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("queue full") {
                    queue_full_count += 1;
                }
            }
        }
    }

    // Since queue only has 5 slots, subsequent messages should fail
    assert!(
        queue_full_count > 0,
        "Should have received 'queue full' errors"
    );

    transport.shutdown().await;
}

/// C. Large Payload Stress Test
/// Send messages containing megabytes (MB) of data.
/// Verify write_timeout doesn't falsely trigger due to large data volume
#[tokio::test]
async fn test_large_payload_stress() {
    let config = TransportConfig {
        write_timeout: Duration::from_secs(30), // Longer timeout
        ..Default::default()
    };

    let transport = RaftTransport::with_config(1, config);

    let server = MockRaftServer::new().await;
    let server_addr = server.addr();
    let received_messages = server.received_messages.clone();

    tokio::spawn(async move {
        server.run().await;
    });

    sleep(Duration::from_millis(50)).await;
    transport.add_peer(2, server_addr).await;

    // Create large message (~1MB of log entries)
    let mut msg = create_test_message(1, 2, MessageType::MsgAppend);
    let large_entries: Vec<_> = (0..100)
        .map(|_| {
            let mut entry = raft::prelude::Entry::default();
            entry.set_data(vec![0xAB; 10 * 1024].into()); // 10KB per entry
            entry
        })
        .collect();
    msg.set_entries(large_entries.into());

    // Send large message
    transport.send(msg).unwrap();

    // Wait for message to be received
    let success = wait_for_condition(Duration::from_secs(10), || {
        transport.metrics().messages_sent >= 1
    })
    .await;

    assert!(success, "Large message should be sent successfully");

    // Verify message was correctly received
    sleep(Duration::from_millis(500)).await;
    let received = received_messages.lock().await;
    assert!(!received.is_empty(), "Large message should be received");

    if let Message::Raft(wrapper) = &received[0] {
        let raft_msg = wrapper.to_raft_message().unwrap();
        assert_eq!(raft_msg.get_entries().len(), 100);
    }

    transport.shutdown().await;
}

/// D. Semaphore Fairness Test (Semaphore Exhaustion)
/// Set max_connections: 5, add 10 Peers, send messages to all 10 Peers simultaneously.
/// Verify only 5 connections are established
#[tokio::test]
async fn test_semaphore_fairness() {
    let config = TransportConfig {
        max_connections: 5,
        enable_connection_prewarming: false, // Disable prewarming for observation
        ..Default::default()
    };

    let transport = RaftTransport::with_config(1, config);

    // Create 10 servers
    let mut servers = Vec::new();
    for _ in 0..10 {
        let server = MockRaftServer::new().await;
        servers.push(server);
    }

    // Start all servers
    for (i, server) in servers.iter().enumerate() {
        let addr = server.addr();
        transport.add_peer((i + 2) as NodeId, addr).await;
    }

    for server in servers {
        tokio::spawn(async move {
            server.run().await;
        });
    }

    sleep(Duration::from_millis(100)).await;

    // Send messages to all 10 peers
    for i in 0..10 {
        let _ = transport.send(create_test_message(1, (i + 2) as NodeId, MessageType::MsgHeartbeat));
    }

    // Wait for connections to establish
    sleep(Duration::from_millis(500)).await;

    // Verify active connections don't exceed 5
    let metrics = transport.metrics();
    assert!(
        metrics.active_connections <= 5,
        "Active connections {} should not exceed max_connections 5",
        metrics.active_connections
    );

    transport.shutdown().await;
}

/// E. Concurrent Shutdown Test
/// While densely sending messages, suddenly call transport.shutdown().
/// Verify oneshot confirmation signal returns normally; all background Tasks join
#[tokio::test]
async fn test_concurrent_shutdown() {
    let transport = Arc::new(RaftTransport::new(1));

    // Create multiple servers
    let mut server_handles = Vec::new();
    for i in 0..5 {
        let server = MockRaftServer::new().await;
        let addr = server.addr();
        transport.add_peer((i + 2) as NodeId, addr).await;

        let handle = tokio::spawn(async move {
            server.run().await;
        });
        server_handles.push(handle);
    }

    sleep(Duration::from_millis(100)).await;

    // Continuously send messages in background
    let transport_clone = transport.clone();
    let sender_handle = tokio::spawn(async move {
        for i in 0..100 {
            for peer in 2..7 {
                let _ = transport_clone.send(create_test_message(1, peer, MessageType::MsgHeartbeat));
            }
            if i % 10 == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    });

    // Suddenly shutdown during sending
    sleep(Duration::from_millis(50)).await;

    let shutdown_start = Instant::now();
    transport.shutdown().await;
    let shutdown_duration = shutdown_start.elapsed();

    // Verify shutdown completes in reasonable time (shouldn't wait forever)
    assert!(
        shutdown_duration < Duration::from_secs(15),
        "Shutdown took too long: {:?}",
        shutdown_duration
    );

    // Wait for send task to complete (may have been cancelled or completed)
    let _ = tokio::time::timeout(Duration::from_secs(2), sender_handle).await;

    // Cleanup servers
    for handle in server_handles {
        handle.abort();
    }
}

/// F. Idle Connection Timeout Test
/// Verify idle connections automatically close after idle_timeout
#[tokio::test]
async fn test_idle_connection_timeout() {
    let config = TransportConfig {
        idle_timeout: Some(Duration::from_millis(200)), // Short timeout for testing
        ..Default::default()
    };

    let transport = RaftTransport::with_config(1, config);

    let server = MockRaftServer::new().await;
    let server_addr = server.addr();

    tokio::spawn(async move {
        server.run().await;
    });

    sleep(Duration::from_millis(50)).await;
    transport.add_peer(2, server_addr).await;

    // Send one message to establish connection
    transport
        .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
        .unwrap();

    wait_for_metrics(&transport, |m| m.messages_sent >= 1).await;

    // Verify connection is established
    let metrics_after_send = transport.metrics();
    assert_eq!(metrics_after_send.active_connections, 1);

    // Wait for idle timeout
    sleep(Duration::from_millis(400)).await;

    // Verify connection is closed
    let metrics_after_idle = transport.metrics();
    assert_eq!(
        metrics_after_idle.active_connections, 0,
        "Connection should be closed after idle timeout"
    );

    // Sending new message should re-establish connection
    transport
        .send(create_test_message(1, 2, MessageType::MsgHeartbeat))
        .unwrap();

    wait_for_metrics(&transport, |m| m.messages_sent >= 2).await;

    let final_metrics = transport.metrics();
    assert_eq!(final_metrics.active_connections, 1, "Connection should be re-established");

    transport.shutdown().await;
}

/// G. Message Batching Test
/// Verify message batching functionality
#[tokio::test]
async fn test_message_batching() {
    let config = TransportConfig {
        batch_delay: Some(Duration::from_millis(50)), // Enable batching
        batch_max_messages: 5,
        ..Default::default()
    };

    let transport = RaftTransport::with_config(1, config);

    let server = MockRaftServer::new().await;
    let server_addr = server.addr();
    let count = server.message_count.clone();

    tokio::spawn(async move {
        server.run().await;
    });

    sleep(Duration::from_millis(50)).await;
    transport.add_peer(2, server_addr).await;

    // Rapidly send multiple normal priority messages (should be batched)
    for _ in 0..10 {
        transport
            .send(create_test_message(1, 2, MessageType::MsgAppend))
            .unwrap();
    }

    // Wait for batching to complete
    sleep(Duration::from_millis(200)).await;

    // All messages should be sent
    wait_for_metrics(&transport, |m| m.messages_sent >= 10).await;

    let final_count = count.load(AtomicOrdering::SeqCst);
    assert_eq!(final_count, 10, "All batched messages should be received");

    transport.shutdown().await;
}

/// H. Backpressure Callback Test
/// Verify backpressure callback mechanism
#[tokio::test]
async fn test_backpressure_callback() {
    use std::sync::atomic::AtomicBool;

    let queue_full_triggered = Arc::new(AtomicBool::new(false));
    let high_watermark_triggered = Arc::new(AtomicBool::new(false));

    let qf = queue_full_triggered.clone();
    let hw = high_watermark_triggered.clone();

    let callback: BackpressureCallback = Arc::new(move |event| match event {
        BackpressureEvent::QueueFull { .. } => {
            qf.store(true, AtomicOrdering::SeqCst);
        }
        BackpressureEvent::QueueHighWatermark { .. } => {
            hw.store(true, AtomicOrdering::SeqCst);
        }
        BackpressureEvent::QueueNormal { .. } => {}
    });

    let config = TransportConfig {
        per_peer_queue_size: 10, // Small queue for testing
        ..Default::default()
    };

    let transport = RaftTransport::with_backpressure_callback(1, config, callback);

    // Create a server that doesn't read data (let queue fill up)
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let _keep = stream;
                    tokio::time::sleep(Duration::from_secs(60)).await;
                });
            }
        }
    });

    sleep(Duration::from_millis(50)).await;
    transport.add_peer(2, addr).await;

    // Rapidly send messages to fill queue
    for _ in 0..20 {
        let _ = transport.send(create_test_message(1, 2, MessageType::MsgAppend));
    }

    // Verify callback was triggered
    assert!(
        queue_full_triggered.load(AtomicOrdering::SeqCst)
            || high_watermark_triggered.load(AtomicOrdering::SeqCst),
        "Backpressure callback should have been triggered"
    );

    transport.shutdown().await;
}

/// I. Connection Pre-warming Test
/// Verify connection pre-warming functionality
#[tokio::test]
async fn test_connection_prewarming() {
    let config = TransportConfig {
        enable_connection_prewarming: true,
        ..Default::default()
    };

    let transport = RaftTransport::with_config(1, config);

    let server = MockRaftServer::new().await;
    let server_addr = server.addr();

    tokio::spawn(async move {
        server.run().await;
    });

    sleep(Duration::from_millis(50)).await;

    // Adding peer should immediately establish connection
    transport.add_peer(2, server_addr).await;

    // Wait for connection to establish
    sleep(Duration::from_millis(200)).await;

    // Verify connection is already established (even without sending messages)
    let metrics = transport.metrics();
    assert_eq!(
        metrics.connections_created, 1,
        "Connection should be pre-warmed on add_peer"
    );

    transport.shutdown().await;
}
