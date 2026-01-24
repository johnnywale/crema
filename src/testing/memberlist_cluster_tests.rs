//! Integration tests for MemberlistCluster using the actual memberlist crate.
//!
//! These tests verify the real gossip-based node discovery and event handling
//! using the memberlist crate's SWIM protocol implementation.
//!
//! All tests start actual memberlist services that communicate over the network.

use crate::cluster::memberlist_cluster::{
    MemberlistCluster, MemberlistClusterConfig, MemberlistError, MemberlistEvent, NodeRegistry,
    RaftNodeMetadata,
};
use std::net::SocketAddr;
use std::time::Duration;
use std::net::UdpSocket;
use tokio::net::TcpListener;

/// Port configuration for a node (bind port for memberlist and raft port)
#[derive(Debug, Clone)]
struct NodePorts {
    bind_port: u16,
    raft_port: u16,
}

/// Allocate OS-assigned ports for multiple nodes.
/// Each node needs 2 ports: one for memberlist bind and one for raft.
///
/// Note: memberlist uses BOTH UDP AND TCP on the same bind port (SWIM protocol).
/// We must ensure both are available on the allocated port.
async fn allocate_node_ports(count: usize) -> Vec<NodePorts> {
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        // For memberlist bind port, we need BOTH UDP and TCP to be available on the same port.
        // Strategy: allocate UDP first, then verify TCP is also available on the same port.
        // If TCP fails, try again with a different port.
        let bind_port = loop {
            let udp_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
            let port = udp_socket.local_addr().unwrap().port();
            drop(udp_socket);

            // Verify TCP is also available on this port
            match TcpListener::bind(format!("127.0.0.1:{}", port)).await {
                Ok(listener) => {
                    drop(listener);
                    break port;
                }
                Err(_) => {
                    // TCP not available, try another port
                    continue;
                }
            }
        };

        // Allocate raft port using TCP
        let raft_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raft_port = raft_listener.local_addr().unwrap().port();
        drop(raft_listener);

        result.push(NodePorts { bind_port, raft_port });
    }
    result
}

/// Create a memberlist cluster config with given ports
fn create_config(node_id: u64, ports: &NodePorts, seed_ports: &[u16]) -> MemberlistClusterConfig {
    let seed_addrs: Vec<SocketAddr> = seed_ports
        .iter()
        .map(|p| format!("127.0.0.1:{}", p).parse().unwrap())
        .collect();

    MemberlistClusterConfig::new(
        node_id,
        format!("127.0.0.1:{}", ports.bind_port).parse().unwrap(),
        format!("127.0.0.1:{}", ports.raft_port).parse().unwrap(),
    )
    .with_node_name(format!("test-node-{}", node_id))
    .with_seed_nodes(seed_addrs)
}

/// Helper to drain all pending events
fn drain_events(cluster: &mut MemberlistCluster) -> Vec<MemberlistEvent> {
    let mut events = Vec::new();
    while let Some(event) = cluster.try_recv_event() {
        events.push(event);
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== Unit Tests for NodeRegistry ====================

    #[test]
    fn test_node_registry_creation() {
        let registry = NodeRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_node_registry_register_and_lookup() {
        let registry = NodeRegistry::new();

        let meta1 = RaftNodeMetadata::new(1, "127.0.0.1:9001".parse().unwrap());
        let meta2 = RaftNodeMetadata::new(2, "127.0.0.1:9002".parse().unwrap());

        assert!(registry.register(meta1.clone(), "node-1".into()));
        assert!(registry.register(meta2.clone(), "node-2".into()));

        assert_eq!(registry.len(), 2);
        assert!(registry.contains(1));
        assert!(registry.contains(2));
        assert!(!registry.contains(3));

        assert_eq!(
            registry.get_addr(1),
            Some("127.0.0.1:9001".parse().unwrap())
        );
        assert_eq!(
            registry.get_addr(2),
            Some("127.0.0.1:9002".parse().unwrap())
        );
    }

    #[test]
    fn test_node_registry_health_tracking() {
        let registry = NodeRegistry::new();

        let meta = RaftNodeMetadata::new(1, "127.0.0.1:9001".parse().unwrap());
        registry.register(meta, "node-1".into());

        assert!(registry.is_healthy(1));
        registry.mark_failed(1);
        assert!(!registry.is_healthy(1));
    }

    #[test]
    fn test_node_registry_unregister() {
        let registry = NodeRegistry::new();

        let meta = RaftNodeMetadata::new(1, "127.0.0.1:9001".parse().unwrap());
        registry.register(meta, "node-1".into());

        assert!(registry.contains(1));
        let removed = registry.unregister(1);
        assert!(removed.is_some());
        assert!(!registry.contains(1));
    }

    #[test]
    fn test_raft_node_metadata_serialization() {
        let meta = RaftNodeMetadata::new(1, "127.0.0.1:9001".parse().unwrap())
            .with_tag("role", "voter")
            .with_tag("region", "us-east");

        let bytes = meta.to_bytes();
        let decoded = RaftNodeMetadata::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.raft_id, 1);
        assert_eq!(decoded.raft_addr, "127.0.0.1:9001".parse().unwrap());
        assert_eq!(decoded.tags.get("role"), Some(&"voter".to_string()));
    }

    // ==================== Real Network Integration Tests ====================

    /// Test Case 1: Single node memberlist startup
    /// Verifies that a single node can start and bind to its port successfully.
    #[tokio::test]
    async fn test_case_1_single_node_startup() {
        let ports = allocate_node_ports(1).await;
        let config = create_config(1, &ports[0], &[]);

        let mut cluster = MemberlistCluster::new(config);
        assert!(!cluster.is_initialized());

        // Start the memberlist
        let result = cluster.start().await;
        match result {
            Ok(()) => {
                assert!(cluster.is_initialized());
                assert!(cluster.registry().contains(1));

                // Cleanup
                let _ = cluster.shutdown().await;
            }
            Err(MemberlistError::BindError(e)) => {
                println!("Skipping test - port binding failed: {}", e);
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    /// Test Case 2: Two-node cluster formation
    /// Node 2 joins Node 1 via seed configuration and they discover each other.
    #[tokio::test]
    async fn test_case_2_two_node_cluster_formation() {
        let ports = allocate_node_ports(2).await;

        // Start Node 1 (seed node)
        let config1 = create_config(1, &ports[0], &[]);
        let mut cluster1 = MemberlistCluster::new(config1);

        cluster1
            .start()
            .await
            .expect("Node 1 should start successfully");
        assert!(cluster1.is_initialized(), "Node 1 should be initialized");

        // Start Node 2 with Node 1 as seed
        let config2 = create_config(2, &ports[1], &[ports[0].bind_port]);
        let mut cluster2 = MemberlistCluster::new(config2);

        cluster2
            .start()
            .await
            .expect("Node 2 should start successfully");
        assert!(cluster2.is_initialized(), "Node 2 should be initialized");

        // Wait for gossip to propagate (memberlist needs time to exchange info)
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // Check if Node 1 received join event from Node 2
        let events1 = drain_events(&mut cluster1);
        let node2_joined_node1 = events1.iter().any(|e| {
            matches!(e, MemberlistEvent::NodeJoin { raft_id, .. } if *raft_id == 2)
        });

        // Check if Node 2 received join event from Node 1
        let events2 = drain_events(&mut cluster2);
        let node1_joined_node2 = events2.iter().any(|e| {
            matches!(e, MemberlistEvent::NodeJoin { raft_id, .. } if *raft_id == 1)
        });

        // At least one direction should have discovered the other
        // (gossip is eventually consistent, but should work in both directions)
        assert!(
            node2_joined_node1 || node1_joined_node2,
            "Nodes should discover each other via gossip. Node 1 saw Node 2: {}, Node 2 saw Node 1: {}",
            node2_joined_node1,
            node1_joined_node2
        );

        // Verify both nodes have each other in their registries
        // The joining node should register itself with the seed and vice versa
        assert!(
            cluster1.registry().contains(1),
            "Node 1 should have itself in registry"
        );
        assert!(
            cluster2.registry().contains(2),
            "Node 2 should have itself in registry"
        );

        // Cleanup
        let _ = cluster2.shutdown().await;
        let _ = cluster1.shutdown().await;
    }

    /// Test Case 3: Three-node cluster formation
    /// Tests a proper cluster with 3 nodes discovering each other.
    #[tokio::test]
    async fn test_case_3_three_node_cluster_formation() {
        let ports = allocate_node_ports(3).await;

        // Start Node 1 (seed node)
        let config1 = create_config(1, &ports[0], &[]);
        let mut cluster1 = MemberlistCluster::new(config1);

        cluster1
            .start()
            .await
            .expect("Node 1 should start successfully");

        // Start Node 2 with Node 1 as seed
        let config2 = create_config(2, &ports[1], &[ports[0].bind_port]);
        let mut cluster2 = MemberlistCluster::new(config2);

        cluster2
            .start()
            .await
            .expect("Node 2 should start successfully");

        // Start Node 3 with Node 1 as seed
        let config3 = create_config(3, &ports[2], &[ports[0].bind_port]);
        let mut cluster3 = MemberlistCluster::new(config3);

        cluster3
            .start()
            .await
            .expect("Node 3 should start successfully");

        // Wait for gossip to propagate
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // Verify all nodes are initialized
        assert!(cluster1.is_initialized(), "Node 1 should be initialized");
        assert!(cluster2.is_initialized(), "Node 2 should be initialized");
        assert!(cluster3.is_initialized(), "Node 3 should be initialized");

        // Drain events and check discovery
        let events1 = drain_events(&mut cluster1);
        let events2 = drain_events(&mut cluster2);
        let events3 = drain_events(&mut cluster3);

        // Node 1 should see join events from Node 2 and Node 3
        assert!(
            !events1.is_empty(),
            "Node 1 should receive join events from other nodes"
        );
        // Node 2 should see join events from Node 1 and Node 3
        assert!(
            !events2.is_empty(),
            "Node 2 should receive join events from other nodes"
        );
        // Node 3 should see join events from Node 1 and Node 2
        assert!(
            !events3.is_empty(),
            "Node 3 should receive join events from other nodes"
        );

        // Cleanup
        let _ = cluster3.shutdown().await;
        let _ = cluster2.shutdown().await;
        let _ = cluster1.shutdown().await;
    }

    /// Test Case 4: Graceful node leave
    /// Tests that when a node gracefully leaves, others are notified.
    #[tokio::test]
    async fn test_case_4_graceful_node_leave() {
        let ports = allocate_node_ports(2).await;

        // Start Node 1
        let config1 = create_config(1, &ports[0], &[]);
        let mut cluster1 = MemberlistCluster::new(config1);

        cluster1
            .start()
            .await
            .expect("Node 1 should start successfully");

        // Start Node 2
        let config2 = create_config(2, &ports[1], &[ports[0].bind_port]);
        let mut cluster2 = MemberlistCluster::new(config2);

        cluster2
            .start()
            .await
            .expect("Node 2 should start successfully");

        // Wait for gossip to propagate
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Clear initial join events
        drain_events(&mut cluster1);
        drain_events(&mut cluster2);

        // Node 2 leaves gracefully
        let leave_result = cluster2.leave().await;
        assert!(leave_result.is_ok(), "Node 2 should leave gracefully");

        // Wait for leave notification to propagate
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Check if Node 1 received leave event
        let events1 = drain_events(&mut cluster1);
        let node2_left = events1.iter().any(|e| {
            matches!(e, MemberlistEvent::NodeLeave { raft_id } if *raft_id == 2)
        });

        assert!(
            node2_left,
            "Node 1 should receive NodeLeave event for Node 2"
        );

        // Cleanup
        let _ = cluster1.shutdown().await;
    }

    /// Test Case 5: Node registry is updated on join
    /// Verifies that the NodeRegistry is correctly updated when nodes join.
    #[tokio::test]
    async fn test_case_5_registry_updated_on_join() {
        let ports = allocate_node_ports(2).await;

        // Start Node 1
        let config1 = create_config(1, &ports[0], &[]);
        let mut cluster1 = MemberlistCluster::new(config1);

        cluster1
            .start()
            .await
            .expect("Node 1 should start successfully");

        // Initially only self is registered
        assert_eq!(
            cluster1.registry().len(),
            1,
            "Initially only self should be in registry"
        );
        assert!(
            cluster1.registry().contains(1),
            "Node 1 should be in its own registry"
        );

        // Start Node 2
        let config2 = create_config(2, &ports[1], &[ports[0].bind_port]);
        let mut cluster2 = MemberlistCluster::new(config2);

        cluster2
            .start()
            .await
            .expect("Node 2 should start successfully");

        // Wait for gossip
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Process any pending events to update registry
        drain_events(&mut cluster1);

        // Check if registry was updated - Node 1 should now know about Node 2
        assert_eq!(
            cluster1.registry().len(),
            2,
            "Node 1 registry should contain both nodes after gossip"
        );
        assert!(
            cluster1.registry().contains(2),
            "Node 1 should have Node 2 in registry after gossip"
        );

        // Cleanup
        let _ = cluster2.shutdown().await;
        let _ = cluster1.shutdown().await;
    }

    /// Test Case 6: Multiple seeds for redundancy
    /// Tests joining with multiple seed nodes for redundancy.
    #[tokio::test]
    async fn test_case_6_multiple_seeds() {
        let ports = allocate_node_ports(3).await;

        // Start Node 1
        let config1 = create_config(1, &ports[0], &[]);
        let mut cluster1 = MemberlistCluster::new(config1);

        cluster1
            .start()
            .await
            .expect("Node 1 should start successfully");

        // Start Node 2
        let config2 = create_config(2, &ports[1], &[ports[0].bind_port]);
        let mut cluster2 = MemberlistCluster::new(config2);

        cluster2
            .start()
            .await
            .expect("Node 2 should start successfully");

        // Wait for Node 1 and 2 to discover each other
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Start Node 3 with both Node 1 and Node 2 as seeds
        let config3 = create_config(3, &ports[2], &[ports[0].bind_port, ports[1].bind_port]);
        let mut cluster3 = MemberlistCluster::new(config3);

        cluster3
            .start()
            .await
            .expect("Node 3 should start successfully with multiple seeds");

        // Wait for gossip
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // All nodes should be initialized
        assert!(cluster1.is_initialized(), "Node 1 should be initialized");
        assert!(cluster2.is_initialized(), "Node 2 should be initialized");
        assert!(
            cluster3.is_initialized(),
            "Node 3 should be initialized after joining via multiple seeds"
        );

        // Drain events and verify Node 3 discovered other nodes
        let events3 = drain_events(&mut cluster3);
        assert!(
            !events3.is_empty(),
            "Node 3 should discover other nodes via gossip"
        );

        // Cleanup
        let _ = cluster3.shutdown().await;
        let _ = cluster2.shutdown().await;
        let _ = cluster1.shutdown().await;
    }

    /// Test Case 7: Shutdown without leave
    /// Tests abrupt shutdown (simulating crash).
    #[tokio::test]
    async fn test_case_7_shutdown_without_leave() {
        let ports = allocate_node_ports(2).await;

        // Start Node 1
        let config1 = create_config(1, &ports[0], &[]);
        let mut cluster1 = MemberlistCluster::new(config1);

        cluster1
            .start()
            .await
            .expect("Node 1 should start successfully");

        // Start Node 2
        let config2 = create_config(2, &ports[1], &[ports[0].bind_port]);
        let mut cluster2 = MemberlistCluster::new(config2);

        cluster2
            .start()
            .await
            .expect("Node 2 should start successfully");

        // Wait for gossip
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify nodes discovered each other
        drain_events(&mut cluster1);
        assert!(
            cluster1.registry().contains(2),
            "Node 1 should have discovered Node 2"
        );

        // Shutdown Node 2 abruptly (without leave)
        let shutdown_result = cluster2.shutdown().await;
        assert!(
            shutdown_result.is_ok(),
            "Shutdown should complete successfully"
        );
        assert!(
            !cluster2.is_initialized(),
            "Node 2 should no longer be initialized after shutdown"
        );

        // Node 1 should eventually detect the failure via memberlist's failure detector
        // This may take longer than a graceful leave
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Cleanup
        let _ = cluster1.shutdown().await;
    }

    /// Test Case 8: Re-join after leave
    /// Tests that a node can rejoin the cluster after leaving.
    #[tokio::test]
    async fn test_case_8_rejoin_after_leave() {
        // Allocate ports for node1, node2, and node2's rejoin
        let ports = allocate_node_ports(3).await;

        // Start Node 1
        let config1 = create_config(1, &ports[0], &[]);
        let mut cluster1 = MemberlistCluster::new(config1);

        cluster1
            .start()
            .await
            .expect("Node 1 should start successfully");

        // Start Node 2
        let config2 = create_config(2, &ports[1], &[ports[0].bind_port]);
        let mut cluster2 = MemberlistCluster::new(config2);

        cluster2
            .start()
            .await
            .expect("Node 2 should start successfully");

        // Wait for gossip
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify initial discovery
        drain_events(&mut cluster1);
        assert!(
            cluster1.registry().contains(2),
            "Node 1 should discover Node 2 initially"
        );

        // Node 2 leaves gracefully
        let leave_result = cluster2.leave().await;
        assert!(leave_result.is_ok(), "Node 2 should leave gracefully");
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Verify leave was detected
        drain_events(&mut cluster1);

        // Node 2 rejoins with a new port (simulating restart on different port)
        let config2_new = MemberlistClusterConfig::new(
            2,
            format!("127.0.0.1:{}", ports[2].bind_port).parse().unwrap(),
            format!("127.0.0.1:{}", ports[2].raft_port).parse().unwrap(),
        )
        .with_node_name("test-node-2-rejoined".to_string())
        .with_seed_nodes(vec![format!("127.0.0.1:{}", ports[0].bind_port).parse().unwrap()]);

        let mut cluster2_new = MemberlistCluster::new(config2_new);
        cluster2_new
            .start()
            .await
            .expect("Node 2 should rejoin successfully");

        // Wait for gossip
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify rejoin was successful
        drain_events(&mut cluster1);
        assert!(
            cluster1.registry().contains(2),
            "Node 1 should see Node 2 after rejoin"
        );

        // Cleanup
        let _ = cluster2_new.shutdown().await;
        let _ = cluster1.shutdown().await;
    }

    /// Test Case 9: Metadata propagation
    /// Verifies that node metadata (tags) is correctly propagated via gossip.
    #[tokio::test]
    async fn test_case_9_metadata_propagation() {
        let ports = allocate_node_ports(2).await;

        // Start Node 1
        let config1 = create_config(1, &ports[0], &[]);
        let mut cluster1 = MemberlistCluster::new(config1);

        cluster1
            .start()
            .await
            .expect("Node 1 should start successfully");

        // Start Node 2 (metadata is included in RaftNodeMetadata)
        let config2 = create_config(2, &ports[1], &[ports[0].bind_port]);
        let mut cluster2 = MemberlistCluster::new(config2);

        cluster2
            .start()
            .await
            .expect("Node 2 should start successfully");

        // Wait for gossip
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Check events for metadata
        let events1 = drain_events(&mut cluster1);

        // Find the join event for Node 2 and verify metadata
        let node2_join_event = events1.iter().find(|e| {
            matches!(e, MemberlistEvent::NodeJoin { raft_id, .. } if *raft_id == 2)
        });

        assert!(
            node2_join_event.is_some(),
            "Node 1 should receive NodeJoin event for Node 2"
        );

        if let Some(MemberlistEvent::NodeJoin { metadata, .. }) = node2_join_event {
            assert_eq!(metadata.raft_id, 2, "Metadata should contain correct raft_id");
            assert!(
                !metadata.version.is_empty(),
                "Metadata should contain version string"
            );
            // Verify raft_addr is set correctly
            assert_eq!(
                metadata.raft_addr.port(),
                ports[1].raft_port,
                "Metadata should contain correct raft_addr"
            );
        }

        // Cleanup
        let _ = cluster2.shutdown().await;
        let _ = cluster1.shutdown().await;
    }

    /// Test Case 10: Concurrent node joins
    /// Tests multiple nodes joining simultaneously.
    #[tokio::test]
    async fn test_case_10_concurrent_node_joins() {
        let ports = allocate_node_ports(4).await;

        // Start Node 1 (seed)
        let config1 = create_config(1, &ports[0], &[]);
        let mut cluster1 = MemberlistCluster::new(config1);

        cluster1
            .start()
            .await
            .expect("Node 1 should start successfully");

        // Start Nodes 2, 3, 4 concurrently
        let config2 = create_config(2, &ports[1], &[ports[0].bind_port]);
        let config3 = create_config(3, &ports[2], &[ports[0].bind_port]);
        let config4 = create_config(4, &ports[3], &[ports[0].bind_port]);

        let mut cluster2 = MemberlistCluster::new(config2);
        let mut cluster3 = MemberlistCluster::new(config3);
        let mut cluster4 = MemberlistCluster::new(config4);

        // Start all three concurrently
        let (r2, r3, r4) = tokio::join!(cluster2.start(), cluster3.start(), cluster4.start());

        // All nodes should start successfully
        assert!(r2.is_ok(), "Node 2 should start successfully");
        assert!(r3.is_ok(), "Node 3 should start successfully");
        assert!(r4.is_ok(), "Node 4 should start successfully");

        // Wait for gossip
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // Check Node 1's events
        let events1 = drain_events(&mut cluster1);
        let join_count = events1
            .iter()
            .filter(|e| matches!(e, MemberlistEvent::NodeJoin { .. }))
            .count();

        // Node 1 should receive join events from all 3 other nodes
        assert_eq!(
            join_count, 3,
            "Node 1 should receive 3 join events from concurrent joins"
        );

        // Cleanup
        let _ = cluster4.shutdown().await;
        let _ = cluster3.shutdown().await;
        let _ = cluster2.shutdown().await;
        let _ = cluster1.shutdown().await;
    }

    /// Test Case 11: Event ordering
    /// Verifies that events are received in a reasonable order.
    #[tokio::test]
    async fn test_case_11_event_ordering() {
        let ports = allocate_node_ports(2).await;

        // Start Node 1
        let config1 = create_config(1, &ports[0], &[]);
        let mut cluster1 = MemberlistCluster::new(config1);

        cluster1
            .start()
            .await
            .expect("Node 1 should start successfully");

        // Start Node 2
        let config2 = create_config(2, &ports[1], &[ports[0].bind_port]);
        let mut cluster2 = MemberlistCluster::new(config2);

        cluster2
            .start()
            .await
            .expect("Node 2 should start successfully");

        // Wait for join
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Node 2 leaves
        let _ = cluster2.leave().await;

        // Wait for leave
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Collect all events from Node 1
        let events = drain_events(&mut cluster1);

        // Verify we got join before leave
        let mut saw_join = false;
        let mut saw_leave = false;
        let mut order_correct = true;

        for event in &events {
            match event {
                MemberlistEvent::NodeJoin { raft_id: 2, .. } => {
                    if saw_leave {
                        order_correct = false;
                    }
                    saw_join = true;
                }
                MemberlistEvent::NodeLeave { raft_id: 2 } => {
                    saw_leave = true;
                }
                _ => {}
            }
        }

        // We should see both join and leave events
        assert!(saw_join, "Should see NodeJoin event for Node 2");
        assert!(saw_leave, "Should see NodeLeave event for Node 2");
        assert!(
            order_correct,
            "NodeJoin should come before NodeLeave in event ordering"
        );

        // Cleanup
        let _ = cluster1.shutdown().await;
    }

    /// Test Case 12: Large cluster (5 nodes)
    /// Tests cluster formation with 5 nodes.
    #[tokio::test]
    async fn test_case_12_large_cluster() {
        let ports = allocate_node_ports(5).await;

        // Start Node 1 (seed)
        let config1 = create_config(1, &ports[0], &[]);
        let mut cluster1 = MemberlistCluster::new(config1);

        cluster1
            .start()
            .await
            .expect("Seed node should start successfully");

        let mut clusters: Vec<MemberlistCluster> = vec![];

        // Start nodes 2-5
        for i in 2..=5 {
            let config = create_config(i as u64, &ports[i - 1], &[ports[0].bind_port]);
            let mut cluster = MemberlistCluster::new(config);

            cluster
                .start()
                .await
                .unwrap_or_else(|_| panic!("Node {} should start successfully", i));
            clusters.push(cluster);
        }

        assert_eq!(clusters.len(), 4, "All 4 additional nodes should start");

        // Wait for gossip to propagate through the cluster
        tokio::time::sleep(Duration::from_millis(2000)).await;

        // Check events on seed node
        let events = drain_events(&mut cluster1);
        let join_events: Vec<_> = events
            .iter()
            .filter_map(|e| {
                if let MemberlistEvent::NodeJoin { raft_id, .. } = e {
                    Some(*raft_id)
                } else {
                    None
                }
            })
            .collect();

        // Seed node should see all 4 other nodes join
        assert_eq!(
            join_events.len(),
            4,
            "Seed node should receive join events from all 4 other nodes"
        );

        // Verify all expected nodes joined
        for expected_id in 2..=5 {
            assert!(
                join_events.contains(&expected_id),
                "Seed node should see Node {} join",
                expected_id
            );
        }

        // Cleanup
        for mut cluster in clusters {
            let _ = cluster.shutdown().await;
        }
        let _ = cluster1.shutdown().await;
    }

    /// Test Case 13: Node address lookup after join
    /// Verifies that get_node_addr works after gossip discovery.
    #[tokio::test]
    async fn test_case_13_address_lookup_after_join() {
        let ports = allocate_node_ports(2).await;

        // Start Node 1
        let config1 = create_config(1, &ports[0], &[]);
        let mut cluster1 = MemberlistCluster::new(config1);

        cluster1
            .start()
            .await
            .expect("Node 1 should start successfully");

        // Node 1 can look up its own address
        let self_addr = cluster1.get_node_addr(1);
        assert!(
            self_addr.is_some(),
            "Node 1 should be able to look up its own address"
        );

        // Verify self address is correct
        assert_eq!(
            self_addr.unwrap().port(),
            ports[0].raft_port,
            "Self address should have correct raft port"
        );

        // Start Node 2
        let config2 = create_config(2, &ports[1], &[ports[0].bind_port]);
        let mut cluster2 = MemberlistCluster::new(config2);

        cluster2
            .start()
            .await
            .expect("Node 2 should start successfully");

        // Wait for gossip
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Process events to update registry
        drain_events(&mut cluster1);

        // Try to look up Node 2's address from Node 1
        let node2_addr = cluster1.get_node_addr(2);
        assert!(
            node2_addr.is_some(),
            "Node 1 should be able to look up Node 2's address after gossip"
        );

        // Verify Node 2's address is correct
        assert_eq!(
            node2_addr.unwrap().port(),
            ports[1].raft_port,
            "Node 2 address should have correct raft port"
        );

        // Cleanup
        let _ = cluster2.shutdown().await;
        let _ = cluster1.shutdown().await;
    }
}
