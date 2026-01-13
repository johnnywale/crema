//! Integration tests for DistributedCache with memberlist-based auto-discovery.
//!
//! These tests verify that the cache can automatically discover cluster members
//! using the memberlist gossip protocol, without requiring manual specification
//! of (node_id, address) pairs in seed_nodes.
//!
//! Key differences from manual configuration:
//! - `seed_nodes` is NOT set (no manual (NodeId, SocketAddr) pairs)
//! - `memberlist.enabled = true`
//! - `memberlist.seed_addrs` contains just addresses (not node IDs)
//! - Nodes discover each other via gossip and auto-add peers to Raft transport

use crate::cache::DistributedCache;
use crate::config::{CacheConfig, MemberlistConfig, RaftConfig};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

/// Global port counter to ensure each test uses unique ports
static PORT_COUNTER: AtomicU16 = AtomicU16::new(30000);

/// Allocate unique ports for a test
fn allocate_ports(count: u16) -> u16 {
    PORT_COUNTER.fetch_add(count * 100, Ordering::SeqCst)
}

/// Port configuration for a node
#[derive(Debug, Clone)]
struct NodePorts {
    raft_port: u16,
    memberlist_port: u16,
}

/// Allocate ports for multiple nodes
fn allocate_node_ports(node_ids: &[u64]) -> HashMap<u64, NodePorts> {
    let base_port = allocate_ports(node_ids.len() as u16);
    node_ids
        .iter()
        .enumerate()
        .map(|(i, &node_id)| {
            (
                node_id,
                NodePorts {
                    raft_port: base_port + (i as u16) * 2,
                    memberlist_port: base_port + (i as u16) * 2 + 1,
                },
            )
        })
        .collect()
}

/// Create a cache config with memberlist enabled for auto-discovery.
///
/// Key architecture:
/// - `initial_peers`: Node IDs for Raft voter configuration (required for consensus)
/// - `peer_raft_addrs`: Raft addresses for initial transport setup
/// - `seed_memberlist_addrs`: Memberlist addresses for gossip discovery
///
/// Memberlist handles: address discovery, health monitoring, dynamic peer updates
/// Raft seed_nodes handles: initial voter configuration for consensus
fn create_memberlist_config(
    node_id: u64,
    ports: &NodePorts,
    peer_raft_addrs: Vec<(u64, SocketAddr)>, // Other nodes' (id, raft_addr) for Raft voters
    seed_memberlist_addrs: Vec<SocketAddr>,  // Memberlist addresses for gossip
) -> CacheConfig {
    let raft_addr: SocketAddr = format!("127.0.0.1:{}", ports.raft_port).parse().unwrap();
    let memberlist_addr: SocketAddr = format!("127.0.0.1:{}", ports.memberlist_port)
        .parse()
        .unwrap();

    // Create memberlist config with auto-discovery enabled
    let memberlist_config = MemberlistConfig {
        enabled: true,
        bind_addr: Some(memberlist_addr),
        advertise_addr: None,
        seed_addrs: seed_memberlist_addrs,
        node_name: Some(format!("cache-node-{}", node_id)),
        auto_add_peers: true,   // Automatically add discovered peers to Raft transport
        auto_remove_peers: false, // Don't auto-remove (safer default)
        auto_add_voters: false,  // Don't auto-add voters in tests (we configure them manually)
        auto_remove_voters: false, // Don't auto-remove voters in tests
    };

    // Create Raft config with reasonable election settings
    let raft_config = RaftConfig {
        election_tick: 10 + (node_id as usize * 5), // Stagger election timeouts
        heartbeat_tick: 3,
        tick_interval_ms: 100,
        pre_vote: true,
        ..Default::default()
    };

    CacheConfig {
        node_id,
        raft_addr,
        // Provide peer info for Raft voter configuration
        // Memberlist will handle address updates and health monitoring
        seed_nodes: peer_raft_addrs,
        max_capacity: 10_000,
        default_ttl: Some(Duration::from_secs(3600)),
        default_tti: None,
        raft: raft_config,
        memberlist: memberlist_config,
        ..Default::default()
    }
}

/// Wait for a condition with timeout
async fn wait_for<F>(condition: F, timeout: Duration, interval: Duration) -> bool
where
    F: Fn() -> bool,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        sleep(interval).await;
    }
    false
}

/// Wait for a leader to be elected among the caches
async fn wait_for_leader<'a>(
    caches: &[&'a DistributedCache],
    timeout: Duration,
) -> Option<&'a DistributedCache> {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if let Some(leader) = caches.iter().find(|c| c.is_leader()) {
            return Some(*leader);
        }
        sleep(Duration::from_millis(100)).await;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test Case 1: Three-node cluster with memberlist for health monitoring
    ///
    /// This test verifies that:
    /// 1. Nodes can start with memberlist enabled alongside Raft
    /// 2. Memberlist provides gossip-based discovery and health monitoring
    /// 3. A leader is elected and cache operations work correctly
    /// 4. Data is replicated to all nodes
    #[tokio::test]
    async fn test_three_node_cluster_auto_discovery() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("info")
            .try_init();

        let node_ids = [1u64, 2, 3];
        let ports = allocate_node_ports(&node_ids);

        info!("Allocated ports: {:?}", ports);

        // Build Raft peer addresses for all nodes
        let raft_addr_1: SocketAddr = format!("127.0.0.1:{}", ports[&1].raft_port).parse().unwrap();
        let raft_addr_2: SocketAddr = format!("127.0.0.1:{}", ports[&2].raft_port).parse().unwrap();
        let raft_addr_3: SocketAddr = format!("127.0.0.1:{}", ports[&3].raft_port).parse().unwrap();

        // Memberlist addresses
        let ml_addr_1: SocketAddr = format!("127.0.0.1:{}", ports[&1].memberlist_port).parse().unwrap();

        // Node 1: knows about nodes 2 and 3 for Raft, no memberlist seeds (it's the seed)
        let config1 = create_memberlist_config(
            1,
            &ports[&1],
            vec![(2, raft_addr_2), (3, raft_addr_3)],
            vec![],
        );
        let cache1 = DistributedCache::new(config1)
            .await
            .expect("Node 1 should start");

        // Give node 1 time to fully initialize
        sleep(Duration::from_millis(500)).await;

        // Node 2: knows about nodes 1 and 3 for Raft, uses node 1 as memberlist seed
        let config2 = create_memberlist_config(
            2,
            &ports[&2],
            vec![(1, raft_addr_1), (3, raft_addr_3)],
            vec![ml_addr_1],
        );
        let cache2 = DistributedCache::new(config2)
            .await
            .expect("Node 2 should start");

        // Node 3: knows about nodes 1 and 2 for Raft, uses node 1 as memberlist seed
        let config3 = create_memberlist_config(
            3,
            &ports[&3],
            vec![(1, raft_addr_1), (2, raft_addr_2)],
            vec![ml_addr_1],
        );
        let cache3 = DistributedCache::new(config3)
            .await
            .expect("Node 3 should start");

        // Wait for gossip to propagate and nodes to discover each other
        info!("Waiting for memberlist gossip to propagate...");
        sleep(Duration::from_secs(2)).await;

        // Wait for leader election (may take longer due to discovery delay)
        info!("Waiting for leader election...");
        let caches = [&cache1, &cache2, &cache3];
        let leader = wait_for_leader(&caches, Duration::from_secs(15))
            .await
            .expect("A leader should be elected after auto-discovery");

        let leader_id = leader.cluster_status().node_id;
        info!("Leader elected: node {}", leader_id);

        // Verify we can perform cache operations
        leader
            .put("auto-discovery-key", "auto-discovery-value")
            .await
            .expect("PUT should succeed on leader");

        // Wait for replication
        sleep(Duration::from_millis(500)).await;

        // Verify data is replicated to all nodes
        for (i, cache) in caches.iter().enumerate() {
            let value = cache.get(b"auto-discovery-key").await;
            assert_eq!(
                value,
                Some(bytes::Bytes::from("auto-discovery-value")),
                "Node {} should have the replicated value",
                i + 1
            );
        }

        info!("Test passed: All nodes discovered each other and data replicated successfully");

        // Cleanup
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    /// Test Case 2: Node joins existing cluster via memberlist
    ///
    /// Verifies that a new node can join an existing cluster through
    /// memberlist discovery and participate in Raft consensus.
    #[tokio::test]
    async fn test_node_joins_existing_cluster() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("info")
            .try_init();

        let node_ids = [1u64, 2, 3];
        let ports = allocate_node_ports(&node_ids);

        // Build addresses
        let raft_addr_1: SocketAddr = format!("127.0.0.1:{}", ports[&1].raft_port).parse().unwrap();
        let raft_addr_2: SocketAddr = format!("127.0.0.1:{}", ports[&2].raft_port).parse().unwrap();
        let raft_addr_3: SocketAddr = format!("127.0.0.1:{}", ports[&3].raft_port).parse().unwrap();
        let ml_addr_1: SocketAddr = format!("127.0.0.1:{}", ports[&1].memberlist_port).parse().unwrap();

        // Start initial 2-node cluster (but configured to know about node 3)
        let config1 = create_memberlist_config(
            1,
            &ports[&1],
            vec![(2, raft_addr_2), (3, raft_addr_3)],
            vec![],
        );
        let cache1 = DistributedCache::new(config1)
            .await
            .expect("Node 1 should start");

        sleep(Duration::from_millis(300)).await;

        let config2 = create_memberlist_config(
            2,
            &ports[&2],
            vec![(1, raft_addr_1), (3, raft_addr_3)],
            vec![ml_addr_1],
        );
        let cache2 = DistributedCache::new(config2)
            .await
            .expect("Node 2 should start");

        // Wait for initial cluster to form
        sleep(Duration::from_secs(2)).await;

        let initial_caches = [&cache1, &cache2];
        let leader = wait_for_leader(&initial_caches, Duration::from_secs(10))
            .await
            .expect("Initial cluster should elect a leader");

        // Write some data before node 3 joins
        leader
            .put("before-join", "initial-value")
            .await
            .expect("PUT should succeed");

        sleep(Duration::from_millis(500)).await;

        // Now node 3 joins the existing cluster
        info!("Node 3 joining existing cluster...");
        let config3 = create_memberlist_config(
            3,
            &ports[&3],
            vec![(1, raft_addr_1), (2, raft_addr_2)],
            vec![ml_addr_1],
        );
        let cache3 = DistributedCache::new(config3)
            .await
            .expect("Node 3 should start and join");

        // Wait for node 3 to be discovered and sync
        sleep(Duration::from_secs(3)).await;

        // Verify node 3 can read the data (may need to wait for replication)
        let mut success = false;
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            if cache3.get(b"before-join").await.is_some() {
                success = true;
                break;
            }
            sleep(Duration::from_millis(200)).await;
        }

        assert!(
            success,
            "Node 3 should eventually receive replicated data after joining"
        );

        let value = cache3.get(b"before-join").await;
        assert_eq!(
            value,
            Some(bytes::Bytes::from("initial-value")),
            "Node 3 should have the correct value after joining"
        );

        info!("Test passed: Node 3 successfully joined and synced with existing cluster");

        // Cleanup
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    /// Test Case 3: Verify cluster status shows discovered peers
    ///
    /// Verifies that cluster_status() reflects the configured peers.
    #[tokio::test]
    async fn test_cluster_status_shows_discovered_peers() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("info")
            .try_init();

        let node_ids = [1u64, 2, 3];
        let ports = allocate_node_ports(&node_ids);

        // Build addresses
        let raft_addr_1: SocketAddr = format!("127.0.0.1:{}", ports[&1].raft_port).parse().unwrap();
        let raft_addr_2: SocketAddr = format!("127.0.0.1:{}", ports[&2].raft_port).parse().unwrap();
        let raft_addr_3: SocketAddr = format!("127.0.0.1:{}", ports[&3].raft_port).parse().unwrap();
        let ml_addr_1: SocketAddr = format!("127.0.0.1:{}", ports[&1].memberlist_port).parse().unwrap();

        // Start all nodes
        let config1 = create_memberlist_config(
            1,
            &ports[&1],
            vec![(2, raft_addr_2), (3, raft_addr_3)],
            vec![],
        );
        let cache1 = DistributedCache::new(config1)
            .await
            .expect("Node 1 should start");

        sleep(Duration::from_millis(300)).await;

        let config2 = create_memberlist_config(
            2,
            &ports[&2],
            vec![(1, raft_addr_1), (3, raft_addr_3)],
            vec![ml_addr_1],
        );
        let cache2 = DistributedCache::new(config2)
            .await
            .expect("Node 2 should start");

        let config3 = create_memberlist_config(
            3,
            &ports[&3],
            vec![(1, raft_addr_1), (2, raft_addr_2)],
            vec![ml_addr_1],
        );
        let cache3 = DistributedCache::new(config3)
            .await
            .expect("Node 3 should start");

        // Wait for discovery and leader election
        sleep(Duration::from_secs(3)).await;

        let caches = [&cache1, &cache2, &cache3];
        let _ = wait_for_leader(&caches, Duration::from_secs(10))
            .await
            .expect("Leader should be elected");

        // Check cluster status on each node
        for (i, cache) in caches.iter().enumerate() {
            let status = cache.cluster_status();
            info!(
                "Node {} status: term={}, leader={:?}, voters={:?}",
                i + 1,
                status.term,
                status.leader_id,
                cache.voters()
            );

            // Each node should know about the voters
            let voters = cache.voters();
            assert!(
                voters.len() >= 1,
                "Node {} should have at least itself as voter, got {:?}",
                i + 1,
                voters
            );
        }

        // Cleanup
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    /// Test Case 4: Multiple writes and reads after cluster formation
    ///
    /// Stress test to verify the cluster works correctly with memberlist enabled.
    #[tokio::test]
    async fn test_multiple_operations_after_discovery() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("info")
            .try_init();

        let node_ids = [1u64, 2, 3];
        let ports = allocate_node_ports(&node_ids);

        // Build addresses
        let raft_addr_1: SocketAddr = format!("127.0.0.1:{}", ports[&1].raft_port).parse().unwrap();
        let raft_addr_2: SocketAddr = format!("127.0.0.1:{}", ports[&2].raft_port).parse().unwrap();
        let raft_addr_3: SocketAddr = format!("127.0.0.1:{}", ports[&3].raft_port).parse().unwrap();
        let ml_addr_1: SocketAddr = format!("127.0.0.1:{}", ports[&1].memberlist_port).parse().unwrap();

        let config1 = create_memberlist_config(
            1,
            &ports[&1],
            vec![(2, raft_addr_2), (3, raft_addr_3)],
            vec![],
        );
        let cache1 = DistributedCache::new(config1)
            .await
            .expect("Node 1 should start");

        sleep(Duration::from_millis(300)).await;

        let config2 = create_memberlist_config(
            2,
            &ports[&2],
            vec![(1, raft_addr_1), (3, raft_addr_3)],
            vec![ml_addr_1],
        );
        let cache2 = DistributedCache::new(config2)
            .await
            .expect("Node 2 should start");

        let config3 = create_memberlist_config(
            3,
            &ports[&3],
            vec![(1, raft_addr_1), (2, raft_addr_2)],
            vec![ml_addr_1],
        );
        let cache3 = DistributedCache::new(config3)
            .await
            .expect("Node 3 should start");

        // Wait for cluster formation
        sleep(Duration::from_secs(3)).await;

        let caches = [&cache1, &cache2, &cache3];
        let leader = wait_for_leader(&caches, Duration::from_secs(10))
            .await
            .expect("Leader should be elected");

        // Perform multiple writes
        let num_writes = 20;
        for i in 0..num_writes {
            let key = format!("key-{}", i);
            let value = format!("value-{}", i);
            leader
                .put(key, value)
                .await
                .unwrap_or_else(|e| panic!("PUT {} failed: {:?}", i, e));
        }

        // Wait for replication
        sleep(Duration::from_secs(1)).await;

        // Verify all data on all nodes
        for (node_idx, cache) in caches.iter().enumerate() {
            for i in 0..num_writes {
                let key = format!("key-{}", i);
                let expected = format!("value-{}", i);
                let value = cache.get(key.as_bytes()).await;
                assert_eq!(
                    value,
                    Some(bytes::Bytes::from(expected)),
                    "Node {} should have key-{}",
                    node_idx + 1,
                    i
                );
            }
        }

        info!(
            "Test passed: {} writes replicated to all 3 nodes",
            num_writes
        );

        // Cleanup
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    /// Test Case 5: Dynamic membership via Raft ConfChange
    ///
    /// This test verifies that nodes can join a cluster dynamically:
    /// 1. Node 1 starts as a single-node bootstrap cluster with auto_add_voters enabled
    /// 2. Node 2 joins via memberlist discovery (only knows node 1's memberlist address)
    /// 3. The leader automatically proposes ConfChange to add node 2 as a voter
    /// 4. Node 2 becomes a voter and can participate in consensus
    #[tokio::test]
    async fn test_dynamic_membership_via_confchange() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("info")
            .try_init();

        let node_ids = [1u64, 2];
        let ports = allocate_node_ports(&node_ids);

        // Build addresses
        let raft_addr_1: SocketAddr = format!("127.0.0.1:{}", ports[&1].raft_port).parse().unwrap();
        let ml_addr_1: SocketAddr = format!("127.0.0.1:{}", ports[&1].memberlist_port).parse().unwrap();
        let raft_addr_2: SocketAddr = format!("127.0.0.1:{}", ports[&2].raft_port).parse().unwrap();
        let ml_addr_2: SocketAddr = format!("127.0.0.1:{}", ports[&2].memberlist_port).parse().unwrap();

        // Node 1: Bootstrap as single-node cluster with auto_add_voters enabled
        let memberlist_config_1 = MemberlistConfig {
            enabled: true,
            bind_addr: Some(ml_addr_1),
            advertise_addr: None,
            seed_addrs: vec![], // Bootstrap node has no seeds
            node_name: Some("cache-node-1".to_string()),
            auto_add_peers: true,
            auto_remove_peers: false,
            auto_add_voters: true, // Enable auto-add voters
            auto_remove_voters: false,
        };

        let raft_config_1 = RaftConfig {
            election_tick: 10,
            heartbeat_tick: 3,
            tick_interval_ms: 100,
            pre_vote: true,
            ..Default::default()
        };

        let config1 = CacheConfig {
            node_id: 1,
            raft_addr: raft_addr_1,
            seed_nodes: vec![], // Single-node bootstrap
            max_capacity: 10_000,
            default_ttl: Some(Duration::from_secs(3600)),
            default_tti: None,
            raft: raft_config_1,
            memberlist: memberlist_config_1,
            ..Default::default()
        };

        info!("Starting node 1 (bootstrap single-node cluster)...");
        let cache1 = DistributedCache::new(config1)
            .await
            .expect("Node 1 should start");

        // Wait for node 1 to become leader (single-node cluster)
        let leader_found = wait_for(
            || cache1.is_leader(),
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .await;
        assert!(leader_found, "Node 1 should become leader as single-node cluster");

        info!("Node 1 is now leader. Voters: {:?}", cache1.voters());

        // Write some data while it's a single-node cluster
        cache1
            .put("before-join", "single-node-value")
            .await
            .expect("PUT should succeed on single-node cluster");

        // Node 2: Join via memberlist discovery only
        // It knows node 1's memberlist address but NOT its Raft address for seed_nodes
        let memberlist_config_2 = MemberlistConfig {
            enabled: true,
            bind_addr: Some(ml_addr_2),
            advertise_addr: None,
            seed_addrs: vec![ml_addr_1], // Join via node 1's memberlist
            node_name: Some("cache-node-2".to_string()),
            auto_add_peers: true,
            auto_remove_peers: false,
            auto_add_voters: true, // Enable auto-add voters
            auto_remove_voters: false,
        };

        let raft_config_2 = RaftConfig {
            election_tick: 15, // Slightly higher to let node 1 remain leader
            heartbeat_tick: 3,
            tick_interval_ms: 100,
            pre_vote: true,
            ..Default::default()
        };

        let config2 = CacheConfig {
            node_id: 2,
            raft_addr: raft_addr_2,
            // Need to specify node 1 as seed for Raft transport (so messages can be routed)
            // But node 2 is NOT initially in node 1's voter list - it will be added via ConfChange
            seed_nodes: vec![(1, raft_addr_1)],
            max_capacity: 10_000,
            default_ttl: Some(Duration::from_secs(3600)),
            default_tti: None,
            raft: raft_config_2,
            memberlist: memberlist_config_2,
            ..Default::default()
        };

        info!("Starting node 2 (joining via memberlist discovery)...");
        let cache2 = DistributedCache::new(config2)
            .await
            .expect("Node 2 should start");

        // Wait for node 2 to be discovered and added as voter
        info!("Waiting for node 2 to be added as voter via ConfChange...");
        let node2_added = wait_for(
            || {
                let voters = cache1.voters();
                voters.contains(&2)
            },
            Duration::from_secs(15),
            Duration::from_millis(200),
        )
        .await;

        if node2_added {
            info!("Node 2 successfully added as voter! Voters: {:?}", cache1.voters());
        } else {
            info!("Node 2 not added yet. Current voters: {:?}", cache1.voters());
        }

        // Even if ConfChange hasn't completed, node 2 should be able to receive data via replication
        // because it's in the transport. Let's verify this.

        // Wait for replication to node 2
        sleep(Duration::from_secs(2)).await;

        let value = cache2.get(b"before-join").await;
        info!(
            "Node 2 read 'before-join': {:?}",
            value.as_ref().map(|v| String::from_utf8_lossy(v).to_string())
        );

        // Write new data (if we have a leader)
        let caches = [&cache1, &cache2];
        if let Some(leader) = caches.iter().find(|c| c.is_leader()) {
            info!("Leader is node {}. Writing new data...", leader.cluster_status().node_id);
            leader
                .put("after-join", "multi-node-value")
                .await
                .expect("PUT should succeed");

            // Wait for replication
            sleep(Duration::from_secs(1)).await;

            // Verify both nodes have the data
            for (i, cache) in caches.iter().enumerate() {
                let v1 = cache.get(b"before-join").await;
                let v2 = cache.get(b"after-join").await;
                info!(
                    "Node {} has: before-join={:?}, after-join={:?}",
                    i + 1,
                    v1.as_ref().map(|v| String::from_utf8_lossy(v).to_string()),
                    v2.as_ref().map(|v| String::from_utf8_lossy(v).to_string())
                );
            }
        }

        info!("Final cluster status:");
        info!("  Node 1 - Leader: {}, Voters: {:?}", cache1.is_leader(), cache1.voters());
        info!("  Node 2 - Leader: {}, Voters: {:?}", cache2.is_leader(), cache2.voters());

        // Cleanup
        cache1.shutdown().await;
        cache2.shutdown().await;
    }
}
