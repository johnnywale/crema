//! Integration tests for DistributedCache with different cluster discovery implementations.
//!
//! These tests verify the cache functionality with:
//! - NoOpClusterDiscovery (single-node and static seed_nodes)
//! - StaticClusterDiscovery (fixed IP list with health checks)
//!
//! All tests use real network connections and actual cache instances.

#[cfg(test)]
mod tests {
    use crate::cache::DistributedCache;
    use crate::cluster::{
        ClusterDiscovery, NoOpClusterDiscovery, StaticClusterDiscovery, StaticDiscoveryConfig,
    };
    use crate::config::{CacheConfig, RaftConfig};
    use crate::types::NodeId;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::time::{Duration, Instant};
    use tokio::time::sleep;

    /// Port counter to ensure unique ports across tests
    static PORT_COUNTER: AtomicU16 = AtomicU16::new(23000);

    /// Allocate a range of ports for a test
    fn allocate_ports(count: u16) -> u16 {
        PORT_COUNTER.fetch_add(count * 2, Ordering::SeqCst)
    }

    /// Create a cache config with NoOp discovery
    fn noop_discovery_config(node_id: NodeId, base_port: u16) -> CacheConfig {
        let raft_addr: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();

        let discovery = NoOpClusterDiscovery::new(node_id, raft_addr);

        CacheConfig::new(node_id, raft_addr)
            .with_cluster_discovery(discovery)
            .with_raft_config(RaftConfig {
                election_tick: 5,
                heartbeat_tick: 2,
                tick_interval_ms: 100,
                ..Default::default()
            })
    }

    /// Create a cache config with NoOp discovery but with seed_nodes for multi-node cluster
    fn noop_discovery_cluster_config(
        node_id: NodeId,
        base_port: u16,
        peer_configs: Vec<(NodeId, u16)>,
    ) -> CacheConfig {
        let raft_addr: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();

        let seed_nodes: Vec<(NodeId, SocketAddr)> = peer_configs
            .into_iter()
            .filter(|(id, _)| *id != node_id)
            .map(|(id, port)| {
                let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
                (id, addr)
            })
            .collect();

        let base_election_tick = 15;

        let discovery = NoOpClusterDiscovery::new(node_id, raft_addr);

        CacheConfig::new(node_id, raft_addr)
            .with_seed_nodes(seed_nodes)
            .with_cluster_discovery(discovery)
            .with_raft_config(RaftConfig {
                tick_interval_ms: 100,
                election_tick: base_election_tick + (node_id as usize * 5),
                heartbeat_tick: 2,
                ..Default::default()
            })
    }

    /// Create a cache config with Static discovery
    fn static_discovery_config(
        node_id: NodeId,
        base_port: u16,
        peer_configs: Vec<(NodeId, u16)>,
    ) -> CacheConfig {
        let raft_addr: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();

        let peers: Vec<(NodeId, SocketAddr)> = peer_configs
            .into_iter()
            .map(|(id, port)| {
                let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
                (id, addr)
            })
            .collect();

        let seed_nodes: Vec<(NodeId, SocketAddr)> = peers
            .iter()
            .filter(|(id, _)| *id != node_id)
            .cloned()
            .collect();

        let base_election_tick = 15;

        let static_config = StaticDiscoveryConfig::new(peers).without_health_checks();
        let discovery = StaticClusterDiscovery::new(node_id, raft_addr, static_config);

        CacheConfig::new(node_id, raft_addr)
            .with_seed_nodes(seed_nodes)
            .with_cluster_discovery(discovery)
            .with_raft_config(RaftConfig {
                tick_interval_ms: 100,
                election_tick: base_election_tick + (node_id as usize * 5),
                heartbeat_tick: 2,
                ..Default::default()
            })
    }

    /// Wait for a condition with timeout
    async fn wait_for<F>(condition: F, timeout: Duration, check_interval: Duration) -> bool
    where
        F: Fn() -> bool,
    {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if condition() {
                return true;
            }
            sleep(check_interval).await;
        }
        false
    }

    /// Wait for a node to become leader
    async fn wait_for_leader(cache: &DistributedCache, timeout: Duration) -> bool {
        wait_for(|| cache.is_leader(), timeout, Duration::from_millis(50)).await
    }

    /// Get election timeout duration based on config
    fn election_timeout(config: &CacheConfig) -> Duration {
        Duration::from_millis(config.raft.tick_interval_ms * config.raft.election_tick as u64)
    }

    // ========================================================================
    // NoOp Discovery Tests
    // ========================================================================

    /// TC-D1: Single node with NoOp discovery becomes leader
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_d1_noop_single_node_becomes_leader() {
        let base_port = allocate_ports(1);
        let config = noop_discovery_config(1, base_port);
        let election_time = election_timeout(&config);

        let cache = DistributedCache::new(config)
            .await
            .expect("Failed to create cache");

        // Verify NoOp discovery is being used (not active)
        assert!(
            !cache.discovery_enabled(),
            "Discovery should not be active (NoOp)"
        );

        // Wait for leader election
        let became_leader = wait_for_leader(&cache, election_time * 3).await;

        assert!(became_leader, "Single node should become leader");
        assert!(cache.is_leader(), "Node should be leader");
        assert_eq!(cache.leader_id(), Some(1), "Leader ID should be self");

        // Verify cluster status
        let status = cache.cluster_status();
        assert_eq!(status.node_id, 1);
        assert!(status.is_leader);

        // NoOp discovery still registers local node
        assert_eq!(
            cache.discovery_members().len(),
            1,
            "Should have 1 member (self)"
        );

        cache.shutdown().await;
    }

    /// TC-D2: Single node with NoOp discovery - read/write operations
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_d2_noop_single_node_read_write() {
        let base_port = allocate_ports(1);
        let config = noop_discovery_config(1, base_port);
        let election_time = election_timeout(&config);

        let cache = DistributedCache::new(config)
            .await
            .expect("Failed to create cache");

        // Wait for leader
        let became_leader = wait_for_leader(&cache, election_time * 3).await;
        assert!(became_leader, "Node should become leader");

        // Write data
        cache
            .put("noop-key1", "noop-value1")
            .await
            .expect("Put should succeed");
        cache
            .put("noop-key2", "noop-value2")
            .await
            .expect("Put should succeed");

        // Read and verify
        let val1 = cache.get(b"noop-key1").await;
        assert_eq!(val1, Some(bytes::Bytes::from("noop-value1")));

        let val2 = cache.get(b"noop-key2").await;
        assert_eq!(val2, Some(bytes::Bytes::from("noop-value2")));

        // Delete
        cache.delete("noop-key1").await.expect("Delete should succeed");
        assert!(cache.get(b"noop-key1").await.is_none());

        cache.shutdown().await;
    }

    /// TC-D3: Three node cluster with NoOp discovery (using seed_nodes)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_d3_noop_three_node_cluster() {
        let base_port = allocate_ports(3);
        let election_time = Duration::from_millis(500);

        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        // Start all three nodes with NoOp discovery
        let config1 = noop_discovery_cluster_config(1, base_port, peer_configs.clone());
        let config2 = noop_discovery_cluster_config(2, base_port + 1, peer_configs.clone());
        let config3 = noop_discovery_cluster_config(3, base_port + 2, peer_configs.clone());

        let cache1 = DistributedCache::new(config1)
            .await
            .expect("Failed to create cache 1");
        let cache2 = DistributedCache::new(config2)
            .await
            .expect("Failed to create cache 2");
        let cache3 = DistributedCache::new(config3)
            .await
            .expect("Failed to create cache 3");

        // Verify all use NoOp discovery
        assert!(!cache1.discovery_enabled(), "Cache 1 should use NoOp");
        assert!(!cache2.discovery_enabled(), "Cache 2 should use NoOp");
        assert!(!cache3.discovery_enabled(), "Cache 3 should use NoOp");

        // Wait for leader election
        let leader_elected = wait_for(
            || cache1.is_leader() || cache2.is_leader() || cache3.is_leader(),
            election_time * 10,
            Duration::from_millis(50),
        )
        .await;
        assert!(leader_elected, "A leader should be elected");

        // Find the leader and write data
        let leader: &DistributedCache = if cache1.is_leader() {
            &cache1
        } else if cache2.is_leader() {
            &cache2
        } else {
            &cache3
        };

        leader
            .put("noop-cluster-key", "noop-cluster-value")
            .await
            .expect("Write should succeed");

        // Wait for replication
        sleep(Duration::from_millis(500)).await;

        // All nodes should have the data
        let val1 = cache1.get(b"noop-cluster-key").await;
        let val2 = cache2.get(b"noop-cluster-key").await;
        let val3 = cache3.get(b"noop-cluster-key").await;

        assert_eq!(val1, Some(bytes::Bytes::from("noop-cluster-value")));
        assert_eq!(val2, Some(bytes::Bytes::from("noop-cluster-value")));
        assert_eq!(val3, Some(bytes::Bytes::from("noop-cluster-value")));

        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    /// TC-D4: NoOp discovery cluster - leader failover
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_d4_noop_cluster_leader_failover() {
        let base_port = allocate_ports(3);
        let election_time = Duration::from_millis(500);

        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        let config1 = noop_discovery_cluster_config(1, base_port, peer_configs.clone());
        let config2 = noop_discovery_cluster_config(2, base_port + 1, peer_configs.clone());
        let config3 = noop_discovery_cluster_config(3, base_port + 2, peer_configs.clone());

        let cache1 = DistributedCache::new(config1)
            .await
            .expect("Failed to create cache 1");
        let cache2 = DistributedCache::new(config2)
            .await
            .expect("Failed to create cache 2");
        let cache3 = DistributedCache::new(config3)
            .await
            .expect("Failed to create cache 3");

        // Wait for initial leader
        let leader_elected = wait_for(
            || cache1.is_leader() || cache2.is_leader() || cache3.is_leader(),
            election_time * 10,
            Duration::from_millis(50),
        )
        .await;
        assert!(leader_elected, "Initial leader should be elected");

        // Find and shutdown the leader
        let (leader_id, remaining_caches): (NodeId, Vec<&DistributedCache>) = if cache1.is_leader()
        {
            cache1.shutdown().await;
            (1, vec![&cache2, &cache3])
        } else if cache2.is_leader() {
            cache2.shutdown().await;
            (2, vec![&cache1, &cache3])
        } else {
            cache3.shutdown().await;
            (3, vec![&cache1, &cache2])
        };

        // Wait for new leader election among remaining nodes
        let new_leader_elected = wait_for(
            || remaining_caches.iter().any(|c| c.is_leader()),
            election_time * 10,
            Duration::from_millis(50),
        )
        .await;

        assert!(
            new_leader_elected,
            "New leader should be elected after old leader shutdown"
        );

        // Verify new leader is not the old leader
        let new_leader = remaining_caches.iter().find(|c| c.is_leader()).unwrap();
        assert_ne!(
            new_leader.node_id(),
            leader_id,
            "New leader should be different node"
        );

        // Cleanup remaining caches
        for cache in remaining_caches {
            if cache.node_id() != leader_id {
                // Only shutdown if not already shutdown
                cache.shutdown().await;
            }
        }
    }

    // ========================================================================
    // Static Discovery Tests
    // ========================================================================

    /// TC-D5: Single node with Static discovery
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_d5_static_single_node() {
        let base_port = allocate_ports(1);
        let raft_addr: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();

        // Create cache config with static discovery
        let static_config = StaticDiscoveryConfig::new(vec![(1, raft_addr)]).without_health_checks();
        let discovery = StaticClusterDiscovery::new(1, raft_addr, static_config);

        let config = CacheConfig::new(1, raft_addr)
            .with_cluster_discovery(discovery)
            .with_raft_config(RaftConfig {
                election_tick: 5,
                heartbeat_tick: 2,
                tick_interval_ms: 100,
                ..Default::default()
            });

        let election_time = election_timeout(&config);

        let cache = DistributedCache::new(config)
            .await
            .expect("Failed to create cache");

        // Verify static discovery is active
        assert!(cache.discovery_enabled(), "Static discovery should be active");

        // Wait for leader
        let became_leader = wait_for_leader(&cache, election_time * 3).await;
        assert!(became_leader, "Node should become leader");

        // Test read/write
        cache
            .put("static-key", "static-value")
            .await
            .expect("Put should succeed");

        let val = cache.get(b"static-key").await;
        assert_eq!(val, Some(bytes::Bytes::from("static-value")));

        cache.shutdown().await;
    }

    /// TC-D6: Three node cluster with Static discovery
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_d6_static_three_node_cluster() {
        let base_port = allocate_ports(3);
        let election_time = Duration::from_millis(500);

        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        // Create cache configs with static discovery
        let config1 = static_discovery_config(1, base_port, peer_configs.clone());
        let config2 = static_discovery_config(2, base_port + 1, peer_configs.clone());
        let config3 = static_discovery_config(3, base_port + 2, peer_configs.clone());

        let cache1 = DistributedCache::new(config1)
            .await
            .expect("Cache 1 create");
        let cache2 = DistributedCache::new(config2)
            .await
            .expect("Cache 2 create");
        let cache3 = DistributedCache::new(config3)
            .await
            .expect("Cache 3 create");

        // Verify static discovery is active
        assert!(cache1.discovery_enabled(), "Cache 1 should use static discovery");
        assert!(cache2.discovery_enabled(), "Cache 2 should use static discovery");
        assert!(cache3.discovery_enabled(), "Cache 3 should use static discovery");

        // Wait for leader
        let leader_elected = wait_for(
            || cache1.is_leader() || cache2.is_leader() || cache3.is_leader(),
            election_time * 10,
            Duration::from_millis(50),
        )
        .await;
        assert!(leader_elected, "Leader should be elected");

        // Write through leader
        let leader: &DistributedCache = if cache1.is_leader() {
            &cache1
        } else if cache2.is_leader() {
            &cache2
        } else {
            &cache3
        };

        leader
            .put("static-cluster-key", "static-cluster-value")
            .await
            .expect("Put succeed");

        // Wait for replication
        sleep(Duration::from_millis(500)).await;

        // Verify all nodes have data
        assert_eq!(
            cache1.get(b"static-cluster-key").await,
            Some(bytes::Bytes::from("static-cluster-value"))
        );
        assert_eq!(
            cache2.get(b"static-cluster-key").await,
            Some(bytes::Bytes::from("static-cluster-value"))
        );
        assert_eq!(
            cache3.get(b"static-cluster-key").await,
            Some(bytes::Bytes::from("static-cluster-value"))
        );

        // Cleanup
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    /// TC-D7: Static discovery - dynamic peer addition
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_d7_static_dynamic_peer_addition() {
        let base_port = allocate_ports(2);

        let addr1: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();
        let addr2: SocketAddr = format!("127.0.0.1:{}", base_port + 1).parse().unwrap();

        // Start with just node 1
        let static_config = StaticDiscoveryConfig::new(vec![(1, addr1)]).without_health_checks();

        let mut discovery = StaticClusterDiscovery::new(1, addr1, static_config);
        discovery.start().await.expect("Discovery start");

        assert_eq!(discovery.members().len(), 1, "Initially 1 member");

        // Dynamically add node 2
        discovery.add_peer(2, addr2);

        assert_eq!(discovery.members().len(), 2, "Now 2 members");
        assert_eq!(discovery.get_node_addr(2), Some(addr2));

        // Check that join event was emitted
        let event = discovery.try_recv_event();
        assert!(
            matches!(event, Some(crate::cluster::ClusterEvent::NodeJoin { node_id: 2, .. })),
            "Should emit NodeJoin event"
        );

        // Remove node 2
        discovery.remove_peer(2);
        assert_eq!(discovery.members().len(), 1, "Back to 1 member");

        let event = discovery.try_recv_event();
        assert!(
            matches!(event, Some(crate::cluster::ClusterEvent::NodeLeave { node_id: 2 })),
            "Should emit NodeLeave event"
        );

        discovery.shutdown().await.ok();
    }

    /// TC-D8: Static discovery config from addresses
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_d8_static_config_from_addrs() {
        let base_port = allocate_ports(3);

        let addrs: Vec<SocketAddr> = vec![
            format!("127.0.0.1:{}", base_port).parse().unwrap(),
            format!("127.0.0.1:{}", base_port + 1).parse().unwrap(),
            format!("127.0.0.1:{}", base_port + 2).parse().unwrap(),
        ];

        // Create config from addresses (auto-assigns IDs 1, 2, 3)
        let static_config = StaticDiscoveryConfig::from_addrs(addrs.clone()).without_health_checks();

        assert_eq!(static_config.peers.len(), 3);
        assert_eq!(static_config.peers[0].0, 1); // Auto ID
        assert_eq!(static_config.peers[1].0, 2);
        assert_eq!(static_config.peers[2].0, 3);

        let mut discovery = StaticClusterDiscovery::new(1, addrs[0], static_config);
        discovery.start().await.expect("Start");

        // Should have all 3 members
        assert_eq!(discovery.members().len(), 3);
        assert_eq!(discovery.get_node_addr(1), Some(addrs[0]));
        assert_eq!(discovery.get_node_addr(2), Some(addrs[1]));
        assert_eq!(discovery.get_node_addr(3), Some(addrs[2]));

        discovery.shutdown().await.ok();
    }

    /// TC-D9: NoOp vs Static discovery comparison - both work for single node
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_d9_noop_vs_static_single_node_comparison() {
        let base_port = allocate_ports(2);

        // Test 1: NoOp discovery
        let config_noop = noop_discovery_config(1, base_port);
        let election_time = election_timeout(&config_noop);

        let cache_noop = DistributedCache::new(config_noop)
            .await
            .expect("NoOp cache");

        // Verify NoOp discovery is not active
        assert!(!cache_noop.discovery_enabled(), "NoOp discovery should not be active");

        let became_leader_noop = wait_for_leader(&cache_noop, election_time * 3).await;
        assert!(became_leader_noop, "NoOp: should become leader");

        cache_noop
            .put("test-key", "noop-value")
            .await
            .expect("NoOp: put");
        let val_noop = cache_noop.get(b"test-key").await;
        assert_eq!(val_noop, Some(bytes::Bytes::from("noop-value")));

        cache_noop.shutdown().await;

        // Small delay between tests
        sleep(Duration::from_millis(100)).await;

        // Test 2: Static discovery (same behavior expected)
        let addr2: SocketAddr = format!("127.0.0.1:{}", base_port + 1).parse().unwrap();

        let static_config = StaticDiscoveryConfig::new(vec![(2, addr2)]).without_health_checks();
        let discovery_static = StaticClusterDiscovery::new(2, addr2, static_config);

        let config_static = CacheConfig::new(2, addr2)
            .with_cluster_discovery(discovery_static)
            .with_raft_config(RaftConfig {
                election_tick: 5,
                heartbeat_tick: 2,
                tick_interval_ms: 100,
                ..Default::default()
            });

        let cache_static = DistributedCache::new(config_static)
            .await
            .expect("Static cache");

        // Verify static discovery is active
        assert!(cache_static.discovery_enabled(), "Static discovery should be active");

        let became_leader_static = wait_for_leader(&cache_static, election_time * 3).await;
        assert!(became_leader_static, "Static: should become leader");

        cache_static
            .put("test-key", "static-value")
            .await
            .expect("Static: put");
        let val_static = cache_static.get(b"test-key").await;
        assert_eq!(val_static, Some(bytes::Bytes::from("static-value")));

        cache_static.shutdown().await;
    }

    /// TC-D10: Multiple writes and reads with NoOp discovery cluster
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_d10_noop_cluster_multiple_writes() {
        let base_port = allocate_ports(3);
        let election_time = Duration::from_millis(500);

        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        let config1 = noop_discovery_cluster_config(1, base_port, peer_configs.clone());
        let config2 = noop_discovery_cluster_config(2, base_port + 1, peer_configs.clone());
        let config3 = noop_discovery_cluster_config(3, base_port + 2, peer_configs.clone());

        let cache1 = DistributedCache::new(config1).await.expect("Cache 1");
        let cache2 = DistributedCache::new(config2).await.expect("Cache 2");
        let cache3 = DistributedCache::new(config3).await.expect("Cache 3");

        // Wait for leader
        let leader_elected = wait_for(
            || cache1.is_leader() || cache2.is_leader() || cache3.is_leader(),
            election_time * 10,
            Duration::from_millis(50),
        )
        .await;
        assert!(leader_elected, "Leader elected");

        let leader: &DistributedCache = if cache1.is_leader() {
            &cache1
        } else if cache2.is_leader() {
            &cache2
        } else {
            &cache3
        };

        // Write multiple entries
        let num_entries = 20;
        for i in 0..num_entries {
            let key = format!("noop-key-{}", i);
            let value = format!("noop-value-{}", i);
            leader
                .put(bytes::Bytes::from(key), bytes::Bytes::from(value))
                .await
                .expect("Put should succeed");
        }

        // Wait for replication
        sleep(Duration::from_millis(500)).await;

        // Verify all entries on all nodes
        for i in 0..num_entries {
            let key = format!("noop-key-{}", i);
            let expected = format!("noop-value-{}", i);

            assert_eq!(
                cache1.get(key.as_bytes()).await,
                Some(bytes::Bytes::from(expected.clone())),
                "Cache 1 should have key {}",
                i
            );
            assert_eq!(
                cache2.get(key.as_bytes()).await,
                Some(bytes::Bytes::from(expected.clone())),
                "Cache 2 should have key {}",
                i
            );
            assert_eq!(
                cache3.get(key.as_bytes()).await,
                Some(bytes::Bytes::from(expected)),
                "Cache 3 should have key {}",
                i
            );
        }

        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }
}
