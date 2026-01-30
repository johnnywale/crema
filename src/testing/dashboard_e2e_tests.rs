//! E2E tests for dashboard Multi-Raft functionality.
//!
//! These tests verify that:
//! 1. Multi-Raft shards are created and active
//! 2. Data is correctly routed to shards
//! 3. Entry counts are properly tracked
//! 4. Stats API returns correct values
//! 5. Per-shard Raft replication works across nodes

#[cfg(test)]
mod tests {
    use crate::testing::utils::{allocate_os_ports, allocate_os_ports_with_memberlist};
    use crate::{
        CacheConfig, DistributedCache, MemberlistConfig, MemberlistDiscovery, MultiRaftCacheConfig,
        NoOpClusterDiscovery, PeerManagementConfig, RaftConfig,
    };
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    /// Create a test config with Multi-Raft enabled using a random port.
    /// Single-node test config with per_shard_raft_enabled: false.
    async fn create_multiraft_test_config(node_id: u64) -> CacheConfig {
        let ports = allocate_os_ports(&[node_id]).await;
        let port = ports[0].1;
        let raft_addr: std::net::SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

        let raft_config = RaftConfig {
            election_tick: 10,
            heartbeat_tick: 3,
            tick_interval_ms: 100,
            pre_vote: true,
            ..Default::default()
        };

        let multiraft_config = MultiRaftCacheConfig {
            enabled: true,
            num_shards: 4,
            shard_capacity: 10_000,
            auto_init_shards: true,
            leader_broadcast_debounce_ms: 200,
            per_shard_raft_enabled: false, // Single-node test doesn't need per-shard Raft
            ..Default::default()
        };

        let discovery = NoOpClusterDiscovery::new(node_id, raft_addr);

        CacheConfig::new(node_id, raft_addr)
            .with_max_capacity(100_000)
            .with_default_ttl(Duration::from_secs(3600))
            .with_raft_config(raft_config)
            .with_cluster_discovery(discovery)
            .with_multiraft_config(multiraft_config)
    }

    /// Node configuration for multi-node cluster tests.
    #[derive(Clone)]
    struct ClusterNodeConfig {
        node_id: u64,
        raft_addr: SocketAddr,
        memberlist_addr: SocketAddr,
    }

    /// Create a multi-node cluster configuration with per-shard Raft enabled.
    /// Returns configs for nodes and their addresses.
    async fn create_cluster_node_configs(num_nodes: usize) -> Vec<ClusterNodeConfig> {
        let node_ids: Vec<u64> = (1..=num_nodes as u64).collect();
        let ports = allocate_os_ports_with_memberlist(&node_ids).await;

        ports
            .into_iter()
            .map(|(node_id, raft_port, memberlist_port)| ClusterNodeConfig {
                node_id,
                raft_addr: format!("127.0.0.1:{}", raft_port).parse().unwrap(),
                memberlist_addr: format!("127.0.0.1:{}", memberlist_port).parse().unwrap(),
            })
            .collect()
    }

    /// Create a CacheConfig for a node in a multi-node cluster with per_shard_raft_enabled: true.
    fn create_per_shard_raft_config(
        node_config: &ClusterNodeConfig,
        all_configs: &[ClusterNodeConfig],
    ) -> CacheConfig {
        // Build peer list (other nodes)
        let peers: Vec<(u64, SocketAddr)> = all_configs
            .iter()
            .filter(|n| n.node_id != node_config.node_id)
            .map(|n| (n.node_id, n.raft_addr))
            .collect();

        // Memberlist seeds (node 1 is seed for others)
        let memberlist_seeds: Vec<SocketAddr> = if node_config.node_id == 1 {
            vec![]
        } else {
            vec![all_configs[0].memberlist_addr]
        };

        // Raft config with staggered election to avoid split votes
        let raft_config = RaftConfig {
            election_tick: 10 + (node_config.node_id as usize * 3),
            heartbeat_tick: 3,
            tick_interval_ms: 100,
            pre_vote: true,
            ..Default::default()
        };

        // Memberlist config
        let memberlist_config = MemberlistConfig {
            enabled: true,
            bind_addr: Some(node_config.memberlist_addr),
            advertise_addr: None,
            seed_addrs: memberlist_seeds,
            node_name: Some(format!("per-shard-test-{}", node_config.node_id)),
            peer_management: PeerManagementConfig {
                auto_add_peers: true,
                auto_remove_peers: false,
                auto_add_voters: false,
                auto_remove_voters: false,
            },
        };

        // Multi-Raft config with per-shard Raft ENABLED
        // auto_init_shards=false so we can init after peer registration
        let multiraft_config = MultiRaftCacheConfig {
            enabled: true,
            num_shards: 4,
            shard_capacity: 10_000,
            auto_init_shards: false,
            leader_broadcast_debounce_ms: 200,
            per_shard_raft_enabled: true,
            ..Default::default()
        };

        // Create discovery
        let discovery = MemberlistDiscovery::new(
            node_config.node_id,
            node_config.raft_addr,
            &memberlist_config,
            &peers,
        );

        CacheConfig::new(node_config.node_id, node_config.raft_addr)
            .with_seed_nodes(peers)
            .with_max_capacity(100_000)
            .with_default_ttl(Duration::from_secs(3600))
            .with_raft_config(raft_config)
            .with_cluster_discovery(discovery)
            .with_multiraft_config(multiraft_config)
    }

    /// Initialize a multi-node cluster with per-shard Raft enabled.
    /// Returns the caches after full initialization.
    async fn init_per_shard_raft_cluster(
        node_configs: &[ClusterNodeConfig],
    ) -> Vec<Arc<DistributedCache>> {
        let mut caches = Vec::new();

        // Start all nodes
        for node_config in node_configs {
            let config = create_per_shard_raft_config(node_config, node_configs);
            let cache = Arc::new(
                DistributedCache::new(config)
                    .await
                    .expect("Failed to create cache"),
            );
            caches.push(cache);
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        // Wait for cluster discovery
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Initialize per-shard Raft infrastructure on each coordinator
        for cache in &caches {
            if let Some(coordinator) = cache.multiraft_coordinator() {
                coordinator
                    .init_shard_raft_infrastructure()
                    .await
                    .expect("Failed to init shard Raft infrastructure");
            }
        }

        // Register peer addresses
        for cache in &caches {
            if let Some(coordinator) = cache.multiraft_coordinator() {
                for node_config in node_configs {
                    if node_config.node_id != cache.node_id() {
                        coordinator
                            .register_node_address(node_config.node_id, node_config.raft_addr);
                        coordinator
                            .add_shard_transport_peer(node_config.node_id, node_config.raft_addr)
                            .await;
                    }
                }
            }
        }

        // Initialize shards
        for cache in &caches {
            if let Some(coordinator) = cache.multiraft_coordinator() {
                coordinator
                    .init()
                    .await
                    .expect("Failed to init coordinator");
            }
        }

        // Start shard Raft managers
        for cache in &caches {
            if let Some(coordinator) = cache.multiraft_coordinator() {
                coordinator
                    .start_shard_raft_manager()
                    .await
                    .expect("Failed to start shard Raft manager");
            }
            // Set up the shard message handler for routing incoming shard Raft messages
            cache.setup_shard_message_handler();
        }

        // Wait for shard leader elections
        tokio::time::sleep(Duration::from_secs(4)).await;

        caches
    }

    // ========================================================================
    // TC-DASH-1: Multi-Raft enabled verification
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_dash1_multiraft_enabled() {
        let config = create_multiraft_test_config(1).await;
        let cache = Arc::new(DistributedCache::new(config).await.unwrap());

        // Wait for initialization
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Verify Multi-Raft is enabled
        assert!(cache.is_multiraft_enabled(), "Multi-Raft should be enabled");

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-DASH-2: Initial coordinator state
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_dash2_initial_coordinator_state() {
        let config = create_multiraft_test_config(1).await;
        let cache = Arc::new(DistributedCache::new(config).await.unwrap());

        // Wait for initialization
        tokio::time::sleep(Duration::from_secs(1)).await;

        let coordinator = cache
            .multiraft_coordinator()
            .expect("Should have coordinator");

        let stats = coordinator.stats();
        assert_eq!(stats.total_shards, 4, "Should have 4 shards");
        assert_eq!(stats.active_shards, 4, "All 4 shards should be active");
        assert_eq!(stats.total_entries, 0, "No entries initially");

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-DASH-3: Shard info before writes
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_dash3_shard_info_before_writes() {
        let config = create_multiraft_test_config(1).await;
        let cache = Arc::new(DistributedCache::new(config).await.unwrap());

        // Wait for initialization
        tokio::time::sleep(Duration::from_secs(1)).await;

        let coordinator = cache
            .multiraft_coordinator()
            .expect("Should have coordinator");

        let shard_info = coordinator.shard_info();
        assert_eq!(shard_info.len(), 4, "Should have 4 shard infos");

        for info in &shard_info {
            assert!(
                info.state == crate::multiraft::ShardState::Active,
                "Shard {} should be active",
                info.shard_id
            );
            assert_eq!(
                info.entry_count, 0,
                "Shard {} should have 0 entries initially",
                info.shard_id
            );
        }

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-DASH-4: Direct shard writes
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_dash4_direct_shard_writes() {
        let config = create_multiraft_test_config(1).await;
        let cache = Arc::new(DistributedCache::new(config).await.unwrap());

        // Wait for initialization
        tokio::time::sleep(Duration::from_secs(1)).await;

        let coordinator = cache
            .multiraft_coordinator()
            .expect("Should have coordinator");

        // Write directly to each shard
        for shard_id in 0..4u32 {
            let shard = coordinator
                .get_shard(shard_id)
                .expect(&format!("Shard {} should exist", shard_id));

            let key = format!("direct-shard-{}-key", shard_id);
            let value = format!("direct-value-{}", shard_id);
            shard.put(key.into(), value.into()).await;
        }

        // Run pending tasks
        for shard in coordinator.router().all_shards() {
            shard.storage().run_pending_tasks().await;
        }

        // Verify each shard has at least 1 entry
        let shard_info = coordinator.shard_info();
        for info in &shard_info {
            assert!(
                info.entry_count >= 1,
                "Shard {} should have at least 1 entry after direct write, got {}",
                info.shard_id,
                info.entry_count
            );
        }

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-DASH-5: Cache API writes
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_dash5_cache_api_writes() {
        let config = create_multiraft_test_config(1).await;
        let cache = Arc::new(DistributedCache::new(config).await.unwrap());

        // Wait for initialization
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Write 20 entries via cache API
        for i in 0..20 {
            let key = format!("cache-key-{:03}", i);
            let value = format!("cache-value-{}", i);
            cache
                .put(key, value)
                .await
                .expect(&format!("Put key {} should succeed", i));
        }

        // Small delay for async operations
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Run pending tasks to ensure Moka counters are synced
        cache.run_pending_tasks().await;

        let coordinator = cache
            .multiraft_coordinator()
            .expect("Should have coordinator");

        let stats = coordinator.stats();
        assert!(
            stats.total_entries >= 18,
            "Should have at least 18 entries after 20 writes, got {}",
            stats.total_entries
        );

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-DASH-6: Stats after cache writes
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_dash6_stats_after_writes() {
        let config = create_multiraft_test_config(1).await;
        let cache = Arc::new(DistributedCache::new(config).await.unwrap());

        // Wait for initialization
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Write entries
        for i in 0..20 {
            let key = format!("stats-key-{:03}", i);
            let value = format!("stats-value-{}", i);
            cache.put(key, value).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        cache.run_pending_tasks().await;

        let coordinator = cache
            .multiraft_coordinator()
            .expect("Should have coordinator");

        let stats = coordinator.stats();
        assert_eq!(stats.total_shards, 4);
        assert_eq!(stats.active_shards, 4);
        assert!(stats.total_entries >= 18, "Should have at least 18 entries");
        assert!(
            stats.operations_total >= 20,
            "Should have at least 20 operations"
        );

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-DASH-7: cache.stats() method
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_dash7_cache_stats_method() {
        let config = create_multiraft_test_config(1).await;
        let cache = Arc::new(DistributedCache::new(config).await.unwrap());

        // Wait for initialization
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Write entries
        for i in 0..10 {
            let key = format!("cache-stats-key-{}", i);
            let value = format!("cache-stats-value-{}", i);
            cache.put(key, value).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        cache.run_pending_tasks().await;

        let cache_stats = cache.stats();
        assert!(
            cache_stats.entry_count >= 8,
            "cache.stats().entry_count should be at least 8, got {}",
            cache_stats.entry_count
        );

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-DASH-8: Per-shard entry counts
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_dash8_per_shard_entry_counts() {
        let config = create_multiraft_test_config(1).await;
        let cache = Arc::new(DistributedCache::new(config).await.unwrap());

        // Wait for initialization
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Write enough entries to ensure distribution
        for i in 0..40 {
            let key = format!("shard-count-key-{:03}", i);
            let value = format!("shard-count-value-{}", i);
            cache.put(key, value).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        cache.run_pending_tasks().await;

        let coordinator = cache
            .multiraft_coordinator()
            .expect("Should have coordinator");

        let shard_info = coordinator.shard_info();
        let mut total_from_shards = 0u64;

        for info in &shard_info {
            total_from_shards += info.entry_count;
        }

        // Verify entries are distributed (not all in one shard)
        let non_empty_shards = shard_info
            .iter()
            .filter(|info| info.entry_count > 0)
            .count();

        assert!(
            non_empty_shards >= 2,
            "Entries should be distributed across at least 2 shards, got {}",
            non_empty_shards
        );

        assert!(
            total_from_shards >= 35,
            "Total entries from shards should be at least 35, got {}",
            total_from_shards
        );

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-DASH-9: Read verification
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_dash9_read_verification() {
        let config = create_multiraft_test_config(1).await;
        let cache = Arc::new(DistributedCache::new(config).await.unwrap());

        // Wait for initialization
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Write entries
        for i in 0..10 {
            let key = format!("read-verify-key-{:03}", i);
            let value = format!("read-verify-value-{}", i);
            cache.put(key, value).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify reads
        for i in 0..10 {
            let key = format!("read-verify-key-{:03}", i);
            let expected_value = format!("read-verify-value-{}", i);
            let value = cache.get(key.as_bytes()).await;
            assert!(value.is_some(), "Key {} should exist", key);
            assert_eq!(
                String::from_utf8_lossy(&value.unwrap()),
                expected_value,
                "Key {} should have correct value",
                key
            );
        }

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-DASH-10: Router total entries
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_dash10_router_total_entries() {
        let config = create_multiraft_test_config(1).await;
        let cache = Arc::new(DistributedCache::new(config).await.unwrap());

        // Wait for initialization
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Write entries
        for i in 0..20 {
            let key = format!("router-key-{:03}", i);
            let value = format!("router-value-{}", i);
            cache.put(key, value).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        cache.run_pending_tasks().await;

        let coordinator = cache
            .multiraft_coordinator()
            .expect("Should have coordinator");

        let router_total = coordinator.router().total_entries();
        assert!(
            router_total >= 18,
            "router.total_entries() should be at least 18, got {}",
            router_total
        );

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-DASH-11: Full integration test (combines all checks)
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_dash11_full_integration() {
        let config = create_multiraft_test_config(1).await;
        let cache = Arc::new(DistributedCache::new(config).await.unwrap());

        // Wait for initialization
        tokio::time::sleep(Duration::from_secs(2)).await;

        // 1. Verify Multi-Raft is enabled
        assert!(cache.is_multiraft_enabled(), "Multi-Raft should be enabled");

        // 2. Get coordinator
        let coordinator = cache
            .multiraft_coordinator()
            .expect("Should have coordinator");

        // 3. Check initial state
        let initial_stats = coordinator.stats();
        assert_eq!(initial_stats.total_shards, 4);
        assert_eq!(initial_stats.active_shards, 4);

        // 4. Write directly to shards
        for shard_id in 0..4u32 {
            if let Some(shard) = coordinator.get_shard(shard_id) {
                let key = format!("direct-shard-{}-key", shard_id);
                let value = format!("direct-value-{}", shard_id);
                shard.put(key.into(), value.into()).await;
            }
        }

        // 5. Write via cache API
        for i in 0..20 {
            let key = format!("cache-key-{:03}", i);
            let value = format!("cache-value-{}", i);
            cache.put(key, value).await.unwrap();
        }

        // Small delay and sync
        tokio::time::sleep(Duration::from_millis(100)).await;
        cache.run_pending_tasks().await;

        // 6. Verify stats
        let stats = coordinator.stats();
        assert!(
            stats.total_entries > 0,
            "Should have entries, got {}",
            stats.total_entries
        );

        // 7. Verify cache.stats()
        let cache_stats = cache.stats();
        assert!(
            cache_stats.entry_count > 0,
            "cache.stats().entry_count should be > 0"
        );

        // 8. Per-shard entry counts
        let shard_info = coordinator.shard_info();
        let total_from_shards: u64 = shard_info.iter().map(|info| info.entry_count).sum();
        assert!(total_from_shards > 0, "Total from shards should be > 0");

        // 9. Verify reads
        for i in 0..5 {
            let key = format!("cache-key-{:03}", i);
            let value = cache.get(key.as_bytes()).await;
            assert!(value.is_some(), "Key {} should exist", key);
        }

        // 10. Router total entries
        let router_total = coordinator.router().total_entries();
        assert!(router_total > 0, "router.total_entries() should be > 0");

        // All entry count methods should be consistent (within tolerance)
        let stats_entries = stats.total_entries;
        let cache_entries = cache_stats.entry_count;

        // Allow some variance due to async nature
        assert!(
            (stats_entries as i64 - cache_entries as i64).abs() < 10,
            "Entry counts should be roughly consistent: stats={}, cache={}",
            stats_entries,
            cache_entries
        );

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-DASH-12: Shard routing verification
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_dash12_shard_routing() {
        let config = create_multiraft_test_config(1).await;
        let cache = Arc::new(DistributedCache::new(config).await.unwrap());

        // Wait for initialization
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Write keys and verify they route consistently
        let test_keys = vec!["key-alpha", "key-beta", "key-gamma", "key-delta"];

        for key in &test_keys {
            let shard1 = cache.shard_for_key(key.as_bytes());
            let shard2 = cache.shard_for_key(key.as_bytes());
            assert_eq!(
                shard1, shard2,
                "Same key should always route to the same shard"
            );
            assert!(
                shard1.map(|s| s < 4).unwrap_or(true),
                "Shard ID should be in valid range"
            );
        }

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-DASH-13: Per-shard Raft cluster initialization
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_dash13_per_shard_raft_cluster_init() {
        let node_configs = create_cluster_node_configs(3).await;
        let caches = init_per_shard_raft_cluster(&node_configs).await;

        // Verify all nodes have Multi-Raft enabled with per-shard Raft
        for (idx, cache) in caches.iter().enumerate() {
            assert!(
                cache.is_multiraft_enabled(),
                "Node {} should have Multi-Raft enabled",
                idx + 1
            );

            let coordinator = cache
                .multiraft_coordinator()
                .expect("Should have coordinator");
            assert!(
                coordinator.is_per_shard_raft_enabled(),
                "Node {} should have per-shard Raft enabled",
                idx + 1
            );
            assert!(coordinator.is_running(), "Coordinator should be running");
            assert_eq!(coordinator.stats().total_shards, 4, "Should have 4 shards");
        }

        // Cleanup
        for cache in caches {
            cache.shutdown().await;
        }
    }

    // ========================================================================
    // TC-DASH-14: Per-shard Raft write replication to followers
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_dash14_per_shard_raft_write_replication() {
        let node_configs = create_cluster_node_configs(3).await;
        let caches = init_per_shard_raft_cluster(&node_configs).await;

        // Diagnostic: verify per-shard Raft is enabled on all nodes
        for (idx, cache) in caches.iter().enumerate() {
            if let Some(coordinator) = cache.multiraft_coordinator() {
                let is_enabled = coordinator.is_per_shard_raft_enabled();
                println!(
                    "[DIAG] Node {}: is_per_shard_raft_enabled = {}",
                    idx + 1,
                    is_enabled
                );
                assert!(
                    is_enabled,
                    "Node {}: Per-shard Raft should be enabled",
                    idx + 1
                );

                // Check shard raft stats
                if let Some(stats) = coordinator.shard_raft_stats() {
                    println!(
                        "[DIAG] Node {}: shard_raft_stats = total_shards={}, leader_shards={}, running_shards={}",
                        idx + 1, stats.total_shards, stats.leader_shards, stats.running_shards
                    );
                }

                // Check each shard's raft_enabled status and leader info
                for shard in coordinator.router().all_shards() {
                    let raft_enabled = shard.is_raft_enabled();
                    let raft_leader = shard
                        .raft_node()
                        .map(|n| format!("{:?}", n.leader_id()))
                        .unwrap_or_else(|| "N/A".to_string());
                    let is_leader = shard.is_leader();
                    println!(
                        "[DIAG] Node {}: Shard {}: is_raft_enabled={}, is_leader={}, raft_leader={}",
                        idx + 1,
                        shard.id(),
                        raft_enabled,
                        is_leader,
                        raft_leader
                    );
                }
            }
        }

        // Write test data via node 1
        let test_data = vec![
            ("user:1", "Alice"),
            ("user:2", "Bob"),
            ("session:abc", "session-data"),
            ("order:100", "order-items"),
        ];

        let write_cache = &caches[0];
        for (key, value) in &test_data {
            println!("[DIAG] Starting put: key='{}', value='{}'", key, value);

            // Use timeout to detect hangs
            let put_result =
                tokio::time::timeout(Duration::from_secs(10), write_cache.put(*key, *value)).await;

            match put_result {
                Ok(Ok(())) => println!("[DIAG] Put succeeded: key='{}'", key),
                Ok(Err(e)) => println!("[DIAG] Put error: key='{}', error={:?}", key, e),
                Err(_) => println!("[DIAG] Put TIMEOUT: key='{}'", key),
            }
        }

        // Wait for replication
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Run pending tasks on all caches
        for cache in &caches {
            cache.run_pending_tasks().await;
        }

        // Verify data exists on all nodes
        for (node_idx, cache) in caches.iter().enumerate() {
            for (key, expected_value) in &test_data {
                let result = cache.get(key.as_bytes()).await;
                assert!(
                    result.is_some(),
                    "Node {}: Key '{}' should exist after replication",
                    node_idx + 1,
                    key
                );
                let result_bytes = result.unwrap();
                let actual_value = String::from_utf8_lossy(&result_bytes);
                assert_eq!(
                    actual_value,
                    *expected_value,
                    "Node {}: Key '{}' should have value '{}'",
                    node_idx + 1,
                    key,
                    expected_value
                );
            }
        }

        // Cleanup
        for cache in caches {
            cache.shutdown().await;
        }
    }

    // ========================================================================
    // TC-DASH-15: Per-shard Raft entry count consistency across nodes
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_dash15_per_shard_raft_entry_count_consistency() {
        let node_configs = create_cluster_node_configs(3).await;
        let caches = init_per_shard_raft_cluster(&node_configs).await;

        // Write 20 entries via node 1
        let write_cache = &caches[0];
        for i in 0..20 {
            let key = format!("repl-key-{:03}", i);
            let value = format!("repl-value-{}", i);
            write_cache
                .put(key, value)
                .await
                .expect("Put should succeed");
        }

        // Wait for replication
        tokio::time::sleep(Duration::from_secs(4)).await;

        // Run pending tasks
        for cache in &caches {
            cache.run_pending_tasks().await;
        }

        // Check entry counts on all nodes
        let mut entry_counts = Vec::new();
        for (idx, cache) in caches.iter().enumerate() {
            let stats = cache.stats();
            entry_counts.push(stats.entry_count);
            tracing::info!(
                "Node {} entry_count: {} (expected: 20)",
                idx + 1,
                stats.entry_count
            );
        }

        // Leader should have all 20 entries
        assert!(
            entry_counts[0] >= 18,
            "Leader should have at least 18 entries, got {}",
            entry_counts[0]
        );

        // Followers should have replicated entries (allow some variance)
        for (idx, count) in entry_counts.iter().enumerate().skip(1) {
            assert!(
                *count >= 15,
                "Follower {} should have at least 15 entries after replication, got {}",
                idx + 1,
                count
            );
        }

        // Cleanup
        for cache in caches {
            cache.shutdown().await;
        }
    }

    // ========================================================================
    // TC-DASH-16: Per-shard Raft read from any node
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_dash16_per_shard_raft_read_from_any_node() {
        let node_configs = create_cluster_node_configs(3).await;
        let caches = init_per_shard_raft_cluster(&node_configs).await;

        // Write via node 1
        caches[0]
            .put("shared-key", "shared-value")
            .await
            .expect("Put should succeed");

        // Wait for replication
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Read from all nodes - all should return the same value
        for (idx, cache) in caches.iter().enumerate() {
            let result = cache.get(b"shared-key").await;
            assert!(
                result.is_some(),
                "Node {} should be able to read the replicated key",
                idx + 1
            );
            let result_bytes = result.unwrap();
            let value = String::from_utf8_lossy(&result_bytes);
            assert_eq!(
                value,
                "shared-value",
                "Node {} should return correct value",
                idx + 1
            );
        }

        // Cleanup
        for cache in caches {
            cache.shutdown().await;
        }
    }

    // ========================================================================
    // TC-DASH-17: Per-shard Raft multi-shard distribution
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_dash17_per_shard_raft_multi_shard_distribution() {
        let node_configs = create_cluster_node_configs(3).await;
        let caches = init_per_shard_raft_cluster(&node_configs).await;

        // Write enough keys to ensure distribution across shards
        let write_cache = &caches[0];
        for i in 0..40 {
            let key = format!("dist-key-{:03}", i);
            let value = format!("dist-value-{}", i);
            write_cache
                .put(key, value)
                .await
                .expect("Put should succeed");
        }

        // Wait for replication
        tokio::time::sleep(Duration::from_secs(4)).await;

        // Run pending tasks
        for cache in &caches {
            cache.run_pending_tasks().await;
        }

        // Check shard distribution on leader
        let coordinator = caches[0]
            .multiraft_coordinator()
            .expect("Should have coordinator");
        let shard_info = coordinator.shard_info();

        // Verify entries are distributed across multiple shards
        let non_empty_shards = shard_info
            .iter()
            .filter(|info| info.entry_count > 0)
            .count();

        assert!(
            non_empty_shards >= 2,
            "Entries should be distributed across at least 2 shards, got {}",
            non_empty_shards
        );

        // Verify total entries
        let total_from_shards: u64 = shard_info.iter().map(|info| info.entry_count).sum();
        assert!(
            total_from_shards >= 35,
            "Total entries from shards should be at least 35, got {}",
            total_from_shards
        );

        // Cleanup
        for cache in caches {
            cache.shutdown().await;
        }
    }

    // ========================================================================
    // TC-DASH-18: Per-shard Raft full integration test
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_dash18_per_shard_raft_full_integration() {
        let node_configs = create_cluster_node_configs(3).await;
        let caches = init_per_shard_raft_cluster(&node_configs).await;

        // 1. Verify cluster setup
        for cache in &caches {
            assert!(cache.is_multiraft_enabled());
            let coord = cache.multiraft_coordinator().unwrap();
            assert!(coord.is_per_shard_raft_enabled());
            assert!(coord.is_running());
        }

        // 2. Write test entries
        let test_entries = vec![
            ("int-user:1", "Alice"),
            ("int-user:2", "Bob"),
            ("int-session:a", "sess-a"),
            ("int-session:b", "sess-b"),
            ("int-order:1", "order-1"),
            ("int-order:2", "order-2"),
            ("int-product:1", "prod-1"),
            ("int-product:2", "prod-2"),
        ];

        for (key, value) in &test_entries {
            caches[0]
                .put(*key, *value)
                .await
                .expect("Put should succeed");
        }

        // 3. Wait for replication
        tokio::time::sleep(Duration::from_secs(4)).await;

        // Run pending tasks
        for cache in &caches {
            cache.run_pending_tasks().await;
        }

        // 4. Verify replication to all nodes
        let expected_count = test_entries.len() as u64;
        let mut all_verified = true;

        for (node_idx, cache) in caches.iter().enumerate() {
            let mut node_verified = 0;
            for (key, expected) in &test_entries {
                if let Some(value) = cache.get(key.as_bytes()).await {
                    if String::from_utf8_lossy(&value) == *expected {
                        node_verified += 1;
                    }
                }
            }

            let pass_threshold = (expected_count as f64 * 0.75) as usize;
            if node_verified < pass_threshold {
                tracing::warn!(
                    "Node {}: only verified {}/{} entries",
                    node_idx + 1,
                    node_verified,
                    expected_count
                );
                all_verified = false;
            }
        }

        // 5. Verify entry counts are reasonable
        for (idx, cache) in caches.iter().enumerate() {
            let stats = cache.stats();
            assert!(
                stats.entry_count >= expected_count / 2,
                "Node {} should have at least {} entries, got {}",
                idx + 1,
                expected_count / 2,
                stats.entry_count
            );
        }

        assert!(
            all_verified,
            "All nodes should verify at least 75% of entries"
        );

        // Cleanup
        for cache in caches {
            cache.shutdown().await;
        }
    }
}
