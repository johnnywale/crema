//! Integration tests for DistributedCache with real network connections.
//!
//! These tests verify the full cache functionality including:
//! - Raft leader election
//! - Log replication
//! - Node restarts
//! - Multi-node cluster formation
//!
//! All tests use real network connections and actual cache instances.

#[cfg(test)]
mod tests {
    use crate::cache::DistributedCache;
    use crate::config::{CacheConfig, MemberlistConfig, RaftConfig};
    use crate::types::NodeId;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::time::{Duration, Instant};
    use tokio::time::sleep;

    /// Port counter to ensure unique ports across tests
    static PORT_COUNTER: AtomicU16 = AtomicU16::new(21000);

    /// Allocate a range of ports for a test
    fn allocate_ports(count: u16) -> u16 {
        PORT_COUNTER.fetch_add(count * 2, Ordering::SeqCst)
    }

    /// Create a cache config for a single node (standalone)
    fn single_node_config(node_id: NodeId, base_port: u16) -> CacheConfig {
        let raft_addr: SocketAddr = format!("127.0.0.1:{}", base_port).parse().unwrap();

        CacheConfig {
            node_id,
            raft_addr,
            seed_nodes: vec![],
            max_capacity: 10_000,
            default_ttl: Some(Duration::from_secs(3600)),
            default_tti: None,
            raft: RaftConfig {
                election_tick: 5,      // 5 ticks = 500ms with 100ms tick
                heartbeat_tick: 2,     // 2 ticks = 200ms
                tick_interval_ms: 100, // 100ms per tick
                max_size_per_msg: 1024 * 1024,
                max_inflight_msgs: 256,
                pre_vote: true,
                applied: 0,
            },
            membership: Default::default(),
            memberlist: MemberlistConfig::default(), // Disabled by default
            checkpoint: Default::default(),
        }
    }

    /// Create a cache config for a node in a multi-node cluster
    fn cluster_node_config(
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

        // 生产环境建议：
        // 基础选举超时设为 10-20 (1s - 2s)
        // 这样可以留出足够的网络往返时间（RTT）和磁盘 IO 时间
        let base_election_tick = 15;

        CacheConfig {
            node_id,
            raft_addr,
            seed_nodes,
            max_capacity: 10_000,
            default_ttl: Some(Duration::from_secs(3600)),
            default_tti: None,
            raft: RaftConfig {
                // 改进点：使用 100ms 作为基础 Tick
                tick_interval_ms: 100,
                election_tick: base_election_tick + (node_id as usize * 5),
                heartbeat_tick: 2,
                max_size_per_msg: 1024 * 1024,
                max_inflight_msgs: 256,
                pre_vote: true,
                applied: 0,
            },
            membership: Default::default(),
            memberlist: MemberlistConfig::default(),
            checkpoint: Default::default(),
        }
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

    /// Helper to get term from cache
    fn get_term(cache: &DistributedCache) -> u64 {
        cache.cluster_status().term
    }

    // ========================================================================
    // TC-1: Single node becomes leader after startup
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc1_single_node_becomes_leader() {
        let base_port = allocate_ports(1);
        let config = single_node_config(1, base_port);
        let election_time = election_timeout(&config);

        // Start the cache
        let cache = DistributedCache::new(config)
            .await
            .expect("Failed to create cache");

        // Initially should not be leader (starts as follower)
        // Give a tiny moment for initialization
        sleep(Duration::from_millis(10)).await;

        // Wait for election timeout + buffer
        let became_leader = wait_for_leader(&cache, election_time * 3).await;

        // Assertions
        assert!(became_leader, "Single node should become leader");
        assert!(cache.is_leader(), "Node should be leader");
        assert_eq!(cache.leader_id(), Some(1), "Leader ID should be self");
        assert!(get_term(&cache) >= 1, "Term should be at least 1");

        // Verify cluster status
        let status = cache.cluster_status();
        assert_eq!(status.node_id, 1);
        assert!(status.is_leader);
        assert_eq!(status.leader_id, Some(1));

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-2: Leader election happens within expected time
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc2_leader_election_within_expected_time() {
        let base_port = allocate_ports(1);
        let config = single_node_config(1, base_port);
        let election_time = election_timeout(&config);

        let start = Instant::now();

        // Start the cache
        let cache = DistributedCache::new(config)
            .await
            .expect("Failed to create cache");

        // Should NOT be leader immediately
        assert!(
            !cache.is_leader(),
            "Node should not be leader immediately on startup"
        );

        // Wait for leader
        let became_leader = wait_for_leader(&cache, election_time * 5).await;
        let elapsed = start.elapsed();

        assert!(became_leader, "Node should become leader");

        // Election should happen after election_timeout but within reasonable bounds
        // Note: There's some startup overhead, so we check a range
        let min_time = Duration::from_millis(100); // Some minimum startup time
        let max_time = election_time * 4; // Allow for jitter and processing

        assert!(
            elapsed >= min_time,
            "Election happened too fast: {:?} < {:?}",
            elapsed,
            min_time
        );
        assert!(
            elapsed <= max_time,
            "Election took too long: {:?} > {:?}",
            elapsed,
            max_time
        );

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-3: Node remains leader after election (stability)
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc3_node_remains_leader_stable() {
        let base_port = allocate_ports(1);
        let config = single_node_config(1, base_port);
        let election_time = election_timeout(&config);

        let cache = DistributedCache::new(config)
            .await
            .expect("Failed to create cache");

        // Wait until leader
        let became_leader = wait_for_leader(&cache, election_time * 3).await;
        assert!(became_leader, "Node should become leader");

        let initial_term = get_term(&cache);

        // Continue running for N × election_timeout
        let stability_period = election_time * 5;
        let check_interval = Duration::from_millis(100);
        let start = Instant::now();

        while start.elapsed() < stability_period {
            // Check stability
            assert!(cache.is_leader(), "Node should remain leader");
            let current_term = get_term(&cache);
            assert!(
                current_term <= initial_term + 1,
                "Term should not increase significantly (was {}, now {})",
                initial_term,
                current_term
            );

            sleep(check_interval).await;
        }

        // Final assertions
        assert!(
            cache.is_leader(),
            "Node should still be leader after stability period"
        );
        assert_eq!(cache.leader_id(), Some(1), "Leader ID should still be self");

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-4: Restart single node
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc4_restart_single_node() {
        let base_port = allocate_ports(1);
        let config = single_node_config(1, base_port);
        let election_time = election_timeout(&config);

        // First run
        let cache1 = DistributedCache::new(config.clone())
            .await
            .expect("Failed to create cache");
        let became_leader = wait_for_leader(&cache1, election_time * 3).await;
        assert!(became_leader, "Node should become leader on first run");

        let _term_before = get_term(&cache1);

        // Shutdown
        cache1.shutdown().await;
        sleep(Duration::from_millis(200)).await; // Allow cleanup

        // Restart with same config (same node ID, same port)
        let cache2 = DistributedCache::new(config)
            .await
            .expect("Failed to restart cache");

        // Should start as follower initially
        // (In-memory storage means state is lost, but that's expected)

        // Wait for leader again
        let became_leader_again = wait_for_leader(&cache2, election_time * 3).await;
        assert!(
            became_leader_again,
            "Node should become leader again after restart"
        );

        // Term should be >= 1 (in practice, starts fresh with in-memory storage)
        let term_after = get_term(&cache2);
        assert!(term_after >= 1, "Term should be at least 1 after restart");

        // Verify no corruption / panic occurred
        assert!(cache2.is_leader(), "Node should be leader after restart");
        assert_eq!(cache2.node_id(), 1, "Node ID should be preserved");

        cache2.shutdown().await;
    }

    // ========================================================================
    // TC-5: Single node with log writes
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc5_single_node_log_writes() {
        let base_port = allocate_ports(1);
        let config = single_node_config(1, base_port);
        let election_time = election_timeout(&config);

        let cache = DistributedCache::new(config)
            .await
            .expect("Failed to create cache");

        // Wait until leader
        let became_leader = wait_for_leader(&cache, election_time * 3).await;
        assert!(became_leader, "Node should become leader");

        // Get initial state
        let initial_commit = cache.cluster_status().commit_index;

        // Submit client commands
        let result1 = cache.put("key1", "value1").await;
        assert!(result1.is_ok(), "First put should succeed: {:?}", result1);

        let result2 = cache.put("key2", "value2").await;
        assert!(result2.is_ok(), "Second put should succeed: {:?}", result2);

        let result3 = cache
            .put_with_ttl("key3", "value3", Duration::from_secs(60))
            .await;
        assert!(
            result3.is_ok(),
            "Put with TTL should succeed: {:?}",
            result3
        );

        // Verify data is readable
        let val1 = cache.get(b"key1").await;
        assert_eq!(
            val1,
            Some(bytes::Bytes::from("value1")),
            "key1 should have value1"
        );

        let val2 = cache.get(b"key2").await;
        assert_eq!(
            val2,
            Some(bytes::Bytes::from("value2")),
            "key2 should have value2"
        );

        let val3 = cache.get(b"key3").await;
        assert_eq!(
            val3,
            Some(bytes::Bytes::from("value3")),
            "key3 should have value3"
        );

        // Verify commit index increased
        let final_status = cache.cluster_status();
        assert!(
            final_status.commit_index > initial_commit,
            "Commit index should increase after writes (was {}, now {})",
            initial_commit,
            final_status.commit_index
        );

        // Test delete
        let delete_result = cache.delete("key1").await;
        assert!(delete_result.is_ok(), "Delete should succeed");

        let val1_after = cache.get(b"key1").await;
        assert!(val1_after.is_none(), "key1 should be deleted");

        // Verify entry count (run pending tasks first to ensure count is accurate)
        cache.run_pending_tasks().await;
        assert!(cache.entry_count() >= 2, "Should have at least 2 entries");

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-6: Single node in multinode config - no quorum
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc6_single_node_in_multinode_config_no_quorum() {
        let base_port = allocate_ports(3);

        // Configure as if there are 3 nodes, but only start 1
        let peer_configs = vec![
            (1, base_port),
            (2, base_port + 1), // Not running
            (3, base_port + 2), // Not running
        ];

        let config = cluster_node_config(1, base_port, peer_configs);
        let election_time = election_timeout(&config);

        let cache = DistributedCache::new(config)
            .await
            .expect("Failed to create cache");

        // Wait for multiple election timeouts
        // Node cannot become leader without quorum (2 of 3)
        let wait_time = election_time * 5;
        sleep(wait_time).await;

        // Should NOT be leader (no quorum)
        assert!(
            !cache.is_leader(),
            "Node should NOT become leader without quorum"
        );

        // Leader ID should be None (no leader elected)
        assert_eq!(
            cache.leader_id(),
            None,
            "Leader ID should be None without quorum"
        );

        // With pre_vote enabled (which is the default), the term does NOT increase
        // unless the node gets pre-vote responses from a majority. This is the
        // expected behavior - pre_vote prevents term inflation when a node is isolated.
        // The term will stay at 0 since no other nodes are responding.
        let term = get_term(&cache);
        assert!(
            term <= 1,
            "With pre_vote, term should stay low without responses (got {})",
            term
        );

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-7: Verify election timeout configuration is respected
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc7_election_timeout_configuration() {
        let base_port = allocate_ports(1);

        // Use a longer election timeout
        let mut config = single_node_config(1, base_port);
        config.raft.election_tick = 10; // 10 ticks = 1 second
        config.raft.tick_interval_ms = 100;

        let expected_min = Duration::from_millis(500); // At least half the timeout
        let expected_max = Duration::from_millis(2000); // Allow for jitter

        let start = Instant::now();
        let cache = DistributedCache::new(config)
            .await
            .expect("Failed to create cache");

        // Wait for leader
        let became_leader = wait_for_leader(&cache, Duration::from_secs(5)).await;
        let elapsed = start.elapsed();

        assert!(became_leader, "Node should become leader");
        assert!(
            elapsed >= expected_min,
            "Election should respect timeout: {:?} < {:?}",
            elapsed,
            expected_min
        );
        assert!(
            elapsed <= expected_max,
            "Election should not take too long: {:?} > {:?}",
            elapsed,
            expected_max
        );

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-8: Clear operation on single node
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc8_single_node_clear_operation() {
        let base_port = allocate_ports(1);
        let config = single_node_config(1, base_port);
        let election_time = election_timeout(&config);

        let cache = DistributedCache::new(config)
            .await
            .expect("Failed to create cache");

        // Wait until leader
        let became_leader = wait_for_leader(&cache, election_time * 3).await;
        assert!(became_leader, "Node should become leader");

        // Add some data
        cache
            .put("key1", "value1")
            .await
            .expect("Put should succeed");
        cache
            .put("key2", "value2")
            .await
            .expect("Put should succeed");
        cache
            .put("key3", "value3")
            .await
            .expect("Put should succeed");

        // Run pending tasks to ensure Moka cache reflects updates
        cache.run_pending_tasks().await;
        assert!(cache.entry_count() >= 3, "Should have at least 3 entries");

        // Clear all entries
        let clear_result = cache.clear().await;
        assert!(clear_result.is_ok(), "Clear should succeed");

        // Verify all entries are gone
        assert!(cache.get(b"key1").await.is_none(), "key1 should be cleared");
        assert!(cache.get(b"key2").await.is_none(), "key2 should be cleared");
        assert!(cache.get(b"key3").await.is_none(), "key3 should be cleared");

        // Run pending tasks to ensure Moka cache reflects clear
        cache.run_pending_tasks().await;

        // Entry count should be 0
        assert_eq!(
            cache.entry_count(),
            0,
            "Entry count should be 0 after clear"
        );

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-9: Multiple writes and reads consistency
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc9_multiple_writes_reads_consistency() {
        let base_port = allocate_ports(1);
        let config = single_node_config(1, base_port);
        let election_time = election_timeout(&config);

        let cache = DistributedCache::new(config)
            .await
            .expect("Failed to create cache");

        // Wait until leader
        let became_leader = wait_for_leader(&cache, election_time * 3).await;
        assert!(became_leader, "Node should become leader");

        // Write multiple entries
        let num_entries = 50;
        for i in 0..num_entries {
            let key = format!("key-{}", i);
            let value = format!("value-{}", i);
            cache
                .put(bytes::Bytes::from(key), bytes::Bytes::from(value))
                .await
                .unwrap_or_else(|_| panic!("Put {} should succeed", i));
        }

        // Read and verify all entries
        for i in 0..num_entries {
            let key = format!("key-{}", i);
            let expected_value = format!("value-{}", i);
            let actual = cache.get(key.as_bytes()).await;
            assert_eq!(
                actual,
                Some(bytes::Bytes::from(expected_value.clone())),
                "Key {} should have correct value",
                key
            );
        }

        // Update some entries
        for i in 0..10 {
            let key = format!("key-{}", i);
            let new_value = format!("updated-value-{}", i);
            cache
                .put(bytes::Bytes::from(key), bytes::Bytes::from(new_value))
                .await
                .unwrap_or_else(|_| panic!("Update {} should succeed", i));
        }

        // Verify updates
        for i in 0..10 {
            let key = format!("key-{}", i);
            let expected_value = format!("updated-value-{}", i);
            let actual = cache.get(key.as_bytes()).await;
            assert_eq!(
                actual,
                Some(bytes::Bytes::from(expected_value)),
                "Updated key {} should have new value",
                key
            );
        }

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-10: Scale from 1 to 3 nodes
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc10_scale_one_to_three_nodes() {
        let base_port = allocate_ports(3);
        let election_time = Duration::from_millis(500); // 5 ticks * 100ms

        // Start with single node configured for 3-node cluster
        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        // Node 1: Start first
        let config1 = cluster_node_config(1, base_port, peer_configs.clone());
        let cache1 = DistributedCache::new(config1)
            .await
            .expect("Failed to create cache 1");

        // Wait a bit - node 1 alone cannot become leader (needs quorum of 2)
        sleep(election_time * 3).await;
        assert!(!cache1.is_leader(), "Node 1 should not be leader alone");

        // Node 2: Start second node
        let config2 = cluster_node_config(2, base_port + 1, peer_configs.clone());

        let cache2 = DistributedCache::new(config2)
            .await
            .expect("Failed to create cache 2");

        // Now with 2 nodes, one should become leader (quorum = 2)
        let leader_elected = wait_for(
            || cache1.is_leader() || cache2.is_leader(),
            election_time * 10,
            Duration::from_millis(50),
        )
        .await;

        assert!(leader_elected, "A leader should be elected with 2 nodes");

        // Node 3: Start third node
        let config3 = cluster_node_config(3, base_port + 2, peer_configs.clone());
        let cache3 = DistributedCache::new(config3)
            .await
            .expect("Failed to create cache 3");

        // Wait for stabilization
        sleep(election_time * 3).await;

        // Verify exactly one leader
        let caches = [&cache1, &cache2, &cache3];
        let leader_count = caches.iter().filter(|c| c.is_leader()).count();

        assert_eq!(
            leader_count, 1,
            "Should have exactly one leader, found {}",
            leader_count
        );

        // All nodes should agree on leader
        let leader_id_1 = cache1.leader_id();
        let leader_id_2 = cache2.leader_id();
        let leader_id_3 = cache3.leader_id();

        // At least the nodes that have discovered the leader should agree
        if leader_id_1.is_some() && leader_id_2.is_some() {
            assert_eq!(
                leader_id_1, leader_id_2,
                "Node 1 and 2 should agree on leader"
            );
        }
        if leader_id_2.is_some() && leader_id_3.is_some() {
            assert_eq!(
                leader_id_2, leader_id_3,
                "Node 2 and 3 should agree on leader"
            );
        }

        // Shutdown all
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    // ========================================================================
    // TC-11: Three node cluster - data replication
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc11_three_node_data_replication() {
        let base_port = allocate_ports(3);
        let election_time = Duration::from_millis(500);

        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        // Start all three nodes
        let config1 = cluster_node_config(1, base_port, peer_configs.clone());
        let config2 = cluster_node_config(2, base_port + 1, peer_configs.clone());
        let config3 = cluster_node_config(3, base_port + 2, peer_configs.clone());

        let cache1 = DistributedCache::new(config1)
            .await
            .expect("Failed to create cache 1");
        let cache2 = DistributedCache::new(config2)
            .await
            .expect("Failed to create cache 2");
        let cache3 = DistributedCache::new(config3)
            .await
            .expect("Failed to create cache 3");

        // Wait for leader election
        let leader_elected = wait_for(
            || cache1.is_leader() || cache2.is_leader() || cache3.is_leader(),
            election_time * 10,
            Duration::from_millis(50),
        )
        .await;
        assert!(leader_elected, "A leader should be elected");

        // Find the leader
        let leader: &DistributedCache = if cache1.is_leader() {
            &cache1
        } else if cache2.is_leader() {
            &cache2
        } else {
            &cache3
        };

        // Write data through leader
        let write_result = leader.put("replicated-key", "replicated-value").await;
        assert!(write_result.is_ok(), "Write should succeed on leader");

        // Wait for replication
        sleep(Duration::from_millis(500)).await;

        // All nodes should have the data (eventually consistent reads)
        let val1 = cache1.get(b"replicated-key").await;
        let val2 = cache2.get(b"replicated-key").await;
        let val3 = cache3.get(b"replicated-key").await;

        assert_eq!(
            val1,
            Some(bytes::Bytes::from("replicated-value")),
            "Node 1 should have replicated data"
        );
        assert_eq!(
            val2,
            Some(bytes::Bytes::from("replicated-value")),
            "Node 2 should have replicated data"
        );
        assert_eq!(
            val3,
            Some(bytes::Bytes::from("replicated-value")),
            "Node 3 should have replicated data"
        );

        // Shutdown all
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    // ========================================================================
    // TC-12: Write to non-leader fails with redirect
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc12_write_to_non_leader_fails() {
        let base_port = allocate_ports(3);
        let election_time = Duration::from_millis(500);

        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        // Start all three nodes
        let config1 = cluster_node_config(1, base_port, peer_configs.clone());
        let config2 = cluster_node_config(2, base_port + 1, peer_configs.clone());
        let config3 = cluster_node_config(3, base_port + 2, peer_configs.clone());

        let cache1 = DistributedCache::new(config1)
            .await
            .expect("Failed to create cache 1");
        let cache2 = DistributedCache::new(config2)
            .await
            .expect("Failed to create cache 2");
        let cache3 = DistributedCache::new(config3)
            .await
            .expect("Failed to create cache 3");

        // Wait for leader election
        let leader_elected = wait_for(
            || cache1.is_leader() || cache2.is_leader() || cache3.is_leader(),
            election_time * 10,
            Duration::from_millis(50),
        )
        .await;
        assert!(leader_elected, "A leader should be elected");

        // Find a follower
        let follower: &DistributedCache = if !cache1.is_leader() {
            &cache1
        } else if !cache2.is_leader() {
            &cache2
        } else {
            &cache3
        };

        // Attempt write to follower - should fail
        let write_result = follower.put("key", "value").await;
        assert!(write_result.is_err(), "Write to follower should fail");

        // Shutdown all
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    // ========================================================================
    // TC-13: Term monotonicity check
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc13_term_monotonicity() {
        let base_port = allocate_ports(1);
        let config = single_node_config(1, base_port);
        let election_time = election_timeout(&config);

        let cache = DistributedCache::new(config)
            .await
            .expect("Failed to create cache");

        // Track term over time
        let mut previous_term = 0u64;
        let check_duration = election_time * 10;
        let start = Instant::now();

        while start.elapsed() < check_duration {
            let current_term = get_term(&cache);

            // Term should never decrease
            assert!(
                current_term >= previous_term,
                "Term should be monotonically increasing: {} < {}",
                current_term,
                previous_term
            );

            previous_term = current_term;
            sleep(Duration::from_millis(50)).await;
        }

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-14: Cluster status consistency
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc14_cluster_status_consistency() {
        let base_port = allocate_ports(1);
        let config = single_node_config(1, base_port);
        let election_time = election_timeout(&config);

        let cache = DistributedCache::new(config)
            .await
            .expect("Failed to create cache");

        // Wait for leader
        let became_leader = wait_for_leader(&cache, election_time * 3).await;
        assert!(became_leader, "Node should become leader");

        // Check status consistency
        let status = cache.cluster_status();

        // Verify all fields are consistent
        assert_eq!(status.node_id, cache.node_id());
        assert_eq!(status.is_leader, cache.is_leader());
        assert_eq!(status.leader_id, cache.leader_id());

        // Commit index should be >= applied index
        assert!(
            status.commit_index >= status.applied_index,
            "Commit index should be >= applied index"
        );

        // After writes, verify indexes update
        cache.put("key", "value").await.expect("Put should succeed");

        let status_after = cache.cluster_status();
        assert!(
            status_after.commit_index >= status.commit_index,
            "Commit index should not decrease"
        );

        cache.shutdown().await;
    }

    // ========================================================================
    // TC-15: Local operations don't require leadership
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc15_local_operations_without_leadership() {
        let base_port = allocate_ports(3);

        // Configure as 3-node cluster but only start 1 (no quorum, no leader)
        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        let config = cluster_node_config(1, base_port, peer_configs);
        let cache = DistributedCache::new(config)
            .await
            .expect("Failed to create cache");

        // Should not be leader (no quorum)
        sleep(Duration::from_millis(300)).await;
        assert!(!cache.is_leader(), "Should not be leader without quorum");

        // Local operations should still work
        cache.put_local("local-key", "local-value").await;

        let value = cache.get(b"local-key").await;
        assert_eq!(
            value,
            Some(bytes::Bytes::from("local-value")),
            "Local put should work without leadership"
        );

        // Contains should work
        assert!(cache.contains(b"local-key"), "Contains should work");
        cache.run_pending_tasks().await;
        // Entry count should work
        assert!(cache.entry_count() >= 1, "Entry count should work");

        // Stats should work
        let stats = cache.stats();
        assert!(stats.entry_count >= 1, "Stats should work");

        cache.shutdown().await;
    }
}
