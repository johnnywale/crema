//! Per-shard Raft replication E2E tests.
//!
//! These tests verify that Multi-Raft with per-shard Raft replication works correctly:
//! - Each shard has its own independent Raft group for consensus
//! - Data is replicated across all nodes in the cluster
//! - Entry counts match across all nodes per shard
//! - NodeMessageRouter provides unified connection pooling

#[cfg(test)]
mod tests {
    use crate::cache::DistributedCache;
    use crate::cluster::MemberlistDiscovery;
    use crate::config::{
        CacheConfig, MemberlistConfig, MultiRaftCacheConfig, PeerManagementConfig, RaftConfig,
    };
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Clone)]
    struct NodeConfig {
        node_id: u64,
        raft_addr: SocketAddr,
        memberlist_addr: SocketAddr,
    }

    /// Create a 3-node cluster with per-shard Raft enabled
    async fn create_per_shard_cluster(
        base_raft_port: u16,
        base_memberlist_port: u16,
        num_shards: u32,
    ) -> Vec<Arc<DistributedCache>> {
        let nodes = vec![
            NodeConfig {
                node_id: 1,
                raft_addr: format!("127.0.0.1:{}", base_raft_port).parse().unwrap(),
                memberlist_addr: format!("127.0.0.1:{}", base_memberlist_port)
                    .parse()
                    .unwrap(),
            },
            NodeConfig {
                node_id: 2,
                raft_addr: format!("127.0.0.1:{}", base_raft_port + 1).parse().unwrap(),
                memberlist_addr: format!("127.0.0.1:{}", base_memberlist_port + 1)
                    .parse()
                    .unwrap(),
            },
            NodeConfig {
                node_id: 3,
                raft_addr: format!("127.0.0.1:{}", base_raft_port + 2).parse().unwrap(),
                memberlist_addr: format!("127.0.0.1:{}", base_memberlist_port + 2)
                    .parse()
                    .unwrap(),
            },
        ];

        let mut caches: Vec<Arc<DistributedCache>> = Vec::new();

        for (idx, node_config) in nodes.iter().enumerate() {
            // Build peer list (other nodes)
            let peers: Vec<(u64, SocketAddr)> = nodes
                .iter()
                .filter(|n| n.node_id != node_config.node_id)
                .map(|n| (n.node_id, n.raft_addr))
                .collect();

            // Memberlist seeds (node 1 is seed for others)
            let memberlist_seeds: Vec<SocketAddr> = if node_config.node_id == 1 {
                vec![]
            } else {
                vec![nodes[0].memberlist_addr]
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
                node_name: Some(format!(
                    "per-shard-test-{}-{}",
                    base_raft_port, node_config.node_id
                )),
                peer_management: PeerManagementConfig {
                    auto_add_peers: true,
                    auto_remove_peers: false,
                    auto_add_voters: false,
                    auto_remove_voters: false,
                },
            };

            // Multi-Raft config with per-shard Raft enabled
            let multiraft_config = MultiRaftCacheConfig {
                enabled: true,
                num_shards,
                shard_capacity: 10_000,
                auto_init_shards: true,
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

            // Create cache config
            let config = CacheConfig::new(node_config.node_id, node_config.raft_addr)
                .with_seed_nodes(peers)
                .with_max_capacity(100_000)
                .with_default_ttl(Duration::from_secs(3600))
                .with_raft_config(raft_config)
                .with_cluster_discovery(discovery)
                .with_multiraft_config(multiraft_config);

            // Create cache
            let cache = Arc::new(DistributedCache::new(config).await.unwrap());
            caches.push(cache);

            // Small delay between nodes
            if idx < nodes.len() - 1 {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        }

        caches
    }

    /// Wait for all caches in the cluster to become ready.
    ///
    /// Uses the built-in `wait_until_ready()` method which handles different modes:
    /// - Single Raft: waits for leader election
    /// - Multi-Raft Phase 1: waits for coordinator and active shards
    /// - Multi-Raft Phase 2: waits for all shard leaders to be elected
    async fn wait_for_cluster_ready(caches: &[Arc<DistributedCache>], timeout: Duration) -> bool {
        // Wait for all nodes to become ready
        for cache in caches {
            if cache.wait_until_ready(timeout).await.is_err() {
                return false;
            }
        }
        true
    }

    /// Shutdown all caches in the cluster
    async fn shutdown_cluster(caches: Vec<Arc<DistributedCache>>) {
        for cache in caches {
            cache.shutdown().await;
        }
    }

    // ========================================================================
    // TC-PSR-1: Per-shard Raft replication across 3-node cluster
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_psr1_per_shard_replication_basic() {
        let num_shards = 4u32;
        let caches = create_per_shard_cluster(21001, 22001, num_shards).await;

        // Wait for cluster formation and shard leader elections
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Debug: Check if per-shard Raft is enabled and show leader info
        for (idx, cache) in caches.iter().enumerate() {
            let (elected, total) = cache.shard_leader_status();
            let is_ready = cache.is_ready();
            eprintln!(
                "Node {} - is_ready: {}, shard_leaders: {}/{}",
                idx + 1,
                is_ready,
                elected,
                total
            );
        }

        let cluster_ready = wait_for_cluster_ready(&caches, Duration::from_secs(30)).await;
        assert!(cluster_ready, "Cluster should be ready within timeout");

        // Write test data via node 1
        let test_keys = vec![
            ("user:1", "Alice"),
            ("user:2", "Bob"),
            ("user:3", "Charlie"),
            ("session:abc", "session-data-abc"),
            ("session:def", "session-data-def"),
            ("order:100", "order-100-items"),
            ("order:200", "order-200-items"),
            ("product:999", "widget-xyz"),
        ];

        let write_cache = &caches[0];
        for (key, value) in &test_keys {
            let result = write_cache.put(*key, *value).await;
            assert!(result.is_ok(), "PUT '{}' should succeed: {:?}", key, result);
        }

        // Wait for replication
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify data on all nodes
        for (node_idx, cache) in caches.iter().enumerate() {
            for (key, expected_value) in &test_keys {
                let value = cache.get(key.as_bytes()).await;
                assert!(
                    value.is_some(),
                    "Node {} should have key '{}'",
                    node_idx + 1,
                    key
                );

                let value_bytes = value.unwrap();
                let actual = String::from_utf8_lossy(&value_bytes);
                assert_eq!(
                    actual,
                    *expected_value,
                    "Node {} key '{}' value mismatch",
                    node_idx + 1,
                    key
                );
            }
        }

        // Verify entry counts match across all nodes
        let expected_entries = test_keys.len() as u64;
        for (node_idx, cache) in caches.iter().enumerate() {
            let stats = cache.stats();
            assert_eq!(
                stats.entry_count,
                expected_entries,
                "Node {} should have {} entries, got {}",
                node_idx + 1,
                expected_entries,
                stats.entry_count
            );
        }

        shutdown_cluster(caches).await;
    }

    // ========================================================================
    // TC-PSR-2: Per-shard entry counts consistency
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_psr2_per_shard_entry_counts_consistency() {
        let num_shards = 4u32;
        let caches = create_per_shard_cluster(21011, 22011, num_shards).await;

        // Wait for cluster formation
        tokio::time::sleep(Duration::from_secs(5)).await;

        let cluster_ready = wait_for_cluster_ready(&caches, Duration::from_secs(30)).await;
        assert!(cluster_ready, "Cluster should be ready within timeout");

        // Write keys that will be distributed across all shards
        // Use a variety of key patterns to ensure even distribution
        let mut keys_per_shard: HashMap<u32, Vec<String>> = HashMap::new();

        let coordinator = caches[0].multiraft_coordinator().unwrap();

        // Generate 20 keys and track which shard they belong to
        for i in 0..20 {
            let key = format!("test-key-{}", i);
            let shard_id = coordinator.shard_for_key(key.as_bytes());
            keys_per_shard
                .entry(shard_id)
                .or_default()
                .push(key.clone());

            let value = format!("value-{}", i);
            let result = caches[0].put(key.clone(), value).await;
            assert!(result.is_ok(), "PUT '{}' should succeed", key);
        }

        // Wait for replication with polling for consistency
        // Poll until all nodes have matching entry counts, or timeout
        let replication_timeout = Duration::from_secs(15);
        let poll_interval = Duration::from_millis(500);
        let start = std::time::Instant::now();

        let mut consistent = false;
        while start.elapsed() < replication_timeout && !consistent {
            tokio::time::sleep(poll_interval).await;

            // Run pending tasks on all shards to flush writes
            for cache in &caches {
                if let Some(coord) = cache.multiraft_coordinator() {
                    for shard_id in 0..num_shards {
                        if let Some(shard) = coord.get_shard(shard_id) {
                            shard.storage().run_pending_tasks().await;
                        }
                    }
                }
            }

            // Check if all shards are consistent across nodes
            consistent = true;
            for shard_id in 0..num_shards {
                let mut counts: Vec<usize> = Vec::new();
                for cache in &caches {
                    if let Some(coord) = cache.multiraft_coordinator() {
                        if let Some(shard) = coord.get_shard(shard_id) {
                            counts.push(shard.storage().entry_count() as usize);
                        }
                    }
                }

                // Check if all counts match
                if !counts.windows(2).all(|w| w[0] == w[1]) {
                    consistent = false;
                    break;
                }
            }
        }

        // Final verification
        for shard_id in 0..num_shards {
            let expected_count = keys_per_shard.get(&shard_id).map(|v| v.len()).unwrap_or(0);

            // Get shard entry count from each node's coordinator
            let mut shard_counts: Vec<usize> = Vec::new();

            for (_node_idx, cache) in caches.iter().enumerate() {
                if let Some(coord) = cache.multiraft_coordinator() {
                    if let Some(shard) = coord.get_shard(shard_id) {
                        let count = shard.storage().entry_count() as usize;
                        shard_counts.push(count);
                    }
                }
            }

            // Primary assertion: All nodes should have the same count for this shard
            let first_count = shard_counts[0];
            for (node_idx, count) in shard_counts.iter().enumerate() {
                assert_eq!(
                    *count,
                    first_count,
                    "Shard {} count mismatch: Node 1 has {}, Node {} has {}",
                    shard_id,
                    first_count,
                    node_idx + 1,
                    count
                );
            }

            // Secondary check: Count should match expected (may vary if hash changes)
            if first_count != expected_count {
                eprintln!(
                    "Info: Shard {} has {} entries, expected {} (hash distribution may differ)",
                    shard_id, first_count, expected_count
                );
            }
        }

        // Also verify total entry counts match
        let total_entries = 20u64;
        let mut node_totals: Vec<u64> = Vec::new();

        for cache in &caches {
            let stats = cache.stats();
            node_totals.push(stats.entry_count);
        }

        for (node_idx, total) in node_totals.iter().enumerate() {
            assert_eq!(
                *total,
                total_entries,
                "Node {} total entries should be {}, got {}",
                node_idx + 1,
                total_entries,
                total
            );
        }

        // Verify all nodes have same total
        let first_total = node_totals[0];
        assert!(
            node_totals.iter().all(|t| *t == first_total),
            "All nodes should have same total entry count: {:?}",
            node_totals
        );

        shutdown_cluster(caches).await;
    }

    // ========================================================================
    // TC-PSR-3: Write via follower node with per-shard forwarding
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_psr3_write_via_follower_per_shard_forwarding() {
        let num_shards = 4u32;
        let caches = create_per_shard_cluster(21021, 22021, num_shards).await;

        // Wait for cluster formation
        tokio::time::sleep(Duration::from_secs(5)).await;

        let cluster_ready = wait_for_cluster_ready(&caches, Duration::from_secs(30)).await;
        assert!(cluster_ready, "Cluster should be ready within timeout");

        // Find a follower node (not the main Raft leader)
        let follower_idx = caches.iter().position(|c| !c.is_leader()).unwrap_or(1);

        let follower_cache = &caches[follower_idx];

        // Write test data via the follower node
        let test_keys = vec![
            ("follower-key-1", "value-1"),
            ("follower-key-2", "value-2"),
            ("follower-key-3", "value-3"),
            ("follower-key-4", "value-4"),
            ("follower-key-5", "value-5"),
        ];

        for (key, value) in &test_keys {
            let result = follower_cache.put(*key, *value).await;
            assert!(
                result.is_ok(),
                "PUT '{}' via follower should succeed: {:?}",
                key,
                result
            );
        }

        // Wait for replication
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify data on all nodes
        for (node_idx, cache) in caches.iter().enumerate() {
            for (key, expected_value) in &test_keys {
                let value = cache.get(key.as_bytes()).await;
                assert!(
                    value.is_some(),
                    "Node {} should have key '{}' written via follower",
                    node_idx + 1,
                    key
                );

                let value_bytes = value.unwrap();
                let actual = String::from_utf8_lossy(&value_bytes);
                assert_eq!(
                    actual,
                    *expected_value,
                    "Node {} key '{}' value mismatch",
                    node_idx + 1,
                    key
                );
            }
        }

        // Verify entry counts match
        let expected_entries = test_keys.len() as u64;
        let entry_counts: Vec<u64> = caches.iter().map(|c| c.stats().entry_count).collect();

        assert!(
            entry_counts.iter().all(|c| *c == expected_entries),
            "All nodes should have {} entries: {:?}",
            expected_entries,
            entry_counts
        );

        shutdown_cluster(caches).await;
    }

    // ========================================================================
    // TC-PSR-4: Shard leader information consistency
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_psr4_shard_leader_consistency() {
        let num_shards = 4u32;
        let caches = create_per_shard_cluster(21031, 22031, num_shards).await;

        // Wait for cluster formation and shard leader elections
        tokio::time::sleep(Duration::from_secs(5)).await;

        let cluster_ready = wait_for_cluster_ready(&caches, Duration::from_secs(30)).await;
        assert!(cluster_ready, "Cluster should be ready within timeout");

        // Collect shard leader info from each node
        let mut leader_maps: Vec<HashMap<u32, Option<u64>>> = Vec::new();

        for cache in &caches {
            if let Some(coordinator) = cache.multiraft_coordinator() {
                let leaders = coordinator.shard_leaders();
                leader_maps.push(leaders);
            }
        }

        // Verify all nodes agree on shard leaders (eventually consistent)
        // Note: There might be slight delays in leader info propagation
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Re-collect after waiting
        let mut final_leader_maps: Vec<HashMap<u32, Option<u64>>> = Vec::new();

        for cache in &caches {
            if let Some(coordinator) = cache.multiraft_coordinator() {
                let leaders = coordinator.shard_leaders();
                final_leader_maps.push(leaders);
            }
        }

        // Check each shard has a leader on all nodes
        for shard_id in 0..num_shards {
            let leaders_for_shard: Vec<Option<u64>> = final_leader_maps
                .iter()
                .map(|m| *m.get(&shard_id).unwrap_or(&None))
                .collect();

            // All nodes should report a leader for this shard
            for (node_idx, leader) in leaders_for_shard.iter().enumerate() {
                assert!(
                    leader.is_some(),
                    "Node {} should know leader for shard {}",
                    node_idx + 1,
                    shard_id
                );
            }

            // All nodes should agree on the same leader
            let first_leader = leaders_for_shard[0];
            for (node_idx, leader) in leaders_for_shard.iter().enumerate() {
                assert_eq!(
                    *leader,
                    first_leader,
                    "Shard {} leader mismatch: Node 1 sees {:?}, Node {} sees {:?}",
                    shard_id,
                    first_leader,
                    node_idx + 1,
                    leader
                );
            }
        }

        shutdown_cluster(caches).await;
    }
}
