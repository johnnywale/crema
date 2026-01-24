#[cfg(test)]
pub(crate) mod multi_node_tests {
    use crate::cache::DistributedCache;
    use crate::config::CacheConfig;
    use crate::testing::utils;
    use crate::testing::utils::allocate_os_ports;
    use crate::types::NodeId;
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};
    use test_log::test;
    use tokio::time::sleep;

    /// Helper function to verify data consistency across nodes
    async fn verify_data_consistency(
        caches: &[&DistributedCache],
        key: &[u8],
        expected_value: Option<bytes::Bytes>,
    ) -> bool {
        for cache in caches {
            let value = cache.get(key).await;
            if value != expected_value {
                return false;
            }
        }
        true
    }

    /// Network partition simulator
    struct NetworkPartition {
        partitions: Arc<Mutex<HashMap<NodeId, HashSet<NodeId>>>>,
    }

    impl NetworkPartition {
        fn new() -> Self {
            Self {
                partitions: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        /// Create a partition: nodes in partition1 cannot communicate with nodes in partition2
        fn create_partition(&self, partition1: Vec<NodeId>, partition2: Vec<NodeId>) {
            let mut partitions = self.partitions.lock().unwrap();

            for &node1 in &partition1 {
                let blocked = partitions.entry(node1).or_insert_with(HashSet::new);
                for &node2 in &partition2 {
                    blocked.insert(node2);
                }
            }

            for &node2 in &partition2 {
                let blocked = partitions.entry(node2).or_insert_with(HashSet::new);
                for &node1 in &partition1 {
                    blocked.insert(node1);
                }
            }
        }

        /// Heal the partition - restore all communications
        fn heal(&self) {
            let mut partitions = self.partitions.lock().unwrap();
            partitions.clear();
        }
    }

    // ========================================================================
    // TC-11: Standard Quorum Election (多数派存在时的选举)
    // ========================================================================
    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn tc28_standard_quorum_election() {
        let port_configs = utils::allocate_os_ports(&[1, 2, 3]).await;

        // Start all three nodes
        let config1 = utils::cluster_node_config(1, &port_configs);
        let config2 = utils::cluster_node_config(2, &port_configs);
        let config3 = utils::cluster_node_config(3, &port_configs);

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Wait for leader election (within election timeout)
        let leader_id = utils::wait_for_single_leader(&caches, Duration::from_secs(5)).await;
        assert!(leader_id.is_some(), "Should elect a unique leader");

        // Verify only one leader
        let leader_count = caches.iter().filter(|c| c.is_leader()).count();
        assert_eq!(leader_count, 1, "Should have exactly one leader");

        // Verify all nodes agree on the same term
        let terms: HashSet<u64> = caches.iter().map(|c| c.cluster_status().term).collect();
        assert_eq!(terms.len(), 1, "All nodes should have the same term");

        // Verify all nodes agree on the leader
        // let agreed_leader = utils::verify_leader_agreement(&caches);

        let agreement_reached = utils::wait_for(
            || {
                let agreed = utils::verify_leader_agreement(&caches);
                agreed.is_some() && agreed == leader_id
            },
            Duration::from_secs(3),
            Duration::from_millis(100),
        )
        .await;

        assert!(
            agreement_reached,
            "All nodes should agree on leader within timeout"
        );
        let agreed_leader = utils::verify_leader_agreement(&caches);
        assert_eq!(agreed_leader, leader_id, "Leader agreement mismatch");

        // Cleanup
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    // ========================================================================
    // TC-13: Heartbeat Stability (Leader自动续期)
    // ========================================================================
    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    #[cfg(test)]
    async fn tc24_heartbeat_stability() {
        let port_configs = allocate_os_ports(&[1, 2, 3]).await;

        let config1 = utils::cluster_node_config(1, &port_configs);
        let config2 = utils::cluster_node_config(2, &port_configs);
        let config3 = utils::cluster_node_config(3, &port_configs);

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Wait for leader election
        let leader_id = utils::wait_for_single_leader(&caches, Duration::from_secs(5)).await;
        assert!(leader_id.is_some(), "Should elect a leader");

        let initial_term = cache1.cluster_status().term;

        // Run for multiple election timeout periods
        let stability_duration = Duration::from_secs(5);
        let start = Instant::now();

        while start.elapsed() < stability_duration {
            // Verify leader hasn't changed
            let current_leader_count = caches.iter().filter(|c| c.is_leader()).count();
            assert_eq!(current_leader_count, 1, "Should maintain single leader");

            // Verify term hasn't increased (no spurious elections)
            let current_term = cache1.cluster_status().term;
            assert_eq!(
                current_term, initial_term,
                "Term should remain stable (no unnecessary elections)"
            );

            sleep(Duration::from_millis(100)).await;
        }

        // Cleanup
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    // ========================================================================
    // TC-14: Minority Failure (少数派故障)
    // ========================================================================
    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn tc25_minority_failure() {
        let port_configs = allocate_os_ports(&[1, 2, 3]).await;

        let config1 = utils::cluster_node_config(1, &port_configs);
        let config2 = utils::cluster_node_config(2, &port_configs);
        let config3 = utils::cluster_node_config(3, &port_configs);

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Wait for leader election
        let initial_leader_id = utils::wait_for_single_leader(&caches, Duration::from_secs(5)).await;
        assert!(initial_leader_id.is_some(), "Should elect a leader");

        // Identify one follower to shut down
        let follower = caches
            .iter()
            .find(|c| !c.is_leader())
            .expect("Should have at least one follower");

        let follower_id = follower.node_id();

        // Shutdown one follower
        follower.shutdown().await;

        // Verify cluster still has a leader (same as before) - poll with timeout
        let remaining_caches: Vec<&DistributedCache> = caches
            .iter()
            .filter(|c| c.node_id() != follower_id)
            .copied()
            .collect();

        // Wait for the cluster to stabilize - the original leader should remain
        let leader_stable = utils::wait_for(
            || {
                // Check if any remaining node is a leader
                let has_leader = remaining_caches.iter().any(|c| c.is_leader());
                if !has_leader {
                    return false;
                }
                // Check if the leader is the same as before
                let current_leader = remaining_caches
                    .iter()
                    .find(|c| c.is_leader())
                    .map(|c| c.node_id());
                current_leader == initial_leader_id
            },
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .await;

        assert!(
            leader_stable,
            "Leader should remain the same after minority failure"
        );

        // Verify cluster can still process writes - retry with timeout
        let write_success = utils::wait_for_result(
            || async {
                if let Some(leader) = remaining_caches.iter().find(|c| c.is_leader()) {
                    leader.put("test-key", "test-value").await
                } else {
                    Err(crate::error::Error::Raft(crate::error::RaftError::NotLeader { leader: None }))
                }
            },
            |r| r.is_ok(),
            Duration::from_secs(15),
        )
        .await;
        assert!(
            write_success.is_some(),
            "Cluster should accept writes with 2/3 nodes"
        );

        // Cleanup remaining nodes
        for cache in remaining_caches {
            cache.shutdown().await;
        }
    }
    // // ========================================================================
    // // TC-15: Leader Failover (Leader宕机切换)
    // // ========================================================================
    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn tc26_leader_failover() {
        let port_configs = allocate_os_ports(&[1, 2, 3]).await;

        let config1 = utils::cluster_node_config(1, &port_configs);
        let config2 = utils::cluster_node_config(2, &port_configs);
        let config3 = utils::cluster_node_config(3, &port_configs);

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Wait for initial leader election
        let initial_leader_id = utils::wait_for_single_leader(&caches, Duration::from_secs(5)).await;
        assert!(initial_leader_id.is_some(), "Should elect initial leader");

        let initial_term = cache1.cluster_status().term;

        // Find and kill the leader
        let leader = caches
            .iter()
            .find(|c| c.is_leader())
            .expect("Should have a leader");

        let killed_leader_id = leader.node_id();
        leader.shutdown().await;

        // Get remaining nodes
        let remaining_caches: Vec<&DistributedCache> = caches
            .iter()
            .filter(|c| c.node_id() != killed_leader_id)
            .copied()
            .collect();

        // Wait for new leader election with polling
        let new_leader_elected = utils::wait_for(
            || remaining_caches.iter().any(|c| c.is_leader()),
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .await;

        assert!(
            new_leader_elected,
            "Should elect new leader after leader failure"
        );

        // Verify new leader has higher term with polling
        let term_increased = utils::wait_for(
            || remaining_caches[0].cluster_status().term > initial_term,
            Duration::from_secs(5),
            Duration::from_millis(100),
        )
        .await;

        let new_term = remaining_caches[0].cluster_status().term;
        assert!(
            term_increased,
            "New leader should have higher term ({} > {})",
            new_term,
            initial_term
        );

        // Verify cluster can accept writes - retry with timeout
        let write_success = utils::wait_for_result(
            || async {
                if let Some(leader) = remaining_caches.iter().find(|c| c.is_leader()) {
                    leader.put("failover-key", "failover-value").await
                } else {
                    Err(crate::error::Error::Raft(crate::error::RaftError::NotLeader { leader: None }))
                }
            },
            |r| r.is_ok(),
            Duration::from_secs(15),
        )
        .await;
        assert!(write_success.is_some(), "New leader should accept writes");

        // Cleanup
        for cache in remaining_caches {
            cache.shutdown().await;
        }
    }

    // ========================================================================
    // TC-17: Network Partition - Majority/Minority Split (对称分区)
    // ========================================================================
    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn tc17_network_partition_majority_minority() {
        let port_configs = allocate_os_ports(&[1, 2, 3, 4, 5]).await;

        // Start all 5 nodes (create configs directly - CacheConfig is not Clone)
        let cache1 = DistributedCache::new(utils::cluster_node_config(1, &port_configs)).await.unwrap();
        let cache2 = DistributedCache::new(utils::cluster_node_config(2, &port_configs)).await.unwrap();
        let cache3 = DistributedCache::new(utils::cluster_node_config(3, &port_configs)).await.unwrap();
        let cache4 = DistributedCache::new(utils::cluster_node_config(4, &port_configs)).await.unwrap();
        let cache5 = DistributedCache::new(utils::cluster_node_config(5, &port_configs)).await.unwrap();

        let all_caches = [&cache1, &cache2, &cache3, &cache4, &cache5];

        // Wait for leader election
        utils::wait_for_single_leader(&all_caches, Duration::from_secs(5)).await;

        let leader = all_caches.iter().find(|c| c.is_leader()).unwrap();

        // Write initial data
        leader
            .put("before-partition", "initial-value")
            .await
            .unwrap();

        // Wait for write to propagate to all nodes (give more time for Raft replication)
        for cache in &all_caches {
            let propagated = utils::wait_for_result(
                || async { cache.get(b"before-partition").await },
                |v| v.is_some(),
                Duration::from_secs(10),
            )
            .await;
            assert!(propagated.is_some(), "Initial write should propagate");
        }

        // Simulate partition: {1,2,3} vs {4,5}
        // In a real test, you would need network-level isolation
        // For this example, we'll shutdown nodes 4 and 5 to simulate partition
        cache4.shutdown().await;
        cache5.shutdown().await;

        // Majority partition {1,2,3} should still have a leader - wait with polling
        let majority = [&cache1, &cache2, &cache3];
        let majority_has_leader = utils::wait_for(
            || majority.iter().any(|c| c.is_leader()),
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .await;
        assert!(
            majority_has_leader,
            "Majority partition should have a leader"
        );

        // Write to majority partition - retry if it fails initially due to stabilization
        // The leader needs time to re-establish quorum communication with remaining nodes
        let write_success = utils::wait_for_result(
            || async {
                // Re-find leader in case it changed
                if let Some(leader) = majority.iter().find(|c| c.is_leader()) {
                    leader.put("majority-write", "success").await
                } else {
                    Err(crate::error::Error::Raft(crate::error::RaftError::NotLeader { leader: None }))
                }
            },
            |r| r.is_ok(),
            Duration::from_secs(15),
        )
        .await;
        assert!(write_success.is_some(), "Write to majority should succeed");

        // Verify data in majority partition with polling (extended timeout for Raft propagation)
        for cache in &majority {
            let data_replicated = utils::wait_for_result(
                || async { cache.get(b"majority-write").await },
                |v| v.as_ref().map(|b| b.as_ref()) == Some(b"success"),
                Duration::from_secs(15),
            )
            .await;
            assert!(
                data_replicated.is_some(),
                "Majority nodes should have the write"
            );
        }

        // Cleanup
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    // ========================================================================
    // TC-19: Split Vote Recovery (选票瓜分)
    // ========================================================================
    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn tc19_split_vote_recovery() {
        let port_configs = allocate_os_ports(&[1, 2, 3]).await;

        // Use same election timeout for all nodes to increase chance of split vote
        let mut config1 = utils::cluster_node_config(1, &port_configs);
        let mut config2 = utils::cluster_node_config(2, &port_configs);
        let mut config3 = utils::cluster_node_config(3, &port_configs);

        // Set identical election timeouts (simulate worst case)
        config1.raft.election_tick = 10;
        config2.raft.election_tick = 10;
        config3.raft.election_tick = 10;

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Even with identical timeouts, randomization should eventually succeed
        let leader_elected = utils::wait_for_single_leader(&caches, Duration::from_secs(10)).await;

        assert!(
            leader_elected.is_some(),
            "Leader should eventually be elected despite split vote risk"
        );

        // Verify exactly one leader
        let leader_count = caches.iter().filter(|c| c.is_leader()).count();
        assert_eq!(leader_count, 1, "Should have exactly one leader");

        // Cleanup
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    // ========================================================================
    // TC-20: No Dual Leader (验证不会出现双Leader)
    // ========================================================================
    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn tc20_no_dual_leader() {
        let port_configs = allocate_os_ports(&[1, 2, 3, 4, 5]).await;

        // Start all 5 nodes (create configs directly - CacheConfig is not Clone)
        let caches = vec![
            DistributedCache::new(utils::cluster_node_config(1, &port_configs)).await.unwrap(),
            DistributedCache::new(utils::cluster_node_config(2, &port_configs)).await.unwrap(),
            DistributedCache::new(utils::cluster_node_config(3, &port_configs)).await.unwrap(),
            DistributedCache::new(utils::cluster_node_config(4, &port_configs)).await.unwrap(),
            DistributedCache::new(utils::cluster_node_config(5, &port_configs)).await.unwrap(),
        ];

        let cache_refs: Vec<&DistributedCache> = caches.iter().collect();

        // Monitor for dual leader over extended period
        let monitoring_duration = Duration::from_secs(10);
        let start = Instant::now();

        while start.elapsed() < monitoring_duration {
            let leaders: Vec<NodeId> = cache_refs
                .iter()
                .filter(|c| c.is_leader())
                .map(|c| c.node_id())
                .collect();

            assert!(
                leaders.len() <= 1,
                "Found multiple leaders at same time: {:?}",
                leaders
            );

            // If we have a leader, verify term consistency
            if leaders.len() == 1 {
                let terms: HashSet<u64> =
                    cache_refs.iter().map(|c| c.cluster_status().term).collect();

                // Terms should be very close (within 1)
                let max_term = *terms.iter().max().unwrap();
                let min_term = *terms.iter().min().unwrap();
                assert!(
                    max_term - min_term <= 1,
                    "Term spread too large: {} - {} = {}",
                    max_term,
                    min_term,
                    max_term - min_term
                );
            }

            sleep(Duration::from_millis(100)).await;
        }

        // Cleanup
        for cache in caches {
            cache.shutdown().await;
        }
    }

    // ========================================================================
    // TC-21: Disk IO Stall Simulation (磁盘IO阻塞)
    // ========================================================================
    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn tc21_disk_io_stall() {
        // Note: This is a simplified version since we can't easily simulate
        // disk IO stalls without modifying the storage layer

        let port_configs = allocate_os_ports(&[1, 2, 3]).await;

        let config1 = utils::cluster_node_config(1, &port_configs);
        let config2 = utils::cluster_node_config(2, &port_configs);
        let config3 = utils::cluster_node_config(3, &port_configs);

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Wait for leader election
        utils::wait_for_single_leader(&caches, Duration::from_secs(5)).await;

        // In a real scenario with slow disk, we would:
        // 1. Inject delays in the storage layer
        // 2. Verify that followers detect missing heartbeats
        // 3. Verify new leader election occurs
        // 4. Verify no election oscillation (repeated elections)

        // For this test, we verify the system remains stable under normal conditions
        let stability_duration = Duration::from_secs(5);
        let start = Instant::now();
        let initial_term = cache1.cluster_status().term;

        while start.elapsed() < stability_duration {
            let leader_count = caches.iter().filter(|c| c.is_leader()).count();
            assert_eq!(leader_count, 1, "Should maintain single leader");

            let current_term = cache1.cluster_status().term;
            assert!(
                current_term <= initial_term + 1,
                "Should not have excessive term increases"
            );

            sleep(Duration::from_millis(100)).await;
        }

        // Cleanup
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    // ========================================================================
    // Additional Helper Tests
    // ========================================================================

    /// Verify quorum check - only nodes with majority votes can become leader
    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn tc_extra_quorum_verification() {
        let port_configs = allocate_os_ports(&[1, 2, 3, 4, 5]).await;

        // Create configs directly - CacheConfig is not Clone
        let caches = vec![
            DistributedCache::new(utils::cluster_node_config(1, &port_configs)).await.unwrap(),
            DistributedCache::new(utils::cluster_node_config(2, &port_configs)).await.unwrap(),
            DistributedCache::new(utils::cluster_node_config(3, &port_configs)).await.unwrap(),
            DistributedCache::new(utils::cluster_node_config(4, &port_configs)).await.unwrap(),
            DistributedCache::new(utils::cluster_node_config(5, &port_configs)).await.unwrap(),
        ];

        let cache_refs: Vec<&DistributedCache> = caches.iter().collect();

        // Wait for leader
        let leader_id = utils::wait_for_single_leader(&cache_refs, Duration::from_secs(5)).await;
        assert!(leader_id.is_some(), "Quorum should elect a leader");

        // For 5 nodes, quorum is 3
        // Shutdown 2 nodes (leaving exactly 3)
        caches[3].shutdown().await;
        caches[4].shutdown().await;

        // Should still have a leader with exactly quorum - poll with timeout (increase timeout)
        let remaining = [&caches[0], &caches[1], &caches[2]];
        let leader_exists = utils::wait_for(
            || remaining.iter().any(|c| c.is_leader()),
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .await;
        assert!(
            leader_exists,
            "Should maintain leader with exactly quorum (3/5)"
        );

        // Write should still succeed - retry with timeout (extended for cluster stabilization)
        let write_success = utils::wait_for_result(
            || async {
                if let Some(leader) = remaining.iter().find(|c| c.is_leader()) {
                    leader.put("quorum-test", "value").await
                } else {
                    Err(crate::error::Error::Raft(crate::error::RaftError::NotLeader { leader: None }))
                }
            },
            |r| r.is_ok(),
            Duration::from_secs(30),
        )
        .await;
        assert!(write_success.is_some(), "Write should succeed with quorum");

        // Cleanup
        for cache in remaining {
            cache.shutdown().await;
        }
    }

    /// Verify term monotonicity across cluster
    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn tc_extra_term_monotonicity_cluster() {
        let port_configs = allocate_os_ports(&[1, 2, 3]).await;

        let config1 = utils::cluster_node_config(1, &port_configs);
        let config2 = utils::cluster_node_config(2, &port_configs);
        let config3 = utils::cluster_node_config(3, &port_configs);

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Wait for leader election
        utils::wait_for_single_leader(&caches, Duration::from_secs(5)).await;

        // Monitor terms over time
        let monitoring_duration = Duration::from_secs(5);
        let start = Instant::now();
        let mut previous_terms = vec![0u64; 3];

        while start.elapsed() < monitoring_duration {
            for (i, cache) in caches.iter().enumerate() {
                let current_term = cache.cluster_status().term;

                assert!(
                    current_term >= previous_terms[i],
                    "Term for node {} decreased: {} -> {}",
                    i + 1,
                    previous_terms[i],
                    current_term
                );

                previous_terms[i] = current_term;
            }

            sleep(Duration::from_millis(100)).await;
        }

        // Cleanup
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    /// Verify linearizability - once leader confirms write, all reads see it
    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn tc_extra_linearizability_check() {
        let port_configs = allocate_os_ports(&[1, 2, 3]).await;

        let config1 = utils::cluster_node_config(1, &port_configs);
        let config2 = utils::cluster_node_config(2, &port_configs);
        let config3 = utils::cluster_node_config(3, &port_configs);
    
        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();
    
        let caches = [&cache1, &cache2, &cache3];
    
        // Wait for leader election
        utils::wait_for_single_leader(&caches, Duration::from_secs(5)).await;
    
        let leader = caches.iter().find(|c| c.is_leader()).unwrap();
    
        // Write and wait for confirmation
        leader
            .put("linearizable-key", "version-1")
            .await
            .expect("Write should succeed");
    
        // Immediately after confirmation, all reads should see the new value
        sleep(Duration::from_millis(500)).await;
    
        for (i, cache) in caches.iter().enumerate() {
            let value = cache.get(b"linearizable-key").await;
            assert_eq!(
                value,
                Some(bytes::Bytes::from("version-1")),
                "Node {} should see confirmed write immediately",
                i + 1
            );
        }
    
        // Update the value
        leader
            .put("linearizable-key", "version-2")
            .await
            .expect("Update should succeed");
    
        sleep(Duration::from_millis(500)).await;
    
        // All subsequent reads should see version-2, never version-1
        for _ in 0..10 {
            for cache in &caches {
                let value = cache.get(b"linearizable-key").await;
                assert_eq!(
                    value,
                    Some(bytes::Bytes::from("version-2")),
                    "Should never read stale value after update confirmed"
                );
            }
            sleep(Duration::from_millis(50)).await;
        }
    
        // Cleanup
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }
}
