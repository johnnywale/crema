#[cfg(test)]
mod multi_node_tests {
    use crate::cache::DistributedCache;
    use crate::testing::utils;
    use std::time::{Duration, Instant};
    use test_log::test;
    use tokio::time::sleep;
    use tracing::info;

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

    /// Helper function to find leader with retry logic (TC18, TC27 fix)
    /// Avoids direct unwrap() which can panic when nodes are not yet ready
    async fn find_leader_with_retry<'a>(
        caches: &[&'a DistributedCache],
        timeout: Duration,
    ) -> Option<&'a DistributedCache> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Some(leader) = caches.iter().find(|c| c.is_leader()) {
                return Some(*leader);
            }
            sleep(Duration::from_millis(100)).await;
        }
        None
    }

    /// Helper function to wait for a rejoined node to catch up with the cluster.
    /// Checks that the node's applied_index reaches a target value.
    async fn wait_for_node_catchup(
        node: &DistributedCache,
        target_applied: u64,
        timeout: Duration,
    ) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            let status = node.cluster_status();
            if status.applied_index >= target_applied {
                info!(
                    node_id = node.node_id(),
                    applied = status.applied_index,
                    target = target_applied,
                    "Node caught up successfully"
                );
                return true;
            }
            sleep(Duration::from_millis(100)).await;
        }
        let status = node.cluster_status();
        info!(
            node_id = node.node_id(),
            applied = status.applied_index,
            target = target_applied,
            "Node failed to catch up within timeout"
        );
        false
    }

    /// Helper function to verify a key-value with retry logic.
    async fn verify_key_with_retry(
        cache: &DistributedCache,
        key: &str,
        expected: Option<bytes::Bytes>,
        timeout: Duration,
    ) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            let actual = cache.get(key.as_bytes()).await;
            if actual == expected {
                return true;
            }
            sleep(Duration::from_millis(100)).await;
        }
        false
    }

    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn tc23_log_replication_consistency() {
        let port_configs = utils::allocate_os_ports(&[1, 2, 3]).await;
        let config1 = utils::cluster_node_config(1, &port_configs);
        sleep(Duration::from_millis(100)).await;
        let config2 = utils::cluster_node_config(2, &port_configs);
        sleep(Duration::from_millis(100)).await;
        let config3 = utils::cluster_node_config(3, &port_configs);
        sleep(Duration::from_millis(100)).await;

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Wait for leader election with longer timeout
        info!("Waiting for leader election...");
        let leader_id = utils::wait_for_single_leader(&caches, Duration::from_secs(10)).await;
        assert!(leader_id.is_some(), "Should elect a leader");
        info!("Leader elected: {:?}", leader_id);

        // Find the leader with retry logic (TC27 fix)
        let leader = find_leader_with_retry(&caches, Duration::from_secs(5))
            .await
            .expect("Leader should be found after election");
        println!("Leader is node {}", leader.node_id());

        // Send 100 commands to the leader with error handling
        println!("Starting to send 100 commands...");
        for i in 0..100 {
            let key = format!("key-{}", i);
            let value = format!("value-{}", i);

            // Use timeout to prevent hanging
            let put_result = tokio::time::timeout(
                Duration::from_secs(5),
                leader.put(bytes::Bytes::from(key.clone()), bytes::Bytes::from(value)),
            )
            .await;

            match put_result {
                Ok(Ok(_)) => {
                    if i % 10 == 0 {
                        println!("Successfully wrote key-{}", i);
                    }
                }
                Ok(Err(e)) => {
                    panic!("Put {} failed with error: {:?}", i, e);
                }
                Err(_) => {
                    panic!("Put {} timed out after 5 seconds", i);
                }
            }

            // Add small delay between writes to avoid overwhelming the system
            if i % 10 == 0 {
                sleep(Duration::from_millis(100)).await;
            }
        }
        println!("All 100 commands sent successfully");

        // Wait for replication to complete
        println!("Waiting for replication...");
        sleep(Duration::from_secs(3)).await;

        // Verify all nodes have consistent commit_index
        let commit_indices: Vec<u64> = caches
            .iter()
            .map(|c| c.cluster_status().commit_index)
            .collect();

        println!("Commit indices: {:?}", commit_indices);

        let max_commit = *commit_indices.iter().max().unwrap();
        for (i, &commit) in commit_indices.iter().enumerate() {
            assert!(
                commit >= max_commit - 5,
                "Node {} commit_index {} too far behind max {}",
                i + 1,
                commit,
                max_commit
            );
        }

        // Verify all nodes have the same data
        println!("Verifying data consistency...");
        for i in 0..100 {
            let key = format!("key-{}", i);
            let expected = Some(bytes::Bytes::from(format!("value-{}", i)));

            let consistent = verify_data_consistency(&caches, key.as_bytes(), expected).await;
            assert!(
                consistent,
                "Data for {} should be consistent across nodes",
                key
            );

            if i % 25 == 0 {
                println!("Verified consistency up to key-{}", i);
            }
        }
        println!("All data verified consistent");

        // Cleanup
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    // ========================================================================
    // TC-16: Node Rejoin (node rejoining the cluster)
    // ========================================================================
    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn tc27_node_rejoin() {
        let port_configs = utils::allocate_os_ports(&[1, 2, 3]).await;

        // Create configs directly - CacheConfig is not Clone, so we'll recreate for restart
        let cache1 = DistributedCache::new(utils::cluster_node_config(1, &port_configs))
            .await
            .unwrap();
        let cache2 = DistributedCache::new(utils::cluster_node_config(2, &port_configs))
            .await
            .unwrap();
        let cache3 = DistributedCache::new(utils::cluster_node_config(3, &port_configs))
            .await
            .unwrap();

        // Wait for leader election
        let caches = [&cache1, &cache2, &cache3];
        utils::wait_for_single_leader(&caches, Duration::from_secs(5)).await;

        // Write some data (TC27 fix: use retry logic)
        let leader = find_leader_with_retry(&caches, Duration::from_secs(5))
            .await
            .expect("Leader should be elected");
        for i in 0..10 {
            leader
                .put(format!("key-{}", i), format!("value-{}", i))
                .await
                .unwrap();
        }

        // Shutdown node 1
        cache1.shutdown().await;
        // Wait for shutdown to complete and election timeout to trigger
        // Election tick for node 2 is ~30 ticks * 100ms = 3s, node 3 is ~40 ticks * 100ms = 4s
        sleep(Duration::from_secs(1)).await;

        // Write more data while node 1 is down (TC27 fix: use retry logic)
        // Increase timeout to 10s to allow for election timeout + vote exchange
        let remaining = [&cache2, &cache3];
        let leader = find_leader_with_retry(&remaining, Duration::from_secs(10))
            .await
            .expect("Leader should be elected among remaining nodes");

        for i in 10..20 {
            leader
                .put(format!("key-{}", i), format!("value-{}", i))
                .await
                .unwrap();
        }

        // Record the leader's applied index - the rejoined node should reach this
        let target_applied = leader.cluster_status().applied_index;
        info!(target_applied, "Target applied index for catch-up");

        sleep(Duration::from_millis(500)).await;

        // Restart node 1 (recreate config since CacheConfig is not Clone)
        let cache1_rejoined = DistributedCache::new(utils::cluster_node_config(1, &port_configs))
            .await
            .unwrap();

        // Wait for node to catch up using the helper (more robust than fixed sleep)
        // Give it enough time for log reconciliation and replication
        let caught_up =
            wait_for_node_catchup(&cache1_rejoined, target_applied, Duration::from_secs(15)).await;

        if !caught_up {
            // Log the current state for debugging
            let status = cache1_rejoined.cluster_status();
            info!(
                applied = status.applied_index,
                commit = status.commit_index,
                target = target_applied,
                "Node did not fully catch up, but will verify data with retry"
            );
        }

        // Verify node 1 has all the data with retry logic
        // Even if catch-up appears complete, give some grace period for state machine apply
        for i in 0..20 {
            let key = format!("key-{}", i);
            let expected = Some(bytes::Bytes::from(format!("value-{}", i)));
            let verified = verify_key_with_retry(
                &cache1_rejoined,
                &key,
                expected.clone(),
                Duration::from_secs(5),
            )
            .await;
            assert!(
                verified,
                "Rejoined node should have caught up: key {} (expected {:?})",
                key, expected
            );
        }

        // Cleanup
        cache1_rejoined.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    // ========================================================================
    // TC-18: Stale Leader Replacement (stale leader returns)
    // ========================================================================
    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn tc18_stale_leader_replacement() {
        let port_configs = utils::allocate_os_ports(&[1, 2, 3]).await;
        // CacheConfig is not Clone, so create configs directly
        let cache1 = DistributedCache::new(utils::cluster_node_config(1, &port_configs))
            .await
            .unwrap();
        let cache2 = DistributedCache::new(utils::cluster_node_config(2, &port_configs))
            .await
            .unwrap();
        let cache3 = DistributedCache::new(utils::cluster_node_config(3, &port_configs))
            .await
            .unwrap();
        let caches = [&cache1, &cache2, &cache3];

        // Short wait time since low tick counts make elections very fast
        utils::wait_for_single_leader(&caches, Duration::from_secs(10)).await;

        // 2. Identify the initial leader and shut it down (TC18 fix: use retry logic)
        let initial_leader = find_leader_with_retry(&caches, Duration::from_secs(10))
            .await
            .expect("Leader should be elected quickly with low tick counts");
        let initial_leader_id = initial_leader.node_id();
        let term1 = initial_leader.cluster_status().term;

        initial_leader.shutdown().await;

        // Wait for port release and network cleanup
        // Ensure old TCP connections are fully closed to avoid port conflicts on restart
        sleep(Duration::from_millis(500)).await;

        // 3. Wait for new leader election
        let remaining: Vec<&DistributedCache> = caches
            .iter()
            .filter(|c| c.node_id() != initial_leader_id)
            .copied()
            .collect();

        // Give remaining nodes enough time to trigger a new election (TC18 fix: use retry logic)
        let new_leader = find_leader_with_retry(&remaining, Duration::from_secs(10))
            .await
            .expect("New leader should be elected from remaining nodes");
        let term2 = new_leader.cluster_status().term;
        assert!(
            term2 > term1,
            "Term should have increased: {} -> {}",
            term1,
            term2
        );

        // 4. Write new data under new leader
        new_leader
            .put("new-leader-key", "new-leader-value")
            .await
            .unwrap();

        // 5. Restart the OLD LEADER (not always node 1!) with a high election tick
        // so it will patiently wait for heartbeats instead of immediately starting Pre-Vote
        let mut reconnected_config = utils::cluster_node_config(initial_leader_id, &port_configs);
        reconnected_config.raft.election_tick = 50;

        sleep(Duration::from_millis(500)).await;
        let cache_reconnected = DistributedCache::new(reconnected_config).await.unwrap();

        // Verify the reconnected node's voter configuration is correct
        let reconnected_voters = cache_reconnected.voters();
        info!(
            "Reconnected node {} has voters: {:?}",
            initial_leader_id, reconnected_voters
        );
        assert!(
            reconnected_voters.contains(&1)
                && reconnected_voters.contains(&2)
                && reconnected_voters.contains(&3),
            "Reconnected node should know all voters. Got: {:?}",
            reconnected_voters
        );

        // Verify the current leader also knows about the reconnected node
        let leader_voters = new_leader.voters();
        info!("Current leader has voters: {:?}", leader_voters);
        assert!(
            leader_voters.contains(&initial_leader_id),
            "Leader should know about node {}. Got: {:?}",
            initial_leader_id,
            leader_voters
        );

        // Trigger writes to force the new leader to send MsgAppend with the new term
        for _ in 0..5 {
            let _ = new_leader.put("trigger-sync", "val").await;
            sleep(Duration::from_millis(200)).await;
        }

        // 6. Verify old leader stepped down and caught up to the current term
        let success = utils::wait_for(
            || {
                let status = cache_reconnected.cluster_status();
                // Core check: term caught up and role changed to follower (is_leader == false)
                status.term >= term2 && !cache_reconnected.is_leader()
            },
            Duration::from_secs(10),
            Duration::from_millis(200),
        )
        .await;

        if !success {
            let status = cache_reconnected.cluster_status();
            panic!(
                "Sync failed! Node {} is at Term {}, Leader: {}",
                initial_leader_id,
                status.term,
                cache_reconnected.is_leader()
            );
        }

        // 7. Verify data synchronization
        let synced_value = utils::wait_for_result(
            || async { cache_reconnected.get(b"new-leader-key").await },
            |res| res.as_ref().map(|b| b.as_ref()) == Some("new-leader-value".as_bytes()),
            Duration::from_secs(5),
        )
        .await;

        assert!(
            synced_value.is_some(),
            "Old leader should have synced data from the current cluster"
        );

        // 8. Cleanup
        cache_reconnected.shutdown().await;
        for cache in remaining {
            cache.shutdown().await;
        }
    }

    // ========================================================================
    // TC-22: Message Reordering Tolerance
    // ========================================================================
    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn tc22_message_reordering_tolerance() {
        let port_configs = utils::allocate_os_ports(&[1, 2, 3]).await;
        let config1 = utils::cluster_node_config(1, &port_configs);
        let config2 = utils::cluster_node_config(2, &port_configs);
        let config3 = utils::cluster_node_config(3, &port_configs);

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Wait for leader election (TC22 fix: use retry logic)
        utils::wait_for_single_leader(&caches, Duration::from_secs(10)).await;

        let leader = find_leader_with_retry(&caches, Duration::from_secs(5))
            .await
            .expect("Leader should be elected");

        // Perform many writes (testing message ordering tolerance)
        for i in 0..50 {
            let key = format!("concurrent-{}", i);
            let value = format!("value-{}", i);
            let result = leader.put(key, value).await;
            assert!(result.is_ok(), "Write {} should succeed", i);
        }

        // Wait for replication
        sleep(Duration::from_secs(10)).await;

        // Verify consistency across all nodes
        for i in 0..50 {
            let key = format!("concurrent-{}", i);
            let expected = Some(bytes::Bytes::from(format!("value-{}", i)));

            let consistent = verify_data_consistency(&caches, key.as_bytes(), expected).await;
            assert!(
                consistent,
                "Data for {} should be consistent despite concurrent writes",
                key
            );
        }

        // Verify idempotency - write same key multiple times
        for _ in 0..5 {
            leader
                .put("idempotent-key", "idempotent-value")
                .await
                .unwrap();
        }

        sleep(Duration::from_millis(500)).await;

        // All nodes should have the same final value
        for cache in &caches {
            let value = cache.get(b"idempotent-key").await;
            assert_eq!(
                value,
                Some(bytes::Bytes::from("idempotent-value")),
                "Idempotent writes should result in consistent state"
            );
        }

        // Cleanup
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    //
}
