//! Integration tests for Multi-Raft coordinator.
//!
//! These tests verify the Multi-Raft functionality including:
//! - Coordinator initialization and shard creation
//! - Key routing and shard distribution
//! - Put/get/delete operations across shards
//! - Shard leader tracking
//! - Batch operations and consistency
//! - Coordinator shutdown and cleanup

#[cfg(test)]
mod tests {
    use crate::metrics::CacheMetrics;
    use crate::multiraft::{
        BatchRouter, CoordinatorState, MultiRaftBuilder, MultiRaftConfig, MultiRaftCoordinator,
        RouterConfig, ShardConfig, ShardRouter, ShardState,
    };
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::time::sleep;

    /// Wait for a synchronous condition with timeout and polling.
    ///
    /// For async conditions or cluster consensus tests, prefer using
    /// `crate::testing::eventually` which is designed for Raft replication timing.
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

    /// Wait for a condition with a default 5-second timeout and 10ms polling interval.
    #[allow(dead_code)]
    async fn wait_for_default<F>(condition: F) -> bool
    where
        F: Fn() -> bool,
    {
        wait_for(condition, Duration::from_secs(5), Duration::from_millis(10)).await
    }

    // ========================================================================
    // TC-MR-1: Coordinator creation with default config
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr1_coordinator_creation_default_config() {
        let metrics = Arc::new(CacheMetrics::new());
        let config = MultiRaftConfig::default();
        let coordinator = MultiRaftCoordinator::new(1, config, metrics);

        assert_eq!(coordinator.state(), CoordinatorState::Initializing);
        assert_eq!(coordinator.node_id(), 1);
        assert!(!coordinator.is_running());

        // Config should have defaults
        assert_eq!(coordinator.config().num_shards, 16);
        assert_eq!(coordinator.config().replica_factor, 3);
        assert_eq!(coordinator.config().shard_capacity, 100_000);
    }

    // ========================================================================
    // TC-MR-2: Coordinator creation with custom config
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr2_coordinator_creation_custom_config() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(8)
            .replica_factor(5)
            .shard_capacity(50_000)
            .default_ttl(Duration::from_secs(3600))
            .build();

        assert_eq!(coordinator.config().num_shards, 8);
        assert_eq!(coordinator.config().replica_factor, 5);
        assert_eq!(coordinator.config().shard_capacity, 50_000);
        assert_eq!(
            coordinator.config().default_ttl,
            Some(Duration::from_secs(3600))
        );
    }

    // ========================================================================
    // TC-MR-3: Coordinator initialization creates all shards
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr3_coordinator_init_creates_shards() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        assert_eq!(
            coordinator.state(),
            CoordinatorState::Running,
            "Coordinator should be in Running state after build_and_init"
        );
        assert!(
            coordinator.is_running(),
            "Coordinator should report is_running=true"
        );

        let stats = coordinator.stats();
        assert_eq!(
            stats.total_shards, 4,
            "Should have 4 total shards configured"
        );
        assert_eq!(
            stats.active_shards, 4,
            "All 4 shards should be active after init"
        );

        // Verify each shard exists and is active
        for shard_id in 0..4 {
            let shard = coordinator
                .get_shard(shard_id)
                .unwrap_or_else(|| panic!("Shard {} should exist after init", shard_id));
            assert!(
                shard.is_active(),
                "Shard {} should be active, got state {:?}",
                shard_id,
                shard.state()
            );
        }

        coordinator
            .shutdown()
            .await
            .expect("Coordinator shutdown should succeed");
    }

    // ========================================================================
    // TC-MR-4: Manual shard creation (no auto-init)
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr4_manual_shard_creation() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .no_auto_init()
            .build();

        coordinator
            .init()
            .await
            .expect("Failed to initialize coordinator");

        // No shards should exist yet (no_auto_init was set)
        assert_eq!(
            coordinator.stats().active_shards,
            0,
            "No shards should be created when no_auto_init is set"
        );

        // Create shards manually one at a time
        coordinator
            .create_shard(0)
            .await
            .expect("Failed to create shard 0");
        assert_eq!(
            coordinator.stats().active_shards,
            1,
            "Should have 1 active shard after creating shard 0"
        );

        coordinator
            .create_shard(1)
            .await
            .expect("Failed to create shard 1");
        assert_eq!(
            coordinator.stats().active_shards,
            2,
            "Should have 2 active shards after creating shard 1"
        );

        // Try to create duplicate shard - should fail
        let result = coordinator.create_shard(0).await;
        assert!(
            result.is_err(),
            "Creating duplicate shard 0 should fail, but got: {:?}",
            result
        );

        coordinator
            .shutdown()
            .await
            .expect("Coordinator shutdown should succeed");
    }

    // ========================================================================
    // TC-MR-5: Shard removal
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr5_shard_removal() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        assert_eq!(
            coordinator.stats().active_shards,
            4,
            "Should start with 4 active shards"
        );

        // Remove shard 2
        coordinator
            .remove_shard(2)
            .await
            .expect("Failed to remove shard 2");
        assert_eq!(
            coordinator.stats().active_shards,
            3,
            "Should have 3 active shards after removing shard 2"
        );
        assert!(
            coordinator.get_shard(2).is_none(),
            "Shard 2 should not be retrievable after removal"
        );

        // Try to remove already-removed shard - should fail
        let result = coordinator.remove_shard(2).await;
        assert!(
            result.is_err(),
            "Removing already-removed shard 2 should fail, but got: {:?}",
            result
        );

        coordinator
            .shutdown()
            .await
            .expect("Coordinator shutdown should succeed");
    }

    // ========================================================================
    // TC-MR-6: Key routing consistency
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr6_key_routing_consistency() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        // Same key should always route to the same shard (deterministic hashing)
        let key = b"test-key";
        let shard1 = coordinator.shard_for_key(key);
        let shard2 = coordinator.shard_for_key(key);
        let shard3 = coordinator.shard_for_key(key);

        assert_eq!(
            shard1, shard2,
            "Same key should route to same shard on repeated calls"
        );
        assert_eq!(shard2, shard3, "Key routing should be deterministic");

        // Shard ID should be in valid range [0, num_shards)
        assert!(
            shard1 < 4,
            "Shard ID {} should be less than num_shards (4)",
            shard1
        );

        coordinator
            .shutdown()
            .await
            .expect("Coordinator shutdown should succeed");
    }

    // ========================================================================
    // TC-MR-7: Key distribution across shards
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr7_key_distribution() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        let mut shard_counts: HashMap<u32, u32> = HashMap::new();
        let num_keys = 1000;

        // Route 1000 keys and count distribution per shard
        for i in 0..num_keys {
            let key = format!("key-{}", i);
            let shard_id = coordinator.shard_for_key(key.as_bytes());
            *shard_counts.entry(shard_id).or_insert(0) += 1;
        }

        // All 4 shards should receive some keys
        assert_eq!(
            shard_counts.len(),
            4,
            "All 4 shards should have keys. Distribution: {:?}",
            shard_counts
        );

        // Distribution should be reasonably even (allow ~40% variance from ideal 250)
        let ideal = num_keys / 4;
        for (shard_id, count) in &shard_counts {
            assert!(
                *count > 150,
                "Shard {} has too few keys: {} (expected ~{}). Full distribution: {:?}",
                shard_id,
                count,
                ideal,
                shard_counts
            );
            assert!(
                *count < 350,
                "Shard {} has too many keys: {} (expected ~{}). Full distribution: {:?}",
                shard_id,
                count,
                ideal,
                shard_counts
            );
        }

        coordinator
            .shutdown()
            .await
            .expect("Coordinator shutdown should succeed");
    }

    // ========================================================================
    // TC-MR-8: Basic put/get operations
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr8_basic_put_get() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        // Put a value
        coordinator
            .put("key1", "value1")
            .await
            .expect("Put should succeed");

        // Get the value back
        let result = coordinator.get(b"key1").await.expect("Get should succeed");
        assert_eq!(
            result,
            Some(Bytes::from("value1")),
            "Should retrieve the value that was just put"
        );

        // Get non-existent key should return None
        let result = coordinator
            .get(b"nonexistent")
            .await
            .expect("Get of non-existent key should not error");
        assert_eq!(
            result, None,
            "Non-existent key should return None, not an error"
        );

        coordinator
            .shutdown()
            .await
            .expect("Coordinator shutdown should succeed");
    }

    // ========================================================================
    // TC-MR-9: Put with TTL (API test)
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr9_put_with_ttl() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        // Note: Per-entry TTL is not fully implemented in CacheStorage
        // (see storage.rs:80-86). This test verifies the API works
        // but TTL expiration behavior is a TODO item.

        // Put a value with TTL - API should accept the call
        coordinator
            .put_with_ttl("ttl-key", "ttl-value", Duration::from_secs(60))
            .await
            .expect("Put with TTL should succeed");

        // Value should exist immediately after put
        let result = coordinator
            .get(b"ttl-key")
            .await
            .expect("Get should succeed");
        assert_eq!(
            result,
            Some(Bytes::from("ttl-value")),
            "Value should exist immediately after put_with_ttl"
        );

        // Verify we can also do a second put with TTL (update)
        coordinator
            .put_with_ttl("ttl-key", "ttl-value-updated", Duration::from_secs(120))
            .await
            .expect("Update with TTL should succeed");

        let result = coordinator
            .get(b"ttl-key")
            .await
            .expect("Get should succeed");
        assert_eq!(
            result,
            Some(Bytes::from("ttl-value-updated")),
            "Value should be updated"
        );

        coordinator.shutdown().await.unwrap();
    }

    // ========================================================================
    // TC-MR-10: Delete operation
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr10_delete_operation() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        // Put a value
        coordinator
            .put("delete-key", "delete-value")
            .await
            .expect("Put should succeed");

        // Verify it exists
        let result = coordinator
            .get(b"delete-key")
            .await
            .expect("Get should succeed");
        assert_eq!(result, Some(Bytes::from("delete-value")));

        // Delete it
        coordinator
            .delete(b"delete-key")
            .await
            .expect("Delete should succeed");

        // Verify it's gone
        let result = coordinator
            .get(b"delete-key")
            .await
            .expect("Get should succeed");
        assert_eq!(result, None, "Value should be deleted");

        coordinator.shutdown().await.unwrap();
    }

    // ========================================================================
    // TC-MR-11: Multiple operations across shards
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr11_multiple_operations_across_shards() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        let num_entries = 100;

        // Insert entries
        for i in 0..num_entries {
            let key = format!("key-{}", i);
            let value = format!("value-{}", i);
            coordinator
                .put(key.clone(), value.clone())
                .await
                .unwrap_or_else(|e| panic!("Put key-{} should succeed: {:?}", i, e));
        }

        // Run pending tasks on all shards
        for shard in coordinator.router().all_shards() {
            shard.storage().run_pending_tasks().await;
        }

        // Verify all entries and count distribution per shard
        let mut shard_entry_counts: HashMap<u32, u32> = HashMap::new();
        let mut missing_keys = Vec::new();

        for i in 0..num_entries {
            let key = format!("key-{}", i);
            let expected_value = format!("value-{}", i);
            let shard_id = coordinator.shard_for_key(key.as_bytes());
            *shard_entry_counts.entry(shard_id).or_insert(0) += 1;

            let result = coordinator
                .get(key.as_bytes())
                .await
                .unwrap_or_else(|e| panic!("Get {} should succeed: {:?}", key, e));

            if result != Some(Bytes::from(expected_value.clone())) {
                missing_keys.push((key.clone(), result.clone()));
            }
        }

        assert!(
            missing_keys.is_empty(),
            "All keys should be retrievable. Missing/incorrect: {:?}",
            missing_keys
        );

        // Verify stats
        let stats = coordinator.stats();
        assert!(
            stats.total_entries >= (num_entries - 10) as u64,
            "Should have at least {} entries, got {}. Distribution: {:?}",
            num_entries - 10,
            stats.total_entries,
            shard_entry_counts
        );
        assert!(
            stats.operations_total >= num_entries as u64,
            "Should have at least {} operations, got {}",
            num_entries,
            stats.operations_total
        );

        coordinator
            .shutdown()
            .await
            .expect("Coordinator shutdown should succeed");
    }

    // ========================================================================
    // TC-MR-12: Update existing keys
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr12_update_existing_keys() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        // Insert initial values
        for i in 0..10 {
            let key = format!("update-key-{}", i);
            let value = format!("initial-{}", i);
            coordinator
                .put(key, value)
                .await
                .expect("Initial put should succeed");
        }

        // Update all values
        for i in 0..10 {
            let key = format!("update-key-{}", i);
            let new_value = format!("updated-{}", i);
            coordinator
                .put(key, new_value)
                .await
                .expect("Update put should succeed");
        }

        // Verify updates
        for i in 0..10 {
            let key = format!("update-key-{}", i);
            let expected_value = format!("updated-{}", i);
            let result = coordinator
                .get(key.as_bytes())
                .await
                .expect("Get should succeed");
            assert_eq!(
                result,
                Some(Bytes::from(expected_value)),
                "Key {} should have updated value",
                key
            );
        }

        coordinator.shutdown().await.unwrap();
    }

    // ========================================================================
    // TC-MR-13: Shard leader tracking
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr13_shard_leader_tracking() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        // Initially no leaders set
        let leaders = coordinator.shard_leaders();
        for shard_id in 0..4 {
            assert_eq!(
                leaders.get(&shard_id),
                Some(&None),
                "Shard {} should have no leader initially",
                shard_id
            );
        }

        // Set leaders for different shards
        coordinator.set_shard_leader(0, 1);
        coordinator.set_shard_leader(1, 2);
        coordinator.set_shard_leader(2, 1);
        coordinator.set_shard_leader(3, 3);

        let leaders = coordinator.shard_leaders();
        assert_eq!(leaders.get(&0), Some(&Some(1)));
        assert_eq!(leaders.get(&1), Some(&Some(2)));
        assert_eq!(leaders.get(&2), Some(&Some(1)));
        assert_eq!(leaders.get(&3), Some(&Some(3)));

        // Verify shard's is_leader reflects when this node is leader
        let shard0 = coordinator.get_shard(0).unwrap();
        assert!(
            shard0.is_leader(),
            "Shard 0 should have this node as leader"
        );

        let shard1 = coordinator.get_shard(1).unwrap();
        assert!(
            !shard1.is_leader(),
            "Shard 1 should not have this node as leader"
        );

        coordinator.shutdown().await.unwrap();
    }

    // ========================================================================
    // TC-MR-14: Stats tracking
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr14_stats_tracking() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        let initial_stats = coordinator.stats();
        assert_eq!(initial_stats.total_shards, 4);
        assert_eq!(initial_stats.active_shards, 4);
        assert_eq!(initial_stats.operations_total, 0);

        // Perform operations
        for i in 0..50 {
            let key = format!("stats-key-{}", i);
            let value = format!("stats-value-{}", i);
            coordinator.put(key, value).await.unwrap();
        }

        // Run pending tasks
        for shard in coordinator.router().all_shards() {
            shard.storage().run_pending_tasks().await;
        }

        let final_stats = coordinator.stats();
        assert!(
            final_stats.operations_total >= 50,
            "Should have at least 50 operations, got {}",
            final_stats.operations_total
        );
        assert!(
            final_stats.total_entries >= 45,
            "Should have entries, got {}",
            final_stats.total_entries
        );
        assert!(
            final_stats.operations_per_sec > 0.0,
            "Operations per second should be positive"
        );

        coordinator.shutdown().await.unwrap();
    }

    // ========================================================================
    // TC-MR-15: Coordinator shutdown
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr15_coordinator_shutdown() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        assert!(coordinator.is_running());
        assert_eq!(coordinator.state(), CoordinatorState::Running);

        // Add some data
        coordinator
            .put("shutdown-key", "shutdown-value")
            .await
            .unwrap();

        // Shutdown
        coordinator
            .shutdown()
            .await
            .expect("Shutdown should succeed");

        assert!(!coordinator.is_running());
        assert_eq!(coordinator.state(), CoordinatorState::Stopped);

        // All shards should be stopped
        for shard in coordinator.router().all_shards() {
            assert_eq!(shard.state(), ShardState::Stopped);
        }
    }

    // ========================================================================
    // TC-MR-16: Shard info retrieval
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr16_shard_info_retrieval() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        // Add data to distribute across shards
        for i in 0..100 {
            let key = format!("info-key-{}", i);
            let value = format!("info-value-{}", i);
            coordinator.put(key, value).await.unwrap();
        }

        // Run pending tasks
        for shard in coordinator.router().all_shards() {
            shard.storage().run_pending_tasks().await;
        }

        let shard_infos = coordinator.shard_info();
        assert_eq!(shard_infos.len(), 4, "Should have 4 shard infos");

        let mut total_entries = 0u64;
        for info in &shard_infos {
            assert_eq!(info.state, ShardState::Active);
            assert!(info.shard_id < 4);
            total_entries += info.entry_count;
        }

        assert!(
            total_entries >= 90,
            "Total entries across shards should be at least 90, got {}",
            total_entries
        );

        coordinator.shutdown().await.unwrap();
    }

    // ========================================================================
    // TC-MR-17: Router operations directly
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr17_router_operations() {
        let router_config = RouterConfig::new(4);
        let router = Arc::new(ShardRouter::new(router_config));

        // Register shards
        for i in 0..4 {
            let shard_config = ShardConfig::new(i, 4);
            let shard = Arc::new(
                crate::multiraft::Shard::new(shard_config)
                    .await
                    .expect("Failed to create shard"),
            );
            shard.set_state(ShardState::Active);
            router.register_shard(shard);
        }

        assert_eq!(router.all_shards().len(), 4);

        // Test put/get via router
        let key = Bytes::from("router-test-key");
        let value = Bytes::from("router-test-value");

        router
            .put(key.clone(), value.clone())
            .await
            .expect("Router put should succeed");

        let result = router.get(&key).await.expect("Router get should succeed");
        assert_eq!(result, Some(value));

        // Test delete via router
        router
            .delete(&key)
            .await
            .expect("Router delete should succeed");
        let result = router.get(&key).await.expect("Router get should succeed");
        assert_eq!(result, None);
    }

    // ========================================================================
    // TC-MR-18: Batch router operations
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr18_batch_router_operations() {
        let router = Arc::new(ShardRouter::new(RouterConfig::new(4)));
        let batch_router = BatchRouter::new(router.clone());

        // Set up leaders
        router.set_shard_leader(0, 1);
        router.set_shard_leader(1, 2);
        router.set_shard_leader(2, 3);
        router.set_shard_leader(3, 1);

        let keys: Vec<&[u8]> = vec![b"key1", b"key2", b"key3", b"key4", b"key5", b"key6"];

        // Test batch routing
        let routing = batch_router.route_batch(&keys);
        let total_keys: usize = routing.values().map(|v| v.len()).sum();
        assert_eq!(total_keys, 6, "All keys should be routed");

        // Test routing decisions
        let decisions = batch_router.get_routing_decisions(&keys, 1);
        assert_eq!(decisions.len(), 6);

        for decision in &decisions {
            assert!(decision.shard_id < 4);
        }

        // Some should be local (leader is node 1 for shards 0 and 3)
        let local_count = decisions.iter().filter(|d| d.is_local).count();
        assert!(local_count > 0, "Some keys should route to local node");
    }

    // ========================================================================
    // TC-MR-19: Router leader tracking
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr19_router_leader_tracking() {
        let router = ShardRouter::with_defaults();

        // Initially no leaders
        assert_eq!(router.get_shard_leader(0), None);
        assert_eq!(router.get_shard_leader(1), None);

        // Set leaders
        router.set_shard_leader(0, 1);
        router.set_shard_leader(1, 2);
        router.set_shard_leader(2, 3);

        assert_eq!(router.get_shard_leader(0), Some(1));
        assert_eq!(router.get_shard_leader(1), Some(2));
        assert_eq!(router.get_shard_leader(2), Some(3));
        assert_eq!(router.get_shard_leader(3), None);

        // Test get_leader_for_key
        let key = b"test-key";
        let shard_id = router.shard_for_key(key);
        let expected_leader = router.get_shard_leader(shard_id);
        let actual_leader = router.get_leader_for_key(key);
        assert_eq!(expected_leader, actual_leader);
    }

    // ========================================================================
    // TC-MR-20: Large scale operations
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_mr20_large_scale_operations() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(8)
            .shard_capacity(10_000)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        let num_entries = 500;

        // Insert many entries
        let start = Instant::now();
        for i in 0..num_entries {
            let key = format!("large-key-{}", i);
            let value = format!("large-value-{}-{}", i, "x".repeat(100));
            coordinator.put(key, value).await.unwrap();
        }
        let insert_elapsed = start.elapsed();

        // Run pending tasks
        for shard in coordinator.router().all_shards() {
            shard.storage().run_pending_tasks().await;
        }

        // Read all entries back
        let start = Instant::now();
        let mut found = 0;
        for i in 0..num_entries {
            let key = format!("large-key-{}", i);
            if coordinator.get(key.as_bytes()).await.unwrap().is_some() {
                found += 1;
            }
        }
        let read_elapsed = start.elapsed();

        // Verify most entries found (allow some variance due to async nature)
        assert!(
            found >= num_entries - 10,
            "Should find at least {} entries, found {}",
            num_entries - 10,
            found
        );

        // Stats should reflect operations
        let stats = coordinator.stats();
        assert!(
            stats.operations_total >= num_entries as u64 * 2,
            "Should have at least {} operations",
            num_entries * 2
        );

        tracing::info!(
            "Large scale test: {} inserts in {:?}, {} reads in {:?}",
            num_entries,
            insert_elapsed,
            num_entries,
            read_elapsed
        );

        coordinator.shutdown().await.unwrap();
    }

    // ========================================================================
    // TC-MR-21: Concurrent operations
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_mr21_concurrent_operations() {
        let coordinator = Arc::new(
            MultiRaftBuilder::new(1)
                .num_shards(4)
                .build_and_init()
                .await
                .expect("Failed to initialize coordinator"),
        );

        let num_tasks = 10;
        let ops_per_task = 50;
        let total_expected = num_tasks * ops_per_task;

        // Spawn concurrent tasks
        let mut handles = vec![];
        for task_id in 0..num_tasks {
            let coord = coordinator.clone();
            let handle = tokio::spawn(async move {
                let mut success_count = 0;
                for i in 0..ops_per_task {
                    let key = format!("concurrent-{}-{}", task_id, i);
                    let value = format!("value-{}-{}", task_id, i);
                    match coord.put(key.clone(), value.clone()).await {
                        Ok(()) => success_count += 1,
                        Err(e) => {
                            eprintln!("Task {} failed to put key {}: {:?}", task_id, key, e);
                        }
                    }
                }
                success_count
            });
            handles.push(handle);
        }

        // Wait for all tasks and count successes
        let mut total_successes = 0;
        for (task_id, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(count) => total_successes += count,
                Err(e) => eprintln!("Task {} panicked: {:?}", task_id, e),
            }
        }

        // Run pending tasks
        for shard in coordinator.router().all_shards() {
            shard.storage().run_pending_tasks().await;
        }

        // Verify entries
        let stats = coordinator.stats();
        let expected_min = (total_expected - 50) as u64;
        assert!(
            stats.total_entries >= expected_min,
            "Should have at least {} entries, got {}. Total successes: {}",
            expected_min,
            stats.total_entries,
            total_successes
        );
        assert!(
            total_successes >= total_expected - 10,
            "At least {} puts should succeed, got {}",
            total_expected - 10,
            total_successes
        );

        coordinator
            .shutdown()
            .await
            .expect("Coordinator shutdown should succeed");
    }

    // ========================================================================
    // TC-MR-22: Shard state transitions
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr22_shard_state_transitions() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        let shard = coordinator.get_shard(0).unwrap();

        // Should be active after init
        assert_eq!(shard.state(), ShardState::Active);
        assert!(shard.is_active());

        // Can change to transferring
        shard.set_state(ShardState::Transferring);
        assert_eq!(shard.state(), ShardState::Transferring);
        assert!(!shard.is_active());

        // Can change to removing
        shard.set_state(ShardState::Removing);
        assert_eq!(shard.state(), ShardState::Removing);
        assert!(!shard.is_active());

        // Can change to stopped
        shard.set_state(ShardState::Stopped);
        assert_eq!(shard.state(), ShardState::Stopped);
        assert!(!shard.is_active());

        coordinator.shutdown().await.unwrap();
    }

    // ========================================================================
    // TC-MR-23: Shard member management
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr23_shard_member_management() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        let shard = coordinator.get_shard(0).unwrap();

        // Initially no members
        assert!(shard.members().is_empty());

        // Add members
        shard.add_member(1);
        shard.add_member(2);
        shard.add_member(3);

        let members = shard.members();
        assert_eq!(members.len(), 3);
        assert!(members.contains(&1));
        assert!(members.contains(&2));
        assert!(members.contains(&3));

        // Remove a member
        shard.remove_member(2);
        let members = shard.members();
        assert_eq!(members.len(), 2);
        assert!(!members.contains(&2));

        coordinator.shutdown().await.unwrap();
    }

    // ========================================================================
    // TC-MR-24: Shard term and index tracking
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr24_shard_term_index_tracking() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        let shard = coordinator.get_shard(0).unwrap();

        // Initial values should be 0
        assert_eq!(shard.term(), 0);
        assert_eq!(shard.commit_index(), 0);
        assert_eq!(shard.applied_index(), 0);

        // Update term
        shard.set_term(5);
        assert_eq!(shard.term(), 5);

        // Update commit index
        shard.set_commit_index(100);
        assert_eq!(shard.commit_index(), 100);

        // Term should be monotonic in real usage
        shard.set_term(10);
        assert_eq!(shard.term(), 10);

        coordinator.shutdown().await.unwrap();
    }

    // ========================================================================
    // TC-MR-25: Key ownership verification
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr25_key_ownership() {
        let config = ShardConfig::new(0, 4);
        let shard = crate::multiraft::Shard::new(config)
            .await
            .expect("Failed to create shard");

        // Keys with hash % 4 == 0 belong to shard 0
        assert!(shard.owns_key(0));
        assert!(shard.owns_key(4));
        assert!(shard.owns_key(8));
        assert!(shard.owns_key(100)); // 100 % 4 == 0

        // Keys with hash % 4 != 0 don't belong to shard 0
        assert!(!shard.owns_key(1));
        assert!(!shard.owns_key(2));
        assert!(!shard.owns_key(3));
        assert!(!shard.owns_key(101)); // 101 % 4 == 1

        // Verify key range
        let range = shard.key_range();
        assert!(range.contains(0));
        assert!(range.contains(4));
        assert!(!range.contains(1));
        assert!(!range.contains(2));
    }

    // ========================================================================
    // TC-MR-26: Metrics integration
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr26_metrics_integration() {
        let metrics = Arc::new(CacheMetrics::new());
        let config = MultiRaftConfig::new(4);
        let coordinator = MultiRaftCoordinator::new(1, config, metrics.clone());

        coordinator.init().await.expect("Failed to initialize");

        // Perform operations
        coordinator.put("m-key1", "m-value1").await.unwrap();
        coordinator.put("m-key2", "m-value2").await.unwrap();
        coordinator.get(b"m-key1").await.unwrap();
        coordinator.get(b"nonexistent").await.unwrap();
        coordinator.delete(b"m-key2").await.unwrap();

        // Verify metrics were recorded
        let snapshot = metrics.snapshot();
        assert!(
            snapshot.put_total >= 2,
            "Should have at least 2 puts, got {}",
            snapshot.put_total
        );
        assert!(
            snapshot.get_total >= 2,
            "Should have at least 2 gets, got {}",
            snapshot.get_total
        );
        assert!(
            snapshot.delete_total >= 1,
            "Should have at least 1 delete, got {}",
            snapshot.delete_total
        );

        coordinator.shutdown().await.unwrap();
    }

    // ========================================================================
    // TC-MR-27: Router cache behavior
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr27_router_cache_behavior() {
        let config = RouterConfig::new(4);
        let router = ShardRouter::new(config);

        // First lookup should compute and cache
        let key = b"cache-test-key";
        let shard1 = router.shard_for_key(key);

        // Second lookup should use cache (same result)
        let shard2 = router.shard_for_key(key);
        assert_eq!(shard1, shard2);

        // Clear cache
        router.clear_cache();

        // Should still work after cache clear
        let shard3 = router.shard_for_key(key);
        assert_eq!(
            shard1, shard3,
            "Routing should be consistent after cache clear"
        );
    }

    // ========================================================================
    // TC-MR-28: Shard info completeness
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr28_shard_info_completeness() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(2)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        // Set up some state
        coordinator.set_shard_leader(0, 1);
        coordinator.set_shard_leader(1, 2);

        let shard0 = coordinator.get_shard(0).unwrap();
        shard0.add_member(1);
        shard0.add_member(2);
        shard0.add_member(3);
        shard0.set_term(5);
        shard0.set_commit_index(100);

        // Add some data to shard 0
        coordinator.put("info-test", "info-value").await.unwrap();

        // Run pending tasks
        for shard in coordinator.router().all_shards() {
            shard.storage().run_pending_tasks().await;
        }

        let info = shard0.info();
        assert_eq!(info.shard_id, 0);
        assert_eq!(info.state, ShardState::Active);
        assert_eq!(info.leader, Some(1));
        assert_eq!(info.members.len(), 3);
        assert_eq!(info.term, 5);
        assert_eq!(info.commit_index, 100);

        coordinator.shutdown().await.unwrap();
    }

    // ========================================================================
    // TC-MR-29: Empty shard operations
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr29_empty_shard_operations() {
        let coordinator = MultiRaftBuilder::new(1)
            .num_shards(4)
            .build_and_init()
            .await
            .expect("Failed to initialize coordinator");

        // Get from empty shard should return None
        let result = coordinator.get(b"nonexistent").await.unwrap();
        assert_eq!(result, None);

        // Delete from empty shard should succeed (no-op)
        coordinator.delete(b"nonexistent").await.unwrap();

        // Stats should reflect empty state
        let stats = coordinator.stats();
        assert_eq!(stats.total_entries, 0);

        coordinator.shutdown().await.unwrap();
    }

    // ========================================================================
    // TC-MR-30: Inactive shard behavior
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tc_mr30_inactive_shard_behavior() {
        let config = ShardConfig::new(0, 4);
        let shard = crate::multiraft::Shard::new(config)
            .await
            .expect("Failed to create shard");

        // Shard starts as initializing (inactive)
        assert!(!shard.is_active());

        // Put some data
        let _ = shard
            .put(Bytes::from("test-key"), Bytes::from("test-value"))
            .await;

        // Get should return None when shard is inactive
        let result = shard.get(b"test-key").await;
        assert_eq!(result, None, "Get should return None for inactive shard");

        // Activate the shard
        shard.set_state(ShardState::Active);
        assert!(shard.is_active());

        // Now put should work and be retrievable
        let _ = shard
            .put(Bytes::from("test-key2"), Bytes::from("test-value2"))
            .await;
        let result = shard.get(b"test-key2").await;
        assert_eq!(result, Some(Bytes::from("test-value2")));
    }
}
