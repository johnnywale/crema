//! Single-node migration E2E tests.
//!
//! These are faster, easier-to-debug equivalents of the multi-node migration tests
//! in `dashboard_e2e_tests.rs`. They use `MigrationTestHarness` and `MigrationAssertions`
//! to eliminate boilerplate, and run with `total_slots: 256` for ~4x speedup.

#[cfg(test)]
mod tests {
    use crate::testing::utils::allocate_os_ports;
    use crate::testing::{MigrationAssertions, MigrationTestHarness};
    use crate::{
        CacheConfig, DistributedCache, MultiRaftCacheConfig, NoOpClusterDiscovery, RaftConfig,
    };
    use std::sync::Arc;
    use std::time::Duration;

    /// Create a single-node cache with per-shard Raft enabled and configurable shard/slot counts.
    async fn create_single_node_cache(
        num_shards: u32,
        total_slots: usize,
    ) -> Arc<DistributedCache> {
        let ports = allocate_os_ports(&[1]).await;
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
            num_shards,
            shard_capacity: 10_000,
            auto_init_shards: false,
            leader_broadcast_debounce_ms: 200,
            per_shard_raft_enabled: true,
            total_slots,
            ..Default::default()
        };

        let discovery = NoOpClusterDiscovery::new(1, raft_addr);

        let config = CacheConfig::new(1, raft_addr)
            .with_max_capacity(100_000)
            .with_default_ttl(Duration::from_secs(3600))
            .with_raft_config(raft_config)
            .with_cluster_discovery(discovery)
            .with_multiraft_config(multiraft_config);

        Arc::new(DistributedCache::new(config).await.unwrap())
    }

    /// 4 shards → add 5th → migrate → assert all 20 MigrationAssertions.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sn_add_shard_with_migration() {
        let cache = create_single_node_cache(4, 256).await;
        let mut harness = MigrationTestHarness::from_caches(vec![cache.clone()]);

        harness.init_and_wait().await;
        harness.write_test_data(100, "key").await;
        harness.flush_shard_storages().await;

        let mut assertions = MigrationAssertions::capture_pre_migration(&harness.coordinator, 100);
        let add_result = harness.add_shard_and_migrate().await;
        assertions.capture_add_shard(&add_result, 5);
        assertions
            .capture_post_migration(&harness.coordinator, &cache)
            .await;
        assertions.assert_all();

        harness.shutdown_all().await;
    }

    /// 5 shards → remove shard 4 → verify data integrity and slot reassignment.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sn_remove_shard_with_migration() {
        let cache = create_single_node_cache(5, 256).await;
        let mut harness = MigrationTestHarness::from_caches(vec![cache.clone()]);

        harness.init_and_wait().await;
        harness.write_test_data(100, "key").await;
        harness.flush_shard_storages().await;

        let initial_epoch = harness.current_epoch();
        harness.remove_shard_and_migrate(4).await;

        harness.assert_all_readable().await;
        assert!(harness.current_epoch() > initial_epoch);
        harness.assert_active_shard_count(4);
        harness.assert_total_slots(256);
        harness.assert_shard_has_no_slots(4);

        harness.shutdown_all().await;
    }

    /// 4 shards → write 50 entries → add shard → migrate → verify checksums via assert_all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sn_checksum_validation() {
        let cache = create_single_node_cache(4, 256).await;
        let mut harness = MigrationTestHarness::from_caches(vec![cache.clone()]);

        harness.init_and_wait().await;
        harness.write_test_data(50, "key").await;
        harness.flush_shard_storages().await;

        let mut assertions = MigrationAssertions::capture_pre_migration(&harness.coordinator, 50);
        let add_result = harness.add_shard_and_migrate().await;
        assertions.capture_add_shard(&add_result, 5);
        assertions
            .capture_post_migration(&harness.coordinator, &cache)
            .await;
        assertions.assert_all();

        harness.shutdown_all().await;
    }

    /// Add shard → migrate → remove same shard → migrate → verify slot consistency.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sn_add_then_remove_slot_consistency() {
        let cache = create_single_node_cache(4, 256).await;
        let mut harness = MigrationTestHarness::from_caches(vec![cache.clone()]);

        harness.init_and_wait().await;
        harness.write_test_data(100, "key").await;

        let add_result = harness.add_shard_and_migrate().await;
        let new_shard_id = add_result.shard_id;
        harness.assert_all_readable().await;
        harness.assert_shard_has_slots(new_shard_id);

        harness.remove_shard_and_migrate(new_shard_id).await;
        harness.assert_all_readable().await;
        harness.assert_total_slots(256);
        harness.assert_shard_has_no_slots(new_shard_id);
        harness.assert_active_shard_count(4);

        harness.shutdown_all().await;
    }

    /// Concurrent writes during add-shard migration — verifies no deadlock and data integrity.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sn_concurrent_add_with_writes() {
        let cache = create_single_node_cache(4, 256).await;
        let mut harness = MigrationTestHarness::from_caches(vec![cache.clone()]);

        harness.init_and_wait().await;
        harness.write_test_data(50, "key").await;
        harness.start_migration_all();

        // Spawn concurrent writer
        let write_cache = cache.clone();
        let writer_handle = tokio::spawn(async move {
            let mut written = 0usize;
            for i in 0..30 {
                let key = format!("concurrent:{:04}", i);
                let value = format!("cval-{}", i);
                if write_cache.put(key, value).await.is_ok() {
                    written += 1;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            written
        });

        // Add shard while writes are ongoing
        tokio::time::sleep(Duration::from_millis(100)).await;
        let add_result = cache.add_shard().await.expect("Add shard should succeed");
        harness
            .wait_for_shard_leader(add_result.shard_id, Duration::from_secs(15))
            .await;

        writer_handle.await.expect("Writer task should not panic");
        harness
            .wait_for_migration_mostly_complete(Duration::from_secs(30), 5)
            .await;
        harness.settle().await;

        harness.assert_all_readable().await;
        if let Some(status) = harness.coordinator.slot_migration_status() {
            assert_eq!(status.failed_migrations, 0, "No migrations should fail");
        }

        harness.shutdown_all().await;
    }
}
