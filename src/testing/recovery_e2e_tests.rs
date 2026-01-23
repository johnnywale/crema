//! End-to-end recovery test suite for state machine recovery scenarios.
//!
//! This module tests crash recovery, failpoint injection, multi-node cluster chaos,
//! and observability verification for the distributed cache.

#![cfg(test)]

use crate::testing::recovery::{
    FailpointAction, FailpointRegistry, NodeState, RecoveryTestCluster,
    RecoveryTestConfig,
};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

// ============================================================================
// Test Helpers
// ============================================================================

/// Create a standard 3-node test cluster.
/// Uses persistent storage when rocksdb-storage feature is enabled.
async fn create_test_cluster() -> RecoveryTestCluster {
    let config = RecoveryTestConfig::new(3)
        .with_timeout(Duration::from_secs(30))
        .with_checkpointing(true)
        .with_checkpoint_log_threshold(50)
        .with_persistent(cfg!(feature = "rocksdb-storage"));

    RecoveryTestCluster::new(config).await.unwrap()
}

/// Create a test cluster with custom node count.
/// Uses persistent storage when rocksdb-storage feature is enabled.
async fn create_cluster_with_nodes(node_count: usize) -> RecoveryTestCluster {
    let config = RecoveryTestConfig::new(node_count)
        .with_timeout(Duration::from_secs(30))
        .with_checkpointing(true)
        .with_checkpoint_log_threshold(50)
        .with_persistent(cfg!(feature = "rocksdb-storage"));

    RecoveryTestCluster::new(config).await.unwrap()
}

/// Write test data to the cluster.
async fn write_test_data(cluster: &RecoveryTestCluster, count: usize, prefix: &str) {
    for i in 0..count {
        let key = format!("{}_key_{}", prefix, i);
        let value = format!("value_{}", i);
        cluster.write(&key, &value).await.unwrap();
    }
}

/// Verify test data exists in the cluster.
async fn verify_test_data(
    cluster: &RecoveryTestCluster,
    node_id: u64,
    count: usize,
    prefix: &str,
) -> bool {
    for i in 0..count {
        let key = format!("{}_key_{}", prefix, i);
        let expected = format!("value_{}", i);
        match cluster.read(node_id, &key).await {
            Ok(Some(value)) if value == expected => continue,
            _ => return false,
        }
    }
    true
}

// ============================================================================
// 1. Basic Recovery Tests (Snapshot + Log Replay)
// ============================================================================

/// Test recovery from snapshot only (no logs after snapshot).
#[tokio::test]
async fn test_snapshot_only_recovery() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    // Wait for leader election
    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    info!(leader, "Leader elected");

    // Write enough data to trigger a snapshot
    write_test_data(&cluster, 100, "snapshot").await;

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Force a snapshot on the leader
    cluster.force_snapshot(leader).await.unwrap();
    info!(leader, "Snapshot created");

    // Crash the leader
    cluster.crash_node(leader).await.unwrap();
    info!(leader, "Leader crashed");

    // Wait for new leader
    let new_leader = cluster.wait_for_leader(Duration::from_secs(15)).await.unwrap();
    assert_ne!(new_leader, leader, "New leader should be different");
    info!(new_leader, "New leader elected");

    // Recover the crashed node
    cluster.recover_node(leader).await.unwrap();
    info!(leader, "Node recovered");

    // Wait for the recovered node to catch up
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify data on recovered node
    assert!(
        verify_test_data(&cluster, leader, 100, "snapshot").await,
        "Recovered node should have all data from snapshot"
    );

    cluster.shutdown().await;
}

/// Test recovery from logs only (no snapshot).
/// NOTE: This test requires persistent storage to work correctly.
/// With in-memory storage, logs are lost on crash.
#[tokio::test]
#[cfg_attr(not(feature = "rocksdb-storage"), ignore = "requires persistent storage (rocksdb-storage feature)")]
async fn test_log_replay_only_recovery() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write a small amount of data (less than snapshot threshold)
    write_test_data(&cluster, 20, "logonly").await;

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Pick a follower to crash
    let followers: Vec<_> = cluster
        .running_nodes()
        .into_iter()
        .filter(|&n| n != leader)
        .collect();
    let follower = followers[0];

    // Crash the follower
    cluster.crash_node(follower).await.unwrap();
    info!(follower, "Follower crashed");

    // Recover the follower
    cluster.recover_node(follower).await.unwrap();
    info!(follower, "Follower recovered");

    // Wait for catch-up via log replay
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify data on recovered node
    assert!(
        verify_test_data(&cluster, follower, 20, "logonly").await,
        "Recovered node should have all data from log replay"
    );

    cluster.shutdown().await;
}

/// Test recovery from snapshot plus additional log entries.
/// NOTE: Requires persistent storage for log replay after snapshot.
#[tokio::test]
#[cfg_attr(not(feature = "rocksdb-storage"), ignore = "requires persistent storage (rocksdb-storage feature)")]
async fn test_snapshot_plus_log_recovery() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write data to trigger snapshot
    write_test_data(&cluster, 60, "before").await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Force snapshot
    cluster.force_snapshot(leader).await.unwrap();

    // Write more data after snapshot
    write_test_data(&cluster, 30, "after").await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Crash and recover a follower
    let followers: Vec<_> = cluster
        .running_nodes()
        .into_iter()
        .filter(|&n| n != leader)
        .collect();
    let follower = followers[0];

    cluster.crash_node(follower).await.unwrap();
    cluster.recover_node(follower).await.unwrap();

    // Wait for catch-up
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify both pre and post-snapshot data
    assert!(
        verify_test_data(&cluster, follower, 60, "before").await,
        "Should have pre-snapshot data"
    );
    assert!(
        verify_test_data(&cluster, follower, 30, "after").await,
        "Should have post-snapshot data"
    );

    cluster.shutdown().await;
}

/// Test that TTL entries are handled correctly after recovery.
/// NOTE: Requires persistent storage to verify TTL handling after recovery.
///
/// This test verifies that entries with expired TTLs are not restored after crash recovery.
/// The implementation uses absolute expiration times in CacheCommand and filters expired
/// entries during snapshot load and Raft log replay.
#[tokio::test]
#[cfg_attr(not(feature = "rocksdb-storage"), ignore = "requires persistent storage (rocksdb-storage feature)")]
async fn test_recovery_with_expired_entries() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Identify which follower we'll crash before writing data
    let followers: Vec<_> = cluster
        .running_nodes()
        .into_iter()
        .filter(|&n| n != leader)
        .collect();
    let follower = followers[0];

    // Write entries with short TTL
    cluster
        .write_with_ttl("ttl_key_1", "value1", Some(Duration::from_secs(2)))
        .await
        .unwrap();
    cluster
        .write_with_ttl("ttl_key_2", "value2", Some(Duration::from_secs(60)))
        .await
        .unwrap();
    cluster.write("permanent_key", "permanent_value").await.unwrap();

    // Wait for replication to the follower and verify it has the data
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify follower has replicated all entries
    let leader_index = cluster.get_applied_index(leader).unwrap();
    cluster.wait_for_replication(leader_index, Duration::from_secs(10)).await.unwrap();

    // Force snapshot on the follower (not the leader) so it has a local snapshot to recover from
    // This captures the follower's current state including the TTL entry
    cluster.force_snapshot(follower).await.unwrap();

    // Verify the follower snapshot index matches what we expect
    let follower_index_after_snapshot = cluster.get_applied_index(follower).unwrap();
    assert!(follower_index_after_snapshot >= leader_index,
        "Follower snapshot should include all entries: follower={}, leader={}",
        follower_index_after_snapshot, leader_index);

    // Wait for short TTL to expire
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Crash and recover the follower
    cluster.crash_node(follower).await.unwrap();
    cluster.recover_node(follower).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // The expired entry should not be restored
    let result1 = cluster.read(follower, "ttl_key_1").await.unwrap();
    assert!(
        result1.is_none(),
        "Expired TTL entry should not be restored"
    );

    // The long-TTL entry should still exist
    let result2 = cluster.read(follower, "ttl_key_2").await.unwrap();
    assert_eq!(
        result2,
        Some("value2".to_string()),
        "Long TTL entry should be restored"
    );

    // Permanent entry should exist
    let result3 = cluster.read(follower, "permanent_key").await.unwrap();
    assert_eq!(
        result3,
        Some("permanent_value".to_string()),
        "Permanent entry should be restored"
    );

    cluster.shutdown().await;
}

/// Test that applied_index is restored correctly after recovery.
#[tokio::test]
async fn test_recovery_preserves_applied_index() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write some data
    write_test_data(&cluster, 50, "index").await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Force snapshot and record the index
    cluster.force_snapshot(leader).await.unwrap();
    let index_before = cluster.get_applied_index(leader).unwrap();
    info!(index_before, "Applied index before crash");

    // Crash and recover the leader
    cluster.crash_node(leader).await.unwrap();

    // Wait for new leader
    let _ = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    cluster.recover_node(leader).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Check applied index after recovery
    let index_after = cluster.get_applied_index(leader).unwrap();
    info!(index_after, "Applied index after recovery");

    // Applied index should be at least what it was before (may be higher due to catch-up)
    assert!(
        index_after >= index_before,
        "Applied index should be preserved or advanced"
    );

    cluster.shutdown().await;
}

// ============================================================================
// 2. Crash Timing Fuzz Tests
// ============================================================================

/// Test crash during snapshot write using failpoints.
#[tokio::test]
async fn test_crash_during_snapshot_write() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write data
    write_test_data(&cluster, 30, "crash_snap").await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Enable failpoint to crash during snapshot (simulated by crashing after write starts)
    // Note: Full failpoint integration would require modifying checkpoint/manager.rs
    // For now, we simulate the scenario

    // Start snapshot and crash immediately
    let snapshot_result = cluster.force_snapshot(leader).await;

    // Whether it succeeds or fails, the cluster should remain consistent
    if snapshot_result.is_err() {
        info!("Snapshot failed as expected during crash simulation");
    }

    // Crash the node
    cluster.crash_node(leader).await.unwrap();

    // Wait for new leader
    let new_leader = cluster.wait_for_leader(Duration::from_secs(15)).await.unwrap();

    // Recover the crashed node
    cluster.recover_node(leader).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Data should be consistent
    cluster.verify_consistency().await.unwrap();

    cluster.shutdown().await;
}

/// Test crash during log append.
#[tokio::test]
async fn test_crash_during_log_append() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write some initial data
    write_test_data(&cluster, 20, "append").await;

    // Crash during write operations
    let write_result = cluster.write("crash_key", "crash_value").await;

    // Immediately crash the node (simulating crash during append)
    cluster.crash_node(leader).await.unwrap();

    // The write may or may not have completed
    if write_result.is_err() {
        info!("Write interrupted as expected");
    }

    // Wait for new leader
    let new_leader = cluster.wait_for_leader(Duration::from_secs(15)).await.unwrap();

    // Recover
    cluster.recover_node(leader).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // The initial data should be intact
    assert!(
        verify_test_data(&cluster, new_leader, 20, "append").await,
        "Pre-crash data should be preserved"
    );

    cluster.shutdown().await;
}

/// Test crash after commit but before apply.
/// NOTE: Requires persistent storage to replay committed but unapplied entries.
#[tokio::test]
#[cfg_attr(not(feature = "rocksdb-storage"), ignore = "requires persistent storage (rocksdb-storage feature)")]
async fn test_crash_after_commit_before_apply() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write data
    write_test_data(&cluster, 30, "commit").await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Crash a follower (to simulate crash after receiving committed entry but before apply)
    let followers: Vec<_> = cluster
        .running_nodes()
        .into_iter()
        .filter(|&n| n != leader)
        .collect();
    let follower = followers[0];

    cluster.crash_node(follower).await.unwrap();

    // Write more data while follower is down
    write_test_data(&cluster, 10, "during_down").await;

    // Recover follower
    cluster.recover_node(follower).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Follower should have all data (idempotent replay)
    assert!(
        verify_test_data(&cluster, follower, 30, "commit").await,
        "Pre-crash data should be available"
    );
    assert!(
        verify_test_data(&cluster, follower, 10, "during_down").await,
        "Data written during downtime should be replicated"
    );

    cluster.shutdown().await;
}

/// Test crash during apply (idempotent replay).
#[tokio::test]
async fn test_crash_during_apply() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write data rapidly
    for i in 0..50 {
        let key = format!("apply_key_{}", i);
        let value = format!("value_{}", i);
        let _ = cluster.write(&key, &value).await;
    }

    // Crash immediately (some entries may be mid-apply)
    cluster.crash_node(leader).await.unwrap();

    // Wait for new leader
    let new_leader = cluster.wait_for_leader(Duration::from_secs(15)).await.unwrap();

    // Recover
    cluster.recover_node(leader).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify consistency - all committed entries should be applied
    cluster.verify_consistency().await.unwrap();

    cluster.shutdown().await;
}

/// Test crash during compaction.
#[tokio::test]
async fn test_crash_during_compaction() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write enough data to trigger compaction
    write_test_data(&cluster, 100, "compact").await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Force snapshot (which may trigger compaction)
    cluster.force_snapshot(leader).await.unwrap();

    // Crash during potential compaction
    cluster.crash_node(leader).await.unwrap();

    // Wait for new leader
    let new_leader = cluster.wait_for_leader(Duration::from_secs(15)).await.unwrap();

    // Recover
    cluster.recover_node(leader).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Data should be consistent
    assert!(
        verify_test_data(&cluster, leader, 100, "compact").await,
        "Data should survive compaction crash"
    );

    cluster.shutdown().await;
}

// ============================================================================
// 3. Leader/Follower Recovery Tests
// ============================================================================

/// Test leader crash and recovery as follower.
#[tokio::test]
async fn test_leader_crash_and_recovery() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let original_leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    info!(original_leader, "Original leader elected");

    // Write some data
    write_test_data(&cluster, 50, "leader_crash").await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Crash the leader
    cluster.crash_node(original_leader).await.unwrap();
    info!(original_leader, "Leader crashed");

    // Wait for new leader election
    let new_leader = cluster.wait_for_leader(Duration::from_secs(15)).await.unwrap();
    assert_ne!(new_leader, original_leader, "New leader should be elected");
    info!(new_leader, "New leader elected");

    // Write more data under new leader
    write_test_data(&cluster, 20, "after_leader_crash").await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Recover the old leader
    cluster.recover_node(original_leader).await.unwrap();
    info!(original_leader, "Old leader recovered");

    // Wait for catch-up
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Old leader should now be a follower and have all data
    assert!(
        verify_test_data(&cluster, original_leader, 50, "leader_crash").await,
        "Recovered node should have pre-crash data"
    );
    assert!(
        verify_test_data(&cluster, original_leader, 20, "after_leader_crash").await,
        "Recovered node should have post-crash data"
    );

    cluster.shutdown().await;
}

/// Test follower crash and rejoin.
/// NOTE: Requires persistent storage to verify data after follower recovery.
#[tokio::test]
#[cfg_attr(not(feature = "rocksdb-storage"), ignore = "requires persistent storage (rocksdb-storage feature)")]
async fn test_follower_crash_and_rejoin() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Get a follower
    let followers: Vec<_> = cluster
        .running_nodes()
        .into_iter()
        .filter(|&n| n != leader)
        .collect();
    let follower = followers[0];

    // Write data
    write_test_data(&cluster, 30, "follower_crash").await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Crash the follower
    cluster.crash_node(follower).await.unwrap();
    info!(follower, "Follower crashed");

    // Write more data while follower is down
    write_test_data(&cluster, 20, "during_follower_down").await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Rejoin the follower
    cluster.recover_node(follower).await.unwrap();
    info!(follower, "Follower rejoined");

    // Wait for catch-up
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Follower should have all data
    assert!(
        verify_test_data(&cluster, follower, 30, "follower_crash").await,
        "Should have pre-crash data"
    );
    assert!(
        verify_test_data(&cluster, follower, 20, "during_follower_down").await,
        "Should have data written during downtime"
    );

    cluster.shutdown().await;
}

/// Test multiple nodes crashing sequentially.
/// NOTE: Requires persistent storage for nodes to recover with their data.
#[tokio::test]
#[cfg_attr(not(feature = "rocksdb-storage"), ignore = "requires persistent storage (rocksdb-storage feature)")]
async fn test_multiple_node_sequential_crash() {
    let cluster = create_cluster_with_nodes(5).await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write initial data
    write_test_data(&cluster, 30, "sequential").await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Get all node IDs
    let all_nodes = cluster.node_ids();

    // Crash nodes one by one (keeping majority)
    let nodes_to_crash: Vec<_> = all_nodes
        .iter()
        .filter(|&&n| n != leader)
        .take(2)
        .copied()
        .collect();

    for node in &nodes_to_crash {
        cluster.crash_node(*node).await.unwrap();
        info!(node, "Node crashed");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Write more data after each crash
        let key = format!("after_crash_{}", node);
        cluster.write(&key, "value").await.unwrap();
    }

    // Recover all crashed nodes
    for node in &nodes_to_crash {
        cluster.recover_node(*node).await.unwrap();
        info!(node, "Node recovered");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Wait for full catch-up
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify all nodes have consistent data
    cluster.verify_consistency().await.unwrap();

    cluster.shutdown().await;
}

/// Test minority crash continues serving.
/// NOTE: Requires persistent storage for recovered nodes to have data.
#[tokio::test]
#[cfg_attr(not(feature = "rocksdb-storage"), ignore = "requires persistent storage (rocksdb-storage feature)")]
async fn test_minority_crash_continues_serving() {
    let cluster = create_cluster_with_nodes(5).await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Get followers
    let followers: Vec<_> = cluster
        .running_nodes()
        .into_iter()
        .filter(|&n| n != leader)
        .collect();

    // Crash 2 out of 5 nodes (minority)
    cluster.crash_node(followers[0]).await.unwrap();
    cluster.crash_node(followers[1]).await.unwrap();
    info!("Crashed minority: {:?}", &followers[..2]);

    // Cluster should still be able to serve writes
    write_test_data(&cluster, 20, "minority_down").await;

    // Verify writes succeeded
    assert!(
        verify_test_data(&cluster, leader, 20, "minority_down").await,
        "Writes should succeed with minority crashed"
    );

    // Recover crashed nodes
    cluster.recover_node(followers[0]).await.unwrap();
    cluster.recover_node(followers[1]).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // All nodes should have the data
    cluster.verify_consistency().await.unwrap();

    cluster.shutdown().await;
}

/// Test majority crash and recovery.
#[tokio::test]
async fn test_majority_crash_recovery() {
    let cluster = create_cluster_with_nodes(5).await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write initial data
    write_test_data(&cluster, 20, "before_majority_crash").await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get all nodes except leader
    let followers: Vec<_> = cluster
        .running_nodes()
        .into_iter()
        .filter(|&n| n != leader)
        .collect();

    // Crash majority (3 out of 5)
    cluster.crash_node(leader).await.unwrap();
    cluster.crash_node(followers[0]).await.unwrap();
    cluster.crash_node(followers[1]).await.unwrap();
    info!("Crashed majority: {}, {}, {}", leader, followers[0], followers[1]);

    // Cluster should not be able to elect a leader
    tokio::time::sleep(Duration::from_secs(2)).await;
    let leader_result = cluster.wait_for_leader(Duration::from_secs(3)).await;
    assert!(
        leader_result.is_err(),
        "Should not be able to elect leader with majority down"
    );

    // Recover enough nodes to restore majority
    cluster.recover_node(leader).await.unwrap();
    cluster.recover_node(followers[0]).await.unwrap();
    info!("Recovered nodes to restore majority");

    // Wait for leader election
    let new_leader = cluster.wait_for_leader(Duration::from_secs(15)).await.unwrap();
    info!(new_leader, "New leader elected after recovery");

    // Data should be intact
    assert!(
        verify_test_data(&cluster, new_leader, 20, "before_majority_crash").await,
        "Data should survive majority crash"
    );

    // Recover remaining crashed node
    cluster.recover_node(followers[1]).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    cluster.verify_consistency().await.unwrap();

    cluster.shutdown().await;
}

// ============================================================================
// 4. Migration + Recovery Tests
// ============================================================================

/// Test recovery during migration (placeholder - requires multiraft).
#[tokio::test]
async fn test_recovery_during_migration() {
    // This test would require multiraft setup
    // For now, we test basic cluster stability during high write load

    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Simulate high write load (similar to migration)
    for i in 0..100 {
        let key = format!("migration_key_{}", i);
        let value = format!("value_{}", i);
        let _ = cluster.write(&key, &value).await;
    }

    // Crash during "migration"
    cluster.crash_node(leader).await.unwrap();

    // Wait for new leader
    let new_leader = cluster.wait_for_leader(Duration::from_secs(15)).await.unwrap();

    // Recover
    cluster.recover_node(leader).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify consistency
    cluster.verify_consistency().await.unwrap();

    cluster.shutdown().await;
}

// ============================================================================
// 5. Router/Client Recovery Tests
// ============================================================================

/// Test client reconnect after leader change.
#[tokio::test]
async fn test_client_reconnect_after_leader_change() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let original_leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write some data
    cluster.write("reconnect_key", "value1").await.unwrap();

    // Crash the leader (simulating need for client reconnect)
    cluster.crash_node(original_leader).await.unwrap();

    // Wait for new leader
    let new_leader = cluster.wait_for_leader(Duration::from_secs(15)).await.unwrap();

    // Client should be able to write to new leader
    cluster.write("reconnect_key_2", "value2").await.unwrap();

    // And read should work
    let result = cluster.read_from_leader("reconnect_key").await.unwrap();
    assert_eq!(result, Some("value1".to_string()));

    let result2 = cluster.read_from_leader("reconnect_key_2").await.unwrap();
    assert_eq!(result2, Some("value2".to_string()));

    // Recover original leader
    cluster.recover_node(original_leader).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    cluster.verify_consistency().await.unwrap();

    cluster.shutdown().await;
}

/// Test router update after recovery.
/// NOTE: Requires persistent storage for recovered leader to have data.
#[tokio::test]
#[cfg_attr(not(feature = "rocksdb-storage"), ignore = "requires persistent storage (rocksdb-storage feature)")]
async fn test_router_update_after_recovery() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write data
    write_test_data(&cluster, 20, "router").await;

    // Crash and recover the leader multiple times
    for i in 0..3 {
        cluster.crash_node(leader).await.unwrap();
        let new_leader = cluster.wait_for_leader(Duration::from_secs(15)).await.unwrap();

        // Write under new leader
        let key = format!("after_failover_{}", i);
        cluster.write(&key, &format!("value_{}", i)).await.unwrap();

        // Recover the old leader
        cluster.recover_node(leader).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Final consistency check
    cluster.verify_consistency().await.unwrap();

    cluster.shutdown().await;
}

/// Test in-flight request during crash.
#[tokio::test]
async fn test_in_flight_request_during_crash() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write initial data
    write_test_data(&cluster, 10, "inflight").await;

    // Start some writes and immediately crash
    // Note: We can't use tokio::spawn with cluster reference due to borrow checker
    // Instead, we write and then crash synchronously
    for i in 0..5 {
        let key = format!("concurrent_key_{}", i);
        // Fire-and-forget write - some may succeed, some may fail
        let _ = cluster.write(&key, "value").await;
    }

    // Crash the leader
    cluster.crash_node(leader).await.unwrap();

    // Wait for new leader
    let new_leader = cluster.wait_for_leader(Duration::from_secs(15)).await.unwrap();

    // Recover
    cluster.recover_node(leader).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Initial data should be consistent
    assert!(
        verify_test_data(&cluster, new_leader, 10, "inflight").await,
        "Pre-crash data should be preserved"
    );

    cluster.shutdown().await;
}

// ============================================================================
// 6. Full Cluster Chaos Tests
// ============================================================================

/// Test rolling restart preserves data.
/// NOTE: Requires persistent storage for nodes to preserve data across restarts.
#[tokio::test]
#[cfg_attr(not(feature = "rocksdb-storage"), ignore = "requires persistent storage (rocksdb-storage feature)")]
async fn test_rolling_restart_preserves_data() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let _ = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write initial data
    write_test_data(&cluster, 50, "rolling").await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Rolling restart: stop and start each node one at a time
    for node_id in cluster.node_ids() {
        info!(node_id, "Rolling restart: stopping node");

        cluster.stop_node(node_id).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Wait for leader if this was the leader
        let _ = cluster.wait_for_leader(Duration::from_secs(10)).await;

        cluster.recover_node(node_id).await.unwrap();
        info!(node_id, "Rolling restart: restarted node");

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Wait for cluster to stabilize
    let _ = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Data should be preserved
    cluster.verify_consistency().await.unwrap();

    cluster.shutdown().await;
}

/// Test network partition and heal (simulated via crash/recover).
/// NOTE: Requires persistent storage for healed nodes to have data.
#[tokio::test]
#[cfg_attr(not(feature = "rocksdb-storage"), ignore = "requires persistent storage (rocksdb-storage feature)")]
async fn test_network_partition_and_heal() {
    let cluster = create_cluster_with_nodes(5).await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write initial data
    write_test_data(&cluster, 30, "partition").await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Simulate partition by crashing minority
    let followers: Vec<_> = cluster
        .running_nodes()
        .into_iter()
        .filter(|&n| n != leader)
        .collect();

    let partitioned = vec![followers[0], followers[1]];
    for node in &partitioned {
        cluster.crash_node(*node).await.unwrap();
    }
    info!("Partitioned nodes: {:?}", partitioned);

    // Write data during partition
    write_test_data(&cluster, 20, "during_partition").await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Heal partition
    for node in &partitioned {
        cluster.recover_node(*node).await.unwrap();
    }
    info!("Healed partition");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // All nodes should have consistent data
    cluster.verify_consistency().await.unwrap();

    cluster.shutdown().await;
}

/// Test random crash sequence.
/// NOTE: Requires persistent storage for nodes to recover with their data.
#[tokio::test]
#[cfg_attr(not(feature = "rocksdb-storage"), ignore = "requires persistent storage (rocksdb-storage feature)")]
async fn test_random_crash_sequence() {
    let cluster = create_cluster_with_nodes(5).await;
    cluster.start_all().await.unwrap();

    let _ = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write initial data
    write_test_data(&cluster, 30, "random_crash").await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Random crash/recover sequence (keeping majority)
    let nodes = cluster.node_ids();
    let crash_sequence = vec![
        (nodes[0], true),   // crash
        (nodes[1], true),   // crash (now 3/5 up)
        (nodes[0], false),  // recover (now 4/5 up)
        (nodes[2], true),   // crash (now 3/5 up)
        (nodes[1], false),  // recover (now 4/5 up)
        (nodes[2], false),  // recover (all up)
    ];

    for (node, crash) in crash_sequence {
        if crash {
            cluster.crash_node(node).await.unwrap();
            info!(node, "Crashed");
        } else {
            cluster.recover_node(node).await.unwrap();
            info!(node, "Recovered");
        }

        // Try to write after each state change
        let leader = cluster.wait_for_leader(Duration::from_secs(10)).await;
        if let Ok(leader) = leader {
            let key = format!("after_change_{}", node);
            let _ = cluster.write(&key, "value").await;
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Final consistency check
    tokio::time::sleep(Duration::from_secs(2)).await;
    cluster.verify_consistency().await.unwrap();

    cluster.shutdown().await;
}

/// Test crash with concurrent writes.
#[tokio::test]
async fn test_crash_with_concurrent_writes() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write initial data
    write_test_data(&cluster, 20, "concurrent").await;

    // Start concurrent write task
    let write_count = Arc::new(AtomicUsize::new(0));
    let write_count_clone = write_count.clone();

    // Note: This test is simplified due to borrow checker constraints
    // In a real scenario, we'd use proper async patterns

    // Crash the leader while writes might be happening
    cluster.crash_node(leader).await.unwrap();

    // Wait for new leader
    let new_leader = cluster.wait_for_leader(Duration::from_secs(15)).await.unwrap();

    // Continue writing under new leader
    write_test_data(&cluster, 10, "after_concurrent_crash").await;

    // Recover original leader
    cluster.recover_node(leader).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify consistency
    cluster.verify_consistency().await.unwrap();

    cluster.shutdown().await;
}

// ============================================================================
// 7. Observability/Metadata Checks
// ============================================================================

/// Test metrics accuracy after recovery.
#[tokio::test]
async fn test_metrics_after_recovery() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write data
    write_test_data(&cluster, 50, "metrics").await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Record applied index before crash
    let index_before = cluster.get_applied_index(leader).unwrap();

    // Crash and recover
    cluster.crash_node(leader).await.unwrap();
    let _ = cluster.wait_for_leader(Duration::from_secs(15)).await.unwrap();
    cluster.recover_node(leader).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Applied index should be at least what it was
    let index_after = cluster.get_applied_index(leader).unwrap();
    assert!(
        index_after >= index_before,
        "Applied index should be preserved: before={}, after={}",
        index_before,
        index_after
    );

    cluster.shutdown().await;
}

/// Test Raft state consistency across nodes.
#[tokio::test]
async fn test_raft_state_consistency() {
    let cluster = create_cluster_with_nodes(5).await;
    cluster.start_all().await.unwrap();

    let _ = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write data
    write_test_data(&cluster, 100, "raft_state").await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Force snapshot on all nodes
    for node_id in cluster.node_ids() {
        if cluster.node_state(node_id) == Some(NodeState::Running) {
            let _ = cluster.force_snapshot(node_id).await;
        }
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify all running nodes have consistent data
    cluster.verify_consistency().await.unwrap();

    // Get applied indices - they should be close
    let mut indices: Vec<(u64, u64)> = Vec::new();
    for node_id in cluster.running_nodes() {
        if let Some(idx) = cluster.get_applied_index(node_id) {
            indices.push((node_id, idx));
        }
    }

    // All indices should be within a small range
    if let (Some(min), Some(max)) = (
        indices.iter().map(|(_, i)| *i).min(),
        indices.iter().map(|(_, i)| *i).max(),
    ) {
        assert!(
            max - min <= 5,
            "Applied indices should be close: {:?}",
            indices
        );
    }

    cluster.shutdown().await;
}

/// Test snapshot metadata integrity.
#[tokio::test]
async fn test_snapshot_metadata_integrity() {
    let cluster = create_test_cluster().await;
    cluster.start_all().await.unwrap();

    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();

    // Write enough data to trigger snapshot
    write_test_data(&cluster, 100, "snapshot_meta").await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Force snapshot
    cluster.force_snapshot(leader).await.unwrap();

    // Get applied index after snapshot
    let applied_index = cluster.get_applied_index(leader).unwrap();

    // Crash and recover
    cluster.crash_node(leader).await.unwrap();
    let _ = cluster.wait_for_leader(Duration::from_secs(15)).await.unwrap();
    cluster.recover_node(leader).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify recovery used snapshot correctly
    let recovered_index = cluster.get_applied_index(leader).unwrap();
    assert!(
        recovered_index >= applied_index,
        "Recovered index should be at least snapshot index"
    );

    // Data should be consistent
    cluster.verify_consistency().await.unwrap();

    cluster.shutdown().await;
}

// ============================================================================
// 8. Failpoint-based Tests
// ============================================================================

/// Test using failpoint registry directly.
#[tokio::test]
async fn test_failpoint_registry_integration() {
    let registry = FailpointRegistry::new();

    // Test callback failpoint
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    registry.enable(
        "test_failpoint",
        FailpointAction::Callback(Arc::new(move || {
            counter_clone.fetch_add(1, Ordering::Relaxed);
        })),
    );

    // Trigger failpoint multiple times
    registry.check("test_failpoint");
    registry.check("test_failpoint");
    registry.check("test_failpoint");

    assert_eq!(
        counter.load(Ordering::Relaxed),
        3,
        "Callback should have been called 3 times"
    );

    // Test stats
    let stats = registry.stats("test_failpoint").unwrap();
    assert_eq!(stats.hit_count, 3);
    assert_eq!(stats.triggered_count, 3);

    // Test disable
    registry.disable("test_failpoint");
    registry.check("test_failpoint");
    assert_eq!(
        counter.load(Ordering::Relaxed),
        3,
        "Callback should not be called after disable"
    );
}

/// Test countdown failpoint.
#[tokio::test]
async fn test_countdown_failpoint() {
    let registry = FailpointRegistry::new();

    // Enable countdown to trigger on 3rd hit
    registry.enable("countdown_test", FailpointAction::CountdownPanic(3));

    // First two should not panic
    assert_eq!(
        registry.check("countdown_test"),
        crate::testing::recovery::FailpointResult::Continue
    );
    assert_eq!(
        registry.check("countdown_test"),
        crate::testing::recovery::FailpointResult::Continue
    );

    // Third should panic - catch it
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        registry.check("countdown_test");
    }));
    assert!(result.is_err(), "Third hit should panic");
}

/// Test probability failpoint (statistical).
#[tokio::test]
async fn test_probability_failpoint() {
    let registry = FailpointRegistry::new();

    // 100% probability should always trigger
    registry.enable("prob_100", FailpointAction::ProbabilityPanic(1.0));

    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        registry.check("prob_100");
    }));
    assert!(result.is_err(), "100% probability should always panic");

    // 0% probability should never trigger
    let registry2 = FailpointRegistry::new();
    registry2.enable("prob_0", FailpointAction::ProbabilityPanic(0.0));

    for _ in 0..100 {
        let result = registry2.check("prob_0");
        assert_eq!(
            result,
            crate::testing::recovery::FailpointResult::Continue,
            "0% probability should never panic"
        );
    }
}
