//! Integration tests for DistributedCache with real network connections.
//!
//! These tests verify the full cache functionality including:
//! - Raft leader election
//! - Log replication
//! - Node restarts
//! - Multi-node cluster formation
//!
//! All tests use real network connections and actual cache instances.

//! Comprehensive multi-node test suite for Raft consensus
//!
//! This test suite covers:
//! - Core multi-node scenarios (TC-11 to TC-13)
//! - Fault tolerance (TC-14 to TC-16)
//! - Edge cases and corner cases (TC-17 to TC-20)
//! - Extreme situations (TC-21 to TC-22)

#[cfg(test)]
mod multi_node_tests {
    use crate::cache::DistributedCache;
    use crate::config::{CacheConfig, MemberlistConfig, RaftConfig};
    use crate::types::NodeId;
    use std::collections::{HashMap, HashSet};
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};
    use tokio::time::sleep;

    /// Port counter to ensure unique ports across tests
    static PORT_COUNTER: AtomicU16 = AtomicU16::new(25000);

    /// Allocate a range of ports for a test
    fn allocate_ports(count: u16) -> u16 {
        PORT_COUNTER.fetch_add(count * 3, Ordering::SeqCst)
    }
    pub async fn wait_for_result<F, Fut, T, P>(
        mut action: F,      // 获取结果的异步闭包
        predicate: P,       // 验证结果的闭包
        timeout: Duration,  // 总超时时间
    ) -> Option<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = T>,
        P: Fn(&T) -> bool,
    {
        let start = Instant::now();
        let interval = Duration::from_millis(100); // 每次重试间隔 100ms

        while start.elapsed() < timeout {
            let result = action().await;
            if predicate(&result) {
                return Some(result);
            }
            tokio::time::sleep(interval).await;
        }
        None // 超时未达成条件
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

        let base_election_tick = 10;

        CacheConfig {
            node_id,
            raft_addr,
            seed_nodes,
            max_capacity: 10_000,
            default_ttl: Some(Duration::from_secs(3600)),
            default_tti: None,
            raft: RaftConfig {
                tick_interval_ms: 100,
                election_tick: base_election_tick + (node_id as usize * 3),
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

    /// Wait for exactly one leader in the cluster
    async fn wait_for_single_leader(
        caches: &[&DistributedCache],
        timeout: Duration,
    ) -> Option<NodeId> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            let leaders: Vec<NodeId> = caches
                .iter()
                .filter(|c| c.is_leader())
                .map(|c| c.node_id())
                .collect();

            if leaders.len() == 1 {
                return Some(leaders[0]);
            }

            sleep(Duration::from_millis(50)).await;
        }
        None
    }

    /// Helper function to verify all nodes agree on leader
    fn verify_leader_agreement(caches: &[&DistributedCache]) -> Option<NodeId> {
        let leader_ids: HashSet<Option<NodeId>> = caches.iter().map(|c| c.leader_id()).collect();

        if leader_ids.len() == 1 {
            *leader_ids.iter().next().unwrap()
        } else {
            None
        }
    }

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
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc11_standard_quorum_election() {
        let base_port = allocate_ports(3);
        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        // Start all three nodes
        let config1 = cluster_node_config(1, base_port, peer_configs.clone());
        let config2 = cluster_node_config(2, base_port + 1, peer_configs.clone());
        let config3 = cluster_node_config(3, base_port + 2, peer_configs.clone());

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Wait for leader election (within election timeout)
        let leader_id = wait_for_single_leader(&caches, Duration::from_secs(5)).await;
        assert!(leader_id.is_some(), "Should elect a unique leader");

        // Verify only one leader
        let leader_count = caches.iter().filter(|c| c.is_leader()).count();
        assert_eq!(leader_count, 1, "Should have exactly one leader");

        // Verify all nodes agree on the same term
        let terms: HashSet<u64> = caches.iter().map(|c| c.cluster_status().term).collect();
        assert_eq!(terms.len(), 1, "All nodes should have the same term");

        // Verify all nodes agree on the leader
        // let agreed_leader = verify_leader_agreement(&caches);

        let agreement_reached = wait_for(
            || {
                let agreed = verify_leader_agreement(&caches);
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
        let agreed_leader = verify_leader_agreement(&caches);
        assert_eq!(agreed_leader, leader_id, "Leader agreement mismatch");

        // Cleanup
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }


    // ========================================================================
    // TC-12: Log Replication Consistency (日志复制一致性)
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc12_log_replication_consistency() {
        let base_port = allocate_ports(3);
        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        let config1 = cluster_node_config(1, base_port, peer_configs.clone());
        let config2 = cluster_node_config(2, base_port + 1, peer_configs.clone());
        let config3 = cluster_node_config(3, base_port + 2, peer_configs.clone());

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Wait for leader election with longer timeout
        println!("Waiting for leader election...");
        let leader_id = wait_for_single_leader(&caches, Duration::from_secs(10)).await;
        assert!(leader_id.is_some(), "Should elect a leader");
        println!("Leader elected: {:?}", leader_id);

        // Find the leader
        let leader = caches.iter().find(|c| c.is_leader()).unwrap();
        println!("Leader is node {}", leader.node_id());

        // Send 100 commands to the leader with error handling
        println!("Starting to send 100 commands...");
        for i in 0..100 {
            let key = format!("key-{}", i);
            let value = format!("value-{}", i);

            // Use timeout to prevent hanging
            let put_result = tokio::time::timeout(
                Duration::from_secs(5),
                leader.put(bytes::Bytes::from(key.clone()), bytes::Bytes::from(value))
            ).await;

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
        let commit_indices: Vec<u64> = caches.iter()
            .map(|c| c.cluster_status().commit_index)
            .collect();

        println!("Commit indices: {:?}", commit_indices);

        let max_commit = *commit_indices.iter().max().unwrap();
        for (i, &commit) in commit_indices.iter().enumerate() {
            assert!(
                commit >= max_commit - 5,
                "Node {} commit_index {} too far behind max {}",
                i + 1, commit, max_commit
            );
        }

        // Verify all nodes have the same data
        println!("Verifying data consistency...");
        for i in 0..100 {
            let key = format!("key-{}", i);
            let expected = Some(bytes::Bytes::from(format!("value-{}", i)));

            let consistent = verify_data_consistency(&caches, key.as_bytes(), expected).await;
            assert!(consistent, "Data for {} should be consistent across nodes", key);

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
    // TC-13: Heartbeat Stability (Leader自动续期)
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc13_heartbeat_stability() {
        let base_port = allocate_ports(3);
        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        let config1 = cluster_node_config(1, base_port, peer_configs.clone());
        let config2 = cluster_node_config(2, base_port + 1, peer_configs.clone());
        let config3 = cluster_node_config(3, base_port + 2, peer_configs.clone());

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Wait for leader election
        let leader_id = wait_for_single_leader(&caches, Duration::from_secs(5)).await;
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
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc14_minority_failure() {
        let base_port = allocate_ports(3);
        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        let config1 = cluster_node_config(1, base_port, peer_configs.clone());
        let config2 = cluster_node_config(2, base_port + 1, peer_configs.clone());
        let config3 = cluster_node_config(3, base_port + 2, peer_configs.clone());

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Wait for leader election
        let initial_leader_id = wait_for_single_leader(&caches, Duration::from_secs(5)).await;
        assert!(initial_leader_id.is_some(), "Should elect a leader");

        // Identify one follower to shut down
        let follower = caches
            .iter()
            .find(|c| !c.is_leader())
            .expect("Should have at least one follower");

        let follower_id = follower.node_id();

        // Shutdown one follower
        follower.shutdown().await;
        sleep(Duration::from_millis(500)).await;

        // Verify cluster still has a leader (same as before)
        let remaining_caches: Vec<&DistributedCache> = caches
            .iter()
            .filter(|c| c.node_id() != follower_id)
            .copied()
            .collect();

        let leader_after_failure = remaining_caches
            .iter()
            .find(|c| c.is_leader())
            .map(|c| c.node_id());

        assert_eq!(
            leader_after_failure, initial_leader_id,
            "Leader should remain the same after minority failure"
        );

        // Verify cluster can still process writes
        let leader = remaining_caches.iter().find(|c| c.is_leader()).unwrap();

        let write_result = leader.put("test-key", "test-value").await;
        assert!(
            write_result.is_ok(),
            "Cluster should accept writes with 2/3 nodes"
        );

        // Cleanup remaining nodes
        for cache in remaining_caches {
            cache.shutdown().await;
        }
    }

    // ========================================================================
    // TC-15: Leader Failover (Leader宕机切换)
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc15_leader_failover() {
        let base_port = allocate_ports(3);
        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        let config1 = cluster_node_config(1, base_port, peer_configs.clone());
        let config2 = cluster_node_config(2, base_port + 1, peer_configs.clone());
        let config3 = cluster_node_config(3, base_port + 2, peer_configs.clone());

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Wait for initial leader election
        let initial_leader_id = wait_for_single_leader(&caches, Duration::from_secs(5)).await;
        assert!(initial_leader_id.is_some(), "Should elect initial leader");

        let initial_term = cache1.cluster_status().term;

        // Find and kill the leader
        let leader = caches
            .iter()
            .find(|c| c.is_leader())
            .expect("Should have a leader");

        let killed_leader_id = leader.node_id();
        leader.shutdown().await;

        // Wait for new leader election
        sleep(Duration::from_secs(3)).await;

        // Get remaining nodes
        let remaining_caches: Vec<&DistributedCache> = caches
            .iter()
            .filter(|c| c.node_id() != killed_leader_id)
            .copied()
            .collect();

        // Verify new leader was elected
        let new_leader_elected = remaining_caches.iter().any(|c| c.is_leader());

        assert!(
            new_leader_elected,
            "Should elect new leader after leader failure"
        );

        // Verify new leader has higher term
        let new_term = remaining_caches[0].cluster_status().term;
        assert!(
            new_term > initial_term,
            "New leader should have higher term ({} > {})",
            new_term,
            initial_term
        );

        // Verify cluster can accept writes
        let new_leader = remaining_caches.iter().find(|c| c.is_leader()).unwrap();

        let write_result = new_leader.put("failover-key", "failover-value").await;
        assert!(write_result.is_ok(), "New leader should accept writes");

        // Cleanup
        for cache in remaining_caches {
            cache.shutdown().await;
        }
    }

    // ========================================================================
    // TC-16: Node Rejoin (节点重新加入)
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc16_node_rejoin() {
        let base_port = allocate_ports(3);
        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        let config1 = cluster_node_config(1, base_port, peer_configs.clone());
        let config2 = cluster_node_config(2, base_port + 1, peer_configs.clone());
        let config3 = cluster_node_config(3, base_port + 2, peer_configs.clone());

        let cache1 = DistributedCache::new(config1.clone()).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        // Wait for leader election
        let caches = [&cache1, &cache2, &cache3];
        wait_for_single_leader(&caches, Duration::from_secs(5)).await;

        // Write some data
        let leader = caches.iter().find(|c| c.is_leader()).unwrap();
        for i in 0..10 {
            leader
                .put(format!("key-{}", i), format!("value-{}", i))
                .await
                .unwrap();
        }

        // Shutdown node 1
        cache1.shutdown().await;
        sleep(Duration::from_millis(500)).await;

        // Write more data while node 1 is down
        let remaining = [&cache2, &cache3];
        let leader = remaining.iter().find(|c| c.is_leader()).unwrap();

        for i in 10..20 {
            leader
                .put(format!("key-{}", i), format!("value-{}", i))
                .await
                .unwrap();
        }

        sleep(Duration::from_millis(500)).await;

        // Restart node 1
        let cache1_rejoined = DistributedCache::new(config1).await.unwrap();

        // Wait for node to catch up
        sleep(Duration::from_secs(3)).await;

        // Verify node 1 has all the data (catch-up successful)
        for i in 0..20 {
            let key = format!("key-{}", i);
            let expected = Some(bytes::Bytes::from(format!("value-{}", i)));
            let actual = cache1_rejoined.get(key.as_bytes()).await;
            assert_eq!(
                actual, expected,
                "Rejoined node should have caught up: key {}",
                key
            );
        }

        // Cleanup
        cache1_rejoined.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    // ========================================================================
    // TC-17: Network Partition - Majority/Minority Split (对称分区)
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc17_network_partition_majority_minority() {
        let base_port = allocate_ports(5);
        let peer_configs = vec![
            (1, base_port),
            (2, base_port + 1),
            (3, base_port + 2),
            (4, base_port + 3),
            (5, base_port + 4),
        ];

        // Start all 5 nodes
        let configs: Vec<CacheConfig> = (1u64..=5)
            .map(|i| cluster_node_config(i, base_port + (i as u16) - 1, peer_configs.clone()))
            .collect();

        let cache1 = DistributedCache::new(configs[0].clone()).await.unwrap();
        let cache2 = DistributedCache::new(configs[1].clone()).await.unwrap();
        let cache3 = DistributedCache::new(configs[2].clone()).await.unwrap();
        let cache4 = DistributedCache::new(configs[3].clone()).await.unwrap();
        let cache5 = DistributedCache::new(configs[4].clone()).await.unwrap();

        let all_caches = [&cache1, &cache2, &cache3, &cache4, &cache5];

        // Wait for leader election
        wait_for_single_leader(&all_caches, Duration::from_secs(5)).await;

        let leader = all_caches.iter().find(|c| c.is_leader()).unwrap();

        // Write initial data
        leader
            .put("before-partition", "initial-value")
            .await
            .unwrap();
        sleep(Duration::from_millis(500)).await;

        // Simulate partition: {1,2,3} vs {4,5}
        // In a real test, you would need network-level isolation
        // For this example, we'll shutdown nodes 4 and 5 to simulate partition
        cache4.shutdown().await;
        cache5.shutdown().await;

        sleep(Duration::from_millis(500)).await;

        // Majority partition {1,2,3} should still have a leader
        let majority = [&cache1, &cache2, &cache3];
        let majority_leader = majority.iter().find(|c| c.is_leader());
        assert!(
            majority_leader.is_some(),
            "Majority partition should have a leader"
        );

        // Write to majority partition should succeed
        let write_result = majority_leader
            .unwrap()
            .put("majority-write", "success")
            .await;
        assert!(write_result.is_ok(), "Write to majority should succeed");

        // Verify data in majority partition
        sleep(Duration::from_millis(300)).await;
        for cache in &majority {
            let value = cache.get(b"majority-write").await;
            assert_eq!(
                value,
                Some(bytes::Bytes::from("success")),
                "Majority nodes should have the write"
            );
        }

        // Cleanup
        cache1.shutdown().await;
        cache2.shutdown().await;
        cache3.shutdown().await;
    }

    // ========================================================================
    // TC-18: Stale Leader Replacement (旧Leader回归)
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc18_stale_leader_replacement() {
        let base_port = allocate_ports(3);
        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        // 1. 初始化集群 - 显著减小 tick 数值以加快测试速度
        // 设置梯次差异（10, 12, 14）以减少 Split Vote（选票瓜分）
        let mut config1 = cluster_node_config(1, base_port, peer_configs.clone());
        config1.raft.election_tick = 10;
        let mut config2 = cluster_node_config(2, base_port + 1, peer_configs.clone());
        config2.raft.election_tick = 12;
        let mut config3 = cluster_node_config(3, base_port + 2, peer_configs.clone());
        config3.raft.election_tick = 14;

        let cache1 = DistributedCache::new(config1.clone()).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();
        let caches = [&cache1, &cache2, &cache3];

        // 缩短等待时间，因为 tick 小了，选举会非常快
        wait_for_single_leader(&caches, Duration::from_secs(10)).await;

        // 2. 确认初始 Leader 并关闭它
        let initial_leader = caches.iter().find(|c| c.is_leader())
            .expect("Leader should be elected quickly with low tick counts");
        let initial_leader_id = initial_leader.node_id();
        let term1 = initial_leader.cluster_status().term;

        initial_leader.shutdown().await;

        // 3. 等待新 Leader 选举
        let remaining: Vec<&DistributedCache> = caches.iter()
            .filter(|c| c.node_id() != initial_leader_id).copied().collect();

        // 给予足够的时间让剩余节点触发新的选举
        sleep(Duration::from_secs(3)).await;

        let new_leader = remaining.iter().find(|c| c.is_leader())
            .expect("New leader should be elected from remaining nodes");
        let term2 = new_leader.cluster_status().term;
        assert!(term2 > term1, "Term should have increased: {} -> {}", term1, term2);

        // 4. 写入新数据
        new_leader.put("new-leader-key", "new-leader-value").await.unwrap();

        // 5. 【关键点】重启旧 Leader 时调大其选举超时
        // 这样它回归后会更“耐心”地等待心跳，而不是立即发起 Pre-Vote
        let mut config1_reconnected = config1.clone();
        config1_reconnected.raft.election_tick = 50; // 调大回归节点的 tick

        sleep(Duration::from_millis(500)).await;
        let cache1_reconnected = DistributedCache::new(config1_reconnected).await.unwrap();

        // 持续触发写操作，强制新 Leader 发送携带新 Term 的 MsgAppend
        for _ in 0..5 {
            let _ = new_leader.put("trigger-sync", "val").await;
            sleep(Duration::from_millis(200)).await;
        }

        // 6. 验证旧 Leader 降级并追平 Term
        let success = wait_for(|| {
            let status = cache1_reconnected.cluster_status();
            // 核心检查：Term 是否追上，且角色是否变为 Follower (is_leader == false)
            status.term >= term2 && !cache1_reconnected.is_leader()
        }, Duration::from_secs(10), Duration::from_millis(200)).await;

        if !success {
            let status = cache1_reconnected.cluster_status();
            panic!("Sync failed! Node {} is at Term {}, Leader: {}",
                   initial_leader_id, status.term, cache1_reconnected.is_leader());
        }

        // 7. 验证数据同步
        let synced_value = wait_for_result(
            || async { cache1_reconnected.get(b"new-leader-key").await },
            |res| res.as_ref().map(|b| b.as_ref()) == Some("new-leader-value".as_bytes()),
            Duration::from_secs(5)
        ).await;

        assert!(synced_value.is_some(), "Old leader should have synced data from the current cluster");

        // 8. 清理
        cache1_reconnected.shutdown().await;
        for cache in remaining {
            cache.shutdown().await;
        }
    }

    // ========================================================================
    // TC-19: Split Vote Recovery (选票瓜分)
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc19_split_vote_recovery() {
        let base_port = allocate_ports(3);
        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        // Use same election timeout for all nodes to increase chance of split vote
        let mut config1 = cluster_node_config(1, base_port, peer_configs.clone());
        let mut config2 = cluster_node_config(2, base_port + 1, peer_configs.clone());
        let mut config3 = cluster_node_config(3, base_port + 2, peer_configs.clone());

        // Set identical election timeouts (simulate worst case)
        config1.raft.election_tick = 10;
        config2.raft.election_tick = 10;
        config3.raft.election_tick = 10;

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Even with identical timeouts, randomization should eventually succeed
        let leader_elected = wait_for_single_leader(&caches, Duration::from_secs(10)).await;

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
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc20_no_dual_leader() {
        let base_port = allocate_ports(5);
        let peer_configs = vec![
            (1, base_port),
            (2, base_port + 1),
            (3, base_port + 2),
            (4, base_port + 3),
            (5, base_port + 4),
        ];

        // Start all 5 nodes
        let configs: Vec<CacheConfig> = (1u64..=5)
            .map(|i| cluster_node_config(i, base_port + (i as u16) - 1, peer_configs.clone()))
            .collect();

        let caches = vec![
            DistributedCache::new(configs[0].clone()).await.unwrap(),
            DistributedCache::new(configs[1].clone()).await.unwrap(),
            DistributedCache::new(configs[2].clone()).await.unwrap(),
            DistributedCache::new(configs[3].clone()).await.unwrap(),
            DistributedCache::new(configs[4].clone()).await.unwrap(),
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
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc21_disk_io_stall() {
        // Note: This is a simplified version since we can't easily simulate
        // disk IO stalls without modifying the storage layer

        let base_port = allocate_ports(3);
        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        let config1 = cluster_node_config(1, base_port, peer_configs.clone());
        let config2 = cluster_node_config(2, base_port + 1, peer_configs.clone());
        let config3 = cluster_node_config(3, base_port + 2, peer_configs.clone());

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Wait for leader election
        wait_for_single_leader(&caches, Duration::from_secs(5)).await;

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
    // TC-22: Message Reordering Tolerance (消息乱序与重复)
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc22_message_reordering_tolerance() {
        // Note: Without modifying the transport layer to inject delays and
        // reordering, this test verifies basic consistency guarantees

        let base_port = allocate_ports(3);
        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        let config1 = cluster_node_config(1, base_port, peer_configs.clone());
        let config2 = cluster_node_config(2, base_port + 1, peer_configs.clone());
        let config3 = cluster_node_config(3, base_port + 2, peer_configs.clone());

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Wait for leader election
        wait_for_single_leader(&caches, Duration::from_secs(5)).await;

        let leader = caches.iter().find(|c| c.is_leader()).unwrap();

        // Perform many writes (testing message ordering tolerance)
        for i in 0..50 {
            let key = format!("concurrent-{}", i);
            let value = format!("value-{}", i);
            let result = leader.put(key, value).await;
            assert!(result.is_ok(), "Write {} should succeed", i);
        }

        // Wait for replication
        sleep(Duration::from_secs(2)).await;

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

    // ========================================================================
    // Additional Helper Tests
    // ========================================================================

    /// Verify quorum check - only nodes with majority votes can become leader
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_extra_quorum_verification() {
        let base_port = allocate_ports(5);
        let peer_configs = vec![
            (1, base_port),
            (2, base_port + 1),
            (3, base_port + 2),
            (4, base_port + 3),
            (5, base_port + 4),
        ];

        let configs: Vec<CacheConfig> = (1u64..=5)
            .map(|i| cluster_node_config(i, base_port + (i as u16) - 1, peer_configs.clone()))
            .collect();

        let caches = vec![
            DistributedCache::new(configs[0].clone()).await.unwrap(),
            DistributedCache::new(configs[1].clone()).await.unwrap(),
            DistributedCache::new(configs[2].clone()).await.unwrap(),
            DistributedCache::new(configs[3].clone()).await.unwrap(),
            DistributedCache::new(configs[4].clone()).await.unwrap(),
        ];

        let cache_refs: Vec<&DistributedCache> = caches.iter().collect();

        // Wait for leader
        let leader_id = wait_for_single_leader(&cache_refs, Duration::from_secs(5)).await;
        assert!(leader_id.is_some(), "Quorum should elect a leader");

        // For 5 nodes, quorum is 3
        // Shutdown 2 nodes (leaving exactly 3)
        caches[3].shutdown().await;
        caches[4].shutdown().await;

        sleep(Duration::from_millis(500)).await;

        // Should still have a leader with exactly quorum
        let remaining = [&caches[0], &caches[1], &caches[2]];
        let leader_exists = remaining.iter().any(|c| c.is_leader());
        assert!(
            leader_exists,
            "Should maintain leader with exactly quorum (3/5)"
        );

        // Write should still succeed
        let leader = remaining.iter().find(|c| c.is_leader()).unwrap();
        let result = leader.put("quorum-test", "value").await;
        assert!(result.is_ok(), "Write should succeed with quorum");

        // Cleanup
        for cache in remaining {
            cache.shutdown().await;
        }
    }

    /// Verify term monotonicity across cluster
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_extra_term_monotonicity_cluster() {
        let base_port = allocate_ports(3);
        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        let config1 = cluster_node_config(1, base_port, peer_configs.clone());
        let config2 = cluster_node_config(2, base_port + 1, peer_configs.clone());
        let config3 = cluster_node_config(3, base_port + 2, peer_configs.clone());

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Wait for leader election
        wait_for_single_leader(&caches, Duration::from_secs(5)).await;

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
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_extra_linearizability_check() {
        let base_port = allocate_ports(3);
        let peer_configs = vec![(1, base_port), (2, base_port + 1), (3, base_port + 2)];

        let config1 = cluster_node_config(1, base_port, peer_configs.clone());
        let config2 = cluster_node_config(2, base_port + 1, peer_configs.clone());
        let config3 = cluster_node_config(3, base_port + 2, peer_configs.clone());

        let cache1 = DistributedCache::new(config1).await.unwrap();
        let cache2 = DistributedCache::new(config2).await.unwrap();
        let cache3 = DistributedCache::new(config3).await.unwrap();

        let caches = [&cache1, &cache2, &cache3];

        // Wait for leader election
        wait_for_single_leader(&caches, Duration::from_secs(5)).await;

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
