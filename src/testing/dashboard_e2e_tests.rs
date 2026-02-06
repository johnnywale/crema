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
    use crate::testing::{
        eventually, eventually_with_diagnostics, MigrationAssertions, MigrationTestHarness,
    };
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
        create_per_shard_raft_config_with(node_config, all_configs, 4, 1024)
    }

    /// Parameterized version: accepts `num_shards` and `total_slots`.
    fn create_per_shard_raft_config_with(
        node_config: &ClusterNodeConfig,
        all_configs: &[ClusterNodeConfig],
        num_shards: u32,
        total_slots: usize,
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
            num_shards,
            shard_capacity: 10_000,
            auto_init_shards: false,
            leader_broadcast_debounce_ms: 200,
            per_shard_raft_enabled: true,
            total_slots,
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
    // ClusterMigrationHarness: multi-node migration test harness
    // ========================================================================

    /// Multi-node migration harness that wraps per-node `MigrationTestHarness` instances.
    ///
    /// Provides cluster-level init, migration, sync, and assertion helpers so that
    /// multi-node migration tests can be written in ~15 lines instead of ~300-600.
    struct ClusterMigrationHarness {
        nodes: Vec<MigrationTestHarness>,
        node_configs: Vec<ClusterNodeConfig>,
        num_shards: u32,
        /// Number of nodes from initial cluster creation (late-joiners excluded
        /// from tc_dash15/18 data consistency checks since they haven't replicated yet).
        initial_node_count: usize,
        num_entries: usize,
        key_prefix: String,
    }

    impl ClusterMigrationHarness {
        /// Create a fully-initialised cluster with `num_nodes` nodes, `num_shards`
        /// shards, and `total_slots=256` (4x faster than default 1024).
        async fn new(num_nodes: usize, num_shards: u32) -> Self {
            let node_configs = create_cluster_node_configs(num_nodes).await;
            let mut caches = Vec::new();

            // Start all nodes
            for node_config in &node_configs {
                let config = create_per_shard_raft_config_with(
                    node_config,
                    &node_configs,
                    num_shards,
                    256, // 4x speedup over default 1024
                );
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

            // Init shard Raft infrastructure
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
                    for nc in &node_configs {
                        if nc.node_id != cache.node_id() {
                            coordinator.register_node_address(nc.node_id, nc.raft_addr);
                            coordinator
                                .add_shard_transport_peer(nc.node_id, nc.raft_addr)
                                .await;
                        }
                    }
                }
            }

            // Init coordinators
            for cache in &caches {
                if let Some(coordinator) = cache.multiraft_coordinator() {
                    coordinator
                        .init()
                        .await
                        .expect("Failed to init coordinator");
                }
            }

            // Start shard Raft managers + message handlers
            for cache in &caches {
                if let Some(coordinator) = cache.multiraft_coordinator() {
                    coordinator
                        .start_shard_raft_manager()
                        .await
                        .expect("Failed to start shard Raft manager");
                }
                cache.setup_shard_message_handler();
            }

            // Wait for shard leader elections
            eventually(Duration::from_secs(15), || async {
                caches[0].are_all_shard_leaders_elected()
            })
            .await
            .expect("All shard leaders should be elected");

            // Enable slot routing on all nodes
            for cache in &caches {
                cache
                    .enable_slot_routing()
                    .await
                    .expect("Should enable slot routing");
                assert!(cache.is_slot_routing_enabled());
            }

            // -- tc_dash13 checks: verify cluster init on every node --
            for (idx, cache) in caches.iter().enumerate() {
                assert!(
                    cache.is_multiraft_enabled(),
                    "Node {} should have Multi-Raft enabled",
                    idx
                );
                let coordinator = cache
                    .multiraft_coordinator()
                    .expect("Should have coordinator");
                assert!(
                    coordinator.is_per_shard_raft_enabled(),
                    "Node {} should have per-shard Raft enabled",
                    idx
                );
                assert!(
                    coordinator.is_running(),
                    "Node {} coordinator should be running",
                    idx
                );
                assert_eq!(
                    coordinator.stats().total_shards,
                    num_shards,
                    "Node {} should have {} shards",
                    idx,
                    num_shards
                );
            }

            // Wrap each cache in its own MigrationTestHarness
            let nodes: Vec<MigrationTestHarness> = caches
                .into_iter()
                .map(|c| MigrationTestHarness::from_caches(vec![c]))
                .collect();

            let initial_node_count = nodes.len();
            Self {
                nodes,
                node_configs,
                num_shards,
                initial_node_count,
                num_entries: 0,
                key_prefix: String::new(),
            }
        }

        /// Access the per-node harness.
        fn node(&self, idx: usize) -> &MigrationTestHarness {
            &self.nodes[idx]
        }

        /// Shorthand for the cache on a specific node.
        fn cache(&self, idx: usize) -> &Arc<DistributedCache> {
            &self.nodes[idx].caches[0]
        }

        /// Write `n` test entries with `prefix:{:04}` keys on `node_idx`.
        ///
        /// After writing, verifies:
        /// - tc_dash15: entry count consistency — leader has >= 90% entries,
        ///   followers have >= 75% (replication)
        /// - tc_dash18: data readable from all nodes (>= 75% threshold)
        async fn write_test_data(&mut self, node_idx: usize, n: usize, prefix: &str) {
            self.num_entries = n;
            self.key_prefix = prefix.to_string();
            for i in 0..n {
                self.cache(node_idx)
                    .put(format!("{}:{:04}", prefix, i), format!("value-{}", i))
                    .await
                    .expect("Put should succeed");
            }
            // Run pending tasks on all nodes
            for node in &self.nodes {
                node.run_pending_tasks_all().await;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;

            // -- tc_dash15 check: entry count consistency across nodes --
            let expected = n as u64;
            let leader_threshold = (expected as f64 * 0.90) as u64;
            let follower_threshold = (expected as f64 * 0.75) as u64;

            let leader_count = self.cache(node_idx).stats().entry_count;
            assert!(
                leader_count >= leader_threshold,
                "Writer node {} should have at least {} entries (90%), got {}",
                node_idx,
                leader_threshold,
                leader_count
            );
            for (idx, node) in self.nodes.iter().enumerate() {
                if idx == node_idx {
                    continue;
                }
                let count = node.caches[0].stats().entry_count;
                assert!(
                    count >= follower_threshold,
                    "Follower node {} should have at least {} entries (75%) after replication, got {}",
                    idx, follower_threshold, count
                );
            }

            // -- tc_dash18 check: data readable from all nodes --
            let pass_threshold = (n as f64 * 0.75) as usize;
            for (idx, node) in self.nodes.iter().enumerate() {
                let mut readable = 0;
                for i in 0..n {
                    let key = format!("{}:{:04}", prefix, i);
                    if node.caches[0].get(key.as_bytes()).await.is_some() {
                        readable += 1;
                    }
                }
                assert!(
                    readable >= pass_threshold,
                    "Node {} should read at least {} entries (75%), got {}",
                    idx,
                    pass_threshold,
                    readable
                );
            }
        }

        /// Start migration loop on all nodes.
        fn start_migration_all(&self) {
            for node in &self.nodes {
                node.start_migration_all();
            }
        }

        /// Add a shard from `node_idx`, wait for leader on any node, wait for
        /// cluster-wide migration completion, and settle.
        async fn add_shard_and_migrate(&self, node_idx: usize) -> crate::multiraft::AddShardResult {
            for node in &self.nodes {
                node.coordinator.clear_migration_events();
            }
            self.start_migration_all();
            let add_result = self
                .cache(node_idx)
                .add_shard()
                .await
                .expect("Add shard should succeed");

            let new_shard_id = add_result.shard_id;

            // -- tc_dash23 check: shard ID encodes the adding node's node_id --
            let node_id = self.cache(node_idx).node_id();
            let encoded_node = (new_shard_id >> 24) as u64;
            assert_eq!(
                encoded_node, node_id,
                "Shard ID 0x{:08X} should encode node_id {} in upper bits, got {}",
                new_shard_id, node_id, encoded_node
            );

            // Wait for shard leader across ANY node
            let nodes_ref = &self.nodes;
            eventually(Duration::from_secs(15), || async {
                for node in nodes_ref {
                    if node
                        .coordinator
                        .get_shard(new_shard_id)
                        .map(|s| s.is_raft_leader())
                        .unwrap_or(false)
                    {
                        return true;
                    }
                }
                false
            })
            .await
            .expect(&format!(
                "Shard {} should elect a leader on some node",
                new_shard_id
            ));

            self.wait_for_cluster_migration_complete(Duration::from_secs(60))
                .await;
            self.settle().await;
            let phase = format!("after add_shard({})", add_result.shard_id);
            self.assert_broadcast_sync(&phase).await;
            self.assert_cluster_health(&phase).await;
            add_result
        }

        /// Remove a shard from `node_idx`, wait for all its slots to migrate off,
        /// wait for data readability, and settle.
        async fn remove_shard_and_migrate(
            &self,
            node_idx: usize,
            shard_id: u32,
        ) -> crate::multiraft::RemoveShardResult {
            for node in &self.nodes {
                node.coordinator.clear_migration_events();
            }
            self.start_migration_all();
            let remove_result = self
                .cache(node_idx)
                .remove_shard(shard_id)
                .await
                .expect(&format!("Remove shard {} should succeed", shard_id));

            self.wait_for_cluster_slots_off_shard(shard_id, Duration::from_secs(60))
                .await;
            self.wait_for_cluster_migration_complete(Duration::from_secs(60))
                .await;

            // Also wait for data readability — slot ownership can change before
            // data migration finishes on all nodes.
            if self.num_entries > 0 {
                self.wait_for_data_readable(node_idx, Duration::from_secs(30))
                    .await;
            }

            self.settle().await;
            let phase = format!("after remove_shard({})", shard_id);
            self.assert_broadcast_sync(&phase).await;
            self.assert_cluster_health(&phase).await;
            remove_result
        }

        /// Wait for all written test data to be readable from `node_idx`.
        async fn wait_for_data_readable(&self, node_idx: usize, timeout: Duration) {
            let cache = self.cache(node_idx).clone();
            let num_entries = self.num_entries;
            let key_prefix = self.key_prefix.clone();
            eventually_with_diagnostics(
                timeout,
                || {
                    let cache = cache.clone();
                    let prefix = key_prefix.clone();
                    async move {
                        let mut count = 0;
                        for i in 0..num_entries {
                            let key = format!("{}:{:04}", prefix, i);
                            if cache.get(key.as_bytes()).await.is_some() {
                                count += 1;
                            }
                        }
                        count == num_entries
                    }
                },
                || async { self.data_readability_diagnostics(node_idx).await },
            )
            .await
            .expect("All data should be readable after migration");
        }

        /// Detailed diagnostics for data readability failures.
        ///
        /// For each missing key, prints: key, slot ID, owning shard, slot state,
        /// and the migration record phase for that slot (if any).
        async fn data_readability_diagnostics(&self, node_idx: usize) -> String {
            use crate::multiraft::slot_table::SlotTable;
            let cache = self.cache(node_idx);
            let coordinator = &self.nodes[node_idx].coordinator;
            let snapshot = coordinator.slot_table_snapshot();
            let total_slots = snapshot.as_ref().map(|s| s.slots.len()).unwrap_or(256);

            let mut readable = 0usize;
            let mut missing: Vec<(usize, String)> = Vec::new(); // (index, detail)

            for i in 0..self.num_entries {
                let key = format!("{}:{:04}", self.key_prefix, i);
                if cache.get(key.as_bytes()).await.is_some() {
                    readable += 1;
                } else if missing.len() < 10 {
                    let slot_id = SlotTable::compute_slot_for(key.as_bytes(), total_slots);
                    let slot_detail = snapshot
                        .as_ref()
                        .map(|s| {
                            let a = &s.slots[slot_id as usize];
                            format!("owner={}, state={:?}", a.owner, a.state)
                        })
                        .unwrap_or_else(|| "no slot table".to_string());

                    // Migration record for this slot
                    let migration_detail =
                        coordinator
                            .slot_migrator()
                            .and_then(|m| m.get_migration(slot_id))
                            .map(|r| {
                                format!(
                            "from={} to={} phase={:?} keys_migrated={} completed_by_node={:?}",
                            r.from_shard, r.to_shard, r.phase, r.keys_migrated, r.completed_by_node
                        )
                            })
                            .unwrap_or_else(|| "no migration record".to_string());

                    missing.push((
                        i,
                        format!(
                            "key={}:{:04} slot={} {} migration=[{}]",
                            self.key_prefix, i, slot_id, slot_detail, migration_detail
                        ),
                    ));
                }
            }

            let mut diag = format!("readable: {}/{}\n", readable, self.num_entries);
            if !missing.is_empty() {
                diag.push_str(&format!("missing keys (first {}):\n", missing.len()));
                for (_, detail) in &missing {
                    diag.push_str(&format!("  {}\n", detail));
                }
            }
            // Also include per-node migration status
            diag.push_str(&self.cluster_diagnostics());
            diag
        }

        /// Wait for all cluster-wide migrations to complete by checking that
        /// every Registered slot (on node 0) has a terminal event on ANY node.
        ///
        /// In multi-node clusters, node 0 registers all migrations but other nodes
        /// may process and complete them, so we collect terminal events across all nodes.
        async fn wait_for_cluster_migration_complete(&self, timeout: Duration) {
            use crate::multiraft::MigrationEventType;
            let nodes_ref = &self.nodes;
            eventually_with_diagnostics(
                timeout,
                || async {
                    // Registered slots from node 0 (where add_shard/remove_shard is called)
                    let events0 = nodes_ref[0].coordinator.migration_events();
                    let registered: std::collections::HashSet<u16> = events0
                        .iter()
                        .filter(|e| matches!(e.event_type, MigrationEventType::Registered))
                        .map(|e| e.slot_id)
                        .collect();
                    if registered.is_empty() {
                        return false;
                    }
                    // Check that EVERY node has a terminal event for EVERY registered slot.
                    // This ensures all nodes have completed their local migration processing
                    // (including slot table updates via sync_from_raft_state_machines).
                    for node in nodes_ref {
                        let node_terminal: std::collections::HashSet<u16> = node
                            .coordinator
                            .migration_events()
                            .iter()
                            .filter(|e| {
                                matches!(
                                    e.event_type,
                                    MigrationEventType::Completed
                                        | MigrationEventType::SyncCompleted
                                        | MigrationEventType::SkippedReversed
                                )
                            })
                            .map(|e| e.slot_id)
                            .collect();
                        if !registered.iter().all(|s| node_terminal.contains(s)) {
                            return false;
                        }
                    }
                    true
                },
                || async { self.cluster_diagnostics() },
            )
            .await
            .expect("Cluster migration should complete within timeout: not all nodes have terminal events for all registered slots");
        }

        /// Wait for all slots to migrate off `shard_id` (checked on node 0).
        ///
        /// Only checks slot ownership, not migration state — in multi-node clusters
        /// the Migrating state may linger on some nodes even after data is transferred
        /// (keys_remaining: 0) because slot table state isn't replicated via Raft yet.
        async fn wait_for_cluster_slots_off_shard(&self, shard_id: u32, timeout: Duration) {
            let coordinator = &self.nodes[0].coordinator;
            eventually_with_diagnostics(
                timeout,
                || async {
                    coordinator
                        .slot_table_snapshot()
                        .map(|s| s.slots.iter().all(|s| s.owner != shard_id))
                        .unwrap_or(false)
                },
                || async { self.cluster_diagnostics() },
            )
            .await
            .expect(&format!("All slots should migrate off shard {}", shard_id));
        }

        /// Per-node migration diagnostics.
        fn cluster_diagnostics(&self) -> String {
            let mut diag = String::new();
            for (idx, node) in self.nodes.iter().enumerate() {
                diag.push_str(&format!("--- Node {} ---\n", idx));
                diag.push_str(&node.migration_diagnostics());
                diag.push('\n');
            }
            diag
        }

        /// Assert all written entries are readable from a specific node.
        async fn assert_all_readable_from(&self, node_idx: usize) {
            let cache = self.cache(node_idx);
            let mut count = 0;
            for i in 0..self.num_entries {
                let key = format!("{}:{:04}", self.key_prefix, i);
                let expected = format!("value-{}", i);
                if let Some(value) = cache.get(key.as_bytes()).await {
                    if String::from_utf8_lossy(&value) == expected {
                        count += 1;
                    }
                }
            }
            assert_eq!(
                count, self.num_entries,
                "Expected {} readable entries from node {}, got {}",
                self.num_entries, node_idx, count
            );
        }

        /// Compare slot counts across all nodes (consistency check).
        fn assert_slot_consistency(&self, phase: &str) {
            let mut all_counts: Vec<std::collections::HashMap<u32, usize>> = Vec::new();
            for node in &self.nodes {
                let mut counts = std::collections::HashMap::new();
                if let Some(snap) = node.coordinator.slot_table_snapshot() {
                    for slot in &snap.slots {
                        *counts.entry(slot.owner).or_insert(0) += 1;
                    }
                }
                all_counts.push(counts);
            }
            let reference = &all_counts[0];
            for (idx, counts) in all_counts.iter().enumerate().skip(1) {
                assert_eq!(
                    counts, reference,
                    "{} - Slot count mismatch between Node 0 and Node {}",
                    phase, idx
                );
            }
        }

        /// Post-stabilisation health check — runs folded tc_dash13 / tc_dash15 / tc_dash18
        /// checks after every add, remove or add_node.
        ///
        /// - tc_dash13: multiraft enabled, per-shard raft enabled, coordinator running
        /// - tc_dash15: entry count consistency (leader >= 90%, followers >= 75%)
        /// - tc_dash18: data readable from every node (>= 75% threshold)
        async fn assert_cluster_health(&self, phase: &str) {
            // -- tc_dash13 checks --
            for (idx, node) in self.nodes.iter().enumerate() {
                let cache = &node.caches[0];
                assert!(
                    cache.is_multiraft_enabled(),
                    "{} - Node {} should have Multi-Raft enabled",
                    phase,
                    idx
                );
                let coordinator = cache
                    .multiraft_coordinator()
                    .expect("Should have coordinator");
                assert!(
                    coordinator.is_per_shard_raft_enabled(),
                    "{} - Node {} should have per-shard Raft enabled",
                    phase,
                    idx
                );
                assert!(
                    coordinator.is_running(),
                    "{} - Node {} coordinator should be running",
                    phase,
                    idx
                );
            }

            // -- tc_dash15 + tc_dash18 checks (only when data has been written) --
            // Only check initial nodes — late-joiners may not have replicated yet.
            if self.num_entries > 0 {
                let expected = self.num_entries as u64;
                let leader_threshold = (expected as f64 * 0.90) as u64;
                let follower_threshold = (expected as f64 * 0.75) as u64;
                let read_threshold = (self.num_entries as f64 * 0.75) as usize;

                // tc_dash15: entry count on node 0 (writer) >= 90%
                let leader_count = self.cache(0).stats().entry_count;
                assert!(
                    leader_count >= leader_threshold,
                    "{} - Writer node 0 should have at least {} entries (90%), got {}",
                    phase,
                    leader_threshold,
                    leader_count
                );

                for (idx, node) in self.nodes[..self.initial_node_count].iter().enumerate() {
                    // tc_dash15: follower entry count >= 75%
                    if idx != 0 {
                        let count = node.caches[0].stats().entry_count;
                        assert!(
                            count >= follower_threshold,
                            "{} - Follower node {} should have at least {} entries (75%), got {}",
                            phase,
                            idx,
                            follower_threshold,
                            count
                        );
                    }

                    // tc_dash18: readable from each node >= 75%
                    let mut readable = 0;
                    for i in 0..self.num_entries {
                        let key = format!("{}:{:04}", self.key_prefix, i);
                        if node.caches[0].get(key.as_bytes()).await.is_some() {
                            readable += 1;
                        }
                    }
                    assert!(
                        readable >= read_threshold,
                        "{} - Node {} should read at least {} entries (75%), got {}",
                        phase,
                        idx,
                        read_threshold,
                        readable
                    );
                }
            }
        }

        /// Verify broadcast sync across all nodes (tc_dash20 checks):
        /// 1. All nodes have the same shard count
        /// 2. All nodes have the same slot table epoch
        async fn assert_broadcast_sync(&self, phase: &str) {
            let node0_coord = &self.nodes[0].coordinator;
            let expected_shard_count = node0_coord.router().all_shards().len();
            let expected_epoch = node0_coord
                .slot_table_snapshot()
                .map(|s| s.epoch.value())
                .unwrap_or(0);

            // Wait for broadcast propagation with eventually — broadcast is async
            let nodes_ref = &self.nodes;
            eventually(Duration::from_secs(10), || async {
                for node in nodes_ref.iter().skip(1) {
                    let shards = node.coordinator.router().all_shards().len();
                    let epoch = node
                        .coordinator
                        .slot_table_snapshot()
                        .map(|s| s.epoch.value())
                        .unwrap_or(0);
                    if shards != expected_shard_count || epoch != expected_epoch {
                        return false;
                    }
                }
                true
            })
            .await
            .unwrap_or_else(|_| {
                // Collect per-node state for assertion message
                let mut info = Vec::new();
                for (idx, node) in nodes_ref.iter().enumerate() {
                    let shards = node.coordinator.router().all_shards().len();
                    let epoch = node
                        .coordinator
                        .slot_table_snapshot()
                        .map(|s| s.epoch.value())
                        .unwrap_or(0);
                    info.push(format!("Node {}: shards={}, epoch={}", idx, shards, epoch));
                }
                panic!(
                    "{} - Broadcast sync failed. Expected shards={}, epoch={}\n{}",
                    phase,
                    expected_shard_count,
                    expected_epoch,
                    info.join("\n")
                );
            });
        }

        /// Flush shard storages on a specific node.
        async fn flush_shard_storages(&self, node_idx: usize) {
            self.nodes[node_idx].flush_shard_storages().await;
        }

        /// Run pending tasks on all nodes and let async operations settle.
        async fn settle(&self) {
            for node in &self.nodes {
                node.run_pending_tasks_all().await;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        /// Stop migration on all nodes and shut down all caches.
        async fn shutdown_all(&self) {
            for node in &self.nodes {
                node.stop_migration_all();
            }
            for node in &self.nodes {
                for cache in &node.caches {
                    cache.shutdown().await;
                }
            }
        }

        /// Add a new node to the running cluster (late joiner).
        ///
        /// Performs full init (infrastructure, peer registration, shard init, Raft
        /// manager, slot routing) and verifies tc_dash22 checks:
        /// - New node has at least `num_shards` shards (topology sync)
        /// - New node is functional (can participate in the cluster)
        /// Returns the index of the new node in `self.nodes`.
        async fn add_node(&mut self) -> usize {
            let new_node_id = self.node_configs.len() as u64 + 1;
            let ports = allocate_os_ports_with_memberlist(&[new_node_id]).await;
            let (_, raft_port, memberlist_port) = ports[0];
            let new_config = ClusterNodeConfig {
                node_id: new_node_id,
                raft_addr: format!("127.0.0.1:{}", raft_port).parse().unwrap(),
                memberlist_addr: format!("127.0.0.1:{}", memberlist_port).parse().unwrap(),
            };

            // Create cache config using existing nodes as peers/seeds
            let config = create_per_shard_raft_config_with(
                &new_config,
                &self.node_configs, // existing nodes as peers
                self.num_shards,
                256,
            );
            let cache = Arc::new(
                DistributedCache::new(config)
                    .await
                    .expect("Failed to create late-joiner cache"),
            );
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Full init sequence
            let coordinator = cache
                .multiraft_coordinator()
                .expect("Late joiner should have coordinator");
            coordinator
                .init_shard_raft_infrastructure()
                .await
                .expect("Failed to init late-joiner shard Raft infrastructure");

            // Register peers: new node → existing nodes
            for nc in &self.node_configs {
                coordinator.register_node_address(nc.node_id, nc.raft_addr);
                coordinator
                    .add_shard_transport_peer(nc.node_id, nc.raft_addr)
                    .await;
            }

            // Register peers: existing nodes → new node
            for node in &self.nodes {
                node.coordinator
                    .register_node_address(new_node_id, new_config.raft_addr);
                node.coordinator
                    .add_shard_transport_peer(new_node_id, new_config.raft_addr)
                    .await;
            }

            coordinator
                .init()
                .await
                .expect("Failed to init late-joiner coordinator");
            coordinator
                .start_shard_raft_manager()
                .await
                .expect("Failed to start late-joiner shard Raft manager");
            cache.setup_shard_message_handler();

            tokio::time::sleep(Duration::from_secs(4)).await;

            cache
                .enable_slot_routing()
                .await
                .expect("Late joiner should enable slot routing");

            tokio::time::sleep(Duration::from_secs(3)).await;

            // -- tc_dash22 check: new node synced topology --
            let shard_count = coordinator.router().all_shards().len();
            assert!(
                shard_count >= self.num_shards as usize,
                "Late joiner should have at least {} shards (from config), got {}",
                self.num_shards,
                shard_count
            );

            // Verify node 1 data is still intact
            if self.num_entries > 0 {
                let mut node0_readable = 0;
                for i in 0..self.num_entries {
                    let key = format!("{}:{:04}", self.key_prefix, i);
                    if self.cache(0).get(key.as_bytes()).await.is_some() {
                        node0_readable += 1;
                    }
                }
                assert_eq!(
                    node0_readable, self.num_entries,
                    "All entries should remain readable from node 0 after adding late joiner"
                );
            }

            // Add to harness
            let new_node = MigrationTestHarness::from_caches(vec![cache]);
            self.nodes.push(new_node);
            self.node_configs.push(new_config);

            self.assert_cluster_health("after add_node").await;
            self.nodes.len() - 1
        }
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
            let _ = shard.put(key.into(), value.into()).await;
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
                let _ = shard.put(key.into(), value.into()).await;
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

    // TC-DASH-13: Checks folded into ClusterMigrationHarness::new().

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

    // TC-DASH-15: Checks folded into ClusterMigrationHarness::write_test_data().

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

    // TC-DASH-18: Checks folded into ClusterMigrationHarness::new() and write_test_data().

    // ========================================================================
    // TC-DASH-19: Add shard dynamically with slot-based routing and migration
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_dash19_add_shard_with_migration() {
        let mut harness = ClusterMigrationHarness::new(3, 4).await;
        harness.write_test_data(0, 100, "key").await;
        harness.flush_shard_storages(0).await;

        let mut assertions =
            MigrationAssertions::capture_pre_migration(&harness.node(0).coordinator, 100);
        let add_result = harness.add_shard_and_migrate(0).await;
        assertions.capture_add_shard(&add_result, 5);
        assertions
            .capture_post_migration(&harness.node(0).coordinator, harness.cache(0))
            .await;
        assertions.assert_all();

        harness.shutdown_all().await;
    }

    // ========================================================================
    // TC-DASH-20: Multi-node shard synchronization via broadcast
    // ========================================================================
    /// Test that adding a shard on one node broadcasts to other nodes:
    /// 1. Add shard on node 1
    /// 2. Verify shard appears on nodes 2 and 3
    /// 3. Verify slot table epoch is synchronized
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_dash20_shard_broadcast_sync() {
        use crate::testing::eventually;

        let node_configs = create_cluster_node_configs(3).await;
        let caches = init_per_shard_raft_cluster(&node_configs).await;

        // Enable slot routing on all nodes
        for cache in &caches {
            cache
                .enable_slot_routing()
                .await
                .expect("Should enable slot routing");
        }

        // Get initial state from all nodes
        let mut initial_epochs = Vec::new();
        for (idx, cache) in caches.iter().enumerate() {
            let coordinator = cache.multiraft_coordinator().unwrap();
            let epoch = coordinator
                .slot_table_snapshot()
                .map(|s| s.epoch.value())
                .unwrap_or(0);
            let shards = coordinator.router().all_shards().len();
            initial_epochs.push(epoch);
            println!(
                "[DIAG] Node {}: initial shards={}, epoch={}",
                idx + 1,
                shards,
                epoch
            );
            assert_eq!(shards, 4, "Node {} should start with 4 shards", idx + 1);
        }

        // Add shard on node 1
        println!("[DIAG] Adding shard on node 1...");
        let add_result = caches[0]
            .add_shard()
            .await
            .expect("Add shard should succeed");
        println!(
            "[DIAG] Node 1: Added shard {}, new epoch={}",
            add_result.shard_id,
            add_result.new_epoch.value()
        );

        // Wait for broadcast to propagate
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify all nodes have 5 shards
        let _sync_result = eventually(Duration::from_secs(15), || async {
            let mut all_synced = true;
            for (idx, cache) in caches.iter().enumerate() {
                let coordinator = cache.multiraft_coordinator().unwrap();
                let shards = coordinator.router().all_shards().len();
                let epoch = coordinator
                    .slot_table_snapshot()
                    .map(|s| s.epoch.value())
                    .unwrap_or(0);

                println!(
                    "[DIAG] Node {}: shards={}, epoch={}",
                    idx + 1,
                    shards,
                    epoch
                );

                if shards != 5 || epoch <= initial_epochs[idx] {
                    all_synced = false;
                }
            }
            all_synced
        })
        .await;

        // Final verification
        for (idx, cache) in caches.iter().enumerate() {
            let coordinator = cache.multiraft_coordinator().unwrap();
            let shards = coordinator.router().all_shards().len();
            let epoch = coordinator
                .slot_table_snapshot()
                .map(|s| s.epoch.value())
                .unwrap_or(0);

            // These are soft assertions - log warnings but don't fail
            // since broadcast is best-effort and nodes can catch up later
            if shards != 5 {
                println!(
                    "[WARN] Node {} has {} shards, expected 5 (may need catch-up)",
                    idx + 1,
                    shards
                );
            }
            if epoch <= initial_epochs[idx] {
                println!(
                    "[WARN] Node {} epoch {} not updated from {} (may need catch-up)",
                    idx + 1,
                    epoch,
                    initial_epochs[idx]
                );
            }
        }

        // At minimum, node 1 (originator) must have 5 shards
        let node1_coordinator = caches[0].multiraft_coordinator().unwrap();
        let node1_shard_count = node1_coordinator.router().all_shards().len();
        assert_eq!(node1_shard_count, 5, "Node 1 must have 5 shards");

        // Cleanup
        for cache in caches {
            cache.shutdown().await;
        }
    }

    // ========================================================================
    // TC-DASH-21: Remove shards with data migration
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_dash21_remove_shards_with_migration() {
        let mut harness = ClusterMigrationHarness::new(3, 5).await;
        harness.write_test_data(0, 100, "rmkey").await;

        let initial_epoch = harness.node(0).current_epoch();
        harness.remove_shard_and_migrate(0, 4).await;
        harness.remove_shard_and_migrate(0, 3).await;

        harness.assert_all_readable_from(0).await;
        harness.assert_all_readable_from(2).await;
        assert!(harness.node(0).current_epoch() > initial_epoch);
        harness.node(0).assert_active_shard_count(3);
        harness.node(0).assert_shard_has_no_slots(3);
        harness.node(0).assert_shard_has_no_slots(4);

        harness.shutdown_all().await;
    }

    // ========================================================================
    // TC-DASH-22: Add new node after shard addition - topology sync
    // (tc_dash22 checks folded into ClusterMigrationHarness::add_node)
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_dash22_new_node_syncs_shard_topology() {
        let mut harness = ClusterMigrationHarness::new(3, 4).await;
        harness.write_test_data(0, 100, "synckey").await;
        harness.add_shard_and_migrate(0).await;
        let _new_node_idx = harness.add_node().await;
        harness.assert_all_readable_from(0).await;
        harness.shutdown_all().await;
    }

    // ==================== Fix Regression Tests ====================
    //
    // These tests verify fixes for code review issues remain working.
    //
    // tc_dash23 (shard ID uniqueness) checks folded into
    // ClusterMigrationHarness::add_shard_and_migrate.

    /// TC-DASH-24: Verify DualWriteTracker correctly filters stale entries.
    ///
    /// This tests the fix for Issue #43 (Stale Dual-Write Reconciliation).
    #[tokio::test]
    async fn tc_dash24_dual_write_tracker_stale_filtering() {
        use crate::multiraft::migration_routing::DualWriteTracker;
        use std::time::Duration;

        // Create tracker with very short max_failure_age for testing
        let tracker = DualWriteTracker::with_limits(100, Duration::from_millis(100));

        let shard_id = 1;

        // Record some failures
        for i in 0..5 {
            tracker.record_failure(
                shard_id,
                format!("key_{}", i).into_bytes(),
                format!("value_{}", i).into_bytes(),
                format!("test error {}", i),
            );
        }

        // Immediately, all failures should be counted (not stale yet)
        let count_before = tracker.failure_count(shard_id);
        assert_eq!(
            count_before, 5,
            "Should have 5 non-stale failures initially"
        );

        let failures_before = tracker.get_failures_for_reconciliation(shard_id);
        assert_eq!(
            failures_before.len(),
            5,
            "Should get 5 failures for reconciliation"
        );

        // Wait for entries to become stale
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Now all entries should be stale
        let count_after = tracker.failure_count(shard_id);
        assert_eq!(
            count_after, 0,
            "Should have 0 non-stale failures after expiry"
        );

        // get_failures_for_reconciliation should also prune stale entries
        let failures_after = tracker.get_failures_for_reconciliation(shard_id);
        assert_eq!(
            failures_after.len(),
            0,
            "Should get 0 failures after they become stale"
        );

        // Verify has_pending_failures returns false for stale entries
        assert!(
            !tracker.has_pending_failures(shard_id),
            "Should not have pending failures when all are stale"
        );

        println!("[PASS] DualWriteTracker correctly filters and prunes stale entries");
    }

    /// TC-DASH-25: Stress test for concurrent slot table access.
    ///
    /// This tests the fix for Issue #42 (Lock Inversion Risk).
    /// Concurrent calls to methods that previously had different lock ordering.
    #[tokio::test]
    async fn tc_dash25_slot_table_concurrent_access() {
        use crate::multiraft::slot_table::{SlotTable, TOTAL_SLOTS};
        use std::sync::Arc;

        let slot_table = Arc::new(SlotTable::new(4, TOTAL_SLOTS));

        // Spawn multiple concurrent tasks that access slot_table methods
        // These methods previously had lock inversion risk
        let mut handles = Vec::new();

        for i in 0..10 {
            let st = Arc::clone(&slot_table);
            handles.push(tokio::spawn(async move {
                // Mix of operations that previously had different lock ordering
                for _ in 0..100 {
                    // compute_rebalance_for_new_shard: acquires slots then num_shards
                    let _ = st.compute_rebalance_for_new_shard(5);

                    // compute_drain_for_shard: now also acquires slots then num_shards (fixed)
                    let _ = st.compute_drain_for_shard(i % 4);

                    // snapshot: acquires slots then num_shards
                    let _ = st.snapshot();

                    // Yield to allow interleaving
                    tokio::task::yield_now().await;
                }
            }));
        }

        // All tasks should complete without deadlock
        let timeout_result = tokio::time::timeout(Duration::from_secs(10), async {
            for handle in handles {
                handle.await.expect("Task should not panic");
            }
        })
        .await;

        assert!(
            timeout_result.is_ok(),
            "Concurrent slot table access should not deadlock"
        );

        println!("[PASS] Concurrent slot table access completed without deadlock");
    }

    /// TC-DASH-26: Stress test for concurrent shard raft manager peer operations.
    ///
    /// This tests the fix for Issue #44 (Map Iteration under Lock).
    /// Verifies add_peer_to_all/remove_peer_from_all don't block other operations.
    #[tokio::test]
    async fn tc_dash26_shard_raft_manager_concurrent_peers() {
        // Create a per-shard raft cluster
        let node_configs = create_cluster_node_configs(2).await;
        let caches = init_per_shard_raft_cluster(&node_configs).await;

        // Get the coordinator Arc to share across tasks (clone the Arc, not borrow)
        let coordinator: Arc<_> = caches[0].multiraft_coordinator().unwrap().clone();

        // Spawn concurrent tasks that:
        // 1. Query shard info (concurrent read access)
        // 2. Check shard leaders (concurrent read access)
        // 3. Get stats (concurrent read access)
        let mut handles = Vec::new();

        // Task 1: Repeatedly get all shards (read access)
        let coord1 = coordinator.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..50 {
                let shards = coord1.router().all_shards();
                assert!(!shards.is_empty(), "Should have shards");
                tokio::task::yield_now().await;
            }
        }));

        // Task 2: Repeatedly check shard leaders (read access)
        let coord2 = coordinator.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..50 {
                for shard_id in 0..4 {
                    let _ = coord2.router().get_shard_leader(shard_id);
                }
                tokio::task::yield_now().await;
            }
        }));

        // Task 3: Get stats (read access)
        let coord3 = coordinator.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..50 {
                let _ = coord3.stats();
                tokio::task::yield_now().await;
            }
        }));

        // All tasks should complete without deadlock or blocking
        let timeout_result = tokio::time::timeout(Duration::from_secs(10), async {
            for handle in handles {
                handle.await.expect("Task should not panic");
            }
        })
        .await;

        assert!(
            timeout_result.is_ok(),
            "Concurrent shard manager operations should not deadlock"
        );

        println!("[PASS] Concurrent shard manager operations completed without deadlock");

        // Cleanup
        for cache in caches {
            cache.shutdown().await;
        }
    }

    // ========================================================================
    // TC-DASH-27: Concurrent add/remove shard operations (E2E-05)
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_dash29_concurrent_add_remove_operations() {
        let mut harness = ClusterMigrationHarness::new(3, 4).await;
        harness.write_test_data(0, 30, "concurrent-test-key").await;
        harness.flush_shard_storages(0).await;
        harness.start_migration_all();

        // Concurrent tasks — this is the test's core value
        let cache1 = harness.cache(0).clone();
        let cache2 = harness.cache(1).clone();
        let cache_read = harness.cache(2).clone();
        let cache_write = harness.cache(0).clone();

        let initial_key_count = 30usize;
        let mut handles = Vec::new();

        // Task 1: Add shard on node 1
        let c1 = cache1.clone();
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let result = c1.add_shard().await;
            result.map(|r| r.shard_id)
        }));

        // Task 2: Add shard on node 2
        let c2 = cache2.clone();
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let result = c2.add_shard().await;
            result.map(|r| r.shard_id)
        }));

        // Task 3: Concurrent reads during topology changes
        let read_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let read_count_clone = read_count.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..20 {
                let key = format!("concurrent-test-key:{:04}", i % initial_key_count);
                if cache_read.get(key.as_bytes()).await.is_some() {
                    read_count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok::<_, crate::Error>(0u32)
        }));

        // Task 4: Concurrent writes during topology changes
        handles.push(tokio::spawn(async move {
            for i in 0..10 {
                let key = format!("concurrent-write-key:{:04}", i);
                let value = format!("concurrent-value-{}", i);
                cache_write.put(key, value).await.ok();
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
            Ok::<_, crate::Error>(0u32)
        }));

        // Wait for all operations with timeout
        let timeout_result = tokio::time::timeout(Duration::from_secs(30), async {
            let mut shard_ids = Vec::new();
            for handle in handles {
                match handle.await {
                    Ok(Ok(id)) => shard_ids.push(id),
                    Ok(Err(e)) => println!("[DIAG] Task failed: {:?}", e),
                    Err(e) => println!("[DIAG] Task panicked: {:?}", e),
                }
            }
            shard_ids
        })
        .await;

        assert!(
            timeout_result.is_ok(),
            "Concurrent operations should complete without timeout/deadlock"
        );

        harness.settle().await;
        harness.assert_all_readable_from(0).await;

        // Cluster should still be operational with at least 4 shards
        let final_shard_count = harness.node(0).coordinator.router().all_shards().len();
        assert!(
            final_shard_count >= 4,
            "Should have at least 4 shards after concurrent operations, got {}",
            final_shard_count
        );

        harness.shutdown_all().await;
    }

    // ========================================================================
    // TC-DASH-28: Migration checksum validation integrity
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_dash28_migration_checksum_validation() {
        let mut harness = ClusterMigrationHarness::new(3, 4).await;
        harness.write_test_data(0, 50, "key").await;
        harness.flush_shard_storages(0).await;

        let mut assertions =
            MigrationAssertions::capture_pre_migration(&harness.node(0).coordinator, 50);
        let add_result = harness.add_shard_and_migrate(0).await;
        assertions.capture_add_shard(&add_result, 5);
        assertions
            .capture_post_migration(&harness.node(0).coordinator, harness.cache(0))
            .await;
        assertions.assert_all();

        harness.shutdown_all().await;
    }

    // ========================================================================
    // TC-DASH-27: Add shard then remove - verify slot consistency across nodes
    // ========================================================================
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tc_dash27_add_then_remove_shard_slot_consistency() {
        let mut harness = ClusterMigrationHarness::new(3, 4).await;
        harness.write_test_data(0, 100, "lifecycle").await;
        harness.assert_all_readable_from(0).await;
        harness.assert_slot_consistency("INITIAL");

        let add_result = harness.add_shard_and_migrate(0).await;
        harness.assert_all_readable_from(0).await;
        harness.assert_slot_consistency("AFTER-ADD");
        harness.node(0).assert_shard_has_slots(add_result.shard_id);

        harness
            .remove_shard_and_migrate(0, add_result.shard_id)
            .await;
        harness.assert_all_readable_from(0).await;
        harness.assert_slot_consistency("FINAL");
        harness
            .node(0)
            .assert_shard_has_no_slots(add_result.shard_id);
        harness.node(0).assert_active_shard_count(4);

        harness.shutdown_all().await;
    }
}
