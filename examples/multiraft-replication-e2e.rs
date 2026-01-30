//! E2E test for cross-node replication
//!
//! This test verifies that:
//! 1. A 3-node cluster properly replicates data
//! 2. Data written to node1 is visible on node2 and node3
//!
//! Run with:
//!   cargo run --example multiraft-replication-e2e

use crema::{
    CacheConfig, DistributedCache, MemberlistConfig, MemberlistDiscovery, PeerManagementConfig,
    RaftConfig,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
struct NodeConfig {
    node_id: u64,
    raft_addr: SocketAddr,
    memberlist_addr: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info,crema=info".to_string()),
        )
        .init();

    println!("===========================================");
    println!("  Cross-Node Replication E2E Test");
    println!("  (Single-Raft Mode with Replication)");
    println!("===========================================\n");

    // Configuration for 3 nodes
    let nodes = vec![
        NodeConfig {
            node_id: 1,
            raft_addr: "127.0.0.1:19001".parse()?,
            memberlist_addr: "127.0.0.1:20001".parse()?,
        },
        NodeConfig {
            node_id: 2,
            raft_addr: "127.0.0.1:19002".parse()?,
            memberlist_addr: "127.0.0.1:20002".parse()?,
        },
        NodeConfig {
            node_id: 3,
            raft_addr: "127.0.0.1:19003".parse()?,
            memberlist_addr: "127.0.0.1:20003".parse()?,
        },
    ];

    // Start all nodes
    let mut caches: Vec<Arc<DistributedCache>> = Vec::new();

    println!("--- Starting 3-node cluster ---\n");

    for (idx, node_config) in nodes.iter().enumerate() {
        println!("Starting Node {}...", node_config.node_id);

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

        // Raft config with staggered election
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
            node_name: Some(format!("replication-test-node-{}", node_config.node_id)),
            peer_management: PeerManagementConfig {
                auto_add_peers: true,
                auto_remove_peers: false,
                auto_add_voters: false,
                auto_remove_voters: false,
            },
        };

        // Create discovery
        let discovery = MemberlistDiscovery::new(
            node_config.node_id,
            node_config.raft_addr,
            &memberlist_config,
            &peers,
        );

        // Create cache config (single-Raft mode for proper replication)
        let config = CacheConfig::new(node_config.node_id, node_config.raft_addr)
            .with_seed_nodes(peers)
            .with_max_capacity(100_000)
            .with_default_ttl(Duration::from_secs(3600))
            .with_raft_config(raft_config)
            .with_cluster_discovery(discovery);
        // Note: NOT using multi-raft config - single Raft properly replicates

        // Create cache
        let cache = Arc::new(DistributedCache::new(config).await?);
        caches.push(cache);

        println!("  Node {} started", node_config.node_id);

        // Small delay between nodes
        if idx < nodes.len() - 1 {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    println!("\n--- Waiting for cluster formation ---\n");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Wait for leader election
    let mut leader_found = false;
    let mut leader_node_idx = 0;
    for _ in 0..30 {
        for (idx, cache) in caches.iter().enumerate() {
            if cache.is_leader() {
                leader_found = true;
                leader_node_idx = idx;
                println!("Leader elected: Node {}", cache.node_id());
                break;
            }
        }
        if leader_found {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    if !leader_found {
        println!("ERROR: No leader elected!");
        for cache in &caches {
            cache.shutdown().await;
        }
        return Err("No leader elected".into());
    }

    // Write data to the leader node
    println!(
        "\n--- Test 1: Write Data to Leader (Node {}) ---\n",
        leader_node_idx + 1
    );
    let leader_cache = &caches[leader_node_idx];

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

    for (key, value) in &test_keys {
        match leader_cache.put(*key, *value).await {
            Ok(()) => {
                println!("  PUT '{}' = '{}' - OK", key, value);
            }
            Err(e) => {
                println!("  PUT '{}' - ERROR: {:?}", key, e);
            }
        }
    }

    // Wait for Raft replication
    println!("\n--- Waiting for Raft replication (3 seconds) ---\n");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify data on leader
    println!(
        "--- Test 2: Verify Data on Leader (Node {}) ---\n",
        leader_node_idx + 1
    );
    let mut leader_success = 0;
    for (key, expected_value) in &test_keys {
        match leader_cache.get(key.as_bytes()).await {
            Some(value) => {
                let actual = String::from_utf8_lossy(&value);
                if actual == *expected_value {
                    println!("  GET '{}' = '{}' - PASS", key, actual);
                    leader_success += 1;
                } else {
                    println!(
                        "  GET '{}' = '{}' (expected '{}') - FAIL",
                        key, actual, expected_value
                    );
                }
            }
            None => {
                println!("  GET '{}' = NOT FOUND - FAIL", key);
            }
        }
    }
    println!(
        "  Leader verification: {}/{} passed",
        leader_success,
        test_keys.len()
    );

    // Verify data on follower nodes (THIS IS THE KEY TEST FOR REPLICATION!)
    println!("\n--- Test 3: Verify Data Replicated to Followers ---\n");

    let mut all_replicated = true;
    for (idx, cache) in caches.iter().enumerate() {
        if idx == leader_node_idx {
            continue; // Skip leader, already tested
        }

        println!("Checking Node {} (follower):", idx + 1);
        let mut node_success = 0;

        for (key, expected_value) in &test_keys {
            match cache.get(key.as_bytes()).await {
                Some(value) => {
                    let actual = String::from_utf8_lossy(&value);
                    if actual == *expected_value {
                        println!("  GET '{}' = '{}' - PASS", key, actual);
                        node_success += 1;
                    } else {
                        println!(
                            "  GET '{}' = '{}' (expected '{}') - FAIL",
                            key, actual, expected_value
                        );
                        all_replicated = false;
                    }
                }
                None => {
                    println!("  GET '{}' = NOT FOUND - FAIL (data not replicated!)", key);
                    all_replicated = false;
                }
            }
        }
        println!(
            "  Node {} verification: {}/{} passed\n",
            idx + 1,
            node_success,
            test_keys.len()
        );
    }

    // Summary
    println!("===========================================");
    println!("  Test Summary");
    println!("===========================================\n");

    if all_replicated && leader_success == test_keys.len() {
        println!("  RESULT: ALL TESTS PASSED!");
        println!("  - Data written to leader node");
        println!("  - Data successfully replicated to all follower nodes");
        println!("  - Cross-node Raft replication is working correctly");
    } else {
        println!("  RESULT: SOME TESTS FAILED");
        if leader_success != test_keys.len() {
            println!("  - Leader data verification failed");
        }
        if !all_replicated {
            println!("  - Follower replication verification failed");
        }
    }

    // Show final stats
    println!("\n--- Final Stats ---\n");
    for (idx, cache) in caches.iter().enumerate() {
        let stats = cache.stats();
        let status = cache.cluster_status();
        println!(
            "Node {}: entries={}, is_leader={}, term={}",
            idx + 1,
            stats.entry_count,
            status.is_leader,
            status.term
        );
    }

    // Cleanup
    println!("\n--- Shutting down ---\n");
    for cache in caches {
        cache.shutdown().await;
    }
    println!("Done!");

    if all_replicated && leader_success == test_keys.len() {
        Ok(())
    } else {
        Err("Replication test failed".into())
    }
}
