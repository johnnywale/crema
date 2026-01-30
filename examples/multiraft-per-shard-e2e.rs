//! E2E test for per-shard Raft replication (Phase 2)
//!
//! This test demonstrates Multi-Raft with per-shard Raft replication:
//! - Each shard has its own independent Raft group for consensus
//! - Uses NodeMessageRouter for unified connection pooling across all shards
//! - Cross-node message routing via ShardRaft messages
//!
//! Run with:
//!   cargo run --example multiraft-per-shard-e2e

use crema::{
    CacheConfig, DistributedCache, MemberlistConfig, MemberlistDiscovery, MultiRaftCacheConfig,
    PeerManagementConfig, RaftConfig,
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
    println!("  Per-Shard Raft Replication E2E Test");
    println!("  (Multi-Raft Phase 2)");
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

    println!("--- Starting 3-node cluster with per-shard Raft ---\n");

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
            node_name: Some(format!("per-shard-test-node-{}", node_config.node_id)),
            peer_management: PeerManagementConfig {
                auto_add_peers: true,
                auto_remove_peers: false,
                auto_add_voters: false,
                auto_remove_voters: false,
            },
        };

        // Multi-Raft config with per-shard Raft enabled
        // auto_init_shards=true handles all initialization automatically
        let multiraft_config = MultiRaftCacheConfig {
            enabled: true,
            num_shards: 4,
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
        let cache = Arc::new(DistributedCache::new(config).await?);
        caches.push(cache);

        println!("  Node {} started", node_config.node_id);

        // Small delay between nodes
        if idx < nodes.len() - 1 {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    // Wait for cluster discovery and shard leader elections
    // All initialization happens automatically with auto_init_shards=true
    println!("\n--- Waiting for cluster to become ready ---\n");

    for (idx, cache) in caches.iter().enumerate() {
        print!("  Waiting for Node {}...", idx + 1);
        match cache.wait_until_ready(Duration::from_secs(30)).await {
            Ok(()) => {
                let (elected, total) = cache.shard_leader_status();
                println!(" READY (shard leaders: {}/{})", elected, total);
            }
            Err(_) => {
                println!(" TIMEOUT");
            }
        }
    }

    // Write test data via node 1
    println!("\n--- Test 1: Write Data via Node 1 ---\n");
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
        match write_cache.put(*key, *value).await {
            Ok(()) => println!("  PUT '{}' = '{}' - OK", key, value),
            Err(e) => println!("  PUT '{}' - ERROR: {:?}", key, e),
        }
    }

    // Wait for replication
    println!("\n--- Waiting for replication (3 seconds) ---\n");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify data on all nodes
    println!("--- Test 2: Verify Data on All Nodes ---\n");

    let mut all_pass = true;
    let mut entry_counts: Vec<u64> = Vec::new();

    for (node_idx, cache) in caches.iter().enumerate() {
        println!("Checking Node {}:", node_idx + 1);
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
                        all_pass = false;
                    }
                }
                None => {
                    println!("  GET '{}' = NOT FOUND - FAIL", key);
                    all_pass = false;
                }
            }
        }
        println!(
            "  Node {} verification: {}/{} passed",
            node_idx + 1,
            node_success,
            test_keys.len()
        );

        // Get entry count
        let stats = cache.stats();
        entry_counts.push(stats.entry_count);
        println!(
            "  Node {} entry_count: {}\n",
            node_idx + 1,
            stats.entry_count
        );
    }

    // Validate entry counts match across all nodes
    println!("--- Test 3: Validate Entry Counts ---\n");
    let expected_entries = test_keys.len() as u64;
    let mut counts_match = true;

    for (node_idx, count) in entry_counts.iter().enumerate() {
        let status = if *count == expected_entries {
            "PASS"
        } else {
            "FAIL"
        };
        println!(
            "  Node {}: {} entries (expected {}) - {}",
            node_idx + 1,
            count,
            expected_entries,
            status
        );
        if *count != expected_entries {
            counts_match = false;
            all_pass = false;
        }
    }

    // Check if all nodes have the same count
    let first_count = entry_counts[0];
    let all_same = entry_counts.iter().all(|c| *c == first_count);
    if !all_same {
        println!("\n  WARNING: Entry counts differ between nodes!");
        println!("  This indicates replication is not working correctly.");
        all_pass = false;
    }

    // Summary
    println!("\n===========================================");
    println!("  Test Summary");
    println!("===========================================\n");

    if all_pass && counts_match {
        println!("  RESULT: ALL TESTS PASSED!");
        println!("  - Data written via node 1");
        println!("  - Data replicated to all nodes");
        println!("  - Entry counts match across all nodes");
    } else {
        println!("  RESULT: SOME TESTS FAILED");
        if !counts_match {
            println!("  - Entry counts do not match (replication issue)");
            println!(
                "  - Leader: {} entries, Followers: {:?}",
                entry_counts[0],
                &entry_counts[1..]
            );
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

    if all_pass && counts_match {
        Ok(())
    } else {
        Err("Per-shard Raft replication test failed".into())
    }
}
