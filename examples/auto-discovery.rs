//! Example of true auto-discovery using memberlist + Raft ConfChange.
//!
//! This example demonstrates how to set up a cluster where:
//! - Node 1 starts as a single-node cluster (bootstrap)
//! - Nodes 2 and 3 can join by only knowing node 1's memberlist address
//! - The leader automatically proposes ConfChange to add new voters
//!
//! Run this in three terminals:
//!   RUST_LOG=info cargo run --example auto-discovery -- 1
//!   (wait for node 1 to become leader, then...)
//!   RUST_LOG=info cargo run --example auto-discovery -- 2
//!   RUST_LOG=info cargo run --example auto-discovery -- 3
//!
//! Key features demonstrated:
//! - Bootstrap mode for first node
//! - Dynamic membership via Raft ConfChange
//! - Gossip-based node discovery
//! - No need to know all node IPs upfront

use crema::{CacheConfig, DistributedCache, MemberlistConfig, RaftConfig};
use std::env;
use std::net::SocketAddr;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    // Get node ID from command line
    let args: Vec<String> = env::args().collect();
    let node_id: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1);

    // Each node only needs to know its own addresses
    // Raft ports: 9001, 9002, 9003
    // Memberlist ports: 10001, 10002, 10003
    let my_raft_addr: SocketAddr = format!("127.0.0.1:{}", 9000 + node_id).parse()?;
    let my_memberlist_addr: SocketAddr = format!("127.0.0.1:{}", 10000 + node_id).parse()?;

    // Node 1 is the bootstrap seed - other nodes only need to know node 1's memberlist address
    let memberlist_seeds: Vec<SocketAddr> = if node_id == 1 {
        vec![] // Bootstrap node has no seeds
    } else {
        vec!["127.0.0.1:10001".parse().unwrap()] // Join via node 1
    };

    // For bootstrap (node 1), we start as a single-node cluster
    // For joining nodes, we provide an empty seed_nodes list - they will be added via ConfChange
    let raft_peers: Vec<(u64, SocketAddr)> = if node_id == 1 {
        vec![] // Bootstrap as single-node cluster
    } else {
        // Need to know at least one existing node to send ConfChange request
        // The leader's address will be discovered via memberlist
        vec![(1, "127.0.0.1:9001".parse().unwrap())]
    };

    println!("===========================================");
    println!("  Auto-Discovery Example - Node {}", node_id);
    println!("===========================================");
    println!();
    if node_id == 1 {
        println!("Mode:               BOOTSTRAP (single-node start)");
    } else {
        println!("Mode:               JOIN (via memberlist discovery)");
    }
    println!("Raft address:       {}", my_raft_addr);
    println!("Memberlist address: {}", my_memberlist_addr);
    println!("Memberlist seeds:   {:?}", memberlist_seeds);
    println!();

    // Create Raft configuration
    let raft_config = RaftConfig {
        election_tick: 10 + (node_id as usize * 3), // Stagger election timeouts
        heartbeat_tick: 3,
        tick_interval_ms: 100,
        pre_vote: true,
        ..Default::default()
    };

    // Create memberlist configuration with auto-add voters ENABLED
    let memberlist_config = MemberlistConfig {
        enabled: true,
        bind_addr: Some(my_memberlist_addr),
        advertise_addr: None,
        seed_addrs: memberlist_seeds,
        node_name: Some(format!("cache-node-{}", node_id)),
        auto_add_peers: true,     // Auto-add discovered peers to Raft transport
        auto_remove_peers: false, // Don't auto-remove (safer)
        auto_add_voters: true,    // ** KEY: Auto-add discovered nodes as Raft voters **
        auto_remove_voters: false, // Don't auto-remove voters
    };

    // Create the cache configuration
    let config = CacheConfig::new(node_id, my_raft_addr)
        .with_seed_nodes(raft_peers)
        .with_max_capacity(100_000)
        .with_default_ttl(Duration::from_secs(3600))
        .with_raft_config(raft_config)
        .with_memberlist_config(memberlist_config);

    // Create the cache
    println!("Starting distributed cache...");
    let cache = DistributedCache::new(config).await?;
    println!("Node {} started successfully!", node_id);
    println!();

    // Wait for cluster formation
    println!("Waiting for cluster formation...");
    println!("-------------------------------------------");

    let mut leader_found = false;
    let mut stable_count = 0;
    for i in 1..=30 {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let status = cache.cluster_status();
        let voters = cache.voters();

        println!(
            "[{:2}s] Term: {:2}, Leader: {:?}, Voters: {:?}, Is Leader: {}",
            i,
            status.term,
            status.leader_id,
            voters,
            status.is_leader
        );

        if status.leader_id.is_some() {
            if !leader_found {
                leader_found = true;
                println!("      ^ Leader elected!");
            }
            stable_count += 1;
        } else {
            stable_count = 0;
        }

        // Wait for leadership to be stable and for voters to include all expected nodes
        if leader_found && stable_count >= 3 {
            // Check if we have enough voters (for node 1, just itself; for others, at least 2)
            let expected_min_voters = if node_id == 1 { 1 } else { 2 };
            if voters.len() >= expected_min_voters {
                println!("      ^ Cluster stable with {} voters", voters.len());
                break;
            }
        }
    }

    println!("-------------------------------------------");
    println!();

    // Show role-specific information
    if cache.is_leader() {
        println!("=== This node is the LEADER ===");
        println!();

        // Demonstrate some operations
        println!("Performing distributed writes...");

        for i in 0..5 {
            let key = format!("auto-key-{}", i);
            let value = format!("value-from-node-{}-item-{}", node_id, i);

            match cache.put(key.clone(), value.clone()).await {
                Ok(()) => println!("  PUT '{}' = '{}' - OK", key, value),
                Err(e) => println!("  PUT '{}' - ERROR: {:?}", key, e),
            }
        }

        println!();
        println!("Verifying data...");

        for i in 0..5 {
            let key = format!("auto-key-{}", i);
            match cache.get(key.as_bytes()).await {
                Some(value) => println!(
                    "  GET '{}' = '{}'",
                    key,
                    String::from_utf8_lossy(&value)
                ),
                None => println!("  GET '{}' = (not found)", key),
            }
        }
    } else {
        println!("=== This node is a FOLLOWER ===");
        println!("Leader is: {:?}", cache.leader_id());
        println!();

        // Wait a bit for replication, then try to read
        println!("Waiting for data replication...");
        tokio::time::sleep(Duration::from_secs(3)).await;

        println!("Reading replicated data (if leader has written)...");
        for i in 0..5 {
            let key = format!("auto-key-{}", i);
            match cache.get(key.as_bytes()).await {
                Some(value) => println!(
                    "  GET '{}' = '{}'",
                    key,
                    String::from_utf8_lossy(&value)
                ),
                None => println!("  GET '{}' = (not found yet)", key),
            }
        }
    }

    // Show final cluster status
    println!();
    println!("===========================================");
    println!("           Final Cluster Status");
    println!("===========================================");

    let status = cache.cluster_status();
    println!("  Node ID:      {}", status.node_id);
    println!("  Is Leader:    {}", status.is_leader);
    println!("  Leader ID:    {:?}", status.leader_id);
    println!("  Term:         {}", status.term);
    println!("  Raft Peers:   {}", status.raft_peer_count);
    println!("  Voters:       {:?}", cache.voters());
    println!();

    // Keep running
    println!("Press Ctrl+C to stop...");
    println!();

    tokio::signal::ctrl_c().await?;

    println!();
    println!("Shutting down gracefully...");
    cache.shutdown().await;
    println!("Node {} stopped.", node_id);

    Ok(())
}
