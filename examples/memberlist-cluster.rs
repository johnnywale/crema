//! Example of a 3-node cluster with memberlist gossip enabled.
//!
//! This example demonstrates how to use memberlist for:
//! - Gossip-based node discovery and health monitoring
//! - Automatic peer address updates
//! - Cluster membership events
//!
//! Run this in three terminals with:
//!   RUST_LOG=info cargo run --example memberlist-cluster -- 1
//!   RUST_LOG=info cargo run --example memberlist-cluster -- 2
//!   RUST_LOG=info cargo run --example memberlist-cluster -- 3
//!
//! Key differences from the basic cluster example:
//! - Memberlist provides gossip-based health monitoring (faster failure detection)
//! - Nodes can discover each other's addresses via gossip
//! - Cluster membership events are available for monitoring

use crema::{CacheConfig, DistributedCache, MemberlistConfig, MemberlistDiscovery, PeerManagementConfig, RaftConfig};
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

    // Define cluster nodes with both Raft and Memberlist ports
    // Raft ports: 9001, 9002, 9003
    // Memberlist ports: 10001, 10002, 10003
    let nodes = vec![
        (1, "127.0.0.1:9001", "127.0.0.1:10001"),
        (2, "127.0.0.1:9002", "127.0.0.1:10002"),
        (3, "127.0.0.1:9003", "127.0.0.1:10003"),
    ];

    // Find this node's addresses
    let (my_raft_addr, my_memberlist_addr) = nodes
        .iter()
        .find(|(id, _, _)| *id == node_id)
        .map(|(_, raft, ml)| (*raft, *ml))
        .unwrap_or(("127.0.0.1:9001", "127.0.0.1:10001"));

    // Get Raft peer nodes (all nodes except self)
    let raft_peers: Vec<(u64, SocketAddr)> = nodes
        .iter()
        .filter(|(id, _, _)| *id != node_id)
        .map(|(id, raft_addr, _)| (*id, raft_addr.parse().unwrap()))
        .collect();

    // Get memberlist seed addresses (node 1 is the seed for others)
    // In production, you might use multiple seeds for redundancy
    let memberlist_seeds: Vec<SocketAddr> = if node_id == 1 {
        // Node 1 is the seed, no seeds for itself
        vec![]
    } else {
        // Other nodes use node 1 as their seed
        vec!["127.0.0.1:10001".parse().unwrap()]
    };

    println!("===========================================");
    println!("  Memberlist Cluster Example - Node {}", node_id);
    println!("===========================================");
    println!();
    println!("Raft address:       {}", my_raft_addr);
    println!("Memberlist address: {}", my_memberlist_addr);
    println!("Raft peers:         {:?}", raft_peers);
    println!("Memberlist seeds:   {:?}", memberlist_seeds);
    println!();

    // Create Raft configuration
    let raft_config = RaftConfig {
        election_tick: 10 + (node_id as usize * 5), // Stagger election timeouts
        heartbeat_tick: 3,
        tick_interval_ms: 100,
        pre_vote: true,
        ..Default::default()
    };

    // Create memberlist configuration
    let memberlist_config = MemberlistConfig {
        enabled: true,
        bind_addr: Some(my_memberlist_addr.parse()?),
        advertise_addr: None,
        seed_addrs: memberlist_seeds,
        node_name: Some(format!("cache-node-{}", node_id)),
        peer_management: PeerManagementConfig {
            auto_add_peers: true,     // Auto-add discovered peers to Raft transport
            auto_remove_peers: false, // Don't auto-remove (safer)
            auto_add_voters: false,   // We manually configure voters in this example
            auto_remove_voters: false,
        },
    };

    // Create MemberlistDiscovery for cluster discovery
    let discovery = MemberlistDiscovery::new(
        node_id,
        my_raft_addr.parse()?,
        &memberlist_config,
        &raft_peers,
    );

    // Create the cache configuration
    let config = CacheConfig::new(node_id, my_raft_addr.parse()?)
        .with_seed_nodes(raft_peers)
        .with_max_capacity(100_000)
        .with_default_ttl(Duration::from_secs(3600))
        .with_raft_config(raft_config)
        .with_cluster_discovery(discovery);

    // Create the cache
    println!("Starting distributed cache with memberlist...");
    let cache = DistributedCache::new(config).await?;
    println!("Node {} started successfully!", node_id);
    println!();

    // Wait for cluster formation
    println!("Waiting for cluster formation...");
    println!("-------------------------------------------");

    let mut leader_found = false;
    for i in 1..=15 {
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

        if status.leader_id.is_some() && !leader_found {
            leader_found = true;
            println!("      ^ Leader elected!");
        }

        // Once leader is stable for a few seconds, proceed
        if leader_found && i >= 5 {
            break;
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
            let key = format!("memberlist-key-{}", i);
            let value = format!("value-from-node-{}-item-{}", node_id, i);

            match cache.put(key.clone(), value.clone()).await {
                Ok(()) => println!("  PUT '{}' = '{}' - OK", key, value),
                Err(e) => println!("  PUT '{}' - ERROR: {:?}", key, e),
            }
        }

        println!();
        println!("Verifying data...");

        for i in 0..5 {
            let key = format!("memberlist-key-{}", i);
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
            let key = format!("memberlist-key-{}", i);
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
