//! Example of a 3-node cluster setup.
//!
//! Run this in three terminals with:
//!   RUST_LOG=info cargo run --example cluster -- 1
//!   RUST_LOG=info cargo run --example cluster -- 2
//!   RUST_LOG=info cargo run --example cluster -- 3

use crema::{CacheConfig, DistributedCache};
use std::env;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            env::var("RUST_LOG").unwrap_or_else(|_| "distributed_cache=info".to_string()),
        )
        .init();

    // Get node ID from command line
    let args: Vec<String> = env::args().collect();
    let node_id: u64 = args
        .get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    // Define cluster nodes
    let nodes = vec![
        (1, "127.0.0.1:9001"),
        (2, "127.0.0.1:9002"),
        (3, "127.0.0.1:9003"),
    ];

    // Find this node's address
    let my_addr = nodes
        .iter()
        .find(|(id, _)| *id == node_id)
        .map(|(_, addr)| *addr)
        .unwrap_or("127.0.0.1:9001");

    // Get seed nodes (all nodes except self) - include node IDs with addresses
    let seed_nodes: Vec<_> = nodes
        .iter()
        .filter(|(id, _)| *id != node_id)
        .map(|(id, addr)| (*id, addr.parse().unwrap()))
        .collect();

    println!("Starting node {} at {}", node_id, my_addr);
    println!("Seed nodes: {:?}", seed_nodes);

    // Create configuration
    let config = CacheConfig::new(node_id, my_addr.parse()?)
        .with_seed_nodes(seed_nodes)
        .with_max_capacity(100_000)
        .with_default_ttl(Duration::from_secs(3600));

    // Create the cache
    let cache = DistributedCache::new(config).await?;

    println!("Node {} started!", node_id);

    // Wait for cluster formation
    println!("Waiting for cluster formation...");
    for i in 1..=10 {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let status = cache.cluster_status();
        println!(
            "[{:2}s] Leader: {:?}, Term: {}, Is Leader: {}",
            i, status.leader_id, status.term, status.is_leader
        );

        if status.leader_id.is_some() {
            break;
        }
    }

    // If we're the leader, do some writes
    if cache.is_leader() {
        println!("\n=== This node is the leader! ===");
        println!("Performing distributed writes...");

        // These would go through Raft consensus
        // Note: In this example, other nodes would need to be running
        // for the writes to succeed (need quorum)

        println!("Leader node ready for operations.");
    } else {
        println!("\n=== This node is a follower ===");
        println!("Leader is: {:?}", cache.leader_id());
    }

    // Show final status
    let status = cache.cluster_status();
    println!("\nFinal cluster status:");
    println!("  Node ID: {}", status.node_id);
    println!("  Is Leader: {}", status.is_leader);
    println!("  Leader ID: {:?}", status.leader_id);
    println!("  Term: {}", status.term);
    println!("  Raft Peers: {}", status.raft_peer_count);

    // Keep running
    println!("\nPress Ctrl+C to stop...");
    tokio::signal::ctrl_c().await?;

    println!("Shutting down...");
    cache.shutdown().await;

    Ok(())
}
