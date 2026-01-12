//! Basic example of using the distributed cache.

use crema::{CacheConfig, DistributedCache};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("distributed_cache=debug,info")
        .init();

    // Create configuration for node 1
    let config = CacheConfig::new(1, "127.0.0.1:9001".parse()?)
        .with_max_capacity(100_000)
        .with_default_ttl(Duration::from_secs(3600));

    println!("Starting distributed cache node 1...");

    // Create the cache
    let cache = DistributedCache::new(config).await?;

    println!("Cache started!");
    println!("Node ID: {}", cache.node_id());
    println!("Is Leader: {}", cache.is_leader());

    // Local operations (don't go through Raft)
    println!("\n--- Local Operations ---");
    cache.put_local("local:key1", "local value 1").await;
    cache.put_local("local:key2", "local value 2").await;

    if let Some(value) = cache.get(b"local:key1").await {
        println!("Got local:key1 = {:?}", String::from_utf8_lossy(&value));
    }

    // Show stats
    let stats = cache.stats();
    println!("\nCache stats:");
    println!("  Entry count: {}", stats.entry_count);
    println!("  Hits: {}", stats.hits);
    println!("  Misses: {}", stats.misses);

    // Show cluster status
    let status = cache.cluster_status();
    println!("\nCluster status:");
    println!("  Node ID: {}", status.node_id);
    println!("  Is Leader: {}", status.is_leader);
    println!("  Leader ID: {:?}", status.leader_id);
    println!("  Term: {}", status.term);
    println!("  Raft Peers: {}", status.raft_peer_count);
    println!("  Commit Index: {}", status.commit_index);
    println!("  Applied Index: {}", status.applied_index);

    // Note: Distributed operations (put, delete) require leader status
    // In a single-node setup, the node should become leader after election timeout
    println!("\nWaiting for leader election...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let status = cache.cluster_status();
    println!("After waiting:");
    println!("  Is Leader: {}", status.is_leader);
    println!("  Leader ID: {:?}", status.leader_id);

    // Shutdown
    println!("\nShutting down...");
    cache.shutdown().await;

    Ok(())
}
