//! Example of a 3-node cluster with Multi-Raft sharding for horizontal scaling.
//!
//! This example demonstrates how to use Multi-Raft mode for:
//! - Horizontal scaling beyond single Raft group limits
//! - Distributing write load across multiple independent Raft groups (shards)
//! - Gossip-based shard leader discovery
//!
//! Run this in three terminals with:
//!   RUST_LOG=info cargo run --example multiraft-cluster -- 1
//!   RUST_LOG=info cargo run --example multiraft-cluster -- 2
//!   RUST_LOG=info cargo run --example multiraft-cluster -- 3
//!
//! Key concepts:
//! - Each shard is an independent Raft group with its own leader
//! - Keys are routed to shards via: shard_id = hash(key) % num_shards
//! - Write throughput scales linearly with shard count
//! - Memberlist is required for gossip-based shard leader discovery

use crema::{CacheConfig, DistributedCache, MemberlistConfig, MultiRaftCacheConfig, RaftConfig};
use std::collections::HashMap;
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

    // Define cluster nodes with Raft and Memberlist ports
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

    // Get Raft peer nodes
    let raft_peers: Vec<(u64, SocketAddr)> = nodes
        .iter()
        .filter(|(id, _, _)| *id != node_id)
        .map(|(id, raft_addr, _)| (*id, raft_addr.parse().unwrap()))
        .collect();

    // Memberlist seeds (node 1 is the seed for others)
    let memberlist_seeds: Vec<SocketAddr> = if node_id == 1 {
        vec![]
    } else {
        vec!["127.0.0.1:10001".parse().unwrap()]
    };

    println!("===========================================");
    println!("  Multi-Raft Cluster Example - Node {}", node_id);
    println!("===========================================");
    println!();
    println!("Raft address:       {}", my_raft_addr);
    println!("Memberlist address: {}", my_memberlist_addr);
    println!("Raft peers:         {:?}", raft_peers);
    println!("Memberlist seeds:   {:?}", memberlist_seeds);
    println!();

    // Number of shards (independent Raft groups)
    let num_shards = 8;

    // Create Raft configuration with staggered election timeouts
    let raft_config = RaftConfig {
        election_tick: 10 + (node_id as usize * 5),
        heartbeat_tick: 3,
        tick_interval_ms: 100,
        pre_vote: true,
        ..Default::default()
    };

    // Create memberlist configuration (REQUIRED for Multi-Raft)
    let memberlist_config = MemberlistConfig {
        enabled: true,
        bind_addr: Some(my_memberlist_addr.parse()?),
        advertise_addr: None,
        seed_addrs: memberlist_seeds,
        node_name: Some(format!("multiraft-node-{}", node_id)),
        auto_add_peers: true,
        auto_remove_peers: false,
        auto_add_voters: false,
        auto_remove_voters: false,
    };

    // Create Multi-Raft configuration
    let multiraft_config = MultiRaftCacheConfig {
        enabled: true,
        num_shards,
        shard_capacity: 10_000,
        auto_init_shards: true,
        leader_broadcast_debounce_ms: 200,
    };

    println!("Multi-Raft Configuration:");
    println!("  Shards: {}", num_shards);
    println!("  Shard capacity: {}", multiraft_config.shard_capacity);
    println!();

    // Create the cache configuration
    let config = CacheConfig::new(node_id, my_raft_addr.parse()?)
        .with_seed_nodes(raft_peers)
        .with_max_capacity(100_000)
        .with_default_ttl(Duration::from_secs(3600))
        .with_raft_config(raft_config)
        .with_memberlist_config(memberlist_config)
        .with_multiraft_config(multiraft_config);

    // Validate configuration
    config.validate().expect("Invalid configuration");

    // Create the cache
    println!("Starting Multi-Raft distributed cache...");
    let cache = DistributedCache::new(config).await?;
    println!("Node {} started successfully!", node_id);
    println!();

    // Verify Multi-Raft is enabled
    if cache.is_multiraft_enabled() {
        println!("Multi-Raft mode: ENABLED");
    } else {
        println!("WARNING: Multi-Raft mode is NOT enabled!");
    }
    println!();

    // Wait for cluster formation
    println!("Waiting for cluster and shard formation...");
    println!("-------------------------------------------");

    for i in 1..=20 {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let status = cache.cluster_status();

        println!(
            "[{:2}s] Term: {:2}, Leader: {:?}, Is Leader: {}",
            i, status.term, status.leader_id, status.is_leader
        );

        // After some time, show shard distribution
        if i == 15 && cache.is_multiraft_enabled() {
            if let Some(coordinator) = cache.multiraft_coordinator() {
                println!();
                println!("Shard Leader Distribution:");
                // Note: In a real scenario, you would track shard leaders
                // via the ShardLeaderTracker which uses gossip
                println!("  (Leaders are distributed across {} shards)", num_shards);
            }
        }

        if status.leader_id.is_some() && i >= 10 {
            break;
        }
    }

    println!("-------------------------------------------");
    println!();

    // Demonstrate Multi-Raft operations
    println!("===========================================");
    println!("       Multi-Raft Operations Demo");
    println!("===========================================");
    println!();

    // Show key-to-shard mapping
    println!("Key to Shard Mapping (hash(key) % {}):", num_shards);
    let test_keys = vec![
        "user:1",
        "user:2",
        "user:3",
        "session:abc",
        "session:def",
        "order:100",
        "order:200",
        "product:999",
    ];

    let mut shard_keys: HashMap<u32, Vec<&str>> = HashMap::new();
    for key in &test_keys {
        if let Some(shard_id) = cache.shard_for_key(key.as_bytes()) {
            println!("  '{}' -> Shard {}", key, shard_id);
            shard_keys.entry(shard_id).or_default().push(key);
        }
    }

    println!();
    println!("Keys per shard:");
    for shard_id in 0..num_shards {
        let keys = shard_keys.get(&shard_id).map(|v| v.len()).unwrap_or(0);
        if keys > 0 {
            println!("  Shard {}: {} keys", shard_id, keys);
        }
    }
    println!();

    // Only leader performs writes
    if cache.is_leader() {
        println!("=== This node is a LEADER for main Raft group ===");
        println!();
        println!("Writing data across shards...");
        println!();

        for key in &test_keys {
            let value = format!("value-for-{}", key);
            let shard = cache.shard_for_key(key.as_bytes()).unwrap_or(0);

            match cache.put(*key, value.clone()).await {
                Ok(()) => {
                    println!("  PUT '{}' (shard {}) = '{}' - OK", key, shard, value)
                }
                Err(e) => println!("  PUT '{}' (shard {}) - ERROR: {:?}", key, shard, e),
            }
        }

        println!();
        println!("Verifying data...");
        println!();

        for key in &test_keys {
            let shard = cache.shard_for_key(key.as_bytes()).unwrap_or(0);
            match cache.get(key.as_bytes()).await {
                Some(value) => {
                    println!(
                        "  GET '{}' (shard {}) = '{}'",
                        key,
                        shard,
                        String::from_utf8_lossy(&value)
                    )
                }
                None => println!("  GET '{}' (shard {}) = (not found)", key, shard),
            }
        }
    } else {
        println!("=== This node is a FOLLOWER ===");
        println!("Leader is: {:?}", cache.leader_id());
        println!();

        // Wait for replication
        println!("Waiting for data replication...");
        tokio::time::sleep(Duration::from_secs(5)).await;

        println!();
        println!("Reading replicated data across shards...");
        println!();

        for key in &test_keys {
            let shard = cache.shard_for_key(key.as_bytes()).unwrap_or(0);
            match cache.get(key.as_bytes()).await {
                Some(value) => {
                    println!(
                        "  GET '{}' (shard {}) = '{}'",
                        key,
                        shard,
                        String::from_utf8_lossy(&value)
                    )
                }
                None => println!("  GET '{}' (shard {}) = (not found yet)", key, shard),
            }
        }
    }

    // Show final status
    println!();
    println!("===========================================");
    println!("           Final Cluster Status");
    println!("===========================================");

    let status = cache.cluster_status();
    println!("  Node ID:        {}", status.node_id);
    println!("  Is Leader:      {}", status.is_leader);
    println!("  Leader ID:      {:?}", status.leader_id);
    println!("  Term:           {}", status.term);
    println!("  Raft Peers:     {}", status.raft_peer_count);
    println!("  Multi-Raft:     {}", cache.is_multiraft_enabled());

    if cache.is_multiraft_enabled() {
        println!("  Num Shards:     {}", num_shards);
    }

    let stats = cache.stats();
    println!("  Cache Entries:  {}", stats.entry_count);
    println!();

    // Keep running
    println!("Press Ctrl+C to stop...");
    println!();
    println!("Tip: Run multiple operations to see load distribution across shards.");
    println!("     Each shard has its own Raft leader for parallel writes.");
    println!();

    tokio::signal::ctrl_c().await?;

    println!();
    println!("Shutting down gracefully...");
    cache.shutdown().await;
    println!("Node {} stopped.", node_id);

    Ok(())
}
