//! Dashboard Demo - A single-command 3-node cluster with web dashboards.
//!
//! This example starts a 3-node Multi-Raft cluster with:
//! - Per-shard Raft replication (Phase 2) - each shard is an independent Raft group
//! - Web dashboards on ports 8081, 8082, 8083
//! - 8 shards for horizontal scaling
//! - 100 sample entries seeded via a FOLLOWER node (demonstrates forwarding)
//! - Background traffic generator targeting follower node for live metrics
//!
//! The demo sends all writes to a follower node to demonstrate per-shard Raft
//! forwarding behavior - follower nodes forward writes to the appropriate shard leader.
//!
//! Run with:
//!   cargo run --example dashboard-demo --features dashboard
//!
//! Then open:
//!   http://localhost:8081 (Node 1)
//!   http://localhost:8082 (Node 2)
//!   http://localhost:8083 (Node 3)

use crema::dashboard::{DashboardConfig, DashboardServer};
use crema::{
    CacheConfig, DistributedCache, MemberlistConfig, MemberlistDiscovery, MultiRaftCacheConfig,
    PeerManagementConfig, RaftConfig,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info,crema=info".to_string()),
        )
        .init();

    println!("===========================================");
    println!("     Crema Dashboard Demo");
    println!("===========================================");
    println!();

    // Configuration for 3 nodes
    let nodes = vec![
        NodeConfig {
            node_id: 1,
            raft_addr: "127.0.0.1:9001".parse()?,
            memberlist_addr: "127.0.0.1:10001".parse()?,
            dashboard_port: 8081,
        },
        NodeConfig {
            node_id: 2,
            raft_addr: "127.0.0.1:9002".parse()?,
            memberlist_addr: "127.0.0.1:10002".parse()?,
            dashboard_port: 8082,
        },
        NodeConfig {
            node_id: 3,
            raft_addr: "127.0.0.1:9003".parse()?,
            memberlist_addr: "127.0.0.1:10003".parse()?,
            dashboard_port: 8083,
        },
    ];

    // Create shutdown broadcast channel
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Start all nodes
    let mut caches = Vec::new();
    let mut dashboard_handles = Vec::new();

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
            node_name: Some(format!("demo-node-{}", node_config.node_id)),
            peer_management: PeerManagementConfig {
                auto_add_peers: true,
                auto_remove_peers: false,
                auto_add_voters: false,
                auto_remove_voters: false,
            },
        };

        // Multi-Raft config (8 shards for horizontal scaling)
        // Phase 2 per-shard Raft: each shard is an independent Raft group
        // auto_init_shards=true handles all initialization automatically
        let multiraft_config = MultiRaftCacheConfig {
            enabled: true,
            num_shards: 8,
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
        caches.push(cache.clone());

        // Create and start dashboard
        let dashboard_config = DashboardConfig::default()
            .with_port(node_config.dashboard_port)
            .with_bind_addr("127.0.0.1");

        let dashboard = DashboardServer::new(cache.clone(), dashboard_config);
        let _url = dashboard.url();
        let handle = dashboard.start_background();
        dashboard_handles.push(handle);

        // Small delay between nodes to avoid port conflicts
        if idx < nodes.len() - 1 {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    // All per-shard Raft initialization happens automatically with auto_init_shards=true
    println!();
    println!("Waiting for cluster formation and shard leader elections...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Wait for ALL shard leaders to be elected (critical for per-shard Raft)
    let num_shards = 8u32;
    let mut all_shards_have_leaders = false;

    for attempt in 0..60 {
        // Check if all shards have leaders on the first node
        if let Some(coordinator) = caches[0].multiraft_coordinator() {
            let leaders = coordinator.shard_leaders();
            let known_leaders: Vec<_> = leaders
                .iter()
                .filter(|(_, leader)| leader.is_some())
                .collect();

            if known_leaders.len() == num_shards as usize {
                all_shards_have_leaders = true;
                println!("All {} shard leaders elected:", num_shards);
                for (shard_id, leader) in leaders.iter() {
                    if let Some(leader_id) = leader {
                        println!("  Shard {}: Leader Node {}", shard_id, leader_id);
                    }
                }
                break;
            } else if attempt % 10 == 0 {
                println!(
                    "  Waiting for shard leaders... {}/{} known (attempt {})",
                    known_leaders.len(),
                    num_shards,
                    attempt + 1
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    if !all_shards_have_leaders {
        println!("Warning: Not all shard leaders elected yet, some operations may fail");
    }

    // Enable slot-based routing for dynamic shard management
    println!();
    println!("Enabling slot-based routing...");
    for cache in &caches {
        if let Err(e) = cache.enable_slot_routing().await {
            eprintln!("Warning: Failed to enable slot routing: {}", e);
        }
    }

    // Start slot migration background loop
    println!("Starting slot migration background loop...");
    for cache in &caches {
        cache.start_slot_migration();
    }
    println!("Slot routing enabled on all nodes.");

    // Also check main Raft leader
    let mut leader_found = false;
    for cache in &caches {
        if cache.is_leader() {
            leader_found = true;
            println!("Main Raft leader: Node {}", cache.node_id());
            break;
        }
    }
    if !leader_found {
        println!("Warning: Main Raft leader not elected yet");
    }

    // Find a follower node to send all traffic to (per-shard Raft replication)
    // With per-shard Raft, writes to a follower are forwarded to the shard leader
    println!();
    println!("Finding a follower node for traffic...");

    let mut follower_idx: Option<usize> = None;
    for (idx, cache) in caches.iter().enumerate() {
        if !cache.is_leader() {
            follower_idx = Some(idx);
            println!("  Using Node {} (follower) for all traffic", idx + 1);
            break;
        }
    }

    // Fallback to node 2 if no follower found yet (cluster still forming)
    let follower_idx = follower_idx.unwrap_or(1);
    let follower_cache = &caches[follower_idx];
    println!(
        "  Selected Node {} (is_leader={}) for seeding",
        follower_idx + 1,
        follower_cache.is_leader()
    );

    // Seed sample data via FOLLOWER node only
    // Per-shard Raft will forward writes to the appropriate shard leader
    println!();
    println!(
        "Seeding 100 sample entries via follower node {}...",
        follower_idx + 1
    );

    for i in 0..100 {
        let key = format!("key:{:03}", i);
        let value = format!("value-{}", i);
        if let Err(e) = follower_cache.put(key.clone(), value).await {
            eprintln!("  Failed to seed {}: {}", key, e);
        }
    }
    println!(
        "  Seeded 100 entries via follower node {}",
        follower_idx + 1
    );

    // Start traffic generator in background
    // All traffic goes to a single follower node (per-shard Raft forwards to leaders)
    // Generates ~1000 ops/sec (batch of 10 ops every 10ms)
    let traffic_cache = caches[follower_idx].clone();
    let mut shutdown_rx = shutdown_tx.subscribe();
    let traffic_node_id = follower_idx + 1;

    println!(
        "Starting traffic generator targeting follower Node {}...",
        traffic_node_id
    );

    tokio::spawn(async move {
        let mut counter = 0u64;
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => break,
                _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    // Batch of 10 operations per tick for higher throughput
                    for _ in 0..10 {
                        counter += 1;

                        // All traffic goes to the follower node
                        // Per-shard Raft will forward writes to the appropriate shard leader

                        // 80% reads, 20% writes
                        if counter % 5 == 0 {
                            // Write
                            let key = format!("traffic:{}", counter % 200);
                            let value = format!("val-{}-{}", counter, chrono_lite_timestamp());
                            let _ = traffic_cache.put(key, value).await;
                        } else {
                            // Read
                            let key = format!("key:{:03}", counter % 100);
                            let _ = traffic_cache.get(key.as_bytes()).await;
                        }
                    }
                }
            }
        }
    });

    println!();
    println!("===========================================");
    println!("     Dashboard URLs");
    println!("===========================================");
    println!();
    println!("  Node 1: http://localhost:8081");
    println!("  Node 2: http://localhost:8082");
    println!("  Node 3: http://localhost:8083");
    println!();
    println!(
        "Traffic generator running (~1000 ops/sec via follower Node {})",
        traffic_node_id
    );
    println!();
    println!("Press Ctrl+C to stop...");
    println!();

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;

    println!();
    println!("Shutting down...");

    // Signal shutdown
    let _ = shutdown_tx.send(());

    // Shutdown all caches
    for cache in caches {
        cache.shutdown().await;
    }

    println!("Done!");

    Ok(())
}

#[derive(Clone)]
struct NodeConfig {
    node_id: u64,
    raft_addr: SocketAddr,
    memberlist_addr: SocketAddr,
    dashboard_port: u16,
}

/// Simple timestamp without chrono dependency
fn chrono_lite_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
