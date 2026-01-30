//! Quick benchmark for Multi-Raft insert performance
//!
//! Run with:
//!   cargo run --example benchmark-multiraft --release

use crema::{
    CacheConfig, DistributedCache, MultiRaftCacheConfig, NoOpClusterDiscovery, RaftConfig,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node_id = 1;
    let raft_addr: std::net::SocketAddr = "127.0.0.1:19001".parse()?;

    let raft_config = RaftConfig {
        election_tick: 10,
        heartbeat_tick: 3,
        tick_interval_ms: 100,
        pre_vote: true,
        ..Default::default()
    };

    let multiraft_config = MultiRaftCacheConfig {
        enabled: true,
        num_shards: 8,
        shard_capacity: 100_000,
        auto_init_shards: true,
        leader_broadcast_debounce_ms: 200,
        per_shard_raft_enabled: false,
        ..Default::default()
    };

    let discovery = NoOpClusterDiscovery::new(node_id, raft_addr);

    let config = CacheConfig::new(node_id, raft_addr)
        .with_max_capacity(100_000)
        .with_default_ttl(Duration::from_secs(3600))
        .with_raft_config(raft_config)
        .with_cluster_discovery(discovery)
        .with_multiraft_config(multiraft_config);

    println!("Creating Multi-Raft cache with 8 shards...");
    let cache = Arc::new(DistributedCache::new(config).await?);

    // Wait for initialization
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("\n=== Benchmark: Sequential Inserts ===");
    let num_ops = 10_000;

    let start = Instant::now();
    for i in 0..num_ops {
        let key = format!("bench-key-{:06}", i);
        let value = format!("bench-value-{}", i);
        cache.put(key, value).await?;
    }
    let elapsed = start.elapsed();

    let ops_per_sec = num_ops as f64 / elapsed.as_secs_f64();
    println!("  {} inserts in {:?}", num_ops, elapsed);
    println!("  Throughput: {:.0} ops/sec", ops_per_sec);

    // Run pending tasks to sync counters
    cache.run_pending_tasks().await;

    println!("\n=== Benchmark: Concurrent Inserts ===");
    let num_tasks = 10;
    let ops_per_task = 1_000;

    let start = Instant::now();
    let mut handles = vec![];

    for task_id in 0..num_tasks {
        let cache = cache.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..ops_per_task {
                let key = format!("concurrent-{}-{:06}", task_id, i);
                let value = format!("value-{}-{}", task_id, i);
                let _ = cache.put(key, value).await;
            }
        }));
    }

    for handle in handles {
        handle.await?;
    }
    let elapsed = start.elapsed();

    let total_ops = num_tasks * ops_per_task;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    println!(
        "  {} inserts ({} tasks x {}) in {:?}",
        total_ops, num_tasks, ops_per_task, elapsed
    );
    println!("  Throughput: {:.0} ops/sec", ops_per_sec);

    // Final stats
    cache.run_pending_tasks().await;
    let stats = cache.stats();
    println!("\n=== Final Stats ===");
    println!("  Entry count: {}", stats.entry_count);

    cache.shutdown().await;
    println!("\nDone!");

    Ok(())
}
