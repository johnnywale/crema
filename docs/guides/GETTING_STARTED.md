# Getting Started

This guide walks you through setting up and running Crema, from a single-node cache to a full distributed cluster.

## Prerequisites

- Rust 1.70 or later
- Cargo
- (Optional) Multiple terminals for cluster setup

## Installation

Add Crema to your `Cargo.toml`:

```toml
[dependencies]
crema = { path = "../distributed-cache" }  # Or your crate source
tokio = { version = "1.0", features = ["full"] }
```

## Quick Start: Single Node

The simplest setup is a single-node cache:

```rust
use crema::{DistributedCache, CacheConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create configuration
    let config = CacheConfig::new(1, "127.0.0.1:9000".parse()?)
        .with_max_capacity(100_000)
        .with_default_ttl(Duration::from_secs(3600));

    // Create the cache
    let cache = DistributedCache::new(config).await?;

    // Wait for the node to become leader (single node = automatic leader)
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Write data (goes through Raft)
    cache.put("user:1", "Alice").await?;
    cache.put("user:2", "Bob").await?;

    // Read data (local read, very fast)
    if let Some(value) = cache.get(b"user:1").await {
        println!("user:1 = {:?}", String::from_utf8_lossy(&value));
    }

    // Shutdown gracefully
    cache.shutdown().await?;
    Ok(())
}
```

Run with:
```bash
cargo run --example basic
```

## Multi-Node Cluster (Manual Setup)

For a 3-node cluster with manual peer management:

### Node 1 (Initial Leader)

```rust
use crema::{DistributedCache, CacheConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = CacheConfig::new(1, "127.0.0.1:9001".parse()?)
        .with_seed_nodes(vec![
            "127.0.0.1:9002".parse()?,
            "127.0.0.1:9003".parse()?,
        ]);

    let cache = DistributedCache::new(config).await?;

    // Wait for cluster formation
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Add peers to Raft (manual approval)
    cache.add_peer(2).await?;
    cache.add_peer(3).await?;

    // Node 1 operations...
    Ok(())
}
```

### Node 2 and Node 3

```rust
// Node 2
let config = CacheConfig::new(2, "127.0.0.1:9002".parse()?)
    .with_seed_nodes(vec!["127.0.0.1:9001".parse()?]);

// Node 3
let config = CacheConfig::new(3, "127.0.0.1:9003".parse()?)
    .with_seed_nodes(vec!["127.0.0.1:9001".parse()?]);
```

Run in separate terminals:
```bash
# Terminal 1
RUST_LOG=info cargo run --example cluster -- 1

# Terminal 2
RUST_LOG=info cargo run --example cluster -- 2

# Terminal 3
RUST_LOG=info cargo run --example cluster -- 3
```

## Multi-Node Cluster with Memberlist (Recommended)

For automatic node discovery using gossip:

```rust
use crema::{DistributedCache, CacheConfig, MemberlistConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node_id = 1;
    let raft_port = 9000 + node_id as u16;
    let memberlist_port = 8000 + node_id as u16;

    let memberlist_config = MemberlistConfig::new(
        format!("127.0.0.1:{}", memberlist_port).parse()?,
    )
    .with_seeds(vec![
        "127.0.0.1:8001".parse()?,
        "127.0.0.1:8002".parse()?,
        "127.0.0.1:8003".parse()?,
    ])
    .with_auto_add_peers(true);  // Auto-register peers in transport

    let config = CacheConfig::new(node_id, format!("127.0.0.1:{}", raft_port).parse()?)
        .with_memberlist(memberlist_config);

    let cache = DistributedCache::new(config).await?;

    // Nodes discover each other automatically via gossip
    // But Raft membership still requires manual approval for safety

    Ok(())
}
```

Run in separate terminals:
```bash
# Terminal 1
RUST_LOG=info cargo run --example memberlist-cluster -- 1

# Terminal 2
RUST_LOG=info cargo run --example memberlist-cluster -- 2

# Terminal 3
RUST_LOG=info cargo run --example memberlist-cluster -- 3
```

## Basic Operations

### Writing Data

```rust
// Simple put
cache.put("key", "value").await?;

// Put with TTL
cache.put_with_ttl("session:abc", "user_data", Duration::from_secs(3600)).await?;

// Delete
cache.delete("key").await?;

// Clear all entries
cache.clear().await?;
```

### Reading Data

```rust
// Get a value
if let Some(value) = cache.get(b"key").await {
    println!("Found: {:?}", value);
}

// Check existence
if cache.contains(b"key").await {
    println!("Key exists");
}

// Get entry count
println!("Cache size: {}", cache.entry_count().await);
```

### Cluster Status

```rust
// Check if this node is the leader
if cache.is_leader() {
    println!("I am the leader");
}

// Get leader ID
if let Some(leader) = cache.leader_id() {
    println!("Leader is node {}", leader);
}

// Get cluster status
let status = cache.cluster_status();
println!("Cluster: {:?}", status);

// List voters
let voters = cache.voters();
println!("Voters: {:?}", voters);
```

### Memberlist Status

```rust
// Check if memberlist is enabled
if cache.memberlist_enabled() {
    // Get all discovered members
    let members = cache.memberlist_members().await;
    println!("Discovered {} nodes", members.len());

    // Get healthy members only
    let healthy = cache.memberlist_healthy_members().await;
    println!("Healthy nodes: {}", healthy.len());
}
```

## Configuration Options

### Cache Configuration

```rust
let config = CacheConfig::new(node_id, raft_addr)
    // Cache settings
    .with_max_capacity(100_000)           // Max entries
    .with_default_ttl(Duration::from_secs(3600))  // Default TTL
    .with_time_to_idle(Duration::from_secs(300))  // TTI (optional)

    // Seed nodes for cluster discovery
    .with_seed_nodes(vec![addr1, addr2])

    // Request forwarding
    .with_forwarding_enabled(true)
    .with_forwarding_timeout(Duration::from_secs(5))

    // Memberlist (gossip)
    .with_memberlist(memberlist_config);
```

### Raft Configuration

```rust
let raft_config = RaftConfig::default()
    .with_election_timeout(Duration::from_millis(1000))
    .with_heartbeat_interval(Duration::from_millis(100))
    .with_pre_vote(true);

let config = CacheConfig::new(node_id, raft_addr)
    .with_raft_config(raft_config);
```

See [Configuration Guide](./CONFIGURATION.md) for all options.

## Error Handling

```rust
use crema::{Error, Result};

match cache.put("key", "value").await {
    Ok(()) => println!("Success"),
    Err(Error::NotLeader { leader_hint }) => {
        println!("Not leader, try node {:?}", leader_hint);
    }
    Err(Error::Timeout) => {
        println!("Operation timed out");
    }
    Err(e) => {
        println!("Error: {:?}", e);
    }
}
```

## Next Steps

- [Configuration Guide](./CONFIGURATION.md) - All configuration options
- [Multi-Raft Guide](./MULTIRAFT_GUIDE.md) - Horizontal scaling
- [Architecture Overview](../architecture/OVERVIEW.md) - System design
- [Feature Status](../status/FEATURE_STATUS.md) - What's implemented
