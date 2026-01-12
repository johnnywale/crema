# Multi-Raft Module

Horizontal scaling through multiple independent Raft groups.

## Overview

Multi-Raft partitions the keyspace into multiple shards, each with its own independent Raft consensus group. This enables linear scaling of write throughput by distributing the leader bottleneck across multiple nodes.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      MultiRaftCoordinator                           │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                       ShardRouter                            │   │
│  │   key → hash(key) % num_shards → shard_id → Shard           │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              │                                      │
│        ┌─────────────────────┼─────────────────────┐               │
│        ▼                     ▼                     ▼               │
│  ┌───────────┐         ┌───────────┐         ┌───────────┐        │
│  │  Shard 0  │         │  Shard 1  │         │  Shard N  │        │
│  │           │         │           │         │           │        │
│  │ Raft Grp  │         │ Raft Grp  │         │ Raft Grp  │        │
│  │ Leader: A │         │ Leader: B │         │ Leader: C │        │
│  │           │         │           │         │           │        │
│  │ Moka Cache│         │ Moka Cache│         │ Moka Cache│        │
│  └───────────┘         └───────────┘         └───────────┘        │
└─────────────────────────────────────────────────────────────────────┘
```

## Why Multi-Raft?

### Single Raft Limitations

A single Raft group has inherent limitations:

| Factor | Impact |
|--------|--------|
| Leader bottleneck | All writes go through one node |
| Network latency | Replication adds RTT per write |
| Disk fsync | Durability requires disk sync |
| **Theoretical max** | ~50K writes/sec |
| **Practical max** | ~10-20K writes/sec |

### Multi-Raft Scaling

With N shards, write throughput scales linearly:

| Shards | Theoretical Max | Practical Max |
|--------|-----------------|---------------|
| 1      | 50K/sec         | 10-20K/sec    |
| 4      | 200K/sec        | 40-80K/sec    |
| 16     | 800K/sec        | 160-320K/sec  |
| 64     | 3.2M/sec        | 640K-1.3M/sec |

## Quick Start

```rust
use distributed_cache::multiraft::{MultiRaftBuilder, MultiRaftConfig};

// Create coordinator with 16 shards
let coordinator = MultiRaftBuilder::new(node_id)
    .num_shards(16)
    .replica_factor(3)
    .shard_capacity(100_000)
    .build_and_init()
    .await?;

// Use like a regular cache
coordinator.put("user:123", "Alice").await?;
let value = coordinator.get(b"user:123").await?;

// Get statistics
let stats = coordinator.stats();
println!("Entries: {}", stats.total_entries);
println!("Ops/sec: {:.2}", stats.operations_per_sec);
```

## Components

### MultiRaftCoordinator

The main entry point for Multi-Raft operations:

```rust
use distributed_cache::multiraft::MultiRaftCoordinator;

// Create and initialize
let coordinator = MultiRaftCoordinator::new(node_id, config, metrics);
coordinator.init().await?;

// Cache operations
coordinator.put(key, value).await?;
coordinator.put_with_ttl(key, value, ttl).await?;
let value = coordinator.get(key).await?;
coordinator.delete(key).await?;

// Shard information
let shard_id = coordinator.shard_for_key(key);
let shard_info = coordinator.shard_info();

// Statistics
let stats = coordinator.stats();

// Shutdown
coordinator.shutdown().await?;
```

### ShardRouter

Routes requests to the appropriate shard:

```rust
use distributed_cache::multiraft::{ShardRouter, RouterConfig};

let config = RouterConfig::new(16);  // 16 shards
let router = ShardRouter::new(config);

// Get shard for key
let shard_id = router.shard_for_key(b"my-key");

// Register shards
router.register_shard(shard);

// Direct operations
router.put(key, value).await?;
let value = router.get(key).await?;
```

### Shard

A single partition with its own Raft group:

```rust
use distributed_cache::multiraft::{Shard, ShardConfig, ShardState};

// Create shard
let config = ShardConfig::new(0, 16);  // Shard 0 of 16
let shard = Shard::new(config).await?;

// Check ownership
if shard.owns_key(key_hash) {
    shard.put(key, value).await;
    let value = shard.get(key).await;
}

// Shard state
shard.set_state(ShardState::Active);
assert!(shard.is_active());

// Get info
let info = shard.info();
println!("Entries: {}", info.entry_count);
println!("Leader: {:?}", info.leader);
```

## Configuration

### MultiRaftConfig

| Option | Default | Description |
|--------|---------|-------------|
| `num_shards` | 16 | Number of shards (partitions) |
| `replica_factor` | 3 | Copies per shard |
| `shard_capacity` | 100,000 | Max entries per shard |
| `default_ttl` | None | Default TTL for entries |
| `auto_init_shards` | true | Create shards on init |
| `tick_interval` | 100ms | Background task interval |

### RouterConfig

| Option | Default | Description |
|--------|---------|-------------|
| `num_shards` | 16 | Number of shards |
| `hash_seed` | 0x5AFE... | Seed for consistent hashing |
| `cache_mappings` | true | Cache key→shard mappings |

### ShardConfig

| Option | Default | Description |
|--------|---------|-------------|
| `shard_id` | - | Unique shard identifier |
| `total_shards` | - | Total shard count |
| `replicas` | 3 | Replica factor |
| `max_capacity` | 100,000 | Max entries |
| `default_ttl` | None | Default TTL |

## Shard Assignment

Keys are assigned using consistent hashing:

```
shard_id = xxhash64(key) % num_shards
```

This ensures:
- **Deterministic**: Same key always maps to same shard
- **Even distribution**: Keys spread evenly across shards
- **Stable**: Key assignment doesn't change unless num_shards changes

### Example Distribution

With 4 shards:
```
Key "user:1"   → hash → shard 2
Key "user:2"   → hash → shard 0
Key "user:3"   → hash → shard 1
Key "user:4"   → hash → shard 3
Key "user:5"   → hash → shard 2
...
```

## Batch Operations

For efficiency, batch multiple keys:

```rust
use distributed_cache::multiraft::BatchRouter;

let batch_router = BatchRouter::new(router);

// Route multiple keys
let keys = vec![b"key1", b"key2", b"key3"];
let routing = batch_router.route_batch(&keys);

// routing: HashMap<ShardId, Vec<key_index>>
// {
//   0: [1],      // key2 goes to shard 0
//   1: [0, 2],   // key1, key3 go to shard 1
// }
```

## Shard States

| State | Description |
|-------|-------------|
| `Initializing` | Shard is starting up |
| `Active` | Serving requests normally |
| `Transferring` | Rebalancing data |
| `Removing` | Being removed from cluster |
| `Stopped` | Not serving requests |

## Leader Distribution

Different shards can have different leaders:

```
Node A: Leader of shards [0, 4, 8, 12]
Node B: Leader of shards [1, 5, 9, 13]
Node C: Leader of shards [2, 6, 10, 14]
Node D: Leader of shards [3, 7, 11, 15]
```

This distributes:
- CPU load (Raft processing)
- Network traffic (replication)
- Write latency (local writes are faster)

## Monitoring

```rust
let stats = coordinator.stats();

println!("Total shards: {}", stats.total_shards);
println!("Active shards: {}", stats.active_shards);
println!("Total entries: {}", stats.total_entries);
println!("Total size: {} bytes", stats.total_size_bytes);
println!("Local leader shards: {}", stats.local_leader_shards);
println!("Operations: {}", stats.operations_total);
println!("Ops/sec: {:.2}", stats.operations_per_sec);
```

Per-shard information:

```rust
for info in coordinator.shard_info() {
    println!("Shard {}: {} entries, leader={:?}, state={}",
        info.shard_id,
        info.entry_count,
        info.leader,
        info.state);
}
```

## Best Practices

### Choosing Number of Shards

| Cluster Size | Recommended Shards |
|--------------|-------------------|
| 3 nodes      | 8-16 shards       |
| 5 nodes      | 16-32 shards      |
| 10+ nodes    | 32-64 shards      |

Rules of thumb:
- More shards = better parallelism but more overhead
- Shards should be >= 2x node count for good distribution
- Powers of 2 are slightly more efficient

### Rebalancing Considerations

When the cluster changes:
- Adding nodes: New nodes can become leaders for some shards
- Removing nodes: Remaining nodes take over leadership
- Data doesn't need to move (shards remain on same key ranges)

### Memory Planning

```
total_memory = num_shards × shard_capacity × avg_entry_size
```

Example:
```
16 shards × 100,000 entries × 1KB = 1.6 GB per node
```

## Limitations

1. **Cross-shard transactions**: Not supported. Each shard is independent.

2. **Shard count is fixed**: Changing `num_shards` requires data migration.

3. **Hot spots**: If one key is very popular, its shard becomes a bottleneck.

4. **Memory overhead**: Each shard has its own Moka cache instance.

## Integration Example

```rust
use distributed_cache::multiraft::{MultiRaftBuilder, MultiRaftConfig};
use distributed_cache::metrics::CacheMetrics;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup metrics
    let metrics = Arc::new(CacheMetrics::new());

    // Create Multi-Raft coordinator
    let coordinator = MultiRaftBuilder::new(1)  // Node ID 1
        .num_shards(16)
        .replica_factor(3)
        .shard_capacity(100_000)
        .metrics(metrics.clone())
        .build_and_init()
        .await?;

    // Use the cache
    for i in 0..1000 {
        let key = format!("key-{}", i);
        let value = format!("value-{}", i);
        coordinator.put(key, value).await?;
    }

    // Check stats
    let stats = coordinator.stats();
    println!("Inserted {} entries across {} shards",
        stats.total_entries, stats.active_shards);

    // Graceful shutdown
    coordinator.shutdown().await?;

    Ok(())
}
```
