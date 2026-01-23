# Multi-Raft Guide

This guide explains how to use Multi-Raft mode for horizontal scaling beyond the limits of a single Raft group.

## Why Multi-Raft?

A single Raft group has inherent throughput limits:

| Bottleneck | Impact |
|------------|--------|
| Leader serialization | All writes go through one leader |
| Log replication | Every write replicated to all followers |
| Network bandwidth | Leader sends to N-1 followers |

**Practical limit**: 10-20K writes/sec per Raft group

Multi-Raft solves this by running multiple independent Raft groups (shards), each with its own leader.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   MultiRaftCoordinator                           │
│                                                                  │
│   Key Routing:  shard_id = xxhash64(key) % num_shards           │
│                                                                  │
│   ┌──────────────────────────────────────────────────────────┐  │
│   │                    ShardRouter                            │  │
│   │  key:"user:1" → shard 3 → Node 2 (leader)                │  │
│   │  key:"user:2" → shard 7 → Node 1 (leader)                │  │
│   │  key:"user:3" → shard 1 → Node 3 (leader)                │  │
│   └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│   │ Shard 0  │  │ Shard 1  │  │ Shard 2  │  │ Shard N  │       │
│   │  Raft    │  │  Raft    │  │  Raft    │  │  Raft    │       │
│   │ Leader:1 │  │ Leader:3 │  │ Leader:2 │  │ Leader:1 │       │
│   └──────────┘  └──────────┘  └──────────┘  └──────────┘       │
└─────────────────────────────────────────────────────────────────┘
```

With N shards distributed across M nodes:
- Each node leads ~N/M shards
- Write throughput scales linearly with shard count
- Each shard maintains independent Raft consensus

## Configuration

### Basic Setup

```rust
use crema::{CacheConfig, MultiRaftCacheConfig, MemberlistConfig};

let multiraft_config = MultiRaftCacheConfig::new()
    .with_num_shards(16)    // 16 independent Raft groups
    .with_auto_init(true);  // Auto-initialize shards on startup

let memberlist_config = MemberlistConfig::new(
    "127.0.0.1:8001".parse()?,
)
.with_seeds(vec![
    "127.0.0.1:8001".parse()?,
    "127.0.0.1:8002".parse()?,
    "127.0.0.1:8003".parse()?,
]);

let config = CacheConfig::new(1, "127.0.0.1:9001".parse()?)
    .with_memberlist(memberlist_config)
    .with_multiraft_config(multiraft_config);

let cache = DistributedCache::new(config).await?;
```

### Choosing Shard Count

| Cluster Size | Recommended Shards | Notes |
|--------------|-------------------|-------|
| 3 nodes | 8-16 | Good distribution |
| 5 nodes | 16-32 | Better parallelism |
| 7+ nodes | 32-64 | Maximum scaling |

**Rule of thumb**: `num_shards = 2-4x node_count`

More shards = better distribution but more overhead. The system supports up to 64 shards.

## API Usage

### Standard Operations (Transparent Routing)

Multi-Raft is transparent to basic operations:

```rust
// These work exactly the same as single-Raft mode
cache.put("user:123", "Alice").await?;
let value = cache.get(b"user:123").await;
cache.delete("user:123").await?;
```

The `ShardRouter` automatically routes each key to the correct shard.

### Querying Shard Information

```rust
// Check if Multi-Raft is enabled
if cache.is_multiraft_enabled() {
    // Get the shard for a specific key
    let shard_id = cache.shard_for_key(b"user:123");
    println!("user:123 is in shard {}", shard_id);
}

// Get the Multi-Raft coordinator
if let Some(coordinator) = cache.multiraft_coordinator() {
    // Get coordinator statistics
    let stats = coordinator.stats().await;
    println!("Total shards: {}", stats.total_shards);
    println!("Active shards: {}", stats.active_shards);
}
```

### Coordinator Statistics

```rust
let stats = coordinator.stats().await;

// Overall statistics
println!("Total shards: {}", stats.total_shards);
println!("Active shards: {}", stats.active_shards);
println!("Migrating shards: {}", stats.migrating_shards);

// Per-shard information
for (shard_id, shard_stats) in &stats.shard_stats {
    println!("Shard {}: leader={:?}, entries={}",
        shard_id,
        shard_stats.leader_id,
        shard_stats.entry_count
    );
}
```

## Shard Migration

When nodes join or leave the cluster, shards are automatically rebalanced.

### Migration Phases

```
1. Planned        - Migration planned, no action yet
2. DualWrite      - Writes go to both old and new locations
3. Streaming      - Data being transferred to new node
4. CatchingUp     - Final writes being synchronized
5. Committing     - Ownership handover (atomic via Raft)
6. Cleanup        - Old data being removed
7. Complete       - Migration finished
```

### Monitoring Migrations

```rust
let coordinator = cache.multiraft_coordinator().unwrap();

// Get active migrations
let migrations = coordinator.active_migrations().await;
for migration in migrations {
    println!("Shard {} migrating: {} -> {}",
        migration.shard_id,
        migration.from_node,
        migration.to_node
    );
    println!("  Phase: {:?}", migration.phase);
    println!("  Progress: {}%", migration.progress.percent_complete());
}
```

### Migration Configuration

```rust
use crema::MigrationConfig;

let migration_config = MigrationConfig::new()
    .with_max_concurrent_migrations(2)    // Max parallel migrations
    .with_batch_size(1000)                // Entries per batch
    .with_rate_limit_bytes_per_sec(10_000_000);  // 10 MB/s

let multiraft_config = MultiRaftCacheConfig::new()
    .with_num_shards(16)
    .with_migration_config(migration_config);
```

## Leader Distribution

Shard leaders are distributed across nodes for load balancing:

```
3-node cluster with 9 shards:

Node 1: Leader for shards [0, 3, 6]
Node 2: Leader for shards [1, 4, 7]
Node 3: Leader for shards [2, 5, 8]
```

### Leader Discovery

Leaders are discovered via memberlist gossip:

1. Each node broadcasts which shards it leads
2. `ShardLeaderTracker` maintains a routing table
3. Routing table updates are eventually consistent

```rust
// Get the leader for a specific shard
let leader_tracker = coordinator.shard_leader_tracker();
if let Some(leader_id) = leader_tracker.get_leader(shard_id) {
    println!("Shard {} leader is node {}", shard_id, leader_id);
}
```

## Best Practices

### 1. Use Odd Node Counts

For Raft quorum, use odd numbers: 3, 5, 7 nodes.

| Nodes | Quorum | Fault Tolerance |
|-------|--------|-----------------|
| 3 | 2 | 1 node failure |
| 5 | 3 | 2 node failures |
| 7 | 4 | 3 node failures |

### 2. Choose Appropriate Shard Count

- Too few shards → Uneven distribution
- Too many shards → Overhead from coordination

Start with `2x node_count` and adjust based on workload.

### 3. Monitor Shard Balance

```rust
let stats = coordinator.stats().await;
let leaders_per_node = stats.leaders_per_node();

for (node_id, count) in leaders_per_node {
    println!("Node {} leads {} shards", node_id, count);
}
```

### 4. Handle Leader Changes

Shard leaders can change due to:
- Node failures
- Network partitions
- Manual rebalancing

The router handles this transparently, but expect brief latency spikes during transitions.

### 5. Key Distribution

Keys are distributed by hash. Ensure your key scheme doesn't cluster:

```rust
// Good: Distributed keys
"user:1", "user:2", "user:3"

// Bad: Sequential IDs might cluster
"item:1000000", "item:1000001", "item:1000002"
```

## Limitations

| Limitation | Description |
|------------|-------------|
| Max 64 shards | Hard limit in current implementation |
| No cross-shard transactions | Each shard is independent |
| Eventual leader discovery | Brief routing delays on topology changes |
| Fixed shard count | Cannot change after cluster creation |

## Troubleshooting

### Uneven Shard Distribution

**Symptom**: Some nodes have many more leaders than others.

**Solution**:
1. Check that all nodes have the same shard configuration
2. Verify memberlist connectivity between all nodes
3. Wait for automatic rebalancing to complete

### High Latency on Writes

**Symptom**: Write latency is higher than expected.

**Possible causes**:
1. Routing to wrong node (leader discovery lag)
2. Migration in progress
3. Network issues between nodes

**Solution**:
1. Check `coordinator.stats()` for migration status
2. Verify network connectivity
3. Consider enabling request forwarding

### Split-Brain During Partition

**Symptom**: Different nodes report different leaders for the same shard.

**Solution**: This is expected during partitions. Raft ensures only the partition with quorum can accept writes. Wait for partition to heal.
