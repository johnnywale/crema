# Crema

A strongly consistent distributed cache built on Raft consensus and Moka local cache.

## Features

- **Strong consistency** for writes via Raft consensus (linearizable)
- **Fast local reads** from Moka cache with TinyLFU eviction
- **Automatic TTL** and eviction policies
- **Two-tier cluster membership** (discovery + manual Raft control)
- **Multi-Raft scaling** for horizontal scalability with automatic shard routing
- **Memberlist integration** for gossip-based node discovery and health monitoring
- **Checkpointing** with LZ4 compression for fast recovery
- **Consistent hashing** for key distribution
- **Chaos testing framework** for resilience testing
- **Prometheus-style metrics** (counters, gauges, histograms)

## Requirements

- Rust 1.75.0 or later
- Tokio async runtime

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
crema = "0.0.1"
```

## Quick Start

### Single-Raft Mode (Default)

```rust
use crema::{CacheConfig, DistributedCache};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create configuration
    let config = CacheConfig::new(1, "127.0.0.1:9000".parse()?)
        .with_max_capacity(100_000)
        .with_default_ttl(Duration::from_secs(3600));

    // Create the distributed cache
    let cache = DistributedCache::new(config).await?;

    // Write operations go through Raft consensus
    cache.put("user:123", "Alice").await?;

    // Read operations are local (fast, but may be stale on followers)
    if let Some(value) = cache.get(b"user:123").await {
        println!("Found: {:?}", value);
    }

    // Delete
    cache.delete("user:123").await?;

    // Shutdown gracefully
    cache.shutdown().await;

    Ok(())
}
```

### Multi-Raft Mode (Sharded)

For high-throughput workloads, enable Multi-Raft mode to distribute writes across multiple shards:

```rust
use crema::{CacheConfig, DistributedCache, MultiRaftCacheConfig, MemberlistConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure Multi-Raft with memberlist for shard leader discovery
    let config = CacheConfig::new(1, "127.0.0.1:9000".parse()?)
        .with_max_capacity(100_000)
        .with_multiraft(MultiRaftCacheConfig {
            enabled: true,
            num_shards: 16,           // 16 independent Raft groups
            shard_capacity: 100_000,  // Max entries per shard
            auto_init_shards: true,   // Auto-create shards on startup
            leader_broadcast_debounce_ms: 200,
        })
        .with_memberlist(MemberlistConfig {
            enabled: true,  // Required for Multi-Raft
            bind_addr: Some("127.0.0.1:9100".parse()?),
            seed_addrs: vec!["127.0.0.1:9101".parse()?, "127.0.0.1:9102".parse()?],
            auto_add_peers: true,
            ..Default::default()
        });

    let cache = DistributedCache::new(config).await?;

    // Operations are automatically routed to the correct shard
    // Keys are assigned via: xxhash64(key) % num_shards
    cache.put("user:123", "Alice").await?;
    cache.put("user:456", "Bob").await?;

    // Check which shard owns a key
    if let Some(coordinator) = cache.multiraft_coordinator() {
        let shard_id = coordinator.shard_for_key(b"user:123");
        println!("Key 'user:123' is in shard {}", shard_id);

        // Get shard statistics
        let stats = coordinator.stats();
        println!("Total shards: {}", stats.total_shards);
        println!("Active shards: {}", stats.active_shards);
        println!("Local leader shards: {}", stats.local_leader_shards);
    }

    cache.shutdown().await;
    Ok(())
}
```

## Architecture

```
┌─────────────────────────────────────────────┐
│            Application Layer                 │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│          DistributedCache API               │
│  • get(key) -> Option<Value>                │
│  • put(key, value) -> Result<()>            │
│  • delete(key) -> Result<()>                │
└─────────────────────────────────────────────┘
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
┌──────────────────┐   ┌──────────────────┐
│   Single-Raft    │   │    Multi-Raft    │
│   (1 group)      │   │   (N shards)     │
└──────────────────┘   └──────────────────┘
          │                   │
    ┌─────┴─────┐       ┌─────┴─────┐
    ▼           ▼       ▼           ▼
┌───────┐ ┌─────────┐ ┌───────┐ ┌───────────┐
│ Raft  │ │  Moka   │ │Shards │ │Memberlist │
│Consen.│ │ Cache   │ │Router │ │  Gossip   │
└───────┘ └─────────┘ └───────┘ └───────────┘
```

### Module Structure

```
src/
├── cache/          # Main DistributedCache API, CacheRouter, and Moka storage
├── consensus/      # Raft node, state machine, storage, transport
├── cluster/        # Two-tier membership (discovery + manual Raft control)
├── network/        # TCP server, RPC messages, wire protocol
├── checkpoint/     # Snapshot writer/reader with LZ4 compression
├── partitioning/   # Consistent hash ring for key distribution
├── multiraft/      # Horizontal scaling via multiple Raft groups + migration
│   ├── coordinator.rs    # MultiRaftCoordinator - manages all shards
│   ├── shard.rs          # Shard - single partition with its own storage
│   ├── router.rs         # ShardRouter - routes keys to shards
│   └── memberlist_integration.rs  # Gossip-based shard leader tracking
├── metrics/        # Prometheus-style counters, gauges, histograms
└── testing/        # Chaos testing framework
```

## Multi-Raft Scaling

### Why Multi-Raft?

A single Raft group has practical limits of ~10-20K writes/sec due to:
- Network latency for replication
- Disk fsync for durability
- Leader bottleneck (all writes go through one node)

Multi-Raft solves this by partitioning the keyspace into N independent shards:

| Shards | Theoretical Max | Practical Max |
|--------|-----------------|---------------|
| 1      | 50K/sec         | 10-20K/sec    |
| 4      | 200K/sec        | 40-80K/sec    |
| 16     | 800K/sec        | 160-320K/sec  |
| 64     | 3.2M/sec        | 640K-1.3M/sec |

### Shard Assignment

Keys are assigned to shards using consistent hashing:

```
shard_id = xxhash64(key) % num_shards
```

This ensures:
- Same key always goes to same shard
- Keys are evenly distributed across shards
- Different shards can have different leaders (load distribution)

### Phase 1: Gossip-Based Routing

The current implementation uses memberlist gossip for shard leader discovery:

- **Eventually consistent** - leader info propagates via gossip
- **Best-effort routing** - shard leader info is a routing hint
- **Epoch-based versioning** - handles out-of-order gossip updates
- **Debounced broadcasts** - batches rapid leader changes (200ms default)

```rust
// Shard leader info is encoded in memberlist metadata
// Format: "shard_id:leader_id:epoch,..."

// When a node becomes leader of a shard:
coordinator.set_local_shard_leader(shard_id, node_id);  // Queues broadcast

// When receiving gossip about shard leaders:
coordinator.set_shard_leader_if_newer(shard_id, leader_id, epoch);  // Epoch check
```

### Multi-Raft API

```rust
use crema::{MultiRaftCoordinator, MultiRaftBuilder, MultiRaftStats, ShardInfo};

// Direct coordinator usage (for advanced use cases)
let coordinator = MultiRaftBuilder::new(node_id)
    .num_shards(16)
    .replica_factor(3)
    .shard_capacity(100_000)
    .default_ttl(Duration::from_secs(3600))
    .build_and_init()
    .await?;

// Cache operations
coordinator.put("key", "value").await?;
let value = coordinator.get(b"key").await?;
coordinator.delete(b"key").await?;

// Shard introspection
let shard_id = coordinator.shard_for_key(b"key");
let shard_info: Vec<ShardInfo> = coordinator.shard_info();
let stats: MultiRaftStats = coordinator.stats();

// Leader management
coordinator.set_shard_leader(shard_id, leader_node_id);
let leaders = coordinator.shard_leaders();  // HashMap<ShardId, Option<NodeId>>

// Invalidate leaders when a node fails
let invalidated = coordinator.invalidate_leader_for_node(failed_node_id);
```

### Shard Types

```rust
use crema::{Shard, ShardConfig, ShardId, ShardState, ShardInfo, ShardRange};

// ShardId is just u32
let shard_id: ShardId = 0;

// ShardConfig for creating shards
let config = ShardConfig::new(shard_id, total_shards)
    .with_replicas(3)
    .with_max_capacity(100_000)
    .with_default_ttl(Duration::from_secs(3600));

// ShardState tracks lifecycle
enum ShardState {
    Initializing,  // Starting up
    Active,        // Serving requests
    Transferring,  // Rebalancing data
    Removing,      // Being decommissioned
    Stopped,       // Shut down
}

// ShardInfo provides runtime statistics
struct ShardInfo {
    shard_id: ShardId,
    state: ShardState,
    leader: Option<NodeId>,
    members: Vec<NodeId>,
    entry_count: u64,
    size_bytes: u64,
    term: u64,
    commit_index: u64,
}

// ShardRange represents key ownership
let range = ShardRange { shard_id: 0, total_shards: 16 };
assert!(range.contains(key_hash));  // key_hash % 16 == 0
```

## Configuration

### CacheConfig

```rust
use crema::{CacheConfig, RaftConfig, MemberlistConfig, MultiRaftCacheConfig};
use std::time::Duration;

let config = CacheConfig::new(node_id, bind_addr)
    // Cache settings
    .with_max_capacity(100_000)
    .with_default_ttl(Duration::from_secs(3600))
    // Raft settings
    .with_raft_config(RaftConfig {
        election_tick: 10,
        heartbeat_tick: 3,
        ..Default::default()
    })
    // Memberlist for node discovery
    .with_memberlist(MemberlistConfig {
        enabled: true,
        bind_addr: Some("127.0.0.1:9100".parse()?),
        seed_addrs: vec!["127.0.0.1:9101".parse()?],
        auto_add_peers: true,
        ..Default::default()
    })
    // Multi-Raft for sharding (requires memberlist)
    .with_multiraft(MultiRaftCacheConfig {
        enabled: true,
        num_shards: 16,
        shard_capacity: 100_000,
        auto_init_shards: true,
        leader_broadcast_debounce_ms: 200,
    });
```

### MultiRaftCacheConfig

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Enable Multi-Raft mode |
| `num_shards` | `16` | Number of shards (max 64 in Phase 1) |
| `shard_capacity` | `100_000` | Max entries per shard |
| `auto_init_shards` | `true` | Auto-create shards on startup |
| `leader_broadcast_debounce_ms` | `200` | Debounce interval for leader broadcasts |

**Note:** Multi-Raft requires memberlist to be enabled. The cache will fail to start if `multiraft.enabled && !memberlist.enabled`.

## Consistency Model

| Operation | Single-Raft | Multi-Raft (Phase 1) |
|-----------|-------------|---------------------|
| Writes | Strongly consistent (linearizable) | Per-shard consistent |
| Reads | Locally consistent | Per-shard locally consistent |
| Leader reads | Strongly consistent | Per-shard leader reads |
| Cross-shard | N/A | No transaction support |
| Shard routing | N/A | Eventually consistent (gossip) |

## Running a Cluster

### Single Node

```bash
cargo run --example basic
```

### Multi-Node Cluster

```bash
# Terminal 1
RUST_LOG=info cargo run --example cluster -- 1

# Terminal 2
RUST_LOG=info cargo run --example cluster -- 2

# Terminal 3
RUST_LOG=info cargo run --example cluster -- 3
```

## Checkpointing

The cache supports periodic checkpointing for fast recovery:

```rust
use crema::{CheckpointConfig, CheckpointManager};

let config = CheckpointConfig::new("./checkpoints")
    .with_log_threshold(10_000)
    .with_compression(true);

// Snapshots are automatically created based on configured triggers
```

### Checkpoint Format

- Binary format with magic bytes "MCRS"
- Version and flags header
- Raft index/term metadata
- LZ4-compressed entries
- CRC32 checksum

## Wire Protocol

- 4-byte length prefix (big-endian u32)
- Bincode-encoded messages
- Protobuf for internal Raft messages
- Max message size: 16MB

## Development

```bash
# Build
cargo build

# Run tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Check code
cargo check

# Lint
cargo clippy

# Format
cargo fmt
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
