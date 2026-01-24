# Crema

A strongly consistent distributed cache built on Raft consensus and Moka local cache.

## Features

### Core Features
- **Strong consistency** for writes via Raft consensus (linearizable)
- **Linearizable reads** via Read-Index protocol (`consistent_get()`)
- **Fast local reads** from Moka cache with TinyLFU eviction
- **Automatic TTL** and eviction policies
- **Request forwarding** - automatic forwarding of writes to Raft leader
- **Graceful shutdown** with proper state cleanup

### Cluster Management
- **Two-tier cluster membership** (discovery + manual Raft control)
- **Memberlist integration** for gossip-based node discovery and health monitoring
- **Automatic peer discovery** via seed nodes

### Multi-Raft Scaling
- **Multi-Raft architecture** for horizontal scalability
- **Automatic shard routing** with consistent hashing
- **Shard migration** with zero-downtime rebalancing
- **Per-shard leadership** for distributed write load

### Persistence & Recovery
- **Pluggable storage backends** - Memory (default) or RocksDB (persistent)
- **Checkpointing** with LZ4 compression for fast recovery
- **Crash recovery** from snapshots and Raft log replay
- **CRC32 checksums** for data integrity

### Observability
- **Prometheus-style metrics** (counters, gauges, histograms)
- **Detailed shard statistics** (entries, ops/sec, leadership)
- **Migration progress tracking**

### Testing
- **Chaos testing framework** for resilience testing
- **Comprehensive test suite** with 471+ tests

## Requirements

- Rust 1.75.0 or later
- Tokio async runtime
- (Optional) RocksDB for persistent storage

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
crema = "0.1.0"

# For persistent storage (optional)
crema = { version = "0.1.0", features = ["rocksdb"] }
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

    // Write operations go through Raft consensus (linearizable)
    cache.put("user:123", "Alice").await?;

    // Fast local read (may be stale on followers)
    if let Some(value) = cache.get(b"user:123").await {
        println!("Found: {:?}", value);
    }

    // Strongly consistent read (always reads from leader)
    if let Some(value) = cache.consistent_get(b"user:123").await? {
        println!("Consistent read: {:?}", value);
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
use crema::{
    CacheConfig, DistributedCache, MemberlistConfig, MemberlistDiscovery,
    MultiRaftCacheConfig, PeerManagementConfig,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node_id = 1;
    let raft_addr = "127.0.0.1:9000".parse()?;
    let memberlist_addr = "127.0.0.1:9100".parse()?;

    // Configure memberlist for gossip-based discovery
    let memberlist_config = MemberlistConfig {
        enabled: true,
        bind_addr: Some(memberlist_addr),
        advertise_addr: None,
        seed_addrs: vec!["127.0.0.1:9101".parse()?, "127.0.0.1:9102".parse()?],
        node_name: Some(format!("node-{}", node_id)),
        peer_management: PeerManagementConfig {
            auto_add_peers: true,
            auto_remove_peers: false,
            auto_add_voters: false,
            auto_remove_voters: false,
        },
    };

    // Create MemberlistDiscovery - users create their own discovery implementation
    let raft_peers = vec![
        (2, "127.0.0.1:9001".parse()?),
        (3, "127.0.0.1:9002".parse()?),
    ];
    let discovery = MemberlistDiscovery::new(node_id, raft_addr, &memberlist_config, &raft_peers);

    // Configure Multi-Raft with memberlist for shard leader discovery
    let config = CacheConfig::new(node_id, raft_addr)
        .with_seed_nodes(raft_peers)
        .with_max_capacity(100_000)
        .with_cluster_discovery(discovery)  // Pass the discovery implementation
        .with_multiraft_config(MultiRaftCacheConfig {
            enabled: true,
            num_shards: 16,           // 16 independent Raft groups
            shard_capacity: 100_000,  // Max entries per shard
            auto_init_shards: true,   // Auto-create shards on startup
            leader_broadcast_debounce_ms: 200,
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
┌────────────────────────────────────────────────────────────────┐
│                        Client Layer                            │
├────────────────────────────────────────────────────────────────┤
│                     Request Router                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ Key → Shard  │  │   Forward    │  │    MOVED     │          │
│  │   Mapping    │  │   to Leader  │  │   Response   │          │
│  │  (versioned) │  │              │  │              │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
├────────────────────────────────────────────────────────────────┤
│              Multi-Raft Coordinator (Control Plane)            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │    Shard     │  │  Migration   │  │    Raft      │          │
│  │   Registry   │  │ Orchestrator │  │  Lifecycle   │          │
│  │ (shard→raft) │  │(ownership Δ) │  │  (start/stop)│          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│  Note: Does NOT participate in normal read/write data path     │
├────────────────────────────────────────────────────────────────┤
│                    Raft Groups (per Shard)                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Shard N Raft Group                                     │   │
│  │  ┌─────────────┐  ┌─────────────────────────────────┐   │   │
│  │  │ Raft Core   │  │      Apply Pipeline             │   │   │
│  │  │ - HardState │  │  ┌─────────────────────────┐    │   │   │
│  │  │ - Log/WAL   │  │  │   State Machine (Moka)  │    │   │   │
│  │  │ - Conf      │  │  │   + Checkpoint Snapshots│    │   │   │
│  │  └─────────────┘  │  └─────────────────────────┘    │   │   │
│  │                   └─────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────┘   │
├────────────────────────────────────────────────────────────────┤
│                     Storage Layer                              │
│  ┌─────────────────────────────────┐  ┌────────────────────┐   │
│  │     Raft Consensus Storage      │  │  State Machine     │   │
│  │  ┌───────────┐ ┌─────────────┐  │  │  ┌──────────────┐  │   │
│  │  │ HardState │ │  Log/WAL    │  │  │  │  Moka Cache  │  │   │
│  │  │ ConfState │ │  (entries)  │  │  │  │  (KV data)   │  │   │
│  │  └───────────┘ └─────────────┘  │  │  └──────────────┘  │   │
│  │  ┌─────────────────────────────┐│  │  ┌──────────────┐  │   │
│  │  │  RocksDB (optional feature) ││  │  │  Checkpoint  │  │   │
│  │  │  OR MemStorage (default)    ││  │  │  (LZ4+CRC32) │  │   │
│  │  └─────────────────────────────┘│  │  └──────────────┘  │   │
│  └─────────────────────────────────┘  └────────────────────┘   │
├────────────────────────────────────────────────────────────────┤
│                     Network Layer                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │     TCP      │  │ Raft Messages│  │   Memberlist │          │
│  │   (Client)   │  │   (Peers)    │  │   (Gossip)   │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└────────────────────────────────────────────────────────────────┘
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
│   ├── migration.rs      # Shard migration for rebalancing
│   └── memberlist_integration.rs  # Gossip-based shard leader tracking
├── metrics/        # Prometheus-style counters, gauges, histograms
└── testing/        # Chaos testing framework
```

## Storage Configuration

Crema supports two storage backends for Raft log persistence:

### Memory Storage (Default)

In-memory storage is the default and requires no additional configuration. Data is lost on restart but provides the fastest performance.

```rust
use crema::{CacheConfig, RaftStorageType};

let config = CacheConfig::new(1, "127.0.0.1:9000".parse()?)
    .with_raft_storage_type(RaftStorageType::Memory);  // Default
```

**Use cases:**
- Development and testing
- Ephemeral caching where data loss is acceptable
- Maximum performance scenarios

### RocksDB Storage (Persistent)

For production deployments requiring durability, enable RocksDB storage:

```toml
# Cargo.toml
[dependencies]
crema = { version = "0.1.0", features = ["rocksdb"] }
```

```rust
use crema::{CacheConfig, RaftStorageType};

let config = CacheConfig::new(1, "127.0.0.1:9000".parse()?)
    .with_raft_storage_type(RaftStorageType::RocksDb)
    .with_data_dir("./data/node1");  // Required for RocksDB
```

**Use cases:**
- Production deployments requiring durability
- Crash recovery without full cluster rebuild
- Compliance requirements for data persistence

### Storage Comparison

| Feature | Memory | RocksDB |
|---------|--------|---------|
| Persistence | No | Yes |
| Recovery on restart | Full replay required | Instant |
| Write latency | ~1ms | ~2-5ms |
| Feature flag | None | `rocksdb` |
| Disk usage | None | Proportional to data |

## Consistency Model

### Read Operations

| API | Consistency | Performance | Use Case |
|-----|-------------|-------------|----------|
| `get()` | Eventually consistent | Fastest (local) | High-throughput reads where staleness is acceptable |
| `consistent_get()` | Linearizable | Slower (leader roundtrip) | When freshness is critical |

### Write Operations

All writes (`put()`, `delete()`) go through Raft consensus and are linearizable.

```rust
// Fast local read - may return stale data on followers
let value = cache.get(b"key").await;

// Strongly consistent read - always reads latest committed value
let value = cache.consistent_get(b"key").await?;

// Linearizable write
cache.put("key", "value").await?;
```

### Request Forwarding

When a write request arrives at a follower node, it is automatically forwarded to the current Raft leader:

```
Client → Follower → Leader (Raft Consensus) → Response → Client
```

This provides transparent handling of writes regardless of which node receives the request.

### Consistency Summary

| Operation | Single-Raft | Multi-Raft |
|-----------|-------------|------------|
| Writes | Strongly consistent (linearizable) | Per-shard consistent |
| `get()` | Locally consistent | Per-shard locally consistent |
| `consistent_get()` | Strongly consistent | Per-shard leader reads |
| Cross-shard | N/A | No transaction support |
| Shard routing | N/A | Eventually consistent (gossip) |

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

### Gossip-Based Routing

The implementation uses memberlist gossip for shard leader discovery:

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
use crema::{
    CacheConfig, RaftConfig, MemberlistConfig, MemberlistDiscovery,
    MultiRaftCacheConfig, PeerManagementConfig, RaftStorageType,
};
use std::time::Duration;

let node_id = 1;
let raft_addr = "127.0.0.1:9000".parse()?;

// Configure memberlist for gossip-based discovery
let memberlist_config = MemberlistConfig {
    enabled: true,
    bind_addr: Some("127.0.0.1:9100".parse()?),
    advertise_addr: None,
    seed_addrs: vec!["127.0.0.1:9101".parse()?],
    node_name: Some(format!("node-{}", node_id)),
    peer_management: PeerManagementConfig {
        auto_add_peers: true,
        auto_remove_peers: false,
        auto_add_voters: false,
        auto_remove_voters: false,
    },
};

// Create MemberlistDiscovery - users create their own discovery implementation
let raft_peers = vec![(2, "127.0.0.1:9001".parse()?)];
let discovery = MemberlistDiscovery::new(node_id, raft_addr, &memberlist_config, &raft_peers);

let config = CacheConfig::new(node_id, raft_addr)
    // Cache settings
    .with_max_capacity(100_000)
    .with_default_ttl(Duration::from_secs(3600))

    // Storage backend
    .with_raft_storage_type(RaftStorageType::RocksDb)  // or Memory (default)
    .with_data_dir("./data/node1")  // Required for RocksDB

    // Raft settings
    .with_raft_config(RaftConfig {
        election_tick: 10,
        heartbeat_tick: 3,
        ..Default::default()
    })

    // Cluster discovery - pass your own ClusterDiscovery implementation
    .with_cluster_discovery(discovery)

    // Multi-Raft for sharding (requires cluster discovery)
    .with_multiraft_config(MultiRaftCacheConfig {
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
| `num_shards` | `16` | Number of shards (max 64) |
| `shard_capacity` | `100_000` | Max entries per shard |
| `auto_init_shards` | `true` | Auto-create shards on startup |
| `leader_broadcast_debounce_ms` | `200` | Debounce interval for leader broadcasts |

**Note:** Multi-Raft requires a `ClusterDiscovery` implementation (like `MemberlistDiscovery`). The cache will fail to start if Multi-Raft is enabled without cluster discovery.

### ClusterDiscovery

Crema uses a `ClusterDiscovery` trait to abstract cluster membership protocols. Users create their own discovery implementation and pass it to the config:

```rust
// Available implementations:
// - MemberlistDiscovery: Gossip-based discovery using SWIM protocol
// - StaticClusterDiscovery: Fixed IP list with health checks
// - NoOpClusterDiscovery: No-op for single-node or manual management

let discovery = MemberlistDiscovery::new(node_id, raft_addr, &memberlist_config, &seed_nodes);
let config = CacheConfig::new(node_id, raft_addr)
    .with_cluster_discovery(discovery);
```

### MemberlistConfig

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Enable memberlist gossip |
| `bind_addr` | `None` | Address for gossip protocol (UDP + TCP) |
| `seed_addrs` | `[]` | Initial nodes to contact for discovery |
| `node_name` | `None` | Human-readable node name |
| `peer_management.auto_add_peers` | `false` | Automatically add discovered nodes as Raft peers |
| `peer_management.auto_add_voters` | `false` | Automatically add discovered nodes as Raft voters |

## Checkpointing

The cache supports periodic checkpointing for fast recovery:

```rust
use crema::{CheckpointConfig, CheckpointManager};

let config = CheckpointConfig::new("./checkpoints")
    .with_log_threshold(10_000)   // Checkpoint after 10K log entries
    .with_compression(true);       // Enable LZ4 compression

// Snapshots are automatically created based on configured triggers
```

### Checkpoint Format

- Binary format with magic bytes "MCRS"
- Version and flags header
- Raft index/term metadata
- LZ4-compressed entries
- CRC32 checksum for integrity verification

### Recovery Process

On startup, the cache:
1. Loads the latest valid checkpoint (if available)
2. Verifies CRC32 checksum
3. Restores state machine to checkpoint state
4. Replays Raft log entries after checkpoint index
5. Joins the cluster and catches up with leader

## Metrics

Crema provides Prometheus-style metrics for monitoring:

```rust
use crema::metrics::{MetricsRegistry, Counter, Gauge, Histogram};

// Access the metrics registry
let metrics = cache.metrics();

// Available metrics:
// - cache_hits_total (Counter)
// - cache_misses_total (Counter)
// - cache_entries (Gauge)
// - cache_size_bytes (Gauge)
// - raft_proposals_total (Counter)
// - raft_proposal_latency_seconds (Histogram)
// - raft_leader_changes_total (Counter)
// - memberlist_nodes (Gauge)
// - shard_entries (Gauge, per-shard)
// - shard_leader_changes_total (Counter)
// - migration_duration_seconds (Histogram)
```

### Shard Statistics

```rust
if let Some(coordinator) = cache.multiraft_coordinator() {
    let stats = coordinator.stats();

    println!("Total shards: {}", stats.total_shards);
    println!("Active shards: {}", stats.active_shards);
    println!("Local leader shards: {}", stats.local_leader_shards);
    println!("Total entries: {}", stats.total_entries);
    println!("Operations/sec: {:.2}", stats.operations_per_sec);
}
```

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

### Production Deployment

For production deployments, consider:

1. **Use RocksDB storage** for durability:
   ```rust
   .with_raft_storage_type(RaftStorageType::RocksDb)
   .with_data_dir("/var/lib/crema/node1")
   ```

2. **Configure appropriate timeouts** for your network:
   ```rust
   .with_raft_config(RaftConfig {
       election_tick: 10,      // 10 * tick_interval
       heartbeat_tick: 3,      // 3 * tick_interval
       ..Default::default()
   })
   ```

3. **Enable cluster discovery** for automatic peer discovery:
   ```rust
   let memberlist_config = MemberlistConfig {
       enabled: true,
       bind_addr: Some("0.0.0.0:9100".parse()?),
       seed_addrs: vec![...],
       ..Default::default()
   };
   let discovery = MemberlistDiscovery::new(node_id, raft_addr, &memberlist_config, &seed_nodes);
   // ...
   .with_cluster_discovery(discovery)
   ```

4. **Set appropriate shard count** based on expected write throughput:
   - Start with 16 shards for most workloads
   - Scale to 64 shards for high-throughput scenarios

5. **Monitor metrics** for cluster health and performance.

## Wire Protocol

- 4-byte length prefix (big-endian u32)
- Bincode-encoded messages
- Protobuf for internal Raft messages
- Max message size: 16MB

## Feature Flags

| Feature | Description |
|---------|-------------|
| `default` | Memory storage + memberlist gossip |
| `memberlist` | Enable memberlist-based cluster discovery (default) |
| `rocksdb` | Enable RocksDB persistent storage |
| `full` | All features including RocksDB and memberlist |

```toml
# Default (with memberlist)
crema = "0.1.0"

# Without memberlist (minimal dependencies)
crema = { version = "0.1.0", default-features = false, features = ["tokio"] }

# With RocksDB
crema = { version = "0.1.0", features = ["rocksdb"] }

# All features
crema = { version = "0.1.0", features = ["full"] }
```

### Cluster Discovery Abstraction

Crema uses a `ClusterDiscovery` trait to abstract cluster membership protocols. Users create their own discovery implementation and pass it to the config via `with_cluster_discovery()`:

```rust
use crema::{
    ClusterDiscovery, MemberlistDiscovery, StaticClusterDiscovery,
    NoOpClusterDiscovery, MemberlistConfig,
};

// Available implementations:

// 1. MemberlistDiscovery - Gossip-based discovery using SWIM protocol
let discovery = MemberlistDiscovery::new(node_id, raft_addr, &memberlist_config, &seed_nodes);

// 2. StaticClusterDiscovery - Fixed IP list with health checks
let discovery = StaticClusterDiscovery::new(node_id, raft_addr, seed_nodes);

// 3. NoOpClusterDiscovery - No-op for single-node or manual management
let discovery = NoOpClusterDiscovery::new(node_id, raft_addr);

// Pass to config
let config = CacheConfig::new(node_id, raft_addr)
    .with_cluster_discovery(discovery);
```

When no discovery is provided, the cache defaults to `NoOpClusterDiscovery`. This is useful for:
- Single-node deployments
- Manual peer management
- Custom discovery implementations

## Development

```bash
# Build
cargo build

# Build with all features
cargo build --all-features

# Run tests
cargo test

# Run tests with all features
cargo test --all-features

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
