# Configuration Guide

This guide covers all configuration options available in Crema.

## Configuration Structure

```
CacheConfig
├── Node Identity
│   ├── node_id: NodeId
│   └── raft_addr: SocketAddr
├── Cache Settings
│   ├── max_capacity: u64
│   ├── default_ttl: Duration
│   └── time_to_idle: Option<Duration>
├── Cluster Settings
│   ├── seed_nodes: Vec<(NodeId, SocketAddr)>
│   └── cluster_discovery: Option<Box<dyn ClusterDiscovery>>
├── Raft Settings
│   └── raft_config: RaftConfig
├── Forwarding Settings
│   └── forwarding_config: ForwardingConfig
├── Checkpoint Settings
│   └── checkpoint_config: Option<CheckpointConfig>
└── Multi-Raft Settings
    └── multiraft_config: Option<MultiRaftCacheConfig>
```

For the full API reference, see the [main README](../../README.md).

## Core Configuration

### CacheConfig

The main configuration entry point.

```rust
use crema::{CacheConfig, NodeId};
use std::net::SocketAddr;
use std::time::Duration;

let config = CacheConfig::new(
    1,                                    // node_id
    "127.0.0.1:9000".parse().unwrap(),   // raft_addr
)
.with_max_capacity(100_000)               // Max cache entries
.with_default_ttl(Duration::from_secs(3600))  // Default TTL
.with_time_to_idle(Duration::from_secs(300))  // Optional TTI
.with_seed_nodes(vec![                    // Peer addresses
    "127.0.0.1:9001".parse().unwrap(),
    "127.0.0.1:9002".parse().unwrap(),
]);
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `node_id` | `NodeId` (u64) | Required | Unique node identifier |
| `raft_addr` | `SocketAddr` | Required | Address for Raft communication |
| `max_capacity` | `u64` | 100,000 | Maximum cache entries |
| `default_ttl` | `Duration` | 1 hour | Default time-to-live |
| `time_to_idle` | `Option<Duration>` | None | Time-to-idle eviction |
| `seed_nodes` | `Vec<SocketAddr>` | Empty | Initial peer addresses |

---

## Raft Configuration

### RaftConfig

Tune Raft consensus behavior.

```rust
use crema::RaftConfig;

let raft_config = RaftConfig::default()
    .with_election_timeout(Duration::from_millis(1000))
    .with_heartbeat_interval(Duration::from_millis(100))
    .with_pre_vote(true)
    .with_max_inflight_msgs(256)
    .with_max_size_per_msg(1024 * 1024);  // 1 MB

let config = CacheConfig::new(node_id, addr)
    .with_raft_config(raft_config);
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `election_timeout` | `Duration` | 1000ms | Time before election starts |
| `heartbeat_interval` | `Duration` | 100ms | Leader heartbeat frequency |
| `pre_vote` | `bool` | true | Enable pre-vote protocol |
| `max_inflight_msgs` | `usize` | 256 | Max in-flight Raft messages |
| `max_size_per_msg` | `u64` | 1 MB | Max Raft message size |
| `check_quorum` | `bool` | true | Check quorum before writes |

**Tuning Tips**:
- `election_timeout` should be 5-10x `heartbeat_interval`
- Lower `heartbeat_interval` = faster failure detection but more network traffic
- `pre_vote` prevents disruption from partitioned nodes

---

## Membership Configuration

### MembershipConfig

Control how cluster membership is managed.

```rust
use crema::{MembershipConfig, MembershipMode};

let membership_config = MembershipConfig::default()
    .with_mode(MembershipMode::Manual)
    .with_max_peer_count(10)
    .with_failure_confirmations(3);

let config = CacheConfig::new(node_id, addr)
    .with_membership_config(membership_config);
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mode` | `MembershipMode` | Manual | How membership changes are handled |
| `max_peer_count` | `usize` | 10 | Maximum cluster size |
| `failure_confirmations` | `usize` | 3 | Failures before marking dead |

### MembershipMode

```rust
pub enum MembershipMode {
    /// All membership changes require explicit API calls
    Manual,
    /// Nodes auto-discovered but require approval to join Raft
    SemiAutomatic,
    /// Fully automatic (DANGEROUS - can cause split-brain)
    Automatic,
}
```

**Recommendation**: Use `Manual` or `SemiAutomatic` for production.

---

## Request Forwarding Configuration

### ForwardingConfig

Control automatic request forwarding to the leader.

```rust
use crema::config::ForwardingConfig;

let config = CacheConfig::new(node_id, addr)
    .with_forwarding_enabled(true)
    .with_forwarding_timeout(Duration::from_secs(5))
    .with_forwarding_max_inflight(100);
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | true | Enable request forwarding |
| `timeout` | `Duration` | 5 seconds | Forward request timeout |
| `max_inflight` | `usize` | 100 | Max concurrent forwarded requests |

**When Disabled**: Write requests to followers return `Error::NotLeader` with a leader hint.

---

## Cluster Discovery Configuration

Crema uses a `ClusterDiscovery` trait to abstract cluster membership. Users create their own discovery implementation and pass it to the config.

### MemberlistDiscovery (Gossip-based)

```rust
use crema::{MemberlistConfig, MemberlistDiscovery, PeerManagementConfig};

let memberlist_config = MemberlistConfig {
    enabled: true,
    bind_addr: Some("127.0.0.1:8000".parse().unwrap()),
    advertise_addr: Some("192.168.1.10:8000".parse().unwrap()),  // Public address
    seed_addrs: vec![
        "192.168.1.11:8000".parse().unwrap(),
        "192.168.1.12:8000".parse().unwrap(),
    ],
    node_name: Some(format!("node-{}", node_id)),
    peer_management: PeerManagementConfig {
        auto_add_peers: true,      // Auto-register in Raft transport
        auto_remove_peers: false,
        auto_add_voters: false,    // Don't auto-add to Raft (safe default)
        auto_remove_voters: false, // Don't auto-remove from Raft
    },
};

// Create discovery and pass to config
let seed_nodes = vec![(2, "192.168.1.11:9000".parse().unwrap())];
let discovery = MemberlistDiscovery::new(node_id, raft_addr, &memberlist_config, &seed_nodes);

let config = CacheConfig::new(node_id, raft_addr)
    .with_seed_nodes(seed_nodes)
    .with_cluster_discovery(discovery);
```

### MemberlistConfig Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | false | Enable memberlist gossip |
| `bind_addr` | `Option<SocketAddr>` | None | Address for gossip |
| `advertise_addr` | `Option<SocketAddr>` | None | Public address (for NAT) |
| `seed_addrs` | `Vec<SocketAddr>` | Empty | Seed nodes for discovery |
| `node_name` | `Option<String>` | None | Human-readable node name |
| `peer_management.auto_add_peers` | `bool` | false | Auto-register discovered peers |
| `peer_management.auto_add_voters` | `bool` | false | Auto-add to Raft voting |
| `peer_management.auto_remove_voters` | `bool` | false | Auto-remove failed nodes |

**Safety Warning**: `auto_add_voters` and `auto_remove_voters` can cause split-brain during network partitions. Keep them `false` for production.

### Other Discovery Options

```rust
// StaticClusterDiscovery - Fixed IP list with health checks
let discovery = StaticClusterDiscovery::new(node_id, raft_addr, seed_nodes);

// NoOpClusterDiscovery - No-op for single-node or manual management
let discovery = NoOpClusterDiscovery::new(node_id, raft_addr);
```

---

## Checkpoint Configuration

### CheckpointConfig

Configure periodic snapshots for fast recovery.

```rust
use crema::CheckpointConfig;

let checkpoint_config = CheckpointConfig::new("./checkpoints")
    .with_log_threshold(10_000)           // Snapshot after 10K entries
    .with_time_interval(Duration::from_secs(3600))  // Or every hour
    .with_compression(true)               // LZ4 compression
    .with_max_snapshots(5);               // Keep last 5 snapshots

let config = CacheConfig::new(node_id, addr)
    .with_checkpoint_config(checkpoint_config);
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `directory` | `PathBuf` | Required | Snapshot storage directory |
| `log_threshold` | `u64` | 10,000 | Entries before snapshot |
| `time_interval` | `Option<Duration>` | None | Time-based snapshot trigger |
| `compression` | `bool` | true | Enable LZ4 compression |
| `max_snapshots` | `usize` | 5 | Max snapshots to retain |

---

## Multi-Raft Configuration

### MultiRaftCacheConfig

Configure horizontal scaling with multiple Raft groups.

```rust
use crema::MultiRaftCacheConfig;

let multiraft_config = MultiRaftCacheConfig::new()
    .with_num_shards(16)                  // 16 independent Raft groups
    .with_auto_init(true)                 // Auto-initialize shards
    .with_leader_broadcast_debounce(Duration::from_millis(100));

let config = CacheConfig::new(node_id, addr)
    .with_multiraft_config(multiraft_config);
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `num_shards` | `u32` | 1 | Number of Raft groups (1-64) |
| `auto_init` | `bool` | true | Auto-initialize shards on startup |
| `leader_broadcast_debounce` | `Duration` | 100ms | Debounce leader broadcasts |

**Shard Calculation**: `shard_id = xxhash64(key) % num_shards`

See [Multi-Raft Guide](./MULTIRAFT_GUIDE.md) for detailed setup.

---

## Complete Example

```rust
use crema::{
    CacheConfig, RaftConfig, MemberlistConfig, MemberlistDiscovery,
    PeerManagementConfig, CheckpointConfig, MultiRaftCacheConfig,
};
use std::time::Duration;

fn create_production_config(
    node_id: u64,
    raft_addr: &str,
    memberlist_addr: &str,
    seed_nodes: Vec<(u64, std::net::SocketAddr)>,
) -> CacheConfig {
    let raft_addr_parsed: std::net::SocketAddr = raft_addr.parse().unwrap();

    // Raft tuning
    let raft_config = RaftConfig {
        election_tick: 15,
        heartbeat_tick: 3,
        tick_interval_ms: 100,
        pre_vote: true,
        ..Default::default()
    };

    // Memberlist (gossip discovery)
    let memberlist_config = MemberlistConfig {
        enabled: true,
        bind_addr: Some(memberlist_addr.parse().unwrap()),
        advertise_addr: None,
        seed_addrs: vec![
            "10.0.0.1:8000".parse().unwrap(),
            "10.0.0.2:8000".parse().unwrap(),
            "10.0.0.3:8000".parse().unwrap(),
        ],
        node_name: Some(format!("node-{}", node_id)),
        peer_management: PeerManagementConfig {
            auto_add_peers: true,
            auto_remove_peers: false,
            auto_add_voters: false,  // Manual Raft approval
            auto_remove_voters: false,
        },
    };

    // Create discovery implementation
    let discovery = MemberlistDiscovery::new(
        node_id,
        raft_addr_parsed,
        &memberlist_config,
        &seed_nodes,
    );

    // Checkpointing
    let checkpoint_config = CheckpointConfig::new("/var/lib/crema/snapshots")
        .with_log_threshold(50_000)
        .with_time_interval(Duration::from_secs(1800))
        .with_compression(true)
        .with_max_snapshots(3);

    // Multi-Raft (for high write throughput)
    let multiraft_config = MultiRaftCacheConfig {
        enabled: true,
        num_shards: 8,
        shard_capacity: 100_000,
        auto_init_shards: true,
        leader_broadcast_debounce_ms: 200,
    };

    // Combine all configs
    CacheConfig::new(node_id, raft_addr_parsed)
        .with_seed_nodes(seed_nodes)
        .with_max_capacity(1_000_000)
        .with_default_ttl(Duration::from_secs(86400))
        .with_raft_config(raft_config)
        .with_cluster_discovery(discovery)
        .with_checkpoint_config(checkpoint_config)
        .with_multiraft_config(multiraft_config)
        .with_forwarding_enabled(true)
}
```

For more examples, see the [main README](../../README.md).

---

## Environment Variables

Crema uses `tracing` for logging. Control verbosity with:

```bash
# Info level
RUST_LOG=info cargo run

# Debug Raft messages
RUST_LOG=crema::consensus=debug cargo run

# Trace everything
RUST_LOG=trace cargo run
```

---

## Validation

Configuration is validated on `DistributedCache::new()`:

```rust
let cache = DistributedCache::new(config).await?;
// Returns Err if:
// - node_id is 0
// - raft_addr is invalid
// - num_shards > 64
// - election_timeout < heartbeat_interval
```

For early validation without creating the cache:

```rust
config.validate()?;  // Returns Result<()>
```
