# Memberlist Integration

This guide explains how to use memberlist for gossip-based node discovery and cluster membership.

For the complete API reference, see the [main README](../../README.md).

## Overview

Memberlist provides:
- **Automatic node discovery**: Nodes find each other via gossip
- **Failure detection**: SWIM protocol detects node failures
- **Metadata propagation**: Share node metadata across cluster
- **Decentralized**: No single point of failure for discovery

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Two-Tier Membership                           │
│                                                                  │
│   ┌──────────────────────────────────────────────────────────┐  │
│   │  Layer 1: Memberlist (Gossip)                             │  │
│   │                                                           │  │
│   │  • Automatic node discovery                               │  │
│   │  • Failure detection via SWIM                             │  │
│   │  • Metadata propagation (Raft addresses)                  │  │
│   │  • Does NOT affect Raft voting                            │  │
│   └──────────────────────────────────────────────────────────┘  │
│                              │                                   │
│                    Manual Approval                               │
│                              │                                   │
│                              ▼                                   │
│   ┌──────────────────────────────────────────────────────────┐  │
│   │  Layer 2: Raft Membership                                 │  │
│   │                                                           │  │
│   │  • Explicit add_peer/remove_peer calls                    │  │
│   │  • Configuration changes via Raft consensus               │  │
│   │  • Quorum-based voting membership                         │  │
│   └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Why Two Tiers?

| Scenario | Gossip Only | Raft Only | Two-Tier |
|----------|-------------|-----------|----------|
| Node discovery | Automatic | Manual | Automatic |
| Split-brain risk | HIGH | Low | Low |
| Operator control | None | Full | Full |
| Recovery complexity | High | Low | Low |

**Problem with gossip-only**: During network partition, gossip might think nodes are alive on both sides, causing each partition to form its own cluster.

**Solution**: Use gossip for discovery, but require manual approval for Raft voting membership.

## Configuration

### Basic Setup

With the new API, users create their own `MemberlistDiscovery` and pass it to the config via `with_cluster_discovery()`:

```rust
use crema::{CacheConfig, DistributedCache, MemberlistConfig, MemberlistDiscovery, PeerManagementConfig};

let node_id = 1;
let raft_addr = "127.0.0.1:9001".parse()?;

// Configure memberlist
let memberlist_config = MemberlistConfig {
    enabled: true,
    bind_addr: Some("127.0.0.1:8001".parse()?),
    advertise_addr: None,
    seed_addrs: vec![
        "127.0.0.1:8002".parse()?,
        "127.0.0.1:8003".parse()?,
    ],
    node_name: Some(format!("node-{}", node_id)),
    peer_management: PeerManagementConfig {
        auto_add_peers: true,     // Auto-register in Raft transport
        auto_remove_peers: false,
        auto_add_voters: false,   // IMPORTANT: Don't auto-add to Raft
        auto_remove_voters: false, // IMPORTANT: Don't auto-remove from Raft
    },
};

// Define Raft peer addresses
let seed_nodes = vec![
    (2, "127.0.0.1:9002".parse()?),
    (3, "127.0.0.1:9003".parse()?),
];

// Create MemberlistDiscovery
let discovery = MemberlistDiscovery::new(node_id, raft_addr, &memberlist_config, &seed_nodes);

// Build config with discovery
let config = CacheConfig::new(node_id, raft_addr)
    .with_seed_nodes(seed_nodes)
    .with_cluster_discovery(discovery);

let cache = DistributedCache::new(config).await?;
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | `bool` | false | Enable memberlist gossip |
| `bind_addr` | `Option<SocketAddr>` | None | Address for gossip protocol |
| `advertise_addr` | `Option<SocketAddr>` | None | Public address (for NAT) |
| `seed_addrs` | `Vec<SocketAddr>` | Empty | Seed nodes for discovery |
| `node_name` | `Option<String>` | None | Human-readable node name |
| `peer_management.auto_add_peers` | `bool` | false | Auto-register discovered peers |
| `peer_management.auto_add_voters` | `bool` | false | Auto-add to Raft voting |
| `peer_management.auto_remove_voters` | `bool` | false | Auto-remove failed from Raft |

### Safety Defaults

The defaults are conservative for safety:

```rust
let peer_management = PeerManagementConfig {
    auto_add_peers: true,      // Safe: Only affects transport
    auto_remove_peers: false,
    auto_add_voters: false,    // IMPORTANT: Don't auto-add to Raft
    auto_remove_voters: false, // IMPORTANT: Don't auto-remove from Raft
};
```

**Never set `auto_add_voters` or `auto_remove_voters` to `true` in production** unless you understand the split-brain risks.

## Node Metadata

Each node broadcasts metadata via gossip:

```rust
use crema::cluster::memberlist_cluster::RaftNodeMetadata;

// Metadata is set automatically from CacheConfig
// Contains:
// - node_id: NodeId
// - raft_addr: SocketAddr
// - custom_metadata: HashMap<String, String>
```

### Custom Metadata

```rust
let memberlist_config = MemberlistConfig::new(bind_addr)
    .with_metadata("region", "us-east-1")
    .with_metadata("rack", "rack-1");
```

## API Usage

### Check Memberlist Status

```rust
// Check if memberlist is enabled
if cache.memberlist_enabled() {
    println!("Memberlist is active");
}
```

### Get Discovered Members

```rust
// All discovered members (alive, suspect, and dead)
let members = cache.memberlist_members().await;
for member in &members {
    println!("Node {}: {} (state: {:?})",
        member.node_id,
        member.raft_addr,
        member.state
    );
}

// Only healthy members
let healthy = cache.memberlist_healthy_members().await;
println!("Healthy nodes: {}", healthy.len());
```

### Member States

```rust
pub enum MemberState {
    Alive,   // Node is healthy
    Suspect, // Node might be failing
    Dead,    // Node is confirmed dead
    Left,    // Node gracefully left
}
```

### Listen for Membership Events

```rust
let mut event_rx = cache.memberlist_events().await;

tokio::spawn(async move {
    while let Some(event) = event_rx.recv().await {
        match event {
            MemberlistEvent::NodeJoin { node_id, raft_addr } => {
                println!("Node {} joined at {}", node_id, raft_addr);
                // Optionally: Add to Raft voting
                // cache.add_peer(node_id).await?;
            }
            MemberlistEvent::NodeLeave { node_id } => {
                println!("Node {} left gracefully", node_id);
            }
            MemberlistEvent::NodeFailed { node_id } => {
                println!("Node {} failed", node_id);
                // Optionally: Remove from Raft voting
                // cache.remove_peer(node_id).await?;
            }
            MemberlistEvent::NodeUpdate { node_id, new_addr } => {
                println!("Node {} updated address to {}", node_id, new_addr);
            }
        }
    }
});
```

## Multi-Raft Integration

For Multi-Raft mode, memberlist also broadcasts shard leader information:

```rust
use crema::cluster::memberlist_cluster::ShardLeaderInfo;

// Each node broadcasts which shards it leads
// This enables gossip-based leader discovery for routing
```

### Shard Leader Tracker

```rust
if let Some(coordinator) = cache.multiraft_coordinator() {
    let tracker = coordinator.shard_leader_tracker();

    // Get leader for a specific shard
    if let Some(leader_id) = tracker.get_leader(shard_id) {
        println!("Shard {} leader is node {}", shard_id, leader_id);
    }

    // Get all shard leaders
    let leaders = tracker.all_leaders();
    for (shard_id, leader_id) in leaders {
        println!("Shard {}: Leader {}", shard_id, leader_id);
    }
}
```

## Deployment Patterns

### Static Seed Nodes

Use a fixed set of well-known seed nodes:

```rust
let seeds = vec![
    "10.0.0.1:8000".parse()?,
    "10.0.0.2:8000".parse()?,
    "10.0.0.3:8000".parse()?,
];

let config = MemberlistConfig::new(bind_addr)
    .with_seeds(seeds);
```

### DNS-Based Seeds

Use DNS SRV records for dynamic seed discovery:

```rust
// Resolve seeds from DNS (application code)
let seeds = resolve_srv("_crema._tcp.cluster.local").await?;

let config = MemberlistConfig::new(bind_addr)
    .with_seeds(seeds);
```

### Kubernetes Headless Service

```yaml
# Kubernetes headless service for peer discovery
apiVersion: v1
kind: Service
metadata:
  name: crema-headless
spec:
  clusterIP: None
  selector:
    app: crema
  ports:
    - name: memberlist
      port: 8000
    - name: raft
      port: 9000
```

```rust
// Use headless service DNS for seeds
let seeds = vec!["crema-headless:8000".parse()?];
```

## NAT/Cloud Environments

When nodes are behind NAT or in cloud environments with internal/external addresses:

```rust
let config = MemberlistConfig::new(
    "0.0.0.0:8000".parse()?,  // Bind to all interfaces
)
.with_advertise_addr(
    "203.0.113.10:8000".parse()?,  // Public/external address
);
```

## Graceful Shutdown

Notify the cluster before shutting down:

```rust
// This sends a Leave message via memberlist
cache.shutdown().await?;

// Other nodes will receive NodeLeave event instead of NodeFailed
```

## Troubleshooting

### Nodes Not Discovering Each Other

**Symptom**: `memberlist_members()` returns only the local node.

**Possible causes**:
1. Firewall blocking gossip port
2. Seeds unreachable
3. Different cluster keys

**Solution**:
```bash
# Check connectivity
nc -zv seed-node 8000

# Verify ports are open
netstat -tlnp | grep 8000
```

### Excessive Failure Detection

**Symptom**: Nodes frequently marked as failed then recovered.

**Possible causes**:
1. Network instability
2. Gossip interval too short
3. High system load

**Solution**: Tune gossip parameters (if exposed in config).

### Split-Brain After Partition Heal

**Symptom**: Two separate clusters formed during partition, both think they're the main cluster.

**Solution**: This is why `auto_add_voters` should be `false`. Manually reconcile Raft membership:

```rust
// On the surviving partition (one with quorum)
// Add back nodes from the other partition
for node_id in rejoining_nodes {
    cache.add_peer(node_id).await?;
}
```

## Best Practices

1. **Use odd seed count**: 3 or 5 seeds for better availability
2. **Don't auto-add voters**: Keep Raft membership manual
3. **Monitor member states**: Alert on excessive Suspect/Dead transitions
4. **Use graceful shutdown**: Call `shutdown()` before stopping nodes
5. **Separate ports**: Use different ports for memberlist and Raft
6. **Configure advertise address**: Essential for NAT/cloud environments
