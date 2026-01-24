# Architecture Overview

This document describes the system architecture of Crema, a strongly consistent distributed cache.

For the complete API reference, see the [main README](../../README.md).

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Application Layer                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   DistributedCache API                           │
│  • get(key) -> Option<Value>                                     │
│  • put(key, value) -> Result<()>                                 │
│  • delete(key) -> Result<()>                                     │
│  • put_with_ttl(key, value, ttl) -> Result<()>                  │
└─────────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│   Memberlist    │   │  Raft Consensus │   │   Moka Cache    │
│    (Gossip)     │   │    (raft-rs)    │   │   (In-Memory)   │
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

## Module Structure

```
src/
├── cache/           # Main DistributedCache API
│   ├── mod.rs       # DistributedCache implementation
│   └── router.rs    # Single/Multi-Raft routing abstraction
│
├── consensus/       # Raft consensus layer
│   ├── mod.rs       # RaftNode implementation
│   ├── state_machine.rs  # CacheStateMachine
│   ├── storage.rs   # MemStorage (Raft log)
│   ├── transport.rs # RaftTransport (peer messaging)
│   ├── flow_control.rs   # Backpressure handling
│   └── health_check.rs   # Node health monitoring
│
├── cluster/         # Cluster membership
│   ├── mod.rs       # ClusterMembership (two-tier)
│   └── memberlist_cluster.rs  # Gossip integration
│
├── network/         # TCP network layer
│   ├── mod.rs       # NetworkServer
│   └── messages.rs  # RPC message types
│
├── checkpoint/      # Snapshot system
│   ├── mod.rs       # CheckpointManager
│   ├── writer.rs    # SnapshotWriter (LZ4 + CRC32)
│   └── reader.rs    # SnapshotReader
│
├── partitioning/    # Consistent hashing
│   ├── mod.rs       # HashRing (256 vnodes/node)
│   └── ownership.rs # OwnershipTracker
│
├── multiraft/       # Horizontal scaling
│   ├── mod.rs       # Module root
│   ├── coordinator.rs    # MultiRaftCoordinator
│   ├── shard.rs          # Individual Raft group
│   ├── router.rs         # ShardRouter
│   ├── registry.rs       # ShardRegistry
│   ├── placement.rs      # ShardPlacement (HashRing)
│   ├── migration.rs      # ShardMigration
│   ├── migration_orchestrator.rs  # MigrationOrchestrator
│   ├── migration_routing.rs       # DualWriteTracker
│   └── leader_tracker.rs # Gossip-based leader discovery
│
├── metrics/         # Observability
│   └── mod.rs       # Counters, Gauges, Histograms
│
├── testing/         # Chaos testing
│   ├── mod.rs       # ChaosController
│   └── runner.rs    # ChaosRunner (scenarios)
│
├── config.rs        # All configuration types
├── error.rs         # Error types
├── types.rs         # Common types (NodeId, CacheCommand)
└── lib.rs           # Public API exports
```

## Key Design Decisions

### 1. Two-Tier Cluster Membership

The cluster uses two independent layers to prevent split-brain:

```
┌───────────────────────────────────────────────────────────────┐
│  Layer 1: Discovery (Memberlist/Gossip)                        │
│  - Automatic node discovery via seed nodes                     │
│  - Failure detection via SWIM protocol                         │
│  - Does NOT affect Raft voting membership                      │
└───────────────────────────────────────────────────────────────┘
                              │
                    Manual Approval Required
                              │
                              ▼
┌───────────────────────────────────────────────────────────────┐
│  Layer 2: Raft Voting Membership                               │
│  - Explicitly controlled via add_peer/remove_peer              │
│  - Configuration changes through Raft consensus                │
│  - Never auto-synced from gossip layer                         │
└───────────────────────────────────────────────────────────────┘
```

**Rationale**: Automatic Raft membership changes based on gossip can cause split-brain during network partitions. Manual approval ensures operators explicitly control quorum.

### 2. Consistency Model

| Operation | Consistency | Latency |
|-----------|-------------|---------|
| Write (put/delete) | Linearizable | Raft round-trip |
| Read (get) | Locally consistent | Sub-millisecond |
| Leader read | Linearizable | Sub-millisecond |

**Rationale**: Most cache workloads are read-heavy. Locally consistent reads provide high performance while writes maintain strong consistency through Raft.

### 3. State Machine Apply is Infallible

```rust
// Pre-validate BEFORE proposing to Raft
if key.is_empty() {
    return Err(Error::InvalidKey);
}

// Apply MUST succeed after Raft commit
fn apply(&mut self, command: CacheCommand) {
    // No Result<> return - this cannot fail
    match command {
        CacheCommand::Put { key, value, ttl } => {
            self.cache.put(key, value, ttl);
        }
        // ...
    }
}
```

**Rationale**: If apply could fail after Raft commit, the cluster would become inconsistent. All validation happens before proposal.

### 4. Multi-Raft Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                   MultiRaftCoordinator                         │
│                                                                │
│   Key → Shard:  shard_id = xxhash64(key) % num_shards         │
│                                                                │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
│   │ Shard 0  │  │ Shard 1  │  │ Shard 2  │  │ Shard N  │     │
│   │  Leader  │  │  Leader  │  │  Leader  │  │  Leader  │     │
│   │  Node 1  │  │  Node 2  │  │  Node 3  │  │  Node 1  │     │
│   └──────────┘  └──────────┘  └──────────┘  └──────────┘     │
└───────────────────────────────────────────────────────────────┘
```

**Rationale**: A single Raft group tops out at ~10-20K writes/sec due to leader serialization. Multiple shards distribute the leader bottleneck across nodes.

### 5. Request Forwarding

```
Client → Follower → Leader → Raft Commit → Response

┌──────────┐     Write Request     ┌──────────┐
│  Client  │ ──────────────────────▶│ Follower │
└──────────┘                        └──────────┘
                                         │
                                   Forward to Leader
                                         │
                                         ▼
                                    ┌──────────┐
                                    │  Leader  │
                                    └──────────┘
                                         │
                                    Raft Commit
                                         │
                                         ▼
                                    Response flows back
```

**Rationale**: Clients don't need to know which node is the leader. Any node can accept writes and forward to the leader transparently.

## Wire Protocol

Messages use a simple length-prefixed format:

```
┌─────────────────────────────────────────────────────────┐
│  4 bytes (BE u32)  │        Variable length             │
│   Message Length   │      Bincode-encoded Message       │
└─────────────────────────────────────────────────────────┘
```

- Max message size: 16 MB
- Raft messages use protobuf internally (via raft-rs)
- Application messages use bincode

## Checkpoint Format

Snapshots use a binary format with LZ4 compression:

```
┌────────────────────────────────────────────────────────────────┐
│  Magic: "MCRS" (4 bytes)                                        │
├────────────────────────────────────────────────────────────────┤
│  Version: u8                                                    │
├────────────────────────────────────────────────────────────────┤
│  Flags: u8                                                      │
├────────────────────────────────────────────────────────────────┤
│  Raft Index: u64                                                │
├────────────────────────────────────────────────────────────────┤
│  Raft Term: u64                                                 │
├────────────────────────────────────────────────────────────────┤
│  LZ4 Compressed Entries                                         │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Entry 1: key_len | key | value_len | value | ttl         │   │
│  │ Entry 2: ...                                              │   │
│  └─────────────────────────────────────────────────────────┘   │
├────────────────────────────────────────────────────────────────┤
│  CRC32 Checksum: u32                                            │
└────────────────────────────────────────────────────────────────┘
```

## Lock Hierarchy (Multi-Raft)

To prevent deadlocks, locks must be acquired in this order:

1. `MultiRaftCoordinator.state`
2. `MultiRaftCoordinator.shard_configs`
3. `MigrationOrchestrator.active_orchestrations`
4. `ShardMigrationCoordinator.active_migrations`
5. `ShardPlacement.ring`
6. `ShardPlacement.pending_ring`
7. `ShardRegistry` internal locks

## Data Flow

### Write Path

```
1. Client calls cache.put(key, value)
2. Router determines target shard (if Multi-Raft)
3. Check if local node is leader
   - Yes: Propose to Raft
   - No: Forward to leader (if forwarding enabled)
4. Raft replicates to followers
5. On commit: State machine applies to Moka cache
6. Response returned to client
```

### Read Path

```
1. Client calls cache.get(key)
2. Router determines target shard (if Multi-Raft)
3. Read directly from local Moka cache
4. Return value (may be stale on followers)
```

### Migration Path (Multi-Raft)

```
1. Topology change detected (node join/leave)
2. ShardPlacement calculates shard movements
3. MigrationOrchestrator creates migration plan
4. For each shard movement:
   a. Phase 1: Add target node as Raft learner
   b. Phase 2: Stream shard data to target
   c. Phase 3: Promote learner to voter
   d. Phase 4: Transfer leadership (if primary)
   e. Phase 5: Remove source from Raft group
5. Update routing tables
```

## Performance Characteristics

| Operation | Expected Latency | Throughput |
|-----------|-----------------|------------|
| Local read | < 1ms | 100K+ ops/sec |
| Raft write | 5-50ms | 10-20K ops/sec per shard |
| Leader read | < 1ms | 100K+ ops/sec |

Scaling horizontally with N shards provides approximately N times write throughput, with each shard handling independent Raft consensus.
