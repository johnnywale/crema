# Production Readiness Gaps

This document outlines the remaining work needed to make this distributed cache production-ready.

---

## Current Status

The following components are **implemented and tested**:
- Multi-Raft shard coordination
- Consistent hashing with virtual nodes (256 vnodes/node)
- Migration orchestration with pipelined streaming
- Rate limiting with token bucket
- Crash recovery via checkpoint/snapshot + Raft log replay
- Transient vs permanent error classification
- Write blocking during ownership transfer
- Router cache invalidation on topology changes
- Janitor for zombie cleanup with orphan learner detection
- Dual-write failure tracking and reconciliation (`DualWriteTracker`)
- Migration cleanup handlers for failed migrations
- Request forwarding for migration proposals (`RpcMigrationRaftProposer`)
- Zombie fencing via coordinator term validation
- Lock hierarchy documentation and TOCTOU fixes
- Persist-before-memory crash consistency (for Raft log, not state machine)
- **Raft consensus via tikv/raft-rs 0.7**
- **Raft log persistence via RocksDB** (optional feature)
- **Data transport for migrations (`RpcDataTransporter`)**
- **Cross-shard client request forwarding (`ShardForwarder`)**
- **Shard-specific error types (`ShardNotLocal`, `ShardLeaderUnknown`)**
- **Persistent shard leader hints for fast recovery**
- **Forwarding metrics with Prometheus export**
- **Linearizable reads via Read-Index protocol**

## Missing Components for Production

### 1. Network Layer (DONE)

**Fully Implemented**:

1. **`RpcMigrationRaftProposer`** (`src/multiraft/migration_transport.rs`):
   - Migration proposal forwarding to leader node
   - Request-response correlation with unique request IDs
   - Timeout handling for forwarded requests
   - Leader hint updates on redirects

2. **`RpcDataTransporter`** (`src/multiraft/migration_transport.rs`):
   - `fetch_batch()` - Fetches data batches from source node during migration
   - `apply_batch()` - Applies data batches to target shard
   - `get_shard_entry_count()` / `get_shard_size()` - Shard statistics
   - Uses existing `RaftTransport` infrastructure

3. **`ShardForwarder`** (`src/multiraft/shard_forwarder.rs`):
   - Cross-shard request forwarding for Multi-Raft
   - Node address registration and discovery
   - Pending request tracking with timeout

4. **Message Types** (`src/network/rpc.rs`):
   - `MigrationFetchRequest/Response` - Data migration
   - `MigrationApplyRequest/Response` - Batch application
   - `ShardForwardedCommand/Response` - Client request forwarding

---

### 2. Persistent Storage (DONE)

**Implemented**: RocksDB-based persistent storage for Raft log.

**Components**:

1. **`RocksDbStorage`** (`src/consensus/rocksdb_storage.rs`):
   - Implements `raft::Storage` trait for persistent Raft log
   - Uses column families: `entries`, `state`, `snapshots`
   - Configurable sync writes for durability vs performance tradeoff
   - Crash recovery with state reload on startup

2. **`RaftStorage`** (`src/consensus/storage.rs`):
   - Unified storage enum supporting both `MemStorage` and `RocksDbStorage`
   - Allows runtime selection of storage backend via configuration

3. **Configuration** (`src/config.rs`):
   ```rust
   pub enum RaftStorageType {
       Memory,                           // Fast, non-durable
       RocksDb(RocksDbConfig),          // Durable, requires feature flag
   }

   pub struct RocksDbConfig {
       pub path: String,
       pub sync_writes: bool,
   }
   ```

**Usage**:
```rust
// Enable RocksDB storage in config
let config = CacheConfig {
    raft: RaftConfig {
        storage_type: RaftStorageType::RocksDb(RocksDbConfig {
            path: "./raft-data".to_string(),
            sync_writes: true,
        }),
        ..Default::default()
    },
    ..Default::default()
};
```

**Features**:
- Write-Ahead Log (WAL) for Raft durability
- Crash recovery replay from persisted state
- Snapshot persistence for state machine
- Log compaction for size management
- Feature-gated: enable with `--features rocksdb-storage`

---

### 3. Client Request Forwarding (DONE)

**Implemented**: Transparent forwarding via `ShardForwarder`.

**Components**:

1. **`ShardForwarder`** (`src/multiraft/shard_forwarder.rs`):
   ```rust
   pub struct ShardForwarder {
       node_id: NodeId,
       config: ShardForwardingConfig,
       node_addresses: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
       pending_forwards: Arc<DashMap<u64, oneshot::Sender<ForwardResult>>>,
   }
   ```

2. **Message Types** (`src/network/rpc.rs`):
   - `ShardForwardedCommand` - Request with TTL hop-count to prevent loops
   - `ShardForwardResponse` - Response with optional leader hints

3. **Error Types** (`src/error.rs`):
   - `ShardNotLocal { shard_id, target_node }` - Key belongs to different node
   - `ShardLeaderUnknown(shard_id)` - Leader not yet discovered via gossip

4. **Coordinator Integration** (`src/multiraft/coordinator.rs`):
   - `get()`, `put()`, `delete()` methods forward when shard is not local
   - Forwarding can be enabled/disabled via configuration

5. **Persistent Leader Hints** (`src/multiraft/shard_storage.rs`):
   - Fast shard leader discovery on restart

---

### 4. Linearizable Reads (DONE)

**Implemented**: Read-Index protocol for strongly consistent reads.

**Components**:

1. **`RaftNode::read_index()`** (`src/consensus/node.rs`):
   - Initiates Raft Read-Index request with unique context
   - Waits for ReadState confirmation (leadership verified via quorum)
   - Waits for state machine to apply up to read index
   - Returns the index at which read is linearizable

2. **`DistributedCache::consistent_get()`** (`src/cache/mod.rs`):
   - High-level API for linearizable reads
   - Verifies leadership via Read-Index protocol
   - Falls back to NotLeader error if not leader

**Usage**:
```rust
// Strongly consistent read - guaranteed to see latest write
let value = cache.consistent_get(b"key").await?;

// vs regular read - may be stale on followers (but faster)
let value = cache.get(b"key").await;
```

**Trade-offs**:
- `consistent_get()` - Linearizable but requires leadership confirmation (~1 RTT)
- `get()` - Eventually consistent but fast (local read with commit index wait)

---

### 5. Raft Library Integration (DONE)

**Implemented**: Using `tikv/raft-rs` version 0.7.

**Components**:

1. **`RaftNode`** (`src/consensus/node.rs`):
   - Full Raft consensus implementation using `raft::RawNode`
   - Leader election, log replication, membership changes
   - Tick-based timing with configurable election/heartbeat ticks

2. **`MemStorage`** (`src/consensus/storage.rs`):
   - In-memory Raft log storage implementing `raft::Storage` trait
   - Hard state, conf state, and log entry management

3. **`CacheStateMachine`** (`src/consensus/state_machine.rs`):
   - Applies committed Raft entries to cache storage
   - Idempotent application for crash recovery

4. **`RaftTransport`** (`src/consensus/transport.rs`):
   - TCP-based message transport between Raft peers
   - Connection pooling and retry logic

**Note**: Raft log storage is configurable. Enable `--features rocksdb-storage` for persistent storage. See "Persistent Storage" section for details.

---

## Recently Implemented Improvements

### Concurrency Safety (Implemented)

The following concurrency issues have been fixed:

1. **Lock Ordering** (`shard_placement.rs`): The `commit()` method now acquires `ring` lock before `pending_ring` to prevent deadlocks.

2. **TOCTOU Fix** (`coordinator.rs`): The `create_shard()` method holds the `shard_configs` write lock for the entire operation to prevent race conditions.

3. **Atomic Read-Modify-Write** (`migration_orchestrator.rs`): The `execute_commit()` method atomically reads the original routing strategy and sets `BlockWrites` in a single lock acquisition.

4. **Persistence Order** (`migration.rs`): All phase transitions now persist to disk FIRST, then update in-memory state for crash consistency.

### Migration Cleanup (Implemented)

1. **Cleanup Handler** (`migration_cleanup.rs`): New `MigrationCleanupHandler` trait and `MigrationCleanupManager` for cleaning up:
   - Partial data on target node
   - Checkpoint files
   - Orphan Raft learners
   - Temporary files

2. **Janitor Enhancements** (`raft_migration.rs`): The janitor now:
   - Detects orphan learners (learners without active migrations)
   - Removes orphan learners automatically
   - Tracks cleanup progress in `JanitorReport`

### Zombie Fencing (Implemented)

1. **Coordinator Term Validation** (`migration_orchestrator.rs`):
   - `AbortMigration` commands include `coordinator_term`
   - `validate_coordinator_term()` rejects commands from stale coordinators
   - Prevents partitioned coordinators from aborting already-committed migrations

### Lock Hierarchy Documentation

The lock acquisition order is documented in `src/multiraft/mod.rs`:
1. `MultiRaftCoordinator.state`
2. `MultiRaftCoordinator.shard_configs`
3. `MigrationOrchestrator.active_orchestrations`
4. `ShardMigrationCoordinator.active_migrations`
5. `ShardPlacement.ring`
6. `ShardPlacement.pending_ring`
7. `ShardRegistry` internal locks

---

## Secondary Improvements

### 6. Hinted Handoff for Dual Writes

**Status**: Partially Implemented

**Implemented (`DualWriteTracker`)**:
- Tracks failed secondary writes during dual-write mode
- Records key, value, timestamp, and error for each failure
- Reconciles failures before commit phase via `reconcile_dual_write_failures()`
- Limits tracked failures per shard to prevent unbounded growth
- Filters out stale failures (older than configurable max age)

```rust
// Existing implementation in src/multiraft/migration_routing.rs
pub struct DualWriteTracker {
    failures: Mutex<HashMap<ShardId, Vec<FailedDualWrite>>>,
    max_failures_per_shard: usize,  // Default: 10,000
    max_failure_age: Duration,       // Default: 5 minutes
}
```

**Remaining Work**:
- Add periodic cleanup of stale failures in background task
- Add metrics for failure counts and reconciliation success rates

### 7. Metrics and Observability (DONE)

**Implemented** (`src/metrics/mod.rs`):

**Cache Operations**:
- `cache_get_total`, `cache_get_hits`, `cache_get_misses` - Cache operation counts
- `cache_put_total`, `cache_put_success`, `cache_put_failures` - Put operation counts
- `cache_delete_total` - Delete operation counts
- `cache_get_latency_seconds`, `cache_put_latency_seconds` - Latency histograms

**Raft Consensus**:
- `raft_proposals_total`, `raft_proposals_failed` - Raft proposal counts
- `raft_propose_latency_seconds` - Raft proposal latency histogram
- `raft_term`, `raft_commit_index`, `raft_applied_index` - Raft state gauges

**Rebalancing/Migration**:
- `rebalance_total`, `rebalance_success`, `rebalance_failures` - Rebalancing counts
- `rebalance_duration_seconds` - Rebalancing duration histogram
- `migration_total`, `migration_success`, `migration_failures` - Migration counts
- `migration_duration_seconds` - Migration duration histogram
- `migration_active` - Currently active migrations gauge
- `write_blocked_during_migration_total` - Write blocking events counter

**Leader Elections**:
- `shard_leader_changes_total` - Leader election frequency
- `shard_leader_tenure_seconds` - Duration of leadership tenure

**Shard Forwarding**:
- `forward_total`, `forward_success`, `forward_failures`, `forward_timeouts` - Forwarding counts
- `forward_latency_seconds` - Forwarding latency histogram
- `forward_pending` - Currently pending forwards gauge

**Checkpointing**:
- `snapshots_created`, `snapshots_loaded`, `snapshots_failed` - Snapshot counts
- `snapshot_duration_seconds`, `snapshot_load_duration_seconds` - Snapshot latencies

**Export**: Prometheus format via `to_prometheus()` method

### 8. Graceful Shutdown (DONE)

**Implemented**: Comprehensive shutdown with timeout and proper cleanup.

**Components**:

1. **`DistributedCache::shutdown()`** and **`shutdown_with_timeout()`** (`src/cache/mod.rs`):
   - Stops accepting new requests
   - Waits for pending Raft proposals to complete (with timeout)
   - Pauses active migrations and checkpoints their state
   - Leaves memberlist cluster gracefully
   - Stops background tasks (tick loop, network server)
   - Flushes Raft storage (important for persistent storage)

2. **`RaftNode::shutdown()`** (`src/consensus/node.rs`):
   - Stops accepting new proposals
   - Waits for pending proposals with configurable timeout
   - Flushes storage if using RocksDB

3. **`MultiRaftCoordinator::shutdown()`** (`src/multiraft/coordinator.rs`):
   - Pauses key-level and Raft-native migrations
   - Sets all shards to stopped state

**Usage**:
```rust
// Default 30-second timeout for pending operations
cache.shutdown().await;

// Custom timeout
cache.shutdown_with_timeout(Duration::from_secs(60)).await;
```

---

## Implementation Priority

| Priority | Component | Effort | Impact | Status |
|----------|-----------|--------|--------|--------|
| 1 | Network Layer (Data Transport) | High | Critical for distributed operation | **Done** (`RpcDataTransporter`) |
| 2 | Persistent Storage (WAL) | High | Required for durability | **Done** (`RocksDbStorage`) |
| 3 | Raft Library Integration | Medium | Core consensus functionality | **Done** (tikv/raft-rs 0.7) |
| 4 | Client Request Forwarding | Medium | Required for client usability | **Done** (`ShardForwarder`) |
| 5 | Linearizable Reads | Medium | Required for strong consistency | **Done** (`consistent_get()`) |
| 6 | Hinted Handoff | Low | Improves dual-write reliability | **Done** (`DualWriteTracker`) |
| 7 | Enhanced Metrics | Low | Operational visibility | **Done** (full metrics suite) |
| 8 | Graceful Shutdown | Low | Clean restarts | **Done** (timeout + cleanup) |
| - | Checkpoint/Snapshot Durability | - | State machine persistence | **Done** (`CheckpointManager`) |
| - | Concurrency Safety | - | Critical for correctness | **Done** |
| - | Migration Cleanup | - | Required for reliability | **Done** |
| - | Zombie Fencing | - | Prevents split-brain | **Done** |
| - | Shard-specific Error Types | - | Better error handling | **Done** (`ShardNotLocal`, `ShardLeaderUnknown`) |
| - | Persistent Leader Hints | - | Fast shard leader discovery | **Done** (`ShardStorageManager`) |

---

## Testing Recommendations

1. **Fuzz Testing**: Random node add/remove while checking data consistency
2. **Chaos Testing**: Kill coordinator during migration phases
3. **Model Checking**: Use `madsim` or similar for deterministic distributed testing
4. **Performance Testing**: Benchmark with realistic workloads (YCSB)

---

## Architecture Diagram

```
┌────────────────────────────────────────────────────────────────┐
│                        Client Layer                            │
├────────────────────────────────────────────────────────────────┤
│                     Request Router                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ Key → Shard  │  │   Forward    │  │    MOVED     │          │
│  │   Mapping    │  │   to Node    │  │   Response   │          │
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

### State Machine Durability Model

**Recovery Path (standard Raft snapshot-based durability):**
```
Crash → Load Checkpoint (snapshot) → Replay Raft Log (entries after snapshot) → Ready
```

**Components:**

1. **Checkpoint/Snapshot** (`src/checkpoint/`):
   - Periodically serializes Moka cache state to disk
   - LZ4 compression for efficient storage
   - CRC32 checksums for data integrity
   - Configurable triggers: log threshold (default: 10K entries) or time interval (default: 5 min)
   - TTL preservation across restarts

2. **Recovery** (`CheckpointManager::load_snapshot()`):
   - Loads latest snapshot into Moka cache
   - Replays any Raft log entries committed after snapshot index
   - Idempotent application for correctness

**Operational Considerations:**

| Configuration | Recommendation |
|--------------|----------------|
| Snapshot frequency | Balance recovery time vs I/O overhead |
| Moka capacity | Set larger than expected working set to avoid eviction |
| TTL entries | Checkpoint preserves absolute expiration time |
| RocksDB feature | Enable for Raft log durability (required for crash safety) |

**Note:** For maximum durability, enable RocksDB storage for Raft log (`--features rocksdb-storage`).
With MemStorage (default), both Raft log and state machine are volatile until checkpoint.

### Router Design Principles

The Request Router follows Redis Cluster / TiKV patterns:
1. **Stateless + Versioned**: Shard map must be versioned
2. **MOVED Tolerance**: Clients must handle MOVED responses gracefully
3. **No Assumptions**: Server cannot assume router's shard map is current

### Coordinator Responsibility Boundaries

| Component | Responsibility | Does NOT |
|-----------|---------------|----------|
| Shard Registry | Map shard → RaftGroup | Touch data |
| Migration Orchestrator | Change shard ownership | Handle reads/writes |
| Raft Lifecycle | Start/stop Raft groups | Participate in data path |
