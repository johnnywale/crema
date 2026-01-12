# Rebalancing Module

The rebalancing module coordinates data transfer when nodes join or leave the cluster, ensuring data consistency and minimal service disruption.

## Architecture

```
rebalancing/
├── mod.rs           # Module exports and documentation
├── coordinator.rs   # Rebalancing orchestration
└── transfer.rs      # Data transfer types
```

## Three-Phase Rebalancing

### Phase 1: Calculate Ownership Changes

When a membership change occurs:

```rust
use distributed_cache::rebalancing::RebalanceCoordinator;

// Start rebalancing
let op_id = coordinator.start_node_join(3)?;

// Calculate which keys need to move
let transfers = coordinator.calculate_transfers()?;
```

### Phase 2: Stream Data

Transfer entries between nodes:

```rust
use distributed_cache::rebalancing::{TransferBatch, TransferEntry};

// Create transfer entries with TTL preservation
let entry = TransferEntry::with_expiration(
    key,
    value,
    expires_at,  // Absolute time, not relative TTL!
);

// Batch entries for efficient transfer
let batch = TransferBatch::new(sequence, entries, is_final);
```

### Phase 3: Commit New Ring

After successful transfer:

```rust
// Commit to make the change effective
let new_ring = coordinator.commit()?;

// Clean up
coordinator.finish()?;
```

## Components

### RebalanceCoordinator

Orchestrates the entire rebalancing process:

```rust
use distributed_cache::rebalancing::{RebalanceCoordinator, RebalanceConfig};

let config = RebalanceConfig {
    batch_size: 1000,
    batch_timeout: Duration::from_secs(30),
    max_concurrent_transfers: 4,
    allow_reads_during_rebalance: true,
    allow_writes_during_rebalance: false,
    throttle_delay: Duration::from_millis(10),
};

let coordinator = RebalanceCoordinator::new(ownership_tracker, config);
```

### TransferEntry

A cache entry being transferred:

```rust
use distributed_cache::rebalancing::TransferEntry;

// Without expiration
let entry = TransferEntry::new(key, value);

// With expiration (preserves absolute time)
let entry = TransferEntry::with_expiration(key, value, expires_at);

// Check if expired
if entry.is_expired() {
    // Skip this entry
}

// Get remaining TTL for insertion
if let Some(ttl) = entry.remaining_ttl() {
    cache.insert_with_ttl(key, value, ttl);
}
```

### TransferBatch

Groups entries for efficient transfer:

```rust
use distributed_cache::rebalancing::TransferBatch;

let batch = TransferBatch::new(
    sequence,   // Batch number
    entries,    // Vec<TransferEntry>
    is_final,   // Last batch?
);

// Filter out expired entries before sending
let batch = batch.filter_expired();
```

## Rebalancing States

```rust
pub enum RebalanceState {
    Pending,     // Initiated, not started
    Calculating, // Computing ownership changes
    Streaming,   // Transferring data
    Committing,  // Finalizing hash ring
    Complete,    // Successfully finished
    Failed,      // Error occurred
    Cancelled,   // Manually cancelled
}
```

## Operation Types

```rust
pub enum RebalanceType {
    NodeJoin(NodeId),   // New node joining
    NodeLeave(NodeId),  // Node leaving/failed
    Manual,             // Admin-triggered
}
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `batch_size` | 1000 | Max entries per batch |
| `batch_timeout` | 30s | Timeout per batch |
| `max_concurrent_transfers` | 4 | Parallel transfer limit |
| `allow_reads_during_rebalance` | true | Allow reads during operation |
| `allow_writes_during_rebalance` | false | Allow writes during operation |
| `throttle_delay` | 10ms | Delay between batches |

## TTL Preservation

Critical: Always transfer **absolute expiration time**, not relative TTL:

```rust
// WRONG: Relative TTL causes reset on transfer
let entry = TransferEntry::with_ttl(key, value, Duration::from_secs(3600));

// CORRECT: Absolute expiration preserves original TTL
let entry = TransferEntry::with_expiration(key, value, expires_at);
```

On the receiving node:
```rust
if let Some(remaining) = entry.remaining_ttl() {
    // Insert with remaining time
    cache.insert_with_ttl(entry.key, entry.value, remaining);
} else if entry.expires_at_nanos.is_none() {
    // No expiration
    cache.insert(entry.key, entry.value);
}
// If remaining_ttl is None but expires_at was set, entry has expired - skip it
```

## Progress Tracking

Monitor rebalancing progress:

```rust
// Get operation info
if let Some(info) = coordinator.current_operation_info() {
    println!("Operation: {}", info.id);
    println!("Progress: {:.1}%", info.progress);
    println!("Duration: {:?}", info.duration);
}

// Update progress during transfer
coordinator.update_transfer_progress(from, to, entries_count)?;
```

## Error Handling

```rust
use distributed_cache::rebalancing::RebalanceError;

match coordinator.start_node_join(3) {
    Ok(id) => println!("Started operation {}", id),
    Err(RebalanceError::AlreadyInProgress) => {
        println!("Wait for current rebalancing to complete");
    }
    Err(e) => println!("Failed: {}", e),
}
```

## Node Join Flow

```
1. coordinator.start_node_join(new_node)
2. coordinator.calculate_transfers()
   → Returns list of (from_node, to_node) pairs
3. For each transfer:
   a. from_node streams entries to to_node
   b. coordinator.update_transfer_progress(from, to, count)
   c. coordinator.complete_transfer(from, to)
4. coordinator.commit()
   → Returns new HashRing
5. Apply new ring to all nodes via Raft
6. coordinator.finish()
```

## Node Leave Flow

```
1. coordinator.start_node_leave(leaving_node)
2. coordinator.calculate_transfers()
   → All transfers are FROM leaving_node
3. Stream data to new owners
4. coordinator.commit()
5. Apply new ring (leaving_node removed)
6. coordinator.finish()
```

## Cancellation

Cancel an in-progress operation:

```rust
coordinator.cancel()?;
let op = coordinator.finish()?;
assert_eq!(op.state, RebalanceState::Cancelled);
```

## History

View past operations:

```rust
for op in coordinator.history() {
    println!(
        "Op {}: {:?} - {:?} ({:?})",
        op.id, op.operation_type, op.state, op.duration()
    );
}
```

## Thread Safety

- `RebalanceCoordinator` is thread-safe
- Only one operation can be active at a time
- Progress updates are atomic

## Best Practices

1. **Complete one rebalancing before starting another**
2. **Don't allow writes during rebalancing** (default) to avoid inconsistency
3. **Monitor progress** to detect stuck transfers
4. **Set appropriate timeouts** for your network conditions
5. **Filter expired entries** before transfer to reduce data volume
