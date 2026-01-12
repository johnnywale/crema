# Partitioning Module

The partitioning module implements consistent hashing for distributing cache keys across cluster nodes. It ensures even key distribution and minimal redistribution when nodes join or leave the cluster.

## Architecture

```
partitioning/
├── mod.rs           # Module exports and documentation
├── hashring.rs      # Consistent hash ring implementation
└── ownership.rs     # Key ownership tracking
```

## Components

### HashRing

The `HashRing` implements consistent hashing with virtual nodes (vnodes):

```rust
use distributed_cache::partitioning::HashRing;

// Create ring with 2 replicas (numOwners)
let mut ring = HashRing::new(2);

// Add nodes to the cluster
ring.add_node(1);
ring.add_node(2);
ring.add_node(3);

// Get primary owner for a key
let primary = ring.get_primary(b"user:123");

// Get all owners (primary + backups)
let owners = ring.get_replica_owners(b"user:123");
```

### OwnershipTracker

The `OwnershipTracker` provides a higher-level interface for ownership queries:

```rust
use distributed_cache::partitioning::OwnershipTracker;

// Create tracker for local node 1 with 2 replicas
let tracker = OwnershipTracker::new(1, 2);
tracker.add_node(1);
tracker.add_node(2);
tracker.add_node(3);

// Check if this node owns a key
if tracker.should_own(b"user:123") {
    // Process the key locally
}

// Check if this node is the primary
if tracker.is_primary(b"user:123") {
    // Handle as primary (coordinate writes)
}

// Get detailed ownership info
if let Some(ownership) = tracker.get_ownership(b"user:123") {
    println!("Primary: {}", ownership.primary);
    println!("Backups: {:?}", ownership.backups);
}
```

## Consistent Hashing

### Virtual Nodes

Each physical node is represented by 256 virtual nodes (vnodes) on the hash ring. This ensures:

- **Even Distribution**: Keys are distributed roughly equally across nodes
- **Minimal Movement**: When a node joins/leaves, only ~1/N keys move (N = node count)
- **Flexible Topology**: Nodes with more capacity could use more vnodes (future)

### Hash Function

We use xxHash64 for fast, high-quality hashing:

```
Key → xxHash64 → 64-bit position on ring → nearest vnode → physical node
```

### Replica Placement

For `num_replicas = 2`:

```
Key "user:123" → hash position 0x1234...
                         ↓
        ┌───────────────────────────────────────┐
        │  Ring: VN1:1 → VN2:3 → VN3:2 → VN4:1  │
        │               ↑ nearest                │
        │  Primary: Node 3                       │
        │  Backup: Node 2 (next unique node)     │
        └───────────────────────────────────────┘
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `num_replicas` | 2 | Number of copies of each key |
| `vnodes_per_node` | 256 | Virtual nodes per physical node |

## Ownership Roles

```rust
pub enum OwnershipRole {
    Primary,  // Handles writes, coordinates replication
    Backup,   // Holds replica for fault tolerance
    None,     // Should not hold this key
}
```

## Transfer Calculation

When nodes join or leave, calculate required data transfers:

```rust
// Node 4 is about to join
let transfers = tracker.calculate_transfer_on_add(4);
for ((from, to), count) in transfers {
    println!("Transfer {} vnodes from {} to {}", count, from, to);
}

// Node 2 is about to leave
let transfers = tracker.calculate_transfer_on_remove(2);
for ((from, to), count) in transfers {
    println!("Transfer {} vnodes from {} to {}", count, from, to);
}
```

## Pending Transfers

Track keys being transferred during rebalancing:

```rust
use distributed_cache::partitioning::PendingTransfers;

let transfers = PendingTransfers::new();

// Mark key as being transferred out
transfers.mark_outgoing(b"user:123".to_vec());

// During transfer, check status
if transfers.is_outgoing(b"user:123") {
    // Key is being moved, handle reads specially
}

// After transfer completes
transfers.complete_outgoing(b"user:123");
```

## Key Distribution

The hash ring provides even key distribution:

```rust
let distribution = ring.get_distribution(10000);
for (node, count) in distribution {
    // Each node should have roughly 10000/N keys
    println!("Node {}: {} keys", node, count);
}
```

Expected variance is ±20% with 256 vnodes per node.

## Thread Safety

All types are thread-safe:
- `HashRing`: Clone and use in multiple threads
- `OwnershipTracker`: Uses RwLock for concurrent access
- `PendingTransfers`: Uses RwLock for concurrent modifications

## Performance

| Operation | Complexity |
|-----------|------------|
| Add node | O(V log N) where V = vnodes |
| Remove node | O(V log N) |
| Get owner | O(log N) where N = total vnodes |
| Get replicas | O(R log N) where R = num_replicas |

## Integration with Rebalancing

The partitioning module provides the foundation for rebalancing:

1. **Node Join**: Calculate ownership changes, stream affected keys
2. **Node Leave**: Redistribute orphaned keys to remaining owners
3. **Key Lookup**: Route requests to correct owner

```rust
// When node 4 joins:
// 1. Calculate what needs to move
let changes = ring.calculate_add_changes(4);

// 2. Add node to ring (changes ownership)
ring.add_node(4);

// 3. Transfer keys based on calculated changes
for change in changes {
    // Transfer keys from change.from_node to change.to_node
}
```

## Best Practices

1. **Always use num_replicas >= 2** for fault tolerance
2. **Don't change vnodes_per_node** after cluster creation
3. **Complete rebalancing** before adding/removing another node
4. **Monitor distribution** periodically to detect hot spots
