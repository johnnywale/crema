# Cluster Module

The cluster module manages cluster membership using a two-tier architecture.

## Two-Tier Architecture

This module implements a critical safety pattern for distributed systems:

### Layer 1: Discovery (Automatic)
- Nodes are automatically discovered via gossip/pings
- Discovery happens in the background
- No manual intervention required
- Fast failure detection

### Layer 2: Raft Membership (Manual)
- Raft voting membership changes require explicit approval
- Prevents split-brain from gossip false positives
- Ensures quorum is always protected
- Admin decides when to add/remove nodes

**Why Two Tiers?**

If Raft membership was automatically updated based on gossip:

```
Initial cluster: 5 nodes [A, B, C, D, E] (quorum = 3)

Network partition:
- Partition 1: [A, B] sees [C, D, E] as down
- Partition 2: [C, D, E] sees [A, B] as down

With automatic sync:
1. Partition 1: Removes C, D, E → Quorum now 2 of [A, B]
2. Partition 2: Removes A, B → Quorum now 2 of [C, D, E]

Result: SPLIT-BRAIN! Both partitions think they have quorum.
```

## Components

### `ClusterMembership` (membership.rs)

Main membership manager with two-tier design.

**Key Methods:**
```rust
// Create membership manager
let (membership, event_rx) = ClusterMembership::new(node_id, config);

// Layer 1: Automatic discovery
membership.handle_node_discovered(node_id, addr);
membership.handle_node_failed_check(node_id);
membership.handle_node_leave(node_id);

// Layer 2: Manual Raft control
membership.add_raft_peer(node_id)?;    // Requires node to be discovered
membership.remove_raft_peer(node_id)?; // Checks quorum safety

// Queries
membership.discovered_nodes();  // All discovered
membership.healthy_nodes();     // Only healthy
membership.raft_peers();        // Voting members
membership.quorum_size();       // Required for consensus
```

### `MemberEvent` (events.rs)

Events emitted when cluster membership changes:

```rust
pub enum MemberEvent {
    NodeJoin { node_id, addr },
    NodeLeave { node_id },
    NodeSuspect { node_id, failed_pings },
    NodeFailed { node_id },
    NodeRecovered { node_id, addr },
    NodeUpdated { node_id, addr },
}
```

### `MemberEventListener` Trait

Subscribe to membership events:

```rust
pub trait MemberEventListener: Send + Sync + 'static {
    fn on_event(&self, event: MemberEvent);
}
```

## Configuration

```rust
MembershipConfig {
    mode: MembershipMode::Manual,  // Safest
    min_peers: 3,                  // Minimum for quorum
    max_peers: 7,                  // Odd number recommended
    failure_confirmations: 3,      // Pings before marking failed
    failure_check_interval: 5s,
    auto_remove_after: None,       // Never auto-remove
}
```

### Membership Modes

- `Manual`: Admin approval required for all changes (safest)
- `SemiAutomatic`: Auto-add after health check, manual remove
- `Automatic`: Full automation (dangerous, not recommended)

## Safe Membership Change Process

```
1. Node Discovery (Automatic)
   ├─ New node joins via gossip
   ├─ Appears in "discovered_nodes" list
   └─ Admin notified via event

2. Health Verification (5-10 minutes)
   ├─ Monitor node stability
   ├─ Verify network connectivity
   └─ Check resource availability

3. Raft Admission (Manual approval)
   ├─ Admin reviews node health
   ├─ Admin executes: add_raft_peer(node_id)
   └─ Raft uses joint consensus for safe addition

4. Node Failure (Automatic detection, manual removal)
   ├─ Gossip detects failure
   ├─ Multi-step verification (3 confirmations)
   ├─ Event sent to admin
   └─ Admin decides: remove or wait for recovery
```

## Node State Machine

```
            ┌─────────┐
            │ Unknown │
            └────┬────┘
                 │ discovered
                 ▼
            ┌─────────┐
    ┌───────│ Healthy │◄──────────┐
    │       └────┬────┘           │
    │ failed     │ ping failed    │ recovered
    │ check      ▼                │
    │       ┌─────────┐           │
    │       │ Suspect │───────────┤
    │       └────┬────┘           │
    │            │ confirmations  │
    │            ▼ reached        │
    │       ┌─────────┐           │
    └──────►│ Failed  │───────────┘
            └─────────┘
```

## Quorum Calculation

For a cluster of N nodes, quorum is: `N / 2 + 1`

| Nodes | Quorum | Tolerates |
|-------|--------|-----------|
| 1     | 1      | 0 failures |
| 3     | 2      | 1 failure  |
| 5     | 3      | 2 failures |
| 7     | 4      | 3 failures |

Always use odd numbers for optimal fault tolerance.
