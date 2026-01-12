# Consensus Module

The consensus module implements Raft-based distributed consensus using the `raft-rs` library.

## Components

### `RaftNode` (node.rs)

Wrapper around `raft-rs` RawNode that manages the Raft consensus algorithm.

**Responsibilities:**
- Leader election
- Log replication
- Proposal handling
- Message routing
- Tick loop management

**Key Methods:**
```rust
// Create a new Raft node
let raft = RaftNode::new(id, peers, config, state_machine)?;

// Propose a command (leader only)
let result = raft.propose(CacheCommand::put("key", "value")).await?;

// Handle incoming Raft message
raft.step(message)?;

// Tick the Raft state machine
raft.tick();

// Process ready state
raft.process_ready().await;
```

### `MemStorage` (storage.rs)

In-memory implementation of Raft's `Storage` trait.

**Features:**
- Stores Raft log entries
- Maintains hard state (term, vote, commit)
- Maintains conf state (voters, learners)
- Supports log compaction
- Supports snapshot storage

**Key Operations:**
```rust
let storage = MemStorage::new_with_conf_state(vec![1, 2, 3]);

// Append entries
storage.append(&entries)?;

// Compact log
storage.compact(index)?;

// Apply snapshot
storage.apply_snapshot(snapshot)?;
```

### `CacheStateMachine` (state_machine.rs)

State machine that applies committed Raft entries to the Moka cache.

**CRITICAL:** The `apply` method is designed to be **infallible**. It cannot fail
after a Raft entry is committed, because:
1. Raft log is the source of truth
2. Apply must be idempotent (safe to replay)
3. Crash recovery replays log to rebuild cache

**Commands Supported:**
- `Put { key, value, ttl }` - Insert/update entry
- `Delete { key }` - Remove entry
- `Clear` - Remove all entries

### `RaftTransport` (transport.rs)

Transport layer for sending Raft messages between peers.

**Features:**
- Peer address management
- Connection pooling
- Async message sending
- Length-prefixed framing

## Raft Configuration

```rust
RaftConfig {
    election_tick: 10,      // 10 ticks = 1 second
    heartbeat_tick: 3,      // 3 ticks = 300ms
    tick_interval_ms: 100,  // 100ms per tick
    max_size_per_msg: 1MB,
    max_inflight_msgs: 256,
    pre_vote: true,
}
```

## Message Flow

```
Client Request
     │
     ▼
1. Forward to Leader ─────────┐
     │                        │ (if follower)
     ▼                        │
2. Leader appends to log      │
     │                        │
     ▼                        │
3. Replicate to majority ─────┘
     │
     ▼
4. Leader commits entry
     │
     ▼
5. Apply to Moka cache (MUST SUCCEED)
     │
     ▼
6. Return success to client
```

## Error Handling

- `RaftError::NotLeader` - Operation requires leader, includes leader hint
- `RaftError::ProposalDropped` - Proposal failed (e.g., leader changed)
- `RaftError::Internal` - Internal Raft error

## Consistency Guarantees

- **Linearizable writes**: All writes go through Raft consensus
- **Sequential consistency**: Operations from same client are ordered
- **Durability**: Committed entries survive leader failures (with quorum)
