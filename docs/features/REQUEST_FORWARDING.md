# Request Forwarding

This guide explains the request forwarding feature that enables transparent write operations from any node.

## Overview

In a Raft-based distributed cache, only the leader can accept write operations. Request forwarding allows followers to automatically forward write requests to the current leader, making the cluster appear as a single endpoint to clients.

```
┌──────────────────────────────────────────────────────────────────┐
│                    Without Forwarding                             │
│                                                                   │
│  Client ──PUT──▶ Follower ──ERROR: NotLeader──▶ Client           │
│                                                                   │
│  Client must track leader and retry                               │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                    With Forwarding                                │
│                                                                   │
│  Client ──PUT──▶ Follower ──Forward──▶ Leader ──Commit──▶ Response │
│                                                                   │
│  Transparent to client                                            │
└──────────────────────────────────────────────────────────────────┘
```

## Configuration

### Basic Setup

Request forwarding is enabled by default:

```rust
use crema::CacheConfig;

// Forwarding is enabled by default
let config = CacheConfig::new(node_id, raft_addr);
```

### Disabling Forwarding

```rust
let config = CacheConfig::new(node_id, raft_addr)
    .with_forwarding_enabled(false);

// Now writes to followers return NotLeader error with leader hint
```

### Timeout Configuration

```rust
use std::time::Duration;

let config = CacheConfig::new(node_id, raft_addr)
    .with_forwarding_enabled(true)
    .with_forwarding_timeout_ms(5000);  // 5 second timeout
```

### Full Configuration

```rust
use crema::config::ForwardingConfig;

let forwarding = ForwardingConfig::new()
    .with_enabled(true)
    .with_timeout_ms(3000)       // 3 second timeout
    .with_max_pending(5000);     // Max 5000 concurrent forwards

let config = CacheConfig::new(node_id, raft_addr)
    .with_forwarding_config(forwarding);
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | `bool` | `true` | Enable/disable forwarding |
| `forward_timeout_ms` | `u64` | `3000` | Timeout for forwarded requests (ms) |
| `max_pending_forwards` | `usize` | `5000` | Max concurrent pending forwards |

## How It Works

### Message Flow

```
1. Client sends PUT to Follower Node 2
2. Node 2 checks: Am I the leader?
   - No: Forward to leader (Node 1)
3. Node 1 (leader) receives ForwardedCommand
4. Node 1 proposes to Raft
5. Raft commits (replicates to all nodes)
6. Node 1 sends ForwardResponse to Node 2
7. Node 2 returns success to Client
```

### Wire Protocol

Forwarded messages use the existing network protocol:

```rust
pub enum Message {
    // ... other variants ...

    /// Request forwarded from follower to leader
    ForwardedCommand {
        request_id: u64,
        from_node: NodeId,
        command: CacheCommand,
    },

    /// Response from leader to follower
    ForwardResponse {
        request_id: u64,
        result: Result<(), String>,
    },
}
```

## Behavior

### When Forwarding is Enabled

1. **Writes to leader**: Execute directly (no forwarding)
2. **Writes to follower**: Forward to leader, wait for response
3. **Leader unknown**: Return `Error::NotLeader` with `leader_hint: None`
4. **Timeout**: Return `Error::Timeout`
5. **Leader changed during forward**: Retry or return error

### When Forwarding is Disabled

1. **Writes to leader**: Execute directly
2. **Writes to follower**: Return `Error::NotLeader { leader_hint: Some(leader_id) }`

Client code for disabled forwarding:
```rust
loop {
    match cache.put("key", "value").await {
        Ok(()) => break,
        Err(Error::NotLeader { leader_hint: Some(leader) }) => {
            // Connect to leader and retry
            cache = connect_to_node(leader).await?;
        }
        Err(e) => return Err(e),
    }
}
```

## Backpressure

The `max_pending_forwards` setting provides backpressure to prevent memory exhaustion:

```rust
// When pending forwards exceed max_pending_forwards:
// - New forward requests are rejected
// - Error::Backpressure is returned
// - Client should retry with exponential backoff
```

Example handling:
```rust
match cache.put("key", "value").await {
    Ok(()) => println!("Success"),
    Err(Error::Backpressure) => {
        // Too many pending requests, wait and retry
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Retry...
    }
    Err(e) => println!("Error: {:?}", e),
}
```

## Performance Considerations

### Latency Impact

| Scenario | Latency |
|----------|---------|
| Write to leader | ~5-50ms (Raft commit) |
| Write to follower (forwarded) | ~10-100ms (forward + Raft commit) |

Forwarding adds one network hop (follower → leader → follower).

### Throughput

- **Forwarding enabled**: All nodes can accept writes (load distributed)
- **Forwarding disabled**: Only leader accepts writes (concentrated load)

For read-heavy workloads, forwarding overhead is minimal since reads don't require forwarding.

## Best Practices

### 1. Use Forwarding for Simplicity

For most applications, keep forwarding enabled:
- Clients don't need to track leader
- Automatic failover when leader changes
- Simpler client implementation

### 2. Disable for Performance-Critical Writes

If write latency is critical:
```rust
// Disable forwarding
let config = CacheConfig::new(node_id, raft_addr)
    .with_forwarding_enabled(false);

// Client always connects to leader
let leader_cache = connect_to_leader().await?;
leader_cache.put("key", "value").await?;
```

### 3. Tune Timeout Appropriately

```rust
// Fast timeout for real-time applications
.with_forwarding_timeout_ms(1000)  // 1 second

// Longer timeout for batch operations
.with_forwarding_timeout_ms(10000)  // 10 seconds
```

### 4. Monitor Forwarding Metrics

Track forwarding health in production:
- Forward request count
- Forward latency (p50, p99)
- Timeout rate
- Backpressure events

## Error Handling

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `NotLeader` | Forwarding disabled or no leader | Retry with leader hint |
| `Timeout` | Forward took too long | Increase timeout or check network |
| `Backpressure` | Too many pending forwards | Implement backoff, reduce load |
| `NetworkError` | Connection to leader failed | Will retry automatically |

### Retry Strategy

```rust
use std::time::Duration;
use tokio::time::sleep;

async fn put_with_retry(cache: &DistributedCache, key: &str, value: &str) -> Result<()> {
    let mut attempts = 0;
    let max_attempts = 3;
    let mut backoff = Duration::from_millis(100);

    loop {
        match cache.put(key, value).await {
            Ok(()) => return Ok(()),
            Err(Error::Timeout) | Err(Error::Backpressure) if attempts < max_attempts => {
                attempts += 1;
                sleep(backoff).await;
                backoff *= 2;  // Exponential backoff
            }
            Err(e) => return Err(e),
        }
    }
}
```

## Comparison: Forwarding vs Client-Side Routing

| Aspect | Forwarding | Client-Side Routing |
|--------|------------|---------------------|
| Client complexity | Simple | Must track leader |
| Write latency | +1 hop | Optimal |
| Failover | Automatic | Manual reconnect |
| Load distribution | Natural | Concentrated on leader |
| Network hops | 2 (client→follower→leader) | 1 (client→leader) |

Choose forwarding for simplicity, client-side routing for optimal latency.
