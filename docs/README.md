# Crema - Strongly Consistent Distributed Cache

A high-performance embedded distributed cache built on **Raft consensus** for strong consistency and **Moka** for fast local caching.

## Features

| Feature | Status | Description |
|---------|--------|-------------|
| Strong Consistency | Production Ready | Linearizable writes via Raft consensus |
| Fast Local Reads | Production Ready | Sub-millisecond reads from Moka cache |
| Two-Tier Membership | Production Ready | Gossip discovery + manual Raft control |
| Multi-Raft Sharding | Implemented | Horizontal scaling via multiple Raft groups |
| Request Forwarding | Implemented | Automatic follower-to-leader forwarding |
| Checkpointing | Implemented | LZ4-compressed snapshots for fast recovery |
| Metrics | Implemented | Prometheus-style counters, gauges, histograms |
| Chaos Testing | Implemented | Framework for failure injection testing |

## Quick Start

```rust
use crema::{DistributedCache, CacheConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = CacheConfig::new(1, "127.0.0.1:9000".parse()?)
        .with_max_capacity(100_000)
        .with_default_ttl(Duration::from_secs(3600));

    let cache = DistributedCache::new(config).await?;

    // Writes are strongly consistent (Raft consensus)
    cache.put("user:123", "Alice").await?;

    // Reads are locally consistent (fast)
    if let Some(value) = cache.get(b"user:123").await {
        println!("Found: {:?}", value);
    }

    cache.shutdown().await?;
    Ok(())
}
```

## Documentation

### Architecture
- [System Overview](./architecture/OVERVIEW.md) - Module structure and design decisions

### Guides
- [Getting Started](./guides/GETTING_STARTED.md) - Step-by-step setup guide
- [Configuration](./guides/CONFIGURATION.md) - All configuration options
- [Multi-Raft Scaling](./guides/MULTIRAFT_GUIDE.md) - Horizontal scaling with shards

### Features
- [Checkpointing](./features/CHECKPOINTING.md) - Snapshots and recovery
- [Metrics](./features/METRICS.md) - Observability and monitoring
- [Memberlist](./features/MEMBERLIST.md) - Gossip-based node discovery
- [Request Forwarding](./features/REQUEST_FORWARDING.md) - Follower-to-leader forwarding
- [Chaos Testing](./features/CHAOS_TESTING.md) - Failure injection framework

### Status
- [Feature Status](./status/FEATURE_STATUS.md) - Implementation status and integration gaps
- [Production Readiness](./PRD_READINESS_GAPS.md) - Production readiness checklist

## Running Examples

```bash
# Single node
cargo run --example basic

# 3-node cluster (manual setup)
cargo run --example cluster -- 1  # Terminal 1
cargo run --example cluster -- 2  # Terminal 2
cargo run --example cluster -- 3  # Terminal 3

# 3-node cluster with memberlist gossip
cargo run --example memberlist-cluster -- 1  # Terminal 1
cargo run --example memberlist-cluster -- 2  # Terminal 2
cargo run --example memberlist-cluster -- 3  # Terminal 3
```

## Consistency Model

- **Writes**: Strongly consistent via Raft (linearizable)
- **Reads**: Locally consistent (may be stale on followers)
- **Leader Reads**: Strongly consistent when reading from the leader

For applications requiring strong read consistency, always read from the Raft leader.

## License

See [LICENSE](../LICENSE) for details.
