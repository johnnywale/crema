# Cache Module

The cache module provides the main `DistributedCache` API and local storage implementation.

## Components

### `DistributedCache` (mod.rs)

The main entry point for the distributed cache library. Provides a strongly consistent
distributed cache backed by Raft consensus.

**Key Features:**
- Strong consistency for writes via Raft consensus
- Fast local reads from Moka cache
- Automatic TTL and TinyLFU eviction
- Two-tier cluster membership management

**Usage:**
```rust
use distributed_cache::{DistributedCache, CacheConfig};
use std::time::Duration;

let config = CacheConfig::new(1, "127.0.0.1:9000".parse()?)
    .with_max_capacity(100_000)
    .with_default_ttl(Duration::from_secs(3600));

let cache = DistributedCache::new(config).await?;

// Local operations (fast, no consensus)
cache.put_local("key", "value").await;

// Distributed operations (through Raft)
cache.put("key", "value").await?;
cache.delete("key").await?;

// Reads are always local
if let Some(value) = cache.get(b"key").await {
    println!("Found: {:?}", value);
}
```

### `CacheStorage` (storage.rs)

Local cache storage backed by Moka. Provides:
- Thread-safe concurrent access
- Automatic TTL expiration
- TinyLFU eviction policy (better than LRU)
- Memory-aware sizing with weigher function
- Hit/miss statistics

**Configuration Options:**
- `max_capacity`: Maximum number of entries
- `default_ttl`: Time-to-live for entries
- `default_tti`: Time-to-idle for entries

## Architecture

```
DistributedCache
├── CacheStorage (Moka)      # Local cache
├── RaftNode                 # Consensus
├── ClusterMembership        # Peer management
└── NetworkServer            # RPC handling
```

## Write Path

1. Client calls `put(key, value)`
2. Command is pre-validated (key not empty, size limits)
3. If not leader, returns `NotLeader` error with leader hint
4. Command serialized and proposed to Raft
5. Raft replicates to majority
6. Entry committed and applied to all nodes' Moka caches
7. Success returned to client

## Read Path

1. Client calls `get(key)`
2. Read directly from local Moka cache
3. Return value (or None if not found)

Note: Reads are eventually consistent on followers. For strongly consistent
reads, ensure you read from the leader.
