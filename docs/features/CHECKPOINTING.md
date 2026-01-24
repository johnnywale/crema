# Checkpointing

This guide explains how to use the checkpointing system for snapshots and fast recovery.

For the complete API reference, see the [main README](../../README.md).

## Overview

Checkpointing creates periodic snapshots of the cache state for:
- **Fast recovery**: Restore from snapshot instead of replaying entire Raft log
- **Log compaction**: Truncate Raft log after snapshot
- **Disaster recovery**: Restore from backup

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    CheckpointManager                             │
│                                                                  │
│   Triggers:                                                      │
│   • Log threshold (e.g., every 10K entries)                     │
│   • Time interval (e.g., every hour)                            │
│                                                                  │
│   ┌────────────────────────────────────────────────────────┐    │
│   │                  SnapshotWriter                         │    │
│   │                                                         │    │
│   │   1. Serialize cache entries                            │    │
│   │   2. LZ4 compress data                                  │    │
│   │   3. Write to file with CRC32 checksum                  │    │
│   └────────────────────────────────────────────────────────┘    │
│                                                                  │
│   ┌────────────────────────────────────────────────────────┐    │
│   │                  SnapshotReader                         │    │
│   │                                                         │    │
│   │   1. Verify CRC32 checksum                              │    │
│   │   2. LZ4 decompress data                                │    │
│   │   3. Restore cache entries                              │    │
│   └────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Configuration

### Basic Setup

```rust
use crema::{CacheConfig, CheckpointConfig};
use std::time::Duration;

let checkpoint_config = CheckpointConfig::new("./checkpoints")
    .with_log_threshold(10_000)           // Snapshot every 10K entries
    .with_time_interval(Duration::from_secs(3600))  // Or every hour
    .with_compression(true)               // Enable LZ4 compression
    .with_max_snapshots(5);               // Keep last 5 snapshots

let config = CacheConfig::new(node_id, addr)
    .with_checkpoint_config(checkpoint_config);

let cache = DistributedCache::new(config).await?;
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `directory` | `PathBuf` | Required | Where to store snapshots |
| `log_threshold` | `u64` | 10,000 | Entries before snapshot |
| `time_interval` | `Option<Duration>` | None | Time-based trigger |
| `compression` | `bool` | true | Enable LZ4 compression |
| `max_snapshots` | `usize` | 5 | Snapshots to retain |

## Snapshot Format

Snapshots use a binary format:

```
┌────────────────────────────────────────────────────────────────┐
│  Header                                                         │
│  ├─ Magic: "MCRS" (4 bytes)                                    │
│  ├─ Version: u8                                                 │
│  ├─ Flags: u8 (compression, etc.)                              │
│  ├─ Raft Index: u64                                            │
│  └─ Raft Term: u64                                             │
├────────────────────────────────────────────────────────────────┤
│  Body (LZ4 compressed if enabled)                               │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Entry Count: u64                                         │  │
│  │  Entry 1:                                                 │  │
│  │    ├─ Key Length: u32                                     │  │
│  │    ├─ Key: [u8]                                           │  │
│  │    ├─ Value Length: u32                                   │  │
│  │    ├─ Value: [u8]                                         │  │
│  │    └─ Expiry (optional): u64 (Unix timestamp)             │  │
│  │  Entry 2: ...                                             │  │
│  └──────────────────────────────────────────────────────────┘  │
├────────────────────────────────────────────────────────────────┤
│  Footer                                                         │
│  └─ CRC32 Checksum: u32                                        │
└────────────────────────────────────────────────────────────────┘
```

## Manual Snapshot Operations

### Creating a Snapshot

```rust
use crema::checkpoint::{SnapshotWriter, SnapshotMetadata};

// Get current Raft state
let raft_index = cache.raft_index();
let raft_term = cache.raft_term();

let metadata = SnapshotMetadata {
    index: raft_index,
    term: raft_term,
    timestamp: std::time::SystemTime::now(),
};

let mut writer = SnapshotWriter::new("./checkpoints/manual_snapshot", metadata)?;

// Write entries
for (key, value, expiry) in cache.entries() {
    writer.write_entry(&key, &value, expiry)?;
}

writer.finish()?;
```

### Reading a Snapshot

```rust
use crema::checkpoint::{SnapshotReader, SnapshotEntry};

let reader = SnapshotReader::open("./checkpoints/snapshot_00001")?;

// Check metadata
let metadata = reader.metadata();
println!("Snapshot at Raft index {}", metadata.index);

// Read entries
for entry in reader.entries()? {
    let entry: SnapshotEntry = entry?;
    println!("Key: {:?}, Value length: {}", entry.key, entry.value.len());

    if let Some(expiry) = entry.expiry {
        println!("Expires at: {:?}", expiry);
    }
}
```

### Listing Snapshots

```rust
use crema::checkpoint::CheckpointManager;

let manager = CheckpointManager::new(checkpoint_config);

// List available snapshots
for info in manager.list_snapshots()? {
    println!("Snapshot: {}", info.path.display());
    println!("  Index: {}", info.metadata.index);
    println!("  Term: {}", info.metadata.term);
    println!("  Size: {} bytes", info.size);
    println!("  Created: {:?}", info.metadata.timestamp);
}
```

## TTL Preservation

Snapshots preserve TTL information as absolute expiration times:

```rust
// Entry has TTL of 1 hour from now
cache.put_with_ttl("session:123", "data", Duration::from_secs(3600)).await?;

// Snapshot stores absolute expiry timestamp
// When restored:
// - If before expiry: Entry restored with remaining TTL
// - If after expiry: Entry is skipped
```

This ensures entries don't get extended TTLs after restore.

## Recovery Process

When a node starts with an existing snapshot:

```
1. Find latest snapshot in checkpoint directory
2. Verify CRC32 checksum
3. Read snapshot metadata (Raft index, term)
4. Restore cache entries (skipping expired)
5. Apply Raft log entries after snapshot index
6. Node ready to serve
```

### Automatic Recovery

```rust
let config = CacheConfig::new(node_id, addr)
    .with_checkpoint_config(
        CheckpointConfig::new("./checkpoints")
            .with_auto_recover(true)  // Restore from latest snapshot on start
    );

let cache = DistributedCache::new(config).await?;
// Cache is restored from snapshot if available
```

### Manual Recovery

```rust
use crema::checkpoint::CheckpointManager;

let manager = CheckpointManager::new(checkpoint_config);

// Find latest snapshot
let snapshots = manager.list_snapshots()?;
if let Some(latest) = snapshots.last() {
    println!("Restoring from {}", latest.path.display());

    let reader = SnapshotReader::open(&latest.path)?;
    for entry in reader.entries()? {
        let entry = entry?;
        // Apply entry to cache
    }
}
```

## Best Practices

### 1. Choose Appropriate Thresholds

| Workload | Log Threshold | Time Interval |
|----------|---------------|---------------|
| Low write | 50,000 | 4 hours |
| Medium write | 10,000 | 1 hour |
| High write | 5,000 | 15 minutes |

### 2. Monitor Snapshot Size

```rust
let info = manager.latest_snapshot()?;
println!("Snapshot size: {} MB", info.size / 1_000_000);

// Alert if snapshots are growing too large
if info.size > 1_000_000_000 {  // 1 GB
    warn!("Snapshot size exceeds 1 GB");
}
```

### 3. Verify Snapshots Regularly

```rust
for info in manager.list_snapshots()? {
    match manager.verify_snapshot(&info.path) {
        Ok(()) => println!("{}: OK", info.path.display()),
        Err(e) => eprintln!("{}: CORRUPTED - {}", info.path.display(), e),
    }
}
```

### 4. Backup Strategy

```bash
# Copy snapshots to backup storage
rsync -av ./checkpoints/ /backup/crema/snapshots/

# Or use the latest snapshot only
cp $(ls -t ./checkpoints/*.snap | head -1) /backup/
```

### 5. Disk Space Management

Configure `max_snapshots` to prevent disk exhaustion:

```rust
CheckpointConfig::new("./checkpoints")
    .with_max_snapshots(3)  // Keep only last 3 snapshots
```

Older snapshots are automatically deleted after new ones are created.

## Troubleshooting

### Snapshot Creation Fails

**Symptom**: Error creating snapshot.

**Possible causes**:
1. Disk full
2. Permission denied
3. Directory doesn't exist

**Solution**:
```rust
// Ensure directory exists
std::fs::create_dir_all(&checkpoint_config.directory)?;

// Check disk space
let available = fs2::available_space(&checkpoint_config.directory)?;
if available < 1_000_000_000 {  // 1 GB
    warn!("Low disk space for snapshots");
}
```

### Snapshot Restore Fails

**Symptom**: CRC32 checksum mismatch or decompression error.

**Possible causes**:
1. Corrupted file (disk error, incomplete write)
2. Modified file (accidental edit)

**Solution**:
```rust
// Try previous snapshot
let snapshots = manager.list_snapshots()?;
for info in snapshots.iter().rev() {
    match manager.restore_from(&info.path) {
        Ok(()) => {
            println!("Restored from {}", info.path.display());
            break;
        }
        Err(e) => {
            warn!("Snapshot {} corrupted: {}", info.path.display(), e);
            continue;
        }
    }
}
```

### Slow Snapshot Creation

**Symptom**: Snapshots take too long to create.

**Possible causes**:
1. Large cache size
2. Slow disk I/O
3. Compression overhead

**Solution**:
- Use faster storage (SSD)
- Disable compression for large caches (trade space for speed)
- Increase log threshold to reduce frequency

```rust
CheckpointConfig::new("/fast-ssd/checkpoints")
    .with_compression(false)  // Faster but larger files
    .with_log_threshold(50_000)  // Less frequent
```
