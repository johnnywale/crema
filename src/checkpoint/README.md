# Checkpoint Module

The checkpoint module provides persistent storage for cache state through snapshots. It enables fast recovery after restarts and reduces Raft log size by compacting applied entries.

## Architecture

```
checkpoint/
├── mod.rs           # Module exports and documentation
├── format.rs        # Binary file format definitions
├── writer.rs        # Snapshot writer with LZ4 compression
├── reader.rs        # Snapshot reader with decompression
└── manager.rs       # High-level checkpoint management
```

## Components

### CheckpointManager

The main coordinator for snapshot operations:

```rust
use distributed_cache::checkpoint::{CheckpointConfig, CheckpointManager};

let config = CheckpointConfig::new("./checkpoints")
    .with_log_threshold(10_000)       // Snapshot after N entries
    .with_time_interval(Duration::from_secs(300))  // Or after 5 min
    .with_compression(true)           // LZ4 compression
    .with_max_snapshots(3);           // Keep 3 most recent

let manager = CheckpointManager::new(config, storage)?;
```

### SnapshotWriter

Low-level writer for creating snapshot files:

```rust
use distributed_cache::checkpoint::SnapshotWriter;

let mut writer = SnapshotWriter::new(&path, raft_index, raft_term, compress)?;

for (key, value) in entries {
    writer.write(key, value)?;
}

let metadata = writer.finalize()?;
println!("Compressed {} entries to {} bytes", metadata.entry_count, metadata.file_size);
```

### SnapshotReader

Low-level reader for loading snapshot files:

```rust
use distributed_cache::checkpoint::SnapshotReader;

let mut reader = SnapshotReader::open(&path)?;

println!("Snapshot at index {} term {}", reader.raft_index(), reader.raft_term());

while let Some(entry) = reader.read_entry()? {
    if !entry.is_expired() {
        // Process entry
    }
}
```

## File Format

The snapshot file format is designed for efficiency and integrity:

```
┌─────────────────────────────────────────────────┐
│ MAGIC: "MCRS" (4 bytes)                         │
├─────────────────────────────────────────────────┤
│ VERSION: u32 = 1                                │
├─────────────────────────────────────────────────┤
│ FLAGS: u32 (bit 0 = compressed)                 │
├─────────────────────────────────────────────────┤
│ RAFT_INDEX: u64                                 │
├─────────────────────────────────────────────────┤
│ RAFT_TERM: u64                                  │
├─────────────────────────────────────────────────┤
│ TIMESTAMP: u64 (Unix seconds)                   │
├─────────────────────────────────────────────────┤
│ ENTRY_COUNT: u64                                │
├─────────────────────────────────────────────────┤
│ DATA_SIZE: u64 (uncompressed)                   │
├─────────────────────────────────────────────────┤
│ RESERVED: 16 bytes                              │
├─────────────────────────────────────────────────┤
│ DATA BLOCK (possibly LZ4 compressed)            │
│   Entry: key_len(4) + key + val_len(4) + val    │
│          + has_expiry(1) + expires_at?(8)       │
├─────────────────────────────────────────────────┤
│ CRC32: u32                                      │
└─────────────────────────────────────────────────┘

Total header size: 64 bytes
```

### Entry Format

Each entry in the data block:

| Field | Type | Description |
|-------|------|-------------|
| key_len | u32 | Length of key in bytes |
| key | [u8] | Key data |
| value_len | u32 | Length of value in bytes |
| value | [u8] | Value data |
| has_expiry | u8 | 0 = no expiry, 1 = has expiry |
| expires_at | u64? | Unix nanoseconds (only if has_expiry=1) |

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `dir` | "./checkpoints" | Directory for snapshot files |
| `log_threshold` | 10,000 | Create snapshot after N log entries |
| `time_interval` | 5 minutes | Create snapshot after this duration |
| `max_snapshots` | 3 | Maximum snapshots to keep |
| `compress` | true | Use LZ4 compression |
| `enabled` | true | Enable/disable checkpointing |

## Snapshot Triggers

Snapshots are created when either condition is met:
1. **Log Threshold**: Number of entries since last snapshot exceeds `log_threshold`
2. **Time Interval**: Time since last snapshot exceeds `time_interval`

## Recovery Process

On startup, the manager can recover from the latest snapshot:

```rust
// Find and load latest snapshot
if let Some(info) = manager.find_latest_snapshot()? {
    let raft_index = manager.load_snapshot(&info.path).await?;
    println!("Recovered to Raft index {}", raft_index);
}

// Replay any log entries after snapshot index
// ...
```

## Compression

LZ4 compression is enabled by default:

- Fast compression/decompression (GB/s)
- Typical compression ratio: 2-4x for cache data
- Prepended size format for streaming decompression
- CRC32 checksum on compressed data

## Error Handling

```rust
use distributed_cache::checkpoint::FormatError;

match manager.load_snapshot(&path).await {
    Ok(index) => println!("Loaded snapshot at index {}", index),
    Err(FormatError::InvalidMagic) => println!("Not a valid snapshot file"),
    Err(FormatError::ChecksumMismatch { expected, actual }) => {
        println!("Corrupted snapshot: expected CRC {}, got {}", expected, actual);
    }
    Err(FormatError::UnsupportedVersion(v)) => {
        println!("Snapshot version {} not supported", v);
    }
    Err(e) => println!("Failed to load: {}", e),
}
```

## Automatic Cleanup

Old snapshots are automatically removed when new ones are created:

```rust
// With max_snapshots = 3:
// snapshot-0000000000000064-0000000000000001.lz4  <- kept
// snapshot-0000000000000032-0000000000000001.lz4  <- kept
// snapshot-0000000000000010-0000000000000001.lz4  <- kept
// snapshot-0000000000000005-0000000000000001.lz4  <- DELETED
```

## Monitoring Loop

The manager can run a background monitoring loop:

```rust
let manager = Arc::new(manager);
let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

// Start monitoring in background
let monitor = manager.clone();
tokio::spawn(async move {
    monitor.run_monitoring_loop(shutdown_rx, raft_state).await;
});

// Later: trigger graceful shutdown
drop(shutdown_tx);
```

## Thread Safety

- `CheckpointManager` is thread-safe (`Send + Sync`)
- Only one snapshot can be created at a time (atomic flag)
- Reading snapshots is independent of writing

## Performance Considerations

1. **Snapshot Creation**: O(n) where n = number of cache entries
2. **Snapshot Loading**: O(n) with periodic yields for async fairness
3. **Memory**: Compressed data is buffered in memory during write
4. **Disk**: Single sequential write per snapshot

## Integration with Raft

The checkpoint module integrates with Raft consensus:

1. Snapshot captures state at specific `(raft_index, raft_term)`
2. After snapshot, Raft log can be compacted up to snapshot index
3. Recovery: Load snapshot, then replay log entries after snapshot index

```rust
// Implement RaftStateProvider for your Raft node
impl RaftStateProvider for MyRaftNode {
    fn get_applied_state(&self) -> (u64, u64) {
        (self.last_applied_index, self.current_term)
    }
}
```
