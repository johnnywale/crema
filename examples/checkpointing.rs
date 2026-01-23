//! Example demonstrating checkpoint/snapshot functionality for cache recovery.
//!
//! This example shows how to:
//! - Configure checkpointing with time and log thresholds
//! - Create manual snapshots
//! - List and inspect snapshots
//! - Recover from a snapshot
//!
//! Run with:
//!   RUST_LOG=info cargo run --example checkpointing
//!
//! Note: This example uses the checkpoint subsystem directly.
//! In a full cluster, checkpointing would be triggered automatically
//! based on Raft log entries.

use crema::checkpoint::{CheckpointConfig, CheckpointManager, SnapshotReader};
use crema::cache::storage::CacheStorage;
use crema::config::CacheConfig;
use bytes::Bytes;
use std::env;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    println!("===========================================");
    println!("     Checkpointing Example");
    println!("===========================================");
    println!();

    // Create a temporary directory for snapshots
    let checkpoint_dir = "./example_checkpoints";
    fs::create_dir_all(checkpoint_dir)?;

    // Configure checkpoint settings
    let checkpoint_config = CheckpointConfig::new(checkpoint_dir)
        .with_log_threshold(100)           // Snapshot after 100 entries
        .with_time_interval(Duration::from_secs(300)) // Or every 5 minutes
        .with_compression(true)            // Enable LZ4 compression
        .with_max_snapshots(3);            // Keep last 3 snapshots

    println!("Checkpoint Configuration:");
    println!("  Directory:      {}", checkpoint_dir);
    println!("  Log threshold:  {}", checkpoint_config.log_threshold);
    println!("  Time interval:  {:?}", checkpoint_config.time_interval);
    println!("  Compression:    {}", checkpoint_config.compress);
    println!("  Max snapshots:  {}", checkpoint_config.max_snapshots);
    println!();

    // Create cache storage
    let cache_config = CacheConfig::default();
    let storage = Arc::new(CacheStorage::new(&cache_config));

    // Create checkpoint manager
    let manager = CheckpointManager::new(checkpoint_config.clone(), storage.clone())?;
    println!("CheckpointManager created successfully!");
    println!();

    // Part 1: Populate the cache with data
    println!("===========================================");
    println!("      Part 1: Populating Cache");
    println!("===========================================");
    println!();

    let test_data = vec![
        ("user:1", "Alice"),
        ("user:2", "Bob"),
        ("user:3", "Charlie"),
        ("session:abc123", "session_data_for_alice"),
        ("session:def456", "session_data_for_bob"),
        ("config:app_version", "1.2.3"),
        ("config:feature_flags", "dark_mode=true,beta=false"),
        ("cache:popular_items", "[1,2,3,4,5]"),
    ];

    for (key, value) in &test_data {
        storage.insert(Bytes::from(*key), Bytes::from(*value)).await;
        manager.record_entry_unchecked();
        println!("  Inserted: '{}' = '{}'", key, value);
    }

    println!();
    println!("Cache populated with {} entries", storage.entry_count());
    println!("Entries since last snapshot: {}", manager.entries_since_snapshot());
    println!();

    // Part 2: Create a snapshot
    println!("===========================================");
    println!("      Part 2: Creating Snapshot");
    println!("===========================================");
    println!();

    // Simulate Raft state
    let raft_index = 1000;
    let raft_term = 5;

    println!("Creating snapshot at Raft index={}, term={}", raft_index, raft_term);
    let metadata = manager.create_snapshot(raft_index, raft_term).await?;

    println!();
    println!("Snapshot created successfully!");
    println!("  Raft index:     {}", metadata.raft_index);
    println!("  Raft term:      {}", metadata.raft_term);
    println!("  Entry count:    {}", metadata.entry_count);
    println!("  File size:      {} bytes", metadata.file_size);
    println!("  Uncompressed:   {} bytes", metadata.uncompressed_size);
    println!(
        "  Compression:    {:.1}%",
        (1.0 - metadata.compression_ratio()) * 100.0
    );
    println!();

    // Part 3: List snapshots
    println!("===========================================");
    println!("      Part 3: Listing Snapshots");
    println!("===========================================");
    println!();

    let snapshots = manager.list_snapshots()?;
    println!("Found {} snapshot(s):", snapshots.len());
    for info in &snapshots {
        println!("  - {}", info.path.display());
        println!("    Raft index: {}", info.raft_index);
        println!("    Raft term:  {}", info.raft_term);
        println!("    Entries:    {}", info.entry_count);
        println!("    Size:       {} bytes", info.file_size);
    }
    println!();

    // Part 4: Clear cache and recover from snapshot
    println!("===========================================");
    println!("      Part 4: Recovery from Snapshot");
    println!("===========================================");
    println!();

    // Find the latest snapshot
    let latest = manager.find_latest_snapshot()?;
    if let Some(snapshot_info) = latest {
        println!("Clearing cache...");
        storage.invalidate_all();
        println!("Cache cleared. Entry count: {}", storage.entry_count());
        println!();

        println!("Loading snapshot from: {}", snapshot_info.path.display());
        let loaded_index = manager.load_snapshot(&snapshot_info.path).await?;
        println!("Snapshot loaded! Raft index restored to: {}", loaded_index);
        println!("Cache entry count after recovery: {}", storage.entry_count());
        println!();

        // Verify data was recovered
        println!("Verifying recovered data:");
        for (key, expected_value) in &test_data {
            let result = storage.get(&Bytes::from(*key)).await;
            match result {
                Some(value) => {
                    let recovered = String::from_utf8_lossy(&value);
                    if recovered == *expected_value {
                        println!("  '{}' = '{}' [OK]", key, recovered);
                    } else {
                        println!("  '{}' = '{}' [MISMATCH: expected '{}']", key, recovered, expected_value);
                    }
                }
                None => {
                    println!("  '{}' = (not found) [MISSING]", key);
                }
            }
        }
    } else {
        println!("No snapshot found to recover from.");
    }

    println!();

    // Part 5: Demonstrate manual snapshot reader
    println!("===========================================");
    println!("      Part 5: Manual Snapshot Inspection");
    println!("===========================================");
    println!();

    if let Some(info) = manager.find_latest_snapshot()? {
        let mut reader = SnapshotReader::open(&info.path)?;
        let header = reader.header();

        println!("Snapshot Header:");
        println!("  Magic:        MCRS (Moka Cache RS)");
        println!("  Version:      {}", header.version);
        println!("  Compressed:   {}", header.flags & 0x01 != 0);
        println!("  Raft index:   {}", header.raft_index);
        println!("  Raft term:    {}", header.raft_term);
        println!("  Timestamp:    {}", header.timestamp);
        println!("  Entry count:  {}", header.entry_count);
        println!();

        println!("First 5 entries:");
        let mut count = 0;
        while let Some(entry) = reader.read_entry()? {
            println!(
                "  [{}] key={:?} value_len={} expired={}",
                count,
                String::from_utf8_lossy(&entry.key),
                entry.value.len(),
                entry.is_expired()
            );
            count += 1;
            if count >= 5 {
                break;
            }
        }
    }

    println!();
    println!("===========================================");
    println!("      Cleanup");
    println!("===========================================");
    println!();

    // Cleanup example directory
    println!("Removing example checkpoint directory...");
    fs::remove_dir_all(checkpoint_dir)?;
    println!("Done!");
    println!();

    println!("===========================================");
    println!("      Summary");
    println!("===========================================");
    println!();
    println!("This example demonstrated:");
    println!("  1. Configuring checkpoint settings");
    println!("  2. Creating LZ4-compressed snapshots");
    println!("  3. Listing and finding snapshots");
    println!("  4. Recovering cache state from snapshots");
    println!("  5. Manually inspecting snapshot contents");
    println!();
    println!("In a production cluster, checkpoints are created");
    println!("automatically based on Raft log entry thresholds");
    println!("or time intervals.");

    Ok(())
}
