//! Example demonstrating dynamic shard management with slot-based routing.
//!
//! This example shows how to:
//! - Enable slot-based routing for dynamic shard management
//! - Add a new shard at runtime
//! - Remove a shard at runtime
//! - Monitor migration progress
//!
//! # Key Concepts
//!
//! - **Slots**: Fixed number (1024) of logical partitions. Keys are mapped to slots
//!   via `crc16(key) % 1024`.
//! - **Epoch**: Version number that increments on any slot table change.
//! - **MOVED redirect**: Permanent redirect when slot ownership changes.
//! - **ASK redirect**: Temporary redirect during migration.
//!
//! Run this example with:
//!   RUST_LOG=info cargo run --example dynamic-shards

use crema::multiraft::{MigrationStatus, TOTAL_SLOTS};
use crema::{CacheConfig, DistributedCache, MultiRaftCacheConfig, RaftConfig};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║       Dynamic Shard Management with Slot Routing          ║");
    println!("╚═══════════════════════════════════════════════════════════╝");
    println!();

    // Configure Multi-Raft with 4 initial shards
    let initial_shards = 4;

    let multiraft_config = MultiRaftCacheConfig {
        enabled: true,
        num_shards: initial_shards,
        auto_init_shards: true,
        ..Default::default()
    };

    let raft_config = RaftConfig {
        election_tick: 10,
        heartbeat_tick: 3,
        tick_interval_ms: 100,
        ..Default::default()
    };

    let config = CacheConfig::new(1, "127.0.0.1:9001".parse()?)
        .with_raft_config(raft_config)
        .with_multiraft_config(multiraft_config);

    println!(
        "Step 1: Creating cache with {} initial shards...",
        initial_shards
    );
    let cache = DistributedCache::new(config).await?;

    // Wait for shards to initialize
    sleep(Duration::from_millis(500)).await;

    // Enable slot-based routing
    println!("Step 2: Enabling slot-based routing...");
    cache.enable_slot_routing().await?;

    // Start the migration background loop
    cache.start_slot_migration();

    // Get initial slot table snapshot
    print_slot_table(&cache);

    // Insert some test data
    println!("\nStep 3: Inserting test data...");
    for i in 0..100 {
        let key = format!("key-{}", i);
        let value = format!("value-{}", i);
        cache.put(key, value).await?;
    }
    println!("  Inserted 100 key-value pairs");

    // Show slot distribution
    print_slot_distribution(&cache);

    // Add a new shard
    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║                    Adding New Shard                        ║");
    println!("╚═══════════════════════════════════════════════════════════╝");

    println!("\nStep 4: Adding a 5th shard...");
    let add_result = cache.add_shard().await?;

    println!("  New shard ID: {}", add_result.shard_id);
    println!("  Slots assigned: {}", add_result.slots_assigned);
    println!("  New epoch: {}", add_result.new_epoch.value());
    println!("  Slot moves: {}", add_result.reassignment.len());

    // Print updated slot table
    print_slot_table(&cache);

    // Monitor migration progress
    println!("\nStep 5: Monitoring migration...");
    for _ in 0..5 {
        if let Some(status) = cache.slot_migration_status() {
            print_migration_status(&status);
        }
        sleep(Duration::from_millis(200)).await;
    }

    // Verify data is still accessible
    println!("\nStep 6: Verifying data accessibility...");
    let mut accessible = 0;
    for i in 0..100 {
        let key = format!("key-{}", i);
        if cache.get(key.as_bytes()).await.is_some() {
            accessible += 1;
        }
    }
    println!("  Accessible keys: {}/100", accessible);

    // Remove a shard
    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║                    Removing Shard                          ║");
    println!("╚═══════════════════════════════════════════════════════════╝");

    println!("\nStep 7: Removing shard 3...");
    let remove_result = cache.remove_shard(3).await?;

    println!("  Shard removed: {}", remove_result.shard_id);
    println!(
        "  Slots redistributed: {}",
        remove_result.slots_to_redistribute
    );
    println!("  New epoch: {}", remove_result.new_epoch.value());
    println!("  Redistribution:");
    for (target, slots) in &remove_result.reassignments {
        println!("    → Shard {}: {} slots", target, slots.len());
    }

    // Print final slot table
    print_slot_table(&cache);

    // Monitor migration progress
    println!("\nStep 8: Monitoring drain migration...");
    for _ in 0..5 {
        if let Some(status) = cache.slot_migration_status() {
            print_migration_status(&status);
        }
        sleep(Duration::from_millis(200)).await;
    }

    // Final verification
    println!("\nStep 9: Final verification...");
    let mut final_accessible = 0;
    for i in 0..100 {
        let key = format!("key-{}", i);
        if cache.get(key.as_bytes()).await.is_some() {
            final_accessible += 1;
        }
    }
    println!("  Accessible keys: {}/100", final_accessible);

    // Stop migration and shutdown
    println!("\nStep 10: Shutting down...");
    cache.stop_slot_migration();
    cache.shutdown().await;

    println!("\n✓ Dynamic shard management example completed successfully!");

    Ok(())
}

fn print_slot_table(cache: &DistributedCache) {
    println!("\n┌─────────────────────────────────────────────────────────────┐");
    println!("│                     Slot Table Snapshot                      │");
    println!("├─────────────────────────────────────────────────────────────┤");

    if let Some(snapshot) = cache.slot_table() {
        println!(
            "│ Epoch: {:6}  │  Shards: {:3}  │  Total Slots: {:4}       │",
            snapshot.epoch.value(),
            snapshot.num_shards,
            TOTAL_SLOTS
        );
        println!("├─────────────────────────────────────────────────────────────┤");

        println!(
            "│ Shard │ Owned │ Incoming │ Outgoing │ Per-Shard Avg: {:3} │",
            TOTAL_SLOTS / snapshot.num_shards.max(1)
        );
        println!("├───────┼───────┼──────────┼──────────┼────────────────────┤");

        let mut shard_ids: Vec<_> = snapshot.shard_info.keys().collect();
        shard_ids.sort();

        for shard_id in shard_ids {
            if let Some(info) = snapshot.shard_info.get(shard_id) {
                println!(
                    "│   {:2}  │  {:4} │    {:4}  │    {:4}  │                    │",
                    info.shard_id,
                    info.owned_slots.len(),
                    info.incoming_slots.len(),
                    info.outgoing_slots.len()
                );
            }
        }
    } else {
        println!("│ Slot routing not enabled                                    │");
    }

    println!("└─────────────────────────────────────────────────────────────┘");
}

fn print_slot_distribution(cache: &DistributedCache) {
    println!("\n┌─────────────────────────────────────────────────────────────┐");
    println!("│                   Slot Distribution Check                     │");
    println!("├─────────────────────────────────────────────────────────────┤");

    if let Some(snapshot) = cache.slot_table() {
        // Count slots per shard
        let mut stable = 0;
        let mut migrating = 0;
        let mut imported = 0;

        for slot in &snapshot.slots {
            match &slot.state {
                crema::multiraft::SlotState::Stable => stable += 1,
                crema::multiraft::SlotState::Migrating { .. } => migrating += 1,
                crema::multiraft::SlotState::Imported { .. } => imported += 1,
            }
        }

        println!(
            "│ Stable: {:5}  │  Migrating: {:4}  │  Imported: {:4}       │",
            stable, migrating, imported
        );
    }

    println!("└─────────────────────────────────────────────────────────────┘");
}

fn print_migration_status(status: &MigrationStatus) {
    println!(
        "  Migration: active={}, completed={}, failed={}, keys={}",
        status.active_migrations,
        status.completed_migrations,
        status.failed_migrations,
        status.total_keys_migrated
    );

    if !status.by_phase.is_empty() {
        print!("    Phases: ");
        for (phase, count) in &status.by_phase {
            print!("{}={} ", phase, count);
        }
        println!();
    }
}
