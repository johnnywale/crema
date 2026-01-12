//! Checkpoint module for creating and loading cache snapshots.
//!
//! This module provides functionality for persisting cache state to disk
//! and recovering from snapshots. Key features include:
//!
//! - LZ4 compression for efficient storage
//! - CRC32 checksums for data integrity
//! - TTL preservation across restarts
//! - Automatic old snapshot cleanup
//! - Configurable snapshot triggers (log threshold, time interval)
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    CheckpointManager                         │
//! │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
//! │  │ SnapshotWriter  │  │ SnapshotReader  │  │   Config    │ │
//! │  │ (compression)   │  │ (decompression) │  │  (triggers) │ │
//! │  └────────┬────────┘  └────────┬────────┘  └─────────────┘ │
//! │           │                    │                            │
//! │           ▼                    ▼                            │
//! │  ┌─────────────────────────────────────────────────────┐   │
//! │  │                  File Format                         │   │
//! │  │  Header (64B) + Data Block (LZ4) + CRC32 (4B)       │   │
//! │  └─────────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use distributed_cache::checkpoint::{CheckpointConfig, CheckpointManager};
//! use std::time::Duration;
//!
//! // Configure checkpointing
//! let config = CheckpointConfig::new("./checkpoints")
//!     .with_log_threshold(10_000)
//!     .with_time_interval(Duration::from_secs(300))
//!     .with_compression(true)
//!     .with_max_snapshots(3);
//!
//! // Create manager
//! let manager = CheckpointManager::new(config, storage)?;
//!
//! // Create snapshot
//! let metadata = manager.create_snapshot(raft_index, raft_term).await?;
//!
//! // Load latest snapshot on recovery
//! if let Some(info) = manager.find_latest_snapshot()? {
//!     manager.load_snapshot(&info.path).await?;
//! }
//! ```

mod format;
mod manager;
mod reader;
mod writer;

pub use format::{FormatError, SnapshotEntry, SnapshotHeader, HEADER_SIZE, MAGIC, VERSION};
pub use manager::{CheckpointConfig, CheckpointManager, RaftStateProvider, SnapshotInfo};
pub use reader::{SnapshotIterator, SnapshotReader};
pub use writer::{SnapshotMetadata, SnapshotWriter};
