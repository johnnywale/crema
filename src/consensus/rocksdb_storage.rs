//! RocksDB-based persistent storage implementation for Raft.
//!
//! This module provides a durable storage backend for Raft that persists
//! log entries, hard state, conf state, and snapshots to RocksDB.
//!
//! # Features
//!
//! - **Durability**: All writes are persisted to disk with configurable sync options
//! - **Crash Recovery**: State is automatically restored on restart
//! - **Compaction**: Log entries can be compacted to reclaim space
//! - **Column Families**: Uses separate column families for entries, state, and snapshots
//!
//! # Thread Safety
//!
//! This implementation uses `SingleThreaded` RocksDB mode, which means:
//! - Reads and writes are serialized at the RocksDB layer
//! - Safe for embedded Raft in a single-threaded executor
//! - Suitable for low-to-medium throughput use cases
//!
//! **Warning**: If you need high concurrent throughput with multiple Raft nodes
//! or heavy async workloads, consider using separate RocksDB instances per Raft
//! group or implementing a multi-threaded variant.
//!
//! # Column Family Layout
//!
//! - `entries`: Log entries keyed by index (big-endian u64)
//! - `state`: Hard state and conf state (versioned keys for migration safety)
//! - `snapshots`: Snapshot metadata and data
//!
//! # Cache Coherence
//!
//! All state mutations follow the pattern: write to RocksDB first, then update
//! in-memory cache. **Do not write directly to RocksDB without using the provided
//! methods**, as this will cause cache divergence and potential correctness issues.

#![cfg(feature = "rocksdb-storage")]

use crate::error::{Result, StorageError};
use parking_lot::RwLock;
use protobuf::Message as ProtoMessage;
use raft::prelude::*;
use raft::{RaftState, Storage};
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, Options,
    SingleThreaded, WriteBatch, WriteOptions,
};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Column family names
const CF_ENTRIES: &str = "entries";
const CF_STATE: &str = "state";
const CF_SNAPSHOTS: &str = "snapshots";

/// Key version prefix for future migration safety.
/// When schema changes are needed, increment this and add migration logic.
const KEY_VERSION: &str = "v1";

/// Keys for state storage (versioned for safe migrations)
const KEY_HARD_STATE: &[u8] = b"v1/hard_state";
const KEY_CONF_STATE: &[u8] = b"v1/conf_state";
const KEY_COMPACTED_INDEX: &[u8] = b"v1/compacted_index";
const KEY_COMPACTED_TERM: &[u8] = b"v1/compacted_term";

/// Keys for snapshot storage (versioned)
const KEY_SNAPSHOT: &[u8] = b"v1/snapshot";

/// Maximum capacity hint for pre-allocating entries vector.
const MAX_ENTRIES_CAPACITY_HINT: usize = 1024;

/// Warning threshold for snapshot size (100MB).
/// Snapshots larger than this will log a warning about potential performance impact.
const SNAPSHOT_SIZE_WARNING_THRESHOLD: usize = 100 * 1024 * 1024;

/// Configuration for RocksDB storage.
#[derive(Debug, Clone)]
pub struct RocksDbStorageConfig {
    /// Path to the RocksDB database directory.
    pub path: String,

    /// Whether to sync writes to disk immediately.
    /// Setting this to true provides stronger durability but lower performance.
    pub sync_writes: bool,

    /// Whether to create the database if it doesn't exist.
    pub create_if_missing: bool,

    /// Maximum number of open files for RocksDB.
    pub max_open_files: i32,

    /// Write buffer size in bytes.
    pub write_buffer_size: usize,

    /// Maximum number of write buffers.
    pub max_write_buffer_number: i32,
}

impl Default for RocksDbStorageConfig {
    fn default() -> Self {
        Self {
            path: "raft-storage".to_string(),
            sync_writes: true,
            create_if_missing: true,
            max_open_files: 1000,
            write_buffer_size: 64 * 1024 * 1024, // 64MB
            max_write_buffer_number: 3,
        }
    }
}

impl RocksDbStorageConfig {
    /// Create a new config with the specified path.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            ..Default::default()
        }
    }

    /// Set whether to sync writes.
    pub fn with_sync_writes(mut self, sync: bool) -> Self {
        self.sync_writes = sync;
        self
    }
}

/// RocksDB-based persistent storage for Raft.
///
/// This storage is cloneable - clones share the same underlying database,
/// which is required for RawNode and process_ready to see the same state.
#[derive(Clone)]
pub struct RocksDbStorage {
    inner: Arc<RocksDbStorageInner>,
}

struct RocksDbStorageInner {
    /// The RocksDB database instance.
    db: DBWithThreadMode<SingleThreaded>,

    /// Write options for durability.
    write_opts: WriteOptions,

    /// Cached hard state for fast reads.
    cached_hard_state: RwLock<HardState>,

    /// Cached conf state for fast reads.
    cached_conf_state: RwLock<ConfState>,

    /// Cached compacted index for fast reads.
    cached_compacted_index: RwLock<u64>,

    /// Cached compacted term for fast reads.
    cached_compacted_term: RwLock<u64>,

    /// Cached snapshot for fast reads.
    cached_snapshot: RwLock<Snapshot>,
}

impl RocksDbStorage {
    /// Create a new RocksDB storage with the given configuration.
    pub fn new(config: RocksDbStorageConfig) -> Result<Self> {
        let path = Path::new(&config.path);

        // Configure RocksDB options
        let mut opts = Options::default();
        opts.create_if_missing(config.create_if_missing);
        opts.create_missing_column_families(true);
        opts.set_max_open_files(config.max_open_files);
        opts.set_write_buffer_size(config.write_buffer_size);
        opts.set_max_write_buffer_number(config.max_write_buffer_number);

        // Configure column families
        let cf_entries = ColumnFamilyDescriptor::new(CF_ENTRIES, Options::default());
        let cf_state = ColumnFamilyDescriptor::new(CF_STATE, Options::default());
        let cf_snapshots = ColumnFamilyDescriptor::new(CF_SNAPSHOTS, Options::default());

        // Open database with column families
        let db = DBWithThreadMode::<SingleThreaded>::open_cf_descriptors(
            &opts,
            path,
            vec![cf_entries, cf_state, cf_snapshots],
        )
        .map_err(|e| StorageError::RocksDb(e.to_string()))?;

        // Configure write options
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(config.sync_writes);

        // Load cached state from database
        let (hard_state, conf_state, compacted_index, compacted_term, snapshot) =
            Self::load_state_from_db(&db)?;

        info!(
            path = %config.path,
            compacted_index,
            compacted_term,
            "RocksDB storage initialized"
        );

        Ok(Self {
            inner: Arc::new(RocksDbStorageInner {
                db,
                write_opts,
                cached_hard_state: RwLock::new(hard_state),
                cached_conf_state: RwLock::new(conf_state),
                cached_compacted_index: RwLock::new(compacted_index),
                cached_compacted_term: RwLock::new(compacted_term),
                cached_snapshot: RwLock::new(snapshot),
            }),
        })
    }

    /// Create storage with initial voters.
    pub fn new_with_conf_state(config: RocksDbStorageConfig, voters: Vec<u64>) -> Result<Self> {
        let storage = Self::new(config)?;

        let mut conf_state = ConfState::default();
        conf_state.voters = voters;
        storage.set_conf_state(conf_state)?;

        Ok(storage)
    }

    /// Load state from the database on startup.
    fn load_state_from_db(
        db: &DBWithThreadMode<SingleThreaded>,
    ) -> Result<(HardState, ConfState, u64, u64, Snapshot)> {
        let cf_state = db
            .cf_handle(CF_STATE)
            .ok_or_else(|| StorageError::RocksDb("State column family not found".to_string()))?;

        let cf_snapshots = db
            .cf_handle(CF_SNAPSHOTS)
            .ok_or_else(|| StorageError::RocksDb("Snapshots column family not found".to_string()))?;

        // Load hard state
        let hard_state = match db.get_cf(cf_state, KEY_HARD_STATE) {
            Ok(Some(data)) => HardState::parse_from_bytes(&data)
                .map_err(|e| StorageError::RocksDb(format!("Failed to parse hard state: {}", e)))?,
            Ok(None) => HardState::default(),
            Err(e) => return Err(StorageError::RocksDb(e.to_string()).into()),
        };

        // Load conf state
        let conf_state = match db.get_cf(cf_state, KEY_CONF_STATE) {
            Ok(Some(data)) => ConfState::parse_from_bytes(&data)
                .map_err(|e| StorageError::RocksDb(format!("Failed to parse conf state: {}", e)))?,
            Ok(None) => ConfState::default(),
            Err(e) => return Err(StorageError::RocksDb(e.to_string()).into()),
        };

        // Load compacted index
        let compacted_index = match db.get_cf(cf_state, KEY_COMPACTED_INDEX) {
            Ok(Some(data)) => {
                let bytes: [u8; 8] = data
                    .try_into()
                    .map_err(|_| StorageError::RocksDb("Invalid compacted index".to_string()))?;
                u64::from_be_bytes(bytes)
            }
            Ok(None) => 0,
            Err(e) => return Err(StorageError::RocksDb(e.to_string()).into()),
        };

        // Load compacted term
        let compacted_term = match db.get_cf(cf_state, KEY_COMPACTED_TERM) {
            Ok(Some(data)) => {
                let bytes: [u8; 8] = data
                    .try_into()
                    .map_err(|_| StorageError::RocksDb("Invalid compacted term".to_string()))?;
                u64::from_be_bytes(bytes)
            }
            Ok(None) => 0,
            Err(e) => return Err(StorageError::RocksDb(e.to_string()).into()),
        };

        // Load snapshot
        let snapshot = match db.get_cf(cf_snapshots, KEY_SNAPSHOT) {
            Ok(Some(data)) => Snapshot::parse_from_bytes(&data)
                .map_err(|e| StorageError::RocksDb(format!("Failed to parse snapshot: {}", e)))?,
            Ok(None) => Snapshot::default(),
            Err(e) => return Err(StorageError::RocksDb(e.to_string()).into()),
        };

        Ok((hard_state, conf_state, compacted_index, compacted_term, snapshot))
    }

    /// Get the entries column family.
    fn cf_entries(&self) -> &ColumnFamily {
        self.inner
            .db
            .cf_handle(CF_ENTRIES)
            .expect("Entries column family should exist")
    }

    /// Get the state column family.
    fn cf_state(&self) -> &ColumnFamily {
        self.inner
            .db
            .cf_handle(CF_STATE)
            .expect("State column family should exist")
    }

    /// Get the snapshots column family.
    fn cf_snapshots(&self) -> &ColumnFamily {
        self.inner
            .db
            .cf_handle(CF_SNAPSHOTS)
            .expect("Snapshots column family should exist")
    }

    /// Convert index to key bytes (big-endian for proper ordering).
    fn index_to_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }

    /// Convert key bytes to index.
    fn key_to_index(key: &[u8]) -> u64 {
        let bytes: [u8; 8] = key.try_into().expect("Key should be 8 bytes");
        u64::from_be_bytes(bytes)
    }

    /// Set the hard state.
    ///
    /// # Cache Coherence
    /// This method writes to RocksDB first, then updates the in-memory cache.
    /// Always use this method instead of writing directly to RocksDB.
    pub fn set_hard_state(&self, hs: HardState) -> Result<()> {
        let data = hs
            .write_to_bytes()
            .map_err(|e| StorageError::RocksDb(format!("Failed to serialize hard state: {}", e)))?;

        // Write to DB first
        self.inner
            .db
            .put_cf_opt(self.cf_state(), KEY_HARD_STATE, &data, &self.inner.write_opts)
            .map_err(|e| StorageError::RocksDb(e.to_string()))?;

        // Then update cache (after successful DB write)
        *self.inner.cached_hard_state.write() = hs;
        Ok(())
    }

    /// Set the conf state.
    ///
    /// # Cache Coherence
    /// This method writes to RocksDB first, then updates the in-memory cache.
    /// Always use this method instead of writing directly to RocksDB.
    pub fn set_conf_state(&self, cs: ConfState) -> Result<()> {
        let data = cs
            .write_to_bytes()
            .map_err(|e| StorageError::RocksDb(format!("Failed to serialize conf state: {}", e)))?;

        // Write to DB first
        self.inner
            .db
            .put_cf_opt(self.cf_state(), KEY_CONF_STATE, &data, &self.inner.write_opts)
            .map_err(|e| StorageError::RocksDb(e.to_string()))?;

        // Then update cache (after successful DB write)
        *self.inner.cached_conf_state.write() = cs;
        Ok(())
    }

    /// Append entries to the log.
    ///
    /// # Cache Coherence
    /// This method writes to RocksDB atomically. The log entries are not cached
    /// in memory (read directly from DB), so there's no cache to update.
    pub fn append(&self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let compacted_index = *self.inner.cached_compacted_index.read();
        let first_index = compacted_index + 1;
        let last_index = self.last_index_internal()?;
        let first_new = entries[0].index;

        // Debug assertion: compacted_index should be <= last_index
        debug_assert!(
            compacted_index <= last_index || last_index == compacted_index,
            "Compacted index {} should be <= last index {}",
            compacted_index,
            last_index
        );

        // 1. Check if entries are already compacted
        if first_new < first_index {
            return Err(StorageError::Compacted(first_index).into());
        }

        // 2. Check for log gaps
        if first_new > last_index + 1 {
            return Err(StorageError::LogGap {
                last_index,
                first_new,
            }
            .into());
        }

        // 3. Verify internal continuity
        for i in 1..entries.len() {
            if entries[i].index != entries[i - 1].index + 1 {
                return Err(StorageError::NonContiguous {
                    prev_index: entries[i - 1].index,
                    curr_index: entries[i].index,
                }
                .into());
            }
        }

        // 4. Handle overlapping entries by deleting conflicting ones
        if first_new <= last_index {
            // Delete entries from first_new to last_index
            let mut batch = WriteBatch::default();
            for idx in first_new..=last_index {
                batch.delete_cf(self.cf_entries(), Self::index_to_key(idx));
            }
            self.inner
                .db
                .write_opt(batch, &self.inner.write_opts)
                .map_err(|e| StorageError::RocksDb(e.to_string()))?;
        }

        // 5. Write new entries
        let mut batch = WriteBatch::default();
        for entry in entries {
            let key = Self::index_to_key(entry.index);
            let data = entry
                .write_to_bytes()
                .map_err(|e| StorageError::RocksDb(format!("Failed to serialize entry: {}", e)))?;
            batch.put_cf(self.cf_entries(), key, data);
        }
        self.inner
            .db
            .write_opt(batch, &self.inner.write_opts)
            .map_err(|e| StorageError::RocksDb(e.to_string()))?;

        debug!(
            first_new,
            last_new = entries.last().map(|e| e.index).unwrap_or(0),
            count = entries.len(),
            "Appended entries to RocksDB"
        );

        Ok(())
    }

    /// Apply a snapshot.
    ///
    /// # Cache Coherence
    /// This method writes to RocksDB first, then updates in-memory caches.
    /// Do not call RocksDB directly - use this method to maintain consistency.
    pub fn apply_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let index = snapshot.get_metadata().index;
        let term = snapshot.get_metadata().term;

        // Debug assertions for invariants
        debug_assert!(
            term > 0 || index == 0,
            "Non-zero snapshot index {} requires non-zero term",
            index
        );

        // Update conf state (writes to DB, then cache)
        let conf_state = snapshot.get_metadata().get_conf_state().clone();
        self.set_conf_state(conf_state)?;

        // Update hard state if needed
        {
            let mut hs = self.inner.cached_hard_state.write();
            if hs.commit < index {
                hs.commit = index;
            }
            if hs.term < term {
                hs.term = term;
            }
            let data = hs
                .write_to_bytes()
                .map_err(|e| StorageError::RocksDb(format!("Failed to serialize hard state: {}", e)))?;
            // Write to DB first
            self.inner
                .db
                .put_cf_opt(self.cf_state(), KEY_HARD_STATE, &data, &self.inner.write_opts)
                .map_err(|e| StorageError::RocksDb(e.to_string()))?;
            // Cache is updated via the mutable reference above
        }

        // Delete all entries (we'll rebuild from snapshot)
        let compacted_index = *self.inner.cached_compacted_index.read();
        let last_index = self.last_index_internal()?;

        // Debug assertion: snapshot index should be >= compacted index
        debug_assert!(
            index >= compacted_index,
            "Snapshot index {} should be >= compacted index {}",
            index,
            compacted_index
        );

        let mut batch = WriteBatch::default();
        for idx in (compacted_index + 1)..=last_index {
            batch.delete_cf(self.cf_entries(), Self::index_to_key(idx));
        }

        // Update compacted index and term
        batch.put_cf(
            self.cf_state(),
            KEY_COMPACTED_INDEX,
            index.to_be_bytes(),
        );
        batch.put_cf(
            self.cf_state(),
            KEY_COMPACTED_TERM,
            term.to_be_bytes(),
        );

        // Serialize and store snapshot
        let snapshot_data = snapshot
            .write_to_bytes()
            .map_err(|e| StorageError::RocksDb(format!("Failed to serialize snapshot: {}", e)))?;

        // Warn if snapshot is large (potential performance impact)
        let snapshot_size = snapshot_data.len();
        if snapshot_size > SNAPSHOT_SIZE_WARNING_THRESHOLD {
            warn!(
                snapshot_size_bytes = snapshot_size,
                threshold_bytes = SNAPSHOT_SIZE_WARNING_THRESHOLD,
                index,
                "Large snapshot detected. Consider implementing incremental snapshots \
                 or chunked storage for better performance."
            );
        }

        batch.put_cf(self.cf_snapshots(), KEY_SNAPSHOT, snapshot_data);

        // Write batch to DB first (atomic)
        self.inner
            .db
            .write_opt(batch, &self.inner.write_opts)
            .map_err(|e| StorageError::RocksDb(e.to_string()))?;

        // Then update caches (after successful DB write)
        *self.inner.cached_compacted_index.write() = index;
        *self.inner.cached_compacted_term.write() = term;
        *self.inner.cached_snapshot.write() = snapshot;

        info!(index, term, snapshot_size_bytes = snapshot_size, "Applied snapshot to RocksDB");

        Ok(())
    }

    /// Compact the log up to the given index.
    ///
    /// # Cache Coherence
    /// This method writes to RocksDB first, then updates in-memory caches.
    /// Do not call RocksDB directly - use this method to maintain consistency.
    pub fn compact(&self, compact_index: u64) -> Result<()> {
        let compacted_index = *self.inner.cached_compacted_index.read();

        // Already compacted (idempotent)
        if compact_index <= compacted_index {
            return Ok(());
        }

        let last_index = self.last_index_internal()?;

        // Debug assertions for invariants
        debug_assert!(
            compacted_index <= last_index,
            "Compacted index {} should be <= last index {}",
            compacted_index,
            last_index
        );

        if compact_index > last_index {
            return Err(StorageError::EntryNotFound(compact_index).into());
        }

        // Get the term at compact_index before deleting
        let compact_term = self.term_internal(compact_index)?;

        // Debug assertion: term should be non-zero for non-zero index
        debug_assert!(
            compact_term != 0 || compact_index == 0,
            "Non-zero compact index {} requires non-zero term",
            compact_index
        );

        // Delete entries before compact_index
        let mut batch = WriteBatch::default();
        for idx in (compacted_index + 1)..compact_index {
            batch.delete_cf(self.cf_entries(), Self::index_to_key(idx));
        }

        // Update compacted index and term
        batch.put_cf(
            self.cf_state(),
            KEY_COMPACTED_INDEX,
            compact_index.to_be_bytes(),
        );
        batch.put_cf(
            self.cf_state(),
            KEY_COMPACTED_TERM,
            compact_term.to_be_bytes(),
        );

        // Write to DB first (atomic)
        self.inner
            .db
            .write_opt(batch, &self.inner.write_opts)
            .map_err(|e| StorageError::RocksDb(e.to_string()))?;

        // Update caches
        *self.inner.cached_compacted_index.write() = compact_index;
        *self.inner.cached_compacted_term.write() = compact_term;

        debug!(compact_index, compact_term, "Compacted log in RocksDB");

        Ok(())
    }

    /// Get the last index in the log.
    pub fn last_index(&self) -> u64 {
        self.last_index_internal().unwrap_or_else(|_| {
            *self.inner.cached_compacted_index.read()
        })
    }

    /// Internal last_index that can return errors.
    fn last_index_internal(&self) -> Result<u64> {
        let compacted_index = *self.inner.cached_compacted_index.read();

        // Iterate in reverse to find the last entry
        let iter = self.inner.db.iterator_cf(self.cf_entries(), IteratorMode::End);

        for item in iter {
            match item {
                Ok((key, _)) => {
                    return Ok(Self::key_to_index(&key));
                }
                Err(e) => {
                    warn!(error = %e, "Error iterating entries");
                }
            }
        }

        // No entries, return compacted index
        Ok(compacted_index)
    }

    /// Get the first index in the log.
    pub fn first_index(&self) -> u64 {
        *self.inner.cached_compacted_index.read() + 1
    }

    /// Get the compacted index.
    pub fn compacted_index(&self) -> u64 {
        *self.inner.cached_compacted_index.read()
    }

    /// Get the number of entries in the log.
    pub fn entry_count(&self) -> usize {
        let compacted_index = *self.inner.cached_compacted_index.read();
        let last_index = self.last_index_internal().unwrap_or(compacted_index);
        (last_index - compacted_index) as usize
    }

    /// Get term at a specific index (internal).
    fn term_internal(&self, idx: u64) -> Result<u64> {
        let compacted_index = *self.inner.cached_compacted_index.read();

        if idx < compacted_index {
            return Err(StorageError::Compacted(compacted_index + 1).into());
        }

        if idx == compacted_index {
            return Ok(*self.inner.cached_compacted_term.read());
        }

        // Read from database
        let key = Self::index_to_key(idx);
        match self.inner.db.get_cf(self.cf_entries(), key) {
            Ok(Some(data)) => {
                let entry = Entry::parse_from_bytes(&data)
                    .map_err(|e| StorageError::RocksDb(format!("Failed to parse entry: {}", e)))?;
                Ok(entry.term)
            }
            Ok(None) => Err(StorageError::EntryNotFound(idx).into()),
            Err(e) => Err(StorageError::RocksDb(e.to_string()).into()),
        }
    }

    /// Set a snapshot for future `Storage::snapshot()` calls.
    ///
    /// Unlike `apply_snapshot` (which is called by followers receiving a snapshot),
    /// this method is called by the leader to store a locally-created snapshot that
    /// can be sent to lagging followers via InstallSnapshot RPC.
    ///
    /// This method:
    /// - Stores the snapshot for `Storage::snapshot()` to return
    /// - Does NOT reset the log (entries are preserved)
    /// - Does NOT modify hard state or conf state
    ///
    /// # Cache Coherence
    /// This method writes to RocksDB first, then updates in-memory cache.
    ///
    /// # Arguments
    /// * `snapshot` - The snapshot to store, with metadata and state machine data
    pub fn set_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let index = snapshot.get_metadata().index;
        let term = snapshot.get_metadata().term;

        // Serialize snapshot
        let snapshot_data = snapshot
            .write_to_bytes()
            .map_err(|e| StorageError::RocksDb(format!("Failed to serialize snapshot: {}", e)))?;

        let snapshot_size = snapshot_data.len();
        if snapshot_size > SNAPSHOT_SIZE_WARNING_THRESHOLD {
            tracing::warn!(
                snapshot_size_bytes = snapshot_size,
                threshold_bytes = SNAPSHOT_SIZE_WARNING_THRESHOLD,
                "Large snapshot stored for InstallSnapshot. Consider increasing snapshot frequency."
            );
        }

        // Write to DB first
        self.inner
            .db
            .put_cf(self.cf_snapshots(), KEY_SNAPSHOT, snapshot_data)
            .map_err(|e| StorageError::RocksDb(e.to_string()))?;

        // Then update cache
        *self.inner.cached_snapshot.write() = snapshot;

        tracing::info!(index, term, snapshot_size_bytes = snapshot_size, "Stored snapshot for InstallSnapshot");

        Ok(())
    }

    /// Get the current snapshot (for testing/inspection).
    pub fn get_snapshot(&self) -> Snapshot {
        self.inner.cached_snapshot.read().clone()
    }

    /// Flush all pending writes to disk.
    pub fn flush(&self) -> Result<()> {
        self.inner
            .db
            .flush()
            .map_err(|e| StorageError::RocksDb(e.to_string()))?;
        Ok(())
    }
}

impl Storage for RocksDbStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState {
            hard_state: self.inner.cached_hard_state.read().clone(),
            conf_state: self.inner.cached_conf_state.read().clone(),
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: raft::GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let compacted_index = *self.inner.cached_compacted_index.read();
        let first_index = compacted_index + 1;

        if low < first_index {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        let last_index = self
            .last_index_internal()
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;

        if high > last_index + 1 {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        let max_size = max_size.into().unwrap_or(u64::MAX);
        let requested_count = (high - low) as usize;
        let capacity = std::cmp::min(requested_count, MAX_ENTRIES_CAPACITY_HINT);
        let mut result = Vec::with_capacity(capacity);
        let mut current_size: u64 = 0;

        for idx in low..high {
            let key = Self::index_to_key(idx);
            match self.inner.db.get_cf(self.cf_entries(), key) {
                Ok(Some(data)) => {
                    let entry = Entry::parse_from_bytes(&data)
                        .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;

                    let entry_size = entry.compute_size() as u64;
                    if !result.is_empty() && current_size + entry_size > max_size {
                        break;
                    }

                    result.push(entry);
                    current_size += entry_size;
                }
                Ok(None) => {
                    return Err(raft::Error::Store(raft::StorageError::Unavailable));
                }
                Err(_) => {
                    return Err(raft::Error::Store(raft::StorageError::Unavailable));
                }
            }
        }

        Ok(result)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let compacted_index = *self.inner.cached_compacted_index.read();

        if idx < compacted_index {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        if idx == compacted_index {
            return Ok(*self.inner.cached_compacted_term.read());
        }

        let key = Self::index_to_key(idx);
        match self.inner.db.get_cf(self.cf_entries(), key) {
            Ok(Some(data)) => {
                let entry = Entry::parse_from_bytes(&data)
                    .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;
                Ok(entry.term)
            }
            Ok(None) => Err(raft::Error::Store(raft::StorageError::Unavailable)),
            Err(_) => Err(raft::Error::Store(raft::StorageError::Unavailable)),
        }
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(*self.inner.cached_compacted_index.read() + 1)
    }

    fn last_index(&self) -> raft::Result<u64> {
        self.last_index_internal()
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        let snapshot = self.inner.cached_snapshot.read();
        if snapshot.get_metadata().index == 0 {
            return Err(raft::Error::Store(
                raft::StorageError::SnapshotTemporarilyUnavailable,
            ));
        }
        Ok(snapshot.clone())
    }
}

impl std::fmt::Debug for RocksDbStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksDbStorage")
            .field("compacted_index", &self.compacted_index())
            .field("first_index", &self.first_index())
            .field("last_index", &self.last_index())
            .field("entry_count", &self.entry_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn create_test_storage() -> (RocksDbStorage, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = RocksDbStorageConfig::new(temp_dir.path().to_str().unwrap());
        let storage = RocksDbStorage::new(config).unwrap();
        (storage, temp_dir)
    }

    #[test]
    fn test_initial_state() {
        let temp_dir = TempDir::new().unwrap();
        let config = RocksDbStorageConfig::new(temp_dir.path().to_str().unwrap());
        let storage = RocksDbStorage::new_with_conf_state(config, vec![1, 2, 3]).unwrap();

        let state = storage.initial_state().unwrap();
        assert_eq!(state.conf_state.voters, vec![1, 2, 3]);
    }

    #[test]
    fn test_append_entries() {
        let (storage, _temp_dir) = create_test_storage();

        let entries: Vec<Entry> = (1..=5)
            .map(|i| {
                let mut e = Entry::default();
                e.index = i;
                e.term = 1;
                e
            })
            .collect();

        storage.append(&entries).unwrap();
        assert_eq!(storage.last_index(), 5);
        assert_eq!(storage.first_index(), 1);
        assert_eq!(storage.entry_count(), 5);
    }

    #[test]
    fn test_append_with_overlap() {
        let (storage, _temp_dir) = create_test_storage();

        // Append entries 1-5
        let entries1: Vec<Entry> = (1..=5)
            .map(|i| {
                let mut e = Entry::default();
                e.index = i;
                e.term = 1;
                e
            })
            .collect();
        storage.append(&entries1).unwrap();

        // Append entries 3-7 with different term
        let entries2: Vec<Entry> = (3..=7)
            .map(|i| {
                let mut e = Entry::default();
                e.index = i;
                e.term = 2;
                e
            })
            .collect();
        storage.append(&entries2).unwrap();

        assert_eq!(storage.last_index(), 7);
        assert_eq!(storage.entry_count(), 7);
        assert_eq!(Storage::term(&storage, 3).unwrap(), 2);
    }

    #[test]
    fn test_compact() {
        let (storage, _temp_dir) = create_test_storage();

        let entries: Vec<Entry> = (1..=10)
            .map(|i| {
                let mut e = Entry::default();
                e.index = i;
                e.term = 1;
                e
            })
            .collect();

        storage.append(&entries).unwrap();
        storage.compact(5).unwrap();

        assert_eq!(storage.compacted_index(), 5);
        assert_eq!(storage.first_index(), 6);
        assert_eq!(storage.last_index(), 10);
        assert_eq!(storage.entry_count(), 5);
    }

    #[test]
    fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_str().unwrap().to_string();

        // Create storage and write data
        {
            let config = RocksDbStorageConfig::new(&path);
            let storage = RocksDbStorage::new(config).unwrap();

            let entries: Vec<Entry> = (1..=5)
                .map(|i| {
                    let mut e = Entry::default();
                    e.index = i;
                    e.term = 1;
                    e.data = Bytes::from(format!("data-{}", i));
                    e
                })
                .collect();

            storage.append(&entries).unwrap();
            storage.set_hard_state(HardState {
                term: 1,
                vote: 2,
                commit: 3,
                ..Default::default()
            }).unwrap();
            storage.flush().unwrap();
        }

        // Reopen and verify data
        {
            let config = RocksDbStorageConfig::new(&path);
            let storage = RocksDbStorage::new(config).unwrap();

            assert_eq!(storage.last_index(), 5);
            assert_eq!(storage.first_index(), 1);

            let state = storage.initial_state().unwrap();
            assert_eq!(state.hard_state.term, 1);
            assert_eq!(state.hard_state.vote, 2);
            assert_eq!(state.hard_state.commit, 3);

            // Verify entries
            let retrieved = storage
                .entries(1, 6, None, raft::GetEntriesContext::empty(false))
                .unwrap();
            assert_eq!(retrieved.len(), 5);
            assert_eq!(retrieved[0].data, Bytes::from("data-1"));
        }
    }

    #[test]
    fn test_apply_snapshot() {
        let (storage, _temp_dir) = create_test_storage();

        // Add some entries
        let entries: Vec<Entry> = (1..=10)
            .map(|i| {
                let mut e = Entry::default();
                e.index = i;
                e.term = 1;
                e
            })
            .collect();
        storage.append(&entries).unwrap();

        // Apply snapshot
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 7;
        snapshot.mut_metadata().term = 2;

        storage.apply_snapshot(snapshot).unwrap();

        assert_eq!(storage.compacted_index(), 7);
        assert_eq!(storage.first_index(), 8);
        assert_eq!(Storage::term(&storage, 7).unwrap(), 2);
    }
}
