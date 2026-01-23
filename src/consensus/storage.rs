//! In-memory storage implementation for Raft.

use crate::error::{Result, StorageError};
use parking_lot::RwLock;
use protobuf::Message as ProtoMessage;
use raft::prelude::*;
use raft::{RaftState, Storage};
use std::sync::Arc;

/// Maximum capacity hint for pre-allocating entries vector.
/// Prevents excessive memory allocation when high-low is large but max_size is small.
const MAX_ENTRIES_CAPACITY_HINT: usize = 1024;

/// In-memory storage for Raft log entries and state.
///
/// This storage is cloneable - clones share the same underlying data,
/// which is required for RawNode and process_ready to see the same state.
///
/// # Index Mapping
/// The entries vector uses a simple mapping: `entries[i]` contains the entry at logical index `i`.
/// - `entries[0]` is always a dummy entry representing the compacted boundary
/// - For logical index `idx`, the physical position is `idx - entries[0].index`
/// - This ensures O(1) index calculation and prevents off-by-one errors
///
/// # Invariants
/// - Log entries are always contiguous (no gaps)
/// - `entries[0]` is the single source of truth for compaction boundary
/// - All operations maintain consistency between entries and metadata
#[derive(Clone)]
pub struct MemStorage {
    inner: Arc<RwLock<MemStorageCore>>,
}

struct MemStorageCore {
    /// The current hard state (term, vote, commit).
    hard_state: HardState,

    /// The current conf state (voters, learners).
    conf_state: ConfState,

    /// Log entries with consistent index mapping.
    ///
    /// Invariants:
    /// - `entries[0]` is always a dummy entry at the compaction boundary
    /// - `entries[0].index` represents the last compacted index
    /// - For any entry at logical index `i`, its position is `i - entries[0].index`
    /// - All entries are contiguous (no gaps in indices)
    entries: Vec<Entry>,

    /// The current snapshot.
    snapshot: Snapshot,
}

impl Default for MemStorageCore {
    fn default() -> Self {
        Self {
            hard_state: HardState::default(),
            conf_state: ConfState::default(),
            entries: vec![Entry::default()], // Dummy entry at index 0
            snapshot: Snapshot::default(),
        }
    }
}

impl MemStorage {
    /// Create a new in-memory storage.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MemStorageCore::default())),
        }
    }

    /// Create storage with initial voters.
    pub fn new_with_conf_state(voters: Vec<u64>) -> Self {
        let mut core = MemStorageCore::default();
        core.conf_state = ConfState {
            voters,
            ..Default::default()
        };
        Self {
            inner: Arc::new(RwLock::new(core)),
        }
    }

    /// Set the hard state.
    pub fn set_hard_state(&self, hs: HardState) {
        self.inner.write().hard_state = hs;
    }

    /// Set the conf state.
    pub fn set_conf_state(&self, cs: ConfState) {
        self.inner.write().conf_state = cs;
    }

    /// Append entries to the log.
    ///
    /// # Guarantees
    /// - Ensures log continuity (no gaps)
    /// - Handles overlapping entries correctly by truncating
    /// - Rejects entries that are already compacted
    /// - Validates internal continuity of new entries
    ///
    /// # Errors
    /// - `StorageError::Compacted` - Entries are before the compacted index
    /// - `StorageError::LogGap` - There's a gap between existing log and new entries
    /// - `StorageError::NonContiguous` - New entries themselves are not contiguous
    ///
    /// # Index Calculation
    /// `entries[0]` is the dummy at `compacted_index`.
    /// Entry at logical index `i` is at physical position `i - compacted_index`.
    pub fn append(&self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut core = self.inner.write();
        let compacted_index = core.compacted_index_internal();
        let first_index = core.first_index_internal();
        let last_index = core.last_index_internal();
        let first_new = entries[0].index;

        // 1. Check if entries are already compacted
        if first_new < first_index {
            return Err(StorageError::Compacted(first_index).into());
        }

        // 2. CRITICAL: Check for log gaps (ensures continuity)
        // New entries must be contiguous with existing log
        if first_new > last_index + 1 {
            // Return error instead of panic for better error handling in production
            return Err(StorageError::LogGap {
                last_index,
                first_new,
            }
                .into());
        }

        // 3. Verify internal continuity of new entries
        for i in 1..entries.len() {
            if entries[i].index != entries[i - 1].index + 1 {
                return Err(StorageError::NonContiguous {
                    prev_index: entries[i - 1].index,
                    curr_index: entries[i].index,
                }
                    .into());
            }
        }

        // 4. Handle overlapping entries by truncating at the overlap point
        if first_new <= last_index {
            // Calculate how many entries to keep
            let keep_len = (first_new - compacted_index) as usize;

            // Defensive check: ensure we don't truncate the dummy entry
            if keep_len == 0 {
                // This would only happen if first_new == compacted_index,
                // but that should be caught by the first_index check above
                return Err(StorageError::Compacted(first_index).into());
            }

            core.entries.truncate(keep_len);
        }

        // 5. Append new entries
        core.entries.extend_from_slice(entries);

        Ok(())
    }

    /// Apply a snapshot.
    ///
    /// This resets the log to match the snapshot, discarding all previous entries.
    /// It also updates the hard state's commit index to ensure consistency.
    pub fn apply_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let mut core = self.inner.write();

        let index = snapshot.get_metadata().index;
        let term = snapshot.get_metadata().term;

        // Update conf state from snapshot
        core.conf_state = snapshot.get_metadata().get_conf_state().clone();

        // Update hard state to ensure consistency
        // When applying a snapshot, we should advance commit to at least the snapshot index
        if core.hard_state.commit < index {
            core.hard_state.commit = index;
        }

        // Update term if snapshot is from a newer term
        // This ensures we don't have stale term information
        if core.hard_state.term < term {
            core.hard_state.term = term;
        }

        // Reset entries with new dummy at snapshot index
        // This is the single source of truth for compaction boundary
        core.entries.clear();
        let mut dummy = Entry::default();
        dummy.index = index;
        dummy.term = term;
        dummy.entry_type = EntryType::EntryNormal;
        // Ensure dummy has no data to avoid memory waste
        dummy.data.clear();
        dummy.context.clear();
        core.entries.push(dummy);

        // Store snapshot
        core.snapshot = snapshot;

        Ok(())
    }

    /// Compact the log up to the given index.
    ///
    /// After compaction, `entries[0]` will be a clean dummy entry at `compact_index`.
    /// All data before this index is discarded to free memory.
    ///
    /// # Idempotency
    /// Calling compact with the same or earlier index is a no-op.
    pub fn compact(&self, compact_index: u64) -> Result<()> {
        let mut core = self.inner.write();
        let compacted_index = core.compacted_index_internal();

        // Already compacted to this index or beyond (idempotent)
        if compact_index <= compacted_index {
            return Ok(());
        }

        let last_index = core.last_index_internal();
        if compact_index > last_index {
            return Err(StorageError::EntryNotFound(compact_index).into());
        }

        // Calculate how many entries to remove
        // entries[0] is at compacted_index, entry at compact_index is at (compact_index - compacted_index)
        let skip = (compact_index - compacted_index) as usize;

        // Get the term at compact_index before draining
        let compact_term = core.entries[skip].term;

        // Remove entries before compact_index
        core.entries.drain(..skip);

        // Now entries[0] is at compact_index, convert it to a clean dummy
        // This is critical for memory efficiency and data consistency
        if let Some(dummy) = core.entries.first_mut() {
            // Preserve logical index and term, but clear all data
            dummy.index = compact_index;
            dummy.term = compact_term;
            dummy.entry_type = EntryType::EntryNormal;
            // Clear data fields to free memory (critical for preventing leaks)
            dummy.data.clear();
            dummy.context.clear();
        }

        Ok(())
    }

    /// Get the last index in the log.
    pub fn last_index(&self) -> u64 {
        self.inner.read().last_index_internal()
    }

    /// Get the first index in the log.
    pub fn first_index(&self) -> u64 {
        self.inner.read().first_index_internal()
    }

    /// Get the compacted index (the dummy entry).
    pub fn compacted_index(&self) -> u64 {
        self.inner.read().compacted_index_internal()
    }

    /// Get the number of entries in the log (excluding the dummy entry).
    pub fn entry_count(&self) -> usize {
        let core = self.inner.read();
        core.entries.len().saturating_sub(1)
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
    /// # Arguments
    /// * `snapshot` - The snapshot to store, with metadata and state machine data
    pub fn set_snapshot(&self, snapshot: Snapshot) {
        let mut core = self.inner.write();
        core.snapshot = snapshot;
    }

    /// Get the current snapshot (for testing/inspection).
    pub fn get_snapshot(&self) -> Snapshot {
        self.inner.read().snapshot.clone()
    }
}

impl MemStorageCore {
    /// Get the compacted index from entries[0] (single source of truth).
    #[inline]
    fn compacted_index_internal(&self) -> u64 {
        self.entries
            .first()
            .map(|e| e.index)
            .unwrap_or(0)
    }

    /// Get the compacted term from entries[0] (single source of truth).
    #[inline]
    fn compacted_term_internal(&self) -> u64 {
        self.entries
            .first()
            .map(|e| e.term)
            .unwrap_or(0)
    }

    /// Get the first available index (internal, lock-free).
    ///
    /// The first available index is always compacted_index + 1,
    /// since entries[0] is a dummy at compacted_index.
    #[inline]
    fn first_index_internal(&self) -> u64 {
        self.compacted_index_internal() + 1
    }

    /// Get the last index in the log (internal, lock-free).
    #[inline]
    fn last_index_internal(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.index)
            .unwrap_or(self.compacted_index_internal())
    }
}

impl Default for MemStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let core = self.inner.read();
        Ok(RaftState {
            hard_state: core.hard_state.clone(),
            conf_state: core.conf_state.clone(),
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: raft::GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let core = self.inner.read();

        let compacted_index = core.compacted_index_internal();
        let first_index = core.first_index_internal();
        let last_index = core.last_index_internal();

        // Validate range
        if low < first_index {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }
        if high > last_index + 1 {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        let max_size = max_size.into().unwrap_or(u64::MAX);

        // Calculate slice bounds using consistent index mapping
        // entries[0] is at compacted_index, so entry at index i is at position (i - compacted_index)
        let offset = (low - compacted_index) as usize;
        let end = (high - compacted_index) as usize;

        let slice = &core.entries[offset..end];

        // Pre-allocate with reasonable capacity to avoid excessive memory usage
        // Balance between avoiding reallocations and not over-allocating
        // when max_size is small but high-low is large
        let requested_count = (high - low) as usize;
        let capacity = std::cmp::min(requested_count, MAX_ENTRIES_CAPACITY_HINT);
        let mut result = Vec::with_capacity(capacity);
        let mut current_size: u64 = 0;

        for entry in slice {
            let entry_size = entry.compute_size() as u64;

            // If we already have entries and adding this one exceeds max_size, stop
            if !result.is_empty() && current_size + entry_size > max_size {
                break;
            }

            result.push(entry.clone());
            current_size += entry_size;
        }

        Ok(result)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let core = self.inner.read();

        let compacted_index = core.compacted_index_internal();
        let first_index = core.first_index_internal();

        // Check bounds
        if idx < compacted_index {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        // Calculate position using consistent index mapping
        let offset = (idx - compacted_index) as usize;

        if offset >= core.entries.len() {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        // Directly return term from entries (works for both dummy and real entries)
        Ok(core.entries[offset].term)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.inner.read().first_index_internal())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.inner.read().last_index_internal())
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        let core = self.inner.read();

        if core.snapshot.get_metadata().index == 0 {
            return Err(raft::Error::Store(
                raft::StorageError::SnapshotTemporarilyUnavailable,
            ));
        }

        Ok(core.snapshot.clone())
    }
}

// ============================================================================
// Unified Storage Wrapper
// ============================================================================

/// Unified storage that can be either in-memory or RocksDB-based.
///
/// This enum allows RaftNode to use either storage backend based on configuration.
/// All variants implement the `Storage` trait through delegation.
#[derive(Clone)]
pub enum RaftStorage {
    /// In-memory storage (fast but not durable).
    Memory(MemStorage),

    /// RocksDB-based persistent storage (durable).
    #[cfg(feature = "rocksdb-storage")]
    RocksDb(super::rocksdb_storage::RocksDbStorage),
}

impl RaftStorage {
    /// Create a new in-memory storage.
    pub fn new_memory() -> Self {
        Self::Memory(MemStorage::new())
    }

    /// Create in-memory storage with initial voters.
    pub fn new_memory_with_conf_state(voters: Vec<u64>) -> Self {
        Self::Memory(MemStorage::new_with_conf_state(voters))
    }

    /// Create RocksDB storage with the given configuration.
    #[cfg(feature = "rocksdb-storage")]
    pub fn new_rocksdb(
        config: super::rocksdb_storage::RocksDbStorageConfig,
    ) -> Result<Self> {
        Ok(Self::RocksDb(super::rocksdb_storage::RocksDbStorage::new(config)?))
    }

    /// Create RocksDB storage with initial voters.
    #[cfg(feature = "rocksdb-storage")]
    pub fn new_rocksdb_with_conf_state(
        config: super::rocksdb_storage::RocksDbStorageConfig,
        voters: Vec<u64>,
    ) -> Result<Self> {
        Ok(Self::RocksDb(
            super::rocksdb_storage::RocksDbStorage::new_with_conf_state(config, voters)?,
        ))
    }

    /// Set the hard state.
    pub fn set_hard_state(&self, hs: HardState) -> Result<()> {
        match self {
            Self::Memory(s) => {
                s.set_hard_state(hs);
                Ok(())
            }
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => s.set_hard_state(hs),
        }
    }

    /// Set the conf state.
    pub fn set_conf_state(&self, cs: ConfState) -> Result<()> {
        match self {
            Self::Memory(s) => {
                s.set_conf_state(cs);
                Ok(())
            }
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => s.set_conf_state(cs),
        }
    }

    /// Append entries to the log.
    pub fn append(&self, entries: &[Entry]) -> Result<()> {
        match self {
            Self::Memory(s) => s.append(entries),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => s.append(entries),
        }
    }

    /// Apply a snapshot.
    pub fn apply_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        match self {
            Self::Memory(s) => s.apply_snapshot(snapshot),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => s.apply_snapshot(snapshot),
        }
    }

    /// Compact the log up to the given index.
    pub fn compact(&self, compact_index: u64) -> Result<()> {
        match self {
            Self::Memory(s) => s.compact(compact_index),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => s.compact(compact_index),
        }
    }

    /// Get the last index in the log.
    pub fn last_index(&self) -> u64 {
        match self {
            Self::Memory(s) => s.last_index(),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => s.last_index(),
        }
    }

    /// Get the first index in the log.
    pub fn first_index(&self) -> u64 {
        match self {
            Self::Memory(s) => s.first_index(),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => s.first_index(),
        }
    }

    /// Get the compacted index.
    pub fn compacted_index(&self) -> u64 {
        match self {
            Self::Memory(s) => s.compacted_index(),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => s.compacted_index(),
        }
    }

    /// Get the number of entries in the log.
    pub fn entry_count(&self) -> usize {
        match self {
            Self::Memory(s) => s.entry_count(),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => s.entry_count(),
        }
    }

    /// Check if this is memory storage.
    pub fn is_memory(&self) -> bool {
        matches!(self, Self::Memory(_))
    }

    /// Check if this is RocksDB storage.
    #[cfg(feature = "rocksdb-storage")]
    pub fn is_rocksdb(&self) -> bool {
        matches!(self, Self::RocksDb(_))
    }

    /// Set a snapshot for future `Storage::snapshot()` calls.
    ///
    /// This method is called by the leader to store a locally-created snapshot
    /// that can be sent to lagging followers via InstallSnapshot RPC.
    pub fn set_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        match self {
            Self::Memory(s) => {
                s.set_snapshot(snapshot);
                Ok(())
            }
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => s.set_snapshot(snapshot),
        }
    }

    /// Get the current snapshot.
    pub fn get_snapshot(&self) -> Snapshot {
        match self {
            Self::Memory(s) => s.get_snapshot(),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => s.get_snapshot(),
        }
    }
}

impl Storage for RaftStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        match self {
            Self::Memory(s) => s.initial_state(),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => s.initial_state(),
        }
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: raft::GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        match self {
            Self::Memory(s) => s.entries(low, high, max_size, context),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => s.entries(low, high, max_size, context),
        }
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        match self {
            Self::Memory(s) => s.term(idx),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => s.term(idx),
        }
    }

    fn first_index(&self) -> raft::Result<u64> {
        match self {
            Self::Memory(s) => Storage::first_index(s),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => Storage::first_index(s),
        }
    }

    fn last_index(&self) -> raft::Result<u64> {
        match self {
            Self::Memory(s) => Storage::last_index(s),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => Storage::last_index(s),
        }
    }

    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        match self {
            Self::Memory(s) => s.snapshot(request_index, to),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => s.snapshot(request_index, to),
        }
    }
}

impl std::fmt::Debug for RaftStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Memory(s) => f.debug_tuple("RaftStorage::Memory")
                .field(&format!("entries={}", s.entry_count()))
                .finish(),
            #[cfg(feature = "rocksdb-storage")]
            Self::RocksDb(s) => f.debug_tuple("RaftStorage::RocksDb")
                .field(s)
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use super::*;

    #[test]
    fn test_initial_state() {
        let storage = MemStorage::new_with_conf_state(vec![1, 2, 3]);
        let state = storage.initial_state().unwrap();
        assert_eq!(state.conf_state.voters, vec![1, 2, 3]);
    }

    #[test]
    fn test_append_entries() {
        let storage = MemStorage::new();

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
        let storage = MemStorage::new();

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

        // Append entries 3-7 with different term (should truncate 4-5 and add 3-7)
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
        assert_eq!(storage.term(3).unwrap(), 2);
    }

    #[test]
    fn test_append_with_gap_returns_error() {
        let storage = MemStorage::new();

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

        // Try to append entries starting at 10 (gap from 6-9)
        let entries2: Vec<Entry> = (10..=15)
            .map(|i| {
                let mut e = Entry::default();
                e.index = i;
                e.term = 2;
                e
            })
            .collect();

        // This should return LogGap error
        let result = storage.append(&entries2);
        assert!(result.is_err());

        // Verify the error type
        match result {
            Err(crate::error::Error::Storage(StorageError::LogGap {
                                                 last_index,
                                                 first_new
                                             })) => {
                assert_eq!(last_index, 5);
                assert_eq!(first_new, 10);
            }
            _ => panic!("Expected LogGap error"),
        }
    }

    #[test]
    fn test_append_non_contiguous_returns_error() {
        let storage = MemStorage::new();

        // Create non-contiguous entries (1, 2, 4 - missing 3)
        let mut entries = vec![];
        for i in vec![1, 2, 4] {
            let mut e = Entry::default();
            e.index = i;
            e.term = 1;
            entries.push(e);
        }

        // This should return NonContiguous error
        let result = storage.append(&entries);
        assert!(result.is_err());

        match result {
            Err(crate::error::Error::Storage(StorageError::NonContiguous {
                                                 prev_index,
                                                 curr_index
                                             })) => {
                assert_eq!(prev_index, 2);
                assert_eq!(curr_index, 4);
            }
            _ => panic!("Expected NonContiguous error"),
        }
    }

    #[test]
    fn test_compact() {
        let storage = MemStorage::new();

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

        // Should be able to query compacted index itself
        assert_eq!(storage.term(5).unwrap(), 1);

        // Should fail for indices before compacted
        assert!(matches!(
            storage.term(4),
            Err(raft::Error::Store(raft::StorageError::Compacted))
        ));
    }

    #[test]
    fn test_compact_idempotent() {
        let storage = MemStorage::new();

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

        // Compacting to the same index should succeed
        storage.compact(5).unwrap();

        // Compacting to earlier index should succeed (no-op)
        storage.compact(3).unwrap();

        assert_eq!(storage.first_index(), 6);
        assert_eq!(storage.compacted_index(), 5);
    }

    #[test]
    fn test_entries_with_max_size() {
        let storage = MemStorage::new();

        let entries: Vec<Entry> = (1..=5)
            .map(|i| {
                let mut e = Entry::default();
                e.index = i;
                e.term = 1;
                e.data = Bytes::from(vec![0u8; 100]); // 100 bytes per entry
                e
            })
            .collect();

        storage.append(&entries).unwrap();

        // Should return fewer entries due to size limit
        let result = storage
            .entries(1, 6, Some(250), raft::GetEntriesContext::empty(false))
            .unwrap();

        // Should get 2-3 entries depending on protobuf overhead
        assert!(result.len() >= 2 && result.len() <= 3);
    }

    #[test]
    fn test_term_at_compacted_index() {
        let storage = MemStorage::new();

        let entries: Vec<Entry> = (1..=5)
            .map(|i| {
                let mut e = Entry::default();
                e.index = i;
                e.term = i;
                e
            })
            .collect();

        storage.append(&entries).unwrap();
        storage.compact(3).unwrap();

        // Should be able to query term at compacted index
        assert_eq!(storage.term(3).unwrap(), 3);

        // Should be able to query term at available indices
        assert_eq!(storage.term(4).unwrap(), 4);
        assert_eq!(storage.term(5).unwrap(), 5);

        // Should fail for compacted indices
        assert!(matches!(
            storage.term(2),
            Err(raft::Error::Store(raft::StorageError::Compacted))
        ));
    }

    #[test]
    fn test_apply_snapshot() {
        let storage = MemStorage::new();

        // Add some initial entries
        let entries: Vec<Entry> = (1..=10)
            .map(|i| {
                let mut e = Entry::default();
                e.index = i;
                e.term = 1;
                e
            })
            .collect();
        storage.append(&entries).unwrap();

        // Set initial hard state
        storage.set_hard_state(HardState {
            term: 1,
            vote: 0,
            commit: 5,
            unknown_fields: Default::default(),
            cached_size: Default::default(),
        });

        // Create and apply snapshot at index 7 with term 2
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 7;
        snapshot.mut_metadata().term = 2;

        storage.apply_snapshot(snapshot).unwrap();

        assert_eq!(storage.compacted_index(), 7);
        assert_eq!(storage.first_index(), 8);
        assert_eq!(storage.last_index(), 7);
        assert_eq!(storage.term(7).unwrap(), 2);

        // Verify hard state was updated
        let state = storage.initial_state().unwrap();
        assert_eq!(state.hard_state.commit, 7); // Updated to snapshot index
        assert_eq!(state.hard_state.term, 2);   // Updated to snapshot term
    }

    #[test]
    fn test_apply_snapshot_preserves_higher_commit() {
        let storage = MemStorage::new();

        // Set hard state with higher commit
        storage.set_hard_state(HardState {
            term: 3,
            vote: 0,
            commit: 10,
            unknown_fields: Default::default(),
            cached_size: Default::default(),
        });

        // Apply snapshot at lower index
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 7;
        snapshot.mut_metadata().term = 2;

        storage.apply_snapshot(snapshot).unwrap();

        // Commit should remain at 10 (not regressed)
        let state = storage.initial_state().unwrap();
        assert_eq!(state.hard_state.commit, 10);

        // But term should update to snapshot term if it was lower
        // In this case 3 > 2, so it should stay at 3
        assert_eq!(state.hard_state.term, 3);
    }

    #[test]
    fn test_continuous_operations() {
        let storage = MemStorage::new();

        // Append initial entries
        let entries1: Vec<Entry> = (1..=10)
            .map(|i| {
                let mut e = Entry::default();
                e.index = i;
                e.term = 1;
                e
            })
            .collect();
        storage.append(&entries1).unwrap();

        // Compact to 5
        storage.compact(5).unwrap();
        assert_eq!(storage.first_index(), 6);

        // Append more entries (must be contiguous)
        let entries2: Vec<Entry> = (11..=15)
            .map(|i| {
                let mut e = Entry::default();
                e.index = i;
                e.term = 2;
                e
            })
            .collect();
        storage.append(&entries2).unwrap();
        assert_eq!(storage.last_index(), 15);

        // Verify all accessible entries
        let retrieved = storage
            .entries(6, 16, None, raft::GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(retrieved.len(), 10);
        assert_eq!(retrieved[0].index, 6);
        assert_eq!(retrieved[9].index, 15);
    }

    #[test]
    fn test_entries_capacity_optimization() {
        let storage = MemStorage::new();

        // Create many entries
        let entries: Vec<Entry> = (1..=2000)
            .map(|i| {
                let mut e = Entry::default();
                e.index = i;
                e.term = 1;
                e.data = Bytes::from(vec![0u8; 10]);
                e
            })
            .collect();

        storage.append(&entries).unwrap();

        // Request a large range but with size limit
        // Should not allocate 2000 entries worth of capacity
        let result = storage
            .entries(1, 2001, Some(500), raft::GetEntriesContext::empty(false))
            .unwrap();

        // Should get limited number of entries
        assert!(result.len() < 100);
    }
}