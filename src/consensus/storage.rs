//! In-memory storage implementation for Raft.

use crate::error::{Result, StorageError};
use parking_lot::RwLock;
use protobuf::Message as ProtoMessage;
use raft::prelude::*;
use raft::{RaftState, Storage};
use std::sync::Arc;

/// In-memory storage for Raft log entries and state.
///
/// This storage is cloneable - clones share the same underlying data,
/// which is required for RawNode and process_ready to see the same state.
#[derive(Clone)]
pub struct MemStorage {
    inner: Arc<RwLock<MemStorageCore>>,
}

struct MemStorageCore {
    /// The current hard state (term, vote, commit).
    hard_state: HardState,

    /// The current conf state (voters, learners).
    conf_state: ConfState,

    /// Log entries. First entry is a dummy entry at index 0.
    entries: Vec<Entry>,

    /// The current snapshot.
    snapshot: Snapshot,

    /// Index of the last compacted entry.
    compacted_index: u64,

    /// Term of the last compacted entry.
    compacted_term: u64,
}

impl Default for MemStorageCore {
    fn default() -> Self {
        Self {
            hard_state: HardState::default(),
            conf_state: ConfState::default(),
            entries: vec![Entry::default()], // Dummy entry at index 0
            snapshot: Snapshot::default(),
            compacted_index: 0,
            compacted_term: 0,
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
    pub fn append(&self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut core = self.inner.write();

        let first_index = core.first_index();
        let last_index = core.last_index();

        // Entries being appended must be contiguous.
        let first_new = entries[0].index;
        if first_new < first_index {
            return Err(StorageError::Compacted(first_index).into());
        }

        // Truncate existing entries if there's an overlap.
        if first_new <= last_index {
            let offset = (first_new - first_index) as usize;
            core.entries.truncate(offset);
        }

        // Append new entries.
        core.entries.extend_from_slice(entries);

        Ok(())
    }

    /// Apply a snapshot.
    pub fn apply_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let mut core = self.inner.write();

        let index = snapshot.get_metadata().index;
        let term = snapshot.get_metadata().term;

        // Update compaction info.
        core.compacted_index = index;
        core.compacted_term = term;

        // Update conf state from snapshot.
        core.conf_state = snapshot.get_metadata().get_conf_state().clone();

        // Clear entries and add dummy.
        core.entries.clear();
        let mut dummy = Entry::default();
        dummy.index = index;
        dummy.term = term;
        core.entries.push(dummy);

        // Store snapshot.
        core.snapshot = snapshot;

        Ok(())
    }

    /// Compact the log up to the given index.
    pub fn compact(&self, compact_index: u64) -> Result<()> {
        let mut core = self.inner.write();

        if compact_index <= core.compacted_index {
            return Ok(());
        }

        let last_index = core.last_index();
        if compact_index > last_index {
            return Err(StorageError::EntryNotFound(compact_index).into());
        }

        // Get term at compact index.
        let offset = (compact_index - core.first_index()) as usize;
        let compact_term = core.entries[offset].term;

        // Remove entries before compact_index.
        core.entries.drain(..offset);

        // Update first entry to be a dummy at compact_index.
        if !core.entries.is_empty() {
            core.entries[0].index = compact_index;
            core.entries[0].term = compact_term;
            core.entries[0].data.clear();
        }

        core.compacted_index = compact_index;
        core.compacted_term = compact_term;

        Ok(())
    }

    /// Get the last index in the log.
    pub fn last_index(&self) -> u64 {
        self.inner.read().last_index()
    }

    /// Get the first index in the log.
    pub fn first_index(&self) -> u64 {
        self.inner.read().first_index()
    }

    /// Get the number of entries in the log.
    pub fn entry_count(&self) -> usize {
        let core = self.inner.read();
        core.entries.len().saturating_sub(1) // Exclude dummy entry
    }
}

impl MemStorageCore {
    fn first_index(&self) -> u64 {
        self.entries
            .first()
            .map(|e| e.index + 1)
            .unwrap_or(self.compacted_index + 1)
    }

    fn last_index(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.index)
            .unwrap_or(self.compacted_index)
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
        let max_size = max_size.into().unwrap_or(u64::MAX);
        let core = self.inner.read();

        let first_index = core.first_index();
        if low < first_index {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        let last_index = core.last_index();
        if high > last_index + 1 {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        let offset = (low - first_index + 1) as usize; // +1 for dummy entry
        let end = (high - first_index + 1) as usize;

        let mut entries = Vec::new();
        let mut size: u64 = 0;

        for entry in core.entries[offset..end].iter() {
            let entry_size = entry.compute_size() as u64;
            if !entries.is_empty() && size + entry_size > max_size {
                break;
            }
            size += entry_size;
            entries.push(entry.clone());
        }

        Ok(entries)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let core = self.inner.read();

        if idx == core.compacted_index {
            return Ok(core.compacted_term);
        }

        let first_index = core.first_index();
        if idx < first_index {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        let offset = (idx - first_index + 1) as usize;
        if offset >= core.entries.len() {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        Ok(core.entries[offset].term)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.inner.read().first_index())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.inner.read().last_index())
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

#[cfg(test)]
mod tests {
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

        assert_eq!(storage.first_index(), 6);
        assert_eq!(storage.last_index(), 10);
    }

    #[test]
    fn test_term() {
        let storage = MemStorage::new();

        let entries: Vec<Entry> = (1..=5)
            .map(|i| {
                let mut e = Entry::default();
                e.index = i;
                e.term = i; // Term equals index for testing
                e
            })
            .collect();

        storage.append(&entries).unwrap();

        assert_eq!(storage.term(3).unwrap(), 3);
        assert_eq!(storage.term(5).unwrap(), 5);
    }
}
