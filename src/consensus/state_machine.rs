//! State machine for applying committed Raft entries to the cache.

use crate::cache::storage::CacheStorage;
use crate::types::CacheCommand;
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

/// The cache state machine that applies committed Raft entries.
///
/// CRITICAL: The apply method must be infallible. Pre-validate all commands
/// before proposing them to Raft.
pub struct CacheStateMachine {
    /// The underlying cache storage.
    storage: Arc<CacheStorage>,

    /// The last applied index.
    applied_index: AtomicU64,

    /// The last applied term.
    applied_term: AtomicU64,

    /// Number of commands applied.
    commands_applied: AtomicU64,
}

impl CacheStateMachine {
    /// Create a new state machine with the given cache storage.
    pub fn new(storage: Arc<CacheStorage>) -> Self {
        Self {
            storage,
            applied_index: AtomicU64::new(0),
            applied_term: AtomicU64::new(0),
            commands_applied: AtomicU64::new(0),
        }
    }

    /// Apply a committed entry to the state machine.
    ///
    /// This method MUST be infallible - it cannot fail after the entry
    /// has been committed by Raft. If something goes wrong, we log the
    /// error but continue.
    pub async fn apply(&self, index: u64, term: u64, data: &[u8]) {
        // Skip if already applied (idempotency)
        if index <= self.applied_index.load(Ordering::SeqCst) {
            debug!(index, "Skipping already applied entry");
            return;
        }

        // Empty data means a noop entry (e.g., for leader election)
        if data.is_empty() {
            self.update_applied(index, term);
            return;
        }

        // Deserialize command
        let command = match CacheCommand::from_bytes(data) {
            Ok(cmd) => cmd,
            Err(e) => {
                // Log error but don't fail - entry is already committed
                error!(index, error = %e, "Failed to deserialize command");
                self.update_applied(index, term);
                return;
            }
        };

        // Apply the command
        match command {
            CacheCommand::Put { key, value, ttl_ms } => {
                let key_preview = String::from_utf8_lossy(&key[..std::cmp::min(key.len(), 32)]);
                info!(
                    raft_index = index,
                    raft_term = term,
                    key = %key_preview,
                    value_len = value.len(),
                    ttl_ms = ?ttl_ms,
                    "STATE_MACHINE: Applying PUT to local storage"
                );

                let key = Bytes::from(key);
                let value = Bytes::from(value);

                if let Some(ttl_ms) = ttl_ms {
                    self.storage
                        .insert_with_ttl(key, value, Duration::from_millis(ttl_ms))
                        .await;
                } else {
                    self.storage.insert(key, value).await;
                }

                info!(
                    raft_index = index,
                    "STATE_MACHINE: PUT applied successfully"
                );
            }

            CacheCommand::Delete { key } => {
                let key_preview = String::from_utf8_lossy(&key[..std::cmp::min(key.len(), 32)]);
                info!(
                    raft_index = index,
                    raft_term = term,
                    key = %key_preview,
                    "STATE_MACHINE: Applying DELETE to local storage"
                );

                self.storage.invalidate(&key).await;

                info!(
                    raft_index = index,
                    "STATE_MACHINE: DELETE applied successfully"
                );
            }

            CacheCommand::Clear => {
                info!(
                    raft_index = index,
                    raft_term = term,
                    "STATE_MACHINE: Applying CLEAR to local storage"
                );

                self.storage.invalidate_all();

                info!(
                    raft_index = index,
                    "STATE_MACHINE: CLEAR applied successfully"
                );
            }
        }

        self.update_applied(index, term);
        self.commands_applied.fetch_add(1, Ordering::Relaxed);
    }

    /// Apply multiple entries in order.
    pub async fn apply_entries(&self, entries: &[raft::prelude::Entry]) {
        for entry in entries {
            self.apply(entry.index, entry.term, &entry.data).await;
        }
    }

    fn update_applied(&self, index: u64, term: u64) {
        self.applied_index.store(index, Ordering::SeqCst);
        self.applied_term.store(term, Ordering::SeqCst);
    }

    /// Get the last applied index.
    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::SeqCst)
    }

    /// Get the last applied term.
    pub fn applied_term(&self) -> u64 {
        self.applied_term.load(Ordering::SeqCst)
    }

    /// Get the number of commands applied.
    pub fn commands_applied(&self) -> u64 {
        self.commands_applied.load(Ordering::Relaxed)
    }

    /// Get the underlying cache storage (for reads).
    pub fn storage(&self) -> &Arc<CacheStorage> {
        &self.storage
    }
}

impl std::fmt::Debug for CacheStateMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheStateMachine")
            .field("applied_index", &self.applied_index())
            .field("applied_term", &self.applied_term())
            .field("commands_applied", &self.commands_applied())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CacheConfig;

    fn create_state_machine() -> CacheStateMachine {
        let config = CacheConfig::default();
        let storage = Arc::new(CacheStorage::new(&config));
        CacheStateMachine::new(storage)
    }

    #[tokio::test]
    async fn test_apply_put() {
        let sm = create_state_machine();

        let cmd = CacheCommand::put(b"key1".to_vec(), b"value1".to_vec());
        let data = cmd.to_bytes().unwrap();

        sm.apply(1, 1, &data).await;

        assert_eq!(sm.applied_index(), 1);
        assert_eq!(sm.applied_term(), 1);

        let value = sm.storage().get(b"key1").await;
        assert_eq!(value, Some(Bytes::from("value1")));
    }

    #[tokio::test]
    async fn test_apply_delete() {
        let sm = create_state_machine();

        // First put
        let cmd = CacheCommand::put(b"key1".to_vec(), b"value1".to_vec());
        sm.apply(1, 1, &cmd.to_bytes().unwrap()).await;

        // Then delete
        let cmd = CacheCommand::delete(b"key1".to_vec());
        sm.apply(2, 1, &cmd.to_bytes().unwrap()).await;

        assert_eq!(sm.applied_index(), 2);

        let value = sm.storage().get(b"key1").await;
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_apply_clear() {
        let sm = create_state_machine();

        // Put some entries
        let cmd1 = CacheCommand::put(b"key1".to_vec(), b"value1".to_vec());
        sm.apply(1, 1, &cmd1.to_bytes().unwrap()).await;

        let cmd2 = CacheCommand::put(b"key2".to_vec(), b"value2".to_vec());
        sm.apply(2, 1, &cmd2.to_bytes().unwrap()).await;

        // Clear
        let cmd = CacheCommand::clear();
        sm.apply(3, 1, &cmd.to_bytes().unwrap()).await;

        sm.storage().run_pending_tasks().await;

        // Wait for invalidation to complete
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        sm.storage().run_pending_tasks().await;

        assert_eq!(sm.storage().entry_count(), 0);
    }

    #[tokio::test]
    async fn test_idempotent_apply() {
        let sm = create_state_machine();

        let cmd = CacheCommand::put(b"key1".to_vec(), b"value1".to_vec());
        let data = cmd.to_bytes().unwrap();

        // Apply same index twice
        sm.apply(1, 1, &data).await;
        sm.apply(1, 1, &data).await;

        // Should still be 1 command applied (second was skipped)
        assert_eq!(sm.commands_applied(), 1);
    }

    #[tokio::test]
    async fn test_apply_empty_data() {
        let sm = create_state_machine();

        // Empty data is a noop
        sm.apply(1, 1, &[]).await;

        assert_eq!(sm.applied_index(), 1);
        assert_eq!(sm.commands_applied(), 0); // No command applied
    }
}
