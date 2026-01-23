//! State machine for applying committed Raft entries to the cache.

use crate::cache::storage::CacheStorage;
use crate::types::CacheCommand;
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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
        let current_applied = self.applied_index.load(Ordering::SeqCst);

        // Skip if already applied (idempotency)
        if index <= current_applied {
            debug!(
                index,
                current_applied,
                "STATE_MACHINE: Skipping already applied entry (index <= applied_index)"
            );
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
            CacheCommand::Put {
                key,
                value,
                expires_at_ms,
            } => {
                let key_preview = String::from_utf8_lossy(&key[..std::cmp::min(key.len(), 32)]);
                debug!(
                    raft_index = index,
                    raft_term = term,
                    key = %key_preview,
                    value_len = value.len(),
                    expires_at_ms = ?expires_at_ms,
                    "STATE_MACHINE: Applying PUT to local storage"
                );

                let key = Bytes::from(key);
                let value = Bytes::from(value);

                if let Some(expires_at_ms) = expires_at_ms {
                    // Calculate remaining TTL from absolute expiration time
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;

                    if expires_at_ms > now_ms {
                        // Entry has not yet expired, calculate remaining TTL
                        let remaining_ttl_ms = expires_at_ms - now_ms;
                        self.storage
                            .insert_with_ttl(key, value, Duration::from_millis(remaining_ttl_ms))
                            .await;
                        debug!(
                            raft_index = index,
                            remaining_ttl_ms = remaining_ttl_ms,
                            "STATE_MACHINE: PUT applied with TTL"
                        );
                    } else {
                        // Entry has already expired, skip insertion
                        debug!(
                            raft_index = index,
                            "STATE_MACHINE: Skipping expired PUT entry (expired {}ms ago)",
                            now_ms - expires_at_ms
                        );
                    }
                } else {
                    self.storage.insert(key, value).await;
                    debug!(
                        raft_index = index,
                        "STATE_MACHINE: PUT applied (no TTL)"
                    );
                }
            }

            CacheCommand::Delete { key } => {
                let key_preview = String::from_utf8_lossy(&key[..std::cmp::min(key.len(), 32)]);
                debug!(
                    raft_index = index,
                    raft_term = term,
                    key = %key_preview,
                    "STATE_MACHINE: Applying DELETE to local storage"
                );

                self.storage.invalidate(&key).await;

                debug!(
                    raft_index = index,
                    "STATE_MACHINE: DELETE applied successfully"
                );
            }

            CacheCommand::Clear => {
                debug!(
                    raft_index = index,
                    raft_term = term,
                    "STATE_MACHINE: Applying CLEAR to local storage"
                );

                self.storage.invalidate_all();

                debug!(
                    raft_index = index,
                    "STATE_MACHINE: CLEAR applied successfully"
                );
            }

            CacheCommand::Get { key: _ } => {
                // Get is a read-only operation used for cross-shard forwarding.
                // It should never be proposed through Raft; if it appears here,
                // just treat it as a no-op.
                debug!(
                    raft_index = index,
                    raft_term = term,
                    "STATE_MACHINE: Ignoring GET command (read-only, should not be proposed)"
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

    /// Set the applied state for recovery from snapshot.
    ///
    /// This should be called after loading state from a snapshot to ensure
    /// the state machine's applied_index matches the snapshot's Raft index.
    /// This prevents re-applying entries that were already included in the snapshot.
    pub fn set_recovered_state(&self, index: u64, term: u64) {
        let old_index = self.applied_index.load(Ordering::SeqCst);
        let old_term = self.applied_term.load(Ordering::SeqCst);
        self.applied_index.store(index, Ordering::SeqCst);
        self.applied_term.store(term, Ordering::SeqCst);
        debug!(
            old_index,
            old_term,
            new_index = index,
            new_term = term,
            "STATE_MACHINE: Set recovered state from snapshot"
        );
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
