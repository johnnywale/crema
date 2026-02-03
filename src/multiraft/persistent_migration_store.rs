//! Persistent Migration State Store.
//!
//! This module provides durable storage for migration state, ensuring that
//! migrations can be recovered after coordinator crashes.
//!
//! # Persistence Guarantees
//!
//! - Every state transition is persisted before returning
//! - Checkpoints are saved atomically (write-to-temp, then rename)
//! - Recovery loads all active migrations on startup
//!
//! # Storage Format
//!
//! ```text
//! migrations/
//!   ├── active/
//!   │   ├── shard_001.bin     # Active migration for shard 1
//!   │   └── shard_007.bin     # Active migration for shard 7
//!   ├── completed/
//!   │   ├── migration_abc123.bin
//!   │   └── migration_def456.bin
//!   └── checkpoints/
//!       ├── shard_001_checkpoint.bin
//!       └── shard_007_checkpoint.bin
//! ```

use crate::error::{Error, Result};
use crate::multiraft::raft_migration::{RaftMigrationPhase, RaftShardMigration};
use crate::multiraft::shard::ShardId;
use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

// ============================================================================
// Persistent Migration State Store Trait
// ============================================================================

/// Trait for persistent migration state storage.
#[async_trait]
pub trait PersistentMigrationStore: Send + Sync + std::fmt::Debug {
    /// Save a migration state (creates or updates).
    async fn save_migration(&self, migration: &RaftShardMigration) -> Result<()>;

    /// Load a migration by shard ID.
    async fn load_migration(&self, shard_id: ShardId) -> Result<Option<RaftShardMigration>>;

    /// Load all active migrations.
    async fn load_active_migrations(&self) -> Result<Vec<RaftShardMigration>>;

    /// Remove a migration (after completion/cancellation).
    async fn remove_migration(&self, shard_id: ShardId) -> Result<()>;

    /// Archive a completed migration.
    async fn archive_migration(&self, migration: &RaftShardMigration) -> Result<()>;

    /// Save a checkpoint for crash recovery.
    async fn save_checkpoint(&self, migration: &RaftShardMigration) -> Result<()>;

    /// Load a checkpoint.
    async fn load_checkpoint(&self, shard_id: ShardId) -> Result<Option<MigrationCheckpointData>>;

    /// Remove a checkpoint.
    async fn remove_checkpoint(&self, shard_id: ShardId) -> Result<()>;

    /// Save the coordinator term for zombie fencing after restart.
    ///
    /// The term must be persisted durably before the coordinator starts
    /// accepting commands with this term. This prevents zombie coordinators
    /// (from before restart) from interfering with the new coordinator.
    async fn save_coordinator_term(&self, term: u64) -> Result<()>;

    /// Load the persisted coordinator term.
    ///
    /// Returns `None` if no term was previously persisted (first startup).
    /// On restart, the coordinator should load this term and increment it
    /// before starting to ensure it has a higher term than any previous instance.
    async fn load_coordinator_term(&self) -> Result<Option<u64>>;
}

/// Checkpoint data for crash recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationCheckpointData {
    /// Migration ID.
    pub migration_id: Uuid,
    /// Shard ID.
    pub shard_id: ShardId,
    /// Current phase.
    pub phase: RaftMigrationPhase,
    /// Progress: learner match index.
    pub learner_match_index: u64,
    /// Progress: leader commit index.
    pub leader_commit_index: u64,
    /// Whether snapshot was applied.
    pub snapshot_applied: bool,
    /// Timestamp.
    pub timestamp_ms: u64,
    /// Sequence number for ordering.
    pub sequence: u64,
}

impl MigrationCheckpointData {
    /// Create a checkpoint from a migration.
    ///
    /// Uses the migration's `progress.last_updated` timestamp to ensure deterministic
    /// checkpoints across replicas. Using `SystemTime::now()` would cause different
    /// timestamps on different nodes, making checkpoint comparison unreliable.
    pub fn from_migration(migration: &RaftShardMigration, sequence: u64) -> Self {
        Self {
            migration_id: migration.id,
            shard_id: migration.change.shard_id,
            phase: migration.phase,
            learner_match_index: migration.progress.learner_match_index,
            leader_commit_index: migration.progress.leader_commit_index,
            snapshot_applied: migration.progress.snapshot_applied,
            // Use the migration's last_updated timestamp for determinism across replicas.
            // Falls back to started_at if last_updated is 0 (should not happen in practice).
            timestamp_ms: if migration.progress.last_updated > 0 {
                migration.progress.last_updated
            } else {
                migration.started_at
            },
            sequence,
        }
    }
}

// ============================================================================
// In-Memory Implementation (for testing)
// ============================================================================

/// In-memory implementation for testing.
#[derive(Debug, Default)]
pub struct InMemoryRaftMigrationStore {
    migrations: RwLock<HashMap<ShardId, RaftShardMigration>>,
    archived: RwLock<Vec<RaftShardMigration>>,
    checkpoints: RwLock<HashMap<ShardId, MigrationCheckpointData>>,
    coordinator_term: RwLock<Option<u64>>,
}

impl InMemoryRaftMigrationStore {
    /// Create a new in-memory store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl PersistentMigrationStore for InMemoryRaftMigrationStore {
    async fn save_migration(&self, migration: &RaftShardMigration) -> Result<()> {
        self.migrations
            .write()
            .insert(migration.change.shard_id, migration.clone());
        Ok(())
    }

    async fn load_migration(&self, shard_id: ShardId) -> Result<Option<RaftShardMigration>> {
        Ok(self.migrations.read().get(&shard_id).cloned())
    }

    async fn load_active_migrations(&self) -> Result<Vec<RaftShardMigration>> {
        Ok(self.migrations.read().values().cloned().collect())
    }

    async fn remove_migration(&self, shard_id: ShardId) -> Result<()> {
        self.migrations.write().remove(&shard_id);
        Ok(())
    }

    async fn archive_migration(&self, migration: &RaftShardMigration) -> Result<()> {
        self.archived.write().push(migration.clone());
        Ok(())
    }

    async fn save_checkpoint(&self, migration: &RaftShardMigration) -> Result<()> {
        let checkpoint = MigrationCheckpointData::from_migration(migration, 0);
        self.checkpoints
            .write()
            .insert(migration.change.shard_id, checkpoint);
        Ok(())
    }

    async fn load_checkpoint(&self, shard_id: ShardId) -> Result<Option<MigrationCheckpointData>> {
        Ok(self.checkpoints.read().get(&shard_id).cloned())
    }

    async fn remove_checkpoint(&self, shard_id: ShardId) -> Result<()> {
        self.checkpoints.write().remove(&shard_id);
        Ok(())
    }

    async fn save_coordinator_term(&self, term: u64) -> Result<()> {
        *self.coordinator_term.write() = Some(term);
        Ok(())
    }

    async fn load_coordinator_term(&self) -> Result<Option<u64>> {
        Ok(*self.coordinator_term.read())
    }
}

// ============================================================================
// File-Based Implementation (using async tokio::fs)
// ============================================================================

/// File-based persistent storage for migrations.
///
/// Uses bincode for efficient serialization and async I/O to avoid
/// blocking the executor.
#[derive(Debug)]
pub struct FileMigrationStore {
    /// Base directory for storage.
    base_dir: PathBuf,
    /// Sequence counter for checkpoints.
    sequence: std::sync::atomic::AtomicU64,
}

impl FileMigrationStore {
    /// Create a new file-based store.
    pub async fn new(base_dir: impl AsRef<Path>) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();

        // Create directory structure using async I/O
        fs::create_dir_all(base_dir.join("active"))
            .await
            .map_err(|e| Error::Internal(format!("Failed to create active dir: {}", e)))?;
        fs::create_dir_all(base_dir.join("completed"))
            .await
            .map_err(|e| Error::Internal(format!("Failed to create completed dir: {}", e)))?;
        fs::create_dir_all(base_dir.join("checkpoints"))
            .await
            .map_err(|e| Error::Internal(format!("Failed to create checkpoints dir: {}", e)))?;

        Ok(Self {
            base_dir,
            sequence: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Get path for active migration file.
    fn active_path(&self, shard_id: ShardId) -> PathBuf {
        self.base_dir
            .join("active")
            .join(format!("shard_{:03}.bin", shard_id))
    }

    /// Get path for checkpoint file.
    fn checkpoint_path(&self, shard_id: ShardId) -> PathBuf {
        self.base_dir
            .join("checkpoints")
            .join(format!("shard_{:03}_checkpoint.bin", shard_id))
    }

    /// Get path for archived migration.
    fn archive_path(&self, migration_id: Uuid) -> PathBuf {
        self.base_dir
            .join("completed")
            .join(format!("migration_{}.bin", migration_id))
    }

    /// Get path for coordinator term file.
    fn coordinator_term_path(&self) -> PathBuf {
        self.base_dir.join("coordinator_term.bin")
    }

    /// Get path for coordinator term lock file.
    fn coordinator_term_lock_path(&self) -> PathBuf {
        self.base_dir.join("coordinator_term.lock")
    }

    /// Atomically initialize the coordinator term on startup.
    ///
    /// This method:
    /// 1. Acquires an exclusive lock to prevent races between multiple processes
    /// 2. Loads the current term (or 0 if none)
    /// 3. Increments and saves the new term
    /// 4. Releases the lock
    ///
    /// This prevents the race condition where multiple processes starting
    /// simultaneously could read the same term and both increment to the same value.
    pub async fn initialize_coordinator_term(&self) -> Result<u64> {
        let lock_path = self.coordinator_term_lock_path();

        // Use blocking file operations for the lock since we need exclusive access
        // This is acceptable because term initialization only happens once at startup
        let lock_result = tokio::task::spawn_blocking({
            let lock_path = lock_path.clone();
            let term_path = self.coordinator_term_path();
            move || -> Result<u64> {
                use std::fs::{File, OpenOptions};
                use std::io::{Read, Write};

                // Create exclusive lock file
                // O_CREAT | O_EXCL ensures only one process can create it
                let lock_file = loop {
                    match OpenOptions::new()
                        .write(true)
                        .create_new(true) // Fails if file exists (O_EXCL)
                        .open(&lock_path)
                    {
                        Ok(f) => break f,
                        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                            // Another process holds the lock, wait and retry
                            std::thread::sleep(std::time::Duration::from_millis(50));

                            // Check if lock is stale (older than 30 seconds)
                            if let Ok(metadata) = std::fs::metadata(&lock_path) {
                                if let Ok(modified) = metadata.modified() {
                                    if modified.elapsed().unwrap_or_default()
                                        > std::time::Duration::from_secs(30)
                                    {
                                        // Stale lock, remove it
                                        let _ = std::fs::remove_file(&lock_path);
                                    }
                                }
                            }
                            continue;
                        }
                        Err(e) => {
                            return Err(Error::Internal(format!(
                                "Failed to create term lock: {}",
                                e
                            )));
                        }
                    }
                };

                // Write our PID to the lock file for debugging
                let _ = writeln!(&lock_file, "{}", std::process::id());

                // Now we have exclusive access - load current term
                let current_term = match File::open(&term_path) {
                    Ok(mut f) => {
                        let mut content = Vec::new();
                        f.read_to_end(&mut content).map_err(|e| {
                            Error::Internal(format!("Failed to read term file: {}", e))
                        })?;
                        bincode::deserialize::<u64>(&content).unwrap_or(0)
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
                    Err(e) => {
                        return Err(Error::Internal(format!("Failed to open term file: {}", e)));
                    }
                };

                // Increment term
                let new_term = current_term + 1;

                // Save new term atomically
                let temp_path = term_path.with_extension("tmp");
                let content = bincode::serialize(&new_term)
                    .map_err(|e| Error::Internal(format!("Failed to serialize term: {}", e)))?;

                let mut temp_file = File::create(&temp_path).map_err(|e| {
                    Error::Internal(format!("Failed to create temp term file: {}", e))
                })?;
                temp_file.write_all(&content).map_err(|e| {
                    Error::Internal(format!("Failed to write temp term file: {}", e))
                })?;
                temp_file.sync_all().map_err(|e| {
                    Error::Internal(format!("Failed to sync temp term file: {}", e))
                })?;

                std::fs::rename(&temp_path, &term_path)
                    .map_err(|e| Error::Internal(format!("Failed to rename term file: {}", e)))?;

                // Release lock by removing the file
                let _ = std::fs::remove_file(&lock_path);

                tracing::info!(
                    old_term = current_term,
                    new_term,
                    "Initialized coordinator term atomically"
                );

                Ok(new_term)
            }
        })
        .await
        .map_err(|e| Error::Internal(format!("Term initialization task panicked: {}", e)))?;

        lock_result
    }

    /// Atomically write a file (write to temp, then rename).
    /// Uses async I/O to avoid blocking the executor.
    async fn atomic_write(&self, path: &Path, content: &[u8]) -> Result<()> {
        let temp_path = path.with_extension("tmp");

        // Write to temp file using async I/O
        let mut file = fs::File::create(&temp_path)
            .await
            .map_err(|e| Error::Internal(format!("Failed to create temp file: {}", e)))?;

        file.write_all(content)
            .await
            .map_err(|e| Error::Internal(format!("Failed to write temp file: {}", e)))?;

        file.sync_all()
            .await
            .map_err(|e| Error::Internal(format!("Failed to sync temp file: {}", e)))?;

        // Rename to final path (atomic on most filesystems)
        fs::rename(&temp_path, path)
            .await
            .map_err(|e| Error::Internal(format!("Failed to rename file: {}", e)))?;

        Ok(())
    }

    /// Read a file's contents using async I/O.
    async fn read_file(&self, path: &Path) -> std::io::Result<Vec<u8>> {
        let mut file = fs::File::open(path).await?;
        let mut content = Vec::new();
        file.read_to_end(&mut content).await?;
        Ok(content)
    }
}

#[async_trait]
impl PersistentMigrationStore for FileMigrationStore {
    async fn save_migration(&self, migration: &RaftShardMigration) -> Result<()> {
        let path = self.active_path(migration.change.shard_id);
        let content = bincode::serialize(migration)
            .map_err(|e| Error::Internal(format!("Failed to serialize migration: {}", e)))?;

        self.atomic_write(&path, &content).await?;

        tracing::debug!(
            migration_id = %migration.id,
            shard_id = migration.change.shard_id,
            phase = %migration.phase,
            "Saved migration state"
        );

        Ok(())
    }

    async fn load_migration(&self, shard_id: ShardId) -> Result<Option<RaftShardMigration>> {
        let path = self.active_path(shard_id);

        match self.read_file(&path).await {
            Ok(content) => {
                let migration: RaftShardMigration =
                    bincode::deserialize(&content).map_err(|e| {
                        Error::Internal(format!("Failed to deserialize migration: {}", e))
                    })?;
                Ok(Some(migration))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::Internal(format!("Failed to read migration: {}", e))),
        }
    }

    async fn load_active_migrations(&self) -> Result<Vec<RaftShardMigration>> {
        let active_dir = self.base_dir.join("active");

        // Collect all .bin file paths first
        let mut paths = Vec::new();
        let mut entries = fs::read_dir(&active_dir)
            .await
            .map_err(|e| Error::Internal(format!("Failed to read active dir: {}", e)))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| Error::Internal(format!("Failed to read dir entry: {}", e)))?
        {
            let path = entry.path();
            if path.extension().map(|e| e == "bin").unwrap_or(false) {
                paths.push(path);
            }
        }

        // Parallel file loading using JoinSet for faster startup
        // Especially beneficial on cloud storage (EBS/pd-ssd) where serial I/O is slow
        let mut join_set = tokio::task::JoinSet::new();

        for path in paths {
            join_set.spawn(async move {
                match fs::read(&path).await {
                    Ok(content) => match bincode::deserialize::<RaftShardMigration>(&content) {
                        Ok(migration) => Some(migration),
                        Err(e) => {
                            tracing::warn!(
                                path = ?path,
                                error = %e,
                                "Failed to deserialize migration file, skipping"
                            );
                            None
                        }
                    },
                    Err(e) => {
                        tracing::warn!(
                            path = ?path,
                            error = %e,
                            "Failed to read migration file, skipping"
                        );
                        None
                    }
                }
            });
        }

        // Collect results from all parallel reads
        let mut migrations = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Some(migration)) => migrations.push(migration),
                Ok(None) => {} // File read/parse failed, already logged
                Err(e) => {
                    tracing::warn!(error = %e, "Migration load task panicked");
                }
            }
        }

        tracing::info!(
            count = migrations.len(),
            "Loaded active migrations from disk (parallel)"
        );

        Ok(migrations)
    }

    async fn remove_migration(&self, shard_id: ShardId) -> Result<()> {
        let path = self.active_path(shard_id);

        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Internal(format!(
                "Failed to remove migration: {}",
                e
            ))),
        }
    }

    async fn archive_migration(&self, migration: &RaftShardMigration) -> Result<()> {
        let path = self.archive_path(migration.id);
        let content = bincode::serialize(migration)
            .map_err(|e| Error::Internal(format!("Failed to serialize migration: {}", e)))?;

        self.atomic_write(&path, &content).await?;

        tracing::debug!(
            migration_id = %migration.id,
            shard_id = migration.change.shard_id,
            "Archived migration"
        );

        Ok(())
    }

    async fn save_checkpoint(&self, migration: &RaftShardMigration) -> Result<()> {
        let seq = self
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let checkpoint = MigrationCheckpointData::from_migration(migration, seq);

        let path = self.checkpoint_path(migration.change.shard_id);
        let content = bincode::serialize(&checkpoint)
            .map_err(|e| Error::Internal(format!("Failed to serialize checkpoint: {}", e)))?;

        self.atomic_write(&path, &content).await?;

        tracing::debug!(
            migration_id = %migration.id,
            shard_id = migration.change.shard_id,
            sequence = seq,
            "Saved checkpoint"
        );

        Ok(())
    }

    async fn load_checkpoint(&self, shard_id: ShardId) -> Result<Option<MigrationCheckpointData>> {
        let path = self.checkpoint_path(shard_id);

        match self.read_file(&path).await {
            Ok(content) => {
                let checkpoint: MigrationCheckpointData =
                    bincode::deserialize(&content).map_err(|e| {
                        Error::Internal(format!("Failed to deserialize checkpoint: {}", e))
                    })?;
                Ok(Some(checkpoint))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::Internal(format!("Failed to read checkpoint: {}", e))),
        }
    }

    async fn remove_checkpoint(&self, shard_id: ShardId) -> Result<()> {
        let path = self.checkpoint_path(shard_id);

        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Internal(format!(
                "Failed to remove checkpoint: {}",
                e
            ))),
        }
    }

    async fn save_coordinator_term(&self, term: u64) -> Result<()> {
        let path = self.coordinator_term_path();
        let content = bincode::serialize(&term)
            .map_err(|e| Error::Internal(format!("Failed to serialize coordinator term: {}", e)))?;

        self.atomic_write(&path, &content).await?;

        tracing::debug!(term, "Saved coordinator term to disk");

        Ok(())
    }

    async fn load_coordinator_term(&self) -> Result<Option<u64>> {
        let path = self.coordinator_term_path();

        match self.read_file(&path).await {
            Ok(content) => {
                let term: u64 = bincode::deserialize(&content).map_err(|e| {
                    Error::Internal(format!("Failed to deserialize coordinator term: {}", e))
                })?;
                tracing::debug!(term, "Loaded coordinator term from disk");
                Ok(Some(term))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::Internal(format!(
                "Failed to read coordinator term: {}",
                e
            ))),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multiraft::raft_migration::{RaftChangeType, RaftMembershipChange};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_in_memory_store() {
        let store = InMemoryRaftMigrationStore::new();

        let change = RaftMembershipChange::new(1, RaftChangeType::AddLearner, 5);
        let migration = RaftShardMigration::new(change);

        // Save
        store.save_migration(&migration).await.unwrap();

        // Load
        let loaded = store.load_migration(1).await.unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().id, migration.id);

        // Load all
        let all = store.load_active_migrations().await.unwrap();
        assert_eq!(all.len(), 1);

        // Remove
        store.remove_migration(1).await.unwrap();
        let loaded = store.load_migration(1).await.unwrap();
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn test_in_memory_checkpoint() {
        let store = InMemoryRaftMigrationStore::new();

        let change = RaftMembershipChange::new(1, RaftChangeType::AddLearner, 5);
        let migration = RaftShardMigration::new(change);

        // Save checkpoint
        store.save_checkpoint(&migration).await.unwrap();

        // Load checkpoint
        let checkpoint = store.load_checkpoint(1).await.unwrap();
        assert!(checkpoint.is_some());
        let cp = checkpoint.unwrap();
        assert_eq!(cp.migration_id, migration.id);
        assert_eq!(cp.shard_id, 1);

        // Remove checkpoint
        store.remove_checkpoint(1).await.unwrap();
        let checkpoint = store.load_checkpoint(1).await.unwrap();
        assert!(checkpoint.is_none());
    }

    #[tokio::test]
    async fn test_file_store_save_load() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileMigrationStore::new(temp_dir.path()).await.unwrap();

        let change = RaftMembershipChange::new(1, RaftChangeType::AddLearner, 5);
        let migration = RaftShardMigration::new(change);

        // Save
        store.save_migration(&migration).await.unwrap();

        // Load
        let loaded = store.load_migration(1).await.unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.id, migration.id);
        assert_eq!(loaded.change.shard_id, 1);
        assert_eq!(loaded.change.target_node, 5);
    }

    #[tokio::test]
    async fn test_file_store_load_all() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileMigrationStore::new(temp_dir.path()).await.unwrap();

        // Save multiple migrations
        for shard_id in 0..3 {
            let change = RaftMembershipChange::new(shard_id, RaftChangeType::AddLearner, 5);
            let migration = RaftShardMigration::new(change);
            store.save_migration(&migration).await.unwrap();
        }

        // Load all
        let all = store.load_active_migrations().await.unwrap();
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn test_file_store_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileMigrationStore::new(temp_dir.path()).await.unwrap();

        let change = RaftMembershipChange::new(1, RaftChangeType::AddLearner, 5);
        let mut migration = RaftShardMigration::new(change);
        migration.update_progress(50, 100, true);

        // Save checkpoint
        store.save_checkpoint(&migration).await.unwrap();

        // Load checkpoint
        let checkpoint = store.load_checkpoint(1).await.unwrap();
        assert!(checkpoint.is_some());
        let cp = checkpoint.unwrap();
        assert_eq!(cp.learner_match_index, 50);
        assert_eq!(cp.leader_commit_index, 100);
        assert!(cp.snapshot_applied);
    }

    #[tokio::test]
    async fn test_file_store_archive() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileMigrationStore::new(temp_dir.path()).await.unwrap();

        let change = RaftMembershipChange::new(1, RaftChangeType::AddLearner, 5);
        let migration = RaftShardMigration::new(change);

        // Archive
        store.archive_migration(&migration).await.unwrap();

        // Verify file exists
        let archive_path = temp_dir
            .path()
            .join("completed")
            .join(format!("migration_{}.bin", migration.id));
        assert!(archive_path.exists());
    }

    #[tokio::test]
    async fn test_file_store_remove() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileMigrationStore::new(temp_dir.path()).await.unwrap();

        let change = RaftMembershipChange::new(1, RaftChangeType::AddLearner, 5);
        let migration = RaftShardMigration::new(change);

        // Save and remove
        store.save_migration(&migration).await.unwrap();
        assert!(store.load_migration(1).await.unwrap().is_some());

        store.remove_migration(1).await.unwrap();
        assert!(store.load_migration(1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_in_memory_coordinator_term() {
        let store = InMemoryRaftMigrationStore::new();

        // Initially no term persisted
        assert!(store.load_coordinator_term().await.unwrap().is_none());

        // Save term
        store.save_coordinator_term(5).await.unwrap();

        // Load term
        let term = store.load_coordinator_term().await.unwrap();
        assert_eq!(term, Some(5));

        // Update term
        store.save_coordinator_term(10).await.unwrap();
        let term = store.load_coordinator_term().await.unwrap();
        assert_eq!(term, Some(10));
    }

    #[tokio::test]
    async fn test_file_store_coordinator_term() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileMigrationStore::new(temp_dir.path()).await.unwrap();

        // Initially no term persisted
        assert!(store.load_coordinator_term().await.unwrap().is_none());

        // Save term
        store.save_coordinator_term(42).await.unwrap();

        // Load term
        let term = store.load_coordinator_term().await.unwrap();
        assert_eq!(term, Some(42));

        // Verify file exists
        let term_path = temp_dir.path().join("coordinator_term.bin");
        assert!(term_path.exists());

        // Create a new store and verify term survives
        let store2 = FileMigrationStore::new(temp_dir.path()).await.unwrap();
        let term = store2.load_coordinator_term().await.unwrap();
        assert_eq!(term, Some(42));
    }

    #[tokio::test]
    async fn test_coordinator_term_persistence_across_restart() {
        let temp_dir = TempDir::new().unwrap();

        // Simulate first instance with term 5
        {
            let store = FileMigrationStore::new(temp_dir.path()).await.unwrap();
            store.save_coordinator_term(5).await.unwrap();
        }

        // Simulate restart - new instance should see persisted term
        {
            let store = FileMigrationStore::new(temp_dir.path()).await.unwrap();
            let persisted = store.load_coordinator_term().await.unwrap().unwrap_or(0);

            // New term should be higher to fence zombies
            let new_term = persisted + 1;
            assert_eq!(new_term, 6);

            store.save_coordinator_term(new_term).await.unwrap();
        }

        // Verify final term
        {
            let store = FileMigrationStore::new(temp_dir.path()).await.unwrap();
            let term = store.load_coordinator_term().await.unwrap();
            assert_eq!(term, Some(6));
        }
    }
}
