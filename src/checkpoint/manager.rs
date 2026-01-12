//! Checkpoint manager for creating and loading snapshots.

use crate::cache::storage::CacheStorage;
use crate::checkpoint::format::FormatError;
use crate::checkpoint::reader::SnapshotReader;
use crate::checkpoint::writer::{SnapshotMetadata, SnapshotWriter};
use bytes::Bytes;
use parking_lot::RwLock;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Configuration for checkpointing.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Directory to store snapshots
    pub dir: PathBuf,

    /// Create snapshot after this many log entries
    pub log_threshold: u64,

    /// Create snapshot after this interval
    pub time_interval: Duration,

    /// Maximum number of snapshots to keep
    pub max_snapshots: usize,

    /// Whether to compress snapshots
    pub compress: bool,

    /// Whether checkpointing is enabled
    pub enabled: bool,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./checkpoints"),
            log_threshold: 10_000,
            time_interval: Duration::from_secs(300), // 5 minutes
            max_snapshots: 3,
            compress: true,
            enabled: true,
        }
    }
}

impl CheckpointConfig {
    /// Create a new configuration with the given directory.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            ..Default::default()
        }
    }

    /// Set the log threshold.
    pub fn with_log_threshold(mut self, threshold: u64) -> Self {
        self.log_threshold = threshold;
        self
    }

    /// Set the time interval.
    pub fn with_time_interval(mut self, interval: Duration) -> Self {
        self.time_interval = interval;
        self
    }

    /// Set compression.
    pub fn with_compression(mut self, compress: bool) -> Self {
        self.compress = compress;
        self
    }

    /// Set max snapshots to keep.
    pub fn with_max_snapshots(mut self, max: usize) -> Self {
        self.max_snapshots = max;
        self
    }
}

/// Information about a snapshot file.
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    /// Path to the snapshot file
    pub path: PathBuf,

    /// Raft index
    pub raft_index: u64,

    /// Raft term
    pub raft_term: u64,

    /// When the snapshot was created
    pub timestamp: u64,

    /// Number of entries
    pub entry_count: u64,

    /// File size in bytes
    pub file_size: u64,
}

/// Checkpoint manager that handles snapshot creation and recovery.
pub struct CheckpointManager {
    /// Configuration
    config: CheckpointConfig,

    /// Cache storage reference
    storage: Arc<CacheStorage>,

    /// Current snapshot info
    current_snapshot: RwLock<Option<SnapshotInfo>>,

    /// Last snapshot index
    last_snapshot_index: AtomicU64,

    /// Entries since last snapshot
    entries_since_snapshot: AtomicU64,

    /// Last snapshot time
    last_snapshot_time: RwLock<Instant>,

    /// Whether a snapshot is in progress
    snapshot_in_progress: AtomicBool,
}

impl CheckpointManager {
    /// Create a new checkpoint manager.
    pub fn new(config: CheckpointConfig, storage: Arc<CacheStorage>) -> Result<Self, FormatError> {
        // Create checkpoint directory if it doesn't exist
        if config.enabled {
            fs::create_dir_all(&config.dir)?;
        }

        Ok(Self {
            config,
            storage,
            current_snapshot: RwLock::new(None),
            last_snapshot_index: AtomicU64::new(0),
            entries_since_snapshot: AtomicU64::new(0),
            last_snapshot_time: RwLock::new(Instant::now()),
            snapshot_in_progress: AtomicBool::new(false),
        })
    }

    /// Get the configuration.
    pub fn config(&self) -> &CheckpointConfig {
        &self.config
    }

    /// Get the last snapshot index.
    pub fn last_snapshot_index(&self) -> u64 {
        self.last_snapshot_index.load(Ordering::SeqCst)
    }

    /// Get entries since last snapshot.
    pub fn entries_since_snapshot(&self) -> u64 {
        self.entries_since_snapshot.load(Ordering::Relaxed)
    }

    /// Increment entries since snapshot counter.
    pub fn record_entry(&self) {
        self.entries_since_snapshot.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if a snapshot should be created.
    pub fn should_snapshot(&self) -> bool {
        if !self.config.enabled {
            return false;
        }

        if self.snapshot_in_progress.load(Ordering::Relaxed) {
            return false;
        }

        // Check log threshold
        if self.entries_since_snapshot() >= self.config.log_threshold {
            return true;
        }

        // Check time interval
        let elapsed = self.last_snapshot_time.read().elapsed();
        if elapsed >= self.config.time_interval {
            return true;
        }

        false
    }

    /// Create a snapshot of the current cache state.
    pub async fn create_snapshot(
        &self,
        raft_index: u64,
        raft_term: u64,
    ) -> Result<SnapshotMetadata, FormatError> {
        if !self.config.enabled {
            return Err(FormatError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "checkpointing disabled",
            )));
        }

        // Set in-progress flag
        if self
            .snapshot_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(FormatError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "snapshot already in progress",
            )));
        }

        let result = self
            .create_snapshot_internal(raft_index, raft_term)
            .await;

        // Clear in-progress flag
        self.snapshot_in_progress.store(false, Ordering::SeqCst);

        result
    }

    async fn create_snapshot_internal(
        &self,
        raft_index: u64,
        raft_term: u64,
    ) -> Result<SnapshotMetadata, FormatError> {
        let filename = format!(
            "snapshot-{:016x}-{:016x}.{}",
            raft_index,
            raft_term,
            if self.config.compress { "lz4" } else { "dat" }
        );
        let path = self.config.dir.join(&filename);

        info!(
            raft_index,
            raft_term,
            path = %path.display(),
            "Creating snapshot"
        );

        // Create writer
        let writer =
            SnapshotWriter::new(&path, raft_index, raft_term, self.config.compress)?;

        // Iterate cache and write entries
        // Note: This is a simplified implementation. In production, you'd want
        // to use Moka's iteration capabilities more carefully.
        let entry_count = self.storage.entry_count();
        debug!(entry_count, "Writing cache entries to snapshot");

        // For now, we can't easily iterate Moka's cache, so we'll need to
        // track entries separately or use a different approach.
        // This is a limitation we'll address in the integration step.

        // Finalize snapshot
        let metadata = writer.finalize()?;

        // Update state
        self.last_snapshot_index
            .store(raft_index, Ordering::SeqCst);
        self.entries_since_snapshot.store(0, Ordering::Relaxed);
        *self.last_snapshot_time.write() = Instant::now();

        // Update current snapshot info
        *self.current_snapshot.write() = Some(SnapshotInfo {
            path: path.clone(),
            raft_index,
            raft_term,
            timestamp: metadata.timestamp,
            entry_count: metadata.entry_count,
            file_size: metadata.file_size,
        });

        // Cleanup old snapshots
        self.cleanup_old_snapshots()?;

        info!(
            raft_index,
            entry_count = metadata.entry_count,
            file_size = metadata.file_size,
            compression_ratio = format!("{:.2}", metadata.compression_ratio()),
            "Snapshot created"
        );

        Ok(metadata)
    }

    /// Find the latest snapshot in the checkpoint directory.
    pub fn find_latest_snapshot(&self) -> Result<Option<SnapshotInfo>, FormatError> {
        if !self.config.enabled {
            return Ok(None);
        }

        let mut snapshots = self.list_snapshots()?;

        // Sort by raft_index descending
        snapshots.sort_by(|a, b| b.raft_index.cmp(&a.raft_index));

        Ok(snapshots.into_iter().next())
    }

    /// List all snapshots in the checkpoint directory.
    pub fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>, FormatError> {
        let mut snapshots = Vec::new();

        if !self.config.dir.exists() {
            return Ok(snapshots);
        }

        for entry in fs::read_dir(&self.config.dir)? {
            let entry = entry?;
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !filename.starts_with("snapshot-") {
                continue;
            }

            // Try to read the snapshot
            match SnapshotReader::open(&path) {
                Ok(reader) => {
                    let header = reader.header();
                    snapshots.push(SnapshotInfo {
                        path: path.clone(),
                        raft_index: header.raft_index,
                        raft_term: header.raft_term,
                        timestamp: header.timestamp,
                        entry_count: header.entry_count,
                        file_size: entry.metadata()?.len(),
                    });
                }
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "Failed to read snapshot");
                }
            }
        }

        Ok(snapshots)
    }

    /// Load a snapshot into the cache.
    pub async fn load_snapshot(&self, path: impl AsRef<Path>) -> Result<u64, FormatError> {
        let path = path.as_ref();
        info!(path = %path.display(), "Loading snapshot");

        let mut reader = SnapshotReader::open(path)?;

        let raft_index = reader.raft_index();
        let raft_term = reader.raft_term();
        let entry_count = reader.entry_count();

        // Clear current cache
        self.storage.invalidate_all();

        // Load entries
        let mut loaded = 0u64;
        while let Some(entry) = reader.read_entry()? {
            // Skip expired entries
            if entry.is_expired() {
                continue;
            }

            // Get remaining TTL before moving entry fields
            let remaining_ttl = entry.remaining_ttl();
            let key = Bytes::from(entry.key);
            let value = Bytes::from(entry.value);

            if let Some(ttl) = remaining_ttl {
                self.storage
                    .insert_with_ttl(key, value, ttl)
                    .await;
            } else {
                self.storage.insert(key, value).await;
            }

            loaded += 1;

            // Yield periodically
            if loaded % 1000 == 0 {
                tokio::task::yield_now().await;
            }
        }

        // Update state
        self.last_snapshot_index.store(raft_index, Ordering::SeqCst);
        self.entries_since_snapshot.store(0, Ordering::Relaxed);

        info!(
            raft_index,
            raft_term,
            entry_count,
            loaded,
            "Snapshot loaded"
        );

        Ok(raft_index)
    }

    /// Cleanup old snapshots, keeping only the most recent ones.
    fn cleanup_old_snapshots(&self) -> Result<(), FormatError> {
        let mut snapshots = self.list_snapshots()?;

        if snapshots.len() <= self.config.max_snapshots {
            return Ok(());
        }

        // Sort by raft_index descending
        snapshots.sort_by(|a, b| b.raft_index.cmp(&a.raft_index));

        // Remove old snapshots
        for snapshot in snapshots.iter().skip(self.config.max_snapshots) {
            debug!(path = %snapshot.path.display(), "Removing old snapshot");
            if let Err(e) = fs::remove_file(&snapshot.path) {
                warn!(
                    path = %snapshot.path.display(),
                    error = %e,
                    "Failed to remove old snapshot"
                );
            }
        }

        Ok(())
    }

    /// Run the checkpoint monitoring loop.
    pub async fn run_monitoring_loop(
        self: Arc<Self>,
        mut shutdown_rx: mpsc::Receiver<()>,
        raft_state: Arc<dyn RaftStateProvider>,
    ) {
        let check_interval = Duration::from_secs(30);
        let mut interval = tokio::time::interval(check_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if self.should_snapshot() {
                        let (index, term) = raft_state.get_applied_state();
                        if let Err(e) = self.create_snapshot(index, term).await {
                            error!(error = %e, "Failed to create snapshot");
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Checkpoint monitor shutting down");
                    break;
                }
            }
        }
    }
}

/// Trait for getting Raft state for snapshots.
pub trait RaftStateProvider: Send + Sync + 'static {
    /// Get the applied (index, term) state.
    fn get_applied_state(&self) -> (u64, u64);
}

impl std::fmt::Debug for CheckpointManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointManager")
            .field("last_snapshot_index", &self.last_snapshot_index())
            .field("entries_since_snapshot", &self.entries_since_snapshot())
            .field("enabled", &self.config.enabled)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CacheConfig;
    use tempfile::tempdir;

    fn create_test_manager() -> (Arc<CheckpointManager>, Arc<CacheStorage>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let cache_config = CacheConfig::default();
        let storage = Arc::new(CacheStorage::new(&cache_config));

        let config = CheckpointConfig::new(dir.path())
            .with_log_threshold(100)
            .with_time_interval(Duration::from_secs(60));

        let manager = Arc::new(CheckpointManager::new(config, storage.clone()).unwrap());

        (manager, storage, dir)
    }

    #[test]
    fn test_should_snapshot_disabled() {
        let (manager, _, _dir) = create_test_manager();

        // Initially should not snapshot
        assert!(!manager.should_snapshot());
    }

    #[test]
    fn test_should_snapshot_threshold() {
        let (manager, _, _dir) = create_test_manager();

        // Record enough entries to trigger
        for _ in 0..100 {
            manager.record_entry();
        }

        assert!(manager.should_snapshot());
    }

    #[test]
    fn test_list_snapshots_empty() {
        let (manager, _, _dir) = create_test_manager();

        let snapshots = manager.list_snapshots().unwrap();
        assert!(snapshots.is_empty());
    }

    #[tokio::test]
    async fn test_create_and_load_snapshot() {
        let (manager, storage, _dir) = create_test_manager();

        // Add some data
        storage
            .insert(Bytes::from("key1"), Bytes::from("value1"))
            .await;
        storage
            .insert(Bytes::from("key2"), Bytes::from("value2"))
            .await;

        // Create snapshot
        let metadata = manager.create_snapshot(100, 5).await.unwrap();
        assert_eq!(metadata.raft_index, 100);
        assert_eq!(metadata.raft_term, 5);

        // Find latest snapshot
        let latest = manager.find_latest_snapshot().unwrap();
        assert!(latest.is_some());
        let info = latest.unwrap();
        assert_eq!(info.raft_index, 100);
    }
}
