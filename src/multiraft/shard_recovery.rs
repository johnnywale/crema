//! Shard recovery coordination for crash recovery.
//!
//! This module provides startup recovery for multiraft shards, loading
//! persisted state and snapshots from disk.
//!
//! # Recovery Flow
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Startup Recovery Flow                         │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ 1. Load shard registry from disk                                 │
//! │    └─ Recover shard topology and membership                      │
//! │                                                                  │
//! │ 2. For each registered shard:                                    │
//! │    ├─ Find latest snapshot                                       │
//! │    ├─ Load snapshot into CacheStorage                           │
//! │    ├─ Apply snapshot to MemStorage (Raft log)                   │
//! │    └─ Restore shard state (term, commit_index, etc.)            │
//! │                                                                  │
//! │ 3. Resume interrupted migrations                                 │
//! │    └─ Check migration state store                                │
//! │                                                                  │
//! │ 4. Mark shards as ready                                          │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use crate::error::Result;
use crate::multiraft::shard::{Shard, ShardConfig, ShardId, ShardState};
use crate::multiraft::shard_storage::{
    PersistedShardMetadata, ShardSnapshotInfo, ShardStorageConfig, ShardStorageManager,
};
use crate::types::NodeId;

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info};

/// Recovery statistics.
#[derive(Debug, Clone, Default)]
pub struct RecoveryStats {
    /// Number of shards recovered.
    pub shards_recovered: u32,
    /// Number of shards that failed recovery.
    pub shards_failed: u32,
    /// Total entries loaded from snapshots.
    pub entries_loaded: u64,
    /// Recovery duration in milliseconds.
    pub duration_ms: u64,
    /// Per-shard recovery details.
    pub shard_details: Vec<ShardRecoveryDetail>,
}

/// Recovery details for a single shard.
#[derive(Debug, Clone)]
pub struct ShardRecoveryDetail {
    /// Shard ID.
    pub shard_id: ShardId,
    /// Whether recovery succeeded.
    pub success: bool,
    /// Snapshot index recovered from.
    pub snapshot_index: Option<u64>,
    /// Entries loaded.
    pub entries_loaded: u64,
    /// Error message if failed.
    pub error: Option<String>,
}

/// Result of recovering a single shard.
pub struct RecoveredShard {
    /// The recovered shard.
    pub shard: Arc<Shard>,
    /// The snapshot info if recovered from snapshot.
    pub snapshot_info: Option<ShardSnapshotInfo>,
    /// Number of entries loaded.
    pub entries_loaded: u64,
}

/// Coordinates shard recovery on startup.
pub struct ShardRecoveryCoordinator {
    /// Node ID.
    node_id: NodeId,
    /// Storage manager.
    storage_manager: Arc<ShardStorageManager>,
    /// Recovery stats.
    stats: RwLock<RecoveryStats>,
}

impl ShardRecoveryCoordinator {
    /// Create a new recovery coordinator.
    pub fn new(node_id: NodeId, storage_manager: Arc<ShardStorageManager>) -> Self {
        Self {
            node_id,
            storage_manager,
            stats: RwLock::new(RecoveryStats::default()),
        }
    }

    /// Perform full recovery, returning recovered shards.
    pub async fn recover_all(&self) -> Result<HashMap<ShardId, RecoveredShard>> {
        let start = Instant::now();
        let mut recovered_shards = HashMap::new();

        info!(node_id = self.node_id, "Starting shard recovery");

        // Load shard metadata from registry
        let shard_metadata = self.storage_manager.get_all_shard_metadata();

        if shard_metadata.is_empty() {
            info!("No shards to recover (empty registry)");
            return Ok(recovered_shards);
        }

        info!(
            shard_count = shard_metadata.len(),
            "Found shards to recover"
        );

        for metadata in shard_metadata {
            let shard_id = metadata.shard_id;

            match self.recover_shard(&metadata).await {
                Ok(recovered) => {
                    let entries = recovered.entries_loaded;
                    recovered_shards.insert(shard_id, recovered);

                    let mut stats = self.stats.write();
                    stats.shards_recovered += 1;
                    stats.entries_loaded += entries;
                    stats.shard_details.push(ShardRecoveryDetail {
                        shard_id,
                        success: true,
                        snapshot_index: None,
                        entries_loaded: entries,
                        error: None,
                    });
                }
                Err(e) => {
                    error!(
                        shard_id = shard_id,
                        error = %e,
                        "Failed to recover shard"
                    );

                    let mut stats = self.stats.write();
                    stats.shards_failed += 1;
                    stats.shard_details.push(ShardRecoveryDetail {
                        shard_id,
                        success: false,
                        snapshot_index: None,
                        entries_loaded: 0,
                        error: Some(e.to_string()),
                    });
                }
            }
        }

        let duration = start.elapsed();
        self.stats.write().duration_ms = duration.as_millis() as u64;

        let stats = self.stats.read().clone();
        info!(
            shards_recovered = stats.shards_recovered,
            shards_failed = stats.shards_failed,
            entries_loaded = stats.entries_loaded,
            duration_ms = stats.duration_ms,
            "Shard recovery completed"
        );

        Ok(recovered_shards)
    }

    /// Recover a single shard from persisted state.
    async fn recover_shard(&self, metadata: &PersistedShardMetadata) -> Result<RecoveredShard> {
        let shard_id = metadata.shard_id;
        info!(
            shard_id = shard_id,
            term = metadata.term,
            commit_index = metadata.commit_index,
            "Recovering shard"
        );

        // Register shard with storage manager
        let shard_config = ShardConfig::new(shard_id, metadata.total_shards)
            .with_replicas(metadata.replicas)
            .with_max_capacity(metadata.max_capacity);

        self.storage_manager.register_shard(&shard_config);

        // Create the shard
        let shard = Shard::new(shard_config).await?;

        // Restore state from metadata
        shard.set_term(metadata.term);
        shard.set_commit_index(metadata.commit_index);

        if let Some(leader) = metadata.leader {
            shard.set_leader(Some(leader));
        }

        for member in &metadata.members {
            shard.add_member(*member);
        }

        // Parse and set state
        let state = match metadata.state.as_str() {
            "active" => ShardState::Active,
            "initializing" => ShardState::Initializing,
            "transferring" => ShardState::Transferring,
            "removing" => ShardState::Removing,
            "stopped" => ShardState::Stopped,
            _ => ShardState::Initializing,
        };
        shard.set_state(state);

        // Find and load latest snapshot
        let mut entries_loaded = 0u64;
        let mut snapshot_info = None;

        match self.storage_manager.find_latest_snapshot(shard_id)? {
            Some(info) => {
                info!(
                    shard_id = shard_id,
                    raft_index = info.raft_index,
                    entry_count = info.entry_count,
                    "Found snapshot to recover from"
                );

                entries_loaded = self
                    .storage_manager
                    .load_snapshot(shard_id, &info, shard.storage())
                    .await?;

                snapshot_info = Some(info);
            }
            None => {
                debug!(shard_id = shard_id, "No snapshot found, starting fresh");
            }
        }

        let shard = Arc::new(shard);

        Ok(RecoveredShard {
            shard,
            snapshot_info,
            entries_loaded,
        })
    }

    /// Recover a specific shard by ID.
    pub async fn recover_shard_by_id(&self, shard_id: ShardId) -> Result<Option<RecoveredShard>> {
        let metadata = match self.storage_manager.get_shard_metadata(shard_id) {
            Some(m) => m,
            None => return Ok(None),
        };

        let recovered = self.recover_shard(&metadata).await?;
        Ok(Some(recovered))
    }

    /// Get recovery statistics.
    pub fn stats(&self) -> RecoveryStats {
        self.stats.read().clone()
    }

    /// Check if any shards need recovery.
    pub fn has_shards_to_recover(&self) -> bool {
        !self.storage_manager.get_all_shard_metadata().is_empty()
    }

    /// Get list of shard IDs that can be recovered.
    pub fn recoverable_shard_ids(&self) -> Vec<ShardId> {
        self.storage_manager
            .get_all_shard_metadata()
            .into_iter()
            .map(|m| m.shard_id)
            .collect()
    }
}

impl std::fmt::Debug for ShardRecoveryCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardRecoveryCoordinator")
            .field("node_id", &self.node_id)
            .field("stats", &*self.stats.read())
            .finish()
    }
}

/// Builder for creating a recovery coordinator with custom options.
pub struct RecoveryCoordinatorBuilder {
    node_id: NodeId,
    storage_config: ShardStorageConfig,
}

impl RecoveryCoordinatorBuilder {
    /// Create a new builder.
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            storage_config: ShardStorageConfig::default(),
        }
    }

    /// Set the storage configuration.
    pub fn with_storage_config(mut self, config: ShardStorageConfig) -> Self {
        self.storage_config = config;
        self
    }

    /// Set the base directory.
    pub fn with_base_dir(mut self, dir: impl Into<std::path::PathBuf>) -> Self {
        self.storage_config.base_dir = dir.into();
        self
    }

    /// Build the recovery coordinator.
    pub fn build(self) -> Result<(ShardRecoveryCoordinator, Arc<ShardStorageManager>)> {
        let storage_manager = Arc::new(ShardStorageManager::new(
            self.storage_config,
            self.node_id,
        )?);

        let coordinator = ShardRecoveryCoordinator::new(self.node_id, storage_manager.clone());

        Ok((coordinator, storage_manager))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_recovery_coordinator_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = ShardStorageConfig::new(temp_dir.path());
        let storage_manager = Arc::new(ShardStorageManager::new(config, 1).unwrap());
        let coordinator = ShardRecoveryCoordinator::new(1, storage_manager);

        assert!(!coordinator.has_shards_to_recover());
        assert!(coordinator.recoverable_shard_ids().is_empty());
    }

    #[tokio::test]
    async fn test_recovery_with_no_shards() {
        let temp_dir = TempDir::new().unwrap();
        let config = ShardStorageConfig::new(temp_dir.path());
        let storage_manager = Arc::new(ShardStorageManager::new(config, 1).unwrap());
        let coordinator = ShardRecoveryCoordinator::new(1, storage_manager);

        let recovered = coordinator.recover_all().await.unwrap();
        assert!(recovered.is_empty());

        let stats = coordinator.stats();
        assert_eq!(stats.shards_recovered, 0);
        assert_eq!(stats.shards_failed, 0);
    }

    #[tokio::test]
    async fn test_recovery_with_saved_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let config = ShardStorageConfig::new(temp_dir.path());
        let storage_manager = Arc::new(ShardStorageManager::new(config, 1).unwrap());

        // Save shard metadata
        let metadata = PersistedShardMetadata {
            shard_id: 0,
            total_shards: 4,
            replicas: 3,
            max_capacity: 100_000,
            state: "active".to_string(),
            leader: Some(1),
            members: vec![1, 2, 3],
            term: 5,
            commit_index: 100,
            applied_index: 100,
            last_snapshot_index: 50,
            last_snapshot_term: 4,
        };
        storage_manager.save_shard_metadata(metadata).unwrap();

        let coordinator = ShardRecoveryCoordinator::new(1, storage_manager);

        assert!(coordinator.has_shards_to_recover());
        assert_eq!(coordinator.recoverable_shard_ids(), vec![0]);

        // Recover
        let recovered = coordinator.recover_all().await.unwrap();
        assert_eq!(recovered.len(), 1);

        let shard = &recovered[&0].shard;
        assert_eq!(shard.id(), 0);
        assert_eq!(shard.term(), 5);
        assert_eq!(shard.commit_index(), 100);
        assert_eq!(shard.state(), ShardState::Active);
    }

    #[tokio::test]
    async fn test_builder_pattern() {
        let temp_dir = TempDir::new().unwrap();

        let (coordinator, storage_manager) = RecoveryCoordinatorBuilder::new(1)
            .with_base_dir(temp_dir.path())
            .build()
            .unwrap();

        assert!(!coordinator.has_shards_to_recover());
    }
}
