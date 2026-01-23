//! Cleanup handlers for failed migrations.
//!
//! When a migration fails, various resources may be left in an inconsistent state:
//! - Partial data on the target node
//! - Checkpoint files that are no longer needed
//! - Raft learner configurations that should be removed
//!
//! This module provides cleanup handlers to ensure these resources are properly
//! cleaned up when a migration fails or is cancelled.

use crate::error::{Result};
use crate::types::NodeId;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

use super::shard::ShardId;

/// Resource types that may need cleanup after a failed migration.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CleanupResource {
    /// Partial data transferred to the target node.
    TargetShardData {
        shard_id: ShardId,
        target_node: NodeId,
    },
    /// Checkpoint file that should be deleted.
    CheckpointFile {
        migration_id: Uuid,
        path: PathBuf,
    },
    /// Raft learner that should be removed from the group.
    RaftLearner {
        shard_id: ShardId,
        learner_node: NodeId,
    },
    /// Temporary files created during migration.
    TempFile {
        path: PathBuf,
    },
}

/// Result of a cleanup operation.
#[derive(Debug)]
pub struct CleanupResult {
    /// Resources that were successfully cleaned up.
    pub cleaned: Vec<CleanupResource>,
    /// Resources that failed to clean up, with error messages.
    pub failed: Vec<(CleanupResource, String)>,
}

impl CleanupResult {
    /// Create a new empty cleanup result.
    pub fn new() -> Self {
        Self {
            cleaned: Vec::new(),
            failed: Vec::new(),
        }
    }

    /// Check if all resources were cleaned up successfully.
    pub fn is_success(&self) -> bool {
        self.failed.is_empty()
    }

    /// Get the number of successfully cleaned resources.
    pub fn cleaned_count(&self) -> usize {
        self.cleaned.len()
    }

    /// Get the number of failed cleanups.
    pub fn failed_count(&self) -> usize {
        self.failed.len()
    }

    /// Record a successful cleanup.
    pub fn record_success(&mut self, resource: CleanupResource) {
        self.cleaned.push(resource);
    }

    /// Record a failed cleanup.
    pub fn record_failure(&mut self, resource: CleanupResource, error: impl Into<String>) {
        self.failed.push((resource, error.into()));
    }

    /// Merge another result into this one.
    pub fn merge(&mut self, other: CleanupResult) {
        self.cleaned.extend(other.cleaned);
        self.failed.extend(other.failed);
    }
}

impl Default for CleanupResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Handler for cleaning up resources after a failed migration.
#[async_trait::async_trait]
pub trait MigrationCleanupHandler: Send + Sync + std::fmt::Debug {
    /// Clean up partial data on the target node.
    ///
    /// This should remove any entries that were transferred as part of
    /// the failed migration.
    async fn cleanup_target_data(
        &self,
        shard_id: ShardId,
        target_node: NodeId,
        migration_id: Uuid,
    ) -> Result<()>;

    /// Clean up checkpoint files for a migration.
    async fn cleanup_checkpoint_files(&self, migration_id: Uuid) -> Result<Vec<PathBuf>>;

    /// Remove a learner from the Raft group.
    ///
    /// This should be called when a migration fails after the learner
    /// was added but before it was promoted to voter.
    async fn remove_raft_learner(
        &self,
        shard_id: ShardId,
        learner_node: NodeId,
    ) -> Result<()>;

    /// Clean up any temporary files associated with a migration.
    async fn cleanup_temp_files(&self, migration_id: Uuid) -> Result<Vec<PathBuf>>;
}

/// No-op cleanup handler for testing.
#[derive(Debug, Default)]
pub struct NoOpCleanupHandler;

#[async_trait::async_trait]
impl MigrationCleanupHandler for NoOpCleanupHandler {
    async fn cleanup_target_data(
        &self,
        _shard_id: ShardId,
        _target_node: NodeId,
        _migration_id: Uuid,
    ) -> Result<()> {
        Ok(())
    }

    async fn cleanup_checkpoint_files(&self, _migration_id: Uuid) -> Result<Vec<PathBuf>> {
        Ok(Vec::new())
    }

    async fn remove_raft_learner(
        &self,
        _shard_id: ShardId,
        _learner_node: NodeId,
    ) -> Result<()> {
        Ok(())
    }

    async fn cleanup_temp_files(&self, _migration_id: Uuid) -> Result<Vec<PathBuf>> {
        Ok(Vec::new())
    }
}

/// Manager for coordinating cleanup of failed migrations.
///
/// This struct coordinates the cleanup process and tracks what resources
/// need to be cleaned up. It can run cleanup asynchronously in the background
/// to avoid blocking the main migration flow.
#[derive(Debug)]
pub struct MigrationCleanupManager {
    /// The cleanup handler implementation.
    handler: Arc<dyn MigrationCleanupHandler>,
    /// Pending cleanup tasks.
    pending_cleanups: Mutex<HashSet<Uuid>>,
}

impl MigrationCleanupManager {
    /// Create a new cleanup manager with the given handler.
    pub fn new(handler: Arc<dyn MigrationCleanupHandler>) -> Self {
        Self {
            handler,
            pending_cleanups: Mutex::new(HashSet::new()),
        }
    }

    /// Create a cleanup manager with a no-op handler (for testing).
    pub fn noop() -> Self {
        Self::new(Arc::new(NoOpCleanupHandler))
    }

    /// Schedule cleanup for a failed migration.
    ///
    /// This spawns a background task to clean up all resources associated
    /// with the failed migration. The cleanup runs asynchronously and does
    /// not block the caller.
    pub fn schedule_cleanup(
        self: &Arc<Self>,
        migration_id: Uuid,
        shard_id: ShardId,
        target_node: NodeId,
    ) {
        // Check if cleanup is already scheduled
        {
            let mut pending = self.pending_cleanups.lock();
            if pending.contains(&migration_id) {
                tracing::debug!(
                    %migration_id,
                    "Cleanup already scheduled, skipping"
                );
                return;
            }
            pending.insert(migration_id);
        }

        let manager = Arc::clone(self);

        tokio::spawn(async move {
            tracing::info!(
                %migration_id,
                shard_id,
                target_node,
                "Starting cleanup for failed migration"
            );

            let result = manager
                .execute_cleanup(migration_id, shard_id, target_node)
                .await;

            // Remove from pending
            manager.pending_cleanups.lock().remove(&migration_id);

            if result.is_success() {
                tracing::info!(
                    %migration_id,
                    cleaned_count = result.cleaned_count(),
                    "Migration cleanup completed successfully"
                );
            } else {
                tracing::warn!(
                    %migration_id,
                    cleaned_count = result.cleaned_count(),
                    failed_count = result.failed_count(),
                    "Migration cleanup completed with failures"
                );
                for (resource, error) in &result.failed {
                    tracing::warn!(
                        %migration_id,
                        resource = ?resource,
                        error,
                        "Failed to clean up resource"
                    );
                }
            }
        });
    }

    /// Execute cleanup synchronously and return the result.
    ///
    /// This is useful when you need to wait for cleanup to complete.
    pub async fn execute_cleanup(
        &self,
        migration_id: Uuid,
        shard_id: ShardId,
        target_node: NodeId,
    ) -> CleanupResult {
        let mut result = CleanupResult::new();

        // 1. Clean up partial data on target
        match self
            .handler
            .cleanup_target_data(shard_id, target_node, migration_id)
            .await
        {
            Ok(()) => {
                result.record_success(CleanupResource::TargetShardData {
                    shard_id,
                    target_node,
                });
            }
            Err(e) => {
                result.record_failure(
                    CleanupResource::TargetShardData {
                        shard_id,
                        target_node,
                    },
                    e.to_string(),
                );
            }
        }

        // 2. Clean up checkpoint files
        match self.handler.cleanup_checkpoint_files(migration_id).await {
            Ok(paths) => {
                for path in paths {
                    result.record_success(CleanupResource::CheckpointFile {
                        migration_id,
                        path,
                    });
                }
            }
            Err(e) => {
                result.record_failure(
                    CleanupResource::CheckpointFile {
                        migration_id,
                        path: PathBuf::new(),
                    },
                    e.to_string(),
                );
            }
        }

        // 3. Remove Raft learner
        match self.handler.remove_raft_learner(shard_id, target_node).await {
            Ok(()) => {
                result.record_success(CleanupResource::RaftLearner {
                    shard_id,
                    learner_node: target_node,
                });
            }
            Err(e) => {
                result.record_failure(
                    CleanupResource::RaftLearner {
                        shard_id,
                        learner_node: target_node,
                    },
                    e.to_string(),
                );
            }
        }

        // 4. Clean up temp files
        match self.handler.cleanup_temp_files(migration_id).await {
            Ok(paths) => {
                for path in paths {
                    result.record_success(CleanupResource::TempFile { path });
                }
            }
            Err(e) => {
                result.record_failure(
                    CleanupResource::TempFile {
                        path: PathBuf::new(),
                    },
                    e.to_string(),
                );
            }
        }

        result
    }

    /// Check if a cleanup is pending for a migration.
    pub fn is_cleanup_pending(&self, migration_id: Uuid) -> bool {
        self.pending_cleanups.lock().contains(&migration_id)
    }

    /// Get the count of pending cleanups.
    pub fn pending_count(&self) -> usize {
        self.pending_cleanups.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cleanup_result() {
        let mut result = CleanupResult::new();
        assert!(result.is_success());

        result.record_success(CleanupResource::TempFile {
            path: PathBuf::from("/tmp/test"),
        });
        assert!(result.is_success());
        assert_eq!(result.cleaned_count(), 1);

        result.record_failure(
            CleanupResource::TargetShardData {
                shard_id: 0,
                target_node: 1,
            },
            "test error",
        );
        assert!(!result.is_success());
        assert_eq!(result.failed_count(), 1);
    }

    #[tokio::test]
    async fn test_noop_cleanup_manager() {
        let manager = MigrationCleanupManager::noop();
        let result = manager
            .execute_cleanup(Uuid::new_v4(), 0, 1)
            .await;

        // No-op handler should succeed for everything
        assert!(result.is_success());
    }
}
