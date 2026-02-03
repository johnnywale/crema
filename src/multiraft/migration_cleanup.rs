//! Cleanup handlers for failed migrations.
//!
//! When a migration fails, various resources may be left in an inconsistent state:
//! - Partial data on the target node
//! - Checkpoint files that are no longer needed
//! - Raft learner configurations that should be removed
//!
//! This module provides cleanup handlers to ensure these resources are properly
//! cleaned up when a migration fails or is cancelled.
//!
//! ## Crash Recovery
//!
//! Pending cleanup tasks can be persisted to survive crashes. Use the
//! `CleanupPersistence` trait to implement storage, and call `load_and_reschedule_cleanups`
//! on startup to resume interrupted cleanups.

use crate::error::Result;
use crate::types::NodeId;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task::JoinSet;
use uuid::Uuid;

use super::shard::ShardId;

/// A pending cleanup task that can be persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingCleanupTask {
    /// The migration ID being cleaned up.
    pub migration_id: Uuid,
    /// The shard being migrated.
    pub shard_id: ShardId,
    /// The target node of the failed migration.
    pub target_node: NodeId,
    /// When the cleanup was scheduled (Unix timestamp in millis).
    pub scheduled_at: u64,
    /// Number of retry attempts.
    pub retry_count: u32,
}

impl PendingCleanupTask {
    /// Create a new pending cleanup task.
    pub fn new(migration_id: Uuid, shard_id: ShardId, target_node: NodeId) -> Self {
        Self {
            migration_id,
            shard_id,
            target_node,
            scheduled_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            retry_count: 0,
        }
    }
}

/// Trait for persisting cleanup tasks to survive crashes.
#[async_trait::async_trait]
pub trait CleanupPersistence: Send + Sync + std::fmt::Debug {
    /// Save a pending cleanup task.
    async fn save_cleanup_task(&self, task: &PendingCleanupTask) -> Result<()>;

    /// Remove a cleanup task (after successful completion).
    async fn remove_cleanup_task(&self, migration_id: Uuid) -> Result<()>;

    /// Load all pending cleanup tasks (for restart recovery).
    async fn load_pending_cleanup_tasks(&self) -> Result<Vec<PendingCleanupTask>>;
}

/// No-op cleanup persistence for when persistence is not needed.
#[derive(Debug, Default)]
pub struct NoOpCleanupPersistence;

#[async_trait::async_trait]
impl CleanupPersistence for NoOpCleanupPersistence {
    async fn save_cleanup_task(&self, _task: &PendingCleanupTask) -> Result<()> {
        Ok(())
    }

    async fn remove_cleanup_task(&self, _migration_id: Uuid) -> Result<()> {
        Ok(())
    }

    async fn load_pending_cleanup_tasks(&self) -> Result<Vec<PendingCleanupTask>> {
        Ok(Vec::new())
    }
}

/// Resource types that may need cleanup after a failed migration.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CleanupResource {
    /// Partial data transferred to the target node.
    TargetShardData {
        shard_id: ShardId,
        target_node: NodeId,
    },
    /// Checkpoint file that should be deleted.
    CheckpointFile { migration_id: Uuid, path: PathBuf },
    /// Raft learner that should be removed from the group.
    RaftLearner {
        shard_id: ShardId,
        learner_node: NodeId,
    },
    /// Temporary files created during migration.
    TempFile { path: PathBuf },
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
    async fn remove_raft_learner(&self, shard_id: ShardId, learner_node: NodeId) -> Result<()>;

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

    async fn remove_raft_learner(&self, _shard_id: ShardId, _learner_node: NodeId) -> Result<()> {
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
///
/// **Task Tracking**: All spawned cleanup tasks are tracked using a JoinSet,
/// enabling graceful shutdown and ensuring no orphaned tasks.
///
/// **Crash Recovery**: When configured with a `CleanupPersistence` implementation,
/// pending cleanup tasks are persisted to disk and can be recovered after crashes.
pub struct MigrationCleanupManager {
    /// The cleanup handler implementation.
    handler: Arc<dyn MigrationCleanupHandler>,
    /// Pending cleanup tasks tracked by migration ID.
    pending_cleanups: Mutex<HashSet<Uuid>>,
    /// JoinSet to track spawned cleanup tasks for graceful shutdown.
    cleanup_tasks: Mutex<JoinSet<Uuid>>,
    /// Optional persistence for crash recovery.
    persistence: Option<Arc<dyn CleanupPersistence>>,
}

impl std::fmt::Debug for MigrationCleanupManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigrationCleanupManager")
            .field("handler", &self.handler)
            .field("pending_cleanups", &self.pending_cleanups.lock().len())
            .field("active_tasks", &self.cleanup_tasks.lock().len())
            .field("has_persistence", &self.persistence.is_some())
            .finish()
    }
}

impl MigrationCleanupManager {
    /// Create a new cleanup manager with the given handler.
    pub fn new(handler: Arc<dyn MigrationCleanupHandler>) -> Self {
        Self {
            handler,
            pending_cleanups: Mutex::new(HashSet::new()),
            cleanup_tasks: Mutex::new(JoinSet::new()),
            persistence: None,
        }
    }

    /// Create a cleanup manager with persistence for crash recovery.
    pub fn with_persistence(
        handler: Arc<dyn MigrationCleanupHandler>,
        persistence: Arc<dyn CleanupPersistence>,
    ) -> Self {
        Self {
            handler,
            pending_cleanups: Mutex::new(HashSet::new()),
            cleanup_tasks: Mutex::new(JoinSet::new()),
            persistence: Some(persistence),
        }
    }

    /// Create a cleanup manager with a no-op handler (for testing).
    pub fn noop() -> Self {
        Self::new(Arc::new(NoOpCleanupHandler))
    }

    /// Load and reschedule any pending cleanup tasks from persistence.
    ///
    /// Call this on startup to resume cleanup tasks that were interrupted
    /// by a crash. Returns the number of tasks rescheduled.
    pub async fn load_and_reschedule_cleanups(self: &Arc<Self>) -> Result<usize> {
        let Some(persistence) = &self.persistence else {
            return Ok(0);
        };

        let pending_tasks = persistence.load_pending_cleanup_tasks().await?;
        let count = pending_tasks.len();

        for task in pending_tasks {
            tracing::info!(
                migration_id = %task.migration_id,
                shard_id = task.shard_id,
                target_node = task.target_node,
                retry_count = task.retry_count,
                "Rescheduling cleanup task from persistence"
            );

            // Schedule the cleanup (will skip if already pending)
            self.schedule_cleanup_internal(
                task.migration_id,
                task.shard_id,
                task.target_node,
                false, // Don't persist again
            );
        }

        if count > 0 {
            tracing::info!(count, "Rescheduled cleanup tasks from persistence");
        }

        Ok(count)
    }

    /// Schedule cleanup for a failed migration.
    ///
    /// This spawns a background task to clean up all resources associated
    /// with the failed migration. The cleanup runs asynchronously and does
    /// not block the caller.
    ///
    /// The spawned task is tracked in the internal JoinSet, allowing for
    /// graceful shutdown via `shutdown()` or `wait_for_all_cleanups()`.
    ///
    /// Note: Persistence (if configured) is done asynchronously in the spawned task.
    pub fn schedule_cleanup(
        self: &Arc<Self>,
        migration_id: Uuid,
        shard_id: ShardId,
        target_node: NodeId,
    ) {
        self.schedule_cleanup_internal(migration_id, shard_id, target_node, true);
    }

    /// Internal scheduling method with persistence control.
    fn schedule_cleanup_internal(
        self: &Arc<Self>,
        migration_id: Uuid,
        shard_id: ShardId,
        target_node: NodeId,
        persist: bool,
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
        let persistence_clone = self.persistence.clone();

        // Track the spawned task in JoinSet for graceful shutdown
        self.cleanup_tasks.lock().spawn(async move {
            // Persist the cleanup task at the start (inside async context)
            if persist {
                if let Some(persistence) = &persistence_clone {
                    let task = PendingCleanupTask::new(migration_id, shard_id, target_node);
                    if let Err(e) = persistence.save_cleanup_task(&task).await {
                        tracing::warn!(
                            %migration_id,
                            error = %e,
                            "Failed to persist cleanup task - cleanup will not survive crash"
                        );
                    }
                }
            }

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
                // Remove from persistence on successful cleanup
                if let Some(persistence) = &manager.persistence {
                    if let Err(e) = persistence.remove_cleanup_task(migration_id).await {
                        tracing::warn!(
                            %migration_id,
                            error = %e,
                            "Failed to remove cleanup task from persistence"
                        );
                    }
                }

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
                // Note: We keep the task in persistence on failure so it can be retried
            }

            // Return migration_id so we can track completion
            migration_id
        });
    }

    /// Execute cleanup synchronously and return the result.
    ///
    /// This is useful when you need to wait for cleanup to complete.
    ///
    /// # Cleanup Order and Safety
    ///
    /// The cleanup follows a specific order to prevent data orphaning:
    /// 1. Target data cleanup - removes partial data on target node
    /// 2. Checkpoint files - ONLY if target data cleanup succeeds
    /// 3. Raft learner removal - independent of above
    /// 4. Temp files - always cleaned up
    ///
    /// If target data cleanup fails, checkpoint files are preserved to maintain
    /// a record of the orphaned partial data for future cleanup attempts.
    pub async fn execute_cleanup(
        &self,
        migration_id: Uuid,
        shard_id: ShardId,
        target_node: NodeId,
    ) -> CleanupResult {
        let mut result = CleanupResult::new();

        // 1. Clean up partial data on target
        let target_cleanup_succeeded = match self
            .handler
            .cleanup_target_data(shard_id, target_node, migration_id)
            .await
        {
            Ok(()) => {
                result.record_success(CleanupResource::TargetShardData {
                    shard_id,
                    target_node,
                });
                true
            }
            Err(e) => {
                result.record_failure(
                    CleanupResource::TargetShardData {
                        shard_id,
                        target_node,
                    },
                    e.to_string(),
                );
                false
            }
        };

        // 2. Clean up checkpoint files ONLY if target data cleanup succeeded
        // Preserving checkpoints when target cleanup fails ensures we have a record
        // of the orphaned partial data for future cleanup attempts
        if target_cleanup_succeeded {
            match self.handler.cleanup_checkpoint_files(migration_id).await {
                Ok(paths) => {
                    for path in paths {
                        result
                            .record_success(CleanupResource::CheckpointFile { migration_id, path });
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
        } else {
            tracing::warn!(
                %migration_id,
                shard_id,
                "Skipping checkpoint cleanup - target data cleanup failed, preserving records"
            );
        }

        // 3. Remove Raft learner (independent of checkpoint cleanup)
        match self
            .handler
            .remove_raft_learner(shard_id, target_node)
            .await
        {
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

        // 4. Clean up temp files (always safe to clean)
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

    /// Get the count of active cleanup tasks.
    pub fn active_task_count(&self) -> usize {
        self.cleanup_tasks.lock().len()
    }

    /// Wait for all active cleanup tasks to complete.
    ///
    /// This is useful during graceful shutdown to ensure all cleanup
    /// operations have finished before the system shuts down.
    pub async fn wait_for_all_cleanups(&self) {
        loop {
            // Check if there are any tasks left
            let has_tasks = !self.cleanup_tasks.lock().is_empty();
            if !has_tasks {
                break;
            }

            // Poll one task to completion
            let task_result: Option<std::result::Result<Uuid, tokio::task::JoinError>> = {
                let mut tasks = self.cleanup_tasks.lock();
                // Use try_join_next to avoid holding lock across await
                tasks.try_join_next()
            };

            match task_result {
                Some(Ok(migration_id)) => {
                    tracing::debug!(%migration_id, "Cleanup task completed during shutdown");
                }
                Some(Err(e)) => {
                    tracing::warn!(error = %e, "Cleanup task panicked during shutdown");
                }
                None => {
                    // No tasks ready, yield and try again
                    tokio::task::yield_now().await;
                }
            }
        }

        tracing::info!("All cleanup tasks completed");
    }

    /// Shutdown the cleanup manager, cancelling any pending tasks.
    ///
    /// This aborts all running cleanup tasks immediately. Use
    /// `wait_for_all_cleanups()` first if you want to allow tasks to complete.
    pub fn shutdown(&self) {
        let mut tasks = self.cleanup_tasks.lock();
        tasks.abort_all();
        tracing::info!(
            aborted_tasks = tasks.len(),
            "Migration cleanup manager shutdown, aborted tasks"
        );
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
        let result = manager.execute_cleanup(Uuid::new_v4(), 0, 1).await;

        // No-op handler should succeed for everything
        assert!(result.is_success());
    }
}
