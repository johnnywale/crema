//! End-to-end migration orchestration.
//!
//! This module provides the high-level orchestration for shard migrations,
//! coordinating between the migration coordinator, placement, registry,
//! and routing components.

use crate::error::{Error, Result};
use crate::types::NodeId;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::migration::{
    MigrationPhase, SharedRateLimiter, ShardMigration, ShardMigrationCoordinator, TransferBatch,
};
use super::migration_metrics::MigrationMetrics;
use super::migration_routing::{DualWriteTracker, MigrationRouter, MigrationRoutingStrategy, RoutingDecision};
use super::shard::ShardId;
use super::shard_placement::{ShardMovement, ShardPlacement};
use super::shard_registry::{ShardLifecycleState, ShardRegistry};

/// Command for ownership transfer that can be committed via Raft.
///
/// This command represents an atomic ownership change that must be
/// consistent across all nodes in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MigrationCommand {
    /// Transfer ownership of a shard from one node to another.
    TransferOwnership {
        /// Migration ID for tracking.
        migration_id: [u8; 16], // Uuid as bytes for serialization
        /// Shard being transferred.
        shard_id: ShardId,
        /// Source node.
        from_node: NodeId,
        /// Target node.
        to_node: NodeId,
        /// Epoch for ordering.
        epoch: u64,
    },
    /// Abort a migration and rollback ownership.
    AbortMigration {
        /// Migration ID.
        migration_id: [u8; 16],
        /// Shard ID.
        shard_id: ShardId,
        /// Reason for abort.
        reason: String,
        /// Coordinator term when this abort was issued.
        /// Used for zombie fencing - rejects abort from old coordinators.
        coordinator_term: u64,
    },
    /// Update shard placement configuration.
    UpdatePlacement {
        /// Shard ID.
        shard_id: ShardId,
        /// New replica nodes.
        replicas: Vec<NodeId>,
        /// Epoch for ordering.
        epoch: u64,
    },
}

impl MigrationCommand {
    /// Create a transfer ownership command.
    pub fn transfer_ownership(
        migration_id: Uuid,
        shard_id: ShardId,
        from_node: NodeId,
        to_node: NodeId,
        epoch: u64,
    ) -> Self {
        Self::TransferOwnership {
            migration_id: *migration_id.as_bytes(),
            shard_id,
            from_node,
            to_node,
            epoch,
        }
    }

    /// Create an abort migration command.
    ///
    /// # Arguments
    ///
    /// * `migration_id` - The migration to abort
    /// * `shard_id` - The shard being migrated
    /// * `reason` - Why the migration is being aborted
    /// * `coordinator_term` - The term of the coordinator issuing this abort.
    ///   Used for zombie fencing - only aborts from the current term are accepted.
    pub fn abort_migration(
        migration_id: Uuid,
        shard_id: ShardId,
        reason: String,
        coordinator_term: u64,
    ) -> Self {
        Self::AbortMigration {
            migration_id: *migration_id.as_bytes(),
            shard_id,
            reason,
            coordinator_term,
        }
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::Internal(e.to_string()))
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data).map_err(|e| Error::Internal(e.to_string()))
    }

    /// Get the migration ID if this command has one.
    pub fn migration_id(&self) -> Option<Uuid> {
        match self {
            Self::TransferOwnership { migration_id, .. } => Some(Uuid::from_bytes(*migration_id)),
            Self::AbortMigration { migration_id, .. } => Some(Uuid::from_bytes(*migration_id)),
            Self::UpdatePlacement { .. } => None,
        }
    }

    /// Get the shard ID this command affects.
    pub fn shard_id(&self) -> ShardId {
        match self {
            Self::TransferOwnership { shard_id, .. } => *shard_id,
            Self::AbortMigration { shard_id, .. } => *shard_id,
            Self::UpdatePlacement { shard_id, .. } => *shard_id,
        }
    }
}

/// Callback for proposing migration commands via Raft.
///
/// Implementors should propose the command through their Raft group
/// and return once it's committed.
#[async_trait::async_trait]
pub trait MigrationRaftProposer: Send + Sync + std::fmt::Debug {
    /// Propose a migration command and wait for it to be committed.
    async fn propose_migration(&self, command: MigrationCommand) -> Result<()>;
}

/// Callback for transferring data between nodes.
#[async_trait::async_trait]
pub trait DataTransporter: Send + Sync + std::fmt::Debug {
    /// Fetch a batch of entries from a shard on the source node.
    async fn fetch_batch(
        &self,
        source_node: NodeId,
        shard_id: ShardId,
        last_key: Option<&[u8]>,
        batch_size: usize,
    ) -> Result<TransferBatch>;

    /// Apply a batch of entries to the local shard.
    async fn apply_batch(&self, shard_id: ShardId, batch: TransferBatch) -> Result<()>;

    /// Get the total entry count for a shard on a node.
    async fn get_shard_entry_count(&self, node_id: NodeId, shard_id: ShardId) -> Result<u64>;

    /// Get the approximate size in bytes for a shard on a node.
    async fn get_shard_size(&self, node_id: NodeId, shard_id: ShardId) -> Result<u64>;
}

/// Configuration for pipelined streaming.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Channel buffer size between reader and writer tasks.
    pub channel_buffer: usize,
    /// Number of batches to prefetch.
    pub prefetch_count: usize,
    /// Whether to enable pipelining (can be disabled for debugging).
    pub enabled: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            channel_buffer: 4,
            prefetch_count: 2,
            enabled: true,
        }
    }
}

/// High-level migration orchestrator.
///
/// Coordinates all aspects of shard migration including:
/// - Planning and scheduling migrations
/// - Data transfer orchestration
/// - Atomic ownership handover via Raft
/// - Registry and placement updates
#[derive(Debug)]
pub struct MigrationOrchestrator {
    /// This node's ID.
    node_id: NodeId,
    /// Migration coordinator for state management.
    coordinator: Arc<ShardMigrationCoordinator>,
    /// Shard registry for metadata.
    registry: Arc<ShardRegistry>,
    /// Shard placement for hash ring management.
    placement: Arc<ShardPlacement>,
    /// Migration router for routing decisions.
    router: Arc<MigrationRouter>,
    /// Metrics.
    metrics: Arc<MigrationMetrics>,
    /// Raft proposer for atomic commits.
    raft_proposer: RwLock<Option<Arc<dyn MigrationRaftProposer>>>,
    /// Data transporter for data transfer.
    data_transporter: RwLock<Option<Arc<dyn DataTransporter>>>,
    /// Whether orchestration is running.
    running: AtomicBool,
    /// Active orchestrations by shard.
    active_orchestrations: RwLock<HashMap<ShardId, OrchestrationState>>,
    /// Shared rate limiter for bandwidth control.
    rate_limiter: Arc<SharedRateLimiter>,
    /// Pipeline configuration.
    pipeline_config: PipelineConfig,
    /// Tracker for failed dual-writes that need reconciliation before commit.
    dual_write_tracker: Arc<DualWriteTracker>,
    /// Current coordinator term for zombie fencing.
    /// Commands from old coordinators (stale terms) are rejected.
    coordinator_term: AtomicU64,
}

/// State of an ongoing orchestration.
#[derive(Debug, Clone)]
pub struct OrchestrationState {
    /// Migration ID.
    pub migration_id: Uuid,
    /// Shard being migrated.
    pub shard_id: ShardId,
    /// Current phase.
    pub phase: MigrationPhase,
    /// When orchestration started.
    pub started_at: Instant,
    /// Last progress update.
    pub last_update: Instant,
    /// Routing strategy for this migration.
    pub routing_strategy: MigrationRoutingStrategy,
}

impl MigrationOrchestrator {
    /// Create a new migration orchestrator.
    pub fn new(
        node_id: NodeId,
        coordinator: Arc<ShardMigrationCoordinator>,
        registry: Arc<ShardRegistry>,
        placement: Arc<ShardPlacement>,
    ) -> Self {
        let rate_limiter = Arc::new(SharedRateLimiter::new(coordinator.config()));
        Self {
            node_id,
            coordinator,
            registry,
            placement,
            router: Arc::new(MigrationRouter::new()),
            metrics: Arc::new(MigrationMetrics::new()),
            raft_proposer: RwLock::new(None),
            data_transporter: RwLock::new(None),
            running: AtomicBool::new(false),
            active_orchestrations: RwLock::new(HashMap::new()),
            rate_limiter,
            pipeline_config: PipelineConfig::default(),
            dual_write_tracker: Arc::new(DualWriteTracker::new()),
            coordinator_term: AtomicU64::new(0),
        }
    }

    /// Create with custom pipeline configuration.
    pub fn with_pipeline_config(mut self, config: PipelineConfig) -> Self {
        self.pipeline_config = config;
        self
    }

    /// Set the Raft proposer for atomic commits.
    pub fn set_raft_proposer(&self, proposer: Arc<dyn MigrationRaftProposer>) {
        *self.raft_proposer.write() = Some(proposer);
    }

    /// Set the data transporter.
    pub fn set_data_transporter(&self, transporter: Arc<dyn DataTransporter>) {
        *self.data_transporter.write() = Some(transporter);
    }

    /// Get the migration router.
    pub fn router(&self) -> &Arc<MigrationRouter> {
        &self.router
    }

    /// Get the metrics.
    pub fn metrics(&self) -> &Arc<MigrationMetrics> {
        &self.metrics
    }

    /// Get the current coordinator term.
    pub fn coordinator_term(&self) -> u64 {
        self.coordinator_term.load(Ordering::SeqCst)
    }

    /// Set the coordinator term (called when this node becomes coordinator/leader).
    ///
    /// The term should be monotonically increasing. This is typically tied to
    /// the Raft term or a similar epoch counter.
    pub fn set_coordinator_term(&self, term: u64) {
        let old_term = self.coordinator_term.swap(term, Ordering::SeqCst);
        if term != old_term {
            tracing::info!(
                old_term,
                new_term = term,
                "Coordinator term updated"
            );
        }
    }

    /// Increment the coordinator term and return the new value.
    pub fn increment_coordinator_term(&self) -> u64 {
        let new_term = self.coordinator_term.fetch_add(1, Ordering::SeqCst) + 1;
        tracing::info!(new_term, "Coordinator term incremented");
        new_term
    }

    /// Validate that a command's term is not stale.
    ///
    /// Commands from old coordinators (with stale terms) must be rejected
    /// to prevent zombie coordinators from interfering with ongoing migrations.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the term is valid (>= current term)
    /// * `Err(StaleCoordinatorTerm)` if the term is stale
    pub fn validate_coordinator_term(&self, command_term: u64) -> Result<()> {
        let current_term = self.coordinator_term();
        if command_term < current_term {
            tracing::warn!(
                command_term,
                current_term,
                "Rejecting command with stale coordinator term"
            );
            return Err(Error::Internal(format!(
                "Stale coordinator term: command has term {}, current term is {}",
                command_term, current_term
            )));
        }
        Ok(())
    }

    /// Check if we're ready to orchestrate (have required dependencies).
    pub fn is_ready(&self) -> bool {
        self.raft_proposer.read().is_some() && self.data_transporter.read().is_some()
    }

    /// Start orchestrating a migration for a shard movement.
    ///
    /// This is the main entry point for initiating a migration.
    pub async fn start_migration(
        &self,
        movement: ShardMovement,
        strategy: Option<MigrationRoutingStrategy>,
    ) -> Result<Uuid> {
        // Check prerequisites
        if !self.is_ready() {
            return Err(Error::Internal(
                "Orchestrator not ready: missing raft proposer or data transporter".to_string(),
            ));
        }

        if self.coordinator.is_paused() {
            return Err(Error::MigrationPaused);
        }

        // Plan the migration
        let migration = self.coordinator.plan_migration(movement).await?;
        let migration_id = migration.id;

        // Update registry state
        self.registry
            .set_state(migration.shard_id, ShardLifecycleState::Migrating);

        // Record orchestration state
        let orch_state = OrchestrationState {
            migration_id,
            shard_id: migration.shard_id,
            phase: MigrationPhase::Planned,
            started_at: Instant::now(),
            last_update: Instant::now(),
            routing_strategy: strategy.unwrap_or_else(|| self.router.default_strategy()),
        };
        self.active_orchestrations
            .write()
            .insert(migration.shard_id, orch_state);

        self.metrics.record_migration_start();

        tracing::info!(
            migration_id = %migration_id,
            shard_id = migration.shard_id,
            from_node = migration.from_node,
            to_node = migration.to_node,
            "Migration orchestration started"
        );

        Ok(migration_id)
    }

    /// Execute the full migration flow for a shard.
    ///
    /// This runs all phases: streaming, catch-up, commit, and cleanup.
    pub async fn execute_migration(&self, shard_id: ShardId) -> Result<()> {
        let migration = self
            .coordinator
            .get_migration(shard_id)
            .ok_or(Error::MigrationNotFound(shard_id))?;

        // Phase 2a: Stream data
        self.execute_streaming(shard_id, &migration).await?;

        // Phase 2b: Catch up
        self.execute_catchup(shard_id).await?;

        // Phase 3: Atomic commit via Raft
        self.execute_commit(shard_id).await?;

        // Phase 4: Cleanup
        self.execute_cleanup(shard_id).await?;

        Ok(())
    }

    /// Execute the streaming phase (Phase 2a).
    async fn execute_streaming(&self, shard_id: ShardId, migration: &ShardMigration) -> Result<()> {
        let transporter = self
            .data_transporter
            .read()
            .clone()
            .ok_or_else(|| Error::Internal("Data transporter not set".to_string()))?;

        // Get shard stats
        let total_entries = transporter
            .get_shard_entry_count(migration.from_node, shard_id)
            .await?;
        let total_bytes = transporter
            .get_shard_size(migration.from_node, shard_id)
            .await?;

        // Start streaming phase
        self.coordinator
            .start_streaming(shard_id, total_entries, total_bytes)
            .await?;

        self.update_orchestration_phase(shard_id, MigrationPhase::Streaming);

        tracing::info!(
            shard_id,
            total_entries,
            total_bytes,
            pipelined = self.pipeline_config.enabled,
            "Streaming phase started"
        );

        if self.pipeline_config.enabled {
            self.execute_streaming_pipelined(shard_id, migration, transporter)
                .await
        } else {
            self.execute_streaming_sequential(shard_id, migration, transporter)
                .await
        }
    }

    /// Execute streaming with pipelined reader/writer tasks.
    ///
    /// This separates fetching from applying, allowing I/O overlap for better throughput.
    async fn execute_streaming_pipelined(
        &self,
        shard_id: ShardId,
        migration: &ShardMigration,
        transporter: Arc<dyn DataTransporter>,
    ) -> Result<()> {
        let batch_size = self.coordinator.config().batch_size;
        let start_time = Instant::now();
        let migration_id = migration.id;
        let from_node = migration.from_node;

        // Shared counters for progress tracking
        let total_transferred = Arc::new(AtomicU64::new(0));
        let bytes_transferred = Arc::new(AtomicU64::new(0));

        // Channel for passing batches from reader to writer
        let (tx, mut rx) = mpsc::channel::<TransferBatch>(self.pipeline_config.channel_buffer);

        // Spawn reader task
        let reader_transporter = Arc::clone(&transporter);
        let reader_paused = Arc::new(AtomicBool::new(false));
        let reader_paused_check = Arc::clone(&reader_paused);
        let coordinator_for_pause = Arc::clone(&self.coordinator);

        let reader_handle = tokio::spawn(async move {
            let mut last_key: Option<Vec<u8>> = None;
            let mut sequence = 0u64;

            loop {
                // Check for pause
                if coordinator_for_pause.is_paused() {
                    reader_paused_check.store(true, Ordering::SeqCst);
                    break;
                }

                // Fetch batch from source
                let batch_result = reader_transporter
                    .fetch_batch(from_node, shard_id, last_key.as_deref(), batch_size)
                    .await;

                match batch_result {
                    Ok(mut batch) => {
                        batch.sequence = sequence;
                        sequence += 1;

                        let is_final = batch.is_final;

                        // Update last key for next fetch
                        if let Some(last_entry) = batch.entries.last() {
                            last_key = Some(last_entry.key.clone());
                        }

                        // Send to writer
                        if tx.send(batch).await.is_err() {
                            // Writer dropped, stop reading
                            break;
                        }

                        if is_final {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::error!(shard_id, error = %e, "Reader task failed");
                        break;
                    }
                }
            }
        });

        // Writer task runs in current task
        let mut last_key_for_checkpoint: Option<Vec<u8>> = None;
        let mut writer_error: Option<Error> = None;

        while let Some(batch) = rx.recv().await {
            // Check if reader detected pause
            if reader_paused.load(Ordering::SeqCst) {
                writer_error = Some(Error::MigrationPaused);
                break;
            }

            let batch_entries = batch.len() as u64;
            let batch_bytes = batch.size() as u64;

            if !batch.is_empty() {
                // Update last key for checkpoint
                if let Some(last_entry) = batch.entries.last() {
                    last_key_for_checkpoint = Some(last_entry.key.clone());
                }

                // Rate limiting
                self.rate_limiter.acquire(migration_id, batch_bytes).await;

                // Apply batch to target
                if let Err(e) = transporter.apply_batch(shard_id, batch).await {
                    writer_error = Some(e);
                    break;
                }

                // Update counters
                total_transferred.fetch_add(batch_entries, Ordering::Relaxed);
                bytes_transferred.fetch_add(batch_bytes, Ordering::Relaxed);

                // Calculate rate
                let elapsed = start_time.elapsed().as_secs_f64();
                let total_bytes = bytes_transferred.load(Ordering::Relaxed);
                let rate = if elapsed > 0.0 {
                    total_bytes as f64 / elapsed
                } else {
                    0.0
                };

                // Update progress
                if let Err(e) = self
                    .coordinator
                    .update_progress(
                        shard_id,
                        batch_entries,
                        batch_bytes,
                        rate,
                        last_key_for_checkpoint.clone(),
                    )
                    .await
                {
                    writer_error = Some(e);
                    break;
                }

                self.metrics.record_transfer(batch_entries, batch_bytes, rate);

                tracing::debug!(
                    shard_id,
                    total_transferred = total_transferred.load(Ordering::Relaxed),
                    bytes_transferred = total_bytes,
                    rate = format!("{:.2} bytes/sec", rate),
                    "Transfer progress (pipelined)"
                );
            }
        }

        // Wait for reader to finish
        let _ = reader_handle.await;

        // Clean up rate limiter
        self.rate_limiter.remove_migration(migration_id);

        // Check for errors
        if let Some(e) = writer_error {
            return Err(e);
        }

        let final_transferred = total_transferred.load(Ordering::Relaxed);
        let final_bytes = bytes_transferred.load(Ordering::Relaxed);

        tracing::info!(
            shard_id,
            total_transferred = final_transferred,
            bytes_transferred = final_bytes,
            duration_ms = start_time.elapsed().as_millis(),
            "Streaming phase completed (pipelined)"
        );

        Ok(())
    }

    /// Execute streaming sequentially (fallback for debugging or simple cases).
    async fn execute_streaming_sequential(
        &self,
        shard_id: ShardId,
        migration: &ShardMigration,
        transporter: Arc<dyn DataTransporter>,
    ) -> Result<()> {
        let batch_size = self.coordinator.config().batch_size;
        let mut last_key: Option<Vec<u8>> = None;
        let mut total_transferred: u64 = 0;
        let mut bytes_transferred: u64 = 0;
        let start_time = Instant::now();

        loop {
            // Check for pause
            if self.coordinator.is_paused() {
                tracing::warn!(shard_id, "Migration paused during streaming");
                return Err(Error::MigrationPaused);
            }

            // Fetch batch from source
            let batch = transporter
                .fetch_batch(
                    migration.from_node,
                    shard_id,
                    last_key.as_deref(),
                    batch_size,
                )
                .await?;

            let batch_entries = batch.len() as u64;
            let batch_bytes = batch.size() as u64;
            let is_final = batch.is_final;

            if !batch.is_empty() {
                // Update last key for resumption
                if let Some(last_entry) = batch.entries.last() {
                    last_key = Some(last_entry.key.clone());
                }

                // Rate limiting
                self.rate_limiter.acquire(migration.id, batch_bytes).await;

                // Apply batch to target (local node)
                transporter.apply_batch(shard_id, batch).await?;

                total_transferred += batch_entries;
                bytes_transferred += batch_bytes;

                // Calculate rate
                let elapsed = start_time.elapsed().as_secs_f64();
                let rate = if elapsed > 0.0 {
                    bytes_transferred as f64 / elapsed
                } else {
                    0.0
                };

                // Update progress
                self.coordinator
                    .update_progress(shard_id, batch_entries, batch_bytes, rate, last_key.clone())
                    .await?;

                self.metrics
                    .record_transfer(batch_entries, batch_bytes, rate);

                tracing::debug!(
                    shard_id,
                    total_transferred,
                    bytes_transferred,
                    rate = format!("{:.2} bytes/sec", rate),
                    "Transfer progress (sequential)"
                );
            }

            if is_final {
                break;
            }
        }

        // Clean up rate limiter
        self.rate_limiter.remove_migration(migration.id);

        tracing::info!(
            shard_id,
            total_transferred,
            bytes_transferred,
            duration_ms = start_time.elapsed().as_millis(),
            "Streaming phase completed (sequential)"
        );

        Ok(())
    }

    /// Execute the catch-up phase (Phase 2b).
    async fn execute_catchup(&self, shard_id: ShardId) -> Result<()> {
        self.coordinator.start_catchup(shard_id).await?;
        self.update_orchestration_phase(shard_id, MigrationPhase::CatchingUp);

        // In a real implementation, this would:
        // 1. Enable dual-writes to both nodes
        // 2. Wait for the new node to catch up
        // 3. Verify data consistency
        //
        // For now, we'll do a simple verification pause
        tracing::info!(shard_id, "Catch-up phase started");

        // Give time for any in-flight writes to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        // CRITICAL: Reconcile any failed dual-writes before proceeding to commit
        self.reconcile_dual_write_failures(shard_id).await?;

        tracing::info!(shard_id, "Catch-up phase completed");

        Ok(())
    }

    /// Reconcile failed dual-writes before commit phase.
    ///
    /// During dual-write mode, secondary writes to the target node may fail
    /// while primary writes succeed. This method retries those failed writes
    /// to ensure data consistency before ownership transfer.
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if all failures were reconciled successfully, or an error
    /// if reconciliation failed and the migration should be aborted.
    async fn reconcile_dual_write_failures(&self, shard_id: ShardId) -> Result<()> {
        let failures = self.dual_write_tracker.get_failures_for_reconciliation(shard_id);

        if failures.is_empty() {
            tracing::debug!(shard_id, "No dual-write failures to reconcile");
            return Ok(());
        }

        tracing::info!(
            shard_id,
            failure_count = failures.len(),
            "Reconciling dual-write failures before commit"
        );

        let transporter = self
            .data_transporter
            .read()
            .clone()
            .ok_or_else(|| Error::Internal("Data transporter not set".to_string()))?;

        let migration = self
            .coordinator
            .get_migration(shard_id)
            .ok_or(Error::MigrationNotFound(shard_id))?;

        let mut reconciled_keys = Vec::new();
        let mut failed_reconciliation = Vec::new();

        for failure in failures {
            // Create a single-entry batch for this failed write
            let entry = super::migration::TransferEntry {
                key: failure.key.clone(),
                value: failure.value.clone(),
                expires_at_nanos: None, // We don't track TTL for dual-writes
                version: 0,
            };

            let batch = TransferBatch::new(
                migration.id,
                0, // sequence doesn't matter for reconciliation
                vec![entry],
                false,
            );

            // Try to apply this batch to the target
            match transporter.apply_batch(shard_id, batch).await {
                Ok(()) => {
                    reconciled_keys.push(failure.key);
                    tracing::debug!(
                        shard_id,
                        "Reconciled dual-write failure"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        shard_id,
                        error = %e,
                        retry_count = failure.retry_count,
                        "Failed to reconcile dual-write"
                    );
                    failed_reconciliation.push((failure, e));
                }
            }
        }

        // Clear successfully reconciled entries
        if !reconciled_keys.is_empty() {
            self.dual_write_tracker.clear_keys(shard_id, &reconciled_keys);
            tracing::info!(
                shard_id,
                reconciled_count = reconciled_keys.len(),
                "Successfully reconciled dual-write failures"
            );
        }

        // If there are still failures, the migration cannot proceed safely
        if !failed_reconciliation.is_empty() {
            return Err(Error::Internal(format!(
                "Failed to reconcile {} dual-write failures - migration cannot proceed safely",
                failed_reconciliation.len()
            )));
        }

        Ok(())
    }

    /// Record a dual-write failure for later reconciliation.
    ///
    /// Call this when a secondary write fails during dual-write mode.
    pub fn record_dual_write_failure(
        &self,
        shard_id: ShardId,
        key: Vec<u8>,
        value: Vec<u8>,
        error: impl Into<String>,
    ) {
        self.dual_write_tracker.record_failure(shard_id, key, value, error);
    }

    /// Check if there are pending dual-write failures for a shard.
    pub fn has_pending_dual_write_failures(&self, shard_id: ShardId) -> bool {
        self.dual_write_tracker.has_pending_failures(shard_id)
    }

    /// Get the dual-write tracker for external use.
    pub fn dual_write_tracker(&self) -> &Arc<DualWriteTracker> {
        &self.dual_write_tracker
    }

    /// Apply a migration command with zombie fencing validation.
    ///
    /// This is the primary entry point for applying migration commands that have
    /// been committed via Raft. It validates the command's term to prevent stale
    /// coordinators from interfering with ongoing migrations.
    ///
    /// # Zombie Fencing
    ///
    /// A "zombie" coordinator is a node that was previously the leader but has
    /// since been replaced (e.g., due to network partition). The zombie might
    /// still try to send abort commands for migrations it started. These commands
    /// must be rejected because:
    ///
    /// 1. The new coordinator may have already completed the migration
    /// 2. The new coordinator may have started a different migration
    /// 3. Accepting stale commands could corrupt cluster state
    ///
    /// We fence zombies by requiring each command to include the coordinator term
    /// when it was issued. Commands with terms older than the current term are
    /// rejected.
    pub async fn apply_migration_command(&self, command: MigrationCommand) -> Result<()> {
        match command {
            MigrationCommand::TransferOwnership {
                migration_id,
                shard_id,
                from_node,
                to_node,
                epoch,
            } => {
                // Transfer commands are ordered by epoch, not term
                self.apply_transfer_ownership(
                    Uuid::from_bytes(migration_id),
                    shard_id,
                    from_node,
                    to_node,
                    epoch,
                )
                .await
            }
            MigrationCommand::AbortMigration {
                migration_id,
                shard_id,
                reason,
                coordinator_term,
            } => {
                // CRITICAL: Validate coordinator term for zombie fencing
                self.validate_coordinator_term(coordinator_term)?;

                self.apply_abort_migration(
                    Uuid::from_bytes(migration_id),
                    shard_id,
                    reason,
                )
                .await
            }
            MigrationCommand::UpdatePlacement {
                shard_id,
                replicas,
                epoch,
            } => {
                // Placement updates are ordered by epoch
                self.apply_update_placement(shard_id, replicas, epoch).await
            }
        }
    }

    /// Apply a transfer ownership command.
    async fn apply_transfer_ownership(
        &self,
        migration_id: Uuid,
        shard_id: ShardId,
        from_node: NodeId,
        to_node: NodeId,
        epoch: u64,
    ) -> Result<()> {
        tracing::info!(
            %migration_id,
            shard_id,
            from_node,
            to_node,
            epoch,
            "Applying ownership transfer"
        );

        // Update registry - set_primary_if_newer handles epoch checking
        if !self.registry.set_primary_if_newer(shard_id, to_node, epoch) {
            if let Some(metadata) = self.registry.get(shard_id) {
                tracing::warn!(
                    shard_id,
                    command_epoch = epoch,
                    current_epoch = metadata.leader_epoch,
                    "Ignoring stale transfer command"
                );
            }
        }

        Ok(())
    }

    /// Apply an abort migration command.
    async fn apply_abort_migration(
        &self,
        migration_id: Uuid,
        shard_id: ShardId,
        reason: String,
    ) -> Result<()> {
        tracing::warn!(
            %migration_id,
            shard_id,
            reason,
            "Applying migration abort"
        );

        // If we have an active orchestration for this shard, cancel it
        {
            let mut orchestrations = self.active_orchestrations.write();
            if let Some(state) = orchestrations.remove(&shard_id) {
                if state.migration_id == migration_id {
                    tracing::info!(
                        %migration_id,
                        shard_id,
                        "Cancelled active orchestration due to abort command"
                    );
                }
            }
        }

        // Mark migration as cancelled in coordinator
        // Ignore error if migration not found (may have already been cleaned up)
        let _ = self.coordinator.cancel_migration(shard_id).await;

        // Clean up any tracked dual-write failures
        self.dual_write_tracker.clear_reconciled(shard_id);

        Ok(())
    }

    /// Apply a placement update command.
    async fn apply_update_placement(
        &self,
        shard_id: ShardId,
        replicas: Vec<NodeId>,
        epoch: u64,
    ) -> Result<()> {
        tracing::info!(
            shard_id,
            ?replicas,
            epoch,
            "Applying placement update"
        );

        // Update registry with new replica set - set_replicas_if_newer handles epoch checking
        if !self.registry.set_replicas_if_newer(shard_id, replicas, epoch) {
            if let Some(metadata) = self.registry.get(shard_id) {
                tracing::warn!(
                    shard_id,
                    command_epoch = epoch,
                    current_epoch = metadata.leader_epoch,
                    "Ignoring stale placement update"
                );
            }
        }

        Ok(())
    }

    /// Execute the atomic commit phase (Phase 3).
    ///
    /// This is the critical phase where ownership changes hands atomically via Raft.
    ///
    /// IMPORTANT: During this phase, writes to the shard are BLOCKED to prevent
    /// split-brain writes. The sequence is:
    /// 1. Block writes to old node (set routing strategy to BlockWrites)
    /// 2. Propose ownership transfer via Raft
    /// 3. Wait for Raft commit
    /// 4. Update routing to NewPrimaryOnly (writes go to new owner)
    /// 5. If failure, restore original routing strategy
    async fn execute_commit(&self, shard_id: ShardId) -> Result<()> {
        let migration = self
            .coordinator
            .get_migration(shard_id)
            .ok_or(Error::MigrationNotFound(shard_id))?;

        let proposer = self
            .raft_proposer
            .read()
            .clone()
            .ok_or_else(|| Error::Internal("Raft proposer not set".to_string()))?;

        // Start commit phase
        let migration_id = self.coordinator.start_commit(shard_id).await?;
        self.update_orchestration_phase(shard_id, MigrationPhase::Committing);

        // CRITICAL: Atomically read original strategy and set BlockWrites in one lock acquisition
        // This prevents TOCTOU race where strategy could change between read and write.
        let original_strategy = {
            let mut orchestrations = self.active_orchestrations.write();
            if let Some(state) = orchestrations.get_mut(&shard_id) {
                let original = state.routing_strategy;
                state.routing_strategy = MigrationRoutingStrategy::BlockWrites;
                state.last_update = Instant::now();
                tracing::debug!(
                    shard_id,
                    old_strategy = %original,
                    new_strategy = %MigrationRoutingStrategy::BlockWrites,
                    "Atomically updated orchestration routing strategy"
                );
                original
            } else {
                self.router.default_strategy()
            }
        };

        tracing::info!(
            migration_id = %migration_id,
            shard_id,
            "Commit phase started - WRITES BLOCKED - proposing ownership transfer via Raft"
        );

        // Get current epoch for ordering
        let current_epoch = self
            .registry
            .get(shard_id)
            .map(|m| m.leader_epoch)
            .unwrap_or(0);

        // Create the ownership transfer command
        let command = MigrationCommand::transfer_ownership(
            migration_id,
            shard_id,
            migration.from_node,
            migration.to_node,
            current_epoch + 1,
        );

        // Propose via Raft and wait for commit
        // This is the atomic point - if this succeeds, ownership has transferred
        match proposer.propose_migration(command).await {
            Ok(()) => {
                tracing::info!(
                    migration_id = %migration_id,
                    shard_id,
                    "Ownership transfer committed via Raft"
                );

                // Update local state after successful Raft commit
                self.apply_ownership_transfer(shard_id, migration.to_node, current_epoch + 1);

                // Switch routing to new primary - writes now go to the new owner
                self.set_orchestration_strategy(shard_id, MigrationRoutingStrategy::NewPrimaryOnly);

                tracing::info!(
                    migration_id = %migration_id,
                    shard_id,
                    new_owner = migration.to_node,
                    "Writes now routed to new owner"
                );

                Ok(())
            }
            Err(e) => {
                tracing::error!(
                    migration_id = %migration_id,
                    shard_id,
                    error = %e,
                    "Ownership transfer failed - restoring write routing"
                );

                // CRITICAL: Restore original routing strategy so writes can resume to old node
                self.set_orchestration_strategy(shard_id, original_strategy);

                // Rollback: mark migration as failed
                self.coordinator
                    .fail_migration(shard_id, format!("Raft commit failed: {}", e))
                    .await?;

                // Restore registry state
                self.registry
                    .set_state(shard_id, ShardLifecycleState::Active);

                Err(e)
            }
        }
    }

    /// Update the routing strategy for an active orchestration.
    fn set_orchestration_strategy(&self, shard_id: ShardId, strategy: MigrationRoutingStrategy) {
        if let Some(state) = self.active_orchestrations.write().get_mut(&shard_id) {
            let old_strategy = state.routing_strategy;
            state.routing_strategy = strategy;
            state.last_update = Instant::now();

            tracing::debug!(
                shard_id,
                old_strategy = %old_strategy,
                new_strategy = %strategy,
                "Updated orchestration routing strategy"
            );
        }
    }

    /// Apply ownership transfer locally after Raft commit.
    fn apply_ownership_transfer(&self, shard_id: ShardId, new_owner: NodeId, epoch: u64) {
        // Update registry
        self.registry.set_primary_if_newer(shard_id, new_owner, epoch);

        // Update placement if needed (commit the pending ring)
        if self.placement.has_pending() {
            self.placement.commit();
        }

        tracing::debug!(
            shard_id,
            new_owner,
            epoch,
            "Applied ownership transfer locally"
        );
    }

    /// Execute the cleanup phase (Phase 4).
    async fn execute_cleanup(&self, shard_id: ShardId) -> Result<()> {
        self.update_orchestration_phase(shard_id, MigrationPhase::Cleanup);

        tracing::info!(shard_id, "Cleanup phase started");

        // Complete the migration
        let migration = self.coordinator.complete_migration(shard_id).await?;

        // Update registry to active
        let new_replicas = self.placement.get_shard_nodes(shard_id);
        self.registry.set_replicas(shard_id, new_replicas);
        self.registry
            .set_state(shard_id, ShardLifecycleState::Active);

        // Remove from active orchestrations
        self.active_orchestrations.write().remove(&shard_id);

        // Record metrics
        self.metrics.record_migration_complete(migration.duration());

        tracing::info!(
            migration_id = %migration.id,
            shard_id,
            duration_ms = migration.duration().as_millis(),
            "Migration orchestration completed successfully"
        );

        Ok(())
    }

    /// Update orchestration phase tracking.
    fn update_orchestration_phase(&self, shard_id: ShardId, phase: MigrationPhase) {
        if let Some(state) = self.active_orchestrations.write().get_mut(&shard_id) {
            state.phase = phase;
            state.last_update = Instant::now();
        }
    }

    /// Cancel an in-progress migration.
    pub async fn cancel_migration(&self, shard_id: ShardId) -> Result<()> {
        let migration = self.coordinator.cancel_migration(shard_id).await?;

        // Restore registry state
        self.registry
            .set_state(shard_id, ShardLifecycleState::Active);

        // Abort any pending placement changes
        self.placement.abort();

        // Remove from active orchestrations
        self.active_orchestrations.write().remove(&shard_id);

        self.metrics.record_migration_cancelled();

        tracing::warn!(
            migration_id = %migration.id,
            shard_id,
            "Migration cancelled"
        );

        Ok(())
    }

    /// Get routing decision for a write operation.
    pub fn route_write(&self, shard_id: ShardId) -> RoutingDecision {
        // Check if shard is migrating
        if let Some(migration) = self.coordinator.get_migration(shard_id) {
            let strategy = self
                .active_orchestrations
                .read()
                .get(&shard_id)
                .map(|s| s.routing_strategy);
            self.router.route_write(shard_id, &migration, strategy)
        } else {
            // Not migrating - use normal routing
            let primary = self
                .registry
                .get(shard_id)
                .and_then(|m| m.primary_node)
                .unwrap_or(self.node_id);
            RoutingDecision::Single {
                node: primary,
                shard_id,
            }
        }
    }

    /// Get routing decision for a read operation.
    pub fn route_read(&self, shard_id: ShardId) -> RoutingDecision {
        // Check if shard is migrating
        if let Some(migration) = self.coordinator.get_migration(shard_id) {
            let strategy = self
                .active_orchestrations
                .read()
                .get(&shard_id)
                .map(|s| s.routing_strategy);
            self.router.route_read(shard_id, &migration, strategy)
        } else {
            // Not migrating - use normal routing
            let primary = self
                .registry
                .get(shard_id)
                .and_then(|m| m.primary_node)
                .unwrap_or(self.node_id);
            RoutingDecision::Single {
                node: primary,
                shard_id,
            }
        }
    }

    /// Handle a node joining the cluster.
    ///
    /// Calculates necessary shard movements and optionally starts migrations.
    pub fn handle_node_join(&self, node_id: NodeId) -> Vec<ShardMovement> {
        // Calculate movements
        let movements = self.placement.calculate_movements_on_add(node_id);

        tracing::info!(
            node_id,
            movements = movements.len(),
            "Calculated shard movements for node join"
        );

        movements
    }

    /// Handle a node leaving the cluster.
    ///
    /// Calculates necessary shard movements and optionally starts migrations.
    pub fn handle_node_leave(&self, node_id: NodeId) -> Vec<ShardMovement> {
        // Calculate movements
        let movements = self.placement.calculate_movements_on_remove(node_id);

        // Update registry
        self.registry.remove_node_from_replicas(node_id);
        self.registry.clear_primary_for_node(node_id);

        tracing::info!(
            node_id,
            movements = movements.len(),
            "Calculated shard movements for node leave"
        );

        movements
    }

    /// Get the current state of all active orchestrations.
    pub fn active_orchestrations(&self) -> Vec<OrchestrationState> {
        self.active_orchestrations.read().values().cloned().collect()
    }

    /// Check if a shard is being orchestrated.
    pub fn is_orchestrating(&self, shard_id: ShardId) -> bool {
        self.active_orchestrations.read().contains_key(&shard_id)
    }
}

/// No-op Raft proposer for testing.
#[derive(Debug)]
pub struct NoOpRaftProposer;

#[async_trait::async_trait]
impl MigrationRaftProposer for NoOpRaftProposer {
    async fn propose_migration(&self, command: MigrationCommand) -> Result<()> {
        tracing::debug!(?command, "NoOp: Would propose migration command");
        Ok(())
    }
}

/// No-op data transporter for testing.
#[derive(Debug)]
pub struct NoOpDataTransporter;

#[async_trait::async_trait]
impl DataTransporter for NoOpDataTransporter {
    async fn fetch_batch(
        &self,
        _source_node: NodeId,
        _shard_id: ShardId,
        _last_key: Option<&[u8]>,
        _batch_size: usize,
    ) -> Result<TransferBatch> {
        Ok(TransferBatch::new(Uuid::new_v4(), 0, vec![], true))
    }

    async fn apply_batch(&self, _shard_id: ShardId, _batch: TransferBatch) -> Result<()> {
        Ok(())
    }

    async fn get_shard_entry_count(&self, _node_id: NodeId, _shard_id: ShardId) -> Result<u64> {
        Ok(0)
    }

    async fn get_shard_size(&self, _node_id: NodeId, _shard_id: ShardId) -> Result<u64> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multiraft::migration::{InMemoryMigrationStore, MigrationConfig};
    use crate::multiraft::shard_placement::{MovementType, PlacementConfig};

    fn create_test_orchestrator() -> MigrationOrchestrator {
        let node_id = 1;
        let num_shards = 16;

        let state_store = Arc::new(InMemoryMigrationStore::new());
        let coordinator = Arc::new(ShardMigrationCoordinator::new(
            node_id,
            MigrationConfig::default(),
            state_store,
        ));
        let registry = Arc::new(ShardRegistry::new(num_shards));
        let placement = Arc::new(ShardPlacement::new(
            num_shards,
            PlacementConfig::new(3),
        ));

        MigrationOrchestrator::new(node_id, coordinator, registry, placement)
    }

    #[test]
    fn test_migration_command_serialization() {
        let cmd = MigrationCommand::transfer_ownership(Uuid::new_v4(), 5, 1, 2, 10);
        let bytes = cmd.to_bytes().unwrap();
        let decoded = MigrationCommand::from_bytes(&bytes).unwrap();
        assert_eq!(cmd, decoded);
    }

    #[test]
    fn test_migration_command_shard_id() {
        let cmd = MigrationCommand::transfer_ownership(Uuid::new_v4(), 7, 1, 2, 10);
        assert_eq!(cmd.shard_id(), 7);

        let abort = MigrationCommand::abort_migration(Uuid::new_v4(), 3, "test".to_string(), 1);
        assert_eq!(abort.shard_id(), 3);
    }

    #[test]
    fn test_orchestrator_not_ready_without_dependencies() {
        let orch = create_test_orchestrator();
        assert!(!orch.is_ready());
    }

    #[test]
    fn test_orchestrator_ready_with_dependencies() {
        let orch = create_test_orchestrator();
        orch.set_raft_proposer(Arc::new(NoOpRaftProposer));
        orch.set_data_transporter(Arc::new(NoOpDataTransporter));
        assert!(orch.is_ready());
    }

    #[tokio::test]
    async fn test_start_migration() {
        let orch = create_test_orchestrator();
        orch.set_raft_proposer(Arc::new(NoOpRaftProposer));
        orch.set_data_transporter(Arc::new(NoOpDataTransporter));

        let movement = ShardMovement::new(0, 1, 2, MovementType::AddReplica);
        let migration_id = orch.start_migration(movement, None).await.unwrap();

        assert!(orch.is_orchestrating(0));
        assert_eq!(orch.active_orchestrations().len(), 1);
    }

    #[tokio::test]
    async fn test_execute_full_migration() {
        let orch = create_test_orchestrator();
        orch.set_raft_proposer(Arc::new(NoOpRaftProposer));
        orch.set_data_transporter(Arc::new(NoOpDataTransporter));

        // Start migration
        let movement = ShardMovement::new(0, 1, 2, MovementType::AddReplica);
        orch.start_migration(movement, None).await.unwrap();

        // Execute full migration
        orch.execute_migration(0).await.unwrap();

        // Should be completed
        assert!(!orch.is_orchestrating(0));
    }

    #[tokio::test]
    async fn test_cancel_migration() {
        let orch = create_test_orchestrator();
        orch.set_raft_proposer(Arc::new(NoOpRaftProposer));
        orch.set_data_transporter(Arc::new(NoOpDataTransporter));

        // Start migration
        let movement = ShardMovement::new(0, 1, 2, MovementType::AddReplica);
        orch.start_migration(movement, None).await.unwrap();

        // Cancel it
        orch.cancel_migration(0).await.unwrap();

        assert!(!orch.is_orchestrating(0));
    }

    #[test]
    fn test_route_write_not_migrating() {
        let orch = create_test_orchestrator();

        // Set up registry with a primary
        orch.registry.set_primary(5, Some(3));

        let decision = orch.route_write(5);
        match decision {
            RoutingDecision::Single { node, shard_id } => {
                assert_eq!(node, 3);
                assert_eq!(shard_id, 5);
            }
            _ => panic!("Expected Single routing decision"),
        }
    }

    #[tokio::test]
    async fn test_route_write_during_migration() {
        let orch = create_test_orchestrator();
        orch.set_raft_proposer(Arc::new(NoOpRaftProposer));
        orch.set_data_transporter(Arc::new(NoOpDataTransporter));

        // Start migration
        let movement = ShardMovement::new(0, 1, 2, MovementType::AddReplica);
        orch.start_migration(movement, Some(MigrationRoutingStrategy::BlockWrites))
            .await
            .unwrap();

        let decision = orch.route_write(0);
        match decision {
            RoutingDecision::Blocked { shard_id, .. } => {
                assert_eq!(shard_id, 0);
            }
            _ => panic!("Expected Blocked routing decision"),
        }
    }

    #[test]
    fn test_handle_node_join() {
        let orch = create_test_orchestrator();

        // Add initial nodes
        orch.placement.add_node(1);
        orch.placement.add_node(2);
        orch.placement.add_node(3);

        // Handle node 4 joining
        let movements = orch.handle_node_join(4);

        // Should have some movements to node 4
        assert!(movements.iter().all(|m| m.to_node == 4));
    }

    #[test]
    fn test_handle_node_leave() {
        let orch = create_test_orchestrator();

        // Add initial nodes
        orch.placement.add_node(1);
        orch.placement.add_node(2);
        orch.placement.add_node(3);
        orch.placement.add_node(4);

        // Set up registry
        for shard_id in 0..4 {
            orch.registry.set_replicas(shard_id, vec![1, 2, 3, 4]);
            orch.registry.set_primary(shard_id, Some((shard_id % 4 + 1) as NodeId));
        }

        // Handle node 4 leaving
        let movements = orch.handle_node_leave(4);

        // Should have some movements from node 4
        assert!(movements.iter().all(|m| m.from_node == 4));
    }
}
