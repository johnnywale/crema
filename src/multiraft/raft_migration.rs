//! Raft-Native Shard Migration.
//!
//! This module implements shard rebalancing using Raft's native membership
//! change mechanism instead of manual key-by-key streaming.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                     Raft-Native Migration Flow                       │
//! │                                                                      │
//! │  1. AddLearner    2. WaitSnapshot    3. PromoteVoter   4. RemoveOld │
//! │  ┌──────────┐     ┌──────────────┐   ┌─────────────┐   ┌──────────┐ │
//! │  │ConfChange│ ──► │Raft Snapshot │ ─►│ConfChange   │ ─►│ConfChange│ │
//! │  │AddLearner│     │Auto Transfer │   │AddNode      │   │RemoveNode│ │
//! │  └──────────┘     └──────────────┘   └─────────────┘   └──────────┘ │
//! │                                                                      │
//! │  Data transfer happens automatically via Raft's snapshot mechanism  │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Benefits over Manual Streaming
//!
//! - **Consistency**: Uses Raft's proven consistency guarantees
//! - **Efficiency**: Snapshots transfer data in bulk, not entry-by-entry
//! - **Simplicity**: Leverages existing Raft infrastructure
//! - **Safety**: Joint consensus prevents split-brain during transitions

use crate::error::{Error, Result};
use crate::multiraft::migration_cleanup::MigrationCleanupManager;
use crate::multiraft::shard::ShardId;
use crate::types::NodeId;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// ============================================================================
// Types
// ============================================================================

/// Type of Raft membership change for a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftChangeType {
    /// Add a node as a learner (non-voting replica).
    /// Learners receive log entries and snapshots but don't vote.
    AddLearner,

    /// Promote a learner to a full voter.
    PromoteToVoter,

    /// Add a node directly as a voter (use with caution).
    AddVoter,

    /// Remove a node from the Raft group.
    RemoveNode,

    /// Transfer leadership to another node.
    TransferLeader,
}

/// A Raft membership change request for a shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftMembershipChange {
    /// Unique ID for this change.
    pub id: Uuid,

    /// The shard (Raft group) being modified.
    pub shard_id: ShardId,

    /// Type of change.
    pub change_type: RaftChangeType,

    /// Target node for the change.
    pub target_node: NodeId,

    /// Optional: node to remove after adding (for transfers).
    pub remove_after: Option<NodeId>,

    /// Priority (lower = higher priority).
    pub priority: u32,

    /// Created timestamp.
    pub created_at: u64,

    /// Coordinator term for fencing (prevents zombie coordinators).
    pub coordinator_term: u64,
}

impl RaftMembershipChange {
    /// Create a new membership change.
    pub fn new(shard_id: ShardId, change_type: RaftChangeType, target_node: NodeId) -> Self {
        Self {
            id: Uuid::new_v4(),
            shard_id,
            change_type,
            target_node,
            remove_after: None,
            priority: 100,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            coordinator_term: 0,
        }
    }

    /// Create a transfer operation (add new node, then remove old).
    pub fn transfer(shard_id: ShardId, from_node: NodeId, to_node: NodeId) -> Self {
        Self {
            id: Uuid::new_v4(),
            shard_id,
            change_type: RaftChangeType::AddLearner,
            target_node: to_node,
            remove_after: Some(from_node),
            priority: 100,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            coordinator_term: 0,
        }
    }

    /// Set priority.
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    /// Set coordinator term for fencing.
    pub fn with_coordinator_term(mut self, term: u64) -> Self {
        self.coordinator_term = term;
        self
    }
}

/// Phase of a Raft-native migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftMigrationPhase {
    /// Migration is planned but not started.
    Planned,

    /// Adding the target node as a learner.
    AddingLearner,

    /// Waiting for the learner to catch up via snapshot.
    WaitingForSnapshot,

    /// Learner is catching up with the log.
    CatchingUp,

    /// Promoting learner to voter.
    PromotingToVoter,

    /// Removing the old node (if this is a transfer).
    RemovingOldNode,

    /// Migration completed successfully.
    Completed,

    /// Migration failed.
    Failed,

    /// Migration was cancelled.
    Cancelled,
}

impl std::fmt::Display for RaftMigrationPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Planned => write!(f, "planned"),
            Self::AddingLearner => write!(f, "adding_learner"),
            Self::WaitingForSnapshot => write!(f, "waiting_for_snapshot"),
            Self::CatchingUp => write!(f, "catching_up"),
            Self::PromotingToVoter => write!(f, "promoting_to_voter"),
            Self::RemovingOldNode => write!(f, "removing_old_node"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// Progress information for a Raft-native migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftMigrationProgress {
    /// Current Raft index on the learner.
    pub learner_match_index: u64,

    /// Leader's current commit index.
    pub leader_commit_index: u64,

    /// Whether snapshot has been sent.
    pub snapshot_sent: bool,

    /// Whether snapshot has been applied.
    pub snapshot_applied: bool,

    /// Lag in entries (leader_commit - learner_match).
    pub lag_entries: u64,

    /// Estimated time to catch up.
    pub eta_seconds: Option<u64>,

    /// Last updated timestamp.
    pub last_updated: u64,
}

impl Default for RaftMigrationProgress {
    fn default() -> Self {
        Self {
            learner_match_index: 0,
            leader_commit_index: 0,
            snapshot_sent: false,
            snapshot_applied: false,
            lag_entries: 0,
            eta_seconds: None,
            last_updated: 0,
        }
    }
}

impl RaftMigrationProgress {
    /// Check if the learner has caught up (lag within threshold).
    pub fn is_caught_up(&self, threshold: u64) -> bool {
        self.snapshot_applied && self.lag_entries <= threshold
    }

    /// Calculate catch-up percentage.
    pub fn catch_up_percentage(&self) -> f64 {
        if self.leader_commit_index == 0 {
            return 100.0;
        }
        (self.learner_match_index as f64 / self.leader_commit_index as f64) * 100.0
    }
}

/// A Raft-native shard migration operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftShardMigration {
    /// Unique migration ID.
    pub id: Uuid,

    /// The membership change being executed.
    pub change: RaftMembershipChange,

    /// Current phase.
    pub phase: RaftMigrationPhase,

    /// Progress information.
    pub progress: RaftMigrationProgress,

    /// Error message if failed.
    pub error: Option<String>,

    /// When the migration started.
    pub started_at: u64,

    /// When the migration completed (or failed).
    pub completed_at: Option<u64>,

    /// Number of retry attempts.
    pub retry_count: u32,

    /// Phase start time (for detecting stalled migrations).
    #[serde(skip)]
    pub phase_started_at: Option<Instant>,
}

impl RaftShardMigration {
    /// Create a new migration from a membership change.
    pub fn new(change: RaftMembershipChange) -> Self {
        Self {
            id: change.id,
            change,
            phase: RaftMigrationPhase::Planned,
            progress: RaftMigrationProgress::default(),
            error: None,
            started_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            completed_at: None,
            retry_count: 0,
            phase_started_at: Some(Instant::now()),
        }
    }

    /// Transition to a new phase.
    pub fn transition_to(&mut self, phase: RaftMigrationPhase) {
        tracing::info!(
            migration_id = %self.id,
            shard_id = self.change.shard_id,
            from_phase = %self.phase,
            to_phase = %phase,
            "Migration phase transition"
        );
        self.phase = phase;
        self.phase_started_at = Some(Instant::now());

        if matches!(phase, RaftMigrationPhase::Completed | RaftMigrationPhase::Failed | RaftMigrationPhase::Cancelled) {
            self.completed_at = Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            );
        }
    }

    /// Mark as failed with error.
    pub fn fail(&mut self, error: String) {
        self.error = Some(error);
        self.transition_to(RaftMigrationPhase::Failed);
    }

    /// Update progress from Raft status.
    pub fn update_progress(&mut self, learner_match: u64, leader_commit: u64, snapshot_applied: bool) {
        self.progress.learner_match_index = learner_match;
        self.progress.leader_commit_index = leader_commit;
        self.progress.snapshot_applied = snapshot_applied;
        self.progress.lag_entries = leader_commit.saturating_sub(learner_match);
        self.progress.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
    }

    /// Check if this is a transfer operation.
    pub fn is_transfer(&self) -> bool {
        self.change.remove_after.is_some()
    }

    /// Get the duration of the migration.
    pub fn duration(&self) -> Duration {
        let end = self.completed_at.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
        });
        Duration::from_millis(end.saturating_sub(self.started_at))
    }

    /// Check if the migration is stalled (stuck in a phase too long).
    pub fn is_stalled(&self, max_phase_duration: Duration) -> bool {
        self.phase_started_at
            .map(|t| t.elapsed() > max_phase_duration)
            .unwrap_or(false)
    }
}

// ============================================================================
// Raft Shard Controller Interface
// ============================================================================

/// Interface for controlling a shard's Raft group.
///
/// This trait abstracts the Raft operations needed for membership changes.
#[async_trait::async_trait]
pub trait ShardRaftController: Send + Sync + std::fmt::Debug {
    /// Add a learner to the shard's Raft group.
    async fn add_learner(&self, shard_id: ShardId, node_id: NodeId, node_addr: std::net::SocketAddr) -> Result<()>;

    /// Promote a learner to voter.
    async fn promote_to_voter(&self, shard_id: ShardId, node_id: NodeId) -> Result<()>;

    /// Remove a node from the shard's Raft group.
    async fn remove_node(&self, shard_id: ShardId, node_id: NodeId) -> Result<()>;

    /// Transfer leadership to another node.
    async fn transfer_leader(&self, shard_id: ShardId, to_node: NodeId) -> Result<()>;

    /// Get the current leader of a shard.
    async fn get_leader(&self, shard_id: ShardId) -> Result<Option<NodeId>>;

    /// Get the match index for a learner (how caught up they are).
    async fn get_learner_progress(&self, shard_id: ShardId, learner_id: NodeId) -> Result<(u64, u64, bool)>;

    /// Get the current commit index for a shard.
    async fn get_commit_index(&self, shard_id: ShardId) -> Result<u64>;

    /// Check if a node is a voter in the shard.
    async fn is_voter(&self, shard_id: ShardId, node_id: NodeId) -> Result<bool>;

    /// Check if a node is a learner in the shard.
    async fn is_learner(&self, shard_id: ShardId, node_id: NodeId) -> Result<bool>;

    /// Get all voters in a shard.
    async fn get_voters(&self, shard_id: ShardId) -> Result<Vec<NodeId>>;

    /// Get all learners in a shard.
    async fn get_learners(&self, shard_id: ShardId) -> Result<Vec<NodeId>>;

    /// List all shards that have learners.
    ///
    /// Returns a map of shard ID to list of learner node IDs.
    /// Used by the janitor to detect orphan learners.
    fn list_shards_with_learners(&self) -> Result<Vec<(ShardId, Vec<NodeId>)>>;

    /// Remove a learner from a shard's Raft group.
    ///
    /// This is a synchronous operation used by the janitor to clean up
    /// orphan learners that were left behind by failed migrations.
    fn remove_learner(&self, shard_id: ShardId, node_id: NodeId) -> Result<()>;
}

/// No-op implementation for testing.
#[derive(Debug, Default)]
pub struct NoOpShardRaftController;

#[async_trait::async_trait]
impl ShardRaftController for NoOpShardRaftController {
    async fn add_learner(&self, _shard_id: ShardId, _node_id: NodeId, _node_addr: std::net::SocketAddr) -> Result<()> {
        Ok(())
    }

    async fn promote_to_voter(&self, _shard_id: ShardId, _node_id: NodeId) -> Result<()> {
        Ok(())
    }

    async fn remove_node(&self, _shard_id: ShardId, _node_id: NodeId) -> Result<()> {
        Ok(())
    }

    async fn transfer_leader(&self, _shard_id: ShardId, _to_node: NodeId) -> Result<()> {
        Ok(())
    }

    async fn get_leader(&self, _shard_id: ShardId) -> Result<Option<NodeId>> {
        Ok(Some(1))
    }

    async fn get_learner_progress(&self, _shard_id: ShardId, _learner_id: NodeId) -> Result<(u64, u64, bool)> {
        // Returns (learner_match_index, leader_commit_index, snapshot_applied)
        Ok((100, 100, true))
    }

    async fn get_commit_index(&self, _shard_id: ShardId) -> Result<u64> {
        Ok(100)
    }

    async fn is_voter(&self, _shard_id: ShardId, _node_id: NodeId) -> Result<bool> {
        Ok(true)
    }

    async fn is_learner(&self, _shard_id: ShardId, _node_id: NodeId) -> Result<bool> {
        Ok(false)
    }

    async fn get_voters(&self, _shard_id: ShardId) -> Result<Vec<NodeId>> {
        Ok(vec![1, 2, 3])
    }

    async fn get_learners(&self, _shard_id: ShardId) -> Result<Vec<NodeId>> {
        Ok(vec![])
    }

    fn list_shards_with_learners(&self) -> Result<Vec<(ShardId, Vec<NodeId>)>> {
        Ok(vec![])
    }

    fn remove_learner(&self, _shard_id: ShardId, _node_id: NodeId) -> Result<()> {
        Ok(())
    }
}

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for Raft-native migrations.
#[derive(Debug, Clone)]
pub struct RaftMigrationConfig {
    /// Maximum concurrent migrations.
    pub max_concurrent: usize,

    /// Lag threshold (in entries) to consider a learner "caught up".
    pub catch_up_threshold: u64,

    /// How often to poll learner progress.
    pub progress_poll_interval: Duration,

    /// Maximum time to wait for a learner to catch up.
    pub catch_up_timeout: Duration,

    /// Maximum retries for failed operations.
    pub max_retries: u32,

    /// Delay between retries.
    pub retry_delay: Duration,

    /// Whether to require leadership transfer before removing old leader.
    pub transfer_leader_first: bool,

    /// Maximum time a migration can stay in any single phase (for zombie detection).
    pub max_phase_duration: Duration,

    /// How often the janitor runs.
    pub janitor_interval: Duration,
}

impl Default for RaftMigrationConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 4,
            catch_up_threshold: 100,
            progress_poll_interval: Duration::from_millis(500),
            catch_up_timeout: Duration::from_secs(300), // 5 minutes
            max_retries: 3,
            retry_delay: Duration::from_secs(5),
            transfer_leader_first: true,
            max_phase_duration: Duration::from_secs(3600), // 1 hour
            janitor_interval: Duration::from_secs(60),     // 1 minute
        }
    }
}

// ============================================================================
// Raft Migration Coordinator
// ============================================================================

/// Coordinates Raft-native shard migrations.
///
/// This coordinator uses Raft's membership change mechanism instead of
/// manual key streaming, ensuring consistency through Raft's guarantees.
///
/// # Concurrency
///
/// Uses DashMap for lock-free concurrent access to active migrations.
///
/// # Split-Brain Protection
///
/// Each migration includes a coordinator_term that is validated before
/// executing operations, preventing zombie coordinators from interfering.
#[derive(Debug)]
pub struct RaftMigrationCoordinator {
    /// This node's ID.
    node_id: NodeId,

    /// Configuration.
    config: RaftMigrationConfig,

    /// Active migrations (lock-free concurrent map).
    active_migrations: DashMap<ShardId, RaftShardMigration>,

    /// Migration history.
    history: RwLock<Vec<RaftShardMigration>>,

    /// Maximum history to keep.
    max_history: usize,

    /// Shard Raft controller.
    raft_controller: RwLock<Option<Arc<dyn ShardRaftController>>>,

    /// Whether migrations are paused.
    paused: AtomicBool,

    /// Current coordinator term (for fencing).
    coordinator_term: AtomicU64,

    /// Statistics.
    migrations_started: AtomicU64,
    migrations_completed: AtomicU64,
    migrations_failed: AtomicU64,

    /// Cancellation token for graceful shutdown.
    cancellation: CancellationToken,

    /// Cleanup manager for failed migrations.
    cleanup_manager: Arc<MigrationCleanupManager>,
}

impl RaftMigrationCoordinator {
    /// Create a new coordinator.
    pub fn new(node_id: NodeId, config: RaftMigrationConfig) -> Self {
        Self {
            node_id,
            config,
            active_migrations: DashMap::new(),
            history: RwLock::new(Vec::new()),
            max_history: 100,
            raft_controller: RwLock::new(None),
            paused: AtomicBool::new(false),
            coordinator_term: AtomicU64::new(1),
            migrations_started: AtomicU64::new(0),
            migrations_completed: AtomicU64::new(0),
            migrations_failed: AtomicU64::new(0),
            cancellation: CancellationToken::new(),
            cleanup_manager: Arc::new(MigrationCleanupManager::noop()),
        }
    }

    /// Set the cleanup manager.
    pub fn set_cleanup_manager(&mut self, manager: Arc<MigrationCleanupManager>) {
        self.cleanup_manager = manager;
    }

    /// Get the cleanup manager.
    pub fn cleanup_manager(&self) -> &Arc<MigrationCleanupManager> {
        &self.cleanup_manager
    }

    /// Set the Raft controller.
    pub fn set_raft_controller(&self, controller: Arc<dyn ShardRaftController>) {
        *self.raft_controller.write() = Some(controller);
    }

    /// Get configuration.
    pub fn config(&self) -> &RaftMigrationConfig {
        &self.config
    }

    /// Get current coordinator term.
    pub fn coordinator_term(&self) -> u64 {
        self.coordinator_term.load(Ordering::SeqCst)
    }

    /// Increment coordinator term (call on leadership change).
    pub fn increment_term(&self) -> u64 {
        self.coordinator_term.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Check if paused.
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    /// Pause all migrations.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
        tracing::warn!(node_id = self.node_id, "Migrations paused");
    }

    /// Resume migrations.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
        tracing::info!(node_id = self.node_id, "Migrations resumed");
    }

    /// Get cancellation token for graceful shutdown.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Signal shutdown.
    pub fn shutdown(&self) {
        self.cancellation.cancel();
    }

    /// Plan a new migration.
    #[tracing::instrument(skip(self), fields(node_id = %self.node_id))]
    pub fn plan_migration(&self, mut change: RaftMembershipChange) -> Result<RaftShardMigration> {
        if self.is_paused() {
            return Err(Error::MigrationPaused);
        }

        // Check concurrent limit
        if self.active_migrations.len() >= self.config.max_concurrent {
            return Err(Error::TooManyMigrations);
        }

        // Check if shard is already migrating
        if self.active_migrations.contains_key(&change.shard_id) {
            return Err(Error::ShardAlreadyMigrating(change.shard_id));
        }

        // Set coordinator term for fencing
        change.coordinator_term = self.coordinator_term();

        let migration = RaftShardMigration::new(change);
        self.active_migrations.insert(migration.change.shard_id, migration.clone());

        self.migrations_started.fetch_add(1, Ordering::Relaxed);

        tracing::info!(
            migration_id = %migration.id,
            shard_id = migration.change.shard_id,
            change_type = ?migration.change.change_type,
            target_node = migration.change.target_node,
            coordinator_term = migration.change.coordinator_term,
            "Migration planned"
        );

        Ok(migration)
    }

    /// Get a migration by shard ID.
    pub fn get_migration(&self, shard_id: ShardId) -> Option<RaftShardMigration> {
        self.active_migrations.get(&shard_id).map(|r| r.clone())
    }

    /// Check if a shard is being migrated.
    pub fn is_migrating(&self, shard_id: ShardId) -> bool {
        self.active_migrations.contains_key(&shard_id)
    }

    /// Execute a migration through all phases.
    #[tracing::instrument(skip(self), fields(node_id = %self.node_id, shard_id = %shard_id))]
    pub async fn execute_migration(&self, shard_id: ShardId) -> Result<()> {
        let controller = self.raft_controller.read().clone()
            .ok_or_else(|| Error::Internal("No Raft controller set".to_string()))?;

        loop {
            // Check for cancellation
            if self.cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }

            if self.is_paused() {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            let migration = self.get_migration(shard_id)
                .ok_or(Error::MigrationNotFound(shard_id))?;

            // Validate coordinator term (fencing)
            if migration.change.coordinator_term != self.coordinator_term() {
                tracing::warn!(
                    migration_id = %migration.id,
                    shard_id,
                    migration_term = migration.change.coordinator_term,
                    current_term = self.coordinator_term(),
                    "Migration term mismatch - rejecting stale migration"
                );
                self.fail_migration(shard_id, "Coordinator term mismatch".to_string())?;
                return Err(Error::MigrationFailed("Stale coordinator term".to_string()));
            }

            match migration.phase {
                RaftMigrationPhase::Planned => {
                    self.execute_add_learner(&controller, shard_id).await?;
                }
                RaftMigrationPhase::AddingLearner => {
                    // Wait for add_learner to complete, then move to waiting for snapshot
                    self.transition_phase(shard_id, RaftMigrationPhase::WaitingForSnapshot)?;
                }
                RaftMigrationPhase::WaitingForSnapshot => {
                    self.wait_for_snapshot(&controller, shard_id).await?;
                }
                RaftMigrationPhase::CatchingUp => {
                    self.wait_for_catchup(&controller, shard_id).await?;
                }
                RaftMigrationPhase::PromotingToVoter => {
                    self.execute_promote_to_voter(&controller, shard_id).await?;
                }
                RaftMigrationPhase::RemovingOldNode => {
                    self.execute_remove_old_node(&controller, shard_id).await?;
                }
                RaftMigrationPhase::Completed => {
                    self.complete_migration(shard_id)?;
                    return Ok(());
                }
                RaftMigrationPhase::Failed | RaftMigrationPhase::Cancelled => {
                    return Err(Error::MigrationFailed(
                        migration.error.unwrap_or_else(|| "Unknown error".to_string())
                    ));
                }
            }

            // Small delay between phase checks
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Execute the add learner phase.
    #[tracing::instrument(skip(self, controller), fields(shard_id = %shard_id))]
    async fn execute_add_learner(&self, controller: &Arc<dyn ShardRaftController>, shard_id: ShardId) -> Result<()> {
        let migration = self.get_migration(shard_id)
            .ok_or(Error::MigrationNotFound(shard_id))?;

        self.transition_phase(shard_id, RaftMigrationPhase::AddingLearner)?;

        // TODO: Get actual node address from cluster membership
        let node_addr = format!("127.0.0.1:{}", 9000 + migration.change.target_node)
            .parse()
            .map_err(|e| Error::Internal(format!("Invalid address: {}", e)))?;

        match controller.add_learner(shard_id, migration.change.target_node, node_addr).await {
            Ok(()) => {
                tracing::info!(
                    migration_id = %migration.id,
                    target_node = migration.change.target_node,
                    "Learner added successfully"
                );
                Ok(())
            }
            Err(e) => {
                self.fail_migration(shard_id, format!("Failed to add learner: {}", e))?;
                Err(e)
            }
        }
    }

    /// Wait for snapshot to be applied.
    #[tracing::instrument(skip(self, controller), fields(shard_id = %shard_id))]
    async fn wait_for_snapshot(&self, controller: &Arc<dyn ShardRaftController>, shard_id: ShardId) -> Result<()> {
        let start = Instant::now();

        loop {
            if self.cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }

            if start.elapsed() > self.config.catch_up_timeout {
                self.fail_migration(shard_id, "Timeout waiting for snapshot".to_string())?;
                return Err(Error::MigrationTimeout);
            }

            let migration = self.get_migration(shard_id)
                .ok_or(Error::MigrationNotFound(shard_id))?;

            let (learner_match, leader_commit, snapshot_applied) = controller
                .get_learner_progress(shard_id, migration.change.target_node)
                .await?;

            // Update progress
            if let Some(mut entry) = self.active_migrations.get_mut(&shard_id) {
                entry.update_progress(learner_match, leader_commit, snapshot_applied);
            }

            if snapshot_applied {
                tracing::info!(
                    migration_id = %migration.id,
                    learner_match,
                    "Snapshot applied, moving to catching up phase"
                );
                self.transition_phase(shard_id, RaftMigrationPhase::CatchingUp)?;
                return Ok(());
            }

            tokio::time::sleep(self.config.progress_poll_interval).await;
        }
    }

    /// Wait for learner to catch up with the log.
    #[tracing::instrument(skip(self, controller), fields(shard_id = %shard_id))]
    async fn wait_for_catchup(&self, controller: &Arc<dyn ShardRaftController>, shard_id: ShardId) -> Result<()> {
        let start = Instant::now();

        loop {
            if self.cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }

            if start.elapsed() > self.config.catch_up_timeout {
                self.fail_migration(shard_id, "Timeout waiting for catch up".to_string())?;
                return Err(Error::MigrationTimeout);
            }

            let migration = self.get_migration(shard_id)
                .ok_or(Error::MigrationNotFound(shard_id))?;

            let (learner_match, leader_commit, snapshot_applied) = controller
                .get_learner_progress(shard_id, migration.change.target_node)
                .await?;

            // Update progress
            if let Some(mut entry) = self.active_migrations.get_mut(&shard_id) {
                entry.update_progress(learner_match, leader_commit, snapshot_applied);
            }

            let lag = leader_commit.saturating_sub(learner_match);
            if lag <= self.config.catch_up_threshold {
                tracing::info!(
                    migration_id = %migration.id,
                    lag,
                    threshold = self.config.catch_up_threshold,
                    "Learner caught up, ready to promote"
                );
                self.transition_phase(shard_id, RaftMigrationPhase::PromotingToVoter)?;
                return Ok(());
            }

            tracing::debug!(
                migration_id = %migration.id,
                learner_match,
                leader_commit,
                lag,
                "Waiting for learner to catch up"
            );

            tokio::time::sleep(self.config.progress_poll_interval).await;
        }
    }

    /// Execute the promote to voter phase.
    #[tracing::instrument(skip(self, controller), fields(shard_id = %shard_id))]
    async fn execute_promote_to_voter(&self, controller: &Arc<dyn ShardRaftController>, shard_id: ShardId) -> Result<()> {
        let migration = self.get_migration(shard_id)
            .ok_or(Error::MigrationNotFound(shard_id))?;

        match controller.promote_to_voter(shard_id, migration.change.target_node).await {
            Ok(()) => {
                tracing::info!(
                    migration_id = %migration.id,
                    target_node = migration.change.target_node,
                    "Promoted to voter successfully"
                );

                // If this is a transfer, move to removing old node
                if migration.is_transfer() {
                    self.transition_phase(shard_id, RaftMigrationPhase::RemovingOldNode)?;
                } else {
                    self.transition_phase(shard_id, RaftMigrationPhase::Completed)?;
                }
                Ok(())
            }
            Err(e) => {
                self.fail_migration(shard_id, format!("Failed to promote to voter: {}", e))?;
                Err(e)
            }
        }
    }

    /// Execute the remove old node phase.
    #[tracing::instrument(skip(self, controller), fields(shard_id = %shard_id))]
    async fn execute_remove_old_node(&self, controller: &Arc<dyn ShardRaftController>, shard_id: ShardId) -> Result<()> {
        let migration = self.get_migration(shard_id)
            .ok_or(Error::MigrationNotFound(shard_id))?;

        let old_node = migration.change.remove_after
            .ok_or_else(|| Error::Internal("No old node to remove".to_string()))?;

        // If old node is the leader, transfer leadership first
        if self.config.transfer_leader_first {
            if let Ok(Some(leader)) = controller.get_leader(shard_id).await {
                if leader == old_node {
                    tracing::info!(
                        migration_id = %migration.id,
                        old_leader = old_node,
                        new_leader = migration.change.target_node,
                        "Transferring leadership before removal"
                    );
                    controller.transfer_leader(shard_id, migration.change.target_node).await?;
                    // Wait a bit for leadership transfer
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }

        match controller.remove_node(shard_id, old_node).await {
            Ok(()) => {
                tracing::info!(
                    migration_id = %migration.id,
                    removed_node = old_node,
                    "Old node removed successfully"
                );
                self.transition_phase(shard_id, RaftMigrationPhase::Completed)?;
                Ok(())
            }
            Err(e) => {
                self.fail_migration(shard_id, format!("Failed to remove old node: {}", e))?;
                Err(e)
            }
        }
    }

    /// Transition a migration to a new phase.
    fn transition_phase(&self, shard_id: ShardId, phase: RaftMigrationPhase) -> Result<()> {
        if let Some(mut entry) = self.active_migrations.get_mut(&shard_id) {
            entry.transition_to(phase);
            Ok(())
        } else {
            Err(Error::MigrationNotFound(shard_id))
        }
    }

    /// Mark a migration as failed and trigger cleanup.
    ///
    /// This method:
    /// 1. Marks the migration as failed
    /// 2. Schedules cleanup of partial resources (target data, learners, checkpoints)
    /// 3. Updates statistics
    fn fail_migration(&self, shard_id: ShardId, error: String) -> Result<()> {
        if let Some(mut entry) = self.active_migrations.get_mut(&shard_id) {
            // Get migration info before marking as failed
            let migration_id = entry.id;
            let target_node = entry.change.target_node;

            // Mark as failed
            entry.fail(error.clone());
            self.migrations_failed.fetch_add(1, Ordering::Relaxed);

            // Schedule cleanup in the background
            // This is non-blocking to avoid slowing down the main migration flow
            self.cleanup_manager.schedule_cleanup(
                migration_id,
                shard_id,
                target_node,
            );

            tracing::warn!(
                migration_id = %migration_id,
                shard_id,
                target_node,
                error,
                "Migration failed, cleanup scheduled"
            );

            Ok(())
        } else {
            Err(Error::MigrationNotFound(shard_id))
        }
    }

    /// Complete a migration.
    fn complete_migration(&self, shard_id: ShardId) -> Result<()> {
        let migration = self.active_migrations.remove(&shard_id)
            .map(|(_, m)| m)
            .ok_or(Error::MigrationNotFound(shard_id))?;

        // Add to history
        let mut history = self.history.write();
        history.push(migration.clone());
        if history.len() > self.max_history {
            history.remove(0);
        }

        self.migrations_completed.fetch_add(1, Ordering::Relaxed);

        tracing::info!(
            migration_id = %migration.id,
            shard_id,
            duration = ?migration.duration(),
            "Migration completed"
        );

        Ok(())
    }

    /// Cancel a migration.
    pub fn cancel_migration(&self, shard_id: ShardId) -> Result<RaftShardMigration> {
        let (_, mut migration) = self.active_migrations.remove(&shard_id)
            .ok_or(Error::MigrationNotFound(shard_id))?;

        migration.transition_to(RaftMigrationPhase::Cancelled);

        // Add to history
        let mut history = self.history.write();
        history.push(migration.clone());
        if history.len() > self.max_history {
            history.remove(0);
        }

        tracing::warn!(
            migration_id = %migration.id,
            shard_id,
            "Migration cancelled"
        );

        Ok(migration)
    }

    /// Get statistics.
    pub fn stats(&self) -> RaftMigrationStats {
        RaftMigrationStats {
            active_count: self.active_migrations.len(),
            started_total: self.migrations_started.load(Ordering::Relaxed),
            completed_total: self.migrations_completed.load(Ordering::Relaxed),
            failed_total: self.migrations_failed.load(Ordering::Relaxed),
            paused: self.is_paused(),
        }
    }

    /// Get all active migrations.
    pub fn active_migrations(&self) -> Vec<RaftShardMigration> {
        self.active_migrations.iter().map(|r| r.clone()).collect()
    }

    /// Get migration history.
    pub fn history(&self) -> Vec<RaftShardMigration> {
        self.history.read().clone()
    }

    // ==================== Janitor (Zombie Cleanup) ====================

    /// Run the janitor to clean up stalled/zombie migrations.
    ///
    /// This should be called periodically (e.g., via a background task).
    ///
    /// The janitor performs several cleanup tasks:
    /// 1. Detects and fails stalled migrations
    /// 2. Trims migration history
    /// 3. Detects orphan learners (learners without active migrations)
    /// 4. Tracks cleanup progress for failed migrations
    #[tracing::instrument(skip(self), fields(node_id = %self.node_id))]
    pub fn run_janitor(&self) -> JanitorReport {
        let mut report = JanitorReport::default();

        // 1. Find and clean up stalled migrations
        let stalled_shards: Vec<ShardId> = self.active_migrations
            .iter()
            .filter(|entry| entry.is_stalled(self.config.max_phase_duration))
            .map(|entry| *entry.key())
            .collect();

        for shard_id in stalled_shards {
            if let Some(migration) = self.get_migration(shard_id) {
                tracing::warn!(
                    migration_id = %migration.id,
                    shard_id,
                    phase = %migration.phase,
                    duration = ?migration.duration(),
                    "Detected stalled migration"
                );

                // Mark as failed (this also triggers cleanup)
                if self.fail_migration(shard_id, "Stalled migration detected by janitor".to_string()).is_ok() {
                    report.stalled_cleaned += 1;
                    report.cleanups_triggered += 1;
                }
            }
        }

        // 2. Trim history if needed
        {
            let mut history = self.history.write();
            while history.len() > self.max_history {
                history.remove(0);
                report.history_trimmed += 1;
            }
        }

        // 3. Detect orphan learners
        // An orphan learner is a node that was added as a learner for a migration
        // but the migration is no longer active (could have failed/crashed mid-way)
        if let Some(controller) = self.raft_controller.read().as_ref() {
            // Get all shards that have learners
            if let Ok(shards_with_learners) = controller.list_shards_with_learners() {
                for (shard_id, learner_nodes) in shards_with_learners {
                    // Check if there's an active migration for this shard
                    let has_active_migration = self.active_migrations.contains_key(&shard_id);

                    if !has_active_migration && !learner_nodes.is_empty() {
                        // This is an orphan learner situation
                        for learner_node in learner_nodes {
                            report.orphan_learners_detected += 1;
                            tracing::warn!(
                                shard_id,
                                learner_node,
                                "Detected orphan learner (no active migration)"
                            );

                            // Try to remove the orphan learner
                            match controller.remove_learner(shard_id, learner_node) {
                                Ok(()) => {
                                    report.orphan_learners_removed += 1;
                                    tracing::info!(
                                        shard_id,
                                        learner_node,
                                        "Removed orphan learner"
                                    );
                                }
                                Err(e) => {
                                    report.add_error(format!(
                                        "Failed to remove orphan learner {} from shard {}: {}",
                                        learner_node, shard_id, e
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        // 4. Check for pending cleanups that might be stuck
        let pending_cleanup_count = self.cleanup_manager.pending_count();
        if pending_cleanup_count > 10 {
            tracing::warn!(
                pending_count = pending_cleanup_count,
                "Large number of pending cleanups - some may be stuck"
            );
        }

        // Log summary if any work was done
        if report.had_work() {
            tracing::info!(
                stalled_cleaned = report.stalled_cleaned,
                history_trimmed = report.history_trimmed,
                orphan_learners_removed = report.orphan_learners_removed,
                cleanups_triggered = report.cleanups_triggered,
                "Janitor run completed"
            );
        }

        if report.had_errors() {
            tracing::warn!(
                error_count = report.errors.len(),
                "Janitor run had errors"
            );
        }

        report
    }

    /// Start the janitor background task.
    pub fn start_janitor(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let coordinator = Arc::clone(self);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(coordinator.config.janitor_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        coordinator.run_janitor();
                    }
                    _ = coordinator.cancellation.cancelled() => {
                        tracing::info!("Janitor task shutting down");
                        break;
                    }
                }
            }
        })
    }
}

/// Report from a janitor run.
#[derive(Debug, Default)]
pub struct JanitorReport {
    /// Number of stalled migrations cleaned up.
    pub stalled_cleaned: usize,
    /// Number of history entries trimmed.
    pub history_trimmed: usize,
    /// Number of orphan learners detected.
    pub orphan_learners_detected: usize,
    /// Number of orphan learners removed.
    pub orphan_learners_removed: usize,
    /// Number of checkpoint files cleaned.
    pub checkpoints_cleaned: usize,
    /// Number of pending cleanups triggered.
    pub cleanups_triggered: usize,
    /// Errors encountered during janitor run.
    pub errors: Vec<String>,
}

impl JanitorReport {
    /// Check if any cleanup work was done.
    pub fn had_work(&self) -> bool {
        self.stalled_cleaned > 0
            || self.history_trimmed > 0
            || self.orphan_learners_removed > 0
            || self.checkpoints_cleaned > 0
            || self.cleanups_triggered > 0
    }

    /// Check if there were any errors.
    pub fn had_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Add an error to the report.
    pub fn add_error(&mut self, error: impl Into<String>) {
        self.errors.push(error.into());
    }
}

/// Statistics for Raft migrations.
#[derive(Debug, Clone)]
pub struct RaftMigrationStats {
    /// Number of active migrations.
    pub active_count: usize,
    /// Total migrations started.
    pub started_total: u64,
    /// Total migrations completed.
    pub completed_total: u64,
    /// Total migrations failed.
    pub failed_total: u64,
    /// Whether migrations are paused.
    pub paused: bool,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_membership_change_creation() {
        let change = RaftMembershipChange::new(1, RaftChangeType::AddLearner, 5);
        assert_eq!(change.shard_id, 1);
        assert_eq!(change.target_node, 5);
        assert_eq!(change.change_type, RaftChangeType::AddLearner);
        assert!(change.remove_after.is_none());
    }

    #[test]
    fn test_transfer_change() {
        let change = RaftMembershipChange::transfer(1, 2, 5);
        assert_eq!(change.shard_id, 1);
        assert_eq!(change.target_node, 5);
        assert_eq!(change.change_type, RaftChangeType::AddLearner);
        assert_eq!(change.remove_after, Some(2));
    }

    #[test]
    fn test_coordinator_term_fencing() {
        let change = RaftMembershipChange::new(1, RaftChangeType::AddLearner, 5)
            .with_coordinator_term(5);
        assert_eq!(change.coordinator_term, 5);
    }

    #[test]
    fn test_migration_phases() {
        let change = RaftMembershipChange::new(1, RaftChangeType::AddLearner, 5);
        let mut migration = RaftShardMigration::new(change);

        assert_eq!(migration.phase, RaftMigrationPhase::Planned);

        migration.transition_to(RaftMigrationPhase::AddingLearner);
        assert_eq!(migration.phase, RaftMigrationPhase::AddingLearner);

        migration.transition_to(RaftMigrationPhase::WaitingForSnapshot);
        assert_eq!(migration.phase, RaftMigrationPhase::WaitingForSnapshot);

        migration.transition_to(RaftMigrationPhase::CatchingUp);
        assert_eq!(migration.phase, RaftMigrationPhase::CatchingUp);

        migration.transition_to(RaftMigrationPhase::Completed);
        assert_eq!(migration.phase, RaftMigrationPhase::Completed);
        assert!(migration.completed_at.is_some());
    }

    #[test]
    fn test_progress_catch_up() {
        let mut progress = RaftMigrationProgress::default();
        progress.learner_match_index = 90;
        progress.leader_commit_index = 100;
        progress.snapshot_applied = true;
        progress.lag_entries = 10;

        assert!(progress.is_caught_up(10));
        assert!(progress.is_caught_up(50));
        assert!(!progress.is_caught_up(5));
    }

    #[test]
    fn test_coordinator_plan_migration() {
        let coordinator = RaftMigrationCoordinator::new(1, RaftMigrationConfig::default());

        let change = RaftMembershipChange::new(1, RaftChangeType::AddLearner, 5);
        let migration = coordinator.plan_migration(change).unwrap();

        assert_eq!(migration.phase, RaftMigrationPhase::Planned);
        assert!(coordinator.is_migrating(1));
        assert_eq!(migration.change.coordinator_term, coordinator.coordinator_term());
    }

    #[test]
    fn test_coordinator_concurrent_limit() {
        let mut config = RaftMigrationConfig::default();
        config.max_concurrent = 2;
        let coordinator = RaftMigrationCoordinator::new(1, config);

        coordinator.plan_migration(RaftMembershipChange::new(1, RaftChangeType::AddLearner, 5)).unwrap();
        coordinator.plan_migration(RaftMembershipChange::new(2, RaftChangeType::AddLearner, 5)).unwrap();

        let result = coordinator.plan_migration(RaftMembershipChange::new(3, RaftChangeType::AddLearner, 5));
        assert!(matches!(result, Err(Error::TooManyMigrations)));
    }

    #[test]
    fn test_coordinator_duplicate_shard() {
        let coordinator = RaftMigrationCoordinator::new(1, RaftMigrationConfig::default());

        coordinator.plan_migration(RaftMembershipChange::new(1, RaftChangeType::AddLearner, 5)).unwrap();

        let result = coordinator.plan_migration(RaftMembershipChange::new(1, RaftChangeType::AddLearner, 6));
        assert!(matches!(result, Err(Error::ShardAlreadyMigrating(1))));
    }

    #[test]
    fn test_coordinator_pause_resume() {
        let coordinator = RaftMigrationCoordinator::new(1, RaftMigrationConfig::default());

        coordinator.pause();
        assert!(coordinator.is_paused());

        let result = coordinator.plan_migration(RaftMembershipChange::new(1, RaftChangeType::AddLearner, 5));
        assert!(matches!(result, Err(Error::MigrationPaused)));

        coordinator.resume();
        assert!(!coordinator.is_paused());

        let result = coordinator.plan_migration(RaftMembershipChange::new(1, RaftChangeType::AddLearner, 5));
        assert!(result.is_ok());
    }

    #[test]
    fn test_coordinator_term_increment() {
        let coordinator = RaftMigrationCoordinator::new(1, RaftMigrationConfig::default());
        assert_eq!(coordinator.coordinator_term(), 1);

        let new_term = coordinator.increment_term();
        assert_eq!(new_term, 2);
        assert_eq!(coordinator.coordinator_term(), 2);
    }

    #[test]
    fn test_janitor_report() {
        let coordinator = RaftMigrationCoordinator::new(1, RaftMigrationConfig::default());
        let report = coordinator.run_janitor();
        assert_eq!(report.stalled_cleaned, 0);
        assert_eq!(report.history_trimmed, 0);
    }

    #[tokio::test]
    async fn test_coordinator_execute_migration() {
        let coordinator = RaftMigrationCoordinator::new(1, RaftMigrationConfig::default());
        coordinator.set_raft_controller(Arc::new(NoOpShardRaftController));

        let change = RaftMembershipChange::new(1, RaftChangeType::AddLearner, 5);
        coordinator.plan_migration(change).unwrap();

        let result = coordinator.execute_migration(1).await;
        assert!(result.is_ok());

        assert!(!coordinator.is_migrating(1));
        let stats = coordinator.stats();
        assert_eq!(stats.completed_total, 1);
    }

    #[tokio::test]
    async fn test_coordinator_execute_transfer() {
        let coordinator = RaftMigrationCoordinator::new(1, RaftMigrationConfig::default());
        coordinator.set_raft_controller(Arc::new(NoOpShardRaftController));

        let change = RaftMembershipChange::transfer(1, 2, 5);
        coordinator.plan_migration(change).unwrap();

        let result = coordinator.execute_migration(1).await;
        assert!(result.is_ok());

        let stats = coordinator.stats();
        assert_eq!(stats.completed_total, 1);
    }
}
