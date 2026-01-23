//! Shard migration coordinator with two-phase handover.
//!
//! This module implements safe shard migration with:
//! - Two-phase handover (data first, then ownership)
//! - Crash recovery via persistent checkpoints
//! - Kill switch for emergency pause
//! - Rate limiting to prevent network saturation

use crate::error::{Error, Result};
use crate::types::NodeId;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use super::shard::ShardId;
use super::shard_placement::ShardMovement;

/// Phase of a shard migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationPhase {
    /// Migration planned but not started.
    Planned,
    /// Data is being streamed from source to target.
    Streaming,
    /// Catching up writes that occurred during streaming.
    CatchingUp,
    /// Ownership handover in progress (atomic!).
    Committing,
    /// Cleaning up after successful migration.
    Cleanup,
    /// Migration completed successfully.
    Complete,
    /// Migration failed.
    Failed,
    /// Migration was cancelled.
    Cancelled,
}

impl std::fmt::Display for MigrationPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationPhase::Planned => write!(f, "planned"),
            MigrationPhase::Streaming => write!(f, "streaming"),
            MigrationPhase::CatchingUp => write!(f, "catching_up"),
            MigrationPhase::Committing => write!(f, "committing"),
            MigrationPhase::Cleanup => write!(f, "cleanup"),
            MigrationPhase::Complete => write!(f, "complete"),
            MigrationPhase::Failed => write!(f, "failed"),
            MigrationPhase::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl MigrationPhase {
    /// Check if this is a terminal phase.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            MigrationPhase::Complete | MigrationPhase::Failed | MigrationPhase::Cancelled
        )
    }

    /// Check if this phase can be resumed after crash.
    pub fn is_resumable(&self) -> bool {
        matches!(
            self,
            MigrationPhase::Planned
                | MigrationPhase::Streaming
                | MigrationPhase::CatchingUp
        )
    }
}

/// Progress tracking for a migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationProgress {
    /// Total entries to transfer (estimated).
    pub total_entries: u64,
    /// Entries transferred so far.
    pub transferred_entries: u64,
    /// Total bytes to transfer (estimated).
    pub total_bytes: u64,
    /// Bytes transferred so far.
    pub transferred_bytes: u64,
    /// Current transfer rate (bytes/sec).
    pub transfer_rate: f64,
    /// Estimated time remaining.
    pub eta_seconds: Option<u64>,
}

impl MigrationProgress {
    /// Create new progress tracking.
    pub fn new(total_entries: u64, total_bytes: u64) -> Self {
        Self {
            total_entries,
            transferred_entries: 0,
            total_bytes,
            transferred_bytes: 0,
            transfer_rate: 0.0,
            eta_seconds: None,
        }
    }

    /// Get progress percentage.
    pub fn percentage(&self) -> f64 {
        if self.total_entries == 0 {
            return 100.0;
        }
        (self.transferred_entries as f64 / self.total_entries as f64) * 100.0
    }

    /// Update progress.
    pub fn update(&mut self, entries: u64, bytes: u64, rate: f64) {
        self.transferred_entries += entries;
        self.transferred_bytes += bytes;
        self.transfer_rate = rate;

        // Calculate ETA
        if rate > 0.0 {
            let remaining_bytes = self.total_bytes.saturating_sub(self.transferred_bytes);
            self.eta_seconds = Some((remaining_bytes as f64 / rate) as u64);
        }
    }

    /// Check if transfer is complete.
    pub fn is_complete(&self) -> bool {
        self.transferred_entries >= self.total_entries
    }
}

/// Checkpoint for migration resumability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationCheckpoint {
    /// Migration ID.
    pub migration_id: Uuid,
    /// Shard being migrated.
    pub shard_id: ShardId,
    /// Current phase.
    pub phase: MigrationPhase,
    /// Last transferred key (for resumption).
    pub last_key: Option<Vec<u8>>,
    /// Progress at checkpoint.
    pub progress: MigrationProgress,
    /// Timestamp (unix millis).
    pub timestamp_ms: u64,
    /// Sequence number for ordering.
    pub sequence: u64,
}

impl MigrationCheckpoint {
    /// Create a new checkpoint.
    pub fn new(migration: &ShardMigration) -> Self {
        Self {
            migration_id: migration.id,
            shard_id: migration.shard_id,
            phase: migration.phase,
            last_key: migration.last_key.clone(),
            progress: migration.progress.clone(),
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            sequence: migration.checkpoint_sequence,
        }
    }
}

/// A shard migration operation.
#[derive(Debug, Clone)]
pub struct ShardMigration {
    /// Unique migration ID.
    pub id: Uuid,
    /// Shard being migrated.
    pub shard_id: ShardId,
    /// Source node.
    pub from_node: NodeId,
    /// Target node.
    pub to_node: NodeId,
    /// Current phase.
    pub phase: MigrationPhase,
    /// Progress tracking.
    pub progress: MigrationProgress,
    /// When migration started.
    pub started_at: Instant,
    /// When migration completed (if done).
    pub completed_at: Option<Instant>,
    /// Last transferred key (for resumption).
    pub last_key: Option<Vec<u8>>,
    /// Error message if failed.
    pub error: Option<String>,
    /// Checkpoint sequence number.
    pub checkpoint_sequence: u64,
}

impl ShardMigration {
    /// Create a new migration from a movement.
    pub fn from_movement(movement: &ShardMovement) -> Self {
        Self {
            id: Uuid::new_v4(),
            shard_id: movement.shard_id,
            from_node: movement.from_node,
            to_node: movement.to_node,
            phase: MigrationPhase::Planned,
            progress: MigrationProgress::new(0, 0),
            started_at: Instant::now(),
            completed_at: None,
            last_key: None,
            error: None,
            checkpoint_sequence: 0,
        }
    }

    /// Get migration duration.
    pub fn duration(&self) -> Duration {
        self.completed_at
            .map(|t| t.duration_since(self.started_at))
            .unwrap_or_else(|| self.started_at.elapsed())
    }

    /// Check if migration is complete.
    pub fn is_complete(&self) -> bool {
        self.phase.is_terminal()
    }

    /// Set the phase.
    pub fn set_phase(&mut self, phase: MigrationPhase) {
        self.phase = phase;
        if phase.is_terminal() {
            self.completed_at = Some(Instant::now());
        }
    }

    /// Mark as failed with error.
    pub fn fail(&mut self, error: String) {
        self.error = Some(error);
        self.set_phase(MigrationPhase::Failed);
    }

    /// Create a checkpoint.
    pub fn checkpoint(&mut self) -> MigrationCheckpoint {
        self.checkpoint_sequence += 1;
        MigrationCheckpoint::new(self)
    }
}

/// Configuration for migration coordinator.
#[derive(Debug, Clone)]
pub struct MigrationConfig {
    /// Maximum concurrent migrations.
    pub max_concurrent: usize,
    /// Maximum bytes per second per migration.
    pub max_bytes_per_sec: u64,
    /// Batch size for streaming.
    pub batch_size: usize,
    /// Checkpoint interval (entries).
    pub checkpoint_interval: u64,
    /// Timeout for each batch.
    pub batch_timeout: Duration,
    /// Delay between batches (for rate limiting).
    pub batch_delay: Duration,
    /// Whether to allow reads during migration.
    pub allow_reads_during_migration: bool,
    /// Whether to allow writes during migration.
    pub allow_writes_during_migration: bool,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 4,
            max_bytes_per_sec: 50 * 1024 * 1024, // 50 MB/s
            batch_size: 1000,
            checkpoint_interval: 10_000,
            batch_timeout: Duration::from_secs(30),
            batch_delay: Duration::from_millis(10),
            allow_reads_during_migration: true,
            allow_writes_during_migration: false,
        }
    }
}

/// Token bucket rate limiter for controlling bandwidth usage.
///
/// Implements a token bucket algorithm where tokens represent bytes.
/// Tokens are added at a configured rate and consumed when data is transferred.
#[derive(Debug)]
pub struct TokenBucketRateLimiter {
    /// Maximum tokens (burst capacity).
    capacity: u64,
    /// Tokens added per second.
    refill_rate: u64,
    /// Current token count.
    tokens: std::sync::atomic::AtomicU64,
    /// Last refill time.
    last_refill: parking_lot::Mutex<Instant>,
}

impl TokenBucketRateLimiter {
    /// Create a new rate limiter.
    ///
    /// # Arguments
    /// * `bytes_per_sec` - Maximum bytes per second (refill rate)
    /// * `burst_bytes` - Maximum burst size (bucket capacity). If None, defaults to 1 second of bandwidth.
    pub fn new(bytes_per_sec: u64, burst_bytes: Option<u64>) -> Self {
        let capacity = burst_bytes.unwrap_or(bytes_per_sec);
        Self {
            capacity,
            refill_rate: bytes_per_sec,
            tokens: std::sync::atomic::AtomicU64::new(capacity),
            last_refill: parking_lot::Mutex::new(Instant::now()),
        }
    }

    /// Create a rate limiter from migration config.
    pub fn from_config(config: &MigrationConfig) -> Self {
        // Allow burst of up to 2x the per-second rate
        Self::new(config.max_bytes_per_sec, Some(config.max_bytes_per_sec * 2))
    }

    /// Try to acquire tokens without waiting.
    ///
    /// Returns `true` if tokens were acquired, `false` if not enough tokens available.
    pub fn try_acquire(&self, bytes: u64) -> bool {
        self.refill();

        let current = self.tokens.load(Ordering::Acquire);
        if current >= bytes {
            // Try to atomically subtract tokens
            match self.tokens.compare_exchange(
                current,
                current - bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => true,
                Err(_) => {
                    // Concurrent modification, retry once
                    let new_current = self.tokens.load(Ordering::Acquire);
                    if new_current >= bytes {
                        self.tokens.fetch_sub(bytes, Ordering::AcqRel);
                        true
                    } else {
                        false
                    }
                }
            }
        } else {
            false
        }
    }

    /// Acquire tokens, waiting if necessary.
    ///
    /// Returns the duration waited, if any.
    pub async fn acquire(&self, bytes: u64) -> Duration {
        let start = Instant::now();

        // Fast path: try to acquire immediately
        if self.try_acquire(bytes) {
            return Duration::ZERO;
        }

        // Slow path: wait for tokens
        loop {
            self.refill();

            let current = self.tokens.load(Ordering::Acquire);
            if current >= bytes {
                if self.try_acquire(bytes) {
                    break;
                }
            }

            // Calculate wait time
            let needed = bytes.saturating_sub(current);
            let wait_secs = needed as f64 / self.refill_rate as f64;
            let wait_duration = Duration::from_secs_f64(wait_secs.max(0.001)); // Min 1ms

            tokio::time::sleep(wait_duration).await;
        }

        start.elapsed()
    }

    /// Acquire tokens with a timeout.
    ///
    /// Returns `Ok(wait_duration)` if tokens were acquired, `Err(())` if timeout.
    pub async fn acquire_timeout(&self, bytes: u64, timeout: Duration) -> std::result::Result<Duration, ()> {
        let start = Instant::now();
        let deadline = start + timeout;

        // Fast path
        if self.try_acquire(bytes) {
            return Ok(Duration::ZERO);
        }

        // Slow path with timeout
        loop {
            if Instant::now() >= deadline {
                return Err(());
            }

            self.refill();

            let current = self.tokens.load(Ordering::Acquire);
            if current >= bytes {
                if self.try_acquire(bytes) {
                    return Ok(start.elapsed());
                }
            }

            // Calculate wait time, capped by remaining timeout
            let needed = bytes.saturating_sub(current);
            let wait_secs = needed as f64 / self.refill_rate as f64;
            let wait_duration = Duration::from_secs_f64(wait_secs.max(0.001));
            let remaining = deadline.saturating_duration_since(Instant::now());
            let actual_wait = wait_duration.min(remaining);

            if actual_wait.is_zero() {
                return Err(());
            }

            tokio::time::sleep(actual_wait).await;
        }
    }

    /// Refill tokens based on elapsed time.
    fn refill(&self) {
        let mut last = self.last_refill.lock();
        let now = Instant::now();
        let elapsed = now.duration_since(*last);

        if elapsed.as_millis() > 0 {
            let new_tokens = (elapsed.as_secs_f64() * self.refill_rate as f64) as u64;
            if new_tokens > 0 {
                let current = self.tokens.load(Ordering::Acquire);
                let updated = (current + new_tokens).min(self.capacity);
                self.tokens.store(updated, Ordering::Release);
                *last = now;
            }
        }
    }

    /// Get current token count.
    pub fn available(&self) -> u64 {
        self.refill();
        self.tokens.load(Ordering::Acquire)
    }

    /// Get the configured rate in bytes per second.
    pub fn rate(&self) -> u64 {
        self.refill_rate
    }

    /// Get the bucket capacity.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Reset the bucket to full capacity.
    pub fn reset(&self) {
        self.tokens.store(self.capacity, Ordering::Release);
        *self.last_refill.lock() = Instant::now();
    }
}

/// Shared rate limiter for coordinating bandwidth across multiple migrations.
#[derive(Debug)]
pub struct SharedRateLimiter {
    /// Global rate limiter.
    global: TokenBucketRateLimiter,
    /// Per-migration rate limiters.
    per_migration: parking_lot::RwLock<HashMap<Uuid, Arc<TokenBucketRateLimiter>>>,
    /// Per-migration rate limit (derived from global / max_concurrent).
    per_migration_rate: u64,
}

impl SharedRateLimiter {
    /// Create a new shared rate limiter.
    pub fn new(config: &MigrationConfig) -> Self {
        let per_migration_rate = config.max_bytes_per_sec / config.max_concurrent.max(1) as u64;
        Self {
            global: TokenBucketRateLimiter::new(config.max_bytes_per_sec, None),
            per_migration: parking_lot::RwLock::new(HashMap::new()),
            per_migration_rate,
        }
    }

    /// Get or create a rate limiter for a specific migration.
    pub fn for_migration(&self, migration_id: Uuid) -> Arc<TokenBucketRateLimiter> {
        // Check if exists
        if let Some(limiter) = self.per_migration.read().get(&migration_id) {
            return Arc::clone(limiter);
        }

        // Create new
        let mut writers = self.per_migration.write();
        writers
            .entry(migration_id)
            .or_insert_with(|| Arc::new(TokenBucketRateLimiter::new(self.per_migration_rate, None)))
            .clone()
    }

    /// Remove a migration's rate limiter.
    pub fn remove_migration(&self, migration_id: Uuid) {
        self.per_migration.write().remove(&migration_id);
    }

    /// Acquire tokens from both global and per-migration limiters.
    pub async fn acquire(&self, migration_id: Uuid, bytes: u64) -> Duration {
        let per_migration = self.for_migration(migration_id);

        // Acquire from both limiters (the slower one determines the rate)
        let (global_wait, per_wait) = tokio::join!(
            self.global.acquire(bytes),
            per_migration.acquire(bytes)
        );

        global_wait.max(per_wait)
    }

    /// Get total available bandwidth.
    pub fn global_available(&self) -> u64 {
        self.global.available()
    }
}

/// Entry being transferred during migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferEntry {
    /// Key.
    pub key: Vec<u8>,
    /// Value.
    pub value: Vec<u8>,
    /// Expiration time (unix nanos), if any.
    pub expires_at_nanos: Option<u64>,
    /// Version/revision for conflict resolution.
    pub version: u64,
}

impl TransferEntry {
    /// Create a new transfer entry.
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            expires_at_nanos: None,
            version: 0,
        }
    }

    /// Create with expiration.
    pub fn with_expiration(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        expires_at_nanos: u64,
    ) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            expires_at_nanos: Some(expires_at_nanos),
            version: 0,
        }
    }

    /// Get the size in bytes.
    pub fn size(&self) -> usize {
        self.key.len() + self.value.len()
    }

    /// Check if the entry has expired.
    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at_nanos {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            expires_at <= now
        } else {
            false
        }
    }

    /// Get remaining TTL as Duration.
    pub fn remaining_ttl(&self) -> Option<Duration> {
        self.expires_at_nanos.and_then(|expires_at| {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            if expires_at > now {
                Some(Duration::from_nanos(expires_at - now))
            } else {
                None
            }
        })
    }
}

/// Batch of entries being transferred.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferBatch {
    /// Migration ID.
    pub migration_id: Uuid,
    /// Sequence number.
    pub sequence: u64,
    /// Entries in this batch.
    pub entries: Vec<TransferEntry>,
    /// Whether this is the final batch.
    pub is_final: bool,
}

impl TransferBatch {
    /// Create a new batch.
    pub fn new(migration_id: Uuid, sequence: u64, entries: Vec<TransferEntry>, is_final: bool) -> Self {
        Self {
            migration_id,
            sequence,
            entries,
            is_final,
        }
    }

    /// Get total size of the batch.
    pub fn size(&self) -> usize {
        self.entries.iter().map(|e| e.size()).sum()
    }

    /// Get number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if batch is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Filter out expired entries.
    pub fn filter_expired(mut self) -> Self {
        self.entries.retain(|e| !e.is_expired());
        self
    }
}

/// Persistent storage for migration state.
#[async_trait::async_trait]
pub trait MigrationStateStore: Send + Sync + std::fmt::Debug {
    /// Save a migration.
    async fn save_migration(&self, migration: &ShardMigration) -> Result<()>;

    /// Save a checkpoint.
    async fn save_checkpoint(&self, checkpoint: &MigrationCheckpoint) -> Result<()>;

    /// Load incomplete migrations.
    async fn load_incomplete_migrations(&self) -> Result<Vec<ShardMigration>>;

    /// Load the latest checkpoint for a migration.
    async fn load_checkpoint(&self, migration_id: Uuid) -> Result<Option<MigrationCheckpoint>>;

    /// Delete a migration and its checkpoints.
    async fn delete_migration(&self, migration_id: Uuid) -> Result<()>;
}

/// Result of checking cluster membership state during recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterMembershipState {
    /// Target node is already a voter - migration completed successfully.
    TargetIsVoter,
    /// Target node is a learner - migration in progress, can continue.
    TargetIsLearner,
    /// Target node is not in the group - migration not started or rolled back.
    TargetNotInGroup,
    /// Source node is not in the group - already removed.
    SourceNotInGroup,
    /// Both nodes are voters - intermediate state during transfer.
    BothAreVoters,
    /// Unable to determine state (e.g., cannot contact cluster).
    Unknown,
}

/// Trait for checking actual cluster membership state during recovery.
///
/// This allows the recovery logic to verify the actual state of Raft groups
/// rather than relying solely on persisted migration state.
#[async_trait::async_trait]
pub trait ClusterStateChecker: Send + Sync + std::fmt::Debug {
    /// Check the membership state for a shard's Raft group.
    ///
    /// Returns the current state of source and target nodes in the Raft group.
    async fn check_membership_state(
        &self,
        shard_id: ShardId,
        source_node: NodeId,
        target_node: NodeId,
    ) -> Result<ClusterMembershipState>;

    /// Check if a node is the current leader for a shard.
    async fn is_leader(&self, shard_id: ShardId, node_id: NodeId) -> Result<bool>;

    /// Get the current owner/primary for a shard.
    async fn get_shard_owner(&self, shard_id: ShardId) -> Result<Option<NodeId>>;
}

/// No-op cluster state checker for testing or when cluster state is unavailable.
#[derive(Debug, Default)]
pub struct NoOpClusterStateChecker;

#[async_trait::async_trait]
impl ClusterStateChecker for NoOpClusterStateChecker {
    async fn check_membership_state(
        &self,
        _shard_id: ShardId,
        _source_node: NodeId,
        _target_node: NodeId,
    ) -> Result<ClusterMembershipState> {
        Ok(ClusterMembershipState::Unknown)
    }

    async fn is_leader(&self, _shard_id: ShardId, _node_id: NodeId) -> Result<bool> {
        Ok(false)
    }

    async fn get_shard_owner(&self, _shard_id: ShardId) -> Result<Option<NodeId>> {
        Ok(None)
    }
}

/// Recovery action determined by analyzing cluster state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryAction {
    /// Migration completed successfully, mark as complete.
    MarkComplete,
    /// Migration can be resumed from current phase.
    Resume,
    /// Migration should be retried from the beginning.
    Retry,
    /// Migration failed and should be marked as such.
    MarkFailed(String),
    /// Cannot determine action, requires manual intervention.
    ManualIntervention(String),
}

/// In-memory migration state store (for testing).
#[derive(Debug, Default)]
pub struct InMemoryMigrationStore {
    migrations: Mutex<HashMap<Uuid, ShardMigration>>,
    checkpoints: Mutex<HashMap<Uuid, MigrationCheckpoint>>,
}

impl InMemoryMigrationStore {
    /// Create a new in-memory store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl MigrationStateStore for InMemoryMigrationStore {
    async fn save_migration(&self, migration: &ShardMigration) -> Result<()> {
        self.migrations.lock().insert(migration.id, migration.clone());
        Ok(())
    }

    async fn save_checkpoint(&self, checkpoint: &MigrationCheckpoint) -> Result<()> {
        self.checkpoints.lock().insert(checkpoint.migration_id, checkpoint.clone());
        Ok(())
    }

    async fn load_incomplete_migrations(&self) -> Result<Vec<ShardMigration>> {
        let migrations = self.migrations.lock();
        Ok(migrations
            .values()
            .filter(|m| !m.phase.is_terminal())
            .cloned()
            .collect())
    }

    async fn load_checkpoint(&self, migration_id: Uuid) -> Result<Option<MigrationCheckpoint>> {
        Ok(self.checkpoints.lock().get(&migration_id).cloned())
    }

    async fn delete_migration(&self, migration_id: Uuid) -> Result<()> {
        self.migrations.lock().remove(&migration_id);
        self.checkpoints.lock().remove(&migration_id);
        Ok(())
    }
}

/// Coordinates shard migrations with two-phase handover.
#[derive(Debug)]
pub struct ShardMigrationCoordinator {
    /// Configuration.
    config: MigrationConfig,
    /// This node's ID.
    node_id: NodeId,
    /// Active migrations.
    active_migrations: RwLock<HashMap<ShardId, ShardMigration>>,
    /// Completed migrations (recent history).
    history: RwLock<Vec<ShardMigration>>,
    /// Maximum history size.
    max_history: usize,
    /// Paused flag (kill switch).
    paused: AtomicBool,
    /// Persistent state store.
    state_store: Arc<dyn MigrationStateStore>,
    /// Total migrations started.
    migrations_started: AtomicU64,
    /// Total migrations completed.
    migrations_completed: AtomicU64,
    /// Total migrations failed.
    migrations_failed: AtomicU64,
}

impl ShardMigrationCoordinator {
    /// Create a new migration coordinator.
    pub fn new(
        node_id: NodeId,
        config: MigrationConfig,
        state_store: Arc<dyn MigrationStateStore>,
    ) -> Self {
        Self {
            config,
            node_id,
            active_migrations: RwLock::new(HashMap::new()),
            history: RwLock::new(Vec::new()),
            max_history: 100,
            paused: AtomicBool::new(false),
            state_store,
            migrations_started: AtomicU64::new(0),
            migrations_completed: AtomicU64::new(0),
            migrations_failed: AtomicU64::new(0),
        }
    }

    /// Create with default config and in-memory store.
    pub fn with_defaults(node_id: NodeId) -> Self {
        Self::new(
            node_id,
            MigrationConfig::default(),
            Arc::new(InMemoryMigrationStore::new()),
        )
    }

    /// Get the configuration.
    pub fn config(&self) -> &MigrationConfig {
        &self.config
    }

    /// Check if migrations are paused.
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// Pause all migrations (kill switch).
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
        tracing::warn!("Migration coordinator paused");
    }

    /// Resume migrations.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        tracing::info!("Migration coordinator resumed");
    }

    /// Get number of active migrations.
    pub fn active_count(&self) -> usize {
        self.active_migrations.read().len()
    }

    /// Check if a shard is being migrated.
    pub fn is_migrating(&self, shard_id: ShardId) -> bool {
        self.active_migrations.read().contains_key(&shard_id)
    }

    /// Get migration for a shard.
    pub fn get_migration(&self, shard_id: ShardId) -> Option<ShardMigration> {
        self.active_migrations.read().get(&shard_id).cloned()
    }

    /// Get all active migrations.
    pub fn active_migrations(&self) -> Vec<ShardMigration> {
        self.active_migrations.read().values().cloned().collect()
    }

    /// Get migration history.
    pub fn history(&self) -> Vec<ShardMigration> {
        self.history.read().clone()
    }

    /// Plan a migration (Phase 1).
    pub async fn plan_migration(&self, movement: ShardMovement) -> Result<ShardMigration> {
        // Check if paused
        if self.is_paused() {
            return Err(Error::MigrationPaused);
        }

        // Check concurrent limit
        if self.active_count() >= self.config.max_concurrent {
            return Err(Error::TooManyMigrations);
        }

        // Check if shard is already being migrated
        if self.is_migrating(movement.shard_id) {
            return Err(Error::ShardAlreadyMigrating(movement.shard_id));
        }

        // Create migration
        let migration = ShardMigration::from_movement(&movement);

        // Persist
        self.state_store.save_migration(&migration).await?;

        // Add to active
        self.active_migrations
            .write()
            .insert(movement.shard_id, migration.clone());

        self.migrations_started.fetch_add(1, Ordering::Relaxed);

        tracing::info!(
            migration_id = %migration.id,
            shard_id = migration.shard_id,
            from_node = migration.from_node,
            to_node = migration.to_node,
            "Migration planned"
        );

        Ok(migration)
    }

    /// Start streaming phase (Phase 2a).
    ///
    /// # Persistence Order
    ///
    /// Persists to disk FIRST, then updates in-memory state. This ensures
    /// crash-consistency: if we crash between operations, disk has the new state.
    pub async fn start_streaming(&self, shard_id: ShardId, total_entries: u64, total_bytes: u64) -> Result<()> {
        // Create updated migration for persistence (validate and clone under read lock)
        let migration_for_persist = {
            let migrations = self.active_migrations.read();
            let migration = migrations
                .get(&shard_id)
                .ok_or(Error::MigrationNotFound(shard_id))?;

            if migration.phase != MigrationPhase::Planned {
                return Err(Error::InvalidMigrationPhase(migration.phase.to_string()));
            }

            let mut updated = migration.clone();
            updated.phase = MigrationPhase::Streaming;
            updated.progress = MigrationProgress::new(total_entries, total_bytes);
            updated
        };

        // CRITICAL: Persist to disk FIRST - if crash happens after this, recovery sees new state
        self.state_store.save_migration(&migration_for_persist).await?;

        // Now update in-memory state (safe - disk is already consistent)
        {
            let mut migrations = self.active_migrations.write();
            if let Some(migration) = migrations.get_mut(&shard_id) {
                migration.phase = MigrationPhase::Streaming;
                migration.progress = MigrationProgress::new(total_entries, total_bytes);
            }
        }

        tracing::info!(
            migration_id = %migration_for_persist.id,
            shard_id,
            total_entries,
            total_bytes,
            "Streaming started"
        );

        Ok(())
    }

    /// Update streaming progress.
    pub async fn update_progress(
        &self,
        shard_id: ShardId,
        entries: u64,
        bytes: u64,
        rate: f64,
        last_key: Option<Vec<u8>>,
    ) -> Result<Option<MigrationCheckpoint>> {
        let mut migrations = self.active_migrations.write();
        let migration = migrations
            .get_mut(&shard_id)
            .ok_or(Error::MigrationNotFound(shard_id))?;

        migration.progress.update(entries, bytes, rate);
        migration.last_key = last_key;

        // Checkpoint periodically
        let should_checkpoint = migration.progress.transferred_entries
            % self.config.checkpoint_interval
            == 0;

        let checkpoint = if should_checkpoint {
            let cp = migration.checkpoint();
            self.state_store.save_checkpoint(&cp).await?;
            Some(cp)
        } else {
            None
        };

        Ok(checkpoint)
    }

    /// Start catch-up phase (Phase 2b).
    ///
    /// # Persistence Order
    ///
    /// Persists to disk FIRST, then updates in-memory state for crash-consistency.
    pub async fn start_catchup(&self, shard_id: ShardId) -> Result<()> {
        // Create updated migration for persistence
        let migration_for_persist = {
            let migrations = self.active_migrations.read();
            let migration = migrations
                .get(&shard_id)
                .ok_or(Error::MigrationNotFound(shard_id))?;

            if migration.phase != MigrationPhase::Streaming {
                return Err(Error::InvalidMigrationPhase(migration.phase.to_string()));
            }

            let mut updated = migration.clone();
            updated.phase = MigrationPhase::CatchingUp;
            updated
        };

        // CRITICAL: Persist to disk FIRST
        self.state_store.save_migration(&migration_for_persist).await?;

        // Now update in-memory state
        {
            let mut migrations = self.active_migrations.write();
            if let Some(migration) = migrations.get_mut(&shard_id) {
                migration.phase = MigrationPhase::CatchingUp;
            }
        }

        tracing::info!(
            migration_id = %migration_for_persist.id,
            shard_id,
            "Catch-up phase started"
        );

        Ok(())
    }

    /// Start commit phase (Phase 3) - ATOMIC!
    ///
    /// This is the critical point where ownership changes hands.
    /// The commit should be done via Raft for atomicity.
    ///
    /// # Persistence Order
    ///
    /// Persists to disk FIRST, then updates in-memory state for crash-consistency.
    pub async fn start_commit(&self, shard_id: ShardId) -> Result<Uuid> {
        // Create updated migration for persistence
        let migration_for_persist = {
            let migrations = self.active_migrations.read();
            let migration = migrations
                .get(&shard_id)
                .ok_or(Error::MigrationNotFound(shard_id))?;

            if migration.phase != MigrationPhase::CatchingUp {
                return Err(Error::InvalidMigrationPhase(migration.phase.to_string()));
            }

            let mut updated = migration.clone();
            updated.phase = MigrationPhase::Committing;
            updated
        };

        let migration_id = migration_for_persist.id;

        // CRITICAL: Persist to disk FIRST
        self.state_store.save_migration(&migration_for_persist).await?;

        // Now update in-memory state
        {
            let mut migrations = self.active_migrations.write();
            if let Some(migration) = migrations.get_mut(&shard_id) {
                migration.phase = MigrationPhase::Committing;
            }
        }

        tracing::info!(
            migration_id = %migration_id,
            shard_id,
            "Commit phase started - ownership handover"
        );

        Ok(migration_id)
    }

    /// Complete the migration after successful commit.
    ///
    /// # Persistence Order
    ///
    /// Persists to disk FIRST, then updates in-memory state for crash-consistency.
    pub async fn complete_migration(&self, shard_id: ShardId) -> Result<ShardMigration> {
        // Create completed migration for persistence (don't remove yet)
        let migration_for_persist = {
            let migrations = self.active_migrations.read();
            let migration = migrations
                .get(&shard_id)
                .ok_or(Error::MigrationNotFound(shard_id))?;

            let mut updated = migration.clone();
            updated.set_phase(MigrationPhase::Complete);
            updated
        };

        // CRITICAL: Persist to disk FIRST - recovery will see completed state
        self.state_store.save_migration(&migration_for_persist).await?;

        // Now safe to remove from active migrations and add to history
        // (we only need the side effect of removal; the updated state is in migration_for_persist)
        {
            let mut migrations = self.active_migrations.write();
            migrations.remove(&shard_id);
        }

        // Add to history
        {
            let mut history = self.history.write();
            history.push(migration_for_persist.clone());
            if history.len() > self.max_history {
                history.remove(0);
            }
        }

        self.migrations_completed.fetch_add(1, Ordering::Relaxed);

        tracing::info!(
            migration_id = %migration_for_persist.id,
            shard_id,
            duration_ms = migration_for_persist.duration().as_millis(),
            "Migration completed successfully"
        );

        Ok(migration_for_persist)
    }

    /// Fail a migration.
    ///
    /// # Persistence Order
    ///
    /// Persists to disk FIRST, then updates in-memory state for crash-consistency.
    pub async fn fail_migration(&self, shard_id: ShardId, error: String) -> Result<ShardMigration> {
        // Create failed migration for persistence (don't remove yet)
        let migration_for_persist = {
            let migrations = self.active_migrations.read();
            let migration = migrations
                .get(&shard_id)
                .ok_or(Error::MigrationNotFound(shard_id))?;

            let mut updated = migration.clone();
            updated.fail(error.clone());
            updated
        };

        // CRITICAL: Persist to disk FIRST - recovery will see failed state
        self.state_store.save_migration(&migration_for_persist).await?;

        // Now safe to remove from active migrations
        {
            let mut migrations = self.active_migrations.write();
            migrations.remove(&shard_id);
        }

        // Add to history
        {
            let mut history = self.history.write();
            history.push(migration_for_persist.clone());
            if history.len() > self.max_history {
                history.remove(0);
            }
        }

        self.migrations_failed.fetch_add(1, Ordering::Relaxed);

        tracing::error!(
            migration_id = %migration_for_persist.id,
            shard_id,
            error,
            "Migration failed"
        );

        Ok(migration_for_persist)
    }

    /// Cancel a migration.
    ///
    /// # Persistence Order
    ///
    /// Persists to disk FIRST, then updates in-memory state for crash-consistency.
    pub async fn cancel_migration(&self, shard_id: ShardId) -> Result<ShardMigration> {
        // Create cancelled migration for persistence (don't remove yet)
        let migration_for_persist = {
            let migrations = self.active_migrations.read();
            let migration = migrations
                .get(&shard_id)
                .ok_or(Error::MigrationNotFound(shard_id))?;

            let mut updated = migration.clone();
            updated.set_phase(MigrationPhase::Cancelled);
            updated
        };

        // CRITICAL: Persist to disk FIRST
        self.state_store.save_migration(&migration_for_persist).await?;

        // Now safe to remove from active migrations
        {
            let mut migrations = self.active_migrations.write();
            migrations.remove(&shard_id);
        }

        // Add to history
        {
            let mut history = self.history.write();
            history.push(migration_for_persist.clone());
            if history.len() > self.max_history {
                history.remove(0);
            }
        }

        tracing::warn!(
            migration_id = %migration_for_persist.id,
            shard_id,
            "Migration cancelled"
        );

        Ok(migration_for_persist)
    }

    /// Recover from crash - resume incomplete migrations.
    ///
    /// This basic recovery method does not check actual cluster state.
    /// For production use, prefer `recover_with_cluster_check` which verifies
    /// the actual Raft membership state before deciding how to proceed.
    pub async fn recover(&self) -> Result<Vec<ShardMigration>> {
        self.recover_with_cluster_check(None).await
    }

    /// Recover from crash with actual cluster state verification.
    ///
    /// This method checks the actual Raft membership state to determine the
    /// correct recovery action for each incomplete migration, rather than
    /// relying solely on persisted state which may be stale.
    pub async fn recover_with_cluster_check(
        &self,
        cluster_checker: Option<&dyn ClusterStateChecker>,
    ) -> Result<Vec<ShardMigration>> {
        let incomplete = self.state_store.load_incomplete_migrations().await?;
        let mut recovered = Vec::new();

        for mut migration in incomplete {
            let action = self
                .determine_recovery_action(&migration, cluster_checker)
                .await;

            match action {
                RecoveryAction::MarkComplete => {
                    tracing::info!(
                        migration_id = %migration.id,
                        shard_id = migration.shard_id,
                        "Recovery: Migration already completed (verified via cluster state)"
                    );
                    migration.set_phase(MigrationPhase::Complete);
                    self.state_store.save_migration(&migration).await?;
                    self.migrations_completed.fetch_add(1, Ordering::Relaxed);
                }
                RecoveryAction::Resume => {
                    // Load checkpoint if available
                    if let Some(checkpoint) = self
                        .state_store
                        .load_checkpoint(migration.id)
                        .await?
                    {
                        migration.last_key = checkpoint.last_key;
                        migration.progress = checkpoint.progress;
                        migration.checkpoint_sequence = checkpoint.sequence;
                        tracing::info!(
                            migration_id = %migration.id,
                            shard_id = migration.shard_id,
                            phase = %migration.phase,
                            "Recovery: Resuming migration from checkpoint"
                        );
                    } else {
                        tracing::info!(
                            migration_id = %migration.id,
                            shard_id = migration.shard_id,
                            phase = %migration.phase,
                            "Recovery: Resuming migration (no checkpoint)"
                        );
                    }

                    self.active_migrations
                        .write()
                        .insert(migration.shard_id, migration.clone());
                    recovered.push(migration);
                }
                RecoveryAction::Retry => {
                    tracing::info!(
                        migration_id = %migration.id,
                        shard_id = migration.shard_id,
                        "Recovery: Retrying migration from beginning"
                    );
                    // Reset to planned phase
                    migration.phase = MigrationPhase::Planned;
                    migration.progress = MigrationProgress::new(0, 0);
                    migration.last_key = None;
                    migration.checkpoint_sequence = 0;
                    self.state_store.save_migration(&migration).await?;

                    self.active_migrations
                        .write()
                        .insert(migration.shard_id, migration.clone());
                    recovered.push(migration);
                }
                RecoveryAction::MarkFailed(reason) => {
                    tracing::error!(
                        migration_id = %migration.id,
                        shard_id = migration.shard_id,
                        reason = %reason,
                        "Recovery: Marking migration as failed"
                    );
                    migration.fail(reason);
                    self.state_store.save_migration(&migration).await?;
                    self.migrations_failed.fetch_add(1, Ordering::Relaxed);
                }
                RecoveryAction::ManualIntervention(reason) => {
                    tracing::warn!(
                        migration_id = %migration.id,
                        shard_id = migration.shard_id,
                        reason = %reason,
                        "Recovery: Manual intervention required"
                    );
                    migration.fail(format!("Manual intervention required: {}", reason));
                    self.state_store.save_migration(&migration).await?;
                    self.migrations_failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        Ok(recovered)
    }

    /// Determine the appropriate recovery action for a migration.
    async fn determine_recovery_action(
        &self,
        migration: &ShardMigration,
        cluster_checker: Option<&dyn ClusterStateChecker>,
    ) -> RecoveryAction {
        match migration.phase {
            MigrationPhase::Committing => {
                // Critical phase - must check actual cluster state
                self.determine_committing_recovery(migration, cluster_checker)
                    .await
            }
            MigrationPhase::Cleanup => {
                // Cleanup phase - migration succeeded, just need to finalize
                RecoveryAction::MarkComplete
            }
            phase if phase.is_resumable() => {
                // Resumable phases - verify we can still proceed
                self.determine_resumable_recovery(migration, cluster_checker)
                    .await
            }
            MigrationPhase::Complete | MigrationPhase::Failed | MigrationPhase::Cancelled => {
                // Terminal states - nothing to do
                RecoveryAction::MarkComplete
            }
            _ => RecoveryAction::ManualIntervention(format!(
                "Unknown phase: {}",
                migration.phase
            )),
        }
    }

    /// Determine recovery action for a migration in the Committing phase.
    async fn determine_committing_recovery(
        &self,
        migration: &ShardMigration,
        cluster_checker: Option<&dyn ClusterStateChecker>,
    ) -> RecoveryAction {
        let Some(checker) = cluster_checker else {
            // No cluster checker available - cannot verify, require manual intervention
            return RecoveryAction::ManualIntervention(
                "Interrupted during commit phase - cluster state verification unavailable".to_string()
            );
        };

        // Check actual cluster membership state
        match checker
            .check_membership_state(migration.shard_id, migration.from_node, migration.to_node)
            .await
        {
            Ok(ClusterMembershipState::TargetIsVoter) => {
                // Target is a voter - check if it's the owner now
                match checker.get_shard_owner(migration.shard_id).await {
                    Ok(Some(owner)) if owner == migration.to_node => {
                        // Migration completed successfully
                        RecoveryAction::MarkComplete
                    }
                    Ok(Some(owner)) if owner == migration.from_node => {
                        // Source is still owner but target is voter
                        // This is an intermediate state - complete the transfer
                        RecoveryAction::Resume
                    }
                    Ok(_) => {
                        // Unexpected owner - manual check needed
                        RecoveryAction::ManualIntervention(
                            "Commit phase: unexpected shard owner".to_string()
                        )
                    }
                    Err(e) => {
                        RecoveryAction::ManualIntervention(format!(
                            "Commit phase: failed to get shard owner: {}",
                            e
                        ))
                    }
                }
            }
            Ok(ClusterMembershipState::TargetIsLearner) => {
                // Target is still a learner - commit didn't complete
                // Can retry from promoting to voter
                RecoveryAction::Resume
            }
            Ok(ClusterMembershipState::TargetNotInGroup) => {
                // Target not in group - commit definitely didn't happen
                // Need to retry from the beginning
                RecoveryAction::Retry
            }
            Ok(ClusterMembershipState::SourceNotInGroup) => {
                // Source removed - migration might have completed or failed
                match checker.get_shard_owner(migration.shard_id).await {
                    Ok(Some(owner)) if owner == migration.to_node => {
                        RecoveryAction::MarkComplete
                    }
                    _ => RecoveryAction::ManualIntervention(
                        "Commit phase: source removed but target not confirmed as owner".to_string()
                    ),
                }
            }
            Ok(ClusterMembershipState::BothAreVoters) => {
                // Both are voters - in middle of transfer, can complete
                RecoveryAction::Resume
            }
            Ok(ClusterMembershipState::Unknown) | Err(_) => {
                RecoveryAction::ManualIntervention(
                    "Commit phase: unable to determine cluster membership state".to_string()
                )
            }
        }
    }

    /// Determine recovery action for resumable phases.
    async fn determine_resumable_recovery(
        &self,
        migration: &ShardMigration,
        cluster_checker: Option<&dyn ClusterStateChecker>,
    ) -> RecoveryAction {
        // For resumable phases, we can usually just resume
        // But let's verify the migration is still valid if we have a checker

        let Some(checker) = cluster_checker else {
            // No checker - assume we can resume
            return RecoveryAction::Resume;
        };

        // Verify source node still owns the shard
        match checker.get_shard_owner(migration.shard_id).await {
            Ok(Some(owner)) if owner == migration.from_node => {
                // Source still owns it - can resume
                RecoveryAction::Resume
            }
            Ok(Some(owner)) if owner == migration.to_node => {
                // Target already owns it - migration completed
                RecoveryAction::MarkComplete
            }
            Ok(Some(_other)) => {
                // Someone else owns it - unexpected state
                RecoveryAction::MarkFailed(
                    "Shard ownership changed unexpectedly during migration".to_string()
                )
            }
            Ok(None) => {
                // No owner - shard might be unassigned
                RecoveryAction::ManualIntervention(
                    "Shard has no owner - cannot determine if migration is valid".to_string()
                )
            }
            Err(_) => {
                // Can't verify - try to resume anyway
                RecoveryAction::Resume
            }
        }
    }

    /// Get migration statistics.
    pub fn stats(&self) -> MigrationStats {
        MigrationStats {
            active_count: self.active_count(),
            started_total: self.migrations_started.load(Ordering::Relaxed),
            completed_total: self.migrations_completed.load(Ordering::Relaxed),
            failed_total: self.migrations_failed.load(Ordering::Relaxed),
            paused: self.is_paused(),
        }
    }
}

/// Migration statistics.
#[derive(Debug, Clone)]
pub struct MigrationStats {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_phase_terminal() {
        assert!(!MigrationPhase::Planned.is_terminal());
        assert!(!MigrationPhase::Streaming.is_terminal());
        assert!(!MigrationPhase::CatchingUp.is_terminal());
        assert!(!MigrationPhase::Committing.is_terminal());
        assert!(!MigrationPhase::Cleanup.is_terminal());
        assert!(MigrationPhase::Complete.is_terminal());
        assert!(MigrationPhase::Failed.is_terminal());
        assert!(MigrationPhase::Cancelled.is_terminal());
    }

    #[test]
    fn test_migration_phase_resumable() {
        assert!(MigrationPhase::Planned.is_resumable());
        assert!(MigrationPhase::Streaming.is_resumable());
        assert!(MigrationPhase::CatchingUp.is_resumable());
        assert!(!MigrationPhase::Committing.is_resumable());
        assert!(!MigrationPhase::Complete.is_resumable());
    }

    #[test]
    fn test_progress_percentage() {
        let mut progress = MigrationProgress::new(100, 1000);
        assert_eq!(progress.percentage(), 0.0);

        progress.update(50, 500, 100.0);
        assert_eq!(progress.percentage(), 50.0);

        progress.update(50, 500, 100.0);
        assert_eq!(progress.percentage(), 100.0);
        assert!(progress.is_complete());
    }

    #[test]
    fn test_transfer_entry_expiration() {
        let entry = TransferEntry::new(b"key", b"value");
        assert!(!entry.is_expired());

        // Expired entry (time in past)
        let expired = TransferEntry::with_expiration(b"key", b"value", 1);
        assert!(expired.is_expired());

        // Future expiration
        let future_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
            + 1_000_000_000; // 1 second from now
        let not_expired = TransferEntry::with_expiration(b"key", b"value", future_nanos);
        assert!(!not_expired.is_expired());
    }

    #[test]
    fn test_transfer_batch() {
        let entries = vec![
            TransferEntry::new(b"k1", b"v1"),
            TransferEntry::new(b"k2", b"v2"),
        ];

        let batch = TransferBatch::new(Uuid::new_v4(), 1, entries, false);
        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
        assert!(!batch.is_final);
    }

    #[tokio::test]
    async fn test_migration_coordinator_plan() {
        let coordinator = ShardMigrationCoordinator::with_defaults(1);

        let movement = ShardMovement::new(0, 1, 2, super::super::shard_placement::MovementType::AddReplica);
        let migration = coordinator.plan_migration(movement).await.unwrap();

        assert_eq!(migration.shard_id, 0);
        assert_eq!(migration.from_node, 1);
        assert_eq!(migration.to_node, 2);
        assert_eq!(migration.phase, MigrationPhase::Planned);

        assert!(coordinator.is_migrating(0));
        assert_eq!(coordinator.active_count(), 1);
    }

    #[tokio::test]
    async fn test_migration_coordinator_pause() {
        let coordinator = ShardMigrationCoordinator::with_defaults(1);

        assert!(!coordinator.is_paused());

        coordinator.pause();
        assert!(coordinator.is_paused());

        // Should fail when paused
        let movement = ShardMovement::new(0, 1, 2, super::super::shard_placement::MovementType::AddReplica);
        let result = coordinator.plan_migration(movement).await;
        assert!(result.is_err());

        coordinator.resume();
        assert!(!coordinator.is_paused());
    }

    #[tokio::test]
    async fn test_migration_lifecycle() {
        let coordinator = ShardMigrationCoordinator::with_defaults(1);

        // Plan
        let movement = ShardMovement::new(0, 1, 2, super::super::shard_placement::MovementType::AddReplica);
        coordinator.plan_migration(movement).await.unwrap();

        // Start streaming
        coordinator.start_streaming(0, 1000, 10000).await.unwrap();
        assert_eq!(
            coordinator.get_migration(0).unwrap().phase,
            MigrationPhase::Streaming
        );

        // Update progress
        coordinator.update_progress(0, 500, 5000, 1000.0, Some(b"key500".to_vec())).await.unwrap();

        // Start catch-up
        coordinator.start_catchup(0).await.unwrap();
        assert_eq!(
            coordinator.get_migration(0).unwrap().phase,
            MigrationPhase::CatchingUp
        );

        // Start commit
        coordinator.start_commit(0).await.unwrap();
        assert_eq!(
            coordinator.get_migration(0).unwrap().phase,
            MigrationPhase::Committing
        );

        // Complete
        let migration = coordinator.complete_migration(0).await.unwrap();
        assert_eq!(migration.phase, MigrationPhase::Complete);
        assert!(!coordinator.is_migrating(0));
    }
}
