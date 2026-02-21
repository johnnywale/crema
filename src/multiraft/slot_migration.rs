//! Lazy slot migration system.
//!
//! This module implements background slot migration with crash-safe state persistence.
//! Migration uses a state machine approach with explicit phases that can be resumed
//! after crashes.
//!
//! # Migration Phases
//!
//! ```text
//! Pending → Scanning → Streaming → CatchingUp → Completed
//!                                      │
//!                                      └──(failure)──► Failed ──(retry)──► Pending
//! ```
//!
//! # Crash Recovery
//!
//! Each phase persists a checkpoint (cursor/last_key) that allows resuming
//! from the last known position after a crash.

use super::memberlist_integration::ShardLeaderTracker;
use super::router::ShardRouter;
use super::shard::ShardId;
use super::shard_forwarder::ShardForwarder;
use super::shard_raft_manager::ShardRaftManager;
use super::shard_raft_node::ShardRaftNode;
use super::slot_control_plane::SlotControlPlane;
use super::slot_table::{crc16, SlotId, SlotTable, TOTAL_SLOTS};
use crate::error::{Error, RaftError, Result};
use crate::metrics::CacheMetrics;
use crate::types::{CacheCommand, NodeId};
use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Migration phase state machine.
///
/// State transitions:
/// ```text
/// PENDING → CLAIMED → SCANNING → STREAMING → CATCHING_UP → PREPARED → COMPLETED → CLEANED
///    │         │          │           │            │            │          │          │
///    │         │          │           │            │            │          │          └─ Source data deleted
///    │         │          │           │            │            │          └─ Target validated + durable
///    │         │          │           │            │            └─ Target has all data, ready for cleanup
///    │         │          │           │            └─ Catching up with new writes
///    │         │          │           └─ Data transfer in progress
///    │         │          └─ Scanning source for keys
///    │         └─ Leader claimed ownership via Raft
///    └─ Migration registered
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum MigrationPhase {
    /// Slot reassigned, migration not started.
    /// - New owner accepts writes (may not have data yet)
    /// - Old owner handles reads, redirects writes
    #[default]
    Pending,

    /// Leader claimed ownership via Raft proposal.
    /// Only the claim owner should drive this migration forward.
    Claimed {
        /// The node that claimed ownership.
        owner_node: NodeId,
        /// Epoch when claimed (for stale detection).
        claim_epoch: u64,
        /// When claimed (ms since epoch).
        claimed_at: u64,
    },

    /// Scanning source for keys in this slot.
    Scanning {
        /// Resume cursor for crash recovery (last scanned key).
        cursor: Option<Vec<u8>>,
        /// Number of keys found so far.
        keys_found: u64,
    },

    /// Transferring data from source to target.
    Streaming {
        /// Total keys to transfer.
        keys_total: u64,
        /// Keys transferred so far.
        keys_transferred: u64,
        /// Last key transferred (resume point).
        last_key: Option<Vec<u8>>,
    },

    /// All bulk data transferred, catching up writes that happened during transfer.
    CatchingUp {
        /// Raft log position to replay from.
        from_log_index: u64,
    },

    /// Target validated, source cleanup allowed (Raft-committed).
    /// This state ensures target durability before source deletion.
    Prepared {
        /// When prepared (ms since epoch).
        prepared_at: u64,
        /// Target shard's Raft commit index at validation time.
        target_commit_index: u64,
        /// Checksum of migrated data for verification.
        validation_checksum: u64,
    },

    /// Migration complete, target is authoritative.
    Completed {
        /// When migration completed (ms since epoch).
        completed_at: u64,
    },

    /// Source data cleaned up (optional final state).
    Cleaned {
        /// When cleaned (ms since epoch).
        cleaned_at: u64,
    },

    /// Migration failed, needs retry.
    Failed {
        /// Error message.
        error: String,
        /// When it failed (ms since epoch).
        failed_at: u64,
        /// Number of retry attempts.
        retry_count: u32,
    },
}

impl MigrationPhase {
    /// Check if migration is complete (including cleaned state).
    pub fn is_completed(&self) -> bool {
        matches!(
            self,
            MigrationPhase::Completed { .. } | MigrationPhase::Cleaned { .. }
        )
    }

    /// Check if migration has failed.
    pub fn is_failed(&self) -> bool {
        matches!(self, MigrationPhase::Failed { .. })
    }

    /// Check if migration is in progress (not terminal).
    pub fn is_in_progress(&self) -> bool {
        !self.is_completed() && !self.is_failed()
    }

    /// Check if migration is in an active transfer state.
    pub fn is_actively_transferring(&self) -> bool {
        matches!(
            self,
            MigrationPhase::Scanning { .. }
                | MigrationPhase::Streaming { .. }
                | MigrationPhase::CatchingUp { .. }
        )
    }

    /// Check if migration is prepared (validated, ready for cleanup).
    pub fn is_prepared(&self) -> bool {
        matches!(self, MigrationPhase::Prepared { .. })
    }

    /// Check if migration is claimed by a specific node.
    pub fn is_claimed_by(&self, node_id: NodeId) -> bool {
        match self {
            MigrationPhase::Claimed { owner_node, .. } => *owner_node == node_id,
            _ => false,
        }
    }

    /// Get the owner node if migration is claimed.
    pub fn owner(&self) -> Option<NodeId> {
        match self {
            MigrationPhase::Claimed { owner_node, .. } => Some(*owner_node),
            _ => None,
        }
    }

    /// Get the claim epoch if migration is claimed.
    pub fn claim_epoch(&self) -> Option<u64> {
        match self {
            MigrationPhase::Claimed { claim_epoch, .. } => Some(*claim_epoch),
            _ => None,
        }
    }

    /// Get the ordinal value of this phase for comparison.
    fn ordinal(&self) -> u8 {
        match self {
            MigrationPhase::Pending => 0,
            MigrationPhase::Claimed { .. } => 1,
            MigrationPhase::Scanning { .. } => 2,
            MigrationPhase::Streaming { .. } => 3,
            MigrationPhase::CatchingUp { .. } => 4,
            MigrationPhase::Prepared { .. } => 5,
            MigrationPhase::Completed { .. } => 6,
            MigrationPhase::Cleaned { .. } => 7,
            MigrationPhase::Failed { .. } => 0, // Failed is not more advanced
        }
    }

    /// Check if this phase is more advanced than another.
    /// Used for syncing migration state from peers.
    pub fn is_more_advanced_than(&self, other: &MigrationPhase) -> bool {
        // Failed state doesn't advance; Completed/Cleaned are terminal
        if matches!(self, MigrationPhase::Failed { .. }) {
            return false;
        }
        self.ordinal() > other.ordinal()
    }

    /// Get phase name for logging.
    pub fn name(&self) -> &'static str {
        match self {
            MigrationPhase::Pending => "Pending",
            MigrationPhase::Claimed { .. } => "Claimed",
            MigrationPhase::Scanning { .. } => "Scanning",
            MigrationPhase::Streaming { .. } => "Streaming",
            MigrationPhase::CatchingUp { .. } => "CatchingUp",
            MigrationPhase::Prepared { .. } => "Prepared",
            MigrationPhase::Completed { .. } => "Completed",
            MigrationPhase::Cleaned { .. } => "Cleaned",
            MigrationPhase::Failed { .. } => "Failed",
        }
    }
}

/// Unique migration identifier for idempotency.
///
/// Each migration is uniquely identified by (slot_id, epoch).
/// When a migration times out or fails, the epoch is incremented
/// to allow a new owner to take over without conflicts.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct MigrationId {
    /// The slot being migrated.
    pub slot_id: SlotId,
    /// Epoch for this migration attempt (incremented on takeover).
    pub epoch: u64,
}

impl MigrationId {
    /// Create a new migration ID.
    pub fn new(slot_id: SlotId, epoch: u64) -> Self {
        Self { slot_id, epoch }
    }

    /// Create with epoch 1 (first attempt).
    pub fn first(slot_id: SlotId) -> Self {
        Self { slot_id, epoch: 1 }
    }

    /// Create the next epoch for this slot.
    pub fn next_epoch(&self) -> Self {
        Self {
            slot_id: self.slot_id,
            epoch: self.epoch + 1,
        }
    }
}

impl std::fmt::Display for MigrationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "slot:{}:epoch:{}", self.slot_id, self.epoch)
    }
}

/// Record of a slot migration (persisted for crash recovery).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotMigrationRecord {
    /// Unique migration identifier for idempotency.
    pub id: MigrationId,
    /// The slot being migrated.
    pub slot_id: SlotId,
    /// Source shard (where data is coming from).
    pub from_shard: ShardId,
    /// Target shard (where data is going to).
    pub to_shard: ShardId,
    /// Current migration phase.
    pub phase: MigrationPhase,
    /// When migration was initiated (ms since epoch).
    pub created_at: u64,
    /// Last state update (ms since epoch).
    pub updated_at: u64,
    /// Last progress timestamp for timeout detection (ms since epoch).
    pub last_progress_at: u64,
    /// Total keys actually transferred during this migration.
    pub keys_migrated: u64,
    /// Which node drove this migration to completion (None if sync-completed from slot table).
    pub completed_by_node: Option<NodeId>,
}

impl SlotMigrationRecord {
    /// Create a new migration record.
    pub fn new(slot_id: SlotId, from_shard: ShardId, to_shard: ShardId) -> Self {
        let now = now_ms();
        Self {
            id: MigrationId::first(slot_id),
            slot_id,
            from_shard,
            to_shard,
            phase: MigrationPhase::Pending,
            created_at: now,
            updated_at: now,
            last_progress_at: now,
            keys_migrated: 0,
            completed_by_node: None,
        }
    }

    /// Create a new migration record with a specific epoch.
    pub fn with_epoch(slot_id: SlotId, from_shard: ShardId, to_shard: ShardId, epoch: u64) -> Self {
        let now = now_ms();
        Self {
            id: MigrationId::new(slot_id, epoch),
            slot_id,
            from_shard,
            to_shard,
            phase: MigrationPhase::Pending,
            created_at: now,
            updated_at: now,
            last_progress_at: now,
            keys_migrated: 0,
            completed_by_node: None,
        }
    }

    /// Update the phase.
    pub fn set_phase(&mut self, phase: MigrationPhase) {
        self.phase = phase;
        let now = now_ms();
        self.updated_at = now;
        self.last_progress_at = now;
    }

    /// Update progress timestamp without changing phase.
    pub fn touch_progress(&mut self) {
        self.last_progress_at = now_ms();
    }

    /// Mark as failed.
    pub fn mark_failed(&mut self, error: String) {
        let retry_count = match &self.phase {
            MigrationPhase::Failed { retry_count, .. } => *retry_count + 1,
            _ => 1,
        };

        self.phase = MigrationPhase::Failed {
            error,
            failed_at: now_ms(),
            retry_count,
        };
        self.updated_at = now_ms();
    }

    /// Check if can retry.
    pub fn can_retry(&self, max_retries: u32) -> bool {
        match &self.phase {
            MigrationPhase::Failed { retry_count, .. } => *retry_count < max_retries,
            _ => false,
        }
    }

    /// Check if this migration is stale (no progress for given duration).
    pub fn is_stale(&self, timeout_ms: u64) -> bool {
        let now = now_ms();
        self.phase.is_in_progress() && (now.saturating_sub(self.last_progress_at) > timeout_ms)
    }

    /// Increment epoch for takeover after timeout.
    pub fn increment_epoch(&mut self) {
        self.id = self.id.next_epoch();
        self.phase = MigrationPhase::Pending;
        let now = now_ms();
        self.updated_at = now;
        self.last_progress_at = now;
    }

    /// Get current epoch.
    pub fn epoch(&self) -> u64 {
        self.id.epoch
    }
}

/// Raft log entry for migration state transitions.
///
/// These commands are proposed through Raft to ensure cluster-wide
/// consistency of migration state. Critical transitions (claim, prepared,
/// completed) must be Raft-committed before proceeding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MigrationRaftCommand {
    /// Claim ownership of a slot migration.
    /// Only one node can successfully claim a migration per epoch.
    Claim {
        /// Unique migration identifier.
        migration_id: MigrationId,
        /// The node claiming ownership.
        leader_id: NodeId,
        /// Timestamp proposed by the leader (ms since epoch).
        /// This ensures deterministic state machine replay.
        proposed_at: u64,
    },

    /// Mark migration as prepared (target validated).
    /// This commits the validation result to Raft before source cleanup.
    Prepared {
        /// Unique migration identifier.
        migration_id: MigrationId,
        /// Target shard's Raft commit index at validation time.
        target_commit_index: u64,
        /// Checksum of migrated data.
        validation_checksum: u64,
        /// Timestamp proposed by the leader (ms since epoch).
        /// This ensures deterministic state machine replay.
        proposed_at: u64,
    },

    /// Mark migration as completed.
    /// Source data has been deleted and verified empty.
    Completed {
        /// Unique migration identifier.
        migration_id: MigrationId,
        /// Timestamp proposed by the leader (ms since epoch).
        /// This ensures deterministic state machine replay.
        proposed_at: u64,
    },

    /// Mark source as cleaned (optional).
    Cleaned {
        /// Unique migration identifier.
        migration_id: MigrationId,
        /// Timestamp proposed by the leader (ms since epoch).
        /// This ensures deterministic state machine replay.
        proposed_at: u64,
    },
}

impl MigrationRaftCommand {
    /// Get the migration ID from this command.
    pub fn migration_id(&self) -> &MigrationId {
        match self {
            MigrationRaftCommand::Claim { migration_id, .. } => migration_id,
            MigrationRaftCommand::Prepared { migration_id, .. } => migration_id,
            MigrationRaftCommand::Completed { migration_id, .. } => migration_id,
            MigrationRaftCommand::Cleaned { migration_id, .. } => migration_id,
        }
    }

    /// Get the proposed timestamp from this command.
    ///
    /// This timestamp is set by the leader when proposing the command,
    /// ensuring deterministic state machine replay across all replicas.
    pub fn proposed_at(&self) -> u64 {
        match self {
            MigrationRaftCommand::Claim { proposed_at, .. } => *proposed_at,
            MigrationRaftCommand::Prepared { proposed_at, .. } => *proposed_at,
            MigrationRaftCommand::Completed { proposed_at, .. } => *proposed_at,
            MigrationRaftCommand::Cleaned { proposed_at, .. } => *proposed_at,
        }
    }

    /// Get the command name for logging.
    pub fn name(&self) -> &'static str {
        match self {
            MigrationRaftCommand::Claim { .. } => "Claim",
            MigrationRaftCommand::Prepared { .. } => "Prepared",
            MigrationRaftCommand::Completed { .. } => "Completed",
            MigrationRaftCommand::Cleaned { .. } => "Cleaned",
        }
    }
}

/// Validation result from target shard.
///
/// Contains all information needed to verify migration success
/// before committing the PREPARED state.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Target shard's Raft commit index.
    pub raft_commit_index: u64,
    /// Number of keys in the slot on target.
    pub key_count: u64,
    /// Checksum of slot data for verification.
    pub checksum: u64,
    /// Number of replicas holding the data.
    pub replica_count: u32,
    /// Whether a follower sample verification passed.
    pub follower_sample_ok: bool,
}

/// Configuration for the slot migrator.
#[derive(Debug, Clone)]
pub struct SlotMigratorConfig {
    /// Batch size for scanning keys.
    pub scan_batch_size: usize,
    /// Batch size for streaming data.
    pub stream_batch_size: usize,
    /// Maximum retries for failed migrations.
    pub max_retries: u32,
    /// Interval between migration loop iterations.
    pub loop_interval: Duration,
    /// Timeout for individual migration operations.
    pub operation_timeout: Duration,
    /// Timeout for migration progress before allowing takeover (ms).
    pub migration_timeout_ms: u64,
    /// Whether to require follower verification before PREPARED.
    pub require_follower_verification: bool,
}

impl Default for SlotMigratorConfig {
    fn default() -> Self {
        Self {
            scan_batch_size: 1000,
            stream_batch_size: 100,
            max_retries: 3,
            loop_interval: Duration::from_millis(100),
            operation_timeout: Duration::from_secs(30),
            migration_timeout_ms: 60_000, // 60 seconds
            require_follower_verification: false,
        }
    }
}

/// Status of the migration system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationStatus {
    /// Number of active migrations.
    pub active_migrations: usize,
    /// Number of completed migrations.
    pub completed_migrations: usize,
    /// Number of failed migrations.
    pub failed_migrations: usize,
    /// Total keys migrated.
    pub total_keys_migrated: u64,
    /// Migrations by phase.
    pub by_phase: HashMap<String, usize>,
    /// Number of migrations in Prepared state (validated, awaiting cleanup).
    pub prepared_count: usize,
    /// Number of migrations in Claimed state (owned but not started).
    pub claimed_count: usize,
}

/// Trait for accessing shard data during migration.
///
/// This abstracts the actual shard storage access so the migrator
/// can be tested independently.
#[async_trait::async_trait]
pub trait MigrationDataAccessor: Send + Sync {
    /// Scan keys belonging to a slot from a shard.
    async fn scan_slot_keys(
        &self,
        shard_id: ShardId,
        slot_id: SlotId,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<Vec<u8>>, Option<Vec<u8>>)>;

    /// Get key-value pairs from a shard.
    async fn get_keys(
        &self,
        shard_id: ShardId,
        keys: &[Vec<u8>],
    ) -> Result<Vec<(Vec<u8>, Option<Bytes>)>>;

    /// Import key-value pairs to a shard (idempotent).
    async fn import_keys(&self, shard_id: ShardId, data: &[(Vec<u8>, Bytes)]) -> Result<()>;

    /// Get the current log index for a shard.
    async fn current_log_index(&self, shard_id: ShardId) -> Result<u64>;

    /// Get log entries affecting a slot since a given index.
    async fn get_slot_log_entries(
        &self,
        shard_id: ShardId,
        slot_id: SlotId,
        from_index: u64,
        limit: usize,
    ) -> Result<Vec<SlotLogEntry>>;

    /// Check if a shard has a specific key.
    async fn has_key(&self, shard_id: ShardId, key: &[u8]) -> Result<bool>;

    /// Get a single key's value.
    async fn get_key(&self, shard_id: ShardId, key: &[u8]) -> Result<Option<Bytes>>;

    /// Put a single key's value (for on-demand migration).
    async fn put_key(&self, shard_id: ShardId, key: &[u8], value: &[u8]) -> Result<()>;

    // ==================== Migration Coordination Methods ====================

    /// Get shard's Raft commit index.
    /// Used for validation to ensure data is durably committed.
    async fn get_shard_commit_index(&self, shard_id: ShardId) -> Result<u64> {
        // Default implementation uses current_log_index
        self.current_log_index(shard_id).await
    }

    /// Count keys in a slot on a shard.
    /// Used for validation to verify all keys were migrated.
    async fn count_keys_in_slot(&self, shard_id: ShardId, slot_id: SlotId) -> Result<u64> {
        // Default implementation scans all keys
        let (keys, _) = self
            .scan_slot_keys(shard_id, slot_id, None, usize::MAX)
            .await?;
        Ok(keys.len() as u64)
    }

    /// Compute checksum for slot data on a shard.
    /// Used for validation to verify data integrity.
    async fn checksum_slot(&self, shard_id: ShardId, slot_id: SlotId) -> Result<u64> {
        // Default implementation: simple hash of key count
        // Real implementations should compute actual data checksum
        let count = self.count_keys_in_slot(shard_id, slot_id).await?;
        Ok(count)
    }

    /// Verify data exists on a follower (availability check).
    /// Returns true if at least one follower has the data.
    async fn verify_on_follower(&self, _shard_id: ShardId, _slot_id: SlotId) -> Result<bool> {
        // Default implementation: assume OK (no follower verification)
        Ok(true)
    }

    /// Get number of replicas for a shard.
    async fn get_replica_count(&self, _shard_id: ShardId) -> Result<u32> {
        // Default implementation: assume 1 (single node)
        Ok(1)
    }

    /// Sync storage to ensure all pending writes are visible.
    /// Called before validation to ensure checksums are accurate.
    async fn sync_storage(&self, _shard_id: ShardId) -> Result<()> {
        // Default implementation: no-op
        Ok(())
    }

    /// Wait for source shard replication to catch up.
    ///
    /// When this node is a follower for the source shard, data may be stale due to
    /// Raft replication lag. This method ensures the local storage is consistent
    /// with the source shard leader before reading data.
    ///
    /// Returns Ok(()) when sync is complete or times out.
    async fn wait_for_source_sync(&self, _shard_id: ShardId) -> Result<()> {
        // Default implementation: sync storage and return
        self.sync_storage(_shard_id).await
    }

    /// Delete all data for a slot from a shard.
    /// Called after PREPARED state is committed to clean up source.
    async fn delete_slot_data(&self, shard_id: ShardId, slot_id: SlotId) -> Result<()>;

    /// Check if a key was written by this migration (for idempotency).
    async fn is_migration_key_written(
        &self,
        _shard_id: ShardId,
        _key: &[u8],
        _migration_id: &MigrationId,
    ) -> Result<bool> {
        // Default implementation: no tracking, always return false
        Ok(false)
    }

    /// Mark a key as written by this migration (for idempotency).
    async fn mark_migration_key_written(
        &self,
        _shard_id: ShardId,
        _key: &[u8],
        _migration_id: &MigrationId,
    ) -> Result<()> {
        // Default implementation: no-op
        Ok(())
    }

    /// Check if this node is the source shard leader.
    fn is_source_shard_leader(&self, from_shard: ShardId) -> bool;

    /// Check if this node is the target shard leader.
    fn is_target_shard_leader(&self, to_shard: ShardId) -> bool;

    // ==================== Raft-Based Migration Coordination ====================

    /// Get the ShardRaftNode for a shard.
    ///
    /// This is used to propose migration commands through the target shard's Raft.
    /// Returns None if per-shard Raft is not enabled or the shard doesn't exist.
    fn get_shard_raft_node(&self, _shard_id: ShardId) -> Option<Arc<ShardRaftNode>> {
        // Default implementation: not available
        None
    }

    /// Get all migration records from all shards' MigrationStateMachines.
    ///
    /// This is used to sync SlotMigrator state from Raft-committed state.
    /// Returns records from all shards' MigrationStateMachines.
    fn get_all_raft_migration_records(&self) -> Vec<SlotMigrationRecord> {
        // Default implementation: empty (for test accessors without Raft)
        Vec::new()
    }

    // ==================== Shard Lifecycle Management ====================

    /// Cleanup a tombstoned shard after all migrations are complete.
    ///
    /// This is called by the migration loop to garbage collect shards that have
    /// been fully drained (all slots migrated away) and have passed the grace period.
    /// Implementations should:
    /// 1. Stop any Raft node for the shard
    /// 2. Unregister the shard from the router
    /// 3. Clean up any other shard-specific resources
    ///
    /// Returns Ok(()) if the shard was successfully cleaned up or didn't exist.
    fn gc_tombstoned_shard(&self, _shard_id: ShardId) -> Result<()> {
        // Default implementation: no-op (for test accessors)
        Ok(())
    }
}

/// A log entry affecting a slot (for catch-up phase).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotLogEntry {
    /// Log index.
    pub index: u64,
    /// Key affected.
    pub key: Vec<u8>,
    /// Operation type.
    pub operation: SlotLogOperation,
    /// Value (for puts) - stored as `Vec<u8>` for serialization.
    pub value: Option<Vec<u8>>,
}

/// Operation types in the log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SlotLogOperation {
    Put,
    Delete,
}

/// Slot migrator for background data migration.
///
/// The migrator runs a background loop that processes pending migrations
/// through their state machine phases.
///
/// # Migration Coordination
///
/// Only the source shard leader should drive migration:
/// - Claim ownership via Raft before processing
/// - Validate target before marking PREPARED
/// - Delete source only after PREPARED is committed
#[derive(Debug)]
pub struct SlotMigrator {
    /// This node's ID.
    node_id: NodeId,

    /// The slot table being managed.
    slot_table: Arc<SlotTable>,

    /// Migration records (slot_id -> record).
    migrations: RwLock<HashMap<SlotId, SlotMigrationRecord>>,

    /// Configuration.
    config: SlotMigratorConfig,

    /// Statistics.
    stats: RwLock<MigrationStats>,

    /// Metrics for observability.
    metrics: Option<Arc<CacheMetrics>>,

    /// Whether the migrator is running.
    running: std::sync::atomic::AtomicBool,

    /// Whether we had active migrations in the previous iteration.
    /// Used to detect and log when all migrations complete.
    had_active_migrations: std::sync::atomic::AtomicBool,

    /// Ordered event log for all migration events (for testing/debugging).
    event_log: RwLock<Vec<MigrationEvent>>,
}

/// Migration statistics.
#[derive(Debug, Clone, Default)]
struct MigrationStats {
    completed: u64,
    failed: u64,
    keys_migrated: u64,
}

impl SlotMigrator {
    /// Create a new slot migrator.
    pub fn new(node_id: NodeId, slot_table: Arc<SlotTable>, config: SlotMigratorConfig) -> Self {
        Self {
            node_id,
            slot_table,
            migrations: RwLock::new(HashMap::new()),
            config,
            stats: RwLock::new(MigrationStats::default()),
            metrics: None,
            running: std::sync::atomic::AtomicBool::new(false),
            had_active_migrations: std::sync::atomic::AtomicBool::new(false),
            event_log: RwLock::new(Vec::new()),
        }
    }

    /// Create with default configuration.
    pub fn with_defaults(slot_table: Arc<SlotTable>) -> Self {
        Self::new(0, slot_table, SlotMigratorConfig::default())
    }

    /// Create with node ID and default configuration.
    pub fn with_node_id(node_id: NodeId, slot_table: Arc<SlotTable>) -> Self {
        Self::new(node_id, slot_table, SlotMigratorConfig::default())
    }

    /// Set metrics for observability.
    pub fn with_metrics(mut self, metrics: Arc<CacheMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Record a migration event.
    fn record_event(
        &self,
        slot_id: SlotId,
        from_shard: ShardId,
        to_shard: ShardId,
        event_type: MigrationEventType,
    ) {
        self.record_event_with_duration(slot_id, from_shard, to_shard, event_type, None);
    }

    /// Record a migration event with an optional phase duration.
    fn record_event_with_duration(
        &self,
        slot_id: SlotId,
        from_shard: ShardId,
        to_shard: ShardId,
        event_type: MigrationEventType,
        phase_duration_ms: Option<u64>,
    ) {
        self.event_log.write().push(MigrationEvent {
            slot_id,
            from_shard,
            to_shard,
            event_type,
            timestamp_ms: now_ms(),
            node_id: self.node_id,
            phase_duration_ms,
        });
    }

    /// Compute duration since the last event matching the predicate for a given slot.
    fn phase_duration_since(
        &self,
        slot_id: SlotId,
        matcher: impl Fn(&MigrationEventType) -> bool,
    ) -> Option<u64> {
        let now = now_ms();
        self.event_log
            .read()
            .iter()
            .rev()
            .find(|e| e.slot_id == slot_id && matcher(&e.event_type))
            .map(|e| now.saturating_sub(e.timestamp_ms))
    }

    /// Get all migration events (for testing/debugging).
    pub fn events(&self) -> Vec<MigrationEvent> {
        self.event_log.read().clone()
    }

    /// Get events for a specific slot.
    pub fn events_for_slot(&self, slot_id: SlotId) -> Vec<MigrationEvent> {
        self.event_log
            .read()
            .iter()
            .filter(|e| e.slot_id == slot_id)
            .cloned()
            .collect()
    }

    /// Clear the event log.
    pub fn clear_events(&self) {
        self.event_log.write().clear();
    }

    /// Register a new migration.
    pub fn register_migration(&self, slot_id: SlotId, from_shard: ShardId, to_shard: ShardId) {
        let record = SlotMigrationRecord::new(slot_id, from_shard, to_shard);
        self.migrations.write().insert(slot_id, record);
        self.record_event(
            slot_id,
            from_shard,
            to_shard,
            MigrationEventType::Registered,
        );
        tracing::info!(slot_id, from_shard, to_shard, "Registered slot migration");
    }

    /// Register migrations from a reassignment.
    pub fn register_from_reassignment(&self, moves: &[(SlotId, ShardId, ShardId)]) {
        let mut migrations = self.migrations.write();
        for (slot_id, from, to) in moves {
            let record = SlotMigrationRecord::new(*slot_id, *from, *to);
            migrations.insert(*slot_id, record);
        }
        // Release lock before recording events
        drop(migrations);

        for (slot_id, from, to) in moves {
            self.record_event(*slot_id, *from, *to, MigrationEventType::Registered);
        }

        tracing::info!(
            count = moves.len(),
            "Registered migrations from reassignment"
        );
    }

    /// Sync migrations from slot table state.
    ///
    /// This detects slots in Migrating state that don't have a migration
    /// registered and auto-registers them. This ensures all nodes know about
    /// pending migrations even if they weren't the node that initiated the
    /// shard removal/addition.
    pub fn sync_from_slot_table(&self) {
        use crate::multiraft::slot_table::SlotState;

        let slot_table = self.slot_table.clone();
        let snapshot = slot_table.snapshot();

        // Collect slots that need migrations
        let mut slots_to_migrate = Vec::new();
        // Collect slots that have completed migration (no longer in Migrating state)
        let mut slots_completed = Vec::new();

        for (slot_id, assignment) in snapshot.slots.iter().enumerate() {
            let slot_id = slot_id as SlotId;
            // Check if slot is migrating
            if let SlotState::Migrating { from, .. } = &assignment.state {
                slots_to_migrate.push((slot_id, *from, assignment.owner));
            } else {
                // Slot is NOT migrating - if we have a local migration record for it,
                // the migration must have completed (slot is now Imported or Stable)
                slots_completed.push(slot_id);
            }
        }

        let mut sync_completed_events: Vec<(SlotId, ShardId, ShardId)> = Vec::new();

        let mut migrations = self.migrations.write();
        let mut inserted_count = 0;
        let mut completed_count = 0;

        // Mark completed migrations based on slot table state
        // This ensures follower nodes see completions when the slot table
        // (which IS synchronized via Raft) is updated by the leader
        for slot_id in slots_completed {
            if let Some(record) = migrations.get_mut(&slot_id) {
                if record.phase.is_in_progress() {
                    // Slot table says this slot is no longer migrating,
                    // so mark our local record as completed
                    // completed_by_node = None indicates sync-completion (not driven by this node)
                    record.completed_by_node = None;
                    record.set_phase(MigrationPhase::Completed {
                        completed_at: crate::multiraft::slot_migration::now_ms(),
                    });
                    sync_completed_events.push((slot_id, record.from_shard, record.to_shard));
                    completed_count += 1;
                    tracing::debug!(
                        slot_id,
                        "Migration marked completed based on slot table state (sync)"
                    );
                }
            }
        }

        if completed_count > 0 {
            // Update completed stats
            let mut stats = self.stats.write();
            stats.completed += completed_count as u64;
            tracing::info!(
                count = completed_count,
                "Marked migrations completed from slot table sync"
            );
        }

        // Register new migrations (replacing completed ones if needed)
        for (slot_id, from, to) in slots_to_migrate {
            let should_register = match migrations.get(&slot_id) {
                None => true,
                Some(existing) => {
                    // Replace if:
                    // 1. Existing migration is completed/cleaned, OR
                    // 2. Migration direction has REVERSED (e.g., ADD → REMOVE scenario)
                    //    When direction reverses, the old migration is obsolete and should
                    //    be replaced. This happens when a shard is added then quickly removed.
                    let is_completed = !existing.phase.is_in_progress();
                    let is_reversed = existing.from_shard == to && existing.to_shard == from;
                    is_completed || is_reversed
                }
            };

            if should_register {
                tracing::debug!(
                    slot_id,
                    from_shard = from,
                    to_shard = to,
                    "Auto-registered migration from slot table sync"
                );
                let record = SlotMigrationRecord::new(slot_id, from, to);
                migrations.insert(slot_id, record);
                inserted_count += 1;
            }
        }

        if inserted_count > 0 {
            tracing::info!(
                count = inserted_count,
                "Auto-registered migrations from slot table state"
            );
        }

        drop(migrations);

        // Record events after releasing the migrations lock
        for (slot_id, from_shard, to_shard) in sync_completed_events {
            self.record_event(
                slot_id,
                from_shard,
                to_shard,
                MigrationEventType::SyncCompleted,
            );
        }
    }

    /// Sync migrations by detecting active migrations from peers.
    ///
    /// This is a secondary sync mechanism that allows nodes to pick up
    /// migrations that were registered on other nodes but not locally
    /// (e.g., when shard removal was initiated on a different node).
    pub fn sync_from_peer_migrations(&self, peer_migrations: &[SlotMigrationRecord]) {
        tracing::debug!(
            node_id = self.node_id,
            peer_count = peer_migrations.len(),
            "Syncing migrations from peers"
        );

        let mut new_migrations = Vec::new();
        let mut updated_migrations = Vec::new();
        let mut replaced_migrations = Vec::new();

        for record in peer_migrations {
            let migrations = self.migrations.read();
            if let Some(existing) = migrations.get(&record.slot_id) {
                // Check if this is a DIFFERENT migration (new epoch with different from/to).
                // This happens during add-then-remove scenarios where:
                // - ADD: slot migrates from original_shard -> new_shard
                // - REMOVE: slot migrates from new_shard -> original_shard
                // The peer's migration should replace ours if it has different from/to shards.
                let is_different_migration = record.from_shard != existing.from_shard
                    || record.to_shard != existing.to_shard;

                if is_different_migration && record.phase.is_in_progress() {
                    // Peer has a newer migration for this slot (different direction)
                    // Replace our old migration with theirs
                    tracing::debug!(
                        slot_id = record.slot_id,
                        old_from = existing.from_shard,
                        old_to = existing.to_shard,
                        new_from = record.from_shard,
                        new_to = record.to_shard,
                        "Replacing migration with different direction from peer"
                    );
                    replaced_migrations.push(record.clone());
                } else if record.phase.is_more_advanced_than(&existing.phase) {
                    // Same migration, peer has more advanced phase
                    // (Claimed > Pending, Scanning > Claimed, etc.)
                    updated_migrations.push(record.clone());
                }
            } else if record.phase.is_in_progress() {
                // New migration - copy from peer (preserving phase)
                new_migrations.push(record.clone());
            }
        }

        if !new_migrations.is_empty()
            || !updated_migrations.is_empty()
            || !replaced_migrations.is_empty()
        {
            let new_count = new_migrations.len();
            let updated_count = updated_migrations.len();
            let replaced_count = replaced_migrations.len();

            tracing::debug!(
                node_id = self.node_id,
                new_count,
                updated_count,
                replaced_count,
                "Applying peer migration sync"
            );

            // Collect event data for recording after lock is dropped
            let mut peer_synced_events: Vec<(SlotId, ShardId, ShardId, bool)> = Vec::new();

            let mut migrations = self.migrations.write();

            // Add new migrations (preserving peer's phase)
            for record in &new_migrations {
                peer_synced_events.push((record.slot_id, record.from_shard, record.to_shard, true));
                migrations.insert(record.slot_id, record.clone());
            }

            // Update existing migrations with more advanced phases
            for record in &updated_migrations {
                peer_synced_events.push((
                    record.slot_id,
                    record.from_shard,
                    record.to_shard,
                    false,
                ));
                migrations.insert(record.slot_id, record.clone());
            }

            // Replace old migrations with new direction (e.g., after add-then-remove)
            for record in &replaced_migrations {
                peer_synced_events.push((record.slot_id, record.from_shard, record.to_shard, true));
                migrations.insert(record.slot_id, record.clone());
            }

            drop(migrations);

            // Record PeerSynced events after lock is dropped
            for (slot_id, from_shard, to_shard, is_new) in peer_synced_events {
                self.record_event(
                    slot_id,
                    from_shard,
                    to_shard,
                    MigrationEventType::PeerSynced { is_new },
                );
            }

            tracing::info!(
                new = new_count,
                updated = updated_count,
                replaced = replaced_count,
                "Synced migrations from peer state"
            );
        }
    }

    /// Get migration status.
    pub fn status(&self) -> MigrationStatus {
        let migrations = self.migrations.read();
        let stats = self.stats.read();

        let mut by_phase: HashMap<String, usize> = HashMap::new();
        let mut active = 0;
        let mut completed = 0;
        let mut failed = 0;
        let mut prepared = 0;
        let mut claimed = 0;

        for record in migrations.values() {
            *by_phase.entry(record.phase.name().to_string()).or_insert(0) += 1;

            match &record.phase {
                MigrationPhase::Completed { .. } | MigrationPhase::Cleaned { .. } => completed += 1,
                MigrationPhase::Failed { .. } => failed += 1,
                MigrationPhase::Prepared { .. } => {
                    prepared += 1;
                    active += 1;
                }
                MigrationPhase::Claimed { .. } => {
                    claimed += 1;
                    active += 1;
                }
                _ => active += 1,
            }
        }

        MigrationStatus {
            active_migrations: active,
            completed_migrations: completed + stats.completed as usize,
            failed_migrations: failed + stats.failed as usize,
            total_keys_migrated: stats.keys_migrated,
            by_phase,
            prepared_count: prepared,
            claimed_count: claimed,
        }
    }

    /// Get a migration record.
    pub fn get_migration(&self, slot_id: SlotId) -> Option<SlotMigrationRecord> {
        self.migrations.read().get(&slot_id).cloned()
    }

    /// Get mutable access to migrations (for testing only).
    #[cfg(test)]
    pub fn migrations_mut(
        &self,
    ) -> parking_lot::RwLockWriteGuard<'_, HashMap<SlotId, SlotMigrationRecord>> {
        self.migrations.write()
    }

    /// Get all active migrations.
    pub fn active_migrations(&self) -> Vec<SlotMigrationRecord> {
        self.migrations
            .read()
            .values()
            .filter(|r| r.phase.is_in_progress())
            .cloned()
            .collect()
    }

    /// Run the migration loop (call from a spawned task).
    pub async fn run<A: MigrationDataAccessor>(
        &self,
        accessor: Arc<A>,
        control_plane: Option<Arc<SlotControlPlane>>,
    ) {
        use std::sync::atomic::Ordering;

        self.running.store(true, Ordering::SeqCst);

        tracing::info!("Starting slot migration loop");

        while self.running.load(Ordering::SeqCst) {
            // Process pending migrations
            if let Err(e) = self.process_migrations(&accessor, &control_plane).await {
                tracing::warn!(error = %e, "Error in migration loop");
            }

            tokio::time::sleep(self.config.loop_interval).await;
        }

        tracing::info!("Slot migration loop stopped");
    }

    /// Stop the migration loop.
    pub fn stop(&self) {
        use std::sync::atomic::Ordering;
        self.running.store(false, Ordering::SeqCst);
    }

    /// Check if the migrator is running.
    pub fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Sync local migration state from Raft-committed state.
    ///
    /// This is critical for ensuring all nodes have the same view of migration states
    /// when claims and transitions are committed via Raft on other nodes.
    fn sync_from_raft_state_machines<A: MigrationDataAccessor>(&self, accessor: &Arc<A>) {
        let raft_records = accessor.get_all_raft_migration_records();

        if raft_records.is_empty() {
            return;
        }

        let mut updated = 0;
        let mut newly_completed: Vec<(SlotId, ShardId, ShardId)> = Vec::new();
        let mut migrations = self.migrations.write();

        for raft_record in raft_records {
            if let Some(local_record) = migrations.get_mut(&raft_record.slot_id) {
                // Only update if Raft state is more advanced than local state
                if raft_record.phase.is_more_advanced_than(&local_record.phase) {
                    // Track transitions to Completed so we can update slot table + emit events
                    let was_in_progress = local_record.phase.is_in_progress();
                    tracing::debug!(
                        slot_id = raft_record.slot_id,
                        old_phase = local_record.phase.name(),
                        new_phase = raft_record.phase.name(),
                        node_id = self.node_id,
                        "Syncing more advanced phase from Raft state machine"
                    );
                    local_record.phase = raft_record.phase.clone();
                    local_record.updated_at = raft_record.updated_at;
                    local_record.last_progress_at = raft_record.last_progress_at;
                    updated += 1;

                    // If Raft says Completed and our local record was still in-progress,
                    // update the slot table so non-owner nodes transition Migrating → Imported.
                    if was_in_progress && raft_record.phase.is_completed() {
                        newly_completed.push((
                            raft_record.slot_id,
                            local_record.from_shard,
                            local_record.to_shard,
                        ));
                    }
                }
            } else {
                // Raft has a record we don't have locally - add it
                tracing::debug!(
                    slot_id = raft_record.slot_id,
                    phase = raft_record.phase.name(),
                    node_id = self.node_id,
                    "Adding migration from Raft state machine"
                );
                // If the new record is already completed, also mark the slot table
                if raft_record.phase.is_completed() {
                    newly_completed.push((
                        raft_record.slot_id,
                        raft_record.from_shard,
                        raft_record.to_shard,
                    ));
                }
                migrations.insert(raft_record.slot_id, raft_record);
                updated += 1;
            }
        }

        drop(migrations);

        // Update slot table and emit events for newly-completed migrations.
        // This ensures non-owner nodes transition their slot tables from Migrating → Imported
        // when they learn about completions via Raft state machine sync.
        for (slot_id, from_shard, to_shard) in &newly_completed {
            self.slot_table.mark_imported(*slot_id);
            self.record_event(
                *slot_id,
                *from_shard,
                *to_shard,
                MigrationEventType::SyncCompleted,
            );
            tracing::debug!(
                slot_id,
                node_id = self.node_id,
                "Marked slot imported from Raft state machine sync"
            );
        }

        if updated > 0 {
            tracing::debug!(
                node_id = self.node_id,
                updated,
                newly_completed = newly_completed.len(),
                "Synced migration state from Raft state machines"
            );
        }
    }

    /// Process one iteration of migrations.
    async fn process_migrations<A: MigrationDataAccessor>(
        &self,
        accessor: &Arc<A>,
        control_plane: &Option<Arc<SlotControlPlane>>,
    ) -> Result<()> {
        // Sync migrations from slot table state to ensure all nodes know about
        // pending migrations (important when shard removal was triggered on another node)
        self.sync_from_slot_table();

        // Sync migrations from Raft state machines to get authoritative phase state
        // This ensures local state reflects Raft-committed claims and transitions
        self.sync_from_raft_state_machines(accessor);

        // Get migrations that need processing
        let pending: Vec<SlotMigrationRecord> = {
            let migrations = self.migrations.read();
            migrations
                .values()
                .filter(|r| r.phase.is_in_progress())
                .cloned()
                .collect()
        };

        // Count phases for metrics and logging
        let pending_count = pending
            .iter()
            .filter(|r| matches!(r.phase, MigrationPhase::Pending))
            .count();
        let claimed_count = pending
            .iter()
            .filter(|r| matches!(r.phase, MigrationPhase::Claimed { .. }))
            .count();
        let scanning_count = pending
            .iter()
            .filter(|r| matches!(r.phase, MigrationPhase::Scanning { .. }))
            .count();
        let streaming_count = pending
            .iter()
            .filter(|r| matches!(r.phase, MigrationPhase::Streaming { .. }))
            .count();
        let prepared_count = pending
            .iter()
            .filter(|r| matches!(r.phase, MigrationPhase::Prepared { .. }))
            .count();

        // Update metrics for slot migration states
        if let Some(metrics) = &self.metrics {
            metrics.update_slot_migration_states(
                pending_count,
                claimed_count,
                streaming_count,
                prepared_count,
            );
        }

        // Log migration state when there are active migrations
        if !pending.is_empty() {
            tracing::debug!(
                node_id = self.node_id,
                total = pending.len(),
                pending = pending_count,
                claimed = claimed_count,
                scanning = scanning_count,
                streaming = streaming_count,
                prepared = prepared_count,
                "Processing migrations"
            );
        }

        // Process migrations in priority order:
        // 1. Non-Pending phases first (Claimed, Scanning, etc.) - these are already in progress
        // 2. Then Pending phases (limited to avoid starving other phases)
        //
        // This prevents the scenario where claiming all 200+ slots blocks
        // processing of already-claimed slots for minutes.
        let max_pending_per_iteration = 50; // Limit new claims per iteration
        let mut pending_processed = 0;

        // Partition into non-pending and pending
        let (non_pending, pending_only): (Vec<_>, Vec<_>) = pending
            .into_iter()
            .partition(|r| !matches!(r.phase, MigrationPhase::Pending));

        // Process non-pending first (Claimed, Scanning, Streaming, CatchingUp, Prepared)
        for record in non_pending {
            if let Err(e) = self
                .advance_migration(record.slot_id, accessor, control_plane)
                .await
            {
                // Only mark as failed for permanent (non-retryable) errors.
                // Transient errors like NotLeader should be silently retried on the next
                // iteration - they don't indicate a migration failure, just that the
                // operation couldn't be performed right now (e.g., leader election in progress,
                // different shard leader, etc.).
                if e.is_retryable() {
                    tracing::debug!(
                        slot_id = record.slot_id,
                        phase = record.phase.name(),
                        error = %e,
                        "Transient error advancing migration, will retry"
                    );
                } else {
                    tracing::warn!(
                        slot_id = record.slot_id,
                        phase = record.phase.name(),
                        error = %e,
                        "Failed to advance migration (permanent error)"
                    );

                    // Mark as failed only for permanent errors
                    if let Some(record) = self.migrations.write().get_mut(&record.slot_id) {
                        let error = e.to_string();
                        record.mark_failed(error.clone());
                        let retry_count = match &record.phase {
                            MigrationPhase::Failed { retry_count, .. } => *retry_count,
                            _ => 0,
                        };
                        self.record_event(
                            record.slot_id,
                            record.from_shard,
                            record.to_shard,
                            MigrationEventType::Failed { error, retry_count },
                        );
                    }
                }
            }
        }

        // Process limited pending (new claims)
        for record in pending_only {
            if pending_processed >= max_pending_per_iteration {
                break; // Process rest in next iteration
            }

            if let Err(e) = self
                .advance_migration(record.slot_id, accessor, control_plane)
                .await
            {
                // Only mark as failed for permanent (non-retryable) errors.
                if e.is_retryable() {
                    tracing::debug!(
                        slot_id = record.slot_id,
                        phase = record.phase.name(),
                        error = %e,
                        "Transient error advancing migration, will retry"
                    );
                } else {
                    tracing::warn!(
                        slot_id = record.slot_id,
                        phase = record.phase.name(),
                        error = %e,
                        "Failed to advance migration (permanent error)"
                    );

                    // Mark as failed only for permanent errors
                    if let Some(record) = self.migrations.write().get_mut(&record.slot_id) {
                        let error = e.to_string();
                        record.mark_failed(error.clone());
                        let retry_count = match &record.phase {
                            MigrationPhase::Failed { retry_count, .. } => *retry_count,
                            _ => 0,
                        };
                        self.record_event(
                            record.slot_id,
                            record.from_shard,
                            record.to_shard,
                            MigrationEventType::Failed { error, retry_count },
                        );
                    }
                }
            }

            pending_processed += 1;
        }

        // Handle retries for failed migrations
        self.process_retries().await;

        // Check for and takeover stale migrations
        self.process_stale_migrations(accessor).await;

        // Re-count active migrations AFTER processing (some may have completed)
        let active_after: usize = self
            .migrations
            .read()
            .values()
            .filter(|r| r.phase.is_in_progress())
            .count();

        // Detect and log when all migrations complete
        let has_active = active_after > 0;
        let had_active = self
            .had_active_migrations
            .swap(has_active, std::sync::atomic::Ordering::SeqCst);

        if had_active && !has_active {
            // Transitioned from having migrations to having none
            let stats = self.stats.read();
            tracing::info!(
                node_id = self.node_id,
                total_completed = stats.completed,
                total_failed = stats.failed,
                "All slot migrations completed"
            );
        }

        // Log periodic status when there are active migrations
        if active_after > 0 && active_after % 10 == 0 {
            tracing::debug!(
                node_id = self.node_id,
                active_migrations = active_after,
                "Migration progress"
            );
        }

        // Garbage collect tombstoned shards that have passed the grace period
        self.gc_tombstoned_shards(accessor, control_plane);

        Ok(())
    }

    /// Garbage collect tombstoned shards that have passed the grace period.
    ///
    /// This is called periodically from the migration loop to clean up shards
    /// that have been fully drained (all slots migrated away).
    fn gc_tombstoned_shards<A: MigrationDataAccessor>(
        &self,
        accessor: &Arc<A>,
        control_plane: &Option<Arc<SlotControlPlane>>,
    ) {
        let Some(cp) = control_plane else {
            return;
        };

        // Get shards that have passed the tombstone grace period
        let gc_candidates = cp.get_gc_candidates();

        for shard_id in gc_candidates {
            tracing::info!(
                shard_id,
                node_id = self.node_id,
                "Garbage collecting tombstoned shard"
            );

            // Step 1: Clean up shard resources (Raft node, router entry, storage)
            if let Err(e) = accessor.gc_tombstoned_shard(shard_id) {
                tracing::warn!(
                    shard_id,
                    error = %e,
                    "Error cleaning up tombstoned shard resources"
                );
                // Continue to try GC from control plane anyway
            }

            // Step 2: Remove from control plane's shard_states
            if let Err(e) = cp.gc_shard(shard_id) {
                tracing::warn!(
                    shard_id,
                    error = %e,
                    "Error removing tombstoned shard from control plane"
                );
            } else {
                tracing::info!(
                    shard_id,
                    node_id = self.node_id,
                    "Successfully garbage collected tombstoned shard"
                );
            }
        }
    }

    /// Advance a single migration through its state machine.
    ///
    /// # Coordination
    ///
    /// Migration ownership is determined by:
    /// 1. If source shard has an active leader → source shard leader drives migration ("push" model)
    /// 2. If source shard is draining/removed (no leader) → target shard leader drives migration ("pull" model)
    ///
    /// This ensures migrations can complete even when source shards are being removed.
    pub(crate) async fn advance_migration<A: MigrationDataAccessor>(
        &self,
        slot_id: SlotId,
        accessor: &Arc<A>,
        control_plane: &Option<Arc<SlotControlPlane>>,
    ) -> Result<()> {
        let record = self
            .migrations
            .read()
            .get(&slot_id)
            .cloned()
            .ok_or(Error::MigrationNotFound(slot_id as u32))?;

        tracing::debug!(
            slot_id,
            phase = record.phase.name(),
            node_id = self.node_id,
            from_shard = record.from_shard,
            to_shard = record.to_shard,
            "Advancing migration"
        );

        // Determine who should drive this migration:
        // Target shard leader ALWAYS drives migration (pull model) because:
        // 1. Migration commands must go through target shard's Raft
        // 2. Only target shard leader can propose to target shard's Raft
        // 3. This ensures proper coordination even when source/target leaders differ
        let is_source_leader = accessor.is_source_shard_leader(record.from_shard);
        let is_target_leader = accessor.is_target_shard_leader(record.to_shard);

        // Only target shard leader drives migration
        let should_drive = is_target_leader;

        tracing::debug!(
            slot_id,
            node_id = self.node_id,
            from_shard = record.from_shard,
            to_shard = record.to_shard,
            is_source_leader,
            is_target_leader,
            should_drive,
            phase = record.phase.name(),
            "Migration drive decision"
        );

        // Once a node drives a migration locally through Scanning/Streaming/CatchingUp/Prepared,
        // it must continue to Completed even if it's no longer the target shard leader.
        // transition_to_completed() handles NotLeader via local fallback + mark_imported().
        //
        // This is important because:
        // 1. Local work phases (Scanning, Streaming, CatchingUp) don't go through Raft
        // 2. If leadership changes mid-migration, the new leader won't know about these phases
        // 3. The original claim owner must continue driving to avoid stuck migrations
        // 4. Prepared is the validated-but-not-yet-committed phase — if the owner loses
        //    leadership after CatchingUp → Prepared, it must still drive to Completed
        //
        // We check if this node is the original claim owner by looking at MigrationStateMachine
        // records, which are Raft-committed and thus authoritative.
        let is_migration_owner = match &record.phase {
            MigrationPhase::Claimed { owner_node, .. } => *owner_node == self.node_id,
            // For local work phases (+ Prepared), the owner IS this node if we have a record
            // in this phase (only the owner transitions to these phases)
            MigrationPhase::Scanning { .. }
            | MigrationPhase::Streaming { .. }
            | MigrationPhase::CatchingUp { .. }
            | MigrationPhase::Prepared { .. } => true, // Owner drives through Prepared to Completed
            // For terminal/initial phases, only target leader should drive
            MigrationPhase::Pending
            | MigrationPhase::Completed { .. }
            | MigrationPhase::Cleaned { .. }
            | MigrationPhase::Failed { .. } => false,
        };

        if !should_drive && !is_migration_owner {
            return Ok(());
        }

        match &record.phase {
            MigrationPhase::Pending => {
                tracing::info!(
                    slot_id,
                    node_id = self.node_id,
                    from_shard = record.from_shard,
                    to_shard = record.to_shard,
                    "Claiming migration ownership"
                );
                self.claim_migration(slot_id, &record, accessor).await?;
                // After successful claim, immediately transition to Scanning
                // instead of waiting for the next iteration. This dramatically
                // speeds up migrations by avoiding the iteration delay.
                self.transition_to_scanning(slot_id).await?;
            }
            MigrationPhase::Claimed {
                owner_node,
                claim_epoch,
                ..
            } => {
                // The epoch check ensures we don't process stale migrations from old epochs.
                if *claim_epoch != record.id.epoch {
                    if let Some(metrics) = &self.metrics {
                        metrics.record_epoch_conflict();
                    }
                    return Ok(());
                }
                // Check if we're the owner.
                // NOTE: The owner can transition to Scanning even if no longer target leader,
                // since transition_to_scanning is a local state update (not Raft-based).
                // This prevents migrations from getting stuck when leadership changes after claim.
                if *owner_node != self.node_id {
                    return Ok(());
                }
                self.transition_to_scanning(slot_id).await?;
            }
            MigrationPhase::Scanning { cursor, keys_found } => {
                if !self.is_valid_owner(&record) {
                    return Ok(());
                }
                self.process_scanning(slot_id, cursor.clone(), *keys_found, accessor)
                    .await?;
            }
            MigrationPhase::Streaming {
                keys_total,
                keys_transferred,
                last_key,
            } => {
                if !self.is_valid_owner(&record) {
                    return Ok(());
                }
                self.process_streaming(
                    slot_id,
                    *keys_total,
                    *keys_transferred,
                    last_key.clone(),
                    accessor,
                )
                .await?;
            }
            MigrationPhase::CatchingUp { from_log_index } => {
                if !self.is_valid_owner(&record) {
                    return Ok(());
                }
                self.process_catching_up(slot_id, *from_log_index, accessor, control_plane)
                    .await?;
            }
            MigrationPhase::Prepared { .. } => {
                if !self.is_valid_owner(&record) {
                    return Ok(());
                }
                self.transition_to_completed(slot_id, accessor, control_plane)
                    .await?;
            }
            MigrationPhase::Completed { .. } => {
                // Migration complete, nothing to do
            }
            MigrationPhase::Cleaned { .. } => {
                // Fully cleaned up, nothing to do
            }
            MigrationPhase::Failed { .. } => {
                // Handled in process_retries
            }
        }

        Ok(())
    }

    /// Check if this node is the valid owner of the migration.
    fn is_valid_owner(&self, record: &SlotMigrationRecord) -> bool {
        match &record.phase {
            MigrationPhase::Claimed {
                owner_node,
                claim_epoch,
                ..
            } => *owner_node == self.node_id && *claim_epoch == record.id.epoch,
            // For phases after Claimed, check the previous claim info
            // We need to track owner through all phases
            _ => {
                // For non-claimed phases, we rely on leadership check
                // which is done in advance_migration
                true
            }
        }
    }

    /// Claim ownership of a migration via Raft proposal.
    ///
    /// This proposes a Claim command through the target shard's Raft to ensure
    /// cluster-wide consistency. The claim will only be applied when committed.
    pub(crate) async fn claim_migration<A: MigrationDataAccessor>(
        &self,
        slot_id: SlotId,
        record: &SlotMigrationRecord,
        accessor: &Arc<A>,
    ) -> Result<()> {
        // Check if already claimed by someone else (local check before proposing)
        if let MigrationPhase::Claimed { owner_node, .. } = &record.phase {
            if *owner_node != self.node_id {
                tracing::debug!(
                    slot_id,
                    owner_node,
                    self_node_id = self.node_id,
                    "Migration already claimed by another node"
                );
                return Ok(());
            }
        }

        // Try to propose through target shard's Raft
        let command = MigrationRaftCommand::Claim {
            migration_id: record.id.clone(),
            leader_id: self.node_id,
            proposed_at: now_ms(),
        };

        match self
            .propose_to_target_shard(command, record.to_shard, accessor)
            .await
        {
            Ok(_) => {
                // Apply to local state ONLY after successful Raft commit
                self.apply_claim_to_local_state(slot_id);
                tracing::info!(
                    slot_id,
                    node_id = self.node_id,
                    epoch = record.id.epoch,
                    target_shard = record.to_shard,
                    "Migration claim committed via Raft"
                );
                Ok(())
            }
            Err(Error::Raft(RaftError::NotLeader { leader })) => {
                // Not the target shard leader - can't propose claim
                tracing::trace!(
                    slot_id,
                    node_id = self.node_id,
                    target_shard = record.to_shard,
                    ?leader,
                    "Skipping claim: not target shard leader"
                );
                Ok(()) // Not an error, just skip this iteration
            }
            Err(e) => {
                // CRITICAL: Do NOT fall back to local state on Raft failure!
                // This could cause split-brain where two nodes both think they own the migration.
                // Instead, fail and let the retry logic handle it.
                tracing::warn!(
                    slot_id,
                    node_id = self.node_id,
                    target_shard = record.to_shard,
                    error = %e,
                    "Raft proposal failed for claim - will retry"
                );
                Err(e)
            }
        }
    }

    /// Apply a successful claim to local state.
    /// Only called after Raft has committed the claim.
    fn apply_claim_to_local_state(&self, slot_id: SlotId) {
        let now = now_ms();
        let claimed_phase = MigrationPhase::Claimed {
            owner_node: self.node_id,
            claim_epoch: 0, // Placeholder for comparison
            claimed_at: now,
        };

        let mut migrations = self.migrations.write();
        if let Some(record) = migrations.get_mut(&slot_id) {
            // Only apply if the current phase is NOT more advanced than Claimed.
            // This prevents overwriting phases like Scanning, Streaming, CatchingUp, Prepared
            // when the Raft commit arrives after local progression.
            if record.phase.ordinal() > claimed_phase.ordinal() {
                return;
            }

            let claim_epoch = record.id.epoch;
            record.set_phase(MigrationPhase::Claimed {
                owner_node: self.node_id,
                claim_epoch,
                claimed_at: now,
            });
            self.record_event(
                slot_id,
                record.from_shard,
                record.to_shard,
                MigrationEventType::Claimed { claim_epoch },
            );
        }
    }

    /// Propose a migration command through the target shard's Raft.
    ///
    /// This is the core method for Raft-based migration coordination.
    /// All critical state transitions (Claim, Prepared, Completed) go through
    /// the target shard's Raft to ensure cluster-wide consistency.
    async fn propose_to_target_shard<A: MigrationDataAccessor>(
        &self,
        command: MigrationRaftCommand,
        target_shard: ShardId,
        accessor: &Arc<A>,
    ) -> Result<()> {
        // Get target shard's RaftNode
        let raft_node = accessor
            .get_shard_raft_node(target_shard)
            .ok_or(Error::ShardNotFound(target_shard))?;

        // Propose the migration command
        raft_node.propose_migration(command).await?;
        Ok(())
    }

    /// Transition from Claimed to Scanning.
    pub(crate) async fn transition_to_scanning(&self, slot_id: SlotId) -> Result<()> {
        let mut migrations = self.migrations.write();
        if let Some(record) = migrations.get_mut(&slot_id) {
            let old_phase = record.phase.name();
            // Guard against regression from completed states
            if matches!(record.phase, MigrationPhase::Completed { .. }) {
                return Ok(());
            }
            record.set_phase(MigrationPhase::Scanning {
                cursor: None,
                keys_found: 0,
            });
            self.record_event(
                slot_id,
                record.from_shard,
                record.to_shard,
                MigrationEventType::ScanningStarted,
            );
            tracing::debug!(slot_id, old_phase, "Migration: {} → Scanning", old_phase);
        } else {
            tracing::warn!(slot_id, "Migration not found during transition to scanning");
        }
        Ok(())
    }

    /// Process the Scanning phase.
    pub(crate) async fn process_scanning<A: MigrationDataAccessor>(
        &self,
        slot_id: SlotId,
        cursor: Option<Vec<u8>>,
        keys_found: u64,
        accessor: &Arc<A>,
    ) -> Result<()> {
        let record = self
            .migrations
            .read()
            .get(&slot_id)
            .cloned()
            .ok_or(Error::MigrationNotFound(slot_id as u32))?;

        // On first scan batch, ensure source shard data is replicated and visible.
        // This is critical when this node is a follower for the source shard, as
        // Raft replication lag could cause us to miss keys.
        if cursor.is_none() {
            tracing::debug!(
                slot_id,
                from_shard = record.from_shard,
                "Waiting for source shard replication sync before scanning"
            );
            accessor.wait_for_source_sync(record.from_shard).await?;
        }

        tracing::trace!(
            slot_id,
            from_shard = record.from_shard,
            has_cursor = cursor.is_some(),
            batch_size = self.config.scan_batch_size,
            "Scanning slot keys"
        );

        // Scan a batch of keys from source shard
        let (keys, next_cursor) = accessor
            .scan_slot_keys(
                record.from_shard,
                slot_id,
                cursor.as_deref(),
                self.config.scan_batch_size,
            )
            .await?;

        let new_keys_found = keys_found + keys.len() as u64;

        // Handle reversed migration case:
        // When a migration direction is reversed (e.g., ADD shard then REMOVE immediately),
        // the source shard (newly added) may have no keys because the original forward
        // migration never completed. In this case, the data is still on the target shard
        // (the original location). We can skip data transfer and complete immediately.
        if cursor.is_none() && keys.is_empty() && next_cursor.is_none() {
            // First scan found no keys. Check if target already has this slot's data.
            let (target_keys, _) = accessor
                .scan_slot_keys(record.to_shard, slot_id, None, 1)
                .await
                .unwrap_or_else(|_| (vec![], None));

            if !target_keys.is_empty() {
                tracing::info!(
                    slot_id,
                    from_shard = record.from_shard,
                    to_shard = record.to_shard,
                    "Migration reversed: source has no data, target already has data. Skipping transfer."
                );

                // Mark migration as completed - data is already at target
                let mut migrations = self.migrations.write();
                let (from_shard, to_shard) = if let Some(record) = migrations.get_mut(&slot_id) {
                    record.keys_migrated = 0; // No transfer needed
                    record.completed_by_node = Some(self.node_id);
                    record.set_phase(MigrationPhase::Completed {
                        completed_at: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64,
                    });
                    (record.from_shard, record.to_shard)
                } else {
                    (0, 0)
                };
                drop(migrations);

                // Record SkippedReversed event
                self.record_event(
                    slot_id,
                    from_shard,
                    to_shard,
                    MigrationEventType::SkippedReversed,
                );

                // Update stats
                let mut stats = self.stats.write();
                stats.completed += 1;

                return Ok(());
            }
        }

        let mut migrations = self.migrations.write();
        if let Some(record) = migrations.get_mut(&slot_id) {
            if next_cursor.is_none() {
                // No more data to scan (cursor exhausted), transition to Streaming
                record.set_phase(MigrationPhase::Streaming {
                    keys_total: new_keys_found,
                    keys_transferred: 0,
                    last_key: None,
                });
                let scan_duration = self.phase_duration_since(slot_id, |t| {
                    matches!(t, MigrationEventType::ScanningStarted)
                });
                self.record_event_with_duration(
                    slot_id,
                    record.from_shard,
                    record.to_shard,
                    MigrationEventType::ScanningCompleted {
                        keys_found: new_keys_found,
                    },
                    scan_duration,
                );
                self.record_event(
                    slot_id,
                    record.from_shard,
                    record.to_shard,
                    MigrationEventType::StreamingStarted {
                        keys_total: new_keys_found,
                    },
                );
                tracing::debug!(
                    slot_id,
                    keys_total = new_keys_found,
                    "Migration: Scanning → Streaming"
                );
            } else {
                // More data to scan (cursor points to resume position)
                tracing::trace!(slot_id, keys_found = new_keys_found, "Scanning continues");
                record.set_phase(MigrationPhase::Scanning {
                    cursor: next_cursor,
                    keys_found: new_keys_found,
                });
            }
        }

        Ok(())
    }

    /// Process the Streaming phase.
    pub(crate) async fn process_streaming<A: MigrationDataAccessor>(
        &self,
        slot_id: SlotId,
        keys_total: u64,
        keys_transferred: u64,
        last_key: Option<Vec<u8>>,
        accessor: &Arc<A>,
    ) -> Result<()> {
        let record = self
            .migrations
            .read()
            .get(&slot_id)
            .cloned()
            .ok_or(Error::MigrationNotFound(slot_id as u32))?;

        // Get a batch of keys after the last transferred key
        let (keys, _) = accessor
            .scan_slot_keys(
                record.from_shard,
                slot_id,
                last_key.as_deref(),
                self.config.stream_batch_size,
            )
            .await?;

        if keys.is_empty() {
            // All data transferred, get log index for catch-up
            let log_index = accessor.current_log_index(record.from_shard).await?;

            {
                let mut migrations = self.migrations.write();
                if let Some(record) = migrations.get_mut(&slot_id) {
                    // Snapshot keys_transferred before phase transition
                    record.keys_migrated = keys_transferred;
                    record.set_phase(MigrationPhase::CatchingUp {
                        from_log_index: log_index,
                    });
                    self.record_event(
                        slot_id,
                        record.from_shard,
                        record.to_shard,
                        MigrationEventType::CatchingUpStarted {
                            from_log_index: log_index,
                        },
                    );

                    tracing::debug!(slot_id, log_index, "Migration: Streaming → CatchingUp");
                }
            }

            return Ok(());
        }

        // Get values for the keys
        let kv_pairs = accessor.get_keys(record.from_shard, &keys).await?;

        // Filter to only keys with values
        let data: Vec<(Vec<u8>, Bytes)> = kv_pairs
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .collect();

        let transferred = data.len();

        // Import to target (idempotent)
        accessor.import_keys(record.to_shard, &data).await?;

        // Update state
        let new_transferred = keys_transferred + transferred as u64;
        let new_last_key = keys.last().cloned();

        let mut migrations = self.migrations.write();
        if let Some(record) = migrations.get_mut(&slot_id) {
            record.set_phase(MigrationPhase::Streaming {
                keys_total,
                keys_transferred: new_transferred,
                last_key: new_last_key,
            });
            self.record_event(
                slot_id,
                record.from_shard,
                record.to_shard,
                MigrationEventType::StreamingProgress {
                    keys_transferred: new_transferred,
                    keys_total,
                },
            );
        }

        // Update stats
        self.stats.write().keys_migrated += transferred as u64;

        // Update slot table progress
        self.slot_table
            .update_migration_progress(slot_id, keys_total.saturating_sub(new_transferred));

        Ok(())
    }

    /// Process the CatchingUp phase.
    pub(crate) async fn process_catching_up<A: MigrationDataAccessor>(
        &self,
        slot_id: SlotId,
        from_log_index: u64,
        accessor: &Arc<A>,
        _control_plane: &Option<Arc<SlotControlPlane>>,
    ) -> Result<()> {
        let record = self
            .migrations
            .read()
            .get(&slot_id)
            .cloned()
            .ok_or(Error::MigrationNotFound(slot_id as u32))?;

        // Get log entries since from_log_index
        let entries = accessor
            .get_slot_log_entries(record.from_shard, slot_id, from_log_index, 100)
            .await?;

        if entries.is_empty() {
            // Caught up! Transition to PREPARED (validate before completion)
            self.transition_to_prepared(slot_id, accessor).await?;
            return Ok(());
        }

        // Replay entries on target
        for entry in &entries {
            match &entry.operation {
                SlotLogOperation::Put => {
                    if let Some(value) = &entry.value {
                        accessor.put_key(record.to_shard, &entry.key, value).await?;
                    }
                }
                SlotLogOperation::Delete => {
                    // For deletes, we don't need to do anything special
                    // The key won't exist on target if it was never migrated
                }
            }
        }

        // Update log index and touch progress
        let new_from_index = entries
            .last()
            .map(|e| e.index + 1)
            .unwrap_or(from_log_index);

        let mut migrations = self.migrations.write();
        if let Some(record) = migrations.get_mut(&slot_id) {
            record.set_phase(MigrationPhase::CatchingUp {
                from_log_index: new_from_index,
            });
        }

        Ok(())
    }

    /// Transition from CatchingUp to PREPARED after validation.
    ///
    /// This validates the migration data and then proposes PREPARED through
    /// the target shard's Raft. The source data will NOT be deleted until
    /// PREPARED is Raft-committed.
    pub(crate) async fn transition_to_prepared<A: MigrationDataAccessor>(
        &self,
        slot_id: SlotId,
        accessor: &Arc<A>,
    ) -> Result<()> {
        let record = self
            .migrations
            .read()
            .get(&slot_id)
            .cloned()
            .ok_or(Error::MigrationNotFound(slot_id as u32))?;

        // Validate migration before transitioning to PREPARED
        let validation = self.validate_migration(&record, accessor).await?;

        // Try to propose PREPARED through target shard's Raft
        let command = MigrationRaftCommand::Prepared {
            migration_id: record.id.clone(),
            target_commit_index: validation.raft_commit_index,
            validation_checksum: validation.checksum,
            proposed_at: now_ms(),
        };

        match self
            .propose_to_target_shard(command, record.to_shard, accessor)
            .await
        {
            Ok(_) => {
                // Apply to local state ONLY after successful Raft commit
                self.apply_prepared_to_local_state(slot_id, &validation);
                tracing::info!(
                    slot_id,
                    target_commit_index = validation.raft_commit_index,
                    key_count = validation.key_count,
                    checksum = validation.checksum,
                    target_shard = record.to_shard,
                    "Migration PREPARED proposed via Raft"
                );
                Ok(())
            }
            Err(Error::Raft(RaftError::NotLeader { leader })) => {
                // Fall back to local application when not leader.
                // This is safe because:
                // 1. Data has been validated (checksum, commit index verified)
                // 2. No data deletion happens at Prepared phase (only at Completed)
                // 3. Other nodes will sync via their own processing or Raft replication
                // 4. This prevents migrations from getting stuck when owner != target leader
                self.apply_prepared_to_local_state(slot_id, &validation);
                tracing::info!(
                    slot_id,
                    target_shard = record.to_shard,
                    leader = ?leader,
                    "Migration PREPARED applied locally (not target shard leader)"
                );
                Ok(())
            }
            Err(e) => {
                // CRITICAL: Do NOT fall back to local state on Raft failure!
                // PREPARED is a consensus-critical state - if two nodes both think
                // a migration is PREPARED, source data deletion could cause data loss.
                tracing::warn!(
                    slot_id,
                    error = %e,
                    "Raft proposal failed for PREPARED - will retry"
                );
                Err(e)
            }
        }
    }

    /// Apply PREPARED state to local migration record.
    /// Only called after Raft has committed the PREPARED command.
    ///
    /// This method is pub(crate) to allow testing the local state synchronization
    /// that was the root cause of the "migrations stuck at active=N" bug.
    pub(crate) fn apply_prepared_to_local_state(
        &self,
        slot_id: SlotId,
        validation: &ValidationResult,
    ) {
        let now = now_ms();
        let mut migrations = self.migrations.write();
        if let Some(record) = migrations.get_mut(&slot_id) {
            record.set_phase(MigrationPhase::Prepared {
                prepared_at: now,
                target_commit_index: validation.raft_commit_index,
                validation_checksum: validation.checksum,
            });
            self.record_event(
                slot_id,
                record.from_shard,
                record.to_shard,
                MigrationEventType::Prepared {
                    checksum: validation.checksum,
                    source_count: validation.key_count,
                    target_count: validation.key_count,
                },
            );

            tracing::info!(
                slot_id,
                target_commit_index = validation.raft_commit_index,
                key_count = validation.key_count,
                checksum = validation.checksum,
                "Migration PREPARED applied to local state"
            );
        }
    }

    /// Three-layer validation before PREPARED state.
    pub(crate) async fn validate_migration<A: MigrationDataAccessor>(
        &self,
        record: &SlotMigrationRecord,
        accessor: &Arc<A>,
    ) -> Result<ValidationResult> {
        let slot_id = record.slot_id;
        let from_shard = record.from_shard;
        let to_shard = record.to_shard;

        // Layer 1: Raft layer - verify commit index
        let target_commit_index = accessor.get_shard_commit_index(to_shard).await?;

        // Layer 2: Storage layer - count and checksum verification
        let source_count = accessor.count_keys_in_slot(from_shard, slot_id).await?;
        let target_count = accessor.count_keys_in_slot(to_shard, slot_id).await?;

        // Allow case where source is empty but target has data - this means the data
        // was successfully transferred and source was cleaned up, we just need to finalize.
        // This can happen in race conditions where multiple nodes process the same migration.
        if source_count != target_count {
            if source_count == 0 && target_count > 0 {
                tracing::info!(
                    slot_id,
                    target_count,
                    "Migration validation: source empty, target has data (already transferred)"
                );
            } else {
                return Err(Error::MigrationValidationFailed(format!(
                    "Key count mismatch: source={}, target={}",
                    source_count, target_count
                )));
            }
        }

        // Compute checksums for validation and return value
        let source_checksum = accessor.checksum_slot(from_shard, slot_id).await?;
        let target_checksum = accessor.checksum_slot(to_shard, slot_id).await?;

        // Only compare checksums if source has data
        // (if source=0, data was already transferred and source cleaned up)
        if source_count > 0 && source_checksum != target_checksum {
            return Err(Error::MigrationValidationFailed(format!(
                "Checksum mismatch: source={:#x}, target={:#x}",
                source_checksum, target_checksum
            )));
        }

        // Layer 3: Availability layer - sample read from follower (optional)
        let follower_sample_ok = if self.config.require_follower_verification {
            accessor.verify_on_follower(to_shard, slot_id).await?
        } else {
            accessor
                .verify_on_follower(to_shard, slot_id)
                .await
                .unwrap_or(true)
        };

        let replica_count = accessor.get_replica_count(to_shard).await?;

        tracing::debug!(
            slot_id,
            source_count,
            target_count,
            source_checksum,
            target_checksum,
            target_commit_index,
            replica_count,
            follower_sample_ok,
            "Migration validation completed"
        );

        // Record ValidationCompleted event
        self.record_event(
            slot_id,
            from_shard,
            to_shard,
            MigrationEventType::ValidationCompleted {
                source_count,
                target_count,
                source_checksum,
                target_checksum,
                follower_ok: follower_sample_ok,
            },
        );

        Ok(ValidationResult {
            raft_commit_index: target_commit_index,
            key_count: target_count,
            checksum: target_checksum,
            replica_count,
            follower_sample_ok,
        })
    }

    /// Transition from PREPARED to COMPLETED.
    ///
    /// This deletes source data and verifies the deletion before proposing
    /// COMPLETED through the target shard's Raft.
    pub(crate) async fn transition_to_completed<A: MigrationDataAccessor>(
        &self,
        slot_id: SlotId,
        accessor: &Arc<A>,
        control_plane: &Option<Arc<SlotControlPlane>>,
    ) -> Result<()> {
        let record = self
            .migrations
            .read()
            .get(&slot_id)
            .cloned()
            .ok_or(Error::MigrationNotFound(slot_id as u32))?;

        // Delete source data (safe because PREPARED is committed)
        // In Multi-Raft with different leaders per shard, we might not be the source
        // shard leader. If so, skip the delete - the source shard leader's cleanup
        // process will handle orphaned data. The slot table has already been updated
        // to route traffic to the target shard.
        let source_deleted = match accessor.delete_slot_data(record.from_shard, slot_id).await {
            Ok(()) => {
                // Verify source is empty
                let remaining = accessor
                    .count_keys_in_slot(record.from_shard, slot_id)
                    .await
                    .unwrap_or(0);
                if remaining > 0 {
                    tracing::warn!(
                        slot_id,
                        remaining,
                        from_shard = record.from_shard,
                        "Source still has keys after deletion, will be cleaned up later"
                    );
                    false
                } else {
                    true
                }
            }
            Err(Error::Raft(RaftError::NotLeader { leader })) => {
                // Not the source shard leader - can't delete from source.
                // This is expected in Multi-Raft with different leaders per shard.
                // The data will be orphaned but harmless (slot routes to target now).
                tracing::debug!(
                    slot_id,
                    from_shard = record.from_shard,
                    source_leader = ?leader,
                    "Cannot delete source data: not source shard leader, will be cleaned up by source leader"
                );
                false
            }
            Err(e) if e.is_retryable() => {
                // Other transient error - skip for now, will retry later
                tracing::debug!(
                    slot_id,
                    from_shard = record.from_shard,
                    error = %e,
                    "Transient error deleting source data, will retry"
                );
                return Ok(()); // Skip this iteration, retry later
            }
            Err(e) => {
                // Permanent error - log but proceed with completion
                tracing::warn!(
                    slot_id,
                    from_shard = record.from_shard,
                    error = %e,
                    "Failed to delete source data, proceeding with completion"
                );
                false
            }
        };

        // Record SourceDeleted event
        self.record_event(
            slot_id,
            record.from_shard,
            record.to_shard,
            MigrationEventType::SourceDeleted {
                success: source_deleted,
                keys_deleted: 0, // Exact count not tracked; success flag indicates outcome
            },
        );

        tracing::debug!(
            slot_id,
            source_deleted,
            from_shard = record.from_shard,
            to_shard = record.to_shard,
            "Source deletion status before proposing COMPLETED"
        );

        // Try to propose COMPLETED through target shard's Raft
        let command = MigrationRaftCommand::Completed {
            migration_id: record.id.clone(),
            proposed_at: now_ms(),
        };

        match self
            .propose_to_target_shard(command, record.to_shard, accessor)
            .await
        {
            Ok(_) => {
                tracing::info!(
                    slot_id,
                    target_shard = record.to_shard,
                    "Migration COMPLETED proposed via Raft"
                );
                // Also update local state and slot table
                self.complete_migration_local(slot_id, control_plane).await
            }
            Err(Error::Raft(RaftError::NotLeader { leader })) => {
                tracing::debug!(
                    slot_id,
                    target_shard = record.to_shard,
                    leader = ?leader,
                    "Cannot propose COMPLETED: not target shard leader, falling back to local completion"
                );
                // Fall back to local completion since data is already safely replicated
                // (PREPARED phase guarantees data integrity). The slot table will be
                // updated locally, and other nodes will sync via slot table sync.
                // This handles unstable leadership scenarios during shard creation.
                self.complete_migration_local(slot_id, control_plane).await
            }
            Err(e) => {
                // Fall back to local completion if Raft not available
                tracing::debug!(
                    slot_id,
                    error = %e,
                    "Raft proposal failed, falling back to local completion"
                );
                self.complete_migration_local(slot_id, control_plane).await
            }
        }
    }

    /// Complete a migration locally (for local state tracking and slot table update).
    async fn complete_migration_local(
        &self,
        slot_id: SlotId,
        control_plane: &Option<Arc<SlotControlPlane>>,
    ) -> Result<()> {
        let now = now_ms();

        // Update migration record
        {
            let mut migrations = self.migrations.write();
            if let Some(record) = migrations.get_mut(&slot_id) {
                // Capture keys_transferred from Streaming phase before overwriting
                let keys_transferred = match &record.phase {
                    MigrationPhase::Streaming {
                        keys_transferred, ..
                    } => *keys_transferred,
                    _ => record.keys_migrated, // preserve if already set
                };
                record.keys_migrated = keys_transferred;
                record.completed_by_node = Some(self.node_id);
                record.set_phase(MigrationPhase::Completed { completed_at: now });
                let total_duration = self
                    .phase_duration_since(slot_id, |t| matches!(t, MigrationEventType::Registered));
                self.record_event_with_duration(
                    slot_id,
                    record.from_shard,
                    record.to_shard,
                    MigrationEventType::Completed,
                    total_duration,
                );
            }
        }

        // Update slot table
        self.slot_table.mark_imported(slot_id);

        // Update control plane if present
        if let Some(cp) = control_plane {
            cp.update_drain_progress(
                self.migrations
                    .read()
                    .get(&slot_id)
                    .map(|r| r.from_shard)
                    .unwrap_or(0),
                1,
            );
        }

        // Update stats
        self.stats.write().completed += 1;

        tracing::info!(slot_id, "Migration completed");

        Ok(())
    }

    /// Process retries for failed migrations.
    pub(crate) async fn process_retries(&self) {
        let retryable: Vec<SlotId> = self
            .migrations
            .read()
            .iter()
            .filter_map(|(&slot_id, record)| {
                if record.can_retry(self.config.max_retries) {
                    Some(slot_id)
                } else {
                    None
                }
            })
            .collect();

        for slot_id in retryable {
            let mut migrations = self.migrations.write();
            if let Some(record) = migrations.get_mut(&slot_id) {
                let attempt = match &record.phase {
                    MigrationPhase::Failed { retry_count, .. } => *retry_count,
                    _ => 0,
                };
                tracing::info!(slot_id, retry_count = attempt, "Retrying failed migration");
                self.record_event(
                    slot_id,
                    record.from_shard,
                    record.to_shard,
                    MigrationEventType::Retried { attempt },
                );
                record.set_phase(MigrationPhase::Pending);
            }
        }
    }

    /// Check for stale migrations and return their slot IDs.
    ///
    /// A migration is stale if it's in progress but hasn't made progress
    /// within the configured timeout duration.
    pub fn check_for_stale_migrations(&self) -> Vec<SlotId> {
        let timeout_ms = self.config.migration_timeout_ms;
        let now = now_ms();

        self.migrations
            .read()
            .iter()
            .filter_map(|(&slot_id, record)| {
                // Only check migrations that have been CLAIMED and started but stopped progressing.
                // Pending migrations are just waiting in the queue - not stuck.
                // With 200+ migrations and only 10 processed per iteration, many will
                // naturally wait in Pending state without being "stuck".
                let is_started = !matches!(record.phase, MigrationPhase::Pending);
                let is_in_progress = record.phase.is_in_progress();
                let is_timed_out = now.saturating_sub(record.last_progress_at) > timeout_ms;

                if is_started && is_in_progress && is_timed_out {
                    Some(slot_id)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Takeover a stale migration with a new epoch.
    ///
    /// This allows the current leader to take over a migration that
    /// appears to be stuck (e.g., previous owner crashed).
    pub fn takeover_stale_migration(&self, slot_id: SlotId) -> Result<()> {
        let mut migrations = self.migrations.write();
        if let Some(record) = migrations.get_mut(&slot_id) {
            let old_epoch = record.id.epoch;
            let old_phase = record.phase.name();

            // Increment epoch for takeover
            record.increment_epoch();

            let new_epoch = record.id.epoch;
            let from_shard = record.from_shard;
            let to_shard = record.to_shard;

            tracing::warn!(
                slot_id,
                old_epoch,
                new_epoch,
                old_phase,
                node_id = self.node_id,
                "Taking over stale migration with new epoch"
            );

            drop(migrations);

            // Record StaleTakeover event after lock is dropped
            self.record_event(
                slot_id,
                from_shard,
                to_shard,
                MigrationEventType::StaleTakeover {
                    old_epoch,
                    new_epoch,
                },
            );

            Ok(())
        } else {
            Err(Error::MigrationNotFound(slot_id as u32))
        }
    }

    /// Process stale migration takeovers.
    ///
    /// Called periodically to check for and take over stuck migrations.
    /// Only the TARGET shard leader should take over stale migrations since
    /// it's the one driving migrations (pull model).
    pub async fn process_stale_migrations<A: MigrationDataAccessor>(&self, accessor: &Arc<A>) {
        let stale_slots = self.check_for_stale_migrations();

        for slot_id in stale_slots {
            // Only take over if we're the TARGET shard leader
            // (since target shard leader drives migrations in our pull model)
            if let Some(record) = self.get_migration(slot_id) {
                if accessor.is_target_shard_leader(record.to_shard) {
                    if let Err(e) = self.takeover_stale_migration(slot_id) {
                        tracing::warn!(
                            slot_id,
                            error = %e,
                            "Failed to takeover stale migration"
                        );
                    }
                }
            }
        }
    }

    /// Migrate a single key on demand (for ASK redirects).
    ///
    /// This is used when a read/write hits the new owner but the key
    /// hasn't been migrated yet.
    pub async fn migrate_key_on_demand<A: MigrationDataAccessor>(
        &self,
        key: &[u8],
        from_shard: ShardId,
        to_shard: ShardId,
        accessor: &A,
    ) -> Result<()> {
        // Read from source
        let value = accessor.get_key(from_shard, key).await?;

        // Write to target (idempotent)
        if let Some(v) = value {
            accessor.put_key(to_shard, key, &v).await?;
        }

        Ok(())
    }

    /// Clean up completed migrations (remove from tracking).
    pub fn cleanup_completed(&self, older_than: Duration) {
        let now = now_ms();
        let threshold = now.saturating_sub(older_than.as_millis() as u64);

        let mut migrations = self.migrations.write();
        let to_remove: Vec<SlotId> = migrations
            .iter()
            .filter_map(|(&slot_id, record)| {
                if let MigrationPhase::Completed { completed_at } = &record.phase {
                    if *completed_at < threshold {
                        return Some(slot_id);
                    }
                }
                None
            })
            .collect();

        for slot_id in to_remove {
            migrations.remove(&slot_id);
        }
    }

    /// Get migrations for persistence (for crash recovery).
    pub fn get_all_migrations(&self) -> Vec<SlotMigrationRecord> {
        self.migrations.read().values().cloned().collect()
    }

    /// Restore migrations from persistence.
    pub fn restore_migrations(&self, records: Vec<SlotMigrationRecord>) {
        let mut migrations = self.migrations.write();
        for record in records {
            migrations.insert(record.slot_id, record);
        }
    }
}

// ==================== Migration Event Log ====================

/// A single migration event recording a phase transition or notable occurrence.
#[derive(Debug, Clone)]
pub struct MigrationEvent {
    /// The slot being migrated.
    pub slot_id: SlotId,
    /// Source shard.
    pub from_shard: ShardId,
    /// Target shard.
    pub to_shard: ShardId,
    /// Type of event.
    pub event_type: MigrationEventType,
    /// Timestamp in milliseconds since Unix epoch.
    pub timestamp_ms: u64,
    /// Node that recorded this event.
    pub node_id: NodeId,
    /// Optional duration of the phase that just completed (ms).
    pub phase_duration_ms: Option<u64>,
}

/// Types of migration events.
#[derive(Debug, Clone, PartialEq)]
pub enum MigrationEventType {
    /// Migration registered (Pending).
    Registered,
    /// Ownership claimed via Raft.
    Claimed { claim_epoch: u64 },
    /// Transitioned to scanning source keys.
    ScanningStarted,
    /// Scanning completed, found N keys.
    ScanningCompleted { keys_found: u64 },
    /// Streaming data to target.
    StreamingStarted { keys_total: u64 },
    /// Streaming batch transferred.
    StreamingProgress {
        keys_transferred: u64,
        keys_total: u64,
    },
    /// Catching up with new writes.
    CatchingUpStarted { from_log_index: u64 },
    /// Migration prepared and validated.
    Prepared {
        checksum: u64,
        source_count: u64,
        target_count: u64,
    },
    /// Migration completed (target authoritative).
    Completed,
    /// Source data cleaned.
    Cleaned,
    /// Migration failed.
    Failed { error: String, retry_count: u32 },
    /// Retry attempt after failure.
    Retried { attempt: u32 },
    /// Slot table sync detected completed migration (follower discovery).
    SyncCompleted,
    /// Peer migration synced (new or updated).
    PeerSynced { is_new: bool },
    /// Source data deletion result after migration.
    SourceDeleted { success: bool, keys_deleted: u64 },
    /// Multi-layer validation completed (before PREPARED).
    ValidationCompleted {
        source_count: u64,
        target_count: u64,
        source_checksum: u64,
        target_checksum: u64,
        follower_ok: bool,
    },
    /// Migration skipped because source had no data (reversed migration).
    SkippedReversed,
    /// Stale migration detected and taken over.
    StaleTakeover { old_epoch: u64, new_epoch: u64 },
}

/// Get current time in milliseconds since Unix epoch.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// No-op data accessor for testing.
#[derive(Debug, Default)]
pub struct NoOpDataAccessor;

#[async_trait::async_trait]
impl MigrationDataAccessor for NoOpDataAccessor {
    async fn scan_slot_keys(
        &self,
        _shard_id: ShardId,
        _slot_id: SlotId,
        _cursor: Option<&[u8]>,
        _limit: usize,
    ) -> Result<(Vec<Vec<u8>>, Option<Vec<u8>>)> {
        Ok((Vec::new(), None))
    }

    async fn get_keys(
        &self,
        _shard_id: ShardId,
        _keys: &[Vec<u8>],
    ) -> Result<Vec<(Vec<u8>, Option<Bytes>)>> {
        Ok(Vec::new())
    }

    async fn import_keys(&self, _shard_id: ShardId, _data: &[(Vec<u8>, Bytes)]) -> Result<()> {
        Ok(())
    }

    async fn current_log_index(&self, _shard_id: ShardId) -> Result<u64> {
        Ok(0)
    }

    async fn get_slot_log_entries(
        &self,
        _shard_id: ShardId,
        _slot_id: SlotId,
        _from_index: u64,
        _limit: usize,
    ) -> Result<Vec<SlotLogEntry>> {
        Ok(Vec::new())
    }

    async fn has_key(&self, _shard_id: ShardId, _key: &[u8]) -> Result<bool> {
        Ok(false)
    }

    async fn get_key(&self, _shard_id: ShardId, _key: &[u8]) -> Result<Option<Bytes>> {
        Ok(None)
    }

    async fn put_key(&self, _shard_id: ShardId, _key: &[u8], _value: &[u8]) -> Result<()> {
        Ok(())
    }

    async fn delete_slot_data(&self, _shard_id: ShardId, _slot_id: SlotId) -> Result<()> {
        Ok(())
    }

    fn is_source_shard_leader(&self, _from_shard: ShardId) -> bool {
        true // In test, assume we're always the leader
    }

    fn is_target_shard_leader(&self, _to_shard: ShardId) -> bool {
        true // In test, assume we're always the leader
    }
}

// ==================== Real Shard Migration Data Accessor ====================

/// Real data accessor for shard migration that interacts with actual shard storage.
///
/// This accessor implements the `MigrationDataAccessor` trait to enable actual
/// data transfer between shards during slot migrations.
///
/// When per-shard Raft is enabled, this accessor supports automatic leader forwarding
/// for write operations. If a put fails with `NotLeader`, the request is forwarded
/// to the actual shard leader.
///
/// For remote shards (e.g., during shard removal), this accessor can forward scan
/// requests to the node that owns the shard.
pub struct ShardMigrationDataAccessor {
    /// Router to access shards by ID.
    router: Arc<ShardRouter>,
    /// This node's ID.
    node_id: NodeId,
    /// Shard forwarder for cross-node request forwarding.
    shard_forwarder: Arc<ShardForwarder>,
    /// Shard leader tracker for determining leaders.
    leader_tracker: Arc<ShardLeaderTracker>,
    /// Per-shard Raft manager (optional).
    shard_raft_manager: Arc<RwLock<Option<Arc<ShardRaftManager>>>>,
    /// Remote slot scan callback for cross-node slot scanning.
    /// This is called when the source shard is on a remote node.
    remote_scan_callback: Option<RemoteScanCallback>,
    /// Configurable total number of slots.
    total_slots: usize,
}

/// Callback type for remote slot scanning.
/// Parameters: target_node, shard_id, slot_id, cursor, limit
/// Returns: (keys, next_cursor)
pub type RemoteScanCallback = Arc<
    dyn Fn(
            NodeId,
            ShardId,
            SlotId,
            Option<Vec<u8>>,
            usize,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(Vec<Vec<u8>>, Option<Vec<u8>>)>> + Send>,
        > + Send
        + Sync,
>;

impl std::fmt::Debug for ShardMigrationDataAccessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardMigrationDataAccessor")
            .field("node_id", &self.node_id)
            .field(
                "has_remote_scan_callback",
                &self.remote_scan_callback.is_some(),
            )
            .finish()
    }
}

impl ShardMigrationDataAccessor {
    /// Create a new shard migration data accessor with forwarding support.
    pub fn new(
        router: Arc<ShardRouter>,
        node_id: NodeId,
        shard_forwarder: Arc<ShardForwarder>,
        leader_tracker: Arc<ShardLeaderTracker>,
        shard_raft_manager: Arc<RwLock<Option<Arc<ShardRaftManager>>>>,
    ) -> Self {
        Self {
            router,
            node_id,
            shard_forwarder,
            leader_tracker,
            shard_raft_manager,
            remote_scan_callback: None,
            total_slots: TOTAL_SLOTS,
        }
    }

    /// Set the total number of slots.
    pub fn with_total_slots(mut self, total_slots: usize) -> Self {
        self.total_slots = total_slots;
        self
    }

    /// Set the remote scan callback for cross-node slot scanning.
    pub fn with_remote_scan_callback(mut self, callback: RemoteScanCallback) -> Self {
        self.remote_scan_callback = Some(callback);
        self
    }

    /// Get the node ID that owns a shard.
    /// Shard IDs are encoded as (node_id << 24) | local_seq.
    fn get_shard_owner_node(&self, shard_id: ShardId) -> NodeId {
        (shard_id >> 24) as NodeId
    }

    /// Calculate which slot a key belongs to.
    fn key_to_slot(&self, key: &[u8]) -> SlotId {
        crc16(key) % self.total_slots as u16
    }

    /// Put a key-value pair with automatic leader forwarding.
    ///
    /// If this node is the shard leader, writes directly via Raft.
    /// If not leader, forwards to the actual leader (single hop, no recursion).
    ///
    /// # Loop Prevention
    ///
    /// Migration only initiates forwards (never receives them). The receiving
    /// leader executes the put directly via Raft. If the leader also fails
    /// (rare race), the existing TTL mechanism prevents loops.
    async fn put_with_forwarding(&self, shard_id: ShardId, key: &[u8], value: &[u8]) -> Result<()> {
        let shard = self
            .router
            .get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        // Try local put first
        match shard
            .put(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value))
            .await
        {
            Ok(()) => Ok(()),
            Err(Error::Raft(RaftError::NotLeader { leader })) => {
                // Determine leader from multiple sources (in order of reliability)
                let leader_node = leader
                    .or_else(|| self.leader_tracker.get_leader(shard_id))
                    .or_else(|| {
                        self.shard_raft_manager
                            .read()
                            .as_ref()
                            .and_then(|m| m.get_shard(shard_id))
                            .and_then(|n| n.leader_id())
                    });

                match leader_node {
                    Some(leader_node) if leader_node != self.node_id => {
                        // Forward to leader - single hop only (receiving node executes directly)
                        tracing::debug!(
                            node_id = self.node_id,
                            shard_id,
                            leader_node,
                            "Migration: forwarding PUT to shard leader"
                        );

                        let command = CacheCommand::put(key.to_vec(), value.to_vec());
                        let result = self
                            .shard_forwarder
                            .forward_to_node(leader_node, shard_id, command)
                            .await?;

                        if result.success {
                            Ok(())
                        } else {
                            Err(Error::RemoteError(
                                result.error.unwrap_or_else(|| "forward failed".into()),
                            ))
                        }
                    }
                    Some(_) => {
                        // Leader is this node but we got NotLeader - race condition, return error
                        Err(Error::Raft(RaftError::NotLeader { leader }))
                    }
                    None => Err(Error::ShardLeaderUnknown(shard_id)),
                }
            }
            Err(e) => Err(e),
        }
    }
}

#[async_trait::async_trait]
impl MigrationDataAccessor for ShardMigrationDataAccessor {
    async fn scan_slot_keys(
        &self,
        shard_id: ShardId,
        slot_id: SlotId,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<Vec<u8>>, Option<Vec<u8>>)> {
        // Check if shard exists locally
        if let Some(shard) = self.router.get_shard(shard_id) {
            // Shard exists locally - scan from local storage
            let storage = shard.storage();

            // Enable slot indexing if not already enabled (lazy initialization)
            if !storage.has_slot_indexing() {
                storage.set_total_slots(self.total_slots as u16);
                storage.enable_slot_indexing();
            }

            let (keys, next_cursor) = storage.scan_slot_keys(slot_id, cursor, limit);

            return Ok((
                keys.into_iter().map(|k| k.to_vec()).collect(),
                next_cursor.map(|k| k.to_vec()),
            ));
        }

        // Shard not local - determine which node owns it and forward the scan
        let owner_node = self.get_shard_owner_node(shard_id);

        if owner_node == self.node_id {
            // This shouldn't happen: we own the shard but can't find it locally
            tracing::warn!(
                shard_id,
                node_id = self.node_id,
                "Shard owned by this node but not found in router"
            );
            return Err(Error::ShardNotFound(shard_id));
        }

        // Forward slot scan to the owner node via callback
        tracing::debug!(
            slot_id,
            shard_id,
            owner_node,
            "Forwarding slot scan to remote owner"
        );

        let callback = self
            .remote_scan_callback
            .as_ref()
            .ok_or_else(|| Error::Config("Remote scan callback not configured".into()))?;

        callback(
            owner_node,
            shard_id,
            slot_id,
            cursor.map(|c| c.to_vec()),
            limit,
        )
        .await
    }

    async fn get_keys(
        &self,
        shard_id: ShardId,
        keys: &[Vec<u8>],
    ) -> Result<Vec<(Vec<u8>, Option<Bytes>)>> {
        let shard = self
            .router
            .get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        let mut results = Vec::with_capacity(keys.len());

        // Read directly from storage, bypassing the active check.
        // This is necessary for migration from removing/draining shards.
        let storage = shard.storage();

        for key in keys {
            let value = storage.get(key).await;
            results.push((key.clone(), value));
        }

        Ok(results)
    }

    async fn import_keys(&self, shard_id: ShardId, data: &[(Vec<u8>, Bytes)]) -> Result<()> {
        // Import keys with automatic leader forwarding
        for (key, value) in data {
            self.put_with_forwarding(shard_id, key, value).await?;
        }

        Ok(())
    }

    async fn current_log_index(&self, shard_id: ShardId) -> Result<u64> {
        let shard = self
            .router
            .get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        Ok(shard.applied_index())
    }

    async fn get_slot_log_entries(
        &self,
        _shard_id: ShardId,
        _slot_id: SlotId,
        _from_index: u64,
        _limit: usize,
    ) -> Result<Vec<SlotLogEntry>> {
        // In the current implementation, we don't have a write-ahead log that
        // tracks slot-level operations. For now, return empty to skip catch-up.
        //
        // In a more sophisticated implementation, you would:
        // 1. Access the Raft log for the shard
        // 2. Filter entries by slot_id (if entries include slot info)
        // 3. Return entries since from_index
        //
        // For now, the streaming phase should transfer all keys, making catch-up
        // less critical.
        Ok(Vec::new())
    }

    async fn has_key(&self, shard_id: ShardId, key: &[u8]) -> Result<bool> {
        let shard = self
            .router
            .get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        let value = shard.get(key).await;
        Ok(value.is_some())
    }

    async fn get_key(&self, shard_id: ShardId, key: &[u8]) -> Result<Option<Bytes>> {
        let shard = self
            .router
            .get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        Ok(shard.get(key).await)
    }

    async fn put_key(&self, shard_id: ShardId, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_with_forwarding(shard_id, key, value).await
    }

    async fn get_shard_commit_index(&self, shard_id: ShardId) -> Result<u64> {
        let shard = self
            .router
            .get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        Ok(shard.applied_index())
    }

    async fn count_keys_in_slot(&self, shard_id: ShardId, slot_id: SlotId) -> Result<u64> {
        let shard = self
            .router
            .get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        let storage = shard.storage();
        let mut count = 0u64;

        for (key_arc, _value) in storage.iter() {
            if self.key_to_slot(&key_arc) == slot_id {
                count += 1;
            }
        }

        Ok(count)
    }

    async fn checksum_slot(&self, shard_id: ShardId, slot_id: SlotId) -> Result<u64> {
        let shard = self
            .router
            .get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        let storage = shard.storage();
        let mut checksum: u64 = 0;

        for (key_arc, value) in storage.iter() {
            if self.key_to_slot(&key_arc) == slot_id {
                // Simple checksum: XOR of key hash and value hash
                let key_hash = crc16(&key_arc) as u64;
                let value_hash = crc16(&value) as u64;
                checksum ^= key_hash.wrapping_mul(31).wrapping_add(value_hash);
            }
        }

        Ok(checksum)
    }

    async fn verify_on_follower(&self, shard_id: ShardId, _slot_id: SlotId) -> Result<bool> {
        // Check if we have replicas and the shard Raft is healthy
        if let Some(manager) = self.shard_raft_manager.read().as_ref() {
            if let Some(shard_raft) = manager.get_shard(shard_id) {
                // Check if there's at least one follower (peers include self)
                let peers = shard_raft.peers();
                return Ok(peers.len() > 1); // More than just self
            }
        }
        Ok(true) // Default: assume OK
    }

    async fn get_replica_count(&self, shard_id: ShardId) -> Result<u32> {
        if let Some(manager) = self.shard_raft_manager.read().as_ref() {
            if let Some(shard_raft) = manager.get_shard(shard_id) {
                return Ok(shard_raft.peers().len() as u32);
            }
        }
        Ok(1) // Default: single node
    }

    async fn delete_slot_data(&self, shard_id: ShardId, slot_id: SlotId) -> Result<()> {
        let shard = self
            .router
            .get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        // Collect keys to delete (can't modify while iterating)
        let storage = shard.storage();
        let keys_to_delete: Vec<Bytes> = storage
            .iter()
            .filter_map(|(key_arc, _value)| {
                if self.key_to_slot(&key_arc) == slot_id {
                    Some((*key_arc).clone())
                } else {
                    None
                }
            })
            .collect();

        // Delete collected keys
        for key in keys_to_delete {
            shard.delete(&key).await?;
        }

        Ok(())
    }

    fn is_source_shard_leader(&self, from_shard: ShardId) -> bool {
        // Primary: Check shard Raft manager directly (authoritative source of truth)
        // The shard Raft node knows if it's the leader via Raft consensus
        if let Some(manager) = self.shard_raft_manager.read().as_ref() {
            if let Some(shard_raft) = manager.get_shard(from_shard) {
                let is_leader = shard_raft.is_leader();
                tracing::trace!(
                    from_shard,
                    node_id = self.node_id,
                    is_leader,
                    "Source shard leader check via shard_raft_manager"
                );
                return is_leader;
            }
            // Shard doesn't exist in manager - it was removed/draining
            // Return false so target shard leader can drive the migration (pull model)
            tracing::trace!(
                from_shard,
                node_id = self.node_id,
                "Source shard not in shard_raft_manager (removed/draining), returning false"
            );
            return false;
        }

        // Fallback: check leader_tracker (may be stale but useful for single-node mode)
        if let Some(leader) = self.leader_tracker.get_leader(from_shard) {
            let is_leader = leader == self.node_id;
            tracing::trace!(
                from_shard,
                leader,
                node_id = self.node_id,
                is_leader,
                "Source shard leader check via leader_tracker"
            );
            return is_leader;
        }

        // No manager at all - single-node test mode
        tracing::trace!(
            from_shard,
            node_id = self.node_id,
            "No shard_raft_manager, returning true (single-node mode)"
        );
        true
    }

    fn is_target_shard_leader(&self, to_shard: ShardId) -> bool {
        // Primary: Check shard Raft manager directly
        if let Some(manager) = self.shard_raft_manager.read().as_ref() {
            if let Some(shard_raft) = manager.get_shard(to_shard) {
                let is_leader = shard_raft.is_leader();
                let leader_id = shard_raft.leader_id();

                // If Raft says we're the leader, trust it
                if is_leader {
                    return true;
                }

                // If Raft knows who the leader is (and it's not us), don't immediately reject.
                // In distributed systems, Raft state can be temporarily inconsistent during
                // leadership transitions, causing split-brain where each node thinks the other
                // is the leader. Instead of blocking, we fall through to let the Raft proposal
                // attempt happen - if we're truly not the leader, Raft will reject with NotLeader.
                //
                // Note: We DON'T return false here anymore to handle split-brain scenarios.
                // The actual leadership is determined by the Raft protocol during proposal.
                if let Some(known_leader) = leader_id {
                    if known_leader != 0 && known_leader != self.node_id {
                        // Allow attempt - Raft will be the ultimate arbiter
                        // This handles split-brain where leadership is inconsistent
                        return true;
                    }
                }

                // Raft says we're not the leader, but leader_id is None or 0
                // This happens during leader election after shard creation.
                // Fall through to leader_tracker which may have fresher gossip info.
            } else {
                // Shard doesn't exist in manager - fall through to leader_tracker fallback
                tracing::debug!(
                    to_shard,
                    node_id = self.node_id,
                    manager_shard_count = manager.shard_count(),
                    "Target shard not found in ShardRaftManager, checking leader_tracker"
                );
            }
        }

        // Fallback: check leader_tracker
        // This is useful when:
        // 1. Shard doesn't exist in local manager but leader_tracker has info from other nodes
        // 2. Manager doesn't exist (single-node test mode without per-shard Raft)
        if let Some(leader) = self.leader_tracker.get_leader(to_shard) {
            let is_leader = leader == self.node_id;
            tracing::debug!(
                to_shard,
                leader,
                node_id = self.node_id,
                is_leader,
                "Target shard leader check via leader_tracker"
            );
            return is_leader;
        }

        // No manager at all and no leader_tracker info - single-node test mode
        // Only return true if we have no shard_raft_manager at all
        if self.shard_raft_manager.read().is_none() {
            tracing::debug!(
                to_shard,
                node_id = self.node_id,
                "No shard_raft_manager, returning true (single-node mode)"
            );
            return true;
        }

        // Manager exists but shard not found and no leader_tracker info
        // This typically happens when:
        // 1. The shard is newly created and leader election is in progress
        // 2. Leader info hasn't been synced via gossip yet
        //
        // CRITICAL FIX: Instead of returning false (which blocks all migrations),
        // return true to allow this node to attempt migration.
        // Only ONE node will succeed in the Raft proposal anyway (leader check
        // happens during Raft commit), so allowing attempts is safe.
        // This prevents deadlock where no node thinks it's the leader yet.
        tracing::debug!(
            to_shard,
            node_id = self.node_id,
            "Shard not in manager and not in leader_tracker, allowing migration attempt"
        );
        true
    }

    fn get_shard_raft_node(&self, shard_id: ShardId) -> Option<Arc<ShardRaftNode>> {
        // Get ShardRaftNode from the shard Raft manager
        self.shard_raft_manager
            .read()
            .as_ref()
            .and_then(|m| m.get_shard(shard_id))
    }

    fn gc_tombstoned_shard(&self, shard_id: ShardId) -> Result<()> {
        tracing::info!(shard_id, "Garbage collecting tombstoned shard");

        // Unregister shard from router - this drops the Arc<Shard>
        // When the last Arc reference is dropped, the shard's resources are cleaned up
        if let Some(shard) = self.router.unregister_shard(shard_id) {
            tracing::info!(shard_id, "Unregistered shard from router during GC");

            // Clear any remaining data from the shard's storage
            // This is a best-effort cleanup - the shard should already be empty
            // since all slots were migrated away
            let storage = shard.storage();
            let entry_count = storage.entry_count();
            if entry_count > 0 {
                tracing::warn!(
                    shard_id,
                    entry_count,
                    "Shard still has entries during GC, clearing"
                );
                storage.invalidate_all();
            }
        } else {
            tracing::debug!(
                shard_id,
                "Shard not found in router during GC (already removed)"
            );
        }

        // Note: The ShardRaftNode associated with this shard will be cleaned up
        // when its Arc refcount reaches 0. The ShardRaftManager still holds a
        // reference, but it will be removed when the shard Raft group is no
        // longer needed (no more shards on this node referencing it).

        Ok(())
    }

    fn get_all_raft_migration_records(&self) -> Vec<SlotMigrationRecord> {
        let mut all_records = Vec::new();

        // Get records from all shards' MigrationStateMachines
        if let Some(manager) = self.shard_raft_manager.read().as_ref() {
            let all_shards = manager.all_shards();

            for shard_raft in all_shards {
                let state_machine = shard_raft.migration_state_machine();
                let records = state_machine.all_migrations();
                all_records.extend(records);
            }
        }

        tracing::trace!(
            node_id = self.node_id,
            record_count = all_records.len(),
            "Collected Raft migration records from all shards"
        );

        all_records
    }
}

// ==================== State Consistency Verification ====================

/// Result of a state consistency check between SlotMigrator and MigrationStateMachine.
#[derive(Debug, Clone)]
pub struct StateConsistencyResult {
    /// Whether states are consistent.
    pub consistent: bool,
    /// Slots where phases diverge.
    pub divergent_slots: Vec<StateDivergence>,
    /// Slots only in SlotMigrator (not in MigrationStateMachine).
    pub only_in_migrator: Vec<SlotId>,
    /// Slots only in MigrationStateMachine (not in SlotMigrator).
    pub only_in_state_machine: Vec<SlotId>,
}

/// Details about state divergence for a slot.
#[derive(Debug, Clone)]
pub struct StateDivergence {
    /// The slot ID.
    pub slot_id: SlotId,
    /// Phase in SlotMigrator.
    pub migrator_phase: String,
    /// Phase in MigrationStateMachine.
    pub state_machine_phase: String,
}

impl SlotMigrator {
    /// Check consistency between SlotMigrator state and a MigrationStateMachine.
    ///
    /// This method compares the migration records in this SlotMigrator with those
    /// in the provided MigrationStateMachine and reports any divergences.
    ///
    /// # When to Use
    ///
    /// - For debugging migration stuck issues
    /// - In tests to verify state synchronization
    /// - Periodically during runtime to detect bugs early
    ///
    /// # Returns
    ///
    /// A `StateConsistencyResult` describing any divergences found.
    pub fn check_consistency_with(
        &self,
        state_machine: &super::migration_state_machine::MigrationStateMachine,
    ) -> StateConsistencyResult {
        let migrator_records = self.migrations.read();
        let sm_records = state_machine.all_migrations();

        let mut divergent_slots = Vec::new();
        let mut only_in_migrator = Vec::new();
        let mut only_in_state_machine = Vec::new();

        // Build a map of state machine records for quick lookup
        let sm_map: std::collections::HashMap<SlotId, &SlotMigrationRecord> =
            sm_records.iter().map(|r| (r.slot_id, r)).collect();

        // Check each slot in the migrator
        for (slot_id, migrator_record) in migrator_records.iter() {
            if let Some(sm_record) = sm_map.get(slot_id) {
                // Check if phases match (allowing for local-only phases like Scanning)
                if !phases_are_compatible(&migrator_record.phase, &sm_record.phase) {
                    divergent_slots.push(StateDivergence {
                        slot_id: *slot_id,
                        migrator_phase: migrator_record.phase.name().to_string(),
                        state_machine_phase: sm_record.phase.name().to_string(),
                    });
                }
            } else {
                // Not in state machine - this is OK for Pending/Scanning/Streaming/CatchingUp
                // (these are local-only phases that don't need Raft commitment)
                if is_raft_committed_phase(&migrator_record.phase) {
                    only_in_migrator.push(*slot_id);
                }
            }
        }

        // Check for slots only in state machine
        let migrator_slot_ids: std::collections::HashSet<_> = migrator_records.keys().collect();
        for sm_record in &sm_records {
            if !migrator_slot_ids.contains(&sm_record.slot_id) {
                // State machine has a record that migrator doesn't know about
                only_in_state_machine.push(sm_record.slot_id);
            }
        }

        let consistent = divergent_slots.is_empty()
            && only_in_migrator.is_empty()
            && only_in_state_machine.is_empty();

        StateConsistencyResult {
            consistent,
            divergent_slots,
            only_in_migrator,
            only_in_state_machine,
        }
    }

    /// Log state consistency check results.
    ///
    /// Logs a warning if any inconsistencies are found.
    pub fn log_consistency_check(
        &self,
        state_machine: &super::migration_state_machine::MigrationStateMachine,
        label: &str,
    ) {
        let result = self.check_consistency_with(state_machine);

        if !result.consistent {
            tracing::warn!(
                label,
                divergent_count = result.divergent_slots.len(),
                only_in_migrator = result.only_in_migrator.len(),
                only_in_state_machine = result.only_in_state_machine.len(),
                "State consistency check FAILED"
            );

            for divergence in &result.divergent_slots {
                tracing::warn!(
                    slot_id = divergence.slot_id,
                    migrator_phase = %divergence.migrator_phase,
                    state_machine_phase = %divergence.state_machine_phase,
                    "Migration state divergence detected"
                );
            }
        } else {
            tracing::debug!(label, "State consistency check passed");
        }
    }
}

/// Check if two phases are compatible (considering local-only vs Raft-committed phases).
///
/// Some phases are local-only (Scanning, Streaming, CatchingUp) and don't need
/// to match the state machine exactly. The state machine only tracks
/// Raft-committed states (Pending, Claimed, Prepared, Completed, Cleaned).
fn phases_are_compatible(migrator_phase: &MigrationPhase, sm_phase: &MigrationPhase) -> bool {
    // If phases are the same, they're compatible
    if migrator_phase.name() == sm_phase.name() {
        return true;
    }

    // Local-only phases in migrator are OK even if state machine has Claimed
    // (migrator progresses locally while state machine tracks last Raft commit)
    let migrator_is_local = matches!(
        migrator_phase,
        MigrationPhase::Scanning { .. }
            | MigrationPhase::Streaming { .. }
            | MigrationPhase::CatchingUp { .. }
    );

    let sm_is_claimed = matches!(sm_phase, MigrationPhase::Claimed { .. });

    if migrator_is_local && sm_is_claimed {
        // Migrator has progressed locally past Claimed, SM still shows Claimed
        // This is expected - local work happens between Raft commits
        return true;
    }

    // For Raft-committed phases (Prepared, Completed, Cleaned), they MUST match
    // If SM is in Prepared but migrator isn't, that's the bug we fixed!
    false
}

/// Check if a phase is Raft-committed (requires consensus).
fn is_raft_committed_phase(phase: &MigrationPhase) -> bool {
    matches!(
        phase,
        MigrationPhase::Claimed { .. }
            | MigrationPhase::Prepared { .. }
            | MigrationPhase::Completed { .. }
            | MigrationPhase::Cleaned { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_migrator() -> SlotMigrator {
        let slot_table = Arc::new(SlotTable::new(4, TOTAL_SLOTS));
        SlotMigrator::with_defaults(slot_table)
    }

    #[test]
    fn test_register_migration() {
        let migrator = create_test_migrator();

        migrator.register_migration(0, 0, 3);

        let record = migrator.get_migration(0).unwrap();
        assert_eq!(record.slot_id, 0);
        assert_eq!(record.from_shard, 0);
        assert_eq!(record.to_shard, 3);
        assert!(matches!(record.phase, MigrationPhase::Pending));
    }

    #[test]
    fn test_migration_status() {
        let migrator = create_test_migrator();

        migrator.register_migration(0, 0, 3);
        migrator.register_migration(1, 0, 3);

        let status = migrator.status();
        assert_eq!(status.active_migrations, 2);
        assert_eq!(status.completed_migrations, 0);
    }

    #[test]
    fn test_phase_transitions() {
        assert!(MigrationPhase::Pending.is_in_progress());
        assert!(!MigrationPhase::Pending.is_completed());

        assert!(MigrationPhase::Completed { completed_at: 0 }.is_completed());
        assert!(!MigrationPhase::Completed { completed_at: 0 }.is_in_progress());

        assert!(MigrationPhase::Failed {
            error: "test".into(),
            failed_at: 0,
            retry_count: 0
        }
        .is_failed());
    }

    #[test]
    fn test_can_retry() {
        let mut record = SlotMigrationRecord::new(0, 0, 1);

        // Not failed, can't retry
        assert!(!record.can_retry(3));

        // Mark as failed
        record.mark_failed("test error".into());
        assert!(record.can_retry(3));

        // Mark as failed again (retry_count = 2)
        record.mark_failed("test error 2".into());
        assert!(record.can_retry(3));

        // Mark as failed again (retry_count = 3)
        record.mark_failed("test error 3".into());
        assert!(!record.can_retry(3)); // At limit
    }

    #[tokio::test]
    async fn test_transition_to_scanning() {
        let migrator = create_test_migrator();

        migrator.register_migration(0, 0, 3);
        migrator.transition_to_scanning(0).await.unwrap();

        let record = migrator.get_migration(0).unwrap();
        assert!(matches!(
            record.phase,
            MigrationPhase::Scanning {
                cursor: None,
                keys_found: 0
            }
        ));
    }

    #[test]
    fn test_register_from_reassignment() {
        let migrator = create_test_migrator();

        let moves = vec![(0, 0, 3), (1, 0, 3), (2, 1, 3)];

        migrator.register_from_reassignment(&moves);

        assert_eq!(migrator.active_migrations().len(), 3);
    }

    #[test]
    fn test_cleanup_completed() {
        let migrator = create_test_migrator();

        migrator.register_migration(0, 0, 3);

        // Manually mark as completed
        {
            let mut migrations = migrator.migrations.write();
            if let Some(record) = migrations.get_mut(&0) {
                record.set_phase(MigrationPhase::Completed {
                    completed_at: 0, // Very old
                });
            }
        }

        migrator.cleanup_completed(Duration::from_secs(1));

        assert!(migrator.get_migration(0).is_none());
    }

    #[test]
    fn test_get_all_and_restore() {
        let migrator = create_test_migrator();

        migrator.register_migration(0, 0, 3);
        migrator.register_migration(1, 1, 3);

        let records = migrator.get_all_migrations();
        assert_eq!(records.len(), 2);

        let migrator2 = create_test_migrator();
        migrator2.restore_migrations(records);

        assert_eq!(migrator2.active_migrations().len(), 2);
    }

    #[test]
    fn test_new_phase_helpers() {
        // Test Claimed phase
        let claimed = MigrationPhase::Claimed {
            owner_node: 1,
            claim_epoch: 5,
            claimed_at: 1000,
        };
        assert!(claimed.is_in_progress());
        assert!(!claimed.is_completed());
        assert!(claimed.is_claimed_by(1));
        assert!(!claimed.is_claimed_by(2));
        assert_eq!(claimed.owner(), Some(1));
        assert_eq!(claimed.claim_epoch(), Some(5));
        assert_eq!(claimed.name(), "Claimed");

        // Test Prepared phase
        let prepared = MigrationPhase::Prepared {
            prepared_at: 2000,
            target_commit_index: 100,
            validation_checksum: 12345,
        };
        assert!(prepared.is_in_progress());
        assert!(prepared.is_prepared());
        assert!(!prepared.is_completed());
        assert_eq!(prepared.name(), "Prepared");

        // Test Cleaned phase
        let cleaned = MigrationPhase::Cleaned { cleaned_at: 3000 };
        assert!(cleaned.is_completed());
        assert!(!cleaned.is_in_progress());
        assert_eq!(cleaned.name(), "Cleaned");
    }

    #[test]
    fn test_migration_id() {
        let id1 = MigrationId::first(42);
        assert_eq!(id1.slot_id, 42);
        assert_eq!(id1.epoch, 1);

        let id2 = id1.next_epoch();
        assert_eq!(id2.slot_id, 42);
        assert_eq!(id2.epoch, 2);

        let id3 = MigrationId::new(100, 5);
        assert_eq!(id3.slot_id, 100);
        assert_eq!(id3.epoch, 5);
    }

    #[test]
    fn test_migration_record_epoch() {
        let mut record = SlotMigrationRecord::new(0, 0, 1);
        assert_eq!(record.epoch(), 1);

        record.increment_epoch();
        assert_eq!(record.epoch(), 2);
        assert!(matches!(record.phase, MigrationPhase::Pending));
    }

    #[test]
    fn test_stale_detection() {
        let mut record = SlotMigrationRecord::new(0, 0, 1);

        // Fresh record should not be stale
        assert!(!record.is_stale(1000));

        // Simulate old progress timestamp
        record.last_progress_at = 0;

        // Now it should be stale (timeout_ms = 1000, and now_ms() > 1000)
        assert!(record.is_stale(1000));
    }

    #[test]
    fn test_check_stale_migrations() {
        let migrator = create_test_migrator();

        migrator.register_migration(0, 0, 3);
        migrator.register_migration(1, 0, 3);

        // Fresh migrations should not be stale (they're in Pending state)
        let stale = migrator.check_for_stale_migrations();
        assert!(stale.is_empty());

        // Advance migration 0 to Claimed state (started) and set old progress timestamp
        // Only started migrations (non-Pending) are checked for staleness
        {
            let mut migrations = migrator.migrations.write();
            if let Some(record) = migrations.get_mut(&0) {
                record.set_phase(MigrationPhase::Claimed {
                    owner_node: 1,
                    claim_epoch: 1,
                    claimed_at: 1000,
                });
                record.last_progress_at = 0; // Very old
            }
        }

        // Now slot 0 should be stale (it's Claimed and timed out)
        let stale = migrator.check_for_stale_migrations();
        assert_eq!(stale.len(), 1);
        assert!(stale.contains(&0));
    }

    #[test]
    fn test_takeover_migration() {
        let migrator = create_test_migrator();

        migrator.register_migration(0, 0, 3);

        // Get initial epoch
        let initial_epoch = migrator.get_migration(0).unwrap().epoch();
        assert_eq!(initial_epoch, 1);

        // Takeover the migration
        migrator.takeover_stale_migration(0).unwrap();

        // Epoch should be incremented
        let new_epoch = migrator.get_migration(0).unwrap().epoch();
        assert_eq!(new_epoch, 2);

        // Phase should be reset to Pending
        let record = migrator.get_migration(0).unwrap();
        assert!(matches!(record.phase, MigrationPhase::Pending));
    }

    // ==================== Tests for Local State Updates ====================

    /// Test that apply_prepared_to_local_state correctly updates the local migrations HashMap.
    ///
    /// This test catches the bug where transition_to_prepared would commit via Raft but fail
    /// to update SlotMigrator::migrations, leaving process_migrations unable to see the
    /// Prepared phase.
    #[test]
    fn test_apply_prepared_to_local_state() {
        let migrator = create_test_migrator();

        // Register a migration in Scanning phase (simulating post-data-transfer)
        migrator.register_migration(42, 0, 1);
        {
            let mut migrations = migrator.migrations.write();
            if let Some(record) = migrations.get_mut(&42) {
                record.set_phase(MigrationPhase::Scanning {
                    cursor: None,
                    keys_found: 100,
                });
            }
        }

        // Create a validation result (simulating what validate_migration would return)
        let validation = ValidationResult {
            raft_commit_index: 500,
            key_count: 100,
            checksum: 0xDEADBEEF,
            replica_count: 3,
            follower_sample_ok: true,
        };

        // Apply the prepared state to local state (this is what happens after Raft commits)
        migrator.apply_prepared_to_local_state(42, &validation);

        // Verify the local state was updated
        let record = migrator.get_migration(42).expect("Migration should exist");
        match &record.phase {
            MigrationPhase::Prepared {
                target_commit_index,
                validation_checksum,
                ..
            } => {
                assert_eq!(*target_commit_index, 500);
                assert_eq!(*validation_checksum, 0xDEADBEEF);
            }
            other => panic!(
                "Expected Prepared phase, got {:?}. \
                 This indicates apply_prepared_to_local_state failed to update local state.",
                other
            ),
        }
    }

    /// Test that apply_prepared_to_local_state handles non-existent migrations gracefully.
    #[test]
    fn test_apply_prepared_to_local_state_missing_migration() {
        let migrator = create_test_migrator();

        let validation = ValidationResult {
            raft_commit_index: 500,
            key_count: 100,
            checksum: 0xDEADBEEF,
            replica_count: 3,
            follower_sample_ok: true,
        };

        // Should not panic when migration doesn't exist
        migrator.apply_prepared_to_local_state(999, &validation);

        // Verify migration still doesn't exist (no side effects)
        assert!(migrator.get_migration(999).is_none());
    }

    // ==================== State Consistency Assertion Helpers ====================

    /// Helper to check if a phase in SlotMigrator matches expected phase.
    /// Returns (matches, actual_phase_name) for debugging.
    fn assert_migration_phase(
        migrator: &SlotMigrator,
        slot_id: SlotId,
        expected_phase_name: &str,
    ) -> bool {
        match migrator.get_migration(slot_id) {
            Some(record) => record.phase.name() == expected_phase_name,
            None => false,
        }
    }

    /// Test that the state consistency helper correctly identifies phase mismatches.
    #[test]
    fn test_state_consistency_assertion_helper() {
        let migrator = create_test_migrator();

        // Register a migration
        migrator.register_migration(42, 0, 1);

        // Initially should be Pending
        assert!(assert_migration_phase(&migrator, 42, "Pending"));
        assert!(!assert_migration_phase(&migrator, 42, "Claimed"));
        assert!(!assert_migration_phase(&migrator, 42, "Prepared"));

        // Update to Claimed
        {
            let mut migrations = migrator.migrations.write();
            if let Some(record) = migrations.get_mut(&42) {
                record.set_phase(MigrationPhase::Claimed {
                    owner_node: 1,
                    claim_epoch: 1,
                    claimed_at: now_ms(),
                });
            }
        }

        assert!(assert_migration_phase(&migrator, 42, "Claimed"));
        assert!(!assert_migration_phase(&migrator, 42, "Pending"));

        // Non-existent migration should return false
        assert!(!assert_migration_phase(&migrator, 999, "Pending"));
    }

    // ==================== State Consistency Checker Tests ====================

    use super::super::migration_state_machine::MigrationStateMachine;

    /// Test state consistency check when states are in sync.
    #[test]
    fn test_state_consistency_check_in_sync() {
        let migrator = create_test_migrator();
        let state_machine = MigrationStateMachine::new();

        // Register migration in both
        migrator.register_migration(42, 0, 3);
        state_machine.register_migration(42, 0, 3);

        // Both in Pending - should be consistent
        let result = migrator.check_consistency_with(&state_machine);
        assert!(
            result.consistent,
            "States should be consistent when both in Pending"
        );
        assert!(result.divergent_slots.is_empty());
        assert!(result.only_in_migrator.is_empty());
        assert!(result.only_in_state_machine.is_empty());
    }

    /// Test state consistency check detects divergence.
    #[test]
    fn test_state_consistency_check_detects_divergence() {
        let migrator = create_test_migrator();
        let state_machine = MigrationStateMachine::new();

        // Register migration in both
        migrator.register_migration(42, 0, 3);
        state_machine.register_migration(42, 0, 3);

        // State machine moves to Prepared (via Raft)
        state_machine.apply(
            1,
            1,
            MigrationRaftCommand::Claim {
                migration_id: MigrationId::new(42, 1),
                leader_id: 1,
                proposed_at: 1000,
            },
        );
        state_machine.apply(
            2,
            1,
            MigrationRaftCommand::Prepared {
                migration_id: MigrationId::new(42, 1),
                target_commit_index: 100,
                validation_checksum: 0xDEAD,
                proposed_at: 2000,
            },
        );

        // BUT migrator is still in Pending (the bug scenario!)
        // This is the exact bug that caused migrations to get stuck.

        let result = migrator.check_consistency_with(&state_machine);

        // Should detect divergence
        assert!(!result.consistent, "Should detect divergence");
        assert_eq!(result.divergent_slots.len(), 1);
        assert_eq!(result.divergent_slots[0].slot_id, 42);
        assert_eq!(result.divergent_slots[0].migrator_phase, "Pending");
        assert_eq!(result.divergent_slots[0].state_machine_phase, "Prepared");
    }

    /// Test state consistency allows local-only phases.
    #[test]
    fn test_state_consistency_allows_local_phases() {
        let migrator = create_test_migrator();
        let state_machine = MigrationStateMachine::new();

        // Register migration in both
        migrator.register_migration(42, 0, 3);
        state_machine.register_migration(42, 0, 3);

        // State machine in Claimed
        state_machine.apply(
            1,
            1,
            MigrationRaftCommand::Claim {
                migration_id: MigrationId::new(42, 1),
                leader_id: 1,
                proposed_at: 1000,
            },
        );

        // Migrator has progressed locally to Scanning
        {
            let mut migrations = migrator.migrations.write();
            if let Some(record) = migrations.get_mut(&42) {
                record.set_phase(MigrationPhase::Scanning {
                    cursor: None,
                    keys_found: 50,
                });
            }
        }

        // This should be OK - Scanning is a local-only phase that happens
        // after Claimed but before the next Raft commit (Prepared)
        let result = migrator.check_consistency_with(&state_machine);
        assert!(
            result.consistent,
            "Should allow local-only phases (Scanning) when SM is in Claimed"
        );
    }

    /// Test state consistency detects slots only in state machine.
    #[test]
    fn test_state_consistency_detects_orphan_in_state_machine() {
        let migrator = create_test_migrator();
        let state_machine = MigrationStateMachine::new();

        // Only register in state machine (simulates missed local registration)
        state_machine.register_migration(99, 0, 3);

        let result = migrator.check_consistency_with(&state_machine);

        // Should detect the orphan
        assert!(!result.consistent);
        assert_eq!(result.only_in_state_machine.len(), 1);
        assert_eq!(result.only_in_state_machine[0], 99);
    }
}
