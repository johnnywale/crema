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
    },

    /// Mark migration as completed.
    /// Source data has been deleted and verified empty.
    Completed {
        /// Unique migration identifier.
        migration_id: MigrationId,
    },

    /// Mark source as cleaned (optional).
    Cleaned {
        /// Unique migration identifier.
        migration_id: MigrationId,
    },
}

impl MigrationRaftCommand {
    /// Get the migration ID from this command.
    pub fn migration_id(&self) -> &MigrationId {
        match self {
            MigrationRaftCommand::Claim { migration_id, .. } => migration_id,
            MigrationRaftCommand::Prepared { migration_id, .. } => migration_id,
            MigrationRaftCommand::Completed { migration_id } => migration_id,
            MigrationRaftCommand::Cleaned { migration_id } => migration_id,
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
    /// Value (for puts) - stored as Vec<u8> for serialization.
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

    /// Whether the migrator is running.
    running: std::sync::atomic::AtomicBool,
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
            running: std::sync::atomic::AtomicBool::new(false),
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

    /// Register a new migration.
    pub fn register_migration(&self, slot_id: SlotId, from_shard: ShardId, to_shard: ShardId) {
        let record = SlotMigrationRecord::new(slot_id, from_shard, to_shard);
        self.migrations.write().insert(slot_id, record);

        eprintln!(
            "[MIG-REGISTER] slot={} from={} to={} (direct)",
            slot_id, from_shard, to_shard
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
        use std::collections::hash_map::Entry;

        let slot_table = self.slot_table.clone();
        let snapshot = slot_table.snapshot();

        // Collect slots that need migrations
        let mut slots_to_migrate = Vec::new();

        for (slot_id, assignment) in snapshot.slots.iter().enumerate() {
            let slot_id = slot_id as SlotId;
            // Check if slot is migrating
            if let SlotState::Migrating { from, .. } = &assignment.state {
                slots_to_migrate.push((slot_id, *from, assignment.owner));
            }
        }

        if slots_to_migrate.is_empty() {
            return;
        }

        // Use entry API to prevent race conditions where we overwrite
        // a record that was just updated (e.g., to Claimed) by another thread
        let mut migrations = self.migrations.write();
        let mut inserted_count = 0;

        for (slot_id, from, to) in slots_to_migrate {
            if let Entry::Vacant(entry) = migrations.entry(slot_id) {
                eprintln!(
                    "[MIG-REGISTER] slot={} from={} to={} (from slot_table sync)",
                    slot_id, from, to
                );
                let record = SlotMigrationRecord::new(slot_id, from, to);
                entry.insert(record);
                inserted_count += 1;
            }
        }

        if inserted_count > 0 {
            tracing::info!(
                count = inserted_count,
                "Auto-registered migrations from slot table state"
            );
        }
    }

    /// Sync migrations by detecting active migrations from peers.
    ///
    /// This is a secondary sync mechanism that allows nodes to pick up
    /// migrations that were registered on other nodes but not locally
    /// (e.g., when shard removal was initiated on a different node).
    pub fn sync_from_peer_migrations(&self, peer_migrations: &[SlotMigrationRecord]) {
        eprintln!(
            "[MIG-SYNC] node={} received {} peer migrations",
            self.node_id,
            peer_migrations.len()
        );

        let mut new_migrations = Vec::new();
        let mut updated_migrations = Vec::new();

        for record in peer_migrations {
            let migrations = self.migrations.read();
            if let Some(existing) = migrations.get(&record.slot_id) {
                // Update phase if peer has a more advanced phase
                // (Claimed > Pending, Scanning > Claimed, etc.)
                if record.phase.is_more_advanced_than(&existing.phase) {
                    updated_migrations.push(record.clone());
                }
            } else if record.phase.is_in_progress() {
                // New migration - copy from peer (preserving phase)
                new_migrations.push(record.clone());
            }
        }

        if !new_migrations.is_empty() || !updated_migrations.is_empty() {
            eprintln!(
                "[MIG-SYNC] node={} adding {} new, updating {} migrations",
                self.node_id,
                new_migrations.len(),
                updated_migrations.len()
            );
            let mut migrations = self.migrations.write();

            // Add new migrations (preserving peer's phase)
            for record in new_migrations {
                migrations.insert(record.slot_id, record);
            }

            // Update existing migrations with more advanced phases
            for record in updated_migrations {
                migrations.insert(record.slot_id, record);
            }

            tracing::info!(
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

    /// Process one iteration of migrations.
    async fn process_migrations<A: MigrationDataAccessor>(
        &self,
        accessor: &Arc<A>,
        control_plane: &Option<Arc<SlotControlPlane>>,
    ) -> Result<()> {
        // Sync migrations from slot table state to ensure all nodes know about
        // pending migrations (important when shard removal was triggered on another node)
        self.sync_from_slot_table();

        // Get migrations that need processing
        let pending: Vec<SlotMigrationRecord> = self
            .migrations
            .read()
            .values()
            .filter(|r| r.phase.is_in_progress())
            .cloned()
            .collect();

        // Debug: Log process_migrations iteration
        static PROCESS_LOG_COUNT: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let count = PROCESS_LOG_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Count phases in the collected records
        let pending_count = pending.iter().filter(|r| matches!(r.phase, MigrationPhase::Pending)).count();
        let claimed_count = pending.iter().filter(|r| matches!(r.phase, MigrationPhase::Claimed { .. })).count();
        let scanning_count = pending.iter().filter(|r| matches!(r.phase, MigrationPhase::Scanning { .. })).count();

        // Log more frequently when there are pending migrations
        if count < 30 || (!pending.is_empty() && count % 50 == 0) {
            eprintln!(
                "[MIG-PROCESS] #{} node={} total={} (P={} C={} S={}) map={}",
                count,
                self.node_id,
                pending.len(),
                pending_count,
                claimed_count,
                scanning_count,
                self.migrations.read().len()
            );
        }

        for record in pending {
            if let Err(e) = self
                .advance_migration(record.slot_id, accessor, control_plane)
                .await
            {
                tracing::warn!(
                    slot_id = record.slot_id,
                    phase = record.phase.name(),
                    error = %e,
                    "Failed to advance migration"
                );

                // Mark as failed
                if let Some(record) = self.migrations.write().get_mut(&record.slot_id) {
                    record.mark_failed(e.to_string());
                }
            }
        }

        // Handle retries for failed migrations
        self.process_retries().await;

        // Check for and takeover stale migrations
        self.process_stale_migrations(accessor).await;

        Ok(())
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
    async fn advance_migration<A: MigrationDataAccessor>(
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

        // Debug: Log entry into advance_migration for non-Pending phases
        if !matches!(record.phase, MigrationPhase::Pending) {
            static ADVANCE_LOG_COUNT: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let count = ADVANCE_LOG_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count < 20 || count % 100 == 0 {
                eprintln!(
                    "[MIG-ADVANCE] #{} slot={} phase={} node={}",
                    count, slot_id, record.phase.name(), self.node_id
                );
            }
        }

        // Determine who should drive this migration:
        // Target shard leader ALWAYS drives migration (pull model) because:
        // 1. Migration commands must go through target shard's Raft
        // 2. Only target shard leader can propose to target shard's Raft
        // 3. This ensures proper coordination even when source/target leaders differ
        let is_source_leader = accessor.is_source_shard_leader(record.from_shard);
        let is_target_leader = accessor.is_target_shard_leader(record.to_shard);

        // Only target shard leader drives migration
        // Source shard leader info is logged for debugging but not used for driving
        let should_drive = is_target_leader;

        // Debug: Log should_drive decision for all phases
        static SHOULD_DRIVE_LOG_COUNT: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let sd_count = SHOULD_DRIVE_LOG_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if sd_count < 30 || sd_count % 500 == 0 {
            eprintln!(
                "[MIG-DRIVE] #{} slot={} node={} from={} to={} src_leader={} tgt_leader={} should_drive={} phase={}",
                sd_count, slot_id, self.node_id, record.from_shard, record.to_shard,
                is_source_leader, is_target_leader, should_drive, record.phase.name()
            );
        }

        if !should_drive {
            // Periodically log why no node is driving (for debugging stuck migrations)
            static SKIP_LOG_COUNT: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let count = SKIP_LOG_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count < 5 || count % 1000 == 0 {
                tracing::warn!(
                    slot_id,
                    node_id = self.node_id,
                    from_shard = record.from_shard,
                    to_shard = record.to_shard,
                    is_source_leader,
                    is_target_leader,
                    "Migration skip count {}: not responsible for driving",
                    count
                );
            }
            tracing::trace!(
                slot_id,
                node_id = self.node_id,
                from_shard = record.from_shard,
                to_shard = record.to_shard,
                is_source_leader,
                is_target_leader,
                "Skipping migration: not responsible for driving"
            );
            return Ok(());
        }

        match &record.phase {
            MigrationPhase::Pending => {
                // Claim ownership before proceeding (via Raft)
                static CLAIM_LOG_COUNT: std::sync::atomic::AtomicU64 =
                    std::sync::atomic::AtomicU64::new(0);
                let claim_count = CLAIM_LOG_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if claim_count < 20 || claim_count % 100 == 0 {
                    eprintln!(
                        "[MIG-CLAIM] #{} slot={} node={} from={} to={} src={} tgt={}",
                        claim_count, slot_id, self.node_id, record.from_shard, record.to_shard,
                        is_source_leader, is_target_leader
                    );
                }
                self.claim_migration(slot_id, &record, accessor).await?;
            }
            MigrationPhase::Claimed {
                owner_node,
                claim_epoch,
                ..
            } => {
                eprintln!(
                    "[MIG-DEBUG] Claimed: slot={} owner={} claim_epoch={} self={} rec_epoch={} owner_match={} epoch_match={}",
                    slot_id, owner_node, claim_epoch, self.node_id, record.id.epoch,
                    *owner_node == self.node_id, *claim_epoch == record.id.epoch
                );
                // With Raft-based coordination, the target shard leader drives all phases.
                // Since we already passed should_drive = is_target_leader check, we ARE
                // the target shard leader and can proceed to Scanning.
                // The epoch check ensures we don't process stale migrations from old epochs.
                if *claim_epoch != record.id.epoch {
                    eprintln!(
                        "[MIG-DEBUG] Skipping: slot={} epoch mismatch (claim_epoch={}, rec_epoch={})",
                        slot_id, claim_epoch, record.id.epoch
                    );
                    return Ok(());
                }
                eprintln!("[MIG-DEBUG] Transitioning slot={} Claimed → Scanning (target shard leader={})", slot_id, self.node_id);
                self.transition_to_scanning(slot_id).await?;
            }
            MigrationPhase::Scanning { cursor, keys_found } => {
                eprintln!(
                    "[MIG-DEBUG] Scanning: slot={} keys_found={} cursor={:?}",
                    slot_id, keys_found, cursor.as_ref().map(|c| c.len())
                );
                if !self.is_valid_owner(&record) {
                    eprintln!("[MIG-DEBUG] Scanning skipped: not valid owner for slot={}", slot_id);
                    return Ok(());
                }
                eprintln!("[MIG-DEBUG] Processing scanning for slot={}", slot_id);
                self.process_scanning(slot_id, cursor.clone(), *keys_found, accessor)
                    .await?;
                eprintln!("[MIG-DEBUG] Scanning processed for slot={}", slot_id);
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
    async fn claim_migration<A: MigrationDataAccessor>(
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
        };

        match self
            .propose_to_target_shard(command, record.to_shard, accessor)
            .await
        {
            Ok(_) => {
                // Update local state after successful Raft proposal
                // The Raft command is committed, but we also need to update the
                // SlotMigrator's local migrations HashMap for the migration loop
                eprintln!(
                    "[MIG-CLAIM-OK] slot={} node={} to_shard={}: Raft proposal succeeded",
                    slot_id, self.node_id, record.to_shard
                );
                eprintln!("[MIG-CLAIM-OK2] About to call claim_migration_local for slot={}", slot_id);
                self.claim_migration_local(slot_id)?;
                eprintln!("[MIG-CLAIM-OK3] claim_migration_local completed for slot={}", slot_id);
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
                static NOT_LEADER_LOG: std::sync::atomic::AtomicU64 =
                    std::sync::atomic::AtomicU64::new(0);
                let nl_count = NOT_LEADER_LOG.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if nl_count < 10 || nl_count % 100 == 0 {
                    eprintln!(
                        "[MIG-CLAIM-NOTLEADER] #{} slot={} node={} to_shard={} leader={:?}",
                        nl_count, slot_id, self.node_id, record.to_shard, leader
                    );
                }
                Ok(()) // Not an error, just skip this iteration
            }
            Err(e) => {
                // Fall back to local claim if Raft not available
                eprintln!(
                    "[MIG-CLAIM-FALLBACK] slot={} node={} to_shard={} error={}",
                    slot_id, self.node_id, record.to_shard, e
                );
                self.claim_migration_local(slot_id)
            }
        }
    }

    /// Claim migration locally (fallback when Raft is not available).
    fn claim_migration_local(&self, slot_id: SlotId) -> Result<()> {
        eprintln!("[MIG-LOCAL-CLAIM-START] slot={} node={}", slot_id, self.node_id);
        let now = now_ms();
        let mut migrations = self.migrations.write();
        eprintln!("[MIG-LOCAL-CLAIM-LOCK] slot={} map_len={}", slot_id, migrations.len());
        if let Some(record) = migrations.get_mut(&slot_id) {
            let old_phase = record.phase.name();
            record.set_phase(MigrationPhase::Claimed {
                owner_node: self.node_id,
                claim_epoch: record.id.epoch,
                claimed_at: now,
            });

            eprintln!(
                "[MIG-LOCAL-CLAIM] slot={} node={} {} → Claimed",
                slot_id, self.node_id, old_phase
            );
        } else {
            eprintln!(
                "[MIG-LOCAL-CLAIM-NOTFOUND] slot={} node={}",
                slot_id, self.node_id
            );
        }
        eprintln!("[MIG-LOCAL-CLAIM-END] slot={}", slot_id);
        Ok(())
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
    async fn transition_to_scanning(&self, slot_id: SlotId) -> Result<()> {
        let mut migrations = self.migrations.write();
        if let Some(record) = migrations.get_mut(&slot_id) {
            let old_phase = record.phase.name();
            record.set_phase(MigrationPhase::Scanning {
                cursor: None,
                keys_found: 0,
            });
            eprintln!(
                "[MIG-DEBUG] transition_to_scanning: slot={} {} → Scanning",
                slot_id, old_phase
            );
            tracing::debug!(slot_id, "Migration: {} → Scanning", old_phase);
        } else {
            eprintln!(
                "[MIG-DEBUG] transition_to_scanning: slot={} NOT FOUND in migrations",
                slot_id
            );
        }
        Ok(())
    }

    /// Process the Scanning phase.
    async fn process_scanning<A: MigrationDataAccessor>(
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

        eprintln!(
            "[MIG-SCAN] slot={} from_shard={} cursor={:?} batch_size={}",
            slot_id,
            record.from_shard,
            cursor.as_ref().map(|c| c.len()),
            self.config.scan_batch_size
        );

        // Scan a batch of keys
        let scan_result = accessor
            .scan_slot_keys(
                record.from_shard,
                slot_id,
                cursor.as_deref(),
                self.config.scan_batch_size,
            )
            .await;

        let (keys, next_cursor) = match scan_result {
            Ok((keys, cursor)) => {
                eprintln!(
                    "[MIG-SCAN-OK] slot={} keys_found={} next_cursor={:?}",
                    slot_id,
                    keys.len(),
                    cursor.as_ref().map(|c| c.len())
                );
                (keys, cursor)
            }
            Err(e) => {
                eprintln!(
                    "[MIG-SCAN-ERR] slot={} error={}",
                    slot_id, e
                );
                return Err(e);
            }
        };

        let new_keys_found = keys_found + keys.len() as u64;

        let mut migrations = self.migrations.write();
        if let Some(record) = migrations.get_mut(&slot_id) {
            if keys.is_empty() && next_cursor.is_none() {
                // Scanning complete, transition to Streaming
                record.set_phase(MigrationPhase::Streaming {
                    keys_total: new_keys_found,
                    keys_transferred: 0,
                    last_key: None,
                });

                eprintln!(
                    "[MIG-PHASE] slot={} Scanning → Streaming (keys_total={})",
                    slot_id, new_keys_found
                );
                tracing::debug!(
                    slot_id,
                    keys_total = new_keys_found,
                    "Migration: Scanning → Streaming"
                );
            } else {
                // Continue scanning
                eprintln!(
                    "[MIG-PHASE] slot={} Scanning continues (new_keys_found={})",
                    slot_id, new_keys_found
                );
                record.set_phase(MigrationPhase::Scanning {
                    cursor: next_cursor,
                    keys_found: new_keys_found,
                });
            }
        }

        Ok(())
    }

    /// Process the Streaming phase.
    async fn process_streaming<A: MigrationDataAccessor>(
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

            let mut migrations = self.migrations.write();
            if let Some(record) = migrations.get_mut(&slot_id) {
                record.set_phase(MigrationPhase::CatchingUp {
                    from_log_index: log_index,
                });

                tracing::debug!(slot_id, log_index, "Migration: Streaming → CatchingUp");
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
        }

        // Update stats
        self.stats.write().keys_migrated += transferred as u64;

        // Update slot table progress
        self.slot_table
            .update_migration_progress(slot_id, keys_total.saturating_sub(new_transferred));

        Ok(())
    }

    /// Process the CatchingUp phase.
    async fn process_catching_up<A: MigrationDataAccessor>(
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
    async fn transition_to_prepared<A: MigrationDataAccessor>(
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
        };

        match self
            .propose_to_target_shard(command, record.to_shard, accessor)
            .await
        {
            Ok(_) => {
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
                tracing::debug!(
                    slot_id,
                    target_shard = record.to_shard,
                    leader = ?leader,
                    "Cannot propose PREPARED: not target shard leader"
                );
                Ok(()) // Not an error, just skip this iteration
            }
            Err(e) => {
                // Fall back to local update if Raft not available
                tracing::debug!(
                    slot_id,
                    error = %e,
                    "Raft proposal failed, falling back to local PREPARED"
                );
                self.transition_to_prepared_local(slot_id, &validation)
            }
        }
    }

    /// Transition to PREPARED locally (fallback when Raft is not available).
    fn transition_to_prepared_local(
        &self,
        slot_id: SlotId,
        validation: &ValidationResult,
    ) -> Result<()> {
        let now = now_ms();
        let mut migrations = self.migrations.write();
        if let Some(record) = migrations.get_mut(&slot_id) {
            record.set_phase(MigrationPhase::Prepared {
                prepared_at: now,
                target_commit_index: validation.raft_commit_index,
                validation_checksum: validation.checksum,
            });

            tracing::info!(
                slot_id,
                target_commit_index = validation.raft_commit_index,
                key_count = validation.key_count,
                checksum = validation.checksum,
                "Migration validated locally: CatchingUp → Prepared"
            );
        }
        Ok(())
    }

    /// Three-layer validation before PREPARED state.
    async fn validate_migration<A: MigrationDataAccessor>(
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

        if source_count != target_count {
            return Err(Error::MigrationValidationFailed(format!(
                "Key count mismatch: source={}, target={}",
                source_count, target_count
            )));
        }

        let source_checksum = accessor.checksum_slot(from_shard, slot_id).await?;
        let target_checksum = accessor.checksum_slot(to_shard, slot_id).await?;

        if source_checksum != target_checksum {
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
    async fn transition_to_completed<A: MigrationDataAccessor>(
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
        accessor
            .delete_slot_data(record.from_shard, slot_id)
            .await?;

        // Verify source is empty
        let remaining = accessor
            .count_keys_in_slot(record.from_shard, slot_id)
            .await?;
        if remaining > 0 {
            return Err(Error::MigrationValidationFailed(format!(
                "Source still has {} keys after deletion",
                remaining
            )));
        }

        // Try to propose COMPLETED through target shard's Raft
        let command = MigrationRaftCommand::Completed {
            migration_id: record.id.clone(),
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
                    "Cannot propose COMPLETED: not target shard leader"
                );
                Ok(()) // Not an error, just skip this iteration
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
                record.set_phase(MigrationPhase::Completed { completed_at: now });
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
    async fn process_retries(&self) {
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
                tracing::info!(
                    slot_id,
                    retry_count = match &record.phase {
                        MigrationPhase::Failed { retry_count, .. } => *retry_count,
                        _ => 0,
                    },
                    "Retrying failed migration"
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
                // Only check migrations that are in progress
                if record.phase.is_in_progress()
                    && (now.saturating_sub(record.last_progress_at) > timeout_ms)
                {
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

            tracing::warn!(
                slot_id,
                old_epoch,
                new_epoch = record.id.epoch,
                old_phase,
                node_id = self.node_id,
                "Taking over stale migration with new epoch"
            );

            Ok(())
        } else {
            Err(Error::MigrationNotFound(slot_id as u32))
        }
    }

    /// Process stale migration takeovers.
    ///
    /// Called periodically to check for and take over stuck migrations.
    pub async fn process_stale_migrations<A: MigrationDataAccessor>(&self, accessor: &Arc<A>) {
        let stale_slots = self.check_for_stale_migrations();

        for slot_id in stale_slots {
            // Only take over if we're the source shard leader
            if let Some(record) = self.get_migration(slot_id) {
                if accessor.is_source_shard_leader(record.from_shard) {
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
#[derive(Debug)]
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
        }
    }

    /// Calculate which slot a key belongs to.
    fn key_to_slot(key: &[u8]) -> SlotId {
        crc16(key) % TOTAL_SLOTS as u16
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
        let shard = self
            .router
            .get_shard(shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        // Iterate through storage, filtering by slot
        let storage = shard.storage();
        let mut keys = Vec::with_capacity(limit);
        let mut next_cursor = None;

        // Collect keys that belong to this slot
        let cursor_bytes = cursor.map(Bytes::copy_from_slice);

        for (key_arc, _value) in storage.iter() {
            let key = (*key_arc).clone();

            // Skip keys until we reach the cursor position
            if let Some(ref cursor) = cursor_bytes {
                if key <= *cursor {
                    continue;
                }
            }

            // Check if key belongs to this slot
            if Self::key_to_slot(&key) == slot_id {
                keys.push(key.to_vec());

                if keys.len() >= limit {
                    next_cursor = Some(key.to_vec());
                    break;
                }
            }
        }

        Ok((keys, next_cursor))
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
            if Self::key_to_slot(&key_arc) == slot_id {
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
            if Self::key_to_slot(&key_arc) == slot_id {
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
                if Self::key_to_slot(&key_arc) == slot_id {
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
        // Primary: Check shard Raft manager directly (authoritative source of truth)
        // The shard Raft node knows if it's the leader via Raft consensus
        if let Some(manager) = self.shard_raft_manager.read().as_ref() {
            if let Some(shard_raft) = manager.get_shard(to_shard) {
                let is_leader = shard_raft.is_leader();
                let leader_id = shard_raft.leader_id();
                // Debug: Log leadership check details
                static TGT_LEADER_LOG: std::sync::atomic::AtomicU64 =
                    std::sync::atomic::AtomicU64::new(0);
                let count = TGT_LEADER_LOG.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if count < 20 || count % 1000 == 0 {
                    eprintln!(
                        "[TGT-LEADER] #{} shard={} node={} is_leader={} leader_id={:?}",
                        count, to_shard, self.node_id, is_leader, leader_id
                    );
                }
                tracing::trace!(
                    to_shard,
                    node_id = self.node_id,
                    is_leader,
                    "Target shard leader check via shard_raft_manager"
                );
                return is_leader;
            }
            // Shard doesn't exist in manager
            static TGT_NOSHARD_LOG: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let count = TGT_NOSHARD_LOG.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count < 10 || count % 1000 == 0 {
                let shard_ids = manager.shard_ids();
                eprintln!(
                    "[TGT-LEADER] #{} shard={} NOT IN manager (has {:?}), node={}",
                    count, to_shard, shard_ids, self.node_id
                );
            }
            return false;
        }

        // Fallback: check leader_tracker (may be stale but useful for single-node mode)
        if let Some(leader) = self.leader_tracker.get_leader(to_shard) {
            let is_leader = leader == self.node_id;
            tracing::trace!(
                to_shard,
                leader,
                node_id = self.node_id,
                is_leader,
                "Target shard leader check via leader_tracker"
            );
            return is_leader;
        }

        // No manager at all - single-node test mode
        tracing::trace!(
            to_shard,
            node_id = self.node_id,
            "No shard_raft_manager, returning true (single-node mode)"
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_migrator() -> SlotMigrator {
        let slot_table = Arc::new(SlotTable::new(4));
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

        // Fresh migrations should not be stale
        let stale = migrator.check_for_stale_migrations();
        assert!(stale.is_empty());

        // Manually set old progress timestamp
        {
            let mut migrations = migrator.migrations.write();
            if let Some(record) = migrations.get_mut(&0) {
                record.last_progress_at = 0; // Very old
            }
        }

        // Now slot 0 should be stale
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
}
