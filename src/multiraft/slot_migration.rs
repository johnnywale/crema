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

use super::shard::ShardId;
use super::slot_control_plane::SlotControlPlane;
use super::slot_table::{SlotId, SlotTable};
use crate::error::{Error, Result};
use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Migration phase state machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum MigrationPhase {
    /// Slot reassigned, migration not started.
    /// - New owner accepts writes (may not have data yet)
    /// - Old owner handles reads, redirects writes
    #[default]
    Pending,

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

    /// Migration complete, source can GC.
    Completed {
        /// When migration completed (ms since epoch).
        completed_at: u64,
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
    /// Check if migration is complete.
    pub fn is_completed(&self) -> bool {
        matches!(self, MigrationPhase::Completed { .. })
    }

    /// Check if migration has failed.
    pub fn is_failed(&self) -> bool {
        matches!(self, MigrationPhase::Failed { .. })
    }

    /// Check if migration is in progress.
    pub fn is_in_progress(&self) -> bool {
        !self.is_completed() && !self.is_failed()
    }

    /// Get phase name for logging.
    pub fn name(&self) -> &'static str {
        match self {
            MigrationPhase::Pending => "Pending",
            MigrationPhase::Scanning { .. } => "Scanning",
            MigrationPhase::Streaming { .. } => "Streaming",
            MigrationPhase::CatchingUp { .. } => "CatchingUp",
            MigrationPhase::Completed { .. } => "Completed",
            MigrationPhase::Failed { .. } => "Failed",
        }
    }
}

/// Record of a slot migration (persisted for crash recovery).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotMigrationRecord {
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
}

impl SlotMigrationRecord {
    /// Create a new migration record.
    pub fn new(slot_id: SlotId, from_shard: ShardId, to_shard: ShardId) -> Self {
        let now = now_ms();
        Self {
            slot_id,
            from_shard,
            to_shard,
            phase: MigrationPhase::Pending,
            created_at: now,
            updated_at: now,
        }
    }

    /// Update the phase.
    pub fn set_phase(&mut self, phase: MigrationPhase) {
        self.phase = phase;
        self.updated_at = now_ms();
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
}

impl Default for SlotMigratorConfig {
    fn default() -> Self {
        Self {
            scan_batch_size: 1000,
            stream_batch_size: 100,
            max_retries: 3,
            loop_interval: Duration::from_millis(100),
            operation_timeout: Duration::from_secs(30),
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
#[derive(Debug)]
pub struct SlotMigrator {
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
    pub fn new(slot_table: Arc<SlotTable>, config: SlotMigratorConfig) -> Self {
        Self {
            slot_table,
            migrations: RwLock::new(HashMap::new()),
            config,
            stats: RwLock::new(MigrationStats::default()),
            running: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Create with default configuration.
    pub fn with_defaults(slot_table: Arc<SlotTable>) -> Self {
        Self::new(slot_table, SlotMigratorConfig::default())
    }

    /// Register a new migration.
    pub fn register_migration(&self, slot_id: SlotId, from_shard: ShardId, to_shard: ShardId) {
        let record = SlotMigrationRecord::new(slot_id, from_shard, to_shard);
        self.migrations.write().insert(slot_id, record);

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

    /// Get migration status.
    pub fn status(&self) -> MigrationStatus {
        let migrations = self.migrations.read();
        let stats = self.stats.read();

        let mut by_phase: HashMap<String, usize> = HashMap::new();
        let mut active = 0;
        let mut completed = 0;
        let mut failed = 0;

        for record in migrations.values() {
            *by_phase.entry(record.phase.name().to_string()).or_insert(0) += 1;

            match &record.phase {
                MigrationPhase::Completed { .. } => completed += 1,
                MigrationPhase::Failed { .. } => failed += 1,
                _ => active += 1,
            }
        }

        MigrationStatus {
            active_migrations: active,
            completed_migrations: completed + stats.completed as usize,
            failed_migrations: failed + stats.failed as usize,
            total_keys_migrated: stats.keys_migrated,
            by_phase,
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
        // Get migrations that need processing
        let pending: Vec<SlotMigrationRecord> = self
            .migrations
            .read()
            .values()
            .filter(|r| r.phase.is_in_progress())
            .cloned()
            .collect();

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

        Ok(())
    }

    /// Advance a single migration through its state machine.
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

        match &record.phase {
            MigrationPhase::Pending => {
                self.transition_to_scanning(slot_id).await?;
            }
            MigrationPhase::Scanning { cursor, keys_found } => {
                self.process_scanning(slot_id, cursor.clone(), *keys_found, accessor)
                    .await?;
            }
            MigrationPhase::Streaming {
                keys_total,
                keys_transferred,
                last_key,
            } => {
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
                self.process_catching_up(slot_id, *from_log_index, accessor, control_plane)
                    .await?;
            }
            MigrationPhase::Completed { .. } => {
                // Nothing to do
            }
            MigrationPhase::Failed { .. } => {
                // Handled in process_retries
            }
        }

        Ok(())
    }

    /// Transition from Pending to Scanning.
    async fn transition_to_scanning(&self, slot_id: SlotId) -> Result<()> {
        let mut migrations = self.migrations.write();
        if let Some(record) = migrations.get_mut(&slot_id) {
            record.set_phase(MigrationPhase::Scanning {
                cursor: None,
                keys_found: 0,
            });

            tracing::debug!(slot_id, "Migration: Pending → Scanning");
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

        // Scan a batch of keys
        let (keys, next_cursor) = accessor
            .scan_slot_keys(
                record.from_shard,
                slot_id,
                cursor.as_deref(),
                self.config.scan_batch_size,
            )
            .await?;

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

                tracing::debug!(
                    slot_id,
                    keys_total = new_keys_found,
                    "Migration: Scanning → Streaming"
                );
            } else {
                // Continue scanning
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
        control_plane: &Option<Arc<SlotControlPlane>>,
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
            // Caught up! Mark as complete
            self.complete_migration(slot_id, control_plane).await?;
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

        // Update log index
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

    /// Complete a migration.
    async fn complete_migration(
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
}
