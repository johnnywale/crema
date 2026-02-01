//! Migration state machine for Raft-based migration coordination.
//!
//! This module provides a state machine for migration commands that runs
//! alongside the CacheStateMachine on the target shard's Raft group.
//!
//! # Design
//!
//! All migration coordination commands (Claim, Prepared, Completed, Cleaned)
//! are proposed through the **target shard's Raft** to ensure:
//! - **Survivability**: Target shard persists after migration (source may be removed)
//! - **Atomicity**: Data and migration state are collocated in same Raft group
//! - **Pull Model Support**: Target shard drives when source is being removed
//!
//! # State Transitions
//!
//! ```text
//! PENDING → CLAIMED → SCANNING → STREAMING → CATCHING_UP → PREPARED → COMPLETED → CLEANED
//!    │         │          │           │            │            │          │          │
//!    │         │          │           │            │            │          │          └─ Source data deleted
//!    │         │          │           │            │            │          └─ Raft committed (safe to cleanup)
//!    │         │          │           │            │            └─ Target validated + Raft committed
//!    │         │          │           │            └─ Replaying new writes
//!    │         │          │           └─ Data transfer in progress
//!    │         │          └─ Scanning source keys
//!    │         └─ Raft committed claim (prevents races)
//!    └─ Migration registered locally
//! ```
//!
//! **Raft-committed states**: CLAIMED, PREPARED, COMPLETED, CLEANED (these affect safety)
//! **Local-only states**: SCANNING, STREAMING, CATCHING_UP, FAILED (leader-only work)

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::slot_migration::{
    MigrationId, MigrationPhase, MigrationRaftCommand, SlotMigrationRecord,
};
use super::slot_table::SlotId;
use crate::types::NodeId;

/// State machine for migration coordination commands.
///
/// Runs on target shard's Raft group. Each shard maintains its own
/// migration state machine for slots being imported to that shard.
#[derive(Debug)]
pub struct MigrationStateMachine {
    /// Active migrations for slots being imported to this shard.
    /// Key is slot_id, value is the migration record.
    migrations: RwLock<HashMap<SlotId, SlotMigrationRecord>>,

    /// Last applied Raft index for idempotency.
    applied_index: AtomicU64,

    /// Last applied Raft term for validation.
    applied_term: AtomicU64,
}

impl Default for MigrationStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl MigrationStateMachine {
    /// Create a new migration state machine.
    pub fn new() -> Self {
        Self {
            migrations: RwLock::new(HashMap::new()),
            applied_index: AtomicU64::new(0),
            applied_term: AtomicU64::new(0),
        }
    }

    /// Get the last applied Raft index.
    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::SeqCst)
    }

    /// Get the last applied Raft term.
    pub fn applied_term(&self) -> u64 {
        self.applied_term.load(Ordering::SeqCst)
    }

    /// Apply a migration command from the Raft log.
    ///
    /// This method is called by the shard's Raft when a migration command
    /// is committed. It applies the command idempotently.
    ///
    /// # Arguments
    ///
    /// * `index` - The Raft log index of this entry
    /// * `term` - The Raft term of this entry
    /// * `command` - The migration command to apply
    ///
    /// # Returns
    ///
    /// Returns `true` if the command was applied, `false` if skipped (already applied).
    pub fn apply(&self, index: u64, term: u64, command: MigrationRaftCommand) -> bool {
        // Idempotency check - skip if already applied
        if index <= self.applied_index.load(Ordering::SeqCst) {
            tracing::trace!(
                index,
                applied_index = self.applied_index.load(Ordering::SeqCst),
                "Skipping already applied migration command"
            );
            return false;
        }

        tracing::debug!(
            index,
            term,
            command = command.name(),
            migration_id = %command.migration_id(),
            "Applying migration command"
        );

        match command {
            MigrationRaftCommand::Claim {
                migration_id,
                leader_id,
            } => {
                self.apply_claim(migration_id, leader_id);
            }
            MigrationRaftCommand::Prepared {
                migration_id,
                target_commit_index,
                validation_checksum,
            } => {
                self.apply_prepared(migration_id, target_commit_index, validation_checksum);
            }
            MigrationRaftCommand::Completed { migration_id } => {
                self.apply_completed(migration_id);
            }
            MigrationRaftCommand::Cleaned { migration_id } => {
                self.apply_cleaned(migration_id);
            }
        }

        // Update applied index and term
        self.applied_index.store(index, Ordering::SeqCst);
        self.applied_term.store(term, Ordering::SeqCst);

        true
    }

    /// Apply a Claim command.
    ///
    /// Transitions the migration to CLAIMED state with the specified owner.
    /// Only accepts the claim if the epoch is >= the current epoch.
    fn apply_claim(&self, migration_id: MigrationId, leader_id: NodeId) {
        let slot_id = migration_id.slot_id;
        let mut migrations = self.migrations.write();

        if let Some(record) = migrations.get_mut(&slot_id) {
            // Validate epoch - only accept if same or higher
            if migration_id.epoch >= record.id.epoch {
                let now = now_ms();
                record.id.epoch = migration_id.epoch;
                record.phase = MigrationPhase::Claimed {
                    owner_node: leader_id,
                    claim_epoch: migration_id.epoch,
                    claimed_at: now,
                };
                record.updated_at = now;
                record.last_progress_at = now;

                tracing::info!(
                    slot_id,
                    epoch = migration_id.epoch,
                    leader_id,
                    "Migration claimed via Raft"
                );
            } else {
                tracing::warn!(
                    slot_id,
                    proposed_epoch = migration_id.epoch,
                    current_epoch = record.id.epoch,
                    "Rejecting claim with stale epoch"
                );
            }
        } else {
            // Migration not registered - this can happen if the target shard
            // received the claim before registering the migration locally.
            // Create a new record in CLAIMED state.
            let now = now_ms();
            let record = SlotMigrationRecord {
                id: migration_id.clone(),
                slot_id,
                from_shard: 0, // Will be updated by the migration coordinator
                to_shard: 0,   // Will be updated by the migration coordinator
                phase: MigrationPhase::Claimed {
                    owner_node: leader_id,
                    claim_epoch: migration_id.epoch,
                    claimed_at: now,
                },
                created_at: now,
                updated_at: now,
                last_progress_at: now,
            };
            // We'll need the shard info, but for now just mark as claimed
            migrations.insert(slot_id, record);

            tracing::info!(
                slot_id,
                epoch = migration_id.epoch,
                leader_id,
                "Migration claimed via Raft (auto-registered)"
            );
        }
    }

    /// Apply a Prepared command.
    ///
    /// Transitions the migration to PREPARED state, recording validation info.
    /// This commits the validation result before source cleanup is allowed.
    fn apply_prepared(
        &self,
        migration_id: MigrationId,
        target_commit_index: u64,
        validation_checksum: u64,
    ) {
        let slot_id = migration_id.slot_id;
        let mut migrations = self.migrations.write();

        if let Some(record) = migrations.get_mut(&slot_id) {
            // Validate epoch matches
            if migration_id.epoch != record.id.epoch {
                tracing::warn!(
                    slot_id,
                    proposed_epoch = migration_id.epoch,
                    current_epoch = record.id.epoch,
                    "Rejecting PREPARED with mismatched epoch"
                );
                return;
            }

            let now = now_ms();
            record.phase = MigrationPhase::Prepared {
                prepared_at: now,
                target_commit_index,
                validation_checksum,
            };
            record.updated_at = now;
            record.last_progress_at = now;

            tracing::info!(
                slot_id,
                epoch = migration_id.epoch,
                target_commit_index,
                validation_checksum,
                "Migration prepared via Raft (safe to cleanup source)"
            );
        } else {
            tracing::warn!(
                slot_id,
                epoch = migration_id.epoch,
                "Received PREPARED for unregistered migration"
            );
        }
    }

    /// Apply a Completed command.
    ///
    /// Transitions the migration to COMPLETED state.
    /// Source data has been deleted and verified empty.
    fn apply_completed(&self, migration_id: MigrationId) {
        let slot_id = migration_id.slot_id;
        let mut migrations = self.migrations.write();

        if let Some(record) = migrations.get_mut(&slot_id) {
            // Validate epoch matches
            if migration_id.epoch != record.id.epoch {
                tracing::warn!(
                    slot_id,
                    proposed_epoch = migration_id.epoch,
                    current_epoch = record.id.epoch,
                    "Rejecting COMPLETED with mismatched epoch"
                );
                return;
            }

            let now = now_ms();
            record.phase = MigrationPhase::Completed { completed_at: now };
            record.updated_at = now;
            record.last_progress_at = now;

            tracing::info!(
                slot_id,
                epoch = migration_id.epoch,
                "Migration completed via Raft"
            );
        } else {
            tracing::warn!(
                slot_id,
                epoch = migration_id.epoch,
                "Received COMPLETED for unregistered migration"
            );
        }
    }

    /// Apply a Cleaned command.
    ///
    /// Transitions the migration to CLEANED state (optional final state).
    fn apply_cleaned(&self, migration_id: MigrationId) {
        let slot_id = migration_id.slot_id;
        let mut migrations = self.migrations.write();

        if let Some(record) = migrations.get_mut(&slot_id) {
            // Validate epoch matches
            if migration_id.epoch != record.id.epoch {
                tracing::warn!(
                    slot_id,
                    proposed_epoch = migration_id.epoch,
                    current_epoch = record.id.epoch,
                    "Rejecting CLEANED with mismatched epoch"
                );
                return;
            }

            let now = now_ms();
            record.phase = MigrationPhase::Cleaned { cleaned_at: now };
            record.updated_at = now;
            record.last_progress_at = now;

            tracing::info!(
                slot_id,
                epoch = migration_id.epoch,
                "Migration cleaned via Raft"
            );
        } else {
            tracing::warn!(
                slot_id,
                epoch = migration_id.epoch,
                "Received CLEANED for unregistered migration"
            );
        }
    }

    // ==================== Query Methods ====================

    /// Get a migration record by slot ID.
    pub fn get_migration(&self, slot_id: SlotId) -> Option<SlotMigrationRecord> {
        self.migrations.read().get(&slot_id).cloned()
    }

    /// Get all active migrations (not completed or cleaned).
    pub fn active_migrations(&self) -> Vec<SlotMigrationRecord> {
        self.migrations
            .read()
            .values()
            .filter(|r| r.phase.is_in_progress())
            .cloned()
            .collect()
    }

    /// Get all migrations.
    pub fn all_migrations(&self) -> Vec<SlotMigrationRecord> {
        self.migrations.read().values().cloned().collect()
    }

    /// Check if a migration is claimed by a specific node.
    pub fn is_claimed_by(&self, slot_id: SlotId, node_id: NodeId) -> bool {
        self.migrations
            .read()
            .get(&slot_id)
            .map(|r| r.phase.is_claimed_by(node_id))
            .unwrap_or(false)
    }

    /// Get the current epoch for a slot's migration.
    pub fn get_epoch(&self, slot_id: SlotId) -> Option<u64> {
        self.migrations.read().get(&slot_id).map(|r| r.id.epoch)
    }

    // ==================== Registration Methods ====================

    /// Register a pending migration.
    ///
    /// This should be called when the slot table indicates a migration
    /// is needed. The actual claim will go through Raft.
    pub fn register_migration(
        &self,
        slot_id: SlotId,
        from_shard: u32,
        to_shard: u32,
    ) -> SlotMigrationRecord {
        let mut migrations = self.migrations.write();

        if let Some(existing) = migrations.get(&slot_id) {
            // Already registered, return existing
            return existing.clone();
        }

        let record = SlotMigrationRecord::new(slot_id, from_shard, to_shard);
        migrations.insert(slot_id, record.clone());

        tracing::debug!(
            slot_id,
            from_shard,
            to_shard,
            "Registered migration in state machine"
        );

        record
    }

    /// Update shard info for a migration (called when claim arrives before registration).
    pub fn update_shard_info(&self, slot_id: SlotId, from_shard: u32, to_shard: u32) {
        let mut migrations = self.migrations.write();
        if let Some(record) = migrations.get_mut(&slot_id) {
            record.from_shard = from_shard;
            record.to_shard = to_shard;
        }
    }

    /// Remove a completed migration from tracking.
    pub fn remove_migration(&self, slot_id: SlotId) -> Option<SlotMigrationRecord> {
        self.migrations.write().remove(&slot_id)
    }

    /// Clear all migrations (for testing).
    #[cfg(test)]
    pub fn clear(&self) {
        self.migrations.write().clear();
        self.applied_index.store(0, Ordering::SeqCst);
        self.applied_term.store(0, Ordering::SeqCst);
    }

    // ==================== Snapshot/Restore ====================

    /// Get snapshot data for persistence.
    pub fn snapshot(&self) -> Vec<SlotMigrationRecord> {
        self.migrations.read().values().cloned().collect()
    }

    /// Restore from snapshot data.
    pub fn restore(&self, records: Vec<SlotMigrationRecord>, index: u64, term: u64) {
        let mut migrations = self.migrations.write();
        migrations.clear();
        for record in records {
            migrations.insert(record.slot_id, record);
        }
        self.applied_index.store(index, Ordering::SeqCst);
        self.applied_term.store(term, Ordering::SeqCst);
    }
}

/// Get current time in milliseconds since Unix epoch.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_state_machine() -> MigrationStateMachine {
        MigrationStateMachine::new()
    }

    #[test]
    fn test_apply_claim() {
        let sm = create_state_machine();

        // Register a migration first
        sm.register_migration(42, 0, 1);

        // Apply claim
        let migration_id = MigrationId::new(42, 1);
        let command = MigrationRaftCommand::Claim {
            migration_id: migration_id.clone(),
            leader_id: 5,
        };

        let applied = sm.apply(1, 1, command);
        assert!(applied);

        // Verify state
        let record = sm.get_migration(42).unwrap();
        assert!(record.phase.is_claimed_by(5));
        assert_eq!(record.phase.claim_epoch(), Some(1));
    }

    #[test]
    fn test_apply_claim_auto_register() {
        let sm = create_state_machine();

        // Apply claim without registering first
        let migration_id = MigrationId::new(42, 1);
        let command = MigrationRaftCommand::Claim {
            migration_id: migration_id.clone(),
            leader_id: 5,
        };

        let applied = sm.apply(1, 1, command);
        assert!(applied);

        // Verify migration was auto-registered
        let record = sm.get_migration(42).unwrap();
        assert!(record.phase.is_claimed_by(5));
    }

    #[test]
    fn test_reject_stale_epoch_claim() {
        let sm = create_state_machine();

        // Register and claim with epoch 2
        sm.register_migration(42, 0, 1);
        let command1 = MigrationRaftCommand::Claim {
            migration_id: MigrationId::new(42, 2),
            leader_id: 5,
        };
        sm.apply(1, 1, command1);

        // Try to claim with epoch 1 (stale)
        let command2 = MigrationRaftCommand::Claim {
            migration_id: MigrationId::new(42, 1),
            leader_id: 6,
        };
        sm.apply(2, 1, command2);

        // Verify original claim is preserved
        let record = sm.get_migration(42).unwrap();
        assert!(record.phase.is_claimed_by(5));
        assert_eq!(record.id.epoch, 2);
    }

    #[test]
    fn test_apply_prepared() {
        let sm = create_state_machine();

        // Register and claim
        sm.register_migration(42, 0, 1);
        sm.apply(
            1,
            1,
            MigrationRaftCommand::Claim {
                migration_id: MigrationId::new(42, 1),
                leader_id: 5,
            },
        );

        // Apply prepared
        let command = MigrationRaftCommand::Prepared {
            migration_id: MigrationId::new(42, 1),
            target_commit_index: 100,
            validation_checksum: 12345,
        };
        sm.apply(2, 1, command);

        // Verify state
        let record = sm.get_migration(42).unwrap();
        assert!(record.phase.is_prepared());
        if let MigrationPhase::Prepared {
            target_commit_index,
            validation_checksum,
            ..
        } = &record.phase
        {
            assert_eq!(*target_commit_index, 100);
            assert_eq!(*validation_checksum, 12345);
        }
    }

    #[test]
    fn test_apply_completed() {
        let sm = create_state_machine();

        // Register, claim, and prepare
        sm.register_migration(42, 0, 1);
        sm.apply(
            1,
            1,
            MigrationRaftCommand::Claim {
                migration_id: MigrationId::new(42, 1),
                leader_id: 5,
            },
        );
        sm.apply(
            2,
            1,
            MigrationRaftCommand::Prepared {
                migration_id: MigrationId::new(42, 1),
                target_commit_index: 100,
                validation_checksum: 12345,
            },
        );

        // Apply completed
        sm.apply(
            3,
            1,
            MigrationRaftCommand::Completed {
                migration_id: MigrationId::new(42, 1),
            },
        );

        // Verify state
        let record = sm.get_migration(42).unwrap();
        assert!(record.phase.is_completed());
    }

    #[test]
    fn test_idempotency() {
        let sm = create_state_machine();
        sm.register_migration(42, 0, 1);

        let command = MigrationRaftCommand::Claim {
            migration_id: MigrationId::new(42, 1),
            leader_id: 5,
        };

        // First apply
        assert!(sm.apply(1, 1, command.clone()));

        // Second apply with same index should be skipped
        assert!(!sm.apply(1, 1, command.clone()));

        // Apply with higher index should succeed
        let command2 = MigrationRaftCommand::Prepared {
            migration_id: MigrationId::new(42, 1),
            target_commit_index: 100,
            validation_checksum: 0,
        };
        assert!(sm.apply(2, 1, command2));
    }

    #[test]
    fn test_active_migrations() {
        let sm = create_state_machine();

        // Register multiple migrations
        sm.register_migration(1, 0, 1);
        sm.register_migration(2, 0, 1);
        sm.register_migration(3, 0, 1);

        // Claim one, complete another
        sm.apply(
            1,
            1,
            MigrationRaftCommand::Claim {
                migration_id: MigrationId::new(1, 1),
                leader_id: 5,
            },
        );
        sm.apply(
            2,
            1,
            MigrationRaftCommand::Claim {
                migration_id: MigrationId::new(2, 1),
                leader_id: 5,
            },
        );
        sm.apply(
            3,
            1,
            MigrationRaftCommand::Prepared {
                migration_id: MigrationId::new(2, 1),
                target_commit_index: 100,
                validation_checksum: 0,
            },
        );
        sm.apply(
            4,
            1,
            MigrationRaftCommand::Completed {
                migration_id: MigrationId::new(2, 1),
            },
        );

        // Check active migrations
        let active = sm.active_migrations();
        assert_eq!(active.len(), 2); // Slot 1 (claimed) and slot 3 (pending)
    }

    #[test]
    fn test_snapshot_restore() {
        let sm1 = create_state_machine();

        // Setup some state
        sm1.register_migration(1, 0, 1);
        sm1.apply(
            1,
            1,
            MigrationRaftCommand::Claim {
                migration_id: MigrationId::new(1, 1),
                leader_id: 5,
            },
        );

        // Take snapshot
        let snapshot = sm1.snapshot();
        let index = sm1.applied_index();
        let term = sm1.applied_term();

        // Restore to new state machine
        let sm2 = create_state_machine();
        sm2.restore(snapshot, index, term);

        // Verify state is preserved
        let record = sm2.get_migration(1).unwrap();
        assert!(record.phase.is_claimed_by(5));
        assert_eq!(sm2.applied_index(), 1);
        assert_eq!(sm2.applied_term(), 1);
    }
}
