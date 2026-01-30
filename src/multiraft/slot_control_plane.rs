//! Slot control plane for shard lifecycle management.
//!
//! The control plane manages shard lifecycles and orchestrates slot rebalancing
//! when shards are added or removed from the cluster.
//!
//! # Shard States
//!
//! ```text
//! ACTIVE → DRAINING → TOMBSTONE → (GC)
//! ```
//!
//! - **Active**: Shard is accepting requests normally.
//! - **Draining**: Shard is being removed; all slots are being migrated away.
//! - **Tombstone**: All slots migrated; shard is waiting for grace period before GC.

use super::shard::ShardId;
use super::slot_table::{
    Epoch, SlotId, SlotReassignment, SlotTable, SlotTableSnapshot, TOTAL_SLOTS,
};
use crate::error::{Error, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// State of a shard in the control plane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ShardState {
    /// Shard is active and accepting requests.
    #[default]
    Active,

    /// Shard is being removed; all slots are being migrated away.
    Draining {
        /// Number of slots remaining to be migrated.
        slots_remaining: usize,
        /// When draining started.
        started_at_ms: u64,
    },

    /// All slots migrated; waiting for grace period before cleanup.
    Tombstone {
        /// When the shard entered tombstone state.
        marked_at_ms: u64,
    },
}

impl ShardState {
    /// Check if the shard is active.
    pub fn is_active(&self) -> bool {
        matches!(self, ShardState::Active)
    }

    /// Check if the shard is draining.
    pub fn is_draining(&self) -> bool {
        matches!(self, ShardState::Draining { .. })
    }

    /// Check if the shard is a tombstone.
    pub fn is_tombstone(&self) -> bool {
        matches!(self, ShardState::Tombstone { .. })
    }
}

/// Information about a shard in the control plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardControlInfo {
    /// Shard ID.
    pub shard_id: ShardId,
    /// Current state.
    pub state: ShardState,
    /// Number of slots owned.
    pub slot_count: usize,
    /// When the shard was created.
    pub created_at_ms: u64,
}

/// Result of an add_shard operation.
#[derive(Debug, Clone)]
pub struct AddShardResult {
    /// The new shard ID.
    pub shard_id: ShardId,
    /// Number of slots assigned to the new shard.
    pub slots_assigned: usize,
    /// New epoch after the operation.
    pub new_epoch: Epoch,
    /// Slot reassignment details.
    pub reassignment: SlotReassignment,
}

/// Result of a remove_shard operation.
#[derive(Debug, Clone)]
pub struct RemoveShardResult {
    /// The shard being removed.
    pub shard_id: ShardId,
    /// Number of slots being redistributed.
    pub slots_to_redistribute: usize,
    /// New epoch after the operation.
    pub new_epoch: Epoch,
    /// Mapping of target shard -> slots being assigned.
    pub reassignments: HashMap<ShardId, Vec<SlotId>>,
}

/// Configuration for the control plane.
#[derive(Debug, Clone)]
pub struct ControlPlaneConfig {
    /// Grace period before a tombstone shard can be GC'd.
    pub tombstone_grace_period: Duration,
    /// Minimum slots per shard (to prevent over-draining).
    pub min_slots_per_shard: usize,
    /// Maximum slots per shard (to prevent imbalance).
    pub max_slots_per_shard: usize,
}

impl Default for ControlPlaneConfig {
    fn default() -> Self {
        Self {
            tombstone_grace_period: Duration::from_secs(300), // 5 minutes
            min_slots_per_shard: 1,
            max_slots_per_shard: TOTAL_SLOTS,
        }
    }
}

/// Slot control plane for managing shard lifecycles.
///
/// The control plane is responsible for:
/// - Adding new shards and computing slot rebalance
/// - Removing shards and draining their slots
/// - Tracking shard states through the lifecycle
/// - Managing tombstones and GC
#[derive(Debug)]
pub struct SlotControlPlane {
    /// The slot table being managed.
    slot_table: Arc<SlotTable>,

    /// Shard states.
    shard_states: RwLock<HashMap<ShardId, ShardControlInfo>>,

    /// Configuration.
    config: ControlPlaneConfig,

    /// Next shard ID to assign.
    next_shard_id: RwLock<ShardId>,
}

impl SlotControlPlane {
    /// Create a new control plane.
    pub fn new(slot_table: Arc<SlotTable>, config: ControlPlaneConfig) -> Self {
        let num_shards = slot_table.num_shards();

        // Initialize shard states
        let mut shard_states = HashMap::new();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        for shard_id in 0..num_shards as ShardId {
            let slot_count = slot_table.slot_count_for_shard(shard_id);
            shard_states.insert(
                shard_id,
                ShardControlInfo {
                    shard_id,
                    state: ShardState::Active,
                    slot_count,
                    created_at_ms: now_ms,
                },
            );
        }

        Self {
            slot_table,
            shard_states: RwLock::new(shard_states),
            config,
            next_shard_id: RwLock::new(num_shards as ShardId),
        }
    }

    /// Create a control plane with default configuration.
    pub fn with_defaults(slot_table: Arc<SlotTable>) -> Self {
        Self::new(slot_table, ControlPlaneConfig::default())
    }

    /// Get the slot table.
    pub fn slot_table(&self) -> Arc<SlotTable> {
        self.slot_table.clone()
    }

    /// Get the current epoch.
    pub fn epoch(&self) -> Epoch {
        self.slot_table.epoch()
    }

    /// Get information about all shards.
    pub fn shard_info(&self) -> Vec<ShardControlInfo> {
        self.shard_states.read().values().cloned().collect()
    }

    /// Get information about a specific shard.
    pub fn get_shard_info(&self, shard_id: ShardId) -> Option<ShardControlInfo> {
        self.shard_states.read().get(&shard_id).cloned()
    }

    /// Get the state of a shard.
    pub fn shard_state(&self, shard_id: ShardId) -> Option<ShardState> {
        self.shard_states
            .read()
            .get(&shard_id)
            .map(|info| info.state.clone())
    }

    /// Get the number of active shards.
    pub fn active_shard_count(&self) -> usize {
        self.shard_states
            .read()
            .values()
            .filter(|info| info.state.is_active())
            .count()
    }

    /// Check if a shard is active.
    pub fn is_shard_active(&self, shard_id: ShardId) -> bool {
        self.shard_states
            .read()
            .get(&shard_id)
            .map(|info| info.state.is_active())
            .unwrap_or(false)
    }

    /// Add a new shard to the cluster.
    ///
    /// This computes a rebalance plan and updates the slot table. The actual
    /// data migration happens in the background via the migration system.
    ///
    /// # Returns
    ///
    /// Returns the new shard ID and the slot reassignment plan.
    pub fn add_shard(&self) -> Result<AddShardResult> {
        // Allocate new shard ID
        let new_shard_id = {
            let mut next = self.next_shard_id.write();
            let id = *next;
            *next += 1;
            id
        };

        // Compute rebalance plan
        let slots_to_move = self
            .slot_table
            .compute_rebalance_for_new_shard(new_shard_id);
        let slots_assigned = slots_to_move.len();

        // Apply reassignment
        let reassignment = self.slot_table.reassign_slots(&slots_to_move, new_shard_id);

        // Register new shard
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let mut states = self.shard_states.write();
        states.insert(
            new_shard_id,
            ShardControlInfo {
                shard_id: new_shard_id,
                state: ShardState::Active,
                slot_count: slots_assigned,
                created_at_ms: now_ms,
            },
        );

        // Update slot counts for affected shards
        for (_, from, _) in &reassignment.moves {
            if let Some(info) = states.get_mut(from) {
                info.slot_count = info.slot_count.saturating_sub(1);
            }
        }

        tracing::info!(
            shard_id = new_shard_id,
            slots_assigned,
            new_epoch = reassignment.new_epoch.value(),
            "Added new shard"
        );

        Ok(AddShardResult {
            shard_id: new_shard_id,
            slots_assigned,
            new_epoch: reassignment.new_epoch,
            reassignment,
        })
    }

    /// Remove a shard from the cluster.
    ///
    /// This marks the shard as draining and computes a plan to redistribute
    /// its slots among remaining shards. The actual data migration happens
    /// in the background.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The shard doesn't exist
    /// - The shard is already draining or tombstoned
    /// - Removing the shard would leave no active shards
    pub fn remove_shard(&self, shard_id: ShardId) -> Result<RemoveShardResult> {
        let mut states = self.shard_states.write();

        // Validate shard exists and is active
        let info = states
            .get(&shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        if !info.state.is_active() {
            return Err(Error::ShardNotActive(shard_id));
        }

        // Check we'd have at least one active shard remaining
        let active_count = states.values().filter(|i| i.state.is_active()).count();
        if active_count <= 1 {
            return Err(Error::Internal(
                "Cannot remove the last active shard".to_string(),
            ));
        }

        // Compute drain plan
        let drain_plan = self.slot_table.compute_drain_for_shard(shard_id);
        let total_slots: usize = drain_plan.values().map(|v| v.len()).sum();

        // Apply reassignments
        let mut all_reassignments = SlotReassignment::new(self.slot_table.epoch().increment());

        for (target_shard, slots) in &drain_plan {
            for &slot_id in slots {
                all_reassignments.add_move(slot_id, shard_id, *target_shard);
            }

            // Update slot count for target
            if let Some(target_info) = states.get_mut(target_shard) {
                target_info.slot_count += slots.len();
            }
        }

        // Apply to slot table
        self.slot_table.apply_reassignment(&all_reassignments);

        // Mark shard as draining
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        if let Some(info) = states.get_mut(&shard_id) {
            info.state = ShardState::Draining {
                slots_remaining: total_slots,
                started_at_ms: now_ms,
            };
            info.slot_count = 0;
        }

        tracing::info!(
            shard_id,
            slots_to_redistribute = total_slots,
            new_epoch = all_reassignments.new_epoch.value(),
            "Started shard removal (draining)"
        );

        Ok(RemoveShardResult {
            shard_id,
            slots_to_redistribute: total_slots,
            new_epoch: all_reassignments.new_epoch,
            reassignments: drain_plan,
        })
    }

    /// Update draining progress for a shard.
    ///
    /// Called by the migration system as slots complete migration.
    pub fn update_drain_progress(&self, shard_id: ShardId, slots_completed: usize) {
        let mut states = self.shard_states.write();

        if let Some(info) = states.get_mut(&shard_id) {
            // Extract needed values before mutation
            let (should_complete, started_at) = match &info.state {
                ShardState::Draining {
                    slots_remaining,
                    started_at_ms,
                } => {
                    let new_remaining = slots_remaining.saturating_sub(slots_completed);
                    (new_remaining == 0, *started_at_ms)
                }
                _ => (false, 0),
            };

            // Update the remaining count
            if let ShardState::Draining {
                slots_remaining, ..
            } = &mut info.state
            {
                *slots_remaining = slots_remaining.saturating_sub(slots_completed);
            }

            // Check if draining is complete
            if should_complete {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);

                info.state = ShardState::Tombstone {
                    marked_at_ms: now_ms,
                };

                tracing::info!(
                    shard_id,
                    drain_duration_ms = now_ms.saturating_sub(started_at),
                    "Shard draining complete, now tombstone"
                );
            }
        }
    }

    /// Mark a shard as tombstoned (ready for GC).
    ///
    /// Called when all slot migrations from the shard are complete.
    pub fn mark_tombstone(&self, shard_id: ShardId) {
        let mut states = self.shard_states.write();

        if let Some(info) = states.get_mut(&shard_id) {
            if info.state.is_draining() {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);

                info.state = ShardState::Tombstone {
                    marked_at_ms: now_ms,
                };

                tracing::info!(shard_id, "Shard marked as tombstone");
            }
        }
    }

    /// Get tombstoned shards that are ready for GC (past grace period).
    pub fn get_gc_candidates(&self) -> Vec<ShardId> {
        let grace_ms = self.config.tombstone_grace_period.as_millis() as u64;

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        self.shard_states
            .read()
            .iter()
            .filter_map(|(&shard_id, info)| {
                if let ShardState::Tombstone { marked_at_ms } = &info.state {
                    if now_ms.saturating_sub(*marked_at_ms) >= grace_ms {
                        return Some(shard_id);
                    }
                }
                None
            })
            .collect()
    }

    /// Remove a tombstoned shard (GC).
    ///
    /// # Safety
    ///
    /// Only call this after the grace period has passed and you're certain
    /// no requests are still being routed to this shard.
    pub fn gc_shard(&self, shard_id: ShardId) -> Result<()> {
        let mut states = self.shard_states.write();

        let info = states
            .get(&shard_id)
            .ok_or(Error::ShardNotFound(shard_id))?;

        if !info.state.is_tombstone() {
            return Err(Error::Internal(format!(
                "Cannot GC shard {} - not in tombstone state",
                shard_id
            )));
        }

        states.remove(&shard_id);

        tracing::info!(shard_id, "Shard garbage collected");

        Ok(())
    }

    /// Get a snapshot of the current state.
    pub fn snapshot(&self) -> ControlPlaneSnapshot {
        let shard_states = self.shard_states.read().clone();
        let slot_snapshot = self.slot_table.snapshot();

        ControlPlaneSnapshot {
            epoch: slot_snapshot.epoch,
            shard_states,
            slot_snapshot,
        }
    }

    /// Validate the control plane state is consistent.
    ///
    /// Returns a list of inconsistencies found (empty if valid).
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        let states = self.shard_states.read();

        // Check each active shard has slots
        for (shard_id, info) in states.iter() {
            if info.state.is_active() {
                let actual_slots = self.slot_table.slot_count_for_shard(*shard_id);
                if actual_slots != info.slot_count {
                    errors.push(format!(
                        "Shard {} slot count mismatch: info={}, actual={}",
                        shard_id, info.slot_count, actual_slots
                    ));
                }
            }
        }

        // Check total slots
        let total_slots: usize = (0..TOTAL_SLOTS as SlotId)
            .map(|s| self.slot_table.slot_owner(s))
            .filter(|owner| states.contains_key(owner))
            .count();

        if total_slots != TOTAL_SLOTS {
            errors.push(format!(
                "Total slots mismatch: expected {}, got {}",
                TOTAL_SLOTS, total_slots
            ));
        }

        errors
    }
}

/// Snapshot of the control plane state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlPlaneSnapshot {
    /// Current epoch.
    pub epoch: Epoch,
    /// Shard states.
    pub shard_states: HashMap<ShardId, ShardControlInfo>,
    /// Slot table snapshot.
    pub slot_snapshot: SlotTableSnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_control_plane(num_shards: usize) -> SlotControlPlane {
        let slot_table = Arc::new(SlotTable::new(num_shards));
        SlotControlPlane::with_defaults(slot_table)
    }

    #[test]
    fn test_initial_state() {
        let cp = create_test_control_plane(4);

        assert_eq!(cp.active_shard_count(), 4);

        for shard_id in 0..4 {
            assert!(cp.is_shard_active(shard_id));
            let info = cp.get_shard_info(shard_id).unwrap();
            assert_eq!(info.slot_count, 256); // 1024 / 4
        }
    }

    #[test]
    fn test_add_shard() {
        let cp = create_test_control_plane(4);

        let result = cp.add_shard().unwrap();

        assert_eq!(result.shard_id, 4);
        assert!(result.slots_assigned > 0);
        assert!(result.slots_assigned < 300); // Should be ~204

        // Verify new shard is active
        assert!(cp.is_shard_active(4));
        assert_eq!(cp.active_shard_count(), 5);

        // Verify slot table was updated
        let slot_count = cp.slot_table().slot_count_for_shard(4);
        assert_eq!(slot_count, result.slots_assigned);
    }

    #[test]
    fn test_remove_shard() {
        let cp = create_test_control_plane(4);

        let result = cp.remove_shard(3).unwrap();

        assert_eq!(result.shard_id, 3);
        assert_eq!(result.slots_to_redistribute, 256);

        // Verify shard is draining
        let state = cp.shard_state(3).unwrap();
        assert!(state.is_draining());

        // Verify slots were redistributed
        assert_eq!(cp.slot_table().slot_count_for_shard(3), 0);
    }

    #[test]
    fn test_cannot_remove_last_shard() {
        let cp = create_test_control_plane(1);

        let result = cp.remove_shard(0);
        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_remove_nonexistent_shard() {
        let cp = create_test_control_plane(4);

        let result = cp.remove_shard(99);
        assert!(matches!(result, Err(Error::ShardNotFound(99))));
    }

    #[test]
    fn test_drain_progress() {
        let cp = create_test_control_plane(4);

        cp.remove_shard(3).unwrap();

        // Simulate migration progress
        cp.update_drain_progress(3, 100);

        if let ShardState::Draining {
            slots_remaining, ..
        } = cp.shard_state(3).unwrap()
        {
            assert_eq!(slots_remaining, 156); // 256 - 100
        } else {
            panic!("Expected draining state");
        }

        // Complete draining
        cp.update_drain_progress(3, 156);

        // Should be tombstoned now
        assert!(cp.shard_state(3).unwrap().is_tombstone());
    }

    #[test]
    fn test_gc_candidates() {
        let cp = SlotControlPlane::new(
            Arc::new(SlotTable::new(4)),
            ControlPlaneConfig {
                tombstone_grace_period: Duration::from_millis(0), // Immediate
                ..Default::default()
            },
        );

        cp.remove_shard(3).unwrap();
        cp.mark_tombstone(3);

        let candidates = cp.get_gc_candidates();
        assert!(candidates.contains(&3));
    }

    #[test]
    fn test_gc_shard() {
        let cp = SlotControlPlane::new(
            Arc::new(SlotTable::new(4)),
            ControlPlaneConfig {
                tombstone_grace_period: Duration::from_millis(0),
                ..Default::default()
            },
        );

        cp.remove_shard(3).unwrap();
        cp.mark_tombstone(3);
        cp.gc_shard(3).unwrap();

        assert!(cp.get_shard_info(3).is_none());
        assert_eq!(cp.shard_info().len(), 3);
    }

    #[test]
    fn test_snapshot() {
        let cp = create_test_control_plane(4);

        let snapshot = cp.snapshot();

        assert_eq!(snapshot.epoch.value(), 1);
        assert_eq!(snapshot.shard_states.len(), 4);
        assert_eq!(snapshot.slot_snapshot.num_shards, 4);
    }

    #[test]
    fn test_validation() {
        let cp = create_test_control_plane(4);

        let errors = cp.validate();
        assert!(errors.is_empty(), "Errors: {:?}", errors);
    }

    #[test]
    fn test_add_multiple_shards() {
        let cp = create_test_control_plane(2);

        // Add shards one by one
        let r1 = cp.add_shard().unwrap();
        assert_eq!(r1.shard_id, 2);

        let r2 = cp.add_shard().unwrap();
        assert_eq!(r2.shard_id, 3);

        let r3 = cp.add_shard().unwrap();
        assert_eq!(r3.shard_id, 4);

        assert_eq!(cp.active_shard_count(), 5);

        // Validate consistency
        let errors = cp.validate();
        assert!(errors.is_empty(), "Errors: {:?}", errors);
    }
}
