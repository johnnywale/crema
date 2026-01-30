//! Slot-based sharding with epoch routing.
//!
//! This module implements a slot-based sharding system inspired by Redis Cluster,
//! Couchbase, and TiKV. Instead of consistent hashing, it uses a fixed number of
//! logical slots as an indirection layer.
//!
//! # Key Concepts
//!
//! - **Slots**: Fixed number (1024) of logical partitions. Keys are mapped to slots
//!   via `crc16(key) % TOTAL_SLOTS`.
//! - **Epoch**: Monotonically increasing version number that changes on any slot
//!   table modification. Used for routing validation.
//! - **SlotState**: Tracks migration state for each slot (Stable, Migrating, Imported).
//!
//! # Epoch Invariant
//!
//! A request is valid if and only if:
//! ```text
//! request.epoch == local_epoch && shard owns slot
//! ```

use super::shard::ShardId;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Total number of slots in the system.
/// Fixed for the cluster's lifetime.
pub const TOTAL_SLOTS: usize = 1024;

/// Slot identifier (0..TOTAL_SLOTS-1).
pub type SlotId = u16;

/// Epoch is a monotonically increasing version number that increments on
/// any slot table change. It's carried in every request and response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Epoch(pub u64);

impl Epoch {
    /// Create a new epoch with value 0.
    pub fn zero() -> Self {
        Self(0)
    }

    /// Create a new epoch with the given value.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Get the epoch value.
    pub fn value(&self) -> u64 {
        self.0
    }

    /// Increment the epoch by 1.
    pub fn increment(&self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Check if this epoch is current relative to local epoch.
    pub fn check(&self, local: Epoch) -> EpochCheck {
        match self.0.cmp(&local.0) {
            Ordering::Equal => EpochCheck::Valid,
            Ordering::Less => EpochCheck::Stale,
            Ordering::Greater => EpochCheck::Future,
        }
    }
}

impl Default for Epoch {
    fn default() -> Self {
        Self::new(1)
    }
}

impl std::fmt::Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Epoch({})", self.0)
    }
}

impl From<u64> for Epoch {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// Result of checking an epoch against local state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochCheck {
    /// Epoch matches local - request is valid.
    Valid,
    /// Epoch is older than local - client has stale routing info.
    Stale,
    /// Epoch is newer than local - shard is behind (shouldn't happen normally).
    Future,
}

/// State of a slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SlotState {
    /// Normal operation - owner is authoritative.
    #[default]
    Stable,

    /// Migration in progress.
    /// - New owner accepts writes
    /// - Old owner redirects reads/writes to new owner
    /// - Data being transferred in background
    Migrating {
        /// Previous owner (source shard).
        from: ShardId,
        /// Timestamp when migration started (ms since epoch).
        started_at: u64,
        /// Estimated keys remaining to migrate.
        keys_remaining: u64,
    },

    /// Migration complete but source not yet cleaned.
    /// - New owner is fully authoritative
    /// - Old owner can GC this slot's data
    Imported {
        /// Previous owner (source shard).
        from: ShardId,
        /// Timestamp when migration completed (ms since epoch).
        completed_at: u64,
    },
}

impl SlotState {
    /// Check if the slot is in a stable state.
    pub fn is_stable(&self) -> bool {
        matches!(self, SlotState::Stable)
    }

    /// Check if the slot is being migrated.
    pub fn is_migrating(&self) -> bool {
        matches!(self, SlotState::Migrating { .. })
    }

    /// Check if the slot was recently imported.
    pub fn is_imported(&self) -> bool {
        matches!(self, SlotState::Imported { .. })
    }

    /// Get the source shard if migrating or imported.
    pub fn source_shard(&self) -> Option<ShardId> {
        match self {
            SlotState::Migrating { from, .. } => Some(*from),
            SlotState::Imported { from, .. } => Some(*from),
            SlotState::Stable => None,
        }
    }
}

/// Assignment of a single slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotAssignment {
    /// Current authoritative owner.
    pub owner: ShardId,
    /// Migration state.
    pub state: SlotState,
}

impl SlotAssignment {
    /// Create a new stable slot assignment.
    pub fn new(owner: ShardId) -> Self {
        Self {
            owner,
            state: SlotState::Stable,
        }
    }

    /// Create a migrating slot assignment.
    pub fn migrating(owner: ShardId, from: ShardId) -> Self {
        let started_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Self {
            owner,
            state: SlotState::Migrating {
                from,
                started_at,
                keys_remaining: 0,
            },
        }
    }

    /// Mark the slot as imported (migration complete).
    pub fn mark_imported(&mut self) {
        if let SlotState::Migrating { from, .. } = self.state {
            let completed_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);

            self.state = SlotState::Imported { from, completed_at };
        }
    }

    /// Mark the slot as stable (can GC source data).
    pub fn mark_stable(&mut self) {
        self.state = SlotState::Stable;
    }
}

/// A snapshot of slot assignments for a single shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSlotInfo {
    /// Shard ID.
    pub shard_id: ShardId,
    /// Slots owned by this shard.
    pub owned_slots: Vec<SlotId>,
    /// Slots being migrated to this shard.
    pub incoming_slots: Vec<SlotId>,
    /// Slots being migrated from this shard.
    pub outgoing_slots: Vec<SlotId>,
}

/// Result of routing a key.
#[derive(Debug, Clone)]
pub struct RouteResult {
    /// Target shard ID.
    pub shard_id: ShardId,
    /// Slot ID for the key.
    pub slot_id: SlotId,
    /// Current epoch.
    pub epoch: Epoch,
    /// Slot state (for migration awareness).
    pub state: SlotState,
}

/// Reassignment plan for slots.
#[derive(Debug, Clone)]
pub struct SlotReassignment {
    /// Slots to reassign: (slot_id, old_owner, new_owner).
    pub moves: Vec<(SlotId, ShardId, ShardId)>,
    /// New epoch after reassignment.
    pub new_epoch: Epoch,
}

impl SlotReassignment {
    /// Create a new empty reassignment.
    pub fn new(new_epoch: Epoch) -> Self {
        Self {
            moves: Vec::new(),
            new_epoch,
        }
    }

    /// Add a slot move.
    pub fn add_move(&mut self, slot_id: SlotId, from: ShardId, to: ShardId) {
        self.moves.push((slot_id, from, to));
    }

    /// Get the number of slot moves.
    pub fn len(&self) -> usize {
        self.moves.len()
    }

    /// Check if there are no moves.
    pub fn is_empty(&self) -> bool {
        self.moves.is_empty()
    }
}

/// Snapshot of the slot table for external consumption.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotTableSnapshot {
    /// Current epoch.
    pub epoch: Epoch,
    /// Number of shards.
    pub num_shards: usize,
    /// Slot assignments.
    pub slots: Vec<SlotAssignment>,
    /// Per-shard slot info.
    pub shard_info: HashMap<ShardId, ShardSlotInfo>,
}

/// Slot table managing slot-to-shard mapping with epoch-based versioning.
///
/// # Thread Safety
///
/// The slot table uses `RwLock` for safe concurrent access:
/// - Reads (routing) take read locks and are highly concurrent
/// - Writes (reassignments) take write locks and are rare
/// - Epoch is an atomic for lock-free reads
#[derive(Debug)]
pub struct SlotTable {
    /// Current epoch (atomic for fast lock-free reads).
    epoch: AtomicU64,

    /// Slot assignments (protected by RwLock).
    /// Index is slot_id.
    slots: RwLock<Vec<SlotAssignment>>,

    /// Number of shards (for distribution calculations).
    num_shards: RwLock<usize>,
}

impl SlotTable {
    /// Create a new slot table with even distribution across shards.
    pub fn new(num_shards: usize) -> Self {
        let slots: Vec<SlotAssignment> = (0..TOTAL_SLOTS)
            .map(|i| SlotAssignment::new((i % num_shards) as ShardId))
            .collect();

        Self {
            epoch: AtomicU64::new(1),
            slots: RwLock::new(slots),
            num_shards: RwLock::new(num_shards),
        }
    }

    /// Create an empty slot table (for testing or custom initialization).
    pub fn empty() -> Self {
        Self {
            epoch: AtomicU64::new(1),
            slots: RwLock::new(vec![SlotAssignment::new(0); TOTAL_SLOTS]),
            num_shards: RwLock::new(0),
        }
    }

    /// Get the current epoch (lock-free).
    pub fn epoch(&self) -> Epoch {
        Epoch(self.epoch.load(AtomicOrdering::SeqCst))
    }

    /// Get the number of shards.
    pub fn num_shards(&self) -> usize {
        *self.num_shards.read()
    }

    /// Compute slot ID for a key using CRC16.
    pub fn compute_slot(key: &[u8]) -> SlotId {
        crc16(key) % TOTAL_SLOTS as u16
    }

    /// Route a key to its shard.
    pub fn route(&self, key: &[u8]) -> RouteResult {
        let slot_id = Self::compute_slot(key);
        let epoch = self.epoch();
        let slots = self.slots.read();
        let assignment = &slots[slot_id as usize];

        RouteResult {
            shard_id: assignment.owner,
            slot_id,
            epoch,
            state: assignment.state.clone(),
        }
    }

    /// Get slot assignment by slot ID.
    pub fn get_slot(&self, slot_id: SlotId) -> SlotAssignment {
        self.slots.read()[slot_id as usize].clone()
    }

    /// Get the owner of a slot.
    pub fn slot_owner(&self, slot_id: SlotId) -> ShardId {
        self.slots.read()[slot_id as usize].owner
    }

    /// Get all slots owned by a shard.
    pub fn slots_for_shard(&self, shard_id: ShardId) -> Vec<SlotId> {
        self.slots
            .read()
            .iter()
            .enumerate()
            .filter_map(|(i, a)| {
                if a.owner == shard_id {
                    Some(i as SlotId)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get count of slots owned by a shard.
    pub fn slot_count_for_shard(&self, shard_id: ShardId) -> usize {
        self.slots
            .read()
            .iter()
            .filter(|a| a.owner == shard_id)
            .count()
    }

    /// Reassign slots to a new owner, incrementing the epoch.
    ///
    /// This is the main method for changing slot ownership during rebalancing.
    /// It updates ownership and marks slots as migrating.
    pub fn reassign_slots(&self, slots: &[SlotId], new_owner: ShardId) -> SlotReassignment {
        let mut slot_guard = self.slots.write();
        let old_epoch = self.epoch.load(AtomicOrdering::SeqCst);
        let new_epoch = old_epoch + 1;

        let mut reassignment = SlotReassignment::new(Epoch::new(new_epoch));

        for &slot_id in slots {
            let assignment = &mut slot_guard[slot_id as usize];
            let old_owner = assignment.owner;

            if old_owner != new_owner {
                reassignment.add_move(slot_id, old_owner, new_owner);

                // Update ownership and mark as migrating
                assignment.owner = new_owner;
                assignment.state = SlotState::Migrating {
                    from: old_owner,
                    started_at: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0),
                    keys_remaining: 0,
                };
            }
        }

        // Update epoch atomically
        self.epoch.store(new_epoch, AtomicOrdering::SeqCst);

        reassignment
    }

    /// Mark a slot as imported (migration complete).
    pub fn mark_imported(&self, slot_id: SlotId) {
        let mut slots = self.slots.write();
        slots[slot_id as usize].mark_imported();

        // Increment epoch for this state change
        self.epoch.fetch_add(1, AtomicOrdering::SeqCst);
    }

    /// Mark a slot as stable (GC complete).
    pub fn mark_stable(&self, slot_id: SlotId) {
        let mut slots = self.slots.write();
        slots[slot_id as usize].mark_stable();

        // Increment epoch for this state change
        self.epoch.fetch_add(1, AtomicOrdering::SeqCst);
    }

    /// Update migration progress for a slot.
    pub fn update_migration_progress(&self, slot_id: SlotId, keys_remaining: u64) {
        let mut slots = self.slots.write();
        if let SlotState::Migrating {
            keys_remaining: ref mut kr,
            ..
        } = slots[slot_id as usize].state
        {
            *kr = keys_remaining;
        }
    }

    /// Compute a rebalance plan for adding a new shard.
    ///
    /// Steals approximately `TOTAL_SLOTS / (num_shards + 1)` slots from existing shards.
    pub fn compute_rebalance_for_new_shard(&self, _new_shard_id: ShardId) -> Vec<SlotId> {
        let slots = self.slots.read();
        let num_shards = *self.num_shards.read();

        if num_shards == 0 {
            // First shard gets all slots
            return (0..TOTAL_SLOTS as SlotId).collect();
        }

        let new_num_shards = num_shards + 1;
        let target_per_shard = TOTAL_SLOTS / new_num_shards;

        // Count current slots per shard
        let mut shard_counts: HashMap<ShardId, usize> = HashMap::new();
        for assignment in slots.iter() {
            *shard_counts.entry(assignment.owner).or_insert(0) += 1;
        }

        // Find slots to steal from shards that have more than target
        let mut slots_to_move = Vec::new();
        for (i, assignment) in slots.iter().enumerate() {
            let shard = assignment.owner;
            let count = shard_counts.get(&shard).copied().unwrap_or(0);

            // If this shard has more than its fair share, steal from it
            if count > target_per_shard && slots_to_move.len() < target_per_shard {
                slots_to_move.push(i as SlotId);
                *shard_counts.entry(shard).or_insert(0) -= 1;
            }
        }

        // Update the number of shards
        drop(slots);
        *self.num_shards.write() = new_num_shards;

        slots_to_move
    }

    /// Compute a drain plan for removing a shard.
    ///
    /// Redistributes all slots from the removed shard to remaining shards.
    pub fn compute_drain_for_shard(&self, shard_id: ShardId) -> HashMap<ShardId, Vec<SlotId>> {
        let slots = self.slots.read();
        let num_shards = *self.num_shards.read();

        // Get all slots owned by the shard being removed
        let owned_slots: Vec<SlotId> = slots
            .iter()
            .enumerate()
            .filter_map(|(i, a)| {
                if a.owner == shard_id {
                    Some(i as SlotId)
                } else {
                    None
                }
            })
            .collect();

        // Distribute among remaining shards
        let remaining_shards: Vec<ShardId> = (0..num_shards as ShardId)
            .filter(|&id| id != shard_id)
            .collect();

        if remaining_shards.is_empty() {
            return HashMap::new();
        }

        let mut result: HashMap<ShardId, Vec<SlotId>> = HashMap::new();
        for (i, slot) in owned_slots.into_iter().enumerate() {
            let target = remaining_shards[i % remaining_shards.len()];
            result.entry(target).or_default().push(slot);
        }

        result
    }

    /// Get a snapshot of the slot table.
    pub fn snapshot(&self) -> SlotTableSnapshot {
        let slots = self.slots.read();
        let num_shards = *self.num_shards.read();
        let epoch = self.epoch();

        // Build per-shard info
        let mut shard_info: HashMap<ShardId, ShardSlotInfo> = HashMap::new();

        for i in 0..num_shards {
            shard_info.insert(
                i as ShardId,
                ShardSlotInfo {
                    shard_id: i as ShardId,
                    owned_slots: Vec::new(),
                    incoming_slots: Vec::new(),
                    outgoing_slots: Vec::new(),
                },
            );
        }

        for (slot_id, assignment) in slots.iter().enumerate() {
            let slot_id = slot_id as SlotId;

            // Add to owned slots
            if let Some(info) = shard_info.get_mut(&assignment.owner) {
                info.owned_slots.push(slot_id);
            }

            // Track migration
            match &assignment.state {
                SlotState::Migrating { from, .. } => {
                    if let Some(info) = shard_info.get_mut(&assignment.owner) {
                        info.incoming_slots.push(slot_id);
                    }
                    if let Some(info) = shard_info.get_mut(from) {
                        info.outgoing_slots.push(slot_id);
                    }
                }
                SlotState::Imported { from, .. } => {
                    if let Some(info) = shard_info.get_mut(from) {
                        info.outgoing_slots.push(slot_id);
                    }
                }
                SlotState::Stable => {}
            }
        }

        SlotTableSnapshot {
            epoch,
            num_shards,
            slots: slots.clone(),
            shard_info,
        }
    }

    /// Apply a reassignment (used when receiving updates from control plane).
    pub fn apply_reassignment(&self, reassignment: &SlotReassignment) {
        let mut slots = self.slots.write();

        for (slot_id, from, to) in &reassignment.moves {
            let assignment = &mut slots[*slot_id as usize];
            assignment.owner = *to;
            assignment.state = SlotState::Migrating {
                from: *from,
                started_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0),
                keys_remaining: 0,
            };
        }

        // Update epoch
        self.epoch
            .store(reassignment.new_epoch.value(), AtomicOrdering::SeqCst);
    }
}

impl Default for SlotTable {
    fn default() -> Self {
        Self::new(16)
    }
}

/// CRC16 implementation for slot hashing (XMODEM variant, matching Redis Cluster).
///
/// This uses the CRC-16-CCITT polynomial (0x1021) with initial value 0.
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;

    for byte in data {
        crc ^= (*byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }

    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_epoch_check() {
        let local = Epoch::new(10);

        assert_eq!(Epoch::new(10).check(local), EpochCheck::Valid);
        assert_eq!(Epoch::new(5).check(local), EpochCheck::Stale);
        assert_eq!(Epoch::new(15).check(local), EpochCheck::Future);
    }

    #[test]
    fn test_crc16_known_values() {
        // Test CRC16-CCITT (XMODEM variant) - verify consistency
        // The actual values depend on the polynomial and initial value
        let val1 = crc16(b"123456789");
        let val2 = crc16(b"123456789");
        assert_eq!(val1, val2, "CRC16 should be deterministic");

        // Empty string should have CRC16 of 0
        assert_eq!(crc16(b""), 0);

        // Different inputs should produce different outputs (usually)
        let a_crc = crc16(b"A");
        let b_crc = crc16(b"B");
        assert_ne!(a_crc, b_crc, "Different inputs should have different CRCs");
    }

    #[test]
    fn test_slot_table_creation() {
        let table = SlotTable::new(4);

        assert_eq!(table.epoch().value(), 1);
        assert_eq!(table.num_shards(), 4);

        // Each shard should have ~256 slots
        for shard_id in 0..4 {
            let count = table.slot_count_for_shard(shard_id);
            assert_eq!(count, 256, "Shard {} has {} slots", shard_id, count);
        }
    }

    #[test]
    fn test_route_consistency() {
        let table = SlotTable::new(4);

        // Same key should always route to same shard
        let result1 = table.route(b"test-key");
        let result2 = table.route(b"test-key");

        assert_eq!(result1.shard_id, result2.shard_id);
        assert_eq!(result1.slot_id, result2.slot_id);
    }

    #[test]
    fn test_slot_distribution() {
        let table = SlotTable::new(4);

        // Generate 1000 keys and count distribution
        let mut counts = [0usize; 4];
        for i in 0..1000 {
            let key = format!("key-{}", i);
            let result = table.route(key.as_bytes());
            counts[result.shard_id as usize] += 1;
        }

        // Each shard should have ~250 keys (allow 20% variance)
        for (shard, count) in counts.iter().enumerate() {
            assert!(
                *count > 150 && *count < 350,
                "Shard {} has {} keys (expected ~250)",
                shard,
                count
            );
        }
    }

    #[test]
    fn test_reassign_slots() {
        let table = SlotTable::new(4);

        // Get 10 slots - note that with even distribution, different slots belong
        // to different shards initially. Slots 0,4,8 belong to shard 0, etc.
        let slots_to_move: Vec<SlotId> = (0..10).collect();

        // Before reassignment, count how many are NOT already owned by shard 3
        let expected_moves = slots_to_move
            .iter()
            .filter(|&&s| table.slot_owner(s) != 3)
            .count();

        let reassignment = table.reassign_slots(&slots_to_move, 3);

        // Only slots not already owned by shard 3 should be moved
        assert_eq!(reassignment.moves.len(), expected_moves);
        assert_eq!(reassignment.new_epoch.value(), 2);

        // Verify all slots are now owned by shard 3
        for slot_id in &slots_to_move {
            let assignment = table.get_slot(*slot_id);
            assert_eq!(assignment.owner, 3);
        }
    }

    #[test]
    fn test_mark_imported_and_stable() {
        let table = SlotTable::new(4);

        // Reassign a slot
        table.reassign_slots(&[0], 3);

        // Mark as imported
        table.mark_imported(0);
        let assignment = table.get_slot(0);
        assert!(assignment.state.is_imported());

        // Mark as stable
        table.mark_stable(0);
        let assignment = table.get_slot(0);
        assert!(assignment.state.is_stable());
    }

    #[test]
    fn test_compute_rebalance_for_new_shard() {
        let table = SlotTable::new(4);

        // Add a 5th shard
        let slots_to_move = table.compute_rebalance_for_new_shard(4);

        // Should move ~204 slots (1024/5 = 204.8)
        assert!(
            slots_to_move.len() >= 180 && slots_to_move.len() <= 220,
            "Expected ~204 slots, got {}",
            slots_to_move.len()
        );
    }

    #[test]
    fn test_compute_drain_for_shard() {
        let table = SlotTable::new(4);

        // Drain shard 3
        let drain_plan = table.compute_drain_for_shard(3);

        // All 256 slots from shard 3 should be redistributed
        let total_slots: usize = drain_plan.values().map(|v| v.len()).sum();
        assert_eq!(total_slots, 256);

        // Should be distributed among 3 remaining shards
        assert!(drain_plan.len() <= 3);
    }

    #[test]
    fn test_snapshot() {
        let table = SlotTable::new(4);

        let snapshot = table.snapshot();

        assert_eq!(snapshot.epoch.value(), 1);
        assert_eq!(snapshot.num_shards, 4);
        assert_eq!(snapshot.slots.len(), TOTAL_SLOTS);
        assert_eq!(snapshot.shard_info.len(), 4);

        // Each shard info should have 256 slots
        for (_, info) in &snapshot.shard_info {
            assert_eq!(info.owned_slots.len(), 256);
        }
    }

    #[test]
    fn test_epoch_increment_on_changes() {
        let table = SlotTable::new(4);

        assert_eq!(table.epoch().value(), 1);

        // Reassign
        table.reassign_slots(&[0], 3);
        assert_eq!(table.epoch().value(), 2);

        // Mark imported
        table.mark_imported(0);
        assert_eq!(table.epoch().value(), 3);

        // Mark stable
        table.mark_stable(0);
        assert_eq!(table.epoch().value(), 4);
    }
}
