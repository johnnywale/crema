//! Memberlist integration for Multi-Raft shard leader gossip.
//!
//! This module provides:
//! - Epoch-based shard leader tracking
//! - Debounced shard leader broadcasting
//! - Shard leader invalidation on node failures

use crate::cluster::discovery::ShardLeaderInfo;
use crate::types::NodeId;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::shard::ShardId;

/// Epoch-based shard leader tracker.
///
/// Tracks shard leaders with monotonically increasing epochs to handle
/// out-of-order gossip updates. Updates with lower epochs are rejected.
#[derive(Debug)]
pub struct ShardLeaderTracker {
    /// Maps shard ID to (leader_id, epoch).
    leaders: Mutex<HashMap<ShardId, ShardLeaderInfo>>,
    /// Counter for generating epochs for local leader changes.
    local_epoch_counter: AtomicU64,
    /// This node's ID.
    node_id: NodeId,
}

impl ShardLeaderTracker {
    /// Create a new shard leader tracker.
    pub fn new(node_id: NodeId) -> Self {
        Self {
            leaders: Mutex::new(HashMap::new()),
            local_epoch_counter: AtomicU64::new(1),
            node_id,
        }
    }

    /// Set a shard leader if the epoch is newer than the current one.
    ///
    /// Returns `true` if the update was applied, `false` if it was rejected
    /// due to a stale epoch.
    pub fn set_leader_if_newer(&self, shard_id: ShardId, leader_id: NodeId, epoch: u64) -> bool {
        let mut leaders = self.leaders.lock();

        if let Some(current) = leaders.get(&shard_id) {
            if epoch <= current.epoch {
                // Reject stale update
                tracing::debug!(
                    shard_id = shard_id,
                    current_epoch = current.epoch,
                    new_epoch = epoch,
                    "Rejecting stale shard leader update"
                );
                return false;
            }
        }

        leaders.insert(shard_id, ShardLeaderInfo::new(leader_id, epoch));
        tracing::debug!(
            shard_id = shard_id,
            leader_id = leader_id,
            epoch = epoch,
            "Updated shard leader"
        );
        true
    }

    /// Set a shard leader for a local leader change.
    ///
    /// Automatically increments the epoch counter.
    /// Returns the new epoch.
    pub fn set_local_leader(&self, shard_id: ShardId, leader_id: NodeId) -> u64 {
        let epoch = self.local_epoch_counter.fetch_add(1, Ordering::SeqCst);
        let mut leaders = self.leaders.lock();
        leaders.insert(shard_id, ShardLeaderInfo::new(leader_id, epoch));
        tracing::debug!(
            shard_id = shard_id,
            leader_id = leader_id,
            epoch = epoch,
            "Set local shard leader"
        );
        epoch
    }

    /// Get the current leader for a shard.
    pub fn get_leader(&self, shard_id: ShardId) -> Option<NodeId> {
        self.leaders.lock().get(&shard_id).map(|info| info.leader_id)
    }

    /// Get the current epoch for a shard.
    pub fn get_epoch(&self, shard_id: ShardId) -> Option<u64> {
        self.leaders.lock().get(&shard_id).map(|info| info.epoch)
    }

    /// Get all shard leaders.
    pub fn all_leaders(&self) -> HashMap<ShardId, ShardLeaderInfo> {
        self.leaders.lock().clone()
    }

    /// Get leaders for shards where this node is the leader.
    pub fn local_leaders(&self) -> HashMap<ShardId, ShardLeaderInfo> {
        self.leaders
            .lock()
            .iter()
            .filter(|(_, info)| info.leader_id == self.node_id)
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    /// Clear the leader for a shard.
    pub fn clear_leader(&self, shard_id: ShardId) {
        self.leaders.lock().remove(&shard_id);
        tracing::debug!(shard_id = shard_id, "Cleared shard leader");
    }

    /// Invalidate all shards where the given node was the leader.
    ///
    /// Called when a node fails or leaves the cluster.
    /// Returns the list of invalidated shard IDs.
    pub fn invalidate_leader_for_node(&self, node_id: NodeId) -> Vec<ShardId> {
        let mut leaders = self.leaders.lock();
        let invalidated: Vec<ShardId> = leaders
            .iter()
            .filter(|(_, info)| info.leader_id == node_id)
            .map(|(shard_id, _)| *shard_id)
            .collect();

        for shard_id in &invalidated {
            leaders.remove(shard_id);
        }

        if !invalidated.is_empty() {
            tracing::info!(
                node_id = node_id,
                shards = ?invalidated,
                "Invalidated shard leaders for failed node"
            );
        }

        invalidated
    }

    /// Get the number of tracked shards.
    pub fn shard_count(&self) -> usize {
        self.leaders.lock().len()
    }
}

/// Debounced shard leader broadcaster.
///
/// Batches rapid shard leader updates to reduce gossip overhead.
/// Updates are collected and broadcast after the debounce interval.
#[derive(Debug)]
pub struct ShardLeaderBroadcaster {
    /// Pending updates to be broadcast.
    pending_updates: Mutex<HashMap<ShardId, ShardLeaderInfo>>,
    /// Debounce interval.
    debounce_interval: Duration,
    /// Time of last broadcast.
    last_broadcast: Mutex<Instant>,
}

impl ShardLeaderBroadcaster {
    /// Create a new broadcaster with the given debounce interval.
    pub fn new(debounce_interval: Duration) -> Self {
        Self {
            pending_updates: Mutex::new(HashMap::new()),
            debounce_interval,
            last_broadcast: Mutex::new(Instant::now()),
        }
    }

    /// Queue a shard leader update for broadcasting.
    ///
    /// The update will be batched with other pending updates and
    /// broadcast after the debounce interval elapses.
    pub fn queue_update(&self, shard_id: ShardId, leader_id: NodeId, epoch: u64) {
        self.pending_updates
            .lock()
            .insert(shard_id, ShardLeaderInfo::new(leader_id, epoch));
        tracing::debug!(
            shard_id = shard_id,
            leader_id = leader_id,
            epoch = epoch,
            "Queued shard leader update for broadcast"
        );
    }

    /// Check if there are pending updates.
    pub fn has_pending_updates(&self) -> bool {
        !self.pending_updates.lock().is_empty()
    }

    /// Get the number of pending updates.
    pub fn pending_count(&self) -> usize {
        self.pending_updates.lock().len()
    }

    /// Check if it's time to broadcast (debounce interval elapsed).
    pub fn should_broadcast(&self) -> bool {
        self.last_broadcast.lock().elapsed() >= self.debounce_interval
            && self.has_pending_updates()
    }

    /// Take all pending updates for broadcasting.
    ///
    /// Returns `None` if the debounce interval hasn't elapsed yet.
    /// Returns `Some(updates)` and resets the timer if ready to broadcast.
    pub fn take_updates_if_ready(&self) -> Option<HashMap<ShardId, ShardLeaderInfo>> {
        let elapsed = self.last_broadcast.lock().elapsed();
        if elapsed < self.debounce_interval {
            return None;
        }

        let mut pending = self.pending_updates.lock();
        if pending.is_empty() {
            return None;
        }

        let updates = std::mem::take(&mut *pending);
        *self.last_broadcast.lock() = Instant::now();

        tracing::debug!(
            count = updates.len(),
            "Broadcasting batched shard leader updates"
        );

        Some(updates)
    }

    /// Force take all pending updates regardless of debounce interval.
    ///
    /// Useful for shutdown or immediate broadcast scenarios.
    pub fn force_take_updates(&self) -> HashMap<ShardId, ShardLeaderInfo> {
        let mut pending = self.pending_updates.lock();
        let updates = std::mem::take(&mut *pending);
        *self.last_broadcast.lock() = Instant::now();
        updates
    }

    /// Clear all pending updates without broadcasting.
    pub fn clear_pending(&self) {
        self.pending_updates.lock().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracker_set_leader_if_newer() {
        let tracker = ShardLeaderTracker::new(1);

        // First update should succeed
        assert!(tracker.set_leader_if_newer(0, 1, 1));
        assert_eq!(tracker.get_leader(0), Some(1));
        assert_eq!(tracker.get_epoch(0), Some(1));

        // Older epoch should be rejected
        assert!(!tracker.set_leader_if_newer(0, 2, 1));
        assert_eq!(tracker.get_leader(0), Some(1)); // Still node 1

        // Same epoch should be rejected
        assert!(!tracker.set_leader_if_newer(0, 2, 1));
        assert_eq!(tracker.get_leader(0), Some(1));

        // Newer epoch should succeed
        assert!(tracker.set_leader_if_newer(0, 2, 2));
        assert_eq!(tracker.get_leader(0), Some(2));
        assert_eq!(tracker.get_epoch(0), Some(2));
    }

    #[test]
    fn test_tracker_set_local_leader() {
        let tracker = ShardLeaderTracker::new(1);

        let epoch1 = tracker.set_local_leader(0, 1);
        let epoch2 = tracker.set_local_leader(1, 1);
        let epoch3 = tracker.set_local_leader(0, 1);

        // Epochs should be monotonically increasing
        assert!(epoch2 > epoch1);
        assert!(epoch3 > epoch2);

        assert_eq!(tracker.get_leader(0), Some(1));
        assert_eq!(tracker.get_leader(1), Some(1));
    }

    #[test]
    fn test_tracker_invalidate_leader_for_node() {
        let tracker = ShardLeaderTracker::new(1);

        tracker.set_leader_if_newer(0, 2, 1);
        tracker.set_leader_if_newer(1, 2, 1);
        tracker.set_leader_if_newer(2, 3, 1);
        tracker.set_leader_if_newer(3, 2, 1);

        // Invalidate node 2's shards
        let invalidated = tracker.invalidate_leader_for_node(2);

        assert_eq!(invalidated.len(), 3);
        assert!(invalidated.contains(&0));
        assert!(invalidated.contains(&1));
        assert!(invalidated.contains(&3));

        // Node 2's shards should be cleared
        assert_eq!(tracker.get_leader(0), None);
        assert_eq!(tracker.get_leader(1), None);
        assert_eq!(tracker.get_leader(3), None);

        // Node 3's shard should remain
        assert_eq!(tracker.get_leader(2), Some(3));
    }

    #[test]
    fn test_tracker_local_leaders() {
        let tracker = ShardLeaderTracker::new(1);

        tracker.set_leader_if_newer(0, 1, 1);
        tracker.set_leader_if_newer(1, 2, 1);
        tracker.set_leader_if_newer(2, 1, 1);

        let local = tracker.local_leaders();
        assert_eq!(local.len(), 2);
        assert!(local.contains_key(&0));
        assert!(local.contains_key(&2));
        assert!(!local.contains_key(&1));
    }

    #[test]
    fn test_broadcaster_queue_and_take() {
        let broadcaster = ShardLeaderBroadcaster::new(Duration::from_millis(100));

        // Queue some updates
        broadcaster.queue_update(0, 1, 1);
        broadcaster.queue_update(1, 2, 1);

        assert!(broadcaster.has_pending_updates());
        assert_eq!(broadcaster.pending_count(), 2);

        // Should not broadcast immediately (debounce)
        assert!(broadcaster.take_updates_if_ready().is_none());

        // Wait for debounce interval
        std::thread::sleep(Duration::from_millis(150));

        // Now should be able to take updates
        let updates = broadcaster.take_updates_if_ready().unwrap();
        assert_eq!(updates.len(), 2);
        assert!(!broadcaster.has_pending_updates());
    }

    #[test]
    fn test_broadcaster_force_take() {
        let broadcaster = ShardLeaderBroadcaster::new(Duration::from_secs(10)); // Long debounce

        broadcaster.queue_update(0, 1, 1);
        broadcaster.queue_update(1, 2, 1);

        // Force take should work regardless of debounce
        let updates = broadcaster.force_take_updates();
        assert_eq!(updates.len(), 2);
        assert!(!broadcaster.has_pending_updates());
    }

    #[test]
    fn test_broadcaster_batching() {
        let broadcaster = ShardLeaderBroadcaster::new(Duration::from_millis(50));

        // Queue multiple updates for the same shard
        broadcaster.queue_update(0, 1, 1);
        broadcaster.queue_update(0, 2, 2);
        broadcaster.queue_update(0, 3, 3);

        // Only the last update should be kept
        let updates = broadcaster.force_take_updates();
        assert_eq!(updates.len(), 1);
        let info = updates.get(&0).unwrap();
        assert_eq!(info.leader_id, 3);
        assert_eq!(info.epoch, 3);
    }
}
