//! Global shard registry for stable shard metadata.
//!
//! The shard registry maintains metadata about all shards in the cluster,
//! independent of which nodes currently host them. This ensures shard IDs
//! are stable and don't change when nodes join/leave.

use crate::types::NodeId;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

use super::shard::ShardId;

/// Lifecycle state of a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ShardLifecycleState {
    /// Shard exists but has no replicas yet.
    Unassigned,
    /// Shard is being initialized on nodes.
    Initializing,
    /// Shard is active and serving traffic.
    Active,
    /// Shard is being migrated to new nodes.
    Migrating,
    /// Shard is being decommissioned.
    Decommissioning,
}

impl std::fmt::Display for ShardLifecycleState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardLifecycleState::Unassigned => write!(f, "unassigned"),
            ShardLifecycleState::Initializing => write!(f, "initializing"),
            ShardLifecycleState::Active => write!(f, "active"),
            ShardLifecycleState::Migrating => write!(f, "migrating"),
            ShardLifecycleState::Decommissioning => write!(f, "decommissioning"),
        }
    }
}

/// Metadata about a shard, independent of node placement.
#[derive(Debug, Clone)]
pub struct ShardMetadata {
    /// Unique shard identifier (stable, never changes).
    pub shard_id: ShardId,
    /// When this shard was created.
    pub created_at: Instant,
    /// Current lifecycle state.
    pub state: ShardLifecycleState,
    /// Nodes currently hosting this shard (from placement).
    pub replica_nodes: Vec<NodeId>,
    /// Current primary node (Raft leader for this shard).
    pub primary_node: Option<NodeId>,
    /// Version for consistency (incremented on changes).
    pub version: u64,
    /// Epoch for leader election ordering.
    pub leader_epoch: u64,
}

impl ShardMetadata {
    /// Create new shard metadata.
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            created_at: Instant::now(),
            state: ShardLifecycleState::Unassigned,
            replica_nodes: Vec::new(),
            primary_node: None,
            version: 1,
            leader_epoch: 0,
        }
    }

    /// Check if the shard is active and serving traffic.
    pub fn is_active(&self) -> bool {
        self.state == ShardLifecycleState::Active
    }

    /// Check if the shard is being migrated.
    pub fn is_migrating(&self) -> bool {
        self.state == ShardLifecycleState::Migrating
    }

    /// Check if a node is a replica for this shard.
    pub fn has_replica(&self, node_id: NodeId) -> bool {
        self.replica_nodes.contains(&node_id)
    }

    /// Check if a node is the primary for this shard.
    pub fn is_primary(&self, node_id: NodeId) -> bool {
        self.primary_node == Some(node_id)
    }

    /// Get the number of replicas.
    pub fn replica_count(&self) -> usize {
        self.replica_nodes.len()
    }
}

/// Global registry of shard metadata.
///
/// Shards exist logically in this registry even if no node currently hosts them.
/// This provides stability for shard IDs and prevents thundering herd issues
/// when routing caches are invalidated.
#[derive(Debug)]
pub struct ShardRegistry {
    /// Shard metadata indexed by shard ID.
    shards: RwLock<HashMap<ShardId, ShardMetadata>>,
    /// Total number of shards (fixed at cluster creation).
    num_shards: u32,
    /// Registry version (incremented on any change).
    version: RwLock<u64>,
}

impl ShardRegistry {
    /// Create a new shard registry with the specified number of shards.
    ///
    /// All shards are created in the `Unassigned` state initially.
    pub fn new(num_shards: u32) -> Self {
        let mut shards = HashMap::new();

        // Pre-create all shard metadata entries
        for shard_id in 0..num_shards {
            shards.insert(shard_id, ShardMetadata::new(shard_id));
        }

        Self {
            shards: RwLock::new(shards),
            num_shards,
            version: RwLock::new(1),
        }
    }

    /// Get the total number of shards.
    pub fn num_shards(&self) -> u32 {
        self.num_shards
    }

    /// Get the current registry version.
    pub fn version(&self) -> u64 {
        *self.version.read()
    }

    /// Get metadata for a specific shard.
    pub fn get(&self, shard_id: ShardId) -> Option<ShardMetadata> {
        self.shards.read().get(&shard_id).cloned()
    }

    /// Get metadata for all shards.
    pub fn all(&self) -> Vec<ShardMetadata> {
        self.shards.read().values().cloned().collect()
    }

    /// Get the replica nodes for a shard.
    pub fn get_replicas(&self, shard_id: ShardId) -> Vec<NodeId> {
        self.shards
            .read()
            .get(&shard_id)
            .map(|m| m.replica_nodes.clone())
            .unwrap_or_default()
    }

    /// Get shards in a specific state.
    pub fn shards_in_state(&self, state: ShardLifecycleState) -> Vec<ShardId> {
        self.shards
            .read()
            .iter()
            .filter(|(_, meta)| meta.state == state)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get shards hosted on a specific node.
    pub fn shards_on_node(&self, node_id: NodeId) -> Vec<ShardId> {
        self.shards
            .read()
            .iter()
            .filter(|(_, meta)| meta.has_replica(node_id))
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get shards where a node is the primary.
    pub fn primary_shards_on_node(&self, node_id: NodeId) -> Vec<ShardId> {
        self.shards
            .read()
            .iter()
            .filter(|(_, meta)| meta.is_primary(node_id))
            .map(|(id, _)| *id)
            .collect()
    }

    /// Update the state of a shard.
    pub fn set_state(&self, shard_id: ShardId, state: ShardLifecycleState) -> bool {
        let mut shards = self.shards.write();
        if let Some(meta) = shards.get_mut(&shard_id) {
            meta.state = state;
            meta.version += 1;
            *self.version.write() += 1;
            tracing::debug!(shard_id, ?state, "Shard state changed");
            true
        } else {
            false
        }
    }

    /// Update the replica nodes for a shard.
    pub fn set_replicas(&self, shard_id: ShardId, replicas: Vec<NodeId>) -> bool {
        let mut shards = self.shards.write();
        if let Some(meta) = shards.get_mut(&shard_id) {
            meta.replica_nodes = replicas;
            meta.version += 1;
            *self.version.write() += 1;
            tracing::debug!(shard_id, ?meta.replica_nodes, "Shard replicas updated");
            true
        } else {
            false
        }
    }

    /// Update the primary node for a shard.
    ///
    /// Returns the new epoch if successful, None if shard not found.
    pub fn set_primary(&self, shard_id: ShardId, primary: Option<NodeId>) -> Option<u64> {
        let mut shards = self.shards.write();
        if let Some(meta) = shards.get_mut(&shard_id) {
            meta.primary_node = primary;
            meta.leader_epoch += 1;
            meta.version += 1;
            *self.version.write() += 1;
            tracing::debug!(shard_id, ?primary, epoch = meta.leader_epoch, "Shard primary updated");
            Some(meta.leader_epoch)
        } else {
            None
        }
    }

    /// Update the primary node for a shard only if the epoch is newer.
    ///
    /// Returns true if the update was applied, false if rejected due to stale epoch.
    pub fn set_primary_if_newer(
        &self,
        shard_id: ShardId,
        primary: NodeId,
        epoch: u64,
    ) -> bool {
        let mut shards = self.shards.write();
        if let Some(meta) = shards.get_mut(&shard_id) {
            if epoch > meta.leader_epoch {
                meta.primary_node = Some(primary);
                meta.leader_epoch = epoch;
                meta.version += 1;
                *self.version.write() += 1;
                tracing::debug!(
                    shard_id,
                    ?primary,
                    epoch,
                    "Shard primary updated with newer epoch"
                );
                true
            } else {
                tracing::debug!(
                    shard_id,
                    ?primary,
                    epoch,
                    current_epoch = meta.leader_epoch,
                    "Rejected stale primary update"
                );
                false
            }
        } else {
            false
        }
    }

    /// Update the replica nodes for a shard only if the epoch is newer.
    ///
    /// Returns true if the update was applied, false if rejected due to stale epoch.
    pub fn set_replicas_if_newer(
        &self,
        shard_id: ShardId,
        replicas: Vec<NodeId>,
        epoch: u64,
    ) -> bool {
        let mut shards = self.shards.write();
        if let Some(meta) = shards.get_mut(&shard_id) {
            if epoch > meta.leader_epoch {
                meta.replica_nodes = replicas;
                meta.leader_epoch = epoch;
                meta.version += 1;
                *self.version.write() += 1;
                tracing::debug!(
                    shard_id,
                    ?meta.replica_nodes,
                    epoch,
                    "Shard replicas updated with newer epoch"
                );
                true
            } else {
                tracing::debug!(
                    shard_id,
                    epoch,
                    current_epoch = meta.leader_epoch,
                    "Rejected stale replica update"
                );
                false
            }
        } else {
            false
        }
    }

    /// Clear the primary for all shards where the given node was primary.
    ///
    /// Called when a node fails or leaves. Returns the list of affected shard IDs.
    pub fn clear_primary_for_node(&self, node_id: NodeId) -> Vec<ShardId> {
        let mut shards = self.shards.write();
        let mut affected = Vec::new();

        for (shard_id, meta) in shards.iter_mut() {
            if meta.primary_node == Some(node_id) {
                meta.primary_node = None;
                meta.version += 1;
                affected.push(*shard_id);
            }
        }

        if !affected.is_empty() {
            *self.version.write() += 1;
            tracing::info!(
                node_id,
                shards = ?affected,
                "Cleared primary for failed node"
            );
        }

        affected
    }

    /// Remove a node from all shard replicas.
    ///
    /// Called when a node leaves the cluster. Returns the list of affected shard IDs.
    pub fn remove_node_from_replicas(&self, node_id: NodeId) -> Vec<ShardId> {
        let mut shards = self.shards.write();
        let mut affected = Vec::new();

        for (shard_id, meta) in shards.iter_mut() {
            if let Some(pos) = meta.replica_nodes.iter().position(|&n| n == node_id) {
                meta.replica_nodes.remove(pos);
                meta.version += 1;
                affected.push(*shard_id);

                // Also clear primary if this node was primary
                if meta.primary_node == Some(node_id) {
                    meta.primary_node = None;
                }
            }
        }

        if !affected.is_empty() {
            *self.version.write() += 1;
            tracing::info!(
                node_id,
                shards = ?affected,
                "Removed node from shard replicas"
            );
        }

        affected
    }

    /// Get a summary of shard states.
    pub fn state_summary(&self) -> HashMap<ShardLifecycleState, u32> {
        let shards = self.shards.read();
        let mut summary = HashMap::new();

        for meta in shards.values() {
            *summary.entry(meta.state).or_insert(0) += 1;
        }

        summary
    }

    /// Check if all shards are in the Active state.
    pub fn all_active(&self) -> bool {
        self.shards
            .read()
            .values()
            .all(|meta| meta.state == ShardLifecycleState::Active)
    }

    /// Check if any shard is migrating.
    pub fn any_migrating(&self) -> bool {
        self.shards
            .read()
            .values()
            .any(|meta| meta.state == ShardLifecycleState::Migrating)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_creation() {
        let registry = ShardRegistry::new(16);

        assert_eq!(registry.num_shards(), 16);
        assert_eq!(registry.all().len(), 16);

        // All shards should start unassigned
        for shard_id in 0..16 {
            let meta = registry.get(shard_id).unwrap();
            assert_eq!(meta.state, ShardLifecycleState::Unassigned);
            assert!(meta.replica_nodes.is_empty());
            assert!(meta.primary_node.is_none());
        }
    }

    #[test]
    fn test_shard_state_changes() {
        let registry = ShardRegistry::new(4);

        assert!(registry.set_state(0, ShardLifecycleState::Initializing));
        assert!(registry.set_state(0, ShardLifecycleState::Active));

        let meta = registry.get(0).unwrap();
        assert_eq!(meta.state, ShardLifecycleState::Active);
        assert!(meta.is_active());
    }

    #[test]
    fn test_shard_replicas() {
        let registry = ShardRegistry::new(4);

        registry.set_replicas(0, vec![1, 2, 3]);

        let meta = registry.get(0).unwrap();
        assert_eq!(meta.replica_nodes, vec![1, 2, 3]);
        assert!(meta.has_replica(1));
        assert!(meta.has_replica(2));
        assert!(meta.has_replica(3));
        assert!(!meta.has_replica(4));
    }

    #[test]
    fn test_shard_primary() {
        let registry = ShardRegistry::new(4);

        let epoch1 = registry.set_primary(0, Some(1)).unwrap();
        assert_eq!(epoch1, 1);

        let meta = registry.get(0).unwrap();
        assert_eq!(meta.primary_node, Some(1));
        assert!(meta.is_primary(1));
        assert!(!meta.is_primary(2));
    }

    #[test]
    fn test_primary_epoch_ordering() {
        let registry = ShardRegistry::new(4);

        // Set initial primary
        registry.set_primary(0, Some(1));

        // Try to set with older epoch - should fail
        assert!(!registry.set_primary_if_newer(0, 2, 0));

        // Set with newer epoch - should succeed
        assert!(registry.set_primary_if_newer(0, 2, 2));

        let meta = registry.get(0).unwrap();
        assert_eq!(meta.primary_node, Some(2));
        assert_eq!(meta.leader_epoch, 2);
    }

    #[test]
    fn test_clear_primary_for_node() {
        let registry = ShardRegistry::new(4);

        // Set node 1 as primary for shards 0 and 2
        registry.set_primary(0, Some(1));
        registry.set_primary(1, Some(2));
        registry.set_primary(2, Some(1));
        registry.set_primary(3, Some(3));

        // Clear node 1
        let affected = registry.clear_primary_for_node(1);

        assert_eq!(affected.len(), 2);
        assert!(affected.contains(&0));
        assert!(affected.contains(&2));

        // Verify primaries are cleared
        assert!(registry.get(0).unwrap().primary_node.is_none());
        assert_eq!(registry.get(1).unwrap().primary_node, Some(2));
        assert!(registry.get(2).unwrap().primary_node.is_none());
        assert_eq!(registry.get(3).unwrap().primary_node, Some(3));
    }

    #[test]
    fn test_shards_on_node() {
        let registry = ShardRegistry::new(4);

        registry.set_replicas(0, vec![1, 2, 3]);
        registry.set_replicas(1, vec![2, 3, 4]);
        registry.set_replicas(2, vec![1, 3, 4]);
        registry.set_replicas(3, vec![1, 2, 4]);

        let shards_on_1 = registry.shards_on_node(1);
        assert_eq!(shards_on_1.len(), 3); // shards 0, 2, 3

        let shards_on_4 = registry.shards_on_node(4);
        assert_eq!(shards_on_4.len(), 3); // shards 1, 2, 3
    }

    #[test]
    fn test_state_summary() {
        let registry = ShardRegistry::new(4);

        registry.set_state(0, ShardLifecycleState::Active);
        registry.set_state(1, ShardLifecycleState::Active);
        registry.set_state(2, ShardLifecycleState::Migrating);

        let summary = registry.state_summary();

        assert_eq!(summary.get(&ShardLifecycleState::Active), Some(&2));
        assert_eq!(summary.get(&ShardLifecycleState::Migrating), Some(&1));
        assert_eq!(summary.get(&ShardLifecycleState::Unassigned), Some(&1));
    }
}
