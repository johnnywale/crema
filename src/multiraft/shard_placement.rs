//! Shard placement using consistent hashing.
//!
//! This module manages the mapping from shards to nodes using a consistent
//! hash ring. When nodes join or leave, the hash ring determines which
//! shards need to be migrated.

use crate::partitioning::HashRing;
use crate::types::NodeId;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};

use super::shard::ShardId;

/// Type of shard movement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MovementType {
    /// Adding a new replica to a node.
    AddReplica,
    /// Removing a replica from a node.
    RemoveReplica,
    /// Transferring primary ownership.
    TransferPrimary,
}

impl std::fmt::Display for MovementType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MovementType::AddReplica => write!(f, "add_replica"),
            MovementType::RemoveReplica => write!(f, "remove_replica"),
            MovementType::TransferPrimary => write!(f, "transfer_primary"),
        }
    }
}

/// Represents a shard that needs to be moved between nodes.
#[derive(Debug, Clone)]
pub struct ShardMovement {
    /// Shard being moved.
    pub shard_id: ShardId,
    /// Source node (where data comes from).
    pub from_node: NodeId,
    /// Target node (where data goes to).
    pub to_node: NodeId,
    /// Type of movement.
    pub movement_type: MovementType,
    /// Priority (lower = higher priority).
    pub priority: u32,
}

impl ShardMovement {
    /// Create a new shard movement.
    pub fn new(
        shard_id: ShardId,
        from_node: NodeId,
        to_node: NodeId,
        movement_type: MovementType,
    ) -> Self {
        Self {
            shard_id,
            from_node,
            to_node,
            movement_type,
            priority: 0,
        }
    }

    /// Set the priority for this movement.
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }
}

/// Configuration for shard placement.
#[derive(Debug, Clone)]
pub struct PlacementConfig {
    /// Number of replicas per shard.
    pub num_replicas: usize,
    /// Number of virtual nodes per physical node in the hash ring.
    pub vnodes_per_node: usize,
}

impl Default for PlacementConfig {
    fn default() -> Self {
        Self {
            num_replicas: 3,
            vnodes_per_node: 256,
        }
    }
}

impl PlacementConfig {
    /// Create a new placement config.
    pub fn new(num_replicas: usize) -> Self {
        Self {
            num_replicas,
            ..Default::default()
        }
    }

    /// Set the number of virtual nodes per physical node.
    pub fn with_vnodes(mut self, vnodes: usize) -> Self {
        self.vnodes_per_node = vnodes;
        self
    }
}

/// Manages shard-to-node placement using consistent hashing.
///
/// The placement manager uses a hash ring to determine which nodes should
/// host which shards. When nodes join or leave, it calculates the minimum
/// set of shard movements needed.
#[derive(Debug)]
pub struct ShardPlacement {
    /// Configuration.
    config: PlacementConfig,
    /// Total number of shards.
    num_shards: u32,
    /// Current hash ring.
    ring: RwLock<HashRing>,
    /// Pending hash ring (during rebalance).
    pending_ring: RwLock<Option<HashRing>>,
    /// Current placement version (for consistency).
    version: RwLock<u64>,
}

impl ShardPlacement {
    /// Create a new shard placement manager.
    pub fn new(num_shards: u32, config: PlacementConfig) -> Self {
        let ring = HashRing::new(config.num_replicas);

        Self {
            config,
            num_shards,
            ring: RwLock::new(ring),
            pending_ring: RwLock::new(None),
            version: RwLock::new(1),
        }
    }

    /// Create with default configuration.
    pub fn with_defaults(num_shards: u32, num_replicas: usize) -> Self {
        Self::new(num_shards, PlacementConfig::new(num_replicas))
    }

    /// Get the current placement version.
    pub fn version(&self) -> u64 {
        *self.version.read()
    }

    /// Get the number of shards.
    pub fn num_shards(&self) -> u32 {
        self.num_shards
    }

    /// Get the number of replicas per shard.
    pub fn num_replicas(&self) -> usize {
        self.config.num_replicas
    }

    /// Get all nodes in the current ring.
    pub fn nodes(&self) -> Vec<NodeId> {
        self.ring.read().nodes().to_vec()
    }

    /// Get the number of nodes in the ring.
    pub fn node_count(&self) -> usize {
        self.ring.read().node_count()
    }

    /// Check if a node is in the ring.
    pub fn has_node(&self, node_id: NodeId) -> bool {
        self.ring.read().nodes().contains(&node_id)
    }

    /// Add a node to the ring.
    ///
    /// Note: This doesn't trigger migration automatically. Use
    /// `calculate_movements_on_add` to get the required movements first.
    pub fn add_node(&self, node_id: NodeId) {
        let mut ring = self.ring.write();
        ring.add_node(node_id);
        *self.version.write() += 1;
        tracing::info!(node_id, "Added node to placement ring");
    }

    /// Remove a node from the ring.
    ///
    /// Note: This doesn't trigger migration automatically. Use
    /// `calculate_movements_on_remove` to get the required movements first.
    pub fn remove_node(&self, node_id: NodeId) {
        let mut ring = self.ring.write();
        ring.remove_node(node_id);
        *self.version.write() += 1;
        tracing::info!(node_id, "Removed node from placement ring");
    }

    /// Get the nodes that should host a shard.
    ///
    /// Returns nodes in preference order: first is primary, rest are backups.
    pub fn get_shard_nodes(&self, shard_id: ShardId) -> Vec<NodeId> {
        let ring = self.ring.read();
        let key = shard_id.to_le_bytes();
        ring.get_replica_owners(&key)
    }

    /// Get the primary node for a shard.
    pub fn get_primary(&self, shard_id: ShardId) -> Option<NodeId> {
        self.get_shard_nodes(shard_id).first().copied()
    }

    /// Get backup nodes for a shard (excludes primary).
    pub fn get_backups(&self, shard_id: ShardId) -> Vec<NodeId> {
        let nodes = self.get_shard_nodes(shard_id);
        if nodes.len() > 1 {
            nodes[1..].to_vec()
        } else {
            Vec::new()
        }
    }

    /// Get all shards that should be hosted on a node.
    pub fn shards_on_node(&self, node_id: NodeId) -> Vec<ShardId> {
        let mut shards = Vec::new();
        for shard_id in 0..self.num_shards {
            if self.get_shard_nodes(shard_id).contains(&node_id) {
                shards.push(shard_id);
            }
        }
        shards
    }

    /// Get shards where a node is the primary.
    pub fn primary_shards_on_node(&self, node_id: NodeId) -> Vec<ShardId> {
        let mut shards = Vec::new();
        for shard_id in 0..self.num_shards {
            if self.get_primary(shard_id) == Some(node_id) {
                shards.push(shard_id);
            }
        }
        shards
    }

    /// Check if a node should host a shard.
    pub fn should_host(&self, node_id: NodeId, shard_id: ShardId) -> bool {
        self.get_shard_nodes(shard_id).contains(&node_id)
    }

    /// Check if a node is the primary for a shard.
    pub fn is_primary(&self, node_id: NodeId, shard_id: ShardId) -> bool {
        self.get_primary(shard_id) == Some(node_id)
    }

    /// Calculate shard movements when a node joins.
    ///
    /// Returns the list of shard movements needed to rebalance the cluster.
    pub fn calculate_movements_on_add(&self, new_node: NodeId) -> Vec<ShardMovement> {
        let ring = self.ring.read();
        let mut movements = Vec::new();

        // Create a hypothetical ring with the new node
        let mut new_ring = ring.clone();
        new_ring.add_node(new_node);

        // For each shard, check if the new node should host it
        for shard_id in 0..self.num_shards {
            let key = shard_id.to_le_bytes();
            let old_nodes: HashSet<_> = ring.get_replica_owners(&key).into_iter().collect();
            let new_nodes: HashSet<_> = new_ring.get_replica_owners(&key).into_iter().collect();

            // If new_node is now a replica for this shard
            if new_nodes.contains(&new_node) && !old_nodes.contains(&new_node) {
                // Find a node that was a replica but no longer is
                let removed_node = old_nodes.difference(&new_nodes).next();

                if let Some(&from_node) = removed_node {
                    // Transfer from the removed node to the new node
                    movements.push(ShardMovement::new(
                        shard_id,
                        from_node,
                        new_node,
                        MovementType::AddReplica,
                    ));
                } else {
                    // No node removed, pick any existing replica to copy from
                    if let Some(&from_node) = old_nodes.iter().next() {
                        movements.push(ShardMovement::new(
                            shard_id,
                            from_node,
                            new_node,
                            MovementType::AddReplica,
                        ));
                    }
                }
            }
        }

        // Store pending ring for two-phase commit
        *self.pending_ring.write() = Some(new_ring);

        tracing::info!(
            new_node,
            movements = movements.len(),
            "Calculated movements for node add"
        );

        movements
    }

    /// Calculate shard movements when a node leaves.
    ///
    /// Returns the list of shard movements needed to maintain replication.
    pub fn calculate_movements_on_remove(&self, leaving_node: NodeId) -> Vec<ShardMovement> {
        let ring = self.ring.read();
        let mut movements = Vec::new();

        // Create a hypothetical ring without the leaving node
        let mut new_ring = ring.clone();
        new_ring.remove_node(leaving_node);

        // For each shard, check if we need to add a new replica
        for shard_id in 0..self.num_shards {
            let key = shard_id.to_le_bytes();
            let old_nodes: HashSet<_> = ring.get_replica_owners(&key).into_iter().collect();
            let new_nodes: HashSet<_> = new_ring.get_replica_owners(&key).into_iter().collect();

            // If leaving_node was a replica for this shard
            if old_nodes.contains(&leaving_node) {
                // Find a node that is now a replica but wasn't before
                let added_node = new_nodes.difference(&old_nodes).next();

                if let Some(&to_node) = added_node {
                    // Transfer from the leaving node to the new replica
                    movements.push(ShardMovement::new(
                        shard_id,
                        leaving_node,
                        to_node,
                        MovementType::RemoveReplica,
                    ));
                } else {
                    // No new replica, just mark for removal
                    // (this can happen if there aren't enough nodes)
                    tracing::warn!(
                        shard_id,
                        leaving_node,
                        "No replacement replica available for shard"
                    );
                }
            }
        }

        // Store pending ring for two-phase commit
        *self.pending_ring.write() = Some(new_ring);

        tracing::info!(
            leaving_node,
            movements = movements.len(),
            "Calculated movements for node remove"
        );

        movements
    }

    /// Commit the pending ring configuration.
    ///
    /// This should be called after all shard movements have completed.
    ///
    /// # Lock Ordering
    ///
    /// Acquires `ring` lock FIRST, then `pending_ring` to maintain consistent
    /// lock ordering and prevent deadlocks. All other code paths that need both
    /// locks must follow this same order.
    pub fn commit(&self) -> bool {
        // CRITICAL: Acquire ring lock FIRST, then pending_ring to prevent deadlock
        // Other methods (get_shard_nodes, snapshot, etc.) acquire ring.read() first,
        // so we must maintain this ordering.
        let mut ring_guard = self.ring.write();
        let pending = self.pending_ring.write().take();
        if let Some(new_ring) = pending {
            *ring_guard = new_ring;
            drop(ring_guard);
            *self.version.write() += 1;
            tracing::info!(version = self.version(), "Committed new placement ring");
            true
        } else {
            false
        }
    }

    /// Abort the pending ring configuration.
    ///
    /// This discards the pending ring without applying it.
    pub fn abort(&self) -> bool {
        let pending = self.pending_ring.write().take();
        if pending.is_some() {
            tracing::warn!("Aborted pending placement ring");
            true
        } else {
            false
        }
    }

    /// Check if there's a pending ring configuration.
    pub fn has_pending(&self) -> bool {
        self.pending_ring.read().is_some()
    }

    /// Get a snapshot of current shard placements.
    pub fn snapshot(&self) -> HashMap<ShardId, Vec<NodeId>> {
        let mut placements = HashMap::new();
        for shard_id in 0..self.num_shards {
            placements.insert(shard_id, self.get_shard_nodes(shard_id));
        }
        placements
    }

    /// Get the current ring (for advanced use cases).
    pub fn ring(&self) -> HashRing {
        self.ring.read().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_placement_creation() {
        let placement = ShardPlacement::with_defaults(16, 3);

        assert_eq!(placement.num_shards(), 16);
        assert_eq!(placement.num_replicas(), 3);
        assert_eq!(placement.node_count(), 0);
    }

    #[test]
    fn test_add_nodes() {
        let placement = ShardPlacement::with_defaults(16, 3);

        placement.add_node(1);
        placement.add_node(2);
        placement.add_node(3);

        assert_eq!(placement.node_count(), 3);
        assert!(placement.has_node(1));
        assert!(placement.has_node(2));
        assert!(placement.has_node(3));
        assert!(!placement.has_node(4));
    }

    #[test]
    fn test_shard_placement() {
        let placement = ShardPlacement::with_defaults(16, 3);

        placement.add_node(1);
        placement.add_node(2);
        placement.add_node(3);

        // Each shard should have 3 replicas
        for shard_id in 0..16 {
            let nodes = placement.get_shard_nodes(shard_id);
            assert_eq!(nodes.len(), 3, "Shard {} should have 3 replicas", shard_id);

            // All nodes should be unique
            let unique: HashSet<_> = nodes.iter().collect();
            assert_eq!(unique.len(), 3, "Shard {} should have unique replicas", shard_id);
        }
    }

    #[test]
    fn test_primary_and_backups() {
        let placement = ShardPlacement::with_defaults(16, 3);

        placement.add_node(1);
        placement.add_node(2);
        placement.add_node(3);

        for shard_id in 0..16 {
            let primary = placement.get_primary(shard_id);
            let backups = placement.get_backups(shard_id);

            assert!(primary.is_some());
            assert_eq!(backups.len(), 2);
            assert!(!backups.contains(&primary.unwrap()));
        }
    }

    #[test]
    fn test_shards_on_node() {
        let placement = ShardPlacement::with_defaults(16, 3);

        placement.add_node(1);
        placement.add_node(2);
        placement.add_node(3);

        // Each node should host some shards
        let shards_on_1 = placement.shards_on_node(1);
        let shards_on_2 = placement.shards_on_node(2);
        let shards_on_3 = placement.shards_on_node(3);

        assert!(!shards_on_1.is_empty());
        assert!(!shards_on_2.is_empty());
        assert!(!shards_on_3.is_empty());

        // With 3 nodes and 3 replicas, each node should host all shards
        assert_eq!(shards_on_1.len(), 16);
        assert_eq!(shards_on_2.len(), 16);
        assert_eq!(shards_on_3.len(), 16);
    }

    #[test]
    fn test_movements_on_add() {
        let placement = ShardPlacement::with_defaults(16, 3);

        placement.add_node(1);
        placement.add_node(2);
        placement.add_node(3);

        // Calculate movements when node 4 joins
        let movements = placement.calculate_movements_on_add(4);

        // Should have some movements (shards moving to the new node)
        assert!(!movements.is_empty());

        // All movements should be to node 4
        for m in &movements {
            assert_eq!(m.to_node, 4);
            assert_ne!(m.from_node, 4);
        }

        // Should have pending ring
        assert!(placement.has_pending());
    }

    #[test]
    fn test_movements_on_remove() {
        let placement = ShardPlacement::with_defaults(16, 3);

        placement.add_node(1);
        placement.add_node(2);
        placement.add_node(3);
        placement.add_node(4);

        // Calculate movements when node 4 leaves
        let movements = placement.calculate_movements_on_remove(4);

        // All movements should be from node 4
        for m in &movements {
            assert_eq!(m.from_node, 4);
            assert_ne!(m.to_node, 4);
        }
    }

    #[test]
    fn test_commit_pending() {
        let placement = ShardPlacement::with_defaults(16, 3);

        placement.add_node(1);
        placement.add_node(2);
        placement.add_node(3);

        let v1 = placement.version();

        // Calculate movements (creates pending ring)
        placement.calculate_movements_on_add(4);
        assert!(placement.has_pending());

        // Node 4 not in ring yet
        assert!(!placement.has_node(4));

        // Commit
        assert!(placement.commit());
        assert!(!placement.has_pending());

        // Now node 4 is in the ring
        assert!(placement.has_node(4));
        assert!(placement.version() > v1);
    }

    #[test]
    fn test_abort_pending() {
        let placement = ShardPlacement::with_defaults(16, 3);

        placement.add_node(1);
        placement.add_node(2);
        placement.add_node(3);

        // Calculate movements (creates pending ring)
        placement.calculate_movements_on_add(4);
        assert!(placement.has_pending());

        // Abort
        assert!(placement.abort());
        assert!(!placement.has_pending());

        // Node 4 still not in ring
        assert!(!placement.has_node(4));
    }

    #[test]
    fn test_snapshot() {
        let placement = ShardPlacement::with_defaults(4, 2);

        placement.add_node(1);
        placement.add_node(2);
        placement.add_node(3);

        let snapshot = placement.snapshot();

        assert_eq!(snapshot.len(), 4);
        for shard_id in 0..4 {
            assert!(snapshot.contains_key(&shard_id));
            assert_eq!(snapshot[&shard_id].len(), 2);
        }
    }
}
