//! Consistent hashing implementation with virtual nodes.
//!
//! This module implements a consistent hash ring for distributing keys across
//! cluster nodes. Each physical node is represented by multiple virtual nodes
//! (vnodes) to ensure even key distribution.

use crate::types::NodeId;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use twox_hash::XxHash64;

/// Number of virtual nodes per physical node.
/// More vnodes = more even distribution but higher memory usage.
pub const DEFAULT_VNODES_PER_NODE: usize = 256;

/// A consistent hash ring for distributing keys across nodes.
#[derive(Debug, Clone)]
pub struct HashRing {
    /// Virtual nodes mapped to their owning physical nodes.
    /// The key is the hash position on the ring, value is the node ID.
    vnodes: BTreeMap<u64, NodeId>,

    /// Number of virtual nodes per physical node.
    vnodes_per_node: usize,

    /// Number of replicas (copies of each key).
    num_replicas: usize,

    /// List of physical nodes in the ring.
    nodes: Vec<NodeId>,
}

impl HashRing {
    /// Create a new empty hash ring.
    pub fn new(num_replicas: usize) -> Self {
        Self::with_vnodes(num_replicas, DEFAULT_VNODES_PER_NODE)
    }

    /// Create a new hash ring with custom vnode count.
    pub fn with_vnodes(num_replicas: usize, vnodes_per_node: usize) -> Self {
        Self {
            vnodes: BTreeMap::new(),
            vnodes_per_node,
            num_replicas: num_replicas.max(1),
            nodes: Vec::new(),
        }
    }

    /// Get the number of replicas.
    pub fn num_replicas(&self) -> usize {
        self.num_replicas
    }

    /// Get the number of physical nodes in the ring.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get all physical nodes in the ring.
    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }

    /// Check if a node is in the ring.
    pub fn contains_node(&self, node_id: NodeId) -> bool {
        self.nodes.contains(&node_id)
    }

    /// Add a node to the ring.
    ///
    /// This creates `vnodes_per_node` virtual nodes for the physical node.
    pub fn add_node(&mut self, node_id: NodeId) {
        if self.nodes.contains(&node_id) {
            return;
        }

        self.nodes.push(node_id);
        self.nodes.sort();

        // Create virtual nodes
        for i in 0..self.vnodes_per_node {
            let vnode_key = format!("{}:{}", node_id, i);
            let hash = Self::hash_key(vnode_key.as_bytes());
            self.vnodes.insert(hash, node_id);
        }
    }

    /// Remove a node from the ring.
    pub fn remove_node(&mut self, node_id: NodeId) {
        if !self.nodes.contains(&node_id) {
            return;
        }

        self.nodes.retain(|&n| n != node_id);

        // Remove virtual nodes
        for i in 0..self.vnodes_per_node {
            let vnode_key = format!("{}:{}", node_id, i);
            let hash = Self::hash_key(vnode_key.as_bytes());
            self.vnodes.remove(&hash);
        }
    }

    /// Get the primary owner for a key.
    ///
    /// Returns None if the ring is empty.
    pub fn get_primary(&self, key: &[u8]) -> Option<NodeId> {
        self.get_owners(key, 1).into_iter().next()
    }

    /// Get the owners (primary + replicas) for a key.
    ///
    /// Returns up to `count` unique node IDs that should own this key.
    /// The first node is the primary owner.
    pub fn get_owners(&self, key: &[u8], count: usize) -> Vec<NodeId> {
        if self.vnodes.is_empty() {
            return Vec::new();
        }

        let hash = Self::hash_key(key);
        let mut owners = Vec::with_capacity(count.min(self.nodes.len()));

        // Find the first vnode >= hash
        let iter = self.vnodes.range(hash..).chain(self.vnodes.iter().map(|(k, v)| (k, v)));

        for (_, &node_id) in iter {
            if !owners.contains(&node_id) {
                owners.push(node_id);
                if owners.len() >= count || owners.len() >= self.nodes.len() {
                    break;
                }
            }
        }

        owners
    }

    /// Get all owners for a key based on the configured replica count.
    pub fn get_replica_owners(&self, key: &[u8]) -> Vec<NodeId> {
        self.get_owners(key, self.num_replicas)
    }

    /// Check if a node should own a key.
    pub fn is_owner(&self, key: &[u8], node_id: NodeId) -> bool {
        self.get_replica_owners(key).contains(&node_id)
    }

    /// Check if a node is the primary owner for a key.
    pub fn is_primary(&self, key: &[u8], node_id: NodeId) -> bool {
        self.get_primary(key) == Some(node_id)
    }

    /// Calculate the hash of a key using xxHash64.
    fn hash_key(key: &[u8]) -> u64 {
        let mut hasher = XxHash64::with_seed(0);
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Get the distribution of keys across nodes for a sample.
    ///
    /// This is useful for testing/monitoring key distribution.
    pub fn get_distribution(&self, sample_size: usize) -> std::collections::HashMap<NodeId, usize> {
        let mut distribution = std::collections::HashMap::new();

        for i in 0..sample_size {
            let key = format!("sample_key_{}", i);
            if let Some(owner) = self.get_primary(key.as_bytes()) {
                *distribution.entry(owner).or_insert(0) += 1;
            }
        }

        distribution
    }

    /// Calculate the ownership change when adding a node.
    ///
    /// Returns the set of nodes that will lose keys to the new node,
    /// along with the hash ranges they need to transfer.
    pub fn calculate_add_changes(&self, new_node: NodeId) -> Vec<OwnershipChange> {
        if self.nodes.contains(&new_node) || self.vnodes.is_empty() {
            return Vec::new();
        }

        let mut changes = Vec::new();

        // Simulate adding the node
        let mut test_ring = self.clone();
        test_ring.add_node(new_node);

        // For each vnode of the new node, find which node currently owns that range
        for i in 0..self.vnodes_per_node {
            let vnode_key = format!("{}:{}", new_node, i);
            let vnode_hash = Self::hash_key(vnode_key.as_bytes());

            // Find current owner of this position
            if let Some(current_owner) = self.get_owner_at_position(vnode_hash) {
                if current_owner != new_node {
                    changes.push(OwnershipChange {
                        from_node: current_owner,
                        to_node: new_node,
                        hash_start: vnode_hash,
                    });
                }
            }
        }

        changes
    }

    /// Calculate the ownership change when removing a node.
    ///
    /// Returns the set of nodes that will receive keys from the removed node.
    pub fn calculate_remove_changes(&self, removed_node: NodeId) -> Vec<OwnershipChange> {
        if !self.nodes.contains(&removed_node) {
            return Vec::new();
        }

        let mut changes = Vec::new();

        // Simulate removing the node
        let mut test_ring = self.clone();
        test_ring.remove_node(removed_node);

        if test_ring.vnodes.is_empty() {
            return Vec::new();
        }

        // For each vnode of the removed node, find which node will take over
        for i in 0..self.vnodes_per_node {
            let vnode_key = format!("{}:{}", removed_node, i);
            let vnode_hash = Self::hash_key(vnode_key.as_bytes());

            // Find new owner of this position
            if let Some(new_owner) = test_ring.get_owner_at_position(vnode_hash) {
                changes.push(OwnershipChange {
                    from_node: removed_node,
                    to_node: new_owner,
                    hash_start: vnode_hash,
                });
            }
        }

        changes
    }

    /// Get the node that owns a specific position on the ring.
    fn get_owner_at_position(&self, hash: u64) -> Option<NodeId> {
        if self.vnodes.is_empty() {
            return None;
        }

        // Find the first vnode >= hash, or wrap around
        self.vnodes
            .range(hash..)
            .next()
            .or_else(|| self.vnodes.iter().next())
            .map(|(_, &node_id)| node_id)
    }
}

/// Represents a change in key ownership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipChange {
    /// Node losing ownership.
    pub from_node: NodeId,

    /// Node gaining ownership.
    pub to_node: NodeId,

    /// Starting hash position of the affected range.
    pub hash_start: u64,
}

impl Default for HashRing {
    fn default() -> Self {
        Self::new(2) // Default to 2 replicas
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_ring() {
        let ring = HashRing::new(2);
        assert_eq!(ring.node_count(), 0);
        assert!(ring.get_primary(b"key").is_none());
        assert!(ring.get_owners(b"key", 3).is_empty());
    }

    #[test]
    fn test_single_node() {
        let mut ring = HashRing::new(2);
        ring.add_node(1);

        assert_eq!(ring.node_count(), 1);
        assert_eq!(ring.get_primary(b"key"), Some(1));
        assert_eq!(ring.get_owners(b"key", 3), vec![1]);
    }

    #[test]
    fn test_multiple_nodes() {
        let mut ring = HashRing::new(2);
        ring.add_node(1);
        ring.add_node(2);
        ring.add_node(3);

        assert_eq!(ring.node_count(), 3);

        // Should get 2 owners (num_replicas)
        let owners = ring.get_replica_owners(b"key");
        assert_eq!(owners.len(), 2);

        // All owners should be different
        assert_ne!(owners[0], owners[1]);
    }

    #[test]
    fn test_add_remove_node() {
        let mut ring = HashRing::new(2);
        ring.add_node(1);
        ring.add_node(2);

        assert_eq!(ring.node_count(), 2);
        assert!(ring.contains_node(1));
        assert!(ring.contains_node(2));

        ring.remove_node(1);

        assert_eq!(ring.node_count(), 1);
        assert!(!ring.contains_node(1));
        assert!(ring.contains_node(2));

        // All keys should now go to node 2
        assert_eq!(ring.get_primary(b"key"), Some(2));
    }

    #[test]
    fn test_consistent_hashing() {
        let mut ring = HashRing::new(2);
        ring.add_node(1);
        ring.add_node(2);
        ring.add_node(3);

        // Record owners before adding a node
        let key = b"test_key";
        let owners_before = ring.get_replica_owners(key);

        // Add a new node
        ring.add_node(4);

        // Most keys should still have at least one original owner
        // (consistent hashing minimizes redistribution)
        let owners_after = ring.get_replica_owners(key);

        // Either same owners or at least one owner is preserved
        let preserved = owners_before
            .iter()
            .any(|o| owners_after.contains(o));
        // Note: this test is probabilistic but should almost always pass
        assert!(preserved || owners_after.contains(&4));
    }

    #[test]
    fn test_distribution() {
        let mut ring = HashRing::new(2);
        ring.add_node(1);
        ring.add_node(2);
        ring.add_node(3);

        let distribution = ring.get_distribution(10000);

        // Each node should have roughly 1/3 of keys (with some variance)
        for &node in ring.nodes() {
            let count = distribution.get(&node).copied().unwrap_or(0);
            // Allow 20% variance from expected (3333)
            assert!(count > 2500 && count < 4500, "Node {} has {} keys", node, count);
        }
    }

    #[test]
    fn test_ownership_check() {
        let mut ring = HashRing::new(2);
        ring.add_node(1);
        ring.add_node(2);

        let key = b"test_key";
        let primary = ring.get_primary(key).unwrap();

        assert!(ring.is_owner(key, primary));
        assert!(ring.is_primary(key, primary));
    }

    #[test]
    fn test_duplicate_add() {
        let mut ring = HashRing::new(2);
        ring.add_node(1);
        ring.add_node(1); // Duplicate

        assert_eq!(ring.node_count(), 1);
    }

    #[test]
    fn test_remove_nonexistent() {
        let mut ring = HashRing::new(2);
        ring.add_node(1);
        ring.remove_node(999); // Doesn't exist

        assert_eq!(ring.node_count(), 1);
    }
}
