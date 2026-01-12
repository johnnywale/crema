//! Key ownership tracking and calculation.
//!
//! This module provides utilities for determining which nodes own which keys
//! and tracking ownership changes during cluster membership changes.

use crate::partitioning::hashring::HashRing;
use crate::types::NodeId;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};

/// Role of a node for a specific key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnershipRole {
    /// Primary owner - handles writes and coordinates replication.
    Primary,
    /// Backup owner - holds a replica for fault tolerance.
    Backup,
    /// Not an owner - should not hold this key.
    None,
}

/// Ownership information for a key.
#[derive(Debug, Clone)]
pub struct KeyOwnership {
    /// The key (hash).
    pub key_hash: u64,

    /// Primary owner node.
    pub primary: NodeId,

    /// Backup nodes (in order of preference).
    pub backups: Vec<NodeId>,
}

impl KeyOwnership {
    /// Get all owner nodes (primary + backups).
    pub fn all_owners(&self) -> Vec<NodeId> {
        let mut owners = vec![self.primary];
        owners.extend(&self.backups);
        owners
    }

    /// Check if a node is an owner.
    pub fn is_owner(&self, node_id: NodeId) -> bool {
        self.primary == node_id || self.backups.contains(&node_id)
    }

    /// Get the role of a node for this key.
    pub fn role(&self, node_id: NodeId) -> OwnershipRole {
        if self.primary == node_id {
            OwnershipRole::Primary
        } else if self.backups.contains(&node_id) {
            OwnershipRole::Backup
        } else {
            OwnershipRole::None
        }
    }
}

/// Tracks key ownership and provides ownership queries.
pub struct OwnershipTracker {
    /// The consistent hash ring.
    ring: RwLock<HashRing>,

    /// This node's ID.
    local_node_id: NodeId,

    /// Cache of recent ownership lookups.
    /// Key is the first 8 bytes of the key hash.
    ownership_cache: RwLock<HashMap<u64, KeyOwnership>>,

    /// Maximum cache size.
    max_cache_size: usize,
}

impl OwnershipTracker {
    /// Create a new ownership tracker.
    pub fn new(local_node_id: NodeId, num_replicas: usize) -> Self {
        Self {
            ring: RwLock::new(HashRing::new(num_replicas)),
            local_node_id,
            ownership_cache: RwLock::new(HashMap::new()),
            max_cache_size: 10_000,
        }
    }

    /// Create with an existing hash ring.
    pub fn with_ring(local_node_id: NodeId, ring: HashRing) -> Self {
        Self {
            ring: RwLock::new(ring),
            local_node_id,
            ownership_cache: RwLock::new(HashMap::new()),
            max_cache_size: 10_000,
        }
    }

    /// Get the local node ID.
    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }

    /// Get a reference to the hash ring.
    pub fn ring(&self) -> impl std::ops::Deref<Target = HashRing> + '_ {
        self.ring.read()
    }

    /// Add a node to the ring.
    pub fn add_node(&self, node_id: NodeId) {
        self.ring.write().add_node(node_id);
        self.invalidate_cache();
    }

    /// Remove a node from the ring.
    pub fn remove_node(&self, node_id: NodeId) {
        self.ring.write().remove_node(node_id);
        self.invalidate_cache();
    }

    /// Get the number of nodes in the ring.
    pub fn node_count(&self) -> usize {
        self.ring.read().node_count()
    }

    /// Get all nodes in the ring.
    pub fn nodes(&self) -> Vec<NodeId> {
        self.ring.read().nodes().to_vec()
    }

    /// Invalidate the ownership cache.
    fn invalidate_cache(&self) {
        self.ownership_cache.write().clear();
    }

    /// Get ownership information for a key.
    pub fn get_ownership(&self, key: &[u8]) -> Option<KeyOwnership> {
        let ring = self.ring.read();

        if ring.node_count() == 0 {
            return None;
        }

        let owners = ring.get_replica_owners(key);
        if owners.is_empty() {
            return None;
        }

        let key_hash = Self::hash_key(key);

        Some(KeyOwnership {
            key_hash,
            primary: owners[0],
            backups: owners[1..].to_vec(),
        })
    }

    /// Check if this node should own a key.
    pub fn should_own(&self, key: &[u8]) -> bool {
        self.ring.read().is_owner(key, self.local_node_id)
    }

    /// Check if this node is the primary owner for a key.
    pub fn is_primary(&self, key: &[u8]) -> bool {
        self.ring.read().is_primary(key, self.local_node_id)
    }

    /// Get the role of this node for a key.
    pub fn local_role(&self, key: &[u8]) -> OwnershipRole {
        let ring = self.ring.read();
        let owners = ring.get_replica_owners(key);

        if owners.is_empty() {
            return OwnershipRole::None;
        }

        if owners[0] == self.local_node_id {
            OwnershipRole::Primary
        } else if owners.contains(&self.local_node_id) {
            OwnershipRole::Backup
        } else {
            OwnershipRole::None
        }
    }

    /// Get the primary owner for a key.
    pub fn get_primary(&self, key: &[u8]) -> Option<NodeId> {
        self.ring.read().get_primary(key)
    }

    /// Get all owners for a key.
    pub fn get_owners(&self, key: &[u8]) -> Vec<NodeId> {
        self.ring.read().get_replica_owners(key)
    }

    /// Calculate which keys need to be transferred when a node is added.
    ///
    /// Returns a map of (from_node, to_node) -> estimated key count.
    pub fn calculate_transfer_on_add(&self, new_node: NodeId) -> HashMap<(NodeId, NodeId), usize> {
        let ring = self.ring.read();
        let changes = ring.calculate_add_changes(new_node);

        let mut transfers: HashMap<(NodeId, NodeId), usize> = HashMap::new();
        for change in changes {
            *transfers.entry((change.from_node, change.to_node)).or_insert(0) += 1;
        }

        transfers
    }

    /// Calculate which keys need to be transferred when a node is removed.
    ///
    /// Returns a map of (from_node, to_node) -> estimated key count.
    pub fn calculate_transfer_on_remove(&self, removed_node: NodeId) -> HashMap<(NodeId, NodeId), usize> {
        let ring = self.ring.read();
        let changes = ring.calculate_remove_changes(removed_node);

        let mut transfers: HashMap<(NodeId, NodeId), usize> = HashMap::new();
        for change in changes {
            *transfers.entry((change.from_node, change.to_node)).or_insert(0) += 1;
        }

        transfers
    }

    /// Get keys from a collection that this node should own.
    pub fn filter_owned_keys<'a, I>(&self, keys: I) -> Vec<&'a [u8]>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        keys.into_iter()
            .filter(|key| self.should_own(key))
            .collect()
    }

    /// Get keys from a collection that this node is the primary owner for.
    pub fn filter_primary_keys<'a, I>(&self, keys: I) -> Vec<&'a [u8]>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        keys.into_iter()
            .filter(|key| self.is_primary(key))
            .collect()
    }

    /// Compute the hash of a key.
    fn hash_key(key: &[u8]) -> u64 {
        use std::hash::{Hash, Hasher};
        use twox_hash::XxHash64;

        let mut hasher = XxHash64::with_seed(0);
        key.hash(&mut hasher);
        hasher.finish()
    }
}

impl std::fmt::Debug for OwnershipTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnershipTracker")
            .field("local_node_id", &self.local_node_id)
            .field("node_count", &self.node_count())
            .finish()
    }
}

/// Tracks pending ownership transfers during rebalancing.
#[derive(Debug)]
pub struct PendingTransfers {
    /// Keys being transferred out from this node.
    outgoing: RwLock<HashSet<Vec<u8>>>,

    /// Keys being transferred in to this node.
    incoming: RwLock<HashSet<Vec<u8>>>,
}

impl PendingTransfers {
    /// Create a new pending transfers tracker.
    pub fn new() -> Self {
        Self {
            outgoing: RwLock::new(HashSet::new()),
            incoming: RwLock::new(HashSet::new()),
        }
    }

    /// Mark a key as pending outgoing transfer.
    pub fn mark_outgoing(&self, key: Vec<u8>) {
        self.outgoing.write().insert(key);
    }

    /// Mark a key as pending incoming transfer.
    pub fn mark_incoming(&self, key: Vec<u8>) {
        self.incoming.write().insert(key);
    }

    /// Complete an outgoing transfer.
    pub fn complete_outgoing(&self, key: &[u8]) {
        self.outgoing.write().remove(key);
    }

    /// Complete an incoming transfer.
    pub fn complete_incoming(&self, key: &[u8]) {
        self.incoming.write().remove(key);
    }

    /// Check if a key has a pending outgoing transfer.
    pub fn is_outgoing(&self, key: &[u8]) -> bool {
        self.outgoing.read().contains(key)
    }

    /// Check if a key has a pending incoming transfer.
    pub fn is_incoming(&self, key: &[u8]) -> bool {
        self.incoming.read().contains(key)
    }

    /// Get the count of pending outgoing transfers.
    pub fn outgoing_count(&self) -> usize {
        self.outgoing.read().len()
    }

    /// Get the count of pending incoming transfers.
    pub fn incoming_count(&self) -> usize {
        self.incoming.read().len()
    }

    /// Clear all pending transfers.
    pub fn clear(&self) {
        self.outgoing.write().clear();
        self.incoming.write().clear();
    }
}

impl Default for PendingTransfers {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ownership_tracker_basic() {
        let tracker = OwnershipTracker::new(1, 2);
        tracker.add_node(1);
        tracker.add_node(2);
        tracker.add_node(3);

        assert_eq!(tracker.node_count(), 3);

        let ownership = tracker.get_ownership(b"test_key").unwrap();
        assert!(ownership.is_owner(ownership.primary));
        assert_eq!(ownership.all_owners().len(), 2);
    }

    #[test]
    fn test_local_ownership() {
        let tracker = OwnershipTracker::new(1, 2);
        tracker.add_node(1);
        tracker.add_node(2);

        // With 2 nodes and 2 replicas, this node should own everything
        assert!(tracker.should_own(b"key1"));
        assert!(tracker.should_own(b"key2"));
    }

    #[test]
    fn test_ownership_role() {
        let tracker = OwnershipTracker::new(1, 2);
        tracker.add_node(1);
        tracker.add_node(2);

        let role = tracker.local_role(b"test_key");
        // Should be either Primary or Backup
        assert!(matches!(role, OwnershipRole::Primary | OwnershipRole::Backup));
    }

    #[test]
    fn test_key_ownership_struct() {
        let ownership = KeyOwnership {
            key_hash: 12345,
            primary: 1,
            backups: vec![2, 3],
        };

        assert!(ownership.is_owner(1));
        assert!(ownership.is_owner(2));
        assert!(ownership.is_owner(3));
        assert!(!ownership.is_owner(4));

        assert_eq!(ownership.role(1), OwnershipRole::Primary);
        assert_eq!(ownership.role(2), OwnershipRole::Backup);
        assert_eq!(ownership.role(4), OwnershipRole::None);

        assert_eq!(ownership.all_owners(), vec![1, 2, 3]);
    }

    #[test]
    fn test_pending_transfers() {
        let transfers = PendingTransfers::new();

        transfers.mark_outgoing(b"key1".to_vec());
        transfers.mark_incoming(b"key2".to_vec());

        assert!(transfers.is_outgoing(b"key1"));
        assert!(!transfers.is_outgoing(b"key2"));
        assert!(transfers.is_incoming(b"key2"));
        assert!(!transfers.is_incoming(b"key1"));

        assert_eq!(transfers.outgoing_count(), 1);
        assert_eq!(transfers.incoming_count(), 1);

        transfers.complete_outgoing(b"key1");
        assert!(!transfers.is_outgoing(b"key1"));
        assert_eq!(transfers.outgoing_count(), 0);
    }

    #[test]
    fn test_transfer_calculation() {
        let tracker = OwnershipTracker::new(1, 2);
        tracker.add_node(1);
        tracker.add_node(2);

        // Adding node 3 should cause some keys to transfer
        let transfers = tracker.calculate_transfer_on_add(3);
        assert!(!transfers.is_empty());

        // All transfers should be TO node 3
        for ((from, to), _) in &transfers {
            assert_ne!(*from, 3);
            assert_eq!(*to, 3);
        }
    }

    #[test]
    fn test_empty_ring() {
        let tracker = OwnershipTracker::new(1, 2);

        assert!(tracker.get_ownership(b"key").is_none());
        assert!(!tracker.should_own(b"key"));
        assert_eq!(tracker.local_role(b"key"), OwnershipRole::None);
    }
}
