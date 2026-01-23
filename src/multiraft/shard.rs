//! Shard management for Multi-Raft.
//!
//! A shard is a single Raft group managing a subset of the keyspace.

use crate::cache::storage::CacheStorage;
use crate::config::CacheConfig;
use crate::consensus::state_machine::CacheStateMachine;
use crate::error::Result;
use crate::types::{CacheCommand, NodeId};
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Unique identifier for a shard.
pub type ShardId = u32;

/// Configuration for a shard.
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// Shard ID.
    pub shard_id: ShardId,

    /// Total number of shards in the cluster.
    pub total_shards: u32,

    /// Replica factor for this shard.
    pub replicas: usize,

    /// Maximum capacity per shard.
    pub max_capacity: u64,

    /// Default TTL for entries.
    pub default_ttl: Option<Duration>,
}

impl ShardConfig {
    /// Create a new shard config.
    pub fn new(shard_id: ShardId, total_shards: u32) -> Self {
        Self {
            shard_id,
            total_shards,
            replicas: 3,
            max_capacity: 100_000,
            default_ttl: None,
        }
    }

    /// Set the replica factor.
    pub fn with_replicas(mut self, replicas: usize) -> Self {
        self.replicas = replicas;
        self
    }

    /// Set the maximum capacity.
    pub fn with_max_capacity(mut self, capacity: u64) -> Self {
        self.max_capacity = capacity;
        self
    }

    /// Set the default TTL.
    pub fn with_default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = Some(ttl);
        self
    }
}

/// State of a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardState {
    /// Shard is initializing.
    Initializing,

    /// Shard is active and serving requests.
    Active,

    /// Shard is transferring data (rebalancing).
    Transferring,

    /// Shard is being removed.
    Removing,

    /// Shard is stopped.
    Stopped,
}

impl std::fmt::Display for ShardState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardState::Initializing => write!(f, "initializing"),
            ShardState::Active => write!(f, "active"),
            ShardState::Transferring => write!(f, "transferring"),
            ShardState::Removing => write!(f, "removing"),
            ShardState::Stopped => write!(f, "stopped"),
        }
    }
}

/// Information about a shard.
#[derive(Debug, Clone)]
pub struct ShardInfo {
    /// Shard ID.
    pub shard_id: ShardId,

    /// Current state.
    pub state: ShardState,

    /// Current leader node.
    pub leader: Option<NodeId>,

    /// All nodes in this shard's Raft group.
    pub members: Vec<NodeId>,

    /// Number of entries in this shard.
    pub entry_count: u64,

    /// Approximate size in bytes.
    pub size_bytes: u64,

    /// Current Raft term.
    pub term: u64,

    /// Current commit index.
    pub commit_index: u64,
}

/// A shard represents a partition of the keyspace with its own Raft group.
#[derive(Debug)]
pub struct Shard {
    /// Configuration.
    config: ShardConfig,

    /// Current state.
    state: RwLock<ShardState>,

    /// Local cache storage for this shard.
    storage: Arc<CacheStorage>,

    /// State machine for applying commands.
    state_machine: Arc<CacheStateMachine>,

    /// Current leader node ID.
    leader: RwLock<Option<NodeId>>,

    /// Members of this shard's Raft group.
    members: RwLock<HashSet<NodeId>>,

    /// Current Raft term.
    term: AtomicU64,

    /// Current commit index.
    commit_index: AtomicU64,

    /// Current applied index.
    applied_index: AtomicU64,

    /// Whether this node is the leader for this shard.
    is_local_leader: RwLock<bool>,
}

impl Shard {
    /// Create a new shard.
    pub async fn new(config: ShardConfig) -> Result<Self> {
        // Create cache storage for this shard
        let cache_config = CacheConfig::new(0, "127.0.0.1:0".parse().unwrap())
            .with_max_capacity(config.max_capacity);

        let storage = Arc::new(CacheStorage::new(&cache_config));
        let state_machine = Arc::new(CacheStateMachine::new(storage.clone()));

        Ok(Self {
            config,
            state: RwLock::new(ShardState::Initializing),
            storage,
            state_machine,
            leader: RwLock::new(None),
            members: RwLock::new(HashSet::new()),
            term: AtomicU64::new(0),
            commit_index: AtomicU64::new(0),
            applied_index: AtomicU64::new(0),
            is_local_leader: RwLock::new(false),
        })
    }

    /// Get the shard ID.
    pub fn id(&self) -> ShardId {
        self.config.shard_id
    }

    /// Get the current state.
    pub fn state(&self) -> ShardState {
        *self.state.read()
    }

    /// Set the shard state.
    pub fn set_state(&self, state: ShardState) {
        *self.state.write() = state;
    }

    /// Check if this shard is active.
    pub fn is_active(&self) -> bool {
        matches!(self.state(), ShardState::Active)
    }

    /// Check if this shard owns the given key.
    pub fn owns_key(&self, key_hash: u64) -> bool {
        let shard_id = (key_hash % self.config.total_shards as u64) as ShardId;
        shard_id == self.config.shard_id
    }

    /// Get the storage.
    pub fn storage(&self) -> &Arc<CacheStorage> {
        &self.storage
    }

    /// Get a value from this shard.
    pub async fn get(&self, key: &[u8]) -> Option<Bytes> {
        if !self.is_active() {
            return None;
        }
        self.storage.get(key).await
    }

    /// Put a value in this shard (local only, for leader).
    pub async fn put(&self, key: Bytes, value: Bytes) {
        self.storage.insert(key, value).await;
    }

    /// Put a value with TTL.
    pub async fn put_with_ttl(&self, key: Bytes, value: Bytes, ttl: Duration) {
        self.storage.insert_with_ttl(key, value, ttl).await;
    }

    /// Delete a key from this shard.
    pub async fn delete(&self, key: &[u8]) {
        self.storage.invalidate(key).await;
    }

    /// Clear all entries in this shard.
    pub async fn clear(&self) {
        self.storage.invalidate_all();
    }

    /// Apply a command to this shard (called by Raft state machine).
    pub async fn apply(&self, command: &CacheCommand) {
        match command {
            CacheCommand::Put {
                key,
                value,
                expires_at_ms,
            } => {
                let key = Bytes::from(key.clone());
                let value = Bytes::from(value.clone());
                if let Some(expires_at_ms) = expires_at_ms {
                    // Calculate remaining TTL from absolute expiration time
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;

                    if *expires_at_ms > now_ms {
                        let remaining_ttl_ms = *expires_at_ms - now_ms;
                        self.storage
                            .insert_with_ttl(key, value, Duration::from_millis(remaining_ttl_ms))
                            .await;
                    }
                    // If already expired, skip insertion
                } else {
                    self.storage.insert(key, value).await;
                }
            }
            CacheCommand::Delete { key } => {
                self.storage.invalidate(key).await;
            }
            CacheCommand::Clear => {
                self.storage.invalidate_all();
            }
            CacheCommand::Get { .. } => {
                // Get is read-only, no state change needed
            }
        }
        self.applied_index.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the current leader.
    pub fn leader(&self) -> Option<NodeId> {
        *self.leader.read()
    }

    /// Set the current leader.
    pub fn set_leader(&self, leader: Option<NodeId>) {
        *self.leader.write() = leader;
    }

    /// Check if this node is the leader for this shard.
    pub fn is_leader(&self) -> bool {
        *self.is_local_leader.read()
    }

    /// Set whether this node is the leader.
    pub fn set_is_leader(&self, is_leader: bool) {
        *self.is_local_leader.write() = is_leader;
    }

    /// Get the members of this shard's Raft group.
    pub fn members(&self) -> Vec<NodeId> {
        self.members.read().iter().copied().collect()
    }

    /// Add a member to this shard's Raft group.
    pub fn add_member(&self, node_id: NodeId) {
        self.members.write().insert(node_id);
    }

    /// Remove a member from this shard's Raft group.
    pub fn remove_member(&self, node_id: NodeId) {
        self.members.write().remove(&node_id);
    }

    /// Get the current term.
    pub fn term(&self) -> u64 {
        self.term.load(Ordering::Relaxed)
    }

    /// Set the current term.
    pub fn set_term(&self, term: u64) {
        self.term.store(term, Ordering::Relaxed);
    }

    /// Get the commit index.
    pub fn commit_index(&self) -> u64 {
        self.commit_index.load(Ordering::Relaxed)
    }

    /// Set the commit index.
    pub fn set_commit_index(&self, index: u64) {
        self.commit_index.store(index, Ordering::Relaxed);
    }

    /// Get the applied index.
    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::Relaxed)
    }

    /// Get shard info.
    pub fn info(&self) -> ShardInfo {
        ShardInfo {
            shard_id: self.config.shard_id,
            state: self.state(),
            leader: self.leader(),
            members: self.members(),
            entry_count: self.storage.entry_count(),
            size_bytes: self.storage.weighted_size(),
            term: self.term(),
            commit_index: self.commit_index(),
        }
    }

    /// Get the key range for this shard.
    pub fn key_range(&self) -> ShardRange {
        ShardRange {
            shard_id: self.config.shard_id,
            total_shards: self.config.total_shards,
        }
    }
}

/// Represents the key range owned by a shard.
#[derive(Debug, Clone, Copy)]
pub struct ShardRange {
    /// Shard ID.
    pub shard_id: ShardId,

    /// Total number of shards.
    pub total_shards: u32,
}

impl ShardRange {
    /// Check if a key hash belongs to this shard.
    pub fn contains(&self, key_hash: u64) -> bool {
        (key_hash % self.total_shards as u64) as ShardId == self.shard_id
    }

    /// Get the start of the range (for display purposes).
    pub fn start(&self) -> u64 {
        self.shard_id as u64
    }

    /// Get the end of the range (for display purposes).
    pub fn end(&self) -> u64 {
        u64::MAX / self.total_shards as u64 * (self.shard_id as u64 + 1)
    }
}

impl std::fmt::Display for ShardRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shard_{}/{}", self.shard_id, self.total_shards)
    }
}

/// Shard assignment for a node.
#[derive(Debug, Clone)]
pub struct ShardAssignment {
    /// Node ID.
    pub node_id: NodeId,

    /// Shards this node is responsible for.
    pub shards: Vec<ShardId>,

    /// Shards where this node is the leader.
    pub leader_shards: Vec<ShardId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_shard_creation() {
        let config = ShardConfig::new(0, 4);
        let shard = Shard::new(config).await.unwrap();

        assert_eq!(shard.id(), 0);
        assert_eq!(shard.state(), ShardState::Initializing);
        assert!(!shard.is_active());
    }

    #[tokio::test]
    async fn test_shard_owns_key() {
        let config = ShardConfig::new(0, 4);
        let shard = Shard::new(config).await.unwrap();

        // Keys with hash % 4 == 0 belong to shard 0
        assert!(shard.owns_key(0));
        assert!(shard.owns_key(4));
        assert!(shard.owns_key(8));
        assert!(!shard.owns_key(1));
        assert!(!shard.owns_key(2));
        assert!(!shard.owns_key(3));
    }

    #[tokio::test]
    async fn test_shard_operations() {
        let config = ShardConfig::new(0, 4);
        let shard = Shard::new(config).await.unwrap();

        shard.set_state(ShardState::Active);
        assert!(shard.is_active());

        // Test put/get
        shard.put(Bytes::from("key1"), Bytes::from("value1")).await;
        let result = shard.get(b"key1").await;
        assert_eq!(result, Some(Bytes::from("value1")));

        // Test delete
        shard.delete(b"key1").await;
        let result = shard.get(b"key1").await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_shard_members() {
        let config = ShardConfig::new(0, 4);
        let shard = Shard::new(config).await.unwrap();

        shard.add_member(1);
        shard.add_member(2);
        shard.add_member(3);

        let members = shard.members();
        assert_eq!(members.len(), 3);
        assert!(members.contains(&1));
        assert!(members.contains(&2));
        assert!(members.contains(&3));

        shard.remove_member(2);
        let members = shard.members();
        assert_eq!(members.len(), 2);
        assert!(!members.contains(&2));
    }

    #[test]
    fn test_shard_range() {
        let range = ShardRange {
            shard_id: 0,
            total_shards: 4,
        };

        assert!(range.contains(0));
        assert!(range.contains(4));
        assert!(range.contains(8));
        assert!(!range.contains(1));
        assert!(!range.contains(2));
        assert!(!range.contains(3));
    }

    #[test]
    fn test_shard_config_builder() {
        let config = ShardConfig::new(1, 8)
            .with_replicas(5)
            .with_max_capacity(500_000)
            .with_default_ttl(Duration::from_secs(3600));

        assert_eq!(config.shard_id, 1);
        assert_eq!(config.total_shards, 8);
        assert_eq!(config.replicas, 5);
        assert_eq!(config.max_capacity, 500_000);
        assert_eq!(config.default_ttl, Some(Duration::from_secs(3600)));
    }
}
