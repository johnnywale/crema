//! Shard management for Multi-Raft.
//!
//! A shard is a single Raft group managing a subset of the keyspace.
//!
//! # Phase 1 vs Phase 2
//!
//! - **Phase 1**: Each shard is a local Moka cache with metadata tracking.
//!   No actual Raft consensus - writes go directly to local storage.
//!
//! - **Phase 2**: Each shard becomes an independent Raft group with real
//!   replication across nodes. Writes are proposed to Raft and committed
//!   before being applied to local storage.

use crate::cache::storage::CacheStorage;
use crate::config::CacheConfig;
use crate::error::Result;
use crate::types::{CacheCommand, NodeId};
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::shard_raft_node::ShardRaftNode;

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

    /// Per-shard RaftNode for Phase 2 replication (optional).
    /// When Some, writes are proposed through Raft consensus.
    /// When None, writes go directly to local storage (Phase 1 behavior).
    raft_node: RwLock<Option<Arc<ShardRaftNode>>>,
}

impl Shard {
    /// Create a new shard.
    pub async fn new(config: ShardConfig) -> Result<Self> {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        // Create cache storage for this shard (placeholder address, not used for Raft)
        let placeholder_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let cache_config =
            CacheConfig::new(0, placeholder_addr).with_max_capacity(config.max_capacity);

        let storage = Arc::new(CacheStorage::new(&cache_config));

        Ok(Self {
            config,
            state: RwLock::new(ShardState::Initializing),
            storage,
            leader: RwLock::new(None),
            members: RwLock::new(HashSet::new()),
            term: AtomicU64::new(0),
            commit_index: AtomicU64::new(0),
            applied_index: AtomicU64::new(0),
            is_local_leader: RwLock::new(false),
            raft_node: RwLock::new(None),
        })
    }

    /// Create a new shard with an existing storage.
    pub fn new_with_storage(config: ShardConfig, storage: Arc<CacheStorage>) -> Self {
        Self {
            config,
            state: RwLock::new(ShardState::Initializing),
            storage,
            leader: RwLock::new(None),
            members: RwLock::new(HashSet::new()),
            term: AtomicU64::new(0),
            commit_index: AtomicU64::new(0),
            applied_index: AtomicU64::new(0),
            is_local_leader: RwLock::new(false),
            raft_node: RwLock::new(None),
        }
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

    /// Put a value in this shard.
    ///
    /// In Phase 2 (per-shard Raft enabled), this proposes via Raft.
    /// In Phase 1, this writes directly to local storage.
    ///
    /// Returns `NotLeader` error if this node isn't the shard leader in Phase 2.
    pub async fn put(&self, key: Bytes, value: Bytes) -> crate::error::Result<()> {
        // Check if per-shard Raft is enabled
        if self.is_raft_enabled() {
            // Use Raft for replication - propagate errors (including NotLeader)
            self.propose_put(key, value, None).await
        } else {
            // Direct local write (Phase 1)
            self.storage.insert(key, value).await;
            Ok(())
        }
    }

    /// Put a value with TTL.
    ///
    /// In Phase 2 (per-shard Raft enabled), this proposes via Raft.
    /// In Phase 1, this writes directly to local storage.
    ///
    /// Returns `NotLeader` error if this node isn't the shard leader in Phase 2.
    pub async fn put_with_ttl(
        &self,
        key: Bytes,
        value: Bytes,
        ttl: Duration,
    ) -> crate::error::Result<()> {
        // Check if per-shard Raft is enabled
        if self.is_raft_enabled() {
            // Use Raft for replication - propagate errors (including NotLeader)
            self.propose_put(key, value, Some(ttl)).await
        } else {
            // Direct local write (Phase 1)
            self.storage.insert_with_ttl(key, value, ttl).await;
            Ok(())
        }
    }

    /// Delete a key from this shard.
    ///
    /// In Phase 2 (per-shard Raft enabled), this proposes via Raft.
    /// In Phase 1, this deletes directly from local storage.
    ///
    /// Returns `NotLeader` error if this node isn't the shard leader in Phase 2.
    pub async fn delete(&self, key: &[u8]) -> crate::error::Result<()> {
        // Check if per-shard Raft is enabled
        if self.is_raft_enabled() {
            // Use Raft for replication - propagate errors (including NotLeader)
            self.propose_delete(key).await
        } else {
            // Direct local delete (Phase 1)
            self.storage.invalidate(key).await;
            Ok(())
        }
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

    /// Check if this node is the leader for this shard (cached value).
    ///
    /// **Note**: This returns a cached value that may be stale. For accurate
    /// leadership status when per-shard Raft is enabled, use `is_raft_leader()`
    /// which queries the actual RaftNode state.
    pub fn is_leader(&self) -> bool {
        *self.is_local_leader.read()
    }

    /// Set whether this node is the leader (cached value).
    ///
    /// This updates the cached leadership status. In Phase 2 (per-shard Raft),
    /// prefer checking `is_raft_leader()` for accurate real-time status.
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

    // ==================== Per-Shard Raft Methods (Phase 2) ====================

    /// Check if per-shard Raft is enabled for this shard.
    pub fn is_raft_enabled(&self) -> bool {
        self.raft_node.read().is_some()
    }

    /// Get the ShardRaftNode if enabled.
    pub fn raft_node(&self) -> Option<Arc<ShardRaftNode>> {
        self.raft_node.read().clone()
    }

    /// Set the ShardRaftNode for this shard.
    ///
    /// This enables per-shard Raft replication (Phase 2).
    pub fn set_raft_node(&self, raft_node: Arc<ShardRaftNode>) {
        *self.raft_node.write() = Some(raft_node);
        tracing::info!(shard_id = self.config.shard_id, "Per-shard Raft enabled");
    }

    /// Clear the ShardRaftNode, reverting to Phase 1 behavior.
    pub fn clear_raft_node(&self) {
        *self.raft_node.write() = None;
        tracing::info!(shard_id = self.config.shard_id, "Per-shard Raft disabled");
    }

    /// Propose a Put command through Raft (Phase 2).
    ///
    /// If Raft is enabled, proposes the command and waits for commit.
    /// If Raft is not enabled, writes directly to local storage (Phase 1).
    pub async fn propose_put(&self, key: Bytes, value: Bytes, ttl: Option<Duration>) -> Result<()> {
        // Clone the raft_node reference while holding the lock briefly
        let maybe_raft_node = self.raft_node.read().clone();

        if let Some(raft_node) = maybe_raft_node {
            // Phase 2: Propose through Raft
            let expires_at_ms = ttl.map(|d| {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64;
                let ttl_ms = d.as_millis().min(u128::from(u64::MAX)) as u64;
                now_ms.saturating_add(ttl_ms)
            });

            let cmd = CacheCommand::Put {
                key: key.to_vec(),
                value: value.to_vec(),
                expires_at_ms,
            };

            raft_node.propose(cmd).await?;
            Ok(())
        } else {
            // Phase 1: Direct local write
            if let Some(ttl) = ttl {
                self.storage.insert_with_ttl(key, value, ttl).await;
            } else {
                self.storage.insert(key, value).await;
            }
            self.applied_index.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    /// Propose a Delete command through Raft (Phase 2).
    ///
    /// If Raft is enabled, proposes the command and waits for commit.
    /// If Raft is not enabled, deletes directly from local storage (Phase 1).
    pub async fn propose_delete(&self, key: &[u8]) -> Result<()> {
        // Clone the raft_node reference while holding the lock briefly
        let maybe_raft_node = self.raft_node.read().clone();

        if let Some(raft_node) = maybe_raft_node {
            // Phase 2: Propose through Raft
            let cmd = CacheCommand::Delete { key: key.to_vec() };
            raft_node.propose(cmd).await?;
            Ok(())
        } else {
            // Phase 1: Direct local delete
            self.storage.invalidate(key).await;
            self.applied_index.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    /// Clear all entries from this shard.
    ///
    /// If Raft is enabled, proposes the command and waits for commit.
    /// If Raft is not enabled, clears directly from local storage (Phase 1).
    pub async fn propose_clear(&self) -> Result<()> {
        // Clone the raft_node reference while holding the lock briefly
        let maybe_raft_node = self.raft_node.read().clone();

        if let Some(raft_node) = maybe_raft_node {
            // Phase 2: Propose through Raft
            let cmd = CacheCommand::Clear;
            raft_node.propose(cmd).await?;
            Ok(())
        } else {
            // Phase 1: Direct local clear
            self.storage.invalidate_all();
            self.applied_index.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    /// Get the Raft leader for this shard.
    ///
    /// Returns the leader from the ShardRaftNode if enabled,
    /// otherwise falls back to the locally tracked leader.
    pub fn raft_leader(&self) -> Option<NodeId> {
        if let Some(ref raft_node) = *self.raft_node.read() {
            raft_node.leader_id()
        } else {
            self.leader()
        }
    }

    /// Check if this node is the Raft leader for this shard.
    pub fn is_raft_leader(&self) -> bool {
        if let Some(ref raft_node) = *self.raft_node.read() {
            raft_node.is_leader()
        } else {
            self.is_leader()
        }
    }

    /// Get the current Raft term for this shard.
    pub fn raft_term(&self) -> u64 {
        if let Some(ref raft_node) = *self.raft_node.read() {
            raft_node.term()
        } else {
            self.term()
        }
    }

    /// Get the commit index from the ShardRaftNode if enabled.
    pub fn raft_commit_index(&self) -> u64 {
        if let Some(ref raft_node) = *self.raft_node.read() {
            raft_node.commit_index()
        } else {
            self.commit_index()
        }
    }

    /// Get the applied index from the ShardRaftNode if enabled.
    pub fn raft_applied_index(&self) -> u64 {
        if let Some(ref raft_node) = *self.raft_node.read() {
            raft_node.applied_index()
        } else {
            self.applied_index()
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
        let _ = shard.put(Bytes::from("key1"), Bytes::from("value1")).await;
        let result = shard.get(b"key1").await;
        assert_eq!(result, Some(Bytes::from("value1")));

        // Test delete
        let _ = shard.delete(b"key1").await;
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
