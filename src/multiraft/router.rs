//! Request routing for Multi-Raft.
//!
//! Routes cache operations to the appropriate shard based on key hash.

use super::shard::{Shard, ShardId, ShardInfo};
use crate::error::{Error, Result};
use crate::types::NodeId;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use twox_hash::XxHash64;

/// Configuration for the shard router.
#[derive(Debug, Clone)]
pub struct RouterConfig {
    /// Total number of shards.
    pub num_shards: u32,

    /// Hash seed for consistent hashing.
    pub hash_seed: u64,

    /// Whether to cache shard mappings.
    pub cache_mappings: bool,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            num_shards: 16,
            hash_seed: 0x5AFE_CAFE_DEAD_BEEF,
            cache_mappings: true,
        }
    }
}

impl RouterConfig {
    /// Create a new router config.
    pub fn new(num_shards: u32) -> Self {
        Self {
            num_shards,
            ..Default::default()
        }
    }

    /// Set the hash seed.
    pub fn with_hash_seed(mut self, seed: u64) -> Self {
        self.hash_seed = seed;
        self
    }
}

/// Routes requests to the appropriate shard.
///
/// # Locking Strategy (Hot Path Analysis)
///
/// This router is designed for high-throughput scenarios with the following
/// locking characteristics:
///
/// ## Read Path (`get`, `put`, `delete`)
/// 1. `shard_for_key()`: Read lock on `key_cache` (fast cache lookup)
///    - Cache hit: Returns immediately with read lock only
///    - Cache miss: Brief write lock to populate cache (amortized over many ops)
/// 2. `get_shard()`: Read lock on `shards` HashMap
/// 3. Actual data operation: Lock-free (Moka cache uses lock-free data structures)
///
/// ## Write Path (Topology Changes)
/// - `register_shard()`, `unregister_shard()`: Write lock on `shards`
/// - These are rare control-plane operations, not hot path
///
/// ## Performance Characteristics
/// - Read locks allow full concurrency for get/put operations
/// - `parking_lot::RwLock` provides better performance than std's RwLock
/// - Cache is bounded to 100K entries to prevent memory bloat
/// - After warmup, cache hit rate is high (read locks dominate)
///
/// ## Potential Improvements for Extreme Throughput
/// - Replace `RwLock<HashMap>` with `DashMap` for shards (avoid all locks)
/// - Use `ArcSwap` for read-heavy shard lookups with rare updates
/// - Pre-populate key_cache on startup for known hot keys
#[derive(Debug)]
pub struct ShardRouter {
    /// Configuration.
    config: RouterConfig,

    /// Map of shard ID to shard instance.
    /// Uses RwLock for safe concurrent access. Read locks dominate in steady state.
    shards: RwLock<HashMap<ShardId, Arc<Shard>>>,

    /// Map of shard ID to leader node.
    /// Updated during leader elections and topology changes.
    shard_leaders: RwLock<HashMap<ShardId, NodeId>>,

    /// Cache of key hash to shard ID (optional optimization).
    /// Reduces repeated hash-to-shard calculations for hot keys.
    /// Bounded to 100K entries. Must be cleared on topology changes.
    key_cache: Option<RwLock<HashMap<u64, ShardId>>>,
}

impl ShardRouter {
    /// Create a new shard router.
    pub fn new(config: RouterConfig) -> Self {
        let key_cache = if config.cache_mappings {
            Some(RwLock::new(HashMap::new()))
        } else {
            None
        };

        Self {
            config,
            shards: RwLock::new(HashMap::new()),
            shard_leaders: RwLock::new(HashMap::new()),
            key_cache,
        }
    }

    /// Create with default config.
    pub fn with_defaults() -> Self {
        Self::new(RouterConfig::default())
    }

    /// Get the number of shards.
    pub fn num_shards(&self) -> u32 {
        self.config.num_shards
    }

    /// Hash a key to get its shard assignment.
    pub fn hash_key(&self, key: &[u8]) -> u64 {
        let mut hasher = XxHash64::with_seed(self.config.hash_seed);
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Get the shard ID for a key.
    pub fn shard_for_key(&self, key: &[u8]) -> ShardId {
        let hash = self.hash_key(key);

        // Check cache first
        if let Some(ref cache) = self.key_cache {
            if let Some(&shard_id) = cache.read().get(&hash) {
                return shard_id;
            }
        }

        let shard_id = (hash % self.config.num_shards as u64) as ShardId;

        // Cache the result
        if let Some(ref cache) = self.key_cache {
            // Limit cache size
            let mut cache = cache.write();
            if cache.len() < 100_000 {
                cache.insert(hash, shard_id);
            }
        }

        shard_id
    }

    /// Register a shard with the router.
    pub fn register_shard(&self, shard: Arc<Shard>) {
        let shard_id = shard.id();
        self.shards.write().insert(shard_id, shard);
    }

    /// Unregister a shard from the router.
    pub fn unregister_shard(&self, shard_id: ShardId) -> Option<Arc<Shard>> {
        self.shards.write().remove(&shard_id)
    }

    /// Get a shard by ID.
    pub fn get_shard(&self, shard_id: ShardId) -> Option<Arc<Shard>> {
        self.shards.read().get(&shard_id).cloned()
    }

    /// Get the shard for a key.
    pub fn get_shard_for_key(&self, key: &[u8]) -> Option<Arc<Shard>> {
        let shard_id = self.shard_for_key(key);
        self.get_shard(shard_id)
    }

    /// Get all registered shards.
    pub fn all_shards(&self) -> Vec<Arc<Shard>> {
        self.shards.read().values().cloned().collect()
    }

    /// Get all shard IDs.
    pub fn shard_ids(&self) -> Vec<ShardId> {
        self.shards.read().keys().copied().collect()
    }

    /// Update the leader for a shard.
    pub fn set_shard_leader(&self, shard_id: ShardId, leader: NodeId) {
        self.shard_leaders.write().insert(shard_id, leader);

        // Also update the shard itself
        if let Some(shard) = self.get_shard(shard_id) {
            shard.set_leader(Some(leader));
        }
    }

    /// Get the leader for a shard.
    pub fn get_shard_leader(&self, shard_id: ShardId) -> Option<NodeId> {
        self.shard_leaders.read().get(&shard_id).copied()
    }

    /// Get the leader for a key.
    pub fn get_leader_for_key(&self, key: &[u8]) -> Option<NodeId> {
        let shard_id = self.shard_for_key(key);
        self.get_shard_leader(shard_id)
    }

    /// Get a value from the appropriate shard.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let shard = self
            .get_shard_for_key(key)
            .ok_or_else(|| Error::ShardNotFound(self.shard_for_key(key)))?;

        Ok(shard.get(key).await)
    }

    /// Put a value in the appropriate shard.
    pub async fn put(&self, key: Bytes, value: Bytes) -> Result<ShardId> {
        let shard = self
            .get_shard_for_key(&key)
            .ok_or_else(|| Error::ShardNotFound(self.shard_for_key(&key)))?;

        shard.put(key, value).await;
        Ok(shard.id())
    }

    /// Put a value with TTL in the appropriate shard.
    pub async fn put_with_ttl(&self, key: Bytes, value: Bytes, ttl: Duration) -> Result<ShardId> {
        let shard = self
            .get_shard_for_key(&key)
            .ok_or_else(|| Error::ShardNotFound(self.shard_for_key(&key)))?;

        shard.put_with_ttl(key, value, ttl).await;
        Ok(shard.id())
    }

    /// Delete a key from the appropriate shard.
    pub async fn delete(&self, key: &[u8]) -> Result<ShardId> {
        let shard = self
            .get_shard_for_key(key)
            .ok_or_else(|| Error::ShardNotFound(self.shard_for_key(key)))?;

        shard.delete(key).await;
        Ok(shard.id())
    }

    /// Get info about all shards.
    pub fn shard_info(&self) -> Vec<ShardInfo> {
        self.all_shards().iter().map(|s| s.info()).collect()
    }

    /// Get total entry count across all shards.
    pub fn total_entries(&self) -> u64 {
        self.all_shards()
            .iter()
            .map(|s| s.storage().entry_count())
            .sum()
    }

    /// Get total size across all shards.
    pub fn total_size(&self) -> u64 {
        self.all_shards()
            .iter()
            .map(|s| s.storage().weighted_size())
            .sum()
    }

    /// Clear the key cache.
    pub fn clear_cache(&self) {
        if let Some(ref cache) = self.key_cache {
            cache.write().clear();
        }
    }
}

impl Default for ShardRouter {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Routing decision for a request.
#[derive(Debug, Clone)]
pub struct RoutingDecision {
    /// Target shard ID.
    pub shard_id: ShardId,

    /// Target node (leader of the shard).
    pub target_node: Option<NodeId>,

    /// Whether the target is local.
    pub is_local: bool,

    /// Key hash used for routing.
    pub key_hash: u64,
}

/// Batch routing for multiple keys.
#[derive(Debug)]
pub struct BatchRouter {
    /// The underlying router.
    router: Arc<ShardRouter>,
}

impl BatchRouter {
    /// Create a new batch router.
    pub fn new(router: Arc<ShardRouter>) -> Self {
        Self { router }
    }

    /// Route a batch of keys to their shards.
    pub fn route_batch(&self, keys: &[&[u8]]) -> HashMap<ShardId, Vec<usize>> {
        let mut result: HashMap<ShardId, Vec<usize>> = HashMap::new();

        for (idx, key) in keys.iter().enumerate() {
            let shard_id = self.router.shard_for_key(key);
            result.entry(shard_id).or_default().push(idx);
        }

        result
    }

    /// Get routing decisions for multiple keys.
    pub fn get_routing_decisions(&self, keys: &[&[u8]], local_node: NodeId) -> Vec<RoutingDecision> {
        keys.iter()
            .map(|key| {
                let hash = self.router.hash_key(key);
                let shard_id = (hash % self.router.num_shards() as u64) as ShardId;
                let target_node = self.router.get_shard_leader(shard_id);
                let is_local = target_node.map(|n| n == local_node).unwrap_or(false);

                RoutingDecision {
                    shard_id,
                    target_node,
                    is_local,
                    key_hash: hash,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multiraft::shard::ShardConfig;

    #[test]
    fn test_router_hash_consistency() {
        let router = ShardRouter::with_defaults();

        // Same key should always hash to same shard
        let shard1 = router.shard_for_key(b"test-key");
        let shard2 = router.shard_for_key(b"test-key");
        assert_eq!(shard1, shard2);
    }

    #[test]
    fn test_router_distribution() {
        let router = ShardRouter::new(RouterConfig::new(4));

        let mut distribution = HashMap::new();

        // Generate 1000 keys and count distribution
        for i in 0..1000 {
            let key = format!("key-{}", i);
            let shard_id = router.shard_for_key(key.as_bytes());
            *distribution.entry(shard_id).or_insert(0) += 1;
        }

        // Each shard should have approximately 250 keys (1000/4)
        // Allow 20% variance
        for &count in distribution.values() {
            assert!(count > 150, "Shard has too few keys: {}", count);
            assert!(count < 350, "Shard has too many keys: {}", count);
        }
    }

    #[tokio::test]
    async fn test_router_shard_registration() {
        let router = ShardRouter::new(RouterConfig::new(4));

        // Register shards
        for i in 0..4 {
            let config = ShardConfig::new(i, 4);
            let shard = Arc::new(Shard::new(config).await.unwrap());
            router.register_shard(shard);
        }

        assert_eq!(router.all_shards().len(), 4);

        // Test getting shard for key
        let key = b"test-key";
        let shard = router.get_shard_for_key(key);
        assert!(shard.is_some());
    }

    #[tokio::test]
    async fn test_router_operations() {
        let router = ShardRouter::new(RouterConfig::new(4));

        // Register shards
        for i in 0..4 {
            let config = ShardConfig::new(i, 4);
            let shard = Arc::new(Shard::new(config).await.unwrap());
            shard.set_state(crate::multiraft::shard::ShardState::Active);
            router.register_shard(shard);
        }

        // Test put/get
        let key = Bytes::from("test-key");
        let value = Bytes::from("test-value");

        router.put(key.clone(), value.clone()).await.unwrap();
        let result = router.get(&key).await.unwrap();
        assert_eq!(result, Some(value));

        // Test delete
        router.delete(&key).await.unwrap();
        let result = router.get(&key).await.unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_router_leader_tracking() {
        let router = ShardRouter::with_defaults();

        router.set_shard_leader(0, 1);
        router.set_shard_leader(1, 2);
        router.set_shard_leader(2, 3);

        assert_eq!(router.get_shard_leader(0), Some(1));
        assert_eq!(router.get_shard_leader(1), Some(2));
        assert_eq!(router.get_shard_leader(2), Some(3));
        assert_eq!(router.get_shard_leader(3), None);
    }

    #[test]
    fn test_batch_router() {
        let router = Arc::new(ShardRouter::new(RouterConfig::new(4)));
        let batch_router = BatchRouter::new(router);

        let keys: Vec<&[u8]> = vec![b"key1", b"key2", b"key3", b"key4", b"key5"];
        let routing = batch_router.route_batch(&keys);

        // All keys should be routed
        let total_keys: usize = routing.values().map(|v| v.len()).sum();
        assert_eq!(total_keys, 5);
    }

    #[test]
    fn test_routing_decision() {
        let router = Arc::new(ShardRouter::new(RouterConfig::new(4)));
        router.set_shard_leader(0, 1);
        router.set_shard_leader(1, 2);

        let batch_router = BatchRouter::new(router);
        let keys: Vec<&[u8]> = vec![b"key1", b"key2"];
        let decisions = batch_router.get_routing_decisions(&keys, 1);

        assert_eq!(decisions.len(), 2);
        for decision in decisions {
            assert!(decision.shard_id < 4);
        }
    }
}
