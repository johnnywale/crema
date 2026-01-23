//! Moka cache wrapper for local storage.

use crate::config::CacheConfig;
use crate::types::CacheStats;
use bytes::Bytes;
use moka::future::Cache;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Local cache storage backed by Moka.
pub struct CacheStorage {
    /// The underlying Moka cache.
    cache: Cache<Bytes, Bytes>,

    /// Hit counter for statistics.
    hits: AtomicU64,

    /// Miss counter for statistics.
    misses: AtomicU64,

    /// Expiration times for entries with per-entry TTL.
    /// Maps key to absolute expiration time in milliseconds since Unix epoch.
    /// Entries without TTL are not present in this map.
    expirations: RwLock<HashMap<Bytes, u64>>,
}

impl CacheStorage {
    /// Create a new cache storage with the given configuration.
    pub fn new(config: &CacheConfig) -> Self {
        let mut builder = Cache::builder().max_capacity(config.max_capacity);

        // Set TTL if configured
        if let Some(ttl) = config.default_ttl {
            builder = builder.time_to_live(ttl);
        }

        // Set TTI if configured
        if let Some(tti) = config.default_tti {
            builder = builder.time_to_idle(tti);
        }

        // Set weigher for memory-aware sizing
        builder = builder.weigher(|key: &Bytes, value: &Bytes| {
            // Weight is key + value size in bytes, capped at u32::MAX
            let size = key.len() + value.len();
            size.min(u32::MAX as usize) as u32
        });

        let cache = builder.build();

        Self {
            cache,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            expirations: RwLock::new(HashMap::new()),
        }
    }

    /// Get a value from the cache.
    pub async fn get(&self, key: &[u8]) -> Option<Bytes> {
        let key = Bytes::copy_from_slice(key);
        let result = self.cache.get(&key).await;

        if result.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }

        result
    }
    

    /// Check if a key exists in the cache.
    pub fn contains(&self, key: &[u8]) -> bool {
        let key = Bytes::copy_from_slice(key);
        self.cache.contains_key(&key)
    }

    /// Insert a key-value pair into the cache.
    pub async fn insert(&self, key: Bytes, value: Bytes) {
        self.cache.insert(key, value).await;
    }

    /// Insert a key-value pair with a custom TTL.
    ///
    /// The TTL is tracked separately from Moka (which doesn't expose per-entry TTL).
    /// This allows snapshots to include expiration times for proper recovery.
    pub async fn insert_with_ttl(&self, key: Bytes, value: Bytes, ttl: Duration) {
        use std::time::{SystemTime, UNIX_EPOCH};

        // Calculate absolute expiration time
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let expires_at_ms = now_ms + ttl.as_millis() as u64;

        // Store expiration time
        self.expirations.write().insert(key.clone(), expires_at_ms);

        // Insert into Moka cache
        self.cache.insert(key, value).await;
    }

    /// Insert a key-value pair with an absolute expiration time.
    ///
    /// Used when restoring from snapshots where we have the absolute expiration time.
    pub async fn insert_with_expiration(&self, key: Bytes, value: Bytes, expires_at_ms: u64) {
        use std::time::{SystemTime, UNIX_EPOCH};

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Only insert if not expired
        if expires_at_ms > now_ms {
            self.expirations.write().insert(key.clone(), expires_at_ms);
            self.cache.insert(key, value).await;
        }
    }

    /// Invalidate (remove) a key from the cache.
    pub async fn invalidate(&self, key: &[u8]) {
        let key = Bytes::copy_from_slice(key);
        self.expirations.write().remove(&key);
        self.cache.invalidate(&key).await;
    }

    /// Invalidate all entries in the cache.
    pub fn invalidate_all(&self) {
        self.expirations.write().clear();
        self.cache.invalidate_all();
    }

    /// Get the number of entries in the cache.
    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Get the weighted size of the cache.
    pub fn weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            entry_count: self.cache.entry_count(),
            weighted_size: self.cache.weighted_size(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }

    /// Run pending maintenance tasks (cleanup expired entries, etc.).
    pub async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks().await;
    }

    /// Iterate over all entries in the cache.
    ///
    /// Returns an iterator over (key, value) pairs. The iterator visits entries
    /// in arbitrary order and does not update popularity estimators or reset
    /// idle timers.
    ///
    /// Note: Due to concurrent access, newly inserted entries may or may not
    /// appear in the iteration, but removed entries will not be returned.
    pub fn iter(&self) -> impl Iterator<Item = (Arc<Bytes>, Bytes)> + '_ {
        self.cache.iter()
    }

    /// Collect all entries for snapshot creation (without expiration times).
    ///
    /// Returns a vector of (key, value) pairs representing the current cache state.
    /// This is used for creating Raft snapshots to transfer state to followers.
    ///
    /// Note: This is a point-in-time snapshot. Concurrent modifications may not
    /// be fully captured.
    pub fn collect_entries(&self) -> Vec<(Bytes, Bytes)> {
        self.cache
            .iter()
            .map(|(k, v)| ((*k).clone(), v))
            .collect()
    }

    /// Collect all entries with their expiration times for snapshot creation.
    ///
    /// Returns a vector of (key, value, expires_at_ms) tuples.
    /// expires_at_ms is None for entries without TTL.
    ///
    /// Note: This is a point-in-time snapshot. Concurrent modifications may not
    /// be fully captured.
    pub fn collect_entries_with_expiration(&self) -> Vec<(Bytes, Bytes, Option<u64>)> {
        let expirations = self.expirations.read();
        self.cache
            .iter()
            .map(|(k, v)| {
                let key = (*k).clone();
                let expires_at = expirations.get(&key).copied();
                (key, v, expires_at)
            })
            .collect()
    }

    /// Get the expiration time for a key.
    ///
    /// Returns None if the key has no expiration or doesn't exist.
    pub fn get_expiration(&self, key: &[u8]) -> Option<u64> {
        let key = Bytes::copy_from_slice(key);
        self.expirations.read().get(&key).copied()
    }
}

impl std::fmt::Debug for CacheStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheStorage")
            .field("entry_count", &self.entry_count())
            .field("weighted_size", &self.weighted_size())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> CacheConfig {
        CacheConfig {
            max_capacity: 1000,
            default_ttl: Some(Duration::from_secs(60)),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let storage = CacheStorage::new(&test_config());

        let key = Bytes::from("key1");
        let value = Bytes::from("value1");

        storage.insert(key.clone(), value.clone()).await;

        let result = storage.get(b"key1").await;
        assert_eq!(result, Some(value));
    }

    #[tokio::test]
    async fn test_contains() {
        let storage = CacheStorage::new(&test_config());

        let key = Bytes::from("key1");
        let value = Bytes::from("value1");

        assert!(!storage.contains(b"key1"));

        storage.insert(key, value).await;

        assert!(storage.contains(b"key1"));
    }

    #[tokio::test]
    async fn test_invalidate() {
        let storage = CacheStorage::new(&test_config());

        let key = Bytes::from("key1");
        let value = Bytes::from("value1");

        storage.insert(key, value).await;
        assert!(storage.contains(b"key1"));

        storage.invalidate(b"key1").await;
        assert!(!storage.contains(b"key1"));
    }

    #[tokio::test]
    async fn test_invalidate_all() {
        let storage = CacheStorage::new(&test_config());

        storage
            .insert(Bytes::from("key1"), Bytes::from("value1"))
            .await;
        storage
            .insert(Bytes::from("key2"), Bytes::from("value2"))
            .await;

        // Sync to ensure entries are written
        storage.cache.run_pending_tasks().await;

        storage.invalidate_all();

        // Note: invalidate_all is lazy, so we need to wait and run tasks
        tokio::time::sleep(Duration::from_millis(50)).await;
        storage.cache.run_pending_tasks().await;

        assert_eq!(storage.entry_count(), 0);
    }

    #[tokio::test]
    async fn test_stats() {
        let storage = CacheStorage::new(&test_config());

        storage
            .insert(Bytes::from("key1"), Bytes::from("value1"))
            .await;

        // Ensure entry is written
        storage.cache.run_pending_tasks().await;

        // Hit
        let _ = storage.get(b"key1").await;
        // Miss
        let _ = storage.get(b"nonexistent").await;

        let stats = storage.stats();
        assert_eq!(stats.entry_count, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }
}
