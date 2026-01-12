//! Moka cache wrapper for local storage.

use crate::config::CacheConfig;
use crate::types::CacheStats;
use bytes::Bytes;
use moka::future::Cache;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Local cache storage backed by Moka.
pub struct CacheStorage {
    /// The underlying Moka cache.
    cache: Cache<Bytes, Bytes>,

    /// Hit counter for statistics.
    hits: AtomicU64,

    /// Miss counter for statistics.
    misses: AtomicU64,
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
    pub async fn insert_with_ttl(&self, key: Bytes, value: Bytes, _ttl: Duration) {
        // Moka doesn't support per-entry TTL directly in the basic API,
        // but we can use the expire_after policy or a workaround.
        // For now, we'll use the cache's policy and note this limitation.
        // In a production implementation, you'd use expire_after.
        self.cache.insert(key, value).await;
        // TODO: Implement per-entry TTL using Moka's expiry policy
    }

    /// Invalidate (remove) a key from the cache.
    pub async fn invalidate(&self, key: &[u8]) {
        let key = Bytes::copy_from_slice(key);
        self.cache.invalidate(&key).await;
    }

    /// Invalidate all entries in the cache.
    pub fn invalidate_all(&self) {
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
