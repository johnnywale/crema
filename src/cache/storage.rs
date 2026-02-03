//! Moka cache wrapper for local storage.

use crate::config::CacheConfig;
use crate::types::CacheStats;
use bytes::Bytes;
use moka::future::Cache;
use parking_lot::RwLock;
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Slot ID for slot-based key routing.
pub type SlotId = u16;

/// Total number of slots (matching Redis Cluster).
pub const TOTAL_SLOTS: u16 = 1024;

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

    /// Optional slot index for O(1) slot-based key lookups.
    /// Maps slot_id -> set of keys in that slot.
    /// This is only populated when slot indexing is enabled.
    slot_index: RwLock<Option<HashMap<SlotId, BTreeSet<Bytes>>>>,
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
            slot_index: RwLock::new(None),
        }
    }

    /// Enable slot indexing for O(1) slot-based key lookups.
    ///
    /// When enabled, the storage maintains an index mapping slot_id -> keys.
    /// This enables efficient scanning for migration without O(N) iteration per batch.
    ///
    /// If `rebuild` is true, rebuilds the index from existing data (O(N) once).
    pub fn enable_slot_indexing(&self) {
        let mut guard = self.slot_index.write();
        if guard.is_none() {
            let mut index: HashMap<SlotId, BTreeSet<Bytes>> = HashMap::new();

            // Build index from existing data (O(N) once)
            for (key_arc, _) in self.cache.iter() {
                let key = (*key_arc).clone();
                let slot = Self::key_to_slot(&key);
                index.entry(slot).or_default().insert(key);
            }

            *guard = Some(index);
        }
    }

    /// Check if slot indexing is enabled.
    pub fn has_slot_indexing(&self) -> bool {
        self.slot_index.read().is_some()
    }

    /// Compute the slot for a key using CRC16 (XMODEM variant).
    #[inline]
    pub fn key_to_slot(key: &[u8]) -> SlotId {
        let crc = crc16(key);
        crc % TOTAL_SLOTS
    }

    /// Add a key to the slot index.
    fn add_to_slot_index(&self, key: &Bytes) {
        if let Some(ref mut index) = *self.slot_index.write() {
            let slot = Self::key_to_slot(key);
            index.entry(slot).or_default().insert(key.clone());
        }
    }

    /// Remove a key from the slot index.
    fn remove_from_slot_index(&self, key: &Bytes) {
        if let Some(ref mut index) = *self.slot_index.write() {
            let slot = Self::key_to_slot(key);
            if let Some(keys) = index.get_mut(&slot) {
                keys.remove(key);
                if keys.is_empty() {
                    index.remove(&slot);
                }
            }
        }
    }

    /// Scan keys belonging to a specific slot with cursor-based pagination.
    ///
    /// Returns (keys, next_cursor). If next_cursor is None, all keys have been scanned.
    /// Uses the slot index for O(keys_in_slot) instead of O(total_keys) iteration.
    pub fn scan_slot_keys(
        &self,
        slot_id: SlotId,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> (Vec<Bytes>, Option<Bytes>) {
        let guard = self.slot_index.read();

        if let Some(ref index) = *guard {
            // Fast path: use slot index
            if let Some(slot_keys) = index.get(&slot_id) {
                let cursor_bytes = cursor.map(Bytes::copy_from_slice);
                let mut keys = Vec::with_capacity(limit.min(slot_keys.len()));
                let mut next_cursor = None;

                // Use BTreeSet's range iteration for efficient cursor-based scanning
                let iter = if let Some(ref c) = cursor_bytes {
                    // Start from the key after cursor
                    slot_keys.range::<Bytes, _>((
                        std::ops::Bound::Excluded(c.clone()),
                        std::ops::Bound::Unbounded,
                    ))
                } else {
                    slot_keys.range::<Bytes, _>(..)
                };

                for key in iter {
                    keys.push(key.clone());
                    if keys.len() >= limit {
                        next_cursor = Some(key.clone());
                        break;
                    }
                }

                return (keys, next_cursor);
            }
            // Slot not in index - no keys
            return (Vec::new(), None);
        }

        // Slow path: no slot index, fall back to full iteration
        self.scan_slot_keys_slow(slot_id, cursor, limit)
    }

    /// Slow path for slot scanning when index is not available.
    fn scan_slot_keys_slow(
        &self,
        slot_id: SlotId,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> (Vec<Bytes>, Option<Bytes>) {
        let cursor_bytes = cursor.map(Bytes::copy_from_slice);
        let mut keys = Vec::with_capacity(limit);
        let mut next_cursor = None;

        for (key_arc, _) in self.cache.iter() {
            let key = (*key_arc).clone();

            // Skip keys until we reach the cursor position
            if let Some(ref c) = cursor_bytes {
                if key <= *c {
                    continue;
                }
            }

            // Check if key belongs to this slot
            if Self::key_to_slot(&key) == slot_id {
                keys.push(key.clone());
                if keys.len() >= limit {
                    next_cursor = Some(key);
                    break;
                }
            }
        }

        (keys, next_cursor)
    }

    /// Count keys in a specific slot.
    pub fn count_slot_keys(&self, slot_id: SlotId) -> u64 {
        let guard = self.slot_index.read();
        if let Some(ref index) = *guard {
            index.get(&slot_id).map(|s| s.len() as u64).unwrap_or(0)
        } else {
            // Slow path
            self.cache
                .iter()
                .filter(|(k, _)| Self::key_to_slot(k) == slot_id)
                .count() as u64
        }
    }

    /// Get all keys in a specific slot.
    pub fn get_slot_keys(&self, slot_id: SlotId) -> Vec<Bytes> {
        let guard = self.slot_index.read();
        if let Some(ref index) = *guard {
            index
                .get(&slot_id)
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default()
        } else {
            self.cache
                .iter()
                .filter(|(k, _)| Self::key_to_slot(k) == slot_id)
                .map(|(k, _)| (*k).clone())
                .collect()
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
        self.add_to_slot_index(&key);
        self.cache.insert(key, value).await;
    }

    /// Insert a key-value pair with a custom TTL.
    ///
    /// The TTL is tracked separately from Moka (which doesn't expose per-entry TTL).
    /// This allows snapshots to include expiration times for proper recovery.
    pub async fn insert_with_ttl(&self, key: Bytes, value: Bytes, ttl: Duration) {
        use std::time::{SystemTime, UNIX_EPOCH};

        // Calculate absolute expiration time (using saturating operations to prevent overflow)
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let ttl_ms = ttl.as_millis().min(u128::from(u64::MAX)) as u64;
        let expires_at_ms = now_ms.saturating_add(ttl_ms);

        // Store expiration time
        self.expirations.write().insert(key.clone(), expires_at_ms);

        // Update slot index
        self.add_to_slot_index(&key);

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
            self.add_to_slot_index(&key);
            self.cache.insert(key, value).await;
        }
    }

    /// Invalidate (remove) a key from the cache.
    pub async fn invalidate(&self, key: &[u8]) {
        let key = Bytes::copy_from_slice(key);
        self.expirations.write().remove(&key);
        self.remove_from_slot_index(&key);
        self.cache.invalidate(&key).await;
    }

    /// Invalidate all entries in the cache.
    pub fn invalidate_all(&self) {
        self.expirations.write().clear();
        // Clear slot index
        if let Some(ref mut index) = *self.slot_index.write() {
            index.clear();
        }
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
        self.cache.iter().map(|(k, v)| ((*k).clone(), v)).collect()
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
            .field("slot_indexed", &self.has_slot_indexing())
            .finish()
    }
}

/// CRC16 implementation for slot hashing (XMODEM variant, matching Redis Cluster).
///
/// This uses the CRC-16-CCITT polynomial (0x1021) with initial value 0.
#[inline]
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;

    for byte in data {
        crc ^= (*byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }

    crc
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
