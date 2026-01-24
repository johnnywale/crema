//! Internal routing abstraction for cache operations.
//!
//! This module provides a `CacheRouter` enum that abstracts over single-Raft
//! and Multi-Raft modes, allowing the `DistributedCache` to use a unified
//! interface regardless of the underlying routing strategy.

use crate::consensus::RaftNode;
use crate::error::Result;
use crate::multiraft::MultiRaftCoordinator;
use crate::types::CacheCommand;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use super::storage::CacheStorage;

/// Internal routing abstraction for cache operations.
///
/// This enum allows `DistributedCache` to handle both single-Raft and Multi-Raft
/// modes without Option checks scattered throughout the code.
#[derive(Debug)]
pub enum CacheRouter {
    /// Single Raft group mode (traditional mode).
    ///
    /// All operations go through a single Raft group, with local reads
    /// served from the cache storage.
    Single {
        /// Local cache storage for reads.
        storage: Arc<CacheStorage>,
        /// Raft node for consensus on writes.
        raft: Arc<RaftNode>,
    },

    /// Multi-Raft mode with sharded routing.
    ///
    /// Operations are routed to the appropriate shard based on key hash.
    /// Phase 1: Gossip-based leader routing (eventual consistency).
    Multi {
        /// Multi-Raft coordinator managing all shards.
        coordinator: Arc<MultiRaftCoordinator>,
    },
}

impl CacheRouter {
    /// Create a new single-mode router.
    pub fn single(storage: Arc<CacheStorage>, raft: Arc<RaftNode>) -> Self {
        CacheRouter::Single { storage, raft }
    }

    /// Create a new multi-mode router.
    pub fn multi(coordinator: Arc<MultiRaftCoordinator>) -> Self {
        CacheRouter::Multi { coordinator }
    }

    /// Check if this router is in Multi-Raft mode.
    pub fn is_multi_raft(&self) -> bool {
        matches!(self, CacheRouter::Multi { .. })
    }

    /// Get a value from the cache.
    ///
    /// In single mode, reads directly from local storage.
    /// In multi mode, routes to the appropriate shard.
    pub async fn get(&self, key: &[u8]) -> Option<Bytes> {
        match self {
            CacheRouter::Single { storage, raft } => {
                // Read-Index: Wait for state machine to apply up to commit_index
                let commit_index = raft.commit_index();
                let start = std::time::Instant::now();
                let max_wait = Duration::from_secs(1);

                while raft.applied_index() < commit_index {
                    if start.elapsed() > max_wait {
                        tracing::warn!(
                            "Read-Index wait timeout: applied={} commit={}",
                            raft.applied_index(),
                            commit_index
                        );
                        break;
                    }
                    tokio::task::yield_now().await;
                }

                storage.get(key).await
            }
            CacheRouter::Multi { coordinator } => {
                // Route to appropriate shard
                match coordinator.get(key).await {
                    Ok(value) => value,
                    Err(e) => {
                        tracing::warn!(error = %e, "Multi-Raft get failed");
                        None
                    }
                }
            }
        }
    }

    /// Put a key-value pair into the cache.
    ///
    /// In single mode, proposes through Raft.
    /// In multi mode, routes to the appropriate shard's coordinator.
    pub async fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        match self {
            CacheRouter::Single { raft, .. } => {
                let command = CacheCommand::put(key.to_vec(), value.to_vec());
                raft.propose(command).await?;
                Ok(())
            }
            CacheRouter::Multi { coordinator } => {
                coordinator.put(key, value).await
            }
        }
    }

    /// Put a key-value pair with a custom TTL.
    pub async fn put_with_ttl(&self, key: Bytes, value: Bytes, ttl: Duration) -> Result<()> {
        match self {
            CacheRouter::Single { raft, .. } => {
                let command = CacheCommand::put_with_ttl(key.to_vec(), value.to_vec(), ttl);
                raft.propose(command).await?;
                Ok(())
            }
            CacheRouter::Multi { coordinator } => {
                coordinator.put_with_ttl(key, value, ttl).await
            }
        }
    }

    /// Delete a key from the cache.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        match self {
            CacheRouter::Single { raft, .. } => {
                let command = CacheCommand::delete(key.to_vec());
                raft.propose(command).await?;
                Ok(())
            }
            CacheRouter::Multi { coordinator } => {
                coordinator.delete(key).await
            }
        }
    }

    /// Clear all entries from the cache.
    pub async fn clear(&self) -> Result<()> {
        match self {
            CacheRouter::Single { raft, .. } => {
                let command = CacheCommand::clear();
                raft.propose(command).await?;
                Ok(())
            }
            CacheRouter::Multi { coordinator } => {
                // Clear is not directly supported in Multi-Raft Phase 1
                // We'd need to clear each shard individually
                tracing::warn!("Clear operation not fully supported in Multi-Raft Phase 1");
                // For now, just try to clear all shards
                for shard in coordinator.router().all_shards() {
                    shard.storage().invalidate_all();
                }
                Ok(())
            }
        }
    }

    /// Check if the current node is the leader (single mode) or has any leader shards (multi mode).
    pub fn is_leader(&self) -> bool {
        match self {
            CacheRouter::Single { raft, .. } => raft.is_leader(),
            CacheRouter::Multi { coordinator } => {
                // In multi mode, check if this node leads any shard
                coordinator.stats().local_leader_shards > 0
            }
        }
    }

    /// Get the Raft node (only available in single mode).
    pub fn raft(&self) -> Option<&Arc<RaftNode>> {
        match self {
            CacheRouter::Single { raft, .. } => Some(raft),
            CacheRouter::Multi { .. } => None,
        }
    }

    /// Get the storage (only available in single mode).
    pub fn storage(&self) -> Option<&Arc<CacheStorage>> {
        match self {
            CacheRouter::Single { storage, .. } => Some(storage),
            CacheRouter::Multi { .. } => None,
        }
    }

    /// Get the Multi-Raft coordinator (only available in multi mode).
    pub fn coordinator(&self) -> Option<&Arc<MultiRaftCoordinator>> {
        match self {
            CacheRouter::Single { .. } => None,
            CacheRouter::Multi { coordinator } => Some(coordinator),
        }
    }

    /// Get the total entry count.
    pub fn entry_count(&self) -> u64 {
        match self {
            CacheRouter::Single { storage, .. } => storage.entry_count(),
            CacheRouter::Multi { coordinator } => coordinator.stats().total_entries,
        }
    }

    /// Check if a key exists in the cache.
    pub fn contains(&self, key: &[u8]) -> bool {
        match self {
            CacheRouter::Single { storage, .. } => storage.contains(key),
            CacheRouter::Multi { coordinator } => {
                // Route to appropriate shard
                let shard_id = coordinator.shard_for_key(key);
                if let Some(shard) = coordinator.get_shard(shard_id) {
                    shard.storage().contains(key)
                } else {
                    false
                }
            }
        }
    }

    /// Run pending maintenance tasks.
    pub async fn run_pending_tasks(&self) {
        match self {
            CacheRouter::Single { storage, .. } => {
                storage.run_pending_tasks().await;
            }
            CacheRouter::Multi { coordinator } => {
                // Run pending tasks on all shards
                for shard in coordinator.router().all_shards() {
                    shard.storage().run_pending_tasks().await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CacheConfig;
    use crate::consensus::CacheStateMachine;

    fn test_config() -> CacheConfig {
        CacheConfig {
            max_capacity: 1000,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_single_mode_router() {
        let config = test_config();
        let storage = Arc::new(CacheStorage::new(&config));
        let state_machine = Arc::new(CacheStateMachine::new(storage.clone()));
        let raft = RaftNode::new(
            1,
            vec![1],
            config.raft.clone(),
            state_machine,
            config.raft_addr.to_string(),
        ).unwrap();

        let router = CacheRouter::single(storage.clone(), raft);

        assert!(!router.is_multi_raft());
        assert!(router.storage().is_some());
        assert!(router.raft().is_some());
        assert!(router.coordinator().is_none());
    }

    #[tokio::test]
    async fn test_router_entry_count() {
        let config = test_config();
        let storage = Arc::new(CacheStorage::new(&config));
        let state_machine = Arc::new(CacheStateMachine::new(storage.clone()));
        let raft = RaftNode::new(
            1,
            vec![1],
            config.raft.clone(),
            state_machine,
            config.raft_addr.to_string(),
        ).unwrap();

        let router = CacheRouter::single(storage.clone(), raft);

        assert_eq!(router.entry_count(), 0);
    }
}
