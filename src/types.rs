//! Core types used throughout the distributed cache.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Node identifier in the cluster.
pub type NodeId = u64;

/// Cache commands that can be proposed to Raft.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CacheCommand {
    /// Insert or update a key-value pair.
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        /// Optional absolute expiration time in milliseconds since Unix epoch.
        /// Using absolute time ensures TTL is preserved correctly across crash recovery.
        expires_at_ms: Option<u64>,
    },

    /// Delete a key from the cache.
    Delete { key: Vec<u8> },

    /// Clear all entries from the cache.
    Clear,

    /// Get a key from the cache (used for cross-shard forwarding only).
    /// Note: GET operations don't go through Raft; this variant is only
    /// used when forwarding GET requests across shards.
    Get { key: Vec<u8> },
}

impl CacheCommand {
    /// Create a Put command.
    pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self::Put {
            key: key.into(),
            value: value.into(),
            expires_at_ms: None,
        }
    }

    /// Create a Put command with TTL.
    ///
    /// The TTL is converted to an absolute expiration time to ensure
    /// correct behavior across crash recovery and Raft log replay.
    pub fn put_with_ttl(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        ttl: Duration,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let expires_at = now + ttl.as_millis() as u64;
        Self::Put {
            key: key.into(),
            value: value.into(),
            expires_at_ms: Some(expires_at),
        }
    }

    /// Create a Delete command.
    pub fn delete(key: impl Into<Vec<u8>>) -> Self {
        Self::Delete { key: key.into() }
    }

    /// Create a Clear command.
    pub fn clear() -> Self {
        Self::Clear
    }

    /// Create a Get command (for cross-shard forwarding).
    pub fn get(key: impl Into<Vec<u8>>) -> Self {
        Self::Get { key: key.into() }
    }

    /// Serialize command to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    /// Deserialize command from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(data)
    }
}

/// Information about a peer node in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Unique node identifier.
    pub id: NodeId,
    /// Address for Raft communication.
    pub raft_addr: SocketAddr,
    /// Address for client communication (optional).
    pub client_addr: Option<SocketAddr>,
    /// Whether this node is part of the Raft quorum.
    pub is_voter: bool,
}

impl PeerInfo {
    /// Create a new PeerInfo.
    pub fn new(id: NodeId, raft_addr: SocketAddr) -> Self {
        Self {
            id,
            raft_addr,
            client_addr: None,
            is_voter: true,
        }
    }
}

/// Current status of the cluster.
#[derive(Debug, Clone)]
pub struct ClusterStatus {
    /// This node's ID.
    pub node_id: NodeId,
    /// Current leader ID, if known.
    pub leader_id: Option<NodeId>,
    /// Whether this node is the leader.
    pub is_leader: bool,
    /// Current Raft term.
    pub term: u64,
    /// Number of peers in the Raft cluster.
    pub raft_peer_count: usize,
    /// Number of discovered nodes (may include non-voting members).
    pub discovered_node_count: usize,
    /// Number of nodes discovered via memberlist gossip.
    pub memberlist_node_count: usize,
    /// Committed index.
    pub commit_index: u64,
    /// Applied index.
    pub applied_index: u64,
}

/// Cache statistics.
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Number of entries in local cache.
    pub entry_count: u64,
    /// Approximate weighted size in bytes.
    pub weighted_size: u64,
    /// Number of cache hits.
    pub hits: u64,
    /// Number of cache misses.
    pub misses: u64,
}

/// Proposal result returned when a command is committed.
#[derive(Debug)]
pub struct ProposalResult {
    /// The index at which the command was committed.
    pub index: u64,
    /// The term when the command was committed.
    pub term: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_command_serialization() {
        let cmd = CacheCommand::put(b"key".to_vec(), b"value".to_vec());
        let bytes = cmd.to_bytes().unwrap();
        let decoded = CacheCommand::from_bytes(&bytes).unwrap();
        assert_eq!(cmd, decoded);
    }

    #[test]
    fn test_cache_command_with_ttl() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let cmd = CacheCommand::put_with_ttl(
            b"key".to_vec(),
            b"value".to_vec(),
            Duration::from_secs(60),
        );
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        if let CacheCommand::Put { expires_at_ms, .. } = cmd {
            // expires_at_ms should be between now+60s before and now+60s after
            let expected_min = before + 60_000;
            let expected_max = after + 60_000 + 1; // +1 for rounding
            assert!(
                expires_at_ms.is_some(),
                "Expected expires_at_ms to be set"
            );
            let expires = expires_at_ms.unwrap();
            assert!(
                expires >= expected_min && expires <= expected_max,
                "expires_at_ms {} not in expected range [{}, {}]",
                expires,
                expected_min,
                expected_max
            );
        } else {
            panic!("Expected Put command");
        }
    }
}
