//! Error types for the distributed cache.

use std::io;
use thiserror::Error;

/// Result type alias for distributed cache operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for the distributed cache.
#[derive(Error, Debug)]
pub enum Error {
    /// Raft consensus errors.
    #[error("raft error: {0}")]
    Raft(#[from] RaftError),

    /// Network communication errors.
    #[error("network error: {0}")]
    Network(#[from] NetworkError),

    /// Storage errors.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    /// Cluster membership errors.
    #[error("membership error: {0}")]
    Membership(#[from] MembershipError),

    /// Configuration errors.
    #[error("config error: {0}")]
    Config(String),

    /// The operation was cancelled.
    #[error("operation cancelled")]
    Cancelled,

    /// The operation timed out.
    #[error("operation timed out")]
    Timeout,

    /// Generic internal error.
    #[error("internal error: {0}")]
    Internal(String),

    /// Shard not found.
    #[error("shard not found: {0}")]
    ShardNotFound(u32),

    /// Shard already exists.
    #[error("shard already exists: {0}")]
    ShardAlreadyExists(u32),

    /// Shard is not active.
    #[error("shard not active: {0}")]
    ShardNotActive(u32),

    /// Shard exists but is hosted on a different node (needs forwarding).
    #[error("shard not local: shard {shard_id} is hosted on node {target_node:?}")]
    ShardNotLocal {
        shard_id: u32,
        target_node: Option<u64>,
    },

    /// Shard leader is not yet known (gossip hasn't propagated leader info).
    #[error("shard leader unknown: shard {0}, waiting for gossip")]
    ShardLeaderUnknown(u32),

    /// Server is busy, too many pending requests (backpressure).
    #[error("server busy: too many pending requests ({pending})")]
    ServerBusy { pending: usize },

    /// Error from a remote node during forwarding.
    #[error("remote error: {0}")]
    RemoteError(String),

    /// Request forwarding failed.
    #[error("forward failed: {0}")]
    ForwardFailed(String),

    /// TTL expired during forwarding (too many hops).
    #[error("forward TTL expired")]
    ForwardTtlExpired,

    /// Migrations are paused.
    #[error("migrations paused")]
    MigrationPaused,

    /// Too many concurrent migrations.
    #[error("too many concurrent migrations")]
    TooManyMigrations,

    /// Shard is already being migrated.
    #[error("shard already migrating: {0}")]
    ShardAlreadyMigrating(u32),

    /// Migration not found.
    #[error("migration not found for shard: {0}")]
    MigrationNotFound(u32),

    /// Invalid migration phase.
    #[error("invalid migration phase: {0}")]
    InvalidMigrationPhase(String),

    /// Migration failed.
    #[error("migration failed: {0}")]
    MigrationFailed(String),

    /// Migration timed out.
    #[error("migration timed out")]
    MigrationTimeout,
}

/// Raft consensus related errors.
#[derive(Error, Debug)]
pub enum RaftError {
    /// Not the leader, includes leader hint if known.
    #[error("not leader, leader is: {leader:?}")]
    NotLeader { leader: Option<u64> },

    /// Proposal was dropped (e.g., due to leader change).
    #[error("proposal dropped")]
    ProposalDropped,

    /// Failed to apply entry to state machine.
    #[error("failed to apply: {0}")]
    ApplyFailed(String),

    /// Raft is not ready to process requests.
    #[error("raft not ready")]
    NotReady,

    /// Configuration change in progress.
    #[error("config change in progress")]
    ConfigChangeInProgress,

    /// Internal raft error.
    #[error("raft internal: {0}")]
    Internal(String),
}

/// Network communication errors.
#[derive(Error, Debug)]
pub enum NetworkError {
    /// Connection failed.
    #[error("connection failed to {addr}: {reason}")]
    ConnectionFailed { addr: String, reason: String },

    /// Connection was closed.
    #[error("connection closed")]
    ConnectionClosed,

    /// Failed to send message.
    #[error("send failed: {0}")]
    SendFailed(String),

    /// Failed to receive message.
    #[error("receive failed: {0}")]
    ReceiveFailed(String),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Deserialization error.
    #[error("deserialization error: {0}")]
    Deserialization(String),

    /// I/O error.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// Address parse error.
    #[error("invalid address: {0}")]
    InvalidAddress(String),
}

/// Storage layer errors.
#[derive(Error, Debug)]
pub enum StorageError {
    /// Entry not found.
    #[error("entry not found: index {0}")]
    EntryNotFound(u64),

    /// Snapshot not found.
    #[error("snapshot not found")]
    SnapshotNotFound,

    /// Non-contiguous entries within a single append operation.
    ///
    /// This indicates that the entries slice itself contains a gap.
    /// The prev_index is followed by curr_index, but they should be consecutive.
    #[error("non-contiguous entries in append: index {prev_index} followed by {curr_index}")]
    NonContiguous {
        prev_index: u64,
        curr_index: u64,
    },

    /// Log gap detected - entries are not contiguous with existing log.
    ///
    /// This indicates a critical bug in the upper layer or data corruption.
    /// The last_index is the highest index currently in the log,
    /// and first_new is the index of the first entry being appended.
    #[error("log gap detected: last_index={last_index}, first_new={first_new}, expected contiguous append")]
    LogGap {
        last_index: u64,
        first_new: u64,
    },
    /// Log compacted, entry no longer available.
    #[error("log compacted at index {0}")]
    Compacted(u64),

    /// Snapshot is temporarily unavailable.
    #[error("snapshot temporarily unavailable")]
    SnapshotTemporarilyUnavailable,

    /// I/O error.
    #[error("storage io error: {0}")]
    Io(String),

    /// RocksDB error.
    #[error("rocksdb error: {0}")]
    RocksDb(String),
}

/// Cluster membership errors.
#[derive(Error, Debug)]
pub enum MembershipError {
    /// Node not found in cluster.
    #[error("node not found: {0}")]
    NodeNotFound(u64),

    /// Node already exists.
    #[error("node already exists: {0}")]
    NodeAlreadyExists(u64),

    /// Cannot remove node, would lose quorum.
    #[error("would lose quorum: current peers {current}, removing would leave {remaining}")]
    WouldLoseQuorum { current: usize, remaining: usize },

    /// Node is not discovered yet.
    #[error("node not discovered: {0}")]
    NodeNotDiscovered(u64),

    /// Node is still alive, cannot remove.
    #[error("node still alive: {0}")]
    NodeStillAlive(u64),

    /// Too many peers.
    #[error("too many peers: max {max}, current {current}")]
    TooManyPeers { max: usize, current: usize },

    /// Join failed.
    #[error("join failed: {0}")]
    JoinFailed(String),
}

impl Error {
    /// Returns true if this error is transient and the operation can be retried.
    ///
    /// Transient errors are temporary conditions that may succeed if retried,
    /// such as network timeouts, temporary unavailability, or backpressure.
    ///
    /// Permanent errors indicate configuration issues, invalid state, or
    /// conditions that won't change without intervention.
    pub fn is_retryable(&self) -> bool {
        match self {
            // Top-level transient errors
            Error::Timeout => true,
            Error::ServerBusy { .. } => true,
            Error::MigrationPaused => true, // May resume later
            Error::TooManyMigrations => true, // May have capacity later

            // Delegate to nested error types
            Error::Raft(e) => e.is_retryable(),
            Error::Network(e) => e.is_retryable(),
            Error::Storage(e) => e.is_retryable(),
            Error::Membership(e) => e.is_retryable(),

            // Shard routing errors - retryable with forwarding or gossip wait
            Error::ShardNotLocal { .. } => true, // Forward to correct node
            Error::ShardLeaderUnknown(_) => true, // Wait for gossip

            // Permanent errors - won't change without intervention
            Error::Config(_) => false,
            Error::Cancelled => false,
            Error::Internal(_) => false,
            Error::ShardNotFound(_) => false,
            Error::ShardAlreadyExists(_) => false,
            Error::ShardNotActive(_) => false,
            Error::RemoteError(_) => false,
            Error::ForwardFailed(_) => true, // Network issue, might succeed
            Error::ForwardTtlExpired => false, // Configuration/routing issue
            Error::ShardAlreadyMigrating(_) => false, // Must wait for current migration
            Error::MigrationNotFound(_) => false,
            Error::InvalidMigrationPhase(_) => false,
            Error::MigrationFailed(_) => false, // Terminal failure
            Error::MigrationTimeout => true, // Might succeed with retry
        }
    }

    /// Returns true if this error is permanent and should not be retried.
    pub fn is_permanent(&self) -> bool {
        !self.is_retryable()
    }

    /// Returns a suggested retry delay for transient errors.
    ///
    /// Returns `None` for permanent errors that should not be retried.
    pub fn retry_delay(&self) -> Option<std::time::Duration> {
        use std::time::Duration;

        if !self.is_retryable() {
            return None;
        }

        match self {
            Error::Timeout => Some(Duration::from_millis(100)),
            Error::ServerBusy { pending } => {
                // Exponential backoff based on queue depth
                let base_ms = 10u64.saturating_mul(*pending as u64).min(1000);
                Some(Duration::from_millis(base_ms))
            }
            Error::MigrationPaused => Some(Duration::from_secs(1)),
            Error::TooManyMigrations => Some(Duration::from_millis(500)),
            Error::MigrationTimeout => Some(Duration::from_millis(200)),
            Error::ForwardFailed(_) => Some(Duration::from_millis(50)),
            Error::ShardNotLocal { .. } => Some(Duration::from_millis(10)), // Fast retry with forwarding
            Error::ShardLeaderUnknown(_) => Some(Duration::from_millis(100)), // Wait for gossip
            Error::Raft(e) => e.retry_delay(),
            Error::Network(e) => e.retry_delay(),
            Error::Storage(e) => e.retry_delay(),
            Error::Membership(e) => e.retry_delay(),
            _ => Some(Duration::from_millis(100)),
        }
    }
}

impl RaftError {
    /// Returns true if this Raft error is transient and can be retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            RaftError::NotLeader { .. } => true,
            RaftError::ProposalDropped => true,
            RaftError::NotReady => true,
            RaftError::ConfigChangeInProgress => true,
            RaftError::ApplyFailed(_) => false,
            RaftError::Internal(_) => false,
        }
    }

    /// Returns suggested retry delay for transient errors.
    pub fn retry_delay(&self) -> Option<std::time::Duration> {
        use std::time::Duration;

        match self {
            RaftError::NotLeader { .. } => Some(Duration::from_millis(50)),
            RaftError::ProposalDropped => Some(Duration::from_millis(10)),
            RaftError::NotReady => Some(Duration::from_millis(100)),
            RaftError::ConfigChangeInProgress => Some(Duration::from_millis(200)),
            _ => None,
        }
    }
}

impl NetworkError {
    /// Returns true if this network error is transient and can be retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            NetworkError::ConnectionFailed { .. } => true,
            NetworkError::ConnectionClosed => true,
            NetworkError::SendFailed(_) => true,
            NetworkError::ReceiveFailed(_) => true,
            NetworkError::Io(_) => true,
            NetworkError::Serialization(_) => false,
            NetworkError::Deserialization(_) => false,
            NetworkError::InvalidAddress(_) => false,
        }
    }

    /// Returns suggested retry delay for transient errors.
    pub fn retry_delay(&self) -> Option<std::time::Duration> {
        use std::time::Duration;

        match self {
            NetworkError::ConnectionFailed { .. } => Some(Duration::from_millis(100)),
            NetworkError::ConnectionClosed => Some(Duration::from_millis(50)),
            NetworkError::SendFailed(_) => Some(Duration::from_millis(20)),
            NetworkError::ReceiveFailed(_) => Some(Duration::from_millis(20)),
            NetworkError::Io(_) => Some(Duration::from_millis(50)),
            _ => None,
        }
    }
}

impl StorageError {
    /// Returns true if this storage error is transient and can be retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            StorageError::SnapshotTemporarilyUnavailable => true,
            StorageError::Io(_) => true,
            StorageError::RocksDb(_) => true, // RocksDB errors may be transient
            StorageError::EntryNotFound(_) => false,
            StorageError::SnapshotNotFound => false,
            StorageError::NonContiguous { .. } => false,
            StorageError::LogGap { .. } => false,
            StorageError::Compacted(_) => false,
        }
    }

    /// Returns suggested retry delay for transient errors.
    pub fn retry_delay(&self) -> Option<std::time::Duration> {
        use std::time::Duration;

        match self {
            StorageError::SnapshotTemporarilyUnavailable => Some(Duration::from_millis(500)),
            StorageError::Io(_) => Some(Duration::from_millis(100)),
            StorageError::RocksDb(_) => Some(Duration::from_millis(100)),
            _ => None,
        }
    }
}

impl MembershipError {
    /// Returns true if this membership error is transient and can be retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            MembershipError::NodeNotDiscovered(_) => true,
            MembershipError::JoinFailed(_) => true,
            MembershipError::NodeStillAlive(_) => true,
            MembershipError::NodeNotFound(_) => false,
            MembershipError::NodeAlreadyExists(_) => false,
            MembershipError::WouldLoseQuorum { .. } => false,
            MembershipError::TooManyPeers { .. } => false,
        }
    }

    /// Returns suggested retry delay for transient errors.
    pub fn retry_delay(&self) -> Option<std::time::Duration> {
        use std::time::Duration;

        match self {
            MembershipError::NodeNotDiscovered(_) => Some(Duration::from_secs(1)),
            MembershipError::JoinFailed(_) => Some(Duration::from_millis(500)),
            MembershipError::NodeStillAlive(_) => Some(Duration::from_secs(2)),
            _ => None,
        }
    }
}

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Error::Network(NetworkError::Serialization(e.to_string()))
    }
}

impl From<raft::Error> for Error {
    fn from(e: raft::Error) -> Self {
        Error::Raft(RaftError::Internal(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_is_retryable() {
        assert!(Error::Timeout.is_retryable());
        assert!(Error::ServerBusy { pending: 10 }.is_retryable());
        assert!(Error::MigrationPaused.is_retryable());
        assert!(Error::TooManyMigrations.is_retryable());
        assert!(Error::MigrationTimeout.is_retryable());

        // New shard routing errors are retryable
        assert!(Error::ShardNotLocal { shard_id: 1, target_node: Some(2) }.is_retryable());
        assert!(Error::ShardNotLocal { shard_id: 1, target_node: None }.is_retryable());
        assert!(Error::ShardLeaderUnknown(1).is_retryable());

        assert!(!Error::Config("bad config".to_string()).is_retryable());
        assert!(!Error::Cancelled.is_retryable());
        assert!(!Error::ShardNotFound(1).is_retryable());
        assert!(!Error::MigrationFailed("failed".to_string()).is_retryable());
    }

    #[test]
    fn test_raft_error_is_retryable() {
        assert!(RaftError::NotLeader { leader: Some(1) }.is_retryable());
        assert!(RaftError::ProposalDropped.is_retryable());
        assert!(RaftError::NotReady.is_retryable());
        assert!(RaftError::ConfigChangeInProgress.is_retryable());
        assert!(!RaftError::ApplyFailed("failed".to_string()).is_retryable());
    }

    #[test]
    fn test_network_error_is_retryable() {
        assert!(NetworkError::ConnectionFailed {
            addr: "127.0.0.1:8080".to_string(),
            reason: "refused".to_string()
        }
        .is_retryable());
        assert!(NetworkError::ConnectionClosed.is_retryable());
        assert!(!NetworkError::Serialization("bad data".to_string()).is_retryable());
        assert!(!NetworkError::InvalidAddress("bad".to_string()).is_retryable());
    }

    #[test]
    fn test_storage_error_is_retryable() {
        assert!(StorageError::SnapshotTemporarilyUnavailable.is_retryable());
        assert!(StorageError::Io("disk error".to_string()).is_retryable());
        assert!(!StorageError::EntryNotFound(1).is_retryable());
        assert!(!StorageError::Compacted(5).is_retryable());
    }

    #[test]
    fn test_membership_error_is_retryable() {
        assert!(MembershipError::NodeNotDiscovered(1).is_retryable());
        assert!(MembershipError::JoinFailed("timeout".to_string()).is_retryable());
        assert!(!MembershipError::NodeNotFound(1).is_retryable());
        assert!(!MembershipError::WouldLoseQuorum {
            current: 3,
            remaining: 1
        }
        .is_retryable());
    }

    #[test]
    fn test_retry_delay() {
        assert!(Error::Timeout.retry_delay().is_some());
        assert!(Error::ServerBusy { pending: 10 }.retry_delay().is_some());
        assert!(Error::Cancelled.retry_delay().is_none());
        assert!(Error::ShardNotFound(1).retry_delay().is_none());

        let delay_10 = Error::ServerBusy { pending: 10 }.retry_delay().unwrap();
        let delay_100 = Error::ServerBusy { pending: 100 }.retry_delay().unwrap();
        assert!(delay_100 > delay_10);
    }

    #[test]
    fn test_is_permanent_inverse_of_retryable() {
        let errors = vec![
            Error::Timeout,
            Error::Cancelled,
            Error::ServerBusy { pending: 5 },
            Error::ShardNotFound(1),
        ];

        for err in errors {
            assert_eq!(err.is_permanent(), !err.is_retryable());
        }
    }
}
