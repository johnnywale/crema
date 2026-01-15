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
