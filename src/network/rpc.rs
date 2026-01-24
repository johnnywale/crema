//! RPC message types for network communication.

use crate::types::{CacheCommand, NodeId};
use protobuf::Message as ProtoMessage;
use raft::prelude::Message as RaftMessage;
use serde::{Deserialize, Serialize};

/// Shard identifier type alias.
pub type ShardId = u32;

/// Network message wrapper for all communication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// Raft protocol message.
    Raft(RaftMessageWrapper),

    /// Client request to the cache.
    ClientRequest(ClientRequest),

    /// Response to a client request.
    ClientResponse(ClientResponse),

    /// Peer discovery/heartbeat.
    Ping(PingRequest),

    /// Response to ping.
    Pong(PongResponse),

    /// Forwarded command from follower to leader.
    ForwardedCommand(ForwardedCommand),

    /// Response to a forwarded command from leader to follower.
    ForwardResponse(ForwardResponse),

    // ==================== Multi-Raft Shard Forwarding ====================

    /// Forwarded command from a node to the shard leader (cross-node, cross-shard).
    /// Used in Multi-Raft mode when a request arrives at a node that doesn't
    /// host the target shard's leader.
    ShardForwardedCommand(ShardForwardedCommand),

    /// Response to a shard forwarded command.
    ShardForwardResponse(ShardForwardResponse),

    // ==================== Migration Messages ====================

    /// Request to fetch a batch of entries from a shard during migration.
    MigrationFetchRequest(MigrationFetchRequest),

    /// Response with a batch of entries for migration.
    MigrationFetchResponse(MigrationFetchResponse),

    /// Request to apply a batch of entries during migration.
    MigrationApplyRequest(MigrationApplyRequest),

    /// Response after applying a migration batch.
    MigrationApplyResponse(MigrationApplyResponse),

    /// Request shard statistics (entry count, size).
    MigrationShardStatsRequest(MigrationShardStatsRequest),

    /// Response with shard statistics.
    MigrationShardStatsResponse(MigrationShardStatsResponse),

    /// Forward a migration proposal to the leader.
    MigrationProposalForward(MigrationProposalForward),

    /// Response to a forwarded migration proposal.
    MigrationProposalForwardResponse(MigrationProposalForwardResponse),
}

/// Wrapper for Raft messages (since RaftMessage uses protobuf).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftMessageWrapper {
    /// Serialized Raft message.
    pub data: Vec<u8>,
}

impl RaftMessageWrapper {
    /// Create a new wrapper from a Raft message.
    pub fn from_raft_message(msg: &RaftMessage) -> Result<Self, protobuf::ProtobufError> {
        let data = msg.write_to_bytes()?;
        Ok(Self { data })
    }

    /// Decode the Raft message.
    pub fn to_raft_message(&self) -> Result<RaftMessage, protobuf::ProtobufError> {
        RaftMessage::parse_from_bytes(&self.data)
    }
}

/// Client request to the cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientRequest {
    /// Unique request ID for correlation.
    pub request_id: u64,

    /// The cache command to execute.
    pub command: CacheCommand,
}

impl ClientRequest {
    /// Create a new client request.
    pub fn new(request_id: u64, command: CacheCommand) -> Self {
        Self {
            request_id,
            command,
        }
    }
}

/// Response to a client request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientResponse {
    /// The request ID this is responding to.
    pub request_id: u64,

    /// Whether the request succeeded.
    pub success: bool,

    /// Error message if failed.
    pub error: Option<String>,

    /// Leader hint if we're not the leader.
    pub leader_hint: Option<NodeId>,
}

impl ClientResponse {
    /// Create a success response.
    pub fn success(request_id: u64) -> Self {
        Self {
            request_id,
            success: true,
            error: None,
            leader_hint: None,
        }
    }

    /// Create an error response.
    pub fn error(request_id: u64, error: String) -> Self {
        Self {
            request_id,
            success: false,
            error: Some(error),
            leader_hint: None,
        }
    }

    /// Create a "not leader" response with leader hint.
    pub fn not_leader(request_id: u64, leader: Option<NodeId>) -> Self {
        Self {
            request_id,
            success: false,
            error: Some("not leader".to_string()),
            leader_hint: leader,
        }
    }
}

/// Ping request for peer discovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingRequest {
    /// Sender's node ID.
    pub node_id: NodeId,

    /// Sender's Raft address.
    pub raft_addr: String,
}

/// Response to ping.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PongResponse {
    /// Responder's node ID.
    pub node_id: NodeId,

    /// Responder's Raft address.
    pub raft_addr: String,

    /// Current leader ID if known.
    pub leader_id: Option<NodeId>,
}

/// Forwarded command from a follower to the leader.
///
/// When a follower receives a write request, it can forward
/// it to the leader instead of rejecting with NotLeader error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardedCommand {
    /// Unique request ID for correlation.
    pub request_id: u64,

    /// Node ID of the follower that received the original request.
    pub origin_node_id: NodeId,

    /// The cache command to execute.
    pub command: CacheCommand,

    /// Time-to-live: remaining forwards allowed.
    /// Prevents infinite forwarding loops.
    /// Starts at 3, decrements on each forward, rejected when 0.
    pub ttl: u8,
}

impl ForwardedCommand {
    /// Create a new forwarded command.
    pub fn new(request_id: u64, origin_node_id: NodeId, command: CacheCommand) -> Self {
        Self {
            request_id,
            origin_node_id,
            command,
            ttl: 3,
        }
    }

    /// Create with specific TTL.
    pub fn with_ttl(request_id: u64, origin_node_id: NodeId, command: CacheCommand, ttl: u8) -> Self {
        Self {
            request_id,
            origin_node_id,
            command,
            ttl,
        }
    }

    /// Decrement TTL and return the new value.
    /// Returns None if TTL is already 0.
    pub fn decrement_ttl(&mut self) -> Option<u8> {
        if self.ttl == 0 {
            None
        } else {
            self.ttl -= 1;
            Some(self.ttl)
        }
    }
}

/// Response to a forwarded command from the leader back to the follower.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardResponse {
    /// The request ID this is responding to.
    pub request_id: u64,

    /// Whether the command was successfully committed.
    pub success: bool,

    /// Error message if failed.
    pub error: Option<String>,

    /// Optional value for GET operations.
    pub value: Option<Vec<u8>>,
}

impl ForwardResponse {
    /// Create a success response for write operations.
    pub fn success(request_id: u64) -> Self {
        Self {
            request_id,
            success: true,
            error: None,
            value: None,
        }
    }

    /// Create a success response for GET operations with a value.
    pub fn success_with_value(request_id: u64, value: Option<Vec<u8>>) -> Self {
        Self {
            request_id,
            success: true,
            error: None,
            value,
        }
    }

    /// Create an error response.
    pub fn error(request_id: u64, error: impl Into<String>) -> Self {
        Self {
            request_id,
            success: false,
            error: Some(error.into()),
            value: None,
        }
    }
}

// ==================== Multi-Raft Shard Forwarding ====================

/// Forwarded command from a node to the shard leader in Multi-Raft mode.
///
/// When a request arrives at node A for a key that belongs to shard S,
/// but shard S's leader is on node B, node A forwards the request to node B.
/// This enables transparent routing in Multi-Raft deployments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardForwardedCommand {
    /// Unique request ID for correlation.
    pub request_id: u64,

    /// Node ID of the node that received the original request.
    pub origin_node_id: NodeId,

    /// The target shard ID for this command.
    pub shard_id: ShardId,

    /// The cache command to execute.
    pub command: CacheCommand,

    /// Time-to-live: remaining forwards allowed.
    /// Prevents infinite forwarding loops in case of stale leader info.
    /// Starts at 3, decrements on each forward, rejected when 0.
    pub ttl: u8,
}

impl ShardForwardedCommand {
    /// Create a new shard forwarded command.
    pub fn new(
        request_id: u64,
        origin_node_id: NodeId,
        shard_id: ShardId,
        command: CacheCommand,
    ) -> Self {
        Self {
            request_id,
            origin_node_id,
            shard_id,
            command,
            ttl: 3,
        }
    }

    /// Create with specific TTL.
    pub fn with_ttl(
        request_id: u64,
        origin_node_id: NodeId,
        shard_id: ShardId,
        command: CacheCommand,
        ttl: u8,
    ) -> Self {
        Self {
            request_id,
            origin_node_id,
            shard_id,
            command,
            ttl,
        }
    }

    /// Decrement TTL and return the new value.
    /// Returns None if TTL is already 0.
    pub fn decrement_ttl(&mut self) -> Option<u8> {
        if self.ttl == 0 {
            None
        } else {
            self.ttl -= 1;
            Some(self.ttl)
        }
    }
}

/// Response to a shard forwarded command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardForwardResponse {
    /// The request ID this is responding to.
    pub request_id: u64,

    /// Whether the command was successfully committed.
    pub success: bool,

    /// Error message if failed.
    pub error: Option<String>,

    /// Optional value for GET operations.
    pub value: Option<Vec<u8>>,

    /// Shard leader hint if we're not the leader.
    /// Contains (shard_id, leader_node_id) for retrying.
    pub leader_hint: Option<(ShardId, NodeId)>,
}

impl ShardForwardResponse {
    /// Create a success response for write operations.
    pub fn success(request_id: u64) -> Self {
        Self {
            request_id,
            success: true,
            error: None,
            value: None,
            leader_hint: None,
        }
    }

    /// Create a success response for GET operations with a value.
    pub fn success_with_value(request_id: u64, value: Option<Vec<u8>>) -> Self {
        Self {
            request_id,
            success: true,
            error: None,
            value,
            leader_hint: None,
        }
    }

    /// Create an error response.
    pub fn error(request_id: u64, error: impl Into<String>) -> Self {
        Self {
            request_id,
            success: false,
            error: Some(error.into()),
            value: None,
            leader_hint: None,
        }
    }

    /// Create a "not shard leader" response with leader hint.
    pub fn not_shard_leader(request_id: u64, shard_id: ShardId, leader: Option<NodeId>) -> Self {
        Self {
            request_id,
            success: false,
            error: Some(format!("Not leader for shard {}", shard_id)),
            value: None,
            leader_hint: leader.map(|l| (shard_id, l)),
        }
    }

    /// Create a "shard not found" response.
    pub fn shard_not_found(request_id: u64, shard_id: ShardId) -> Self {
        Self {
            request_id,
            success: false,
            error: Some(format!("Shard {} not found on this node", shard_id)),
            value: None,
            leader_hint: None,
        }
    }
}

// ==================== Migration Message Types ====================

/// Request to fetch a batch of entries from a shard during migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationFetchRequest {
    /// Unique request ID for correlation.
    pub request_id: u64,
    /// The shard to fetch from.
    pub shard_id: ShardId,
    /// Last key from previous batch (for pagination).
    /// None means start from the beginning.
    pub last_key: Option<Vec<u8>>,
    /// Maximum number of entries to fetch.
    pub batch_size: usize,
}

impl MigrationFetchRequest {
    /// Create a new fetch request.
    pub fn new(request_id: u64, shard_id: ShardId, last_key: Option<Vec<u8>>, batch_size: usize) -> Self {
        Self {
            request_id,
            shard_id,
            last_key,
            batch_size,
        }
    }
}

/// A single entry being transferred during migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationEntry {
    /// Key.
    pub key: Vec<u8>,
    /// Value.
    pub value: Vec<u8>,
    /// Expiration time (unix nanos), if any.
    pub expires_at_nanos: Option<u64>,
}

impl MigrationEntry {
    /// Create a new entry.
    pub fn new(key: Vec<u8>, value: Vec<u8>, expires_at_nanos: Option<u64>) -> Self {
        Self {
            key,
            value,
            expires_at_nanos,
        }
    }

    /// Get the size of this entry in bytes.
    pub fn size(&self) -> usize {
        self.key.len() + self.value.len() + 8 // 8 for expires_at_nanos
    }
}

/// Response with a batch of entries for migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationFetchResponse {
    /// The request ID this is responding to.
    pub request_id: u64,
    /// Whether the request succeeded.
    pub success: bool,
    /// Error message if failed.
    pub error: Option<String>,
    /// The batch of entries.
    pub entries: Vec<MigrationEntry>,
    /// Whether this is the final batch (no more entries).
    pub is_final: bool,
    /// Batch sequence number.
    pub sequence: u64,
}

impl MigrationFetchResponse {
    /// Create a successful response with entries.
    pub fn success(request_id: u64, entries: Vec<MigrationEntry>, is_final: bool, sequence: u64) -> Self {
        Self {
            request_id,
            success: true,
            error: None,
            entries,
            is_final,
            sequence,
        }
    }

    /// Create an error response.
    pub fn error(request_id: u64, error: impl Into<String>) -> Self {
        Self {
            request_id,
            success: false,
            error: Some(error.into()),
            entries: Vec::new(),
            is_final: true,
            sequence: 0,
        }
    }

    /// Get the total size of entries in this response.
    pub fn size(&self) -> usize {
        self.entries.iter().map(|e| e.size()).sum()
    }
}

/// Request to apply a batch of entries during migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationApplyRequest {
    /// Unique request ID for correlation.
    pub request_id: u64,
    /// The shard to apply to.
    pub shard_id: ShardId,
    /// The batch of entries to apply.
    pub entries: Vec<MigrationEntry>,
    /// Batch sequence number for ordering.
    pub sequence: u64,
}

impl MigrationApplyRequest {
    /// Create a new apply request.
    pub fn new(request_id: u64, shard_id: ShardId, entries: Vec<MigrationEntry>, sequence: u64) -> Self {
        Self {
            request_id,
            shard_id,
            entries,
            sequence,
        }
    }
}

/// Response after applying a migration batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationApplyResponse {
    /// The request ID this is responding to.
    pub request_id: u64,
    /// Whether the apply succeeded.
    pub success: bool,
    /// Error message if failed.
    pub error: Option<String>,
    /// Number of entries applied.
    pub entries_applied: u64,
}

impl MigrationApplyResponse {
    /// Create a success response.
    pub fn success(request_id: u64, entries_applied: u64) -> Self {
        Self {
            request_id,
            success: true,
            error: None,
            entries_applied,
        }
    }

    /// Create an error response.
    pub fn error(request_id: u64, error: impl Into<String>) -> Self {
        Self {
            request_id,
            success: false,
            error: Some(error.into()),
            entries_applied: 0,
        }
    }
}

/// Request shard statistics for migration planning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationShardStatsRequest {
    /// Unique request ID for correlation.
    pub request_id: u64,
    /// The shard to get stats for.
    pub shard_id: ShardId,
}

impl MigrationShardStatsRequest {
    /// Create a new stats request.
    pub fn new(request_id: u64, shard_id: ShardId) -> Self {
        Self { request_id, shard_id }
    }
}

/// Response with shard statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationShardStatsResponse {
    /// The request ID this is responding to.
    pub request_id: u64,
    /// Whether the request succeeded.
    pub success: bool,
    /// Error message if failed.
    pub error: Option<String>,
    /// Number of entries in the shard.
    pub entry_count: u64,
    /// Approximate size in bytes.
    pub size_bytes: u64,
}

impl MigrationShardStatsResponse {
    /// Create a success response.
    pub fn success(request_id: u64, entry_count: u64, size_bytes: u64) -> Self {
        Self {
            request_id,
            success: true,
            error: None,
            entry_count,
            size_bytes,
        }
    }

    /// Create an error response.
    pub fn error(request_id: u64, error: impl Into<String>) -> Self {
        Self {
            request_id,
            success: false,
            error: Some(error.into()),
            entry_count: 0,
            size_bytes: 0,
        }
    }
}

// ==================== Migration Proposal Forwarding ====================

/// Forward a migration proposal to the leader node.
///
/// When a follower receives a migration command and is not the leader,
/// it can forward the proposal to the current leader instead of failing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationProposalForward {
    /// Unique request ID for correlation.
    pub request_id: u64,
    /// The originating node ID.
    pub origin_node: NodeId,
    /// Serialized migration command bytes.
    pub command_bytes: Vec<u8>,
    /// TTL to prevent infinite forwarding loops.
    pub ttl: u8,
}

impl MigrationProposalForward {
    /// Create a new proposal forward request.
    pub fn new(request_id: u64, origin_node: NodeId, command_bytes: Vec<u8>) -> Self {
        Self {
            request_id,
            origin_node,
            command_bytes,
            ttl: 3, // Max 3 hops
        }
    }

    /// Create with specific TTL.
    pub fn with_ttl(request_id: u64, origin_node: NodeId, command_bytes: Vec<u8>, ttl: u8) -> Self {
        Self {
            request_id,
            origin_node,
            command_bytes,
            ttl,
        }
    }

    /// Decrement TTL for forwarding. Returns None if TTL exhausted.
    pub fn decrement_ttl(&mut self) -> Option<u8> {
        if self.ttl == 0 {
            None
        } else {
            self.ttl -= 1;
            Some(self.ttl)
        }
    }
}

/// Response to a forwarded migration proposal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationProposalForwardResponse {
    /// The request ID this is responding to.
    pub request_id: u64,
    /// Whether the proposal was successfully committed.
    pub success: bool,
    /// Error message if failed.
    pub error: Option<String>,
    /// Leader hint if we're not the leader (for further forwarding).
    pub leader_hint: Option<NodeId>,
}

impl MigrationProposalForwardResponse {
    /// Create a success response.
    pub fn success(request_id: u64) -> Self {
        Self {
            request_id,
            success: true,
            error: None,
            leader_hint: None,
        }
    }

    /// Create an error response.
    pub fn error(request_id: u64, error: impl Into<String>) -> Self {
        Self {
            request_id,
            success: false,
            error: Some(error.into()),
            leader_hint: None,
        }
    }

    /// Create a "not leader" response with leader hint.
    pub fn not_leader(request_id: u64, leader: Option<NodeId>) -> Self {
        Self {
            request_id,
            success: false,
            error: Some("Not leader".to_string()),
            leader_hint: leader,
        }
    }
}

/// Encode a message to bytes.
pub fn encode_message(msg: &Message) -> Result<Vec<u8>, bincode::Error> {
    bincode::serialize(msg)
}

/// Zero-copy encoding: encode message directly into a BytesMut buffer.
/// Returns the number of bytes written.
///
/// This is more efficient than encode_message when you already have a BytesMut
/// buffer to write into, as it avoids intermediate allocations.
pub fn encode_message_into(msg: &Message, buffer: &mut bytes::BytesMut) -> Result<usize, bincode::Error> {
    // First, calculate the serialized size
    let size = bincode::serialized_size(msg)? as usize;

    // Reserve space for length prefix + message
    buffer.reserve(4 + size);

    // Write length prefix
    buffer.extend_from_slice(&(size as u32).to_be_bytes());

    // Get the current length and extend buffer with zeros
    let start = buffer.len();
    buffer.resize(start + size, 0);

    // Serialize directly into the buffer
    let mut cursor = std::io::Cursor::new(&mut buffer[start..]);
    bincode::serialize_into(&mut cursor, msg)?;

    Ok(4 + size)
}

/// Decode a message from bytes.
pub fn decode_message(data: &[u8]) -> Result<Message, bincode::Error> {
    bincode::deserialize(data)
}

/// Frame a message with length prefix for TCP transmission.
pub fn frame_message(msg: &Message) -> Result<Vec<u8>, bincode::Error> {
    let data = encode_message(msg)?;
    let len = data.len() as u32;

    let mut framed = Vec::with_capacity(4 + data.len());
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(&data);

    Ok(framed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_request_serialization() {
        let req = ClientRequest::new(42, CacheCommand::put(b"key".to_vec(), b"value".to_vec()));

        let msg = Message::ClientRequest(req);
        let encoded = encode_message(&msg).unwrap();
        let decoded = decode_message(&encoded).unwrap();

        if let Message::ClientRequest(decoded_req) = decoded {
            assert_eq!(decoded_req.request_id, 42);
        } else {
            panic!("Wrong message type");
        }
    }

    #[test]
    fn test_client_response() {
        let resp = ClientResponse::success(42);
        assert!(resp.success);
        assert!(resp.error.is_none());

        let resp = ClientResponse::error(42, "test error".to_string());
        assert!(!resp.success);
        assert_eq!(resp.error, Some("test error".to_string()));

        let resp = ClientResponse::not_leader(42, Some(5));
        assert!(!resp.success);
        assert_eq!(resp.leader_hint, Some(5));
    }

    #[test]
    fn test_frame_message() {
        let msg = Message::Ping(PingRequest {
            node_id: 1,
            raft_addr: "127.0.0.1:9000".to_string(),
        });

        let framed = frame_message(&msg).unwrap();

        // First 4 bytes should be length
        let len = u32::from_be_bytes([framed[0], framed[1], framed[2], framed[3]]) as usize;
        assert_eq!(len, framed.len() - 4);

        // Rest should be the message
        let decoded = decode_message(&framed[4..]).unwrap();
        if let Message::Ping(ping) = decoded {
            assert_eq!(ping.node_id, 1);
        } else {
            panic!("Wrong message type");
        }
    }
}
