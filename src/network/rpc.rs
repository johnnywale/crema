//! RPC message types for network communication.

use crate::types::{CacheCommand, NodeId};
use protobuf::Message as ProtoMessage;
use raft::prelude::Message as RaftMessage;
use serde::{Deserialize, Serialize};

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

/// Encode a message to bytes.
pub fn encode_message(msg: &Message) -> Result<Vec<u8>, bincode::Error> {
    bincode::serialize(msg)
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
