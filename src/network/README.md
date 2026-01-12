# Network Module

The network module handles all network communication for the distributed cache.

## Components

### `NetworkServer` (server.rs)

TCP server that accepts connections and handles incoming messages.

**Features:**
- Async TCP listener using Tokio
- Length-prefixed message framing
- Pluggable message handler interface
- Graceful shutdown support

**Usage:**
```rust
let handler = Arc::new(MyMessageHandler);
let (server, shutdown_tx) = NetworkServer::new(
    "127.0.0.1:9000".parse()?,
    node_id,
    handler,
);

// Run server (usually spawned as a task)
tokio::spawn(server.run());

// Shutdown when done
shutdown_tx.send(()).await;
```

### `MessageHandler` Trait

Interface for handling incoming messages:

```rust
pub trait MessageHandler: Send + Sync + 'static {
    fn handle(&self, msg: Message) -> Option<Message>;
}
```

Return `Some(response)` to send a response, or `None` for no response.

### Message Types (rpc.rs)

All network messages are wrapped in the `Message` enum:

```rust
pub enum Message {
    Raft(RaftMessageWrapper),      // Raft protocol messages
    ClientRequest(ClientRequest),  // Cache operations
    ClientResponse(ClientResponse),// Operation results
    Ping(PingRequest),             // Health check
    Pong(PongResponse),            // Health response
}
```

#### RaftMessageWrapper

Wraps protobuf-encoded Raft messages for network transmission:

```rust
let wrapper = RaftMessageWrapper::from_raft_message(&raft_msg)?;
let raft_msg = wrapper.to_raft_message()?;
```

#### ClientRequest / ClientResponse

For cache operations:

```rust
// Request
ClientRequest {
    request_id: u64,
    command: CacheCommand,
}

// Response
ClientResponse {
    request_id: u64,
    success: bool,
    error: Option<String>,
    leader_hint: Option<NodeId>,  // If not leader
}
```

#### Ping / Pong

For peer discovery and health checking:

```rust
PingRequest { node_id, raft_addr }
PongResponse { node_id, raft_addr, leader_id }
```

## Wire Protocol

Messages are framed with a 4-byte length prefix:

```
┌─────────────────────────────────────────┐
│ Length (4 bytes, big-endian u32)        │
├─────────────────────────────────────────┤
│ Message Data (bincode-encoded)          │
│ ...                                     │
└─────────────────────────────────────────┘
```

Maximum message size: 16MB

## Serialization

- **Raft messages**: Protobuf (via raft-rs)
- **Application messages**: Bincode (fast binary format)

## Error Handling

```rust
NetworkError::ConnectionFailed { addr, reason }
NetworkError::ConnectionClosed
NetworkError::SendFailed(String)
NetworkError::ReceiveFailed(String)
NetworkError::Serialization(String)
NetworkError::Deserialization(String)
NetworkError::Io(std::io::Error)
```

## Connection Management

The `RaftTransport` maintains a connection cache for efficient messaging:
- Connections are reused when possible
- Failed connections are automatically removed
- New connections created on demand
