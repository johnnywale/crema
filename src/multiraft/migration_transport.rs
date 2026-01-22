//! Real transport implementations for migration using existing network infrastructure.
//!
//! This module provides production-ready implementations of `DataTransporter` and
//! `MigrationRaftProposer` that use the existing `RaftTransport` and `RaftNode`
//! infrastructure rather than the NoOp placeholders.

use crate::consensus::transport::RaftTransport;
use crate::error::{Error, Result};
use crate::multiraft::migration::{TransferBatch, TransferEntry};
use crate::multiraft::migration_orchestrator::{DataTransporter, MigrationRaftProposer, MigrationCommand};
use crate::multiraft::shard::ShardId;
use crate::network::rpc::{
    Message, MigrationApplyRequest, MigrationApplyResponse, MigrationEntry,
    MigrationFetchRequest, MigrationFetchResponse, MigrationShardStatsRequest,
    MigrationShardStatsResponse, MigrationProposalForward, MigrationProposalForwardResponse,
};
use crate::types::NodeId;

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use uuid::Uuid;

/// Timeout for RPC operations.
const RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// Real data transporter using RaftTransport for network communication.
///
/// This implementation:
/// - Uses the existing RaftTransport for sending/receiving messages
/// - Handles request/response correlation via request IDs
/// - Provides timeout handling for stuck requests
#[derive(Debug)]
pub struct RpcDataTransporter {
    /// This node's ID.
    node_id: NodeId,
    /// The underlying transport.
    transport: Arc<RaftTransport>,
    /// Pending fetch requests awaiting responses.
    pending_fetch: RwLock<HashMap<u64, oneshot::Sender<Result<MigrationFetchResponse>>>>,
    /// Pending apply requests awaiting responses.
    pending_apply: RwLock<HashMap<u64, oneshot::Sender<Result<MigrationApplyResponse>>>>,
    /// Pending stats requests awaiting responses.
    pending_stats: RwLock<HashMap<u64, oneshot::Sender<Result<MigrationShardStatsResponse>>>>,
    /// Request ID counter.
    next_request_id: AtomicU64,
    /// Local shard accessor for applying batches.
    shard_accessor: Option<Arc<dyn ShardAccessor>>,
}

/// Trait for accessing local shards to fetch/apply data.
#[async_trait::async_trait]
pub trait ShardAccessor: Send + Sync + std::fmt::Debug {
    /// Fetch entries from a local shard.
    async fn fetch_entries(
        &self,
        shard_id: ShardId,
        last_key: Option<&[u8]>,
        batch_size: usize,
    ) -> Result<(Vec<TransferEntry>, bool)>;

    /// Apply entries to a local shard.
    async fn apply_entries(&self, shard_id: ShardId, entries: Vec<TransferEntry>) -> Result<u64>;

    /// Get entry count for a shard.
    async fn get_entry_count(&self, shard_id: ShardId) -> Result<u64>;

    /// Get size in bytes for a shard.
    async fn get_size_bytes(&self, shard_id: ShardId) -> Result<u64>;
}

impl RpcDataTransporter {
    /// Create a new RPC data transporter.
    pub fn new(node_id: NodeId, transport: Arc<RaftTransport>) -> Self {
        Self {
            node_id,
            transport,
            pending_fetch: RwLock::new(HashMap::new()),
            pending_apply: RwLock::new(HashMap::new()),
            pending_stats: RwLock::new(HashMap::new()),
            next_request_id: AtomicU64::new(1),
            shard_accessor: None,
        }
    }

    /// Set the shard accessor for local operations.
    pub fn with_shard_accessor(mut self, accessor: Arc<dyn ShardAccessor>) -> Self {
        self.shard_accessor = Some(accessor);
        self
    }

    /// Set the shard accessor.
    pub fn set_shard_accessor(&mut self, accessor: Arc<dyn ShardAccessor>) {
        self.shard_accessor = Some(accessor);
    }

    /// Generate a unique request ID.
    fn next_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Handle an incoming migration message (called by message handler).
    pub async fn handle_message(&self, msg: Message) -> Option<Message> {
        match msg {
            Message::MigrationFetchRequest(req) => {
                Some(self.handle_fetch_request(req).await)
            }
            Message::MigrationFetchResponse(resp) => {
                self.handle_fetch_response(resp);
                None
            }
            Message::MigrationApplyRequest(req) => {
                Some(self.handle_apply_request(req).await)
            }
            Message::MigrationApplyResponse(resp) => {
                self.handle_apply_response(resp);
                None
            }
            Message::MigrationShardStatsRequest(req) => {
                Some(self.handle_stats_request(req).await)
            }
            Message::MigrationShardStatsResponse(resp) => {
                self.handle_stats_response(resp);
                None
            }
            _ => None,
        }
    }

    /// Handle incoming fetch request (we are the source node).
    async fn handle_fetch_request(&self, req: MigrationFetchRequest) -> Message {
        let Some(accessor) = &self.shard_accessor else {
            return Message::MigrationFetchResponse(MigrationFetchResponse::error(
                req.request_id,
                "No shard accessor configured",
            ));
        };

        match accessor
            .fetch_entries(req.shard_id, req.last_key.as_deref(), req.batch_size)
            .await
        {
            Ok((entries, is_final)) => {
                let migration_entries: Vec<MigrationEntry> = entries
                    .into_iter()
                    .map(|e| MigrationEntry::new(e.key, e.value, e.expires_at_nanos))
                    .collect();

                Message::MigrationFetchResponse(MigrationFetchResponse::success(
                    req.request_id,
                    migration_entries,
                    is_final,
                    0, // Sequence set by caller
                ))
            }
            Err(e) => Message::MigrationFetchResponse(MigrationFetchResponse::error(
                req.request_id,
                e.to_string(),
            )),
        }
    }

    /// Handle incoming fetch response (we requested the data).
    fn handle_fetch_response(&self, resp: MigrationFetchResponse) {
        if let Some(tx) = self.pending_fetch.write().remove(&resp.request_id) {
            let _ = tx.send(Ok(resp));
        }
    }

    /// Handle incoming apply request (we are the target node).
    async fn handle_apply_request(&self, req: MigrationApplyRequest) -> Message {
        let Some(accessor) = &self.shard_accessor else {
            return Message::MigrationApplyResponse(MigrationApplyResponse::error(
                req.request_id,
                "No shard accessor configured",
            ));
        };

        let entries: Vec<TransferEntry> = req
            .entries
            .into_iter()
            .map(|e| match e.expires_at_nanos {
                Some(expires) => TransferEntry::with_expiration(e.key, e.value, expires),
                None => TransferEntry::new(e.key, e.value),
            })
            .collect();

        match accessor.apply_entries(req.shard_id, entries).await {
            Ok(count) => Message::MigrationApplyResponse(MigrationApplyResponse::success(
                req.request_id,
                count,
            )),
            Err(e) => Message::MigrationApplyResponse(MigrationApplyResponse::error(
                req.request_id,
                e.to_string(),
            )),
        }
    }

    /// Handle incoming apply response.
    fn handle_apply_response(&self, resp: MigrationApplyResponse) {
        if let Some(tx) = self.pending_apply.write().remove(&resp.request_id) {
            let _ = tx.send(Ok(resp));
        }
    }

    /// Handle incoming stats request.
    async fn handle_stats_request(&self, req: MigrationShardStatsRequest) -> Message {
        let Some(accessor) = &self.shard_accessor else {
            return Message::MigrationShardStatsResponse(MigrationShardStatsResponse::error(
                req.request_id,
                "No shard accessor configured",
            ));
        };

        let entry_count = accessor.get_entry_count(req.shard_id).await.unwrap_or(0);
        let size_bytes = accessor.get_size_bytes(req.shard_id).await.unwrap_or(0);

        Message::MigrationShardStatsResponse(MigrationShardStatsResponse::success(
            req.request_id,
            entry_count,
            size_bytes,
        ))
    }

    /// Handle incoming stats response.
    fn handle_stats_response(&self, resp: MigrationShardStatsResponse) {
        if let Some(tx) = self.pending_stats.write().remove(&resp.request_id) {
            let _ = tx.send(Ok(resp));
        }
    }
}

#[async_trait::async_trait]
impl DataTransporter for RpcDataTransporter {
    async fn fetch_batch(
        &self,
        source_node: NodeId,
        shard_id: ShardId,
        last_key: Option<&[u8]>,
        batch_size: usize,
    ) -> Result<TransferBatch> {
        let request_id = self.next_request_id();
        let (tx, rx) = oneshot::channel();

        // Register pending request
        self.pending_fetch.write().insert(request_id, tx);

        // Create and send request
        let req = MigrationFetchRequest::new(
            request_id,
            shard_id,
            last_key.map(|k| k.to_vec()),
            batch_size,
        );

        if let Err(e) = self
            .transport
            .send_message(source_node, Message::MigrationFetchRequest(req))
            .await
        {
            self.pending_fetch.write().remove(&request_id);
            return Err(e);
        }

        // Wait for response with timeout
        match tokio::time::timeout(RPC_TIMEOUT, rx).await {
            Ok(Ok(Ok(resp))) => {
                if !resp.success {
                    return Err(Error::RemoteError(
                        resp.error.unwrap_or_else(|| "Unknown error".to_string()),
                    ));
                }

                let entries: Vec<TransferEntry> = resp
                    .entries
                    .into_iter()
                    .map(|e| match e.expires_at_nanos {
                        Some(expires) => TransferEntry::with_expiration(e.key, e.value, expires),
                        None => TransferEntry::new(e.key, e.value),
                    })
                    .collect();

                Ok(TransferBatch::new(
                    Uuid::new_v4(),
                    resp.sequence,
                    entries,
                    resp.is_final,
                ))
            }
            Ok(Ok(Err(e))) => {
                self.pending_fetch.write().remove(&request_id);
                Err(e)
            }
            Ok(Err(_)) => {
                self.pending_fetch.write().remove(&request_id);
                Err(Error::Internal("Response channel closed".to_string()))
            }
            Err(_) => {
                self.pending_fetch.write().remove(&request_id);
                Err(Error::Timeout)
            }
        }
    }

    async fn apply_batch(&self, shard_id: ShardId, batch: TransferBatch) -> Result<()> {
        // Apply locally using shard accessor
        let Some(accessor) = &self.shard_accessor else {
            return Err(Error::Internal("No shard accessor configured".to_string()));
        };

        accessor.apply_entries(shard_id, batch.entries).await?;
        Ok(())
    }

    async fn get_shard_entry_count(&self, node_id: NodeId, shard_id: ShardId) -> Result<u64> {
        // If local, use accessor directly
        if node_id == self.node_id {
            if let Some(accessor) = &self.shard_accessor {
                return accessor.get_entry_count(shard_id).await;
            }
        }

        let request_id = self.next_request_id();
        let (tx, rx) = oneshot::channel();

        self.pending_stats.write().insert(request_id, tx);

        let req = MigrationShardStatsRequest::new(request_id, shard_id);

        if let Err(e) = self
            .transport
            .send_message(node_id, Message::MigrationShardStatsRequest(req))
            .await
        {
            self.pending_stats.write().remove(&request_id);
            return Err(e);
        }

        match tokio::time::timeout(RPC_TIMEOUT, rx).await {
            Ok(Ok(Ok(resp))) => {
                if !resp.success {
                    return Err(Error::RemoteError(
                        resp.error.unwrap_or_else(|| "Unknown error".to_string()),
                    ));
                }
                Ok(resp.entry_count)
            }
            Ok(Ok(Err(e))) => {
                self.pending_stats.write().remove(&request_id);
                Err(e)
            }
            Ok(Err(_)) => {
                self.pending_stats.write().remove(&request_id);
                Err(Error::Internal("Response channel closed".to_string()))
            }
            Err(_) => {
                self.pending_stats.write().remove(&request_id);
                Err(Error::Timeout)
            }
        }
    }

    async fn get_shard_size(&self, node_id: NodeId, shard_id: ShardId) -> Result<u64> {
        // If local, use accessor directly
        if node_id == self.node_id {
            if let Some(accessor) = &self.shard_accessor {
                return accessor.get_size_bytes(shard_id).await;
            }
        }

        let request_id = self.next_request_id();
        let (tx, rx) = oneshot::channel();

        self.pending_stats.write().insert(request_id, tx);

        let req = MigrationShardStatsRequest::new(request_id, shard_id);

        if let Err(e) = self
            .transport
            .send_message(node_id, Message::MigrationShardStatsRequest(req))
            .await
        {
            self.pending_stats.write().remove(&request_id);
            return Err(e);
        }

        match tokio::time::timeout(RPC_TIMEOUT, rx).await {
            Ok(Ok(Ok(resp))) => {
                if !resp.success {
                    return Err(Error::RemoteError(
                        resp.error.unwrap_or_else(|| "Unknown error".to_string()),
                    ));
                }
                Ok(resp.size_bytes)
            }
            Ok(Ok(Err(e))) => {
                self.pending_stats.write().remove(&request_id);
                Err(e)
            }
            Ok(Err(_)) => {
                self.pending_stats.write().remove(&request_id);
                Err(Error::Internal("Response channel closed".to_string()))
            }
            Err(_) => {
                self.pending_stats.write().remove(&request_id);
                Err(Error::Timeout)
            }
        }
    }
}

/// Callback type for proposing commands via Raft.
pub type RaftProposerFn = Arc<dyn Fn(MigrationCommand) -> Result<()> + Send + Sync>;

/// Timeout for proposal forwarding.
const FORWARD_TIMEOUT: Duration = Duration::from_secs(10);

/// Real Raft proposer that uses the existing RaftNode infrastructure.
///
/// This implementation can work in two modes:
/// 1. Direct mode: Calls a provided proposal function (when we are the leader)
/// 2. Forward mode: Forwards to the leader node (when we are a follower)
pub struct RpcMigrationRaftProposer {
    /// This node's ID.
    node_id: NodeId,
    /// Function to propose commands locally (if we're the leader).
    local_proposer: RwLock<Option<RaftProposerFn>>,
    /// Transport for forwarding to leader.
    transport: Option<Arc<RaftTransport>>,
    /// Current leader hint.
    leader_hint: RwLock<Option<NodeId>>,
    /// Pending forwarded proposals awaiting responses.
    pending_forwards: RwLock<HashMap<u64, oneshot::Sender<Result<MigrationProposalForwardResponse>>>>,
    /// Request ID counter for forwarding.
    next_request_id: AtomicU64,
}

impl std::fmt::Debug for RpcMigrationRaftProposer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcMigrationRaftProposer")
            .field("node_id", &self.node_id)
            .field("local_proposer", &self.local_proposer.read().is_some())
            .field("transport", &self.transport.is_some())
            .field("leader_hint", &*self.leader_hint.read())
            .field("pending_forwards", &self.pending_forwards.read().len())
            .finish()
    }
}

impl RpcMigrationRaftProposer {
    /// Create a new proposer.
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            local_proposer: RwLock::new(None),
            transport: None,
            leader_hint: RwLock::new(None),
            pending_forwards: RwLock::new(HashMap::new()),
            next_request_id: AtomicU64::new(1),
        }
    }

    /// Set the local proposer function.
    pub fn with_local_proposer(self, proposer: RaftProposerFn) -> Self {
        *self.local_proposer.write() = Some(proposer);
        self
    }

    /// Set the transport for forwarding.
    pub fn with_transport(mut self, transport: Arc<RaftTransport>) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Set the leader hint.
    pub fn set_leader_hint(&self, leader: Option<NodeId>) {
        *self.leader_hint.write() = leader;
    }

    /// Set the local proposer.
    pub fn set_local_proposer(&self, proposer: RaftProposerFn) {
        *self.local_proposer.write() = Some(proposer);
    }
}

#[async_trait::async_trait]
impl MigrationRaftProposer for RpcMigrationRaftProposer {
    async fn propose_migration(&self, command: MigrationCommand) -> Result<()> {
        // Try local proposer first
        if let Some(proposer) = self.local_proposer.read().as_ref() {
            return proposer(command);
        }

        // Forward to leader if we have transport and leader hint
        let leader = self.leader_hint.read().clone();
        if let (Some(transport), Some(leader_id)) = (&self.transport, leader) {
            if leader_id == self.node_id {
                return Err(Error::Internal(
                    "We are supposed to be leader but no local proposer set".to_string(),
                ));
            }

            // Actually forward the proposal to the leader
            return self.forward_to_leader(transport, leader_id, command).await;
        }

        Err(Error::Internal(
            "No proposer available and no leader to forward to".to_string(),
        ))
    }
}

impl RpcMigrationRaftProposer {
    /// Forward a migration proposal to the leader node.
    async fn forward_to_leader(
        &self,
        transport: &Arc<RaftTransport>,
        leader_id: NodeId,
        command: MigrationCommand,
    ) -> Result<()> {
        // Serialize the command
        let command_bytes = bincode::serialize(&command)
            .map_err(|e| Error::Internal(format!("Failed to serialize migration command: {}", e)))?;

        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);

        // Create forward request
        let forward_req = MigrationProposalForward::new(
            request_id,
            self.node_id,
            command_bytes,
        );

        // Setup response channel
        let (tx, rx) = oneshot::channel();
        self.pending_forwards.write().insert(request_id, tx);

        // Send the forward request
        let msg = Message::MigrationProposalForward(forward_req);
        if let Err(e) = transport.send_message(leader_id, msg).await {
            self.pending_forwards.write().remove(&request_id);
            return Err(Error::Internal(format!("Failed to forward to leader: {}", e)));
        }

        tracing::debug!(
            request_id,
            leader_id,
            command_type = ?std::mem::discriminant(&command),
            "Forwarded migration proposal to leader"
        );

        // Wait for response with timeout
        match tokio::time::timeout(FORWARD_TIMEOUT, rx).await {
            Ok(Ok(Ok(response))) => {
                // Successfully received a success response
                if response.success {
                    Ok(())
                } else if let Some(new_leader) = response.leader_hint {
                    // Leader changed, update hint
                    *self.leader_hint.write() = Some(new_leader);
                    Err(Error::Raft(crate::error::RaftError::NotLeader {
                        leader: Some(new_leader),
                    }))
                } else {
                    Err(Error::Internal(
                        response.error.unwrap_or_else(|| "Unknown forwarding error".to_string()),
                    ))
                }
            }
            Ok(Ok(Err(e))) => {
                // Forward request failed with an error
                Err(e)
            }
            Ok(Err(_)) => {
                // Channel closed - leader probably crashed
                Err(Error::Internal("Forward response channel closed".to_string()))
            }
            Err(_) => {
                // Timeout
                self.pending_forwards.write().remove(&request_id);
                Err(Error::Timeout)
            }
        }
    }

    /// Handle a forwarded migration proposal (called on the leader).
    ///
    /// Returns a response to send back to the originating node.
    pub async fn handle_forward_request(
        &self,
        request: MigrationProposalForward,
    ) -> MigrationProposalForwardResponse {
        // Deserialize the command
        let command: MigrationCommand = match bincode::deserialize(&request.command_bytes) {
            Ok(cmd) => cmd,
            Err(e) => {
                return MigrationProposalForwardResponse::error(
                    request.request_id,
                    format!("Failed to deserialize command: {}", e),
                );
            }
        };

        // Try to propose locally
        if let Some(proposer) = self.local_proposer.read().as_ref() {
            match proposer(command) {
                Ok(()) => MigrationProposalForwardResponse::success(request.request_id),
                Err(Error::Raft(crate::error::RaftError::NotLeader { leader })) => {
                    // We're not actually the leader, provide hint
                    *self.leader_hint.write() = leader;
                    MigrationProposalForwardResponse::not_leader(request.request_id, leader)
                }
                Err(e) => {
                    MigrationProposalForwardResponse::error(request.request_id, e.to_string())
                }
            }
        } else {
            // No local proposer - we're not the leader
            let leader = self.leader_hint.read().clone();
            MigrationProposalForwardResponse::not_leader(request.request_id, leader)
        }
    }

    /// Handle a forward response (called on the follower that sent the request).
    pub fn handle_forward_response(&self, response: MigrationProposalForwardResponse) {
        if let Some(tx) = self.pending_forwards.write().remove(&response.request_id) {
            let _ = tx.send(Ok(response));
        } else {
            tracing::warn!(
                request_id = response.request_id,
                "Received forward response for unknown request"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_entry_size() {
        let entry = MigrationEntry::new(vec![1, 2, 3], vec![4, 5, 6, 7], Some(12345));
        assert_eq!(entry.size(), 3 + 4 + 8); // key + value + expires
    }

    #[test]
    fn test_fetch_response_size() {
        let entries = vec![
            MigrationEntry::new(vec![1, 2], vec![3, 4], None),
            MigrationEntry::new(vec![5, 6, 7], vec![8, 9], Some(100)),
        ];
        let resp = MigrationFetchResponse::success(1, entries, false, 0);
        assert_eq!(resp.size(), (2 + 2 + 8) + (3 + 2 + 8));
    }

    #[tokio::test]
    async fn test_proposer_no_local_no_leader() {
        let proposer = RpcMigrationRaftProposer::new(1);
        let cmd = MigrationCommand::transfer_ownership(Uuid::new_v4(), 0, 1, 2, 1);
        let result = proposer.propose_migration(cmd).await;
        assert!(result.is_err());
    }
}
