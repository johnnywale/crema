//! Real transport implementations for migration using existing network infrastructure.
//!
//! This module provides production-ready implementations of `DataTransporter` and
//! `MigrationRaftProposer` that use the existing `RaftTransport` and `RaftNode`
//! infrastructure rather than the NoOp placeholders.

use crate::consensus::transport::RaftTransport;
use crate::error::{Error, Result};
use crate::multiraft::migration::{TransferBatch, TransferEntry};
use crate::multiraft::migration_orchestrator::{
    DataTransporter, MigrationCommand, MigrationRaftProposer,
};
use crate::multiraft::shard::ShardId;
use crate::network::rpc::{
    Message, MigrationApplyRequest, MigrationApplyResponse, MigrationEntry, MigrationFetchRequest,
    MigrationFetchResponse, MigrationProposalForward, MigrationProposalForwardResponse,
    MigrationShardStatsRequest, MigrationShardStatsResponse,
};
use crate::types::NodeId;

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use uuid::Uuid;

/// Timeout for RPC operations.
const RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum batch size in bytes to prevent memory explosion.
/// Default: 4MB - prevents OOM from large batches while allowing efficient transfers.
pub const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// Age after which pending requests are considered stale and eligible for cleanup.
/// This should be significantly longer than RPC_TIMEOUT to avoid premature cleanup.
const STALE_REQUEST_AGE: Duration = Duration::from_secs(120);

/// Wrapper for pending requests with creation timestamp for GC.
struct PendingRequest<T> {
    sender: T,
    created_at: Instant,
}

impl<T> PendingRequest<T> {
    fn new(sender: T) -> Self {
        Self {
            sender,
            created_at: Instant::now(),
        }
    }

    fn is_stale(&self) -> bool {
        self.created_at.elapsed() > STALE_REQUEST_AGE
    }
}

/// Real data transporter using RaftTransport for network communication.
///
/// This implementation:
/// - Uses the existing RaftTransport for sending/receiving messages
/// - Handles request/response correlation via request IDs
/// - Provides timeout handling for stuck requests
/// - Includes periodic garbage collection to prevent memory leaks from abandoned requests
pub struct RpcDataTransporter {
    /// This node's ID.
    node_id: NodeId,
    /// The underlying transport.
    transport: Arc<RaftTransport>,
    /// Pending fetch requests awaiting responses (with timestamps for GC).
    pending_fetch:
        RwLock<HashMap<u64, PendingRequest<oneshot::Sender<Result<MigrationFetchResponse>>>>>,
    /// Pending apply requests awaiting responses (with timestamps for GC).
    pending_apply:
        RwLock<HashMap<u64, PendingRequest<oneshot::Sender<Result<MigrationApplyResponse>>>>>,
    /// Pending stats requests awaiting responses (with timestamps for GC).
    pending_stats:
        RwLock<HashMap<u64, PendingRequest<oneshot::Sender<Result<MigrationShardStatsResponse>>>>>,
    /// Request ID counter.
    next_request_id: AtomicU64,
    /// Local shard accessor for applying batches.
    shard_accessor: Option<Arc<dyn ShardAccessor>>,
}

impl std::fmt::Debug for RpcDataTransporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcDataTransporter")
            .field("node_id", &self.node_id)
            .field("pending_fetch_count", &self.pending_fetch.read().len())
            .field("pending_apply_count", &self.pending_apply.read().len())
            .field("pending_stats_count", &self.pending_stats.read().len())
            .finish()
    }
}

/// Trait for accessing local shards to fetch/apply data.
#[async_trait::async_trait]
pub trait ShardAccessor: Send + Sync + std::fmt::Debug {
    /// Fetch entries from a local shard with byte limit.
    ///
    /// # Arguments
    /// * `shard_id` - The shard to fetch from
    /// * `last_key` - Resume from this key (exclusive), None for start
    /// * `batch_size` - Maximum number of entries to fetch
    /// * `max_bytes` - Maximum total bytes to fetch (prevents OOM from large entries)
    ///
    /// # Returns
    /// * `Ok((entries, is_final))` - Entries fetched and whether this is the last batch
    ///
    /// # Implementation Requirements
    ///
    /// Implementations should stop adding entries when either `batch_size` or `max_bytes`
    /// is reached, whichever comes first.
    ///
    /// **CRITICAL**: To prevent infinite loops, if a single entry is larger than
    /// `max_bytes`, the implementation MUST still include that entry as a single-entry
    /// batch. This ensures forward progress even with oversized entries. Implementations
    /// should log a warning when returning oversized entries.
    async fn fetch_entries(
        &self,
        shard_id: ShardId,
        last_key: Option<&[u8]>,
        batch_size: usize,
        max_bytes: usize,
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

    /// Garbage collect stale pending requests to prevent memory leaks.
    ///
    /// This should be called periodically (e.g., every 60 seconds) to clean up
    /// requests that have been abandoned due to network failures or bugs.
    /// Returns the number of stale requests removed.
    pub fn garbage_collect(&self) -> usize {
        let mut removed = 0;

        // Clean up stale fetch requests
        {
            let mut pending = self.pending_fetch.write();
            let before = pending.len();
            pending.retain(|id, req| {
                if req.is_stale() {
                    tracing::warn!(
                        request_id = id,
                        age_secs = req.created_at.elapsed().as_secs(),
                        "Garbage collecting stale fetch request"
                    );
                    false
                } else {
                    true
                }
            });
            removed += before - pending.len();
        }

        // Clean up stale apply requests
        {
            let mut pending = self.pending_apply.write();
            let before = pending.len();
            pending.retain(|id, req| {
                if req.is_stale() {
                    tracing::warn!(
                        request_id = id,
                        age_secs = req.created_at.elapsed().as_secs(),
                        "Garbage collecting stale apply request"
                    );
                    false
                } else {
                    true
                }
            });
            removed += before - pending.len();
        }

        // Clean up stale stats requests
        {
            let mut pending = self.pending_stats.write();
            let before = pending.len();
            pending.retain(|id, req| {
                if req.is_stale() {
                    tracing::warn!(
                        request_id = id,
                        age_secs = req.created_at.elapsed().as_secs(),
                        "Garbage collecting stale stats request"
                    );
                    false
                } else {
                    true
                }
            });
            removed += before - pending.len();
        }

        if removed > 0 {
            tracing::info!(removed, "Garbage collected stale transport requests");
        }

        removed
    }

    /// Get the count of pending requests (for monitoring).
    pub fn pending_count(&self) -> (usize, usize, usize) {
        (
            self.pending_fetch.read().len(),
            self.pending_apply.read().len(),
            self.pending_stats.read().len(),
        )
    }

    /// Handle an incoming migration message (called by message handler).
    pub async fn handle_message(&self, msg: Message) -> Option<Message> {
        match msg {
            Message::MigrationFetchRequest(req) => Some(self.handle_fetch_request(req).await),
            Message::MigrationFetchResponse(resp) => {
                self.handle_fetch_response(resp);
                None
            }
            Message::MigrationApplyRequest(req) => Some(self.handle_apply_request(req).await),
            Message::MigrationApplyResponse(resp) => {
                self.handle_apply_response(resp);
                None
            }
            Message::MigrationShardStatsRequest(req) => Some(self.handle_stats_request(req).await),
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
            .fetch_entries(
                req.shard_id,
                req.last_key.as_deref(),
                req.batch_size,
                MAX_BATCH_BYTES,
            )
            .await
        {
            Ok((entries, is_final)) => {
                // Safety check: empty batch without is_final indicates potential infinite loop
                // (e.g., first entry larger than MAX_BATCH_BYTES)
                if entries.is_empty() && !is_final {
                    tracing::error!(
                        shard_id = req.shard_id,
                        last_key = ?req.last_key,
                        max_bytes = MAX_BATCH_BYTES,
                        "Empty batch returned but not final - possible oversized entry causing infinite loop"
                    );
                    return Message::MigrationFetchResponse(MigrationFetchResponse::error(
                        req.request_id,
                        format!(
                            "Migration stall detected: entry may exceed MAX_BATCH_BYTES ({}). \
                             Check ShardAccessor implementation.",
                            MAX_BATCH_BYTES
                        ),
                    ));
                }

                // Warn if batch size is unexpectedly large (possible oversized single entry)
                let total_bytes: usize = entries.iter().map(|e| e.key.len() + e.value.len()).sum();
                if entries.len() == 1 && total_bytes > MAX_BATCH_BYTES {
                    tracing::warn!(
                        shard_id = req.shard_id,
                        entry_bytes = total_bytes,
                        max_bytes = MAX_BATCH_BYTES,
                        "Oversized entry included to prevent migration stall"
                    );
                }

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
        if let Some(pending) = self.pending_fetch.write().remove(&resp.request_id) {
            let _ = pending.sender.send(Ok(resp));
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
        if let Some(pending) = self.pending_apply.write().remove(&resp.request_id) {
            let _ = pending.sender.send(Ok(resp));
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
        if let Some(pending) = self.pending_stats.write().remove(&resp.request_id) {
            let _ = pending.sender.send(Ok(resp));
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

        // Register pending request with timestamp for GC
        self.pending_fetch
            .write()
            .insert(request_id, PendingRequest::new(tx));

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

        self.pending_stats
            .write()
            .insert(request_id, PendingRequest::new(tx));

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

        self.pending_stats
            .write()
            .insert(request_id, PendingRequest::new(tx));

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

/// Maximum time to wait for a leader on startup before failing.
const WAIT_FOR_LEADER_TIMEOUT: Duration = Duration::from_secs(30);

/// Interval between leader checks when waiting.
const WAIT_FOR_LEADER_INTERVAL: Duration = Duration::from_millis(100);

/// Real Raft proposer that uses the existing RaftNode infrastructure.
///
/// Maximum age of leader hint before it's considered stale.
/// After this duration, we'll proactively try to verify the leader.
const LEADER_HINT_MAX_AGE: Duration = Duration::from_secs(30);

/// A leader hint with timestamp for staleness detection.
#[derive(Debug, Clone)]
struct TimestampedLeaderHint {
    leader: NodeId,
    updated_at: Instant,
}

impl TimestampedLeaderHint {
    fn new(leader: NodeId) -> Self {
        Self {
            leader,
            updated_at: Instant::now(),
        }
    }

    fn is_stale(&self) -> bool {
        self.updated_at.elapsed() > LEADER_HINT_MAX_AGE
    }

    /// Refresh the timestamp without changing the leader.
    fn refresh(&mut self) {
        self.updated_at = Instant::now();
    }
}

/// This implementation can work in two modes:
/// 1. Direct mode: Calls a provided proposal function (when we are the leader)
/// 2. Forward mode: Forwards to the leader node (when we are a follower)
///
/// ## Stale Leader Detection
///
/// Leader hints are timestamped to detect staleness. If a hint is older than
/// `LEADER_HINT_MAX_AGE` (30 seconds), we'll be more cautious about using it
/// and update it promptly when we get a redirect response.
pub struct RpcMigrationRaftProposer {
    /// This node's ID.
    node_id: NodeId,
    /// Function to propose commands locally (if we're the leader).
    local_proposer: RwLock<Option<RaftProposerFn>>,
    /// Transport for forwarding to leader.
    transport: Option<Arc<RaftTransport>>,
    /// Current leader hint with timestamp for staleness detection.
    leader_hint: RwLock<Option<TimestampedLeaderHint>>,
    /// Pending forwarded proposals awaiting responses (with timestamps for GC).
    pending_forwards: RwLock<
        HashMap<u64, PendingRequest<oneshot::Sender<Result<MigrationProposalForwardResponse>>>>,
    >,
    /// Request ID counter for forwarding.
    next_request_id: AtomicU64,
    /// Count of stale hint detections (for monitoring).
    stale_hint_count: AtomicU64,
}

impl std::fmt::Debug for RpcMigrationRaftProposer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let hint = self.leader_hint.read();
        f.debug_struct("RpcMigrationRaftProposer")
            .field("node_id", &self.node_id)
            .field("local_proposer", &self.local_proposer.read().is_some())
            .field("transport", &self.transport.is_some())
            .field("leader_hint", &hint.as_ref().map(|h| h.leader))
            .field(
                "leader_hint_age_secs",
                &hint.as_ref().map(|h| h.updated_at.elapsed().as_secs()),
            )
            .field("pending_forwards", &self.pending_forwards.read().len())
            .field(
                "stale_hint_count",
                &self.stale_hint_count.load(Ordering::Relaxed),
            )
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
            stale_hint_count: AtomicU64::new(0),
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

    /// Set the leader hint with fresh timestamp.
    pub fn set_leader_hint(&self, leader: Option<NodeId>) {
        *self.leader_hint.write() = leader.map(TimestampedLeaderHint::new);
    }

    /// Get the current leader hint (for monitoring).
    pub fn get_leader_hint(&self) -> Option<NodeId> {
        self.leader_hint.read().as_ref().map(|h| h.leader)
    }

    /// Check if the current leader hint is stale.
    pub fn is_leader_hint_stale(&self) -> bool {
        self.leader_hint
            .read()
            .as_ref()
            .map(|h| h.is_stale())
            .unwrap_or(true)
    }

    /// Set the local proposer.
    pub fn set_local_proposer(&self, proposer: RaftProposerFn) {
        *self.local_proposer.write() = Some(proposer);
    }

    /// Get the count of stale hint detections (for monitoring).
    pub fn stale_hint_count(&self) -> u64 {
        self.stale_hint_count.load(Ordering::Relaxed)
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
        let (leader_id, is_stale) = {
            let hint = self.leader_hint.read();
            match hint.as_ref() {
                Some(h) => {
                    let stale = h.is_stale();
                    if stale {
                        self.stale_hint_count.fetch_add(1, Ordering::Relaxed);
                        tracing::debug!(
                            node_id = self.node_id,
                            leader = h.leader,
                            age_secs = h.updated_at.elapsed().as_secs(),
                            "Using stale leader hint"
                        );
                    }
                    (Some(h.leader), stale)
                }
                None => (None, true),
            }
        };

        if let (Some(transport), Some(leader_id)) = (&self.transport, leader_id) {
            if leader_id == self.node_id {
                return Err(Error::Internal(
                    "We are supposed to be leader but no local proposer set".to_string(),
                ));
            }

            // Log if using stale hint
            if is_stale {
                tracing::info!(
                    node_id = self.node_id,
                    leader_id,
                    "Forwarding with stale leader hint - may get redirected"
                );
            }

            // Actually forward the proposal to the leader
            return self.forward_to_leader(transport, leader_id, command).await;
        }

        // No leader available - wait for leader election with backoff
        // This handles the startup case where leader_hint is None before first heartbeat
        if let Some(transport) = &self.transport {
            let start = std::time::Instant::now();
            tracing::info!(
                node_id = self.node_id,
                "No leader available, waiting for leader election"
            );

            while start.elapsed() < WAIT_FOR_LEADER_TIMEOUT {
                tokio::time::sleep(WAIT_FOR_LEADER_INTERVAL).await;

                // Check if local proposer was set (we became leader)
                if let Some(proposer) = self.local_proposer.read().as_ref() {
                    tracing::info!(node_id = self.node_id, "Became leader during wait");
                    return proposer(command);
                }

                // Check if leader hint was set (another node became leader)
                let leader = self.leader_hint.read().as_ref().map(|h| h.leader);
                if let Some(leader_id) = leader {
                    if leader_id == self.node_id {
                        // We're supposed to be leader but no proposer - keep waiting
                        continue;
                    }
                    tracing::info!(
                        node_id = self.node_id,
                        leader_id,
                        waited_ms = start.elapsed().as_millis(),
                        "Leader discovered, forwarding proposal"
                    );
                    return self.forward_to_leader(transport, leader_id, command).await;
                }
            }

            return Err(Error::Internal(format!(
                "No leader elected after {:?} - cluster may be partitioned or not enough nodes",
                WAIT_FOR_LEADER_TIMEOUT
            )));
        }

        Err(Error::Internal(
            "No proposer available and no transport configured for forwarding".to_string(),
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
        let command_bytes = bincode::serialize(&command).map_err(|e| {
            Error::Internal(format!("Failed to serialize migration command: {}", e))
        })?;

        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);

        // Create forward request
        let forward_req = MigrationProposalForward::new(request_id, self.node_id, command_bytes);

        // Setup response channel with timestamp for GC
        let (tx, rx) = oneshot::channel();
        self.pending_forwards
            .write()
            .insert(request_id, PendingRequest::new(tx));

        // Send the forward request
        let msg = Message::MigrationProposalForward(forward_req);
        if let Err(e) = transport.send_message(leader_id, msg).await {
            self.pending_forwards.write().remove(&request_id);
            return Err(Error::Internal(format!(
                "Failed to forward to leader: {}",
                e
            )));
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
                    // Leader changed, update hint with fresh timestamp
                    *self.leader_hint.write() = Some(TimestampedLeaderHint::new(new_leader));
                    Err(Error::Raft(crate::error::RaftError::NotLeader {
                        leader: Some(new_leader),
                    }))
                } else {
                    Err(Error::Internal(
                        response
                            .error
                            .unwrap_or_else(|| "Unknown forwarding error".to_string()),
                    ))
                }
            }
            Ok(Ok(Err(e))) => {
                // Forward request failed with an error
                Err(e)
            }
            Ok(Err(_)) => {
                // Channel closed - leader probably crashed
                Err(Error::Internal(
                    "Forward response channel closed".to_string(),
                ))
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
                    // We're not actually the leader, provide hint with fresh timestamp
                    *self.leader_hint.write() = leader.map(TimestampedLeaderHint::new);
                    MigrationProposalForwardResponse::not_leader(request.request_id, leader)
                }
                Err(e) => {
                    MigrationProposalForwardResponse::error(request.request_id, e.to_string())
                }
            }
        } else {
            // No local proposer - we're not the leader
            let leader = self.leader_hint.read().as_ref().map(|h| h.leader);
            MigrationProposalForwardResponse::not_leader(request.request_id, leader)
        }
    }

    /// Handle a forward response (called on the follower that sent the request).
    pub fn handle_forward_response(&self, response: MigrationProposalForwardResponse) {
        if let Some(pending) = self.pending_forwards.write().remove(&response.request_id) {
            let _ = pending.sender.send(Ok(response));
        } else {
            tracing::warn!(
                request_id = response.request_id,
                "Received forward response for unknown request"
            );
        }
    }

    /// Garbage collect stale pending forward requests.
    /// Returns the number of stale requests removed.
    pub fn garbage_collect(&self) -> usize {
        let mut pending = self.pending_forwards.write();
        let before = pending.len();
        pending.retain(|id, req| {
            if req.is_stale() {
                tracing::warn!(
                    request_id = id,
                    age_secs = req.created_at.elapsed().as_secs(),
                    "Garbage collecting stale forward request"
                );
                false
            } else {
                true
            }
        });
        before - pending.len()
    }

    /// Get the count of pending forwards (for monitoring).
    pub fn pending_count(&self) -> usize {
        self.pending_forwards.read().len()
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
