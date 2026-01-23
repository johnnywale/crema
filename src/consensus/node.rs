//! Raft node wrapper that drives the consensus algorithm.

use crate::checkpoint::{
    deserialize_snapshot_data, is_valid_snapshot_data, serialize_snapshot_data, CheckpointConfig,
    CheckpointManager, RaftStateProvider, SnapshotMetadata,
};
use crate::config::RaftConfig;
use crate::consensus::flow_control::FlowControl;
use crate::consensus::health::{HealthChecker, HealthReport, HealthStatus};
use crate::consensus::state_machine::CacheStateMachine;
use crate::config::RaftStorageType;
use crate::consensus::storage::RaftStorage;
use crate::consensus::transport::{BackpressureCallback, RaftTransport};
use crate::error::{RaftError, Result};
use crate::network::rpc::Message;
use crate::types::{CacheCommand, NodeId, ProposalResult};
use parking_lot::{Mutex, RwLock};
use protobuf::Message as ProtobufMessage;
use raft::prelude::{ConfChange, EntryType, Message as RaftMessage};
use raft::{RawNode, Storage};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

/// Pending proposal waiting for commit.
struct PendingProposal {
    /// Channel to notify when committed or failed.
    tx: oneshot::Sender<Result<ProposalResult>>,
}

/// The Raft node wrapper.
pub struct RaftNode {
    /// The underlying raft-rs RawNode.
    node: Arc<Mutex<RawNode<RaftStorage>>>,

    /// Raft storage (shared reference for access outside RawNode).
    storage: RaftStorage,

    /// State machine for applying commands.
    state_machine: Arc<CacheStateMachine>,

    /// Transport for sending messages.
    transport: Arc<RaftTransport>,

    /// Pending proposals indexed by proposal ID.
    pending: Arc<Mutex<HashMap<u64, PendingProposal>>>,

    /// Next proposal ID.
    next_proposal_id: Arc<AtomicU64>,

    /// This node's ID.
    id: NodeId,

    /// Current leader ID.
    leader_id: AtomicU64,

    /// Whether this node has a valid leader.
    has_leader: AtomicBool,

    /// Configuration.
    config: RaftConfig,

    /// Checkpoint manager (optional).
    checkpoint_manager: RwLock<Option<Arc<CheckpointManager>>>,

    /// Flow control for backpressure handling.
    flow_control: Arc<FlowControl>,

    /// Whether this node is accepting new proposals.
    accepting_proposals: AtomicBool,

    /// Health checker for monitoring.
    health_checker: HealthChecker,
}

impl RaftNode {
    /// Create a new Raft node.
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        config: RaftConfig,
        state_machine: Arc<CacheStateMachine>,
    ) -> Result<Arc<Self>> {
        // Create storage with initial voters
        let mut voters = peers.clone();
        if !voters.contains(&id) {
            voters.push(id);
        }
        voters.sort();

        // Create storage based on configuration
        // IMPORTANT: RawNode needs to see our storage updates, so we must use the same instance
        let storage = Self::create_storage(&config.storage_type, voters.clone())?;

        debug!("RAFT_NODE: Creating node {} with voters {:?}", id, voters);

        // Verify storage ConfState before creating RawNode
        {
            let initial_state = storage
                .initial_state()
                .map_err(|e| RaftError::Internal(e.to_string()))?;
            debug!(
                "RAFT_NODE: node {} initial_state from storage: term={}, vote={}, commit={}, voters={:?}, learners={:?}",
                id,
                initial_state.hard_state.term,
                initial_state.hard_state.vote,
                initial_state.hard_state.commit,
                initial_state.conf_state.voters,
                initial_state.conf_state.learners
            );
        }

        // Create raft config
        let raft_config = config.to_raft_config(id);

        // Create a logger (using slog)
        let logger = slog::Logger::root(slog::Discard, slog::o!());

        // Create the raw node with a clone of our storage (MemStorage uses Arc internally)
        let node = RawNode::new(&raft_config, storage.clone(), &logger)
            .map_err(|e| RaftError::Internal(e.to_string()))?;

        // Debug: print what raft-rs sees as voters
        let prs_voters: Vec<_> = node.raft.prs().conf().voters().ids().iter().collect();
        debug!(
            "RAFT_NODE: node {} ProgressTracker voters: {:?}",
            id, prs_voters
        );

        // CRITICAL: Verify raft-rs properly initialized the voters
        if prs_voters.len() != voters.len() {
            warn!(
                "RAFT_NODE: node {} VOTER MISMATCH! Expected {} voters {:?}, but raft-rs has {} voters {:?}",
                id, voters.len(), voters, prs_voters.len(), prs_voters
            );
        }

        // Verify this node is in the voter list
        if !prs_voters.contains(&id) {
            error!(
                "RAFT_NODE: node {} is NOT in the voter list! This node cannot participate in elections.",
                id
            );
        }

        // Create transport
        let transport = Arc::new(RaftTransport::new(id));

        // Create flow control with configurable max inflight
        let flow_control = Arc::new(FlowControl::new(config.max_inflight_msgs as usize));

        // Register backpressure callback
        // Note: The callback is prepared here for future integration when
        // RaftTransport supports setting callbacks after creation.
        let flow_control_for_callback = flow_control.clone();
        let node_id_for_callback = id;
        let _backpressure_callback: BackpressureCallback = Arc::new(move |event| {
            debug!(
                node_id = node_id_for_callback,
                "Backpressure event received: {:?}", event
            );
            flow_control_for_callback.handle_backpressure_event(event);
        });

        let raft_node = Arc::new(Self {
            node: Arc::new(Mutex::new(node)),
            storage,
            state_machine,
            transport,
            pending: Arc::new(Mutex::new(HashMap::new())),
            next_proposal_id: Arc::new(AtomicU64::new(1)),
            id,
            leader_id: AtomicU64::new(0),
            has_leader: AtomicBool::new(false),
            config,
            checkpoint_manager: RwLock::new(None),
            flow_control,
            accepting_proposals: AtomicBool::new(true),
            health_checker: HealthChecker::new(),
        });

        // Store the callback for later use if needed
        // The transport already has backpressure callback support

        Ok(raft_node)
    }

    /// Create storage based on the configuration.
    fn create_storage(storage_type: &RaftStorageType, voters: Vec<u64>) -> Result<RaftStorage> {
        match storage_type {
            RaftStorageType::Memory => {
                debug!("Creating in-memory storage for Raft");
                Ok(RaftStorage::new_memory_with_conf_state(voters))
            }
            #[cfg(feature = "rocksdb-storage")]
            RaftStorageType::RocksDb(config) => {
                debug!(path = %config.path, sync = config.sync_writes, "Creating RocksDB storage for Raft");
                let rocksdb_config = crate::consensus::rocksdb_storage::RocksDbStorageConfig {
                    path: config.path.clone(),
                    sync_writes: config.sync_writes,
                    ..Default::default()
                };
                RaftStorage::new_rocksdb_with_conf_state(rocksdb_config, voters)
            }
        }
    }

    /// Initialize checkpoint manager for this node.
    ///
    /// This should be called after creating the node if checkpointing is desired.
    pub fn init_checkpoint_manager(
        &self,
        config: CheckpointConfig,
    ) -> std::result::Result<Arc<CheckpointManager>, crate::checkpoint::FormatError> {
        let manager = Arc::new(CheckpointManager::new(
            config,
            self.state_machine.storage().clone(),
        )?);
        *self.checkpoint_manager.write() = Some(manager.clone());
        Ok(manager)
    }

    /// Set an existing checkpoint manager on this node.
    ///
    /// This is used during recovery when the checkpoint manager is created
    /// before the Raft node to determine the initial applied index.
    pub fn set_checkpoint_manager(&self, manager: Arc<CheckpointManager>) {
        *self.checkpoint_manager.write() = Some(manager);
    }

    /// Get the checkpoint manager if configured.
    pub fn checkpoint_manager(&self) -> Option<Arc<CheckpointManager>> {
        self.checkpoint_manager.read().clone()
    }

    /// Create a snapshot of the current state.
    ///
    /// This should only be called on the leader.
    pub async fn create_snapshot(&self) -> Result<SnapshotMetadata> {
        let manager =
            self.checkpoint_manager.read().clone().ok_or_else(|| {
                RaftError::Internal("checkpoint manager not configured".to_string())
            })?;

        let (index, term) = (self.applied_index(), self.term());

        // Also create a Raft snapshot for InstallSnapshot RPC
        self.create_raft_snapshot_internal(index, term)?;

        manager
            .create_snapshot(index, term)
            .await
            .map_err(|e| RaftError::Internal(e.to_string()).into())
    }

    /// Create a Raft snapshot for InstallSnapshot RPC.
    ///
    /// This serializes the current Moka cache state and stores it in the Raft
    /// storage so that `Storage::snapshot()` can return it when a lagging
    /// follower needs to be caught up.
    ///
    /// The snapshot includes:
    /// - All current cache entries (key-value pairs)
    /// - Raft metadata (index, term, conf_state)
    ///
    /// # Returns
    /// The Raft index at which the snapshot was created.
    pub fn create_raft_snapshot(&self) -> Result<u64> {
        let index = self.applied_index();
        let term = self.term();
        self.create_raft_snapshot_internal(index, term)?;
        Ok(index)
    }

    /// Internal method to create Raft snapshot at a specific index/term.
    fn create_raft_snapshot_internal(&self, index: u64, term: u64) -> Result<()> {
        // Collect all entries from the cache with expiration times
        let entries = self.state_machine.storage().collect_entries_with_expiration();
        let entry_count = entries.len();

        // Serialize entries using LZ4 compression (includes expiration times)
        let data = serialize_snapshot_data(
            entries.iter().map(|(k, v, expires)| (k.as_ref(), v.as_ref(), *expires)),
        )
        .map_err(|e| RaftError::Internal(format!("Failed to serialize snapshot data: {}", e)))?;

        let data_size = data.len();

        // Get current conf_state from storage
        let conf_state = {
            let node = self.node.lock();
            node.raft.store().initial_state().map_err(|e| {
                RaftError::Internal(format!("Failed to get conf_state: {}", e))
            })?.conf_state
        };

        // Create Raft Snapshot
        let mut snapshot = raft::prelude::Snapshot::default();
        snapshot.mut_metadata().index = index;
        snapshot.mut_metadata().term = term;
        *snapshot.mut_metadata().mut_conf_state() = conf_state;
        snapshot.data = data.into();

        // Store the snapshot
        self.storage.set_snapshot(snapshot)?;

        debug!(
            node_id = self.id,
            index,
            term,
            entry_count,
            data_size,
            "Created Raft snapshot for InstallSnapshot RPC"
        );

        Ok(())
    }

    /// Load state from the latest snapshot.
    ///
    /// Returns the Raft index of the loaded snapshot, or None if no snapshot exists.
    pub async fn recover_from_snapshot(&self) -> Result<Option<u64>> {
        let manager = match self.checkpoint_manager.read().clone() {
            Some(m) => m,
            None => {
                debug!("No checkpoint manager configured, skipping recovery");
                return Ok(None);
            }
        };

        let latest = manager
            .find_latest_snapshot()
            .map_err(|e| RaftError::Internal(e.to_string()))?;

        match latest {
            Some(info) => {
                debug!(
                    path = %info.path.display(),
                    raft_index = info.raft_index,
                    raft_term = info.raft_term,
                    "Loading snapshot for recovery"
                );
                let index = manager
                    .load_snapshot(&info.path)
                    .await
                    .map_err(|e| RaftError::Internal(e.to_string()))?;

                // Update state machine's applied state to match snapshot
                // This prevents re-applying entries that were already in the snapshot
                self.state_machine
                    .set_recovered_state(info.raft_index, info.raft_term);

                debug!(
                    raft_index = info.raft_index,
                    raft_term = info.raft_term,
                    "State machine recovered from snapshot"
                );

                Ok(Some(index))
            }
            None => {
                debug!("No snapshot found for recovery");
                Ok(None)
            }
        }
    }

    /// Record an entry for checkpoint threshold tracking.
    ///
    /// Returns `true` if the entry was recorded, `false` if backpressure was applied.
    pub fn record_checkpoint_entry(&self) -> bool {
        if let Some(manager) = self.checkpoint_manager.read().as_ref() {
            if let Err(_e) = manager.record_entry() {
                // Backpressure - snapshot is taking too long
                // We still allow the operation but log the event
                return false;
            }
        }
        true
    }

    /// Check if a snapshot should be created based on configured thresholds.
    pub fn should_create_snapshot(&self) -> bool {
        self.checkpoint_manager
            .read()
            .as_ref()
            .map(|m| m.should_snapshot())
            .unwrap_or(false)
    }

    /// Get this node's ID.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Get the current leader ID, if known.
    pub fn leader_id(&self) -> Option<NodeId> {
        if self.has_leader.load(Ordering::Relaxed) {
            Some(self.leader_id.load(Ordering::Relaxed))
        } else {
            None
        }
    }

    /// Check if this node is the leader.
    pub fn is_leader(&self) -> bool {
        self.leader_id() == Some(self.id)
    }

    /// Get the transport for adding peers.
    pub fn transport(&self) -> &Arc<RaftTransport> {
        &self.transport
    }

    /// Get the state machine.
    pub fn state_machine(&self) -> &Arc<CacheStateMachine> {
        &self.state_machine
    }

    /// Get the current term.
    pub fn term(&self) -> u64 {
        let node = self.node.lock();
        node.raft.term
    }

    /// Get the commit index.
    pub fn commit_index(&self) -> u64 {
        let node = self.node.lock();
        node.raft.raft_log.committed
    }

    /// Get the applied index.
    pub fn applied_index(&self) -> u64 {
        self.state_machine.applied_index()
    }

    /// Get the current voters in the cluster.
    /// This is useful for debugging and verifying cluster configuration.
    pub fn voters(&self) -> Vec<NodeId> {
        let node = self.node.lock();
        node.raft.prs().conf().voters().ids().iter().collect()
    }

    /// Check if a given node ID is a known voter in this cluster.
    pub fn is_known_voter(&self, node_id: NodeId) -> bool {
        self.voters().contains(&node_id)
    }

    /// Request a linearizable read using the Read-Index protocol.
    ///
    /// This method implements the Read-Index protocol for linearizable reads:
    /// 1. Verifies this node is the leader
    /// 2. Initiates a read_index request with a unique context
    /// 3. Waits for the ReadState to be ready (confirms leadership via quorum)
    /// 4. Waits for the state machine to apply up to the read index
    ///
    /// After this method returns successfully, it's safe to read from the local
    /// state machine with linearizable consistency.
    ///
    /// # Returns
    /// - `Ok(read_index)` - The index at which the read is linearizable
    /// - `Err(NotLeader)` - This node is not the leader
    /// - `Err(Timeout)` - Read-Index request timed out
    pub async fn read_index(&self) -> Result<u64> {
        // Step 1: Check if we're the leader
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader: self.leader_id(),
            }
            .into());
        }

        // Step 2: Generate unique context for this read request
        let read_id = self.next_proposal_id.fetch_add(1, Ordering::SeqCst);
        let ctx = read_id.to_le_bytes().to_vec();

        debug!(
            node_id = self.id,
            read_id, "READ_INDEX: Initiating read-index request"
        );

        // Step 3: Request read index from Raft
        {
            let mut node = self.node.lock();
            node.read_index(ctx.clone().into());
        }

        // Step 4: Wait for ReadState with our context
        let timeout = Duration::from_millis(
            self.config.election_tick as u64 * self.config.tick_interval_ms * 2,
        );
        let start = Instant::now();

        loop {
            // Check timeout
            if start.elapsed() > timeout {
                warn!(
                    node_id = self.id,
                    read_id, "READ_INDEX: Timeout waiting for ReadState"
                );
                return Err(RaftError::Internal("read-index timeout".to_string()).into());
            }

            // Check if we're still leader
            if !self.is_leader() {
                return Err(RaftError::NotLeader {
                    leader: self.leader_id(),
                }
                .into());
            }

            // Check for our ReadState in ready
            {
                let mut node = self.node.lock();
                if node.has_ready() {
                    let ready = node.ready();
                    let read_states = ready.read_states();

                    for rs in read_states {
                        if rs.request_ctx.as_slice() == ctx.as_slice() {
                            let read_index = rs.index;
                            debug!(
                                node_id = self.id,
                                read_id, read_index, "READ_INDEX: Got ReadState"
                            );

                            // Advance the ready state
                            let light_ready = node.advance(ready);
                            node.advance_apply();

                            // Send any messages generated
                            drop(node);

                            // Process any messages from light_ready
                            let messages = light_ready.messages();
                            if !messages.is_empty() {
                                self.transport.send_messages(messages.to_vec());
                            }

                            // Step 5: Wait for applied_index to reach read_index
                            while self.applied_index() < read_index {
                                if start.elapsed() > timeout {
                                    warn!(
                                        node_id = self.id,
                                        read_id,
                                        read_index,
                                        applied = self.applied_index(),
                                        "READ_INDEX: Timeout waiting for apply"
                                    );
                                    return Err(
                                        RaftError::Internal("read-index apply timeout".to_string())
                                            .into(),
                                    );
                                }
                                tokio::task::yield_now().await;
                            }

                            debug!(
                                node_id = self.id,
                                read_id, read_index, "READ_INDEX: Read linearizable at index"
                            );
                            return Ok(read_index);
                        }
                    }

                    // ReadState not found yet, advance ready anyway
                    let _light_ready = node.advance(ready);
                    node.advance_apply();
                }
            }

            // Yield to allow other tasks and tick processing
            tokio::task::yield_now().await;
        }
    }

    /// Propose a command to the Raft cluster.
    ///
    /// This will return when the command is committed (not just proposed).
    pub async fn propose(&self, command: CacheCommand) -> Result<ProposalResult> {
        debug!("PROPOSE: Starting proposal, is_leader={}", self.is_leader());

        // Check if accepting proposals (for graceful shutdown)
        if !self.accepting_proposals.load(Ordering::SeqCst) {
            return Err(RaftError::NotReady.into());
        }

        // Check rate limit
        self.flow_control.check_propose_rate().await?;

        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader: self.leader_id(),
            }
            .into());
        }

        // Pre-validate command
        Self::validate_command(&command)?;

        // Generate proposal ID
        let proposal_id = self.next_proposal_id.fetch_add(1, Ordering::SeqCst);
        debug!("PROPOSE: Generated proposal_id={}", proposal_id);

        // Serialize command
        let data = command.to_bytes()?;

        // Create completion channel
        let (tx, rx) = oneshot::channel();

        // Store pending proposal
        {
            let mut pending = self.pending.lock();
            pending.insert(proposal_id, PendingProposal { tx });
        }

        // Propose to Raft
        {
            let mut node = self.node.lock();
            // Use proposal_id as context for tracking
            let context = proposal_id.to_le_bytes().to_vec();
            debug!("PROPOSE: Calling node.propose()");
            if let Err(e) = node.propose(context, data) {
                // Remove pending proposal
                self.pending.lock().remove(&proposal_id);
                debug!("PROPOSE: node.propose() failed: {}", e);
                return Err(RaftError::Internal(e.to_string()).into());
            }
            debug!("PROPOSE: node.propose() succeeded");
        }

        // Wait for commit with timeout to prevent indefinite hangs
        // TC22 fix: Increase timeout to handle backpressure and message reordering scenarios.
        // Use at least 5 seconds or 3x election_timeout, whichever is larger.
        let election_timeout_ms = self.config.election_tick as u64 * self.config.tick_interval_ms;
        let min_timeout_ms = 5000u64; // Minimum 5 seconds for robustness
        let timeout_ms = std::cmp::max(election_timeout_ms * 3, min_timeout_ms);
        let timeout_duration = Duration::from_millis(timeout_ms);
        debug!(
            "PROPOSE: Waiting for commit on proposal_id={} (timeout={}ms)",
            proposal_id, timeout_ms
        );

        tokio::select! {
            result = rx => {
                match result {
                    Ok(res) => {
                        debug!("PROPOSE: Commit received for proposal_id={}", proposal_id);
                        res
                    }
                    Err(_) => {
                        debug!("PROPOSE: Proposal dropped for proposal_id={}", proposal_id);
                        Err(RaftError::ProposalDropped.into())
                    }
                }
            }
            _ = tokio::time::sleep(timeout_duration) => {
                // Timeout: clean up pending mapping to prevent memory leak
                self.pending.lock().remove(&proposal_id);
                warn!("PROPOSE: Timeout waiting for proposal_id={} after {}ms", proposal_id, timeout_ms);
                Err(RaftError::Internal("proposal timeout".to_string()).into())
            }
        }
    }

    /// Propose a configuration change to add or remove a node from the Raft cluster.
    ///
    /// This enables dynamic cluster membership:
    /// - AddNode: Add a new voter to the cluster
    /// - RemoveNode: Remove an existing voter from the cluster
    ///
    /// The change will be replicated through Raft consensus and applied when committed.
    /// Only the leader can propose configuration changes.
    pub async fn propose_conf_change(
        &self,
        change_type: raft::prelude::ConfChangeType,
        node_id: NodeId,
        node_addr: Option<std::net::SocketAddr>,
    ) -> Result<()> {
        debug!(
            "CONF_CHANGE: Proposing {:?} for node {} (addr: {:?})",
            change_type, node_id, node_addr
        );

        // Check if accepting proposals
        if !self.accepting_proposals.load(Ordering::SeqCst) {
            return Err(RaftError::NotReady.into());
        }

        // Only leader can propose conf changes
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader: self.leader_id(),
            }
            .into());
        }

        // Don't add a node that's already a voter
        if change_type == raft::prelude::ConfChangeType::AddNode {
            if self.voters().contains(&node_id) {
                debug!(
                    "CONF_CHANGE: Node {} is already a voter, skipping",
                    node_id
                );
                return Ok(());
            }
        }

        // Don't remove a node that's not a voter
        if change_type == raft::prelude::ConfChangeType::RemoveNode {
            if !self.voters().contains(&node_id) {
                debug!(
                    "CONF_CHANGE: Node {} is not a voter, skipping removal",
                    node_id
                );
                return Ok(());
            }
        }

        // Create ConfChange
        let mut cc = raft::prelude::ConfChange::default();
        cc.set_change_type(change_type);
        cc.set_node_id(node_id);

        // Store node address in context if provided (for transport update)
        let context = if let Some(addr) = node_addr {
            addr.to_string().into_bytes()
        } else {
            vec![]
        };
        cc.set_context(context.into());

        // Propose to Raft
        {
            let mut node = self.node.lock();
            if let Err(e) = node.propose_conf_change(vec![], cc) {
                error!("CONF_CHANGE: Failed to propose: {}", e);
                return Err(RaftError::Internal(e.to_string()).into());
            }
        }

        debug!(
            "CONF_CHANGE: Successfully proposed {:?} for node {}",
            change_type, node_id
        );
        Ok(())
    }

    /// Add a new node to the Raft cluster.
    ///
    /// This is a convenience method that proposes AddNode configuration change.
    /// The node will become a voter after the change is committed.
    pub async fn add_voter(&self, node_id: NodeId, node_addr: std::net::SocketAddr) -> Result<()> {
        // First add to transport so we can communicate
        self.transport.add_peer(node_id, node_addr).await;

        // Then propose the configuration change
        self.propose_conf_change(
            raft::prelude::ConfChangeType::AddNode,
            node_id,
            Some(node_addr),
        )
        .await
    }

    /// Remove a node from the Raft cluster.
    ///
    /// This is a convenience method that proposes RemoveNode configuration change.
    pub async fn remove_voter(&self, node_id: NodeId) -> Result<()> {
        self.propose_conf_change(raft::prelude::ConfChangeType::RemoveNode, node_id, None)
            .await
    }

    /// Validate a command before proposing.
    fn validate_command(command: &CacheCommand) -> Result<()> {
        match command {
            CacheCommand::Put { key, value, .. } => {
                if key.is_empty() {
                    return Err(crate::error::Error::Config(
                        "key cannot be empty".to_string(),
                    ));
                }
                // Check size limits
                if key.len() + value.len() > 16 * 1024 * 1024 {
                    return Err(crate::error::Error::Config(
                        "entry too large (max 16MB)".to_string(),
                    ));
                }
            }
            CacheCommand::Delete { key } => {
                if key.is_empty() {
                    return Err(crate::error::Error::Config(
                        "key cannot be empty".to_string(),
                    ));
                }
            }
            CacheCommand::Clear => {}
            CacheCommand::Get { key } => {
                // Get should not be proposed to Raft (read-only), but validate anyway
                if key.is_empty() {
                    return Err(crate::error::Error::Config(
                        "key cannot be empty".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Handle an incoming Raft message from a peer.
    pub fn step(&self, msg: RaftMessage) -> Result<()> {
        let msg_type = msg.get_msg_type();
        let msg_term = msg.term;
        let msg_from = msg.from;
        let msg_to = msg.to;

        debug!(
            "STEP: node={} received msg type={:?} from={} to={} msg_term={}",
            self.id, msg_type, msg_from, msg_to, msg_term
        );

        // Log current node state before processing
        let (current_term, current_leader) = {
            let node = self.node.lock();
            (node.raft.term, node.raft.leader_id)
        };
        debug!(
            "STEP: node={} current_state: term={}, leader_id={}, is_leader={}",
            self.id,
            current_term,
            current_leader,
            self.is_leader()
        );

        // Check if sender is a known voter
        let is_known_voter = {
            let node = self.node.lock();
            let voters: Vec<u64> = node.raft.prs().conf().voters().ids().iter().collect();
            let is_known = voters.contains(&msg_from);
            if !is_known {
                warn!(
                    "STEP: node={} received message from UNKNOWN sender {}. Known voters: {:?}",
                    self.id, msg_from, voters
                );
            }
            is_known
        };

        // Log vote-related messages for election tracking
        match msg_type {
            raft::prelude::MessageType::MsgRequestVote => {
                debug!(
                    node_id = self.id,
                    from = msg_from,
                    term = msg_term,
                    "ELECTION: Received vote REQUEST from node"
                );
            }
            raft::prelude::MessageType::MsgRequestPreVote => {
                debug!(
                    node_id = self.id,
                    from = msg_from,
                    term = msg_term,
                    "ELECTION: Received pre-vote REQUEST from node"
                );
            }
            raft::prelude::MessageType::MsgRequestVoteResponse => {
                let vote_granted = !msg.reject;
                debug!(
                    node_id = self.id,
                    from = msg_from,
                    term = msg_term,
                    vote_granted = vote_granted,
                    "ELECTION: Received vote RESPONSE"
                );
            }
            raft::prelude::MessageType::MsgRequestPreVoteResponse => {
                let vote_granted = !msg.reject;
                debug!(
                    node_id = self.id,
                    from = msg_from,
                    term = msg_term,
                    vote_granted = vote_granted,
                    "ELECTION: Received pre-vote RESPONSE"
                );
            }
            _ => {}
        }

        // Log if receiving a higher term message (should trigger term update)
        if msg_term > current_term {
            debug!(
                node_id = self.id,
                msg_term = msg_term,
                current_term = current_term,
                "RAFT_STATE: Receiving message with HIGHER term, will update"
            );
        }

        let mut node = self.node.lock();
        let result = node.step(msg);

        // Log state after step
        let term_after = node.raft.term;
        let leader_after = node.raft.leader_id;
        let state_after = node.raft.state;
        drop(node); // Release lock before logging

        if term_after != current_term {
            debug!(
                node_id = self.id,
                before_term = current_term,
                after_term = term_after,
                new_leader = leader_after,
                state = ?state_after,
                "RAFT_STATE: Term changed after processing message"
            );
        } else if msg_term > current_term && !is_known_voter {
            warn!(
                node_id = self.id,
                msg_term = msg_term,
                current_term = term_after,
                sender = msg_from,
                "RAFT_STATE: Term NOT updated - sender is not a known voter"
            );
        }

        result.map_err(|e| {
            error!(node_id = self.id, error = %e, "STEP: Failed to process message");
            RaftError::Internal(e.to_string())
        })?;
        Ok(())
    }

    /// Handle an incoming message (called from network layer).
    pub fn handle_message(&self, msg: Message) -> Option<Message> {
        match msg {
            Message::Raft(wrapper) => {
                match wrapper.to_raft_message() {
                    Ok(raft_msg) => {
                        if let Err(e) = self.step(raft_msg) {
                            warn!(error = %e, "Failed to step Raft message");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to decode Raft message");
                    }
                }
                None
            }
            Message::Ping(_ping) => {
                Some(Message::Pong(crate::network::rpc::PongResponse {
                    node_id: self.id,
                    raft_addr: String::new(), // TODO: fill in
                    leader_id: self.leader_id(),
                }))
            }
            Message::ForwardedCommand(fwd) => {
                self.handle_forwarded_command(fwd);
                None // Response sent asynchronously
            }
            _ => None,
        }
    }

    /// Handle a forwarded command from a follower.
    ///
    /// This is called on the leader when it receives a ForwardedCommand.
    /// The leader proposes the command to Raft and sends back a ForwardResponse.
    fn handle_forwarded_command(&self, fwd: crate::network::rpc::ForwardedCommand) {
        use crate::network::rpc::ForwardResponse;

        let request_id = fwd.request_id;
        let origin_node_id = fwd.origin_node_id;
        let command = fwd.command;
        let ttl = fwd.ttl;

        debug!(
            node_id = self.id,
            request_id = request_id,
            origin = origin_node_id,
            ttl = ttl,
            "FORWARD: Received ForwardedCommand from follower"
        );

        // TTL check to prevent infinite forwarding loops
        if ttl == 0 {
            warn!(
                node_id = self.id,
                request_id = request_id,
                "FORWARD: TTL expired, rejecting"
            );
            let response = Message::ForwardResponse(ForwardResponse::error(
                request_id,
                "TTL expired",
            ));
            let transport = self.transport.clone();
            tokio::spawn(async move {
                if let Err(e) = transport.send_message(origin_node_id, response).await {
                    warn!(error = %e, "Failed to send ForwardResponse (TTL expired)");
                }
            });
            return;
        }

        // Check if we're the leader
        if !self.is_leader() {
            warn!(
                node_id = self.id,
                request_id = request_id,
                leader = ?self.leader_id(),
                "FORWARD: Not leader, rejecting"
            );
            let response = Message::ForwardResponse(ForwardResponse::error(
                request_id,
                format!("not leader, leader is {:?}", self.leader_id()),
            ));
            let transport = self.transport.clone();
            tokio::spawn(async move {
                if let Err(e) = transport.send_message(origin_node_id, response).await {
                    warn!(error = %e, "Failed to send ForwardResponse (not leader)");
                }
            });
            return;
        }

        // Propose the command asynchronously
        let transport = self.transport.clone();
        let pending = self.pending.clone();
        let next_proposal_id = self.next_proposal_id.clone();
        let node_id = self.id;
        let node_lock = self.node.clone();
        let config = self.config.clone();

        // Spawn task to handle the proposal
        tokio::spawn(async move {
            // Pre-validate command
            if let Err(e) = Self::validate_command(&command) {
                let response = Message::ForwardResponse(ForwardResponse::error(
                    request_id,
                    e.to_string(),
                ));
                if let Err(e) = transport.send_message(origin_node_id, response).await {
                    warn!(error = %e, "Failed to send ForwardResponse (validation failed)");
                }
                return;
            }

            // Serialize command
            let data = match command.to_bytes() {
                Ok(d) => d,
                Err(e) => {
                    let response = Message::ForwardResponse(ForwardResponse::error(
                        request_id,
                        e.to_string(),
                    ));
                    if let Err(e) = transport.send_message(origin_node_id, response).await {
                        warn!(error = %e, "Failed to send ForwardResponse (serialization failed)");
                    }
                    return;
                }
            };

            // Generate proposal ID
            let proposal_id = next_proposal_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            // Create completion channel
            let (tx, rx) = tokio::sync::oneshot::channel();

            // Store pending proposal
            pending.lock().insert(proposal_id, PendingProposal { tx });

            // Propose to Raft
            {
                let mut node = node_lock.lock();
                let context = proposal_id.to_le_bytes().to_vec();
                if let Err(e) = node.propose(context, data) {
                    pending.lock().remove(&proposal_id);
                    let response = Message::ForwardResponse(ForwardResponse::error(
                        request_id,
                        e.to_string(),
                    ));
                    if let Err(e) = transport.send_message(origin_node_id, response).await {
                        warn!(error = %e, "Failed to send ForwardResponse (propose failed)");
                    }
                    return;
                }
            }

            // Wait for commit with timeout
            let election_timeout_ms = config.election_tick as u64 * config.tick_interval_ms;
            let min_timeout_ms = 5000u64;
            let timeout_ms = std::cmp::max(election_timeout_ms * 3, min_timeout_ms);
            let timeout_duration = std::time::Duration::from_millis(timeout_ms);

            let result = tokio::select! {
                result = rx => {
                    match result {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(e)) => Err(e.to_string()),
                        Err(_) => Err("proposal dropped".to_string()),
                    }
                }
                _ = tokio::time::sleep(timeout_duration) => {
                    pending.lock().remove(&proposal_id);
                    Err("proposal timeout".to_string())
                }
            };

            // Send response back to origin
            let response = match result {
                Ok(()) => {
                    debug!(
                        node_id = node_id,
                        request_id = request_id,
                        origin = origin_node_id,
                        "FORWARD: Command committed successfully"
                    );
                    Message::ForwardResponse(ForwardResponse::success(request_id))
                }
                Err(e) => {
                    warn!(
                        node_id = node_id,
                        request_id = request_id,
                        origin = origin_node_id,
                        error = %e,
                        "FORWARD: Command failed"
                    );
                    Message::ForwardResponse(ForwardResponse::error(request_id, e))
                }
            };

            if let Err(e) = transport.send_message(origin_node_id, response).await {
                warn!(error = %e, "Failed to send ForwardResponse");
            }
        });
    }

    /// Tick the Raft node (called periodically).
    pub fn tick(&self) {
        let mut node = self.node.lock();
        let before_term = node.raft.term;
        let before_state = node.raft.state;
        let before_msg_count = node.raft.msgs.len();

        node.tick();

        let after_term = node.raft.term;
        let after_state = node.raft.state;
        let after_msg_count = node.raft.msgs.len();

        // Log term or state changes with detailed role information
        if after_term != before_term || after_state != before_state {
            let state_name = match after_state {
                raft::StateRole::Leader => "LEADER",
                raft::StateRole::Follower => "FOLLOWER",
                raft::StateRole::Candidate => "CANDIDATE",
                raft::StateRole::PreCandidate => "PRE_CANDIDATE",
            };
            debug!(
                node_id = self.id,
                before_term = before_term,
                after_term = after_term,
                before_state = ?before_state,
                after_state = state_name,
                "RAFT_STATE: Node role/term changed"
            );

            // Extra log for becoming candidate (starting election)
            if after_state == raft::StateRole::Candidate
                || after_state == raft::StateRole::PreCandidate
            {
                debug!(
                    node_id = self.id,
                    term = after_term,
                    "ELECTION: Starting election as {:?}",
                    after_state
                );
            }
        }

        if after_msg_count > before_msg_count {
            debug!(
                node_id = self.id,
                msg_count = after_msg_count - before_msg_count,
                term = after_term,
                state = ?after_state,
                "TICK: Generated outbound messages"
            );
        }
    }

    /// Process ready state (called after tick or message handling).
    ///
    /// CRITICAL FIX: Ensures atomicity of ready -> advance cycle to prevent
    /// "not leader but has new msg after advance" panic.
    ///
    /// Key principles:
    /// 1. Minimize lock holding time but maintain logical continuity
    /// 2. Complete all persistence BEFORE sending any messages
    /// 3. Call advance() and advance_apply() with lock held for atomicity
    /// 4. Execute all async operations (like apply_entry) OUTSIDE of locks
    ///
    /// PERFORMANCE WARNING: apply_entry() is called with .await in the tick loop.
    /// If the state machine operations are slow (disk I/O, network calls), this will
    /// delay the entire tick cycle and may cause election timeouts. For high-throughput
    /// scenarios, consider moving apply operations to a separate async task queue.
    ///
    /// Order of operations follows raft-rs guidelines:
    /// 1. Get ready with minimal lock time
    /// 2. Persist entries to stable storage (MUST complete before sending messages)
    /// 3. Persist hard state
    /// 4. Apply snapshot if any
    /// 5. Send messages
    /// 6. Apply committed entries to state machine (outside of lock)
    /// 7. Call advance() with lock held to ensure atomicity
    /// 8. Process light_ready messages and entries (outside of lock)
    ///    - advance_apply() must be called BEFORE releasing lock
    ///
    /// CRITICAL FIX: To prevent "not leader but has new msg after advance" panic,
    /// we must hold the lock from ready() through advance(). The panic occurs when
    /// step() changes leadership status between ready() and advance().
    pub async fn process_ready(&self) {
        // Report unreachable peers to Raft before processing ready
        let unreachable_peers = self.flow_control.unreachable_peers();
        if !unreachable_peers.is_empty() {
            let mut node = self.node.lock();
            for peer_id in unreachable_peers {
                node.report_unreachable(peer_id);
                debug!(
                    self.id,
                    peer_id, "Reported peer as unreachable due to backpressure"
                );
            }
        }

        // CRITICAL: Hold the lock throughout ready -> persist -> advance cycle
        // to prevent race condition with step() that causes "not leader but has new msg" panic.
        //
        // We extract data to process outside the lock, but keep the lock held during:
        // 1. ready() - get the Ready struct
        // 2. persist entries and hard state (storage is thread-safe)
        // 3. advance() - complete the ready cycle
        //
        // This ensures step() cannot change leadership between ready() and advance().

        let (
            messages_to_send,
            persisted_messages_to_send,
            committed_entries_to_apply,
            light_ready_messages,
            light_ready_entries,
            new_leader,
            new_term,
            snapshot_data_to_apply,
        ) = {
            let mut node = self.node.lock();

            if !node.has_ready() {
                return;
            }

            let mut ready = node.ready();
            debug!(
                "PROCESS_READY: node={} entries={}, committed={}, has_hs={}, has_ss={}, msg_count={}, persisted_msg_count={}",
                self.id,
                ready.entries().len(),
                ready.committed_entries().len(),
                ready.hs().is_some(),
                ready.ss().is_some(),
                ready.messages().len(),
                ready.persisted_messages().len()
            );

            // 1. Persist entries FIRST (must complete before sending messages)
            // Storage uses internal locking, safe to call while holding node lock
            let entries = ready.entries();
            if !entries.is_empty() {
                debug!(
                    node_id = self.id,
                    "PROCESS_READY: Persisting {} entries",
                    entries.len()
                );
                if let Err(e) = self.storage.append(entries) {
                    error!(error = %e, "CRITICAL: Failed to append entries, aborting ready processing");
                    return;
                }
            }

            // 2. Update hard state if changed
            if let Some(hs) = ready.hs() {
                debug!(node_id = self.id, "PROCESS_READY: Persisting hard state");
                if let Err(e) = self.storage.set_hard_state(hs.clone()) {
                    error!(error = %e, "CRITICAL: Failed to persist hard state, aborting ready processing");
                    return;
                }
            }

            // 3. Handle snapshot if any - extract data for async processing outside lock
            let snapshot_data = if !ready.snapshot().is_empty() {
                let snapshot = ready.snapshot().clone();
                let index = snapshot.get_metadata().index;
                let term = snapshot.get_metadata().term;
                let data = snapshot.data.clone();

                debug!(
                    node_id = self.id,
                    index,
                    term,
                    data_len = data.len(),
                    "PROCESS_READY: Applying snapshot to Raft storage"
                );

                if let Err(e) = self.storage.apply_snapshot(snapshot) {
                    error!(error = %e, "CRITICAL: Failed to apply snapshot, aborting ready processing");
                    return;
                }

                // Return snapshot data for state machine restoration outside lock
                if !data.is_empty() && is_valid_snapshot_data(&data) {
                    Some((index, term, data))
                } else {
                    None
                }
            } else {
                None
            };

            // 4. Extract messages to send (will send outside lock)
            let messages = ready.take_messages();
            debug!(
                node_id = self.id,
                "PROCESS_READY: Sending {} take messages",
                messages.len()
            );

            let persisted_messages = ready.take_persisted_messages();
            if !persisted_messages.is_empty() {
                debug!(
                    node_id = self.id,
                    "PROCESS_READY: Sending {} persisted messages",
                    persisted_messages.len()
                );
            }

            // 5. Extract committed entries (will apply outside lock)
            let committed_entries = ready.take_committed_entries();
            if !committed_entries.is_empty() {
                debug!(
                    node_id = self.id,
                    "PROCESS_READY: Processing {} committed entries",
                    committed_entries.len()
                );
            }

            // 6. CRITICAL: Call advance() while still holding the lock
            // This prevents the "not leader but has new msg after advance" panic
            let mut light_ready = node.advance(ready);

            // Extract light_ready data
            let lr_messages = light_ready.take_messages();
            let lr_entries = light_ready.take_committed_entries();

            // Must call advance_apply before releasing the lock
            node.advance_apply();

            // Get leader info for tracking
            let leader = node.raft.leader_id;
            let term = node.raft.term;

            (
                messages,
                persisted_messages,
                committed_entries,
                lr_messages,
                lr_entries,
                leader,
                term,
                snapshot_data,
            )
        };
        // Lock released here - now safe for step() to run

        // 7. Apply snapshot data to state machine (outside of lock, async)
        if let Some((index, term, data)) = snapshot_data_to_apply {
            debug!(
                node_id = self.id,
                index,
                term,
                data_len = data.len(),
                "PROCESS_READY: Restoring state machine from snapshot"
            );

            match deserialize_snapshot_data(&data) {
                Ok(entries) => {
                    let total_entries = entries.len();

                    // Clear current cache and restore from snapshot
                    self.state_machine.storage().invalidate_all();

                    // Insert entries from snapshot, filtering expired ones
                    let mut inserted_count = 0;
                    let mut expired_count = 0;

                    for entry in entries {
                        // Skip expired entries
                        if entry.is_expired() {
                            expired_count += 1;
                            debug!(
                                node_id = self.id,
                                key_len = entry.key.len(),
                                expires_at_ms = ?entry.expires_at_ms,
                                "PROCESS_READY: Skipping expired entry from snapshot"
                            );
                            continue;
                        }

                        // Insert with expiration if present, otherwise just insert
                        if let Some(expires_at_ms) = entry.expires_at_ms {
                            self.state_machine
                                .storage()
                                .insert_with_expiration(entry.key, entry.value, expires_at_ms)
                                .await;
                        } else {
                            self.state_machine
                                .storage()
                                .insert(entry.key, entry.value)
                                .await;
                        }
                        inserted_count += 1;
                    }

                    // Update state machine applied index/term
                    // Note: The state machine will skip applying entries <= this index
                    self.state_machine.storage().run_pending_tasks().await;

                    debug!(
                        node_id = self.id,
                        index,
                        term,
                        total_entries,
                        inserted_count,
                        expired_count,
                        "PROCESS_READY: State machine restored from snapshot"
                    );
                }
                Err(e) => {
                    error!(
                        node_id = self.id,
                        error = %e,
                        "PROCESS_READY: Failed to deserialize snapshot data"
                    );
                }
            }
        }

        // 8. Send messages (outside of lock)
        if !messages_to_send.is_empty() {
            debug!(
                node_id = self.id,
                "PROCESS_READY: Sending {} immediate messages",
                messages_to_send.len()
            );
            self.transport.send_messages(messages_to_send);
        }

        if !persisted_messages_to_send.is_empty() {
            self.transport.send_messages(persisted_messages_to_send);
        }

        // 9. Update leader tracking
        let old_leader = self.leader_id.load(Ordering::Relaxed);
        let had_leader = self.has_leader.load(Ordering::Relaxed);

        if new_leader != raft::INVALID_ID {
            if new_leader != old_leader || !had_leader {
                if new_leader == self.id {
                    debug!(
                        node_id = self.id,
                        term = new_term,
                        previous_leader = old_leader,
                        "LEADER_ELECTION: This node is now the LEADER"
                    );
                } else {
                    debug!(
                        node_id = self.id,
                        new_leader_id = new_leader,
                        term = new_term,
                        previous_leader = old_leader,
                        "LEADER_ELECTION: New leader detected, this node is FOLLOWER"
                    );
                }
            }
            self.leader_id.store(new_leader, Ordering::Relaxed);
            self.has_leader.store(true, Ordering::Relaxed);
        } else if had_leader {
            debug!(
                node_id = self.id,
                previous_leader = old_leader,
                term = new_term,
                "LEADER_ELECTION: No leader currently (election in progress)"
            );
            self.has_leader.store(false, Ordering::Relaxed);
        }

        // 10. Apply committed entries to state machine (outside of lock)
        for entry in committed_entries_to_apply {
            self.apply_entry(&entry).await;
        }

        // 11. Process light_ready messages (outside of lock)
        if !light_ready_messages.is_empty() {
            debug!(
                node_id = self.id,
                "PROCESS_READY: Sending {} light_ready messages",
                light_ready_messages.len()
            );
            self.transport.send_messages(light_ready_messages);
        }

        // 12. Apply light_ready committed entries
        if !light_ready_entries.is_empty() {
            debug!(
                node_id = self.id,
                "PROCESS_READY: Processing {} light_ready committed entries",
                light_ready_entries.len()
            );
        }
        for entry in light_ready_entries {
            self.apply_entry(&entry).await;
        }
    }

    /// Apply a single committed entry to the state machine.
    /// This is extracted to avoid code duplication and ensure consistent handling.
    ///
    /// IMPORTANT: Raft generates a no-op entry when a new leader is elected.
    /// These entries have empty data and must be skipped to avoid parse errors.
    async fn apply_entry(&self, entry: &raft::prelude::Entry) {
        debug!(
            node_id = self.id,
            "APPLY_ENTRY: index={}, term={}, data_len={}, context_len={}, entry_type={:?}",
            entry.index,
            entry.term,
            entry.data.len(),
            entry.context.len(),
            entry.get_entry_type()
        );

        // Handle noop entries (empty data) - these are generated during leader election
        // IMPORTANT: We must still update the state machine's applied_index even for
        // noop entries, otherwise applied_index becomes stale and triggers issues with:
        // - Checkpoint threshold calculations
        // - Snapshot creation timing
        // - Follower catch-up detection
        if entry.data.is_empty() {
            debug!(
                node_id = self.id,
                "APPLY_ENTRY: Processing noop entry at index={}", entry.index
            );
            // State machine handles empty data correctly - it updates applied_index
            self.state_machine.apply(entry.index, entry.term, &[]).await;
            return;
        }

        // Handle ConfChange entries (membership changes)
        if entry.get_entry_type() == EntryType::EntryConfChange {
            self.apply_conf_change(entry).await;
            return;
        }

        // Handle normal entries
        self.apply_normal_entry(entry).await;
    }

    /// Apply a configuration change entry (membership change).
    async fn apply_conf_change(&self, entry: &raft::prelude::Entry) {
        debug!(
            node_id = self.id,
            "APPLY_CONF_CHANGE: Processing conf change at index={}", entry.index
        );

        // Parse the ConfChange from entry data
        let cc = match ConfChange::parse_from_bytes(&entry.data) {
            Ok(cc) => cc,
            Err(e) => {
                error!(
                    node_id = self.id,
                    "APPLY_CONF_CHANGE: Failed to decode ConfChange: {}", e
                );
                return;
            }
        };

        debug!(
            node_id = self.id,
            "APPLY_CONF_CHANGE: ConfChange type={:?}, node_id={}, context_len={}",
            cc.get_change_type(),
            cc.node_id,
            cc.context.len()
        );

        // Extract peer address from context if present
        let peer_addr: Option<std::net::SocketAddr> = if !cc.context.is_empty() {
            String::from_utf8(cc.context.to_vec())
                .ok()
                .and_then(|s| s.parse().ok())
        } else {
            None
        };

        // Apply the conf change to the raft node
        let conf_state = {
            let mut node = self.node.lock();
            let conf_state = node.apply_conf_change(&cc).expect("apply_conf_change failed");
            debug!(
                node_id = self.id,
                "APPLY_CONF_CHANGE: New conf_state - voters={:?}, learners={:?}",
                conf_state.voters,
                conf_state.learners
            );
            conf_state
        };

        // Update transport with new peer address if this is an add operation
        if cc.get_change_type() == raft::prelude::ConfChangeType::AddNode
            || cc.get_change_type() == raft::prelude::ConfChangeType::AddLearnerNode
        {
            if let Some(addr) = peer_addr {
                debug!(
                    node_id = self.id,
                    "APPLY_CONF_CHANGE: Adding peer {} with address {} to transport",
                    cc.node_id,
                    addr
                );
                self.transport.add_peer(cc.node_id, addr).await;
            }
        } else if cc.get_change_type() == raft::prelude::ConfChangeType::RemoveNode {
            debug!(
                node_id = self.id,
                "APPLY_CONF_CHANGE: Removing peer {} from transport",
                cc.node_id
            );
            self.transport.remove_peer(cc.node_id);
        }

        // Update storage with new ConfState
        if let Err(e) = self.storage.set_conf_state(conf_state) {
            error!(
                node_id = self.id,
                error = %e,
                "APPLY_CONF_CHANGE: Failed to persist conf state"
            );
        }

        // Track entry for checkpoint threshold
        self.record_checkpoint_entry();
    }

    /// Apply a normal (cache command) entry.
    async fn apply_normal_entry(&self, entry: &raft::prelude::Entry) {
        // Extract proposal ID from context
        let proposal_id = if entry.context.len() >= 8 {
            let bytes: [u8; 8] = entry.context[..8].try_into().unwrap_or([0; 8]);
            u64::from_le_bytes(bytes)
        } else {
            0
        };
        debug!("APPLY_ENTRY: Entry has proposal_id={}", proposal_id);

        // Apply to state machine
        self.state_machine
            .apply(entry.index, entry.term, &entry.data)
            .await;

        // Track entry for checkpoint threshold
        self.record_checkpoint_entry();

        // Notify pending proposal if exists
        if proposal_id > 0 {
            if let Some(pending) = self.pending.lock().remove(&proposal_id) {
                debug!(
                    node_id = self.id,
                    "APPLY_ENTRY: Notifying proposal_id={}", proposal_id
                );
                let _ = pending.tx.send(Ok(ProposalResult {
                    index: entry.index,
                    term: entry.term,
                }));
            } else {
                debug!(
                    node_id = self.id,
                    "APPLY_ENTRY: proposal_id={} not found in pending (possibly from another node)",
                    proposal_id
                );
            }
        }
    }

    /// Run the Raft tick loop.
    pub async fn run_tick_loop(self: Arc<Self>, mut shutdown_rx: mpsc::Receiver<()>) {
        let tick_interval = Duration::from_millis(self.config.tick_interval_ms);
        let mut interval = tokio::time::interval(tick_interval);
        let mut tick_count = 0u64;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    tick_count += 1;
                    // debug!("TICK_LOOP: node={} tick #{}, term={}, is_leader={}",
                    //     self.id, tick_count, self.term(), self.is_leader());
                    self.tick();
                    self.process_ready().await;
                    // 关键：强制 yield，让异步发送任务有机会运行
                    tokio::task::yield_now().await;
                }
                _ = shutdown_rx.recv() => {
                    debug!("Raft tick loop shutting down");
                    break;
                }
            }
        }
    }

    // =========================================================================
    // Flow Control and Health Monitoring
    // =========================================================================

    /// Get the flow control instance.
    pub fn flow_control(&self) -> &Arc<FlowControl> {
        &self.flow_control
    }

    /// Check if a peer is marked as unreachable due to backpressure.
    pub fn is_peer_unreachable(&self, peer_id: NodeId) -> bool {
        self.flow_control.is_unreachable(peer_id)
    }

    /// Get the current health status of this node.
    pub fn health_check(&self) -> HealthStatus {
        let metrics = self.transport.metrics();
        self.health_checker.check(
            &metrics,
            self.has_leader.load(Ordering::Relaxed),
            self.commit_index(),
            self.applied_index(),
        )
    }

    /// Get a detailed health report for this node.
    pub fn health_report(&self) -> HealthReport {
        let metrics = self.transport.metrics();
        self.health_checker.generate_report(
            self.id,
            self.is_leader(),
            self.leader_id(),
            self.term(),
            self.commit_index(),
            self.applied_index(),
            metrics,
            self.has_leader.load(Ordering::Relaxed),
        )
    }

    // =========================================================================
    // Graceful Shutdown
    // =========================================================================

    /// Gracefully shutdown this Raft node.
    ///
    /// This method:
    /// 1. Stops accepting new proposals
    /// 2. Waits for pending proposals to complete (with timeout)
    /// 3. Processes final ready state
    /// 4. Shuts down transport
    /// 5. Optionally creates a final snapshot
    pub async fn shutdown(self: Arc<Self>) -> Result<()> {
        debug!(node_id = self.id, "Starting graceful shutdown");

        // Step 1: Stop accepting new proposals
        self.accepting_proposals.store(false, Ordering::SeqCst);
        debug!(node_id = self.id, "Stopped accepting new proposals");

        // Step 2: Wait for pending proposals (with timeout)
        let timeout = Duration::from_secs(5);
        let deadline = Instant::now() + timeout;

        while !self.pending.lock().is_empty() {
            if Instant::now() > deadline {
                let remaining = self.pending.lock().len();
                warn!(
                    node_id = self.id,
                    remaining, "Timeout waiting for pending proposals, proceeding with shutdown"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        debug!(
            node_id = self.id,
            "All pending proposals completed or timed out"
        );

        // Step 3: Final ready processing
        self.process_ready().await;
        debug!(node_id = self.id, "Final ready state processed");

        // Step 4: Shutdown transport
        self.transport.shutdown().await;
        debug!(node_id = self.id, "Transport shutdown complete");

        // Step 5: Optionally create final snapshot
        if let Some(checkpoint_mgr) = self.checkpoint_manager.read().as_ref() {
            if self.should_create_snapshot() {
                match checkpoint_mgr
                    .create_snapshot(self.applied_index(), self.term())
                    .await
                {
                    Ok(metadata) => {
                        debug!(
                            node_id = self.id,
                            index = metadata.raft_index,
                            "Created shutdown snapshot"
                        );
                    }
                    Err(e) => {
                        warn!(
                            node_id = self.id,
                            error = %e,
                            "Failed to create shutdown snapshot"
                        );
                    }
                }
            }
        }

        debug!(node_id = self.id, "Node shutdown complete");
        Ok(())
    }

    /// Check if the node is accepting proposals.
    pub fn is_accepting_proposals(&self) -> bool {
        self.accepting_proposals.load(Ordering::SeqCst)
    }

    /// Stop accepting new proposals without full shutdown.
    pub fn stop_accepting_proposals(&self) {
        self.accepting_proposals.store(false, Ordering::SeqCst);
        debug!(node_id = self.id, "Stopped accepting proposals");
    }

    /// Resume accepting proposals.
    pub fn resume_accepting_proposals(&self) {
        self.accepting_proposals.store(true, Ordering::SeqCst);
        debug!(node_id = self.id, "Resumed accepting proposals");
    }

    /// Check if there are any pending proposals waiting to be committed.
    pub fn has_pending_proposals(&self) -> bool {
        !self.pending.lock().is_empty()
    }

    /// Get the number of pending proposals.
    pub fn pending_proposal_count(&self) -> usize {
        self.pending.lock().len()
    }
}

impl std::fmt::Debug for RaftNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftNode")
            .field("id", &self.id)
            .field("leader_id", &self.leader_id())
            .field("is_leader", &self.is_leader())
            .field("term", &self.term())
            .finish()
    }
}

/// Implement RaftStateProvider for Arc<RaftNode> to allow checkpoint manager
/// to query the current applied state.
impl RaftStateProvider for Arc<RaftNode> {
    fn get_applied_state(&self) -> (u64, u64) {
        (self.applied_index(), self.term())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::storage::CacheStorage;
    use crate::config::CacheConfig;

    fn create_state_machine() -> Arc<CacheStateMachine> {
        let config = CacheConfig::default();
        let storage = Arc::new(CacheStorage::new(&config));
        Arc::new(CacheStateMachine::new(storage))
    }

    #[test]
    fn test_validate_empty_key() {
        let cmd = CacheCommand::put(vec![], b"value".to_vec());
        assert!(RaftNode::validate_command(&cmd).is_err());
    }

    #[test]
    fn test_validate_valid_command() {
        let cmd = CacheCommand::put(b"key".to_vec(), b"value".to_vec());
        assert!(RaftNode::validate_command(&cmd).is_ok());
    }
}
