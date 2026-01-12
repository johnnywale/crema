//! Raft node wrapper that drives the consensus algorithm.

use crate::checkpoint::{CheckpointConfig, CheckpointManager, RaftStateProvider, SnapshotMetadata};
use crate::config::RaftConfig;
use crate::consensus::state_machine::CacheStateMachine;
use crate::consensus::storage::MemStorage;
use crate::consensus::transport::RaftTransport;
use crate::error::{RaftError, Result};
use crate::network::rpc::Message;
use crate::types::{CacheCommand, NodeId, ProposalResult};
use parking_lot::{Mutex, RwLock};
use raft::prelude::Message as RaftMessage;
use raft::RawNode;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
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
    node: Mutex<RawNode<MemStorage>>,

    /// Raft storage (shared reference for access outside RawNode).
    storage: MemStorage,

    /// State machine for applying commands.
    state_machine: Arc<CacheStateMachine>,

    /// Transport for sending messages.
    transport: Arc<RaftTransport>,

    /// Pending proposals indexed by proposal ID.
    pending: Mutex<HashMap<u64, PendingProposal>>,

    /// Next proposal ID.
    next_proposal_id: AtomicU64,

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

        // Create a SINGLE storage instance that will be shared with RawNode
        // IMPORTANT: RawNode needs to see our storage updates, so we must use the same instance
        let storage = MemStorage::new_with_conf_state(voters.clone());

        eprintln!("RAFT_NODE: Creating node {} with voters {:?}", id, voters);

        // Create raft config
        let raft_config = config.to_raft_config(id);

        // Create a logger (using slog)
        let logger = slog::Logger::root(slog::Discard, slog::o!());

        // Create the raw node with a clone of our storage (MemStorage uses Arc internally)
        let node = RawNode::new(&raft_config, storage.clone(), &logger)
            .map_err(|e| RaftError::Internal(e.to_string()))?;

        // Debug: print what raft-rs sees as voters
        let prs_voters: Vec<_> = node.raft.prs().conf().voters().ids().iter().collect();
        eprintln!("RAFT_NODE: node {} ProgressTracker voters: {:?}", id, prs_voters);

        // Create transport
        let transport = Arc::new(RaftTransport::new(id));

        let raft_node = Arc::new(Self {
            node: Mutex::new(node),
            storage,
            state_machine,
            transport,
            pending: Mutex::new(HashMap::new()),
            next_proposal_id: AtomicU64::new(1),
            id,
            leader_id: AtomicU64::new(0),
            has_leader: AtomicBool::new(false),
            config,
            checkpoint_manager: RwLock::new(None),
        });

        Ok(raft_node)
    }

    /// Initialize checkpoint manager for this node.
    ///
    /// This should be called after creating the node if checkpointing is desired.
    pub fn init_checkpoint_manager(
        &self,
        config: CheckpointConfig,
    ) -> std::result::Result<Arc<CheckpointManager>, crate::checkpoint::FormatError> {
        let manager =
            Arc::new(CheckpointManager::new(config, self.state_machine.storage().clone())?);
        *self.checkpoint_manager.write() = Some(manager.clone());
        Ok(manager)
    }

    /// Get the checkpoint manager if configured.
    pub fn checkpoint_manager(&self) -> Option<Arc<CheckpointManager>> {
        self.checkpoint_manager.read().clone()
    }

    /// Create a snapshot of the current state.
    ///
    /// This should only be called on the leader.
    pub async fn create_snapshot(&self) -> Result<SnapshotMetadata> {
        let manager = self
            .checkpoint_manager
            .read()
            .clone()
            .ok_or_else(|| RaftError::Internal("checkpoint manager not configured".to_string()))?;

        let (index, term) = (self.applied_index(), self.term());
        manager
            .create_snapshot(index, term)
            .await
            .map_err(|e| RaftError::Internal(e.to_string()).into())
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
                info!(
                    path = %info.path.display(),
                    raft_index = info.raft_index,
                    raft_term = info.raft_term,
                    "Loading snapshot for recovery"
                );
                let index = manager
                    .load_snapshot(&info.path)
                    .await
                    .map_err(|e| RaftError::Internal(e.to_string()))?;
                Ok(Some(index))
            }
            None => {
                debug!("No snapshot found for recovery");
                Ok(None)
            }
        }
    }

    /// Record an entry for checkpoint threshold tracking.
    pub fn record_checkpoint_entry(&self) {
        if let Some(manager) = self.checkpoint_manager.read().as_ref() {
            manager.record_entry();
        }
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

    /// Propose a command to the Raft cluster.
    ///
    /// This will return when the command is committed (not just proposed).
    pub async fn propose(&self, command: CacheCommand) -> Result<ProposalResult> {
        eprintln!("PROPOSE: Starting proposal, is_leader={}", self.is_leader());
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
        eprintln!("PROPOSE: Generated proposal_id={}", proposal_id);

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
            eprintln!("PROPOSE: Calling node.propose()");
            if let Err(e) = node.propose(context, data) {
                // Remove pending proposal
                self.pending.lock().remove(&proposal_id);
                eprintln!("PROPOSE: node.propose() failed: {}", e);
                return Err(RaftError::Internal(e.to_string()).into());
            }
            eprintln!("PROPOSE: node.propose() succeeded");
        }

        // Wait for commit
        eprintln!("PROPOSE: Waiting for commit on proposal_id={}", proposal_id);
        match rx.await {
            Ok(result) => {
                eprintln!("PROPOSE: Commit received for proposal_id={}", proposal_id);
                result
            }
            Err(_) => {
                eprintln!("PROPOSE: Proposal dropped for proposal_id={}", proposal_id);
                Err(RaftError::ProposalDropped.into())
            }
        }
    }

    /// Validate a command before proposing.
    fn validate_command(command: &CacheCommand) -> Result<()> {
        match command {
            CacheCommand::Put { key, value, .. } => {
                if key.is_empty() {
                    return Err(crate::error::Error::Config("key cannot be empty".to_string()));
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
                    return Err(crate::error::Error::Config("key cannot be empty".to_string()));
                }
            }
            CacheCommand::Clear => {}
        }
        Ok(())
    }

    /// Handle an incoming Raft message from a peer.
    pub fn step(&self, msg: RaftMessage) -> Result<()> {
        eprintln!("STEP: node={} received msg type={:?} from={} to={}", self.id, msg.msg_type, msg.from, msg.to);

        if msg.get_msg_type() == raft::prelude::MessageType::MsgRequestVoteResponse
            || msg.get_msg_type() == raft::prelude::MessageType::MsgRequestPreVoteResponse {
            eprintln!("STEP: Node {} got vote response from {}, rejected={}",
                      self.id, msg.from, msg.reject);
        }

        let mut node = self.node.lock();
        node.step(msg)
            .map_err(|e| RaftError::Internal(e.to_string()))?;
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
            _ => None,
        }
    }

    /// Tick the Raft node (called periodically).
    pub fn tick(&self) {
        let mut node = self.node.lock();
        let before_msg_count = node.raft.msgs.len();
        node.tick();
        let after_msg_count = node.raft.msgs.len();
        if after_msg_count > before_msg_count {
            eprintln!("TICK: node={} generated {} messages", self.id, after_msg_count - before_msg_count);
        }
    }

    /// Process ready state (called after tick or message handling).
    ///
    /// Order of operations follows raft-rs guidelines:
    /// 1. Persist entries to stable storage
    /// 2. Persist hard state
    /// 3. Apply snapshot if any
    /// 4. Send messages
    /// 5. Apply committed entries to state machine
    /// 6. Call advance()
    pub async fn process_ready(&self) {
        let mut ready = {
            let mut node = self.node.lock();
            // Check message count BEFORE has_ready
            let msgs_before = node.raft.msgs.len();
            if !node.has_ready() {
                return;
            }
            // Check message count AFTER has_ready
            let msgs_after_check = node.raft.msgs.len();
            let raft_state = node.raft.state;
            let leader = node.raft.leader_id;
            eprintln!("PROCESS_READY: node={} has ready, state={:?}, leader={}, msgs_before={}, msgs_after_check={}",
                self.id, raft_state, leader, msgs_before, msgs_after_check);
            let ready = node.ready();
            // Check messages after ready
            let msgs_after_ready = node.raft.msgs.len();
            eprintln!("PROCESS_READY: node={} msgs_after_ready={}", self.id, msgs_after_ready);
            ready
        };

        // Debug: print what's in ready
        eprintln!("PROCESS_READY: node={} entries={}, committed={}, has_hs={}, has_ss={}, msg_count={}, persisted_msg_count={}",
            self.id,
            ready.entries().len(),
            ready.committed_entries().len(),
            ready.hs().is_some(),
            ready.ss().is_some(),
            ready.messages().len(),
            ready.persisted_messages().len()
        );

        // 1. Append entries to storage FIRST (must persist before commit)
        let entries: Vec<_> = ready.take_entries();
        if !entries.is_empty() {
            eprintln!("READY: Appending {} entries to storage", entries.len());
            if let Err(e) = self.storage.append(&entries) {
                error!(error = %e, "Failed to append entries");
            }
        }

        // 2. Update hard state if changed
        if let Some(hs) = ready.hs() {
            self.storage.set_hard_state(hs.clone());
        }

        // 3. Handle snapshot if any
        if !ready.snapshot().is_empty() {
            if let Err(e) = self.storage.apply_snapshot(ready.snapshot().clone()) {
                error!(error = %e, "Failed to apply snapshot");
            }
        }

        // 4. Send messages (both immediate and persisted messages)
        let messages: Vec<_> = ready.take_messages();
        if !messages.is_empty() {
            eprintln!("PROCESS_READY: node={} sending {} immediate messages", self.id, messages.len());
        }
        self.transport.send_messages(messages);

        // Also send persisted messages (messages that require persistence first)
        let persisted_messages: Vec<_> = ready.take_persisted_messages();
        if !persisted_messages.is_empty() {
            eprintln!("PROCESS_READY: node={} sending {} persisted messages", self.id, persisted_messages.len());
        }
        self.transport.send_messages(persisted_messages);

        // Update leader tracking
        let leader = {
            let node = self.node.lock();
            node.raft.leader_id
        };
        if leader != raft::INVALID_ID {
            self.leader_id.store(leader, Ordering::Relaxed);
            self.has_leader.store(true, Ordering::Relaxed);
        }

        // 5. Apply committed entries to state machine
        let committed_entries: Vec<_> = ready.take_committed_entries();
        if !committed_entries.is_empty() {
            eprintln!("READY: Processing {} committed entries", committed_entries.len());
        }
        for entry in &committed_entries {
            eprintln!("READY: Committed entry index={}, term={}, data_len={}, context_len={}",
                entry.index, entry.term, entry.data.len(), entry.context.len());
            if entry.data.is_empty() {
                // Noop entry
                eprintln!("READY: Skipping noop entry");
                continue;
            }

            // Extract proposal ID from context
            let proposal_id = if entry.context.len() >= 8 {
                let bytes: [u8; 8] = entry.context[..8].try_into().unwrap_or([0; 8]);
                u64::from_le_bytes(bytes)
            } else {
                0
            };
            eprintln!("READY: Entry has proposal_id={}", proposal_id);

            // Apply to state machine
            self.state_machine
                .apply(entry.index, entry.term, &entry.data)
                .await;

            // Track entry for checkpoint threshold
            self.record_checkpoint_entry();

            // Notify pending proposal
            if proposal_id > 0 {
                let pending_count = self.pending.lock().len();
                eprintln!("READY: Looking for proposal_id={} in {} pending proposals", proposal_id, pending_count);
                if let Some(pending) = self.pending.lock().remove(&proposal_id) {
                    eprintln!("READY: Found and notifying proposal_id={}", proposal_id);
                    let _ = pending.tx.send(Ok(ProposalResult {
                        index: entry.index,
                        term: entry.term,
                    }));
                } else {
                    eprintln!("READY: proposal_id={} NOT FOUND in pending", proposal_id);
                }
            }
        }

        // 6. Advance the ready
        let mut light_ready = {
            let mut node = self.node.lock();
            node.advance(ready)
        };

        // Send persisted messages
        let messages: Vec<_> = light_ready.take_messages();
        self.transport.send_messages(messages);

        // Apply more committed entries from light_ready
        let committed_entries: Vec<_> = light_ready.take_committed_entries();
        if !committed_entries.is_empty() {
            eprintln!("LIGHT_READY: Processing {} committed entries", committed_entries.len());
        }
        for entry in &committed_entries {
            eprintln!("LIGHT_READY: Committed entry index={}, term={}, data_len={}, context_len={}",
                entry.index, entry.term, entry.data.len(), entry.context.len());
            if entry.data.is_empty() {
                // Noop entry
                eprintln!("LIGHT_READY: Skipping noop entry");
                continue;
            }

            // Extract proposal ID from context
            let proposal_id = if entry.context.len() >= 8 {
                let bytes: [u8; 8] = entry.context[..8].try_into().unwrap_or([0; 8]);
                u64::from_le_bytes(bytes)
            } else {
                0
            };
            eprintln!("LIGHT_READY: Entry has proposal_id={}", proposal_id);

            // Apply to state machine
            self.state_machine
                .apply(entry.index, entry.term, &entry.data)
                .await;

            // Track entry for checkpoint threshold
            self.record_checkpoint_entry();

            // Notify pending proposal
            if proposal_id > 0 {
                let pending_count = self.pending.lock().len();
                eprintln!("LIGHT_READY: Looking for proposal_id={} in {} pending proposals", proposal_id, pending_count);
                if let Some(pending) = self.pending.lock().remove(&proposal_id) {
                    eprintln!("LIGHT_READY: Found and notifying proposal_id={}", proposal_id);
                    let _ = pending.tx.send(Ok(ProposalResult {
                        index: entry.index,
                        term: entry.term,
                    }));
                } else {
                    eprintln!("LIGHT_READY: proposal_id={} NOT FOUND in pending", proposal_id);
                }
            }
        }

        // Advance the light ready
        {
            let mut node = self.node.lock();
            node.advance_apply();
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
                    eprintln!("TICK_LOOP: node={} tick #{}, term={}, is_leader={}",
                        self.id, tick_count, self.term(), self.is_leader());
                    self.tick();
                    self.process_ready().await;
                    // 关键：强制 yield，让异步发送任务有机会运行
                    tokio::task::yield_now().await;
                }
                _ = shutdown_rx.recv() => {
                    info!("Raft tick loop shutting down");
                    break;
                }
            }
        }
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
