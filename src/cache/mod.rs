//! Distributed cache implementation.

pub mod router;
pub mod storage;

use crate::cluster::{ClusterDiscovery, ClusterEvent, ClusterMembership, NoOpClusterDiscovery};
use crate::config::CacheConfig;
use crate::consensus::{CacheStateMachine, RaftNode};
use crate::error::{Error, Result};
use crate::metrics::CacheMetrics;
use crate::multiraft::{MultiRaftBuilder, MultiRaftCoordinator};
use crate::network::rpc::{ForwardedCommand, ForwardResponse};
use crate::network::{Message, MessageHandler, NetworkServer};
use crate::types::{CacheCommand, CacheStats, ClusterStatus, NodeId};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex;
use router::CacheRouter;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use storage::CacheStorage;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

/// The main distributed cache instance.
///
/// This provides a strongly consistent distributed cache backed by Raft consensus.
/// All write operations go through the Raft leader, while reads can be served locally.
///
/// When Multi-Raft mode is enabled, operations are routed to the appropriate shard
/// based on key hash, allowing for horizontal scaling of write throughput.
pub struct DistributedCache {
    /// Cache router (single or multi-raft mode).
    router: CacheRouter,

    /// Local cache storage (for single mode, also accessible via router).
    storage: Arc<CacheStorage>,

    /// Raft consensus node (for single mode, also accessible via router).
    raft: Arc<RaftNode>,

    /// Cluster membership manager.
    membership: Arc<ClusterMembership>,

    /// Cluster discovery service (trait-based, supports multiple backends).
    discovery: Arc<Mutex<Box<dyn ClusterDiscovery>>>,

    /// Configuration.
    config: CacheConfig,

    /// Network server shutdown signal sender.
    shutdown_tx: mpsc::Sender<()>,

    /// Raft tick loop shutdown sender.
    tick_shutdown_tx: mpsc::Sender<()>,

    /// Discovery event loop shutdown sender.
    discovery_shutdown_tx: Option<mpsc::Sender<()>>,

    /// Pending forwarded requests awaiting leader response.
    /// Maps request_id -> oneshot sender for the response.
    pending_forwards: Arc<DashMap<u64, oneshot::Sender<Result<Option<Bytes>>>>>,

    /// Counter for generating unique forward request IDs.
    next_forward_id: AtomicU64,

    /// Shutdown flag to stop accepting new requests.
    shutdown_flag: AtomicBool,
}

impl DistributedCache {
    /// Create a new distributed cache instance.
    ///
    /// This will:
    /// 1. Validate configuration
    /// 2. Initialize the local Moka cache
    /// 3. Set up the Raft consensus layer
    /// 4. Start the network server
    /// 5. Begin the Raft tick loop
    /// 6. Start memberlist gossip (if enabled)
    /// 7. Initialize Multi-Raft coordinator (if enabled)
    pub async fn new(mut config: CacheConfig) -> Result<Self> {
        use crate::checkpoint::CheckpointManager;

        // Validate configuration
        if let Err(e) = config.validate() {
            return Err(Error::Config(e));
        }

        info!(
            node_id = config.node_id,
            raft_addr = %config.raft_addr,
            seed_nodes = ?config.seed_nodes,
            multiraft_enabled = config.multiraft.enabled,
            "Starting distributed cache"
        );
        info!(node_id = config.node_id, "Starting distributed cache");

        // Create local cache storage
        let storage = Arc::new(CacheStorage::new(&config));

        // Create state machine
        let state_machine = Arc::new(CacheStateMachine::new(storage.clone()));

        // Determine initial peers from seed nodes
        // Include this node and all seed nodes in the initial peer list
        let mut initial_peers: Vec<NodeId> = vec![config.node_id];
        for (peer_id, _) in &config.seed_nodes {
            if *peer_id != config.node_id && !initial_peers.contains(peer_id) {
                initial_peers.push(*peer_id);
            }
        }

        // Check for existing snapshot BEFORE creating Raft node
        // This allows us to set the correct applied index in raft-rs
        // Only do recovery when using persistent storage (RocksDB), not in-memory
        #[cfg(feature = "rocksdb-storage")]
        let uses_persistent_storage = matches!(
            config.raft.storage_type,
            crate::config::RaftStorageType::RocksDb(_)
        );
        #[cfg(not(feature = "rocksdb-storage"))]
        let uses_persistent_storage = false;

        let (recovered_index, checkpoint_manager) = if config.checkpoint.enabled && uses_persistent_storage {
            match CheckpointManager::new(config.checkpoint.clone(), storage.clone()) {
                Ok(manager) => {
                    let manager = Arc::new(manager);
                    // Find latest snapshot and get its index
                    match manager.find_latest_snapshot() {
                        Ok(Some(info)) => {
                            info!(
                                node_id = config.node_id,
                                path = %info.path.display(),
                                raft_index = info.raft_index,
                                raft_term = info.raft_term,
                                "Found existing snapshot for recovery"
                            );
                            (Some((info, manager.clone())), Some(manager))
                        }
                        Ok(None) => {
                            debug!(
                                node_id = config.node_id,
                                "No existing snapshot found"
                            );
                            (None, Some(manager))
                        }
                        Err(e) => {
                            warn!(
                                node_id = config.node_id,
                                error = %e,
                                "Failed to find snapshot"
                            );
                            (None, Some(manager))
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        node_id = config.node_id,
                        error = %e,
                        "Failed to create checkpoint manager"
                    );
                    (None, None)
                }
            }
        } else if config.checkpoint.enabled {
            // Checkpointing enabled but using in-memory storage - create manager but don't recover
            match CheckpointManager::new(config.checkpoint.clone(), storage.clone()) {
                Ok(manager) => (None, Some(Arc::new(manager))),
                Err(e) => {
                    warn!(
                        node_id = config.node_id,
                        error = %e,
                        "Failed to create checkpoint manager"
                    );
                    (None, None)
                }
            }
        } else {
            (None, None)
        };

        // Create Raft config with correct applied index for recovery
        let mut raft_config = config.raft.clone();
        if let Some((ref info, _)) = recovered_index {
            raft_config.applied = info.raft_index;
            info!(
                node_id = config.node_id,
                applied = info.raft_index,
                "Setting Raft applied index from snapshot for recovery"
            );
        }

        // Create Raft node with the correct applied index
        let raft = RaftNode::new(
            config.node_id,
            initial_peers.clone(),
            raft_config,
            state_machine.clone(),
            config.raft_addr.to_string(),
        )?;

        // Set checkpoint manager on Raft node if available
        if let Some(manager) = checkpoint_manager {
            raft.set_checkpoint_manager(manager);
        }

        // Load snapshot data into cache if recovering
        if let Some((info, manager)) = recovered_index {
            info!(
                node_id = config.node_id,
                path = %info.path.display(),
                "Loading snapshot data"
            );
            match manager.load_snapshot(&info.path).await {
                Ok(loaded_index) => {
                    // Update state machine's applied state
                    state_machine.set_recovered_state(info.raft_index, info.raft_term);
                    info!(
                        node_id = config.node_id,
                        loaded_index = loaded_index,
                        "Snapshot loaded successfully"
                    );
                }
                Err(e) => {
                    warn!(
                        node_id = config.node_id,
                        error = %e,
                        "Failed to load snapshot data"
                    );
                }
            }
        }

        // Add peers to transport using their actual node IDs
        for (peer_id, addr) in &config.seed_nodes {
            if *peer_id != config.node_id {
                raft.transport().add_peer(*peer_id, *addr).await;
            }
        }

        // Create membership manager with default config
        let (membership, _event_rx) =
            ClusterMembership::new(config.node_id, crate::config::MembershipConfig::default());

        // Create pending forwards map (shared with message handler)
        let pending_forwards = Arc::new(DashMap::new());

        // Create message handler
        let handler = CacheMessageHandler {
            raft: raft.clone(),
            pending_forwards: pending_forwards.clone(),
            node_id: config.node_id,
        };

        // Create and start network server
        let (server, shutdown_tx) =
            NetworkServer::new(config.raft_addr, config.node_id, Arc::new(handler));

        tokio::spawn(async move {
            if let Err(e) = server.run().await {
                tracing::error!(error = %e, "Network server error");
            }
        });

        // Start Raft tick loop
        let raft_clone = raft.clone();
        let (tick_shutdown_tx, tick_shutdown_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            raft_clone.run_tick_loop(tick_shutdown_rx).await;
        });

        // Get cluster discovery from config or create default NoOp
        let mut discovery_box: Box<dyn ClusterDiscovery> =
            if let Some(user_discovery) = config.cluster_discovery.take() {
                user_discovery
            } else {
                Box::new(NoOpClusterDiscovery::new(config.node_id, config.raft_addr))
            };

        // Start discovery if it's active
        let discovery_shutdown_tx = if discovery_box.is_active() {
            match discovery_box.start().await {
                Ok(()) => {
                    info!(
                        node_id = config.node_id,
                        active = discovery_box.is_active(),
                        "Cluster discovery started"
                    );

                    let discovery = Arc::new(Mutex::new(discovery_box));

                    // Start event processing loop
                    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
                    let raft_for_events = raft.clone();
                    let discovery_for_events = discovery.clone();

                    // Peer management defaults to false (conservative)
                    // Users can implement custom logic in their discovery implementation
                    let auto_add = false;
                    let auto_remove = false;
                    let auto_add_voters = false;
                    let auto_remove_voters = false;

                    tokio::spawn(async move {
                        Self::run_discovery_event_loop(
                            discovery_for_events,
                            raft_for_events,
                            shutdown_rx,
                            auto_add,
                            auto_remove,
                            auto_add_voters,
                            auto_remove_voters,
                        )
                        .await;
                    });

                    (discovery, Some(shutdown_tx))
                }
                Err(e) => {
                    warn!(error = %e, "Failed to start cluster discovery, continuing without it");
                    (Arc::new(Mutex::new(discovery_box)), None)
                }
            }
        } else {
            // Discovery not active, just wrap it
            (Arc::new(Mutex::new(discovery_box)), None)
        };
        let (discovery, discovery_shutdown_tx) = discovery_shutdown_tx;

        // Create the appropriate router based on configuration
        let router = if config.multiraft.enabled {
            // Create Multi-Raft coordinator
            let metrics = Arc::new(CacheMetrics::new());
            let coordinator = MultiRaftBuilder::new(config.node_id)
                .num_shards(config.multiraft.num_shards)
                .shard_capacity(config.multiraft.shard_capacity)
                .metrics(metrics)
                .build();

            // Initialize if auto-init is enabled
            if config.multiraft.auto_init_shards {
                coordinator.init().await.map_err(|e| {
                    Error::Internal(format!("Failed to initialize Multi-Raft coordinator: {}", e))
                })?;
            }

            info!(
                node_id = config.node_id,
                num_shards = config.multiraft.num_shards,
                "Multi-Raft mode enabled"
            );

            CacheRouter::multi(Arc::new(coordinator))
        } else {
            CacheRouter::single(storage.clone(), raft.clone())
        };

        info!(node_id = config.node_id, "Distributed cache started");

        Ok(Self {
            router,
            storage,
            raft,
            membership,
            discovery,
            config,
            shutdown_tx,
            tick_shutdown_tx,
            discovery_shutdown_tx,
            pending_forwards,
            next_forward_id: AtomicU64::new(1),
            shutdown_flag: AtomicBool::new(false),
        })
    }

    /// Run the memberlist event processing loop.
    ///
    /// This handles events from cluster discovery (node joins, leaves, failures) and
    /// updates the Raft transport accordingly. Works with any ClusterDiscovery implementation.
    async fn run_discovery_event_loop(
        discovery: Arc<Mutex<Box<dyn ClusterDiscovery>>>,
        raft: Arc<RaftNode>,
        mut shutdown_rx: mpsc::Receiver<()>,
        auto_add_peers: bool,
        auto_remove_peers: bool,
        auto_add_voters: bool,
        auto_remove_voters: bool,
    ) {
        info!("Starting cluster discovery event processing loop");

        loop {
            // Try to receive event with timeout
            let event = {
                let mut disc = discovery.lock();
                disc.try_recv_event()
            };

            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Discovery event loop shutting down");
                    break;
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Process any pending event
                    if let Some(event) = event {
                        Self::handle_discovery_event(
                            &event,
                            &raft,
                            auto_add_peers,
                            auto_remove_peers,
                            auto_add_voters,
                            auto_remove_voters,
                        ).await;
                    }
                }
            }
        }
    }

    /// Handle a single cluster discovery event.
    async fn handle_discovery_event(
        event: &ClusterEvent,
        raft: &Arc<RaftNode>,
        auto_add_peers: bool,
        auto_remove_peers: bool,
        auto_add_voters: bool,
        auto_remove_voters: bool,
    ) {
        match event {
            ClusterEvent::NodeJoin {
                node_id,
                raft_addr,
                metadata: _,
            } => {
                info!(
                    node_id = *node_id,
                    raft_addr = %raft_addr,
                    "Node discovered via cluster discovery"
                );

                if auto_add_peers {
                    // Add to Raft transport so we can communicate
                    raft.transport().add_peer(*node_id, *raft_addr).await;
                    debug!(node_id = *node_id, "Added peer to Raft transport");
                }

                // Propose ConfChange to add as voter if we're the leader
                if auto_add_voters && raft.is_leader() {
                    info!(
                        node_id = *node_id,
                        raft_addr = %raft_addr,
                        "Leader proposing ConfChange to add new voter"
                    );
                    match raft.add_voter(*node_id, *raft_addr).await {
                        Ok(()) => {
                            info!(node_id = *node_id, "Successfully proposed adding voter");
                        }
                        Err(e) => {
                            warn!(
                                node_id = *node_id,
                                error = %e,
                                "Failed to propose adding voter"
                            );
                        }
                    }
                }
            }

            ClusterEvent::NodeLeave { node_id } => {
                info!(node_id = *node_id, "Node left via cluster discovery");

                if auto_remove_peers {
                    // Remove from Raft transport
                    raft.transport().remove_peer(*node_id);
                    debug!(node_id = *node_id, "Removed peer from Raft transport");
                }

                // Propose ConfChange to remove voter if we're the leader
                if auto_remove_voters && raft.is_leader() {
                    info!(
                        node_id = *node_id,
                        "Leader proposing ConfChange to remove voter"
                    );
                    match raft.remove_voter(*node_id).await {
                        Ok(()) => {
                            info!(node_id = *node_id, "Successfully proposed removing voter");
                        }
                        Err(e) => {
                            warn!(
                                node_id = *node_id,
                                error = %e,
                                "Failed to propose removing voter"
                            );
                        }
                    }
                }
            }

            ClusterEvent::NodeFailed { node_id } => {
                warn!(node_id = *node_id, "Node failed via cluster discovery");

                if auto_remove_peers {
                    // Remove from Raft transport
                    raft.transport().remove_peer(*node_id);
                    debug!(
                        node_id = *node_id,
                        "Removed failed peer from Raft transport"
                    );
                }

                // Propose ConfChange to remove voter if we're the leader
                if auto_remove_voters && raft.is_leader() {
                    info!(
                        node_id = *node_id,
                        "Leader proposing ConfChange to remove failed voter"
                    );
                    match raft.remove_voter(*node_id).await {
                        Ok(()) => {
                            info!(node_id = *node_id, "Successfully proposed removing voter");
                        }
                        Err(e) => {
                            warn!(
                                node_id = *node_id,
                                error = %e,
                                "Failed to propose removing voter"
                            );
                        }
                    }
                }
            }

            ClusterEvent::NodeUpdate { node_id, metadata } => {
                debug!(
                    node_id = *node_id,
                    raft_addr = %metadata.raft_addr,
                    "Node metadata updated via cluster discovery"
                );

                // Update address in case it changed
                if auto_add_peers {
                    raft.transport()
                        .add_peer(*node_id, metadata.raft_addr)
                        .await;
                }
            }
        }
    }

    // ==================== Read Operations ====================

    /// Get a value from the local cache.
    ///
    /// This reads directly from the local Moka cache. On followers, this may
    /// return stale data. For strongly consistent reads, use `get_consistent`.
    ///
    /// Note: This method implements a Read-Index style wait to ensure the local
    /// state machine has caught up to the known commit index before reading.
    /// This helps avoid stale reads in test scenarios (TC23 fix).
    pub async fn get(&self, key: &[u8]) -> Option<Bytes> {
        // Read-Index: Wait for state machine to apply up to commit_index
        let commit_index = self.raft.commit_index();
        let start = std::time::Instant::now();
        let max_wait = Duration::from_secs(1);

        while self.raft.applied_index() < commit_index {
            if start.elapsed() > max_wait {
                warn!(
                    "Read-Index wait timeout: applied={} commit={}",
                    self.raft.applied_index(),
                    commit_index
                );
                break;
            }
            // Use yield_now() for minimal latency instead of sleep
            tokio::task::yield_now().await;
        }

        self.storage.get(key).await
    }

    /// Get a value with linearizable consistency (strongly consistent read).
    ///
    /// This method uses the Read-Index protocol to ensure the read is linearizable:
    /// 1. Verifies this node is the leader (or forwards to leader if configured)
    /// 2. Confirms leadership via Raft quorum before reading
    /// 3. Waits for state machine to apply up to the read index
    ///
    /// This is more expensive than `get()` but provides strong consistency guarantees.
    /// Use this when you need to read the most recent value and cannot tolerate stale reads.
    ///
    /// # Returns
    /// - `Ok(Some(value))` - The value exists and was read with linearizable consistency
    /// - `Ok(None)` - The key doesn't exist (confirmed with linearizable consistency)
    /// - `Err(NotLeader)` - This node is not the leader and forwarding is disabled
    /// - `Err(...)` - Other errors (timeout, etc.)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Strongly consistent read - guaranteed to see latest write
    /// let value = cache.consistent_get(b"key").await?;
    ///
    /// // vs regular read - may be stale on followers
    /// let value = cache.get(b"key").await;
    /// ```
    pub async fn consistent_get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        // If not leader and forwarding is enabled, forward to leader
        if !self.raft.is_leader() {
            // Check if we should forward to leader
            if self.config.forwarding.enabled {
                if let Some(_leader_id) = self.raft.leader_id() {
                    debug!(
                        node_id = self.raft.id(),
                        "CONSISTENT_GET: Not leader, forwarding read to leader"
                    );
                    // Forward the read to leader using existing forwarding mechanism
                    let command = CacheCommand::Get { key: key.to_vec() };
                    return self.forward_to_leader(command).await;
                }
            }
            return Err(crate::error::RaftError::NotLeader {
                leader: self.raft.leader_id(),
            }
            .into());
        }

        // Use Read-Index protocol to verify leadership and get linearizable read point
        let read_index = self.raft.read_index().await?;

        debug!(
            node_id = self.raft.id(),
            read_index,
            key = %String::from_utf8_lossy(&key[..std::cmp::min(key.len(), 32)]),
            "CONSISTENT_GET: Read linearizable at index"
        );

        // Now safe to read from local storage
        Ok(self.storage.get(key).await)
    }

    /// Check if a key exists in the local cache.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.storage.contains(key)
    }

    /// Get the number of entries in the local cache.
    pub fn entry_count(&self) -> u64 {
        self.storage.entry_count()
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        self.storage.stats()
    }

    /// Run pending cache maintenance tasks.
    /// This ensures all async cache operations have been processed.
    pub async fn run_pending_tasks(&self) {
        self.storage.run_pending_tasks().await;
    }

    // ==================== Write Operations ====================

    /// Put a key-value pair into the cache.
    ///
    /// This operation goes through Raft consensus and will be replicated
    /// to all nodes in the cluster. If this node is not the leader and
    /// forwarding is enabled, the request will be forwarded to the leader.
    pub async fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        let key = key.into();
        let value = value.into();

        let key_preview = String::from_utf8_lossy(&key[..std::cmp::min(key.len(), 32)]);
        let command = CacheCommand::put(key.to_vec(), value.to_vec());

        // Try local propose if leader, otherwise forward
        if self.raft.is_leader() {
            debug!(
                node_id = self.config.node_id,
                key = %key_preview,
                value_len = value.len(),
                "PUT: Submitting to Raft for replication (leader)"
            );

            let result = self.raft.propose(command).await?;

            debug!(
                node_id = self.config.node_id,
                key = %key_preview,
                raft_index = result.index,
                raft_term = result.term,
                "PUT: Successfully replicated via Raft"
            );

            Ok(())
        } else {
            info!(
                node_id = self.config.node_id,
                key = %key_preview,
                value_len = value.len(),
                "PUT: Forwarding to leader (not leader)"
            );

            self.forward_to_leader(command).await.map(|_| ())
        }
    }

    /// Put a key-value pair with a custom TTL.
    pub async fn put_with_ttl(
        &self,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
        ttl: Duration,
    ) -> Result<()> {
        let key = key.into();
        let value = value.into();

        let key_preview = String::from_utf8_lossy(&key[..std::cmp::min(key.len(), 32)]);
        let command = CacheCommand::put_with_ttl(key.to_vec(), value.to_vec(), ttl);

        if self.raft.is_leader() {
            debug!(
                node_id = self.config.node_id,
                key = %key_preview,
                value_len = value.len(),
                ttl_ms = ttl.as_millis(),
                "PUT_TTL: Submitting to Raft for replication (leader)"
            );

            let result = self.raft.propose(command).await?;

            debug!(
                node_id = self.config.node_id,
                key = %key_preview,
                raft_index = result.index,
                raft_term = result.term,
                "PUT_TTL: Successfully replicated via Raft"
            );

            Ok(())
        } else {
            debug!(
                node_id = self.config.node_id,
                key = %key_preview,
                value_len = value.len(),
                ttl_ms = ttl.as_millis(),
                "PUT_TTL: Forwarding to leader (not leader)"
            );

            self.forward_to_leader(command).await.map(|_| ())
        }
    }

    /// Delete a key from the cache.
    pub async fn delete(&self, key: impl Into<Bytes>) -> Result<()> {
        let key = key.into();

        let key_preview = String::from_utf8_lossy(&key[..std::cmp::min(key.len(), 32)]);
        let command = CacheCommand::delete(key.to_vec());

        if self.raft.is_leader() {
            debug!(
                node_id = self.config.node_id,
                key = %key_preview,
                "DELETE: Submitting to Raft for replication (leader)"
            );

            let result = self.raft.propose(command).await?;

            debug!(
                node_id = self.config.node_id,
                key = %key_preview,
                raft_index = result.index,
                raft_term = result.term,
                "DELETE: Successfully replicated via Raft"
            );

            Ok(())
        } else {
            debug!(
                node_id = self.config.node_id,
                key = %key_preview,
                "DELETE: Forwarding to leader (not leader)"
            );

            self.forward_to_leader(command).await.map(|_| ())
        }
    }

    /// Clear all entries from the cache.
    pub async fn clear(&self) -> Result<()> {
        let command = CacheCommand::clear();

        if self.raft.is_leader() {
            info!(
                node_id = self.config.node_id,
                "CLEAR: Submitting to Raft for replication (leader)"
            );

            let result = self.raft.propose(command).await?;

            info!(
                node_id = self.config.node_id,
                raft_index = result.index,
                raft_term = result.term,
                "CLEAR: Successfully replicated via Raft"
            );

            Ok(())
        } else {
            info!(
                node_id = self.config.node_id,
                "CLEAR: Forwarding to leader (not leader)"
            );

            self.forward_to_leader(command).await.map(|_| ())
        }
    }

    // ==================== Forwarding Logic ====================

    /// Forward a command to the leader node.
    ///
    /// This is called when this node receives a request but is not the leader.
    /// The request is forwarded to the leader and we wait for the response.
    /// Returns `Ok(None)` for write operations, `Ok(Some(value))` for GET operations.
    async fn forward_to_leader(&self, command: CacheCommand) -> Result<Option<Bytes>> {
        // Check if forwarding is enabled
        if !self.config.forwarding.enabled {
            return Err(Error::Raft(crate::error::RaftError::NotLeader {
                leader: self.raft.leader_id(),
            }));
        }

        // Backpressure check
        let pending_count = self.pending_forwards.len();
        if pending_count >= self.config.forwarding.max_pending_forwards {
            warn!(
                node_id = self.config.node_id,
                pending = pending_count,
                max = self.config.forwarding.max_pending_forwards,
                "FORWARD: Rejecting request due to backpressure"
            );
            return Err(Error::ServerBusy { pending: pending_count });
        }

        // Get leader ID
        let leader_id = self.raft.leader_id().ok_or_else(|| {
            warn!(
                node_id = self.config.node_id,
                "FORWARD: No leader available for forwarding"
            );
            Error::Raft(crate::error::RaftError::NotReady)
        })?;

        // Generate unique request ID
        let request_id = self.next_forward_id.fetch_add(1, Ordering::SeqCst);

        // Create completion channel
        let (tx, rx) = oneshot::channel();
        self.pending_forwards.insert(request_id, tx);

        // Create forwarded command message
        let msg = Message::ForwardedCommand(ForwardedCommand::new(
            request_id,
            self.config.node_id,
            command,
        ));

        debug!(
            node_id = self.config.node_id,
            request_id = request_id,
            leader_id = leader_id,
            "FORWARD: Sending ForwardedCommand to leader"
        );

        // Send to leader via transport
        if let Err(e) = self.raft.transport().send_message(leader_id, msg).await {
            self.pending_forwards.remove(&request_id);
            warn!(
                node_id = self.config.node_id,
                request_id = request_id,
                leader_id = leader_id,
                error = %e,
                "FORWARD: Failed to send to leader"
            );
            return Err(Error::ForwardFailed(e.to_string()));
        }

        // Wait for response with timeout
        let timeout = self.config.forwarding.timeout();
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => {
                debug!(
                    node_id = self.config.node_id,
                    request_id = request_id,
                    success = result.is_ok(),
                    "FORWARD: Received response from leader"
                );
                result
            }
            Ok(Err(_)) => {
                // Channel closed unexpectedly
                self.pending_forwards.remove(&request_id);
                warn!(
                    node_id = self.config.node_id,
                    request_id = request_id,
                    "FORWARD: Channel closed unexpectedly"
                );
                Err(Error::Internal("forward channel closed".into()))
            }
            Err(_) => {
                // Timeout
                self.pending_forwards.remove(&request_id);
                warn!(
                    node_id = self.config.node_id,
                    request_id = request_id,
                    timeout_ms = timeout.as_millis(),
                    "FORWARD: Timeout waiting for leader response"
                );
                Err(Error::Timeout)
            }
        }
    }

    /// Handle a ForwardResponse from the leader.
    ///
    /// This is called when we receive a response to a forwarded request.
    pub fn handle_forward_response(&self, response: &ForwardResponse) {
        if let Some((_, tx)) = self.pending_forwards.remove(&response.request_id) {
            let result = if response.success {
                // Convert Option<Vec<u8>> to Option<Bytes>
                Ok(response.value.as_ref().map(|v| Bytes::from(v.clone())))
            } else {
                Err(Error::RemoteError(
                    response.error.clone().unwrap_or_else(|| "unknown error".to_string()),
                ))
            };
            debug!(
                node_id = self.config.node_id,
                request_id = response.request_id,
                success = response.success,
                has_value = response.value.is_some(),
                "FORWARD: Completing pending forward"
            );
            let _ = tx.send(result);
        } else {
            warn!(
                node_id = self.config.node_id,
                request_id = response.request_id,
                "FORWARD: Received response for unknown request ID"
            );
        }
    }

    /// Get the pending forwards map (for message handler access).
    pub fn pending_forwards(&self) -> &Arc<DashMap<u64, oneshot::Sender<Result<Option<Bytes>>>>> {
        &self.pending_forwards
    }

    // ==================== Local Operations ====================

    /// Put a value into the local cache only (no replication).
    ///
    /// Use this for caching data that doesn't need consistency,
    /// such as locally computed values.
    pub async fn put_local(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) {
        self.storage.insert(key.into(), value.into()).await;
    }

    /// Invalidate a key in the local cache only.
    pub async fn invalidate_local(&self, key: &[u8]) {
        self.storage.invalidate(key).await;
    }

    // ==================== Cluster Management ====================

    /// Add a peer to the Raft cluster.
    ///
    /// The node must first be discovered before it can be added.
    pub fn add_peer(&self, node_id: NodeId) -> Result<()> {
        self.membership.add_raft_peer(node_id)
    }

    /// Remove a peer from the Raft cluster.
    pub fn remove_peer(&self, node_id: NodeId) -> Result<()> {
        self.membership.remove_raft_peer(node_id)
    }

    /// Get the current cluster status.
    pub fn cluster_status(&self) -> ClusterStatus {
        ClusterStatus {
            node_id: self.config.node_id,
            leader_id: self.raft.leader_id(),
            is_leader: self.raft.is_leader(),
            term: self.raft.term(),
            raft_peer_count: self.membership.raft_peer_count(),
            discovered_node_count: self.membership.discovered_nodes().len(),
            memberlist_node_count: self.memberlist_members().len(),
            commit_index: self.raft.commit_index(),
            applied_index: self.raft.applied_index(),
        }
    }

    /// Check if this node is the leader.
    pub fn is_leader(&self) -> bool {
        self.raft.is_leader()
    }

    /// Get the leader ID, if known.
    pub fn leader_id(&self) -> Option<NodeId> {
        self.raft.leader_id()
    }

    /// Get this node's ID.
    pub fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    /// Get the current voters in the Raft cluster.
    /// This is useful for debugging and verifying cluster configuration.
    pub fn voters(&self) -> Vec<NodeId> {
        self.raft.voters()
    }

    /// Check if a given node ID is a known voter in this cluster.
    pub fn is_known_voter(&self, node_id: NodeId) -> bool {
        self.raft.is_known_voter(node_id)
    }

    // ==================== Cluster Discovery ====================

    /// Check if cluster discovery (memberlist or other) is active.
    ///
    /// Returns true if using an active discovery mechanism like memberlist,
    /// false if using NoOp discovery (static configuration only).
    pub fn discovery_enabled(&self) -> bool {
        self.discovery.lock().is_active()
    }

    /// Check if memberlist gossip is enabled and running (alias for discovery_enabled).
    pub fn memberlist_enabled(&self) -> bool {
        self.discovery_enabled()
    }

    /// Get all nodes discovered via cluster discovery.
    pub fn discovery_members(&self) -> Vec<NodeId> {
        self.discovery.lock().members()
    }

    /// Get all nodes discovered via memberlist (alias for discovery_members).
    pub fn memberlist_members(&self) -> Vec<NodeId> {
        self.discovery_members()
    }

    /// Get healthy nodes discovered via cluster discovery.
    pub fn discovery_healthy_members(&self) -> Vec<NodeId> {
        self.discovery.lock().healthy_members()
    }

    /// Get healthy nodes discovered via memberlist (alias for discovery_healthy_members).
    pub fn memberlist_healthy_members(&self) -> Vec<NodeId> {
        self.discovery_healthy_members()
    }

    // ==================== Lifecycle ====================

    /// Shutdown the distributed cache gracefully.
    ///
    /// This method performs a graceful shutdown by:
    /// 1. Stopping acceptance of new requests
    /// 2. Waiting for pending Raft proposals to complete (with timeout)
    /// 3. Pausing active migrations and checkpointing their state
    /// 4. Leaving the cluster gracefully via memberlist
    /// 5. Stopping background tasks
    ///
    /// The shutdown has a default timeout of 30 seconds for pending operations.
    pub async fn shutdown(&self) {
        self.shutdown_with_timeout(Duration::from_secs(30)).await;
    }

    /// Shutdown the distributed cache with a custom timeout for pending operations.
    pub async fn shutdown_with_timeout(&self, timeout: Duration) {
        info!(
            node_id = self.config.node_id,
            timeout_secs = timeout.as_secs(),
            "Shutting down distributed cache"
        );

        let start = std::time::Instant::now();

        // 1. Stop accepting new requests
        self.shutdown_flag.store(true, Ordering::SeqCst);
        self.raft.stop_accepting_proposals();

        // 2. Wait for pending Raft proposals to complete (with timeout)
        let pending_deadline = start + timeout / 3; // Use 1/3 of timeout for proposals
        while self.raft.has_pending_proposals() {
            if std::time::Instant::now() > pending_deadline {
                warn!(
                    node_id = self.config.node_id,
                    pending = self.raft.pending_proposal_count(),
                    "Timeout waiting for pending proposals, continuing shutdown"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // 3. Shutdown Multi-Raft coordinator (if Multi mode)
        // This pauses migrations and checkpoints their state
        if let Some(coordinator) = self.router.coordinator() {
            if let Err(e) = coordinator.shutdown().await {
                warn!(
                    node_id = self.config.node_id,
                    error = %e,
                    "Error shutting down Multi-Raft coordinator"
                );
            }
        }

        // 4. Shutdown discovery event loop
        if let Some(ref tx) = self.discovery_shutdown_tx {
            let _ = tx.send(()).await;
        }

        // 5. Leave cluster discovery gracefully
        {
            let mut disc = self.discovery.lock();
            if disc.is_active() {
                if let Err(e) = disc.leave().await {
                    warn!(node_id = self.config.node_id, error = %e, "Error leaving cluster discovery");
                }
                if let Err(e) = disc.shutdown().await {
                    warn!(node_id = self.config.node_id, error = %e, "Error shutting down cluster discovery");
                }
            }
        }

        // 6. Shutdown Raft tick loop
        let _ = self.tick_shutdown_tx.send(()).await;

        // 7. Shutdown Raft node (flushes storage if persistent)
        if let Err(e) = self.raft.clone().shutdown().await {
            warn!(
                node_id = self.config.node_id,
                error = %e,
                "Error during Raft shutdown"
            );
        }

        // 8. Shutdown network server
        let _ = self.shutdown_tx.send(()).await;

        info!(
            node_id = self.config.node_id,
            elapsed_ms = start.elapsed().as_millis(),
            "Distributed cache shutdown complete"
        );
    }

    // ==================== Multi-Raft ====================

    /// Check if Multi-Raft mode is enabled.
    pub fn is_multiraft_enabled(&self) -> bool {
        self.router.is_multi_raft()
    }

    /// Get the Multi-Raft coordinator (only available in Multi-Raft mode).
    pub fn multiraft_coordinator(&self) -> Option<&Arc<MultiRaftCoordinator>> {
        self.router.coordinator()
    }

    /// Get the shard ID for a key (only meaningful in Multi-Raft mode).
    ///
    /// Returns None if Multi-Raft is not enabled.
    pub fn shard_for_key(&self, key: &[u8]) -> Option<u32> {
        self.router.coordinator().map(|c| c.shard_for_key(key))
    }

    // ==================== Recovery/Checkpoint Operations ====================

    /// Get the applied index (for recovery testing and monitoring).
    pub fn applied_index(&self) -> u64 {
        self.raft.applied_index()
    }

    /// Force a snapshot of the current state to disk.
    ///
    /// This creates both an in-memory Raft snapshot (for InstallSnapshot RPC)
    /// and a persistent disk snapshot (for recovery after restart).
    ///
    /// This is useful for testing recovery scenarios.
    pub async fn force_checkpoint(&self) -> Result<()> {
        self.raft
            .create_snapshot()
            .await
            .map(|_| ())
            .map_err(|e| Error::Internal(format!("Snapshot failed: {}", e)))
    }
}

/// Message handler that routes messages to the Raft node.
struct CacheMessageHandler {
    raft: Arc<RaftNode>,
    /// Pending forwarded requests awaiting leader response.
    pending_forwards: Arc<DashMap<u64, oneshot::Sender<Result<Option<Bytes>>>>>,
    /// Node ID for logging.
    node_id: NodeId,
}

impl MessageHandler for CacheMessageHandler {
    fn handle(&self, msg: Message) -> Option<Message> {
        // Handle ForwardResponse separately - complete pending forwards
        if let Message::ForwardResponse(ref response) = msg {
            if let Some((_, tx)) = self.pending_forwards.remove(&response.request_id) {
                let result = if response.success {
                    // Convert Option<Vec<u8>> to Option<Bytes>
                    Ok(response.value.as_ref().map(|v| Bytes::from(v.clone())))
                } else {
                    Err(Error::RemoteError(
                        response.error.clone().unwrap_or_else(|| "unknown error".to_string()),
                    ))
                };
                debug!(
                    node_id = self.node_id,
                    request_id = response.request_id,
                    success = response.success,
                    has_value = response.value.is_some(),
                    "FORWARD: Completing pending forward"
                );
                let _ = tx.send(result);
            } else {
                warn!(
                    node_id = self.node_id,
                    request_id = response.request_id,
                    "FORWARD: Received response for unknown request ID"
                );
            }
            return None;
        }

        // All other messages go to RaftNode
        self.raft.handle_message(msg)
    }
}

impl std::fmt::Debug for DistributedCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedCache")
            .field("node_id", &self.config.node_id)
            .field("is_leader", &self.raft.is_leader())
            .field("entry_count", &self.entry_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn test_config(node_id: NodeId) -> CacheConfig {
        CacheConfig {
            node_id,
            raft_addr: format!("127.0.0.1:{}", 19000 + node_id).parse().unwrap(),
            ..Default::default()
        }
    }

    fn test_config_with_peers(
        node_id: NodeId,
        seed_nodes: Vec<(NodeId, SocketAddr)>,
    ) -> CacheConfig {
        CacheConfig {
            node_id,
            raft_addr: format!("127.0.0.1:{}", 19000 + node_id).parse().unwrap(),
            seed_nodes,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_create_cache() {
        let config = test_config(1);
        let cache = DistributedCache::new(config).await;
        assert!(cache.is_ok());

        let cache = cache.unwrap();
        assert_eq!(cache.node_id(), 1);
        assert_eq!(cache.entry_count(), 0);
    }

    #[tokio::test]
    async fn test_local_operations() {
        let config = test_config(1);
        let cache = DistributedCache::new(config).await.unwrap();

        // Local put doesn't go through Raft
        cache.put_local("key1", "value1").await;

        let value = cache.get(b"key1").await;
        assert_eq!(value, Some(Bytes::from("value1")));

        cache.invalidate_local(b"key1").await;

        let value = cache.get(b"key1").await;
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_cluster_status() {
        let config = test_config(1);
        let cache = DistributedCache::new(config).await.unwrap();

        let status = cache.cluster_status();
        assert_eq!(status.node_id, 1);
        assert_eq!(status.raft_peer_count, 1);
    }

    #[tokio::test]
    async fn test_peer_registration_with_seed_nodes() {
        // Test that peer IDs are correctly registered from seed_nodes
        // Node 2 should register peers 1 and 3 with correct addresses
        let seed_nodes = vec![
            (1u64, "127.0.0.1:19001".parse().unwrap()),
            (3u64, "127.0.0.1:19003".parse().unwrap()),
        ];
        let config = test_config_with_peers(2, seed_nodes);
        let cache = DistributedCache::new(config).await.unwrap();

        // Verify transport has correct peer mappings
        let transport = cache.raft.transport();

        // Peer 1 should be at port 19001
        let peer1_addr = transport.get_peer(1);
        assert!(peer1_addr.is_some(), "Peer 1 should be registered");
        assert_eq!(peer1_addr.unwrap().port(), 19001);

        // Peer 3 should be at port 19003
        let peer3_addr = transport.get_peer(3);
        assert!(peer3_addr.is_some(), "Peer 3 should be registered");
        assert_eq!(peer3_addr.unwrap().port(), 19003);

        // Peer 2 (self) should NOT be registered
        let peer2_addr = transport.get_peer(2);
        assert!(
            peer2_addr.is_none(),
            "Self (peer 2) should not be registered"
        );
    }

    #[tokio::test]
    async fn test_peer_registration_non_sequential_ids() {
        // Test with non-sequential node IDs (e.g., 10, 20, 30) to ensure
        // we don't assume sequential IDs starting from 1
        let seed_nodes = vec![
            (10u64, "127.0.0.1:19010".parse().unwrap()),
            (30u64, "127.0.0.1:19030".parse().unwrap()),
        ];
        let config = test_config_with_peers(20, seed_nodes);

        // Override raft_addr for node 20
        let mut config = config;
        config.raft_addr = "127.0.0.1:19020".parse().unwrap();

        let cache = DistributedCache::new(config).await.unwrap();

        let transport = cache.raft.transport();

        // Peer 10 should be at port 19010
        let peer10_addr = transport.get_peer(10);
        assert!(peer10_addr.is_some(), "Peer 10 should be registered");
        assert_eq!(peer10_addr.unwrap().port(), 19010);

        // Peer 30 should be at port 19030
        let peer30_addr = transport.get_peer(30);
        assert!(peer30_addr.is_some(), "Peer 30 should be registered");
        assert_eq!(peer30_addr.unwrap().port(), 19030);

        // Old buggy behavior would have registered peers 1 and 2 instead
        let peer1_addr = transport.get_peer(1);
        assert!(
            peer1_addr.is_none(),
            "Peer 1 should NOT be registered (bug regression)"
        );
        let peer2_addr = transport.get_peer(2);
        assert!(
            peer2_addr.is_none(),
            "Peer 2 should NOT be registered (bug regression)"
        );
    }

    #[tokio::test]
    async fn test_peer_registration_empty_seed_nodes() {
        // Test single-node cluster with no seed nodes
        let config = test_config(1);
        let cache = DistributedCache::new(config).await.unwrap();

        let transport = cache.raft.transport();

        // No peers should be registered
        let peer_ids = transport.peer_ids();
        assert!(
            peer_ids.is_empty(),
            "No peers should be registered for single-node cluster"
        );
    }

    #[tokio::test]
    async fn test_peer_registration_duplicate_prevention() {
        // Test that duplicate node IDs in seed_nodes don't cause issues
        let seed_nodes = vec![
            (3u64, "127.0.0.1:19003".parse().unwrap()),
            (3u64, "127.0.0.1:19003".parse().unwrap()), // duplicate
        ];
        let config = test_config_with_peers(1, seed_nodes);
        let cache = DistributedCache::new(config).await.unwrap();

        let transport = cache.raft.transport();
        let peer_ids = transport.peer_ids();

        // Should only have one peer (3), not duplicates
        assert_eq!(peer_ids.len(), 1, "Should have exactly one peer");
        assert!(peer_ids.contains(&3), "Peer 3 should be registered");
    }

    #[tokio::test]
    async fn test_memberlist_disabled_by_default() {
        // Active discovery (memberlist) should be disabled by default
        let config = test_config(1);
        let cache = DistributedCache::new(config).await.unwrap();

        assert!(
            !cache.memberlist_enabled(),
            "Active discovery should be disabled by default"
        );
        // NoOp discovery still registers the local node
        assert_eq!(
            cache.memberlist_members().len(),
            1,
            "Local node should be registered even with NoOp discovery"
        );
    }

    #[tokio::test]
    async fn test_memberlist_config_fields() {
        // Test that memberlist config fields are properly initialized
        let config = crate::config::MemberlistConfig::default();

        assert!(!config.enabled);
        assert!(config.bind_addr.is_none());
        assert!(config.advertise_addr.is_none());
        assert!(config.seed_addrs.is_empty());
        assert!(config.auto_add_peers());
        assert!(!config.auto_remove_peers());
    }

    #[tokio::test]
    async fn test_memberlist_bind_addr_derivation() {
        // Test that memberlist bind addr is derived from raft addr when not specified
        let config = crate::config::MemberlistConfig::default();
        let raft_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

        let ml_addr = config.get_bind_addr(raft_addr);

        assert_eq!(ml_addr.ip(), raft_addr.ip());
        assert_eq!(ml_addr.port(), raft_addr.port() + 1000);
    }

    #[tokio::test]
    async fn test_cluster_status_includes_memberlist() {
        // Test that cluster status includes discovery node count
        // (local node is always registered even with NoOp discovery)
        let config = test_config(1);
        let cache = DistributedCache::new(config).await.unwrap();

        let status = cache.cluster_status();

        assert_eq!(status.memberlist_node_count, 1, "Local node should be counted");
    }
}
