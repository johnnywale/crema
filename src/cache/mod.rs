//! Distributed cache implementation.

pub mod router;
pub mod storage;

use crate::cluster::{ClusterDiscovery, ClusterEvent, ClusterMembership, NoOpClusterDiscovery};
use crate::config::CacheConfig;
use crate::consensus::{CacheStateMachine, RaftNode, TransportConfig};
use crate::error::{Error, Result};
use crate::metrics::CacheMetrics;
use crate::multiraft::{MultiRaftBuilder, MultiRaftCoordinator};
use crate::network::router::{MainRaftAdapter, NodeMessageRouter};
use crate::network::rpc::{ForwardResponse, ForwardedCommand};
use crate::network::{Message, MessageHandler, NetworkServer};
use crate::types::{CacheCommand, CacheStats, ClusterStatus, NodeId};
use crate::{counter_inc, gauge_set, histogram_record, histogram_record_duration};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex;
use router::CacheRouter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage::CacheStorage;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

/// Type alias for pending forward requests map.
/// Maps request_id -> (response sender, creation time for cleanup).
type PendingForwardsMap = Arc<DashMap<u64, (oneshot::Sender<Result<Option<Bytes>>>, Instant)>>;

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

    /// Metrics for monitoring.
    metrics: Arc<CacheMetrics>,

    /// Configuration.
    config: CacheConfig,

    /// Network server shutdown signal sender.
    shutdown_tx: mpsc::Sender<()>,

    /// Raft tick loop shutdown sender.
    tick_shutdown_tx: mpsc::Sender<()>,

    /// Discovery event loop shutdown sender.
    discovery_shutdown_tx: Option<mpsc::Sender<()>>,

    /// Pending forwarded requests awaiting leader response.
    pending_forwards: PendingForwardsMap,

    /// Counter for generating unique forward request IDs.
    next_forward_id: AtomicU64,

    /// Message handler reference (for setting shard handler after coordinator init).
    message_handler: Arc<CacheMessageHandler>,
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

        let (recovered_index, checkpoint_manager) =
            if config.checkpoint.enabled && uses_persistent_storage {
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
                                debug!(node_id = config.node_id, "No existing snapshot found");
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

        // Create node-wide message router FIRST - this is shared between main Raft and per-shard Raft
        // This ensures a single connection pool to each peer regardless of shard count
        let node_router = Arc::new(NodeMessageRouter::new(
            config.node_id,
            TransportConfig::default(),
        ));

        // Add peers to the router before creating Raft node
        for (peer_id, addr) in &config.seed_nodes {
            if *peer_id != config.node_id {
                node_router.add_peer(*peer_id, *addr).await;
            }
        }

        // Create MainRaftAdapter wrapping the shared router
        let main_transport = Arc::new(MainRaftAdapter::new(node_router.clone()));

        // Create Raft node with the shared transport
        let raft = RaftNode::new_with_transport(
            config.node_id,
            initial_peers.clone(),
            raft_config,
            state_machine.clone(),
            config.raft_addr.to_string(),
            main_transport,
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

        // Create membership manager with default config
        let (membership, _event_rx) =
            ClusterMembership::new(config.node_id, crate::config::MembershipConfig::default());

        // Create pending forwards map (shared with message handler)
        let pending_forwards = Arc::new(DashMap::new());

        // Create message handler
        let handler = Arc::new(CacheMessageHandler {
            raft: raft.clone(),
            pending_forwards: pending_forwards.clone(),
            node_id: config.node_id,
            shard_handler: parking_lot::RwLock::new(None),
            shard_forward_handler: parking_lot::RwLock::new(None),
            shard_forward_response_handler: parking_lot::RwLock::new(None),
            shard_creation_broadcast_handler: parking_lot::RwLock::new(None),
            get_topology_handler: parking_lot::RwLock::new(None),
            pending_shard_messages: parking_lot::Mutex::new(Vec::new()),
        });

        // Create and start network server
        let (server, shutdown_tx) =
            NetworkServer::new(config.raft_addr, config.node_id, handler.clone());

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

        // Start pending forwards cleanup task (prevent memory leaks from orphaned requests)
        let cleanup_pending_forwards = pending_forwards.clone();
        let cleanup_timeout = config.forwarding.timeout();
        tokio::spawn(async move {
            let cleanup_interval = cleanup_timeout.max(Duration::from_secs(5));
            // Clean up entries older than timeout + buffer to avoid race with normal timeout handling
            let max_age = cleanup_timeout + Duration::from_secs(5);
            loop {
                tokio::time::sleep(cleanup_interval).await;
                let now = Instant::now();
                let mut removed = 0u64;
                cleanup_pending_forwards.retain(|_id, (_tx, created_at)| {
                    let is_stale = now.duration_since(*created_at) > max_age;
                    if is_stale {
                        removed = removed.saturating_add(1);
                    }
                    !is_stale
                });
                if removed > 0 {
                    tracing::debug!(
                        removed = removed,
                        "Cleaned up stale pending forward entries"
                    );
                }
            }
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

        // Create metrics instance
        let metrics = Arc::new(CacheMetrics::new());

        // Create the appropriate router based on configuration
        let router = if config.multiraft.enabled {
            // Create Multi-Raft coordinator with optional per-shard Raft
            // Pass the shared node_router so per-shard Raft shares the same connection pool
            let builder = MultiRaftBuilder::new(config.node_id)
                .num_shards(config.multiraft.num_shards)
                .shard_capacity(config.multiraft.shard_capacity)
                .metrics(metrics.clone())
                .local_raft_addr(config.raft_addr.to_string())
                .per_shard_raft(config.multiraft.per_shard_raft_enabled)
                .with_node_router(node_router.clone())
                .with_seed_nodes(config.seed_nodes.clone());

            let coordinator = if config.multiraft.auto_init_shards {
                // Use build_and_init which handles per-shard Raft setup:
                // - Initializes shard Raft infrastructure
                // - Registers peer addresses with shard transport
                // - Initializes shards
                // - Starts shard Raft manager
                builder.build_and_init().await.map_err(|e| {
                    Error::Internal(format!(
                        "Failed to initialize Multi-Raft coordinator: {}",
                        e
                    ))
                })?
            } else {
                builder.build()
            };

            info!(
                node_id = config.node_id,
                num_shards = config.multiraft.num_shards,
                per_shard_raft = config.multiraft.per_shard_raft_enabled,
                "Multi-Raft mode enabled"
            );

            CacheRouter::multi(Arc::new(coordinator))
        } else {
            CacheRouter::single(storage.clone(), raft.clone())
        };

        // Set up shard message handlers if per-shard Raft is enabled and initialized
        // This enables ShardRaft message routing for replication
        if router.is_multi_raft() {
            if let Some(coordinator) = router.coordinator() {
                if coordinator.is_per_shard_raft_enabled() {
                    // 1. Set the transport on the shard forwarder so it can forward requests
                    // to shard leaders on other nodes
                    coordinator
                        .shard_forwarder()
                        .set_transport(raft.transport().clone());

                    info!(
                        node_id = config.node_id,
                        "Shard forwarder transport initialized"
                    );

                    // 2. Set up ShardRaft message handler (CRITICAL for replication)
                    if let Some(manager) = coordinator.shard_raft_manager() {
                        let multiplexer = manager.transport().clone();
                        let shard_handler: ShardMessageHandler =
                            Arc::new(move |shard_msg| multiplexer.handle_shard_message(shard_msg));
                        handler.set_shard_handler(shard_handler);

                        info!(
                            node_id = config.node_id,
                            "Per-shard Raft message handler installed"
                        );
                    }

                    // 3. Set up handler for ShardForwardedCommand messages (leader forwarding)
                    let coord_for_forward = coordinator.clone();
                    let node_id = config.node_id;

                    let forward_handler: ShardForwardHandler = Arc::new(move |fwd_cmd| {
                        let coord = coord_for_forward.clone();
                        Box::pin(async move {
                            let shard_id = fwd_cmd.shard_id;
                            let command = fwd_cmd.command;
                            let request_id = fwd_cmd.request_id;
                            let origin = fwd_cmd.origin_node_id;

                            tracing::debug!(
                                shard_id,
                                request_id,
                                origin,
                                "Processing ShardForwardedCommand"
                            );

                            // Get the shard and execute the command
                            if let Some(shard) = coord.get_shard(shard_id) {
                                match &command {
                                    crate::types::CacheCommand::Put {
                                        key,
                                        value,
                                        expires_at_ms,
                                    } => {
                                        let key = bytes::Bytes::from(key.clone());
                                        let value = bytes::Bytes::from(value.clone());

                                        let result = if let Some(exp_ms) = expires_at_ms {
                                            let now_ms = std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_millis()
                                                as u64;
                                            if *exp_ms > now_ms {
                                                let ttl = std::time::Duration::from_millis(
                                                    *exp_ms - now_ms,
                                                );
                                                shard.put_with_ttl(key, value, ttl).await
                                            } else {
                                                Ok(())
                                            }
                                        } else {
                                            shard.put(key, value).await
                                        };

                                        match result {
                                            Ok(()) => (true, None, None),
                                            Err(e) => (false, None, Some(e.to_string())),
                                        }
                                    }
                                    crate::types::CacheCommand::Delete { key } => {
                                        match shard.delete(key).await {
                                            Ok(()) => (true, None, None),
                                            Err(e) => (false, None, Some(e.to_string())),
                                        }
                                    }
                                    crate::types::CacheCommand::Get { key } => {
                                        let value = shard.get(key).await.map(|b| b.to_vec());
                                        (true, value, None)
                                    }
                                    crate::types::CacheCommand::Clear => {
                                        shard.clear().await;
                                        (true, None, None)
                                    }
                                }
                            } else {
                                tracing::warn!(
                                    node_id,
                                    shard_id,
                                    "Received ShardForwardedCommand for unknown shard"
                                );
                                (false, None, Some(format!("shard {} not found", shard_id)))
                            }
                        })
                    });

                    handler.set_shard_forward_handler(forward_handler);
                    info!(
                        node_id = config.node_id,
                        "Shard forward command handler installed"
                    );

                    // 4. Set up handler for ShardForwardResponse messages
                    let coord_for_response = coordinator.clone();
                    let response_handler: ShardForwardResponseHandler = Arc::new(move |response| {
                        coord_for_response
                            .shard_forwarder()
                            .handle_response(response);
                    });
                    handler.set_shard_forward_response_handler(response_handler);
                    info!(
                        node_id = config.node_id,
                        "Shard forward response handler installed"
                    );

                    // 5. Set up handler for ShardCreationBroadcast messages (cluster-wide shard sync)
                    let coord_for_broadcast = coordinator.clone();
                    let broadcast_handler: ShardCreationBroadcastHandler = Arc::new(
                        move |broadcast| {
                            let coord = coord_for_broadcast.clone();
                            let request_id = broadcast.request_id;
                            Box::pin(async move {
                                match coord.handle_shard_creation_broadcast(broadcast).await {
                                    Ok(ack) => ack,
                                    Err(e) => {
                                        tracing::warn!(error = %e, "Failed to handle shard creation broadcast");
                                        crate::network::rpc::ShardCreationAck {
                                            request_id,
                                            success: false,
                                            local_epoch: 0,
                                            error: Some(e.to_string()),
                                        }
                                    }
                                }
                            })
                        },
                    );
                    handler.set_shard_creation_broadcast_handler(broadcast_handler);
                    info!(
                        node_id = config.node_id,
                        "Shard creation broadcast handler installed"
                    );

                    // 6. Set up handler for GetTopology messages (cluster state catch-up)
                    let coord_for_topology = coordinator.clone();
                    let topology_handler: GetTopologyHandler =
                        Arc::new(move |request| coord_for_topology.handle_get_topology(request));
                    handler.set_get_topology_handler(topology_handler);
                    info!(node_id = config.node_id, "Get topology handler installed");
                }
            }
        }

        info!(node_id = config.node_id, "Distributed cache started");

        Ok(Self {
            router,
            storage,
            raft,
            membership,
            discovery,
            metrics,
            config,
            shutdown_tx,
            tick_shutdown_tx,
            discovery_shutdown_tx,
            pending_forwards,
            next_forward_id: AtomicU64::new(1),
            message_handler: handler,
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

    /// Get a value from the cache.
    ///
    /// In Multi-Raft mode, this routes to the appropriate shard based on key hash.
    /// In single-Raft mode, this reads directly from the local Moka cache.
    /// On followers, this may return stale data. For strongly consistent reads,
    /// use `get_consistent`.
    ///
    /// Note: In single-Raft mode, this method implements a Read-Index style wait
    /// to ensure the local state machine has caught up to the known commit index
    /// before reading. This helps avoid stale reads in test scenarios (TC23 fix).
    pub async fn get(&self, key: &[u8]) -> Option<Bytes> {
        let start = Instant::now();
        let node_id_str = self.config.node_id.to_string();

        // Record key size
        histogram_record!(
            crate::metrics::descriptors::CACHE_KEY_SIZE_BYTES,
            key.len() as f64,
            "node_id" => node_id_str.clone()
        );

        // In Multi-Raft mode, delegate to router which handles shard routing
        let result = if self.router.is_multi_raft() {
            self.router.get(key).await
        } else {
            // Single-Raft mode: Read-Index wait for state machine to apply up to commit_index
            let commit_index = self.raft.commit_index();
            let max_wait = Duration::from_secs(1);
            let wait_start = Instant::now();

            while self.raft.applied_index() < commit_index {
                if wait_start.elapsed() > max_wait {
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
        };

        // Record metrics
        let duration = start.elapsed();
        let hit = result.is_some();
        let result_str = if hit { "hit" } else { "miss" };

        counter_inc!(
            crate::metrics::descriptors::CACHE_GET_TOTAL,
            "node_id" => node_id_str.clone(),
            "result" => result_str
        );
        histogram_record_duration!(
            crate::metrics::descriptors::CACHE_GET_DURATION_SECONDS,
            duration,
            "node_id" => node_id_str.clone(),
            "result" => result_str
        );

        // Also record to legacy metrics
        self.metrics.record_get(hit, duration);

        result
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

    /// Check if a key exists in the cache.
    ///
    /// In Multi-Raft mode, this routes to the appropriate shard based on key hash.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.router.contains(key)
    }

    /// Get the number of entries in the cache.
    ///
    /// In Multi-Raft mode, this returns the total across all shards.
    pub fn entry_count(&self) -> u64 {
        self.router.entry_count()
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        let node_id_str = self.config.node_id.to_string();

        let stats = if let Some(coordinator) = self.multiraft_coordinator() {
            // In Multi-Raft mode, aggregate stats from all shards
            let stats = coordinator.stats();
            CacheStats {
                entry_count: stats.total_entries,
                weighted_size: stats.total_size_bytes,
                hits: self.metrics.get_hits.get(),
                misses: self.metrics.get_misses.get(),
            }
        } else {
            self.storage.stats()
        };

        // Update gauges with current stats
        gauge_set!(
            crate::metrics::descriptors::CACHE_ENTRIES,
            stats.entry_count as f64,
            "node_id" => node_id_str.clone()
        );
        gauge_set!(
            crate::metrics::descriptors::CACHE_SIZE_BYTES,
            stats.weighted_size as f64,
            "node_id" => node_id_str
        );

        // Also update legacy metrics
        self.metrics
            .update_cache_stats(stats.entry_count, stats.weighted_size);

        stats
    }

    /// Get the metrics instance.
    pub fn metrics(&self) -> &Arc<CacheMetrics> {
        &self.metrics
    }

    /// Run pending cache maintenance tasks.
    /// This ensures all async cache operations have been processed.
    ///
    /// In Multi-Raft mode, this runs pending tasks on all shards.
    pub async fn run_pending_tasks(&self) {
        self.router.run_pending_tasks().await;
    }

    // ==================== Write Operations ====================

    /// Put a key-value pair into the cache.
    ///
    /// This operation goes through Raft consensus and will be replicated
    /// to all nodes in the cluster. In Multi-Raft mode, the operation is
    /// routed to the appropriate shard based on key hash. In single-Raft mode,
    /// if this node is not the leader and forwarding is enabled, the request
    /// will be forwarded to the leader.
    pub async fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        let start = Instant::now();
        let key = key.into();
        let value = value.into();
        let node_id_str = self.config.node_id.to_string();

        // Validate key
        if key.is_empty() {
            return Err(Error::InvalidKey("key cannot be empty".to_string()));
        }

        // Record key and value sizes
        histogram_record!(
            crate::metrics::descriptors::CACHE_KEY_SIZE_BYTES,
            key.len() as f64,
            "node_id" => node_id_str.clone()
        );
        histogram_record!(
            crate::metrics::descriptors::CACHE_VALUE_SIZE_BYTES,
            value.len() as f64,
            "node_id" => node_id_str.clone()
        );

        let key_preview = String::from_utf8_lossy(&key[..std::cmp::min(key.len(), 32)]);

        // In Multi-Raft mode, delegate to router which handles shard routing
        let result = if self.router.is_multi_raft() {
            debug!(
                node_id = self.config.node_id,
                key = %key_preview,
                value_len = value.len(),
                "PUT: Routing to shard (Multi-Raft mode)"
            );
            self.router.put(key, value).await
        } else {
            // Single-Raft mode: use traditional leader forwarding
            let command = CacheCommand::put(key.to_vec(), value.to_vec());

            // Try local propose if leader, otherwise forward
            if self.raft.is_leader() {
                debug!(
                    node_id = self.config.node_id,
                    key = %key_preview,
                    value_len = value.len(),
                    "PUT: Submitting to Raft for replication (leader)"
                );

                let raft_result = self.raft.propose(command).await?;

                debug!(
                    node_id = self.config.node_id,
                    key = %key_preview,
                    raft_index = raft_result.index,
                    raft_term = raft_result.term,
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
        };

        // Record metrics
        let duration = start.elapsed();
        let success = result.is_ok();
        let success_str = if success { "true" } else { "false" };

        counter_inc!(
            crate::metrics::descriptors::CACHE_PUT_TOTAL,
            "node_id" => node_id_str.clone(),
            "success" => success_str,
            "has_ttl" => "false"
        );
        histogram_record_duration!(
            crate::metrics::descriptors::CACHE_PUT_DURATION_SECONDS,
            duration,
            "node_id" => node_id_str.clone(),
            "success" => success_str
        );

        // Also record to legacy metrics
        self.metrics.record_put(success, duration);

        result
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

        // Validate key
        if key.is_empty() {
            return Err(Error::InvalidKey("key cannot be empty".to_string()));
        }

        let key_preview = String::from_utf8_lossy(&key[..std::cmp::min(key.len(), 32)]);

        // In Multi-Raft mode, delegate to router which handles shard routing
        if self.router.is_multi_raft() {
            debug!(
                node_id = self.config.node_id,
                key = %key_preview,
                value_len = value.len(),
                ttl_ms = ttl.as_millis(),
                "PUT_TTL: Routing to shard (Multi-Raft mode)"
            );
            return self.router.put_with_ttl(key, value, ttl).await;
        }

        // Single-Raft mode: use traditional leader forwarding
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
        let start = Instant::now();
        let key = key.into();
        let node_id_str = self.config.node_id.to_string();

        // Validate key
        if key.is_empty() {
            return Err(Error::InvalidKey("key cannot be empty".to_string()));
        }

        let key_preview = String::from_utf8_lossy(&key[..std::cmp::min(key.len(), 32)]);

        // In Multi-Raft mode, delegate to router which handles shard routing
        let result = if self.router.is_multi_raft() {
            debug!(
                node_id = self.config.node_id,
                key = %key_preview,
                "DELETE: Routing to shard (Multi-Raft mode)"
            );
            self.router.delete(&key).await
        } else {
            // Single-Raft mode: use traditional leader forwarding
            let command = CacheCommand::delete(key.to_vec());

            if self.raft.is_leader() {
                debug!(
                    node_id = self.config.node_id,
                    key = %key_preview,
                    "DELETE: Submitting to Raft for replication (leader)"
                );

                let raft_result = self.raft.propose(command).await?;

                debug!(
                    node_id = self.config.node_id,
                    key = %key_preview,
                    raft_index = raft_result.index,
                    raft_term = raft_result.term,
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
        };

        // Record metrics
        let duration = start.elapsed();
        let success = result.is_ok();
        let success_str = if success { "true" } else { "false" };

        counter_inc!(
            crate::metrics::descriptors::CACHE_DELETE_TOTAL,
            "node_id" => node_id_str.clone(),
            "success" => success_str
        );
        histogram_record_duration!(
            crate::metrics::descriptors::CACHE_DELETE_DURATION_SECONDS,
            duration,
            "node_id" => node_id_str
        );

        // Also record to legacy metrics
        self.metrics.record_delete(duration);

        result
    }

    /// Clear all entries from the cache.
    pub async fn clear(&self) -> Result<()> {
        let node_id_str = self.config.node_id.to_string();

        // In Multi-Raft mode, delegate to router which handles shard clearing
        let result = if self.router.is_multi_raft() {
            info!(
                node_id = self.config.node_id,
                "CLEAR: Clearing all shards (Multi-Raft mode)"
            );
            self.router.clear().await
        } else {
            // Single-Raft mode: use traditional leader forwarding
            let command = CacheCommand::clear();

            if self.raft.is_leader() {
                info!(
                    node_id = self.config.node_id,
                    "CLEAR: Submitting to Raft for replication (leader)"
                );

                let raft_result = self.raft.propose(command).await?;

                info!(
                    node_id = self.config.node_id,
                    raft_index = raft_result.index,
                    raft_term = raft_result.term,
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
        };

        // Record metrics
        if result.is_ok() {
            counter_inc!(
                crate::metrics::descriptors::CACHE_CLEAR_TOTAL,
                "node_id" => node_id_str
            );
        }

        result
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
            return Err(Error::ServerBusy {
                pending: pending_count,
            });
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
        self.pending_forwards
            .insert(request_id, (tx, Instant::now()));

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
        if let Some((_, (tx, _))) = self.pending_forwards.remove(&response.request_id) {
            let result = if response.success {
                // Convert Option<Vec<u8>> to Option<Bytes>
                Ok(response.value.as_ref().map(|v| Bytes::from(v.clone())))
            } else {
                Err(Error::RemoteError(
                    response
                        .error
                        .clone()
                        .unwrap_or_else(|| "unknown error".to_string()),
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
    pub fn pending_forwards(&self) -> &PendingForwardsMap {
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

    /// Check if the cache is ready to handle requests.
    ///
    /// Readiness depends on the operating mode:
    /// - **Single Raft mode**: Ready when a Raft leader is known
    /// - **Multi-Raft mode (Phase 1)**: Ready when coordinator is running and shards are active
    /// - **Multi-Raft mode (Phase 2 / per-shard Raft)**: Ready when all shard leaders are elected
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let cache = DistributedCache::new(config).await?;
    ///
    /// // Wait until the cache is ready
    /// cache.wait_until_ready(Duration::from_secs(30)).await?;
    ///
    /// // Now safe to use
    /// cache.put("key", "value").await?;
    /// ```
    pub fn is_ready(&self) -> bool {
        if self.router.is_multi_raft() {
            // Multi-Raft mode
            if let Some(coordinator) = self.router.coordinator() {
                if coordinator.is_per_shard_raft_enabled() {
                    // Phase 2: All shard leaders must be elected
                    self.are_all_shard_leaders_elected()
                } else {
                    // Phase 1: Coordinator must be running and have active shards
                    coordinator.is_running()
                        && coordinator.stats().active_shards == self.config.multiraft.num_shards
                }
            } else {
                false
            }
        } else {
            // Single Raft mode: Need a known leader
            self.raft.leader_id().is_some()
        }
    }

    /// Check if all shard leaders are elected (for per-shard Raft mode).
    ///
    /// Returns true if every shard has a known leader, false otherwise.
    /// In non-Multi-Raft mode, always returns true.
    pub fn are_all_shard_leaders_elected(&self) -> bool {
        if let Some(coordinator) = self.router.coordinator() {
            if coordinator.is_per_shard_raft_enabled() {
                if let Some(manager) = coordinator.shard_raft_manager() {
                    let num_shards = self.config.multiraft.num_shards;
                    for shard_id in 0..num_shards {
                        if let Some(shard_node) = manager.get_shard(shard_id) {
                            if shard_node.leader_id().is_none() {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    return true;
                }
            }
        }
        // Not in per-shard Raft mode, or coordinator not available
        true
    }

    /// Get the number of shard leaders that have been elected.
    ///
    /// Returns `(elected_count, total_shards)`.
    /// In non-Multi-Raft mode, returns `(0, 0)`.
    pub fn shard_leader_status(&self) -> (u32, u32) {
        if let Some(coordinator) = self.router.coordinator() {
            let num_shards = self.config.multiraft.num_shards;

            if coordinator.is_per_shard_raft_enabled() {
                if let Some(manager) = coordinator.shard_raft_manager() {
                    let mut elected = 0u32;
                    for shard_id in 0..num_shards {
                        if let Some(shard_node) = manager.get_shard(shard_id) {
                            if shard_node.leader_id().is_some() {
                                elected += 1;
                            }
                        }
                    }
                    return (elected, num_shards);
                }
            }

            // Phase 1: Check router's cached shard leaders
            let leaders = coordinator.shard_leaders();
            let elected = leaders.values().filter(|l| l.is_some()).count() as u32;
            (elected, num_shards)
        } else {
            (0, 0)
        }
    }

    /// Wait until the cache is ready to handle requests.
    ///
    /// This method polls `is_ready()` until it returns true or the timeout is reached.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum time to wait for readiness
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Cache is ready
    /// * `Err(Error::Timeout)` - Timeout reached before cache became ready
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let cache = DistributedCache::new(config).await?;
    /// cache.wait_until_ready(Duration::from_secs(30)).await?;
    /// ```
    pub async fn wait_until_ready(&self, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        let poll_interval = Duration::from_millis(100);

        while start.elapsed() < timeout {
            if self.is_ready() {
                // Sync shard leaders to router cache if in per-shard Raft mode
                if let Some(coordinator) = self.router.coordinator() {
                    if coordinator.is_per_shard_raft_enabled() {
                        coordinator.sync_shard_leaders_from_raft_manager();
                    }
                }
                return Ok(());
            }
            tokio::time::sleep(poll_interval).await;
        }

        Err(Error::Timeout)
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
    #[allow(clippy::await_holding_lock)]
    pub async fn shutdown_with_timeout(&self, timeout: Duration) {
        info!(
            node_id = self.config.node_id,
            timeout_secs = timeout.as_secs(),
            "Shutting down distributed cache"
        );

        let start = std::time::Instant::now();

        // 1. Stop accepting new requests
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
        // Note: We hold the parking_lot mutex across await points here. This is generally
        // discouraged, but is acceptable for shutdown-only code because:
        // 1. Shutdown is single-threaded (no concurrent access expected)
        // 2. Discovery implementations should not acquire locks that could deadlock
        // 3. Converting to tokio::sync::Mutex would require making non-async methods async
        let discovery_active = self.discovery.lock().is_active();
        if discovery_active {
            {
                let mut disc = self.discovery.lock();
                if let Err(e) = disc.leave().await {
                    warn!(node_id = self.config.node_id, error = %e, "Error leaving cluster discovery");
                }
            }
            {
                let mut disc = self.discovery.lock();
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

    /// Set up the shard message handler for per-shard Raft.
    ///
    /// This should be called after the coordinator's shard Raft manager is initialized.
    /// It's automatically called during cache creation if auto_init_shards is true.
    /// For manual initialization (auto_init_shards=false), call this after
    /// `start_shard_raft_manager()`.
    /// Set up all handlers for per-shard Raft message routing (Phase 2).
    ///
    /// This installs handlers for:
    /// - ShardRaft messages (Raft protocol messages between shard replicas)
    /// - ShardForwardedCommand messages (forwarded writes from non-leaders)
    /// - ShardForwardResponse messages (responses to forwarded commands)
    pub fn setup_shard_message_handler(&self) {
        if let Some(coordinator) = self.router.coordinator() {
            if coordinator.is_per_shard_raft_enabled() {
                // Set the transport on the shard forwarder so it can send messages
                coordinator
                    .shard_forwarder()
                    .set_transport(self.raft.transport().clone());
                debug!(
                    node_id = self.config.node_id,
                    "Shard forwarder transport initialized"
                );

                // 1. Set up handler for ShardRaft messages (Raft replication)
                if let Some(manager) = coordinator.shard_raft_manager() {
                    let multiplexer = manager.transport().clone();
                    let shard_handler: ShardMessageHandler =
                        Arc::new(move |shard_msg| multiplexer.handle_shard_message(shard_msg));
                    self.message_handler.set_shard_handler(shard_handler);
                    debug!(
                        node_id = self.config.node_id,
                        "Shard Raft message handler installed"
                    );
                }

                // 2. Set up handler for ShardForwardedCommand messages (leader forwarding)
                let coord_for_forward = coordinator.clone();
                let node_id = self.config.node_id;

                let forward_handler: ShardForwardHandler = Arc::new(move |fwd_cmd| {
                    let coord = coord_for_forward.clone();
                    Box::pin(async move {
                        let shard_id = fwd_cmd.shard_id;
                        let command = fwd_cmd.command;
                        let request_id = fwd_cmd.request_id;
                        let origin = fwd_cmd.origin_node_id;

                        tracing::debug!(
                            shard_id,
                            request_id,
                            origin,
                            "Processing ShardForwardedCommand"
                        );

                        // Get the shard and execute the command
                        if let Some(shard) = coord.get_shard(shard_id) {
                            match &command {
                                crate::types::CacheCommand::Put {
                                    key,
                                    value,
                                    expires_at_ms,
                                } => {
                                    let key_preview = String::from_utf8_lossy(
                                        &key[..std::cmp::min(key.len(), 32)],
                                    );
                                    tracing::debug!(
                                        shard_id,
                                        key = %key_preview,
                                        "Forward handler: executing PUT"
                                    );

                                    let key = bytes::Bytes::from(key.clone());
                                    let value = bytes::Bytes::from(value.clone());

                                    let result = if let Some(exp_ms) = expires_at_ms {
                                        let now_ms = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap_or_default()
                                            .as_millis()
                                            as u64;
                                        if *exp_ms > now_ms {
                                            let ttl =
                                                std::time::Duration::from_millis(*exp_ms - now_ms);
                                            shard.put_with_ttl(key, value, ttl).await
                                        } else {
                                            Ok(())
                                        }
                                    } else {
                                        shard.put(key, value).await
                                    };

                                    match &result {
                                        Ok(()) => {
                                            tracing::debug!(shard_id, "Forward PUT succeeded")
                                        }
                                        Err(e) => {
                                            tracing::warn!(shard_id, error = %e, "Forward PUT failed")
                                        }
                                    }

                                    match result {
                                        Ok(()) => (true, None, None),
                                        Err(e) => (false, None, Some(e.to_string())),
                                    }
                                }
                                crate::types::CacheCommand::Delete { key } => {
                                    match shard.delete(key).await {
                                        Ok(()) => (true, None, None),
                                        Err(e) => (false, None, Some(e.to_string())),
                                    }
                                }
                                crate::types::CacheCommand::Get { key } => {
                                    let value = shard.get(key).await.map(|b| b.to_vec());
                                    (true, value, None)
                                }
                                crate::types::CacheCommand::Clear => {
                                    shard.clear().await;
                                    (true, None, None)
                                }
                            }
                        } else {
                            tracing::warn!(
                                node_id = node_id,
                                shard_id = shard_id,
                                "Received ShardForwardedCommand for unknown shard"
                            );
                            (false, None, Some(format!("shard {} not found", shard_id)))
                        }
                    })
                });

                self.message_handler
                    .set_shard_forward_handler(forward_handler);
                debug!(
                    node_id = self.config.node_id,
                    "Shard forward command handler installed"
                );

                // 3. Set up handler for ShardForwardResponse messages
                let coord_for_response = coordinator.clone();
                let response_handler: ShardForwardResponseHandler = Arc::new(move |response| {
                    coord_for_response
                        .shard_forwarder()
                        .handle_response(response);
                });
                self.message_handler
                    .set_shard_forward_response_handler(response_handler);
                debug!(
                    node_id = self.config.node_id,
                    "Shard forward response handler installed"
                );

                // 4. Set up handler for ShardCreationBroadcast messages (cluster-wide shard sync)
                let coord_for_broadcast = coordinator.clone();
                let broadcast_handler: ShardCreationBroadcastHandler = Arc::new(move |broadcast| {
                    let coord = coord_for_broadcast.clone();
                    let request_id = broadcast.request_id;
                    Box::pin(async move {
                        match coord.handle_shard_creation_broadcast(broadcast).await {
                            Ok(ack) => ack,
                            Err(e) => {
                                tracing::warn!(error = %e, "Failed to handle shard creation broadcast");
                                crate::network::rpc::ShardCreationAck {
                                    request_id,
                                    success: false,
                                    local_epoch: 0,
                                    error: Some(e.to_string()),
                                }
                            }
                        }
                    })
                });
                self.message_handler
                    .set_shard_creation_broadcast_handler(broadcast_handler);
                debug!(
                    node_id = self.config.node_id,
                    "Shard creation broadcast handler installed (via setup_shard_message_handler)"
                );

                // 5. Set up handler for GetTopology messages (cluster state catch-up)
                let coord_for_topology = coordinator.clone();
                let topology_handler: GetTopologyHandler =
                    Arc::new(move |request| coord_for_topology.handle_get_topology(request));
                self.message_handler
                    .set_get_topology_handler(topology_handler);
                debug!(
                    node_id = self.config.node_id,
                    "Get topology handler installed (via setup_shard_message_handler)"
                );
            }
        }
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

    // ==================== Dynamic Shard Management ====================

    /// Enable slot-based routing for dynamic shard management.
    ///
    /// This enables the slot-based sharding system which allows adding and removing
    /// shards at runtime. When enabled:
    /// - Keys are mapped to slots via `crc16(key) % 1024`
    /// - Slots are assigned to shards
    /// - Epoch-based routing ensures consistency
    ///
    /// Only available in Multi-Raft mode.
    pub async fn enable_slot_routing(&self) -> Result<()> {
        let coordinator = self
            .router
            .coordinator()
            .ok_or_else(|| Error::Internal("Multi-Raft not enabled".to_string()))?;
        coordinator.enable_slot_routing().await
    }

    /// Check if slot-based routing is enabled.
    pub fn is_slot_routing_enabled(&self) -> bool {
        self.router
            .coordinator()
            .map(|c| c.is_slot_routing_enabled())
            .unwrap_or(false)
    }

    /// Add a new shard dynamically (slot-based routing only).
    ///
    /// This method:
    /// 1. Creates a new empty shard
    /// 2. Computes a rebalance plan (steals slots from existing shards)
    /// 3. Updates the slot table with new ownership
    /// 4. Starts background migration of data
    ///
    /// # Returns
    ///
    /// Returns the new shard ID and slot assignment details.
    ///
    /// # Errors
    ///
    /// Returns an error if Multi-Raft or slot routing is not enabled.
    pub async fn add_shard(&self) -> Result<crate::multiraft::AddShardResult> {
        let coordinator = self
            .router
            .coordinator()
            .ok_or_else(|| Error::Internal("Multi-Raft not enabled".to_string()))?;

        coordinator.add_shard_dynamic().await
    }

    /// Remove a shard dynamically (slot-based routing only).
    ///
    /// This method:
    /// 1. Marks the shard as draining
    /// 2. Redistributes its slots among remaining shards
    /// 3. Starts background migration of data
    /// 4. Removes the shard when migration completes
    ///
    /// # Note
    ///
    /// The shard is not immediately removed. It enters DRAINING state first,
    /// then TOMBSTONE, and finally gets garbage collected.
    ///
    /// # Errors
    ///
    /// Returns an error if Multi-Raft or slot routing is not enabled.
    pub async fn remove_shard(&self, shard_id: u32) -> Result<crate::multiraft::RemoveShardResult> {
        let coordinator = self
            .router
            .coordinator()
            .ok_or_else(|| Error::Internal("Multi-Raft not enabled".to_string()))?;

        coordinator.remove_shard_dynamic(shard_id).await
    }

    /// Get a snapshot of the slot table.
    ///
    /// Returns None if slot routing is not enabled.
    pub fn slot_table(&self) -> Option<crate::multiraft::SlotTableSnapshot> {
        self.router.coordinator()?.slot_table_snapshot()
    }

    /// Get the current slot routing epoch.
    ///
    /// Returns None if slot routing is not enabled.
    pub fn slot_epoch(&self) -> Option<crate::multiraft::Epoch> {
        self.router.coordinator()?.slot_epoch()
    }

    /// Get the migration status for slot-based routing.
    ///
    /// Returns None if slot routing is not enabled.
    pub fn slot_migration_status(&self) -> Option<crate::multiraft::MigrationStatus> {
        self.router.coordinator()?.slot_migration_status()
    }

    /// Start the slot migration background loop.
    ///
    /// This should be called after enabling slot routing to start
    /// background data migration when shards are added or removed.
    pub fn start_slot_migration(&self) {
        if let Some(coordinator) = self.router.coordinator() {
            coordinator.start_slot_migration_loop();
        }
    }

    /// Stop the slot migration background loop.
    pub fn stop_slot_migration(&self) {
        if let Some(coordinator) = self.router.coordinator() {
            coordinator.stop_slot_migration_loop();
        }
    }

    // ==================== End Dynamic Shard Management ====================
}

/// Callback type for handling shard Raft messages.
pub type ShardMessageHandler =
    Arc<dyn Fn(crate::network::rpc::ShardRaftMessage) -> Result<()> + Send + Sync>;

/// Callback type for handling shard forwarded commands.
/// Returns (success, value, error) tuple for the response.
pub type ShardForwardHandler = Arc<
    dyn Fn(
            crate::network::rpc::ShardForwardedCommand,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = (bool, Option<Vec<u8>>, Option<String>)> + Send>,
        > + Send
        + Sync,
>;

/// Callback type for handling shard forward responses.
pub type ShardForwardResponseHandler =
    Arc<dyn Fn(&crate::network::rpc::ShardForwardResponse) + Send + Sync>;

/// Callback type for handling shard creation broadcasts.
/// Returns a ShardCreationAck.
pub type ShardCreationBroadcastHandler = Arc<
    dyn Fn(
            crate::network::rpc::ShardCreationBroadcast,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = crate::network::rpc::ShardCreationAck> + Send>,
        > + Send
        + Sync,
>;

/// Callback type for handling topology requests.
/// Returns a TopologyResponse.
pub type GetTopologyHandler = Arc<
    dyn Fn(crate::network::rpc::GetTopologyRequest) -> crate::network::rpc::TopologyResponse
        + Send
        + Sync,
>;

/// Message handler that routes messages to the Raft node.
struct CacheMessageHandler {
    raft: Arc<RaftNode>,
    /// Pending forwarded requests awaiting leader response.
    pending_forwards: PendingForwardsMap,
    /// Node ID for logging.
    node_id: NodeId,
    /// Optional handler for shard Raft messages (set after coordinator init).
    shard_handler: parking_lot::RwLock<Option<ShardMessageHandler>>,
    /// Optional handler for shard forwarded commands (set after coordinator init).
    shard_forward_handler: parking_lot::RwLock<Option<ShardForwardHandler>>,
    /// Optional handler for shard forward responses (set after coordinator init).
    shard_forward_response_handler: parking_lot::RwLock<Option<ShardForwardResponseHandler>>,
    /// Optional handler for shard creation broadcasts.
    shard_creation_broadcast_handler: parking_lot::RwLock<Option<ShardCreationBroadcastHandler>>,
    /// Optional handler for topology requests.
    get_topology_handler: parking_lot::RwLock<Option<GetTopologyHandler>>,
    /// Pending shard messages that arrived before handler was set (limited to avoid memory issues).
    pending_shard_messages: parking_lot::Mutex<Vec<crate::network::rpc::ShardRaftMessage>>,
}

impl CacheMessageHandler {
    /// Set the shard message handler for routing per-shard Raft messages.
    ///
    /// This also processes any pending shard messages that arrived before the handler was set.
    pub fn set_shard_handler(&self, handler: ShardMessageHandler) {
        // Set the handler first
        *self.shard_handler.write() = Some(handler.clone());

        // Process any pending messages that arrived before the handler was set
        let pending = std::mem::take(&mut *self.pending_shard_messages.lock());
        if !pending.is_empty() {
            debug!(
                node_id = self.node_id,
                count = pending.len(),
                "Processing pending shard messages after handler installation"
            );
            for shard_msg in pending {
                if let Err(e) = handler(shard_msg) {
                    warn!(
                        node_id = self.node_id,
                        error = %e,
                        "Failed to handle pending shard Raft message"
                    );
                }
            }
        }
    }

    /// Set the shard forward handler for handling forwarded shard commands.
    pub fn set_shard_forward_handler(&self, handler: ShardForwardHandler) {
        *self.shard_forward_handler.write() = Some(handler);
    }

    /// Set the shard forward response handler for completing pending forwards.
    pub fn set_shard_forward_response_handler(&self, handler: ShardForwardResponseHandler) {
        *self.shard_forward_response_handler.write() = Some(handler);
    }

    /// Set the shard creation broadcast handler for handling shard coordination.
    pub fn set_shard_creation_broadcast_handler(&self, handler: ShardCreationBroadcastHandler) {
        *self.shard_creation_broadcast_handler.write() = Some(handler);
    }

    /// Set the get topology handler for handling topology requests.
    pub fn set_get_topology_handler(&self, handler: GetTopologyHandler) {
        *self.get_topology_handler.write() = Some(handler);
    }
}

impl MessageHandler for CacheMessageHandler {
    fn handle(&self, msg: Message) -> Option<Message> {
        // Handle ForwardResponse separately - complete pending forwards
        if let Message::ForwardResponse(ref response) = msg {
            if let Some((_, (tx, _))) = self.pending_forwards.remove(&response.request_id) {
                let result = if response.success {
                    // Convert Option<Vec<u8>> to Option<Bytes>
                    Ok(response.value.as_ref().map(|v| Bytes::from(v.clone())))
                } else {
                    Err(Error::RemoteError(
                        response
                            .error
                            .clone()
                            .unwrap_or_else(|| "unknown error".to_string()),
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

        // Handle per-shard Raft messages
        if let Message::ShardRaft(shard_msg) = msg {
            if let Some(handler) = self.shard_handler.read().as_ref() {
                if let Err(e) = handler(shard_msg) {
                    warn!(
                        node_id = self.node_id,
                        error = %e,
                        "Failed to handle shard Raft message"
                    );
                }
            } else {
                // Handler not set yet (startup race) - queue the message for later processing.
                // This typically only happens during the brief window between network server
                // starting and coordinator initialization completing.
                //
                // Limit queue size to prevent memory issues. If handler is never set (e.g.,
                // Multi-Raft disabled), messages will be dropped once queue is full.
                // This is acceptable as Raft will retry from the sender side.
                const MAX_PENDING_SHARD_MESSAGES: usize = 1000;
                let mut pending = self.pending_shard_messages.lock();
                if pending.len() < MAX_PENDING_SHARD_MESSAGES {
                    debug!(
                        node_id = self.node_id,
                        shard_id = shard_msg.shard_id,
                        pending_count = pending.len() + 1,
                        "Queuing shard Raft message (handler not yet set)"
                    );
                    pending.push(shard_msg);
                } else {
                    warn!(
                        node_id = self.node_id,
                        shard_id = shard_msg.shard_id,
                        "Dropping shard Raft message - pending queue full (handler not set)"
                    );
                }
            }
            return None;
        }

        // Handle shard forwarded commands (for per-shard Raft Phase 2)
        if let Message::ShardForwardedCommand(fwd_cmd) = msg {
            let request_id = fwd_cmd.request_id;
            let origin_node_id = fwd_cmd.origin_node_id;
            let ttl = fwd_cmd.ttl;

            // TTL check to prevent infinite forwarding loops
            if ttl == 0 {
                warn!(
                    node_id = self.node_id,
                    request_id = request_id,
                    shard_id = fwd_cmd.shard_id,
                    origin = origin_node_id,
                    "ShardForwardedCommand TTL expired, rejecting to prevent infinite loop"
                );
                let transport = self.raft.transport().clone();
                let response = Message::ShardForwardResponse(
                    crate::network::rpc::ShardForwardResponse::error(
                        request_id,
                        "TTL expired - forwarding loop detected",
                    ),
                );
                tokio::spawn(async move {
                    if let Err(e) = transport.send_message(origin_node_id, response).await {
                        warn!(error = %e, "Failed to send TTL expired response");
                    }
                });
                return None;
            }

            if let Some(handler) = self.shard_forward_handler.read().clone() {
                let transport = self.raft.transport().clone();
                let node_id = self.node_id;

                debug!(
                    node_id = node_id,
                    request_id = request_id,
                    origin = origin_node_id,
                    shard_id = fwd_cmd.shard_id,
                    ttl = ttl,
                    "Handling ShardForwardedCommand"
                );

                // Spawn task to process the forwarded command and send response
                tokio::spawn(async move {
                    let (success, value, error) = handler(fwd_cmd).await;

                    let response =
                        Message::ShardForwardResponse(crate::network::rpc::ShardForwardResponse {
                            request_id,
                            success,
                            error,
                            value,
                            leader_hint: None,
                        });

                    if let Err(e) = transport.send_message(origin_node_id, response).await {
                        warn!(
                            node_id = node_id,
                            request_id = request_id,
                            origin = origin_node_id,
                            error = %e,
                            "Failed to send ShardForwardResponse"
                        );
                    }
                });
            } else {
                warn!(
                    node_id = self.node_id,
                    shard_id = fwd_cmd.shard_id,
                    request_id = fwd_cmd.request_id,
                    "Received ShardForwardedCommand but no handler is set"
                );
            }
            return None;
        }

        // Handle shard forward responses (complete pending forwards)
        if let Message::ShardForwardResponse(ref response) = msg {
            debug!(
                node_id = self.node_id,
                request_id = response.request_id,
                success = response.success,
                "Received ShardForwardResponse"
            );

            if let Some(handler) = self.shard_forward_response_handler.read().as_ref() {
                handler(response);
            } else {
                warn!(
                    node_id = self.node_id,
                    request_id = response.request_id,
                    "Received ShardForwardResponse but no handler is set"
                );
            }
            return None;
        }

        // Handle shard creation broadcasts
        if let Message::ShardCreationBroadcast(broadcast) = msg {
            debug!(
                node_id = self.node_id,
                request_id = broadcast.request_id,
                shard_id = broadcast.shard_id,
                originator = broadcast.originator_node,
                "Received ShardCreationBroadcast"
            );

            if let Some(handler) = self.shard_creation_broadcast_handler.read().clone() {
                let transport = self.raft.transport().clone();
                let node_id = self.node_id;
                let request_id = broadcast.request_id;
                let origin = broadcast.originator_node;

                // Spawn task to process broadcast and send response
                tokio::spawn(async move {
                    let ack = handler(broadcast).await;
                    let response = Message::ShardCreationAck(ack);

                    if let Err(e) = transport.send_message(origin, response).await {
                        warn!(
                            node_id = node_id,
                            request_id = request_id,
                            origin = origin,
                            error = %e,
                            "Failed to send ShardCreationAck"
                        );
                    }
                });
            } else {
                warn!(
                    node_id = self.node_id,
                    request_id = broadcast.request_id,
                    "Received ShardCreationBroadcast but no handler is set"
                );
            }
            return None;
        }

        // Handle shard creation acks (just log for now - fire-and-forget broadcast)
        if let Message::ShardCreationAck(ref ack) = msg {
            debug!(
                node_id = self.node_id,
                request_id = ack.request_id,
                success = ack.success,
                peer_epoch = ack.local_epoch,
                "Received ShardCreationAck"
            );
            // Acks are currently fire-and-forget, no action needed
            return None;
        }

        // Handle topology requests
        if let Message::GetTopology(request) = msg {
            debug!(
                node_id = self.node_id,
                request_id = request.request_id,
                requesting_node = request.requesting_node,
                "Received GetTopology request"
            );

            if let Some(handler) = self.get_topology_handler.read().clone() {
                let response = handler(request);
                return Some(Message::TopologyResponse(response));
            } else {
                warn!(
                    node_id = self.node_id,
                    "Received GetTopology but no handler is set"
                );
            }
            return None;
        }

        // Handle topology responses (just log for now - handled by caller)
        if let Message::TopologyResponse(ref response) = msg {
            debug!(
                node_id = self.node_id,
                request_id = response.request_id,
                epoch = response.current_epoch,
                shards = response.shard_ids.len(),
                "Received TopologyResponse"
            );
            // Responses are handled by the requesting side via send_raw_message_with_response
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

        assert_eq!(
            status.memberlist_node_count, 1,
            "Local node should be counted"
        );
    }
}
