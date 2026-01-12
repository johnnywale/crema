//! Distributed cache implementation.

pub mod storage;

use crate::cluster::memberlist_cluster::{
    MemberlistCluster, MemberlistClusterConfig, MemberlistEvent,
};
use crate::cluster::ClusterMembership;
use crate::config::CacheConfig;
use crate::consensus::{CacheStateMachine, RaftNode};
use crate::error::Result;
use crate::network::{Message, MessageHandler, NetworkServer};
use crate::types::{CacheCommand, CacheStats, ClusterStatus, NodeId};
use bytes::Bytes;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use storage::CacheStorage;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// The main distributed cache instance.
///
/// This provides a strongly consistent distributed cache backed by Raft consensus.
/// All write operations go through the Raft leader, while reads can be served locally.
pub struct DistributedCache {
    /// Local cache storage.
    storage: Arc<CacheStorage>,

    /// Raft consensus node.
    raft: Arc<RaftNode>,

    /// Cluster membership manager.
    membership: Arc<ClusterMembership>,

    /// Memberlist cluster for gossip-based discovery (optional).
    memberlist: Option<Arc<Mutex<MemberlistCluster>>>,

    /// Configuration.
    config: CacheConfig,

    /// Network server shutdown signal sender.
    shutdown_tx: mpsc::Sender<()>,

    /// Raft tick loop shutdown sender.
    tick_shutdown_tx: mpsc::Sender<()>,

    /// Memberlist event loop shutdown sender.
    memberlist_shutdown_tx: Option<mpsc::Sender<()>>,
}

impl DistributedCache {
    /// Create a new distributed cache instance.
    ///
    /// This will:
    /// 1. Initialize the local Moka cache
    /// 2. Set up the Raft consensus layer
    /// 3. Start the network server
    /// 4. Begin the Raft tick loop
    /// 5. Start memberlist gossip (if enabled)
    pub async fn new(config: CacheConfig) -> Result<Self> {
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

        // Create Raft node
        let raft = RaftNode::new(
            config.node_id,
            initial_peers.clone(),
            config.raft.clone(),
            state_machine,
        )?;

        // Add peers to transport using their actual node IDs
        for (peer_id, addr) in &config.seed_nodes {
            if *peer_id != config.node_id {
                raft.transport().add_peer(*peer_id, *addr);
            }
        }

        // Create membership manager
        let (membership, _event_rx) =
            ClusterMembership::new(config.node_id, config.membership.clone());

        // Create message handler
        let handler = CacheMessageHandler {
            raft: raft.clone(),
        };

        // Create and start network server
        let (server, shutdown_tx) = NetworkServer::new(
            config.raft_addr,
            config.node_id,
            Arc::new(handler),
        );

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

        // Start memberlist if enabled
        let (memberlist, memberlist_shutdown_tx) = if config.memberlist.enabled {
            let memberlist_bind_addr = config.memberlist.get_bind_addr(config.raft_addr);

            // Build memberlist config
            let mut ml_config =
                MemberlistClusterConfig::new(config.node_id, memberlist_bind_addr, config.raft_addr);

            // Add seed addresses from config
            if !config.memberlist.seed_addrs.is_empty() {
                ml_config = ml_config.with_seed_nodes(config.memberlist.seed_addrs.clone());
            } else {
                // Fall back to seed_nodes addresses if no memberlist-specific seeds
                let seed_addrs: Vec<_> = config
                    .seed_nodes
                    .iter()
                    .map(|(_, addr)| {
                        // Convert raft addr to memberlist addr (port + 1000)
                        std::net::SocketAddr::new(addr.ip(), addr.port() + 1000)
                    })
                    .collect();
                if !seed_addrs.is_empty() {
                    ml_config = ml_config.with_seed_nodes(seed_addrs);
                }
            }

            if let Some(advertise) = config.memberlist.advertise_addr {
                ml_config = ml_config.with_advertise_addr(advertise);
            }

            if let Some(ref name) = config.memberlist.node_name {
                ml_config = ml_config.with_node_name(name.clone());
            }

            // Create and start memberlist
            let mut cluster = MemberlistCluster::new(ml_config);

            match cluster.start().await {
                Ok(()) => {
                    info!(
                        node_id = config.node_id,
                        bind_addr = %memberlist_bind_addr,
                        "Memberlist gossip started"
                    );

                    let memberlist = Arc::new(Mutex::new(cluster));

                    // Start event processing loop
                    let (ml_shutdown_tx, ml_shutdown_rx) = mpsc::channel(1);
                    let raft_for_events = raft.clone();
                    let ml_for_events = memberlist.clone();
                    let auto_add = config.memberlist.auto_add_peers;
                    let auto_remove = config.memberlist.auto_remove_peers;

                    tokio::spawn(async move {
                        Self::run_memberlist_event_loop(
                            ml_for_events,
                            raft_for_events,
                            ml_shutdown_rx,
                            auto_add,
                            auto_remove,
                        )
                        .await;
                    });

                    (Some(memberlist), Some(ml_shutdown_tx))
                }
                Err(e) => {
                    warn!(error = %e, "Failed to start memberlist, continuing without gossip");
                    (None, None)
                }
            }
        } else {
            (None, None)
        };

        info!(node_id = config.node_id, "Distributed cache started");

        Ok(Self {
            storage,
            raft,
            membership,
            memberlist,
            config,
            shutdown_tx,
            tick_shutdown_tx,
            memberlist_shutdown_tx,
        })
    }

    /// Run the memberlist event processing loop.
    ///
    /// This handles events from memberlist (node joins, leaves, failures) and
    /// updates the Raft transport accordingly.
    async fn run_memberlist_event_loop(
        memberlist: Arc<Mutex<MemberlistCluster>>,
        raft: Arc<RaftNode>,
        mut shutdown_rx: mpsc::Receiver<()>,
        auto_add_peers: bool,
        auto_remove_peers: bool,
    ) {
        info!("Starting memberlist event processing loop");

        loop {
            // Try to receive event with timeout
            let event = {
                let mut ml = memberlist.lock();
                ml.try_recv_event()
            };

            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Memberlist event loop shutting down");
                    break;
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Process any pending event
                    if let Some(event) = event {
                        Self::handle_memberlist_event(
                            &event,
                            &raft,
                            auto_add_peers,
                            auto_remove_peers,
                        );
                    }
                }
            }
        }
    }

    /// Handle a single memberlist event.
    fn handle_memberlist_event(
        event: &MemberlistEvent,
        raft: &Arc<RaftNode>,
        auto_add_peers: bool,
        auto_remove_peers: bool,
    ) {
        match event {
            MemberlistEvent::NodeJoin {
                raft_id,
                raft_addr,
                metadata: _,
            } => {
                info!(
                    raft_id = *raft_id,
                    raft_addr = %raft_addr,
                    "Node discovered via memberlist"
                );

                if auto_add_peers {
                    // Add to Raft transport so we can communicate
                    raft.transport().add_peer(*raft_id, *raft_addr);
                    debug!(raft_id = *raft_id, "Added peer to Raft transport");
                }
            }

            MemberlistEvent::NodeLeave { raft_id } => {
                info!(raft_id = *raft_id, "Node left via memberlist");

                if auto_remove_peers {
                    // Remove from Raft transport
                    raft.transport().remove_peer(*raft_id);
                    debug!(raft_id = *raft_id, "Removed peer from Raft transport");
                }
            }

            MemberlistEvent::NodeFailed { raft_id } => {
                warn!(raft_id = *raft_id, "Node failed via memberlist");

                if auto_remove_peers {
                    // Remove from Raft transport
                    raft.transport().remove_peer(*raft_id);
                    debug!(raft_id = *raft_id, "Removed failed peer from Raft transport");
                }
            }

            MemberlistEvent::NodeUpdate {
                raft_id,
                metadata,
            } => {
                debug!(
                    raft_id = *raft_id,
                    raft_addr = %metadata.raft_addr,
                    "Node metadata updated via memberlist"
                );

                // Update address in case it changed
                if auto_add_peers {
                    raft.transport().add_peer(*raft_id, metadata.raft_addr);
                }
            }
        }
    }

    // ==================== Read Operations ====================

    /// Get a value from the local cache.
    ///
    /// This reads directly from the local Moka cache. On followers, this may
    /// return stale data. For strongly consistent reads, use `get_consistent`.
    pub async fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.storage.get(key).await
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
    /// to all nodes in the cluster.
    pub async fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        let key = key.into();
        let value = value.into();

        let command = CacheCommand::put(key.to_vec(), value.to_vec());
        self.raft.propose(command).await?;

        Ok(())
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

        let command = CacheCommand::put_with_ttl(key.to_vec(), value.to_vec(), ttl);
        self.raft.propose(command).await?;

        Ok(())
    }

    /// Delete a key from the cache.
    pub async fn delete(&self, key: impl Into<Bytes>) -> Result<()> {
        let key = key.into();

        let command = CacheCommand::delete(key.to_vec());
        self.raft.propose(command).await?;

        Ok(())
    }

    /// Clear all entries from the cache.
    pub async fn clear(&self) -> Result<()> {
        let command = CacheCommand::clear();
        self.raft.propose(command).await?;

        Ok(())
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

    // ==================== Memberlist ====================

    /// Check if memberlist gossip is enabled and running.
    pub fn memberlist_enabled(&self) -> bool {
        self.memberlist.is_some()
    }

    /// Get all nodes discovered via memberlist.
    pub fn memberlist_members(&self) -> Vec<NodeId> {
        self.memberlist
            .as_ref()
            .map(|ml| ml.lock().members())
            .unwrap_or_default()
    }

    /// Get healthy nodes discovered via memberlist.
    pub fn memberlist_healthy_members(&self) -> Vec<NodeId> {
        self.memberlist
            .as_ref()
            .map(|ml| ml.lock().healthy_members())
            .unwrap_or_default()
    }

    // ==================== Lifecycle ====================

    /// Shutdown the distributed cache.
    pub async fn shutdown(&self) {
        info!("Shutting down distributed cache");

        // Shutdown memberlist event loop first
        if let Some(ref tx) = self.memberlist_shutdown_tx {
            let _ = tx.send(()).await;
        }

        // Leave memberlist gracefully
        if let Some(ref ml) = self.memberlist {
            let mut ml = ml.lock();
            if let Err(e) = ml.leave().await {
                warn!(error = %e, "Error leaving memberlist");
            }
            if let Err(e) = ml.shutdown().await {
                warn!(error = %e, "Error shutting down memberlist");
            }
        }

        // Shutdown Raft tick loop
        let _ = self.tick_shutdown_tx.send(()).await;

        // Shutdown network server
        let _ = self.shutdown_tx.send(()).await;
    }
}

/// Message handler that routes messages to the Raft node.
struct CacheMessageHandler {
    raft: Arc<RaftNode>,
}

impl MessageHandler for CacheMessageHandler {
    fn handle(&self, msg: Message) -> Option<Message> {
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
            raft_addr: format!("127.0.0.1:{}", 19000 + node_id)
                .parse()
                .unwrap(),
            ..Default::default()
        }
    }

    fn test_config_with_peers(node_id: NodeId, seed_nodes: Vec<(NodeId, SocketAddr)>) -> CacheConfig {
        CacheConfig {
            node_id,
            raft_addr: format!("127.0.0.1:{}", 19000 + node_id)
                .parse()
                .unwrap(),
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
        assert!(peer2_addr.is_none(), "Self (peer 2) should not be registered");
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
        assert!(peer1_addr.is_none(), "Peer 1 should NOT be registered (bug regression)");
        let peer2_addr = transport.get_peer(2);
        assert!(peer2_addr.is_none(), "Peer 2 should NOT be registered (bug regression)");
    }

    #[tokio::test]
    async fn test_peer_registration_empty_seed_nodes() {
        // Test single-node cluster with no seed nodes
        let config = test_config(1);
        let cache = DistributedCache::new(config).await.unwrap();

        let transport = cache.raft.transport();

        // No peers should be registered
        let peer_ids = transport.peer_ids();
        assert!(peer_ids.is_empty(), "No peers should be registered for single-node cluster");
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
        // Memberlist should be disabled by default
        let config = test_config(1);
        let cache = DistributedCache::new(config).await.unwrap();

        assert!(!cache.memberlist_enabled(), "Memberlist should be disabled by default");
        assert!(cache.memberlist_members().is_empty(), "No memberlist members when disabled");
    }

    #[tokio::test]
    async fn test_memberlist_config_fields() {
        // Test that memberlist config fields are properly initialized
        let config = crate::config::MemberlistConfig::default();

        assert!(!config.enabled);
        assert!(config.bind_addr.is_none());
        assert!(config.advertise_addr.is_none());
        assert!(config.seed_addrs.is_empty());
        assert!(config.auto_add_peers);
        assert!(!config.auto_remove_peers);
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
        // Test that cluster status includes memberlist node count
        let config = test_config(1);
        let cache = DistributedCache::new(config).await.unwrap();

        let status = cache.cluster_status();

        assert_eq!(status.memberlist_node_count, 0);
    }
}
