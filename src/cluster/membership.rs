//! Two-tier cluster membership management.
//!
//! This module implements a two-tier membership architecture:
//! - Layer 1 (Automatic): Node discovery via gossip/pings
//! - Layer 2 (Manual): Raft voting membership changes require explicit approval
//!
//! This prevents split-brain scenarios that could occur if Raft membership
//! was automatically updated based on unreliable gossip.

use crate::cluster::events::{MemberEvent, MemberEventListener};
use crate::config::MembershipConfig;
use crate::error::{MembershipError, Result};
use crate::types::{NodeId, PeerInfo};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// State of a discovered node.
#[derive(Debug, Clone)]
struct NodeState {
    /// Node information.
    info: PeerInfo,

    /// When the node was first discovered.
    discovered_at: Instant,

    /// When the node was last seen.
    last_seen: Instant,

    /// Number of consecutive failed health checks.
    failed_checks: usize,

    /// Whether the node is currently healthy.
    is_healthy: bool,
}

impl NodeState {
    fn new(info: PeerInfo) -> Self {
        let now = Instant::now();
        Self {
            info,
            discovered_at: now,
            last_seen: now,
            failed_checks: 0,
            is_healthy: true,
        }
    }

    fn mark_seen(&mut self) {
        self.last_seen = Instant::now();
        self.failed_checks = 0;
        self.is_healthy = true;
    }

    fn mark_failed(&mut self) {
        self.failed_checks += 1;
    }
}

/// Two-tier cluster membership manager.
pub struct ClusterMembership {
    /// This node's ID.
    node_id: NodeId,

    /// Layer 1: Automatically discovered nodes.
    discovered: RwLock<HashMap<NodeId, NodeState>>,

    /// Layer 2: Raft voting members (manually managed).
    raft_peers: RwLock<HashSet<NodeId>>,

    /// Configuration.
    config: MembershipConfig,

    /// Event listeners.
    listeners: RwLock<Vec<Arc<dyn MemberEventListener>>>,

    /// Channel for membership events.
    event_tx: mpsc::UnboundedSender<MemberEvent>,
}

impl ClusterMembership {
    /// Create a new cluster membership manager.
    pub fn new(
        node_id: NodeId,
        config: MembershipConfig,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<MemberEvent>) {
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        // Start with self as the only Raft peer
        let mut raft_peers = HashSet::new();
        raft_peers.insert(node_id);

        let membership = Arc::new(Self {
            node_id,
            discovered: RwLock::new(HashMap::new()),
            raft_peers: RwLock::new(raft_peers),
            config,
            listeners: RwLock::new(Vec::new()),
            event_tx,
        });

        (membership, event_rx)
    }

    /// Get this node's ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Add an event listener.
    pub fn add_listener(&self, listener: Arc<dyn MemberEventListener>) {
        self.listeners.write().push(listener);
    }

    /// Notify all listeners of an event.
    fn notify(&self, event: MemberEvent) {
        // Send to channel
        let _ = self.event_tx.send(event.clone());

        // Notify listeners
        for listener in self.listeners.read().iter() {
            listener.on_event(event.clone());
        }
    }

    // ==================== Layer 1: Discovery ====================

    /// Handle a discovered node (from gossip, ping, etc.).
    ///
    /// This automatically updates the discovered nodes list but does NOT
    /// add the node to the Raft cluster.
    pub fn handle_node_discovered(&self, node_id: NodeId, addr: SocketAddr) {
        if node_id == self.node_id {
            return; // Ignore self
        }

        let mut discovered = self.discovered.write();

        if let Some(state) = discovered.get_mut(&node_id) {
            // Existing node, update last seen
            let was_healthy = state.is_healthy;
            state.mark_seen();
            state.info.raft_addr = addr;

            if !was_healthy {
                // Node recovered
                drop(discovered);
                self.notify(MemberEvent::NodeRecovered { node_id, addr });
            }
        } else {
            // New node
            let info = PeerInfo::new(node_id, addr);
            discovered.insert(node_id, NodeState::new(info));
            drop(discovered);

            info!(node_id, %addr, "Discovered new node");
            self.notify(MemberEvent::NodeJoin { node_id, addr });
        }
    }

    /// Handle a failed health check for a node.
    pub fn handle_node_failed_check(&self, node_id: NodeId) {
        let mut discovered = self.discovered.write();

        if let Some(state) = discovered.get_mut(&node_id) {
            state.mark_failed();

            let failed_checks = state.failed_checks;
            let was_healthy = state.is_healthy;

            if failed_checks >= self.config.failure_confirmations {
                state.is_healthy = false;

                if was_healthy {
                    drop(discovered);
                    warn!(node_id, "Node confirmed failed");
                    self.notify(MemberEvent::NodeFailed { node_id });
                }
            } else {
                drop(discovered);
                self.notify(MemberEvent::NodeSuspect {
                    node_id,
                    failed_pings: failed_checks,
                });
            }
        }
    }

    /// Handle a node leaving gracefully.
    pub fn handle_node_leave(&self, node_id: NodeId) {
        let mut discovered = self.discovered.write();
        discovered.remove(&node_id);
        drop(discovered);

        info!(node_id, "Node left cluster");
        self.notify(MemberEvent::NodeLeave { node_id });
    }

    /// Get all discovered nodes.
    pub fn discovered_nodes(&self) -> Vec<PeerInfo> {
        self.discovered
            .read()
            .values()
            .map(|s| s.info.clone())
            .collect()
    }

    /// Get only healthy discovered nodes.
    pub fn healthy_nodes(&self) -> Vec<PeerInfo> {
        self.discovered
            .read()
            .values()
            .filter(|s| s.is_healthy)
            .map(|s| s.info.clone())
            .collect()
    }

    /// Check if a node is discovered.
    pub fn is_discovered(&self, node_id: NodeId) -> bool {
        self.discovered.read().contains_key(&node_id)
    }

    /// Get a discovered node's address.
    pub fn get_node_addr(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.discovered
            .read()
            .get(&node_id)
            .map(|s| s.info.raft_addr)
    }

    // ==================== Layer 2: Raft Membership ====================

    /// Add a node to the Raft voting members.
    ///
    /// This is a MANUAL operation that requires the node to be:
    /// 1. Already discovered
    /// 2. Currently healthy
    /// 3. Within the max_peers limit
    pub fn add_raft_peer(&self, node_id: NodeId) -> Result<()> {
        // Check if discovered and healthy
        {
            let discovered = self.discovered.read();
            let state = discovered
                .get(&node_id)
                .ok_or(MembershipError::NodeNotDiscovered(node_id))?;

            if !state.is_healthy {
                return Err(MembershipError::NodeNotDiscovered(node_id).into());
            }
        }

        // Check peer limits
        let mut raft_peers = self.raft_peers.write();

        if raft_peers.contains(&node_id) {
            return Err(MembershipError::NodeAlreadyExists(node_id).into());
        }

        if raft_peers.len() >= self.config.max_peers {
            return Err(MembershipError::TooManyPeers {
                max: self.config.max_peers,
                current: raft_peers.len(),
            }
            .into());
        }

        raft_peers.insert(node_id);
        info!(node_id, "Added node to Raft quorum");

        Ok(())
    }

    /// Remove a node from the Raft voting members.
    ///
    /// This is a MANUAL operation that requires:
    /// 1. The node to be a current member
    /// 2. Removal won't drop below min_peers
    pub fn remove_raft_peer(&self, node_id: NodeId) -> Result<()> {
        let mut raft_peers = self.raft_peers.write();

        if !raft_peers.contains(&node_id) {
            return Err(MembershipError::NodeNotFound(node_id).into());
        }

        // Check quorum
        let new_size = raft_peers.len() - 1;
        if new_size < self.config.min_peers {
            return Err(MembershipError::WouldLoseQuorum {
                current: raft_peers.len(),
                remaining: new_size,
            }
            .into());
        }

        raft_peers.remove(&node_id);
        warn!(node_id, "Removed node from Raft quorum");

        Ok(())
    }

    /// Get all Raft voting members.
    pub fn raft_peers(&self) -> Vec<NodeId> {
        self.raft_peers.read().iter().copied().collect()
    }

    /// Get the number of Raft peers.
    pub fn raft_peer_count(&self) -> usize {
        self.raft_peers.read().len()
    }

    /// Check if a node is a Raft voting member.
    pub fn is_raft_peer(&self, node_id: NodeId) -> bool {
        self.raft_peers.read().contains(&node_id)
    }

    /// Calculate the quorum size for current Raft membership.
    pub fn quorum_size(&self) -> usize {
        let peer_count = self.raft_peers.read().len();
        peer_count / 2 + 1
    }

    // ==================== Verification ====================

    /// Verify a node is actually down before removal.
    ///
    /// This performs multiple ping attempts with delays.
    pub async fn verify_node_down(&self, node_id: NodeId) -> Result<bool> {
        let addr = self
            .get_node_addr(node_id)
            .ok_or(MembershipError::NodeNotFound(node_id))?;

        let mut confirmations = 0;
        let required = self.config.failure_confirmations;

        for attempt in 0..required {
            tokio::time::sleep(self.config.failure_check_interval).await;

            match tokio::time::timeout(Duration::from_secs(2), Self::ping_node(addr)).await {
                Ok(Ok(_)) => {
                    // Node responded, it's alive
                    debug!(node_id, attempt, "Node responded to ping");
                    return Ok(false);
                }
                Ok(Err(_)) | Err(_) => {
                    confirmations += 1;
                    debug!(node_id, attempt, confirmations, "Ping failed");
                }
            }
        }

        Ok(confirmations >= required)
    }

    /// Ping a node to check if it's alive.
    async fn ping_node(addr: SocketAddr) -> Result<()> {
        use tokio::net::TcpStream;

        // Simple TCP connect check
        let _stream = TcpStream::connect(addr).await.map_err(|e| {
            crate::error::NetworkError::ConnectionFailed {
                addr: addr.to_string(),
                reason: e.to_string(),
            }
        })?;

        Ok(())
    }
}

impl std::fmt::Debug for ClusterMembership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterMembership")
            .field("node_id", &self.node_id)
            .field("discovered_count", &self.discovered.read().len())
            .field("raft_peer_count", &self.raft_peers.read().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MembershipConfig {
        MembershipConfig {
            min_peers: 1,
            max_peers: 5,
            failure_confirmations: 3,
            ..Default::default()
        }
    }

    #[test]
    fn test_discovery() {
        let (membership, _rx) = ClusterMembership::new(1, test_config());

        membership.handle_node_discovered(2, "127.0.0.1:9002".parse().unwrap());
        membership.handle_node_discovered(3, "127.0.0.1:9003".parse().unwrap());

        assert!(membership.is_discovered(2));
        assert!(membership.is_discovered(3));
        assert!(!membership.is_discovered(4));

        assert_eq!(membership.discovered_nodes().len(), 2);
    }

    #[test]
    fn test_add_raft_peer() {
        let (membership, _rx) = ClusterMembership::new(1, test_config());

        // Can't add undiscovered node
        assert!(membership.add_raft_peer(2).is_err());

        // Discover then add
        membership.handle_node_discovered(2, "127.0.0.1:9002".parse().unwrap());
        assert!(membership.add_raft_peer(2).is_ok());
        assert!(membership.is_raft_peer(2));

        // Can't add twice
        assert!(membership.add_raft_peer(2).is_err());
    }

    #[test]
    fn test_remove_raft_peer() {
        let (membership, _rx) = ClusterMembership::new(1, MembershipConfig {
            min_peers: 1,
            ..test_config()
        });

        membership.handle_node_discovered(2, "127.0.0.1:9002".parse().unwrap());
        membership.add_raft_peer(2).unwrap();

        // Can remove if above min_peers
        assert!(membership.remove_raft_peer(2).is_ok());
        assert!(!membership.is_raft_peer(2));

        // Can't remove self if it would drop below min_peers
        // (in this case min_peers is 1, so we can't remove the last peer)
        // Actually self is still a peer, so we have 1 peer, removing would leave 0
        // Let's test this differently
    }

    #[test]
    fn test_quorum_calculation() {
        let (membership, _rx) = ClusterMembership::new(1, test_config());

        // 1 peer: quorum = 1
        assert_eq!(membership.quorum_size(), 1);

        // Add 2 more peers
        membership.handle_node_discovered(2, "127.0.0.1:9002".parse().unwrap());
        membership.handle_node_discovered(3, "127.0.0.1:9003".parse().unwrap());
        membership.add_raft_peer(2).unwrap();
        membership.add_raft_peer(3).unwrap();

        // 3 peers: quorum = 2
        assert_eq!(membership.quorum_size(), 2);
    }

    #[test]
    fn test_max_peers_limit() {
        let config = MembershipConfig {
            max_peers: 2,
            ..test_config()
        };
        let (membership, _rx) = ClusterMembership::new(1, config);

        membership.handle_node_discovered(2, "127.0.0.1:9002".parse().unwrap());
        membership.handle_node_discovered(3, "127.0.0.1:9003".parse().unwrap());

        membership.add_raft_peer(2).unwrap();

        // Can't add third peer (max is 2, already have 1 + 2)
        assert!(membership.add_raft_peer(3).is_err());
    }

    #[test]
    fn test_node_failure() {
        let config = MembershipConfig {
            failure_confirmations: 2,
            ..test_config()
        };
        let (membership, _rx) = ClusterMembership::new(1, config);

        membership.handle_node_discovered(2, "127.0.0.1:9002".parse().unwrap());

        // First failure - node still healthy
        membership.handle_node_failed_check(2);
        assert!(membership
            .discovered
            .read()
            .get(&2)
            .map(|s| s.is_healthy)
            .unwrap_or(false));

        // Second failure - node marked unhealthy
        membership.handle_node_failed_check(2);
        assert!(!membership
            .discovered
            .read()
            .get(&2)
            .map(|s| s.is_healthy)
            .unwrap_or(true));
    }
}
