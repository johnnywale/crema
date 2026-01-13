//! Configuration types for the distributed cache.

use crate::checkpoint::CheckpointConfig;
use crate::types::NodeId;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// Main configuration for the distributed cache.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Unique identifier for this node.
    pub node_id: NodeId,

    /// Address to bind for Raft communication.
    pub raft_addr: SocketAddr,

    /// Seed nodes to join the cluster (node_id, address pairs).
    pub seed_nodes: Vec<(NodeId, SocketAddr)>,

    /// Maximum number of entries in the cache.
    pub max_capacity: u64,

    /// Default time-to-live for cache entries.
    pub default_ttl: Option<Duration>,

    /// Default time-to-idle for cache entries.
    pub default_tti: Option<Duration>,

    /// Raft-specific configuration.
    pub raft: RaftConfig,

    /// Membership configuration.
    pub membership: MembershipConfig,

    /// Memberlist gossip configuration.
    pub memberlist: MemberlistConfig,

    /// Checkpoint configuration.
    pub checkpoint: CheckpointConfig,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            raft_addr: "127.0.0.1:9000".parse().unwrap(),
            seed_nodes: Vec::new(),
            max_capacity: 100_000,
            default_ttl: Some(Duration::from_secs(3600)), // 1 hour
            default_tti: None,
            raft: RaftConfig::default(),
            membership: MembershipConfig::default(),
            memberlist: MemberlistConfig::default(),
            checkpoint: CheckpointConfig::default(),
        }
    }
}

impl CacheConfig {
    /// Create a new configuration with the given node ID and address.
    pub fn new(node_id: NodeId, raft_addr: SocketAddr) -> Self {
        Self {
            node_id,
            raft_addr,
            ..Default::default()
        }
    }

    /// Set seed nodes for cluster discovery.
    /// Each seed node is a (node_id, address) pair.
    pub fn with_seed_nodes(mut self, seeds: Vec<(NodeId, SocketAddr)>) -> Self {
        self.seed_nodes = seeds;
        self
    }

    /// Set maximum cache capacity.
    pub fn with_max_capacity(mut self, capacity: u64) -> Self {
        self.max_capacity = capacity;
        self
    }

    /// Set default TTL for cache entries.
    pub fn with_default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = Some(ttl);
        self
    }

    /// Set default TTI for cache entries.
    pub fn with_default_tti(mut self, tti: Duration) -> Self {
        self.default_tti = Some(tti);
        self
    }

    /// Set Raft configuration.
    pub fn with_raft_config(mut self, raft: RaftConfig) -> Self {
        self.raft = raft;
        self
    }

    /// Set membership configuration.
    pub fn with_membership_config(mut self, membership: MembershipConfig) -> Self {
        self.membership = membership;
        self
    }

    /// Set checkpoint configuration.
    pub fn with_checkpoint_config(mut self, checkpoint: CheckpointConfig) -> Self {
        self.checkpoint = checkpoint;
        self
    }

    /// Set checkpoint directory.
    pub fn with_checkpoint_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.checkpoint.dir = dir.into();
        self
    }

    /// Enable or disable checkpointing.
    pub fn with_checkpointing_enabled(mut self, enabled: bool) -> Self {
        self.checkpoint.enabled = enabled;
        self
    }

    /// Set memberlist configuration.
    pub fn with_memberlist_config(mut self, memberlist: MemberlistConfig) -> Self {
        self.memberlist = memberlist;
        self
    }

    /// Enable or disable memberlist gossip.
    pub fn with_memberlist_enabled(mut self, enabled: bool) -> Self {
        self.memberlist.enabled = enabled;
        self
    }

    /// Set memberlist bind address.
    pub fn with_memberlist_addr(mut self, addr: SocketAddr) -> Self {
        self.memberlist.bind_addr = Some(addr);
        self
    }
}

/// Raft consensus configuration.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// Election timeout range in milliseconds.
    /// The actual timeout is randomly chosen in [min, max].
    pub election_tick: usize,

    /// Heartbeat interval in ticks.
    pub heartbeat_tick: usize,

    /// Tick interval in milliseconds.
    pub tick_interval_ms: u64,

    /// Maximum size of entries in a single append message.
    pub max_size_per_msg: u64,

    /// Maximum number of inflight append messages.
    pub max_inflight_msgs: usize,

    /// Whether to enable pre-vote.
    pub pre_vote: bool,

    /// Applied index to start from (for recovery).
    pub applied: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_tick: 10,      // 10 ticks = 1 second with 100ms tick
            heartbeat_tick: 3,      // 3 ticks = 300ms
            tick_interval_ms: 100,  // 100ms per tick
            max_size_per_msg: 1024 * 1024, // 1MB
            max_inflight_msgs: 256,
            pre_vote: true,
            applied: 0,
        }
    }
}

impl RaftConfig {
    /// Convert to raft-rs Config.
    pub fn to_raft_config(&self, id: NodeId) -> raft::Config {
        raft::Config {
            id,
            election_tick: self.election_tick,
            heartbeat_tick: self.heartbeat_tick,
            max_size_per_msg: self.max_size_per_msg,
            max_inflight_msgs: self.max_inflight_msgs,
            pre_vote: self.pre_vote,
            applied: self.applied,
            ..Default::default()
        }
    }
}

/// Cluster membership configuration.
#[derive(Debug, Clone)]
pub struct MembershipConfig {
    /// Membership mode for Raft peers.
    pub mode: MembershipMode,

    /// Minimum number of Raft peers (for quorum).
    pub min_peers: usize,

    /// Maximum number of Raft peers.
    pub max_peers: usize,

    /// Number of confirmations needed before removing a failed node.
    pub failure_confirmations: usize,

    /// Interval between failure verification attempts.
    pub failure_check_interval: Duration,

    /// Automatically remove nodes after this duration of being down.
    /// None means never auto-remove.
    pub auto_remove_after: Option<Duration>,
}

impl Default for MembershipConfig {
    fn default() -> Self {
        Self {
            mode: MembershipMode::Manual,
            min_peers: 1,
            max_peers: 7,
            failure_confirmations: 3,
            failure_check_interval: Duration::from_secs(5),
            auto_remove_after: None,
        }
    }
}

/// Mode for managing Raft membership changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipMode {
    /// Require manual approval for all membership changes.
    Manual,

    /// Automatically add nodes after health check, but require manual removal.
    SemiAutomatic,

    /// Fully automatic membership management (dangerous, not recommended).
    Automatic,
}

/// Memberlist gossip configuration.
#[derive(Debug, Clone)]
pub struct MemberlistConfig {
    /// Whether memberlist is enabled.
    pub enabled: bool,

    /// Address to bind for memberlist gossip.
    /// If None, uses raft_addr port + 1000.
    pub bind_addr: Option<SocketAddr>,

    /// Address to advertise to other nodes.
    /// If None, uses bind_addr.
    pub advertise_addr: Option<SocketAddr>,

    /// Seed nodes for memberlist gossip (addresses only, not node IDs).
    /// These can be different from Raft seed nodes.
    pub seed_addrs: Vec<SocketAddr>,

    /// Custom node name (defaults to "node-{node_id}").
    pub node_name: Option<String>,

    /// Whether to automatically add discovered peers to Raft transport.
    pub auto_add_peers: bool,

    /// Whether to automatically remove failed peers from Raft transport.
    pub auto_remove_peers: bool,

    /// Whether to automatically propose ConfChange to add discovered nodes as Raft voters.
    /// Only the leader will propose ConfChange. Requires auto_add_peers to be true.
    pub auto_add_voters: bool,

    /// Whether to automatically propose ConfChange to remove failed nodes from Raft voters.
    /// Only the leader will propose ConfChange. Requires auto_remove_peers to be true.
    pub auto_remove_voters: bool,
}

impl Default for MemberlistConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Disabled by default for backwards compatibility
            bind_addr: None,
            advertise_addr: None,
            seed_addrs: Vec::new(),
            node_name: None,
            auto_add_peers: true,
            auto_remove_peers: false, // Conservative default
            auto_add_voters: false,   // Conservative default - requires explicit opt-in
            auto_remove_voters: false, // Conservative default
        }
    }
}

impl MemberlistConfig {
    /// Create a new memberlist config with the given bind address.
    pub fn new(bind_addr: SocketAddr) -> Self {
        Self {
            enabled: true,
            bind_addr: Some(bind_addr),
            ..Default::default()
        }
    }

    /// Enable memberlist.
    pub fn enabled(mut self) -> Self {
        self.enabled = true;
        self
    }

    /// Set seed addresses for memberlist gossip.
    pub fn with_seed_addrs(mut self, addrs: Vec<SocketAddr>) -> Self {
        self.seed_addrs = addrs;
        self
    }

    /// Set advertise address.
    pub fn with_advertise_addr(mut self, addr: SocketAddr) -> Self {
        self.advertise_addr = Some(addr);
        self
    }

    /// Set custom node name.
    pub fn with_node_name(mut self, name: String) -> Self {
        self.node_name = Some(name);
        self
    }

    /// Enable automatic peer addition.
    pub fn with_auto_add_peers(mut self, enabled: bool) -> Self {
        self.auto_add_peers = enabled;
        self
    }

    /// Enable automatic peer removal.
    pub fn with_auto_remove_peers(mut self, enabled: bool) -> Self {
        self.auto_remove_peers = enabled;
        self
    }

    /// Enable automatic voter addition via Raft ConfChange.
    /// When enabled, the leader will propose ConfChange to add newly discovered nodes as voters.
    pub fn with_auto_add_voters(mut self, enabled: bool) -> Self {
        self.auto_add_voters = enabled;
        self
    }

    /// Enable automatic voter removal via Raft ConfChange.
    /// When enabled, the leader will propose ConfChange to remove failed nodes from voters.
    pub fn with_auto_remove_voters(mut self, enabled: bool) -> Self {
        self.auto_remove_voters = enabled;
        self
    }

    /// Get the bind address, defaulting to raft_addr port + 1000.
    pub fn get_bind_addr(&self, raft_addr: SocketAddr) -> SocketAddr {
        self.bind_addr.unwrap_or_else(|| {
            SocketAddr::new(raft_addr.ip(), raft_addr.port() + 1000)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = CacheConfig::default();
        assert_eq!(config.node_id, 1);
        assert_eq!(config.max_capacity, 100_000);
        assert!(config.default_ttl.is_some());
    }

    #[test]
    fn test_config_builder() {
        let config = CacheConfig::new(42, "127.0.0.1:9000".parse().unwrap())
            .with_max_capacity(1_000_000)
            .with_default_ttl(Duration::from_secs(300));

        assert_eq!(config.node_id, 42);
        assert_eq!(config.max_capacity, 1_000_000);
        assert_eq!(config.default_ttl, Some(Duration::from_secs(300)));
    }
}
