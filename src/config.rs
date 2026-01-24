//! Configuration types for the distributed cache.

use crate::checkpoint::CheckpointConfig;
use crate::cluster::ClusterDiscovery;
use crate::types::NodeId;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// Main configuration for the distributed cache.
pub struct CacheConfig {
    /// Unique identifier for this node.
    pub node_id: NodeId,

    /// Address to bind for Raft communication.
    pub raft_addr: SocketAddr,

    /// Seed nodes to join the cluster (node_id, address pairs).
    /// Used as fallback when no cluster_discovery is provided.
    pub seed_nodes: Vec<(NodeId, SocketAddr)>,

    /// Maximum number of entries in the cache.
    pub max_capacity: u64,

    /// Default time-to-live for cache entries.
    pub default_ttl: Option<Duration>,

    /// Default time-to-idle for cache entries.
    pub default_tti: Option<Duration>,

    /// Raft-specific configuration.
    pub raft: RaftConfig,

    /// User-provided cluster discovery implementation.
    /// If None, a NoOpClusterDiscovery will be created automatically.
    ///
    /// # Example
    /// ```rust,ignore
    /// use crema::{CacheConfig, NoOpClusterDiscovery, StaticClusterDiscovery};
    ///
    /// // Using NoOp discovery (single node or manual management)
    /// let discovery = NoOpClusterDiscovery::new(1, "127.0.0.1:9000".parse().unwrap());
    /// let config = CacheConfig::new(1, "127.0.0.1:9000".parse().unwrap())
    ///     .with_cluster_discovery(discovery);
    ///
    /// // Using Static discovery (known peer addresses)
    /// let discovery = StaticClusterDiscovery::new(1, addr, static_config);
    /// let config = CacheConfig::new(1, addr)
    ///     .with_cluster_discovery(discovery);
    /// ```
    pub cluster_discovery: Option<Box<dyn ClusterDiscovery + Send>>,

    /// Checkpoint configuration.
    pub checkpoint: CheckpointConfig,

    /// Forwarding configuration for follower-to-leader request routing.
    pub forwarding: ForwardingConfig,

    /// Multi-Raft configuration for horizontal scaling.
    pub multiraft: MultiRaftCacheConfig,
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
            cluster_discovery: None,
            checkpoint: CheckpointConfig::default(),
            forwarding: ForwardingConfig::default(),
            multiraft: MultiRaftCacheConfig::default(),
        }
    }
}

impl std::fmt::Debug for CacheConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheConfig")
            .field("node_id", &self.node_id)
            .field("raft_addr", &self.raft_addr)
            .field("seed_nodes", &self.seed_nodes)
            .field("max_capacity", &self.max_capacity)
            .field("default_ttl", &self.default_ttl)
            .field("default_tti", &self.default_tti)
            .field("raft", &self.raft)
            .field("cluster_discovery", &self.cluster_discovery.is_some())
            .field("checkpoint", &self.checkpoint)
            .field("forwarding", &self.forwarding)
            .field("multiraft", &self.multiraft)
            .finish()
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
    /// Used as fallback when no cluster_discovery is provided.
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

    /// Set the cluster discovery implementation.
    ///
    /// The discovery is responsible for finding and tracking cluster members.
    /// If not set, a NoOpClusterDiscovery will be created automatically.
    ///
    /// # Example
    /// ```rust,ignore
    /// use crema::{CacheConfig, NoOpClusterDiscovery, StaticClusterDiscovery};
    ///
    /// // NoOp discovery (single node)
    /// let discovery = NoOpClusterDiscovery::new(1, addr);
    /// let config = CacheConfig::new(1, addr)
    ///     .with_cluster_discovery(discovery);
    ///
    /// // Static discovery (known peers)
    /// let static_config = StaticDiscoveryConfig::new(peers);
    /// let discovery = StaticClusterDiscovery::new(1, addr, static_config);
    /// let config = CacheConfig::new(1, addr)
    ///     .with_cluster_discovery(discovery);
    /// ```
    pub fn with_cluster_discovery<D: ClusterDiscovery + Send + 'static>(
        mut self,
        discovery: D,
    ) -> Self {
        self.cluster_discovery = Some(Box::new(discovery));
        self
    }

    /// Set the cluster discovery from a boxed trait object.
    pub fn with_cluster_discovery_boxed(mut self, discovery: Box<dyn ClusterDiscovery + Send>) -> Self {
        self.cluster_discovery = Some(discovery);
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

    /// Set forwarding configuration.
    pub fn with_forwarding_config(mut self, forwarding: ForwardingConfig) -> Self {
        self.forwarding = forwarding;
        self
    }

    /// Enable or disable request forwarding.
    pub fn with_forwarding_enabled(mut self, enabled: bool) -> Self {
        self.forwarding.enabled = enabled;
        self
    }

    /// Set forwarding timeout in milliseconds.
    pub fn with_forwarding_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.forwarding.forward_timeout_ms = timeout_ms;
        self
    }

    /// Set Multi-Raft configuration.
    pub fn with_multiraft_config(mut self, multiraft: MultiRaftCacheConfig) -> Self {
        self.multiraft = multiraft;
        self
    }

    /// Enable or disable Multi-Raft mode.
    pub fn with_multiraft_enabled(mut self, enabled: bool) -> Self {
        self.multiraft.enabled = enabled;
        self
    }

    /// Set the number of Multi-Raft shards.
    pub fn with_multiraft_shards(mut self, num_shards: u32) -> Self {
        self.multiraft.num_shards = num_shards;
        self
    }

    /// Validate the configuration and return an error if invalid.
    ///
    /// Checks:
    /// - Multi-Raft shard count is within limits
    pub fn validate(&self) -> Result<(), String> {
        // Validate Multi-Raft config
        if let Some(err) = self.multiraft.validate() {
            return Err(err);
        }

        Ok(())
    }

    /// Check if a cluster discovery has been configured.
    pub fn has_cluster_discovery(&self) -> bool {
        self.cluster_discovery.is_some()
    }

    /// Take ownership of the cluster discovery.
    /// This is used internally by DistributedCache::new().
    pub fn take_cluster_discovery(&mut self) -> Option<Box<dyn ClusterDiscovery + Send>> {
        self.cluster_discovery.take()
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

    /// Storage type for Raft log persistence.
    pub storage_type: RaftStorageType,
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
            storage_type: RaftStorageType::Memory,
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

    /// Create a fast Raft configuration suitable for tests.
    /// Uses shorter tick intervals and election timeouts to speed up test execution.
    pub fn fast_for_tests() -> Self {
        Self {
            tick_interval_ms: 20,   // 20ms per tick (5x faster than default)
            election_tick: 5,       // 5 ticks = 100ms election timeout
            heartbeat_tick: 1,      // 1 tick = 20ms heartbeat
            ..Default::default()
        }
    }

    /// Create a fast test config with staggered election ticks to avoid split votes.
    /// Each node gets a different election_tick based on node_id.
    pub fn fast_for_tests_staggered(node_id: NodeId) -> Self {
        Self {
            tick_interval_ms: 20,
            election_tick: 5 + (node_id as usize * 2), // Stagger: 7, 9, 11, 13...
            heartbeat_tick: 1,
            ..Default::default()
        }
    }
}

/// Storage type for Raft log persistence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftStorageType {
    /// In-memory storage (fast but not durable across restarts).
    Memory,

    /// RocksDB-based persistent storage (durable but requires `rocksdb-storage` feature).
    #[cfg(feature = "rocksdb-storage")]
    RocksDb(RocksDbConfig),
}

impl Default for RaftStorageType {
    fn default() -> Self {
        Self::Memory
    }
}

/// Configuration for RocksDB storage.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg(feature = "rocksdb-storage")]
pub struct RocksDbConfig {
    /// Path to the RocksDB database directory.
    pub path: String,

    /// Whether to sync writes to disk immediately.
    /// Setting this to true provides stronger durability but lower performance.
    pub sync_writes: bool,
}

#[cfg(feature = "rocksdb-storage")]
impl Default for RocksDbConfig {
    fn default() -> Self {
        Self {
            path: "raft-storage".to_string(),
            sync_writes: true,
        }
    }
}

#[cfg(feature = "rocksdb-storage")]
impl RocksDbConfig {
    /// Create a new config with the specified path.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            sync_writes: true,
        }
    }

    /// Set whether to sync writes.
    pub fn with_sync_writes(mut self, sync: bool) -> Self {
        self.sync_writes = sync;
        self
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

/// Configuration for automatic peer management.
///
/// These settings control how the cache automatically manages Raft peers
/// based on cluster discovery events.
#[derive(Debug, Clone)]
pub struct PeerManagementConfig {
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

impl Default for PeerManagementConfig {
    fn default() -> Self {
        Self {
            auto_add_peers: true,
            auto_remove_peers: false, // Conservative default
            auto_add_voters: false,   // Conservative default - requires explicit opt-in
            auto_remove_voters: false, // Conservative default
        }
    }
}

impl PeerManagementConfig {
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
    pub fn with_auto_add_voters(mut self, enabled: bool) -> Self {
        self.auto_add_voters = enabled;
        self
    }

    /// Enable automatic voter removal via Raft ConfChange.
    pub fn with_auto_remove_voters(mut self, enabled: bool) -> Self {
        self.auto_remove_voters = enabled;
        self
    }
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

    /// Peer management configuration.
    pub peer_management: PeerManagementConfig,
}

impl Default for MemberlistConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Disabled by default for backwards compatibility
            bind_addr: None,
            advertise_addr: None,
            seed_addrs: Vec::new(),
            node_name: None,
            peer_management: PeerManagementConfig::default(),
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

    /// Set peer management configuration.
    pub fn with_peer_management(mut self, config: PeerManagementConfig) -> Self {
        self.peer_management = config;
        self
    }

    /// Enable automatic peer addition.
    pub fn with_auto_add_peers(mut self, enabled: bool) -> Self {
        self.peer_management.auto_add_peers = enabled;
        self
    }

    /// Enable automatic peer removal.
    pub fn with_auto_remove_peers(mut self, enabled: bool) -> Self {
        self.peer_management.auto_remove_peers = enabled;
        self
    }

    /// Enable automatic voter addition via Raft ConfChange.
    /// When enabled, the leader will propose ConfChange to add newly discovered nodes as voters.
    pub fn with_auto_add_voters(mut self, enabled: bool) -> Self {
        self.peer_management.auto_add_voters = enabled;
        self
    }

    /// Enable automatic voter removal via Raft ConfChange.
    /// When enabled, the leader will propose ConfChange to remove failed nodes from voters.
    pub fn with_auto_remove_voters(mut self, enabled: bool) -> Self {
        self.peer_management.auto_remove_voters = enabled;
        self
    }

    /// Get the bind address, defaulting to raft_addr port + 1000.
    pub fn get_bind_addr(&self, raft_addr: SocketAddr) -> SocketAddr {
        self.bind_addr.unwrap_or_else(|| {
            SocketAddr::new(raft_addr.ip(), raft_addr.port() + 1000)
        })
    }

    // Backward compatibility accessors
    /// Get auto_add_peers setting.
    pub fn auto_add_peers(&self) -> bool {
        self.peer_management.auto_add_peers
    }

    /// Get auto_remove_peers setting.
    pub fn auto_remove_peers(&self) -> bool {
        self.peer_management.auto_remove_peers
    }

    /// Get auto_add_voters setting.
    pub fn auto_add_voters(&self) -> bool {
        self.peer_management.auto_add_voters
    }

    /// Get auto_remove_voters setting.
    pub fn auto_remove_voters(&self) -> bool {
        self.peer_management.auto_remove_voters
    }
}

/// Configuration for Multi-Raft cache mode.
///
/// When enabled, the cache uses multiple Raft groups (shards) for horizontal scaling.
/// Phase 1 uses gossip-based routing (eventual consistency for shard leader info).
#[derive(Debug, Clone)]
pub struct MultiRaftCacheConfig {
    /// Whether Multi-Raft mode is enabled.
    /// When disabled, the cache uses a single Raft group.
    pub enabled: bool,

    /// Number of shards to partition the keyspace.
    /// Must be between 1 and 64 (Phase 1 limit due to metadata size constraints).
    pub num_shards: u32,

    /// Maximum capacity per shard.
    pub shard_capacity: u64,

    /// Whether to automatically initialize shards on startup.
    pub auto_init_shards: bool,

    /// Debounce interval for broadcasting shard leader updates (milliseconds).
    /// Batches rapid leader changes to reduce gossip overhead.
    pub leader_broadcast_debounce_ms: u64,
}

impl Default for MultiRaftCacheConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            num_shards: 16,
            shard_capacity: 100_000,
            auto_init_shards: true,
            leader_broadcast_debounce_ms: 200,
        }
    }
}

impl MultiRaftCacheConfig {
    /// Create a new Multi-Raft config with the given number of shards.
    pub fn new(num_shards: u32) -> Self {
        Self {
            enabled: true,
            num_shards,
            ..Default::default()
        }
    }

    /// Enable Multi-Raft mode.
    pub fn enabled(mut self) -> Self {
        self.enabled = true;
        self
    }

    /// Set the number of shards.
    pub fn with_num_shards(mut self, num_shards: u32) -> Self {
        self.num_shards = num_shards;
        self
    }

    /// Set the shard capacity.
    pub fn with_shard_capacity(mut self, capacity: u64) -> Self {
        self.shard_capacity = capacity;
        self
    }

    /// Disable automatic shard initialization.
    pub fn with_manual_init(mut self) -> Self {
        self.auto_init_shards = false;
        self
    }

    /// Set the leader broadcast debounce interval.
    pub fn with_leader_broadcast_debounce_ms(mut self, ms: u64) -> Self {
        self.leader_broadcast_debounce_ms = ms;
        self
    }

    /// Get the debounce duration for leader broadcasts.
    pub fn leader_broadcast_debounce(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.leader_broadcast_debounce_ms)
    }

    /// Validate the configuration.
    ///
    /// Returns an error message if the configuration is invalid.
    pub fn validate(&self) -> Option<String> {
        if self.num_shards == 0 {
            return Some("num_shards must be at least 1".to_string());
        }
        if self.num_shards > 64 {
            return Some("Phase 1 supports max 64 shards (metadata size limit)".to_string());
        }
        None
    }
}

/// Configuration for follower-to-leader request forwarding.
#[derive(Debug, Clone)]
pub struct ForwardingConfig {
    /// Whether forwarding is enabled.
    /// When disabled, followers will return NotLeader errors instead of forwarding.
    pub enabled: bool,

    /// Timeout for forwarded requests in milliseconds.
    pub forward_timeout_ms: u64,

    /// Maximum number of pending forwarded requests.
    /// This provides backpressure to prevent memory exhaustion.
    pub max_pending_forwards: usize,
}

impl Default for ForwardingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            forward_timeout_ms: 3000, // 3 seconds
            max_pending_forwards: 5000,
        }
    }
}

impl ForwardingConfig {
    /// Create a new forwarding config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable forwarding.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set the forward timeout in milliseconds.
    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.forward_timeout_ms = timeout_ms;
        self
    }

    /// Set the forward timeout as a Duration.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.forward_timeout_ms = timeout.as_millis() as u64;
        self
    }

    /// Set the maximum number of pending forwards.
    pub fn with_max_pending(mut self, max: usize) -> Self {
        self.max_pending_forwards = max;
        self
    }

    /// Get the forward timeout as a Duration.
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.forward_timeout_ms)
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
        assert!(config.cluster_discovery.is_none());
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

    #[test]
    fn test_multiraft_config_default() {
        let config = MultiRaftCacheConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.num_shards, 16);
        assert_eq!(config.shard_capacity, 100_000);
        assert!(config.auto_init_shards);
        assert_eq!(config.leader_broadcast_debounce_ms, 200);
    }

    #[test]
    fn test_multiraft_config_validation() {
        // Valid config
        let config = MultiRaftCacheConfig::new(16);
        assert!(config.validate().is_none());

        // Zero shards is invalid
        let config = MultiRaftCacheConfig {
            num_shards: 0,
            ..Default::default()
        };
        assert!(config.validate().is_some());

        // More than 64 shards is invalid (Phase 1 limit)
        let config = MultiRaftCacheConfig {
            num_shards: 65,
            ..Default::default()
        };
        assert!(config.validate().is_some());

        // Max 64 shards is valid
        let config = MultiRaftCacheConfig {
            num_shards: 64,
            ..Default::default()
        };
        assert!(config.validate().is_none());
    }

    #[test]
    fn test_multiraft_config_builder() {
        let config = MultiRaftCacheConfig::new(32)
            .with_shard_capacity(50_000)
            .with_manual_init()
            .with_leader_broadcast_debounce_ms(500);

        assert!(config.enabled);
        assert_eq!(config.num_shards, 32);
        assert_eq!(config.shard_capacity, 50_000);
        assert!(!config.auto_init_shards);
        assert_eq!(config.leader_broadcast_debounce_ms, 500);
        assert_eq!(
            config.leader_broadcast_debounce(),
            Duration::from_millis(500)
        );
    }
}
