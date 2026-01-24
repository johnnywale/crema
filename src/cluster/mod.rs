//! Cluster membership and discovery.
//!
//! This module provides abstractions for cluster discovery and membership management.
//!
//! # Architecture
//!
//! The `ClusterDiscovery` trait abstracts cluster membership protocols, allowing
//! different implementations (memberlist, static, etcd, Consul, etc.) to be used interchangeably.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    DistributedCache                              │
//! │                                                                  │
//! │   discovery: Box<dyn ClusterDiscovery>                          │
//! │                                                                  │
//! └─────────────────────────────────────────────────────────────────┘
//!                               │
//!                               ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                  ClusterDiscovery Trait                          │
//! └─────────────────────────────────────────────────────────────────┘
//!       │              │              │              │
//!       ▼              ▼              ▼              ▼
//! ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐
//! │Memberlist│  │ Static   │  │  NoOp    │  │Future: etcd, │
//! │Discovery │  │Discovery │  │Discovery │  │Consul, etc.  │
//! └──────────┘  └──────────┘  └──────────┘  └──────────────┘
//! ```
//!
//! # Implementations
//!
//! - **MemberlistDiscovery** (requires `memberlist` feature): Gossip-based discovery using SWIM protocol.
//!   Best for dynamic clusters where nodes join/leave frequently.
//!
//! - **StaticClusterDiscovery**: Uses a fixed list of node addresses.
//!   Best for deployments with known addresses (Kubernetes, Docker Compose, etc.).
//!
//! - **NoOpClusterDiscovery**: No-op implementation for single-node or manual management.
//!
//! # Feature Flags
//!
//! - `memberlist` (default): Enables memberlist-based gossip discovery.
//!   When disabled, only the trait, static, and no-op implementations are available.

pub mod discovery;
pub mod events;
pub mod membership;

#[cfg(feature = "memberlist")]
pub mod memberlist_cluster;

#[cfg(feature = "memberlist")]
pub mod memberlist_discovery;

// Re-export the discovery trait and types (always available)
pub use discovery::{
    ClusterDiscovery, ClusterDiscoveryError, ClusterEvent, NodeMetadata, NodeRegistry,
    NoOpClusterDiscovery, ShardLeaderInfo, ShardLeaderMetadata,
    // Static discovery (always available)
    StaticClusterDiscovery, StaticDiscoveryConfig,
};
pub use events::{MemberEvent, MemberEventListener};
pub use membership::ClusterMembership;

// Re-export memberlist implementation (only when feature enabled)
#[cfg(feature = "memberlist")]
pub use memberlist_cluster::{
    MemberlistCluster, MemberlistClusterConfig, MemberlistError, MemberlistEvent,
    NodeRegistry as MemberlistNodeRegistry, RaftNodeMetadata,
};

#[cfg(feature = "memberlist")]
pub use memberlist_discovery::MemberlistDiscovery;


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_discovery() {
        let discovery = NoOpClusterDiscovery::new(1, "127.0.0.1:9000".parse().unwrap());
        assert!(!discovery.is_active());
        assert_eq!(discovery.node_id(), 1);
    }

    #[test]
    fn test_static_discovery_config_builder() {
        let config = StaticDiscoveryConfig::new(vec![
            (1, "127.0.0.1:9000".parse().unwrap()),
        ])
        .with_peer(2, "127.0.0.1:9001".parse().unwrap())
        .with_health_check_interval(std::time::Duration::from_secs(10))
        .with_health_check_timeout(std::time::Duration::from_secs(5));

        assert_eq!(config.peers.len(), 2);
        assert_eq!(config.health_check_interval, std::time::Duration::from_secs(10));
        assert_eq!(config.health_check_timeout, std::time::Duration::from_secs(5));
        assert!(config.health_check_enabled);
    }

    #[test]
    fn test_static_discovery_config_from_addrs() {
        let addrs = vec![
            "127.0.0.1:9000".parse().unwrap(),
            "127.0.0.1:9001".parse().unwrap(),
            "127.0.0.1:9002".parse().unwrap(),
        ];

        let config = StaticDiscoveryConfig::from_addrs(addrs);

        assert_eq!(config.peers.len(), 3);
        assert_eq!(config.peers[0].0, 1); // Auto-assigned ID
        assert_eq!(config.peers[1].0, 2);
        assert_eq!(config.peers[2].0, 3);
    }

    #[tokio::test]
    async fn test_static_discovery_start_and_members() {
        let peers = vec![
            (1, "127.0.0.1:9000".parse().unwrap()),
            (2, "127.0.0.1:9001".parse().unwrap()),
            (3, "127.0.0.1:9002".parse().unwrap()),
        ];

        // Disable health checks to avoid actual TCP connections in tests
        let config = StaticDiscoveryConfig::new(peers).without_health_checks();
        let mut discovery =
            StaticClusterDiscovery::new(1, "127.0.0.1:9000".parse().unwrap(), config);

        assert!(!discovery.is_initialized());

        discovery.start().await.unwrap();

        assert!(discovery.is_initialized());
        assert_eq!(discovery.members().len(), 3);
        assert_eq!(discovery.healthy_members().len(), 3);

        // Should have received join events for peers 2 and 3
        let event1 = discovery.try_recv_event();
        assert!(matches!(event1, Some(ClusterEvent::NodeJoin { node_id: 2, .. })));

        let event2 = discovery.try_recv_event();
        assert!(matches!(event2, Some(ClusterEvent::NodeJoin { node_id: 3, .. })));

        discovery.shutdown().await.unwrap();
        assert!(!discovery.is_initialized());
    }

    #[tokio::test]
    async fn test_static_discovery_add_remove_peer() {
        let config = StaticDiscoveryConfig::new(vec![
            (1, "127.0.0.1:9000".parse().unwrap()),
        ])
        .without_health_checks();

        let mut discovery =
            StaticClusterDiscovery::new(1, "127.0.0.1:9000".parse().unwrap(), config);
        discovery.start().await.unwrap();

        assert_eq!(discovery.members().len(), 1);

        // Add a peer dynamically
        discovery.add_peer(2, "127.0.0.1:9001".parse().unwrap());
        assert_eq!(discovery.members().len(), 2);

        let event = discovery.try_recv_event();
        assert!(matches!(event, Some(ClusterEvent::NodeJoin { node_id: 2, .. })));

        // Remove the peer
        discovery.remove_peer(2);
        assert_eq!(discovery.members().len(), 1);

        let event = discovery.try_recv_event();
        assert!(matches!(event, Some(ClusterEvent::NodeLeave { node_id: 2 })));
    }

    #[tokio::test]
    async fn test_static_discovery_get_node_addr() {
        let peers = vec![
            (1, "127.0.0.1:9000".parse().unwrap()),
            (2, "127.0.0.1:9001".parse().unwrap()),
        ];

        let config = StaticDiscoveryConfig::new(peers).without_health_checks();
        let mut discovery =
            StaticClusterDiscovery::new(1, "127.0.0.1:9000".parse().unwrap(), config);
        discovery.start().await.unwrap();

        assert_eq!(
            discovery.get_node_addr(1),
            Some("127.0.0.1:9000".parse().unwrap())
        );
        assert_eq!(
            discovery.get_node_addr(2),
            Some("127.0.0.1:9001".parse().unwrap())
        );
        assert_eq!(discovery.get_node_addr(99), None);
    }
}
