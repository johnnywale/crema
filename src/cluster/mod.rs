//! Cluster membership and discovery.

pub mod events;
pub mod membership;
pub mod memberlist_cluster;

pub use events::{MemberEvent, MemberEventListener};
pub use membership::ClusterMembership;
pub use memberlist_cluster::{
    MemberlistCluster, MemberlistClusterConfig, MemberlistError, MemberlistEvent, NodeRegistry,
    RaftNodeMetadata,
};
