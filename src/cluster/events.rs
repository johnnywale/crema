//! Cluster membership events.

use crate::types::NodeId;
use std::net::SocketAddr;

/// Events related to cluster membership changes.
#[derive(Debug, Clone)]
pub enum MemberEvent {
    /// A new node joined the cluster (discovered via gossip).
    NodeJoin {
        /// The node's ID.
        node_id: NodeId,
        /// The node's Raft address.
        addr: SocketAddr,
    },

    /// A node left the cluster gracefully.
    NodeLeave {
        /// The node's ID.
        node_id: NodeId,
    },

    /// A node is suspected to have failed.
    NodeSuspect {
        /// The node's ID.
        node_id: NodeId,
        /// Number of failed ping attempts.
        failed_pings: usize,
    },

    /// A node has been confirmed as failed.
    NodeFailed {
        /// The node's ID.
        node_id: NodeId,
    },

    /// A previously failed node has recovered.
    NodeRecovered {
        /// The node's ID.
        node_id: NodeId,
        /// The node's Raft address.
        addr: SocketAddr,
    },

    /// A node's metadata was updated.
    NodeUpdated {
        /// The node's ID.
        node_id: NodeId,
        /// The new address.
        addr: SocketAddr,
    },
}

impl MemberEvent {
    /// Get the node ID associated with this event.
    pub fn node_id(&self) -> NodeId {
        match self {
            MemberEvent::NodeJoin { node_id, .. } => *node_id,
            MemberEvent::NodeLeave { node_id } => *node_id,
            MemberEvent::NodeSuspect { node_id, .. } => *node_id,
            MemberEvent::NodeFailed { node_id } => *node_id,
            MemberEvent::NodeRecovered { node_id, .. } => *node_id,
            MemberEvent::NodeUpdated { node_id, .. } => *node_id,
        }
    }

    /// Check if this is a join-type event.
    pub fn is_join(&self) -> bool {
        matches!(
            self,
            MemberEvent::NodeJoin { .. } | MemberEvent::NodeRecovered { .. }
        )
    }

    /// Check if this is a leave-type event.
    pub fn is_leave(&self) -> bool {
        matches!(
            self,
            MemberEvent::NodeLeave { .. } | MemberEvent::NodeFailed { .. }
        )
    }
}

/// Listener for membership events.
pub trait MemberEventListener: Send + Sync + 'static {
    /// Called when a membership event occurs.
    fn on_event(&self, event: MemberEvent);
}

/// No-op event listener.
pub struct NoopEventListener;

impl MemberEventListener for NoopEventListener {
    fn on_event(&self, _event: MemberEvent) {}
}

/// Event listener that logs events.
pub struct LoggingEventListener;

impl MemberEventListener for LoggingEventListener {
    fn on_event(&self, event: MemberEvent) {
        match &event {
            MemberEvent::NodeJoin { node_id, addr } => {
                tracing::info!(node_id, %addr, "Node joined cluster");
            }
            MemberEvent::NodeLeave { node_id } => {
                tracing::info!(node_id, "Node left cluster");
            }
            MemberEvent::NodeSuspect {
                node_id,
                failed_pings,
            } => {
                tracing::warn!(node_id, failed_pings, "Node suspected failed");
            }
            MemberEvent::NodeFailed { node_id } => {
                tracing::error!(node_id, "Node confirmed failed");
            }
            MemberEvent::NodeRecovered { node_id, addr } => {
                tracing::info!(node_id, %addr, "Node recovered");
            }
            MemberEvent::NodeUpdated { node_id, addr } => {
                tracing::debug!(node_id, %addr, "Node updated");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_node_id() {
        let event = MemberEvent::NodeJoin {
            node_id: 42,
            addr: "127.0.0.1:9000".parse().unwrap(),
        };
        assert_eq!(event.node_id(), 42);
    }

    #[test]
    fn test_event_types() {
        let join = MemberEvent::NodeJoin {
            node_id: 1,
            addr: "127.0.0.1:9000".parse().unwrap(),
        };
        assert!(join.is_join());
        assert!(!join.is_leave());

        let failed = MemberEvent::NodeFailed { node_id: 1 };
        assert!(!failed.is_join());
        assert!(failed.is_leave());
    }
}
