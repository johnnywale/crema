//! Partitioning module for distributing keys across cluster nodes.
//!
//! This module implements consistent hashing for key distribution, ensuring:
//! - Even distribution of keys across nodes
//! - Minimal key redistribution when nodes join/leave
//! - Support for configurable replication (numOwners)
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    OwnershipTracker                          │
//! │  ┌──────────────────────────────────────────────────────┐  │
//! │  │                    HashRing                           │  │
//! │  │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐   │  │
//! │  │  │VN1:1│→│VN2:2│→│VN3:3│→│VN4:1│→│VN5:2│→│VN6:3│   │  │
//! │  │  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘ └─────┘   │  │
//! │  │      256 virtual nodes per physical node            │  │
//! │  └──────────────────────────────────────────────────────┘  │
//! │                                                             │
//! │  Key "user:123" → hash → find VN → Node 2 (primary)        │
//! │                                  → Node 3 (backup)         │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use distributed_cache::partitioning::{HashRing, OwnershipTracker};
//!
//! // Create a hash ring with 2 replicas
//! let mut ring = HashRing::new(2);
//! ring.add_node(1);
//! ring.add_node(2);
//! ring.add_node(3);
//!
//! // Get owners for a key
//! let owners = ring.get_replica_owners(b"user:123");
//! println!("Primary: {}, Backup: {:?}", owners[0], &owners[1..]);
//!
//! // Use ownership tracker for local node queries
//! let tracker = OwnershipTracker::new(1, 2);
//! tracker.add_node(1);
//! tracker.add_node(2);
//!
//! if tracker.is_primary(b"user:123") {
//!     println!("This node is the primary owner");
//! }
//! ```

mod hashring;
mod ownership;

pub use hashring::{HashRing, OwnershipChange, DEFAULT_VNODES_PER_NODE};
pub use ownership::{KeyOwnership, OwnershipRole, OwnershipTracker, PendingTransfers};
