//! Rebalancing module for data transfer during membership changes.
//!
//! This module handles the coordination of data transfer when nodes join or
//! leave the cluster, ensuring data consistency and minimal downtime.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                  RebalanceCoordinator                        │
//! │  ┌──────────────────────────────────────────────────────┐  │
//! │  │  Phase 1: Calculate Ownership Changes                │  │
//! │  │  - Snapshot current hash ring                        │  │
//! │  │  - Calculate which keys need to move                 │  │
//! │  └──────────────────────────────────────────────────────┘  │
//! │                          ↓                                   │
//! │  ┌──────────────────────────────────────────────────────┐  │
//! │  │  Phase 2: Stream Data                                 │  │
//! │  │  - Transfer entries in batches                        │  │
//! │  │  - Preserve TTL metadata                              │  │
//! │  │  - Skip expired entries                               │  │
//! │  └──────────────────────────────────────────────────────┘  │
//! │                          ↓                                   │
//! │  ┌──────────────────────────────────────────────────────┐  │
//! │  │  Phase 3: Commit New Ring                             │  │
//! │  │  - Update hash ring atomically                        │  │
//! │  │  - Notify all nodes                                   │  │
//! │  └──────────────────────────────────────────────────────┘  │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Three-Phase Rebalancing
//!
//! ## Phase 1: Calculate Ownership Changes
//!
//! When a node joins or leaves:
//! 1. Snapshot the current hash ring
//! 2. Calculate the new hash ring
//! 3. Determine which keys need to transfer between nodes
//!
//! ## Phase 2: Stream Data
//!
//! Transfer affected keys with their metadata:
//! - Key and value data
//! - Absolute expiration time (not relative TTL)
//! - Filter out already-expired entries
//!
//! ## Phase 3: Commit New Ring
//!
//! After successful transfer:
//! 1. Commit new hash ring to Raft log
//! 2. All nodes switch to new ring atomically
//! 3. Old owner can now delete transferred keys
//!
//! # Example
//!
//! ```rust,ignore
//! use distributed_cache::rebalancing::{RebalanceCoordinator, RebalanceConfig};
//! use distributed_cache::partitioning::OwnershipTracker;
//!
//! // Create coordinator
//! let tracker = Arc::new(OwnershipTracker::new(1, 2));
//! let coordinator = RebalanceCoordinator::with_defaults(tracker);
//!
//! // Start rebalancing for new node
//! let op_id = coordinator.start_node_join(3)?;
//!
//! // Calculate transfers
//! let transfers = coordinator.calculate_transfers()?;
//!
//! // Execute transfers (in practice, this involves network calls)
//! for transfer in transfers {
//!     // ... stream data ...
//!     coordinator.complete_transfer(transfer.from_node, transfer.to_node)?;
//! }
//!
//! // Commit new ring
//! let new_ring = coordinator.commit()?;
//! ```

mod coordinator;
mod transfer;

pub use coordinator::{
    RebalanceConfig, RebalanceCoordinator, RebalanceError, RebalanceOperation,
    RebalanceOperationInfo, RebalanceState, RebalanceType,
};
pub use transfer::{
    TransferBatch, TransferEntry, TransferProgress, TransferRequest, TransferResponse,
};
