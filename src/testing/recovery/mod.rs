//! Recovery testing framework for crash recovery scenarios.
//!
//! This module provides infrastructure for testing state machine recovery,
//! including crash simulation, failpoint injection, and consistency verification.
//!
//! # Overview
//!
//! The recovery testing framework consists of three main components:
//!
//! 1. **Failpoint Injection** (`failpoint.rs`): A mechanism to inject failures
//!    at specific points in the code to test recovery behavior.
//!
//! 2. **Recovery Test Cluster** (`cluster.rs`): A test cluster that supports
//!    starting, stopping, crashing, and recovering nodes with persistent state.
//!
//! 3. **Consistency Checker** (`checker.rs`): Tracks expected state and verifies
//!    that nodes contain the correct data after recovery.
//!
//! # Example
//!
//! ```rust,ignore
//! use distributed_cache::testing::recovery::{
//!     RecoveryTestCluster, RecoveryTestConfig, FailpointAction,
//! };
//! use std::time::Duration;
//!
//! #[tokio::test]
//! async fn test_recovery_after_crash() {
//!     // Create a 3-node cluster with persistence
//!     let config = RecoveryTestConfig::new(3)
//!         .with_persistent(true)
//!         .with_checkpointing(true);
//!
//!     let cluster = RecoveryTestCluster::new(config).await.unwrap();
//!     cluster.start_all().await.unwrap();
//!
//!     // Write some data
//!     cluster.write("key1", "value1").await.unwrap();
//!     cluster.write("key2", "value2").await.unwrap();
//!
//!     // Force a snapshot
//!     let leader = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
//!     cluster.force_snapshot(leader).await.unwrap();
//!
//!     // Crash a node
//!     cluster.crash_node(leader).await.unwrap();
//!
//!     // Wait for new leader
//!     let new_leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();
//!
//!     // Recover the crashed node
//!     cluster.recover_node(leader).await.unwrap();
//!
//!     // Verify consistency
//!     cluster.verify_consistency().await.unwrap();
//!
//!     cluster.shutdown().await;
//! }
//! ```
//!
//! # Failpoint Injection
//!
//! Failpoints can be used to inject failures at specific points in the code:
//!
//! ```rust,ignore
//! use distributed_cache::testing::recovery::{FailpointRegistry, FailpointAction};
//!
//! let registry = FailpointRegistry::new();
//!
//! // Panic on the 5th hit
//! registry.enable("checkpoint_before_write", FailpointAction::CountdownPanic(5));
//!
//! // Panic with 10% probability
//! registry.enable("raft_during_apply", FailpointAction::ProbabilityPanic(0.1));
//!
//! // Execute custom logic
//! registry.enable("storage_before_append", FailpointAction::Callback(Arc::new(|| {
//!     println!("Storage append about to happen!");
//! })));
//! ```
//!
//! # Test Categories
//!
//! The recovery test suite covers the following scenarios:
//!
//! ## Basic Recovery
//! - Snapshot-only recovery
//! - Log-replay-only recovery
//! - Snapshot + log recovery
//! - TTL preservation after recovery
//! - Applied index restoration
//!
//! ## Crash Timing
//! - Crash during snapshot write
//! - Crash during log append
//! - Crash after commit, before apply
//! - Crash during apply
//! - Crash during compaction
//!
//! ## Leader/Follower Recovery
//! - Leader crash and recovery
//! - Follower crash and rejoin
//! - Multiple sequential crashes
//! - Minority/majority crash scenarios
//!
//! ## Migration + Recovery
//! - Recovery during migration
//! - Source/target crash during migration
//!
//! ## Full Cluster Chaos
//! - Rolling restart
//! - Network partition and heal
//! - Random crash sequences
//! - Crashes during write load

pub mod checker;
pub mod cluster;
pub mod failpoint;

pub use checker::ConsistencyChecker;
pub use cluster::{NodeState, RecoveryTestCluster, RecoveryTestConfig};
pub use failpoint::{
    fail_point, fail_point_async, global_registry, FailpointAction, FailpointRegistry,
    FailpointResult, FailpointStats,
};
