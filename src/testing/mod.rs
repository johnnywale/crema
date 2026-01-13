//! Testing utilities for the distributed cache.
//!
//! This module provides tools for testing distributed systems including:
//! - Chaos testing for failure injection
//! - Predefined test scenarios
//! - Test helpers and utilities
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      Testing Framework                          │
//! │                                                                 │
//! │  ┌───────────────────────────────────────────────────────────┐ │
//! │  │                    ChaosController                         │ │
//! │  │  - Network partitions                                      │ │
//! │  │  - Node crashes                                            │ │
//! │  │  - Message drops/delays                                    │ │
//! │  └───────────────────────────────────────────────────────────┘ │
//! │                             │                                   │
//! │                             ▼                                   │
//! │  ┌───────────────────────────────────────────────────────────┐ │
//! │  │                    ChaosRunner                             │ │
//! │  │  - Execute predefined scenarios                           │ │
//! │  │  - Leader failover tests                                  │ │
//! │  │  - Network partition tests                                │ │
//! │  │  - Rolling restart tests                                  │ │
//! │  └───────────────────────────────────────────────────────────┘ │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use distributed_cache::testing::{ChaosController, ChaosConfig, ChaosScenario, ChaosRunner};
//! use std::sync::Arc;
//!
//! // Create a chaos controller with moderate settings
//! let config = ChaosConfig::moderate();
//! let controller = Arc::new(ChaosController::new(config));
//!
//! // Register nodes
//! controller.register_node(1);
//! controller.register_node(2);
//! controller.register_node(3);
//!
//! // Enable chaos
//! controller.enable();
//!
//! // Check if nodes can communicate
//! if controller.is_partitioned(1, 2) {
//!     println!("Nodes 1 and 2 are partitioned!");
//! }
//!
//! // Run a predefined scenario
//! let runner = ChaosRunner::new(controller.clone());
//! let scenario = ChaosScenario::leader_failover(1);
//! // runner.run(&scenario).await;
//! ```
//!
//! # Chaos Testing Presets
//!
//! - `ChaosConfig::none()` - No failures (default)
//! - `ChaosConfig::light()` - Low probability of failures
//! - `ChaosConfig::moderate()` - Medium failure rates
//! - `ChaosConfig::heavy()` - High failure rates for stress testing

mod chaos;

mod cache_integration_tests_basic;
mod cache_integration_tests_edge_failed;
mod cache_integration_tests_edge_pass;
mod memberlist_cache_integration_tests;
mod memberlist_cluster_tests;
mod raft;
mod utils;

pub use chaos::{
    ChaosAction, ChaosConfig, ChaosController, ChaosRunner, ChaosScenario, ChaosStats,
    NetworkPartition, NodeCrash,
};

use crate::types::NodeId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A test cluster for integration testing.
#[derive(Debug)]
pub struct TestCluster {
    /// Nodes in the cluster.
    pub nodes: Vec<NodeId>,

    /// Chaos controller for failure injection.
    pub chaos: Arc<ChaosController>,

    /// When the test started.
    pub started_at: Instant,
}

impl TestCluster {
    /// Create a new test cluster with the given number of nodes.
    pub fn new(node_count: usize) -> Self {
        let chaos = Arc::new(ChaosController::with_defaults());
        let nodes: Vec<NodeId> = (1..=node_count as u64).collect();

        for &node in &nodes {
            chaos.register_node(node);
        }

        Self {
            nodes,
            chaos,
            started_at: Instant::now(),
        }
    }

    /// Create a test cluster with custom chaos config.
    pub fn with_chaos(node_count: usize, config: ChaosConfig) -> Self {
        let chaos = Arc::new(ChaosController::new(config));
        let nodes: Vec<NodeId> = (1..=node_count as u64).collect();

        for &node in &nodes {
            chaos.register_node(node);
        }

        Self {
            nodes,
            chaos,
            started_at: Instant::now(),
        }
    }

    /// Get the number of nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get the first node (often the initial leader).
    pub fn first_node(&self) -> NodeId {
        self.nodes[0]
    }

    /// Get all nodes except the specified one.
    pub fn other_nodes(&self, except: NodeId) -> Vec<NodeId> {
        self.nodes
            .iter()
            .copied()
            .filter(|&n| n != except)
            .collect()
    }

    /// Enable chaos testing.
    pub fn enable_chaos(&self) {
        self.chaos.enable();
    }

    /// Disable chaos testing.
    pub fn disable_chaos(&self) {
        self.chaos.disable();
    }

    /// Create a partition isolating a node from others.
    pub fn isolate_node(&self, node_id: NodeId, duration: Duration) {
        let isolated: std::collections::HashSet<_> = [node_id].into_iter().collect();
        let others: std::collections::HashSet<_> = self.other_nodes(node_id).into_iter().collect();
        self.chaos.create_partition(isolated, others, duration);
    }

    /// Crash a node for the specified duration.
    pub fn crash_node(&self, node_id: NodeId, duration: Duration) {
        self.chaos.crash_node(node_id, duration);
    }

    /// Heal all partitions.
    pub fn heal_partitions(&self) {
        self.chaos.heal_all_partitions();
    }

    /// Recover all crashed nodes.
    pub fn recover_nodes(&self) {
        self.chaos.recover_all_nodes();
    }

    /// Get elapsed time since test started.
    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Get chaos statistics.
    pub fn stats(&self) -> ChaosStats {
        self.chaos.stats()
    }
}

/// Assertions for testing distributed cache behavior.
pub struct TestAssertions;

impl TestAssertions {
    /// Assert that a value was eventually consistent across nodes.
    pub fn assert_eventually<F>(check: F, timeout: Duration, message: &str)
    where
        F: Fn() -> bool,
    {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if check() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("Assertion failed after {:?}: {}", timeout, message);
    }

    /// Assert that a condition becomes true within timeout.
    pub fn wait_for<F>(condition: F, timeout: Duration) -> bool
    where
        F: Fn() -> bool,
    {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if condition() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        false
    }
}

/// Test metrics collector for tracking test results.
#[derive(Debug, Default)]
pub struct TestMetrics {
    /// Operation latencies.
    latencies: Vec<Duration>,

    /// Error count by type.
    errors: HashMap<String, u64>,

    /// Custom metrics.
    custom: HashMap<String, f64>,
}

impl TestMetrics {
    /// Create a new test metrics collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an operation latency.
    pub fn record_latency(&mut self, latency: Duration) {
        self.latencies.push(latency);
    }

    /// Record an error.
    pub fn record_error(&mut self, error_type: &str) {
        *self.errors.entry(error_type.to_string()).or_insert(0) += 1;
    }

    /// Set a custom metric.
    pub fn set_custom(&mut self, name: &str, value: f64) {
        self.custom.insert(name.to_string(), value);
    }

    /// Get average latency.
    pub fn avg_latency(&self) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }
        let total: Duration = self.latencies.iter().sum();
        total / self.latencies.len() as u32
    }

    /// Get p99 latency.
    pub fn p99_latency(&self) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }
        let mut sorted = self.latencies.clone();
        sorted.sort();
        let idx = (sorted.len() as f64 * 0.99) as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    /// Get total error count.
    pub fn total_errors(&self) -> u64 {
        self.errors.values().sum()
    }

    /// Generate a summary report.
    pub fn report(&self) -> String {
        let mut output = String::new();
        output.push_str("=== Test Metrics Report ===\n");
        output.push_str(&format!("Operations: {}\n", self.latencies.len()));
        output.push_str(&format!("Avg latency: {:?}\n", self.avg_latency()));
        output.push_str(&format!("P99 latency: {:?}\n", self.p99_latency()));
        output.push_str(&format!("Total errors: {}\n", self.total_errors()));

        if !self.errors.is_empty() {
            output.push_str("Errors by type:\n");
            for (error_type, count) in &self.errors {
                output.push_str(&format!("  {}: {}\n", error_type, count));
            }
        }

        if !self.custom.is_empty() {
            output.push_str("Custom metrics:\n");
            for (name, value) in &self.custom {
                output.push_str(&format!("  {}: {:.3}\n", name, value));
            }
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_test_cluster() {
        let cluster = TestCluster::new(3);
        assert_eq!(cluster.node_count(), 3);
        assert_eq!(cluster.first_node(), 1);
        assert_eq!(cluster.other_nodes(1), vec![2, 3]);
    }

    #[test]
    fn test_cluster_chaos() {
        let cluster = TestCluster::new(3);
        cluster.enable_chaos();

        cluster.isolate_node(1, Duration::from_secs(60));

        assert!(cluster.chaos.is_partitioned(1, 2));
        assert!(cluster.chaos.is_partitioned(1, 3));
        assert!(!cluster.chaos.is_partitioned(2, 3));

        cluster.heal_partitions();
        assert!(!cluster.chaos.is_partitioned(1, 2));
    }

    #[test]
    fn test_test_metrics() {
        let mut metrics = TestMetrics::new();

        metrics.record_latency(Duration::from_millis(10));
        metrics.record_latency(Duration::from_millis(20));
        metrics.record_latency(Duration::from_millis(30));

        metrics.record_error("timeout");
        metrics.record_error("timeout");
        metrics.record_error("connection");

        metrics.set_custom("throughput", 1000.0);

        assert_eq!(metrics.avg_latency(), Duration::from_millis(20));
        assert_eq!(metrics.total_errors(), 3);

        let report = metrics.report();
        assert!(report.contains("Operations: 3"));
        assert!(report.contains("throughput"));
    }

    #[test]
    fn test_wait_for() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();

        // Spawn thread to set flag after delay
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            flag_clone.store(true, Ordering::Relaxed);
        });

        let result =
            TestAssertions::wait_for(|| flag.load(Ordering::Relaxed), Duration::from_millis(200));

        assert!(result);
    }
}
