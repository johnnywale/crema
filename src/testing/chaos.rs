//! Chaos testing utilities for distributed cache testing.
//!
//! This module provides tools for simulating failures and adverse conditions
//! to test the resilience of the distributed cache.

use crate::types::NodeId;
use parking_lot::RwLock;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for chaos testing.
#[derive(Debug, Clone)]
pub struct ChaosConfig {
    /// Probability of network partition (0.0 - 1.0).
    pub partition_probability: f64,

    /// Minimum partition duration.
    pub partition_min_duration: Duration,

    /// Maximum partition duration.
    pub partition_max_duration: Duration,

    /// Probability of message drop (0.0 - 1.0).
    pub message_drop_probability: f64,

    /// Probability of message delay (0.0 - 1.0).
    pub message_delay_probability: f64,

    /// Minimum message delay.
    pub message_delay_min: Duration,

    /// Maximum message delay.
    pub message_delay_max: Duration,

    /// Probability of node crash (0.0 - 1.0).
    pub crash_probability: f64,

    /// Minimum crash duration.
    pub crash_min_duration: Duration,

    /// Maximum crash duration.
    pub crash_max_duration: Duration,

    /// Random seed for reproducibility.
    pub seed: Option<u64>,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            partition_probability: 0.0,
            partition_min_duration: Duration::from_secs(1),
            partition_max_duration: Duration::from_secs(10),
            message_drop_probability: 0.0,
            message_delay_probability: 0.0,
            message_delay_min: Duration::from_millis(10),
            message_delay_max: Duration::from_millis(100),
            crash_probability: 0.0,
            crash_min_duration: Duration::from_secs(1),
            crash_max_duration: Duration::from_secs(30),
            seed: None,
        }
    }
}

impl ChaosConfig {
    /// Create a config for light chaos testing.
    pub fn light() -> Self {
        Self {
            partition_probability: 0.01,
            message_drop_probability: 0.01,
            message_delay_probability: 0.05,
            crash_probability: 0.001,
            ..Default::default()
        }
    }

    /// Create a config for moderate chaos testing.
    pub fn moderate() -> Self {
        Self {
            partition_probability: 0.05,
            message_drop_probability: 0.05,
            message_delay_probability: 0.1,
            crash_probability: 0.01,
            ..Default::default()
        }
    }

    /// Create a config for heavy chaos testing.
    pub fn heavy() -> Self {
        Self {
            partition_probability: 0.1,
            message_drop_probability: 0.1,
            message_delay_probability: 0.2,
            crash_probability: 0.05,
            ..Default::default()
        }
    }

    /// Disable all chaos.
    pub fn none() -> Self {
        Self::default()
    }
}

/// A network partition affecting communication between nodes.
#[derive(Debug, Clone)]
pub struct NetworkPartition {
    /// Nodes in partition A.
    pub side_a: HashSet<NodeId>,

    /// Nodes in partition B.
    pub side_b: HashSet<NodeId>,

    /// When the partition started.
    pub started_at: Instant,

    /// When the partition will heal.
    pub heals_at: Instant,
}

impl NetworkPartition {
    /// Check if this partition separates the two nodes.
    pub fn separates(&self, from: NodeId, to: NodeId) -> bool {
        (self.side_a.contains(&from) && self.side_b.contains(&to))
            || (self.side_b.contains(&from) && self.side_a.contains(&to))
    }

    /// Check if the partition has healed.
    pub fn is_healed(&self) -> bool {
        Instant::now() >= self.heals_at
    }

    /// Get the duration of the partition.
    pub fn duration(&self) -> Duration {
        self.heals_at.duration_since(self.started_at)
    }

    /// Get remaining time until healing.
    pub fn remaining(&self) -> Duration {
        self.heals_at.saturating_duration_since(Instant::now())
    }
}

/// A simulated node crash.
#[derive(Debug, Clone)]
pub struct NodeCrash {
    /// The crashed node.
    pub node_id: NodeId,

    /// When the crash occurred.
    pub crashed_at: Instant,

    /// When the node will recover.
    pub recovers_at: Instant,
}

impl NodeCrash {
    /// Check if the node has recovered.
    pub fn is_recovered(&self) -> bool {
        Instant::now() >= self.recovers_at
    }

    /// Get the downtime duration.
    pub fn downtime(&self) -> Duration {
        self.recovers_at.duration_since(self.crashed_at)
    }
}

/// Chaos controller for injecting failures.
#[derive(Debug)]
pub struct ChaosController {
    /// Configuration.
    config: ChaosConfig,

    /// Whether chaos is enabled.
    enabled: AtomicBool,

    /// Active network partitions.
    partitions: RwLock<Vec<NetworkPartition>>,

    /// Active node crashes.
    crashes: RwLock<HashMap<NodeId, NodeCrash>>,

    /// Statistics: messages dropped.
    messages_dropped: AtomicU64,

    /// Statistics: messages delayed.
    messages_delayed: AtomicU64,

    /// Statistics: partitions created.
    partitions_created: AtomicU64,

    /// Statistics: crashes triggered.
    crashes_triggered: AtomicU64,

    /// All nodes in the cluster.
    nodes: RwLock<HashSet<NodeId>>,
}

impl ChaosController {
    /// Create a new chaos controller.
    pub fn new(config: ChaosConfig) -> Self {
        Self {
            config,
            enabled: AtomicBool::new(false),
            partitions: RwLock::new(Vec::new()),
            crashes: RwLock::new(HashMap::new()),
            messages_dropped: AtomicU64::new(0),
            messages_delayed: AtomicU64::new(0),
            partitions_created: AtomicU64::new(0),
            crashes_triggered: AtomicU64::new(0),
            nodes: RwLock::new(HashSet::new()),
        }
    }

    /// Create with default config.
    pub fn with_defaults() -> Self {
        Self::new(ChaosConfig::default())
    }

    /// Enable chaos injection.
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }

    /// Disable chaos injection.
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
    }

    /// Check if chaos is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Register a node with the controller.
    pub fn register_node(&self, node_id: NodeId) {
        self.nodes.write().insert(node_id);
    }

    /// Unregister a node from the controller.
    pub fn unregister_node(&self, node_id: NodeId) {
        self.nodes.write().remove(&node_id);
    }

    /// Check if a message should be dropped.
    pub fn should_drop_message(&self, _from: NodeId, _to: NodeId) -> bool {
        if !self.is_enabled() {
            return false;
        }

        let mut rng = rand::rng();
        if rng.random::<f64>() < self.config.message_drop_probability {
            self.messages_dropped.fetch_add(1, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Get delay for a message (returns None if no delay).
    pub fn get_message_delay(&self, _from: NodeId, _to: NodeId) -> Option<Duration> {
        if !self.is_enabled() {
            return None;
        }

        let mut rng = rand::rng();
        if rng.random::<f64>() < self.config.message_delay_probability {
            self.messages_delayed.fetch_add(1, Ordering::Relaxed);

            let min_ms = self.config.message_delay_min.as_millis() as u64;
            let max_ms = self.config.message_delay_max.as_millis() as u64;
            let delay_ms = rng.random_range(min_ms..=max_ms);

            return Some(Duration::from_millis(delay_ms));
        }

        None
    }

    /// Check if communication is blocked by a partition.
    pub fn is_partitioned(&self, from: NodeId, to: NodeId) -> bool {
        if !self.is_enabled() {
            return false;
        }

        // Clean up healed partitions and check active ones
        let mut partitions = self.partitions.write();
        partitions.retain(|p| !p.is_healed());

        partitions.iter().any(|p| p.separates(from, to))
    }

    /// Check if a node is crashed.
    pub fn is_crashed(&self, node_id: NodeId) -> bool {
        if !self.is_enabled() {
            return false;
        }

        let mut crashes = self.crashes.write();

        // Clean up recovered nodes
        crashes.retain(|_, c| !c.is_recovered());

        crashes.contains_key(&node_id)
    }

    /// Manually create a network partition.
    pub fn create_partition(&self, side_a: HashSet<NodeId>, side_b: HashSet<NodeId>, duration: Duration) {
        let partition = NetworkPartition {
            side_a,
            side_b,
            started_at: Instant::now(),
            heals_at: Instant::now() + duration,
        };

        self.partitions.write().push(partition);
        self.partitions_created.fetch_add(1, Ordering::Relaxed);
    }

    /// Manually crash a node.
    pub fn crash_node(&self, node_id: NodeId, duration: Duration) {
        let crash = NodeCrash {
            node_id,
            crashed_at: Instant::now(),
            recovers_at: Instant::now() + duration,
        };

        self.crashes.write().insert(node_id, crash);
        self.crashes_triggered.fetch_add(1, Ordering::Relaxed);
    }

    /// Heal all partitions immediately.
    pub fn heal_all_partitions(&self) {
        self.partitions.write().clear();
    }

    /// Recover all crashed nodes immediately.
    pub fn recover_all_nodes(&self) {
        self.crashes.write().clear();
    }

    /// Reset all chaos state.
    pub fn reset(&self) {
        self.heal_all_partitions();
        self.recover_all_nodes();
        self.messages_dropped.store(0, Ordering::Relaxed);
        self.messages_delayed.store(0, Ordering::Relaxed);
        self.partitions_created.store(0, Ordering::Relaxed);
        self.crashes_triggered.store(0, Ordering::Relaxed);
    }

    /// Maybe trigger random chaos based on probabilities.
    pub fn maybe_inject_chaos(&self) {
        if !self.is_enabled() {
            return;
        }

        let mut rng = rand::rng();
        let nodes: Vec<_> = self.nodes.read().iter().copied().collect();

        if nodes.len() < 2 {
            return;
        }

        // Maybe create partition
        if rng.random::<f64>() < self.config.partition_probability {
            // Split nodes into two groups
            let mid = nodes.len() / 2;
            let side_a: HashSet<_> = nodes[..mid].iter().copied().collect();
            let side_b: HashSet<_> = nodes[mid..].iter().copied().collect();

            let min_ms = self.config.partition_min_duration.as_millis() as u64;
            let max_ms = self.config.partition_max_duration.as_millis() as u64;
            let duration = Duration::from_millis(rng.random_range(min_ms..=max_ms));

            self.create_partition(side_a, side_b, duration);
        }

        // Maybe crash a node
        if rng.random::<f64>() < self.config.crash_probability {
            let node = nodes[rng.random_range(0..nodes.len())];

            // Don't crash already crashed nodes
            if !self.is_crashed(node) {
                let min_ms = self.config.crash_min_duration.as_millis() as u64;
                let max_ms = self.config.crash_max_duration.as_millis() as u64;
                let duration = Duration::from_millis(rng.random_range(min_ms..=max_ms));

                self.crash_node(node, duration);
            }
        }
    }

    /// Get chaos statistics.
    pub fn stats(&self) -> ChaosStats {
        ChaosStats {
            enabled: self.is_enabled(),
            messages_dropped: self.messages_dropped.load(Ordering::Relaxed),
            messages_delayed: self.messages_delayed.load(Ordering::Relaxed),
            partitions_created: self.partitions_created.load(Ordering::Relaxed),
            active_partitions: self.partitions.read().len(),
            crashes_triggered: self.crashes_triggered.load(Ordering::Relaxed),
            active_crashes: self.crashes.read().len(),
        }
    }
}

impl Default for ChaosController {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Chaos testing statistics.
#[derive(Debug, Clone)]
pub struct ChaosStats {
    /// Whether chaos is enabled.
    pub enabled: bool,
    /// Total messages dropped.
    pub messages_dropped: u64,
    /// Total messages delayed.
    pub messages_delayed: u64,
    /// Total partitions created.
    pub partitions_created: u64,
    /// Currently active partitions.
    pub active_partitions: usize,
    /// Total crashes triggered.
    pub crashes_triggered: u64,
    /// Currently crashed nodes.
    pub active_crashes: usize,
}

/// A chaos scenario for testing.
#[derive(Debug, Clone)]
pub struct ChaosScenario {
    /// Scenario name.
    pub name: String,
    /// Scenario description.
    pub description: String,
    /// Actions to execute.
    pub actions: Vec<ChaosAction>,
}

/// A chaos action to execute.
#[derive(Debug, Clone)]
pub enum ChaosAction {
    /// Wait for a duration.
    Wait(Duration),
    /// Create a network partition.
    Partition {
        side_a: Vec<NodeId>,
        side_b: Vec<NodeId>,
        duration: Duration,
    },
    /// Crash a node.
    CrashNode { node_id: NodeId, duration: Duration },
    /// Heal all partitions.
    HealPartitions,
    /// Recover all nodes.
    RecoverNodes,
    /// Enable chaos.
    EnableChaos,
    /// Disable chaos.
    DisableChaos,
}

impl ChaosScenario {
    /// Create a leader failover scenario.
    pub fn leader_failover(leader_id: NodeId) -> Self {
        Self {
            name: "leader_failover".to_string(),
            description: "Crash the leader and verify new election".to_string(),
            actions: vec![
                ChaosAction::CrashNode {
                    node_id: leader_id,
                    duration: Duration::from_secs(30),
                },
                ChaosAction::Wait(Duration::from_secs(5)),
                // Leader election should happen
            ],
        }
    }

    /// Create a network partition scenario.
    pub fn network_partition(side_a: Vec<NodeId>, side_b: Vec<NodeId>) -> Self {
        Self {
            name: "network_partition".to_string(),
            description: "Create network partition and verify behavior".to_string(),
            actions: vec![
                ChaosAction::Partition {
                    side_a,
                    side_b,
                    duration: Duration::from_secs(30),
                },
                ChaosAction::Wait(Duration::from_secs(10)),
                ChaosAction::HealPartitions,
                ChaosAction::Wait(Duration::from_secs(5)),
            ],
        }
    }

    /// Create a rolling restart scenario.
    pub fn rolling_restart(nodes: Vec<NodeId>, interval: Duration) -> Self {
        let mut actions = Vec::new();

        for node_id in nodes {
            actions.push(ChaosAction::CrashNode {
                node_id,
                duration: Duration::from_secs(5),
            });
            actions.push(ChaosAction::Wait(interval));
        }

        Self {
            name: "rolling_restart".to_string(),
            description: "Restart nodes one by one".to_string(),
            actions,
        }
    }
}

/// Runner for chaos scenarios.
pub struct ChaosRunner {
    controller: Arc<ChaosController>,
}

impl ChaosRunner {
    /// Create a new chaos runner.
    pub fn new(controller: Arc<ChaosController>) -> Self {
        Self { controller }
    }

    /// Execute a chaos scenario.
    pub async fn run(&self, scenario: &ChaosScenario) {
        tracing::info!(name = %scenario.name, "Starting chaos scenario");

        for action in &scenario.actions {
            self.execute_action(action).await;
        }

        tracing::info!(name = %scenario.name, "Completed chaos scenario");
    }

    async fn execute_action(&self, action: &ChaosAction) {
        match action {
            ChaosAction::Wait(duration) => {
                tokio::time::sleep(*duration).await;
            }
            ChaosAction::Partition {
                side_a,
                side_b,
                duration,
            } => {
                let a: HashSet<_> = side_a.iter().copied().collect();
                let b: HashSet<_> = side_b.iter().copied().collect();
                self.controller.create_partition(a, b, *duration);
            }
            ChaosAction::CrashNode { node_id, duration } => {
                self.controller.crash_node(*node_id, *duration);
            }
            ChaosAction::HealPartitions => {
                self.controller.heal_all_partitions();
            }
            ChaosAction::RecoverNodes => {
                self.controller.recover_all_nodes();
            }
            ChaosAction::EnableChaos => {
                self.controller.enable();
            }
            ChaosAction::DisableChaos => {
                self.controller.disable();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chaos_controller() {
        let controller = ChaosController::new(ChaosConfig::moderate());

        controller.register_node(1);
        controller.register_node(2);
        controller.register_node(3);

        assert!(!controller.is_enabled());
        controller.enable();
        assert!(controller.is_enabled());

        // Without explicit chaos, nodes aren't partitioned
        assert!(!controller.is_partitioned(1, 2));
        assert!(!controller.is_crashed(1));
    }

    #[test]
    fn test_manual_partition() {
        let controller = ChaosController::with_defaults();
        controller.enable();

        let side_a: HashSet<_> = [1, 2].into_iter().collect();
        let side_b: HashSet<_> = [3, 4].into_iter().collect();

        controller.create_partition(side_a, side_b, Duration::from_secs(60));

        assert!(controller.is_partitioned(1, 3));
        assert!(controller.is_partitioned(2, 4));
        assert!(!controller.is_partitioned(1, 2));
        assert!(!controller.is_partitioned(3, 4));
    }

    #[test]
    fn test_manual_crash() {
        let controller = ChaosController::with_defaults();
        controller.enable();

        controller.crash_node(1, Duration::from_secs(60));

        assert!(controller.is_crashed(1));
        assert!(!controller.is_crashed(2));

        controller.recover_all_nodes();
        assert!(!controller.is_crashed(1));
    }

    #[test]
    fn test_chaos_stats() {
        let controller = ChaosController::with_defaults();
        controller.enable();

        let side_a: HashSet<_> = [1].into_iter().collect();
        let side_b: HashSet<_> = [2].into_iter().collect();
        controller.create_partition(side_a, side_b, Duration::from_secs(60));
        controller.crash_node(3, Duration::from_secs(60));

        let stats = controller.stats();
        assert!(stats.enabled);
        assert_eq!(stats.partitions_created, 1);
        assert_eq!(stats.active_partitions, 1);
        assert_eq!(stats.crashes_triggered, 1);
        assert_eq!(stats.active_crashes, 1);
    }

    #[test]
    fn test_chaos_config_presets() {
        let light = ChaosConfig::light();
        assert!(light.partition_probability > 0.0);

        let moderate = ChaosConfig::moderate();
        assert!(moderate.partition_probability > light.partition_probability);

        let heavy = ChaosConfig::heavy();
        assert!(heavy.partition_probability > moderate.partition_probability);

        let none = ChaosConfig::none();
        assert_eq!(none.partition_probability, 0.0);
    }

    #[test]
    fn test_scenario_creation() {
        let scenario = ChaosScenario::leader_failover(1);
        assert_eq!(scenario.name, "leader_failover");
        assert!(!scenario.actions.is_empty());

        let scenario = ChaosScenario::network_partition(vec![1, 2], vec![3, 4]);
        assert_eq!(scenario.name, "network_partition");

        let scenario = ChaosScenario::rolling_restart(vec![1, 2, 3], Duration::from_secs(5));
        assert_eq!(scenario.name, "rolling_restart");
        assert_eq!(scenario.actions.len(), 6); // 3 crashes + 3 waits
    }
}
