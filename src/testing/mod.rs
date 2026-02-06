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
pub mod recovery;

mod cache_integration_tests_basic;
mod cache_integration_tests_discovery;
mod cache_integration_tests_edge_failed;
mod cache_integration_tests_edge_pass;
mod dashboard_e2e_tests;
#[cfg(feature = "memberlist")]
mod fix_regression_tests;
#[cfg(feature = "memberlist")]
mod memberlist_cache_integration_tests;
#[cfg(feature = "memberlist")]
mod memberlist_cluster_tests;
mod migration_unit_tests;
mod multiraft_forwarding_tests;
mod multiraft_integration_tests;
#[cfg(feature = "memberlist")]
mod multiraft_per_shard_replication_tests;
mod raft;
#[cfg(feature = "memberlist")]
mod rebalance_e2e_tests;
#[cfg(feature = "memberlist")]
mod recovery_e2e_tests;
mod single_node_migration_e2e;
mod slot_integration_tests;
mod utils;

pub use chaos::{
    ChaosAction, ChaosConfig, ChaosController, ChaosRunner, ChaosScenario, ChaosStats,
    NetworkPartition, NodeCrash,
};

use crate::types::NodeId;
use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub struct PortAllocator {
    /// Next port to try (starts at a high range to avoid conflicts)
    next_port: AtomicU16,
    /// Ports currently allocated (prevents reuse during test run)
    allocated: Mutex<HashSet<u16>>,
}
impl PortAllocator {
    /// Create a new port allocator starting at the given port.
    pub fn new(start_port: u16) -> Self {
        Self {
            next_port: AtomicU16::new(start_port),
            allocated: Mutex::new(HashSet::new()),
        }
    }

    /// Allocate a single available port.
    ///
    /// This method:
    /// 1. Finds a port that's not in the allocated set
    /// 2. Verifies it's bindable on both TCP and UDP
    /// 3. Marks it as allocated to prevent reuse
    pub fn allocate(&self) -> u16 {
        self.allocate_n(1)[0]
    }

    /// Allocate N available ports.
    ///
    /// All ports are verified to be bindable before returning.
    pub fn allocate_n(&self, count: usize) -> Vec<u16> {
        let mut ports = Vec::with_capacity(count);
        let mut allocated = self.allocated.lock().unwrap();

        while ports.len() < count {
            let port = self.next_port.fetch_add(1, Ordering::SeqCst);

            // Skip if already allocated
            if allocated.contains(&port) {
                continue;
            }

            // Skip reserved/problematic port ranges on Windows
            if Self::is_problematic_port(port) {
                continue;
            }

            // Verify the port is actually available
            if Self::is_port_available(port) {
                allocated.insert(port);
                ports.push(port);
            }
        }

        ports
    }

    /// Release a previously allocated port.
    ///
    /// Call this in test cleanup to allow port reuse in subsequent tests.
    #[allow(dead_code)]
    pub fn release(&self, port: u16) {
        let mut allocated = self.allocated.lock().unwrap();
        allocated.remove(&port);
    }

    /// Release multiple ports.
    #[allow(dead_code)]
    pub fn release_all(&self, ports: &[u16]) {
        let mut allocated = self.allocated.lock().unwrap();
        for port in ports {
            allocated.remove(port);
        }
    }

    /// Check if a port is in a problematic range (Windows-specific).
    fn is_problematic_port(port: u16) -> bool {
        // Avoid well-known ports
        if port < 1024 {
            return true;
        }

        // Avoid ephemeral port range that Windows uses for outbound connections
        // Windows typically uses 49152-65535 for ephemeral ports
        if port >= 49152 {
            return true;
        }

        // Hyper-V often reserves ports in certain ranges
        // These can vary by system, but some common ones:
        // Check if port is in a commonly problematic range
        false
    }

    /// Verify a port is available by actually binding to it.
    fn is_port_available(port: u16) -> bool {
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

        // Check TCP availability
        let tcp_available = TcpListener::bind(addr).is_ok();

        // Check UDP availability (memberlist uses UDP)
        let udp_available = UdpSocket::bind(addr).is_ok();

        tcp_available && udp_available
    }
}

pub static PORT_ALLOCATOR: Lazy<PortAllocator> = Lazy::new(|| PortAllocator::new(17000));

/// Convenience function to allocate a single available port.
pub fn allocate_port() -> u16 {
    PORT_ALLOCATOR.allocate()
}

/// Convenience function to allocate multiple available ports.
pub fn allocate_ports(count: usize) -> Vec<u16> {
    PORT_ALLOCATOR.allocate_n(count)
}

/// RAII guard that releases ports when dropped.
///
/// Use this to ensure ports are released even if a test panics.
pub struct PortGuard {
    ports: Vec<u16>,
}

impl PortGuard {
    /// Create a new port guard that will release the given ports on drop.
    pub fn new(ports: Vec<u16>) -> Self {
        Self { ports }
    }

    /// Get the allocated ports.
    #[allow(dead_code)]
    pub fn ports(&self) -> &[u16] {
        &self.ports
    }

    /// Get a single port (panics if more than one allocated).
    #[allow(dead_code)]
    pub fn port(&self) -> u16 {
        assert_eq!(self.ports.len(), 1, "Expected single port allocation");
        self.ports[0]
    }
}

impl Drop for PortGuard {
    fn drop(&mut self) {
        PORT_ALLOCATOR.release_all(&self.ports);
    }
}

/// Allocate a port with automatic cleanup via RAII guard.
#[allow(dead_code)]
pub fn allocate_port_guarded() -> PortGuard {
    PortGuard::new(vec![PORT_ALLOCATOR.allocate()])
}

/// Allocate multiple ports with automatic cleanup via RAII guard.
#[allow(dead_code)]
pub fn allocate_ports_guarded(count: usize) -> PortGuard {
    PortGuard::new(PORT_ALLOCATOR.allocate_n(count))
}

/// Async helper to wait for a condition to become true.
///
/// This is the recommended way to handle timing-sensitive assertions in async tests.
/// It repeatedly checks the condition until it returns true or the timeout is reached.
///
/// # Example
///
/// ```rust,ignore
/// use std::time::Duration;
/// use crema::testing::eventually;
///
/// // Wait for leader election
/// eventually(Duration::from_secs(5), || async {
///     cache.leader_id().is_some()
/// }).await.expect("leader should be elected");
///
/// // Wait for value to replicate
/// eventually(Duration::from_secs(3), || async {
///     cache.get(b"key").await.is_ok()
/// }).await.expect("value should replicate");
/// ```
pub async fn eventually<F, Fut>(timeout: Duration, mut f: F) -> Result<(), &'static str>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = tokio::time::Instant::now();
    loop {
        if f().await {
            return Ok(());
        }
        if start.elapsed() > timeout {
            return Err("timeout");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Async helper with custom error message.
pub async fn eventually_with_msg<F, Fut>(
    timeout: Duration,
    msg: &str,
    mut f: F,
) -> Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = tokio::time::Instant::now();
    loop {
        if f().await {
            return Ok(());
        }
        if start.elapsed() > timeout {
            return Err(format!("timeout after {:?}: {}", timeout, msg));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Async helper that runs a diagnostic callback on timeout.
///
/// When the condition is not met within the timeout, `on_timeout` is called
/// and its output is included in the error message, helping debug what step
/// the test was stuck at.
pub async fn eventually_with_diagnostics<F, Fut, D, DFut>(
    timeout: Duration,
    mut f: F,
    on_timeout: D,
) -> Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
    D: FnOnce() -> DFut,
    DFut: std::future::Future<Output = String>,
{
    let start = tokio::time::Instant::now();
    loop {
        if f().await {
            return Ok(());
        }
        if start.elapsed() > timeout {
            let diagnostics = on_timeout().await;
            return Err(format!("timeout after {:?}:\n{}", timeout, diagnostics));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

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

/// Test harness that wraps common migration test boilerplate.
///
/// Handles coordinator initialization, slot routing setup, writing test data,
/// waiting for leaders/migration, and cleanup. Each migration test creates
/// a harness and calls helper methods instead of duplicating 10-20 lines of
/// boilerplate per phase.
pub struct MigrationTestHarness {
    pub caches: Vec<Arc<crate::DistributedCache>>,
    pub coordinator: Arc<crate::multiraft::MultiRaftCoordinator>,
    pub num_entries: usize,
    pub key_prefix: String,
    migration_started: std::sync::atomic::AtomicBool,
}

impl MigrationTestHarness {
    /// Wrap existing caches into a harness. The first cache's coordinator is used.
    pub fn from_caches(caches: Vec<Arc<crate::DistributedCache>>) -> Self {
        let coordinator = caches[0]
            .multiraft_coordinator()
            .expect("First cache must have a coordinator")
            .clone();
        Self {
            caches,
            coordinator,
            num_entries: 0,
            key_prefix: "key".to_string(),
            migration_started: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Initialize single-node coordinator: init_shard_raft_infrastructure + init +
    /// start_shard_raft_manager + setup_shard_message_handler + wait for leader election.
    pub async fn init_single_node_coordinator(&self) {
        self.coordinator
            .init_shard_raft_infrastructure()
            .await
            .expect("Failed to init shard Raft infrastructure");
        self.coordinator
            .init()
            .await
            .expect("Failed to init coordinator");
        self.coordinator
            .start_shard_raft_manager()
            .await
            .expect("Failed to start shard Raft manager");
        self.caches[0].setup_shard_message_handler();

        // Wait for single-node shard leader elections
        tokio::time::sleep(Duration::from_secs(4)).await;
    }

    /// Initialize coordinator + enable slot routing in one call.
    pub async fn init_and_wait(&self) {
        self.init_single_node_coordinator().await;
        self.enable_slot_routing_all().await;
    }

    /// Enable slot routing on all caches and assert each succeeds.
    pub async fn enable_slot_routing_all(&self) {
        for cache in &self.caches {
            cache
                .enable_slot_routing()
                .await
                .expect("Should enable slot routing");
            assert!(cache.is_slot_routing_enabled());
        }
    }

    /// Write `n` test entries with `prefix:{:04}` keys and `value-{i}` values.
    pub async fn write_test_data(&mut self, n: usize, prefix: &str) {
        self.num_entries = n;
        self.key_prefix = prefix.to_string();
        for i in 0..n {
            self.caches[0]
                .put(format!("{}:{:04}", prefix, i), format!("value-{}", i))
                .await
                .expect("Put should succeed");
        }
        self.run_pending_tasks_all().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    /// Run pending tasks on all caches.
    pub async fn run_pending_tasks_all(&self) {
        for cache in &self.caches {
            cache.run_pending_tasks().await;
        }
    }

    /// Wait for a shard to become Raft leader within `timeout`.
    pub async fn wait_for_shard_leader(&self, shard_id: u32, timeout: Duration) {
        eventually(timeout, || async {
            self.coordinator
                .get_shard(shard_id)
                .map(|s| s.is_raft_leader())
                .unwrap_or(false)
        })
        .await
        .expect(&format!(
            "Shard {} should elect a leader within {:?}",
            shard_id, timeout
        ));
    }

    /// Start slot migration on all caches (idempotent — safe to call multiple times).
    pub fn start_migration_all(&self) {
        if self
            .migration_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            for cache in &self.caches {
                cache.start_slot_migration();
            }
        }
    }

    /// Stop slot migration on all caches.
    pub fn stop_migration_all(&self) {
        for cache in &self.caches {
            cache.stop_slot_migration();
        }
        self.migration_started.store(false, Ordering::SeqCst);
    }

    /// Build a diagnostic string with migration status and slot table state.
    pub fn migration_diagnostics(&self) -> String {
        let mut diag = String::new();
        if let Some(status) = self.coordinator.slot_migration_status() {
            diag.push_str(&format!(
                "migration status: active={}, completed={}, failed={}",
                status.active_migrations, status.completed_migrations, status.failed_migrations
            ));
            if !status.by_phase.is_empty() {
                diag.push_str(&format!(" phases={:?}", status.by_phase));
            }
        } else {
            diag.push_str("migration status: unavailable");
        }
        if let Some(snapshot) = self.coordinator.slot_table_snapshot() {
            let migrating: Vec<_> = snapshot
                .slots
                .iter()
                .enumerate()
                .filter(|(_, s)| s.state.is_migrating())
                .map(|(i, s)| format!("slot {}->shard {} ({:?})", i, s.owner, s.state))
                .collect();
            let imported: Vec<_> = snapshot
                .slots
                .iter()
                .enumerate()
                .filter(|(_, s)| s.state.is_imported())
                .map(|(i, s)| format!("slot {}->shard {} ({:?})", i, s.owner, s.state))
                .collect();
            diag.push_str(&format!(
                "\nslot table: total={}, migrating={}, imported={}",
                snapshot.slots.len(),
                migrating.len(),
                imported.len(),
            ));
            // Show first 15 migrating slots for debugging
            for m in migrating.iter().take(15) {
                diag.push_str(&format!("\n  {}", m));
            }
            if migrating.len() > 15 {
                diag.push_str(&format!("\n  ... and {} more", migrating.len() - 15));
            }
        }
        // Per-shard entry counts
        let shards = self.coordinator.router().all_shards();
        if !shards.is_empty() {
            diag.push_str("\nper-shard entries:");
            for shard in &shards {
                diag.push_str(&format!(
                    " shard{}={}",
                    shard.id(),
                    shard.storage().entry_count()
                ));
            }
        }
        // Migration record summary (non-completed phases)
        if let Some(migrator) = self.coordinator.slot_migrator() {
            diag.push_str(&format!("\nmigrator running: {}", migrator.is_running()));
            let records = migrator.get_all_migrations();
            let in_progress: Vec<_> = records
                .iter()
                .filter(|r| r.phase.is_in_progress())
                .collect();
            if !in_progress.is_empty() {
                diag.push_str(&format!(
                    "\nin-progress migrations ({}):",
                    in_progress.len()
                ));
                for r in in_progress.iter().take(10) {
                    diag.push_str(&format!(
                        "\n  slot {} from={} to={} phase={:?} keys_migrated={} completed_by={:?}",
                        r.slot_id,
                        r.from_shard,
                        r.to_shard,
                        r.phase,
                        r.keys_migrated,
                        r.completed_by_node
                    ));
                }
                if in_progress.len() > 10 {
                    diag.push_str(&format!("\n  ... and {} more", in_progress.len() - 10));
                }
            }
        } else {
            diag.push_str("\nmigrator: not initialized");
        }
        diag
    }

    /// Wait for all migrations to complete (active==0, completed>0) within `timeout`.
    pub async fn wait_for_migration_complete(&self, timeout: Duration) {
        use crate::multiraft::MigrationEventType;
        let coordinator = &self.coordinator;
        eventually_with_diagnostics(
            timeout,
            || async {
                let events = coordinator.migration_events();
                let registered: std::collections::HashSet<u16> = events
                    .iter()
                    .filter(|e| matches!(e.event_type, MigrationEventType::Registered))
                    .map(|e| e.slot_id)
                    .collect();
                if registered.is_empty() {
                    return false;
                }
                let terminal: std::collections::HashSet<u16> = events
                    .iter()
                    .filter(|e| {
                        matches!(
                            e.event_type,
                            MigrationEventType::Completed
                                | MigrationEventType::SyncCompleted
                                | MigrationEventType::SkippedReversed
                        )
                    })
                    .map(|e| e.slot_id)
                    .collect();
                registered.iter().all(|s| terminal.contains(s))
            },
            || async { self.migration_diagnostics() },
        )
        .await
        .expect("Migration should complete within timeout: not all registered slots have terminal events");
    }

    /// Wait for all slots to stop migrating within `timeout`.
    pub async fn wait_for_slots_stable(&self, timeout: Duration) {
        let coordinator = &self.coordinator;
        eventually_with_diagnostics(
            timeout,
            || async {
                coordinator
                    .slot_table_snapshot()
                    .map(|s| s.slots.iter().all(|s| !s.state.is_migrating()))
                    .unwrap_or(false)
            },
            || async { self.migration_diagnostics() },
        )
        .await
        .expect("All slots should finish migrating");
    }

    /// Wait for all slots to stop migrating AND none owned by `shard_id`.
    pub async fn wait_for_slots_off_shard(&self, shard_id: u32, timeout: Duration) {
        let coordinator = &self.coordinator;
        eventually_with_diagnostics(
            timeout,
            || async {
                coordinator
                    .slot_table_snapshot()
                    .map(|s| {
                        s.slots.iter().all(|s| !s.state.is_migrating())
                            && s.slots.iter().all(|s| s.owner != shard_id)
                    })
                    .unwrap_or(false)
            },
            || async { self.migration_diagnostics() },
        )
        .await
        .expect(&format!(
            "All slots should finish migrating off shard {}",
            shard_id
        ));
    }

    /// Wait for most migrations to complete (completed>0, active<=max_active).
    pub async fn wait_for_migration_mostly_complete(&self, timeout: Duration, max_active: usize) {
        let coordinator = &self.coordinator;
        eventually_with_diagnostics(
            timeout,
            || async {
                coordinator
                    .slot_migration_status()
                    .map(|s| s.completed_migrations > 0 && s.active_migrations <= max_active)
                    .unwrap_or(false)
            },
            || async { self.migration_diagnostics() },
        )
        .await
        .expect("Most migrations should complete within timeout");
    }

    /// Run pending tasks and let async operations settle.
    pub async fn settle(&self) {
        self.run_pending_tasks_all().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    /// Add a shard, start migration, wait for completion, and settle.
    ///
    /// Migration is started before the add so the loop is ready to process
    /// new slots immediately. The loop stays alive for subsequent operations.
    ///
    /// Returns the `AddShardResult` for use with `MigrationAssertions::capture_add_shard`.
    pub async fn add_shard_and_migrate(&self) -> crate::multiraft::AddShardResult {
        self.coordinator.clear_migration_events();
        self.start_migration_all();
        let add_result = self.caches[0]
            .add_shard()
            .await
            .expect("Add shard should succeed");
        self.wait_for_shard_leader(add_result.shard_id, Duration::from_secs(15))
            .await;
        self.wait_for_migration_complete(Duration::from_secs(30))
            .await;
        self.settle().await;
        add_result
    }

    /// Remove a shard, wait for all its slots to migrate off, and settle.
    ///
    /// The migration loop must already be running (via a prior `add_shard_and_migrate`
    /// or explicit `start_migration_all`). The loop picks up the new migrating slots
    /// from the remove operation.
    ///
    /// Returns the `RemoveShardResult` for use with `MigrationAssertions::capture_remove_shard`.
    pub async fn remove_shard_and_migrate(
        &self,
        shard_id: u32,
    ) -> crate::multiraft::RemoveShardResult {
        self.coordinator.clear_migration_events();
        self.start_migration_all();
        let remove_result = self.caches[0]
            .remove_shard(shard_id)
            .await
            .expect(&format!("Remove shard {} should succeed", shard_id));
        self.wait_for_slots_off_shard(shard_id, Duration::from_secs(45))
            .await;
        self.wait_for_migration_complete(Duration::from_secs(30))
            .await;
        self.settle().await;
        remove_result
    }

    /// Get the current slot table epoch.
    pub fn current_epoch(&self) -> u64 {
        self.coordinator
            .slot_table_snapshot()
            .map(|s| s.epoch.value())
            .unwrap_or(0)
    }

    /// Assert the active shard count equals `expected`.
    pub fn assert_active_shard_count(&self, expected: usize) {
        if let Some(cp) = self.coordinator.slot_control_plane() {
            assert_eq!(
                cp.active_shard_count(),
                expected,
                "Expected {} active shards, got {}",
                expected,
                cp.active_shard_count()
            );
        } else {
            panic!("No slot control plane available");
        }
    }

    /// Assert a shard owns zero slots in the slot table.
    pub fn assert_shard_has_no_slots(&self, shard_id: u32) {
        if let Some(snapshot) = self.coordinator.slot_table_snapshot() {
            let count = snapshot
                .slots
                .iter()
                .filter(|s| s.owner == shard_id)
                .count();
            assert_eq!(
                count, 0,
                "Shard {} should have 0 slots, got {}",
                shard_id, count
            );
        } else {
            panic!("No slot table snapshot available");
        }
    }

    /// Assert a shard owns at least one slot in the slot table.
    pub fn assert_shard_has_slots(&self, shard_id: u32) {
        if let Some(snapshot) = self.coordinator.slot_table_snapshot() {
            let count = snapshot
                .slots
                .iter()
                .filter(|s| s.owner == shard_id)
                .count();
            assert!(count > 0, "Shard {} should have slots, got 0", shard_id);
        } else {
            panic!("No slot table snapshot available");
        }
    }

    /// Assert total slot count equals `expected`.
    pub fn assert_total_slots(&self, expected: usize) {
        if let Some(snapshot) = self.coordinator.slot_table_snapshot() {
            assert_eq!(
                snapshot.slots.len(),
                expected,
                "Expected {} total slots, got {}",
                expected,
                snapshot.slots.len()
            );
        } else {
            panic!("No slot table snapshot available");
        }
    }

    /// Verify all written entries are readable and assert count matches.
    pub async fn assert_all_readable(&self) {
        let count = self.verify_all_readable().await;
        assert_eq!(
            count, self.num_entries,
            "Expected {} readable entries, got {}",
            self.num_entries, count
        );
    }

    /// Verify all written entries are readable via cache.get(). Returns count.
    pub async fn verify_all_readable(&self) -> usize {
        let mut count = 0;
        for i in 0..self.num_entries {
            let key = format!("{}:{:04}", self.key_prefix, i);
            let expected = format!("value-{}", i);
            if let Some(value) = self.caches[0].get(key.as_bytes()).await {
                if String::from_utf8_lossy(&value) == expected {
                    count += 1;
                }
            }
        }
        count
    }

    /// Stop migration and shut down all caches.
    pub async fn shutdown_all(&self) {
        self.stop_migration_all();
        for cache in &self.caches {
            cache.shutdown().await;
        }
    }

    /// Flush all shard storages (run_pending_tasks on each shard).
    pub async fn flush_shard_storages(&self) {
        for shard in self.coordinator.router().all_shards() {
            shard.storage().run_pending_tasks().await;
        }
    }
}

/// Summary of a ValidationCompleted event for assertion purposes.
pub struct ValidationEventSummary {
    pub slot_id: u16,
    pub source_count: u64,
    pub target_count: u64,
    pub checksums_match: bool,
}

/// Diagnostic information for a single slot that is stuck in Migrating state.
struct StuckSlotDiagnostic {
    slot_id: u16,
    owner: u32,
    from_shard: u32,
    keys_remaining: u64,
    time_stuck_ms: u64,
    // Migration record snapshot
    record_phase: Option<String>,
    record_keys_migrated: Option<u64>,
    record_completed_by_node: Option<Option<u64>>,
    record_last_progress_ms_ago: Option<u64>,
    // Full event timeline
    events: Vec<String>,
    failed_errors: Vec<String>,
    event_count: usize,
    // Diagnosis
    diagnosis: String,
}

/// System-wide migration context for diagnostic reports.
struct MigrationSystemContext {
    migrator_running: Option<bool>,
    active_migrations: usize,
    completed_migrations: usize,
    failed_migrations: usize,
    by_phase: HashMap<String, usize>,
    entries_per_shard: Vec<(u32, u64)>,
}

impl Default for MigrationSystemContext {
    fn default() -> Self {
        Self {
            migrator_running: None,
            active_migrations: 0,
            completed_migrations: 0,
            failed_migrations: 0,
            by_phase: HashMap::new(),
            entries_per_shard: Vec::new(),
        }
    }
}

/// Reusable post-migration validation for slot migration tests.
///
/// Captures pre- and post-migration state and runs assertions to verify
/// migration correctness: epoch changes, data integrity, slot table consistency,
/// and event log completeness.
pub struct MigrationAssertions {
    // Pre-migration state
    /// Initial slot table epoch before migration.
    pub initial_epoch: u64,
    /// Initial number of shards.
    pub initial_shard_count: usize,
    /// Total entries across all shards before migration.
    pub total_entries_before: u64,
    /// Entries per shard before migration.
    pub entries_per_shard_before: HashMap<u32, u64>,
    /// Number of entries written during test.
    pub num_entries_written: usize,

    // Post add_shard state
    /// The new shard ID.
    pub new_shard_id: u32,
    /// Number of slots assigned to the new shard.
    pub slots_assigned: usize,
    /// New epoch after add_shard.
    pub new_epoch: u64,
    /// Expected total shard count after add_shard.
    pub expected_shard_count: usize,

    // Post-migration state
    /// Number of migrations completed.
    pub migrations_completed: usize,
    /// Number of migrations failed.
    pub migrations_failed: usize,
    /// Entries on the new shard after migration.
    pub new_shard_entries: u64,
    /// Total entries across all shards after migration.
    pub total_entries_after: u64,
    /// Entries per shard after migration.
    pub entries_per_shard_after: HashMap<u32, u64>,
    /// Number of entries readable via cache.get().
    pub readable_count: usize,
    /// Keys that are not readable (first N, for diagnostics).
    pub unreadable_keys: Vec<String>,
    /// Number of slots assigned to the new shard in the slot table.
    pub slots_on_new_shard: usize,

    // Slot table integrity — rich diagnostics
    /// Diagnostics for each slot still in Migrating state (capped at 20).
    stuck_slots: Vec<StuckSlotDiagnostic>,
    /// System-wide migration context captured at diagnostic time.
    system_context: MigrationSystemContext,
    /// Total number of slots accounted for in the slot table.
    pub total_slots_accounted: usize,
    /// Expected total number of slots.
    pub expected_total_slots: usize,

    // Event log
    /// Migration events from the event log.
    pub events: Vec<crate::multiraft::MigrationEvent>,

    // Phase timing (from events)
    /// Total migration duration from first Registered to last Completed (ms).
    pub total_migration_duration_ms: Option<u64>,

    // Validation details (from ValidationCompleted events)
    /// Summaries of ValidationCompleted events.
    pub validation_events: Vec<ValidationEventSummary>,

    // Sync/lifecycle counters
    /// Number of SyncCompleted events.
    pub sync_completed_count: usize,
    /// Number of PeerSynced events.
    pub peer_synced_count: usize,
    /// Number of successful SourceDeleted events.
    pub source_deletions_successful: usize,
    /// Number of SkippedReversed events.
    pub skipped_reversed_count: usize,
    /// Number of StaleTakeover events.
    pub stale_takeover_count: usize,
}

/// Format a MigrationEventType for display in event timelines.
fn format_event_type(event_type: &crate::multiraft::MigrationEventType) -> String {
    use crate::multiraft::MigrationEventType;
    match event_type {
        MigrationEventType::Registered => "Registered".to_string(),
        MigrationEventType::Claimed { claim_epoch } => format!("Claimed(epoch={})", claim_epoch),
        MigrationEventType::ScanningStarted => "ScanningStarted".to_string(),
        MigrationEventType::ScanningCompleted { keys_found } => {
            format!("ScanningCompleted({} keys)", keys_found)
        }
        MigrationEventType::StreamingStarted { keys_total } => {
            format!("StreamingStarted({} keys)", keys_total)
        }
        MigrationEventType::StreamingProgress {
            keys_transferred,
            keys_total,
        } => format!("StreamingProgress({}/{})", keys_transferred, keys_total),
        MigrationEventType::CatchingUpStarted { from_log_index } => {
            format!("CatchingUp(idx={})", from_log_index)
        }
        MigrationEventType::Prepared {
            source_count,
            target_count,
            ..
        } => format!("Prepared(src={} tgt={})", source_count, target_count),
        MigrationEventType::Completed => "Completed".to_string(),
        MigrationEventType::Cleaned => "Cleaned".to_string(),
        MigrationEventType::Failed { error, retry_count } => {
            format!("Failed({:?}, retry {})", error, retry_count)
        }
        MigrationEventType::Retried { attempt } => format!("Retried({})", attempt),
        MigrationEventType::SyncCompleted => "SyncCompleted".to_string(),
        MigrationEventType::PeerSynced { is_new } => {
            format!("PeerSynced(new={})", is_new)
        }
        MigrationEventType::SourceDeleted {
            success,
            keys_deleted,
        } => format!("SourceDeleted(ok={} keys={})", success, keys_deleted),
        MigrationEventType::ValidationCompleted {
            source_count,
            target_count,
            follower_ok,
            ..
        } => format!(
            "Validation(src={} tgt={} follower={})",
            source_count, target_count, follower_ok
        ),
        MigrationEventType::SkippedReversed => "SkippedReversed".to_string(),
        MigrationEventType::StaleTakeover {
            old_epoch,
            new_epoch,
        } => format!("StaleTakeover({}->{})", old_epoch, new_epoch),
    }
}

/// Produce a human-readable root-cause diagnosis for a stuck migrating slot.
fn diagnose_stuck_slot(
    record_phase: &Option<String>,
    event_count: usize,
    failed_errors: &[String],
    keys_remaining: u64,
    time_stuck_ms: u64,
) -> String {
    match record_phase.as_deref() {
        None => {
            if event_count == 0 {
                "No migration record and no events — slot marked Migrating in slot table but migrator never registered it".to_string()
            } else {
                "No migration record but has events — record may have been cleaned up prematurely"
                    .to_string()
            }
        }
        Some("Completed") | Some("Cleaned") => {
            "Slot table still Migrating but record is Completed — mark_imported() not applied"
                .to_string()
        }
        Some("Prepared") => {
            "Record in Prepared state — validated but commit/mark_imported not applied".to_string()
        }
        Some("Failed") => {
            let last_err = failed_errors
                .last()
                .map(|s| s.as_str())
                .unwrap_or("unknown");
            format!("Record in Failed state, last error: {}", last_err)
        }
        Some("Claimed") => {
            if !failed_errors.is_empty() {
                let last_err = failed_errors.last().unwrap();
                format!(
                    "Stuck in Claimed, last error: {} — claiming node may have lost leadership",
                    last_err
                )
            } else if time_stuck_ms > 10_000 {
                "Stuck in Claimed for >10s with no errors — claiming node may be unreachable or overloaded".to_string()
            } else {
                "In Claimed state, awaiting scan start".to_string()
            }
        }
        Some("Pending") => {
            if time_stuck_ms > 10_000 {
                "Stuck in Pending for >10s — no node has claimed this slot, migrator may not be running".to_string()
            } else {
                "In Pending state, awaiting claim".to_string()
            }
        }
        Some("Scanning") => {
            format!("Stuck in Scanning with {} keys remaining — scan may be blocked or source unreachable", keys_remaining)
        }
        Some("Streaming") => {
            format!(
                "Stuck in Streaming with {} keys remaining — data transfer stalled",
                keys_remaining
            )
        }
        Some("CatchingUp") => {
            "Stuck in CatchingUp — write catch-up stalled, source may be under heavy write load"
                .to_string()
        }
        Some(other) => {
            format!(
                "In unexpected phase '{}' — check migration state machine",
                other
            )
        }
    }
}

impl MigrationAssertions {
    /// Capture pre-migration state from the coordinator.
    pub fn capture_pre_migration(
        coordinator: &Arc<crate::multiraft::MultiRaftCoordinator>,
        num_entries_written: usize,
    ) -> Self {
        let initial_epoch = coordinator
            .slot_table_snapshot()
            .map(|s| s.epoch.value())
            .unwrap_or(0);
        let initial_shard_count = coordinator.router().all_shards().len();

        let mut entries_per_shard_before = HashMap::new();
        let mut total_entries_before = 0u64;
        for shard in coordinator.router().all_shards() {
            let count = shard.storage().entry_count();
            entries_per_shard_before.insert(shard.id(), count);
            total_entries_before += count;
        }

        Self {
            initial_epoch,
            initial_shard_count,
            total_entries_before,
            entries_per_shard_before,
            num_entries_written,
            // Filled in later
            new_shard_id: 0,
            slots_assigned: 0,
            new_epoch: 0,
            expected_shard_count: 0,
            migrations_completed: 0,
            migrations_failed: 0,
            new_shard_entries: 0,
            total_entries_after: 0,
            entries_per_shard_after: HashMap::new(),
            readable_count: 0,
            unreadable_keys: Vec::new(),
            slots_on_new_shard: 0,
            stuck_slots: Vec::new(),
            system_context: MigrationSystemContext::default(),
            total_slots_accounted: 0,
            expected_total_slots: 0,
            events: Vec::new(),
            total_migration_duration_ms: None,
            validation_events: Vec::new(),
            sync_completed_count: 0,
            peer_synced_count: 0,
            source_deletions_successful: 0,
            skipped_reversed_count: 0,
            stale_takeover_count: 0,
        }
    }

    /// Record the result of add_shard.
    pub fn capture_add_shard(
        &mut self,
        add_result: &crate::multiraft::AddShardResult,
        expected_shard_count: usize,
    ) {
        self.new_shard_id = add_result.shard_id;
        self.slots_assigned = add_result.slots_assigned;
        self.new_epoch = add_result.new_epoch.value();
        self.expected_shard_count = expected_shard_count;
    }

    /// Record the result of remove_shard.
    pub fn capture_remove_shard(
        &mut self,
        remove_result: &crate::multiraft::RemoveShardResult,
        expected_shard_count: usize,
    ) {
        self.new_shard_id = remove_result.shard_id;
        self.slots_assigned = remove_result.slots_to_redistribute;
        self.new_epoch = remove_result.new_epoch.value();
        self.expected_shard_count = expected_shard_count;
    }

    /// Capture post-migration state from coordinator and cache.
    pub async fn capture_post_migration(
        &mut self,
        coordinator: &Arc<crate::multiraft::MultiRaftCoordinator>,
        cache: &Arc<crate::DistributedCache>,
    ) {
        // Migration stats
        if let Some(status) = coordinator.slot_migration_status() {
            self.migrations_completed = status.completed_migrations;
            self.migrations_failed = status.failed_migrations;
        }

        // New shard entries
        if let Some(shard) = coordinator.get_shard(self.new_shard_id) {
            shard.storage().run_pending_tasks().await;
            self.new_shard_entries = shard.storage().entry_count();
        }

        // Total entries per shard
        self.total_entries_after = 0;
        for shard in coordinator.router().all_shards() {
            shard.storage().run_pending_tasks().await;
            let count = shard.storage().entry_count();
            self.entries_per_shard_after.insert(shard.id(), count);
            self.total_entries_after += count;
        }

        // Data readability
        self.readable_count = 0;
        self.unreadable_keys.clear();
        for i in 0..self.num_entries_written {
            let key = format!("key:{:04}", i);
            let expected_value = format!("value-{}", i);
            if let Some(value) = cache.get(key.as_bytes()).await {
                if String::from_utf8_lossy(&value) == expected_value {
                    self.readable_count += 1;
                } else if self.unreadable_keys.len() < 10 {
                    self.unreadable_keys.push(format!("{} (wrong value)", key));
                }
            } else if self.unreadable_keys.len() < 10 {
                self.unreadable_keys.push(key);
            }
        }

        // Slot table integrity — rich diagnostics
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        if let Some(snapshot) = coordinator.slot_table_snapshot() {
            self.expected_total_slots = snapshot.slots.len();
            self.slots_on_new_shard = snapshot
                .slots
                .iter()
                .filter(|s| s.owner == self.new_shard_id)
                .count();
            self.total_slots_accounted = snapshot.slots.len();

            // Capture rich per-slot diagnostics for stuck (migrating) slots
            self.stuck_slots = snapshot
                .slots
                .iter()
                .enumerate()
                .filter(|(_, s)| s.state.is_migrating())
                .take(20)
                .map(|(i, s)| {
                    let slot_id = i as u16;
                    let (from_shard, started_at, keys_remaining) =
                        if let crate::multiraft::SlotState::Migrating {
                            from,
                            started_at,
                            keys_remaining,
                        } = &s.state
                        {
                            (*from, *started_at, *keys_remaining)
                        } else {
                            (0, 0, 0)
                        };
                    let time_stuck_ms = now_ms.saturating_sub(started_at);

                    // Migration record snapshot
                    let record = coordinator
                        .slot_migrator()
                        .and_then(|m| m.get_migration(slot_id));
                    let record_phase = record.as_ref().map(|r| r.phase.name().to_string());
                    let record_keys_migrated = record.as_ref().map(|r| r.keys_migrated);
                    let record_completed_by_node = record.as_ref().map(|r| r.completed_by_node);
                    let record_last_progress_ms_ago = record
                        .as_ref()
                        .map(|r| now_ms.saturating_sub(r.last_progress_at));

                    // Full event timeline
                    let slot_events = coordinator.migration_events_for_slot(slot_id);
                    let event_count = slot_events.len();
                    let first_ts = slot_events
                        .first()
                        .map(|e| e.timestamp_ms)
                        .unwrap_or(now_ms);
                    let events: Vec<String> = slot_events
                        .iter()
                        .map(|e| {
                            let relative_ms = e.timestamp_ms.saturating_sub(first_ts);
                            format!("{} (+{}ms)", format_event_type(&e.event_type), relative_ms)
                        })
                        .collect();
                    let failed_errors: Vec<String> = slot_events
                        .iter()
                        .filter_map(|e| {
                            if let crate::multiraft::MigrationEventType::Failed {
                                error,
                                retry_count,
                            } = &e.event_type
                            {
                                Some(format!("{} (retry {})", error, retry_count))
                            } else {
                                None
                            }
                        })
                        .collect();

                    let diagnosis = diagnose_stuck_slot(
                        &record_phase,
                        event_count,
                        &failed_errors,
                        keys_remaining,
                        time_stuck_ms,
                    );

                    StuckSlotDiagnostic {
                        slot_id,
                        owner: s.owner,
                        from_shard,
                        keys_remaining,
                        time_stuck_ms,
                        record_phase,
                        record_keys_migrated,
                        record_completed_by_node,
                        record_last_progress_ms_ago,
                        events,
                        failed_errors,
                        event_count,
                        diagnosis,
                    }
                })
                .collect();
        }

        // System-wide migration context
        {
            let migrator_running = coordinator.slot_migrator().map(|m| m.is_running());
            let status = coordinator.slot_migration_status();
            let (active, completed, failed, by_phase) = match &status {
                Some(s) => (
                    s.active_migrations,
                    s.completed_migrations,
                    s.failed_migrations,
                    s.by_phase.clone(),
                ),
                None => (0, 0, 0, HashMap::new()),
            };
            let mut entries_per_shard: Vec<(u32, u64)> = self
                .entries_per_shard_after
                .iter()
                .map(|(&k, &v)| (k, v))
                .collect();
            entries_per_shard.sort_by_key(|(id, _)| *id);

            self.system_context = MigrationSystemContext {
                migrator_running,
                active_migrations: active,
                completed_migrations: completed,
                failed_migrations: failed,
                by_phase,
                entries_per_shard,
            };
        }

        // Event log
        self.events = coordinator.migration_events();

        // Populate new fields from events
        use crate::multiraft::MigrationEventType;

        // Total migration duration: first Registered to last Completed
        let first_registered = self
            .events
            .iter()
            .find(|e| matches!(e.event_type, MigrationEventType::Registered))
            .map(|e| e.timestamp_ms);
        let last_completed = self
            .events
            .iter()
            .rev()
            .find(|e| matches!(e.event_type, MigrationEventType::Completed))
            .map(|e| e.timestamp_ms);
        self.total_migration_duration_ms = match (first_registered, last_completed) {
            (Some(start), Some(end)) => Some(end.saturating_sub(start)),
            _ => None,
        };

        // Validation event summaries
        self.validation_events = self
            .events
            .iter()
            .filter_map(|e| {
                if let MigrationEventType::ValidationCompleted {
                    source_count,
                    target_count,
                    source_checksum,
                    target_checksum,
                    ..
                } = &e.event_type
                {
                    Some(ValidationEventSummary {
                        slot_id: e.slot_id,
                        source_count: *source_count,
                        target_count: *target_count,
                        checksums_match: source_checksum == target_checksum,
                    })
                } else {
                    None
                }
            })
            .collect();

        // Count events by type
        for event in &self.events {
            match &event.event_type {
                MigrationEventType::SyncCompleted => self.sync_completed_count += 1,
                MigrationEventType::PeerSynced { .. } => self.peer_synced_count += 1,
                MigrationEventType::SourceDeleted { success, .. } => {
                    if *success {
                        self.source_deletions_successful += 1;
                    }
                }
                MigrationEventType::SkippedReversed => self.skipped_reversed_count += 1,
                MigrationEventType::StaleTakeover { .. } => self.stale_takeover_count += 1,
                _ => {}
            }
        }
    }

    /// Build a multi-section failure report with per-slot event timelines, diagnosis, and summary.
    ///
    /// Only called when an assertion fails — renders the structured diagnostics captured
    /// during `capture_post_migration()` into a human-readable report.
    fn build_failure_report(&self) -> String {
        let mut out = String::new();
        out.push_str("=== MIGRATION FAILURE REPORT ===\n\n");

        // System context
        let ctx = &self.system_context;
        out.push_str(&format!(
            "SYSTEM: migrator_running={} | active={} completed={} failed={}\n",
            ctx.migrator_running
                .map(|b| b.to_string())
                .unwrap_or_else(|| "N/A".to_string()),
            ctx.active_migrations,
            ctx.completed_migrations,
            ctx.failed_migrations,
        ));
        if !ctx.by_phase.is_empty() {
            let mut phases: Vec<_> = ctx.by_phase.iter().collect();
            phases.sort_by_key(|(name, _)| (*name).clone());
            let phase_str: Vec<String> = phases
                .iter()
                .map(|(name, count)| format!("{}={}", name, count))
                .collect();
            out.push_str(&format!("PHASES: {}\n", phase_str.join(" ")));
        }
        if !ctx.entries_per_shard.is_empty() {
            let shard_str: Vec<String> = ctx
                .entries_per_shard
                .iter()
                .map(|(id, count)| format!("shard{}={}", id, count))
                .collect();
            out.push_str(&format!("SHARDS: {}\n", shard_str.join(" ")));
        }

        // Per-slot diagnostics
        if !self.stuck_slots.is_empty() {
            out.push_str(&format!("\nSTUCK SLOTS ({}):\n", self.stuck_slots.len()));
            for diag in &self.stuck_slots {
                out.push_str(&format!(
                    "\n  slot {}: owner={} from={} stuck={:.1}s keys_remaining={}\n",
                    diag.slot_id,
                    diag.owner,
                    diag.from_shard,
                    diag.time_stuck_ms as f64 / 1000.0,
                    diag.keys_remaining,
                ));
                // Record info
                if let Some(ref phase) = diag.record_phase {
                    out.push_str(&format!(
                        "    record: phase={} keys_migrated={} completed_by={} last_progress={}\n",
                        phase,
                        diag.record_keys_migrated
                            .map(|k| k.to_string())
                            .unwrap_or_else(|| "?".to_string()),
                        diag.record_completed_by_node
                            .map(|n| {
                                n.map(|id| format!("node{}", id))
                                    .unwrap_or_else(|| "None".to_string())
                            })
                            .unwrap_or_else(|| "?".to_string()),
                        diag.record_last_progress_ms_ago
                            .map(|ms| format!("{:.1}s ago", ms as f64 / 1000.0))
                            .unwrap_or_else(|| "?".to_string()),
                    ));
                } else {
                    out.push_str("    record: NONE\n");
                }
                // Event timeline
                if diag.events.is_empty() {
                    out.push_str(&format!("    events ({}): (no events)\n", diag.event_count));
                } else {
                    let timeline: Vec<&str> = diag.events.iter().map(|s| s.as_str()).collect();
                    out.push_str(&format!(
                        "    events ({}): {}\n",
                        diag.event_count,
                        timeline.join(" -> ")
                    ));
                }
                // Failed errors
                if !diag.failed_errors.is_empty() {
                    let err_str: Vec<&str> =
                        diag.failed_errors.iter().map(|s| s.as_str()).collect();
                    out.push_str(&format!("    errors: {}\n", err_str.join("; ")));
                }
                // Diagnosis
                out.push_str(&format!("    -> {}\n", diag.diagnosis));
            }
        }

        // Summary: group by diagnosis category
        if !self.stuck_slots.is_empty() {
            let mut by_diagnosis: HashMap<&str, usize> = HashMap::new();
            for diag in &self.stuck_slots {
                *by_diagnosis.entry(&diag.diagnosis).or_insert(0) += 1;
            }
            let mut summary_items: Vec<_> = by_diagnosis.into_iter().collect();
            summary_items.sort_by(|a, b| b.1.cmp(&a.1));
            let summary_str: Vec<String> = summary_items
                .iter()
                .map(|(diag, count)| {
                    if *count == 1 {
                        diag.to_string()
                    } else {
                        format!("{} (x{})", diag, count)
                    }
                })
                .collect();
            out.push_str(&format!("\nSUMMARY: {}\n", summary_str.join(", ")));
        }

        out
    }

    /// Run all assertions (1-20) with descriptive messages.
    pub fn assert_all(&self) {
        self.assert_core();
        self.assert_event_lifecycle();
        self.assert_performance();
        self.assert_validation_integrity();
        self.assert_balance();
    }

    /// Assertions 1-13: core migration correctness.
    pub fn assert_core(&self) {
        // 1. Epoch increased
        assert!(
            self.new_epoch > self.initial_epoch,
            "Assertion 1 failed: Epoch should increase after add_shard. \
             initial={}, new={}",
            self.initial_epoch,
            self.new_epoch
        );

        // 2. New shard has slots > 0
        assert!(
            self.slots_assigned > 0,
            "Assertion 2 failed: New shard should have slots assigned. Got 0."
        );

        // 3. Migrations completed > 0
        assert!(
            self.migrations_completed > 0,
            "Assertion 3 failed: At least one migration should complete. \
             completed={}, failed={}",
            self.migrations_completed,
            self.migrations_failed
        );

        // 4. Migrations failed == 0
        assert_eq!(
            self.migrations_failed, 0,
            "Assertion 4 failed: No migrations should fail. failed={}",
            self.migrations_failed
        );

        // 5. New shard has entries > 0
        assert!(
            self.new_shard_entries > 0,
            "Assertion 5 failed: New shard {} should have entries after migration. Got 0.",
            self.new_shard_id
        );

        // 6. Total entries >= before (no data loss)
        assert!(
            self.total_entries_after >= self.total_entries_before,
            "Assertion 6 failed: Should not lose entries during migration. \
             before={}, after={}",
            self.total_entries_before,
            self.total_entries_after
        );

        // 7. All entries readable
        assert_eq!(
            self.readable_count,
            self.num_entries_written,
            "Assertion 7 failed: All entries should be readable after migration. \
             readable={}, expected={}, unreadable keys (first {}): [{}]\n\n{}",
            self.readable_count,
            self.num_entries_written,
            self.unreadable_keys.len(),
            self.unreadable_keys.join(", "),
            self.build_failure_report()
        );

        // 8. Slot table has new shard slots
        assert!(
            self.slots_on_new_shard > 0,
            "Assertion 8 failed: New shard should have slots in slot table. Got 0."
        );

        // 9. All slots not actively migrating (Stable or Imported are both acceptable)
        assert!(
            self.stuck_slots.is_empty(),
            "Assertion 9 failed: {} slots still in Migrating state.\n\n{}",
            self.stuck_slots.len(),
            self.build_failure_report()
        );

        // 10. Total slots == expected
        assert_eq!(
            self.total_slots_accounted, self.expected_total_slots,
            "Assertion 10 failed: Total slots mismatch. expected={}, got={}",
            self.expected_total_slots, self.total_slots_accounted
        );

        // 11. Events contain at least one Registered per migrated slot
        let registered_count = self
            .events
            .iter()
            .filter(|e| {
                matches!(
                    e.event_type,
                    crate::multiraft::MigrationEventType::Registered
                )
            })
            .count();
        assert!(
            registered_count > 0,
            "Assertion 11 failed: Event log should contain Registered events. Got 0."
        );

        // 12. Events contain Completed for each completed migration
        let completed_count = self
            .events
            .iter()
            .filter(|e| {
                matches!(
                    e.event_type,
                    crate::multiraft::MigrationEventType::Completed
                )
            })
            .count();
        assert!(
            completed_count > 0,
            "Assertion 12 failed: Event log should contain Completed events. Got 0."
        );

        // 13. No Failed events without matching Retried (or 0 failed)
        let failed_events: Vec<_> = self
            .events
            .iter()
            .filter(|e| {
                matches!(
                    e.event_type,
                    crate::multiraft::MigrationEventType::Failed { .. }
                )
            })
            .collect();
        if !failed_events.is_empty() {
            let retried_count = self
                .events
                .iter()
                .filter(|e| {
                    matches!(
                        e.event_type,
                        crate::multiraft::MigrationEventType::Retried { .. }
                    )
                })
                .count();
            assert!(
                retried_count >= failed_events.len(),
                "Assertion 13 failed: Failed events ({}) should have matching Retried events ({})",
                failed_events.len(),
                retried_count
            );
        }
    }

    /// Assertions 14-17: event lifecycle correctness.
    pub fn assert_event_lifecycle(&self) {
        use crate::multiraft::MigrationEventType;

        // 14. Event accounting: unique terminal slots should cover all migrated slots
        // Uses unique slot IDs from terminal events (Completed, SyncCompleted, SkippedReversed)
        let terminal_slot_count: HashSet<u16> = self
            .events
            .iter()
            .filter(|e| {
                matches!(
                    e.event_type,
                    MigrationEventType::Completed
                        | MigrationEventType::SyncCompleted
                        | MigrationEventType::SkippedReversed
                )
            })
            .map(|e| e.slot_id)
            .collect();
        assert!(
            !terminal_slot_count.is_empty(),
            "Assertion 14 failed: Should have at least one terminal event (Completed/SyncCompleted/SkippedReversed)"
        );
        assert_eq!(
            terminal_slot_count.len(),
            self.slots_assigned,
            "Assertion 14 failed: terminal slots ({}) should equal slots_assigned ({})",
            terminal_slot_count.len(),
            self.slots_assigned
        );

        // 15. Phase ordering: every directly Completed slot has a preceding Prepared or SkippedReversed
        // (SyncCompleted slots are discovered via slot table, so they don't need Prepared)
        let sync_completed_slots: HashSet<u16> = self
            .events
            .iter()
            .filter(|e| matches!(e.event_type, MigrationEventType::SyncCompleted))
            .map(|e| e.slot_id)
            .collect();
        let completed_slots: HashSet<u16> = self
            .events
            .iter()
            .filter(|e| matches!(e.event_type, MigrationEventType::Completed))
            .map(|e| e.slot_id)
            .collect();
        let prepared_or_skipped_slots: HashSet<u16> = self
            .events
            .iter()
            .filter(|e| {
                matches!(
                    e.event_type,
                    MigrationEventType::Prepared { .. } | MigrationEventType::SkippedReversed
                )
            })
            .map(|e| e.slot_id)
            .collect();
        for slot in &completed_slots {
            // Skip slots that were also discovered via sync (they may not have local Prepared)
            if sync_completed_slots.contains(slot) {
                continue;
            }
            assert!(
                prepared_or_skipped_slots.contains(slot),
                "Assertion 15 failed: Completed slot {} has no preceding Prepared or SkippedReversed event",
                slot
            );
        }

        // 16. Event chronology: for each slot, events are chronologically ordered
        let mut slot_events: HashMap<u16, Vec<u64>> = HashMap::new();
        for event in &self.events {
            slot_events
                .entry(event.slot_id)
                .or_default()
                .push(event.timestamp_ms);
        }
        for (slot_id, timestamps) in &slot_events {
            for window in timestamps.windows(2) {
                assert!(
                    window[0] <= window[1],
                    "Assertion 16 failed: Events for slot {} are not chronologically ordered: {} > {}",
                    slot_id, window[0], window[1]
                );
            }
        }

        // 17. Registered→Completed coverage: every Registered slot has a terminal event
        let registered_slots: HashSet<u16> = self
            .events
            .iter()
            .filter(|e| matches!(e.event_type, MigrationEventType::Registered))
            .map(|e| e.slot_id)
            .collect();
        let terminal_slots: HashSet<u16> = self
            .events
            .iter()
            .filter(|e| {
                matches!(
                    e.event_type,
                    MigrationEventType::Completed
                        | MigrationEventType::SkippedReversed
                        | MigrationEventType::SyncCompleted
                )
            })
            .map(|e| e.slot_id)
            .collect();
        for slot in &registered_slots {
            assert!(
                terminal_slots.contains(slot),
                "Assertion 17 failed: Registered slot {} has no terminal event (Completed/SkippedReversed/SyncCompleted)",
                slot
            );
        }
    }

    /// Assertion 18: performance bound.
    pub fn assert_performance(&self) {
        // 18. Total migration duration < 120s (not stuck)
        if let Some(duration_ms) = self.total_migration_duration_ms {
            assert!(
                duration_ms < 120_000,
                "Assertion 18 failed: Total migration took {}ms (>120s), likely stuck",
                duration_ms
            );
        }
    }

    /// Assertion 19: validation integrity.
    pub fn assert_validation_integrity(&self) {
        // 19. For each validation event, source_count == target_count
        for v in &self.validation_events {
            assert!(
                v.source_count == v.target_count || v.source_count == 0,
                "Assertion 19 failed: Validation for slot {} has count mismatch: source={}, target={}",
                v.slot_id, v.source_count, v.target_count
            );
        }
    }

    /// Assertion 20: shard balance.
    pub fn assert_balance(&self) {
        // 20. No shard has >60% of total entries (basic balance check)
        if self.total_entries_after > 0 && self.entries_per_shard_after.len() >= 2 {
            let threshold = (self.total_entries_after as f64 * 0.60) as u64;
            for (&shard_id, &count) in &self.entries_per_shard_after {
                assert!(
                    count <= threshold,
                    "Assertion 20 failed: Shard {} has {} entries ({:.1}% of {}), exceeds 60% threshold",
                    shard_id,
                    count,
                    (count as f64 / self.total_entries_after as f64) * 100.0,
                    self.total_entries_after
                );
            }
        }
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
