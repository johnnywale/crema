//! Migration-aware routing for read/write operations.
//!
//! This module implements routing strategies during shard migration to ensure
//! consistency and availability while data is being transferred between nodes.

use crate::error::{Error, Result};
use crate::types::NodeId;
use std::sync::atomic::{AtomicU8, Ordering};

use super::migration::ShardMigration;
use super::shard::ShardId;

/// Routing strategy during shard migration.
///
/// Different strategies trade off between consistency, availability, and performance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum MigrationRoutingStrategy {
    /// Writes go to old primary only, reads can go to both nodes.
    ///
    /// This is the default strategy. It minimizes write amplification while
    /// allowing reads from either node (with potential staleness on the new node
    /// until migration completes).
    ///
    /// - Writes: Old primary only
    /// - Reads: Both nodes (old primary preferred)
    /// - Consistency: Strong for writes, eventual for reads on new node
    /// - Performance: Best write performance
    #[default]
    OldPrimaryWritesDualReads = 0,

    /// Writes go to both nodes simultaneously.
    ///
    /// This strategy ensures the new node is always up-to-date but doubles
    /// write latency and bandwidth. Use when you need strong consistency
    /// during migration and can tolerate the performance hit.
    ///
    /// - Writes: Both nodes (old must succeed, new failure tolerated)
    /// - Reads: Both nodes
    /// - Consistency: Strong (both nodes always in sync)
    /// - Performance: 2x write latency, 2x write bandwidth
    DualWrites = 1,

    /// Block all writes during migration.
    ///
    /// The safest strategy - no writes are allowed until migration completes.
    /// Use for critical data where consistency is paramount and brief
    /// unavailability is acceptable.
    ///
    /// - Writes: Blocked (returns error)
    /// - Reads: Both nodes
    /// - Consistency: Perfect (no concurrent modifications)
    /// - Performance: No write availability during migration
    BlockWrites = 2,

    /// Writes go to new primary only (post-cutover).
    ///
    /// Used during the final phase of migration after ownership has been
    /// transferred but before cleanup is complete.
    ///
    /// - Writes: New primary only
    /// - Reads: Both nodes (new primary preferred)
    /// - Consistency: Strong
    /// - Performance: Normal
    NewPrimaryOnly = 3,
}

impl MigrationRoutingStrategy {
    /// Convert from u8 representation.
    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::OldPrimaryWritesDualReads,
            1 => Self::DualWrites,
            2 => Self::BlockWrites,
            3 => Self::NewPrimaryOnly,
            _ => Self::OldPrimaryWritesDualReads,
        }
    }

    /// Check if writes are allowed with this strategy.
    pub fn allows_writes(&self) -> bool {
        !matches!(self, Self::BlockWrites)
    }

    /// Check if this strategy requires dual writes.
    pub fn requires_dual_writes(&self) -> bool {
        matches!(self, Self::DualWrites)
    }

    /// Get the preferred write node for this strategy.
    pub fn write_target(&self, migration: &ShardMigration) -> WriteTarget {
        match self {
            Self::OldPrimaryWritesDualReads => WriteTarget::Single(migration.from_node),
            Self::DualWrites => WriteTarget::Both {
                primary: migration.from_node,
                secondary: migration.to_node,
            },
            Self::BlockWrites => WriteTarget::Blocked,
            Self::NewPrimaryOnly => WriteTarget::Single(migration.to_node),
        }
    }

    /// Get the preferred read nodes for this strategy.
    pub fn read_targets(&self, migration: &ShardMigration) -> ReadTargets {
        match self {
            Self::OldPrimaryWritesDualReads => ReadTargets {
                preferred: migration.from_node,
                fallback: Some(migration.to_node),
            },
            Self::DualWrites => ReadTargets {
                preferred: migration.from_node,
                fallback: Some(migration.to_node),
            },
            Self::BlockWrites => ReadTargets {
                preferred: migration.from_node,
                fallback: Some(migration.to_node),
            },
            Self::NewPrimaryOnly => ReadTargets {
                preferred: migration.to_node,
                fallback: Some(migration.from_node),
            },
        }
    }
}

impl std::fmt::Display for MigrationRoutingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OldPrimaryWritesDualReads => write!(f, "old_primary_writes_dual_reads"),
            Self::DualWrites => write!(f, "dual_writes"),
            Self::BlockWrites => write!(f, "block_writes"),
            Self::NewPrimaryOnly => write!(f, "new_primary_only"),
        }
    }
}

/// Target for write operations during migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteTarget {
    /// Write to a single node.
    Single(NodeId),
    /// Write to both nodes (primary must succeed, secondary failure tolerated).
    Both { primary: NodeId, secondary: NodeId },
    /// Writes are blocked.
    Blocked,
}

/// Targets for read operations during migration.
#[derive(Debug, Clone)]
pub struct ReadTargets {
    /// Preferred node to read from.
    pub preferred: NodeId,
    /// Fallback node if preferred is unavailable.
    pub fallback: Option<NodeId>,
}

/// Routing decision for a specific operation.
#[derive(Debug, Clone)]
pub enum RoutingDecision {
    /// Route to a single node.
    Single { node: NodeId, shard_id: ShardId },
    /// Route to the primary with optional secondary for redundancy.
    WithFallback {
        primary: NodeId,
        fallback: NodeId,
        shard_id: ShardId,
    },
    /// Write to both nodes.
    DualWrite {
        primary: NodeId,
        secondary: NodeId,
        shard_id: ShardId,
    },
    /// Operation is blocked.
    Blocked { shard_id: ShardId, reason: String },
}

impl RoutingDecision {
    /// Get the shard ID for this decision.
    pub fn shard_id(&self) -> ShardId {
        match self {
            Self::Single { shard_id, .. } => *shard_id,
            Self::WithFallback { shard_id, .. } => *shard_id,
            Self::DualWrite { shard_id, .. } => *shard_id,
            Self::Blocked { shard_id, .. } => *shard_id,
        }
    }

    /// Check if this decision allows the operation.
    pub fn is_allowed(&self) -> bool {
        !matches!(self, Self::Blocked { .. })
    }

    /// Get the primary target node.
    pub fn primary_node(&self) -> Option<NodeId> {
        match self {
            Self::Single { node, .. } => Some(*node),
            Self::WithFallback { primary, .. } => Some(*primary),
            Self::DualWrite { primary, .. } => Some(*primary),
            Self::Blocked { .. } => None,
        }
    }
}

/// Migration-aware router that handles routing during shard migrations.
#[derive(Debug)]
pub struct MigrationRouter {
    /// Default routing strategy for new migrations.
    default_strategy: AtomicU8,
}

impl MigrationRouter {
    /// Create a new migration router with the default strategy.
    pub fn new() -> Self {
        Self {
            default_strategy: AtomicU8::new(MigrationRoutingStrategy::default() as u8),
        }
    }

    /// Create with a specific default strategy.
    pub fn with_strategy(strategy: MigrationRoutingStrategy) -> Self {
        Self {
            default_strategy: AtomicU8::new(strategy as u8),
        }
    }

    /// Get the current default strategy.
    pub fn default_strategy(&self) -> MigrationRoutingStrategy {
        MigrationRoutingStrategy::from_u8(self.default_strategy.load(Ordering::Relaxed))
    }

    /// Set the default strategy.
    pub fn set_default_strategy(&self, strategy: MigrationRoutingStrategy) {
        self.default_strategy
            .store(strategy as u8, Ordering::Relaxed);
    }

    /// Determine routing for a write operation during migration.
    pub fn route_write(
        &self,
        shard_id: ShardId,
        migration: &ShardMigration,
        strategy: Option<MigrationRoutingStrategy>,
    ) -> RoutingDecision {
        let strategy = strategy.unwrap_or_else(|| self.default_strategy());

        match strategy.write_target(migration) {
            WriteTarget::Single(node) => RoutingDecision::Single { node, shard_id },
            WriteTarget::Both { primary, secondary } => RoutingDecision::DualWrite {
                primary,
                secondary,
                shard_id,
            },
            WriteTarget::Blocked => RoutingDecision::Blocked {
                shard_id,
                reason: "Writes blocked during migration".to_string(),
            },
        }
    }

    /// Determine routing for a read operation during migration.
    pub fn route_read(
        &self,
        shard_id: ShardId,
        migration: &ShardMigration,
        strategy: Option<MigrationRoutingStrategy>,
    ) -> RoutingDecision {
        let strategy = strategy.unwrap_or_else(|| self.default_strategy());
        let targets = strategy.read_targets(migration);

        match targets.fallback {
            Some(fallback) => RoutingDecision::WithFallback {
                primary: targets.preferred,
                fallback,
                shard_id,
            },
            None => RoutingDecision::Single {
                node: targets.preferred,
                shard_id,
            },
        }
    }

    /// Check if a write should be allowed based on the current strategy.
    pub fn should_allow_write(&self, strategy: Option<MigrationRoutingStrategy>) -> bool {
        let strategy = strategy.unwrap_or_else(|| self.default_strategy());
        strategy.allows_writes()
    }
}

impl Default for MigrationRouter {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of executing a dual-write operation.
#[derive(Debug)]
pub struct DualWriteResult {
    /// Result from the primary node.
    pub primary_result: Result<()>,
    /// Result from the secondary node.
    pub secondary_result: Result<()>,
}

impl DualWriteResult {
    /// Create a new dual-write result.
    pub fn new(primary: Result<()>, secondary: Result<()>) -> Self {
        Self {
            primary_result: primary,
            secondary_result: secondary,
        }
    }

    /// Check if the dual-write was successful (primary succeeded).
    ///
    /// Secondary failures are tolerated in dual-write mode.
    pub fn is_success(&self) -> bool {
        self.primary_result.is_ok()
    }

    /// Check if both writes succeeded.
    pub fn both_succeeded(&self) -> bool {
        self.primary_result.is_ok() && self.secondary_result.is_ok()
    }

    /// Check if the secondary write failed.
    pub fn secondary_failed(&self) -> bool {
        self.secondary_result.is_err()
    }

    /// Get the secondary error if present.
    pub fn secondary_error(&self) -> Option<&Error> {
        self.secondary_result.as_ref().err()
    }

    /// Get the secondary error message if present.
    pub fn secondary_error_message(&self) -> Option<String> {
        self.secondary_result.as_ref().err().map(|e| e.to_string())
    }

    /// Convert to a single result (based on primary).
    ///
    /// WARNING: This discards secondary error information. Before calling this,
    /// check `secondary_failed()` and use `secondary_error_message()` to log
    /// or track the secondary error for reconciliation.
    pub fn into_result(self) -> Result<()> {
        self.primary_result
    }

    /// Convert to a single result, logging any secondary failure.
    ///
    /// This is the recommended way to convert a DualWriteResult when you don't
    /// need to track failures for reconciliation but want visibility into errors.
    pub fn into_result_with_logging(
        self,
        shard_id: super::shard::ShardId,
        key_hint: &str,
    ) -> Result<()> {
        if let Some(error) = self.secondary_error_message() {
            tracing::warn!(
                shard_id,
                key_hint,
                secondary_error = %error,
                "Dual-write secondary failed (primary succeeded)"
            );
        }
        self.primary_result
    }
}

/// Configuration for migration routing behavior.
#[derive(Debug, Clone)]
pub struct MigrationRoutingConfig {
    /// Default routing strategy.
    pub default_strategy: MigrationRoutingStrategy,
    /// Timeout for write operations during migration.
    pub write_timeout_ms: u64,
    /// Whether to log routing decisions.
    pub log_routing_decisions: bool,
    /// Whether to allow strategy override per-operation.
    pub allow_strategy_override: bool,
}

impl Default for MigrationRoutingConfig {
    fn default() -> Self {
        Self {
            default_strategy: MigrationRoutingStrategy::default(),
            write_timeout_ms: 5000,
            log_routing_decisions: false,
            allow_strategy_override: true,
        }
    }
}

impl MigrationRoutingConfig {
    /// Create a new config with the specified strategy.
    pub fn with_strategy(strategy: MigrationRoutingStrategy) -> Self {
        Self {
            default_strategy: strategy,
            ..Default::default()
        }
    }

    /// Set write timeout.
    pub fn with_write_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.write_timeout_ms = timeout_ms;
        self
    }

    /// Enable routing decision logging.
    pub fn with_logging(mut self) -> Self {
        self.log_routing_decisions = true;
        self
    }
}

// ==================== Dual Write Failure Tracking ====================

use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::Instant;

/// A failed dual-write entry that needs reconciliation.
#[derive(Debug, Clone)]
pub struct FailedDualWrite {
    /// The key that failed to write to secondary.
    pub key: Vec<u8>,
    /// The value that should have been written.
    pub value: Vec<u8>,
    /// When the failure occurred.
    pub failed_at: Instant,
    /// The error that caused the failure.
    pub error: String,
    /// Number of retry attempts.
    pub retry_count: u32,
}

impl FailedDualWrite {
    /// Create a new failed dual-write record.
    pub fn new(key: Vec<u8>, value: Vec<u8>, error: impl Into<String>) -> Self {
        Self {
            key,
            value,
            failed_at: Instant::now(),
            error: error.into(),
            retry_count: 0,
        }
    }

    /// Increment retry count.
    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
    }

    /// Check if this failure is old (potentially stale).
    pub fn is_stale(&self, max_age: std::time::Duration) -> bool {
        self.failed_at.elapsed() > max_age
    }
}

/// Tracks failed secondary writes during dual-write mode for later reconciliation.
///
/// During dual-write migration, the secondary write to the target node may fail
/// while the primary write succeeds. This tracker records these failures so they
/// can be reconciled before the commit phase to ensure data consistency.
///
/// # Usage Pattern
///
/// 1. During dual-write phase, record failures via `record_failure()`
/// 2. Before commit phase, call `get_failures_for_reconciliation()` to get all failures
/// 3. Retry each failure against the target node
/// 4. Call `clear_reconciled()` after successful reconciliation
///
/// # Thread Safety
///
/// Uses DashMap for per-shard lock granularity. Operations on different shards
/// can proceed concurrently without blocking each other. This prevents deadlock
/// risks where multiple shards are being reconciled simultaneously.
#[derive(Debug)]
pub struct DualWriteTracker {
    /// Failed writes per shard, keyed by shard ID.
    /// Uses DashMap for per-shard locking to avoid global lock bottleneck
    /// and potential deadlocks when multiple shards are being processed.
    failures: DashMap<ShardId, Vec<FailedDualWrite>>,
    /// Approximate total count (may drift slightly due to concurrent updates).
    /// Used for soft limit checking without global locking.
    total_count: AtomicUsize,
    /// Maximum failures to track per shard (prevents unbounded growth).
    max_failures_per_shard: usize,
    /// Maximum total failures across all shards.
    max_total_failures: usize,
    /// Maximum age of failures before they're considered stale.
    max_failure_age: std::time::Duration,
    /// Lock for global pruning operations that need to touch all shards.
    /// Only used when global limit is hit and we need to prune across shards.
    prune_lock: Mutex<()>,
}

impl Default for DualWriteTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl DualWriteTracker {
    /// Create a new tracker with default settings.
    pub fn new() -> Self {
        Self {
            failures: DashMap::new(),
            total_count: AtomicUsize::new(0),
            max_failures_per_shard: 10000,
            max_total_failures: 100000, // 100K total across all shards
            max_failure_age: std::time::Duration::from_secs(300), // 5 minutes
            prune_lock: Mutex::new(()),
        }
    }

    /// Create with custom settings.
    pub fn with_limits(
        max_failures_per_shard: usize,
        max_failure_age: std::time::Duration,
    ) -> Self {
        Self {
            failures: DashMap::new(),
            total_count: AtomicUsize::new(0),
            max_failures_per_shard,
            max_total_failures: max_failures_per_shard * 10, // Default to 10x per-shard limit
            max_failure_age,
            prune_lock: Mutex::new(()),
        }
    }

    /// Record a failed secondary write.
    ///
    /// Returns true if the failure was recorded, false if the limit was reached.
    /// Automatically prunes stale entries when limits are reached.
    pub fn record_failure(
        &self,
        shard_id: ShardId,
        key: Vec<u8>,
        value: Vec<u8>,
        error: impl Into<String>,
    ) -> bool {
        // Check approximate global limit first (soft check without global locking)
        let approx_total = self.total_count.load(AtomicOrdering::Relaxed);
        if approx_total >= self.max_total_failures {
            // Try global prune under lock
            let _guard = self.prune_lock.lock();
            let pruned = self.prune_stale_internal();
            if pruned > 0 {
                tracing::info!(
                    pruned_count = pruned,
                    "Auto-pruned stale failures globally on limit reached"
                );
            }

            // Re-check after pruning
            let new_total = self.total_count.load(AtomicOrdering::Relaxed);
            if new_total >= self.max_total_failures {
                tracing::warn!(
                    total_count = new_total,
                    max_total = self.max_total_failures,
                    "Global dual-write failure limit reached, dropping failure"
                );
                return false;
            }
        }

        // Per-shard insertion with per-shard lock only
        let mut entry = self.failures.entry(shard_id).or_default();
        let shard_failures = entry.value_mut();

        // Check per-shard limit
        if shard_failures.len() >= self.max_failures_per_shard {
            // Prune stale entries for this shard only
            let before = shard_failures.len();
            shard_failures.retain(|f| !f.is_stale(self.max_failure_age));
            let pruned = before - shard_failures.len();

            if pruned > 0 {
                self.total_count.fetch_sub(pruned, AtomicOrdering::Relaxed);
                tracing::debug!(
                    shard_id,
                    pruned_count = pruned,
                    "Auto-pruned stale failures on shard limit reached"
                );
            }

            // Check again after pruning
            if shard_failures.len() >= self.max_failures_per_shard {
                tracing::warn!(
                    shard_id,
                    count = shard_failures.len(),
                    "Per-shard dual-write failure limit reached, dropping failure"
                );
                return false;
            }
        }

        shard_failures.push(FailedDualWrite::new(key, value, error));
        self.total_count.fetch_add(1, AtomicOrdering::Relaxed);
        tracing::debug!(
            shard_id,
            total_failures = shard_failures.len(),
            "Recorded dual-write failure"
        );
        true
    }

    /// Get all failures for a shard that need reconciliation.
    ///
    /// This also prunes stale entries to prevent unbounded memory growth.
    /// Returns cloned failures for processing without holding the lock long.
    pub fn get_failures_for_reconciliation(&self, shard_id: ShardId) -> Vec<FailedDualWrite> {
        if let Some(mut entry) = self.failures.get_mut(&shard_id) {
            let shard_failures = entry.value_mut();
            let before = shard_failures.len();

            // Prune stale entries while we have mutable access
            shard_failures.retain(|f| !f.is_stale(self.max_failure_age));

            let pruned = before - shard_failures.len();
            if pruned > 0 {
                self.total_count.fetch_sub(pruned, AtomicOrdering::Relaxed);
                tracing::debug!(
                    shard_id,
                    pruned,
                    remaining = shard_failures.len(),
                    "Pruned stale entries during reconciliation fetch"
                );
            }

            // Clone the non-stale failures for processing
            shard_failures.clone()
        } else {
            Vec::new()
        }
    }

    /// Get the count of actionable (non-stale) failures for a shard.
    ///
    /// Only counts failures that are not stale (within max_failure_age).
    /// Use `total_failure_count()` to get the count including stale entries.
    pub fn failure_count(&self, shard_id: ShardId) -> usize {
        self.failures
            .get(&shard_id)
            .map(|entry| {
                entry
                    .value()
                    .iter()
                    .filter(|f| !f.is_stale(self.max_failure_age))
                    .count()
            })
            .unwrap_or(0)
    }

    /// Check if there are any actionable (non-stale) failures pending for a shard.
    pub fn has_pending_failures(&self, shard_id: ShardId) -> bool {
        self.failure_count(shard_id) > 0
    }

    /// Clear all failures for a shard after successful reconciliation.
    pub fn clear_reconciled(&self, shard_id: ShardId) {
        if let Some((_, removed)) = self.failures.remove(&shard_id) {
            let count = removed.len();
            self.total_count.fetch_sub(count, AtomicOrdering::Relaxed);
            tracing::info!(
                shard_id,
                cleared_count = count,
                "Cleared reconciled dual-write failures"
            );
        }
    }

    /// Clear specific keys from failures (partial reconciliation).
    ///
    /// Uses HashSet for O(1) key lookup instead of O(n) linear search,
    /// minimizing the time the per-shard lock is held.
    pub fn clear_keys(&self, shard_id: ShardId, keys: &[Vec<u8>]) {
        if let Some(mut entry) = self.failures.get_mut(&shard_id) {
            let shard_failures = entry.value_mut();
            let before = shard_failures.len();

            // Use HashSet for O(1) lookup instead of O(n) contains check
            let keys_set: HashSet<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
            shard_failures.retain(|f| !keys_set.contains(f.key.as_slice()));

            let removed = before - shard_failures.len();
            if removed > 0 {
                self.total_count.fetch_sub(removed, AtomicOrdering::Relaxed);
                tracing::debug!(
                    shard_id,
                    removed_count = removed,
                    remaining = shard_failures.len(),
                    "Partially cleared dual-write failures"
                );
            }
        }
    }

    /// Internal pruning without lock (caller must hold prune_lock if needed).
    fn prune_stale_internal(&self) -> usize {
        let mut total_pruned = 0;

        // Iterate over all shards - DashMap iteration doesn't block other operations
        for mut entry in self.failures.iter_mut() {
            let shard_id = *entry.key();
            let shard_failures = entry.value_mut();
            let before = shard_failures.len();
            shard_failures.retain(|f| !f.is_stale(self.max_failure_age));
            let pruned = before - shard_failures.len();
            if pruned > 0 {
                tracing::debug!(
                    shard_id,
                    pruned_count = pruned,
                    "Pruned stale dual-write failures"
                );
                total_pruned += pruned;
            }
        }

        // Remove empty entries
        self.failures.retain(|_, v| !v.is_empty());

        // Update total count
        if total_pruned > 0 {
            self.total_count
                .fetch_sub(total_pruned, AtomicOrdering::Relaxed);
        }

        total_pruned
    }

    /// Remove stale failures across all shards.
    pub fn prune_stale(&self) -> usize {
        let _guard = self.prune_lock.lock();
        self.prune_stale_internal()
    }

    /// Get total failure count across all shards.
    pub fn total_failure_count(&self) -> usize {
        self.total_count.load(AtomicOrdering::Relaxed)
    }

    /// Get summary of failures per shard.
    pub fn failure_summary(&self) -> HashMap<ShardId, usize> {
        self.failures
            .iter()
            .map(|entry| (*entry.key(), entry.value().len()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::multiraft::shard_placement::MovementType;

    fn test_migration() -> ShardMigration {
        use crate::multiraft::shard_placement::ShardMovement;
        let movement = ShardMovement::new(0, 1, 2, MovementType::AddReplica);
        ShardMigration::from_movement(&movement)
    }

    #[test]
    fn test_strategy_from_u8() {
        assert_eq!(
            MigrationRoutingStrategy::from_u8(0),
            MigrationRoutingStrategy::OldPrimaryWritesDualReads
        );
        assert_eq!(
            MigrationRoutingStrategy::from_u8(1),
            MigrationRoutingStrategy::DualWrites
        );
        assert_eq!(
            MigrationRoutingStrategy::from_u8(2),
            MigrationRoutingStrategy::BlockWrites
        );
        assert_eq!(
            MigrationRoutingStrategy::from_u8(3),
            MigrationRoutingStrategy::NewPrimaryOnly
        );
        assert_eq!(
            MigrationRoutingStrategy::from_u8(99),
            MigrationRoutingStrategy::OldPrimaryWritesDualReads
        );
    }

    #[test]
    fn test_strategy_allows_writes() {
        assert!(MigrationRoutingStrategy::OldPrimaryWritesDualReads.allows_writes());
        assert!(MigrationRoutingStrategy::DualWrites.allows_writes());
        assert!(!MigrationRoutingStrategy::BlockWrites.allows_writes());
        assert!(MigrationRoutingStrategy::NewPrimaryOnly.allows_writes());
    }

    #[test]
    fn test_write_target_old_primary() {
        let migration = test_migration();
        let target = MigrationRoutingStrategy::OldPrimaryWritesDualReads.write_target(&migration);
        assert_eq!(target, WriteTarget::Single(1)); // from_node
    }

    #[test]
    fn test_write_target_dual_writes() {
        let migration = test_migration();
        let target = MigrationRoutingStrategy::DualWrites.write_target(&migration);
        assert_eq!(
            target,
            WriteTarget::Both {
                primary: 1,
                secondary: 2
            }
        );
    }

    #[test]
    fn test_write_target_blocked() {
        let migration = test_migration();
        let target = MigrationRoutingStrategy::BlockWrites.write_target(&migration);
        assert_eq!(target, WriteTarget::Blocked);
    }

    #[test]
    fn test_write_target_new_primary() {
        let migration = test_migration();
        let target = MigrationRoutingStrategy::NewPrimaryOnly.write_target(&migration);
        assert_eq!(target, WriteTarget::Single(2)); // to_node
    }

    #[test]
    fn test_router_route_write_single() {
        let router = MigrationRouter::new();
        let migration = test_migration();

        let decision = router.route_write(0, &migration, None);
        match decision {
            RoutingDecision::Single { node, shard_id } => {
                assert_eq!(node, 1); // from_node (old primary)
                assert_eq!(shard_id, 0);
            }
            _ => panic!("Expected Single routing decision"),
        }
    }

    #[test]
    fn test_router_route_write_dual() {
        let router = MigrationRouter::with_strategy(MigrationRoutingStrategy::DualWrites);
        let migration = test_migration();

        let decision = router.route_write(0, &migration, None);
        match decision {
            RoutingDecision::DualWrite {
                primary,
                secondary,
                shard_id,
            } => {
                assert_eq!(primary, 1);
                assert_eq!(secondary, 2);
                assert_eq!(shard_id, 0);
            }
            _ => panic!("Expected DualWrite routing decision"),
        }
    }

    #[test]
    fn test_router_route_write_blocked() {
        let router = MigrationRouter::with_strategy(MigrationRoutingStrategy::BlockWrites);
        let migration = test_migration();

        let decision = router.route_write(0, &migration, None);
        match decision {
            RoutingDecision::Blocked { shard_id, .. } => {
                assert_eq!(shard_id, 0);
            }
            _ => panic!("Expected Blocked routing decision"),
        }
    }

    #[test]
    fn test_router_route_read() {
        let router = MigrationRouter::new();
        let migration = test_migration();

        let decision = router.route_read(0, &migration, None);
        match decision {
            RoutingDecision::WithFallback {
                primary,
                fallback,
                shard_id,
            } => {
                assert_eq!(primary, 1); // from_node preferred
                assert_eq!(fallback, 2); // to_node as fallback
                assert_eq!(shard_id, 0);
            }
            _ => panic!("Expected WithFallback routing decision"),
        }
    }

    #[test]
    fn test_routing_decision_methods() {
        let decision = RoutingDecision::Single {
            node: 1,
            shard_id: 5,
        };
        assert_eq!(decision.shard_id(), 5);
        assert!(decision.is_allowed());
        assert_eq!(decision.primary_node(), Some(1));

        let blocked = RoutingDecision::Blocked {
            shard_id: 3,
            reason: "test".to_string(),
        };
        assert!(!blocked.is_allowed());
        assert_eq!(blocked.primary_node(), None);
    }

    #[test]
    fn test_dual_write_result() {
        let success = DualWriteResult::new(Ok(()), Ok(()));
        assert!(success.is_success());
        assert!(success.both_succeeded());

        let primary_only =
            DualWriteResult::new(Ok(()), Err(Error::Internal("secondary failed".to_string())));
        assert!(primary_only.is_success());
        assert!(!primary_only.both_succeeded());

        let failure =
            DualWriteResult::new(Err(Error::Internal("primary failed".to_string())), Ok(()));
        assert!(!failure.is_success());
    }

    #[test]
    fn test_migration_routing_config() {
        let config = MigrationRoutingConfig::with_strategy(MigrationRoutingStrategy::DualWrites)
            .with_write_timeout_ms(10000)
            .with_logging();

        assert_eq!(
            config.default_strategy,
            MigrationRoutingStrategy::DualWrites
        );
        assert_eq!(config.write_timeout_ms, 10000);
        assert!(config.log_routing_decisions);
    }

    #[test]
    fn test_set_default_strategy() {
        let router = MigrationRouter::new();
        assert_eq!(
            router.default_strategy(),
            MigrationRoutingStrategy::OldPrimaryWritesDualReads
        );

        router.set_default_strategy(MigrationRoutingStrategy::BlockWrites);
        assert_eq!(
            router.default_strategy(),
            MigrationRoutingStrategy::BlockWrites
        );
    }
}
