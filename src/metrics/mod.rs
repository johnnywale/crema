//! Metrics module for monitoring and observability.
//!
//! This module provides Prometheus-style metrics for monitoring the cache:
//! - Counters for request counts, errors, etc.
//! - Gauges for current values like cache size, connections
//! - Histograms for latency distributions
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      CacheMetrics                            │
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
//! │  │  Counters    │  │   Gauges     │  │   Histograms     │  │
//! │  │ - requests   │  │ - cache_size │  │ - latencies      │  │
//! │  │ - hits/miss  │  │ - raft_peers │  │ - raft_propose   │  │
//! │  │ - errors     │  │ - is_leader  │  │ - rebalance_time │  │
//! │  └──────────────┘  └──────────────┘  └──────────────────┘  │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use distributed_cache::metrics::CacheMetrics;
//!
//! let metrics = CacheMetrics::new();
//!
//! // Record a cache hit
//! metrics.record_get(true, Duration::from_micros(50));
//!
//! // Record a put operation
//! metrics.record_put(true, Duration::from_millis(5));
//!
//! // Get current stats
//! let snapshot = metrics.snapshot();
//! println!("Hit rate: {:.2}%", snapshot.hit_rate() * 100.0);
//! ```

mod counters;
mod gauges;
mod histograms;

pub use counters::{Counter, LabeledCounter};
pub use gauges::{FloatGauge, Gauge, LabeledGauge};
pub use histograms::{Histogram, HistogramSnapshot, HistogramTimer, LabeledHistogram, DEFAULT_BUCKETS};

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Latency buckets optimized for cache operations (in seconds).
pub const CACHE_LATENCY_BUCKETS: &[f64] = &[
    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
];

/// Latency buckets for Raft operations (in seconds).
pub const RAFT_LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Comprehensive metrics for the distributed cache.
#[derive(Debug)]
pub struct CacheMetrics {
    // Request counters
    /// Total GET requests.
    pub get_total: Counter,
    /// GET cache hits.
    pub get_hits: Counter,
    /// GET cache misses.
    pub get_misses: Counter,
    /// Total PUT requests.
    pub put_total: Counter,
    /// Successful PUT requests.
    pub put_success: Counter,
    /// Failed PUT requests.
    pub put_failures: Counter,
    /// Total DELETE requests.
    pub delete_total: Counter,

    // Latency histograms
    /// GET operation latency.
    pub get_latency: Histogram,
    /// PUT operation latency.
    pub put_latency: Histogram,
    /// DELETE operation latency.
    pub delete_latency: Histogram,

    // Cache state gauges
    /// Current number of entries in cache.
    pub cache_entries: Gauge,
    /// Current cache size in bytes (estimated).
    pub cache_size_bytes: Gauge,

    // Raft metrics
    /// Total Raft proposals.
    pub raft_proposals_total: Counter,
    /// Failed Raft proposals.
    pub raft_proposals_failed: Counter,
    /// Raft proposal latency.
    pub raft_propose_latency: Histogram,
    /// Current Raft term.
    pub raft_term: Gauge,
    /// Current commit index.
    pub raft_commit_index: Gauge,
    /// Current applied index.
    pub raft_applied_index: Gauge,
    /// Number of Raft peers.
    pub raft_peers: Gauge,
    /// Whether this node is the leader.
    pub is_leader: AtomicBool,

    // Cluster metrics
    /// Total cluster nodes.
    pub cluster_nodes: Gauge,
    /// Nodes marked as healthy.
    pub cluster_healthy_nodes: Gauge,

    // Rebalancing metrics
    /// Total rebalancing operations.
    pub rebalance_total: Counter,
    /// Successful rebalancing operations.
    pub rebalance_success: Counter,
    /// Failed rebalancing operations.
    pub rebalance_failures: Counter,
    /// Rebalancing duration.
    pub rebalance_duration: Histogram,
    /// Entries transferred during rebalancing.
    pub rebalance_entries_transferred: Counter,

    // Checkpoint metrics
    /// Total snapshots created.
    pub snapshots_created: Counter,
    /// Total snapshots loaded.
    pub snapshots_loaded: Counter,
    /// Snapshot creation duration.
    pub snapshot_duration: Histogram,
    /// Last snapshot size in bytes.
    pub last_snapshot_size: Gauge,

    // Error counters by type
    /// Errors by type.
    pub errors: LabeledCounter<1>,
}

impl CacheMetrics {
    /// Create a new metrics instance.
    pub fn new() -> Self {
        Self {
            // Request counters
            get_total: Counter::new("cache_get_total", "Total GET requests"),
            get_hits: Counter::new("cache_get_hits", "GET cache hits"),
            get_misses: Counter::new("cache_get_misses", "GET cache misses"),
            put_total: Counter::new("cache_put_total", "Total PUT requests"),
            put_success: Counter::new("cache_put_success", "Successful PUT requests"),
            put_failures: Counter::new("cache_put_failures", "Failed PUT requests"),
            delete_total: Counter::new("cache_delete_total", "Total DELETE requests"),

            // Latency histograms
            get_latency: Histogram::with_buckets(
                "cache_get_latency_seconds",
                "GET operation latency",
                CACHE_LATENCY_BUCKETS.to_vec(),
            ),
            put_latency: Histogram::with_buckets(
                "cache_put_latency_seconds",
                "PUT operation latency",
                RAFT_LATENCY_BUCKETS.to_vec(),
            ),
            delete_latency: Histogram::with_buckets(
                "cache_delete_latency_seconds",
                "DELETE operation latency",
                RAFT_LATENCY_BUCKETS.to_vec(),
            ),

            // Cache state
            cache_entries: Gauge::new("cache_entries", "Current cache entries"),
            cache_size_bytes: Gauge::new("cache_size_bytes", "Current cache size in bytes"),

            // Raft metrics
            raft_proposals_total: Counter::new("raft_proposals_total", "Total Raft proposals"),
            raft_proposals_failed: Counter::new("raft_proposals_failed", "Failed Raft proposals"),
            raft_propose_latency: Histogram::with_buckets(
                "raft_propose_latency_seconds",
                "Raft proposal latency",
                RAFT_LATENCY_BUCKETS.to_vec(),
            ),
            raft_term: Gauge::new("raft_term", "Current Raft term"),
            raft_commit_index: Gauge::new("raft_commit_index", "Current Raft commit index"),
            raft_applied_index: Gauge::new("raft_applied_index", "Current Raft applied index"),
            raft_peers: Gauge::new("raft_peers", "Number of Raft peers"),
            is_leader: AtomicBool::new(false),

            // Cluster metrics
            cluster_nodes: Gauge::new("cluster_nodes", "Total cluster nodes"),
            cluster_healthy_nodes: Gauge::new("cluster_healthy_nodes", "Healthy cluster nodes"),

            // Rebalancing metrics
            rebalance_total: Counter::new("rebalance_total", "Total rebalancing operations"),
            rebalance_success: Counter::new("rebalance_success", "Successful rebalancing"),
            rebalance_failures: Counter::new("rebalance_failures", "Failed rebalancing"),
            rebalance_duration: Histogram::with_buckets(
                "rebalance_duration_seconds",
                "Rebalancing duration",
                vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0],
            ),
            rebalance_entries_transferred: Counter::new(
                "rebalance_entries_transferred",
                "Entries transferred during rebalancing",
            ),

            // Checkpoint metrics
            snapshots_created: Counter::new("snapshots_created", "Total snapshots created"),
            snapshots_loaded: Counter::new("snapshots_loaded", "Total snapshots loaded"),
            snapshot_duration: Histogram::with_buckets(
                "snapshot_duration_seconds",
                "Snapshot creation duration",
                vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0],
            ),
            last_snapshot_size: Gauge::new("last_snapshot_size_bytes", "Last snapshot size"),

            // Errors
            errors: LabeledCounter::new("cache_errors_total", "Errors by type", ["type"]),
        }
    }

    /// Record a GET operation.
    pub fn record_get(&self, hit: bool, latency: Duration) {
        self.get_total.inc();
        if hit {
            self.get_hits.inc();
        } else {
            self.get_misses.inc();
        }
        self.get_latency.observe_duration(latency);
    }

    /// Record a PUT operation.
    pub fn record_put(&self, success: bool, latency: Duration) {
        self.put_total.inc();
        if success {
            self.put_success.inc();
        } else {
            self.put_failures.inc();
        }
        self.put_latency.observe_duration(latency);
    }

    /// Record a DELETE operation.
    pub fn record_delete(&self, latency: Duration) {
        self.delete_total.inc();
        self.delete_latency.observe_duration(latency);
    }

    /// Record a Raft proposal.
    pub fn record_raft_proposal(&self, success: bool, latency: Duration) {
        self.raft_proposals_total.inc();
        if !success {
            self.raft_proposals_failed.inc();
        }
        self.raft_propose_latency.observe_duration(latency);
    }

    /// Record a rebalancing operation.
    pub fn record_rebalance(&self, success: bool, duration: Duration, entries: u64) {
        self.rebalance_total.inc();
        if success {
            self.rebalance_success.inc();
        } else {
            self.rebalance_failures.inc();
        }
        self.rebalance_duration.observe_duration(duration);
        self.rebalance_entries_transferred.inc_by(entries);
    }

    /// Record a snapshot operation.
    pub fn record_snapshot(&self, duration: Duration, size: u64) {
        self.snapshots_created.inc();
        self.snapshot_duration.observe_duration(duration);
        self.last_snapshot_size.set(size as i64);
    }

    /// Record an error.
    pub fn record_error(&self, error_type: &str) {
        self.errors.inc([error_type]);
    }

    /// Update leader status.
    pub fn set_leader(&self, is_leader: bool) {
        self.is_leader.store(is_leader, Ordering::Relaxed);
    }

    /// Check if this node is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Relaxed)
    }

    /// Update cache stats.
    pub fn update_cache_stats(&self, entries: u64, size_bytes: u64) {
        self.cache_entries.set(entries as i64);
        self.cache_size_bytes.set(size_bytes as i64);
    }

    /// Update Raft stats.
    pub fn update_raft_stats(&self, term: u64, commit: u64, applied: u64, peers: usize) {
        self.raft_term.set(term as i64);
        self.raft_commit_index.set(commit as i64);
        self.raft_applied_index.set(applied as i64);
        self.raft_peers.set(peers as i64);
    }

    /// Update cluster stats.
    pub fn update_cluster_stats(&self, total: usize, healthy: usize) {
        self.cluster_nodes.set(total as i64);
        self.cluster_healthy_nodes.set(healthy as i64);
    }

    /// Get a snapshot of current metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            get_total: self.get_total.get(),
            get_hits: self.get_hits.get(),
            get_misses: self.get_misses.get(),
            put_total: self.put_total.get(),
            put_success: self.put_success.get(),
            put_failures: self.put_failures.get(),
            delete_total: self.delete_total.get(),
            cache_entries: self.cache_entries.get(),
            is_leader: self.is_leader(),
            raft_term: self.raft_term.get(),
            get_latency: self.get_latency.snapshot(),
            put_latency: self.put_latency.snapshot(),
        }
    }

    /// Format metrics in Prometheus exposition format.
    pub fn to_prometheus(&self) -> String {
        let mut output = String::new();

        // Helper to add a metric
        macro_rules! add_counter {
            ($out:expr, $metric:expr) => {
                output.push_str(&format!(
                    "# HELP {} {}\n# TYPE {} counter\n{} {}\n",
                    $metric.name(),
                    $metric.help(),
                    $metric.name(),
                    $metric.name(),
                    $metric.get()
                ));
            };
        }

        macro_rules! add_gauge {
            ($out:expr, $metric:expr) => {
                output.push_str(&format!(
                    "# HELP {} {}\n# TYPE {} gauge\n{} {}\n",
                    $metric.name(),
                    $metric.help(),
                    $metric.name(),
                    $metric.name(),
                    $metric.get()
                ));
            };
        }

        // Counters
        add_counter!(output, self.get_total);
        add_counter!(output, self.get_hits);
        add_counter!(output, self.get_misses);
        add_counter!(output, self.put_total);
        add_counter!(output, self.put_success);
        add_counter!(output, self.put_failures);
        add_counter!(output, self.delete_total);

        // Gauges
        add_gauge!(output, self.cache_entries);
        add_gauge!(output, self.cache_size_bytes);
        add_gauge!(output, self.raft_term);
        add_gauge!(output, self.raft_commit_index);
        add_gauge!(output, self.raft_applied_index);
        add_gauge!(output, self.raft_peers);
        add_gauge!(output, self.cluster_nodes);
        add_gauge!(output, self.cluster_healthy_nodes);

        // Leader gauge
        output.push_str(&format!(
            "# HELP cache_is_leader Whether this node is the Raft leader\n\
             # TYPE cache_is_leader gauge\n\
             cache_is_leader {}\n",
            if self.is_leader() { 1 } else { 0 }
        ));

        // Histograms (simplified output)
        let get_snap = self.get_latency.snapshot();
        output.push_str(&format!(
            "# HELP cache_get_latency_seconds GET operation latency\n\
             # TYPE cache_get_latency_seconds histogram\n\
             cache_get_latency_seconds_sum {}\n\
             cache_get_latency_seconds_count {}\n",
            get_snap.sum, get_snap.count
        ));

        output
    }
}

impl Default for CacheMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A snapshot of cache metrics.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub get_total: u64,
    pub get_hits: u64,
    pub get_misses: u64,
    pub put_total: u64,
    pub put_success: u64,
    pub put_failures: u64,
    pub delete_total: u64,
    pub cache_entries: i64,
    pub is_leader: bool,
    pub raft_term: i64,
    pub get_latency: HistogramSnapshot,
    pub put_latency: HistogramSnapshot,
}

impl MetricsSnapshot {
    /// Calculate the cache hit rate.
    pub fn hit_rate(&self) -> f64 {
        if self.get_total == 0 {
            0.0
        } else {
            self.get_hits as f64 / self.get_total as f64
        }
    }

    /// Calculate the PUT success rate.
    pub fn put_success_rate(&self) -> f64 {
        if self.put_total == 0 {
            0.0
        } else {
            self.put_success as f64 / self.put_total as f64
        }
    }

    /// Get average GET latency in milliseconds.
    pub fn avg_get_latency_ms(&self) -> f64 {
        self.get_latency.mean() * 1000.0
    }

    /// Get average PUT latency in milliseconds.
    pub fn avg_put_latency_ms(&self) -> f64 {
        self.put_latency.mean() * 1000.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_metrics() {
        let metrics = CacheMetrics::new();

        // Record some operations
        metrics.record_get(true, Duration::from_micros(100));
        metrics.record_get(true, Duration::from_micros(200));
        metrics.record_get(false, Duration::from_micros(50));

        metrics.record_put(true, Duration::from_millis(5));
        metrics.record_put(false, Duration::from_millis(10));

        // Check counters
        assert_eq!(metrics.get_total.get(), 3);
        assert_eq!(metrics.get_hits.get(), 2);
        assert_eq!(metrics.get_misses.get(), 1);
        assert_eq!(metrics.put_total.get(), 2);
        assert_eq!(metrics.put_success.get(), 1);
        assert_eq!(metrics.put_failures.get(), 1);
    }

    #[test]
    fn test_metrics_snapshot() {
        let metrics = CacheMetrics::new();

        metrics.record_get(true, Duration::from_micros(100));
        metrics.record_get(true, Duration::from_micros(100));
        metrics.record_get(false, Duration::from_micros(100));

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.get_total, 3);
        assert!((snapshot.hit_rate() - 0.666).abs() < 0.01);
    }

    #[test]
    fn test_leader_status() {
        let metrics = CacheMetrics::new();

        assert!(!metrics.is_leader());

        metrics.set_leader(true);
        assert!(metrics.is_leader());

        metrics.set_leader(false);
        assert!(!metrics.is_leader());
    }

    #[test]
    fn test_prometheus_output() {
        let metrics = CacheMetrics::new();
        metrics.record_get(true, Duration::from_micros(100));

        let output = metrics.to_prometheus();

        assert!(output.contains("cache_get_total"));
        assert!(output.contains("cache_get_hits"));
        assert!(output.contains("TYPE"));
        assert!(output.contains("HELP"));
    }
}
