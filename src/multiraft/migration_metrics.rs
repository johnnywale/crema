//! Metrics for shard migration observability.
//!
//! Provides Prometheus-style metrics for monitoring migration operations.

use crate::metrics::{Counter, Gauge, Histogram, CACHE_LATENCY_BUCKETS};
use std::time::{Duration, Instant};

/// Metrics for migration operations.
#[derive(Debug)]
pub struct MigrationMetrics {
    /// Number of migrations currently in progress.
    pub migrations_active: Gauge,
    /// Total migrations started.
    pub migrations_started: Counter,
    /// Total migrations completed successfully.
    pub migrations_completed: Counter,
    /// Total migrations failed.
    pub migrations_failed: Counter,
    /// Total migrations cancelled.
    pub migrations_cancelled: Counter,
    /// Migration duration histogram (seconds).
    pub migration_duration: Histogram,
    /// Bytes transferred total.
    pub bytes_transferred: Counter,
    /// Entries transferred total.
    pub entries_transferred: Counter,
    /// Current transfer rate (bytes/sec).
    pub transfer_rate: Gauge,
    /// Whether migrations are paused.
    pub paused: Gauge,
    /// Shards currently migrating.
    pub shards_migrating: Gauge,
}

impl MigrationMetrics {
    /// Create new migration metrics.
    pub fn new() -> Self {
        Self {
            migrations_active: Gauge::new("migrations_active", "Number of active migrations"),
            migrations_started: Counter::new("migrations_started", "Total migrations started"),
            migrations_completed: Counter::new("migrations_completed", "Total migrations completed"),
            migrations_failed: Counter::new("migrations_failed", "Total migrations failed"),
            migrations_cancelled: Counter::new("migrations_cancelled", "Total migrations cancelled"),
            migration_duration: Histogram::with_buckets(
                "migration_duration",
                "Migration duration in seconds",
                CACHE_LATENCY_BUCKETS.to_vec(),
            ),
            bytes_transferred: Counter::new("migration_bytes", "Total bytes transferred"),
            entries_transferred: Counter::new("migration_entries", "Total entries transferred"),
            transfer_rate: Gauge::new("migration_rate", "Current transfer rate (bytes/sec)"),
            paused: Gauge::new("migrations_paused", "Whether migrations are paused"),
            shards_migrating: Gauge::new("shards_migrating", "Number of shards being migrated"),
        }
    }

    /// Record a migration start.
    pub fn record_migration_start(&self) {
        self.migrations_started.inc();
        self.migrations_active.inc();
        self.shards_migrating.inc();
    }

    /// Record a migration completion.
    pub fn record_migration_complete(&self, duration: Duration) {
        self.migrations_completed.inc();
        self.migrations_active.dec();
        self.shards_migrating.dec();
        self.migration_duration.observe(duration.as_secs_f64());
    }

    /// Record a migration failure.
    pub fn record_migration_failed(&self) {
        self.migrations_failed.inc();
        self.migrations_active.dec();
        self.shards_migrating.dec();
    }

    /// Record a migration cancellation.
    pub fn record_migration_cancelled(&self) {
        self.migrations_cancelled.inc();
        self.migrations_active.dec();
        self.shards_migrating.dec();
    }

    /// Record transfer progress.
    pub fn record_transfer(&self, entries: u64, bytes: u64, rate: f64) {
        self.entries_transferred.inc_by(entries);
        self.bytes_transferred.inc_by(bytes);
        self.transfer_rate.set(rate as i64);
    }

    /// Set paused state.
    pub fn set_paused(&self, paused: bool) {
        self.paused.set(if paused { 1 } else { 0 });
    }

    /// Get a snapshot of migration metrics.
    pub fn snapshot(&self) -> MigrationMetricsSnapshot {
        MigrationMetricsSnapshot {
            active: self.migrations_active.get() as u64,
            started: self.migrations_started.get(),
            completed: self.migrations_completed.get(),
            failed: self.migrations_failed.get(),
            cancelled: self.migrations_cancelled.get(),
            bytes_transferred: self.bytes_transferred.get(),
            entries_transferred: self.entries_transferred.get(),
            transfer_rate: self.transfer_rate.get() as f64,
            paused: self.paused.get() != 0,
        }
    }
}

impl Default for MigrationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of migration metrics.
#[derive(Debug, Clone)]
pub struct MigrationMetricsSnapshot {
    /// Number of active migrations.
    pub active: u64,
    /// Total migrations started.
    pub started: u64,
    /// Total migrations completed.
    pub completed: u64,
    /// Total migrations failed.
    pub failed: u64,
    /// Total migrations cancelled.
    pub cancelled: u64,
    /// Total bytes transferred.
    pub bytes_transferred: u64,
    /// Total entries transferred.
    pub entries_transferred: u64,
    /// Current transfer rate (bytes/sec).
    pub transfer_rate: f64,
    /// Whether migrations are paused.
    pub paused: bool,
}

impl MigrationMetricsSnapshot {
    /// Get success rate as percentage.
    pub fn success_rate(&self) -> f64 {
        let total = self.completed + self.failed;
        if total == 0 {
            return 100.0;
        }
        (self.completed as f64 / total as f64) * 100.0
    }
}

/// Timer for measuring migration duration.
pub struct MigrationTimer {
    start: Instant,
    metrics: Option<std::sync::Arc<MigrationMetrics>>,
    completed: bool,
}

impl MigrationTimer {
    /// Create a new timer.
    pub fn new(metrics: std::sync::Arc<MigrationMetrics>) -> Self {
        metrics.record_migration_start();
        Self {
            start: Instant::now(),
            metrics: Some(metrics),
            completed: false,
        }
    }

    /// Create a timer without metrics.
    pub fn without_metrics() -> Self {
        Self {
            start: Instant::now(),
            metrics: None,
            completed: false,
        }
    }

    /// Mark as completed successfully.
    pub fn complete(mut self) {
        if let Some(ref metrics) = self.metrics {
            metrics.record_migration_complete(self.start.elapsed());
        }
        self.completed = true;
    }

    /// Mark as failed.
    pub fn fail(mut self) {
        if let Some(ref metrics) = self.metrics {
            metrics.record_migration_failed();
        }
        self.completed = true;
    }

    /// Mark as cancelled.
    pub fn cancel(mut self) {
        if let Some(ref metrics) = self.metrics {
            metrics.record_migration_cancelled();
        }
        self.completed = true;
    }

    /// Get elapsed time.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Drop for MigrationTimer {
    fn drop(&mut self) {
        // If not explicitly completed, count as failed
        if !self.completed {
            if let Some(ref metrics) = self.metrics {
                metrics.record_migration_failed();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_metrics_recording() {
        let metrics = MigrationMetrics::new();

        metrics.record_migration_start();
        metrics.record_migration_start();
        assert_eq!(metrics.migrations_active.get(), 2);
        assert_eq!(metrics.migrations_started.get(), 2);

        metrics.record_migration_complete(Duration::from_secs(1));
        assert_eq!(metrics.migrations_active.get(), 1);
        assert_eq!(metrics.migrations_completed.get(), 1);

        metrics.record_migration_failed();
        assert_eq!(metrics.migrations_active.get(), 0);
        assert_eq!(metrics.migrations_failed.get(), 1);
    }

    #[test]
    fn test_transfer_metrics() {
        let metrics = MigrationMetrics::new();

        metrics.record_transfer(100, 10000, 1000.0);
        metrics.record_transfer(50, 5000, 500.0);

        assert_eq!(metrics.entries_transferred.get(), 150);
        assert_eq!(metrics.bytes_transferred.get(), 15000);
        assert_eq!(metrics.transfer_rate.get(), 500); // Last value
    }

    #[test]
    fn test_metrics_snapshot() {
        let metrics = MigrationMetrics::new();

        metrics.record_migration_start();
        metrics.record_migration_complete(Duration::from_secs(1));
        metrics.record_migration_start();
        metrics.record_migration_failed();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.active, 0);
        assert_eq!(snapshot.started, 2);
        assert_eq!(snapshot.completed, 1);
        assert_eq!(snapshot.failed, 1);
        assert_eq!(snapshot.success_rate(), 50.0);
    }

    #[test]
    fn test_migration_timer() {
        let metrics = Arc::new(MigrationMetrics::new());

        {
            let timer = MigrationTimer::new(metrics.clone());
            timer.complete();
        }
        assert_eq!(metrics.migrations_completed.get(), 1);

        {
            let timer = MigrationTimer::new(metrics.clone());
            timer.fail();
        }
        assert_eq!(metrics.migrations_failed.get(), 1);

        {
            let _timer = MigrationTimer::new(metrics.clone());
            // Drop without completing - should count as failed
        }
        assert_eq!(metrics.migrations_failed.get(), 2);
    }

    #[test]
    fn test_paused_state() {
        let metrics = MigrationMetrics::new();

        assert_eq!(metrics.paused.get(), 0);
        metrics.set_paused(true);
        assert_eq!(metrics.paused.get(), 1);
        metrics.set_paused(false);
        assert_eq!(metrics.paused.get(), 0);
    }
}
