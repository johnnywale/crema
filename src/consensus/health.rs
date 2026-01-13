//! Health checking and monitoring for Raft nodes.
//!
//! This module provides health status tracking and reporting
//! for monitoring the state of Raft cluster nodes.

use crate::consensus::transport::TransportMetricsSnapshot;
use crate::types::NodeId;
use std::time::SystemTime;

/// Health status of a Raft node.
#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    /// Node is operating normally
    Healthy,
    /// Node has minor issues that may need attention
    Warning { reason: String },
    /// Node is experiencing significant issues
    Degraded { reason: String },
    /// Node is in a critical state requiring immediate attention
    Critical { reason: String },
}

impl HealthStatus {
    /// Check if the status indicates a healthy node.
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthStatus::Healthy)
    }

    /// Check if the status indicates a problem.
    pub fn is_problem(&self) -> bool {
        !self.is_healthy()
    }

    /// Get the severity level (0 = healthy, 1 = warning, 2 = degraded, 3 = critical).
    pub fn severity(&self) -> u8 {
        match self {
            HealthStatus::Healthy => 0,
            HealthStatus::Warning { .. } => 1,
            HealthStatus::Degraded { .. } => 2,
            HealthStatus::Critical { .. } => 3,
        }
    }

    /// Get the reason string, if any.
    pub fn reason(&self) -> Option<&str> {
        match self {
            HealthStatus::Healthy => None,
            HealthStatus::Warning { reason }
            | HealthStatus::Degraded { reason }
            | HealthStatus::Critical { reason } => Some(reason),
        }
    }
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "healthy"),
            HealthStatus::Warning { reason } => write!(f, "warning: {}", reason),
            HealthStatus::Degraded { reason } => write!(f, "degraded: {}", reason),
            HealthStatus::Critical { reason } => write!(f, "critical: {}", reason),
        }
    }
}

/// Comprehensive health report for a Raft node.
#[derive(Debug, Clone)]
pub struct HealthReport {
    /// Overall health status
    pub status: HealthStatus,
    /// Node identifier
    pub node_id: NodeId,
    /// Whether this node is the leader
    pub is_leader: bool,
    /// Current leader ID, if known
    pub leader_id: Option<NodeId>,
    /// Current Raft term
    pub term: u64,
    /// Committed log index
    pub commit_index: u64,
    /// Applied log index
    pub applied_index: u64,
    /// Transport metrics snapshot
    pub metrics: TransportMetricsSnapshot,
    /// Timestamp of this report
    pub timestamp: SystemTime,
}

impl HealthReport {
    /// Get the commit lag (difference between commit and applied index).
    pub fn commit_lag(&self) -> u64 {
        self.commit_index.saturating_sub(self.applied_index)
    }

    /// Check if there is significant commit lag.
    pub fn has_significant_lag(&self) -> bool {
        self.commit_lag() > 1000
    }
}

/// Configuration for health checks.
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Drop rate threshold for warning (0.0 - 1.0)
    pub drop_rate_warning: f64,
    /// Drop rate threshold for degraded (0.0 - 1.0)
    pub drop_rate_degraded: f64,
    /// Drop rate threshold for critical (0.0 - 1.0)
    pub drop_rate_critical: f64,
    /// Backpressure event threshold for warning
    pub backpressure_warning: u64,
    /// Backpressure event threshold for degraded
    pub backpressure_degraded: u64,
    /// Queue usage threshold for warning (0.0 - 1.0)
    pub queue_usage_warning: f64,
    /// Queue usage threshold for degraded (0.0 - 1.0)
    pub queue_usage_degraded: f64,
    /// Commit lag threshold for warning
    pub commit_lag_warning: u64,
    /// Commit lag threshold for degraded
    pub commit_lag_degraded: u64,
    /// Queue capacity for usage calculation
    pub queue_capacity: usize,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            drop_rate_warning: 0.01,    // 1%
            drop_rate_degraded: 0.05,   // 5%
            drop_rate_critical: 0.10,   // 10%
            backpressure_warning: 100,
            backpressure_degraded: 1000,
            queue_usage_warning: 0.7,   // 70%
            queue_usage_degraded: 0.9,  // 90%
            commit_lag_warning: 1000,
            commit_lag_degraded: 10000,
            queue_capacity: 1000,
        }
    }
}

/// Health checker that evaluates node health based on metrics.
#[derive(Debug, Clone)]
pub struct HealthChecker {
    config: HealthCheckConfig,
}

impl HealthChecker {
    /// Create a new health checker with default configuration.
    pub fn new() -> Self {
        Self {
            config: HealthCheckConfig::default(),
        }
    }

    /// Create a new health checker with custom configuration.
    pub fn with_config(config: HealthCheckConfig) -> Self {
        Self { config }
    }

    /// Perform a health check and return the status.
    pub fn check(
        &self,
        metrics: &TransportMetricsSnapshot,
        has_leader: bool,
        commit_index: u64,
        applied_index: u64,
    ) -> HealthStatus {
        // Check 1: Leadership
        if !has_leader {
            return HealthStatus::Critical {
                reason: "No leader elected".to_string(),
            };
        }

        // Check 2: Message drop rate
        let drop_rate = self.calculate_drop_rate(metrics);
        if drop_rate > self.config.drop_rate_critical {
            return HealthStatus::Critical {
                reason: format!(
                    "Critical message drop rate: {:.2}% ({}/{} dropped)",
                    drop_rate * 100.0,
                    metrics.messages_failed,
                    metrics.messages_sent
                ),
            };
        } else if drop_rate > self.config.drop_rate_degraded {
            return HealthStatus::Degraded {
                reason: format!("High message drop rate: {:.2}%", drop_rate * 100.0),
            };
        } else if drop_rate > self.config.drop_rate_warning {
            return HealthStatus::Warning {
                reason: format!("Elevated message drop rate: {:.2}%", drop_rate * 100.0),
            };
        }

        // Check 3: Backpressure events - use connection_retries as an indicator
        let backpressure_events = metrics.connection_retries;
        if backpressure_events > self.config.backpressure_degraded {
            return HealthStatus::Degraded {
                reason: format!("Frequent connection retries: {}", backpressure_events),
            };
        } else if backpressure_events > self.config.backpressure_warning {
            return HealthStatus::Warning {
                reason: "Moderate connection retries detected".to_string(),
            };
        }

        // Check 4: Pending retries (queue pressure indicator)
        let pending = metrics.pending_retries;
        let pending_usage = pending as f64 / 10.0; // Assuming max_pending_retries is 10
        if pending_usage > self.config.queue_usage_degraded {
            return HealthStatus::Degraded {
                reason: format!(
                    "Retry queue nearly full: {:.1}% ({} pending)",
                    pending_usage * 100.0,
                    pending
                ),
            };
        } else if pending_usage > self.config.queue_usage_warning {
            return HealthStatus::Warning {
                reason: format!("Retry queue usage high: {:.1}%", pending_usage * 100.0),
            };
        }

        // Check 5: Commit lag
        let lag = commit_index.saturating_sub(applied_index);
        if lag > self.config.commit_lag_degraded {
            return HealthStatus::Degraded {
                reason: format!("High commit lag: {} entries", lag),
            };
        } else if lag > self.config.commit_lag_warning {
            return HealthStatus::Warning {
                reason: format!("Moderate commit lag: {} entries", lag),
            };
        }

        HealthStatus::Healthy
    }

    /// Calculate the drop rate from metrics.
    fn calculate_drop_rate(&self, metrics: &TransportMetricsSnapshot) -> f64 {
        let total = metrics.messages_sent + metrics.messages_failed;
        if total > 0 {
            metrics.messages_failed as f64 / total as f64
        } else {
            0.0
        }
    }

    /// Generate a full health report.
    pub fn generate_report(
        &self,
        node_id: NodeId,
        is_leader: bool,
        leader_id: Option<NodeId>,
        term: u64,
        commit_index: u64,
        applied_index: u64,
        metrics: TransportMetricsSnapshot,
        has_leader: bool,
    ) -> HealthReport {
        let status = self.check(&metrics, has_leader, commit_index, applied_index);

        HealthReport {
            status,
            node_id,
            is_leader,
            leader_id,
            term,
            commit_index,
            applied_index,
            metrics,
            timestamp: SystemTime::now(),
        }
    }
}

impl Default for HealthChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_metrics() -> TransportMetricsSnapshot {
        TransportMetricsSnapshot {
            messages_sent: 1000,
            messages_failed: 0,
            high_priority_sent: 100,
            normal_priority_sent: 900,
            connections_created: 5,
            connections_failed: 0,
            active_connections: 5,
            average_send_latency_us: 100,
            connection_retries: 0,
            messages_dropped_queue_full: 0,
            pending_retries: 0,
            background_reconnect_attempts: 0,
        }
    }

    #[test]
    fn test_healthy_status() {
        let checker = HealthChecker::new();
        let metrics = create_test_metrics();

        let status = checker.check(&metrics, true, 100, 100);
        assert!(status.is_healthy());
    }

    #[test]
    fn test_no_leader_critical() {
        let checker = HealthChecker::new();
        let metrics = create_test_metrics();

        let status = checker.check(&metrics, false, 100, 100);
        assert_eq!(status.severity(), 3); // Critical
    }

    #[test]
    fn test_high_drop_rate() {
        let checker = HealthChecker::new();
        let mut metrics = create_test_metrics();
        metrics.messages_failed = 150; // 15% of 1000

        let status = checker.check(&metrics, true, 100, 100);
        assert_eq!(status.severity(), 3); // Critical (>10%)
    }

    #[test]
    fn test_commit_lag_warning() {
        let checker = HealthChecker::new();
        let metrics = create_test_metrics();

        let status = checker.check(&metrics, true, 2000, 500); // 1500 lag
        assert_eq!(status.severity(), 1); // Warning
    }

    #[test]
    fn test_health_status_display() {
        let healthy = HealthStatus::Healthy;
        assert_eq!(format!("{}", healthy), "healthy");

        let warning = HealthStatus::Warning {
            reason: "test".to_string(),
        };
        assert_eq!(format!("{}", warning), "warning: test");
    }
}
