//! Metrics registry for initializing and managing all metrics.
//!
//! This module provides `MetricsRegistry` for registering metric descriptions
//! and initializing the metrics system.

#[cfg(feature = "metrics")]
use crate::metrics::descriptors::*;

/// Result type for metrics operations.
pub type MetricsResult<T> = Result<T, MetricsError>;

/// Error type for metrics operations.
#[derive(Debug, Clone)]
pub enum MetricsError {
    /// Failed to install recorder.
    RecorderInstallFailed(String),
    /// Configuration error.
    ConfigError(String),
}

impl std::fmt::Display for MetricsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricsError::RecorderInstallFailed(msg) => {
                write!(f, "Failed to install metrics recorder: {}", msg)
            }
            MetricsError::ConfigError(msg) => write!(f, "Metrics configuration error: {}", msg),
        }
    }
}

impl std::error::Error for MetricsError {}

/// Configuration for the metrics registry.
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// Node ID to use as a default label.
    pub node_id: Option<String>,
    /// Whether to enable Prometheus exporter.
    pub prometheus_enabled: bool,
    /// Address for Prometheus exporter to listen on (if enabled).
    pub prometheus_listen_addr: Option<std::net::SocketAddr>,
    /// Whether to add default labels to all metrics.
    pub add_default_labels: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            node_id: None,
            prometheus_enabled: false,
            prometheus_listen_addr: None,
            add_default_labels: true,
        }
    }
}

impl MetricsConfig {
    /// Create a new metrics configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the node ID.
    pub fn with_node_id(mut self, node_id: impl Into<String>) -> Self {
        self.node_id = Some(node_id.into());
        self
    }

    /// Enable Prometheus exporter.
    pub fn with_prometheus(mut self, listen_addr: std::net::SocketAddr) -> Self {
        self.prometheus_enabled = true;
        self.prometheus_listen_addr = Some(listen_addr);
        self
    }

    /// Disable default labels.
    pub fn without_default_labels(mut self) -> Self {
        self.add_default_labels = false;
        self
    }
}

/// Metrics registry for initializing and describing all metrics.
///
/// The registry handles:
/// - Describing all metrics with their help text
/// - Optionally initializing the Prometheus exporter
/// - Providing a consistent interface for metrics operations
#[derive(Debug)]
pub struct MetricsRegistry {
    config: MetricsConfig,
    #[cfg(feature = "metrics")]
    initialized: std::sync::atomic::AtomicBool,
}

impl MetricsRegistry {
    /// Create a new metrics registry with default configuration.
    pub fn new() -> Self {
        Self::with_config(MetricsConfig::default())
    }

    /// Create a new metrics registry with the given configuration.
    pub fn with_config(config: MetricsConfig) -> Self {
        Self {
            config,
            #[cfg(feature = "metrics")]
            initialized: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Initialize the metrics system.
    ///
    /// This should be called once at application startup.
    /// When the `metrics` feature is enabled, this will:
    /// - Install the Prometheus recorder if configured
    /// - Register all metric descriptions
    ///
    /// When the feature is disabled, this is a no-op.
    #[cfg(feature = "metrics")]
    pub fn init(&self) -> MetricsResult<()> {
        use std::sync::atomic::Ordering;

        // Only initialize once
        if self
            .initialized
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }

        // Register all metric descriptions
        self.describe_all_metrics();

        Ok(())
    }

    #[cfg(not(feature = "metrics"))]
    pub fn init(&self) -> MetricsResult<()> {
        Ok(())
    }

    /// Describe all metrics with their help text and units.
    #[cfg(feature = "metrics")]
    fn describe_all_metrics(&self) {
        self.describe_cache_metrics();
        self.describe_raft_metrics();
        self.describe_transport_metrics();
        self.describe_cluster_metrics();
        self.describe_network_metrics();
        self.describe_shard_metrics();
        self.describe_migration_metrics();
        self.describe_checkpoint_metrics();
        self.describe_slot_metrics();
        self.describe_error_metrics();
    }

    /// Describe cache subsystem metrics.
    #[cfg(feature = "metrics")]
    fn describe_cache_metrics(&self) {
        use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

        describe_counter!(CACHE_GET_TOTAL, CACHE_GET_TOTAL_DESC);
        describe_histogram!(
            CACHE_GET_DURATION_SECONDS,
            Unit::Seconds,
            CACHE_GET_DURATION_SECONDS_DESC
        );
        describe_counter!(CACHE_PUT_TOTAL, CACHE_PUT_TOTAL_DESC);
        describe_histogram!(
            CACHE_PUT_DURATION_SECONDS,
            Unit::Seconds,
            CACHE_PUT_DURATION_SECONDS_DESC
        );
        describe_counter!(CACHE_DELETE_TOTAL, CACHE_DELETE_TOTAL_DESC);
        describe_histogram!(
            CACHE_DELETE_DURATION_SECONDS,
            Unit::Seconds,
            CACHE_DELETE_DURATION_SECONDS_DESC
        );
        describe_counter!(CACHE_CLEAR_TOTAL, CACHE_CLEAR_TOTAL_DESC);
        describe_counter!(CACHE_PUT_LOCAL_TOTAL, CACHE_PUT_LOCAL_TOTAL_DESC);
        describe_gauge!(CACHE_ENTRIES, CACHE_ENTRIES_DESC);
        describe_gauge!(CACHE_SIZE_BYTES, Unit::Bytes, CACHE_SIZE_BYTES_DESC);
        describe_counter!(CACHE_EVICTIONS_TOTAL, CACHE_EVICTIONS_TOTAL_DESC);
        describe_histogram!(CACHE_KEY_SIZE_BYTES, Unit::Bytes, CACHE_KEY_SIZE_BYTES_DESC);
        describe_histogram!(
            CACHE_VALUE_SIZE_BYTES,
            Unit::Bytes,
            CACHE_VALUE_SIZE_BYTES_DESC
        );
        describe_histogram!(CACHE_TTL_SECONDS, Unit::Seconds, CACHE_TTL_SECONDS_DESC);
    }

    /// Describe Raft consensus subsystem metrics.
    #[cfg(feature = "metrics")]
    fn describe_raft_metrics(&self) {
        use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

        describe_counter!(RAFT_PROPOSALS_TOTAL, RAFT_PROPOSALS_TOTAL_DESC);
        describe_histogram!(
            RAFT_PROPOSAL_DURATION_SECONDS,
            Unit::Seconds,
            RAFT_PROPOSAL_DURATION_SECONDS_DESC
        );
        describe_gauge!(RAFT_TERM, RAFT_TERM_DESC);
        describe_gauge!(RAFT_COMMIT_INDEX, RAFT_COMMIT_INDEX_DESC);
        describe_gauge!(RAFT_APPLIED_INDEX, RAFT_APPLIED_INDEX_DESC);
        describe_gauge!(RAFT_COMMIT_APPLY_LAG, RAFT_COMMIT_APPLY_LAG_DESC);
        describe_gauge!(RAFT_IS_LEADER, RAFT_IS_LEADER_DESC);
        describe_gauge!(RAFT_LEADER_ID, RAFT_LEADER_ID_DESC);
        describe_gauge!(RAFT_PEERS, RAFT_PEERS_DESC);
        describe_counter!(RAFT_ELECTIONS_TOTAL, RAFT_ELECTIONS_TOTAL_DESC);
        describe_histogram!(
            RAFT_ELECTION_DURATION_SECONDS,
            Unit::Seconds,
            RAFT_ELECTION_DURATION_SECONDS_DESC
        );
        describe_counter!(RAFT_MESSAGES_TOTAL, RAFT_MESSAGES_TOTAL_DESC);
        describe_counter!(RAFT_MESSAGES_BYTES_TOTAL, RAFT_MESSAGES_BYTES_TOTAL_DESC);
        describe_gauge!(
            RAFT_REPLICATION_LAG_ENTRIES,
            RAFT_REPLICATION_LAG_ENTRIES_DESC
        );
        describe_gauge!(RAFT_PENDING_PROPOSALS, RAFT_PENDING_PROPOSALS_DESC);
        describe_gauge!(RAFT_QUEUE_DEPTH, RAFT_QUEUE_DEPTH_DESC);
        describe_histogram!(
            RAFT_APPLY_DURATION_SECONDS,
            Unit::Seconds,
            RAFT_APPLY_DURATION_SECONDS_DESC
        );
        describe_histogram!(
            RAFT_STEP_DURATION_SECONDS,
            Unit::Seconds,
            RAFT_STEP_DURATION_SECONDS_DESC
        );
        describe_counter!(RAFT_CONF_CHANGE_TOTAL, RAFT_CONF_CHANGE_TOTAL_DESC);
        describe_counter!(RAFT_HEARTBEAT_TOTAL, RAFT_HEARTBEAT_TOTAL_DESC);
        describe_histogram!(
            RAFT_HEARTBEAT_LATENCY_SECONDS,
            Unit::Seconds,
            RAFT_HEARTBEAT_LATENCY_SECONDS_DESC
        );
        describe_gauge!(RAFT_LOG_ENTRIES, RAFT_LOG_ENTRIES_DESC);
    }

    /// Describe transport subsystem metrics.
    #[cfg(feature = "metrics")]
    fn describe_transport_metrics(&self) {
        use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

        describe_counter!(
            TRANSPORT_MESSAGES_SENT_TOTAL,
            TRANSPORT_MESSAGES_SENT_TOTAL_DESC
        );
        describe_counter!(
            TRANSPORT_MESSAGES_FAILED_TOTAL,
            TRANSPORT_MESSAGES_FAILED_TOTAL_DESC
        );
        describe_histogram!(
            TRANSPORT_SEND_DURATION_SECONDS,
            Unit::Seconds,
            TRANSPORT_SEND_DURATION_SECONDS_DESC
        );
        describe_gauge!(
            TRANSPORT_CONNECTIONS_ACTIVE,
            TRANSPORT_CONNECTIONS_ACTIVE_DESC
        );
        describe_counter!(
            TRANSPORT_CONNECTIONS_CREATED_TOTAL,
            TRANSPORT_CONNECTIONS_CREATED_TOTAL_DESC
        );
        describe_counter!(
            TRANSPORT_CONNECTIONS_FAILED_TOTAL,
            TRANSPORT_CONNECTIONS_FAILED_TOTAL_DESC
        );
        describe_histogram!(
            TRANSPORT_CONNECTION_DURATION_SECONDS,
            Unit::Seconds,
            TRANSPORT_CONNECTION_DURATION_SECONDS_DESC
        );
        describe_counter!(TRANSPORT_BYTES_SENT_TOTAL, TRANSPORT_BYTES_SENT_TOTAL_DESC);
        describe_counter!(
            TRANSPORT_BYTES_RECEIVED_TOTAL,
            TRANSPORT_BYTES_RECEIVED_TOTAL_DESC
        );
        describe_counter!(TRANSPORT_QUEUE_FULL_TOTAL, TRANSPORT_QUEUE_FULL_TOTAL_DESC);
        describe_gauge!(TRANSPORT_QUEUE_DEPTH, TRANSPORT_QUEUE_DEPTH_DESC);
        describe_counter!(TRANSPORT_RETRIES_TOTAL, TRANSPORT_RETRIES_TOTAL_DESC);
        describe_gauge!(TRANSPORT_PENDING_RETRIES, TRANSPORT_PENDING_RETRIES_DESC);
        describe_counter!(TRANSPORT_RECONNECTS_TOTAL, TRANSPORT_RECONNECTS_TOTAL_DESC);
        describe_histogram!(
            TRANSPORT_ENCODE_DURATION_SECONDS,
            Unit::Seconds,
            TRANSPORT_ENCODE_DURATION_SECONDS_DESC
        );
    }

    /// Describe cluster subsystem metrics.
    #[cfg(feature = "metrics")]
    fn describe_cluster_metrics(&self) {
        use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

        describe_gauge!(CLUSTER_NODES_TOTAL, CLUSTER_NODES_TOTAL_DESC);
        describe_gauge!(CLUSTER_NODES_HEALTHY, CLUSTER_NODES_HEALTHY_DESC);
        describe_counter!(CLUSTER_NODE_JOINS_TOTAL, CLUSTER_NODE_JOINS_TOTAL_DESC);
        describe_counter!(CLUSTER_NODE_LEAVES_TOTAL, CLUSTER_NODE_LEAVES_TOTAL_DESC);
        describe_histogram!(
            CLUSTER_JOIN_DURATION_SECONDS,
            Unit::Seconds,
            CLUSTER_JOIN_DURATION_SECONDS_DESC
        );
        describe_counter!(CLUSTER_HEALTH_CHECK_TOTAL, CLUSTER_HEALTH_CHECK_TOTAL_DESC);
        describe_histogram!(
            CLUSTER_HEALTH_CHECK_DURATION_SECONDS,
            Unit::Seconds,
            CLUSTER_HEALTH_CHECK_DURATION_SECONDS_DESC
        );
        describe_counter!(
            CLUSTER_GOSSIP_ROUNDS_TOTAL,
            CLUSTER_GOSSIP_ROUNDS_TOTAL_DESC
        );
        describe_counter!(
            CLUSTER_GOSSIP_MESSAGES_TOTAL,
            CLUSTER_GOSSIP_MESSAGES_TOTAL_DESC
        );
        describe_histogram!(
            CLUSTER_CONVERGENCE_DURATION_SECONDS,
            Unit::Seconds,
            CLUSTER_CONVERGENCE_DURATION_SECONDS_DESC
        );
        describe_histogram!(
            CLUSTER_DISCOVERY_DURATION_SECONDS,
            Unit::Seconds,
            CLUSTER_DISCOVERY_DURATION_SECONDS_DESC
        );
        describe_counter!(CLUSTER_EVENTS_TOTAL, CLUSTER_EVENTS_TOTAL_DESC);
    }

    /// Describe network subsystem metrics.
    #[cfg(feature = "metrics")]
    fn describe_network_metrics(&self) {
        use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

        describe_gauge!(NETWORK_CONNECTIONS_ACTIVE, NETWORK_CONNECTIONS_ACTIVE_DESC);
        describe_counter!(NETWORK_CONNECTIONS_TOTAL, NETWORK_CONNECTIONS_TOTAL_DESC);
        describe_counter!(
            NETWORK_CONNECTION_ERRORS_TOTAL,
            NETWORK_CONNECTION_ERRORS_TOTAL_DESC
        );
        describe_counter!(NETWORK_REQUESTS_TOTAL, NETWORK_REQUESTS_TOTAL_DESC);
        describe_histogram!(
            NETWORK_REQUEST_DURATION_SECONDS,
            Unit::Seconds,
            NETWORK_REQUEST_DURATION_SECONDS_DESC
        );
        describe_counter!(
            NETWORK_BYTES_RECEIVED_TOTAL,
            NETWORK_BYTES_RECEIVED_TOTAL_DESC
        );
        describe_counter!(NETWORK_BYTES_SENT_TOTAL, NETWORK_BYTES_SENT_TOTAL_DESC);
        describe_histogram!(
            NETWORK_MESSAGE_SIZE_BYTES,
            Unit::Bytes,
            NETWORK_MESSAGE_SIZE_BYTES_DESC
        );
        describe_counter!(
            NETWORK_DECODE_ERRORS_TOTAL,
            NETWORK_DECODE_ERRORS_TOTAL_DESC
        );
        describe_counter!(
            NETWORK_PROTOCOL_ERRORS_TOTAL,
            NETWORK_PROTOCOL_ERRORS_TOTAL_DESC
        );
    }

    /// Describe shard subsystem metrics.
    #[cfg(feature = "metrics")]
    fn describe_shard_metrics(&self) {
        use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

        describe_counter!(SHARD_OPERATIONS_TOTAL, SHARD_OPERATIONS_TOTAL_DESC);
        describe_histogram!(
            SHARD_OPERATION_DURATION_SECONDS,
            Unit::Seconds,
            SHARD_OPERATION_DURATION_SECONDS_DESC
        );
        describe_gauge!(SHARD_ENTRIES, SHARD_ENTRIES_DESC);
        describe_gauge!(SHARD_SIZE_BYTES, Unit::Bytes, SHARD_SIZE_BYTES_DESC);
        describe_gauge!(SHARD_IS_LEADER, SHARD_IS_LEADER_DESC);
        describe_counter!(SHARD_LEADER_CHANGES_TOTAL, SHARD_LEADER_CHANGES_TOTAL_DESC);
        describe_histogram!(
            SHARD_LEADER_TENURE_SECONDS,
            Unit::Seconds,
            SHARD_LEADER_TENURE_SECONDS_DESC
        );
        describe_counter!(SHARD_FORWARDS_TOTAL, SHARD_FORWARDS_TOTAL_DESC);
        describe_histogram!(
            SHARD_FORWARD_DURATION_SECONDS,
            Unit::Seconds,
            SHARD_FORWARD_DURATION_SECONDS_DESC
        );
        describe_gauge!(SHARD_FORWARD_PENDING, SHARD_FORWARD_PENDING_DESC);
        describe_histogram!(SHARD_FORWARD_HOPS, SHARD_FORWARD_HOPS_DESC);
        describe_counter!(SHARD_ROUTING_TOTAL, SHARD_ROUTING_TOTAL_DESC);
        describe_counter!(
            SHARD_ROUTING_CACHE_HITS_TOTAL,
            SHARD_ROUTING_CACHE_HITS_TOTAL_DESC
        );
        describe_gauge!(SHARD_COUNT, SHARD_COUNT_DESC);
        describe_counter!(SHARD_CREATED_TOTAL, SHARD_CREATED_TOTAL_DESC);
        describe_counter!(SHARD_DELETED_TOTAL, SHARD_DELETED_TOTAL_DESC);
    }

    /// Describe migration subsystem metrics.
    #[cfg(feature = "metrics")]
    fn describe_migration_metrics(&self) {
        use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

        describe_gauge!(MIGRATION_ACTIVE, MIGRATION_ACTIVE_DESC);
        describe_counter!(MIGRATION_TOTAL, MIGRATION_TOTAL_DESC);
        describe_histogram!(
            MIGRATION_DURATION_SECONDS,
            Unit::Seconds,
            MIGRATION_DURATION_SECONDS_DESC
        );
        describe_counter!(MIGRATION_ENTRIES_TOTAL, MIGRATION_ENTRIES_TOTAL_DESC);
        describe_counter!(MIGRATION_BYTES_TOTAL, MIGRATION_BYTES_TOTAL_DESC);
        describe_gauge!(
            MIGRATION_RATE_BYTES_PER_SECOND,
            MIGRATION_RATE_BYTES_PER_SECOND_DESC
        );
        describe_gauge!(MIGRATION_PHASE, MIGRATION_PHASE_DESC);
        describe_counter!(
            MIGRATION_BLOCKED_WRITES_TOTAL,
            MIGRATION_BLOCKED_WRITES_TOTAL_DESC
        );
        describe_counter!(
            MIGRATION_DUAL_WRITES_TOTAL,
            MIGRATION_DUAL_WRITES_TOTAL_DESC
        );
        describe_gauge!(MIGRATION_CATCHUP_LAG, MIGRATION_CATCHUP_LAG_DESC);
        describe_counter!(MIGRATION_RETRIES_TOTAL, MIGRATION_RETRIES_TOTAL_DESC);
        describe_histogram!(
            MIGRATION_PHASE_DURATION_SECONDS,
            Unit::Seconds,
            MIGRATION_PHASE_DURATION_SECONDS_DESC
        );
    }

    /// Describe checkpoint subsystem metrics.
    #[cfg(feature = "metrics")]
    fn describe_checkpoint_metrics(&self) {
        use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

        describe_counter!(CHECKPOINT_CREATED_TOTAL, CHECKPOINT_CREATED_TOTAL_DESC);
        describe_histogram!(
            CHECKPOINT_CREATE_DURATION_SECONDS,
            Unit::Seconds,
            CHECKPOINT_CREATE_DURATION_SECONDS_DESC
        );
        describe_gauge!(
            CHECKPOINT_SIZE_BYTES,
            Unit::Bytes,
            CHECKPOINT_SIZE_BYTES_DESC
        );
        describe_gauge!(CHECKPOINT_ENTRIES, CHECKPOINT_ENTRIES_DESC);
        describe_gauge!(
            CHECKPOINT_COMPRESSION_RATIO,
            CHECKPOINT_COMPRESSION_RATIO_DESC
        );
        describe_counter!(CHECKPOINT_LOAD_TOTAL, CHECKPOINT_LOAD_TOTAL_DESC);
        describe_histogram!(
            CHECKPOINT_LOAD_DURATION_SECONDS,
            Unit::Seconds,
            CHECKPOINT_LOAD_DURATION_SECONDS_DESC
        );
        describe_gauge!(CHECKPOINT_ENTRIES_SINCE, CHECKPOINT_ENTRIES_SINCE_DESC);
        describe_counter!(
            CHECKPOINT_BACKPRESSURE_TOTAL,
            CHECKPOINT_BACKPRESSURE_TOTAL_DESC
        );
        describe_counter!(CHECKPOINT_CLEANUP_TOTAL, CHECKPOINT_CLEANUP_TOTAL_DESC);
    }

    /// Describe slot routing subsystem metrics.
    #[cfg(feature = "metrics")]
    fn describe_slot_metrics(&self) {
        use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

        describe_counter!(SLOT_LOOKUPS_TOTAL, SLOT_LOOKUPS_TOTAL_DESC);
        describe_histogram!(
            SLOT_LOOKUP_DURATION_SECONDS,
            Unit::Seconds,
            SLOT_LOOKUP_DURATION_SECONDS_DESC
        );
        describe_gauge!(SLOT_EPOCH, SLOT_EPOCH_DESC);
        describe_counter!(SLOT_REASSIGNMENTS_TOTAL, SLOT_REASSIGNMENTS_TOTAL_DESC);
        describe_gauge!(SLOT_MIGRATIONS_ACTIVE, SLOT_MIGRATIONS_ACTIVE_DESC);
        describe_counter!(SLOT_MIGRATIONS_TOTAL, SLOT_MIGRATIONS_TOTAL_DESC);
        describe_counter!(SLOT_MIGRATION_KEYS_TOTAL, SLOT_MIGRATION_KEYS_TOTAL_DESC);
        describe_gauge!(SLOT_DISTRIBUTION, SLOT_DISTRIBUTION_DESC);
        describe_gauge!(SLOT_TOTAL, SLOT_TOTAL_DESC);
        describe_gauge!(SLOT_OWNED, SLOT_OWNED_DESC);
    }

    /// Describe error and retry metrics.
    #[cfg(feature = "metrics")]
    fn describe_error_metrics(&self) {
        use metrics::describe_counter;

        describe_counter!(ERRORS_TOTAL, ERRORS_TOTAL_DESC);
        describe_counter!(RETRIES_TOTAL, RETRIES_TOTAL_DESC);
        describe_counter!(RETRY_SUCCESS_TOTAL, RETRY_SUCCESS_TOTAL_DESC);
    }

    /// Get the configuration.
    pub fn config(&self) -> &MetricsConfig {
        &self.config
    }

    /// Get the node ID if set.
    pub fn node_id(&self) -> Option<&str> {
        self.config.node_id.as_deref()
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Global metrics registry instance.
static GLOBAL_REGISTRY: once_cell::sync::OnceCell<MetricsRegistry> =
    once_cell::sync::OnceCell::new();

/// Initialize the global metrics registry.
///
/// This should be called once at application startup.
/// Subsequent calls will be no-ops.
pub fn init_global_registry(config: MetricsConfig) -> MetricsResult<()> {
    let registry = GLOBAL_REGISTRY.get_or_init(|| MetricsRegistry::with_config(config));
    registry.init()
}

/// Get the global metrics registry.
///
/// Returns None if the registry hasn't been initialized.
pub fn global_registry() -> Option<&'static MetricsRegistry> {
    GLOBAL_REGISTRY.get()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_config_builder() {
        let config = MetricsConfig::new()
            .with_node_id("node-1")
            .without_default_labels();

        assert_eq!(config.node_id, Some("node-1".to_string()));
        assert!(!config.add_default_labels);
    }

    #[test]
    fn test_registry_creation() {
        let registry = MetricsRegistry::new();
        assert!(registry.config().node_id.is_none());
    }

    #[test]
    fn test_registry_init() {
        let registry = MetricsRegistry::with_config(MetricsConfig::new().with_node_id("test"));
        assert!(registry.init().is_ok());
    }
}
