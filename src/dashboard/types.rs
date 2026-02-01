//! API request and response types for the dashboard.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::multiraft::ShardId;
use crate::types::NodeId;

/// Request body for PUT operations.
#[derive(Debug, Deserialize)]
pub struct PutRequest {
    pub key: String,
    pub value: String,
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
}

/// Response for cache operations.
#[derive(Debug, Serialize)]
pub struct CacheResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<f64>,
}

impl CacheResponse {
    pub fn success() -> Self {
        Self {
            success: true,
            value: None,
            error: None,
            shard_id: None,
            latency_ms: None,
        }
    }

    pub fn success_with_value(value: String) -> Self {
        Self {
            success: true,
            value: Some(value),
            error: None,
            shard_id: None,
            latency_ms: None,
        }
    }

    pub fn not_found() -> Self {
        Self {
            success: true,
            value: None,
            error: None,
            shard_id: None,
            latency_ms: None,
        }
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Self {
            success: false,
            value: None,
            error: Some(msg.into()),
            shard_id: None,
            latency_ms: None,
        }
    }

    pub fn with_shard(mut self, shard_id: u32) -> Self {
        self.shard_id = Some(shard_id);
        self
    }

    pub fn with_latency(mut self, latency_ms: f64) -> Self {
        self.latency_ms = Some(latency_ms);
        self
    }
}

/// Cache statistics response.
#[derive(Debug, Serialize)]
pub struct CacheStatsResponse {
    pub entry_count: u64,
    pub weighted_size: u64,
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: f64,
}

/// Cluster status response.
#[derive(Debug, Serialize)]
pub struct ClusterStatusResponse {
    pub node_id: NodeId,
    pub leader_id: Option<NodeId>,
    pub is_leader: bool,
    pub term: u64,
    pub raft_peer_count: usize,
    pub discovered_node_count: usize,
    pub memberlist_node_count: usize,
    pub commit_index: u64,
    pub applied_index: u64,
}

/// Discovered node information.
#[derive(Debug, Serialize)]
pub struct NodeInfo {
    pub node_id: NodeId,
    pub is_healthy: bool,
}

/// Nodes list response.
#[derive(Debug, Serialize)]
pub struct NodesResponse {
    pub nodes: Vec<NodeInfo>,
}

/// Metrics response (JSON format).
#[derive(Debug, Serialize)]
pub struct MetricsResponse {
    pub get_total: u64,
    pub get_hits: u64,
    pub get_misses: u64,
    pub hit_rate: f64,
    pub put_total: u64,
    pub put_success: u64,
    pub put_failures: u64,
    pub delete_total: u64,
    pub cache_entries: i64,
    pub is_leader: bool,
    pub raft_term: i64,
    pub avg_get_latency_ms: f64,
    pub avg_put_latency_ms: f64,
    pub get_latency_p50_ms: f64,
    pub get_latency_p90_ms: f64,
    pub get_latency_p99_ms: f64,
    pub put_latency_p50_ms: f64,
    pub put_latency_p90_ms: f64,
    pub put_latency_p99_ms: f64,
    pub forward_total: u64,
    pub forward_success: u64,
    pub forward_failures: u64,
    pub forward_timeouts: u64,
    pub forward_pending: i64,
}

/// Shard information response.
#[derive(Debug, Clone, Serialize)]
pub struct ShardInfoResponse {
    pub shard_id: ShardId,
    pub is_active: bool,
    pub is_leader: bool,
    pub leader_id: Option<NodeId>,
    pub entry_count: u64,
    pub percentage: f64,
    pub term: u64,
    /// Number of slots owned by this shard.
    pub slot_count: usize,
    /// Number of slots being migrated TO this shard.
    pub incoming_slots: usize,
    /// Number of slots being migrated FROM this shard.
    pub outgoing_slots: usize,
}

/// Multi-Raft shards list response.
#[derive(Debug, Serialize)]
pub struct ShardsResponse {
    pub shards: Vec<ShardInfoResponse>,
    pub total_shards: u32,
    pub total_entries: u64,
}

/// Multi-Raft statistics response.
#[derive(Debug, Serialize)]
pub struct MultiRaftStatsResponse {
    pub enabled: bool,
    pub total_shards: u32,
    pub active_shards: u32,
    pub total_entries: u64,
    pub total_size_bytes: u64,
    pub local_leader_shards: u32,
    pub operations_total: u64,
    pub operations_per_sec: f64,
}

/// Shard leaders response.
#[derive(Debug, Serialize)]
pub struct ShardLeadersResponse {
    pub leaders: HashMap<ShardId, Option<NodeId>>,
}

/// Shard routing response.
#[derive(Debug, Serialize)]
pub struct ShardRoutingResponse {
    pub key: String,
    pub shard_id: ShardId,
    pub leader_id: Option<NodeId>,
}

/// SSE event types.
#[derive(Debug, Serialize)]
#[serde(tag = "type", content = "data")]
pub enum SseEvent {
    #[serde(rename = "metrics")]
    Metrics(MetricsResponse),
    #[serde(rename = "cluster_status")]
    ClusterStatus(ClusterStatusResponse),
    #[serde(rename = "shard_update")]
    ShardUpdate(ShardsResponse),
    #[serde(rename = "leader_change")]
    LeaderChange {
        shard_id: ShardId,
        new_leader: Option<NodeId>,
    },
}

/// Health check response.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub node_id: NodeId,
    pub is_leader: bool,
}

/// Slot routing status response.
#[derive(Debug, Serialize)]
pub struct SlotRoutingStatusResponse {
    /// Whether slot-based routing is enabled.
    pub enabled: bool,
    /// Current slot table epoch.
    pub epoch: u64,
    /// Total number of slots (always 1024).
    pub total_slots: usize,
    /// Number of active slot migrations.
    pub active_migrations: usize,
    /// Number of completed slot migrations.
    pub completed_migrations: usize,
    /// Number of failed slot migrations.
    pub failed_migrations: usize,
    /// Total keys migrated.
    pub total_keys_migrated: u64,
}

/// Add shard response.
#[derive(Debug, Serialize)]
pub struct AddShardResponse {
    /// Whether the operation succeeded.
    pub success: bool,
    /// The new shard ID (if successful).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<u32>,
    /// Number of slots assigned to the new shard.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slots_assigned: Option<usize>,
    /// New slot table epoch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_epoch: Option<u64>,
    /// Error message (if failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Remove shard response.
#[derive(Debug, Serialize)]
pub struct RemoveShardResponse {
    /// Whether the operation succeeded.
    pub success: bool,
    /// The shard ID being removed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<u32>,
    /// Number of slots being redistributed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slots_redistributed: Option<usize>,
    /// New slot table epoch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_epoch: Option<u64>,
    /// Error message (if failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ============================================================================
// Comprehensive Metrics Types
// ============================================================================

/// A single metric value with metadata.
#[derive(Debug, Clone, Serialize)]
pub struct MetricValue {
    /// Metric name.
    pub name: String,
    /// Metric description.
    pub description: String,
    /// Metric type (counter, gauge, histogram).
    pub metric_type: String,
    /// Current value (for counters and gauges).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<f64>,
    /// Histogram data (for histograms).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub histogram: Option<HistogramData>,
}

/// Histogram data.
#[derive(Debug, Clone, Serialize)]
pub struct HistogramData {
    pub count: u64,
    pub sum: f64,
    pub mean: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
}

/// Metrics grouped by category.
#[derive(Debug, Clone, Serialize)]
pub struct MetricCategory {
    /// Category name.
    pub name: String,
    /// Metrics in this category.
    pub metrics: Vec<MetricValue>,
}

/// Comprehensive metrics response with all metrics.
#[derive(Debug, Serialize)]
pub struct ComprehensiveMetricsResponse {
    /// Node ID.
    pub node_id: NodeId,
    /// Timestamp of the snapshot.
    pub timestamp_ms: u64,
    /// All metrics grouped by category.
    pub categories: Vec<MetricCategory>,
    /// Total metric count.
    pub total_metrics: usize,
}

// ============================================================================
// Cluster-Wide Shard Comparison Types
// ============================================================================

/// Shard information for a specific node.
#[derive(Debug, Clone, Serialize)]
pub struct NodeShardInfo {
    /// Node ID.
    pub node_id: NodeId,
    /// Whether this node responded.
    pub reachable: bool,
    /// Error message if not reachable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Shard information from this node.
    pub shards: Vec<ShardInfoResponse>,
}

/// Shard difference between nodes.
#[derive(Debug, Clone, Serialize)]
pub struct ShardDifference {
    /// Shard ID.
    pub shard_id: ShardId,
    /// Difference type.
    pub diff_type: String,
    /// Description of the difference.
    pub description: String,
    /// Node A value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_a_value: Option<String>,
    /// Node B value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_b_value: Option<String>,
}

/// Cluster-wide shard comparison response.
#[derive(Debug, Serialize)]
pub struct ClusterShardsComparisonResponse {
    /// Local node ID.
    pub local_node_id: NodeId,
    /// Shard info from all nodes.
    pub nodes: Vec<NodeShardInfo>,
    /// Differences detected between nodes.
    pub differences: Vec<ShardDifference>,
    /// Whether all nodes are consistent.
    pub is_consistent: bool,
    /// Summary of the comparison.
    pub summary: String,
}
