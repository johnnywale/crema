//! Metrics handlers.
//!
//! Provides endpoints for metrics in both JSON and Prometheus formats.
//! When the `metrics` feature is enabled, the Prometheus endpoint uses
//! the native metrics-exporter-prometheus output alongside the legacy format.

use axum::{
    extract::State,
    http::{header, StatusCode},
    response::IntoResponse,
    Json,
};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::dashboard::handlers::AppState;
use crate::dashboard::types::{
    ComprehensiveMetricsResponse, HistogramData, MetricCategory, MetricValue, MetricsResponse,
};

/// GET /api/metrics
///
/// Returns cache metrics in JSON format for easy consumption by dashboards.
pub async fn get_metrics(State(cache): State<AppState>) -> Json<MetricsResponse> {
    let snapshot = cache.metrics().snapshot();

    let hit_rate = if snapshot.get_total > 0 {
        snapshot.get_hits as f64 / snapshot.get_total as f64
    } else {
        0.0
    };

    Json(MetricsResponse {
        get_total: snapshot.get_total,
        get_hits: snapshot.get_hits,
        get_misses: snapshot.get_misses,
        hit_rate,
        put_total: snapshot.put_total,
        put_success: snapshot.put_success,
        put_failures: snapshot.put_failures,
        delete_total: snapshot.delete_total,
        cache_entries: snapshot.cache_entries,
        is_leader: snapshot.is_leader,
        raft_term: snapshot.raft_term,
        avg_get_latency_ms: snapshot.get_latency.mean() * 1000.0,
        avg_put_latency_ms: snapshot.put_latency.mean() * 1000.0,
        get_latency_p50_ms: snapshot.get_latency.percentile(50.0) * 1000.0,
        get_latency_p90_ms: snapshot.get_latency.percentile(90.0) * 1000.0,
        get_latency_p99_ms: snapshot.get_latency.percentile(99.0) * 1000.0,
        put_latency_p50_ms: snapshot.put_latency.percentile(50.0) * 1000.0,
        put_latency_p90_ms: snapshot.put_latency.percentile(90.0) * 1000.0,
        put_latency_p99_ms: snapshot.put_latency.percentile(99.0) * 1000.0,
        forward_total: snapshot.forward_total,
        forward_success: snapshot.forward_success,
        forward_failures: snapshot.forward_failures,
        forward_timeouts: snapshot.forward_timeouts,
        forward_pending: snapshot.forward_pending,
    })
}

/// GET /api/metrics/prometheus
///
/// Returns metrics in Prometheus exposition format for scraping.
///
/// When the `metrics` feature is enabled, this combines output from both:
/// 1. The native `metrics-exporter-prometheus` recorder (comprehensive metrics)
/// 2. The legacy `CacheMetrics::to_prometheus()` (backward compatibility)
pub async fn get_prometheus_metrics(State(cache): State<AppState>) -> impl IntoResponse {
    let mut prometheus_output = String::new();

    // When metrics feature is enabled, try to get native prometheus output first
    #[cfg(feature = "metrics")]
    {
        let native_output = crate::metrics::prometheus::render_metrics();
        if !native_output.is_empty() {
            prometheus_output.push_str(&native_output);
            prometheus_output.push_str("\n# Legacy metrics below\n");
        }
    }

    // Always include legacy metrics for backward compatibility
    prometheus_output.push_str(&cache.metrics().to_prometheus());

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        prometheus_output,
    )
}

/// GET /api/metrics/comprehensive
///
/// Returns all metrics grouped by category with metadata.
/// Useful for dashboards that want to display all available metrics.
pub async fn get_comprehensive_metrics(
    State(cache): State<AppState>,
) -> Json<ComprehensiveMetricsResponse> {
    let snapshot = cache.metrics().snapshot();
    let node_id = cache.node_id();

    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let mut categories = Vec::new();

    // Cache Operations Category
    let cache_metrics = vec![
        MetricValue {
            name: "cache_get_total".to_string(),
            description: "Total GET operations".to_string(),
            metric_type: "counter".to_string(),
            value: Some(snapshot.get_total as f64),
            histogram: None,
        },
        MetricValue {
            name: "cache_get_hits".to_string(),
            description: "Successful cache hits".to_string(),
            metric_type: "counter".to_string(),
            value: Some(snapshot.get_hits as f64),
            histogram: None,
        },
        MetricValue {
            name: "cache_get_misses".to_string(),
            description: "Cache misses".to_string(),
            metric_type: "counter".to_string(),
            value: Some(snapshot.get_misses as f64),
            histogram: None,
        },
        MetricValue {
            name: "cache_hit_rate".to_string(),
            description: "Cache hit rate (0.0 - 1.0)".to_string(),
            metric_type: "gauge".to_string(),
            value: Some(if snapshot.get_total > 0 {
                snapshot.get_hits as f64 / snapshot.get_total as f64
            } else {
                0.0
            }),
            histogram: None,
        },
        MetricValue {
            name: "cache_put_total".to_string(),
            description: "Total PUT operations".to_string(),
            metric_type: "counter".to_string(),
            value: Some(snapshot.put_total as f64),
            histogram: None,
        },
        MetricValue {
            name: "cache_put_success".to_string(),
            description: "Successful PUT operations".to_string(),
            metric_type: "counter".to_string(),
            value: Some(snapshot.put_success as f64),
            histogram: None,
        },
        MetricValue {
            name: "cache_put_failures".to_string(),
            description: "Failed PUT operations".to_string(),
            metric_type: "counter".to_string(),
            value: Some(snapshot.put_failures as f64),
            histogram: None,
        },
        MetricValue {
            name: "cache_delete_total".to_string(),
            description: "Total DELETE operations".to_string(),
            metric_type: "counter".to_string(),
            value: Some(snapshot.delete_total as f64),
            histogram: None,
        },
        MetricValue {
            name: "cache_entries".to_string(),
            description: "Current number of cache entries".to_string(),
            metric_type: "gauge".to_string(),
            value: Some(snapshot.cache_entries as f64),
            histogram: None,
        },
    ];
    categories.push(MetricCategory {
        name: "Cache Operations".to_string(),
        metrics: cache_metrics,
    });

    // Latency Category
    let latency_metrics = vec![
        MetricValue {
            name: "get_latency".to_string(),
            description: "GET operation latency distribution".to_string(),
            metric_type: "histogram".to_string(),
            value: None,
            histogram: Some(HistogramData {
                count: snapshot.get_total,
                sum: snapshot.get_latency.mean() * snapshot.get_total as f64,
                mean: snapshot.get_latency.mean() * 1000.0,
                p50: snapshot.get_latency.percentile(50.0) * 1000.0,
                p90: snapshot.get_latency.percentile(90.0) * 1000.0,
                p99: snapshot.get_latency.percentile(99.0) * 1000.0,
            }),
        },
        MetricValue {
            name: "put_latency".to_string(),
            description: "PUT operation latency distribution".to_string(),
            metric_type: "histogram".to_string(),
            value: None,
            histogram: Some(HistogramData {
                count: snapshot.put_total,
                sum: snapshot.put_latency.mean() * snapshot.put_total as f64,
                mean: snapshot.put_latency.mean() * 1000.0,
                p50: snapshot.put_latency.percentile(50.0) * 1000.0,
                p90: snapshot.put_latency.percentile(90.0) * 1000.0,
                p99: snapshot.put_latency.percentile(99.0) * 1000.0,
            }),
        },
    ];
    categories.push(MetricCategory {
        name: "Latency".to_string(),
        metrics: latency_metrics,
    });

    // Raft/Consensus Category
    let raft_metrics = vec![
        MetricValue {
            name: "raft_is_leader".to_string(),
            description: "Whether this node is the Raft leader".to_string(),
            metric_type: "gauge".to_string(),
            value: Some(if snapshot.is_leader { 1.0 } else { 0.0 }),
            histogram: None,
        },
        MetricValue {
            name: "raft_term".to_string(),
            description: "Current Raft term".to_string(),
            metric_type: "gauge".to_string(),
            value: Some(snapshot.raft_term as f64),
            histogram: None,
        },
    ];
    categories.push(MetricCategory {
        name: "Raft/Consensus".to_string(),
        metrics: raft_metrics,
    });

    // Forwarding Category
    let forward_metrics = vec![
        MetricValue {
            name: "forward_total".to_string(),
            description: "Total forwarded requests".to_string(),
            metric_type: "counter".to_string(),
            value: Some(snapshot.forward_total as f64),
            histogram: None,
        },
        MetricValue {
            name: "forward_success".to_string(),
            description: "Successful forwarded requests".to_string(),
            metric_type: "counter".to_string(),
            value: Some(snapshot.forward_success as f64),
            histogram: None,
        },
        MetricValue {
            name: "forward_failures".to_string(),
            description: "Failed forwarded requests".to_string(),
            metric_type: "counter".to_string(),
            value: Some(snapshot.forward_failures as f64),
            histogram: None,
        },
        MetricValue {
            name: "forward_timeouts".to_string(),
            description: "Timed out forwarded requests".to_string(),
            metric_type: "counter".to_string(),
            value: Some(snapshot.forward_timeouts as f64),
            histogram: None,
        },
        MetricValue {
            name: "forward_pending".to_string(),
            description: "Currently pending forwarded requests".to_string(),
            metric_type: "gauge".to_string(),
            value: Some(snapshot.forward_pending as f64),
            histogram: None,
        },
    ];
    categories.push(MetricCategory {
        name: "Forwarding".to_string(),
        metrics: forward_metrics,
    });

    let total_metrics: usize = categories.iter().map(|c| c.metrics.len()).sum();

    Json(ComprehensiveMetricsResponse {
        node_id,
        timestamp_ms,
        categories,
        total_metrics,
    })
}
