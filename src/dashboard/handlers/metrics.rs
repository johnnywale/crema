//! Metrics handlers.

use axum::{
    extract::State,
    http::{header, StatusCode},
    response::IntoResponse,
    Json,
};

use crate::dashboard::handlers::AppState;
use crate::dashboard::types::MetricsResponse;

/// GET /api/metrics
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
pub async fn get_prometheus_metrics(State(cache): State<AppState>) -> impl IntoResponse {
    let prometheus_output = cache.metrics().to_prometheus();

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        prometheus_output,
    )
}
