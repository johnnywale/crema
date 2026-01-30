//! Route definitions for the dashboard API.

use axum::{
    extract::Path,
    routing::{delete, get, post},
    Router,
};
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

use crate::cache::DistributedCache;
use crate::dashboard::handlers::{
    cache::{delete_cache_value, get_cache_stats, get_cache_value, put_cache_value},
    cluster::{get_cluster_nodes, get_cluster_status, health_check},
    metrics::{get_metrics, get_prometheus_metrics},
    multiraft::{
        add_shard, get_multiraft_stats, get_shard_leaders, get_shard_routing, get_shards,
        get_slot_status, remove_shard,
    },
};
use crate::dashboard::sse::events_stream;
use crate::dashboard::static_files::serve_static;

/// Build the router with all dashboard routes.
pub fn build_router(cache: Arc<DistributedCache>) -> Router {
    // CORS configuration for development
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // API routes
    let api_routes = Router::new()
        // Cache operations
        .route("/cache/get/:key", get(get_cache_value))
        .route("/cache/put", post(put_cache_value))
        .route("/cache/delete/:key", delete(delete_cache_value))
        .route("/cache/stats", get(get_cache_stats))
        // Cluster status
        .route("/cluster/status", get(get_cluster_status))
        .route("/cluster/nodes", get(get_cluster_nodes))
        // Metrics
        .route("/metrics", get(get_metrics))
        .route("/metrics/prometheus", get(get_prometheus_metrics))
        // Multi-Raft
        .route("/multiraft/shards", get(get_shards))
        .route("/multiraft/stats", get(get_multiraft_stats))
        .route("/multiraft/leaders", get(get_shard_leaders))
        .route("/multiraft/routing/:key", get(get_shard_routing))
        // Slot routing and dynamic shard management
        .route("/multiraft/slots/status", get(get_slot_status))
        .route("/multiraft/shards/add", post(add_shard))
        .route("/multiraft/shards/:shard_id", delete(remove_shard))
        // SSE events
        .route("/events", get(events_stream))
        // Health check
        .route("/health", get(health_check))
        .with_state(cache.clone());

    // Static file handler for frontend
    let static_handler = get(|Path(path): Path<String>| async move { serve_static(&path).await });

    Router::new()
        .nest("/api", api_routes)
        .route("/", get(|| async { serve_static("index.html").await }))
        .route("/*path", static_handler)
        .layer(cors)
}
