//! Cache operation handlers.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cache::DistributedCache;
use crate::dashboard::types::{CacheResponse, CacheStatsResponse, PutRequest};

/// Application state shared with handlers.
pub type AppState = Arc<DistributedCache>;

/// GET /api/cache/get/{key}
pub async fn get_cache_value(
    State(cache): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<CacheResponse>) {
    // Validate key
    if key.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(CacheResponse::error("key cannot be empty".to_string())),
        );
    }

    let start = Instant::now();

    let shard_id = cache.shard_for_key(key.as_bytes());

    match cache.get(key.as_bytes()).await {
        Some(value) => {
            let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
            let value_str = String::from_utf8_lossy(&value).to_string();
            let mut response =
                CacheResponse::success_with_value(value_str).with_latency(latency_ms);
            if let Some(shard) = shard_id {
                response = response.with_shard(shard);
            }
            (StatusCode::OK, Json(response))
        }
        None => {
            let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
            let mut response = CacheResponse::not_found().with_latency(latency_ms);
            if let Some(shard) = shard_id {
                response = response.with_shard(shard);
            }
            (StatusCode::OK, Json(response))
        }
    }
}

/// POST /api/cache/put
pub async fn put_cache_value(
    State(cache): State<AppState>,
    Json(request): Json<PutRequest>,
) -> (StatusCode, Json<CacheResponse>) {
    let start = Instant::now();

    let shard_id = cache.shard_for_key(request.key.as_bytes());

    let result = if let Some(ttl_seconds) = request.ttl_seconds {
        cache
            .put_with_ttl(
                request.key.clone(),
                request.value.clone(),
                Duration::from_secs(ttl_seconds),
            )
            .await
    } else {
        cache.put(request.key.clone(), request.value.clone()).await
    };

    let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

    match result {
        Ok(()) => {
            let mut response = CacheResponse::success().with_latency(latency_ms);
            if let Some(shard) = shard_id {
                response = response.with_shard(shard);
            }
            (StatusCode::OK, Json(response))
        }
        Err(e) => {
            let mut response = CacheResponse::error(e.to_string()).with_latency(latency_ms);
            if let Some(shard) = shard_id {
                response = response.with_shard(shard);
            }
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
        }
    }
}

/// DELETE /api/cache/delete/{key}
pub async fn delete_cache_value(
    State(cache): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<CacheResponse>) {
    // Validate key
    if key.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(CacheResponse::error("key cannot be empty".to_string())),
        );
    }

    let start = Instant::now();

    let shard_id = cache.shard_for_key(key.as_bytes());

    match cache.delete(key.clone()).await {
        Ok(()) => {
            let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
            let mut response = CacheResponse::success().with_latency(latency_ms);
            if let Some(shard) = shard_id {
                response = response.with_shard(shard);
            }
            (StatusCode::OK, Json(response))
        }
        Err(e) => {
            let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
            let mut response = CacheResponse::error(e.to_string()).with_latency(latency_ms);
            if let Some(shard) = shard_id {
                response = response.with_shard(shard);
            }
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
        }
    }
}

/// GET /api/cache/stats
pub async fn get_cache_stats(State(cache): State<AppState>) -> Json<CacheStatsResponse> {
    let stats = cache.stats();

    let hit_rate = if stats.hits + stats.misses > 0 {
        stats.hits as f64 / (stats.hits + stats.misses) as f64
    } else {
        0.0
    };

    Json(CacheStatsResponse {
        entry_count: stats.entry_count,
        weighted_size: stats.weighted_size,
        hits: stats.hits,
        misses: stats.misses,
        hit_rate,
    })
}
