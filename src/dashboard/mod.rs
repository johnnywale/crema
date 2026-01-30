//! Web dashboard for monitoring and managing the distributed cache.
//!
//! This module provides a web-based dashboard for:
//! - Interactive cache operations (GET/PUT/DELETE with TTL support)
//! - Real-time cluster monitoring (status, metrics, latency graphs)
//! - Multi-Raft visualization (shard distribution, leader mapping)
//!
//! # Example
//!
//! ```rust,ignore
//! use crema::{DistributedCache, CacheConfig};
//! use crema::dashboard::{DashboardConfig, DashboardServer};
//!
//! let cache = DistributedCache::new(config).await?;
//!
//! // Start dashboard server
//! let dashboard_config = DashboardConfig::default().with_port(8080);
//! let server = DashboardServer::new(cache.clone(), dashboard_config);
//! server.start().await?;
//! ```
//!
//! # API Endpoints
//!
//! ## Cache Operations
//! - `GET /api/cache/get/{key}` - Get value by key
//! - `POST /api/cache/put` - Put key-value with optional TTL
//! - `DELETE /api/cache/delete/{key}` - Delete key
//! - `GET /api/cache/stats` - Get cache statistics
//!
//! ## Cluster Status
//! - `GET /api/cluster/status` - Get cluster status
//! - `GET /api/cluster/nodes` - List discovered nodes
//!
//! ## Metrics
//! - `GET /api/metrics` - Get metrics as JSON
//! - `GET /api/metrics/prometheus` - Get metrics in Prometheus format
//!
//! ## Multi-Raft
//! - `GET /api/multiraft/shards` - List all shards
//! - `GET /api/multiraft/stats` - Get Multi-Raft statistics
//! - `GET /api/multiraft/leaders` - Get shard leader mapping
//! - `GET /api/multiraft/routing/{key}` - Get shard for key
//!
//! ## Real-Time (SSE)
//! - `GET /api/events` - Server-Sent Events stream

mod config;
mod handlers;
mod routes;
mod server;
mod sse;
mod static_files;
mod types;

pub use config::DashboardConfig;
pub use server::DashboardServer;
pub use types::*;
