# Metrics

This guide explains how to use the metrics system for observability and monitoring.

For the complete API reference, see the [main README](../../README.md).

## Overview

Crema provides comprehensive metrics for:
- **Cache operations**: Hits, misses, latencies
- **Raft consensus**: Proposals, commits, elections
- **Cluster health**: Node status, replication lag
- **Multi-Raft**: Per-shard statistics, migrations

## Metric Types

### Counter
Monotonically increasing value (e.g., total requests).

```rust
use crema::Counter;

let requests = Counter::new();
requests.increment();
requests.add(5);
println!("Total: {}", requests.get());
```

### Gauge
Point-in-time value (e.g., cache size).

```rust
use crema::Gauge;

let cache_size = Gauge::new();
cache_size.set(1000);
cache_size.increment();
cache_size.decrement();
println!("Current: {}", cache_size.get());
```

### Histogram
Distribution of values (e.g., latencies).

```rust
use crema::Histogram;

let latency = Histogram::new(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]);
latency.observe(0.003);  // 3ms
latency.observe(0.042);  // 42ms

let snapshot = latency.snapshot();
println!("p50: {}ms", snapshot.percentile(0.50) * 1000.0);
println!("p99: {}ms", snapshot.percentile(0.99) * 1000.0);
```

### Labeled Metrics
Metrics with dimensional labels.

```rust
use crema::LabeledCounter;

// 2-dimensional counter
let requests: LabeledCounter<2> = LabeledCounter::new();
requests.with_labels(["get", "hit"]).increment();
requests.with_labels(["get", "miss"]).increment();
requests.with_labels(["put", "success"]).increment();
```

## Built-in Metrics

### CacheMetrics

```rust
let metrics = cache.metrics();

// Counters
println!("Total requests: {}", metrics.requests.get());
println!("Cache hits: {}", metrics.hits.get());
println!("Cache misses: {}", metrics.misses.get());
println!("Errors: {}", metrics.errors.get());

// Gauges
println!("Cache size: {}", metrics.cache_size.get());
println!("Raft term: {}", metrics.raft_term.get());
println!("Raft commit: {}", metrics.raft_commit.get());

// Histograms
let get_latency = metrics.get_latency.snapshot();
println!("Get p50: {}ms", get_latency.percentile(0.50) * 1000.0);
println!("Get p99: {}ms", get_latency.percentile(0.99) * 1000.0);

let put_latency = metrics.put_latency.snapshot();
println!("Put p50: {}ms", put_latency.percentile(0.50) * 1000.0);
println!("Put p99: {}ms", put_latency.percentile(0.99) * 1000.0);
```

### Available Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `requests` | Counter | Total requests |
| `hits` | Counter | Cache hits |
| `misses` | Counter | Cache misses |
| `errors` | Counter | Operation errors |
| `cache_size` | Gauge | Current entry count |
| `raft_term` | Gauge | Current Raft term |
| `raft_commit` | Gauge | Committed log index |
| `raft_applied` | Gauge | Applied log index |
| `get_latency` | Histogram | Get operation latency |
| `put_latency` | Histogram | Put operation latency |
| `delete_latency` | Histogram | Delete operation latency |
| `raft_propose_latency` | Histogram | Raft proposal latency |

## Prometheus Export

### Text Format

```rust
let metrics = cache.metrics();
let prometheus_text = metrics.to_prometheus();
println!("{}", prometheus_text);
```

Output:
```
# HELP crema_requests_total Total number of requests
# TYPE crema_requests_total counter
crema_requests_total 12345

# HELP crema_cache_hits_total Total cache hits
# TYPE crema_cache_hits_total counter
crema_cache_hits_total 10000

# HELP crema_cache_misses_total Total cache misses
# TYPE crema_cache_misses_total counter
crema_cache_misses_total 2345

# HELP crema_cache_size Current cache entry count
# TYPE crema_cache_size gauge
crema_cache_size 50000

# HELP crema_get_latency_seconds Get operation latency
# TYPE crema_get_latency_seconds histogram
crema_get_latency_seconds_bucket{le="0.001"} 8000
crema_get_latency_seconds_bucket{le="0.005"} 9500
crema_get_latency_seconds_bucket{le="0.01"} 9900
crema_get_latency_seconds_bucket{le="+Inf"} 10000
crema_get_latency_seconds_sum 15.5
crema_get_latency_seconds_count 10000
```

### HTTP Endpoint (Manual Integration)

Crema doesn't include a built-in HTTP server, but you can expose metrics via your application:

```rust
use axum::{routing::get, Router};

async fn metrics_handler(cache: Arc<DistributedCache>) -> String {
    cache.metrics().to_prometheus()
}

let app = Router::new()
    .route("/metrics", get(move || metrics_handler(cache.clone())));

axum::Server::bind(&"0.0.0.0:9090".parse().unwrap())
    .serve(app.into_make_service())
    .await?;
```

## Metric Snapshots

For periodic reporting without Prometheus:

```rust
use crema::MetricsSnapshot;

let snapshot = cache.metrics().snapshot();

// Access all metrics at once
println!("Requests: {}", snapshot.requests);
println!("Hits: {}", snapshot.hits);
println!("Misses: {}", snapshot.misses);
println!("Hit ratio: {:.2}%", snapshot.hit_ratio() * 100.0);
println!("Cache size: {}", snapshot.cache_size);

// Latency percentiles
println!("Get p50: {:.3}ms", snapshot.get_latency_p50 * 1000.0);
println!("Get p99: {:.3}ms", snapshot.get_latency_p99 * 1000.0);
println!("Put p50: {:.3}ms", snapshot.put_latency_p50 * 1000.0);
println!("Put p99: {:.3}ms", snapshot.put_latency_p99 * 1000.0);
```

## Histogram Buckets

### Cache Latency Buckets
Optimized for sub-millisecond to second latencies:

```rust
pub const CACHE_LATENCY_BUCKETS: &[f64] = &[
    0.0001,  // 100μs
    0.0005,  // 500μs
    0.001,   // 1ms
    0.005,   // 5ms
    0.01,    // 10ms
    0.05,    // 50ms
    0.1,     // 100ms
    0.5,     // 500ms
    1.0,     // 1s
];
```

### Raft Latency Buckets
Optimized for Raft consensus (typically higher latency):

```rust
pub const RAFT_LATENCY_BUCKETS: &[f64] = &[
    0.001,   // 1ms
    0.005,   // 5ms
    0.01,    // 10ms
    0.05,    // 50ms
    0.1,     // 100ms
    0.5,     // 500ms
    1.0,     // 1s
    5.0,     // 5s
    10.0,    // 10s
];
```

## Multi-Raft Metrics

When using Multi-Raft mode, additional per-shard metrics are available:

```rust
if let Some(coordinator) = cache.multiraft_coordinator() {
    let stats = coordinator.stats().await;

    for (shard_id, shard_stats) in &stats.shard_stats {
        println!("Shard {}:", shard_id);
        println!("  Leader: {:?}", shard_stats.leader_id);
        println!("  Entries: {}", shard_stats.entry_count);
        println!("  Raft term: {}", shard_stats.raft_term);
        println!("  Raft commit: {}", shard_stats.raft_commit);
    }

    // Migration metrics
    for migration in coordinator.active_migrations().await {
        println!("Migration {}:", migration.id);
        println!("  Shard: {}", migration.shard_id);
        println!("  Phase: {:?}", migration.phase);
        println!("  Progress: {:.1}%", migration.progress.percent_complete());
        println!("  Bytes transferred: {}", migration.progress.bytes_transferred);
    }
}
```

## Custom Metrics

Add application-specific metrics:

```rust
use crema::{Counter, Histogram, CACHE_LATENCY_BUCKETS};

struct MyAppMetrics {
    cache_metrics: CacheMetrics,
    custom_operations: Counter,
    custom_latency: Histogram,
}

impl MyAppMetrics {
    fn new(cache_metrics: CacheMetrics) -> Self {
        Self {
            cache_metrics,
            custom_operations: Counter::new(),
            custom_latency: Histogram::new(CACHE_LATENCY_BUCKETS.to_vec()),
        }
    }

    fn record_operation(&self, duration: Duration) {
        self.custom_operations.increment();
        self.custom_latency.observe(duration.as_secs_f64());
    }

    fn to_prometheus(&self) -> String {
        let mut output = self.cache_metrics.to_prometheus();

        output.push_str(&format!(
            "# HELP myapp_custom_ops_total Custom operations\n\
             # TYPE myapp_custom_ops_total counter\n\
             myapp_custom_ops_total {}\n",
            self.custom_operations.get()
        ));

        output
    }
}
```

## Alerting Guidelines

### Recommended Alerts

| Alert | Condition | Severity |
|-------|-----------|----------|
| High miss ratio | `miss_ratio > 0.5 for 5m` | Warning |
| Slow gets | `get_p99 > 10ms for 5m` | Warning |
| Slow puts | `put_p99 > 100ms for 5m` | Warning |
| Raft proposal slow | `raft_propose_p99 > 1s for 5m` | Critical |
| Cache full | `cache_size > 0.9 * max_capacity` | Warning |
| Replication lag | `raft_commit - raft_applied > 1000` | Warning |

### Example Prometheus Rules

```yaml
groups:
  - name: crema
    rules:
      - alert: HighCacheMissRatio
        expr: |
          crema_cache_misses_total /
          (crema_cache_hits_total + crema_cache_misses_total) > 0.5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: Cache miss ratio above 50%

      - alert: SlowRaftProposals
        expr: |
          histogram_quantile(0.99,
            rate(crema_raft_propose_latency_seconds_bucket[5m])
          ) > 1
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: Raft proposals taking more than 1s at p99
```

## Grafana Dashboard

Example dashboard panels:

### Cache Operations
```
# Requests per second
rate(crema_requests_total[1m])

# Hit ratio
crema_cache_hits_total / (crema_cache_hits_total + crema_cache_misses_total)

# Cache size
crema_cache_size
```

### Latency
```
# Get latency percentiles
histogram_quantile(0.50, rate(crema_get_latency_seconds_bucket[5m]))
histogram_quantile(0.99, rate(crema_get_latency_seconds_bucket[5m]))

# Put latency percentiles
histogram_quantile(0.50, rate(crema_put_latency_seconds_bucket[5m]))
histogram_quantile(0.99, rate(crema_put_latency_seconds_bucket[5m]))
```

### Raft Health
```
# Raft term (should be stable)
crema_raft_term

# Replication lag
crema_raft_commit - crema_raft_applied
```

## HTTP Endpoint Integration

Crema doesn't include a built-in HTTP server to avoid forcing a specific web framework. Here's how to integrate with popular frameworks:

### Axum Integration

Add axum to your Cargo.toml:
```toml
[dependencies]
axum = "0.7"
```

Create a metrics endpoint:
```rust
use axum::{routing::get, Router, Extension};
use crema::CacheMetrics;
use std::sync::Arc;

async fn metrics_handler(
    Extension(metrics): Extension<Arc<CacheMetrics>>,
) -> String {
    metrics.to_prometheus()
}

// In your main function:
let metrics = Arc::new(CacheMetrics::new());

let app = Router::new()
    .route("/metrics", get(metrics_handler))
    .layer(Extension(metrics.clone()));

// Run on port 9090
let listener = tokio::net::TcpListener::bind("0.0.0.0:9090").await?;
axum::serve(listener, app).await?;
```

### Actix-web Integration

Add actix-web to your Cargo.toml:
```toml
[dependencies]
actix-web = "4"
```

Create a metrics endpoint:
```rust
use actix_web::{web, App, HttpServer, HttpResponse};
use crema::CacheMetrics;
use std::sync::Arc;

async fn metrics_handler(
    metrics: web::Data<Arc<CacheMetrics>>,
) -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/plain; charset=utf-8")
        .body(metrics.to_prometheus())
}

// In your main function:
let metrics = Arc::new(CacheMetrics::new());
let metrics_data = web::Data::new(metrics);

HttpServer::new(move || {
    App::new()
        .app_data(metrics_data.clone())
        .route("/metrics", web::get().to(metrics_handler))
})
.bind("0.0.0.0:9090")?
.run()
.await?;
```

### Warp Integration

Add warp to your Cargo.toml:
```toml
[dependencies]
warp = "0.3"
```

Create a metrics endpoint:
```rust
use warp::Filter;
use crema::CacheMetrics;
use std::sync::Arc;

// In your main function:
let metrics = Arc::new(CacheMetrics::new());

let metrics_filter = warp::path("metrics")
    .map(move || {
        metrics.to_prometheus()
    });

warp::serve(metrics_filter)
    .run(([0, 0, 0, 0], 9090))
    .await;
```

### Prometheus Configuration

Add to your `prometheus.yml`:
```yaml
scrape_configs:
  - job_name: 'crema'
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost:9090']
    # Optional: Add labels
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
        regex: '(.+):.*'
        replacement: '$1'
```

For multiple nodes:
```yaml
scrape_configs:
  - job_name: 'crema-cluster'
    scrape_interval: 15s
    static_configs:
      - targets:
        - 'node1:9090'
        - 'node2:9090'
        - 'node3:9090'
```

## Best Practices

1. **Export regularly**: Scrape metrics every 15-30 seconds
2. **Use labels sparingly**: High cardinality labels can cause memory issues
3. **Monitor hit ratio**: Target > 80% for effective caching
4. **Track p99 latencies**: Better indicator of user experience than averages
5. **Alert on trends**: Rate of change often more useful than absolute values
