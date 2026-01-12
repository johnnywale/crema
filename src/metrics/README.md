# Metrics Module

Prometheus-style metrics for monitoring and observability of the distributed cache.

## Overview

This module provides comprehensive metrics collection:

- **Counters** - Monotonically increasing values (requests, errors)
- **Gauges** - Values that can go up and down (cache size, connections)
- **Histograms** - Distribution measurements (latencies, sizes)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CacheMetrics                             │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │    Counters     │  │     Gauges      │  │   Histograms    │ │
│  │                 │  │                 │  │                 │ │
│  │ - get_total     │  │ - cache_entries │  │ - get_latency   │ │
│  │ - get_hits      │  │ - cache_size    │  │ - put_latency   │ │
│  │ - get_misses    │  │ - raft_term     │  │ - raft_propose  │ │
│  │ - put_total     │  │ - raft_peers    │  │ - rebalance     │ │
│  │ - errors        │  │ - cluster_nodes │  │ - snapshot      │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                   Prometheus Export                         ││
│  │        to_prometheus() -> Exposition Format                 ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

```rust
use distributed_cache::metrics::CacheMetrics;
use std::time::Duration;

let metrics = CacheMetrics::new();

// Record a cache hit with latency
metrics.record_get(true, Duration::from_micros(50));

// Record a cache miss
metrics.record_get(false, Duration::from_micros(30));

// Record a successful put
metrics.record_put(true, Duration::from_millis(5));

// Record an error
metrics.record_error("connection_timeout");

// Get current snapshot
let snapshot = metrics.snapshot();
println!("Hit rate: {:.2}%", snapshot.hit_rate() * 100.0);
println!("Avg GET latency: {:.2}ms", snapshot.avg_get_latency_ms());

// Export to Prometheus format
let prometheus_output = metrics.to_prometheus();
```

## Metric Types

### Counters

Monotonically increasing values.

```rust
use distributed_cache::metrics::Counter;

let counter = Counter::new("requests_total", "Total requests");

counter.inc();           // Increment by 1
counter.inc_by(10);      // Increment by 10

let value = counter.get();
```

### Labeled Counters

Counters with dimensional labels.

```rust
use distributed_cache::metrics::LabeledCounter;

let counter = LabeledCounter::<2>::new(
    "http_requests",
    "HTTP requests by method and status",
    ["method", "status"],
);

counter.inc(["GET", "200"]);
counter.inc(["POST", "201"]);
counter.inc_by(["GET", "404"], 5);

let get_200 = counter.get(["GET", "200"]);
```

### Gauges

Values that can increase or decrease.

```rust
use distributed_cache::metrics::Gauge;

let gauge = Gauge::new("active_connections", "Active connections");

gauge.set(10);       // Set to specific value
gauge.inc();         // Increment by 1
gauge.dec();         // Decrement by 1
gauge.add(5);        // Add 5
gauge.sub(3);        // Subtract 3

let value = gauge.get();
```

### Float Gauges

Gauges for floating-point values.

```rust
use distributed_cache::metrics::FloatGauge;

let gauge = FloatGauge::new("cpu_usage", "CPU usage percentage");

gauge.set(45.7);
gauge.add(2.3);

let value = gauge.get();
```

### Histograms

Distribution measurements with buckets.

```rust
use distributed_cache::metrics::Histogram;
use std::time::Duration;

let histogram = Histogram::new("request_latency", "Request latency");

// Observe values
histogram.observe(0.05);
histogram.observe_duration(Duration::from_millis(50));

// Use timer for automatic observation
{
    let _timer = histogram.start_timer();
    // ... do work ...
} // Timer records duration on drop

// Get statistics
let snapshot = histogram.snapshot();
println!("Mean: {}", snapshot.mean());
println!("P50: {}", snapshot.percentile(50.0));
println!("P99: {}", snapshot.percentile(99.0));
```

### Custom Buckets

```rust
use distributed_cache::metrics::Histogram;

let histogram = Histogram::with_buckets(
    "response_size",
    "Response size in bytes",
    vec![100.0, 1000.0, 10000.0, 100000.0, 1000000.0],
);
```

## CacheMetrics

Comprehensive metrics for the distributed cache.

### Request Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `get_total` | Counter | Total GET requests |
| `get_hits` | Counter | GET cache hits |
| `get_misses` | Counter | GET cache misses |
| `put_total` | Counter | Total PUT requests |
| `put_success` | Counter | Successful PUTs |
| `put_failures` | Counter | Failed PUTs |
| `delete_total` | Counter | Total DELETE requests |

### Latency Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `get_latency` | Histogram | GET operation latency |
| `put_latency` | Histogram | PUT operation latency |
| `delete_latency` | Histogram | DELETE operation latency |

### Cache State Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `cache_entries` | Gauge | Current cache entries |
| `cache_size_bytes` | Gauge | Current cache size |

### Raft Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `raft_proposals_total` | Counter | Total Raft proposals |
| `raft_proposals_failed` | Counter | Failed proposals |
| `raft_propose_latency` | Histogram | Proposal latency |
| `raft_term` | Gauge | Current Raft term |
| `raft_commit_index` | Gauge | Commit index |
| `raft_applied_index` | Gauge | Applied index |
| `raft_peers` | Gauge | Number of peers |

### Cluster Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `cluster_nodes` | Gauge | Total nodes |
| `cluster_healthy_nodes` | Gauge | Healthy nodes |

### Rebalancing Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `rebalance_total` | Counter | Total rebalances |
| `rebalance_success` | Counter | Successful rebalances |
| `rebalance_failures` | Counter | Failed rebalances |
| `rebalance_duration` | Histogram | Rebalance duration |
| `rebalance_entries_transferred` | Counter | Entries transferred |

### Checkpoint Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `snapshots_created` | Counter | Snapshots created |
| `snapshots_loaded` | Counter | Snapshots loaded |
| `snapshot_duration` | Histogram | Snapshot duration |
| `last_snapshot_size` | Gauge | Last snapshot size |

## Prometheus Export

```rust
let metrics = CacheMetrics::new();

// ... record some metrics ...

// Get Prometheus exposition format
let output = metrics.to_prometheus();

// Output looks like:
// # HELP cache_get_total Total GET requests
// # TYPE cache_get_total counter
// cache_get_total 1234
//
// # HELP cache_get_hits GET cache hits
// # TYPE cache_get_hits counter
// cache_get_hits 1000
// ...
```

## Integration Example

```rust
use distributed_cache::metrics::CacheMetrics;
use std::sync::Arc;
use std::time::Instant;

let metrics = Arc::new(CacheMetrics::new());

// In GET handler
async fn handle_get(metrics: &CacheMetrics, key: &[u8]) -> Option<Vec<u8>> {
    let start = Instant::now();
    let result = cache_get(key).await;
    let hit = result.is_some();
    metrics.record_get(hit, start.elapsed());
    result
}

// In PUT handler
async fn handle_put(metrics: &CacheMetrics, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
    let start = Instant::now();
    let result = cache_put(key, value).await;
    let success = result.is_ok();
    metrics.record_put(success, start.elapsed());
    if !success {
        metrics.record_error("put_failed");
    }
    result
}

// Periodic stats update
fn update_stats(metrics: &CacheMetrics, cache: &Cache) {
    metrics.update_cache_stats(
        cache.entry_count(),
        cache.weighted_size(),
    );
}

// HTTP endpoint for Prometheus
fn metrics_endpoint(metrics: &CacheMetrics) -> String {
    metrics.to_prometheus()
}
```

## Latency Buckets

Pre-defined bucket configurations:

```rust
use distributed_cache::metrics::{CACHE_LATENCY_BUCKETS, RAFT_LATENCY_BUCKETS};

// For cache operations (0.1ms to 1s)
// [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]

// For Raft operations (1ms to 10s)
// [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
```

## Thread Safety

All metric types are thread-safe and can be shared across threads:

- Counters use `AtomicU64`
- Gauges use `AtomicI64` or `AtomicU64` (for floats)
- Histograms use atomic operations with CAS for float sum
- Labeled metrics use `parking_lot::RwLock`

## Performance Considerations

1. **Atomic operations** - All metrics use lock-free atomics where possible
2. **Relaxed ordering** - Uses `Ordering::Relaxed` for performance (eventual consistency is acceptable for metrics)
3. **Pre-allocated buckets** - Histogram buckets are allocated once
4. **Efficient labels** - Labeled metrics use hash maps with read-optimized locking
