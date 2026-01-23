//! Example demonstrating the metrics system for observability.
//!
//! This example shows how to:
//! - Use the CacheMetrics API for monitoring
//! - Record operations (gets, puts, deletes)
//! - Track cache state (entries, size)
//! - Get metric snapshots
//! - Export metrics in Prometheus format
//!
//! Run with:
//!   RUST_LOG=info cargo run --example metrics
//!
//! Note: This example demonstrates the metrics subsystem directly.
//! In a production setup, you would expose metrics via an HTTP endpoint
//! for Prometheus to scrape.

use crema::metrics::{CacheMetrics, Histogram, CACHE_LATENCY_BUCKETS, RAFT_LATENCY_BUCKETS};
use std::time::{Duration, Instant};

fn main() {
    println!("===========================================");
    println!("         Metrics Example");
    println!("===========================================");
    println!();

    // Create metrics instance
    let metrics = CacheMetrics::new();

    // Part 1: Recording Operations
    println!("===========================================");
    println!("      Part 1: Recording Operations");
    println!("===========================================");
    println!();

    // Simulate some cache operations
    println!("Simulating cache operations...");
    println!();

    // Simulate GET operations (mix of hits and misses)
    for i in 0..100 {
        let start = Instant::now();
        // Simulate lookup time
        std::thread::sleep(Duration::from_micros(50 + (i % 10) * 10));
        let latency = start.elapsed();

        // 70% hit rate
        let is_hit = i % 10 < 7;
        metrics.record_get(is_hit, latency);
    }
    println!("  Recorded 100 GET operations (70% hit rate)");

    // Simulate PUT operations
    for i in 0..50 {
        let start = Instant::now();
        // Simulate Raft proposal time
        std::thread::sleep(Duration::from_micros(500 + (i % 5) * 100));
        let latency = start.elapsed();

        // 95% success rate
        let success = i % 20 != 0;
        metrics.record_put(success, latency);
    }
    println!("  Recorded 50 PUT operations (95% success rate)");

    // Simulate DELETE operations
    for i in 0..20 {
        let start = Instant::now();
        std::thread::sleep(Duration::from_micros(400 + (i % 3) * 50));
        let latency = start.elapsed();

        metrics.record_delete(latency);
    }
    println!("  Recorded 20 DELETE operations");

    // Simulate Raft proposals
    for i in 0..30 {
        let start = Instant::now();
        std::thread::sleep(Duration::from_micros(1000 + (i % 10) * 200));
        let latency = start.elapsed();

        let success = i % 15 != 0;
        metrics.record_raft_proposal(success, latency);
    }
    println!("  Recorded 30 Raft proposals");

    println!();

    // Part 2: Updating Cache State
    println!("===========================================");
    println!("      Part 2: Cache State Gauges");
    println!("===========================================");
    println!();

    // Simulate cache state
    metrics.update_cache_stats(15_000, 45_000_000); // entries, size bytes
    metrics.set_leader(true);
    metrics.update_raft_stats(42, 1000, 998, 2); // term, commit, applied, peers
    metrics.update_cluster_stats(3, 3); // total, healthy

    println!("Set cache state:");
    println!("  Cache entries:  15,000");
    println!("  Cache size:     45 MB");
    println!("  Is leader:      true");
    println!("  Raft term:      42");
    println!("  Commit index:   1000");
    println!("  Applied index:  998");
    println!("  Raft peers:     2");
    println!("  Cluster nodes:  3/3 healthy");
    println!();

    // Part 3: Getting Metric Snapshots
    println!("===========================================");
    println!("      Part 3: Metric Snapshots");
    println!("===========================================");
    println!();

    let snapshot = metrics.snapshot();

    println!("Metric Snapshot:");
    println!("  GET Operations:");
    println!("    Total:      {}", snapshot.get_total);
    println!("    Hits:       {}", snapshot.get_hits);
    println!("    Misses:     {}", snapshot.get_misses);
    println!("    Hit Rate:   {:.1}%", snapshot.hit_rate() * 100.0);
    println!();
    println!("  PUT Operations:");
    println!("    Total:      {}", snapshot.put_total);
    println!("    Success:    {}", snapshot.put_success);
    println!("    Failures:   {}", snapshot.put_failures);
    println!("    Success Rate: {:.1}%", snapshot.put_success_rate() * 100.0);
    println!();
    println!("  DELETE Operations:");
    println!("    Total:      {}", snapshot.delete_total);
    println!();
    println!("  Cluster State:");
    println!("    Is Leader:  {}", snapshot.is_leader);
    println!("    Raft Term:  {}", snapshot.raft_term);
    println!("    Entries:    {}", snapshot.cache_entries);
    println!();

    // Part 4: Latency Percentiles
    println!("===========================================");
    println!("      Part 4: Latency Percentiles");
    println!("===========================================");
    println!();

    let get_latency = snapshot.get_latency;
    println!("GET Latency Distribution:");
    println!("  Count:  {}", get_latency.count);
    println!("  Sum:    {:.3}ms", get_latency.sum * 1000.0);
    println!("  Mean:   {:.3}ms", get_latency.mean() * 1000.0);
    println!("  p50:    {:.3}ms", get_latency.percentile(0.50) * 1000.0);
    println!("  p90:    {:.3}ms", get_latency.percentile(0.90) * 1000.0);
    println!("  p99:    {:.3}ms", get_latency.percentile(0.99) * 1000.0);
    println!();

    let put_latency = snapshot.put_latency;
    println!("PUT Latency Distribution:");
    println!("  Count:  {}", put_latency.count);
    println!("  Sum:    {:.3}ms", put_latency.sum * 1000.0);
    println!("  Mean:   {:.3}ms", put_latency.mean() * 1000.0);
    println!("  p50:    {:.3}ms", put_latency.percentile(0.50) * 1000.0);
    println!("  p90:    {:.3}ms", put_latency.percentile(0.90) * 1000.0);
    println!("  p99:    {:.3}ms", put_latency.percentile(0.99) * 1000.0);
    println!();

    // Part 5: Prometheus Export
    println!("===========================================");
    println!("      Part 5: Prometheus Export");
    println!("===========================================");
    println!();

    let prometheus_output = metrics.to_prometheus();

    println!("Prometheus Format Output:");
    println!("-------------------------------------------");
    // Show first 40 lines
    for line in prometheus_output.lines().take(40) {
        println!("{}", line);
    }
    println!("... (truncated)");
    println!("-------------------------------------------");
    println!();
    println!("Total output length: {} bytes", prometheus_output.len());
    println!();

    // Part 6: Custom Histograms
    println!("===========================================");
    println!("      Part 6: Custom Histograms");
    println!("===========================================");
    println!();

    println!("Built-in latency buckets:");
    println!();
    println!("  CACHE_LATENCY_BUCKETS (for fast operations):");
    for bucket in CACHE_LATENCY_BUCKETS {
        println!("    - {}ms", bucket * 1000.0);
    }
    println!();
    println!("  RAFT_LATENCY_BUCKETS (for consensus operations):");
    for bucket in RAFT_LATENCY_BUCKETS {
        println!("    - {}ms", bucket * 1000.0);
    }
    println!();

    // Create a custom histogram with custom buckets
    let custom_buckets = vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0];
    let custom_histogram = Histogram::with_buckets("custom_operation", "Custom operation latency", custom_buckets);

    // Record some values
    custom_histogram.observe(0.003);
    custom_histogram.observe(0.008);
    custom_histogram.observe(0.025);
    custom_histogram.observe(0.150);

    let custom_snap = custom_histogram.snapshot();
    println!("Custom Histogram Example:");
    println!("  Buckets: [1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s]");
    println!("  Recorded values: 3ms, 8ms, 25ms, 150ms");
    println!("  Count:  {}", custom_snap.count);
    println!("  Mean:   {:.1}ms", custom_snap.mean() * 1000.0);
    println!("  p50:    {:.1}ms", custom_snap.percentile(0.50) * 1000.0);
    println!("  p99:    {:.1}ms", custom_snap.percentile(0.99) * 1000.0);
    println!();

    // Part 7: Integration Guide
    println!("===========================================");
    println!("      Part 7: Integration Guide");
    println!("===========================================");
    println!();
    println!("To expose metrics via HTTP for Prometheus:");
    println!();
    println!("  1. Add an HTTP server (e.g., axum, actix-web)");
    println!();
    println!("  2. Create a /metrics endpoint:");
    println!();
    println!("     async fn metrics_handler(metrics: Arc<CacheMetrics>) -> String {{");
    println!("         metrics.to_prometheus()");
    println!("     }}");
    println!();
    println!("  3. Configure Prometheus to scrape:");
    println!();
    println!("     scrape_configs:");
    println!("       - job_name: 'crema'");
    println!("         static_configs:");
    println!("           - targets: ['localhost:9090']");
    println!();
    println!("  4. Create Grafana dashboards for:");
    println!("     - Cache hit rate");
    println!("     - Operation latencies (p50, p99)");
    println!("     - Raft health (term stability, commit lag)");
    println!("     - Cluster membership");
    println!();

    println!("===========================================");
    println!("         Example Complete");
    println!("===========================================");
}
