# Testing Module

Testing utilities for the distributed cache, including chaos testing and integration test helpers.

## Overview

This module provides comprehensive tools for testing distributed systems:

- **Chaos Testing** - Inject failures to test resilience
- **Test Cluster** - Helpers for setting up test environments
- **Test Assertions** - Eventually-consistent assertion helpers
- **Test Metrics** - Track test performance metrics

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Testing Framework                          │
│                                                                 │
│  ┌───────────────────────┐  ┌───────────────────────────────┐  │
│  │    ChaosController    │  │        TestCluster            │  │
│  │  - Partitions         │  │  - Multi-node setup           │  │
│  │  - Node crashes       │  │  - Chaos integration          │  │
│  │  - Message drops      │  │  - Node management            │  │
│  └───────────────────────┘  └───────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────┐  ┌───────────────────────────────┐  │
│  │    ChaosScenario      │  │       TestMetrics             │  │
│  │  - Leader failover    │  │  - Latency tracking           │  │
│  │  - Network partition  │  │  - Error counting             │  │
│  │  - Rolling restart    │  │  - Custom metrics             │  │
│  └───────────────────────┘  └───────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Chaos Testing

### Configuration Presets

```rust
use distributed_cache::testing::ChaosConfig;

// No failures (default)
let config = ChaosConfig::none();

// Low failure rates for smoke tests
let config = ChaosConfig::light();

// Medium failure rates for integration tests
let config = ChaosConfig::moderate();

// High failure rates for stress tests
let config = ChaosConfig::heavy();
```

### Manual Chaos Injection

```rust
use distributed_cache::testing::{ChaosController, ChaosConfig};
use std::time::Duration;
use std::collections::HashSet;

let controller = ChaosController::new(ChaosConfig::moderate());

// Register nodes
controller.register_node(1);
controller.register_node(2);
controller.register_node(3);

// Enable chaos
controller.enable();

// Create a network partition
let side_a: HashSet<_> = [1, 2].into_iter().collect();
let side_b: HashSet<_> = [3].into_iter().collect();
controller.create_partition(side_a, side_b, Duration::from_secs(30));

// Crash a node
controller.crash_node(2, Duration::from_secs(10));

// Check partition status
if controller.is_partitioned(1, 3) {
    println!("Nodes 1 and 3 cannot communicate");
}

// Heal all issues
controller.heal_all_partitions();
controller.recover_all_nodes();
```

### Predefined Scenarios

```rust
use distributed_cache::testing::{ChaosScenario, ChaosRunner, ChaosController};
use std::sync::Arc;
use std::time::Duration;

let controller = Arc::new(ChaosController::with_defaults());

// Leader failover scenario
let scenario = ChaosScenario::leader_failover(1);

// Network partition scenario
let scenario = ChaosScenario::network_partition(vec![1, 2], vec![3, 4, 5]);

// Rolling restart scenario
let scenario = ChaosScenario::rolling_restart(
    vec![1, 2, 3],
    Duration::from_secs(10),
);

// Run the scenario
let runner = ChaosRunner::new(controller);
// runner.run(&scenario).await;
```

## Test Cluster Helper

```rust
use distributed_cache::testing::{TestCluster, ChaosConfig};
use std::time::Duration;

// Create a 3-node test cluster
let cluster = TestCluster::new(3);

// Or with custom chaos config
let cluster = TestCluster::with_chaos(5, ChaosConfig::moderate());

// Enable chaos testing
cluster.enable_chaos();

// Isolate a node
cluster.isolate_node(1, Duration::from_secs(30));

// Crash and recover
cluster.crash_node(2, Duration::from_secs(5));

// Get statistics
let stats = cluster.stats();
println!("Partitions: {}", stats.active_partitions);
println!("Crashed nodes: {}", stats.active_crashes);

// Cleanup
cluster.heal_partitions();
cluster.recover_nodes();
```

## Test Assertions

```rust
use distributed_cache::testing::TestAssertions;
use std::time::Duration;

// Wait for eventual consistency
TestAssertions::assert_eventually(
    || check_all_nodes_have_value(),
    Duration::from_secs(10),
    "Value should replicate to all nodes",
);

// Non-panicking wait
let success = TestAssertions::wait_for(
    || leader_elected(),
    Duration::from_secs(5),
);
```

## Test Metrics

```rust
use distributed_cache::testing::TestMetrics;
use std::time::Duration;

let mut metrics = TestMetrics::new();

// Record operations
metrics.record_latency(Duration::from_millis(5));
metrics.record_latency(Duration::from_millis(10));

// Record errors
metrics.record_error("timeout");
metrics.record_error("connection_refused");

// Custom metrics
metrics.set_custom("throughput_ops_sec", 10000.0);

// Get statistics
println!("Avg latency: {:?}", metrics.avg_latency());
println!("P99 latency: {:?}", metrics.p99_latency());
println!("Total errors: {}", metrics.total_errors());

// Generate report
println!("{}", metrics.report());
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `partition_probability` | 0.0 | Probability of random partition |
| `partition_min_duration` | 1s | Minimum partition duration |
| `partition_max_duration` | 10s | Maximum partition duration |
| `message_drop_probability` | 0.0 | Probability of dropping messages |
| `message_delay_probability` | 0.0 | Probability of delaying messages |
| `message_delay_min` | 10ms | Minimum message delay |
| `message_delay_max` | 100ms | Maximum message delay |
| `crash_probability` | 0.0 | Probability of random crash |
| `crash_min_duration` | 1s | Minimum crash duration |
| `crash_max_duration` | 30s | Maximum crash duration |
| `seed` | None | Random seed for reproducibility |

## Best Practices

1. **Start with low chaos** - Use `ChaosConfig::light()` for initial tests
2. **Use deterministic seeds** - Set `seed` for reproducible failures
3. **Clean up after tests** - Always heal partitions and recover nodes
4. **Monitor metrics** - Track latency and errors during chaos
5. **Test recovery** - Verify the system recovers after chaos ends

## Integration Test Example

```rust
#[tokio::test]
async fn test_leader_failover() {
    let cluster = TestCluster::new(3);
    cluster.enable_chaos();

    // Identify the leader
    let leader = 1; // In real test, query the cluster

    // Crash the leader
    cluster.crash_node(leader, Duration::from_secs(10));

    // Wait for new leader election
    let elected = TestAssertions::wait_for(
        || new_leader_elected(),
        Duration::from_secs(5),
    );
    assert!(elected, "New leader should be elected");

    // Verify operations still work
    // cache.put(key, value).await.unwrap();

    // Recover the crashed node
    cluster.recover_nodes();

    // Verify the recovered node catches up
    TestAssertions::assert_eventually(
        || node_caught_up(leader),
        Duration::from_secs(10),
        "Recovered node should catch up",
    );
}
```
