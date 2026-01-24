# Chaos Testing

This guide explains how to use the chaos testing framework for verifying system resilience.

For the complete API reference, see the [main README](../../README.md).

## Overview

The chaos testing framework provides:
- **Failure injection**: Simulate crashes, partitions, message drops
- **Predefined scenarios**: Common failure patterns
- **Configurable intensity**: From light to heavy chaos
- **Test assertions**: Verify system behavior under failures

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    ChaosController                               │
│                                                                  │
│   Actions:                                                       │
│   • Network partitions (isolate groups of nodes)                │
│   • Node crashes (temporary/permanent)                          │
│   • Message drops (percentage-based)                            │
│   • Message delays (add latency)                                │
│                                                                  │
│   ┌──────────────────────────────────────────────────────────┐  │
│   │                  ChaosRunner                              │  │
│   │                                                           │  │
│   │   Scenarios:                                              │  │
│   │   • Leader failover                                       │  │
│   │   • Network partition and recovery                        │  │
│   │   • Rolling restart                                       │  │
│   │   • Cascading failures                                    │  │
│   └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Configuration

### ChaosConfig Presets

```rust
use crema::ChaosConfig;

// No failures (default for production)
let config = ChaosConfig::none();

// Light chaos (low probability failures)
let config = ChaosConfig::light();

// Moderate chaos (medium probability)
let config = ChaosConfig::moderate();

// Heavy chaos (high probability - stress testing)
let config = ChaosConfig::heavy();
```

### Custom Configuration

```rust
let config = ChaosConfig::new()
    .with_message_drop_probability(0.05)    // 5% message drops
    .with_message_delay_probability(0.10)   // 10% delayed messages
    .with_delay_range(Duration::from_millis(10), Duration::from_millis(100))
    .with_partition_probability(0.01)       // 1% chance of partition
    .with_crash_probability(0.001);         // 0.1% crash probability
```

### Configuration Parameters

| Parameter | Light | Moderate | Heavy | Description |
|-----------|-------|----------|-------|-------------|
| `message_drop` | 1% | 5% | 15% | Messages silently dropped |
| `message_delay` | 5% | 15% | 30% | Messages delayed |
| `delay_range` | 1-10ms | 10-100ms | 50-500ms | Delay duration range |
| `partition` | 0.1% | 1% | 5% | Network partition events |
| `crash` | 0.01% | 0.1% | 1% | Node crash events |

## Basic Usage

### Injecting Failures

```rust
use crema::{ChaosController, ChaosAction};

let controller = ChaosController::new(ChaosConfig::moderate());

// Inject a network partition
controller.inject(ChaosAction::Partition {
    groups: vec![
        vec![1, 2],      // Group A: nodes 1, 2
        vec![3, 4, 5],   // Group B: nodes 3, 4, 5
    ],
}).await?;

// Let the system run under partition
tokio::time::sleep(Duration::from_secs(10)).await;

// Heal the partition
controller.inject(ChaosAction::HealPartition).await?;
```

### Chaos Actions

```rust
pub enum ChaosAction {
    /// Create network partition between node groups
    Partition {
        groups: Vec<Vec<NodeId>>,
    },

    /// Heal all network partitions
    HealPartition,

    /// Crash a specific node (temporary or permanent)
    CrashNode {
        node_id: NodeId,
        permanent: bool,
    },

    /// Recover a crashed node
    RecoverNode {
        node_id: NodeId,
    },

    /// Drop messages to/from a node
    DropMessages {
        node_id: NodeId,
        drop_rate: f64,  // 0.0 to 1.0
    },

    /// Add latency to messages to/from a node
    DelayMessages {
        node_id: NodeId,
        delay: Duration,
    },

    /// Reset all chaos (return to normal)
    Reset,
}
```

## Predefined Scenarios

### ChaosRunner

```rust
use crema::{ChaosRunner, ChaosScenario};

let runner = ChaosRunner::new(controller);

// Run a predefined scenario
runner.run(ChaosScenario::LeaderFailover).await?;
```

### Available Scenarios

#### Leader Failover

Tests leader election when the current leader crashes.

```rust
runner.run(ChaosScenario::LeaderFailover).await?;

// What happens:
// 1. Identify current leader
// 2. Crash the leader node
// 3. Wait for new leader election
// 4. Verify cluster continues operating
// 5. Recover crashed node
// 6. Verify it rejoins as follower
```

#### Network Partition

Tests behavior during and after network partition.

```rust
runner.run(ChaosScenario::NetworkPartition {
    duration: Duration::from_secs(30),
    minority_size: 1,  // Isolate 1 node
}).await?;

// What happens:
// 1. Partition cluster (majority/minority)
// 2. Verify majority continues operating
// 3. Verify minority rejects writes (no quorum)
// 4. Heal partition
// 5. Verify minority rejoins and syncs
```

#### Rolling Restart

Tests cluster behavior during rolling upgrade.

```rust
runner.run(ChaosScenario::RollingRestart {
    delay_between_restarts: Duration::from_secs(10),
}).await?;

// What happens:
// 1. For each node (starting with followers):
//    a. Crash node
//    b. Wait for cluster to adapt
//    c. Recover node
//    d. Wait for resync
// 2. Verify no data loss
```

#### Cascading Failures

Tests recovery from multiple simultaneous failures.

```rust
runner.run(ChaosScenario::CascadingFailures {
    failure_count: 2,
    recovery_delay: Duration::from_secs(5),
}).await?;

// What happens:
// 1. Crash N nodes rapidly
// 2. Verify cluster degrades gracefully
// 3. Recover nodes one by one
// 4. Verify full recovery
```

## Test Assertions

### TestAssertions Helper

```rust
use crema::TestAssertions;

let assertions = TestAssertions::new(&cluster);

// Verify exactly one leader
assertions.assert_single_leader().await?;

// Verify data consistency across nodes
assertions.assert_data_consistent("test_key").await?;

// Verify all nodes have the same Raft log
assertions.assert_logs_consistent().await?;

// Verify cluster has quorum
assertions.assert_has_quorum().await?;
```

### Custom Assertions

```rust
// After chaos, verify system recovered
async fn verify_recovery(cluster: &TestCluster) -> Result<()> {
    // Wait for leader election
    let leader = tokio::time::timeout(
        Duration::from_secs(30),
        cluster.wait_for_leader()
    ).await??;

    // Verify write still works
    cluster.node(leader).put("recovery_test", "value").await?;

    // Verify read from all nodes
    for node in cluster.nodes() {
        let value = node.get(b"recovery_test").await;
        assert_eq!(value, Some(b"value".to_vec().into()));
    }

    Ok(())
}
```

## Integration with Tests

### Unit Test Example

```rust
#[tokio::test]
async fn test_leader_failover() {
    let controller = ChaosController::new(ChaosConfig::none());
    let cluster = TestCluster::new(3).await;

    // Initial setup
    let leader = cluster.wait_for_leader().await;
    cluster.node(leader).put("key", "value").await.unwrap();

    // Inject failure
    controller.inject(ChaosAction::CrashNode {
        node_id: leader,
        permanent: false,
    }).await.unwrap();

    // Wait for new leader
    tokio::time::sleep(Duration::from_secs(5)).await;
    let new_leader = cluster.wait_for_leader().await;
    assert_ne!(new_leader, leader);

    // Verify data still accessible
    let value = cluster.node(new_leader).get(b"key").await;
    assert_eq!(value, Some(b"value".to_vec().into()));

    // Recover and cleanup
    controller.inject(ChaosAction::RecoverNode { node_id: leader }).await.unwrap();
    controller.inject(ChaosAction::Reset).await.unwrap();
}
```

### Stress Test Example

```rust
#[tokio::test]
#[ignore]  // Run with: cargo test -- --ignored
async fn stress_test_under_chaos() {
    let controller = ChaosController::new(ChaosConfig::moderate());
    let cluster = TestCluster::new(5).await;

    // Start chaos in background
    let chaos_handle = tokio::spawn({
        let controller = controller.clone();
        async move {
            loop {
                // Random chaos action
                controller.random_action().await;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    });

    // Run workload
    let mut success = 0;
    let mut failure = 0;

    for i in 0..1000 {
        match cluster.any_node().put(&format!("key{}", i), "value").await {
            Ok(()) => success += 1,
            Err(_) => failure += 1,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Stop chaos
    chaos_handle.abort();
    controller.inject(ChaosAction::Reset).await.unwrap();

    // Allow recovery
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify consistency
    let assertions = TestAssertions::new(&cluster);
    assertions.assert_data_consistent_all_keys().await.unwrap();

    println!("Success: {}, Failure: {} ({:.1}% availability)",
        success, failure,
        success as f64 / (success + failure) as f64 * 100.0
    );
}
```

## Chaos Statistics

### Tracking Chaos Effects

```rust
let controller = ChaosController::new(ChaosConfig::moderate());

// Run chaos for a while...

let stats = controller.stats();
println!("Messages dropped: {}", stats.messages_dropped);
println!("Messages delayed: {}", stats.messages_delayed);
println!("Partitions created: {}", stats.partitions_created);
println!("Nodes crashed: {}", stats.nodes_crashed);
println!("Total actions: {}", stats.total_actions);
```

### ChaosStats

```rust
pub struct ChaosStats {
    pub messages_dropped: u64,
    pub messages_delayed: u64,
    pub partitions_created: u64,
    pub partitions_healed: u64,
    pub nodes_crashed: u64,
    pub nodes_recovered: u64,
    pub total_actions: u64,
    pub duration: Duration,
}
```

## Best Practices

### 1. Start Small

Begin with `ChaosConfig::light()` and increase gradually:

```rust
// Development
let config = ChaosConfig::light();

// CI/CD
let config = ChaosConfig::moderate();

// Dedicated chaos testing
let config = ChaosConfig::heavy();
```

### 2. Always Reset After Tests

```rust
#[tokio::test]
async fn my_chaos_test() {
    let controller = ChaosController::new(ChaosConfig::moderate());

    // ... test code ...

    // ALWAYS reset at the end
    controller.inject(ChaosAction::Reset).await.unwrap();
}
```

### 3. Use Timeouts

```rust
// Chaos can cause operations to hang
let result = tokio::time::timeout(
    Duration::from_secs(30),
    cluster.node(1).put("key", "value")
).await;

match result {
    Ok(Ok(())) => println!("Success"),
    Ok(Err(e)) => println!("Operation error: {}", e),
    Err(_) => println!("Timeout (expected under chaos)"),
}
```

### 4. Verify Eventual Consistency

After chaos, allow time for recovery:

```rust
// Stop chaos
controller.inject(ChaosAction::Reset).await?;

// Allow recovery time
tokio::time::sleep(Duration::from_secs(30)).await;

// Then verify consistency
assertions.assert_data_consistent_all_keys().await?;
```

### 5. Log Chaos Events

```rust
let controller = ChaosController::new(config)
    .with_event_logging(true);

// Events logged to tracing
// RUST_LOG=crema::testing=debug cargo test
```

## Limitations

| Limitation | Description |
|------------|-------------|
| In-process only | Cannot test cross-machine failures |
| Simulated network | Uses in-memory message routing |
| No disk failures | Storage failures not simulated |
| No clock skew | Time-based issues not tested |

For production chaos testing, consider tools like:
- [Chaos Monkey](https://github.com/Netflix/chaosmonkey)
- [Pumba](https://github.com/alexei-led/pumba)
- [Litmus](https://github.com/litmuschaos/litmus)
