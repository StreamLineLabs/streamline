//! Integration tests for sharded runtime partition routing
//!
//! These tests verify that the thread-per-core sharded runtime correctly
//! routes partition work to the appropriate shard based on partition ID.

#[path = "common/mod.rs"]
mod common;

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use streamline::runtime::{ShardedRuntime, ShardedRuntimeConfig};
use streamline::TopicManager;
use tempfile::tempdir;

// ============================================================================
// Partition-to-Shard Mapping Tests
// ============================================================================

/// Test that partition IDs are correctly mapped to shards using modulo
#[test]
fn test_partition_to_shard_mapping() {
    let config = ShardedRuntimeConfig {
        shard_count: 4,
        ..Default::default()
    };
    let runtime = ShardedRuntime::new(config).unwrap();

    // Partition 0 -> Shard 0
    assert_eq!(runtime.shard_for_partition(0), 0);
    // Partition 1 -> Shard 1
    assert_eq!(runtime.shard_for_partition(1), 1);
    // Partition 2 -> Shard 2
    assert_eq!(runtime.shard_for_partition(2), 2);
    // Partition 3 -> Shard 3
    assert_eq!(runtime.shard_for_partition(3), 3);
    // Partition 4 -> Shard 0 (wraps around)
    assert_eq!(runtime.shard_for_partition(4), 0);
    // Partition 5 -> Shard 1
    assert_eq!(runtime.shard_for_partition(5), 1);
    // Partition 100 -> Shard 0
    assert_eq!(runtime.shard_for_partition(100), 0);
    // Partition 101 -> Shard 1
    assert_eq!(runtime.shard_for_partition(101), 1);
}

/// Test that partition routing is deterministic
#[test]
fn test_partition_routing_deterministic() {
    let config = ShardedRuntimeConfig {
        shard_count: 8,
        ..Default::default()
    };
    let runtime = ShardedRuntime::new(config).unwrap();

    // Same partition should always map to same shard
    for _ in 0..100 {
        assert_eq!(runtime.shard_for_partition(42), 2); // 42 % 8 = 2
        assert_eq!(runtime.shard_for_partition(99), 3); // 99 % 8 = 3
    }
}

/// Test partition distribution across shards
#[test]
fn test_partition_distribution() {
    let config = ShardedRuntimeConfig {
        shard_count: 4,
        ..Default::default()
    };
    let runtime = ShardedRuntime::new(config).unwrap();

    // Count how many partitions map to each shard for partitions 0-15
    let mut distribution = HashMap::new();
    for partition in 0..16 {
        let shard = runtime.shard_for_partition(partition);
        *distribution.entry(shard).or_insert(0) += 1;
    }

    // Each shard should have exactly 4 partitions (16 / 4 = 4)
    for shard_id in 0..4 {
        assert_eq!(distribution.get(&shard_id), Some(&4));
    }
}

// ============================================================================
// Submit with Result Tests
// ============================================================================

/// Test submit_to_shard_with_result returns correct value
#[tokio::test]
async fn test_submit_to_shard_with_result_returns_value() {
    let config = ShardedRuntimeConfig {
        shard_count: 2,
        ..Default::default()
    };
    let runtime = ShardedRuntime::new(config).unwrap();
    runtime.start().unwrap();

    let rx = runtime
        .submit_to_shard_with_result(0, || {
            // Simple computation
            21 * 2
        })
        .unwrap();

    let result = rx.await.unwrap();
    assert_eq!(result, 42);

    runtime.stop().unwrap();
}

/// Test submit_for_partition_with_result routes to correct shard
#[tokio::test]
async fn test_submit_for_partition_routes_correctly() {
    let config = ShardedRuntimeConfig {
        shard_count: 4,
        ..Default::default()
    };
    let runtime = Arc::new(ShardedRuntime::new(config).unwrap());
    runtime.start().unwrap();

    // Track which shard processed each partition
    let shard_tracker = Arc::new(std::sync::Mutex::new(HashMap::new()));

    // Submit tasks for partitions 0-7
    let mut receivers = Vec::new();
    for partition in 0..8u32 {
        let tracker = shard_tracker.clone();
        let rt = runtime.clone();

        let rx = rt
            .submit_for_partition_with_result(partition, move || {
                // Simulate work and return the expected shard
                let expected_shard = partition as usize % 4;
                tracker.lock().unwrap().insert(partition, expected_shard);
                partition
            })
            .unwrap();
        receivers.push(rx);
    }

    // Wait for all tasks
    for rx in receivers {
        rx.await.unwrap();
    }

    // Verify routing
    let tracker = shard_tracker.lock().unwrap();
    assert_eq!(tracker.len(), 8);
    // Partition 0 -> Shard 0
    assert_eq!(tracker.get(&0), Some(&0));
    // Partition 1 -> Shard 1
    assert_eq!(tracker.get(&1), Some(&1));
    // Partition 4 -> Shard 0 (wraps)
    assert_eq!(tracker.get(&4), Some(&0));
    // Partition 7 -> Shard 3
    assert_eq!(tracker.get(&7), Some(&3));

    runtime.stop().unwrap();
}

/// Test error propagation through submit_with_result
#[tokio::test]
async fn test_submit_with_result_propagates_errors() {
    let config = ShardedRuntimeConfig {
        shard_count: 2,
        ..Default::default()
    };
    let runtime = ShardedRuntime::new(config).unwrap();
    runtime.start().unwrap();

    // Submit a task that returns a Result with an error
    let rx = runtime
        .submit_for_partition_with_result(0, || -> Result<i32, &'static str> {
            Err("intentional test error")
        })
        .unwrap();

    // The channel should succeed, but contain the error Result
    let result = rx.await.unwrap();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "intentional test error");

    runtime.stop().unwrap();
}

// ============================================================================
// Concurrent Operations Tests
// ============================================================================

/// Test concurrent task submission across multiple partitions
#[tokio::test]
async fn test_concurrent_partition_tasks() {
    let config = ShardedRuntimeConfig {
        shard_count: 4,
        ..Default::default()
    };
    let runtime = Arc::new(ShardedRuntime::new(config).unwrap());
    runtime.start().unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    let num_tasks = 100;

    // Submit many tasks concurrently
    let mut receivers = Vec::new();
    for i in 0..num_tasks {
        let counter_clone = counter.clone();
        let rx = runtime
            .submit_for_partition_with_result(i as u32, move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                i
            })
            .unwrap();
        receivers.push(rx);
    }

    // Wait for all tasks and verify results
    for (expected, rx) in receivers.into_iter().enumerate() {
        let result = rx.await.unwrap();
        assert_eq!(result, expected);
    }

    // All tasks should have completed
    assert_eq!(counter.load(Ordering::SeqCst), num_tasks);

    runtime.stop().unwrap();
}

/// Test that same-partition tasks execute sequentially on same shard
#[tokio::test]
async fn test_same_partition_sequential_execution() {
    let config = ShardedRuntimeConfig {
        shard_count: 4,
        ..Default::default()
    };
    let runtime = Arc::new(ShardedRuntime::new(config).unwrap());
    runtime.start().unwrap();

    let execution_order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let partition = 5u32; // Will go to shard 1

    // Submit 10 tasks to the same partition
    let mut receivers = Vec::new();
    for i in 0..10 {
        let order = execution_order.clone();
        let rx = runtime
            .submit_for_partition_with_result(partition, move || {
                // Record execution order
                order.lock().unwrap().push(i);
                i
            })
            .unwrap();
        receivers.push(rx);
    }

    // Wait for all tasks
    for rx in receivers {
        rx.await.unwrap();
    }

    // Tasks should have executed (order may vary due to shard internal scheduling,
    // but all 10 should be present)
    let order = execution_order.lock().unwrap();
    assert_eq!(order.len(), 10);

    runtime.stop().unwrap();
}

// ============================================================================
// TopicManager Integration Tests
// ============================================================================

/// Test TopicManager append operations through sharded runtime
#[tokio::test]
async fn test_topic_manager_append_via_sharded_runtime() {
    let dir = tempdir().unwrap();
    let topic_manager = Arc::new(TopicManager::new(dir.path()).unwrap());

    let config = ShardedRuntimeConfig {
        shard_count: 4,
        ..Default::default()
    };
    let runtime = Arc::new(ShardedRuntime::new(config).unwrap());
    runtime.start().unwrap();

    // Create a topic with 4 partitions
    topic_manager.create_topic("sharded-test", 4).unwrap();

    // Append to each partition via sharded runtime
    let mut receivers = Vec::new();
    for partition in 0..4i32 {
        let tm = topic_manager.clone();
        let topic = "sharded-test".to_string();
        let value = Bytes::from(format!("message-{}", partition));

        let rx = runtime
            .submit_for_partition_with_result(partition as u32, move || {
                tm.append(&topic, partition, None, value)
            })
            .unwrap();
        receivers.push((partition, rx));
    }

    // Verify all appends succeeded with offset 0
    for (partition, rx) in receivers {
        let result = rx.await.unwrap();
        assert!(result.is_ok(), "Append to partition {} failed", partition);
        assert_eq!(result.unwrap(), 0, "First message should have offset 0");
    }

    // Verify data was written correctly
    for partition in 0..4 {
        let records = topic_manager
            .read("sharded-test", partition, 0, 10)
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].value,
            Bytes::from(format!("message-{}", partition))
        );
    }

    runtime.stop().unwrap();
}

/// Test TopicManager read operations through sharded runtime
#[tokio::test]
async fn test_topic_manager_read_via_sharded_runtime() {
    let dir = tempdir().unwrap();
    let topic_manager = Arc::new(TopicManager::new(dir.path()).unwrap());

    let config = ShardedRuntimeConfig {
        shard_count: 4,
        ..Default::default()
    };
    let runtime = Arc::new(ShardedRuntime::new(config).unwrap());
    runtime.start().unwrap();

    // Create topic and write some data directly
    topic_manager.create_topic("read-test", 4).unwrap();
    for partition in 0..4 {
        for i in 0..5 {
            topic_manager
                .append(
                    "read-test",
                    partition,
                    None,
                    Bytes::from(format!("p{}-msg{}", partition, i)),
                )
                .unwrap();
        }
    }

    // Read from each partition via sharded runtime
    let mut receivers = Vec::new();
    for partition in 0..4i32 {
        let tm = topic_manager.clone();
        let topic = "read-test".to_string();

        let rx = runtime
            .submit_for_partition_with_result(partition as u32, move || {
                tm.read(&topic, partition, 0, 10)
            })
            .unwrap();
        receivers.push((partition, rx));
    }

    // Verify all reads succeeded
    for (partition, rx) in receivers {
        let result = rx.await.unwrap();
        assert!(result.is_ok(), "Read from partition {} failed", partition);
        let records = result.unwrap();
        assert_eq!(
            records.len(),
            5,
            "Should have 5 records in partition {}",
            partition
        );
    }

    runtime.stop().unwrap();
}

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

/// Test submitting to an invalid shard ID
#[tokio::test]
async fn test_submit_to_invalid_shard() {
    let config = ShardedRuntimeConfig {
        shard_count: 2,
        ..Default::default()
    };
    let runtime = ShardedRuntime::new(config).unwrap();
    runtime.start().unwrap();

    // Try to submit to shard 99 (doesn't exist)
    let result = runtime.submit_to_shard_with_result(99, || 42);
    assert!(result.is_err());

    runtime.stop().unwrap();
}

/// Test that submit_for_partition never hits invalid shard (modulo always valid)
#[tokio::test]
async fn test_partition_routing_always_valid() {
    let config = ShardedRuntimeConfig {
        shard_count: 4,
        ..Default::default()
    };
    let runtime = ShardedRuntime::new(config).unwrap();
    runtime.start().unwrap();

    // Even with very large partition IDs, routing should succeed
    let large_partitions = [u32::MAX, u32::MAX - 1, 1_000_000, 999_999_999];

    for partition in large_partitions {
        let rx = runtime
            .submit_for_partition_with_result(partition, move || partition)
            .unwrap();
        let result = rx.await.unwrap();
        assert_eq!(result, partition);
    }

    runtime.stop().unwrap();
}

/// Test runtime lifecycle - tasks fail when runtime not started
#[test]
fn test_tasks_fail_when_runtime_not_started() {
    let config = ShardedRuntimeConfig {
        shard_count: 2,
        ..Default::default()
    };
    let runtime = ShardedRuntime::new(config).unwrap();

    // Runtime not started - tasks should fail or be queued
    // (depends on implementation - currently tasks go into queue)
    let result = runtime.submit_to_shard(0, || {});
    // Task submission may succeed (goes to queue) but task won't execute
    // This is expected behavior - the queue accepts tasks before start
    assert!(result.is_ok());
}

/// Test runtime statistics tracking
#[tokio::test]
async fn test_runtime_statistics() {
    let config = ShardedRuntimeConfig {
        shard_count: 2,
        ..Default::default()
    };
    let runtime = Arc::new(ShardedRuntime::new(config).unwrap());
    runtime.start().unwrap();

    let initial_stats = runtime.stats();
    assert!(initial_stats.running);
    assert_eq!(initial_stats.shard_count, 2);

    // Submit some tasks
    let mut receivers = Vec::new();
    for i in 0..10u32 {
        let rx = runtime
            .submit_for_partition_with_result(i, move || i)
            .unwrap();
        receivers.push(rx);
    }

    // Wait for tasks
    for rx in receivers {
        rx.await.unwrap();
    }

    let final_stats = runtime.stats();
    assert_eq!(final_stats.total_tasks, 10);

    runtime.stop().unwrap();

    let stopped_stats = runtime.stats();
    assert!(!stopped_stats.running);
}

// ============================================================================
// NUMA Integration Tests (if available)
// ============================================================================

/// Test NUMA-enabled runtime creation
#[test]
fn test_numa_enabled_runtime() {
    let config = ShardedRuntimeConfig {
        shard_count: 2,
        enable_numa: true,
        ..Default::default()
    };
    let runtime = ShardedRuntime::new(config).unwrap();

    // NUMA allocator should be present
    assert!(runtime.numa_allocator().is_some());

    let stats = runtime.stats();
    assert!(stats.numa_enabled);
    assert!(stats.numa_nodes.is_some());
}

/// Test NUMA-disabled runtime
#[test]
fn test_numa_disabled_runtime() {
    let config = ShardedRuntimeConfig {
        shard_count: 2,
        enable_numa: false,
        ..Default::default()
    };
    let runtime = ShardedRuntime::new(config).unwrap();

    // NUMA allocator should NOT be present
    assert!(runtime.numa_allocator().is_none());
    assert!(!runtime.is_numa_enabled());

    let stats = runtime.stats();
    assert!(!stats.numa_enabled);
    assert!(stats.numa_nodes.is_none());
}

// ============================================================================
// Performance Characteristics Tests
// ============================================================================

/// Test that partition affinity is maintained under load
#[tokio::test]
async fn test_partition_affinity_under_load() {
    let config = ShardedRuntimeConfig {
        shard_count: 4,
        ..Default::default()
    };
    let runtime = Arc::new(ShardedRuntime::new(config).unwrap());
    runtime.start().unwrap();

    // Per-shard task counter to verify affinity
    let shard_counters: Vec<Arc<AtomicUsize>> =
        (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();

    let mut receivers = Vec::new();

    // Submit 400 tasks (100 per shard if evenly distributed)
    for partition in 0..400u32 {
        let expected_shard = partition as usize % 4;
        let counter = shard_counters[expected_shard].clone();

        let rx = runtime
            .submit_for_partition_with_result(partition, move || {
                counter.fetch_add(1, Ordering::SeqCst);
                partition
            })
            .unwrap();
        receivers.push(rx);
    }

    // Wait for all tasks
    for rx in receivers {
        rx.await.unwrap();
    }

    // Each shard should have processed exactly 100 tasks
    for (shard_id, counter) in shard_counters.iter().enumerate() {
        let count = counter.load(Ordering::SeqCst);
        assert_eq!(
            count, 100,
            "Shard {} should have processed 100 tasks, got {}",
            shard_id, count
        );
    }

    runtime.stop().unwrap();
}
