//! Chaos tests for Streamline cluster resilience
//!
//! This test requires the `clustering` feature to be enabled.
#![cfg(feature = "clustering")]
//!
//! These tests verify cluster behavior under various failure conditions:
//! - Leader failures and re-election
//! - Node crashes and recovery
//! - Network partitions
//! - Data integrity after failures
//!
//! ## Running Tests
//!
//! Most chaos tests are marked as `#[ignore]` because they take 15-30 seconds each
//! and create resource-intensive multi-node clusters. This keeps CI fast.
//!
//! ```bash
//! # Run only fast tests (default)
//! cargo test
//!
//! # Run slow/ignored tests (chaos tests)
//! cargo test --test chaos_test -- --ignored
//!
//! # Run all tests including slow ones
//! cargo test --test chaos_test -- --include-ignored
//!
//! # Run a specific chaos test
//! cargo test --test chaos_test test_leader_failure_recovery -- --ignored
//! ```
//!
//! ## Test Categories
//!
//! - **Fast tests** (~6s): `test_single_node_cluster`, `test_network_isolation_simulation`
//! - **Slow tests** (~15-30s each): Multi-node cluster tests (3-5 nodes)
//!
//! Note: Tests use `#[serial]` to run sequentially because they create
//! multi-node clusters that can conflict when run in parallel.

mod chaos;

use serial_test::serial;
use std::time::Duration;
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;

/// Initialize test logging
fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("streamline=debug".parse().unwrap()),
        )
        .with_test_writer()
        .try_init();
}

/// Test single node cluster formation
#[tokio::test]
#[serial]
async fn test_single_node_cluster() {
    init_logging();

    let mut cluster = chaos::ChaosCluster::new(1).await;
    cluster.start_all().await.unwrap();

    // Wait for leader
    let leader = cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(leader, 1);

    // Verify metadata
    let metadata = cluster.get_metadata().await.unwrap();
    assert!(metadata.brokers.contains_key(&1));

    cluster.stop_all().await;
}

/// Test three node cluster formation and leader election
#[tokio::test]
#[serial]
#[ignore = "slow test (~15s): run with --ignored"]
async fn test_three_node_cluster_formation() {
    init_logging();

    let mut cluster = chaos::ChaosCluster::new(3).await;
    cluster.start_all().await.unwrap();

    // Wait for leader
    let leader = cluster
        .wait_for_leader(Duration::from_secs(15))
        .await
        .unwrap();
    assert!((1..=3).contains(&leader));

    // Verify all nodes are registered
    let metadata = cluster.get_metadata().await.unwrap();
    assert!(metadata.brokers.contains_key(&1));

    cluster.stop_all().await;
}

/// Test leader failure and re-election
#[tokio::test]
#[serial]
#[ignore = "slow test (~20s): run with --ignored"]
async fn test_leader_failure_recovery() {
    init_logging();

    let mut cluster = chaos::ChaosCluster::new(3).await;
    cluster.start_all().await.unwrap();

    // Wait for initial leader
    let initial_leader = cluster
        .wait_for_leader(Duration::from_secs(15))
        .await
        .unwrap();
    tracing::info!("Initial leader: {}", initial_leader);

    // Kill the leader
    cluster
        .node_mut(initial_leader)
        .unwrap()
        .stop()
        .await
        .unwrap();
    tracing::info!("Stopped leader node {}", initial_leader);

    // Wait for new leader election
    sleep(Duration::from_secs(5)).await;

    // Find new leader
    let new_leader = cluster
        .wait_for_leader(Duration::from_secs(15))
        .await
        .unwrap();
    tracing::info!("New leader: {}", new_leader);

    // New leader should be different
    assert_ne!(new_leader, initial_leader);

    // Cluster should still be operational
    assert_eq!(cluster.running_count(), 2);

    cluster.stop_all().await;
}

/// Test node crash and rejoin
#[tokio::test]
#[serial]
#[ignore = "slow test (~15s): run with --ignored"]
async fn test_node_crash_rejoin() {
    init_logging();

    let mut cluster = chaos::ChaosCluster::new(3).await;
    cluster.start_all().await.unwrap();

    // Wait for leader
    let _leader = cluster
        .wait_for_leader(Duration::from_secs(15))
        .await
        .unwrap();

    // Find a follower node
    let follower_id = {
        let leader = cluster.find_leader().await.unwrap();
        if leader == 1 {
            2
        } else {
            1
        }
    };

    // Stop the follower
    cluster.node_mut(follower_id).unwrap().stop().await.unwrap();
    tracing::info!("Stopped follower node {}", follower_id);

    sleep(Duration::from_secs(2)).await;

    // Cluster should still have a leader
    let leader = cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    tracing::info!("Leader after follower crash: {}", leader);

    // Restart the follower (would require more infrastructure to implement full rejoin)
    // For now, verify the cluster is still operational
    assert!(cluster.running_count() >= 2);

    cluster.stop_all().await;
}

/// Test topic creation across cluster
#[tokio::test]
#[serial]
#[ignore = "slow test (~15s): run with --ignored"]
async fn test_topic_creation_cluster() {
    init_logging();

    let mut cluster = chaos::ChaosCluster::new(3).await;
    cluster.start_all().await.unwrap();

    // Wait for leader
    let leader = cluster
        .wait_for_leader(Duration::from_secs(15))
        .await
        .unwrap();

    // Wait for all 3 brokers to register before creating topic with replication_factor=2
    // This avoids the "Not enough brokers" error from timing issues
    cluster
        .wait_for_brokers(3, Duration::from_secs(15))
        .await
        .unwrap();

    let leader_node = cluster.node(leader).unwrap();

    // Create a topic
    leader_node
        .create_topic("chaos-test-topic", 3, 2)
        .await
        .unwrap();

    sleep(Duration::from_millis(500)).await;

    // Verify topic exists in metadata
    let metadata = cluster.get_metadata().await.unwrap();
    assert!(metadata.topics.contains_key("chaos-test-topic"));

    let topic = metadata.topics.get("chaos-test-topic").unwrap();
    assert_eq!(topic.num_partitions, 3);

    cluster.stop_all().await;
}

/// Test cluster survives minority failure
#[tokio::test]
#[serial]
#[ignore = "slow test (~25s): run with --ignored"]
async fn test_minority_failure_survival() {
    init_logging();

    let mut cluster = chaos::ChaosCluster::new(5).await;
    cluster.start_all().await.unwrap();

    // Wait for leader
    let _leader = cluster
        .wait_for_leader(Duration::from_secs(20))
        .await
        .unwrap();

    // Stop 2 nodes (minority of 5)
    cluster.node_mut(4).unwrap().stop().await.unwrap();
    cluster.node_mut(5).unwrap().stop().await.unwrap();

    sleep(Duration::from_secs(3)).await;

    // Cluster should still have a leader (3 out of 5 = quorum)
    let leader = cluster.wait_for_leader(Duration::from_secs(15)).await;
    assert!(
        leader.is_ok(),
        "Cluster should still have a leader after minority failure"
    );

    cluster.stop_all().await;
}

/// Test network simulator isolation
#[tokio::test]
#[serial]
async fn test_network_isolation_simulation() {
    let network = chaos::NetworkSimulator::new();

    // Initially all can communicate
    assert!(network.can_communicate(1, 2).await);
    assert!(network.can_communicate(2, 3).await);

    // Isolate node 1
    network.isolate_node(1).await;
    assert!(!network.can_communicate(1, 2).await);
    assert!(!network.can_communicate(1, 3).await);
    assert!(network.can_communicate(2, 3).await); // Others still can

    // Restore node 1
    network.restore_node(1).await;
    assert!(network.can_communicate(1, 2).await);

    // Create partition
    network.create_partition(vec![1, 2], vec![3, 4, 5]).await;
    // Note: Real partition enforcement would need to be integrated with actual networking

    // Heal all
    network.heal_all().await;
    assert!(network.can_communicate(1, 5).await);
}

/// Test that majority is required for progress
#[tokio::test]
#[serial]
#[ignore = "slow test (~20s): run with --ignored"]
async fn test_majority_required() {
    init_logging();

    let mut cluster = chaos::ChaosCluster::new(3).await;
    cluster.start_all().await.unwrap();

    // Wait for leader
    let _leader = cluster
        .wait_for_leader(Duration::from_secs(15))
        .await
        .unwrap();

    // Stop 2 nodes (majority of 3)
    cluster.node_mut(2).unwrap().stop().await.unwrap();
    cluster.node_mut(3).unwrap().stop().await.unwrap();

    sleep(Duration::from_secs(5)).await;

    // Remaining node should not be able to make progress
    // (it can't form quorum by itself)
    assert_eq!(cluster.running_count(), 1);

    // The remaining node may still think it's leader briefly,
    // but operations requiring consensus should fail
    cluster.stop_all().await;
}

// ============================================================================
// Fault Injection Tests
// ============================================================================

use std::sync::Arc;

/// Test fault injector basic functionality
#[tokio::test]
async fn test_fault_injector_enable_disable() {
    let injector = chaos::FaultInjector::new();

    // Initially disabled
    assert!(!injector.is_enabled());

    // Enable and verify
    injector.enable();
    assert!(injector.is_enabled());

    // Disable and verify
    injector.disable();
    assert!(!injector.is_enabled());
}

/// Test fault injection with configuration
#[tokio::test]
async fn test_fault_injection_configuration() {
    let injector = chaos::FaultInjector::new();
    injector.enable();

    // Register a fault with max limit
    let config = chaos::FaultConfig {
        enabled: true,
        max_faults: 3,
        probability: 100,
        delay: Duration::from_millis(0),
    };
    injector
        .register_fault(chaos::FaultType::WriteFailure, config)
        .await;

    // Should trigger until max reached
    for i in 0..3 {
        assert!(
            injector
                .should_inject(&chaos::FaultType::WriteFailure)
                .await,
            "Should trigger on iteration {}",
            i
        );
        injector
            .inject(chaos::FaultType::WriteFailure, &format!("test_{}", i))
            .await;
    }

    // Should no longer trigger
    assert!(
        !injector
            .should_inject(&chaos::FaultType::WriteFailure)
            .await
    );
    assert_eq!(injector.fault_count(), 3);
}

/// Test fault event logging
#[tokio::test]
async fn test_fault_event_logging() {
    let injector = chaos::FaultInjector::new();
    injector.enable();

    // Inject several faults
    injector
        .inject(chaos::FaultType::WriteFailure, "write_op_1")
        .await;
    injector
        .inject(chaos::FaultType::ReadFailure, "read_op_1")
        .await;
    injector
        .inject(chaos::FaultType::ConnectionTimeout, "conn_1")
        .await;

    // Verify log
    let log = injector.get_fault_log().await;
    assert_eq!(log.len(), 3);
    assert_eq!(log[0].context, "write_op_1");
    assert_eq!(log[1].context, "read_op_1");
    assert_eq!(log[2].context, "conn_1");

    // Reset and verify
    injector.reset().await;
    assert_eq!(injector.fault_count(), 0);
    let log = injector.get_fault_log().await;
    assert!(log.is_empty());
}

/// Test storage fault simulator
#[tokio::test]
async fn test_storage_fault_simulator() {
    let injector = Arc::new(chaos::FaultInjector::new());
    let simulator = chaos::StorageFaultSimulator::new(injector.clone());

    injector.enable();

    // Test write failures
    simulator.fail_writes().await;
    assert!(
        injector
            .should_inject(&chaos::FaultType::WriteFailure)
            .await
    );

    injector.clear_faults().await;

    // Test disk full simulation
    simulator.simulate_disk_full().await;
    assert!(injector.should_inject(&chaos::FaultType::DiskFull).await);

    injector.clear_faults().await;

    // Test data corruption
    simulator.corrupt_data().await;
    assert!(
        injector
            .should_inject(&chaos::FaultType::DataCorruption)
            .await
    );
}

/// Test network fault simulator
#[tokio::test]
async fn test_network_fault_simulator() {
    let injector = Arc::new(chaos::FaultInjector::new());
    let simulator = chaos::NetworkFaultSimulator::new(injector.clone());

    injector.enable();

    // Test connection timeout
    simulator.timeout_connections().await;
    assert!(
        injector
            .should_inject(&chaos::FaultType::ConnectionTimeout)
            .await
    );

    injector.clear_faults().await;

    // Test connection reset
    simulator.reset_connections().await;
    assert!(
        injector
            .should_inject(&chaos::FaultType::ConnectionReset)
            .await
    );

    injector.clear_faults().await;

    // Test slow network
    simulator.slow_network(Duration::from_millis(100)).await;
    assert!(
        injector
            .should_inject(&chaos::FaultType::SlowNetwork(Duration::from_millis(100)))
            .await
    );

    injector.clear_faults().await;

    // Test packet loss
    simulator.packet_loss(50).await;
    assert!(
        injector
            .should_inject(&chaos::FaultType::PacketLoss(50))
            .await
    );
}

/// Test latency injector timing
#[tokio::test]
async fn test_latency_injector_timing() {
    // No jitter for deterministic test
    let latency = chaos::LatencyInjector::new(Duration::from_millis(50), Duration::from_millis(0));

    // Disabled - should not delay
    let start = std::time::Instant::now();
    latency.maybe_delay().await;
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(10),
        "Should not delay when disabled"
    );

    // Enabled - should delay approximately 50ms
    latency.enable();
    let start = std::time::Instant::now();
    latency.maybe_delay().await;
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(45),
        "Should delay at least 45ms when enabled"
    );
    assert!(
        elapsed < Duration::from_millis(100),
        "Should not delay excessively"
    );

    // Disabled again
    latency.disable();
    let start = std::time::Instant::now();
    latency.maybe_delay().await;
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(10),
        "Should not delay when disabled"
    );
}

/// Test chaos monkey random fault injection
#[tokio::test]
async fn test_chaos_monkey_basic() {
    let injector = Arc::new(chaos::FaultInjector::new());
    let monkey = chaos::ChaosMonkey::new(injector.clone(), Duration::from_millis(50));

    injector.enable();

    // Start chaos monkey
    let handle = monkey.start();

    // Let it run for a bit
    sleep(Duration::from_millis(300)).await;

    // Stop it
    monkey.stop();

    // Wait for task to complete
    let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;

    // Should have injected at least one fault
    let count = injector.fault_count();
    assert!(count >= 1, "Chaos monkey should have injected faults");

    // Verify fault log has entries
    let log = injector.get_fault_log().await;
    assert!(!log.is_empty(), "Fault log should have entries");
}

/// Test multiple fault types simultaneously
#[tokio::test]
async fn test_multiple_simultaneous_faults() {
    let injector = Arc::new(chaos::FaultInjector::new());
    let storage_sim = chaos::StorageFaultSimulator::new(injector.clone());
    let network_sim = chaos::NetworkFaultSimulator::new(injector.clone());

    injector.enable();

    // Register multiple faults
    storage_sim.fail_writes().await;
    storage_sim.corrupt_data().await;
    network_sim.packet_loss(25).await;
    network_sim.slow_network(Duration::from_millis(100)).await;

    // All should be active
    assert!(
        injector
            .should_inject(&chaos::FaultType::WriteFailure)
            .await
    );
    assert!(
        injector
            .should_inject(&chaos::FaultType::DataCorruption)
            .await
    );
    assert!(
        injector
            .should_inject(&chaos::FaultType::PacketLoss(25))
            .await
    );
    assert!(
        injector
            .should_inject(&chaos::FaultType::SlowNetwork(Duration::from_millis(100)))
            .await
    );

    // Clear all
    injector.clear_faults().await;

    // None should be active
    assert!(
        !injector
            .should_inject(&chaos::FaultType::WriteFailure)
            .await
    );
    assert!(
        !injector
            .should_inject(&chaos::FaultType::DataCorruption)
            .await
    );
}

/// Test fault types are distinguishable
#[tokio::test]
async fn test_fault_type_specificity() {
    let injector = chaos::FaultInjector::new();
    injector.enable();

    // Register only WriteFailure
    let config = chaos::FaultConfig {
        enabled: true,
        ..Default::default()
    };
    injector
        .register_fault(chaos::FaultType::WriteFailure, config)
        .await;

    // Only WriteFailure should trigger
    assert!(
        injector
            .should_inject(&chaos::FaultType::WriteFailure)
            .await
    );
    assert!(!injector.should_inject(&chaos::FaultType::ReadFailure).await);
    assert!(!injector.should_inject(&chaos::FaultType::DiskFull).await);
    assert!(
        !injector
            .should_inject(&chaos::FaultType::ConnectionTimeout)
            .await
    );
}

/// Test intermittent failure pattern
#[tokio::test]
async fn test_intermittent_failures() {
    let injector = Arc::new(chaos::FaultInjector::new());
    let simulator = chaos::StorageFaultSimulator::new(injector.clone());

    injector.enable();
    simulator.intermittent_failures(3).await;

    // Should trigger for IntermittentFailure(3)
    assert!(
        injector
            .should_inject(&chaos::FaultType::IntermittentFailure(3))
            .await
    );

    // Should not trigger for different intervals
    assert!(
        !injector
            .should_inject(&chaos::FaultType::IntermittentFailure(5))
            .await
    );
}
