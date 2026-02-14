//! Integration tests for Streamline clustering
//!
//! These tests verify cluster formation, Raft consensus, leadership,
//! and topic creation across a multi-node cluster.
//!
//! This test requires the `clustering` feature to be enabled.
#![cfg(feature = "clustering")]

use std::sync::Arc;
use std::time::Duration;

use streamline::{
    AcksPolicy, ClusterConfig, ClusterManager, InterBrokerTlsConfig, NodeState, ReplicationConfig,
    ReplicationManager, TopicManager,
};
use tempfile::tempdir;
use tokio::time::sleep;

/// Helper to create a test cluster config
fn test_cluster_config(node_id: u64, kafka_port: u16, inter_broker_port: u16) -> ClusterConfig {
    ClusterConfig {
        node_id,
        advertised_addr: format!("127.0.0.1:{}", kafka_port).parse().unwrap(),
        inter_broker_addr: format!("127.0.0.1:{}", inter_broker_port).parse().unwrap(),
        seed_nodes: vec![],
        default_replication_factor: 1,
        min_insync_replicas: 1,
        heartbeat_interval_ms: 200,
        session_timeout_ms: 5000,
        election_timeout_min_ms: 500,
        election_timeout_max_ms: 1000,
        unclean_leader_election: false,
        inter_broker_tls: InterBrokerTlsConfig::default(),
        rack_id: None,
        rack_aware_assignment: true,
    }
}

/// Test single-node cluster bootstrap
#[tokio::test]
async fn test_single_node_cluster_bootstrap() {
    let dir = tempdir().unwrap();
    let config = test_cluster_config(1, 19092, 19093);

    let manager = ClusterManager::new(config.clone(), dir.path().to_path_buf())
        .await
        .unwrap();

    assert_eq!(manager.node_id(), 1);
    assert!(!manager.is_leader().await);

    // Bootstrap the cluster
    manager.bootstrap().await.unwrap();

    // Wait for leader election
    sleep(Duration::from_secs(3)).await;

    // Single node should become leader
    assert!(manager.is_leader().await);
    assert!(manager.is_controller().await);

    // Metadata should have our broker registered
    let metadata = manager.metadata().await;
    assert!(metadata.brokers.contains_key(&1));

    let broker = metadata.brokers.get(&1).unwrap();
    assert_eq!(broker.node_id, 1);
    assert_eq!(broker.state, NodeState::Running);

    // Shutdown gracefully
    manager.shutdown().await.unwrap();
}

/// Test cluster metadata updates
#[tokio::test]
async fn test_cluster_metadata_updates() {
    let dir = tempdir().unwrap();
    let config = test_cluster_config(1, 19094, 19095);

    let manager = ClusterManager::new(config.clone(), dir.path().to_path_buf())
        .await
        .unwrap();

    manager.bootstrap().await.unwrap();
    sleep(Duration::from_secs(3)).await;

    // Initial metadata
    let metadata = manager.metadata().await;
    assert_eq!(metadata.brokers.len(), 1);
    assert!(metadata.topics.is_empty());

    // Create a topic
    manager
        .create_topic("test-topic".to_string(), 3, 1)
        .await
        .unwrap();

    // Verify topic in metadata
    let metadata = manager.metadata().await;
    assert!(metadata.topics.contains_key("test-topic"));

    let topic = metadata.topics.get("test-topic").unwrap();
    assert_eq!(topic.name, "test-topic");
    assert_eq!(topic.num_partitions, 3);
    assert_eq!(topic.partitions.len(), 3);

    // Each partition should have the leader assigned
    for partition in topic.partitions.values() {
        assert_eq!(partition.leader, Some(1));
        assert_eq!(partition.replicas, vec![1]);
    }

    // Delete topic
    manager.delete_topic("test-topic").await.unwrap();

    let metadata = manager.metadata().await;
    assert!(!metadata.topics.contains_key("test-topic"));

    manager.shutdown().await.unwrap();
}

/// Test cluster metrics
#[tokio::test]
async fn test_cluster_metrics() {
    let dir = tempdir().unwrap();
    let config = test_cluster_config(1, 19096, 19097);

    let manager = ClusterManager::new(config.clone(), dir.path().to_path_buf())
        .await
        .unwrap();

    manager.bootstrap().await.unwrap();
    sleep(Duration::from_secs(3)).await;

    let metrics = manager.metrics();

    // Should be leader
    assert_eq!(metrics.current_leader, Some(1));

    // Should have one voter (self)
    let membership = metrics.membership_config.membership();
    let voters: Vec<_> = membership.voter_ids().collect();
    assert_eq!(voters, vec![1]);

    manager.shutdown().await.unwrap();
}

/// Test controller ID tracking
#[tokio::test]
async fn test_controller_id() {
    let dir = tempdir().unwrap();
    let config = test_cluster_config(1, 19098, 19099);

    let manager = ClusterManager::new(config.clone(), dir.path().to_path_buf())
        .await
        .unwrap();

    // Before bootstrap, no leader
    assert_eq!(manager.leader_id().await, None);
    assert_eq!(manager.controller_id().await, None);

    manager.bootstrap().await.unwrap();
    sleep(Duration::from_secs(3)).await;

    // After bootstrap, self is leader/controller
    assert_eq!(manager.leader_id().await, Some(1));
    assert_eq!(manager.controller_id().await, Some(1));

    manager.shutdown().await.unwrap();
}

/// Test creating multiple topics
#[tokio::test]
async fn test_multiple_topics_creation() {
    let dir = tempdir().unwrap();
    let config = test_cluster_config(1, 19100, 19101);

    let manager = ClusterManager::new(config.clone(), dir.path().to_path_buf())
        .await
        .unwrap();

    manager.bootstrap().await.unwrap();
    sleep(Duration::from_secs(3)).await;

    // Create multiple topics
    manager
        .create_topic("topic-a".to_string(), 1, 1)
        .await
        .unwrap();
    manager
        .create_topic("topic-b".to_string(), 2, 1)
        .await
        .unwrap();
    manager
        .create_topic("topic-c".to_string(), 4, 1)
        .await
        .unwrap();

    let metadata = manager.metadata().await;
    assert_eq!(metadata.topics.len(), 3);

    assert_eq!(metadata.topics.get("topic-a").unwrap().num_partitions, 1);
    assert_eq!(metadata.topics.get("topic-b").unwrap().num_partitions, 2);
    assert_eq!(metadata.topics.get("topic-c").unwrap().num_partitions, 4);

    manager.shutdown().await.unwrap();
}

/// Test ISR updates through cluster
#[tokio::test]
async fn test_isr_update() {
    let dir = tempdir().unwrap();
    let config = test_cluster_config(1, 19102, 19103);

    let manager = ClusterManager::new(config.clone(), dir.path().to_path_buf())
        .await
        .unwrap();

    manager.bootstrap().await.unwrap();
    sleep(Duration::from_secs(3)).await;

    // Create topic
    manager
        .create_topic("isr-test".to_string(), 1, 1)
        .await
        .unwrap();

    // Initial ISR should have leader
    let metadata = manager.metadata().await;
    let partition = metadata
        .topics
        .get("isr-test")
        .unwrap()
        .partitions
        .get(&0)
        .unwrap();
    assert_eq!(partition.isr, vec![1]);

    // Update ISR (in real scenario, this happens during replication)
    manager
        .update_partition_isr("isr-test", 0, vec![1])
        .await
        .unwrap();

    manager.shutdown().await.unwrap();
}

/// Test partition leader update
#[tokio::test]
async fn test_partition_leader_update() {
    let dir = tempdir().unwrap();
    let config = test_cluster_config(1, 19104, 19105);

    let manager = ClusterManager::new(config.clone(), dir.path().to_path_buf())
        .await
        .unwrap();

    manager.bootstrap().await.unwrap();
    sleep(Duration::from_secs(3)).await;

    // Create topic
    manager
        .create_topic("leader-test".to_string(), 1, 1)
        .await
        .unwrap();

    // Update leader with new epoch
    manager
        .update_partition_leader("leader-test", 0, 1, 2)
        .await
        .unwrap();

    let metadata = manager.metadata().await;
    let partition = metadata
        .topics
        .get("leader-test")
        .unwrap()
        .partitions
        .get(&0)
        .unwrap();
    assert_eq!(partition.leader, Some(1));
    assert_eq!(partition.leader_epoch, 2);

    manager.shutdown().await.unwrap();
}

/// Test alive brokers filtering
#[tokio::test]
async fn test_alive_brokers() {
    let dir = tempdir().unwrap();
    let config = test_cluster_config(1, 19106, 19107);

    let manager = ClusterManager::new(config.clone(), dir.path().to_path_buf())
        .await
        .unwrap();

    manager.bootstrap().await.unwrap();
    sleep(Duration::from_secs(3)).await;

    let metadata = manager.metadata().await;
    let alive_brokers = metadata.alive_brokers();

    assert_eq!(alive_brokers.len(), 1);
    assert_eq!(alive_brokers[0].node_id, 1);
    assert_eq!(alive_brokers[0].state, NodeState::Running);

    manager.shutdown().await.unwrap();
}

/// Test replication manager integration with cluster
#[tokio::test]
async fn test_replication_manager_with_cluster() {
    let dir = tempdir().unwrap();
    let topic_dir = dir.path().join("topics");
    std::fs::create_dir_all(&topic_dir).unwrap();

    let config = test_cluster_config(1, 19108, 19109);

    let cluster_manager = Arc::new(
        ClusterManager::new(config.clone(), dir.path().join("cluster"))
            .await
            .unwrap(),
    );

    cluster_manager.bootstrap().await.unwrap();
    sleep(Duration::from_secs(3)).await;

    // Create topic manager
    let topic_manager = Arc::new(TopicManager::new(&topic_dir).unwrap());
    topic_manager.create_topic("repl-test", 1).unwrap();

    // Create replication manager
    let repl_config = ReplicationConfig::default();
    let replication_manager = ReplicationManager::new(1, repl_config);

    // Before becoming leader, should not be able to accept writes with acks=all
    assert!(
        !replication_manager
            .can_accept_write("repl-test", 0, AcksPolicy::All)
            .await
    );

    // Become leader for partition (topic, partition_id, leader_epoch, replicas, isr, leo)
    replication_manager
        .become_leader("repl-test", 0, 1, vec![1], vec![1], 0)
        .await;

    // Now should be able to accept writes with acks=all (single-node ISR)
    assert!(
        replication_manager
            .can_accept_write("repl-test", 0, AcksPolicy::All)
            .await
    );

    cluster_manager.shutdown().await.unwrap();
}

/// Test cluster config validation
#[test]
fn test_cluster_config_validation() {
    // Valid config
    let config = test_cluster_config(1, 9092, 9093);
    assert!(config.validate().is_ok());

    // Invalid: election timeout min > max
    let invalid_config = ClusterConfig {
        election_timeout_min_ms: 2000,
        election_timeout_max_ms: 1000,
        ..test_cluster_config(1, 9092, 9093)
    };
    assert!(invalid_config.validate().is_err());

    // Invalid: min_insync > replication_factor
    let invalid_config = ClusterConfig {
        default_replication_factor: 1,
        min_insync_replicas: 2,
        ..test_cluster_config(1, 9092, 9093)
    };
    assert!(invalid_config.validate().is_err());
}

/// Test membership manager
#[tokio::test]
async fn test_membership_manager() {
    let dir = tempdir().unwrap();
    let config = test_cluster_config(1, 19110, 19111);

    let manager = ClusterManager::new(config.clone(), dir.path().to_path_buf())
        .await
        .unwrap();

    manager.bootstrap().await.unwrap();
    sleep(Duration::from_secs(3)).await;

    let membership = manager.membership();

    // Should have self as node_id
    assert_eq!(membership.node_id(), 1);

    // No other peers in single-node cluster (self is registered as broker, not peer)
    let peers = membership.peers().await;
    assert!(peers.is_empty());

    manager.shutdown().await.unwrap();
}
