//! Integration tests for Streamline
//!
//! These tests verify end-to-end functionality of the storage layer
//! and topic management.

use bytes::Bytes;
use streamline::{ServerConfig, TopicManager};
use tempfile::tempdir;

/// Test creating and managing topics through TopicManager
#[test]
fn test_topic_lifecycle() {
    let dir = tempdir().unwrap();
    let manager = TopicManager::new(dir.path()).unwrap();

    // Create multiple topics
    manager.create_topic("orders", 4).unwrap();
    manager.create_topic("events", 2).unwrap();
    manager.create_topic("logs", 1).unwrap();

    // Verify topics were created
    let topics = manager.list_topics().unwrap();
    assert_eq!(topics.len(), 3);

    // Verify topic metadata
    let orders_meta = manager.get_topic_metadata("orders").unwrap();
    assert_eq!(orders_meta.name, "orders");
    assert_eq!(orders_meta.num_partitions, 4);

    // Delete a topic
    manager.delete_topic("logs").unwrap();
    let topics = manager.list_topics().unwrap();
    assert_eq!(topics.len(), 2);

    // Verify deleted topic is gone
    let result = manager.get_topic_metadata("logs");
    assert!(result.is_err());
}

/// Test writing and reading records across multiple partitions
#[test]
fn test_multi_partition_write_read() {
    let dir = tempdir().unwrap();
    let manager = TopicManager::new(dir.path()).unwrap();

    manager.create_topic("multi-partition-test", 3).unwrap();

    // Write records to different partitions
    let offsets: Vec<i64> = (0..3)
        .map(|partition| {
            manager
                .append(
                    "multi-partition-test",
                    partition,
                    Some(Bytes::from(format!("key-{}", partition))),
                    Bytes::from(format!("value-{}", partition)),
                )
                .unwrap()
        })
        .collect();

    // All should be offset 0 (first record in each partition)
    assert!(offsets.iter().all(|&o| o == 0));

    // Write more records to partition 0
    for i in 1..10 {
        let offset = manager
            .append(
                "multi-partition-test",
                0,
                Some(Bytes::from(format!("key-{}", i))),
                Bytes::from(format!("value-{}", i)),
            )
            .unwrap();
        assert_eq!(offset, i);
    }

    // Read all records from partition 0
    let records = manager.read("multi-partition-test", 0, 0, 100).unwrap();
    assert_eq!(records.len(), 10);

    // Verify record content
    assert_eq!(records[0].value, Bytes::from("value-0"));
    assert_eq!(records[9].value, Bytes::from("value-9"));

    // Read from middle offset
    let records = manager.read("multi-partition-test", 0, 5, 100).unwrap();
    assert_eq!(records.len(), 5);
    assert_eq!(records[0].offset, 5);
}

/// Test offset tracking
#[test]
fn test_offset_tracking() {
    let dir = tempdir().unwrap();
    let manager = TopicManager::new(dir.path()).unwrap();

    manager.create_topic("offset-test", 1).unwrap();

    // Initially, offsets should be 0
    let earliest = manager.earliest_offset("offset-test", 0).unwrap();
    let latest = manager.latest_offset("offset-test", 0).unwrap();
    assert_eq!(earliest, 0);
    assert_eq!(latest, 0);

    // Write some records
    for i in 0..5 {
        manager
            .append("offset-test", 0, None, Bytes::from(format!("msg-{}", i)))
            .unwrap();
    }

    // Check offsets after writes
    let earliest = manager.earliest_offset("offset-test", 0).unwrap();
    let latest = manager.latest_offset("offset-test", 0).unwrap();
    let high_watermark = manager.high_watermark("offset-test", 0).unwrap();

    assert_eq!(earliest, 0);
    assert_eq!(latest, 5); // Next offset to be written
    assert_eq!(high_watermark, 5); // Committed offset
}

/// Test persistence across restarts
#[test]
fn test_persistence() {
    let dir = tempdir().unwrap();
    let topic_name = "persistent-topic";

    // First session: create topic and write data
    {
        let manager = TopicManager::new(dir.path()).unwrap();
        manager.create_topic(topic_name, 2).unwrap();

        manager
            .append(
                topic_name,
                0,
                Some(Bytes::from("key1")),
                Bytes::from("persisted-value-1"),
            )
            .unwrap();
        manager
            .append(
                topic_name,
                1,
                Some(Bytes::from("key2")),
                Bytes::from("persisted-value-2"),
            )
            .unwrap();
    }

    // Second session: verify data persisted
    {
        let manager = TopicManager::new(dir.path()).unwrap();

        // Topic should exist
        let topics = manager.list_topics().unwrap();
        assert_eq!(topics.len(), 1);

        // Data should be readable
        let records = manager.read(topic_name, 0, 0, 100).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value, Bytes::from("persisted-value-1"));

        let records = manager.read(topic_name, 1, 0, 100).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value, Bytes::from("persisted-value-2"));
    }
}

/// Test auto-create topic functionality
#[test]
fn test_auto_create_topic() {
    let dir = tempdir().unwrap();
    let manager = TopicManager::new(dir.path()).unwrap();

    // Topic doesn't exist yet
    let topics = manager.list_topics().unwrap();
    assert_eq!(topics.len(), 0);

    // Auto-create on first use
    manager.get_or_create_topic("auto-created", 3).unwrap();

    // Verify topic was created
    let metadata = manager.get_topic_metadata("auto-created").unwrap();
    assert_eq!(metadata.num_partitions, 3);

    // Calling again should be idempotent
    manager.get_or_create_topic("auto-created", 3).unwrap();
    let topics = manager.list_topics().unwrap();
    assert_eq!(topics.len(), 1);
}

/// Test error handling for invalid operations
#[test]
fn test_error_handling() {
    let dir = tempdir().unwrap();
    let manager = TopicManager::new(dir.path()).unwrap();

    // Reading from non-existent topic should fail
    let result = manager.read("non-existent", 0, 0, 100);
    assert!(result.is_err());

    // Writing to non-existent topic should fail
    let result = manager.append("non-existent", 0, None, Bytes::from("data"));
    assert!(result.is_err());

    // Create topic
    manager.create_topic("test-topic", 2).unwrap();

    // Writing to invalid partition should fail
    let result = manager.append("test-topic", 99, None, Bytes::from("data"));
    assert!(result.is_err());

    // Creating duplicate topic should fail
    let result = manager.create_topic("test-topic", 2);
    assert!(result.is_err());

    // Deleting non-existent topic should fail
    let result = manager.delete_topic("non-existent");
    assert!(result.is_err());
}

/// Test large record batches
#[test]
fn test_large_batch_write() {
    let dir = tempdir().unwrap();
    let manager = TopicManager::new(dir.path()).unwrap();

    manager.create_topic("large-batch", 1).unwrap();

    // Write 1000 records
    let num_records = 1000;
    for i in 0..num_records {
        manager
            .append(
                "large-batch",
                0,
                Some(Bytes::from(format!("key-{:05}", i))),
                Bytes::from(format!("value-{:05}-{}", i, "x".repeat(100))),
            )
            .unwrap();
    }

    // Read all records
    let records = manager
        .read("large-batch", 0, 0, num_records + 100)
        .unwrap();
    assert_eq!(records.len(), num_records);

    // Verify ordering
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.offset, i as i64);
    }
}

/// Test ServerConfig defaults
#[test]
fn test_server_config_defaults() {
    let config = ServerConfig::default();

    assert_eq!(config.listen_addr.to_string(), "0.0.0.0:9092");
    assert_eq!(config.http_addr.to_string(), "0.0.0.0:9094");
}
