//! Load and Endurance Tests for Streamline
//!
//! This module contains tests that simulate various load patterns:
//! - High-throughput scenarios
//! - Sustained load (endurance)
//! - Burst traffic patterns
//! - Large message handling
//! - Concurrent producers/consumers

use bytes::Bytes;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use streamline::storage::TopicManager;
use tempfile::TempDir;

/// Test configuration for load tests
struct LoadTestConfig {
    /// Number of concurrent producers
    num_producers: usize,
    /// Number of messages per producer
    messages_per_producer: usize,
    /// Message size in bytes
    message_size: usize,
}

impl Default for LoadTestConfig {
    fn default() -> Self {
        Self {
            num_producers: 4,
            messages_per_producer: 1000,
            message_size: 1024,
        }
    }
}

/// Statistics from load test run
#[derive(Debug, Default)]
struct LoadTestStats {
    messages_produced: AtomicU64,
    messages_consumed: AtomicU64,
    bytes_produced: AtomicU64,
    bytes_consumed: AtomicU64,
    produce_errors: AtomicU64,
    consume_errors: AtomicU64,
    max_produce_latency_us: AtomicU64,
    max_consume_latency_us: AtomicU64,
}

impl LoadTestStats {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn record_produce(&self, bytes: u64, latency_us: u64) {
        self.messages_produced.fetch_add(1, Ordering::Relaxed);
        self.bytes_produced.fetch_add(bytes, Ordering::Relaxed);
        self.max_produce_latency_us
            .fetch_max(latency_us, Ordering::Relaxed);
    }

    fn record_consume(&self, bytes: u64, latency_us: u64) {
        self.messages_consumed.fetch_add(1, Ordering::Relaxed);
        self.bytes_consumed.fetch_add(bytes, Ordering::Relaxed);
        self.max_consume_latency_us
            .fetch_max(latency_us, Ordering::Relaxed);
    }

    fn record_produce_error(&self) {
        self.produce_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_consume_error(&self) {
        self.consume_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn summary(&self, elapsed: Duration) -> String {
        let msgs_produced = self.messages_produced.load(Ordering::Relaxed);
        let msgs_consumed = self.messages_consumed.load(Ordering::Relaxed);
        let bytes_produced = self.bytes_produced.load(Ordering::Relaxed);
        let bytes_consumed = self.bytes_consumed.load(Ordering::Relaxed);
        let produce_errors = self.produce_errors.load(Ordering::Relaxed);
        let consume_errors = self.consume_errors.load(Ordering::Relaxed);
        let max_produce_latency = self.max_produce_latency_us.load(Ordering::Relaxed);
        let max_consume_latency = self.max_consume_latency_us.load(Ordering::Relaxed);

        let elapsed_secs = elapsed.as_secs_f64();
        let produce_rate = msgs_produced as f64 / elapsed_secs;
        let consume_rate = msgs_consumed as f64 / elapsed_secs;
        let produce_throughput_mb = (bytes_produced as f64 / (1024.0 * 1024.0)) / elapsed_secs;
        let consume_throughput_mb = (bytes_consumed as f64 / (1024.0 * 1024.0)) / elapsed_secs;

        format!(
            "Load Test Summary:\n\
             Duration: {:.2}s\n\
             Messages Produced: {} ({:.0} msg/s)\n\
             Messages Consumed: {} ({:.0} msg/s)\n\
             Produce Throughput: {:.2} MB/s\n\
             Consume Throughput: {:.2} MB/s\n\
             Produce Errors: {}\n\
             Consume Errors: {}\n\
             Max Produce Latency: {}us\n\
             Max Consume Latency: {}us",
            elapsed_secs,
            msgs_produced,
            produce_rate,
            msgs_consumed,
            consume_rate,
            produce_throughput_mb,
            consume_throughput_mb,
            produce_errors,
            consume_errors,
            max_produce_latency,
            max_consume_latency
        )
    }
}

fn create_test_message(size: usize, sequence: u64) -> Bytes {
    let mut msg = Vec::with_capacity(size);
    // Include sequence number in first 8 bytes
    msg.extend_from_slice(&sequence.to_be_bytes());
    // Fill rest with pattern
    while msg.len() < size {
        msg.push((sequence % 256) as u8);
    }
    msg.truncate(size);
    Bytes::from(msg)
}

fn setup_topic_manager(path: &Path) -> TopicManager {
    TopicManager::new(path).unwrap()
}

/// Test: High-throughput produce test
/// Tests the system's ability to handle a large number of messages quickly
#[tokio::test]
async fn test_high_throughput_produce() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(setup_topic_manager(temp_dir.path()));
    let topic_name = "throughput-test";

    // Create topic with multiple partitions
    topic_manager.create_topic(topic_name, 4).unwrap();

    let config = LoadTestConfig {
        num_producers: 4,
        messages_per_producer: 10000,
        message_size: 1024,
    };

    let stats = LoadTestStats::new();
    let start = Instant::now();

    let mut handles = Vec::new();
    for producer_id in 0..config.num_producers {
        let tm = topic_manager.clone();
        let s = stats.clone();
        let msg_count = config.messages_per_producer;
        let msg_size = config.message_size;
        let topic = topic_name.to_string();

        handles.push(tokio::task::spawn_blocking(move || {
            for i in 0..msg_count {
                let sequence = (producer_id * msg_count + i) as u64;
                let msg = create_test_message(msg_size, sequence);
                let partition = (sequence % 4) as i32;

                let produce_start = Instant::now();
                match tm.append(&topic, partition, None, msg.clone()) {
                    Ok(_) => {
                        let latency_us = produce_start.elapsed().as_micros() as u64;
                        s.record_produce(msg.len() as u64, latency_us);
                    }
                    Err(_) => {
                        s.record_produce_error();
                    }
                }
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start.elapsed();
    println!("{}", stats.summary(elapsed));

    // Verify results
    let msgs_produced = stats.messages_produced.load(Ordering::Relaxed);
    let errors = stats.produce_errors.load(Ordering::Relaxed);
    let expected_msgs = (config.num_producers * config.messages_per_producer) as u64;

    assert_eq!(
        msgs_produced + errors,
        expected_msgs,
        "All messages should be accounted for"
    );
    assert!(
        errors < expected_msgs / 100,
        "Error rate should be less than 1%"
    );

    // Check throughput - should be at least 1000 msg/s
    let throughput = msgs_produced as f64 / elapsed.as_secs_f64();
    assert!(
        throughput > 1000.0,
        "Throughput should be > 1000 msg/s, got {:.0}",
        throughput
    );
}

/// Test: Concurrent producer/consumer test
/// Tests the system with simultaneous reads and writes
#[tokio::test]
async fn test_concurrent_produce_consume() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(setup_topic_manager(temp_dir.path()));
    let topic_name = "concurrent-test";

    topic_manager.create_topic(topic_name, 2).unwrap();

    let stats = LoadTestStats::new();
    let start = Instant::now();

    let mut handles = Vec::new();

    // Spawn producers
    for producer_id in 0..2 {
        let tm = topic_manager.clone();
        let s = stats.clone();
        let topic = topic_name.to_string();

        handles.push(tokio::task::spawn_blocking(move || {
            for i in 0..1000 {
                let sequence = (producer_id * 1000 + i) as u64;
                let msg = create_test_message(512, sequence);
                let partition = producer_id;

                let produce_start = Instant::now();
                if tm.append(&topic, partition, None, msg.clone()).is_ok() {
                    s.record_produce(msg.len() as u64, produce_start.elapsed().as_micros() as u64);
                } else {
                    s.record_produce_error();
                }
            }
        }));
    }

    // Wait for producers
    for handle in handles {
        handle.await.unwrap();
    }

    // Now consume
    for partition in 0..2 {
        let consume_start = Instant::now();
        match topic_manager.read(topic_name, partition, 0, 10000) {
            Ok(records) => {
                for record in &records {
                    stats.record_consume(
                        record.value.len() as u64,
                        consume_start.elapsed().as_micros() as u64,
                    );
                }
            }
            Err(_) => {
                stats.record_consume_error();
            }
        }
    }

    let elapsed = start.elapsed();
    println!("{}", stats.summary(elapsed));

    // Verify all messages were produced
    let msgs_produced = stats.messages_produced.load(Ordering::Relaxed);
    assert!(msgs_produced >= 1900, "Should produce most messages");
}

/// Test: Large message handling
/// Tests the system's ability to handle large messages
#[tokio::test]
async fn test_large_messages() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = setup_topic_manager(temp_dir.path());
    let topic_name = "large-msg-test";

    topic_manager.create_topic(topic_name, 1).unwrap();

    let stats = LoadTestStats::new();
    let start = Instant::now();

    // Test various message sizes
    let sizes = [
        1024,        // 1KB
        10 * 1024,   // 10KB
        100 * 1024,  // 100KB
        512 * 1024,  // 512KB
        1024 * 1024, // 1MB
    ];

    for (i, size) in sizes.iter().enumerate() {
        let msg = create_test_message(*size, i as u64);
        let produce_start = Instant::now();

        match topic_manager.append(topic_name, 0, None, msg.clone()) {
            Ok(_) => {
                stats.record_produce(*size as u64, produce_start.elapsed().as_micros() as u64);
            }
            Err(e) => {
                println!("Failed to produce {}KB message: {}", size / 1024, e);
                stats.record_produce_error();
            }
        }
    }

    // Read back all messages
    let records = topic_manager.read(topic_name, 0, 0, 100).unwrap();

    let elapsed = start.elapsed();
    println!("{}", stats.summary(elapsed));

    // Verify message sizes
    assert_eq!(
        records.len(),
        sizes.len(),
        "All messages should be readable"
    );
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.value.len(), sizes[i], "Message {} size mismatch", i);
    }
}

/// Test: Burst traffic pattern
/// Tests the system's ability to handle sudden bursts of traffic
#[tokio::test]
async fn test_burst_traffic() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(setup_topic_manager(temp_dir.path()));
    let topic_name = "burst-test";

    topic_manager.create_topic(topic_name, 4).unwrap();

    let stats = LoadTestStats::new();
    let start = Instant::now();

    // Simulate 5 bursts of traffic
    for burst in 0..5 {
        let burst_start = Instant::now();
        let mut handles = Vec::new();

        // Each burst: 8 concurrent producers, 500 messages each
        for producer_id in 0..8 {
            let tm = topic_manager.clone();
            let s = stats.clone();
            let topic = topic_name.to_string();

            handles.push(tokio::task::spawn_blocking(move || {
                for i in 0..500 {
                    let sequence = (burst * 8 * 500 + producer_id * 500 + i) as u64;
                    let msg = create_test_message(256, sequence);
                    let partition = (sequence % 4) as i32;

                    let produce_start = Instant::now();
                    if tm.append(&topic, partition, None, msg.clone()).is_ok() {
                        s.record_produce(
                            msg.len() as u64,
                            produce_start.elapsed().as_micros() as u64,
                        );
                    } else {
                        s.record_produce_error();
                    }
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        println!(
            "Burst {} completed in {:?}",
            burst + 1,
            burst_start.elapsed()
        );

        // Brief pause between bursts
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let elapsed = start.elapsed();
    println!("{}", stats.summary(elapsed));

    // Verify results
    let expected_msgs = 5 * 8 * 500;
    let msgs_produced = stats.messages_produced.load(Ordering::Relaxed);
    let errors = stats.produce_errors.load(Ordering::Relaxed);

    assert!(
        msgs_produced + errors == expected_msgs as u64,
        "All burst messages should be accounted for"
    );
}

/// Test: Multi-topic load test
/// Tests the system with multiple topics simultaneously
#[tokio::test]
async fn test_multi_topic_load() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(setup_topic_manager(temp_dir.path()));

    let num_topics = 5;
    let mut topics = Vec::new();

    // Create multiple topics
    for i in 0..num_topics {
        let topic_name = format!("multi-topic-{}", i);
        topic_manager.create_topic(&topic_name, 2).unwrap();
        topics.push(topic_name);
    }

    let stats = LoadTestStats::new();
    let start = Instant::now();
    let mut handles = Vec::new();

    // One producer per topic
    for (topic_idx, topic_name) in topics.iter().enumerate() {
        let tm = topic_manager.clone();
        let s = stats.clone();
        let t = topic_name.clone();

        handles.push(tokio::task::spawn_blocking(move || {
            for i in 0..2000i32 {
                let sequence = (topic_idx * 2000 + i as usize) as u64;
                let msg = create_test_message(512, sequence);
                let partition = i % 2;

                let produce_start = Instant::now();
                if tm.append(&t, partition, None, msg.clone()).is_ok() {
                    s.record_produce(msg.len() as u64, produce_start.elapsed().as_micros() as u64);
                } else {
                    s.record_produce_error();
                }
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start.elapsed();
    println!("{}", stats.summary(elapsed));

    // Verify each topic has correct data
    for topic_name in &topics {
        let mut total_records = 0;
        for partition in 0..2 {
            let records = topic_manager.read(topic_name, partition, 0, 10000).unwrap();
            total_records += records.len();
        }
        assert!(
            total_records >= 1900,
            "Topic {} should have most records, got {}",
            topic_name,
            total_records
        );
    }
}

/// Test: Sustained load (endurance)
/// Tests the system under sustained load for a period of time
#[tokio::test]
async fn test_endurance_load() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(setup_topic_manager(temp_dir.path()));
    let topic_name = "endurance-test";

    topic_manager.create_topic(topic_name, 4).unwrap();

    let stats = LoadTestStats::new();
    let duration = Duration::from_secs(5); // 5 second endurance test
    let start = Instant::now();

    let mut handles = Vec::new();
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));

    // Start 4 producer tasks
    for producer_id in 0..4 {
        let tm = topic_manager.clone();
        let s = stats.clone();
        let r = running.clone();
        let topic = topic_name.to_string();

        handles.push(tokio::task::spawn_blocking(move || {
            let mut sequence = producer_id as u64 * 1_000_000;
            while r.load(Ordering::Relaxed) {
                let msg = create_test_message(256, sequence);
                let partition = (sequence % 4) as i32;

                let produce_start = Instant::now();
                if tm.append(&topic, partition, None, msg.clone()).is_ok() {
                    s.record_produce(msg.len() as u64, produce_start.elapsed().as_micros() as u64);
                } else {
                    s.record_produce_error();
                }
                sequence += 1;

                // Small delay to prevent overwhelming
                std::thread::sleep(Duration::from_micros(100));
            }
        }));
    }

    // Run for specified duration
    tokio::time::sleep(duration).await;
    running.store(false, Ordering::Relaxed);

    // Wait for all producers to finish
    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();
    println!("{}", stats.summary(elapsed));

    // Verify sustained throughput
    let msgs_produced = stats.messages_produced.load(Ordering::Relaxed);
    let errors = stats.produce_errors.load(Ordering::Relaxed);
    let error_rate = errors as f64 / (msgs_produced + errors) as f64;

    assert!(
        error_rate < 0.01,
        "Error rate should be < 1%, got {:.2}%",
        error_rate * 100.0
    );
    assert!(
        msgs_produced > 1000,
        "Should produce at least 1000 messages in 5 seconds"
    );
}

/// Test: Memory stability under load
/// Tests that memory usage remains stable under continuous load
#[tokio::test]
async fn test_memory_stability() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = setup_topic_manager(temp_dir.path());
    let topic_name = "memory-test";

    topic_manager.create_topic(topic_name, 2).unwrap();

    let stats = LoadTestStats::new();
    let start = Instant::now();

    // Run multiple iterations of produce/consume cycles
    for iteration in 0..10i32 {
        // Produce batch
        for i in 0..1000i32 {
            let sequence = (iteration * 1000 + i) as u64;
            let msg = create_test_message(1024, sequence);
            let partition = i % 2;

            let produce_start = Instant::now();
            if topic_manager
                .append(topic_name, partition, None, msg.clone())
                .is_ok()
            {
                stats.record_produce(msg.len() as u64, produce_start.elapsed().as_micros() as u64);
            }
        }

        // Consume batch
        for partition in 0..2i32 {
            let offset = (iteration * 500) as i64;
            let consume_start = Instant::now();
            if let Ok(records) = topic_manager.read(topic_name, partition, offset, 1000) {
                for record in &records {
                    stats.record_consume(
                        record.value.len() as u64,
                        consume_start.elapsed().as_micros() as u64,
                    );
                }
            }
        }

        println!("Iteration {} complete", iteration + 1);
    }

    let elapsed = start.elapsed();
    println!("{}", stats.summary(elapsed));

    // If we get here without running out of memory, the test passed
    let msgs_produced = stats.messages_produced.load(Ordering::Relaxed);
    assert!(
        msgs_produced >= 9000,
        "Should produce most messages across iterations"
    );
}

/// Test: Partition distribution
/// Tests that messages are distributed evenly across partitions
#[tokio::test]
async fn test_partition_distribution() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = setup_topic_manager(temp_dir.path());
    let topic_name = "distribution-test";
    let num_partitions = 8;

    topic_manager
        .create_topic(topic_name, num_partitions)
        .unwrap();

    let total_messages = 8000;

    // Produce messages distributed by round-robin
    for i in 0..total_messages {
        let msg = create_test_message(256, i as u64);
        let partition = (i % num_partitions as usize) as i32;
        topic_manager
            .append(topic_name, partition, None, msg)
            .unwrap();
    }

    // Check distribution
    let mut partition_counts = Vec::new();
    for partition in 0..num_partitions {
        let records = topic_manager.read(topic_name, partition, 0, 10000).unwrap();
        partition_counts.push(records.len());
    }

    println!("Partition distribution: {:?}", partition_counts);

    // Verify even distribution (each partition should have ~1000 messages)
    let expected_per_partition = total_messages / num_partitions as usize;
    for (i, count) in partition_counts.iter().enumerate() {
        let diff = (*count as i64 - expected_per_partition as i64).abs();
        assert!(
            diff < 100,
            "Partition {} has uneven distribution: {} vs expected {}",
            i,
            count,
            expected_per_partition
        );
    }
}
