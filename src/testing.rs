//! Testing utilities for Streamline integration tests
//!
//! This module provides testing utilities for writing integration tests with Streamline.
//! It supports both embedded (in-process) testing and Docker-based testing.
//!
//! # Stability
//!
//! **⚠️ Experimental** - This module is under active development.
//!
//! # Features
//!
//! - **TestStreamline**: In-process test instance with automatic cleanup
//! - **TestContainer**: Docker-based testing using testcontainers
//! - **TestProducer/TestConsumer**: High-level test helpers
//! - **Assertions**: Fluent assertion helpers for stream testing
//! - **Fixtures**: Pre-built test data fixtures
//!
//! # Example: In-Process Testing
//!
//! ```ignore
//! use streamline::testing::{TestStreamline, TestProducer, TestConsumer};
//! use bytes::Bytes;
//!
//! #[tokio::test]
//! async fn test_produce_consume() {
//!     // Create an in-memory test instance
//!     let test = TestStreamline::new().await.unwrap();
//!
//!     // Create a topic
//!     test.create_topic("test-topic", 3).await.unwrap();
//!
//!     // Produce messages
//!     let producer = test.producer("test-topic");
//!     producer.send("key1", "value1").await.unwrap();
//!     producer.send("key2", "value2").await.unwrap();
//!
//!     // Consume and assert
//!     let consumer = test.consumer("test-topic");
//!     let messages = consumer.poll(10, Duration::from_secs(5)).await.unwrap();
//!     assert_eq!(messages.len(), 2);
//! }
//! ```
//!
//! # Example: Docker Testing
//!
//! ```rust,ignore
//! use streamline::testing::{StreamlineContainer, TestClient};
//!
//! #[tokio::test]
//! async fn test_with_docker() {
//!     // Start Streamline in Docker
//!     let container = StreamlineContainer::start().await.unwrap();
//!
//!     // Get a test client
//!     let client = container.client().await;
//!
//!     // Test against the containerized instance
//!     client.create_topic("docker-topic", 1).await.unwrap();
//!     // ...
//! }
//! ```

use crate::embedded::{EmbeddedConfig, EmbeddedStreamline};
use crate::error::{Result, StreamlineError};
use crate::storage::Record;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

// ============================================================================
// Test Instance
// ============================================================================

/// Test instance of Streamline for integration testing
///
/// Creates an isolated, in-memory Streamline instance that is automatically
/// cleaned up when dropped. Ideal for unit and integration tests.
///
/// # Example
///
/// ```ignore
/// use streamline::testing::TestStreamline;
///
/// #[tokio::test]
/// async fn my_test() {
///     let test = TestStreamline::new().await.unwrap();
///     test.create_topic("my-topic", 1).await.unwrap();
///     // Test code here...
/// }
/// ```
pub struct TestStreamline {
    /// Underlying embedded instance
    instance: Arc<EmbeddedStreamline>,
    /// Test instance ID for isolation
    test_id: String,
    /// Topics created in this test (for cleanup)
    topics: Arc<RwLock<Vec<String>>>,
    /// Default partition count for new topics
    default_partitions: i32,
}

impl TestStreamline {
    /// Create a new in-memory test instance
    pub async fn new() -> Result<Self> {
        Self::with_config(TestConfig::default()).await
    }

    /// Create a test instance with custom configuration
    pub async fn with_config(config: TestConfig) -> Result<Self> {
        let embedded_config =
            EmbeddedConfig::in_memory().with_partitions(config.default_partitions);

        let instance = EmbeddedStreamline::new(embedded_config)?;
        let uuid = uuid::Uuid::new_v4().to_string();
        let test_id = format!("test-{}", &uuid[..8]);

        Ok(Self {
            instance: Arc::new(instance),
            test_id,
            topics: Arc::new(RwLock::new(Vec::new())),
            default_partitions: config.default_partitions as i32,
        })
    }

    /// Get the test instance ID
    pub fn test_id(&self) -> &str {
        &self.test_id
    }

    /// Create a topic for this test
    ///
    /// The topic name is automatically prefixed with the test ID for isolation.
    pub async fn create_topic(&self, name: &str, partitions: i32) -> Result<()> {
        let full_name = self.full_topic_name(name);
        self.instance.create_topic(&full_name, partitions)?;
        self.topics.write().await.push(full_name);
        Ok(())
    }

    /// Create a topic with default partitions
    pub async fn create_topic_default(&self, name: &str) -> Result<()> {
        self.create_topic(name, self.default_partitions).await
    }

    /// Get a producer for a topic
    pub fn producer(&self, topic: &str) -> TestProducer {
        TestProducer::new(self.instance.clone(), self.full_topic_name(topic))
    }

    /// Get a consumer for a topic
    pub fn consumer(&self, topic: &str) -> TestConsumer {
        TestConsumer::new(self.instance.clone(), self.full_topic_name(topic))
    }

    /// Get the underlying embedded instance
    pub fn embedded(&self) -> &EmbeddedStreamline {
        &self.instance
    }

    /// Generate the full topic name with test prefix
    fn full_topic_name(&self, name: &str) -> String {
        format!("{}-{}", self.test_id, name)
    }

    /// Cleanup all resources created by this test
    pub async fn cleanup(&self) -> Result<()> {
        let topics = self.topics.read().await;
        for topic in topics.iter() {
            let _ = self.instance.delete_topic(topic);
        }
        Ok(())
    }
}

impl Drop for TestStreamline {
    fn drop(&mut self) {
        // Cleanup is handled by the in-memory nature of the instance
        // Topics are automatically deleted when the instance is dropped
    }
}

/// Configuration for test instances
#[derive(Debug, Clone)]
pub struct TestConfig {
    /// Default number of partitions for new topics
    pub default_partitions: u32,
    /// Enable verbose logging for debugging
    pub verbose: bool,
    /// Timeout for operations
    pub timeout: Duration,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            default_partitions: 1,
            verbose: false,
            timeout: Duration::from_secs(30),
        }
    }
}

impl TestConfig {
    /// Create a new test configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the default number of partitions
    pub fn with_partitions(mut self, partitions: u32) -> Self {
        self.default_partitions = partitions;
        self
    }

    /// Enable verbose logging
    pub fn verbose(mut self) -> Self {
        self.verbose = true;
        self
    }

    /// Set the operation timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

// ============================================================================
// Test Producer
// ============================================================================

/// Test producer for sending messages
///
/// Provides a convenient API for producing messages in tests.
pub struct TestProducer {
    instance: Arc<EmbeddedStreamline>,
    topic: String,
    partition: Option<i32>,
    messages_sent: AtomicU64,
}

impl TestProducer {
    /// Create a new test producer
    pub fn new(instance: Arc<EmbeddedStreamline>, topic: String) -> Self {
        Self {
            instance,
            topic,
            partition: None,
            messages_sent: AtomicU64::new(0),
        }
    }

    /// Set a specific partition to send to
    pub fn to_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Send a message with key and value
    pub async fn send(&self, key: &str, value: &str) -> Result<i64> {
        let partition = self.partition.unwrap_or(0);
        let offset = self.instance.produce(
            &self.topic,
            partition,
            Some(Bytes::from(key.to_string())),
            Bytes::from(value.to_string()),
        )?;
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        Ok(offset)
    }

    /// Send a message with only a value (no key)
    pub async fn send_value(&self, value: &str) -> Result<i64> {
        let partition = self.partition.unwrap_or(0);
        let offset =
            self.instance
                .produce(&self.topic, partition, None, Bytes::from(value.to_string()))?;
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        Ok(offset)
    }

    /// Send raw bytes
    pub async fn send_bytes(&self, key: Option<Bytes>, value: Bytes) -> Result<i64> {
        let partition = self.partition.unwrap_or(0);
        let offset = self.instance.produce(&self.topic, partition, key, value)?;
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        Ok(offset)
    }

    /// Send a batch of messages
    pub async fn send_batch(&self, messages: Vec<(&str, &str)>) -> Result<Vec<i64>> {
        let mut offsets = Vec::with_capacity(messages.len());
        for (key, value) in messages {
            let offset = self.send(key, value).await?;
            offsets.push(offset);
        }
        Ok(offsets)
    }

    /// Get the number of messages sent
    pub fn messages_sent(&self) -> u64 {
        self.messages_sent.load(Ordering::Relaxed)
    }
}

// ============================================================================
// Test Consumer
// ============================================================================

/// Test consumer for receiving messages
///
/// Provides a convenient API for consuming messages in tests.
pub struct TestConsumer {
    instance: Arc<EmbeddedStreamline>,
    topic: String,
    partition: i32,
    offset: i64,
}

impl TestConsumer {
    /// Create a new test consumer
    pub fn new(instance: Arc<EmbeddedStreamline>, topic: String) -> Self {
        Self {
            instance,
            topic,
            partition: 0,
            offset: 0,
        }
    }

    /// Set the partition to consume from
    pub fn from_partition(mut self, partition: i32) -> Self {
        self.partition = partition;
        self
    }

    /// Set the starting offset
    pub fn from_offset(mut self, offset: i64) -> Self {
        self.offset = offset;
        self
    }

    /// Start from the beginning
    pub fn from_beginning(mut self) -> Self {
        self.offset = 0;
        self
    }

    /// Poll for messages
    pub async fn poll(
        &mut self,
        max_records: usize,
        _timeout: Duration,
    ) -> Result<Vec<TestRecord>> {
        let records =
            self.instance
                .consume(&self.topic, self.partition, self.offset, max_records)?;

        if let Some(last) = records.last() {
            self.offset = last.offset + 1;
        }

        Ok(records.into_iter().map(TestRecord::from).collect())
    }

    /// Poll for a specific number of messages
    ///
    /// Waits until the expected number of messages is received or timeout.
    pub async fn poll_exact(&mut self, count: usize, timeout: Duration) -> Result<Vec<TestRecord>> {
        let start = std::time::Instant::now();
        let mut all_records = Vec::new();

        while all_records.len() < count && start.elapsed() < timeout {
            let remaining = count - all_records.len();
            let records = self.poll(remaining, Duration::from_millis(100)).await?;
            all_records.extend(records);

            if all_records.len() < count {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        if all_records.len() < count {
            return Err(StreamlineError::Internal(format!(
                "Timeout waiting for {} messages, got {}",
                count,
                all_records.len()
            )));
        }

        Ok(all_records)
    }

    /// Drain all available messages
    pub async fn drain(&mut self) -> Result<Vec<TestRecord>> {
        self.poll(10000, Duration::from_millis(100)).await
    }

    /// Assert that the topic is empty
    pub async fn assert_empty(&mut self) -> Result<()> {
        let records = self.poll(1, Duration::from_millis(100)).await?;
        if !records.is_empty() {
            return Err(StreamlineError::Internal(format!(
                "Expected empty topic, but found {} records",
                records.len()
            )));
        }
        Ok(())
    }
}

// ============================================================================
// Test Record
// ============================================================================

/// A consumed record with convenient accessors for testing
#[derive(Debug, Clone)]
pub struct TestRecord {
    /// Record offset
    pub offset: i64,
    /// Record timestamp
    pub timestamp: i64,
    /// Record key (if present)
    pub key: Option<String>,
    /// Record value
    pub value: String,
    /// Record headers
    pub headers: HashMap<String, String>,
}

impl TestRecord {
    /// Get the key as a string, panics if no key
    pub fn key_str(&self) -> &str {
        self.key.as_ref().expect("Expected record to have a key")
    }

    /// Get the value as a string
    pub fn value_str(&self) -> &str {
        &self.value
    }

    /// Check if a header exists
    pub fn has_header(&self, key: &str) -> bool {
        self.headers.contains_key(key)
    }

    /// Get a header value
    pub fn header(&self, key: &str) -> Option<&String> {
        self.headers.get(key)
    }

    /// Parse the value as JSON
    pub fn value_json<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_str(&self.value)
            .map_err(|e| StreamlineError::Internal(format!("Failed to parse JSON: {}", e)))
    }
}

impl From<Record> for TestRecord {
    fn from(record: Record) -> Self {
        let key = record.key.map(|k| String::from_utf8_lossy(&k).to_string());
        let value = String::from_utf8_lossy(&record.value).to_string();
        let headers: HashMap<String, String> = record
            .headers
            .into_iter()
            .map(|h| (h.key, String::from_utf8_lossy(&h.value).to_string()))
            .collect();

        Self {
            offset: record.offset,
            timestamp: record.timestamp,
            key,
            value,
            headers,
        }
    }
}

// ============================================================================
// Assertion Helpers
// ============================================================================

/// Assertion helpers for test records
pub trait TestAssertions {
    /// Assert that the collection has the expected number of items
    fn assert_count(&self, expected: usize) -> &Self;

    /// Assert that records contain expected values
    fn assert_values_contain(&self, expected: &[&str]) -> &Self;
}

impl TestAssertions for Vec<TestRecord> {
    fn assert_count(&self, expected: usize) -> &Self {
        assert_eq!(
            self.len(),
            expected,
            "Expected {} records, got {}",
            expected,
            self.len()
        );
        self
    }

    fn assert_values_contain(&self, expected: &[&str]) -> &Self {
        for exp in expected {
            let found = self.iter().any(|r| r.value.contains(exp));
            assert!(found, "Expected to find record containing '{}'", exp);
        }
        self
    }
}

// ============================================================================
// Test Fixtures
// ============================================================================

/// Pre-built test data fixtures
pub struct TestFixtures;

impl TestFixtures {
    /// Generate a batch of test messages
    pub fn messages(count: usize) -> Vec<(String, String)> {
        (0..count)
            .map(|i| (format!("key-{}", i), format!("value-{}", i)))
            .collect()
    }

    /// Generate JSON messages
    pub fn json_messages(count: usize) -> Vec<(String, String)> {
        (0..count)
            .map(|i| {
                let json = serde_json::json!({
                    "id": i,
                    "name": format!("item-{}", i),
                    "timestamp": chrono::Utc::now().timestamp_millis()
                });
                (format!("key-{}", i), json.to_string())
            })
            .collect()
    }

    /// Generate large messages for throughput testing
    pub fn large_messages(count: usize, size_bytes: usize) -> Vec<(String, String)> {
        let value: String = "x".repeat(size_bytes);
        (0..count)
            .map(|i| (format!("key-{}", i), value.clone()))
            .collect()
    }
}

// ============================================================================
// Docker Container Support (Testcontainers)
// ============================================================================

/// Docker container configuration for Streamline
///
/// Used with the testcontainers crate for Docker-based integration testing.
///
/// # Example
///
/// ```rust,ignore
/// use streamline::testing::StreamlineContainer;
/// use testcontainers::clients::Cli;
///
/// #[tokio::test]
/// async fn test_with_docker() {
///     let docker = Cli::default();
///     let container = docker.run(StreamlineContainer::default());
///
///     let kafka_port = container.get_host_port_ipv4(9092);
///     let http_port = container.get_host_port_ipv4(9094);
///
///     // Connect using standard Kafka clients
///     // ...
/// }
/// ```
#[derive(Debug, Clone)]
pub struct StreamlineContainerConfig {
    /// Docker image name
    pub image: String,
    /// Docker image tag
    pub tag: String,
    /// Environment variables
    pub env_vars: HashMap<String, String>,
    /// Exposed ports
    pub exposed_ports: Vec<u16>,
}

impl Default for StreamlineContainerConfig {
    fn default() -> Self {
        let mut env_vars = HashMap::new();
        env_vars.insert("STREAMLINE_LOG_LEVEL".to_string(), "info".to_string());

        Self {
            image: "streamline".to_string(),
            tag: "latest".to_string(),
            env_vars,
            exposed_ports: vec![9092, 9094],
        }
    }
}

impl StreamlineContainerConfig {
    /// Create a new container configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the Docker image
    pub fn with_image(mut self, image: &str) -> Self {
        self.image = image.to_string();
        self
    }

    /// Set the Docker image tag
    pub fn with_tag(mut self, tag: &str) -> Self {
        self.tag = tag.to_string();
        self
    }

    /// Add an environment variable
    pub fn with_env(mut self, key: &str, value: &str) -> Self {
        self.env_vars.insert(key.to_string(), value.to_string());
        self
    }

    /// Enable in-memory mode (no persistence)
    pub fn in_memory(mut self) -> Self {
        self.env_vars
            .insert("STREAMLINE_IN_MEMORY".to_string(), "true".to_string());
        self
    }

    /// Enable playground mode with demo topics
    pub fn playground(mut self) -> Self {
        self.env_vars
            .insert("STREAMLINE_PLAYGROUND".to_string(), "true".to_string());
        self
    }

    /// Set the log level
    pub fn with_log_level(mut self, level: &str) -> Self {
        self.env_vars
            .insert("STREAMLINE_LOG_LEVEL".to_string(), level.to_string());
        self
    }
}

// ============================================================================
// Wait Strategies
// ============================================================================

/// Wait strategy for container readiness
#[derive(Debug, Clone)]
pub enum WaitStrategy {
    /// Wait for HTTP health check to pass
    HttpHealthCheck {
        /// Port to check
        port: u16,
        /// Path to check
        path: String,
        /// Timeout
        timeout: Duration,
    },
    /// Wait for TCP port to be open
    TcpPort {
        /// Port to check
        port: u16,
        /// Timeout
        timeout: Duration,
    },
    /// Wait for log message
    LogMessage {
        /// Message to wait for
        message: String,
        /// Timeout
        timeout: Duration,
    },
}

impl Default for WaitStrategy {
    fn default() -> Self {
        Self::HttpHealthCheck {
            port: 9094,
            path: "/health".to_string(),
            timeout: Duration::from_secs(30),
        }
    }
}

// ============================================================================
// Test Utilities
// ============================================================================

/// Utility functions for testing
pub struct TestUtils;

impl TestUtils {
    /// Generate a unique topic name
    pub fn unique_topic_name(prefix: &str) -> String {
        let uuid = uuid::Uuid::new_v4().to_string();
        format!("{}-{}", prefix, &uuid[..8])
    }

    /// Wait for a condition with timeout
    pub async fn wait_for<F, Fut>(condition: F, timeout: Duration) -> Result<()>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if condition().await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(StreamlineError::Internal(
            "Timeout waiting for condition".to_string(),
        ))
    }

    /// Retry an operation with exponential backoff
    pub async fn retry<F, Fut, T>(
        operation: F,
        max_attempts: u32,
        initial_delay: Duration,
    ) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut delay = initial_delay;
        let mut last_error = None;

        for attempt in 0..max_attempts {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < max_attempts - 1 {
                        tokio::time::sleep(delay).await;
                        delay *= 2;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            StreamlineError::Internal("Retry exhausted with no error".to_string())
        }))
    }
}

// ============================================================================
// Test Scenario Builder
// ============================================================================

/// Builder for constructing complex test scenarios
///
/// Provides a fluent API for setting up topics, producing messages,
/// and validating consumption patterns in a declarative style.
///
/// # Example
///
/// ```rust,ignore
/// use streamline::testing::TestScenarioBuilder;
///
/// let results = TestScenarioBuilder::new()
///     .with_topic("orders", 3)
///     .with_topic("events", 1)
///     .produce("orders", vec![("key1", "order-1"), ("key2", "order-2")])
///     .produce("events", vec![("e1", "event-1")])
///     .build_and_run()
///     .await
///     .unwrap();
///
/// assert_eq!(results.messages_produced("orders"), 2);
/// assert_eq!(results.messages_produced("events"), 1);
/// ```
pub struct TestScenarioBuilder {
    topics: Vec<(String, i32)>,
    messages: Vec<(String, Vec<(String, String)>)>,
    config: TestConfig,
}

impl TestScenarioBuilder {
    /// Create a new scenario builder
    pub fn new() -> Self {
        Self {
            topics: Vec::new(),
            messages: Vec::new(),
            config: TestConfig::default(),
        }
    }

    /// Add a topic to the scenario
    pub fn with_topic(mut self, name: &str, partitions: i32) -> Self {
        self.topics.push((name.to_string(), partitions));
        self
    }

    /// Add messages to produce to a topic
    pub fn produce(mut self, topic: &str, messages: Vec<(&str, &str)>) -> Self {
        let owned: Vec<(String, String)> = messages
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        self.messages.push((topic.to_string(), owned));
        self
    }

    /// Set custom test configuration
    pub fn with_config(mut self, config: TestConfig) -> Self {
        self.config = config;
        self
    }

    /// Build and execute the scenario
    pub async fn build_and_run(self) -> Result<ScenarioResult> {
        let instance = TestStreamline::with_config(self.config).await?;
        let mut produced: HashMap<String, u64> = HashMap::new();

        // Create topics
        for (name, partitions) in &self.topics {
            instance.create_topic(name, *partitions).await?;
        }

        // Produce messages
        for (topic, messages) in &self.messages {
            let producer = instance.producer(topic);
            for (key, value) in messages {
                producer.send(key, value).await?;
            }
            *produced.entry(topic.clone()).or_insert(0) += messages.len() as u64;
        }

        Ok(ScenarioResult {
            instance,
            produced_counts: produced,
        })
    }
}

impl Default for TestScenarioBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of executing a test scenario
pub struct ScenarioResult {
    /// The test instance (still running for further assertions)
    pub instance: TestStreamline,
    produced_counts: HashMap<String, u64>,
}

impl ScenarioResult {
    /// Get the number of messages produced to a topic
    pub fn messages_produced(&self, topic: &str) -> u64 {
        self.produced_counts.get(topic).copied().unwrap_or(0)
    }

    /// Get a consumer for further assertions
    pub fn consumer(&self, topic: &str) -> TestConsumer {
        self.instance.consumer(topic)
    }
}

// ============================================================================
// Data Generators
// ============================================================================

/// Configurable test data generators for producing realistic test messages
///
/// Supports generating various data patterns: sequential, random, time-series,
/// and custom schemas.
pub struct DataGenerator;

impl DataGenerator {
    /// Generate sequential integer messages
    pub fn sequential(count: usize) -> Vec<(String, String)> {
        (0..count)
            .map(|i| (format!("seq-{}", i), format!("{}", i)))
            .collect()
    }

    /// Generate user event messages (JSON)
    pub fn user_events(count: usize) -> Vec<(String, String)> {
        let event_types = ["click", "view", "purchase", "signup", "logout"];
        (0..count)
            .map(|i| {
                let event_type = event_types[i % event_types.len()];
                let json = serde_json::json!({
                    "user_id": format!("user-{}", i % 100),
                    "event_type": event_type,
                    "timestamp": chrono::Utc::now().timestamp_millis() + i as i64,
                    "properties": {
                        "page": format!("/page-{}", i % 10),
                        "session_id": format!("sess-{}", i % 50)
                    }
                });
                (format!("user-{}", i % 100), json.to_string())
            })
            .collect()
    }

    /// Generate IoT sensor messages (JSON)
    pub fn sensor_readings(count: usize) -> Vec<(String, String)> {
        (0..count)
            .map(|i| {
                let json = serde_json::json!({
                    "sensor_id": format!("sensor-{}", i % 20),
                    "temperature": 20.0 + (i as f64 * 0.1) % 15.0,
                    "humidity": 40.0 + (i as f64 * 0.3) % 40.0,
                    "pressure": 1013.0 + (i as f64 * 0.05) % 10.0,
                    "timestamp": chrono::Utc::now().timestamp_millis() + i as i64 * 1000
                });
                (format!("sensor-{}", i % 20), json.to_string())
            })
            .collect()
    }

    /// Generate log-like messages with varied severity
    pub fn log_entries(count: usize) -> Vec<(String, String)> {
        let levels = ["DEBUG", "INFO", "WARN", "ERROR"];
        let services = ["api", "auth", "db", "cache", "worker"];
        (0..count)
            .map(|i| {
                let level = levels[i % levels.len()];
                let service = services[i % services.len()];
                let json = serde_json::json!({
                    "level": level,
                    "service": service,
                    "message": format!("Log message #{}", i),
                    "timestamp": chrono::Utc::now().timestamp_millis() + i as i64,
                    "trace_id": format!("trace-{:08x}", i)
                });
                (format!("{}-{}", service, i), json.to_string())
            })
            .collect()
    }

    /// Generate messages of exact byte size for throughput testing
    pub fn fixed_size(count: usize, value_size: usize) -> Vec<(String, String)> {
        let value: String = "A".repeat(value_size);
        (0..count)
            .map(|i| (format!("key-{}", i), value.clone()))
            .collect()
    }
}

// ============================================================================
// Stream Matchers - Fluent assertion extensions
// ============================================================================

/// Extended assertion trait for more expressive test assertions
pub trait StreamMatchers {
    /// Assert records are in order by offset
    fn assert_ordered_by_offset(&self) -> &Self;

    /// Assert all records have keys matching a prefix
    fn assert_keys_start_with(&self, prefix: &str) -> &Self;

    /// Assert no duplicate keys exist
    fn assert_no_duplicate_keys(&self) -> &Self;

    /// Assert all values are valid JSON
    fn assert_all_valid_json(&self) -> &Self;

    /// Assert all records have timestamps within a range
    fn assert_timestamps_within(&self, duration: Duration) -> &Self;

    /// Filter records by key prefix and return matching records
    fn filter_by_key_prefix(&self, prefix: &str) -> Vec<&TestRecord>;

    /// Filter records by value containing substring
    fn filter_by_value_contains(&self, substring: &str) -> Vec<&TestRecord>;

    /// Extract all values as strings
    fn values(&self) -> Vec<&str>;

    /// Extract all keys as strings (skipping None)
    fn keys(&self) -> Vec<&str>;
}

impl StreamMatchers for Vec<TestRecord> {
    fn assert_ordered_by_offset(&self) -> &Self {
        for window in self.windows(2) {
            assert!(
                window[0].offset < window[1].offset,
                "Records not ordered by offset: {} >= {}",
                window[0].offset,
                window[1].offset
            );
        }
        self
    }

    fn assert_keys_start_with(&self, prefix: &str) -> &Self {
        for record in self {
            if let Some(ref key) = record.key {
                assert!(
                    key.starts_with(prefix),
                    "Key '{}' does not start with '{}'",
                    key,
                    prefix
                );
            }
        }
        self
    }

    fn assert_no_duplicate_keys(&self) -> &Self {
        let mut seen = std::collections::HashSet::new();
        for record in self {
            if let Some(ref key) = record.key {
                assert!(seen.insert(key.clone()), "Duplicate key found: '{}'", key);
            }
        }
        self
    }

    fn assert_all_valid_json(&self) -> &Self {
        for record in self {
            let result: std::result::Result<serde_json::Value, _> =
                serde_json::from_str(&record.value);
            assert!(
                result.is_ok(),
                "Record at offset {} is not valid JSON: {}",
                record.offset,
                record.value
            );
        }
        self
    }

    fn assert_timestamps_within(&self, duration: Duration) -> &Self {
        if self.is_empty() {
            return self;
        }
        let first_ts = self[0].timestamp;
        let duration_ms = duration.as_millis() as i64;
        for record in self {
            let diff = (record.timestamp - first_ts).abs();
            assert!(
                diff <= duration_ms,
                "Timestamp at offset {} is {}ms from first, exceeds {}ms",
                record.offset,
                diff,
                duration_ms
            );
        }
        self
    }

    fn filter_by_key_prefix(&self, prefix: &str) -> Vec<&TestRecord> {
        self.iter()
            .filter(|r| r.key.as_ref().is_some_and(|k| k.starts_with(prefix)))
            .collect()
    }

    fn filter_by_value_contains(&self, substring: &str) -> Vec<&TestRecord> {
        self.iter()
            .filter(|r| r.value.contains(substring))
            .collect()
    }

    fn values(&self) -> Vec<&str> {
        self.iter().map(|r| r.value.as_str()).collect()
    }

    fn keys(&self) -> Vec<&str> {
        self.iter().filter_map(|r| r.key.as_deref()).collect()
    }
}

// ============================================================================
// CI/CD Integration Helpers
// ============================================================================

/// Helpers for CI/CD pipeline integration
pub struct CiCdHelpers;

impl CiCdHelpers {
    /// Check if running inside a CI environment
    pub fn is_ci() -> bool {
        std::env::var("CI").is_ok()
            || std::env::var("GITHUB_ACTIONS").is_ok()
            || std::env::var("GITLAB_CI").is_ok()
            || std::env::var("JENKINS_URL").is_ok()
            || std::env::var("CIRCLECI").is_ok()
            || std::env::var("TRAVIS").is_ok()
    }

    /// Get appropriate timeout based on environment
    pub fn default_timeout() -> Duration {
        if Self::is_ci() {
            Duration::from_secs(60)
        } else {
            Duration::from_secs(30)
        }
    }

    /// Get appropriate config for CI environments
    pub fn ci_config() -> TestConfig {
        TestConfig {
            default_partitions: 1,
            verbose: Self::is_ci(),
            timeout: Self::default_timeout(),
        }
    }
}

// ============================================================================
// Test Harness - Multi-topic test setup
// ============================================================================

/// A test harness that manages multiple topics and provides
/// coordinated produce/consume operations
///
/// # Example
///
/// ```rust,ignore
/// let harness = TestHarness::new()
///     .await
///     .unwrap()
///     .with_topics(&["input", "output", "errors"])
///     .await
///     .unwrap();
///
/// harness.produce("input", "key", "value").await.unwrap();
/// let records = harness.consume_all("input").await.unwrap();
/// assert_eq!(records.len(), 1);
/// ```
pub struct TestHarness {
    instance: TestStreamline,
    topic_names: Vec<String>,
}

impl TestHarness {
    /// Create a new test harness
    pub async fn new() -> Result<Self> {
        let instance = TestStreamline::new().await?;
        Ok(Self {
            instance,
            topic_names: Vec::new(),
        })
    }

    /// Create multiple topics at once
    pub async fn with_topics(mut self, topics: &[&str]) -> Result<Self> {
        for topic in topics {
            self.instance.create_topic(topic, 1).await?;
            self.topic_names.push(topic.to_string());
        }
        Ok(self)
    }

    /// Produce a single message to a topic
    pub async fn produce(&self, topic: &str, key: &str, value: &str) -> Result<i64> {
        self.instance.producer(topic).send(key, value).await
    }

    /// Produce multiple messages to a topic
    pub async fn produce_batch(
        &self,
        topic: &str,
        messages: Vec<(&str, &str)>,
    ) -> Result<Vec<i64>> {
        self.instance.producer(topic).send_batch(messages).await
    }

    /// Consume all available messages from a topic
    pub async fn consume_all(&self, topic: &str) -> Result<Vec<TestRecord>> {
        let mut consumer = self.instance.consumer(topic);
        consumer.drain().await
    }

    /// Get the underlying test instance
    pub fn instance(&self) -> &TestStreamline {
        &self.instance
    }

    /// Get list of created topics
    pub fn topics(&self) -> &[String] {
        &self.topic_names
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_test_streamline_basic() {
        let test = TestStreamline::new().await.unwrap();
        test.create_topic("basic-test", 1).await.unwrap();

        let producer = test.producer("basic-test");
        producer.send("key1", "value1").await.unwrap();
        producer.send("key2", "value2").await.unwrap();

        let mut consumer = test.consumer("basic-test");
        let records = consumer.poll(10, Duration::from_secs(1)).await.unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key.as_ref().unwrap(), "key1");
        assert_eq!(records[0].value, "value1");
    }

    #[tokio::test]
    async fn test_test_producer_batch() {
        let test = TestStreamline::new().await.unwrap();
        test.create_topic("batch-test", 1).await.unwrap();

        let producer = test.producer("batch-test");
        let offsets = producer
            .send_batch(vec![("k1", "v1"), ("k2", "v2"), ("k3", "v3")])
            .await
            .unwrap();

        assert_eq!(offsets.len(), 3);
        assert_eq!(producer.messages_sent(), 3);
    }

    #[tokio::test]
    async fn test_test_consumer_poll_exact() {
        let test = TestStreamline::new().await.unwrap();
        test.create_topic("poll-exact-test", 1).await.unwrap();

        let producer = test.producer("poll-exact-test");
        for i in 0..5 {
            producer
                .send(&format!("k{}", i), &format!("v{}", i))
                .await
                .unwrap();
        }

        let mut consumer = test.consumer("poll-exact-test");
        let records = consumer
            .poll_exact(5, Duration::from_secs(5))
            .await
            .unwrap();

        assert_eq!(records.len(), 5);
    }

    #[test]
    fn test_fixtures() {
        let messages = TestFixtures::messages(10);
        assert_eq!(messages.len(), 10);
        assert_eq!(messages[0].0, "key-0");
        assert_eq!(messages[0].1, "value-0");

        let json_messages = TestFixtures::json_messages(5);
        assert_eq!(json_messages.len(), 5);

        let large_messages = TestFixtures::large_messages(3, 1024);
        assert_eq!(large_messages.len(), 3);
        assert_eq!(large_messages[0].1.len(), 1024);
    }

    #[test]
    fn test_test_record_from_record() {
        let record = Record {
            offset: 42,
            timestamp: 1234567890,
            key: Some(Bytes::from("test-key")),
            value: Bytes::from("test-value"),
            headers: vec![],
            crc: None,
        };

        let test_record = TestRecord::from(record);
        assert_eq!(test_record.offset, 42);
        assert_eq!(test_record.key_str(), "test-key");
        assert_eq!(test_record.value_str(), "test-value");
    }

    #[test]
    fn test_assertions() {
        let records = vec![
            TestRecord {
                offset: 0,
                timestamp: 0,
                key: Some("k1".to_string()),
                value: "hello world".to_string(),
                headers: HashMap::new(),
            },
            TestRecord {
                offset: 1,
                timestamp: 0,
                key: Some("k2".to_string()),
                value: "foo bar".to_string(),
                headers: HashMap::new(),
            },
        ];

        records.assert_count(2);
        records.assert_values_contain(&["hello", "foo"]);
    }

    #[test]
    fn test_container_config() {
        let config = StreamlineContainerConfig::new()
            .with_image("my-streamline")
            .with_tag("v1.0")
            .in_memory()
            .with_log_level("debug");

        assert_eq!(config.image, "my-streamline");
        assert_eq!(config.tag, "v1.0");
        assert_eq!(
            config.env_vars.get("STREAMLINE_IN_MEMORY"),
            Some(&"true".to_string())
        );
        assert_eq!(
            config.env_vars.get("STREAMLINE_LOG_LEVEL"),
            Some(&"debug".to_string())
        );
    }

    #[test]
    fn test_unique_topic_name() {
        let name1 = TestUtils::unique_topic_name("test");
        let name2 = TestUtils::unique_topic_name("test");

        assert!(name1.starts_with("test-"));
        assert!(name2.starts_with("test-"));
        assert_ne!(name1, name2);
    }

    // ========================================================================
    // Tests for new Dev SDK features
    // ========================================================================

    #[tokio::test]
    async fn test_scenario_builder_basic() {
        let result = TestScenarioBuilder::new()
            .with_topic("scenario-topic", 1)
            .produce("scenario-topic", vec![("k1", "v1"), ("k2", "v2")])
            .build_and_run()
            .await
            .unwrap();

        assert_eq!(result.messages_produced("scenario-topic"), 2);
        assert_eq!(result.messages_produced("nonexistent"), 0);
    }

    #[tokio::test]
    async fn test_scenario_builder_multi_topic() {
        let result = TestScenarioBuilder::new()
            .with_topic("orders", 1)
            .with_topic("events", 1)
            .produce("orders", vec![("o1", "order-1")])
            .produce("events", vec![("e1", "event-1"), ("e2", "event-2")])
            .build_and_run()
            .await
            .unwrap();

        assert_eq!(result.messages_produced("orders"), 1);
        assert_eq!(result.messages_produced("events"), 2);
    }

    #[tokio::test]
    async fn test_scenario_builder_consume() {
        let result = TestScenarioBuilder::new()
            .with_topic("consume-test", 1)
            .produce("consume-test", vec![("k1", "v1"), ("k2", "v2")])
            .build_and_run()
            .await
            .unwrap();

        let mut consumer = result.consumer("consume-test");
        let records = consumer.drain().await.unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_data_generator_sequential() {
        let data = DataGenerator::sequential(5);
        assert_eq!(data.len(), 5);
        assert_eq!(data[0].0, "seq-0");
        assert_eq!(data[0].1, "0");
        assert_eq!(data[4].0, "seq-4");
        assert_eq!(data[4].1, "4");
    }

    #[test]
    fn test_data_generator_user_events() {
        let events = DataGenerator::user_events(10);
        assert_eq!(events.len(), 10);

        // Verify JSON structure
        for (_, value) in &events {
            let parsed: serde_json::Value = serde_json::from_str(value).unwrap();
            assert!(parsed.get("user_id").is_some());
            assert!(parsed.get("event_type").is_some());
            assert!(parsed.get("timestamp").is_some());
        }
    }

    #[test]
    fn test_data_generator_sensor_readings() {
        let readings = DataGenerator::sensor_readings(5);
        assert_eq!(readings.len(), 5);

        for (_, value) in &readings {
            let parsed: serde_json::Value = serde_json::from_str(value).unwrap();
            assert!(parsed.get("sensor_id").is_some());
            assert!(parsed.get("temperature").is_some());
            assert!(parsed.get("humidity").is_some());
        }
    }

    #[test]
    fn test_data_generator_log_entries() {
        let logs = DataGenerator::log_entries(8);
        assert_eq!(logs.len(), 8);

        for (_, value) in &logs {
            let parsed: serde_json::Value = serde_json::from_str(value).unwrap();
            assert!(parsed.get("level").is_some());
            assert!(parsed.get("service").is_some());
        }
    }

    #[test]
    fn test_data_generator_fixed_size() {
        let data = DataGenerator::fixed_size(3, 512);
        assert_eq!(data.len(), 3);
        assert_eq!(data[0].1.len(), 512);
    }

    #[test]
    fn test_stream_matchers_ordered_by_offset() {
        let records = vec![
            TestRecord {
                offset: 0,
                timestamp: 100,
                key: Some("k0".to_string()),
                value: "v0".to_string(),
                headers: HashMap::new(),
            },
            TestRecord {
                offset: 1,
                timestamp: 200,
                key: Some("k1".to_string()),
                value: "v1".to_string(),
                headers: HashMap::new(),
            },
            TestRecord {
                offset: 2,
                timestamp: 300,
                key: Some("k2".to_string()),
                value: "v2".to_string(),
                headers: HashMap::new(),
            },
        ];

        records.assert_ordered_by_offset();
    }

    #[test]
    fn test_stream_matchers_keys_start_with() {
        let records = vec![
            TestRecord {
                offset: 0,
                timestamp: 0,
                key: Some("user-1".to_string()),
                value: "v".to_string(),
                headers: HashMap::new(),
            },
            TestRecord {
                offset: 1,
                timestamp: 0,
                key: Some("user-2".to_string()),
                value: "v".to_string(),
                headers: HashMap::new(),
            },
        ];

        records.assert_keys_start_with("user-");
    }

    #[test]
    fn test_stream_matchers_no_duplicate_keys() {
        let records = vec![
            TestRecord {
                offset: 0,
                timestamp: 0,
                key: Some("unique-1".to_string()),
                value: "v1".to_string(),
                headers: HashMap::new(),
            },
            TestRecord {
                offset: 1,
                timestamp: 0,
                key: Some("unique-2".to_string()),
                value: "v2".to_string(),
                headers: HashMap::new(),
            },
        ];

        records.assert_no_duplicate_keys();
    }

    #[test]
    fn test_stream_matchers_valid_json() {
        let records = vec![
            TestRecord {
                offset: 0,
                timestamp: 0,
                key: None,
                value: r#"{"name":"test"}"#.to_string(),
                headers: HashMap::new(),
            },
            TestRecord {
                offset: 1,
                timestamp: 0,
                key: None,
                value: r#"{"id":42}"#.to_string(),
                headers: HashMap::new(),
            },
        ];

        records.assert_all_valid_json();
    }

    #[test]
    fn test_stream_matchers_filter_by_key_prefix() {
        let records = vec![
            TestRecord {
                offset: 0,
                timestamp: 0,
                key: Some("user-1".to_string()),
                value: "v1".to_string(),
                headers: HashMap::new(),
            },
            TestRecord {
                offset: 1,
                timestamp: 0,
                key: Some("order-1".to_string()),
                value: "v2".to_string(),
                headers: HashMap::new(),
            },
            TestRecord {
                offset: 2,
                timestamp: 0,
                key: Some("user-2".to_string()),
                value: "v3".to_string(),
                headers: HashMap::new(),
            },
        ];

        let users = records.filter_by_key_prefix("user-");
        assert_eq!(users.len(), 2);

        let orders = records.filter_by_key_prefix("order-");
        assert_eq!(orders.len(), 1);
    }

    #[test]
    fn test_stream_matchers_filter_by_value() {
        let records = vec![
            TestRecord {
                offset: 0,
                timestamp: 0,
                key: None,
                value: "error: something failed".to_string(),
                headers: HashMap::new(),
            },
            TestRecord {
                offset: 1,
                timestamp: 0,
                key: None,
                value: "info: all good".to_string(),
                headers: HashMap::new(),
            },
        ];

        let errors = records.filter_by_value_contains("error");
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn test_stream_matchers_values_and_keys() {
        let records = vec![
            TestRecord {
                offset: 0,
                timestamp: 0,
                key: Some("k1".to_string()),
                value: "v1".to_string(),
                headers: HashMap::new(),
            },
            TestRecord {
                offset: 1,
                timestamp: 0,
                key: Some("k2".to_string()),
                value: "v2".to_string(),
                headers: HashMap::new(),
            },
        ];

        assert_eq!(records.values(), vec!["v1", "v2"]);
        assert_eq!(records.keys(), vec!["k1", "k2"]);
    }

    #[test]
    fn test_ci_cd_helpers() {
        // Just verify the helpers don't panic
        let _is_ci = CiCdHelpers::is_ci();
        let timeout = CiCdHelpers::default_timeout();
        assert!(timeout.as_secs() > 0);

        let config = CiCdHelpers::ci_config();
        assert_eq!(config.default_partitions, 1);
    }

    #[tokio::test]
    async fn test_harness_basic() {
        let harness = TestHarness::new()
            .await
            .unwrap()
            .with_topics(&["input", "output"])
            .await
            .unwrap();

        assert_eq!(harness.topics().len(), 2);

        harness.produce("input", "k1", "v1").await.unwrap();
        let records = harness.consume_all("input").await.unwrap();
        assert_eq!(records.len(), 1);
    }

    #[tokio::test]
    async fn test_harness_batch_produce() {
        let harness = TestHarness::new()
            .await
            .unwrap()
            .with_topics(&["batch-harness"])
            .await
            .unwrap();

        let offsets = harness
            .produce_batch(
                "batch-harness",
                vec![("k1", "v1"), ("k2", "v2"), ("k3", "v3")],
            )
            .await
            .unwrap();

        assert_eq!(offsets.len(), 3);

        let records = harness.consume_all("batch-harness").await.unwrap();
        assert_eq!(records.len(), 3);
    }
}
