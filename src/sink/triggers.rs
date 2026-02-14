//! Event Source Mapping and Function Triggers
//!
//! This module provides an AWS Lambda-style event source mapping system for
//! triggering serverless functions from Streamline topics.
//!
//! # Overview
//!
//! Event source mappings connect Streamline topics to serverless functions,
//! automatically invoking the function when new records are produced.
//!
//! # Example
//!
//! ```rust,ignore
//! use streamline::sink::triggers::{EventSourceMapping, TriggerConfig};
//!
//! let mapping = EventSourceMapping::builder()
//!     .name("my-trigger")
//!     .topic("events")
//!     .function_arn("arn:aws:lambda:us-east-1:123456789:function:process-events")
//!     .batch_size(100)
//!     .build()?;
//! ```

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Event source mapping state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MappingState {
    /// Mapping is being created
    Creating,
    /// Mapping is active and processing
    Enabled,
    /// Mapping is temporarily disabled
    Disabled,
    /// Mapping is being updated
    Updating,
    /// Mapping is being deleted
    Deleting,
}

/// Supported function providers
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FunctionProvider {
    /// AWS Lambda
    AwsLambda,
    /// Google Cloud Functions
    GoogleCloudFunctions,
    /// Azure Functions
    AzureFunctions,
    /// Cloudflare Workers
    CloudflareWorkers,
    /// Generic HTTP webhook
    HttpWebhook,
}

/// Starting position for consuming from the topic
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StartingPosition {
    /// Start from the beginning of the topic
    TrimHorizon,
    /// Start from the end of the topic (new records only)
    #[default]
    Latest,
    /// Start at a specific timestamp
    AtTimestamp,
}

/// Failure handling behavior
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnFailure {
    /// Send failed events to a dead-letter queue
    #[default]
    DeadLetterQueue,
    /// Discard failed events
    Discard,
    /// Retry failed events (with backoff)
    Retry,
}

/// Configuration for an event source mapping
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerConfig {
    /// Unique name for this mapping
    pub name: String,

    /// Source topic to consume from
    pub topic: String,

    /// Specific partitions to consume (empty = all partitions)
    #[serde(default)]
    pub partitions: Vec<i32>,

    /// Function provider
    pub provider: FunctionProvider,

    /// Function identifier (ARN, URL, or name depending on provider)
    pub function_arn: String,

    /// Maximum number of records per invocation
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Maximum batching window in milliseconds
    #[serde(default = "default_batch_window_ms")]
    pub batch_window_ms: u64,

    /// Starting position for new mappings
    #[serde(default)]
    pub starting_position: StartingPosition,

    /// Starting timestamp (used when starting_position = AtTimestamp)
    #[serde(default)]
    pub starting_position_timestamp: Option<i64>,

    /// Maximum retries for failed invocations
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Parallelization factor (concurrent batches per shard)
    #[serde(default = "default_parallelization_factor")]
    pub parallelization_factor: u32,

    /// Failure handling behavior
    #[serde(default)]
    pub on_failure: OnFailure,

    /// Dead-letter queue topic (if on_failure = DeadLetterQueue)
    #[serde(default)]
    pub dlq_topic: Option<String>,

    /// Filter pattern (JSONPath expression to filter records)
    #[serde(default)]
    pub filter_pattern: Option<String>,

    /// Provider-specific configuration
    #[serde(default)]
    pub provider_config: HashMap<String, String>,

    /// Whether the mapping is enabled
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_batch_size() -> usize {
    100
}
fn default_batch_window_ms() -> u64 {
    500
}
fn default_max_retries() -> u32 {
    2
}
fn default_parallelization_factor() -> u32 {
    1
}
fn default_enabled() -> bool {
    true
}

impl TriggerConfig {
    /// Create a builder for TriggerConfig
    pub fn builder() -> TriggerConfigBuilder {
        TriggerConfigBuilder::new()
    }
}

/// Builder for TriggerConfig
#[derive(Debug, Default)]
pub struct TriggerConfigBuilder {
    name: Option<String>,
    topic: Option<String>,
    partitions: Vec<i32>,
    provider: Option<FunctionProvider>,
    function_arn: Option<String>,
    batch_size: usize,
    batch_window_ms: u64,
    starting_position: StartingPosition,
    starting_position_timestamp: Option<i64>,
    max_retries: u32,
    parallelization_factor: u32,
    on_failure: OnFailure,
    dlq_topic: Option<String>,
    filter_pattern: Option<String>,
    provider_config: HashMap<String, String>,
    enabled: bool,
}

impl TriggerConfigBuilder {
    pub fn new() -> Self {
        Self {
            batch_size: default_batch_size(),
            batch_window_ms: default_batch_window_ms(),
            max_retries: default_max_retries(),
            parallelization_factor: default_parallelization_factor(),
            enabled: true,
            ..Default::default()
        }
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    pub fn partitions(mut self, partitions: Vec<i32>) -> Self {
        self.partitions = partitions;
        self
    }

    pub fn provider(mut self, provider: FunctionProvider) -> Self {
        self.provider = Some(provider);
        self
    }

    pub fn function_arn(mut self, arn: impl Into<String>) -> Self {
        self.function_arn = Some(arn.into());
        self
    }

    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn batch_window_ms(mut self, ms: u64) -> Self {
        self.batch_window_ms = ms;
        self
    }

    pub fn starting_position(mut self, pos: StartingPosition) -> Self {
        self.starting_position = pos;
        self
    }

    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    pub fn parallelization_factor(mut self, factor: u32) -> Self {
        self.parallelization_factor = factor;
        self
    }

    pub fn on_failure(mut self, behavior: OnFailure) -> Self {
        self.on_failure = behavior;
        self
    }

    pub fn dlq_topic(mut self, topic: impl Into<String>) -> Self {
        self.dlq_topic = Some(topic.into());
        self
    }

    pub fn filter_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.filter_pattern = Some(pattern.into());
        self
    }

    pub fn provider_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.provider_config.insert(key.into(), value.into());
        self
    }

    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn build(self) -> Result<TriggerConfig> {
        let name = self
            .name
            .ok_or_else(|| StreamlineError::config("trigger", "name is required"))?;
        let topic = self
            .topic
            .ok_or_else(|| StreamlineError::config("trigger", "topic is required"))?;
        let provider = self
            .provider
            .ok_or_else(|| StreamlineError::config("trigger", "provider is required"))?;
        let function_arn = self
            .function_arn
            .ok_or_else(|| StreamlineError::config("trigger", "function_arn is required"))?;

        Ok(TriggerConfig {
            name,
            topic,
            partitions: self.partitions,
            provider,
            function_arn,
            batch_size: self.batch_size,
            batch_window_ms: self.batch_window_ms,
            starting_position: self.starting_position,
            starting_position_timestamp: self.starting_position_timestamp,
            max_retries: self.max_retries,
            parallelization_factor: self.parallelization_factor,
            on_failure: self.on_failure,
            dlq_topic: self.dlq_topic,
            filter_pattern: self.filter_pattern,
            provider_config: self.provider_config,
            enabled: self.enabled,
        })
    }
}

/// Runtime metrics for an event source mapping
#[derive(Debug, Default)]
pub struct TriggerMetrics {
    /// Total records processed
    pub records_processed: AtomicU64,
    /// Total records failed
    pub records_failed: AtomicU64,
    /// Total function invocations
    pub invocations: AtomicU64,
    /// Failed invocations
    pub invocation_errors: AtomicU64,
    /// Records sent to DLQ
    pub dlq_records: AtomicU64,
    /// Average batch size
    pub avg_batch_size: AtomicU64,
    /// Last processed offset per partition
    pub last_offsets: RwLock<HashMap<i32, i64>>,
}

impl TriggerMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_processed(&self, count: u64) {
        self.records_processed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_failed(&self, count: u64) {
        self.records_failed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_invocation(&self, success: bool) {
        self.invocations.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.invocation_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_dlq(&self, count: u64) {
        self.dlq_records.fetch_add(count, Ordering::Relaxed);
    }

    pub async fn update_offset(&self, partition: i32, offset: i64) {
        let mut offsets = self.last_offsets.write().await;
        offsets.insert(partition, offset);
    }

    pub fn snapshot(&self) -> TriggerMetricsSnapshot {
        TriggerMetricsSnapshot {
            records_processed: self.records_processed.load(Ordering::Relaxed),
            records_failed: self.records_failed.load(Ordering::Relaxed),
            invocations: self.invocations.load(Ordering::Relaxed),
            invocation_errors: self.invocation_errors.load(Ordering::Relaxed),
            dlq_records: self.dlq_records.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of trigger metrics for serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerMetricsSnapshot {
    pub records_processed: u64,
    pub records_failed: u64,
    pub invocations: u64,
    pub invocation_errors: u64,
    pub dlq_records: u64,
}

/// Event source mapping runtime
pub struct EventSourceMapping {
    /// Configuration
    pub config: TriggerConfig,
    /// Current state
    state: MappingState,
    /// Runtime metrics
    metrics: Arc<TriggerMetrics>,
    /// Shutdown signal
    shutdown: Arc<AtomicBool>,
}

impl EventSourceMapping {
    /// Create a new event source mapping
    pub fn new(config: TriggerConfig) -> Self {
        Self {
            config,
            state: MappingState::Creating,
            metrics: Arc::new(TriggerMetrics::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create a builder for event source mapping
    pub fn builder() -> TriggerConfigBuilder {
        TriggerConfigBuilder::new()
    }

    /// Get the current state
    pub fn state(&self) -> MappingState {
        self.state
    }

    /// Enable the mapping
    pub fn enable(&mut self) {
        self.state = MappingState::Enabled;
        info!(name = %self.config.name, "Event source mapping enabled");
    }

    /// Disable the mapping
    pub fn disable(&mut self) {
        self.state = MappingState::Disabled;
        info!(name = %self.config.name, "Event source mapping disabled");
    }

    /// Get metrics
    pub fn metrics(&self) -> &Arc<TriggerMetrics> {
        &self.metrics
    }

    /// Signal shutdown
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Check if shutdown was signaled
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Get mapping info for API responses
    pub fn info(&self) -> EventSourceMappingInfo {
        EventSourceMappingInfo {
            name: self.config.name.clone(),
            topic: self.config.topic.clone(),
            provider: self.config.provider.clone(),
            function_arn: self.config.function_arn.clone(),
            state: self.state,
            batch_size: self.config.batch_size,
            max_retries: self.config.max_retries,
            metrics: self.metrics.snapshot(),
        }
    }
}

/// Event source mapping info for API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSourceMappingInfo {
    pub name: String,
    pub topic: String,
    pub provider: FunctionProvider,
    pub function_arn: String,
    pub state: MappingState,
    pub batch_size: usize,
    pub max_retries: u32,
    pub metrics: TriggerMetricsSnapshot,
}

/// Manager for event source mappings
pub struct TriggerManager {
    /// Active mappings by name
    mappings: RwLock<HashMap<String, EventSourceMapping>>,
}

impl TriggerManager {
    /// Create a new trigger manager
    pub fn new() -> Self {
        Self {
            mappings: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new trigger manager wrapped in Arc
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Create a new event source mapping
    pub async fn create(&self, config: TriggerConfig) -> Result<EventSourceMappingInfo> {
        let name = config.name.clone();

        {
            let mappings = self.mappings.read().await;
            if mappings.contains_key(&name) {
                return Err(StreamlineError::config(
                    "trigger",
                    format!("Mapping '{}' already exists", name),
                ));
            }
        }

        let mut mapping = EventSourceMapping::new(config);
        if mapping.config.enabled {
            mapping.enable();
        }

        let info = mapping.info();

        {
            let mut mappings = self.mappings.write().await;
            mappings.insert(name.clone(), mapping);
        }

        info!(name = %name, "Created event source mapping");
        Ok(info)
    }

    /// Get a mapping by name
    pub async fn get(&self, name: &str) -> Option<EventSourceMappingInfo> {
        let mappings = self.mappings.read().await;
        mappings.get(name).map(|m| m.info())
    }

    /// List all mappings
    pub async fn list(&self) -> Vec<EventSourceMappingInfo> {
        let mappings = self.mappings.read().await;
        mappings.values().map(|m| m.info()).collect()
    }

    /// Update a mapping
    pub async fn update(
        &self,
        name: &str,
        enabled: Option<bool>,
    ) -> Result<EventSourceMappingInfo> {
        let mut mappings = self.mappings.write().await;

        let mapping = mappings.get_mut(name).ok_or_else(|| {
            StreamlineError::config("trigger", format!("Mapping '{}' not found", name))
        })?;

        if let Some(enabled) = enabled {
            if enabled {
                mapping.enable();
            } else {
                mapping.disable();
            }
        }

        Ok(mapping.info())
    }

    /// Delete a mapping
    pub async fn delete(&self, name: &str) -> Result<()> {
        let mut mappings = self.mappings.write().await;

        let mapping = mappings.remove(name).ok_or_else(|| {
            StreamlineError::config("trigger", format!("Mapping '{}' not found", name))
        })?;

        mapping.shutdown();
        info!(name = %name, "Deleted event source mapping");
        Ok(())
    }

    /// Get metrics for a mapping
    pub async fn metrics(&self, name: &str) -> Option<TriggerMetricsSnapshot> {
        let mappings = self.mappings.read().await;
        mappings.get(name).map(|m| m.metrics.snapshot())
    }
}

impl Default for TriggerManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trigger_config_builder() {
        let config = TriggerConfig::builder()
            .name("test-trigger")
            .topic("events")
            .provider(FunctionProvider::AwsLambda)
            .function_arn("arn:aws:lambda:us-east-1:123456789:function:test")
            .batch_size(50)
            .build()
            .unwrap();

        assert_eq!(config.name, "test-trigger");
        assert_eq!(config.topic, "events");
        assert_eq!(config.batch_size, 50);
        assert!(config.enabled);
    }

    #[test]
    fn test_trigger_config_builder_missing_required() {
        let result = TriggerConfig::builder().topic("events").build();
        assert!(result.is_err());
    }

    #[test]
    fn test_mapping_state_transitions() {
        let config = TriggerConfig::builder()
            .name("test")
            .topic("events")
            .provider(FunctionProvider::HttpWebhook)
            .function_arn("https://example.com/webhook")
            .build()
            .unwrap();

        let mut mapping = EventSourceMapping::new(config);
        assert_eq!(mapping.state(), MappingState::Creating);

        mapping.enable();
        assert_eq!(mapping.state(), MappingState::Enabled);

        mapping.disable();
        assert_eq!(mapping.state(), MappingState::Disabled);
    }

    #[test]
    fn test_trigger_metrics() {
        let metrics = TriggerMetrics::new();

        metrics.record_processed(100);
        metrics.record_failed(5);
        metrics.record_invocation(true);
        metrics.record_invocation(false);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.records_processed, 100);
        assert_eq!(snapshot.records_failed, 5);
        assert_eq!(snapshot.invocations, 2);
        assert_eq!(snapshot.invocation_errors, 1);
    }

    #[tokio::test]
    async fn test_trigger_manager() {
        let manager = TriggerManager::new();

        // Create a mapping
        let config = TriggerConfig::builder()
            .name("my-trigger")
            .topic("events")
            .provider(FunctionProvider::AwsLambda)
            .function_arn("arn:aws:lambda:us-east-1:123456789:function:test")
            .build()
            .unwrap();

        let info = manager.create(config).await.unwrap();
        assert_eq!(info.name, "my-trigger");
        assert_eq!(info.state, MappingState::Enabled);

        // List mappings
        let mappings = manager.list().await;
        assert_eq!(mappings.len(), 1);

        // Update mapping
        let updated = manager.update("my-trigger", Some(false)).await.unwrap();
        assert_eq!(updated.state, MappingState::Disabled);

        // Delete mapping
        manager.delete("my-trigger").await.unwrap();
        assert!(manager.get("my-trigger").await.is_none());
    }
}
