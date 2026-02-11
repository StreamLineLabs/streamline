//! Dead Letter Queue (DLQ) support for failed message handling
//!
//! This module provides automatic dead letter queue functionality:
//! - DLQ topics auto-created as `{topic}.dlq`
//! - Full context preserved in DLQ records
//! - Replay capability to re-process messages
//! - Metrics for monitoring DLQ depth and growth

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::error::{Result, StreamlineError};
use crate::storage::record::Header;
use crate::storage::{Record, TopicManager};

/// Default DLQ topic suffix
pub const DEFAULT_DLQ_SUFFIX: &str = ".dlq";

/// Header key for DLQ metadata
pub const DLQ_METADATA_HEADER: &str = "x-dlq-metadata";

/// DLQ configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqConfig {
    /// Whether DLQ is enabled
    pub enabled: bool,

    /// Suffix appended to topic names for DLQ topics
    pub suffix: String,

    /// Number of partitions for auto-created DLQ topics
    pub partitions: i32,

    /// Whether to enable automatic retry
    pub retry_enabled: bool,

    /// Maximum retry attempts before giving up
    pub max_retry_attempts: u32,

    /// Initial retry delay in milliseconds
    pub initial_delay_ms: u64,

    /// Maximum retry delay in milliseconds
    pub max_delay_ms: u64,

    /// Backoff multiplier for exponential backoff
    pub backoff_multiplier: f64,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            suffix: DEFAULT_DLQ_SUFFIX.to_string(),
            partitions: 1,
            retry_enabled: false,
            max_retry_attempts: 3,
            initial_delay_ms: 1000,
            max_delay_ms: 60000,
            backoff_multiplier: 2.0,
        }
    }
}

impl DlqConfig {
    /// Create a new DLQ config with DLQ enabled
    pub fn enabled() -> Self {
        Self::default()
    }

    /// Create a new DLQ config with DLQ disabled
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Enable automatic retry with default settings
    pub fn with_retry(mut self) -> Self {
        self.retry_enabled = true;
        self
    }

    /// Set maximum retry attempts
    pub fn with_max_retries(mut self, max: u32) -> Self {
        self.max_retry_attempts = max;
        self
    }

    /// Set the DLQ topic suffix
    pub fn with_suffix(mut self, suffix: impl Into<String>) -> Self {
        self.suffix = suffix.into();
        self
    }
}

/// Context for a message being sent to DLQ
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqContext {
    /// Reason for sending to DLQ
    pub reason: String,

    /// Number of times this message has been retried
    pub retry_count: u32,

    /// Consumer group that failed to process the message
    pub consumer_group: Option<String>,

    /// Consumer ID that failed to process the message
    pub consumer_id: Option<String>,
}

impl DlqContext {
    /// Create a new DLQ context with a reason
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            retry_count: 0,
            consumer_group: None,
            consumer_id: None,
        }
    }

    /// Set the retry count
    pub fn with_retry_count(mut self, count: u32) -> Self {
        self.retry_count = count;
        self
    }

    /// Set the consumer group
    pub fn with_consumer_group(mut self, group: impl Into<String>) -> Self {
        self.consumer_group = Some(group.into());
        self
    }

    /// Set the consumer ID
    pub fn with_consumer_id(mut self, id: impl Into<String>) -> Self {
        self.consumer_id = Some(id.into());
        self
    }
}

/// Metadata stored with DLQ records
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqMetadata {
    /// Original topic name
    pub original_topic: String,

    /// Original partition
    pub original_partition: i32,

    /// Original offset
    pub original_offset: i64,

    /// Original timestamp
    pub original_timestamp: i64,

    /// Error reason
    pub error_reason: String,

    /// Error timestamp (when sent to DLQ)
    pub error_timestamp: i64,

    /// Number of retry attempts
    pub retry_count: u32,

    /// Consumer group that failed
    pub consumer_group: Option<String>,

    /// Consumer ID that failed
    pub consumer_id: Option<String>,
}

impl DlqMetadata {
    /// Create metadata from a record and context
    pub fn from_record(record: &Record, topic: &str, partition: i32, context: &DlqContext) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        Self {
            original_topic: topic.to_string(),
            original_partition: partition,
            original_offset: record.offset,
            original_timestamp: record.timestamp,
            error_reason: context.reason.clone(),
            error_timestamp: now,
            retry_count: context.retry_count,
            consumer_group: context.consumer_group.clone(),
            consumer_id: context.consumer_id.clone(),
        }
    }
}

/// Dead Letter Queue manager
pub struct DlqManager {
    /// Topic manager for storage operations
    topic_manager: Arc<TopicManager>,

    /// DLQ configuration
    config: DlqConfig,
}

impl DlqManager {
    /// Create a new DLQ manager
    pub fn new(topic_manager: Arc<TopicManager>, config: DlqConfig) -> Self {
        Self {
            topic_manager,
            config,
        }
    }

    /// Check if DLQ is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the DLQ topic name for a given topic
    pub fn dlq_topic_for(&self, topic: &str) -> String {
        format!("{}{}", topic, self.config.suffix)
    }

    /// Get the original topic name from a DLQ topic
    pub fn original_topic_for(&self, dlq_topic: &str) -> Option<String> {
        if dlq_topic.ends_with(&self.config.suffix) {
            Some(dlq_topic[..dlq_topic.len() - self.config.suffix.len()].to_string())
        } else {
            None
        }
    }

    /// Check if a topic is a DLQ topic
    pub fn is_dlq_topic(&self, topic: &str) -> bool {
        topic.ends_with(&self.config.suffix)
    }

    /// Send a failed record to the DLQ
    ///
    /// This will:
    /// 1. Auto-create the DLQ topic if it doesn't exist
    /// 2. Add DLQ metadata to the record
    /// 3. Preserve the original key and value
    pub fn send_to_dlq(
        &self,
        original_record: &Record,
        original_topic: &str,
        original_partition: i32,
        context: DlqContext,
    ) -> Result<i64> {
        if !self.config.enabled {
            return Err(StreamlineError::Config("DLQ is disabled".to_string()));
        }

        let dlq_topic = self.dlq_topic_for(original_topic);

        // Auto-create DLQ topic if needed
        let _ = self
            .topic_manager
            .get_or_create_topic(&dlq_topic, self.config.partitions);

        // Create DLQ metadata
        let metadata = DlqMetadata::from_record(
            original_record,
            original_topic,
            original_partition,
            &context,
        );

        // Serialize metadata to JSON
        let metadata_json = serde_json::to_string(&metadata).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to serialize DLQ metadata: {}", e))
        })?;

        // Build headers with DLQ metadata plus original headers
        let mut headers = vec![Header {
            key: DLQ_METADATA_HEADER.to_string(),
            value: Bytes::from(metadata_json),
        }];
        headers.extend(original_record.headers.clone());

        // Send to DLQ topic (partition 0 by default, or based on original key)
        let dlq_partition = if let Some(ref key) = original_record.key {
            let hash = key.iter().fold(0u32, |acc, b| acc.wrapping_add(*b as u32));
            (hash as i32) % self.config.partitions
        } else {
            0
        };

        let offset = self.topic_manager.append_with_headers(
            &dlq_topic,
            dlq_partition,
            original_record.key.clone(),
            original_record.value.clone(),
            headers,
        )?;

        info!(
            original_topic = %original_topic,
            dlq_topic = %dlq_topic,
            original_offset = %original_record.offset,
            dlq_offset = %offset,
            reason = %context.reason,
            "Message sent to DLQ"
        );

        Ok(offset)
    }

    /// Read records from a DLQ topic
    pub fn read_dlq(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_records: usize,
    ) -> Result<Vec<(Record, DlqMetadata)>> {
        let dlq_topic = self.dlq_topic_for(topic);

        let records = self
            .topic_manager
            .read(&dlq_topic, partition, offset, max_records)?;

        let mut result = Vec::with_capacity(records.len());

        for record in records {
            // Extract DLQ metadata from header
            let metadata = record
                .headers
                .iter()
                .find(|h| h.key == DLQ_METADATA_HEADER)
                .and_then(|h| serde_json::from_slice::<DlqMetadata>(&h.value).ok());

            if let Some(meta) = metadata {
                result.push((record, meta));
            } else {
                warn!(
                    topic = %dlq_topic,
                    offset = %record.offset,
                    "DLQ record missing metadata"
                );
            }
        }

        Ok(result)
    }

    /// Replay messages from DLQ back to the original topic
    ///
    /// Returns the number of messages replayed
    pub fn replay(
        &self,
        original_topic: &str,
        partition: i32,
        offset: i64,
        max_records: usize,
    ) -> Result<usize> {
        let dlq_records = self.read_dlq(original_topic, partition, offset, max_records)?;

        let mut replayed = 0;

        for (record, metadata) in dlq_records {
            // Remove DLQ metadata header before replaying
            let headers: Vec<Header> = record
                .headers
                .into_iter()
                .filter(|h| h.key != DLQ_METADATA_HEADER)
                .collect();

            // Replay to original topic
            let replay_partition = metadata.original_partition;

            let result = self.topic_manager.append_with_headers(
                original_topic,
                replay_partition,
                record.key,
                record.value,
                headers,
            );

            match result {
                Ok(new_offset) => {
                    debug!(
                        original_topic = %original_topic,
                        original_offset = %metadata.original_offset,
                        new_offset = %new_offset,
                        "Message replayed from DLQ"
                    );
                    replayed += 1;
                }
                Err(e) => {
                    warn!(
                        original_topic = %original_topic,
                        error = %e,
                        "Failed to replay message from DLQ"
                    );
                }
            }
        }

        info!(
            topic = %original_topic,
            replayed = %replayed,
            "DLQ replay completed"
        );

        Ok(replayed)
    }

    /// Get statistics for a DLQ topic
    pub fn dlq_stats(&self, topic: &str) -> Result<DlqStats> {
        let dlq_topic = self.dlq_topic_for(topic);

        match self.topic_manager.get_topic_metadata(&dlq_topic) {
            Ok(metadata) => {
                let mut total_messages = 0i64;

                for partition_id in 0..metadata.num_partitions {
                    let earliest = self
                        .topic_manager
                        .earliest_offset(&dlq_topic, partition_id)
                        .unwrap_or(0);
                    let latest = self
                        .topic_manager
                        .latest_offset(&dlq_topic, partition_id)
                        .unwrap_or(0);
                    total_messages += latest - earliest;
                }

                Ok(DlqStats {
                    topic: topic.to_string(),
                    dlq_topic,
                    depth: total_messages,
                    partitions: metadata.num_partitions,
                })
            }
            Err(_) => Ok(DlqStats {
                topic: topic.to_string(),
                dlq_topic,
                depth: 0,
                partitions: 0,
            }),
        }
    }

    /// Purge (delete) messages from a DLQ topic up to a given offset
    pub fn purge(&self, topic: &str, partition: i32, before_offset: i64) -> Result<i64> {
        let dlq_topic = self.dlq_topic_for(topic);

        let deleted =
            self.topic_manager
                .delete_records_before(&dlq_topic, partition, before_offset)?;

        info!(
            dlq_topic = %dlq_topic,
            partition = %partition,
            before_offset = %before_offset,
            deleted = %deleted,
            "DLQ purged"
        );

        Ok(deleted)
    }

    /// Calculate retry delay based on retry count (exponential backoff)
    pub fn calculate_retry_delay(&self, retry_count: u32) -> u64 {
        if !self.config.retry_enabled {
            return 0;
        }

        let delay = self.config.initial_delay_ms as f64
            * self.config.backoff_multiplier.powi(retry_count as i32);

        (delay as u64).min(self.config.max_delay_ms)
    }

    /// Check if a message should be retried
    pub fn should_retry(&self, retry_count: u32) -> bool {
        self.config.retry_enabled && retry_count < self.config.max_retry_attempts
    }
}

/// DLQ statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqStats {
    /// Original topic name
    pub topic: String,

    /// DLQ topic name
    pub dlq_topic: String,

    /// Number of messages in the DLQ
    pub depth: i64,

    /// Number of partitions
    pub partitions: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dlq_config_default() {
        let config = DlqConfig::default();
        assert!(config.enabled);
        assert_eq!(config.suffix, ".dlq");
        assert!(!config.retry_enabled);
        assert_eq!(config.max_retry_attempts, 3);
    }

    #[test]
    fn test_dlq_config_disabled() {
        let config = DlqConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_dlq_config_with_retry() {
        let config = DlqConfig::default().with_retry().with_max_retries(5);
        assert!(config.retry_enabled);
        assert_eq!(config.max_retry_attempts, 5);
    }

    #[test]
    fn test_dlq_context() {
        let context = DlqContext::new("Validation failed")
            .with_retry_count(2)
            .with_consumer_group("my-group")
            .with_consumer_id("consumer-1");

        assert_eq!(context.reason, "Validation failed");
        assert_eq!(context.retry_count, 2);
        assert_eq!(context.consumer_group, Some("my-group".to_string()));
        assert_eq!(context.consumer_id, Some("consumer-1".to_string()));
    }

    #[test]
    fn test_dlq_topic_naming() {
        let topic_manager = TopicManager::in_memory().unwrap();
        let dlq_manager = DlqManager::new(Arc::new(topic_manager), DlqConfig::default());

        assert_eq!(dlq_manager.dlq_topic_for("events"), "events.dlq");
        assert_eq!(
            dlq_manager.dlq_topic_for("orders.placed"),
            "orders.placed.dlq"
        );

        assert!(dlq_manager.is_dlq_topic("events.dlq"));
        assert!(!dlq_manager.is_dlq_topic("events"));

        assert_eq!(
            dlq_manager.original_topic_for("events.dlq"),
            Some("events".to_string())
        );
        assert_eq!(dlq_manager.original_topic_for("events"), None);
    }

    #[test]
    fn test_dlq_send_and_read() {
        let topic_manager = Arc::new(TopicManager::in_memory().unwrap());
        let dlq_manager = DlqManager::new(topic_manager.clone(), DlqConfig::default());

        // Create original topic
        topic_manager.create_topic("test-topic", 1).unwrap();

        // Create a test record
        let record = Record::new(
            42,
            1234567890,
            Some(Bytes::from("key")),
            Bytes::from("value"),
        );

        // Send to DLQ
        let context = DlqContext::new("Test error").with_consumer_group("test-group");
        let dlq_offset = dlq_manager
            .send_to_dlq(&record, "test-topic", 0, context)
            .unwrap();

        assert_eq!(dlq_offset, 0);

        // Read from DLQ
        let dlq_records = dlq_manager.read_dlq("test-topic", 0, 0, 100).unwrap();
        assert_eq!(dlq_records.len(), 1);

        let (dlq_record, metadata) = &dlq_records[0];
        assert_eq!(dlq_record.value, Bytes::from("value"));
        assert_eq!(metadata.original_topic, "test-topic");
        assert_eq!(metadata.original_offset, 42);
        assert_eq!(metadata.error_reason, "Test error");
        assert_eq!(metadata.consumer_group, Some("test-group".to_string()));
    }

    #[test]
    fn test_dlq_replay() {
        let topic_manager = Arc::new(TopicManager::in_memory().unwrap());
        let dlq_manager = DlqManager::new(topic_manager.clone(), DlqConfig::default());

        // Create original topic
        topic_manager.create_topic("replay-test", 1).unwrap();

        // Send some messages to DLQ
        for i in 0..5 {
            let record = Record::new(
                i,
                1234567890 + i,
                Some(Bytes::from(format!("key-{}", i))),
                Bytes::from(format!("value-{}", i)),
            );
            let context = DlqContext::new(format!("Error {}", i));
            dlq_manager
                .send_to_dlq(&record, "replay-test", 0, context)
                .unwrap();
        }

        // Replay messages
        let replayed = dlq_manager.replay("replay-test", 0, 0, 100).unwrap();
        assert_eq!(replayed, 5);

        // Check messages were replayed to original topic
        let records = topic_manager.read("replay-test", 0, 0, 100).unwrap();
        assert_eq!(records.len(), 5);
    }

    #[test]
    fn test_dlq_stats() {
        let topic_manager = Arc::new(TopicManager::in_memory().unwrap());
        let dlq_manager = DlqManager::new(topic_manager.clone(), DlqConfig::default());

        // Create original topic
        topic_manager.create_topic("stats-test", 1).unwrap();

        // Stats for empty DLQ
        let stats = dlq_manager.dlq_stats("stats-test").unwrap();
        assert_eq!(stats.depth, 0);

        // Send some messages to DLQ
        for i in 0..3 {
            let record = Record::new(i, 1234567890, None, Bytes::from("value"));
            let context = DlqContext::new("Error");
            dlq_manager
                .send_to_dlq(&record, "stats-test", 0, context)
                .unwrap();
        }

        // Check stats
        let stats = dlq_manager.dlq_stats("stats-test").unwrap();
        assert_eq!(stats.depth, 3);
        assert_eq!(stats.topic, "stats-test");
        assert_eq!(stats.dlq_topic, "stats-test.dlq");
    }

    #[test]
    fn test_retry_delay_calculation() {
        let config = DlqConfig::default().with_retry();
        let topic_manager = Arc::new(TopicManager::in_memory().unwrap());
        let dlq_manager = DlqManager::new(topic_manager, config);

        // First retry: 1000ms
        assert_eq!(dlq_manager.calculate_retry_delay(0), 1000);

        // Second retry: 2000ms
        assert_eq!(dlq_manager.calculate_retry_delay(1), 2000);

        // Third retry: 4000ms
        assert_eq!(dlq_manager.calculate_retry_delay(2), 4000);

        // Should not exceed max
        assert_eq!(dlq_manager.calculate_retry_delay(10), 60000);
    }

    #[test]
    fn test_should_retry() {
        let config = DlqConfig::default().with_retry().with_max_retries(3);
        let topic_manager = Arc::new(TopicManager::in_memory().unwrap());
        let dlq_manager = DlqManager::new(topic_manager, config);

        assert!(dlq_manager.should_retry(0));
        assert!(dlq_manager.should_retry(1));
        assert!(dlq_manager.should_retry(2));
        assert!(!dlq_manager.should_retry(3)); // Max reached
        assert!(!dlq_manager.should_retry(4));
    }

    #[test]
    fn test_dlq_disabled() {
        let config = DlqConfig::disabled();
        let topic_manager = Arc::new(TopicManager::in_memory().unwrap());
        let dlq_manager = DlqManager::new(topic_manager, config);

        assert!(!dlq_manager.is_enabled());

        let record = Record::new(0, 0, None, Bytes::from("value"));
        let result = dlq_manager.send_to_dlq(&record, "test", 0, DlqContext::new("Error"));
        assert!(result.is_err());
    }
}
