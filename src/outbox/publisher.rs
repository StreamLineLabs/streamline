//! Outbox Publisher
//!
//! Polls the outbox table and publishes events to Streamline topics.

use super::config::PublisherConfig;
use super::deduplication::DeduplicationStore;
use super::schema::OutboxEntry;
use crate::error::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Outbox publisher
pub struct OutboxPublisher {
    /// Publisher ID
    id: String,
    /// Configuration
    config: PublisherConfig,
    /// Deduplication store
    dedup_store: Arc<DeduplicationStore>,
    /// Statistics
    stats: Arc<PublisherStatsInner>,
    /// Running flag
    running: Arc<std::sync::atomic::AtomicBool>,
    /// Pending entries queue
    #[allow(dead_code)]
    pending: Arc<RwLock<Vec<OutboxEntry>>>,
}

struct PublisherStatsInner {
    messages_published: AtomicU64,
    messages_failed: AtomicU64,
    messages_retried: AtomicU64,
    messages_deduplicated: AtomicU64,
    poll_count: AtomicU64,
    last_poll_timestamp: AtomicU64,
}

impl OutboxPublisher {
    /// Create a new outbox publisher
    pub fn new(
        id: impl Into<String>,
        config: PublisherConfig,
        dedup_store: Arc<DeduplicationStore>,
    ) -> Self {
        Self {
            id: id.into(),
            config,
            dedup_store,
            stats: Arc::new(PublisherStatsInner {
                messages_published: AtomicU64::new(0),
                messages_failed: AtomicU64::new(0),
                messages_retried: AtomicU64::new(0),
                messages_deduplicated: AtomicU64::new(0),
                poll_count: AtomicU64::new(0),
                last_poll_timestamp: AtomicU64::new(0),
            }),
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            pending: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Get publisher ID
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Start the publisher
    pub async fn start(&self) -> Result<()> {
        if self.running.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }

        self.running
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let running = self.running.clone();
        let config = self.config.clone();
        let stats = self.stats.clone();
        let dedup_store = self.dedup_store.clone();
        let id = self.id.clone();

        tokio::spawn(async move {
            let interval = std::time::Duration::from_millis(config.poll_interval_ms);

            while running.load(std::sync::atomic::Ordering::SeqCst) {
                // Poll and process
                if let Err(e) = Self::poll_and_process(&id, &config, &stats, &dedup_store).await {
                    tracing::error!(publisher_id = %id, error = %e, "Poll error");
                }

                tokio::time::sleep(interval).await;
            }
        });

        Ok(())
    }

    /// Stop the publisher
    pub async fn stop(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// Check if running
    pub fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Poll outbox and process entries
    async fn poll_and_process(
        id: &str,
        config: &PublisherConfig,
        stats: &PublisherStatsInner,
        _dedup_store: &DeduplicationStore,
    ) -> Result<()> {
        stats.poll_count.fetch_add(1, Ordering::Relaxed);
        stats.last_poll_timestamp.store(
            chrono::Utc::now().timestamp_millis() as u64,
            Ordering::Relaxed,
        );

        // In production, this would:
        // 1. Connect to database using config.connection_string
        // 2. Execute SELECT query to get pending entries
        // 3. Process each entry

        // Simulated polling (production would use actual database)
        tracing::debug!(
            publisher_id = %id,
            batch_size = config.batch_size,
            "Polling outbox table"
        );

        Ok(())
    }

    /// Process a single outbox entry
    pub async fn process_entry(&self, entry: &mut OutboxEntry) -> Result<PublishResult> {
        // Check for deduplication
        if self.dedup_store.is_duplicate(&entry.dedup_key()).await {
            self.stats
                .messages_deduplicated
                .fetch_add(1, Ordering::Relaxed);
            return Ok(PublishResult {
                id: entry.id.clone(),
                success: true,
                deduplicated: true,
                offset: None,
                error: None,
            });
        }

        // Claim the entry
        entry.claim();

        // Determine target topic
        let topic = entry
            .topic
            .clone()
            .unwrap_or_else(|| self.config.default_topic.clone());

        // In production, this would publish to Streamline
        tracing::debug!(
            entry_id = %entry.id,
            topic = %topic,
            event_type = %entry.event_type,
            "Publishing outbox entry"
        );

        // Simulate publish (production would use actual producer)
        let result = self.publish_to_topic(&topic, entry).await;

        match &result {
            Ok(_) => {
                entry.mark_published();
                self.stats
                    .messages_published
                    .fetch_add(1, Ordering::Relaxed);

                // Record for deduplication
                self.dedup_store.record(&entry.dedup_key()).await;

                Ok(PublishResult {
                    id: entry.id.clone(),
                    success: true,
                    deduplicated: false,
                    offset: Some(0), // Would be actual offset
                    error: None,
                })
            }
            Err(e) => {
                entry.mark_failed(e.to_string());
                self.stats.messages_failed.fetch_add(1, Ordering::Relaxed);

                if entry.retry_count > 1 {
                    self.stats.messages_retried.fetch_add(1, Ordering::Relaxed);
                }

                Ok(PublishResult {
                    id: entry.id.clone(),
                    success: false,
                    deduplicated: false,
                    offset: None,
                    error: Some(e.to_string()),
                })
            }
        }
    }

    /// Publish entry to topic
    async fn publish_to_topic(&self, topic: &str, entry: &OutboxEntry) -> Result<i64> {
        // In production, this would use the actual producer
        // For now, just simulate success
        tracing::info!(
            topic = %topic,
            aggregate_type = %entry.aggregate_type,
            aggregate_id = %entry.aggregate_id,
            event_type = %entry.event_type,
            "Publishing message"
        );

        Ok(0) // Would return actual offset
    }

    /// Get publisher statistics
    pub fn stats(&self) -> PublisherStats {
        PublisherStats {
            messages_published: self.stats.messages_published.load(Ordering::Relaxed),
            messages_failed: self.stats.messages_failed.load(Ordering::Relaxed),
            messages_retried: self.stats.messages_retried.load(Ordering::Relaxed),
            messages_deduplicated: self.stats.messages_deduplicated.load(Ordering::Relaxed),
            poll_count: self.stats.poll_count.load(Ordering::Relaxed),
            last_poll_timestamp: self.stats.last_poll_timestamp.load(Ordering::Relaxed),
        }
    }

    /// Get configuration
    pub fn config(&self) -> &PublisherConfig {
        &self.config
    }
}

/// Publish result
#[derive(Debug, Clone)]
pub struct PublishResult {
    /// Entry ID
    pub id: String,
    /// Success flag
    pub success: bool,
    /// Was deduplicated (skipped)
    pub deduplicated: bool,
    /// Published offset (if successful)
    pub offset: Option<i64>,
    /// Error message (if failed)
    pub error: Option<String>,
}

/// Publisher statistics
#[derive(Debug, Clone, Default)]
pub struct PublisherStats {
    /// Messages successfully published
    pub messages_published: u64,
    /// Messages that failed to publish
    pub messages_failed: u64,
    /// Messages that were retried
    pub messages_retried: u64,
    /// Messages skipped due to deduplication
    pub messages_deduplicated: u64,
    /// Number of poll operations
    pub poll_count: u64,
    /// Last poll timestamp (milliseconds)
    pub last_poll_timestamp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outbox::config::DeduplicationConfig;
    use crate::outbox::schema::OutboxState;

    #[tokio::test]
    async fn test_publisher_creation() {
        let config = PublisherConfig::default();
        let dedup = Arc::new(DeduplicationStore::new(DeduplicationConfig::default()));
        let publisher = OutboxPublisher::new("test", config, dedup);

        assert_eq!(publisher.id(), "test");
        assert!(!publisher.is_running());
    }

    #[tokio::test]
    async fn test_process_entry() {
        let config = PublisherConfig::default();
        let dedup = Arc::new(DeduplicationStore::new(DeduplicationConfig::default()));
        let publisher = OutboxPublisher::new("test", config, dedup);

        let mut entry =
            OutboxEntry::new("Order", "1", "Created", b"{}".to_vec()).with_topic("orders");

        let result = publisher.process_entry(&mut entry).await.unwrap();

        assert!(result.success);
        assert!(!result.deduplicated);
        assert_eq!(entry.state, OutboxState::Published);
    }

    #[tokio::test]
    async fn test_deduplication() {
        let config = PublisherConfig::default();
        let dedup = Arc::new(DeduplicationStore::new(DeduplicationConfig::default()));
        let publisher = OutboxPublisher::new("test", config, dedup);

        let mut entry = OutboxEntry::new("Order", "1", "Created", b"{}".to_vec());

        // First publish
        let result1 = publisher.process_entry(&mut entry).await.unwrap();
        assert!(result1.success);
        assert!(!result1.deduplicated);

        // Create new entry with same dedup key
        let mut entry2 = entry.clone();
        entry2.state = OutboxState::Pending;

        // Second publish should be deduplicated
        let result2 = publisher.process_entry(&mut entry2).await.unwrap();
        assert!(result2.success);
        assert!(result2.deduplicated);
    }
}
