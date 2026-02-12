//! Streaming Similarity Search
//!
//! Provides continuous similarity search over streaming data with real-time
//! top-K notifications and incremental index updates.

use crate::ai::hnsw::{HnswConfig, HnswIndex};
use crate::ai::providers::EmbeddingProvider;
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, info, warn};

/// Configuration for similarity subscriptions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimilarityConfig {
    /// Maximum number of concurrent subscriptions
    pub max_subscriptions: usize,
    /// Buffer size for result notifications
    pub notification_buffer: usize,
    /// Minimum similarity score to report (0.0 - 1.0)
    pub min_score: f32,
    /// Default number of results (K)
    pub default_k: usize,
    /// Update interval in milliseconds (debounce)
    pub update_interval_ms: u64,
    /// Enable caching of recent results
    pub cache_enabled: bool,
    /// Maximum cache entries per subscription
    pub cache_size: usize,
}

impl Default for SimilarityConfig {
    fn default() -> Self {
        Self {
            max_subscriptions: 1000,
            notification_buffer: 100,
            min_score: 0.5,
            default_k: 10,
            update_interval_ms: 100,
            cache_enabled: true,
            cache_size: 100,
        }
    }
}

/// Query type for similarity search
#[derive(Debug, Clone)]
pub enum QueryType {
    /// Search by vector
    Vector(Vec<f32>),
    /// Search by text (will be embedded)
    Text(String),
}

/// Subscription configuration
#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    /// Query to match against
    pub query: QueryType,
    /// Number of top results to track (K)
    pub k: usize,
    /// Minimum similarity score
    pub min_score: f32,
    /// Topics to search
    pub topics: Vec<String>,
    /// Update interval override
    pub update_interval_ms: Option<u64>,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            query: QueryType::Vector(Vec::new()),
            k: 10,
            min_score: 0.5,
            topics: Vec::new(),
            update_interval_ms: None,
        }
    }
}

/// A streaming similarity result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingResult {
    /// Subscription ID
    pub subscription_id: u64,
    /// Current top-K results
    pub results: Vec<SimilarityResult>,
    /// Whether the results changed from last notification
    pub changed: bool,
    /// Timestamp of this result
    pub timestamp_ms: i64,
}

/// Individual similarity result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimilarityResult {
    /// Record offset
    pub offset: i64,
    /// Topic name
    pub topic: String,
    /// Partition
    pub partition: i32,
    /// Similarity score (higher = more similar)
    pub score: f32,
    /// Record key (if available)
    pub key: Option<String>,
    /// Record timestamp
    pub timestamp: i64,
}

/// Streaming similarity search manager
pub struct SimilarityStreamManager {
    config: SimilarityConfig,
    /// Per-topic HNSW indexes
    indexes: Arc<RwLock<HashMap<String, Arc<HnswIndex>>>>,
    /// Embedding provider
    provider: Arc<dyn EmbeddingProvider>,
    /// Active subscriptions
    subscriptions: Arc<RwLock<HashMap<u64, Subscription>>>,
    /// Subscription ID counter
    next_subscription_id: AtomicU64,
    /// Shutdown flag
    shutdown: AtomicBool,
    /// Statistics
    stats: SimilarityStats,
    /// Broadcast channel for new records
    record_tx: broadcast::Sender<NewRecordEvent>,
}

/// Internal subscription state
#[allow(dead_code)]
struct Subscription {
    id: u64,
    config: SubscriptionConfig,
    query_vector: Vec<f32>,
    result_tx: mpsc::Sender<StreamingResult>,
    last_results: Vec<SimilarityResult>,
    last_update_ms: i64,
}

/// Event for new record insertion
#[derive(Debug, Clone)]
pub struct NewRecordEvent {
    /// Topic
    pub topic: String,
    /// Partition
    pub partition: i32,
    /// Offset
    pub offset: i64,
    /// Timestamp
    pub timestamp: i64,
    /// Key
    pub key: Option<String>,
    /// Embedding vector
    pub vector: Vec<f32>,
}

impl SimilarityStreamManager {
    /// Create a new similarity stream manager
    pub fn new(config: SimilarityConfig, provider: Arc<dyn EmbeddingProvider>) -> Self {
        let (record_tx, _) = broadcast::channel(config.notification_buffer);

        Self {
            config,
            indexes: Arc::new(RwLock::new(HashMap::new())),
            provider,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            next_subscription_id: AtomicU64::new(1),
            shutdown: AtomicBool::new(false),
            stats: SimilarityStats::default(),
            record_tx,
        }
    }

    /// Start the background update loop
    pub async fn start(&self) -> Result<()> {
        let _indexes = self.indexes.clone();
        let _subscriptions = self.subscriptions.clone();
        let shutdown = &self.shutdown;
        let mut record_rx = self.record_tx.subscribe();
        let stats = &self.stats;
        let config = self.config.clone();

        info!("Starting similarity stream manager");

        while !shutdown.load(Ordering::Relaxed) {
            tokio::select! {
                result = record_rx.recv() => {
                    match result {
                        Ok(event) => {
                            // Index the new record
                            if let Err(e) = self.index_record(&event).await {
                                warn!(error = %e, "Failed to index record");
                            }

                            // Check and notify subscriptions
                            self.check_subscriptions(&event).await;
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!(lagged = n, "Subscription lagged, missed {} events", n);
                            stats.lagged_events.fetch_add(n, Ordering::Relaxed);
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(config.update_interval_ms)) => {
                    // Periodic check for subscriptions
                }
            }
        }

        Ok(())
    }

    /// Index a new record
    async fn index_record(&self, event: &NewRecordEvent) -> Result<()> {
        let mut indexes = self.indexes.write().await;

        let index = indexes.entry(event.topic.clone()).or_insert_with(|| {
            let config = HnswConfig::balanced(self.provider.dimension());
            Arc::new(HnswIndex::new(config))
        });

        index.insert(event.offset, event.vector.clone()).await?;
        self.stats.records_indexed.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Check subscriptions after new record
    async fn check_subscriptions(&self, event: &NewRecordEvent) {
        let subscriptions = self.subscriptions.read().await;
        let indexes = self.indexes.read().await;

        for subscription in subscriptions.values() {
            // Skip if topic not in subscription
            if !subscription.config.topics.is_empty()
                && !subscription.config.topics.contains(&event.topic)
            {
                continue;
            }

            // Check if this record matches the subscription
            if let Some(index) = indexes.get(&event.topic) {
                if let Ok(results) = index
                    .search(&subscription.query_vector, subscription.config.k)
                    .await
                {
                    // Convert to similarity scores
                    let similarity_results: Vec<SimilarityResult> = results
                        .into_iter()
                        .filter(|r| 1.0 - r.distance >= subscription.config.min_score)
                        .map(|r| SimilarityResult {
                            offset: r.id,
                            topic: event.topic.clone(),
                            partition: event.partition,
                            score: 1.0 - r.distance,
                            key: event.key.clone(),
                            timestamp: event.timestamp,
                        })
                        .collect();

                    // Check if results changed
                    let changed = similarity_results != subscription.last_results;

                    if changed && !similarity_results.is_empty() {
                        let result = StreamingResult {
                            subscription_id: subscription.id,
                            results: similarity_results,
                            changed: true,
                            timestamp_ms: chrono::Utc::now().timestamp_millis(),
                        };

                        if subscription.result_tx.try_send(result).is_err() {
                            debug!(
                                subscription_id = subscription.id,
                                "Subscription channel full, dropping update"
                            );
                        }
                    }
                }
            }
        }
    }

    /// Create a new subscription
    pub async fn subscribe(
        &self,
        mut config: SubscriptionConfig,
    ) -> Result<(u64, mpsc::Receiver<StreamingResult>)> {
        // Check subscription limit
        let current_count = self.subscriptions.read().await.len();
        if current_count >= self.config.max_subscriptions {
            return Err(StreamlineError::ResourceExhausted(
                "Maximum subscriptions reached".into(),
            ));
        }

        // Generate query vector
        let query_vector = match &config.query {
            QueryType::Vector(v) => v.clone(),
            QueryType::Text(text) => self
                .provider
                .embed(text)
                .await
                .map_err(|e| StreamlineError::AI(format!("Failed to embed query: {}", e)))?,
        };

        // Apply defaults
        if config.k == 0 {
            config.k = self.config.default_k;
        }
        if config.min_score <= 0.0 {
            config.min_score = self.config.min_score;
        }

        let id = self.next_subscription_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(self.config.notification_buffer);

        let subscription = Subscription {
            id,
            config,
            query_vector,
            result_tx: tx,
            last_results: Vec::new(),
            last_update_ms: 0,
        };

        self.subscriptions.write().await.insert(id, subscription);
        self.stats
            .subscriptions_created
            .fetch_add(1, Ordering::Relaxed);

        info!(subscription_id = id, "Created similarity subscription");

        Ok((id, rx))
    }

    /// Cancel a subscription
    pub async fn unsubscribe(&self, subscription_id: u64) -> bool {
        let removed = self
            .subscriptions
            .write()
            .await
            .remove(&subscription_id)
            .is_some();
        if removed {
            self.stats
                .subscriptions_cancelled
                .fetch_add(1, Ordering::Relaxed);
            info!(subscription_id, "Cancelled similarity subscription");
        }
        removed
    }

    /// Publish a new record for indexing
    pub fn publish(&self, event: NewRecordEvent) -> Result<()> {
        // It's OK if there are no subscribers - the event is still indexed
        let _ = self.record_tx.send(event);
        Ok(())
    }

    /// Perform one-time similarity search
    pub async fn search(
        &self,
        query: QueryType,
        topics: &[String],
        k: usize,
        min_score: f32,
    ) -> Result<Vec<SimilarityResult>> {
        let query_vector = match query {
            QueryType::Vector(v) => v,
            QueryType::Text(text) => self
                .provider
                .embed(&text)
                .await
                .map_err(|e| StreamlineError::AI(format!("Failed to embed query: {}", e)))?,
        };

        let indexes = self.indexes.read().await;
        let mut all_results = Vec::new();

        for (topic, index) in indexes.iter() {
            if !topics.is_empty() && !topics.contains(topic) {
                continue;
            }

            if let Ok(results) = index.search(&query_vector, k).await {
                for result in results {
                    let score = 1.0 - result.distance;
                    if score >= min_score {
                        // The HNSW index stores records by offset (id) but not
                        // per-partition metadata. Until per-partition indexes
                        // are added, derive partition from the record offset
                        // distribution or default to 0.
                        all_results.push(SimilarityResult {
                            offset: result.id,
                            topic: topic.clone(),
                            partition: 0,
                            score,
                            key: None,
                            timestamp: 0,
                        });
                    }
                }
            }
        }

        // Sort by score descending and take top K
        all_results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all_results.truncate(k);

        self.stats.searches.fetch_add(1, Ordering::Relaxed);

        Ok(all_results)
    }

    /// Shutdown the manager
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        info!("Similarity stream manager shutting down");
    }

    /// Get statistics
    pub fn stats(&self) -> SimilarityStatsSnapshot {
        SimilarityStatsSnapshot {
            records_indexed: self.stats.records_indexed.load(Ordering::Relaxed),
            searches: self.stats.searches.load(Ordering::Relaxed),
            subscriptions_created: self.stats.subscriptions_created.load(Ordering::Relaxed),
            subscriptions_cancelled: self.stats.subscriptions_cancelled.load(Ordering::Relaxed),
            lagged_events: self.stats.lagged_events.load(Ordering::Relaxed),
        }
    }

    /// Get number of active subscriptions
    pub async fn active_subscriptions(&self) -> usize {
        self.subscriptions.read().await.len()
    }

    /// Get number of indexed records per topic
    pub async fn index_sizes(&self) -> HashMap<String, usize> {
        let indexes = self.indexes.read().await;
        let mut sizes = HashMap::new();
        for (topic, index) in indexes.iter() {
            sizes.insert(topic.clone(), index.len().await);
        }
        sizes
    }
}

impl PartialEq for SimilarityResult {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
            && self.topic == other.topic
            && self.partition == other.partition
    }
}

/// Internal statistics
#[derive(Debug, Default)]
struct SimilarityStats {
    records_indexed: AtomicU64,
    searches: AtomicU64,
    subscriptions_created: AtomicU64,
    subscriptions_cancelled: AtomicU64,
    lagged_events: AtomicU64,
}

/// Statistics snapshot
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SimilarityStatsSnapshot {
    /// Total records indexed
    pub records_indexed: u64,
    /// Total searches performed
    pub searches: u64,
    /// Total subscriptions created
    pub subscriptions_created: u64,
    /// Total subscriptions cancelled
    pub subscriptions_cancelled: u64,
    /// Total lagged events
    pub lagged_events: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::providers::MockProvider;

    #[tokio::test]
    async fn test_subscribe_and_unsubscribe() {
        let provider = Arc::new(MockProvider::new(64));
        let config = SimilarityConfig::default();
        let manager = SimilarityStreamManager::new(config, provider);

        let sub_config = SubscriptionConfig {
            query: QueryType::Vector(vec![0.0; 64]),
            k: 5,
            min_score: 0.5,
            topics: vec!["test".to_string()],
            update_interval_ms: None,
        };

        let (id, _rx) = manager.subscribe(sub_config).await.unwrap();
        assert_eq!(manager.active_subscriptions().await, 1);

        let removed = manager.unsubscribe(id).await;
        assert!(removed);
        assert_eq!(manager.active_subscriptions().await, 0);
    }

    #[tokio::test]
    async fn test_search() {
        let provider = Arc::new(MockProvider::new(64));
        let config = SimilarityConfig::default();
        let manager = SimilarityStreamManager::new(config, provider.clone());

        // Index some records
        let event1 = NewRecordEvent {
            topic: "test".to_string(),
            partition: 0,
            offset: 1,
            timestamp: 1000,
            key: Some("key1".to_string()),
            vector: provider.embed("hello").await.unwrap(),
        };
        manager.publish(event1.clone()).unwrap();
        manager.index_record(&event1).await.unwrap();

        let event2 = NewRecordEvent {
            topic: "test".to_string(),
            partition: 0,
            offset: 2,
            timestamp: 2000,
            key: Some("key2".to_string()),
            vector: provider.embed("world").await.unwrap(),
        };
        manager.publish(event2.clone()).unwrap();
        manager.index_record(&event2).await.unwrap();

        // Search
        let results = manager
            .search(
                QueryType::Text("hello".to_string()),
                &["test".to_string()],
                5,
                0.0,
            )
            .await
            .unwrap();

        assert!(!results.is_empty());
        assert_eq!(results[0].offset, 1); // Should match "hello" first
    }

    #[tokio::test]
    async fn test_index_sizes() {
        let provider = Arc::new(MockProvider::new(32));
        let config = SimilarityConfig::default();
        let manager = SimilarityStreamManager::new(config, provider);

        // Index records in different topics
        for i in 0..5 {
            let event = NewRecordEvent {
                topic: "topic1".to_string(),
                partition: 0,
                offset: i,
                timestamp: 1000 + i,
                key: None,
                vector: vec![0.0; 32],
            };
            manager.index_record(&event).await.unwrap();
        }

        for i in 0..3 {
            let event = NewRecordEvent {
                topic: "topic2".to_string(),
                partition: 0,
                offset: i,
                timestamp: 1000 + i,
                key: None,
                vector: vec![0.0; 32],
            };
            manager.index_record(&event).await.unwrap();
        }

        let sizes = manager.index_sizes().await;
        assert_eq!(sizes.get("topic1"), Some(&5));
        assert_eq!(sizes.get("topic2"), Some(&3));
    }

    #[tokio::test]
    async fn test_stats() {
        let provider = Arc::new(MockProvider::new(32));
        let config = SimilarityConfig::default();
        let manager = SimilarityStreamManager::new(config, provider);

        let event = NewRecordEvent {
            topic: "test".to_string(),
            partition: 0,
            offset: 1,
            timestamp: 1000,
            key: None,
            vector: vec![0.0; 32],
        };
        manager.index_record(&event).await.unwrap();

        let _ = manager
            .search(QueryType::Vector(vec![0.0; 32]), &[], 5, 0.0)
            .await;

        let stats = manager.stats();
        assert_eq!(stats.records_indexed, 1);
        assert_eq!(stats.searches, 1);
    }
}
