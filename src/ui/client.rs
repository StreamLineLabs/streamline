//! HTTP client for communicating with the Streamline server.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Configuration for the Streamline client.
#[derive(Debug, Clone)]
pub struct StreamlineClientConfig {
    /// Base URL of the Streamline HTTP API.
    pub base_url: String,
    /// Request timeout in milliseconds.
    pub timeout_ms: u64,
    /// Cache TTL in milliseconds.
    pub cache_ttl_ms: u64,
}

impl Default for StreamlineClientConfig {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:9094".to_string(),
            timeout_ms: 5000,   // Reduced from 10s to 5s for better UX
            cache_ttl_ms: 2000, // 2 second cache TTL
        }
    }
}

/// A cached value with timestamp for TTL checking.
struct CachedValue<T> {
    value: T,
    cached_at: Instant,
}

impl<T: Clone> CachedValue<T> {
    fn new(value: T) -> Self {
        Self {
            value,
            cached_at: Instant::now(),
        }
    }

    fn get(&self, ttl: Duration) -> Option<T> {
        if self.cached_at.elapsed() < ttl {
            Some(self.value.clone())
        } else {
            None
        }
    }
}

/// Request cache for frequently accessed data.
struct RequestCache {
    cluster_overview: RwLock<Option<CachedValue<ClusterOverview>>>,
    topics: RwLock<Option<CachedValue<Vec<TopicInfo>>>>,
    consumer_groups: RwLock<Option<CachedValue<Vec<ConsumerGroupInfo>>>>,
    brokers: RwLock<Option<CachedValue<Vec<BrokerInfo>>>>,
}

impl RequestCache {
    fn new() -> Self {
        Self {
            cluster_overview: RwLock::new(None),
            topics: RwLock::new(None),
            consumer_groups: RwLock::new(None),
            brokers: RwLock::new(None),
        }
    }

    fn invalidate_all(&self) {
        *self.cluster_overview.write() = None;
        *self.topics.write() = None;
        *self.consumer_groups.write() = None;
        *self.brokers.write() = None;
    }
}

/// HTTP client for the Streamline API.
pub struct StreamlineClient {
    config: StreamlineClientConfig,
    client: reqwest::Client,
    cache: RequestCache,
}

impl StreamlineClient {
    /// Create a new Streamline client.
    pub fn new(config: StreamlineClientConfig) -> crate::error::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .build()
            .map_err(|e| crate::error::StreamlineError::Internal(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            config,
            client,
            cache: RequestCache::new(),
        })
    }

    /// Get the cache TTL duration.
    fn cache_ttl(&self) -> Duration {
        Duration::from_millis(self.config.cache_ttl_ms)
    }

    /// Invalidate all caches. Call after mutations (create/delete topic).
    pub fn invalidate_cache(&self) {
        self.cache.invalidate_all();
    }

    /// Get the cluster overview.
    pub async fn get_cluster_overview(&self) -> Result<ClusterOverview, ClientError> {
        // Check cache first
        {
            let cache = self.cache.cluster_overview.read();
            if let Some(ref cached) = *cache {
                if let Some(value) = cached.get(self.cache_ttl()) {
                    return Ok(value);
                }
            }
        }

        // Cache miss - fetch from server
        let url = format!("{}/api/v1/cluster", self.config.base_url);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let overview: ClusterOverview = response.json().await?;

        // Update cache
        {
            let mut cache = self.cache.cluster_overview.write();
            *cache = Some(CachedValue::new(overview.clone()));
        }

        Ok(overview)
    }

    /// List all topics.
    pub async fn list_topics(&self) -> Result<Vec<TopicInfo>, ClientError> {
        // Check cache first
        {
            let cache = self.cache.topics.read();
            if let Some(ref cached) = *cache {
                if let Some(value) = cached.get(self.cache_ttl()) {
                    return Ok(value);
                }
            }
        }

        // Cache miss - fetch from server
        let url = format!("{}/api/v1/topics", self.config.base_url);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let topics: Vec<TopicInfo> = response.json().await?;

        // Update cache
        {
            let mut cache = self.cache.topics.write();
            *cache = Some(CachedValue::new(topics.clone()));
        }

        Ok(topics)
    }

    /// Get topic details.
    pub async fn get_topic(&self, name: &str) -> Result<TopicDetails, ClientError> {
        let url = format!("{}/api/v1/topics/{}", self.config.base_url, name);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let topic = response.json().await?;
        Ok(topic)
    }

    /// Create a new topic.
    pub async fn create_topic(&self, request: CreateTopicRequest) -> Result<(), ClientError> {
        let url = format!("{}/api/v1/topics", self.config.base_url);
        let response = self.client.post(&url).json(&request).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        // Invalidate caches on mutation
        self.invalidate_cache();

        Ok(())
    }

    /// Delete a topic.
    pub async fn delete_topic(&self, name: &str) -> Result<(), ClientError> {
        let url = format!("{}/api/v1/topics/{}", self.config.base_url, name);
        let response = self.client.delete(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        // Invalidate caches on mutation
        self.invalidate_cache();

        Ok(())
    }

    /// List all consumer groups.
    pub async fn list_consumer_groups(&self) -> Result<Vec<ConsumerGroupInfo>, ClientError> {
        // Check cache first
        {
            let cache = self.cache.consumer_groups.read();
            if let Some(ref cached) = *cache {
                if let Some(value) = cached.get(self.cache_ttl()) {
                    return Ok(value);
                }
            }
        }

        // Cache miss - fetch from server
        let url = format!("{}/api/v1/consumer-groups", self.config.base_url);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let groups: Vec<ConsumerGroupInfo> = response.json().await?;

        // Update cache
        {
            let mut cache = self.cache.consumer_groups.write();
            *cache = Some(CachedValue::new(groups.clone()));
        }

        Ok(groups)
    }

    /// Get consumer group details.
    pub async fn get_consumer_group(
        &self,
        group_id: &str,
    ) -> Result<ConsumerGroupDetails, ClientError> {
        let url = format!(
            "{}/api/v1/consumer-groups/{}",
            self.config.base_url, group_id
        );
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let group = response.json().await?;
        Ok(group)
    }

    /// Get consumer group lag.
    pub async fn get_consumer_group_lag(
        &self,
        group_id: &str,
    ) -> Result<ConsumerGroupLag, ClientError> {
        let url = format!(
            "{}/api/v1/consumer-groups/{}/lag",
            self.config.base_url, group_id
        );
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let lag = response.json().await?;
        Ok(lag)
    }

    /// Reset consumer group offsets (dry run - preview only).
    pub async fn reset_offsets_dry_run(
        &self,
        group_id: &str,
        request: ResetOffsetsRequest,
    ) -> Result<ResetOffsetsResponse, ClientError> {
        let url = format!(
            "{}/api/v1/consumer-groups/{}/reset-offsets/dry-run",
            self.config.base_url, group_id
        );
        let response = self.client.post(&url).json(&request).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let result = response.json().await?;
        Ok(result)
    }

    /// Reset consumer group offsets (execute).
    pub async fn reset_offsets(
        &self,
        group_id: &str,
        request: ResetOffsetsRequest,
    ) -> Result<ResetOffsetsResponse, ClientError> {
        let url = format!(
            "{}/api/v1/consumer-groups/{}/reset-offsets",
            self.config.base_url, group_id
        );
        let response = self.client.post(&url).json(&request).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let result = response.json().await?;
        Ok(result)
    }

    /// List all brokers.
    pub async fn list_brokers(&self) -> Result<Vec<BrokerInfo>, ClientError> {
        // Check cache first
        {
            let cache = self.cache.brokers.read();
            if let Some(ref cached) = *cache {
                if let Some(value) = cached.get(self.cache_ttl()) {
                    return Ok(value);
                }
            }
        }

        // Cache miss - fetch from server
        let url = format!("{}/api/v1/brokers", self.config.base_url);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let brokers: Vec<BrokerInfo> = response.json().await?;

        // Update cache
        {
            let mut cache = self.cache.brokers.write();
            *cache = Some(CachedValue::new(brokers.clone()));
        }

        Ok(brokers)
    }

    /// Get Prometheus metrics.
    pub async fn get_metrics(&self) -> Result<String, ClientError> {
        let url = format!("{}/metrics", self.config.base_url);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let metrics = response.text().await?;
        Ok(metrics)
    }

    /// Health check.
    pub async fn health_check(&self) -> Result<HealthStatus, ClientError> {
        let url = format!("{}/health", self.config.base_url);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let health = response.json().await?;
        Ok(health)
    }

    /// Get consumer lag heatmap data.
    pub async fn get_heatmap(&self) -> Result<HeatmapData, ClientError> {
        let url = format!("{}/api/v1/consumer-groups/heatmap", self.config.base_url);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let heatmap = response.json().await?;
        Ok(heatmap)
    }

    /// Get historical metrics.
    pub async fn get_metrics_history(
        &self,
        duration: &str,
    ) -> Result<HistoricalMetrics, ClientError> {
        let url = format!(
            "{}/api/v1/metrics/history?duration={}",
            self.config.base_url, duration
        );
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let history = response.json().await?;
        Ok(history)
    }

    /// Browse messages from a topic partition.
    pub async fn browse_messages(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        limit: usize,
    ) -> Result<ConsumeResponse, ClientError> {
        let url = format!(
            "{}/api/v1/topics/{}/partitions/{}/messages?offset={}&limit={}",
            self.config.base_url, topic, partition, offset, limit
        );
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let consume_response = response.json().await?;
        Ok(consume_response)
    }

    /// Produce a message to a topic.
    pub async fn produce_message(
        &self,
        topic: &str,
        request: ProduceMessageRequest,
    ) -> Result<ProduceResponse, ClientError> {
        let url = format!("{}/api/v1/topics/{}/messages", self.config.base_url, topic);
        let response = self.client.post(&url).json(&request).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let produce_response = response.json().await?;
        Ok(produce_response)
    }

    /// Compare two topics.
    pub async fn compare_topics(
        &self,
        topic1: &str,
        topic2: &str,
    ) -> Result<TopicComparison, ClientError> {
        let url = format!(
            "{}/api/v1/topics/compare?topics={},{}",
            self.config.base_url, topic1, topic2
        );
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let comparison = response.json().await?;
        Ok(comparison)
    }

    /// List all benchmarks.
    pub async fn list_benchmarks(&self) -> Result<ListBenchmarksResponse, ClientError> {
        let url = format!("{}/api/v1/benchmark", self.config.base_url);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let benchmarks = response.json().await?;
        Ok(benchmarks)
    }

    /// Start a produce benchmark.
    pub async fn start_produce_benchmark(
        &self,
        config: BenchmarkConfig,
    ) -> Result<Benchmark, ClientError> {
        let url = format!("{}/api/v1/benchmark/produce", self.config.base_url);
        let response = self.client.post(&url).json(&config).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let benchmark = response.json().await?;
        Ok(benchmark)
    }

    /// Start a consume benchmark.
    pub async fn start_consume_benchmark(
        &self,
        config: BenchmarkConfig,
    ) -> Result<Benchmark, ClientError> {
        let url = format!("{}/api/v1/benchmark/consume", self.config.base_url);
        let response = self.client.post(&url).json(&config).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let benchmark = response.json().await?;
        Ok(benchmark)
    }

    /// Get benchmark status.
    pub async fn get_benchmark_status(&self, id: &str) -> Result<Benchmark, ClientError> {
        let url = format!("{}/api/v1/benchmark/{}/status", self.config.base_url, id);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let benchmark = response.json().await?;
        Ok(benchmark)
    }

    /// Get benchmark results.
    pub async fn get_benchmark_results(&self, id: &str) -> Result<BenchmarkResults, ClientError> {
        let url = format!("{}/api/v1/benchmark/{}/results", self.config.base_url, id);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let results = response.json().await?;
        Ok(results)
    }

    /// Get connection statistics.
    pub async fn get_connection_stats(&self) -> Result<ConnectionsStatsResponse, ClientError> {
        let url = format!("{}/api/v1/connections/stats", self.config.base_url);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let stats = response.json().await?;
        Ok(stats)
    }

    /// Get connections information.
    pub async fn get_connections(&self) -> Result<ConnectionsResponse, ClientError> {
        let url = format!("{}/api/v1/connections", self.config.base_url);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let connections = response.json().await?;
        Ok(connections)
    }

    /// Get log entries.
    pub async fn get_logs(
        &self,
        level: Option<&str>,
        limit: Option<usize>,
        after: Option<u64>,
    ) -> Result<LogsResponse, ClientError> {
        let mut url = format!("{}/api/v1/logs", self.config.base_url);
        let mut params = vec![];
        if let Some(l) = level {
            params.push(format!("level={}", l));
        }
        if let Some(lim) = limit {
            params.push(format!("limit={}", lim));
        }
        if let Some(a) = after {
            params.push(format!("after={}", a));
        }
        if !params.is_empty() {
            url = format!("{}?{}", url, params.join("&"));
        }

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let logs = response.json().await?;
        Ok(logs)
    }

    /// Search log entries.
    pub async fn search_logs(
        &self,
        query: &str,
        level: Option<&str>,
        limit: Option<usize>,
    ) -> Result<LogsResponse, ClientError> {
        let url = format!("{}/api/v1/logs/search", self.config.base_url);

        let mut request = self.client.get(&url).query(&[("q", query)]);
        if let Some(l) = level {
            request = request.query(&[("level", l)]);
        }
        if let Some(lim) = limit {
            request = request.query(&[("limit", &lim.to_string())]);
        }

        let response = request.send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let logs = response.json().await?;
        Ok(logs)
    }

    /// Clear log buffer.
    pub async fn clear_logs(&self) -> Result<(), ClientError> {
        let url = format!("{}/api/v1/logs", self.config.base_url);
        let response = self.client.delete(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        Ok(())
    }

    /// List all alerts.
    pub async fn list_alerts(&self) -> Result<ListAlertsResponse, ClientError> {
        let url = format!("{}/api/v1/alerts", self.config.base_url);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let alerts = response.json().await?;
        Ok(alerts)
    }

    /// Get alert statistics.
    pub async fn get_alert_stats(&self) -> Result<AlertStats, ClientError> {
        let url = format!("{}/api/v1/alerts/stats", self.config.base_url);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let stats = response.json().await?;
        Ok(stats)
    }

    /// Get alert history.
    pub async fn get_alert_history(
        &self,
        limit: Option<usize>,
    ) -> Result<AlertHistoryResponse, ClientError> {
        let mut url = format!("{}/api/v1/alerts/history", self.config.base_url);
        if let Some(lim) = limit {
            url = format!("{}?limit={}", url, lim);
        }

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let history = response.json().await?;
        Ok(history)
    }

    /// Get alert by ID.
    pub async fn get_alert(&self, id: &str) -> Result<AlertConfig, ClientError> {
        let url = format!("{}/api/v1/alerts/{}", self.config.base_url, id);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let alert = response.json().await?;
        Ok(alert)
    }

    /// Create a new alert.
    pub async fn create_alert(&self, config: AlertConfig) -> Result<AlertConfig, ClientError> {
        let url = format!("{}/api/v1/alerts", self.config.base_url);
        let response = self.client.post(&url).json(&config).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let alert = response.json().await?;
        Ok(alert)
    }

    /// Update an alert.
    pub async fn update_alert(
        &self,
        id: &str,
        config: AlertConfig,
    ) -> Result<AlertConfig, ClientError> {
        let url = format!("{}/api/v1/alerts/{}", self.config.base_url, id);
        let response = self.client.put(&url).json(&config).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let alert = response.json().await?;
        Ok(alert)
    }

    /// Toggle alert enabled state.
    pub async fn toggle_alert(&self, id: &str) -> Result<ToggleAlertResponse, ClientError> {
        let url = format!("{}/api/v1/alerts/{}/toggle", self.config.base_url, id);
        let response = self.client.post(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let result = response.json().await?;
        Ok(result)
    }

    /// Delete an alert.
    pub async fn delete_alert(&self, id: &str) -> Result<(), ClientError> {
        let url = format!("{}/api/v1/alerts/{}", self.config.base_url, id);
        let response = self.client.delete(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        Ok(())
    }

    /// Get a streaming logs response for SSE proxying.
    /// Returns the raw reqwest Response for streaming.
    pub async fn get_logs_stream(
        &self,
        level: Option<&str>,
    ) -> Result<reqwest::Response, ClientError> {
        let mut url = format!("{}/api/v1/logs/stream", self.config.base_url);
        if let Some(level) = level {
            url.push_str(&format!("?level={}", level));
        }

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ClientError::ApiError(format!(
                "HTTP {}: failed to start logs stream",
                response.status()
            )));
        }

        Ok(response)
    }
}

/// Errors from the Streamline client.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("API error: {0}")]
    ApiError(String),

    #[error("Parse error: {0}")]
    Parse(String),
}

// API response types

/// Cluster overview.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterOverview {
    pub cluster_id: String,
    pub controller_id: Option<u64>,
    pub broker_count: usize,
    pub topic_count: usize,
    pub partition_count: usize,
    pub online_partition_count: usize,
    pub offline_partition_count: usize,
    pub under_replicated_partitions: usize,
    pub messages_per_second: f64,
    pub bytes_in_per_second: f64,
    pub bytes_out_per_second: f64,
    pub total_messages: u64,
    pub total_bytes: u64,
}

impl Default for ClusterOverview {
    fn default() -> Self {
        Self {
            cluster_id: "streamline".to_string(),
            controller_id: Some(1),
            broker_count: 1,
            topic_count: 0,
            partition_count: 0,
            online_partition_count: 0,
            offline_partition_count: 0,
            under_replicated_partitions: 0,
            messages_per_second: 0.0,
            bytes_in_per_second: 0.0,
            bytes_out_per_second: 0.0,
            total_messages: 0,
            total_bytes: 0,
        }
    }
}

/// Topic information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicInfo {
    pub name: String,
    pub partition_count: usize,
    pub replication_factor: usize,
    pub is_internal: bool,
    pub total_messages: u64,
    pub total_bytes: u64,
}

/// Detailed topic information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicDetails {
    pub name: String,
    pub partition_count: usize,
    pub replication_factor: usize,
    pub is_internal: bool,
    pub partitions: Vec<PartitionInfo>,
    pub config: HashMap<String, String>,
    pub total_messages: u64,
    pub total_bytes: u64,
    pub messages_per_second: f64,
    pub bytes_per_second: f64,
}

/// Partition information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub partition_id: i32,
    pub leader: Option<u64>,
    pub replicas: Vec<u64>,
    pub isr: Vec<u64>,
    pub start_offset: i64,
    pub end_offset: i64,
    pub size_bytes: u64,
}

/// Request to create a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTopicRequest {
    pub name: String,
    pub partitions: usize,
    pub replication_factor: usize,
    #[serde(default)]
    pub config: HashMap<String, String>,
}

/// Consumer group information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupInfo {
    pub group_id: String,
    pub state: String,
    pub member_count: usize,
    pub coordinator: Option<u64>,
    pub protocol_type: String,
}

/// Detailed consumer group information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupDetails {
    pub group_id: String,
    pub state: String,
    pub protocol_type: String,
    pub protocol: String,
    pub coordinator: Option<u64>,
    pub members: Vec<ConsumerGroupMember>,
    pub offsets: Vec<ConsumerGroupOffset>,
}

/// Consumer group member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub assignments: Vec<MemberAssignment>,
}

/// Member assignment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberAssignment {
    pub topic: String,
    pub partitions: Vec<i32>,
}

/// Consumer group offset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupOffset {
    pub topic: String,
    pub partition: i32,
    pub committed_offset: i64,
    pub metadata: Option<String>,
}

/// Consumer group lag.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupLag {
    pub group_id: String,
    pub total_lag: i64,
    pub partitions: Vec<PartitionLag>,
}

/// Partition lag information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionLag {
    pub topic: String,
    pub partition: i32,
    pub committed_offset: i64,
    pub log_end_offset: i64,
    pub lag: i64,
    pub consumer_id: Option<String>,
}

/// Lag severity for heatmap coloring.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LagSeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// Heatmap cell data for a group/topic combination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeatmapCell {
    pub total_lag: i64,
    pub severity: LagSeverity,
    pub partition_count: usize,
}

/// Heatmap data response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeatmapData {
    pub groups: Vec<String>,
    pub topics: Vec<String>,
    pub cells: Vec<Vec<Option<HeatmapCell>>>,
}

/// Strategy for resetting consumer group offsets.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResetStrategy {
    Earliest,
    Latest,
    Timestamp,
    Offset,
}

/// Request to reset consumer group offsets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResetOffsetsRequest {
    pub topic: String,
    pub strategy: ResetStrategy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions: Option<Vec<i32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<i64>,
}

/// Single partition offset result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionOffsetResult {
    pub partition: i32,
    pub previous_offset: i64,
    pub new_offset: i64,
    pub error: Option<String>,
}

/// Response from offset reset operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResetOffsetsResponse {
    pub group_id: String,
    pub topic: String,
    pub strategy: String,
    pub dry_run: bool,
    pub partitions: Vec<PartitionOffsetResult>,
    pub success_count: usize,
    pub error_count: usize,
}

/// A single metrics data point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricPoint {
    /// Unix timestamp in milliseconds.
    pub timestamp: u64,
    /// Messages produced per second.
    pub messages_per_sec: f64,
    /// Bytes received per second.
    pub bytes_in_per_sec: f64,
    /// Bytes sent per second.
    pub bytes_out_per_sec: f64,
    /// Active connections count.
    pub connections: usize,
    /// Total topics count.
    pub topics: usize,
    /// Total partitions count.
    pub partitions: usize,
    /// Consumer group count.
    pub consumer_groups: usize,
    /// Total consumer lag.
    pub total_consumer_lag: i64,
}

/// Historical metrics response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalMetrics {
    /// Data points.
    pub points: Vec<MetricPoint>,
    /// Start time of the data range (Unix ms).
    pub start_time: u64,
    /// End time of the data range (Unix ms).
    pub end_time: u64,
    /// Sample interval in milliseconds.
    pub interval_ms: u64,
}

/// Request to produce a message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProduceMessageRequest {
    /// Records to produce.
    pub records: Vec<ProduceRecord>,
}

/// Single record to produce.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProduceRecord {
    /// Optional key (for partitioning).
    pub key: Option<String>,
    /// Message value.
    pub value: serde_json::Value,
    /// Optional partition (overrides key-based partitioning).
    pub partition: Option<i32>,
    /// Optional headers.
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
}

/// Produce response with offsets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProduceResponse {
    /// Results for each record.
    pub offsets: Vec<ProduceOffsetResult>,
}

/// Result for a single produced record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProduceOffsetResult {
    /// Partition the record was written to.
    pub partition: i32,
    /// Offset assigned to the record.
    pub offset: i64,
}

/// Consume response with messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumeResponse {
    /// Topic name.
    pub topic: String,
    /// Partition ID.
    pub partition: i32,
    /// Records returned.
    pub records: Vec<ConsumeRecord>,
    /// Next offset to fetch from.
    pub next_offset: i64,
}

/// Single consumed record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumeRecord {
    /// Record offset.
    pub offset: i64,
    /// Record timestamp (milliseconds).
    pub timestamp: i64,
    /// Record key (if present).
    pub key: Option<String>,
    /// Record value.
    pub value: serde_json::Value,
    /// Record headers.
    pub headers: std::collections::HashMap<String, String>,
}

/// Broker information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerInfo {
    pub node_id: u64,
    pub host: String,
    pub port: u16,
    pub rack: Option<String>,
    pub is_controller: bool,
    pub state: String,
}

/// Health status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: String,
    pub version: String,
    pub uptime_seconds: u64,
}

/// Topic comparison response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicComparison {
    /// Details for each topic being compared.
    pub topics: Vec<TopicDetails>,
    /// Configuration differences between topics.
    pub config_diff: Vec<ConfigDifference>,
    /// Metrics comparison.
    pub metrics_comparison: MetricsComparison,
}

/// Configuration difference between topics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDifference {
    /// Configuration key.
    pub key: String,
    /// Value for each topic (in same order as topics array).
    pub values: Vec<Option<String>>,
    /// Whether all values are the same.
    pub is_same: bool,
}

/// Metrics comparison between topics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsComparison {
    /// Topic with more messages.
    pub messages_leader: Option<String>,
    /// Topic with higher throughput.
    pub throughput_leader: Option<String>,
    /// Topic with more partitions.
    pub partitions_leader: Option<String>,
}

/// List benchmarks response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListBenchmarksResponse {
    /// All benchmarks.
    pub benchmarks: Vec<Benchmark>,
    /// Total count.
    pub total: usize,
}

/// Benchmark configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    /// Topic to benchmark.
    pub topic: String,
    /// Message size in bytes.
    #[serde(default = "default_message_size")]
    pub message_size: usize,
    /// Number of messages to send/receive.
    #[serde(default = "default_message_count")]
    pub message_count: u64,
    /// Rate limit in messages per second.
    #[serde(default)]
    pub rate_limit: Option<u64>,
    /// Number of partitions (produce only).
    #[serde(default)]
    pub num_partitions: Option<i32>,
    /// Consumer group (consume only).
    #[serde(default)]
    pub consumer_group: Option<String>,
}

fn default_message_size() -> usize {
    1024
}

fn default_message_count() -> u64 {
    10000
}

/// Benchmark type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum BenchmarkType {
    Produce,
    Consume,
}

/// Benchmark status.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", content = "error")]
pub enum BenchmarkStatus {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "running")]
    Running,
    #[serde(rename = "completed")]
    Completed,
    #[serde(rename = "failed")]
    Failed(String),
}

/// Benchmark progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkProgress {
    /// Messages processed so far.
    pub messages_processed: u64,
    /// Bytes processed so far.
    pub bytes_processed: u64,
    /// Elapsed time in milliseconds.
    pub elapsed_ms: u64,
    /// Percent complete (0-100).
    pub percent_complete: f64,
}

/// Benchmark results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResults {
    /// Total duration in milliseconds.
    pub duration_ms: u64,
    /// Total messages sent/received.
    pub messages_total: u64,
    /// Total bytes sent/received.
    pub bytes_total: u64,
    /// Throughput in messages per second.
    pub throughput_msg_sec: f64,
    /// Throughput in MB per second.
    pub throughput_mb_sec: f64,
    /// Latency p50 in milliseconds.
    pub latency_p50_ms: f64,
    /// Latency p95 in milliseconds.
    pub latency_p95_ms: f64,
    /// Latency p99 in milliseconds.
    pub latency_p99_ms: f64,
    /// Average latency in milliseconds.
    pub latency_avg_ms: f64,
    /// Number of errors.
    pub errors: u64,
}

/// Benchmark data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Benchmark {
    /// Unique benchmark ID.
    pub id: String,
    /// Benchmark configuration.
    pub config: BenchmarkConfig,
    /// Benchmark type.
    pub benchmark_type: BenchmarkType,
    /// Current status.
    pub status: BenchmarkStatus,
    /// Progress information.
    pub progress: BenchmarkProgress,
    /// Results (when completed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results: Option<BenchmarkResults>,
    /// Start timestamp (ms since epoch).
    pub started_at: u64,
    /// Completion timestamp (ms since epoch).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_config_default() {
        let config = StreamlineClientConfig::default();
        assert_eq!(config.base_url, "http://localhost:9094");
        assert_eq!(config.timeout_ms, 5000);
        assert_eq!(config.cache_ttl_ms, 2000);
    }

    #[test]
    fn test_cluster_overview_default() {
        let overview = ClusterOverview::default();
        assert_eq!(overview.cluster_id, "streamline");
        assert_eq!(overview.broker_count, 1);
    }

    #[test]
    fn test_client_error_display() {
        let err = ClientError::ApiError("test error".to_string());
        assert_eq!(err.to_string(), "API error: test error");
    }
}

// Connection and Log API types

/// Connection statistics response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionsStatsResponse {
    pub active_connections: u64,
    pub in_flight_requests: u64,
    pub uptime_seconds: f64,
    pub is_shutting_down: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shutdown_phase: Option<String>,
}

/// Connections response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionsResponse {
    pub stats: ConnectionsStatsResponse,
    pub note: String,
}

/// Log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub id: u64,
    pub timestamp: String,
    pub level: String,
    pub target: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<serde_json::Value>,
}

/// Logs response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogsResponse {
    pub entries: Vec<LogEntry>,
    pub total: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_id: Option<u64>,
}

// Alert API types

/// Alert configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertConfig {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub alert_type: String,
    pub condition: serde_json::Value,
    pub actions: Vec<serde_json::Value>,
    pub enabled: bool,
    pub created_at: u64,
    pub updated_at: u64,
}

/// Alert event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEvent {
    pub id: String,
    pub alert_id: String,
    pub timestamp: u64,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

/// Alert statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertStats {
    pub total_alerts: usize,
    pub enabled_alerts: usize,
    pub triggered_last_hour: usize,
    pub triggered_last_day: usize,
}

/// List alerts response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListAlertsResponse {
    pub alerts: Vec<AlertConfig>,
    pub total: usize,
}

/// Alert history response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertHistoryResponse {
    pub events: Vec<AlertEvent>,
    pub total: usize,
}

/// Toggle alert response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToggleAlertResponse {
    pub id: String,
    pub enabled: bool,
}

// Message template types

/// A saved message production template.
/// Note: This is primarily a client-side type stored in localStorage and serialized via JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct MessageTemplate {
    /// Unique template ID.
    pub id: String,
    /// Human-readable template name.
    pub name: String,
    /// Topic this template is for (empty = any topic).
    pub topic: String,
    /// Key template (may contain variables).
    pub key_template: Option<String>,
    /// Value template (may contain variables).
    pub value_template: String,
    /// Header templates.
    pub headers: std::collections::HashMap<String, String>,
    /// Description of the template.
    pub description: Option<String>,
    /// Created timestamp (ms since epoch).
    pub created_at: u64,
    /// Last used timestamp (ms since epoch).
    pub last_used_at: Option<u64>,
}

// Command palette search types

/// Search result type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SearchResultType {
    Topic,
    ConsumerGroup,
    Broker,
    Action,
}

/// A single search result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    /// Type of result.
    pub result_type: SearchResultType,
    /// Display name.
    pub name: String,
    /// URL to navigate to.
    pub url: String,
    /// Description or subtitle.
    pub description: String,
    /// Icon name (for UI rendering).
    pub icon: String,
}

/// Search response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    /// Search results.
    pub results: Vec<SearchResult>,
    /// Query that was searched.
    pub query: String,
}

// Audit log types for UI

/// Audit event type for display.
/// Note: Currently using String for event_type in AuditEventView for flexibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[allow(dead_code)]
pub enum AuditEventType {
    AuthSuccess,
    AuthFailure,
    AuthLogout,
    AclAllow,
    AclDeny,
    TopicCreate,
    TopicDelete,
    NodeJoin,
    NodeLeave,
    LeaderChange,
    ConfigChange,
    Connection,
}

/// Audit event for UI display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEventView {
    /// Timestamp in human-readable format.
    pub timestamp: String,
    /// Event type.
    pub event_type: String,
    /// User involved (if any).
    pub user: Option<String>,
    /// Resource affected.
    pub resource: Option<String>,
    /// Client IP address.
    pub client_ip: Option<String>,
    /// Additional details.
    pub details: String,
}

/// Audit log response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogResponse {
    /// Audit events.
    pub events: Vec<AuditEventView>,
    /// Total count.
    pub total: usize,
    /// Current page.
    pub page: usize,
    /// Page size.
    pub page_size: usize,
}

/// Time Machine timeline bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineBucket {
    /// Bucket start timestamp (ISO 8601).
    pub timestamp: String,
    /// Unix timestamp in milliseconds.
    pub timestamp_ms: i64,
    /// Message count in this bucket.
    pub count: usize,
    /// Start offset for this bucket.
    pub start_offset: i64,
    /// End offset for this bucket.
    pub end_offset: i64,
}

/// Time Machine timeline data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineData {
    /// Topic name.
    pub topic: String,
    /// Partition (if specified).
    pub partition: Option<i32>,
    /// Earliest timestamp (ISO 8601).
    pub start_timestamp: String,
    /// Latest timestamp (ISO 8601).
    pub end_timestamp: String,
    /// Earliest timestamp in milliseconds.
    pub start_timestamp_ms: i64,
    /// Latest timestamp in milliseconds.
    pub end_timestamp_ms: i64,
    /// Total message count.
    pub total_messages: usize,
    /// Time-bucketed message counts.
    pub buckets: Vec<TimelineBucket>,
    /// Bucket duration in milliseconds.
    pub bucket_duration_ms: i64,
}

/// DLQ error type classification.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub enum DlqErrorType {
    /// Message deserialization failed.
    DeserializationFailure,
    /// Schema validation or compatibility error.
    SchemaMismatch,
    /// Processing error in consumer.
    ProcessingError,
    /// Operation timed out.
    TimeoutError,
    /// Unknown or unclassified error.
    Unknown,
}

impl std::fmt::Display for DlqErrorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DlqErrorType::DeserializationFailure => write!(f, "Deserialization"),
            DlqErrorType::SchemaMismatch => write!(f, "Schema Mismatch"),
            DlqErrorType::ProcessingError => write!(f, "Processing Error"),
            DlqErrorType::TimeoutError => write!(f, "Timeout"),
            DlqErrorType::Unknown => write!(f, "Unknown"),
        }
    }
}

/// DLQ topic summary for dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqTopic {
    /// DLQ topic name.
    pub name: String,
    /// Source topic (original topic that failed).
    pub source_topic: Option<String>,
    /// Total messages in DLQ.
    pub message_count: usize,
    /// Messages by error type.
    pub error_counts: std::collections::HashMap<String, usize>,
    /// Recent spike detected.
    pub has_spike: bool,
    /// Last message timestamp.
    pub last_message_time: Option<String>,
}

/// DLQ message details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqMessage {
    /// Message offset in DLQ topic.
    pub offset: i64,
    /// Error type.
    pub error_type: String,
    /// Original source topic.
    pub source_topic: Option<String>,
    /// Original partition.
    pub source_partition: Option<i32>,
    /// Original offset.
    pub source_offset: Option<i64>,
    /// Timestamp when message was sent to DLQ.
    pub timestamp: String,
    /// Error details/reason.
    pub error_details: String,
    /// Original message key.
    pub original_key: Option<String>,
    /// Original message value (preview).
    pub original_value_preview: Option<String>,
}

/// DLQ stats response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqStats {
    /// Total DLQ topics.
    pub total_topics: usize,
    /// Total messages across all DLQs.
    pub total_messages: usize,
    /// Messages by error type.
    pub by_error_type: std::collections::HashMap<String, usize>,
    /// Messages by source topic.
    pub by_source: std::collections::HashMap<String, usize>,
    /// Recent activity (messages in last hour).
    pub recent_count: usize,
}

/// DLQ topics list response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqListResponse {
    /// DLQ topics.
    pub topics: Vec<DlqTopic>,
    /// Overall stats.
    pub stats: DlqStats,
}

// Consumer Lag History types

/// Lag velocity indicator showing trend direction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LagVelocity {
    /// Lag is increasing.
    Growing,
    /// Lag is stable.
    Stable,
    /// Lag is decreasing.
    Shrinking,
}

impl std::fmt::Display for LagVelocity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LagVelocity::Growing => write!(f, "Growing"),
            LagVelocity::Stable => write!(f, "Stable"),
            LagVelocity::Shrinking => write!(f, "Shrinking"),
        }
    }
}

/// A single point in lag history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LagHistoryPoint {
    /// Timestamp (ISO 8601).
    pub timestamp: String,
    /// Timestamp in milliseconds since epoch.
    pub timestamp_ms: i64,
    /// Lag value at this point.
    pub lag: i64,
    /// Velocity at this point.
    pub velocity: LagVelocity,
    /// Messages consumed per second.
    pub consume_rate: f64,
    /// Messages produced per second.
    pub produce_rate: f64,
}

/// Lag history for a single partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionLagHistory {
    /// Topic name.
    pub topic: String,
    /// Partition ID.
    pub partition: i32,
    /// Current lag.
    pub current_lag: i64,
    /// Current velocity.
    pub velocity: LagVelocity,
    /// History points (for sparkline).
    pub sparkline: Vec<i64>,
    /// Min lag in time range.
    pub min_lag: i64,
    /// Max lag in time range.
    pub max_lag: i64,
    /// Average lag in time range.
    pub avg_lag: f64,
}

/// Lag history response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LagHistoryResponse {
    /// Consumer group ID.
    pub group_id: String,
    /// Time range (e.g., "1h", "6h", "24h", "7d").
    pub range: String,
    /// Start timestamp (ISO 8601).
    pub start_timestamp: String,
    /// End timestamp (ISO 8601).
    pub end_timestamp: String,
    /// Start timestamp in ms.
    pub start_timestamp_ms: i64,
    /// End timestamp in ms.
    pub end_timestamp_ms: i64,
    /// Full history points for chart.
    pub history: Vec<LagHistoryPoint>,
    /// Per-partition history for sparklines.
    pub partitions: Vec<PartitionLagHistory>,
    /// Overall current lag.
    pub current_total_lag: i64,
    /// Overall velocity.
    pub overall_velocity: LagVelocity,
    /// Alert threshold (if set).
    pub alert_threshold: Option<i64>,
    /// Whether lag has exceeded threshold.
    pub threshold_exceeded: bool,
}

// Schema Evolution types

/// Schema format type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SchemaType {
    /// Apache Avro schema.
    Avro,
    /// Protocol Buffers schema.
    Protobuf,
    /// JSON Schema.
    #[serde(rename = "json")]
    JsonSchema,
}

impl std::fmt::Display for SchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaType::Avro => write!(f, "Avro"),
            SchemaType::Protobuf => write!(f, "Protobuf"),
            SchemaType::JsonSchema => write!(f, "JSON Schema"),
        }
    }
}

/// Schema compatibility level.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompatibilityLevel {
    /// No compatibility checking.
    None,
    /// New schema can read data produced by old schema.
    Backward,
    /// Backward compatible with all prior versions.
    BackwardTransitive,
    /// Old schema can read data produced by new schema.
    Forward,
    /// Forward compatible with all prior versions.
    ForwardTransitive,
    /// Both backward and forward compatible.
    Full,
    /// Full compatible with all prior versions.
    FullTransitive,
}

impl std::fmt::Display for CompatibilityLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompatibilityLevel::None => write!(f, "None"),
            CompatibilityLevel::Backward => write!(f, "Backward"),
            CompatibilityLevel::BackwardTransitive => write!(f, "Backward Transitive"),
            CompatibilityLevel::Forward => write!(f, "Forward"),
            CompatibilityLevel::ForwardTransitive => write!(f, "Forward Transitive"),
            CompatibilityLevel::Full => write!(f, "Full"),
            CompatibilityLevel::FullTransitive => write!(f, "Full Transitive"),
        }
    }
}

/// Schema subject summary for listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSubject {
    /// Subject name (e.g., "topic-value", "topic-key").
    pub name: String,
    /// Schema format type.
    pub schema_type: SchemaType,
    /// Latest version number.
    pub latest_version: i32,
    /// Compatibility level.
    pub compatibility: CompatibilityLevel,
    /// Topics using this schema.
    pub linked_topics: Vec<String>,
    /// Total number of versions.
    pub versions_count: i32,
}

/// Schema field change type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SchemaChangeType {
    /// Field was added.
    Added,
    /// Field was removed.
    Removed,
    /// Field was modified.
    Modified,
}

impl std::fmt::Display for SchemaChangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaChangeType::Added => write!(f, "Added"),
            SchemaChangeType::Removed => write!(f, "Removed"),
            SchemaChangeType::Modified => write!(f, "Modified"),
        }
    }
}

/// A single schema field change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChange {
    /// Type of change.
    pub change_type: SchemaChangeType,
    /// Field name or path.
    pub field_name: String,
    /// Human-readable description.
    pub description: String,
    /// Previous value (for modified/removed).
    pub old_value: Option<String>,
    /// New value (for modified/added).
    pub new_value: Option<String>,
}

/// Schema version details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    /// Version number.
    pub version: i32,
    /// Global schema ID.
    pub schema_id: i32,
    /// Schema definition.
    pub schema: String,
    /// Registration timestamp (ISO 8601).
    pub registered_at: String,
    /// Compatibility status for this version.
    pub compatibility_status: String,
    /// Changes from previous version.
    pub changes_summary: Vec<SchemaChange>,
}

/// Schema subject details with version history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSubjectDetails {
    /// Subject name.
    pub name: String,
    /// Schema format type.
    pub schema_type: SchemaType,
    /// Compatibility level.
    pub compatibility: CompatibilityLevel,
    /// All versions (newest first).
    pub versions: Vec<SchemaVersion>,
    /// Topics using this schema.
    pub linked_topics: Vec<String>,
}

/// Schema diff between two versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDiff {
    /// Subject name.
    pub subject: String,
    /// First version number.
    pub version1: i32,
    /// Second version number.
    pub version2: i32,
    /// First version schema.
    pub schema1: String,
    /// Second version schema.
    pub schema2: String,
    /// List of changes.
    pub changes: Vec<SchemaChange>,
    /// Are the versions compatible.
    pub is_compatible: bool,
}

/// Schema list response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaListResponse {
    /// Schema subjects.
    pub subjects: Vec<SchemaSubject>,
    /// Total count.
    pub total: usize,
}

// Multi-Cluster Management types

/// Cluster health status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ClusterHealth {
    /// All systems operational.
    Healthy,
    /// Some issues detected.
    Degraded,
    /// Cluster is down or unreachable.
    Unhealthy,
    /// Health status unknown.
    Unknown,
}

impl std::fmt::Display for ClusterHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterHealth::Healthy => write!(f, "Healthy"),
            ClusterHealth::Degraded => write!(f, "Degraded"),
            ClusterHealth::Unhealthy => write!(f, "Unhealthy"),
            ClusterHealth::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Cluster information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInfo {
    /// Unique cluster ID.
    pub id: String,
    /// Display name/alias.
    pub name: String,
    /// Cluster URL.
    pub url: String,
    /// Current health status.
    pub health: ClusterHealth,
    /// Streamline version.
    pub version: Option<String>,
    /// Number of brokers.
    pub broker_count: i32,
    /// Number of topics.
    pub topic_count: i32,
    /// Total messages across all topics.
    pub message_count: i64,
    /// Consumer groups count.
    pub consumer_group_count: i32,
    /// UI color for identification.
    pub color: Option<String>,
    /// When cluster was added.
    pub added_at: String,
    /// Last successful connection.
    pub last_seen: Option<String>,
    /// Is this the currently selected cluster.
    pub is_current: bool,
}

/// Request to add or update a cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Display name/alias.
    pub name: String,
    /// Cluster URL.
    pub url: String,
    /// UI color for identification.
    pub color: Option<String>,
}

/// Cluster list response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterListResponse {
    /// List of clusters.
    pub clusters: Vec<ClusterInfo>,
    /// Currently selected cluster ID.
    pub current_cluster_id: Option<String>,
}

/// Cluster health details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHealthDetails {
    /// Cluster ID.
    pub cluster_id: String,
    /// Health status.
    pub health: ClusterHealth,
    /// Broker statuses.
    pub brokers_healthy: i32,
    /// Total brokers.
    pub brokers_total: i32,
    /// Under-replicated partitions.
    pub under_replicated_partitions: i32,
    /// Active alerts.
    pub active_alerts: i32,
    /// Latest error message if any.
    pub error_message: Option<String>,
    /// Timestamp of health check.
    pub checked_at: String,
}

/// Cross-cluster comparison data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterComparison {
    /// Clusters being compared.
    pub clusters: Vec<ClusterHealthDetails>,
    /// Comparison timestamp.
    pub compared_at: String,
}

// ============================================================================
// Partition Rebalancing Types
// ============================================================================

/// Trigger reason for a consumer group rebalance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RebalanceTrigger {
    /// A new member joined the group.
    MemberJoin,
    /// A member left the group.
    MemberLeave,
    /// A member failed heartbeat check.
    HeartbeatTimeout,
    /// Topic metadata changed (partitions added/removed).
    TopicMetadataChange,
    /// Manual rebalance triggered.
    Manual,
}

/// Status of a rebalance operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RebalanceStatus {
    /// Rebalance is currently in progress.
    InProgress,
    /// Rebalance completed successfully.
    Completed,
    /// Rebalance failed.
    Failed,
    /// Rebalance is stuck (exceeded timeout threshold).
    Stuck,
}

/// State of a consumer group member.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MemberState {
    /// Member is stable with assigned partitions.
    Stable,
    /// Member is preparing for rebalance.
    PreparingRebalance,
    /// Member is completing rebalance.
    CompletingRebalance,
    /// Member is dead (no longer active).
    Dead,
}

/// A consumer group rebalance event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceEvent {
    /// Unique event ID.
    pub id: String,
    /// Consumer group ID.
    pub group_id: String,
    /// What triggered the rebalance.
    pub trigger: RebalanceTrigger,
    /// When the rebalance started.
    pub started_at: String,
    /// When the rebalance completed.
    pub completed_at: Option<String>,
    /// Duration in milliseconds.
    pub duration_ms: Option<i64>,
    /// Generation ID after rebalance.
    pub generation_id: i32,
    /// Previous generation ID.
    pub previous_generation: Option<i32>,
    /// Number of members before rebalance.
    pub members_before: i32,
    /// Number of members after rebalance.
    pub members_after: i32,
    /// Number of partitions that moved.
    pub partitions_moved: i32,
    /// Current status of the rebalance.
    pub status: RebalanceStatus,
}

/// A topic partition reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicPartition {
    /// Topic name.
    pub topic: String,
    /// Partition number.
    pub partition: i32,
}

/// A partition assignment to a consumer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionAssignment {
    /// Topic name.
    pub topic: String,
    /// Partition number.
    pub partition: i32,
    /// Consumer/member ID.
    pub consumer_id: String,
    /// Client ID.
    pub client_id: Option<String>,
    /// Host address.
    pub host: Option<String>,
}

/// A consumer group member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerMember {
    /// Member ID.
    pub member_id: String,
    /// Client ID.
    pub client_id: String,
    /// Host address.
    pub host: String,
    /// Assigned partitions.
    pub assigned_partitions: Vec<TopicPartition>,
    /// Session timeout in milliseconds.
    pub session_timeout_ms: i32,
    /// Heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: i32,
    /// Last heartbeat timestamp.
    pub last_heartbeat: Option<String>,
    /// Current state.
    pub state: MemberState,
}

/// A partition movement during rebalance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMove {
    /// Topic name.
    pub topic: String,
    /// Partition number.
    pub partition: i32,
    /// Previous consumer.
    pub from_consumer: String,
    /// New consumer.
    pub to_consumer: String,
}

/// Diff between two assignment generations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignmentDiff {
    /// Consumer group ID.
    pub group_id: String,
    /// Generation before.
    pub before_generation: i32,
    /// Generation after.
    pub after_generation: i32,
    /// Newly added assignments.
    pub added: Vec<PartitionAssignment>,
    /// Removed assignments.
    pub removed: Vec<PartitionAssignment>,
    /// Moved partitions.
    pub moved: Vec<PartitionMove>,
}

/// Rebalance events list response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceListResponse {
    /// List of rebalance events.
    pub events: Vec<RebalanceEvent>,
    /// Total count.
    pub total: i32,
}

/// Current assignment state for a consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupAssignmentState {
    /// Consumer group ID.
    pub group_id: String,
    /// Current generation ID.
    pub generation_id: i32,
    /// All members in the group.
    pub members: Vec<ConsumerMember>,
    /// Current rebalance status if any.
    pub rebalance_status: Option<RebalanceStatus>,
    /// Topics subscribed by this group.
    pub subscribed_topics: Vec<String>,
    /// Total partitions assigned.
    pub total_partitions: i32,
    /// Last updated timestamp.
    pub last_updated: String,
}
