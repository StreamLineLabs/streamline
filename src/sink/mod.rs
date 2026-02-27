//! Sink connector system for Streamline
//!
//! This module provides a framework for streaming data from Streamline topics
//! to external data sinks like data lakehouses, data warehouses, and object storage.
//!
//! # Stability
//!
//! **⚠️ Experimental** - This module is under active development. The API and
//! configuration format may change significantly in any version. Use with caution
//! in production environments.
//!
//! ## Architecture
//!
//! The sink system consists of:
//! - [`SinkConnector`] trait: Defines the interface for sink implementations
//! - [`SinkManager`]: Manages multiple sink instances and their lifecycle
//! - Sink implementations: Specific connectors (e.g., Iceberg, Parquet, Delta Lake)
//!
//! ## Usage
//!
//! ```rust,ignore
//! use streamline::sink::{SinkManager, SinkConfig};
//!
//! let manager = SinkManager::new();
//! manager.create_sink(SinkConfig {
//!     name: "my-iceberg-sink".to_string(),
//!     sink_type: SinkType::Iceberg,
//!     topics: vec!["events".to_string()],
//!     config: IcebergSinkConfig { /* ... */ },
//! }).await?;
//! ```

pub mod config;
pub mod iceberg_catalog;

#[cfg(feature = "delta-lake")]
pub mod delta;

#[cfg(feature = "iceberg")]
pub mod iceberg;

#[allow(dead_code)]
pub mod serverless;

#[allow(dead_code)]
pub mod cloud_functions;

pub mod triggers;

use crate::error::{Result, StreamlineError};
use crate::storage::{Record, TopicManager};
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

pub use config::{SinkConfig, SinkType};
pub use iceberg_catalog::{
    CatalogStats, CatalogStatsSnapshot, IcebergCatalog, IcebergCatalogConfig, IcebergField,
    IcebergNamespace, IcebergSchema, IcebergSnapshot, IcebergTableMetadata, PartitionField,
};

/// Trait for sink connector implementations
///
/// A sink connector consumes records from Streamline topics and writes them
/// to an external data sink. Implementations must handle:
/// - Record format conversion
/// - Batching and buffering
/// - Error handling and retries
/// - State management and checkpointing
#[async_trait]
pub trait SinkConnector: Send + Sync {
    /// Get the sink name
    fn name(&self) -> &str;

    /// Get the sink type
    fn sink_type(&self) -> SinkType;

    /// Get the list of topics this sink consumes from
    fn topics(&self) -> Vec<String>;

    /// Start the sink connector
    ///
    /// This method should start background tasks to consume from topics
    /// and write to the sink. It should return immediately and not block.
    async fn start(&mut self) -> Result<()>;

    /// Stop the sink connector
    ///
    /// This method should gracefully stop the sink, flushing any pending
    /// writes and cleaning up resources.
    async fn stop(&mut self) -> Result<()>;

    /// Process a batch of records
    ///
    /// This method is called by the consumer loop with batches of records
    /// from the subscribed topics. Implementations should process the batch
    /// and return the highest offset successfully written.
    async fn process_batch(
        &mut self,
        topic: &str,
        partition: i32,
        records: Vec<Record>,
    ) -> Result<i64>;

    /// Get the current status of the sink
    fn status(&self) -> SinkStatus;

    /// Get sink metrics
    fn metrics(&self) -> SinkMetrics;

    /// Flush any pending writes without stopping the sink
    ///
    /// Default implementation is a no-op. Sink implementations should
    /// override this to flush their internal buffers.
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    /// Check the health of the sink connector
    ///
    /// Returns Ok(true) if the sink is healthy, Ok(false) if degraded,
    /// or Err if the health check itself fails.
    async fn health_check(&self) -> Result<bool> {
        Ok(self.status() == SinkStatus::Running)
    }
}

/// Status of a sink connector
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum SinkStatus {
    /// Sink is stopped
    Stopped,
    /// Sink is starting
    Starting,
    /// Sink is running normally
    Running,
    /// Sink is paused
    Paused,
    /// Sink has encountered an error
    Failed,
    /// Sink is stopping
    Stopping,
}

impl std::fmt::Display for SinkStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SinkStatus::Stopped => write!(f, "STOPPED"),
            SinkStatus::Starting => write!(f, "STARTING"),
            SinkStatus::Running => write!(f, "RUNNING"),
            SinkStatus::Paused => write!(f, "PAUSED"),
            SinkStatus::Failed => write!(f, "FAILED"),
            SinkStatus::Stopping => write!(f, "STOPPING"),
        }
    }
}

/// Metrics for a sink connector
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SinkMetrics {
    /// Total records processed
    pub records_processed: u64,
    /// Total records failed
    pub records_failed: u64,
    /// Total bytes processed
    pub bytes_processed: u64,
    /// Current lag (records behind)
    pub lag_records: u64,
    /// Last successful commit timestamp (ms since epoch)
    pub last_commit_timestamp: i64,
    /// Last committed offset by topic-partition
    pub committed_offsets: std::collections::HashMap<String, i64>,
    /// Error message if status is Failed
    pub error_message: Option<String>,
    /// Total number of successful commits
    pub commit_count: u64,
    /// Last commit latency in milliseconds
    pub last_commit_latency_ms: u64,
    /// Total error count (transient + permanent)
    pub error_count: u64,
}

/// Manager for sink connectors
pub struct SinkManager {
    /// Map of sink name to sink connector
    #[allow(clippy::type_complexity)]
    sinks: Arc<DashMap<String, Arc<RwLock<Box<dyn SinkConnector>>>>>,
    /// Topic manager for reading records
    topic_manager: Arc<TopicManager>,
}

impl SinkManager {
    /// Create a new sink manager
    pub fn new(topic_manager: Arc<TopicManager>) -> Self {
        Self {
            sinks: Arc::new(DashMap::new()),
            topic_manager,
        }
    }

    /// Create and register a new sink
    pub async fn create_sink(&self, config: SinkConfig) -> Result<()> {
        info!(sink = %config.name, sink_type = ?config.sink_type, "Creating sink");

        // Check if sink already exists
        if self.sinks.contains_key(&config.name) {
            return Err(StreamlineError::Sink(format!(
                "Sink '{}' already exists",
                config.name
            )));
        }

        // Validate topics exist
        for topic in &config.topics {
            self.topic_manager
                .get_topic_metadata(topic)
                .map_err(|_| StreamlineError::Sink(format!("Topic '{}' does not exist", topic)))?;
        }

        // Create sink instance
        let sink: Box<dyn SinkConnector> = match config.sink_type {
            #[cfg(feature = "iceberg")]
            SinkType::Iceberg => {
                let iceberg_config = serde_json::from_value(config.config.clone())
                    .map_err(|e| StreamlineError::Sink(format!("Invalid Iceberg config: {}", e)))?;
                Box::new(iceberg::IcebergSink::new(
                    config.name.clone(),
                    config.topics.clone(),
                    iceberg_config,
                    self.topic_manager.clone(),
                )?)
            }
            #[cfg(not(feature = "iceberg"))]
            SinkType::Iceberg => {
                return Err(StreamlineError::Sink(
                    "Iceberg sink not available. Compile with --features iceberg".to_string(),
                ));
            }
            #[cfg(feature = "delta-lake")]
            SinkType::DeltaLake => {
                let delta_config = serde_json::from_value(config.config.clone()).map_err(|e| {
                    StreamlineError::Sink(format!("Invalid Delta Lake config: {}", e))
                })?;
                Box::new(delta::DeltaLakeSink::new(
                    config.name.clone(),
                    config.topics.clone(),
                    delta_config,
                    self.topic_manager.clone(),
                )?)
            }
            #[cfg(not(feature = "delta-lake"))]
            SinkType::DeltaLake => {
                return Err(StreamlineError::Sink(
                    "Delta Lake sink not available. Compile with --features delta-lake".to_string(),
                ));
            }
            SinkType::Serverless => {
                let serverless_config =
                    serde_json::from_value(config.config.clone()).map_err(|e| {
                        StreamlineError::Sink(format!("Invalid serverless config: {}", e))
                    })?;
                Box::new(serverless::ServerlessConnector::new(
                    config.name.clone(),
                    config.topics.clone(),
                    serverless_config,
                    self.topic_manager.clone(),
                )?)
            }
            SinkType::CloudFunction => {
                let cloud_function_config =
                    serde_json::from_value(config.config.clone()).map_err(|e| {
                        StreamlineError::Sink(format!("Invalid cloud function config: {}", e))
                    })?;
                Box::new(cloud_functions::CloudFunctionConnector::new(
                    config.name.clone(),
                    config.topics.clone(),
                    cloud_function_config,
                    self.topic_manager.clone(),
                )?)
            }
        };

        // Register sink
        self.sinks
            .insert(config.name.clone(), Arc::new(RwLock::new(sink)));

        info!(sink = %config.name, "Sink created successfully");
        Ok(())
    }

    /// Start a sink
    pub async fn start_sink(&self, name: &str) -> Result<()> {
        let sink_ref = self
            .sinks
            .get(name)
            .ok_or_else(|| StreamlineError::Sink(format!("Sink '{}' not found", name)))?;

        let mut sink = sink_ref.write().await;
        sink.start().await?;

        info!(sink = %name, "Sink started");
        Ok(())
    }

    /// Stop a sink
    pub async fn stop_sink(&self, name: &str) -> Result<()> {
        let sink_ref = self
            .sinks
            .get(name)
            .ok_or_else(|| StreamlineError::Sink(format!("Sink '{}' not found", name)))?;

        let mut sink = sink_ref.write().await;
        sink.stop().await?;

        info!(sink = %name, "Sink stopped");
        Ok(())
    }

    /// Delete a sink
    pub async fn delete_sink(&self, name: &str) -> Result<()> {
        // Stop the sink first if it's running
        if let Some(sink_ref) = self.sinks.get(name) {
            let mut sink = sink_ref.write().await;
            let status = sink.status();
            if status == SinkStatus::Running {
                warn!(sink = %name, "Stopping running sink before deletion");
                sink.stop().await?;
            }
        }

        // Remove from registry
        self.sinks
            .remove(name)
            .ok_or_else(|| StreamlineError::Sink(format!("Sink '{}' not found", name)))?;

        info!(sink = %name, "Sink deleted");
        Ok(())
    }

    /// Get sink status
    pub async fn get_status(&self, name: &str) -> Result<SinkStatus> {
        let sink_ref = self
            .sinks
            .get(name)
            .ok_or_else(|| StreamlineError::Sink(format!("Sink '{}' not found", name)))?;

        let sink = sink_ref.read().await;
        Ok(sink.status())
    }

    /// Get sink metrics
    pub async fn get_metrics(&self, name: &str) -> Result<SinkMetrics> {
        let sink_ref = self
            .sinks
            .get(name)
            .ok_or_else(|| StreamlineError::Sink(format!("Sink '{}' not found", name)))?;

        let sink = sink_ref.read().await;
        Ok(sink.metrics())
    }

    /// List all sinks
    pub fn list_sinks(&self) -> Vec<String> {
        self.sinks.iter().map(|e| e.key().clone()).collect()
    }

    /// Get detailed information about all sinks
    pub async fn list_sinks_detailed(&self) -> Vec<SinkInfo> {
        let mut infos = Vec::new();
        for entry in self.sinks.iter() {
            let name = entry.key().clone();
            let sink = entry.value().read().await;
            infos.push(SinkInfo {
                name: name.clone(),
                sink_type: sink.sink_type(),
                topics: sink.topics(),
                status: sink.status(),
                metrics: sink.metrics(),
            });
        }
        infos
    }

    /// Run health checks on all sinks and return results
    pub async fn health_check_all(&self) -> Vec<(String, bool)> {
        let mut results = Vec::new();
        for entry in self.sinks.iter() {
            let name = entry.key().clone();
            let sink = entry.value().read().await;
            let healthy = sink.health_check().await.unwrap_or(false);
            results.push((name, healthy));
        }
        results
    }

    /// Shutdown all sinks
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down all sinks");
        let mut errors = Vec::new();

        for entry in self.sinks.iter() {
            let name = entry.key();
            debug!(sink = %name, "Stopping sink");
            let mut sink = entry.value().write().await;
            if let Err(e) = sink.stop().await {
                error!(sink = %name, error = %e, "Error stopping sink");
                errors.push(format!("Sink '{}': {}", name, e));
            }
        }

        if !errors.is_empty() {
            return Err(StreamlineError::Sink(format!(
                "Errors during shutdown: {}",
                errors.join(", ")
            )));
        }

        info!("All sinks stopped successfully");
        Ok(())
    }
}

/// Detailed information about a sink
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkInfo {
    /// Sink name
    pub name: String,
    /// Sink type
    pub sink_type: SinkType,
    /// Topics being consumed
    pub topics: Vec<String>,
    /// Current status
    pub status: SinkStatus,
    /// Metrics
    pub metrics: SinkMetrics,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_sink_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let manager = SinkManager::new(topic_manager);
        assert_eq!(manager.list_sinks().len(), 0);
    }

    #[test]
    fn test_sink_status_display() {
        assert_eq!(format!("{}", SinkStatus::Running), "RUNNING");
        assert_eq!(format!("{}", SinkStatus::Failed), "FAILED");
        assert_eq!(format!("{}", SinkStatus::Stopped), "STOPPED");
    }

    #[test]
    fn test_sink_metrics_default() {
        let metrics = SinkMetrics::default();
        assert_eq!(metrics.records_processed, 0);
        assert_eq!(metrics.records_failed, 0);
        assert_eq!(metrics.bytes_processed, 0);
        assert_eq!(metrics.commit_count, 0);
        assert_eq!(metrics.last_commit_latency_ms, 0);
        assert_eq!(metrics.error_count, 0);
    }
}
