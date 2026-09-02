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
//! - Sink implementations: Specific connectors (e.g., Serverless, Cloud Functions)
//!
//! ## Connector availability
//!
//! The **Iceberg** and **Delta Lake** connectors are unavailable in this
//! release: their upstream crates pull dependencies with unfixed CVSS 7.5
//! advisories (quick-xml < 0.41 — RUSTSEC-2026-0194 / RUSTSEC-2026-0195) and,
//! for Delta Lake, native-tls/OpenSSL. Creating them fails with an explicit
//! error.
//!
//! The **Serverless** and **Cloud Function** connectors require the
//! `serverless` Cargo feature, which is what brings in the HTTP client they
//! deliver through. In a build without it they are reported unavailable and
//! [`SinkManager::create_sink`] rejects them up front rather than registering a
//! sink that would fail on every delivery.
//!
//! See [`unavailable`] for the full rationale and
//! [`unavailable::is_available`] to query it programmatically.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use streamline::sink::{SinkManager, SinkConfig};
//!
//! let manager = SinkManager::new(topic_manager);
//! manager.create_sink(SinkConfig {
//!     name: "my-webhook-sink".to_string(),
//!     sink_type: SinkType::Serverless,
//!     topics: vec!["events".to_string()],
//!     config: serverless_config,
//! }).await?;
//! ```

pub mod config;
pub mod iceberg_catalog;
pub mod unavailable;

// Preserved upstream-backed implementations.
//
// `delta_backend` and `iceberg_backend` are never set by any feature or
// profile, so these modules are not compiled in any supported build. They are
// kept verbatim so the connectors can be restored without re-implementing them
// once `deltalake` / `iceberg` ship MSRV-compatible releases on
// quick-xml >= 0.41 without native-tls. See `unavailable` for details.
#[cfg(delta_backend)]
pub mod delta;

#[cfg(iceberg_backend)]
pub mod iceberg;

// The Serverless and Cloud Function connectors deliver over HTTP, and their
// HTTP client (`crate::http_client`, backed by reqwest) is only compiled when
// the `serverless` feature is on. Compiling the modules without it produced
// connectors that could be constructed and registered but failed on *every*
// delivery — the sink accepted records and dropped them. The modules are
// therefore gated on the same feature that makes them functional, which is also
// what `unavailable::is_available` reports.
#[cfg(feature = "serverless")]
pub mod serverless;

#[cfg(feature = "serverless")]
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

        // Reject connectors that cannot run in this build *before* any other
        // validation — duplicate names, topic existence, or config shape.
        //
        // Ordering matters. A caller who asks for a connector this binary was
        // not built with must be told that, not handed an unrelated
        // "topic does not exist" or "invalid config" error. It also guarantees
        // that no such sink is ever inserted into the registry: a sink that
        // registers successfully and then fails on every delivery silently
        // drops records.
        if let Some(err) = unavailable::unavailable_error(config.sink_type) {
            error!(
                sink = %config.name,
                sink_type = %config.sink_type,
                "Refusing to create sink: connector unavailable"
            );
            return Err(err);
        }

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
                .map_err(|_| StreamlineError::Sink(format!("Topic '{topic}' does not exist")))?;
        }

        // Create sink instance. `build_connector` fails closed for anything the
        // availability check above would have rejected, so a future reordering
        // cannot register an unusable sink.
        let sink = self.build_connector(&config)?;

        // Register sink
        self.sinks
            .insert(config.name.clone(), Arc::new(RwLock::new(sink)));

        info!(sink = %config.name, "Sink created successfully");
        Ok(())
    }

    /// Construct the connector for `config.sink_type`.
    ///
    /// Only the connectors this build actually contains have an arm here:
    /// `iceberg`/`delta` are fenced off by the never-enabled `iceberg_backend`
    /// and `delta_backend` cfgs, and `serverless`/`cloud_function` by the
    /// `serverless` feature that supplies their HTTP client.
    ///
    /// Everything else falls through to the availability error, so this is a
    /// second, independent gate: even if `create_sink`'s up-front check were
    /// removed, no unusable connector could be built and registered here.
    #[allow(
        unreachable_patterns,
        reason = "the catch-all is unreachable only in a build where every backend \
                  cfg and feature is on, which no supported configuration does"
    )]
    fn build_connector(&self, config: &SinkConfig) -> Result<Box<dyn SinkConnector>> {
        // Every arm yields a `Result`, and the catch-all yields `Err` rather
        // than returning, so the match is an ordinary expression in every
        // feature combination — including the default build, where the
        // catch-all is the only arm.
        match config.sink_type {
            #[cfg(iceberg_backend)]
            SinkType::Iceberg => {
                let iceberg_config = serde_json::from_value(config.config.clone())
                    .map_err(|e| StreamlineError::Sink(format!("Invalid Iceberg config: {e}")))?;
                Ok(Box::new(iceberg::IcebergSink::new(
                    config.name.clone(),
                    config.topics.clone(),
                    iceberg_config,
                    self.topic_manager.clone(),
                )?))
            }
            #[cfg(delta_backend)]
            SinkType::DeltaLake => {
                let delta_config = serde_json::from_value(config.config.clone()).map_err(|e| {
                    StreamlineError::Sink(format!("Invalid Delta Lake config: {e}"))
                })?;
                Ok(Box::new(delta::DeltaLakeSink::new(
                    config.name.clone(),
                    config.topics.clone(),
                    delta_config,
                    self.topic_manager.clone(),
                )?))
            }
            #[cfg(feature = "serverless")]
            SinkType::Serverless => {
                let serverless_config =
                    serde_json::from_value(config.config.clone()).map_err(|e| {
                        StreamlineError::Sink(format!("Invalid serverless config: {e}"))
                    })?;
                Ok(Box::new(serverless::ServerlessConnector::new(
                    config.name.clone(),
                    config.topics.clone(),
                    serverless_config,
                    self.topic_manager.clone(),
                )?))
            }
            #[cfg(feature = "serverless")]
            SinkType::CloudFunction => {
                let cloud_function_config =
                    serde_json::from_value(config.config.clone()).map_err(|e| {
                        StreamlineError::Sink(format!("Invalid cloud function config: {e}"))
                    })?;
                Ok(Box::new(cloud_functions::CloudFunctionConnector::new(
                    config.name.clone(),
                    config.topics.clone(),
                    cloud_function_config,
                    self.topic_manager.clone(),
                )?))
            }
            unsupported => Err(
                unavailable::unavailable_error(unsupported).unwrap_or_else(|| {
                    StreamlineError::Sink(format!(
                        "Sink connector '{unsupported}' is not available in this build"
                    ))
                }),
            ),
        }
    }

    /// Start a sink
    pub async fn start_sink(&self, name: &str) -> Result<()> {
        let sink_ref = self
            .sinks
            .get(name)
            .ok_or_else(|| StreamlineError::Sink(format!("Sink '{name}' not found")))?;

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
            .ok_or_else(|| StreamlineError::Sink(format!("Sink '{name}' not found")))?;

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
            .ok_or_else(|| StreamlineError::Sink(format!("Sink '{name}' not found")))?;

        info!(sink = %name, "Sink deleted");
        Ok(())
    }

    /// Get sink status
    pub async fn get_status(&self, name: &str) -> Result<SinkStatus> {
        let sink_ref = self
            .sinks
            .get(name)
            .ok_or_else(|| StreamlineError::Sink(format!("Sink '{name}' not found")))?;

        let sink = sink_ref.read().await;
        Ok(sink.status())
    }

    /// Get sink metrics
    pub async fn get_metrics(&self, name: &str) -> Result<SinkMetrics> {
        let sink_ref = self
            .sinks
            .get(name)
            .ok_or_else(|| StreamlineError::Sink(format!("Sink '{name}' not found")))?;

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
                errors.push(format!("Sink '{name}': {e}"));
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
