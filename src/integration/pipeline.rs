//! Feature Integration Pipeline
//!
//! Connects CDC, AI, and Lakehouse features into unified data pipelines.

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

/// Pipeline status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineStatus {
    /// Pipeline is created but not started
    Created,
    /// Pipeline is starting up
    Starting,
    /// Pipeline is running
    Running,
    /// Pipeline is paused
    Paused,
    /// Pipeline is stopping
    Stopping,
    /// Pipeline is stopped
    Stopped,
    /// Pipeline has encountered an error
    Failed,
}

impl std::fmt::Display for PipelineStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineStatus::Created => write!(f, "CREATED"),
            PipelineStatus::Starting => write!(f, "STARTING"),
            PipelineStatus::Running => write!(f, "RUNNING"),
            PipelineStatus::Paused => write!(f, "PAUSED"),
            PipelineStatus::Stopping => write!(f, "STOPPING"),
            PipelineStatus::Stopped => write!(f, "STOPPED"),
            PipelineStatus::Failed => write!(f, "FAILED"),
        }
    }
}

/// Pipeline processing stage
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PipelineStage {
    /// CDC source stage - captures database changes
    CdcSource {
        source_type: String,
        database: String,
        tables: Vec<String>,
    },
    /// AI enrichment stage - adds AI-generated metadata
    AiEnrichment {
        model: String,
        prompt: Option<String>,
    },
    /// AI classification stage - categorizes messages
    AiClassification {
        model: String,
        categories: Vec<String>,
    },
    /// AI anomaly detection stage
    AiAnomalyDetection { detector: String, sensitivity: f64 },
    /// AI embedding stage - generates vector embeddings
    AiEmbedding { model: String, dimension: usize },
    /// Transform stage - applies data transformations
    Transform {
        /// Transform expression (e.g., JSONPath, JQ)
        expression: String,
    },
    /// Filter stage - filters messages
    Filter {
        /// Filter predicate expression
        predicate: String,
    },
    /// Lakehouse sink stage - writes to lakehouse format
    LakehouseSink {
        destination: String,
        format: String,
        partition_by: Vec<String>,
    },
    /// Topic sink stage - writes to Streamline topic
    TopicSink { topic: String },
}

impl PipelineStage {
    /// Get stage type name
    pub fn stage_type(&self) -> &'static str {
        match self {
            PipelineStage::CdcSource { .. } => "cdc_source",
            PipelineStage::AiEnrichment { .. } => "ai_enrichment",
            PipelineStage::AiClassification { .. } => "ai_classification",
            PipelineStage::AiAnomalyDetection { .. } => "ai_anomaly_detection",
            PipelineStage::AiEmbedding { .. } => "ai_embedding",
            PipelineStage::Transform { .. } => "transform",
            PipelineStage::Filter { .. } => "filter",
            PipelineStage::LakehouseSink { .. } => "lakehouse_sink",
            PipelineStage::TopicSink { .. } => "topic_sink",
        }
    }
}

/// Pipeline metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PipelineMetrics {
    /// Total records processed
    pub records_processed: u64,
    /// Records processed per stage
    pub records_by_stage: HashMap<String, u64>,
    /// Total bytes processed
    pub bytes_processed: u64,
    /// Processing errors
    pub errors: u64,
    /// Errors by stage
    pub errors_by_stage: HashMap<String, u64>,
    /// Average latency in milliseconds
    pub avg_latency_ms: f64,
    /// P99 latency in milliseconds
    pub p99_latency_ms: f64,
    /// Records per second throughput
    pub throughput_rps: f64,
    /// Last processed timestamp
    pub last_processed_at: Option<DateTime<Utc>>,
    /// Pipeline started timestamp
    pub started_at: Option<DateTime<Utc>>,
}

/// Internal metrics for atomic updates
struct InternalMetrics {
    records_processed: AtomicU64,
    bytes_processed: AtomicU64,
    errors: AtomicU64,
}

impl Default for InternalMetrics {
    fn default() -> Self {
        Self {
            records_processed: AtomicU64::new(0),
            bytes_processed: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }
}

/// Pipeline configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Pipeline name
    pub name: String,
    /// Pipeline description
    pub description: Option<String>,
    /// Processing stages in order
    pub stages: Vec<PipelineStage>,
    /// Parallelism (number of concurrent workers)
    pub parallelism: usize,
    /// Batch size for processing
    pub batch_size: usize,
    /// Commit interval in milliseconds
    pub commit_interval_ms: u64,
    /// Error handling policy
    pub error_policy: ErrorPolicy,
    /// Enable dead letter queue for failed records
    pub dlq_enabled: bool,
    /// Dead letter topic name
    pub dlq_topic: Option<String>,
    /// Retry configuration
    pub retry_config: RetryConfig,
    /// Labels for organization
    pub labels: HashMap<String, String>,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            description: None,
            stages: Vec::new(),
            parallelism: 4,
            batch_size: 100,
            commit_interval_ms: 5000,
            error_policy: ErrorPolicy::default(),
            dlq_enabled: true,
            dlq_topic: None,
            retry_config: RetryConfig::default(),
            labels: HashMap::new(),
        }
    }
}

/// Error handling policy
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub enum ErrorPolicy {
    /// Continue processing, log errors
    #[default]
    Continue,
    /// Fail the pipeline on first error
    FailFast,
    /// Skip failed records and continue
    SkipAndLog,
    /// Send failed records to DLQ
    DeadLetter,
}

/// Retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum retry attempts
    pub max_attempts: usize,
    /// Initial delay in milliseconds
    pub initial_delay_ms: u64,
    /// Maximum delay in milliseconds
    pub max_delay_ms: u64,
    /// Delay multiplier for exponential backoff
    pub multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay_ms: 100,
            max_delay_ms: 30000,
            multiplier: 2.0,
        }
    }
}

/// Pipeline builder for fluent API
pub struct PipelineBuilder {
    config: PipelineConfig,
}

impl PipelineBuilder {
    /// Create a new pipeline builder
    pub fn new(name: &str) -> Self {
        Self {
            config: PipelineConfig {
                name: name.to_string(),
                ..Default::default()
            },
        }
    }

    /// Set pipeline description
    pub fn description(mut self, desc: &str) -> Self {
        self.config.description = Some(desc.to_string());
        self
    }

    /// Add CDC source stage
    pub fn cdc_source(mut self, source_type: &str, database: &str, tables: &[&str]) -> Self {
        self.config.stages.push(PipelineStage::CdcSource {
            source_type: source_type.to_string(),
            database: database.to_string(),
            tables: tables.iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    /// Add AI enrichment stage
    pub fn ai_enrichment(mut self, model: &str, prompt: Option<&str>) -> Self {
        self.config.stages.push(PipelineStage::AiEnrichment {
            model: model.to_string(),
            prompt: prompt.map(|s| s.to_string()),
        });
        self
    }

    /// Add AI classification stage
    pub fn ai_classification(mut self, model: &str, categories: &[&str]) -> Self {
        self.config.stages.push(PipelineStage::AiClassification {
            model: model.to_string(),
            categories: categories.iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    /// Add AI anomaly detection stage
    pub fn ai_anomaly_detection(mut self, detector: &str, sensitivity: f64) -> Self {
        self.config.stages.push(PipelineStage::AiAnomalyDetection {
            detector: detector.to_string(),
            sensitivity,
        });
        self
    }

    /// Add AI embedding stage
    pub fn ai_embedding(mut self, model: &str, dimension: usize) -> Self {
        self.config.stages.push(PipelineStage::AiEmbedding {
            model: model.to_string(),
            dimension,
        });
        self
    }

    /// Add transform stage
    pub fn transform(mut self, expression: &str) -> Self {
        self.config.stages.push(PipelineStage::Transform {
            expression: expression.to_string(),
        });
        self
    }

    /// Add filter stage
    pub fn filter(mut self, predicate: &str) -> Self {
        self.config.stages.push(PipelineStage::Filter {
            predicate: predicate.to_string(),
        });
        self
    }

    /// Add lakehouse sink stage
    pub fn lakehouse_sink(
        mut self,
        destination: &str,
        format: &str,
        partition_by: &[&str],
    ) -> Self {
        self.config.stages.push(PipelineStage::LakehouseSink {
            destination: destination.to_string(),
            format: format.to_string(),
            partition_by: partition_by.iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    /// Add topic sink stage
    pub fn topic_sink(mut self, topic: &str) -> Self {
        self.config.stages.push(PipelineStage::TopicSink {
            topic: topic.to_string(),
        });
        self
    }

    /// Set parallelism
    pub fn parallelism(mut self, n: usize) -> Self {
        self.config.parallelism = n;
        self
    }

    /// Set batch size
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Set commit interval
    pub fn commit_interval_ms(mut self, ms: u64) -> Self {
        self.config.commit_interval_ms = ms;
        self
    }

    /// Set error policy
    pub fn error_policy(mut self, policy: ErrorPolicy) -> Self {
        self.config.error_policy = policy;
        self
    }

    /// Enable dead letter queue
    pub fn enable_dlq(mut self, topic: &str) -> Self {
        self.config.dlq_enabled = true;
        self.config.dlq_topic = Some(topic.to_string());
        self
    }

    /// Add a label
    pub fn label(mut self, key: &str, value: &str) -> Self {
        self.config
            .labels
            .insert(key.to_string(), value.to_string());
        self
    }

    /// Build the pipeline
    pub fn build(self) -> Result<Pipeline> {
        if self.config.name.is_empty() {
            return Err(StreamlineError::Config("Pipeline name is required".into()));
        }
        if self.config.stages.is_empty() {
            return Err(StreamlineError::Config(
                "Pipeline must have at least one stage".into(),
            ));
        }

        Pipeline::new(self.config)
    }
}

/// Integration pipeline
pub struct Pipeline {
    /// Pipeline configuration
    config: PipelineConfig,
    /// Current status
    status: Arc<RwLock<PipelineStatus>>,
    /// Pipeline metrics
    metrics: Arc<InternalMetrics>,
    /// Shutdown signal sender
    shutdown_tx: broadcast::Sender<()>,
    /// Created timestamp
    created_at: DateTime<Utc>,
    /// Started timestamp
    started_at: Arc<RwLock<Option<DateTime<Utc>>>>,
}

impl Pipeline {
    /// Create a new pipeline
    pub fn new(config: PipelineConfig) -> Result<Self> {
        let (shutdown_tx, _) = broadcast::channel(1);

        Ok(Self {
            config,
            status: Arc::new(RwLock::new(PipelineStatus::Created)),
            metrics: Arc::new(InternalMetrics::default()),
            shutdown_tx,
            created_at: Utc::now(),
            started_at: Arc::new(RwLock::new(None)),
        })
    }

    /// Get pipeline name
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Get pipeline configuration
    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    /// Get current status
    pub async fn status(&self) -> PipelineStatus {
        *self.status.read().await
    }

    /// Get pipeline metrics
    pub fn metrics(&self) -> PipelineMetrics {
        PipelineMetrics {
            records_processed: self.metrics.records_processed.load(Ordering::Relaxed),
            records_by_stage: HashMap::new(), // Would be tracked per stage
            bytes_processed: self.metrics.bytes_processed.load(Ordering::Relaxed),
            errors: self.metrics.errors.load(Ordering::Relaxed),
            errors_by_stage: HashMap::new(),
            avg_latency_ms: 0.0, // Would need histogram for accurate tracking
            p99_latency_ms: 0.0,
            throughput_rps: 0.0,
            last_processed_at: None,
            started_at: None,
        }
    }

    /// Get pipeline stages
    pub fn stages(&self) -> &[PipelineStage] {
        &self.config.stages
    }

    /// Start the pipeline
    pub async fn start(&self) -> Result<()> {
        let mut status = self.status.write().await;

        match *status {
            PipelineStatus::Created | PipelineStatus::Stopped | PipelineStatus::Failed => {
                *status = PipelineStatus::Starting;
            }
            _ => {
                return Err(StreamlineError::Config(format!(
                    "Cannot start pipeline in {} state",
                    *status
                )));
            }
        }
        drop(status);

        // Set started timestamp
        *self.started_at.write().await = Some(Utc::now());

        // Start the processing loop
        let status_clone = self.status.clone();
        let config = self.config.clone();
        let metrics = self.metrics.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            // Update status to running
            *status_clone.write().await = PipelineStatus::Running;

            tracing::info!(
                pipeline = %config.name,
                stages = config.stages.len(),
                "Pipeline started"
            );

            // Main processing loop
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        tracing::info!(pipeline = %config.name, "Pipeline shutdown received");
                        break;
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                        // Simulated processing - in real implementation:
                        // 1. Read from CDC source
                        // 2. Process through each stage
                        // 3. Write to sink

                        // For demonstration, increment metrics
                        metrics.records_processed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            *status_clone.write().await = PipelineStatus::Stopped;
            tracing::info!(pipeline = %config.name, "Pipeline stopped");
        });

        Ok(())
    }

    /// Stop the pipeline
    pub async fn stop(&self) -> Result<()> {
        let mut status = self.status.write().await;

        match *status {
            PipelineStatus::Running | PipelineStatus::Paused => {
                *status = PipelineStatus::Stopping;
            }
            _ => {
                return Err(StreamlineError::Config(format!(
                    "Cannot stop pipeline in {} state",
                    *status
                )));
            }
        }
        drop(status);

        // Send shutdown signal
        let _ = self.shutdown_tx.send(());

        Ok(())
    }

    /// Pause the pipeline
    pub async fn pause(&self) -> Result<()> {
        let mut status = self.status.write().await;

        if *status != PipelineStatus::Running {
            return Err(StreamlineError::Config(format!(
                "Cannot pause pipeline in {} state",
                *status
            )));
        }

        *status = PipelineStatus::Paused;
        tracing::info!(pipeline = %self.config.name, "Pipeline paused");

        Ok(())
    }

    /// Resume the pipeline
    pub async fn resume(&self) -> Result<()> {
        let mut status = self.status.write().await;

        if *status != PipelineStatus::Paused {
            return Err(StreamlineError::Config(format!(
                "Cannot resume pipeline in {} state",
                *status
            )));
        }

        *status = PipelineStatus::Running;
        tracing::info!(pipeline = %self.config.name, "Pipeline resumed");

        Ok(())
    }

    /// Get pipeline info as JSON
    pub fn info(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.config.name,
            "description": self.config.description,
            "stages": self.config.stages.iter().map(|s| s.stage_type()).collect::<Vec<_>>(),
            "parallelism": self.config.parallelism,
            "batch_size": self.config.batch_size,
            "error_policy": format!("{:?}", self.config.error_policy),
            "dlq_enabled": self.config.dlq_enabled,
            "labels": self.config.labels,
            "created_at": self.created_at.to_rfc3339(),
            "metrics": {
                "records_processed": self.metrics.records_processed.load(Ordering::Relaxed),
                "bytes_processed": self.metrics.bytes_processed.load(Ordering::Relaxed),
                "errors": self.metrics.errors.load(Ordering::Relaxed),
            }
        })
    }
}

/// Pipeline manager for managing multiple pipelines
pub struct PipelineManager {
    /// Active pipelines
    pipelines: dashmap::DashMap<String, Arc<Pipeline>>,
}

impl PipelineManager {
    /// Create a new pipeline manager
    pub fn new() -> Self {
        Self {
            pipelines: dashmap::DashMap::new(),
        }
    }

    /// Create and register a new pipeline
    pub fn create_pipeline(&self, config: PipelineConfig) -> Result<Arc<Pipeline>> {
        let name = config.name.clone();

        if self.pipelines.contains_key(&name) {
            return Err(StreamlineError::Config(format!(
                "Pipeline '{}' already exists",
                name
            )));
        }

        let pipeline = Arc::new(Pipeline::new(config)?);
        self.pipelines.insert(name, pipeline.clone());

        Ok(pipeline)
    }

    /// Get a pipeline by name
    pub fn get_pipeline(&self, name: &str) -> Option<Arc<Pipeline>> {
        self.pipelines.get(name).map(|r| r.clone())
    }

    /// Remove a pipeline
    pub fn remove_pipeline(&self, name: &str) -> Option<Arc<Pipeline>> {
        self.pipelines.remove(name).map(|(_, v)| v)
    }

    /// List all pipelines
    pub fn list_pipelines(&self) -> Vec<Arc<Pipeline>> {
        self.pipelines.iter().map(|r| r.value().clone()).collect()
    }

    /// Start a pipeline by name
    pub async fn start_pipeline(&self, name: &str) -> Result<()> {
        let pipeline = self
            .get_pipeline(name)
            .ok_or_else(|| StreamlineError::Config(format!("Pipeline '{}' not found", name)))?;
        pipeline.start().await
    }

    /// Stop a pipeline by name
    pub async fn stop_pipeline(&self, name: &str) -> Result<()> {
        let pipeline = self
            .get_pipeline(name)
            .ok_or_else(|| StreamlineError::Config(format!("Pipeline '{}' not found", name)))?;
        pipeline.stop().await
    }

    /// Stop all pipelines
    pub async fn stop_all(&self) -> Result<()> {
        for entry in self.pipelines.iter() {
            if let Err(e) = entry.value().stop().await {
                tracing::warn!(
                    pipeline = %entry.key(),
                    error = %e,
                    "Failed to stop pipeline"
                );
            }
        }
        Ok(())
    }
}

impl Default for PipelineManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_builder() {
        let pipeline = PipelineBuilder::new("test-pipeline")
            .description("Test pipeline")
            .cdc_source("postgres", "mydb", &["users", "orders"])
            .ai_enrichment("gpt-4", Some("Enrich with sentiment"))
            .ai_classification("classifier", &["high_value", "low_value"])
            .lakehouse_sink("warehouse", "iceberg", &["date", "region"])
            .parallelism(8)
            .batch_size(500)
            .build();

        assert!(pipeline.is_ok());
        let pipeline = pipeline.unwrap();
        assert_eq!(pipeline.name(), "test-pipeline");
        assert_eq!(pipeline.stages().len(), 4);
    }

    #[test]
    fn test_pipeline_builder_empty_name() {
        let result = PipelineBuilder::new("")
            .cdc_source("postgres", "db", &["table"])
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_pipeline_builder_no_stages() {
        let result = PipelineBuilder::new("test").build();
        assert!(result.is_err());
    }

    #[test]
    fn test_pipeline_stage_types() {
        assert_eq!(
            PipelineStage::CdcSource {
                source_type: "pg".into(),
                database: "db".into(),
                tables: vec![]
            }
            .stage_type(),
            "cdc_source"
        );
        assert_eq!(
            PipelineStage::AiEnrichment {
                model: "gpt".into(),
                prompt: None
            }
            .stage_type(),
            "ai_enrichment"
        );
        assert_eq!(
            PipelineStage::LakehouseSink {
                destination: "wh".into(),
                format: "iceberg".into(),
                partition_by: vec![]
            }
            .stage_type(),
            "lakehouse_sink"
        );
    }

    #[test]
    fn test_pipeline_manager() {
        let manager = PipelineManager::new();

        let config = PipelineConfig {
            name: "test".to_string(),
            stages: vec![PipelineStage::TopicSink {
                topic: "output".to_string(),
            }],
            ..Default::default()
        };

        let pipeline = manager.create_pipeline(config);
        assert!(pipeline.is_ok());

        // Check we can get it back
        let retrieved = manager.get_pipeline("test");
        assert!(retrieved.is_some());

        // Check duplicate fails
        let config2 = PipelineConfig {
            name: "test".to_string(),
            stages: vec![PipelineStage::TopicSink {
                topic: "output".to_string(),
            }],
            ..Default::default()
        };
        let dup = manager.create_pipeline(config2);
        assert!(dup.is_err());
    }

    #[tokio::test]
    async fn test_pipeline_lifecycle() {
        let pipeline = PipelineBuilder::new("lifecycle-test")
            .topic_sink("output")
            .build()
            .unwrap();

        assert_eq!(pipeline.status().await, PipelineStatus::Created);

        // Start
        pipeline.start().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        assert_eq!(pipeline.status().await, PipelineStatus::Running);

        // Pause
        pipeline.pause().await.unwrap();
        assert_eq!(pipeline.status().await, PipelineStatus::Paused);

        // Resume
        pipeline.resume().await.unwrap();
        assert_eq!(pipeline.status().await, PipelineStatus::Running);

        // Stop
        pipeline.stop().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        assert_eq!(pipeline.status().await, PipelineStatus::Stopped);
    }
}
