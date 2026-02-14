//! Feature Pipelines
//!
//! Streaming and batch transformations for feature computation.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Feature pipeline for streaming transformations
#[derive(Debug)]
pub struct FeaturePipeline {
    /// Pipeline ID
    pub id: String,
    /// Configuration
    pub config: PipelineConfig,
    /// Running state
    running: Arc<RwLock<bool>>,
}

impl FeaturePipeline {
    /// Create a new feature pipeline
    pub fn new(config: PipelineConfig) -> Result<Self> {
        let id = config.name.clone();
        Ok(Self {
            id,
            config,
            running: Arc::new(RwLock::new(false)),
        })
    }

    /// Start the pipeline
    pub async fn start(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            return Err(StreamlineError::Config(format!(
                "Pipeline {} is already running",
                self.id
            )));
        }
        *running = true;
        // In a real implementation, this would start the streaming job
        Ok(())
    }

    /// Stop the pipeline
    pub async fn stop(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if !*running {
            return Err(StreamlineError::Config(format!(
                "Pipeline {} is not running",
                self.id
            )));
        }
        *running = false;
        Ok(())
    }

    /// Check if pipeline is running
    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }
}

/// Pipeline configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Pipeline name
    pub name: String,
    /// Description
    pub description: Option<String>,
    /// Source topic
    pub source_topic: String,
    /// Target feature view
    pub target_view: String,
    /// Transformations
    pub transforms: Vec<TransformConfig>,
    /// Processing mode
    pub mode: ProcessingMode,
    /// Batch size (for micro-batch mode)
    pub batch_size: Option<usize>,
    /// Window configuration (for windowed aggregations)
    pub window: Option<WindowConfig>,
    /// Parallelism
    pub parallelism: usize,
    /// Enable checkpointing
    pub checkpointing: bool,
    /// Checkpoint interval in milliseconds
    pub checkpoint_interval_ms: u64,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            description: None,
            source_topic: String::new(),
            target_view: String::new(),
            transforms: Vec::new(),
            mode: ProcessingMode::Streaming,
            batch_size: None,
            window: None,
            parallelism: 1,
            checkpointing: true,
            checkpoint_interval_ms: 10000,
        }
    }
}

impl PipelineConfig {
    /// Create a new pipeline configuration
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set source topic
    pub fn with_source(mut self, topic: impl Into<String>) -> Self {
        self.source_topic = topic.into();
        self
    }

    /// Set target feature view
    pub fn with_target(mut self, view: impl Into<String>) -> Self {
        self.target_view = view.into();
        self
    }

    /// Add a transform
    pub fn with_transform(mut self, transform: TransformConfig) -> Self {
        self.transforms.push(transform);
        self
    }

    /// Set processing mode
    pub fn with_mode(mut self, mode: ProcessingMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set window configuration
    pub fn with_window(mut self, window: WindowConfig) -> Self {
        self.window = Some(window);
        self
    }

    /// Set parallelism
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }
}

/// Processing mode
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessingMode {
    /// Real-time streaming
    #[default]
    Streaming,
    /// Micro-batch processing
    MicroBatch,
    /// Batch processing
    Batch,
}

/// Transform configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformConfig {
    /// Transform name
    pub name: String,
    /// Transform type
    pub transform_type: TransformType,
    /// Input columns
    pub inputs: Vec<String>,
    /// Output column
    pub output: String,
    /// Parameters
    pub params: HashMap<String, serde_json::Value>,
}

impl TransformConfig {
    /// Create a new transform configuration
    pub fn new(name: impl Into<String>, transform_type: TransformType) -> Self {
        Self {
            name: name.into(),
            transform_type,
            inputs: Vec::new(),
            output: String::new(),
            params: HashMap::new(),
        }
    }

    /// Set input columns
    pub fn with_inputs(mut self, inputs: Vec<String>) -> Self {
        self.inputs = inputs;
        self
    }

    /// Set output column
    pub fn with_output(mut self, output: impl Into<String>) -> Self {
        self.output = output.into();
        self
    }

    /// Add a parameter
    pub fn with_param(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.params.insert(key.into(), value);
        self
    }
}

/// Transform type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransformType {
    /// Select columns
    Select,
    /// Filter rows
    Filter,
    /// Map values
    Map,
    /// Add computed column
    WithColumn,
    /// Drop columns
    Drop,
    /// Rename column
    Rename,
    /// Cast type
    Cast,
    /// Fill nulls
    FillNull,
    /// Apply aggregation
    Aggregate(AggregationType),
    /// Custom SQL expression
    SqlExpression,
    /// Custom UDF
    Udf(String),
    /// Normalize (z-score)
    Normalize,
    /// One-hot encode
    OneHotEncode,
    /// Bucketize
    Bucketize,
    /// Hash
    Hash,
    /// Embedding lookup
    EmbeddingLookup,
}

/// Aggregation type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregationType {
    /// Count
    Count,
    /// Sum
    Sum,
    /// Average
    Avg,
    /// Minimum
    Min,
    /// Maximum
    Max,
    /// Standard deviation
    StdDev,
    /// Variance
    Variance,
    /// First value
    First,
    /// Last value
    Last,
    /// Collect to list
    CollectList,
    /// Collect to set
    CollectSet,
    /// Approximate count distinct
    ApproxCountDistinct,
}

/// Window configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowConfig {
    /// Window type
    pub window_type: WindowType,
    /// Window duration in seconds
    pub duration_seconds: u64,
    /// Slide interval in seconds (for sliding windows)
    pub slide_seconds: Option<u64>,
    /// Allowed lateness in seconds
    pub allowed_lateness_seconds: u64,
    /// Timestamp column
    pub timestamp_column: String,
}

impl WindowConfig {
    /// Create a tumbling window
    pub fn tumbling(duration_seconds: u64) -> Self {
        Self {
            window_type: WindowType::Tumbling,
            duration_seconds,
            slide_seconds: None,
            allowed_lateness_seconds: 0,
            timestamp_column: "event_timestamp".to_string(),
        }
    }

    /// Create a sliding window
    pub fn sliding(duration_seconds: u64, slide_seconds: u64) -> Self {
        Self {
            window_type: WindowType::Sliding,
            duration_seconds,
            slide_seconds: Some(slide_seconds),
            allowed_lateness_seconds: 0,
            timestamp_column: "event_timestamp".to_string(),
        }
    }

    /// Create a session window
    pub fn session(gap_seconds: u64) -> Self {
        Self {
            window_type: WindowType::Session,
            duration_seconds: gap_seconds,
            slide_seconds: None,
            allowed_lateness_seconds: 0,
            timestamp_column: "event_timestamp".to_string(),
        }
    }

    /// Set allowed lateness
    pub fn with_allowed_lateness(mut self, seconds: u64) -> Self {
        self.allowed_lateness_seconds = seconds;
        self
    }

    /// Set timestamp column
    pub fn with_timestamp_column(mut self, column: impl Into<String>) -> Self {
        self.timestamp_column = column.into();
        self
    }
}

/// Window type
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowType {
    /// Tumbling window (non-overlapping)
    #[default]
    Tumbling,
    /// Sliding window (overlapping)
    Sliding,
    /// Session window (based on activity)
    Session,
    /// Global window (all data)
    Global,
}

/// Pipeline status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStatus {
    /// Pipeline ID
    pub id: String,
    /// Running state
    pub running: bool,
    /// Records processed
    pub records_processed: u64,
    /// Records output
    pub records_output: u64,
    /// Errors
    pub errors: u64,
    /// Last checkpoint timestamp
    pub last_checkpoint: Option<i64>,
    /// Current lag (records behind)
    pub lag: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pipeline_creation() {
        let config = PipelineConfig::new("test_pipeline")
            .with_source("user_events")
            .with_target("user_features")
            .with_transform(
                TransformConfig::new("select_fields", TransformType::Select)
                    .with_inputs(vec!["user_id".into(), "event_type".into()])
                    .with_output("selected"),
            )
            .with_parallelism(4);

        let pipeline = FeaturePipeline::new(config);
        assert!(pipeline.is_ok());
    }

    #[tokio::test]
    async fn test_pipeline_lifecycle() {
        let config = PipelineConfig::new("test_pipeline")
            .with_source("source")
            .with_target("target");

        let pipeline = FeaturePipeline::new(config).unwrap();

        assert!(!pipeline.is_running().await);

        pipeline.start().await.unwrap();
        assert!(pipeline.is_running().await);

        // Should fail to start again
        assert!(pipeline.start().await.is_err());

        pipeline.stop().await.unwrap();
        assert!(!pipeline.is_running().await);
    }

    #[test]
    fn test_window_config() {
        let tumbling = WindowConfig::tumbling(300);
        assert_eq!(tumbling.window_type, WindowType::Tumbling);
        assert_eq!(tumbling.duration_seconds, 300);
        assert!(tumbling.slide_seconds.is_none());

        let sliding = WindowConfig::sliding(300, 60);
        assert_eq!(sliding.window_type, WindowType::Sliding);
        assert_eq!(sliding.slide_seconds, Some(60));
    }

    #[test]
    fn test_transform_config() {
        let transform = TransformConfig::new("normalize_score", TransformType::Normalize)
            .with_inputs(vec!["score".into()])
            .with_output("score_normalized")
            .with_param("mean", serde_json::json!(0.5))
            .with_param("std", serde_json::json!(0.1));

        assert_eq!(transform.name, "normalize_score");
        assert_eq!(transform.inputs, vec!["score"]);
        assert_eq!(transform.output, "score_normalized");
        assert_eq!(transform.params.len(), 2);
    }
}
