//! Pipeline Execution Engine
//!
//! Provides pipeline building and execution for stream processing DSL.

use super::operators::{
    AggregateFunction, AggregateOperator, Expression, FilterOperator, MapOperator, OperatorChain,
    ProjectOperator,
};
use super::parser::{DslParser, ParsedQuery, SelectField};
use super::window::{create_window, Window, WindowConfig, WindowType};
use super::{DslContext, DslError, DslRecord, DslResult};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Pipeline configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Pipeline name
    pub name: String,
    /// Source topic
    pub source_topic: String,
    /// Sink topic (optional)
    pub sink_topic: Option<String>,
    /// Batch size for processing
    pub batch_size: usize,
    /// Processing interval in milliseconds
    pub processing_interval_ms: u64,
    /// Window configuration
    pub window: Option<WindowConfig>,
    /// Enable checkpointing
    pub checkpointing: bool,
    /// Checkpoint interval in milliseconds
    pub checkpoint_interval_ms: u64,
    /// Maximum records in flight
    pub max_in_flight: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            source_topic: "".to_string(),
            sink_topic: None,
            batch_size: 1000,
            processing_interval_ms: 100,
            window: None,
            checkpointing: false,
            checkpoint_interval_ms: 30000,
            max_in_flight: 10000,
        }
    }
}

/// Pipeline state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineState {
    Created,
    Running,
    Paused,
    Stopped,
    Failed,
}

/// Pipeline metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PipelineMetrics {
    /// Records processed
    pub records_processed: u64,
    /// Records emitted
    pub records_emitted: u64,
    /// Records filtered
    pub records_filtered: u64,
    /// Errors encountered
    pub errors: u64,
    /// Average processing latency (microseconds)
    pub avg_latency_us: u64,
    /// Windows emitted
    pub windows_emitted: u64,
    /// Current watermark
    pub watermark: i64,
}

/// Pipeline statistics (atomic version for concurrent access)
pub struct PipelineStats {
    records_processed: AtomicU64,
    records_emitted: AtomicU64,
    records_filtered: AtomicU64,
    errors: AtomicU64,
    total_latency_us: AtomicU64,
    windows_emitted: AtomicU64,
}

impl Default for PipelineStats {
    fn default() -> Self {
        Self {
            records_processed: AtomicU64::new(0),
            records_emitted: AtomicU64::new(0),
            records_filtered: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            windows_emitted: AtomicU64::new(0),
        }
    }
}

impl PipelineStats {
    pub fn snapshot(&self, watermark: i64) -> PipelineMetrics {
        let processed = self.records_processed.load(Ordering::Relaxed);
        let total_latency = self.total_latency_us.load(Ordering::Relaxed);

        PipelineMetrics {
            records_processed: processed,
            records_emitted: self.records_emitted.load(Ordering::Relaxed),
            records_filtered: self.records_filtered.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            avg_latency_us: if processed > 0 {
                total_latency / processed
            } else {
                0
            },
            windows_emitted: self.windows_emitted.load(Ordering::Relaxed),
            watermark,
        }
    }

    fn record_processed(&self, latency_us: u64) {
        self.records_processed.fetch_add(1, Ordering::Relaxed);
        self.total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
    }

    fn record_emitted(&self) {
        self.records_emitted.fetch_add(1, Ordering::Relaxed);
    }

    fn record_filtered(&self) {
        self.records_filtered.fetch_add(1, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    fn window_emitted(&self) {
        self.windows_emitted.fetch_add(1, Ordering::Relaxed);
    }
}

/// Pipeline error types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PipelineError {
    /// Configuration error
    Config(String),
    /// Execution error
    Execution(String),
    /// Source error
    Source(String),
    /// Sink error
    Sink(String),
    /// Operator error
    Operator(String),
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineError::Config(msg) => write!(f, "Config error: {}", msg),
            PipelineError::Execution(msg) => write!(f, "Execution error: {}", msg),
            PipelineError::Source(msg) => write!(f, "Source error: {}", msg),
            PipelineError::Sink(msg) => write!(f, "Sink error: {}", msg),
            PipelineError::Operator(msg) => write!(f, "Operator error: {}", msg),
        }
    }
}

impl std::error::Error for PipelineError {}

/// Pipeline definition
pub struct Pipeline {
    config: PipelineConfig,
    operators: OperatorChain,
    aggregate: Option<AggregateOperator>,
    window_config: Option<WindowConfig>,
    parsed_query: Option<ParsedQuery>,
}

impl Pipeline {
    /// Create a pipeline from a topic
    pub fn from_topic(topic: &str) -> PipelineBuilder {
        PipelineBuilder::new(topic)
    }

    /// Create a pipeline from a SQL-like query
    pub fn from_query(query: &str) -> DslResult<Self> {
        let parsed = DslParser::parse(query).map_err(|e| DslError::Parse(e.to_string()))?;
        Self::from_parsed_query(parsed)
    }

    /// Create from parsed query
    pub fn from_parsed_query(query: ParsedQuery) -> DslResult<Self> {
        let mut config = PipelineConfig {
            name: format!("query_{}", query.from.topic),
            source_topic: query.from.topic.clone(),
            sink_topic: query.emit_to.clone(),
            ..Default::default()
        };

        let window_config = query.window.as_ref().map(|w| WindowConfig::new(w.clone()));
        config.window = window_config.clone();

        let mut operators = OperatorChain::new();

        // Add filter if WHERE clause
        if let Some(ref filter_expr) = query.filter {
            operators.add(FilterOperator::new(filter_expr.clone()));
        }

        // Build projections from SELECT
        let (project_fields, aggregations) = Self::build_select_operators(&query.select)?;

        if !project_fields.is_empty() && aggregations.is_empty() {
            operators.add(ProjectOperator::new(project_fields));
        }

        // Build aggregate if GROUP BY
        let aggregate = if !query.group_by.is_empty() || !aggregations.is_empty() {
            Some(AggregateOperator::new(aggregations, query.group_by.clone()))
        } else {
            None
        };

        Ok(Self {
            config,
            operators,
            aggregate,
            window_config,
            parsed_query: Some(query),
        })
    }

    #[allow(clippy::type_complexity)]
    fn build_select_operators(
        fields: &[SelectField],
    ) -> DslResult<(
        Vec<String>,
        Vec<(String, AggregateFunction, Option<String>)>,
    )> {
        let mut project_fields = Vec::new();
        let mut aggregations = Vec::new();

        for field in fields {
            match field {
                SelectField::All => {
                    // All fields - no projection needed
                }
                SelectField::Field { name, alias: _ } => {
                    project_fields.push(name.clone());
                }
                SelectField::Aggregate {
                    function,
                    field,
                    alias,
                } => {
                    aggregations.push((alias.clone(), *function, field.clone()));
                }
                SelectField::Expression { expr: _, alias } => {
                    // For now, just add as field
                    project_fields.push(alias.clone());
                }
            }
        }

        Ok((project_fields, aggregations))
    }

    /// Get pipeline configuration
    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    /// Get the parsed query (if created from query)
    pub fn parsed_query(&self) -> Option<&ParsedQuery> {
        self.parsed_query.as_ref()
    }
}

/// Pipeline builder for fluent API
pub struct PipelineBuilder {
    config: PipelineConfig,
    operators: OperatorChain,
    aggregate: Option<AggregateOperator>,
    window_config: Option<WindowConfig>,
}

impl PipelineBuilder {
    /// Create a new builder
    pub fn new(source_topic: &str) -> Self {
        Self {
            config: PipelineConfig {
                source_topic: source_topic.to_string(),
                ..Default::default()
            },
            operators: OperatorChain::new(),
            aggregate: None,
            window_config: None,
        }
    }

    /// Set pipeline name
    pub fn name(mut self, name: &str) -> Self {
        self.config.name = name.to_string();
        self
    }

    /// Add a filter
    pub fn filter(mut self, condition: Expression) -> Self {
        self.operators.add(FilterOperator::new(condition));
        self
    }

    /// Add a filter with closure (convenience method)
    pub fn filter_fn<F>(self, _f: F) -> Self
    where
        F: Fn(&DslRecord) -> bool + Send + Sync + 'static,
    {
        // Note: This is a placeholder - real implementation would need
        // a way to convert closures to expressions
        self
    }

    /// Add a map transformation
    pub fn map(mut self, transformations: Vec<(String, Expression)>) -> Self {
        self.operators.add(MapOperator::new(transformations));
        self
    }

    /// Add a projection
    pub fn project(mut self, fields: Vec<String>) -> Self {
        self.operators.add(ProjectOperator::new(fields));
        self
    }

    /// Set window
    pub fn window(mut self, window_type: WindowType) -> Self {
        self.window_config = Some(WindowConfig::new(window_type));
        self.config.window = self.window_config.clone();
        self
    }

    /// Set window with config
    pub fn window_with_config(mut self, config: WindowConfig) -> Self {
        self.window_config = Some(config.clone());
        self.config.window = Some(config);
        self
    }

    /// Add aggregation
    pub fn aggregate(
        mut self,
        aggregations: Vec<(String, AggregateFunction, Option<String>)>,
        group_by: Vec<String>,
    ) -> Self {
        self.aggregate = Some(AggregateOperator::new(aggregations, group_by));
        self
    }

    /// Set output topic
    #[allow(clippy::wrong_self_convention)]
    pub fn to_topic(mut self, topic: &str) -> Self {
        self.config.sink_topic = Some(topic.to_string());
        self
    }

    /// Set batch size
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Enable checkpointing
    pub fn with_checkpointing(mut self, interval_ms: u64) -> Self {
        self.config.checkpointing = true;
        self.config.checkpoint_interval_ms = interval_ms;
        self
    }

    /// Build the pipeline
    pub fn build(self) -> Pipeline {
        Pipeline {
            config: self.config,
            operators: self.operators,
            aggregate: self.aggregate,
            window_config: self.window_config,
            parsed_query: None,
        }
    }
}

/// Pipeline executor
pub struct PipelineExecutor {
    pipeline: Pipeline,
    state: Arc<RwLock<PipelineState>>,
    stats: Arc<PipelineStats>,
    window: Option<Box<dyn Window>>,
    running: Arc<AtomicBool>,
    watermark: Arc<RwLock<i64>>,
}

impl PipelineExecutor {
    /// Create a new executor for a pipeline
    pub fn new(pipeline: Pipeline) -> Self {
        let window = pipeline.window_config.as_ref().map(|c| create_window(c));

        Self {
            pipeline,
            state: Arc::new(RwLock::new(PipelineState::Created)),
            stats: Arc::new(PipelineStats::default()),
            window,
            running: Arc::new(AtomicBool::new(false)),
            watermark: Arc::new(RwLock::new(0)),
        }
    }

    /// Get current state
    pub async fn state(&self) -> PipelineState {
        *self.state.read().await
    }

    /// Get metrics snapshot
    pub async fn metrics(&self) -> PipelineMetrics {
        let watermark = *self.watermark.read().await;
        self.stats.snapshot(watermark)
    }

    /// Process a batch of records
    pub async fn process_batch(&mut self, records: Vec<DslRecord>) -> DslResult<Vec<DslRecord>> {
        let ctx = DslContext::new();
        let mut results = Vec::new();

        for record in records {
            let start = std::time::Instant::now();

            // Process through operator chain
            match self.pipeline.operators.process(record, &ctx) {
                Ok(processed) => {
                    if processed.is_empty() {
                        self.stats.record_filtered();
                    } else {
                        for r in processed {
                            // Add to window if configured
                            if let Some(ref mut window) = self.window {
                                if let Err(e) = window.add(r.clone()) {
                                    tracing::warn!("Window add error: {}", e);
                                    self.stats.record_error();
                                }
                            } else {
                                results.push(r);
                                self.stats.record_emitted();
                            }
                        }
                    }

                    let latency = start.elapsed().as_micros() as u64;
                    self.stats.record_processed(latency);
                }
                Err(e) => {
                    tracing::warn!("Operator error: {}", e);
                    self.stats.record_error();
                }
            }
        }

        // Emit window results if applicable
        if let Some(ref mut window) = self.window {
            let watermark = *self.watermark.read().await;
            if let Ok(outputs) = window.emit(watermark) {
                for output in outputs {
                    self.stats.window_emitted();

                    // Apply aggregation if configured
                    if let Some(ref agg) = self.pipeline.aggregate {
                        match agg.aggregate_batch(&output.records) {
                            Ok(agg_results) => {
                                for r in agg_results {
                                    results.push(r);
                                    self.stats.record_emitted();
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Aggregation error: {}", e);
                                self.stats.record_error();
                            }
                        }
                    } else {
                        for r in output.records {
                            results.push(r);
                            self.stats.record_emitted();
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Advance watermark
    pub async fn advance_watermark(&mut self, watermark: i64) {
        *self.watermark.write().await = watermark;
        if let Some(ref mut window) = self.window {
            window.advance_watermark(watermark);
        }
    }

    /// Start the executor (for background processing)
    pub async fn start(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if *state == PipelineState::Running {
            return Ok(());
        }
        *state = PipelineState::Running;
        self.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Stop the executor
    pub async fn stop(&self) -> Result<()> {
        let mut state = self.state.write().await;
        *state = PipelineState::Stopped;
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Pause the executor
    pub async fn pause(&self) -> Result<()> {
        let mut state = self.state.write().await;
        *state = PipelineState::Paused;
        Ok(())
    }

    /// Resume the executor
    pub async fn resume(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if *state == PipelineState::Paused {
            *state = PipelineState::Running;
        }
        Ok(())
    }

    /// Check if running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get pipeline config
    pub fn config(&self) -> &PipelineConfig {
        &self.pipeline.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_pipeline_builder() {
        let pipeline = Pipeline::from_topic("events")
            .name("click-counter")
            .filter(Expression::eq(
                Expression::field("type"),
                Expression::literal(serde_json::json!("click")),
            ))
            .project(vec!["user_id".to_string(), "timestamp".to_string()])
            .window(WindowType::tumbling(60000))
            .aggregate(
                vec![("count".to_string(), AggregateFunction::Count, None)],
                vec!["user_id".to_string()],
            )
            .to_topic("click_counts")
            .build();

        assert_eq!(pipeline.config.name, "click-counter");
        assert_eq!(pipeline.config.source_topic, "events");
        assert_eq!(pipeline.config.sink_topic, Some("click_counts".to_string()));
        assert!(pipeline.window_config.is_some());
        assert!(pipeline.aggregate.is_some());
    }

    #[test]
    fn test_pipeline_from_query() {
        let query = r#"
            SELECT user_id, COUNT(*) AS clicks
            FROM STREAM 'events'
            WHERE type = 'click'
            WINDOW TUMBLING 5 MINUTES
            GROUP BY user_id
            EMIT TO 'click_counts'
        "#;

        let pipeline = Pipeline::from_query(query).unwrap();

        assert_eq!(pipeline.config.source_topic, "events");
        assert_eq!(pipeline.config.sink_topic, Some("click_counts".to_string()));
        assert!(pipeline.window_config.is_some());
        assert!(pipeline.aggregate.is_some());
    }

    #[tokio::test]
    async fn test_pipeline_executor() {
        let pipeline = Pipeline::from_topic("test")
            .filter(Expression::gt(
                Expression::field("value"),
                Expression::literal(serde_json::json!(5)),
            ))
            .build();

        let mut executor = PipelineExecutor::new(pipeline);

        let records = vec![
            DslRecord::new(serde_json::json!({"value": 10}), "test"),
            DslRecord::new(serde_json::json!({"value": 3}), "test"),
            DslRecord::new(serde_json::json!({"value": 7}), "test"),
        ];

        let results = executor.process_batch(records).await.unwrap();

        assert_eq!(results.len(), 2); // Only records with value > 5

        let metrics = executor.metrics().await;
        assert_eq!(metrics.records_processed, 3);
        assert_eq!(metrics.records_emitted, 2);
        assert_eq!(metrics.records_filtered, 1);
    }

    #[tokio::test]
    async fn test_pipeline_with_window() {
        let pipeline = Pipeline::from_topic("test")
            .window(WindowType::tumbling(1000))
            .aggregate(
                vec![("total".to_string(), AggregateFunction::Count, None)],
                vec![],
            )
            .build();

        let mut executor = PipelineExecutor::new(pipeline);

        // Add records
        let records = vec![
            DslRecord {
                key: None,
                value: serde_json::json!({"value": 1}),
                timestamp: 100,
                headers: HashMap::new(),
                source_topic: "test".to_string(),
                source_partition: 0,
                source_offset: 0,
            },
            DslRecord {
                key: None,
                value: serde_json::json!({"value": 2}),
                timestamp: 500,
                headers: HashMap::new(),
                source_topic: "test".to_string(),
                source_partition: 0,
                source_offset: 1,
            },
        ];

        let _ = executor.process_batch(records).await.unwrap();

        // Advance watermark to trigger window emit
        executor.advance_watermark(2000).await;

        // Process empty batch to trigger window emission
        let results = executor.process_batch(vec![]).await.unwrap();

        // Should have aggregated result
        assert!(!results.is_empty() || executor.metrics().await.windows_emitted > 0);
    }

    #[tokio::test]
    async fn test_executor_state_transitions() {
        let pipeline = Pipeline::from_topic("test").build();
        let executor = PipelineExecutor::new(pipeline);

        assert_eq!(executor.state().await, PipelineState::Created);

        executor.start().await.unwrap();
        assert_eq!(executor.state().await, PipelineState::Running);

        executor.pause().await.unwrap();
        assert_eq!(executor.state().await, PipelineState::Paused);

        executor.resume().await.unwrap();
        assert_eq!(executor.state().await, PipelineState::Running);

        executor.stop().await.unwrap();
        assert_eq!(executor.state().await, PipelineState::Stopped);
    }
}
