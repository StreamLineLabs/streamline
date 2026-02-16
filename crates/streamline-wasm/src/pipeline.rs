//! WebAssembly Transform Pipelines
//!
//! This module allows deploying WASM modules as inline stream processors
//! with hot-deploy capability. A pipeline connects a source topic to a
//! sink topic through an ordered series of transform stages, each backed
//! by a deployed WASM module.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────────────┐
//! │                       TransformPipeline                                  │
//! │                                                                          │
//! │  source_topic ──▶ ┌─────────┐ ──▶ ┌─────────┐ ──▶ ┌─────────┐ ──▶ sink │
//! │                   │ Stage 0 │     │ Stage 1 │     │ Stage N │          │
//! │                   │ (WASM)  │     │ (WASM)  │     │ (WASM)  │          │
//! │                   └─────────┘     └─────────┘     └─────────┘          │
//! │                                                                          │
//! │  Error Handling: Skip | DeadLetter | Retry                               │
//! └──────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Error Handling Strategies
//!
//! - **Skip**: Drop the record and continue processing.
//! - **DeadLetter**: Route the failed record to a dead-letter topic.
//! - **Retry**: Re-attempt the transform up to `max_attempts` times with
//!   exponential backoff.

use crate::error::{Result, WasmError};
use crate::runtime::WasmRuntime;
use crate::TransformInput;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

// ---------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------

/// Configuration for a transform pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Unique pipeline name (used as identifier).
    pub name: String,

    /// Human-readable description.
    #[serde(default)]
    pub description: String,

    /// Topic from which records are consumed.
    pub source_topic: String,

    /// Topic to which transformed records are produced.
    pub sink_topic: String,

    /// Ordered list of transform stages.
    pub stages: Vec<PipelineStage>,

    /// Strategy for handling errors during transformation.
    #[serde(default)]
    pub error_handling: ErrorHandling,

    /// Maximum number of records to process in a single batch.
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,

    /// Maximum latency (in milliseconds) before a partial batch is flushed.
    #[serde(default = "default_max_latency_ms")]
    pub max_latency_ms: u64,
}

fn default_max_batch_size() -> usize {
    1000
}

fn default_max_latency_ms() -> u64 {
    100
}

/// A single transform step within a pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStage {
    /// Human-readable name for this stage.
    pub name: String,

    /// Name of the deployed WASM transform module to invoke.
    pub transform_name: String,

    /// Stage-specific configuration passed to the WASM module.
    #[serde(default)]
    pub config: HashMap<String, String>,

    /// Execution order (lower values execute first).
    #[serde(default)]
    pub order: u32,
}

/// Strategy for handling errors during transformation.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum ErrorHandling {
    /// Drop the failed record and continue.
    #[default]
    Skip,

    /// Route the failed record to a dead-letter topic.
    DeadLetter {
        /// Dead-letter topic name.
        topic: String,
    },

    /// Retry the transform with exponential backoff.
    Retry {
        /// Maximum number of retry attempts.
        max_attempts: u32,
        /// Initial delay between retries in milliseconds.
        delay_ms: u64,
    },
}

// ---------------------------------------------------------------------------
// Output / status types
// ---------------------------------------------------------------------------

/// Output record produced by the pipeline after transformation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformOutput {
    /// Optional record key.
    pub key: Option<Vec<u8>>,

    /// Record value (payload).
    pub value: Vec<u8>,

    /// Record headers.
    pub headers: Vec<(String, Vec<u8>)>,

    /// If set, overrides the default sink topic for this record (content-based routing).
    pub target_topic: Option<String>,
}

/// Lifecycle status of a pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PipelineStatus {
    /// Pipeline has been created but not yet started.
    Created,
    /// Pipeline is actively processing records.
    Running,
    /// Pipeline processing is temporarily suspended.
    Paused,
    /// Pipeline has been gracefully stopped.
    Stopped,
    /// Pipeline encountered an unrecoverable error.
    Failed(String),
}

impl std::fmt::Display for PipelineStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineStatus::Created => write!(f, "created"),
            PipelineStatus::Running => write!(f, "running"),
            PipelineStatus::Paused => write!(f, "paused"),
            PipelineStatus::Stopped => write!(f, "stopped"),
            PipelineStatus::Failed(reason) => write!(f, "failed: {}", reason),
        }
    }
}

/// Runtime statistics for a pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStats {
    /// Number of records consumed from the source topic.
    pub records_in: u64,
    /// Number of records produced to the sink topic.
    pub records_out: u64,
    /// Number of records that failed transformation.
    pub records_errored: u64,
    /// Average per-record latency in microseconds.
    pub avg_latency_us: f64,
    /// 99th percentile per-record latency in microseconds.
    pub p99_latency_us: u64,
    /// Total bytes read from the source topic.
    pub bytes_in: u64,
    /// Total bytes written to the sink topic.
    pub bytes_out: u64,
    /// Most recent error message, if any.
    pub last_error: Option<String>,
    /// Time the pipeline has been in the Running state (milliseconds).
    pub uptime_ms: u64,
}

impl Default for PipelineStats {
    fn default() -> Self {
        Self {
            records_in: 0,
            records_out: 0,
            records_errored: 0,
            avg_latency_us: 0.0,
            p99_latency_us: 0,
            bytes_in: 0,
            bytes_out: 0,
            last_error: None,
            uptime_ms: 0,
        }
    }
}

/// Summary information for listing pipelines.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineInfo {
    /// Pipeline name.
    pub name: String,
    /// Pipeline description.
    pub description: String,
    /// Source topic.
    pub source_topic: String,
    /// Sink topic.
    pub sink_topic: String,
    /// Number of transform stages.
    pub stage_count: usize,
    /// Current status.
    pub status: PipelineStatus,
    /// Total records processed.
    pub records_in: u64,
    /// Total records produced.
    pub records_out: u64,
}

// ---------------------------------------------------------------------------
// Internal atomic counters (lock-free fast path for hot stats)
// ---------------------------------------------------------------------------

struct AtomicStats {
    records_in: AtomicU64,
    records_out: AtomicU64,
    records_errored: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    total_latency_us: AtomicU64,
}

impl Default for AtomicStats {
    fn default() -> Self {
        Self {
            records_in: AtomicU64::new(0),
            records_out: AtomicU64::new(0),
            records_errored: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// TransformPipeline
// ---------------------------------------------------------------------------

/// Manages a single running pipeline.
///
/// Records flow through the ordered list of stages.  Each stage delegates to
/// a deployed WASM transform module via the [`WasmRuntime`].  The pipeline
/// handles lifecycle management, stats tracking, error handling (skip,
/// dead-letter, retry), and content-based routing.
pub struct TransformPipeline {
    config: PipelineConfig,
    status: Arc<RwLock<PipelineStatus>>,
    stats: Arc<AtomicStats>,
    last_error: Arc<RwLock<Option<String>>>,
    started_at: Arc<RwLock<Option<Instant>>>,
    /// Per-record latencies kept for percentile computation (bounded buffer).
    latencies: Arc<RwLock<Vec<u64>>>,
    /// Optional WASM runtime for executing transform stages
    runtime: Option<Arc<WasmRuntime>>,
}

impl TransformPipeline {
    /// Create a new pipeline in the `Created` state.
    pub fn new(config: PipelineConfig) -> Self {
        Self {
            config,
            status: Arc::new(RwLock::new(PipelineStatus::Created)),
            stats: Arc::new(AtomicStats::default()),
            last_error: Arc::new(RwLock::new(None)),
            started_at: Arc::new(RwLock::new(None)),
            latencies: Arc::new(RwLock::new(Vec::with_capacity(10_000))),
            runtime: None,
        }
    }

    /// Create a new pipeline with a WASM runtime for executing stages.
    pub fn with_runtime(config: PipelineConfig, runtime: Arc<WasmRuntime>) -> Self {
        Self {
            config,
            status: Arc::new(RwLock::new(PipelineStatus::Created)),
            stats: Arc::new(AtomicStats::default()),
            last_error: Arc::new(RwLock::new(None)),
            started_at: Arc::new(RwLock::new(None)),
            latencies: Arc::new(RwLock::new(Vec::with_capacity(10_000))),
            runtime: Some(runtime),
        }
    }

    // -- Lifecycle ----------------------------------------------------------

    /// Transition the pipeline to the `Running` state.
    pub async fn start(&self) -> Result<()> {
        let mut status = self.status.write().await;
        match &*status {
            PipelineStatus::Created | PipelineStatus::Stopped | PipelineStatus::Paused => {
                *status = PipelineStatus::Running;
                let mut started = self.started_at.write().await;
                *started = Some(Instant::now());
                info!(pipeline = %self.config.name, "Pipeline started");
                Ok(())
            }
            PipelineStatus::Running => Err(WasmError::Wasm(format!(
                "Pipeline '{}' is already running",
                self.config.name
            ))),
            PipelineStatus::Failed(reason) => Err(WasmError::Wasm(format!(
                "Pipeline '{}' is in failed state: {}. Stop and recreate it.",
                self.config.name, reason
            ))),
        }
    }

    /// Gracefully stop the pipeline.
    pub async fn stop(&self) -> Result<()> {
        let mut status = self.status.write().await;
        match &*status {
            PipelineStatus::Running | PipelineStatus::Paused => {
                *status = PipelineStatus::Stopped;
                info!(pipeline = %self.config.name, "Pipeline stopped");
                Ok(())
            }
            PipelineStatus::Stopped => Ok(()),
            PipelineStatus::Created => Err(WasmError::Wasm(format!(
                "Pipeline '{}' was never started",
                self.config.name
            ))),
            PipelineStatus::Failed(_) => {
                *status = PipelineStatus::Stopped;
                Ok(())
            }
        }
    }

    /// Pause the pipeline (it can be resumed later).
    pub async fn pause(&self) -> Result<()> {
        let mut status = self.status.write().await;
        if *status == PipelineStatus::Running {
            *status = PipelineStatus::Paused;
            info!(pipeline = %self.config.name, "Pipeline paused");
            Ok(())
        } else {
            Err(WasmError::Wasm(format!(
                "Pipeline '{}' is not running (status: {})",
                self.config.name, *status
            )))
        }
    }

    /// Resume a paused pipeline.
    pub async fn resume(&self) -> Result<()> {
        let mut status = self.status.write().await;
        if *status == PipelineStatus::Paused {
            *status = PipelineStatus::Running;
            info!(pipeline = %self.config.name, "Pipeline resumed");
            Ok(())
        } else {
            Err(WasmError::Wasm(format!(
                "Pipeline '{}' is not paused (status: {})",
                self.config.name, *status
            )))
        }
    }

    // -- Queries ------------------------------------------------------------

    /// Return the current pipeline status.
    pub async fn get_status(&self) -> PipelineStatus {
        self.status.read().await.clone()
    }

    /// Return a snapshot of pipeline statistics.
    pub async fn get_stats(&self) -> PipelineStats {
        let records_in = self.stats.records_in.load(Ordering::Relaxed);
        let records_out = self.stats.records_out.load(Ordering::Relaxed);
        let records_errored = self.stats.records_errored.load(Ordering::Relaxed);
        let bytes_in = self.stats.bytes_in.load(Ordering::Relaxed);
        let bytes_out = self.stats.bytes_out.load(Ordering::Relaxed);
        let total_latency_us = self.stats.total_latency_us.load(Ordering::Relaxed);

        let avg_latency_us = if records_in > 0 {
            total_latency_us as f64 / records_in as f64
        } else {
            0.0
        };

        let p99_latency_us = {
            let latencies = self.latencies.read().await;
            compute_p99(&latencies)
        };

        let last_error = self.last_error.read().await.clone();

        let uptime_ms = {
            let started = self.started_at.read().await;
            started.map(|s| s.elapsed().as_millis() as u64).unwrap_or(0)
        };

        PipelineStats {
            records_in,
            records_out,
            records_errored,
            avg_latency_us,
            p99_latency_us,
            bytes_in,
            bytes_out,
            last_error,
            uptime_ms,
        }
    }

    /// Return the pipeline configuration.
    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    // -- Record processing --------------------------------------------------

    /// Process a single record through the pipeline stages.
    ///
    /// Returns a vec of `TransformOutput` (one-to-many is possible when a
    /// stage fans out).
    pub async fn process_record(
        &self,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
        headers: Vec<(String, Vec<u8>)>,
    ) -> Result<Vec<TransformOutput>> {
        // Only process when running
        let status = self.status.read().await;
        if *status != PipelineStatus::Running {
            return Err(WasmError::Wasm(format!(
                "Pipeline '{}' is not running (status: {})",
                self.config.name, *status
            )));
        }
        drop(status);

        let start = Instant::now();
        let input_bytes = value.len() as u64 + key.as_ref().map(|k| k.len() as u64).unwrap_or(0);
        self.stats.records_in.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_in
            .fetch_add(input_bytes, Ordering::Relaxed);

        // Seed the pipeline with the initial record.
        let mut current_outputs: Vec<TransformOutput> = vec![TransformOutput {
            key,
            value,
            headers,
            target_topic: None,
        }];

        // Iterate through stages in order.
        let mut sorted_stages = self.config.stages.clone();
        sorted_stages.sort_by_key(|s| s.order);

        for stage in &sorted_stages {
            let mut next_outputs: Vec<TransformOutput> = Vec::new();

            for output in current_outputs {
                match self.execute_stage(stage, &output).await {
                    Ok(stage_results) => {
                        next_outputs.extend(stage_results);
                    }
                    Err(e) => {
                        warn!(
                            pipeline = %self.config.name,
                            stage = %stage.name,
                            error = %e,
                            "Transform stage failed"
                        );
                        match &self.config.error_handling {
                            ErrorHandling::Skip => {
                                self.stats.records_errored.fetch_add(1, Ordering::Relaxed);
                                let mut le = self.last_error.write().await;
                                *le = Some(e.to_string());
                                debug!(
                                    pipeline = %self.config.name,
                                    stage = %stage.name,
                                    "Skipping failed record"
                                );
                                // Record is dropped; do not add to next_outputs.
                            }
                            ErrorHandling::DeadLetter { topic } => {
                                self.stats.records_errored.fetch_add(1, Ordering::Relaxed);
                                let mut le = self.last_error.write().await;
                                *le = Some(e.to_string());
                                // Route to dead-letter topic.
                                let mut dlq_record = output.clone();
                                dlq_record.target_topic = Some(topic.clone());
                                // Attach error info as a header.
                                dlq_record.headers.push((
                                    "x-pipeline-error".to_string(),
                                    e.to_string().into_bytes(),
                                ));
                                dlq_record.headers.push((
                                    "x-pipeline-stage".to_string(),
                                    stage.name.clone().into_bytes(),
                                ));
                                next_outputs.push(dlq_record);
                            }
                            ErrorHandling::Retry {
                                max_attempts,
                                delay_ms,
                            } => {
                                let mut succeeded = false;
                                let mut last_err = e;
                                for attempt in 1..=*max_attempts {
                                    let backoff =
                                        *delay_ms * 2u64.saturating_pow(attempt.saturating_sub(1));
                                    tokio::time::sleep(std::time::Duration::from_millis(backoff))
                                        .await;
                                    match self.execute_stage(stage, &output).await {
                                        Ok(stage_results) => {
                                            next_outputs.extend(stage_results);
                                            succeeded = true;
                                            break;
                                        }
                                        Err(retry_err) => {
                                            last_err = retry_err;
                                        }
                                    }
                                }
                                if !succeeded {
                                    self.stats.records_errored.fetch_add(1, Ordering::Relaxed);
                                    let mut le = self.last_error.write().await;
                                    *le = Some(last_err.to_string());
                                    error!(
                                        pipeline = %self.config.name,
                                        stage = %stage.name,
                                        attempts = max_attempts,
                                        "All retry attempts exhausted"
                                    );
                                }
                            }
                        }
                    }
                }
            }

            current_outputs = next_outputs;

            // If nothing survived this stage, short-circuit.
            if current_outputs.is_empty() {
                break;
            }
        }

        // Record output stats.
        let output_bytes: u64 = current_outputs
            .iter()
            .map(|o| o.value.len() as u64 + o.key.as_ref().map(|k| k.len() as u64).unwrap_or(0))
            .sum();
        self.stats
            .records_out
            .fetch_add(current_outputs.len() as u64, Ordering::Relaxed);
        self.stats
            .bytes_out
            .fetch_add(output_bytes, Ordering::Relaxed);

        let latency_us = start.elapsed().as_micros() as u64;
        self.stats
            .total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);

        // Append to latency buffer (bounded to last 10 000 entries).
        {
            let mut latencies = self.latencies.write().await;
            if latencies.len() >= 10_000 {
                latencies.drain(..5_000);
            }
            latencies.push(latency_us);
        }

        Ok(current_outputs)
    }

    // -- Stage execution (WASM invocation placeholder) -----------------------

    /// Execute a single pipeline stage against one record.
    ///
    /// Invokes the WASM module registered under `stage.transform_name` via
    /// the `WasmRuntime`, converting between pipeline and runtime types.
    async fn execute_stage(
        &self,
        stage: &PipelineStage,
        input: &TransformOutput,
    ) -> Result<Vec<TransformOutput>> {
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            WasmError::Wasm("No WASM runtime configured for pipeline".to_string())
        })?;

        // Convert pipeline TransformOutput to runtime TransformInput
        let headers_map = input
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let transform_input = TransformInput {
            topic: self.config.source_topic.clone(),
            partition: 0,
            key: input.key.clone(),
            value: input.value.clone(),
            headers: headers_map,
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
        };

        // Execute the WASM module
        let wasm_output = runtime
            .execute(&stage.transform_name, transform_input.clone())
            .await?;

        // Convert runtime TransformOutput back to pipeline TransformOutput
        let results = match wasm_output {
            crate::TransformOutput::Pass(ti) => {
                vec![TransformOutput {
                    key: ti.key,
                    value: ti.value,
                    headers: ti.headers.into_iter().collect(),
                    target_topic: None,
                }]
            }
            crate::TransformOutput::Drop => Vec::new(),
            crate::TransformOutput::Transformed(results) => results
                .into_iter()
                .map(|r| TransformOutput {
                    key: r.key,
                    value: r.value,
                    headers: r.headers.into_iter().collect(),
                    target_topic: None,
                })
                .collect(),
            crate::TransformOutput::Route(routes) => routes
                .into_iter()
                .map(|r| TransformOutput {
                    key: r.key,
                    value: r.value,
                    headers: r.headers.into_iter().collect(),
                    target_topic: Some(r.topic),
                })
                .collect(),
            crate::TransformOutput::Error(msg) => {
                return Err(WasmError::Wasm(format!(
                    "Stage '{}' transform error: {}",
                    stage.name, msg
                )));
            }
        };

        debug!(
            stage = %stage.name,
            transform = %stage.transform_name,
            output_count = results.len(),
            "Pipeline stage executed"
        );

        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// PipelineManager
// ---------------------------------------------------------------------------

/// Manages the full set of transform pipelines.
pub struct PipelineManager {
    pipelines: Arc<RwLock<HashMap<String, TransformPipeline>>>,
    /// Optional WASM runtime shared across all pipelines
    runtime: Option<Arc<WasmRuntime>>,
}

impl PipelineManager {
    /// Create a new, empty pipeline manager.
    pub fn new() -> Self {
        Self {
            pipelines: Arc::new(RwLock::new(HashMap::new())),
            runtime: None,
        }
    }

    /// Create a pipeline manager with a WASM runtime.
    pub fn with_runtime(runtime: Arc<WasmRuntime>) -> Self {
        Self {
            pipelines: Arc::new(RwLock::new(HashMap::new())),
            runtime: Some(runtime),
        }
    }

    /// Create and register a new pipeline.  Returns the pipeline name.
    pub async fn create_pipeline(&self, config: PipelineConfig) -> Result<String> {
        self.validate_config(&config)?;

        let name = config.name.clone();
        let pipeline = match &self.runtime {
            Some(rt) => TransformPipeline::with_runtime(config, Arc::clone(rt)),
            None => TransformPipeline::new(config),
        };

        let mut pipelines = self.pipelines.write().await;
        if pipelines.contains_key(&name) {
            return Err(WasmError::Wasm(format!(
                "Pipeline '{}' already exists",
                name
            )));
        }
        pipelines.insert(name.clone(), pipeline);

        info!(pipeline = %name, "Pipeline created");
        Ok(name)
    }

    /// Delete an existing pipeline.  The pipeline is stopped first if running.
    pub async fn delete_pipeline(&self, name: &str) -> Result<()> {
        let mut pipelines = self.pipelines.write().await;
        let pipeline = pipelines
            .get(name)
            .ok_or_else(|| WasmError::Wasm(format!("Pipeline '{}' not found", name)))?;

        // Stop if running.
        let status = pipeline.get_status().await;
        if status == PipelineStatus::Running || status == PipelineStatus::Paused {
            pipeline.stop().await.ok();
        }

        pipelines.remove(name);
        info!(pipeline = %name, "Pipeline deleted");
        Ok(())
    }

    /// Start a pipeline by name.
    pub async fn start_pipeline(&self, name: &str) -> Result<()> {
        let pipelines = self.pipelines.read().await;
        let pipeline = pipelines
            .get(name)
            .ok_or_else(|| WasmError::Wasm(format!("Pipeline '{}' not found", name)))?;
        pipeline.start().await
    }

    /// Stop a pipeline by name.
    pub async fn stop_pipeline(&self, name: &str) -> Result<()> {
        let pipelines = self.pipelines.read().await;
        let pipeline = pipelines
            .get(name)
            .ok_or_else(|| WasmError::Wasm(format!("Pipeline '{}' not found", name)))?;
        pipeline.stop().await
    }

    /// Pause a pipeline by name.
    pub async fn pause_pipeline(&self, name: &str) -> Result<()> {
        let pipelines = self.pipelines.read().await;
        let pipeline = pipelines
            .get(name)
            .ok_or_else(|| WasmError::Wasm(format!("Pipeline '{}' not found", name)))?;
        pipeline.pause().await
    }

    /// Resume a paused pipeline by name.
    pub async fn resume_pipeline(&self, name: &str) -> Result<()> {
        let pipelines = self.pipelines.read().await;
        let pipeline = pipelines
            .get(name)
            .ok_or_else(|| WasmError::Wasm(format!("Pipeline '{}' not found", name)))?;
        pipeline.resume().await
    }

    /// List summary information for all pipelines.
    pub async fn list_pipelines(&self) -> Vec<PipelineInfo> {
        let pipelines = self.pipelines.read().await;
        let mut infos = Vec::with_capacity(pipelines.len());

        for (name, pipeline) in pipelines.iter() {
            let status = pipeline.get_status().await;
            let stats = pipeline.get_stats().await;
            infos.push(PipelineInfo {
                name: name.clone(),
                description: pipeline.config.description.clone(),
                source_topic: pipeline.config.source_topic.clone(),
                sink_topic: pipeline.config.sink_topic.clone(),
                stage_count: pipeline.config.stages.len(),
                status,
                records_in: stats.records_in,
                records_out: stats.records_out,
            });
        }

        infos
    }

    /// Get a reference to a pipeline by name (behind the read lock).
    ///
    /// Callers should prefer `get_pipeline_status` or `get_pipeline_stats`
    /// for most use-cases.  This method is provided for advanced scenarios
    /// such as directly calling `process_record`.
    pub async fn get_pipeline_status(&self, name: &str) -> Option<PipelineStatus> {
        let pipelines = self.pipelines.read().await;
        match pipelines.get(name) {
            Some(p) => Some(p.get_status().await),
            None => None,
        }
    }

    /// Retrieve stats for a specific pipeline.
    pub async fn get_pipeline_stats(&self, name: &str) -> Option<PipelineStats> {
        let pipelines = self.pipelines.read().await;
        match pipelines.get(name) {
            Some(p) => Some(p.get_stats().await),
            None => None,
        }
    }

    /// Process a record through a named pipeline.
    pub async fn process_record(
        &self,
        name: &str,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
        headers: Vec<(String, Vec<u8>)>,
    ) -> Result<Vec<TransformOutput>> {
        let pipelines = self.pipelines.read().await;
        let pipeline = pipelines
            .get(name)
            .ok_or_else(|| WasmError::Wasm(format!("Pipeline '{}' not found", name)))?;
        pipeline.process_record(key, value, headers).await
    }

    /// Hot-reload a pipeline with a new configuration.
    ///
    /// Stops the existing pipeline, replaces it with the new config, and
    /// restarts it if it was running.
    pub async fn hot_reload(&self, config: PipelineConfig) -> Result<()> {
        self.validate_config(&config)?;
        let name = config.name.clone();

        let mut pipelines = self.pipelines.write().await;
        let was_running = if let Some(existing) = pipelines.get(&name) {
            let status = existing.get_status().await;
            if status == PipelineStatus::Running || status == PipelineStatus::Paused {
                existing.stop().await.ok();
                true
            } else {
                false
            }
        } else {
            false
        };

        let new_pipeline = match &self.runtime {
            Some(rt) => TransformPipeline::with_runtime(config, Arc::clone(rt)),
            None => TransformPipeline::new(config),
        };
        if was_running {
            new_pipeline.start().await?;
        }
        pipelines.insert(name.clone(), new_pipeline);

        info!(pipeline = %name, "Pipeline hot-reloaded");
        Ok(())
    }

    /// Update a single stage's config within a pipeline (requires restart).
    pub async fn update_stage_config(
        &self,
        pipeline_name: &str,
        stage_name: &str,
        stage_config: HashMap<String, String>,
    ) -> Result<()> {
        let pipelines = self.pipelines.read().await;
        let pipeline = pipelines
            .get(pipeline_name)
            .ok_or_else(|| WasmError::Wasm(format!("Pipeline '{}' not found", pipeline_name)))?;

        let mut new_config = pipeline.config().clone();
        let stage = new_config
            .stages
            .iter_mut()
            .find(|s| s.name == stage_name)
            .ok_or_else(|| {
                WasmError::Wasm(format!(
                    "Stage '{}' not found in pipeline '{}'",
                    stage_name, pipeline_name
                ))
            })?;
        stage.config = stage_config;
        drop(pipelines);

        self.hot_reload(new_config).await
    }

    /// Get aggregate stats across all pipelines
    pub async fn aggregate_stats(&self) -> PipelineManagerStats {
        let pipelines = self.pipelines.read().await;
        let mut total_in = 0u64;
        let mut total_out = 0u64;
        let mut total_errors = 0u64;
        let mut running = 0usize;
        let mut stopped = 0usize;

        for pipeline in pipelines.values() {
            let stats = pipeline.get_stats().await;
            total_in += stats.records_in;
            total_out += stats.records_out;
            total_errors += stats.records_errored;
            match pipeline.get_status().await {
                PipelineStatus::Running => running += 1,
                _ => stopped += 1,
            }
        }

        PipelineManagerStats {
            total_pipelines: pipelines.len(),
            running,
            stopped,
            total_records_in: total_in,
            total_records_out: total_out,
            total_errors,
        }
    }

    // -- Validation ---------------------------------------------------------

    fn validate_config(&self, config: &PipelineConfig) -> Result<()> {
        if config.name.is_empty() {
            return Err(WasmError::Wasm(
                "Pipeline name must not be empty".to_string(),
            ));
        }
        if config.source_topic.is_empty() {
            return Err(WasmError::Wasm(
                "Pipeline source_topic must not be empty".to_string(),
            ));
        }
        if config.sink_topic.is_empty() {
            return Err(WasmError::Wasm(
                "Pipeline sink_topic must not be empty".to_string(),
            ));
        }
        if config.stages.is_empty() {
            return Err(WasmError::Wasm(
                "Pipeline must have at least one stage".to_string(),
            ));
        }
        for stage in &config.stages {
            if stage.name.is_empty() {
                return Err(WasmError::Wasm("Stage name must not be empty".to_string()));
            }
            if stage.transform_name.is_empty() {
                return Err(WasmError::Wasm(format!(
                    "Stage '{}' must reference a transform_name",
                    stage.name
                )));
            }
        }
        if config.max_batch_size == 0 {
            return Err(WasmError::Wasm(
                "max_batch_size must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

impl Default for PipelineManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Aggregate statistics across all pipelines
#[derive(Debug, Clone, Serialize)]
pub struct PipelineManagerStats {
    /// Total number of pipelines
    pub total_pipelines: usize,
    /// Number of running pipelines
    pub running: usize,
    /// Number of stopped/created pipelines
    pub stopped: usize,
    /// Total records consumed across all pipelines
    pub total_records_in: u64,
    /// Total records produced across all pipelines
    pub total_records_out: u64,
    /// Total errors across all pipelines
    pub total_errors: u64,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Compute the 99th-percentile value from a slice of latencies.
fn compute_p99(latencies: &[u64]) -> u64 {
    if latencies.is_empty() {
        return 0;
    }
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    let idx = ((sorted.len() as f64) * 0.99).ceil() as usize;
    let idx = idx.min(sorted.len()) - 1;
    sorted[idx]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a minimal valid pipeline config.
    fn test_config(name: &str) -> PipelineConfig {
        PipelineConfig {
            name: name.to_string(),
            description: "test pipeline".to_string(),
            source_topic: "input-topic".to_string(),
            sink_topic: "output-topic".to_string(),
            stages: vec![PipelineStage {
                name: "stage-1".to_string(),
                transform_name: "identity".to_string(),
                config: HashMap::new(),
                order: 0,
            }],
            error_handling: ErrorHandling::Skip,
            max_batch_size: 100,
            max_latency_ms: 50,
        }
    }

    // -- PipelineConfig defaults -------------------------------------------

    #[test]
    fn test_pipeline_config_serde_defaults() {
        let json = r#"{
            "name": "p1",
            "description": "",
            "source_topic": "src",
            "sink_topic": "dst",
            "stages": [
                {"name": "s1", "transform_name": "t1", "order": 0}
            ]
        }"#;
        let config: PipelineConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_batch_size, 1000);
        assert_eq!(config.max_latency_ms, 100);
        assert_eq!(config.error_handling, ErrorHandling::Skip);
    }

    // -- ErrorHandling serde -----------------------------------------------

    #[test]
    fn test_error_handling_serde_variants() {
        let skip: ErrorHandling = serde_json::from_str(r#"{"strategy":"skip"}"#).unwrap();
        assert_eq!(skip, ErrorHandling::Skip);

        let dlq: ErrorHandling =
            serde_json::from_str(r#"{"strategy":"dead_letter","topic":"dlq-topic"}"#).unwrap();
        assert_eq!(
            dlq,
            ErrorHandling::DeadLetter {
                topic: "dlq-topic".to_string()
            }
        );

        let retry: ErrorHandling =
            serde_json::from_str(r#"{"strategy":"retry","max_attempts":3,"delay_ms":500}"#)
                .unwrap();
        assert_eq!(
            retry,
            ErrorHandling::Retry {
                max_attempts: 3,
                delay_ms: 500
            }
        );
    }

    // -- Pipeline lifecycle -------------------------------------------------

    #[tokio::test]
    async fn test_pipeline_lifecycle_start_stop() {
        let pipeline = TransformPipeline::new(test_config("lc-test"));

        assert_eq!(pipeline.get_status().await, PipelineStatus::Created);

        pipeline.start().await.unwrap();
        assert_eq!(pipeline.get_status().await, PipelineStatus::Running);

        pipeline.stop().await.unwrap();
        assert_eq!(pipeline.get_status().await, PipelineStatus::Stopped);
    }

    #[tokio::test]
    async fn test_pipeline_lifecycle_pause_resume() {
        let pipeline = TransformPipeline::new(test_config("pr-test"));

        pipeline.start().await.unwrap();
        pipeline.pause().await.unwrap();
        assert_eq!(pipeline.get_status().await, PipelineStatus::Paused);

        pipeline.resume().await.unwrap();
        assert_eq!(pipeline.get_status().await, PipelineStatus::Running);
    }

    #[tokio::test]
    async fn test_pipeline_double_start_fails() {
        let pipeline = TransformPipeline::new(test_config("ds-test"));
        pipeline.start().await.unwrap();

        let result = pipeline.start().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pipeline_pause_when_not_running_fails() {
        let pipeline = TransformPipeline::new(test_config("pnr-test"));
        // Pipeline is in Created state, pause should fail.
        let result = pipeline.pause().await;
        assert!(result.is_err());
    }

    // -- Record processing --------------------------------------------------

    #[tokio::test]
    async fn test_process_record_without_runtime_skips_output() {
        let pipeline = TransformPipeline::new(test_config("proc-test"));
        pipeline.start().await.unwrap();

        let key = Some(b"key-1".to_vec());
        let value = b"hello world".to_vec();
        let headers = vec![("h1".to_string(), b"v1".to_vec())];

        let outputs = pipeline
            .process_record(key.clone(), value.clone(), headers.clone())
            .await
            .unwrap();

        assert!(outputs.is_empty());
    }

    #[tokio::test]
    async fn test_process_record_when_stopped_fails() {
        let pipeline = TransformPipeline::new(test_config("stopped-test"));
        // Pipeline is in Created state, processing should fail.
        let result = pipeline
            .process_record(None, b"data".to_vec(), vec![])
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_process_record_updates_stats() {
        let pipeline = TransformPipeline::new(test_config("stats-test"));
        pipeline.start().await.unwrap();

        for _ in 0..5 {
            pipeline
                .process_record(None, b"payload".to_vec(), vec![])
                .await
                .unwrap();
        }

        let stats = pipeline.get_stats().await;
        assert_eq!(stats.records_in, 5);
        assert_eq!(stats.records_out, 0);
        assert_eq!(stats.records_errored, 5);
        assert!(stats.avg_latency_us > 0.0);
        let _ = stats.uptime_ms; // may be 0 on fast machines
        assert!(stats.bytes_in > 0);
    }

    // -- PipelineManager ----------------------------------------------------

    #[tokio::test]
    async fn test_manager_create_and_list() {
        let manager = PipelineManager::new();

        let name = manager
            .create_pipeline(test_config("m-test"))
            .await
            .unwrap();
        assert_eq!(name, "m-test");

        let list = manager.list_pipelines().await;
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "m-test");
        assert_eq!(list[0].stage_count, 1);
        assert_eq!(list[0].status, PipelineStatus::Created);
    }

    #[tokio::test]
    async fn test_manager_duplicate_pipeline_fails() {
        let manager = PipelineManager::new();
        manager.create_pipeline(test_config("dup")).await.unwrap();

        let result = manager.create_pipeline(test_config("dup")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_manager_delete_pipeline() {
        let manager = PipelineManager::new();
        manager.create_pipeline(test_config("del")).await.unwrap();
        manager.start_pipeline("del").await.unwrap();

        manager.delete_pipeline("del").await.unwrap();
        let list = manager.list_pipelines().await;
        assert!(list.is_empty());
    }

    #[tokio::test]
    async fn test_manager_delete_nonexistent_fails() {
        let manager = PipelineManager::new();
        let result = manager.delete_pipeline("ghost").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_manager_validation_empty_name() {
        let manager = PipelineManager::new();
        let mut cfg = test_config("");
        cfg.name = String::new();
        let result = manager.create_pipeline(cfg).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_manager_validation_no_stages() {
        let manager = PipelineManager::new();
        let mut cfg = test_config("empty-stages");
        cfg.stages = vec![];
        let result = manager.create_pipeline(cfg).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_manager_process_record_through_pipeline() {
        let manager = PipelineManager::new();
        manager
            .create_pipeline(test_config("proc-mgr"))
            .await
            .unwrap();
        manager.start_pipeline("proc-mgr").await.unwrap();

        let outputs = manager
            .process_record("proc-mgr", Some(b"k".to_vec()), b"v".to_vec(), vec![])
            .await
            .unwrap();
        assert!(outputs.is_empty());
    }

    // -- Helpers ------------------------------------------------------------

    #[test]
    fn test_compute_p99_empty() {
        assert_eq!(compute_p99(&[]), 0);
    }

    #[test]
    fn test_compute_p99_values() {
        let latencies: Vec<u64> = (1..=100).collect();
        let p99 = compute_p99(&latencies);
        // 99th percentile of 1..=100 should be 99 or 100
        assert!(p99 >= 99);
    }

    #[test]
    fn test_pipeline_status_display() {
        assert_eq!(PipelineStatus::Created.to_string(), "created");
        assert_eq!(PipelineStatus::Running.to_string(), "running");
        assert_eq!(PipelineStatus::Paused.to_string(), "paused");
        assert_eq!(PipelineStatus::Stopped.to_string(), "stopped");
        assert_eq!(
            PipelineStatus::Failed("boom".to_string()).to_string(),
            "failed: boom"
        );
    }

    #[test]
    fn test_transform_output_clone() {
        let output = TransformOutput {
            key: Some(b"k".to_vec()),
            value: b"v".to_vec(),
            headers: vec![("h".to_string(), b"hv".to_vec())],
            target_topic: Some("routed".to_string()),
        };
        let cloned = output.clone();
        assert_eq!(cloned.key, output.key);
        assert_eq!(cloned.value, output.value);
        assert_eq!(cloned.target_topic, Some("routed".to_string()));
    }

    #[test]
    fn test_pipeline_info_fields() {
        let info = PipelineInfo {
            name: "test".to_string(),
            description: "desc".to_string(),
            source_topic: "src".to_string(),
            sink_topic: "dst".to_string(),
            stage_count: 3,
            status: PipelineStatus::Running,
            records_in: 1000,
            records_out: 998,
        };
        assert_eq!(info.name, "test");
        assert_eq!(info.stage_count, 3);
        assert_eq!(info.status, PipelineStatus::Running);
    }

    #[tokio::test]
    async fn test_multi_stage_pipeline() {
        let config = PipelineConfig {
            name: "multi-stage".to_string(),
            description: "pipeline with multiple stages".to_string(),
            source_topic: "in".to_string(),
            sink_topic: "out".to_string(),
            stages: vec![
                PipelineStage {
                    name: "filter".to_string(),
                    transform_name: "wasm-filter".to_string(),
                    config: HashMap::new(),
                    order: 0,
                },
                PipelineStage {
                    name: "enrich".to_string(),
                    transform_name: "wasm-enrich".to_string(),
                    config: HashMap::new(),
                    order: 1,
                },
                PipelineStage {
                    name: "route".to_string(),
                    transform_name: "wasm-route".to_string(),
                    config: HashMap::new(),
                    order: 2,
                },
            ],
            error_handling: ErrorHandling::DeadLetter {
                topic: "dlq".to_string(),
            },
            max_batch_size: 500,
            max_latency_ms: 200,
        };

        let pipeline = TransformPipeline::new(config);
        pipeline.start().await.unwrap();

        let outputs = pipeline
            .process_record(Some(b"key".to_vec()), b"value".to_vec(), vec![])
            .await
            .unwrap();

        // With stub errors, the record should be routed to the dead-letter topic.
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].value, b"value");
        assert_eq!(outputs[0].target_topic.as_deref(), Some("dlq"));
    }

    // -- Pipeline with WASM runtime ----------------------------------------

    /// Helper: create a WasmRuntime and load a stub module under the given id.
    async fn setup_runtime_with_module(module_id: &str) -> Arc<crate::runtime::WasmRuntime> {
        let runtime = Arc::new(crate::runtime::WasmRuntime::new().unwrap());

        // Minimal valid WASM header (stub mode accepts this)
        let wasm_bytes = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        let config = crate::TransformationConfig {
            id: module_id.to_string(),
            name: module_id.to_string(),
            ..Default::default()
        };

        runtime
            .load_module_bytes(module_id, config, wasm_bytes)
            .await
            .unwrap();

        runtime
    }

    #[cfg(not(feature = "wasm-runtime"))]
    #[tokio::test]
    async fn test_process_record_with_runtime_produces_output() {
        let runtime = setup_runtime_with_module("identity").await;

        let config = PipelineConfig {
            name: "rt-test".to_string(),
            description: "pipeline with runtime".to_string(),
            source_topic: "src".to_string(),
            sink_topic: "dst".to_string(),
            stages: vec![PipelineStage {
                name: "s1".to_string(),
                transform_name: "identity".to_string(),
                config: HashMap::new(),
                order: 0,
            }],
            error_handling: ErrorHandling::Skip,
            max_batch_size: 100,
            max_latency_ms: 50,
        };

        let pipeline = TransformPipeline::with_runtime(config, runtime);
        pipeline.start().await.unwrap();

        let outputs = pipeline
            .process_record(Some(b"k".to_vec()), b"hello".to_vec(), vec![])
            .await
            .unwrap();

        // Stub runtime passes through as Transformed with the same value.
        assert!(!outputs.is_empty());
        assert_eq!(outputs[0].value, b"hello");

        let stats = pipeline.get_stats().await;
        assert_eq!(stats.records_in, 1);
        assert_eq!(stats.records_out, 1);
        assert_eq!(stats.records_errored, 0);
    }

    #[cfg(not(feature = "wasm-runtime"))]
    #[tokio::test]
    async fn test_process_record_multi_stage_with_runtime() {
        let runtime = setup_runtime_with_module("stage-a").await;
        runtime
            .load_module_bytes(
                "stage-b",
                crate::TransformationConfig {
                    id: "stage-b".to_string(),
                    name: "stage-b".to_string(),
                    ..Default::default()
                },
                vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00],
            )
            .await
            .unwrap();

        let config = PipelineConfig {
            name: "multi-rt".to_string(),
            description: "multi-stage with runtime".to_string(),
            source_topic: "in".to_string(),
            sink_topic: "out".to_string(),
            stages: vec![
                PipelineStage {
                    name: "first".to_string(),
                    transform_name: "stage-a".to_string(),
                    config: HashMap::new(),
                    order: 0,
                },
                PipelineStage {
                    name: "second".to_string(),
                    transform_name: "stage-b".to_string(),
                    config: HashMap::new(),
                    order: 1,
                },
            ],
            error_handling: ErrorHandling::Skip,
            max_batch_size: 100,
            max_latency_ms: 50,
        };

        let pipeline = TransformPipeline::with_runtime(config, runtime);
        pipeline.start().await.unwrap();

        let outputs = pipeline
            .process_record(None, b"data".to_vec(), vec![])
            .await
            .unwrap();

        assert!(!outputs.is_empty());
        assert_eq!(outputs[0].value, b"data");

        let stats = pipeline.get_stats().await;
        assert_eq!(stats.records_in, 1);
        assert_eq!(stats.records_out, 1);
        assert_eq!(stats.records_errored, 0);
    }

    #[cfg(not(feature = "wasm-runtime"))]
    #[tokio::test]
    async fn test_manager_with_runtime_creates_working_pipelines() {
        let runtime = setup_runtime_with_module("identity").await;
        let manager = PipelineManager::with_runtime(runtime);

        manager.create_pipeline(test_config("mgr-rt")).await.unwrap();
        manager.start_pipeline("mgr-rt").await.unwrap();

        let outputs = manager
            .process_record("mgr-rt", Some(b"k".to_vec()), b"v".to_vec(), vec![])
            .await
            .unwrap();

        assert!(!outputs.is_empty());
        assert_eq!(outputs[0].value, b"v");
    }

    #[cfg(not(feature = "wasm-runtime"))]
    #[tokio::test]
    async fn test_hot_reload_preserves_runtime() {
        let runtime = setup_runtime_with_module("identity").await;
        let manager = PipelineManager::with_runtime(runtime);

        manager.create_pipeline(test_config("hr-rt")).await.unwrap();
        manager.start_pipeline("hr-rt").await.unwrap();

        // Hot-reload with updated config (auto-restarts because it was running)
        let mut new_cfg = test_config("hr-rt");
        new_cfg.max_batch_size = 500;
        manager.hot_reload(new_cfg).await.unwrap();

        // Pipeline should still work with the runtime after hot-reload
        let outputs = manager
            .process_record("hr-rt", None, b"payload".to_vec(), vec![])
            .await
            .unwrap();

        assert!(!outputs.is_empty());
    }

    #[cfg(not(feature = "wasm-runtime"))]
    #[tokio::test]
    async fn test_missing_module_errors_with_skip() {
        let runtime = Arc::new(crate::runtime::WasmRuntime::new().unwrap());

        let config = PipelineConfig {
            name: "missing-mod".to_string(),
            description: "references non-existent module".to_string(),
            source_topic: "src".to_string(),
            sink_topic: "dst".to_string(),
            stages: vec![PipelineStage {
                name: "s1".to_string(),
                transform_name: "does-not-exist".to_string(),
                config: HashMap::new(),
                order: 0,
            }],
            error_handling: ErrorHandling::Skip,
            max_batch_size: 100,
            max_latency_ms: 50,
        };

        let pipeline = TransformPipeline::with_runtime(config, runtime);
        pipeline.start().await.unwrap();

        let outputs = pipeline
            .process_record(None, b"data".to_vec(), vec![])
            .await
            .unwrap();

        // Missing module → error → Skip drops the record
        assert!(outputs.is_empty());
        let stats = pipeline.get_stats().await;
        assert_eq!(stats.records_errored, 1);
    }
}
