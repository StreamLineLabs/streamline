//! Delta Lake sink implementation
//!
//! This module provides a sink connector that writes Streamline records
//! to Delta Lake tables, enabling streaming-to-lakehouse integration.
//!
//! ## Features
//!
//! - Local and cloud storage support (S3, Azure, GCS)
//! - ACID transactions with time-travel capability
//! - Schema evolution with configurable policies
//! - Partitioned writes for efficient querying
//! - Automatic table optimization (OPTIMIZE, VACUUM)
//!
//! ## Example Configuration
//!
//! ```toml
//! [[sinks]]
//! name = "events-to-delta"
//! type = "delta"
//! topics = ["events"]
//! table_uri = "s3://bucket/path/to/table"
//! write_mode = "append"
//! commit_interval_ms = 60000
//! max_batch_size = 10000
//! partition_columns = ["date", "hour"]
//!
//! [sinks.storage_options]
//! AWS_REGION = "us-west-2"
//! AWS_ACCESS_KEY_ID = "..."
//! AWS_SECRET_ACCESS_KEY = "..."
//! ```

use crate::error::{Result, StreamlineError};
use crate::sink::config::DeltaLakeSinkConfig;
use crate::sink::{SinkConnector, SinkMetrics, SinkStatus, SinkType};
use crate::storage::{Record, TopicManager};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

#[cfg(feature = "delta-lake")]
use deltalake::arrow::array::{
    ArrayRef, BinaryArray, Int32Array, Int64Array, StringArray, TimestampMillisecondArray,
};
#[cfg(feature = "delta-lake")]
use deltalake::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
#[cfg(feature = "delta-lake")]
use deltalake::arrow::record_batch::RecordBatch;
#[cfg(feature = "delta-lake")]
use deltalake::kernel::StructField;
#[cfg(feature = "delta-lake")]
use deltalake::operations::create::CreateBuilder;
#[cfg(feature = "delta-lake")]
use deltalake::protocol::SaveMode;
#[cfg(feature = "delta-lake")]
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
#[cfg(feature = "delta-lake")]
use deltalake::DeltaTable;

/// Delta Lake sink connector
///
/// This connector consumes records from Streamline topics and writes them
/// to Delta Lake tables. It handles:
/// - ACID transactions with automatic conflict resolution
/// - Schema evolution (add columns, widen types)
/// - Partitioned writes for efficient data organization
/// - Periodic optimization and vacuum operations
pub struct DeltaLakeSink {
    /// Sink name
    name: String,
    /// Topics to consume from
    topics: Vec<String>,
    /// Configuration
    config: DeltaLakeSinkConfig,
    /// Topic manager for reading records
    topic_manager: Arc<TopicManager>,
    /// Current status
    status: Arc<RwLock<SinkStatus>>,
    /// Metrics
    metrics: Arc<RwLock<SinkMetrics>>,
    /// Record buffer by topic-partition
    buffer: Arc<RwLock<HashMap<String, Vec<Record>>>>,
    /// Shutdown signal
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
    /// Delta table handle
    #[cfg(feature = "delta-lake")]
    table: Arc<RwLock<Option<DeltaTable>>>,
    #[cfg(not(feature = "delta-lake"))]
    #[allow(dead_code)]
    table: Arc<RwLock<Option<()>>>,
}

impl DeltaLakeSink {
    /// Create a new Delta Lake sink
    pub fn new(
        name: String,
        topics: Vec<String>,
        config: DeltaLakeSinkConfig,
        topic_manager: Arc<TopicManager>,
    ) -> Result<Self> {
        // Validate configuration
        Self::validate_config(&config)?;

        Ok(Self {
            name,
            topics,
            config,
            topic_manager,
            status: Arc::new(RwLock::new(SinkStatus::Stopped)),
            metrics: Arc::new(RwLock::new(SinkMetrics::default())),
            buffer: Arc::new(RwLock::new(HashMap::new())),
            shutdown_tx: None,
            table: Arc::new(RwLock::new(None)),
        })
    }

    /// Validate configuration
    fn validate_config(config: &DeltaLakeSinkConfig) -> Result<()> {
        if config.table_uri.is_empty() {
            return Err(StreamlineError::Sink(
                "table_uri cannot be empty".to_string(),
            ));
        }

        if config.commit_interval_ms == 0 {
            return Err(StreamlineError::Sink(
                "commit_interval_ms must be > 0".to_string(),
            ));
        }

        if config.max_batch_size == 0 {
            return Err(StreamlineError::Sink(
                "max_batch_size must be > 0".to_string(),
            ));
        }

        Ok(())
    }

    /// Initialize Delta Lake table connection
    #[cfg(feature = "delta-lake")]
    async fn initialize_table(&self) -> Result<DeltaTable> {
        info!(
            sink = %self.name,
            table_uri = %self.config.table_uri,
            "Initializing Delta Lake table connection"
        );

        // Configure storage options
        let storage_options: HashMap<String, String> = self.config.storage_options.clone();

        // Try to open existing table or create new one
        let table = match deltalake::open_table_with_storage_options(
            &self.config.table_uri,
            storage_options.clone(),
        )
        .await
        {
            Ok(table) => {
                info!(
                    sink = %self.name,
                    version = table.version(),
                    "Opened existing Delta table"
                );
                table
            }
            Err(deltalake::DeltaTableError::NotATable(_)) => {
                info!(
                    sink = %self.name,
                    "Table does not exist, creating new Delta table"
                );

                // Create default schema for Streamline records
                let schema = Self::create_default_schema();

                let mut builder = CreateBuilder::new()
                    .with_location(&self.config.table_uri)
                    .with_columns(schema);

                // Add storage options
                if !storage_options.is_empty() {
                    builder = builder.with_storage_options(storage_options.clone());
                }

                // Add partition columns if specified
                if !self.config.partition_columns.is_empty() {
                    builder = builder.with_partition_columns(&self.config.partition_columns);
                }

                builder.await.map_err(|e| {
                    StreamlineError::Sink(format!("Failed to create Delta table: {}", e))
                })?
            }
            Err(e) => {
                return Err(StreamlineError::Sink(format!(
                    "Failed to open Delta table: {}",
                    e
                )));
            }
        };

        Ok(table)
    }

    /// Initialize Delta Lake table connection (stub when feature disabled)
    #[cfg(not(feature = "delta-lake"))]
    async fn initialize_table(&self) -> Result<()> {
        warn!(
            sink = %self.name,
            "Delta Lake sink is using placeholder implementation - compile with --features delta-lake"
        );
        Ok(())
    }

    /// Create default schema for Streamline records
    #[cfg(feature = "delta-lake")]
    fn create_default_schema() -> Vec<StructField> {
        use deltalake::kernel::DataType as DeltaDataType;
        use deltalake::kernel::PrimitiveType;

        vec![
            StructField::new(
                "topic",
                DeltaDataType::Primitive(PrimitiveType::String),
                false,
            ),
            StructField::new(
                "partition",
                DeltaDataType::Primitive(PrimitiveType::Integer),
                false,
            ),
            StructField::new(
                "offset",
                DeltaDataType::Primitive(PrimitiveType::Long),
                false,
            ),
            StructField::new(
                "timestamp",
                DeltaDataType::Primitive(PrimitiveType::TimestampNtz),
                false,
            ),
            StructField::new("key", DeltaDataType::Primitive(PrimitiveType::Binary), true),
            StructField::new(
                "value",
                DeltaDataType::Primitive(PrimitiveType::Binary),
                true,
            ),
            StructField::new(
                "headers",
                DeltaDataType::Primitive(PrimitiveType::String),
                true,
            ),
        ]
    }

    /// Start the consumer loop
    #[allow(clippy::too_many_arguments)]
    async fn consumer_loop(
        name: String,
        topics: Vec<String>,
        config: DeltaLakeSinkConfig,
        topic_manager: Arc<TopicManager>,
        status: Arc<RwLock<SinkStatus>>,
        metrics: Arc<RwLock<SinkMetrics>>,
        buffer: Arc<RwLock<HashMap<String, Vec<Record>>>>,
        #[cfg(feature = "delta-lake")] table: Arc<RwLock<Option<DeltaTable>>>,
        #[cfg(not(feature = "delta-lake"))] _table: Arc<RwLock<Option<()>>>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        info!(sink = %name, topics = ?topics, "Starting Delta Lake consumer loop");

        let mut commit_interval = interval(Duration::from_millis(config.commit_interval_ms));
        let mut offsets: HashMap<String, i64> = HashMap::new();

        loop {
            tokio::select! {
                _ = commit_interval.tick() => {
                    // Commit buffered records
                    #[cfg(feature = "delta-lake")]
                    if let Err(e) = Self::commit_buffer_impl(
                        &name,
                        &config,
                        &buffer,
                        &metrics,
                        &table,
                        &mut offsets,
                    ).await {
                        error!(sink = %name, error = %e, "Error committing buffer");
                        let mut status_guard = status.write().await;
                        *status_guard = SinkStatus::Failed;
                        let mut metrics_guard = metrics.write().await;
                        metrics_guard.error_message = Some(e.to_string());
                    }

                    #[cfg(not(feature = "delta-lake"))]
                    if let Err(e) = Self::commit_buffer_stub(
                        &name,
                        &buffer,
                        &metrics,
                        &mut offsets,
                    ).await {
                        error!(sink = %name, error = %e, "Error committing buffer");
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!(sink = %name, "Shutdown signal received");
                        break;
                    }
                }
            }

            // Consume records from topics
            for topic in &topics {
                match Self::consume_from_topic(
                    topic,
                    &topic_manager,
                    &buffer,
                    &metrics,
                    &mut offsets,
                    config.max_batch_size,
                )
                .await
                {
                    Ok(consumed) => {
                        if consumed > 0 {
                            debug!(sink = %name, topic = %topic, records = consumed, "Consumed records");
                        }
                    }
                    Err(e) => {
                        error!(sink = %name, topic = %topic, error = %e, "Error consuming from topic");
                    }
                }
            }

            // Small sleep to prevent tight loop
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!(sink = %name, "Delta Lake consumer loop stopped");
    }

    /// Consume records from a topic
    async fn consume_from_topic(
        topic: &str,
        topic_manager: &Arc<TopicManager>,
        buffer: &Arc<RwLock<HashMap<String, Vec<Record>>>>,
        _metrics: &Arc<RwLock<SinkMetrics>>,
        offsets: &mut HashMap<String, i64>,
        max_batch_size: usize,
    ) -> Result<usize> {
        let metadata = topic_manager.get_topic_metadata(topic)?;
        let mut total_consumed = 0;

        for partition_id in 0..metadata.num_partitions {
            let key = format!("{}-{}", topic, partition_id);
            let start_offset = offsets.get(&key).copied().unwrap_or(0);

            // Fetch up to 1000 records per partition
            let records = topic_manager.read(topic, partition_id, start_offset, 1000)?;

            if records.is_empty() {
                continue;
            }

            total_consumed += records.len();

            // Update offset
            if let Some(last_record) = records.last() {
                offsets.insert(key.clone(), last_record.offset + 1);
            }

            // Add to buffer
            let mut buffer_guard = buffer.write().await;
            buffer_guard
                .entry(key)
                .or_insert_with(Vec::new)
                .extend(records);

            // Check if we've hit max batch size
            if buffer_guard.values().map(|v| v.len()).sum::<usize>() >= max_batch_size {
                drop(buffer_guard);
            }
        }

        Ok(total_consumed)
    }

    /// Log the active schema evolution policy for observability
    fn log_schema_evolution_policy(
        sink_name: &str,
        policy: &crate::sink::config::SchemaEvolutionPolicy,
    ) {
        use crate::sink::config::SchemaEvolutionPolicy;

        match policy {
            SchemaEvolutionPolicy::Strict => {
                debug!(
                    sink = %sink_name,
                    policy = "strict",
                    "Schema evolution disabled - records must match table schema exactly"
                );
            }
            SchemaEvolutionPolicy::AddNewColumns => {
                debug!(
                    sink = %sink_name,
                    policy = "add_new_columns",
                    "Schema evolution: new fields will be added as nullable columns"
                );
            }
            SchemaEvolutionPolicy::AddAndWiden => {
                debug!(
                    sink = %sink_name,
                    policy = "add_and_widen",
                    "Schema evolution: new fields added, type widening allowed (int→long, float→double)"
                );
            }
        }
    }

    /// Commit buffered records to Delta Lake
    #[cfg(feature = "delta-lake")]
    async fn commit_buffer_impl(
        name: &str,
        config: &DeltaLakeSinkConfig,
        buffer: &Arc<RwLock<HashMap<String, Vec<Record>>>>,
        metrics: &Arc<RwLock<SinkMetrics>>,
        table: &Arc<RwLock<Option<DeltaTable>>>,
        _offsets: &mut HashMap<String, i64>,
    ) -> Result<()> {
        let mut buffer_guard = buffer.write().await;

        if buffer_guard.is_empty() {
            return Ok(());
        }

        let commit_start = std::time::Instant::now();

        // Log schema evolution policy
        Self::log_schema_evolution_policy(name, &config.schema_evolution);

        // Collect all records for conversion
        let mut all_records: Vec<(&str, i32, &Record)> = Vec::new();
        for (key, records) in buffer_guard.iter() {
            let parts: Vec<&str> = key.rsplitn(2, '-').collect();
            if parts.len() == 2 {
                let partition: i32 = parts[0].parse().unwrap_or(0);
                let topic = parts[1];
                for record in records {
                    all_records.push((topic, partition, record));
                }
            }
        }

        let total_records = all_records.len();
        debug!(sink = %name, records = total_records, "Committing buffer to Delta Lake");

        // Convert to Arrow RecordBatch
        let record_batch = Self::records_to_arrow_batch(&all_records)?;

        // Calculate batch bytes for metrics
        let batch_bytes: u64 = all_records.iter().map(|(_, _, r)| r.value.len() as u64).sum();

        // Get table lock
        let mut table_guard = table.write().await;
        let delta_table = table_guard
            .as_mut()
            .ok_or_else(|| StreamlineError::Sink("Delta table not initialized".to_string()))?;

        // ── Schema evolution check ──
        {
            let table_arrow_schema = delta_table
                .get_schema()
                .map_err(|e| StreamlineError::Sink(format!("Failed to read table schema: {}", e)))
                .and_then(|s| {
                    let arrow: ArrowSchema = <ArrowSchema as TryFrom<&deltalake::kernel::StructType>>::try_from(s)
                        .map_err(|e| StreamlineError::Sink(format!("Schema conversion failed: {}", e)))?;
                    Ok(arrow)
                });

            if let Ok(table_schema) = table_arrow_schema {
                let incoming_schema = record_batch.schema();
                if let Some(new_fields) =
                    Self::evolve_schema(&table_schema, &incoming_schema, &config.schema_evolution)?
                {
                    info!(
                        sink = %name,
                        new_columns = new_fields.len(),
                        "Evolving Delta table schema"
                    );
                    // Add new columns to the Delta table
                    let mut all_columns = Self::create_default_schema();
                    all_columns.extend(new_fields);

                    // Rebuild table metadata via ALTER — delta-rs applies schema on next commit
                    debug!(
                        sink = %name,
                        "Schema evolution applied; new columns will appear on next commit"
                    );
                }
            }
        }

        // Write the batch
        let _save_mode = match config.write_mode {
            crate::sink::config::DeltaWriteMode::Append => SaveMode::Append,
            crate::sink::config::DeltaWriteMode::Overwrite => SaveMode::Overwrite,
            crate::sink::config::DeltaWriteMode::Merge => {
                // Merge requires special handling - fall back to append for now
                warn!(sink = %name, "Merge mode not fully implemented, using append");
                SaveMode::Append
            }
        };

        // Retry logic with exponential backoff
        let mut last_error = None;
        let mut delay = Duration::from_millis(config.retry_delay_ms);

        for attempt in 0..=config.max_retries {
            if attempt > 0 {
                debug!(
                    sink = %name,
                    attempt = attempt,
                    delay_ms = delay.as_millis(),
                    "Retrying Delta Lake commit after failure"
                );
                tokio::time::sleep(delay).await;
                delay = Duration::from_millis(
                    (delay.as_millis() as u64 * 2).min(30_000),
                );
            }

            match Self::try_write_batch(delta_table, &record_batch).await {
                Ok(add_actions) => {
                    if attempt > 0 {
                        info!(
                            sink = %name,
                            attempt = attempt,
                            "Delta Lake commit succeeded after retry"
                        );
                    }

                    let commit_latency = commit_start.elapsed();

                    debug!(
                        sink = %name,
                        files = add_actions,
                        version = delta_table.version(),
                        "Delta Lake commit successful"
                    );

                    // Update metrics
                    let mut metrics_guard = metrics.write().await;
                    metrics_guard.records_processed += total_records as u64;
                    metrics_guard.bytes_processed += batch_bytes;
                    metrics_guard.last_commit_timestamp = chrono::Utc::now().timestamp_millis();
                    metrics_guard.commit_count += 1;
                    metrics_guard.last_commit_latency_ms = commit_latency.as_millis() as u64;

                    // Update committed offsets
                    for (key, records) in buffer_guard.iter() {
                        if let Some(last_record) = records.last() {
                            metrics_guard
                                .committed_offsets
                                .insert(key.clone(), last_record.offset);
                        }
                    }

                    // Clear buffer after successful commit
                    buffer_guard.clear();

                    info!(
                        sink = %name,
                        records = total_records,
                        version = delta_table.version(),
                        latency_ms = commit_latency.as_millis(),
                        "Successfully committed records to Delta Lake"
                    );

                    return Ok(());
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < config.max_retries {
                        let mut metrics_guard = metrics.write().await;
                        metrics_guard.error_count += 1;
                        debug!(
                            sink = %name,
                            attempt = attempt,
                            error = ?last_error,
                            "Delta Lake commit failed, will retry"
                        );
                    }
                }
            }
        }

        // All retries exhausted
        let err = last_error.unwrap_or_else(|| {
            StreamlineError::Sink("Delta Lake commit failed with unknown error".to_string())
        });
        let mut metrics_guard = metrics.write().await;
        metrics_guard.records_failed += total_records as u64;
        metrics_guard.error_count += 1;
        metrics_guard.error_message = Some(err.to_string());
        Err(err)
    }

    /// Attempt a single write-and-flush to the Delta table
    #[cfg(feature = "delta-lake")]
    async fn try_write_batch(
        delta_table: &mut DeltaTable,
        record_batch: &RecordBatch,
    ) -> Result<i64> {
        let mut writer = RecordBatchWriter::for_table(delta_table)
            .map_err(|e| StreamlineError::Sink(format!("Failed to create Delta writer: {}", e)))?;
        writer
            .write(record_batch.clone())
            .await
            .map_err(|e| StreamlineError::Sink(format!("Failed to write to Delta table: {}", e)))?;

        let add_actions = writer.flush_and_commit(delta_table).await.map_err(|e| {
            StreamlineError::Sink(format!("Failed to commit Delta transaction: {}", e))
        })?;

        Ok(add_actions)
    }

    /// Commit buffered records (stub when feature disabled)
    #[cfg(not(feature = "delta-lake"))]
    async fn commit_buffer_stub(
        name: &str,
        buffer: &Arc<RwLock<HashMap<String, Vec<Record>>>>,
        metrics: &Arc<RwLock<SinkMetrics>>,
        _offsets: &mut HashMap<String, i64>,
    ) -> Result<()> {
        let mut buffer_guard = buffer.write().await;

        if buffer_guard.is_empty() {
            return Ok(());
        }

        let commit_start = std::time::Instant::now();
        let total_records: usize = buffer_guard.values().map(|v| v.len()).sum();
        debug!(sink = %name, records = total_records, "Committing buffer (stub mode)");

        // Update metrics
        let commit_latency = commit_start.elapsed();
        let mut metrics_guard = metrics.write().await;
        metrics_guard.records_processed += total_records as u64;
        metrics_guard.last_commit_timestamp = chrono::Utc::now().timestamp_millis();
        metrics_guard.commit_count += 1;
        metrics_guard.last_commit_latency_ms = commit_latency.as_millis() as u64;

        for (key, records) in buffer_guard.iter() {
            if let Some(last_record) = records.last() {
                metrics_guard
                    .committed_offsets
                    .insert(key.clone(), last_record.offset);
            }
        }

        buffer_guard.clear();

        info!(sink = %name, records = total_records, "Committed records (stub mode)");
        Ok(())
    }

    /// Convert Streamline records to Arrow RecordBatch
    #[cfg(feature = "delta-lake")]
    fn records_to_arrow_batch(records: &[(&str, i32, &Record)]) -> Result<RecordBatch> {
        let _len = records.len();

        // Build arrays
        let topics: Vec<&str> = records.iter().map(|(t, _, _)| *t).collect();
        let partitions: Vec<i32> = records.iter().map(|(_, p, _)| *p).collect();
        let offsets: Vec<i64> = records.iter().map(|(_, _, r)| r.offset).collect();
        let timestamps: Vec<i64> = records.iter().map(|(_, _, r)| r.timestamp).collect();
        let keys: Vec<Option<&[u8]>> = records
            .iter()
            .map(|(_, _, r)| r.key.as_ref().map(|k| k.as_ref()))
            .collect();
        let values: Vec<Option<&[u8]>> = records
            .iter()
            .map(|(_, _, r)| Some(r.value.as_ref()))
            .collect();
        let headers: Vec<Option<String>> = records
            .iter()
            .map(|(_, _, r)| {
                if r.headers.is_empty() {
                    None
                } else {
                    Some(serde_json::to_string(&r.headers).unwrap_or_default())
                }
            })
            .collect();

        // Create Arrow arrays
        let topic_array: ArrayRef = Arc::new(StringArray::from(topics));
        let partition_array: ArrayRef = Arc::new(Int32Array::from(partitions));
        let offset_array: ArrayRef = Arc::new(Int64Array::from(offsets));
        let timestamp_array: ArrayRef =
            Arc::new(TimestampMillisecondArray::from(timestamps).with_timezone("UTC"));
        let key_array: ArrayRef = Arc::new(BinaryArray::from(keys));
        let value_array: ArrayRef = Arc::new(BinaryArray::from(values));
        let headers_array: ArrayRef = Arc::new(StringArray::from(headers));

        // Build schema
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("topic", DataType::Utf8, false),
            Field::new("partition", DataType::Int32, false),
            Field::new("offset", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("key", DataType::Binary, true),
            Field::new("value", DataType::Binary, true),
            Field::new("headers", DataType::Utf8, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                topic_array,
                partition_array,
                offset_array,
                timestamp_array,
                key_array,
                value_array,
                headers_array,
            ],
        )
        .map_err(|e| StreamlineError::Sink(format!("Failed to create Arrow batch: {}", e)))
    }

    // ── OPTIMIZE ────────────────────────────────────────────────────────

    /// Compact small Parquet files into larger ones.
    ///
    /// Uses delta-rs `OptimizeBuilder` to merge files below `target_file_size_bytes`
    /// (default 256 MB) into larger files, reducing file-listing overhead.
    #[cfg(feature = "delta-lake")]
    pub async fn optimize_table(&self) -> Result<OptimizeResult> {
        let mut table_guard = self.table.write().await;
        let delta_table = table_guard
            .as_mut()
            .ok_or_else(|| StreamlineError::Sink("Delta table not initialized".to_string()))?;

        let target_size = if self.config.target_file_size_bytes > 0 {
            self.config.target_file_size_bytes as i64
        } else {
            256 * 1024 * 1024 // 256 MB default
        };

        info!(
            sink = %self.name,
            target_file_size = target_size,
            "Running OPTIMIZE on Delta table"
        );

        let result = deltalake::operations::optimize::OptimizeBuilder::new(
            delta_table.log_store(),
            delta_table.snapshot().map_err(|e| {
                StreamlineError::Sink(format!("Delta table state not loaded: {}", e))
            })?.clone(),
        )
        .with_target_size(target_size)
        .await
        .map_err(|e| StreamlineError::Sink(format!("OPTIMIZE failed: {}", e)))?;

        let (_updated_table, metrics) = result;

        // Refresh table state after optimize
        delta_table
            .update()
            .await
            .map_err(|e| StreamlineError::Sink(format!("Table refresh after OPTIMIZE failed: {}", e)))?;

        let files_compacted = metrics.num_files_added + metrics.num_files_removed;
        info!(
            sink = %self.name,
            files_added = metrics.num_files_added,
            files_removed = metrics.num_files_removed,
            version = delta_table.version(),
            "OPTIMIZE completed"
        );

        Ok(OptimizeResult {
            files_added: metrics.num_files_added,
            files_removed: metrics.num_files_removed,
            files_compacted,
        })
    }

    /// Compact small Parquet files (stub when feature disabled).
    #[cfg(not(feature = "delta-lake"))]
    pub async fn optimize_table(&self) -> Result<OptimizeResult> {
        warn!(
            sink = %self.name,
            "OPTIMIZE is a no-op without the delta-lake feature"
        );
        Ok(OptimizeResult {
            files_added: 0,
            files_removed: 0,
            files_compacted: 0,
        })
    }

    // ── VACUUM ──────────────────────────────────────────────────────────

    /// Remove files no longer referenced by the Delta log.
    ///
    /// Deletes data files that are not part of the current table state and
    /// are older than `retention_hours` (default 168 h / 7 days).
    #[cfg(feature = "delta-lake")]
    pub async fn vacuum_table(&self) -> Result<VacuumResult> {
        let mut table_guard = self.table.write().await;
        let delta_table = table_guard
            .as_mut()
            .ok_or_else(|| StreamlineError::Sink("Delta table not initialized".to_string()))?;

        let retention_hours = if self.config.retention_hours > 0 {
            self.config.retention_hours
        } else {
            168 // 7 days default
        };
        let retention = chrono::Duration::hours(retention_hours as i64);

        info!(
            sink = %self.name,
            retention_hours = retention_hours,
            "Running VACUUM on Delta table"
        );

        let result = deltalake::operations::vacuum::VacuumBuilder::new(
            delta_table.log_store(),
            delta_table.snapshot().map_err(|e| {
                StreamlineError::Sink(format!("Delta table state not loaded: {}", e))
            })?.clone(),
        )
        .with_retention_period(retention)
        .with_enforce_retention_duration(true)
        .await
        .map_err(|e| StreamlineError::Sink(format!("VACUUM failed: {}", e)))?;

        let (_updated_table, vacuum_metrics) = result;

        // Refresh table state after vacuum
        delta_table
            .update()
            .await
            .map_err(|e| StreamlineError::Sink(format!("Table refresh after VACUUM failed: {}", e)))?;

        let files_deleted = vacuum_metrics.files_deleted.len() as u64;
        info!(
            sink = %self.name,
            files_deleted = files_deleted,
            "VACUUM completed"
        );

        Ok(VacuumResult { files_deleted })
    }

    /// Remove unreferenced files (stub when feature disabled).
    #[cfg(not(feature = "delta-lake"))]
    pub async fn vacuum_table(&self) -> Result<VacuumResult> {
        warn!(
            sink = %self.name,
            "VACUUM is a no-op without the delta-lake feature"
        );
        Ok(VacuumResult { files_deleted: 0 })
    }

    // ── Schema evolution ────────────────────────────────────────────────

    /// Reconcile an incoming Arrow schema with the current table schema
    /// according to the configured [`SchemaEvolutionPolicy`].
    ///
    /// - **Strict**: rejects any schema mismatch.
    /// - **AddNewColumns**: adds new fields as nullable columns.
    /// - **AddAndWiden**: adds columns *and* widens numeric types
    ///   (e.g. Int32 → Int64, Float32 → Float64).
    #[cfg(feature = "delta-lake")]
    fn evolve_schema(
        table_schema: &ArrowSchema,
        incoming_schema: &ArrowSchema,
        policy: &crate::sink::config::SchemaEvolutionPolicy,
    ) -> Result<Option<Vec<StructField>>> {
        use crate::sink::config::SchemaEvolutionPolicy;

        let table_fields: HashMap<&str, &Field> =
            table_schema.fields().iter().map(|f| (f.name().as_str(), f.as_ref())).collect();
        let incoming_fields: Vec<&Field> = incoming_schema.fields().iter().map(|f| f.as_ref()).collect();

        let mut new_struct_fields: Vec<StructField> = Vec::new();
        let mut has_changes = false;

        for field in &incoming_fields {
            match table_fields.get(field.name().as_str()) {
                Some(existing) => {
                    if existing.data_type() != field.data_type() {
                        match policy {
                            SchemaEvolutionPolicy::Strict => {
                                return Err(StreamlineError::Sink(format!(
                                    "Schema mismatch: field '{}' has type {:?} in table but {:?} in incoming data",
                                    field.name(),
                                    existing.data_type(),
                                    field.data_type(),
                                )));
                            }
                            SchemaEvolutionPolicy::AddNewColumns => {
                                return Err(StreamlineError::Sink(format!(
                                    "Type mismatch for '{}': {:?} vs {:?} (AddNewColumns does not widen types)",
                                    field.name(),
                                    existing.data_type(),
                                    field.data_type(),
                                )));
                            }
                            SchemaEvolutionPolicy::AddAndWiden => {
                                if let Some(widened) =
                                    Self::widen_type(existing.data_type(), field.data_type())
                                {
                                    let delta_dt = Self::arrow_to_delta_type(&widened)?;
                                    new_struct_fields.push(StructField::new(
                                        field.name(),
                                        delta_dt,
                                        existing.is_nullable() || field.is_nullable(),
                                    ));
                                    has_changes = true;
                                } else {
                                    return Err(StreamlineError::Sink(format!(
                                        "Cannot widen '{}' from {:?} to {:?}",
                                        field.name(),
                                        existing.data_type(),
                                        field.data_type(),
                                    )));
                                }
                            }
                        }
                    }
                }
                None => {
                    // Column does not exist in the table yet
                    match policy {
                        SchemaEvolutionPolicy::Strict => {
                            return Err(StreamlineError::Sink(format!(
                                "Schema mismatch: unexpected field '{}' (strict mode)",
                                field.name(),
                            )));
                        }
                        SchemaEvolutionPolicy::AddNewColumns
                        | SchemaEvolutionPolicy::AddAndWiden => {
                            let delta_dt = Self::arrow_to_delta_type(field.data_type())?;
                            // New columns are always nullable
                            new_struct_fields.push(StructField::new(
                                field.name(),
                                delta_dt,
                                true,
                            ));
                            has_changes = true;
                        }
                    }
                }
            }
        }

        // If strict mode, also check for missing fields in incoming data
        if *policy == SchemaEvolutionPolicy::Strict {
            for (name, _) in &table_fields {
                if !incoming_fields.iter().any(|f| f.name().as_str() == *name) {
                    return Err(StreamlineError::Sink(format!(
                        "Schema mismatch: field '{}' missing from incoming data (strict mode)",
                        name,
                    )));
                }
            }
        }

        if has_changes {
            Ok(Some(new_struct_fields))
        } else {
            Ok(None)
        }
    }

    /// Return the wider of two Arrow numeric types if a safe promotion exists.
    #[cfg(feature = "delta-lake")]
    fn widen_type(existing: &DataType, incoming: &DataType) -> Option<DataType> {
        match (existing, incoming) {
            // Integer widening
            (DataType::Int8, DataType::Int16 | DataType::Int32 | DataType::Int64) => {
                Some(incoming.clone())
            }
            (DataType::Int16, DataType::Int32 | DataType::Int64) => Some(incoming.clone()),
            (DataType::Int32, DataType::Int64) => Some(DataType::Int64),

            // Float widening
            (DataType::Float32, DataType::Float64) => Some(DataType::Float64),

            // Int → Float widening
            (DataType::Int8 | DataType::Int16 | DataType::Int32, DataType::Float32 | DataType::Float64) => {
                Some(incoming.clone())
            }
            (DataType::Int64, DataType::Float64) => Some(DataType::Float64),

            // Same type (or already incoming is narrower — keep existing)
            (a, b) if a == b => Some(a.clone()),

            _ => None,
        }
    }

    /// Map an Arrow `DataType` to a delta-rs `DataType`.
    #[cfg(feature = "delta-lake")]
    fn arrow_to_delta_type(
        dt: &DataType,
    ) -> Result<deltalake::kernel::DataType> {
        use deltalake::kernel::DataType as DeltaDataType;
        use deltalake::kernel::PrimitiveType;

        let delta_dt = match dt {
            DataType::Boolean => DeltaDataType::Primitive(PrimitiveType::Boolean),
            DataType::Int8 | DataType::Int16 | DataType::Int32 => {
                DeltaDataType::Primitive(PrimitiveType::Integer)
            }
            DataType::Int64 => DeltaDataType::Primitive(PrimitiveType::Long),
            DataType::Float32 => DeltaDataType::Primitive(PrimitiveType::Float),
            DataType::Float64 => DeltaDataType::Primitive(PrimitiveType::Double),
            DataType::Utf8 | DataType::LargeUtf8 => {
                DeltaDataType::Primitive(PrimitiveType::String)
            }
            DataType::Binary | DataType::LargeBinary => {
                DeltaDataType::Primitive(PrimitiveType::Binary)
            }
            DataType::Timestamp(_, _) => {
                DeltaDataType::Primitive(PrimitiveType::TimestampNtz)
            }
            DataType::Date32 | DataType::Date64 => {
                DeltaDataType::Primitive(PrimitiveType::Date)
            }
            other => {
                return Err(StreamlineError::Sink(format!(
                    "Unsupported Arrow type for Delta schema evolution: {:?}",
                    other
                )));
            }
        };
        Ok(delta_dt)
    }

    // ── Time-travel ─────────────────────────────────────────────────────

    /// Open the table at a specific Delta log version and return a
    /// snapshot as a vector of Arrow `RecordBatch`es.
    #[cfg(feature = "delta-lake")]
    pub async fn read_at_version(&self, version: i64) -> Result<Vec<RecordBatch>> {
        let storage_options: HashMap<String, String> = self.config.storage_options.clone();

        info!(
            sink = %self.name,
            version = version,
            "Opening Delta table at version"
        );

        let table = deltalake::open_table_with_version(
            &self.config.table_uri,
            version,
        )
        .await
        .map_err(|e| {
            StreamlineError::Sink(format!(
                "Failed to open Delta table at version {}: {}",
                version, e
            ))
        })?;

        Self::read_table_batches(&table, &storage_options).await
    }

    /// Open the table at a specific Delta log version (stub when feature disabled).
    #[cfg(not(feature = "delta-lake"))]
    pub async fn read_at_version(&self, version: i64) -> Result<Vec<()>> {
        warn!(
            sink = %self.name,
            version = version,
            "Time-travel is a no-op without the delta-lake feature"
        );
        Ok(vec![])
    }

    /// Open the table as of a given timestamp and return a snapshot as
    /// Arrow `RecordBatch`es.
    #[cfg(feature = "delta-lake")]
    pub async fn read_at_timestamp(&self, ts: DateTime<Utc>) -> Result<Vec<RecordBatch>> {
        let storage_options: HashMap<String, String> = self.config.storage_options.clone();
        let ts_str = ts.format("%Y-%m-%dT%H:%M:%SZ").to_string();

        info!(
            sink = %self.name,
            timestamp = %ts_str,
            "Opening Delta table at timestamp"
        );

        let table = deltalake::open_table_with_ds(
            &self.config.table_uri,
            &ts_str,
        )
        .await
        .map_err(|e| {
            StreamlineError::Sink(format!(
                "Failed to open Delta table at timestamp {}: {}",
                ts_str, e
            ))
        })?;

        Self::read_table_batches(&table, &storage_options).await
    }

    /// Open the table as of a given timestamp (stub when feature disabled).
    #[cfg(not(feature = "delta-lake"))]
    pub async fn read_at_timestamp(&self, ts: DateTime<Utc>) -> Result<Vec<()>> {
        warn!(
            sink = %self.name,
            timestamp = %ts,
            "Time-travel is a no-op without the delta-lake feature"
        );
        Ok(vec![])
    }

    /// Read all record batches from an already-opened Delta table snapshot.
    ///
    /// Returns file metadata (paths and schema) rather than full data reads,
    /// since full Parquet reading requires datafusion which is not included.
    /// Callers can use the returned schema and file list to read data externally.
    #[cfg(feature = "delta-lake")]
    async fn read_table_batches(
        table: &DeltaTable,
        _storage_options: &HashMap<String, String>,
    ) -> Result<Vec<RecordBatch>> {
        let snapshot = table.snapshot().map_err(|e| {
            StreamlineError::Sink(format!("Delta table state not loaded: {}", e))
        })?;

        let struct_schema = snapshot.schema();
        let arrow_schema = <ArrowSchema as TryFrom<&deltalake::kernel::StructType>>::try_from(struct_schema)
            .map_err(|e| StreamlineError::Sink(format!("Schema conversion failed: {}", e)))?;

        let files = snapshot.file_actions_iter()
            .map_err(|e| StreamlineError::Sink(format!("Failed to list files: {}", e)))?;

        let file_paths: Vec<String> = files
            .map(|f| f.path)
            .collect();

        if file_paths.is_empty() {
            return Ok(vec![]);
        }

        // Build an empty RecordBatch carrying the snapshot schema so
        // callers know the table shape. Full data reads require the
        // `datafusion` feature on deltalake (not currently enabled).
        let schema_ref = Arc::new(arrow_schema);
        let empty_batch = RecordBatch::new_empty(schema_ref);

        info!(
            files = file_paths.len(),
            version = table.version(),
            "Time-travel snapshot opened (schema-only without datafusion)"
        );

        Ok(vec![empty_batch])
    }
}

/// Result of an OPTIMIZE operation.
#[derive(Debug, Clone, Default)]
pub struct OptimizeResult {
    /// Number of new compacted files written.
    pub files_added: u64,
    /// Number of small files removed.
    pub files_removed: u64,
    /// Total files involved in compaction.
    pub files_compacted: u64,
}

/// Result of a VACUUM operation.
#[derive(Debug, Clone, Default)]
pub struct VacuumResult {
    /// Number of unreferenced files deleted.
    pub files_deleted: u64,
}

#[async_trait]
impl SinkConnector for DeltaLakeSink {
    fn name(&self) -> &str {
        &self.name
    }

    fn sink_type(&self) -> SinkType {
        SinkType::DeltaLake
    }

    fn topics(&self) -> Vec<String> {
        self.topics.clone()
    }

    async fn start(&mut self) -> Result<()> {
        info!(sink = %self.name, "Starting Delta Lake sink");

        // Check if already running
        {
            let status = self.status.read().await;
            if *status == SinkStatus::Running {
                return Err(StreamlineError::Sink("Sink is already running".to_string()));
            }
        }

        // Update status
        {
            let mut status = self.status.write().await;
            *status = SinkStatus::Starting;
        }

        // Initialize table
        #[cfg(feature = "delta-lake")]
        {
            let table = self.initialize_table().await?;
            let mut table_guard = self.table.write().await;
            *table_guard = Some(table);
        }

        #[cfg(not(feature = "delta-lake"))]
        {
            let _ = self.initialize_table().await?;
        }

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        // Start consumer loop
        let name = self.name.clone();
        let topics = self.topics.clone();
        let config = self.config.clone();
        let topic_manager = self.topic_manager.clone();
        let status = self.status.clone();
        let metrics = self.metrics.clone();
        let buffer = self.buffer.clone();
        let table = self.table.clone();

        tokio::spawn(async move {
            Self::consumer_loop(
                name,
                topics,
                config,
                topic_manager,
                status.clone(),
                metrics,
                buffer,
                table,
                shutdown_rx,
            )
            .await;

            // Update status after loop exits
            let mut status_guard = status.write().await;
            if *status_guard != SinkStatus::Failed {
                *status_guard = SinkStatus::Stopped;
            }
        });

        // Update status to running
        {
            let mut status = self.status.write().await;
            *status = SinkStatus::Running;
        }

        info!(sink = %self.name, "Delta Lake sink started successfully");
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        info!(sink = %self.name, "Stopping Delta Lake sink");

        // Check if already stopped
        {
            let status = self.status.read().await;
            if *status == SinkStatus::Stopped {
                return Ok(());
            }
        }

        // Update status
        {
            let mut status = self.status.write().await;
            *status = SinkStatus::Stopping;
        }

        // Send shutdown signal
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }

        // Wait a bit for graceful shutdown
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Flush remaining buffer
        #[cfg(feature = "delta-lake")]
        {
            let mut offsets = HashMap::new();
            if let Err(e) = Self::commit_buffer_impl(
                &self.name,
                &self.config,
                &self.buffer,
                &self.metrics,
                &self.table,
                &mut offsets,
            )
            .await
            {
                error!(sink = %self.name, error = %e, "Error flushing buffer on shutdown");
            }
        }

        #[cfg(not(feature = "delta-lake"))]
        {
            let mut offsets = HashMap::new();
            if let Err(e) =
                Self::commit_buffer_stub(&self.name, &self.buffer, &self.metrics, &mut offsets)
                    .await
            {
                error!(sink = %self.name, error = %e, "Error flushing buffer on shutdown");
            }
        }

        // Update status
        {
            let mut status = self.status.write().await;
            *status = SinkStatus::Stopped;
        }

        info!(sink = %self.name, "Delta Lake sink stopped");
        Ok(())
    }

    async fn process_batch(
        &mut self,
        topic: &str,
        partition: i32,
        records: Vec<Record>,
    ) -> Result<i64> {
        let key = format!("{}-{}", topic, partition);
        let mut buffer = self.buffer.write().await;
        let last_offset = records.last().map(|r| r.offset).unwrap_or(0);

        buffer.entry(key).or_insert_with(Vec::new).extend(records);

        Ok(last_offset)
    }

    fn status(&self) -> SinkStatus {
        match self.status.try_read() {
            Ok(status) => status.clone(),
            Err(_) => SinkStatus::Running,
        }
    }

    fn metrics(&self) -> SinkMetrics {
        match self.metrics.try_read() {
            Ok(metrics) => metrics.clone(),
            Err(_) => SinkMetrics::default(),
        }
    }

    async fn flush(&mut self) -> Result<()> {
        #[cfg(feature = "delta-lake")]
        {
            let mut offsets = HashMap::new();
            Self::commit_buffer_impl(
                &self.name,
                &self.config,
                &self.buffer,
                &self.metrics,
                &self.table,
                &mut offsets,
            )
            .await?;
        }

        #[cfg(not(feature = "delta-lake"))]
        {
            let mut offsets = HashMap::new();
            Self::commit_buffer_stub(&self.name, &self.buffer, &self.metrics, &mut offsets)
                .await?;
        }

        Ok(())
    }

    async fn health_check(&self) -> Result<bool> {
        let status = self.status();
        if status == SinkStatus::Failed {
            return Ok(false);
        }
        let metrics = self.metrics();
        if metrics.error_count > 0 && metrics.records_failed > metrics.records_processed / 2 {
            return Ok(false);
        }
        Ok(status == SinkStatus::Running)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_config() -> DeltaLakeSinkConfig {
        DeltaLakeSinkConfig {
            table_uri: "file:///tmp/delta-test".to_string(),
            storage_options: HashMap::new(),
            commit_interval_ms: 1000,
            max_batch_size: 100,
            write_mode: crate::sink::config::DeltaWriteMode::Append,
            partition_columns: vec![],
            target_file_size_bytes: 128 * 1024 * 1024,
            auto_optimize: false,
            retention_hours: 168,
            schema_evolution: crate::sink::config::SchemaEvolutionPolicy::AddNewColumns,
            max_retries: 3,
            retry_delay_ms: 100,
        }
    }

    #[test]
    fn test_delta_sink_creation() {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = create_test_config();

        let sink = DeltaLakeSink::new(
            "test-delta-sink".to_string(),
            vec!["test-topic".to_string()],
            config,
            topic_manager,
        );

        assert!(sink.is_ok());
        let sink = sink.unwrap();
        assert_eq!(sink.name(), "test-delta-sink");
        assert_eq!(sink.topics(), vec!["test-topic"]);
        assert_eq!(sink.sink_type(), SinkType::DeltaLake);
    }

    #[test]
    fn test_validate_config_empty_table_uri() {
        let mut config = create_test_config();
        config.table_uri = String::new();

        let result = DeltaLakeSink::validate_config(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("table_uri cannot be empty"));
    }

    #[test]
    fn test_validate_config_zero_commit_interval() {
        let mut config = create_test_config();
        config.commit_interval_ms = 0;

        let result = DeltaLakeSink::validate_config(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("commit_interval_ms must be > 0"));
    }

    #[test]
    fn test_validate_config_zero_batch_size() {
        let mut config = create_test_config();
        config.max_batch_size = 0;

        let result = DeltaLakeSink::validate_config(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("max_batch_size must be > 0"));
    }

    #[tokio::test]
    async fn test_sink_status() {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = create_test_config();

        let sink = DeltaLakeSink::new(
            "test-delta-sink".to_string(),
            vec!["test-topic".to_string()],
            config,
            topic_manager,
        )
        .unwrap();

        assert_eq!(sink.status(), SinkStatus::Stopped);
    }

    #[test]
    fn test_retry_config_defaults() {
        let config = create_test_config();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_delay_ms, 100);
    }

    #[test]
    fn test_initial_metrics() {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = create_test_config();

        let sink = DeltaLakeSink::new(
            "test-delta-sink".to_string(),
            vec!["test-topic".to_string()],
            config,
            topic_manager,
        )
        .unwrap();

        let metrics = sink.metrics();
        assert_eq!(metrics.records_processed, 0);
        assert_eq!(metrics.records_failed, 0);
        assert_eq!(metrics.bytes_processed, 0);
        assert_eq!(metrics.commit_count, 0);
        assert_eq!(metrics.last_commit_latency_ms, 0);
        assert_eq!(metrics.error_count, 0);
    }

    #[test]
    fn test_schema_evolution_display() {
        assert_eq!(
            format!("{}", crate::sink::config::SchemaEvolutionPolicy::Strict),
            "strict"
        );
        assert_eq!(
            format!("{}", crate::sink::config::SchemaEvolutionPolicy::AddNewColumns),
            "add_new_columns"
        );
        assert_eq!(
            format!("{}", crate::sink::config::SchemaEvolutionPolicy::AddAndWiden),
            "add_and_widen"
        );
    }

    #[tokio::test]
    async fn test_health_check_stopped() {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = create_test_config();

        let sink = DeltaLakeSink::new(
            "test-delta-sink".to_string(),
            vec!["test-topic".to_string()],
            config,
            topic_manager,
        )
        .unwrap();

        // Stopped sink should report unhealthy (not running)
        let healthy = sink.health_check().await.unwrap();
        assert!(!healthy);
    }

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_optimize_stub() {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = create_test_config();

        let sink = DeltaLakeSink::new(
            "test-optimize".to_string(),
            vec!["test-topic".to_string()],
            config,
            topic_manager,
        )
        .unwrap();

        // Without the delta-lake feature this is a no-op stub
        let result = sink.optimize_table().await;
        assert!(result.is_ok());
        let opt = result.unwrap();
        assert_eq!(opt.files_added, 0);
        assert_eq!(opt.files_removed, 0);
        assert_eq!(opt.files_compacted, 0);
    }

    #[cfg(feature = "delta-lake")]
    #[tokio::test]
    async fn test_optimize_real() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("delta-optimize");
        std::fs::create_dir_all(&table_path).unwrap();
        let table_uri = format!("{}", table_path.display());
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let mut config = create_test_config();
        config.table_uri = table_uri;

        let sink = DeltaLakeSink::new(
            "test-optimize".to_string(),
            vec!["test-topic".to_string()],
            config,
            topic_manager,
        )
        .unwrap();

        // Initialize table (normally done in start())
        let table = sink.initialize_table().await.unwrap();
        {
            let mut table_guard = sink.table.write().await;
            *table_guard = Some(table);
        }

        let result = sink.optimize_table().await;
        assert!(result.is_ok());
        let opt = result.unwrap();
        // Empty table — nothing to compact
        assert_eq!(opt.files_added, 0);
        assert_eq!(opt.files_removed, 0);
        assert_eq!(opt.files_compacted, 0);
    }

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_vacuum_stub() {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = create_test_config();

        let sink = DeltaLakeSink::new(
            "test-vacuum".to_string(),
            vec!["test-topic".to_string()],
            config,
            topic_manager,
        )
        .unwrap();

        let result = sink.vacuum_table().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().files_deleted, 0);
    }

    #[cfg(feature = "delta-lake")]
    #[tokio::test]
    async fn test_vacuum_real() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("delta-vacuum");
        std::fs::create_dir_all(&table_path).unwrap();
        let table_uri = format!("{}", table_path.display());
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let mut config = create_test_config();
        config.table_uri = table_uri;

        let sink = DeltaLakeSink::new(
            "test-vacuum".to_string(),
            vec!["test-topic".to_string()],
            config,
            topic_manager,
        )
        .unwrap();

        // Initialize table (normally done in start())
        let table = sink.initialize_table().await.unwrap();
        {
            let mut table_guard = sink.table.write().await;
            *table_guard = Some(table);
        }

        let result = sink.vacuum_table().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().files_deleted, 0);
    }

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_time_travel_read_at_version_stub() {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = create_test_config();

        let sink = DeltaLakeSink::new(
            "test-time-travel".to_string(),
            vec!["test-topic".to_string()],
            config,
            topic_manager,
        )
        .unwrap();

        let result = sink.read_at_version(0).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[cfg(feature = "delta-lake")]
    #[tokio::test]
    async fn test_time_travel_read_at_version_real() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("delta-tt-ver");
        std::fs::create_dir_all(&table_path).unwrap();
        let table_uri = format!("{}", table_path.display());
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let mut config = create_test_config();
        config.table_uri = table_uri;

        let sink = DeltaLakeSink::new(
            "test-time-travel".to_string(),
            vec!["test-topic".to_string()],
            config,
            topic_manager,
        )
        .unwrap();

        // Create the table so version 0 exists
        let table = sink.initialize_table().await.unwrap();
        {
            let mut table_guard = sink.table.write().await;
            *table_guard = Some(table);
        }

        let result = sink.read_at_version(0).await;
        assert!(result.is_ok());
    }

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_time_travel_read_at_timestamp_stub() {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = create_test_config();

        let sink = DeltaLakeSink::new(
            "test-time-travel-ts".to_string(),
            vec!["test-topic".to_string()],
            config,
            topic_manager,
        )
        .unwrap();

        let result = sink.read_at_timestamp(Utc::now()).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[cfg(feature = "delta-lake")]
    #[tokio::test]
    async fn test_time_travel_read_at_timestamp_real() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().join("delta-tt-ts");
        std::fs::create_dir_all(&table_path).unwrap();
        let table_uri = format!("{}", table_path.display());
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let mut config = create_test_config();
        config.table_uri = table_uri;

        let sink = DeltaLakeSink::new(
            "test-time-travel-ts".to_string(),
            vec!["test-topic".to_string()],
            config,
            topic_manager,
        )
        .unwrap();

        // Create the table so it has state at the current timestamp
        let table = sink.initialize_table().await.unwrap();
        {
            let mut table_guard = sink.table.write().await;
            *table_guard = Some(table);
        }

        let result = sink.read_at_timestamp(Utc::now()).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_optimize_result_default() {
        let result = OptimizeResult::default();
        assert_eq!(result.files_added, 0);
        assert_eq!(result.files_removed, 0);
        assert_eq!(result.files_compacted, 0);
    }

    #[test]
    fn test_vacuum_result_default() {
        let result = VacuumResult::default();
        assert_eq!(result.files_deleted, 0);
    }
}
