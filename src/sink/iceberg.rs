//! Apache Iceberg sink implementation
//!
//! This module provides a sink connector that writes Streamline records
//! to Apache Iceberg tables, enabling streaming-to-lakehouse integration.
//!
//! ## Features
//!
//! - REST catalog support (Hive and Glue catalogs planned — see [`CatalogStatus`](crate::sink::config::CatalogStatus))
//! - Schema evolution policies: Strict, AddNewColumns, AddAndPromote
//! - Time-based and field-based partitioning
//! - Automatic schema inference from Kafka records
//! - Parquet file format for efficient storage
//! - Configurable commit intervals and batch sizes
//! - DataFile registration with Iceberg table via transactions
//!
//! ## Example Configuration
//!
//! ```toml
//! [[sinks]]
//! name = "events-to-iceberg"
//! type = "iceberg"
//! topics = ["events"]
//! catalog_uri = "http://localhost:8181"
//! namespace = "default"
//! table = "events"
//! commit_interval_ms = 60000
//! max_batch_size = 10000
//!
//! [sinks.partitioning]
//! strategy = "time_based_hour"
//! time_granularity = "hour"
//! ```

use crate::error::{Result, StreamlineError};
use crate::sink::config::IcebergSinkConfig;
use crate::sink::{SinkConnector, SinkMetrics, SinkStatus};
use crate::storage::{Record, TopicManager};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

// Iceberg and Arrow imports (only with iceberg feature)
#[cfg(feature = "iceberg")]
use arrow::array::{
    ArrayRef, BinaryBuilder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
};
#[cfg(feature = "iceberg")]
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
#[cfg(feature = "iceberg")]
use arrow::record_batch::RecordBatch;
#[cfg(feature = "iceberg")]
use chrono::{TimeZone, Utc};
// Note: FileIOBuilder may be needed for future features like remote storage
// #[cfg(feature = "iceberg")]
// use iceberg::io::FileIOBuilder;
#[cfg(feature = "iceberg")]
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
#[cfg(feature = "iceberg")]
use iceberg::table::Table;
#[cfg(feature = "iceberg")]
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
#[cfg(feature = "iceberg")]
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
// Note: HMS and Glue catalog support disabled until upstream hive_metastore dependency is fixed
// #[cfg(feature = "iceberg")]
// use iceberg_catalog_hms::{HmsCatalog, HmsCatalogConfig};
// #[cfg(feature = "iceberg")]
// use iceberg_catalog_glue::{GlueCatalog, GlueCatalogConfig};
#[cfg(feature = "iceberg")]
use crate::sink::config::CatalogType;
#[cfg(feature = "iceberg")]
use parquet::arrow::ArrowWriter;
#[cfg(feature = "iceberg")]
use parquet::basic::Compression;
#[cfg(feature = "iceberg")]
use parquet::file::properties::WriterProperties;
#[cfg(feature = "iceberg")]
use std::fs::File;
#[cfg(feature = "iceberg")]
use std::path::Path;

// Catalog availability:
//   REST  — fully supported via `iceberg-catalog-rest`
//   Hive  — planned, blocked by upstream `hive_metastore` dep compilation issues
//   Glue  — planned, blocked by upstream `hive_metastore` dep (required by Glue)
// Use `CatalogType::status()` / `CatalogType::status_message()` to query at runtime.

/// Apache Iceberg sink connector
///
/// This connector consumes records from Streamline topics and writes them
/// to Apache Iceberg tables. It handles:
/// - Schema evolution and compatibility
/// - Partitioning (time-based or field-based)
/// - Batching and buffering
/// - Periodic commits
pub struct IcebergSink {
    /// Sink name
    name: String,
    /// Topics to consume from
    topics: Vec<String>,
    /// Configuration
    config: IcebergSinkConfig,
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
    /// Iceberg catalog (currently only REST catalog is supported)
    #[cfg(feature = "iceberg")]
    catalog: Option<Arc<RestCatalog>>,
    /// Iceberg table handle
    #[cfg(feature = "iceberg")]
    table: Arc<RwLock<Option<Table>>>,
    /// Iceberg table identifier (for loading table in commit)
    #[cfg(feature = "iceberg")]
    table_ident: Option<TableIdent>,
    /// Arrow schema for record batches
    #[cfg(feature = "iceberg")]
    arrow_schema: Option<Arc<ArrowSchema>>,
}

impl IcebergSink {
    /// Create a new Iceberg sink
    pub fn new(
        name: String,
        topics: Vec<String>,
        config: IcebergSinkConfig,
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
            #[cfg(feature = "iceberg")]
            catalog: None,
            #[cfg(feature = "iceberg")]
            table: Arc::new(RwLock::new(None)),
            #[cfg(feature = "iceberg")]
            table_ident: None,
            #[cfg(feature = "iceberg")]
            arrow_schema: None,
        })
    }

    /// Validate configuration
    fn validate_config(config: &IcebergSinkConfig) -> Result<()> {
        if config.catalog_uri.is_empty() {
            return Err(StreamlineError::Sink(
                "catalog_uri cannot be empty".to_string(),
            ));
        }

        if config.table.is_empty() {
            return Err(StreamlineError::Sink("table cannot be empty".to_string()));
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

    /// Create Arrow schema for Streamline records
    #[cfg(feature = "iceberg")]
    fn create_arrow_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("offset", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("key", DataType::Binary, true),
            Field::new("value", DataType::Binary, false),
            Field::new("headers_json", DataType::Utf8, true),
            Field::new("_partition_value", DataType::Utf8, true),
        ])
    }

    /// Create Iceberg schema for the table
    #[cfg(feature = "iceberg")]
    fn create_iceberg_schema() -> Result<IcebergSchema> {
        IcebergSchema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "offset",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    2,
                    "timestamp",
                    Type::Primitive(PrimitiveType::Timestamptz),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "key",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::required(
                    4,
                    "value",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::optional(
                    5,
                    "headers_json",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    6,
                    "_partition_value",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .map_err(|e| StreamlineError::Sink(format!("Failed to build Iceberg schema: {}", e)))
    }

    /// Convert records to Arrow RecordBatch
    #[cfg(feature = "iceberg")]
    fn records_to_arrow_batch(
        records: &[Record],
        schema: &Arc<ArrowSchema>,
        config: &IcebergSinkConfig,
    ) -> Result<RecordBatch> {
        let len = records.len();

        // Build offset array
        let mut offset_builder = Int64Builder::with_capacity(len);
        for record in records {
            offset_builder.append_value(record.offset);
        }
        let offsets: ArrayRef = Arc::new(offset_builder.finish());

        // Build timestamp array
        let mut timestamp_builder = TimestampMillisecondBuilder::with_capacity(len);
        for record in records {
            timestamp_builder.append_value(record.timestamp);
        }
        let timestamps: ArrayRef = Arc::new(timestamp_builder.finish().with_timezone("UTC"));

        // Build key array (nullable binary)
        let mut key_builder = BinaryBuilder::with_capacity(len, len * 64);
        for record in records {
            match &record.key {
                Some(k) => key_builder.append_value(k.as_ref()),
                None => key_builder.append_null(),
            }
        }
        let keys: ArrayRef = Arc::new(key_builder.finish());

        // Build value array
        let mut value_builder = BinaryBuilder::with_capacity(len, len * 256);
        for record in records {
            value_builder.append_value(record.value.as_ref());
        }
        let values: ArrayRef = Arc::new(value_builder.finish());

        // Build headers_json array (JSON-serialized headers)
        let mut headers_builder = StringBuilder::with_capacity(len, len * 128);
        for record in records {
            if record.headers.is_empty() {
                headers_builder.append_null();
            } else {
                let json =
                    serde_json::to_string(&record.headers).unwrap_or_else(|_| "[]".to_string());
                headers_builder.append_value(&json);
            }
        }
        let headers_json: ArrayRef = Arc::new(headers_builder.finish());

        // Build partition value column based on timestamp and strategy
        let mut partition_builder = StringBuilder::with_capacity(len, len * 16);
        for record in records {
            let partition_value =
                Self::compute_partition_value(record.timestamp, &config.partitioning.strategy);
            match partition_value {
                Some(v) => partition_builder.append_value(&v),
                None => partition_builder.append_null(),
            }
        }
        let partition_values: ArrayRef = Arc::new(partition_builder.finish());

        RecordBatch::try_new(
            schema.clone(),
            vec![
                offsets,
                timestamps,
                keys,
                values,
                headers_json,
                partition_values,
            ],
        )
        .map_err(|e| StreamlineError::Sink(format!("Failed to create Arrow RecordBatch: {}", e)))
    }

    /// Compute partition value from timestamp based on strategy
    #[cfg(feature = "iceberg")]
    fn compute_partition_value(
        timestamp_ms: i64,
        strategy: &crate::sink::config::PartitioningStrategy,
    ) -> Option<String> {
        use crate::sink::config::PartitioningStrategy;

        let dt = Utc.timestamp_millis_opt(timestamp_ms).single()?;

        match strategy {
            PartitioningStrategy::TimeBasedHour => Some(dt.format("%Y-%m-%d-%H").to_string()),
            PartitioningStrategy::TimeBasedDay => Some(dt.format("%Y-%m-%d").to_string()),
            PartitioningStrategy::TimeBasedMonth => Some(dt.format("%Y-%m").to_string()),
            PartitioningStrategy::None | PartitioningStrategy::FieldBased => None,
        }
    }

    /// Compute partition value from record field (field-based partitioning)
    #[cfg(feature = "iceberg")]
    fn compute_field_partition_value(record: &Record, field_name: &str) -> Option<String> {
        // Try to extract the field value from JSON-encoded record value
        if let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(record.value.as_ref()) {
            if let Some(field_value) = json_value.get(field_name) {
                return match field_value {
                    serde_json::Value::String(s) => Some(s.clone()),
                    serde_json::Value::Number(n) => Some(n.to_string()),
                    serde_json::Value::Bool(b) => Some(b.to_string()),
                    _ => Some(field_value.to_string().trim_matches('"').to_string()),
                };
            }
        }

        // Try extracting from key if value didn't have the field
        if let Some(key) = &record.key {
            if let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(key.as_ref()) {
                if let Some(field_value) = json_value.get(field_name) {
                    return match field_value {
                        serde_json::Value::String(s) => Some(s.clone()),
                        serde_json::Value::Number(n) => Some(n.to_string()),
                        serde_json::Value::Bool(b) => Some(b.to_string()),
                        _ => Some(field_value.to_string().trim_matches('"').to_string()),
                    };
                }
            }
        }

        None
    }

    /// Get partition value for a record based on partitioning config
    #[cfg(feature = "iceberg")]
    fn get_partition_value(
        record: &Record,
        config: &crate::sink::config::PartitioningConfig,
    ) -> Option<String> {
        use crate::sink::config::PartitioningStrategy;

        match &config.strategy {
            PartitioningStrategy::FieldBased => {
                if let Some(field_name) = &config.field {
                    Self::compute_field_partition_value(record, field_name)
                } else {
                    // Fallback to timestamp-based if no field specified
                    Self::compute_partition_value(
                        record.timestamp,
                        &PartitioningStrategy::TimeBasedDay,
                    )
                }
            }
            other => Self::compute_partition_value(record.timestamp, other),
        }
    }

    /// Base schema field names always present in the Iceberg table.
    /// Not considered "user data" fields for schema evolution purposes.
    #[cfg(feature = "iceberg")]
    const BASE_SCHEMA_FIELDS: &'static [&'static str] = &[
        "offset",
        "timestamp",
        "key",
        "value",
        "headers_json",
        "_partition_value",
    ];

    /// Infer an Arrow [`DataType`] from a JSON value.
    #[cfg(feature = "iceberg")]
    fn infer_arrow_type(value: &serde_json::Value) -> DataType {
        match value {
            serde_json::Value::Null => DataType::Utf8,
            serde_json::Value::Bool(_) => DataType::Boolean,
            serde_json::Value::Number(n) => {
                if n.is_f64() && !n.is_i64() && !n.is_u64() {
                    DataType::Float64
                } else {
                    DataType::Int64
                }
            }
            serde_json::Value::String(_) => DataType::Utf8,
            // Arrays and nested objects are stored as JSON strings
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => DataType::Utf8,
        }
    }

    /// Check if widening `from` to `to` is a compatible type promotion.
    ///
    /// Compatible promotions follow Iceberg's type promotion rules:
    /// `int` → `long`, `float` → `double`, `int` → `double`, `long` → `double`.
    #[cfg(feature = "iceberg")]
    #[allow(dead_code)] // Tested; will be used when schema ALTER is wired in
    fn is_compatible_promotion(from: &DataType, to: &DataType) -> bool {
        matches!(
            (from, to),
            (DataType::Int32, DataType::Int64)
                | (DataType::Float32, DataType::Float64)
                | (DataType::Int32, DataType::Float64)
                | (DataType::Int64, DataType::Float64)
        )
    }

    /// Check incoming records against the schema evolution policy.
    ///
    /// Parses a sample of JSON record values to detect fields not present in the
    /// current Arrow schema. Based on the configured policy:
    /// - **Strict**: returns an error if unknown fields are detected
    /// - **AddNewColumns**: logs new fields and allows the batch to proceed
    /// - **AddAndPromote**: like AddNewColumns, plus allows compatible type widening
    ///
    /// Returns the list of new `(field_name, inferred_type)` pairs detected.
    /// Returns an empty list if records aren't JSON or all fields are already known.
    #[cfg(feature = "iceberg")]
    fn check_schema_evolution(
        schema: &ArrowSchema,
        records: &[Record],
        policy: &crate::sink::config::IcebergSchemaEvolution,
        sink_name: &str,
    ) -> Result<Vec<(String, DataType)>> {
        use crate::sink::config::IcebergSchemaEvolution;

        let known_fields: HashSet<&str> = schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .chain(Self::BASE_SCHEMA_FIELDS.iter().copied())
            .collect();

        let mut new_fields: HashMap<String, DataType> = HashMap::new();

        // Sample up to 100 records to detect JSON structure
        let sample_size = records.len().min(100);
        for record in &records[..sample_size] {
            if let Ok(serde_json::Value::Object(map)) =
                serde_json::from_slice::<serde_json::Value>(record.value.as_ref())
            {
                for (key, value) in &map {
                    if !known_fields.contains(key.as_str()) {
                        new_fields
                            .entry(key.clone())
                            .or_insert_with(|| Self::infer_arrow_type(value));
                    }
                }
            }
        }

        if new_fields.is_empty() {
            debug!(
                sink = %sink_name,
                policy = %policy,
                "Schema evolution check passed: no new fields detected"
            );
            return Ok(vec![]);
        }

        let new_field_list: Vec<(String, DataType)> = new_fields.into_iter().collect();

        match policy {
            IcebergSchemaEvolution::Strict => {
                let field_names: Vec<&str> =
                    new_field_list.iter().map(|(n, _)| n.as_str()).collect();
                Err(StreamlineError::Sink(format!(
                    "Schema evolution policy is 'strict' but records contain unknown fields: {:?}. \
                     Change policy to 'add_new_columns' or 'add_and_promote' to allow schema changes.",
                    field_names
                )))
            }
            IcebergSchemaEvolution::AddNewColumns => {
                info!(
                    sink = %sink_name,
                    new_fields = ?new_field_list.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
                    "Schema evolution: new fields detected, will be added as nullable columns"
                );
                Ok(new_field_list)
            }
            IcebergSchemaEvolution::AddAndPromote => {
                info!(
                    sink = %sink_name,
                    new_fields = ?new_field_list.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
                    "Schema evolution: new fields detected, adding columns with type promotion"
                );
                Ok(new_field_list)
            }
        }
    }

    /// Write Arrow RecordBatch to a Parquet file
    ///
    /// Returns the path to the written file and the number of bytes written.
    #[cfg(feature = "iceberg")]
    fn write_parquet_file(
        batch: &RecordBatch,
        output_dir: &str,
        table_name: &str,
        partition_value: Option<&str>,
        compression: &crate::sink::config::ParquetCompression,
    ) -> Result<(String, u64)> {
        use crate::sink::config::ParquetCompression;

        // Create output directory structure
        let base_path = Path::new(output_dir).join(table_name);

        let partition_path = if let Some(pv) = partition_value {
            base_path.join(format!("_partition_value={}", pv))
        } else {
            base_path
        };

        std::fs::create_dir_all(&partition_path).map_err(|e| {
            StreamlineError::Sink(format!("Failed to create output directory: {}", e))
        })?;

        // Generate unique filename with timestamp and UUID
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let uuid_suffix = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let filename = format!("{}_{}.parquet", timestamp, uuid_suffix);
        let file_path = partition_path.join(&filename);

        // Create Parquet writer with configured compression
        let file = File::create(&file_path)
            .map_err(|e| StreamlineError::Sink(format!("Failed to create Parquet file: {}", e)))?;

        // Map our compression enum to Parquet's Compression type
        let parquet_compression = match compression {
            ParquetCompression::None => Compression::UNCOMPRESSED,
            ParquetCompression::Snappy => Compression::SNAPPY,
            ParquetCompression::Gzip => Compression::GZIP(Default::default()),
            ParquetCompression::Lz4 => Compression::LZ4,
            ParquetCompression::Zstd => Compression::ZSTD(Default::default()),
        };

        let props = WriterProperties::builder()
            .set_compression(parquet_compression)
            .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
            .map_err(|e| StreamlineError::Sink(format!("Failed to create Arrow writer: {}", e)))?;

        // Write the batch
        writer
            .write(batch)
            .map_err(|e| StreamlineError::Sink(format!("Failed to write RecordBatch: {}", e)))?;

        // Close the writer and get metadata
        let metadata = writer
            .close()
            .map_err(|e| StreamlineError::Sink(format!("Failed to close Parquet writer: {}", e)))?;

        let rows_written = metadata.num_rows as u64;
        let file_path_str = file_path.to_string_lossy().to_string();

        Ok((file_path_str, rows_written))
    }

    /// Register a Parquet file with the Iceberg catalog
    ///
    /// This creates a DataFile entry and appends it to the table via a transaction.
    /// Uses exponential backoff retry for transient failures.
    #[cfg(feature = "iceberg")]
    #[allow(clippy::too_many_arguments)]
    async fn register_data_file(
        catalog: &Arc<RestCatalog>,
        table_ident: &TableIdent,
        file_path: &str,
        record_count: u64,
        file_size: u64,
        sink_name: &str,
        max_retries: u32,
        retry_delay_ms: u64,
    ) -> Result<()> {
        let mut last_error = None;
        let mut delay = Duration::from_millis(retry_delay_ms);

        for attempt in 0..=max_retries {
            if attempt > 0 {
                debug!(
                    sink = %sink_name,
                    attempt = attempt,
                    delay_ms = delay.as_millis(),
                    "Retrying Iceberg commit after failure"
                );
                tokio::time::sleep(delay).await;
                // Exponential backoff with jitter
                delay = Duration::from_millis(
                    (delay.as_millis() as u64 * 2).min(30_000) + rand::random::<u64>() % 100,
                );
            }

            match Self::try_register_data_file(
                catalog,
                table_ident,
                file_path,
                record_count,
                file_size,
            )
            .await
            {
                Ok(()) => {
                    if attempt > 0 {
                        info!(
                            sink = %sink_name,
                            file = %file_path,
                            attempt = attempt,
                            "Iceberg commit succeeded after retry"
                        );
                    } else {
                        info!(
                            sink = %sink_name,
                            file = %file_path,
                            records = record_count,
                            bytes = file_size,
                            "Registered DataFile with Iceberg catalog"
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < max_retries {
                        debug!(
                            sink = %sink_name,
                            attempt = attempt,
                            error = ?last_error,
                            "Iceberg commit failed, will retry"
                        );
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            StreamlineError::Sink("Iceberg commit failed with unknown error".to_string())
        }))
    }

    /// Internal helper to attempt a single data file registration
    #[cfg(feature = "iceberg")]
    async fn try_register_data_file(
        catalog: &Arc<RestCatalog>,
        table_ident: &TableIdent,
        file_path: &str,
        record_count: u64,
        file_size: u64,
    ) -> Result<()> {
        use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat};
        use iceberg::transaction::Transaction;

        // Load the table fresh to get current metadata
        let table = catalog.load_table(table_ident).await.map_err(|e| {
            StreamlineError::Sink(format!("Failed to load table for commit: {}", e))
        })?;

        // Build the DataFile descriptor
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(file_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(file_size)
            .record_count(record_count)
            .build()
            .map_err(|e| StreamlineError::Sink(format!("Failed to build DataFile: {}", e)))?;

        // Create transaction with FastAppend action
        let tx = Transaction::new(&table);
        let mut action = tx
            .fast_append(None, vec![])
            .map_err(|e| StreamlineError::Sink(format!("Failed to create append action: {}", e)))?;
        action
            .add_data_files(vec![data_file])
            .map_err(|e| StreamlineError::Sink(format!("Failed to add data file: {}", e)))?;
        let tx = action
            .apply()
            .await
            .map_err(|e| StreamlineError::Sink(format!("Failed to apply append action: {}", e)))?;
        let _updated_table = tx
            .commit(catalog.as_ref())
            .await
            .map_err(|e| StreamlineError::Sink(format!("Failed to commit transaction: {}", e)))?;

        Ok(())
    }

    /// Initialize Iceberg catalog connection
    #[cfg(feature = "iceberg")]
    async fn initialize_catalog(&mut self) -> Result<()> {
        info!(
            sink = %self.name,
            catalog_uri = %self.config.catalog_uri,
            catalog_type = %self.config.catalog_type,
            table = %self.config.table,
            "Initializing Iceberg catalog connection"
        );

        // Create the appropriate catalog based on type
        // Note: Currently only REST catalog is supported. Hive and Glue catalog support
        // is planned but blocked by upstream `hive_metastore` dependency compilation issues.
        let catalog: RestCatalog = match self.config.catalog_type {
            CatalogType::Rest => {
                let catalog_config = RestCatalogConfig::builder()
                    .uri(self.config.catalog_uri.clone())
                    .build();
                RestCatalog::new(catalog_config)
            }
            CatalogType::Hive => {
                return Err(StreamlineError::Sink(format!(
                    "Hive catalog is currently '{}': {}",
                    self.config.catalog_type.status(),
                    self.config.catalog_type.status_message(),
                )));
            }
            CatalogType::Glue => {
                return Err(StreamlineError::Sink(format!(
                    "Glue catalog is currently '{}': {}",
                    self.config.catalog_type.status(),
                    self.config.catalog_type.status_message(),
                )));
            }
        };

        // Create namespace identifier
        let namespace = NamespaceIdent::new(self.config.namespace.clone());

        // Check if namespace exists, create if not
        let namespace_exists = catalog.namespace_exists(&namespace).await.map_err(|e| {
            StreamlineError::Sink(format!("Failed to check namespace existence: {}", e))
        })?;

        if !namespace_exists {
            info!(sink = %self.name, namespace = %self.config.namespace, "Creating namespace");
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await
                .map_err(|e| StreamlineError::Sink(format!("Failed to create namespace: {}", e)))?;
        }

        // Create table identifier
        let table_ident = TableIdent::new(namespace.clone(), self.config.table.clone());

        // Check if table exists, load or create
        let table_exists = catalog.table_exists(&table_ident).await.map_err(|e| {
            StreamlineError::Sink(format!("Failed to check table existence: {}", e))
        })?;

        let table = if table_exists {
            info!(sink = %self.name, table = %self.config.table, "Loading existing Iceberg table");
            catalog
                .load_table(&table_ident)
                .await
                .map_err(|e| StreamlineError::Sink(format!("Failed to load table: {}", e)))?
        } else {
            info!(sink = %self.name, table = %self.config.table, "Creating new Iceberg table");

            // Create table with our schema
            let iceberg_schema = Self::create_iceberg_schema()?;
            let creation = TableCreation::builder()
                .name(self.config.table.clone())
                .schema(iceberg_schema)
                .build();

            catalog
                .create_table(&namespace, creation)
                .await
                .map_err(|e| StreamlineError::Sink(format!("Failed to create table: {}", e)))?
        };

        // Store handles
        self.catalog = Some(Arc::new(catalog));
        self.table_ident = Some(table_ident);
        {
            let mut table_guard = self.table.write().await;
            *table_guard = Some(table);
        }
        self.arrow_schema = Some(Arc::new(Self::create_arrow_schema()));

        info!(sink = %self.name, "Iceberg catalog initialized successfully");
        Ok(())
    }

    /// Stub initialize_catalog for non-iceberg builds
    #[cfg(not(feature = "iceberg"))]
    #[allow(unused)]
    async fn initialize_catalog(&mut self) -> Result<()> {
        warn!(
            sink = %self.name,
            "Iceberg sink requires --features iceberg to be enabled"
        );
        Ok(())
    }

    /// Start the consumer loop
    #[allow(clippy::too_many_arguments)]
    #[cfg(feature = "iceberg")]
    async fn consumer_loop(
        name: String,
        topics: Vec<String>,
        config: IcebergSinkConfig,
        topic_manager: Arc<TopicManager>,
        status: Arc<RwLock<SinkStatus>>,
        metrics: Arc<RwLock<SinkMetrics>>,
        buffer: Arc<RwLock<HashMap<String, Vec<Record>>>>,
        table: Arc<RwLock<Option<Table>>>,
        catalog: Arc<RestCatalog>,
        table_ident: TableIdent,
        arrow_schema: Arc<ArrowSchema>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        info!(sink = %name, topics = ?topics, "Starting consumer loop");

        let mut commit_interval = interval(Duration::from_millis(config.commit_interval_ms));

        // Track offsets per topic-partition
        let mut offsets: HashMap<String, i64> = HashMap::new();

        loop {
            tokio::select! {
                _ = commit_interval.tick() => {
                    // Commit buffered records
                    if let Err(e) = Self::commit_buffer(
                        &name,
                        &config,
                        &buffer,
                        &metrics,
                        &table,
                        &catalog,
                        &table_ident,
                        &arrow_schema,
                        &mut offsets,
                    ).await {
                        error!(sink = %name, error = %e, "Error committing buffer");
                        let mut status_guard = status.write().await;
                        *status_guard = SinkStatus::Failed;
                        let mut metrics_guard = metrics.write().await;
                        metrics_guard.error_message = Some(e.to_string());
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

        info!(sink = %name, "Consumer loop stopped");
    }

    /// Start the consumer loop (non-iceberg version)
    #[allow(clippy::too_many_arguments)]
    #[cfg(not(feature = "iceberg"))]
    async fn consumer_loop(
        name: String,
        topics: Vec<String>,
        config: IcebergSinkConfig,
        topic_manager: Arc<TopicManager>,
        status: Arc<RwLock<SinkStatus>>,
        metrics: Arc<RwLock<SinkMetrics>>,
        buffer: Arc<RwLock<HashMap<String, Vec<Record>>>>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        info!(sink = %name, topics = ?topics, "Starting consumer loop (iceberg feature not enabled)");

        let mut commit_interval = interval(Duration::from_millis(config.commit_interval_ms));
        let mut offsets: HashMap<String, i64> = HashMap::new();

        loop {
            tokio::select! {
                _ = commit_interval.tick() => {
                    if let Err(e) = Self::commit_buffer(
                        &name,
                        &config,
                        &buffer,
                        &metrics,
                        &mut offsets,
                    ).await {
                        error!(sink = %name, error = %e, "Error committing buffer");
                        let mut status_guard = status.write().await;
                        *status_guard = SinkStatus::Failed;
                        let mut metrics_guard = metrics.write().await;
                        metrics_guard.error_message = Some(e.to_string());
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!(sink = %name, "Shutdown signal received");
                        break;
                    }
                }
            }

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

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!(sink = %name, "Consumer loop stopped");
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
                // This will be committed on next interval tick
            }
        }

        Ok(total_consumed)
    }

    /// Commit buffered records to Iceberg (iceberg feature enabled)
    #[cfg(feature = "iceberg")]
    #[allow(clippy::too_many_arguments)]
    async fn commit_buffer(
        name: &str,
        config: &IcebergSinkConfig,
        buffer: &Arc<RwLock<HashMap<String, Vec<Record>>>>,
        metrics: &Arc<RwLock<SinkMetrics>>,
        _table: &Arc<RwLock<Option<Table>>>,
        catalog: &Arc<RestCatalog>,
        table_ident: &TableIdent,
        arrow_schema: &Arc<ArrowSchema>,
        _offsets: &mut HashMap<String, i64>,
    ) -> Result<()> {
        let mut buffer_guard = buffer.write().await;

        if buffer_guard.is_empty() {
            return Ok(());
        }

        let commit_start = std::time::Instant::now();

        // Collect all records from buffer
        let all_records: Vec<Record> = buffer_guard.values().flatten().cloned().collect();
        let total_records = all_records.len();

        debug!(sink = %name, records = total_records, "Committing buffer to Iceberg");

        // Check schema evolution policy against incoming records
        let _new_fields =
            Self::check_schema_evolution(arrow_schema, &all_records, &config.schema_evolution, name)?;

        // Convert records to Arrow RecordBatch
        let batch = Self::records_to_arrow_batch(&all_records, arrow_schema, config)?;

        // Determine partition value from the first record using config
        let partition_value = all_records
            .first()
            .and_then(|r| Self::get_partition_value(r, &config.partitioning));

        // Write to Parquet file with configured compression
        let (file_path, rows_written) = Self::write_parquet_file(
            &batch,
            &config.output_dir,
            &config.table,
            partition_value.as_deref(),
            &config.compression,
        )?;

        // Calculate file size for DataFile metadata
        let file_size = match std::fs::metadata(&file_path) {
            Ok(m) => m.len(),
            Err(e) => {
                warn!(
                    sink = %name,
                    file = %file_path,
                    error = %e,
                    "Failed to read Parquet file metadata, reporting size as 0"
                );
                0
            }
        };

        info!(
            sink = %name,
            records = total_records,
            rows = rows_written,
            file = %file_path,
            partition = ?partition_value,
            compression = %config.compression,
            "Wrote Parquet file"
        );

        // Register the file with Iceberg catalog (with retry logic)
        if let Err(e) = Self::register_data_file(
            catalog,
            table_ident,
            &file_path,
            rows_written,
            file_size,
            name,
            config.max_retries,
            config.retry_delay_ms,
        )
        .await
        {
            // Track failed records in metrics
            let mut metrics_guard = metrics.write().await;
            metrics_guard.records_failed += total_records as u64;
            metrics_guard.error_count += 1;
            metrics_guard.error_message = Some(e.to_string());
            return Err(e);
        }

        let commit_latency = commit_start.elapsed();

        // Update metrics
        let mut metrics_guard = metrics.write().await;
        metrics_guard.records_processed += total_records as u64;
        metrics_guard.last_commit_timestamp = chrono::Utc::now().timestamp_millis();
        metrics_guard.bytes_processed += file_size;
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
            bytes = file_size,
            latency_ms = commit_latency.as_millis(),
            "Successfully committed records to Iceberg (file registered)"
        );

        Ok(())
    }

    /// Commit buffered records (non-iceberg fallback)
    #[cfg(not(feature = "iceberg"))]
    async fn commit_buffer(
        name: &str,
        _config: &IcebergSinkConfig,
        buffer: &Arc<RwLock<HashMap<String, Vec<Record>>>>,
        metrics: &Arc<RwLock<SinkMetrics>>,
        _offsets: &mut HashMap<String, i64>,
    ) -> Result<()> {
        let mut buffer_guard = buffer.write().await;

        if buffer_guard.is_empty() {
            return Ok(());
        }

        let total_records: usize = buffer_guard.values().map(|v| v.len()).sum();
        debug!(sink = %name, records = total_records, "Committing buffer (iceberg feature not enabled)");

        // Update metrics
        let mut metrics_guard = metrics.write().await;
        metrics_guard.records_processed += total_records as u64;
        metrics_guard.last_commit_timestamp = chrono::Utc::now().timestamp_millis();

        // Update committed offsets
        for (key, records) in buffer_guard.iter() {
            if let Some(last_record) = records.last() {
                metrics_guard
                    .committed_offsets
                    .insert(key.clone(), last_record.offset);
            }
        }

        // Clear buffer
        buffer_guard.clear();

        warn!(sink = %name, records = total_records, "Records buffered but iceberg feature not enabled");

        Ok(())
    }
}

#[async_trait]
impl SinkConnector for IcebergSink {
    fn name(&self) -> &str {
        &self.name
    }

    fn sink_type(&self) -> crate::sink::SinkType {
        crate::sink::SinkType::Iceberg
    }

    fn topics(&self) -> Vec<String> {
        self.topics.clone()
    }

    async fn start(&mut self) -> Result<()> {
        info!(sink = %self.name, "Starting Iceberg sink");

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

        // Initialize catalog
        self.initialize_catalog().await?;

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

        #[cfg(feature = "iceberg")]
        {
            let table = self.table.clone();
            let catalog = self
                .catalog
                .clone()
                .ok_or_else(|| StreamlineError::Sink("Catalog not initialized".to_string()))?;
            let table_ident = self.table_ident.clone().ok_or_else(|| {
                StreamlineError::Sink("Table identifier not initialized".to_string())
            })?;
            let arrow_schema = self
                .arrow_schema
                .clone()
                .ok_or_else(|| StreamlineError::Sink("Arrow schema not initialized".to_string()))?;

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
                    catalog,
                    table_ident,
                    arrow_schema,
                    shutdown_rx,
                )
                .await;

                // Update status after loop exits
                let mut status_guard = status.write().await;
                if *status_guard != SinkStatus::Failed {
                    *status_guard = SinkStatus::Stopped;
                }
            });
        }

        #[cfg(not(feature = "iceberg"))]
        {
            tokio::spawn(async move {
                Self::consumer_loop(
                    name,
                    topics,
                    config,
                    topic_manager,
                    status.clone(),
                    metrics,
                    buffer,
                    shutdown_rx,
                )
                .await;

                // Update status after loop exits
                let mut status_guard = status.write().await;
                if *status_guard != SinkStatus::Failed {
                    *status_guard = SinkStatus::Stopped;
                }
            });
        }

        // Update status to running
        {
            let mut status = self.status.write().await;
            *status = SinkStatus::Running;
        }

        info!(sink = %self.name, "Iceberg sink started successfully");
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        info!(sink = %self.name, "Stopping Iceberg sink");

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
        let mut offsets = HashMap::new();
        #[cfg(feature = "iceberg")]
        {
            if let (Some(arrow_schema), Some(catalog), Some(table_ident)) =
                (&self.arrow_schema, &self.catalog, &self.table_ident)
            {
                if let Err(e) = Self::commit_buffer(
                    &self.name,
                    &self.config,
                    &self.buffer,
                    &self.metrics,
                    &self.table,
                    catalog,
                    table_ident,
                    arrow_schema,
                    &mut offsets,
                )
                .await
                {
                    error!(sink = %self.name, error = %e, "Error flushing buffer on shutdown");
                }
            }
        }

        #[cfg(not(feature = "iceberg"))]
        {
            if let Err(e) = Self::commit_buffer(
                &self.name,
                &self.config,
                &self.buffer,
                &self.metrics,
                &mut offsets,
            )
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

        info!(sink = %self.name, "Iceberg sink stopped");
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
        // This is synchronous, so we can't await. Return a snapshot.
        // In production, you might want to use a different synchronization mechanism.
        match self.status.try_read() {
            Ok(status) => status.clone(),
            Err(_) => SinkStatus::Running, // Assume running if locked
        }
    }

    fn metrics(&self) -> SinkMetrics {
        // This is synchronous, so we can't await. Return a snapshot.
        match self.metrics.try_read() {
            Ok(metrics) => metrics.clone(),
            Err(_) => SinkMetrics::default(),
        }
    }

    async fn flush(&mut self) -> Result<()> {
        let mut offsets = HashMap::new();
        #[cfg(feature = "iceberg")]
        {
            if let (Some(arrow_schema), Some(catalog), Some(table_ident)) =
                (&self.arrow_schema, &self.catalog, &self.table_ident)
            {
                Self::commit_buffer(
                    &self.name,
                    &self.config,
                    &self.buffer,
                    &self.metrics,
                    &self.table,
                    catalog,
                    table_ident,
                    arrow_schema,
                    &mut offsets,
                )
                .await?;
            }
        }

        #[cfg(not(feature = "iceberg"))]
        {
            Self::commit_buffer(
                &self.name,
                &self.config,
                &self.buffer,
                &self.metrics,
                &mut offsets,
            )
            .await?;
        }

        Ok(())
    }

    async fn health_check(&self) -> Result<bool> {
        let status = self.status();
        if status == SinkStatus::Failed {
            return Ok(false);
        }
        // Check if error_count is growing
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
    use crate::sink::config::CatalogType;
    use tempfile::TempDir;

    fn create_test_config() -> IcebergSinkConfig {
        IcebergSinkConfig {
            catalog_uri: "http://localhost:8181".to_string(),
            catalog_type: CatalogType::Rest,
            namespace: "default".to_string(),
            table: "test_table".to_string(),
            commit_interval_ms: 1000,
            max_batch_size: 100,
            partitioning: Default::default(),
            catalog_properties: HashMap::new(),
            output_dir: "./data/iceberg".to_string(),
            warehouse: None,  // Not needed for REST catalog
            aws_region: None, // Not needed for REST catalog
            compression: Default::default(),
            max_retries: 3,
            retry_delay_ms: 100,
            schema_evolution: Default::default(),
        }
    }

    #[test]
    fn test_iceberg_sink_creation() {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = create_test_config();

        let sink = IcebergSink::new(
            "test-sink".to_string(),
            vec!["test-topic".to_string()],
            config,
            topic_manager,
        );

        assert!(sink.is_ok());
        let sink = sink.unwrap();
        assert_eq!(sink.name(), "test-sink");
        assert_eq!(sink.topics(), vec!["test-topic"]);
    }

    #[test]
    fn test_validate_config_empty_catalog_uri() {
        let mut config = create_test_config();
        config.catalog_uri = String::new();

        let result = IcebergSink::validate_config(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("catalog_uri cannot be empty"));
    }

    #[test]
    fn test_validate_config_empty_table() {
        let mut config = create_test_config();
        config.table = String::new();

        let result = IcebergSink::validate_config(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("table cannot be empty"));
    }

    #[test]
    fn test_validate_config_zero_commit_interval() {
        let mut config = create_test_config();
        config.commit_interval_ms = 0;

        let result = IcebergSink::validate_config(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("commit_interval_ms must be > 0"));
    }

    #[tokio::test]
    async fn test_sink_status() {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = create_test_config();

        let sink = IcebergSink::new(
            "test-sink".to_string(),
            vec!["test-topic".to_string()],
            config,
            topic_manager,
        )
        .unwrap();

        assert_eq!(sink.status(), SinkStatus::Stopped);
    }

    #[test]
    fn test_initial_metrics() {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = create_test_config();

        let sink = IcebergSink::new(
            "test-sink".to_string(),
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
        use crate::sink::config::IcebergSchemaEvolution;

        assert_eq!(format!("{}", IcebergSchemaEvolution::Strict), "strict");
        assert_eq!(
            format!("{}", IcebergSchemaEvolution::AddNewColumns),
            "add_new_columns"
        );
        assert_eq!(
            format!("{}", IcebergSchemaEvolution::AddAndPromote),
            "add_and_promote"
        );
    }

    #[tokio::test]
    async fn test_health_check_stopped() {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let config = create_test_config();

        let sink = IcebergSink::new(
            "test-sink".to_string(),
            vec!["test-topic".to_string()],
            config,
            topic_manager,
        )
        .unwrap();

        // Stopped sink should report unhealthy (not running)
        let healthy = sink.health_check().await.unwrap();
        assert!(!healthy);
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_create_arrow_schema() {
        let schema = IcebergSink::create_arrow_schema();

        assert_eq!(schema.fields().len(), 6);
        assert_eq!(schema.field(0).name(), "offset");
        assert_eq!(schema.field(1).name(), "timestamp");
        assert_eq!(schema.field(2).name(), "key");
        assert_eq!(schema.field(3).name(), "value");
        assert_eq!(schema.field(4).name(), "headers_json");
        assert_eq!(schema.field(5).name(), "_partition_value");

        // Check nullability
        assert!(!schema.field(0).is_nullable()); // offset
        assert!(!schema.field(1).is_nullable()); // timestamp
        assert!(schema.field(2).is_nullable()); // key
        assert!(!schema.field(3).is_nullable()); // value
        assert!(schema.field(4).is_nullable()); // headers_json
        assert!(schema.field(5).is_nullable()); // _partition_value
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_compute_partition_value_hour() {
        use crate::sink::config::PartitioningStrategy;

        // 2026-01-06 12:30:00 UTC in milliseconds
        let timestamp_ms = 1767702600000i64;

        let result = IcebergSink::compute_partition_value(
            timestamp_ms,
            &PartitioningStrategy::TimeBasedHour,
        );
        assert_eq!(result, Some("2026-01-06-12".to_string()));
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_compute_partition_value_day() {
        use crate::sink::config::PartitioningStrategy;

        // 2026-01-06 14:30:00 UTC in milliseconds
        let timestamp_ms = 1767702600000i64;

        let result =
            IcebergSink::compute_partition_value(timestamp_ms, &PartitioningStrategy::TimeBasedDay);
        assert_eq!(result, Some("2026-01-06".to_string()));
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_compute_partition_value_month() {
        use crate::sink::config::PartitioningStrategy;

        // 2026-01-06 14:30:00 UTC in milliseconds
        let timestamp_ms = 1767702600000i64;

        let result = IcebergSink::compute_partition_value(
            timestamp_ms,
            &PartitioningStrategy::TimeBasedMonth,
        );
        assert_eq!(result, Some("2026-01".to_string()));
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_compute_partition_value_none() {
        use crate::sink::config::PartitioningStrategy;

        let timestamp_ms = 1767702600000i64;

        let result =
            IcebergSink::compute_partition_value(timestamp_ms, &PartitioningStrategy::None);
        assert!(result.is_none());
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_records_to_arrow_batch() {
        use crate::storage::Header;
        use bytes::Bytes;

        let config = create_test_config();
        let schema = Arc::new(IcebergSink::create_arrow_schema());

        // Create test records
        let records = vec![
            Record {
                offset: 0,
                timestamp: 1767702600000,
                key: Some(Bytes::from("key1")),
                value: Bytes::from("value1"),
                headers: vec![Header {
                    key: "h1".to_string(),
                    value: Bytes::from("v1"),
                }],
                crc: None,
            },
            Record {
                offset: 1,
                timestamp: 1767702601000,
                key: None,
                value: Bytes::from("value2"),
                headers: vec![],
                crc: None,
            },
        ];

        let batch = IcebergSink::records_to_arrow_batch(&records, &schema, &config);
        assert!(batch.is_ok());

        let batch = batch.unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 6);
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_create_iceberg_schema() {
        let schema = IcebergSink::create_iceberg_schema().unwrap();

        // Verify the schema has the expected fields
        // Iceberg schema uses as_struct() to get fields
        let fields = schema.as_struct();
        assert_eq!(fields.fields().len(), 6);
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_write_parquet_file() {
        use crate::storage::Header;
        use bytes::Bytes;
        use parquet::file::reader::FileReader;
        use parquet::file::serialized_reader::SerializedFileReader;
        use std::fs::File;

        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().to_string_lossy().to_string();

        let config = IcebergSinkConfig {
            output_dir: output_dir.clone(),
            ..create_test_config()
        };
        let schema = Arc::new(IcebergSink::create_arrow_schema());

        // Create test records
        let records = vec![
            Record {
                offset: 0,
                timestamp: 1767702600000,
                key: Some(Bytes::from("key1")),
                value: Bytes::from("value1"),
                headers: vec![Header {
                    key: "h1".to_string(),
                    value: Bytes::from("v1"),
                }],
                crc: None,
            },
            Record {
                offset: 1,
                timestamp: 1767702601000,
                key: None,
                value: Bytes::from("value2"),
                headers: vec![],
                crc: None,
            },
            Record {
                offset: 2,
                timestamp: 1767702602000,
                key: Some(Bytes::from("key3")),
                value: Bytes::from("value3"),
                headers: vec![],
                crc: None,
            },
        ];

        // Convert to Arrow batch
        let batch = IcebergSink::records_to_arrow_batch(&records, &schema, &config).unwrap();

        // Write to Parquet file
        let result = IcebergSink::write_parquet_file(
            &batch,
            &output_dir,
            "test_table",
            Some("2026-01-06-12"),
            &config.compression,
        );
        assert!(result.is_ok());

        let (file_path, rows_written) = result.unwrap();
        assert_eq!(rows_written, 3);
        assert!(file_path.contains("test_table"));
        assert!(file_path.contains("_partition_value=2026-01-06-12"));
        assert!(file_path.ends_with(".parquet"));

        // Verify the file exists and can be read
        let file = File::open(&file_path).expect("Parquet file should exist");
        let reader = SerializedFileReader::new(file).expect("Should create Parquet reader");
        let metadata = reader.metadata();

        assert_eq!(metadata.num_row_groups(), 1);
        assert_eq!(metadata.file_metadata().num_rows(), 3);
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_write_parquet_file_no_partition() {
        use bytes::Bytes;

        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().to_string_lossy().to_string();

        let mut config = create_test_config();
        config.output_dir = output_dir.clone();
        config.partitioning.strategy = crate::sink::config::PartitioningStrategy::None;

        let schema = Arc::new(IcebergSink::create_arrow_schema());

        // Create a single test record
        let records = vec![Record {
            offset: 0,
            timestamp: 1767702600000,
            key: Some(Bytes::from("key1")),
            value: Bytes::from("value1"),
            headers: vec![],
            crc: None,
        }];

        // Convert to Arrow batch
        let batch = IcebergSink::records_to_arrow_batch(&records, &schema, &config).unwrap();

        // Write without partition
        let result = IcebergSink::write_parquet_file(
            &batch,
            &output_dir,
            "test_table",
            None,
            &config.compression,
        );
        assert!(result.is_ok());

        let (file_path, rows_written) = result.unwrap();
        assert_eq!(rows_written, 1);
        // Should not have partition directory
        assert!(!file_path.contains("_partition_value"));
        assert!(file_path.ends_with(".parquet"));
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_field_based_partitioning_json_value() {
        use crate::sink::config::{PartitioningConfig, PartitioningStrategy};
        use bytes::Bytes;

        // Create a record with JSON value containing a region field
        let json_value = r#"{"region": "us-west-2", "user_id": 12345}"#;
        let record = Record {
            offset: 0,
            timestamp: 1767702600000,
            key: None,
            value: Bytes::from(json_value),
            headers: vec![],
            crc: None,
        };

        // Test field-based partitioning
        let config = PartitioningConfig {
            strategy: PartitioningStrategy::FieldBased,
            field: Some("region".to_string()),
            time_granularity: Default::default(),
        };

        let result = IcebergSink::get_partition_value(&record, &config);
        assert_eq!(result, Some("us-west-2".to_string()));
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_field_based_partitioning_numeric_field() {
        use crate::sink::config::{PartitioningConfig, PartitioningStrategy};
        use bytes::Bytes;

        // Create a record with numeric field
        let json_value = r#"{"tenant_id": 42, "data": "test"}"#;
        let record = Record {
            offset: 0,
            timestamp: 1767702600000,
            key: None,
            value: Bytes::from(json_value),
            headers: vec![],
            crc: None,
        };

        let config = PartitioningConfig {
            strategy: PartitioningStrategy::FieldBased,
            field: Some("tenant_id".to_string()),
            time_granularity: Default::default(),
        };

        let result = IcebergSink::get_partition_value(&record, &config);
        assert_eq!(result, Some("42".to_string()));
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_field_based_partitioning_missing_field() {
        use crate::sink::config::{PartitioningConfig, PartitioningStrategy};
        use bytes::Bytes;

        // Create a record without the expected field
        let json_value = r#"{"other_field": "value"}"#;
        let record = Record {
            offset: 0,
            timestamp: 1767702600000,
            key: None,
            value: Bytes::from(json_value),
            headers: vec![],
            crc: None,
        };

        let config = PartitioningConfig {
            strategy: PartitioningStrategy::FieldBased,
            field: Some("missing_field".to_string()),
            time_granularity: Default::default(),
        };

        let result = IcebergSink::get_partition_value(&record, &config);
        assert!(result.is_none());
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_field_based_partitioning_from_key() {
        use crate::sink::config::{PartitioningConfig, PartitioningStrategy};
        use bytes::Bytes;

        // Create a record with JSON key (field not in value)
        let json_key = r#"{"customer_id": "cust-123"}"#;
        let record = Record {
            offset: 0,
            timestamp: 1767702600000,
            key: Some(Bytes::from(json_key)),
            value: Bytes::from("non-json-value"),
            headers: vec![],
            crc: None,
        };

        let config = PartitioningConfig {
            strategy: PartitioningStrategy::FieldBased,
            field: Some("customer_id".to_string()),
            time_granularity: Default::default(),
        };

        let result = IcebergSink::get_partition_value(&record, &config);
        assert_eq!(result, Some("cust-123".to_string()));
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_compression_options() {
        use crate::sink::config::ParquetCompression;
        use bytes::Bytes;
        use parquet::file::reader::FileReader;
        use parquet::file::serialized_reader::SerializedFileReader;
        use std::fs::File;

        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().to_string_lossy().to_string();

        let mut config = create_test_config();
        config.output_dir = output_dir.clone();

        let schema = Arc::new(IcebergSink::create_arrow_schema());

        let records = vec![Record {
            offset: 0,
            timestamp: 1767702600000,
            key: Some(Bytes::from("key1")),
            value: Bytes::from("value1"),
            headers: vec![],
            crc: None,
        }];

        let batch = IcebergSink::records_to_arrow_batch(&records, &schema, &config).unwrap();

        // Test each compression type
        for compression in [
            ParquetCompression::None,
            ParquetCompression::Snappy,
            ParquetCompression::Gzip,
            ParquetCompression::Lz4,
            ParquetCompression::Zstd,
        ] {
            let result = IcebergSink::write_parquet_file(
                &batch,
                &output_dir,
                &format!("test_table_{:?}", compression).to_lowercase(),
                None,
                &compression,
            );
            assert!(result.is_ok(), "Failed for compression {:?}", compression);

            let (file_path, rows_written) = result.unwrap();
            assert_eq!(rows_written, 1);

            // Verify the file can be read
            let file = File::open(&file_path).expect("Parquet file should exist");
            let reader = SerializedFileReader::new(file).expect("Should create Parquet reader");
            let metadata = reader.metadata();
            assert_eq!(metadata.file_metadata().num_rows(), 1);
        }
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_retry_config_defaults() {
        let config = create_test_config();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_delay_ms, 100);
    }

    // ---- Integration test helpers ----

    /// Helper: Create a test record with a JSON value payload.
    #[cfg(feature = "iceberg")]
    fn make_json_record(offset: i64, json: &str) -> Record {
        use bytes::Bytes;
        Record {
            offset,
            timestamp: 1767702600000 + offset * 1000,
            key: None,
            value: Bytes::from(json.to_owned()),
            headers: vec![],
            crc: None,
        }
    }

    /// Helper: Create a batch of test records from JSON payloads.
    #[cfg(feature = "iceberg")]
    fn make_record_batch_for_test(payloads: &[&str]) -> Vec<Record> {
        payloads
            .iter()
            .enumerate()
            .map(|(i, json)| make_json_record(i as i64, json))
            .collect()
    }

    /// Mock Iceberg context for integration testing.
    ///
    /// Provides helpers to validate Iceberg sink behavior (schema evolution,
    /// Parquet writing, partitioning) without requiring a real catalog server.
    #[cfg(feature = "iceberg")]
    struct MockIcebergContext {
        _output_dir: TempDir,
        config: IcebergSinkConfig,
        schema: Arc<ArrowSchema>,
    }

    #[cfg(feature = "iceberg")]
    impl MockIcebergContext {
        fn new() -> Self {
            let output_dir = TempDir::new().expect("Failed to create temp dir");
            let mut config = create_test_config();
            config.output_dir = output_dir.path().to_string_lossy().to_string();
            let schema = Arc::new(IcebergSink::create_arrow_schema());
            Self {
                _output_dir: output_dir,
                config,
                schema,
            }
        }

        /// Write records to a Parquet file and return `(file_path, rows_written)`.
        fn write_records(&self, records: &[Record]) -> Result<(String, u64)> {
            let batch =
                IcebergSink::records_to_arrow_batch(records, &self.schema, &self.config)?;
            let partition = records
                .first()
                .and_then(|r| IcebergSink::get_partition_value(r, &self.config.partitioning));
            IcebergSink::write_parquet_file(
                &batch,
                &self.config.output_dir,
                &self.config.table,
                partition.as_deref(),
                &self.config.compression,
            )
        }

        /// Validate schema evolution for the given records.
        fn check_evolution(
            &self,
            records: &[Record],
            policy: &crate::sink::config::IcebergSchemaEvolution,
        ) -> Result<Vec<(String, DataType)>> {
            IcebergSink::check_schema_evolution(&self.schema, records, policy, "mock-sink")
        }
    }

    // ---- Schema evolution tests ----

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_schema_evolution_strict_rejects_unknown_fields() {
        use crate::sink::config::IcebergSchemaEvolution;

        let schema = Arc::new(IcebergSink::create_arrow_schema());
        let records = make_record_batch_for_test(&[
            r#"{"new_field": "value", "another": 42}"#,
        ]);

        let result = IcebergSink::check_schema_evolution(
            &schema,
            &records,
            &IcebergSchemaEvolution::Strict,
            "test-sink",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("strict"));
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_schema_evolution_add_new_columns_allows_unknown_fields() {
        use crate::sink::config::IcebergSchemaEvolution;

        let schema = Arc::new(IcebergSink::create_arrow_schema());
        let records = make_record_batch_for_test(&[
            r#"{"region": "us-west-2", "count": 5}"#,
        ]);

        let result = IcebergSink::check_schema_evolution(
            &schema,
            &records,
            &IcebergSchemaEvolution::AddNewColumns,
            "test-sink",
        );
        assert!(result.is_ok());
        let new_fields = result.unwrap();
        assert!(!new_fields.is_empty());
        let field_names: Vec<&str> = new_fields.iter().map(|(n, _)| n.as_str()).collect();
        assert!(field_names.contains(&"region"));
        assert!(field_names.contains(&"count"));
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_schema_evolution_add_and_promote() {
        use crate::sink::config::IcebergSchemaEvolution;

        let schema = Arc::new(IcebergSink::create_arrow_schema());
        let records = make_record_batch_for_test(&[
            r#"{"temperature": 98.6, "active": true}"#,
        ]);

        let result = IcebergSink::check_schema_evolution(
            &schema,
            &records,
            &IcebergSchemaEvolution::AddAndPromote,
            "test-sink",
        );
        assert!(result.is_ok());
        let new_fields = result.unwrap();
        assert_eq!(new_fields.len(), 2);
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_schema_evolution_no_new_fields_for_binary_values() {
        use crate::sink::config::IcebergSchemaEvolution;

        let schema = Arc::new(IcebergSink::create_arrow_schema());
        // Binary (non-JSON) value → no fields detected, strict should pass
        let records = vec![Record {
            offset: 0,
            timestamp: 1767702600000,
            key: None,
            value: bytes::Bytes::from(vec![0u8, 1, 2, 3]),
            headers: vec![],
            crc: None,
        }];

        let result = IcebergSink::check_schema_evolution(
            &schema,
            &records,
            &IcebergSchemaEvolution::Strict,
            "test-sink",
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_schema_evolution_empty_records() {
        use crate::sink::config::IcebergSchemaEvolution;

        let schema = Arc::new(IcebergSink::create_arrow_schema());
        let records: Vec<Record> = vec![];

        let result = IcebergSink::check_schema_evolution(
            &schema,
            &records,
            &IcebergSchemaEvolution::Strict,
            "test-sink",
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    // ---- Type inference tests ----

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_infer_arrow_type() {
        assert_eq!(
            IcebergSink::infer_arrow_type(&serde_json::json!(42)),
            DataType::Int64
        );
        assert_eq!(
            IcebergSink::infer_arrow_type(&serde_json::json!(3.14)),
            DataType::Float64
        );
        assert_eq!(
            IcebergSink::infer_arrow_type(&serde_json::json!("hello")),
            DataType::Utf8
        );
        assert_eq!(
            IcebergSink::infer_arrow_type(&serde_json::json!(true)),
            DataType::Boolean
        );
        assert_eq!(
            IcebergSink::infer_arrow_type(&serde_json::json!(null)),
            DataType::Utf8
        );
        assert_eq!(
            IcebergSink::infer_arrow_type(&serde_json::json!([1, 2, 3])),
            DataType::Utf8
        );
        assert_eq!(
            IcebergSink::infer_arrow_type(&serde_json::json!({"nested": true})),
            DataType::Utf8
        );
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_is_compatible_promotion() {
        // Valid promotions
        assert!(IcebergSink::is_compatible_promotion(
            &DataType::Int32,
            &DataType::Int64
        ));
        assert!(IcebergSink::is_compatible_promotion(
            &DataType::Float32,
            &DataType::Float64
        ));
        assert!(IcebergSink::is_compatible_promotion(
            &DataType::Int32,
            &DataType::Float64
        ));
        assert!(IcebergSink::is_compatible_promotion(
            &DataType::Int64,
            &DataType::Float64
        ));

        // Invalid promotions
        assert!(!IcebergSink::is_compatible_promotion(
            &DataType::Utf8,
            &DataType::Int64
        ));
        assert!(!IcebergSink::is_compatible_promotion(
            &DataType::Int64,
            &DataType::Int32
        ));
        assert!(!IcebergSink::is_compatible_promotion(
            &DataType::Float64,
            &DataType::Float32
        ));
    }

    // ---- Catalog status tests ----

    #[test]
    fn test_catalog_status_from_iceberg() {
        use crate::sink::config::{CatalogStatus, CatalogType};

        assert_eq!(CatalogType::Rest.status(), CatalogStatus::Supported);
        assert_eq!(CatalogType::Hive.status(), CatalogStatus::Planned);
        assert_eq!(CatalogType::Glue.status(), CatalogStatus::Planned);

        // Verify status messages are non-empty and descriptive
        assert!(!CatalogType::Rest.status_message().is_empty());
        assert!(CatalogType::Hive.status_message().contains("hive_metastore"));
        assert!(CatalogType::Glue.status_message().contains("hive_metastore"));
    }

    // ---- Mock integration tests ----

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_mock_context_write_and_evolve() {
        use crate::sink::config::IcebergSchemaEvolution;

        let ctx = MockIcebergContext::new();
        let records = make_record_batch_for_test(&[
            r#"{"user": "alice", "score": 100}"#,
            r#"{"user": "bob", "score": 200}"#,
        ]);

        // Writing should succeed
        let result = ctx.write_records(&records);
        assert!(result.is_ok());
        let (path, rows) = result.unwrap();
        assert_eq!(rows, 2);
        assert!(std::path::Path::new(&path).exists());

        // Schema evolution with AddNewColumns should detect user + score
        let evolution = ctx.check_evolution(&records, &IcebergSchemaEvolution::AddNewColumns);
        assert!(evolution.is_ok());
        let new_fields = evolution.unwrap();
        assert!(!new_fields.is_empty());

        // Strict should reject the same records
        let strict = ctx.check_evolution(&records, &IcebergSchemaEvolution::Strict);
        assert!(strict.is_err());
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn test_mock_context_schema_compatible_records() {
        use crate::sink::config::IcebergSchemaEvolution;

        let ctx = MockIcebergContext::new();

        // Non-JSON binary records should pass even strict evolution
        let records = vec![Record {
            offset: 0,
            timestamp: 1767702600000,
            key: Some(bytes::Bytes::from("key")),
            value: bytes::Bytes::from(vec![0xCA, 0xFE]),
            headers: vec![],
            crc: None,
        }];

        let result = ctx.check_evolution(&records, &IcebergSchemaEvolution::Strict);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());

        let result = ctx.write_records(&records);
        assert!(result.is_ok());
    }
}
