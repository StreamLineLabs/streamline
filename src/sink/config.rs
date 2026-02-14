//! Configuration types for sink connectors.
//!
//! This module provides configuration structures for Streamline's sink connectors,
//! which enable streaming data to external storage systems like Apache Iceberg
//! and Delta Lake.
//!
//! # Supported Sinks
//!
//! - **Iceberg**: Apache Iceberg lakehouse format with support for REST, Hive,
//!   and AWS Glue catalogs
//! - **Delta Lake**: Delta Lake lakehouse format with support for local and
//!   cloud storage (S3, Azure, GCS)
//!
//! # Example
//!
//! ```
//! use streamline::sink::config::{SinkConfig, SinkType, IcebergSinkConfig, CatalogType};
//!
//! // Create an Iceberg sink configuration
//! let iceberg_config = IcebergSinkConfig {
//!     catalog_uri: "http://localhost:8181".to_string(),
//!     catalog_type: CatalogType::Rest,
//!     namespace: "default".to_string(),
//!     table: "events".to_string(),
//!     commit_interval_ms: 60_000,
//!     max_batch_size: 10_000,
//!     partitioning: Default::default(),
//!     catalog_properties: Default::default(),
//!     output_dir: "./data/iceberg".to_string(),
//!     warehouse: None,   // Required for Hive/Glue catalogs
//!     aws_region: None,  // Required for Glue catalog
//!     compression: Default::default(),     // Snappy compression
//!     max_retries: 3,                      // Retry failed commits
//!     retry_delay_ms: 100,                 // Initial retry delay
//!     schema_evolution: Default::default(), // Strict schema
//! };
//!
//! // Create the main sink configuration
//! let sink_config = SinkConfig {
//!     name: "events-iceberg".to_string(),
//!     sink_type: SinkType::Iceberg,
//!     topics: vec!["events".to_string()],
//!     config: serde_json::to_value(&iceberg_config).unwrap(),
//! };
//! ```
//!
//! # Partitioning
//!
//! Both Iceberg and Delta Lake sinks support partitioning strategies to optimize
//! query performance:
//!
//! - **Time-based**: Partition by hour, day, or month based on record timestamps
//! - **Field-based**: Partition by specific field values in the records
//!
//! # Batching and Commits
//!
//! Sinks batch records before committing to storage. Commits are triggered when:
//!
//! 1. `commit_interval_ms` has elapsed since the last commit
//! 2. `max_batch_size` records have been buffered
//!
//! This balances latency (smaller intervals) vs. efficiency (larger batches).

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Sink connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    /// Unique name for this sink
    pub name: String,

    /// Type of sink
    #[serde(rename = "type")]
    pub sink_type: SinkType,

    /// Topics to consume from
    pub topics: Vec<String>,

    /// Sink-specific configuration
    #[serde(flatten)]
    pub config: JsonValue,
}

/// Type of sink connector
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SinkType {
    /// Apache Iceberg lakehouse sink
    Iceberg,
    /// Delta Lake lakehouse sink
    #[serde(rename = "delta", alias = "delta-lake", alias = "deltalake")]
    DeltaLake,
    /// Serverless connector (HTTP webhooks, SaaS integrations)
    Serverless,
    /// Cloud function trigger (AWS Lambda, GCF, Azure Functions, Cloudflare Workers)
    #[serde(rename = "cloud_function", alias = "cloud-function", alias = "lambda")]
    CloudFunction,
}

impl std::fmt::Display for SinkType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SinkType::Iceberg => write!(f, "iceberg"),
            SinkType::DeltaLake => write!(f, "delta"),
            SinkType::Serverless => write!(f, "serverless"),
            SinkType::CloudFunction => write!(f, "cloud_function"),
        }
    }
}

/// Configuration for Iceberg sink
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergSinkConfig {
    /// Iceberg catalog URI
    ///
    /// Examples:
    /// - REST catalog: "http://localhost:8181"
    /// - Hive catalog: "thrift://localhost:9083"
    /// - Glue catalog: "glue://us-west-2"
    pub catalog_uri: String,

    /// Catalog type
    #[serde(default = "default_catalog_type")]
    pub catalog_type: CatalogType,

    /// Database/namespace name
    #[serde(default = "default_namespace")]
    pub namespace: String,

    /// Iceberg table name
    pub table: String,

    /// Commit interval in milliseconds
    ///
    /// Records are batched and committed at this interval.
    /// Default: 60000 (1 minute)
    #[serde(default = "default_commit_interval_ms")]
    pub commit_interval_ms: u64,

    /// Maximum batch size (number of records)
    ///
    /// If this many records are buffered, a commit is triggered
    /// even if commit_interval_ms hasn't elapsed.
    /// Default: 10000
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,

    /// Partitioning configuration
    #[serde(default)]
    pub partitioning: PartitioningConfig,

    /// Additional properties for Iceberg catalog
    #[serde(default)]
    pub catalog_properties: std::collections::HashMap<String, String>,

    /// Output directory for Parquet files
    ///
    /// Local directory where Parquet files are staged before being
    /// registered with the Iceberg catalog.
    /// Default: "./data/iceberg"
    #[serde(default = "default_output_dir")]
    pub output_dir: String,

    /// Warehouse location for Hive/Glue catalogs
    ///
    /// Required for Hive and Glue catalog types. This is the root path
    /// where Iceberg tables are stored.
    ///
    /// Examples:
    /// - Local: "file:///data/warehouse"
    /// - S3: "s3://bucket/warehouse"
    /// - Azure: "az://container/warehouse"
    /// - GCS: "gs://bucket/warehouse"
    #[serde(default)]
    pub warehouse: Option<String>,

    /// AWS region for Glue catalog
    ///
    /// Required when using the Glue catalog type.
    /// Example: "us-west-2"
    #[serde(default)]
    pub aws_region: Option<String>,

    /// Parquet compression codec
    ///
    /// Compression algorithm for Parquet files.
    /// Default: snappy
    #[serde(default)]
    pub compression: ParquetCompression,

    /// Maximum retry attempts for catalog commits
    ///
    /// Number of times to retry failed commit operations before giving up.
    /// Uses exponential backoff between retries.
    /// Default: 3
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Initial retry delay in milliseconds
    ///
    /// Base delay for exponential backoff. Each retry doubles the delay.
    /// Default: 100
    #[serde(default = "default_retry_delay_ms")]
    pub retry_delay_ms: u64,

    /// Schema evolution policy
    ///
    /// How to handle schema changes in incoming records.
    /// Default: strict (fail on schema mismatch)
    #[serde(default)]
    pub schema_evolution: IcebergSchemaEvolution,
}

fn default_output_dir() -> String {
    "./data/iceberg".to_string()
}

fn default_catalog_type() -> CatalogType {
    CatalogType::Rest
}

fn default_max_retries() -> u32 {
    3
}

fn default_retry_delay_ms() -> u64 {
    100
}

/// Parquet compression codec for Iceberg sink
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ParquetCompression {
    /// No compression
    None,
    /// Snappy compression (default, good balance of speed and ratio)
    #[default]
    Snappy,
    /// Gzip compression (better ratio, slower)
    Gzip,
    /// LZ4 compression (fastest, lower ratio)
    Lz4,
    /// Zstd compression (best ratio, moderate speed)
    Zstd,
}

impl std::fmt::Display for ParquetCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParquetCompression::None => write!(f, "none"),
            ParquetCompression::Snappy => write!(f, "snappy"),
            ParquetCompression::Gzip => write!(f, "gzip"),
            ParquetCompression::Lz4 => write!(f, "lz4"),
            ParquetCompression::Zstd => write!(f, "zstd"),
        }
    }
}

/// Schema evolution policy for Iceberg sink
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IcebergSchemaEvolution {
    /// No schema evolution - fail if schema doesn't match
    #[default]
    Strict,
    /// Add new columns automatically when new fields are detected
    AddNewColumns,
    /// Allow adding columns and promote types (e.g., int to long)
    AddAndPromote,
}

impl std::fmt::Display for IcebergSchemaEvolution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IcebergSchemaEvolution::Strict => write!(f, "strict"),
            IcebergSchemaEvolution::AddNewColumns => write!(f, "add_new_columns"),
            IcebergSchemaEvolution::AddAndPromote => write!(f, "add_and_promote"),
        }
    }
}

fn default_namespace() -> String {
    "default".to_string()
}

fn default_commit_interval_ms() -> u64 {
    60_000 // 1 minute
}

fn default_max_batch_size() -> usize {
    10_000
}

/// Iceberg catalog type
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CatalogType {
    /// REST catalog
    Rest,
    /// Hive Metastore catalog
    Hive,
    /// AWS Glue catalog
    Glue,
}

impl std::fmt::Display for CatalogType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogType::Rest => write!(f, "rest"),
            CatalogType::Hive => write!(f, "hive"),
            CatalogType::Glue => write!(f, "glue"),
        }
    }
}

/// Availability status for Iceberg catalog implementations.
///
/// Tracks which catalog types are production-ready vs. planned for future release.
/// Use [`CatalogType::status()`] to check availability at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogStatus {
    /// Fully supported and tested in production
    Supported,
    /// Implementation planned but not yet available.
    ///
    /// Typically blocked by upstream dependency issues (e.g., `hive_metastore`
    /// crate compilation failures).
    Planned,
}

impl std::fmt::Display for CatalogStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogStatus::Supported => write!(f, "supported"),
            CatalogStatus::Planned => write!(f, "planned"),
        }
    }
}

impl CatalogType {
    /// Returns the current availability status of this catalog type.
    ///
    /// - `Rest`: Fully supported via `iceberg-catalog-rest`
    /// - `Hive`: Planned — blocked by upstream `hive_metastore` dependency issues
    /// - `Glue`: Planned — blocked by upstream `hive_metastore` dependency issues
    pub fn status(&self) -> CatalogStatus {
        match self {
            CatalogType::Rest => CatalogStatus::Supported,
            CatalogType::Hive | CatalogType::Glue => CatalogStatus::Planned,
        }
    }

    /// Human-readable description of the catalog type's current availability.
    pub fn status_message(&self) -> &'static str {
        match self {
            CatalogType::Rest => "Fully supported via iceberg-catalog-rest",
            CatalogType::Hive => {
                "Planned — blocked by upstream `hive_metastore` crate compilation issues. \
                 Use REST catalog as an alternative."
            }
            CatalogType::Glue => {
                "Planned — blocked by upstream `hive_metastore` dependency (required by Glue). \
                 Use REST catalog as an alternative."
            }
        }
    }
}

/// Partitioning configuration for Iceberg tables
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitioningConfig {
    /// Partitioning strategy
    #[serde(default)]
    pub strategy: PartitioningStrategy,

    /// Field to partition by (for field-based partitioning)
    pub field: Option<String>,

    /// Time granularity (for time-based partitioning)
    #[serde(default)]
    pub time_granularity: TimeGranularity,
}

impl Default for PartitioningConfig {
    fn default() -> Self {
        Self {
            strategy: PartitioningStrategy::TimeBasedHour,
            field: None,
            time_granularity: TimeGranularity::Hour,
        }
    }
}

/// Partitioning strategy for Iceberg tables
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitioningStrategy {
    /// No partitioning
    None,
    /// Time-based partitioning by day
    TimeBasedDay,
    /// Time-based partitioning by hour
    #[default]
    TimeBasedHour,
    /// Time-based partitioning by month
    TimeBasedMonth,
    /// Field-based partitioning
    FieldBased,
}

/// Time granularity for partitioning
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TimeGranularity {
    /// Partition by hour
    #[default]
    Hour,
    /// Partition by day
    Day,
    /// Partition by month
    Month,
    /// Partition by year
    Year,
}

/// Configuration for Delta Lake sink
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaLakeSinkConfig {
    /// Table location URI
    ///
    /// Can be a local path or cloud storage URL:
    /// - Local: "file:///data/delta/events"
    /// - S3: "s3://bucket/path/to/table"
    /// - Azure: "az://container/path/to/table"
    /// - GCS: "gs://bucket/path/to/table"
    pub table_uri: String,

    /// Storage options for cloud access
    ///
    /// For S3: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
    /// For Azure: AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ACCOUNT_KEY
    /// For GCS: GOOGLE_SERVICE_ACCOUNT
    #[serde(default)]
    pub storage_options: std::collections::HashMap<String, String>,

    /// Commit interval in milliseconds
    ///
    /// Records are batched and committed at this interval.
    /// Default: 60000 (1 minute)
    #[serde(default = "default_delta_commit_interval_ms")]
    pub commit_interval_ms: u64,

    /// Maximum batch size (number of records)
    ///
    /// If this many records are buffered, a commit is triggered
    /// even if commit_interval_ms hasn't elapsed.
    /// Default: 10000
    #[serde(default = "default_delta_max_batch_size")]
    pub max_batch_size: usize,

    /// Write mode for the sink
    #[serde(default)]
    pub write_mode: DeltaWriteMode,

    /// Partitioning columns
    ///
    /// List of column names to partition by. The partition values
    /// are extracted from record fields.
    #[serde(default)]
    pub partition_columns: Vec<String>,

    /// Target file size in bytes
    ///
    /// The sink will try to create files of approximately this size.
    /// Default: 128MB (134217728)
    #[serde(default = "default_target_file_size")]
    pub target_file_size_bytes: u64,

    /// Enable table auto-optimization
    ///
    /// If enabled, the sink will periodically run OPTIMIZE and VACUUM.
    #[serde(default)]
    pub auto_optimize: bool,

    /// Retention period for time travel (hours)
    ///
    /// How long to keep historical versions for time-travel queries.
    /// Default: 168 (7 days)
    #[serde(default = "default_retention_hours")]
    pub retention_hours: u64,

    /// Schema evolution policy
    #[serde(default)]
    pub schema_evolution: SchemaEvolutionPolicy,

    /// Maximum retry attempts for commits
    ///
    /// Number of times to retry failed commit operations before giving up.
    /// Uses exponential backoff between retries.
    /// Default: 3
    #[serde(default = "default_delta_max_retries")]
    pub max_retries: u32,

    /// Initial retry delay in milliseconds
    ///
    /// Base delay for exponential backoff. Each retry doubles the delay.
    /// Default: 100
    #[serde(default = "default_delta_retry_delay_ms")]
    pub retry_delay_ms: u64,
}

fn default_delta_commit_interval_ms() -> u64 {
    60_000 // 1 minute
}

fn default_delta_max_batch_size() -> usize {
    10_000
}

fn default_target_file_size() -> u64 {
    128 * 1024 * 1024 // 128MB
}

fn default_retention_hours() -> u64 {
    168 // 7 days
}

fn default_delta_max_retries() -> u32 {
    3
}

fn default_delta_retry_delay_ms() -> u64 {
    100
}

/// Write mode for Delta Lake
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DeltaWriteMode {
    /// Append records to existing table
    #[default]
    Append,
    /// Overwrite the table with new records
    Overwrite,
    /// Merge records based on key columns (upsert)
    Merge,
}

impl std::fmt::Display for DeltaWriteMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeltaWriteMode::Append => write!(f, "append"),
            DeltaWriteMode::Overwrite => write!(f, "overwrite"),
            DeltaWriteMode::Merge => write!(f, "merge"),
        }
    }
}

/// Schema evolution policy for Delta Lake
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaEvolutionPolicy {
    /// No schema evolution - fail if schema doesn't match
    Strict,
    /// Add new columns automatically
    #[default]
    AddNewColumns,
    /// Allow adding columns and widening types
    AddAndWiden,
}

impl std::fmt::Display for SchemaEvolutionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaEvolutionPolicy::Strict => write!(f, "strict"),
            SchemaEvolutionPolicy::AddNewColumns => write!(f, "add_new_columns"),
            SchemaEvolutionPolicy::AddAndWiden => write!(f, "add_and_widen"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sink_type_serialization() {
        let sink_type = SinkType::Iceberg;
        let json = serde_json::to_string(&sink_type).unwrap();
        assert_eq!(json, r#""iceberg""#);

        let deserialized: SinkType = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, SinkType::Iceberg);
    }

    #[test]
    fn test_delta_sink_type_serialization() {
        let sink_type = SinkType::DeltaLake;
        let json = serde_json::to_string(&sink_type).unwrap();
        assert_eq!(json, r#""delta""#);

        let deserialized: SinkType = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, SinkType::DeltaLake);

        // Test alias deserialization
        let from_alias: SinkType = serde_json::from_str(r#""delta-lake""#).unwrap();
        assert_eq!(from_alias, SinkType::DeltaLake);
    }

    #[test]
    fn test_catalog_type_display() {
        assert_eq!(format!("{}", CatalogType::Rest), "rest");
        assert_eq!(format!("{}", CatalogType::Hive), "hive");
        assert_eq!(format!("{}", CatalogType::Glue), "glue");
    }

    #[test]
    fn test_catalog_status() {
        assert_eq!(CatalogType::Rest.status(), CatalogStatus::Supported);
        assert_eq!(CatalogType::Hive.status(), CatalogStatus::Planned);
        assert_eq!(CatalogType::Glue.status(), CatalogStatus::Planned);

        assert_eq!(format!("{}", CatalogStatus::Supported), "supported");
        assert_eq!(format!("{}", CatalogStatus::Planned), "planned");

        assert!(CatalogType::Rest.status_message().contains("Fully supported"));
        assert!(CatalogType::Hive.status_message().contains("Planned"));
        assert!(CatalogType::Glue.status_message().contains("Planned"));
    }

    #[test]
    fn test_iceberg_sink_config_defaults() {
        let json = r#"{
            "catalog_uri": "http://localhost:8181",
            "table": "events"
        }"#;

        let config: IcebergSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.catalog_type, CatalogType::Rest);
        assert_eq!(config.namespace, "default");
        assert_eq!(config.commit_interval_ms, 60_000);
        assert_eq!(config.max_batch_size, 10_000);
    }

    #[test]
    fn test_partitioning_config_default() {
        let config = PartitioningConfig::default();
        assert_eq!(config.strategy, PartitioningStrategy::TimeBasedHour);
        assert_eq!(config.time_granularity, TimeGranularity::Hour);
        assert!(config.field.is_none());
    }

    #[test]
    fn test_sink_config_serialization() {
        let config = SinkConfig {
            name: "my-sink".to_string(),
            sink_type: SinkType::Iceberg,
            topics: vec!["events".to_string()],
            config: serde_json::json!({
                "catalog_uri": "http://localhost:8181",
                "table": "events"
            }),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: SinkConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, "my-sink");
        assert_eq!(deserialized.sink_type, SinkType::Iceberg);
        assert_eq!(deserialized.topics.len(), 1);
    }

    #[test]
    fn test_delta_lake_sink_config_defaults() {
        let json = r#"{
            "table_uri": "s3://bucket/path/to/table"
        }"#;

        let config: DeltaLakeSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.table_uri, "s3://bucket/path/to/table");
        assert_eq!(config.commit_interval_ms, 60_000);
        assert_eq!(config.max_batch_size, 10_000);
        assert_eq!(config.write_mode, DeltaWriteMode::Append);
        assert!(config.partition_columns.is_empty());
        assert_eq!(config.target_file_size_bytes, 128 * 1024 * 1024);
        assert!(!config.auto_optimize);
        assert_eq!(config.retention_hours, 168);
        assert_eq!(
            config.schema_evolution,
            SchemaEvolutionPolicy::AddNewColumns
        );
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_delay_ms, 100);
    }

    #[test]
    fn test_delta_write_mode_display() {
        assert_eq!(format!("{}", DeltaWriteMode::Append), "append");
        assert_eq!(format!("{}", DeltaWriteMode::Overwrite), "overwrite");
        assert_eq!(format!("{}", DeltaWriteMode::Merge), "merge");
    }

    #[test]
    fn test_schema_evolution_policy_display() {
        assert_eq!(format!("{}", SchemaEvolutionPolicy::Strict), "strict");
        assert_eq!(
            format!("{}", SchemaEvolutionPolicy::AddNewColumns),
            "add_new_columns"
        );
        assert_eq!(
            format!("{}", SchemaEvolutionPolicy::AddAndWiden),
            "add_and_widen"
        );
    }
}
