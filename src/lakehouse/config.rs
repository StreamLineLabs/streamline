//! Lakehouse configuration types

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Lakehouse configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LakehouseConfig {
    /// Enable Parquet storage format
    pub parquet_enabled: bool,
    /// Parquet configuration
    pub parquet: ParquetConfig,
    /// Materialized view settings
    pub materialized_views: MaterializedViewConfig,
    /// Query optimization settings
    pub query_optimizer: QueryOptimizerConfig,
    /// Statistics collection
    pub statistics: StatisticsConfig,
}

impl Default for LakehouseConfig {
    fn default() -> Self {
        Self {
            parquet_enabled: true,
            parquet: ParquetConfig::default(),
            materialized_views: MaterializedViewConfig::default(),
            query_optimizer: QueryOptimizerConfig::default(),
            statistics: StatisticsConfig::default(),
        }
    }
}

/// Parquet storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetConfig {
    /// Row group size (number of rows per row group)
    pub row_group_size: usize,
    /// Compression codec
    pub compression: ParquetCompression,
    /// Enable dictionary encoding
    pub dictionary_enabled: bool,
    /// Enable bloom filters
    pub bloom_filter_enabled: bool,
    /// Target file size in bytes
    pub target_file_size: usize,
    /// Enable statistics collection
    pub statistics_enabled: bool,
    /// Write batch size
    pub write_batch_size: usize,
    /// Page size for Parquet pages
    pub page_size: usize,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            row_group_size: 100_000,
            compression: ParquetCompression::Zstd,
            dictionary_enabled: true,
            bloom_filter_enabled: true,
            target_file_size: 128 * 1024 * 1024, // 128 MB
            statistics_enabled: true,
            write_batch_size: 10_000,
            page_size: 1024 * 1024, // 1 MB
        }
    }
}

/// Parquet compression codecs
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParquetCompression {
    /// No compression
    None,
    /// Snappy compression (fast)
    Snappy,
    /// Gzip compression
    Gzip,
    /// LZ4 compression (very fast)
    Lz4,
    /// Zstd compression (good ratio)
    #[default]
    Zstd,
    /// Brotli compression (best ratio)
    Brotli,
}

/// Materialized view configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedViewConfig {
    /// Enable materialized views
    pub enabled: bool,
    /// Default refresh mode
    pub default_refresh: RefreshMode,
    /// Max concurrent refreshes
    pub max_concurrent_refreshes: usize,
    /// Storage location for MVs
    pub storage_path: PathBuf,
    /// Maximum view size in bytes
    pub max_view_size: usize,
    /// Enable incremental refresh
    pub incremental_enabled: bool,
}

impl Default for MaterializedViewConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_refresh: RefreshMode::Periodic { interval_secs: 60 },
            max_concurrent_refreshes: 4,
            storage_path: PathBuf::from("./data/views"),
            max_view_size: 1024 * 1024 * 1024, // 1 GB
            incremental_enabled: true,
        }
    }
}

/// Refresh modes for materialized views
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum RefreshMode {
    /// Refresh on every message
    Immediate,
    /// Refresh periodically
    Periodic {
        /// Interval in seconds
        interval_secs: u64,
    },
    /// Refresh on demand
    Manual,
    /// Refresh when queried (lazy)
    OnQuery {
        /// Maximum staleness threshold in seconds
        staleness_threshold_secs: u64,
    },
}

impl Default for RefreshMode {
    fn default() -> Self {
        Self::Periodic { interval_secs: 60 }
    }
}

/// Query optimizer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOptimizerConfig {
    /// Enable predicate pushdown
    pub predicate_pushdown: bool,
    /// Enable projection pushdown
    pub projection_pushdown: bool,
    /// Enable partition pruning
    pub partition_pruning: bool,
    /// Enable statistics-based optimization
    pub use_statistics: bool,
    /// Enable bloom filter pruning
    pub use_bloom_filters: bool,
    /// Cost model weights
    pub cost_weights: CostWeights,
}

impl Default for QueryOptimizerConfig {
    fn default() -> Self {
        Self {
            predicate_pushdown: true,
            projection_pushdown: true,
            partition_pruning: true,
            use_statistics: true,
            use_bloom_filters: true,
            cost_weights: CostWeights::default(),
        }
    }
}

/// Cost model weights for query optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostWeights {
    /// Weight for CPU cost
    pub cpu: f64,
    /// Weight for I/O cost
    pub io: f64,
    /// Weight for memory cost
    pub memory: f64,
    /// Weight for network cost
    pub network: f64,
}

impl Default for CostWeights {
    fn default() -> Self {
        Self {
            cpu: 1.0,
            io: 10.0,
            memory: 5.0,
            network: 20.0,
        }
    }
}

/// Statistics collection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticsConfig {
    /// Enable statistics collection
    pub enabled: bool,
    /// Collect min/max values
    pub collect_min_max: bool,
    /// Collect null counts
    pub collect_null_counts: bool,
    /// Collect distinct count estimation
    pub collect_distinct_count: bool,
    /// Sample rate for distinct count (0.0 - 1.0)
    pub distinct_count_sample_rate: f64,
    /// Storage path for statistics
    pub storage_path: PathBuf,
}

impl Default for StatisticsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            collect_min_max: true,
            collect_null_counts: true,
            collect_distinct_count: true,
            distinct_count_sample_rate: 0.1,
            storage_path: PathBuf::from("./data/stats"),
        }
    }
}
