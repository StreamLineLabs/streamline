//! Feature Store Configuration

use serde::{Deserialize, Serialize};

/// Feature store configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureStoreConfig {
    /// Enable feature store
    pub enabled: bool,
    /// Feature registry configuration
    pub registry: RegistryConfig,
    /// Online store configuration
    pub online: OnlineStoreConfig,
    /// Offline store configuration
    pub offline: OfflineStoreConfig,
    /// Serving configuration
    pub serving: ServingConfig,
}

impl Default for FeatureStoreConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            registry: RegistryConfig::default(),
            online: OnlineStoreConfig::default(),
            offline: OfflineStoreConfig::default(),
            serving: ServingConfig::default(),
        }
    }
}

/// Registry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryConfig {
    /// Registry storage type
    pub storage_type: RegistryStorageType,
    /// Storage path (for local storage)
    pub path: Option<String>,
    /// Enable caching
    pub cache_enabled: bool,
    /// Cache TTL in seconds
    pub cache_ttl_seconds: u64,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            storage_type: RegistryStorageType::InMemory,
            path: None,
            cache_enabled: true,
            cache_ttl_seconds: 300,
        }
    }
}

/// Registry storage type
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegistryStorageType {
    /// In-memory storage
    #[default]
    InMemory,
    /// Local file storage
    Local,
    /// S3 storage
    S3,
}

/// Online store configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnlineStoreConfig {
    /// Store type
    pub store_type: OnlineStoreType,
    /// Connection string (for external stores)
    pub connection_string: Option<String>,
    /// Maximum entries
    pub max_entries: usize,
    /// TTL for entries in seconds (0 = no expiry)
    pub ttl_seconds: u64,
    /// Enable compression
    pub compression: bool,
}

impl Default for OnlineStoreConfig {
    fn default() -> Self {
        Self {
            store_type: OnlineStoreType::InMemory,
            connection_string: None,
            max_entries: 1_000_000,
            ttl_seconds: 0,
            compression: false,
        }
    }
}

/// Online store type
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum OnlineStoreType {
    /// In-memory store
    #[default]
    InMemory,
    /// Redis
    Redis,
    /// DynamoDB
    DynamoDB,
    /// Streamline internal store
    Streamline,
}

/// Offline store configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OfflineStoreConfig {
    /// Store type
    pub store_type: OfflineStoreType,
    /// Storage path
    pub path: Option<String>,
    /// Connection string (for external stores)
    pub connection_string: Option<String>,
    /// Partition scheme
    pub partition_scheme: PartitionScheme,
    /// File format
    pub file_format: FileFormat,
}

impl Default for OfflineStoreConfig {
    fn default() -> Self {
        Self {
            store_type: OfflineStoreType::InMemory,
            path: None,
            connection_string: None,
            partition_scheme: PartitionScheme::Daily,
            file_format: FileFormat::Parquet,
        }
    }
}

/// Offline store type
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum OfflineStoreType {
    /// In-memory store
    #[default]
    InMemory,
    /// Local file storage
    Local,
    /// S3
    S3,
    /// BigQuery
    BigQuery,
    /// Snowflake
    Snowflake,
    /// Streamline internal store
    Streamline,
}

/// Partition scheme
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionScheme {
    /// No partitioning
    None,
    /// Hourly partitions
    Hourly,
    /// Daily partitions
    #[default]
    Daily,
    /// Monthly partitions
    Monthly,
}

/// File format
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileFormat {
    /// Parquet
    #[default]
    Parquet,
    /// Avro
    Avro,
    /// Delta
    Delta,
}

/// Serving configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServingConfig {
    /// Enable online serving
    pub online_enabled: bool,
    /// Enable offline serving
    pub offline_enabled: bool,
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Request timeout in milliseconds
    pub timeout_ms: u64,
    /// Enable caching
    pub cache_enabled: bool,
    /// Cache size (entries)
    pub cache_size: usize,
}

impl Default for ServingConfig {
    fn default() -> Self {
        Self {
            online_enabled: true,
            offline_enabled: true,
            max_batch_size: 1000,
            timeout_ms: 1000,
            cache_enabled: true,
            cache_size: 10000,
        }
    }
}
