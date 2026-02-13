//! CDC configuration types

use serde::{Deserialize, Serialize};

/// Output format for CDC events
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputFormat {
    /// Streamline's native CDC JSON format
    #[default]
    Native,
    /// Debezium-compatible JSON envelope (schema + payload)
    Debezium,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Native => write!(f, "native"),
            OutputFormat::Debezium => write!(f, "debezium"),
        }
    }
}

/// Base CDC source configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcConfig {
    /// Source name (unique identifier)
    pub name: String,
    /// Topic prefix for CDC events
    pub topic_prefix: Option<String>,
    /// Whether to perform initial snapshot
    pub snapshot_enabled: bool,
    /// Maximum batch size for processing
    pub batch_size: usize,
    /// Commit interval in milliseconds
    pub commit_interval_ms: u64,
    /// Tables to include (empty means all)
    pub include_tables: Vec<String>,
    /// Tables to exclude
    pub exclude_tables: Vec<String>,
    /// Schemas to include (empty means all)
    pub include_schemas: Vec<String>,
    /// Schemas to exclude
    pub exclude_schemas: Vec<String>,
    /// Output format: `native` (default) or `debezium`
    #[serde(default)]
    pub output_format: OutputFormat,
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            name: "cdc-source".to_string(),
            topic_prefix: Some("cdc".to_string()),
            snapshot_enabled: true,
            batch_size: 1000,
            commit_interval_ms: 10000,
            include_tables: Vec::new(),
            exclude_tables: Vec::new(),
            include_schemas: Vec::new(),
            exclude_schemas: Vec::new(),
            output_format: OutputFormat::Native,
        }
    }
}

impl CdcConfig {
    /// Check if a table should be captured
    pub fn should_capture_table(&self, schema: &str, table: &str) -> bool {
        // Check schema filters
        if !self.include_schemas.is_empty() && !self.include_schemas.contains(&schema.to_string()) {
            return false;
        }
        if self.exclude_schemas.contains(&schema.to_string()) {
            return false;
        }

        // Check table filters
        let full_name = format!("{}.{}", schema, table);
        if !self.include_tables.is_empty()
            && !self.include_tables.contains(&table.to_string())
            && !self.include_tables.contains(&full_name)
        {
            return false;
        }
        if self.exclude_tables.contains(&table.to_string())
            || self.exclude_tables.contains(&full_name)
        {
            return false;
        }

        true
    }
}

/// PostgreSQL CDC configuration
#[cfg(feature = "postgres-cdc")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresCdcConfig {
    /// Base CDC configuration
    #[serde(flatten)]
    pub base: CdcConfig,

    /// PostgreSQL connection string
    pub connection_string: String,

    /// Replication slot name
    pub slot_name: String,

    /// Publication name (for pgoutput plugin)
    pub publication_name: String,

    /// Output plugin (pgoutput, wal2json, etc.)
    pub output_plugin: PostgresOutputPlugin,

    /// Whether to create the replication slot if it doesn't exist
    pub create_slot: bool,

    /// Whether to drop the replication slot on stop
    pub drop_slot_on_stop: bool,

    /// Maximum number of connection retries
    pub max_retries: u32,

    /// Retry backoff in milliseconds
    pub retry_backoff_ms: u64,

    /// Heartbeat interval in seconds
    pub heartbeat_interval_secs: u64,

    /// SSL mode
    pub ssl_mode: PostgresSslMode,

    /// Table-to-topic mapping overrides (table name → topic name).
    /// Tables not in this map use the default naming: `{topic_prefix}.{schema}.{table}`.
    #[serde(default)]
    pub topic_mapping: std::collections::HashMap<String, String>,

    /// Snapshot mode (initial, schema_only, never)
    #[serde(default)]
    pub snapshot_mode: PostgresSnapshotMode,
}

/// Snapshot mode for PostgreSQL CDC
#[cfg(feature = "postgres-cdc")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresSnapshotMode {
    /// Full initial snapshot of all captured tables
    #[default]
    Initial,
    /// Capture table schemas only, no row data
    SchemaOnly,
    /// Skip snapshot entirely, start from current WAL position
    Never,
}

#[cfg(feature = "postgres-cdc")]
impl std::fmt::Display for PostgresSnapshotMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PostgresSnapshotMode::Initial => write!(f, "initial"),
            PostgresSnapshotMode::SchemaOnly => write!(f, "schema_only"),
            PostgresSnapshotMode::Never => write!(f, "never"),
        }
    }
}

#[cfg(feature = "postgres-cdc")]
impl Default for PostgresCdcConfig {
    fn default() -> Self {
        Self {
            base: CdcConfig::default(),
            connection_string: "host=localhost port=5432 user=postgres dbname=postgres".to_string(),
            slot_name: "streamline_cdc".to_string(),
            publication_name: "streamline_publication".to_string(),
            output_plugin: PostgresOutputPlugin::PgOutput,
            create_slot: true,
            drop_slot_on_stop: false,
            max_retries: 3,
            retry_backoff_ms: 1000,
            heartbeat_interval_secs: 10,
            ssl_mode: PostgresSslMode::Prefer,
            topic_mapping: std::collections::HashMap::new(),
            snapshot_mode: PostgresSnapshotMode::Initial,
        }
    }
}

/// PostgreSQL output plugin
#[cfg(feature = "postgres-cdc")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PostgresOutputPlugin {
    /// pgoutput - native logical replication plugin
    #[serde(rename = "pgoutput")]
    PgOutput,
    /// wal2json - JSON output plugin
    #[serde(rename = "wal2json")]
    Wal2Json,
}

#[cfg(feature = "postgres-cdc")]
impl std::fmt::Display for PostgresOutputPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PostgresOutputPlugin::PgOutput => write!(f, "pgoutput"),
            PostgresOutputPlugin::Wal2Json => write!(f, "wal2json"),
        }
    }
}

/// PostgreSQL SSL mode
#[cfg(feature = "postgres-cdc")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PostgresSslMode {
    /// No SSL
    #[serde(rename = "disable")]
    Disable,
    /// Try SSL, fall back to non-SSL
    #[serde(rename = "prefer")]
    Prefer,
    /// Require SSL
    #[serde(rename = "require")]
    Require,
}

#[cfg(feature = "postgres-cdc")]
impl std::fmt::Display for PostgresSslMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PostgresSslMode::Disable => write!(f, "disable"),
            PostgresSslMode::Prefer => write!(f, "prefer"),
            PostgresSslMode::Require => write!(f, "require"),
        }
    }
}

/// MySQL CDC configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MySqlCdcConfig {
    /// Base CDC configuration
    #[serde(flatten)]
    pub base: CdcConfig,

    /// MySQL hostname
    pub host: String,

    /// MySQL port
    pub port: u16,

    /// MySQL username
    pub username: String,

    /// MySQL password
    pub password: String,

    /// Database name
    pub database: String,

    /// Server ID for replication (must be unique across replicas)
    pub server_id: u32,

    /// Binlog position to start from (empty for latest)
    pub binlog_position: Option<String>,

    /// Binlog file to start from
    pub binlog_file: Option<String>,

    /// Enable GTID mode
    pub gtid_mode: bool,

    /// Connection timeout in seconds
    pub connection_timeout_secs: u64,

    /// Read timeout in seconds
    pub read_timeout_secs: u64,

    /// Whether to use SSL
    pub ssl_enabled: bool,

    /// SSL CA certificate path
    pub ssl_ca_path: Option<String>,
}

impl Default for MySqlCdcConfig {
    fn default() -> Self {
        Self {
            base: CdcConfig::default(),
            host: "localhost".to_string(),
            port: 3306,
            username: "root".to_string(),
            password: "".to_string(),
            database: "".to_string(),
            server_id: 1001,
            binlog_position: None,
            binlog_file: None,
            gtid_mode: false,
            connection_timeout_secs: 30,
            read_timeout_secs: 60,
            ssl_enabled: false,
            ssl_ca_path: None,
        }
    }
}

/// MongoDB CDC configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MongoDbCdcConfig {
    /// Base CDC configuration
    #[serde(flatten)]
    pub base: CdcConfig,

    /// MongoDB connection URI
    pub connection_uri: String,

    /// Database name to monitor
    pub database: String,

    /// Collections to monitor (empty for all)
    pub collections: Vec<String>,

    /// Resume token for continuing from previous position
    pub resume_token: Option<String>,

    /// Full document mode: updateLookup, whenAvailable, required
    pub full_document: MongoFullDocumentMode,

    /// Watch options: include pre-image for updates/deletes
    pub include_pre_image: bool,

    /// Maximum await time for change stream in milliseconds
    pub max_await_time_ms: u64,

    /// Batch size for change stream
    pub batch_size: u32,

    /// Read preference: primary, secondary, primaryPreferred, etc.
    pub read_preference: String,
}

impl Default for MongoDbCdcConfig {
    fn default() -> Self {
        Self {
            base: CdcConfig::default(),
            connection_uri: "mongodb://localhost:27017".to_string(),
            database: "".to_string(),
            collections: Vec::new(),
            resume_token: None,
            full_document: MongoFullDocumentMode::UpdateLookup,
            include_pre_image: false,
            max_await_time_ms: 1000,
            batch_size: 1000,
            read_preference: "primary".to_string(),
        }
    }
}

/// MongoDB full document mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MongoFullDocumentMode {
    /// Return full document on updates (lookup)
    #[default]
    UpdateLookup,
    /// Return full document when available
    WhenAvailable,
    /// Require full document
    Required,
    /// Don't return full document
    Off,
}

/// SQL Server CDC configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlServerCdcConfig {
    /// Base CDC configuration
    #[serde(flatten)]
    pub base: CdcConfig,

    /// SQL Server hostname
    pub host: String,

    /// SQL Server port
    pub port: u16,

    /// Username
    pub username: String,

    /// Password
    pub password: String,

    /// Database name
    pub database: String,

    /// Instance name (optional)
    pub instance: Option<String>,

    /// Polling interval in milliseconds
    pub poll_interval_ms: u64,

    /// Maximum transactions per poll
    pub max_transactions_per_poll: u32,

    /// LSN to start from (empty for latest)
    pub start_lsn: Option<String>,

    /// Enable encryption
    pub encrypt: bool,

    /// Trust server certificate
    pub trust_server_certificate: bool,

    /// Connection timeout in seconds
    pub connection_timeout_secs: u64,
}

impl Default for SqlServerCdcConfig {
    fn default() -> Self {
        Self {
            base: CdcConfig::default(),
            host: "localhost".to_string(),
            port: 1433,
            username: "sa".to_string(),
            password: "".to_string(),
            database: "".to_string(),
            instance: None,
            poll_interval_ms: 500,
            max_transactions_per_poll: 500,
            start_lsn: None,
            encrypt: true,
            trust_server_certificate: false,
            connection_timeout_secs: 30,
        }
    }
}

/// Schema evolution configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEvolutionConfig {
    /// Enable schema evolution tracking
    pub enabled: bool,

    /// Compatibility mode: BACKWARD, FORWARD, FULL, NONE
    pub compatibility: SchemaCompatibility,

    /// Store schema history in topic
    pub history_topic: Option<String>,

    /// Maximum schema versions to keep
    pub max_versions: usize,

    /// Auto-evolve schema on changes
    pub auto_evolve: bool,

    /// Break on incompatible changes
    pub break_on_incompatible: bool,
}

impl Default for SchemaEvolutionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            compatibility: SchemaCompatibility::Backward,
            history_topic: Some("cdc.schema.history".to_string()),
            max_versions: 100,
            auto_evolve: true,
            break_on_incompatible: false,
        }
    }
}

/// Schema compatibility modes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum SchemaCompatibility {
    /// New schema can read old data
    #[default]
    Backward,
    /// Old schema can read new data
    Forward,
    /// Both backward and forward compatible
    Full,
    /// No compatibility checking
    None,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_config_default() {
        let config = CdcConfig::default();
        assert_eq!(config.name, "cdc-source");
        assert!(config.snapshot_enabled);
    }

    #[test]
    fn test_should_capture_table() {
        let mut config = CdcConfig::default();

        // Default: capture all
        assert!(config.should_capture_table("public", "users"));

        // Include specific schemas
        config.include_schemas = vec!["public".to_string()];
        assert!(config.should_capture_table("public", "users"));
        assert!(!config.should_capture_table("private", "users"));

        // Exclude specific tables
        config.include_schemas.clear();
        config.exclude_tables = vec!["audit_log".to_string()];
        assert!(config.should_capture_table("public", "users"));
        assert!(!config.should_capture_table("public", "audit_log"));
    }

    #[cfg(feature = "postgres-cdc")]
    #[test]
    fn test_postgres_config_default() {
        let config = PostgresCdcConfig::default();
        assert_eq!(config.slot_name, "streamline_cdc");
        assert!(config.create_slot);
        assert!(config.topic_mapping.is_empty());
        assert_eq!(config.snapshot_mode, PostgresSnapshotMode::Initial);
    }
}
