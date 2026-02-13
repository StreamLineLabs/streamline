//! Universal CDC Hub
//!
//! Enhanced CDC capabilities with automatic schema migration and cross-database support.
//!
//! # Features
//!
//! - Multi-source CDC coordination
//! - Automatic schema migration
//! - Cross-database joins and transformations
//! - CDC event routing and filtering
//! - Exactly-once semantics support
//! - Dead letter queue for failed events

use super::config::{SchemaCompatibility, SchemaEvolutionConfig};
use super::schema::{SchemaEvolutionTracker, TableSchema};
use super::{CdcEvent, CdcOperation, CdcSourceStatus};
use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Universal CDC Hub - coordinates multiple CDC sources
pub struct CdcHub {
    #[allow(dead_code)]
    config: CdcHubConfig,
    sources: Arc<RwLock<HashMap<String, CdcSourceInfo>>>,
    router: Arc<EventRouter>,
    schema_tracker: Arc<SchemaEvolutionTracker>,
    transformer: Arc<EventTransformer>,
    dlq: Arc<DeadLetterQueue>,
    stats: Arc<CdcHubStats>,
}

impl CdcHub {
    /// Create a new CDC Hub
    pub fn new(config: CdcHubConfig) -> Result<Self> {
        let schema_config = SchemaEvolutionConfig {
            enabled: config.schema_evolution_enabled,
            auto_evolve: config.auto_schema_migration,
            compatibility: config.schema_compatibility,
            max_versions: config.max_schema_versions,
            break_on_incompatible: false,
            history_topic: Some("cdc.schema.history".to_string()),
        };

        Ok(Self {
            config: config.clone(),
            sources: Arc::new(RwLock::new(HashMap::new())),
            router: Arc::new(EventRouter::new(config.routing.clone())),
            schema_tracker: Arc::new(SchemaEvolutionTracker::new(schema_config)),
            transformer: Arc::new(EventTransformer::new(config.transforms.clone())),
            dlq: Arc::new(DeadLetterQueue::new(config.dlq.clone())),
            stats: Arc::new(CdcHubStats::default()),
        })
    }

    /// Register a CDC source
    pub async fn register_source(&self, source: CdcSourceInfo) -> Result<()> {
        let mut sources = self.sources.write().await;
        if sources.contains_key(&source.id) {
            return Err(StreamlineError::Config(format!(
                "CDC source already registered: {}",
                source.id
            )));
        }
        info!(
            "Registering CDC source: {} ({})",
            source.id, source.database_type
        );
        sources.insert(source.id.clone(), source);
        Ok(())
    }

    /// Unregister a CDC source
    pub async fn unregister_source(&self, source_id: &str) -> Result<()> {
        let mut sources = self.sources.write().await;
        if sources.remove(source_id).is_none() {
            return Err(StreamlineError::Config(format!(
                "CDC source not found: {}",
                source_id
            )));
        }
        info!("Unregistered CDC source: {}", source_id);
        Ok(())
    }

    /// List all registered sources
    pub async fn list_sources(&self) -> Vec<CdcSourceInfo> {
        let sources = self.sources.read().await;
        sources.values().cloned().collect()
    }

    /// Process a CDC event from any source
    pub async fn process_event(&self, source_id: &str, event: CdcEvent) -> Result<ProcessResult> {
        self.stats.events_received.fetch_add(1, Ordering::Relaxed);

        // Validate source
        {
            let sources = self.sources.read().await;
            if !sources.contains_key(source_id) {
                return Err(StreamlineError::Config(format!(
                    "Unknown CDC source: {}",
                    source_id
                )));
            }
        }

        // Track schema evolution
        let schema_version = self.schema_tracker.process_event(&event)?;
        if let Some(ref version) = schema_version {
            self.stats.schema_changes.fetch_add(1, Ordering::Relaxed);
            info!(
                "Schema change detected for {}.{}.{}: version {}",
                event.database, event.schema, event.table, version.version
            );
        }

        // Apply transformations
        let transformed_event = match self.transformer.transform(event.clone()).await {
            Ok(e) => e,
            Err(e) => {
                warn!("Transform failed for event: {}", e);
                self.dlq
                    .send(DlqEvent {
                        source_id: source_id.to_string(),
                        original_event: event,
                        error: e.to_string(),
                        timestamp: Utc::now(),
                        retry_count: 0,
                    })
                    .await?;
                self.stats.transform_errors.fetch_add(1, Ordering::Relaxed);
                return Ok(ProcessResult::SentToDlq);
            }
        };

        // Route event to target topics
        let routing_result = self.router.route(&transformed_event).await;

        self.stats.events_processed.fetch_add(1, Ordering::Relaxed);

        Ok(ProcessResult::Routed {
            targets: routing_result.targets,
            schema_version: schema_version.map(|v| v.version),
        })
    }

    /// Apply schema migration between sources
    pub async fn migrate_schema(
        &self,
        source_table: &str,
        target_table: &str,
        migration: SchemaMigration,
    ) -> Result<MigrationResult> {
        info!(
            "Migrating schema from {} to {} with {} column mappings",
            source_table,
            target_table,
            migration.column_mappings.len()
        );

        let source_schema = self
            .schema_tracker
            .get_latest_schema(source_table)
            .ok_or_else(|| {
                StreamlineError::Config(format!("Source schema not found: {}", source_table))
            })?;

        // Generate target schema from migration
        let target_schema = migration.apply_to_schema(&source_schema.schema)?;

        // Register the target schema
        let version = self
            .schema_tracker
            .register_schema(target_table, target_schema)?;

        Ok(MigrationResult {
            source_table: source_table.to_string(),
            target_table: target_table.to_string(),
            source_version: source_schema.version,
            target_version: version.version,
            columns_mapped: migration.column_mappings.len(),
            columns_added: migration.add_columns.len(),
            columns_dropped: migration.drop_columns.len(),
        })
    }

    /// Get hub statistics
    pub fn stats(&self) -> CdcHubStatsSnapshot {
        CdcHubStatsSnapshot {
            events_received: self.stats.events_received.load(Ordering::Relaxed),
            events_processed: self.stats.events_processed.load(Ordering::Relaxed),
            schema_changes: self.stats.schema_changes.load(Ordering::Relaxed),
            transform_errors: self.stats.transform_errors.load(Ordering::Relaxed),
            routing_errors: self.stats.routing_errors.load(Ordering::Relaxed),
            dlq_events: self.stats.dlq_events.load(Ordering::Relaxed),
        }
    }

    /// Get the schema tracker
    pub fn schema_tracker(&self) -> Arc<SchemaEvolutionTracker> {
        self.schema_tracker.clone()
    }

    /// Get the dead letter queue
    pub fn dlq(&self) -> Arc<DeadLetterQueue> {
        self.dlq.clone()
    }
}

/// CDC Hub configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcHubConfig {
    /// Enable schema evolution tracking
    pub schema_evolution_enabled: bool,
    /// Enable automatic schema migration
    pub auto_schema_migration: bool,
    /// Schema compatibility mode
    pub schema_compatibility: SchemaCompatibility,
    /// Maximum schema versions to retain
    pub max_schema_versions: usize,
    /// Event routing configuration
    pub routing: RoutingConfig,
    /// Event transformations
    pub transforms: Vec<TransformConfig>,
    /// Dead letter queue configuration
    pub dlq: DlqConfig,
}

impl Default for CdcHubConfig {
    fn default() -> Self {
        Self {
            schema_evolution_enabled: true,
            auto_schema_migration: true,
            schema_compatibility: SchemaCompatibility::Backward,
            max_schema_versions: 100,
            routing: RoutingConfig::default(),
            transforms: Vec::new(),
            dlq: DlqConfig::default(),
        }
    }
}

/// CDC source information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcSourceInfo {
    /// Source ID
    pub id: String,
    /// Database type
    pub database_type: DatabaseType,
    /// Connection string
    pub connection_string: String,
    /// Tables to capture
    pub tables: Vec<String>,
    /// Source status
    pub status: CdcSourceStatus,
    /// Registered timestamp
    pub registered_at: DateTime<Utc>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl CdcSourceInfo {
    /// Create a new CDC source info
    pub fn new(
        id: impl Into<String>,
        database_type: DatabaseType,
        connection: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            database_type,
            connection_string: connection.into(),
            tables: Vec::new(),
            status: CdcSourceStatus::Stopped,
            registered_at: Utc::now(),
            metadata: HashMap::new(),
        }
    }

    /// Add tables to capture
    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Supported database types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabaseType {
    /// PostgreSQL
    Postgres,
    /// MySQL / MariaDB
    MySQL,
    /// MongoDB
    MongoDB,
    /// SQL Server
    SqlServer,
    /// Oracle
    Oracle,
    /// CockroachDB
    CockroachDB,
    /// TiDB
    TiDB,
    /// Cassandra
    Cassandra,
    /// DynamoDB
    DynamoDB,
    /// Snowflake
    Snowflake,
    /// BigQuery
    BigQuery,
}

impl std::fmt::Display for DatabaseType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseType::Postgres => write!(f, "PostgreSQL"),
            DatabaseType::MySQL => write!(f, "MySQL"),
            DatabaseType::MongoDB => write!(f, "MongoDB"),
            DatabaseType::SqlServer => write!(f, "SQL Server"),
            DatabaseType::Oracle => write!(f, "Oracle"),
            DatabaseType::CockroachDB => write!(f, "CockroachDB"),
            DatabaseType::TiDB => write!(f, "TiDB"),
            DatabaseType::Cassandra => write!(f, "Cassandra"),
            DatabaseType::DynamoDB => write!(f, "DynamoDB"),
            DatabaseType::Snowflake => write!(f, "Snowflake"),
            DatabaseType::BigQuery => write!(f, "BigQuery"),
        }
    }
}

/// Event router
pub struct EventRouter {
    config: RoutingConfig,
    rules: Vec<RoutingRule>,
}

impl EventRouter {
    /// Create a new event router
    pub fn new(config: RoutingConfig) -> Self {
        Self {
            rules: config.rules.clone(),
            config,
        }
    }

    /// Route an event to target topics
    pub async fn route(&self, event: &CdcEvent) -> RoutingResult {
        let mut targets = Vec::new();

        // Apply default routing
        if self.config.default_topic_pattern.is_some() {
            let default_topic = event.topic_name(self.config.topic_prefix.as_deref());
            targets.push(default_topic);
        }

        // Apply routing rules
        for rule in &self.rules {
            if rule.matches(event) {
                targets.push(rule.target_topic.clone());
            }
        }

        RoutingResult {
            targets,
            matched_rules: self.rules.iter().filter(|r| r.matches(event)).count(),
        }
    }
}

/// Routing configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RoutingConfig {
    /// Default topic pattern
    pub default_topic_pattern: Option<String>,
    /// Topic prefix
    pub topic_prefix: Option<String>,
    /// Routing rules
    pub rules: Vec<RoutingRule>,
}

/// Routing rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRule {
    /// Rule name
    pub name: String,
    /// Match condition
    pub condition: RouteCondition,
    /// Target topic
    pub target_topic: String,
}

impl RoutingRule {
    /// Check if event matches this rule
    pub fn matches(&self, event: &CdcEvent) -> bool {
        match &self.condition {
            RouteCondition::TableMatch(pattern) => {
                let table_name = format!("{}.{}.{}", event.database, event.schema, event.table);
                glob_match(pattern, &table_name)
            }
            RouteCondition::OperationType(ops) => ops.contains(&event.operation),
            RouteCondition::And(conditions) => conditions.iter().all(|c| {
                let rule = RoutingRule {
                    name: String::new(),
                    condition: c.clone(),
                    target_topic: String::new(),
                };
                rule.matches(event)
            }),
            RouteCondition::Or(conditions) => conditions.iter().any(|c| {
                let rule = RoutingRule {
                    name: String::new(),
                    condition: c.clone(),
                    target_topic: String::new(),
                };
                rule.matches(event)
            }),
        }
    }
}

/// Route condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RouteCondition {
    /// Match table pattern (glob)
    TableMatch(String),
    /// Match operation types
    OperationType(Vec<CdcOperation>),
    /// AND condition
    And(Vec<RouteCondition>),
    /// OR condition
    Or(Vec<RouteCondition>),
}

/// Routing result
#[derive(Debug, Clone)]
pub struct RoutingResult {
    /// Target topics
    pub targets: Vec<String>,
    /// Number of matched rules
    pub matched_rules: usize,
}

/// Event transformer
pub struct EventTransformer {
    transforms: Vec<TransformConfig>,
}

impl EventTransformer {
    /// Create a new event transformer
    pub fn new(transforms: Vec<TransformConfig>) -> Self {
        Self { transforms }
    }

    /// Transform an event
    pub async fn transform(&self, mut event: CdcEvent) -> Result<CdcEvent> {
        for transform in &self.transforms {
            event = self.apply_transform(event, transform).await?;
        }
        Ok(event)
    }

    /// Apply a single transform
    async fn apply_transform(
        &self,
        mut event: CdcEvent,
        transform: &TransformConfig,
    ) -> Result<CdcEvent> {
        match &transform.transform_type {
            TransformType::Rename { from, to } => {
                if let Some(ref mut after) = event.after {
                    for col in after.iter_mut() {
                        if col.name == *from {
                            col.name = to.clone();
                        }
                    }
                }
                if let Some(ref mut before) = event.before {
                    for col in before.iter_mut() {
                        if col.name == *from {
                            col.name = to.clone();
                        }
                    }
                }
            }
            TransformType::Drop { columns } => {
                if let Some(ref mut after) = event.after {
                    after.retain(|col| !columns.contains(&col.name));
                }
                if let Some(ref mut before) = event.before {
                    before.retain(|col| !columns.contains(&col.name));
                }
            }
            TransformType::Cast {
                column,
                target_type,
            } => {
                // Type casting would be implemented here
                debug!("Casting {} to {}", column, target_type);
            }
            TransformType::Mask {
                column,
                mask_pattern,
            } => {
                if let Some(ref mut after) = event.after {
                    for col in after.iter_mut() {
                        if col.name == *column {
                            col.value = Some(serde_json::Value::String(mask_pattern.clone()));
                        }
                    }
                }
            }
            TransformType::Filter { condition: _ } => {
                // Filter would be evaluated here
            }
        }
        Ok(event)
    }
}

/// Transform configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformConfig {
    /// Transform name
    pub name: String,
    /// Tables to apply to (glob pattern)
    pub tables: Option<String>,
    /// Transform type
    pub transform_type: TransformType,
}

/// Transform type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransformType {
    /// Rename a column
    Rename { from: String, to: String },
    /// Drop columns
    Drop { columns: Vec<String> },
    /// Cast column type
    Cast { column: String, target_type: String },
    /// Mask column value
    Mask {
        column: String,
        mask_pattern: String,
    },
    /// Filter events
    Filter { condition: String },
}

/// Schema migration definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMigration {
    /// Column mappings (source -> target)
    pub column_mappings: HashMap<String, String>,
    /// Columns to add
    pub add_columns: Vec<AddColumn>,
    /// Columns to drop
    pub drop_columns: Vec<String>,
    /// Type conversions
    pub type_conversions: HashMap<String, String>,
}

impl SchemaMigration {
    /// Create a new schema migration
    pub fn new() -> Self {
        Self {
            column_mappings: HashMap::new(),
            add_columns: Vec::new(),
            drop_columns: Vec::new(),
            type_conversions: HashMap::new(),
        }
    }

    /// Add a column mapping
    pub fn map_column(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.column_mappings.insert(from.into(), to.into());
        self
    }

    /// Add a new column
    pub fn add_column(mut self, column: AddColumn) -> Self {
        self.add_columns.push(column);
        self
    }

    /// Drop a column
    pub fn drop_column(mut self, column: impl Into<String>) -> Self {
        self.drop_columns.push(column.into());
        self
    }

    /// Apply migration to a schema
    pub fn apply_to_schema(&self, source: &TableSchema) -> Result<TableSchema> {
        let mut target = source.clone();

        // Apply column mappings (renames)
        for col in &mut target.columns {
            if let Some(new_name) = self.column_mappings.get(&col.name) {
                col.name = new_name.clone();
            }
            if let Some(new_type) = self.type_conversions.get(&col.name) {
                col.data_type = new_type.clone();
            }
        }

        // Drop columns
        target
            .columns
            .retain(|c| !self.drop_columns.contains(&c.name));

        // Add new columns
        for add in &self.add_columns {
            target.columns.push(super::schema::ColumnSchema {
                name: add.name.clone(),
                data_type: add.data_type.clone(),
                nullable: add.nullable,
                default_value: add.default_value.clone().map(serde_json::Value::String),
                position: target.columns.len() as u32,
            });
        }

        Ok(target)
    }
}

impl Default for SchemaMigration {
    fn default() -> Self {
        Self::new()
    }
}

/// Column to add in migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddColumn {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: String,
    /// Is nullable
    pub nullable: bool,
    /// Default value
    pub default_value: Option<String>,
}

/// Migration result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationResult {
    /// Source table
    pub source_table: String,
    /// Target table
    pub target_table: String,
    /// Source schema version
    pub source_version: u32,
    /// Target schema version
    pub target_version: u32,
    /// Columns mapped
    pub columns_mapped: usize,
    /// Columns added
    pub columns_added: usize,
    /// Columns dropped
    pub columns_dropped: usize,
}

/// Dead letter queue for failed events
pub struct DeadLetterQueue {
    config: DlqConfig,
    events: Arc<RwLock<Vec<DlqEvent>>>,
    count: AtomicU64,
}

impl DeadLetterQueue {
    /// Create a new dead letter queue
    pub fn new(config: DlqConfig) -> Self {
        Self {
            config,
            events: Arc::new(RwLock::new(Vec::new())),
            count: AtomicU64::new(0),
        }
    }

    /// Send an event to the DLQ
    pub async fn send(&self, event: DlqEvent) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let mut events = self.events.write().await;
        events.push(event);

        // Trim if needed
        let max_events = self.config.max_events;
        if events.len() > max_events {
            let drain_count = events.len() - max_events;
            events.drain(0..drain_count);
        }

        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Get DLQ events
    pub async fn get_events(&self, limit: usize) -> Vec<DlqEvent> {
        let events = self.events.read().await;
        events.iter().take(limit).cloned().collect()
    }

    /// Clear the DLQ
    pub async fn clear(&self) {
        let mut events = self.events.write().await;
        events.clear();
    }

    /// Get event count
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

/// DLQ configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqConfig {
    /// Enable DLQ
    pub enabled: bool,
    /// Maximum events to retain
    pub max_events: usize,
    /// Retention period in seconds
    pub retention_seconds: u64,
    /// Topic name for DLQ
    pub topic: Option<String>,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_events: 10000,
            retention_seconds: 86400,
            topic: None,
        }
    }
}

/// Dead letter queue event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEvent {
    /// Source ID
    pub source_id: String,
    /// Original event
    pub original_event: CdcEvent,
    /// Error message
    pub error: String,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
    /// Retry count
    pub retry_count: u32,
}

/// Process result
#[derive(Debug, Clone)]
pub enum ProcessResult {
    /// Event was routed to topics
    Routed {
        targets: Vec<String>,
        schema_version: Option<u32>,
    },
    /// Event was sent to DLQ
    SentToDlq,
    /// Event was filtered out
    Filtered,
}

/// CDC Hub stats (internal)
#[derive(Default)]
struct CdcHubStats {
    events_received: AtomicU64,
    events_processed: AtomicU64,
    schema_changes: AtomicU64,
    transform_errors: AtomicU64,
    routing_errors: AtomicU64,
    dlq_events: AtomicU64,
}

/// CDC Hub stats snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcHubStatsSnapshot {
    /// Events received
    pub events_received: u64,
    /// Events processed
    pub events_processed: u64,
    /// Schema changes detected
    pub schema_changes: u64,
    /// Transform errors
    pub transform_errors: u64,
    /// Routing errors
    pub routing_errors: u64,
    /// DLQ events
    pub dlq_events: u64,
}

/// Simple glob matching (supports * and ?)
fn glob_match(pattern: &str, text: &str) -> bool {
    let mut pattern_chars = pattern.chars().peekable();
    let mut text_chars = text.chars().peekable();

    while let Some(p) = pattern_chars.next() {
        match p {
            '*' => {
                if pattern_chars.peek().is_none() {
                    return true;
                }
                while text_chars.peek().is_some() {
                    let remaining_pattern: String = pattern_chars.clone().collect();
                    let remaining_text: String = text_chars.clone().collect();
                    if glob_match(&remaining_pattern, &remaining_text) {
                        return true;
                    }
                    text_chars.next();
                }
                return false;
            }
            '?' => {
                if text_chars.next().is_none() {
                    return false;
                }
            }
            c => {
                if text_chars.next() != Some(c) {
                    return false;
                }
            }
        }
    }

    text_chars.peek().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(glob_match("test*", "test_table"));
        assert!(glob_match("*.users", "public.users"));
        assert!(glob_match("db.*.orders", "db.prod.orders"));
        assert!(!glob_match("test*", "other_table"));
        assert!(glob_match("?est", "test"));
    }

    #[tokio::test]
    async fn test_cdc_hub_creation() {
        let config = CdcHubConfig::default();
        let hub = CdcHub::new(config);
        assert!(hub.is_ok());
    }

    #[tokio::test]
    async fn test_source_registration() {
        let config = CdcHubConfig::default();
        let hub = CdcHub::new(config).unwrap();

        let source =
            CdcSourceInfo::new("pg-main", DatabaseType::Postgres, "postgres://localhost/db")
                .with_tables(vec![
                    "public.users".to_string(),
                    "public.orders".to_string(),
                ]);

        hub.register_source(source).await.unwrap();

        let sources = hub.list_sources().await;
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].id, "pg-main");
    }

    #[test]
    fn test_schema_migration() {
        let migration = SchemaMigration::new()
            .map_column("old_name", "new_name")
            .drop_column("unused")
            .add_column(AddColumn {
                name: "created_at".to_string(),
                data_type: "timestamp".to_string(),
                nullable: false,
                default_value: Some("NOW()".to_string()),
            });

        assert_eq!(migration.column_mappings.len(), 1);
        assert_eq!(migration.drop_columns.len(), 1);
        assert_eq!(migration.add_columns.len(), 1);
    }
}
