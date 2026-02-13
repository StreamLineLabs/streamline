//! Schema Evolution Tracking
//!
//! This module tracks schema changes across CDC sources and provides
//! schema compatibility checking and evolution management.

use super::config::{SchemaCompatibility, SchemaEvolutionConfig};
use super::CdcEvent;
use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Schema evolution tracker for CDC sources
pub struct SchemaEvolutionTracker {
    /// Configuration
    config: SchemaEvolutionConfig,
    /// Schema registry (table -> versions)
    schemas: Arc<RwLock<HashMap<String, SchemaHistory>>>,
    /// Schema change listeners
    listeners: Arc<RwLock<Vec<Box<dyn SchemaChangeListener>>>>,
}

impl SchemaEvolutionTracker {
    /// Create a new schema evolution tracker
    pub fn new(config: SchemaEvolutionConfig) -> Self {
        Self {
            config,
            schemas: Arc::new(RwLock::new(HashMap::new())),
            listeners: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Register a schema from a CDC event
    pub fn register_schema(&self, table_key: &str, schema: TableSchema) -> Result<SchemaVersion> {
        if !self.config.enabled {
            return Ok(SchemaVersion {
                version: 0,
                schema,
                registered_at: Utc::now(),
                fingerprint: String::new(),
            });
        }

        let mut schemas = self.schemas.write();
        let history = schemas
            .entry(table_key.to_string())
            .or_insert_with(|| SchemaHistory::new(table_key.to_string()));

        // Check if schema is compatible with the latest
        if let Some(latest) = history.latest() {
            let compatibility = self.check_compatibility(&latest.schema, &schema);

            if !self.is_compatible(&compatibility) {
                if self.config.break_on_incompatible {
                    return Err(StreamlineError::Config(format!(
                        "Schema change for '{}' is not compatible: {:?}",
                        table_key, compatibility
                    )));
                } else {
                    warn!(
                        "Incompatible schema change for '{}': {:?}",
                        table_key, compatibility
                    );
                }
            }
        }

        // Create new version
        let version = history.add_version(schema.clone());

        // Trim old versions if needed
        if history.versions.len() > self.config.max_versions {
            history.trim_to(self.config.max_versions);
        }

        // Notify listeners
        self.notify_schema_change(table_key, &version);

        info!(
            "Registered schema version {} for '{}'",
            version.version, table_key
        );

        Ok(version)
    }

    /// Get the latest schema for a table
    pub fn get_latest_schema(&self, table_key: &str) -> Option<SchemaVersion> {
        let schemas = self.schemas.read();
        schemas.get(table_key).and_then(|h| h.latest().cloned())
    }

    /// Get a specific schema version
    pub fn get_schema_version(&self, table_key: &str, version: u32) -> Option<SchemaVersion> {
        let schemas = self.schemas.read();
        schemas
            .get(table_key)
            .and_then(|h| h.get_version(version).cloned())
    }

    /// Get schema history for a table
    pub fn get_schema_history(&self, table_key: &str) -> Option<Vec<SchemaVersion>> {
        let schemas = self.schemas.read();
        schemas.get(table_key).map(|h| h.versions.clone())
    }

    /// List all tracked tables
    pub fn list_tables(&self) -> Vec<String> {
        let schemas = self.schemas.read();
        schemas.keys().cloned().collect()
    }

    /// Check compatibility between two schemas
    pub fn check_compatibility(
        &self,
        old_schema: &TableSchema,
        new_schema: &TableSchema,
    ) -> CompatibilityResult {
        let mut result = CompatibilityResult {
            is_backward_compatible: true,
            is_forward_compatible: true,
            added_columns: Vec::new(),
            removed_columns: Vec::new(),
            modified_columns: Vec::new(),
            type_changes: Vec::new(),
        };

        // Build column maps
        let old_columns: HashMap<String, &ColumnSchema> = old_schema
            .columns
            .iter()
            .map(|c| (c.name.clone(), c))
            .collect();
        let new_columns: HashMap<String, &ColumnSchema> = new_schema
            .columns
            .iter()
            .map(|c| (c.name.clone(), c))
            .collect();

        // Find added columns
        for (name, col) in &new_columns {
            if !old_columns.contains_key(name) {
                result.added_columns.push(name.clone());
                // Added columns break forward compatibility (old readers can't read new data)
                result.is_forward_compatible = false;
                // New required columns also break backward compatibility
                if !col.nullable && col.default_value.is_none() {
                    result.is_backward_compatible = false;
                }
            }
        }

        // Find removed columns
        for (name, col) in &old_columns {
            if !new_columns.contains_key(name) {
                result.removed_columns.push(name.clone());
                // Removed columns break forward compatibility
                result.is_forward_compatible = false;
                // Required column removal breaks backward compatibility
                if !col.nullable {
                    result.is_backward_compatible = false;
                }
            }
        }

        // Find modified columns
        for (name, old_col) in &old_columns {
            if let Some(new_col) = new_columns.get(name) {
                if old_col.data_type != new_col.data_type {
                    result.type_changes.push(TypeChange {
                        column: name.clone(),
                        old_type: old_col.data_type.clone(),
                        new_type: new_col.data_type.clone(),
                        is_widening: is_widening_type_change(
                            &old_col.data_type,
                            &new_col.data_type,
                        ),
                    });

                    // Narrowing type changes break both directions
                    if !is_widening_type_change(&old_col.data_type, &new_col.data_type) {
                        result.is_backward_compatible = false;
                        result.is_forward_compatible = false;
                    }
                }

                if old_col.nullable != new_col.nullable {
                    result.modified_columns.push(name.clone());
                    // Making nullable -> not-null breaks backward
                    if old_col.nullable && !new_col.nullable {
                        result.is_backward_compatible = false;
                    }
                    // Making not-null -> nullable breaks forward
                    if !old_col.nullable && new_col.nullable {
                        result.is_forward_compatible = false;
                    }
                }
            }
        }

        result
    }

    /// Check if compatibility result matches configured mode
    fn is_compatible(&self, result: &CompatibilityResult) -> bool {
        match self.config.compatibility {
            SchemaCompatibility::None => true,
            SchemaCompatibility::Backward => result.is_backward_compatible,
            SchemaCompatibility::Forward => result.is_forward_compatible,
            SchemaCompatibility::Full => {
                result.is_backward_compatible && result.is_forward_compatible
            }
        }
    }

    /// Register a schema change listener
    pub fn add_listener(&self, listener: Box<dyn SchemaChangeListener>) {
        let mut listeners = self.listeners.write();
        listeners.push(listener);
    }

    /// Notify listeners of schema change
    fn notify_schema_change(&self, table_key: &str, version: &SchemaVersion) {
        let listeners = self.listeners.read();
        for listener in listeners.iter() {
            listener.on_schema_change(table_key, version);
        }
    }

    /// Detect schema changes from CDC events
    pub fn detect_schema_from_event(&self, event: &CdcEvent) -> Option<TableSchema> {
        if !self.config.auto_evolve {
            return None;
        }

        // Extract schema from event columns
        let columns = event.after.as_ref().or(event.before.as_ref())?;

        let column_schemas: Vec<ColumnSchema> = columns
            .iter()
            .map(|col| ColumnSchema {
                name: col.name.clone(),
                data_type: col.data_type.clone(),
                nullable: col.value.is_none(),
                default_value: None,
                position: 0,
            })
            .collect();

        Some(TableSchema {
            database: event.database.clone(),
            schema: event.schema.clone(),
            table: event.table.clone(),
            columns: column_schemas,
            primary_key: event.primary_key.clone(),
            created_at: event.timestamp,
        })
    }

    /// Process a CDC event and track schema if needed
    pub fn process_event(&self, event: &CdcEvent) -> Result<Option<SchemaVersion>> {
        if !self.config.enabled {
            return Ok(None);
        }

        let table_key = format!("{}.{}.{}", event.database, event.schema, event.table);

        // Check if we already have this schema
        if let Some(latest) = self.get_latest_schema(&table_key) {
            // Compare with event columns
            if let Some(detected) = self.detect_schema_from_event(event) {
                let columns_match = latest.schema.columns.len() == detected.columns.len()
                    && latest
                        .schema
                        .columns
                        .iter()
                        .all(|c| detected.columns.iter().any(|d| d.name == c.name));

                if !columns_match {
                    debug!("Schema change detected for '{}'", table_key);
                    return Ok(Some(self.register_schema(&table_key, detected)?));
                }
            }
            return Ok(None);
        }

        // First time seeing this table
        if let Some(schema) = self.detect_schema_from_event(event) {
            return Ok(Some(self.register_schema(&table_key, schema)?));
        }

        Ok(None)
    }

    /// Get schema evolution statistics
    pub fn stats(&self) -> SchemaEvolutionStats {
        let schemas = self.schemas.read();

        let total_tables = schemas.len();
        let total_versions: usize = schemas.values().map(|h| h.versions.len()).sum();

        SchemaEvolutionStats {
            total_tables,
            total_versions,
            compatibility_mode: format!("{:?}", self.config.compatibility),
            auto_evolve_enabled: self.config.auto_evolve,
        }
    }
}

/// Schema history for a table
#[derive(Debug, Clone)]
pub struct SchemaHistory {
    /// Table key (database.schema.table)
    pub table_key: String,
    /// Schema versions
    pub versions: Vec<SchemaVersion>,
}

impl SchemaHistory {
    /// Create new schema history
    pub fn new(table_key: String) -> Self {
        Self {
            table_key,
            versions: Vec::new(),
        }
    }

    /// Get latest version
    pub fn latest(&self) -> Option<&SchemaVersion> {
        self.versions.last()
    }

    /// Get specific version
    pub fn get_version(&self, version: u32) -> Option<&SchemaVersion> {
        self.versions.iter().find(|v| v.version == version)
    }

    /// Add a new version
    pub fn add_version(&mut self, schema: TableSchema) -> SchemaVersion {
        let version = self.versions.len() as u32 + 1;
        let fingerprint = compute_schema_fingerprint(&schema);

        let schema_version = SchemaVersion {
            version,
            schema,
            registered_at: Utc::now(),
            fingerprint,
        };

        self.versions.push(schema_version.clone());
        schema_version
    }

    /// Trim to maximum number of versions
    pub fn trim_to(&mut self, max: usize) {
        if self.versions.len() > max {
            let remove_count = self.versions.len() - max;
            self.versions.drain(0..remove_count);
        }
    }
}

/// A specific schema version
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    /// Version number
    pub version: u32,
    /// The schema
    pub schema: TableSchema,
    /// When this version was registered
    pub registered_at: DateTime<Utc>,
    /// Schema fingerprint (hash)
    pub fingerprint: String,
}

/// Table schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    /// Database name
    pub database: String,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Columns
    pub columns: Vec<ColumnSchema>,
    /// Primary key columns
    pub primary_key: Vec<String>,
    /// When schema was captured
    pub created_at: DateTime<Utc>,
}

impl TableSchema {
    /// Get the full table name
    pub fn full_name(&self) -> String {
        format!("{}.{}.{}", self.database, self.schema, self.table)
    }
}

/// Column schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: String,
    /// Is nullable
    pub nullable: bool,
    /// Default value
    pub default_value: Option<serde_json::Value>,
    /// Ordinal position
    pub position: u32,
}

/// Schema compatibility check result
#[derive(Debug, Clone)]
pub struct CompatibilityResult {
    /// Is backward compatible (new readers can read old data)
    pub is_backward_compatible: bool,
    /// Is forward compatible (old readers can read new data)
    pub is_forward_compatible: bool,
    /// Added columns
    pub added_columns: Vec<String>,
    /// Removed columns
    pub removed_columns: Vec<String>,
    /// Modified columns
    pub modified_columns: Vec<String>,
    /// Type changes
    pub type_changes: Vec<TypeChange>,
}

impl CompatibilityResult {
    /// Check if any changes were detected
    pub fn has_changes(&self) -> bool {
        !self.added_columns.is_empty()
            || !self.removed_columns.is_empty()
            || !self.modified_columns.is_empty()
            || !self.type_changes.is_empty()
    }
}

/// Type change information
#[derive(Debug, Clone)]
pub struct TypeChange {
    /// Column name
    pub column: String,
    /// Old type
    pub old_type: String,
    /// New type
    pub new_type: String,
    /// Is this a widening change (safe)
    pub is_widening: bool,
}

/// Schema change listener trait
pub trait SchemaChangeListener: Send + Sync {
    /// Called when a schema changes
    fn on_schema_change(&self, table_key: &str, version: &SchemaVersion);
}

/// Schema evolution statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEvolutionStats {
    /// Total tables tracked
    pub total_tables: usize,
    /// Total schema versions
    pub total_versions: usize,
    /// Compatibility mode
    pub compatibility_mode: String,
    /// Auto-evolve enabled
    pub auto_evolve_enabled: bool,
}

/// Check if a type change is a widening change (safe promotion)
fn is_widening_type_change(old_type: &str, new_type: &str) -> bool {
    // Define safe type promotions
    let old_lower = old_type.to_lowercase();
    let new_lower = new_type.to_lowercase();

    // Integer promotions
    if old_lower.contains("int") && new_lower.contains("bigint") {
        return true;
    }
    if old_lower.contains("smallint") && new_lower.contains("int") {
        return true;
    }
    if old_lower.contains("tinyint") && new_lower.contains("int") {
        return true;
    }

    // Float promotions
    if old_lower.contains("float") && new_lower.contains("double") {
        return true;
    }
    if old_lower.contains("real") && new_lower.contains("double") {
        return true;
    }

    // Decimal precision increase
    if old_lower.contains("decimal") && new_lower.contains("decimal") {
        // Would need to parse precision - simplified here
        return true;
    }

    // String length increase
    if (old_lower.contains("varchar") || old_lower.contains("char"))
        && (new_lower.contains("varchar") || new_lower.contains("text"))
    {
        return true;
    }

    false
}

/// Compute a fingerprint for a schema
fn compute_schema_fingerprint(schema: &TableSchema) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();

    schema.database.hash(&mut hasher);
    schema.schema.hash(&mut hasher);
    schema.table.hash(&mut hasher);

    for col in &schema.columns {
        col.name.hash(&mut hasher);
        col.data_type.hash(&mut hasher);
        col.nullable.hash(&mut hasher);
    }

    for pk in &schema.primary_key {
        pk.hash(&mut hasher);
    }

    format!("{:016x}", hasher.finish())
}

/// Schema registry for storing schemas (can be backed by topic or external registry)
pub struct SchemaRegistry {
    /// Tracker instance
    tracker: SchemaEvolutionTracker,
    /// History topic name
    history_topic: Option<String>,
}

impl SchemaRegistry {
    /// Create a new schema registry
    pub fn new(config: SchemaEvolutionConfig) -> Self {
        let history_topic = config.history_topic.clone();
        Self {
            tracker: SchemaEvolutionTracker::new(config),
            history_topic,
        }
    }

    /// Register a schema
    pub fn register(&self, table_key: &str, schema: TableSchema) -> Result<SchemaVersion> {
        self.tracker.register_schema(table_key, schema)
    }

    /// Get latest schema
    pub fn get_latest(&self, table_key: &str) -> Option<SchemaVersion> {
        self.tracker.get_latest_schema(table_key)
    }

    /// Get schema by version
    pub fn get_version(&self, table_key: &str, version: u32) -> Option<SchemaVersion> {
        self.tracker.get_schema_version(table_key, version)
    }

    /// Get history topic name
    pub fn history_topic(&self) -> Option<&str> {
        self.history_topic.as_deref()
    }

    /// Get the underlying tracker
    pub fn tracker(&self) -> &SchemaEvolutionTracker {
        &self.tracker
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_schema(columns: Vec<(&str, &str, bool)>) -> TableSchema {
        TableSchema {
            database: "test_db".to_string(),
            schema: "public".to_string(),
            table: "test_table".to_string(),
            columns: columns
                .into_iter()
                .enumerate()
                .map(|(i, (name, data_type, nullable))| ColumnSchema {
                    name: name.to_string(),
                    data_type: data_type.to_string(),
                    nullable,
                    default_value: None,
                    position: i as u32,
                })
                .collect(),
            primary_key: vec!["id".to_string()],
            created_at: Utc::now(),
        }
    }

    #[test]
    fn test_schema_registration() {
        let config = SchemaEvolutionConfig::default();
        let tracker = SchemaEvolutionTracker::new(config);

        let schema = create_test_schema(vec![("id", "int", false), ("name", "varchar", true)]);

        let version = tracker.register_schema("test.table", schema).unwrap();
        assert_eq!(version.version, 1);
    }

    #[test]
    fn test_get_latest_schema() {
        let config = SchemaEvolutionConfig::default();
        let tracker = SchemaEvolutionTracker::new(config);

        let schema1 = create_test_schema(vec![("id", "int", false)]);
        tracker.register_schema("test.table", schema1).unwrap();

        let schema2 = create_test_schema(vec![("id", "int", false), ("name", "varchar", true)]);
        tracker.register_schema("test.table", schema2).unwrap();

        let latest = tracker.get_latest_schema("test.table").unwrap();
        assert_eq!(latest.version, 2);
        assert_eq!(latest.schema.columns.len(), 2);
    }

    #[test]
    fn test_compatibility_added_column() {
        let config = SchemaEvolutionConfig::default();
        let tracker = SchemaEvolutionTracker::new(config);

        let old_schema = create_test_schema(vec![("id", "int", false)]);
        let new_schema = create_test_schema(vec![("id", "int", false), ("name", "varchar", true)]);

        let result = tracker.check_compatibility(&old_schema, &new_schema);

        assert!(result.is_backward_compatible);
        assert!(!result.is_forward_compatible);
        assert_eq!(result.added_columns, vec!["name".to_string()]);
    }

    #[test]
    fn test_compatibility_removed_column() {
        let config = SchemaEvolutionConfig::default();
        let tracker = SchemaEvolutionTracker::new(config);

        let old_schema = create_test_schema(vec![("id", "int", false), ("name", "varchar", true)]);
        let new_schema = create_test_schema(vec![("id", "int", false)]);

        let result = tracker.check_compatibility(&old_schema, &new_schema);

        assert!(result.is_backward_compatible); // nullable column removed
        assert!(!result.is_forward_compatible);
        assert_eq!(result.removed_columns, vec!["name".to_string()]);
    }

    #[test]
    fn test_compatibility_type_change() {
        let config = SchemaEvolutionConfig::default();
        let tracker = SchemaEvolutionTracker::new(config);

        let old_schema = create_test_schema(vec![("id", "int", false)]);
        let new_schema = create_test_schema(vec![("id", "bigint", false)]);

        let result = tracker.check_compatibility(&old_schema, &new_schema);

        assert!(result.is_backward_compatible); // int -> bigint is safe
        assert_eq!(result.type_changes.len(), 1);
        assert!(result.type_changes[0].is_widening);
    }

    #[test]
    fn test_schema_fingerprint() {
        let schema1 = create_test_schema(vec![("id", "int", false)]);
        let schema2 = create_test_schema(vec![("id", "int", false)]);
        let schema3 = create_test_schema(vec![("id", "bigint", false)]);

        let fp1 = compute_schema_fingerprint(&schema1);
        let fp2 = compute_schema_fingerprint(&schema2);
        let fp3 = compute_schema_fingerprint(&schema3);

        assert_eq!(fp1, fp2);
        assert_ne!(fp1, fp3);
    }

    #[test]
    fn test_schema_history_trim() {
        let config = SchemaEvolutionConfig {
            max_versions: 2,
            ..Default::default()
        };
        let tracker = SchemaEvolutionTracker::new(config);

        // Register 3 versions
        for i in 1..=3 {
            let schema = create_test_schema(vec![(&format!("col{}", i), "int", false)]);
            tracker.register_schema("test.table", schema).unwrap();
        }

        let history = tracker.get_schema_history("test.table").unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].version, 2); // First version should be trimmed
    }

    #[test]
    fn test_is_widening_type_change() {
        assert!(is_widening_type_change("int", "bigint"));
        assert!(is_widening_type_change("float", "double"));
        assert!(is_widening_type_change("varchar(50)", "text"));
        assert!(!is_widening_type_change("bigint", "int"));
    }

    #[test]
    fn test_stats() {
        let config = SchemaEvolutionConfig::default();
        let tracker = SchemaEvolutionTracker::new(config);

        let schema = create_test_schema(vec![("id", "int", false)]);
        tracker
            .register_schema("test.table1", schema.clone())
            .unwrap();
        tracker
            .register_schema("test.table1", schema.clone())
            .unwrap();
        tracker.register_schema("test.table2", schema).unwrap();

        let stats = tracker.stats();
        assert_eq!(stats.total_tables, 2);
        assert_eq!(stats.total_versions, 3);
    }
}
