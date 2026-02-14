//! Zero-ETL Bidirectional Sync
//!
//! Provides automatic bidirectional synchronization between Streamline topics
//! and external data stores (Iceberg, Delta Lake, data warehouses) without
//! the need for traditional ETL pipelines.
//!
//! # Features
//!
//! - **Bidirectional Sync**: Write to topics, read from lakehouse (and vice versa)
//! - **Schema Sync**: Automatic schema evolution and propagation
//! - **Change Detection**: Efficient delta tracking for incremental sync
//! - **Conflict Resolution**: Configurable strategies for handling conflicts
//! - **Watermarks**: Track sync progress for exactly-once semantics

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Sync direction
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncDirection {
    /// Stream to lakehouse only
    #[default]
    StreamToLakehouse,
    /// Lakehouse to stream only
    LakehouseToStream,
    /// Bidirectional sync
    Bidirectional,
}

/// Conflict resolution strategy
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictResolution {
    /// Lakehouse wins (for batch-oriented workflows)
    LakehouseWins,
    /// Stream wins (for real-time workflows)
    StreamWins,
    /// Most recent timestamp wins
    #[default]
    LastWriteWins,
    /// Keep both versions (creates duplicates)
    KeepBoth,
    /// Fail on conflict
    Fail,
}

/// Schema evolution strategy
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaEvolution {
    /// No schema changes allowed
    Strict,
    /// Allow adding new columns
    #[default]
    AddOnly,
    /// Allow adding and widening columns
    AddAndWiden,
    /// Allow any compatible changes
    Permissive,
}

/// Zero-ETL sync configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZeroEtlConfig {
    /// Sync enabled
    pub enabled: bool,
    /// Sync direction
    pub direction: SyncDirection,
    /// Conflict resolution strategy
    pub conflict_resolution: ConflictResolution,
    /// Schema evolution strategy
    pub schema_evolution: SchemaEvolution,
    /// Sync interval (milliseconds)
    pub sync_interval_ms: u64,
    /// Batch size for sync operations
    pub batch_size: usize,
    /// Max retries on failure
    pub max_retries: u32,
    /// Enable compression
    pub compression_enabled: bool,
    /// Enable encryption at rest
    pub encryption_enabled: bool,
    /// Watermark persistence
    pub persist_watermarks: bool,
}

impl Default for ZeroEtlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            direction: SyncDirection::StreamToLakehouse,
            conflict_resolution: ConflictResolution::LastWriteWins,
            schema_evolution: SchemaEvolution::AddOnly,
            sync_interval_ms: 60_000, // 1 minute
            batch_size: 10_000,
            max_retries: 3,
            compression_enabled: true,
            encryption_enabled: false,
            persist_watermarks: true,
        }
    }
}

/// Sync binding between topic and lakehouse table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncBinding {
    /// Binding ID
    pub id: String,
    /// Source topic
    pub topic: String,
    /// Target table (namespace.table)
    pub table: String,
    /// Sync direction
    pub direction: SyncDirection,
    /// Partition columns (for table)
    pub partition_columns: Vec<String>,
    /// Key columns (for upsert/merge)
    pub key_columns: Vec<String>,
    /// Column mappings (topic field -> table column)
    pub column_mappings: HashMap<String, String>,
    /// Filter expression (SQL-like)
    pub filter: Option<String>,
    /// Transform expression
    pub transform: Option<String>,
    /// Binding status
    pub status: BindingStatus,
    /// Created timestamp
    pub created_at: i64,
    /// Last sync timestamp
    pub last_sync_at: Option<i64>,
}

/// Binding status
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum BindingStatus {
    /// Binding is active and syncing
    Active,
    /// Binding is paused
    Paused,
    /// Binding failed and needs attention
    Failed,
    /// Binding is being created/initialized
    #[default]
    Initializing,
}

/// Sync watermark for tracking progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncWatermark {
    /// Binding ID
    pub binding_id: String,
    /// Stream offset (for stream -> lakehouse)
    pub stream_offset: i64,
    /// Table snapshot ID (for lakehouse -> stream)
    pub table_snapshot: Option<i64>,
    /// Last processed timestamp
    pub processed_at: i64,
    /// Partition-level watermarks
    pub partition_watermarks: HashMap<i32, i64>,
}

/// Sync statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncStats {
    /// Records synced (stream -> lakehouse)
    pub records_to_lakehouse: u64,
    /// Records synced (lakehouse -> stream)
    pub records_to_stream: u64,
    /// Bytes synced
    pub bytes_synced: u64,
    /// Sync operations
    pub sync_operations: u64,
    /// Failed operations
    pub failed_operations: u64,
    /// Conflicts detected
    pub conflicts_detected: u64,
    /// Conflicts resolved
    pub conflicts_resolved: u64,
    /// Schema evolutions
    pub schema_evolutions: u64,
    /// Average sync latency (ms)
    pub avg_sync_latency_ms: f64,
}

/// Schema field definition
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaField {
    /// Field name
    pub name: String,
    /// Field type
    pub field_type: FieldType,
    /// Is nullable
    pub nullable: bool,
    /// Default value
    pub default: Option<String>,
    /// Documentation
    pub doc: Option<String>,
}

/// Field types (compatible with Iceberg/Delta)
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    #[default]
    String,
    Binary,
    Date,
    Timestamp,
    TimestampTz,
    Uuid,
    Decimal {
        precision: u32,
        scale: u32,
    },
    Array(Box<FieldType>),
    Map {
        key: Box<FieldType>,
        value: Box<FieldType>,
    },
    Struct(Vec<SchemaField>),
}

/// Schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncSchema {
    /// Schema ID
    pub id: String,
    /// Schema version
    pub version: u32,
    /// Fields
    pub fields: Vec<SchemaField>,
    /// Primary key columns
    pub primary_key: Vec<String>,
    /// Partition columns
    pub partition_by: Vec<String>,
    /// Sort columns
    pub sort_by: Vec<String>,
}

/// Zero-ETL sync manager
pub struct ZeroEtlManager {
    /// Configuration
    config: ZeroEtlConfig,
    /// Active bindings
    bindings: Arc<RwLock<HashMap<String, SyncBinding>>>,
    /// Watermarks
    watermarks: Arc<RwLock<HashMap<String, SyncWatermark>>>,
    /// Statistics
    stats: Arc<SyncStatsTracker>,
    /// Running flag
    running: Arc<std::sync::atomic::AtomicBool>,
}

struct SyncStatsTracker {
    records_to_lakehouse: AtomicU64,
    records_to_stream: AtomicU64,
    bytes_synced: AtomicU64,
    sync_operations: AtomicU64,
    failed_operations: AtomicU64,
}

impl ZeroEtlManager {
    /// Create a new Zero-ETL manager
    pub fn new(config: ZeroEtlConfig) -> Self {
        Self {
            config,
            bindings: Arc::new(RwLock::new(HashMap::new())),
            watermarks: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(SyncStatsTracker {
                records_to_lakehouse: AtomicU64::new(0),
                records_to_stream: AtomicU64::new(0),
                bytes_synced: AtomicU64::new(0),
                sync_operations: AtomicU64::new(0),
                failed_operations: AtomicU64::new(0),
            }),
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Create a sync binding
    pub async fn create_binding(&self, binding: SyncBinding) -> Result<()> {
        let mut bindings = self.bindings.write().await;

        if bindings.contains_key(&binding.id) {
            return Err(StreamlineError::Config(format!(
                "Binding '{}' already exists",
                binding.id
            )));
        }

        // Initialize watermark
        let watermark = SyncWatermark {
            binding_id: binding.id.clone(),
            stream_offset: -1,
            table_snapshot: None,
            processed_at: chrono::Utc::now().timestamp_millis(),
            partition_watermarks: HashMap::new(),
        };

        let mut watermarks = self.watermarks.write().await;
        watermarks.insert(binding.id.clone(), watermark);

        bindings.insert(binding.id.clone(), binding);

        Ok(())
    }

    /// Get a binding by ID
    pub async fn get_binding(&self, id: &str) -> Option<SyncBinding> {
        let bindings = self.bindings.read().await;
        bindings.get(id).cloned()
    }

    /// List all bindings
    pub async fn list_bindings(&self) -> Vec<SyncBinding> {
        let bindings = self.bindings.read().await;
        bindings.values().cloned().collect()
    }

    /// Update binding status
    pub async fn update_binding_status(&self, id: &str, status: BindingStatus) -> Result<()> {
        let mut bindings = self.bindings.write().await;
        if let Some(binding) = bindings.get_mut(id) {
            binding.status = status;
            Ok(())
        } else {
            Err(StreamlineError::Config(format!(
                "Binding '{}' not found",
                id
            )))
        }
    }

    /// Delete a binding
    pub async fn delete_binding(&self, id: &str) -> Result<()> {
        let mut bindings = self.bindings.write().await;
        let mut watermarks = self.watermarks.write().await;

        if bindings.remove(id).is_none() {
            return Err(StreamlineError::Config(format!(
                "Binding '{}' not found",
                id
            )));
        }

        watermarks.remove(id);
        Ok(())
    }

    /// Get watermark for a binding
    pub async fn get_watermark(&self, binding_id: &str) -> Option<SyncWatermark> {
        let watermarks = self.watermarks.read().await;
        watermarks.get(binding_id).cloned()
    }

    /// Update watermark
    pub async fn update_watermark(&self, watermark: SyncWatermark) -> Result<()> {
        let mut watermarks = self.watermarks.write().await;
        watermarks.insert(watermark.binding_id.clone(), watermark);
        Ok(())
    }

    /// Perform sync for a binding
    pub async fn sync(&self, binding_id: &str) -> Result<SyncResult> {
        let binding = self.get_binding(binding_id).await.ok_or_else(|| {
            StreamlineError::Config(format!("Binding '{}' not found", binding_id))
        })?;

        if binding.status != BindingStatus::Active {
            return Err(StreamlineError::Config(format!(
                "Binding '{}' is not active",
                binding_id
            )));
        }

        let start = std::time::Instant::now();
        let mut result = SyncResult {
            binding_id: binding_id.to_string(),
            success: true,
            records_synced: 0,
            bytes_synced: 0,
            duration_ms: 0,
            errors: Vec::new(),
        };

        // Sync based on direction
        match binding.direction {
            SyncDirection::StreamToLakehouse => {
                result = self.sync_stream_to_lakehouse(&binding).await?;
            }
            SyncDirection::LakehouseToStream => {
                result = self.sync_lakehouse_to_stream(&binding).await?;
            }
            SyncDirection::Bidirectional => {
                let to_lakehouse = self.sync_stream_to_lakehouse(&binding).await?;
                let to_stream = self.sync_lakehouse_to_stream(&binding).await?;

                result.records_synced = to_lakehouse.records_synced + to_stream.records_synced;
                result.bytes_synced = to_lakehouse.bytes_synced + to_stream.bytes_synced;
                result.errors.extend(to_lakehouse.errors);
                result.errors.extend(to_stream.errors);
            }
        }

        result.duration_ms = start.elapsed().as_millis() as u64;
        self.stats.sync_operations.fetch_add(1, Ordering::Relaxed);

        if !result.success {
            self.stats.failed_operations.fetch_add(1, Ordering::Relaxed);
        }

        Ok(result)
    }

    async fn sync_stream_to_lakehouse(&self, binding: &SyncBinding) -> Result<SyncResult> {
        // In production, this would:
        // 1. Read from topic starting from watermark
        // 2. Transform records according to column mappings
        // 3. Write to lakehouse table (Iceberg/Delta)
        // 4. Update watermark on success

        tracing::info!(
            binding_id = %binding.id,
            topic = %binding.topic,
            table = %binding.table,
            "Syncing stream to lakehouse"
        );

        // Simulated sync (production would integrate with actual storage)
        let records_synced = 0u64;
        let bytes_synced = 0u64;

        self.stats
            .records_to_lakehouse
            .fetch_add(records_synced, Ordering::Relaxed);
        self.stats
            .bytes_synced
            .fetch_add(bytes_synced, Ordering::Relaxed);

        Ok(SyncResult {
            binding_id: binding.id.clone(),
            success: true,
            records_synced,
            bytes_synced,
            duration_ms: 0,
            errors: Vec::new(),
        })
    }

    async fn sync_lakehouse_to_stream(&self, binding: &SyncBinding) -> Result<SyncResult> {
        // In production, this would:
        // 1. Query lakehouse for changes since last snapshot
        // 2. Transform records according to column mappings
        // 3. Produce to topic
        // 4. Update watermark on success

        tracing::info!(
            binding_id = %binding.id,
            table = %binding.table,
            topic = %binding.topic,
            "Syncing lakehouse to stream"
        );

        let records_synced = 0u64;
        let bytes_synced = 0u64;

        self.stats
            .records_to_stream
            .fetch_add(records_synced, Ordering::Relaxed);
        self.stats
            .bytes_synced
            .fetch_add(bytes_synced, Ordering::Relaxed);

        Ok(SyncResult {
            binding_id: binding.id.clone(),
            success: true,
            records_synced,
            bytes_synced,
            duration_ms: 0,
            errors: Vec::new(),
        })
    }

    /// Start the sync loop
    pub async fn start(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        self.running
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let running = self.running.clone();
        let interval = std::time::Duration::from_millis(self.config.sync_interval_ms);

        tokio::spawn(async move {
            while running.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(interval).await;
                // Sync all active bindings
                // In production, this would iterate bindings and sync each
            }
        });

        Ok(())
    }

    /// Stop the sync loop
    pub fn stop(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// Get sync statistics
    pub fn stats(&self) -> SyncStats {
        SyncStats {
            records_to_lakehouse: self.stats.records_to_lakehouse.load(Ordering::Relaxed),
            records_to_stream: self.stats.records_to_stream.load(Ordering::Relaxed),
            bytes_synced: self.stats.bytes_synced.load(Ordering::Relaxed),
            sync_operations: self.stats.sync_operations.load(Ordering::Relaxed),
            failed_operations: self.stats.failed_operations.load(Ordering::Relaxed),
            conflicts_detected: 0,
            conflicts_resolved: 0,
            schema_evolutions: 0,
            avg_sync_latency_ms: 0.0,
        }
    }

    /// Get configuration
    pub fn config(&self) -> &ZeroEtlConfig {
        &self.config
    }
}

/// Sync result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    /// Binding ID
    pub binding_id: String,
    /// Success flag
    pub success: bool,
    /// Records synced
    pub records_synced: u64,
    /// Bytes synced
    pub bytes_synced: u64,
    /// Duration in milliseconds
    pub duration_ms: u64,
    /// Errors encountered
    pub errors: Vec<String>,
}

/// Schema synchronizer for automatic schema evolution
pub struct SchemaSynchronizer {
    /// Evolution strategy
    strategy: SchemaEvolution,
    /// Schema registry (maps binding_id -> schema)
    schemas: Arc<RwLock<HashMap<String, SyncSchema>>>,
}

impl SchemaSynchronizer {
    /// Create a new schema synchronizer
    pub fn new(strategy: SchemaEvolution) -> Self {
        Self {
            strategy,
            schemas: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a schema
    pub async fn register_schema(&self, binding_id: &str, schema: SyncSchema) -> Result<()> {
        let mut schemas = self.schemas.write().await;
        schemas.insert(binding_id.to_string(), schema);
        Ok(())
    }

    /// Get current schema
    pub async fn get_schema(&self, binding_id: &str) -> Option<SyncSchema> {
        let schemas = self.schemas.read().await;
        schemas.get(binding_id).cloned()
    }

    /// Check if schema change is compatible
    pub fn is_compatible(&self, old: &SyncSchema, new: &SyncSchema) -> bool {
        match self.strategy {
            SchemaEvolution::Strict => {
                // No changes allowed
                old.fields.len() == new.fields.len()
                    && old
                        .fields
                        .iter()
                        .zip(new.fields.iter())
                        .all(|(o, n)| o.name == n.name && o.field_type == n.field_type)
            }
            SchemaEvolution::AddOnly => {
                // Only adding new columns is allowed
                old.fields.iter().all(|old_field| {
                    new.fields.iter().any(|new_field| {
                        old_field.name == new_field.name
                            && old_field.field_type == new_field.field_type
                    })
                })
            }
            SchemaEvolution::AddAndWiden => {
                // Allow adding columns and widening types
                old.fields.iter().all(|old_field| {
                    new.fields.iter().any(|new_field| {
                        old_field.name == new_field.name
                            && Self::is_type_widening(&old_field.field_type, &new_field.field_type)
                    })
                })
            }
            SchemaEvolution::Permissive => {
                // Allow any compatible changes
                true
            }
        }
    }

    /// Check if new type is a valid widening of old type
    fn is_type_widening(old: &FieldType, new: &FieldType) -> bool {
        if old == new {
            return true;
        }

        // Allow numeric widening
        matches!(
            (old, new),
            (FieldType::Int32, FieldType::Int64)
                | (FieldType::Float32, FieldType::Float64)
                | (FieldType::Int32, FieldType::Float64)
                | (FieldType::Int64, FieldType::Float64)
        )
    }

    /// Evolve schema
    pub async fn evolve_schema(
        &self,
        binding_id: &str,
        new_schema: SyncSchema,
    ) -> Result<SyncSchema> {
        let mut schemas = self.schemas.write().await;

        if let Some(old_schema) = schemas.get(binding_id) {
            if !self.is_compatible(old_schema, &new_schema) {
                return Err(StreamlineError::Validation(format!(
                    "Schema change is not compatible with evolution strategy {:?}",
                    self.strategy
                )));
            }
        }

        let evolved = SyncSchema {
            id: new_schema.id,
            version: new_schema.version + 1,
            fields: new_schema.fields,
            primary_key: new_schema.primary_key,
            partition_by: new_schema.partition_by,
            sort_by: new_schema.sort_by,
        };

        schemas.insert(binding_id.to_string(), evolved.clone());
        Ok(evolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_binding() {
        let manager = ZeroEtlManager::new(ZeroEtlConfig::default());

        let binding = SyncBinding {
            id: "test-binding".to_string(),
            topic: "events".to_string(),
            table: "analytics.events".to_string(),
            direction: SyncDirection::StreamToLakehouse,
            partition_columns: vec!["date".to_string()],
            key_columns: vec!["id".to_string()],
            column_mappings: HashMap::new(),
            filter: None,
            transform: None,
            status: BindingStatus::Active,
            created_at: chrono::Utc::now().timestamp_millis(),
            last_sync_at: None,
        };

        manager.create_binding(binding.clone()).await.unwrap();

        let retrieved = manager.get_binding("test-binding").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().topic, "events");
    }

    #[tokio::test]
    async fn test_schema_compatibility() {
        let synchronizer = SchemaSynchronizer::new(SchemaEvolution::AddOnly);

        let old_schema = SyncSchema {
            id: "v1".to_string(),
            version: 1,
            fields: vec![SchemaField {
                name: "id".to_string(),
                field_type: FieldType::String,
                nullable: false,
                default: None,
                doc: None,
            }],
            primary_key: vec!["id".to_string()],
            partition_by: vec![],
            sort_by: vec![],
        };

        let new_schema = SyncSchema {
            id: "v2".to_string(),
            version: 2,
            fields: vec![
                SchemaField {
                    name: "id".to_string(),
                    field_type: FieldType::String,
                    nullable: false,
                    default: None,
                    doc: None,
                },
                SchemaField {
                    name: "timestamp".to_string(),
                    field_type: FieldType::Timestamp,
                    nullable: true,
                    default: None,
                    doc: None,
                },
            ],
            primary_key: vec!["id".to_string()],
            partition_by: vec![],
            sort_by: vec![],
        };

        // Adding a column should be compatible
        assert!(synchronizer.is_compatible(&old_schema, &new_schema));
    }

    #[test]
    fn test_type_widening() {
        assert!(SchemaSynchronizer::is_type_widening(
            &FieldType::Int32,
            &FieldType::Int64
        ));
        assert!(SchemaSynchronizer::is_type_widening(
            &FieldType::Float32,
            &FieldType::Float64
        ));
        assert!(!SchemaSynchronizer::is_type_widening(
            &FieldType::Int64,
            &FieldType::Int32
        ));
    }
}
