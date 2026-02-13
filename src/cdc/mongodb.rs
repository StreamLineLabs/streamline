//! MongoDB CDC source implementation
//!
//! This module provides CDC capabilities for MongoDB using Change Streams.
//! Change Streams allow applications to access real-time data changes without
//! polling or tailing the oplog.

use super::config::{MongoDbCdcConfig, MongoFullDocumentMode};
use super::{
    CdcColumnValue, CdcEvent, CdcOperation, CdcSource, CdcSourceInfo, CdcSourceMetrics,
    CdcSourceStatus,
};
use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// MongoDB CDC source using Change Streams
pub struct MongoDbCdcSource {
    /// Configuration
    config: MongoDbCdcConfig,
    /// Current status
    status: RwLock<CdcSourceStatus>,
    /// Metrics
    metrics: RwLock<CdcSourceMetrics>,
    /// Running flag
    running: AtomicBool,
    /// Created timestamp
    created_at: DateTime<Utc>,
    /// Collection schemas (populated during initialization)
    collection_schemas: RwLock<HashMap<String, CollectionSchema>>,
    /// Shutdown signal sender
    shutdown_tx: RwLock<Option<tokio::sync::oneshot::Sender<()>>>,
    /// Current resume token
    resume_token: RwLock<Option<ResumeToken>>,
}

/// MongoDB resume token for change stream continuation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeToken {
    /// The resume token data (BSON document serialized)
    pub data: String,
    /// Timestamp when token was captured
    pub timestamp: DateTime<Utc>,
}

impl ResumeToken {
    /// Create a new resume token
    pub fn new(data: String) -> Self {
        Self {
            data,
            timestamp: Utc::now(),
        }
    }

    /// Parse from string
    pub fn from_string(s: &str) -> Option<Self> {
        serde_json::from_str(s).ok()
    }

    /// Convert to position string
    pub fn to_position_string(&self) -> String {
        self.data.clone()
    }
}

/// Schema information for a MongoDB collection
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct CollectionSchema {
    /// Database name
    database: String,
    /// Collection name
    collection: String,
    /// Discovered field names (from sampling)
    fields: Vec<FieldDef>,
    /// Document ID field name (usually "_id")
    id_field: String,
}

/// Field definition (discovered from documents)
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct FieldDef {
    /// Field name (dot notation for nested)
    name: String,
    /// Inferred BSON type
    bson_type: String,
}

/// MongoDB change event type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeEventType {
    /// Document inserted
    Insert,
    /// Document updated
    Update,
    /// Document replaced
    Replace,
    /// Document deleted
    Delete,
    /// Collection dropped
    Drop,
    /// Database dropped
    DropDatabase,
    /// Collection renamed
    Rename,
    /// Index invalidated
    Invalidate,
}

impl ChangeEventType {
    /// Parse from MongoDB operation type string
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "insert" => Some(Self::Insert),
            "update" => Some(Self::Update),
            "replace" => Some(Self::Replace),
            "delete" => Some(Self::Delete),
            "drop" => Some(Self::Drop),
            "dropDatabase" => Some(Self::DropDatabase),
            "rename" => Some(Self::Rename),
            "invalidate" => Some(Self::Invalidate),
            _ => None,
        }
    }

    /// Convert to CDC operation
    pub fn to_cdc_operation(self) -> CdcOperation {
        match self {
            Self::Insert => CdcOperation::Insert,
            Self::Update | Self::Replace => CdcOperation::Update,
            Self::Delete => CdcOperation::Delete,
            Self::Drop | Self::DropDatabase | Self::Rename => CdcOperation::Ddl,
            Self::Invalidate => CdcOperation::Commit,
        }
    }
}

/// Parsed change stream event
#[derive(Debug, Clone)]
pub struct ChangeStreamEvent {
    /// Event type
    pub operation_type: ChangeEventType,
    /// Database name
    pub database: String,
    /// Collection name
    pub collection: String,
    /// Document key (_id)
    pub document_key: Option<serde_json::Value>,
    /// Full document (for insert/replace/update with fullDocument option)
    pub full_document: Option<serde_json::Value>,
    /// Full document before change (if pre-image enabled)
    pub full_document_before_change: Option<serde_json::Value>,
    /// Update description (for update operations)
    pub update_description: Option<UpdateDescription>,
    /// Cluster time
    pub cluster_time: Option<DateTime<Utc>>,
    /// Resume token
    pub resume_token: String,
    /// Transaction number (if part of transaction)
    pub txn_number: Option<i64>,
}

/// Update description for update operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateDescription {
    /// Updated fields
    pub updated_fields: HashMap<String, serde_json::Value>,
    /// Removed fields
    pub removed_fields: Vec<String>,
    /// Truncated arrays (MongoDB 6.0+)
    pub truncated_arrays: Option<Vec<TruncatedArray>>,
}

/// Truncated array information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TruncatedArray {
    /// Field path
    pub field: String,
    /// New size
    pub new_size: i32,
}

#[allow(dead_code)]
impl MongoDbCdcSource {
    /// Create a new MongoDB CDC source
    pub fn new(config: MongoDbCdcConfig) -> Self {
        let resume_token = config
            .resume_token
            .as_ref()
            .and_then(|s| ResumeToken::from_string(s));

        Self {
            config,
            status: RwLock::new(CdcSourceStatus::Stopped),
            metrics: RwLock::new(CdcSourceMetrics::default()),
            running: AtomicBool::new(false),
            created_at: Utc::now(),
            collection_schemas: RwLock::new(HashMap::new()),
            shutdown_tx: RwLock::new(None),
            resume_token: RwLock::new(resume_token),
        }
    }

    /// Get full document mode as string for MongoDB driver
    pub fn full_document_mode_string(&self) -> &'static str {
        match self.config.full_document {
            MongoFullDocumentMode::UpdateLookup => "updateLookup",
            MongoFullDocumentMode::WhenAvailable => "whenAvailable",
            MongoFullDocumentMode::Required => "required",
            MongoFullDocumentMode::Off => "off",
        }
    }

    /// Discover collection schemas by sampling documents
    async fn discover_schemas(&self) -> Result<()> {
        info!("Discovering MongoDB collection schemas");

        let mut schemas = HashMap::new();

        // In a real implementation, this would:
        // 1. Connect to MongoDB
        // 2. List collections (or use configured list)
        // 3. Sample documents from each collection
        // 4. Infer field types

        for collection in &self.config.collections {
            let key = format!("{}.{}", self.config.database, collection);
            schemas.insert(
                key,
                CollectionSchema {
                    database: self.config.database.clone(),
                    collection: collection.clone(),
                    fields: Vec::new(),
                    id_field: "_id".to_string(),
                },
            );
        }

        // If no collections specified, we'd discover all
        if self.config.collections.is_empty() {
            // Would list all collections in the database
            debug!("No collections specified, will watch all collections in database");
        }

        *self.collection_schemas.write() = schemas;
        Ok(())
    }

    /// Perform initial snapshot of collections
    async fn perform_snapshot(&self, tx: &mpsc::Sender<CdcEvent>) -> Result<()> {
        if !self.config.base.snapshot_enabled {
            return Ok(());
        }

        *self.status.write() = CdcSourceStatus::Snapshotting;
        info!("Starting MongoDB initial snapshot");

        let schemas = self.collection_schemas.read().clone();
        for (key, schema) in schemas {
            info!("Snapshotting collection: {}", key);

            // In a real implementation:
            // 1. Start a change stream first to capture position
            // 2. Perform full collection scan
            // 3. Emit snapshot events

            let mut event = CdcEvent::new(
                self.config.base.name.clone(),
                schema.database.clone(),
                schema.collection.clone(),
                CdcOperation::Snapshot,
                "snapshot".to_string(),
            );
            event
                .metadata
                .insert("snapshot_type".to_string(), "initial".to_string());
            event.primary_key = vec![schema.id_field.clone()];

            self.metrics.write().record_event(&event);

            if tx.send(event).await.is_err() {
                warn!("Receiver dropped during snapshot");
                return Ok(());
            }
        }

        info!("MongoDB snapshot complete");
        Ok(())
    }

    /// Parse change stream event into CDC events
    fn parse_change_event(&self, event: &ChangeStreamEvent) -> Option<CdcEvent> {
        let operation = event.operation_type.to_cdc_operation();

        // Check if we should capture this collection
        if !self.should_capture_collection(&event.database, &event.collection) {
            return None;
        }

        let mut cdc_event = CdcEvent::new(
            self.config.base.name.clone(),
            event.database.clone(),
            event.collection.clone(),
            operation,
            event.resume_token.clone(),
        );

        if let Some(ref cluster_time) = event.cluster_time {
            cdc_event.timestamp = *cluster_time;
        }

        // Set document key as primary key
        if let Some(ref doc_key) = event.document_key {
            if let Some(id) = doc_key.get("_id") {
                cdc_event.primary_key = vec!["_id".to_string()];
                cdc_event.metadata.insert("_id".to_string(), id.to_string());
            }
        }

        // Handle different operation types
        match event.operation_type {
            ChangeEventType::Insert | ChangeEventType::Replace => {
                if let Some(ref full_doc) = event.full_document {
                    cdc_event.after = Some(self.document_to_columns(full_doc));
                }
            }
            ChangeEventType::Update => {
                if let Some(ref full_doc) = event.full_document {
                    cdc_event.after = Some(self.document_to_columns(full_doc));
                } else if let Some(ref update_desc) = event.update_description {
                    // Convert update description to columns
                    let mut columns = Vec::new();
                    for (field, value) in &update_desc.updated_fields {
                        columns.push(CdcColumnValue {
                            name: field.clone(),
                            data_type: infer_bson_type(value),
                            value: Some(value.clone()),
                        });
                    }
                    for field in &update_desc.removed_fields {
                        columns.push(CdcColumnValue {
                            name: field.clone(),
                            data_type: "null".to_string(),
                            value: None,
                        });
                    }
                    cdc_event.after = Some(columns);
                }

                // Include pre-image if available
                if let Some(ref before_doc) = event.full_document_before_change {
                    cdc_event.before = Some(self.document_to_columns(before_doc));
                }
            }
            ChangeEventType::Delete => {
                if let Some(ref before_doc) = event.full_document_before_change {
                    cdc_event.before = Some(self.document_to_columns(before_doc));
                } else if let Some(ref doc_key) = event.document_key {
                    // At minimum, include the document key
                    cdc_event.before = Some(self.document_to_columns(doc_key));
                }
            }
            _ => {}
        }

        // Add transaction info if present
        if let Some(txn) = event.txn_number {
            cdc_event.transaction_id = Some(txn.to_string());
        }

        Some(cdc_event)
    }

    /// Check if we should capture changes from this collection
    fn should_capture_collection(&self, database: &str, collection: &str) -> bool {
        // Use base CDC config filtering
        self.config.base.should_capture_table(database, collection)
    }

    /// Convert MongoDB document to CDC columns
    fn document_to_columns(&self, doc: &serde_json::Value) -> Vec<CdcColumnValue> {
        let mut columns = Vec::new();

        if let serde_json::Value::Object(map) = doc {
            for (key, value) in map {
                columns.push(CdcColumnValue {
                    name: key.clone(),
                    data_type: infer_bson_type(value),
                    value: if value.is_null() {
                        None
                    } else {
                        Some(value.clone())
                    },
                });
            }
        }

        columns
    }

    /// Stream changes using Change Streams
    async fn stream_changes(
        self: Arc<Self>,
        _tx: mpsc::Sender<CdcEvent>,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<()> {
        *self.status.write() = CdcSourceStatus::Running;
        info!("Starting MongoDB change stream");

        // In a real implementation:
        // 1. Connect to MongoDB
        // 2. Create change stream with options:
        //    - fullDocument: updateLookup/whenAvailable/required
        //    - fullDocumentBeforeChange: if pre-image enabled
        //    - resumeAfter: if we have a resume token
        // 3. Process change events

        let poll_interval = tokio::time::Duration::from_millis(self.config.max_await_time_ms);

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    info!("Shutdown signal received, stopping MongoDB CDC");
                    break;
                }
                _ = tokio::time::sleep(poll_interval) => {
                    // In a real implementation, we would process change stream events
                    debug!("MongoDB change stream polling...");
                }
            }
        }

        *self.status.write() = CdcSourceStatus::Stopped;
        Ok(())
    }

    /// Get current resume token
    pub fn current_resume_token(&self) -> Option<ResumeToken> {
        self.resume_token.read().clone()
    }

    /// Set resume token
    pub fn set_resume_token(&self, token: ResumeToken) {
        *self.resume_token.write() = Some(token);
    }
}

/// Infer BSON type from JSON value
#[allow(dead_code)]
fn infer_bson_type(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(_) => "bool".to_string(),
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "int64".to_string()
            } else {
                "double".to_string()
            }
        }
        serde_json::Value::String(_) => "string".to_string(),
        serde_json::Value::Array(_) => "array".to_string(),
        serde_json::Value::Object(map) => {
            // Check for special BSON types
            if map.contains_key("$oid") {
                "objectId".to_string()
            } else if map.contains_key("$date") {
                "date".to_string()
            } else if map.contains_key("$binary") {
                "binData".to_string()
            } else if map.contains_key("$regex") {
                "regex".to_string()
            } else if map.contains_key("$numberDecimal") {
                "decimal128".to_string()
            } else {
                "object".to_string()
            }
        }
    }
}

#[async_trait::async_trait]
impl CdcSource for MongoDbCdcSource {
    fn name(&self) -> &str {
        &self.config.base.name
    }

    fn source_type(&self) -> &str {
        "mongodb"
    }

    fn info(&self) -> CdcSourceInfo {
        let schemas = self.collection_schemas.read();
        let tables: Vec<String> = schemas.keys().cloned().collect();

        CdcSourceInfo {
            name: self.config.base.name.clone(),
            source_type: "mongodb".to_string(),
            database: self.config.database.clone(),
            tables,
            status: *self.status.read(),
            metrics: self.metrics.read().clone(),
            created_at: self.created_at,
        }
    }

    async fn start(&self) -> Result<mpsc::Receiver<CdcEvent>> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(StreamlineError::Config(
                "MongoDB CDC source is already running".to_string(),
            ));
        }

        *self.status.write() = CdcSourceStatus::Initializing;
        info!(
            "Starting MongoDB CDC source: {} -> {}",
            self.config.base.name, self.config.connection_uri
        );

        // Discover collection schemas
        self.discover_schemas().await?;

        // Create event channel
        let (tx, rx) = mpsc::channel(10000);

        // Perform snapshot if enabled
        self.perform_snapshot(&tx).await?;

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        *self.shutdown_tx.write() = Some(shutdown_tx);

        // Clone self for the streaming task
        let self_arc = Arc::new(Self {
            config: self.config.clone(),
            status: RwLock::new(*self.status.read()),
            metrics: RwLock::new(self.metrics.read().clone()),
            running: AtomicBool::new(true),
            created_at: self.created_at,
            collection_schemas: RwLock::new(self.collection_schemas.read().clone()),
            shutdown_tx: RwLock::new(None),
            resume_token: RwLock::new(self.resume_token.read().clone()),
        });

        tokio::spawn(async move {
            if let Err(e) = self_arc.stream_changes(tx, shutdown_rx).await {
                error!("MongoDB CDC streaming error: {}", e);
            }
        });

        Ok(rx)
    }

    async fn stop(&self) -> Result<()> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Ok(());
        }

        info!("Stopping MongoDB CDC source: {}", self.config.base.name);

        // Send shutdown signal
        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.send(());
        }

        *self.status.write() = CdcSourceStatus::Stopped;
        Ok(())
    }

    async fn pause(&self) -> Result<()> {
        *self.status.write() = CdcSourceStatus::Paused;
        Ok(())
    }

    async fn resume(&self) -> Result<()> {
        *self.status.write() = CdcSourceStatus::Running;
        Ok(())
    }

    fn status(&self) -> CdcSourceStatus {
        *self.status.read()
    }

    fn metrics(&self) -> CdcSourceMetrics {
        self.metrics.read().clone()
    }

    async fn commit(&self, position: &str) -> Result<()> {
        debug!("MongoDB CDC commit position: {}", position);
        self.metrics.write().last_position = Some(position.to_string());

        // Update resume token
        *self.resume_token.write() = Some(ResumeToken::new(position.to_string()));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mongodb_cdc_source_creation() {
        let config = MongoDbCdcConfig::default();
        let source = MongoDbCdcSource::new(config);
        assert_eq!(source.name(), "cdc-source");
        assert_eq!(source.source_type(), "mongodb");
    }

    #[test]
    fn test_change_event_type_parsing() {
        assert_eq!(
            ChangeEventType::parse("insert"),
            Some(ChangeEventType::Insert)
        );
        assert_eq!(
            ChangeEventType::parse("update"),
            Some(ChangeEventType::Update)
        );
        assert_eq!(
            ChangeEventType::parse("delete"),
            Some(ChangeEventType::Delete)
        );
        assert_eq!(ChangeEventType::parse("unknown"), None);
    }

    #[test]
    fn test_change_event_to_cdc_operation() {
        assert_eq!(
            ChangeEventType::Insert.to_cdc_operation(),
            CdcOperation::Insert
        );
        assert_eq!(
            ChangeEventType::Update.to_cdc_operation(),
            CdcOperation::Update
        );
        assert_eq!(
            ChangeEventType::Delete.to_cdc_operation(),
            CdcOperation::Delete
        );
    }

    #[test]
    fn test_resume_token() {
        let token = ResumeToken::new("test-token".to_string());
        assert_eq!(token.to_position_string(), "test-token");
    }

    #[test]
    fn test_mongodb_source_status() {
        let config = MongoDbCdcConfig::default();
        let source = MongoDbCdcSource::new(config);
        assert_eq!(source.status(), CdcSourceStatus::Stopped);
    }

    #[test]
    fn test_infer_bson_type() {
        assert_eq!(infer_bson_type(&serde_json::json!(null)), "null");
        assert_eq!(infer_bson_type(&serde_json::json!(true)), "bool");
        assert_eq!(infer_bson_type(&serde_json::json!(42)), "int64");
        assert_eq!(infer_bson_type(&serde_json::json!(3.5)), "double");
        assert_eq!(infer_bson_type(&serde_json::json!("hello")), "string");
        assert_eq!(infer_bson_type(&serde_json::json!([1, 2, 3])), "array");
        assert_eq!(infer_bson_type(&serde_json::json!({"a": 1})), "object");
        assert_eq!(
            infer_bson_type(&serde_json::json!({"$oid": "123"})),
            "objectId"
        );
    }

    #[test]
    fn test_full_document_mode() {
        let mut config = MongoDbCdcConfig::default();
        let source = MongoDbCdcSource::new(config.clone());
        assert_eq!(source.full_document_mode_string(), "updateLookup");

        config.full_document = MongoFullDocumentMode::Required;
        let source = MongoDbCdcSource::new(config);
        assert_eq!(source.full_document_mode_string(), "required");
    }
}
