//! Native Iceberg REST Catalog
//!
//! Provides an in-process implementation of the Apache Iceberg REST catalog
//! specification, managing namespaces, tables, schemas, partitions, and
//! snapshots for lakehouse integration.
//!
//! Features:
//! - Hierarchical namespace management
//! - Table creation with schema and partition specs
//! - Snapshot lifecycle with configurable retention
//! - Automatic topic-to-table registration
//! - Atomic statistics via atomics

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// In-process Iceberg REST catalog.
pub struct IcebergCatalog {
    namespaces: Arc<RwLock<HashMap<String, IcebergNamespace>>>,
    tables: Arc<RwLock<HashMap<String, IcebergTableMetadata>>>,
    config: IcebergCatalogConfig,
    stats: Arc<CatalogStats>,
}

/// Configuration for the Iceberg catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergCatalogConfig {
    /// Base warehouse location URI.
    pub warehouse_location: String,
    /// Default file format for new tables.
    pub default_file_format: String,
    /// Automatically register Streamline topics as Iceberg tables.
    pub auto_register_topics: bool,
    /// Number of snapshots to retain per table.
    pub snapshot_retention_count: usize,
}

impl Default for IcebergCatalogConfig {
    fn default() -> Self {
        Self {
            warehouse_location: "s3://streamline-warehouse/".into(),
            default_file_format: "parquet".into(),
            auto_register_topics: false,
            snapshot_retention_count: 10,
        }
    }
}

/// An Iceberg namespace (database / schema).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergNamespace {
    /// Hierarchical name segments (e.g. `["production", "events"]`).
    pub name: Vec<String>,
    /// Arbitrary properties.
    pub properties: HashMap<String, String>,
    /// ISO-8601 creation timestamp.
    pub created_at: String,
}

/// Full metadata for an Iceberg table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergTableMetadata {
    pub table_id: String,
    pub namespace: Vec<String>,
    pub name: String,
    pub location: String,
    pub schema: IcebergSchema,
    pub partition_spec: Vec<PartitionField>,
    pub properties: HashMap<String, String>,
    pub snapshots: Vec<IcebergSnapshot>,
    pub current_snapshot_id: Option<i64>,
    pub created_at: String,
    pub updated_at: String,
}

/// Schema for an Iceberg table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergSchema {
    pub schema_id: i32,
    pub fields: Vec<IcebergField>,
}

/// A single field in an Iceberg schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergField {
    pub id: i32,
    pub name: String,
    /// Logical type: `"string"`, `"long"`, `"double"`, `"timestamp"`, etc.
    pub field_type: String,
    pub required: bool,
    pub doc: Option<String>,
}

/// A partition field specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionField {
    pub source_id: i32,
    pub field_id: i32,
    pub name: String,
    /// Transform function: `"identity"`, `"year"`, `"month"`, `"day"`, `"hour"`, `"bucket[N]"`.
    pub transform: String,
}

/// An Iceberg table snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergSnapshot {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub timestamp_ms: u64,
    pub manifest_list: String,
    pub summary: HashMap<String, String>,
}

/// Atomic statistics for catalog operations.
pub struct CatalogStats {
    pub namespaces_created: AtomicU64,
    pub tables_created: AtomicU64,
    pub snapshots_added: AtomicU64,
    pub queries_served: AtomicU64,
}

impl Default for CatalogStats {
    fn default() -> Self {
        Self {
            namespaces_created: AtomicU64::new(0),
            tables_created: AtomicU64::new(0),
            snapshots_added: AtomicU64::new(0),
            queries_served: AtomicU64::new(0),
        }
    }
}

/// Serialisable snapshot of [`CatalogStats`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogStatsSnapshot {
    pub namespaces_created: u64,
    pub tables_created: u64,
    pub snapshots_added: u64,
    pub queries_served: u64,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Canonical key for a namespace: segments joined by `"."`.
fn ns_key(name: &[String]) -> String {
    name.join(".")
}

/// Canonical key for a table: `namespace.table_name`.
fn table_key(namespace: &[String], table_name: &str) -> String {
    format!("{}.{}", ns_key(namespace), table_name)
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl IcebergCatalog {
    /// Create a new catalog with the given configuration.
    pub fn new(config: IcebergCatalogConfig) -> Self {
        info!(
            warehouse = %config.warehouse_location,
            auto_register = config.auto_register_topics,
            "Initialising Iceberg catalog"
        );
        Self {
            namespaces: Arc::new(RwLock::new(HashMap::new())),
            tables: Arc::new(RwLock::new(HashMap::new())),
            config,
            stats: Arc::new(CatalogStats::default()),
        }
    }

    /// Create a namespace. Fails if it already exists.
    pub async fn create_namespace(
        &self,
        name: Vec<String>,
        properties: HashMap<String, String>,
    ) -> Result<IcebergNamespace> {
        if name.is_empty() {
            return Err(StreamlineError::Sink(
                "Namespace name must not be empty".into(),
            ));
        }
        for segment in &name {
            if segment.is_empty() {
                return Err(StreamlineError::Sink(
                    "Namespace name segments must not be empty".into(),
                ));
            }
        }

        let key = ns_key(&name);
        let mut nss = self.namespaces.write().await;
        if nss.contains_key(&key) {
            return Err(StreamlineError::Sink(format!(
                "Namespace '{}' already exists",
                key
            )));
        }

        let ns = IcebergNamespace {
            name: name.clone(),
            properties,
            created_at: chrono::Utc::now().to_rfc3339(),
        };
        nss.insert(key.clone(), ns.clone());
        self.stats.namespaces_created.fetch_add(1, Ordering::Relaxed);
        info!(namespace = %key, "Namespace created");
        Ok(ns)
    }

    /// Retrieve a namespace by name.
    pub async fn get_namespace(&self, name: &[String]) -> Result<IcebergNamespace> {
        let key = ns_key(name);
        self.stats.queries_served.fetch_add(1, Ordering::Relaxed);
        self.namespaces
            .read()
            .await
            .get(&key)
            .cloned()
            .ok_or_else(|| StreamlineError::Sink(format!("Namespace '{}' not found", key)))
    }

    /// List all namespaces.
    pub async fn list_namespaces(&self) -> Vec<IcebergNamespace> {
        self.stats.queries_served.fetch_add(1, Ordering::Relaxed);
        self.namespaces.read().await.values().cloned().collect()
    }

    /// Drop a namespace. Fails if it contains tables.
    pub async fn drop_namespace(&self, name: &[String]) -> Result<()> {
        let key = ns_key(name);

        // Ensure no tables belong to this namespace
        let tables = self.tables.read().await;
        let has_tables = tables.values().any(|t| ns_key(&t.namespace) == key);
        if has_tables {
            return Err(StreamlineError::Sink(format!(
                "Namespace '{}' is not empty; drop all tables first",
                key
            )));
        }
        drop(tables);

        let mut nss = self.namespaces.write().await;
        nss.remove(&key)
            .ok_or_else(|| StreamlineError::Sink(format!("Namespace '{}' not found", key)))?;
        warn!(namespace = %key, "Namespace dropped");
        Ok(())
    }

    /// Create a table within an existing namespace.
    pub async fn create_table(
        &self,
        namespace: Vec<String>,
        name: String,
        schema: IcebergSchema,
        partition_spec: Vec<PartitionField>,
        properties: HashMap<String, String>,
    ) -> Result<IcebergTableMetadata> {
        // Validate namespace exists
        let ns_k = ns_key(&namespace);
        {
            let nss = self.namespaces.read().await;
            if !nss.contains_key(&ns_k) {
                return Err(StreamlineError::Sink(format!(
                    "Namespace '{}' not found",
                    ns_k
                )));
            }
        }

        if name.is_empty() {
            return Err(StreamlineError::Sink("Table name must not be empty".into()));
        }

        let tk = table_key(&namespace, &name);
        let mut tables = self.tables.write().await;
        if tables.contains_key(&tk) {
            return Err(StreamlineError::Sink(format!(
                "Table '{}' already exists",
                tk
            )));
        }

        let now = chrono::Utc::now().to_rfc3339();
        let location = format!(
            "{}{}/{}",
            self.config.warehouse_location, ns_k, name
        );

        let table = IcebergTableMetadata {
            table_id: Uuid::new_v4().to_string(),
            namespace,
            name: name.clone(),
            location,
            schema,
            partition_spec,
            properties,
            snapshots: Vec::new(),
            current_snapshot_id: None,
            created_at: now.clone(),
            updated_at: now,
        };

        tables.insert(tk.clone(), table.clone());
        self.stats.tables_created.fetch_add(1, Ordering::Relaxed);
        info!(table = %tk, "Table created");
        Ok(table)
    }

    /// Retrieve table metadata.
    pub async fn get_table(
        &self,
        namespace: &[String],
        name: &str,
    ) -> Result<IcebergTableMetadata> {
        let tk = table_key(namespace, name);
        self.stats.queries_served.fetch_add(1, Ordering::Relaxed);
        self.tables
            .read()
            .await
            .get(&tk)
            .cloned()
            .ok_or_else(|| StreamlineError::Sink(format!("Table '{}' not found", tk)))
    }

    /// List tables in a namespace.
    pub async fn list_tables(&self, namespace: &[String]) -> Vec<IcebergTableMetadata> {
        let ns_k = ns_key(namespace);
        self.stats.queries_served.fetch_add(1, Ordering::Relaxed);
        self.tables
            .read()
            .await
            .values()
            .filter(|t| ns_key(&t.namespace) == ns_k)
            .cloned()
            .collect()
    }

    /// Drop a table.
    pub async fn drop_table(&self, namespace: &[String], name: &str) -> Result<()> {
        let tk = table_key(namespace, name);
        let mut tables = self.tables.write().await;
        tables
            .remove(&tk)
            .ok_or_else(|| StreamlineError::Sink(format!("Table '{}' not found", tk)))?;
        warn!(table = %tk, "Table dropped");
        Ok(())
    }

    /// Append a snapshot to a table, applying retention policy.
    pub async fn add_snapshot(
        &self,
        namespace: &[String],
        table_name: &str,
        snapshot: IcebergSnapshot,
    ) -> Result<()> {
        let tk = table_key(namespace, table_name);
        let mut tables = self.tables.write().await;
        let table = tables
            .get_mut(&tk)
            .ok_or_else(|| StreamlineError::Sink(format!("Table '{}' not found", tk)))?;

        let sid = snapshot.snapshot_id;
        table.current_snapshot_id = Some(sid);
        table.snapshots.push(snapshot);
        table.updated_at = chrono::Utc::now().to_rfc3339();

        // Enforce retention
        let max = self.config.snapshot_retention_count;
        if table.snapshots.len() > max {
            let drain = table.snapshots.len() - max;
            table.snapshots.drain(..drain);
            debug!(table = %tk, removed = drain, "Snapshot retention applied");
        }

        self.stats.snapshots_added.fetch_add(1, Ordering::Relaxed);
        debug!(table = %tk, snapshot_id = sid, "Snapshot added");
        Ok(())
    }

    /// Convenience helper: create a namespace (if needed) and a table for a
    /// Streamline topic with a default Kafka-record schema.
    pub async fn register_topic_as_table(
        &self,
        topic: &str,
        namespace: Vec<String>,
    ) -> Result<IcebergTableMetadata> {
        // Ensure namespace exists
        let ns_k = ns_key(&namespace);
        {
            let nss = self.namespaces.read().await;
            if !nss.contains_key(&ns_k) {
                drop(nss);
                self.create_namespace(namespace.clone(), HashMap::new())
                    .await?;
            }
        }

        let schema = IcebergSchema {
            schema_id: 0,
            fields: vec![
                IcebergField {
                    id: 1,
                    name: "offset".into(),
                    field_type: "long".into(),
                    required: true,
                    doc: Some("Kafka offset".into()),
                },
                IcebergField {
                    id: 2,
                    name: "timestamp".into(),
                    field_type: "timestamp".into(),
                    required: true,
                    doc: Some("Record timestamp".into()),
                },
                IcebergField {
                    id: 3,
                    name: "key".into(),
                    field_type: "string".into(),
                    required: false,
                    doc: Some("Record key".into()),
                },
                IcebergField {
                    id: 4,
                    name: "value".into(),
                    field_type: "string".into(),
                    required: false,
                    doc: Some("Record value".into()),
                },
            ],
        };

        let partition_spec = vec![PartitionField {
            source_id: 2,
            field_id: 1000,
            name: "ts_hour".into(),
            transform: "hour".into(),
        }];

        let mut props = HashMap::new();
        props.insert("streamline.topic".into(), topic.into());
        props.insert(
            "write.format.default".into(),
            self.config.default_file_format.clone(),
        );

        self.create_table(namespace, topic.into(), schema, partition_spec, props)
            .await
    }

    /// Return an atomic stats snapshot.
    pub fn stats(&self) -> CatalogStatsSnapshot {
        CatalogStatsSnapshot {
            namespaces_created: self.stats.namespaces_created.load(Ordering::Relaxed),
            tables_created: self.stats.tables_created.load(Ordering::Relaxed),
            snapshots_added: self.stats.snapshots_added.load(Ordering::Relaxed),
            queries_served: self.stats.queries_served.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_catalog() -> IcebergCatalog {
        IcebergCatalog::new(IcebergCatalogConfig::default())
    }

    fn simple_schema() -> IcebergSchema {
        IcebergSchema {
            schema_id: 0,
            fields: vec![IcebergField {
                id: 1,
                name: "id".into(),
                field_type: "long".into(),
                required: true,
                doc: None,
            }],
        }
    }

    #[test]
    fn test_default_config() {
        let cfg = IcebergCatalogConfig::default();
        assert_eq!(cfg.warehouse_location, "s3://streamline-warehouse/");
        assert_eq!(cfg.default_file_format, "parquet");
        assert!(!cfg.auto_register_topics);
        assert_eq!(cfg.snapshot_retention_count, 10);
    }

    #[tokio::test]
    async fn test_create_namespace() {
        let catalog = default_catalog();
        let ns = catalog
            .create_namespace(vec!["default".into()], HashMap::new())
            .await
            .unwrap();
        assert_eq!(ns.name, vec!["default"]);
    }

    #[tokio::test]
    async fn test_create_namespace_duplicate() {
        let catalog = default_catalog();
        catalog
            .create_namespace(vec!["default".into()], HashMap::new())
            .await
            .unwrap();
        assert!(catalog
            .create_namespace(vec!["default".into()], HashMap::new())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_create_namespace_empty_name() {
        let catalog = default_catalog();
        assert!(catalog
            .create_namespace(vec![], HashMap::new())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_create_namespace_empty_segment() {
        let catalog = default_catalog();
        assert!(catalog
            .create_namespace(vec!["".into()], HashMap::new())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_get_namespace() {
        let catalog = default_catalog();
        catalog
            .create_namespace(vec!["prod".into()], HashMap::new())
            .await
            .unwrap();
        let ns = catalog.get_namespace(&["prod".into()]).await.unwrap();
        assert_eq!(ns.name, vec!["prod"]);
    }

    #[tokio::test]
    async fn test_get_namespace_not_found() {
        let catalog = default_catalog();
        assert!(catalog.get_namespace(&["nope".into()]).await.is_err());
    }

    #[tokio::test]
    async fn test_list_namespaces() {
        let catalog = default_catalog();
        catalog
            .create_namespace(vec!["a".into()], HashMap::new())
            .await
            .unwrap();
        catalog
            .create_namespace(vec!["b".into()], HashMap::new())
            .await
            .unwrap();
        assert_eq!(catalog.list_namespaces().await.len(), 2);
    }

    #[tokio::test]
    async fn test_drop_namespace() {
        let catalog = default_catalog();
        catalog
            .create_namespace(vec!["temp".into()], HashMap::new())
            .await
            .unwrap();
        catalog.drop_namespace(&["temp".into()]).await.unwrap();
        assert!(catalog.get_namespace(&["temp".into()]).await.is_err());
    }

    #[tokio::test]
    async fn test_drop_namespace_not_empty() {
        let catalog = default_catalog();
        catalog
            .create_namespace(vec!["ns".into()], HashMap::new())
            .await
            .unwrap();
        catalog
            .create_table(
                vec!["ns".into()],
                "t".into(),
                simple_schema(),
                vec![],
                HashMap::new(),
            )
            .await
            .unwrap();
        assert!(catalog.drop_namespace(&["ns".into()]).await.is_err());
    }

    #[tokio::test]
    async fn test_create_table() {
        let catalog = default_catalog();
        catalog
            .create_namespace(vec!["db".into()], HashMap::new())
            .await
            .unwrap();
        let table = catalog
            .create_table(
                vec!["db".into()],
                "events".into(),
                simple_schema(),
                vec![],
                HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(table.name, "events");
        assert!(table.location.contains("db/events"));
    }

    #[tokio::test]
    async fn test_create_table_no_namespace() {
        let catalog = default_catalog();
        assert!(catalog
            .create_table(
                vec!["nope".into()],
                "t".into(),
                simple_schema(),
                vec![],
                HashMap::new(),
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_create_table_duplicate() {
        let catalog = default_catalog();
        catalog
            .create_namespace(vec!["db".into()], HashMap::new())
            .await
            .unwrap();
        catalog
            .create_table(
                vec!["db".into()],
                "t".into(),
                simple_schema(),
                vec![],
                HashMap::new(),
            )
            .await
            .unwrap();
        assert!(catalog
            .create_table(
                vec!["db".into()],
                "t".into(),
                simple_schema(),
                vec![],
                HashMap::new(),
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_create_table_empty_name() {
        let catalog = default_catalog();
        catalog
            .create_namespace(vec!["db".into()], HashMap::new())
            .await
            .unwrap();
        assert!(catalog
            .create_table(
                vec!["db".into()],
                "".into(),
                simple_schema(),
                vec![],
                HashMap::new(),
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_get_table() {
        let catalog = default_catalog();
        catalog
            .create_namespace(vec!["db".into()], HashMap::new())
            .await
            .unwrap();
        catalog
            .create_table(
                vec!["db".into()],
                "t".into(),
                simple_schema(),
                vec![],
                HashMap::new(),
            )
            .await
            .unwrap();
        let t = catalog.get_table(&["db".into()], "t").await.unwrap();
        assert_eq!(t.name, "t");
    }

    #[tokio::test]
    async fn test_get_table_not_found() {
        let catalog = default_catalog();
        assert!(catalog.get_table(&["db".into()], "t").await.is_err());
    }

    #[tokio::test]
    async fn test_list_tables() {
        let catalog = default_catalog();
        catalog
            .create_namespace(vec!["db".into()], HashMap::new())
            .await
            .unwrap();
        catalog
            .create_table(
                vec!["db".into()],
                "a".into(),
                simple_schema(),
                vec![],
                HashMap::new(),
            )
            .await
            .unwrap();
        catalog
            .create_table(
                vec!["db".into()],
                "b".into(),
                simple_schema(),
                vec![],
                HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(catalog.list_tables(&["db".into()]).await.len(), 2);
    }

    #[tokio::test]
    async fn test_drop_table() {
        let catalog = default_catalog();
        catalog
            .create_namespace(vec!["db".into()], HashMap::new())
            .await
            .unwrap();
        catalog
            .create_table(
                vec!["db".into()],
                "t".into(),
                simple_schema(),
                vec![],
                HashMap::new(),
            )
            .await
            .unwrap();
        catalog.drop_table(&["db".into()], "t").await.unwrap();
        assert!(catalog.get_table(&["db".into()], "t").await.is_err());
    }

    #[tokio::test]
    async fn test_add_snapshot() {
        let catalog = default_catalog();
        catalog
            .create_namespace(vec!["db".into()], HashMap::new())
            .await
            .unwrap();
        catalog
            .create_table(
                vec!["db".into()],
                "t".into(),
                simple_schema(),
                vec![],
                HashMap::new(),
            )
            .await
            .unwrap();

        let snap = IcebergSnapshot {
            snapshot_id: 1,
            parent_snapshot_id: None,
            timestamp_ms: 1000,
            manifest_list: "s3://manifests/1".into(),
            summary: HashMap::new(),
        };
        catalog
            .add_snapshot(&["db".into()], "t", snap)
            .await
            .unwrap();

        let t = catalog.get_table(&["db".into()], "t").await.unwrap();
        assert_eq!(t.current_snapshot_id, Some(1));
        assert_eq!(t.snapshots.len(), 1);
    }

    #[tokio::test]
    async fn test_snapshot_retention() {
        let config = IcebergCatalogConfig {
            snapshot_retention_count: 3,
            ..Default::default()
        };
        let catalog = IcebergCatalog::new(config);
        catalog
            .create_namespace(vec!["db".into()], HashMap::new())
            .await
            .unwrap();
        catalog
            .create_table(
                vec!["db".into()],
                "t".into(),
                simple_schema(),
                vec![],
                HashMap::new(),
            )
            .await
            .unwrap();

        for i in 0..5 {
            let snap = IcebergSnapshot {
                snapshot_id: i,
                parent_snapshot_id: if i > 0 { Some(i - 1) } else { None },
                timestamp_ms: i as u64 * 1000,
                manifest_list: format!("s3://manifests/{}", i),
                summary: HashMap::new(),
            };
            catalog
                .add_snapshot(&["db".into()], "t", snap)
                .await
                .unwrap();
        }

        let t = catalog.get_table(&["db".into()], "t").await.unwrap();
        assert_eq!(t.snapshots.len(), 3);
        assert_eq!(t.snapshots[0].snapshot_id, 2);
    }

    #[tokio::test]
    async fn test_register_topic_as_table() {
        let catalog = default_catalog();
        let table = catalog
            .register_topic_as_table("events", vec!["default".into()])
            .await
            .unwrap();
        assert_eq!(table.name, "events");
        assert_eq!(table.schema.fields.len(), 4);
        assert!(table.properties.contains_key("streamline.topic"));
    }

    #[tokio::test]
    async fn test_register_topic_creates_namespace() {
        let catalog = default_catalog();
        catalog
            .register_topic_as_table("orders", vec!["auto_ns".into()])
            .await
            .unwrap();
        let ns = catalog.get_namespace(&["auto_ns".into()]).await.unwrap();
        assert_eq!(ns.name, vec!["auto_ns"]);
    }

    #[tokio::test]
    async fn test_stats_initial() {
        let catalog = default_catalog();
        let s = catalog.stats();
        assert_eq!(s.namespaces_created, 0);
        assert_eq!(s.tables_created, 0);
        assert_eq!(s.snapshots_added, 0);
        assert_eq!(s.queries_served, 0);
    }

    #[tokio::test]
    async fn test_stats_after_operations() {
        let catalog = default_catalog();
        catalog
            .create_namespace(vec!["ns".into()], HashMap::new())
            .await
            .unwrap();
        catalog
            .create_table(
                vec!["ns".into()],
                "t".into(),
                simple_schema(),
                vec![],
                HashMap::new(),
            )
            .await
            .unwrap();
        catalog.get_table(&["ns".into()], "t").await.unwrap();

        let s = catalog.stats();
        assert_eq!(s.namespaces_created, 1);
        assert_eq!(s.tables_created, 1);
        assert_eq!(s.queries_served, 1);
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let cfg = IcebergCatalogConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let back: IcebergCatalogConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.warehouse_location, cfg.warehouse_location);
        assert_eq!(back.snapshot_retention_count, cfg.snapshot_retention_count);
    }

    #[test]
    fn test_schema_serde_roundtrip() {
        let schema = simple_schema();
        let json = serde_json::to_string(&schema).unwrap();
        let back: IcebergSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(back.fields.len(), 1);
        assert_eq!(back.fields[0].name, "id");
    }

    #[tokio::test]
    async fn test_hierarchical_namespace() {
        let catalog = default_catalog();
        catalog
            .create_namespace(vec!["org".into(), "team".into()], HashMap::new())
            .await
            .unwrap();
        let ns = catalog
            .get_namespace(&["org".into(), "team".into()])
            .await
            .unwrap();
        assert_eq!(ns.name, vec!["org", "team"]);
    }

    #[tokio::test]
    async fn test_drop_nonexistent_namespace() {
        let catalog = default_catalog();
        assert!(catalog.drop_namespace(&["nope".into()]).await.is_err());
    }

    #[tokio::test]
    async fn test_drop_nonexistent_table() {
        let catalog = default_catalog();
        assert!(catalog.drop_table(&["db".into()], "t").await.is_err());
    }

    #[tokio::test]
    async fn test_add_snapshot_table_not_found() {
        let catalog = default_catalog();
        let snap = IcebergSnapshot {
            snapshot_id: 1,
            parent_snapshot_id: None,
            timestamp_ms: 1000,
            manifest_list: "s3://m/1".into(),
            summary: HashMap::new(),
        };
        assert!(catalog
            .add_snapshot(&["db".into()], "t", snap)
            .await
            .is_err());
    }
}
