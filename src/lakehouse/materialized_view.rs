//! Materialized Views
//!
//! Precomputed query results with incremental refresh support.

use super::config::{MaterializedViewConfig, RefreshMode};
use super::query_optimizer::{AggregateExpr, LogicalPlan, Predicate, QueryOptimizer, SortExpr};
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Materialized view manager
pub struct MaterializedViewManager {
    /// Configuration
    config: MaterializedViewConfig,
    /// Registered views
    views: Arc<RwLock<HashMap<String, MaterializedView>>>,
    /// Query optimizer
    #[allow(dead_code)]
    optimizer: Arc<QueryOptimizer>,
    /// Storage path
    storage_path: PathBuf,
}

impl MaterializedViewManager {
    /// Create a new materialized view manager
    pub fn new(config: MaterializedViewConfig, optimizer: Arc<QueryOptimizer>) -> Result<Self> {
        let storage_path = config.storage_path.clone();

        // Create storage directory if it doesn't exist
        if config.enabled {
            std::fs::create_dir_all(&storage_path).map_err(|e| {
                StreamlineError::storage_msg(format!(
                    "Failed to create materialized view directory: {}",
                    e
                ))
            })?;
        }

        Ok(Self {
            config,
            views: Arc::new(RwLock::new(HashMap::new())),
            optimizer,
            storage_path,
        })
    }

    /// Create a new materialized view
    pub async fn create_view(&self, definition: ViewDefinition) -> Result<MaterializedView> {
        if !self.config.enabled {
            return Err(StreamlineError::Config(
                "Materialized views are disabled".into(),
            ));
        }

        // Validate view definition
        self.validate_definition(&definition)?;

        let view = MaterializedView::new(definition, self.storage_path.clone())?;

        let mut views = self.views.write().await;
        if views.contains_key(&view.name) {
            return Err(StreamlineError::Config(format!(
                "View '{}' already exists",
                view.name
            )));
        }

        views.insert(view.name.clone(), view.clone());

        // Persist view metadata
        self.persist_view_metadata(&view)?;

        Ok(view)
    }

    /// Drop a materialized view
    pub async fn drop_view(&self, name: &str) -> Result<()> {
        let mut views = self.views.write().await;

        if let Some(view) = views.remove(name) {
            // Remove storage
            if view.storage_path.exists() {
                std::fs::remove_dir_all(&view.storage_path).map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to remove view storage: {}", e))
                })?;
            }

            // Remove metadata
            let metadata_path = self.storage_path.join(format!("{}.view.json", name));
            if metadata_path.exists() {
                std::fs::remove_file(&metadata_path).map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to remove view metadata: {}", e))
                })?;
            }

            Ok(())
        } else {
            Err(StreamlineError::Config(format!(
                "View '{}' not found",
                name
            )))
        }
    }

    /// Get a materialized view by name
    pub async fn get_view(&self, name: &str) -> Option<MaterializedView> {
        let views = self.views.read().await;
        views.get(name).cloned()
    }

    /// List all materialized views
    pub async fn list_views(&self) -> Vec<MaterializedView> {
        let views = self.views.read().await;
        views.values().cloned().collect()
    }

    /// Refresh a materialized view
    pub async fn refresh_view(&self, name: &str) -> Result<RefreshResult> {
        let views = self.views.read().await;

        let view = views
            .get(name)
            .ok_or_else(|| StreamlineError::Config(format!("View '{}' not found", name)))?;

        let start_time = std::time::Instant::now();

        // Determine refresh type
        let refresh_type = if self.config.incremental_enabled && view.supports_incremental() {
            RefreshType::Incremental
        } else {
            RefreshType::Full
        };

        // Execute refresh (placeholder - would integrate with query engine)
        let rows_affected = self.execute_refresh(view, &refresh_type).await?;

        let duration = start_time.elapsed();

        Ok(RefreshResult {
            view_name: name.to_string(),
            refresh_type,
            rows_affected,
            duration_ms: duration.as_millis() as u64,
            timestamp: chrono::Utc::now().timestamp(),
        })
    }

    /// Validate view definition
    fn validate_definition(&self, definition: &ViewDefinition) -> Result<()> {
        if definition.name.is_empty() {
            return Err(StreamlineError::Config("View name cannot be empty".into()));
        }

        if definition.source_topics.is_empty() {
            return Err(StreamlineError::Config(
                "View must have at least one source topic".into(),
            ));
        }

        if definition.columns.is_empty() {
            return Err(StreamlineError::Config(
                "View must have at least one column".into(),
            ));
        }

        Ok(())
    }

    /// Persist view metadata to disk
    fn persist_view_metadata(&self, view: &MaterializedView) -> Result<()> {
        let path = self.storage_path.join(format!("{}.view.json", view.name));
        let json = serde_json::to_string_pretty(view).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to serialize view metadata: {}", e))
        })?;

        std::fs::write(&path, json).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to write view metadata: {}", e))
        })?;

        Ok(())
    }

    /// Execute refresh for a view
    async fn execute_refresh(
        &self,
        view: &MaterializedView,
        refresh_type: &RefreshType,
    ) -> Result<u64> {
        // This would integrate with the query engine
        // For now, return a placeholder

        tracing::info!(
            view = %view.name,
            refresh_type = ?refresh_type,
            "Executing view refresh"
        );

        // Placeholder: would execute the view query and store results
        Ok(0)
    }

    /// Load views from storage
    pub async fn load_views(&self) -> Result<()> {
        if !self.storage_path.exists() {
            return Ok(());
        }

        let mut views = self.views.write().await;

        let entries = std::fs::read_dir(&self.storage_path).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to read views directory: {}", e))
        })?;

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "json")
                && path
                    .file_name()
                    .is_some_and(|n| n.to_string_lossy().ends_with(".view.json"))
            {
                let content = std::fs::read_to_string(&path).map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to read view metadata: {}", e))
                })?;

                let view: MaterializedView = serde_json::from_str(&content).map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to parse view metadata: {}", e))
                })?;

                views.insert(view.name.clone(), view);
            }
        }

        Ok(())
    }

    /// Get views that need refresh
    pub async fn get_stale_views(&self) -> Vec<String> {
        let views = self.views.read().await;
        let now = chrono::Utc::now().timestamp();

        views
            .values()
            .filter(|v| v.is_stale(now))
            .map(|v| v.name.clone())
            .collect()
    }
}

/// Materialized view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedView {
    /// View name
    pub name: String,
    /// View definition
    pub definition: ViewDefinition,
    /// Current status
    pub status: ViewStatus,
    /// Storage path for view data
    #[serde(skip)]
    pub storage_path: PathBuf,
    /// Last refresh timestamp
    pub last_refresh: Option<i64>,
    /// Last refresh duration in ms
    pub last_refresh_duration_ms: Option<u64>,
    /// Row count
    pub row_count: u64,
    /// Size in bytes
    pub size_bytes: u64,
    /// Creation timestamp
    pub created_at: i64,
    /// Watermark for incremental refresh
    pub watermark: Option<Watermark>,
}

impl MaterializedView {
    /// Create a new materialized view
    pub fn new(definition: ViewDefinition, base_path: PathBuf) -> Result<Self> {
        let storage_path = base_path.join(&definition.name);

        std::fs::create_dir_all(&storage_path).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to create view storage: {}", e))
        })?;

        Ok(Self {
            name: definition.name.clone(),
            definition,
            status: ViewStatus::Created,
            storage_path,
            last_refresh: None,
            last_refresh_duration_ms: None,
            row_count: 0,
            size_bytes: 0,
            created_at: chrono::Utc::now().timestamp(),
            watermark: None,
        })
    }

    /// Check if the view supports incremental refresh
    pub fn supports_incremental(&self) -> bool {
        // Views with append-only aggregations support incremental refresh
        matches!(
            self.definition.refresh_mode,
            RefreshMode::Periodic { .. } | RefreshMode::OnQuery { .. }
        ) && self.definition.aggregates.iter().all(|agg| {
            matches!(
                agg.function.to_uppercase().as_str(),
                "COUNT" | "SUM" | "MIN" | "MAX"
            )
        })
    }

    /// Check if view is stale and needs refresh
    pub fn is_stale(&self, now: i64) -> bool {
        match &self.definition.refresh_mode {
            RefreshMode::Immediate => true,
            RefreshMode::Periodic { interval_secs } => {
                if let Some(last) = self.last_refresh {
                    now - last > *interval_secs as i64
                } else {
                    true
                }
            }
            RefreshMode::Manual => false,
            RefreshMode::OnQuery {
                staleness_threshold_secs,
            } => {
                if let Some(last) = self.last_refresh {
                    now - last > *staleness_threshold_secs as i64
                } else {
                    true
                }
            }
        }
    }

    /// Get the logical plan for this view
    pub fn to_logical_plan(&self) -> LogicalPlan {
        // Start with a scan
        let mut plan = LogicalPlan::Scan {
            source: self
                .definition
                .source_topics
                .first()
                .cloned()
                .unwrap_or_default(),
            schema: self
                .definition
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect(),
            existing_predicate: self.definition.filter.clone(),
        };

        // Add projection
        if !self.definition.columns.is_empty() {
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                columns: self
                    .definition
                    .columns
                    .iter()
                    .map(|c| c.name.clone())
                    .collect(),
            };
        }

        // Add aggregation
        if !self.definition.aggregates.is_empty() {
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by: self.definition.group_by.clone(),
                aggregates: self.definition.aggregates.clone(),
            };
        }

        // Add sort
        if !self.definition.order_by.is_empty() {
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: self.definition.order_by.clone(),
            };
        }

        // Add limit
        if let Some(limit) = self.definition.limit {
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit,
            };
        }

        plan
    }
}

/// View definition
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ViewDefinition {
    /// View name
    pub name: String,
    /// Source topics
    pub source_topics: Vec<String>,
    /// Output columns
    pub columns: Vec<ColumnDefinition>,
    /// Filter predicate
    pub filter: Option<Predicate>,
    /// Group by columns
    pub group_by: Vec<String>,
    /// Aggregate expressions
    pub aggregates: Vec<AggregateExpr>,
    /// Order by expressions
    pub order_by: Vec<SortExpr>,
    /// Limit
    pub limit: Option<usize>,
    /// Refresh mode
    pub refresh_mode: RefreshMode,
    /// Retention policy
    pub retention: Option<RetentionPolicy>,
    /// Description
    pub description: Option<String>,
}

/// Column definition in a view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: String,
    /// Source expression
    pub expression: Option<String>,
    /// Whether column is nullable
    pub nullable: bool,
}

/// View status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ViewStatus {
    /// View created but not yet refreshed
    Created,
    /// View is being refreshed
    Refreshing,
    /// View is ready
    Ready,
    /// View refresh failed
    Failed,
    /// View is disabled
    Disabled,
}

/// Refresh result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshResult {
    /// View name
    pub view_name: String,
    /// Refresh type
    pub refresh_type: RefreshType,
    /// Rows affected
    pub rows_affected: u64,
    /// Duration in milliseconds
    pub duration_ms: u64,
    /// Timestamp
    pub timestamp: i64,
}

/// Type of refresh
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefreshType {
    /// Full refresh - recompute entire view
    Full,
    /// Incremental refresh - only process new data
    Incremental,
}

/// Watermark for incremental refresh
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Watermark {
    /// Partition watermarks
    pub partitions: HashMap<i32, i64>,
    /// Timestamp watermark
    pub timestamp: Option<i64>,
}

/// Retention policy for view data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Maximum age in seconds
    pub max_age_secs: Option<u64>,
    /// Maximum row count
    pub max_rows: Option<u64>,
    /// Maximum size in bytes
    pub max_bytes: Option<u64>,
}

/// View builder for fluent API
pub struct ViewBuilder {
    definition: ViewDefinition,
}

impl ViewBuilder {
    /// Create a new view builder
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            definition: ViewDefinition {
                name: name.into(),
                ..Default::default()
            },
        }
    }

    /// Set source topics
    pub fn from_topics(mut self, topics: Vec<String>) -> Self {
        self.definition.source_topics = topics;
        self
    }

    /// Add a column
    pub fn column(mut self, name: impl Into<String>, data_type: impl Into<String>) -> Self {
        self.definition.columns.push(ColumnDefinition {
            name: name.into(),
            data_type: data_type.into(),
            expression: None,
            nullable: true,
        });
        self
    }

    /// Set filter predicate
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.definition.filter = Some(predicate);
        self
    }

    /// Set group by columns
    pub fn group_by(mut self, columns: Vec<String>) -> Self {
        self.definition.group_by = columns;
        self
    }

    /// Add an aggregate
    pub fn aggregate(
        mut self,
        function: impl Into<String>,
        column: Option<String>,
        alias: impl Into<String>,
    ) -> Self {
        self.definition.aggregates.push(AggregateExpr {
            function: function.into(),
            column,
            distinct: false,
            alias: alias.into(),
        });
        self
    }

    /// Set order by
    pub fn order_by(mut self, column: impl Into<String>, ascending: bool) -> Self {
        self.definition.order_by.push(SortExpr {
            column: column.into(),
            ascending,
            nulls_first: false,
        });
        self
    }

    /// Set limit
    pub fn limit(mut self, limit: usize) -> Self {
        self.definition.limit = Some(limit);
        self
    }

    /// Set refresh mode
    pub fn refresh_mode(mut self, mode: RefreshMode) -> Self {
        self.definition.refresh_mode = mode;
        self
    }

    /// Set description
    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.definition.description = Some(desc.into());
        self
    }

    /// Build the view definition
    pub fn build(self) -> ViewDefinition {
        self.definition
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_view_builder() {
        let definition = ViewBuilder::new("hourly_sales")
            .from_topics(vec!["sales".to_string()])
            .column("hour", "timestamp")
            .column("product_id", "string")
            .column("total_sales", "decimal")
            .group_by(vec!["hour".to_string(), "product_id".to_string()])
            .aggregate("SUM", Some("amount".to_string()), "total_sales")
            .aggregate("COUNT", None, "transaction_count")
            .refresh_mode(RefreshMode::Periodic { interval_secs: 300 })
            .build();

        assert_eq!(definition.name, "hourly_sales");
        assert_eq!(definition.source_topics, vec!["sales"]);
        assert_eq!(definition.columns.len(), 3);
        assert_eq!(definition.aggregates.len(), 2);
    }

    #[test]
    fn test_view_staleness() {
        let definition = ViewDefinition {
            name: "test".to_string(),
            source_topics: vec!["test_topic".to_string()],
            columns: vec![ColumnDefinition {
                name: "col".to_string(),
                data_type: "string".to_string(),
                expression: None,
                nullable: true,
            }],
            refresh_mode: RefreshMode::Periodic { interval_secs: 60 },
            ..Default::default()
        };

        let mut view = MaterializedView {
            name: "test".to_string(),
            definition,
            status: ViewStatus::Ready,
            storage_path: PathBuf::from("/tmp/test"),
            last_refresh: Some(0),
            last_refresh_duration_ms: None,
            row_count: 0,
            size_bytes: 0,
            created_at: 0,
            watermark: None,
        };

        // Should be stale after 60 seconds
        assert!(view.is_stale(61));

        // Should not be stale before 60 seconds
        view.last_refresh = Some(30);
        assert!(!view.is_stale(60));
    }
}
