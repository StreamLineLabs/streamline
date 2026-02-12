//! Materialized Views for StreamlineQL
//!
//! Continuous queries that maintain incrementally-updated materialized views
//! over streaming data. Views are queryable via point lookups and range scans.
//!
//! ## Example
//!
//! ```sql
//! CREATE MATERIALIZED VIEW user_stats AS
//! SELECT
//!     user_id,
//!     COUNT(*) as event_count,
//!     SUM(amount) as total_amount,
//!     MAX(timestamp) as last_seen
//! FROM events
//! WINDOW TUMBLING(INTERVAL '1 hour')
//! GROUP BY user_id;
//! ```

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Materialized view definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedViewDef {
    /// View name (unique identifier)
    pub name: String,
    /// SQL query that defines the view
    pub query: String,
    /// Source topics this view reads from
    pub source_topics: Vec<String>,
    /// Whether the view is currently active
    pub active: bool,
    /// Refresh mode
    pub refresh_mode: RefreshMode,
    /// Creation timestamp (ms since epoch)
    pub created_at: u64,
    /// Last refresh timestamp
    pub last_refreshed_at: Option<u64>,
}

/// How the view is kept up-to-date
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum RefreshMode {
    /// Continuously updated as new records arrive
    #[default]
    Continuous,
    /// Refreshed at a fixed interval
    Periodic { interval_ms: u64 },
    /// Only refreshed on explicit request
    OnDemand,
}

/// State of a materialized view
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ViewState {
    /// View is being created / initial snapshot in progress
    Creating,
    /// View is active and being maintained
    Active,
    /// View is paused (no updates)
    Paused,
    /// View encountered an error
    Error(String),
    /// View is being dropped
    Dropping,
}

/// Statistics for a materialized view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewStats {
    /// Number of rows in the materialized view
    pub row_count: u64,
    /// Total records processed from source topics
    pub records_processed: u64,
    /// Records processed since last snapshot
    pub records_since_snapshot: u64,
    /// Last update timestamp
    pub last_update_ms: Option<u64>,
    /// Average processing latency in microseconds
    pub avg_latency_us: f64,
    /// Memory usage in bytes
    pub memory_bytes: u64,
}

impl Default for ViewStats {
    fn default() -> Self {
        Self {
            row_count: 0,
            records_processed: 0,
            records_since_snapshot: 0,
            last_update_ms: None,
            avg_latency_us: 0.0,
            memory_bytes: 0,
        }
    }
}

/// A row in the materialized view's state store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewRow {
    /// Primary key columns
    pub key: Vec<serde_json::Value>,
    /// Value columns
    pub values: HashMap<String, serde_json::Value>,
    /// Last update timestamp
    pub updated_at: u64,
}

/// Info returned about a view
#[derive(Debug, Clone, Serialize)]
pub struct ViewInfo {
    pub definition: MaterializedViewDef,
    pub state: ViewState,
    pub stats: ViewStats,
}

/// Manages all materialized views
pub struct MaterializedViewManager {
    views: Arc<RwLock<HashMap<String, ManagedView>>>,
}

struct ManagedView {
    definition: MaterializedViewDef,
    state: ViewState,
    stats: ViewStats,
    /// In-memory state store (key -> row)
    store: HashMap<String, ViewRow>,
}

impl MaterializedViewManager {
    pub fn new() -> Self {
        Self {
            views: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new materialized view
    pub async fn create_view(
        &self,
        name: &str,
        query: &str,
        refresh_mode: RefreshMode,
    ) -> Result<ViewInfo> {
        let mut views = self.views.write().await;
        if views.contains_key(name) {
            return Err(StreamlineError::Query(format!(
                "Materialized view '{}' already exists",
                name
            )));
        }

        // Extract source topics from the query (simplified parser)
        let source_topics = extract_source_topics(query);

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let definition = MaterializedViewDef {
            name: name.to_string(),
            query: query.to_string(),
            source_topics,
            active: true,
            refresh_mode,
            created_at: now_ms,
            last_refreshed_at: None,
        };

        let managed = ManagedView {
            definition: definition.clone(),
            state: ViewState::Creating,
            stats: ViewStats::default(),
            store: HashMap::new(),
        };

        let info = ViewInfo {
            definition: managed.definition.clone(),
            state: managed.state.clone(),
            stats: managed.stats.clone(),
        };

        views.insert(name.to_string(), managed);
        info!(view = name, "Materialized view created");

        // Transition to active after creation
        if let Some(view) = views.get_mut(name) {
            view.state = ViewState::Active;
        }

        Ok(info)
    }

    /// Drop a materialized view
    pub async fn drop_view(&self, name: &str) -> Result<()> {
        let mut views = self.views.write().await;
        if views.remove(name).is_none() {
            return Err(StreamlineError::Query(format!(
                "Materialized view '{}' not found",
                name
            )));
        }
        info!(view = name, "Materialized view dropped");
        Ok(())
    }

    /// Get information about a view
    pub async fn get_view(&self, name: &str) -> Option<ViewInfo> {
        let views = self.views.read().await;
        views.get(name).map(|v| ViewInfo {
            definition: v.definition.clone(),
            state: v.state.clone(),
            stats: v.stats.clone(),
        })
    }

    /// List all materialized views
    pub async fn list_views(&self) -> Vec<ViewInfo> {
        let views = self.views.read().await;
        views
            .values()
            .map(|v| ViewInfo {
                definition: v.definition.clone(),
                state: v.state.clone(),
                stats: v.stats.clone(),
            })
            .collect()
    }

    /// Pause a view (stop processing updates)
    pub async fn pause_view(&self, name: &str) -> Result<()> {
        let mut views = self.views.write().await;
        let view = views
            .get_mut(name)
            .ok_or_else(|| StreamlineError::Query(format!("View '{}' not found", name)))?;
        view.state = ViewState::Paused;
        view.definition.active = false;
        Ok(())
    }

    /// Resume a paused view
    pub async fn resume_view(&self, name: &str) -> Result<()> {
        let mut views = self.views.write().await;
        let view = views
            .get_mut(name)
            .ok_or_else(|| StreamlineError::Query(format!("View '{}' not found", name)))?;
        view.state = ViewState::Active;
        view.definition.active = true;
        Ok(())
    }

    /// Process a new record and update relevant views
    pub async fn process_record(
        &self,
        topic: &str,
        key: Option<&str>,
        value: &serde_json::Value,
    ) -> Result<usize> {
        let mut views = self.views.write().await;
        let mut updated = 0;

        for view in views.values_mut() {
            if view.state != ViewState::Active {
                continue;
            }
            if !view.definition.source_topics.contains(&topic.to_string()) {
                continue;
            }

            // Update stats
            view.stats.records_processed += 1;
            view.stats.records_since_snapshot += 1;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            view.stats.last_update_ms = Some(now_ms);

            // Upsert into the state store
            let store_key = key.unwrap_or("_default").to_string();
            if let serde_json::Value::Object(map) = value {
                let row = ViewRow {
                    key: vec![serde_json::Value::String(store_key.clone())],
                    values: map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                    updated_at: now_ms,
                };
                view.store.insert(store_key, row);
                view.stats.row_count = view.store.len() as u64;
            }
            updated += 1;
        }

        Ok(updated)
    }

    /// Query a view's state store by key
    pub async fn lookup(&self, view_name: &str, key: &str) -> Result<Option<ViewRow>> {
        let views = self.views.read().await;
        let view = views
            .get(view_name)
            .ok_or_else(|| StreamlineError::Query(format!("View '{}' not found", view_name)))?;
        Ok(view.store.get(key).cloned())
    }

    /// Scan all rows in a view (with optional limit)
    pub async fn scan(&self, view_name: &str, limit: Option<usize>) -> Result<Vec<ViewRow>> {
        let views = self.views.read().await;
        let view = views
            .get(view_name)
            .ok_or_else(|| StreamlineError::Query(format!("View '{}' not found", view_name)))?;

        let rows: Vec<ViewRow> = if let Some(limit) = limit {
            view.store.values().take(limit).cloned().collect()
        } else {
            view.store.values().cloned().collect()
        };

        Ok(rows)
    }

    /// Force refresh a view
    pub async fn refresh(&self, name: &str) -> Result<()> {
        let mut views = self.views.write().await;
        let view = views
            .get_mut(name)
            .ok_or_else(|| StreamlineError::Query(format!("View '{}' not found", name)))?;

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        view.definition.last_refreshed_at = Some(now_ms);
        view.stats.records_since_snapshot = 0;

        info!(view = name, "Materialized view refreshed");
        Ok(())
    }

    /// Get aggregate stats across all views
    pub async fn stats(&self) -> MaterializedViewManagerStats {
        let views = self.views.read().await;
        let active = views
            .values()
            .filter(|v| v.state == ViewState::Active)
            .count();
        let total_rows: u64 = views.values().map(|v| v.stats.row_count).sum();
        let total_processed: u64 = views.values().map(|v| v.stats.records_processed).sum();

        MaterializedViewManagerStats {
            total_views: views.len(),
            active_views: active,
            total_rows,
            total_records_processed: total_processed,
        }
    }
}

impl Default for MaterializedViewManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Aggregate stats for the view manager
#[derive(Debug, Clone, Serialize)]
pub struct MaterializedViewManagerStats {
    pub total_views: usize,
    pub active_views: usize,
    pub total_rows: u64,
    pub total_records_processed: u64,
}

/// Extract topic names from a FROM clause (simplified)
fn extract_source_topics(query: &str) -> Vec<String> {
    let query_upper = query.to_uppercase();
    let mut topics = Vec::new();

    if let Some(from_pos) = query_upper.find("FROM") {
        let after_from = &query[from_pos + 4..];
        let trimmed = after_from.trim();
        // Take until the next keyword
        let end = trimmed.find([' ', '\n', '\r']).unwrap_or(trimmed.len());
        let topic = trimmed[..end].trim().to_string();
        if !topic.is_empty() {
            topics.push(topic);
        }
    }

    topics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_and_list_views() {
        let manager = MaterializedViewManager::new();

        let info = manager
            .create_view(
                "user_stats",
                "SELECT user_id, COUNT(*) FROM events GROUP BY user_id",
                RefreshMode::Continuous,
            )
            .await
            .unwrap();

        assert_eq!(info.definition.name, "user_stats");
        assert_eq!(info.definition.source_topics, vec!["events"]);

        let views = manager.list_views().await;
        assert_eq!(views.len(), 1);
    }

    #[tokio::test]
    async fn test_duplicate_view_fails() {
        let manager = MaterializedViewManager::new();
        manager
            .create_view("v1", "SELECT * FROM t1", RefreshMode::Continuous)
            .await
            .unwrap();
        let result = manager
            .create_view("v1", "SELECT * FROM t1", RefreshMode::Continuous)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_drop_view() {
        let manager = MaterializedViewManager::new();
        manager
            .create_view("v1", "SELECT * FROM t1", RefreshMode::Continuous)
            .await
            .unwrap();
        manager.drop_view("v1").await.unwrap();
        assert!(manager.list_views().await.is_empty());
    }

    #[tokio::test]
    async fn test_process_record_updates_view() {
        let manager = MaterializedViewManager::new();
        manager
            .create_view(
                "v1",
                "SELECT user_id, amount FROM orders GROUP BY user_id",
                RefreshMode::Continuous,
            )
            .await
            .unwrap();

        let updated = manager
            .process_record(
                "orders",
                Some("user-1"),
                &serde_json::json!({"user_id": "user-1", "amount": 100}),
            )
            .await
            .unwrap();

        assert_eq!(updated, 1);

        let row = manager.lookup("v1", "user-1").await.unwrap();
        assert!(row.is_some());
        assert_eq!(
            row.unwrap().values.get("amount").unwrap(),
            &serde_json::json!(100)
        );
    }

    #[tokio::test]
    async fn test_scan_view() {
        let manager = MaterializedViewManager::new();
        manager
            .create_view("v1", "SELECT * FROM events", RefreshMode::Continuous)
            .await
            .unwrap();

        for i in 0..5 {
            manager
                .process_record(
                    "events",
                    Some(&format!("key-{}", i)),
                    &serde_json::json!({"id": i}),
                )
                .await
                .unwrap();
        }

        let rows = manager.scan("v1", Some(3)).await.unwrap();
        assert_eq!(rows.len(), 3);

        let all = manager.scan("v1", None).await.unwrap();
        assert_eq!(all.len(), 5);
    }

    #[tokio::test]
    async fn test_pause_resume_view() {
        let manager = MaterializedViewManager::new();
        manager
            .create_view("v1", "SELECT * FROM events", RefreshMode::Continuous)
            .await
            .unwrap();

        manager.pause_view("v1").await.unwrap();
        let info = manager.get_view("v1").await.unwrap();
        assert_eq!(info.state, ViewState::Paused);

        // Paused view should not process records
        let updated = manager
            .process_record("events", Some("k"), &serde_json::json!({"x": 1}))
            .await
            .unwrap();
        assert_eq!(updated, 0);

        manager.resume_view("v1").await.unwrap();
        let info = manager.get_view("v1").await.unwrap();
        assert_eq!(info.state, ViewState::Active);
    }

    #[test]
    fn test_extract_source_topics() {
        assert_eq!(
            extract_source_topics("SELECT * FROM events WHERE x > 1"),
            vec!["events"]
        );
        assert_eq!(
            extract_source_topics("SELECT user_id FROM user_events GROUP BY user_id"),
            vec!["user_events"]
        );
    }
}
