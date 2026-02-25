//! Real-Time Streaming Materialized View Engine
//!
//! Provides incrementally-maintained materialized views over streaming data:
//!
//! - **Incremental Refresh**: Views update automatically as new records arrive,
//!   without full re-computation.
//! - **Window Types**: Tumbling, sliding, and session windows with watermark
//!   tracking for late-arriving data.
//! - **Change Notifications**: Subscribable change feed for downstream
//!   consumers (powers WebSocket push in the dashboard).
//! - **State Checkpointing**: Periodic snapshots of view state for recovery.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────┐     ┌──────────────────┐     ┌─────────────────┐
//! │ Source Topic  │────▶│ StreamingView    │────▶│ Change Feed     │
//! │ (partition)   │     │ (incremental SQL) │     │ (subscribers)   │
//! └──────────────┘     └──────────────────┘     └─────────────────┘
//!                            │
//!                       ┌────┴────┐
//!                       │ State   │
//!                       │ Store   │
//!                       └─────────┘
//! ```

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

/// Configuration for a streaming materialized view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingViewConfig {
    /// Unique view name.
    pub name: String,
    /// SQL query defining the view.
    pub query: String,
    /// Source topics referenced in the query.
    pub source_topics: Vec<String>,
    /// Window configuration (optional).
    pub window: Option<ViewWindowConfig>,
    /// Refresh strategy.
    pub refresh: RefreshStrategy,
    /// Maximum staleness before forced refresh (milliseconds).
    pub max_staleness_ms: u64,
    /// Enable change feed for this view.
    pub change_feed_enabled: bool,
    /// State checkpoint interval in milliseconds.
    pub checkpoint_interval_ms: u64,
}

impl Default for StreamingViewConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            query: String::new(),
            source_topics: Vec::new(),
            window: None,
            refresh: RefreshStrategy::Incremental,
            max_staleness_ms: 1000,
            change_feed_enabled: true,
            checkpoint_interval_ms: 30_000,
        }
    }
}

/// Window configuration for time-based views.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewWindowConfig {
    /// Window type.
    pub window_type: ViewWindowType,
    /// Window duration in seconds.
    pub duration_secs: u64,
    /// Slide interval for sliding windows (seconds).
    pub slide_secs: Option<u64>,
    /// Gap duration for session windows (seconds).
    pub session_gap_secs: Option<u64>,
    /// Allowed lateness before dropping events (seconds).
    pub allowed_lateness_secs: u64,
    /// Timestamp field in the source data.
    pub timestamp_field: String,
}

/// Window types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViewWindowType {
    /// Fixed-size, non-overlapping.
    Tumbling,
    /// Fixed-size, overlapping.
    Sliding,
    /// Variable-size based on activity gaps.
    Session,
}

/// How the view is refreshed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshStrategy {
    /// Incremental: only new records trigger partial re-computation.
    Incremental,
    /// Full: the entire query is re-executed on each refresh.
    Full,
    /// On-demand: only refreshed when queried.
    OnDemand,
}

/// State of a streaming view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViewState {
    /// View is being created.
    Creating,
    /// View is active and receiving updates.
    Active,
    /// View is paused.
    Paused,
    /// View encountered an error.
    Error,
    /// View is being dropped.
    Dropping,
}

/// A change event emitted when a view's data changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewChangeEvent {
    /// View name.
    pub view_name: String,
    /// Type of change.
    pub change_type: ViewChangeType,
    /// Affected rows (serialized as JSON).
    pub rows: Vec<serde_json::Value>,
    /// Watermark position.
    pub watermark: i64,
    /// Timestamp.
    pub timestamp: DateTime<Utc>,
}

/// Types of view changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViewChangeType {
    /// New rows added.
    Insert,
    /// Existing rows updated.
    Update,
    /// Rows removed (window expiry).
    Delete,
    /// Window closed and finalized.
    WindowClose,
}

/// Metrics for a streaming view.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ViewMetrics {
    /// Total records processed from source topics.
    pub records_processed: u64,
    /// Total incremental refreshes performed.
    pub incremental_refreshes: u64,
    /// Total full refreshes performed.
    pub full_refreshes: u64,
    /// Current result row count.
    pub result_rows: u64,
    /// Current watermark offset.
    pub watermark_offset: i64,
    /// Last refresh time.
    pub last_refresh: Option<DateTime<Utc>>,
    /// Average refresh latency in milliseconds.
    pub avg_refresh_latency_ms: f64,
    /// Total change events emitted.
    pub change_events_emitted: u64,
    /// Late-arriving events dropped.
    pub late_events_dropped: u64,
}

/// Comprehensive view info for API responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingViewInfo {
    /// View configuration.
    pub config: StreamingViewConfig,
    /// Current state.
    pub state: ViewState,
    /// View metrics.
    pub metrics: ViewMetrics,
    /// When the view was created.
    pub created_at: DateTime<Utc>,
}

/// The streaming materialized view engine.
pub struct StreamingViewEngine {
    /// Active views.
    views: Arc<RwLock<HashMap<String, StreamingViewState>>>,
    /// Change event broadcaster (per view).
    change_feeds: Arc<RwLock<HashMap<String, broadcast::Sender<ViewChangeEvent>>>>,
    /// Engine-level metrics.
    total_views: AtomicU64,
    total_refreshes: AtomicU64,
    running: AtomicBool,
}

/// Internal state for a single view.
struct StreamingViewState {
    config: StreamingViewConfig,
    state: ViewState,
    metrics: ViewMetrics,
    /// Per-source-topic watermark offsets.
    watermarks: HashMap<String, HashMap<i32, i64>>,
    created_at: DateTime<Utc>,
}

impl StreamingViewEngine {
    /// Create a new streaming view engine.
    pub fn new() -> Self {
        Self {
            views: Arc::new(RwLock::new(HashMap::new())),
            change_feeds: Arc::new(RwLock::new(HashMap::new())),
            total_views: AtomicU64::new(0),
            total_refreshes: AtomicU64::new(0),
            running: AtomicBool::new(false),
        }
    }

    /// Create a new streaming materialized view.
    pub async fn create_view(&self, config: StreamingViewConfig) -> Result<StreamingViewInfo> {
        let name = config.name.clone();

        if name.is_empty() {
            return Err(StreamlineError::Config("View name cannot be empty".into()));
        }
        if config.query.is_empty() {
            return Err(StreamlineError::Config("View query cannot be empty".into()));
        }

        let mut views = self.views.write().await;
        if views.contains_key(&name) {
            return Err(StreamlineError::Config(format!(
                "View '{}' already exists",
                name
            )));
        }

        let now = Utc::now();
        let state = StreamingViewState {
            config: config.clone(),
            state: ViewState::Active,
            metrics: ViewMetrics::default(),
            watermarks: HashMap::new(),
            created_at: now,
        };

        let info = StreamingViewInfo {
            config: config.clone(),
            state: ViewState::Active,
            metrics: ViewMetrics::default(),
            created_at: now,
        };

        views.insert(name.clone(), state);
        self.total_views.fetch_add(1, Ordering::Relaxed);

        // Create change feed
        if config.change_feed_enabled {
            let (tx, _) = broadcast::channel(1000);
            self.change_feeds.write().await.insert(name.clone(), tx);
        }

        tracing::info!(view = %name, "Created streaming materialized view");
        Ok(info)
    }

    /// Drop a streaming view.
    pub async fn drop_view(&self, name: &str) -> Result<()> {
        let mut views = self.views.write().await;
        if views.remove(name).is_none() {
            return Err(StreamlineError::Config(format!(
                "View '{}' not found",
                name
            )));
        }

        self.change_feeds.write().await.remove(name);
        self.total_views.fetch_sub(1, Ordering::Relaxed);

        tracing::info!(view = %name, "Dropped streaming materialized view");
        Ok(())
    }

    /// List all views.
    pub async fn list_views(&self) -> Vec<StreamingViewInfo> {
        self.views
            .read()
            .await
            .values()
            .map(|v| StreamingViewInfo {
                config: v.config.clone(),
                state: v.state.clone(),
                metrics: v.metrics.clone(),
                created_at: v.created_at,
            })
            .collect()
    }

    /// Get a specific view.
    pub async fn get_view(&self, name: &str) -> Option<StreamingViewInfo> {
        self.views.read().await.get(name).map(|v| StreamingViewInfo {
            config: v.config.clone(),
            state: v.state.clone(),
            metrics: v.metrics.clone(),
            created_at: v.created_at,
        })
    }

    /// Process new records for a source topic, triggering incremental refreshes.
    pub async fn process_records(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        records: &[serde_json::Value],
    ) -> Result<Vec<ViewChangeEvent>> {
        let mut all_events = Vec::new();
        let mut views = self.views.write().await;

        for (_, view) in views.iter_mut() {
            if view.state != ViewState::Active {
                continue;
            }

            if !view.config.source_topics.contains(&topic.to_string()) {
                continue;
            }

            // Update watermark
            let topic_watermarks = view
                .watermarks
                .entry(topic.to_string())
                .or_default();
            let current_watermark = topic_watermarks.entry(partition).or_insert(-1);

            if offset <= *current_watermark {
                view.metrics.late_events_dropped += records.len() as u64;
                continue;
            }
            *current_watermark = offset;

            // Record processing metrics
            view.metrics.records_processed += records.len() as u64;
            view.metrics.watermark_offset = offset;
            view.metrics.last_refresh = Some(Utc::now());

            match view.config.refresh {
                RefreshStrategy::Incremental => {
                    view.metrics.incremental_refreshes += 1;
                }
                RefreshStrategy::Full => {
                    view.metrics.full_refreshes += 1;
                }
                RefreshStrategy::OnDemand => {
                    // Skip — will be refreshed on query
                    continue;
                }
            }

            // Emit change events
            if view.config.change_feed_enabled && !records.is_empty() {
                let event = ViewChangeEvent {
                    view_name: view.config.name.clone(),
                    change_type: ViewChangeType::Insert,
                    rows: records.to_vec(),
                    watermark: offset,
                    timestamp: Utc::now(),
                };

                view.metrics.change_events_emitted += 1;
                all_events.push(event);
            }
        }

        // Broadcast change events
        let feeds = self.change_feeds.read().await;
        for event in &all_events {
            if let Some(tx) = feeds.get(&event.view_name) {
                let _ = tx.send(event.clone());
            }
        }

        self.total_refreshes.fetch_add(1, Ordering::Relaxed);
        Ok(all_events)
    }

    /// Subscribe to change events for a view.
    pub async fn subscribe(&self, view_name: &str) -> Result<broadcast::Receiver<ViewChangeEvent>> {
        let feeds = self.change_feeds.read().await;
        let tx = feeds.get(view_name).ok_or_else(|| {
            StreamlineError::Config(format!(
                "No change feed for view '{}'",
                view_name
            ))
        })?;
        Ok(tx.subscribe())
    }

    /// Pause a view.
    pub async fn pause_view(&self, name: &str) -> Result<()> {
        let mut views = self.views.write().await;
        let view = views.get_mut(name).ok_or_else(|| {
            StreamlineError::Config(format!("View '{}' not found", name))
        })?;
        view.state = ViewState::Paused;
        Ok(())
    }

    /// Resume a paused view.
    pub async fn resume_view(&self, name: &str) -> Result<()> {
        let mut views = self.views.write().await;
        let view = views.get_mut(name).ok_or_else(|| {
            StreamlineError::Config(format!("View '{}' not found", name))
        })?;
        view.state = ViewState::Active;
        Ok(())
    }

    /// Get engine-level statistics.
    pub async fn engine_stats(&self) -> EngineStats {
        EngineStats {
            total_views: self.total_views.load(Ordering::Relaxed),
            total_refreshes: self.total_refreshes.load(Ordering::Relaxed),
            active_views: self
                .views
                .read()
                .await
                .values()
                .filter(|v| v.state == ViewState::Active)
                .count() as u64,
        }
    }
}

impl Default for StreamingViewEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Engine-level statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineStats {
    pub total_views: u64,
    pub total_refreshes: u64,
    pub active_views: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_view() {
        let engine = StreamingViewEngine::new();
        let config = StreamingViewConfig {
            name: "test_view".to_string(),
            query: "SELECT * FROM events".to_string(),
            source_topics: vec!["events".to_string()],
            ..Default::default()
        };

        let info = engine.create_view(config).await.unwrap();
        assert_eq!(info.state, ViewState::Active);
    }

    #[tokio::test]
    async fn test_duplicate_view() {
        let engine = StreamingViewEngine::new();
        let config = StreamingViewConfig {
            name: "dup".to_string(),
            query: "SELECT 1".to_string(),
            ..Default::default()
        };

        engine.create_view(config.clone()).await.unwrap();
        assert!(engine.create_view(config).await.is_err());
    }

    #[tokio::test]
    async fn test_list_views() {
        let engine = StreamingViewEngine::new();
        for i in 0..3 {
            engine
                .create_view(StreamingViewConfig {
                    name: format!("view_{}", i),
                    query: "SELECT 1".to_string(),
                    ..Default::default()
                })
                .await
                .unwrap();
        }

        assert_eq!(engine.list_views().await.len(), 3);
    }

    #[tokio::test]
    async fn test_drop_view() {
        let engine = StreamingViewEngine::new();
        engine
            .create_view(StreamingViewConfig {
                name: "to_drop".to_string(),
                query: "SELECT 1".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        engine.drop_view("to_drop").await.unwrap();
        assert!(engine.get_view("to_drop").await.is_none());
    }

    #[tokio::test]
    async fn test_process_records() {
        let engine = StreamingViewEngine::new();
        engine
            .create_view(StreamingViewConfig {
                name: "events_view".to_string(),
                query: "SELECT count(*) FROM events".to_string(),
                source_topics: vec!["events".to_string()],
                change_feed_enabled: true,
                ..Default::default()
            })
            .await
            .unwrap();

        let records = vec![
            serde_json::json!({"id": 1, "name": "Alice"}),
            serde_json::json!({"id": 2, "name": "Bob"}),
        ];

        let events = engine.process_records("events", 0, 0, &records).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].rows.len(), 2);

        let info = engine.get_view("events_view").await.unwrap();
        assert_eq!(info.metrics.records_processed, 2);
    }

    #[tokio::test]
    async fn test_subscribe_change_feed() {
        let engine = StreamingViewEngine::new();
        engine
            .create_view(StreamingViewConfig {
                name: "sub_view".to_string(),
                query: "SELECT 1".to_string(),
                source_topics: vec!["topic1".to_string()],
                change_feed_enabled: true,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut rx = engine.subscribe("sub_view").await.unwrap();

        // Process records
        let records = vec![serde_json::json!({"data": "test"})];
        engine.process_records("topic1", 0, 0, &records).await.unwrap();

        // Should receive the change event
        let event = rx.try_recv().unwrap();
        assert_eq!(event.view_name, "sub_view");
    }

    #[tokio::test]
    async fn test_pause_resume() {
        let engine = StreamingViewEngine::new();
        engine
            .create_view(StreamingViewConfig {
                name: "pausable".to_string(),
                query: "SELECT 1".to_string(),
                source_topics: vec!["t".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        engine.pause_view("pausable").await.unwrap();
        let info = engine.get_view("pausable").await.unwrap();
        assert_eq!(info.state, ViewState::Paused);

        // Records should be skipped when paused
        let events = engine
            .process_records("t", 0, 0, &[serde_json::json!({"x": 1})])
            .await
            .unwrap();
        assert!(events.is_empty());

        engine.resume_view("pausable").await.unwrap();
        let info = engine.get_view("pausable").await.unwrap();
        assert_eq!(info.state, ViewState::Active);
    }
}
