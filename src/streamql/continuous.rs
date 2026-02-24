//! Continuous Query Manager for StreamQL 1.0
//!
//! Manages the lifecycle of continuous queries that persist as background tasks:
//! - CREATE STREAM VIEW — defines a continuously-maintained materialized view
//! - CREATE ALERT — triggers notifications when query conditions are met
//! - SHOW QUERIES — lists all running continuous queries
//! - DROP QUERY — stops and removes a continuous query
//!
//! # Example
//!
//! ```sql
//! -- Create a continuously-updated materialized view
//! CREATE STREAM VIEW orders_by_region AS
//!   SELECT
//!     region,
//!     COUNT(*) as order_count,
//!     SUM(total) as revenue,
//!     AVG(total) as avg_order_value
//!   FROM orders
//!   WINDOW TUMBLING(INTERVAL '1 hour')
//!   GROUP BY region;
//!
//! -- Create an alert that fires on conditions
//! CREATE ALERT high_error_rate AS
//!   SELECT service, COUNT(*) as error_count
//!   FROM logs
//!   WHERE level = 'ERROR'
//!   WINDOW TUMBLING(INTERVAL '5 minutes')
//!   GROUP BY service
//!   HAVING COUNT(*) > 100
//!   NOTIFY WEBHOOK 'https://hooks.slack.com/...';
//!
//! -- Query the materialized view (point-in-time, <10ms)
//! SELECT * FROM orders_by_region WHERE region = 'us-east';
//!
//! -- Show running queries
//! SHOW QUERIES;
//!
//! -- Stop a query
//! DROP QUERY orders_by_region;
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Type of continuous query
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ContinuousQueryKind {
    /// Materialized view (CREATE STREAM VIEW)
    StreamView,
    /// Alert query (CREATE ALERT)
    Alert,
    /// Insert-select (INSERT INTO ... SELECT)
    InsertSelect,
}

impl std::fmt::Display for ContinuousQueryKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StreamView => write!(f, "STREAM VIEW"),
            Self::Alert => write!(f, "ALERT"),
            Self::InsertSelect => write!(f, "INSERT SELECT"),
        }
    }
}

/// State of a continuous query
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum QueryState {
    /// Query is running
    Running,
    /// Query is paused (can be resumed)
    Paused,
    /// Query failed with an error
    Failed(String),
    /// Query was stopped (DROP QUERY)
    Stopped,
}

impl std::fmt::Display for QueryState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => write!(f, "RUNNING"),
            Self::Paused => write!(f, "PAUSED"),
            Self::Failed(e) => write!(f, "FAILED: {}", e),
            Self::Stopped => write!(f, "STOPPED"),
        }
    }
}

/// Definition of a continuous query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousQueryDef {
    /// Query name (used as identifier)
    pub name: String,
    /// The SQL query text
    pub sql: String,
    /// Query kind
    pub kind: ContinuousQueryKind,
    /// Source topics
    pub source_topics: Vec<String>,
    /// Sink topic (for INSERT SELECT) or view name (for STREAM VIEW)
    pub sink: Option<String>,
    /// Window specification
    pub window: Option<WindowDef>,
    /// Alert notification config (for CREATE ALERT)
    pub notification: Option<NotificationConfig>,
    /// Created timestamp
    pub created_at: String,
}

/// Window definition for continuous queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowDef {
    /// Window type
    pub window_type: WindowKind,
    /// Window size in seconds
    pub size_secs: u64,
    /// Advance interval for hopping windows (in seconds)
    pub advance_secs: Option<u64>,
    /// Gap duration for session windows (in seconds)
    pub gap_secs: Option<u64>,
}

/// Window kinds
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum WindowKind {
    Tumbling,
    Hopping,
    Sliding,
    Session,
}

/// Notification configuration for CREATE ALERT queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationConfig {
    /// Notification channel type
    pub channel: NotifyChannel,
    /// Notification URL/endpoint
    pub endpoint: String,
    /// Optional headers
    pub headers: HashMap<String, String>,
    /// Cooldown period between notifications (seconds)
    pub cooldown_secs: u64,
}

/// Notification channel types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NotifyChannel {
    Webhook,
    Slack,
    PagerDuty,
    Log,
}

/// Runtime state for a running continuous query
pub struct RunningQuery {
    pub def: ContinuousQueryDef,
    pub state: QueryState,
    pub started_at: Instant,
    pub rows_processed: u64,
    pub rows_emitted: u64,
    pub last_error: Option<String>,
    pub last_emit: Option<Instant>,
}

/// Statistics for a continuous query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStats {
    pub name: String,
    pub kind: String,
    pub state: String,
    pub sql: String,
    pub source_topics: Vec<String>,
    pub rows_processed: u64,
    pub rows_emitted: u64,
    pub uptime_secs: u64,
    pub created_at: String,
}

/// The continuous query manager
pub struct ContinuousQueryManager {
    queries: Arc<RwLock<HashMap<String, RunningQuery>>>,
    /// Checkpoint manager for state recovery
    checkpoint_mgr: Arc<super::checkpoint::CheckpointManager>,
    /// Backpressure controller
    backpressure: Arc<super::checkpoint::BackpressureController>,
}

impl ContinuousQueryManager {
    /// Create a new manager
    pub fn new() -> Self {
        Self {
            queries: Arc::new(RwLock::new(HashMap::new())),
            checkpoint_mgr: Arc::new(super::checkpoint::CheckpointManager::new(
                super::checkpoint::CheckpointConfig::default(),
            )),
            backpressure: Arc::new(super::checkpoint::BackpressureController::new(10_000)),
        }
    }

    /// Create a new manager with custom checkpoint and backpressure config
    pub fn with_config(
        checkpoint_config: super::checkpoint::CheckpointConfig,
        max_lag_ms: u64,
    ) -> Self {
        Self {
            queries: Arc::new(RwLock::new(HashMap::new())),
            checkpoint_mgr: Arc::new(super::checkpoint::CheckpointManager::new(checkpoint_config)),
            backpressure: Arc::new(super::checkpoint::BackpressureController::new(max_lag_ms)),
        }
    }

    /// Get the checkpoint manager (for external use/testing).
    pub fn checkpoint_manager(&self) -> &Arc<super::checkpoint::CheckpointManager> {
        &self.checkpoint_mgr
    }

    /// Get the backpressure controller.
    pub fn backpressure_controller(&self) -> &Arc<super::checkpoint::BackpressureController> {
        &self.backpressure
    }

    /// Create and start a STREAM VIEW query
    pub async fn create_stream_view(
        &self,
        name: &str,
        sql: &str,
        source_topics: Vec<String>,
        window: Option<WindowDef>,
    ) -> Result<(), String> {
        let mut queries = self.queries.write().await;
        if queries.contains_key(name) {
            return Err(format!("Query '{}' already exists", name));
        }

        let def = ContinuousQueryDef {
            name: name.to_string(),
            sql: sql.to_string(),
            kind: ContinuousQueryKind::StreamView,
            source_topics,
            sink: Some(name.to_string()),
            window,
            notification: None,
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        let running = RunningQuery {
            def,
            state: QueryState::Running,
            started_at: Instant::now(),
            rows_processed: 0,
            rows_emitted: 0,
            last_error: None,
            last_emit: None,
        };

        info!(name = name, "Created continuous STREAM VIEW query");
        queries.insert(name.to_string(), running);
        Ok(())
    }

    /// Create and start an ALERT query
    pub async fn create_alert(
        &self,
        name: &str,
        sql: &str,
        source_topics: Vec<String>,
        notification: NotificationConfig,
        window: Option<WindowDef>,
    ) -> Result<(), String> {
        let mut queries = self.queries.write().await;
        if queries.contains_key(name) {
            return Err(format!("Alert '{}' already exists", name));
        }

        let def = ContinuousQueryDef {
            name: name.to_string(),
            sql: sql.to_string(),
            kind: ContinuousQueryKind::Alert,
            source_topics,
            sink: None,
            window,
            notification: Some(notification),
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        let running = RunningQuery {
            def,
            state: QueryState::Running,
            started_at: Instant::now(),
            rows_processed: 0,
            rows_emitted: 0,
            last_error: None,
            last_emit: None,
        };

        info!(name = name, "Created ALERT query");
        queries.insert(name.to_string(), running);
        Ok(())
    }

    /// Stop and remove a query
    pub async fn drop_query(&self, name: &str) -> Result<(), String> {
        let mut queries = self.queries.write().await;
        match queries.get_mut(name) {
            Some(q) => {
                q.state = QueryState::Stopped;
                info!(name = name, "Stopped continuous query");
                queries.remove(name);
                Ok(())
            }
            None => Err(format!("Query '{}' not found", name)),
        }
    }

    /// Pause a running query
    pub async fn pause_query(&self, name: &str) -> Result<(), String> {
        let mut queries = self.queries.write().await;
        match queries.get_mut(name) {
            Some(q) if q.state == QueryState::Running => {
                q.state = QueryState::Paused;
                info!(name = name, "Paused continuous query");
                Ok(())
            }
            Some(_) => Err(format!("Query '{}' is not running", name)),
            None => Err(format!("Query '{}' not found", name)),
        }
    }

    /// Resume a paused query
    pub async fn resume_query(&self, name: &str) -> Result<(), String> {
        let mut queries = self.queries.write().await;
        match queries.get_mut(name) {
            Some(q) if q.state == QueryState::Paused => {
                q.state = QueryState::Running;
                info!(name = name, "Resumed continuous query");
                Ok(())
            }
            Some(_) => Err(format!("Query '{}' is not paused", name)),
            None => Err(format!("Query '{}' not found", name)),
        }
    }

    /// List all queries
    pub async fn list_queries(&self) -> Vec<QueryStats> {
        let queries = self.queries.read().await;
        queries
            .values()
            .map(|q| QueryStats {
                name: q.def.name.clone(),
                kind: q.def.kind.to_string(),
                state: q.state.to_string(),
                sql: q.def.sql.clone(),
                source_topics: q.def.source_topics.clone(),
                rows_processed: q.rows_processed,
                rows_emitted: q.rows_emitted,
                uptime_secs: q.started_at.elapsed().as_secs(),
                created_at: q.def.created_at.clone(),
            })
            .collect()
    }

    /// Get statistics for a specific query
    pub async fn get_query_stats(&self, name: &str) -> Option<QueryStats> {
        let queries = self.queries.read().await;
        queries.get(name).map(|q| QueryStats {
            name: q.def.name.clone(),
            kind: q.def.kind.to_string(),
            state: q.state.to_string(),
            sql: q.def.sql.clone(),
            source_topics: q.def.source_topics.clone(),
            rows_processed: q.rows_processed,
            rows_emitted: q.rows_emitted,
            uptime_secs: q.started_at.elapsed().as_secs(),
            created_at: q.def.created_at.clone(),
        })
    }

    /// Update processing counters for a query
    /// Record processing progress for a query.
    ///
    /// Also triggers periodic checkpointing and backpressure checks.
    pub async fn record_processing(&self, name: &str, rows_in: u64, rows_out: u64) {
        let mut queries = self.queries.write().await;
        if let Some(q) = queries.get_mut(name) {
            q.rows_processed += rows_in;
            q.rows_emitted += rows_out;
            if rows_out > 0 {
                q.last_emit = Some(Instant::now());
            }

            // Periodic checkpoint (every 10,000 rows processed)
            if q.rows_processed % 10_000 == 0 && q.rows_processed > 0 {
                let offsets = std::collections::HashMap::from([(
                    format!("{}:processed", name),
                    q.rows_processed as i64,
                )]);
                let state = format!("rows_emitted:{}", q.rows_emitted).into_bytes();
                let checkpoint_mgr = self.checkpoint_mgr.clone();
                let query_name = name.to_string();
                // Checkpoint async to avoid blocking the processing loop
                tokio::spawn(async move {
                    let _ = checkpoint_mgr
                        .create_checkpoint(&query_name, offsets, state)
                        .await;
                });
            }
        }
    }

    /// Record processing with lag tracking for backpressure.
    ///
    /// Returns true if the query should pause due to backpressure.
    pub async fn record_processing_with_lag(
        &self,
        name: &str,
        rows_in: u64,
        rows_out: u64,
        lag_ms: u64,
        messages_behind: u64,
    ) -> bool {
        self.record_processing(name, rows_in, rows_out).await;
        self.backpressure.report_lag(name, lag_ms, messages_behind).await
    }

    /// Check if a query is backpressured.
    pub async fn is_backpressured(&self, name: &str) -> bool {
        self.backpressure.is_backpressured(name).await
    }

    /// Restore a query's state from the latest checkpoint.
    pub async fn restore_query_state(
        &self,
        name: &str,
    ) -> Option<(std::collections::HashMap<String, i64>, Vec<u8>)> {
        self.checkpoint_mgr.restore(name).await
    }

    /// Get the count of running queries
    pub async fn running_count(&self) -> usize {
        let queries = self.queries.read().await;
        queries
            .values()
            .filter(|q| q.state == QueryState::Running)
            .count()
    }
}

impl Default for ContinuousQueryManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a CREATE STREAM VIEW statement and extract components
pub fn parse_create_stream_view(sql: &str) -> Result<(String, String, Vec<String>), String> {
    let upper = sql.trim().to_uppercase();

    if !upper.starts_with("CREATE STREAM VIEW") {
        return Err("Expected CREATE STREAM VIEW statement".to_string());
    }

    // Extract view name: CREATE STREAM VIEW <name> AS ...
    let after_csview = sql.trim()["CREATE STREAM VIEW".len()..].trim();
    let as_pos = after_csview
        .to_uppercase()
        .find(" AS ")
        .ok_or("Expected AS keyword after view name")?;

    let view_name = after_csview[..as_pos].trim().to_string();
    let select_sql = after_csview[as_pos + 4..].trim().to_string();

    // Extract source topics from FROM clause
    let from_upper = select_sql.to_uppercase();
    let from_pos = from_upper.find("FROM ").ok_or("Expected FROM clause")?;
    let after_from = &select_sql[from_pos + 5..];

    let end_pos = after_from
        .find(|c: char| c.is_whitespace() && !c.is_ascii())
        .or_else(|| {
            let next_keyword = ["WHERE", "WINDOW", "GROUP", "HAVING", "ORDER", "LIMIT"];
            next_keyword
                .iter()
                .filter_map(|kw| after_from.to_uppercase().find(kw))
                .min()
        })
        .unwrap_or(after_from.len());

    let topics_str = after_from[..end_pos].trim();
    let source_topics: Vec<String> = topics_str
        .split(',')
        .map(|t| t.trim().trim_matches('\'').trim_matches('"').to_string())
        .filter(|t| !t.is_empty())
        .collect();

    Ok((view_name, select_sql, source_topics))
}

/// Parse a CREATE ALERT statement
pub fn parse_create_alert(sql: &str) -> Result<(String, String, Vec<String>, String), String> {
    let upper = sql.trim().to_uppercase();

    if !upper.starts_with("CREATE ALERT") {
        return Err("Expected CREATE ALERT statement".to_string());
    }

    let after_ca = sql.trim()["CREATE ALERT".len()..].trim();
    let as_pos = after_ca
        .to_uppercase()
        .find(" AS ")
        .ok_or("Expected AS keyword after alert name")?;

    let alert_name = after_ca[..as_pos].trim().to_string();
    let rest = after_ca[as_pos + 4..].trim();

    // Extract NOTIFY clause
    let notify_endpoint = if let Some(notify_pos) = rest.to_uppercase().find("NOTIFY") {
        let after_notify = rest[notify_pos + 6..].trim();
        let channel_end = after_notify
            .find(|c: char| c == '\'' || c == '"')
            .unwrap_or(0);
        let after_channel = after_notify[channel_end..].trim();
        let url_start = after_channel.find(|c: char| c == '\'' || c == '"').unwrap_or(0) + 1;
        let url_end = after_channel[url_start..]
            .find(|c: char| c == '\'' || c == '"')
            .unwrap_or(after_channel.len() - url_start);
        after_channel[url_start..url_start + url_end].to_string()
    } else {
        String::new()
    };

    let select_sql = if let Some(notify_pos) = rest.to_uppercase().find("NOTIFY") {
        rest[..notify_pos].trim().to_string()
    } else {
        rest.to_string()
    };

    // Extract source topics
    let from_upper = select_sql.to_uppercase();
    let source_topics = if let Some(from_pos) = from_upper.find("FROM ") {
        let after_from = &select_sql[from_pos + 5..];
        let end = after_from
            .to_uppercase()
            .find("WHERE")
            .or_else(|| after_from.to_uppercase().find("WINDOW"))
            .or_else(|| after_from.to_uppercase().find("GROUP"))
            .unwrap_or(after_from.len());
        after_from[..end]
            .trim()
            .split(',')
            .map(|t| t.trim().trim_matches('\'').trim_matches('"').to_string())
            .filter(|t| !t.is_empty())
            .collect()
    } else {
        vec![]
    };

    Ok((alert_name, select_sql, source_topics, notify_endpoint))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streamql::checkpoint::{BackpressureController, CheckpointConfig};

    #[test]
    fn test_parse_create_stream_view() {
        let sql = "CREATE STREAM VIEW orders_by_region AS
            SELECT region, COUNT(*) as order_count
            FROM orders
            WINDOW TUMBLING(INTERVAL '1 hour')
            GROUP BY region";

        let (name, select, topics) = parse_create_stream_view(sql).unwrap();
        assert_eq!(name, "orders_by_region");
        assert!(select.contains("SELECT"));
        assert!(topics.contains(&"orders".to_string()));
    }

    #[test]
    fn test_parse_create_alert() {
        let sql = "CREATE ALERT high_errors AS
            SELECT service, COUNT(*) as cnt
            FROM logs
            WHERE level = 'ERROR'
            GROUP BY service
            HAVING COUNT(*) > 100
            NOTIFY WEBHOOK 'https://hooks.slack.com/xxx'";

        let (name, select, topics, endpoint) = parse_create_alert(sql).unwrap();
        assert_eq!(name, "high_errors");
        assert!(select.contains("SELECT"));
        assert!(topics.contains(&"logs".to_string()));
        assert_eq!(endpoint, "https://hooks.slack.com/xxx");
    }

    #[tokio::test]
    async fn test_continuous_query_lifecycle() {
        let mgr = ContinuousQueryManager::new();

        // Create
        mgr.create_stream_view(
            "test_view",
            "SELECT * FROM events GROUP BY user_id",
            vec!["events".to_string()],
            None,
        )
        .await
        .unwrap();

        assert_eq!(mgr.running_count().await, 1);

        // List
        let queries = mgr.list_queries().await;
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].name, "test_view");
        assert_eq!(queries[0].state, "RUNNING");

        // Pause
        mgr.pause_query("test_view").await.unwrap();
        assert_eq!(mgr.running_count().await, 0);

        // Resume
        mgr.resume_query("test_view").await.unwrap();
        assert_eq!(mgr.running_count().await, 1);

        // Record processing
        mgr.record_processing("test_view", 100, 10).await;
        let stats = mgr.get_query_stats("test_view").await.unwrap();
        assert_eq!(stats.rows_processed, 100);
        assert_eq!(stats.rows_emitted, 10);

        // Drop
        mgr.drop_query("test_view").await.unwrap();
        assert_eq!(mgr.running_count().await, 0);
    }

    #[tokio::test]
    async fn test_duplicate_query_rejected() {
        let mgr = ContinuousQueryManager::new();
        mgr.create_stream_view("v1", "SELECT 1", vec![], None)
            .await
            .unwrap();
        let result = mgr
            .create_stream_view("v1", "SELECT 2", vec![], None)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_alert_with_notification() {
        let mgr = ContinuousQueryManager::new();
        mgr.create_alert(
            "high_cpu",
            "SELECT host FROM metrics WHERE cpu > 90",
            vec!["metrics".to_string()],
            NotificationConfig {
                channel: NotifyChannel::Webhook,
                endpoint: "https://example.com/hook".to_string(),
                headers: HashMap::new(),
                cooldown_secs: 300,
            },
            Some(WindowDef {
                window_type: WindowKind::Tumbling,
                size_secs: 300,
                advance_secs: None,
                gap_secs: None,
            }),
        )
        .await
        .unwrap();

        let queries = mgr.list_queries().await;
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].kind, "ALERT");
    }

    #[tokio::test]
    async fn test_record_processing_with_checkpoint() {
        let mgr = ContinuousQueryManager::new();
        mgr.create_stream_view(
            "test-view",
            "SELECT * FROM t",
            vec!["t".to_string()],
            None,
        )
        .await
        .unwrap();

        // Process 10,000 rows to trigger a checkpoint
        mgr.record_processing("test-view", 10_000, 5_000).await;

        // Allow the async checkpoint to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let checkpoint = mgr.checkpoint_manager().latest_checkpoint("test-view").await;
        assert!(checkpoint.is_some(), "Should have created a checkpoint after 10K rows");
    }

    #[tokio::test]
    async fn test_backpressure_integration() {
        let mgr = ContinuousQueryManager::with_config(
            CheckpointConfig::default(),
            5_000, // 5s max lag
        );
        mgr.create_stream_view("q1", "SELECT * FROM t", vec!["t".to_string()], None)
            .await
            .unwrap();

        // Below threshold
        let bp = mgr
            .record_processing_with_lag("q1", 100, 50, 2_000, 100)
            .await;
        assert!(!bp, "Should not be backpressured at 2s lag");
        assert!(!mgr.is_backpressured("q1").await);

        // Above threshold
        let bp = mgr
            .record_processing_with_lag("q1", 100, 50, 10_000, 50_000)
            .await;
        assert!(bp, "Should be backpressured at 10s lag");
        assert!(mgr.is_backpressured("q1").await);
    }

    #[tokio::test]
    async fn test_restore_query_state() {
        let mgr = ContinuousQueryManager::new();
        mgr.create_stream_view("q1", "SELECT * FROM t", vec!["t".to_string()], None)
            .await
            .unwrap();

        // Create a manual checkpoint
        let offsets = std::collections::HashMap::from([("t:0".to_string(), 500i64)]);
        mgr.checkpoint_manager()
            .create_checkpoint("q1", offsets, b"state-data".to_vec())
            .await
            .unwrap();

        let restored = mgr.restore_query_state("q1").await;
        assert!(restored.is_some());
        let (off, state) = restored.unwrap();
        assert_eq!(off["t:0"], 500);
        assert_eq!(state, b"state-data");
    }
}
