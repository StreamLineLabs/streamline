//! Federated Query Engine
//!
//! Provides cross-cluster and cross-database SQL query capabilities,
//! allowing StreamlineQL to query data from remote Streamline clusters,
//! Kafka clusters, PostgreSQL, MySQL, and HTTP APIs.
//!
//! # Features
//!
//! - **Multi-source federation**: Query across heterogeneous data sources
//! - **Predicate pushdown**: Push filters to remote sources for efficiency
//! - **Schema caching**: Cache remote schemas to reduce round-trips
//! - **Query planning**: Generate optimized federated execution plans
//!
//! # Example
//!
//! ```text
//! SELECT o.order_id, c.name
//! FROM remote_cluster.orders o
//! JOIN postgres_db.customers c ON o.customer_id = c.id
//! WHERE o.status = 'pending'
//! ```

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the federated query engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationConfig {
    /// Maximum number of remote sources that can be registered.
    #[serde(default = "default_max_remote_sources")]
    pub max_remote_sources: usize,
    /// Query timeout in milliseconds.
    #[serde(default = "default_query_timeout_ms")]
    pub query_timeout_ms: u64,
    /// Maximum number of concurrent remote queries.
    #[serde(default = "default_max_concurrent_remote_queries")]
    pub max_concurrent_remote_queries: usize,
    /// Whether to enable predicate pushdown to remote sources.
    #[serde(default = "default_enable_pushdown")]
    pub enable_pushdown: bool,
    /// How long to cache remote schemas (seconds).
    #[serde(default = "default_cache_remote_schemas_secs")]
    pub cache_remote_schemas_secs: u64,
}

fn default_max_remote_sources() -> usize {
    50
}
fn default_query_timeout_ms() -> u64 {
    30_000
}
fn default_max_concurrent_remote_queries() -> usize {
    10
}
fn default_enable_pushdown() -> bool {
    true
}
fn default_cache_remote_schemas_secs() -> u64 {
    300
}

impl Default for FederationConfig {
    fn default() -> Self {
        Self {
            max_remote_sources: default_max_remote_sources(),
            query_timeout_ms: default_query_timeout_ms(),
            max_concurrent_remote_queries: default_max_concurrent_remote_queries(),
            enable_pushdown: default_enable_pushdown(),
            cache_remote_schemas_secs: default_cache_remote_schemas_secs(),
        }
    }
}

// ---------------------------------------------------------------------------
// Source types
// ---------------------------------------------------------------------------

/// Type of federated data source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceType {
    StreamlineCluster,
    KafkaCluster,
    PostgreSQL,
    MySQL,
    HttpApi,
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StreamlineCluster => write!(f, "StreamlineCluster"),
            Self::KafkaCluster => write!(f, "KafkaCluster"),
            Self::PostgreSQL => write!(f, "PostgreSQL"),
            Self::MySQL => write!(f, "MySQL"),
            Self::HttpApi => write!(f, "HttpApi"),
        }
    }
}

/// Current status of a federated source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceStatus {
    Connected,
    Disconnected,
    Error(String),
    Connecting,
}

/// Authentication method for a remote source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthType {
    Password,
    Token,
    Certificate,
    ApiKey,
}

/// Credentials for a remote source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credentials {
    pub username: String,
    pub auth_type: AuthType,
}

/// Connection information for a remote source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub host: String,
    pub port: u16,
    pub database: Option<String>,
    pub credentials: Option<Credentials>,
    pub tls: bool,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Remote schema
// ---------------------------------------------------------------------------

/// Cached schema information from a remote source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteSchema {
    pub tables: Vec<RemoteTable>,
    pub cached_at: u64,
}

/// A table available in a remote source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteTable {
    pub name: String,
    pub columns: Vec<RemoteColumn>,
    pub row_count_estimate: Option<u64>,
}

/// A column in a remote table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

// ---------------------------------------------------------------------------
// Federated source
// ---------------------------------------------------------------------------

/// A registered federated data source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedSource {
    pub name: String,
    pub source_type: SourceType,
    pub connection: ConnectionInfo,
    pub status: SourceStatus,
    pub cached_schema: Option<RemoteSchema>,
    pub registered_at: u64,
    pub last_query_at: Option<u64>,
    pub query_count: u64,
}

// ---------------------------------------------------------------------------
// Federated query plan
// ---------------------------------------------------------------------------

/// A step in a federated execution plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlanStep {
    LocalScan {
        topic: String,
        predicates: Vec<String>,
    },
    RemoteScan {
        source: String,
        query: String,
    },
    Join {
        left: Box<PlanStep>,
        right: Box<PlanStep>,
        join_type: String,
        on: String,
    },
    Filter {
        input: Box<PlanStep>,
        predicate: String,
    },
    Project {
        input: Box<PlanStep>,
        columns: Vec<String>,
    },
    Aggregate {
        input: Box<PlanStep>,
        group_by: Vec<String>,
        aggregates: Vec<String>,
    },
}

/// An optimized plan for executing a federated query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedPlan {
    pub steps: Vec<PlanStep>,
    pub estimated_rows: u64,
    pub pushdown_predicates: Vec<String>,
}

/// A parsed federated query with its execution plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedQuery {
    pub query: String,
    pub sources_used: Vec<String>,
    pub plan: FederatedPlan,
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// Runtime statistics for the federation engine.
#[derive(Debug, Default)]
pub struct FederationStats {
    pub queries_executed: AtomicU64,
    pub remote_scans: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub errors: AtomicU64,
    pub total_rows_fetched: AtomicU64,
}

/// Snapshot of [`FederationStats`] suitable for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationStatsSnapshot {
    pub queries_executed: u64,
    pub remote_scans: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub errors: u64,
    pub total_rows_fetched: u64,
}

impl FederationStats {
    pub fn snapshot(&self) -> FederationStatsSnapshot {
        FederationStatsSnapshot {
            queries_executed: self.queries_executed.load(Ordering::Relaxed),
            remote_scans: self.remote_scans.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            total_rows_fetched: self.total_rows_fetched.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Federation engine
// ---------------------------------------------------------------------------

/// Federated query engine for cross-cluster and cross-database SQL queries.
pub struct FederationEngine {
    sources: Arc<RwLock<HashMap<String, FederatedSource>>>,
    config: FederationConfig,
    stats: Arc<FederationStats>,
}

impl FederationEngine {
    /// Create a new [`FederationEngine`] with the given configuration.
    pub fn new(config: FederationConfig) -> Self {
        info!(
            max_sources = config.max_remote_sources,
            timeout_ms = config.query_timeout_ms,
            pushdown = config.enable_pushdown,
            "Initializing federation engine"
        );
        Self {
            sources: Arc::new(RwLock::new(HashMap::new())),
            config,
            stats: Arc::new(FederationStats::default()),
        }
    }

    /// Register a new federated data source.
    pub async fn register_source(
        &self,
        name: String,
        source_type: SourceType,
        connection: ConnectionInfo,
    ) -> Result<()> {
        let mut sources = self.sources.write().await;

        if sources.len() >= self.config.max_remote_sources {
            warn!(
                max = self.config.max_remote_sources,
                "Maximum number of remote sources reached"
            );
            return Err(StreamlineError::Query(format!(
                "Maximum number of remote sources ({}) reached",
                self.config.max_remote_sources
            )));
        }

        if sources.contains_key(&name) {
            return Err(StreamlineError::Query(format!(
                "Source '{}' is already registered",
                name
            )));
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        info!(
            source = %name,
            source_type = %source_type,
            host = %connection.host,
            port = connection.port,
            "Registering federated source"
        );

        sources.insert(
            name.clone(),
            FederatedSource {
                name,
                source_type,
                connection,
                status: SourceStatus::Connecting,
                cached_schema: None,
                registered_at: now,
                last_query_at: None,
                query_count: 0,
            },
        );

        Ok(())
    }

    /// Remove a previously registered source.
    pub async fn remove_source(&self, name: &str) -> Result<FederatedSource> {
        let mut sources = self.sources.write().await;
        sources.remove(name).ok_or_else(|| {
            StreamlineError::Query(format!("Source '{}' not found", name))
        })
    }

    /// Get a clone of a registered source by name.
    pub async fn get_source(&self, name: &str) -> Result<FederatedSource> {
        let sources = self.sources.read().await;
        sources.get(name).cloned().ok_or_else(|| {
            StreamlineError::Query(format!("Source '{}' not found", name))
        })
    }

    /// List all registered sources.
    pub async fn list_sources(&self) -> Vec<FederatedSource> {
        let sources = self.sources.read().await;
        sources.values().cloned().collect()
    }

    /// Plan a federated query by analysing source references in the SQL text.
    ///
    /// Source references use dot-notation: `source_name.table_name`.
    pub async fn plan_query(&self, sql: &str) -> Result<FederatedQuery> {
        debug!(sql = %sql, "Planning federated query");

        let sources = self.sources.read().await;
        let mut sources_used: Vec<String> = Vec::new();
        let mut steps: Vec<PlanStep> = Vec::new();
        let mut pushdown_predicates: Vec<String> = Vec::new();

        // Extract WHERE predicates for pushdown
        let predicates = Self::extract_predicates(sql);

        // Identify source.table references
        for (source_name, source) in sources.iter() {
            if sql.contains(&format!("{}.", source_name)) {
                sources_used.push(source_name.clone());

                // Determine which predicates can be pushed down
                let source_predicates: Vec<String> = if self.config.enable_pushdown {
                    predicates
                        .iter()
                        .filter(|p| p.contains(&format!("{}.", source_name)))
                        .cloned()
                        .collect()
                } else {
                    Vec::new()
                };

                if !source_predicates.is_empty() {
                    pushdown_predicates.extend(source_predicates.clone());
                }

                let remote_query = Self::build_remote_query(source_name, sql, &source_predicates);

                if source.cached_schema.is_some() {
                    self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
                }

                steps.push(PlanStep::RemoteScan {
                    source: source_name.clone(),
                    query: remote_query,
                });
            }
        }

        // If no remote sources referenced, treat as a local-only query
        if sources_used.is_empty() {
            let topic = Self::extract_table_name(sql).unwrap_or_else(|| "unknown".to_string());
            steps.push(PlanStep::LocalScan {
                topic,
                predicates: predicates.clone(),
            });
        }

        let estimated_rows = Self::estimate_rows(&steps, &sources);

        self.stats.queries_executed.fetch_add(1, Ordering::Relaxed);
        self.stats
            .remote_scans
            .fetch_add(sources_used.len() as u64, Ordering::Relaxed);

        info!(
            sources = ?sources_used,
            steps = steps.len(),
            estimated_rows = estimated_rows,
            "Federated query planned"
        );

        Ok(FederatedQuery {
            query: sql.to_string(),
            sources_used,
            plan: FederatedPlan {
                steps,
                estimated_rows,
                pushdown_predicates,
            },
        })
    }

    /// Return a human-readable explanation of a federated query plan.
    pub async fn explain_federated(&self, sql: &str) -> Result<String> {
        let query = self.plan_query(sql).await?;
        let mut explanation = String::new();

        explanation.push_str("=== Federated Query Plan ===\n");
        explanation.push_str(&format!("Query: {}\n", query.query));
        explanation.push_str(&format!("Sources: {}\n", query.sources_used.join(", ")));
        explanation.push_str(&format!(
            "Estimated rows: {}\n",
            query.plan.estimated_rows
        ));

        if !query.plan.pushdown_predicates.is_empty() {
            explanation.push_str(&format!(
                "Pushdown predicates: {}\n",
                query.plan.pushdown_predicates.join(", ")
            ));
        }

        explanation.push_str("\nSteps:\n");
        for (i, step) in query.plan.steps.iter().enumerate() {
            explanation.push_str(&format!("  {}. {}\n", i + 1, Self::format_step(step)));
        }

        Ok(explanation)
    }

    /// Refresh the cached schema for a given source.
    pub async fn refresh_schema(&self, source_name: &str, schema: RemoteSchema) -> Result<()> {
        let mut sources = self.sources.write().await;
        let source = sources.get_mut(source_name).ok_or_else(|| {
            StreamlineError::Query(format!("Source '{}' not found", source_name))
        })?;

        debug!(
            source = %source_name,
            tables = schema.tables.len(),
            "Refreshing remote schema"
        );

        source.cached_schema = Some(schema);
        source.status = SourceStatus::Connected;
        Ok(())
    }

    /// Return a snapshot of the engine statistics.
    pub fn stats(&self) -> FederationStatsSnapshot {
        self.stats.snapshot()
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Extract simple WHERE predicates from a SQL string.
    fn extract_predicates(sql: &str) -> Vec<String> {
        let upper = sql.to_uppercase();
        let where_pos = match upper.find("WHERE") {
            Some(pos) => pos,
            None => return Vec::new(),
        };

        let after_where = &sql[where_pos + 5..];

        // Cut off anything after GROUP BY / ORDER BY / LIMIT / HAVING
        let end = ["GROUP BY", "ORDER BY", "LIMIT", "HAVING", ";"]
            .iter()
            .filter_map(|kw| after_where.to_uppercase().find(kw))
            .min()
            .unwrap_or(after_where.len());

        let clause = after_where[..end].trim();
        clause
            .split(" AND ")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Build a remote sub-query for a given source.
    fn build_remote_query(source_name: &str, _sql: &str, predicates: &[String]) -> String {
        if predicates.is_empty() {
            format!("SELECT * FROM {}", source_name)
        } else {
            let cleaned: Vec<String> = predicates
                .iter()
                .map(|p| p.replace(&format!("{}.", source_name), ""))
                .collect();
            format!("SELECT * FROM {} WHERE {}", source_name, cleaned.join(" AND "))
        }
    }

    /// Try to extract the first table name from a FROM clause.
    fn extract_table_name(sql: &str) -> Option<String> {
        let upper = sql.to_uppercase();
        let from_pos = upper.find("FROM")?;
        let after_from = sql[from_pos + 4..].trim();
        let token = after_from.split_whitespace().next()?;
        Some(token.trim_end_matches(',').to_string())
    }

    /// Rough row-count estimate based on plan steps and cached schemas.
    fn estimate_rows(
        steps: &[PlanStep],
        sources: &HashMap<String, FederatedSource>,
    ) -> u64 {
        let mut total: u64 = 0;
        for step in steps {
            match step {
                PlanStep::RemoteScan { source, .. } => {
                    if let Some(src) = sources.get(source) {
                        if let Some(schema) = &src.cached_schema {
                            total += schema
                                .tables
                                .iter()
                                .filter_map(|t| t.row_count_estimate)
                                .sum::<u64>();
                        } else {
                            total += 1000; // default estimate
                        }
                    } else {
                        total += 1000;
                    }
                }
                PlanStep::LocalScan { .. } => {
                    total += 1000;
                }
                _ => {}
            }
        }
        total
    }

    /// Format a single plan step for display.
    fn format_step(step: &PlanStep) -> String {
        match step {
            PlanStep::LocalScan { topic, predicates } => {
                if predicates.is_empty() {
                    format!("LocalScan(topic={})", topic)
                } else {
                    format!(
                        "LocalScan(topic={}, predicates=[{}])",
                        topic,
                        predicates.join(", ")
                    )
                }
            }
            PlanStep::RemoteScan { source, query } => {
                format!("RemoteScan(source={}, query={})", source, query)
            }
            PlanStep::Join {
                join_type, on, ..
            } => {
                format!("Join(type={}, on={})", join_type, on)
            }
            PlanStep::Filter { predicate, .. } => {
                format!("Filter({})", predicate)
            }
            PlanStep::Project { columns, .. } => {
                format!("Project(columns=[{}])", columns.join(", "))
            }
            PlanStep::Aggregate {
                group_by,
                aggregates,
                ..
            } => {
                format!(
                    "Aggregate(group_by=[{}], aggs=[{}])",
                    group_by.join(", "),
                    aggregates.join(", ")
                )
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> FederationConfig {
        FederationConfig {
            max_remote_sources: 5,
            ..Default::default()
        }
    }

    fn test_connection() -> ConnectionInfo {
        ConnectionInfo {
            host: "localhost".to_string(),
            port: 9092,
            database: None,
            credentials: None,
            tls: false,
            options: HashMap::new(),
        }
    }

    // -- FederationConfig tests ----------------------------------------------

    #[test]
    fn test_config_defaults() {
        let cfg = FederationConfig::default();
        assert_eq!(cfg.max_remote_sources, 50);
        assert_eq!(cfg.query_timeout_ms, 30_000);
        assert_eq!(cfg.max_concurrent_remote_queries, 10);
        assert!(cfg.enable_pushdown);
        assert_eq!(cfg.cache_remote_schemas_secs, 300);
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let cfg = FederationConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let deserialized: FederationConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.max_remote_sources, cfg.max_remote_sources);
        assert_eq!(deserialized.query_timeout_ms, cfg.query_timeout_ms);
    }

    #[test]
    fn test_config_serde_defaults_on_missing_fields() {
        let json = "{}";
        let cfg: FederationConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.max_remote_sources, 50);
        assert!(cfg.enable_pushdown);
    }

    // -- SourceType / SourceStatus tests ------------------------------------

    #[test]
    fn test_source_type_display() {
        assert_eq!(SourceType::StreamlineCluster.to_string(), "StreamlineCluster");
        assert_eq!(SourceType::PostgreSQL.to_string(), "PostgreSQL");
        assert_eq!(SourceType::HttpApi.to_string(), "HttpApi");
    }

    #[test]
    fn test_source_type_serde() {
        let st = SourceType::KafkaCluster;
        let json = serde_json::to_string(&st).unwrap();
        let deserialized: SourceType = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, st);
    }

    #[test]
    fn test_source_status_serde() {
        let statuses = vec![
            SourceStatus::Connected,
            SourceStatus::Disconnected,
            SourceStatus::Error("timeout".into()),
            SourceStatus::Connecting,
        ];
        for status in &statuses {
            let json = serde_json::to_string(status).unwrap();
            let de: SourceStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(&de, status);
        }
    }

    // -- Engine creation ----------------------------------------------------

    #[tokio::test]
    async fn test_engine_new() {
        let engine = FederationEngine::new(test_config());
        let sources = engine.list_sources().await;
        assert!(sources.is_empty());
    }

    // -- register_source ----------------------------------------------------

    #[tokio::test]
    async fn test_register_source() {
        let engine = FederationEngine::new(test_config());
        engine
            .register_source("pg".into(), SourceType::PostgreSQL, test_connection())
            .await
            .unwrap();

        let src = engine.get_source("pg").await.unwrap();
        assert_eq!(src.name, "pg");
        assert_eq!(src.source_type, SourceType::PostgreSQL);
        assert_eq!(src.status, SourceStatus::Connecting);
        assert_eq!(src.query_count, 0);
    }

    #[tokio::test]
    async fn test_register_duplicate_source_fails() {
        let engine = FederationEngine::new(test_config());
        engine
            .register_source("src1".into(), SourceType::MySQL, test_connection())
            .await
            .unwrap();
        let err = engine
            .register_source("src1".into(), SourceType::MySQL, test_connection())
            .await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_register_source_max_limit() {
        let engine = FederationEngine::new(test_config()); // max 5
        for i in 0..5 {
            engine
                .register_source(format!("s{}", i), SourceType::HttpApi, test_connection())
                .await
                .unwrap();
        }
        let err = engine
            .register_source("s_extra".into(), SourceType::HttpApi, test_connection())
            .await;
        assert!(err.is_err());
    }

    // -- remove_source ------------------------------------------------------

    #[tokio::test]
    async fn test_remove_source() {
        let engine = FederationEngine::new(test_config());
        engine
            .register_source("rm_me".into(), SourceType::KafkaCluster, test_connection())
            .await
            .unwrap();
        let removed = engine.remove_source("rm_me").await.unwrap();
        assert_eq!(removed.name, "rm_me");
        assert!(engine.get_source("rm_me").await.is_err());
    }

    #[tokio::test]
    async fn test_remove_nonexistent_source() {
        let engine = FederationEngine::new(test_config());
        assert!(engine.remove_source("nope").await.is_err());
    }

    // -- get_source / list_sources ------------------------------------------

    #[tokio::test]
    async fn test_get_source_not_found() {
        let engine = FederationEngine::new(test_config());
        assert!(engine.get_source("missing").await.is_err());
    }

    #[tokio::test]
    async fn test_list_sources_multiple() {
        let engine = FederationEngine::new(test_config());
        engine
            .register_source("a".into(), SourceType::PostgreSQL, test_connection())
            .await
            .unwrap();
        engine
            .register_source("b".into(), SourceType::MySQL, test_connection())
            .await
            .unwrap();
        let list = engine.list_sources().await;
        assert_eq!(list.len(), 2);
    }

    // -- plan_query ---------------------------------------------------------

    #[tokio::test]
    async fn test_plan_query_local_only() {
        let engine = FederationEngine::new(test_config());
        let plan = engine
            .plan_query("SELECT * FROM orders WHERE status = 'active'")
            .await
            .unwrap();

        assert!(plan.sources_used.is_empty());
        assert_eq!(plan.plan.steps.len(), 1);
        matches!(&plan.plan.steps[0], PlanStep::LocalScan { .. });
    }

    #[tokio::test]
    async fn test_plan_query_with_remote_source() {
        let engine = FederationEngine::new(test_config());
        engine
            .register_source("warehouse".into(), SourceType::PostgreSQL, test_connection())
            .await
            .unwrap();

        let plan = engine
            .plan_query("SELECT * FROM warehouse.orders")
            .await
            .unwrap();

        assert_eq!(plan.sources_used, vec!["warehouse"]);
        assert!(matches!(&plan.plan.steps[0], PlanStep::RemoteScan { source, .. } if source == "warehouse"));
    }

    #[tokio::test]
    async fn test_plan_query_pushdown() {
        let cfg = FederationConfig {
            enable_pushdown: true,
            ..test_config()
        };
        let engine = FederationEngine::new(cfg);
        engine
            .register_source("pg".into(), SourceType::PostgreSQL, test_connection())
            .await
            .unwrap();

        let plan = engine
            .plan_query("SELECT * FROM pg.users WHERE pg.active = true")
            .await
            .unwrap();

        assert!(!plan.plan.pushdown_predicates.is_empty());
    }

    #[tokio::test]
    async fn test_plan_query_no_pushdown() {
        let cfg = FederationConfig {
            enable_pushdown: false,
            ..test_config()
        };
        let engine = FederationEngine::new(cfg);
        engine
            .register_source("pg".into(), SourceType::PostgreSQL, test_connection())
            .await
            .unwrap();

        let plan = engine
            .plan_query("SELECT * FROM pg.users WHERE pg.active = true")
            .await
            .unwrap();

        assert!(plan.plan.pushdown_predicates.is_empty());
    }

    // -- explain_federated --------------------------------------------------

    #[tokio::test]
    async fn test_explain_federated_local() {
        let engine = FederationEngine::new(test_config());
        let explanation = engine
            .explain_federated("SELECT * FROM events")
            .await
            .unwrap();

        assert!(explanation.contains("Federated Query Plan"));
        assert!(explanation.contains("LocalScan"));
    }

    #[tokio::test]
    async fn test_explain_federated_remote() {
        let engine = FederationEngine::new(test_config());
        engine
            .register_source("remote".into(), SourceType::StreamlineCluster, test_connection())
            .await
            .unwrap();

        let explanation = engine
            .explain_federated("SELECT * FROM remote.events")
            .await
            .unwrap();

        assert!(explanation.contains("RemoteScan"));
        assert!(explanation.contains("remote"));
    }

    // -- refresh_schema -----------------------------------------------------

    #[tokio::test]
    async fn test_refresh_schema() {
        let engine = FederationEngine::new(test_config());
        engine
            .register_source("pg".into(), SourceType::PostgreSQL, test_connection())
            .await
            .unwrap();

        let schema = RemoteSchema {
            tables: vec![RemoteTable {
                name: "users".into(),
                columns: vec![
                    RemoteColumn {
                        name: "id".into(),
                        data_type: "INT".into(),
                        nullable: false,
                    },
                    RemoteColumn {
                        name: "name".into(),
                        data_type: "VARCHAR".into(),
                        nullable: true,
                    },
                ],
                row_count_estimate: Some(10_000),
            }],
            cached_at: 1_700_000_000,
        };

        engine.refresh_schema("pg", schema).await.unwrap();

        let src = engine.get_source("pg").await.unwrap();
        assert!(src.cached_schema.is_some());
        assert_eq!(src.status, SourceStatus::Connected);
        let cached = src.cached_schema.unwrap();
        assert_eq!(cached.tables.len(), 1);
        assert_eq!(cached.tables[0].columns.len(), 2);
    }

    #[tokio::test]
    async fn test_refresh_schema_unknown_source() {
        let engine = FederationEngine::new(test_config());
        let schema = RemoteSchema {
            tables: vec![],
            cached_at: 0,
        };
        assert!(engine.refresh_schema("unknown", schema).await.is_err());
    }

    // -- stats --------------------------------------------------------------

    #[tokio::test]
    async fn test_stats_initial() {
        let engine = FederationEngine::new(test_config());
        let snap = engine.stats();
        assert_eq!(snap.queries_executed, 0);
        assert_eq!(snap.remote_scans, 0);
        assert_eq!(snap.errors, 0);
    }

    #[tokio::test]
    async fn test_stats_after_queries() {
        let engine = FederationEngine::new(test_config());
        engine
            .register_source("x".into(), SourceType::HttpApi, test_connection())
            .await
            .unwrap();

        engine.plan_query("SELECT * FROM x.data").await.unwrap();
        engine.plan_query("SELECT * FROM x.data").await.unwrap();

        let snap = engine.stats();
        assert_eq!(snap.queries_executed, 2);
        assert_eq!(snap.remote_scans, 2);
    }

    #[tokio::test]
    async fn test_stats_cache_hits_with_schema() {
        let engine = FederationEngine::new(test_config());
        engine
            .register_source("cached".into(), SourceType::PostgreSQL, test_connection())
            .await
            .unwrap();
        engine
            .refresh_schema(
                "cached",
                RemoteSchema {
                    tables: vec![],
                    cached_at: 100,
                },
            )
            .await
            .unwrap();

        engine
            .plan_query("SELECT * FROM cached.t")
            .await
            .unwrap();
        let snap = engine.stats();
        assert_eq!(snap.cache_hits, 1);
        assert_eq!(snap.cache_misses, 0);
    }

    #[tokio::test]
    async fn test_stats_cache_miss_without_schema() {
        let engine = FederationEngine::new(test_config());
        engine
            .register_source("no_schema".into(), SourceType::MySQL, test_connection())
            .await
            .unwrap();

        engine
            .plan_query("SELECT * FROM no_schema.t")
            .await
            .unwrap();
        let snap = engine.stats();
        assert_eq!(snap.cache_misses, 1);
    }

    // -- helpers / edge cases -----------------------------------------------

    #[test]
    fn test_extract_predicates_no_where() {
        let preds = FederationEngine::extract_predicates("SELECT * FROM t");
        assert!(preds.is_empty());
    }

    #[test]
    fn test_extract_predicates_single() {
        let preds = FederationEngine::extract_predicates("SELECT * FROM t WHERE a = 1");
        assert_eq!(preds, vec!["a = 1"]);
    }

    #[test]
    fn test_extract_predicates_multiple() {
        let preds =
            FederationEngine::extract_predicates("SELECT * FROM t WHERE a = 1 AND b > 2 AND c < 3");
        assert_eq!(preds.len(), 3);
    }

    #[test]
    fn test_extract_predicates_with_group_by() {
        let preds = FederationEngine::extract_predicates(
            "SELECT * FROM t WHERE x = 1 GROUP BY y",
        );
        assert_eq!(preds, vec!["x = 1"]);
    }

    #[test]
    fn test_extract_table_name() {
        assert_eq!(
            FederationEngine::extract_table_name("SELECT * FROM orders WHERE id = 1"),
            Some("orders".to_string())
        );
    }

    #[test]
    fn test_extract_table_name_none() {
        assert_eq!(FederationEngine::extract_table_name("INSERT INTO t VALUES (1)"), None);
    }

    #[test]
    fn test_format_step_local_scan() {
        let step = PlanStep::LocalScan {
            topic: "events".into(),
            predicates: vec![],
        };
        assert_eq!(
            FederationEngine::format_step(&step),
            "LocalScan(topic=events)"
        );
    }

    #[test]
    fn test_format_step_aggregate() {
        let step = PlanStep::Aggregate {
            input: Box::new(PlanStep::LocalScan {
                topic: "t".into(),
                predicates: vec![],
            }),
            group_by: vec!["user_id".into()],
            aggregates: vec!["COUNT(*)".into()],
        };
        let formatted = FederationEngine::format_step(&step);
        assert!(formatted.contains("Aggregate"));
        assert!(formatted.contains("user_id"));
    }

    #[test]
    fn test_connection_info_serde() {
        let conn = ConnectionInfo {
            host: "db.example.com".into(),
            port: 5432,
            database: Some("mydb".into()),
            credentials: Some(Credentials {
                username: "admin".into(),
                auth_type: AuthType::Password,
            }),
            tls: true,
            options: HashMap::new(),
        };
        let json = serde_json::to_string(&conn).unwrap();
        let de: ConnectionInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(de.host, "db.example.com");
        assert_eq!(de.port, 5432);
        assert!(de.tls);
        assert_eq!(de.credentials.unwrap().auth_type, AuthType::Password);
    }

    #[test]
    fn test_federated_plan_serde() {
        let plan = FederatedPlan {
            steps: vec![
                PlanStep::LocalScan {
                    topic: "t".into(),
                    predicates: vec!["a > 1".into()],
                },
                PlanStep::RemoteScan {
                    source: "pg".into(),
                    query: "SELECT * FROM pg".into(),
                },
            ],
            estimated_rows: 5000,
            pushdown_predicates: vec!["a > 1".into()],
        };
        let json = serde_json::to_string(&plan).unwrap();
        let de: FederatedPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(de.steps.len(), 2);
        assert_eq!(de.estimated_rows, 5000);
    }

    #[test]
    fn test_plan_step_join_serde() {
        let step = PlanStep::Join {
            left: Box::new(PlanStep::LocalScan {
                topic: "a".into(),
                predicates: vec![],
            }),
            right: Box::new(PlanStep::RemoteScan {
                source: "b".into(),
                query: "SELECT *".into(),
            }),
            join_type: "INNER".into(),
            on: "a.id = b.id".into(),
        };
        let json = serde_json::to_string(&step).unwrap();
        let de: PlanStep = serde_json::from_str(&json).unwrap();
        assert!(matches!(de, PlanStep::Join { .. }));
    }

    #[tokio::test]
    async fn test_estimate_rows_with_schema() {
        let engine = FederationEngine::new(test_config());
        engine
            .register_source("est".into(), SourceType::PostgreSQL, test_connection())
            .await
            .unwrap();
        engine
            .refresh_schema(
                "est",
                RemoteSchema {
                    tables: vec![RemoteTable {
                        name: "big".into(),
                        columns: vec![],
                        row_count_estimate: Some(50_000),
                    }],
                    cached_at: 0,
                },
            )
            .await
            .unwrap();

        let query = engine
            .plan_query("SELECT * FROM est.big")
            .await
            .unwrap();
        assert_eq!(query.plan.estimated_rows, 50_000);
    }

    #[test]
    fn test_auth_type_serde() {
        for at in &[AuthType::Password, AuthType::Token, AuthType::Certificate, AuthType::ApiKey] {
            let json = serde_json::to_string(at).unwrap();
            let de: AuthType = serde_json::from_str(&json).unwrap();
            assert_eq!(&de, at);
        }
    }

    #[test]
    fn test_remote_schema_serde() {
        let schema = RemoteSchema {
            tables: vec![RemoteTable {
                name: "users".into(),
                columns: vec![RemoteColumn {
                    name: "id".into(),
                    data_type: "BIGINT".into(),
                    nullable: false,
                }],
                row_count_estimate: Some(100),
            }],
            cached_at: 1_700_000_000,
        };
        let json = serde_json::to_string(&schema).unwrap();
        let de: RemoteSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(de.tables.len(), 1);
        assert_eq!(de.tables[0].columns[0].name, "id");
    }
}
