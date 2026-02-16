//! DuckDB-based analytics engine for Streamline.
//!
//! This module implements a SQL query engine that operates directly on
//! Streamline topics using DuckDB's in-memory database. Topic data is
//! loaded into temporary tables via the [`TopicDataSource`] trait, so the
//! engine never needs a direct dependency on storage internals.
//!
//! # Architecture
//!
//! 1. **Topic extraction** -- A regex scanner finds `streamline_topic('name')`
//!    or `topic('name')` references in the SQL string.
//! 2. **Table materialisation** -- For each referenced topic, records are read
//!    from all partitions and inserted into a temporary DuckDB table with a
//!    flexible JSON-based schema.
//! 3. **Query execution** -- The rewritten SQL is executed against the
//!    materialised tables, and results are streamed back as JSON rows.
//! 4. **Caching** -- Results may be cached with a configurable TTL to avoid
//!    redundant table loads for repeated queries.
//!
//! # Stability
//!
//! **Stable** -- Breaking changes only in major versions.
//!
//! # Examples
//!
//! ```ignore
//! use streamline_analytics::duckdb::{DuckDBEngine, QueryOptions};
//! use std::sync::Arc;
//!
//! // Assuming `source` implements TopicDataSource:
//! let engine = DuckDBEngine::new(source).unwrap();
//!
//! // Execute a query with default options
//! let result = engine.execute_query(
//!     "SELECT * FROM streamline_topic_events LIMIT 10",
//!     QueryOptions::default(),
//! ).await.unwrap();
//! println!("Got {} rows", result.row_count);
//! ```

use crate::error::{AnalyticsError, Result};
use crate::topic_source::TopicDataSource;
use dashmap::DashMap;
use duckdb::{params, types::ValueRef, Connection};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tracing::{debug, info};

// ─── Public types ────────────────────────────────────────────────────────────

/// DuckDB analytics engine.
///
/// Provides SQL query capabilities on Streamline topics with automatic schema
/// inference. Each query materialises the referenced topics into temporary
/// DuckDB tables, executes the SQL, and returns results as JSON rows.
///
/// The engine is safe to share across threads (`Send + Sync`).
pub struct DuckDBEngine {
    /// DuckDB connection (in-memory database).
    /// Note: Using Mutex because DuckDB Connection contains RefCell which is not Sync.
    connection: Arc<Mutex<Connection>>,

    /// Topic data source for reading stream data.
    topic_manager: Arc<dyn TopicDataSource>,

    /// Cache for materialized views.
    materialized_views: Arc<DashMap<String, MaterializedView>>,

    /// Query result cache (SQL string -> cached result).
    query_cache: Arc<DashMap<String, CachedQueryResult>>,
}

/// Options controlling query execution behaviour.
///
/// # Defaults
///
/// | Field | Default |
/// |---|---|
/// | `max_rows` | `10 000` |
/// | `enable_cache` | `true` |
/// | `cache_ttl_seconds` | `60` |
/// | `timeout_ms` | `30 000` (30 s) |
/// | `offset` | `0` |
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOptions {
    /// Maximum number of rows to return (`None` = unlimited).
    pub max_rows: Option<usize>,

    /// Enable query result caching.
    pub enable_cache: bool,

    /// Cache TTL in seconds.
    pub cache_ttl_seconds: u64,

    /// Query timeout in milliseconds (`None` = no timeout).
    pub timeout_ms: Option<u64>,

    /// Number of rows to skip before returning results (pagination offset).
    pub offset: usize,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            max_rows: Some(10_000), // Default limit to prevent OOM
            enable_cache: true,
            cache_ttl_seconds: 60,
            timeout_ms: Some(30_000), // 30 seconds
            offset: 0,
        }
    }
}

/// The result of executing a SQL query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Column names in the result set.
    pub columns: Vec<String>,

    /// Rows returned by the query.
    pub rows: Vec<QueryResultRow>,

    /// Number of rows in `rows`.
    pub row_count: usize,

    /// Total number of rows that matched (before pagination).
    /// Equal to `row_count` when no offset/limit is applied.
    pub total_rows: usize,

    /// Query execution time in milliseconds.
    pub execution_time_ms: u64,

    /// Whether the result was served from cache.
    pub from_cache: bool,

    /// `true` when there are more rows beyond the current page.
    pub has_more: bool,
}

/// A single row in query results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResultRow {
    /// Column values encoded as JSON.
    pub values: Vec<JsonValue>,
}

/// Cached query result (internal).
#[derive(Debug, Clone)]
struct CachedQueryResult {
    result: QueryResult,
    cached_at: std::time::Instant,
    ttl_seconds: u64,
}

impl CachedQueryResult {
    fn is_expired(&self) -> bool {
        self.cached_at.elapsed().as_secs() > self.ttl_seconds
    }
}

/// Materialized view definition.
///
/// A materialized view is a named SQL query whose results are refreshed on a
/// configurable interval. Between refreshes the cached results are served.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedView {
    /// View name.
    pub name: String,

    /// SQL query defining the view.
    pub query: String,

    /// Refresh interval in seconds.
    pub refresh_interval_seconds: u64,

    /// Unix timestamp (seconds) of the last refresh.
    pub last_refreshed: i64,
}

/// Virtual table handle for exposing Streamline topics to DuckDB.
///
/// Currently used as a marker type; actual topic data is loaded into temporary
/// tables before each query.
#[allow(dead_code)]
pub struct StreamTable {
    topic_manager: Arc<dyn TopicDataSource>,
}

/// Cache statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    /// Total entries in the cache (valid + expired).
    pub total_entries: usize,
    /// Entries that are still within their TTL.
    pub valid_entries: usize,
    /// Entries whose TTL has elapsed.
    pub expired_entries: usize,
}

/// Result of an EXPLAIN query showing the query execution plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainResult {
    /// The original SQL query.
    pub sql: String,
    /// The rewritten SQL (with topic references resolved).
    pub rewritten_sql: String,
    /// DuckDB query plan output lines.
    pub plan: Vec<String>,
    /// Topics referenced by the query.
    pub referenced_topics: Vec<String>,
}

// ─── Engine implementation ───────────────────────────────────────────────────

impl DuckDBEngine {
    /// Create a new DuckDB analytics engine.
    ///
    /// Opens an in-memory DuckDB database and registers any custom functions.
    /// Returns an error if DuckDB initialisation fails.
    ///
    /// # Arguments
    ///
    /// * `topic_manager` - Data source implementing [`TopicDataSource`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use streamline_analytics::duckdb::DuckDBEngine;
    /// # use streamline_analytics::topic_source::TopicDataSource;
    /// # use std::sync::Arc;
    /// # fn example(src: Arc<dyn TopicDataSource>) {
    /// let engine = DuckDBEngine::new(src).unwrap();
    /// # }
    /// ```
    pub fn new(topic_manager: Arc<dyn TopicDataSource>) -> Result<Self> {
        let connection = Connection::open_in_memory().map_err(|e| {
            AnalyticsError::DuckDb(format!("Failed to create DuckDB connection: {}", e))
        })?;

        // Disable automatic extension installation to avoid failures on
        // systems where dynamic extension loading is restricted (e.g., macOS
        // code-signing policy).  The bundled build already includes core
        // extensions; we only need to LOAD them.
        let _ = connection.execute_batch(
            "SET autoinstall_known_extensions=false; SET autoload_known_extensions=true;",
        );
        // Try to load the JSON extension (bundled) so that ->> and JSON
        // functions are available.  Ignore errors if not bundled.
        let _ = connection.execute_batch("LOAD json;");

        info!("DuckDB analytics engine initialized");

        let engine = Self {
            connection: Arc::new(Mutex::new(connection)),
            topic_manager: topic_manager.clone(),
            materialized_views: Arc::new(DashMap::new()),
            query_cache: Arc::new(DashMap::new()),
        };

        // Register custom functions
        engine.register_functions()?;

        Ok(engine)
    }

    /// Register custom SQL functions.
    fn register_functions(&self) -> Result<()> {
        // Note: Virtual table registration would happen here.
        // DuckDB's Rust API for virtual tables is still evolving.
        // For now we use a simpler approach with temporary tables.
        debug!("Registered DuckDB custom functions");
        Ok(())
    }

    // ── Query execution ──────────────────────────────────────────────────

    /// Execute a SQL query on stream data.
    ///
    /// Topic references of the form `streamline_topic('name')` are
    /// automatically detected and the corresponding data is loaded into
    /// DuckDB before execution. The table name follows the pattern
    /// `streamline_topic_{name}` (with `-` replaced by `_`).
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL query to execute.
    /// * `options` - Execution options (limits, caching, timeout, pagination).
    ///
    /// # Errors
    ///
    /// Returns `AnalyticsError::InvalidSql` for malformed SQL,
    /// `AnalyticsError::TopicNotFound` when a referenced topic does not exist,
    /// and `AnalyticsError::QueryTimeout` when the configured timeout is
    /// exceeded.
    ///
    /// # Examples
    ///
    /// ```sql
    /// SELECT user_id, COUNT(*) as event_count
    /// FROM streamline_topic('events')
    /// WHERE timestamp > now() - interval '1 hour'
    /// GROUP BY user_id
    /// ORDER BY event_count DESC
    /// LIMIT 10
    /// ```
    pub async fn execute_query(&self, sql: &str, options: QueryOptions) -> Result<QueryResult> {
        let start_time = std::time::Instant::now();

        // Check cache first
        if options.enable_cache {
            if let Some(cached) = self.query_cache.get(sql) {
                if !cached.is_expired() {
                    debug!("Serving query from cache");
                    let mut result = cached.result.clone();
                    result.from_cache = true;
                    return Ok(result);
                } else {
                    // Remove expired cache entry
                    drop(cached);
                    self.query_cache.remove(sql);
                }
            }
        }

        // Enforce timeout via tokio::time::timeout when configured
        let result = if let Some(timeout_ms) = options.timeout_ms {
            let duration = std::time::Duration::from_millis(timeout_ms);
            match tokio::time::timeout(duration, self.execute_query_internal(sql, &options)).await {
                Ok(inner) => inner?,
                Err(_elapsed) => {
                    return Err(AnalyticsError::query_timeout(timeout_ms));
                }
            }
        } else {
            self.execute_query_internal(sql, &options).await?
        };

        let execution_time_ms = start_time.elapsed().as_millis() as u64;

        let total_rows = result.1.len();

        // Apply pagination (offset + limit) on the collected rows
        let offset = options.offset;
        let limit = options.max_rows.unwrap_or(usize::MAX);
        let paginated: Vec<QueryResultRow> = result
            .1
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect();
        let has_more = total_rows > offset + paginated.len();

        let query_result = QueryResult {
            columns: result.0,
            row_count: paginated.len(),
            total_rows,
            rows: paginated,
            execution_time_ms,
            from_cache: false,
            has_more,
        };

        // Cache result if enabled
        if options.enable_cache {
            self.query_cache.insert(
                sql.to_string(),
                CachedQueryResult {
                    result: query_result.clone(),
                    cached_at: std::time::Instant::now(),
                    ttl_seconds: options.cache_ttl_seconds,
                },
            );
        }

        info!(
            sql = %sql,
            rows = query_result.row_count,
            total_rows = query_result.total_rows,
            execution_time_ms = execution_time_ms,
            "Query executed successfully"
        );

        Ok(query_result)
    }

    /// Internal query execution (no caching, no timeout wrapping).
    async fn execute_query_internal(
        &self,
        sql: &str,
        _options: &QueryOptions,
    ) -> Result<(Vec<String>, Vec<QueryResultRow>)> {
        // Extract topic references from SQL
        let topic_names = self.extract_topic_references(sql)?;

        // Load topic data into temporary tables
        for topic_name in &topic_names {
            self.load_topic_into_table(topic_name).await?;
        }

        // Rewrite function-call syntax to actual table names
        let rewritten_sql = self.rewrite_topic_references(sql);

        // Execute the query (single pass to avoid re-execution issues with
        // temporary topic tables that are only available for one query run).
        let conn = self.connection.lock();
        let mut stmt = conn.prepare(&rewritten_sql).map_err(|e| {
            AnalyticsError::invalid_sql(sql, e.to_string())
        })?;

        let mut rows_result = stmt.query([]).map_err(|e| {
            AnalyticsError::DuckDb(format!("Failed to execute query: {}", e))
        })?;

        // Collect all rows, probing column count dynamically from each row.
        // We cannot call stmt.column_names() here because Rows holds a
        // mutable borrow on stmt; we retrieve names after dropping Rows.
        // NOTE: pagination (offset + limit) is applied by the caller
        // (execute_query) so we collect all rows here to preserve the
        // correct total_rows count.
        let mut rows = Vec::new();

        while let Some(row) = rows_result
            .next()
            .map_err(|e| AnalyticsError::DuckDb(format!("Failed to fetch row: {}", e)))?
        {
            let mut values = Vec::new();
            for i in 0.. {
                match row.get_ref(i) {
                    Ok(value) => values.push(duckdb_value_to_json(value)),
                    Err(_) => break,
                }
            }
            rows.push(QueryResultRow { values });
        }

        // Release the Rows borrow so we can access column metadata.
        drop(rows_result);

        // Column names are available after the statement has been executed.
        let columns: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();

        Ok((columns, rows))
    }

    // ── Topic extraction ─────────────────────────────────────────────────

    /// Extract topic names from a SQL query.
    ///
    /// Looks for patterns like:
    /// - `streamline_topic('topic_name')`
    /// - `topic('topic_name')`
    fn extract_topic_references(&self, sql: &str) -> Result<Vec<String>> {
        use std::collections::HashSet;
        let mut topics_set = HashSet::new();

        // Match function-call syntax: streamline_topic('name') or topic('name')
        let fn_patterns = [
            regex::Regex::new(r"streamline_topic\s*\(\s*'([^']+)'\s*\)").unwrap(),
            // Use word boundary to avoid matching 'topic' inside 'streamline_topic'
            regex::Regex::new(r"\btopic\s*\(\s*'([^']+)'\s*\)").unwrap(),
        ];

        for pattern in &fn_patterns {
            for cap in pattern.captures_iter(sql) {
                if let Some(topic_name) = cap.get(1) {
                    topics_set.insert(topic_name.as_str().to_string());
                }
            }
        }

        // Match direct table name syntax: streamline_topic_<name>
        // This handles queries like "FROM streamline_topic_events"
        let table_pattern =
            regex::Regex::new(r"\bstreamline_topic_(\w+)\b").unwrap();
        for cap in table_pattern.captures_iter(sql) {
            if let Some(topic_name) = cap.get(1) {
                topics_set.insert(topic_name.as_str().to_string());
            }
        }

        Ok(topics_set.into_iter().collect())
    }

    /// Rewrite function-call syntax to table names in SQL.
    ///
    /// Converts `streamline_topic('name')` and `topic('name')` to the
    /// corresponding DuckDB table name `streamline_topic_<name>`.
    fn rewrite_topic_references(&self, sql: &str) -> String {
        let mut result = sql.to_string();

        // Replace streamline_topic('name') with streamline_topic_name
        let st_pattern =
            regex::Regex::new(r"streamline_topic\s*\(\s*'([^']+)'\s*\)").unwrap();
        result = st_pattern
            .replace_all(&result, |caps: &regex::Captures| {
                let name = caps[1].replace('-', "_");
                format!("streamline_topic_{}", name)
            })
            .to_string();

        // Replace topic('name') with streamline_topic_name (word boundary)
        let t_pattern =
            regex::Regex::new(r"\btopic\s*\(\s*'([^']+)'\s*\)").unwrap();
        result = t_pattern
            .replace_all(&result, |caps: &regex::Captures| {
                let name = caps[1].replace('-', "_");
                format!("streamline_topic_{}", name)
            })
            .to_string();

        result
    }

    // ── Topic loading ────────────────────────────────────────────────────

    /// Load topic data into a temporary DuckDB table.
    ///
    /// Creates (or replaces) a table named `streamline_topic_{name}` with
    /// columns: `offset`, `partition`, `timestamp`, `key`, `value`, `headers`.
    async fn load_topic_into_table(&self, topic_name: &str) -> Result<()> {
        debug!(topic = %topic_name, "Loading topic data into DuckDB table");

        // Get partition count to verify topic exists
        let num_partitions = self.topic_manager.num_partitions(topic_name).map_err(|_| {
            AnalyticsError::topic_not_found(topic_name)
        })?;

        // Create table schema (flexible JSON-based approach)
        let table_name = format!("streamline_topic_{}", topic_name.replace('-', "_"));
        let conn = self.connection.lock();

        // Drop table if it exists
        let _ = conn.execute(&format!("DROP TABLE IF EXISTS {}", table_name), []);

        // Create table with flexible schema
        conn.execute(
            &format!(
                "CREATE TABLE {} (
                    \"offset\" BIGINT,
                    \"partition\" INT,
                    \"timestamp\" TIMESTAMP,
                    \"key\" VARCHAR,
                    \"value\" VARCHAR,
                    \"headers\" VARCHAR
                )",
                table_name
            ),
            [],
        )
        .map_err(|e| AnalyticsError::DuckDb(format!("Failed to create table: {}", e)))?;

        // Read data from all partitions via TopicDataSource trait
        for partition_id in 0..num_partitions {
            let start_offset = self
                .topic_manager
                .earliest_offset(topic_name, partition_id)?;
            let end_offset = self.topic_manager.latest_offset(topic_name, partition_id)?;

            if start_offset >= end_offset {
                continue; // Empty partition
            }

            // Fetch records (limit to prevent OOM)
            let max_records = 100_000;
            let record_count = (end_offset - start_offset).min(max_records);
            let records = self.topic_manager.read_records(
                topic_name,
                partition_id,
                start_offset,
                record_count as usize,
            )?;

            // Insert records into DuckDB table
            for record in records {
                let value_json =
                    if let Ok(json) = serde_json::from_slice::<JsonValue>(&record.value) {
                        serde_json::to_string(&json).unwrap_or_else(|_| "null".to_string())
                    } else {
                        serde_json::to_string(&String::from_utf8_lossy(&record.value))
                            .unwrap_or_else(|_| "null".to_string())
                    };

                let headers_json =
                    serde_json::to_string(&record.headers).unwrap_or_else(|_| "[]".to_string());

                let key_str = record
                    .key
                    .as_ref()
                    .map(|k| String::from_utf8_lossy(k).to_string());

                conn.execute(
                    &format!(
                        "INSERT INTO {} (\"offset\", \"partition\", \"timestamp\", \"key\", \"value\", \"headers\") VALUES (?, ?, ?, ?, ?, ?)",
                        table_name
                    ),
                    params![
                        record.offset,
                        partition_id,
                        chrono::DateTime::from_timestamp_millis(record.timestamp)
                            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                            .unwrap_or_default(),
                        key_str,
                        value_json,
                        headers_json,
                    ],
                )
                .map_err(|e| {
                    AnalyticsError::DuckDb(format!("Failed to insert record: {}", e))
                })?;
            }
        }

        // Also create a view alias
        conn.execute(
            &format!(
                "CREATE OR REPLACE VIEW streamline_topic_{}_view AS SELECT * FROM {}",
                topic_name.replace('-', "_"),
                table_name
            ),
            [],
        )
        .map_err(|e| AnalyticsError::DuckDb(format!("Failed to create view: {}", e)))?;

        info!(topic = %topic_name, table = %table_name, "Topic data loaded into DuckDB");

        Ok(())
    }

    // ── Materialized views ───────────────────────────────────────────────

    /// Create a materialized view.
    ///
    /// The view is immediately materialised in DuckDB and its metadata is
    /// stored in memory. Use [`refresh_materialized_view`](Self::refresh_materialized_view)
    /// to update the contents.
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL is invalid or DuckDB cannot create the view.
    pub fn create_materialized_view(
        &self,
        name: &str,
        query: &str,
        refresh_interval_seconds: u64,
    ) -> Result<()> {
        let view = MaterializedView {
            name: name.to_string(),
            query: query.to_string(),
            refresh_interval_seconds,
            last_refreshed: chrono::Utc::now().timestamp(),
        };

        let conn = self.connection.lock();
        conn.execute(&format!("CREATE OR REPLACE VIEW {} AS {}", name, query), [])
            .map_err(|e| {
                AnalyticsError::invalid_sql(query, format!("Failed to create materialized view: {}", e))
            })?;

        self.materialized_views.insert(name.to_string(), view);

        info!(view = %name, "Materialized view created");
        Ok(())
    }

    /// Refresh a materialized view by re-executing its defining query.
    ///
    /// # Errors
    ///
    /// Returns an error if the view does not exist or the SQL fails.
    pub async fn refresh_materialized_view(&self, name: &str) -> Result<()> {
        let mut view = self.materialized_views.get_mut(name).ok_or_else(|| {
            AnalyticsError::Analytics(format!("Materialized view not found: {}", name))
        })?;

        let conn = self.connection.lock();
        conn.execute(
            &format!("CREATE OR REPLACE VIEW {} AS {}", name, view.query),
            [],
        )
        .map_err(|e| {
            AnalyticsError::DuckDb(format!("Failed to refresh materialized view: {}", e))
        })?;

        view.last_refreshed = chrono::Utc::now().timestamp();

        info!(view = %name, "Materialized view refreshed");
        Ok(())
    }

    /// List all materialized views.
    pub fn list_materialized_views(&self) -> Vec<MaterializedView> {
        self.materialized_views
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Drop a materialized view by name.
    ///
    /// # Errors
    ///
    /// Returns an error if the view does not exist.
    pub async fn drop_materialized_view(&self, name: &str) -> Result<()> {
        self.materialized_views.remove(name).ok_or_else(|| {
            AnalyticsError::Analytics(format!("Materialized view '{}' not found", name))
        })?;
        info!(view = %name, "Materialized view dropped");
        Ok(())
    }

    // ── Query plan / EXPLAIN ────────────────────────────────────────────

    /// Return the DuckDB query plan for the given SQL without executing it.
    ///
    /// This is the programmatic equivalent of `EXPLAIN <sql>`.  The
    /// referenced topics are loaded into temporary tables so that DuckDB
    /// can produce a meaningful plan.
    ///
    /// # Errors
    ///
    /// Returns `AnalyticsError::InvalidSql` if the SQL is malformed, or
    /// `AnalyticsError::TopicNotFound` if a referenced topic does not exist.
    pub async fn explain_query(&self, sql: &str) -> Result<ExplainResult> {
        let topic_names = self.extract_topic_references(sql)?;

        for topic_name in &topic_names {
            self.load_topic_into_table(topic_name).await?;
        }

        let rewritten_sql = self.rewrite_topic_references(sql);
        let explain_sql = format!("EXPLAIN {}", rewritten_sql);

        let conn = self.connection.lock();
        let mut stmt = conn.prepare(&explain_sql).map_err(|e| {
            AnalyticsError::invalid_sql(sql, e.to_string())
        })?;

        let mut rows = stmt.query([]).map_err(|e| {
            AnalyticsError::DuckDb(format!("Failed to run EXPLAIN: {}", e))
        })?;

        let mut plan_lines = Vec::new();
        while let Some(row) = rows
            .next()
            .map_err(|e| AnalyticsError::DuckDb(format!("Failed to read EXPLAIN output: {}", e)))?
        {
            // DuckDB EXPLAIN returns rows with varying column counts;
            // concatenate all text columns on each row.
            let mut line = String::new();
            for i in 0.. {
                match row.get_ref(i) {
                    Ok(val) => {
                        if !line.is_empty() {
                            line.push('\t');
                        }
                        line.push_str(&duckdb_value_to_json(val).to_string());
                    }
                    Err(_) => break,
                }
            }
            plan_lines.push(line);
        }

        Ok(ExplainResult {
            sql: sql.to_string(),
            rewritten_sql,
            plan: plan_lines,
            referenced_topics: topic_names,
        })
    }

    // ── Cache management ─────────────────────────────────────────────────

    /// Clear the entire query result cache.
    pub fn clear_cache(&self) {
        self.query_cache.clear();
        info!("Query cache cleared");
    }

    /// Return cache statistics.
    pub fn cache_stats(&self) -> CacheStats {
        let total_entries = self.query_cache.len();
        let expired_entries = self
            .query_cache
            .iter()
            .filter(|entry| entry.value().is_expired())
            .count();

        CacheStats {
            total_entries,
            valid_entries: total_entries - expired_entries,
            expired_entries,
        }
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Convert a DuckDB `ValueRef` to a `serde_json::Value`.
fn duckdb_value_to_json(value: ValueRef<'_>) -> JsonValue {
    match value {
        ValueRef::Null => JsonValue::Null,
        ValueRef::Boolean(b) => JsonValue::Bool(b),
        ValueRef::TinyInt(i) => JsonValue::Number(i.into()),
        ValueRef::SmallInt(i) => JsonValue::Number(i.into()),
        ValueRef::Int(i) => JsonValue::Number(i.into()),
        ValueRef::BigInt(i) => JsonValue::Number(i.into()),
        ValueRef::HugeInt(i) => {
            // Try to fit into i64 for a proper JSON Number; fall back to string for very large values.
            if let Ok(n) = i64::try_from(i) {
                JsonValue::Number(n.into())
            } else {
                JsonValue::String(i.to_string())
            }
        }
        ValueRef::UTinyInt(i) => JsonValue::Number(i.into()),
        ValueRef::USmallInt(i) => JsonValue::Number(i.into()),
        ValueRef::UInt(i) => JsonValue::Number(i.into()),
        ValueRef::UBigInt(i) => JsonValue::Number(i.into()),
        ValueRef::Float(f) => serde_json::Number::from_f64(f as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        ValueRef::Double(f) => serde_json::Number::from_f64(f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        ValueRef::Text(s) => {
            let text = String::from_utf8_lossy(s);
            // Try to parse as a JSON literal first (numbers, booleans, null).
            // DuckDB may return numeric results as Text when expressions go
            // through the ->> (JSON extract string) operator.
            serde_json::from_str(&text).unwrap_or_else(|_| JsonValue::String(text.into_owned()))
        }
        ValueRef::Blob(b) => {
            JsonValue::String(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                b,
            ))
        }
        _ => JsonValue::String(format!("{:?}", value)),
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_source::{AnalyticsRecord, TopicDataSource};

    /// Mock topic source returning empty data.
    struct MockTopicSource;

    impl TopicDataSource for MockTopicSource {
        fn num_partitions(&self, _topic: &str) -> crate::error::Result<i32> {
            Ok(1)
        }
        fn earliest_offset(&self, _topic: &str, _partition: i32) -> crate::error::Result<i64> {
            Ok(0)
        }
        fn latest_offset(&self, _topic: &str, _partition: i32) -> crate::error::Result<i64> {
            Ok(0)
        }
        fn read_records(
            &self,
            _topic: &str,
            _partition: i32,
            _offset: i64,
            _max: usize,
        ) -> crate::error::Result<Vec<AnalyticsRecord>> {
            Ok(vec![])
        }
    }

    /// Mock topic source that returns sample data for the "events" topic.
    struct PopulatedMockSource;

    impl TopicDataSource for PopulatedMockSource {
        fn num_partitions(&self, topic: &str) -> crate::error::Result<i32> {
            if topic == "events" || topic == "metrics" {
                Ok(1)
            } else {
                Err(AnalyticsError::topic_not_found(topic))
            }
        }

        fn earliest_offset(&self, _topic: &str, _partition: i32) -> crate::error::Result<i64> {
            Ok(0)
        }

        fn latest_offset(&self, topic: &str, _partition: i32) -> crate::error::Result<i64> {
            if topic == "events" {
                Ok(5)
            } else if topic == "metrics" {
                Ok(3)
            } else {
                Ok(0)
            }
        }

        fn read_records(
            &self,
            topic: &str,
            _partition: i32,
            _offset: i64,
            _max: usize,
        ) -> crate::error::Result<Vec<AnalyticsRecord>> {
            if topic == "events" {
                Ok(vec![
                    AnalyticsRecord {
                        offset: 0,
                        timestamp: 1700000000000,
                        key: Some(b"user-1".to_vec()),
                        value: br#"{"user_id":"u1","action":"click","amount":10}"#.to_vec(),
                        headers: vec![],
                    },
                    AnalyticsRecord {
                        offset: 1,
                        timestamp: 1700000001000,
                        key: Some(b"user-2".to_vec()),
                        value: br#"{"user_id":"u2","action":"view","amount":20}"#.to_vec(),
                        headers: vec![],
                    },
                    AnalyticsRecord {
                        offset: 2,
                        timestamp: 1700000002000,
                        key: Some(b"user-1".to_vec()),
                        value: br#"{"user_id":"u1","action":"purchase","amount":100}"#.to_vec(),
                        headers: vec![],
                    },
                    AnalyticsRecord {
                        offset: 3,
                        timestamp: 1700000003000,
                        key: Some(b"user-3".to_vec()),
                        value: br#"{"user_id":"u3","action":"click","amount":5}"#.to_vec(),
                        headers: vec![],
                    },
                    AnalyticsRecord {
                        offset: 4,
                        timestamp: 1700000004000,
                        key: Some(b"user-2".to_vec()),
                        value: br#"{"user_id":"u2","action":"purchase","amount":50}"#.to_vec(),
                        headers: vec![],
                    },
                ])
            } else if topic == "metrics" {
                Ok(vec![
                    AnalyticsRecord {
                        offset: 0,
                        timestamp: 1700000000000,
                        key: None,
                        value: br#"{"host":"h1","cpu":45.5,"mem":1024}"#.to_vec(),
                        headers: vec![],
                    },
                    AnalyticsRecord {
                        offset: 1,
                        timestamp: 1700000001000,
                        key: None,
                        value: br#"{"host":"h2","cpu":78.2,"mem":2048}"#.to_vec(),
                        headers: vec![],
                    },
                    AnalyticsRecord {
                        offset: 2,
                        timestamp: 1700000002000,
                        key: None,
                        value: br#"{"host":"h1","cpu":52.0,"mem":1536}"#.to_vec(),
                        headers: vec![],
                    },
                ])
            } else {
                Ok(vec![])
            }
        }
    }

    // ── Engine creation ──────────────────────────────────────────────────

    #[tokio::test]
    async fn test_duckdb_engine_creation() {
        let topic_source: Arc<dyn TopicDataSource> = Arc::new(MockTopicSource);
        let engine = DuckDBEngine::new(topic_source);
        assert!(engine.is_ok());
    }

    // ── Topic reference extraction ───────────────────────────────────────

    #[tokio::test]
    async fn test_extract_topic_references() {
        let topic_source: Arc<dyn TopicDataSource> = Arc::new(MockTopicSource);
        let engine = DuckDBEngine::new(topic_source).unwrap();

        let sql = "SELECT * FROM streamline_topic('events') WHERE topic('logs')";
        let topics = engine.extract_topic_references(sql).unwrap();

        assert_eq!(topics.len(), 2);
        assert!(topics.contains(&"events".to_string()));
        assert!(topics.contains(&"logs".to_string()));
    }

    #[tokio::test]
    async fn test_extract_no_topic_references() {
        let topic_source: Arc<dyn TopicDataSource> = Arc::new(MockTopicSource);
        let engine = DuckDBEngine::new(topic_source).unwrap();

        let sql = "SELECT 1 + 1 AS result";
        let topics = engine.extract_topic_references(sql).unwrap();
        assert!(topics.is_empty());
    }

    // ── Basic SQL queries ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_simple_select() {
        let src: Arc<dyn TopicDataSource> = Arc::new(MockTopicSource);
        let engine = DuckDBEngine::new(src).unwrap();

        let result = engine
            .execute_query("SELECT 1 AS one, 'hello' AS greeting", QueryOptions::default())
            .await
            .unwrap();

        assert_eq!(result.row_count, 1);
        assert_eq!(result.columns, vec!["one", "greeting"]);
        assert_eq!(result.rows[0].values[0], serde_json::json!(1));
        assert_eq!(result.rows[0].values[1], serde_json::json!("hello"));
    }

    #[tokio::test]
    async fn test_query_on_populated_topic() {
        let src: Arc<dyn TopicDataSource> = Arc::new(PopulatedMockSource);
        let engine = DuckDBEngine::new(src).unwrap();

        let result = engine
            .execute_query(
                "SELECT * FROM streamline_topic_events ORDER BY \"offset\"",
                QueryOptions::default(),
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 5);
        assert!(!result.from_cache);
    }

    // ── Aggregation queries ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_count_aggregation() {
        let src: Arc<dyn TopicDataSource> = Arc::new(PopulatedMockSource);
        let engine = DuckDBEngine::new(src).unwrap();

        let result = engine
            .execute_query(
                "SELECT COUNT(*) AS cnt FROM streamline_topic_events",
                QueryOptions { enable_cache: false, ..Default::default() },
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 1);
        assert_eq!(result.rows[0].values[0], serde_json::json!(5));
    }

    #[tokio::test]
    async fn test_sum_aggregation() {
        let src: Arc<dyn TopicDataSource> = Arc::new(PopulatedMockSource);
        let engine = DuckDBEngine::new(src).unwrap();

        // DuckDB can extract JSON fields with json_extract_string / ->>
        let result = engine
            .execute_query(
                "SELECT SUM(CAST(value->>'amount' AS INTEGER)) AS total FROM streamline_topic_events",
                QueryOptions { enable_cache: false, ..Default::default() },
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 1);
        // 10 + 20 + 100 + 5 + 50 = 185
        assert_eq!(result.rows[0].values[0], serde_json::json!(185));
    }

    #[tokio::test]
    async fn test_avg_aggregation() {
        let src: Arc<dyn TopicDataSource> = Arc::new(PopulatedMockSource);
        let engine = DuckDBEngine::new(src).unwrap();

        let result = engine
            .execute_query(
                "SELECT AVG(CAST(value->>'amount' AS DOUBLE)) AS avg_amount FROM streamline_topic_events",
                QueryOptions { enable_cache: false, ..Default::default() },
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 1);
        // 185 / 5 = 37.0
        let avg_val = result.rows[0].values[0].as_f64().unwrap();
        assert!((avg_val - 37.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_group_by_aggregation() {
        let src: Arc<dyn TopicDataSource> = Arc::new(PopulatedMockSource);
        let engine = DuckDBEngine::new(src).unwrap();

        let result = engine
            .execute_query(
                "SELECT key, COUNT(*) AS cnt FROM streamline_topic_events GROUP BY key ORDER BY cnt DESC",
                QueryOptions { enable_cache: false, ..Default::default() },
            )
            .await
            .unwrap();

        // user-1: 2, user-2: 2, user-3: 1
        assert_eq!(result.row_count, 3);
    }

    // ── Window functions ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_window_function_row_number() {
        let src: Arc<dyn TopicDataSource> = Arc::new(PopulatedMockSource);
        let engine = DuckDBEngine::new(src).unwrap();

        let result = engine
            .execute_query(
                "SELECT \"offset\", ROW_NUMBER() OVER (ORDER BY \"offset\") AS rn FROM streamline_topic_events",
                QueryOptions { enable_cache: false, ..Default::default() },
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 5);
        // Check that row numbers go 1..5
        for (i, row) in result.rows.iter().enumerate() {
            assert_eq!(row.values[1], serde_json::json!((i + 1) as i64));
        }
    }

    // ── Error handling ───────────────────────────────────────────────────

    #[tokio::test]
    async fn test_invalid_sql_returns_error() {
        let src: Arc<dyn TopicDataSource> = Arc::new(MockTopicSource);
        let engine = DuckDBEngine::new(src).unwrap();

        let result = engine
            .execute_query("SELCT * FORM nothing", QueryOptions::default())
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        // Should be an InvalidSql variant
        assert!(
            matches!(err, AnalyticsError::InvalidSql(_)),
            "Expected InvalidSql, got: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn test_nonexistent_topic_returns_error() {
        let src: Arc<dyn TopicDataSource> = Arc::new(PopulatedMockSource);
        let engine = DuckDBEngine::new(src).unwrap();

        let result = engine
            .execute_query(
                "SELECT * FROM streamline_topic_does_not_exist",
                QueryOptions { enable_cache: false, ..Default::default() },
            )
            .await;

        // The table does not exist so DuckDB will report an error
        assert!(result.is_err());
    }

    // ── Caching ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_cache_hit() {
        let src: Arc<dyn TopicDataSource> = Arc::new(MockTopicSource);
        let engine = DuckDBEngine::new(src).unwrap();

        let sql = "SELECT 42 AS answer";
        let opts = QueryOptions {
            enable_cache: true,
            cache_ttl_seconds: 60,
            ..Default::default()
        };

        let r1 = engine.execute_query(sql, opts.clone()).await.unwrap();
        assert!(!r1.from_cache);

        let r2 = engine.execute_query(sql, opts).await.unwrap();
        assert!(r2.from_cache);
    }

    #[tokio::test]
    async fn test_cache_stats() {
        let src: Arc<dyn TopicDataSource> = Arc::new(MockTopicSource);
        let engine = DuckDBEngine::new(src).unwrap();

        let stats = engine.cache_stats();
        assert_eq!(stats.total_entries, 0);

        engine
            .execute_query("SELECT 1", QueryOptions::default())
            .await
            .unwrap();

        let stats = engine.cache_stats();
        assert_eq!(stats.total_entries, 1);
        assert_eq!(stats.valid_entries, 1);

        engine.clear_cache();
        let stats = engine.cache_stats();
        assert_eq!(stats.total_entries, 0);
    }

    // ── Pagination ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_pagination_offset_and_limit() {
        let src: Arc<dyn TopicDataSource> = Arc::new(PopulatedMockSource);
        let engine = DuckDBEngine::new(src).unwrap();

        // First page: 2 rows
        let page1 = engine
            .execute_query(
                "SELECT * FROM streamline_topic_events ORDER BY \"offset\"",
                QueryOptions {
                    max_rows: Some(2),
                    offset: 0,
                    enable_cache: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(page1.row_count, 2);
        assert_eq!(page1.total_rows, 5);
        assert!(page1.has_more);

        // Second page: offset=2, limit=2
        let page2 = engine
            .execute_query(
                "SELECT * FROM streamline_topic_events ORDER BY \"offset\"",
                QueryOptions {
                    max_rows: Some(2),
                    offset: 2,
                    enable_cache: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(page2.row_count, 2);
        assert!(page2.has_more);

        // Third page: offset=4, limit=2 (only 1 row left)
        let page3 = engine
            .execute_query(
                "SELECT * FROM streamline_topic_events ORDER BY \"offset\"",
                QueryOptions {
                    max_rows: Some(2),
                    offset: 4,
                    enable_cache: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(page3.row_count, 1);
        assert!(!page3.has_more);
    }

    // ── Query timeout ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_query_does_not_timeout_on_fast_query() {
        let src: Arc<dyn TopicDataSource> = Arc::new(MockTopicSource);
        let engine = DuckDBEngine::new(src).unwrap();

        let result = engine
            .execute_query(
                "SELECT 1",
                QueryOptions {
                    timeout_ms: Some(5_000),
                    enable_cache: false,
                    ..Default::default()
                },
            )
            .await;

        assert!(result.is_ok());
    }

    // ── Concurrent queries ───────────────────────────────────────────────

    #[tokio::test]
    async fn test_concurrent_queries() {
        let src: Arc<dyn TopicDataSource> = Arc::new(MockTopicSource);
        let engine = Arc::new(DuckDBEngine::new(src).unwrap());

        let mut handles = Vec::new();
        for i in 0..5 {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                eng.execute_query(
                    &format!("SELECT {} AS val", i),
                    QueryOptions {
                        enable_cache: false,
                        ..Default::default()
                    },
                )
                .await
            }));
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }
    }

    // ── Materialized views ───────────────────────────────────────────────

    #[tokio::test]
    async fn test_materialized_view_lifecycle() {
        let src: Arc<dyn TopicDataSource> = Arc::new(MockTopicSource);
        let engine = DuckDBEngine::new(src).unwrap();

        // Create
        engine
            .create_materialized_view("test_view", "SELECT 42 AS answer", 300)
            .unwrap();
        assert_eq!(engine.list_materialized_views().len(), 1);

        // Refresh
        engine.refresh_materialized_view("test_view").await.unwrap();

        // Drop
        engine.drop_materialized_view("test_view").await.unwrap();
        assert_eq!(engine.list_materialized_views().len(), 0);
    }

    #[tokio::test]
    async fn test_drop_nonexistent_view_returns_error() {
        let src: Arc<dyn TopicDataSource> = Arc::new(MockTopicSource);
        let engine = DuckDBEngine::new(src).unwrap();

        let result = engine.drop_materialized_view("no_such_view").await;
        assert!(result.is_err());
    }

    // ── Large result set ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_max_rows_limit() {
        let src: Arc<dyn TopicDataSource> = Arc::new(PopulatedMockSource);
        let engine = DuckDBEngine::new(src).unwrap();

        let result = engine
            .execute_query(
                "SELECT * FROM streamline_topic_events",
                QueryOptions {
                    max_rows: Some(2),
                    enable_cache: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 2);
        assert!(result.has_more);
    }
}
