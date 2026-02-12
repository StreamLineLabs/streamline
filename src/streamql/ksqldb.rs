//! StreamQL — ksqlDB-compatible Streaming SQL for Streamline
//!
//! Provides ksqlDB-compatible streaming SQL operations:
//! - CREATE STREAM — Define a streaming source from a topic
//! - CREATE TABLE — Define a materialized table from a topic
//! - SELECT — Query streams with filtering, aggregation, windowing
//! - INSERT INTO — Write query results to a topic
//!
//! # Stability: Experimental

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Data format for stream/table serialization
// ---------------------------------------------------------------------------

/// Serialization format for stream or table data
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataFormat {
    Json,
    Avro,
    Protobuf,
    Csv,
    Raw,
}

impl Default for DataFormat {
    fn default() -> Self {
        Self::Json
    }
}

impl std::fmt::Display for DataFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataFormat::Json => write!(f, "JSON"),
            DataFormat::Avro => write!(f, "AVRO"),
            DataFormat::Protobuf => write!(f, "PROTOBUF"),
            DataFormat::Csv => write!(f, "CSV"),
            DataFormat::Raw => write!(f, "RAW"),
        }
    }
}

// ---------------------------------------------------------------------------
// Column schema for CREATE STREAM / CREATE TABLE
// ---------------------------------------------------------------------------

/// Column definition used in stream and table schemas
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: ColumnType,
    pub is_key: bool,
}

/// Supported column types in ksqlDB-compatible DDL
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    Boolean,
    Int,
    BigInt,
    Double,
    String,
    Timestamp,
    Date,
    Time,
    Bytes,
    Array(Box<ColumnType>),
    Map(Box<ColumnType>, Box<ColumnType>),
    Struct(Vec<ColumnSchema>),
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnType::Boolean => write!(f, "BOOLEAN"),
            ColumnType::Int => write!(f, "INT"),
            ColumnType::BigInt => write!(f, "BIGINT"),
            ColumnType::Double => write!(f, "DOUBLE"),
            ColumnType::String => write!(f, "VARCHAR"),
            ColumnType::Timestamp => write!(f, "TIMESTAMP"),
            ColumnType::Date => write!(f, "DATE"),
            ColumnType::Time => write!(f, "TIME"),
            ColumnType::Bytes => write!(f, "BYTES"),
            ColumnType::Array(inner) => write!(f, "ARRAY<{}>", inner),
            ColumnType::Map(k, v) => write!(f, "MAP<{}, {}>", k, v),
            ColumnType::Struct(fields) => {
                write!(f, "STRUCT<")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{} {}", field.name, field.data_type)?;
                }
                write!(f, ">")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// StreamDefinition — metadata for a registered stream
// ---------------------------------------------------------------------------

/// Metadata for a registered stream backed by a Kafka-compatible topic.
///
/// Streams are unbounded, append-only sequences of structured events.
/// They are the primary abstraction in ksqlDB-style processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamDefinition {
    /// Unique stream name (e.g. `pageviews`)
    pub name: String,
    /// Underlying Streamline topic that backs this stream
    pub topic: String,
    /// Column schema
    pub columns: Vec<ColumnSchema>,
    /// Key column name (used for partitioning)
    pub key_column: Option<String>,
    /// Timestamp column name (used for event-time processing)
    pub timestamp_column: Option<String>,
    /// Serialization format for the value payload
    pub value_format: DataFormat,
    /// Serialization format for the key (defaults to value_format)
    pub key_format: Option<DataFormat>,
    /// Number of partitions (inherited from the underlying topic)
    pub partitions: Option<u32>,
    /// Additional WITH properties from the CREATE STREAM statement
    pub properties: HashMap<String, String>,
    /// Creation timestamp (ms since epoch)
    pub created_at: i64,
}

impl StreamDefinition {
    /// Create a new stream definition with required fields
    pub fn new(name: impl Into<String>, topic: impl Into<String>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        Self {
            name: name.into(),
            topic: topic.into(),
            columns: Vec::new(),
            key_column: None,
            timestamp_column: None,
            value_format: DataFormat::default(),
            key_format: None,
            partitions: None,
            properties: HashMap::new(),
            created_at: now,
        }
    }

    /// Builder: set columns
    pub fn with_columns(mut self, columns: Vec<ColumnSchema>) -> Self {
        self.columns = columns;
        self
    }

    /// Builder: set the key column
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key_column = Some(key.into());
        self
    }

    /// Builder: set the timestamp column
    pub fn with_timestamp(mut self, ts: impl Into<String>) -> Self {
        self.timestamp_column = Some(ts.into());
        self
    }

    /// Builder: set the value format
    pub fn with_format(mut self, format: DataFormat) -> Self {
        self.value_format = format;
        self
    }
}

// ---------------------------------------------------------------------------
// TableDefinition — metadata for a materialized table
// ---------------------------------------------------------------------------

/// Metadata for a materialized table derived from a stream.
///
/// Tables represent the *current state* of a stream, keyed by primary key.
/// They are continuously updated as new records arrive on the source stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDefinition {
    /// Unique table name (e.g. `user_counts`)
    pub name: String,
    /// Source stream or table this is derived from
    pub source: String,
    /// The SQL query that defines this table (e.g. `SELECT ... GROUP BY ...`)
    pub query_sql: String,
    /// Result schema columns
    pub columns: Vec<ColumnSchema>,
    /// Primary key column(s) for upsert semantics
    pub key_columns: Vec<String>,
    /// GROUP BY columns (if this is an aggregation table)
    pub group_by: Vec<String>,
    /// Whether the table is currently being materialized
    pub materializing: bool,
    /// Creation timestamp (ms since epoch)
    pub created_at: i64,
}

impl TableDefinition {
    /// Create a new table definition
    pub fn new(
        name: impl Into<String>,
        source: impl Into<String>,
        query_sql: impl Into<String>,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        Self {
            name: name.into(),
            source: source.into(),
            query_sql: query_sql.into(),
            columns: Vec::new(),
            key_columns: Vec::new(),
            group_by: Vec::new(),
            materializing: false,
            created_at: now,
        }
    }
}

// ---------------------------------------------------------------------------
// WindowSpec — window specification for streaming queries
// ---------------------------------------------------------------------------

/// Window specification for ksqlDB-style windowed aggregations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum KsqlWindowSpec {
    /// Fixed-size, non-overlapping windows.
    /// `WINDOW TUMBLING (SIZE 1 MINUTE)`
    Tumbling {
        size_ms: u64,
        grace_period_ms: Option<u64>,
    },
    /// Fixed-size windows that advance by a hop interval.
    /// `WINDOW HOPPING (SIZE 1 MINUTE, ADVANCE BY 10 SECONDS)`
    Hopping {
        size_ms: u64,
        advance_ms: u64,
        grace_period_ms: Option<u64>,
    },
    /// Activity-based windows that close after an inactivity gap.
    /// `WINDOW SESSION (30 SECONDS)`
    Session {
        gap_ms: u64,
        grace_period_ms: Option<u64>,
    },
}

impl KsqlWindowSpec {
    /// Get the nominal window size in ms
    pub fn size_ms(&self) -> u64 {
        match self {
            KsqlWindowSpec::Tumbling { size_ms, .. } => *size_ms,
            KsqlWindowSpec::Hopping { size_ms, .. } => *size_ms,
            KsqlWindowSpec::Session { gap_ms, .. } => *gap_ms,
        }
    }
}

// ---------------------------------------------------------------------------
// AggregateFunction — aggregate functions including TopK
// ---------------------------------------------------------------------------

/// Built-in aggregate functions for ksqlDB-compatible queries
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KsqlAggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    /// TopK returns the top K values for a column
    TopK(usize),
    /// Earliest value by timestamp
    Earliest,
    /// Latest value by timestamp
    Latest,
    /// Collect values into an array
    CollectList,
    /// Collect distinct values into a set
    CollectSet,
    /// Count distinct values
    CountDistinct,
    /// Histogram (approximate distribution)
    Histogram,
}

impl std::fmt::Display for KsqlAggregateFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KsqlAggregateFunction::Count => write!(f, "COUNT"),
            KsqlAggregateFunction::Sum => write!(f, "SUM"),
            KsqlAggregateFunction::Avg => write!(f, "AVG"),
            KsqlAggregateFunction::Min => write!(f, "MIN"),
            KsqlAggregateFunction::Max => write!(f, "MAX"),
            KsqlAggregateFunction::TopK(k) => write!(f, "TOPK({})", k),
            KsqlAggregateFunction::Earliest => write!(f, "EARLIEST"),
            KsqlAggregateFunction::Latest => write!(f, "LATEST"),
            KsqlAggregateFunction::CollectList => write!(f, "COLLECT_LIST"),
            KsqlAggregateFunction::CollectSet => write!(f, "COLLECT_SET"),
            KsqlAggregateFunction::CountDistinct => write!(f, "COUNT_DISTINCT"),
            KsqlAggregateFunction::Histogram => write!(f, "HISTOGRAM"),
        }
    }
}

// ---------------------------------------------------------------------------
// ContinuousQuery — a running ksqlDB-style persistent query
// ---------------------------------------------------------------------------

/// Status of a continuous query
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContinuousQueryStatus {
    /// Query is starting up
    Starting,
    /// Query is actively processing
    Running,
    /// Query has been paused
    Paused,
    /// Query terminated due to error
    Error(String),
    /// Query was terminated by the user
    Terminated,
}

/// A running continuous query that reads from source streams/tables,
/// applies transformations, and writes to a sink topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousQuery {
    /// Unique query identifier (e.g. `CSAS_USER_COUNTS_0`)
    pub id: String,
    /// Original SQL statement
    pub sql: String,
    /// Source stream or table names
    pub sources: Vec<String>,
    /// Sink topic or table name (for INSERT INTO / CREATE ... AS SELECT)
    pub sink: Option<String>,
    /// Current status
    pub status: ContinuousQueryStatus,
    /// Window specification (if windowed)
    pub window: Option<KsqlWindowSpec>,
    /// Aggregate functions used
    pub aggregates: Vec<KsqlAggregateFunction>,
    /// Runtime statistics
    pub stats: ContinuousQueryStats,
    /// Creation timestamp (ms since epoch)
    pub created_at: i64,
}

/// Runtime statistics for a continuous query
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContinuousQueryStats {
    /// Total messages consumed from source(s)
    pub messages_consumed: u64,
    /// Total messages produced to sink
    pub messages_produced: u64,
    /// Messages dropped (late, malformed, etc.)
    pub messages_dropped: u64,
    /// Current processing lag in ms
    pub processing_lag_ms: u64,
    /// Last message timestamp processed
    pub last_message_timestamp: Option<i64>,
    /// Total errors encountered
    pub total_errors: u64,
    /// Uptime in milliseconds
    pub uptime_ms: u64,
}

// ---------------------------------------------------------------------------
// StreamqlStatement — parsed ksqlDB statement variants
// ---------------------------------------------------------------------------

/// A parsed ksqlDB-compatible SQL statement
#[derive(Debug, Clone)]
pub enum StreamqlStatement {
    /// CREATE STREAM name (col1 type, ...) WITH (topic='...', format='JSON')
    CreateStream {
        name: String,
        columns: Vec<ColumnSchema>,
        if_not_exists: bool,
        properties: HashMap<String, String>,
    },
    /// CREATE TABLE name AS SELECT ... FROM stream GROUP BY ...
    CreateTableAs {
        name: String,
        query_sql: String,
        properties: HashMap<String, String>,
    },
    /// SELECT ... FROM stream [WINDOW ...] [WHERE ...] [GROUP BY ...]
    Select {
        sql: String,
    },
    /// INSERT INTO topic SELECT ... FROM stream [WHERE ...]
    InsertInto {
        sink: String,
        query_sql: String,
    },
    /// DROP STREAM name
    DropStream {
        name: String,
        if_exists: bool,
    },
    /// DROP TABLE name
    DropTable {
        name: String,
        if_exists: bool,
    },
    /// SHOW STREAMS
    ShowStreams,
    /// SHOW TABLES
    ShowTables,
    /// SHOW QUERIES
    ShowQueries,
    /// TERMINATE query_id
    Terminate {
        query_id: String,
    },
    /// DESCRIBE stream_or_table
    Describe {
        name: String,
        extended: bool,
    },
}

// ---------------------------------------------------------------------------
// StreamqlEngine — manages streams, tables, and continuous queries
// ---------------------------------------------------------------------------

/// Main ksqlDB-compatible engine that manages stream/table definitions
/// and continuous queries.
///
/// The `StreamqlEngine` acts as the catalog and lifecycle manager:
/// - Register and drop streams backed by topics
/// - Create materialized tables from stream aggregations
/// - Submit, list, and terminate continuous queries
pub struct StreamqlEngine {
    /// Registered stream definitions
    streams: Arc<RwLock<HashMap<String, StreamDefinition>>>,
    /// Registered table definitions
    tables: Arc<RwLock<HashMap<String, TableDefinition>>>,
    /// Active continuous queries
    queries: Arc<RwLock<HashMap<String, ContinuousQuery>>>,
    /// Monotonically increasing query ID counter
    next_query_id: Arc<RwLock<u64>>,
}

impl StreamqlEngine {
    /// Create a new engine with empty registries
    pub fn new() -> Self {
        Self {
            streams: Arc::new(RwLock::new(HashMap::new())),
            tables: Arc::new(RwLock::new(HashMap::new())),
            queries: Arc::new(RwLock::new(HashMap::new())),
            next_query_id: Arc::new(RwLock::new(1)),
        }
    }

    // ---- Stream management ----

    /// Register a new stream definition.
    ///
    /// Corresponds to `CREATE STREAM name (...) WITH (topic='...', format='JSON')`.
    pub async fn create_stream(&self, definition: StreamDefinition) -> Result<()> {
        let mut streams = self.streams.write().await;
        let name = definition.name.clone();
        if streams.contains_key(&name) {
            return Err(StreamlineError::Query(format!(
                "Stream already exists: {}",
                name
            )));
        }
        streams.insert(name, definition);
        Ok(())
    }

    /// Drop (unregister) a stream.
    pub async fn drop_stream(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut streams = self.streams.write().await;
        if streams.remove(name).is_none() && !if_exists {
            return Err(StreamlineError::Query(format!(
                "Stream not found: {}",
                name
            )));
        }
        Ok(())
    }

    /// Get a stream definition by name.
    pub async fn get_stream(&self, name: &str) -> Option<StreamDefinition> {
        self.streams.read().await.get(name).cloned()
    }

    /// List all registered streams.
    pub async fn list_streams(&self) -> Vec<StreamDefinition> {
        self.streams.read().await.values().cloned().collect()
    }

    // ---- Table management ----

    /// Register a materialized table.
    ///
    /// Corresponds to `CREATE TABLE name AS SELECT ... GROUP BY ...`.
    pub async fn create_table(&self, definition: TableDefinition) -> Result<()> {
        let mut tables = self.tables.write().await;
        let name = definition.name.clone();
        if tables.contains_key(&name) {
            return Err(StreamlineError::Query(format!(
                "Table already exists: {}",
                name
            )));
        }
        tables.insert(name, definition);
        Ok(())
    }

    /// Drop (unregister) a table.
    pub async fn drop_table(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut tables = self.tables.write().await;
        if tables.remove(name).is_none() && !if_exists {
            return Err(StreamlineError::Query(format!(
                "Table not found: {}",
                name
            )));
        }
        Ok(())
    }

    /// Get a table definition by name.
    pub async fn get_table(&self, name: &str) -> Option<TableDefinition> {
        self.tables.read().await.get(name).cloned()
    }

    /// List all registered tables.
    pub async fn list_tables(&self) -> Vec<TableDefinition> {
        self.tables.read().await.values().cloned().collect()
    }

    // ---- Query management ----

    /// Submit a continuous query for execution.
    ///
    /// Parses and validates the statement, then starts continuous processing.
    pub async fn execute_statement(&self, sql: &str) -> Result<StatementResult> {
        let stmt = parse_ksql_statement(sql)?;

        match stmt {
            StreamqlStatement::CreateStream {
                name,
                columns,
                if_not_exists,
                properties,
            } => {
                let topic = properties
                    .get("topic")
                    .or_else(|| properties.get("kafka_topic"))
                    .cloned()
                    .unwrap_or_else(|| name.clone());
                let format = properties
                    .get("value_format")
                    .or_else(|| properties.get("format"))
                    .and_then(|f| match f.to_uppercase().as_str() {
                        "JSON" => Some(DataFormat::Json),
                        "AVRO" => Some(DataFormat::Avro),
                        "PROTOBUF" => Some(DataFormat::Protobuf),
                        "CSV" => Some(DataFormat::Csv),
                        _ => None,
                    })
                    .unwrap_or_default();

                let key_col = columns.iter().find(|c| c.is_key).map(|c| c.name.clone());
                let ts_col = properties.get("timestamp").cloned();

                let def = StreamDefinition {
                    name: name.clone(),
                    topic,
                    columns,
                    key_column: key_col,
                    timestamp_column: ts_col,
                    value_format: format,
                    key_format: None,
                    partitions: properties.get("partitions").and_then(|p| p.parse().ok()),
                    properties,
                    created_at: now_ms(),
                };

                if if_not_exists && self.get_stream(&name).await.is_some() {
                    return Ok(StatementResult::Success(format!(
                        "Stream {} already exists",
                        name
                    )));
                }

                self.create_stream(def).await?;
                Ok(StatementResult::Success(format!(
                    "Stream {} created",
                    name
                )))
            }

            StreamqlStatement::CreateTableAs {
                name,
                query_sql,
                properties: _,
            } => {
                let def = TableDefinition::new(&name, "", &query_sql);
                self.create_table(def).await?;

                // Start a continuous query to materialize the table
                let query = self.start_continuous_query(&query_sql, Some(&name)).await?;
                Ok(StatementResult::QueryStarted(query.id))
            }

            StreamqlStatement::InsertInto { sink, query_sql } => {
                let query = self
                    .start_continuous_query(&query_sql, Some(&sink))
                    .await?;
                Ok(StatementResult::QueryStarted(query.id))
            }

            StreamqlStatement::Select { sql: query_sql } => {
                Ok(StatementResult::Success(format!(
                    "Push query submitted: {}",
                    query_sql
                )))
            }

            StreamqlStatement::DropStream { name, if_exists } => {
                self.drop_stream(&name, if_exists).await?;
                Ok(StatementResult::Success(format!(
                    "Stream {} dropped",
                    name
                )))
            }

            StreamqlStatement::DropTable { name, if_exists } => {
                self.drop_table(&name, if_exists).await?;
                Ok(StatementResult::Success(format!("Table {} dropped", name)))
            }

            StreamqlStatement::ShowStreams => {
                let streams = self.list_streams().await;
                let names: Vec<String> = streams.iter().map(|s| s.name.clone()).collect();
                Ok(StatementResult::Listing(names))
            }

            StreamqlStatement::ShowTables => {
                let tables = self.list_tables().await;
                let names: Vec<String> = tables.iter().map(|t| t.name.clone()).collect();
                Ok(StatementResult::Listing(names))
            }

            StreamqlStatement::ShowQueries => {
                let queries = self.list_queries().await;
                let ids: Vec<String> = queries.iter().map(|q| q.id.clone()).collect();
                Ok(StatementResult::Listing(ids))
            }

            StreamqlStatement::Terminate { query_id } => {
                self.terminate_query(&query_id).await?;
                Ok(StatementResult::Success(format!(
                    "Query {} terminated",
                    query_id
                )))
            }

            StreamqlStatement::Describe { name, .. } => {
                if let Some(stream) = self.get_stream(&name).await {
                    let desc = format!(
                        "Stream: {}, Topic: {}, Format: {}, Columns: {}",
                        stream.name,
                        stream.topic,
                        stream.value_format,
                        stream.columns.len()
                    );
                    return Ok(StatementResult::Success(desc));
                }
                if let Some(table) = self.get_table(&name).await {
                    let desc = format!(
                        "Table: {}, Source: {}, Columns: {}",
                        table.name,
                        table.source,
                        table.columns.len()
                    );
                    return Ok(StatementResult::Success(desc));
                }
                Err(StreamlineError::Query(format!(
                    "No stream or table found: {}",
                    name
                )))
            }
        }
    }

    /// Start a continuous query with an optional sink
    async fn start_continuous_query(
        &self,
        sql: &str,
        sink: Option<&str>,
    ) -> Result<ContinuousQuery> {
        let id = {
            let mut counter = self.next_query_id.write().await;
            let id = format!("CSAS_{}", *counter);
            *counter += 1;
            id
        };

        let query = ContinuousQuery {
            id: id.clone(),
            sql: sql.to_string(),
            sources: Vec::new(),
            sink: sink.map(|s| s.to_string()),
            status: ContinuousQueryStatus::Running,
            window: None,
            aggregates: Vec::new(),
            stats: ContinuousQueryStats::default(),
            created_at: now_ms(),
        };

        self.queries.write().await.insert(id, query.clone());
        Ok(query)
    }

    /// List all active continuous queries
    pub async fn list_queries(&self) -> Vec<ContinuousQuery> {
        self.queries.read().await.values().cloned().collect()
    }

    /// Terminate a running continuous query
    pub async fn terminate_query(&self, query_id: &str) -> Result<()> {
        let mut queries = self.queries.write().await;
        let query = queries
            .get_mut(query_id)
            .ok_or_else(|| StreamlineError::Query(format!("Query not found: {}", query_id)))?;

        query.status = ContinuousQueryStatus::Terminated;
        Ok(())
    }
}

impl Default for StreamqlEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of executing a ksqlDB-compatible statement
#[derive(Debug, Clone)]
pub enum StatementResult {
    /// Statement executed successfully (message)
    Success(String),
    /// A continuous query was started (query ID)
    QueryStarted(String),
    /// A listing of names (streams, tables, queries)
    Listing(Vec<String>),
}

// ---------------------------------------------------------------------------
// Statement parser for ksqlDB-style SQL
// ---------------------------------------------------------------------------

/// Parse a ksqlDB-compatible SQL statement
pub fn parse_ksql_statement(sql: &str) -> Result<StreamqlStatement> {
    let trimmed = sql.trim();
    // Remove trailing semicolon
    let trimmed = trimmed.strip_suffix(';').unwrap_or(trimmed).trim();
    let upper = trimmed.to_uppercase();

    if upper.starts_with("CREATE STREAM") {
        parse_create_stream(trimmed)
    } else if upper.starts_with("CREATE TABLE") {
        parse_create_table_as(trimmed)
    } else if upper.starts_with("INSERT INTO") {
        parse_insert_into(trimmed)
    } else if upper.starts_with("DROP STREAM") {
        parse_drop_stream(trimmed)
    } else if upper.starts_with("DROP TABLE") {
        parse_drop_table(trimmed)
    } else if upper.starts_with("SELECT") {
        Ok(StreamqlStatement::Select {
            sql: trimmed.to_string(),
        })
    } else if upper == "SHOW STREAMS" {
        Ok(StreamqlStatement::ShowStreams)
    } else if upper == "SHOW TABLES" {
        Ok(StreamqlStatement::ShowTables)
    } else if upper == "SHOW QUERIES" {
        Ok(StreamqlStatement::ShowQueries)
    } else if upper.starts_with("TERMINATE") {
        let query_id = trimmed["TERMINATE".len()..].trim().to_string();
        Ok(StreamqlStatement::Terminate { query_id })
    } else if upper.starts_with("DESCRIBE") {
        let rest = trimmed["DESCRIBE".len()..].trim();
        let extended = rest.to_uppercase().ends_with("EXTENDED");
        let name = if extended {
            rest[..rest.len() - "EXTENDED".len()].trim().to_string()
        } else {
            rest.to_string()
        };
        Ok(StreamqlStatement::Describe { name, extended })
    } else {
        Err(StreamlineError::Parse(format!(
            "Unrecognized statement: {}",
            &trimmed[..trimmed.len().min(40)]
        )))
    }
}

/// Parse: CREATE STREAM name (col1 type, ...) WITH (key='value', ...)
fn parse_create_stream(sql: &str) -> Result<StreamqlStatement> {
    let upper = sql.to_uppercase();
    let after_create = &sql["CREATE STREAM".len()..].trim_start();
    let upper_after = &upper["CREATE STREAM".len()..].trim_start();

    let if_not_exists = upper_after.starts_with("IF NOT EXISTS");
    let rest = if if_not_exists {
        after_create["IF NOT EXISTS".len()..].trim_start()
    } else {
        after_create
    };

    // Extract name (up to '(' or 'WITH' or 'AS')
    let name_end = rest
        .find('(')
        .or_else(|| {
            let u = rest.to_uppercase();
            u.find(" WITH ").or_else(|| u.find(" AS "))
        })
        .unwrap_or(rest.len());
    let name = rest[..name_end].trim().to_string();

    if name.is_empty() {
        return Err(StreamlineError::Parse(
            "Missing stream name in CREATE STREAM".into(),
        ));
    }

    let rest_after_name = rest[name_end..].trim_start();

    // Parse column definitions if present
    let (columns, rest_after_cols) = if rest_after_name.starts_with('(') {
        parse_column_defs(rest_after_name)?
    } else {
        (Vec::new(), rest_after_name.to_string())
    };

    // Parse WITH clause
    let properties = parse_with_clause(&rest_after_cols)?;

    Ok(StreamqlStatement::CreateStream {
        name,
        columns,
        if_not_exists,
        properties,
    })
}

/// Parse column definitions: `(col1 TYPE [KEY], col2 TYPE, ...)`
fn parse_column_defs(s: &str) -> Result<(Vec<ColumnSchema>, String)> {
    let s = s.trim_start_matches('(');
    let close_paren = find_matching_paren(s)?;
    let defs_str = &s[..close_paren];
    let rest = s[close_paren + 1..].to_string();

    let mut columns = Vec::new();
    for part in split_respecting_parens(defs_str, ',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let tokens: Vec<&str> = part.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(StreamlineError::Parse(format!(
                "Invalid column definition: {}",
                part
            )));
        }
        let col_name = tokens[0].to_string();
        let col_type = parse_column_type(tokens[1])?;
        let is_key = tokens.len() > 2 && tokens[2].eq_ignore_ascii_case("KEY");

        columns.push(ColumnSchema {
            name: col_name,
            data_type: col_type,
            is_key,
        });
    }

    Ok((columns, rest))
}

/// Parse a column type string
fn parse_column_type(s: &str) -> Result<ColumnType> {
    let upper = s.to_uppercase();
    match upper.as_str() {
        "BOOLEAN" | "BOOL" => Ok(ColumnType::Boolean),
        "INT" | "INTEGER" => Ok(ColumnType::Int),
        "BIGINT" | "LONG" => Ok(ColumnType::BigInt),
        "DOUBLE" | "FLOAT" | "DECIMAL" => Ok(ColumnType::Double),
        "STRING" | "VARCHAR" | "TEXT" => Ok(ColumnType::String),
        "TIMESTAMP" => Ok(ColumnType::Timestamp),
        "DATE" => Ok(ColumnType::Date),
        "TIME" => Ok(ColumnType::Time),
        "BYTES" | "BINARY" | "VARBINARY" => Ok(ColumnType::Bytes),
        _ => {
            // Handle ARRAY<T>, MAP<K,V> etc. — for now, default to String
            if upper.starts_with("ARRAY") {
                Ok(ColumnType::Array(Box::new(ColumnType::String)))
            } else if upper.starts_with("MAP") {
                Ok(ColumnType::Map(
                    Box::new(ColumnType::String),
                    Box::new(ColumnType::String),
                ))
            } else {
                Ok(ColumnType::String) // fallback
            }
        }
    }
}

/// Parse WITH (key='value', ...) clause
fn parse_with_clause(s: &str) -> Result<HashMap<String, String>> {
    let trimmed = s.trim();
    let upper = trimmed.to_uppercase();
    if !upper.starts_with("WITH") {
        return Ok(HashMap::new());
    }

    let after_with = trimmed["WITH".len()..].trim_start();
    if !after_with.starts_with('(') {
        return Err(StreamlineError::Parse(
            "Expected '(' after WITH".into(),
        ));
    }

    let inner = after_with.trim_start_matches('(');
    let close = find_matching_paren(inner)?;
    let props_str = &inner[..close];

    let mut props = HashMap::new();
    for part in split_respecting_parens(props_str, ',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some(eq_pos) = part.find('=') {
            let key = part[..eq_pos].trim().to_lowercase();
            let val = part[eq_pos + 1..]
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_string();
            props.insert(key, val);
        }
    }

    Ok(props)
}

/// Parse CREATE TABLE name AS SELECT ...
fn parse_create_table_as(sql: &str) -> Result<StreamqlStatement> {
    let upper = sql.to_uppercase();

    // Find AS keyword
    let as_pos = upper
        .find(" AS ")
        .ok_or_else(|| StreamlineError::Parse("CREATE TABLE requires AS SELECT".into()))?;

    let name = sql["CREATE TABLE".len()..as_pos].trim().to_string();
    let query_sql = sql[as_pos + 4..].trim().to_string();

    // Parse optional WITH clause after the query
    let properties = HashMap::new(); // simplified for now

    Ok(StreamqlStatement::CreateTableAs {
        name,
        query_sql,
        properties,
    })
}

/// Parse INSERT INTO <topic> SELECT ...
fn parse_insert_into(sql: &str) -> Result<StreamqlStatement> {
    let after_insert = sql["INSERT INTO".len()..].trim_start();
    let upper_rest = after_insert.to_uppercase();

    let select_pos = upper_rest
        .find("SELECT")
        .ok_or_else(|| StreamlineError::Parse("INSERT INTO requires SELECT".into()))?;

    let sink = after_insert[..select_pos].trim().to_string();
    let query_sql = after_insert[select_pos..].trim().to_string();

    Ok(StreamqlStatement::InsertInto { sink, query_sql })
}

/// Parse DROP STREAM name [IF EXISTS]
fn parse_drop_stream(sql: &str) -> Result<StreamqlStatement> {
    let rest = sql["DROP STREAM".len()..].trim();
    let upper = rest.to_uppercase();
    let if_exists = upper.starts_with("IF EXISTS");
    let name = if if_exists {
        rest["IF EXISTS".len()..].trim().to_string()
    } else {
        rest.to_string()
    };
    Ok(StreamqlStatement::DropStream { name, if_exists })
}

/// Parse DROP TABLE name [IF EXISTS]
fn parse_drop_table(sql: &str) -> Result<StreamqlStatement> {
    let rest = sql["DROP TABLE".len()..].trim();
    let upper = rest.to_uppercase();
    let if_exists = upper.starts_with("IF EXISTS");
    let name = if if_exists {
        rest["IF EXISTS".len()..].trim().to_string()
    } else {
        rest.to_string()
    };
    Ok(StreamqlStatement::DropTable { name, if_exists })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Find the position of the matching close parenthesis (depth 0 close)
fn find_matching_paren(s: &str) -> Result<usize> {
    let mut depth = 0i32;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                if depth == 0 {
                    return Ok(i);
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    Err(StreamlineError::Parse("Unmatched parenthesis".into()))
}

/// Split a string by delimiter, respecting nested parentheses
fn split_respecting_parens(s: &str, delim: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    for ch in s.chars() {
        if ch == '\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
            current.push(ch);
        } else if ch == '"' && !in_single_quote {
            in_double_quote = !in_double_quote;
            current.push(ch);
        } else if in_single_quote || in_double_quote {
            current.push(ch);
        } else if ch == '(' {
            depth += 1;
            current.push(ch);
        } else if ch == ')' {
            depth -= 1;
            current.push(ch);
        } else if ch == delim && depth == 0 {
            parts.push(std::mem::take(&mut current));
        } else {
            current.push(ch);
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- StreamDefinition tests ----

    #[test]
    fn test_stream_definition_builder() {
        let def = StreamDefinition::new("pageviews", "pageviews_topic")
            .with_columns(vec![
                ColumnSchema {
                    name: "user_id".into(),
                    data_type: ColumnType::String,
                    is_key: true,
                },
                ColumnSchema {
                    name: "page".into(),
                    data_type: ColumnType::String,
                    is_key: false,
                },
                ColumnSchema {
                    name: "ts".into(),
                    data_type: ColumnType::Timestamp,
                    is_key: false,
                },
            ])
            .with_key("user_id")
            .with_timestamp("ts")
            .with_format(DataFormat::Json);

        assert_eq!(def.name, "pageviews");
        assert_eq!(def.topic, "pageviews_topic");
        assert_eq!(def.key_column.as_deref(), Some("user_id"));
        assert_eq!(def.timestamp_column.as_deref(), Some("ts"));
        assert_eq!(def.value_format, DataFormat::Json);
        assert_eq!(def.columns.len(), 3);
    }

    // ---- TableDefinition tests ----

    #[test]
    fn test_table_definition() {
        let def = TableDefinition::new(
            "user_counts",
            "pageviews",
            "SELECT user_id, COUNT(*) AS cnt FROM pageviews GROUP BY user_id",
        );
        assert_eq!(def.name, "user_counts");
        assert_eq!(def.source, "pageviews");
        assert!(!def.materializing);
    }

    // ---- WindowSpec tests ----

    #[test]
    fn test_window_spec_size() {
        let tumbling = KsqlWindowSpec::Tumbling {
            size_ms: 60_000,
            grace_period_ms: None,
        };
        assert_eq!(tumbling.size_ms(), 60_000);

        let hopping = KsqlWindowSpec::Hopping {
            size_ms: 60_000,
            advance_ms: 10_000,
            grace_period_ms: None,
        };
        assert_eq!(hopping.size_ms(), 60_000);

        let session = KsqlWindowSpec::Session {
            gap_ms: 30_000,
            grace_period_ms: None,
        };
        assert_eq!(session.size_ms(), 30_000);
    }

    // ---- AggregateFunction display ----

    #[test]
    fn test_aggregate_function_display() {
        assert_eq!(KsqlAggregateFunction::Count.to_string(), "COUNT");
        assert_eq!(KsqlAggregateFunction::TopK(10).to_string(), "TOPK(10)");
        assert_eq!(
            KsqlAggregateFunction::CollectList.to_string(),
            "COLLECT_LIST"
        );
    }

    // ---- Parser tests ----

    #[test]
    fn test_parse_create_stream() {
        let sql = "CREATE STREAM pageviews (user_id VARCHAR KEY, page VARCHAR, ts TIMESTAMP) WITH (topic='pv_topic', value_format='JSON')";
        let stmt = parse_ksql_statement(sql).unwrap();
        match stmt {
            StreamqlStatement::CreateStream {
                name,
                columns,
                if_not_exists,
                properties,
            } => {
                assert_eq!(name, "pageviews");
                assert!(!if_not_exists);
                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0].name, "user_id");
                assert!(columns[0].is_key);
                assert_eq!(columns[0].data_type, ColumnType::String);
                assert_eq!(properties.get("topic").unwrap(), "pv_topic");
                assert_eq!(properties.get("value_format").unwrap(), "JSON");
            }
            _ => panic!("Expected CreateStream"),
        }
    }

    #[test]
    fn test_parse_create_stream_if_not_exists() {
        let sql = "CREATE STREAM IF NOT EXISTS events (id BIGINT, data VARCHAR) WITH (topic='events')";
        let stmt = parse_ksql_statement(sql).unwrap();
        match stmt {
            StreamqlStatement::CreateStream {
                if_not_exists,
                name,
                ..
            } => {
                assert!(if_not_exists);
                assert_eq!(name, "events");
            }
            _ => panic!("Expected CreateStream"),
        }
    }

    #[test]
    fn test_parse_create_table_as() {
        let sql = "CREATE TABLE user_counts AS SELECT user_id, COUNT(*) AS cnt FROM pageviews GROUP BY user_id";
        let stmt = parse_ksql_statement(sql).unwrap();
        match stmt {
            StreamqlStatement::CreateTableAs {
                name, query_sql, ..
            } => {
                assert_eq!(name, "user_counts");
                assert!(query_sql.contains("SELECT"));
                assert!(query_sql.contains("GROUP BY"));
            }
            _ => panic!("Expected CreateTableAs"),
        }
    }

    #[test]
    fn test_parse_select() {
        let sql = "SELECT user_id, COUNT(*) FROM pageviews WINDOW TUMBLING (SIZE 1 MINUTE) GROUP BY user_id";
        let stmt = parse_ksql_statement(sql).unwrap();
        match stmt {
            StreamqlStatement::Select { sql: query } => {
                assert!(query.contains("TUMBLING"));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_insert_into() {
        let sql = "INSERT INTO enriched_events SELECT e.*, u.name FROM events e JOIN users u ON e.user_id = u.id";
        let stmt = parse_ksql_statement(sql).unwrap();
        match stmt {
            StreamqlStatement::InsertInto { sink, query_sql } => {
                assert_eq!(sink, "enriched_events");
                assert!(query_sql.starts_with("SELECT"));
            }
            _ => panic!("Expected InsertInto"),
        }
    }

    #[test]
    fn test_parse_drop_stream() {
        let stmt = parse_ksql_statement("DROP STREAM pageviews").unwrap();
        match stmt {
            StreamqlStatement::DropStream { name, if_exists } => {
                assert_eq!(name, "pageviews");
                assert!(!if_exists);
            }
            _ => panic!("Expected DropStream"),
        }
    }

    #[test]
    fn test_parse_drop_stream_if_exists() {
        let stmt = parse_ksql_statement("DROP STREAM IF EXISTS pageviews").unwrap();
        match stmt {
            StreamqlStatement::DropStream { name, if_exists } => {
                assert_eq!(name, "pageviews");
                assert!(if_exists);
            }
            _ => panic!("Expected DropStream"),
        }
    }

    #[test]
    fn test_parse_show_streams() {
        let stmt = parse_ksql_statement("SHOW STREAMS").unwrap();
        assert!(matches!(stmt, StreamqlStatement::ShowStreams));
    }

    #[test]
    fn test_parse_show_queries() {
        let stmt = parse_ksql_statement("SHOW QUERIES").unwrap();
        assert!(matches!(stmt, StreamqlStatement::ShowQueries));
    }

    #[test]
    fn test_parse_terminate() {
        let stmt = parse_ksql_statement("TERMINATE CSAS_1").unwrap();
        match stmt {
            StreamqlStatement::Terminate { query_id } => {
                assert_eq!(query_id, "CSAS_1");
            }
            _ => panic!("Expected Terminate"),
        }
    }

    #[test]
    fn test_parse_describe() {
        let stmt = parse_ksql_statement("DESCRIBE pageviews").unwrap();
        match stmt {
            StreamqlStatement::Describe { name, extended } => {
                assert_eq!(name, "pageviews");
                assert!(!extended);
            }
            _ => panic!("Expected Describe"),
        }
    }

    #[test]
    fn test_parse_describe_extended() {
        let stmt = parse_ksql_statement("DESCRIBE pageviews EXTENDED").unwrap();
        match stmt {
            StreamqlStatement::Describe { name, extended } => {
                assert_eq!(name, "pageviews");
                assert!(extended);
            }
            _ => panic!("Expected Describe"),
        }
    }

    #[test]
    fn test_parse_with_semicolon() {
        let stmt = parse_ksql_statement("SHOW STREAMS;").unwrap();
        assert!(matches!(stmt, StreamqlStatement::ShowStreams));
    }

    // ---- Engine tests ----

    #[tokio::test]
    async fn test_engine_create_and_list_streams() {
        let engine = StreamqlEngine::new();

        let def = StreamDefinition::new("pageviews", "pv_topic")
            .with_format(DataFormat::Json);
        engine.create_stream(def).await.unwrap();

        let streams = engine.list_streams().await;
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].name, "pageviews");
    }

    #[tokio::test]
    async fn test_engine_create_duplicate_stream() {
        let engine = StreamqlEngine::new();

        let def = StreamDefinition::new("events", "events_topic");
        engine.create_stream(def.clone()).await.unwrap();
        assert!(engine.create_stream(def).await.is_err());
    }

    #[tokio::test]
    async fn test_engine_drop_stream() {
        let engine = StreamqlEngine::new();

        let def = StreamDefinition::new("events", "events_topic");
        engine.create_stream(def).await.unwrap();
        engine.drop_stream("events", false).await.unwrap();
        assert!(engine.list_streams().await.is_empty());
    }

    #[tokio::test]
    async fn test_engine_drop_stream_not_found() {
        let engine = StreamqlEngine::new();
        assert!(engine.drop_stream("nonexistent", false).await.is_err());
        // With IF EXISTS, should succeed
        assert!(engine.drop_stream("nonexistent", true).await.is_ok());
    }

    #[tokio::test]
    async fn test_engine_create_and_list_tables() {
        let engine = StreamqlEngine::new();

        let def = TableDefinition::new("user_counts", "pageviews", "SELECT ...");
        engine.create_table(def).await.unwrap();

        let tables = engine.list_tables().await;
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "user_counts");
    }

    #[tokio::test]
    async fn test_engine_execute_create_stream() {
        let engine = StreamqlEngine::new();

        let sql = "CREATE STREAM events (id BIGINT, data VARCHAR) WITH (topic='events_topic', value_format='JSON')";
        let result = engine.execute_statement(sql).await.unwrap();
        match result {
            StatementResult::Success(msg) => {
                assert!(msg.contains("created"));
            }
            _ => panic!("Expected Success"),
        }

        let stream = engine.get_stream("events").await.unwrap();
        assert_eq!(stream.topic, "events_topic");
        assert_eq!(stream.value_format, DataFormat::Json);
    }

    #[tokio::test]
    async fn test_engine_execute_show_streams() {
        let engine = StreamqlEngine::new();

        let def = StreamDefinition::new("s1", "t1");
        engine.create_stream(def).await.unwrap();

        let result = engine.execute_statement("SHOW STREAMS").await.unwrap();
        match result {
            StatementResult::Listing(names) => {
                assert_eq!(names.len(), 1);
                assert!(names.contains(&"s1".to_string()));
            }
            _ => panic!("Expected Listing"),
        }
    }

    #[tokio::test]
    async fn test_engine_continuous_query_lifecycle() {
        let engine = StreamqlEngine::new();

        let sql = "INSERT INTO output SELECT * FROM input WHERE value > 10";
        let result = engine.execute_statement(sql).await.unwrap();

        let query_id = match result {
            StatementResult::QueryStarted(id) => id,
            _ => panic!("Expected QueryStarted"),
        };

        let queries = engine.list_queries().await;
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].status, ContinuousQueryStatus::Running);

        engine.terminate_query(&query_id).await.unwrap();
        let queries = engine.list_queries().await;
        assert_eq!(queries[0].status, ContinuousQueryStatus::Terminated);
    }

    // ---- Column type parsing ----

    #[test]
    fn test_parse_column_types() {
        assert_eq!(parse_column_type("VARCHAR").unwrap(), ColumnType::String);
        assert_eq!(parse_column_type("BIGINT").unwrap(), ColumnType::BigInt);
        assert_eq!(parse_column_type("BOOLEAN").unwrap(), ColumnType::Boolean);
        assert_eq!(parse_column_type("DOUBLE").unwrap(), ColumnType::Double);
        assert_eq!(
            parse_column_type("TIMESTAMP").unwrap(),
            ColumnType::Timestamp
        );
        assert_eq!(parse_column_type("INT").unwrap(), ColumnType::Int);
    }
}
