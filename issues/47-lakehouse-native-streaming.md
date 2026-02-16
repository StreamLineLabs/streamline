# Lakehouse-Native Streaming

## Summary

Unify streaming and analytics in a single platform by adding native Parquet/Arrow columnar storage, materialized views with incremental refresh, SQL pushdown to the storage layer, and Zero-ETL direct streaming to warehouse formats. This positions Streamline as "Kafka + Data Warehouse in one binary."

## Current State

- Iceberg sink connector exists (`src/sink/iceberg.rs`)
- Delta Lake sink connector exists (`src/sink/delta.rs`)
- DuckDB analytics integration (`src/analytics/duckdb.rs`)
- Arrow columnar storage partially implemented (`src/storage/arrow.rs`)
- Time-travel queries exist (`src/storage/time_travel.rs`)
- StreamQL provides SQL-like querying (`src/streamql/`)
- No native Parquet segment storage
- No materialized views
- No SQL pushdown optimization
- No incremental view maintenance

## Requirements

### Core Lakehouse Features

| Requirement | Priority | Description |
|-------------|----------|-------------|
| Parquet Segments | P0 | Store data natively in Parquet format |
| Columnar Queries | P0 | Query specific columns efficiently |
| Materialized Views | P0 | Precomputed query results with refresh |
| Incremental Refresh | P0 | Update MVs incrementally on new data |
| SQL Pushdown | P1 | Push filters/projections to storage |
| Time-Travel Queries | P1 | Query historical snapshots |
| Zero-ETL Export | P1 | Direct export to warehouse formats |
| Partition Pruning | P1 | Skip irrelevant partitions |
| Statistics | P2 | Column min/max/count for optimization |
| Z-Order Clustering | P2 | Optimize data layout for queries |

### Data Structures

Create `src/lakehouse/mod.rs`:

```rust
//! Lakehouse-Native Streaming Module
//!
//! Provides columnar storage, materialized views, and SQL pushdown
//! for unified streaming and analytics.

pub mod parquet_segment;
pub mod materialized_view;
pub mod query_optimizer;
pub mod incremental;
pub mod statistics;
pub mod partitioning;
pub mod export;

/// Lakehouse configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LakehouseConfig {
    /// Enable Parquet storage format
    pub parquet_enabled: bool,
    /// Parquet configuration
    pub parquet: ParquetConfig,
    /// Materialized view settings
    pub materialized_views: MaterializedViewConfig,
    /// Query optimization settings
    pub query_optimizer: QueryOptimizerConfig,
    /// Statistics collection
    pub statistics: StatisticsConfig,
}

/// Parquet storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetConfig {
    /// Row group size
    pub row_group_size: usize,
    /// Compression codec
    pub compression: ParquetCompression,
    /// Enable dictionary encoding
    pub dictionary_enabled: bool,
    /// Enable bloom filters
    pub bloom_filter_enabled: bool,
    /// Target file size
    pub target_file_size: usize,
    /// Enable statistics
    pub statistics_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParquetCompression {
    None,
    Snappy,
    Gzip,
    Lz4,
    Zstd,
    Brotli,
}

/// Materialized view configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedViewConfig {
    /// Enable materialized views
    pub enabled: bool,
    /// Default refresh mode
    pub default_refresh: RefreshMode,
    /// Max concurrent refreshes
    pub max_concurrent_refreshes: usize,
    /// Storage location for MVs
    pub storage_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RefreshMode {
    /// Refresh on every message
    Immediate,
    /// Refresh periodically
    Periodic { interval_secs: u64 },
    /// Refresh on demand
    Manual,
    /// Refresh when queried (lazy)
    OnQuery { staleness_threshold_secs: u64 },
}
```

Create `src/lakehouse/parquet_segment.rs`:

```rust
//! Parquet Segment Storage
//!
//! Stores stream data in Parquet format for efficient analytics.

use arrow::array::RecordBatch;
use parquet::arrow::ArrowWriter;

/// Parquet-based segment storage
pub struct ParquetSegment {
    /// Segment ID
    id: SegmentId,
    /// Parquet file path
    path: PathBuf,
    /// Arrow schema
    schema: Arc<Schema>,
    /// Row group metadata
    row_groups: Vec<RowGroupMetadata>,
    /// Column statistics
    statistics: ColumnStatistics,
    /// Bloom filters (per column)
    bloom_filters: HashMap<String, BloomFilter>,
}

/// Segment writer for Parquet format
pub struct ParquetSegmentWriter {
    /// Arrow writer
    writer: ArrowWriter<File>,
    /// Buffer of pending records
    buffer: Vec<RecordBatch>,
    /// Current row count
    row_count: usize,
    /// Target file size
    target_size: usize,
}

impl ParquetSegmentWriter {
    /// Write a record to the segment
    pub fn write(&mut self, record: &Record) -> Result<()>;

    /// Write a batch of records
    pub fn write_batch(&mut self, batch: RecordBatch) -> Result<()>;

    /// Flush and close the segment
    pub fn close(self) -> Result<ParquetSegment>;
}

/// Segment reader with predicate pushdown
pub struct ParquetSegmentReader {
    /// Parquet reader
    reader: ParquetRecordBatchReader,
    /// Projection (columns to read)
    projection: Option<Vec<String>>,
    /// Row filter predicate
    predicate: Option<Predicate>,
}

impl ParquetSegmentReader {
    /// Create reader with projection
    pub fn with_projection(segment: &ParquetSegment, columns: &[&str]) -> Result<Self>;

    /// Create reader with predicate
    pub fn with_predicate(segment: &ParquetSegment, predicate: Predicate) -> Result<Self>;

    /// Read next batch
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>>;

    /// Read all matching records
    pub fn read_all(&mut self) -> Result<Vec<RecordBatch>>;
}

/// Column statistics for query optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    pub columns: HashMap<String, ColumnStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    pub name: String,
    pub data_type: DataType,
    pub null_count: u64,
    pub distinct_count: Option<u64>,
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
    pub total_size_bytes: u64,
}
```

Create `src/lakehouse/materialized_view.rs`:

```rust
//! Materialized Views
//!
//! Precomputed query results with incremental maintenance.

/// Materialized view definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedView {
    /// View name
    pub name: String,
    /// Source query
    pub query: String,
    /// Parsed AST
    pub ast: Statement,
    /// Source topics
    pub sources: Vec<String>,
    /// Refresh configuration
    pub refresh: RefreshMode,
    /// Last refresh timestamp
    pub last_refresh: Option<DateTime<Utc>>,
    /// Watermark (last processed offset per source)
    pub watermarks: HashMap<String, HashMap<i32, i64>>,
    /// Schema of the view
    pub schema: Schema,
    /// Storage format
    pub storage_format: StorageFormat,
}

/// Materialized view manager
pub struct MaterializedViewManager {
    /// Active views
    views: DashMap<String, MaterializedView>,
    /// View storage
    storage: Arc<dyn ViewStorage>,
    /// Query executor
    executor: Arc<StreamQLExecutor>,
    /// Refresh scheduler
    scheduler: RefreshScheduler,
    /// Incremental engine
    incremental: IncrementalEngine,
}

impl MaterializedViewManager {
    /// Create a new materialized view
    pub async fn create_view(&self, definition: CreateViewRequest) -> Result<MaterializedView>;

    /// Drop a materialized view
    pub async fn drop_view(&self, name: &str) -> Result<()>;

    /// Refresh a view
    pub async fn refresh(&self, name: &str) -> Result<RefreshResult>;

    /// Query a materialized view
    pub async fn query(&self, name: &str, query: Option<&str>) -> Result<QueryResult>;

    /// Get view status
    pub fn status(&self, name: &str) -> Option<ViewStatus>;

    /// List all views
    pub fn list_views(&self) -> Vec<ViewInfo>;

    /// Process new records for incremental refresh
    pub async fn process_records(&self, topic: &str, records: &[Record]) -> Result<()>;
}

/// Incremental view maintenance
pub struct IncrementalEngine {
    /// Delta processors per view
    processors: DashMap<String, DeltaProcessor>,
}

impl IncrementalEngine {
    /// Process delta (new records) for a view
    pub async fn process_delta(
        &self,
        view: &MaterializedView,
        delta: Vec<Record>,
    ) -> Result<ViewDelta>;

    /// Merge delta into view
    pub async fn merge_delta(
        &self,
        view: &mut MaterializedView,
        delta: ViewDelta,
    ) -> Result<()>;
}

/// View refresh result
#[derive(Debug, Clone)]
pub struct RefreshResult {
    pub view_name: String,
    pub records_processed: u64,
    pub duration: Duration,
    pub refresh_type: RefreshType,
    pub watermarks: HashMap<String, HashMap<i32, i64>>,
}

#[derive(Debug, Clone, Copy)]
pub enum RefreshType {
    Full,
    Incremental,
}
```

Create `src/lakehouse/query_optimizer.rs`:

```rust
//! Query Optimizer with Pushdown
//!
//! Optimizes queries by pushing operations to the storage layer.

/// Query optimizer
pub struct QueryOptimizer {
    /// Statistics manager
    statistics: Arc<StatisticsManager>,
    /// Cost model
    cost_model: CostModel,
}

impl QueryOptimizer {
    /// Optimize a query plan
    pub fn optimize(&self, plan: LogicalPlan) -> Result<PhysicalPlan>;

    /// Apply predicate pushdown
    fn pushdown_predicates(&self, plan: &mut LogicalPlan) -> Result<()>;

    /// Apply projection pushdown
    fn pushdown_projections(&self, plan: &mut LogicalPlan) -> Result<()>;

    /// Apply partition pruning
    fn prune_partitions(&self, plan: &mut LogicalPlan) -> Result<()>;

    /// Estimate plan cost
    fn estimate_cost(&self, plan: &LogicalPlan) -> Cost;
}

/// Predicate that can be pushed to storage
#[derive(Debug, Clone)]
pub enum Predicate {
    /// Column equals value
    Eq { column: String, value: Value },
    /// Column not equals value
    Ne { column: String, value: Value },
    /// Column less than value
    Lt { column: String, value: Value },
    /// Column less than or equal
    Le { column: String, value: Value },
    /// Column greater than value
    Gt { column: String, value: Value },
    /// Column greater than or equal
    Ge { column: String, value: Value },
    /// Column is null
    IsNull { column: String },
    /// Column is not null
    IsNotNull { column: String },
    /// Column in list
    In { column: String, values: Vec<Value> },
    /// Column between values
    Between { column: String, low: Value, high: Value },
    /// String starts with
    StartsWith { column: String, prefix: String },
    /// AND of predicates
    And(Vec<Predicate>),
    /// OR of predicates
    Or(Vec<Predicate>),
    /// NOT predicate
    Not(Box<Predicate>),
}

impl Predicate {
    /// Check if predicate can use statistics to skip row group
    pub fn can_skip(&self, stats: &ColumnStats) -> bool;

    /// Check if predicate can use bloom filter
    pub fn can_use_bloom_filter(&self) -> bool;
}

/// Physical execution plan
#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    /// Plan nodes
    pub root: PhysicalPlanNode,
    /// Estimated cost
    pub estimated_cost: Cost,
    /// Estimated row count
    pub estimated_rows: u64,
}

#[derive(Debug, Clone)]
pub enum PhysicalPlanNode {
    /// Scan Parquet segments
    ParquetScan {
        segments: Vec<SegmentId>,
        projection: Option<Vec<String>>,
        predicate: Option<Predicate>,
    },
    /// Filter operation
    Filter {
        input: Box<PhysicalPlanNode>,
        predicate: Predicate,
    },
    /// Projection operation
    Project {
        input: Box<PhysicalPlanNode>,
        columns: Vec<String>,
    },
    /// Aggregation
    Aggregate {
        input: Box<PhysicalPlanNode>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
    },
    /// Sort
    Sort {
        input: Box<PhysicalPlanNode>,
        order_by: Vec<SortExpr>,
    },
    /// Limit
    Limit {
        input: Box<PhysicalPlanNode>,
        limit: usize,
    },
    /// Materialized view scan
    MaterializedViewScan {
        view_name: String,
    },
}
```

Create `src/lakehouse/export.rs`:

```rust
//! Zero-ETL Export
//!
//! Direct export to warehouse formats without intermediate processing.

/// Export manager for Zero-ETL
pub struct ExportManager {
    /// Export jobs
    jobs: DashMap<String, ExportJob>,
    /// Export executors
    executors: Arc<ExportExecutorPool>,
}

/// Export job definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportJob {
    /// Job ID
    pub id: String,
    /// Source topic
    pub source_topic: String,
    /// Export format
    pub format: ExportFormat,
    /// Destination
    pub destination: ExportDestination,
    /// Partitioning
    pub partitioning: Option<PartitioningStrategy>,
    /// Schema override
    pub schema: Option<Schema>,
    /// Job status
    pub status: ExportStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExportFormat {
    Parquet,
    Iceberg,
    DeltaLake,
    Hudi,
    Csv,
    Json,
    Avro,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExportDestination {
    /// Local filesystem
    Local { path: PathBuf },
    /// S3
    S3 { bucket: String, prefix: String },
    /// Azure Blob Storage
    Azure { container: String, prefix: String },
    /// GCS
    Gcs { bucket: String, prefix: String },
    /// HDFS
    Hdfs { path: String },
}

impl ExportManager {
    /// Create a new export job
    pub async fn create_job(&self, definition: ExportJobDefinition) -> Result<ExportJob>;

    /// Start an export job
    pub async fn start_job(&self, job_id: &str) -> Result<()>;

    /// Stop an export job
    pub async fn stop_job(&self, job_id: &str) -> Result<()>;

    /// Get job status
    pub fn job_status(&self, job_id: &str) -> Option<ExportStatus>;

    /// Export topic snapshot
    pub async fn export_snapshot(
        &self,
        topic: &str,
        format: ExportFormat,
        destination: ExportDestination,
    ) -> Result<ExportResult>;
}

/// Zero-ETL streaming export
pub struct StreamingExporter {
    /// Export configuration
    config: StreamingExportConfig,
    /// Output writer
    writer: Box<dyn ExportWriter>,
    /// Buffer
    buffer: Vec<RecordBatch>,
    /// Partition tracker
    partitions: PartitionTracker,
}

impl StreamingExporter {
    /// Process incoming records
    pub async fn process(&mut self, records: Vec<Record>) -> Result<()>;

    /// Flush pending data
    pub async fn flush(&mut self) -> Result<FlushResult>;

    /// Commit current state
    pub async fn commit(&mut self) -> Result<CommitResult>;
}
```

### CLI Commands

Add to `src/cli.rs`:

```bash
# Materialized views
streamline-cli mv create <name> "SELECT ..." --refresh periodic --interval 60s
streamline-cli mv list
streamline-cli mv describe <name>
streamline-cli mv refresh <name>
streamline-cli mv drop <name>
streamline-cli mv query <name> "WHERE ..."

# Parquet management
streamline-cli parquet convert <topic>       # Convert segments to Parquet
streamline-cli parquet compact <topic>       # Compact small files
streamline-cli parquet stats <topic>         # Show column statistics
streamline-cli parquet schema <topic>        # Show Parquet schema

# Export
streamline-cli export <topic> --format parquet --destination s3://bucket/path
streamline-cli export <topic> --format iceberg --catalog rest --namespace db --table events
streamline-cli export jobs list
streamline-cli export jobs status <job-id>
streamline-cli export jobs stop <job-id>

# Analytics queries
streamline-cli query "SELECT * FROM topic('events') WHERE timestamp > now() - interval '1 hour'"
streamline-cli query "SELECT count(*) FROM mv('hourly_counts')"
```

### HTTP API Endpoints

Add to `src/server/lakehouse_api.rs`:

```
# Materialized Views
POST   /api/v1/views                    # Create view
GET    /api/v1/views                    # List views
GET    /api/v1/views/{name}             # Get view details
DELETE /api/v1/views/{name}             # Drop view
POST   /api/v1/views/{name}/refresh     # Trigger refresh
GET    /api/v1/views/{name}/query       # Query view

# Parquet Management
GET    /api/v1/topics/{topic}/parquet/stats      # Column statistics
POST   /api/v1/topics/{topic}/parquet/compact    # Trigger compaction
GET    /api/v1/topics/{topic}/parquet/schema     # Get schema

# Export
POST   /api/v1/export/jobs              # Create export job
GET    /api/v1/export/jobs              # List jobs
GET    /api/v1/export/jobs/{id}         # Get job status
DELETE /api/v1/export/jobs/{id}         # Cancel job
POST   /api/v1/export/snapshot          # One-time export

# SQL Queries
POST   /api/v1/query                    # Execute SQL query
GET    /api/v1/query/{id}               # Get query result
DELETE /api/v1/query/{id}               # Cancel query
```

### Implementation Tasks

1. **Task 1: Lakehouse Module Structure**
   - Create `src/lakehouse/mod.rs` with module exports
   - Define `LakehouseConfig`, `ParquetConfig`
   - Add `lakehouse` feature flag
   - Files: `src/lakehouse/mod.rs`, `src/lakehouse/config.rs`, `Cargo.toml`
   - Acceptance: Module structure compiles

2. **Task 2: Parquet Segment Storage**
   - Implement `ParquetSegment` reader/writer
   - Add schema inference from records
   - Implement row group management
   - Add column statistics collection
   - Files: `src/lakehouse/parquet_segment.rs`
   - Acceptance: Data stored and read in Parquet format

3. **Task 3: Columnar Query Support**
   - Implement projection pushdown
   - Add column pruning
   - Optimize for columnar scans
   - Files: `src/lakehouse/columnar.rs`
   - Acceptance: Queries read only required columns

4. **Task 4: Statistics & Bloom Filters**
   - Implement `StatisticsManager`
   - Add min/max/count collection
   - Implement bloom filters for columns
   - Add distinct count estimation
   - Files: `src/lakehouse/statistics.rs`
   - Acceptance: Statistics used for query optimization

5. **Task 5: Query Optimizer**
   - Implement `QueryOptimizer`
   - Add predicate pushdown
   - Add partition pruning
   - Implement cost-based optimization
   - Files: `src/lakehouse/query_optimizer.rs`
   - Acceptance: Queries optimized with pushdown

6. **Task 6: Materialized View Core**
   - Implement `MaterializedView` storage
   - Add view definition parsing
   - Implement full refresh
   - Add view querying
   - Files: `src/lakehouse/materialized_view.rs`
   - Acceptance: MVs created and refreshed

7. **Task 7: Incremental Refresh**
   - Implement `IncrementalEngine`
   - Add delta processing for aggregates
   - Support incremental joins
   - Track watermarks per source
   - Files: `src/lakehouse/incremental.rs`
   - Acceptance: MVs refresh incrementally

8. **Task 8: Automatic View Maintenance**
   - Add background refresh scheduler
   - Implement staleness detection
   - Add refresh triggers
   - Files: `src/lakehouse/scheduler.rs`
   - Acceptance: MVs automatically maintained

9. **Task 9: Zero-ETL Export**
   - Implement `ExportManager`
   - Add Parquet export
   - Add Iceberg export (enhance existing)
   - Add Delta Lake export (enhance existing)
   - Files: `src/lakehouse/export.rs`
   - Acceptance: Direct export to warehouse formats

10. **Task 10: Time-Travel Enhancement**
    - Enhance existing time-travel with Parquet
    - Add snapshot management
    - Implement point-in-time queries
    - Files: `src/lakehouse/time_travel.rs`
    - Acceptance: Query any historical point

11. **Task 11: CLI Commands**
    - Add `mv` subcommand for materialized views
    - Add `parquet` management commands
    - Add `export` commands
    - Files: `src/cli.rs`, `src/cli_utils/lakehouse_commands.rs`
    - Acceptance: CLI commands functional

12. **Task 12: HTTP API**
    - Implement lakehouse REST endpoints
    - Add query streaming for large results
    - Add OpenAPI documentation
    - Files: `src/server/lakehouse_api.rs`
    - Acceptance: API fully functional

13. **Task 13: Integration with StreamQL**
    - Integrate optimizer with StreamQL
    - Add MV support to query parser
    - Implement unified query execution
    - Files: `src/streamql/`, `src/lakehouse/`
    - Acceptance: SQL queries use lakehouse features

14. **Task 14: Documentation & Examples**
    - Add lakehouse usage guide
    - Create MV examples
    - Add performance tuning guide
    - Files: `docs/lakehouse.md`, `examples/lakehouse/`
    - Acceptance: Documentation complete

## Dependencies

```toml
[dependencies]
# Already have parquet and arrow from iceberg feature
# Ensure they're available for lakehouse

[features]
lakehouse = ["dep:parquet", "dep:arrow", "analytics"]
full = ["...", "lakehouse"]
```

## Acceptance Criteria

- [ ] Parquet segment storage functional
- [ ] Columnar queries read only required columns
- [ ] Materialized views created and queried
- [ ] Incremental refresh for aggregations
- [ ] SQL pushdown to storage layer
- [ ] Statistics used for optimization
- [ ] Zero-ETL export to Iceberg/Delta/Parquet
- [ ] CLI commands fully functional
- [ ] HTTP API documented and working
- [ ] Performance: 10x faster than row scans for analytics
- [ ] All tests pass
- [ ] Documentation complete

## Example Usage

```sql
-- Create materialized view with incremental refresh
CREATE MATERIALIZED VIEW hourly_sales AS
SELECT
    date_trunc('hour', timestamp) as hour,
    product_id,
    SUM(amount) as total_sales,
    COUNT(*) as transaction_count
FROM topic('sales')
GROUP BY 1, 2
WITH (refresh = 'incremental', interval = '5 minutes');

-- Query with pushdown
SELECT *
FROM topic('events')
WHERE timestamp > now() - interval '1 hour'
  AND user_id = 'user-123'
  AND event_type IN ('click', 'purchase')
LIMIT 100;

-- Query materialized view
SELECT * FROM mv('hourly_sales')
WHERE hour > now() - interval '24 hours'
ORDER BY total_sales DESC
LIMIT 10;

-- Export to Iceberg
EXPORT topic('events')
TO ICEBERG 'rest://catalog/namespace/events'
PARTITIONED BY (date(timestamp))
WITH (format = 'parquet', compression = 'zstd');
```

## Related Files

- `src/lakehouse/` - Lakehouse module (all files)
- `src/streamql/` - SQL engine integration
- `src/storage/` - Storage layer modifications
- `src/sink/` - Export integration
- `src/server/lakehouse_api.rs` - HTTP API
- `src/cli.rs` - CLI commands

## Labels

`enhancement`, `feature`, `analytics`, `lakehouse`, `game-changer`, `xlarge`
