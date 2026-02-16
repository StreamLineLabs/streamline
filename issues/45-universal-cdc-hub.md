# Universal CDC Hub

## Summary

Transform Streamline into a universal Change Data Capture (CDC) hub that captures changes from all major databases (PostgreSQL, MySQL, MongoDB, SQLServer, DynamoDB) and streams them in real-time. This eliminates the need for Kafka + Debezium + Kafka Connect, replacing the entire stack with a single binary.

## Current State

- PostgreSQL CDC implemented (`src/cdc/postgres.rs`) with logical replication
- Basic CDC config (`src/cdc/config.rs`) and types (`src/cdc/mod.rs`)
- No MySQL, MongoDB, SQLServer, or DynamoDB support
- No unified connector API
- No schema evolution tracking across sources
- No Debezium-compatible message format

## Requirements

### Database Support

| Database | Priority | Method | Description |
|----------|----------|--------|-------------|
| PostgreSQL | P0 (Done) | Logical Replication | Already implemented |
| MySQL | P0 | Binlog Replication | Parse MySQL binary log |
| MongoDB | P0 | Change Streams | Native change stream API |
| SQLServer | P1 | CDC Tables | Query CDC tables |
| DynamoDB | P1 | Streams | DynamoDB Streams API |
| Oracle | P2 | LogMiner | Oracle LogMiner API |
| CockroachDB | P2 | Changefeeds | Native changefeed support |

### Core Features

| Requirement | Priority | Description |
|-------------|----------|-------------|
| Unified Connector API | P0 | Common trait for all CDC sources |
| Debezium Format | P0 | Compatible message envelope format |
| Schema Registry Integration | P0 | Track schema evolution |
| Exactly-Once Delivery | P1 | Transactional CDC with deduplication |
| Multi-Source Join | P1 | Join streams from multiple databases |
| Schema Evolution | P1 | Handle ALTER TABLE across sources |
| Snapshot + Streaming | P1 | Initial snapshot then live changes |

### Data Structures

Create `src/cdc/connector.rs`:

```rust
//! Unified CDC Connector API
//!
//! Provides a common interface for all CDC sources with automatic
//! schema tracking and Debezium-compatible message format.

use async_trait::async_trait;

/// Unified CDC connector trait
#[async_trait]
pub trait CdcConnector: Send + Sync {
    /// Connector name (e.g., "mysql", "mongodb")
    fn name(&self) -> &str;

    /// Start the CDC connector
    async fn start(&mut self) -> Result<()>;

    /// Stop the CDC connector gracefully
    async fn stop(&mut self) -> Result<()>;

    /// Get the next batch of change events
    async fn poll(&mut self, timeout: Duration) -> Result<Vec<ChangeEvent>>;

    /// Commit processed offsets
    async fn commit(&mut self, offsets: &[SourceOffset]) -> Result<()>;

    /// Get current source position/offset
    fn current_offset(&self) -> SourceOffset;

    /// Get connector status
    fn status(&self) -> ConnectorStatus;

    /// Get schema for a table
    async fn get_schema(&self, table: &TableId) -> Result<TableSchema>;

    /// Handle schema change
    async fn handle_schema_change(&mut self, change: SchemaChange) -> Result<()>;
}

/// CDC change event (Debezium-compatible)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Event schema (for schema registry)
    pub schema: Option<SchemaRef>,
    /// Event payload
    pub payload: ChangePayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangePayload {
    /// Before state (for updates/deletes)
    pub before: Option<Value>,
    /// After state (for inserts/updates)
    pub after: Option<Value>,
    /// Source metadata
    pub source: SourceInfo,
    /// Operation type
    pub op: ChangeOperation,
    /// Event timestamp (ms since epoch)
    pub ts_ms: i64,
    /// Transaction metadata
    pub transaction: Option<TransactionInfo>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ChangeOperation {
    #[serde(rename = "c")]
    Create,
    #[serde(rename = "u")]
    Update,
    #[serde(rename = "d")]
    Delete,
    #[serde(rename = "r")]
    Read,  // Snapshot read
    #[serde(rename = "t")]
    Truncate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceInfo {
    /// Connector version
    pub version: String,
    /// Connector name
    pub connector: String,
    /// Database name
    pub name: String,
    /// Server/cluster ID
    pub server_id: Option<String>,
    /// Timestamp of the change in the source
    pub ts_sec: i64,
    /// Database-specific position
    pub pos: String,
    /// Database name
    pub db: String,
    /// Schema name (if applicable)
    pub schema: Option<String>,
    /// Table name
    pub table: String,
    /// Is this a snapshot event?
    pub snapshot: bool,
}

/// Source offset for resumption
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SourceOffset {
    Postgres { lsn: String, txid: Option<i64> },
    MySQL { file: String, position: u64, gtid: Option<String> },
    MongoDB { resume_token: String },
    SQLServer { lsn: String, seqval: String },
    DynamoDB { shard_id: String, sequence_number: String },
}
```

Create `src/cdc/mysql.rs`:

```rust
//! MySQL CDC Connector
//!
//! Captures changes from MySQL using binary log (binlog) replication.
//! Supports MySQL 5.6+, MariaDB 10.0+, and Aurora MySQL.

use mysql_cdc::{BinlogClient, BinlogEvent};

pub struct MySqlConnector {
    config: MySqlConfig,
    client: Option<BinlogClient>,
    current_position: BinlogPosition,
    schema_cache: SchemaCache,
    tables: HashSet<TableId>,
    status: ConnectorStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MySqlConfig {
    /// MySQL host
    pub host: String,
    /// MySQL port (default: 3306)
    pub port: u16,
    /// Username
    pub username: String,
    /// Password
    pub password: String,
    /// Database to capture (or "*" for all)
    pub database: String,
    /// Tables to capture (empty = all)
    pub tables: Vec<String>,
    /// Server ID for replication
    pub server_id: u32,
    /// Use GTID for positioning
    pub use_gtid: bool,
    /// Initial snapshot mode
    pub snapshot_mode: SnapshotMode,
}

#[derive(Debug, Clone)]
pub struct BinlogPosition {
    pub file: String,
    pub position: u64,
    pub gtid_set: Option<String>,
}

impl MySqlConnector {
    pub async fn new(config: MySqlConfig) -> Result<Self>;

    /// Parse binlog events into ChangeEvents
    async fn parse_event(&mut self, event: BinlogEvent) -> Result<Option<ChangeEvent>>;

    /// Handle row insert
    fn handle_insert(&self, table: &TableId, row: &[Value]) -> ChangePayload;

    /// Handle row update
    fn handle_update(&self, table: &TableId, before: &[Value], after: &[Value]) -> ChangePayload;

    /// Handle row delete
    fn handle_delete(&self, table: &TableId, row: &[Value]) -> ChangePayload;

    /// Take initial snapshot
    async fn snapshot(&mut self) -> Result<Vec<ChangeEvent>>;
}

#[async_trait]
impl CdcConnector for MySqlConnector {
    fn name(&self) -> &str { "mysql" }

    async fn start(&mut self) -> Result<()> {
        // Connect to MySQL
        // Start binlog replication
        // Optionally take snapshot first
    }

    async fn poll(&mut self, timeout: Duration) -> Result<Vec<ChangeEvent>> {
        // Read binlog events
        // Parse into ChangeEvents
        // Track schema changes
    }

    // ... implement other trait methods
}
```

Create `src/cdc/mongodb.rs`:

```rust
//! MongoDB CDC Connector
//!
//! Captures changes from MongoDB using Change Streams API.
//! Supports MongoDB 4.0+ (replica sets) and 4.2+ (sharded clusters).

use mongodb::{Client, options::ChangeStreamOptions, change_stream::event::ChangeStreamEvent};

pub struct MongoDbConnector {
    config: MongoDbConfig,
    client: Option<Client>,
    resume_token: Option<ResumeToken>,
    status: ConnectorStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MongoDbConfig {
    /// MongoDB connection string
    pub connection_string: String,
    /// Database to capture (or "*" for all)
    pub database: String,
    /// Collections to capture (empty = all)
    pub collections: Vec<String>,
    /// Full document mode for updates
    pub full_document: FullDocumentMode,
    /// Full document before change (MongoDB 6.0+)
    pub full_document_before_change: bool,
    /// Pipeline for filtering changes
    pub pipeline: Option<Vec<Document>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FullDocumentMode {
    Default,
    UpdateLookup,
    WhenAvailable,
    Required,
}

impl MongoDbConnector {
    pub async fn new(config: MongoDbConfig) -> Result<Self>;

    /// Convert MongoDB change event to CDC ChangeEvent
    fn convert_event(&self, event: ChangeStreamEvent<Document>) -> Result<ChangeEvent>;
}

#[async_trait]
impl CdcConnector for MongoDbConnector {
    fn name(&self) -> &str { "mongodb" }

    async fn poll(&mut self, timeout: Duration) -> Result<Vec<ChangeEvent>> {
        // Use change stream cursor
        // Handle resume tokens
        // Convert to ChangeEvents
    }

    // ... implement other trait methods
}
```

Create `src/cdc/sqlserver.rs`:

```rust
//! SQL Server CDC Connector
//!
//! Captures changes from SQL Server using CDC tables or Change Tracking.
//! Supports SQL Server 2016+ (Standard/Enterprise for CDC).

use tiberius::{Client, Config};

pub struct SqlServerConnector {
    config: SqlServerConfig,
    client: Option<Client<TcpStream>>,
    last_lsn: Option<Lsn>,
    status: ConnectorStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlServerConfig {
    /// SQL Server host
    pub host: String,
    /// Port (default: 1433)
    pub port: u16,
    /// Database name
    pub database: String,
    /// Username
    pub username: String,
    /// Password
    pub password: String,
    /// Tables to capture
    pub tables: Vec<String>,
    /// Polling interval
    pub poll_interval_ms: u64,
    /// Use Change Tracking instead of CDC
    pub use_change_tracking: bool,
}

impl SqlServerConnector {
    /// Query CDC tables for changes
    async fn poll_cdc_tables(&mut self) -> Result<Vec<ChangeEvent>>;

    /// Query Change Tracking for changes
    async fn poll_change_tracking(&mut self) -> Result<Vec<ChangeEvent>>;
}
```

Create `src/cdc/dynamodb.rs`:

```rust
//! DynamoDB CDC Connector
//!
//! Captures changes from DynamoDB using DynamoDB Streams.

use aws_sdk_dynamodbstreams::{Client, types::StreamRecord};

pub struct DynamoDbConnector {
    config: DynamoDbConfig,
    client: Option<Client>,
    shard_iterators: HashMap<String, String>,
    status: ConnectorStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamoDbConfig {
    /// AWS region
    pub region: String,
    /// Table name
    pub table_name: String,
    /// Stream ARN (optional, derived from table)
    pub stream_arn: Option<String>,
    /// Starting position
    pub starting_position: StreamStartPosition,
    /// AWS credentials (optional, uses default chain)
    pub credentials: Option<AwsCredentials>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamStartPosition {
    TrimHorizon,
    Latest,
    AtSequenceNumber(String),
}
```

Create `src/cdc/manager.rs`:

```rust
//! CDC Manager - Orchestrates multiple CDC connectors

pub struct CdcManager {
    /// Active connectors by name
    connectors: DashMap<String, Box<dyn CdcConnector>>,
    /// Topic manager for output
    topic_manager: Arc<TopicManager>,
    /// Schema registry for tracking schemas
    schema_registry: Option<Arc<SchemaRegistry>>,
    /// Metrics
    metrics: CdcMetrics,
    /// Background tasks
    tasks: JoinSet<Result<()>>,
}

impl CdcManager {
    /// Create a new connector from config
    pub async fn create_connector(&self, name: &str, config: CdcConnectorConfig) -> Result<()>;

    /// Start a connector
    pub async fn start_connector(&self, name: &str) -> Result<()>;

    /// Stop a connector
    pub async fn stop_connector(&self, name: &str) -> Result<()>;

    /// List all connectors
    pub fn list_connectors(&self) -> Vec<ConnectorInfo>;

    /// Get connector status
    pub fn connector_status(&self, name: &str) -> Option<ConnectorStatus>;

    /// Run the CDC pipeline (connector -> topic)
    async fn run_pipeline(&self, name: &str) -> Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CdcConnectorConfig {
    Postgres(PostgresConfig),
    MySQL(MySqlConfig),
    MongoDB(MongoDbConfig),
    SQLServer(SqlServerConfig),
    DynamoDB(DynamoDbConfig),
}
```

### CLI Commands

Add to `src/cli.rs`:

```bash
# Connector management
streamline-cli cdc create <name> --type mysql --config connector.yaml
streamline-cli cdc list
streamline-cli cdc describe <name>
streamline-cli cdc start <name>
streamline-cli cdc stop <name>
streamline-cli cdc delete <name>

# Status and monitoring
streamline-cli cdc status <name>
streamline-cli cdc lag <name>          # Show replication lag
streamline-cli cdc offsets <name>      # Show current offsets
streamline-cli cdc offsets set <name> --position "mysql-bin.000001:12345"

# Schema management
streamline-cli cdc schemas <name>      # List captured schemas
streamline-cli cdc schema <name> --table users

# Snapshot
streamline-cli cdc snapshot <name> --tables users,orders

# Validation
streamline-cli cdc validate <name>     # Test connection and permissions
```

### HTTP API Endpoints

Add to `src/server/cdc_api.rs`:

```
POST   /api/v1/cdc/connectors              # Create connector
GET    /api/v1/cdc/connectors              # List connectors
GET    /api/v1/cdc/connectors/{name}       # Get connector details
DELETE /api/v1/cdc/connectors/{name}       # Delete connector
POST   /api/v1/cdc/connectors/{name}/start # Start connector
POST   /api/v1/cdc/connectors/{name}/stop  # Stop connector
GET    /api/v1/cdc/connectors/{name}/status# Get status
GET    /api/v1/cdc/connectors/{name}/lag   # Get replication lag
POST   /api/v1/cdc/connectors/{name}/snapshot # Trigger snapshot
GET    /api/v1/cdc/connectors/{name}/schemas # List schemas
PUT    /api/v1/cdc/connectors/{name}/offsets # Set offsets
POST   /api/v1/cdc/validate                # Validate config
```

### Implementation Tasks

1. **Task 1: Unified Connector API**
   - Create `CdcConnector` trait in `src/cdc/connector.rs`
   - Define `ChangeEvent`, `SourceOffset`, `SourceInfo` types
   - Add Debezium-compatible serialization
   - Refactor existing PostgreSQL connector to use trait
   - Files: `src/cdc/connector.rs`, `src/cdc/postgres.rs`
   - Acceptance: PostgreSQL works with new trait

2. **Task 2: MySQL CDC Connector**
   - Add `mysql_async` and `mysql_cdc` dependencies
   - Implement `MySqlConnector` with binlog parsing
   - Support GTID and file/position modes
   - Add initial snapshot capability
   - Handle DDL events (schema changes)
   - Files: `src/cdc/mysql.rs`
   - Acceptance: Captures INSERT/UPDATE/DELETE from MySQL

3. **Task 3: MongoDB CDC Connector**
   - Add `mongodb` driver dependency
   - Implement `MongoDbConnector` with change streams
   - Support full document lookup
   - Handle resume tokens for resumption
   - Files: `src/cdc/mongodb.rs`
   - Acceptance: Captures changes from MongoDB collections

4. **Task 4: SQL Server CDC Connector**
   - Add `tiberius` (SQL Server client) dependency
   - Implement `SqlServerConnector`
   - Support both CDC tables and Change Tracking
   - Handle LSN-based positioning
   - Files: `src/cdc/sqlserver.rs`
   - Acceptance: Captures changes from SQL Server

5. **Task 5: DynamoDB CDC Connector**
   - Add AWS SDK dependencies
   - Implement `DynamoDbConnector` with Streams API
   - Handle shard management
   - Support sequence number positioning
   - Files: `src/cdc/dynamodb.rs`
   - Acceptance: Captures changes from DynamoDB

6. **Task 6: CDC Manager**
   - Implement `CdcManager` for orchestration
   - Add connector lifecycle management
   - Implement pipeline (connector -> topic)
   - Add offset storage and recovery
   - Add metrics collection
   - Files: `src/cdc/manager.rs`
   - Acceptance: Multiple connectors run concurrently

7. **Task 7: Schema Evolution Tracking**
   - Integrate with Schema Registry
   - Track schema changes per source table
   - Store schema history
   - Handle compatibility checking
   - Files: `src/cdc/schema_tracker.rs`
   - Acceptance: Schema changes tracked and stored

8. **Task 8: Exactly-Once Delivery**
   - Add transaction tracking
   - Implement deduplication window
   - Add idempotent writes to topics
   - Files: `src/cdc/transaction.rs`
   - Acceptance: No duplicate events on restart

9. **Task 9: CLI Commands**
   - Add `cdc` subcommand group
   - Implement all management commands
   - Add status and monitoring commands
   - Files: `src/cli.rs`, `src/cli_utils/cdc_commands.rs`
   - Acceptance: All CLI commands functional

10. **Task 10: HTTP API**
    - Implement CDC REST endpoints
    - Add WebSocket for real-time status
    - Add OpenAPI documentation
    - Files: `src/server/cdc_api.rs`
    - Acceptance: API fully functional

11. **Task 11: Multi-Source Joins**
    - Add stream join capability across CDC sources
    - Implement temporal join windows
    - Add join state management
    - Files: `src/cdc/join.rs`
    - Acceptance: Can join changes from multiple databases

12. **Task 12: Documentation & Examples**
    - Add comprehensive documentation
    - Create example configurations
    - Add migration guide from Debezium
    - Files: `docs/cdc-hub.md`, `examples/cdc/`
    - Acceptance: Documentation complete

## Dependencies

Add to `Cargo.toml`:

```toml
# MySQL CDC (optional - mysql-cdc feature)
mysql_async = { version = "0.34", optional = true }

# MongoDB CDC (optional - mongodb-cdc feature)
mongodb = { version = "3.1", optional = true }

# SQL Server CDC (optional - sqlserver-cdc feature)
tiberius = { version = "0.12", default-features = false, features = ["rustls"], optional = true }

# DynamoDB CDC (optional - dynamodb-cdc feature)
aws-sdk-dynamodbstreams = { version = "1.0", optional = true }
aws-config = { version = "1.0", optional = true }

[features]
mysql-cdc = ["dep:mysql_async"]
mongodb-cdc = ["dep:mongodb"]
sqlserver-cdc = ["dep:tiberius"]
dynamodb-cdc = ["dep:aws-sdk-dynamodbstreams", "dep:aws-config"]
cdc-hub = ["postgres-cdc", "mysql-cdc", "mongodb-cdc", "sqlserver-cdc", "dynamodb-cdc"]
full = ["...", "cdc-hub"]
```

## Acceptance Criteria

- [ ] All 5 database connectors implemented and tested
- [ ] Unified connector API works across all sources
- [ ] Debezium-compatible message format
- [ ] Schema evolution properly tracked
- [ ] Exactly-once delivery with deduplication
- [ ] CLI commands fully functional
- [ ] HTTP API documented and working
- [ ] Performance: >50K events/sec per connector
- [ ] Resume from any position after restart
- [ ] All tests pass
- [ ] Documentation complete

## Example Usage

```yaml
# connector.yaml
name: mysql-users
type: mysql
config:
  host: localhost
  port: 3306
  username: cdc_user
  password: ${MYSQL_PASSWORD}
  database: myapp
  tables:
    - users
    - orders
  server_id: 12345
  snapshot_mode: initial
output:
  topic_prefix: cdc.myapp
  # Creates topics: cdc.myapp.users, cdc.myapp.orders
```

```bash
# Create and start connector
streamline-cli cdc create mysql-users --config connector.yaml
streamline-cli cdc start mysql-users

# Monitor
streamline-cli cdc status mysql-users
streamline-cli cdc lag mysql-users

# Consume CDC events
streamline-cli consume cdc.myapp.users --from-beginning
```

## Related Files

- `src/cdc/` - CDC module (all connectors)
- `src/cdc/connector.rs` - Unified trait
- `src/cdc/manager.rs` - Orchestration
- `src/server/cdc_api.rs` - HTTP API
- `src/cli.rs` - CLI commands
- `Cargo.toml` - Dependencies

## Labels

`enhancement`, `feature`, `cdc`, `game-changer`, `xlarge`
