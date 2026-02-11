use clap::Subcommand;
use std::path::PathBuf;

#[derive(Subcommand, Debug)]
pub(crate) enum TelemetryCommands {
    /// Show current telemetry report (what would be sent)
    Show {
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Show telemetry configuration
    Config,

    /// Reset installation ID (generates a new anonymous ID)
    ResetId {
        /// Skip confirmation prompt
        #[arg(long, short = 'y')]
        yes: bool,
    },

    /// View privacy information and data collection details
    Privacy,
}

#[derive(Subcommand, Debug)]
pub(crate) enum GitOpsCommands {
    /// Apply a configuration manifest (YAML or JSON)
    Apply {
        /// Path to manifest file
        #[arg(short, long)]
        file: String,

        /// Dry run - show changes without applying
        #[arg(long)]
        dry_run: bool,

        /// Force apply even if validation warnings exist
        #[arg(long)]
        force: bool,
    },

    /// Validate a manifest without applying
    Validate {
        /// Path to manifest file
        #[arg(short, long)]
        file: String,
    },

    /// Show diff between current state and manifest
    Diff {
        /// Path to manifest file
        #[arg(short, long)]
        file: String,
    },

    /// Export current configuration as YAML manifest
    Export {
        /// Output file path (stdout if not specified)
        #[arg(short, long)]
        output: Option<String>,

        /// Export format (yaml or json)
        #[arg(long, default_value = "yaml")]
        format: String,
    },

    /// Show reconciliation status
    Status,
}

#[derive(Subcommand, Debug)]
pub(crate) enum GeoReplicationCommands {
    /// Show geo-replication status
    Status,

    /// List peer regions
    Peers,

    /// Add a peer region
    AddPeer {
        /// Region identifier
        #[arg(long)]
        region_id: String,

        /// Display name
        #[arg(long)]
        name: String,

        /// Bootstrap servers endpoint
        #[arg(long)]
        endpoint: String,
    },

    /// Remove a peer region
    RemovePeer {
        /// Region identifier
        region_id: String,
    },

    /// Start replication
    Start,

    /// Stop replication
    Stop,
}

#[derive(Subcommand, Debug)]
pub(crate) enum ContractCommands {
    /// Run contract tests from a YAML file
    Run {
        /// Path to contract file or directory
        #[arg(short, long)]
        file: String,

        /// Output format (text, json, junit)
        #[arg(long, default_value = "text")]
        output: String,

        /// Write results to file
        #[arg(short = 'o', long)]
        output_file: Option<String>,
    },

    /// Validate a contract definition without running tests
    Validate {
        /// Path to contract file
        #[arg(short, long)]
        file: String,
    },

    /// Generate a contract from an existing topic's data
    Generate {
        /// Topic name to analyze
        topic: String,

        /// Number of messages to sample
        #[arg(long, default_value = "100")]
        sample_size: usize,

        /// Output file path
        #[arg(short = 'o', long)]
        output: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum DebugCommands {
    /// Inspect events in a topic
    Inspect {
        /// Topic name
        topic: String,

        /// Partition to inspect
        #[arg(short, long, default_value = "0")]
        partition: i32,

        /// Starting offset
        #[arg(long)]
        offset: Option<i64>,

        /// Number of messages to show
        #[arg(short = 'n', long, default_value = "10")]
        count: usize,

        /// Pretty-print JSON values
        #[arg(long, default_value = "true")]
        pretty: bool,
    },

    /// Search for messages matching a pattern
    Search {
        /// Topic name
        topic: String,

        /// Regex pattern to search for
        pattern: String,

        /// Partition to search
        #[arg(short, long, default_value = "0")]
        partition: i32,

        /// Maximum messages to scan
        #[arg(long, default_value = "10000")]
        max_scan: usize,
    },

    /// Diff consumer group states
    GroupDiff {
        /// Consumer group ID
        group: String,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum ConnectorCommands {
    /// Search available connectors
    Search {
        /// Search query
        query: String,
    },

    /// List installed connectors
    List,

    /// Show connector details
    Info {
        /// Connector name
        name: String,
    },

    /// Install a connector
    Install {
        /// Connector name
        name: String,
    },

    /// Uninstall a connector
    Uninstall {
        /// Connector name
        name: String,

        /// Skip confirmation
        #[arg(long, short = 'y')]
        yes: bool,
    },

    /// Apply a declarative connector spec (YAML file)
    Apply {
        /// Path to connector YAML spec file
        file: PathBuf,

        /// Dry-run mode (validate only)
        #[arg(long)]
        dry_run: bool,
    },

    /// Show status of managed connectors
    Status {
        /// Connector name (omit for all)
        name: Option<String>,
    },

    /// Generate an example connector YAML spec
    Example,
}

#[derive(Subcommand, Debug)]
pub(crate) enum LineageCommands {
    /// Show data lineage graph
    Show {
        /// Output format (text, json, dot)
        #[arg(long, default_value = "text")]
        format: String,
    },

    /// Show upstream producers for a topic
    Upstream {
        /// Topic name
        topic: String,
    },

    /// Show downstream consumers for a topic
    Downstream {
        /// Topic name
        topic: String,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum MigrateCommands {
    /// Migrate from Apache Kafka
    FromKafka {
        /// Kafka bootstrap servers
        #[arg(long, default_value = "localhost:9092")]
        kafka_servers: String,

        /// Topics to migrate (comma-separated, empty for all)
        #[arg(long)]
        topics: Option<String>,

        /// Execute migration (default is dry-run)
        #[arg(long)]
        execute: bool,

        /// Also migrate data (not just topic configuration)
        #[arg(long)]
        with_data: bool,
    },

    /// Autopilot — fully automated Kafka-to-Streamline migration
    Autopilot {
        /// Kafka bootstrap servers
        #[arg(long, default_value = "localhost:9092")]
        kafka_servers: String,

        /// Topics to migrate (comma-separated, empty for all)
        #[arg(long)]
        topics: Option<String>,

        /// Migration strategy (blue-green, rolling, shadow)
        #[arg(long, default_value = "shadow")]
        strategy: String,

        /// Execute migration (default is dry-run / plan only)
        #[arg(long)]
        execute: bool,

        /// Parallel topic migrations
        #[arg(long, default_value = "4")]
        parallelism: usize,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum CloudCommands {
    /// List API keys for a tenant
    ApiKeys {
        /// Tenant ID
        tenant_id: String,
    },

    /// Onboard a new tenant
    Onboard {
        /// Organization name
        #[arg(long)]
        org_name: String,

        /// Admin email
        #[arg(long)]
        admin_email: String,

        /// Plan (free, pro, enterprise)
        #[arg(long, default_value = "free")]
        plan: String,
    },

    /// Show usage dashboard for a tenant
    Usage {
        /// Tenant ID
        tenant_id: String,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum AlertingCommands {
    /// List all alert rules
    List,

    /// Create a new alert rule
    Create {
        /// Alert name
        name: String,

        /// Topic to monitor
        #[arg(long)]
        topic: String,

        /// Condition type (lag_threshold, error_rate, throughput_drop)
        #[arg(long)]
        condition: String,

        /// Threshold value
        #[arg(long)]
        threshold: f64,

        /// Notification channel (webhook, email)
        #[arg(long, default_value = "webhook")]
        channel: String,
    },

    /// Show SLO status
    Slos,

    /// Create an SLO
    CreateSlo {
        /// SLO name
        name: String,

        /// Target (e.g., 99.9)
        #[arg(long)]
        target: f64,

        /// SLO indicator (availability, latency_p99, throughput)
        #[arg(long)]
        indicator: String,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum GovernorCommands {
    /// Show resource governor status
    Status,

    /// Show partition auto-scaling recommendations
    Recommendations,

    /// Show actions history
    History,
}

#[derive(Subcommand, Debug)]
#[command(long_about = r#"Topic management commands

EXAMPLES:
    # List all topics
    streamline-cli topics list

    # Create topic with 3 partitions
    streamline-cli topics create events --partitions 3

    # Create in-memory topic (diskless mode)
    streamline-cli topics create temp-data --storage-mode diskless

    # Describe topic details
    streamline-cli topics describe my-topic

    # Delete a topic
    streamline-cli topics delete old-topic

    # Change retention to 7 days
    streamline-cli topics alter my-topic --retention-ms 604800000

    # Watch real-time topic metrics
    streamline-cli topics watch

    # Export topic data to file
    streamline-cli topics export events -o backup.jsonl

    # Import from backup
    streamline-cli topics import events -i backup.jsonl"#)]
pub(crate) enum TopicCommands {
    /// List all topics
    List,

    /// Create a new topic
    Create {
        /// Topic name
        name: String,

        /// Number of partitions
        #[arg(long, short, default_value = "1")]
        partitions: i32,

        /// Retention time in milliseconds (-1 for infinite)
        #[arg(long, default_value = "-1")]
        retention_ms: i64,

        /// Retention size in bytes (-1 for infinite)
        #[arg(long, default_value = "-1")]
        retention_bytes: i64,

        /// Segment max size in bytes
        #[arg(long, default_value = "104857600")]
        segment_bytes: u64,

        /// Storage mode: local, hybrid, or diskless
        ///
        /// - local: Traditional local disk storage (default)
        /// - hybrid: Local WAL + object storage segments
        /// - diskless: All data written to object storage
        #[arg(long, default_value = "local")]
        storage_mode: String,

        /// Batch size for diskless/hybrid mode (bytes)
        #[arg(long, default_value = "1048576")]
        remote_batch_size: usize,

        /// Batch timeout for diskless/hybrid mode (milliseconds)
        #[arg(long, default_value = "100")]
        remote_batch_timeout_ms: u64,

        /// Read cache size for diskless/hybrid mode (bytes, 0 to disable)
        #[arg(long, default_value = "104857600")]
        remote_cache_bytes: u64,

        /// Show detailed explanation of what's happening
        #[arg(long)]
        explain: bool,
    },

    /// Describe a topic
    Describe {
        /// Topic name
        name: String,
    },

    /// Delete a topic
    Delete {
        /// Topic name
        name: String,
    },

    /// Alter topic configuration
    Alter {
        /// Topic name
        name: String,

        /// Retention time in milliseconds (-1 for infinite)
        #[arg(long)]
        retention_ms: Option<i64>,

        /// Retention size in bytes (-1 for infinite)
        #[arg(long)]
        retention_bytes: Option<i64>,

        /// Segment max size in bytes
        #[arg(long)]
        segment_bytes: Option<u64>,
    },

    /// Export topic data to a file for backup/migration
    Export {
        /// Topic name to export
        name: String,

        /// Output file path (default: <topic>.jsonl)
        #[arg(short, long)]
        output: Option<String>,

        /// Partition to export (default: all partitions)
        #[arg(short, long)]
        partition: Option<i32>,

        /// Start offset (default: beginning)
        #[arg(long)]
        from_offset: Option<i64>,

        /// End offset (default: latest)
        #[arg(long)]
        to_offset: Option<i64>,

        /// Include message metadata (keys, headers, timestamps)
        #[arg(long, default_value = "true")]
        include_metadata: bool,
    },

    /// Import topic data from a backup file
    Import {
        /// Topic name to import into
        name: String,

        /// Input file path
        #[arg(short, long)]
        input: String,

        /// Create topic if it doesn't exist
        #[arg(long)]
        create: bool,

        /// Number of partitions for auto-created topic
        #[arg(long, default_value = "1")]
        partitions: i32,

        /// Dry run - show what would be imported without actually importing
        #[arg(long)]
        dry_run: bool,
    },

    /// Watch topic metrics in real-time
    Watch {
        /// Topic name (optional, watches all topics if not specified)
        #[arg(short, long)]
        topic: Option<String>,

        /// Refresh interval in seconds
        #[arg(short, long, default_value = "2")]
        interval: u64,

        /// Show message rate (messages/sec)
        #[arg(long, default_value = "true")]
        rate: bool,

        /// Show size stats
        #[arg(long)]
        size: bool,
    },

    /// Clone a topic to a new topic (copies all messages)
    Clone {
        /// Source topic name
        source: String,

        /// Destination topic name
        dest: String,

        /// Number of partitions for destination (defaults to source's partition count)
        #[arg(long, short)]
        partitions: Option<i32>,

        /// Only copy messages from specific partition
        #[arg(long)]
        source_partition: Option<i32>,

        /// Starting offset to copy from (default: 0 = beginning)
        #[arg(long, default_value = "0")]
        from_offset: i64,

        /// Ending offset to copy to (default: -1 = latest)
        #[arg(long, default_value = "-1")]
        to_offset: i64,

        /// Show progress bar
        #[arg(long, default_value = "true")]
        progress: bool,
    },

    /// Calculate recommended retention settings based on current usage
    Retention {
        /// Topic name to analyze
        name: String,

        /// Target storage budget in bytes (e.g., "1GB", "500MB")
        #[arg(long)]
        budget: Option<String>,

        /// Target retention duration (e.g., "7d", "30d", "1y")
        #[arg(long)]
        duration: Option<String>,

        /// Show detailed analysis
        #[arg(long)]
        detailed: bool,
    },

    /// Compare two topics to find differences
    Diff {
        /// First topic name
        topic1: String,

        /// Second topic name
        topic2: String,

        /// Compare configuration only (skip message comparison)
        #[arg(long)]
        config_only: bool,

        /// Compare messages (can be slow for large topics)
        #[arg(long)]
        messages: bool,

        /// Maximum messages to compare when using --messages
        #[arg(long, default_value = "1000")]
        max_messages: usize,

        /// Show only differences (not similarities)
        #[arg(long)]
        diff_only: bool,
    },
}

#[cfg(feature = "auth")]
#[derive(Subcommand, Debug)]
pub(crate) enum UserCommands {
    /// List all users
    List {
        /// Path to users file
        #[arg(long, env = "STREAMLINE_AUTH_USERS_FILE")]
        users_file: PathBuf,
    },

    /// Add a new user
    Add {
        /// Path to users file
        #[arg(long, env = "STREAMLINE_AUTH_USERS_FILE")]
        users_file: PathBuf,

        /// Username
        username: String,

        /// Password (will be hashed)
        password: String,

        /// Permissions (e.g., "admin", "produce:*", "consume:topic-name")
        #[arg(long, short, value_delimiter = ',')]
        permissions: Vec<String>,
    },

    /// Remove a user
    Remove {
        /// Path to users file
        #[arg(long, env = "STREAMLINE_AUTH_USERS_FILE")]
        users_file: PathBuf,

        /// Username
        username: String,
    },
}

#[derive(Subcommand, Debug)]
#[command(long_about = r#"Consumer group management commands

EXAMPLES:
    # List all consumer groups
    streamline-cli groups list

    # Describe a consumer group (shows members, offsets, lag)
    streamline-cli groups describe my-app-group

    # Delete a consumer group
    streamline-cli groups delete old-group

OFFSET RESET:
    # Dry-run: show what would be reset
    streamline-cli groups reset my-group -t events --to-earliest

    # Actually reset to earliest
    streamline-cli groups reset my-group -t events --to-earliest --execute

    # Reset to latest offset
    streamline-cli groups reset my-group -t events --to-latest --execute

    # Reset to specific offset
    streamline-cli groups reset my-group -t events --to-offset 1000 --execute

    # Reset specific partition only
    streamline-cli groups reset my-group -t events -p 0 --to-earliest --execute

MONITORING:
    # Watch all groups with 5 second refresh
    streamline-cli groups watch

    # Watch specific group
    streamline-cli groups watch -g my-group

    # Alert when lag exceeds 1000
    streamline-cli groups watch --alert-lag 1000

    # CI/CD: exit with error if lag too high
    streamline-cli groups watch -g my-group --alert-lag 500 --exit-on-alert"#)]
pub(crate) enum GroupCommands {
    /// List all consumer groups
    List,

    /// Describe a consumer group
    Describe {
        /// Group ID
        group_id: String,
    },

    /// Delete a consumer group
    Delete {
        /// Group ID
        group_id: String,
    },

    /// Reset consumer group offsets
    Reset {
        /// Group ID
        group_id: String,

        /// Topic name
        #[arg(long, short)]
        topic: String,

        /// Partition (if not specified, resets all partitions)
        #[arg(long, short)]
        partition: Option<i32>,

        /// Reset to earliest offset
        #[arg(long, conflicts_with_all = ["to_latest", "to_offset"])]
        to_earliest: bool,

        /// Reset to latest offset
        #[arg(long, conflicts_with_all = ["to_earliest", "to_offset"])]
        to_latest: bool,

        /// Reset to specific offset
        #[arg(long, conflicts_with_all = ["to_earliest", "to_latest"])]
        to_offset: Option<i64>,

        /// Execute the reset (without this flag, only shows what would be done)
        #[arg(long)]
        execute: bool,
    },

    /// Watch consumer group lag in real-time with optional alerts
    Watch {
        /// Group ID to watch (optional, watches all groups if not specified)
        #[arg(short, long)]
        group: Option<String>,

        /// Alert when lag exceeds this threshold
        #[arg(long)]
        alert_lag: Option<i64>,

        /// Refresh interval in seconds
        #[arg(short, long, default_value = "5")]
        interval: u64,

        /// Exit with error code 1 if alert threshold is exceeded
        #[arg(long)]
        exit_on_alert: bool,
    },
}

#[cfg(feature = "iceberg")]
#[derive(Subcommand, Debug)]
pub(crate) enum SinkCommands {
    /// Create a new sink connector
    Create {
        /// Sink name
        name: String,

        /// Sink type (currently only "iceberg" is supported)
        #[arg(long, default_value = "iceberg")]
        sink_type: String,

        /// Topics to consume from (comma-separated)
        #[arg(long, short, value_delimiter = ',')]
        topics: Vec<String>,

        /// Iceberg catalog URI (e.g., "http://localhost:8181")
        #[arg(long)]
        catalog_uri: String,

        /// Catalog type: rest, hive, or glue
        #[arg(long, default_value = "rest")]
        catalog_type: String,

        /// Database/namespace name
        #[arg(long, default_value = "default")]
        namespace: String,

        /// Iceberg table name
        #[arg(long)]
        table: String,

        /// Commit interval in milliseconds
        #[arg(long, default_value = "60000")]
        commit_interval_ms: u64,

        /// Maximum batch size (records)
        #[arg(long, default_value = "10000")]
        max_batch_size: usize,

        /// Partitioning strategy: none, time_based_hour, time_based_day, time_based_month, field_based
        #[arg(long, default_value = "time_based_hour")]
        partitioning: String,

        /// Start the sink immediately after creation
        #[arg(long)]
        start: bool,
    },

    /// List all sink connectors
    List,

    /// Show detailed status of a sink
    Status {
        /// Sink name
        name: String,

        /// Show metrics in JSON format
        #[arg(long)]
        json: bool,
    },

    /// Start a stopped sink
    Start {
        /// Sink name
        name: String,
    },

    /// Stop a running sink
    Stop {
        /// Sink name
        name: String,
    },

    /// Delete a sink connector
    Delete {
        /// Sink name
        name: String,

        /// Skip confirmation prompt
        #[arg(short, long)]
        yes: bool,
    },
}

#[cfg(feature = "edge")]
#[derive(Subcommand, Debug)]
pub(crate) enum EdgeCommands {
    /// Show edge node status
    Status,

    /// Trigger synchronization with cloud
    Sync {
        /// Sync direction: edge-to-cloud, cloud-to-edge, or bidirectional
        #[arg(long, default_value = "bidirectional")]
        direction: String,

        /// Force sync even if no changes detected
        #[arg(long)]
        force: bool,
    },

    /// List connected edge nodes
    ListNodes,

    /// Show or update edge configuration
    Config {
        /// Show current configuration
        #[arg(long)]
        show: bool,

        /// Set configuration values (key=value format)
        #[arg(long, value_delimiter = ',')]
        set: Option<Vec<String>>,
    },

    /// View or resolve sync conflicts
    Conflicts {
        /// Filter by topic
        #[arg(long)]
        topic: Option<String>,

        /// Automatically resolve conflicts
        #[arg(long)]
        resolve: bool,
    },
}

#[cfg(any(
    feature = "postgres-cdc",
    feature = "mysql-cdc",
    feature = "mongodb-cdc",
    feature = "sqlserver-cdc"
))]
#[derive(Subcommand, Debug)]
pub(crate) enum CdcCommands {
    /// List CDC sources
    Sources,

    /// Create a new CDC source
    Create {
        /// Source name
        name: String,

        /// Source type: postgres, mysql, mongodb, sqlserver
        #[arg(long, short = 't')]
        source_type: String,

        /// Connection string
        #[arg(long, short)]
        connection: String,

        /// Tables to capture (comma-separated)
        #[arg(long, value_delimiter = ',')]
        tables: Vec<String>,

        /// Topic prefix for CDC events
        #[arg(long, default_value = "cdc")]
        topic_prefix: String,
    },

    /// Start a CDC source
    Start {
        /// Source name
        name: String,
    },

    /// Stop a CDC source
    Stop {
        /// Source name
        name: String,
    },

    /// Delete a CDC source
    Delete {
        /// Source name
        name: String,
    },

    /// Show detailed CDC source status
    Status {
        /// Source name
        name: String,
    },

    /// Show schema evolution history
    Schema {
        /// Source name
        name: String,

        /// Filter by table name
        #[arg(long)]
        table: Option<String>,
    },
}

#[cfg(feature = "ai")]
#[derive(Subcommand, Debug)]
pub(crate) enum AiCommands {
    /// Enrich stream data using LLM
    Enrich {
        /// Input topic
        topic: String,

        /// Enrichment prompt
        #[arg(long, short)]
        prompt: String,

        /// Output topic (default: {topic}-enriched)
        #[arg(long, short)]
        output: Option<String>,

        /// Model to use
        #[arg(long, default_value = "gpt-4")]
        model: String,
    },

    /// Semantic search across streams
    Search {
        /// Search query
        query: String,

        /// Topics to search (comma-separated, empty for all)
        #[arg(long, value_delimiter = ',')]
        topics: Vec<String>,

        /// Maximum results
        #[arg(long, default_value = "10")]
        limit: usize,

        /// Minimum similarity threshold (0.0-1.0)
        #[arg(long, default_value = "0.7")]
        threshold: f64,
    },

    /// Detect anomalies in stream data
    Anomalies {
        /// Topic to analyze
        topic: String,

        /// Detector type: statistical, pattern, semantic
        #[arg(long, default_value = "statistical")]
        detector: String,

        /// Detection sensitivity (0.0-1.0)
        #[arg(long, default_value = "0.8")]
        sensitivity: f64,
    },

    /// Classify messages using AI
    Classify {
        /// Input topic
        topic: String,

        /// Classification categories (comma-separated)
        #[arg(long, value_delimiter = ',')]
        categories: Vec<String>,

        /// Output topic (default: {topic}-classified)
        #[arg(long)]
        output: Option<String>,
    },

    /// Manage vector embeddings
    Embeddings {
        /// Topic name
        topic: String,

        /// Action: status, rebuild, enable, disable
        #[arg(long, default_value = "status")]
        action: String,
    },

    /// Configure semantic routing
    Route {
        /// Input topic
        topic: String,

        /// Routing rules (format: "condition -> target_topic")
        #[arg(long, value_delimiter = ';')]
        rules: Vec<String>,
    },

    /// List AI processing pipelines
    Pipelines,
}

#[cfg(feature = "lakehouse")]
#[derive(Subcommand, Debug)]
pub(crate) enum LakehouseCommands {
    /// List materialized views
    Views,

    /// Create a materialized view
    ViewCreate {
        /// View name
        name: String,

        /// SQL query definition
        #[arg(long, short)]
        query: String,

        /// Refresh mode: incremental, full, scheduled
        #[arg(long, default_value = "incremental")]
        refresh: String,
    },

    /// Refresh a materialized view
    ViewRefresh {
        /// View name
        name: String,

        /// Force full refresh
        #[arg(long)]
        full: bool,
    },

    /// Export topic data to lakehouse format
    Export {
        /// Topic to export
        topic: String,

        /// Output format: parquet, iceberg, delta
        #[arg(long, default_value = "parquet")]
        format: String,

        /// Destination path (local or cloud storage)
        #[arg(long, short)]
        destination: String,

        /// Partition by column/expression
        #[arg(long)]
        partition_by: Option<String>,
    },

    /// Execute SQL query on lakehouse data
    Query {
        /// SQL query
        query: String,

        /// Maximum rows to return
        #[arg(long, default_value = "100")]
        max_rows: usize,

        /// Output format
        #[arg(long, default_value = "text")]
        output_format: String,
    },

    /// List Parquet tables
    Tables,

    /// List export and compaction jobs
    Jobs,

    /// Show lakehouse statistics
    Stats,
}

#[derive(Subcommand, Debug)]
pub(crate) enum ClusterCommands {
    /// Initialize a new cluster configuration
    Init {
        /// Number of nodes in the cluster (3, 5, or 7 recommended)
        #[arg(long, short, default_value = "3")]
        nodes: usize,

        /// Base Kafka port (nodes will use port, port+10, port+20, etc.)
        #[arg(long, default_value = "9092")]
        kafka_port: u16,

        /// Base cluster port (nodes will use port, port+10, port+20, etc.)
        #[arg(long, default_value = "9093")]
        cluster_port: u16,

        /// Base HTTP port (nodes will use port, port+10, port+20, etc.)
        #[arg(long, default_value = "8080")]
        http_port: u16,

        /// Base hostname or IP (e.g., "localhost" or "192.168.1")
        #[arg(long, default_value = "localhost")]
        host: String,

        /// Output directory for generated files
        #[arg(long, short, default_value = "./cluster")]
        output_dir: PathBuf,

        /// Generate docker-compose.yml
        #[arg(long)]
        docker: bool,
    },

    /// Validate cluster configuration files
    Validate {
        /// Directory containing cluster configuration files
        #[arg(long, short, default_value = "./cluster")]
        config_dir: PathBuf,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum EncryptionCommands {
    /// Generate a new encryption key
    GenerateKey {
        /// Output file path for the key (writes hex-encoded key)
        #[arg(long, short)]
        output: Option<PathBuf>,

        /// Output format: "hex" or "raw"
        #[arg(long, default_value = "hex")]
        format: String,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum ConfigCommands {
    /// Validate a configuration file
    Validate {
        /// Path to configuration file (TOML format)
        #[arg(short, long)]
        config: PathBuf,

        /// Show detailed validation results
        #[arg(long)]
        verbose: bool,
    },

    /// Show effective configuration (merged from defaults + file + env)
    Show {
        /// Path to configuration file (TOML format)
        #[arg(short, long)]
        config: Option<PathBuf>,

        /// Output format
        #[arg(long, default_value = "toml")]
        format: String,
    },

    /// Generate a sample configuration file
    Generate {
        /// Output file path
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Include all options with comments
        #[arg(long)]
        full: bool,

        /// Edition: "lite" or "full"
        #[arg(long, default_value = "lite")]
        edition: String,
    },

    /// Check environment variables
    Env {
        /// Show only variables that are set
        #[arg(long)]
        set_only: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum ProfileCommands {
    /// List all connection profiles
    List,

    /// Show a specific profile
    Show {
        /// Profile name
        name: String,
    },

    /// Initialize a local profile with defaults
    Init {
        /// Profile name (default: local)
        #[arg(default_value = "local")]
        name: String,

        /// Data directory
        #[arg(long, default_value = "./data")]
        data_dir: String,

        /// Server address
        #[arg(long, default_value = "localhost:9092")]
        server_addr: String,

        /// HTTP API address
        #[arg(long, default_value = "localhost:9094")]
        http_addr: String,

        /// Description
        #[arg(long, short)]
        description: Option<String>,

        /// Set as default profile
        #[arg(long)]
        set_default: bool,
    },

    /// Add or update a connection profile
    Add {
        /// Profile name
        name: String,

        /// Data directory
        #[arg(long)]
        data_dir: Option<String>,

        /// Server address
        #[arg(long)]
        server_addr: Option<String>,

        /// HTTP API address
        #[arg(long)]
        http_addr: Option<String>,

        /// Description
        #[arg(long, short)]
        description: Option<String>,
    },

    /// Remove a connection profile
    Remove {
        /// Profile name
        name: String,
    },

    /// Set the default profile
    Default {
        /// Profile name to set as default
        name: String,
    },
}

#[cfg(feature = "auth")]
#[derive(Subcommand, Debug)]
pub(crate) enum AclCommands {
    /// List all ACLs
    List {
        /// Path to ACL file
        #[arg(long, env = "STREAMLINE_ACL_FILE")]
        acl_file: PathBuf,
    },

    /// Add a new ACL
    Add {
        /// Path to ACL file
        #[arg(long, env = "STREAMLINE_ACL_FILE")]
        acl_file: PathBuf,

        /// Resource type (topic, group, cluster)
        #[arg(long, short = 'r')]
        resource_type: String,

        /// Resource name (e.g., topic name, group ID, or "*" for all)
        #[arg(long, short = 'n')]
        resource_name: String,

        /// Pattern type (literal, prefixed)
        #[arg(long, short = 't', default_value = "literal")]
        pattern_type: String,

        /// Principal (e.g., "User:alice" or "User:*")
        #[arg(long, short = 'p')]
        principal: String,

        /// Host ("*" for any)
        #[arg(long, default_value = "*")]
        host: String,

        /// Operation (read, write, create, delete, alter, describe, all)
        #[arg(long, short = 'o')]
        operation: String,

        /// Permission (allow, deny)
        #[arg(long, default_value = "allow")]
        permission: String,
    },

    /// Remove ACLs matching the filter
    Remove {
        /// Path to ACL file
        #[arg(long, env = "STREAMLINE_ACL_FILE")]
        acl_file: PathBuf,

        /// Resource type filter (topic, group, cluster)
        #[arg(long, short = 'r')]
        resource_type: Option<String>,

        /// Resource name filter
        #[arg(long, short = 'n')]
        resource_name: Option<String>,

        /// Principal filter
        #[arg(long, short = 'p')]
        principal: Option<String>,

        /// Operation filter
        #[arg(long, short = 'o')]
        operation: Option<String>,
    },
}
