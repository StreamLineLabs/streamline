//! Connector Hub — Registry of 20+ pre-built connectors
//!
//! Provides a discoverable catalog of source and sink connectors that
//! can be configured via declarative YAML manifests or the CLI.
//!
//! # Usage
//!
//! ```bash
//! # List available connectors
//! streamline-cli connector list
//!
//! # Scaffold a new connector pipeline
//! streamline-cli connector init --source postgres-cdc --sink s3 --name my-pipeline
//!
//! # Deploy a connector from YAML
//! streamline-cli connector deploy -f pipeline.yaml
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A connector in the hub catalog
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorEntry {
    /// Unique connector identifier
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Short description
    pub description: String,
    /// Connector direction
    pub direction: ConnectorDirection,
    /// Category
    pub category: ConnectorCategory,
    /// Connector version
    pub version: String,
    /// Required configuration keys
    pub required_config: Vec<ConfigParam>,
    /// Optional configuration keys
    pub optional_config: Vec<ConfigParam>,
    /// Example YAML snippet
    pub example_yaml: String,
    /// Documentation URL
    pub docs_url: String,
    /// Stability tier
    pub stability: Stability,
}

/// Connector direction
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConnectorDirection {
    Source,
    Sink,
    Both,
}

/// Connector category
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConnectorCategory {
    Database,
    CloudStorage,
    Messaging,
    Http,
    Monitoring,
    DataWarehouse,
    FileSystem,
    Debug,
}

impl std::fmt::Display for ConnectorCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database => write!(f, "Database"),
            Self::CloudStorage => write!(f, "Cloud Storage"),
            Self::Messaging => write!(f, "Messaging"),
            Self::Http => write!(f, "HTTP"),
            Self::Monitoring => write!(f, "Monitoring"),
            Self::DataWarehouse => write!(f, "Data Warehouse"),
            Self::FileSystem => write!(f, "File System"),
            Self::Debug => write!(f, "Debug"),
        }
    }
}

/// Configuration parameter definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigParam {
    pub name: String,
    pub description: String,
    pub param_type: String,
    pub default: Option<String>,
    pub example: Option<String>,
}

/// Stability tier
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Stability {
    Stable,
    Beta,
    Experimental,
}

/// Build the complete connector catalog
pub fn connector_catalog() -> Vec<ConnectorEntry> {
    vec![
        // ===== DATABASE SOURCES =====
        ConnectorEntry {
            id: "postgres-cdc".into(),
            name: "PostgreSQL CDC".into(),
            description: "Change Data Capture from PostgreSQL via logical replication (pgoutput)".into(),
            direction: ConnectorDirection::Source,
            category: ConnectorCategory::Database,
            version: "0.2.0".into(),
            required_config: vec![
                param("connection_url", "PostgreSQL connection URL", "string", "postgresql://user:pass@localhost:5432/db"),
            ],
            optional_config: vec![
                param("publication", "Logical replication publication name", "string", "streamline_pub"),
                param("slot_name", "Replication slot name", "string", "streamline_slot"),
                param("tables", "Tables to capture (comma-separated)", "string", "public.orders,public.users"),
                param("snapshot_mode", "Initial snapshot mode (initial, never, always)", "string", "initial"),
            ],
            example_yaml: "type: postgres-cdc\nconfig:\n  connection_url: postgresql://user:pass@localhost:5432/mydb\n  tables: public.orders".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/postgres-cdc".into(),
            stability: Stability::Beta,
        },
        ConnectorEntry {
            id: "mysql-cdc".into(),
            name: "MySQL CDC".into(),
            description: "Change Data Capture from MySQL via binlog replication".into(),
            direction: ConnectorDirection::Source,
            category: ConnectorCategory::Database,
            version: "0.2.0".into(),
            required_config: vec![
                param("connection_url", "MySQL connection URL", "string", "mysql://user:pass@localhost:3306/db"),
            ],
            optional_config: vec![
                param("server_id", "MySQL server ID for replication", "integer", "12345"),
                param("tables", "Tables to capture", "string", "mydb.orders"),
            ],
            example_yaml: "type: mysql-cdc\nconfig:\n  connection_url: mysql://user:pass@localhost:3306/mydb".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/mysql-cdc".into(),
            stability: Stability::Beta,
        },
        ConnectorEntry {
            id: "mongodb-cdc".into(),
            name: "MongoDB CDC".into(),
            description: "Change Data Capture from MongoDB via change streams".into(),
            direction: ConnectorDirection::Source,
            category: ConnectorCategory::Database,
            version: "0.2.0".into(),
            required_config: vec![
                param("connection_url", "MongoDB connection string", "string", "mongodb://localhost:27017"),
                param("database", "Database name", "string", "mydb"),
            ],
            optional_config: vec![
                param("collections", "Collections to watch (comma-separated)", "string", "orders,users"),
            ],
            example_yaml: "type: mongodb-cdc\nconfig:\n  connection_url: mongodb://localhost:27017\n  database: mydb".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/mongodb-cdc".into(),
            stability: Stability::Experimental,
        },
        ConnectorEntry {
            id: "jdbc-source".into(),
            name: "JDBC Source".into(),
            description: "Poll-based source for any JDBC database (polling queries)".into(),
            direction: ConnectorDirection::Source,
            category: ConnectorCategory::Database,
            version: "0.2.0".into(),
            required_config: vec![
                param("connection_url", "JDBC connection URL", "string", "jdbc:postgresql://localhost:5432/db"),
                param("query", "SQL query to poll", "string", "SELECT * FROM events WHERE id > :last_id"),
            ],
            optional_config: vec![
                param("poll_interval_ms", "Polling interval in milliseconds", "integer", "5000"),
                param("incrementing_column", "Column for incremental polling", "string", "id"),
            ],
            example_yaml: "type: jdbc-source\nconfig:\n  connection_url: jdbc:postgresql://localhost/db\n  query: SELECT * FROM events".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/jdbc-source".into(),
            stability: Stability::Experimental,
        },

        // ===== CLOUD STORAGE SINKS =====
        ConnectorEntry {
            id: "s3-sink".into(),
            name: "Amazon S3 Sink".into(),
            description: "Write messages to S3 or S3-compatible object storage in Parquet, JSON, or CSV".into(),
            direction: ConnectorDirection::Sink,
            category: ConnectorCategory::CloudStorage,
            version: "0.2.0".into(),
            required_config: vec![
                param("bucket", "S3 bucket name", "string", "my-data-lake"),
                param("region", "AWS region", "string", "us-east-1"),
            ],
            optional_config: vec![
                param("prefix", "Object key prefix", "string", "streams/"),
                param("format", "Output format (parquet, json, csv)", "string", "parquet"),
                param("batch_size", "Messages per file", "integer", "10000"),
                param("flush_interval_secs", "Max seconds before flush", "integer", "300"),
                param("endpoint", "Custom S3 endpoint (MinIO, etc)", "string", "http://minio:9000"),
            ],
            example_yaml: "type: s3-sink\nconfig:\n  bucket: my-data-lake\n  region: us-east-1\n  format: parquet".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/s3-sink".into(),
            stability: Stability::Beta,
        },
        ConnectorEntry {
            id: "gcs-sink".into(),
            name: "Google Cloud Storage Sink".into(),
            description: "Write messages to Google Cloud Storage".into(),
            direction: ConnectorDirection::Sink,
            category: ConnectorCategory::CloudStorage,
            version: "0.2.0".into(),
            required_config: vec![
                param("bucket", "GCS bucket name", "string", "my-gcs-bucket"),
                param("project", "GCP project ID", "string", "my-project"),
            ],
            optional_config: vec![
                param("prefix", "Object name prefix", "string", "data/"),
                param("format", "Output format", "string", "json"),
                param("credentials_file", "Path to service account JSON", "string", "/secrets/gcp.json"),
            ],
            example_yaml: "type: gcs-sink\nconfig:\n  bucket: my-gcs-bucket\n  project: my-project".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/gcs-sink".into(),
            stability: Stability::Beta,
        },

        // ===== HTTP =====
        ConnectorEntry {
            id: "http-source".into(),
            name: "HTTP Source".into(),
            description: "Poll an HTTP endpoint periodically and emit responses as messages".into(),
            direction: ConnectorDirection::Source,
            category: ConnectorCategory::Http,
            version: "0.2.0".into(),
            required_config: vec![
                param("url", "HTTP URL to poll", "string", "https://api.example.com/data"),
            ],
            optional_config: vec![
                param("method", "HTTP method", "string", "GET"),
                param("interval_ms", "Polling interval", "integer", "60000"),
                param("headers", "Request headers (JSON)", "string", "{\"Authorization\": \"Bearer xxx\"}"),
            ],
            example_yaml: "type: http-source\nconfig:\n  url: https://api.example.com/data\n  interval_ms: 60000".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/http-source".into(),
            stability: Stability::Stable,
        },
        ConnectorEntry {
            id: "webhook-sink".into(),
            name: "HTTP Webhook Sink".into(),
            description: "Forward messages to an HTTP webhook endpoint".into(),
            direction: ConnectorDirection::Sink,
            category: ConnectorCategory::Http,
            version: "0.2.0".into(),
            required_config: vec![
                param("url", "Webhook URL", "string", "https://hooks.example.com/events"),
            ],
            optional_config: vec![
                param("method", "HTTP method", "string", "POST"),
                param("headers", "Request headers (JSON)", "string", "{}"),
                param("batch_size", "Messages per request", "integer", "1"),
                param("retry_count", "Max retries on failure", "integer", "3"),
            ],
            example_yaml: "type: webhook-sink\nconfig:\n  url: https://hooks.example.com/events".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/webhook-sink".into(),
            stability: Stability::Stable,
        },

        // ===== MONITORING =====
        ConnectorEntry {
            id: "elasticsearch-sink".into(),
            name: "Elasticsearch Sink".into(),
            description: "Index messages into Elasticsearch or OpenSearch".into(),
            direction: ConnectorDirection::Sink,
            category: ConnectorCategory::Monitoring,
            version: "0.2.0".into(),
            required_config: vec![
                param("hosts", "Elasticsearch hosts (comma-separated)", "string", "http://localhost:9200"),
                param("index", "Index name pattern", "string", "events-{yyyy-MM-dd}"),
            ],
            optional_config: vec![
                param("username", "Auth username", "string", "elastic"),
                param("password", "Auth password", "string", "changeme"),
                param("batch_size", "Bulk index batch size", "integer", "1000"),
            ],
            example_yaml: "type: elasticsearch-sink\nconfig:\n  hosts: http://localhost:9200\n  index: events".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/elasticsearch-sink".into(),
            stability: Stability::Beta,
        },
        ConnectorEntry {
            id: "datadog-sink".into(),
            name: "Datadog Sink".into(),
            description: "Forward metrics and logs to Datadog".into(),
            direction: ConnectorDirection::Sink,
            category: ConnectorCategory::Monitoring,
            version: "0.2.0".into(),
            required_config: vec![
                param("api_key", "Datadog API key", "string", "your-api-key"),
            ],
            optional_config: vec![
                param("site", "Datadog site (datadoghq.com, datadoghq.eu)", "string", "datadoghq.com"),
                param("data_type", "Data type (logs, metrics)", "string", "logs"),
            ],
            example_yaml: "type: datadog-sink\nconfig:\n  api_key: ${DD_API_KEY}".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/datadog-sink".into(),
            stability: Stability::Experimental,
        },

        // ===== MESSAGING =====
        ConnectorEntry {
            id: "slack-sink".into(),
            name: "Slack Sink".into(),
            description: "Send messages to Slack channels via webhook".into(),
            direction: ConnectorDirection::Sink,
            category: ConnectorCategory::Messaging,
            version: "0.2.0".into(),
            required_config: vec![
                param("webhook_url", "Slack webhook URL", "string", "https://hooks.slack.com/services/xxx"),
            ],
            optional_config: vec![
                param("channel", "Override channel", "string", "#alerts"),
                param("username", "Bot username", "string", "Streamline Bot"),
                param("template", "Message template (Go template)", "string", "New event: {{.value}}"),
            ],
            example_yaml: "type: slack-sink\nconfig:\n  webhook_url: https://hooks.slack.com/services/xxx".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/slack-sink".into(),
            stability: Stability::Stable,
        },
        ConnectorEntry {
            id: "pagerduty-sink".into(),
            name: "PagerDuty Sink".into(),
            description: "Create PagerDuty incidents from stream events".into(),
            direction: ConnectorDirection::Sink,
            category: ConnectorCategory::Messaging,
            version: "0.2.0".into(),
            required_config: vec![
                param("routing_key", "PagerDuty Events API v2 routing key", "string", "your-routing-key"),
            ],
            optional_config: vec![
                param("severity", "Default severity (critical, error, warning, info)", "string", "error"),
                param("dedup_key_field", "JSON field for deduplication key", "string", "alert_id"),
            ],
            example_yaml: "type: pagerduty-sink\nconfig:\n  routing_key: ${PD_ROUTING_KEY}".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/pagerduty-sink".into(),
            stability: Stability::Stable,
        },
        ConnectorEntry {
            id: "kafka-source".into(),
            name: "Kafka Source (Mirror)".into(),
            description: "Mirror topics from an Apache Kafka cluster into Streamline".into(),
            direction: ConnectorDirection::Source,
            category: ConnectorCategory::Messaging,
            version: "0.2.0".into(),
            required_config: vec![
                param("bootstrap_servers", "Kafka bootstrap servers", "string", "kafka:9092"),
            ],
            optional_config: vec![
                param("topics", "Topics to mirror (comma-separated, or regex)", "string", ".*"),
                param("group_id", "Consumer group ID", "string", "streamline-mirror"),
                param("auto_offset_reset", "Start from (earliest, latest)", "string", "earliest"),
            ],
            example_yaml: "type: kafka-source\nconfig:\n  bootstrap_servers: kafka:9092\n  topics: events,orders".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/kafka-source".into(),
            stability: Stability::Beta,
        },

        // ===== DATA WAREHOUSE =====
        ConnectorEntry {
            id: "snowflake-sink".into(),
            name: "Snowflake Sink".into(),
            description: "Load streaming data into Snowflake tables via Snowpipe".into(),
            direction: ConnectorDirection::Sink,
            category: ConnectorCategory::DataWarehouse,
            version: "0.2.0".into(),
            required_config: vec![
                param("account", "Snowflake account identifier", "string", "myorg-account"),
                param("database", "Database name", "string", "EVENTS_DB"),
                param("schema", "Schema name", "string", "PUBLIC"),
                param("table", "Table name", "string", "RAW_EVENTS"),
            ],
            optional_config: vec![
                param("warehouse", "Compute warehouse", "string", "EVENTS_WH"),
                param("role", "Role for access", "string", "EVENTS_LOADER"),
                param("private_key_path", "Path to private key for key pair auth", "string", "/secrets/rsa_key.p8"),
            ],
            example_yaml: "type: snowflake-sink\nconfig:\n  account: myorg-account\n  database: EVENTS_DB\n  schema: PUBLIC\n  table: RAW_EVENTS".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/snowflake-sink".into(),
            stability: Stability::Experimental,
        },
        ConnectorEntry {
            id: "bigquery-sink".into(),
            name: "BigQuery Sink".into(),
            description: "Stream data into Google BigQuery tables".into(),
            direction: ConnectorDirection::Sink,
            category: ConnectorCategory::DataWarehouse,
            version: "0.2.0".into(),
            required_config: vec![
                param("project", "GCP project ID", "string", "my-project"),
                param("dataset", "BigQuery dataset", "string", "events"),
                param("table", "BigQuery table", "string", "raw_events"),
            ],
            optional_config: vec![
                param("credentials_file", "Service account JSON path", "string", "/secrets/gcp.json"),
                param("batch_size", "Rows per insert", "integer", "500"),
            ],
            example_yaml: "type: bigquery-sink\nconfig:\n  project: my-project\n  dataset: events\n  table: raw_events".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/bigquery-sink".into(),
            stability: Stability::Experimental,
        },
        ConnectorEntry {
            id: "redis-sink".into(),
            name: "Redis Sink".into(),
            description: "Write messages to Redis (SET, HSET, PUBLISH, XADD)".into(),
            direction: ConnectorDirection::Sink,
            category: ConnectorCategory::Database,
            version: "0.2.0".into(),
            required_config: vec![
                param("url", "Redis connection URL", "string", "redis://localhost:6379"),
            ],
            optional_config: vec![
                param("command", "Redis command (SET, HSET, PUBLISH, XADD)", "string", "XADD"),
                param("key_field", "JSON field for Redis key", "string", "id"),
                param("stream_name", "Redis stream name (for XADD)", "string", "events"),
            ],
            example_yaml: "type: redis-sink\nconfig:\n  url: redis://localhost:6379\n  command: XADD\n  stream_name: events".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/redis-sink".into(),
            stability: Stability::Beta,
        },

        // ===== FILE SYSTEM =====
        ConnectorEntry {
            id: "file-source".into(),
            name: "File Source".into(),
            description: "Read lines from files (supports tail -f mode)".into(),
            direction: ConnectorDirection::Source,
            category: ConnectorCategory::FileSystem,
            version: "0.2.0".into(),
            required_config: vec![
                param("path", "File path or glob pattern", "string", "/var/log/*.log"),
            ],
            optional_config: vec![
                param("mode", "Read mode (full, tail)", "string", "tail"),
                param("delimiter", "Line delimiter", "string", "\\n"),
            ],
            example_yaml: "type: file-source\nconfig:\n  path: /var/log/app.log\n  mode: tail".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/file-source".into(),
            stability: Stability::Stable,
        },
        ConnectorEntry {
            id: "sqs-source".into(),
            name: "AWS SQS Source".into(),
            description: "Consume messages from Amazon SQS queues".into(),
            direction: ConnectorDirection::Source,
            category: ConnectorCategory::Messaging,
            version: "0.2.0".into(),
            required_config: vec![
                param("queue_url", "SQS queue URL", "string", "https://sqs.us-east-1.amazonaws.com/123/my-queue"),
                param("region", "AWS region", "string", "us-east-1"),
            ],
            optional_config: vec![
                param("max_messages", "Max messages per poll", "integer", "10"),
                param("wait_time_secs", "Long poll wait time", "integer", "20"),
                param("visibility_timeout", "Message visibility timeout", "integer", "30"),
            ],
            example_yaml: "type: sqs-source\nconfig:\n  queue_url: https://sqs.us-east-1.amazonaws.com/123/my-queue\n  region: us-east-1".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/sqs-source".into(),
            stability: Stability::Experimental,
        },

        // ===== INTERNAL =====
        ConnectorEntry {
            id: "streamline-topic".into(),
            name: "Streamline Topic".into(),
            description: "Route messages between Streamline topics (internal routing)".into(),
            direction: ConnectorDirection::Both,
            category: ConnectorCategory::Messaging,
            version: "0.2.0".into(),
            required_config: vec![],
            optional_config: vec![
                param("topic", "Target topic name", "string", "output-events"),
                param("topic_prefix", "Prefix for auto-generated topic names", "string", "cdc-"),
                param("partitions", "Default partition count for new topics", "integer", "6"),
            ],
            example_yaml: "type: streamline-topic\nconfig:\n  topic: processed-events\n  partitions: 6".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/streamline-topic".into(),
            stability: Stability::Stable,
        },

        // ===== DEBUG =====
        ConnectorEntry {
            id: "console-sink".into(),
            name: "Console Sink".into(),
            description: "Print messages to stdout (useful for debugging pipelines)".into(),
            direction: ConnectorDirection::Sink,
            category: ConnectorCategory::Debug,
            version: "0.2.0".into(),
            required_config: vec![],
            optional_config: vec![
                param("format", "Output format (json, text, raw)", "string", "json"),
                param("prefix", "Line prefix", "string", "[connector]"),
            ],
            example_yaml: "type: console-sink\nconfig:\n  format: json".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/console-sink".into(),
            stability: Stability::Stable,
        },

        // ===== CLOUD MESSAGING SOURCES =====
        ConnectorEntry {
            id: "kinesis-source".into(),
            name: "AWS Kinesis Source".into(),
            description: "Consume records from Amazon Kinesis Data Streams".into(),
            direction: ConnectorDirection::Source,
            category: ConnectorCategory::Messaging,
            version: "0.2.0".into(),
            required_config: vec![
                param("stream_name", "Kinesis stream name", "string", "my-stream"),
                param("region", "AWS region", "string", "us-east-1"),
            ],
            optional_config: vec![
                param("iterator_type", "TRIM_HORIZON or LATEST", "string", "TRIM_HORIZON"),
                param("checkpoint_interval_ms", "Checkpoint interval", "integer", "60000"),
            ],
            example_yaml: "type: kinesis-source\nconfig:\n  stream_name: my-stream\n  region: us-east-1".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/kinesis-source".into(),
            stability: Stability::Experimental,
        },
        ConnectorEntry {
            id: "pubsub-source".into(),
            name: "Google Pub/Sub Source".into(),
            description: "Consume messages from Google Cloud Pub/Sub subscriptions".into(),
            direction: ConnectorDirection::Source,
            category: ConnectorCategory::Messaging,
            version: "0.2.0".into(),
            required_config: vec![
                param("project", "GCP project ID", "string", "my-project"),
                param("subscription", "Pub/Sub subscription name", "string", "my-sub"),
            ],
            optional_config: vec![
                param("credentials_file", "Service account JSON", "string", "/secrets/gcp.json"),
                param("max_messages", "Max messages per pull", "integer", "100"),
            ],
            example_yaml: "type: pubsub-source\nconfig:\n  project: my-project\n  subscription: my-sub".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/pubsub-source".into(),
            stability: Stability::Experimental,
        },
        ConnectorEntry {
            id: "eventhubs-source".into(),
            name: "Azure Event Hubs Source".into(),
            description: "Consume events from Azure Event Hubs using AMQP protocol".into(),
            direction: ConnectorDirection::Source,
            category: ConnectorCategory::Messaging,
            version: "0.2.0".into(),
            required_config: vec![
                param("connection_string", "Event Hubs connection string", "string", "Endpoint=sb://..."),
                param("event_hub", "Event Hub name", "string", "my-hub"),
            ],
            optional_config: vec![
                param("consumer_group", "Consumer group", "string", "$Default"),
            ],
            example_yaml: "type: eventhubs-source\nconfig:\n  connection_string: Endpoint=sb://...\n  event_hub: my-hub".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/eventhubs-source".into(),
            stability: Stability::Experimental,
        },

        // ===== ADDITIONAL DATABASE SINKS =====
        ConnectorEntry {
            id: "dynamodb-sink".into(),
            name: "Amazon DynamoDB Sink".into(),
            description: "Write messages to DynamoDB tables with configurable key mapping".into(),
            direction: ConnectorDirection::Sink,
            category: ConnectorCategory::Database,
            version: "0.2.0".into(),
            required_config: vec![
                param("table", "DynamoDB table name", "string", "events"),
                param("region", "AWS region", "string", "us-east-1"),
            ],
            optional_config: vec![
                param("partition_key_field", "JSON field for partition key", "string", "id"),
                param("sort_key_field", "JSON field for sort key", "string", "timestamp"),
                param("batch_size", "Batch write size", "integer", "25"),
            ],
            example_yaml: "type: dynamodb-sink\nconfig:\n  table: events\n  region: us-east-1".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/dynamodb-sink".into(),
            stability: Stability::Experimental,
        },
        ConnectorEntry {
            id: "postgres-sink".into(),
            name: "PostgreSQL Sink".into(),
            description: "Insert or upsert messages into PostgreSQL tables".into(),
            direction: ConnectorDirection::Sink,
            category: ConnectorCategory::Database,
            version: "0.2.0".into(),
            required_config: vec![
                param("connection_url", "PostgreSQL connection URL", "string", "postgresql://user:pass@localhost:5432/db"),
                param("table", "Target table name", "string", "events"),
            ],
            optional_config: vec![
                param("upsert_key", "Column(s) for upsert conflict resolution", "string", "id"),
                param("batch_size", "Insert batch size", "integer", "100"),
                param("schema", "Database schema", "string", "public"),
            ],
            example_yaml: "type: postgres-sink\nconfig:\n  connection_url: postgresql://localhost:5432/db\n  table: events".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/postgres-sink".into(),
            stability: Stability::Beta,
        },

        // ===== ADDITIONAL FILE/TRANSFER =====
        ConnectorEntry {
            id: "sftp-source".into(),
            name: "SFTP Source".into(),
            description: "Poll an SFTP server for new files and emit their contents as messages".into(),
            direction: ConnectorDirection::Source,
            category: ConnectorCategory::FileSystem,
            version: "0.2.0".into(),
            required_config: vec![
                param("host", "SFTP hostname", "string", "sftp.example.com"),
                param("username", "SFTP username", "string", "user"),
                param("remote_path", "Remote directory to watch", "string", "/uploads/"),
            ],
            optional_config: vec![
                param("password", "SFTP password", "string", ""),
                param("private_key_path", "SSH private key path", "string", "~/.ssh/id_rsa"),
                param("poll_interval_ms", "Poll interval", "integer", "60000"),
                param("pattern", "Filename glob pattern", "string", "*.csv"),
            ],
            example_yaml: "type: sftp-source\nconfig:\n  host: sftp.example.com\n  username: user\n  remote_path: /uploads/".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/sftp-source".into(),
            stability: Stability::Experimental,
        },

        // ===== ADDITIONAL MONITORING =====
        ConnectorEntry {
            id: "prometheus-sink".into(),
            name: "Prometheus Remote Write Sink".into(),
            description: "Forward stream metrics to Prometheus via remote write API".into(),
            direction: ConnectorDirection::Sink,
            category: ConnectorCategory::Monitoring,
            version: "0.2.0".into(),
            required_config: vec![
                param("endpoint", "Prometheus remote write URL", "string", "http://prometheus:9090/api/v1/write"),
            ],
            optional_config: vec![
                param("metric_name_field", "JSON field for metric name", "string", "metric"),
                param("value_field", "JSON field for metric value", "string", "value"),
                param("labels_field", "JSON field for labels", "string", "labels"),
            ],
            example_yaml: "type: prometheus-sink\nconfig:\n  endpoint: http://prometheus:9090/api/v1/write".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/prometheus-sink".into(),
            stability: Stability::Experimental,
        },
        ConnectorEntry {
            id: "clickhouse-sink".into(),
            name: "ClickHouse Sink".into(),
            description: "Insert messages into ClickHouse tables via HTTP interface for analytics workloads".into(),
            direction: ConnectorDirection::Sink,
            category: ConnectorCategory::DataWarehouse,
            version: "0.2.0".into(),
            required_config: vec![
                param("url", "ClickHouse HTTP URL", "string", "http://localhost:8123"),
                param("database", "Database name", "string", "default"),
                param("table", "Table name", "string", "events"),
            ],
            optional_config: vec![
                param("username", "Auth username", "string", "default"),
                param("password", "Auth password", "string", ""),
                param("batch_size", "Insert batch size", "integer", "1000"),
                param("format", "Insert format (JSONEachRow, CSV)", "string", "JSONEachRow"),
            ],
            example_yaml: "type: clickhouse-sink\nconfig:\n  url: http://localhost:8123\n  database: default\n  table: events".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/clickhouse-sink".into(),
            stability: Stability::Experimental,
        },
        ConnectorEntry {
            id: "mqtt-source".into(),
            name: "MQTT Source".into(),
            description: "Subscribe to MQTT topics and forward messages to Streamline topics. Alternative to the built-in MQTT gateway for pull-based ingestion".into(),
            direction: ConnectorDirection::Source,
            category: ConnectorCategory::Messaging,
            version: "0.2.0".into(),
            required_config: vec![
                param("broker_url", "MQTT broker URL", "string", "mqtt://localhost:1883"),
                param("topics", "MQTT topic patterns (comma-separated)", "string", "sensors/#,devices/+/telemetry"),
            ],
            optional_config: vec![
                param("client_id", "MQTT client ID", "string", "streamline-connector"),
                param("qos", "Quality of Service (0, 1, 2)", "integer", "1"),
                param("username", "MQTT username", "string", ""),
                param("password", "MQTT password", "string", ""),
            ],
            example_yaml: "type: mqtt-source\nconfig:\n  broker_url: mqtt://localhost:1883\n  topics: sensors/#".into(),
            docs_url: "https://streamlinelabs.dev/docs/connectors/mqtt-source".into(),
            stability: Stability::Beta,
        },
    ]
}

fn param(name: &str, description: &str, param_type: &str, example: &str) -> ConfigParam {
    ConfigParam {
        name: name.to_string(),
        description: description.to_string(),
        param_type: param_type.to_string(),
        default: None,
        example: Some(example.to_string()),
    }
}

/// Search connectors by query string
pub fn search_connectors(query: &str) -> Vec<&'static ConnectorEntry> {
    let catalog = Box::leak(Box::new(connector_catalog()));
    let q = query.to_lowercase();
    catalog
        .iter()
        .filter(|c| {
            c.id.to_lowercase().contains(&q)
                || c.name.to_lowercase().contains(&q)
                || c.description.to_lowercase().contains(&q)
                || c.category.to_string().to_lowercase().contains(&q)
        })
        .collect()
}

/// Get a connector by ID
pub fn get_connector(id: &str) -> Option<ConnectorEntry> {
    connector_catalog().into_iter().find(|c| c.id == id)
}

/// Generate a scaffold YAML manifest for a pipeline
pub fn scaffold_pipeline(source_id: &str, sink_id: &str, name: &str) -> Result<String, String> {
    let source = get_connector(source_id)
        .ok_or_else(|| format!("Unknown source connector: {}", source_id))?;
    let sink = get_connector(sink_id)
        .ok_or_else(|| format!("Unknown sink connector: {}", sink_id))?;

    if source.direction == ConnectorDirection::Sink {
        return Err(format!("'{}' is a sink-only connector", source_id));
    }
    if sink.direction == ConnectorDirection::Source {
        return Err(format!("'{}' is a source-only connector", sink_id));
    }

    let source_config = source
        .required_config
        .iter()
        .map(|p| format!("      {}: {} # {}", p.name, p.example.as_deref().unwrap_or("<required>"), p.description))
        .collect::<Vec<_>>()
        .join("\n");

    let sink_config = sink
        .required_config
        .iter()
        .map(|p| format!("      {}: {} # {}", p.name, p.example.as_deref().unwrap_or("<required>"), p.description))
        .collect::<Vec<_>>()
        .join("\n");

    Ok(format!(
        r#"# Connector Pipeline: {name}
# Source: {source_name} ({source_id})
# Sink: {sink_name} ({sink_id})
#
# Deploy: streamline-cli connector deploy -f {name}.yaml

apiVersion: streamline.io/v1
kind: ConnectorPipeline
metadata:
  name: {name}
spec:
  source:
    type: {source_id}
    config:
{source_config}
  transforms: []
    # Uncomment to add transforms:
    # - type: field-filter
    #   config:
    #     include: [id, name, timestamp]
    # - type: pii-redactor
    #   config:
    #     fields: [email, phone]
  sink:
    type: {sink_id}
    config:
{sink_config}
  error_handling:
    dead_letter_topic: dlq-{name}
    max_retries: 3
    retry_backoff_ms: 1000
  parallelism: 1
"#,
        name = name,
        source_name = source.name,
        source_id = source_id,
        sink_name = sink.name,
        sink_id = sink_id,
        source_config = source_config,
        sink_config = sink_config,
    ))
}

/// Format the connector catalog as a markdown table
pub fn catalog_as_markdown() -> String {
    let catalog = connector_catalog();
    let mut md = String::from("| Connector | Direction | Category | Stability | Description |\n");
    md.push_str("|-----------|-----------|----------|-----------|-------------|\n");
    for c in &catalog {
        let dir = match c.direction {
            ConnectorDirection::Source => "Source",
            ConnectorDirection::Sink => "Sink",
            ConnectorDirection::Both => "Both",
        };
        let stab = match c.stability {
            Stability::Stable => "🟢 Stable",
            Stability::Beta => "🟡 Beta",
            Stability::Experimental => "🔴 Experimental",
        };
        md.push_str(&format!("| {} | {} | {} | {} | {} |\n", c.name, dir, c.category, stab, c.description));
    }
    md
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_has_30_connectors() {
        let catalog = connector_catalog();
        assert!(catalog.len() >= 30, "Expected 30+ connectors, got {}", catalog.len());
    }

    #[test]
    fn test_unique_ids() {
        let catalog = connector_catalog();
        let mut ids: Vec<&str> = catalog.iter().map(|c| c.id.as_str()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), catalog.len(), "Duplicate connector IDs found");
    }

    #[test]
    fn test_get_connector() {
        assert!(get_connector("postgres-cdc").is_some());
        assert!(get_connector("nonexistent").is_none());
    }

    #[test]
    fn test_scaffold_pipeline() {
        let yaml = scaffold_pipeline("postgres-cdc", "s3-sink", "pg-to-s3").unwrap();
        assert!(yaml.contains("postgres-cdc"));
        assert!(yaml.contains("s3-sink"));
        assert!(yaml.contains("pg-to-s3"));
        assert!(yaml.contains("connection_url"));
        assert!(yaml.contains("bucket"));
    }

    #[test]
    fn test_scaffold_rejects_wrong_direction() {
        let result = scaffold_pipeline("console-sink", "s3-sink", "test");
        assert!(result.is_err());
    }

    #[test]
    fn test_catalog_markdown() {
        let md = catalog_as_markdown();
        assert!(md.contains("PostgreSQL CDC"));
        assert!(md.contains("S3"));
        assert!(md.contains("Slack"));
    }
}
