//! GraphQL type definitions for the Streamline API
//!
//! Defines all GraphQL object types, input types, and enums used across
//! queries, mutations, and subscriptions.

use async_graphql::{Enum, InputObject, SimpleObject};

// =============================================================================
// Output Types
// =============================================================================

/// Topic information returned by queries
#[derive(SimpleObject, Clone, Debug)]
pub struct Topic {
    /// Topic name
    pub name: String,
    /// Number of partitions
    pub partitions: i32,
    /// Replication factor
    pub replication_factor: i32,
    /// Total message count across all partitions
    pub message_count: i64,
    /// Retention time in milliseconds (None = infinite)
    pub retention_ms: Option<i64>,
    /// ISO 8601 creation timestamp
    pub created_at: String,
}

/// A single message/record in a topic
#[derive(SimpleObject, Clone, Debug)]
pub struct Message {
    /// Topic name
    pub topic: String,
    /// Partition number
    pub partition: i32,
    /// Offset within the partition
    pub offset: i64,
    /// Message key (if present)
    pub key: Option<String>,
    /// Message value (UTF-8 string)
    pub value: String,
    /// Timestamp as ISO 8601 string
    pub timestamp: String,
    /// Message headers
    pub headers: Vec<Header>,
}

/// Message header key-value pair
#[derive(SimpleObject, Clone, Debug)]
pub struct Header {
    /// Header key
    pub key: String,
    /// Header value (UTF-8 string)
    pub value: String,
}

/// Result of producing a message
#[derive(SimpleObject, Clone, Debug)]
pub struct ProduceResult {
    /// Topic the message was produced to
    pub topic: String,
    /// Partition the message was written to
    pub partition: i32,
    /// Offset assigned to the message
    pub offset: i64,
    /// Timestamp as ISO 8601 string
    pub timestamp: String,
}

/// Partition-level statistics
#[derive(SimpleObject, Clone, Debug)]
pub struct Partition {
    /// Partition ID
    pub id: i32,
    /// Earliest offset in the partition
    pub earliest_offset: i64,
    /// Latest offset (next offset to be written)
    pub latest_offset: i64,
    /// Number of messages in the partition
    pub message_count: i64,
}

/// Real-time metrics for a specific topic
#[derive(SimpleObject, Clone, Debug)]
pub struct TopicMetrics {
    /// Topic name
    pub topic: String,
    /// Total message count across all partitions
    pub message_count: i64,
    /// Number of partitions
    pub partition_count: i32,
    /// Per-partition statistics
    pub partitions: Vec<Partition>,
    /// Timestamp of the snapshot as ISO 8601 string
    pub timestamp: String,
}

/// Cluster health and information
#[derive(SimpleObject, Clone, Debug)]
pub struct ClusterInfo {
    /// Node/broker ID
    pub node_id: i32,
    /// Server version
    pub version: String,
    /// Uptime in seconds
    pub uptime: i64,
    /// Number of topics
    pub topic_count: i32,
}

/// Consumer group information
#[derive(SimpleObject, Clone, Debug)]
pub struct ConsumerGroup {
    /// Group identifier
    pub group_id: String,
    /// Current group state
    pub state: GroupState,
    /// Protocol type (e.g., "consumer")
    pub protocol_type: String,
    /// Number of members in the group
    pub member_count: i32,
}

/// Consumer group state
#[derive(Enum, Copy, Clone, Debug, Eq, PartialEq)]
pub enum GroupState {
    /// No active members
    Empty,
    /// Awaiting members for rebalance
    PreparingRebalance,
    /// Rebalance in progress
    CompletingRebalance,
    /// Group is stable and consuming
    Stable,
    /// Group has been removed
    Dead,
}

/// Snapshot of server metrics for real-time subscription
#[derive(SimpleObject, Clone, Debug)]
pub struct MetricsSnapshot {
    /// Total messages across all topics
    pub total_messages: i64,
    /// Total number of topics
    pub topic_count: i32,
    /// Timestamp of the snapshot as ISO 8601 string
    pub timestamp: String,
}

/// Event emitted when a topic is created or deleted
#[derive(SimpleObject, Clone, Debug)]
pub struct TopicEvent {
    /// Type of event
    pub event_type: TopicEventType,
    /// Name of the topic
    pub topic_name: String,
    /// Timestamp of the event as ISO 8601 string
    pub timestamp: String,
}

/// Type of topic lifecycle event
#[derive(Enum, Copy, Clone, Debug, Eq, PartialEq)]
pub enum TopicEventType {
    /// A new topic was created
    Created,
    /// A topic was deleted
    Deleted,
}

/// Time-windowed statistics for a topic
#[derive(SimpleObject, Clone, Debug)]
pub struct TopicStatsWindow {
    /// Topic name
    pub name: String,
    /// Number of partitions
    pub partition_count: i32,
    /// Total messages across all partitions
    pub total_messages: i64,
    /// Messages within the requested time window
    pub messages_in_window: i64,
    /// Approximate message rate (messages per second) within the window
    pub message_rate: f64,
    /// Per-partition statistics
    pub partitions: Vec<Partition>,
    /// Requested time window (e.g. "1m", "5m", "1h")
    pub window: String,
    /// Timestamp of the snapshot as ISO 8601 string
    pub timestamp: String,
}

/// Search result for content-based message search
#[derive(SimpleObject, Clone, Debug)]
pub struct SearchResult {
    /// Messages matching the search query
    pub matches: Vec<Message>,
    /// Total number of matches found (may be less than scanned)
    pub total_matches: i32,
    /// Number of messages scanned
    pub scanned: i64,
}

/// Result of producing a message with schema validation
#[derive(SimpleObject, Clone, Debug)]
pub struct ProduceWithSchemaResult {
    /// Topic the message was produced to
    pub topic: String,
    /// Partition the message was written to
    pub partition: i32,
    /// Offset assigned to the message
    pub offset: i64,
    /// Whether schema validation passed
    pub schema_validated: bool,
    /// Timestamp as ISO 8601 string
    pub timestamp: String,
}

/// Cluster state change event for the clusterEvents subscription
#[derive(SimpleObject, Clone, Debug)]
pub struct ClusterEvent {
    /// Type of cluster event
    pub event_type: ClusterEventType,
    /// Human-readable description of the event
    pub description: String,
    /// Affected resource name (topic, node, etc.)
    pub resource: Option<String>,
    /// Timestamp of the event as ISO 8601 string
    pub timestamp: String,
}

/// Types of cluster state change events
#[derive(Enum, Copy, Clone, Debug, Eq, PartialEq)]
pub enum ClusterEventType {
    /// A new topic was created
    TopicCreated,
    /// A topic was deleted
    TopicDeleted,
    /// Partitions were added to a topic
    PartitionsAdded,
    /// Topic message count changed significantly
    TopicActivity,
    /// A new node joined the cluster
    NodeJoined,
    /// A node left the cluster
    NodeLeft,
}

// =============================================================================
// Input Types
// =============================================================================

/// Input for producing a message
#[derive(InputObject, Debug)]
pub struct ProduceInput {
    /// Message key (optional, used for partition routing)
    pub key: Option<String>,
    /// Message value (required)
    pub value: String,
    /// Optional headers
    pub headers: Option<Vec<HeaderInput>>,
    /// Target partition (optional, auto-assigned if omitted)
    pub partition: Option<i32>,
}

/// Header input for message production
#[derive(InputObject, Debug)]
pub struct HeaderInput {
    /// Header key
    pub key: String,
    /// Header value
    pub value: String,
}

/// Optional configuration for topic creation
#[derive(InputObject, Debug)]
pub struct TopicConfig {
    /// Retention time in milliseconds
    pub retention_ms: Option<i64>,
    /// Maximum message size in bytes
    pub max_message_bytes: Option<i32>,
}

// =============================================================================
// Conversion helpers
// =============================================================================

/// Convert a storage Record to a GraphQL Message
pub fn record_to_message(
    topic: &str,
    partition: i32,
    record: crate::storage::record::Record,
) -> Message {
    let value = String::from_utf8_lossy(&record.value).to_string();
    let key = record
        .key
        .as_ref()
        .map(|k| String::from_utf8_lossy(k).to_string());
    let timestamp = chrono::DateTime::from_timestamp_millis(record.timestamp)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default();
    let headers = record
        .headers
        .iter()
        .map(|h| Header {
            key: h.key.clone(),
            value: String::from_utf8_lossy(&h.value).to_string(),
        })
        .collect();

    Message {
        topic: topic.to_string(),
        partition,
        offset: record.offset,
        key,
        value,
        timestamp,
        headers,
    }
}
