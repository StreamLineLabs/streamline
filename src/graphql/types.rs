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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_to_message_basic() {
        let record = crate::storage::record::Record {
            offset: 42,
            timestamp: 1700000000000,
            key: Some(b"my-key".to_vec()),
            value: b"hello world".to_vec(),
            headers: vec![],
        };

        let msg = record_to_message("events", 0, record);
        assert_eq!(msg.topic, "events");
        assert_eq!(msg.partition, 0);
        assert_eq!(msg.offset, 42);
        assert_eq!(msg.key, Some("my-key".to_string()));
        assert_eq!(msg.value, "hello world");
        assert!(msg.headers.is_empty());
    }

    #[test]
    fn test_record_to_message_no_key() {
        let record = crate::storage::record::Record {
            offset: 0,
            timestamp: 1700000000000,
            key: None,
            value: b"data".to_vec(),
            headers: vec![],
        };

        let msg = record_to_message("logs", 3, record);
        assert!(msg.key.is_none());
        assert_eq!(msg.partition, 3);
    }

    #[test]
    fn test_record_to_message_with_headers() {
        let record = crate::storage::record::Record {
            offset: 10,
            timestamp: 1700000000000,
            key: None,
            value: b"v".to_vec(),
            headers: vec![
                crate::storage::record::RecordHeader {
                    key: "trace-id".to_string(),
                    value: b"abc-123".to_vec(),
                },
                crate::storage::record::RecordHeader {
                    key: "content-type".to_string(),
                    value: b"application/json".to_vec(),
                },
            ],
        };

        let msg = record_to_message("events", 0, record);
        assert_eq!(msg.headers.len(), 2);
        assert_eq!(msg.headers[0].key, "trace-id");
        assert_eq!(msg.headers[0].value, "abc-123");
        assert_eq!(msg.headers[1].key, "content-type");
        assert_eq!(msg.headers[1].value, "application/json");
    }

    #[test]
    fn test_record_to_message_non_utf8_value() {
        let record = crate::storage::record::Record {
            offset: 0,
            timestamp: 1700000000000,
            key: None,
            value: vec![0xFF, 0xFE, 0xFD],
            headers: vec![],
        };

        let msg = record_to_message("binary", 0, record);
        // from_utf8_lossy should handle non-UTF8 gracefully
        assert!(!msg.value.is_empty());
    }

    #[test]
    fn test_record_to_message_timestamp_formatting() {
        let record = crate::storage::record::Record {
            offset: 0,
            timestamp: 1700000000000, // 2023-11-14T22:13:20Z
            key: None,
            value: b"v".to_vec(),
            headers: vec![],
        };

        let msg = record_to_message("t", 0, record);
        assert!(msg.timestamp.contains("2023"));
        assert!(msg.timestamp.contains("T"));
    }

    #[test]
    fn test_topic_type_construction() {
        let topic = Topic {
            name: "events".to_string(),
            partitions: 6,
            replication_factor: 3,
            message_count: 1000,
            retention_ms: Some(86400000),
            created_at: "2024-01-01T00:00:00Z".to_string(),
        };
        assert_eq!(topic.name, "events");
        assert_eq!(topic.partitions, 6);
        assert_eq!(topic.retention_ms, Some(86400000));
    }

    #[test]
    fn test_group_state_enum() {
        assert_ne!(GroupState::Stable, GroupState::Dead);
        assert_ne!(GroupState::Empty, GroupState::PreparingRebalance);
        assert_eq!(GroupState::Stable, GroupState::Stable);
    }

    #[test]
    fn test_topic_event_types() {
        assert_ne!(TopicEventType::Created, TopicEventType::Deleted);
    }

    #[test]
    fn test_cluster_event_types() {
        let event = ClusterEvent {
            event_type: ClusterEventType::TopicCreated,
            description: "Topic 'events' created".to_string(),
            resource: Some("events".to_string()),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
        };
        assert_eq!(event.event_type, ClusterEventType::TopicCreated);
        assert_eq!(event.resource, Some("events".to_string()));
    }

    #[test]
    fn test_produce_input_construction() {
        let input = ProduceInput {
            key: Some("k".to_string()),
            value: "v".to_string(),
            headers: Some(vec![HeaderInput {
                key: "h1".to_string(),
                value: "val".to_string(),
            }]),
            partition: Some(2),
        };
        assert_eq!(input.key, Some("k".to_string()));
        assert_eq!(input.partition, Some(2));
        assert_eq!(input.headers.unwrap().len(), 1);
    }

    #[test]
    fn test_search_result_construction() {
        let result = SearchResult {
            matches: vec![],
            total_matches: 0,
            scanned: 1000,
        };
        assert!(result.matches.is_empty());
        assert_eq!(result.scanned, 1000);
    }

    #[test]
    fn test_partition_stats() {
        let p = Partition {
            id: 0,
            earliest_offset: 0,
            latest_offset: 500,
            message_count: 500,
        };
        assert_eq!(p.message_count, p.latest_offset - p.earliest_offset);
    }

    #[test]
    fn test_topic_config_optional_fields() {
        let config = TopicConfig {
            retention_ms: None,
            max_message_bytes: None,
        };
        assert!(config.retention_ms.is_none());
        assert!(config.max_message_bytes.is_none());

        let config2 = TopicConfig {
            retention_ms: Some(3600000),
            max_message_bytes: Some(1048576),
        };
        assert_eq!(config2.retention_ms, Some(3600000));
    }
}
