//! MCP Resource implementations for Streamline
//!
//! Exposes Streamline topics, consumer groups, and configuration
//! as MCP resources that AI agents can read.

use crate::error::{Result, StreamlineError};
use crate::mcp::{ResourceContent, ResourceDefinition, ResourceTemplate};
use crate::storage::TopicManager;
use std::sync::Arc;

/// List all available resources
pub async fn list_resources(topic_manager: &Arc<TopicManager>) -> Result<Vec<ResourceDefinition>> {
    let mut resources = Vec::new();

    // Add a resource for each topic
    let topics = topic_manager.list_topics()?;
    for meta in &topics {
        resources.push(ResourceDefinition {
            uri: format!("streamline://topics/{}", meta.name),
            name: format!("Topic: {}", meta.name),
            description: Some(format!(
                "Streaming topic '{}' - metadata and recent messages",
                meta.name
            )),
            mime_type: Some("application/json".to_string()),
        });
    }

    // Add server info resource
    resources.push(ResourceDefinition {
        uri: "streamline://server/info".to_string(),
        name: "Server Info".to_string(),
        description: Some("Streamline server information and configuration".to_string()),
        mime_type: Some("application/json".to_string()),
    });

    // Add topics overview resource
    resources.push(ResourceDefinition {
        uri: "streamline://topics".to_string(),
        name: "All Topics".to_string(),
        description: Some("Overview of all topics with partition counts and offsets".to_string()),
        mime_type: Some("application/json".to_string()),
    });

    Ok(resources)
}

/// List resource templates
pub fn list_resource_templates() -> Vec<ResourceTemplate> {
    vec![
        ResourceTemplate {
            uri_template: "streamline://topics/{topic}".to_string(),
            name: "Topic Details".to_string(),
            description: Some("Get metadata and recent messages for a specific topic".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceTemplate {
            uri_template: "streamline://topics/{topic}/partitions/{partition}".to_string(),
            name: "Partition Details".to_string(),
            description: Some("Get detailed information about a specific partition".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceTemplate {
            uri_template: "streamline://topics/{topic}/messages?offset={offset}&count={count}"
                .to_string(),
            name: "Topic Messages".to_string(),
            description: Some("Read messages from a topic at a specific offset".to_string()),
            mime_type: Some("application/json".to_string()),
        },
    ]
}

/// Read a specific resource by URI
pub async fn read_resource(
    uri: &str,
    topic_manager: &Arc<TopicManager>,
) -> Result<Vec<ResourceContent>> {
    // Parse the URI
    let path = uri
        .strip_prefix("streamline://")
        .ok_or_else(|| StreamlineError::Validation(format!("Invalid resource URI: {}", uri)))?;

    let parts: Vec<&str> = path.split('/').collect();

    match parts.as_slice() {
        ["server", "info"] => read_server_info(),
        ["topics"] => read_topics_overview(topic_manager),
        ["topics", topic_name] => read_topic_detail(topic_name, topic_manager),
        ["topics", topic_name, "partitions", partition] => {
            let partition: i32 = partition
                .parse()
                .map_err(|_| StreamlineError::Validation("Invalid partition number".into()))?;
            read_partition_detail(topic_name, partition, topic_manager)
        }
        _ => Err(StreamlineError::Validation(format!(
            "Unknown resource path: {}",
            path
        ))),
    }
}

fn read_server_info() -> Result<Vec<ResourceContent>> {
    let info = serde_json::json!({
        "name": "Streamline",
        "version": env!("CARGO_PKG_VERSION"),
        "description": "Kafka-compatible streaming platform",
        "protocol": "Kafka 3.x compatible",
        "features": {
            "kafka_apis": 50,
            "max_partitions_per_topic": 1024,
            "compression": ["lz4", "zstd", "snappy", "gzip"],
            "storage_modes": ["persistent", "in-memory", "hybrid"],
        },
    });

    Ok(vec![ResourceContent {
        uri: "streamline://server/info".to_string(),
        mime_type: Some("application/json".to_string()),
        text: Some(
            serde_json::to_string_pretty(&info)
                .map_err(|e| StreamlineError::Internal(e.to_string()))?,
        ),
        blob: None,
    }])
}

fn read_topics_overview(topic_manager: &Arc<TopicManager>) -> Result<Vec<ResourceContent>> {
    let topics = topic_manager.list_topics()?;
    let mut topic_list = Vec::new();

    for info in &topics {
        let mut total_messages: i64 = 0;
        for i in 0..info.num_partitions {
            let start = topic_manager.earliest_offset(&info.name, i).unwrap_or(0);
            let end = topic_manager.latest_offset(&info.name, i).unwrap_or(0);
            total_messages += end - start;
        }
        topic_list.push(serde_json::json!({
            "name": info.name,
            "partitions": info.num_partitions,
            "replication_factor": info.replication_factor,
            "total_messages": total_messages,
        }));
    }

    let result = serde_json::json!({
        "topics": topic_list,
        "total_topics": topic_list.len(),
    });

    Ok(vec![ResourceContent {
        uri: "streamline://topics".to_string(),
        mime_type: Some("application/json".to_string()),
        text: Some(
            serde_json::to_string_pretty(&result)
                .map_err(|e| StreamlineError::Internal(e.to_string()))?,
        ),
        blob: None,
    }])
}

fn read_topic_detail(
    topic_name: &str,
    topic_manager: &Arc<TopicManager>,
) -> Result<Vec<ResourceContent>> {
    let info = topic_manager.get_topic_metadata(topic_name)?;
    let mut partitions = Vec::new();

    for i in 0..info.num_partitions {
        let start = topic_manager.earliest_offset(topic_name, i).unwrap_or(0);
        let end = topic_manager.latest_offset(topic_name, i).unwrap_or(0);
        partitions.push(serde_json::json!({
            "id": i,
            "start_offset": start,
            "end_offset": end,
            "message_count": end - start,
        }));
    }

    // Read a sample of recent messages from partition 0
    let sample_messages = if info.num_partitions > 0 {
        let end = topic_manager.latest_offset(topic_name, 0).unwrap_or(0);
        let start = (end - 5).max(0);
        let records = topic_manager
            .read(topic_name, 0, start, 5)
            .unwrap_or_default();
        records
            .iter()
            .map(|r| {
                let s = String::from_utf8_lossy(&r.value);
                let value_preview = if s.len() > 200 {
                    format!("{}...", &s[..200])
                } else {
                    s.to_string()
                };
                serde_json::json!({
                    "offset": r.offset,
                    "timestamp": r.timestamp,
                    "key": r.key.as_ref().map(|k| String::from_utf8_lossy(k).to_string()),
                    "value_preview": value_preview,
                })
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    let result = serde_json::json!({
        "name": topic_name,
        "partitions": partitions,
        "partition_count": info.num_partitions,
        "replication_factor": info.replication_factor,
        "recent_messages_sample": sample_messages,
    });

    Ok(vec![ResourceContent {
        uri: format!("streamline://topics/{}", topic_name),
        mime_type: Some("application/json".to_string()),
        text: Some(
            serde_json::to_string_pretty(&result)
                .map_err(|e| StreamlineError::Internal(e.to_string()))?,
        ),
        blob: None,
    }])
}

fn read_partition_detail(
    topic_name: &str,
    partition: i32,
    topic_manager: &Arc<TopicManager>,
) -> Result<Vec<ResourceContent>> {
    let start = topic_manager.earliest_offset(topic_name, partition)?;
    let end = topic_manager.latest_offset(topic_name, partition)?;

    let result = serde_json::json!({
        "topic": topic_name,
        "partition": partition,
        "start_offset": start,
        "end_offset": end,
        "message_count": end - start,
    });

    Ok(vec![ResourceContent {
        uri: format!(
            "streamline://topics/{}/partitions/{}",
            topic_name, partition
        ),
        mime_type: Some("application/json".to_string()),
        text: Some(
            serde_json::to_string_pretty(&result)
                .map_err(|e| StreamlineError::Internal(e.to_string()))?,
        ),
        blob: None,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_resource_templates() {
        let templates = list_resource_templates();
        assert!(!templates.is_empty());
        assert!(templates.iter().any(|t| t.uri_template.contains("{topic}")));
    }

    #[test]
    fn test_read_server_info() {
        let contents = read_server_info().unwrap();
        assert_eq!(contents.len(), 1);
        assert_eq!(contents[0].uri, "streamline://server/info");
        let text = contents[0].text.as_ref().unwrap();
        assert!(text.contains("Streamline"));
    }
}
