//! MCP Resource implementations for Streamline

use crate::mcp::backend::{McpBackend, McpError, McpResult};
use crate::mcp::{ResourceContent, ResourceDefinition, ResourceTemplate};

/// List all available resources
pub async fn list_resources(backend: &dyn McpBackend) -> McpResult<Vec<ResourceDefinition>> {
    let mut resources = Vec::new();
    if let Ok(topics) = backend.list_topics().await {
        for topic in &topics {
            resources.push(ResourceDefinition {
                uri: format!("streamline://topics/{}", topic.name),
                name: format!("Topic: {}", topic.name),
                description: Some(format!(
                    "Topic '{}' ({} partitions, {} messages)",
                    topic.name, topic.partitions, topic.total_messages
                )),
                mime_type: Some("application/json".to_string()),
            });
        }
    }
    resources.push(ResourceDefinition {
        uri: "streamline://topics".to_string(),
        name: "All Topics".to_string(),
        description: Some("Overview of all topics".to_string()),
        mime_type: Some("application/json".to_string()),
    });
    resources.push(ResourceDefinition {
        uri: "streamline://server/info".to_string(),
        name: "Server Info".to_string(),
        description: Some("Streamline server information".to_string()),
        mime_type: Some("application/json".to_string()),
    });
    resources.push(ResourceDefinition {
        uri: "streamline://metrics".to_string(),
        name: "Metrics".to_string(),
        description: Some("Server metrics snapshot".to_string()),
        mime_type: Some("application/json".to_string()),
    });
    resources.push(ResourceDefinition {
        uri: "streamline://config".to_string(),
        name: "Configuration".to_string(),
        description: Some("Current server configuration (secrets redacted)".to_string()),
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
            description: Some("Get metadata for a specific topic".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceTemplate {
            uri_template: "streamline://topics/{topic}/partitions/{partition}".to_string(),
            name: "Partition Details".to_string(),
            description: Some("Get partition information".to_string()),
            mime_type: Some("application/json".to_string()),
        },
    ]
}

/// Read a specific resource by URI
pub async fn read_resource(uri: &str, backend: &dyn McpBackend) -> McpResult<Vec<ResourceContent>> {
    let path = uri
        .strip_prefix("streamline://")
        .ok_or_else(|| McpError::invalid_params(format!("Invalid resource URI: {uri}")))?;
    let parts: Vec<&str> = path.split('/').collect();
    match parts.as_slice() {
        ["server", "info"] => read_server_info(backend).await,
        ["topics"] => read_topics_overview(backend).await,
        ["topics", topic_name] => read_topic_detail(topic_name, backend).await,
        ["topics", topic_name, "partitions", partition] => {
            let p: i32 = partition
                .parse()
                .map_err(|_| McpError::invalid_params("Invalid partition number"))?;
            read_partition_detail(topic_name, p, backend).await
        }
        ["metrics"] => read_metrics(backend).await,
        ["config"] => read_config(backend).await,
        _ => Err(McpError::resource_not_found(uri)),
    }
}

async fn read_server_info(backend: &dyn McpBackend) -> McpResult<Vec<ResourceContent>> {
    let config = backend.get_server_config().await.ok();
    let info = serde_json::json!({"name":"Streamline","version":config.as_ref().map(|c| c.version.as_str()).unwrap_or(env!("CARGO_PKG_VERSION")),"description":"Kafka-compatible streaming platform","features":config.as_ref().map(|c| &c.features)});
    Ok(vec![ResourceContent {
        uri: "streamline://server/info".to_string(),
        mime_type: Some("application/json".to_string()),
        text: Some(json_pretty(&info)?),
        blob: None,
    }])
}

async fn read_topics_overview(backend: &dyn McpBackend) -> McpResult<Vec<ResourceContent>> {
    let topics = backend.list_topics().await?;
    let list: Vec<serde_json::Value> = topics.iter().map(|t| serde_json::json!({"name":t.name,"partitions":t.partitions,"replication_factor":t.replication_factor,"retention_ms":t.retention_ms,"total_messages":t.total_messages})).collect();
    let result = serde_json::json!({"topics":list,"total_topics":list.len()});
    Ok(vec![ResourceContent {
        uri: "streamline://topics".to_string(),
        mime_type: Some("application/json".to_string()),
        text: Some(json_pretty(&result)?),
        blob: None,
    }])
}

async fn read_topic_detail(
    topic_name: &str,
    backend: &dyn McpBackend,
) -> McpResult<Vec<ResourceContent>> {
    let detail = backend.describe_topic(topic_name).await?;
    let sample = if !detail.partitions.is_empty() {
        backend
            .consume(topic_name, 0, -1, 5)
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let sample_json: Vec<serde_json::Value> = sample.iter().map(|m| {
        let preview = if m.value.len() > 200 { format!("{}...", &m.value[..200]) } else { m.value.clone() };
        serde_json::json!({"offset":m.offset,"timestamp":m.timestamp,"key":m.key,"value_preview":preview})
    }).collect();
    let result = serde_json::json!({"name":detail.name,"partitions":detail.partitions,"partition_count":detail.partitions.len(),"replication_factor":detail.replication_factor,"config":detail.config,"total_messages":detail.total_messages,"recent_messages_sample":sample_json});
    Ok(vec![ResourceContent {
        uri: format!("streamline://topics/{topic_name}"),
        mime_type: Some("application/json".to_string()),
        text: Some(json_pretty(&result)?),
        blob: None,
    }])
}

async fn read_partition_detail(
    topic_name: &str,
    partition: i32,
    backend: &dyn McpBackend,
) -> McpResult<Vec<ResourceContent>> {
    let detail = backend.describe_topic(topic_name).await?;
    let part = detail
        .partitions
        .iter()
        .find(|p| p.id == partition)
        .ok_or_else(|| McpError::partition_not_found(topic_name, partition))?;
    let result = serde_json::json!({"topic":topic_name,"partition":part.id,"start_offset":part.start_offset,"end_offset":part.end_offset,"message_count":part.message_count,"high_watermark":part.high_watermark});
    Ok(vec![ResourceContent {
        uri: format!("streamline://topics/{topic_name}/partitions/{partition}"),
        mime_type: Some("application/json".to_string()),
        text: Some(json_pretty(&result)?),
        blob: None,
    }])
}

async fn read_metrics(backend: &dyn McpBackend) -> McpResult<Vec<ResourceContent>> {
    let metrics = backend.get_metrics().await?;
    let result = serde_json::to_value(&metrics).map_err(|e| McpError::internal(e.to_string()))?;
    Ok(vec![ResourceContent {
        uri: "streamline://metrics".to_string(),
        mime_type: Some("application/json".to_string()),
        text: Some(json_pretty(&result)?),
        blob: None,
    }])
}

async fn read_config(backend: &dyn McpBackend) -> McpResult<Vec<ResourceContent>> {
    let config = backend.get_server_config().await?;
    let result = serde_json::to_value(&config).map_err(|e| McpError::internal(e.to_string()))?;
    Ok(vec![ResourceContent {
        uri: "streamline://config".to_string(),
        mime_type: Some("application/json".to_string()),
        text: Some(json_pretty(&result)?),
        blob: None,
    }])
}

fn json_pretty(value: &serde_json::Value) -> McpResult<String> {
    serde_json::to_string_pretty(value).map_err(|e| McpError::internal(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::backend::{MessageInfo, MockMcpBackend};

    #[test]
    fn test_list_resource_templates() {
        let templates = list_resource_templates();
        assert!(!templates.is_empty());
        assert!(templates.iter().any(|t| t.uri_template.contains("{topic}")));
    }

    fn make_mock_with_data() -> MockMcpBackend {
        let mock = MockMcpBackend::new();
        mock.add_topic("events", 2);
        mock.add_topic("logs", 1);
        mock.add_messages(
            "events",
            vec![
                MessageInfo {
                    offset: 0,
                    timestamp: 1700000000000,
                    key: Some("k1".to_string()),
                    value: "event-data-1".to_string(),
                    headers: vec![],
                },
                MessageInfo {
                    offset: 1,
                    timestamp: 1700000001000,
                    key: None,
                    value: "event-data-2".to_string(),
                    headers: vec![],
                },
            ],
        );
        mock
    }

    #[tokio::test]
    async fn test_list_resources() {
        let mock = make_mock_with_data();
        let resources = list_resources(&mock).await.unwrap();
        assert!(resources.len() >= 6);
        let uris: Vec<&str> = resources.iter().map(|r| r.uri.as_str()).collect();
        assert!(uris.contains(&"streamline://topics"));
        assert!(uris.contains(&"streamline://topics/events"));
        assert!(uris.contains(&"streamline://topics/logs"));
        assert!(uris.contains(&"streamline://metrics"));
        assert!(uris.contains(&"streamline://config"));
        assert!(uris.contains(&"streamline://server/info"));
    }

    #[tokio::test]
    async fn test_read_server_info() {
        let mock = MockMcpBackend::new();
        let contents = read_resource("streamline://server/info", &mock)
            .await
            .unwrap();
        assert_eq!(contents.len(), 1);
        assert_eq!(contents[0].uri, "streamline://server/info");
        let text = contents[0].text.as_ref().unwrap();
        assert!(text.contains("Streamline"));
    }

    #[tokio::test]
    async fn test_read_topics_overview() {
        let mock = make_mock_with_data();
        let contents = read_resource("streamline://topics", &mock).await.unwrap();
        assert_eq!(contents.len(), 1);
        let text = contents[0].text.as_ref().unwrap();
        assert!(text.contains("events"));
        assert!(text.contains("logs"));
        assert!(text.contains("total_topics"));
    }

    #[tokio::test]
    async fn test_read_topic_detail() {
        let mock = make_mock_with_data();
        let contents = read_resource("streamline://topics/events", &mock)
            .await
            .unwrap();
        assert_eq!(contents.len(), 1);
        assert_eq!(contents[0].uri, "streamline://topics/events");
        let text = contents[0].text.as_ref().unwrap();
        assert!(text.contains("events"));
        assert!(text.contains("partition_count"));
    }

    #[tokio::test]
    async fn test_read_topic_detail_not_found() {
        let mock = MockMcpBackend::new();
        let err = read_resource("streamline://topics/nope", &mock)
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::mcp::backend::McpErrorCode::TopicNotFound);
    }

    #[tokio::test]
    async fn test_read_partition_detail() {
        let mock = make_mock_with_data();
        let contents = read_resource("streamline://topics/events/partitions/0", &mock)
            .await
            .unwrap();
        assert_eq!(contents.len(), 1);
        let text = contents[0].text.as_ref().unwrap();
        assert!(text.contains("message_count"));
    }

    #[tokio::test]
    async fn test_read_partition_not_found() {
        let mock = make_mock_with_data();
        let err = read_resource("streamline://topics/events/partitions/99", &mock)
            .await
            .unwrap_err();
        assert_eq!(
            err.code,
            crate::mcp::backend::McpErrorCode::PartitionNotFound
        );
    }

    #[tokio::test]
    async fn test_read_metrics() {
        let mock = MockMcpBackend::new();
        let contents = read_resource("streamline://metrics", &mock).await.unwrap();
        assert_eq!(contents.len(), 1);
        assert_eq!(contents[0].uri, "streamline://metrics");
        let text = contents[0].text.as_ref().unwrap();
        assert!(text.contains("total_topics"));
    }

    #[tokio::test]
    async fn test_read_config() {
        let mock = MockMcpBackend::new();
        let contents = read_resource("streamline://config", &mock).await.unwrap();
        assert_eq!(contents.len(), 1);
        assert_eq!(contents[0].uri, "streamline://config");
        let text = contents[0].text.as_ref().unwrap();
        assert!(text.contains("kafka_port"));
        assert!(text.contains("features"));
    }

    #[tokio::test]
    async fn test_read_invalid_uri() {
        let mock = MockMcpBackend::new();
        let err = read_resource("http://invalid", &mock).await.unwrap_err();
        assert_eq!(err.code, crate::mcp::backend::McpErrorCode::InvalidParams);
    }

    #[tokio::test]
    async fn test_read_unknown_path() {
        let mock = MockMcpBackend::new();
        let err = read_resource("streamline://unknown/path", &mock)
            .await
            .unwrap_err();
        assert_eq!(
            err.code,
            crate::mcp::backend::McpErrorCode::ResourceNotFound
        );
    }

    #[tokio::test]
    async fn test_read_invalid_partition_number() {
        let mock = make_mock_with_data();
        let err = read_resource("streamline://topics/events/partitions/abc", &mock)
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::mcp::backend::McpErrorCode::InvalidParams);
    }
}
