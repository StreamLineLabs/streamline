//! MCP Tool implementations for Streamline
//!
//! Defines the tools that AI agents can invoke via the MCP protocol:
//! - produce: Send messages to topics
//! - consume: Read messages from topics
//! - list_topics: List available topics
//! - create_topic: Create a new topic
//! - describe_topic: Get topic metadata and stats
//! - query: Run SQL queries on stream data (if analytics enabled)

use crate::error::{Result, StreamlineError};
use crate::mcp::{ToolCallResult, ToolDefinition, ToolResultContent};
use crate::storage::TopicManager;
use bytes::Bytes;
use std::sync::Arc;

/// Return all available MCP tool definitions
pub fn get_tool_definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "streamline_produce".to_string(),
            description: "Produce a message to a Streamline topic. Use this to write data into a stream.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "The topic name to produce to"
                    },
                    "message": {
                        "type": "string",
                        "description": "The message content (string or JSON)"
                    },
                    "key": {
                        "type": "string",
                        "description": "Optional message key for partitioning"
                    },
                    "partition": {
                        "type": "integer",
                        "description": "Optional partition number (default: auto-assign)"
                    }
                },
                "required": ["topic", "message"]
            }),
        },
        ToolDefinition {
            name: "streamline_consume".to_string(),
            description: "Consume messages from a Streamline topic. Returns recent messages from the stream.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "The topic name to consume from"
                    },
                    "partition": {
                        "type": "integer",
                        "description": "Partition number (default: 0)"
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Starting offset (default: latest - count)"
                    },
                    "count": {
                        "type": "integer",
                        "description": "Maximum number of messages to return (default: 10, max: 100)"
                    }
                },
                "required": ["topic"]
            }),
        },
        ToolDefinition {
            name: "streamline_list_topics".to_string(),
            description: "List all available topics in Streamline with their partition counts and message counts.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {},
                "required": []
            }),
        },
        ToolDefinition {
            name: "streamline_create_topic".to_string(),
            description: "Create a new topic in Streamline.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Topic name (alphanumeric, dots, hyphens, underscores)"
                    },
                    "partitions": {
                        "type": "integer",
                        "description": "Number of partitions (default: 1)"
                    }
                },
                "required": ["name"]
            }),
        },
        ToolDefinition {
            name: "streamline_describe_topic".to_string(),
            description: "Get detailed information about a topic including partition offsets, message counts, and configuration.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "Topic name to describe"
                    }
                },
                "required": ["topic"]
            }),
        },
        ToolDefinition {
            name: "streamline_delete_topic".to_string(),
            description: "Delete a topic and all its data. This action is irreversible.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "Topic name to delete"
                    }
                },
                "required": ["topic"]
            }),
        },
        ToolDefinition {
            name: "streamline_search_messages".to_string(),
            description: "Search for messages in a topic matching a pattern. Searches message values using regex.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "Topic name to search"
                    },
                    "pattern": {
                        "type": "string",
                        "description": "Regex pattern to search for in message values"
                    },
                    "partition": {
                        "type": "integer",
                        "description": "Partition to search (default: 0)"
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum results to return (default: 10, max: 50)"
                    }
                },
                "required": ["topic", "pattern"]
            }),
        },
    ]
}

/// Execute an MCP tool by name with the given arguments
pub async fn execute_tool(
    name: &str,
    arguments: serde_json::Value,
    topic_manager: &Arc<TopicManager>,
) -> Result<ToolCallResult> {
    match name {
        "streamline_produce" => execute_produce(arguments, topic_manager).await,
        "streamline_consume" => execute_consume(arguments, topic_manager).await,
        "streamline_list_topics" => execute_list_topics(topic_manager).await,
        "streamline_create_topic" => execute_create_topic(arguments, topic_manager).await,
        "streamline_describe_topic" => execute_describe_topic(arguments, topic_manager).await,
        "streamline_delete_topic" => execute_delete_topic(arguments, topic_manager).await,
        "streamline_search_messages" => execute_search_messages(arguments, topic_manager).await,
        _ => Err(StreamlineError::protocol_msg(format!(
            "Unknown tool: {}",
            name
        ))),
    }
}

async fn execute_produce(
    args: serde_json::Value,
    topic_manager: &Arc<TopicManager>,
) -> Result<ToolCallResult> {
    let topic = args
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| StreamlineError::Validation("Missing 'topic' parameter".into()))?;

    let message = args
        .get("message")
        .and_then(|v| v.as_str())
        .ok_or_else(|| StreamlineError::Validation("Missing 'message' parameter".into()))?;

    let key = args.get("key").and_then(|v| v.as_str());
    let partition = args.get("partition").and_then(|v| v.as_i64()).unwrap_or(0) as i32;

    let key_bytes = key.map(|k| Bytes::from(k.to_string()));
    let value_bytes = Bytes::from(message.to_string());

    let offset = topic_manager.append(topic, partition, key_bytes, value_bytes)?;

    Ok(ToolCallResult {
        content: vec![ToolResultContent::Text {
            text: format!(
                "Message produced to topic '{}' partition {} at offset {}",
                topic, partition, offset
            ),
        }],
        is_error: false,
    })
}

async fn execute_consume(
    args: serde_json::Value,
    topic_manager: &Arc<TopicManager>,
) -> Result<ToolCallResult> {
    let topic = args
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| StreamlineError::Validation("Missing 'topic' parameter".into()))?;

    let partition = args.get("partition").and_then(|v| v.as_i64()).unwrap_or(0) as i32;

    let count = args
        .get("count")
        .and_then(|v| v.as_u64())
        .unwrap_or(10)
        .min(100) as usize;

    let topic_info = topic_manager.get_topic_metadata(topic)?;
    let partition_count = topic_info.num_partitions;
    if partition >= partition_count {
        return Err(StreamlineError::PartitionNotFound(
            topic.to_string(),
            partition,
        ));
    }

    let end_offset = topic_manager.latest_offset(topic, partition)?;
    let start_offset = args
        .get("offset")
        .and_then(|v| v.as_i64())
        .unwrap_or_else(|| (end_offset - count as i64).max(0));

    let records = topic_manager.read(topic, partition, start_offset, count)?;

    let mut messages = Vec::new();
    for record in &records {
        let key_str = record
            .key
            .as_ref()
            .map(|k| String::from_utf8_lossy(k).to_string());
        let value_str = String::from_utf8_lossy(&record.value).to_string();

        messages.push(serde_json::json!({
            "offset": record.offset,
            "timestamp": record.timestamp,
            "key": key_str,
            "value": value_str,
        }));
    }

    let result_text = serde_json::to_string_pretty(&serde_json::json!({
        "topic": topic,
        "partition": partition,
        "messages": messages,
        "count": records.len(),
        "start_offset": start_offset,
        "end_offset": end_offset,
    }))
    .map_err(|e| StreamlineError::Internal(e.to_string()))?;

    Ok(ToolCallResult {
        content: vec![ToolResultContent::Text { text: result_text }],
        is_error: false,
    })
}

async fn execute_list_topics(topic_manager: &Arc<TopicManager>) -> Result<ToolCallResult> {
    let topics = topic_manager.list_topics()?;
    let mut topic_list = Vec::new();

    for info in &topics {
        topic_list.push(serde_json::json!({
            "name": info.name,
            "partitions": info.num_partitions,
            "replication_factor": info.replication_factor,
        }));
    }

    let result_text = serde_json::to_string_pretty(&serde_json::json!({
        "topics": topic_list,
        "total": topic_list.len(),
    }))
    .map_err(|e| StreamlineError::Internal(e.to_string()))?;

    Ok(ToolCallResult {
        content: vec![ToolResultContent::Text { text: result_text }],
        is_error: false,
    })
}

async fn execute_create_topic(
    args: serde_json::Value,
    topic_manager: &Arc<TopicManager>,
) -> Result<ToolCallResult> {
    let name = args
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| StreamlineError::Validation("Missing 'name' parameter".into()))?;

    let partitions = args.get("partitions").and_then(|v| v.as_u64()).unwrap_or(1) as i32;

    topic_manager.create_topic(name, partitions)?;

    Ok(ToolCallResult {
        content: vec![ToolResultContent::Text {
            text: format!("Topic '{}' created with {} partition(s)", name, partitions),
        }],
        is_error: false,
    })
}

async fn execute_describe_topic(
    args: serde_json::Value,
    topic_manager: &Arc<TopicManager>,
) -> Result<ToolCallResult> {
    let topic = args
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| StreamlineError::Validation("Missing 'topic' parameter".into()))?;

    let info = topic_manager.get_topic_metadata(topic)?;
    let mut partition_details = Vec::new();

    for i in 0..info.num_partitions {
        let start = topic_manager.earliest_offset(topic, i).unwrap_or(0);
        let end = topic_manager.latest_offset(topic, i).unwrap_or(0);
        partition_details.push(serde_json::json!({
            "id": i,
            "start_offset": start,
            "end_offset": end,
            "message_count": end - start,
        }));
    }

    let result_text = serde_json::to_string_pretty(&serde_json::json!({
        "name": topic,
        "partitions": info.num_partitions,
        "replication_factor": info.replication_factor,
        "partition_details": partition_details,
    }))
    .map_err(|e| StreamlineError::Internal(e.to_string()))?;

    Ok(ToolCallResult {
        content: vec![ToolResultContent::Text { text: result_text }],
        is_error: false,
    })
}

async fn execute_delete_topic(
    args: serde_json::Value,
    topic_manager: &Arc<TopicManager>,
) -> Result<ToolCallResult> {
    let topic = args
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| StreamlineError::Validation("Missing 'topic' parameter".into()))?;

    topic_manager.delete_topic(topic)?;

    Ok(ToolCallResult {
        content: vec![ToolResultContent::Text {
            text: format!("Topic '{}' deleted successfully", topic),
        }],
        is_error: false,
    })
}

async fn execute_search_messages(
    args: serde_json::Value,
    topic_manager: &Arc<TopicManager>,
) -> Result<ToolCallResult> {
    let topic = args
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| StreamlineError::Validation("Missing 'topic' parameter".into()))?;

    let pattern = args
        .get("pattern")
        .and_then(|v| v.as_str())
        .ok_or_else(|| StreamlineError::Validation("Missing 'pattern' parameter".into()))?;

    let partition = args.get("partition").and_then(|v| v.as_i64()).unwrap_or(0) as i32;

    let max_results = args
        .get("max_results")
        .and_then(|v| v.as_u64())
        .unwrap_or(10)
        .min(50) as usize;

    let re = regex::Regex::new(pattern)
        .map_err(|e| StreamlineError::Validation(format!("Invalid regex: {}", e)))?;

    let end_offset = topic_manager.latest_offset(topic, partition)?;
    let scan_start = (end_offset - 1000).max(0);
    let records = topic_manager.read(topic, partition, scan_start, 1000)?;

    let mut matches = Vec::new();
    for record in &records {
        if matches.len() >= max_results {
            break;
        }
        let text = String::from_utf8_lossy(&record.value);
        if re.is_match(&text) {
            let key_str = record
                .key
                .as_ref()
                .map(|k| String::from_utf8_lossy(k).to_string());
            matches.push(serde_json::json!({
                "offset": record.offset,
                "timestamp": record.timestamp,
                "key": key_str,
                "value": text.to_string(),
            }));
        }
    }

    let result_text = serde_json::to_string_pretty(&serde_json::json!({
        "topic": topic,
        "partition": partition,
        "pattern": pattern,
        "matches": matches,
        "match_count": matches.len(),
    }))
    .map_err(|e| StreamlineError::Internal(e.to_string()))?;

    Ok(ToolCallResult {
        content: vec![ToolResultContent::Text { text: result_text }],
        is_error: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_tool_definitions() {
        let tools = get_tool_definitions();
        assert!(!tools.is_empty());

        let names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"streamline_produce"));
        assert!(names.contains(&"streamline_consume"));
        assert!(names.contains(&"streamline_list_topics"));
        assert!(names.contains(&"streamline_create_topic"));
        assert!(names.contains(&"streamline_describe_topic"));
        assert!(names.contains(&"streamline_delete_topic"));
        assert!(names.contains(&"streamline_search_messages"));
    }

    #[test]
    fn test_tool_schemas_are_valid_json() {
        let tools = get_tool_definitions();
        for tool in &tools {
            assert!(
                tool.input_schema.is_object(),
                "Tool {} schema must be an object",
                tool.name
            );
            assert_eq!(
                tool.input_schema["type"], "object",
                "Tool {} schema type must be 'object'",
                tool.name
            );
        }
    }
}
