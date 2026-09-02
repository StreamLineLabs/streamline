//! MCP Tool implementations for Streamline
//!
//! Defines the tools that AI agents can invoke via the MCP protocol:
//! - produce: Send messages to topics
//! - consume: Read messages from topics
//! - list_topics: List available topics
//! - create_topic: Create a new topic
//! - describe_topic: Get topic metadata and stats
//! - query: Run SQL queries on stream data (if analytics enabled)

use crate::mcp::backend::{McpBackend, McpError, McpResult};
use crate::mcp::{ToolCallResult, ToolDefinition, ToolResultContent};

/// Return all available MCP tool definitions
pub fn get_tool_definitions() -> Vec<ToolDefinition> {
    // `defs` is only extended by the moonshot tool registrations below.
    #[cfg_attr(
        not(any(feature = "agent-memory", feature = "semantic-topics")),
        allow(unused_mut)
    )]
    let mut defs = vec![
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
        ToolDefinition {
            name: "streamline_list_consumer_groups".to_string(),
            description: "List all consumer groups with their state, members, and lag information.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {},
                "required": []
            }),
        },
        ToolDefinition {
            name: "streamline_query".to_string(),
            description: "Execute a SQL/StreamQL query against Streamline's analytics engine. Returns tabular results.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL or StreamQL query to execute"
                    }
                },
                "required": ["sql"]
            }),
        },
        ToolDefinition {
            name: "streamline_get_metrics".to_string(),
            description: "Get current server metrics including throughput, latency, storage usage, and connection counts.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {},
                "required": []
            }),
        },
    ];

    #[cfg(feature = "agent-memory")]
    {
        defs.push(ToolDefinition {
            name: "streamline_remember".to_string(),
            description: "Persist a memory for an agent. kind = observation | fact | procedure.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "agent_id": {"type": "string", "description": "Agent owning the memory"},
                    "content":  {"type": "string", "description": "Memory text"},
                    "kind":     {"type": "string", "description": "observation | fact | procedure", "enum": ["observation", "fact", "procedure"]},
                    "skill":    {"type": "string", "description": "Required when kind=procedure"},
                    "importance": {"type": "number", "description": "0.0..1.0; >=0.3 mirrors facts to semantic", "default": 0.5},
                    "tags":     {"type": "array", "items": {"type": "string"}}
                },
                "required": ["agent_id", "content", "kind"]
            }),
        });
        defs.push(ToolDefinition {
            name: "streamline_recall".to_string(),
            description: "Search an agent's long-term memory for the top-k most relevant entries.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "agent_id": {"type": "string"},
                    "query":    {"type": "string"},
                    "k":        {"type": "integer", "default": 10, "minimum": 1, "maximum": 100},
                    "min_hits": {"type": "integer", "default": 1, "description": "Fall back to episodic if semantic returns fewer than this"}
                },
                "required": ["agent_id", "query"]
            }),
        });
    }

    #[cfg(feature = "semantic-topics")]
    {
        defs.push(ToolDefinition {
            name: "streamline_semantic_search".to_string(),
            description: "Search a semantic-enabled topic by meaning. Returns the top-k records most similar to the query text.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "topic": {"type": "string", "description": "Topic name (must have semantic.embed=on)"},
                    "query": {"type": "string", "description": "Natural-language search query"},
                    "k":     {"type": "integer", "default": 10, "minimum": 1, "maximum": 1000, "description": "Number of results to return"}
                },
                "required": ["topic", "query"]
            }),
        });
    }

    defs
}

/// Execute an MCP tool by name with the given arguments.
///
/// Returns `Err(McpError)` for protocol-level errors (unknown tool, missing params).
/// Returns `Ok(ToolCallResult { is_error: true })` for backend/business logic errors.
pub async fn execute_tool(
    name: &str,
    arguments: serde_json::Value,
    backend: &dyn McpBackend,
) -> McpResult<ToolCallResult> {
    match name {
        "streamline_produce" => execute_produce(arguments, backend).await,
        "streamline_consume" => execute_consume(arguments, backend).await,
        "streamline_list_topics" => execute_list_topics(backend).await,
        "streamline_create_topic" => execute_create_topic(arguments, backend).await,
        "streamline_describe_topic" => execute_describe_topic(arguments, backend).await,
        "streamline_delete_topic" => execute_delete_topic(arguments, backend).await,
        "streamline_search_messages" => execute_search_messages(arguments, backend).await,
        "streamline_list_consumer_groups" => execute_list_consumer_groups(backend).await,
        "streamline_query" => execute_query(arguments, backend).await,
        "streamline_get_metrics" => execute_get_metrics(backend).await,
        #[cfg(feature = "agent-memory")]
        "streamline_remember" => execute_remember(arguments).await,
        #[cfg(feature = "agent-memory")]
        "streamline_recall" => execute_recall(arguments).await,
        #[cfg(feature = "semantic-topics")]
        "streamline_semantic_search" => execute_semantic_search(arguments).await,
        _ => Err(McpError::method_not_found(format!("Unknown tool: {name}"))),
    }
}

#[cfg(feature = "agent-memory")]
async fn execute_remember(args: serde_json::Value) -> McpResult<ToolCallResult> {
    use crate::memory::{tier_router, MemoryWrite, WriteKind};

    let agent_id = args
        .get("agent_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::invalid_params("Missing required parameter 'agent_id'"))?
        .to_string();
    let content = args
        .get("content")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::invalid_params("Missing required parameter 'content'"))?
        .to_string();
    let kind_str = args
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::invalid_params("Missing required parameter 'kind'"))?;
    let importance = args
        .get("importance")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.5) as f32;
    let tags: Vec<String> = args
        .get("tags")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let kind = match kind_str {
        "observation" => WriteKind::Observation,
        "fact" => WriteKind::Fact,
        "procedure" => {
            let skill = args
                .get("skill")
                .and_then(|v| v.as_str())
                .ok_or_else(|| McpError::invalid_params("kind=procedure requires 'skill'"))?
                .to_string();
            WriteKind::Procedure { skill }
        }
        other => return Err(McpError::invalid_params(format!("Unknown kind: {other}"))),
    };
    let w = MemoryWrite {
        agent_id,
        kind,
        content,
        importance,
        tags,
    };
    match tier_router::remember(&w) {
        Ok(written) => {
            let value = serde_json::json!({
                "written": written.iter().map(|(t, o)| serde_json::json!({"topic": t, "offset": o})).collect::<Vec<_>>(),
                "count": written.len(),
            });
            Ok(success_result(json_text(&value)))
        }
        Err(e) => Ok(error_result(format!("remember failed: {e}"))),
    }
}

#[cfg(feature = "agent-memory")]
async fn execute_recall(args: serde_json::Value) -> McpResult<ToolCallResult> {
    use crate::memory::tier_router;

    let agent_id = args
        .get("agent_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::invalid_params("Missing required parameter 'agent_id'"))?;
    let query = args
        .get("query")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::invalid_params("Missing required parameter 'query'"))?;
    let k = args.get("k").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
    let min_hits = args.get("min_hits").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
    let hits = tier_router::recall(agent_id, query, k, min_hits);
    let value = serde_json::json!({
        "hits": hits.iter().map(|h| serde_json::json!({
            "tier": format!("{:?}", h.tier),
            "topic": h.topic,
            "offset": h.offset,
            "content": h.content,
            "score": h.score,
        })).collect::<Vec<_>>(),
        "count": hits.len(),
    });
    Ok(success_result(json_text(&value)))
}

fn success_result(text: impl Into<String>) -> ToolCallResult {
    ToolCallResult {
        content: vec![ToolResultContent::Text { text: text.into() }],
        is_error: false,
    }
}

fn error_result(msg: impl Into<String>) -> ToolCallResult {
    ToolCallResult {
        content: vec![ToolResultContent::Text { text: msg.into() }],
        is_error: true,
    }
}

fn json_text(value: &serde_json::Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

async fn execute_produce(
    args: serde_json::Value,
    backend: &dyn McpBackend,
) -> McpResult<ToolCallResult> {
    let topic = args
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::invalid_params("Missing required parameter 'topic'"))?;
    let message = args
        .get("message")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::invalid_params("Missing required parameter 'message'"))?;
    let key = args.get("key").and_then(|v| v.as_str());
    let partition = args
        .get("partition")
        .and_then(|v| v.as_i64())
        .map(|p| p as i32);
    match backend.produce(topic, key, message, partition).await {
        Ok(result) => Ok(success_result(json_text(
            &serde_json::json!({"status":"ok","topic":result.topic,"partition":result.partition,"offset":result.offset}),
        ))),
        Err(e) => Ok(error_result(format!("Failed to produce message: {e}"))),
    }
}

async fn execute_consume(
    args: serde_json::Value,
    backend: &dyn McpBackend,
) -> McpResult<ToolCallResult> {
    let topic = args
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::invalid_params("Missing required parameter 'topic'"))?;
    let partition = args.get("partition").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    let count = args
        .get("count")
        .and_then(|v| v.as_u64())
        .unwrap_or(10)
        .min(100) as u32;
    let offset = args.get("offset").and_then(|v| v.as_i64()).unwrap_or(-1);
    match backend.consume(topic, partition, offset, count).await {
        Ok(messages) => {
            let msg_json: Vec<serde_json::Value> = messages.iter().map(|m| serde_json::json!({"offset":m.offset,"timestamp":m.timestamp,"key":m.key,"value":m.value,"headers":m.headers})).collect();
            Ok(success_result(json_text(
                &serde_json::json!({"topic":topic,"partition":partition,"messages":msg_json,"count":messages.len()}),
            )))
        }
        Err(e) => Ok(error_result(format!("Failed to consume messages: {e}"))),
    }
}

async fn execute_list_topics(backend: &dyn McpBackend) -> McpResult<ToolCallResult> {
    match backend.list_topics().await {
        Ok(topics) => {
            let list: Vec<serde_json::Value> = topics.iter().map(|t| serde_json::json!({"name":t.name,"partitions":t.partitions,"replication_factor":t.replication_factor,"retention_ms":t.retention_ms,"total_messages":t.total_messages})).collect();
            Ok(success_result(json_text(
                &serde_json::json!({"topics":list,"total":list.len()}),
            )))
        }
        Err(e) => Ok(error_result(format!("Failed to list topics: {e}"))),
    }
}

async fn execute_create_topic(
    args: serde_json::Value,
    backend: &dyn McpBackend,
) -> McpResult<ToolCallResult> {
    let name = args
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::invalid_params("Missing required parameter 'name'"))?;
    let partitions = args.get("partitions").and_then(|v| v.as_u64()).unwrap_or(1) as u32;
    if partitions == 0 {
        return Err(McpError::invalid_params("Partition count must be >= 1"));
    }
    let retention_ms = args.get("retention_ms").and_then(|v| v.as_i64());
    match backend.create_topic(name, partitions, retention_ms).await {
        Ok(()) => Ok(success_result(json_text(
            &serde_json::json!({"status":"ok","topic":name,"partitions":partitions,"retention_ms":retention_ms}),
        ))),
        Err(e) => Ok(error_result(format!("Failed to create topic: {e}"))),
    }
}

async fn execute_describe_topic(
    args: serde_json::Value,
    backend: &dyn McpBackend,
) -> McpResult<ToolCallResult> {
    let topic = args
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::invalid_params("Missing required parameter 'topic'"))?;
    match backend.describe_topic(topic).await {
        Ok(detail) => Ok(success_result(json_text(
            &serde_json::json!({"name":detail.name,"partitions":detail.partitions,"replication_factor":detail.replication_factor,"config":detail.config,"created_at":detail.created_at,"total_messages":detail.total_messages}),
        ))),
        Err(e) => Ok(error_result(format!("Failed to describe topic: {e}"))),
    }
}

async fn execute_delete_topic(
    args: serde_json::Value,
    backend: &dyn McpBackend,
) -> McpResult<ToolCallResult> {
    let topic = args
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::invalid_params("Missing required parameter 'topic'"))?;
    match backend.delete_topic(topic).await {
        Ok(()) => Ok(success_result(json_text(
            &serde_json::json!({"status":"ok","topic":topic,"message":format!("Topic '{}' deleted successfully", topic)}),
        ))),
        Err(e) => Ok(error_result(format!("Failed to delete topic: {e}"))),
    }
}

async fn execute_search_messages(
    args: serde_json::Value,
    backend: &dyn McpBackend,
) -> McpResult<ToolCallResult> {
    let topic = args
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::invalid_params("Missing required parameter 'topic'"))?;
    let pattern = args
        .get("pattern")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::invalid_params("Missing required parameter 'pattern'"))?;
    let partition = args.get("partition").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    let max_results = args
        .get("max_results")
        .and_then(|v| v.as_u64())
        .unwrap_or(10)
        .min(50) as usize;
    match backend
        .search_messages(topic, partition, pattern, max_results)
        .await
    {
        Ok(matches) => {
            let list: Vec<serde_json::Value> = matches.iter().map(|m| serde_json::json!({"offset":m.offset,"timestamp":m.timestamp,"key":m.key,"value":m.value})).collect();
            Ok(success_result(json_text(
                &serde_json::json!({"topic":topic,"partition":partition,"pattern":pattern,"matches":list,"match_count":matches.len()}),
            )))
        }
        Err(e) => Ok(error_result(format!("Search failed: {e}"))),
    }
}

async fn execute_list_consumer_groups(backend: &dyn McpBackend) -> McpResult<ToolCallResult> {
    match backend.list_consumer_groups().await {
        Ok(groups) => {
            let list: Vec<serde_json::Value> = groups.iter().map(|g| serde_json::json!({"group_id":g.group_id,"state":g.state,"members":g.members.len(),"total_lag":g.total_lag})).collect();
            Ok(success_result(json_text(
                &serde_json::json!({"consumer_groups":list,"total":list.len()}),
            )))
        }
        Err(e) => Ok(error_result(format!("Failed to list consumer groups: {e}"))),
    }
}

async fn execute_query(
    args: serde_json::Value,
    backend: &dyn McpBackend,
) -> McpResult<ToolCallResult> {
    let sql = args
        .get("sql")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpError::invalid_params("Missing required parameter 'sql'"))?;
    if sql.trim().is_empty() {
        return Err(McpError::invalid_params("SQL query must not be empty"));
    }
    match backend.query(sql).await {
        Ok(result) => Ok(success_result(json_text(
            &serde_json::json!({"columns":result.columns,"rows":result.rows,"row_count":result.row_count,"execution_time_ms":result.execution_time_ms}),
        ))),
        Err(e) => Ok(error_result(format!("Query failed: {e}"))),
    }
}

async fn execute_get_metrics(backend: &dyn McpBackend) -> McpResult<ToolCallResult> {
    match backend.get_metrics().await {
        Ok(m) => Ok(success_result(json_text(
            &serde_json::json!({"messages_in_per_sec":m.messages_in_per_sec,"messages_out_per_sec":m.messages_out_per_sec,"bytes_in_per_sec":m.bytes_in_per_sec,"bytes_out_per_sec":m.bytes_out_per_sec,"total_topics":m.total_topics,"total_partitions":m.total_partitions,"total_messages":m.total_messages,"storage_bytes":m.storage_bytes,"active_connections":m.active_connections,"uptime_seconds":m.uptime_seconds}),
        ))),
        Err(e) => Ok(error_result(format!("Failed to get metrics: {e}"))),
    }
}

#[cfg(feature = "semantic-topics")]
async fn execute_semantic_search(args: serde_json::Value) -> McpResult<ToolCallResult> {
    let topic = args["topic"]
        .as_str()
        .ok_or_else(|| McpError::invalid_params("topic is required"))?;
    let query = args["query"]
        .as_str()
        .ok_or_else(|| McpError::invalid_params("query is required"))?;
    let k = args["k"].as_u64().unwrap_or(10) as usize;

    if query.trim().is_empty() {
        return Ok(error_result("query must not be empty"));
    }
    if k == 0 || k > 1000 {
        return Ok(error_result("k must be in [1, 1000]"));
    }

    let resp = crate::server::search_api::handle_search(
        topic,
        crate::server::search_api::SearchRequest {
            query: query.to_string(),
            k,
            filter: None,
        },
    );
    let result = serde_json::json!({
        "topic": topic,
        "hits": resp.hits.iter().map(|h| serde_json::json!({
            "partition": h.partition,
            "offset": h.offset,
            "score": h.score,
        })).collect::<Vec<_>>(),
        "took_ms": resp.took_ms,
        "total_hits": resp.hits.len(),
    });
    Ok(success_result(json_text(&result)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::backend::{MessageInfo, MockMcpBackend};

    #[test]
    fn test_get_tool_definitions() {
        let tools = get_tool_definitions();
        let mut expected = 10; // base tools
        if cfg!(feature = "agent-memory") {
            expected += 2;
        } // remember + recall
        if cfg!(feature = "semantic-topics") {
            expected += 1;
        } // semantic_search
        assert_eq!(tools.len(), expected);

        let names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"streamline_produce"));
        assert!(names.contains(&"streamline_consume"));
        assert!(names.contains(&"streamline_list_topics"));
        assert!(names.contains(&"streamline_create_topic"));
        assert!(names.contains(&"streamline_describe_topic"));
        assert!(names.contains(&"streamline_delete_topic"));
        assert!(names.contains(&"streamline_search_messages"));
        assert!(names.contains(&"streamline_list_consumer_groups"));
        assert!(names.contains(&"streamline_query"));
        assert!(names.contains(&"streamline_get_metrics"));
        #[cfg(feature = "agent-memory")]
        {
            assert!(names.contains(&"streamline_remember"));
            assert!(names.contains(&"streamline_recall"));
        }
        #[cfg(feature = "semantic-topics")]
        {
            assert!(names.contains(&"streamline_semantic_search"));
        }
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

    fn make_mock_with_data() -> MockMcpBackend {
        let mock = MockMcpBackend::new();
        mock.add_topic("events", 3);
        mock.add_topic("logs", 1);
        mock.add_messages(
            "events",
            (0..5)
                .map(|i| MessageInfo {
                    offset: i,
                    timestamp: 1700000000000 + i * 1000,
                    key: Some(format!("key-{i}")),
                    value: format!("event-{i}"),
                    headers: vec![],
                })
                .collect(),
        );
        mock
    }

    #[tokio::test]
    async fn test_execute_unknown_tool() {
        let mock = MockMcpBackend::new();
        let err = execute_tool("nonexistent", serde_json::json!({}), &mock)
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::mcp::backend::McpErrorCode::MethodNotFound);
    }

    #[tokio::test]
    async fn test_execute_list_topics() {
        let mock = make_mock_with_data();
        let result = execute_tool("streamline_list_topics", serde_json::json!({}), &mock)
            .await
            .unwrap();
        assert!(!result.is_error);
        let text = match &result.content[0] {
            ToolResultContent::Text { text } => text,
            _ => panic!("expected text"),
        };
        assert!(text.contains("events"));
        assert!(text.contains("logs"));
    }

    #[tokio::test]
    async fn test_execute_describe_topic() {
        let mock = make_mock_with_data();
        let result = execute_tool(
            "streamline_describe_topic",
            serde_json::json!({"topic": "events"}),
            &mock,
        )
        .await
        .unwrap();
        assert!(!result.is_error);
    }

    #[tokio::test]
    async fn test_execute_describe_topic_not_found() {
        let mock = MockMcpBackend::new();
        let result = execute_tool(
            "streamline_describe_topic",
            serde_json::json!({"topic": "nope"}),
            &mock,
        )
        .await
        .unwrap();
        assert!(result.is_error);
    }

    #[tokio::test]
    async fn test_execute_produce() {
        let mock = make_mock_with_data();
        let result = execute_tool(
            "streamline_produce",
            serde_json::json!({"topic": "events", "message": "hello", "key": "k1"}),
            &mock,
        )
        .await
        .unwrap();
        assert!(!result.is_error);
        assert_eq!(mock.produced_messages.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_execute_produce_missing_params() {
        let mock = MockMcpBackend::new();
        let err = execute_tool(
            "streamline_produce",
            serde_json::json!({"topic": "events"}),
            &mock,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, crate::mcp::backend::McpErrorCode::InvalidParams);
    }

    #[tokio::test]
    async fn test_execute_consume() {
        let mock = make_mock_with_data();
        let result = execute_tool(
            "streamline_consume",
            serde_json::json!({"topic": "events", "offset": 1, "count": 3}),
            &mock,
        )
        .await
        .unwrap();
        assert!(!result.is_error);
        let text = match &result.content[0] {
            ToolResultContent::Text { text } => text,
            _ => panic!("expected text"),
        };
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["count"], 3);
    }

    #[tokio::test]
    async fn test_execute_consume_missing_topic() {
        let mock = MockMcpBackend::new();
        let err = execute_tool("streamline_consume", serde_json::json!({}), &mock)
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::mcp::backend::McpErrorCode::InvalidParams);
    }

    #[tokio::test]
    async fn test_execute_create_topic() {
        let mock = MockMcpBackend::new();
        let result = execute_tool(
            "streamline_create_topic",
            serde_json::json!({"name": "new-topic", "partitions": 4, "retention_ms": 3600000}),
            &mock,
        )
        .await
        .unwrap();
        assert!(!result.is_error);
        assert_eq!(mock.created_topics.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_execute_create_topic_zero_partitions() {
        let mock = MockMcpBackend::new();
        let err = execute_tool(
            "streamline_create_topic",
            serde_json::json!({"name": "t", "partitions": 0}),
            &mock,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, crate::mcp::backend::McpErrorCode::InvalidParams);
    }

    #[tokio::test]
    async fn test_execute_create_topic_duplicate() {
        let mock = make_mock_with_data();
        let result = execute_tool(
            "streamline_create_topic",
            serde_json::json!({"name": "events"}),
            &mock,
        )
        .await
        .unwrap();
        assert!(result.is_error);
    }

    #[tokio::test]
    async fn test_execute_delete_topic() {
        let mock = make_mock_with_data();
        let result = execute_tool(
            "streamline_delete_topic",
            serde_json::json!({"topic": "events"}),
            &mock,
        )
        .await
        .unwrap();
        assert!(!result.is_error);
        assert_eq!(mock.deleted_topics.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_execute_delete_topic_not_found() {
        let mock = MockMcpBackend::new();
        let result = execute_tool(
            "streamline_delete_topic",
            serde_json::json!({"topic": "nope"}),
            &mock,
        )
        .await
        .unwrap();
        assert!(result.is_error);
    }

    #[tokio::test]
    async fn test_execute_search_messages() {
        let mock = make_mock_with_data();
        let result = execute_tool(
            "streamline_search_messages",
            serde_json::json!({"topic": "events", "pattern": "event-[12]"}),
            &mock,
        )
        .await
        .unwrap();
        assert!(!result.is_error);
        let text = match &result.content[0] {
            ToolResultContent::Text { text } => text,
            _ => panic!("expected text"),
        };
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["match_count"], 2);
    }

    #[tokio::test]
    async fn test_execute_list_consumer_groups() {
        let mock = MockMcpBackend::new();
        let result = execute_tool(
            "streamline_list_consumer_groups",
            serde_json::json!({}),
            &mock,
        )
        .await
        .unwrap();
        assert!(!result.is_error);
    }

    #[tokio::test]
    async fn test_execute_query() {
        let mock = MockMcpBackend::new();
        let result = execute_tool(
            "streamline_query",
            serde_json::json!({"sql": "SELECT * FROM events"}),
            &mock,
        )
        .await
        .unwrap();
        assert!(!result.is_error);
    }

    #[tokio::test]
    async fn test_execute_query_empty_sql() {
        let mock = MockMcpBackend::new();
        let err = execute_tool("streamline_query", serde_json::json!({"sql": "  "}), &mock)
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::mcp::backend::McpErrorCode::InvalidParams);
    }

    #[tokio::test]
    async fn test_execute_query_missing_sql() {
        let mock = MockMcpBackend::new();
        let err = execute_tool("streamline_query", serde_json::json!({}), &mock)
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::mcp::backend::McpErrorCode::InvalidParams);
    }

    #[tokio::test]
    async fn test_execute_get_metrics() {
        let mock = MockMcpBackend::new();
        let result = execute_tool("streamline_get_metrics", serde_json::json!({}), &mock)
            .await
            .unwrap();
        assert!(!result.is_error);
        let text = match &result.content[0] {
            ToolResultContent::Text { text } => text,
            _ => panic!("expected text"),
        };
        assert!(text.contains("total_topics"));
    }

    #[tokio::test]
    async fn test_execute_tool_backend_error_returns_is_error() {
        let mock = make_mock_with_data();
        mock.set_fail_method(Some("list_topics"));
        let result = execute_tool("streamline_list_topics", serde_json::json!({}), &mock)
            .await
            .unwrap();
        assert!(result.is_error);
    }

    #[tokio::test]
    async fn test_success_and_error_result_helpers() {
        let ok = success_result("ok");
        assert!(!ok.is_error);
        let err = error_result("bad");
        assert!(err.is_error);
    }

    #[cfg(feature = "agent-memory")]
    #[tokio::test]
    // `test_lock` exists to serialize tests against the process-global registry,
    // so it must be held across the awaits in the body.
    #[allow(clippy::await_holding_lock)]
    async fn test_remember_then_recall_via_mcp() {
        let _guard = crate::ai::semantic_topics::registry::test_lock();
        crate::memory::tier_router::reset_for_tests();
        let backend = make_mock_with_data();
        let r = execute_tool(
            "streamline_remember",
            serde_json::json!({
                "agent_id": "mcp-bob",
                "content": "kafka rebalance protocol uses cooperative sticky",
                "kind": "fact",
                "importance": 0.9
            }),
            &backend,
        )
        .await
        .unwrap();
        assert!(!r.is_error, "remember should succeed: {r:?}");

        let recall = execute_tool(
            "streamline_recall",
            serde_json::json!({"agent_id": "mcp-bob", "query": "rebalance protocol", "k": 3}),
            &backend,
        )
        .await
        .unwrap();
        assert!(!recall.is_error);
        let text = match &recall.content[0] {
            ToolResultContent::Text { text } => text.clone(),
            _ => panic!("expected Text content"),
        };
        assert!(
            text.contains("rebalance"),
            "expected hit content; got {text}"
        );
    }

    #[cfg(feature = "agent-memory")]
    #[tokio::test]
    async fn test_remember_missing_agent_id_is_invalid_params() {
        let backend = make_mock_with_data();
        let r = execute_tool(
            "streamline_remember",
            serde_json::json!({"content": "x", "kind": "fact"}),
            &backend,
        )
        .await;
        assert!(r.is_err());
    }

    #[cfg(feature = "agent-memory")]
    #[tokio::test]
    async fn test_remember_procedure_requires_skill() {
        let backend = make_mock_with_data();
        let r = execute_tool(
            "streamline_remember",
            serde_json::json!({"agent_id": "a", "content": "x", "kind": "procedure"}),
            &backend,
        )
        .await;
        assert!(r.is_err());
    }
}
