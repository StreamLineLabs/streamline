//! GraphQL mutation resolvers
//!
//! Provides write operations: create/delete topics and produce messages.

use async_graphql::{Context, Object, Result};
use bytes::Bytes;
use std::sync::Arc;

use crate::graphql::types::{ProduceInput, ProduceResult, ProduceWithSchemaResult, Topic, TopicConfig};
use crate::storage::TopicManager;

/// GraphQL Mutation root
pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Create a new topic
    async fn create_topic(
        &self,
        ctx: &Context<'_>,
        name: String,
        #[graphql(default = 1, desc = "Number of partitions")] partitions: i32,
        #[graphql(default = 1, desc = "Replication factor")] replication_factor: i32,
        #[graphql(desc = "Optional topic configuration")] config: Option<TopicConfig>,
    ) -> Result<Topic> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?;

        let num_partitions = partitions.max(1);
        topic_manager
            .create_topic(&name, num_partitions)
            .map_err(|e| async_graphql::Error::new(format!("Failed to create topic: {e}")))?;

        let message_count = topic_manager
            .get_topic_stats(&name)
            .map(|s| s.total_messages as i64)
            .unwrap_or(0);

        Ok(Topic {
            name,
            partitions: num_partitions,
            replication_factor,
            message_count,
            retention_ms: config.as_ref().and_then(|c| c.retention_ms),
            created_at: chrono::Utc::now().to_rfc3339(),
        })
    }

    /// Delete a topic
    async fn delete_topic(&self, ctx: &Context<'_>, name: String) -> Result<bool> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?;

        topic_manager
            .delete_topic(&name)
            .map_err(|e| async_graphql::Error::new(format!("Failed to delete topic: {e}")))?;

        Ok(true)
    }

    /// Produce a single message to a topic
    async fn produce(
        &self,
        ctx: &Context<'_>,
        topic: String,
        #[graphql(desc = "Optional message key")] key: Option<String>,
        #[graphql(desc = "Message value")] value: String,
    ) -> Result<ProduceResult> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?;

        let partition = 0;
        let key_bytes = key.map(Bytes::from);
        let value_bytes = Bytes::from(value);
        let timestamp = chrono::Utc::now();

        let offset = topic_manager
            .append(&topic, partition, key_bytes, value_bytes)
            .map_err(|e| async_graphql::Error::new(format!("Failed to produce message: {e}")))?;

        Ok(ProduceResult {
            topic,
            partition,
            offset,
            timestamp: timestamp.to_rfc3339(),
        })
    }

    /// Produce a message using the full ProduceInput type
    async fn produce_message(
        &self,
        ctx: &Context<'_>,
        topic: String,
        message: ProduceInput,
    ) -> Result<ProduceResult> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?;

        let partition = message.partition.unwrap_or(0);
        let key_bytes = message.key.map(Bytes::from);
        let value_bytes = Bytes::from(message.value);
        let timestamp = chrono::Utc::now();

        // If headers are provided, use append_with_headers
        if let Some(headers) = message.headers {
            let record_headers: Vec<crate::storage::record::Header> = headers
                .into_iter()
                .map(|h| crate::storage::record::Header {
                    key: h.key,
                    value: Bytes::from(h.value),
                })
                .collect();

            let offset = topic_manager
                .append_with_headers(&topic, partition, key_bytes, value_bytes, record_headers)
                .map_err(|e| {
                    async_graphql::Error::new(format!("Failed to produce message: {e}"))
                })?;

            Ok(ProduceResult {
                topic,
                partition,
                offset,
                timestamp: timestamp.to_rfc3339(),
            })
        } else {
            let offset = topic_manager
                .append(&topic, partition, key_bytes, value_bytes)
                .map_err(|e| {
                    async_graphql::Error::new(format!("Failed to produce message: {e}"))
                })?;

            Ok(ProduceResult {
                topic,
                partition,
                offset,
                timestamp: timestamp.to_rfc3339(),
            })
        }
    }

    /// Produce a message with optional JSON schema validation.
    ///
    /// When a `schema` is provided, the `value` is validated against it before
    /// being written. The schema should be a JSON Schema string (draft 2020-12
    /// compatible). If no schema is provided, the value is written as-is after
    /// verifying it is valid JSON.
    async fn produce_with_schema(
        &self,
        ctx: &Context<'_>,
        topic: String,
        value: String,
        #[graphql(desc = "JSON Schema to validate against (optional)")] schema: Option<String>,
    ) -> Result<ProduceWithSchemaResult> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?;

        // Validate that value is valid JSON
        let parsed_value: serde_json::Value = serde_json::from_str(&value)
            .map_err(|e| async_graphql::Error::new(format!("Invalid JSON value: {e}")))?;

        // If a schema is provided, validate value against it
        let schema_validated = if let Some(ref schema_str) = schema {
            let schema_value: serde_json::Value =
                serde_json::from_str(schema_str).map_err(|e| {
                    async_graphql::Error::new(format!("Invalid JSON schema: {e}"))
                })?;

            // Perform basic structural validation: check required fields from schema
            if let Some(required) = schema_value
                .get("required")
                .and_then(|r| r.as_array())
            {
                for field in required {
                    if let Some(field_name) = field.as_str() {
                        if parsed_value.get(field_name).is_none() {
                            return Err(async_graphql::Error::new(format!(
                                "Schema validation failed: missing required field '{}'",
                                field_name
                            )));
                        }
                    }
                }
            }

            // Validate property types if defined
            if let Some(properties) = schema_value
                .get("properties")
                .and_then(|p| p.as_object())
            {
                for (prop_name, prop_schema) in properties {
                    if let Some(val) = parsed_value.get(prop_name) {
                        if let Some(expected_type) =
                            prop_schema.get("type").and_then(|t| t.as_str())
                        {
                            let type_matches = match expected_type {
                                "string" => val.is_string(),
                                "number" | "integer" => val.is_number(),
                                "boolean" => val.is_boolean(),
                                "array" => val.is_array(),
                                "object" => val.is_object(),
                                "null" => val.is_null(),
                                _ => true,
                            };
                            if !type_matches {
                                return Err(async_graphql::Error::new(format!(
                                    "Schema validation failed: field '{}' expected type '{}'",
                                    prop_name, expected_type
                                )));
                            }
                        }
                    }
                }
            }

            true
        } else {
            false
        };

        let partition = 0;
        let value_bytes = Bytes::from(value);
        let timestamp = chrono::Utc::now();

        let offset = topic_manager
            .append(&topic, partition, None, value_bytes)
            .map_err(|e| async_graphql::Error::new(format!("Failed to produce message: {e}")))?;

        Ok(ProduceWithSchemaResult {
            topic,
            partition,
            offset,
            schema_validated,
            timestamp: timestamp.to_rfc3339(),
        })
    }
}
