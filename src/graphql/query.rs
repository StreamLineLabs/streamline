//! GraphQL query resolvers
//!
//! Provides read-only access to topics, messages, consumer groups, and cluster info.

use async_graphql::{Context, Object, Result};
use std::sync::Arc;
use std::time::Instant;

use crate::consumer::GroupCoordinator;
use crate::graphql::types::{
    record_to_message, ClusterInfo, ConsumerGroup, GroupState, Message, Partition, SearchResult,
    Topic, TopicStatsWindow,
};
use crate::storage::TopicManager;

/// GraphQL Query root
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// List all topics with metadata
    async fn topics(&self, ctx: &Context<'_>) -> Result<Vec<Topic>> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?;
        let topics = topic_manager
            .list_topics()
            .map_err(|e| async_graphql::Error::new(format!("Failed to list topics: {e}")))?;

        let mut result = Vec::with_capacity(topics.len());
        for metadata in topics {
            let message_count = topic_manager
                .get_topic_stats(&metadata.name)
                .map(|s| s.total_messages as i64)
                .unwrap_or(0);

            result.push(Topic {
                name: metadata.name,
                partitions: metadata.num_partitions,
                replication_factor: metadata.replication_factor as i32,
                message_count,
                retention_ms: None,
                created_at: chrono::Utc::now().to_rfc3339(),
            });
        }

        Ok(result)
    }

    /// Get a single topic by name
    async fn topic(&self, ctx: &Context<'_>, name: String) -> Result<Option<Topic>> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?;

        match topic_manager.get_topic_stats(&name) {
            Ok(stats) => Ok(Some(Topic {
                name: stats.name,
                partitions: stats.num_partitions,
                replication_factor: stats.replication_factor as i32,
                message_count: stats.total_messages as i64,
                retention_ms: None,
                created_at: chrono::Utc::now().to_rfc3339(),
            })),
            Err(_) => Ok(None),
        }
    }

    /// Fetch messages from a topic with optional offset and limit
    async fn messages(
        &self,
        ctx: &Context<'_>,
        topic: String,
        #[graphql(desc = "Partition to read from (default: 0)")] partition: Option<i32>,
        #[graphql(desc = "Starting offset (default: 0)")] offset: Option<i64>,
        #[graphql(default = 100, desc = "Maximum number of messages to return")] limit: i32,
    ) -> Result<Vec<Message>> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?;

        let partition = partition.unwrap_or(0);
        let offset = offset.unwrap_or(0);
        let limit = limit.min(1000) as usize;

        let records = topic_manager
            .read(&topic, partition, offset, limit)
            .map_err(|e| async_graphql::Error::new(format!("Failed to read messages: {e}")))?;

        Ok(records
            .into_iter()
            .map(|r| record_to_message(&topic, partition, r))
            .collect())
    }

    /// List consumer groups
    async fn consumer_groups(&self, ctx: &Context<'_>) -> Result<Vec<ConsumerGroup>> {
        // If GroupCoordinator is available, use it to list groups
        if let Ok(coordinator) = ctx.data::<Arc<GroupCoordinator>>() {
            let group_ids = coordinator
                .list_groups()
                .map_err(|e| async_graphql::Error::new(format!("Failed to list groups: {e}")))?;

            let mut groups = Vec::with_capacity(group_ids.len());
            for group_id in group_ids {
                if let Ok(Some(group)) = coordinator.get_group(&group_id) {
                    let state = match group.state {
                        crate::consumer::group::GroupState::Empty => GroupState::Empty,
                        crate::consumer::group::GroupState::PreparingRebalance => {
                            GroupState::PreparingRebalance
                        }
                        crate::consumer::group::GroupState::CompletingRebalance => {
                            GroupState::CompletingRebalance
                        }
                        crate::consumer::group::GroupState::Stable => GroupState::Stable,
                        crate::consumer::group::GroupState::Dead => GroupState::Dead,
                    };
                    groups.push(ConsumerGroup {
                        group_id: group.group_id.clone(),
                        state,
                        protocol_type: group.protocol_type.clone(),
                        member_count: group.members.len() as i32,
                    });
                }
            }
            return Ok(groups);
        }

        // No coordinator available, return empty list
        Ok(vec![])
    }

    /// Get cluster health and information
    async fn cluster_info(&self, ctx: &Context<'_>) -> Result<ClusterInfo> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?;
        let start_time = ctx.data::<Instant>()?;

        let topic_count = topic_manager
            .list_topics()
            .map(|t| t.len() as i32)
            .unwrap_or(0);

        Ok(ClusterInfo {
            node_id: 0,
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime: start_time.elapsed().as_secs() as i64,
            topic_count,
        })
    }

    /// Get time-windowed statistics for a topic.
    ///
    /// The `window` parameter accepts duration strings like "1m", "5m", "1h", "24h".
    /// Returns partition stats and an approximate message rate for the window.
    async fn topic_stats(
        &self,
        ctx: &Context<'_>,
        name: String,
        #[graphql(default_with = r#""5m".to_string()"#, desc = "Time window (e.g. '1m', '5m', '1h')")]
        window: String,
    ) -> Result<TopicStatsWindow> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?;

        let stats = topic_manager
            .get_topic_stats(&name)
            .map_err(|e| async_graphql::Error::new(format!("Topic not found: {e}")))?;

        // Parse window duration
        let window_ms = parse_window_duration(&window)
            .map_err(|e| async_graphql::Error::new(format!("Invalid window: {e}")))?;
        let window_start = chrono::Utc::now().timestamp_millis() - window_ms;

        // Count messages within the window by scanning partitions from timestamp
        let mut messages_in_window: i64 = 0;
        let partitions: Vec<Partition> = stats
            .partition_stats
            .iter()
            .map(|ps| {
                // Find the offset at the start of the window
                let window_offset = topic_manager
                    .find_offset_by_timestamp(&name, ps.partition_id, window_start)
                    .ok()
                    .flatten()
                    .unwrap_or(ps.latest_offset);

                let msgs_in_part = (ps.latest_offset - window_offset).max(0);
                messages_in_window += msgs_in_part;

                Partition {
                    id: ps.partition_id,
                    earliest_offset: ps.earliest_offset,
                    latest_offset: ps.latest_offset,
                    message_count: ps.message_count as i64,
                }
            })
            .collect();

        let window_secs = (window_ms as f64) / 1000.0;
        let message_rate = if window_secs > 0.0 {
            messages_in_window as f64 / window_secs
        } else {
            0.0
        };

        Ok(TopicStatsWindow {
            name: stats.name,
            partition_count: stats.num_partitions,
            total_messages: stats.total_messages as i64,
            messages_in_window,
            message_rate,
            partitions,
            window,
            timestamp: chrono::Utc::now().to_rfc3339(),
        })
    }

    /// Search messages in a topic by content substring matching.
    ///
    /// Scans messages across all partitions and returns those whose value
    /// contains the query string. Limited to `limit` results (default 50).
    async fn search_messages(
        &self,
        ctx: &Context<'_>,
        topic: String,
        query: String,
        #[graphql(default = 50, desc = "Maximum number of results")] limit: i32,
    ) -> Result<SearchResult> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?;

        let stats = topic_manager
            .get_topic_stats(&topic)
            .map_err(|e| async_graphql::Error::new(format!("Topic not found: {e}")))?;

        let limit = limit.min(1000).max(1) as usize;
        let query_lower = query.to_lowercase();
        let mut matches = Vec::new();
        let mut scanned: i64 = 0;

        'outer: for ps in &stats.partition_stats {
            let mut offset = ps.earliest_offset;
            while offset < ps.latest_offset {
                let batch_size = 200;
                let records = topic_manager
                    .read(&topic, ps.partition_id, offset, batch_size)
                    .unwrap_or_default();

                if records.is_empty() {
                    break;
                }

                for record in records {
                    offset = record.offset + 1;
                    scanned += 1;
                    let value_str = String::from_utf8_lossy(&record.value);
                    if value_str.to_lowercase().contains(&query_lower) {
                        matches.push(record_to_message(&topic, ps.partition_id, record));
                        if matches.len() >= limit {
                            break 'outer;
                        }
                    }
                }
            }
        }

        let total_matches = matches.len() as i32;
        Ok(SearchResult {
            matches,
            total_matches,
            scanned,
        })
    }
}

/// Parse a window duration string (e.g. "1m", "5m", "1h", "24h") into milliseconds.
fn parse_window_duration(window: &str) -> std::result::Result<i64, String> {
    let trimmed = window.trim();
    if trimmed.is_empty() {
        return Err("empty window string".into());
    }

    let (num_str, unit) = trimmed.split_at(trimmed.len() - 1);
    let num: i64 = num_str
        .parse()
        .map_err(|_| format!("invalid number in window: '{}'", num_str))?;

    match unit {
        "s" => Ok(num * 1_000),
        "m" => Ok(num * 60_000),
        "h" => Ok(num * 3_600_000),
        "d" => Ok(num * 86_400_000),
        _ => Err(format!("unknown time unit '{}', use s/m/h/d", unit)),
    }
}
