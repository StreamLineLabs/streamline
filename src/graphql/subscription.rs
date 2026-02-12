//! GraphQL subscription resolvers
//!
//! Provides real-time streaming for messages, topic events, and metrics.

use async_graphql::{Context, Result, Subscription};
use futures_util::Stream;
use std::sync::Arc;
use std::time::Duration;

use crate::graphql::types::{
    record_to_message, ClusterEvent, ClusterEventType, Message, MetricsSnapshot, Partition,
    TopicEvent, TopicEventType, TopicMetrics,
};
use crate::storage::TopicManager;

/// GraphQL Subscription root
pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    /// Subscribe to messages on a topic in real time.
    ///
    /// Polls the topic for new messages and yields them as they arrive.
    /// If `from_offset` is provided, starts from that offset; otherwise starts
    /// from the latest offset (tail).
    async fn messages(
        &self,
        ctx: &Context<'_>,
        topic: String,
        #[graphql(desc = "Starting offset (default: latest)")] from_offset: Option<i64>,
        #[graphql(desc = "Partition to subscribe to (default: 0)")] partition: Option<i32>,
    ) -> Result<impl Stream<Item = Message>> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?.clone();
        let partition = partition.unwrap_or(0);

        // Determine starting offset: use provided value, or start at the latest offset
        let initial_offset = from_offset.unwrap_or_else(|| {
            topic_manager
                .get_topic_stats(&topic)
                .ok()
                .and_then(|s| {
                    s.partition_stats
                        .get(partition as usize)
                        .map(|ps| ps.latest_offset)
                })
                .unwrap_or(0)
        });

        let topic_name = topic.clone();
        let stream = async_stream::stream! {
            let mut offset = initial_offset;
            loop {
                // Poll for new messages
                if let Ok(records) = topic_manager.read(&topic_name, partition, offset, 100) {
                    for record in records {
                        let next_offset = record.offset + 1;
                        let msg = record_to_message(&topic_name, partition, record);
                        offset = next_offset;
                        yield msg;
                    }
                }

                // Small delay to avoid busy-waiting (100ms poll interval)
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };

        Ok(stream)
    }

    /// Subscribe to topic lifecycle events (creation and deletion).
    ///
    /// Polls the topic list periodically and emits events when topics are
    /// created or removed.
    async fn topic_events(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = TopicEvent>> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?.clone();

        let stream = async_stream::stream! {
            // Track known topics to detect changes
            let mut known_topics: std::collections::HashSet<String> = topic_manager
                .list_topics()
                .unwrap_or_default()
                .into_iter()
                .map(|t| t.name)
                .collect();

            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;

                if let Ok(current_topics_meta) = topic_manager.list_topics() {
                    let current_topics: std::collections::HashSet<String> = current_topics_meta
                        .into_iter()
                        .map(|t| t.name)
                        .collect();

                    // Detect newly created topics
                    for name in current_topics.difference(&known_topics) {
                        yield TopicEvent {
                            event_type: TopicEventType::Created,
                            topic_name: name.clone(),
                            timestamp: chrono::Utc::now().to_rfc3339(),
                        };
                    }

                    // Detect deleted topics
                    for name in known_topics.difference(&current_topics) {
                        yield TopicEvent {
                            event_type: TopicEventType::Deleted,
                            topic_name: name.clone(),
                            timestamp: chrono::Utc::now().to_rfc3339(),
                        };
                    }

                    known_topics = current_topics;
                }
            }
        };

        Ok(stream)
    }

    /// Subscribe to live metrics snapshots.
    ///
    /// Emits a MetricsSnapshot at the specified interval (default: 1 second).
    async fn metrics(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 1000, desc = "Update interval in milliseconds")] interval_ms: i32,
    ) -> Result<impl Stream<Item = MetricsSnapshot>> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?.clone();
        let interval = Duration::from_millis(interval_ms.max(100) as u64);

        let stream = async_stream::stream! {
            loop {
                let topics = topic_manager.list_topics().unwrap_or_default();
                let topic_count = topics.len() as i32;
                let total_messages: i64 = topics
                    .iter()
                    .filter_map(|t| {
                        topic_manager
                            .get_topic_stats(&t.name)
                            .ok()
                            .map(|s| s.total_messages as i64)
                    })
                    .sum();

                yield MetricsSnapshot {
                    total_messages,
                    topic_count,
                    timestamp: chrono::Utc::now().to_rfc3339(),
                };

                tokio::time::sleep(interval).await;
            }
        };

        Ok(stream)
    }

    /// Subscribe to real-time metrics for a specific topic.
    ///
    /// Emits a TopicMetrics snapshot at the specified interval (default: 1 second).
    async fn topic_metrics(
        &self,
        ctx: &Context<'_>,
        topic: String,
        #[graphql(default = 1000, desc = "Update interval in milliseconds")] interval_ms: i32,
    ) -> Result<impl Stream<Item = TopicMetrics>> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?.clone();
        let interval = Duration::from_millis(interval_ms.max(100) as u64);

        // Validate the topic exists before starting the stream
        topic_manager
            .get_topic_stats(&topic)
            .map_err(|e| async_graphql::Error::new(format!("Topic not found: {e}")))?;

        let topic_name = topic.clone();
        let stream = async_stream::stream! {
            loop {
                if let Ok(stats) = topic_manager.get_topic_stats(&topic_name) {
                    let partitions: Vec<Partition> = stats
                        .partition_stats
                        .iter()
                        .map(|ps| Partition {
                            id: ps.partition_id,
                            earliest_offset: ps.earliest_offset,
                            latest_offset: ps.latest_offset,
                            message_count: ps.message_count as i64,
                        })
                        .collect();

                    yield TopicMetrics {
                        topic: topic_name.clone(),
                        message_count: stats.total_messages as i64,
                        partition_count: stats.num_partitions,
                        partitions,
                        timestamp: chrono::Utc::now().to_rfc3339(),
                    };
                }

                tokio::time::sleep(interval).await;
            }
        };

        Ok(stream)
    }

    /// Stream messages from a topic in real-time with consumer group support.
    ///
    /// When `from_beginning` is true, starts from offset 0; otherwise starts
    /// from the latest offset. The optional `group` parameter is recorded for
    /// tracking but does not perform full consumer-group coordination.
    /// Uses bounded internal buffering to handle backpressure — slow consumers
    /// will skip intermediate messages rather than blocking producers.
    async fn message_stream(
        &self,
        ctx: &Context<'_>,
        topic: String,
        #[graphql(default = false, desc = "Start from the beginning of the topic")]
        from_beginning: bool,
        #[graphql(desc = "Consumer group name (for tracking)")] group: Option<String>,
    ) -> Result<impl Stream<Item = Message>> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?.clone();

        // Validate topic exists
        let stats = topic_manager
            .get_topic_stats(&topic)
            .map_err(|e| async_graphql::Error::new(format!("Topic not found: {e}")))?;

        let num_partitions = stats.num_partitions;

        // Determine starting offsets for each partition
        let mut start_offsets: Vec<i64> = Vec::with_capacity(num_partitions as usize);
        for p in 0..num_partitions {
            let offset = if from_beginning {
                0
            } else {
                stats
                    .partition_stats
                    .get(p as usize)
                    .map(|ps| ps.latest_offset)
                    .unwrap_or(0)
            };
            start_offsets.push(offset);
        }

        if let Some(ref g) = group {
            tracing::debug!(group = %g, topic = %topic, "message_stream subscription started");
        }

        let topic_name = topic.clone();
        // Bounded channel for backpressure: slow consumers drop old messages
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(256);

        // Spawn a producer task that reads from all partitions
        let _producer = tokio::spawn(async move {
            let mut offsets = start_offsets;
            loop {
                for partition in 0..num_partitions {
                    let p = partition;
                    if let Ok(records) =
                        topic_manager.read(&topic_name, p, offsets[p as usize], 100)
                    {
                        for record in records {
                            let next_offset = record.offset + 1;
                            let msg = record_to_message(&topic_name, p, record);
                            offsets[p as usize] = next_offset;
                            // Non-blocking send: if channel is full, skip (backpressure)
                            if tx.try_send(msg).is_err() {
                                // Consumer is slow; yield to let it catch up
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });

        let stream = async_stream::stream! {
            while let Some(msg) = rx.recv().await {
                yield msg;
            }
        };

        Ok(stream)
    }

    /// Stream topic metadata changes for a specific topic.
    ///
    /// Emits events when partitions are added or the topic's message count
    /// changes significantly (batch of new messages).
    async fn topic_events_detail(
        &self,
        ctx: &Context<'_>,
        topic: String,
    ) -> Result<impl Stream<Item = TopicEvent>> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?.clone();

        // Validate topic exists and capture initial state
        let initial_stats = topic_manager
            .get_topic_stats(&topic)
            .map_err(|e| async_graphql::Error::new(format!("Topic not found: {e}")))?;

        let topic_name = topic.clone();
        let stream = async_stream::stream! {
            let mut last_partition_count = initial_stats.num_partitions;
            let mut last_message_count = initial_stats.total_messages;

            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;

                if let Ok(stats) = topic_manager.get_topic_stats(&topic_name) {
                    // Detect partition count changes
                    if stats.num_partitions != last_partition_count {
                        yield TopicEvent {
                            event_type: TopicEventType::Created, // reuse for partition changes
                            topic_name: topic_name.clone(),
                            timestamp: chrono::Utc::now().to_rfc3339(),
                        };
                        last_partition_count = stats.num_partitions;
                    }

                    // Detect significant message count changes (new data produced)
                    if stats.total_messages > last_message_count {
                        yield TopicEvent {
                            event_type: TopicEventType::Created, // signals activity
                            topic_name: topic_name.clone(),
                            timestamp: chrono::Utc::now().to_rfc3339(),
                        };
                        last_message_count = stats.total_messages;
                    }
                } else {
                    // Topic was deleted
                    yield TopicEvent {
                        event_type: TopicEventType::Deleted,
                        topic_name: topic_name.clone(),
                        timestamp: chrono::Utc::now().to_rfc3339(),
                    };
                    break;
                }
            }
        };

        Ok(stream)
    }

    /// Stream cluster state changes.
    ///
    /// Emits events when topics are created/deleted, partitions change, or
    /// significant activity occurs. Polls the cluster state every second.
    async fn cluster_events(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = ClusterEvent>> {
        let topic_manager = ctx.data::<Arc<TopicManager>>()?.clone();

        let stream = async_stream::stream! {
            let mut known_topics: std::collections::HashMap<String, (i32, u64)> = topic_manager
                .list_topics()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|t| {
                    topic_manager
                        .get_topic_stats(&t.name)
                        .ok()
                        .map(|s| (t.name, (s.num_partitions, s.total_messages)))
                })
                .collect();

            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;

                if let Ok(current_topics_meta) = topic_manager.list_topics() {
                    let mut current: std::collections::HashMap<String, (i32, u64)> =
                        std::collections::HashMap::new();

                    for meta in &current_topics_meta {
                        let stats = topic_manager
                            .get_topic_stats(&meta.name)
                            .ok()
                            .map(|s| (s.num_partitions, s.total_messages))
                            .unwrap_or((meta.num_partitions, 0));
                        current.insert(meta.name.clone(), stats);
                    }

                    // Detect new topics
                    for (name, _) in &current {
                        if !known_topics.contains_key(name) {
                            yield ClusterEvent {
                                event_type: ClusterEventType::TopicCreated,
                                description: format!("Topic '{}' was created", name),
                                resource: Some(name.clone()),
                                timestamp: chrono::Utc::now().to_rfc3339(),
                            };
                        }
                    }

                    // Detect deleted topics
                    for (name, _) in &known_topics {
                        if !current.contains_key(name) {
                            yield ClusterEvent {
                                event_type: ClusterEventType::TopicDeleted,
                                description: format!("Topic '{}' was deleted", name),
                                resource: Some(name.clone()),
                                timestamp: chrono::Utc::now().to_rfc3339(),
                            };
                        }
                    }

                    // Detect partition changes and activity on existing topics
                    for (name, &(new_parts, new_msgs)) in &current {
                        if let Some(&(old_parts, old_msgs)) = known_topics.get(name) {
                            if new_parts != old_parts {
                                yield ClusterEvent {
                                    event_type: ClusterEventType::PartitionsAdded,
                                    description: format!(
                                        "Topic '{}' partitions changed from {} to {}",
                                        name, old_parts, new_parts
                                    ),
                                    resource: Some(name.clone()),
                                    timestamp: chrono::Utc::now().to_rfc3339(),
                                };
                            }
                            if new_msgs > old_msgs + 100 {
                                yield ClusterEvent {
                                    event_type: ClusterEventType::TopicActivity,
                                    description: format!(
                                        "Topic '{}' received {} new messages",
                                        name,
                                        new_msgs - old_msgs
                                    ),
                                    resource: Some(name.clone()),
                                    timestamp: chrono::Utc::now().to_rfc3339(),
                                };
                            }
                        }
                    }

                    known_topics = current;
                }
            }
        };

        Ok(stream)
    }
}
