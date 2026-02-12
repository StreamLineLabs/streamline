//! GraphQL API module for Streamline
//!
//! Provides a GraphQL interface for querying topics, producing messages,
//! and subscribing to real-time message streams.
//!
//! # Architecture
//!
//! The GraphQL layer is mounted on the existing HTTP server (port 9094) at
//! `/graphql`, with a playground at `/graphql` (GET) and WebSocket subscriptions
//! at `/graphql/ws`.
//!
//! # Feature Gate
//!
//! This module requires the `graphql` feature flag:
//!
//! ```bash
//! cargo build --features graphql
//! ```
//!
//! # Example Queries
//!
//! ```graphql
//! # List all topics
//! query {
//!   topics {
//!     name
//!     partitions
//!     messageCount
//!   }
//! }
//!
//! # Time-windowed topic statistics
//! query {
//!   topicStats(name: "events", window: "5m") {
//!     totalMessages
//!     messagesInWindow
//!     messageRate
//!   }
//! }
//!
//! # Search messages by content
//! query {
//!   searchMessages(topic: "events", query: "click", limit: 50) {
//!     matches { offset key value }
//!     totalMatches
//!     scanned
//!   }
//! }
//!
//! # Produce a message
//! mutation {
//!   produce(topic: "events", value: "{\"action\": \"click\"}") {
//!     offset
//!     timestamp
//!   }
//! }
//!
//! # Produce with schema validation
//! mutation {
//!   produceWithSchema(topic: "events", value: "{\"name\": \"Alice\"}", schema: "{\"required\": [\"name\"]}") {
//!     offset
//!     schemaValidated
//!   }
//! }
//!
//! # Subscribe to messages (WebSocket)
//! subscription {
//!   messages(topic: "events") {
//!     offset
//!     key
//!     value
//!     timestamp
//!   }
//! }
//!
//! # Stream messages with backpressure handling
//! subscription {
//!   messageStream(topic: "events", fromBeginning: true, group: "my-group") {
//!     offset
//!     key
//!     value
//!   }
//! }
//!
//! # Stream cluster state changes
//! subscription {
//!   clusterEvents {
//!     eventType
//!     description
//!     resource
//!   }
//! }
//! ```

pub mod mutation;
pub mod query;
pub mod subscription;
pub mod types;

use async_graphql::Schema;
use std::sync::Arc;
use std::time::Instant;

use crate::consumer::GroupCoordinator;
use crate::storage::TopicManager;

use self::mutation::MutationRoot;
use self::query::QueryRoot;
use self::subscription::SubscriptionRoot;

/// The full GraphQL schema type for Streamline
pub type StreamlineSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

/// Build the GraphQL schema with required shared state.
///
/// The schema is injected with:
/// - `Arc<TopicManager>` for storage operations
/// - `Instant` for server uptime calculation
/// - Optionally `Arc<GroupCoordinator>` for consumer group queries
pub fn build_schema(
    topic_manager: Arc<TopicManager>,
    start_time: Instant,
    group_coordinator: Option<Arc<GroupCoordinator>>,
) -> StreamlineSchema {
    let mut builder = Schema::build(QueryRoot, MutationRoot, SubscriptionRoot)
        .data(topic_manager)
        .data(start_time);

    if let Some(coordinator) = group_coordinator {
        builder = builder.data(coordinator);
    }

    builder.finish()
}
