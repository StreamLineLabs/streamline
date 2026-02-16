# GraphQL Streaming API — Design Document

> **Status:** Experimental
> **ADR:** [ADR-025](/docs/architecture/decisions/adr-025-graphql-streaming.md)
> **Feature flag:** `graphql`
> **Crate dependencies:** `async-graphql`, `async-graphql-axum`

## Overview

This document describes the design of an experimental GraphQL API for Streamline, enabling developers to query topics, produce messages, and subscribe to real-time message streams through a single GraphQL endpoint.

The GraphQL layer is mounted on the existing HTTP server (port 9094) at `/graphql`, with a GraphQL Playground available at `/graphql/playground` in development builds.

## GraphQL Schema

```graphql
# ─── Types ───────────────────────────────────────────────────────────

type Topic {
  name: String!
  partitions: Int!
  replicationFactor: Int!
  messageCount: Int!
  retentionMs: Int
  createdAt: String!
}

type Message {
  topic: String!
  partition: Int!
  offset: Int!
  key: String
  value: String!
  timestamp: String!
  headers: [Header!]!
}

type Header {
  key: String!
  value: String!
}

type ProduceResult {
  topic: String!
  partition: Int!
  offset: Int!
  timestamp: String!
}

type ClusterInfo {
  nodeId: Int!
  version: String!
  uptime: Int!
  topicCount: Int!
}

# ─── Inputs ──────────────────────────────────────────────────────────

input ProduceInput {
  key: String
  value: String!
  headers: [HeaderInput!]
  partition: Int
}

input HeaderInput {
  key: String!
  value: String!
}

input TopicConfig {
  retentionMs: Int
  maxMessageBytes: Int
}

# ─── Query ───────────────────────────────────────────────────────────

type Query {
  "List all topics"
  topics: [Topic!]!

  "Get a single topic by name"
  topic(name: String!): Topic

  "Read messages from a topic with optional filtering"
  messages(
    topic: String!
    partition: Int
    offset: Int
    limit: Int = 100
  ): [Message!]!

  "Get cluster information"
  cluster: ClusterInfo!
}

# ─── Mutation ────────────────────────────────────────────────────────

type Mutation {
  "Create a new topic"
  createTopic(
    name: String!
    partitions: Int = 1
    replicationFactor: Int = 1
    config: TopicConfig
  ): Topic!

  "Delete a topic"
  deleteTopic(name: String!): Boolean!

  "Produce a single message to a topic"
  produce(
    topic: String!
    message: ProduceInput!
  ): ProduceResult!

  "Produce a batch of messages to a topic"
  produceBatch(
    topic: String!
    messages: [ProduceInput!]!
  ): [ProduceResult!]!
}

# ─── Subscription ────────────────────────────────────────────────────

type Subscription {
  "Subscribe to messages on a topic in real time"
  messages(
    topic: String!
    fromBeginning: Boolean = false
    partition: Int
  ): Message!
}
```

## Implementation Notes

### Crate Integration

Add dependencies gated behind the `graphql` feature flag in `Cargo.toml`:

```toml
[features]
graphql = ["dep:async-graphql", "dep:async-graphql-axum"]

[dependencies]
async-graphql = { version = "7", optional = true }
async-graphql-axum = { version = "7", optional = true }
```

### Axum Router Setup

The GraphQL endpoint is mounted alongside existing HTTP routes:

```rust
use async_graphql::{Schema, EmptySubscription};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{Router, routing::get, Extension};

// Build schema from resolvers
let schema = Schema::build(QueryRoot, MutationRoot, SubscriptionRoot)
    .data(app_state.clone())
    .finish();

// Mount at /graphql
let graphql_routes = Router::new()
    .route("/graphql", get(graphql_playground).post(graphql_handler))
    .route("/graphql/ws", get(GraphQLSubscription::new(schema.clone())))
    .layer(Extension(schema));

// Merge with existing HTTP router on port 9094
let app = existing_router.merge(graphql_routes);
```

### Subscription Implementation

Subscriptions use `async-graphql`'s `Stream`-based model backed by Streamline's internal consumer:

```rust
use async_graphql::*;
use futures_util::stream::Stream;

struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    async fn messages(
        &self,
        ctx: &Context<'_>,
        topic: String,
        #[graphql(default = false)] from_beginning: bool,
        partition: Option<i32>,
    ) -> impl Stream<Item = Message> {
        let state = ctx.data::<AppState>().unwrap();
        let consumer = state.create_internal_consumer(&topic, from_beginning, partition).await;

        async_stream::stream! {
            while let Some(record) = consumer.next().await {
                yield Message::from(record);
            }
        }
    }
}
```

### Authentication

When the `auth` feature is enabled, the GraphQL endpoint respects the same bearer token authentication as the REST API. Tokens are extracted from the `Authorization` header or the `connectionParams` in WebSocket init messages.

## Example Queries and Subscriptions

### List all topics

```graphql
query {
  topics {
    name
    partitions
    messageCount
  }
}
```

**Response:**
```json
{
  "data": {
    "topics": [
      { "name": "orders", "partitions": 3, "messageCount": 15420 },
      { "name": "events", "partitions": 1, "messageCount": 8301 }
    ]
  }
}
```

### Produce a message

```graphql
mutation {
  produce(
    topic: "orders"
    message: {
      key: "order-123"
      value: "{\"item\": \"widget\", \"qty\": 5}"
      headers: [{ key: "source", value: "graphql-api" }]
    }
  ) {
    partition
    offset
    timestamp
  }
}
```

**Response:**
```json
{
  "data": {
    "produce": {
      "partition": 1,
      "offset": 15421,
      "timestamp": "2025-01-15T10:30:00Z"
    }
  }
}
```

### Subscribe to messages (WebSocket)

```graphql
subscription {
  messages(topic: "orders", fromBeginning: false) {
    offset
    key
    value
    timestamp
  }
}
```

Each message is pushed to the client as it arrives:

```json
{
  "data": {
    "messages": {
      "offset": 15422,
      "key": "order-124",
      "value": "{\"item\": \"gadget\", \"qty\": 2}",
      "timestamp": "2025-01-15T10:30:05Z"
    }
  }
}
```

### Read historical messages with pagination

```graphql
query {
  messages(topic: "orders", partition: 0, offset: 100, limit: 10) {
    offset
    key
    value
    headers { key value }
  }
}
```

## Performance Considerations

| Aspect | Impact | Mitigation |
|---|---|---|
| JSON serialization overhead | ~2-5x slower than binary Kafka protocol | Use `simd-json` feature; consider binary values as base64 |
| WebSocket framing | Small per-message overhead (~6-14 bytes) | Negligible for typical message sizes |
| GraphQL parsing | Per-request schema validation cost | Enable query caching with `async-graphql`'s persisted queries |
| Backpressure | WebSocket clients may not drain fast enough | Implement configurable buffer size; drop or pause on overflow |
| Connection limits | Each subscription holds a consumer | Pool internal consumers; enforce max concurrent subscriptions |
| Batching | Individual message delivery per subscription event | Support `produceBatch` mutation; consider DataLoader for queries |

### Benchmark Targets

For the experiment to be considered successful, the GraphQL API should achieve:

- **Produce latency:** < 2x overhead vs REST API (`POST /topics/{name}/messages`)
- **Subscription throughput:** > 10,000 messages/sec per WebSocket connection
- **Query latency:** < 5ms p99 for topic listing

## Migration Path

### From REST API

The existing REST endpoints on port 9094 remain unchanged. GraphQL runs alongside them:

| REST Endpoint | GraphQL Equivalent |
|---|---|
| `GET /health` | `query { cluster { version uptime } }` |
| `GET /topics` | `query { topics { name partitions } }` |
| `POST /topics` | `mutation { createTopic(...) { name } }` |
| `POST /topics/{name}/messages` | `mutation { produce(...) { offset } }` |
| — (no equivalent) | `subscription { messages(...) { value } }` |

### From Kafka Protocol

GraphQL is **not** intended to replace the Kafka protocol for production workloads. The migration path is additive:

1. **Phase 1 (current):** GraphQL for prototyping, dashboards, and admin
2. **Phase 2:** Evaluate adoption; if demand exists, add consumer group support
3. **Phase 3:** Consider GraphQL federation for multi-cluster queries

## Integration with Existing HTTP API

The GraphQL endpoint is added to the existing axum router on port 9094:

```
Port 9094
├── /health          (existing REST)
├── /metrics         (existing Prometheus)
├── /api/v1/...      (existing REST API)
├── /graphql         (new — queries & mutations via POST)
├── /graphql/ws      (new — subscriptions via WebSocket)
└── /graphql/playground  (new — interactive explorer, dev only)
```

No new ports are opened. The `graphql` feature flag controls whether these routes are registered. When disabled, the binary size and attack surface remain unchanged.

## Open Questions

1. **Binary message values** — Should large binary payloads be base64-encoded or served via a separate download endpoint?
2. **Schema versioning** — How do we evolve the GraphQL schema without breaking existing clients?
3. **Rate limiting** — Should GraphQL have its own rate limits separate from REST?
4. **Observability** — How do we trace GraphQL operations in the existing metrics pipeline?
