# Developer Experience Revolution

## Summary

Make Streamline as easy to use as a database by providing first-class SDKs for Python and Node.js, a GraphQL subscriptions API for real-time data, an enhanced interactive CLI, and a playground mode for instant experimentation. This focuses on developer adoption and viral growth through exceptional DX.

## Current State

- Kafka clients work (via protocol compatibility)
- CLI tool exists with comprehensive commands
- REST API exists for management
- WebSocket support for real-time streaming
- No native Python SDK
- No native Node.js SDK
- No GraphQL API
- No browser-based playground
- No `npx streamline` instant experience

## Requirements

### SDK & API Features

| Requirement | Priority | Description |
|-------------|----------|-------------|
| Python SDK | P0 | `pip install streamline` with async support |
| Node.js SDK | P0 | `npm install streamline` with TypeScript |
| GraphQL API | P0 | Subscriptions for real-time data |
| Enhanced CLI | P1 | REPL mode, autocomplete, smart suggestions |
| Playground Mode | P1 | Demo mode with sample data and tutorials |
| Quick Start | P1 | `npx streamline` instant experience |
| SDK Generators | P2 | Generate SDKs from OpenAPI spec |
| Client Libraries | P2 | Go, Java, Rust native clients |

### Python SDK

Create `sdks/python/streamline/`:

```python
# streamline/__init__.py
"""
Streamline Python SDK

A developer-friendly SDK for the Streamline streaming platform.

Quick Start:
    from streamline import Streamline

    # Connect to Streamline
    client = Streamline("localhost:9092")

    # Produce messages
    await client.produce("my-topic", {"event": "hello"})

    # Consume messages
    async for message in client.consume("my-topic"):
        print(message.value)
"""

from .client import Streamline
from .producer import Producer
from .consumer import Consumer
from .admin import Admin
from .types import Message, Record, TopicConfig

__version__ = "0.1.0"
__all__ = [
    "Streamline",
    "Producer",
    "Consumer",
    "Admin",
    "Message",
    "Record",
    "TopicConfig",
]
```

```python
# streamline/client.py
"""Main Streamline client."""

from typing import Optional, AsyncIterator, Dict, Any, List
import asyncio
from .producer import Producer
from .consumer import Consumer
from .admin import Admin

class Streamline:
    """
    Main client for interacting with Streamline.

    Example:
        async with Streamline("localhost:9092") as client:
            await client.produce("events", {"user": "alice", "action": "login"})

            async for msg in client.consume("events", group="my-app"):
                print(f"Got: {msg.value}")
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        *,
        client_id: Optional[str] = None,
        api_key: Optional[str] = None,
        tls: bool = False,
        tls_ca: Optional[str] = None,
    ):
        """
        Create a new Streamline client.

        Args:
            bootstrap_servers: Comma-separated list of broker addresses
            client_id: Optional client identifier
            api_key: Optional API key for authentication
            tls: Enable TLS encryption
            tls_ca: Path to CA certificate file
        """
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id or self._generate_client_id()
        self._producer: Optional[Producer] = None
        self._consumers: Dict[str, Consumer] = {}
        self._admin: Optional[Admin] = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def connect(self) -> None:
        """Connect to Streamline cluster."""
        ...

    async def close(self) -> None:
        """Close all connections."""
        ...

    # ========== Produce API ==========

    async def produce(
        self,
        topic: str,
        value: Any,
        *,
        key: Optional[str] = None,
        partition: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        timestamp: Optional[int] = None,
    ) -> int:
        """
        Produce a message to a topic.

        Args:
            topic: Topic name
            value: Message value (will be JSON serialized if dict/list)
            key: Optional message key
            partition: Optional partition number
            headers: Optional message headers
            timestamp: Optional timestamp in milliseconds

        Returns:
            Offset of the produced message
        """
        ...

    async def produce_batch(
        self,
        topic: str,
        messages: List[Dict[str, Any]],
    ) -> List[int]:
        """Produce multiple messages in a batch."""
        ...

    # ========== Consume API ==========

    async def consume(
        self,
        topic: str,
        *,
        group: Optional[str] = None,
        from_beginning: bool = False,
        from_offset: Optional[int] = None,
        from_timestamp: Optional[int] = None,
    ) -> AsyncIterator[Message]:
        """
        Consume messages from a topic.

        Args:
            topic: Topic name
            group: Consumer group ID (enables offset tracking)
            from_beginning: Start from earliest offset
            from_offset: Start from specific offset
            from_timestamp: Start from specific timestamp

        Yields:
            Message objects
        """
        ...

    async def consume_batch(
        self,
        topic: str,
        max_messages: int = 100,
        timeout_ms: int = 5000,
        **kwargs,
    ) -> List[Message]:
        """Consume a batch of messages."""
        ...

    # ========== Admin API ==========

    async def create_topic(
        self,
        name: str,
        partitions: int = 1,
        **config,
    ) -> None:
        """Create a new topic."""
        ...

    async def delete_topic(self, name: str) -> None:
        """Delete a topic."""
        ...

    async def list_topics(self) -> List[str]:
        """List all topics."""
        ...

    async def topic_info(self, name: str) -> Dict[str, Any]:
        """Get topic information."""
        ...

    # ========== Query API ==========

    async def query(
        self,
        sql: str,
        *,
        timeout_ms: int = 30000,
    ) -> List[Dict[str, Any]]:
        """
        Execute a SQL query on stream data.

        Args:
            sql: SQL query string
            timeout_ms: Query timeout

        Returns:
            List of result rows as dictionaries
        """
        ...

    # ========== AI API (if enabled) ==========

    async def semantic_search(
        self,
        topic: str,
        query: str,
        *,
        top_k: int = 10,
        min_score: float = 0.7,
    ) -> List[Message]:
        """Search messages by semantic meaning."""
        ...
```

```python
# streamline/consumer.py
"""Async consumer with automatic offset management."""

from typing import AsyncIterator, Optional, Callable, Awaitable
from .types import Message

class Consumer:
    """
    High-level consumer with automatic offset management.

    Example:
        consumer = Consumer(client, "events", group="my-app")

        async for msg in consumer:
            process(msg)
            # Offset auto-committed periodically

        # Or with manual commit:
        async for msg in consumer.messages():
            process(msg)
            await consumer.commit()
    """

    def __init__(
        self,
        client: "Streamline",
        topic: str,
        *,
        group: Optional[str] = None,
        auto_commit: bool = True,
        auto_commit_interval_ms: int = 5000,
    ):
        ...

    async def __aiter__(self) -> AsyncIterator[Message]:
        """Iterate over messages with auto-commit."""
        ...

    async def messages(self) -> AsyncIterator[Message]:
        """Iterate over messages without auto-commit."""
        ...

    async def commit(self) -> None:
        """Commit current offsets."""
        ...

    async def seek(self, partition: int, offset: int) -> None:
        """Seek to specific offset."""
        ...

    async def pause(self) -> None:
        """Pause consumption."""
        ...

    async def resume(self) -> None:
        """Resume consumption."""
        ...

    def on_rebalance(
        self,
        callback: Callable[[List[int], List[int]], Awaitable[None]],
    ) -> None:
        """Register rebalance callback."""
        ...
```

### Node.js SDK

Create `sdks/nodejs/src/`:

```typescript
// src/index.ts
/**
 * Streamline Node.js SDK
 *
 * A developer-friendly SDK for the Streamline streaming platform.
 *
 * @example
 * import { Streamline } from 'streamline';
 *
 * const client = new Streamline('localhost:9092');
 *
 * // Produce messages
 * await client.produce('my-topic', { event: 'hello' });
 *
 * // Consume messages
 * for await (const message of client.consume('my-topic')) {
 *   console.log(message.value);
 * }
 */

export { Streamline } from './client';
export { Producer } from './producer';
export { Consumer } from './consumer';
export { Admin } from './admin';
export * from './types';
```

```typescript
// src/client.ts
import { Producer } from './producer';
import { Consumer, ConsumeOptions } from './consumer';
import { Admin } from './admin';
import { Message, TopicConfig, QueryResult } from './types';

export interface StreamlineOptions {
  /** Comma-separated list of broker addresses */
  bootstrapServers?: string;
  /** Client identifier */
  clientId?: string;
  /** API key for authentication */
  apiKey?: string;
  /** Enable TLS */
  tls?: boolean;
  /** Path to CA certificate */
  tlsCa?: string;
}

export class Streamline {
  private producer?: Producer;
  private consumers: Map<string, Consumer> = new Map();
  private admin?: Admin;

  constructor(
    bootstrapServers: string = 'localhost:9092',
    options: StreamlineOptions = {}
  ) {
    this.options = { bootstrapServers, ...options };
  }

  /**
   * Connect to Streamline cluster
   */
  async connect(): Promise<void> { ... }

  /**
   * Close all connections
   */
  async close(): Promise<void> { ... }

  // ========== Produce API ==========

  /**
   * Produce a message to a topic
   *
   * @param topic - Topic name
   * @param value - Message value (will be JSON serialized if object)
   * @param options - Optional produce options
   * @returns Offset of the produced message
   */
  async produce(
    topic: string,
    value: unknown,
    options?: {
      key?: string;
      partition?: number;
      headers?: Record<string, string>;
      timestamp?: number;
    }
  ): Promise<number> { ... }

  /**
   * Produce multiple messages in a batch
   */
  async produceBatch(
    topic: string,
    messages: Array<{ value: unknown; key?: string; headers?: Record<string, string> }>
  ): Promise<number[]> { ... }

  // ========== Consume API ==========

  /**
   * Consume messages from a topic
   *
   * @param topic - Topic name
   * @param options - Consume options
   * @yields Message objects
   */
  async *consume(
    topic: string,
    options?: ConsumeOptions
  ): AsyncGenerator<Message> { ... }

  /**
   * Consume messages with a callback
   */
  async subscribe(
    topic: string,
    callback: (message: Message) => Promise<void>,
    options?: ConsumeOptions
  ): Promise<() => void> { ... }

  // ========== Admin API ==========

  /**
   * Create a new topic
   */
  async createTopic(name: string, config?: TopicConfig): Promise<void> { ... }

  /**
   * Delete a topic
   */
  async deleteTopic(name: string): Promise<void> { ... }

  /**
   * List all topics
   */
  async listTopics(): Promise<string[]> { ... }

  /**
   * Get topic information
   */
  async topicInfo(name: string): Promise<TopicInfo> { ... }

  // ========== Query API ==========

  /**
   * Execute a SQL query on stream data
   */
  async query(sql: string, options?: { timeout?: number }): Promise<QueryResult> { ... }

  // ========== AI API ==========

  /**
   * Search messages by semantic meaning
   */
  async semanticSearch(
    topic: string,
    query: string,
    options?: { topK?: number; minScore?: number }
  ): Promise<Message[]> { ... }
}
```

```typescript
// src/types.ts
export interface Message {
  topic: string;
  partition: number;
  offset: number;
  timestamp: number;
  key?: string;
  value: unknown;
  headers: Record<string, string>;
}

export interface TopicConfig {
  partitions?: number;
  replicationFactor?: number;
  retentionMs?: number;
  retentionBytes?: number;
  [key: string]: unknown;
}

export interface TopicInfo {
  name: string;
  partitions: PartitionInfo[];
  config: TopicConfig;
}

export interface PartitionInfo {
  id: number;
  leader: number;
  replicas: number[];
  isr: number[];
  highWatermark: number;
}

export interface QueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
  executionTimeMs: number;
}
```

### GraphQL API

Create `src/server/graphql.rs`:

```rust
//! GraphQL API with Subscriptions
//!
//! Provides a GraphQL interface for querying and subscribing to streams.

use async_graphql::{
    Context, EmptyMutation, Object, Schema, SimpleObject, Subscription,
    ID, Json,
};
use futures_util::Stream;

/// GraphQL schema type
pub type StreamlineSchema = Schema<Query, EmptyMutation, Subscription>;

/// GraphQL Query root
pub struct Query;

#[Object]
impl Query {
    /// List all topics
    async fn topics(&self, ctx: &Context<'_>) -> Vec<Topic> { ... }

    /// Get topic by name
    async fn topic(&self, ctx: &Context<'_>, name: String) -> Option<Topic> { ... }

    /// Get messages from a topic
    async fn messages(
        &self,
        ctx: &Context<'_>,
        topic: String,
        partition: Option<i32>,
        offset: Option<i64>,
        limit: Option<i32>,
    ) -> Vec<Message> { ... }

    /// Execute SQL query
    async fn query(
        &self,
        ctx: &Context<'_>,
        sql: String,
    ) -> QueryResult { ... }

    /// Semantic search (AI feature)
    async fn search(
        &self,
        ctx: &Context<'_>,
        topic: String,
        query: String,
        top_k: Option<i32>,
    ) -> Vec<Message> { ... }

    /// Get consumer groups
    async fn consumer_groups(&self, ctx: &Context<'_>) -> Vec<ConsumerGroup> { ... }

    /// Get consumer group by ID
    async fn consumer_group(
        &self,
        ctx: &Context<'_>,
        group_id: String,
    ) -> Option<ConsumerGroup> { ... }
}

/// GraphQL Subscription root
pub struct Subscription;

#[Subscription]
impl Subscription {
    /// Subscribe to messages from a topic
    async fn messages(
        &self,
        ctx: &Context<'_>,
        topic: String,
        partition: Option<i32>,
        from_offset: Option<i64>,
    ) -> impl Stream<Item = Message> { ... }

    /// Subscribe to messages matching a filter
    async fn filtered_messages(
        &self,
        ctx: &Context<'_>,
        topic: String,
        filter: String,  // JSONPath or simple key=value
    ) -> impl Stream<Item = Message> { ... }

    /// Subscribe to topic metrics
    async fn topic_metrics(
        &self,
        ctx: &Context<'_>,
        topic: String,
    ) -> impl Stream<Item = TopicMetrics> { ... }

    /// Subscribe to consumer group updates
    async fn consumer_group_updates(
        &self,
        ctx: &Context<'_>,
        group_id: String,
    ) -> impl Stream<Item = ConsumerGroupUpdate> { ... }
}

/// Topic type
#[derive(SimpleObject)]
pub struct Topic {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i32,
    pub config: Json<TopicConfig>,
    pub metrics: TopicMetrics,
}

/// Message type
#[derive(SimpleObject)]
pub struct Message {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<String>,
    pub value: Json<serde_json::Value>,
    pub headers: Json<HashMap<String, String>>,
}

/// Query result type
#[derive(SimpleObject)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Json<serde_json::Value>>,
    pub row_count: i32,
    pub execution_time_ms: i64,
}
```

### Enhanced CLI

Enhance `src/cli.rs`:

```rust
// Add REPL mode
pub async fn run_repl() -> Result<()> {
    println!("Streamline Interactive Shell");
    println!("Type 'help' for commands, 'exit' to quit\n");

    let mut rl = Editor::new()?;
    rl.set_helper(Some(StreamlineCompleter::new()));

    loop {
        let readline = rl.readline("streamline> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(&line)?;
                match parse_and_execute(&line).await {
                    Ok(output) => println!("{}", output),
                    Err(e) => eprintln!("Error: {}", e),
                }
            }
            Err(ReadlineError::Interrupted) => continue,
            Err(ReadlineError::Eof) => break,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

// Smart autocomplete
pub struct StreamlineCompleter {
    topics: Vec<String>,
    commands: Vec<String>,
}

impl Completer for StreamlineCompleter {
    fn complete(&self, line: &str, pos: usize) -> Vec<Pair> {
        // Context-aware completion:
        // - After "consume " -> suggest topics
        // - After "produce " -> suggest topics
        // - After "groups " -> suggest group IDs
        // - SQL keywords in query mode
        ...
    }
}
```

### CLI Commands

```bash
# REPL mode
streamline-cli shell                   # Interactive shell
streamline-cli repl                    # Alias for shell

# Playground mode
streamline-cli playground              # Start with demo data
streamline-cli quickstart              # Interactive tutorial

# Smart commands
streamline-cli consume events --jq ".user.id" --filter "status=error"
streamline-cli tail events             # Like tail -f for topics
streamline-cli watch events            # Real-time metrics

# npx support (package.json bin)
npx streamline                         # Start server
npx streamline-cli topics list         # CLI commands
```

### HTTP API Additions

```
# GraphQL
POST   /graphql                         # GraphQL queries/mutations
GET    /graphql/playground              # GraphQL Playground UI

# SDK endpoints (simple REST)
POST   /api/v1/produce/{topic}          # Produce message (JSON body)
GET    /api/v1/consume/{topic}          # Consume messages (SSE stream)
POST   /api/v1/consume/{topic}/batch    # Consume batch (JSON response)

# Playground
GET    /playground                      # Browser-based playground
GET    /tutorial                        # Interactive tutorial
```

### Implementation Tasks

1. **Task 1: Python SDK Core**
   - Create `sdks/python/` directory structure
   - Implement `Streamline` client class
   - Add Kafka protocol client
   - Implement produce/consume APIs
   - Files: `sdks/python/streamline/`
   - Acceptance: Basic produce/consume works

2. **Task 2: Python SDK Advanced Features**
   - Add Consumer class with auto-commit
   - Add Admin API
   - Add query API
   - Add AI features (if enabled)
   - Add comprehensive type hints
   - Files: `sdks/python/streamline/`
   - Acceptance: All APIs functional

3. **Task 3: Python SDK Packaging**
   - Create `pyproject.toml`
   - Add tests with pytest
   - Set up CI/CD for PyPI publishing
   - Write README and examples
   - Files: `sdks/python/`
   - Acceptance: `pip install streamline` works

4. **Task 4: Node.js SDK Core**
   - Create `sdks/nodejs/` directory structure
   - Implement `Streamline` client class
   - Add TypeScript types
   - Implement produce/consume APIs
   - Files: `sdks/nodejs/src/`
   - Acceptance: Basic produce/consume works

5. **Task 5: Node.js SDK Advanced Features**
   - Add Consumer with async iterators
   - Add Admin API
   - Add query API
   - Add AI features (if enabled)
   - Files: `sdks/nodejs/src/`
   - Acceptance: All APIs functional

6. **Task 6: Node.js SDK Packaging**
   - Create `package.json` with TypeScript build
   - Add tests with Jest
   - Set up CI/CD for npm publishing
   - Write README and examples
   - Add ESM and CJS builds
   - Files: `sdks/nodejs/`
   - Acceptance: `npm install streamline` works

7. **Task 7: GraphQL Schema**
   - Add `async-graphql` dependency
   - Define Query, Mutation, Subscription types
   - Implement resolvers
   - Files: `src/server/graphql.rs`
   - Acceptance: GraphQL queries work

8. **Task 8: GraphQL Subscriptions**
   - Implement real-time subscriptions
   - Add WebSocket transport
   - Add filtering support
   - Files: `src/server/graphql.rs`
   - Acceptance: Subscriptions stream data

9. **Task 9: GraphQL Playground**
   - Add GraphQL Playground UI
   - Serve at `/graphql/playground`
   - Add documentation
   - Files: `src/server/http.rs`
   - Acceptance: Playground accessible in browser

10. **Task 10: CLI REPL Mode**
    - Add `shell` command
    - Implement readline with history
    - Add context-aware autocomplete
    - Add syntax highlighting
    - Files: `src/cli.rs`, `src/cli_utils/repl.rs`
    - Acceptance: Interactive shell works

11. **Task 11: Playground Mode**
    - Add `--playground` flag to server
    - Generate sample topics and data
    - Add interactive tutorial
    - Files: `src/main.rs`, `src/cli_utils/playground.rs`
    - Acceptance: Demo mode works out of box

12. **Task 12: npx Support**
    - Create npm package for CLI
    - Add bin entries for `streamline` commands
    - Publish to npm
    - Files: `npm/streamline-cli/`
    - Acceptance: `npx streamline` works

13. **Task 13: SDK Generators**
    - Create OpenAPI spec from HTTP API
    - Generate Go SDK stub
    - Generate Java SDK stub
    - Files: `tools/sdk-generator/`
    - Acceptance: SDKs generated from spec

14. **Task 14: Documentation & Examples**
    - Add SDK documentation
    - Create example applications
    - Add migration guides from kafka-python/node-rdkafka
    - Files: `docs/sdks/`, `examples/`
    - Acceptance: Documentation complete

## Dependencies

```toml
# GraphQL
async-graphql = { version = "7.0", features = ["playground"], optional = true }
async-graphql-axum = { version = "7.0", optional = true }

[features]
graphql = ["dep:async-graphql", "dep:async-graphql-axum"]
full = ["...", "graphql"]
```

Python SDK (`sdks/python/pyproject.toml`):
```toml
[project]
name = "streamline"
version = "0.1.0"
requires-python = ">=3.8"
dependencies = [
    "aiokafka>=0.10",
    "httpx>=0.25",
    "pydantic>=2.0",
]

[project.optional-dependencies]
dev = ["pytest", "pytest-asyncio", "black", "mypy"]
```

Node.js SDK (`sdks/nodejs/package.json`):
```json
{
  "name": "streamline",
  "version": "0.1.0",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "exports": {
    ".": {
      "import": "./dist/index.mjs",
      "require": "./dist/index.js"
    }
  },
  "dependencies": {
    "kafkajs": "^2.0.0"
  },
  "devDependencies": {
    "typescript": "^5.0.0",
    "@types/node": "^20.0.0",
    "jest": "^29.0.0",
    "tsup": "^8.0.0"
  }
}
```

## Acceptance Criteria

- [ ] Python SDK published to PyPI
- [ ] Node.js SDK published to npm
- [ ] GraphQL API with subscriptions working
- [ ] CLI REPL mode with autocomplete
- [ ] Playground mode for instant experimentation
- [ ] `npx streamline` works
- [ ] SDK documentation complete
- [ ] Example applications for each SDK
- [ ] 100% TypeScript type coverage in Node.js SDK
- [ ] Python type hints pass mypy strict
- [ ] All tests pass
- [ ] Migration guides from kafka-python/kafkajs

## Example Usage

### Python
```python
from streamline import Streamline
import asyncio

async def main():
    async with Streamline("localhost:9092") as client:
        # Create topic
        await client.create_topic("events", partitions=3)

        # Produce
        offset = await client.produce("events", {
            "user": "alice",
            "action": "login",
            "timestamp": "2024-01-15T10:30:00Z"
        })
        print(f"Produced at offset {offset}")

        # Consume
        async for msg in client.consume("events", group="my-app"):
            print(f"Got: {msg.value}")
            if msg.value["action"] == "logout":
                break

        # Query
        results = await client.query("""
            SELECT user, COUNT(*) as logins
            FROM topic('events')
            WHERE action = 'login'
            GROUP BY user
        """)
        for row in results:
            print(f"{row['user']}: {row['logins']} logins")

asyncio.run(main())
```

### Node.js / TypeScript
```typescript
import { Streamline } from 'streamline';

const client = new Streamline('localhost:9092');

// Create topic
await client.createTopic('events', { partitions: 3 });

// Produce
const offset = await client.produce('events', {
  user: 'alice',
  action: 'login',
  timestamp: new Date().toISOString(),
});
console.log(`Produced at offset ${offset}`);

// Consume with async iterator
for await (const msg of client.consume('events', { group: 'my-app' })) {
  console.log('Got:', msg.value);
  if (msg.value.action === 'logout') break;
}

// Or with callback
const unsubscribe = await client.subscribe('events', async (msg) => {
  console.log('Got:', msg.value);
}, { group: 'my-app' });

// Later...
unsubscribe();

// Query
const results = await client.query(`
  SELECT user, COUNT(*) as logins
  FROM topic('events')
  WHERE action = 'login'
  GROUP BY user
`);
console.table(results.rows);

await client.close();
```

### GraphQL
```graphql
# Query
query {
  topics {
    name
    partitions
    metrics {
      messagesPerSecond
      bytesPerSecond
    }
  }

  messages(topic: "events", limit: 10) {
    offset
    timestamp
    value
  }
}

# Subscription
subscription {
  messages(topic: "events") {
    offset
    timestamp
    value
    key
  }
}

# SQL Query
query {
  query(sql: "SELECT user, COUNT(*) FROM topic('events') GROUP BY user") {
    columns
    rows
  }
}
```

## Related Files

- `sdks/python/` - Python SDK
- `sdks/nodejs/` - Node.js SDK
- `src/server/graphql.rs` - GraphQL API
- `src/cli.rs` - CLI enhancements
- `src/cli_utils/repl.rs` - REPL mode
- `npm/streamline-cli/` - npx package
- `docs/sdks/` - SDK documentation

## Labels

`enhancement`, `feature`, `devex`, `sdk`, `game-changer`, `xlarge`
