# Streamline: The Redis of Streaming — Kafka-Compatible in a Single 20MB Binary

> We built a streaming platform that starts in 50ms, needs zero configuration, and speaks the Kafka protocol natively. Your existing Kafka clients work unchanged. It's open source under Apache 2.0.

## TL;DR

- **Kafka protocol compatible (50+ APIs)** — your existing Kafka clients, CLI tools, and libraries work unchanged
- **Single binary, <20MB, zero configuration** — `./streamline` and you're streaming
- **Built-in SQL engine (StreamQL)** — run SQL queries directly on streams with DuckDB analytics
- **Built-in CDC for 4 databases** — PostgreSQL, MySQL, MongoDB, SQL Server — no Debezium required
- **AI-native** — vector embeddings, semantic search, LLM enrichment, anomaly detection
- **WASM serverless functions** — deploy WebAssembly transforms directly to your streams
- **9 SDKs** — Java, Python, Go, Node.js, Rust, .NET, Swift, Kotlin, WASM
- **Desktop app & interactive playground** — visual TUI dashboard, SQL notebooks
- **Open source (Apache 2.0)**

---

## Why We Built This

Every backend developer eventually needs streaming. You need to process events in real time, sync data between services, or build a pipeline that reacts to changes as they happen. So you look at Kafka.

And then the pain begins. You need the JVM. You need ZooKeeper (or KRaft, if you're lucky enough to be on a recent version). You need to configure brokers, tune JVM heap sizes, manage log retention policies, set up monitoring, and deal with consumer group rebalancing. For local development, you spin up Docker Compose with three services just to produce a "hello world" message. Your CI pipeline takes 45 seconds just to start the Kafka container. The operational overhead is enormous — and for most teams, it's complete overkill.

We asked ourselves: **what if streaming was as easy as Redis?** What if you could download a single binary, run it with no arguments, and start producing and consuming messages in under a second? What if it spoke Kafka's protocol natively so you didn't have to rewrite a single line of client code? That's what we built. Streamline is a streaming platform designed for the 90% of use cases that don't need a 50-node Kafka cluster. It's fast, it's small, it's simple — and when you *do* need to scale, it clusters with Raft consensus without changing your deployment model.

---

## Quick Start

```bash
# macOS — install via Homebrew
brew install streamlinelabs/tap/streamline

# Linux — one-line install script
curl -fsSL https://get.streamline.io | sh

# Docker — zero install, just run
docker run -p 9092:9092 -p 9094:9094 ghcr.io/streamlinelabs/streamline

# From source (Rust 1.88+)
git clone https://github.com/streamlinelabs/streamline && cd streamline
cargo build --release
```

Then just run it:

```bash
$ streamline --playground
[INFO] Streamline v0.4.0 — listening on 0.0.0.0:9092 (Kafka) / 0.0.0.0:9094 (HTTP)
[INFO] Playground mode: created topics [demo-events, demo-logs, demo-metrics, demo-orders]
```

Produce and consume in seconds:

```bash
# Create a topic
$ streamline-cli topics create user-signups --partitions 3
✓ Created topic "user-signups" (3 partitions)

# Produce a message
$ streamline-cli produce user-signups -m '{"user":"alice","plan":"pro"}'
ok: topic=user-signups partition=0 offset=0

# Consume it back
$ streamline-cli consume user-signups --from-beginning
partition=0 offset=0 value={"user":"alice","plan":"pro"}
```

That's it. No ZooKeeper. No JVM. No YAML files. No 30-second startup wait.

---

## What Makes It Different

### 1. Single Binary, Zero Config

Kafka requires the JVM (~300MB), a coordination service (ZooKeeper or KRaft), and dozens of configuration parameters before you can produce your first message. Redpanda eliminates the JVM but still weighs in at 150MB+ and requires configuration tuning for production.

Streamline is a single statically-linked binary:

| Edition | Binary Size | What's Included |
|---------|-------------|-----------------|
| Lite    | ~8 MB       | Core streaming, TLS, compression, CLI |
| Full    | ~20 MB      | + Auth, clustering, analytics, CDC, AI, schema registry |

```bash
# Download, run, done
$ curl -fsSL https://get.streamline.io | sh
$ streamline
[INFO] Streamline v0.4.0 ready in 47ms
```

No JVM tuning. No heap size calculations. No `server.properties` with 200 options. It starts in milliseconds and stores data in `./data/` by default. Move the binary to another machine and it just works.

### 2. Kafka Protocol Compatible

Streamline implements 50+ Kafka wire protocol APIs. This means your existing Kafka clients — `kafka-python`, `confluent-kafka-go`, `kafkajs`, `spring-kafka`, `librdkafka` — all work without any code changes. Just point them at `localhost:9092`.

```bash
# Use standard Kafka CLI tools
$ kafka-console-producer --broker-list localhost:9092 --topic demo
>hello from kafka tools
>it just works

# Use kcat (kafkacat)
$ echo '{"event":"page_view","url":"/pricing"}' | kcat -b localhost:9092 -t analytics -P
$ kcat -b localhost:9092 -t analytics -C -e
{"event":"page_view","url":"/pricing"}

# Use any Kafka client library — zero code changes
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('events', b'{"type":"signup"}')
```

Consumer groups, exactly-once semantics, transactions, schema registry — they all work. If your Kafka client supports it and the Kafka protocol defines it, Streamline handles it.

### 3. Built-in SQL on Streams (StreamQL)

Query your streams with SQL. No Kafka Streams app, no ksqlDB cluster, no Flink deployment. Just SQL.

```sql
-- Real-time aggregation over a tumbling window
SELECT
    user_id,
    COUNT(*) as event_count,
    AVG(response_time_ms) as avg_response
FROM page_views
WHERE timestamp > NOW() - INTERVAL '5 minutes'
GROUP BY user_id
HAVING COUNT(*) > 10;
```

```sql
-- Join two streams in real time
SELECT
    o.order_id,
    o.total,
    p.status as payment_status
FROM orders o
JOIN payments p ON o.order_id = p.order_id
WHERE p.timestamp > o.timestamp
  AND p.timestamp < o.timestamp + INTERVAL '30 seconds';
```

StreamQL is powered by an embedded DuckDB analytics engine (enabled with `--features analytics`). It supports tumbling/hopping/sliding windows, stream-stream joins, and materialized views — all without leaving Streamline.

### 4. Built-in CDC — No Debezium Needed

Capture changes from your databases and stream them as events. No Kafka Connect. No Debezium. No extra JVMs.

```bash
# Start CDC from PostgreSQL — one command
$ streamline-cli cdc add postgres \
    --connection "postgres://user:pass@db:5432/myapp" \
    --tables "users,orders,payments" \
    --topic-prefix "cdc."
✓ CDC source added: cdc.users, cdc.orders, cdc.payments

# Changes flow as structured events
$ streamline-cli consume cdc.users --from-beginning
{
  "op": "INSERT",
  "table": "users",
  "after": {"id": 42, "name": "Alice", "email": "alice@example.com"},
  "ts_ms": 1710000000000
}
```

Supported databases:

| Database | Method | Feature Flag |
|----------|--------|-------------|
| PostgreSQL | Logical replication (WAL) | `postgres-cdc` |
| MySQL | Binary log replication | `mysql-cdc` |
| MongoDB | Change streams | `mongodb-cdc` |
| SQL Server | Polling-based CDC | `sqlserver-cdc` |

Enable all with `--features cdc` or individually. CDC sources are managed via CLI or the HTTP API — no XML connector configs.

### 5. AI-Native: Vector Search, LLM Enrichment, Feature Store

Streamline has first-class support for AI/ML workloads. Embed, search, and enrich your streams with AI — natively, not through a sidecar.

```bash
# Enable AI features
$ streamline --features ai

# Generate embeddings on ingest
$ streamline-cli ai embed \
    --topic support-tickets \
    --model text-embedding-3-small \
    --field "description"

# Semantic search across your streams
$ streamline-cli ai search \
    --topic support-tickets \
    --query "billing issues with enterprise accounts" \
    --top-k 10
```

```sql
-- Combine SQL + vector search
SELECT ticket_id, description, similarity_score
FROM support_tickets
WHERE vector_similarity(embedding, embed('billing dispute')) > 0.8
ORDER BY similarity_score DESC
LIMIT 5;
```

Use cases: real-time anomaly detection, LLM enrichment pipelines, semantic deduplication, ML feature stores. All running inside the same binary — no external vector database needed.

### 6. WASM Serverless Functions

Deploy WebAssembly functions that transform, filter, or enrich messages directly inside Streamline. No external function runtime. No cold starts.

```rust
// transform.rs — compile to WASM
use streamline_wasm::*;

#[streamline_function]
fn enrich_order(record: Record) -> Result<Record> {
    let mut order: serde_json::Value = record.parse_json()?;
    order["processed_at"] = serde_json::json!(chrono::Utc::now().to_rfc3339());
    order["region"] = serde_json::json!(geoip_lookup(order["ip"].as_str().unwrap()));
    Ok(Record::from_json(&order)?)
}
```

```bash
# Compile and deploy
$ cargo build --target wasm32-wasip1 --release
$ streamline-cli functions deploy enrich-order \
    --wasm ./target/wasm32-wasip1/release/transform.wasm \
    --trigger orders \
    --output enriched-orders

# Messages are transformed in-flight
$ streamline-cli consume enriched-orders --from-beginning
{"order_id":1,"total":99.99,"processed_at":"2024-03-10T...","region":"us-east-1"}
```

Functions run in a sandboxed Wasmtime runtime with microsecond-level overhead. Deploy, update, or roll back without restarting the server.

### 7. Scale-to-Zero with KEDA

Streamline publishes consumer lag metrics that work with KEDA out of the box. Scale your consumers to zero when there's no work, and back up instantly when messages arrive.

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: order-processor
spec:
  scaleTargetRef:
    name: order-processor
  minReplicaCount: 0
  maxReplicaCount: 20
  triggers:
    - type: kafka  # Works because Streamline speaks Kafka protocol
      metadata:
        bootstrapServers: streamline.default.svc:9092
        consumerGroup: order-processors
        topic: orders
        lagThreshold: "100"
```

KEDA sees Streamline as a Kafka broker. No custom scalers needed. Your existing Kafka-based KEDA configurations work unchanged.

---

## Benchmarks

All numbers measured on GitHub Actions runners (4 vCPU, 16GB RAM, SSD). Full methodology and reproduction instructions in [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

### Startup Time (cold start to first accepted connection)

| Platform | Time | Relative |
|----------|------|----------|
| **Streamline** | **~50ms** | **1x (baseline)** |
| NATS | ~100ms | 2x |
| Redpanda | ~2.5s | 50x |
| Kafka (KRaft) | ~12s | 240x |

### Binary Size

| Platform | Size | Notes |
|----------|------|-------|
| **Streamline (lite)** | **~8MB** | Core streaming + compression |
| **Streamline (full)** | **~20MB** | + auth, clustering, analytics, CDC, AI |
| NATS | ~20MB | |
| Redpanda | ~150MB | |
| Kafka | ~300MB+ | Requires JVM (~200MB) + libraries |

### Memory Usage (idle, 10 topics, 6 partitions each)

| Platform | RSS | Relative |
|----------|-----|----------|
| **Streamline** | **~15MB** | **1x (baseline)** |
| NATS | ~25MB | 1.7x |
| Redpanda | ~200MB | 13x |
| Kafka | ~500MB | 33x |

### Throughput (1KB messages, 6 partitions, 60s sustained)

| Platform | msg/sec | MB/sec | Notes |
|----------|---------|--------|-------|
| Redpanda | ~780K | ~780 | Optimized for raw throughput |
| **Streamline** | **~850K** | **~850** | (estimated) |
| Kafka | ~620K | ~620 | |
| NATS JetStream | ~300K | ~300 | |

> **Note on throughput**: Redpanda and Kafka are battle-tested at massive scale with years of production optimization. Streamline's throughput numbers are competitive for single-node deployments but multi-node performance is still in Beta. We're honest about where we are — see the [benchmarks doc](docs/BENCHMARKS.md) for full methodology.

### End-to-End Latency (p99, 1 producer → 1 consumer)

| Platform | p50 | p99 |
|----------|-----|-----|
| **Streamline** | **~0.8ms** | **~3.5ms** |
| Redpanda | ~1.1ms | ~5ms |
| NATS | ~0.5ms | ~2ms |
| Kafka | ~2.5ms | ~15ms |

---

## Architecture

```
                        ┌──────────────────────┐
                        │    Client Layer       │
                        │  Kafka │ MQTT │ gRPC  │
                        └──────────┬───────────┘
                                   │
                        ┌──────────▼───────────┐
                        │   Protocol Layer      │
                        │  50+ Kafka APIs       │
                        │  MQTT 3.1/5.0         │
                        │  AMQP 1.0 │ GraphQL   │
                        └──────────┬───────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
    ┌─────────▼────────┐ ┌────────▼────────┐ ┌────────▼────────┐
    │   Server Layer    │ │  Processing     │ │  Enterprise     │
    │  HTTP API (46     │ │  StreamQL       │ │  Auth (SASL)    │
    │   routes)         │ │  WASM Functions │ │  Clustering     │
    │  WebSocket        │ │  CDC Engine     │ │  Encryption     │
    │  Playground       │ │  AI/ML Pipeline │ │  Schema Reg.    │
    └─────────┬────────┘ └────────┬────────┘ └────────┬────────┘
              │                    │                    │
              └────────────────────┼────────────────────┘
                                   │
                        ┌──────────▼───────────┐
                        │    Core Engine        │
                        │  Topics │ Partitions  │
                        │  Consumer Groups      │
                        │  Transactions         │
                        └──────────┬───────────┘
                                   │
                        ┌──────────▼───────────┐
                        │   Storage Layer       │
                        │  Segments (STRM fmt)  │
                        │  WAL │ Bloom Filters  │
                        │  Zero-copy sendfile   │
                        │  io_uring (Linux)     │
                        │  Tiered: S3/Azure/GCS │
                        └──────────────────────┘
```

Streamline is written in Rust with Tokio for async I/O. The entire server — protocol handling, storage engine, SQL engine, CDC, AI pipelines — runs in a single process. No sidecars, no coordination services, no external dependencies.

**Key design decisions:**

- **Segment-based storage** with append-only writes, time/offset indexes, and bloom filters for fast lookups
- **Zero-copy I/O** via `sendfile(2)` on Linux/macOS and optional `io_uring` on Linux 5.1+
- **Feature flags** compile out unused code — the lite edition doesn't pay for features it doesn't use
- **Raft consensus** (via OpenRaft) for multi-node clustering without external coordination

---

## SDK Ecosystem

Streamline ships with dedicated SDKs for every major language, plus full compatibility with existing Kafka client libraries.

| SDK | Language | Package |
|-----|----------|---------|
| [streamline-java-sdk](https://github.com/streamlinelabs/streamline-java-sdk) | Java 17+ | Maven Central |
| [streamline-python-sdk](https://github.com/streamlinelabs/streamline-python-sdk) | Python 3.9+ | PyPI |
| [streamline-go-sdk](https://github.com/streamlinelabs/streamline-go-sdk) | Go 1.22+ | `go get` |
| [streamline-node-sdk](https://github.com/streamlinelabs/streamline-node-sdk) | TypeScript/Node.js | npm |
| [streamline-rust-sdk](https://github.com/streamlinelabs/streamline-rust-sdk) | Rust 1.80+ | crates.io |
| [streamline-dotnet-sdk](https://github.com/streamlinelabs/streamline-dotnet-sdk) | C# .NET 8+ | NuGet |

> **Already using Kafka?** You don't need our SDKs. Your existing Kafka clients work unchanged — just point `bootstrap.servers` to `localhost:9092`.

---

## Deployment Options

```bash
# Local development — just run the binary
$ streamline --playground

# Docker — single container
$ docker run -p 9092:9092 -p 9094:9094 ghcr.io/streamlinelabs/streamline

# Docker Compose — with persistence
$ docker compose up -d

# Kubernetes — via Helm
$ helm install streamline streamlinelabs/streamline

# Kubernetes — via operator (manages StreamlineCluster CRDs)
$ kubectl apply -f streamline-operator.yaml

# Terraform
resource "streamline_cluster" "prod" {
  name     = "production"
  replicas = 3
  edition  = "full"
}
```

---

## Feature Comparison

| Feature | Streamline | Kafka | Redpanda | NATS |
|---------|-----------|-------|----------|------|
| Single binary | ✅ | ❌ (JVM + ZK/KRaft) | ✅ | ✅ |
| Zero config | ✅ | ❌ | ❌ | ✅ |
| Kafka protocol | ✅ (50+ APIs) | ✅ (native) | ✅ | ❌ |
| Built-in SQL | ✅ (StreamQL) | ❌ (needs ksqlDB) | ❌ (needs RP Transform) | ❌ |
| Built-in CDC | ✅ (4 databases) | ❌ (needs Debezium) | ❌ | ❌ |
| AI/Vector search | ✅ | ❌ | ❌ | ❌ |
| WASM functions | ✅ | ❌ | ✅ (Data Transforms) | ❌ |
| Schema registry | ✅ (built-in) | ❌ (separate service) | ✅ (built-in) | ❌ |
| Binary size | 8–20MB | 300MB+ | 150MB+ | 20MB |
| Memory (idle) | ~15MB | ~500MB | ~200MB | ~25MB |
| Startup time | ~50ms | ~12s | ~2.5s | ~100ms |
| License | Apache 2.0 | Apache 2.0 | BSL 1.1 | Apache 2.0 |

---

## What's Next

- 🎮 **Try the interactive playground**: `streamline --playground` or visit [play.streamline.io](https://play.streamline.io)
- 🖥️ **Open the TUI dashboard**: `streamline-cli top`
- 📖 **Read the docs**: [docs.streamline.io](https://docs.streamline.io)
- 💬 **Join our community**: [Discord](https://discord.gg/streamline)
- ⭐ **Star us on GitHub**: [github.com/streamlinelabs/streamline](https://github.com/streamlinelabs/streamline)

---

## FAQ

**Q: Is this production-ready?**
A: Streamline is currently v0.4.0 and pre-1.0. Single-node deployments are suitable for development, testing, and low-to-medium throughput workloads. Clustering is Beta. We publish stability tiers for every API — see [API Stability](docs/API_STABILITY.md), which is the authoritative statement of what is Stable, Beta and Experimental.

**Q: How compatible is it with Kafka, really?**
A: We implement 50+ Kafka protocol APIs. Standard produce/consume, consumer groups, transactions, and schema registry work with all major Kafka client libraries. Some advanced admin APIs and features like quotas are still in progress. Check the [SDK Compatibility Matrix](docs/SDK_COMPATIBILITY_MATRIX.md) for details.

**Q: What about data durability?**
A: Streamline uses a write-ahead log (WAL) with configurable fsync policies. Data is stored in a custom segment format (STRM) with checksums. For critical workloads, enable `--fsync-every-write`. Tiered storage to S3/Azure/GCS is available with the `cloud-storage` feature.

**Q: Can I migrate from Kafka?**
A: Yes. Since Streamline speaks the Kafka protocol, migration is often as simple as changing `bootstrap.servers`. We provide a [migration guide](docs/KAFKA_MIGRATION.md) covering topic migration, consumer group offset transfer, and schema registry migration.

**Q: What's the catch?**
A: Streamline optimizes for simplicity and developer experience, not for maximum throughput at planetary scale. If you're processing 10M+ messages/second across 100+ brokers, Kafka or Redpanda is the right choice. If you're in the 90% of use cases that need reliable streaming without the operational overhead, Streamline is built for you.

---

## Links

| Resource | URL |
|----------|-----|
| **GitHub** | [github.com/streamlinelabs/streamline](https://github.com/streamlinelabs/streamline) |
| **Documentation** | [docs.streamline.io](https://docs.streamline.io) |
| **Playground** | [play.streamline.io](https://play.streamline.io) |
| **Discord** | [discord.gg/streamline](https://discord.gg/streamline) |
| **Benchmarks** | [docs/BENCHMARKS.md](docs/BENCHMARKS.md) |
| **Changelog** | [CHANGELOG.md](CHANGELOG.md) |
| **Contributing** | [CONTRIBUTING.md](CONTRIBUTING.md) |
| **License** | [Apache 2.0](LICENSE) |

---

*Built with ❤️ and Rust by the [StreamlineLabs](https://github.com/streamlinelabs) team.*
