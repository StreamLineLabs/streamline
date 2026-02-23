# Migrating from Apache Kafka to Streamline

Step-by-step guide for migrating from Apache Kafka to Streamline.

## Overview

Streamline is wire-compatible with Kafka protocol (50+ APIs, versions 3.6–3.8).
Existing Kafka clients work unchanged — you only need to change the bootstrap
server address.

## Migration Strategies

| Strategy | Downtime | Complexity | Best For |
|----------|----------|------------|----------|
| **Direct cutover** | Minutes | Low | Dev/test, small clusters |
| **Dual-write** | Zero | Medium | Production, gradual migration |
| **Shadow mode** | Zero | High | Risk-averse, large clusters |

## Quick Migration (Direct Cutover)

### Step 1: Start Streamline

```bash
# Install
curl -fsSL https://get.streamline.dev | sh

# Start
./streamline --data-dir /var/lib/streamline
```

### Step 2: Migrate topics and data

```bash
# Dry run — see what would be migrated
streamline-cli migrate from-kafka --kafka-servers kafka:9092

# Execute migration
streamline-cli migrate from-kafka --kafka-servers kafka:9092 --execute

# Include message data (not just config)
streamline-cli migrate from-kafka --kafka-servers kafka:9092 --execute --with-data
```

### Step 3: Validate

```bash
streamline-cli migrate validate \
  --kafka-servers kafka:9092 \
  --streamline-servers localhost:9092
```

### Step 4: Switch clients

Change `bootstrap.servers` in your application config:

```properties
# Before
bootstrap.servers=kafka1:9092,kafka2:9092

# After
bootstrap.servers=streamline:9092
```

No other code changes needed — Kafka clients work unchanged.

## Zero-Downtime Migration (Dual-Write)

### Step 1: Start Streamline alongside Kafka

```bash
# Use a different port to avoid conflicts
./streamline --listen-addr 0.0.0.0:19092 --data-dir /var/lib/streamline
```

### Step 2: Migrate topics

```bash
streamline-cli migrate from-kafka \
  --kafka-servers kafka:9092 \
  --execute
```

### Step 3: Enable dual-write replication

The dual-write replicator consumes from Kafka and produces to Streamline
in real-time, keeping both systems in sync:

```bash
# Start replication (Kafka → Streamline)
streamline-cli migrate autopilot \
  --kafka-servers kafka:9092 \
  --strategy shadow \
  --execute
```

### Step 4: Migrate consumers one by one

Switch each consumer's `bootstrap.servers` to Streamline.
Monitor lag to ensure no messages are missed:

```bash
# Check replication status
streamline-cli migrate status

# Monitor consumer lag
streamline-cli groups lag my-consumer-group
```

### Step 5: Switch producers

Once all consumers are on Streamline, switch producers:

```properties
bootstrap.servers=streamline:19092
```

### Step 6: Stop Kafka

```bash
# Final validation
streamline-cli migrate validate \
  --kafka-servers kafka:9092 \
  --streamline-servers localhost:19092

# Decommission Kafka
```

## What Gets Migrated

| Resource | Migrated | Notes |
|----------|----------|-------|
| Topics | ✅ | Name, partitions, replication factor |
| Topic config | ✅ | retention.ms, segment.bytes, cleanup.policy |
| Messages | ✅ (opt-in) | With `--with-data` flag |
| Consumer groups | ✅ | Group IDs and committed offsets |
| ACLs | ✅ | If auth is enabled on both sides |
| Schemas | ✅ | Schema Registry subjects and versions |
| Connectors | ⚠️ | Kafka Connect configs — manual mapping to Streamline Connect |

## Rollback

If anything goes wrong, roll back instantly:

```bash
# See migration history
streamline-cli migrate status

# Rollback a specific migration
streamline-cli migrate rollback --id <migration-id> --execute
```

## Client Compatibility

Streamline is tested with these Kafka client libraries:

| Client | Tested Versions | Status |
|--------|----------------|--------|
| kafka-clients (Java) | 3.0+ | ✅ |
| librdkafka (C/C++) | 2.0+ | ✅ |
| confluent-kafka-python | 2.0+ | ✅ |
| kafka-python | 2.0+ | ✅ |
| Sarama (Go) | 1.40+ | ✅ |
| franz-go | 1.15+ | ✅ |
| KafkaJS (Node.js) | 2.0+ | ✅ |
| confluent-kafka-dotnet | 2.0+ | ✅ |

## Web-Based Migration Wizard

A guided migration wizard is available at:
https://streamlinelabs.dev/migrate

## Need Help?

- [Troubleshooting Guide](TROUBLESHOOTING.md)
- [GitHub Discussions](https://github.com/streamlinelabs/streamline/discussions)
- [Discord](https://discord.gg/streamlinelabs)
