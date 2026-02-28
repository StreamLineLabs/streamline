# Pub/Sub (Redis-Compatible)

Streamline includes a Redis-compatible pub/sub system alongside its Kafka-protocol streaming.

## Overview

The pub/sub module provides **fire-and-forget real-time messaging** without persistence. This is complementary to Streamline's primary Kafka-protocol topics which provide durable, ordered, replayable streams.

| Feature | Pub/Sub | Kafka-Protocol Topics |
|---------|---------|----------------------|
| Persistence | ❌ No | ✅ Yes |
| Replay | ❌ No | ✅ Yes (from any offset) |
| Consumer Groups | ❌ No | ✅ Yes |
| Ordering | ❌ Best-effort | ✅ Per-partition |
| Latency | ⚡ Ultra-low | ⚡ Low |
| Pattern Matching | ✅ Glob patterns | ❌ No |
| Use Case | Real-time notifications | Event streaming |

## When to Use Pub/Sub

- **Real-time notifications** (e.g., "user X is typing")
- **Cache invalidation** signals
- **Live dashboards** with ephemeral updates
- **Service discovery** announcements

## When to Use Kafka-Protocol Topics

- **Event sourcing** and audit logs
- **Stream processing** pipelines
- **Consumer groups** with load balancing
- **Replay** from any point in time

## API

### Subscribe to a channel
```bash
# Via WebSocket
ws://localhost:9094/ws/pubsub/subscribe?channel=notifications
```

### Publish to a channel
```bash
curl -X POST http://localhost:9094/api/pubsub/publish \
  -H "Content-Type: application/json" \
  -d '{"channel": "notifications", "payload": "hello"}'
```

### Pattern subscriptions
```bash
# Subscribe to all channels matching a glob pattern
ws://localhost:9094/ws/pubsub/subscribe?pattern=user.*
```

## Configuration

```toml
[pubsub]
channel_capacity = 1000        # Max messages buffered per channel
cleanup_interval_secs = 300    # Cleanup empty channels every 5 minutes
```
