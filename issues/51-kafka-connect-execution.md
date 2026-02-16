# Kafka Connect Execution Engine

## Summary

Complete the Kafka Connect compatibility layer by implementing actual connector execution (source polling, sink putting), offset persistence to internal topics, filesystem-based plugin discovery, and dead letter queue for failed records.

## Current State

- REST API (Confluent-compatible): ✅ Complete (`src/connect/api.rs`)
- SMT framework: ✅ Complete (`src/connect/transforms.rs`)
- Health monitoring: ✅ Complete (`src/connect/health.rs`)
- Internal topic management: ✅ Complete (`src/connect/mod.rs`)
- Native connector SDK traits: ✅ Complete (`src/connect/native.rs`)
- Connector execution: ❌ State management only, no data processing
- Plugin discovery: ❌ Stub at `runtime.rs:211`
- Offset persistence: ❌ In-memory only
- DLQ: ❌ Stub at `api.rs:808`

## Requirements

### P0: Connector Execution Engine
| Requirement | Priority | Description |
|-------------|----------|-------------|
| Source task polling | P0 | Periodically call poll() on source connectors and produce records to target topics |
| Sink task processing | P0 | Consume from source topics and call put() on sink connectors |
| Task lifecycle | P0 | Start/stop/restart task execution loops |
| Error handling | P0 | Retry failed operations, track error counts |

### P1: Offset Persistence
| Requirement | Priority | Description |
|-------------|----------|-------------|
| Save offsets | P1 | Persist source connector offsets to connect-offsets topic |
| Load offsets | P1 | Restore offsets on connector restart |

### P1: Plugin Discovery
| Requirement | Priority | Description |
|-------------|----------|-------------|
| Filesystem scan | P1 | Scan plugin directory for native connector libraries |
| Plugin registry | P1 | Register discovered plugins with ConnectorManager |

### P2: Dead Letter Queue
| Requirement | Priority | Description |
|-------------|----------|-------------|
| DLQ topic | P2 | Create DLQ topic for failed records |
| Error routing | P2 | Route failed records to DLQ with error metadata |
| DLQ API | P2 | Expose DLQ entries via REST API |

## Implementation Tasks

1. **Execution engine**: Add source/sink task execution loops in runtime.rs
   - Files: `src/connect/runtime.rs`
   - Acceptance: Source connectors produce records, sink connectors consume records

2. **Offset persistence**: Store/restore offsets via internal topic
   - Files: `src/connect/runtime.rs`, `src/connect/mod.rs`
   - Acceptance: Connector offsets survive restarts

3. **Plugin discovery**: Filesystem-based native plugin scanning
   - Files: `src/connect/runtime.rs`
   - Acceptance: Plugins in configured directory auto-registered

4. **Dead letter queue**: DLQ for failed records
   - Files: `src/connect/runtime.rs`, `src/connect/api.rs`
   - Acceptance: Failed records routed to DLQ topic, visible via API

## Labels

`enhancement`, `feature`, `kafka-connect`, `size:large`
