# SDK Conformance Test Specification

## Overview
This specification defines 46 mandatory tests that every Tier-1 Streamline SDK must pass. Results are tracked in the [SDK Compatibility Matrix](SDK_COMPATIBILITY_MATRIX.md).

## Tier-1 SDKs
Java, Python, Go, Node.js, Rust, .NET

## Test Categories

### PRODUCER (8 tests)

| ID | Name | Description | Pass Criteria |
|----|------|-------------|---------------|
| P01 | Simple Produce | Produce a single message to a topic | Message appears at expected offset |
| P02 | Keyed Produce | Produce a message with a key | Key is preserved on consume |
| P03 | Headers Produce | Produce a message with custom headers | Headers are preserved on consume |
| P04 | Batch Produce | Produce 1000 messages in a batch | All messages appear in order |
| P05 | Compression | Produce with LZ4 compression | Consumer receives decompressed messages |
| P06 | Partitioner | Produce to specific partition | Message lands in correct partition |
| P07 | Idempotent | Produce with idempotent enabled | No duplicates after retry |
| P08 | Timeout | Produce with unreachable server | Error returned within timeout period |

### CONSUMER (8 tests)

| ID | Name | Description | Pass Criteria |
|----|------|-------------|---------------|
| C01 | Subscribe | Subscribe and consume from a topic | Receives messages in order |
| C02 | From Beginning | Consume from earliest offset | First message is offset 0 |
| C03 | From Offset | Consume starting from offset N | First message is offset N |
| C04 | From Timestamp | Consume from a specific timestamp | First message has timestamp >= target |
| C05 | Follow | Follow mode (tail -f style) | Receives new messages as produced |
| C06 | Filter | Consume with key/value filter | Only matching messages received |
| C07 | Headers | Consume and read message headers | Headers match what was produced |
| C08 | Timeout | Consume from empty topic with timeout | Returns empty or timeout error |

### CONSUMER GROUPS (6 tests)

| ID | Name | Description | Pass Criteria |
|----|------|-------------|---------------|
| G01 | Join Group | Join a consumer group | Group membership confirmed |
| G02 | Rebalance | Add second consumer to group | Partitions rebalanced |
| G03 | Commit Offsets | Commit offsets for a group | Offsets persisted and retrievable |
| G04 | Lag Monitoring | Check consumer lag | Lag value matches expected |
| G05 | Reset Offsets | Reset group offsets to earliest | Next consume starts from beginning |
| G06 | Leave Group | Leave a consumer group | Group membership updated |

### AUTHENTICATION (6 tests)

| ID | Name | Description | Pass Criteria |
|----|------|-------------|---------------|
| A01 | TLS Connect | Connect with TLS enabled | Connection established, encrypted |
| A02 | Mutual TLS | Connect with client certificate | mTLS handshake succeeds |
| A03 | SASL PLAIN | Authenticate with SASL/PLAIN | Auth succeeds, operations work |
| A04 | SCRAM-SHA-256 | Authenticate with SCRAM-SHA-256 | Auth succeeds, operations work |
| A05 | SCRAM-SHA-512 | Authenticate with SCRAM-SHA-512 | Auth succeeds, operations work |
| A06 | Auth Failure | Authenticate with wrong credentials | Auth error returned (not crash) |

### SCHEMA REGISTRY (6 tests)

| ID | Name | Description | Pass Criteria |
|----|------|-------------|---------------|
| S01 | Register Schema | Register a new schema | Schema ID returned |
| S02 | Get by ID | Retrieve schema by ID | Schema content matches |
| S03 | Get Versions | List versions for a subject | Version list returned |
| S04 | Compatibility Check | Check schema compatibility | Compatible/incompatible result |
| S05 | Avro Schema | Register and use Avro schema | Round-trip works |
| S06 | JSON Schema | Register and use JSON Schema | Round-trip works |

### ADMIN (4 tests)

| ID | Name | Description | Pass Criteria |
|----|------|-------------|---------------|
| D01 | Create Topic | Create a topic with partitions | Topic exists with correct config |
| D02 | List Topics | List all topics | Created topic appears in list |
| D03 | Describe Topic | Get topic metadata | Partition count, config correct |
| D04 | Delete Topic | Delete a topic | Topic no longer exists |

### ERROR HANDLING (4 tests)

| ID | Name | Description | Pass Criteria |
|----|------|-------------|---------------|
| E01 | Connection Refused | Connect to non-existent server | Meaningful error (not panic/crash) |
| E02 | Auth Denied | Access topic without permission | Authorization error returned |
| E03 | Topic Not Found | Consume from non-existent topic | Topic not found error |
| E04 | Request Timeout | Operation with very short timeout | Timeout error returned |

### PERFORMANCE (4 tests)

| ID | Name | Description | Pass Criteria |
|----|------|-------------|---------------|
| F01 | Throughput 1KB | Produce 100K x 1KB messages | >= 50K msg/s |
| F02 | Latency P99 | Measure P99 produce latency | < 10ms |
| F03 | Startup Time | SDK client initialization | < 500ms |
| F04 | Memory Usage | Produce 1M messages, check RSS | < 200MB |

## Test Infrastructure

### Docker Setup
All conformance tests run against a Streamline server in Docker:

```bash
docker compose -f docker-compose.conformance.yml up -d
```

The conformance server is pre-configured with:
- TLS certificates (self-signed, in `test-certs/`)
- 3 test users: `admin` (superuser), `alice` (read/write), `bob` (read-only)
- ACLs for user access control
- 3 pre-registered schemas (Avro, Protobuf, JSON)
- 10 pre-created topics (`conformance-1` through `conformance-10`)

### Reporting
Each SDK generates a JSON report:
```json
{
  "sdk": "java",
  "version": "0.2.0",
  "timestamp": "2026-03-04T10:00:00Z",
  "results": [
    { "id": "P01", "name": "Simple Produce", "status": "pass", "duration_ms": 42 },
    { "id": "P02", "name": "Keyed Produce", "status": "pass", "duration_ms": 38 }
  ],
  "summary": { "total": 46, "pass": 44, "fail": 2, "skip": 0 }
}
```
