# SDK Conformance Test Specification

This document defines the canonical set of tests that every official Streamline SDK
must pass to be certified as compatible. The conformance suite is versioned and
tracks the SDK-to-server compatibility matrix in `docs/API_STABILITY.md`.

## Version: 1.0.0

**Minimum Server Version**: 0.2.0

---

## Test Categories

### 1. Connection & Discovery (5 tests)

| ID | Test Name | Description | Required |
|----|-----------|-------------|----------|
| C01 | `connect_basic` | Connect to bootstrap server, verify connection is established | ✅ |
| C02 | `connect_metadata` | Fetch cluster metadata after connection | ✅ |
| C03 | `connect_api_versions` | Request API versions, verify expected APIs present | ✅ |
| C04 | `connect_invalid_addr` | Connect to invalid address, verify error is returned within 5s | ✅ |
| C05 | `connect_close` | Graceful close/disconnect | ✅ |

### 2. Topic Management (6 tests)

| ID | Test Name | Description | Required |
|----|-----------|-------------|----------|
| T01 | `topic_create` | Create topic with 3 partitions, verify success | ✅ |
| T02 | `topic_list` | List topics, verify created topic appears | ✅ |
| T03 | `topic_describe` | Describe topic, verify partition count matches | ✅ |
| T04 | `topic_delete` | Delete topic, verify removed from listing | ✅ |
| T05 | `topic_create_duplicate` | Create existing topic, verify appropriate error | ✅ |
| T06 | `topic_auto_create` | Produce to non-existent topic, verify auto-creation | ✅ |

### 3. Producer (8 tests)

| ID | Test Name | Description | Required |
|----|-----------|-------------|----------|
| P01 | `produce_single` | Produce 1 message, verify offset returned | ✅ |
| P02 | `produce_keyed` | Produce with string key, verify key roundtrip | ✅ |
| P03 | `produce_batch` | Produce 100 messages in batch, verify all offsets | ✅ |
| P04 | `produce_null_key` | Produce with null key, verify success | ✅ |
| P05 | `produce_large_value` | Produce 1MB message, verify success | ✅ |
| P06 | `produce_headers` | Produce with custom headers, verify header roundtrip | ✅ |
| P07 | `produce_multi_partition` | Produce to all partitions, verify per-partition offsets | ✅ |
| P08 | `produce_json_value` | Produce JSON string value, verify JSON deserialization on consume | ✅ |

### 4. Consumer (10 tests)

| ID | Test Name | Description | Required |
|----|-----------|-------------|----------|
| N01 | `consume_from_beginning` | Consume from offset 0, verify all messages returned | ✅ |
| N02 | `consume_from_latest` | Consume from latest, verify no old messages | ✅ |
| N03 | `consume_with_timeout` | Consume empty topic with timeout, verify graceful timeout | ✅ |
| N04 | `consume_max_records` | Request max_records=5, verify <= 5 returned | ✅ |
| N05 | `consume_verify_ordering` | Produce ordered messages, verify consumed in order | ✅ |
| N06 | `consume_headers` | Produce with headers, verify headers present on consume | ✅ |
| N07 | `consume_large_message` | Produce 1MB, consume and verify payload integrity | ✅ |
| N08 | `consume_multi_partition` | Consume across partitions, verify all messages received | ✅ |
| N09 | `consume_offset_seek` | Seek to specific offset, verify messages from that point | ✅ |
| N10 | `consume_earliest_latest` | Query earliest/latest offsets, verify consistency | ✅ |

### 5. Consumer Groups (8 tests)

| ID | Test Name | Description | Required |
|----|-----------|-------------|----------|
| G01 | `group_join` | Join consumer group, verify assignment received | ✅ |
| G02 | `group_commit_offset` | Commit offset, verify committed value persists | ✅ |
| G03 | `group_fetch_offset` | Fetch committed offset, verify matches committed | ✅ |
| G04 | `group_auto_commit` | Consume with auto-commit, verify offsets advance | ✅ |
| G05 | `group_rebalance` | Two consumers join, verify partition assignment splits | ✅ |
| G06 | `group_leave` | Consumer leaves, verify partitions reassigned | ✅ |
| G07 | `group_independent` | Two different groups consume same topic independently | ✅ |
| G08 | `group_describe` | Describe group, verify members and state | ✅ |

### 6. Error Handling (5 tests)

| ID | Test Name | Description | Required |
|----|-----------|-------------|----------|
| E01 | `error_unknown_topic` | Consume from non-existent topic, verify error | ✅ |
| E02 | `error_invalid_partition` | Produce to invalid partition, verify error | ✅ |
| E03 | `error_invalid_offset` | Fetch from negative offset, verify error | ✅ |
| E04 | `error_is_retryable` | Verify errors have `.retryable` flag | ✅ |
| E05 | `error_has_hint` | Verify errors have `.hint` field with guidance | ✅ |

### 7. Compression (4 tests, optional)

| ID | Test Name | Description | Required |
|----|-----------|-------------|----------|
| X01 | `compress_lz4` | Produce with LZ4, consume, verify integrity | Optional |
| X02 | `compress_zstd` | Produce with Zstd, consume, verify integrity | Optional |
| X03 | `compress_snappy` | Produce with Snappy, consume, verify integrity | Optional |
| X04 | `compress_gzip` | Produce with Gzip, consume, verify integrity | Optional |

### 8. Admin API (4 tests, optional)

| ID | Test Name | Description | Required |
|----|-----------|-------------|----------|
| A01 | `admin_health_check` | HTTP GET /health returns 200 | Optional |
| A02 | `admin_info` | HTTP GET /info returns server info | Optional |
| A03 | `admin_metrics` | HTTP GET /metrics returns Prometheus metrics | Optional |
| A04 | `admin_list_topics` | HTTP GET /api/v1/topics returns topic list | Optional |

---

## Total: 50 tests (46 required + 4 optional)

## Target SDKs

The following official SDKs must pass this conformance suite:

| SDK | Repository | Language | Test Framework |
|-----|-----------|----------|----------------|
| Java | `streamline-java-sdk` | Java 17+ | JUnit 5 |
| Python | `streamline-python-sdk` | Python 3.9+ | pytest |
| Go | `streamline-go-sdk` | Go 1.22+ | go test |
| Node.js | `streamline-node-sdk` | TypeScript | vitest |
| Rust | `streamline-rust-sdk` | Rust 1.80+ | cargo test |
| .NET | `streamline-dotnet-sdk` | C# .NET 8+ | xUnit |
| WASM | `streamline-wasm-sdk` | Rust/WASM | wasm-pack test |

## Certification Process

1. SDK maintainer adds conformance tests matching the IDs above
2. CI runs the conformance suite against a Streamline container (via Testcontainers)
3. Results are reported as a JSON summary:

```json
{
  "sdk": "streamline-java-sdk",
  "version": "0.2.0",
  "server_version": "0.2.0",
  "timestamp": "2026-02-21T15:00:00Z",
  "results": {
    "total": 50,
    "passed": 48,
    "failed": 0,
    "skipped": 2
  },
  "details": [
    { "id": "C01", "name": "connect_basic", "status": "passed", "duration_ms": 120 },
    ...
  ]
}
```

4. A conformance badge is generated:
   - ✅ **Certified** — 100% required tests pass
   - ⚠️ **Partial** — 90%+ required tests pass
   - ❌ **Failing** — <90% required tests pass

## Implementation Notes

- Each SDK should implement tests in its native test framework (JUnit, pytest, go test, vitest, xUnit)
- Tests should use the SDK's Testcontainers module to start a fresh Streamline container
- Test IDs (C01, T01, etc.) should be used as test name prefixes for traceability
- The test harness should output JSON results compatible with the schema above
- Optional tests are encouraged but do not affect certification status
