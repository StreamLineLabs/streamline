# Streamline Test Suite

This directory contains integration tests and test utilities for Streamline.

## Test Organization

### Integration Tests

| File | Description |
|------|-------------|
| `integration_test.rs` | Core integration tests for the server |
| `load_test.rs` | Load testing and performance benchmarks |
| `property_test.rs` | Property-based testing with quickcheck |
| `chaos_test.rs` | Chaos testing for resilience |
| `cluster_test.rs` | Multi-node cluster tests |
| `auth_test.rs` | Authentication and authorization tests |
| `tls_test.rs` | TLS configuration validation tests |
| `zerocopy_test.rs` | Zero-copy networking tests |
| `multi_dc_replication_test.rs` | Multi-datacenter replication tests |
| `binary_test.rs` | Binary and CLI tests |

### Protocol Tests

Protocol tests verify Kafka wire protocol compatibility across all supported API versions.

| File | Description |
|------|-------------|
| `protocol_compatibility_test.rs` | Overall protocol compatibility |
| `protocol_framing_test.rs` | Request/response framing |
| `protocol_stress_test.rs` | Protocol stress testing |
| `protocol_headers_test.rs` | Header encoding/decoding |
| `protocol_acl_test.rs` | ACL API tests (DescribeAcls, CreateAcls, DeleteAcls) |
| `protocol_admin_test.rs` | Admin API tests |
| `protocol_api_versions_test.rs` | ApiVersions API tests |
| `protocol_config_test.rs` | Config API tests (DescribeConfigs, AlterConfigs) |
| `protocol_consumer_group_test.rs` | Consumer group API tests |
| `protocol_coordinator_test.rs` | Coordinator API tests |
| `protocol_encoding_test.rs` | Wire encoding tests |
| `protocol_error_codes_test.rs` | Error code handling tests |
| `protocol_error_handling_test.rs` | Error handling behavior tests |
| `protocol_fetch_features_test.rs` | Fetch API feature tests |
| `protocol_leader_epoch_test.rs` | Leader epoch handling tests |
| `protocol_misc_test.rs` | Miscellaneous protocol tests |
| `protocol_primitives_test.rs` | Primitive type encoding tests |
| `protocol_sasl_test.rs` | SASL authentication tests |
| `protocol_edge_cases_test.rs` | Edge case handling |
| `protocol_idempotent_producer_test.rs` | Idempotent producer tests |
| `protocol_isolation_levels_test.rs` | Transaction isolation tests |
| `protocol_pipelining_test.rs` | Request pipelining tests |
| `protocol_record_batch_test.rs` | Record batch format tests |
| `protocol_tagged_fields_test.rs` | Flexible version tagged fields |
| `protocol_topic_uuid_test.rs` | Topic UUID handling tests |
| `protocol_transaction_test.rs` | Transaction API tests |
| `protocol_version_negotiation_test.rs` | Version negotiation tests |

## Test Helpers

### `protocol_test_helpers.rs`

This module provides utilities for building and parsing Kafka protocol messages in tests.

#### Request Building

```rust
use crate::protocol_test_helpers::*;

// Build a complete Kafka request with proper headers
let request = build_request(
    api_key,
    api_version,
    correlation_id,
    client_id,
    &request_body,
);

// Convenience functions for common requests
let api_versions_req = build_api_versions_request(3, 1234);
let metadata_req = build_metadata_request(9, 1234, Some(vec!["topic1", "topic2"]));
```

#### Response Parsing

```rust
// Parse size-prefixed response
if let Some(body) = parse_response(&response_bytes) {
    let correlation_id = extract_correlation_id(body);

    // Parse specific response types
    let api_versions = parse_api_versions_response(body, api_version)?;
    let metadata = parse_metadata_response(body, api_version)?;
}
```

#### Header Version Helpers

The Kafka protocol uses different header versions depending on the API and version.
These functions determine the correct header versions:

```rust
// Get request header version (1 = non-flexible, 2 = flexible)
let req_header_ver = request_header_version(api_key, api_version);

// Get response header version (0 = non-flexible, 1 = flexible)
let resp_header_ver = response_header_version(api_key, api_version);

// Get the threshold version where flexible encoding starts
let (req_flex_start, resp_flex_start) = expected_header_versions(api_key);
```

#### API Constants

```rust
// All supported API keys
for &api_key in SUPPORTED_API_KEYS {
    let name = api_key_name(api_key);
    let max_version = max_supported_version(api_key);
    println!("{} ({}): supports v0-{}", name, api_key, max_version);
}
```

## Running Tests

```bash
# Run all tests
cargo test

# Run specific test file
cargo test --test integration_test

# Run tests matching a pattern
cargo test protocol_

# Run with output visible
cargo test -- --nocapture

# Run a single test
cargo test test_api_versions_request
```

## Test Certificates

Some TLS tests require certificates. Generate them with:

```bash
./tests/certs/generate_certs.sh
```

This creates:
- `tests/certs/ca-cert.pem` - CA certificate
- `tests/certs/server-cert.pem` - Server certificate
- `tests/certs/server-key.pem` - Server private key
- `tests/certs/client-cert.pem` - Client certificate (for mTLS)
- `tests/certs/client-key.pem` - Client private key (for mTLS)

## Writing New Tests

1. **Protocol tests**: Use `protocol_test_helpers.rs` for request/response handling
2. **Integration tests**: Start a test server, connect via TCP, exercise the API
3. **Unit tests**: Place in `#[cfg(test)] mod tests { }` within source files

### Example Protocol Test

```rust
mod protocol_test_helpers;
use protocol_test_helpers::*;

#[test]
fn test_my_api() {
    let request = build_request(
        MY_API_KEY,
        api_version,
        1,
        Some("test-client"),
        &MyRequest::default(),
    );

    // Send to test server...
    // Parse response...
    // Assert expected behavior...
}
```
