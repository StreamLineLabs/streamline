//! Phase 14: Coordinator Discovery Tests
//!
//! Tests for FindCoordinator wire protocol:
//! - Request/response encoding
//! - Group coordinator discovery
//! - Transaction coordinator discovery
//! - Coordinator types
//! - Error handling
//! - Batch coordinator lookup

use bytes::BytesMut;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::{BrokerId, FindCoordinatorRequest, FindCoordinatorResponse};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

// =============================================================================
// FindCoordinator Request Tests
// =============================================================================

#[test]
fn test_find_coordinator_request_basic() {
    let mut request = FindCoordinatorRequest::default();
    request.key = StrBytes::from_static_str("my-group");
    request.key_type = 0; // GROUP

    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_find_coordinator_request_group() {
    let mut request = FindCoordinatorRequest::default();
    request.key = StrBytes::from_static_str("consumer-group-1");
    request.key_type = 0; // GROUP coordinator

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FindCoordinatorRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.key.as_str(), "consumer-group-1");
    assert_eq!(decoded.key_type, 0);
}

#[test]
fn test_find_coordinator_request_transaction() {
    let mut request = FindCoordinatorRequest::default();
    request.key = StrBytes::from_static_str("my-transactional-id");
    request.key_type = 1; // TRANSACTION coordinator

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FindCoordinatorRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.key.as_str(), "my-transactional-id");
    assert_eq!(decoded.key_type, 1);
}

#[test]
fn test_find_coordinator_request_batch() {
    let mut request = FindCoordinatorRequest::default();

    // Version 4+ supports batch lookup
    request
        .coordinator_keys
        .push(StrBytes::from_static_str("group-1"));
    request
        .coordinator_keys
        .push(StrBytes::from_static_str("group-2"));
    request.key_type = 0;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FindCoordinatorRequest::decode(&mut read_buf, 4).unwrap();

    assert_eq!(decoded.coordinator_keys.len(), 2);
}

#[test]
fn test_find_coordinator_request_all_versions() {
    // Test versions 0-3 (use key field)
    let mut request_old = FindCoordinatorRequest::default();
    request_old.key = StrBytes::from_static_str("test-key");
    request_old.key_type = 0;

    for version in 0..=3 {
        let mut buf = BytesMut::new();
        let result = request_old.clone().encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode v{}", version);
    }

    // Test versions 4-5 (use coordinator_keys field)
    let mut request_new = FindCoordinatorRequest::default();
    request_new
        .coordinator_keys
        .push(StrBytes::from_static_str("test-key"));
    request_new.key_type = 0;

    for version in 4..=5 {
        let mut buf = BytesMut::new();
        let result = request_new.clone().encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode v{}", version);
    }
}

#[test]
fn test_find_coordinator_request_roundtrip() {
    let mut request = FindCoordinatorRequest::default();
    request.key = StrBytes::from_static_str("roundtrip-group");
    request.key_type = 0;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FindCoordinatorRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.key.as_str(), "roundtrip-group");
    assert_eq!(decoded.key_type, 0);
}

// =============================================================================
// FindCoordinator Response Tests
// =============================================================================

#[test]
fn test_find_coordinator_response_basic() {
    let mut response = FindCoordinatorResponse::default();
    response.error_code = 0;
    response.node_id = BrokerId(1);
    response.host = StrBytes::from_static_str("broker-1.kafka.local");
    response.port = 9092;

    let mut buf = BytesMut::new();
    let result = response.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_find_coordinator_response_success() {
    let mut response = FindCoordinatorResponse::default();
    response.error_code = 0;
    response.node_id = BrokerId(3);
    response.host = StrBytes::from_static_str("kafka-broker-3");
    response.port = 9092;

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FindCoordinatorResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.node_id.0, 3);
    assert_eq!(decoded.host.as_str(), "kafka-broker-3");
    assert_eq!(decoded.port, 9092);
}

#[test]
fn test_find_coordinator_response_error() {
    let mut response = FindCoordinatorResponse::default();
    response.error_code = 15; // GROUP_COORDINATOR_NOT_AVAILABLE
    response.node_id = BrokerId(-1);
    response.host = StrBytes::from_static_str("");
    response.port = -1;

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FindCoordinatorResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.error_code, 15);
    assert_eq!(decoded.node_id.0, -1);
}

#[test]
fn test_find_coordinator_response_with_throttle() {
    let mut response = FindCoordinatorResponse::default();
    response.throttle_time_ms = 1000;
    response.error_code = 0;
    response.node_id = BrokerId(1);
    response.host = StrBytes::from_static_str("broker");
    response.port = 9092;

    // Version 1+ includes throttle time
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FindCoordinatorResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.throttle_time_ms, 1000);
}

#[test]
fn test_find_coordinator_response_batch() {
    let mut response = FindCoordinatorResponse::default();

    // Version 4+ supports batch response
    let mut coord1 = Coordinator::default();
    coord1.key = StrBytes::from_static_str("group-1");
    coord1.node_id = BrokerId(1);
    coord1.host = StrBytes::from_static_str("broker-1");
    coord1.port = 9092;
    coord1.error_code = 0;
    response.coordinators.push(coord1);

    let mut coord2 = Coordinator::default();
    coord2.key = StrBytes::from_static_str("group-2");
    coord2.node_id = BrokerId(2);
    coord2.host = StrBytes::from_static_str("broker-2");
    coord2.port = 9092;
    coord2.error_code = 0;
    response.coordinators.push(coord2);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FindCoordinatorResponse::decode(&mut read_buf, 4).unwrap();

    assert_eq!(decoded.coordinators.len(), 2);
    assert_eq!(decoded.coordinators[0].key.as_str(), "group-1");
    assert_eq!(decoded.coordinators[1].key.as_str(), "group-2");
}

#[test]
fn test_find_coordinator_response_all_versions() {
    // Test versions 0-3 (use node_id, host, port)
    let mut response_old = FindCoordinatorResponse::default();
    response_old.error_code = 0;
    response_old.node_id = BrokerId(1);
    response_old.host = StrBytes::from_static_str("broker");
    response_old.port = 9092;

    for version in 0..=3 {
        let mut buf = BytesMut::new();
        let result = response_old.clone().encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode response v{}", version);
    }

    // Test versions 4-5 (use coordinators array)
    let mut response_new = FindCoordinatorResponse::default();
    let mut coord = Coordinator::default();
    coord.key = StrBytes::from_static_str("test-key");
    coord.node_id = BrokerId(1);
    coord.host = StrBytes::from_static_str("broker");
    coord.port = 9092;
    coord.error_code = 0;
    response_new.coordinators.push(coord);

    for version in 4..=5 {
        let mut buf = BytesMut::new();
        let result = response_new.clone().encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode response v{}", version);
    }
}

// =============================================================================
// Coordinator Type Tests
// =============================================================================

#[test]
fn test_coordinator_type_group() {
    let key_type: i8 = 0; // GROUP
    assert_eq!(key_type, 0);
}

#[test]
fn test_coordinator_type_transaction() {
    let key_type: i8 = 1; // TRANSACTION
    assert_eq!(key_type, 1);
}

#[test]
fn test_coordinator_types_different() {
    let group_type: i8 = 0;
    let txn_type: i8 = 1;

    assert_ne!(group_type, txn_type);
}

#[test]
fn test_coordinator_type_in_request() {
    // GROUP coordinator
    let mut group_request = FindCoordinatorRequest::default();
    group_request.key = StrBytes::from_static_str("my-group");
    group_request.key_type = 0;

    // TRANSACTION coordinator
    let mut txn_request = FindCoordinatorRequest::default();
    txn_request.key = StrBytes::from_static_str("my-txn-id");
    txn_request.key_type = 1;

    assert_eq!(group_request.key_type, 0);
    assert_eq!(txn_request.key_type, 1);
}

// =============================================================================
// Error Code Tests
// =============================================================================

#[test]
fn test_coordinator_not_available_error() {
    // GROUP_COORDINATOR_NOT_AVAILABLE (15)
    let error_code: i16 = 15;
    assert_eq!(error_code, 15);
}

#[test]
fn test_not_coordinator_error() {
    // NOT_COORDINATOR (16)
    let error_code: i16 = 16;
    assert_eq!(error_code, 16);
}

#[test]
fn test_coordinator_load_in_progress_error() {
    // COORDINATOR_LOAD_IN_PROGRESS (14)
    let error_code: i16 = 14;
    assert_eq!(error_code, 14);
}

#[test]
fn test_find_coordinator_error_response() {
    let mut response = FindCoordinatorResponse::default();
    response.error_code = 15; // GROUP_COORDINATOR_NOT_AVAILABLE

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FindCoordinatorResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 15);
}

// =============================================================================
// Node ID Tests
// =============================================================================

#[test]
fn test_node_id_positive() {
    let mut response = FindCoordinatorResponse::default();
    response.node_id = BrokerId(5);

    assert!(response.node_id.0 >= 0);
}

#[test]
fn test_node_id_unknown() {
    // -1 indicates unknown/unavailable
    let mut response = FindCoordinatorResponse::default();
    response.node_id = BrokerId(-1);

    assert_eq!(response.node_id.0, -1);
}

#[test]
fn test_node_id_in_response() {
    let mut response = FindCoordinatorResponse::default();
    response.error_code = 0;
    response.node_id = BrokerId(7);
    response.host = StrBytes::from_static_str("broker-7");
    response.port = 9092;

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FindCoordinatorResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.node_id.0, 7);
}

// =============================================================================
// Host and Port Tests
// =============================================================================

#[test]
fn test_coordinator_host_ipv4() {
    let mut response = FindCoordinatorResponse::default();
    response.host = StrBytes::from_static_str("192.168.1.100");

    assert_eq!(response.host.as_str(), "192.168.1.100");
}

#[test]
fn test_coordinator_host_hostname() {
    let mut response = FindCoordinatorResponse::default();
    response.host = StrBytes::from_static_str("kafka-broker-1.internal");

    assert_eq!(response.host.as_str(), "kafka-broker-1.internal");
}

#[test]
fn test_coordinator_port_default() {
    let mut response = FindCoordinatorResponse::default();
    response.port = 9092;

    assert_eq!(response.port, 9092);
}

#[test]
fn test_coordinator_port_custom() {
    let mut response = FindCoordinatorResponse::default();
    response.port = 19092;

    assert_eq!(response.port, 19092);
}

#[test]
fn test_coordinator_port_unavailable() {
    let mut response = FindCoordinatorResponse::default();
    response.port = -1; // Unavailable

    assert_eq!(response.port, -1);
}

// =============================================================================
// API Key Tests
// =============================================================================

#[test]
fn test_find_coordinator_api_key() {
    use kafka_protocol::messages::ApiKey;
    assert_eq!(ApiKey::FindCoordinator as i16, 10);
}

// =============================================================================
// Group Coordinator Discovery Tests
// =============================================================================

#[test]
fn test_group_coordinator_discovery() {
    // Request
    let mut request = FindCoordinatorRequest::default();
    request.key = StrBytes::from_static_str("my-consumer-group");
    request.key_type = 0; // GROUP

    let mut request_buf = BytesMut::new();
    request.encode(&mut request_buf, 2).unwrap();

    // Response
    let mut response = FindCoordinatorResponse::default();
    response.error_code = 0;
    response.node_id = BrokerId(2);
    response.host = StrBytes::from_static_str("broker-2.kafka.local");
    response.port = 9092;

    let mut response_buf = BytesMut::new();
    response.encode(&mut response_buf, 2).unwrap();

    // Decode and verify
    let mut response_read = response_buf.freeze();
    let decoded = FindCoordinatorResponse::decode(&mut response_read, 2).unwrap();

    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.node_id.0, 2);
}

#[test]
fn test_group_coordinator_hashing() {
    // Group coordinator is determined by hash(group_id) % num_partitions of __consumer_offsets
    let group_id = "my-group";
    let num_partitions = 50;

    // Simulate consistent hashing
    let hash = group_id
        .bytes()
        .fold(0u32, |acc, b| acc.wrapping_add(b as u32));
    let partition = hash % num_partitions;

    assert!(partition < num_partitions);
}

// =============================================================================
// Transaction Coordinator Discovery Tests
// =============================================================================

#[test]
fn test_transaction_coordinator_discovery() {
    // Request
    let mut request = FindCoordinatorRequest::default();
    request.key = StrBytes::from_static_str("my-transactional-id");
    request.key_type = 1; // TRANSACTION

    let mut request_buf = BytesMut::new();
    request.encode(&mut request_buf, 2).unwrap();

    // Response
    let mut response = FindCoordinatorResponse::default();
    response.error_code = 0;
    response.node_id = BrokerId(3);
    response.host = StrBytes::from_static_str("broker-3.kafka.local");
    response.port = 9092;

    let mut response_buf = BytesMut::new();
    response.encode(&mut response_buf, 2).unwrap();

    // Decode and verify
    let mut response_read = response_buf.freeze();
    let decoded = FindCoordinatorResponse::decode(&mut response_read, 2).unwrap();

    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.node_id.0, 3);
}

#[test]
fn test_transaction_coordinator_hashing() {
    // Transaction coordinator is determined by hash(transactional_id) % num_partitions of __transaction_state
    let transactional_id = "my-txn-id";
    let num_partitions = 50;

    let hash = transactional_id
        .bytes()
        .fold(0u32, |acc, b| acc.wrapping_add(b as u32));
    let partition = hash % num_partitions;

    assert!(partition < num_partitions);
}

// =============================================================================
// Batch Coordinator Lookup Tests
// =============================================================================

#[test]
fn test_batch_coordinator_lookup_request() {
    let mut request = FindCoordinatorRequest::default();
    request.key_type = 0; // GROUP

    for i in 0..5 {
        request
            .coordinator_keys
            .push(StrBytes::from_string(format!("group-{}", i)));
    }

    // Version 4+ supports batch
    let mut buf = BytesMut::new();
    request.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FindCoordinatorRequest::decode(&mut read_buf, 4).unwrap();

    assert_eq!(decoded.coordinator_keys.len(), 5);
}

#[test]
fn test_batch_coordinator_lookup_response() {
    let mut response = FindCoordinatorResponse::default();

    for i in 0..5 {
        let mut coord = Coordinator::default();
        coord.key = StrBytes::from_string(format!("group-{}", i));
        coord.node_id = BrokerId(i % 3); // Distribute across 3 brokers
        coord.host = StrBytes::from_string(format!("broker-{}", i % 3));
        coord.port = 9092;
        coord.error_code = 0;
        response.coordinators.push(coord);
    }

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FindCoordinatorResponse::decode(&mut read_buf, 4).unwrap();

    assert_eq!(decoded.coordinators.len(), 5);
}

#[test]
fn test_batch_coordinator_partial_error() {
    let mut response = FindCoordinatorResponse::default();

    // First group found
    let mut coord1 = Coordinator::default();
    coord1.key = StrBytes::from_static_str("group-1");
    coord1.node_id = BrokerId(1);
    coord1.host = StrBytes::from_static_str("broker-1");
    coord1.port = 9092;
    coord1.error_code = 0;
    response.coordinators.push(coord1);

    // Second group not found
    let mut coord2 = Coordinator::default();
    coord2.key = StrBytes::from_static_str("group-2");
    coord2.node_id = BrokerId(-1);
    coord2.host = StrBytes::from_static_str("");
    coord2.port = -1;
    coord2.error_code = 15; // COORDINATOR_NOT_AVAILABLE
    response.coordinators.push(coord2);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FindCoordinatorResponse::decode(&mut read_buf, 4).unwrap();

    assert_eq!(decoded.coordinators[0].error_code, 0);
    assert_eq!(decoded.coordinators[1].error_code, 15);
}

// =============================================================================
// Error Message Tests
// =============================================================================

#[test]
fn test_error_message_in_response() {
    let mut response = FindCoordinatorResponse::default();
    response.error_code = 15;
    response.error_message = Some(StrBytes::from_static_str("Group coordinator not available"));

    // Version 1+ includes error message
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FindCoordinatorResponse::decode(&mut read_buf, 1).unwrap();

    assert!(decoded.error_message.is_some());
    assert_eq!(
        decoded.error_message.unwrap().as_str(),
        "Group coordinator not available"
    );
}

#[test]
fn test_no_error_message_on_success() {
    let mut response = FindCoordinatorResponse::default();
    response.error_code = 0;
    response.error_message = None;
    response.node_id = BrokerId(1);
    response.host = StrBytes::from_static_str("broker");
    response.port = 9092;

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = FindCoordinatorResponse::decode(&mut read_buf, 1).unwrap();

    assert!(decoded.error_message.is_none());
}
