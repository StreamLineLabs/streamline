//! ACL API Wire Protocol Tests
//!
//! Tests for Kafka ACL API protocol including:
//! - DescribeAcls request/response encoding (v0-v3)
//! - CreateAcls request/response encoding (v0-v3)
//! - DeleteAcls request/response encoding (v0-v3)
//!
//! Based on Kafka Protocol Specification: https://kafka.apache.org/protocol.html

use bytes::BytesMut;
use kafka_protocol::messages::{
    CreateAclsRequest, CreateAclsResponse, DeleteAclsRequest, DeleteAclsResponse,
    DescribeAclsRequest, DescribeAclsResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

// ACL Resource Types (per Kafka spec)
const RESOURCE_TYPE_ANY: i8 = 1;
const RESOURCE_TYPE_TOPIC: i8 = 2;
const RESOURCE_TYPE_GROUP: i8 = 3;
const RESOURCE_TYPE_CLUSTER: i8 = 4;
const RESOURCE_TYPE_TRANSACTIONAL_ID: i8 = 5;

// ACL Pattern Types
const PATTERN_TYPE_ANY: i8 = 1;
const PATTERN_TYPE_LITERAL: i8 = 3;
const PATTERN_TYPE_PREFIXED: i8 = 4;

// ACL Operations (full set for reference, not all used in tests)
#[allow(dead_code)]
const OPERATION_ANY: i8 = 1;
#[allow(dead_code)]
const OPERATION_ALL: i8 = 2;
const OPERATION_READ: i8 = 3;
const OPERATION_WRITE: i8 = 4;
#[allow(dead_code)]
const OPERATION_CREATE: i8 = 5;
#[allow(dead_code)]
const OPERATION_DELETE: i8 = 6;
const OPERATION_ALTER: i8 = 7;
#[allow(dead_code)]
const OPERATION_DESCRIBE: i8 = 8;

// ACL Permission Types
const PERMISSION_ANY: i8 = 1;
const PERMISSION_DENY: i8 = 2;
const PERMISSION_ALLOW: i8 = 3;

// ============================================================================
// DescribeAcls Request Tests (API Key 29, v0-v3)
// ============================================================================

/// Test DescribeAclsRequest v0 basic encoding - any filter
/// Note: pattern_type_filter was introduced in v1, so v0 doesn't support it
#[test]
fn test_describe_acls_request_v0_any_filter() {
    let mut request = DescribeAclsRequest::default();
    request.resource_type_filter = RESOURCE_TYPE_ANY;
    request.resource_name_filter = None;
    // pattern_type_filter not available in v0, leave at default
    request.principal_filter = None;
    request.host_filter = None;
    request.operation = OPERATION_ANY;
    request.permission_type = PERMISSION_ANY;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeAclsRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.resource_type_filter, RESOURCE_TYPE_ANY);
    assert_eq!(decoded.operation, OPERATION_ANY);
    assert_eq!(decoded.permission_type, PERMISSION_ANY);
}

/// Test DescribeAclsRequest v0 with topic filter
/// Note: pattern_type_filter was introduced in v1
#[test]
fn test_describe_acls_request_v0_topic_filter() {
    let mut request = DescribeAclsRequest::default();
    request.resource_type_filter = RESOURCE_TYPE_TOPIC;
    request.resource_name_filter = Some(StrBytes::from_static_str("my-topic"));
    // pattern_type_filter not available in v0, leave at default
    request.principal_filter = Some(StrBytes::from_static_str("User:alice"));
    request.host_filter = Some(StrBytes::from_static_str("*"));
    request.operation = OPERATION_READ;
    request.permission_type = PERMISSION_ALLOW;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeAclsRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.resource_type_filter, RESOURCE_TYPE_TOPIC);
    assert_eq!(decoded.operation, OPERATION_READ);
    assert!(decoded.resource_name_filter.is_some());
}

/// Test DescribeAclsRequest with group filter
#[test]
fn test_describe_acls_request_group_filter() {
    let mut request = DescribeAclsRequest::default();
    request.resource_type_filter = RESOURCE_TYPE_GROUP;
    request.resource_name_filter = Some(StrBytes::from_static_str("my-consumer-group"));
    request.pattern_type_filter = PATTERN_TYPE_LITERAL;
    request.operation = OPERATION_READ;
    request.permission_type = PERMISSION_ALLOW;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeAclsRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.resource_type_filter, RESOURCE_TYPE_GROUP);
}

/// Test DescribeAclsRequest with cluster filter
#[test]
fn test_describe_acls_request_cluster_filter() {
    let mut request = DescribeAclsRequest::default();
    request.resource_type_filter = RESOURCE_TYPE_CLUSTER;
    request.resource_name_filter = Some(StrBytes::from_static_str("kafka-cluster"));
    request.pattern_type_filter = PATTERN_TYPE_LITERAL;
    request.operation = OPERATION_ALTER;
    request.permission_type = PERMISSION_ALLOW;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeAclsRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.resource_type_filter, RESOURCE_TYPE_CLUSTER);
    assert_eq!(decoded.operation, OPERATION_ALTER);
}

/// Test DescribeAclsRequest with prefixed pattern
#[test]
fn test_describe_acls_request_prefixed_pattern() {
    let mut request = DescribeAclsRequest::default();
    request.resource_type_filter = RESOURCE_TYPE_TOPIC;
    request.resource_name_filter = Some(StrBytes::from_static_str("prod-"));
    request.pattern_type_filter = PATTERN_TYPE_PREFIXED;
    request.operation = OPERATION_WRITE;
    request.permission_type = PERMISSION_ALLOW;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeAclsRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.pattern_type_filter, PATTERN_TYPE_PREFIXED);
}

/// Test DescribeAclsRequest v2 (flexible version)
#[test]
fn test_describe_acls_request_v2_flexible() {
    let mut request = DescribeAclsRequest::default();
    request.resource_type_filter = RESOURCE_TYPE_TOPIC;
    request.resource_name_filter = Some(StrBytes::from_static_str("flexible-topic"));
    request.pattern_type_filter = PATTERN_TYPE_LITERAL;
    request.operation = OPERATION_ALL;
    request.permission_type = PERMISSION_ANY;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeAclsRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.operation, OPERATION_ALL);
}

/// Test DescribeAclsRequest v3
#[test]
fn test_describe_acls_request_v3() {
    let mut request = DescribeAclsRequest::default();
    request.resource_type_filter = RESOURCE_TYPE_TRANSACTIONAL_ID;
    request.resource_name_filter = Some(StrBytes::from_static_str("txn-id"));
    request.pattern_type_filter = PATTERN_TYPE_LITERAL;
    request.operation = OPERATION_WRITE;
    request.permission_type = PERMISSION_ALLOW;

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeAclsRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.resource_type_filter, RESOURCE_TYPE_TRANSACTIONAL_ID);
}

// ============================================================================
// DescribeAcls Response Tests
// ============================================================================

/// Test DescribeAclsResponse v0 basic encoding
#[test]
fn test_describe_acls_response_v0() {
    use kafka_protocol::messages::describe_acls_response::{AclDescription, DescribeAclsResource};

    let mut response = DescribeAclsResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = 0;

    let mut resource = DescribeAclsResource::default();
    resource.resource_type = RESOURCE_TYPE_TOPIC;
    resource.resource_name = StrBytes::from_static_str("my-topic");
    resource.pattern_type = PATTERN_TYPE_LITERAL;

    let mut acl = AclDescription::default();
    acl.principal = StrBytes::from_static_str("User:alice");
    acl.host = StrBytes::from_static_str("*");
    acl.operation = OPERATION_READ;
    acl.permission_type = PERMISSION_ALLOW;
    resource.acls.push(acl);

    response.resources.push(resource);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeAclsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.resources.len(), 1);
    assert_eq!(decoded.resources[0].acls.len(), 1);
}

/// Test DescribeAclsResponse with multiple ACLs
#[test]
fn test_describe_acls_response_multiple_acls() {
    use kafka_protocol::messages::describe_acls_response::{AclDescription, DescribeAclsResource};

    let mut response = DescribeAclsResponse::default();
    response.error_code = 0;

    let mut resource = DescribeAclsResource::default();
    resource.resource_type = RESOURCE_TYPE_TOPIC;
    resource.resource_name = StrBytes::from_static_str("shared-topic");
    resource.pattern_type = PATTERN_TYPE_LITERAL;

    // Multiple ACLs for same resource
    for (principal, op, perm) in [
        ("User:alice", OPERATION_READ, PERMISSION_ALLOW),
        ("User:bob", OPERATION_WRITE, PERMISSION_ALLOW),
        ("User:eve", OPERATION_READ, PERMISSION_DENY),
    ] {
        let mut acl = AclDescription::default();
        acl.principal = StrBytes::from_static_str(principal);
        acl.host = StrBytes::from_static_str("*");
        acl.operation = op;
        acl.permission_type = perm;
        resource.acls.push(acl);
    }

    response.resources.push(resource);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeAclsResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.resources[0].acls.len(), 3);
}

/// Test DescribeAclsResponse with error
#[test]
fn test_describe_acls_response_with_error() {
    let mut response = DescribeAclsResponse::default();
    response.error_code = 31; // CLUSTER_AUTHORIZATION_FAILED
    response.error_message = Some(StrBytes::from_static_str("Not authorized to describe ACLs"));

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeAclsResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.error_code, 31);
    assert!(decoded.error_message.is_some());
}

/// Test DescribeAclsResponse v2 (flexible version)
#[test]
fn test_describe_acls_response_v2_flexible() {
    use kafka_protocol::messages::describe_acls_response::{AclDescription, DescribeAclsResource};

    let mut response = DescribeAclsResponse::default();
    response.throttle_time_ms = 100;
    response.error_code = 0;

    let mut resource = DescribeAclsResource::default();
    resource.resource_type = RESOURCE_TYPE_GROUP;
    resource.resource_name = StrBytes::from_static_str("consumer-group");
    resource.pattern_type = PATTERN_TYPE_LITERAL;

    let mut acl = AclDescription::default();
    acl.principal = StrBytes::from_static_str("User:consumer");
    acl.host = StrBytes::from_static_str("*");
    acl.operation = OPERATION_READ;
    acl.permission_type = PERMISSION_ALLOW;
    resource.acls.push(acl);

    response.resources.push(resource);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeAclsResponse::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.throttle_time_ms, 100);
}

// ============================================================================
// CreateAcls Request Tests (API Key 30, v0-v3)
// ============================================================================

/// Test CreateAclsRequest v0 basic encoding
#[test]
fn test_create_acls_request_v0() {
    use kafka_protocol::messages::create_acls_request::AclCreation;

    let mut request = CreateAclsRequest::default();

    let mut acl = AclCreation::default();
    acl.resource_type = RESOURCE_TYPE_TOPIC;
    acl.resource_name = StrBytes::from_static_str("new-topic");
    acl.resource_pattern_type = PATTERN_TYPE_LITERAL;
    acl.principal = StrBytes::from_static_str("User:producer");
    acl.host = StrBytes::from_static_str("*");
    acl.operation = OPERATION_WRITE;
    acl.permission_type = PERMISSION_ALLOW;
    request.creations.push(acl);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateAclsRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.creations.len(), 1);
    assert_eq!(decoded.creations[0].resource_type, RESOURCE_TYPE_TOPIC);
    assert_eq!(decoded.creations[0].operation, OPERATION_WRITE);
}

/// Test CreateAclsRequest with multiple ACLs
#[test]
fn test_create_acls_request_multiple() {
    use kafka_protocol::messages::create_acls_request::AclCreation;

    let mut request = CreateAclsRequest::default();

    // Create multiple ACLs
    let acl_configs = vec![
        (RESOURCE_TYPE_TOPIC, "topic-1", OPERATION_READ),
        (RESOURCE_TYPE_TOPIC, "topic-2", OPERATION_WRITE),
        (RESOURCE_TYPE_GROUP, "group-1", OPERATION_READ),
    ];

    for (res_type, res_name, op) in acl_configs {
        let mut acl = AclCreation::default();
        acl.resource_type = res_type;
        acl.resource_name = StrBytes::from_static_str(res_name);
        acl.resource_pattern_type = PATTERN_TYPE_LITERAL;
        acl.principal = StrBytes::from_static_str("User:admin");
        acl.host = StrBytes::from_static_str("*");
        acl.operation = op;
        acl.permission_type = PERMISSION_ALLOW;
        request.creations.push(acl);
    }

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateAclsRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.creations.len(), 3);
}

/// Test CreateAclsRequest with prefixed pattern
#[test]
fn test_create_acls_request_prefixed() {
    use kafka_protocol::messages::create_acls_request::AclCreation;

    let mut request = CreateAclsRequest::default();

    let mut acl = AclCreation::default();
    acl.resource_type = RESOURCE_TYPE_TOPIC;
    acl.resource_name = StrBytes::from_static_str("dev-");
    acl.resource_pattern_type = PATTERN_TYPE_PREFIXED;
    acl.principal = StrBytes::from_static_str("User:developer");
    acl.host = StrBytes::from_static_str("*");
    acl.operation = OPERATION_ALL;
    acl.permission_type = PERMISSION_ALLOW;
    request.creations.push(acl);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateAclsRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(
        decoded.creations[0].resource_pattern_type,
        PATTERN_TYPE_PREFIXED
    );
}

/// Test CreateAclsRequest with DENY permission
#[test]
fn test_create_acls_request_deny() {
    use kafka_protocol::messages::create_acls_request::AclCreation;

    let mut request = CreateAclsRequest::default();

    let mut acl = AclCreation::default();
    acl.resource_type = RESOURCE_TYPE_TOPIC;
    acl.resource_name = StrBytes::from_static_str("sensitive-topic");
    acl.resource_pattern_type = PATTERN_TYPE_LITERAL;
    acl.principal = StrBytes::from_static_str("User:untrusted");
    acl.host = StrBytes::from_static_str("*");
    acl.operation = OPERATION_ALL;
    acl.permission_type = PERMISSION_DENY;
    request.creations.push(acl);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateAclsRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.creations[0].permission_type, PERMISSION_DENY);
}

/// Test CreateAclsRequest v2 (flexible version)
#[test]
fn test_create_acls_request_v2_flexible() {
    use kafka_protocol::messages::create_acls_request::AclCreation;

    let mut request = CreateAclsRequest::default();

    let mut acl = AclCreation::default();
    acl.resource_type = RESOURCE_TYPE_CLUSTER;
    acl.resource_name = StrBytes::from_static_str("kafka-cluster");
    acl.resource_pattern_type = PATTERN_TYPE_LITERAL;
    acl.principal = StrBytes::from_static_str("User:admin");
    acl.host = StrBytes::from_static_str("*");
    acl.operation = OPERATION_ALTER;
    acl.permission_type = PERMISSION_ALLOW;
    request.creations.push(acl);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateAclsRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.creations[0].resource_type, RESOURCE_TYPE_CLUSTER);
}

/// Test CreateAclsRequest v3
#[test]
fn test_create_acls_request_v3() {
    use kafka_protocol::messages::create_acls_request::AclCreation;

    let mut request = CreateAclsRequest::default();

    let mut acl = AclCreation::default();
    acl.resource_type = RESOURCE_TYPE_TRANSACTIONAL_ID;
    acl.resource_name = StrBytes::from_static_str("my-txn-id");
    acl.resource_pattern_type = PATTERN_TYPE_LITERAL;
    acl.principal = StrBytes::from_static_str("User:transactional-producer");
    acl.host = StrBytes::from_static_str("*");
    acl.operation = OPERATION_WRITE;
    acl.permission_type = PERMISSION_ALLOW;
    request.creations.push(acl);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateAclsRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(
        decoded.creations[0].resource_type,
        RESOURCE_TYPE_TRANSACTIONAL_ID
    );
}

// ============================================================================
// CreateAcls Response Tests
// ============================================================================

/// Test CreateAclsResponse v0 basic encoding
#[test]
fn test_create_acls_response_v0() {
    use kafka_protocol::messages::create_acls_response::AclCreationResult;

    let mut response = CreateAclsResponse::default();
    response.throttle_time_ms = 0;

    let mut result = AclCreationResult::default();
    result.error_code = 0;
    response.results.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateAclsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.results.len(), 1);
    assert_eq!(decoded.results[0].error_code, 0);
}

/// Test CreateAclsResponse with error
#[test]
fn test_create_acls_response_with_error() {
    use kafka_protocol::messages::create_acls_response::AclCreationResult;

    let mut response = CreateAclsResponse::default();

    let mut result = AclCreationResult::default();
    result.error_code = 31; // CLUSTER_AUTHORIZATION_FAILED
    result.error_message = Some(StrBytes::from_static_str("Not authorized to create ACL"));
    response.results.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateAclsResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.results[0].error_code, 31);
    assert!(decoded.results[0].error_message.is_some());
}

/// Test CreateAclsResponse with mixed results
#[test]
fn test_create_acls_response_mixed() {
    use kafka_protocol::messages::create_acls_response::AclCreationResult;

    let mut response = CreateAclsResponse::default();

    // Success result
    let mut result1 = AclCreationResult::default();
    result1.error_code = 0;
    response.results.push(result1);

    // Error result
    let mut result2 = AclCreationResult::default();
    result2.error_code = 38; // INVALID_REQUEST
    result2.error_message = Some(StrBytes::from_static_str("Invalid ACL"));
    response.results.push(result2);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateAclsResponse::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.results.len(), 2);
    assert_eq!(decoded.results[0].error_code, 0);
    assert_eq!(decoded.results[1].error_code, 38);
}

/// Test CreateAclsResponse with throttle time
#[test]
fn test_create_acls_response_throttle() {
    use kafka_protocol::messages::create_acls_response::AclCreationResult;

    let mut response = CreateAclsResponse::default();
    response.throttle_time_ms = 500;

    let mut result = AclCreationResult::default();
    result.error_code = 0;
    response.results.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = CreateAclsResponse::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.throttle_time_ms, 500);
}

// ============================================================================
// DeleteAcls Request Tests (API Key 31, v0-v3)
// ============================================================================

/// Test DeleteAclsRequest v0 basic encoding
#[test]
fn test_delete_acls_request_v0() {
    use kafka_protocol::messages::delete_acls_request::DeleteAclsFilter;

    let mut request = DeleteAclsRequest::default();

    let mut filter = DeleteAclsFilter::default();
    filter.resource_type_filter = RESOURCE_TYPE_TOPIC;
    filter.resource_name_filter = Some(StrBytes::from_static_str("delete-topic"));
    filter.pattern_type_filter = PATTERN_TYPE_LITERAL;
    filter.principal_filter = Some(StrBytes::from_static_str("User:removed"));
    filter.host_filter = Some(StrBytes::from_static_str("*"));
    filter.operation = OPERATION_ALL;
    filter.permission_type = PERMISSION_ANY;
    request.filters.push(filter);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteAclsRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.filters.len(), 1);
    assert_eq!(decoded.filters[0].resource_type_filter, RESOURCE_TYPE_TOPIC);
}

/// Test DeleteAclsRequest with multiple filters
#[test]
fn test_delete_acls_request_multiple_filters() {
    use kafka_protocol::messages::delete_acls_request::DeleteAclsFilter;

    let mut request = DeleteAclsRequest::default();

    // First filter
    let mut filter1 = DeleteAclsFilter::default();
    filter1.resource_type_filter = RESOURCE_TYPE_TOPIC;
    filter1.resource_name_filter = Some(StrBytes::from_static_str("topic-1"));
    filter1.pattern_type_filter = PATTERN_TYPE_LITERAL;
    filter1.operation = OPERATION_ANY;
    filter1.permission_type = PERMISSION_ANY;
    request.filters.push(filter1);

    // Second filter
    let mut filter2 = DeleteAclsFilter::default();
    filter2.resource_type_filter = RESOURCE_TYPE_GROUP;
    filter2.resource_name_filter = Some(StrBytes::from_static_str("group-1"));
    filter2.pattern_type_filter = PATTERN_TYPE_LITERAL;
    filter2.operation = OPERATION_ANY;
    filter2.permission_type = PERMISSION_ANY;
    request.filters.push(filter2);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteAclsRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.filters.len(), 2);
}

/// Test DeleteAclsRequest with wildcard filter
#[test]
fn test_delete_acls_request_wildcard() {
    use kafka_protocol::messages::delete_acls_request::DeleteAclsFilter;

    let mut request = DeleteAclsRequest::default();

    let mut filter = DeleteAclsFilter::default();
    filter.resource_type_filter = RESOURCE_TYPE_ANY;
    filter.resource_name_filter = None;
    filter.pattern_type_filter = PATTERN_TYPE_ANY;
    filter.principal_filter = Some(StrBytes::from_static_str("User:revoked"));
    filter.host_filter = None;
    filter.operation = OPERATION_ANY;
    filter.permission_type = PERMISSION_ANY;
    request.filters.push(filter);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteAclsRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.filters[0].resource_type_filter, RESOURCE_TYPE_ANY);
}

/// Test DeleteAclsRequest v2 (flexible version)
#[test]
fn test_delete_acls_request_v2_flexible() {
    use kafka_protocol::messages::delete_acls_request::DeleteAclsFilter;

    let mut request = DeleteAclsRequest::default();

    let mut filter = DeleteAclsFilter::default();
    filter.resource_type_filter = RESOURCE_TYPE_CLUSTER;
    filter.resource_name_filter = Some(StrBytes::from_static_str("kafka-cluster"));
    filter.pattern_type_filter = PATTERN_TYPE_LITERAL;
    filter.operation = OPERATION_ALTER;
    filter.permission_type = PERMISSION_ALLOW;
    request.filters.push(filter);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteAclsRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(
        decoded.filters[0].resource_type_filter,
        RESOURCE_TYPE_CLUSTER
    );
}

/// Test DeleteAclsRequest v3
#[test]
fn test_delete_acls_request_v3() {
    use kafka_protocol::messages::delete_acls_request::DeleteAclsFilter;

    let mut request = DeleteAclsRequest::default();

    let mut filter = DeleteAclsFilter::default();
    filter.resource_type_filter = RESOURCE_TYPE_TRANSACTIONAL_ID;
    filter.resource_name_filter = Some(StrBytes::from_static_str("txn-to-delete"));
    filter.pattern_type_filter = PATTERN_TYPE_LITERAL;
    filter.operation = OPERATION_WRITE;
    filter.permission_type = PERMISSION_ALLOW;
    request.filters.push(filter);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteAclsRequest::decode(&mut read_buf, 3).unwrap();

    assert_eq!(
        decoded.filters[0].resource_type_filter,
        RESOURCE_TYPE_TRANSACTIONAL_ID
    );
}

// ============================================================================
// DeleteAcls Response Tests
// ============================================================================

/// Test DeleteAclsResponse v0 basic encoding
#[test]
fn test_delete_acls_response_v0() {
    use kafka_protocol::messages::delete_acls_response::{
        DeleteAclsFilterResult, DeleteAclsMatchingAcl,
    };

    let mut response = DeleteAclsResponse::default();
    response.throttle_time_ms = 0;

    let mut filter_result = DeleteAclsFilterResult::default();
    filter_result.error_code = 0;

    let mut matching_acl = DeleteAclsMatchingAcl::default();
    matching_acl.error_code = 0;
    matching_acl.resource_type = RESOURCE_TYPE_TOPIC;
    matching_acl.resource_name = StrBytes::from_static_str("deleted-topic");
    matching_acl.pattern_type = PATTERN_TYPE_LITERAL;
    matching_acl.principal = StrBytes::from_static_str("User:removed");
    matching_acl.host = StrBytes::from_static_str("*");
    matching_acl.operation = OPERATION_WRITE;
    matching_acl.permission_type = PERMISSION_ALLOW;
    filter_result.matching_acls.push(matching_acl);

    response.filter_results.push(filter_result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteAclsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.filter_results.len(), 1);
    assert_eq!(decoded.filter_results[0].matching_acls.len(), 1);
}

/// Test DeleteAclsResponse with error
#[test]
fn test_delete_acls_response_with_error() {
    use kafka_protocol::messages::delete_acls_response::DeleteAclsFilterResult;

    let mut response = DeleteAclsResponse::default();

    let mut filter_result = DeleteAclsFilterResult::default();
    filter_result.error_code = 31; // CLUSTER_AUTHORIZATION_FAILED
    filter_result.error_message = Some(StrBytes::from_static_str("Not authorized to delete ACL"));
    response.filter_results.push(filter_result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteAclsResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.filter_results[0].error_code, 31);
}

/// Test DeleteAclsResponse with multiple matching ACLs
#[test]
fn test_delete_acls_response_multiple_matches() {
    use kafka_protocol::messages::delete_acls_response::{
        DeleteAclsFilterResult, DeleteAclsMatchingAcl,
    };

    let mut response = DeleteAclsResponse::default();

    let mut filter_result = DeleteAclsFilterResult::default();
    filter_result.error_code = 0;

    for i in 0..3 {
        let mut matching_acl = DeleteAclsMatchingAcl::default();
        matching_acl.error_code = 0;
        matching_acl.resource_type = RESOURCE_TYPE_TOPIC;
        matching_acl.resource_name = StrBytes::from_string(format!("topic-{}", i));
        matching_acl.pattern_type = PATTERN_TYPE_LITERAL;
        matching_acl.principal = StrBytes::from_static_str("User:bulk-delete");
        matching_acl.host = StrBytes::from_static_str("*");
        matching_acl.operation = OPERATION_READ;
        matching_acl.permission_type = PERMISSION_ALLOW;
        filter_result.matching_acls.push(matching_acl);
    }

    response.filter_results.push(filter_result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteAclsResponse::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.filter_results[0].matching_acls.len(), 3);
}

/// Test DeleteAclsResponse with throttle time
#[test]
fn test_delete_acls_response_throttle() {
    use kafka_protocol::messages::delete_acls_response::DeleteAclsFilterResult;

    let mut response = DeleteAclsResponse::default();
    response.throttle_time_ms = 1000;

    let mut filter_result = DeleteAclsFilterResult::default();
    filter_result.error_code = 0;
    response.filter_results.push(filter_result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DeleteAclsResponse::decode(&mut read_buf, 3).unwrap();

    assert_eq!(decoded.throttle_time_ms, 1000);
}

// ============================================================================
// Version Roundtrip Tests
// ============================================================================

/// Test DescribeAcls request roundtrip across all versions
#[test]
fn test_describe_acls_version_roundtrip() {
    for version in 0..=3 {
        let mut request = DescribeAclsRequest::default();
        request.resource_type_filter = RESOURCE_TYPE_TOPIC;
        request.resource_name_filter = Some(StrBytes::from_static_str("test-topic"));
        request.pattern_type_filter = PATTERN_TYPE_LITERAL;
        request.operation = OPERATION_READ;
        request.permission_type = PERMISSION_ALLOW;

        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = DescribeAclsRequest::decode(&mut read_buf, version).unwrap();

        assert_eq!(decoded.resource_type_filter, RESOURCE_TYPE_TOPIC);
        assert_eq!(decoded.operation, OPERATION_READ);
    }
}

/// Test CreateAcls request roundtrip across all versions
#[test]
fn test_create_acls_version_roundtrip() {
    use kafka_protocol::messages::create_acls_request::AclCreation;

    for version in 0..=3 {
        let mut request = CreateAclsRequest::default();

        let mut acl = AclCreation::default();
        acl.resource_type = RESOURCE_TYPE_TOPIC;
        acl.resource_name = StrBytes::from_static_str("test-topic");
        acl.resource_pattern_type = PATTERN_TYPE_LITERAL;
        acl.principal = StrBytes::from_static_str("User:test");
        acl.host = StrBytes::from_static_str("*");
        acl.operation = OPERATION_READ;
        acl.permission_type = PERMISSION_ALLOW;
        request.creations.push(acl);

        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = CreateAclsRequest::decode(&mut read_buf, version).unwrap();

        assert_eq!(decoded.creations.len(), 1);
        assert_eq!(decoded.creations[0].resource_type, RESOURCE_TYPE_TOPIC);
    }
}

/// Test DeleteAcls request roundtrip across all versions
#[test]
fn test_delete_acls_version_roundtrip() {
    use kafka_protocol::messages::delete_acls_request::DeleteAclsFilter;

    for version in 0..=3 {
        let mut request = DeleteAclsRequest::default();

        let mut filter = DeleteAclsFilter::default();
        filter.resource_type_filter = RESOURCE_TYPE_TOPIC;
        filter.resource_name_filter = Some(StrBytes::from_static_str("test-topic"));
        filter.pattern_type_filter = PATTERN_TYPE_LITERAL;
        filter.operation = OPERATION_ANY;
        filter.permission_type = PERMISSION_ANY;
        request.filters.push(filter);

        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = DeleteAclsRequest::decode(&mut read_buf, version).unwrap();

        assert_eq!(decoded.filters.len(), 1);
        assert_eq!(decoded.filters[0].resource_type_filter, RESOURCE_TYPE_TOPIC);
    }
}
