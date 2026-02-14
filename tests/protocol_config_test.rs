//! Config API Wire Protocol Tests
//!
//! Tests for Kafka config API protocol including:
//! - DescribeConfigs request/response encoding (v0-v4)
//! - AlterConfigs request/response encoding (v0-v2)
//! - IncrementalAlterConfigs request/response encoding (v0-v1)
//!
//! Based on Kafka Protocol Specification: https://kafka.apache.org/protocol.html

use bytes::BytesMut;
use kafka_protocol::messages::{
    AlterConfigsRequest, AlterConfigsResponse, DescribeConfigsRequest, DescribeConfigsResponse,
    IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

// ============================================================================
// DescribeConfigs Request Tests (API Key 32, v0-v4)
// ============================================================================

/// Test DescribeConfigsRequest v0 basic encoding with broker resource
#[test]
fn test_describe_configs_request_v0_broker() {
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;

    let mut request = DescribeConfigsRequest::default();

    let mut resource = DescribeConfigsResource::default();
    resource.resource_type = 4; // BROKER
    resource.resource_name = StrBytes::from_static_str("0");
    request.resources.push(resource);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeConfigsRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.resources.len(), 1);
    assert_eq!(decoded.resources[0].resource_type, 4);
}

/// Test DescribeConfigsRequest v0 with topic resource
#[test]
fn test_describe_configs_request_v0_topic() {
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;

    let mut request = DescribeConfigsRequest::default();

    let mut resource = DescribeConfigsResource::default();
    resource.resource_type = 2; // TOPIC
    resource.resource_name = StrBytes::from_static_str("my-topic");
    request.resources.push(resource);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeConfigsRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.resources[0].resource_type, 2);
    assert_eq!(
        decoded.resources[0].resource_name,
        StrBytes::from_static_str("my-topic")
    );
}

/// Test DescribeConfigsRequest with specific config names
#[test]
fn test_describe_configs_request_with_config_names() {
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;

    let mut request = DescribeConfigsRequest::default();

    let mut resource = DescribeConfigsResource::default();
    resource.resource_type = 2; // TOPIC
    resource.resource_name = StrBytes::from_static_str("test-topic");
    resource.configuration_keys = Some(vec![
        StrBytes::from_static_str("retention.ms"),
        StrBytes::from_static_str("cleanup.policy"),
    ]);
    request.resources.push(resource);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeConfigsRequest::decode(&mut read_buf, 1).unwrap();

    assert!(decoded.resources[0].configuration_keys.is_some());
    assert_eq!(
        decoded.resources[0]
            .configuration_keys
            .as_ref()
            .unwrap()
            .len(),
        2
    );
}

/// Test DescribeConfigsRequest with multiple resources
#[test]
fn test_describe_configs_request_multiple_resources() {
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;

    let mut request = DescribeConfigsRequest::default();

    // Broker resource
    let mut broker_resource = DescribeConfigsResource::default();
    broker_resource.resource_type = 4; // BROKER
    broker_resource.resource_name = StrBytes::from_static_str("1");
    request.resources.push(broker_resource);

    // Topic resource
    let mut topic_resource = DescribeConfigsResource::default();
    topic_resource.resource_type = 2; // TOPIC
    topic_resource.resource_name = StrBytes::from_static_str("my-topic");
    request.resources.push(topic_resource);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeConfigsRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.resources.len(), 2);
    assert_eq!(decoded.resources[0].resource_type, 4);
    assert_eq!(decoded.resources[1].resource_type, 2);
}

/// Test DescribeConfigsRequest v3 with include_synonyms
#[test]
fn test_describe_configs_request_v3_synonyms() {
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;

    let mut request = DescribeConfigsRequest::default();
    request.include_synonyms = true;

    let mut resource = DescribeConfigsResource::default();
    resource.resource_type = 4; // BROKER
    resource.resource_name = StrBytes::from_static_str("0");
    request.resources.push(resource);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeConfigsRequest::decode(&mut read_buf, 3).unwrap();

    assert!(decoded.include_synonyms);
}

/// Test DescribeConfigsRequest v4 with include_documentation
#[test]
fn test_describe_configs_request_v4_documentation() {
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;

    let mut request = DescribeConfigsRequest::default();
    request.include_documentation = true;

    let mut resource = DescribeConfigsResource::default();
    resource.resource_type = 2; // TOPIC
    resource.resource_name = StrBytes::from_static_str("doc-topic");
    request.resources.push(resource);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeConfigsRequest::decode(&mut read_buf, 4).unwrap();

    assert!(decoded.include_documentation);
}

// ============================================================================
// DescribeConfigs Response Tests
// ============================================================================

/// Test DescribeConfigsResponse v0 basic encoding
#[test]
fn test_describe_configs_response_v0() {
    use kafka_protocol::messages::describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResult,
    };

    let mut response = DescribeConfigsResponse::default();
    response.throttle_time_ms = 0;

    let mut result = DescribeConfigsResult::default();
    result.error_code = 0;
    result.resource_type = 2; // TOPIC
    result.resource_name = StrBytes::from_static_str("my-topic");

    let mut config = DescribeConfigsResourceResult::default();
    config.name = StrBytes::from_static_str("retention.ms");
    config.value = Some(StrBytes::from_static_str("604800000"));
    config.read_only = false;
    config.is_default = true;
    config.is_sensitive = false;
    result.configs.push(config);

    response.results.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeConfigsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.results.len(), 1);
    assert_eq!(decoded.results[0].configs.len(), 1);
    assert!(decoded.results[0].configs[0].is_default);
}

/// Test DescribeConfigsResponse with error
#[test]
fn test_describe_configs_response_with_error() {
    use kafka_protocol::messages::describe_configs_response::DescribeConfigsResult;

    let mut response = DescribeConfigsResponse::default();

    let mut result = DescribeConfigsResult::default();
    result.error_code = 29; // TOPIC_AUTHORIZATION_FAILED
    result.error_message = Some(StrBytes::from_static_str("Not authorized"));
    result.resource_type = 2;
    result.resource_name = StrBytes::from_static_str("private-topic");
    response.results.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeConfigsResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.results[0].error_code, 29);
    assert!(decoded.results[0].error_message.is_some());
}

/// Test DescribeConfigsResponse with multiple configs
#[test]
fn test_describe_configs_response_multiple_configs() {
    use kafka_protocol::messages::describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResult,
    };

    let mut response = DescribeConfigsResponse::default();

    let mut result = DescribeConfigsResult::default();
    result.error_code = 0;
    result.resource_type = 2;
    result.resource_name = StrBytes::from_static_str("config-topic");

    let configs = vec![
        ("retention.ms", "604800000"),
        ("cleanup.policy", "delete"),
        ("compression.type", "producer"),
    ];

    for (name, value) in configs {
        let mut config = DescribeConfigsResourceResult::default();
        config.name = StrBytes::from_static_str(name);
        config.value = Some(StrBytes::from_static_str(value));
        result.configs.push(config);
    }

    response.results.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeConfigsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.results[0].configs.len(), 3);
}

/// Test DescribeConfigsResponse v4 (flexible version)
#[test]
fn test_describe_configs_response_v4_flexible() {
    use kafka_protocol::messages::describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResult,
    };

    let mut response = DescribeConfigsResponse::default();
    response.throttle_time_ms = 100;

    let mut result = DescribeConfigsResult::default();
    result.error_code = 0;
    result.resource_type = 4; // BROKER
    result.resource_name = StrBytes::from_static_str("0");

    let mut config = DescribeConfigsResourceResult::default();
    config.name = StrBytes::from_static_str("log.retention.hours");
    config.value = Some(StrBytes::from_static_str("168"));
    result.configs.push(config);

    response.results.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 4).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeConfigsResponse::decode(&mut read_buf, 4).unwrap();

    assert_eq!(decoded.throttle_time_ms, 100);
}

// ============================================================================
// AlterConfigs Request Tests (API Key 33, v0-v2)
// ============================================================================

/// Test AlterConfigsRequest v0 basic encoding
#[test]
fn test_alter_configs_request_v0() {
    use kafka_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};

    let mut request = AlterConfigsRequest::default();
    request.validate_only = false;

    let mut resource = AlterConfigsResource::default();
    resource.resource_type = 2; // TOPIC
    resource.resource_name = StrBytes::from_static_str("alter-topic");

    let mut config = AlterableConfig::default();
    config.name = StrBytes::from_static_str("retention.ms");
    config.value = Some(StrBytes::from_static_str("86400000"));
    resource.configs.push(config);

    request.resources.push(resource);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = AlterConfigsRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.resources.len(), 1);
    assert_eq!(decoded.resources[0].configs.len(), 1);
    assert!(!decoded.validate_only);
}

/// Test AlterConfigsRequest with validate_only
#[test]
fn test_alter_configs_request_validate_only() {
    use kafka_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};

    let mut request = AlterConfigsRequest::default();
    request.validate_only = true;

    let mut resource = AlterConfigsResource::default();
    resource.resource_type = 2; // TOPIC
    resource.resource_name = StrBytes::from_static_str("validate-topic");

    let mut config = AlterableConfig::default();
    config.name = StrBytes::from_static_str("cleanup.policy");
    config.value = Some(StrBytes::from_static_str("compact"));
    resource.configs.push(config);

    request.resources.push(resource);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = AlterConfigsRequest::decode(&mut read_buf, 1).unwrap();

    assert!(decoded.validate_only);
}

/// Test AlterConfigsRequest with multiple configs
#[test]
fn test_alter_configs_request_multiple_configs() {
    use kafka_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};

    let mut request = AlterConfigsRequest::default();

    let mut resource = AlterConfigsResource::default();
    resource.resource_type = 2; // TOPIC
    resource.resource_name = StrBytes::from_static_str("multi-config-topic");

    let configs = vec![
        ("retention.ms", "604800000"),
        ("cleanup.policy", "delete"),
        ("compression.type", "lz4"),
    ];

    for (name, value) in configs {
        let mut config = AlterableConfig::default();
        config.name = StrBytes::from_static_str(name);
        config.value = Some(StrBytes::from_static_str(value));
        resource.configs.push(config);
    }

    request.resources.push(resource);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = AlterConfigsRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.resources[0].configs.len(), 3);
}

/// Test AlterConfigsRequest v2 (flexible version)
#[test]
fn test_alter_configs_request_v2_flexible() {
    use kafka_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};

    let mut request = AlterConfigsRequest::default();

    let mut resource = AlterConfigsResource::default();
    resource.resource_type = 4; // BROKER
    resource.resource_name = StrBytes::from_static_str("0");

    let mut config = AlterableConfig::default();
    config.name = StrBytes::from_static_str("log.retention.hours");
    config.value = Some(StrBytes::from_static_str("48"));
    resource.configs.push(config);

    request.resources.push(resource);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = AlterConfigsRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.resources.len(), 1);
}

// ============================================================================
// AlterConfigs Response Tests
// ============================================================================

/// Test AlterConfigsResponse v0 basic encoding
#[test]
fn test_alter_configs_response_v0() {
    use kafka_protocol::messages::alter_configs_response::AlterConfigsResourceResponse;

    let mut response = AlterConfigsResponse::default();
    response.throttle_time_ms = 0;

    let mut result = AlterConfigsResourceResponse::default();
    result.error_code = 0;
    result.resource_type = 2; // TOPIC
    result.resource_name = StrBytes::from_static_str("altered-topic");
    response.responses.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = AlterConfigsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.responses.len(), 1);
    assert_eq!(decoded.responses[0].error_code, 0);
}

/// Test AlterConfigsResponse with error
#[test]
fn test_alter_configs_response_with_error() {
    use kafka_protocol::messages::alter_configs_response::AlterConfigsResourceResponse;

    let mut response = AlterConfigsResponse::default();

    let mut result = AlterConfigsResourceResponse::default();
    result.error_code = 41; // INVALID_CONFIG
    result.error_message = Some(StrBytes::from_static_str("Invalid configuration value"));
    result.resource_type = 2;
    result.resource_name = StrBytes::from_static_str("invalid-config-topic");
    response.responses.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = AlterConfigsResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.responses[0].error_code, 41);
    assert!(decoded.responses[0].error_message.is_some());
}

/// Test AlterConfigsResponse with throttle time
#[test]
fn test_alter_configs_response_throttle() {
    use kafka_protocol::messages::alter_configs_response::AlterConfigsResourceResponse;

    let mut response = AlterConfigsResponse::default();
    response.throttle_time_ms = 2000;

    let mut result = AlterConfigsResourceResponse::default();
    result.error_code = 0;
    result.resource_type = 2;
    result.resource_name = StrBytes::from_static_str("throttled-alter");
    response.responses.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = AlterConfigsResponse::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.throttle_time_ms, 2000);
}

// ============================================================================
// IncrementalAlterConfigs Request Tests (API Key 44, v0-v1)
// ============================================================================

/// Test IncrementalAlterConfigsRequest v0 basic encoding
#[test]
fn test_incremental_alter_configs_request_v0() {
    use kafka_protocol::messages::incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig,
    };

    let mut request = IncrementalAlterConfigsRequest::default();
    request.validate_only = false;

    let mut resource = AlterConfigsResource::default();
    resource.resource_type = 2; // TOPIC
    resource.resource_name = StrBytes::from_static_str("incremental-topic");

    let mut config = AlterableConfig::default();
    config.name = StrBytes::from_static_str("retention.ms");
    config.config_operation = 0; // SET
    config.value = Some(StrBytes::from_static_str("172800000"));
    resource.configs.push(config);

    request.resources.push(resource);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = IncrementalAlterConfigsRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.resources.len(), 1);
    assert_eq!(decoded.resources[0].configs[0].config_operation, 0);
}

/// Test IncrementalAlterConfigsRequest with DELETE operation
#[test]
fn test_incremental_alter_configs_request_delete() {
    use kafka_protocol::messages::incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig,
    };

    let mut request = IncrementalAlterConfigsRequest::default();

    let mut resource = AlterConfigsResource::default();
    resource.resource_type = 2; // TOPIC
    resource.resource_name = StrBytes::from_static_str("delete-config-topic");

    let mut config = AlterableConfig::default();
    config.name = StrBytes::from_static_str("cleanup.policy");
    config.config_operation = 1; // DELETE
    resource.configs.push(config);

    request.resources.push(resource);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = IncrementalAlterConfigsRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.resources[0].configs[0].config_operation, 1);
}

/// Test IncrementalAlterConfigsRequest with APPEND operation
#[test]
fn test_incremental_alter_configs_request_append() {
    use kafka_protocol::messages::incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig,
    };

    let mut request = IncrementalAlterConfigsRequest::default();

    let mut resource = AlterConfigsResource::default();
    resource.resource_type = 4; // BROKER
    resource.resource_name = StrBytes::from_static_str("0");

    let mut config = AlterableConfig::default();
    config.name = StrBytes::from_static_str("listener.name.internal.ssl.keystore.location");
    config.config_operation = 2; // APPEND
    config.value = Some(StrBytes::from_static_str("/path/to/new/keystore"));
    resource.configs.push(config);

    request.resources.push(resource);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = IncrementalAlterConfigsRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.resources[0].configs[0].config_operation, 2);
}

/// Test IncrementalAlterConfigsRequest with SUBTRACT operation
#[test]
fn test_incremental_alter_configs_request_subtract() {
    use kafka_protocol::messages::incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig,
    };

    let mut request = IncrementalAlterConfigsRequest::default();

    let mut resource = AlterConfigsResource::default();
    resource.resource_type = 4; // BROKER
    resource.resource_name = StrBytes::from_static_str("1");

    let mut config = AlterableConfig::default();
    config.name = StrBytes::from_static_str("listener.name.internal.ssl.keystore.location");
    config.config_operation = 3; // SUBTRACT
    config.value = Some(StrBytes::from_static_str("/path/to/remove"));
    resource.configs.push(config);

    request.resources.push(resource);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = IncrementalAlterConfigsRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.resources[0].configs[0].config_operation, 3);
}

/// Test IncrementalAlterConfigsRequest with multiple operations
#[test]
fn test_incremental_alter_configs_request_multiple_ops() {
    use kafka_protocol::messages::incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig,
    };

    let mut request = IncrementalAlterConfigsRequest::default();

    let mut resource = AlterConfigsResource::default();
    resource.resource_type = 2; // TOPIC
    resource.resource_name = StrBytes::from_static_str("multi-op-topic");

    // SET operation
    let mut config1 = AlterableConfig::default();
    config1.name = StrBytes::from_static_str("retention.ms");
    config1.config_operation = 0; // SET
    config1.value = Some(StrBytes::from_static_str("86400000"));
    resource.configs.push(config1);

    // DELETE operation
    let mut config2 = AlterableConfig::default();
    config2.name = StrBytes::from_static_str("cleanup.policy");
    config2.config_operation = 1; // DELETE
    resource.configs.push(config2);

    request.resources.push(resource);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = IncrementalAlterConfigsRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.resources[0].configs.len(), 2);
    assert_eq!(decoded.resources[0].configs[0].config_operation, 0);
    assert_eq!(decoded.resources[0].configs[1].config_operation, 1);
}

/// Test IncrementalAlterConfigsRequest v1 (flexible version)
#[test]
fn test_incremental_alter_configs_request_v1_flexible() {
    use kafka_protocol::messages::incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig,
    };

    let mut request = IncrementalAlterConfigsRequest::default();

    let mut resource = AlterConfigsResource::default();
    resource.resource_type = 2; // TOPIC
    resource.resource_name = StrBytes::from_static_str("flexible-topic");

    let mut config = AlterableConfig::default();
    config.name = StrBytes::from_static_str("compression.type");
    config.config_operation = 0; // SET
    config.value = Some(StrBytes::from_static_str("zstd"));
    resource.configs.push(config);

    request.resources.push(resource);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = IncrementalAlterConfigsRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.resources.len(), 1);
}

// ============================================================================
// IncrementalAlterConfigs Response Tests
// ============================================================================

/// Test IncrementalAlterConfigsResponse v0 basic encoding
#[test]
fn test_incremental_alter_configs_response_v0() {
    use kafka_protocol::messages::incremental_alter_configs_response::AlterConfigsResourceResponse;

    let mut response = IncrementalAlterConfigsResponse::default();
    response.throttle_time_ms = 0;

    let mut result = AlterConfigsResourceResponse::default();
    result.error_code = 0;
    result.resource_type = 2; // TOPIC
    result.resource_name = StrBytes::from_static_str("altered-topic");
    response.responses.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = IncrementalAlterConfigsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.responses.len(), 1);
    assert_eq!(decoded.responses[0].error_code, 0);
}

/// Test IncrementalAlterConfigsResponse with error
#[test]
fn test_incremental_alter_configs_response_with_error() {
    use kafka_protocol::messages::incremental_alter_configs_response::AlterConfigsResourceResponse;

    let mut response = IncrementalAlterConfigsResponse::default();

    let mut result = AlterConfigsResourceResponse::default();
    result.error_code = 41; // INVALID_CONFIG
    result.error_message = Some(StrBytes::from_static_str("Invalid config operation"));
    result.resource_type = 2;
    result.resource_name = StrBytes::from_static_str("invalid-op-topic");
    response.responses.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = IncrementalAlterConfigsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.responses[0].error_code, 41);
}

/// Test IncrementalAlterConfigsResponse with throttle time
#[test]
fn test_incremental_alter_configs_response_throttle() {
    use kafka_protocol::messages::incremental_alter_configs_response::AlterConfigsResourceResponse;

    let mut response = IncrementalAlterConfigsResponse::default();
    response.throttle_time_ms = 1500;

    let mut result = AlterConfigsResourceResponse::default();
    result.error_code = 0;
    result.resource_type = 4; // BROKER
    result.resource_name = StrBytes::from_static_str("0");
    response.responses.push(result);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = IncrementalAlterConfigsResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.throttle_time_ms, 1500);
}

// ============================================================================
// Version Roundtrip Tests
// ============================================================================

/// Test DescribeConfigs request roundtrip across all versions
#[test]
fn test_describe_configs_version_roundtrip() {
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;

    for version in 0..=4 {
        let mut request = DescribeConfigsRequest::default();

        let mut resource = DescribeConfigsResource::default();
        resource.resource_type = 2; // TOPIC
        resource.resource_name = StrBytes::from_static_str("test-topic");
        request.resources.push(resource);

        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = DescribeConfigsRequest::decode(&mut read_buf, version).unwrap();

        assert_eq!(decoded.resources.len(), 1);
    }
}

/// Test AlterConfigs request roundtrip across all versions
#[test]
fn test_alter_configs_version_roundtrip() {
    use kafka_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};

    for version in 0..=2 {
        let mut request = AlterConfigsRequest::default();

        let mut resource = AlterConfigsResource::default();
        resource.resource_type = 2; // TOPIC
        resource.resource_name = StrBytes::from_static_str("test-topic");

        let mut config = AlterableConfig::default();
        config.name = StrBytes::from_static_str("retention.ms");
        config.value = Some(StrBytes::from_static_str("86400000"));
        resource.configs.push(config);

        request.resources.push(resource);

        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = AlterConfigsRequest::decode(&mut read_buf, version).unwrap();

        assert_eq!(decoded.resources.len(), 1);
        assert_eq!(decoded.resources[0].configs.len(), 1);
    }
}

/// Test IncrementalAlterConfigs request roundtrip across all versions
#[test]
fn test_incremental_alter_configs_version_roundtrip() {
    use kafka_protocol::messages::incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig,
    };

    for version in 0..=1 {
        let mut request = IncrementalAlterConfigsRequest::default();

        let mut resource = AlterConfigsResource::default();
        resource.resource_type = 2; // TOPIC
        resource.resource_name = StrBytes::from_static_str("test-topic");

        let mut config = AlterableConfig::default();
        config.name = StrBytes::from_static_str("retention.ms");
        config.config_operation = 0; // SET
        config.value = Some(StrBytes::from_static_str("86400000"));
        resource.configs.push(config);

        request.resources.push(resource);

        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = IncrementalAlterConfigsRequest::decode(&mut read_buf, version).unwrap();

        assert_eq!(decoded.resources.len(), 1);
    }
}
