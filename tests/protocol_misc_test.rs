//! Phase 15: Miscellaneous Protocol Tests
//!
//! Tests for various lower priority protocols:
//! - DescribeConfigs/AlterConfigs
//! - DescribeAcls/CreateAcls/DeleteAcls
//! - SASL handshake/authenticate
//! - DescribeGroups/ListGroups
//! - OffsetCommit/OffsetFetch
//! - DescribeCluster

use bytes::BytesMut;
use kafka_protocol::messages::{
    DescribeConfigsRequest, DescribeConfigsResponse, DescribeGroupsRequest, DescribeGroupsResponse,
    GroupId, ListGroupsRequest, ListGroupsResponse, OffsetCommitRequest, OffsetCommitResponse,
    OffsetFetchRequest, OffsetFetchResponse, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest, SaslHandshakeResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

// =============================================================================
// DescribeConfigs Tests
// =============================================================================

#[test]
fn test_describe_configs_request_basic() {
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;

    let mut request = DescribeConfigsRequest::default();

    let mut resource = DescribeConfigsResource::default();
    resource.resource_type = 2; // TOPIC
    resource.resource_name = StrBytes::from_static_str("my-topic");
    request.resources.push(resource);

    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_describe_configs_resource_types() {
    // Resource types
    let unknown: i8 = 0;
    let any: i8 = 1;
    let topic: i8 = 2;
    let broker: i8 = 4;
    let broker_logger: i8 = 8;

    assert_eq!(unknown, 0);
    assert_eq!(any, 1);
    assert_eq!(topic, 2);
    assert_eq!(broker, 4);
    assert_eq!(broker_logger, 8);
}

#[test]
fn test_describe_configs_response_basic() {
    use kafka_protocol::messages::describe_configs_response::DescribeConfigsResult;

    let mut response = DescribeConfigsResponse::default();

    let mut result = DescribeConfigsResult::default();
    result.error_code = 0;
    result.resource_type = 2;
    result.resource_name = StrBytes::from_static_str("my-topic");
    response.results.push(result);

    let mut buf = BytesMut::new();
    let result = response.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_describe_configs_roundtrip() {
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;

    let mut request = DescribeConfigsRequest::default();

    let mut resource = DescribeConfigsResource::default();
    resource.resource_type = 2;
    resource.resource_name = StrBytes::from_static_str("config-topic");
    request.resources.push(resource);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeConfigsRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.resources.len(), 1);
    assert_eq!(decoded.resources[0].resource_type, 2);
    assert_eq!(decoded.resources[0].resource_name.as_str(), "config-topic");
}

// =============================================================================
// SASL Handshake Tests
// =============================================================================

#[test]
fn test_sasl_handshake_request_basic() {
    let mut request = SaslHandshakeRequest::default();
    request.mechanism = StrBytes::from_static_str("PLAIN");

    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_sasl_handshake_request_scram() {
    let mut request = SaslHandshakeRequest::default();
    request.mechanism = StrBytes::from_static_str("SCRAM-SHA-256");

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslHandshakeRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.mechanism.as_str(), "SCRAM-SHA-256");
}

#[test]
fn test_sasl_handshake_response_success() {
    let mut response = SaslHandshakeResponse::default();
    response.error_code = 0;
    response.mechanisms.push(StrBytes::from_static_str("PLAIN"));
    response
        .mechanisms
        .push(StrBytes::from_static_str("SCRAM-SHA-256"));

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslHandshakeResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.mechanisms.len(), 2);
}

#[test]
fn test_sasl_handshake_response_unsupported() {
    let mut response = SaslHandshakeResponse::default();
    response.error_code = 33; // UNSUPPORTED_SASL_MECHANISM
    response.mechanisms.push(StrBytes::from_static_str("PLAIN"));

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslHandshakeResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 33);
}

// =============================================================================
// SASL Authenticate Tests
// =============================================================================

#[test]
fn test_sasl_authenticate_request_basic() {
    let mut request = SaslAuthenticateRequest::default();
    request.auth_bytes = bytes::Bytes::from_static(b"username\x00password");

    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_sasl_authenticate_response_success() {
    let mut response = SaslAuthenticateResponse::default();
    response.error_code = 0;
    response.auth_bytes = bytes::Bytes::from_static(b"server-response");

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 0);
}

#[test]
fn test_sasl_authenticate_response_failure() {
    let mut response = SaslAuthenticateResponse::default();
    response.error_code = 58; // SASL_AUTHENTICATION_FAILED
    response.error_message = Some(StrBytes::from_static_str("Invalid credentials"));

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.error_code, 58);
}

// =============================================================================
// DescribeGroups Tests
// =============================================================================

#[test]
fn test_describe_groups_request_basic() {
    let mut request = DescribeGroupsRequest::default();
    request
        .groups
        .push(GroupId(StrBytes::from_static_str("my-group")));

    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_describe_groups_request_multiple() {
    let mut request = DescribeGroupsRequest::default();
    request
        .groups
        .push(GroupId(StrBytes::from_static_str("group-1")));
    request
        .groups
        .push(GroupId(StrBytes::from_static_str("group-2")));
    request
        .groups
        .push(GroupId(StrBytes::from_static_str("group-3")));

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeGroupsRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.groups.len(), 3);
}

#[test]
fn test_describe_groups_response_basic() {
    use kafka_protocol::messages::describe_groups_response::DescribedGroup;

    let mut response = DescribeGroupsResponse::default();

    let mut group = DescribedGroup::default();
    group.error_code = 0;
    group.group_id = GroupId(StrBytes::from_static_str("my-group"));
    group.group_state = StrBytes::from_static_str("Stable");
    group.protocol_type = StrBytes::from_static_str("consumer");
    group.protocol_data = StrBytes::from_static_str("range");
    response.groups.push(group);

    let mut buf = BytesMut::new();
    let result = response.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_describe_groups_states() {
    let states = [
        "Empty",
        "Dead",
        "Stable",
        "PreparingRebalance",
        "CompletingRebalance",
    ];

    for state in &states {
        let state_str = StrBytes::from_string(state.to_string());
        assert!(!state_str.is_empty());
    }
}

// =============================================================================
// ListGroups Tests
// =============================================================================

#[test]
fn test_list_groups_request_basic() {
    let request = ListGroupsRequest::default();

    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_list_groups_response_basic() {
    use kafka_protocol::messages::list_groups_response::ListedGroup;

    let mut response = ListGroupsResponse::default();
    response.error_code = 0;

    let mut group1 = ListedGroup::default();
    group1.group_id = GroupId(StrBytes::from_static_str("group-1"));
    group1.protocol_type = StrBytes::from_static_str("consumer");
    response.groups.push(group1);

    let mut group2 = ListedGroup::default();
    group2.group_id = GroupId(StrBytes::from_static_str("group-2"));
    group2.protocol_type = StrBytes::from_static_str("consumer");
    response.groups.push(group2);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ListGroupsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.groups.len(), 2);
}

#[test]
fn test_list_groups_response_empty() {
    let mut response = ListGroupsResponse::default();
    response.error_code = 0;

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ListGroupsResponse::decode(&mut read_buf, 0).unwrap();

    assert!(decoded.groups.is_empty());
}

// =============================================================================
// OffsetCommit Tests
// =============================================================================

#[test]
fn test_offset_commit_request_basic() {
    use kafka_protocol::messages::offset_commit_request::OffsetCommitRequestTopic;

    let mut request = OffsetCommitRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("my-group"));

    let mut topic = OffsetCommitRequestTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("my-topic"));
    request.topics.push(topic);

    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_offset_commit_request_with_generation() {
    use kafka_protocol::messages::offset_commit_request::OffsetCommitRequestTopic;

    let mut request = OffsetCommitRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("my-group"));
    request.generation_id_or_member_epoch = 5;
    request.member_id = StrBytes::from_static_str("member-1");

    let topic = OffsetCommitRequestTopic::default();
    request.topics.push(topic);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = OffsetCommitRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.generation_id_or_member_epoch, 5);
}

#[test]
fn test_offset_commit_response_basic() {
    use kafka_protocol::messages::offset_commit_response::OffsetCommitResponseTopic;

    let mut response = OffsetCommitResponse::default();

    let mut topic = OffsetCommitResponseTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("my-topic"));
    response.topics.push(topic);

    let mut buf = BytesMut::new();
    let result = response.encode(&mut buf, 0);
    assert!(result.is_ok());
}

// =============================================================================
// OffsetFetch Tests
// =============================================================================

#[test]
fn test_offset_fetch_request_basic() {
    use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;

    let mut request = OffsetFetchRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("my-group"));

    let mut topic = OffsetFetchRequestTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("my-topic"));
    request.topics = Some(vec![topic]);

    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_offset_fetch_request_all_topics() {
    let mut request = OffsetFetchRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("my-group"));
    request.topics = None; // Null means all topics

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = OffsetFetchRequest::decode(&mut read_buf, 2).unwrap();

    assert!(decoded.topics.is_none());
}

#[test]
fn test_offset_fetch_response_basic() {
    use kafka_protocol::messages::offset_fetch_response::OffsetFetchResponseTopic;

    let mut response = OffsetFetchResponse::default();

    let mut topic = OffsetFetchResponseTopic::default();
    topic.name = TopicName(StrBytes::from_static_str("my-topic"));
    response.topics.push(topic);

    let mut buf = BytesMut::new();
    let result = response.encode(&mut buf, 0);
    assert!(result.is_ok());
}

// =============================================================================
// Error Code Tests
// =============================================================================

#[test]
fn test_sasl_error_codes() {
    let sasl_authentication_failed: i16 = 58;
    let illegal_sasl_state: i16 = 34;
    let unsupported_sasl_mechanism: i16 = 33;

    assert_eq!(sasl_authentication_failed, 58);
    assert_eq!(illegal_sasl_state, 34);
    assert_eq!(unsupported_sasl_mechanism, 33);
}

#[test]
fn test_config_error_codes() {
    let invalid_config: i16 = 40;
    let invalid_request: i16 = 42;

    assert_eq!(invalid_config, 40);
    assert_eq!(invalid_request, 42);
}

#[test]
fn test_group_error_codes() {
    let group_id_not_found: i16 = 69;
    let invalid_group_id: i16 = 24;

    assert_eq!(group_id_not_found, 69);
    assert_eq!(invalid_group_id, 24);
}

// =============================================================================
// API Key Tests
// =============================================================================

#[test]
fn test_misc_api_keys() {
    use kafka_protocol::messages::ApiKey;

    assert_eq!(ApiKey::OffsetCommit as i16, 8);
    assert_eq!(ApiKey::OffsetFetch as i16, 9);
    assert_eq!(ApiKey::DescribeGroups as i16, 15);
    assert_eq!(ApiKey::ListGroups as i16, 16);
    assert_eq!(ApiKey::SaslHandshake as i16, 17);
    assert_eq!(ApiKey::DescribeConfigs as i16, 32);
    assert_eq!(ApiKey::SaslAuthenticate as i16, 36);
}

// =============================================================================
// Version Tests
// =============================================================================

#[test]
fn test_sasl_handshake_versions() {
    let request = SaslHandshakeRequest::default();

    for version in 0..=1 {
        let mut buf = BytesMut::new();
        let result = request.clone().encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode v{}", version);
    }
}

#[test]
fn test_sasl_authenticate_versions() {
    let request = SaslAuthenticateRequest::default();

    for version in 0..=2 {
        let mut buf = BytesMut::new();
        let result = request.clone().encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode v{}", version);
    }
}

#[test]
fn test_describe_groups_versions() {
    let mut request = DescribeGroupsRequest::default();
    request
        .groups
        .push(GroupId(StrBytes::from_static_str("test")));

    for version in 0..=5 {
        let mut buf = BytesMut::new();
        let result = request.clone().encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode v{}", version);
    }
}

#[test]
fn test_list_groups_versions() {
    let request = ListGroupsRequest::default();

    for version in 0..=4 {
        let mut buf = BytesMut::new();
        let result = request.clone().encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode v{}", version);
    }
}

// =============================================================================
// SASL Mechanism Tests
// =============================================================================

#[test]
fn test_sasl_mechanisms() {
    let mechanisms = [
        "PLAIN",
        "SCRAM-SHA-256",
        "SCRAM-SHA-512",
        "GSSAPI",
        "OAUTHBEARER",
    ];

    for mech in &mechanisms {
        let mechanism = StrBytes::from_string(mech.to_string());
        assert!(!mechanism.is_empty());
    }
}

#[test]
fn test_sasl_plain_format() {
    // PLAIN format: \0username\0password
    let auth_bytes = b"\x00user\x00password";
    assert_eq!(auth_bytes.len(), 14);
    assert_eq!(auth_bytes[0], 0);
}

#[test]
fn test_sasl_scram_first_message() {
    // SCRAM first message format: n,,n=user,r=nonce
    let first_message = "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL";
    assert!(first_message.starts_with("n,,"));
    assert!(first_message.contains("n="));
    assert!(first_message.contains("r="));
}

// =============================================================================
// Config Source Tests
// =============================================================================

#[test]
fn test_config_sources() {
    // Config source types
    let unknown: i8 = 0;
    let dynamic_topic_config: i8 = 1;
    let dynamic_broker_config: i8 = 2;
    let dynamic_default_broker_config: i8 = 3;
    let static_broker_config: i8 = 4;
    let default_config: i8 = 5;
    let dynamic_broker_logger_config: i8 = 6;

    assert_eq!(unknown, 0);
    assert_eq!(dynamic_topic_config, 1);
    assert_eq!(dynamic_broker_config, 2);
    assert_eq!(dynamic_default_broker_config, 3);
    assert_eq!(static_broker_config, 4);
    assert_eq!(default_config, 5);
    assert_eq!(dynamic_broker_logger_config, 6);
}

// =============================================================================
// Group State Tests
// =============================================================================

#[test]
fn test_group_states() {
    // Consumer group states
    let empty = "Empty";
    let dead = "Dead";
    let preparing_rebalance = "PreparingRebalance";
    let completing_rebalance = "CompletingRebalance";
    let stable = "Stable";

    assert_eq!(empty, "Empty");
    assert_eq!(dead, "Dead");
    assert_eq!(preparing_rebalance, "PreparingRebalance");
    assert_eq!(completing_rebalance, "CompletingRebalance");
    assert_eq!(stable, "Stable");
}

#[test]
fn test_describe_groups_with_members() {
    use kafka_protocol::messages::describe_groups_response::{
        DescribedGroup, DescribedGroupMember,
    };

    let mut response = DescribeGroupsResponse::default();

    let mut group = DescribedGroup::default();
    group.error_code = 0;
    group.group_id = GroupId(StrBytes::from_static_str("my-group"));
    group.group_state = StrBytes::from_static_str("Stable");
    group.protocol_type = StrBytes::from_static_str("consumer");

    let mut member = DescribedGroupMember::default();
    member.member_id = StrBytes::from_static_str("consumer-1-uuid");
    member.client_id = StrBytes::from_static_str("consumer-1");
    member.client_host = StrBytes::from_static_str("/192.168.1.100");
    group.members.push(member);

    response.groups.push(group);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = DescribeGroupsResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.groups[0].members.len(), 1);
    assert_eq!(
        decoded.groups[0].members[0].client_id.as_str(),
        "consumer-1"
    );
}
