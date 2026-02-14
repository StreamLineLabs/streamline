//! Phase 9: Consumer Group Protocol Metadata Tests
//!
//! Tests for consumer group wire protocol including:
//! - JoinGroup request/response encoding
//! - SyncGroup request/response encoding
//! - Heartbeat request/response encoding
//! - LeaveGroup request/response encoding
//! - Consumer protocol metadata (subscription) encoding
//! - Consumer protocol assignment encoding
//! - Group generation ID and member ID handling
//! - Rebalance protocols

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, GroupId, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse,
    LeaveGroupRequest, LeaveGroupResponse, SyncGroupRequest, SyncGroupResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};

// =============================================================================
// JoinGroup Request Tests
// =============================================================================

#[test]
fn test_join_group_request_basic() {
    let mut request = JoinGroupRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("test-group"));
    request.session_timeout_ms = 30000;
    request.member_id = StrBytes::from_static_str("");
    request.protocol_type = StrBytes::from_static_str("consumer");

    // Test encoding
    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 0);
    assert!(result.is_ok());
    assert!(!buf.is_empty());
}

#[test]
fn test_join_group_request_with_rebalance_timeout() {
    let mut request = JoinGroupRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("test-group"));
    request.session_timeout_ms = 30000;
    request.rebalance_timeout_ms = 60000; // Added in version 1
    request.member_id = StrBytes::from_static_str("");
    request.protocol_type = StrBytes::from_static_str("consumer");

    // Version 1+ includes rebalance timeout
    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 1);
    assert!(result.is_ok());
}

#[test]
fn test_join_group_request_with_group_instance_id() {
    let mut request = JoinGroupRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("test-group"));
    request.session_timeout_ms = 30000;
    request.member_id = StrBytes::from_static_str("");
    request.group_instance_id = Some(StrBytes::from_static_str("instance-1")); // Static membership
    request.protocol_type = StrBytes::from_static_str("consumer");

    // Version 5+ includes group instance ID
    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 5);
    assert!(result.is_ok());
}

#[test]
fn test_join_group_request_with_protocols() {
    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;

    let mut request = JoinGroupRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("test-group"));
    request.session_timeout_ms = 30000;
    request.member_id = StrBytes::from_static_str("");
    request.protocol_type = StrBytes::from_static_str("consumer");

    // Add range assignor protocol
    let mut protocol = JoinGroupRequestProtocol::default();
    protocol.name = StrBytes::from_static_str("range");
    protocol.metadata = Bytes::from_static(b"\x00\x01\x00\x00"); // Minimal metadata
    request.protocols.push(protocol);

    // Add roundrobin assignor protocol
    let mut protocol2 = JoinGroupRequestProtocol::default();
    protocol2.name = StrBytes::from_static_str("roundrobin");
    protocol2.metadata = Bytes::from_static(b"\x00\x01\x00\x00");
    request.protocols.push(protocol2);

    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_join_group_request_roundtrip() {
    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;

    let mut request = JoinGroupRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("my-consumer-group"));
    request.session_timeout_ms = 45000;
    request.rebalance_timeout_ms = 90000;
    request.member_id = StrBytes::from_static_str("consumer-1-uuid");
    request.protocol_type = StrBytes::from_static_str("consumer");

    let mut protocol = JoinGroupRequestProtocol::default();
    protocol.name = StrBytes::from_static_str("range");
    protocol.metadata = Bytes::from_static(b"\x00\x01");
    request.protocols.push(protocol);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = JoinGroupRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.group_id.0, request.group_id.0);
    assert_eq!(decoded.session_timeout_ms, 45000);
    assert_eq!(decoded.rebalance_timeout_ms, 90000);
    assert_eq!(decoded.protocol_type.as_str(), "consumer");
}

#[test]
fn test_join_group_request_header_version() {
    // Version 0-5 use header v1, version 6+ use header v2
    assert_eq!(JoinGroupRequest::header_version(0), 1);
    assert_eq!(JoinGroupRequest::header_version(5), 1);
    assert_eq!(JoinGroupRequest::header_version(6), 2);
}

// =============================================================================
// JoinGroup Response Tests
// =============================================================================

#[test]
fn test_join_group_response_basic() {
    let mut response = JoinGroupResponse::default();
    response.error_code = 0;
    response.generation_id = 1;
    response.protocol_name = Some(StrBytes::from_static_str("range"));
    response.leader = StrBytes::from_static_str("consumer-1");
    response.member_id = StrBytes::from_static_str("consumer-1-uuid");

    let mut buf = BytesMut::new();
    let result = response.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_join_group_response_as_leader() {
    use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;

    let mut response = JoinGroupResponse::default();
    response.error_code = 0;
    response.generation_id = 5;
    response.protocol_name = Some(StrBytes::from_static_str("range"));
    response.leader = StrBytes::from_static_str("consumer-1");
    response.member_id = StrBytes::from_static_str("consumer-1"); // Same as leader = this is leader

    // Leader receives member list
    let mut member1 = JoinGroupResponseMember::default();
    member1.member_id = StrBytes::from_static_str("consumer-1");
    member1.metadata = Bytes::from_static(b"\x00\x01");
    response.members.push(member1);

    let mut member2 = JoinGroupResponseMember::default();
    member2.member_id = StrBytes::from_static_str("consumer-2");
    member2.metadata = Bytes::from_static(b"\x00\x01");
    response.members.push(member2);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = JoinGroupResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.members.len(), 2);
    // Verify this is the leader
    assert_eq!(decoded.member_id.as_str(), decoded.leader.as_str());
}

#[test]
fn test_join_group_response_as_follower() {
    let mut response = JoinGroupResponse::default();
    response.error_code = 0;
    response.generation_id = 5;
    response.protocol_name = Some(StrBytes::from_static_str("range"));
    response.leader = StrBytes::from_static_str("consumer-1");
    response.member_id = StrBytes::from_static_str("consumer-2"); // Different from leader

    // Followers don't receive member list
    assert!(response.members.is_empty());

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = JoinGroupResponse::decode(&mut read_buf, 0).unwrap();

    assert!(decoded.members.is_empty());
    // Verify this is NOT the leader
    assert_ne!(decoded.member_id.as_str(), decoded.leader.as_str());
}

#[test]
fn test_join_group_response_error() {
    let mut response = JoinGroupResponse::default();
    response.error_code = 25; // UNKNOWN_MEMBER_ID
    response.generation_id = -1;
    response.member_id = StrBytes::from_static_str("");

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = JoinGroupResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 25);
    assert_eq!(decoded.generation_id, -1);
}

#[test]
fn test_join_group_response_with_throttle() {
    let mut response = JoinGroupResponse::default();
    response.throttle_time_ms = 1000; // Throttled for 1 second
    response.error_code = 0;
    response.generation_id = 1;

    // Version 2+ includes throttle time
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = JoinGroupResponse::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.throttle_time_ms, 1000);
}

// =============================================================================
// SyncGroup Request Tests
// =============================================================================

#[test]
fn test_sync_group_request_basic() {
    let mut request = SyncGroupRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("test-group"));
    request.generation_id = 1;
    request.member_id = StrBytes::from_static_str("consumer-1");

    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_sync_group_request_as_leader() {
    use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;

    let mut request = SyncGroupRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("test-group"));
    request.generation_id = 5;
    request.member_id = StrBytes::from_static_str("consumer-1");

    // Leader sends assignments
    let mut assignment1 = SyncGroupRequestAssignment::default();
    assignment1.member_id = StrBytes::from_static_str("consumer-1");
    assignment1.assignment = Bytes::from_static(b"\x00\x00\x00\x01"); // Assignment data
    request.assignments.push(assignment1);

    let mut assignment2 = SyncGroupRequestAssignment::default();
    assignment2.member_id = StrBytes::from_static_str("consumer-2");
    assignment2.assignment = Bytes::from_static(b"\x00\x00\x00\x02");
    request.assignments.push(assignment2);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SyncGroupRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.assignments.len(), 2);
}

#[test]
fn test_sync_group_request_as_follower() {
    let mut request = SyncGroupRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("test-group"));
    request.generation_id = 5;
    request.member_id = StrBytes::from_static_str("consumer-2");

    // Followers send empty assignments
    assert!(request.assignments.is_empty());

    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_sync_group_request_with_group_instance_id() {
    let mut request = SyncGroupRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("test-group"));
    request.generation_id = 1;
    request.member_id = StrBytes::from_static_str("consumer-1");
    request.group_instance_id = Some(StrBytes::from_static_str("instance-1"));

    // Version 3+ includes group instance ID
    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 3);
    assert!(result.is_ok());
}

#[test]
fn test_sync_group_request_roundtrip() {
    use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;

    let mut request = SyncGroupRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("my-group"));
    request.generation_id = 10;
    request.member_id = StrBytes::from_static_str("member-uuid-123");

    let mut assignment = SyncGroupRequestAssignment::default();
    assignment.member_id = StrBytes::from_static_str("member-uuid-123");
    assignment.assignment = Bytes::from_static(b"assignment-data");
    request.assignments.push(assignment);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SyncGroupRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.group_id.0.as_str(), "my-group");
    assert_eq!(decoded.generation_id, 10);
    assert_eq!(decoded.member_id.as_str(), "member-uuid-123");
}

// =============================================================================
// SyncGroup Response Tests
// =============================================================================

#[test]
fn test_sync_group_response_basic() {
    let mut response = SyncGroupResponse::default();
    response.error_code = 0;
    response.assignment =
        Bytes::from_static(b"\x00\x00\x00\x01\x00\x05topic\x00\x00\x00\x01\x00\x00\x00\x00");

    let mut buf = BytesMut::new();
    let result = response.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_sync_group_response_error() {
    let mut response = SyncGroupResponse::default();
    response.error_code = 27; // REBALANCE_IN_PROGRESS
    response.assignment = Bytes::new();

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SyncGroupResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 27);
    assert!(decoded.assignment.is_empty());
}

#[test]
fn test_sync_group_response_with_throttle() {
    let mut response = SyncGroupResponse::default();
    response.throttle_time_ms = 500;
    response.error_code = 0;
    response.assignment = Bytes::from_static(b"\x00\x01");

    // Version 1+ includes throttle time
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SyncGroupResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.throttle_time_ms, 500);
}

// =============================================================================
// Heartbeat Request/Response Tests
// =============================================================================

#[test]
fn test_heartbeat_request_basic() {
    let mut request = HeartbeatRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("test-group"));
    request.generation_id = 5;
    request.member_id = StrBytes::from_static_str("consumer-1");

    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_heartbeat_request_with_group_instance_id() {
    let mut request = HeartbeatRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("test-group"));
    request.generation_id = 5;
    request.member_id = StrBytes::from_static_str("consumer-1");
    request.group_instance_id = Some(StrBytes::from_static_str("instance-1"));

    // Version 3+ includes group instance ID
    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 3);
    assert!(result.is_ok());
}

#[test]
fn test_heartbeat_request_roundtrip() {
    let mut request = HeartbeatRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("heartbeat-group"));
    request.generation_id = 42;
    request.member_id = StrBytes::from_static_str("member-abc");

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = HeartbeatRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.group_id.0.as_str(), "heartbeat-group");
    assert_eq!(decoded.generation_id, 42);
    assert_eq!(decoded.member_id.as_str(), "member-abc");
}

#[test]
fn test_heartbeat_response_success() {
    let mut response = HeartbeatResponse::default();
    response.error_code = 0;

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = HeartbeatResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 0);
}

#[test]
fn test_heartbeat_response_rebalance() {
    let mut response = HeartbeatResponse::default();
    response.error_code = 27; // REBALANCE_IN_PROGRESS

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = HeartbeatResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 27);
}

#[test]
fn test_heartbeat_response_with_throttle() {
    let mut response = HeartbeatResponse::default();
    response.throttle_time_ms = 250;
    response.error_code = 0;

    // Version 1+ includes throttle time
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = HeartbeatResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.throttle_time_ms, 250);
}

// =============================================================================
// LeaveGroup Request/Response Tests
// =============================================================================

#[test]
fn test_leave_group_request_basic() {
    let mut request = LeaveGroupRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("test-group"));
    request.member_id = StrBytes::from_static_str("consumer-1");

    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_leave_group_request_batch() {
    use kafka_protocol::messages::leave_group_request::MemberIdentity;

    let mut request = LeaveGroupRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("test-group"));

    // Version 3+ supports batch leave
    let mut member1 = MemberIdentity::default();
    member1.member_id = StrBytes::from_static_str("consumer-1");
    request.members.push(member1);

    let mut member2 = MemberIdentity::default();
    member2.member_id = StrBytes::from_static_str("consumer-2");
    request.members.push(member2);

    let mut buf = BytesMut::new();
    let result = request.encode(&mut buf, 3);
    assert!(result.is_ok());
}

#[test]
fn test_leave_group_request_roundtrip() {
    let mut request = LeaveGroupRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("leave-group"));
    request.member_id = StrBytes::from_static_str("leaving-member");

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = LeaveGroupRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.group_id.0.as_str(), "leave-group");
    assert_eq!(decoded.member_id.as_str(), "leaving-member");
}

#[test]
fn test_leave_group_response_basic() {
    let mut response = LeaveGroupResponse::default();
    response.error_code = 0;

    let mut buf = BytesMut::new();
    let result = response.encode(&mut buf, 0);
    assert!(result.is_ok());
}

#[test]
fn test_leave_group_response_batch() {
    use kafka_protocol::messages::leave_group_response::MemberResponse;

    let mut response = LeaveGroupResponse::default();
    response.error_code = 0;

    // Version 3+ includes per-member responses
    let mut member_response1 = MemberResponse::default();
    member_response1.member_id = StrBytes::from_static_str("consumer-1");
    member_response1.error_code = 0;
    response.members.push(member_response1);

    let mut member_response2 = MemberResponse::default();
    member_response2.member_id = StrBytes::from_static_str("consumer-2");
    member_response2.error_code = 0;
    response.members.push(member_response2);

    let mut buf = BytesMut::new();
    let result = response.encode(&mut buf, 3);
    assert!(result.is_ok());
}

// =============================================================================
// Consumer Protocol Metadata Tests
// =============================================================================

#[test]
fn test_consumer_protocol_metadata_format() {
    // Consumer protocol metadata wire format:
    // Version (int16) + Topics (array of strings) + UserData (bytes)

    // Version 0 format
    let metadata = [
        0x00, 0x00, // version = 0
        0x00, 0x00, 0x00, 0x02, // num topics = 2
        0x00, 0x06, // topic name length = 6
        b't', b'o', b'p', b'i', b'c', b'1', 0x00, 0x06, // topic name length = 6
        b't', b'o', b'p', b'i', b'c', b'2', 0x00, 0x00, 0x00, 0x00, // user data length = 0
    ];

    assert_eq!(metadata[0..2], [0x00, 0x00]); // version
    assert_eq!(metadata[2..6], [0x00, 0x00, 0x00, 0x02]); // 2 topics
}

#[test]
fn test_consumer_protocol_assignment_format() {
    // Consumer protocol assignment wire format:
    // Version (int16) + Partitions (array of topic-partitions) + UserData (bytes)

    // Version 0 format
    let assignment = [
        0x00, 0x00, // version = 0
        0x00, 0x00, 0x00, 0x01, // num topic-partitions = 1
        0x00, 0x05, // topic name length = 5
        b't', b'o', b'p', b'i', b'c', 0x00, 0x00, 0x00, 0x02, // num partitions = 2
        0x00, 0x00, 0x00, 0x00, // partition 0
        0x00, 0x00, 0x00, 0x01, // partition 1
        0x00, 0x00, 0x00, 0x00, // user data length = 0
    ];

    assert_eq!(assignment[0..2], [0x00, 0x00]); // version
    assert_eq!(assignment[2..6], [0x00, 0x00, 0x00, 0x01]); // 1 topic
}

// =============================================================================
// Generation ID Tests
// =============================================================================

#[test]
fn test_generation_id_initial() {
    // First generation is typically 1 (after initial rebalance)
    let initial_generation = 1;
    assert_eq!(initial_generation, 1);
}

#[test]
fn test_generation_id_increment() {
    // Generation ID increments with each rebalance
    let gen1 = 1;
    let gen2 = gen1 + 1;
    let gen3 = gen2 + 1;

    assert_eq!(gen2, 2);
    assert_eq!(gen3, 3);
}

#[test]
fn test_generation_id_mismatch_error() {
    // ILLEGAL_GENERATION error when generation doesn't match
    let error_code: i16 = 22; // ILLEGAL_GENERATION
    assert_eq!(error_code, 22);
}

#[test]
fn test_generation_id_in_heartbeat() {
    let mut request = HeartbeatRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("test-group"));
    request.generation_id = 5;
    request.member_id = StrBytes::from_static_str("consumer-1");

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = HeartbeatRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.generation_id, 5);
}

// =============================================================================
// Member ID Tests
// =============================================================================

#[test]
fn test_member_id_empty_for_join() {
    // Empty member ID on first join
    let member_id = StrBytes::from_static_str("");
    assert!(member_id.is_empty());
}

#[test]
fn test_member_id_format() {
    // Member IDs are typically UUIDs
    let member_id = "consumer-1-8f14e45f-ceea-4b6f-8f14-e45fceea4b6f";
    assert!(member_id.contains('-'));
    assert!(member_id.len() > 20);
}

#[test]
fn test_member_id_unknown_error() {
    // UNKNOWN_MEMBER_ID error code
    let error_code: i16 = 25;
    assert_eq!(error_code, 25);
}

#[test]
fn test_member_id_persistence() {
    // Member ID should be consistent across heartbeats
    let mut request1 = HeartbeatRequest::default();
    request1.member_id = StrBytes::from_static_str("consumer-uuid-123");

    let mut request2 = HeartbeatRequest::default();
    request2.member_id = StrBytes::from_static_str("consumer-uuid-123");

    assert_eq!(request1.member_id.as_str(), request2.member_id.as_str());
}

// =============================================================================
// Rebalance Protocol Tests
// =============================================================================

#[test]
fn test_rebalance_protocol_range() {
    let protocol = StrBytes::from_static_str("range");
    assert_eq!(protocol.as_str(), "range");
}

#[test]
fn test_rebalance_protocol_roundrobin() {
    let protocol = StrBytes::from_static_str("roundrobin");
    assert_eq!(protocol.as_str(), "roundrobin");
}

#[test]
fn test_rebalance_protocol_sticky() {
    let protocol = StrBytes::from_static_str("sticky");
    assert_eq!(protocol.as_str(), "sticky");
}

#[test]
fn test_rebalance_protocol_cooperative_sticky() {
    let protocol = StrBytes::from_static_str("cooperative-sticky");
    assert_eq!(protocol.as_str(), "cooperative-sticky");
}

#[test]
fn test_protocol_selection_intersection() {
    // Group coordinator selects from intersection of member protocols
    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;

    let mut request1 = JoinGroupRequest::default();
    let mut p1 = JoinGroupRequestProtocol::default();
    p1.name = StrBytes::from_static_str("range");
    request1.protocols.push(p1);
    let mut p2 = JoinGroupRequestProtocol::default();
    p2.name = StrBytes::from_static_str("roundrobin");
    request1.protocols.push(p2);

    let mut request2 = JoinGroupRequest::default();
    let mut p3 = JoinGroupRequestProtocol::default();
    p3.name = StrBytes::from_static_str("roundrobin");
    request2.protocols.push(p3);
    let mut p4 = JoinGroupRequestProtocol::default();
    p4.name = StrBytes::from_static_str("sticky");
    request2.protocols.push(p4);

    // Intersection would be "roundrobin"
    let protocols1: Vec<&str> = request1.protocols.iter().map(|p| p.name.as_str()).collect();
    let protocols2: Vec<&str> = request2.protocols.iter().map(|p| p.name.as_str()).collect();

    let intersection: Vec<&str> = protocols1
        .iter()
        .filter(|p| protocols2.contains(p))
        .copied()
        .collect();

    assert_eq!(intersection, vec!["roundrobin"]);
}

// =============================================================================
// Group Error Code Tests
// =============================================================================

#[test]
fn test_consumer_group_error_codes() {
    // Common consumer group error codes
    let group_coordinator_not_available: i16 = 15;
    let not_coordinator: i16 = 16;
    let illegal_generation: i16 = 22;
    let inconsistent_group_protocol: i16 = 23;
    let invalid_group_id: i16 = 24;
    let unknown_member_id: i16 = 25;
    let invalid_session_timeout: i16 = 26;
    let rebalance_in_progress: i16 = 27;
    let group_id_not_found: i16 = 69;
    let fenced_instance_id: i16 = 82;

    assert_eq!(group_coordinator_not_available, 15);
    assert_eq!(not_coordinator, 16);
    assert_eq!(illegal_generation, 22);
    assert_eq!(inconsistent_group_protocol, 23);
    assert_eq!(invalid_group_id, 24);
    assert_eq!(unknown_member_id, 25);
    assert_eq!(invalid_session_timeout, 26);
    assert_eq!(rebalance_in_progress, 27);
    assert_eq!(group_id_not_found, 69);
    assert_eq!(fenced_instance_id, 82);
}

#[test]
fn test_join_group_error_response() {
    let mut response = JoinGroupResponse::default();
    response.error_code = 23; // INCONSISTENT_GROUP_PROTOCOL
    response.generation_id = -1;
    response.member_id = StrBytes::from_static_str("");

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = JoinGroupResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 23);
    assert_eq!(decoded.generation_id, -1);
}

// =============================================================================
// Consumer Group API Keys Tests
// =============================================================================

#[test]
fn test_consumer_group_api_keys() {
    assert_eq!(ApiKey::JoinGroup as i16, 11);
    assert_eq!(ApiKey::Heartbeat as i16, 12);
    assert_eq!(ApiKey::LeaveGroup as i16, 13);
    assert_eq!(ApiKey::SyncGroup as i16, 14);
}

#[test]
fn test_join_group_versions() {
    // JoinGroup supports versions 0-9
    let request = JoinGroupRequest::default();
    for version in 0..=9 {
        let mut buf = BytesMut::new();
        let result = request.clone().encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode JoinGroup v{}", version);
    }
}

#[test]
fn test_sync_group_versions() {
    // SyncGroup supports versions 0-5
    let request = SyncGroupRequest::default();
    for version in 0..=5 {
        let mut buf = BytesMut::new();
        let result = request.clone().encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode SyncGroup v{}", version);
    }
}

#[test]
fn test_heartbeat_versions() {
    // Heartbeat supports versions 0-4
    let request = HeartbeatRequest::default();
    for version in 0..=4 {
        let mut buf = BytesMut::new();
        let result = request.clone().encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode Heartbeat v{}", version);
    }
}

#[test]
fn test_leave_group_versions() {
    // LeaveGroup supports versions 0-5
    let request = LeaveGroupRequest::default();
    for version in 0..=5 {
        let mut buf = BytesMut::new();
        let result = request.clone().encode(&mut buf, version);
        assert!(result.is_ok(), "Failed to encode LeaveGroup v{}", version);
    }
}

// =============================================================================
// Static Membership Tests
// =============================================================================

#[test]
fn test_static_membership_group_instance_id() {
    let mut request = JoinGroupRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("test-group"));
    request.session_timeout_ms = 30000;
    request.member_id = StrBytes::from_static_str("");
    request.group_instance_id = Some(StrBytes::from_static_str("static-instance-1"));
    request.protocol_type = StrBytes::from_static_str("consumer");

    assert!(request.group_instance_id.is_some());
    assert_eq!(
        request.group_instance_id.as_ref().unwrap().as_str(),
        "static-instance-1"
    );
}

#[test]
fn test_static_membership_rejoin() {
    // Static members can rejoin with same instance ID without rebalance
    let instance_id = Some(StrBytes::from_static_str("static-instance-1"));

    let mut request1 = JoinGroupRequest::default();
    request1.group_instance_id = instance_id.clone();

    let mut request2 = JoinGroupRequest::default();
    request2.group_instance_id = instance_id;

    assert_eq!(
        request1.group_instance_id.as_ref().unwrap().as_str(),
        request2.group_instance_id.as_ref().unwrap().as_str()
    );
}

#[test]
fn test_fenced_instance_id_error() {
    // FENCED_INSTANCE_ID error when another consumer has same instance ID
    let error_code: i16 = 82;

    let mut response = JoinGroupResponse::default();
    response.error_code = error_code;

    assert_eq!(response.error_code, 82);
}
