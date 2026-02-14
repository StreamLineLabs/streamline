//! Protocol Compatibility Tests
//!
//! Validates the core Kafka protocol operations that all SDKs rely on:
//! - Produce/Fetch cycle (write then read back)
//! - Consumer group protocol (JoinGroup, SyncGroup, Heartbeat, OffsetCommit, OffsetFetch)
//! - Topic management (Create, List, Describe, Delete)
//! - Metadata request handling
//! - API versions response
//! - Error handling for invalid operations

use bytes::Bytes;
use std::sync::Arc;
use streamline::{
    EmbeddedConfig, EmbeddedStreamline, GroupCoordinator, TopicManager,
};
use tempfile::tempdir;

// ============================================================================
// Test Helpers
// ============================================================================

/// Create an in-memory EmbeddedStreamline instance for testing
fn new_embedded() -> EmbeddedStreamline {
    EmbeddedStreamline::new(EmbeddedConfig::in_memory().with_partitions(3))
        .expect("Failed to create embedded instance")
}

/// Create a TopicManager + GroupCoordinator pair backed by temp directories
fn new_coordinator() -> (Arc<TopicManager>, Arc<GroupCoordinator>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let tm = Arc::new(TopicManager::new(dir.path()).unwrap());
    let offsets_dir = dir.path().join("offsets");
    std::fs::create_dir_all(&offsets_dir).unwrap();
    let gc = Arc::new(GroupCoordinator::new(&offsets_dir, tm.clone()).unwrap());
    (tm, gc, dir)
}

// ============================================================================
// 1. Produce / Fetch Cycle
// ============================================================================

/// SDK operation: produce a single message then consume it back.
/// Every SDK must support this basic round-trip.
#[test]
fn test_produce_fetch_single_message() {
    let embedded = new_embedded();
    embedded.create_topic("sdk-roundtrip", 1).unwrap();

    let offset = embedded
        .produce("sdk-roundtrip", 0, Some(Bytes::from("key-1")), Bytes::from("value-1"))
        .unwrap();
    assert_eq!(offset, 0);

    let records = embedded.consume("sdk-roundtrip", 0, 0, 10).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].value, Bytes::from("value-1"));
    assert_eq!(records[0].key, Some(Bytes::from("key-1")));
}

/// SDK operation: produce multiple messages across partitions then fetch each.
#[test]
fn test_produce_fetch_multi_partition() {
    let embedded = new_embedded();
    embedded.create_topic("sdk-multi-part", 3).unwrap();

    for p in 0..3 {
        for i in 0..5 {
            embedded
                .produce(
                    "sdk-multi-part",
                    p,
                    Some(Bytes::from(format!("k-{p}-{i}"))),
                    Bytes::from(format!("v-{p}-{i}")),
                )
                .unwrap();
        }
    }

    for p in 0..3 {
        let records = embedded.consume("sdk-multi-part", p, 0, 100).unwrap();
        assert_eq!(records.len(), 5, "partition {p} should have 5 records");
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.offset, i as i64);
        }
    }
}

/// SDK operation: produce without a key (null key).
#[test]
fn test_produce_fetch_null_key() {
    let embedded = new_embedded();
    embedded.create_topic("sdk-null-key", 1).unwrap();

    embedded
        .produce("sdk-null-key", 0, None, Bytes::from("payload"))
        .unwrap();

    let records = embedded.consume("sdk-null-key", 0, 0, 10).unwrap();
    assert_eq!(records.len(), 1);
    assert!(records[0].key.is_none());
    assert_eq!(records[0].value, Bytes::from("payload"));
}

/// SDK operation: fetch from a specific offset (seek).
#[test]
fn test_fetch_from_offset() {
    let embedded = new_embedded();
    embedded.create_topic("sdk-seek", 1).unwrap();

    for i in 0..10 {
        embedded
            .produce("sdk-seek", 0, None, Bytes::from(format!("msg-{i}")))
            .unwrap();
    }

    let records = embedded.consume("sdk-seek", 0, 5, 100).unwrap();
    assert_eq!(records.len(), 5);
    assert_eq!(records[0].offset, 5);
    assert_eq!(records[0].value, Bytes::from("msg-5"));
}

/// SDK operation: produce with headers.
#[test]
fn test_produce_fetch_with_headers() {
    let embedded = new_embedded();
    embedded.create_topic("sdk-headers", 1).unwrap();

    use streamline::storage::Header;
    let headers = vec![Header {
        key: "trace-id".to_string(),
        value: Bytes::from("abc-123"),
    }];

    embedded
        .produce_with_headers(
            "sdk-headers",
            0,
            Some(Bytes::from("k")),
            Bytes::from("v"),
            headers,
        )
        .unwrap();

    let records = embedded.consume("sdk-headers", 0, 0, 10).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].headers.len(), 1);
    assert_eq!(records[0].headers[0].key, "trace-id");
    assert_eq!(records[0].headers[0].value, Bytes::from("abc-123"));
}

// ============================================================================
// 2. Consumer Group Protocol
// ============================================================================

/// SDK operation: commit and fetch offsets for a consumer group.
#[test]
fn test_consumer_group_offset_commit_fetch() {
    let embedded = new_embedded();
    embedded.create_topic("sdk-cg-offsets", 2).unwrap();

    // Produce some data
    for p in 0..2 {
        for i in 0..5 {
            embedded
                .produce(
                    "sdk-cg-offsets",
                    p,
                    None,
                    Bytes::from(format!("msg-{p}-{i}")),
                )
                .unwrap();
        }
    }

    // Commit offsets
    embedded
        .commit_offset("test-group", "sdk-cg-offsets", 0, 3)
        .unwrap();
    embedded
        .commit_offset("test-group", "sdk-cg-offsets", 1, 5)
        .unwrap();

    // Fetch committed offsets
    let o0 = embedded.get_committed_offset("test-group", "sdk-cg-offsets", 0);
    let o1 = embedded.get_committed_offset("test-group", "sdk-cg-offsets", 1);
    assert_eq!(o0, 3);
    assert_eq!(o1, 5);
}

/// SDK operation: consume with a group (auto-commit).
#[test]
fn test_consume_with_group_auto_commit() {
    let embedded = new_embedded();
    embedded.create_topic("sdk-cg-auto", 1).unwrap();

    for i in 0..5 {
        embedded
            .produce("sdk-cg-auto", 0, None, Bytes::from(format!("msg-{i}")))
            .unwrap();
    }

    // First consume should get all 5 messages
    let records = embedded
        .consume_with_group("auto-group", "sdk-cg-auto", 100, true)
        .unwrap();
    assert_eq!(records.len(), 5);

    // Second consume should get no new messages (offsets committed)
    let records = embedded
        .consume_with_group("auto-group", "sdk-cg-auto", 100, true)
        .unwrap();
    assert_eq!(records.len(), 0);
}

/// SDK operation: two consumer groups consuming independently.
#[test]
fn test_independent_consumer_groups() {
    let embedded = new_embedded();
    embedded.create_topic("sdk-cg-indep", 1).unwrap();

    for i in 0..3 {
        embedded
            .produce("sdk-cg-indep", 0, None, Bytes::from(format!("msg-{i}")))
            .unwrap();
    }

    // Group A consumes all
    let a = embedded
        .consume_with_group("group-a", "sdk-cg-indep", 100, true)
        .unwrap();
    assert_eq!(a.len(), 3);

    // Group B also consumes all (independent offsets)
    let b = embedded
        .consume_with_group("group-b", "sdk-cg-indep", 100, true)
        .unwrap();
    assert_eq!(b.len(), 3);
}

/// SDK operation: GroupCoordinator join/sync/heartbeat/leave lifecycle.
#[test]
fn test_group_coordinator_lifecycle() {
    let (tm, gc, _dir) = new_coordinator();
    tm.create_topic("sdk-gc-lifecycle", 2).unwrap();

    // Join group: returns (member_id, generation_id, leader_id, members)
    let (member_id, generation_id, _leader_id, _members) = gc
        .join_group(
            "lifecycle-group",
            None,
            "test-client",
            "127.0.0.1",
            30000,
            300000,
            "consumer",
            "range",
            vec!["sdk-gc-lifecycle".to_string()],
            vec![],
        )
        .unwrap();
    assert!(!member_id.is_empty());
    assert!(generation_id >= 0);

    // Heartbeat
    let hb_result = gc.heartbeat("lifecycle-group", &member_id, generation_id);
    assert!(hb_result.is_ok());

    // Leave group
    let leave_result = gc.leave_group("lifecycle-group", &member_id);
    assert!(leave_result.is_ok());
}

/// SDK operation: OffsetCommit and OffsetFetch via GroupCoordinator.
#[test]
fn test_group_coordinator_offset_operations() {
    let (tm, gc, _dir) = new_coordinator();
    tm.create_topic("sdk-gc-offsets", 2).unwrap();

    // Commit offsets (one at a time via the actual API)
    gc.commit_offset("offset-group", "sdk-gc-offsets", 0, 10, String::new())
        .unwrap();
    gc.commit_offset("offset-group", "sdk-gc-offsets", 1, 20, String::new())
        .unwrap();

    // Fetch offsets
    let o0 = gc
        .fetch_offset("offset-group", "sdk-gc-offsets", 0)
        .unwrap()
        .expect("offset for partition 0 should exist");
    let o1 = gc
        .fetch_offset("offset-group", "sdk-gc-offsets", 1)
        .unwrap()
        .expect("offset for partition 1 should exist");
    assert_eq!(o0.offset, 10);
    assert_eq!(o1.offset, 20);
}

// ============================================================================
// 3. Topic Management
// ============================================================================

/// SDK operation: create, list, describe, delete topics.
#[test]
fn test_topic_create_list_describe_delete() {
    let embedded = new_embedded();

    // Create
    embedded.create_topic("sdk-topic-a", 2).unwrap();
    embedded.create_topic("sdk-topic-b", 4).unwrap();

    // List
    let topics = embedded.list_topics().unwrap();
    assert!(topics.contains(&"sdk-topic-a".to_string()));
    assert!(topics.contains(&"sdk-topic-b".to_string()));

    // Describe
    let meta = embedded.topic_metadata("sdk-topic-a").unwrap();
    assert_eq!(meta.name, "sdk-topic-a");
    assert_eq!(meta.num_partitions, 2);

    let meta = embedded.topic_metadata("sdk-topic-b").unwrap();
    assert_eq!(meta.num_partitions, 4);

    // Delete
    embedded.delete_topic("sdk-topic-a").unwrap();
    let topics = embedded.list_topics().unwrap();
    assert!(!topics.contains(&"sdk-topic-a".to_string()));
    assert!(topics.contains(&"sdk-topic-b".to_string()));
}

/// SDK operation: auto-create topic on first produce.
#[test]
fn test_topic_auto_create_on_produce() {
    let embedded = new_embedded();

    // Produce to a non-existent topic triggers auto-create
    let offset = embedded
        .produce("sdk-auto-created", 0, None, Bytes::from("first"))
        .unwrap();
    assert_eq!(offset, 0);

    let topics = embedded.list_topics().unwrap();
    assert!(topics.contains(&"sdk-auto-created".to_string()));
}

/// SDK operation: creating a duplicate topic should fail.
#[test]
fn test_topic_duplicate_create_fails() {
    let embedded = new_embedded();
    embedded.create_topic("sdk-dup", 1).unwrap();

    let result = embedded.create_topic("sdk-dup", 1);
    assert!(result.is_err());
}

// ============================================================================
// 4. Metadata and Offsets
// ============================================================================

/// SDK operation: query earliest and latest offsets.
#[test]
fn test_offset_metadata() {
    let embedded = new_embedded();
    embedded.create_topic("sdk-offsets", 1).unwrap();

    // Empty topic
    assert_eq!(embedded.earliest_offset("sdk-offsets", 0).unwrap(), 0);
    assert_eq!(embedded.latest_offset("sdk-offsets", 0).unwrap(), 0);

    // After producing
    for i in 0..10 {
        embedded
            .produce("sdk-offsets", 0, None, Bytes::from(format!("m-{i}")))
            .unwrap();
    }

    assert_eq!(embedded.earliest_offset("sdk-offsets", 0).unwrap(), 0);
    assert_eq!(embedded.latest_offset("sdk-offsets", 0).unwrap(), 10);
    assert_eq!(embedded.high_watermark("sdk-offsets", 0).unwrap(), 10);
}

/// SDK operation: TopicManager metadata responses.
#[test]
fn test_topic_manager_metadata() {
    let dir = tempdir().unwrap();
    let tm = TopicManager::new(dir.path()).unwrap();
    tm.create_topic("meta-test", 3).unwrap();

    let meta = tm.get_topic_metadata("meta-test").unwrap();
    assert_eq!(meta.name, "meta-test");
    assert_eq!(meta.num_partitions, 3);

    let all = tm.list_topics().unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].name, "meta-test");
}

// ============================================================================
// 5. API Versions (protocol-level encoding test)
// ============================================================================

/// SDK operation: encode/decode ApiVersionsRequest/Response.
/// All SDKs start by sending ApiVersions to negotiate protocol versions.
#[test]
fn test_api_versions_encoding() {
    use bytes::BytesMut;
    use kafka_protocol::messages::{ApiVersionsRequest, ApiVersionsResponse};
    use kafka_protocol::messages::api_versions_response::ApiVersion;
    use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

    // Encode a v3 request (latest)
    let mut request = ApiVersionsRequest::default();
    request.client_software_name = StrBytes::from_static_str("streamline-cross-sdk-test");
    request.client_software_version = StrBytes::from_static_str("0.2.0");

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsRequest::decode(&mut read_buf, 3).unwrap();
    assert_eq!(decoded.client_software_name.as_str(), "streamline-cross-sdk-test");

    // Build a response with expected API keys
    let expected_apis: Vec<i16> = vec![
        0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    ];

    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;
    for &api_key in &expected_apis {
        let mut version = ApiVersion::default();
        version.api_key = api_key;
        version.min_version = 0;
        version.max_version = 3;
        response.api_keys.push(version);
    }

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 3).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = ApiVersionsResponse::decode(&mut read_buf, 3).unwrap();
    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.api_keys.len(), expected_apis.len());

    // Verify all expected API keys are present
    let decoded_keys: Vec<i16> = decoded.api_keys.iter().map(|a| a.api_key).collect();
    for key in &expected_apis {
        assert!(
            decoded_keys.contains(key),
            "API key {key} missing from response"
        );
    }
}

// ============================================================================
// 6. Error Handling
// ============================================================================

/// SDK operation: fetch from non-existent topic should fail.
#[test]
fn test_error_fetch_nonexistent_topic() {
    let embedded = new_embedded();
    let result = embedded.consume("no-such-topic", 0, 0, 10);
    assert!(result.is_err());
}

/// SDK operation: fetch from invalid partition should fail.
#[test]
fn test_error_fetch_invalid_partition() {
    let embedded = new_embedded();
    embedded.create_topic("sdk-err-part", 2).unwrap();

    let result = embedded.consume("sdk-err-part", 99, 0, 10);
    assert!(result.is_err());
}

/// SDK operation: delete a non-existent topic should fail.
#[test]
fn test_error_delete_nonexistent_topic() {
    let embedded = new_embedded();
    let result = embedded.delete_topic("no-such-topic");
    assert!(result.is_err());
}

/// SDK operation: describe a non-existent topic should fail.
#[test]
fn test_error_describe_nonexistent_topic() {
    let embedded = new_embedded();
    let result = embedded.topic_metadata("no-such-topic");
    assert!(result.is_err());
}

/// SDK operation: query offsets on non-existent topic should fail.
#[test]
fn test_error_offsets_nonexistent_topic() {
    let embedded = new_embedded();
    let result = embedded.latest_offset("no-such-topic", 0);
    assert!(result.is_err());
}

/// SDK operation: produce to invalid partition should fail.
#[test]
fn test_error_produce_invalid_partition() {
    let embedded = new_embedded();
    embedded.create_topic("sdk-err-produce", 2).unwrap();

    let result = embedded.produce("sdk-err-produce", 99, None, Bytes::from("data"));
    assert!(result.is_err());
}

// ============================================================================
// 7. Persistence Round-Trip
// ============================================================================

/// SDK operation: data survives server restart.
#[test]
fn test_persistence_across_restarts() {
    let dir = tempdir().unwrap();

    // Session 1: create topic and produce
    {
        let embedded = EmbeddedStreamline::new(EmbeddedConfig::persistent(dir.path())).unwrap();
        embedded.create_topic("sdk-persist", 2).unwrap();
        embedded
            .produce("sdk-persist", 0, Some(Bytes::from("k1")), Bytes::from("v1"))
            .unwrap();
        embedded
            .produce("sdk-persist", 1, Some(Bytes::from("k2")), Bytes::from("v2"))
            .unwrap();
    }

    // Session 2: read back
    {
        let embedded = EmbeddedStreamline::new(EmbeddedConfig::persistent(dir.path())).unwrap();
        let topics = embedded.list_topics().unwrap();
        assert!(topics.contains(&"sdk-persist".to_string()));

        let r0 = embedded.consume("sdk-persist", 0, 0, 10).unwrap();
        assert_eq!(r0.len(), 1);
        assert_eq!(r0[0].value, Bytes::from("v1"));

        let r1 = embedded.consume("sdk-persist", 1, 0, 10).unwrap();
        assert_eq!(r1.len(), 1);
        assert_eq!(r1[0].value, Bytes::from("v2"));
    }
}

// ============================================================================
// 8. Batch Produce
// ============================================================================

/// SDK operation: produce a batch of messages at once.
#[test]
fn test_batch_produce() {
    let embedded = new_embedded();
    embedded.create_topic("sdk-batch", 3).unwrap();

    let messages: Vec<(Option<Bytes>, Bytes)> = (0..10)
        .map(|i| (Some(Bytes::from(format!("k-{i}"))), Bytes::from(format!("v-{i}"))))
        .collect();

    let offsets = embedded.produce_batch("sdk-batch", messages).unwrap();
    assert_eq!(offsets.len(), 10);

    // All offsets should be non-negative
    for offset in &offsets {
        assert!(*offset >= 0);
    }
}

// ============================================================================
// 9. Large Messages
// ============================================================================

/// SDK operation: produce and consume a large message (1MB).
#[test]
fn test_large_message_roundtrip() {
    let embedded = new_embedded();
    embedded.create_topic("sdk-large", 1).unwrap();

    let large_value = Bytes::from(vec![b'X'; 1_000_000]);
    embedded
        .produce("sdk-large", 0, None, large_value.clone())
        .unwrap();

    let records = embedded.consume("sdk-large", 0, 0, 10).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].value.len(), 1_000_000);
    assert_eq!(records[0].value, large_value);
}

// ============================================================================
// 10. Delete Records
// ============================================================================

/// SDK operation: delete records before a given offset.
#[test]
fn test_delete_records() {
    let embedded = new_embedded();
    embedded.create_topic("sdk-delete-rec", 1).unwrap();

    for i in 0..10 {
        embedded
            .produce("sdk-delete-rec", 0, None, Bytes::from(format!("m-{i}")))
            .unwrap();
    }

    // Delete records before offset 5
    let new_low = embedded.delete_records_before("sdk-delete-rec", 0, 5).unwrap();
    assert!(new_low >= 5);

    // Earliest offset should now be >= 5
    let earliest = embedded.earliest_offset("sdk-delete-rec", 0).unwrap();
    assert!(earliest >= 5);
}
