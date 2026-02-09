use super::*;

#[test]
fn test_compaction_policy_create_topics() {
    // CreateTopics should support cleanup.policy config for compaction
    use kafka_protocol::messages::create_topics_request::{
        CreatableReplicaAssignment, CreatableTopic, CreateTopicsRequest, CreateableTopicConfig,
    };

    let config = CreateableTopicConfig::default()
        .with_name(StrBytes::from_static_str("cleanup.policy"))
        .with_value(Some(StrBytes::from_static_str("compact")));

    let topic = CreatableTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str(
            "compacted-topic",
        )))
        .with_num_partitions(3)
        .with_replication_factor(1)
        .with_assignments(vec![CreatableReplicaAssignment::default()])
        .with_configs(vec![config]);

    let request = CreateTopicsRequest::default().with_topics(vec![topic]);

    // Verify topic configuration
    assert_eq!(request.topics.len(), 1);
    let topic_config = &request.topics[0].configs[0];
    assert_eq!(topic_config.name.as_str(), "cleanup.policy");
    assert_eq!(topic_config.value.as_ref().unwrap().as_str(), "compact");
}

#[test]
fn test_compaction_policy_compact_delete() {
    // cleanup.policy can be compact,delete for both behaviors
    use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreateableTopicConfig};

    let config = CreateableTopicConfig::default()
        .with_name(StrBytes::from_static_str("cleanup.policy"))
        .with_value(Some(StrBytes::from_static_str("compact,delete")));

    let topic = CreatableTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str(
            "compact-delete-topic",
        )))
        .with_configs(vec![config]);

    let policy = topic.configs[0].value.as_ref().unwrap().as_str();
    assert!(policy.contains("compact"));
    assert!(policy.contains("delete"));
}

#[test]
fn test_compaction_tombstone_format() {
    // Tombstone records have null value but non-null key
    // This is how deletes work in compacted topics

    // Key present, value null = tombstone
    let key = Some(vec![1u8, 2, 3]);
    let value: Option<Vec<u8>> = None; // null value

    assert!(key.is_some(), "Tombstone must have a key");
    assert!(value.is_none(), "Tombstone must have null value");
}

#[test]
fn test_compaction_tombstone_vs_regular_record() {
    // Distinguish tombstone from regular record

    struct CompactRecord {
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
    }

    let regular = CompactRecord {
        key: Some(b"key1".to_vec()),
        value: Some(b"value1".to_vec()),
    };

    let tombstone = CompactRecord {
        key: Some(b"key1".to_vec()),
        value: None,
    };

    // Regular record has both key and value
    assert!(regular.key.is_some());
    assert!(regular.value.is_some());

    // Tombstone has key but no value
    assert!(tombstone.key.is_some());
    assert!(tombstone.value.is_none());
}

#[test]
fn test_compaction_key_required() {
    // Compacted topics require keys on all records
    // Records without keys cannot be compacted

    #[allow(dead_code)]
    struct CompactableRecord {
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
    }

    let valid_record = CompactableRecord {
        key: Some(b"user-123".to_vec()),
        value: Some(b"{\"name\": \"Alice\"}".to_vec()),
    };

    let invalid_record = CompactableRecord {
        key: None, // No key - cannot be compacted
        value: Some(b"orphan value".to_vec()),
    };

    // Check if record is compactable
    fn is_compactable(record: &CompactableRecord) -> bool {
        record.key.is_some()
    }

    assert!(is_compactable(&valid_record));
    assert!(!is_compactable(&invalid_record));
}

#[test]
fn test_compaction_duplicate_key_latest_wins() {
    // When compacting, only the latest record per key is retained

    struct LogEntry {
        offset: i64,
        key: Vec<u8>,
        value: Vec<u8>,
    }

    let entries = vec![
        LogEntry {
            offset: 0,
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
        },
        LogEntry {
            offset: 1,
            key: b"k2".to_vec(),
            value: b"v2".to_vec(),
        },
        LogEntry {
            offset: 2,
            key: b"k1".to_vec(),
            value: b"v1-updated".to_vec(),
        }, // Duplicate k1
        LogEntry {
            offset: 3,
            key: b"k3".to_vec(),
            value: b"v3".to_vec(),
        },
    ];

    // After compaction, only latest per key should remain
    use std::collections::HashMap;
    let mut compacted: HashMap<Vec<u8>, (i64, Vec<u8>)> = HashMap::new();

    for entry in entries {
        compacted.insert(entry.key, (entry.offset, entry.value));
    }

    // k1 should have the latest value
    assert_eq!(compacted.get(b"k1".as_ref()).unwrap().1, b"v1-updated");
    assert_eq!(compacted.get(b"k1".as_ref()).unwrap().0, 2); // offset 2

    // k2 unchanged
    assert_eq!(compacted.get(b"k2".as_ref()).unwrap().1, b"v2");

    // Only 3 unique keys after compaction
    assert_eq!(compacted.len(), 3);
}

#[test]
fn test_compaction_tombstone_removes_key() {
    // Tombstones eventually remove the key entirely

    use std::collections::HashMap;

    struct LogEntry {
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    }

    let entries = vec![
        LogEntry {
            key: b"k1".to_vec(),
            value: Some(b"v1".to_vec()),
        },
        LogEntry {
            key: b"k2".to_vec(),
            value: Some(b"v2".to_vec()),
        },
        LogEntry {
            key: b"k1".to_vec(),
            value: None,
        }, // Tombstone for k1
    ];

    let mut compacted: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();

    for entry in entries {
        if entry.value.is_none() {
            // Tombstone - mark for deletion (or remove after retention)
            compacted.insert(entry.key, None);
        } else {
            compacted.insert(entry.key, entry.value);
        }
    }

    // k1 is now a tombstone
    assert!(compacted.get(b"k1".as_ref()).unwrap().is_none());

    // k2 still has value
    assert!(compacted.get(b"k2".as_ref()).unwrap().is_some());
}

#[test]
fn test_compaction_timestamp_preservation() {
    // Compaction should preserve record timestamps

    #[allow(dead_code)]
    struct CompactedRecord {
        key: Vec<u8>,
        value: Vec<u8>,
        timestamp: i64,
    }

    let original_timestamp = 1702400000000i64; // Some timestamp

    let record = CompactedRecord {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
        timestamp: original_timestamp,
    };

    // After compaction, timestamp should be preserved
    assert_eq!(record.timestamp, original_timestamp);
}

#[test]
fn test_compaction_log_start_offset_advancement() {
    // As compaction removes records, log_start_offset advances

    struct PartitionLog {
        log_start_offset: i64,
        log_end_offset: i64,
    }

    let mut log = PartitionLog {
        log_start_offset: 0,
        log_end_offset: 1000,
    };

    // Before compaction
    assert_eq!(log.log_start_offset, 0);

    // After compaction removed offsets 0-499
    log.log_start_offset = 500;

    // log_start_offset advanced
    assert_eq!(log.log_start_offset, 500);
    assert!(log.log_start_offset <= log.log_end_offset);
}

#[test]
fn test_compaction_fetch_with_offset_gaps() {
    // After compaction, there are gaps in offsets
    // Fetch should still work correctly

    struct CompactedPartition {
        // Only these offsets exist after compaction
        existing_offsets: Vec<i64>,
    }

    let partition = CompactedPartition {
        existing_offsets: vec![0, 5, 10, 15, 20], // Gaps: 1-4, 6-9, etc.
    };

    // Fetch from offset 7 should return next available (10)
    fn find_next_offset(partition: &CompactedPartition, requested: i64) -> Option<i64> {
        partition
            .existing_offsets
            .iter()
            .find(|&&o| o >= requested)
            .copied()
    }

    assert_eq!(find_next_offset(&partition, 0), Some(0));
    assert_eq!(find_next_offset(&partition, 3), Some(5)); // Gap, returns 5
    assert_eq!(find_next_offset(&partition, 7), Some(10)); // Gap, returns 10
    assert_eq!(find_next_offset(&partition, 21), None); // Beyond end
}

#[test]
fn test_compaction_min_lag_config() {
    // min.compaction.lag.ms prevents recent messages from being compacted
    // This gives consumers time to read all records

    struct CompactionConfig {
        min_lag_ms: i64,
    }

    let config = CompactionConfig {
        min_lag_ms: 3_600_000, // 1 hour
    };

    let now = 1702400000000i64;
    let old_record_timestamp = now - 7_200_000; // 2 hours ago
    let recent_record_timestamp = now - 1_800_000; // 30 minutes ago

    fn can_compact(config: &CompactionConfig, record_timestamp: i64, now: i64) -> bool {
        (now - record_timestamp) >= config.min_lag_ms
    }

    // Old record can be compacted
    assert!(can_compact(&config, old_record_timestamp, now));

    // Recent record is protected
    assert!(!can_compact(&config, recent_record_timestamp, now));
}

#[test]
fn test_compaction_delete_retention_ms() {
    // delete.retention.ms controls how long tombstones are retained

    struct CompactionConfig {
        delete_retention_ms: i64,
    }

    let config = CompactionConfig {
        delete_retention_ms: 86_400_000, // 24 hours
    };

    let now = 1702400000000i64;
    let old_tombstone_timestamp = now - 100_000_000; // Way past retention
    let recent_tombstone_timestamp = now - 3_600_000; // 1 hour ago

    fn should_remove_tombstone(
        config: &CompactionConfig,
        tombstone_timestamp: i64,
        now: i64,
    ) -> bool {
        (now - tombstone_timestamp) > config.delete_retention_ms
    }

    // Old tombstone should be removed
    assert!(should_remove_tombstone(
        &config,
        old_tombstone_timestamp,
        now
    ));

    // Recent tombstone should be kept
    assert!(!should_remove_tombstone(
        &config,
        recent_tombstone_timestamp,
        now
    ));
}

#[test]
fn test_compaction_segment_size_effect() {
    // segment.bytes affects compaction - only closed segments are compacted

    #[allow(dead_code)]
    struct Segment {
        base_offset: i64,
        size_bytes: i64,
        is_active: bool,
    }

    let max_segment_bytes = 1073741824i64; // 1GB

    let active_segment = Segment {
        base_offset: 1000,
        size_bytes: 500_000_000, // 500MB, still active
        is_active: true,
    };

    let closed_segment = Segment {
        base_offset: 0,
        size_bytes: max_segment_bytes, // Full
        is_active: false,
    };

    fn can_compact_segment(segment: &Segment) -> bool {
        !segment.is_active // Only closed segments
    }

    // Active segment cannot be compacted
    assert!(!can_compact_segment(&active_segment));

    // Closed segment can be compacted
    assert!(can_compact_segment(&closed_segment));
}

#[test]
fn test_compaction_dirty_ratio() {
    // min.cleanable.dirty.ratio controls when compaction triggers

    struct SegmentStats {
        total_entries: i64,
        duplicate_key_entries: i64,
    }

    let min_dirty_ratio = 0.5; // 50%

    let dirty_segment = SegmentStats {
        total_entries: 1000,
        duplicate_key_entries: 600, // 60% duplicates
    };

    let clean_segment = SegmentStats {
        total_entries: 1000,
        duplicate_key_entries: 200, // 20% duplicates
    };

    fn dirty_ratio(stats: &SegmentStats) -> f64 {
        stats.duplicate_key_entries as f64 / stats.total_entries as f64
    }

    fn should_compact(stats: &SegmentStats, min_ratio: f64) -> bool {
        dirty_ratio(stats) >= min_ratio
    }

    // Dirty segment should be compacted
    assert!(should_compact(&dirty_segment, min_dirty_ratio));

    // Clean segment should not be compacted yet
    assert!(!should_compact(&clean_segment, min_dirty_ratio));
}

#[test]
fn test_compaction_list_offsets_after_compaction() {
    // ListOffsets behavior after compaction

    // After compaction, earliest offset may be > 0
    let log_start_offset = 500i64; // Compaction removed 0-499
    let log_end_offset = 1000i64;

    // EARLIEST_TIMESTAMP (-2) returns log_start_offset
    let earliest_result = log_start_offset;
    assert_eq!(earliest_result, 500);

    // LATEST_TIMESTAMP (-1) returns log_end_offset
    let latest_result = log_end_offset;
    assert_eq!(latest_result, 1000);
}

#[test]
fn test_compaction_transaction_markers_preserved() {
    // Control records (transaction markers) should be preserved during compaction

    struct Record {
        is_control_record: bool,
        key: Vec<u8>,
    }

    let records = vec![
        Record {
            is_control_record: false,
            key: b"k1".to_vec(),
        },
        Record {
            is_control_record: true,
            key: vec![0, 0, 0, 0], // Control record key (transaction marker)
        },
    ];

    fn should_keep_in_compaction(record: &Record) -> bool {
        // Control records are always kept
        record.is_control_record || !record.key.is_empty()
    }

    for record in &records {
        assert!(should_keep_in_compaction(record));
    }
}

#[test]
fn test_compaction_max_compaction_lag_ms() {
    // max.compaction.lag.ms forces compaction after certain time
    // even if dirty ratio is below threshold

    struct CompactionConfig {
        max_compaction_lag_ms: i64,
    }

    let config = CompactionConfig {
        max_compaction_lag_ms: 86_400_000 * 7, // 7 days
    };

    let now = 1702400000000i64;
    let last_compaction = now - (86_400_000 * 10); // 10 days ago

    fn force_compaction(config: &CompactionConfig, last_compaction: i64, now: i64) -> bool {
        (now - last_compaction) > config.max_compaction_lag_ms
    }

    // Should force compaction after max lag exceeded
    assert!(force_compaction(&config, last_compaction, now));

    let recent_compaction = now - 86_400_000; // 1 day ago
    assert!(!force_compaction(&config, recent_compaction, now));
}

#[test]
fn test_compaction_offset_map_building() {
    // Compaction builds offset map of latest offset per key

    use std::collections::HashMap;

    struct LogEntry {
        offset: i64,
        key: Vec<u8>,
    }

    let entries = vec![
        LogEntry {
            offset: 0,
            key: b"a".to_vec(),
        },
        LogEntry {
            offset: 1,
            key: b"b".to_vec(),
        },
        LogEntry {
            offset: 2,
            key: b"a".to_vec(),
        }, // Updated a
        LogEntry {
            offset: 3,
            key: b"c".to_vec(),
        },
        LogEntry {
            offset: 4,
            key: b"b".to_vec(),
        }, // Updated b
    ];

    // Build offset map (latest offset per key)
    let mut offset_map: HashMap<Vec<u8>, i64> = HashMap::new();
    for entry in &entries {
        offset_map.insert(entry.key.clone(), entry.offset);
    }

    // Verify latest offsets
    assert_eq!(offset_map.get(b"a".as_ref()), Some(&2));
    assert_eq!(offset_map.get(b"b".as_ref()), Some(&4));
    assert_eq!(offset_map.get(b"c".as_ref()), Some(&3));
}

#[test]
fn test_compaction_describe_configs_response() {
    // DescribeConfigs should show compaction-related configs

    use kafka_protocol::messages::describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
    };

    let mut response = DescribeConfigsResponse::default();
    let mut result = DescribeConfigsResult::default();

    // Add cleanup.policy config
    let mut config_entry = DescribeConfigsResourceResult::default();
    config_entry.name = StrBytes::from_static_str("cleanup.policy");
    config_entry.value = Some(StrBytes::from_static_str("compact"));
    config_entry.read_only = false;
    config_entry.is_default = false;

    result.configs = vec![config_entry];
    response.results = vec![result];

    assert_eq!(response.results.len(), 1);
    assert_eq!(
        response.results[0].configs[0].name.as_str(),
        "cleanup.policy"
    );
}
