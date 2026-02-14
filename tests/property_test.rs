//! Property-based tests for Streamline storage layer
//!
//! Uses proptest to generate random inputs and verify invariants hold
//! across a wide range of scenarios that unit tests might miss.

use bytes::Bytes;
use proptest::prelude::*;
use std::collections::HashSet;
use streamline::storage::{
    record::{Record, RecordBatch},
    segment::Segment,
};
use tempfile::tempdir;

/// Strategy to generate arbitrary byte vectors for record values
fn arbitrary_bytes(max_size: usize) -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..max_size)
}

/// Strategy to generate valid record keys (optional bytes)
fn arbitrary_key() -> impl Strategy<Value = Option<Bytes>> {
    prop::option::of(arbitrary_bytes(256).prop_map(Bytes::from))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Property: Records written to a segment can be read back with identical content
    #[test]
    fn segment_roundtrip_preserves_data(
        payloads in prop::collection::vec(arbitrary_bytes(10_000), 1..50)
    ) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.segment");

        let mut segment = Segment::create(&path, 0).unwrap();

        // Write records
        for (i, payload) in payloads.iter().enumerate() {
            let record = Record::new(
                i as i64,
                1000 + i as i64,
                None,
                Bytes::from(payload.clone()),
            );
            segment.append_record(record).unwrap();
        }

        // Read back and verify
        let records = segment.read_all().unwrap();
        prop_assert_eq!(records.len(), payloads.len());

        for (i, (record, original)) in records.iter().zip(payloads.iter()).enumerate() {
            prop_assert_eq!(record.offset, i as i64, "Offset mismatch at index {}", i);
            prop_assert_eq!(
                record.value.as_ref(),
                original.as_slice(),
                "Payload mismatch at index {}",
                i
            );
        }
    }

    /// Property: Offsets are always monotonically increasing
    #[test]
    fn offsets_are_monotonically_increasing(
        batch_sizes in prop::collection::vec(1..20usize, 1..10)
    ) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.segment");

        let mut segment = Segment::create(&path, 0).unwrap();
        let mut current_offset: i64 = 0;

        // Write multiple batches with varying sizes
        for batch_size in batch_sizes.iter() {
            let mut batch = RecordBatch::new(current_offset, 1000);
            for _ in 0..*batch_size {
                let record = Record::new(
                    current_offset,
                    1000,
                    None,
                    Bytes::from("test"),
                );
                batch.add_record(record);
                current_offset += 1;
            }
            segment.append_batch(&batch).unwrap();
        }

        // Verify offsets are monotonically increasing
        let records = segment.read_all().unwrap();
        let mut prev_offset: Option<i64> = None;

        for record in records {
            if let Some(prev) = prev_offset {
                prop_assert!(
                    record.offset > prev,
                    "Offset {} should be greater than previous {}",
                    record.offset,
                    prev
                );
            }
            prev_offset = Some(record.offset);
        }
    }

    /// Property: All offsets in a segment are unique
    #[test]
    fn offsets_are_unique(
        record_count in 1..100usize
    ) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.segment");

        let mut segment = Segment::create(&path, 0).unwrap();

        for i in 0..record_count {
            let record = Record::new(
                i as i64,
                1000 + i as i64,
                None,
                Bytes::from(format!("value-{}", i)),
            );
            segment.append_record(record).unwrap();
        }

        let records = segment.read_all().unwrap();
        let offsets: HashSet<i64> = records.iter().map(|r| r.offset).collect();

        prop_assert_eq!(
            offsets.len(),
            records.len(),
            "All offsets should be unique"
        );
    }

    /// Property: Record size calculation is consistent
    #[test]
    fn record_size_is_consistent(
        key in arbitrary_key(),
        value in arbitrary_bytes(10_000),
    ) {
        let record = Record::new(
            0,
            1000,
            key.clone(),
            Bytes::from(value.clone()),
        );

        let size = record.size();

        // Expected: 8 (offset) + 8 (timestamp) + key_len + value_len
        let expected_key_size = key.as_ref().map(|k| k.len()).unwrap_or(0);
        let expected = 8 + 8 + expected_key_size + value.len();

        prop_assert_eq!(size, expected, "Record size calculation should match");
    }

    /// Property: RecordBatch size is sum of records plus metadata
    #[test]
    fn record_batch_size_is_sum_of_records(
        values in prop::collection::vec(arbitrary_bytes(1000), 1..20)
    ) {
        let mut batch = RecordBatch::new(0, 1000);

        for (i, value) in values.iter().enumerate() {
            let record = Record::new(i as i64, 1000, None, Bytes::from(value.clone()));
            batch.add_record(record);
        }

        let batch_size = batch.size();
        let records_size: usize = batch.records.iter().map(|r| r.size()).sum();

        // Batch overhead: 8 (base_offset) + 8 (timestamp) = 16
        prop_assert_eq!(
            batch_size,
            16 + records_size,
            "Batch size should be metadata + sum of record sizes"
        );
    }

    /// Property: Empty payloads are handled correctly
    #[test]
    fn empty_payloads_roundtrip(
        record_count in 1..50usize
    ) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.segment");

        let mut segment = Segment::create(&path, 0).unwrap();

        for i in 0..record_count {
            let record = Record::new(
                i as i64,
                1000,
                None,
                Bytes::new(), // Empty payload
            );
            segment.append_record(record).unwrap();
        }

        let records = segment.read_all().unwrap();
        prop_assert_eq!(records.len(), record_count);

        for record in records {
            prop_assert!(record.value.is_empty(), "All payloads should be empty");
        }
    }

    /// Property: Large payloads are handled correctly
    #[test]
    fn large_payloads_roundtrip(
        payload in arbitrary_bytes(100_000) // Up to 100KB
    ) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.segment");

        let mut segment = Segment::create(&path, 0).unwrap();

        let record = Record::new(0, 1000, None, Bytes::from(payload.clone()));
        segment.append_record(record).unwrap();

        let records = segment.read_all().unwrap();
        prop_assert_eq!(records.len(), 1);
        prop_assert_eq!(records[0].value.as_ref(), payload.as_slice());
    }

    /// Property: Keys are preserved correctly
    #[test]
    fn keys_are_preserved(
        key in arbitrary_key(),
        value in arbitrary_bytes(1000)
    ) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.segment");

        let mut segment = Segment::create(&path, 0).unwrap();

        let record = Record::new(0, 1000, key.clone(), Bytes::from(value));
        segment.append_record(record).unwrap();

        let records = segment.read_all().unwrap();
        prop_assert_eq!(records.len(), 1);
        prop_assert_eq!(records[0].key.clone(), key);
    }

    /// Property: Timestamps are preserved correctly
    #[test]
    fn timestamps_are_preserved(
        timestamps in prop::collection::vec(0i64..i64::MAX, 1..50)
    ) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.segment");

        let mut segment = Segment::create(&path, 0).unwrap();

        for (i, ts) in timestamps.iter().enumerate() {
            let record = Record::new(i as i64, *ts, None, Bytes::from("test"));
            segment.append_record(record).unwrap();
        }

        let records = segment.read_all().unwrap();
        prop_assert_eq!(records.len(), timestamps.len());

        for (record, expected_ts) in records.iter().zip(timestamps.iter()) {
            prop_assert_eq!(
                record.timestamp, *expected_ts,
                "Timestamp should be preserved"
            );
        }
    }

    /// Property: read_from_offset returns correct subset
    #[test]
    fn read_from_offset_returns_correct_subset(
        record_count in 10..100usize,
        start_offset in 0..10i64
    ) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.segment");

        let mut segment = Segment::create(&path, 0).unwrap();

        for i in 0..record_count {
            let record = Record::new(
                i as i64,
                1000 + i as i64,
                None,
                Bytes::from(format!("value-{}", i)),
            );
            segment.append_record(record).unwrap();
        }

        let records = segment.read_from_offset(start_offset, 1000).unwrap();

        // All returned records should have offset >= start_offset
        for record in &records {
            prop_assert!(
                record.offset >= start_offset,
                "Record offset {} should be >= start_offset {}",
                record.offset,
                start_offset
            );
        }

        // Count should match expected
        let expected_count = record_count.saturating_sub(start_offset as usize);
        prop_assert_eq!(
            records.len(),
            expected_count,
            "Should return {} records starting from offset {}",
            expected_count,
            start_offset
        );
    }

    /// Property: RecordBatch last_offset returns correct value
    #[test]
    fn record_batch_last_offset_is_correct(
        offsets in prop::collection::vec(0i64..1000, 1..20)
    ) {
        let sorted_offsets: Vec<i64> = {
            let mut v = offsets.clone();
            v.sort();
            v
        };

        let mut batch = RecordBatch::new(sorted_offsets[0], 1000);

        for offset in &sorted_offsets {
            let record = Record::new(*offset, 1000, None, Bytes::from("test"));
            batch.add_record(record);
        }

        let last = batch.last_offset();
        prop_assert_eq!(
            last,
            Some(*sorted_offsets.last().unwrap()),
            "last_offset should return the offset of the last record"
        );
    }

    /// Property: Empty RecordBatch has correct properties
    #[test]
    fn empty_batch_properties(
        base_offset in any::<i64>(),
        timestamp in any::<i64>()
    ) {
        let batch = RecordBatch::new(base_offset, timestamp);

        prop_assert!(batch.is_empty());
        prop_assert_eq!(batch.len(), 0);
        prop_assert_eq!(batch.last_offset(), None);
        prop_assert_eq!(batch.size(), 16); // Just metadata overhead
    }

    /// Property: TTL expiration logic is correct
    #[test]
    fn ttl_expiration_is_correct(
        record_time in 0i64..1_000_000,
        ttl_ms in 1i64..100_000,
        time_delta in 0i64..200_000
    ) {
        let record = Record::new(0, record_time, None, Bytes::from("test"));
        let current_time = record_time + time_delta;

        let is_expired = record.is_expired(ttl_ms, current_time);
        let expiry_time = record_time + ttl_ms;

        if current_time > expiry_time {
            prop_assert!(is_expired, "Record should be expired when current_time > expiry_time");
        } else {
            prop_assert!(!is_expired, "Record should not be expired when current_time <= expiry_time");
        }
    }

    /// Property: Infinite TTL (-1) never expires
    #[test]
    fn infinite_ttl_never_expires(
        record_time in 0i64..i64::MAX/2,
        time_delta in 0i64..i64::MAX/2
    ) {
        let record = Record::new(0, record_time, None, Bytes::from("test"));
        let current_time = record_time.saturating_add(time_delta);

        prop_assert!(
            !record.is_expired(-1, current_time),
            "Record with infinite TTL should never expire"
        );
    }
}

#[cfg(test)]
mod segment_integrity_tests {
    use super::*;
    use streamline::storage::compression::CompressionCodec;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        /// Property: Compression does not affect data integrity
        #[test]
        fn compression_preserves_data(
            payloads in prop::collection::vec(arbitrary_bytes(5_000), 1..20),
            codec in prop::sample::select(vec![
                CompressionCodec::None,
                CompressionCodec::Lz4,
                CompressionCodec::Zstd,
                CompressionCodec::Snappy,
                CompressionCodec::Gzip,
            ])
        ) {
            let dir = tempdir().unwrap();
            let path = dir.path().join("test.segment");

            let mut segment = Segment::create_with_compression(&path, 0, codec).unwrap();

            for (i, payload) in payloads.iter().enumerate() {
                let record = Record::new(
                    i as i64,
                    1000,
                    None,
                    Bytes::from(payload.clone()),
                );
                segment.append_record(record).unwrap();
            }

            let records = segment.read_all().unwrap();
            prop_assert_eq!(records.len(), payloads.len());

            for (record, original) in records.iter().zip(payloads.iter()) {
                prop_assert_eq!(
                    record.value.as_ref(),
                    original.as_slice(),
                    "Data should be preserved with {:?} compression",
                    codec
                );
            }
        }

        /// Property: Segment can be reopened and read correctly
        #[test]
        fn segment_reopen_preserves_data(
            payloads in prop::collection::vec(arbitrary_bytes(1_000), 1..30)
        ) {
            let dir = tempdir().unwrap();
            let path = dir.path().join("test.segment");

            // Write and seal
            {
                let mut segment = Segment::create(&path, 0).unwrap();
                for (i, payload) in payloads.iter().enumerate() {
                    let record = Record::new(
                        i as i64,
                        1000,
                        None,
                        Bytes::from(payload.clone()),
                    );
                    segment.append_record(record).unwrap();
                }
                segment.seal().unwrap();
            }

            // Reopen and verify
            let segment = Segment::open(&path).unwrap();
            let records = segment.read_all().unwrap();

            prop_assert_eq!(records.len(), payloads.len());

            for (record, original) in records.iter().zip(payloads.iter()) {
                prop_assert_eq!(
                    record.value.as_ref(),
                    original.as_slice(),
                    "Data should survive segment reopen"
                );
            }
        }
    }
}

// ============================================================================
// Error Handling Property Tests
// ============================================================================

mod error_properties {
    use super::*;
    use streamline::StreamlineError;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        /// Property: All error types produce valid Kafka error codes
        #[test]
        fn all_errors_have_valid_kafka_codes(
            msg in "[a-zA-Z0-9 ._-]{0,100}"
        ) {
            let errors = vec![
                StreamlineError::Storage(msg.clone()),
                StreamlineError::Protocol(msg.clone()),
                StreamlineError::Config(msg.clone()),
                StreamlineError::Server(msg.clone()),
                StreamlineError::Cluster(msg.clone()),
                StreamlineError::Replication(msg.clone()),
                StreamlineError::NotLeader(msg.clone(), 0),
                StreamlineError::TopicNotFound(msg.clone()),
                StreamlineError::PartitionNotFound(msg.clone(), 0),
                StreamlineError::InvalidOffset(0),
                StreamlineError::MessageTooLarge(100, 50),
                StreamlineError::TopicAlreadyExists(msg.clone()),
                StreamlineError::AuthenticationFailed(msg.clone()),
                StreamlineError::AuthorizationFailed(msg.clone()),
                StreamlineError::CorruptedData(msg.clone()),
                StreamlineError::InvalidTopicName(msg.clone()),
            ];

            for error in errors {
                let code = error.kafka_error_code();
                // Kafka error codes are in the i16 range
                let code_i16 = code.as_i16();
                prop_assert!(
                    (-1..=120).contains(&code_i16),
                    "Error code {} out of valid range for {:?}",
                    code_i16,
                    error
                );
            }
        }

        /// Property: Error display includes the message content
        #[test]
        fn error_display_includes_message(
            msg in "[a-zA-Z0-9]{1,50}"
        ) {
            let error = StreamlineError::Storage(msg.clone());
            let display = format!("{}", error);
            prop_assert!(
                display.contains(&msg) || display.to_lowercase().contains("storage"),
                "Display '{}' should reference the error type or message",
                display
            );
        }

        /// Property: Error context builders produce consistent format
        #[test]
        fn error_builders_produce_valid_errors(
            operation in "[a-z_]{1,20}",
            detail in "[a-zA-Z0-9 ]{0,50}"
        ) {
            let storage_err = StreamlineError::storage(&operation, &detail);
            let display = format!("{}", storage_err);
            prop_assert!(
                display.contains(&operation) || display.contains(&detail) || display.to_lowercase().contains("storage"),
                "Storage error should contain operation or detail"
            );

            let protocol_err = StreamlineError::protocol(&operation, &detail);
            let display = format!("{}", protocol_err);
            prop_assert!(
                display.contains(&operation) || display.contains(&detail) || display.to_lowercase().contains("protocol"),
                "Protocol error should contain operation or detail"
            );
        }

        /// Property: Partition-specific errors include partition info
        #[test]
        fn partition_errors_include_context(
            topic in "[a-z_]{1,20}",
            partition in 0i32..1000,
            operation in "[a-z_]{1,10}",
            detail in "[a-zA-Z0-9 ]{0,30}"
        ) {
            let error = StreamlineError::storage_partition(&topic, partition, &operation, &detail);
            let display = format!("{}", error);

            // The error should include the topic or partition context
            prop_assert!(
                display.contains(&topic) || display.contains(&partition.to_string()),
                "Partition error should include topic or partition: {}",
                display
            );
        }

        /// Property: is_retriable is consistent for same error type
        #[test]
        fn is_retriable_is_deterministic(
            msg in "[a-zA-Z0-9]{0,50}",
            partition in 0i32..100
        ) {
            // Create the same error twice
            let error1 = StreamlineError::NotLeader(msg.clone(), partition);
            let error2 = StreamlineError::NotLeader(msg.clone(), partition);
            prop_assert_eq!(
                error1.is_retriable(),
                error2.is_retriable(),
                "Same error type should have same retriable status"
            );

            // NotLeader should be retriable
            prop_assert!(
                error1.is_retriable(),
                "NotLeader should be retriable"
            );

            // AuthenticationFailed should not be retriable
            let auth_error = StreamlineError::AuthenticationFailed(msg);
            prop_assert!(
                !auth_error.is_retriable(),
                "AuthenticationFailed should not be retriable"
            );
        }
    }
}

// ============================================================================
// Record Batch Property Tests
// ============================================================================

mod record_batch_properties {
    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        /// Property: Record batch maintains insertion order
        #[test]
        fn batch_maintains_order(
            values in prop::collection::vec(arbitrary_bytes(100), 1..20)
        ) {
            let base_offset = 0i64;
            let timestamp = 1000i64;
            let mut batch = RecordBatch::new(base_offset, timestamp);

            for (i, value) in values.iter().enumerate() {
                let record = Record::new(
                    base_offset + i as i64,
                    timestamp,
                    None,
                    Bytes::from(value.clone()),
                );
                batch.add_record(record);
            }

            // Access records via public field
            prop_assert_eq!(batch.records.len(), values.len());

            for (i, record) in batch.records.iter().enumerate() {
                prop_assert_eq!(
                    record.value.as_ref(),
                    values[i].as_slice(),
                    "Record {} should match original value",
                    i
                );
            }
        }

        /// Property: Batch record count is accurate
        #[test]
        fn batch_count_is_accurate(
            count in 0usize..100
        ) {
            let mut batch = RecordBatch::new(0, 1000);

            for i in 0..count {
                let record = Record::new(i as i64, 1000, None, Bytes::from("test"));
                batch.add_record(record);
            }

            prop_assert_eq!(
                batch.len(),
                count,
                "Record count should match number of records added"
            );
        }

        /// Property: Empty batch has zero records
        #[test]
        fn empty_batch_has_zero_records(
            base_offset in any::<i64>(),
            timestamp in any::<i64>()
        ) {
            let batch = RecordBatch::new(base_offset, timestamp);
            prop_assert_eq!(batch.len(), 0);
            prop_assert!(batch.is_empty());
        }
    }
}

// ============================================================================
// Topic Name Validation Property Tests
// ============================================================================

mod topic_name_properties {
    use super::*;

    /// Generate valid topic names using regex pattern
    fn valid_topic_name() -> impl Strategy<Value = String> {
        "[a-zA-Z0-9._-]{1,249}"
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        /// Property: Valid topic names don't contain forbidden characters
        #[test]
        fn valid_names_have_allowed_chars(
            name in valid_topic_name()
        ) {
            for c in name.chars() {
                prop_assert!(
                    c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-',
                    "Character '{}' should be allowed in topic name",
                    c
                );
            }
        }

        /// Property: Topic names have reasonable length
        #[test]
        fn topic_names_within_length_limits(
            name in valid_topic_name()
        ) {
            prop_assert!(
                !name.is_empty(),
                "Topic name should not be empty"
            );
            prop_assert!(
                name.len() <= 249,
                "Topic name should be at most 249 characters"
            );
        }
    }
}
