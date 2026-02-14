//! Snapshot tests using insta
//!
//! This module demonstrates how to use insta for snapshot testing.
//! Snapshots are stored in tests/snapshots/ and can be reviewed with:
//!
//!   cargo insta review
//!
//! To run snapshot tests:
//!   cargo test --test snapshot_test
//!
//! To update snapshots:
//!   cargo insta test --accept

use streamline::{ErrorHint, StreamlineError};

/// Test error hint output for TopicNotFound
#[test]
fn test_topic_not_found_hint() {
    let error = StreamlineError::TopicNotFound("orders".to_string());
    insta::assert_snapshot!(error.with_hint());
}

/// Test error hint output for PartitionNotFound
#[test]
fn test_partition_not_found_hint() {
    let error = StreamlineError::PartitionNotFound("events".to_string(), 5);
    insta::assert_snapshot!(error.with_hint());
}

/// Test error hint output for TopicAlreadyExists
#[test]
fn test_topic_already_exists_hint() {
    let error = StreamlineError::TopicAlreadyExists("users".to_string());
    insta::assert_snapshot!(error.with_hint());
}

/// Test error hint output for InvalidTopicName
#[test]
fn test_invalid_topic_name_hint() {
    let error = StreamlineError::InvalidTopicName("invalid/topic/name".to_string());
    insta::assert_snapshot!(error.with_hint());
}

/// Test error hint output for MessageTooLarge
#[test]
fn test_message_too_large_hint() {
    let error = StreamlineError::MessageTooLarge(2_000_000, 1_000_000);
    insta::assert_snapshot!(error.with_hint());
}

/// Test error hint output for InvalidOffset
#[test]
fn test_invalid_offset_hint() {
    let error = StreamlineError::InvalidOffset(-100);
    insta::assert_snapshot!(error.with_hint());
}

/// Test error hint output for RequestTimeout
#[test]
fn test_request_timeout_hint() {
    let error = StreamlineError::RequestTimeout;
    insta::assert_snapshot!(error.with_hint());
}

/// Test error hint output for AuthenticationFailed
#[test]
fn test_auth_failed_hint() {
    let error = StreamlineError::AuthenticationFailed("SASL handshake failed".to_string());
    insta::assert_snapshot!(error.with_hint());
}

/// Test JSON error output format
#[test]
fn test_error_json_format() {
    use serde_json::json;

    let error = StreamlineError::TopicNotFound("orders".to_string());
    let json_output = json!({
        "error": error.to_string(),
        "hint": error.hint(),
        "retriable": error.is_retriable(),
    });

    insta::assert_json_snapshot!(json_output);
}

/// Test multiple errors in sequence
#[test]
fn test_multiple_errors() {
    let errors = [
        StreamlineError::TopicNotFound("topic1".to_string()),
        StreamlineError::InvalidOffset(999),
        StreamlineError::RateLimitExceeded,
        StreamlineError::Storage("disk full".to_string()),
    ];

    let output: Vec<String> = errors.iter().map(|e| e.with_hint()).collect();
    insta::assert_yaml_snapshot!(output);
}
