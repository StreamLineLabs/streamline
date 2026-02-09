use super::*;

#[test]
fn test_error_code_none_success() {
    let handler = create_test_handler();
    let response = handler
        .handle_api_versions(ApiVersionsRequest::default())
        .unwrap();

    assert_eq!(response.error_code, NONE, "Success = error code 0");
}

#[test]
fn test_error_code_unknown_topic() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    let handler = create_test_handler();

    let request = ListOffsetsRequest::default().with_topics(vec![ListOffsetsTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str("nope")))
        .with_partitions(vec![ListOffsetsPartition::default()
            .with_partition_index(0)
            .with_timestamp(-1)])]);

    let response = handler.handle_list_offsets(request, 7).unwrap();

    assert_eq!(
        response.topics[0].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
}

#[test]
fn test_error_code_topic_exists() {
    use kafka_protocol::messages::create_topics_request::CreatableTopic;

    let handler = create_handler_with_topics(&[("exists", 1)]);

    let request = CreateTopicsRequest::default().with_topics(vec![CreatableTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str("exists")))
        .with_num_partitions(1)
        .with_replication_factor(1)]);

    let response = handler.handle_create_topics(request).unwrap();

    assert_eq!(response.topics[0].error_code, TOPIC_ALREADY_EXISTS);
}

#[test]
fn test_error_code_none() {
    // Error code 0 = NONE (success)
    assert_eq!(NONE, 0);
}

#[test]
fn test_error_code_unknown_server_error() {
    // Error code -1 = UNKNOWN_SERVER_ERROR
    assert_eq!(UNKNOWN_SERVER_ERROR, -1);
}

#[test]
fn test_error_code_offset_out_of_range() {
    // Error code 1 = OFFSET_OUT_OF_RANGE
    assert_eq!(OFFSET_OUT_OF_RANGE, 1);
}

#[test]
fn test_error_code_corrupt_message() {
    // Error code 2 = CORRUPT_MESSAGE
    assert_eq!(CORRUPT_MESSAGE, 2);
}

#[test]
fn test_error_code_unknown_topic_or_partition() {
    // Error code 3 = UNKNOWN_TOPIC_OR_PARTITION
    assert_eq!(UNKNOWN_TOPIC_OR_PARTITION, 3);
}

#[test]
fn test_error_code_invalid_fetch_size() {
    // Error code 4 = INVALID_FETCH_SIZE
    assert_eq!(INVALID_FETCH_SIZE, 4);
}

#[test]
fn test_error_code_leader_not_available() {
    // Error code 5 = LEADER_NOT_AVAILABLE
    assert_eq!(LEADER_NOT_AVAILABLE, 5);
}

#[test]
fn test_error_code_not_leader_or_follower() {
    // Error code 6 = NOT_LEADER_OR_FOLLOWER
    assert_eq!(NOT_LEADER_OR_FOLLOWER, 6);
}

#[test]
fn test_error_code_request_timed_out() {
    // Error code 7 = REQUEST_TIMED_OUT
    assert_eq!(REQUEST_TIMED_OUT, 7);
}

#[test]
fn test_error_code_broker_not_available() {
    // Error code 8 = BROKER_NOT_AVAILABLE
    assert_eq!(BROKER_NOT_AVAILABLE, 8);
}

#[test]
fn test_error_code_replica_not_available() {
    // Error code 9 = REPLICA_NOT_AVAILABLE
    assert_eq!(REPLICA_NOT_AVAILABLE, 9);
}

#[test]
fn test_error_code_message_too_large() {
    // Error code 10 = MESSAGE_TOO_LARGE
    assert_eq!(MESSAGE_TOO_LARGE, 10);
}

#[test]
fn test_error_code_stale_controller_epoch() {
    // Error code 11 = STALE_CONTROLLER_EPOCH
    assert_eq!(STALE_CONTROLLER_EPOCH, 11);
}

#[test]
fn test_error_code_offset_metadata_too_large() {
    // Error code 12 = OFFSET_METADATA_TOO_LARGE
    assert_eq!(OFFSET_METADATA_TOO_LARGE, 12);
}

#[test]
fn test_error_code_network_exception() {
    // Error code 13 = NETWORK_EXCEPTION
    assert_eq!(NETWORK_EXCEPTION, 13);
}

#[test]
fn test_error_code_coordinator_load_in_progress() {
    // Error code 14 = COORDINATOR_LOAD_IN_PROGRESS
    assert_eq!(COORDINATOR_LOAD_IN_PROGRESS, 14);
}

#[test]
fn test_error_code_coordinator_not_available() {
    // Error code 15 = COORDINATOR_NOT_AVAILABLE
    assert_eq!(COORDINATOR_NOT_AVAILABLE, 15);
}

#[test]
fn test_error_code_not_coordinator() {
    // Error code 16 = NOT_COORDINATOR
    assert_eq!(NOT_COORDINATOR, 16);
}

#[test]
fn test_error_code_invalid_topic_exception() {
    // Error code 17 = INVALID_TOPIC_EXCEPTION
    assert_eq!(INVALID_TOPIC_EXCEPTION, 17);
}

#[test]
fn test_error_code_record_list_too_large() {
    // Error code 18 = RECORD_LIST_TOO_LARGE
    assert_eq!(RECORD_LIST_TOO_LARGE, 18);
}

#[test]
fn test_error_code_not_enough_replicas() {
    // Error code 19 = NOT_ENOUGH_REPLICAS
    assert_eq!(NOT_ENOUGH_REPLICAS, 19);
}

#[test]
fn test_error_code_not_enough_replicas_after_append() {
    // Error code 20 = NOT_ENOUGH_REPLICAS_AFTER_APPEND
    assert_eq!(NOT_ENOUGH_REPLICAS_AFTER_APPEND, 20);
}

#[test]
fn test_error_code_invalid_required_acks() {
    // Error code 21 = INVALID_REQUIRED_ACKS
    assert_eq!(INVALID_REQUIRED_ACKS, 21);
}

#[test]
fn test_error_code_illegal_generation() {
    // Error code 22 = ILLEGAL_GENERATION
    assert_eq!(ILLEGAL_GENERATION, 22);
}

#[test]
fn test_error_code_inconsistent_group_protocol() {
    // Error code 23 = INCONSISTENT_GROUP_PROTOCOL
    assert_eq!(INCONSISTENT_GROUP_PROTOCOL, 23);
}

#[test]
fn test_error_code_invalid_group_id() {
    // Error code 24 = INVALID_GROUP_ID
    assert_eq!(INVALID_GROUP_ID, 24);
}

#[test]
fn test_error_code_unknown_member_id() {
    // Error code 25 = UNKNOWN_MEMBER_ID
    assert_eq!(UNKNOWN_MEMBER_ID, 25);
}

#[test]
fn test_error_code_invalid_session_timeout() {
    // Error code 26 = INVALID_SESSION_TIMEOUT
    assert_eq!(INVALID_SESSION_TIMEOUT, 26);
}

#[test]
fn test_error_code_rebalance_in_progress() {
    // Error code 27 = REBALANCE_IN_PROGRESS
    assert_eq!(REBALANCE_IN_PROGRESS, 27);
}

#[test]
fn test_error_code_invalid_commit_offset_size() {
    // Error code 28 = INVALID_COMMIT_OFFSET_SIZE
    assert_eq!(INVALID_COMMIT_OFFSET_SIZE, 28);
}

#[test]
fn test_error_code_topic_authorization_failed() {
    // Error code 29 = TOPIC_AUTHORIZATION_FAILED
    assert_eq!(TOPIC_AUTHORIZATION_FAILED, 29);
}

#[test]
fn test_error_code_group_authorization_failed() {
    // Error code 30 = GROUP_AUTHORIZATION_FAILED
    assert_eq!(GROUP_AUTHORIZATION_FAILED, 30);
}

#[test]
fn test_error_code_cluster_authorization_failed() {
    // Error code 31 = CLUSTER_AUTHORIZATION_FAILED
    assert_eq!(CLUSTER_AUTHORIZATION_FAILED, 31);
}

#[test]
fn test_error_code_invalid_timestamp() {
    // Error code 32 = INVALID_TIMESTAMP
    assert_eq!(INVALID_TIMESTAMP, 32);
}

#[test]
fn test_error_code_unsupported_sasl_mechanism() {
    // Error code 33 = UNSUPPORTED_SASL_MECHANISM
    assert_eq!(UNSUPPORTED_SASL_MECHANISM, 33);
}

#[test]
fn test_error_code_illegal_sasl_state() {
    // Error code 34 = ILLEGAL_SASL_STATE
    assert_eq!(ILLEGAL_SASL_STATE, 34);
}

#[test]
fn test_error_code_unsupported_version() {
    // Error code 35 = UNSUPPORTED_VERSION
    assert_eq!(UNSUPPORTED_VERSION, 35);
}

#[test]
fn test_error_code_topic_already_exists() {
    // Error code 36 = TOPIC_ALREADY_EXISTS
    assert_eq!(TOPIC_ALREADY_EXISTS, 36);
}

#[test]
fn test_error_code_invalid_partitions() {
    // Error code 37 = INVALID_PARTITIONS
    assert_eq!(INVALID_PARTITIONS, 37);
}

#[test]
fn test_error_code_invalid_replication_factor() {
    // Error code 38 = INVALID_REPLICATION_FACTOR
    assert_eq!(INVALID_REPLICATION_FACTOR, 38);
}

#[test]
fn test_error_code_invalid_replica_assignment() {
    // Error code 39 = INVALID_REPLICA_ASSIGNMENT
    assert_eq!(INVALID_REPLICA_ASSIGNMENT, 39);
}

#[test]
fn test_error_code_invalid_config() {
    // Error code 40 = INVALID_CONFIG
    assert_eq!(INVALID_CONFIG, 40);
}

#[test]
fn test_error_code_not_controller() {
    // Error code 41 = NOT_CONTROLLER
    assert_eq!(NOT_CONTROLLER, 41);
}

#[test]
fn test_error_code_invalid_request() {
    // Error code 42 = INVALID_REQUEST
    assert_eq!(INVALID_REQUEST, 42);
}

#[test]
fn test_error_code_unsupported_for_message_format() {
    // Error code 43 = UNSUPPORTED_FOR_MESSAGE_FORMAT
    assert_eq!(UNSUPPORTED_FOR_MESSAGE_FORMAT, 43);
}

#[test]
fn test_error_code_policy_violation() {
    // Error code 44 = POLICY_VIOLATION
    assert_eq!(POLICY_VIOLATION, 44);
}

#[test]
fn test_error_code_out_of_order_sequence_number() {
    // Error code 45 = OUT_OF_ORDER_SEQUENCE_NUMBER
    assert_eq!(OUT_OF_ORDER_SEQUENCE_NUMBER, 45);
}

#[test]
fn test_error_code_duplicate_sequence_number() {
    // Error code 46 = DUPLICATE_SEQUENCE_NUMBER
    assert_eq!(DUPLICATE_SEQUENCE_NUMBER, 46);
}

#[test]
fn test_error_code_invalid_producer_epoch() {
    // Error code 47 = INVALID_PRODUCER_EPOCH
    assert_eq!(INVALID_PRODUCER_EPOCH, 47);
}

#[test]
fn test_error_code_invalid_txn_state() {
    // Error code 48 = INVALID_TXN_STATE
    assert_eq!(INVALID_TXN_STATE, 48);
}

#[test]
fn test_error_code_invalid_producer_id_mapping() {
    // Error code 49 = INVALID_PRODUCER_ID_MAPPING
    assert_eq!(INVALID_PRODUCER_ID_MAPPING, 49);
}

#[test]
fn test_error_code_invalid_transaction_timeout() {
    // Error code 50 = INVALID_TRANSACTION_TIMEOUT
    assert_eq!(INVALID_TRANSACTION_TIMEOUT, 50);
}

#[test]
fn test_error_code_concurrent_transactions() {
    // Error code 51 = CONCURRENT_TRANSACTIONS
    assert_eq!(CONCURRENT_TRANSACTIONS, 51);
}

#[test]
fn test_error_code_transaction_coordinator_fenced() {
    // Error code 52 = TRANSACTION_COORDINATOR_FENCED
    assert_eq!(TRANSACTION_COORDINATOR_FENCED, 52);
}

#[test]
fn test_error_code_transactional_id_authorization_failed() {
    // Error code 53 = TRANSACTIONAL_ID_AUTHORIZATION_FAILED
    assert_eq!(TRANSACTIONAL_ID_AUTHORIZATION_FAILED, 53);
}

#[test]
fn test_error_code_security_disabled() {
    // Error code 54 = SECURITY_DISABLED
    assert_eq!(SECURITY_DISABLED, 54);
}

#[test]
fn test_error_code_operation_not_attempted() {
    // Error code 55 = OPERATION_NOT_ATTEMPTED
    assert_eq!(OPERATION_NOT_ATTEMPTED, 55);
}

#[test]
fn test_error_code_kafka_storage_error() {
    // Error code 56 = KAFKA_STORAGE_ERROR
    assert_eq!(KAFKA_STORAGE_ERROR, 56);
}

#[test]
fn test_error_code_log_dir_not_found() {
    // Error code 57 = LOG_DIR_NOT_FOUND
    assert_eq!(LOG_DIR_NOT_FOUND, 57);
}

#[test]
fn test_error_code_sasl_authentication_failed() {
    // Error code 58 = SASL_AUTHENTICATION_FAILED
    assert_eq!(SASL_AUTHENTICATION_FAILED, 58);
}

#[test]
fn test_error_code_unknown_producer_id() {
    // Error code 59 = UNKNOWN_PRODUCER_ID
    assert_eq!(UNKNOWN_PRODUCER_ID, 59);
}

#[test]
fn test_error_code_reassignment_in_progress() {
    // Error code 60 = REASSIGNMENT_IN_PROGRESS
    assert_eq!(REASSIGNMENT_IN_PROGRESS, 60);
}

#[test]
fn test_error_code_delegation_token_auth_disabled() {
    // Error code 61 = DELEGATION_TOKEN_AUTH_DISABLED
    assert_eq!(DELEGATION_TOKEN_AUTH_DISABLED, 61);
}

#[test]
fn test_error_code_delegation_token_not_found() {
    // Error code 62 = DELEGATION_TOKEN_NOT_FOUND
    assert_eq!(DELEGATION_TOKEN_NOT_FOUND, 62);
}

#[test]
fn test_error_code_delegation_token_owner_mismatch() {
    // Error code 63 = DELEGATION_TOKEN_OWNER_MISMATCH
    assert_eq!(DELEGATION_TOKEN_OWNER_MISMATCH, 63);
}

#[test]
fn test_error_code_delegation_token_request_not_allowed() {
    // Error code 64 = DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
    assert_eq!(DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, 64);
}

#[test]
fn test_error_code_delegation_token_authorization_failed() {
    // Error code 65 = DELEGATION_TOKEN_AUTHORIZATION_FAILED
    assert_eq!(DELEGATION_TOKEN_AUTHORIZATION_FAILED, 65);
}

#[test]
fn test_error_code_delegation_token_expired() {
    // Error code 66 = DELEGATION_TOKEN_EXPIRED
    assert_eq!(DELEGATION_TOKEN_EXPIRED, 66);
}

#[test]
fn test_error_code_invalid_principal_type() {
    // Error code 67 = INVALID_PRINCIPAL_TYPE
    assert_eq!(INVALID_PRINCIPAL_TYPE, 67);
}

#[test]
fn test_error_code_non_empty_group() {
    // Error code 68 = NON_EMPTY_GROUP
    assert_eq!(NON_EMPTY_GROUP, 68);
}

#[test]
fn test_error_code_group_id_not_found() {
    // Error code 69 = GROUP_ID_NOT_FOUND
    assert_eq!(GROUP_ID_NOT_FOUND, 69);
}

#[test]
fn test_error_code_fetch_session_id_not_found() {
    // Error code 70 = FETCH_SESSION_ID_NOT_FOUND
    assert_eq!(FETCH_SESSION_ID_NOT_FOUND, 70);
}

#[test]
fn test_error_code_invalid_fetch_session_epoch() {
    // Error code 71 = INVALID_FETCH_SESSION_EPOCH
    assert_eq!(INVALID_FETCH_SESSION_EPOCH, 71);
}

#[test]
fn test_error_code_listener_not_found() {
    // Error code 72 = LISTENER_NOT_FOUND
    assert_eq!(LISTENER_NOT_FOUND, 72);
}

#[test]
fn test_error_code_topic_deletion_disabled() {
    // Error code 73 = TOPIC_DELETION_DISABLED
    assert_eq!(TOPIC_DELETION_DISABLED, 73);
}

#[test]
fn test_error_code_fenced_leader_epoch() {
    // Error code 74 = FENCED_LEADER_EPOCH
    assert_eq!(FENCED_LEADER_EPOCH, 74);
}

#[test]
fn test_error_code_unknown_leader_epoch() {
    // Error code 75 = UNKNOWN_LEADER_EPOCH
    assert_eq!(UNKNOWN_LEADER_EPOCH, 75);
}

#[test]
fn test_error_code_unsupported_compression_type() {
    // Error code 76 = UNSUPPORTED_COMPRESSION_TYPE
    assert_eq!(UNSUPPORTED_COMPRESSION_TYPE, 76);
}

#[test]
fn test_error_code_stale_broker_epoch() {
    // Error code 77 = STALE_BROKER_EPOCH
    assert_eq!(STALE_BROKER_EPOCH, 77);
}

#[test]
fn test_error_code_offset_not_available() {
    // Error code 78 = OFFSET_NOT_AVAILABLE
    assert_eq!(OFFSET_NOT_AVAILABLE, 78);
}

#[test]
fn test_error_code_member_id_required() {
    // Error code 79 = MEMBER_ID_REQUIRED
    assert_eq!(MEMBER_ID_REQUIRED, 79);
}

#[test]
fn test_error_code_preferred_leader_not_available() {
    // Error code 80 = PREFERRED_LEADER_NOT_AVAILABLE
    assert_eq!(PREFERRED_LEADER_NOT_AVAILABLE, 80);
}

#[test]
fn test_error_code_group_max_size_reached() {
    // Error code 81 = GROUP_MAX_SIZE_REACHED
    assert_eq!(GROUP_MAX_SIZE_REACHED, 81);
}

#[test]
fn test_error_code_fenced_instance_id() {
    // Error code 82 = FENCED_INSTANCE_ID
    assert_eq!(FENCED_INSTANCE_ID, 82);
}

#[test]
fn test_error_code_throttling_quota_exceeded() {
    // Error code 89 = THROTTLING_QUOTA_EXCEEDED
    assert_eq!(THROTTLING_QUOTA_EXCEEDED, 89);
}

#[test]
fn test_error_code_producer_fenced() {
    // Error code 90 = PRODUCER_FENCED
    assert_eq!(PRODUCER_FENCED, 90);
}

#[test]
fn test_error_code_transactional_id_not_found() {
    // Error code 105 = TRANSACTIONAL_ID_NOT_FOUND
    assert_eq!(TRANSACTIONAL_ID_NOT_FOUND, 105);
}

#[test]
fn test_error_codes_are_unique() {
    // Verify that all defined error codes have unique values (no duplicates)
    let codes = vec![
        (NONE, "NONE"),
        (UNKNOWN_SERVER_ERROR, "UNKNOWN_SERVER_ERROR"),
        (OFFSET_OUT_OF_RANGE, "OFFSET_OUT_OF_RANGE"),
        (CORRUPT_MESSAGE, "CORRUPT_MESSAGE"),
        (UNKNOWN_TOPIC_OR_PARTITION, "UNKNOWN_TOPIC_OR_PARTITION"),
        (INVALID_FETCH_SIZE, "INVALID_FETCH_SIZE"),
        (LEADER_NOT_AVAILABLE, "LEADER_NOT_AVAILABLE"),
        (NOT_LEADER_OR_FOLLOWER, "NOT_LEADER_OR_FOLLOWER"),
        (REQUEST_TIMED_OUT, "REQUEST_TIMED_OUT"),
        (BROKER_NOT_AVAILABLE, "BROKER_NOT_AVAILABLE"),
        (REPLICA_NOT_AVAILABLE, "REPLICA_NOT_AVAILABLE"),
        (MESSAGE_TOO_LARGE, "MESSAGE_TOO_LARGE"),
        (STALE_CONTROLLER_EPOCH, "STALE_CONTROLLER_EPOCH"),
        (OFFSET_METADATA_TOO_LARGE, "OFFSET_METADATA_TOO_LARGE"),
        (NETWORK_EXCEPTION, "NETWORK_EXCEPTION"),
        (COORDINATOR_LOAD_IN_PROGRESS, "COORDINATOR_LOAD_IN_PROGRESS"),
        (COORDINATOR_NOT_AVAILABLE, "COORDINATOR_NOT_AVAILABLE"),
        (NOT_COORDINATOR, "NOT_COORDINATOR"),
        (INVALID_TOPIC_EXCEPTION, "INVALID_TOPIC_EXCEPTION"),
        (RECORD_LIST_TOO_LARGE, "RECORD_LIST_TOO_LARGE"),
        (NOT_ENOUGH_REPLICAS, "NOT_ENOUGH_REPLICAS"),
        (
            NOT_ENOUGH_REPLICAS_AFTER_APPEND,
            "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
        ),
        (INVALID_REQUIRED_ACKS, "INVALID_REQUIRED_ACKS"),
        (ILLEGAL_GENERATION, "ILLEGAL_GENERATION"),
        (INCONSISTENT_GROUP_PROTOCOL, "INCONSISTENT_GROUP_PROTOCOL"),
        (INVALID_GROUP_ID, "INVALID_GROUP_ID"),
        (UNKNOWN_MEMBER_ID, "UNKNOWN_MEMBER_ID"),
        (INVALID_SESSION_TIMEOUT, "INVALID_SESSION_TIMEOUT"),
        (REBALANCE_IN_PROGRESS, "REBALANCE_IN_PROGRESS"),
        (INVALID_COMMIT_OFFSET_SIZE, "INVALID_COMMIT_OFFSET_SIZE"),
        (TOPIC_AUTHORIZATION_FAILED, "TOPIC_AUTHORIZATION_FAILED"),
        (GROUP_AUTHORIZATION_FAILED, "GROUP_AUTHORIZATION_FAILED"),
        (CLUSTER_AUTHORIZATION_FAILED, "CLUSTER_AUTHORIZATION_FAILED"),
        (INVALID_TIMESTAMP, "INVALID_TIMESTAMP"),
        (UNSUPPORTED_SASL_MECHANISM, "UNSUPPORTED_SASL_MECHANISM"),
        (ILLEGAL_SASL_STATE, "ILLEGAL_SASL_STATE"),
        (UNSUPPORTED_VERSION, "UNSUPPORTED_VERSION"),
        (TOPIC_ALREADY_EXISTS, "TOPIC_ALREADY_EXISTS"),
        (INVALID_PARTITIONS, "INVALID_PARTITIONS"),
        (INVALID_REPLICATION_FACTOR, "INVALID_REPLICATION_FACTOR"),
        (INVALID_REPLICA_ASSIGNMENT, "INVALID_REPLICA_ASSIGNMENT"),
        (INVALID_CONFIG, "INVALID_CONFIG"),
        (NOT_CONTROLLER, "NOT_CONTROLLER"),
        (INVALID_REQUEST, "INVALID_REQUEST"),
        (
            UNSUPPORTED_FOR_MESSAGE_FORMAT,
            "UNSUPPORTED_FOR_MESSAGE_FORMAT",
        ),
        (POLICY_VIOLATION, "POLICY_VIOLATION"),
        (OUT_OF_ORDER_SEQUENCE_NUMBER, "OUT_OF_ORDER_SEQUENCE_NUMBER"),
        (DUPLICATE_SEQUENCE_NUMBER, "DUPLICATE_SEQUENCE_NUMBER"),
        (INVALID_PRODUCER_EPOCH, "INVALID_PRODUCER_EPOCH"),
        (INVALID_TXN_STATE, "INVALID_TXN_STATE"),
        (INVALID_PRODUCER_ID_MAPPING, "INVALID_PRODUCER_ID_MAPPING"),
        (INVALID_TRANSACTION_TIMEOUT, "INVALID_TRANSACTION_TIMEOUT"),
        (CONCURRENT_TRANSACTIONS, "CONCURRENT_TRANSACTIONS"),
        (
            TRANSACTION_COORDINATOR_FENCED,
            "TRANSACTION_COORDINATOR_FENCED",
        ),
        (
            TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
            "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
        ),
        (SECURITY_DISABLED, "SECURITY_DISABLED"),
        (OPERATION_NOT_ATTEMPTED, "OPERATION_NOT_ATTEMPTED"),
        (KAFKA_STORAGE_ERROR, "KAFKA_STORAGE_ERROR"),
        (LOG_DIR_NOT_FOUND, "LOG_DIR_NOT_FOUND"),
        (SASL_AUTHENTICATION_FAILED, "SASL_AUTHENTICATION_FAILED"),
        (UNKNOWN_PRODUCER_ID, "UNKNOWN_PRODUCER_ID"),
        (REASSIGNMENT_IN_PROGRESS, "REASSIGNMENT_IN_PROGRESS"),
        (
            DELEGATION_TOKEN_AUTH_DISABLED,
            "DELEGATION_TOKEN_AUTH_DISABLED",
        ),
        (DELEGATION_TOKEN_NOT_FOUND, "DELEGATION_TOKEN_NOT_FOUND"),
        (
            DELEGATION_TOKEN_OWNER_MISMATCH,
            "DELEGATION_TOKEN_OWNER_MISMATCH",
        ),
        (
            DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED",
        ),
        (
            DELEGATION_TOKEN_AUTHORIZATION_FAILED,
            "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
        ),
        (DELEGATION_TOKEN_EXPIRED, "DELEGATION_TOKEN_EXPIRED"),
        (INVALID_PRINCIPAL_TYPE, "INVALID_PRINCIPAL_TYPE"),
        (NON_EMPTY_GROUP, "NON_EMPTY_GROUP"),
        (GROUP_ID_NOT_FOUND, "GROUP_ID_NOT_FOUND"),
        (FETCH_SESSION_ID_NOT_FOUND, "FETCH_SESSION_ID_NOT_FOUND"),
        (INVALID_FETCH_SESSION_EPOCH, "INVALID_FETCH_SESSION_EPOCH"),
        (LISTENER_NOT_FOUND, "LISTENER_NOT_FOUND"),
        (TOPIC_DELETION_DISABLED, "TOPIC_DELETION_DISABLED"),
        (FENCED_LEADER_EPOCH, "FENCED_LEADER_EPOCH"),
        (UNKNOWN_LEADER_EPOCH, "UNKNOWN_LEADER_EPOCH"),
        (UNSUPPORTED_COMPRESSION_TYPE, "UNSUPPORTED_COMPRESSION_TYPE"),
        (STALE_BROKER_EPOCH, "STALE_BROKER_EPOCH"),
        (OFFSET_NOT_AVAILABLE, "OFFSET_NOT_AVAILABLE"),
        (MEMBER_ID_REQUIRED, "MEMBER_ID_REQUIRED"),
        (
            PREFERRED_LEADER_NOT_AVAILABLE,
            "PREFERRED_LEADER_NOT_AVAILABLE",
        ),
        (GROUP_MAX_SIZE_REACHED, "GROUP_MAX_SIZE_REACHED"),
        (FENCED_INSTANCE_ID, "FENCED_INSTANCE_ID"),
        (THROTTLING_QUOTA_EXCEEDED, "THROTTLING_QUOTA_EXCEEDED"),
        (PRODUCER_FENCED, "PRODUCER_FENCED"),
        (TRANSACTIONAL_ID_NOT_FOUND, "TRANSACTIONAL_ID_NOT_FOUND"),
    ];

    // Check for duplicates
    use std::collections::HashSet;
    let mut seen_codes: HashSet<i16> = HashSet::new();
    for (code, name) in &codes {
        assert!(
            seen_codes.insert(*code),
            "Duplicate error code value {}: {}",
            code,
            name
        );
    }
}

#[test]
fn test_error_codes_in_valid_range() {
    // All error codes should be in the valid range per Kafka spec
    // -1 (UNKNOWN_SERVER_ERROR) to ~105 (TRANSACTIONAL_ID_NOT_FOUND)
    let codes = [
        NONE,
        UNKNOWN_SERVER_ERROR,
        OFFSET_OUT_OF_RANGE,
        CORRUPT_MESSAGE,
        UNKNOWN_TOPIC_OR_PARTITION,
        INVALID_FETCH_SIZE,
        LEADER_NOT_AVAILABLE,
        NOT_LEADER_OR_FOLLOWER,
        REQUEST_TIMED_OUT,
        BROKER_NOT_AVAILABLE,
        REPLICA_NOT_AVAILABLE,
        MESSAGE_TOO_LARGE,
        STALE_CONTROLLER_EPOCH,
        OFFSET_METADATA_TOO_LARGE,
        NETWORK_EXCEPTION,
        COORDINATOR_LOAD_IN_PROGRESS,
        COORDINATOR_NOT_AVAILABLE,
        NOT_COORDINATOR,
        INVALID_TOPIC_EXCEPTION,
        RECORD_LIST_TOO_LARGE,
        NOT_ENOUGH_REPLICAS,
        NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        INVALID_REQUIRED_ACKS,
        ILLEGAL_GENERATION,
        INCONSISTENT_GROUP_PROTOCOL,
        INVALID_GROUP_ID,
        UNKNOWN_MEMBER_ID,
        INVALID_SESSION_TIMEOUT,
        REBALANCE_IN_PROGRESS,
        INVALID_COMMIT_OFFSET_SIZE,
        TOPIC_AUTHORIZATION_FAILED,
        GROUP_AUTHORIZATION_FAILED,
        CLUSTER_AUTHORIZATION_FAILED,
        INVALID_TIMESTAMP,
        UNSUPPORTED_SASL_MECHANISM,
        ILLEGAL_SASL_STATE,
        UNSUPPORTED_VERSION,
        TOPIC_ALREADY_EXISTS,
        INVALID_PARTITIONS,
        INVALID_REPLICATION_FACTOR,
        INVALID_REPLICA_ASSIGNMENT,
        INVALID_CONFIG,
        NOT_CONTROLLER,
        INVALID_REQUEST,
        UNSUPPORTED_FOR_MESSAGE_FORMAT,
        POLICY_VIOLATION,
        OUT_OF_ORDER_SEQUENCE_NUMBER,
        DUPLICATE_SEQUENCE_NUMBER,
        INVALID_PRODUCER_EPOCH,
        INVALID_TXN_STATE,
        INVALID_PRODUCER_ID_MAPPING,
        INVALID_TRANSACTION_TIMEOUT,
        CONCURRENT_TRANSACTIONS,
        TRANSACTION_COORDINATOR_FENCED,
        TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        SECURITY_DISABLED,
        OPERATION_NOT_ATTEMPTED,
        KAFKA_STORAGE_ERROR,
        LOG_DIR_NOT_FOUND,
        SASL_AUTHENTICATION_FAILED,
        UNKNOWN_PRODUCER_ID,
        REASSIGNMENT_IN_PROGRESS,
        DELEGATION_TOKEN_AUTH_DISABLED,
        DELEGATION_TOKEN_NOT_FOUND,
        DELEGATION_TOKEN_OWNER_MISMATCH,
        DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
        DELEGATION_TOKEN_AUTHORIZATION_FAILED,
        DELEGATION_TOKEN_EXPIRED,
        INVALID_PRINCIPAL_TYPE,
        NON_EMPTY_GROUP,
        GROUP_ID_NOT_FOUND,
        FETCH_SESSION_ID_NOT_FOUND,
        INVALID_FETCH_SESSION_EPOCH,
        LISTENER_NOT_FOUND,
        TOPIC_DELETION_DISABLED,
        FENCED_LEADER_EPOCH,
        UNKNOWN_LEADER_EPOCH,
        UNSUPPORTED_COMPRESSION_TYPE,
        STALE_BROKER_EPOCH,
        OFFSET_NOT_AVAILABLE,
        MEMBER_ID_REQUIRED,
        PREFERRED_LEADER_NOT_AVAILABLE,
        GROUP_MAX_SIZE_REACHED,
        FENCED_INSTANCE_ID,
        THROTTLING_QUOTA_EXCEEDED,
        PRODUCER_FENCED,
        TRANSACTIONAL_ID_NOT_FOUND,
    ];

    for code in codes {
        // Valid range: -1 to 127 (i16 allows more, but Kafka uses -1 to ~105)
        assert!(
            (-1..=127).contains(&code),
            "Error code {} is outside valid range [-1, 127]",
            code
        );
    }
}
