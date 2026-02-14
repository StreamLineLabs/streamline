//! Integration tests for Delta Lake sink connector
//!
//! These tests verify the Delta Lake sink functionality including:
//! - Sink creation and configuration
//! - Configuration validation
//! - Lifecycle management
//!
//! Note: Tests that require actual Delta Lake operations are marked with `#[ignore]`.

#![cfg(feature = "delta-lake")]

use std::collections::HashMap;
use std::sync::Arc;
use streamline::sink::config::{DeltaLakeSinkConfig, DeltaWriteMode, SchemaEvolutionPolicy};
use streamline::sink::delta::DeltaLakeSink;
use streamline::sink::{SinkConnector, SinkStatus, SinkType};
use streamline::TopicManager;
use tempfile::TempDir;

/// Create a test configuration for the Delta Lake sink
fn create_test_config(table_uri: &str) -> DeltaLakeSinkConfig {
    DeltaLakeSinkConfig {
        table_uri: table_uri.to_string(),
        storage_options: HashMap::new(),
        commit_interval_ms: 1000,
        max_batch_size: 100,
        write_mode: DeltaWriteMode::Append,
        partition_columns: vec![],
        target_file_size_bytes: 128 * 1024 * 1024, // 128MB
        auto_optimize: false,
        retention_hours: 168,
        schema_evolution: SchemaEvolutionPolicy::AddNewColumns,
        max_retries: 3,
        retry_delay_ms: 100,
    }
}

/// Test that a Delta Lake sink can be created with valid configuration
#[test]
fn test_delta_sink_lifecycle() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    let config = create_test_config(&temp_dir.path().join("delta").to_string_lossy());

    let sink = DeltaLakeSink::new(
        "test-sink".to_string(),
        vec!["events".to_string()],
        config,
        topic_manager,
    );

    assert!(sink.is_ok());
    let sink = sink.unwrap();
    assert_eq!(sink.name(), "test-sink");
    assert_eq!(sink.topics(), vec!["events"]);
    assert_eq!(sink.sink_type(), SinkType::DeltaLake);
    assert_eq!(sink.status(), SinkStatus::Stopped);
}

/// Test configuration validation - empty table URI
#[test]
fn test_config_validation_empty_table_uri() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    let mut config = create_test_config(&temp_dir.path().join("delta").to_string_lossy());
    config.table_uri = String::new();

    let result = DeltaLakeSink::new(
        "test-sink".to_string(),
        vec!["events".to_string()],
        config,
        topic_manager,
    );

    assert!(result.is_err());
}

/// Test configuration validation - zero commit interval
#[test]
fn test_config_validation_zero_commit_interval() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    let mut config = create_test_config(&temp_dir.path().join("delta").to_string_lossy());
    config.commit_interval_ms = 0;

    let result = DeltaLakeSink::new(
        "test-sink".to_string(),
        vec!["events".to_string()],
        config,
        topic_manager,
    );

    assert!(result.is_err());
}

/// Test configuration validation - zero batch size
#[test]
fn test_config_validation_zero_batch_size() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    let mut config = create_test_config(&temp_dir.path().join("delta").to_string_lossy());
    config.max_batch_size = 0;

    let result = DeltaLakeSink::new(
        "test-sink".to_string(),
        vec!["events".to_string()],
        config,
        topic_manager,
    );

    assert!(result.is_err());
}

/// Test different write modes
#[test]
fn test_write_modes() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    // Test each write mode
    for mode in [
        DeltaWriteMode::Append,
        DeltaWriteMode::Overwrite,
        DeltaWriteMode::Merge,
    ] {
        let mut config = create_test_config(&temp_dir.path().join("delta").to_string_lossy());
        config.write_mode = mode;

        let sink = DeltaLakeSink::new(
            format!("sink-{:?}", mode),
            vec!["events".to_string()],
            config,
            topic_manager.clone(),
        );

        assert!(sink.is_ok(), "Failed for write mode {:?}", mode);
    }
}

/// Test schema evolution configurations
#[test]
fn test_schema_evolution_config() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    // Test each schema evolution policy
    for policy in [
        SchemaEvolutionPolicy::Strict,
        SchemaEvolutionPolicy::AddNewColumns,
        SchemaEvolutionPolicy::AddAndWiden,
    ] {
        let mut config = create_test_config(&temp_dir.path().join("delta").to_string_lossy());
        config.schema_evolution = policy;

        let sink = DeltaLakeSink::new(
            format!("sink-{:?}", policy),
            vec!["events".to_string()],
            config,
            topic_manager.clone(),
        );

        assert!(sink.is_ok(), "Failed for schema evolution {:?}", policy);
    }
}

/// Test partitioned table configuration
#[test]
fn test_partition_columns() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    let mut config = create_test_config(&temp_dir.path().join("delta").to_string_lossy());
    config.partition_columns = vec!["date".to_string(), "region".to_string()];

    let sink = DeltaLakeSink::new(
        "partitioned-sink".to_string(),
        vec!["events".to_string()],
        config,
        topic_manager,
    );

    assert!(sink.is_ok());
}

/// Test sink with multiple topics
#[test]
fn test_multi_topic_sink() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    let config = create_test_config(&temp_dir.path().join("delta").to_string_lossy());

    let sink = DeltaLakeSink::new(
        "multi-topic-sink".to_string(),
        vec![
            "events".to_string(),
            "metrics".to_string(),
            "logs".to_string(),
        ],
        config,
        topic_manager,
    );

    assert!(sink.is_ok());
    let sink = sink.unwrap();
    assert_eq!(sink.topics(), vec!["events", "metrics", "logs"]);
}

/// Test storage options configuration
#[test]
fn test_storage_options() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    let mut config = create_test_config(&temp_dir.path().join("delta").to_string_lossy());
    config
        .storage_options
        .insert("AWS_REGION".to_string(), "us-west-2".to_string());
    config
        .storage_options
        .insert("AWS_PROFILE".to_string(), "default".to_string());

    let sink = DeltaLakeSink::new(
        "s3-sink".to_string(),
        vec!["events".to_string()],
        config,
        topic_manager,
    );

    assert!(sink.is_ok());
}

/// End-to-end test with local Delta table
///
/// This test verifies the full flow:
/// 1. Create sink
/// 2. Start sink
/// 3. Verify status
/// 4. Stop sink
#[tokio::test]
async fn test_sink_start_stop() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    // Create the delta table directory
    let delta_path = temp_dir.path().join("delta");
    std::fs::create_dir_all(&delta_path).unwrap();

    let config = create_test_config(&delta_path.to_string_lossy());
    let mut sink = DeltaLakeSink::new(
        "lifecycle-sink".to_string(),
        vec!["events".to_string()],
        config,
        topic_manager,
    )
    .unwrap();

    // Start sink (this uses placeholder implementation without full delta-lake feature)
    let start_result = sink.start().await;
    assert!(
        start_result.is_ok(),
        "Failed to start sink: {:?}",
        start_result.err()
    );
    assert_eq!(sink.status(), SinkStatus::Running);

    // Stop sink
    let stop_result = sink.stop().await;
    assert!(stop_result.is_ok());
    assert_eq!(sink.status(), SinkStatus::Stopped);
}

/// Test retry configuration
#[test]
fn test_retry_config() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    let mut config = create_test_config(&temp_dir.path().join("delta").to_string_lossy());
    config.max_retries = 5;
    config.retry_delay_ms = 500;

    let sink = DeltaLakeSink::new(
        "retry-sink".to_string(),
        vec!["events".to_string()],
        config,
        topic_manager,
    );

    assert!(sink.is_ok());
}

/// Test initial metrics have expected default values
#[test]
fn test_initial_metrics() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    let config = create_test_config(&temp_dir.path().join("delta").to_string_lossy());

    let sink = DeltaLakeSink::new(
        "metrics-sink".to_string(),
        vec!["events".to_string()],
        config,
        topic_manager,
    )
    .unwrap();

    let metrics = sink.metrics();
    assert_eq!(metrics.records_processed, 0);
    assert_eq!(metrics.records_failed, 0);
    assert_eq!(metrics.bytes_processed, 0);
    assert_eq!(metrics.commit_count, 0);
    assert_eq!(metrics.last_commit_latency_ms, 0);
    assert_eq!(metrics.error_count, 0);
}
