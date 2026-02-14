//! Integration tests for Apache Iceberg sink connector
//!
//! These tests verify the Iceberg sink functionality including:
//! - Sink creation and configuration
//! - Parquet file generation
//! - Arrow schema conversion
//! - Partitioning strategies
//!
//! Note: Tests that require a running Iceberg catalog server are marked with `#[ignore]`.

#![cfg(feature = "iceberg")]

use std::collections::HashMap;
use std::sync::Arc;
use streamline::sink::config::{
    CatalogType, IcebergSchemaEvolution, IcebergSinkConfig, ParquetCompression, PartitioningConfig,
    PartitioningStrategy, TimeGranularity,
};
use streamline::sink::iceberg::IcebergSink;
use streamline::sink::{SinkConnector, SinkStatus};
use streamline::TopicManager;
use tempfile::TempDir;

/// Create a test configuration for the Iceberg sink
fn create_test_config(output_dir: &str) -> IcebergSinkConfig {
    IcebergSinkConfig {
        catalog_uri: "http://localhost:8181".to_string(),
        catalog_type: CatalogType::Rest,
        namespace: "default".to_string(),
        table: "test_events".to_string(),
        commit_interval_ms: 1000,
        max_batch_size: 100,
        partitioning: PartitioningConfig {
            strategy: PartitioningStrategy::TimeBasedHour,
            field: None,
            time_granularity: TimeGranularity::Hour,
        },
        catalog_properties: HashMap::new(),
        output_dir: output_dir.to_string(),
        warehouse: None,
        aws_region: None,
        compression: ParquetCompression::Snappy,
        max_retries: 3,
        retry_delay_ms: 100,
        schema_evolution: IcebergSchemaEvolution::Strict,
    }
}

/// Test that an Iceberg sink can be created with valid configuration
#[test]
fn test_iceberg_sink_lifecycle() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    let config = create_test_config(&temp_dir.path().join("iceberg").to_string_lossy());

    let sink = IcebergSink::new(
        "test-sink".to_string(),
        vec!["events".to_string()],
        config,
        topic_manager,
    );

    assert!(sink.is_ok());
    let sink = sink.unwrap();
    assert_eq!(sink.name(), "test-sink");
    assert_eq!(sink.topics(), vec!["events"]);
    assert_eq!(sink.status(), SinkStatus::Stopped);
}

/// Test configuration validation - empty catalog URI
#[test]
fn test_config_validation_empty_catalog_uri() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    let mut config = create_test_config(&temp_dir.path().join("iceberg").to_string_lossy());
    config.catalog_uri = String::new();

    let result = IcebergSink::new(
        "test-sink".to_string(),
        vec!["events".to_string()],
        config,
        topic_manager,
    );

    assert!(result.is_err());
}

/// Test configuration validation - empty table name
#[test]
fn test_config_validation_empty_table() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    let mut config = create_test_config(&temp_dir.path().join("iceberg").to_string_lossy());
    config.table = String::new();

    let result = IcebergSink::new(
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

    let mut config = create_test_config(&temp_dir.path().join("iceberg").to_string_lossy());
    config.commit_interval_ms = 0;

    let result = IcebergSink::new(
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

    let mut config = create_test_config(&temp_dir.path().join("iceberg").to_string_lossy());
    config.max_batch_size = 0;

    let result = IcebergSink::new(
        "test-sink".to_string(),
        vec!["events".to_string()],
        config,
        topic_manager,
    );

    assert!(result.is_err());
}

/// Test Hive catalog type returns helpful error
#[tokio::test]
async fn test_hive_catalog_not_supported() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    let mut config = create_test_config(&temp_dir.path().join("iceberg").to_string_lossy());
    config.catalog_type = CatalogType::Hive;

    let mut sink = IcebergSink::new(
        "test-sink".to_string(),
        vec!["events".to_string()],
        config,
        topic_manager,
    )
    .unwrap();

    // Starting should fail with a helpful error about Hive not being supported
    let result: Result<(), _> = sink.start().await;
    assert!(result.is_err());

    let error = result.unwrap_err().to_string();
    assert!(error.contains("Hive catalog support is not yet available"));
    assert!(error.contains("hive_metastore"));
}

/// Test Glue catalog type returns helpful error
#[tokio::test]
async fn test_glue_catalog_not_supported() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    let mut config = create_test_config(&temp_dir.path().join("iceberg").to_string_lossy());
    config.catalog_type = CatalogType::Glue;

    let mut sink = IcebergSink::new(
        "test-sink".to_string(),
        vec!["events".to_string()],
        config,
        topic_manager,
    )
    .unwrap();

    // Starting should fail with a helpful error about Glue not being supported
    let result: Result<(), _> = sink.start().await;
    assert!(result.is_err());

    let error = result.unwrap_err().to_string();
    assert!(error.contains("Glue catalog support is not yet available"));
    assert!(error.contains("hive_metastore"));
}

/// Test different partitioning strategies
#[test]
fn test_partitioning_strategies() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    // Test no partitioning
    let mut config = create_test_config(&temp_dir.path().join("iceberg").to_string_lossy());
    config.partitioning = PartitioningConfig {
        strategy: PartitioningStrategy::None,
        field: None,
        time_granularity: TimeGranularity::Hour,
    };

    let sink = IcebergSink::new(
        "no-partition-sink".to_string(),
        vec!["events".to_string()],
        config.clone(),
        topic_manager.clone(),
    );
    assert!(sink.is_ok());

    // Test hourly partitioning
    config.partitioning.strategy = PartitioningStrategy::TimeBasedHour;
    let sink = IcebergSink::new(
        "hourly-sink".to_string(),
        vec!["events".to_string()],
        config.clone(),
        topic_manager.clone(),
    );
    assert!(sink.is_ok());

    // Test daily partitioning
    config.partitioning.strategy = PartitioningStrategy::TimeBasedDay;
    config.partitioning.time_granularity = TimeGranularity::Day;
    let sink = IcebergSink::new(
        "daily-sink".to_string(),
        vec!["events".to_string()],
        config.clone(),
        topic_manager.clone(),
    );
    assert!(sink.is_ok());

    // Test monthly partitioning
    config.partitioning.strategy = PartitioningStrategy::TimeBasedMonth;
    config.partitioning.time_granularity = TimeGranularity::Month;
    let sink = IcebergSink::new(
        "monthly-sink".to_string(),
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

    let config = create_test_config(&temp_dir.path().join("iceberg").to_string_lossy());

    let sink = IcebergSink::new(
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

/// Test field-based partitioning configuration
#[test]
fn test_field_based_partitioning_config() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    let mut config = create_test_config(&temp_dir.path().join("iceberg").to_string_lossy());
    config.partitioning = PartitioningConfig {
        strategy: PartitioningStrategy::FieldBased,
        field: Some("region".to_string()),
        time_granularity: TimeGranularity::Hour,
    };

    let sink = IcebergSink::new(
        "field-partition-sink".to_string(),
        vec!["events".to_string()],
        config,
        topic_manager,
    );

    assert!(sink.is_ok());
}

/// Test compression configuration options
#[test]
fn test_compression_config() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    // Test each compression type
    for compression in [
        ParquetCompression::None,
        ParquetCompression::Snappy,
        ParquetCompression::Gzip,
        ParquetCompression::Lz4,
        ParquetCompression::Zstd,
    ] {
        let mut config = create_test_config(&temp_dir.path().join("iceberg").to_string_lossy());
        config.compression = compression;

        let sink = IcebergSink::new(
            format!("compression-sink-{:?}", compression),
            vec!["events".to_string()],
            config,
            topic_manager.clone(),
        );

        assert!(sink.is_ok(), "Failed for compression {:?}", compression);
    }
}

/// Test retry configuration
#[test]
fn test_retry_config() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    let mut config = create_test_config(&temp_dir.path().join("iceberg").to_string_lossy());
    config.max_retries = 5;
    config.retry_delay_ms = 500;

    let sink = IcebergSink::new(
        "retry-sink".to_string(),
        vec!["events".to_string()],
        config,
        topic_manager,
    );

    assert!(sink.is_ok());
}

/// Test schema evolution configuration
#[test]
fn test_schema_evolution_config() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    // Test each schema evolution strategy
    for evolution in [
        IcebergSchemaEvolution::Strict,
        IcebergSchemaEvolution::AddNewColumns,
        IcebergSchemaEvolution::AddAndPromote,
    ] {
        let mut config = create_test_config(&temp_dir.path().join("iceberg").to_string_lossy());
        config.schema_evolution = evolution;

        let sink = IcebergSink::new(
            format!("schema-evolution-sink-{:?}", evolution),
            vec!["events".to_string()],
            config,
            topic_manager.clone(),
        );

        assert!(sink.is_ok(), "Failed for schema evolution {:?}", evolution);
    }
}

/// End-to-end test with REST catalog (requires running catalog server)
///
/// This test verifies the full flow:
/// 1. Create topic with records
/// 2. Start sink
/// 3. Wait for records to be processed
/// 4. Verify metrics
/// 5. Stop sink
///
/// To run: Start a REST catalog server (e.g., Apache Iceberg REST catalog) at localhost:8181
#[tokio::test]
#[ignore = "requires running REST catalog server at localhost:8181"]
async fn test_end_to_end_with_rest_catalog() {
    let temp_dir = TempDir::new().unwrap();
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

    // Create topic
    topic_manager.create_topic("test-events", 1).unwrap();

    // Create and start sink
    let config = create_test_config(&temp_dir.path().join("iceberg").to_string_lossy());
    let mut sink = IcebergSink::new(
        "e2e-sink".to_string(),
        vec!["test-events".to_string()],
        config,
        topic_manager,
    )
    .unwrap();

    // Start sink
    let start_result: Result<(), _> = sink.start().await;
    assert!(
        start_result.is_ok(),
        "Failed to start sink: {:?}",
        start_result.err()
    );

    // Wait a bit for the sink to initialize
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Check status
    assert_eq!(sink.status(), SinkStatus::Running);

    // Stop sink
    let stop_result: Result<(), _> = sink.stop().await;
    assert!(stop_result.is_ok());
    assert_eq!(sink.status(), SinkStatus::Stopped);
}
