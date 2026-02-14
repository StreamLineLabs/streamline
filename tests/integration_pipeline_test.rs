//! Integration Pipeline Tests
//!
//! Tests for the CDC → AI → Lakehouse integration pipeline framework.

#[path = "common/mod.rs"]
mod common;

/// Test that the pipeline builder creates valid configurations
#[test]
fn test_pipeline_builder_creates_valid_config() {
    use streamline::integration::{PipelineBuilder, PipelineStage};

    let pipeline = PipelineBuilder::new("orders-analytics")
        .description("Process orders from CDC to lakehouse")
        .cdc_source("postgres", "inventory", &["orders", "customers"])
        .ai_enrichment("gpt-4", Some("Analyze customer sentiment"))
        .ai_classification(
            "order-classifier",
            &["high_value", "standard", "suspicious"],
        )
        .lakehouse_sink("warehouse", "iceberg", &["date", "region"])
        .parallelism(4)
        .batch_size(100)
        .build();

    assert!(pipeline.is_ok());
    let pipeline = pipeline.unwrap();

    // Verify pipeline properties
    assert_eq!(pipeline.name(), "orders-analytics");
    assert_eq!(pipeline.stages().len(), 4);

    // Verify stage order
    let stages = pipeline.stages();
    assert!(matches!(stages[0], PipelineStage::CdcSource { .. }));
    assert!(matches!(stages[1], PipelineStage::AiEnrichment { .. }));
    assert!(matches!(stages[2], PipelineStage::AiClassification { .. }));
    assert!(matches!(stages[3], PipelineStage::LakehouseSink { .. }));
}

/// Test pipeline builder validation - empty name
#[test]
fn test_pipeline_builder_rejects_empty_name() {
    use streamline::integration::PipelineBuilder;

    let result = PipelineBuilder::new("")
        .cdc_source("postgres", "db", &["table"])
        .build();

    assert!(result.is_err());
    match result {
        Err(err) => assert!(err.to_string().contains("name")),
        Ok(_) => panic!("Expected error for empty name"),
    }
}

/// Test pipeline builder validation - no stages
#[test]
fn test_pipeline_builder_rejects_no_stages() {
    use streamline::integration::PipelineBuilder;

    let result = PipelineBuilder::new("empty-pipeline").build();

    assert!(result.is_err());
    match result {
        Err(err) => assert!(err.to_string().contains("stage")),
        Ok(_) => panic!("Expected error for no stages"),
    }
}

/// Test pipeline stage type identification
#[test]
fn test_pipeline_stage_types() {
    use streamline::integration::PipelineStage;

    let cdc_stage = PipelineStage::CdcSource {
        source_type: "postgres".into(),
        database: "inventory".into(),
        tables: vec!["orders".into()],
    };
    assert_eq!(cdc_stage.stage_type(), "cdc_source");

    let ai_enrich = PipelineStage::AiEnrichment {
        model: "gpt-4".into(),
        prompt: Some("Analyze sentiment".into()),
    };
    assert_eq!(ai_enrich.stage_type(), "ai_enrichment");

    let ai_classify = PipelineStage::AiClassification {
        model: "classifier".into(),
        categories: vec!["a".into(), "b".into()],
    };
    assert_eq!(ai_classify.stage_type(), "ai_classification");

    let ai_anomaly = PipelineStage::AiAnomalyDetection {
        detector: "detector".into(),
        sensitivity: 0.95,
    };
    assert_eq!(ai_anomaly.stage_type(), "ai_anomaly_detection");

    let ai_embed = PipelineStage::AiEmbedding {
        model: "ada-002".into(),
        dimension: 1536,
    };
    assert_eq!(ai_embed.stage_type(), "ai_embedding");

    let transform = PipelineStage::Transform {
        expression: "$.data | uppercase".into(),
    };
    assert_eq!(transform.stage_type(), "transform");

    let filter = PipelineStage::Filter {
        predicate: "amount > 100".into(),
    };
    assert_eq!(filter.stage_type(), "filter");

    let lakehouse = PipelineStage::LakehouseSink {
        destination: "warehouse".into(),
        format: "iceberg".into(),
        partition_by: vec!["date".into()],
    };
    assert_eq!(lakehouse.stage_type(), "lakehouse_sink");

    let topic = PipelineStage::TopicSink {
        topic: "output".into(),
    };
    assert_eq!(topic.stage_type(), "topic_sink");
}

/// Test pipeline manager creation and retrieval
#[test]
fn test_pipeline_manager_crud() {
    use streamline::integration::{PipelineConfig, PipelineManager, PipelineStage};

    let manager = PipelineManager::new();

    // Create a pipeline
    let config = PipelineConfig {
        name: "test-pipeline".to_string(),
        description: Some("Test pipeline".to_string()),
        stages: vec![PipelineStage::TopicSink {
            topic: "output".to_string(),
        }],
        ..Default::default()
    };

    let result = manager.create_pipeline(config);
    assert!(result.is_ok());

    // Retrieve the pipeline
    let retrieved = manager.get_pipeline("test-pipeline");
    assert!(retrieved.is_some());
    let pipeline = retrieved.unwrap();
    assert_eq!(pipeline.name(), "test-pipeline");

    // List pipelines
    let list = manager.list_pipelines();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].name(), "test-pipeline");

    // Remove pipeline
    let removed = manager.remove_pipeline("test-pipeline");
    assert!(removed.is_some());

    // Verify removal
    let after_remove = manager.get_pipeline("test-pipeline");
    assert!(after_remove.is_none());
}

/// Test pipeline manager rejects duplicates
#[test]
fn test_pipeline_manager_rejects_duplicate() {
    use streamline::integration::{PipelineConfig, PipelineManager, PipelineStage};

    let manager = PipelineManager::new();

    let config = PipelineConfig {
        name: "duplicate-test".to_string(),
        stages: vec![PipelineStage::TopicSink {
            topic: "output".to_string(),
        }],
        ..Default::default()
    };

    // First creation succeeds
    assert!(manager.create_pipeline(config.clone()).is_ok());

    // Second creation fails
    let duplicate = manager.create_pipeline(config);
    assert!(duplicate.is_err());
    match duplicate {
        Err(err) => assert!(err.to_string().contains("exists")),
        Ok(_) => panic!("Expected duplicate error"),
    }
}

/// Test pipeline status transitions
#[tokio::test]
async fn test_pipeline_status_transitions() {
    use streamline::integration::{PipelineConfig, PipelineManager, PipelineStage, PipelineStatus};

    let manager = PipelineManager::new();

    let config = PipelineConfig {
        name: "status-test".to_string(),
        stages: vec![PipelineStage::TopicSink {
            topic: "output".to_string(),
        }],
        ..Default::default()
    };

    manager.create_pipeline(config).unwrap();

    // Initial status is Created
    let pipeline = manager.get_pipeline("status-test").unwrap();
    assert_eq!(pipeline.status().await, PipelineStatus::Created);

    // Start pipeline via manager (starts asynchronously, goes through Starting -> Running)
    manager.start_pipeline("status-test").await.unwrap();
    // Give the spawned task time to transition to Running
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let pipeline = manager.get_pipeline("status-test").unwrap();
    assert_eq!(pipeline.status().await, PipelineStatus::Running);

    // Pause pipeline (via Pipeline directly)
    pipeline.pause().await.unwrap();
    assert_eq!(pipeline.status().await, PipelineStatus::Paused);

    // Resume pipeline (via Pipeline directly)
    pipeline.resume().await.unwrap();
    assert_eq!(pipeline.status().await, PipelineStatus::Running);

    // Stop pipeline via manager (transitions through Stopping -> Stopped)
    manager.stop_pipeline("status-test").await.unwrap();
    // Give time for the status to transition to Stopped
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let pipeline = manager.get_pipeline("status-test").unwrap();
    assert_eq!(pipeline.status().await, PipelineStatus::Stopped);
}

/// Test pipeline metrics tracking
#[test]
fn test_pipeline_metrics_initialization() {
    use streamline::integration::PipelineMetrics;

    let metrics = PipelineMetrics::default();

    assert_eq!(metrics.records_processed, 0);
    assert_eq!(metrics.bytes_processed, 0);
    assert_eq!(metrics.errors, 0);
    assert!(metrics.avg_latency_ms >= 0.0);
}

/// Test complex pipeline with all stage types
#[test]
fn test_complex_pipeline_all_stages() {
    use streamline::integration::PipelineBuilder;

    let pipeline = PipelineBuilder::new("complex-pipeline")
        .description("Pipeline with all stage types")
        .cdc_source("postgres", "inventory", &["orders"])
        .transform("$.data | normalize")
        .filter("amount > 0")
        .ai_enrichment("gpt-4", Some("Enrich with context"))
        .ai_classification("classifier", &["a", "b", "c"])
        .ai_anomaly_detection("detector", 0.9)
        .ai_embedding("ada-002", 1536)
        .lakehouse_sink("warehouse", "parquet", &["date"])
        .topic_sink("output-topic")
        .parallelism(16)
        .batch_size(1000)
        .build();

    assert!(pipeline.is_ok());
    let pipeline = pipeline.unwrap();

    // Should have 9 stages
    assert_eq!(pipeline.stages().len(), 9);

    // Verify each stage type
    let stage_types: Vec<&str> = pipeline.stages().iter().map(|s| s.stage_type()).collect();
    assert_eq!(
        stage_types,
        vec![
            "cdc_source",
            "transform",
            "filter",
            "ai_enrichment",
            "ai_classification",
            "ai_anomaly_detection",
            "ai_embedding",
            "lakehouse_sink",
            "topic_sink"
        ]
    );
}

/// Test pipeline config serialization
#[test]
fn test_pipeline_config_serialization() {
    use streamline::integration::{PipelineConfig, PipelineStage};

    let config = PipelineConfig {
        name: "serialize-test".to_string(),
        description: Some("Test serialization".to_string()),
        stages: vec![
            PipelineStage::CdcSource {
                source_type: "postgres".into(),
                database: "db".into(),
                tables: vec!["t1".into(), "t2".into()],
            },
            PipelineStage::AiEnrichment {
                model: "gpt-4".into(),
                prompt: Some("Enrich".into()),
            },
        ],
        parallelism: 8,
        batch_size: 500,
        ..Default::default()
    };

    // Serialize to JSON
    let json = serde_json::to_string(&config).unwrap();
    assert!(json.contains("serialize-test"));
    assert!(json.contains("postgres"));
    assert!(json.contains("gpt-4"));

    // Deserialize back
    let deserialized: PipelineConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.name, "serialize-test");
    assert_eq!(deserialized.stages.len(), 2);
    assert_eq!(deserialized.parallelism, 8);
}

/// Test pipeline with edge cases in configuration
#[test]
fn test_pipeline_edge_cases() {
    use streamline::integration::PipelineBuilder;

    // Very long name (should work)
    let long_name = "a".repeat(256);
    let result = PipelineBuilder::new(&long_name)
        .cdc_source("pg", "db", &["t"])
        .build();
    assert!(result.is_ok());

    // Unicode in name (should work)
    let result = PipelineBuilder::new("pipeline-日本語-тест")
        .cdc_source("pg", "db", &["t"])
        .build();
    assert!(result.is_ok());

    // Empty tables list (should work - captures all tables)
    let result = PipelineBuilder::new("empty-tables")
        .cdc_source("pg", "db", &[])
        .build();
    assert!(result.is_ok());

    // Zero parallelism is accepted (kept as 0)
    let pipeline = PipelineBuilder::new("zero-parallel")
        .cdc_source("pg", "db", &["t"])
        .parallelism(0)
        .build()
        .unwrap();
    assert_eq!(pipeline.config().parallelism, 0);
}
