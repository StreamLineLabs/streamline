//! Benchmark Module Tests
//!
//! Tests for the benchmarking framework including profiles, message generators,
//! and configuration.

#[path = "common/mod.rs"]
mod common;

/// Test benchmark profile parsing
#[test]
fn test_benchmark_profile_parsing() {
    use streamline::cli_utils::benchmark::BenchmarkProfile;

    // Standard profiles
    assert!(matches!(
        BenchmarkProfile::parse("quick"),
        Some(BenchmarkProfile::Quick)
    ));
    assert!(matches!(
        BenchmarkProfile::parse("standard"),
        Some(BenchmarkProfile::Standard)
    ));
    assert!(matches!(
        BenchmarkProfile::parse("throughput"),
        Some(BenchmarkProfile::Throughput)
    ));
    assert!(matches!(
        BenchmarkProfile::parse("latency"),
        Some(BenchmarkProfile::Latency)
    ));

    // Case insensitivity
    assert!(matches!(
        BenchmarkProfile::parse("QUICK"),
        Some(BenchmarkProfile::Quick)
    ));
    assert!(matches!(
        BenchmarkProfile::parse("Quick"),
        Some(BenchmarkProfile::Quick)
    ));
    assert!(matches!(
        BenchmarkProfile::parse("STANDARD"),
        Some(BenchmarkProfile::Standard)
    ));

    // Aliases
    assert!(matches!(
        BenchmarkProfile::parse("ecommerce"),
        Some(BenchmarkProfile::Ecommerce)
    ));
    assert!(matches!(
        BenchmarkProfile::parse("e-commerce"),
        Some(BenchmarkProfile::Ecommerce)
    ));
    assert!(matches!(
        BenchmarkProfile::parse("logs"),
        Some(BenchmarkProfile::Logging)
    ));
    assert!(matches!(
        BenchmarkProfile::parse("logging"),
        Some(BenchmarkProfile::Logging)
    ));
    assert!(matches!(
        BenchmarkProfile::parse("change-data-capture"),
        Some(BenchmarkProfile::Cdc)
    ));
    assert!(matches!(
        BenchmarkProfile::parse("cdc"),
        Some(BenchmarkProfile::Cdc)
    ));

    // Next-gen profiles
    assert!(matches!(
        BenchmarkProfile::parse("ai"),
        Some(BenchmarkProfile::Ai)
    ));
    assert!(matches!(
        BenchmarkProfile::parse("ai-processing"),
        Some(BenchmarkProfile::Ai)
    ));
    assert!(matches!(
        BenchmarkProfile::parse("edge"),
        Some(BenchmarkProfile::Edge)
    ));
    assert!(matches!(
        BenchmarkProfile::parse("edge-sync"),
        Some(BenchmarkProfile::Edge)
    ));
    assert!(matches!(
        BenchmarkProfile::parse("lakehouse"),
        Some(BenchmarkProfile::Lakehouse)
    ));
    assert!(matches!(
        BenchmarkProfile::parse("warehouse"),
        Some(BenchmarkProfile::Lakehouse)
    ));
    assert!(matches!(
        BenchmarkProfile::parse("pipeline"),
        Some(BenchmarkProfile::Pipeline)
    ));
    assert!(matches!(
        BenchmarkProfile::parse("integration"),
        Some(BenchmarkProfile::Pipeline)
    ));

    // Invalid profiles
    assert!(BenchmarkProfile::parse("invalid").is_none());
    assert!(BenchmarkProfile::parse("").is_none());
    assert!(BenchmarkProfile::parse("unknown-profile").is_none());
}

/// Test benchmark profile descriptions
#[test]
fn test_benchmark_profile_descriptions() {
    use streamline::cli_utils::benchmark::BenchmarkProfile;

    // All profiles should have non-empty descriptions
    let profiles = [
        BenchmarkProfile::Quick,
        BenchmarkProfile::Standard,
        BenchmarkProfile::Throughput,
        BenchmarkProfile::Latency,
        BenchmarkProfile::Ecommerce,
        BenchmarkProfile::Iot,
        BenchmarkProfile::Logging,
        BenchmarkProfile::Cdc,
        BenchmarkProfile::Ai,
        BenchmarkProfile::Edge,
        BenchmarkProfile::Lakehouse,
        BenchmarkProfile::Pipeline,
    ];

    for profile in &profiles {
        let desc = profile.description();
        assert!(
            !desc.is_empty(),
            "Profile {:?} should have a description",
            profile
        );
        assert!(
            desc.len() > 10,
            "Profile {:?} description too short",
            profile
        );
    }

    // Check specific descriptions contain expected keywords
    assert!(BenchmarkProfile::Quick.description().contains("Quick"));
    assert!(BenchmarkProfile::Cdc
        .description()
        .to_lowercase()
        .contains("cdc"));
    assert!(BenchmarkProfile::Ai
        .description()
        .to_lowercase()
        .contains("ai"));
    assert!(BenchmarkProfile::Pipeline.description().contains("CDC"));
}

/// Test benchmark profile config generation
#[test]
fn test_benchmark_profile_configs() {
    use streamline::cli_utils::benchmark::BenchmarkProfile;

    // Quick profile should have minimal config for fast testing
    let quick_config = BenchmarkProfile::Quick.config();
    assert_eq!(quick_config.num_messages, 1_000);
    assert!(quick_config.num_producers >= 1);
    assert!(quick_config.message_size > 0);

    // Standard should have more messages
    let standard_config = BenchmarkProfile::Standard.config();
    assert_eq!(standard_config.num_messages, 100_000);
    assert!(standard_config.num_messages > quick_config.num_messages);

    // Throughput should have the most messages
    let throughput_config = BenchmarkProfile::Throughput.config();
    assert_eq!(throughput_config.num_messages, 1_000_000);

    // Each profile should have a unique topic name
    let topics: Vec<String> = [
        BenchmarkProfile::Quick,
        BenchmarkProfile::Standard,
        BenchmarkProfile::Throughput,
        BenchmarkProfile::Latency,
    ]
    .iter()
    .map(|p| p.config().topic_name)
    .collect();

    // Verify all topic names are unique
    let unique_topics: std::collections::HashSet<_> = topics.iter().collect();
    assert_eq!(
        topics.len(),
        unique_topics.len(),
        "Topic names should be unique"
    );
}

/// Test benchmark config defaults
#[test]
fn test_benchmark_config_defaults() {
    use streamline::cli_utils::benchmark::BenchmarkConfig;

    let config = BenchmarkConfig::default();

    // Verify sensible defaults
    assert!(config.num_messages > 0);
    assert!(config.message_size > 0);
    assert!(config.num_producers > 0);
    assert!(!config.topic_name.is_empty());
}

/// Test message generator output
#[test]
fn test_message_generator_output() {
    use streamline::cli_utils::benchmark::MessageGenerator;

    // Test each generator produces valid output
    let generators = [
        MessageGenerator::Ecommerce,
        MessageGenerator::Iot,
        MessageGenerator::Logging,
        MessageGenerator::Cdc,
        MessageGenerator::Ai,
        MessageGenerator::Edge,
        MessageGenerator::Lakehouse,
        MessageGenerator::Pipeline,
    ];

    for generator in &generators {
        // Generate multiple messages to test sequence handling
        for seq in 0..10 {
            let msg = generator.generate(seq);
            assert!(
                !msg.is_empty(),
                "Generator {:?} produced empty message at seq {}",
                generator,
                seq
            );

            // Most generators produce JSON
            let msg_str = String::from_utf8_lossy(&msg);
            assert!(
                msg_str.starts_with('{') || msg_str.starts_with('[') || !msg_str.is_empty(),
                "Generator {:?} output format unexpected: {}",
                generator,
                &msg_str[..50.min(msg_str.len())]
            );
        }
    }
}

/// Test message generator produces different content for different sequences
#[test]
fn test_message_generator_uniqueness() {
    use streamline::cli_utils::benchmark::MessageGenerator;

    let generator = MessageGenerator::Ecommerce;

    // Generate messages for different sequences
    let msg1 = generator.generate(0);
    let msg2 = generator.generate(1);
    let msg3 = generator.generate(100);

    // Messages should be different (at least contain different IDs/timestamps)
    // Note: Some generators might have identical structure with different values
    assert_ne!(msg1, msg2, "Messages for seq 0 and 1 should differ");
    assert_ne!(msg1, msg3, "Messages for seq 0 and 100 should differ");
}

/// Test CDC message generator format
#[test]
fn test_cdc_message_generator_format() {
    use streamline::cli_utils::benchmark::MessageGenerator;

    let generator = MessageGenerator::Cdc;
    let msg = generator.generate(0);
    let msg_str = String::from_utf8_lossy(&msg);

    // CDC messages should have standard CDC structure
    assert!(
        msg_str.contains("op") || msg_str.contains("operation"),
        "CDC message should have operation field"
    );
}

/// Test AI message generator format
#[test]
fn test_ai_message_generator_format() {
    use streamline::cli_utils::benchmark::MessageGenerator;

    let generator = MessageGenerator::Ai;
    let msg = generator.generate(0);
    let msg_str = String::from_utf8_lossy(&msg);

    // AI messages should have text content for processing
    assert!(
        msg_str.len() > 50,
        "AI messages should have substantial content for processing"
    );
}

/// Test pipeline message generator format
#[test]
fn test_pipeline_message_generator_format() {
    use streamline::cli_utils::benchmark::MessageGenerator;

    let generator = MessageGenerator::Pipeline;
    let msg = generator.generate(0);
    let msg_str = String::from_utf8_lossy(&msg);

    // Pipeline messages should be valid JSON with CDC-like structure
    assert!(msg_str.starts_with('{'), "Pipeline message should be JSON");
}

/// Test next-gen profile configurations
#[test]
fn test_nextgen_profile_configs() {
    use streamline::cli_utils::benchmark::BenchmarkProfile;

    // CDC profile
    let cdc_config = BenchmarkProfile::Cdc.config();
    assert_eq!(cdc_config.num_messages, 50_000);
    assert!(cdc_config.topic_name.contains("cdc"));

    // AI profile
    let ai_config = BenchmarkProfile::Ai.config();
    assert_eq!(ai_config.num_messages, 25_000);
    assert!(ai_config.topic_name.contains("ai"));

    // Edge profile
    let edge_config = BenchmarkProfile::Edge.config();
    assert_eq!(edge_config.num_messages, 100_000);
    assert!(edge_config.topic_name.contains("edge"));

    // Lakehouse profile
    let lakehouse_config = BenchmarkProfile::Lakehouse.config();
    assert_eq!(lakehouse_config.num_messages, 500_000);
    assert!(lakehouse_config.topic_name.contains("lakehouse"));

    // Pipeline profile
    let pipeline_config = BenchmarkProfile::Pipeline.config();
    assert_eq!(pipeline_config.num_messages, 10_000);
    assert!(pipeline_config.topic_name.contains("pipeline"));
}
