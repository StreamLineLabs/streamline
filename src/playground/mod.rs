//! Streamline Playground — one-command demo environment.
//!
//! Launches Streamline with sample data, demo topics, and a web dashboard
//! for interactive exploration. Perfect for first-time users.
//!
//! Provides:
//! - Ephemeral in-memory Streamline instances for experimentation
//! - Interactive tutorial engine with step-by-step guidance
//! - Scenario management for shareable playground states
//! - Sample data generators for realistic demo content
//! - Web dashboard for browser-based exploration

pub mod dashboard;
pub mod progress;
pub mod sample_data;
pub mod scenario;
pub mod tutorial;

pub use scenario::{PlaygroundConfig, PlaygroundManager, PlaygroundStats};
pub use tutorial::{Tutorial, TutorialEngine};
pub use progress::{
    Badge, CurriculumItem, CurriculumLevel, ProgressState, ProgressTracker,
    default_curriculum,
};

use crate::error::Result;
use crate::storage::TopicManager;

/// Demo topic definitions: (name, partition_count)
const DEMO_TOPICS: &[(&str, i32)] = &[
    ("demo-events", 3),
    ("demo-users", 1),
    ("demo-metrics", 2),
    ("demo-logs", 1),
];

/// Default number of sample messages per topic.
const DEFAULT_SAMPLE_COUNT: u32 = 100;

/// Create demo topics for playground mode.
pub fn setup_demo_topics(topic_manager: &TopicManager) -> Result<()> {
    for (name, partitions) in DEMO_TOPICS {
        match topic_manager.create_topic(name, *partitions) {
            Ok(()) => tracing::info!(topic = %name, partitions, "Created playground demo topic"),
            Err(crate::error::StreamlineError::TopicAlreadyExists(_)) => {
                tracing::debug!(topic = %name, "Demo topic already exists, skipping");
            }
            Err(e) => {
                tracing::warn!(topic = %name, error = %e, "Failed to create demo topic");
            }
        }
    }
    Ok(())
}

/// Populate demo topics with realistic sample data.
pub fn populate_sample_data(topic_manager: &TopicManager) -> Result<()> {
    let count = DEFAULT_SAMPLE_COUNT;

    let datasets: &[(&str, Vec<String>)] = &[
        ("demo-events", sample_data::generate_events(count)),
        ("demo-users", sample_data::generate_users(count)),
        ("demo-metrics", sample_data::generate_metrics(count)),
        ("demo-logs", sample_data::generate_logs(count)),
    ];

    for (topic, messages) in datasets {
        let partitions = topic_manager
            .get_topic_metadata(topic)
            .map(|m| m.num_partitions)
            .unwrap_or(1);

        let mut loaded = 0u32;
        for (i, msg) in messages.iter().enumerate() {
            let partition = (i as i32) % partitions;
            match topic_manager.append(topic, partition, None, bytes::Bytes::from(msg.clone())) {
                Ok(_) => loaded += 1,
                Err(e) => {
                    tracing::warn!(topic, error = %e, "Failed to insert sample message");
                    break;
                }
            }
        }
        tracing::info!(topic, messages = loaded, "Populated demo topic with sample data");
    }
    Ok(())
}

/// Print the playground welcome banner with URLs and quick-start commands.
pub fn print_playground_banner(kafka_port: u16, http_port: u16) {
    println!();
    println!("  ╔══════════════════════════════════════════════════════╗");
    println!("  ║          ⚡ Streamline Playground ⚡                ║");
    println!("  ║          The Redis of Streaming                     ║");
    println!("  ╚══════════════════════════════════════════════════════╝");
    println!();
    println!("  🌐 Dashboard:  http://localhost:{}/playground", http_port);
    println!("  📡 Kafka API:  localhost:{}", kafka_port);
    println!("  🔗 HTTP API:   http://localhost:{}/api/v1", http_port);
    println!("  ❤️  Health:     http://localhost:{}/health", http_port);
    println!();
    println!("  📋 Demo Topics:");
    for (name, partitions) in DEMO_TOPICS {
        println!(
            "     • {} ({} partition{})",
            name,
            partitions,
            if *partitions > 1 { "s" } else { "" }
        );
    }
    println!();
    println!("  🚀 Quick Start:");
    println!("     # Produce a message");
    println!(
        "     echo '{{\"hello\":\"world\"}}' | kcat -b localhost:{} -t demo-events -P",
        kafka_port
    );
    println!("     # Consume messages");
    println!(
        "     kcat -b localhost:{} -t demo-events -C -e",
        kafka_port
    );
    println!("     # Browse via HTTP");
    println!(
        "     curl http://localhost:{}/api/v1/topics",
        http_port
    );
    println!();
    println!("  ⚠️  In-memory mode — data will not persist after restart.");
    println!();
}
