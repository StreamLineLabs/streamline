//! Topic Management Example
//!
//! This example demonstrates how to create, list, describe,
//! and delete topics using Streamline's TopicManager.
//!
//! Run with: cargo run --example topic_management

use streamline::TopicManager;
use tempfile::tempdir;

fn main() -> streamline::Result<()> {
    // Use a temporary directory for this example
    let temp_dir = tempdir().expect("Failed to create temp directory");
    let data_dir = temp_dir.path();

    println!("Using temporary data directory: {}\n", data_dir.display());

    // Initialize the topic manager
    let manager = TopicManager::new(data_dir)?;

    // Create some topics
    println!("Creating topics...");
    manager.create_topic("user-events", 4)?;
    println!("  Created 'user-events' with 4 partitions");

    manager.create_topic("order-events", 8)?;
    println!("  Created 'order-events' with 8 partitions");

    manager.create_topic("logs", 1)?;
    println!("  Created 'logs' with 1 partition");

    // List all topics
    println!("\nListing all topics:");
    let topics = manager.list_topics()?;
    for topic in &topics {
        println!(
            "  - {} ({} partitions, created at {})",
            topic.name, topic.num_partitions, topic.created_at
        );
    }

    // Get detailed metadata for a specific topic
    println!("\nDetailed metadata for 'order-events':");
    let metadata = manager.get_topic_metadata("order-events")?;
    println!("  Name: {}", metadata.name);
    println!("  Partitions: {}", metadata.num_partitions);
    println!("  Replication Factor: {}", metadata.replication_factor);
    println!("  Retention (ms): {}", metadata.config.retention_ms);
    println!("  Retention (bytes): {}", metadata.config.retention_bytes);
    println!("  Segment size: {} bytes", metadata.config.segment_bytes);
    println!("  Cleanup policy: {}", metadata.config.cleanup_policy);

    // Delete a topic
    println!("\nDeleting 'logs' topic...");
    manager.delete_topic("logs")?;
    println!("  Deleted 'logs'");

    // Verify deletion
    println!("\nRemaining topics:");
    let topics = manager.list_topics()?;
    for topic in &topics {
        println!("  - {}", topic.name);
    }

    // Demonstrate auto-create
    println!("\nUsing get_or_create_topic for 'new-topic'...");
    manager.get_or_create_topic("new-topic", 2)?;
    println!("  Topic 'new-topic' created (or already existed)");

    // Calling again is idempotent
    manager.get_or_create_topic("new-topic", 2)?;
    println!("  Calling again is safe (idempotent)");

    // Final topic list
    println!("\nFinal topic list:");
    let topics = manager.list_topics()?;
    for topic in &topics {
        println!("  - {} ({} partitions)", topic.name, topic.num_partitions);
    }

    println!("\nDone! Temporary directory will be cleaned up automatically.");

    Ok(())
}
