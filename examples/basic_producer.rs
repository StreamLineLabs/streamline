//! Basic Producer Example
//!
//! This example demonstrates how to use Streamline's storage layer
//! to write records to topics programmatically.
//!
//! Run with: cargo run --example basic_producer

use bytes::Bytes;
use std::path::Path;
use streamline::TopicManager;

fn main() -> streamline::Result<()> {
    // Initialize the topic manager with a data directory
    let data_dir = Path::new("./example_data");
    let manager = TopicManager::new(data_dir)?;

    // Create a topic with 3 partitions (or use existing one)
    let topic_name = "orders";
    manager.get_or_create_topic(topic_name, 3)?;

    println!("Writing records to topic '{}'...", topic_name);

    // Write records to different partitions
    for i in 0..10 {
        // Simple partition assignment based on key
        let partition = i % 3;
        let key = format!("order-{}", i);
        let value = format!(
            r#"{{"order_id": {}, "product": "Widget {}", "quantity": {}}}"#,
            i,
            i,
            (i + 1) * 10
        );

        let offset = manager.append(
            topic_name,
            partition,
            Some(Bytes::from(key.clone())),
            Bytes::from(value),
        )?;

        println!(
            "  Wrote record: key='{}' to partition {} at offset {}",
            key, partition, offset
        );
    }

    // Check the latest offsets for each partition
    println!("\nPartition offsets:");
    for partition in 0..3 {
        let earliest = manager.earliest_offset(topic_name, partition)?;
        let latest = manager.latest_offset(topic_name, partition)?;
        println!(
            "  Partition {}: earliest={}, latest={}",
            partition, earliest, latest
        );
    }

    println!("\nDone! Data is persisted in '{}'", data_dir.display());

    Ok(())
}
