//! Basic Consumer Example
//!
//! This example demonstrates how to use Streamline's storage layer
//! to read records from topics programmatically.
//!
//! Run with: cargo run --example basic_consumer
//!
//! Note: Run basic_producer first to create some data.

use std::path::Path;
use streamline::TopicManager;

fn main() -> streamline::Result<()> {
    // Initialize the topic manager with the same data directory
    let data_dir = Path::new("./example_data");
    let manager = TopicManager::new(data_dir)?;

    let topic_name = "orders";

    // Check if topic exists
    match manager.get_topic_metadata(topic_name) {
        Ok(metadata) => {
            println!(
                "Topic '{}' found with {} partitions",
                topic_name, metadata.num_partitions
            );
        }
        Err(_) => {
            println!(
                "Topic '{}' not found. Run basic_producer first!",
                topic_name
            );
            return Ok(());
        }
    }

    println!("\nReading records from all partitions...\n");

    // Read from each partition
    for partition in 0..3 {
        let earliest = manager.earliest_offset(topic_name, partition)?;
        let latest = manager.latest_offset(topic_name, partition)?;

        println!(
            "--- Partition {} (offsets {} to {}) ---",
            partition, earliest, latest
        );

        // Read all records from this partition
        let records = manager.read(topic_name, partition, earliest, 100)?;

        if records.is_empty() {
            println!("  (no records)");
        } else {
            for record in records {
                let key = record
                    .key
                    .as_ref()
                    .map(|k| String::from_utf8_lossy(k).to_string())
                    .unwrap_or_else(|| "(null)".to_string());

                let value = String::from_utf8_lossy(&record.value);

                println!("  offset={}, key='{}', value={}", record.offset, key, value);
            }
        }
        println!();
    }

    // Demonstrate reading from a specific offset
    println!("--- Reading from partition 0, starting at offset 1 ---");
    let records = manager.read(topic_name, 0, 1, 10)?;
    for record in records {
        println!("  offset={}", record.offset);
    }

    Ok(())
}
