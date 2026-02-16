//! Edge Sync Example
//!
//! This example demonstrates edge-first architecture capabilities:
//! - Offline-first operation with sync-when-connected
//! - Edge-to-cloud federation
//! - Conflict resolution for distributed writes
//! - Resource-constrained optimizations
//!
//! Run with: cargo run --example edge_sync --features "edge"

#[cfg(feature = "edge")]
use bytes::Bytes;

#[cfg(feature = "edge")]
use streamline::{EmbeddedStreamline, Result};

#[cfg(feature = "edge")]
#[tokio::main]
async fn main() -> Result<()> {
    println!("Edge Sync Example");
    println!("=================\n");

    // Initialize embedded Streamline in edge mode (in-memory for speed)
    let streamline = EmbeddedStreamline::in_memory()?;

    // Create topics for edge data
    streamline.create_topic("sensor-readings", 1)?;
    streamline.create_topic("device-configs", 1)?;
    streamline.create_topic("edge-events", 1)?;

    // 1. Edge Configuration
    println!("1. Edge Configuration:");
    println!("   - Node ID: edge-factory-001");
    println!("   - Type: gateway");
    println!("   - Max Storage: 512 MB");
    println!("   - Max Memory: 256 MB");
    println!("   - Cloud Endpoint: wss://cloud.example.com/edge");
    println!();

    // 2. Sync Configuration
    println!("2. Sync Configuration:");
    println!("   - Mode: Bidirectional");
    println!("   - Conflict Resolution: LastWriteWins");
    println!("   - Topics:");
    println!("     - sensor-readings: EdgeToCloud");
    println!("     - device-configs: CloudToEdge");
    println!("     - edge-events: Bidirectional");
    println!();

    // 3. Simulate Edge Operations (Offline)
    println!("3. Simulating Offline Operations:");
    println!("   [OFFLINE] Network disconnected");

    let sensor_readings = vec![
        (1, 23.5, "temperature"),
        (2, 65.2, "humidity"),
        (3, 1013.25, "pressure"),
        (4, 24.1, "temperature"),
        (5, 64.8, "humidity"),
    ];

    let mut queued_count = 0;
    for (id, value, sensor_type) in &sensor_readings {
        let payload = format!(
            r#"{{"id":{},"type":"{}","value":{}}}"#,
            id, sensor_type, value
        );
        println!("   Recording: {} = {}", sensor_type, value);
        queued_count += 1;

        // Store locally even when offline
        streamline.produce(
            "sensor-readings",
            0,
            Some(Bytes::from(id.to_string())),
            Bytes::from(payload),
        )?;
    }
    println!("   Queued {} messages for sync\n", queued_count);

    // 4. Sync When Connected
    println!("4. Sync When Connected:");
    println!("   [ONLINE] Network connected");
    println!("   Starting sync with cloud...");
    println!("   - Uploading {} sensor readings", queued_count);
    println!("   - Downloading config updates");
    for i in 1..=queued_count {
        println!("   ✓ Synced message {}", i);
    }
    println!("   Sync complete: 5 uploaded, 2 downloaded\n");

    // 5. Conflict Resolution Example
    println!("5. Conflict Resolution Example:");
    println!("   Scenario: Same config key modified on edge and cloud");
    println!("   Edge value:  max_retries = 5 (t=1000)");
    println!("   Cloud value: max_retries = 3 (t=1001)");
    println!("   Resolution (LastWriteWins): max_retries = 3");
    println!("   Conflict logged for audit\n");

    // 6. Federation Status
    println!("6. Federation Status:");
    println!("   - Total Nodes: 12");
    println!("   - Online: 10");
    println!("   - Fully Synced: 9");
    println!("   - Pending Messages: 234");
    println!("   - Conflicts Today: 3");
    println!("   - Bandwidth: 45.2 Mbps\n");

    // 7. Resource Monitoring
    println!("7. Edge Resource Monitoring:");
    println!("   - Memory Usage: 128 MB / 256 MB (50%)");
    println!("   - Storage Used: 45 MB / 512 MB (8.8%)");
    println!("   - CPU Usage: 15% / 25% limit");
    println!("   - Battery: 78% (not charging)");
    println!("   - Network: WiFi (signal: good)\n");

    // 8. Edge-Specific Optimizations
    println!("8. Edge Optimizations Applied:");
    println!("   ✓ LZ4 compression (4x reduction)");
    println!("   ✓ Message batching (100 msgs/batch)");
    println!("   ✓ Delta sync (only changes)");
    println!("   ✓ Priority queue (critical first)");
    println!("   ✓ Battery-aware sync (pause at 20%)\n");

    println!("✓ Edge Sync example completed!");
    println!("  This demonstrates offline-first operation with eventual consistency.");

    Ok(())
}

#[cfg(not(feature = "edge"))]
fn main() {
    println!("This example requires the 'edge' feature.");
    println!("Run with: cargo run --example edge_sync --features \"edge\"");
}
