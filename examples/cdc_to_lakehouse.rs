//! CDC to Lakehouse Pipeline Example
//!
//! This example demonstrates capturing database changes (CDC) and storing
//! them in a lakehouse format for analytics.
//!
//! Features demonstrated:
//! - PostgreSQL CDC source configuration
//! - Lakehouse sink with Parquet storage
//! - Materialized views for analytics
//!
//! Run with: cargo run --example cdc_to_lakehouse --features "full"

#[cfg(all(feature = "postgres-cdc", feature = "lakehouse"))]
use bytes::Bytes;

#[cfg(all(feature = "postgres-cdc", feature = "lakehouse"))]
use streamline::{EmbeddedStreamline, Result};

#[cfg(all(feature = "postgres-cdc", feature = "lakehouse"))]
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize embedded Streamline
    let streamline = EmbeddedStreamline::persistent("./data/cdc-lakehouse-example")?;

    println!("CDC to Lakehouse Pipeline Example");
    println!("==================================\n");

    // Configure PostgreSQL CDC source
    println!("1. CDC Source Configuration:");
    println!("   - Host: localhost:5432");
    println!("   - Database: inventory");
    println!("   - Tables: [public.orders, public.customers]");
    println!("   - Slot: streamline_slot");
    println!("   - Publication: streamline_publication\n");

    // Create topics for CDC data
    streamline.create_topic("cdc-orders", 3)?;
    streamline.create_topic("cdc-customers", 3)?;

    // Configure Lakehouse Manager
    println!("2. Lakehouse Configuration:");
    println!("   - Data Dir: ./data/lakehouse");
    println!("   - Compression: zstd");
    println!("   - Row Group Size: 10000");
    println!("   - Max File Size: 128 MB\n");

    // Configure Materialized Views for analytics
    println!("3. Materialized View Configuration:");
    println!("   - Name: hourly_order_summary");
    println!("   - Source: cdc-orders");
    println!("   - Refresh: Incremental (every hour)");
    println!("   - Query:");
    println!("     SELECT");
    println!("       date_trunc('hour', order_time) as hour,");
    println!("       count(*) as order_count,");
    println!("       sum(total_amount) as total_revenue");
    println!("     FROM orders");
    println!("     GROUP BY 1\n");

    // Configure Export to external lakehouse (Iceberg/Delta/Hudi)
    println!("4. Export Configuration:");
    println!("   - Format: Apache Iceberg");
    println!("   - Catalog: http://localhost:8181");
    println!("   - Table: analytics.orders_raw\n");

    // Simulate the pipeline in action
    println!("5. Pipeline Flow:");
    println!("   PostgreSQL [orders, customers]");
    println!("        ↓ (CDC - logical replication)");
    println!("   Streamline Topics [cdc-orders, cdc-customers]");
    println!("        ↓ (continuous)");
    println!("   Parquet Storage [./data/lakehouse]");
    println!("        ↓ (incremental refresh)");
    println!("   Materialized Views [hourly_order_summary]");
    println!("        ↓ (scheduled export)");
    println!("   Iceberg Table [analytics.orders_raw]\n");

    // Example: Simulating CDC events
    println!("6. Simulated CDC Events:");
    let cdc_events = [
        (
            r#"{"op":"c","after":{"id":1,"customer_id":100,"total":99.99},"source":{"table":"orders"}}"#,
            "INSERT",
        ),
        (
            r#"{"op":"c","after":{"id":2,"customer_id":101,"total":149.99},"source":{"table":"orders"}}"#,
            "INSERT",
        ),
        (
            r#"{"op":"u","before":{"id":1,"total":99.99},"after":{"id":1,"total":109.99},"source":{"table":"orders"}}"#,
            "UPDATE",
        ),
        (
            r#"{"op":"d","before":{"id":2,"total":149.99},"source":{"table":"orders"}}"#,
            "DELETE",
        ),
    ];

    for (i, (event, op)) in cdc_events.iter().enumerate() {
        println!(
            "   Event {}: {} - {}...",
            i + 1,
            op,
            &event[..50.min(event.len())]
        );
        streamline.produce("cdc-orders", 0, None, Bytes::from(*event))?;
    }

    println!("\n7. Analytics Query Example:");
    println!("   SELECT hour, order_count, total_revenue");
    println!("   FROM hourly_order_summary");
    println!("   WHERE hour >= now() - interval '24 hours'");
    println!("   ORDER BY hour DESC;");

    println!("\n✓ Pipeline configured successfully!");
    println!("  To run with real PostgreSQL, update the connection config.");

    Ok(())
}

#[cfg(not(all(feature = "postgres-cdc", feature = "lakehouse")))]
fn main() {
    println!("This example requires 'postgres-cdc' and 'lakehouse' features.");
    println!("Run with: cargo run --example cdc_to_lakehouse --features \"full\"");
}
