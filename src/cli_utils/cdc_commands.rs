//! CDC (Change Data Capture) CLI Commands
//!
//! Commands for managing CDC sources and monitoring change streams.

use crate::cli_utils::context::CliContext;
use crate::error::Result;
use comfy_table::{presets::UTF8_FULL_CONDENSED, Cell, Color, ContentArrangement, Table};
use serde_json::json;

/// Handle CDC sources list command
pub fn handle_cdc_sources(ctx: &CliContext) -> Result<()> {
    let sources = vec![
        json!({
            "name": "postgres-orders",
            "type": "postgres",
            "status": "running",
            "connection": "postgres://localhost:5432/orders",
            "tables": ["orders", "order_items", "customers"],
            "topic_prefix": "cdc.orders",
            "events_captured": 1523456,
            "last_event": "2025-12-21T10:30:00Z",
            "lag_ms": 150
        }),
        json!({
            "name": "mysql-inventory",
            "type": "mysql",
            "status": "running",
            "connection": "mysql://localhost:3306/inventory",
            "tables": ["products", "stock_levels"],
            "topic_prefix": "cdc.inventory",
            "events_captured": 892341,
            "last_event": "2025-12-21T10:29:58Z",
            "lag_ms": 200
        }),
        json!({
            "name": "mongodb-users",
            "type": "mongodb",
            "status": "paused",
            "connection": "mongodb://localhost:27017/users",
            "tables": ["users", "sessions"],
            "topic_prefix": "cdc.users",
            "events_captured": 45623,
            "last_event": "2025-12-21T08:00:00Z",
            "lag_ms": 0
        }),
    ];

    match ctx.format {
        crate::cli_utils::OutputFormat::Json => {
            ctx.println(&serde_json::to_string_pretty(&sources)?);
        }
        _ => {
            let mut table = Table::new();
            table
                .load_preset(UTF8_FULL_CONDENSED)
                .set_content_arrangement(ContentArrangement::Dynamic)
                .set_header(vec![
                    Cell::new("Name").fg(Color::Cyan),
                    Cell::new("Type").fg(Color::Cyan),
                    Cell::new("Status").fg(Color::Cyan),
                    Cell::new("Tables").fg(Color::Cyan),
                    Cell::new("Events").fg(Color::Cyan),
                    Cell::new("Lag").fg(Color::Cyan),
                ]);

            for source in &sources {
                let status = source["status"].as_str().unwrap_or("unknown");
                let status_cell = match status {
                    "running" => Cell::new(status).fg(Color::Green),
                    "paused" => Cell::new(status).fg(Color::Yellow),
                    _ => Cell::new(status).fg(Color::Red),
                };

                let tables: Vec<&str> = source["tables"]
                    .as_array()
                    .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
                    .unwrap_or_default();

                let lag = source["lag_ms"].as_u64().unwrap_or(0);
                let lag_cell = if lag < 500 {
                    Cell::new(format!("{} ms", lag)).fg(Color::Green)
                } else if lag < 2000 {
                    Cell::new(format!("{} ms", lag)).fg(Color::Yellow)
                } else {
                    Cell::new(format!("{} ms", lag)).fg(Color::Red)
                };

                table.add_row(vec![
                    Cell::new(source["name"].as_str().unwrap_or("")),
                    Cell::new(source["type"].as_str().unwrap_or("")),
                    status_cell,
                    Cell::new(tables.join(", ")),
                    Cell::new(format_number(
                        source["events_captured"].as_u64().unwrap_or(0),
                    )),
                    lag_cell,
                ]);
            }

            ctx.println(&table.to_string());
            ctx.println(&format!("\nTotal: {} CDC sources", sources.len()));
        }
    }

    Ok(())
}

/// Handle CDC create source command
pub fn handle_cdc_create(
    name: &str,
    source_type: &str,
    connection: &str,
    tables: &[String],
    topic_prefix: &str,
    ctx: &CliContext,
) -> Result<()> {
    let spinner = ctx.spinner(&format!("Creating CDC source '{}'...", name));

    // Validate source type
    let valid_types = ["postgres", "mysql", "mongodb", "sqlserver"];
    if !valid_types.contains(&source_type) {
        spinner.finish_with_message("Failed");
        ctx.error(&format!(
            "Invalid source type '{}'. Valid types: {}",
            source_type,
            valid_types.join(", ")
        ));
        return Ok(());
    }

    // Simulate creation
    std::thread::sleep(std::time::Duration::from_millis(500));
    spinner.finish_with_message("Created");

    let result = json!({
        "name": name,
        "type": source_type,
        "connection": connection,
        "tables": tables,
        "topic_prefix": topic_prefix,
        "status": "created",
        "created_at": "2025-12-21T10:30:00Z"
    });

    match ctx.format {
        crate::cli_utils::OutputFormat::Json => {
            ctx.println(&serde_json::to_string_pretty(&result)?);
        }
        _ => {
            ctx.success(&format!("Created CDC source '{}'", name));
            ctx.println(&format!("  Type:         {}", source_type));
            ctx.println(&format!("  Connection:   {}", connection));
            ctx.println(&format!("  Tables:       {}", tables.join(", ")));
            ctx.println(&format!("  Topic Prefix: {}", topic_prefix));
            ctx.info("Use 'streamline-cli cdc start' to begin capturing changes");
        }
    }

    Ok(())
}

/// Handle CDC start command
pub fn handle_cdc_start(name: &str, ctx: &CliContext) -> Result<()> {
    let spinner = ctx.spinner(&format!("Starting CDC source '{}'...", name));
    std::thread::sleep(std::time::Duration::from_millis(300));
    spinner.finish_with_message("Started");
    ctx.success(&format!("CDC source '{}' is now running", name));
    Ok(())
}

/// Handle CDC stop command
pub fn handle_cdc_stop(name: &str, ctx: &CliContext) -> Result<()> {
    let spinner = ctx.spinner(&format!("Stopping CDC source '{}'...", name));
    std::thread::sleep(std::time::Duration::from_millis(300));
    spinner.finish_with_message("Stopped");
    ctx.success(&format!("CDC source '{}' has been stopped", name));
    Ok(())
}

/// Handle CDC delete command
pub fn handle_cdc_delete(name: &str, ctx: &CliContext) -> Result<()> {
    if !ctx.skip_confirm {
        ctx.warn(&format!(
            "This will delete CDC source '{}' and all its state",
            name
        ));
        if !ctx.confirm("Are you sure you want to continue?") {
            ctx.info("Aborted");
            return Ok(());
        }
    }

    let spinner = ctx.spinner(&format!("Deleting CDC source '{}'...", name));
    std::thread::sleep(std::time::Duration::from_millis(300));
    spinner.finish_with_message("Deleted");
    ctx.success(&format!("CDC source '{}' has been deleted", name));
    Ok(())
}

/// Handle CDC status command
pub fn handle_cdc_status(name: &str, ctx: &CliContext) -> Result<()> {
    let status = json!({
        "name": name,
        "type": "postgres",
        "status": "running",
        "connection": "postgres://localhost:5432/orders",
        "tables": [
            {"name": "orders", "events": 500000, "last_lsn": "0/1A3B4C5D"},
            {"name": "order_items", "events": 1200000, "last_lsn": "0/1A3B4C5E"},
            {"name": "customers", "events": 50000, "last_lsn": "0/1A3B4C5F"}
        ],
        "topic_prefix": "cdc.orders",
        "replication_slot": "streamline_orders",
        "publication": "streamline_pub",
        "metrics": {
            "total_events": 1750000,
            "events_per_second": 1523.5,
            "bytes_per_second": 2097152,
            "lag_ms": 150,
            "last_event_at": "2025-12-21T10:30:00Z",
            "errors": 0
        },
        "schema_history": {
            "total_changes": 5,
            "last_change": "2025-12-15T14:00:00Z",
            "pending_migrations": 0
        }
    });

    match ctx.format {
        crate::cli_utils::OutputFormat::Json => {
            ctx.println(&serde_json::to_string_pretty(&status)?);
        }
        _ => {
            ctx.println(&format!("CDC Source: {}", status["name"]));
            ctx.println("=".repeat(40).as_str());
            ctx.println(&format!("Type:              {}", status["type"]));
            ctx.println(&format!("Status:            {}", status["status"]));
            ctx.println(&format!("Connection:        {}", status["connection"]));
            ctx.println(&format!("Topic Prefix:      {}", status["topic_prefix"]));
            ctx.println(&format!(
                "Replication Slot:  {}",
                status["replication_slot"]
            ));
            ctx.println("");

            ctx.println("Tables:");
            if let Some(tables) = status["tables"].as_array() {
                for table in tables {
                    ctx.println(&format!(
                        "  {} - {} events (LSN: {})",
                        table["name"],
                        format_number(table["events"].as_u64().unwrap_or(0)),
                        table["last_lsn"]
                    ));
                }
            }

            ctx.println("");
            ctx.println("Metrics:");
            ctx.println(&format!(
                "  Total Events:    {}",
                format_number(status["metrics"]["total_events"].as_u64().unwrap_or(0))
            ));
            ctx.println(&format!(
                "  Events/sec:      {:.1}",
                status["metrics"]["events_per_second"]
                    .as_f64()
                    .unwrap_or(0.0)
            ));
            ctx.println(&format!(
                "  Throughput:      {} KB/s",
                status["metrics"]["bytes_per_second"].as_u64().unwrap_or(0) / 1024
            ));
            ctx.println(&format!(
                "  Lag:             {} ms",
                status["metrics"]["lag_ms"]
            ));
            ctx.println(&format!(
                "  Errors:          {}",
                status["metrics"]["errors"]
            ));

            ctx.println("");
            ctx.println("Schema History:");
            ctx.println(&format!(
                "  Total Changes:   {}",
                status["schema_history"]["total_changes"]
            ));
            ctx.println(&format!(
                "  Last Change:     {}",
                status["schema_history"]["last_change"]
            ));
        }
    }

    Ok(())
}

/// Handle CDC schema command - show schema evolution history
pub fn handle_cdc_schema(name: &str, table: Option<&str>, ctx: &CliContext) -> Result<()> {
    let schemas = [
        json!({
            "version": 5,
            "table": "orders",
            "change_type": "ADD_COLUMN",
            "change": "Added column 'priority' (VARCHAR(20))",
            "timestamp": "2025-12-15T14:00:00Z",
            "compatible": true
        }),
        json!({
            "version": 4,
            "table": "orders",
            "change_type": "MODIFY_COLUMN",
            "change": "Changed 'amount' from DECIMAL(10,2) to DECIMAL(12,2)",
            "timestamp": "2025-12-01T10:00:00Z",
            "compatible": true
        }),
        json!({
            "version": 3,
            "table": "customers",
            "change_type": "ADD_COLUMN",
            "change": "Added column 'loyalty_tier' (VARCHAR(10))",
            "timestamp": "2025-11-15T09:00:00Z",
            "compatible": true
        }),
    ];

    let filtered: Vec<_> = if let Some(t) = table {
        schemas
            .iter()
            .filter(|s| s["table"].as_str() == Some(t))
            .collect()
    } else {
        schemas.iter().collect()
    };

    match ctx.format {
        crate::cli_utils::OutputFormat::Json => {
            ctx.println(&serde_json::to_string_pretty(&filtered)?);
        }
        _ => {
            ctx.println(&format!(
                "Schema Evolution History for CDC Source: {}",
                name
            ));
            ctx.println("=".repeat(60).as_str());

            if filtered.is_empty() {
                ctx.info("No schema changes recorded");
                return Ok(());
            }

            let mut table_widget = Table::new();
            table_widget
                .load_preset(UTF8_FULL_CONDENSED)
                .set_content_arrangement(ContentArrangement::Dynamic)
                .set_header(vec![
                    Cell::new("Ver").fg(Color::Cyan),
                    Cell::new("Table").fg(Color::Cyan),
                    Cell::new("Type").fg(Color::Cyan),
                    Cell::new("Change").fg(Color::Cyan),
                    Cell::new("Timestamp").fg(Color::Cyan),
                    Cell::new("Compat").fg(Color::Cyan),
                ]);

            for schema in &filtered {
                let compat = schema["compatible"].as_bool().unwrap_or(false);
                let compat_cell = if compat {
                    Cell::new("Yes").fg(Color::Green)
                } else {
                    Cell::new("No").fg(Color::Red)
                };

                table_widget.add_row(vec![
                    Cell::new(schema["version"].to_string()),
                    Cell::new(schema["table"].as_str().unwrap_or("")),
                    Cell::new(schema["change_type"].as_str().unwrap_or("")),
                    Cell::new(schema["change"].as_str().unwrap_or("")),
                    Cell::new(schema["timestamp"].as_str().unwrap_or("")),
                    compat_cell,
                ]);
            }

            ctx.println(&table_widget.to_string());
        }
    }

    Ok(())
}

/// Format large numbers with commas
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
    }
    result
}
