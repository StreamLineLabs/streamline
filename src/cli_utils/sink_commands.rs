//! CLI commands for sink connector management

#[cfg(feature = "iceberg")]
pub use super::context::{CliContext, OutputFormat};
#[cfg(feature = "iceberg")]
use crate::error::{Result, StreamlineError};
#[cfg(feature = "iceberg")]
use crate::sink::SinkManager;
#[cfg(feature = "iceberg")]
use crate::storage::TopicManager;
#[cfg(feature = "iceberg")]
use colored::Colorize;
#[cfg(feature = "iceberg")]
use comfy_table::presets::UTF8_FULL_CONDENSED;
#[cfg(feature = "iceberg")]
use comfy_table::{Cell, Color, ContentArrangement, Table};
#[cfg(feature = "iceberg")]
use serde_json::json;
#[cfg(feature = "iceberg")]
use std::io::Write;
#[cfg(feature = "iceberg")]
use std::sync::Arc;

#[cfg(feature = "iceberg")]
#[allow(clippy::too_many_arguments)]
/// Handle sink create command
pub async fn handle_sink_create(
    name: String,
    sink_type: String,
    topics: Vec<String>,
    catalog_uri: String,
    catalog_type: String,
    namespace: String,
    table: String,
    commit_interval_ms: u64,
    max_batch_size: usize,
    partitioning: String,
    start: bool,
    ctx: &CliContext,
) -> Result<()> {
    use crate::sink::config::{
        CatalogType, IcebergSinkConfig, PartitioningConfig, PartitioningStrategy, SinkConfig,
        SinkType, TimeGranularity,
    };

    let data_dir = &ctx.data_dir;
    std::fs::create_dir_all(data_dir)?;

    let topic_manager = Arc::new(TopicManager::new(data_dir)?);
    let sink_manager = SinkManager::new(topic_manager);

    // Validate sink type
    if sink_type != "iceberg" {
        return Err(StreamlineError::Config(format!(
            "Unsupported sink type: {}. Only 'iceberg' is currently supported.",
            sink_type
        )));
    }

    // Parse catalog type
    let catalog_type_enum = match catalog_type.as_str() {
        "rest" => CatalogType::Rest,
        "hive" => {
            ctx.warn("Hive catalog support is not yet available. The sink will fail to start. Use 'rest' catalog type instead.");
            CatalogType::Hive
        }
        "glue" => {
            ctx.warn("Glue catalog support is not yet available. The sink will fail to start. Use 'rest' catalog type instead.");
            CatalogType::Glue
        }
        _ => {
            return Err(StreamlineError::Config(format!(
                "Invalid catalog type: {}. Must be one of: rest, hive, glue",
                catalog_type
            )))
        }
    };

    // Parse partitioning strategy
    let partitioning_config = match partitioning.as_str() {
        "none" => PartitioningConfig {
            strategy: PartitioningStrategy::None,
            field: None,
            time_granularity: TimeGranularity::Hour,
        },
        "time_based_hour" => PartitioningConfig {
            strategy: PartitioningStrategy::TimeBasedHour,
            field: None,
            time_granularity: TimeGranularity::Hour,
        },
        "time_based_day" => PartitioningConfig {
            strategy: PartitioningStrategy::TimeBasedDay,
            field: None,
            time_granularity: TimeGranularity::Day,
        },
        "time_based_month" => PartitioningConfig {
            strategy: PartitioningStrategy::TimeBasedMonth,
            field: None,
            time_granularity: TimeGranularity::Month,
        },
        _ => {
            return Err(StreamlineError::Config(format!(
                "Invalid partitioning strategy: {}. Must be one of: none, time_based_hour, time_based_day, time_based_month",
                partitioning
            )))
        }
    };

    // Create Iceberg sink config
    let iceberg_config = IcebergSinkConfig {
        catalog_uri,
        catalog_type: catalog_type_enum,
        namespace,
        table: table.clone(),
        commit_interval_ms,
        max_batch_size,
        partitioning: partitioning_config,
        catalog_properties: std::collections::HashMap::new(),
        output_dir: "./data/iceberg".to_string(),
        warehouse: None,                      // Not needed for REST catalog
        aws_region: None,                     // Not needed for REST catalog
        compression: Default::default(),      // Snappy by default
        max_retries: 3,                       // Default retry count
        retry_delay_ms: 1000,                 // 1 second initial delay
        schema_evolution: Default::default(), // Strict by default
    };

    // Create sink config
    let config = SinkConfig {
        name: name.clone(),
        sink_type: SinkType::Iceberg,
        topics: topics.clone(),
        config: serde_json::to_value(&iceberg_config)
            .map_err(|e| StreamlineError::Config(format!("Failed to serialize config: {}", e)))?,
    };

    // Create the sink
    sink_manager.create_sink(config).await?;

    // Start if requested
    if start {
        sink_manager.start_sink(&name).await?;
        ctx.success(&format!("Created and started sink '{}'", name));
    } else {
        ctx.success(&format!("Created sink '{}'", name));
    }

    println!();
    println!("Sink Configuration:");
    println!("  Name:         {}", name.cyan());
    println!("  Type:         {}", "Iceberg".cyan());
    println!("  Topics:       {}", topics.join(", ").cyan());
    println!("  Table:        {}", table.cyan());
    println!(
        "  Status:       {}",
        if start {
            "Running".green()
        } else {
            "Stopped".yellow()
        }
    );
    println!();
    println!("Usage:");
    println!("  {} sink status {}", "streamline-cli".cyan(), name.cyan());
    println!("  {} sink stop {}", "streamline-cli".cyan(), name.cyan());

    Ok(())
}

#[cfg(feature = "iceberg")]
/// Handle sink list command
pub async fn handle_sink_list(ctx: &CliContext) -> Result<()> {
    use crate::sink::SinkStatus;

    let data_dir = &ctx.data_dir;
    std::fs::create_dir_all(data_dir)?;

    let topic_manager = Arc::new(TopicManager::new(data_dir)?);
    let sink_manager = SinkManager::new(topic_manager);

    let sinks = sink_manager.list_sinks_detailed().await;

    match ctx.format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&sinks)?);
        }
        _ => {
            if sinks.is_empty() {
                ctx.info("No sink connectors found.");
                println!();
                println!("Create one with:");
                println!(
                    "  {} sink create <name> --topics events --catalog-uri http://localhost:8181 --table events",
                    "streamline-cli".cyan()
                );
            } else {
                let mut table = Table::new();
                table.load_preset(UTF8_FULL_CONDENSED);
                table.set_content_arrangement(ContentArrangement::Dynamic);
                table.set_header(vec![
                    Cell::new("Name").fg(Color::Cyan),
                    Cell::new("Type").fg(Color::Cyan),
                    Cell::new("Topics").fg(Color::Cyan),
                    Cell::new("Status").fg(Color::Cyan),
                    Cell::new("Records").fg(Color::Cyan),
                ]);

                for sink in sinks {
                    let status_color = match sink.status {
                        SinkStatus::Running => Color::Green,
                        SinkStatus::Stopped => Color::Yellow,
                        SinkStatus::Failed => Color::Red,
                        _ => Color::DarkGrey,
                    };

                    table.add_row(vec![
                        Cell::new(&sink.name),
                        Cell::new(format!("{:?}", sink.sink_type)),
                        Cell::new(sink.topics.join(", ")),
                        Cell::new(format!("{}", sink.status)).fg(status_color),
                        Cell::new(sink.metrics.records_processed.to_string()),
                    ]);
                }

                println!("{}", table);
            }
        }
    }

    Ok(())
}

#[cfg(feature = "iceberg")]
/// Handle sink status command
pub async fn handle_sink_status(name: String, json: bool, ctx: &CliContext) -> Result<()> {
    use crate::sink::SinkStatus;

    let data_dir = &ctx.data_dir;
    std::fs::create_dir_all(data_dir)?;

    let topic_manager = Arc::new(TopicManager::new(data_dir)?);
    let sink_manager = SinkManager::new(topic_manager);

    let status = sink_manager.get_status(&name).await?;
    let metrics = sink_manager.get_metrics(&name).await?;

    if json || ctx.format == OutputFormat::Json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "name": name,
                "status": status,
                "metrics": metrics
            }))?
        );
    } else {
        println!("{}", format!("Sink: {}", name).bold().cyan());
        println!();

        let status_color = match status {
            SinkStatus::Running => Color::Green,
            SinkStatus::Stopped => Color::Yellow,
            SinkStatus::Failed => Color::Red,
            _ => Color::DarkGrey,
        };

        let status_text = format!("{}", status);
        println!(
            "  Status:               {}",
            match status_color {
                Color::Green => status_text.green(),
                Color::Yellow => status_text.yellow(),
                Color::Red => status_text.red(),
                _ => status_text.dimmed(),
            }
        );
        println!(
            "  Records Processed:    {}",
            metrics.records_processed.to_string().cyan()
        );
        println!(
            "  Records Failed:       {}",
            metrics.records_failed.to_string().cyan()
        );
        println!(
            "  Bytes Processed:      {}",
            format_bytes(metrics.bytes_processed)
        );
        if metrics.last_commit_timestamp > 0 {
            let timestamp =
                chrono::DateTime::from_timestamp(metrics.last_commit_timestamp / 1000, 0);
            if let Some(dt) = timestamp {
                println!(
                    "  Last Commit:          {}",
                    dt.format("%Y-%m-%d %H:%M:%S").to_string().cyan()
                );
            }
        }

        if !metrics.committed_offsets.is_empty() {
            println!();
            println!("  Committed Offsets:");
            for (partition, offset) in &metrics.committed_offsets {
                println!("    {}: {}", partition, offset);
            }
        }

        if let Some(error) = &metrics.error_message {
            println!();
            println!("  Error: {}", error.red());
        }
    }

    Ok(())
}

#[cfg(feature = "iceberg")]
/// Handle sink start command
pub async fn handle_sink_start(name: String, ctx: &CliContext) -> Result<()> {
    let data_dir = &ctx.data_dir;
    std::fs::create_dir_all(data_dir)?;

    let topic_manager = Arc::new(TopicManager::new(data_dir)?);
    let sink_manager = SinkManager::new(topic_manager);

    sink_manager.start_sink(&name).await?;
    ctx.success(&format!("Started sink '{}'", name));

    Ok(())
}

#[cfg(feature = "iceberg")]
/// Handle sink stop command
pub async fn handle_sink_stop(name: String, ctx: &CliContext) -> Result<()> {
    let data_dir = &ctx.data_dir;
    std::fs::create_dir_all(data_dir)?;

    let topic_manager = Arc::new(TopicManager::new(data_dir)?);
    let sink_manager = SinkManager::new(topic_manager);

    sink_manager.stop_sink(&name).await?;
    ctx.success(&format!("Stopped sink '{}'", name));

    Ok(())
}

#[cfg(feature = "iceberg")]
/// Handle sink delete command
pub async fn handle_sink_delete(name: String, yes: bool, ctx: &CliContext) -> Result<()> {
    let data_dir = &ctx.data_dir;
    std::fs::create_dir_all(data_dir)?;

    let topic_manager = Arc::new(TopicManager::new(data_dir)?);
    let sink_manager = SinkManager::new(topic_manager);

    if !yes {
        print!("Are you sure you want to delete sink '{}'? [y/N] ", name);
        std::io::stdout().flush()?;
        let mut response = String::new();
        std::io::stdin().read_line(&mut response)?;
        if !response.trim().eq_ignore_ascii_case("y") {
            ctx.info("Cancelled.");
            return Ok(());
        }
    }

    sink_manager.delete_sink(&name).await?;
    ctx.success(&format!("Deleted sink '{}'", name));

    Ok(())
}

#[cfg(feature = "iceberg")]
/// Format bytes into human-readable format
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    format!("{:.2} {}", size, UNITS[unit_idx])
        .cyan()
        .to_string()
}
