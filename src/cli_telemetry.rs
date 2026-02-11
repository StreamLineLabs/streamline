use colored::Colorize;
use dialoguer::Confirm;
use std::sync::Arc;

use crate::cli_context::CliContext;
use streamline::Result;

use super::TelemetryCommands;

#[cfg(feature = "analytics")]
pub(super) fn handle_query_command(
    query: &str,
    output_format: &str,
    max_rows: Option<usize>,
    enable_cache: bool,
    timeout: u64,
    explain: bool,
    ctx: &CliContext,
) -> Result<()> {
    use comfy_table::{presets::UTF8_FULL_CONDENSED, Cell, Color, ContentArrangement, Table};
    use serde_json::json;
    use streamline::{AnalyticsQueryOptions, DuckDBEngine, StreamlineError, TopicManager};

    // Create topic manager and analytics engine
    let manager = std::sync::Arc::new(TopicManager::new(&ctx.data_dir)?);
    let engine = DuckDBEngine::new(manager)?;

    // Create tokio runtime for async operations
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|e| StreamlineError::Server(format!("Failed to create runtime: {}", e)))?;

    // Handle EXPLAIN mode
    if explain {
        let result = runtime.block_on(async { engine.explain_query(query).await })?;

        match output_format {
            "json" => {
                println!("{}", serde_json::to_string_pretty(&result)?);
            }
            _ => {
                println!("{}", "Query Plan".bold().cyan());
                println!("{}", "=".repeat(60));
                if !result.referenced_topics.is_empty() {
                    println!(
                        "{} {}",
                        "Topics:".bold(),
                        result.referenced_topics.join(", ")
                    );
                }
                println!("{} {}", "Rewritten SQL:".bold(), result.rewritten_sql);
                println!();
                for line in &result.plan {
                    println!("  {}", line);
                }
            }
        }
        return Ok(());
    }

    // Configure query options
    let options = AnalyticsQueryOptions {
        max_rows,
        enable_cache,
        cache_ttl_seconds: 60,
        timeout_ms: Some(timeout * 1000),
        offset: 0,
    };

    // Execute query
    let result = runtime.block_on(async { engine.execute_query(query, options).await })?;

    // Display results
    match output_format {
        "json" => {
            let json = json!({
                "columns": result.columns,
                "rows": result.rows,
                "row_count": result.row_count,
                "execution_time_ms": result.execution_time_ms,
                "from_cache": result.from_cache
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&json)?
            );
        }
        _ => {
            // Text format - display as a table
            if result.row_count == 0 {
                println!("{}", "No results".dimmed());
                return Ok(());
            }

            let mut table = Table::new();
            table
                .load_preset(UTF8_FULL_CONDENSED)
                .set_content_arrangement(ContentArrangement::Dynamic);

            // Header row
            let header_cells: Vec<Cell> = result
                .columns
                .iter()
                .map(|col| Cell::new(col).fg(Color::Cyan))
                .collect();
            table.set_header(header_cells);

            // Data rows
            for row in &result.rows {
                let cells: Vec<String> = row
                    .values
                    .iter()
                    .map(|v| match v {
                        serde_json::Value::Null => "NULL".dimmed().to_string(),
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Number(n) => n.to_string(),
                        serde_json::Value::Bool(b) => b.to_string(),
                        _ => serde_json::to_string(&v).unwrap_or_else(|_| "?".to_string()),
                    })
                    .collect();
                table.add_row(cells);
            }

            println!("{}", table);
            println!();
            println!(
                "{} {} row(s) in {} ms{}",
                "Query returned".dimmed(),
                result.row_count.to_string().bold(),
                result.execution_time_ms.to_string().bold(),
                if result.from_cache {
                    " (from cache)".dimmed().to_string()
                } else {
                    "".to_string()
                }
            );
        }
    }

    Ok(())
}

/// Handle telemetry command
pub(super) fn handle_telemetry_command(cmd: TelemetryCommands, ctx: &CliContext) -> Result<()> {
    use streamline::telemetry::{
        EnabledFeatures, SystemInfo as TelemetrySystemInfo, TelemetryReport, UsageStats,
    };
    use streamline::{GroupCoordinator, TopicManager};
    use uuid::Uuid;

    match cmd {
        TelemetryCommands::Show { json } => {
            // Generate a telemetry report showing what would be sent
            let manager = TopicManager::new(&ctx.data_dir)?;
            let topics = manager.list_topics()?;

            let mut partition_count = 0u32;
            for topic in &topics {
                partition_count += topic.num_partitions as u32;
            }

            // Get consumer group count
            let consumer_group_count = {
                let manager_arc = Arc::new(TopicManager::new(&ctx.data_dir)?);
                let consumer_offsets_path = ctx.data_dir.join("consumer_offsets");
                match GroupCoordinator::new(consumer_offsets_path, manager_arc) {
                    Ok(coord) => coord.list_groups().map(|g| g.len() as u32).unwrap_or(0),
                    Err(_) => 0,
                }
            };

            // Load or generate installation ID
            let id_file = ctx.data_dir.join(".telemetry_id");
            let installation_id = if id_file.exists() {
                std::fs::read_to_string(&id_file)
                    .map(|s| s.trim().to_string())
                    .unwrap_or_else(|_| "<not configured>".to_string())
            } else {
                "<not configured - telemetry disabled>".to_string()
            };

            let report = TelemetryReport {
                schema_version: 1,
                installation_id,
                version: env!("CARGO_PKG_VERSION").to_string(),
                timestamp: chrono::Utc::now().to_rfc3339(),
                system: TelemetrySystemInfo::default(),
                usage: UsageStats {
                    uptime_hours: 0, // Would need server connection for real value
                    topic_count: topics.len() as u32,
                    partition_count,
                    throughput_bucket: "unknown".to_string(), // Would need server metrics
                    storage_gb: 0,                            // Would calculate from data dir
                    consumer_group_count,
                    connected_clients: 0, // Would need server connection
                },
                features: EnabledFeatures::default(),
            };

            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("{}", "Telemetry Report Preview".bold().cyan());
                println!("{}", "=".repeat(50));
                println!();
                println!("{}", "What would be sent:".bold());
                println!();
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report).unwrap_or_default()
                );
                println!();
                println!(
                    "{}",
                    "Note: This is a preview. To enable telemetry, start the server with".dimmed()
                );
                println!("{}", "      --telemetry-enabled=true".dimmed());
            }
        }

        TelemetryCommands::Config => {
            // Show telemetry configuration
            let id_file = ctx.data_dir.join(".telemetry_id");
            let has_id = id_file.exists();
            let installation_id = if has_id {
                std::fs::read_to_string(&id_file)
                    .map(|s| s.trim().to_string())
                    .ok()
            } else {
                None
            };

            println!("{}", "Telemetry Configuration".bold().cyan());
            println!("{}", "=".repeat(50));
            println!();

            println!(
                "{:25} {}",
                "Status:".bold(),
                if has_id {
                    "Configured (opt-in)".green()
                } else {
                    "Not configured (disabled by default)".yellow()
                }
            );

            if let Some(id) = installation_id {
                println!("{:25} {}", "Installation ID:".bold(), id);
            }

            println!(
                "{:25} {}",
                "ID file:".bold(),
                id_file.display().to_string().dimmed()
            );
            println!(
                "{:25} {}",
                "Endpoint:".bold(),
                streamline::telemetry::DEFAULT_TELEMETRY_ENDPOINT.dimmed()
            );
            println!(
                "{:25} {} hours",
                "Reporting interval:".bold(),
                streamline::telemetry::DEFAULT_INTERVAL_HOURS
            );

            println!();
            println!("{}", "To enable telemetry:".bold());
            println!("  Start server with: --telemetry-enabled=true");
            println!();
            println!("{}", "To disable telemetry:".bold());
            println!("  Start server with: --telemetry-enabled=false");
            println!("  Or run: streamline-cli telemetry reset-id");
        }

        TelemetryCommands::ResetId { yes } => {
            let id_file = ctx.data_dir.join(".telemetry_id");

            if !id_file.exists() {
                ctx.info("No telemetry ID configured. Nothing to reset.");
                return Ok(());
            }

            // Read current ID for display
            let current_id = std::fs::read_to_string(&id_file)
                .map(|s| s.trim().to_string())
                .unwrap_or_else(|_| "<unknown>".to_string());

            if !yes && !ctx.skip_confirm {
                println!("{}", "Reset Telemetry Installation ID".bold().yellow());
                println!();
                println!("Current ID: {}", current_id);
                println!();
                println!("This will:");
                println!(
                    "  - Delete your current installation ID (unlinking historical telemetry data)"
                );
                println!(
                    "  - A new ID will be generated on next server start (if telemetry is enabled)"
                );
                println!();

                let confirmed = Confirm::new()
                    .with_prompt("Continue with reset?")
                    .default(false)
                    .interact()
                    .unwrap_or(false);

                if !confirmed {
                    ctx.info("Reset cancelled.");
                    return Ok(());
                }
            }

            // Generate a new ID
            let new_id = Uuid::new_v4().to_string();
            std::fs::write(&id_file, &new_id)?;

            ctx.success(&format!(
                "Installation ID reset successfully\n  Old ID: {}\n  New ID: {}",
                current_id, new_id
            ));
        }

        TelemetryCommands::Privacy => {
            // Display privacy information
            println!("{}", "Streamline Telemetry Privacy".bold().cyan());
            println!("{}", "=".repeat(50));
            println!();

            println!("{}", "Privacy Principles".bold());
            println!();
            println!(
                "  1. {} - Disabled by default, requires explicit consent",
                "Opt-In Only".green()
            );
            println!(
                "  2. {} - All collected data is documented and viewable",
                "Transparent".green()
            );
            println!(
                "  3. {} - No PII, no message content, no IP addresses",
                "Anonymous".green()
            );
            println!(
                "  4. {} - Only collect what's needed for product decisions",
                "Minimal".green()
            );
            println!(
                "  5. {} - Easy to disable, data deletion available",
                "Controllable".green()
            );
            println!();

            println!("{}", "What We Collect".bold());
            println!();
            println!("  - OS, architecture, Streamline version");
            println!("  - Topic/partition counts (not names)");
            println!("  - Throughput buckets (not actual values)");
            println!("  - Enabled features (TLS, auth, clustering)");
            println!();

            println!("{}", "What We DON'T Collect".bold());
            println!();
            println!("  - Message content, keys, or topic names");
            println!("  - IP addresses or hostnames");
            println!("  - User credentials or configuration details");
            println!("  - Personally identifiable information");
            println!();

            println!("{}", "Commands".bold());
            println!();
            println!("  streamline-cli telemetry show     View what data would be sent");
            println!("  streamline-cli telemetry config   View telemetry configuration");
            println!("  streamline-cli telemetry reset-id Reset your installation ID");
            println!();

            println!("{}", "Documentation".bold());
            println!("  See docs/TELEMETRY.md for full documentation");
        }
    }

    Ok(())
}
