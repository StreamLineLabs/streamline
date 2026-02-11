//! Shell, benchmark, tune, and migrate CLI commands

use colored::Colorize;
use serde_json::json;
use streamline::Result;

use crate::cli_context::CliContext;
use crate::OutputFormat;

use super::MigrateCommands;

pub(super) fn handle_migrate_command(cmd: MigrateCommands, ctx: &CliContext) -> Result<()> {
    use streamline::cli_utils::migrate::{run_migration, MigrationConfig};

    match cmd {
        MigrateCommands::FromKafka {
            kafka_servers,
            topics,
            execute,
            with_data,
        } => {
            let config = MigrationConfig {
                kafka_bootstrap_servers: kafka_servers,
                streamline_data_dir: ctx.data_dir.clone(),
                migrate_data: with_data,
                topics: topics
                    .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
                    .unwrap_or_default(),
                dry_run: !execute,
                interactive: true,
            };

            let results = run_migration(&config)?;

            if matches!(ctx.format, OutputFormat::Json) {
                let json_results: Vec<serde_json::Value> = results
                    .iter()
                    .map(|r| {
                        json!({
                            "topic": r.topic,
                            "success": r.success,
                            "message": r.message,
                            "records_migrated": r.records_migrated
                        })
                    })
                    .collect();
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json_results)?
                );
            }
        }
        MigrateCommands::Autopilot {
            kafka_servers,
            topics,
            strategy,
            execute,
            parallelism,
        } => {
            use streamline::cli_utils::autopilot::{AutopilotConfig, MigrationAutopilot};

            let config = AutopilotConfig {
                source_bootstrap_servers: kafka_servers,
                topics: topics
                    .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
                    .unwrap_or_default(),
                parallelism,
                dry_run: !execute,
                shadow_mode: strategy == "shadow",
                ..AutopilotConfig::default()
            };

            let mut autopilot = MigrationAutopilot::new(config);

            println!("{}", "🚀 Migration Autopilot".bold().cyan());
            println!("  Strategy: {}", strategy);
            println!();

            match autopilot.run() {
                Ok(report) => {
                    println!();
                    println!(
                        "  {} Phase reached: {:?}",
                        if report.success {
                            "✓".green()
                        } else {
                            "⚠".yellow()
                        },
                        report.phase_reached
                    );
                    println!("  Topics planned:     {}", report.plan.topic_plans.len());
                    println!("  Messages replicated: {}", report.messages_replicated);
                    println!(
                        "  Duration:           {:.1}s",
                        report.duration.as_secs_f64()
                    );
                    if !report.errors.is_empty() {
                        println!("  Errors:");
                        for err in &report.errors {
                            println!("    {} {}", "✗".red(), err);
                        }
                    }
                }
                Err(e) => {
                    println!("{} Migration failed: {}", "✗".red(), e);
                }
            }
        }
    }

    Ok(())
}

/// Handle benchmark command
pub(super) fn handle_benchmark_command(
    profile: &str,
    messages: Option<u64>,
    message_size: Option<usize>,
    producers: Option<usize>,
    topic: &str,
    list_profiles: bool,
    ctx: &CliContext,
) -> Result<()> {
    use streamline::cli_utils::benchmark::{
        list_profiles as show_profiles, run_benchmark, BenchmarkProfile,
    };

    if list_profiles {
        show_profiles();
        return Ok(());
    }

    let mut config = if let Some(p) = BenchmarkProfile::parse(profile) {
        p.config()
    } else {
        ctx.error(&format!(
            "Unknown profile '{}'. Use --list-profiles to see available profiles.",
            profile
        ));
        return Ok(());
    };

    // Override with explicit options
    if let Some(m) = messages {
        config.num_messages = m;
    }
    if let Some(s) = message_size {
        config.message_size = s;
    }
    if let Some(p) = producers {
        config.num_producers = p;
    }
    config.topic_name = topic.to_string();
    config.data_dir = ctx.data_dir.clone();

    let results = run_benchmark(&config)?;

    match ctx.format {
        OutputFormat::Json => {
            let json = json!({
                "messages_produced": results.messages_produced,
                "bytes_produced": results.bytes_produced,
                "duration_ms": results.duration.as_millis(),
                "throughput_msgs_per_sec": results.throughput_msgs,
                "throughput_bytes_per_sec": results.throughput_bytes,
                "latency_p50_ms": results.latency_p50.map(|d| d.as_secs_f64() * 1000.0),
                "latency_p99_ms": results.latency_p99.map(|d| d.as_secs_f64() * 1000.0),
                "latency_p999_ms": results.latency_p999.map(|d| d.as_secs_f64() * 1000.0),
                "errors": results.errors
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&json)?
            );
        }
        _ => {
            results.print();
            results.print_insights(&config);
        }
    }

    Ok(())
}

/// Handle interactive REPL shell
pub(super) fn handle_shell_command(ctx: &CliContext) -> Result<()> {
    use std::io::{self, BufRead, Write};
    use streamline::{GroupCoordinator, TopicManager};

    println!("{}", "Streamline Interactive Shell".bold().cyan());
    println!(
        "{}",
        "Type 'help' for available commands, 'exit' to quit".dimmed()
    );
    println!();

    let manager = std::sync::Arc::new(TopicManager::new(&ctx.data_dir)?);
    let coordinator = GroupCoordinator::new(&ctx.data_dir, manager.clone())?;

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    loop {
        print!("{} ", "streamline>".green().bold());
        stdout.flush().ok();

        let mut line = String::new();
        if stdin.lock().read_line(&mut line).is_err() {
            break;
        }

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        let (cmd, args) = match parts.split_first() {
            Some((c, a)) => (*c, a),
            None => continue,
        };

        match cmd.to_lowercase().as_str() {
            "help" | "?" => {
                println!("{}", "Available commands:".bold());
                println!("  {} - List all topics", "topics".cyan());
                println!("  {} <topic> - Show topic details", "describe".cyan());
                println!(
                    "  {} <topic> [offset] [count] - Read messages",
                    "read".cyan()
                );
                println!("  {} - List consumer groups", "groups".cyan());
                println!("  {} <group> - Show group details", "group".cyan());
                println!("  {} <topic> - Show topic stats", "stats".cyan());
                println!("  {} - Show storage info", "storage".cyan());
                println!("  {} - Clear screen", "clear".cyan());
                println!("  {} / {} - Exit shell", "exit".cyan(), "quit".cyan());
                println!();
            }
            "exit" | "quit" | "q" => {
                println!("{}", "Goodbye!".dimmed());
                break;
            }
            "clear" | "cls" => {
                print!("\x1B[2J\x1B[1;1H");
                stdout.flush().ok();
            }
            "topics" | "ls" => {
                match manager.list_topics() {
                    Ok(topics) => {
                        if topics.is_empty() {
                            println!("{}", "No topics found".dimmed());
                        } else {
                            println!("{}", format!("Found {} topic(s):", topics.len()).bold());
                            for topic in topics {
                                println!(
                                    "  {} {} partition(s)",
                                    topic.name.cyan(),
                                    topic.num_partitions
                                );
                            }
                        }
                    }
                    Err(e) => println!("{}", format!("Error: {}", e).red()),
                }
                println!();
            }
            "describe" | "desc" => {
                if args.is_empty() {
                    println!("{}", "Usage: describe <topic>".yellow());
                } else {
                    match manager.get_topic_metadata(args[0]) {
                        Ok(meta) => {
                            println!("{}", format!("Topic: {}", meta.name).bold());
                            println!("  Partitions: {}", meta.num_partitions);
                            println!("  Replication: {}", meta.replication_factor);
                            println!("  Retention.ms: {}", meta.config.retention_ms);
                            println!("  Retention.bytes: {}", meta.config.retention_bytes);
                            for p in 0..meta.num_partitions {
                                let earliest = manager.earliest_offset(&meta.name, p).unwrap_or(0);
                                let latest = manager.latest_offset(&meta.name, p).unwrap_or(0);
                                println!(
                                    "  Partition {}: {} messages (offset {} to {})",
                                    p,
                                    (latest - earliest).max(0),
                                    earliest,
                                    latest
                                );
                            }
                        }
                        Err(e) => println!("{}", format!("Error: {}", e).red()),
                    }
                }
                println!();
            }
            "read" | "cat" => {
                if args.is_empty() {
                    println!("{}", "Usage: read <topic> [offset] [count]".yellow());
                } else {
                    let topic = args[0];
                    let offset: i64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
                    let count: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10);

                    match manager.read(topic, 0, offset, count) {
                        Ok(records) => {
                            if records.is_empty() {
                                println!("{}", "No messages found".dimmed());
                            } else {
                                for record in records {
                                    let value = String::from_utf8_lossy(&record.value);
                                    println!("[{}] {}", record.offset.to_string().cyan(), value);
                                }
                            }
                        }
                        Err(e) => println!("{}", format!("Error: {}", e).red()),
                    }
                }
                println!();
            }
            "groups" => {
                match coordinator.list_groups() {
                    Ok(groups) => {
                        if groups.is_empty() {
                            println!("{}", "No consumer groups found".dimmed());
                        } else {
                            println!("{}", format!("Found {} group(s):", groups.len()).bold());
                            for group in groups {
                                println!("  {}", group.cyan());
                            }
                        }
                    }
                    Err(e) => println!("{}", format!("Error: {}", e).red()),
                }
                println!();
            }
            "group" => {
                if args.is_empty() {
                    println!("{}", "Usage: group <group_id>".yellow());
                } else {
                    match coordinator.get_group(args[0]) {
                        Ok(Some(group)) => {
                            println!("{}", format!("Group: {}", group.group_id).bold());
                            println!("  State: {:?}", group.state);
                            println!("  Members: {}", group.members.len());
                            println!("  Generation: {}", group.generation_id);
                            if !group.offsets.is_empty() {
                                println!("  Committed offsets:");
                                for ((topic, partition), offset) in &group.offsets {
                                    println!("    {}:{} = {}", topic, partition, offset.offset);
                                }
                            }
                        }
                        Ok(None) => {
                            println!("{}", format!("Group '{}' not found", args[0]).yellow())
                        }
                        Err(e) => println!("{}", format!("Error: {}", e).red()),
                    }
                }
                println!();
            }
            "stats" => {
                if args.is_empty() {
                    println!("{}", "Usage: stats <topic>".yellow());
                } else {
                    match manager.get_topic_metadata(args[0]) {
                        Ok(meta) => {
                            let mut total_messages = 0i64;
                            for p in 0..meta.num_partitions {
                                let earliest = manager.earliest_offset(&meta.name, p).unwrap_or(0);
                                let latest = manager.latest_offset(&meta.name, p).unwrap_or(0);
                                total_messages += (latest - earliest).max(0);
                            }
                            println!("{}", format!("Stats for '{}':", meta.name).bold());
                            println!("  Total messages: {}", total_messages.to_string().cyan());
                            println!("  Partitions: {}", meta.num_partitions);
                        }
                        Err(e) => println!("{}", format!("Error: {}", e).red()),
                    }
                }
                println!();
            }
            "storage" => {
                println!("{}", "Storage Info:".bold());
                println!(
                    "  Data directory: {}",
                    ctx.data_dir.display().to_string().cyan()
                );
                if ctx.data_dir.exists() {
                    if let Ok(entries) = std::fs::read_dir(&ctx.data_dir) {
                        let count = entries.count();
                        println!("  Entries: {}", count);
                    }
                } else {
                    println!("  {}", "(directory does not exist)".dimmed());
                }
                println!();
            }
            _ => {
                println!("{} Unknown command: {}", "?".yellow(), cmd);
                println!("{}", "Type 'help' for available commands".dimmed());
                println!();
            }
        }
    }

    Ok(())
}

/// Handle system tuning command
pub(super) fn handle_tune_command(
    profile: &str,
    dry_run: bool,
    apply: bool,
    force: bool,
    script: bool,
    ctx: &CliContext,
) -> Result<()> {
    use streamline::cli_utils::tune::{
        format_tune_result, generate_tune_script, is_tuning_supported, run_tune, TuneConfig,
        TuneProfile,
    };

    // Parse profile
    let profile: TuneProfile = profile
        .parse()
        .map_err(|e| streamline::StreamlineError::Config(format!("Invalid profile: {}", e)))?;

    // If --script is requested, just output the script
    if script {
        let script_content = generate_tune_script(profile);
        println!("{}", script_content);
        return Ok(());
    }

    // Check platform support
    if !is_tuning_supported() {
        println!(
            "{}",
            "Warning: System tuning is only fully supported on Linux.".yellow()
        );
        println!(
            "{}",
            "Some parameters may not be available on this platform.".dimmed()
        );
        println!();
    }

    // Determine if this is a dry run
    // Default to dry run unless --apply is explicitly specified
    let is_dry_run = !apply || dry_run;

    if !is_dry_run {
        // Check for root permissions
        #[cfg(unix)]
        {
            if unsafe { libc::geteuid() != 0 } {
                println!(
                    "{}",
                    "Error: Root permissions required to apply changes.".red()
                );
                println!(
                    "{}",
                    "Run with sudo: sudo streamline-cli tune --profile {} --apply".dimmed()
                );
                return Ok(());
            }
        }

        // Confirmation
        if !ctx.skip_confirm {
            println!(
                "{}",
                "This will modify system kernel parameters.".yellow().bold()
            );
            println!("Profile: {}", profile.to_string().cyan());
            println!();

            let confirm = dialoguer::Confirm::new()
                .with_prompt("Do you want to continue?")
                .default(false)
                .interact()
                .unwrap_or(false);

            if !confirm {
                println!("{}", "Aborted.".dimmed());
                return Ok(());
            }
        }
    }

    let config = TuneConfig {
        profile,
        dry_run: is_dry_run,
        force,
    };

    // Run tuning
    let result = run_tune(config);

    // Display results (check if colored output is enabled by checking if colors are being used)
    let use_color = colored::control::SHOULD_COLORIZE.should_colorize();
    let output = format_tune_result(&result, use_color);
    println!("{}", output);

    // Summary
    if result.all_optimal() {
        println!(
            "{}",
            "All parameters are already optimally configured!"
                .green()
                .bold()
        );
    } else if is_dry_run {
        println!();
        println!("{}", "This was a dry run. To apply changes:".dimmed());
        println!(
            "  {}",
            format!(
                "sudo streamline-cli tune --profile {} --apply",
                result.profile
            )
            .cyan()
        );
    } else if result.failed > 0 {
        println!(
            "{}",
            format!("Warning: {} parameter(s) failed to apply.", result.failed).yellow()
        );
    }

    Ok(())
}
