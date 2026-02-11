//! Consumer group management CLI commands

use colored::Colorize;
use comfy_table::{presets::UTF8_FULL_CONDENSED, Cell, Color, ContentArrangement, Table};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use streamline::{consumer::LagCalculator, GroupCoordinator, Result, TopicManager};

use crate::cli_context::CliContext;
use crate::{format_delimited_row, GroupCommands, OutputFormat};

pub(crate) fn handle_group_command(cmd: GroupCommands, ctx: &CliContext) -> Result<()> {
    let data_dir = &ctx.data_dir;
    std::fs::create_dir_all(data_dir)?;

    let topic_manager = Arc::new(TopicManager::new(data_dir)?);
    let consumer_offsets_path = data_dir.join("consumer_offsets");
    let coordinator = GroupCoordinator::new(consumer_offsets_path, topic_manager.clone())?;

    match cmd {
        GroupCommands::List => {
            let groups = coordinator.list_groups()?;

            match ctx.format {
                OutputFormat::Json => {
                    let data: Vec<_> = groups
                        .iter()
                        .map(|group_id| {
                            let state = coordinator
                                .get_group(group_id)
                                .ok()
                                .flatten()
                                .map(|g| format!("{:?}", g.state))
                                .unwrap_or_else(|| "Unknown".to_string());
                            json!({ "group_id": group_id, "state": state })
                        })
                        .collect();
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&data)?
                    );
                }
                OutputFormat::Csv | OutputFormat::Tsv => {
                    // Print header
                    println!(
                        "{}",
                        format_delimited_row(ctx.format, &["group_id", "state"])
                    );
                    // Print data rows
                    for group_id in groups {
                        let state = coordinator
                            .get_group(&group_id)
                            .ok()
                            .flatten()
                            .map(|g| format!("{:?}", g.state))
                            .unwrap_or_else(|| "Unknown".to_string());
                        println!("{}", format_delimited_row(ctx.format, &[&group_id, &state]));
                    }
                }
                _ => {
                    if groups.is_empty() {
                        ctx.info("No consumer groups found.");
                    } else {
                        let mut table = Table::new();
                        table.load_preset(UTF8_FULL_CONDENSED);
                        table.set_content_arrangement(ContentArrangement::Dynamic);
                        table.set_header(vec![
                            Cell::new("Group ID").fg(Color::Cyan),
                            Cell::new("State").fg(Color::Cyan),
                        ]);

                        for group_id in groups {
                            let (state_str, state_color) =
                                if let Ok(Some(group)) = coordinator.get_group(&group_id) {
                                    match group.state {
                                        streamline::GroupState::Empty => ("Empty", Color::Yellow),
                                        streamline::GroupState::PreparingRebalance => {
                                            ("Preparing", Color::Yellow)
                                        }
                                        streamline::GroupState::CompletingRebalance => {
                                            ("Completing", Color::Yellow)
                                        }
                                        streamline::GroupState::Stable => ("Stable", Color::Green),
                                        streamline::GroupState::Dead => ("Dead", Color::Red),
                                    }
                                } else {
                                    ("Unknown", Color::DarkGrey)
                                };

                            table.add_row(vec![
                                Cell::new(&group_id),
                                Cell::new(state_str).fg(state_color),
                            ]);
                        }

                        println!("{}", table);
                    }
                }
            }
        }

        GroupCommands::Describe { group_id } => match coordinator.get_group(&group_id)? {
            Some(group) => {
                let state_str = match group.state {
                    streamline::GroupState::Empty => "Empty",
                    streamline::GroupState::PreparingRebalance => "PreparingRebalance",
                    streamline::GroupState::CompletingRebalance => "CompletingRebalance",
                    streamline::GroupState::Stable => "Stable",
                    streamline::GroupState::Dead => "Dead",
                };

                match ctx.format {
                    OutputFormat::Json => {
                        let members: Vec<_> = group
                            .members
                            .iter()
                            .map(|(id, m)| {
                                json!({
                                    "member_id": id,
                                    "client_id": m.client_id,
                                    "client_host": m.client_host,
                                    "subscriptions": m.subscriptions,
                                    "assignment": m.assignment
                                })
                            })
                            .collect();
                        let offsets: Vec<_> = group
                            .offsets
                            .iter()
                            .map(|((topic, partition), offset)| {
                                json!({
                                    "topic": topic,
                                    "partition": partition,
                                    "offset": offset.offset
                                })
                            })
                            .collect();
                        let info = json!({
                            "group_id": group.group_id,
                            "state": state_str,
                            "generation_id": group.generation_id,
                            "protocol_type": group.protocol_type,
                            "protocol_name": group.protocol_name,
                            "leader": group.leader,
                            "members": members,
                            "offsets": offsets
                        });
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&info)?
                        );
                    }
                    _ => {
                        println!("{}: {}", "Group".bold(), group.group_id);
                        println!("{}: {}", "State".bold(), state_str.green());
                        println!("{}: {}", "Generation".bold(), group.generation_id);
                        println!("{}: {}", "Protocol Type".bold(), group.protocol_type);
                        println!("{}: {}", "Protocol".bold(), group.protocol_name);
                        println!(
                            "{}: {}",
                            "Leader".bold(),
                            group
                                .leader
                                .as_deref()
                                .unwrap_or("None")
                                .to_string()
                                .dimmed()
                        );
                        println!();
                        println!(
                            "{} ({}):",
                            "Members".bold().underline(),
                            group.members.len()
                        );
                        for (member_id, member) in &group.members {
                            println!("  {}", member_id.cyan());
                            println!("    {}: {}", "Client ID".dimmed(), member.client_id);
                            println!("    {}: {}", "Host".dimmed(), member.client_host);
                            println!(
                                "    {}: {:?}",
                                "Subscriptions".dimmed(),
                                member.subscriptions
                            );
                            println!(
                                "    {}: {:?}",
                                "Assigned Partitions".dimmed(),
                                member.assignment
                            );
                        }
                        println!();
                        println!(
                            "{} ({}):",
                            "Committed Offsets".bold().underline(),
                            group.offsets.len()
                        );
                        for ((topic, partition), offset) in &group.offsets {
                            println!(
                                "  {} [{}]: {}",
                                topic.cyan(),
                                partition,
                                offset.offset.to_string().green()
                            );
                        }
                    }
                }
            }
            None => {
                ctx.warn(&format!("Group '{}' not found", group_id));
            }
        },

        GroupCommands::Delete { group_id } => {
            if !ctx.confirm(&format!("Delete consumer group '{}'?", group_id)) {
                ctx.info("Cancelled.");
                return Ok(());
            }

            coordinator.delete_group(&group_id)?;

            match ctx.format {
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "status": "deleted",
                            "group_id": group_id
                        }))?
                    );
                }
                _ => {
                    ctx.success(&format!("Deleted consumer group '{}'", group_id.bold()));
                }
            }
        }

        GroupCommands::Reset {
            group_id,
            topic,
            partition,
            to_earliest,
            to_latest,
            to_offset,
            execute,
        } => {
            // Determine reset strategy
            let strategy = if to_earliest {
                "earliest"
            } else if to_latest {
                "latest"
            } else if to_offset.is_some() {
                "offset"
            } else {
                return Err(streamline::StreamlineError::Config(
                    "Must specify one of: --to-earliest, --to-latest, or --to-offset".into(),
                ));
            };

            // Get topic metadata to find partitions
            let topic_metadata = topic_manager.get_topic_metadata(&topic)?;
            let partitions: Vec<i32> = if let Some(p) = partition {
                vec![p]
            } else {
                (0..topic_metadata.num_partitions).collect()
            };

            // Calculate target offsets for each partition
            let mut reset_plan: Vec<(i32, i64, i64)> = Vec::new(); // (partition, current, target)

            for p in &partitions {
                let current_offset = coordinator
                    .fetch_offset(&group_id, &topic, *p)?
                    .map(|o| o.offset)
                    .unwrap_or(-1);

                let target_offset = match strategy {
                    "earliest" => topic_manager.earliest_offset(&topic, *p)?,
                    "latest" => topic_manager.latest_offset(&topic, *p)?,
                    "offset" => to_offset.ok_or_else(|| streamline::StreamlineError::Config(
                        "offset required when strategy is 'offset'".into()
                    ))?,
                    _ => unreachable!(),
                };

                reset_plan.push((*p, current_offset, target_offset));
            }

            match ctx.format {
                OutputFormat::Json => {
                    let plan: Vec<_> = reset_plan
                        .iter()
                        .map(|(p, current, target)| {
                            json!({
                                "partition": p,
                                "current_offset": if *current >= 0 { json!(current) } else { json!(null) },
                                "target_offset": target
                            })
                        })
                        .collect();

                    if execute {
                        for (p, _, target) in &reset_plan {
                            coordinator.commit_offset(
                                &group_id,
                                &topic,
                                *p,
                                *target,
                                format!("CLI reset to {}", strategy),
                            )?;
                        }

                        println!(
                            "{}",
                            serde_json::to_string_pretty(&json!({
                                "status": "reset",
                                "group_id": group_id,
                                "topic": topic,
                                "strategy": strategy,
                                "partitions": plan
                            }))?
                        );
                    } else {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&json!({
                                "status": "dry-run",
                                "group_id": group_id,
                                "topic": topic,
                                "strategy": strategy,
                                "partitions": plan,
                                "note": "Use --execute to apply changes"
                            }))?
                        );
                    }
                }
                _ => {
                    println!(
                        "{} offset reset plan for group '{}' on topic '{}':",
                        if execute { "Executing" } else { "Planned" },
                        group_id.bold(),
                        topic.cyan()
                    );
                    println!();

                    let mut table = Table::new();
                    table.load_preset(UTF8_FULL_CONDENSED);
                    table.set_content_arrangement(ContentArrangement::Dynamic);
                    table.set_header(vec![
                        Cell::new("Partition").fg(Color::Cyan),
                        Cell::new("Current Offset").fg(Color::Cyan),
                        Cell::new("Target Offset").fg(Color::Cyan),
                        Cell::new("Change").fg(Color::Cyan),
                    ]);

                    for (p, current, target) in &reset_plan {
                        let current_str = if *current >= 0 {
                            current.to_string()
                        } else {
                            "none".to_string()
                        };
                        let change = target - current.max(&0);
                        let change_str = if change > 0 {
                            format!("+{}", change)
                        } else if change < 0 {
                            change.to_string()
                        } else {
                            "0".to_string()
                        };

                        table.add_row(vec![
                            Cell::new(p),
                            Cell::new(&current_str),
                            Cell::new(target).fg(Color::Green),
                            Cell::new(&change_str).fg(if change != 0 {
                                Color::Yellow
                            } else {
                                Color::DarkGrey
                            }),
                        ]);
                    }

                    println!("{}", table);
                    println!();

                    if execute {
                        for (p, _, target) in &reset_plan {
                            coordinator.commit_offset(
                                &group_id,
                                &topic,
                                *p,
                                *target,
                                format!("CLI reset to {}", strategy),
                            )?;
                        }
                        ctx.success(&format!(
                            "Reset offsets for {} partition(s) to {}",
                            reset_plan.len(),
                            strategy
                        ));
                    } else {
                        ctx.warn("Dry run - no changes made. Use --execute to apply.");
                    }
                }
            }
        }

        GroupCommands::Watch {
            group,
            alert_lag,
            interval,
            exit_on_alert,
        } => {
            use std::collections::HashMap;

            println!(
                "{}",
                "Consumer Group Watch Mode - Press Ctrl+C to stop"
                    .bold()
                    .cyan()
            );
            if let Some(threshold) = alert_lag {
                println!(
                    "{}",
                    format!("Alert threshold: {} messages", threshold).yellow()
                );
            }
            println!("{}", "─".repeat(60).dimmed());
            println!();

            let mut iteration = 0u64;
            let mut prev_lags: HashMap<String, i64> = HashMap::new();
            let lag_calculator = LagCalculator::new(topic_manager.clone());

            loop {
                let groups = coordinator.list_groups()?;
                let groups_to_watch: Vec<_> = if let Some(ref id) = group {
                    groups.iter().filter(|g| *g == id).cloned().collect()
                } else {
                    groups.clone()
                };

                if groups_to_watch.is_empty() {
                    if let Some(ref id) = group {
                        ctx.error(&format!("Consumer group '{}' not found", id));
                        return Ok(());
                    }
                    println!("{}", "No consumer groups found".dimmed());
                } else {
                    // Clear previous output
                    if iteration > 0 {
                        print!("\x1B[{}A", groups_to_watch.len() + 4);
                    }

                    let now = chrono::Utc::now();
                    println!(
                        "{} {}",
                        "Last updated:".dimmed(),
                        now.format("%H:%M:%S").to_string().cyan()
                    );
                    println!();

                    let mut current_alerts = Vec::new();

                    for group_id in &groups_to_watch {
                        // Use LagCalculator to get lag information
                        let group_lag =
                            lag_calculator.calculate_group_lag(&coordinator, group_id)?;

                        let total_lag = group_lag.total_lag;

                        // Count unique topics from partitions
                        let topic_count = {
                            let mut topics: std::collections::HashSet<&str> =
                                std::collections::HashSet::new();
                            for p in &group_lag.partitions {
                                topics.insert(&p.topic);
                            }
                            topics.len()
                        };

                        // Calculate rate of lag change
                        let lag_change = if let Some(prev_lag) = prev_lags.get(group_id) {
                            let delta = total_lag - prev_lag;
                            if delta > 0 {
                                format!(" {} +{}", "↗".red(), delta)
                            } else if delta < 0 {
                                format!(" {} {}", "↘".green(), delta)
                            } else {
                                " → stable".dimmed().to_string()
                            }
                        } else {
                            String::new()
                        };

                        // Check alert threshold
                        let alert_marker = if let Some(threshold) = alert_lag {
                            if total_lag > threshold {
                                current_alerts.push((group_id.clone(), total_lag, threshold));
                                format!(" {} ALERT!", "⚠".red().bold())
                            } else {
                                String::new()
                            }
                        } else {
                            String::new()
                        };

                        let lag_color = if total_lag == 0 {
                            total_lag.to_string().green()
                        } else if total_lag < 100 {
                            total_lag.to_string().yellow()
                        } else {
                            total_lag.to_string().red()
                        };

                        println!(
                            "  {} {} | {} topic(s) | lag: {}{}{}",
                            "●".cyan(),
                            group_id.bold(),
                            topic_count.to_string().cyan(),
                            lag_color,
                            lag_change,
                            alert_marker
                        );

                        prev_lags.insert(group_id.clone(), total_lag);
                    }

                    println!();

                    // Print alert summary if any
                    if !current_alerts.is_empty() {
                        println!("{}", "ALERTS:".red().bold());
                        for (gid, lag, threshold) in &current_alerts {
                            println!(
                                "  {} Group '{}' lag ({}) exceeds threshold ({})",
                                "⚠".red(),
                                gid.bold(),
                                lag.to_string().red(),
                                threshold
                            );
                        }
                        println!();

                        if exit_on_alert {
                            return Err(streamline::StreamlineError::Server(
                                "Alert threshold exceeded".into(),
                            ));
                        }
                    }
                }

                iteration += 1;
                std::thread::sleep(Duration::from_secs(interval));
            }
        }
    }

    Ok(())
}
