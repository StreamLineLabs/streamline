//! Topic management CLI commands

use colored::Colorize;
use comfy_table::{presets::UTF8_FULL_CONDENSED, Cell, Color, ContentArrangement, Table};
use indicatif::{ProgressBar, ProgressStyle};
use serde_json::json;
use std::time::Duration;
use streamline::{
    storage::RemoteStorageConfig, storage::StorageMode, Result, TopicManager, TopicMetadata,
};

use crate::cli_context::CliContext;
use crate::{format_delimited_row, OutputFormat, TopicCommands};

pub(crate) fn handle_topic_command(cmd: TopicCommands, ctx: &CliContext) -> Result<()> {
    use streamline::TopicConfig;

    let data_dir = &ctx.data_dir;
    std::fs::create_dir_all(data_dir)?;
    let manager = TopicManager::new(data_dir)?;

    match cmd {
        TopicCommands::List => {
            let topics = manager.list_topics()?;

            match ctx.format {
                OutputFormat::Json => {
                    let data: Vec<_> = topics
                        .iter()
                        .map(|t| {
                            json!({
                                "name": t.name,
                                "partitions": t.num_partitions
                            })
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
                        format_delimited_row(ctx.format, &["name", "partitions"])
                    );
                    // Print data rows
                    for topic in topics {
                        println!(
                            "{}",
                            format_delimited_row(
                                ctx.format,
                                &[&topic.name, &topic.num_partitions.to_string()]
                            )
                        );
                    }
                }
                _ => {
                    if topics.is_empty() {
                        ctx.info("No topics found.");
                    } else {
                        let mut table = Table::new();
                        table.load_preset(UTF8_FULL_CONDENSED);
                        table.set_content_arrangement(ContentArrangement::Dynamic);
                        table.set_header(vec![
                            Cell::new("Topic").fg(Color::Cyan),
                            Cell::new("Partitions").fg(Color::Cyan),
                        ]);

                        for topic in topics {
                            table.add_row(vec![
                                Cell::new(&topic.name),
                                Cell::new(topic.num_partitions),
                            ]);
                        }

                        println!("{}", table);
                    }
                }
            }
        }

        TopicCommands::Create {
            name,
            partitions,
            retention_ms,
            retention_bytes,
            segment_bytes,
            storage_mode,
            remote_batch_size,
            remote_batch_timeout_ms,
            remote_cache_bytes,
            explain,
        } => {
            let spinner = ctx.spinner(&format!("Creating topic '{}'...", name));

            // Parse storage mode
            let mode: StorageMode = storage_mode
                .parse()
                .map_err(|e: String| streamline::StreamlineError::Config(e))?;

            // Build remote storage config if needed
            let remote_storage = if mode.requires_remote_storage() {
                Some(RemoteStorageConfig {
                    batch_size_bytes: remote_batch_size,
                    batch_timeout_ms: remote_batch_timeout_ms,
                    read_cache_bytes: remote_cache_bytes,
                    ..Default::default()
                })
            } else {
                None
            };

            let config = TopicConfig {
                retention_ms,
                retention_bytes,
                segment_bytes,
                cleanup_policy: streamline::storage::topic::CleanupPolicy::Delete,
                storage_mode: mode,
                remote_storage,
                ..Default::default()
            };

            // Validate config
            if let Err(e) = config.validate() {
                return Err(streamline::StreamlineError::Config(e));
            }

            manager.create_topic_with_config(&name, partitions, config)?;

            if let Some(s) = spinner {
                s.finish_and_clear();
            }

            match ctx.format {
                OutputFormat::Json => {
                    let info = json!({
                        "status": "created",
                        "topic": name,
                        "partitions": partitions,
                        "retention_ms": retention_ms,
                        "retention_bytes": retention_bytes,
                        "segment_bytes": segment_bytes,
                        "storage_mode": mode.to_string()
                    });
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&info)?
                    );
                }
                _ => {
                    ctx.success(&format!(
                        "Created topic '{}' with {} partition(s)",
                        name.bold(),
                        partitions.to_string().green()
                    ));
                    println!(
                        "  {} {} ms, {} bytes",
                        "Retention:".dimmed(),
                        retention_ms,
                        retention_bytes
                    );
                    println!("  {} {} bytes", "Segment size:".dimmed(), segment_bytes);
                    println!("  {} {}", "Storage mode:".dimmed(), mode);
                }
            }

            // Show explanation if enabled
            if explain {
                use streamline::cli_utils::explain::{explanations, ExplainContext};
                let mut explain_ctx = ExplainContext::new(true);
                explanations::explain_create_topic(
                    &mut explain_ctx,
                    &name,
                    partitions,
                    retention_ms,
                );
            }
        }

        TopicCommands::Describe { name } => {
            let metadata = manager.get_topic_metadata(&name)?;
            print_topic_metadata(&metadata, ctx);
        }

        TopicCommands::Delete { name } => {
            if !ctx.confirm(&format!("Delete topic '{}'?", name)) {
                ctx.info("Cancelled.");
                return Ok(());
            }

            let spinner = ctx.spinner(&format!("Deleting topic '{}'...", name));
            manager.delete_topic(&name)?;
            if let Some(s) = spinner {
                s.finish_and_clear();
            }

            match ctx.format {
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "status": "deleted",
                            "topic": name
                        }))?
                    );
                }
                _ => {
                    ctx.success(&format!("Deleted topic '{}'", name.bold()));
                }
            }
        }

        TopicCommands::Alter {
            name,
            retention_ms,
            retention_bytes,
            segment_bytes,
        } => {
            // Load existing metadata
            let mut metadata = manager.get_topic_metadata(&name)?;

            // Update only specified fields
            if let Some(ms) = retention_ms {
                metadata.config.retention_ms = ms;
            }
            if let Some(bytes) = retention_bytes {
                metadata.config.retention_bytes = bytes;
            }
            if let Some(seg_bytes) = segment_bytes {
                metadata.config.segment_bytes = seg_bytes;
            }

            // Save updated metadata
            let topic_path = data_dir.join("topics").join(&name);
            let metadata_path = topic_path.join("metadata.json");
            let data = serde_json::to_string_pretty(&metadata)?;
            std::fs::write(&metadata_path, data)?;

            match ctx.format {
                OutputFormat::Json => {
                    print_topic_metadata(&metadata, ctx);
                }
                _ => {
                    ctx.success(&format!(
                        "Updated configuration for topic '{}'",
                        name.bold()
                    ));
                    print_topic_metadata(&metadata, ctx);
                }
            }
        }

        TopicCommands::Export {
            name,
            output,
            partition,
            from_offset,
            to_offset,
            include_metadata,
        } => {
            use std::io::Write;

            let output_path = output.unwrap_or_else(|| format!("{}.jsonl", name));
            let spinner = ctx.spinner(&format!(
                "Exporting topic '{}' to '{}'...",
                name, output_path
            ));

            // Get topic metadata to know partitions
            let metadata = manager.get_topic_metadata(&name)?;
            let partitions_to_export: Vec<i32> = if let Some(p) = partition {
                vec![p]
            } else {
                (0..metadata.num_partitions).collect()
            };

            let mut file = std::fs::File::create(&output_path)?;
            let mut total_records = 0u64;

            for part in &partitions_to_export {
                let start = from_offset
                    .unwrap_or_else(|| manager.earliest_offset(&name, *part).unwrap_or(0));
                let end =
                    to_offset.unwrap_or_else(|| manager.latest_offset(&name, *part).unwrap_or(0));

                if start >= end {
                    continue;
                }

                let batch_size = 1000;
                let mut current = start;

                while current < end {
                    let count = ((end - current) as usize).min(batch_size);
                    let records = manager.read(&name, *part, current, count)?;

                    for record in &records {
                        let mut entry = json!({
                            "partition": part,
                            "offset": record.offset,
                            "value": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &record.value),
                        });

                        if include_metadata {
                            entry["timestamp"] = json!(record.timestamp);
                            if let Some(ref key) = record.key {
                                entry["key"] = json!(base64::Engine::encode(
                                    &base64::engine::general_purpose::STANDARD,
                                    key
                                ));
                            }
                            if !record.headers.is_empty() {
                                entry["headers"] = json!(record.headers.iter().map(|h| {
                                    json!({
                                        "key": h.key,
                                        "value": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &h.value)
                                    })
                                }).collect::<Vec<_>>());
                            }
                        }

                        writeln!(file, "{}", serde_json::to_string(&entry)?)?;
                        total_records += 1;
                    }

                    if records.is_empty() {
                        break;
                    }
                    current = records.last().map(|r| r.offset + 1).unwrap_or(end);
                }
            }

            if let Some(s) = spinner {
                s.finish_and_clear();
            }

            match ctx.format {
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "status": "exported",
                            "topic": name,
                            "file": output_path,
                            "records": total_records,
                            "partitions": partitions_to_export
                        }))?
                    );
                }
                _ => {
                    ctx.success(&format!(
                        "Exported {} record(s) from topic '{}' to '{}'",
                        total_records.to_string().green(),
                        name.bold(),
                        output_path.cyan()
                    ));
                }
            }
        }

        TopicCommands::Import {
            name,
            input,
            create,
            partitions,
            dry_run,
        } => {
            use std::io::BufRead;

            if !std::path::Path::new(&input).exists() {
                return Err(streamline::StreamlineError::storage_msg(format!(
                    "Input file not found: {}",
                    input
                )));
            }

            // Create topic if requested and doesn't exist
            let topics = manager.list_topics()?;
            let topic_exists = topics.iter().any(|t| t.name == name);

            if !topic_exists {
                if create {
                    if !dry_run {
                        manager.get_or_create_topic(&name, partitions)?;
                        ctx.info(&format!(
                            "Created topic '{}' with {} partition(s)",
                            name, partitions
                        ));
                    } else {
                        ctx.info(&format!(
                            "[DRY RUN] Would create topic '{}' with {} partition(s)",
                            name, partitions
                        ));
                    }
                } else {
                    return Err(streamline::StreamlineError::storage_msg(format!(
                        "Topic '{}' does not exist. Use --create to create it.",
                        name
                    )));
                }
            }

            let spinner = if !dry_run {
                ctx.spinner(&format!(
                    "Importing to topic '{}' from '{}'...",
                    name, input
                ))
            } else {
                None
            };

            let file = std::fs::File::open(&input)?;
            let reader = std::io::BufReader::new(file);
            let mut imported = 0u64;
            let mut errors = 0u64;

            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }

                match serde_json::from_str::<serde_json::Value>(&line) {
                    Ok(entry) => {
                        let partition = entry
                            .get("partition")
                            .and_then(|v| v.as_i64())
                            .map(|v| v as i32)
                            .unwrap_or(0);

                        let value = entry
                            .get("value")
                            .and_then(|v| v.as_str())
                            .map(|s| {
                                base64::Engine::decode(
                                    &base64::engine::general_purpose::STANDARD,
                                    s,
                                )
                                .unwrap_or_default()
                            })
                            .unwrap_or_default();

                        let key = entry
                            .get("key")
                            .and_then(|v| v.as_str())
                            .and_then(|s| {
                                base64::Engine::decode(
                                    &base64::engine::general_purpose::STANDARD,
                                    s,
                                )
                                .ok()
                            })
                            .map(bytes::Bytes::from);

                        if !dry_run {
                            manager.append(&name, partition, key, bytes::Bytes::from(value))?;
                        }
                        imported += 1;
                    }
                    Err(e) => {
                        errors += 1;
                        if errors <= 5 {
                            eprintln!("{} Failed to parse line: {}", "Warning:".yellow(), e);
                        }
                    }
                }
            }

            if let Some(s) = spinner {
                s.finish_and_clear();
            }

            match ctx.format {
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "status": if dry_run { "dry_run" } else { "imported" },
                            "topic": name,
                            "file": input,
                            "records": imported,
                            "errors": errors
                        }))?
                    );
                }
                _ => {
                    if dry_run {
                        ctx.info(&format!(
                            "[DRY RUN] Would import {} record(s) to topic '{}' ({} parse errors)",
                            imported.to_string().green(),
                            name.bold(),
                            errors
                        ));
                    } else {
                        ctx.success(&format!(
                            "Imported {} record(s) to topic '{}' from '{}'",
                            imported.to_string().green(),
                            name.bold(),
                            input.cyan()
                        ));
                        if errors > 0 {
                            ctx.warn(&format!("{} record(s) failed to parse", errors));
                        }
                    }
                }
            }
        }

        TopicCommands::Watch {
            topic,
            interval,
            rate,
            size,
        } => {
            use std::collections::HashMap;

            println!(
                "{}",
                "Topic Watch Mode - Press Ctrl+C to stop".bold().cyan()
            );
            println!("{}", "─".repeat(60).dimmed());
            println!();

            // Store previous offsets to calculate rates
            let mut prev_offsets: HashMap<String, HashMap<i32, i64>> = HashMap::new();
            let mut iteration = 0u64;

            loop {
                let topics = manager.list_topics()?;
                let topics_to_watch: Vec<_> = if let Some(ref name) = topic {
                    topics.iter().filter(|t| &t.name == name).collect()
                } else {
                    topics.iter().collect()
                };

                if topics_to_watch.is_empty() {
                    if let Some(ref name) = topic {
                        ctx.error(&format!("Topic '{}' not found", name));
                        return Ok(());
                    }
                    println!("{}", "No topics found".dimmed());
                } else {
                    // Clear previous output (simple approach using newlines)
                    if iteration > 0 {
                        // Move cursor up
                        print!("\x1B[{}A", topics_to_watch.len() * 2 + 3);
                    }

                    let now = chrono::Utc::now();
                    println!(
                        "{} {}",
                        "Last updated:".dimmed(),
                        now.format("%H:%M:%S").to_string().cyan()
                    );
                    println!();

                    for topic_meta in &topics_to_watch {
                        let topic_name = &topic_meta.name;

                        // Get current offsets for each partition
                        let mut total_offset: i64 = 0;
                        let mut total_size: u64 = 0;
                        let mut partition_offsets: HashMap<i32, i64> = HashMap::new();

                        for p in 0..topic_meta.num_partitions {
                            if let Ok(records) = manager.read(topic_name, p, 0, 0) {
                                // Get last offset
                                let last_offset = records.last().map(|r| r.offset).unwrap_or(-1);
                                partition_offsets.insert(p, last_offset + 1);
                                total_offset += last_offset + 1;

                                if size {
                                    // Estimate size (simplified - would need actual segment size)
                                    for r in &records {
                                        total_size += r.value.len() as u64;
                                    }
                                }
                            }
                        }

                        // Calculate rate if we have previous data
                        let rate_str = if rate {
                            if let Some(prev) = prev_offsets.get(topic_name) {
                                let prev_total: i64 = prev.values().sum();
                                let delta = total_offset - prev_total;
                                let rate_val = delta as f64 / interval as f64;
                                format!(" {} {:.1} msg/s", "↗".green(), rate_val)
                            } else {
                                " (calculating rate...)".dimmed().to_string()
                            }
                        } else {
                            String::new()
                        };

                        let size_str = if size && total_size > 0 {
                            let size_display = if total_size > 1024 * 1024 {
                                format!("{:.1} MB", total_size as f64 / 1024.0 / 1024.0)
                            } else if total_size > 1024 {
                                format!("{:.1} KB", total_size as f64 / 1024.0)
                            } else {
                                format!("{} B", total_size)
                            };
                            format!(" | {}", size_display.dimmed())
                        } else {
                            String::new()
                        };

                        println!(
                            "  {} {} | {} partitions | {} messages{}{}",
                            "●".cyan(),
                            topic_name.bold(),
                            topic_meta.num_partitions.to_string().cyan(),
                            total_offset.to_string().green(),
                            rate_str,
                            size_str
                        );

                        // Store current offsets for next iteration
                        prev_offsets.insert(topic_name.clone(), partition_offsets);
                    }

                    println!();
                }

                iteration += 1;
                std::thread::sleep(Duration::from_secs(interval));
            }
        }

        TopicCommands::Clone {
            source,
            dest,
            partitions,
            source_partition,
            from_offset,
            to_offset,
            progress,
        } => {
            // Verify source topic exists
            let source_meta = match manager.get_topic_metadata(&source) {
                Ok(meta) => meta,
                Err(_) => {
                    ctx.error(&format!("Source topic '{}' not found", source));
                    return Ok(());
                }
            };

            // Check if destination topic already exists
            if manager.get_topic_metadata(&dest).is_ok() {
                ctx.error(&format!("Destination topic '{}' already exists", dest));
                return Ok(());
            }

            // Determine partition count
            let dest_partitions = partitions.unwrap_or(source_meta.num_partitions);

            // Create destination topic
            println!(
                "{} Creating destination topic '{}' with {} partition(s)...",
                "→".cyan(),
                dest.bold(),
                dest_partitions
            );
            manager.get_or_create_topic(&dest, dest_partitions)?;

            // Determine which partitions to copy
            let partitions_to_copy: Vec<i32> = if let Some(p) = source_partition {
                if p >= source_meta.num_partitions {
                    ctx.error(&format!(
                        "Source partition {} does not exist (topic has {} partitions)",
                        p, source_meta.num_partitions
                    ));
                    return Ok(());
                }
                vec![p]
            } else {
                (0..source_meta.num_partitions).collect()
            };

            let mut total_messages = 0u64;
            let mut total_copied = 0u64;

            // First pass: count messages for progress bar
            if progress {
                for &partition in &partitions_to_copy {
                    let start = from_offset.max(0);
                    let end = if to_offset < 0 {
                        manager.latest_offset(&source, partition).unwrap_or(0)
                    } else {
                        to_offset
                    };
                    total_messages += (end - start).max(0) as u64;
                }
            }

            let pb = if progress && total_messages > 0 {
                let bar = ProgressBar::new(total_messages);
                bar.set_style(
                    ProgressStyle::default_bar()
                        .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})")
                        .unwrap_or_else(|_| ProgressStyle::default_bar())
                        .progress_chars("#>-"),
                );
                Some(bar)
            } else {
                None
            };

            // Copy messages partition by partition
            for &src_partition in &partitions_to_copy {
                let dest_partition = src_partition % dest_partitions;

                let start_offset = from_offset.max(0);
                let end_offset = if to_offset < 0 {
                    manager.latest_offset(&source, src_partition).unwrap_or(0)
                } else {
                    to_offset
                };

                if start_offset >= end_offset {
                    continue;
                }

                // Read in batches
                let batch_size = 1000;
                let mut current_offset = start_offset;

                while current_offset < end_offset {
                    let limit = ((end_offset - current_offset) as usize).min(batch_size);
                    let records = manager.read(&source, src_partition, current_offset, limit)?;

                    if records.is_empty() {
                        break;
                    }

                    for record in &records {
                        if record.offset >= end_offset {
                            break;
                        }

                        let key = record.key.clone();

                        manager.append(&dest, dest_partition, key, record.value.clone())?;
                        total_copied += 1;

                        if let Some(ref bar) = pb {
                            bar.inc(1);
                        }
                    }

                    current_offset = records.last().map(|r| r.offset + 1).unwrap_or(end_offset);
                }
            }

            if let Some(bar) = pb {
                bar.finish_with_message("done");
            }

            println!();
            ctx.success(&format!(
                "Cloned {} message(s) from '{}' to '{}'",
                total_copied, source, dest
            ));
        }

        TopicCommands::Retention {
            name,
            budget,
            duration,
            detailed,
        } => {
            // Parse budget string to bytes
            fn parse_size(s: &str) -> Option<u64> {
                let s = s.trim().to_uppercase();
                if let Some(num) = s.strip_suffix("TB") {
                    num.trim()
                        .parse::<f64>()
                        .ok()
                        .map(|n| (n * 1024.0 * 1024.0 * 1024.0 * 1024.0) as u64)
                } else if let Some(num) = s.strip_suffix("GB") {
                    num.trim()
                        .parse::<f64>()
                        .ok()
                        .map(|n| (n * 1024.0 * 1024.0 * 1024.0) as u64)
                } else if let Some(num) = s.strip_suffix("MB") {
                    num.trim()
                        .parse::<f64>()
                        .ok()
                        .map(|n| (n * 1024.0 * 1024.0) as u64)
                } else if let Some(num) = s.strip_suffix("KB") {
                    num.trim().parse::<f64>().ok().map(|n| (n * 1024.0) as u64)
                } else if let Some(num) = s.strip_suffix('B') {
                    num.trim().parse::<u64>().ok()
                } else {
                    s.parse::<u64>().ok()
                }
            }

            // Parse duration string to milliseconds
            fn parse_duration_ms(s: &str) -> Option<i64> {
                let s = s.trim().to_lowercase();
                if let Some(num) = s.strip_suffix('y') {
                    num.trim()
                        .parse::<i64>()
                        .ok()
                        .map(|n| n * 365 * 24 * 60 * 60 * 1000)
                } else if let Some(num) = s.strip_suffix('d') {
                    num.trim()
                        .parse::<i64>()
                        .ok()
                        .map(|n| n * 24 * 60 * 60 * 1000)
                } else if let Some(num) = s.strip_suffix('h') {
                    num.trim().parse::<i64>().ok().map(|n| n * 60 * 60 * 1000)
                } else if let Some(num) = s.strip_suffix('m') {
                    num.trim().parse::<i64>().ok().map(|n| n * 60 * 1000)
                } else if let Some(num) = s.strip_suffix('s') {
                    num.trim().parse::<i64>().ok().map(|n| n * 1000)
                } else {
                    s.parse::<i64>().ok().map(|n| n * 24 * 60 * 60 * 1000) // Default to days
                }
            }

            // Format bytes for display
            fn format_bytes(bytes: u64) -> String {
                if bytes >= 1024 * 1024 * 1024 * 1024 {
                    format!("{:.2} TB", bytes as f64 / 1024.0 / 1024.0 / 1024.0 / 1024.0)
                } else if bytes >= 1024 * 1024 * 1024 {
                    format!("{:.2} GB", bytes as f64 / 1024.0 / 1024.0 / 1024.0)
                } else if bytes >= 1024 * 1024 {
                    format!("{:.2} MB", bytes as f64 / 1024.0 / 1024.0)
                } else if bytes >= 1024 {
                    format!("{:.2} KB", bytes as f64 / 1024.0)
                } else {
                    format!("{} B", bytes)
                }
            }

            // Format duration for display
            fn format_duration_ms(ms: i64) -> String {
                if ms >= 365 * 24 * 60 * 60 * 1000 {
                    format!(
                        "{:.1} years",
                        ms as f64 / 365.0 / 24.0 / 60.0 / 60.0 / 1000.0
                    )
                } else if ms >= 24 * 60 * 60 * 1000 {
                    format!("{:.1} days", ms as f64 / 24.0 / 60.0 / 60.0 / 1000.0)
                } else if ms >= 60 * 60 * 1000 {
                    format!("{:.1} hours", ms as f64 / 60.0 / 60.0 / 1000.0)
                } else if ms >= 60 * 1000 {
                    format!("{:.1} minutes", ms as f64 / 60.0 / 1000.0)
                } else {
                    format!("{:.1} seconds", ms as f64 / 1000.0)
                }
            }

            // Get topic metadata
            let metadata = match manager.get_topic_metadata(&name) {
                Ok(meta) => meta,
                Err(_) => {
                    ctx.error(&format!("Topic '{}' not found", name));
                    return Ok(());
                }
            };

            println!(
                "{}",
                format!("Retention Calculator for '{}'", name).bold().cyan()
            );
            println!("{}", "─".repeat(50).dimmed());
            println!();

            // Analyze current data
            let mut total_messages: u64 = 0;
            let mut total_size: u64 = 0;
            let mut oldest_timestamp: i64 = i64::MAX;
            let mut newest_timestamp: i64 = 0;

            for partition in 0..metadata.num_partitions {
                let earliest = manager.earliest_offset(&name, partition).unwrap_or(0);
                let latest = manager.latest_offset(&name, partition).unwrap_or(0);

                if latest > earliest {
                    total_messages += (latest - earliest) as u64;

                    // Sample to estimate size and timestamps
                    let sample_count = 100.min((latest - earliest) as usize);
                    if let Ok(records) = manager.read(&name, partition, earliest, sample_count) {
                        for record in &records {
                            total_size += record.value.len() as u64;
                            if let Some(ref key) = record.key {
                                total_size += key.len() as u64;
                            }
                            oldest_timestamp = oldest_timestamp.min(record.timestamp);
                            newest_timestamp = newest_timestamp.max(record.timestamp);
                        }
                        // Extrapolate size
                        if !records.is_empty() {
                            let avg_msg_size = total_size / records.len() as u64;
                            total_size = avg_msg_size * total_messages;
                        }
                    }
                }
            }

            let data_span_ms = if newest_timestamp > oldest_timestamp {
                newest_timestamp - oldest_timestamp
            } else {
                0
            };

            // Current statistics
            println!("{}", "Current Statistics:".bold());
            println!("  Total messages: {}", total_messages.to_string().cyan());
            println!("  Estimated size: {}", format_bytes(total_size).cyan());
            if data_span_ms > 0 {
                println!("  Data span: {}", format_duration_ms(data_span_ms).cyan());
            }
            println!(
                "  Partitions: {}",
                metadata.num_partitions.to_string().cyan()
            );
            println!();

            // Current retention settings
            println!("{}", "Current Retention Settings:".bold());
            println!(
                "  retention.ms: {} ({})",
                metadata.config.retention_ms.to_string().yellow(),
                if metadata.config.retention_ms < 0 {
                    "infinite".to_string()
                } else {
                    format_duration_ms(metadata.config.retention_ms)
                }
            );
            println!(
                "  retention.bytes: {} ({})",
                metadata.config.retention_bytes.to_string().yellow(),
                if metadata.config.retention_bytes < 0 {
                    "infinite".to_string()
                } else {
                    format_bytes(metadata.config.retention_bytes as u64)
                }
            );
            println!();

            // Calculate recommendations
            let msg_rate_per_ms = if data_span_ms > 0 {
                total_messages as f64 / data_span_ms as f64
            } else {
                0.0
            };
            let byte_rate_per_ms = if data_span_ms > 0 {
                total_size as f64 / data_span_ms as f64
            } else {
                0.0
            };

            println!("{}", "Calculated Rates:".bold());
            if msg_rate_per_ms > 0.0 {
                let msg_per_sec = msg_rate_per_ms * 1000.0;
                let msg_per_day = msg_rate_per_ms * 24.0 * 60.0 * 60.0 * 1000.0;
                println!(
                    "  Message rate: {:.2} msg/sec ({:.0} msg/day)",
                    msg_per_sec, msg_per_day
                );
            }
            if byte_rate_per_ms > 0.0 {
                let bytes_per_day = byte_rate_per_ms * 24.0 * 60.0 * 60.0 * 1000.0;
                println!("  Data rate: {}/day", format_bytes(bytes_per_day as u64));
            }
            println!();

            // Recommendations based on budget
            if let Some(ref budget_str) = budget {
                if let Some(budget_bytes) = parse_size(budget_str) {
                    println!(
                        "{}",
                        format!("Recommendations for {} budget:", budget_str)
                            .bold()
                            .green()
                    );
                    if byte_rate_per_ms > 0.0 {
                        let retention_ms = (budget_bytes as f64 / byte_rate_per_ms) as i64;
                        println!(
                            "  Recommended retention.ms: {}",
                            retention_ms.to_string().cyan()
                        );
                        println!(
                            "  Which equals: {}",
                            format_duration_ms(retention_ms).cyan()
                        );
                        println!("  Set retention.bytes: {}", budget_bytes.to_string().cyan());
                        println!();
                        println!("  {} streamline-cli topics alter {} --retention-ms {} --retention-bytes {}",
                            "Command:".dimmed(),
                            name,
                            retention_ms,
                            budget_bytes
                        );
                    } else {
                        println!(
                            "  {}",
                            "Not enough data to calculate (need more message history)".yellow()
                        );
                    }
                    println!();
                }
            }

            // Recommendations based on duration
            if let Some(ref duration_str) = duration {
                if let Some(duration_ms) = parse_duration_ms(duration_str) {
                    println!(
                        "{}",
                        format!("Recommendations for {} retention:", duration_str)
                            .bold()
                            .green()
                    );
                    if byte_rate_per_ms > 0.0 {
                        let estimated_size = (byte_rate_per_ms * duration_ms as f64) as u64;
                        println!(
                            "  Estimated storage needed: {}",
                            format_bytes(estimated_size).cyan()
                        );
                        println!("  Set retention.ms: {}", duration_ms.to_string().cyan());
                        println!();
                        println!(
                            "  {} streamline-cli topics alter {} --retention-ms {}",
                            "Command:".dimmed(),
                            name,
                            duration_ms
                        );
                    } else {
                        println!(
                            "  {}",
                            "Not enough data to calculate (need more message history)".yellow()
                        );
                    }
                    println!();
                }
            }

            // Detailed per-partition analysis
            if detailed {
                println!("{}", "Per-Partition Analysis:".bold());
                for partition in 0..metadata.num_partitions {
                    let earliest = manager.earliest_offset(&name, partition).unwrap_or(0);
                    let latest = manager.latest_offset(&name, partition).unwrap_or(0);
                    let messages = (latest - earliest).max(0);
                    println!(
                        "  Partition {}: {} messages (offsets {} to {})",
                        partition.to_string().cyan(),
                        messages,
                        earliest,
                        latest
                    );
                }
            }
        }

        TopicCommands::Diff {
            topic1,
            topic2,
            config_only,
            messages,
            max_messages,
            diff_only,
        } => {
            let manager = TopicManager::new(&ctx.data_dir)?;

            // Get metadata for both topics
            let meta1 = manager.get_topic_metadata(&topic1)?;
            let meta2 = manager.get_topic_metadata(&topic2)?;

            println!();
            println!(
                "{} {} {} {}",
                "Comparing".bold(),
                topic1.cyan(),
                "vs".dimmed(),
                topic2.cyan()
            );
            println!("{}", "═".repeat(60).dimmed());

            let mut has_differences = false;

            // Compare partitions
            if meta1.num_partitions != meta2.num_partitions {
                has_differences = true;
                println!("\n{} {}", "⚠".yellow(), "Partition count differs:".bold());
                println!(
                    "  {} {} partitions",
                    topic1.cyan(),
                    meta1.num_partitions.to_string().red()
                );
                println!(
                    "  {} {} partitions",
                    topic2.cyan(),
                    meta2.num_partitions.to_string().green()
                );
            } else if !diff_only {
                println!(
                    "\n{} Partitions: {} (same)",
                    "✓".green(),
                    meta1.num_partitions.to_string().cyan()
                );
            }

            // Compare replication factor
            if meta1.replication_factor != meta2.replication_factor {
                has_differences = true;
                println!(
                    "\n{} {}",
                    "⚠".yellow(),
                    "Replication factor differs:".bold()
                );
                println!(
                    "  {} replication factor {}",
                    topic1.cyan(),
                    meta1.replication_factor.to_string().red()
                );
                println!(
                    "  {} replication factor {}",
                    topic2.cyan(),
                    meta2.replication_factor.to_string().green()
                );
            } else if !diff_only {
                println!(
                    "{} Replication factor: {} (same)",
                    "✓".green(),
                    meta1.replication_factor.to_string().cyan()
                );
            }

            // Compare configuration
            println!("\n{}", "Configuration Comparison:".bold().underline());

            // Retention time
            if meta1.config.retention_ms != meta2.config.retention_ms {
                has_differences = true;
                println!("  {} {}", "⚠".yellow(), "retention.ms differs:".bold());
                println!(
                    "    {} {} ms",
                    topic1.cyan(),
                    meta1.config.retention_ms.to_string().red()
                );
                println!(
                    "    {} {} ms",
                    topic2.cyan(),
                    meta2.config.retention_ms.to_string().green()
                );
            } else if !diff_only {
                println!(
                    "  {} retention.ms: {} (same)",
                    "✓".green(),
                    meta1.config.retention_ms.to_string().dimmed()
                );
            }

            // Retention bytes
            if meta1.config.retention_bytes != meta2.config.retention_bytes {
                has_differences = true;
                println!("  {} {}", "⚠".yellow(), "retention.bytes differs:".bold());
                println!(
                    "    {} {} bytes",
                    topic1.cyan(),
                    meta1.config.retention_bytes.to_string().red()
                );
                println!(
                    "    {} {} bytes",
                    topic2.cyan(),
                    meta2.config.retention_bytes.to_string().green()
                );
            } else if !diff_only {
                println!(
                    "  {} retention.bytes: {} (same)",
                    "✓".green(),
                    meta1.config.retention_bytes.to_string().dimmed()
                );
            }

            // Segment bytes
            if meta1.config.segment_bytes != meta2.config.segment_bytes {
                has_differences = true;
                println!("  {} {}", "⚠".yellow(), "segment.bytes differs:".bold());
                println!(
                    "    {} {} bytes",
                    topic1.cyan(),
                    meta1.config.segment_bytes.to_string().red()
                );
                println!(
                    "    {} {} bytes",
                    topic2.cyan(),
                    meta2.config.segment_bytes.to_string().green()
                );
            } else if !diff_only {
                println!(
                    "  {} segment.bytes: {} (same)",
                    "✓".green(),
                    meta1.config.segment_bytes.to_string().dimmed()
                );
            }

            // Cleanup policy
            if meta1.config.cleanup_policy != meta2.config.cleanup_policy {
                has_differences = true;
                println!("  {} {}", "⚠".yellow(), "cleanup.policy differs:".bold());
                println!(
                    "    {} {}",
                    topic1.cyan(),
                    meta1.config.cleanup_policy.to_string().red()
                );
                println!(
                    "    {} {}",
                    topic2.cyan(),
                    meta2.config.cleanup_policy.to_string().green()
                );
            } else if !diff_only {
                println!(
                    "  {} cleanup.policy: {} (same)",
                    "✓".green(),
                    meta1.config.cleanup_policy.to_string().dimmed()
                );
            }

            // Storage mode
            let mode1 = meta1.config.storage_mode.to_string();
            let mode2 = meta2.config.storage_mode.to_string();
            if mode1 != mode2 {
                has_differences = true;
                println!("  {} {}", "⚠".yellow(), "storage.mode differs:".bold());
                println!("    {} {}", topic1.cyan(), mode1.red());
                println!("    {} {}", topic2.cyan(), mode2.green());
            } else if !diff_only {
                println!("  {} storage.mode: {} (same)", "✓".green(), mode1.dimmed());
            }

            // Compare messages if requested
            if messages && !config_only {
                println!("\n{}", "Message Comparison:".bold().underline());

                // Compare partition 0 for simplicity (could extend to all partitions)
                let partition = 0;

                let records1 = manager.read(&topic1, partition, 0, max_messages)?;
                let records2 = manager.read(&topic2, partition, 0, max_messages)?;

                let count1 = records1.len();
                let count2 = records2.len();

                if count1 != count2 {
                    has_differences = true;
                    println!(
                        "  {} Message count differs (partition {}):",
                        "⚠".yellow(),
                        partition
                    );
                    println!(
                        "    {} {} messages",
                        topic1.cyan(),
                        count1.to_string().red()
                    );
                    println!(
                        "    {} {} messages",
                        topic2.cyan(),
                        count2.to_string().green()
                    );
                } else if !diff_only {
                    println!(
                        "  {} Message count: {} (same in partition {})",
                        "✓".green(),
                        count1.to_string().cyan(),
                        partition
                    );
                }

                // Compare message contents
                let mut content_diffs = 0;
                let compare_count = count1.min(count2).min(max_messages);

                for i in 0..compare_count {
                    let r1 = &records1[i];
                    let r2 = &records2[i];

                    // Compare values
                    if r1.value != r2.value {
                        content_diffs += 1;
                        if content_diffs <= 5 {
                            println!("  {} Message at offset {} differs:", "⚠".yellow(), i);
                            let v1 = String::from_utf8_lossy(&r1.value);
                            let v2 = String::from_utf8_lossy(&r2.value);
                            let preview1 = if v1.len() > 50 {
                                format!("{}...", &v1[..50])
                            } else {
                                v1.to_string()
                            };
                            let preview2 = if v2.len() > 50 {
                                format!("{}...", &v2[..50])
                            } else {
                                v2.to_string()
                            };
                            println!("    {} {}", topic1.cyan(), preview1.red());
                            println!("    {} {}", topic2.cyan(), preview2.green());
                        }
                    }
                }

                if content_diffs > 5 {
                    println!(
                        "  {} ... and {} more message differences",
                        "⚠".yellow(),
                        content_diffs - 5
                    );
                }

                if content_diffs > 0 {
                    has_differences = true;
                    println!(
                        "\n  {} Total: {} of {} messages differ",
                        "Summary:".bold(),
                        content_diffs.to_string().red(),
                        compare_count
                    );
                } else if compare_count > 0 && !diff_only {
                    println!(
                        "  {} All {} compared messages are identical",
                        "✓".green(),
                        compare_count
                    );
                }
            }

            // Summary
            println!("\n{}", "─".repeat(60).dimmed());
            if has_differences {
                println!(
                    "{} Topics {} and {} have differences",
                    "⚠".yellow(),
                    topic1.cyan(),
                    topic2.cyan()
                );
            } else {
                println!(
                    "{} Topics {} and {} are identical{}",
                    "✓".green(),
                    topic1.cyan(),
                    topic2.cyan(),
                    if config_only { " (config only)" } else { "" }.dimmed()
                );
            }
            println!();
        }
    }

    Ok(())
}

fn print_topic_metadata(metadata: &TopicMetadata, ctx: &CliContext) {
    let created_at = chrono::DateTime::from_timestamp_millis(metadata.created_at)
        .map(|dt| dt.to_string())
        .unwrap_or_else(|| "Unknown".to_string());

    let storage_mode = metadata.config.storage_mode.to_string();

    let info = json!({
        "name": metadata.name,
        "partitions": metadata.num_partitions,
        "replication_factor": metadata.replication_factor,
        "created_at": created_at,
        "config": {
            "retention_ms": metadata.config.retention_ms,
            "retention_bytes": metadata.config.retention_bytes,
            "segment_bytes": metadata.config.segment_bytes,
            "cleanup_policy": metadata.config.cleanup_policy.to_string(),
            "storage_mode": storage_mode,
            "remote_storage": metadata.config.remote_storage
        }
    });

    match ctx.format {
        OutputFormat::Json => match serde_json::to_string_pretty(&info) {
            Ok(json_str) => println!("{}", json_str),
            Err(e) => eprintln!("Error formatting topic metadata: {}", e),
        },
        _ => {
            println!();
            println!("  {}: {}", "Name".bold(), metadata.name);
            println!(
                "  {}: {}",
                "Partitions".bold(),
                metadata.num_partitions.to_string().green()
            );
            println!(
                "  {}: {}",
                "Replication".bold(),
                metadata.replication_factor
            );
            println!("  {}: {}", "Created".bold(), created_at.dimmed());
            println!();
            println!("  {}:", "Configuration".bold().underline());
            println!(
                "    {}: {} ms",
                "Retention time".dimmed(),
                metadata.config.retention_ms
            );
            println!(
                "    {}: {} bytes",
                "Retention size".dimmed(),
                metadata.config.retention_bytes
            );
            println!(
                "    {}: {} bytes",
                "Segment size".dimmed(),
                metadata.config.segment_bytes
            );
            println!(
                "    {}: {}",
                "Cleanup policy".dimmed(),
                metadata.config.cleanup_policy
            );
            // Display storage mode with color coding
            let mode_display = match metadata.config.storage_mode {
                StorageMode::Local => storage_mode.green(),
                StorageMode::Hybrid => storage_mode.yellow(),
                StorageMode::Diskless => storage_mode.cyan(),
            };
            println!("    {}: {}", "Storage mode".dimmed(), mode_display);

            // Show remote storage config if present
            if let Some(ref remote) = metadata.config.remote_storage {
                println!();
                println!("  {}:", "Remote Storage".bold().underline());
                println!(
                    "    {}: {} bytes",
                    "Batch size".dimmed(),
                    remote.batch_size_bytes
                );
                println!(
                    "    {}: {} ms",
                    "Batch timeout".dimmed(),
                    remote.batch_timeout_ms
                );
                println!(
                    "    {}: {} bytes",
                    "Read cache".dimmed(),
                    remote.read_cache_bytes
                );
            }
        }
    }
}
