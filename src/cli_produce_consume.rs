use colored::Colorize;
use comfy_table::{presets::UTF8_FULL_CONDENSED, Cell, Color, ContentArrangement, Table};
use serde_json::json;
use std::path::PathBuf;
use std::time::Duration;
use streamline::{storage::Header, Result, TopicManager};

use crate::cli_context::CliContext;
use crate::OutputFormat;

pub(super) fn show_info(ctx: &CliContext) -> Result<()> {
    let version = env!("CARGO_PKG_VERSION");

    match ctx.format {
        OutputFormat::Json => {
            let data_dir = &ctx.data_dir;
            let topics = if data_dir.exists() {
                let manager = TopicManager::new(data_dir)?;
                manager.list_topics()?
            } else {
                vec![]
            };

            let info = json!({
                "version": version,
                "data_dir": data_dir.display().to_string(),
                "topics": topics.iter().map(|t| json!({
                    "name": t.name,
                    "partitions": t.num_partitions
                })).collect::<Vec<_>>()
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&info)?
            );
        }
        _ => {
            println!(
                "{} {}",
                "Streamline".bold().cyan(),
                format!("v{}", version).dimmed()
            );
            println!("{}", "═".repeat(40).dimmed());
            println!();

            let data_dir = &ctx.data_dir;
            if !data_dir.exists() {
                println!(
                    "{}: {} {}",
                    "Data directory".bold(),
                    data_dir.display(),
                    "(not found)".red()
                );
                println!();
                ctx.warn(
                    "No data directory found. Make sure the server has been started at least once.",
                );
                return Ok(());
            }

            let manager = TopicManager::new(data_dir)?;
            let topics = manager.list_topics()?;

            println!("{}: {}", "Data directory".bold(), data_dir.display());
            println!();
            println!("{}: {}", "Topics".bold(), topics.len().to_string().green());

            if !topics.is_empty() {
                let mut table = Table::new();
                table.load_preset(UTF8_FULL_CONDENSED);
                table.set_content_arrangement(ContentArrangement::Dynamic);
                table.set_header(vec![
                    Cell::new("Topic").fg(Color::Cyan),
                    Cell::new("Partitions").fg(Color::Cyan),
                ]);

                for topic in &topics {
                    table.add_row(vec![
                        Cell::new(&topic.name),
                        Cell::new(topic.num_partitions),
                    ]);
                }

                println!();
                println!("{}", table);
            }
        }
    }

    Ok(())
}

/// Expand template placeholders in a string
fn expand_template(template: &str, iteration: u64) -> String {
    use chrono::Utc;
    use uuid::Uuid;

    let mut result = template.to_string();

    // Replace {{uuid}}
    while result.contains("{{uuid}}") {
        result = result.replacen("{{uuid}}", &Uuid::new_v4().to_string(), 1);
    }

    // Replace {{i}} with iteration number
    result = result.replace("{{i}}", &iteration.to_string());

    // Replace {{timestamp}} with Unix timestamp (seconds)
    result = result.replace("{{timestamp}}", &Utc::now().timestamp().to_string());

    // Replace {{timestamp_ms}} with Unix timestamp (milliseconds)
    result = result.replace(
        "{{timestamp_ms}}",
        &Utc::now().timestamp_millis().to_string(),
    );

    // Replace {{now}} with ISO 8601 timestamp
    result = result.replace("{{now}}", &Utc::now().to_rfc3339());

    // Replace {{now:FORMAT}} with custom formatted timestamp
    if let Ok(re_now) = regex::Regex::new(r"\{\{now:([^}]+)\}\}") {
        while let Some(caps) = re_now.captures(&result) {
            let format = &caps[1];
            let formatted = Utc::now().format(format).to_string();
            let Some(m) = caps.get(0) else { break };
            result = result.replacen(m.as_str(), &formatted, 1);
        }
    }

    // Replace {{random}} with random number 0-100
    while result.contains("{{random}}") {
        use rand::Rng;
        let num: u32 = rand::thread_rng().gen_range(0..=100);
        result = result.replacen("{{random}}", &num.to_string(), 1);
    }

    // Replace {{random:MIN:MAX}} with random number in range
    if let Ok(re_random) = regex::Regex::new(r"\{\{random:(\d+):(\d+)\}\}") {
        while let Some(caps) = re_random.captures(&result) {
            use rand::Rng;
            let min: i64 = caps[1].parse().unwrap_or(0);
            let max: i64 = caps[2].parse().unwrap_or(100);
            let num: i64 = rand::thread_rng().gen_range(min..=max);
            let Some(m) = caps.get(0) else { break };
            result = result.replacen(m.as_str(), &num.to_string(), 1);
        }
    }

    result
}

#[derive(Debug, Clone)]
pub(super) struct ProduceOptions {
    pub(super) topic: String,
    pub(super) message: Option<String>,
    pub(super) file: Option<PathBuf>,
    pub(super) partition: i32,
    pub(super) key: Option<String>,
    pub(super) headers: Vec<(String, String)>,
    pub(super) auto_create: bool,
    pub(super) auto_create_partitions: i32,
    pub(super) explain: bool,
    pub(super) template: Option<String>,
    pub(super) count: u64,
}

#[derive(Debug, Clone)]
pub(super) struct ConsumeOptions {
    pub(super) topic: String,
    pub(super) partition: i32,
    pub(super) from_beginning: bool,
    pub(super) offset: Option<i64>,
    pub(super) at: Option<String>,
    pub(super) last: Option<String>,
    pub(super) max_messages: Option<usize>,
    pub(super) follow: bool,
    pub(super) show_keys: bool,
    pub(super) show_headers: bool,
    pub(super) show_timestamps: bool,
    pub(super) show_offsets: bool,
    pub(super) poll_interval: u64,
    pub(super) explain: bool,
    pub(super) grep: Option<String>,
    pub(super) grep_key: bool,
    pub(super) invert_match: bool,
    pub(super) ignore_case: bool,
    pub(super) jq: Option<String>,
    pub(super) sample: Option<String>,
    pub(super) sample_seed: Option<u64>,
}
pub(super) fn handle_produce_command(options: ProduceOptions, ctx: &CliContext) -> Result<()> {
    use bytes::Bytes;
    use streamline::cli_utils::explain::ExplainContext;

    let ProduceOptions {
        topic,
        message,
        file,
        partition,
        key,
        headers,
        auto_create,
        auto_create_partitions,
        explain,
        template,
        count,
    } = options;

    let data_dir = &ctx.data_dir;
    std::fs::create_dir_all(data_dir)?;
    let manager = TopicManager::new(data_dir)?;

    // Auto-create topic if needed
    if auto_create {
        let _ = manager.get_or_create_topic(&topic, auto_create_partitions);
    }

    // Collect messages to produce
    let messages: Vec<String> = if let Some(tmpl) = template {
        // Template mode: generate count messages with template expansion
        (0..count).map(|i| expand_template(&tmpl, i)).collect()
    } else if let Some(msg) = message {
        vec![msg]
    } else if let Some(file_path) = file {
        let content = std::fs::read_to_string(&file_path)?;
        // Try to parse as JSON array first
        if let Ok(arr) = serde_json::from_str::<Vec<String>>(&content) {
            arr
        } else {
            // Otherwise, treat as one message per line
            content.lines().map(|s| s.to_string()).collect()
        }
    } else {
        return Err(streamline::StreamlineError::Config(
            "Either --message, --file, or --template must be provided".into(),
        ));
    };

    // Convert headers
    let record_headers: Vec<Header> = headers
        .into_iter()
        .map(|(k, v)| Header {
            key: k,
            value: Bytes::from(v),
        })
        .collect();

    let key_str = key.clone(); // Keep a copy for explain output
    let key_bytes = key.map(Bytes::from);
    let mut produced_offsets = Vec::new();

    let spinner = if messages.len() > 1 {
        ctx.spinner(&format!(
            "Producing {} messages to '{}'...",
            messages.len(),
            topic
        ))
    } else {
        None
    };

    for msg in &messages {
        let offset = if record_headers.is_empty() {
            manager.append(
                &topic,
                partition,
                key_bytes.clone(),
                Bytes::from(msg.clone()),
            )?
        } else {
            manager.append_with_headers(
                &topic,
                partition,
                key_bytes.clone(),
                Bytes::from(msg.clone()),
                record_headers.clone(),
            )?
        };
        produced_offsets.push(offset);
    }

    if let Some(s) = spinner {
        s.finish_and_clear();
    }

    match ctx.format {
        OutputFormat::Json => {
            let info = if produced_offsets.len() == 1 {
                json!({
                    "status": "produced",
                    "topic": topic,
                    "partition": partition,
                    "offset": produced_offsets[0]
                })
            } else {
                json!({
                    "status": "produced",
                    "topic": topic,
                    "partition": partition,
                    "count": produced_offsets.len(),
                    "offsets": {
                        "first": produced_offsets.first(),
                        "last": produced_offsets.last()
                    }
                })
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&info)?
            );
        }
        _ => {
            if produced_offsets.len() == 1 {
                ctx.success(&format!(
                    "Produced message to {} [{}] at offset {}",
                    topic.bold(),
                    partition.to_string().cyan(),
                    produced_offsets[0].to_string().green()
                ));

                // Show explanation if enabled
                if explain {
                    let mut explain_ctx = ExplainContext::new(true);
                    let msg_size = messages.first().map(|m| m.len()).unwrap_or(0);
                    streamline::cli_utils::explain::explanations::explain_produce(
                        &mut explain_ctx,
                        &topic,
                        partition,
                        produced_offsets[0],
                        key_str.as_deref(),
                        msg_size,
                    );
                }
            } else {
                ctx.success(&format!(
                    "Produced {} messages to {} [{}] at offsets {}-{}",
                    produced_offsets.len().to_string().green(),
                    topic.bold(),
                    partition.to_string().cyan(),
                    produced_offsets.first().unwrap_or(&0),
                    produced_offsets.last().unwrap_or(&0)
                ));
            }
        }
    }

    Ok(())
}

pub(super) fn handle_consume_command(options: ConsumeOptions, ctx: &CliContext) -> Result<()> {
    use jsonpath_rust::JsonPath;
    use rand::{Rng, SeedableRng};
    use regex::RegexBuilder;
    use streamline::cli_utils::{parse_time_expression, ExplainContext};
    use streamline::StreamlineError;

    let ConsumeOptions {
        topic,
        partition,
        from_beginning,
        offset,
        at,
        last,
        max_messages,
        follow,
        show_keys,
        show_headers,
        show_timestamps,
        show_offsets,
        poll_interval,
        explain,
        grep,
        grep_key,
        invert_match,
        ignore_case,
        jq,
        sample,
        sample_seed,
    } = options;

    // Parse sample specification
    enum SampleMode {
        None,
        Percentage(f64),
        Count(usize),
    }
    let sample_mode = if let Some(ref spec) = sample {
        if spec.ends_with('%') {
            let pct_str = spec.trim_end_matches('%');
            let pct: f64 = pct_str.parse().map_err(|_| {
                StreamlineError::Config(format!("Invalid sample percentage: {}", spec))
            })?;
            if !(0.0..=100.0).contains(&pct) {
                return Err(StreamlineError::Config(
                    "Sample percentage must be between 0 and 100".into(),
                ));
            }
            SampleMode::Percentage(pct / 100.0)
        } else {
            let count: usize = spec
                .parse()
                .map_err(|_| StreamlineError::Config(format!("Invalid sample count: {}", spec)))?;
            SampleMode::Count(count)
        }
    } else {
        SampleMode::None
    };

    // Create RNG for sampling
    let mut rng: rand::rngs::StdRng = if let Some(seed) = sample_seed {
        rand::rngs::StdRng::seed_from_u64(seed)
    } else {
        rand::rngs::StdRng::from_entropy()
    };

    // Compile grep pattern if provided
    let grep_regex = if let Some(ref pattern) = grep {
        Some(
            RegexBuilder::new(pattern)
                .case_insensitive(ignore_case)
                .build()
                .map_err(|e| {
                    StreamlineError::Config(format!("Invalid grep pattern '{}': {}", pattern, e))
                })?,
        )
    } else {
        None
    };

    // Parse JSONPath if provided
    let json_path =
        if let Some(ref path) = jq {
            Some(JsonPath::try_from(path.as_str()).map_err(|e| {
                StreamlineError::Config(format!("Invalid JSONPath '{}': {}", path, e))
            })?)
        } else {
            None
        };

    let mut explain_ctx = ExplainContext::new(explain);

    let data_dir = &ctx.data_dir;
    if !data_dir.exists() {
        ctx.info("Make sure the server has been started at least once.");
        return Err(streamline::StreamlineError::Config(format!(
            "Data directory not found: {}",
            data_dir.display()
        )));
    }

    explain_ctx.step_with_details(
        "Connecting to storage",
        format!("Opening TopicManager at {}", data_dir.display()),
    );
    let manager = TopicManager::new(data_dir)?;

    // Determine starting offset based on time travel parameters or traditional options
    let mut current_offset = if let Some(at_expr) = &at {
        // Time travel: --at flag
        explain_ctx.step_with_details(
            "Parsing time expression",
            format!("Interpreting --at '{}'", at_expr),
        );
        let time_expr = parse_time_expression(at_expr).map_err(|e| {
            StreamlineError::Config(format!("Invalid time expression '{}': {}", at_expr, e))
        })?;

        let target_timestamp = time_expr.to_timestamp_ms();
        explain_ctx.step_with_details(
            "Finding offset",
            format!("Searching for offset at timestamp {} ms", target_timestamp),
        );

        // Find offset for timestamp (we'll search linearly for now)
        let earliest = manager.earliest_offset(&topic, partition)?;
        let latest = manager.latest_offset(&topic, partition)?;

        // Binary search would be more efficient, but for now start from earliest
        // and find the first record >= target timestamp
        let mut found_offset = earliest;
        let records = manager.read(
            &topic,
            partition,
            earliest,
            (latest - earliest + 1) as usize,
        )?;
        for record in &records {
            if record.timestamp >= target_timestamp {
                found_offset = record.offset;
                break;
            }
        }
        explain_ctx.step_with_details(
            "Offset resolved",
            format!(
                "Starting from offset {} (timestamp >= {})",
                found_offset, target_timestamp
            ),
        );
        found_offset
    } else if let Some(last_expr) = &last {
        // Time travel: --last flag (e.g., "5m", "1h")
        explain_ctx.step_with_details(
            "Parsing duration",
            format!("Interpreting --last '{}'", last_expr),
        );
        let time_expr = parse_time_expression(last_expr).map_err(|e| {
            StreamlineError::Config(format!("Invalid duration '{}': {}", last_expr, e))
        })?;

        let target_timestamp = time_expr.to_timestamp_ms();
        explain_ctx.step_with_details(
            "Finding offset",
            format!(
                "Searching for messages from {} ms ago",
                chrono::Utc::now().timestamp_millis() - target_timestamp
            ),
        );

        let earliest = manager.earliest_offset(&topic, partition)?;
        let latest = manager.latest_offset(&topic, partition)?;

        let mut found_offset = earliest;
        let records = manager.read(
            &topic,
            partition,
            earliest,
            (latest - earliest + 1) as usize,
        )?;
        for record in &records {
            if record.timestamp >= target_timestamp {
                found_offset = record.offset;
                break;
            }
        }
        explain_ctx.step_with_details(
            "Offset resolved",
            format!("Starting from offset {}", found_offset),
        );
        found_offset
    } else if from_beginning {
        explain_ctx.step_with_details(
            "Starting position",
            "Using --from-beginning: starting at earliest offset",
        );
        manager.earliest_offset(&topic, partition)?
    } else if let Some(off) = offset {
        explain_ctx.step_with_details(
            "Starting position",
            format!("Using explicit offset: {}", off),
        );
        off
    } else {
        explain_ctx.step_with_details(
            "Starting position",
            "No position specified: starting at latest offset",
        );
        // Default: start from latest
        manager.latest_offset(&topic, partition)?
    };

    explain_ctx.step_with_details(
        "Beginning consumption",
        format!("Reading from topic '{}' partition {}", topic, partition),
    );

    // Print explanation if enabled
    explain_ctx.print();

    let batch_size = 100;
    let mut consumed_count = 0usize;

    // For JSON output, collect all records
    let mut all_records_json: Vec<serde_json::Value> = Vec::new();

    loop {
        let remaining = max_messages.map(|max| max.saturating_sub(consumed_count));
        if remaining == Some(0) {
            break;
        }

        let fetch_count = remaining.unwrap_or(batch_size).min(batch_size);
        let records = manager.read(&topic, partition, current_offset, fetch_count)?;

        if records.is_empty() {
            if follow {
                std::thread::sleep(Duration::from_millis(poll_interval));
                continue;
            } else {
                break;
            }
        }

        for record in &records {
            current_offset = record.offset + 1;

            // Apply grep filtering if pattern is provided
            if let Some(ref regex) = grep_regex {
                let value_str = String::from_utf8_lossy(&record.value);
                let key_str = record
                    .key
                    .as_ref()
                    .map(|k| String::from_utf8_lossy(k).to_string());

                let value_matches = regex.is_match(&value_str);
                let key_matches =
                    grep_key && key_str.as_ref().map(|k| regex.is_match(k)).unwrap_or(false);

                let matches = value_matches || key_matches;
                let should_show = if invert_match { !matches } else { matches };

                if !should_show {
                    continue;
                }
            }

            // Apply sampling if configured
            match sample_mode {
                SampleMode::Percentage(pct) => {
                    if rng.gen::<f64>() > pct {
                        continue;
                    }
                }
                SampleMode::Count(count) => {
                    if consumed_count >= count {
                        break;
                    }
                }
                SampleMode::None => {}
            }

            consumed_count += 1;

            match ctx.format {
                OutputFormat::Json => {
                    let value_str = String::from_utf8_lossy(&record.value).to_string();

                    // Apply JSONPath extraction if provided
                    let display_value: serde_json::Value = if let Some(ref jp) = json_path {
                        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&value_str) {
                            let results = jp.find(&parsed);
                            // find() returns an array of matches
                            match &results {
                                serde_json::Value::Array(arr) if arr.is_empty() => json!(null),
                                serde_json::Value::Array(arr) if arr.len() == 1 => arr[0].clone(),
                                other => other.clone(),
                            }
                        } else {
                            json!({"_error": "not JSON", "_raw": value_str})
                        }
                    } else {
                        // Try to parse as JSON, otherwise use string
                        serde_json::from_str(&value_str).unwrap_or_else(|_| json!(value_str))
                    };

                    let mut rec_json = json!({
                        "value": display_value,
                        "offset": record.offset,
                        "timestamp": record.timestamp
                    });

                    if show_keys {
                        rec_json["key"] = record
                            .key
                            .as_ref()
                            .map(|k| String::from_utf8_lossy(k).to_string())
                            .map(|s| json!(s))
                            .unwrap_or(json!(null));
                    }
                    if show_headers && !record.headers.is_empty() {
                        rec_json["headers"] = json!(record
                            .headers
                            .iter()
                            .map(|h| json!({
                                "key": h.key,
                                "value": String::from_utf8_lossy(&h.value).to_string()
                            }))
                            .collect::<Vec<_>>());
                    }

                    all_records_json.push(rec_json);
                }
                _ => {
                    let mut prefix_parts = Vec::new();

                    if show_offsets {
                        prefix_parts.push(format!("[{}]", record.offset.to_string().dimmed()));
                    }
                    if show_timestamps {
                        let ts = chrono::DateTime::from_timestamp_millis(record.timestamp)
                            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                            .unwrap_or_else(|| record.timestamp.to_string());
                        prefix_parts.push(format!("{}", ts.dimmed()));
                    }
                    if show_keys {
                        let key_str = record
                            .key
                            .as_ref()
                            .map(|k| String::from_utf8_lossy(k).to_string())
                            .unwrap_or_else(|| "<null>".to_string());
                        prefix_parts.push(format!("{}", key_str.cyan()));
                    }

                    let value_str = String::from_utf8_lossy(&record.value);

                    // Apply JSONPath extraction if provided
                    let display_value = if let Some(ref jp) = json_path {
                        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&value_str) {
                            let results = jp.find(&parsed);
                            // find() returns an array of matches
                            match &results {
                                serde_json::Value::Array(arr) if arr.is_empty() => {
                                    "(no match)".dimmed().to_string()
                                }
                                serde_json::Value::Array(arr) if arr.len() == 1 => match &arr[0] {
                                    serde_json::Value::String(s) => s.clone(),
                                    other => other.to_string(),
                                },
                                other => serde_json::to_string(other)
                                    .unwrap_or_else(|_| "(error)".to_string()),
                            }
                        } else {
                            format!("{} {}", "(not JSON)".red(), value_str)
                        }
                    } else {
                        value_str.to_string()
                    };

                    if prefix_parts.is_empty() {
                        println!("{}", display_value);
                    } else {
                        println!("{} {}", prefix_parts.join(" "), display_value);
                    }

                    if show_headers && !record.headers.is_empty() {
                        for header in &record.headers {
                            println!(
                                "  {}: {}",
                                header.key.dimmed(),
                                String::from_utf8_lossy(&header.value)
                            );
                        }
                    }
                }
            }

            if max_messages
                .map(|max| consumed_count >= max)
                .unwrap_or(false)
            {
                break;
            }
        }

        // In follow mode, continue; otherwise check if we should stop
        if !follow && records.len() < fetch_count {
            break;
        }
    }

    // Print JSON output at the end
    if matches!(ctx.format, OutputFormat::Json) {
        println!(
            "{}",
            serde_json::to_string_pretty(&all_records_json)?
        );
    }

    // Print summary in text mode
    if matches!(ctx.format, OutputFormat::Text) && !follow && consumed_count > 0 {
        eprintln!(
            "\n{}: Consumed {} message(s) from {} [{}]",
            "Summary".dimmed(),
            consumed_count.to_string().green(),
            topic,
            partition
        );
    }

    Ok(())
}

#[cfg(test)]
mod produce_consume_option_tests {
    use super::{ConsumeOptions, ProduceOptions};

    #[test]
    fn test_produce_options_clone() {
        let options = ProduceOptions {
            topic: "test-topic".to_string(),
            message: Some("msg".to_string()),
            file: None,
            partition: 0,
            key: None,
            headers: vec![("k".to_string(), "v".to_string())],
            auto_create: true,
            auto_create_partitions: 3,
            explain: false,
            template: None,
            count: 1,
        };

        let cloned = options.clone();
        assert_eq!(cloned.topic, "test-topic");
        assert_eq!(cloned.message.as_deref(), Some("msg"));
        assert_eq!(cloned.partition, 0);
        assert_eq!(cloned.auto_create_partitions, 3);
    }

    #[test]
    fn test_consume_options_clone() {
        let options = ConsumeOptions {
            topic: "test-topic".to_string(),
            partition: 1,
            from_beginning: true,
            offset: Some(5),
            at: None,
            last: None,
            max_messages: Some(10),
            follow: false,
            show_keys: true,
            show_headers: false,
            show_timestamps: true,
            show_offsets: true,
            poll_interval: 250,
            explain: false,
            grep: Some("warn".to_string()),
            grep_key: false,
            invert_match: false,
            ignore_case: false,
            jq: None,
            sample: None,
            sample_seed: None,
        };

        let cloned = options.clone();
        assert_eq!(cloned.topic, "test-topic");
        assert_eq!(cloned.partition, 1);
        assert_eq!(cloned.offset, Some(5));
        assert_eq!(cloned.max_messages, Some(10));
        assert_eq!(cloned.grep.as_deref(), Some("warn"));
    }
}
