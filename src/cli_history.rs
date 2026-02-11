use colored::Colorize;
use serde_json::json;
use std::path::PathBuf;

use crate::cli_context::CliContext;
use crate::OutputFormat;
use streamline::Result;

/// Get the history file path
fn get_history_file() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("streamline")
        .join("cli_history.txt")
}

/// Save a command to history
pub(crate) fn save_to_history(args: &[String]) {
    // Skip saving "history" commands themselves
    if args.iter().any(|a| a == "history") {
        return;
    }

    let history_file = get_history_file();

    // Create directory if needed
    if let Some(parent) = history_file.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    // Read existing history
    let mut history: Vec<String> = std::fs::read_to_string(&history_file)
        .map(|s| s.lines().map(String::from).collect())
        .unwrap_or_default();

    // Add new command with timestamp
    let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
    let command = args[1..].join(" "); // Skip the binary name
    let entry = format!("[{}] {}", timestamp, command);

    history.push(entry);

    // Keep last 100 commands
    if history.len() > 100 {
        let skip_count = history.len() - 100;
        history = history.into_iter().skip(skip_count).collect();
    }

    // Write back
    let _ = std::fs::write(&history_file, history.join("\n") + "\n");
}

/// Handle history command
pub(crate) fn handle_history(count: usize, clear: bool, ctx: &CliContext) -> Result<()> {
    let history_file = get_history_file();

    if clear {
        if history_file.exists() {
            std::fs::remove_file(&history_file)?;
            ctx.success("Command history cleared");
        } else {
            ctx.info("No history to clear");
        }
        return Ok(());
    }

    if !history_file.exists() {
        ctx.info("No command history yet");
        return Ok(());
    }

    let content = std::fs::read_to_string(&history_file)?;
    let lines: Vec<&str> = content.lines().collect();

    if lines.is_empty() {
        ctx.info("No command history yet");
        return Ok(());
    }

    match ctx.format {
        OutputFormat::Json => {
            let history: Vec<serde_json::Value> = lines
                .iter()
                .rev()
                .take(count)
                .rev()
                .enumerate()
                .map(|(i, line)| {
                    // Parse "[timestamp] command" format
                    let (timestamp, command) = if line.starts_with('[') {
                        if let Some(end) = line.find(']') {
                            (line[1..end].to_string(), line[end + 2..].to_string())
                        } else {
                            (String::new(), line.to_string())
                        }
                    } else {
                        (String::new(), line.to_string())
                    };

                    json!({
                        "index": lines.len().saturating_sub(count) + i + 1,
                        "timestamp": timestamp,
                        "command": command
                    })
                })
                .collect();

            println!(
                "{}",
                serde_json::to_string_pretty(&history)?
            );
        }
        _ => {
            println!("{}", "Recent Commands".bold().cyan());
            println!("{}", "═".repeat(60).dimmed());

            let start = lines.len().saturating_sub(count);
            for (i, line) in lines.iter().skip(start).enumerate() {
                let num = start + i + 1;
                // Color the timestamp part if present
                if line.starts_with('[') {
                    if let Some(end) = line.find(']') {
                        print!("{:>4}  ", num.to_string().dimmed());
                        print!("{}", line[..=end].dimmed());
                        println!(" {}", &line[end + 2..]);
                    } else {
                        println!("{:>4}  {}", num.to_string().dimmed(), line);
                    }
                } else {
                    println!("{:>4}  {}", num.to_string().dimmed(), line);
                }
            }

            println!();
            println!(
                "{}: {} commands in history",
                "Total".dimmed(),
                lines.len().to_string().cyan()
            );
        }
    }

    Ok(())
}
