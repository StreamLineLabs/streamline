//! Quickstart wizard for zero-to-hero experience
//!
//! Provides an interactive quickstart that gets developers up and running
//! in seconds with a working Streamline setup.

use crate::cli_utils::profile_store::{
    get_default_profile, load_profiles, save_profiles, set_default_profile, ConnectionProfile,
};
use crate::{StreamlineError, TopicManager};
use bytes::Bytes;
use colored::Colorize;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, Read, Write};
use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Sample messages for demo topics
const SAMPLE_MESSAGES: &[(&str, &str, &str)] = &[
    (
        "orders",
        "order-001",
        r#"{"id": "order-001", "customer": "alice", "total": 99.99, "status": "pending"}"#,
    ),
    (
        "orders",
        "order-002",
        r#"{"id": "order-002", "customer": "bob", "total": 149.50, "status": "shipped"}"#,
    ),
    (
        "orders",
        "order-003",
        r#"{"id": "order-003", "customer": "charlie", "total": 29.99, "status": "delivered"}"#,
    ),
    (
        "events",
        "evt-001",
        r#"{"type": "user.signup", "user_id": "u123", "timestamp": "2024-01-15T10:30:00Z"}"#,
    ),
    (
        "events",
        "evt-002",
        r#"{"type": "user.login", "user_id": "u123", "timestamp": "2024-01-15T10:35:00Z"}"#,
    ),
    (
        "events",
        "evt-003",
        r#"{"type": "purchase", "user_id": "u123", "amount": 99.99, "timestamp": "2024-01-15T10:40:00Z"}"#,
    ),
    (
        "logs",
        "log-001",
        r#"{"level": "INFO", "service": "api", "message": "Request received", "request_id": "req-123"}"#,
    ),
    (
        "logs",
        "log-002",
        r#"{"level": "DEBUG", "service": "api", "message": "Processing order", "order_id": "order-001"}"#,
    ),
    (
        "logs",
        "log-003",
        r#"{"level": "INFO", "service": "api", "message": "Order confirmed", "order_id": "order-001"}"#,
    ),
];

const SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const SERVER_STARTUP_INTERVAL: Duration = Duration::from_millis(200);

/// Configuration for quickstart
pub struct QuickstartConfig {
    /// Data directory
    pub data_dir: PathBuf,
    /// Whether to create sample data
    pub with_sample_data: bool,
    /// Whether to show interactive demo
    pub interactive: bool,
    /// Whether to auto-start the server if missing
    pub start_server: bool,
    /// Server address for connectivity check
    pub server_addr: String,
    /// HTTP API address for health check
    pub http_addr: String,
}

impl Default for QuickstartConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            with_sample_data: true,
            interactive: true,
            start_server: true,
            server_addr: "localhost:9092".into(),
            http_addr: "localhost:9094".into(),
        }
    }
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    body: String,
}

enum QuickstartBackend {
    Local { manager: Box<TopicManager> },
    Http { http_addr: String },
}

#[derive(Debug)]
struct QuickstartRecord {
    offset: i64,
    value: String,
}

#[derive(Debug, Serialize)]
struct HttpProduceRequest {
    records: Vec<HttpProduceRecord>,
}

#[derive(Debug, Serialize)]
struct HttpProduceRecord {
    key: Option<String>,
    value: Value,
    partition: Option<i32>,
    #[serde(default)]
    headers: std::collections::HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct HttpProduceResponse {
    offsets: Vec<HttpProduceOffset>,
}

#[derive(Debug, Deserialize)]
struct HttpProduceOffset {
    offset: i64,
}

#[derive(Debug, Deserialize)]
struct HttpConsumeResponse {
    records: Vec<HttpConsumeRecord>,
}

#[derive(Debug, Deserialize)]
struct HttpConsumeRecord {
    offset: i64,
    value: Value,
}

/// Run the quickstart wizard
pub fn run_quickstart(config: &QuickstartConfig) -> crate::Result<()> {
    print_banner();

    // Step 1: Check if server is running
    println!();
    print_step(1, "Checking Streamline server...");

    validate_addr("server", &config.server_addr)?;
    validate_addr("http", &config.http_addr)?;
    std::fs::create_dir_all(&config.data_dir)?;

    let mut server_running = check_server(&config.server_addr);
    if server_running {
        println!(
            "  {} Server is running at {}",
            "✓".green(),
            config.server_addr.cyan()
        );
    } else {
        println!(
            "  {} Server not detected at {}",
            "!".yellow(),
            config.server_addr
        );

        if config.start_server {
            println!();
            println!("  {} Starting server in background...", "→".cyan());
            if let Some(pid) = start_server(config)? {
                server_running = wait_for_server(&config.server_addr, SERVER_STARTUP_TIMEOUT);
                if server_running {
                    println!(
                        "  {} Server started (pid {})",
                        "✓".green(),
                        pid.to_string().cyan()
                    );
                } else {
                    println!("  {} Server did not become ready in time.", "✗".red());
                    print_diagnostics_hint(&config.server_addr, &config.http_addr);
                }
            }
        }

        if !server_running {
            println!();
            print_server_instructions(config);

            if config.interactive {
                print!("  Press Enter when server is started (or Ctrl+C to exit)...");
                io::stdout().flush().ok();
                let mut input = String::new();
                io::stdin().read_line(&mut input).ok();

                if !check_server(&config.server_addr) {
                    println!(
                        "  {} Server still not detected. Please start it first.",
                        "✗".red()
                    );
                    print_diagnostics_hint(&config.server_addr, &config.http_addr);
                    return Ok(());
                }
                server_running = true;
            } else {
                print_diagnostics_hint(&config.server_addr, &config.http_addr);
                return Ok(());
            }
        }
    }

    let http_ready = if server_running {
        wait_for_http(&config.http_addr, SERVER_STARTUP_TIMEOUT)
    } else {
        false
    };

    if server_running && !http_ready {
        println!(
            "  {} HTTP API not detected at {}. Falling back to local data dir.",
            "!".yellow(),
            config.http_addr
        );
    }

    let backend = if http_ready {
        QuickstartBackend::Http {
            http_addr: config.http_addr.clone(),
        }
    } else {
        QuickstartBackend::Local {
            manager: Box::new(TopicManager::new(&config.data_dir)?),
        }
    };

    // Step 2: Create demo topics
    println!();
    print_step(2, "Setting up demo topics...");

    let demo_topics = [
        ("orders", 3, "E-commerce orders"),
        ("events", 3, "Application events"),
        ("logs", 1, "System logs"),
    ];

    for (name, partitions, description) in &demo_topics {
        match backend.create_topic(name, *partitions) {
            Ok(_) => println!(
                "  {} Topic '{}' ({}) - {} partitions",
                "✓".green(),
                name.cyan(),
                description.dimmed(),
                partitions
            ),
            Err(e) => println!(
                "  {} Topic '{}': {}",
                "!".yellow(),
                name,
                e.to_string().dimmed()
            ),
        }
    }

    // Step 3: Add sample data
    if config.with_sample_data {
        println!();
        print_step(3, "Adding sample messages...");

        let mut counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();

        for (topic, key, value) in SAMPLE_MESSAGES {
            match backend.append_message(topic, Some(key), value) {
                Ok(_) => {
                    *counts.entry(*topic).or_insert(0) += 1;
                }
                Err(e) => {
                    println!(
                        "  {} Failed to add message to '{}': {}",
                        "!".yellow(),
                        topic,
                        e
                    );
                }
            }
        }

        for (topic, count) in &counts {
            println!(
                "  {} Added {} sample message(s) to '{}'",
                "✓".green(),
                count.to_string().cyan(),
                topic
            );
        }
    }

    if let Err(error) = ensure_default_profile(config) {
        println!(
            "  {} Failed to write CLI profile: {}",
            "!".yellow(),
            error.to_string().dimmed()
        );
    }

    // Step 4: Show quick examples
    println!();
    print_step(4, "You're all set! Try these commands:");
    println!();

    print_example_section(
        "Consume messages",
        &[
            (
                "streamline-cli consume orders --from-beginning",
                "Read all orders",
            ),
            (
                "streamline-cli consume events -f",
                "Follow events in real-time",
            ),
            (
                "streamline-cli consume logs --last 5m",
                "Logs from last 5 minutes",
            ),
        ],
    );

    print_example_section(
        "Produce messages",
        &[
            (
                "streamline-cli produce orders -m '{\"id\": \"test\"}'",
                "Send a message",
            ),
            (
                "streamline-cli produce orders -m 'hello' -k 'my-key'",
                "With a key",
            ),
        ],
    );

    print_example_section(
        "Explore",
        &[
            ("streamline-cli topics list", "List all topics"),
            ("streamline-cli topics describe orders", "Topic details"),
            ("streamline-cli doctor", "System diagnostics"),
        ],
    );

    // Interactive demo
    if config.interactive {
        println!();
        println!("{}", "─".repeat(60).dimmed());
        println!();
        print!("  Would you like to see a live demo? [{}] ", "Y/n".cyan());
        io::stdout().flush().ok();

        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_ok() {
            let input = input.trim().to_lowercase();
            if input.is_empty() || input == "y" || input == "yes" {
                run_interactive_demo(&backend)?;
            }
        }

        // Offer to generate a sample project
        println!();
        println!("{}", "─".repeat(60).dimmed());
        println!();
        print!(
            "  Would you like to generate a sample project? [{}] ",
            "Y/n".cyan()
        );
        io::stdout().flush().ok();

        let mut proj_input = String::new();
        if io::stdin().read_line(&mut proj_input).is_ok() {
            let proj_input = proj_input.trim().to_lowercase();
            if proj_input.is_empty() || proj_input == "y" || proj_input == "yes" {
                crate::cli_utils::quickstart_wizard::run_quickstart_wizard()?;
            }
        }
    }

    println!();
    println!(
        "  🚀 Happy streaming! {}",
        "https://github.com/josedab/streamline".dimmed()
    );
    println!();

    Ok(())
}

impl QuickstartBackend {
    fn create_topic(&self, name: &str, partitions: i32) -> crate::Result<()> {
        match self {
            QuickstartBackend::Local { manager } => {
                manager.get_or_create_topic(name, partitions)?;
                Ok(())
            }
            QuickstartBackend::Http { http_addr } => {
                let payload = serde_json::json!({
                    "name": name,
                    "partitions": partitions,
                });
                let response = http_request(
                    http_addr,
                    "POST",
                    "/api/v1/topics",
                    Some(&payload.to_string()),
                )?;
                if response.status == 409 {
                    return Ok(());
                }
                if !(200..300).contains(&response.status) {
                    return Err(StreamlineError::Network(format!(
                        "HTTP {} creating topic '{}': {}",
                        response.status,
                        name,
                        response.body.trim()
                    )));
                }
                Ok(())
            }
        }
    }

    fn append_message(&self, topic: &str, key: Option<&str>, value: &str) -> crate::Result<i64> {
        match self {
            QuickstartBackend::Local { manager } => manager.append(
                topic,
                0,
                key.map(|k| Bytes::from(k.as_bytes().to_vec())),
                Bytes::from(value.to_string()),
            ),
            QuickstartBackend::Http { http_addr } => {
                let record = HttpProduceRecord {
                    key: key.map(str::to_string),
                    value: parse_message_value(value),
                    partition: Some(0),
                    headers: std::collections::HashMap::new(),
                };
                let payload = HttpProduceRequest {
                    records: vec![record],
                };
                let response = http_request(
                    http_addr,
                    "POST",
                    &format!("/api/v1/topics/{}/messages", topic),
                    Some(&serde_json::to_string(&payload)?),
                )?;
                if !(200..300).contains(&response.status) {
                    return Err(StreamlineError::Network(format!(
                        "HTTP {} producing to '{}': {}",
                        response.status,
                        topic,
                        response.body.trim()
                    )));
                }
                let parsed: HttpProduceResponse = serde_json::from_str(&response.body)?;
                Ok(parsed.offsets.first().map(|o| o.offset).unwrap_or(0))
            }
        }
    }

    fn read_messages(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        limit: usize,
    ) -> crate::Result<Vec<QuickstartRecord>> {
        match self {
            QuickstartBackend::Local { manager } => {
                let records = manager.read(topic, partition, offset, limit)?;
                Ok(records
                    .into_iter()
                    .map(|record| QuickstartRecord {
                        offset: record.offset,
                        value: String::from_utf8_lossy(&record.value).to_string(),
                    })
                    .collect())
            }
            QuickstartBackend::Http { http_addr } => {
                let path = format!(
                    "/api/v1/topics/{}/partitions/{}/messages?offset={}&limit={}",
                    topic, partition, offset, limit
                );
                let response = http_request(http_addr, "GET", &path, None)?;
                if !(200..300).contains(&response.status) {
                    return Err(StreamlineError::Network(format!(
                        "HTTP {} consuming from '{}': {}",
                        response.status,
                        topic,
                        response.body.trim()
                    )));
                }
                let parsed: HttpConsumeResponse = serde_json::from_str(&response.body)?;
                Ok(parsed
                    .records
                    .into_iter()
                    .map(|record| QuickstartRecord {
                        offset: record.offset,
                        value: value_to_string(record.value),
                    })
                    .collect())
            }
        }
    }
}

fn parse_message_value(value: &str) -> Value {
    serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_string()))
}

fn value_to_string(value: Value) -> String {
    match value {
        Value::String(value) => value,
        other => other.to_string(),
    }
}

fn http_request(
    addr: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> crate::Result<HttpResponse> {
    let mut stream = std::net::TcpStream::connect(addr).map_err(|err| {
        StreamlineError::Network(format!("Failed to connect to {}: {}", addr, err))
    })?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(5))).ok();

    let body_str = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nUser-Agent: streamline-cli\r\nAccept: application/json\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{body}",
        method = method,
        path = path,
        addr = addr,
        len = body_str.len(),
        body = body_str
    );

    stream.write_all(request.as_bytes())?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;

    let status_line = response.lines().next().unwrap_or("");
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| {
            StreamlineError::Network(format!(
                "Invalid HTTP response from {}: {}",
                addr, status_line
            ))
        })?;

    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();

    Ok(HttpResponse { status, body })
}

fn check_http_server(addr: &str) -> bool {
    match http_request(addr, "GET", "/health", None) {
        Ok(response) => (200..300).contains(&response.status),
        Err(_) => false,
    }
}

fn wait_for_http(addr: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if check_http_server(addr) {
            return true;
        }
        std::thread::sleep(SERVER_STARTUP_INTERVAL);
    }
    false
}

fn ensure_default_profile(config: &QuickstartConfig) -> crate::Result<()> {
    let mut profiles = load_profiles()?;
    let profile_name = "local".to_string();

    if !profiles.contains_key(&profile_name) {
        let profile = ConnectionProfile {
            name: Some(profile_name.clone()),
            data_dir: Some(config.data_dir.display().to_string()),
            server_addr: Some(config.server_addr.clone()),
            http_addr: Some(config.http_addr.clone()),
            description: Some("Quickstart profile".to_string()),
        };
        profiles.insert(profile_name.clone(), profile);
        save_profiles(&profiles)?;
        println!(
            "  {} Saved CLI profile '{}'",
            "✓".green(),
            profile_name.cyan()
        );
    }

    if get_default_profile().is_none() {
        set_default_profile(&profile_name)?;
        println!("  {}", "(set as default profile)".dimmed());
    }

    Ok(())
}

fn print_diagnostics_hint(server_addr: &str, http_addr: &str) {
    println!("  Troubleshooting:");
    println!(
        "    {} doctor --server-addr {} --http-addr {}",
        "streamline-cli".cyan(),
        server_addr.cyan(),
        http_addr.cyan()
    );
    println!("    curl http://{}/health", http_addr);
    println!("    See docs/TROUBLESHOOTING.md");
}

fn print_banner() {
    println!();
    println!(
        "{}",
        r#"
   _____ _                            _ _
  / ____| |                          | (_)
 | (___ | |_ _ __ ___  __ _ _ __ ___ | |_ _ __   ___
  \___ \| __| '__/ _ \/ _` | '_ ` _ \| | | '_ \ / _ \
  ____) | |_| | |  __/ (_| | | | | | | | | | | |  __/
 |_____/ \__|_|  \___|\__,_|_| |_| |_|_|_|_| |_|\___|

"#
        .cyan()
    );
    println!(
        "  {}",
        "The Redis of Streaming - Zero to Streaming in 30 Seconds"
            .bold()
            .white()
    );
    println!("{}", "═".repeat(60).dimmed());
}

fn print_step(num: u32, message: &str) {
    println!(
        "  {} {}",
        format!("[{}/4]", num).cyan().bold(),
        message.bold()
    );
}

fn print_example_section(title: &str, examples: &[(&str, &str)]) {
    println!("  {}", title.bold());
    for (cmd, desc) in examples {
        println!("    {} {}", cmd.cyan(), format!("# {}", desc).dimmed());
    }
    println!();
}

fn check_server(addr: &str) -> bool {
    let addrs = match addr.to_socket_addrs() {
        Ok(addrs) => addrs,
        Err(_) => return false,
    };
    for addr in addrs {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(1)).is_ok() {
            return true;
        }
    }
    false
}

fn validate_addr(label: &str, addr: &str) -> crate::Result<()> {
    match addr.to_socket_addrs().map(|mut addrs| addrs.next()) {
        Ok(Some(_)) => Ok(()),
        _ => Err(StreamlineError::Config(format!(
            "Invalid {} address: {}",
            label, addr
        ))),
    }
}

fn start_server(config: &QuickstartConfig) -> crate::Result<Option<u32>> {
    let binary = match resolve_streamline_binary() {
        Some(path) => path,
        None => {
            println!(
                "  {} Streamline binary not found. Install with Homebrew or build from source.",
                "!".yellow()
            );
            println!("  {} {}", "→".cyan(), "cargo build --release".dimmed());
            return Ok(None);
        }
    };

    let child = Command::new(binary)
        .arg("--data-dir")
        .arg(&config.data_dir)
        .arg("--listen-addr")
        .arg(&config.server_addr)
        .arg("--http-addr")
        .arg(&config.http_addr)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();

    match child {
        Ok(child) => {
            let pid = child.id();
            std::mem::forget(child);
            Ok(Some(pid))
        }
        Err(err) => {
            println!(
                "  {} Failed to start server: {}",
                "!".yellow(),
                err.to_string().dimmed()
            );
            Ok(None)
        }
    }
}

fn resolve_streamline_binary() -> Option<PathBuf> {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let candidate = dir.join("streamline");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }

    if let Ok(cwd) = std::env::current_dir() {
        let candidates = [
            cwd.join("streamline"),
            cwd.join("target").join("debug").join("streamline"),
            cwd.join("target").join("release").join("streamline"),
        ];
        if let Some(found) = candidates.into_iter().find(|path| path.is_file()) {
            return Some(found);
        }
    }

    for dir in std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()) {
        let candidate = dir.join("streamline");
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    None
}

fn wait_for_server(addr: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if check_server(addr) {
            return true;
        }
        std::thread::sleep(SERVER_STARTUP_INTERVAL);
    }
    false
}

fn print_server_instructions(config: &QuickstartConfig) {
    println!("  Run in another terminal:");
    println!(
        "    {}",
        format!(
            "streamline --data-dir {} --listen-addr {} --http-addr {}",
            config.data_dir.display(),
            config.server_addr,
            config.http_addr
        )
        .cyan()
    );
}

fn run_interactive_demo(backend: &QuickstartBackend) -> crate::Result<()> {
    println!();
    println!("{}", "─".repeat(60).dimmed());
    println!();
    println!("  {}", "Live Demo".bold().cyan());
    println!();

    // Produce a message
    println!("  {} Producing a message to 'orders'...", "→".cyan());
    std::thread::sleep(Duration::from_millis(500));

    let demo_msg =
        r#"{"id": "demo-001", "customer": "quickstart-user", "total": 42.00, "status": "new"}"#;
    let offset = backend.append_message("orders", Some("demo-001"), demo_msg)?;

    println!(
        "  {} Produced message at offset {}",
        "✓".green(),
        offset.to_string().cyan()
    );
    println!();
    println!("    {}", demo_msg.dimmed());
    println!();

    // Consume messages
    println!("  {} Reading messages from 'orders'...", "→".cyan());
    std::thread::sleep(Duration::from_millis(500));

    let records = backend.read_messages("orders", 0, 0, 5)?;
    println!(
        "  {} Found {} message(s):",
        "✓".green(),
        records.len().to_string().cyan()
    );
    println!();

    for (i, record) in records.iter().take(3).enumerate() {
        let preview = if record.value.len() > 60 {
            format!("{}...", &record.value[..60])
        } else {
            record.value.to_string()
        };
        println!(
            "    {} [offset {}] {}",
            format!("{}.", i + 1).dimmed(),
            record.offset.to_string().cyan(),
            preview.dimmed()
        );
    }

    if records.len() > 3 {
        println!("    {} ... and {} more", "".dimmed(), records.len() - 3);
    }

    println!();
    println!(
        "  {} Demo complete! Now try the commands above.",
        "✓".green()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_quickstart_creates_topics() {
        let temp_dir = TempDir::new().unwrap();
        let config = QuickstartConfig {
            data_dir: temp_dir.path().to_path_buf(),
            with_sample_data: false,
            interactive: false,
            start_server: false,
            server_addr: "localhost:19092".into(), // Non-existent port
            http_addr: "localhost:19094".into(),
        };

        // This should succeed even without server (local storage)
        let manager = TopicManager::new(&config.data_dir).unwrap();
        manager.get_or_create_topic("test", 1).unwrap();

        let topics = manager.list_topics().unwrap();
        assert!(topics.iter().any(|t| t.name == "test"));
    }
}
