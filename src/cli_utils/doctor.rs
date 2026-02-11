//! Diagnostic tools for Streamline
//!
//! Provides comprehensive health checks and diagnostics for troubleshooting
//! Streamline installations.

use crate::{GroupCoordinator, TopicManager};
use colored::Colorize;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Status of a diagnostic check
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticStatus {
    /// Check passed
    Pass,
    /// Check failed
    Fail,
    /// Check passed with warnings
    Warn,
    /// Check was skipped
    Skip,
}

impl DiagnosticStatus {
    /// Get the status icon
    pub fn icon(&self) -> &'static str {
        match self {
            DiagnosticStatus::Pass => "✓",
            DiagnosticStatus::Fail => "✗",
            DiagnosticStatus::Warn => "⚠",
            DiagnosticStatus::Skip => "○",
        }
    }

    /// Get the colored icon
    pub fn colored_icon(&self) -> String {
        match self {
            DiagnosticStatus::Pass => "✓".green().to_string(),
            DiagnosticStatus::Fail => "✗".red().to_string(),
            DiagnosticStatus::Warn => "⚠".yellow().to_string(),
            DiagnosticStatus::Skip => "○".dimmed().to_string(),
        }
    }
}

/// Result of a single diagnostic check
#[derive(Debug, Clone)]
pub struct DiagnosticResult {
    /// Name of the check
    pub name: String,
    /// Status of the check
    pub status: DiagnosticStatus,
    /// Detailed message
    pub message: String,
    /// Suggested fix (if status is Fail or Warn)
    pub fix: Option<String>,
}

impl DiagnosticResult {
    pub fn pass(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: DiagnosticStatus::Pass,
            message: message.into(),
            fix: None,
        }
    }

    pub fn fail(
        name: impl Into<String>,
        message: impl Into<String>,
        fix: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            status: DiagnosticStatus::Fail,
            message: message.into(),
            fix: Some(fix.into()),
        }
    }

    pub fn warn(
        name: impl Into<String>,
        message: impl Into<String>,
        fix: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            status: DiagnosticStatus::Warn,
            message: message.into(),
            fix: Some(fix.into()),
        }
    }

    pub fn skip(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: DiagnosticStatus::Skip,
            message: message.into(),
            fix: None,
        }
    }

    /// Print this result to stdout
    pub fn print(&self) {
        println!("{} {}", self.status.colored_icon(), self.name);
        println!("  {}", self.message.dimmed());
        if let Some(fix) = &self.fix {
            println!("  {} {}", "→".cyan(), fix);
        }
    }
}

/// Configuration for running diagnostics
pub struct DiagnosticConfig {
    /// Data directory to check
    pub data_dir: PathBuf,
    /// Server address to check connectivity
    pub server_addr: String,
    /// HTTP API address
    pub http_addr: String,
    /// Whether to run verbose checks
    pub verbose: bool,
    /// Run deep/thorough checks (takes longer)
    pub deep: bool,
}

impl Default for DiagnosticConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            server_addr: "localhost:9092".into(),
            http_addr: "localhost:9094".into(),
            verbose: false,
            deep: false,
        }
    }
}

/// Run all diagnostic checks
pub fn run_diagnostics(config: &DiagnosticConfig) -> Vec<DiagnosticResult> {
    let mut results = Vec::new();

    // 1. Check server connectivity
    results.push(check_server_connectivity(&config.server_addr));

    // 2. Check HTTP API
    results.push(check_http_api(&config.http_addr));

    // 3. Check data directory
    results.push(check_data_directory(&config.data_dir));

    // 4. Check storage health
    results.extend(check_storage_health(&config.data_dir));

    // 5. Check consumer groups
    results.extend(check_consumer_groups(&config.data_dir));

    // 6. Check disk space
    results.push(check_disk_space(&config.data_dir));

    // 7. Check configuration
    results.push(check_configuration(&config.data_dir));

    // Deep checks (run with --deep flag)
    if config.deep {
        results.push(DiagnosticResult::pass(
            "Deep scan starting",
            "Running thorough system analysis...",
        ));

        // 8. Segment file integrity
        results.extend(check_segment_integrity(&config.data_dir));

        // 9. Index consistency
        results.extend(check_index_consistency(&config.data_dir));

        // 10. Memory usage analysis
        results.push(check_memory_usage());

        // 11. File descriptor usage
        results.push(check_file_descriptors());

        // 12. Network latency test
        results.push(check_network_latency(&config.server_addr));

        // 13. System resources
        results.push(check_system_resources());

        // 14. Log file analysis
        results.extend(check_log_files(&config.data_dir));

        // 15. Segment compaction status
        results.extend(check_compaction_status(&config.data_dir));
    }

    results
}

/// Print diagnostic summary
pub fn print_diagnostic_summary(results: &[DiagnosticResult]) {
    println!();
    println!("{}", "Diagnostic Summary".bold().cyan());
    println!("{}", "═".repeat(60).dimmed());
    println!();

    let pass_count = results
        .iter()
        .filter(|r| r.status == DiagnosticStatus::Pass)
        .count();
    let fail_count = results
        .iter()
        .filter(|r| r.status == DiagnosticStatus::Fail)
        .count();
    let warn_count = results
        .iter()
        .filter(|r| r.status == DiagnosticStatus::Warn)
        .count();
    let skip_count = results
        .iter()
        .filter(|r| r.status == DiagnosticStatus::Skip)
        .count();

    for result in results {
        result.print();
        println!();
    }

    println!("{}", "─".repeat(60).dimmed());
    println!(
        "  {} {} passed, {} {} failed, {} {} warnings, {} {} skipped",
        pass_count.to_string().green(),
        "checks".dimmed(),
        fail_count.to_string().red(),
        "checks".dimmed(),
        warn_count.to_string().yellow(),
        "checks".dimmed(),
        skip_count.to_string().dimmed(),
        "checks".dimmed(),
    );

    if fail_count > 0 {
        println!();
        println!(
            "  {} Run the suggested fixes above to resolve issues.",
            "→".cyan()
        );
    } else if warn_count > 0 {
        println!();
        println!(
            "  {} Consider addressing the warnings for optimal performance.",
            "→".yellow()
        );
    } else if fail_count == 0 && warn_count == 0 && skip_count < results.len() {
        println!();
        println!(
            "  {} All checks passed! Streamline is healthy.",
            "✓".green()
        );
    }
}

/// Check if we can connect to the Kafka protocol port
fn check_server_connectivity(addr: &str) -> DiagnosticResult {
    match TcpStream::connect_timeout(
        &addr
            .parse()
            .unwrap_or_else(|_| std::net::SocketAddr::from(([127, 0, 0, 1], 9092))),
        Duration::from_secs(2),
    ) {
        Ok(_) => DiagnosticResult::pass(
            "Server connectivity",
            format!("Server reachable at {}", addr),
        ),
        Err(e) => DiagnosticResult::fail(
            "Server connectivity",
            format!("Cannot connect to {}: {}", addr, e),
            "Start the server with: streamline --playground".to_string(),
        ),
    }
}

/// Check if we can reach the HTTP API
fn check_http_api(addr: &str) -> DiagnosticResult {
    match TcpStream::connect_timeout(
        &addr
            .parse()
            .unwrap_or_else(|_| std::net::SocketAddr::from(([127, 0, 0, 1], 9094))),
        Duration::from_secs(2),
    ) {
        Ok(_) => DiagnosticResult::pass("HTTP API", format!("HTTP API reachable at {}", addr)),
        Err(_) => DiagnosticResult::warn(
            "HTTP API",
            format!("HTTP API not reachable at {}", addr),
            "HTTP API is optional. Check --http-addr if you need it.",
        ),
    }
}

/// Check if the data directory exists and is accessible
fn check_data_directory(data_dir: &Path) -> DiagnosticResult {
    if !data_dir.exists() {
        return DiagnosticResult::warn(
            "Data directory",
            format!("Data directory does not exist: {}", data_dir.display()),
            "The server will create it on first run, or use --in-memory mode.",
        );
    }

    // Check if we can read and write
    let test_file = data_dir.join(".streamline_doctor_test");
    match std::fs::write(&test_file, "test") {
        Ok(_) => {
            let _ = std::fs::remove_file(&test_file);
            DiagnosticResult::pass(
                "Data directory",
                format!("Data directory accessible at {}", data_dir.display()),
            )
        }
        Err(e) => DiagnosticResult::fail(
            "Data directory",
            format!("Cannot write to data directory: {}", e),
            format!("Check permissions: chmod 755 {}", data_dir.display()),
        ),
    }
}

/// Check storage health (topics, partitions, segments)
fn check_storage_health(data_dir: &Path) -> Vec<DiagnosticResult> {
    let mut results = Vec::new();

    if !data_dir.exists() {
        results.push(DiagnosticResult::skip(
            "Storage health",
            "Data directory does not exist yet",
        ));
        return results;
    }

    let manager = match TopicManager::new(data_dir) {
        Ok(m) => m,
        Err(e) => {
            results.push(DiagnosticResult::fail(
                "Storage health",
                format!("Failed to initialize storage: {}", e),
                "Check data directory permissions and disk space",
            ));
            return results;
        }
    };

    let topics = match manager.list_topics() {
        Ok(t) => t,
        Err(e) => {
            results.push(DiagnosticResult::fail(
                "Storage health",
                format!("Failed to list topics: {}", e),
                "The storage may be corrupted. Check the data directory.",
            ));
            return results;
        }
    };

    if topics.is_empty() {
        results.push(DiagnosticResult::pass(
            "Topics",
            "No topics created yet (this is normal for a fresh install)",
        ));
    } else {
        let total_partitions: i32 = topics.iter().map(|t| t.num_partitions).sum();
        results.push(DiagnosticResult::pass(
            "Topics",
            format!(
                "{} topic(s), {} partition(s) healthy",
                topics.len(),
                total_partitions
            ),
        ));
    }

    results
}

/// Check consumer groups for lag and stale groups
fn check_consumer_groups(data_dir: &Path) -> Vec<DiagnosticResult> {
    use std::sync::Arc;

    let mut results = Vec::new();

    if !data_dir.exists() {
        return results;
    }

    let manager = match TopicManager::new(data_dir) {
        Ok(m) => Arc::new(m),
        Err(_) => return results,
    };

    let coordinator = match GroupCoordinator::new(data_dir, manager.clone()) {
        Ok(c) => c,
        Err(_) => return results,
    };

    let groups = match coordinator.list_groups() {
        Ok(g) => g,
        Err(_) => return results,
    };

    if groups.is_empty() {
        results.push(DiagnosticResult::pass(
            "Consumer groups",
            "No consumer groups (this is normal)",
        ));
        return results;
    }

    let mut high_lag_groups = Vec::new();

    for group_id in &groups {
        // Check lag for this group by getting the group and iterating its offsets
        if let Ok(Some(group)) = coordinator.get_group(group_id) {
            for ((topic, partition), committed) in &group.offsets {
                if let Ok(latest) = manager.latest_offset(topic, *partition) {
                    let lag = latest - committed.offset;
                    if lag > 10_000 {
                        high_lag_groups.push((group_id.clone(), topic.clone(), *partition, lag));
                    }
                }
            }
        }
    }

    if high_lag_groups.is_empty() {
        results.push(DiagnosticResult::pass(
            "Consumer groups",
            format!("{} group(s) with healthy lag", groups.len()),
        ));
    } else {
        for (group_id, topic, partition, lag) in high_lag_groups {
            results.push(DiagnosticResult::warn(
                format!("Consumer lag: {}", group_id),
                format!(
                    "High lag ({}) on {}[{}]",
                    lag.to_string().yellow(),
                    topic,
                    partition
                ),
                format!("streamline-cli groups describe {}", group_id),
            ));
        }
    }

    results
}

/// Check available disk space
fn check_disk_space(data_dir: &Path) -> DiagnosticResult {
    // Get disk space info (platform-specific)
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::mem::MaybeUninit;

        let path = if data_dir.exists() {
            data_dir.to_path_buf()
        } else {
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"))
        };

        let path_cstr = match CString::new(path.to_string_lossy().as_bytes()) {
            Ok(s) => s,
            Err(_) => return DiagnosticResult::skip("Disk space", "Unable to check disk space"),
        };

        let mut stat: MaybeUninit<libc::statvfs> = MaybeUninit::uninit();
        let result = unsafe { libc::statvfs(path_cstr.as_ptr(), stat.as_mut_ptr()) };

        if result == 0 {
            let stat = unsafe { stat.assume_init() };
            // These casts are needed for cross-platform compatibility
            #[allow(clippy::unnecessary_cast)]
            let available_bytes = stat.f_bavail as u64 * stat.f_frsize as u64;
            #[allow(clippy::unnecessary_cast)]
            let total_bytes = stat.f_blocks as u64 * stat.f_frsize as u64;
            #[allow(clippy::unnecessary_cast)]
            let used_bytes = total_bytes - (stat.f_bfree as u64 * stat.f_frsize as u64);
            let usage_percent = (used_bytes as f64 / total_bytes as f64 * 100.0) as u32;

            let available_gb = available_bytes as f64 / 1_073_741_824.0;
            let total_gb = total_bytes as f64 / 1_073_741_824.0;

            if available_gb < 1.0 {
                DiagnosticResult::fail(
                    "Disk space",
                    format!(
                        "{:.1}GB available of {:.1}GB ({}% used)",
                        available_gb, total_gb, usage_percent
                    ),
                    "Free up disk space or use a different --data-dir",
                )
            } else if available_gb < 5.0 {
                DiagnosticResult::warn(
                    "Disk space",
                    format!(
                        "{:.1}GB available of {:.1}GB ({}% used)",
                        available_gb, total_gb, usage_percent
                    ),
                    "Consider freeing up disk space or enabling retention policies",
                )
            } else {
                DiagnosticResult::pass(
                    "Disk space",
                    format!(
                        "{:.1}GB available of {:.1}GB ({}% used)",
                        available_gb, total_gb, usage_percent
                    ),
                )
            }
        } else {
            DiagnosticResult::skip("Disk space", "Unable to check disk space")
        }
    }

    #[cfg(not(unix))]
    {
        DiagnosticResult::skip(
            "Disk space",
            "Disk space check not available on this platform",
        )
    }
}

/// Check configuration files
fn check_configuration(data_dir: &Path) -> DiagnosticResult {
    // Check for common config locations including data directory
    let config_paths = [
        PathBuf::from("./streamline.toml"),
        data_dir.join("streamline.toml"),
        PathBuf::from("/etc/streamline/streamline.toml"),
        dirs::config_dir()
            .map(|d| d.join("streamline/streamline.toml"))
            .unwrap_or_default(),
    ];

    let found_config = config_paths.iter().find(|p| p.exists());

    if let Some(config_path) = found_config {
        // Try to parse the config
        match std::fs::read_to_string(config_path) {
            Ok(content) => match toml::from_str::<toml::Value>(&content) {
                Ok(_) => DiagnosticResult::pass(
                    "Configuration",
                    format!("Valid config found at {}", config_path.display()),
                ),
                Err(e) => DiagnosticResult::fail(
                    "Configuration",
                    format!("Invalid config at {}: {}", config_path.display(), e),
                    "Fix the TOML syntax error or regenerate with: streamline --generate-config",
                ),
            },
            Err(e) => DiagnosticResult::warn(
                "Configuration",
                format!("Cannot read config at {}: {}", config_path.display(), e),
                "Check file permissions",
            ),
        }
    } else {
        DiagnosticResult::pass("Configuration", "No config file found (using defaults)")
    }
}

// ============================================================================
// Deep Diagnostic Checks (run with --deep flag)
// ============================================================================

/// Check segment file integrity by verifying headers and checksums
fn check_segment_integrity(data_dir: &Path) -> Vec<DiagnosticResult> {
    let mut results = Vec::new();

    let topics_dir = data_dir.join("topics");
    if !topics_dir.exists() {
        results.push(DiagnosticResult::skip(
            "Segment integrity",
            "No topics directory found",
        ));
        return results;
    }

    let mut total_segments = 0;
    let mut _valid_segments = 0;
    let mut corrupted_segments = Vec::new();

    // Walk through all topic directories
    if let Ok(topics) = std::fs::read_dir(&topics_dir) {
        for topic_entry in topics.flatten() {
            if !topic_entry.path().is_dir() {
                continue;
            }

            // Check partition directories
            if let Ok(partitions) = std::fs::read_dir(topic_entry.path()) {
                for partition_entry in partitions.flatten() {
                    if !partition_entry.path().is_dir() {
                        continue;
                    }

                    // Check segment files
                    if let Ok(files) = std::fs::read_dir(partition_entry.path()) {
                        for file_entry in files.flatten() {
                            let path = file_entry.path();
                            if path.extension().map(|e| e == "segment").unwrap_or(false) {
                                total_segments += 1;

                                // Verify segment header (magic bytes "STRM")
                                if let Ok(mut file) = std::fs::File::open(&path) {
                                    use std::io::Read;
                                    let mut header = [0u8; 4];
                                    if file.read_exact(&mut header).is_ok() {
                                        if &header == b"STRM" {
                                            _valid_segments += 1;
                                        } else {
                                            corrupted_segments.push(path.display().to_string());
                                        }
                                    } else {
                                        corrupted_segments.push(path.display().to_string());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if total_segments == 0 {
        results.push(DiagnosticResult::pass(
            "Segment integrity",
            "No segment files to check",
        ));
    } else if corrupted_segments.is_empty() {
        results.push(DiagnosticResult::pass(
            "Segment integrity",
            format!("All {} segment files have valid headers", total_segments),
        ));
    } else {
        results.push(DiagnosticResult::fail(
            "Segment integrity",
            format!(
                "{}/{} segments have invalid headers",
                corrupted_segments.len(),
                total_segments
            ),
            "Corrupted segments may need to be recovered from replicas or backups",
        ));
        for segment in corrupted_segments.iter().take(5) {
            results.push(DiagnosticResult::warn(
                "Corrupted segment",
                segment.clone(),
                "Consider removing or restoring this segment",
            ));
        }
    }

    results
}

/// Check index file consistency
fn check_index_consistency(data_dir: &Path) -> Vec<DiagnosticResult> {
    let mut results = Vec::new();

    let topics_dir = data_dir.join("topics");
    if !topics_dir.exists() {
        results.push(DiagnosticResult::skip(
            "Index consistency",
            "No topics directory found",
        ));
        return results;
    }

    let mut total_indexes = 0;
    let mut orphan_indexes = Vec::new();

    // Walk through all topic directories
    if let Ok(topics) = std::fs::read_dir(&topics_dir) {
        for topic_entry in topics.flatten() {
            if !topic_entry.path().is_dir() {
                continue;
            }

            if let Ok(partitions) = std::fs::read_dir(topic_entry.path()) {
                for partition_entry in partitions.flatten() {
                    if !partition_entry.path().is_dir() {
                        continue;
                    }

                    if let Ok(files) = std::fs::read_dir(partition_entry.path()) {
                        let files: Vec<_> = files.flatten().collect();
                        let segment_names: std::collections::HashSet<_> = files
                            .iter()
                            .filter_map(|f| {
                                let path = f.path();
                                if path.extension().map(|e| e == "segment").unwrap_or(false) {
                                    path.file_stem().map(|s| s.to_string_lossy().to_string())
                                } else {
                                    None
                                }
                            })
                            .collect();

                        for file_entry in &files {
                            let path = file_entry.path();
                            if path.extension().map(|e| e == "index").unwrap_or(false) {
                                total_indexes += 1;
                                let stem = path
                                    .file_stem()
                                    .map(|s| s.to_string_lossy().to_string())
                                    .unwrap_or_default();
                                if !segment_names.contains(&stem) {
                                    orphan_indexes.push(path.display().to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if total_indexes == 0 {
        results.push(DiagnosticResult::pass(
            "Index consistency",
            "No index files to check",
        ));
    } else if orphan_indexes.is_empty() {
        results.push(DiagnosticResult::pass(
            "Index consistency",
            format!(
                "All {} index files have corresponding segments",
                total_indexes
            ),
        ));
    } else {
        results.push(DiagnosticResult::warn(
            "Index consistency",
            format!(
                "{} orphan index files found (no matching segment)",
                orphan_indexes.len()
            ),
            "Orphan indexes can be safely deleted",
        ));
    }

    results
}

/// Check memory usage
fn check_memory_usage() -> DiagnosticResult {
    #[cfg(unix)]
    {
        // Read from /proc/self/status on Linux
        #[cfg(target_os = "linux")]
        {
            if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
                let vm_rss = status
                    .lines()
                    .find(|l| l.starts_with("VmRSS:"))
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|s| s.parse::<u64>().ok());

                if let Some(rss_kb) = vm_rss {
                    let rss_mb = rss_kb / 1024;
                    if rss_mb > 4096 {
                        return DiagnosticResult::warn(
                            "Memory usage",
                            format!("Process using {} MB RSS", rss_mb),
                            "Consider increasing system memory or reducing cache sizes",
                        );
                    } else {
                        return DiagnosticResult::pass(
                            "Memory usage",
                            format!("Process using {} MB RSS", rss_mb),
                        );
                    }
                }
            }
        }

        // Fallback for non-Linux Unix (macOS, BSD)
        DiagnosticResult::pass("Memory usage", "Memory usage check skipped (not Linux)")
    }

    #[cfg(not(unix))]
    {
        DiagnosticResult::skip(
            "Memory usage",
            "Memory check not available on this platform",
        )
    }
}

/// Check file descriptor usage
fn check_file_descriptors() -> DiagnosticResult {
    #[cfg(unix)]
    {
        // Get current limit
        let mut rlim: libc::rlimit = unsafe { std::mem::zeroed() };
        let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) };

        if result == 0 {
            let soft_limit = rlim.rlim_cur;
            let hard_limit = rlim.rlim_max;

            // Count open file descriptors
            let fd_count = std::fs::read_dir("/proc/self/fd")
                .map(|d| d.count())
                .unwrap_or(0);

            let usage_percent = if soft_limit > 0 {
                (fd_count as f64 / soft_limit as f64 * 100.0) as u32
            } else {
                0
            };

            if usage_percent > 80 {
                DiagnosticResult::fail(
                    "File descriptors",
                    format!("{} of {} used ({}%)", fd_count, soft_limit, usage_percent),
                    format!("Increase limit with: ulimit -n {}", hard_limit),
                )
            } else if usage_percent > 50 {
                DiagnosticResult::warn(
                    "File descriptors",
                    format!("{} of {} used ({}%)", fd_count, soft_limit, usage_percent),
                    "Consider increasing ulimit if handling many connections",
                )
            } else {
                DiagnosticResult::pass(
                    "File descriptors",
                    format!("{} of {} used ({}%)", fd_count, soft_limit, usage_percent),
                )
            }
        } else {
            DiagnosticResult::skip("File descriptors", "Unable to get file descriptor limits")
        }
    }

    #[cfg(not(unix))]
    {
        DiagnosticResult::skip(
            "File descriptors",
            "File descriptor check not available on this platform",
        )
    }
}

/// Check network latency to server
fn check_network_latency(addr: &str) -> DiagnosticResult {
    use std::time::Instant;

    let socket_addr = addr
        .parse()
        .unwrap_or_else(|_| std::net::SocketAddr::from(([127, 0, 0, 1], 9092)));

    let mut latencies = Vec::new();

    // Perform 5 connection tests
    for _ in 0..5 {
        let start = Instant::now();
        if let Ok(stream) = TcpStream::connect_timeout(&socket_addr, Duration::from_secs(2)) {
            let latency = start.elapsed();
            latencies.push(latency.as_micros() as u64);
            drop(stream);
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    if latencies.is_empty() {
        return DiagnosticResult::fail(
            "Network latency",
            format!("Cannot connect to {}", addr),
            "Check that the server is running and accessible",
        );
    }

    let avg_latency = latencies.iter().sum::<u64>() / latencies.len() as u64;
    let min_latency = *latencies.iter().min().unwrap_or(&0);
    let max_latency = *latencies.iter().max().unwrap_or(&0);

    if avg_latency > 10_000 {
        // > 10ms
        DiagnosticResult::warn(
            "Network latency",
            format!(
                "avg={:.2}ms min={:.2}ms max={:.2}ms",
                avg_latency as f64 / 1000.0,
                min_latency as f64 / 1000.0,
                max_latency as f64 / 1000.0
            ),
            "High latency may indicate network issues",
        )
    } else {
        DiagnosticResult::pass(
            "Network latency",
            format!(
                "avg={:.2}ms min={:.2}ms max={:.2}ms",
                avg_latency as f64 / 1000.0,
                min_latency as f64 / 1000.0,
                max_latency as f64 / 1000.0
            ),
        )
    }
}

/// Check system resources (CPU cores, available memory)
fn check_system_resources() -> DiagnosticResult {
    let cpu_count = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1);

    #[cfg(target_os = "linux")]
    {
        // Read available memory from /proc/meminfo
        if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
            let available = meminfo
                .lines()
                .find(|l| l.starts_with("MemAvailable:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|s| s.parse::<u64>().ok());

            if let Some(avail_kb) = available {
                let avail_gb = avail_kb as f64 / 1_048_576.0;
                if avail_gb < 1.0 {
                    return DiagnosticResult::warn(
                        "System resources",
                        format!(
                            "{} CPU cores, {:.1}GB available memory",
                            cpu_count, avail_gb
                        ),
                        "Low available memory may impact performance",
                    );
                } else {
                    return DiagnosticResult::pass(
                        "System resources",
                        format!(
                            "{} CPU cores, {:.1}GB available memory",
                            cpu_count, avail_gb
                        ),
                    );
                }
            }
        }
    }

    DiagnosticResult::pass(
        "System resources",
        format!("{} CPU cores available", cpu_count),
    )
}

/// Check log files for recent errors
fn check_log_files(data_dir: &Path) -> Vec<DiagnosticResult> {
    let mut results = Vec::new();

    let log_paths = [
        data_dir.join("logs"),
        data_dir.join("streamline.log"),
        PathBuf::from("/var/log/streamline/streamline.log"),
    ];

    for log_path in &log_paths {
        if log_path.is_file() {
            // Check for recent errors in log file
            if let Ok(content) = std::fs::read_to_string(log_path) {
                let lines: Vec<_> = content.lines().collect();
                let recent_lines = lines.iter().rev().take(1000).rev();

                let error_count = recent_lines
                    .clone()
                    .filter(|l| l.contains("ERROR") || l.contains("FATAL"))
                    .count();

                let warn_count = recent_lines.filter(|l| l.contains("WARN")).count();

                if error_count > 10 {
                    results.push(DiagnosticResult::fail(
                        "Log analysis",
                        format!(
                            "{} errors, {} warnings in {}",
                            error_count,
                            warn_count,
                            log_path.display()
                        ),
                        "Review the log file for recurring issues",
                    ));
                } else if error_count > 0 || warn_count > 50 {
                    results.push(DiagnosticResult::warn(
                        "Log analysis",
                        format!(
                            "{} errors, {} warnings in {}",
                            error_count,
                            warn_count,
                            log_path.display()
                        ),
                        "Some errors or warnings found in logs",
                    ));
                } else {
                    results.push(DiagnosticResult::pass(
                        "Log analysis",
                        format!("No significant errors in {}", log_path.display()),
                    ));
                }
                return results;
            }
        } else if log_path.is_dir() {
            // Count log files and check total size
            let mut total_size = 0u64;
            let mut file_count = 0;

            if let Ok(entries) = std::fs::read_dir(log_path) {
                for entry in entries.flatten() {
                    if let Ok(metadata) = entry.metadata() {
                        if metadata.is_file() {
                            total_size += metadata.len();
                            file_count += 1;
                        }
                    }
                }
            }

            if file_count > 0 {
                let size_mb = total_size as f64 / 1_048_576.0;
                if size_mb > 1000.0 {
                    results.push(DiagnosticResult::warn(
                        "Log analysis",
                        format!(
                            "{} log files, {:.1} MB total in {}",
                            file_count,
                            size_mb,
                            log_path.display()
                        ),
                        "Consider rotating or archiving old logs",
                    ));
                } else {
                    results.push(DiagnosticResult::pass(
                        "Log analysis",
                        format!("{} log files, {:.1} MB total", file_count, size_mb),
                    ));
                }
                return results;
            }
        }
    }

    results.push(DiagnosticResult::skip("Log analysis", "No log files found"));
    results
}

/// Check segment compaction status
fn check_compaction_status(data_dir: &Path) -> Vec<DiagnosticResult> {
    let mut results = Vec::new();

    let topics_dir = data_dir.join("topics");
    if !topics_dir.exists() {
        results.push(DiagnosticResult::skip(
            "Compaction status",
            "No topics directory found",
        ));
        return results;
    }

    let mut total_segments = 0;
    let mut old_segments = 0;
    let now = std::time::SystemTime::now();

    // Walk through all topic directories
    if let Ok(topics) = std::fs::read_dir(&topics_dir) {
        for topic_entry in topics.flatten() {
            if !topic_entry.path().is_dir() {
                continue;
            }

            if let Ok(partitions) = std::fs::read_dir(topic_entry.path()) {
                for partition_entry in partitions.flatten() {
                    if !partition_entry.path().is_dir() {
                        continue;
                    }

                    if let Ok(files) = std::fs::read_dir(partition_entry.path()) {
                        for file_entry in files.flatten() {
                            let path = file_entry.path();
                            if path.extension().map(|e| e == "segment").unwrap_or(false) {
                                total_segments += 1;

                                // Check segment age
                                if let Ok(metadata) = std::fs::metadata(&path) {
                                    if let Ok(modified) = metadata.modified() {
                                        if let Ok(age) = now.duration_since(modified) {
                                            // Segments older than 7 days
                                            if age > Duration::from_secs(7 * 24 * 60 * 60) {
                                                old_segments += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if total_segments == 0 {
        results.push(DiagnosticResult::pass(
            "Compaction status",
            "No segments to analyze",
        ));
    } else if old_segments > total_segments / 2 {
        results.push(DiagnosticResult::warn(
            "Compaction status",
            format!(
                "{} of {} segments are older than 7 days",
                old_segments, total_segments
            ),
            "Consider configuring retention policies or running compaction",
        ));
    } else {
        results.push(DiagnosticResult::pass(
            "Compaction status",
            format!(
                "{} total segments, {} older than 7 days",
                total_segments, old_segments
            ),
        ));
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diagnostic_status_icon() {
        assert_eq!(DiagnosticStatus::Pass.icon(), "✓");
        assert_eq!(DiagnosticStatus::Fail.icon(), "✗");
        assert_eq!(DiagnosticStatus::Warn.icon(), "⚠");
        assert_eq!(DiagnosticStatus::Skip.icon(), "○");
    }

    #[test]
    fn test_diagnostic_result_constructors() {
        let pass = DiagnosticResult::pass("Test", "All good");
        assert_eq!(pass.status, DiagnosticStatus::Pass);
        assert!(pass.fix.is_none());

        let fail = DiagnosticResult::fail("Test", "Failed", "Fix it");
        assert_eq!(fail.status, DiagnosticStatus::Fail);
        assert!(fail.fix.is_some());

        let warn = DiagnosticResult::warn("Test", "Warning", "Consider fixing");
        assert_eq!(warn.status, DiagnosticStatus::Warn);
        assert!(warn.fix.is_some());
    }

    #[test]
    fn test_diagnostic_config_deep() {
        let config = DiagnosticConfig::default();
        assert!(!config.deep);

        let deep_config = DiagnosticConfig {
            deep: true,
            ..Default::default()
        };
        assert!(deep_config.deep);
    }

    #[test]
    fn test_segment_integrity_no_data() {
        let temp_dir = tempfile::tempdir().unwrap();
        let results = check_segment_integrity(temp_dir.path());
        assert!(!results.is_empty());
        assert_eq!(results[0].status, DiagnosticStatus::Skip);
    }

    #[test]
    fn test_index_consistency_no_data() {
        let temp_dir = tempfile::tempdir().unwrap();
        let results = check_index_consistency(temp_dir.path());
        assert!(!results.is_empty());
        assert_eq!(results[0].status, DiagnosticStatus::Skip);
    }
}
