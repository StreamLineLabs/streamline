//! System tuning utilities for optimal streaming performance
//!
//! This module provides `rpk redpanda tune`-style system configuration
//! for optimal streaming performance. It detects current system settings
//! and suggests or applies kernel parameter optimizations.
//!
//! ## Usage
//!
//! ```bash
//! # Check current settings (dry run)
//! streamline tune --dry-run
//!
//! # Apply low-latency profile
//! streamline tune --profile low-latency --apply
//!
//! # Apply high-throughput profile
//! streamline tune --profile high-throughput --apply
//! ```
//!
//! ## Profiles
//!
//! - **low-latency**: Optimized for minimal latency (default)
//! - **high-throughput**: Optimized for maximum throughput
//!
//! ## Tuned Parameters
//!
//! - `vm.swappiness` - Reduce swapping
//! - `vm.dirty_ratio` - Control write buffering
//! - `vm.dirty_background_ratio` - Background flush threshold
//! - `net.core.somaxconn` - TCP listen backlog
//! - `net.ipv4.tcp_max_syn_backlog` - SYN backlog
//! - `fs.file-max` - Max file descriptors
//! - CPU governor - Performance mode
//! - Transparent Huge Pages - madvise mode

use std::collections::HashMap;
use std::fs;
use std::io;
use tracing::{debug, info, warn};

/// Tuning profile
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TuneProfile {
    /// Optimized for low latency
    #[default]
    LowLatency,
    /// Optimized for high throughput
    HighThroughput,
}

impl std::fmt::Display for TuneProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LowLatency => write!(f, "low-latency"),
            Self::HighThroughput => write!(f, "high-throughput"),
        }
    }
}

impl std::str::FromStr for TuneProfile {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "low-latency" | "lowlatency" | "latency" => Ok(Self::LowLatency),
            "high-throughput" | "highthroughput" | "throughput" => Ok(Self::HighThroughput),
            _ => Err(format!(
                "Unknown profile: {}. Use 'low-latency' or 'high-throughput'",
                s
            )),
        }
    }
}

/// Configuration for system tuning
#[derive(Debug, Clone)]
pub struct TuneConfig {
    /// Tuning profile to apply
    pub profile: TuneProfile,
    /// Just check settings without applying
    pub dry_run: bool,
    /// Force apply even if already optimal
    pub force: bool,
}

impl Default for TuneConfig {
    fn default() -> Self {
        Self {
            profile: TuneProfile::default(),
            dry_run: true,
            force: false,
        }
    }
}

/// A single tuning parameter
#[derive(Debug, Clone)]
pub struct TuneParameter {
    /// Parameter name (e.g., "vm.swappiness")
    pub name: String,
    /// Sysctl path or file path
    pub path: String,
    /// Current value (if readable)
    pub current: Option<String>,
    /// Recommended value
    pub recommended: String,
    /// Description of what this parameter does
    pub description: String,
    /// Whether the current value is optimal
    pub is_optimal: bool,
}

/// Result of the tuning operation
#[derive(Debug)]
pub struct TuneResult {
    /// Parameters that were checked/modified
    pub parameters: Vec<TuneParameter>,
    /// Number of parameters that were already optimal
    pub already_optimal: usize,
    /// Number of parameters that were changed
    pub changed: usize,
    /// Number of parameters that failed to change
    pub failed: usize,
    /// Whether this was a dry run
    pub dry_run: bool,
    /// Profile that was applied
    pub profile: TuneProfile,
    /// Any warnings or notes
    pub warnings: Vec<String>,
}

impl TuneResult {
    /// Check if all parameters are optimal
    pub fn all_optimal(&self) -> bool {
        self.parameters.iter().all(|p| p.is_optimal)
    }
}

/// Get the tuning parameters for a profile
fn get_profile_parameters(
    profile: TuneProfile,
) -> Vec<(&'static str, &'static str, &'static str, &'static str)> {
    // (name, path, recommended_value, description)
    match profile {
        TuneProfile::LowLatency => vec![
            // Memory settings
            (
                "vm.swappiness",
                "/proc/sys/vm/swappiness",
                "1",
                "Minimize swapping to reduce latency spikes",
            ),
            (
                "vm.dirty_ratio",
                "/proc/sys/vm/dirty_ratio",
                "10",
                "Percentage of memory that can be dirty before forced flush",
            ),
            (
                "vm.dirty_background_ratio",
                "/proc/sys/vm/dirty_background_ratio",
                "5",
                "Percentage of memory that triggers background flush",
            ),
            (
                "vm.dirty_expire_centisecs",
                "/proc/sys/vm/dirty_expire_centisecs",
                "500",
                "Age in centiseconds before dirty data is eligible for writeout",
            ),
            (
                "vm.dirty_writeback_centisecs",
                "/proc/sys/vm/dirty_writeback_centisecs",
                "100",
                "Interval for pdflush/writeback threads",
            ),
            // Network settings
            (
                "net.core.somaxconn",
                "/proc/sys/net/core/somaxconn",
                "65535",
                "Maximum TCP listen backlog",
            ),
            (
                "net.ipv4.tcp_max_syn_backlog",
                "/proc/sys/net/ipv4/tcp_max_syn_backlog",
                "65535",
                "Maximum SYN queue length",
            ),
            (
                "net.core.netdev_max_backlog",
                "/proc/sys/net/core/netdev_max_backlog",
                "65535",
                "Maximum packets queued on input",
            ),
            (
                "net.ipv4.tcp_fastopen",
                "/proc/sys/net/ipv4/tcp_fastopen",
                "3",
                "Enable TCP Fast Open for both client and server",
            ),
            (
                "net.ipv4.tcp_tw_reuse",
                "/proc/sys/net/ipv4/tcp_tw_reuse",
                "1",
                "Reuse TIME_WAIT sockets",
            ),
            // File descriptor limits
            (
                "fs.file-max",
                "/proc/sys/fs/file-max",
                "1000000",
                "Maximum number of file handles",
            ),
            (
                "fs.nr_open",
                "/proc/sys/fs/nr_open",
                "1048576",
                "Maximum file descriptors per process",
            ),
            // Scheduler settings for latency
            (
                "kernel.sched_migration_cost_ns",
                "/proc/sys/kernel/sched_migration_cost_ns",
                "5000000",
                "Task migration cost (higher = less migration = better cache)",
            ),
            (
                "kernel.sched_autogroup_enabled",
                "/proc/sys/kernel/sched_autogroup_enabled",
                "0",
                "Disable autogroup for more predictable scheduling",
            ),
        ],
        TuneProfile::HighThroughput => vec![
            // Memory settings - more aggressive buffering
            (
                "vm.swappiness",
                "/proc/sys/vm/swappiness",
                "1",
                "Minimize swapping",
            ),
            (
                "vm.dirty_ratio",
                "/proc/sys/vm/dirty_ratio",
                "40",
                "Allow more dirty pages for batching",
            ),
            (
                "vm.dirty_background_ratio",
                "/proc/sys/vm/dirty_background_ratio",
                "10",
                "Start background flush later",
            ),
            (
                "vm.dirty_expire_centisecs",
                "/proc/sys/vm/dirty_expire_centisecs",
                "3000",
                "Keep dirty data longer for better batching",
            ),
            (
                "vm.dirty_writeback_centisecs",
                "/proc/sys/vm/dirty_writeback_centisecs",
                "500",
                "Less frequent writeback",
            ),
            // Network settings - same as low-latency
            (
                "net.core.somaxconn",
                "/proc/sys/net/core/somaxconn",
                "65535",
                "Maximum TCP listen backlog",
            ),
            (
                "net.ipv4.tcp_max_syn_backlog",
                "/proc/sys/net/ipv4/tcp_max_syn_backlog",
                "65535",
                "Maximum SYN queue length",
            ),
            (
                "net.core.netdev_max_backlog",
                "/proc/sys/net/core/netdev_max_backlog",
                "65535",
                "Maximum packets queued on input",
            ),
            (
                "net.core.rmem_max",
                "/proc/sys/net/core/rmem_max",
                "16777216",
                "Maximum receive buffer size",
            ),
            (
                "net.core.wmem_max",
                "/proc/sys/net/core/wmem_max",
                "16777216",
                "Maximum send buffer size",
            ),
            (
                "net.ipv4.tcp_rmem",
                "/proc/sys/net/ipv4/tcp_rmem",
                "4096 87380 16777216",
                "TCP receive buffer sizes (min, default, max)",
            ),
            (
                "net.ipv4.tcp_wmem",
                "/proc/sys/net/ipv4/tcp_wmem",
                "4096 65536 16777216",
                "TCP send buffer sizes (min, default, max)",
            ),
            // File descriptor limits
            (
                "fs.file-max",
                "/proc/sys/fs/file-max",
                "1000000",
                "Maximum number of file handles",
            ),
            // Scheduler settings for throughput
            (
                "kernel.sched_migration_cost_ns",
                "/proc/sys/kernel/sched_migration_cost_ns",
                "500000",
                "Lower migration cost for throughput",
            ),
        ],
    }
}

/// Read current value of a sysctl parameter
fn read_sysctl(path: &str) -> io::Result<String> {
    fs::read_to_string(path).map(|s| s.trim().to_string())
}

/// Write a value to a sysctl parameter
#[cfg(target_os = "linux")]
fn write_sysctl(path: &str, value: &str) -> io::Result<()> {
    fs::write(path, value)
}

#[cfg(not(target_os = "linux"))]
fn write_sysctl(_path: &str, _value: &str) -> io::Result<()> {
    // No-op on non-Linux
    Ok(())
}

/// Check if running as root
fn is_root() -> bool {
    #[cfg(unix)]
    {
        unsafe { libc::geteuid() == 0 }
    }
    #[cfg(not(unix))]
    {
        false
    }
}

/// Get the CPU governor for a CPU
#[cfg(target_os = "linux")]
fn get_cpu_governor(cpu: usize) -> io::Result<String> {
    let path = format!(
        "/sys/devices/system/cpu/cpu{}/cpufreq/scaling_governor",
        cpu
    );
    fs::read_to_string(&path).map(|s| s.trim().to_string())
}

#[cfg(not(target_os = "linux"))]
fn get_cpu_governor(_cpu: usize) -> io::Result<String> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Not supported on this platform",
    ))
}

/// Set the CPU governor for a CPU
#[cfg(target_os = "linux")]
fn set_cpu_governor(cpu: usize, governor: &str) -> io::Result<()> {
    let path = format!(
        "/sys/devices/system/cpu/cpu{}/cpufreq/scaling_governor",
        cpu
    );
    fs::write(&path, governor)
}

#[cfg(not(target_os = "linux"))]
fn set_cpu_governor(_cpu: usize, _governor: &str) -> io::Result<()> {
    Ok(())
}

/// Get transparent huge pages setting
#[cfg(target_os = "linux")]
fn get_thp_setting() -> io::Result<String> {
    let content = fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled")?;
    // Parse format like "[always] madvise never" - bracketed is current
    for part in content.split_whitespace() {
        if part.starts_with('[') && part.ends_with(']') {
            return Ok(part[1..part.len() - 1].to_string());
        }
    }
    Ok(content.trim().to_string())
}

#[cfg(not(target_os = "linux"))]
fn get_thp_setting() -> io::Result<String> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "Not supported"))
}

/// Set transparent huge pages setting
#[cfg(target_os = "linux")]
fn set_thp_setting(value: &str) -> io::Result<()> {
    fs::write("/sys/kernel/mm/transparent_hugepage/enabled", value)
}

#[cfg(not(target_os = "linux"))]
fn set_thp_setting(_value: &str) -> io::Result<()> {
    Ok(())
}

/// Get the number of CPUs
fn get_cpu_count() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
}

/// Run system tuning
pub fn run_tune(config: TuneConfig) -> TuneResult {
    info!(
        profile = %config.profile,
        dry_run = config.dry_run,
        "Starting system tuning"
    );

    let mut parameters = Vec::new();
    let mut warnings = Vec::new();
    let mut changed = 0;
    let mut failed = 0;

    // Check platform
    #[cfg(not(target_os = "linux"))]
    {
        warnings.push(
            "System tuning is only fully supported on Linux. Some parameters may not apply."
                .to_string(),
        );
    }

    // Check root permissions
    if !config.dry_run && !is_root() {
        warnings.push("Root permissions required to apply changes. Run with sudo.".to_string());
    }

    // Check sysctl parameters
    let profile_params = get_profile_parameters(config.profile);
    for (name, path, recommended, description) in profile_params {
        let current = read_sysctl(path).ok();
        let is_optimal = current.as_deref() == Some(recommended);

        let mut param = TuneParameter {
            name: name.to_string(),
            path: path.to_string(),
            current: current.clone(),
            recommended: recommended.to_string(),
            description: description.to_string(),
            is_optimal,
        };

        // Apply if not dry run and not optimal
        if !config.dry_run && (!is_optimal || config.force) && is_root() {
            match write_sysctl(path, recommended) {
                Ok(_) => {
                    debug!(name = name, value = recommended, "Applied sysctl");
                    param.is_optimal = true;
                    changed += 1;
                }
                Err(e) => {
                    warn!(name = name, error = %e, "Failed to apply sysctl");
                    failed += 1;
                }
            }
        }

        parameters.push(param);
    }

    // Check CPU governor
    let cpu_count = get_cpu_count();
    let mut governors: HashMap<String, Vec<usize>> = HashMap::new();

    for cpu in 0..cpu_count {
        if let Ok(gov) = get_cpu_governor(cpu) {
            governors.entry(gov).or_default().push(cpu);
        }
    }

    if !governors.is_empty() {
        let recommended_governor = "performance";
        let current_governors: Vec<_> = governors.keys().collect();
        let is_optimal = governors.len() == 1 && governors.contains_key(recommended_governor);

        let current_str = current_governors
            .iter()
            .map(|g| g.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        let mut param = TuneParameter {
            name: "CPU Governor".to_string(),
            path: "/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor".to_string(),
            current: Some(current_str),
            recommended: recommended_governor.to_string(),
            description: "CPU frequency scaling governor".to_string(),
            is_optimal,
        };

        // Apply if not dry run
        if !config.dry_run && (!is_optimal || config.force) && is_root() {
            let mut all_success = true;
            for cpu in 0..cpu_count {
                if let Err(e) = set_cpu_governor(cpu, recommended_governor) {
                    warn!(cpu = cpu, error = %e, "Failed to set CPU governor");
                    all_success = false;
                }
            }
            if all_success {
                param.is_optimal = true;
                changed += 1;
            } else {
                failed += 1;
            }
        }

        parameters.push(param);
    }

    // Check THP setting
    if let Ok(thp) = get_thp_setting() {
        let recommended_thp = "madvise";
        let is_optimal = thp == recommended_thp;

        let mut param = TuneParameter {
            name: "Transparent Huge Pages".to_string(),
            path: "/sys/kernel/mm/transparent_hugepage/enabled".to_string(),
            current: Some(thp),
            recommended: recommended_thp.to_string(),
            description: "THP mode (madvise = app controlled, best for streaming)".to_string(),
            is_optimal,
        };

        if !config.dry_run && (!is_optimal || config.force) && is_root() {
            match set_thp_setting(recommended_thp) {
                Ok(_) => {
                    param.is_optimal = true;
                    changed += 1;
                }
                Err(e) => {
                    warn!(error = %e, "Failed to set THP");
                    failed += 1;
                }
            }
        }

        parameters.push(param);
    }

    // Check ulimit (informational)
    #[cfg(unix)]
    {
        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) };
        if result == 0 {
            let current = rlim.rlim_cur;
            let recommended: u64 = 1048576;
            let is_optimal = current >= recommended;

            parameters.push(TuneParameter {
                name: "ulimit -n (soft)".to_string(),
                path: "(process limit)".to_string(),
                current: Some(format!("{}", current)),
                recommended: format!("{}", recommended),
                description: "Max open files per process (adjust with ulimit -n)".to_string(),
                is_optimal,
            });
        }
    }

    let already_optimal = parameters.iter().filter(|p| p.is_optimal).count();

    TuneResult {
        parameters,
        already_optimal,
        changed,
        failed,
        dry_run: config.dry_run,
        profile: config.profile,
        warnings,
    }
}

/// Format the tuning result for display
pub fn format_tune_result(result: &TuneResult, use_color: bool) -> String {
    use std::fmt::Write;

    let mut output = String::new();

    // Header
    writeln!(
        output,
        "\n{}",
        if use_color {
            "\x1b[1mSystem Tuning Report\x1b[0m"
        } else {
            "System Tuning Report"
        }
    )
    .ok();
    writeln!(output, "{}", "=".repeat(60)).ok();
    writeln!(output, "Profile: {}", result.profile).ok();
    writeln!(
        output,
        "Mode: {}",
        if result.dry_run {
            "Dry Run (no changes applied)"
        } else {
            "Apply Changes"
        }
    )
    .ok();
    writeln!(output).ok();

    // Warnings
    if !result.warnings.is_empty() {
        writeln!(
            output,
            "{}Warnings:{}",
            if use_color { "\x1b[33m" } else { "" },
            if use_color { "\x1b[0m" } else { "" }
        )
        .ok();
        for warning in &result.warnings {
            writeln!(output, "  - {}", warning).ok();
        }
        writeln!(output).ok();
    }

    // Parameters table
    writeln!(
        output,
        "{:<40} {:<15} {:<15} Status",
        "Parameter", "Current", "Recommended"
    )
    .ok();
    writeln!(output, "{}", "-".repeat(90)).ok();

    for param in &result.parameters {
        let current = param.current.as_deref().unwrap_or("N/A");
        let status = if param.is_optimal {
            if use_color {
                "\x1b[32m[OK]\x1b[0m"
            } else {
                "[OK]"
            }
        } else if use_color {
            "\x1b[31m[CHANGE]\x1b[0m"
        } else {
            "[CHANGE]"
        };

        // Truncate long values
        let current_display = if current.len() > 14 {
            format!("{}...", &current[..11])
        } else {
            current.to_string()
        };

        let rec_display = if param.recommended.len() > 14 {
            format!("{}...", &param.recommended[..11])
        } else {
            param.recommended.clone()
        };

        writeln!(
            output,
            "{:<40} {:<15} {:<15} {}",
            param.name, current_display, rec_display, status
        )
        .ok();
    }

    writeln!(output).ok();

    // Summary
    writeln!(output, "Summary:").ok();
    writeln!(output, "  Already optimal: {}", result.already_optimal).ok();
    if !result.dry_run {
        writeln!(output, "  Changed: {}", result.changed).ok();
        writeln!(output, "  Failed: {}", result.failed).ok();
    } else {
        let needs_change = result.parameters.len() - result.already_optimal;
        writeln!(output, "  Needs change: {}", needs_change).ok();
    }

    // Recommendations
    if result.dry_run && result.already_optimal < result.parameters.len() {
        writeln!(output).ok();
        writeln!(output, "To apply changes, run:").ok();
        writeln!(
            output,
            "  sudo streamline-cli tune --profile {} --apply",
            result.profile
        )
        .ok();
    }

    output
}

/// Generate a shell script for the recommended settings
pub fn generate_tune_script(profile: TuneProfile) -> String {
    let mut script = String::new();
    script.push_str("#!/bin/bash\n");
    script.push_str("# Streamline system tuning script\n");
    script.push_str(&format!("# Profile: {}\n", profile));
    script.push_str("# Generated by: streamline-cli tune --script\n\n");

    script.push_str("set -e\n\n");
    script.push_str("if [ \"$EUID\" -ne 0 ]; then\n");
    script.push_str("  echo \"Please run as root\"\n");
    script.push_str("  exit 1\n");
    script.push_str("fi\n\n");

    script.push_str("echo \"Applying Streamline tuning settings...\"\n\n");

    let params = get_profile_parameters(profile);
    for (name, path, value, _desc) in params {
        script.push_str(&format!("# {}\n", name));
        script.push_str(&format!("echo \"{}\" > {}\n\n", value, path));
    }

    // CPU governor
    script.push_str("# Set CPU governor to performance\n");
    script.push_str("for cpu in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do\n");
    script.push_str("  echo \"performance\" > \"$cpu\" 2>/dev/null || true\n");
    script.push_str("done\n\n");

    // THP
    script.push_str("# Set THP to madvise\n");
    script.push_str(
        "echo \"madvise\" > /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || true\n\n",
    );

    script.push_str("echo \"Tuning complete!\"\n");

    script
}

/// Check if we're on a supported platform
pub fn is_tuning_supported() -> bool {
    cfg!(target_os = "linux")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_parsing() {
        assert_eq!(
            "low-latency".parse::<TuneProfile>().unwrap(),
            TuneProfile::LowLatency
        );
        assert_eq!(
            "high-throughput".parse::<TuneProfile>().unwrap(),
            TuneProfile::HighThroughput
        );
        assert_eq!(
            "latency".parse::<TuneProfile>().unwrap(),
            TuneProfile::LowLatency
        );
        assert_eq!(
            "throughput".parse::<TuneProfile>().unwrap(),
            TuneProfile::HighThroughput
        );
        assert!("invalid".parse::<TuneProfile>().is_err());
    }

    #[test]
    fn test_profile_display() {
        assert_eq!(TuneProfile::LowLatency.to_string(), "low-latency");
        assert_eq!(TuneProfile::HighThroughput.to_string(), "high-throughput");
    }

    #[test]
    fn test_tune_config_default() {
        let config = TuneConfig::default();
        assert_eq!(config.profile, TuneProfile::LowLatency);
        assert!(config.dry_run);
        assert!(!config.force);
    }

    #[test]
    fn test_get_profile_parameters() {
        let low_lat = get_profile_parameters(TuneProfile::LowLatency);
        let high_tp = get_profile_parameters(TuneProfile::HighThroughput);

        assert!(!low_lat.is_empty());
        assert!(!high_tp.is_empty());

        // Both should have vm.swappiness
        assert!(low_lat
            .iter()
            .any(|(name, _, _, _)| *name == "vm.swappiness"));
        assert!(high_tp
            .iter()
            .any(|(name, _, _, _)| *name == "vm.swappiness"));
    }

    #[test]
    fn test_dry_run() {
        // Dry run should work on any platform
        let config = TuneConfig {
            profile: TuneProfile::LowLatency,
            dry_run: true,
            force: false,
        };

        let result = run_tune(config);
        assert!(result.dry_run);
        assert_eq!(result.changed, 0); // No changes in dry run
    }

    #[test]
    fn test_generate_script() {
        let script = generate_tune_script(TuneProfile::LowLatency);
        assert!(script.contains("#!/bin/bash"));
        assert!(script.contains("vm.swappiness"));
        assert!(script.contains("performance"));
    }

    #[test]
    fn test_format_result() {
        let result = TuneResult {
            parameters: vec![TuneParameter {
                name: "test.param".to_string(),
                path: "/test/path".to_string(),
                current: Some("1".to_string()),
                recommended: "2".to_string(),
                description: "Test parameter".to_string(),
                is_optimal: false,
            }],
            already_optimal: 0,
            changed: 0,
            failed: 0,
            dry_run: true,
            profile: TuneProfile::LowLatency,
            warnings: vec![],
        };

        let output = format_tune_result(&result, false);
        assert!(output.contains("System Tuning Report"));
        assert!(output.contains("test.param"));
        assert!(output.contains("[CHANGE]"));
    }
}
