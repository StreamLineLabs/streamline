//! System-level metrics collection
//!
//! This module provides platform-specific utilities for collecting
//! system metrics like CPU, memory, network, and disk statistics.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// System information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    /// Operating system name
    pub os_name: String,
    /// OS version
    pub os_version: String,
    /// Kernel version
    pub kernel_version: String,
    /// Hostname
    pub hostname: String,
    /// CPU architecture
    pub arch: String,
    /// Number of CPU cores
    pub cpu_cores: u32,
    /// Total memory in bytes
    pub total_memory: u64,
}

impl SystemInfo {
    /// Collect system information
    pub fn collect() -> Self {
        Self {
            os_name: std::env::consts::OS.to_string(),
            os_version: get_os_version(),
            kernel_version: get_kernel_version(),
            hostname: get_hostname(),
            arch: std::env::consts::ARCH.to_string(),
            cpu_cores: get_cpu_cores(),
            total_memory: get_total_memory(),
        }
    }
}

/// Get OS version
fn get_os_version() -> String {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/etc/os-release")
            .ok()
            .and_then(|content| {
                content
                    .lines()
                    .find(|line| line.starts_with("VERSION_ID="))
                    .map(|line| {
                        line.trim_start_matches("VERSION_ID=")
                            .trim_matches('"')
                            .to_string()
                    })
            })
            .unwrap_or_else(|| "unknown".to_string())
    }
    #[cfg(not(target_os = "linux"))]
    {
        "unknown".to_string()
    }
}

/// Get kernel version
fn get_kernel_version() -> String {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/version")
            .ok()
            .and_then(|content| content.split_whitespace().nth(2).map(|s| s.to_string()))
            .unwrap_or_else(|| "unknown".to_string())
    }
    #[cfg(not(target_os = "linux"))]
    {
        "unknown".to_string()
    }
}

/// Get hostname
fn get_hostname() -> String {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/etc/hostname")
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|_| "unknown".to_string())
    }
    #[cfg(not(target_os = "linux"))]
    {
        "unknown".to_string()
    }
}

/// Get number of CPU cores
fn get_cpu_cores() -> u32 {
    std::thread::available_parallelism()
        .map(|p| p.get() as u32)
        .unwrap_or(1)
}

/// Get total memory in bytes
fn get_total_memory() -> u64 {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|content| {
                content
                    .lines()
                    .find(|line| line.starts_with("MemTotal:"))
                    .and_then(|line| {
                        line.split_whitespace()
                            .nth(1)
                            .and_then(|s| s.parse::<u64>().ok())
                            .map(|kb| kb * 1024)
                    })
            })
            .unwrap_or(0)
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

/// Process resource limits
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Max open files
    pub max_open_files: u64,
    /// Max processes
    pub max_processes: u64,
    /// Max memory (bytes, 0 = unlimited)
    pub max_memory: u64,
    /// Max CPU time (seconds, 0 = unlimited)
    pub max_cpu_time: u64,
}

impl ResourceLimits {
    /// Get current resource limits
    pub fn get() -> Self {
        #[cfg(target_os = "linux")]
        {
            Self::get_linux()
        }
        #[cfg(not(target_os = "linux"))]
        {
            Self::default()
        }
    }

    #[cfg(target_os = "linux")]
    fn get_linux() -> Self {
        use std::fs;

        let mut limits = Self::default();

        if let Ok(content) = fs::read_to_string("/proc/self/limits") {
            for line in content.lines() {
                if line.starts_with("Max open files") {
                    limits.max_open_files = parse_limit_value(line);
                } else if line.starts_with("Max processes") {
                    limits.max_processes = parse_limit_value(line);
                }
            }
        }

        limits
    }
}

#[cfg(target_os = "linux")]
fn parse_limit_value(line: &str) -> u64 {
    line.split_whitespace()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// Uptime information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UptimeInfo {
    /// System uptime
    pub system_uptime: Duration,
    /// Process uptime
    pub process_uptime: Duration,
}

impl UptimeInfo {
    /// Get uptime information
    pub fn get(process_start: std::time::Instant) -> Self {
        Self {
            system_uptime: get_system_uptime(),
            process_uptime: process_start.elapsed(),
        }
    }
}

/// Get system uptime
fn get_system_uptime() -> Duration {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/uptime")
            .ok()
            .and_then(|content| {
                content
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse::<f64>().ok())
                    .map(Duration::from_secs_f64)
            })
            .unwrap_or_default()
    }
    #[cfg(not(target_os = "linux"))]
    {
        Duration::default()
    }
}

/// Network interface information
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetworkInterface {
    /// Interface name
    pub name: String,
    /// MAC address
    pub mac_address: String,
    /// IP addresses
    pub ip_addresses: Vec<String>,
    /// Is up
    pub is_up: bool,
    /// Speed in Mbps
    pub speed_mbps: u32,
}

/// Get network interfaces
#[allow(dead_code)]
pub fn get_network_interfaces() -> Vec<NetworkInterface> {
    #[cfg(target_os = "linux")]
    {
        get_network_interfaces_linux()
    }
    #[cfg(not(target_os = "linux"))]
    {
        Vec::new()
    }
}

#[cfg(target_os = "linux")]
fn get_network_interfaces_linux() -> Vec<NetworkInterface> {
    use std::fs;

    let mut interfaces = Vec::new();

    if let Ok(entries) = fs::read_dir("/sys/class/net") {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name == "lo" {
                continue; // Skip loopback
            }

            let mut iface = NetworkInterface {
                name: name.clone(),
                ..Default::default()
            };

            // Check if interface is up
            if let Ok(operstate) = fs::read_to_string(format!("/sys/class/net/{}/operstate", name))
            {
                iface.is_up = operstate.trim() == "up";
            }

            // Get MAC address
            if let Ok(mac) = fs::read_to_string(format!("/sys/class/net/{}/address", name)) {
                iface.mac_address = mac.trim().to_string();
            }

            // Get speed
            if let Ok(speed) = fs::read_to_string(format!("/sys/class/net/{}/speed", name)) {
                iface.speed_mbps = speed.trim().parse().unwrap_or(0);
            }

            interfaces.push(iface);
        }
    }

    interfaces
}

/// Disk information
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiskInfo {
    /// Device name
    pub device: String,
    /// Mount point
    pub mount_point: String,
    /// Filesystem type
    pub fs_type: String,
    /// Total size in bytes
    pub total_bytes: u64,
    /// Used bytes
    pub used_bytes: u64,
    /// Available bytes
    pub available_bytes: u64,
}

/// Get disk information
#[allow(dead_code)]
pub fn get_disk_info() -> Vec<DiskInfo> {
    #[cfg(target_os = "linux")]
    {
        get_disk_info_linux()
    }
    #[cfg(not(target_os = "linux"))]
    {
        Vec::new()
    }
}

#[cfg(target_os = "linux")]
fn get_disk_info_linux() -> Vec<DiskInfo> {
    use std::fs;

    let mut disks = Vec::new();

    if let Ok(content) = fs::read_to_string("/proc/mounts") {
        for line in content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 {
                let device = parts[0];
                let mount_point = parts[1];
                let fs_type = parts[2];

                // Only include real filesystems
                if device.starts_with("/dev/") {
                    let disk = DiskInfo {
                        device: device.to_string(),
                        mount_point: mount_point.to_string(),
                        fs_type: fs_type.to_string(),
                        ..Default::default()
                    };
                    disks.push(disk);
                }
            }
        }
    }

    disks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_info() {
        let info = SystemInfo::collect();
        assert!(!info.os_name.is_empty());
        assert!(!info.arch.is_empty());
        assert!(info.cpu_cores > 0);
    }

    #[test]
    fn test_resource_limits() {
        let limits = ResourceLimits::get();
        // On non-Linux, defaults are returned
        #[cfg(target_os = "linux")]
        {
            assert!(limits.max_open_files > 0);
        }
        let _ = limits;
    }

    #[test]
    fn test_uptime() {
        let start = std::time::Instant::now();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let uptime = UptimeInfo::get(start);
        assert!(uptime.process_uptime.as_millis() >= 10);
    }
}
