//! CPU core affinity management
//!
//! This module provides utilities for pinning threads to specific CPU cores,
//! which is essential for the thread-per-core architecture to achieve
//! predictable latencies and cache efficiency.

use super::RuntimeError;
use tracing::warn;

/// Get number of available CPUs
fn available_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
}

/// CPU topology information
#[derive(Debug, Clone, Default)]
pub struct CpuTopology {
    /// Total number of logical CPUs
    pub cpu_count: usize,

    /// Number of physical cores
    pub physical_cores: usize,

    /// Number of NUMA nodes
    pub numa_nodes: usize,

    /// CPUs per NUMA node
    pub cpus_per_node: Vec<Vec<usize>>,

    /// Physical core to logical CPU mapping
    pub core_to_cpus: Vec<Vec<usize>>,

    /// CPU to NUMA node mapping
    pub cpu_to_node: Vec<usize>,
}

impl CpuTopology {
    /// Detect the CPU topology of the system
    pub fn detect() -> Self {
        let cpu_count = available_cpus();
        // For physical cores, we assume no hyperthreading if we can't detect it
        let physical_cores = cpu_count;

        // Try to detect NUMA topology
        let (numa_nodes, cpus_per_node, cpu_to_node) = Self::detect_numa(cpu_count);

        // Detect hyperthreading/SMT pairs
        let core_to_cpus = Self::detect_smt_pairs(cpu_count, physical_cores);

        Self {
            cpu_count,
            physical_cores,
            numa_nodes,
            cpus_per_node,
            core_to_cpus,
            cpu_to_node,
        }
    }

    /// Detect NUMA topology
    fn detect_numa(cpu_count: usize) -> (usize, Vec<Vec<usize>>, Vec<usize>) {
        // Try to read NUMA info from /sys on Linux
        #[cfg(target_os = "linux")]
        {
            if let Ok(nodes) = Self::read_numa_nodes() {
                return nodes;
            }
        }

        // Fallback: assume single NUMA node
        let cpu_to_node = vec![0; cpu_count];
        let cpus_per_node = vec![(0..cpu_count).collect()];
        (1, cpus_per_node, cpu_to_node)
    }

    /// Read NUMA nodes from sysfs (Linux only)
    #[cfg(target_os = "linux")]
    fn read_numa_nodes() -> std::io::Result<(usize, Vec<Vec<usize>>, Vec<usize>)> {
        use std::fs;
        use std::path::Path;

        let numa_path = Path::new("/sys/devices/system/node");
        if !numa_path.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "NUMA sysfs not found",
            ));
        }

        let mut numa_nodes = 0;
        let mut cpus_per_node = Vec::new();
        let cpu_count = available_cpus();
        let mut cpu_to_node = vec![0; cpu_count];

        for entry in fs::read_dir(numa_path)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();

            if name.starts_with("node") {
                if let Ok(node_id) = name[4..].parse::<usize>() {
                    numa_nodes = numa_nodes.max(node_id + 1);

                    // Read CPUs for this node
                    let cpulist_path = entry.path().join("cpulist");
                    if let Ok(cpulist) = fs::read_to_string(cpulist_path) {
                        let cpus = parse_cpu_list(&cpulist);

                        // Ensure cpus_per_node is large enough
                        while cpus_per_node.len() <= node_id {
                            cpus_per_node.push(Vec::new());
                        }
                        cpus_per_node[node_id] = cpus.clone();

                        // Update cpu_to_node mapping
                        for &cpu in &cpus {
                            if cpu < cpu_count {
                                cpu_to_node[cpu] = node_id;
                            }
                        }
                    }
                }
            }
        }

        Ok((numa_nodes.max(1), cpus_per_node, cpu_to_node))
    }

    /// Detect SMT (hyperthreading) pairs
    fn detect_smt_pairs(cpu_count: usize, physical_cores: usize) -> Vec<Vec<usize>> {
        if cpu_count == physical_cores {
            // No hyperthreading
            return (0..cpu_count).map(|c| vec![c]).collect();
        }

        // Try to read from sysfs on Linux
        #[cfg(target_os = "linux")]
        {
            if let Ok(pairs) = Self::read_smt_pairs(cpu_count) {
                return pairs;
            }
        }

        // Fallback: assume interleaved mapping
        // e.g., CPUs 0-3 on cores 0-3, CPUs 4-7 on cores 0-3
        let smt_factor = cpu_count / physical_cores;
        (0..physical_cores)
            .map(|core| {
                (0..smt_factor)
                    .map(|smt| core + smt * physical_cores)
                    .collect()
            })
            .collect()
    }

    /// Read SMT pairs from sysfs (Linux only)
    #[cfg(target_os = "linux")]
    fn read_smt_pairs(cpu_count: usize) -> std::io::Result<Vec<Vec<usize>>> {
        use std::collections::HashMap;
        use std::fs;

        let mut core_id_to_cpus: HashMap<usize, Vec<usize>> = HashMap::new();

        for cpu in 0..cpu_count {
            let core_id_path = format!("/sys/devices/system/cpu/cpu{}/topology/core_id", cpu);
            if let Ok(core_id_str) = fs::read_to_string(&core_id_path) {
                if let Ok(core_id) = core_id_str.trim().parse::<usize>() {
                    core_id_to_cpus.entry(core_id).or_default().push(cpu);
                }
            }
        }

        let mut result: Vec<_> = core_id_to_cpus.into_values().collect();
        result.sort_by_key(|cpus| cpus.first().copied().unwrap_or(0));
        Ok(result)
    }

    /// Get CPUs for a NUMA node
    pub fn cpus_for_node(&self, node: usize) -> &[usize] {
        self.cpus_per_node
            .get(node)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Get NUMA node for a CPU
    pub fn node_for_cpu(&self, cpu: usize) -> usize {
        self.cpu_to_node.get(cpu).copied().unwrap_or(0)
    }

    /// Get recommended CPUs for sharding (avoiding SMT siblings initially)
    pub fn recommended_cpus(&self, count: usize) -> Vec<usize> {
        let mut result = Vec::with_capacity(count);

        // First, pick one CPU from each physical core
        for cpus in &self.core_to_cpus {
            if result.len() >= count {
                break;
            }
            if let Some(&cpu) = cpus.first() {
                result.push(cpu);
            }
        }

        // If we need more, add SMT siblings
        if result.len() < count {
            for cpus in &self.core_to_cpus {
                for &cpu in cpus.iter().skip(1) {
                    if result.len() >= count {
                        break;
                    }
                    if !result.contains(&cpu) {
                        result.push(cpu);
                    }
                }
            }
        }

        result.sort();
        result
    }
}

/// Parse a CPU list string (e.g., "0-3,5,7-9")
#[allow(dead_code)]
fn parse_cpu_list(s: &str) -> Vec<usize> {
    let mut result = Vec::new();

    for part in s.trim().split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        if let Some((start, end)) = part.split_once('-') {
            if let (Ok(start), Ok(end)) = (start.parse::<usize>(), end.parse::<usize>()) {
                result.extend(start..=end);
            }
        } else if let Ok(cpu) = part.parse::<usize>() {
            result.push(cpu);
        }
    }

    result.sort();
    result.dedup();
    result
}

/// Set CPU affinity for the current thread
#[cfg(target_os = "linux")]
pub fn set_affinity(cpu: usize) -> Result<(), RuntimeError> {
    use std::mem;

    unsafe {
        let mut cpuset: libc::cpu_set_t = mem::zeroed();
        libc::CPU_ZERO(&mut cpuset);
        libc::CPU_SET(cpu, &mut cpuset);

        let result = libc::sched_setaffinity(
            0, // current thread
            mem::size_of::<libc::cpu_set_t>(),
            &cpuset,
        );

        if result == 0 {
            debug!(cpu = cpu, "Set thread affinity");
            Ok(())
        } else {
            Err(RuntimeError::AffinityError(format!(
                "sched_setaffinity failed: {}",
                std::io::Error::last_os_error()
            )))
        }
    }
}

/// Set CPU affinity for the current thread (macOS - no-op with warning)
#[cfg(target_os = "macos")]
pub fn set_affinity(cpu: usize) -> Result<(), RuntimeError> {
    warn!(cpu = cpu, "CPU affinity not supported on macOS");
    Ok(())
}

/// Set CPU affinity for the current thread (other platforms)
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn set_affinity(cpu: usize) -> Result<(), RuntimeError> {
    warn!(cpu = cpu, "CPU affinity not supported on this platform");
    Ok(())
}

/// Get CPU affinity for the current thread
#[cfg(target_os = "linux")]
pub fn get_affinity() -> Result<Vec<usize>, RuntimeError> {
    use std::mem;

    unsafe {
        let mut cpuset: libc::cpu_set_t = mem::zeroed();

        let result = libc::sched_getaffinity(
            0, // current thread
            mem::size_of::<libc::cpu_set_t>(),
            &mut cpuset,
        );

        if result == 0 {
            let mut cpus = Vec::new();
            for cpu in 0..libc::CPU_SETSIZE as usize {
                if libc::CPU_ISSET(cpu, &cpuset) {
                    cpus.push(cpu);
                }
            }
            Ok(cpus)
        } else {
            Err(RuntimeError::AffinityError(format!(
                "sched_getaffinity failed: {}",
                std::io::Error::last_os_error()
            )))
        }
    }
}

/// Get CPU affinity (non-Linux)
#[cfg(not(target_os = "linux"))]
pub fn get_affinity() -> Result<Vec<usize>, RuntimeError> {
    // Return all CPUs
    Ok((0..available_cpus()).collect())
}

/// Set thread priority (Linux - requires CAP_SYS_NICE)
#[cfg(target_os = "linux")]
pub fn set_priority(priority: u8) -> Result<(), RuntimeError> {
    if priority == 0 {
        return Ok(());
    }

    // Map 1-99 to nice values -20 to 19 (inverted)
    let nice = 20 - (priority as i32 * 40 / 100);

    unsafe {
        let result = libc::setpriority(libc::PRIO_PROCESS, 0, nice);
        if result == 0 {
            debug!(priority = priority, nice = nice, "Set thread priority");
            Ok(())
        } else {
            warn!(
                priority = priority,
                error = %std::io::Error::last_os_error(),
                "Failed to set thread priority (may require CAP_SYS_NICE)"
            );
            Ok(()) // Don't fail, just warn
        }
    }
}

/// Set thread priority (non-Linux)
#[cfg(not(target_os = "linux"))]
pub fn set_priority(priority: u8) -> Result<(), RuntimeError> {
    if priority > 0 {
        warn!(
            priority = priority,
            "Thread priority not supported on this platform"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cpu_topology_detect() {
        let topo = CpuTopology::detect();
        assert!(topo.cpu_count > 0);
        assert!(topo.physical_cores > 0);
        assert!(topo.physical_cores <= topo.cpu_count);
        assert!(topo.numa_nodes >= 1);
    }

    #[test]
    fn test_parse_cpu_list() {
        assert_eq!(parse_cpu_list("0"), vec![0]);
        assert_eq!(parse_cpu_list("0-3"), vec![0, 1, 2, 3]);
        assert_eq!(parse_cpu_list("0,2,4"), vec![0, 2, 4]);
        assert_eq!(parse_cpu_list("0-2,5,7-9"), vec![0, 1, 2, 5, 7, 8, 9]);
        assert_eq!(parse_cpu_list(""), Vec::<usize>::new());
    }

    #[test]
    fn test_recommended_cpus() {
        let topo = CpuTopology::detect();
        let cpus = topo.recommended_cpus(2);
        assert_eq!(cpus.len(), 2.min(topo.cpu_count));
    }

    #[test]
    fn test_get_affinity() {
        let affinity = get_affinity().unwrap();
        assert!(!affinity.is_empty());
    }
}
