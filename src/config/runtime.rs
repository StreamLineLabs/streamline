//! Runtime configuration for Streamline server
//!
//! This module provides configuration for the server's execution runtime,
//! allowing selection between different runtime modes for optimal performance.

use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Runtime mode for the server
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeMode {
    /// Standard Tokio multi-threaded runtime (default)
    /// - Good general-purpose performance
    /// - Work-stealing scheduler
    /// - Easier to debug and reason about
    #[default]
    Tokio,

    /// Thread-per-core sharded runtime
    /// - CPU affinity for each shard
    /// - Lock-free cross-shard communication
    /// - Better cache locality and reduced context switching
    /// - Higher throughput and lower latency at scale
    Sharded,
}

impl RuntimeMode {
    /// Check if this is the sharded runtime mode
    pub fn is_sharded(&self) -> bool {
        matches!(self, RuntimeMode::Sharded)
    }

    /// Check if this is the tokio runtime mode
    pub fn is_tokio(&self) -> bool {
        matches!(self, RuntimeMode::Tokio)
    }
}

impl FromStr for RuntimeMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "tokio" => Ok(RuntimeMode::Tokio),
            "sharded" => Ok(RuntimeMode::Sharded),
            _ => Err(format!(
                "Invalid runtime mode '{}'. Valid options: tokio, sharded",
                s
            )),
        }
    }
}

impl std::fmt::Display for RuntimeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeMode::Tokio => write!(f, "tokio"),
            RuntimeMode::Sharded => write!(f, "sharded"),
        }
    }
}

/// Runtime configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    /// Runtime mode (tokio or sharded)
    pub mode: RuntimeMode,

    /// Number of shards for sharded runtime (None = use CPU count)
    pub shard_count: Option<usize>,

    /// Enable CPU core affinity for sharded runtime
    pub enable_cpu_affinity: bool,

    /// Enable busy-polling mode for lower latency (uses more CPU)
    pub enable_polling: bool,

    /// Polling interval in microseconds when polling is disabled
    pub polling_interval_us: u64,

    /// Task queue capacity per shard
    pub task_queue_capacity: usize,

    /// Mailbox capacity for cross-shard messages
    pub mailbox_capacity: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            mode: RuntimeMode::Tokio,
            shard_count: None, // Auto-detect from CPU count
            enable_cpu_affinity: true,
            enable_polling: false,
            polling_interval_us: 100,
            task_queue_capacity: 8192,
            mailbox_capacity: 4096,
        }
    }
}

impl RuntimeConfig {
    /// Get the effective shard count
    pub fn effective_shard_count(&self) -> usize {
        self.shard_count.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1)
        })
    }

    /// Validate the runtime configuration
    pub fn validate(&self) -> Result<(), String> {
        if let Some(count) = self.shard_count {
            if count == 0 {
                return Err("shard_count must be at least 1".to_string());
            }
            if count > 256 {
                return Err("shard_count exceeds maximum of 256".to_string());
            }
        }

        if self.task_queue_capacity == 0 {
            return Err("task_queue_capacity must be at least 1".to_string());
        }

        if self.mailbox_capacity == 0 {
            return Err("mailbox_capacity must be at least 1".to_string());
        }

        Ok(())
    }

    /// Convert to ShardedRuntimeConfig for the runtime module
    pub fn to_sharded_config(&self) -> crate::runtime::ShardedRuntimeConfig {
        crate::runtime::ShardedRuntimeConfig {
            shard_count: self.effective_shard_count(),
            core_ids: Vec::new(), // Use default core assignment
            enable_affinity: self.enable_cpu_affinity,
            enable_numa: false, // Will be enabled in Phase 4
            task_queue_capacity: self.task_queue_capacity,
            mailbox_capacity: self.mailbox_capacity,
            enable_polling: self.enable_polling,
            polling_interval_us: self.polling_interval_us,
            enable_work_stealing: false,
            stack_size: 2 * 1024 * 1024, // 2MB default
            thread_priority: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_mode_from_str() {
        assert_eq!(RuntimeMode::from_str("tokio").unwrap(), RuntimeMode::Tokio);
        assert_eq!(
            RuntimeMode::from_str("sharded").unwrap(),
            RuntimeMode::Sharded
        );
        assert_eq!(RuntimeMode::from_str("TOKIO").unwrap(), RuntimeMode::Tokio);
        assert_eq!(
            RuntimeMode::from_str("SHARDED").unwrap(),
            RuntimeMode::Sharded
        );
        assert!(RuntimeMode::from_str("invalid").is_err());
    }

    #[test]
    fn test_runtime_mode_display() {
        assert_eq!(RuntimeMode::Tokio.to_string(), "tokio");
        assert_eq!(RuntimeMode::Sharded.to_string(), "sharded");
    }

    #[test]
    fn test_runtime_config_default() {
        let config = RuntimeConfig::default();
        assert_eq!(config.mode, RuntimeMode::Tokio);
        assert!(config.shard_count.is_none());
        assert!(config.enable_cpu_affinity);
        assert!(!config.enable_polling);
    }

    #[test]
    fn test_runtime_config_validate() {
        let mut config = RuntimeConfig::default();
        assert!(config.validate().is_ok());

        config.shard_count = Some(0);
        assert!(config.validate().is_err());

        config.shard_count = Some(4);
        assert!(config.validate().is_ok());

        config.task_queue_capacity = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_effective_shard_count() {
        let mut config = RuntimeConfig::default();
        assert!(config.effective_shard_count() > 0);

        config.shard_count = Some(8);
        assert_eq!(config.effective_shard_count(), 8);
    }
}
