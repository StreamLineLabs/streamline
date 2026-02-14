//! Configuration for the sharded runtime

use serde::{Deserialize, Serialize};

/// Configuration for the sharded runtime
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardedRuntimeConfig {
    /// Number of shards (typically one per CPU core)
    #[serde(default = "default_shard_count")]
    pub shard_count: usize,

    /// Specific CPU core IDs to use for each shard
    /// If empty, uses cores 0..shard_count
    #[serde(default)]
    pub core_ids: Vec<usize>,

    /// Enable CPU core affinity (pin threads to cores)
    #[serde(default = "default_true")]
    pub enable_affinity: bool,

    /// Enable NUMA-aware memory allocation
    #[serde(default)]
    pub enable_numa: bool,

    /// Task queue capacity per shard
    #[serde(default = "default_task_queue_capacity")]
    pub task_queue_capacity: usize,

    /// Mailbox capacity per shard (cross-shard messages)
    #[serde(default = "default_mailbox_capacity")]
    pub mailbox_capacity: usize,

    /// Enable busy-polling mode (lower latency, higher CPU)
    #[serde(default)]
    pub enable_polling: bool,

    /// Polling interval in microseconds (if polling disabled)
    #[serde(default = "default_polling_interval")]
    pub polling_interval_us: u64,

    /// Enable work stealing between shards
    #[serde(default)]
    pub enable_work_stealing: bool,

    /// Stack size for shard threads (bytes)
    #[serde(default = "default_stack_size")]
    pub stack_size: usize,

    /// Thread priority (0-99, higher = more priority)
    #[serde(default = "default_thread_priority")]
    pub thread_priority: u8,
}

fn default_shard_count() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
}

fn default_true() -> bool {
    true
}

fn default_task_queue_capacity() -> usize {
    8192
}

fn default_mailbox_capacity() -> usize {
    4096
}

fn default_polling_interval() -> u64 {
    100 // 100 microseconds
}

fn default_stack_size() -> usize {
    2 * 1024 * 1024 // 2MB
}

fn default_thread_priority() -> u8 {
    0 // Normal priority
}

impl Default for ShardedRuntimeConfig {
    fn default() -> Self {
        Self {
            shard_count: default_shard_count(),
            core_ids: Vec::new(),
            enable_affinity: true,
            enable_numa: false,
            task_queue_capacity: 8192,
            mailbox_capacity: 4096,
            enable_polling: false,
            polling_interval_us: 100,
            enable_work_stealing: false,
            stack_size: 2 * 1024 * 1024,
            thread_priority: 0,
        }
    }
}

impl ShardedRuntimeConfig {
    /// Create a new config builder
    pub fn builder() -> ShardedRuntimeConfigBuilder {
        ShardedRuntimeConfigBuilder::new()
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.shard_count == 0 {
            return Err("shard_count must be at least 1".to_string());
        }

        if self.shard_count > 256 {
            return Err("shard_count exceeds maximum of 256".to_string());
        }

        if !self.core_ids.is_empty() && self.core_ids.len() != self.shard_count {
            return Err(format!(
                "core_ids length ({}) must match shard_count ({})",
                self.core_ids.len(),
                self.shard_count
            ));
        }

        if self.task_queue_capacity == 0 {
            return Err("task_queue_capacity must be at least 1".to_string());
        }

        if self.mailbox_capacity == 0 {
            return Err("mailbox_capacity must be at least 1".to_string());
        }

        if self.stack_size < 64 * 1024 {
            return Err("stack_size must be at least 64KB".to_string());
        }

        Ok(())
    }

    /// Get effective core IDs
    pub fn effective_core_ids(&self) -> Vec<usize> {
        if self.core_ids.is_empty() {
            (0..self.shard_count).collect()
        } else {
            self.core_ids.clone()
        }
    }
}

/// Builder for ShardedRuntimeConfig
pub struct ShardedRuntimeConfigBuilder {
    config: ShardedRuntimeConfig,
}

impl ShardedRuntimeConfigBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: ShardedRuntimeConfig::default(),
        }
    }

    /// Set the number of shards
    pub fn shard_count(mut self, count: usize) -> Self {
        self.config.shard_count = count;
        self
    }

    /// Set specific core IDs to use
    pub fn core_ids(mut self, ids: Vec<usize>) -> Self {
        self.config.core_ids = ids;
        self
    }

    /// Enable/disable CPU affinity
    pub fn enable_affinity(mut self, enable: bool) -> Self {
        self.config.enable_affinity = enable;
        self
    }

    /// Enable/disable NUMA-aware allocation
    pub fn enable_numa(mut self, enable: bool) -> Self {
        self.config.enable_numa = enable;
        self
    }

    /// Set task queue capacity
    pub fn task_queue_capacity(mut self, capacity: usize) -> Self {
        self.config.task_queue_capacity = capacity;
        self
    }

    /// Set mailbox capacity
    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        self.config.mailbox_capacity = capacity;
        self
    }

    /// Enable/disable busy-polling
    pub fn enable_polling(mut self, enable: bool) -> Self {
        self.config.enable_polling = enable;
        self
    }

    /// Set polling interval
    pub fn polling_interval_us(mut self, interval: u64) -> Self {
        self.config.polling_interval_us = interval;
        self
    }

    /// Enable/disable work stealing
    pub fn enable_work_stealing(mut self, enable: bool) -> Self {
        self.config.enable_work_stealing = enable;
        self
    }

    /// Set thread stack size
    pub fn stack_size(mut self, size: usize) -> Self {
        self.config.stack_size = size;
        self
    }

    /// Set thread priority
    pub fn thread_priority(mut self, priority: u8) -> Self {
        self.config.thread_priority = priority;
        self
    }

    /// Build the configuration
    pub fn build(self) -> ShardedRuntimeConfig {
        self.config
    }
}

impl Default for ShardedRuntimeConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for a single shard
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// Shard ID
    pub shard_id: usize,

    /// CPU core to run on
    pub core_id: usize,

    /// NUMA node for this shard (auto-detected from core_id if not specified)
    pub numa_node: Option<usize>,

    /// Task queue capacity
    pub task_queue_capacity: usize,

    /// Enable busy-polling
    pub enable_polling: bool,

    /// Polling interval in microseconds
    pub polling_interval_us: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ShardedRuntimeConfig::default();
        assert!(config.validate().is_ok());
        assert!(config.shard_count > 0);
    }

    #[test]
    fn test_builder() {
        let config = ShardedRuntimeConfig::builder()
            .shard_count(4)
            .enable_numa(true)
            .task_queue_capacity(16384)
            .build();

        assert_eq!(config.shard_count, 4);
        assert!(config.enable_numa);
        assert_eq!(config.task_queue_capacity, 16384);
    }

    #[test]
    fn test_validation() {
        let mut config = ShardedRuntimeConfig::default();
        assert!(config.validate().is_ok());

        config.shard_count = 0;
        assert!(config.validate().is_err());

        config.shard_count = 4;
        config.core_ids = vec![0, 1]; // Wrong length
        assert!(config.validate().is_err());

        config.core_ids = vec![0, 1, 2, 3];
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_effective_core_ids() {
        let config = ShardedRuntimeConfig {
            shard_count: 4,
            core_ids: Vec::new(),
            ..Default::default()
        };
        assert_eq!(config.effective_core_ids(), vec![0, 1, 2, 3]);

        let config2 = ShardedRuntimeConfig {
            shard_count: 2,
            core_ids: vec![2, 4],
            ..Default::default()
        };
        assert_eq!(config2.effective_core_ids(), vec![2, 4]);
    }
}
