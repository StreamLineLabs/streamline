//! Thread-per-core runtime for high-performance streaming
//!
//! This module implements a Seastar-inspired sharded execution model where
//! each CPU core has its own independent event loop, data structures, and
//! partition ownership. This architecture minimizes cross-core synchronization
//! and achieves near-linear scaling.
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────┐
//! │                    Thread-per-Core Runtime                              │
//! ├────────────────────────────────────────────────────────────────────────┤
//! │                                                                         │
//! │   Core 0 (Shard 0)      Core 1 (Shard 1)      Core N (Shard N)         │
//! │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐        │
//! │  │ Local Executor  │  │ Local Executor  │  │ Local Executor  │        │
//! │  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │        │
//! │  │ │ Task Queue  │ │  │ │ Task Queue  │ │  │ │ Task Queue  │ │        │
//! │  │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘ │        │
//! │  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │        │
//! │  │ │ Partitions  │ │  │ │ Partitions  │ │  │ │ Partitions  │ │        │
//! │  │ │  [0,3,6..]  │ │  │ │  [1,4,7..]  │ │  │ │  [2,5,8..]  │ │        │
//! │  │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘ │        │
//! │  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │        │
//! │  │ │  Mailbox    │◄──┼──│  Mailbox    │◄──┼──│  Mailbox    │ │        │
//! │  │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘ │        │
//! │  └─────────────────┘  └─────────────────┘  └─────────────────┘        │
//! │          ▲                    ▲                    ▲                   │
//! │          │                    │                    │                   │
//! │          └────────────────────┴────────────────────┘                   │
//! │                     Message Passing (SPSC)                             │
//! │                                                                         │
//! └────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Key Properties
//!
//! - **No shared state**: Each shard owns its partitions exclusively
//! - **CPU affinity**: Threads pinned to specific cores
//! - **NUMA-aware**: Memory allocated local to each core
//! - **Lock-free**: Cross-shard communication via SPSC queues
//! - **Predictable latency**: No garbage collection, no locks
//!
//! # Usage
//!
//! ```rust,ignore
//! use streamline::runtime::{ShardedRuntime, ShardedRuntimeConfig};
//!
//! let config = ShardedRuntimeConfig::builder()
//!     .shard_count(4)
//!     .enable_numa(true)
//!     .build();
//!
//! let runtime = ShardedRuntime::new(config)?;
//!
//! // Submit work to specific shard
//! runtime.submit_to_shard(0, async {
//!     // This runs on core 0
//!     println!("Hello from shard 0");
//! }).await;
//!
//! // Submit work to shard owning partition
//! runtime.submit_for_partition("my-topic", 5, async {
//!     // Routed to shard = partition_id % shard_count
//! }).await;
//!
//! runtime.shutdown().await;
//! ```

pub mod affinity;
pub mod config;
pub mod executor;
pub mod mailbox;
pub mod numa;
pub mod shard;

pub use affinity::*;
pub use config::*;
pub use executor::*;
pub use mailbox::*;
pub use numa::{
    is_numa_available, NumaAllocator, NumaBuffer, NumaConfig, NumaStats, NumaVec, HUGE_PAGE_SIZE,
    PAGE_SIZE,
};
pub use shard::*;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::{error, info};

/// Errors from the sharded runtime
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("Failed to spawn shard thread: {0}")]
    SpawnFailed(String),

    #[error("Shard {shard_id} not found")]
    ShardNotFound { shard_id: usize },

    #[error("Runtime already started")]
    AlreadyStarted,

    #[error("Runtime already stopped")]
    AlreadyStopped,

    #[error("CPU affinity error: {0}")]
    AffinityError(String),

    #[error("Channel error: {0}")]
    ChannelError(String),

    #[error("NUMA error: {0}")]
    NumaError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Task error: {0}")]
    TaskError(String),
}

/// Result type for runtime operations
pub type RuntimeResult<T> = Result<T, RuntimeError>;

/// Sharded runtime for thread-per-core execution
///
/// This is the main entry point for the sharded execution model.
/// It manages multiple shards, each running on its own dedicated CPU core.
pub struct ShardedRuntime {
    /// Configuration
    #[allow(dead_code)]
    config: ShardedRuntimeConfig,

    /// Shards (one per core)
    shards: Vec<Arc<Shard>>,

    /// Cross-shard message router
    router: Arc<MessageRouter>,

    /// NUMA allocator (if enabled)
    numa_allocator: Option<Arc<NumaAllocator>>,

    /// Whether runtime is running
    running: AtomicBool,

    /// Total tasks submitted
    total_tasks: AtomicU64,

    /// Total messages routed
    total_messages: AtomicU64,
}

impl ShardedRuntime {
    /// Create a new sharded runtime
    pub fn new(config: ShardedRuntimeConfig) -> RuntimeResult<Self> {
        config.validate().map_err(RuntimeError::ConfigError)?;

        let shard_count = config.shard_count;
        info!(
            shard_count = shard_count,
            enable_numa = config.enable_numa,
            "Creating sharded runtime"
        );

        // Create NUMA allocator if enabled
        let numa_allocator = if config.enable_numa {
            match NumaAllocator::new() {
                Ok(allocator) => {
                    let allocator = Arc::new(allocator);
                    info!(
                        numa_nodes = allocator.numa_node_count(),
                        numa_enabled = allocator.is_numa_enabled(),
                        "NUMA allocator initialized"
                    );
                    Some(allocator)
                }
                Err(e) => {
                    error!(error = %e, "Failed to create NUMA allocator, falling back to standard allocation");
                    None
                }
            }
        } else {
            None
        };

        // Create message router
        let router = Arc::new(MessageRouter::new(shard_count));

        // Create shards
        let mut shards = Vec::with_capacity(shard_count);
        for shard_id in 0..shard_count {
            let core_id = config.core_ids.get(shard_id).copied().unwrap_or(shard_id);

            // Determine NUMA node for this shard's core
            let numa_node = numa_allocator
                .as_ref()
                .map(|alloc| alloc.node_for_cpu(core_id));

            let shard_config = ShardConfig {
                shard_id,
                core_id,
                numa_node,
                task_queue_capacity: config.task_queue_capacity,
                enable_polling: config.enable_polling,
                polling_interval_us: config.polling_interval_us,
            };

            let mailbox = router.create_mailbox(shard_id);
            let shard = Arc::new(Shard::with_numa_allocator(
                shard_config,
                mailbox,
                numa_allocator.clone(),
            )?);
            shards.push(shard);
        }

        Ok(Self {
            config,
            shards,
            router,
            numa_allocator,
            running: AtomicBool::new(false),
            total_tasks: AtomicU64::new(0),
            total_messages: AtomicU64::new(0),
        })
    }

    /// Start the runtime (spawn all shard threads)
    pub fn start(&self) -> RuntimeResult<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(RuntimeError::AlreadyStarted);
        }

        info!(shard_count = self.shards.len(), "Starting sharded runtime");

        for shard in &self.shards {
            shard.start()?;
        }

        info!("Sharded runtime started");
        Ok(())
    }

    /// Stop the runtime
    pub fn stop(&self) -> RuntimeResult<()> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::AlreadyStopped);
        }

        info!("Stopping sharded runtime");

        for shard in &self.shards {
            shard.stop();
        }

        info!("Sharded runtime stopped");
        Ok(())
    }

    /// Check if runtime is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get number of shards
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Get shard for a partition
    pub fn shard_for_partition(&self, partition_id: u32) -> usize {
        partition_id as usize % self.shards.len()
    }

    /// Submit a task to a specific shard
    pub fn submit_to_shard<F>(&self, shard_id: usize, task: F) -> RuntimeResult<()>
    where
        F: FnOnce() + Send + 'static,
    {
        let shard = self
            .shards
            .get(shard_id)
            .ok_or(RuntimeError::ShardNotFound { shard_id })?;

        shard.submit(task)?;
        self.total_tasks.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Submit a task for a partition (routed to owning shard)
    pub fn submit_for_partition<F>(&self, partition_id: u32, task: F) -> RuntimeResult<()>
    where
        F: FnOnce() + Send + 'static,
    {
        let shard_id = self.shard_for_partition(partition_id);
        self.submit_to_shard(shard_id, task)
    }

    /// Submit a task to a specific shard and receive the result via a channel.
    ///
    /// This bridges the sync shard executor with async code by returning a
    /// `oneshot::Receiver` that can be awaited to get the task's result.
    ///
    /// # Example
    /// ```rust,ignore
    /// let rx = runtime.submit_to_shard_with_result(0, || {
    ///     // Sync work on shard 0
    ///     expensive_computation()
    /// })?;
    /// let result = rx.await.map_err(|_| "shard task cancelled")?;
    /// ```
    pub fn submit_to_shard_with_result<F, R>(
        &self,
        shard_id: usize,
        task: F,
    ) -> RuntimeResult<oneshot::Receiver<R>>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        self.submit_to_shard(shard_id, move || {
            let result = task();
            // Ignore send error - receiver may have been dropped
            let _ = tx.send(result);
        })?;

        Ok(rx)
    }

    /// Submit a task for a partition and receive the result via a channel.
    ///
    /// Routes the task to the shard owning the partition (partition_id % shard_count)
    /// and returns a receiver for the result.
    ///
    /// # Example
    /// ```rust,ignore
    /// let rx = runtime.submit_for_partition_with_result(partition_id, move || {
    ///     topic_manager.append_batch(topic, partition, records)
    /// })?;
    /// let offset = rx.await.map_err(|_| "shard task cancelled")??;
    /// ```
    pub fn submit_for_partition_with_result<F, R>(
        &self,
        partition_id: u32,
        task: F,
    ) -> RuntimeResult<oneshot::Receiver<R>>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let shard_id = self.shard_for_partition(partition_id);
        self.submit_to_shard_with_result(shard_id, task)
    }

    /// Send a message to a specific shard
    pub fn send_to_shard(&self, shard_id: usize, message: ShardMessage) -> RuntimeResult<()> {
        self.router.send(shard_id, message)?;
        self.total_messages.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Get runtime statistics
    pub fn stats(&self) -> ShardedRuntimeStats {
        let shard_stats: Vec<_> = self.shards.iter().map(|s| s.stats()).collect();

        ShardedRuntimeStats {
            running: self.running.load(Ordering::Relaxed),
            shard_count: self.shards.len(),
            numa_enabled: self.numa_allocator.is_some(),
            numa_nodes: self.numa_allocator.as_ref().map(|a| a.numa_node_count()),
            total_tasks: self.total_tasks.load(Ordering::Relaxed),
            total_messages: self.total_messages.load(Ordering::Relaxed),
            shard_stats,
        }
    }

    /// Get a shard by ID
    pub fn get_shard(&self, shard_id: usize) -> Option<&Arc<Shard>> {
        self.shards.get(shard_id)
    }

    /// Get the NUMA allocator (if enabled)
    pub fn numa_allocator(&self) -> Option<&Arc<NumaAllocator>> {
        self.numa_allocator.as_ref()
    }

    /// Check if NUMA allocation is enabled
    pub fn is_numa_enabled(&self) -> bool {
        self.numa_allocator.is_some()
    }
}

impl Drop for ShardedRuntime {
    fn drop(&mut self) {
        if self.running.load(Ordering::SeqCst) {
            if let Err(e) = self.stop() {
                error!(error = %e, "Failed to stop runtime during drop");
            }
        }
    }
}

/// Runtime statistics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ShardedRuntimeStats {
    /// Whether runtime is running
    pub running: bool,
    /// Number of shards
    pub shard_count: usize,
    /// Whether NUMA-aware allocation is enabled
    pub numa_enabled: bool,
    /// Number of NUMA nodes (if NUMA is enabled)
    pub numa_nodes: Option<usize>,
    /// Total tasks submitted
    pub total_tasks: u64,
    /// Total cross-shard messages
    pub total_messages: u64,
    /// Per-shard statistics
    pub shard_stats: Vec<ShardStats>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_creation() {
        let config = ShardedRuntimeConfig::default();
        let runtime = ShardedRuntime::new(config).unwrap();

        assert!(!runtime.is_running());
        // Shard count should match available parallelism
        let expected = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);
        assert_eq!(runtime.shard_count(), expected);
    }

    #[test]
    fn test_shard_for_partition() {
        let config = ShardedRuntimeConfig {
            shard_count: 4,
            ..Default::default()
        };
        let runtime = ShardedRuntime::new(config).unwrap();

        assert_eq!(runtime.shard_for_partition(0), 0);
        assert_eq!(runtime.shard_for_partition(1), 1);
        assert_eq!(runtime.shard_for_partition(4), 0);
        assert_eq!(runtime.shard_for_partition(7), 3);
    }

    #[test]
    fn test_runtime_start_stop() {
        let config = ShardedRuntimeConfig {
            shard_count: 2,
            ..Default::default()
        };
        let runtime = ShardedRuntime::new(config).unwrap();

        assert!(runtime.start().is_ok());
        assert!(runtime.is_running());

        // Double start should fail
        assert!(matches!(runtime.start(), Err(RuntimeError::AlreadyStarted)));

        assert!(runtime.stop().is_ok());
        assert!(!runtime.is_running());

        // Double stop should fail
        assert!(matches!(runtime.stop(), Err(RuntimeError::AlreadyStopped)));
    }

    #[test]
    fn test_runtime_with_numa() {
        let config = ShardedRuntimeConfig {
            shard_count: 2,
            enable_numa: true,
            ..Default::default()
        };
        let runtime = ShardedRuntime::new(config).unwrap();

        // NUMA allocator should be present (though it may not be "enabled" on non-NUMA systems)
        assert!(runtime.numa_allocator().is_some());

        let stats = runtime.stats();
        assert!(stats.numa_enabled);
        assert!(stats.numa_nodes.is_some());

        // Each shard should have the NUMA allocator
        for i in 0..2 {
            let shard = runtime.get_shard(i).unwrap();
            assert!(shard.numa_allocator().is_some());
        }
    }

    #[test]
    fn test_runtime_without_numa() {
        let config = ShardedRuntimeConfig {
            shard_count: 2,
            enable_numa: false,
            ..Default::default()
        };
        let runtime = ShardedRuntime::new(config).unwrap();

        assert!(runtime.numa_allocator().is_none());
        assert!(!runtime.is_numa_enabled());

        let stats = runtime.stats();
        assert!(!stats.numa_enabled);
        assert!(stats.numa_nodes.is_none());

        // Each shard should NOT have a NUMA allocator
        for i in 0..2 {
            let shard = runtime.get_shard(i).unwrap();
            assert!(shard.numa_allocator().is_none());
        }
    }

    #[tokio::test]
    async fn test_submit_to_shard_with_result() {
        let config = ShardedRuntimeConfig {
            shard_count: 2,
            ..Default::default()
        };
        let runtime = ShardedRuntime::new(config).unwrap();
        runtime.start().unwrap();

        // Submit a task that returns a value
        let rx = runtime
            .submit_to_shard_with_result(0, || {
                // Simulate some work
                42 + 1
            })
            .unwrap();

        // Await the result
        let result = rx.await.unwrap();
        assert_eq!(result, 43);

        runtime.stop().unwrap();
    }

    #[tokio::test]
    async fn test_submit_for_partition_with_result() {
        let config = ShardedRuntimeConfig {
            shard_count: 4,
            ..Default::default()
        };
        let runtime = ShardedRuntime::new(config).unwrap();
        runtime.start().unwrap();

        // Submit tasks for different partitions
        let rx0 = runtime
            .submit_for_partition_with_result(0, || "partition 0")
            .unwrap();
        let rx1 = runtime
            .submit_for_partition_with_result(1, || "partition 1")
            .unwrap();
        let rx4 = runtime
            .submit_for_partition_with_result(4, || "partition 4 (shard 0)")
            .unwrap();

        // Verify routing: partition 0 and 4 should go to shard 0
        assert_eq!(runtime.shard_for_partition(0), 0);
        assert_eq!(runtime.shard_for_partition(4), 0);
        assert_eq!(runtime.shard_for_partition(1), 1);

        // Await results
        assert_eq!(rx0.await.unwrap(), "partition 0");
        assert_eq!(rx1.await.unwrap(), "partition 1");
        assert_eq!(rx4.await.unwrap(), "partition 4 (shard 0)");

        runtime.stop().unwrap();
    }

    #[tokio::test]
    async fn test_submit_with_result_error_propagation() {
        let config = ShardedRuntimeConfig {
            shard_count: 2,
            ..Default::default()
        };
        let runtime = ShardedRuntime::new(config).unwrap();
        runtime.start().unwrap();

        // Submit a task that returns a Result
        let rx = runtime
            .submit_for_partition_with_result(0, || -> Result<i32, &'static str> {
                Err("intentional error")
            })
            .unwrap();

        // The channel should succeed, but contain the error
        let result = rx.await.unwrap();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "intentional error");

        runtime.stop().unwrap();
    }

    #[tokio::test]
    async fn test_submit_with_result_concurrent() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let config = ShardedRuntimeConfig {
            shard_count: 4,
            ..Default::default()
        };
        let runtime = std::sync::Arc::new(ShardedRuntime::new(config).unwrap());
        runtime.start().unwrap();

        let counter = std::sync::Arc::new(AtomicUsize::new(0));

        // Submit many tasks concurrently
        let mut receivers = Vec::new();
        for partition in 0..20 {
            let counter_clone = counter.clone();
            let rx = runtime
                .submit_for_partition_with_result(partition, move || {
                    counter_clone.fetch_add(1, Ordering::SeqCst);
                    partition
                })
                .unwrap();
            receivers.push(rx);
        }

        // Collect all results - await each sequentially to avoid futures crate dep in test
        let mut results = Vec::new();
        for rx in receivers {
            results.push(rx.await.unwrap());
        }

        // Verify all tasks completed
        assert_eq!(counter.load(Ordering::SeqCst), 20);
        assert_eq!(results.len(), 20);

        // Verify results are correct
        for (i, result) in results.iter().enumerate() {
            assert_eq!(*result, i as u32);
        }

        runtime.stop().unwrap();
    }
}
