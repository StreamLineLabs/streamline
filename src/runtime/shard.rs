//! Shard abstraction for thread-per-core execution
//!
//! A shard is the fundamental unit of execution in the thread-per-core model.
//! Each shard runs on a dedicated CPU core and owns a set of partitions.

use super::{
    set_affinity, set_priority, Executor, ExecutorStatsSnapshot, Mailbox, MailboxStats,
    NumaAllocator, RuntimeError, RuntimeResult, ShardConfig, ShardMessage,
};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use tracing::{debug, error, info, warn};

/// A shard is a single-threaded execution unit that owns partitions
pub struct Shard {
    /// Shard configuration
    config: ShardConfig,

    /// Local task executor
    executor: Arc<Executor>,

    /// Mailbox for receiving messages from other shards
    mailbox: Arc<Mailbox>,

    /// NUMA-aware allocator (if enabled)
    numa_allocator: Option<Arc<NumaAllocator>>,

    /// NUMA node this shard runs on (detected at thread startup)
    numa_node: AtomicU64,

    /// Partitions owned by this shard
    /// Map of topic -> set of partition IDs
    partitions: RwLock<HashMap<String, HashSet<u32>>>,

    /// Thread handle
    thread_handle: RwLock<Option<JoinHandle<()>>>,

    /// Running flag (Arc for sharing with spawned thread)
    running: Arc<AtomicBool>,

    /// Total tasks processed
    tasks_processed: AtomicU64,

    /// Messages processed
    messages_processed: AtomicU64,
}

impl Shard {
    /// Create a new shard
    pub fn new(config: ShardConfig, mailbox: Arc<Mailbox>) -> RuntimeResult<Self> {
        Self::with_numa_allocator(config, mailbox, None)
    }

    /// Create a new shard with an optional NUMA allocator
    pub fn with_numa_allocator(
        config: ShardConfig,
        mailbox: Arc<Mailbox>,
        numa_allocator: Option<Arc<NumaAllocator>>,
    ) -> RuntimeResult<Self> {
        let executor = Arc::new(Executor::new(
            config.shard_id,
            config.task_queue_capacity,
            config.enable_polling,
            config.polling_interval_us,
        ));

        // Use configured NUMA node or default to u64::MAX (will be detected at startup)
        let initial_numa_node = config.numa_node.map(|n| n as u64).unwrap_or(u64::MAX);

        Ok(Self {
            config,
            executor,
            mailbox,
            numa_allocator,
            numa_node: AtomicU64::new(initial_numa_node),
            partitions: RwLock::new(HashMap::new()),
            thread_handle: RwLock::new(None),
            running: Arc::new(AtomicBool::new(false)),
            tasks_processed: AtomicU64::new(0),
            messages_processed: AtomicU64::new(0),
        })
    }

    /// Start the shard (spawn thread)
    pub fn start(&self) -> RuntimeResult<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(()); // Already running
        }

        let shard_id = self.config.shard_id;
        let core_id = self.config.core_id;
        let executor = self.executor.clone();
        let mailbox = self.mailbox.clone();
        let running = self.running.clone();
        let numa_allocator = self.numa_allocator.clone();

        info!(shard_id = shard_id, core_id = core_id, "Starting shard");

        let handle = thread::Builder::new()
            .name(format!("shard-{}", shard_id))
            .spawn(move || {
                // Set CPU affinity
                if let Err(e) = set_affinity(core_id) {
                    warn!(shard_id = shard_id, error = %e, "Failed to set CPU affinity");
                }

                // Set thread priority
                if let Err(e) = set_priority(0) {
                    warn!(shard_id = shard_id, error = %e, "Failed to set thread priority");
                }

                // Detect NUMA node after affinity is set
                let numa_node = if let Some(ref allocator) = numa_allocator {
                    let node = allocator.current_node();
                    info!(
                        shard_id = shard_id,
                        core_id = core_id,
                        numa_node = node,
                        numa_enabled = allocator.is_numa_enabled(),
                        "Shard thread started with NUMA-aware allocation"
                    );
                    node
                } else {
                    debug!(
                        shard_id = shard_id,
                        core_id = core_id,
                        "Shard thread started (NUMA disabled)"
                    );
                    0
                };

                // Log NUMA node for debugging
                debug!(
                    shard_id = shard_id,
                    core_id = core_id,
                    numa_node = numa_node,
                    "Shard thread initialized"
                );

                // Run the executor
                while running.load(Ordering::SeqCst) {
                    // Process local tasks
                    let _tasks_processed = executor.process_batch();

                    // Process mailbox messages
                    let messages = mailbox.receive_all();
                    for message in messages {
                        Self::handle_message(shard_id, message);
                    }

                    // If no work, yield or sleep
                    if executor.queue_len() == 0 {
                        std::hint::spin_loop();
                    }
                }

                debug!(shard_id = shard_id, "Shard thread stopped");
            })
            .map_err(|e| RuntimeError::SpawnFailed(e.to_string()))?;

        *self.thread_handle.write() = Some(handle);
        Ok(())
    }

    /// Stop the shard
    pub fn stop(&self) {
        if !self.running.swap(false, Ordering::SeqCst) {
            return; // Already stopped
        }

        self.executor.stop();

        // Wait for thread to finish
        if let Some(handle) = self.thread_handle.write().take() {
            if let Err(e) = handle.join() {
                error!(
                    shard_id = self.config.shard_id,
                    "Shard thread panicked: {:?}", e
                );
            }
        }

        info!(shard_id = self.config.shard_id, "Shard stopped");
    }

    /// Handle a message from another shard
    fn handle_message(shard_id: usize, message: ShardMessage) {
        match message {
            ShardMessage::Ping { request_id } => {
                debug!(
                    shard_id = shard_id,
                    request_id = request_id,
                    "Received ping"
                );
                // Would send pong response back
            }
            ShardMessage::Pong { request_id } => {
                debug!(
                    shard_id = shard_id,
                    request_id = request_id,
                    "Received pong"
                );
            }
            ShardMessage::Shutdown => {
                debug!(shard_id = shard_id, "Received shutdown signal");
            }
            ShardMessage::ProduceBatch {
                topic, partition, ..
            } => {
                debug!(
                    shard_id = shard_id,
                    topic = %topic,
                    partition = partition,
                    "Received produce batch"
                );
            }
            ShardMessage::FetchRequest {
                topic,
                partition,
                offset,
                ..
            } => {
                debug!(
                    shard_id = shard_id,
                    topic = %topic,
                    partition = partition,
                    offset = offset,
                    "Received fetch request"
                );
            }
            ShardMessage::FetchResponse { request_id, .. } => {
                debug!(
                    shard_id = shard_id,
                    request_id = request_id,
                    "Received fetch response"
                );
            }
            ShardMessage::PartitionOwnership {
                topic,
                partition,
                new_owner,
            } => {
                debug!(
                    shard_id = shard_id,
                    topic = %topic,
                    partition = partition,
                    new_owner = new_owner,
                    "Partition ownership change"
                );
            }
            ShardMessage::Custom { message_type, .. } => {
                debug!(
                    shard_id = shard_id,
                    message_type = message_type,
                    "Custom message"
                );
            }
        }
    }

    /// Submit a task to this shard
    pub fn submit<F>(&self, task: F) -> RuntimeResult<()>
    where
        F: FnOnce() + Send + 'static,
    {
        self.executor.submit(task)
    }

    /// Check if shard is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get shard ID
    pub fn shard_id(&self) -> usize {
        self.config.shard_id
    }

    /// Get core ID
    pub fn core_id(&self) -> usize {
        self.config.core_id
    }

    /// Get the detected NUMA node for this shard
    pub fn numa_node(&self) -> Option<usize> {
        let node = self.numa_node.load(Ordering::Relaxed);
        if node == u64::MAX {
            None
        } else {
            Some(node as usize)
        }
    }

    /// Get the NUMA allocator (if enabled)
    pub fn numa_allocator(&self) -> Option<&Arc<NumaAllocator>> {
        self.numa_allocator.as_ref()
    }

    /// Allocate NUMA-local memory (if NUMA is enabled, otherwise returns error)
    pub fn alloc_local(&self, size: usize) -> Result<super::NumaBuffer, std::io::Error> {
        match &self.numa_allocator {
            Some(allocator) => super::NumaBuffer::new_local(allocator, size),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "NUMA allocation not enabled for this shard",
            )),
        }
    }

    /// Add partition ownership
    pub fn add_partition(&self, topic: &str, partition_id: u32) {
        self.partitions
            .write()
            .entry(topic.to_string())
            .or_default()
            .insert(partition_id);

        debug!(
            shard_id = self.config.shard_id,
            topic = topic,
            partition = partition_id,
            "Added partition ownership"
        );
    }

    /// Remove partition ownership
    pub fn remove_partition(&self, topic: &str, partition_id: u32) {
        if let Some(partitions) = self.partitions.write().get_mut(topic) {
            partitions.remove(&partition_id);
        }

        debug!(
            shard_id = self.config.shard_id,
            topic = topic,
            partition = partition_id,
            "Removed partition ownership"
        );
    }

    /// Check if shard owns a partition
    pub fn owns_partition(&self, topic: &str, partition_id: u32) -> bool {
        self.partitions
            .read()
            .get(topic)
            .map(|p| p.contains(&partition_id))
            .unwrap_or(false)
    }

    /// Get all owned partitions
    pub fn owned_partitions(&self) -> HashMap<String, HashSet<u32>> {
        self.partitions.read().clone()
    }

    /// Get statistics
    pub fn stats(&self) -> ShardStats {
        ShardStats {
            shard_id: self.config.shard_id,
            core_id: self.config.core_id,
            numa_node: self.numa_node(),
            numa_enabled: self.numa_allocator.is_some(),
            running: self.running.load(Ordering::Relaxed),
            tasks_processed: self.tasks_processed.load(Ordering::Relaxed),
            messages_processed: self.messages_processed.load(Ordering::Relaxed),
            executor_stats: self.executor.stats(),
            mailbox_stats: self.mailbox.stats(),
            partition_count: self.partitions.read().values().map(|p| p.len()).sum(),
        }
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        if self.running.load(Ordering::SeqCst) {
            self.stop();
        }
    }
}

/// Shard statistics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ShardStats {
    /// Shard ID
    pub shard_id: usize,
    /// CPU core ID
    pub core_id: usize,
    /// NUMA node (None if not yet detected or NUMA disabled)
    pub numa_node: Option<usize>,
    /// Whether NUMA-aware allocation is enabled
    pub numa_enabled: bool,
    /// Whether shard is running
    pub running: bool,
    /// Total tasks processed
    pub tasks_processed: u64,
    /// Total messages processed
    pub messages_processed: u64,
    /// Executor statistics
    pub executor_stats: ExecutorStatsSnapshot,
    /// Mailbox statistics
    pub mailbox_stats: MailboxStats,
    /// Number of partitions owned
    pub partition_count: usize,
}

/// Partition assignment strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PartitionAssignment {
    /// Round-robin assignment (partition_id % shard_count)
    #[default]
    RoundRobin,
    /// Hash-based assignment using topic and partition
    Hash,
    /// NUMA-aware assignment (prefer local memory)
    NumaAware,
    /// Custom assignment (user-provided)
    Custom,
}

/// Partition router for determining which shard owns a partition
pub struct PartitionRouter {
    /// Number of shards
    shard_count: usize,
    /// Assignment strategy
    strategy: PartitionAssignment,
    /// Custom assignment map (topic:partition -> shard)
    custom_assignments: RwLock<HashMap<(String, u32), usize>>,
}

impl PartitionRouter {
    /// Create a new partition router
    pub fn new(shard_count: usize, strategy: PartitionAssignment) -> Self {
        Self {
            shard_count,
            strategy,
            custom_assignments: RwLock::new(HashMap::new()),
        }
    }

    /// Get the shard ID for a partition
    pub fn shard_for_partition(&self, topic: &str, partition_id: u32) -> usize {
        match self.strategy {
            PartitionAssignment::RoundRobin => partition_id as usize % self.shard_count,
            PartitionAssignment::Hash => {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};

                let mut hasher = DefaultHasher::new();
                topic.hash(&mut hasher);
                partition_id.hash(&mut hasher);
                (hasher.finish() as usize) % self.shard_count
            }
            PartitionAssignment::NumaAware => {
                // For NUMA-aware, we'd consider NUMA topology
                // For now, fall back to round-robin
                partition_id as usize % self.shard_count
            }
            PartitionAssignment::Custom => self
                .custom_assignments
                .read()
                .get(&(topic.to_string(), partition_id))
                .copied()
                .unwrap_or_else(|| partition_id as usize % self.shard_count),
        }
    }

    /// Set custom assignment for a partition
    pub fn set_assignment(&self, topic: &str, partition_id: u32, shard_id: usize) {
        self.custom_assignments
            .write()
            .insert((topic.to_string(), partition_id), shard_id);
    }

    /// Remove custom assignment
    pub fn remove_assignment(&self, topic: &str, partition_id: u32) {
        self.custom_assignments
            .write()
            .remove(&(topic.to_string(), partition_id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_creation() {
        let config = ShardConfig {
            shard_id: 0,
            core_id: 0,
            numa_node: None,
            task_queue_capacity: 1024,
            enable_polling: false,
            polling_interval_us: 100,
        };
        let mailbox = Arc::new(Mailbox::new(0, 1, 1024));
        let shard = Shard::new(config, mailbox).unwrap();

        assert_eq!(shard.shard_id(), 0);
        assert!(!shard.is_running());
    }

    #[test]
    fn test_partition_ownership() {
        let config = ShardConfig {
            shard_id: 0,
            core_id: 0,
            numa_node: None,
            task_queue_capacity: 1024,
            enable_polling: false,
            polling_interval_us: 100,
        };
        let mailbox = Arc::new(Mailbox::new(0, 1, 1024));
        let shard = Shard::new(config, mailbox).unwrap();

        shard.add_partition("topic1", 0);
        shard.add_partition("topic1", 1);
        shard.add_partition("topic2", 0);

        assert!(shard.owns_partition("topic1", 0));
        assert!(shard.owns_partition("topic1", 1));
        assert!(shard.owns_partition("topic2", 0));
        assert!(!shard.owns_partition("topic1", 2));

        shard.remove_partition("topic1", 0);
        assert!(!shard.owns_partition("topic1", 0));
    }

    #[test]
    fn test_partition_router_round_robin() {
        let router = PartitionRouter::new(4, PartitionAssignment::RoundRobin);

        assert_eq!(router.shard_for_partition("topic", 0), 0);
        assert_eq!(router.shard_for_partition("topic", 1), 1);
        assert_eq!(router.shard_for_partition("topic", 4), 0);
        assert_eq!(router.shard_for_partition("topic", 7), 3);
    }

    #[test]
    fn test_partition_router_custom() {
        let router = PartitionRouter::new(4, PartitionAssignment::Custom);

        // Without custom assignment, falls back to round-robin
        assert_eq!(router.shard_for_partition("topic", 0), 0);

        // With custom assignment
        router.set_assignment("topic", 0, 3);
        assert_eq!(router.shard_for_partition("topic", 0), 3);

        // Remove custom assignment
        router.remove_assignment("topic", 0);
        assert_eq!(router.shard_for_partition("topic", 0), 0);
    }

    #[test]
    fn test_shard_stats() {
        let config = ShardConfig {
            shard_id: 0,
            core_id: 0,
            numa_node: None,
            task_queue_capacity: 1024,
            enable_polling: false,
            polling_interval_us: 100,
        };
        let mailbox = Arc::new(Mailbox::new(0, 1, 1024));
        let shard = Shard::new(config, mailbox).unwrap();

        shard.add_partition("topic", 0);
        shard.add_partition("topic", 1);

        let stats = shard.stats();
        assert_eq!(stats.shard_id, 0);
        assert_eq!(stats.partition_count, 2);
        assert!(!stats.running);
        assert!(!stats.numa_enabled);
    }

    #[test]
    fn test_shard_with_numa() {
        let allocator = Arc::new(NumaAllocator::default());
        let config = ShardConfig {
            shard_id: 0,
            core_id: 0,
            numa_node: Some(0),
            task_queue_capacity: 1024,
            enable_polling: false,
            polling_interval_us: 100,
        };
        let mailbox = Arc::new(Mailbox::new(0, 1, 1024));
        let shard = Shard::with_numa_allocator(config, mailbox, Some(allocator)).unwrap();

        let stats = shard.stats();
        assert!(stats.numa_enabled);
        assert!(shard.numa_allocator().is_some());
    }
}
