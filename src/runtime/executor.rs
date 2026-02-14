//! Per-core task executor
//!
//! This module provides a single-threaded executor that runs on a dedicated
//! CPU core, processing tasks from a local queue without locks.

use super::{RuntimeError, RuntimeResult};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, trace};

/// A task that can be executed by the executor
pub type Task = Box<dyn FnOnce() + Send + 'static>;

/// Per-core task executor
///
/// This executor is designed to run on a single CPU core, processing
/// tasks from its local queue. It never blocks and uses either
/// busy-polling or timed sleeps between polls.
pub struct Executor {
    /// Executor ID (same as shard ID)
    id: usize,

    /// Task queue (local, single-threaded access)
    task_queue: Mutex<VecDeque<Task>>,

    /// Max queue capacity
    capacity: usize,

    /// Running flag
    running: AtomicBool,

    /// Statistics
    stats: ExecutorStats,

    /// Enable busy-polling
    enable_polling: bool,

    /// Polling interval
    polling_interval: Duration,
}

impl Executor {
    /// Create a new executor
    pub fn new(id: usize, capacity: usize, enable_polling: bool, polling_interval_us: u64) -> Self {
        Self {
            id,
            task_queue: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            running: AtomicBool::new(false),
            stats: ExecutorStats::default(),
            enable_polling,
            polling_interval: Duration::from_micros(polling_interval_us),
        }
    }

    /// Submit a task to the executor
    pub fn submit<F>(&self, task: F) -> RuntimeResult<()>
    where
        F: FnOnce() + Send + 'static,
    {
        let mut queue = self.task_queue.lock();

        if queue.len() >= self.capacity {
            self.stats.tasks_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(RuntimeError::TaskError("Task queue full".to_string()));
        }

        queue.push_back(Box::new(task));
        self.stats.tasks_submitted.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Run the executor loop
    ///
    /// This method blocks and runs tasks until stop() is called.
    pub fn run(&self) {
        self.running.store(true, Ordering::SeqCst);
        debug!(executor_id = self.id, "Executor started");

        while self.running.load(Ordering::SeqCst) {
            let loop_start = Instant::now();

            // Process all available tasks
            let tasks_processed = self.process_batch();

            // Update idle time if no work
            if tasks_processed == 0 {
                if self.enable_polling {
                    // Busy poll - yield to scheduler but don't sleep
                    std::hint::spin_loop();
                } else {
                    // Sleep briefly
                    std::thread::sleep(self.polling_interval);
                }

                let idle_ns = loop_start.elapsed().as_nanos() as u64;
                self.stats
                    .idle_time_ns
                    .fetch_add(idle_ns, Ordering::Relaxed);
            }
        }

        debug!(executor_id = self.id, "Executor stopped");
    }

    /// Process a batch of tasks
    pub fn process_batch(&self) -> usize {
        let mut tasks: Vec<Task> = {
            let mut queue = self.task_queue.lock();
            // Take up to 64 tasks at a time
            let count = queue.len().min(64);
            queue.drain(..count).collect()
        };

        let count = tasks.len();
        let _batch_start = Instant::now();

        for task in tasks.drain(..) {
            let task_start = Instant::now();

            // Execute the task
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                task();
            }))
            .unwrap_or_else(|_| {
                error!(executor_id = self.id, "Task panicked");
                self.stats.task_panics.fetch_add(1, Ordering::Relaxed);
            });

            let task_duration = task_start.elapsed();
            self.stats
                .task_time_ns
                .fetch_add(task_duration.as_nanos() as u64, Ordering::Relaxed);
        }

        if count > 0 {
            self.stats
                .tasks_completed
                .fetch_add(count as u64, Ordering::Relaxed);
            self.stats.batches_processed.fetch_add(1, Ordering::Relaxed);
        }

        count
    }

    /// Stop the executor
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Check if executor is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get executor ID
    pub fn id(&self) -> usize {
        self.id
    }

    /// Get current queue length
    pub fn queue_len(&self) -> usize {
        self.task_queue.lock().len()
    }

    /// Get statistics snapshot
    pub fn stats(&self) -> ExecutorStatsSnapshot {
        ExecutorStatsSnapshot {
            tasks_submitted: self.stats.tasks_submitted.load(Ordering::Relaxed),
            tasks_completed: self.stats.tasks_completed.load(Ordering::Relaxed),
            tasks_rejected: self.stats.tasks_rejected.load(Ordering::Relaxed),
            task_panics: self.stats.task_panics.load(Ordering::Relaxed),
            batches_processed: self.stats.batches_processed.load(Ordering::Relaxed),
            task_time_ns: self.stats.task_time_ns.load(Ordering::Relaxed),
            idle_time_ns: self.stats.idle_time_ns.load(Ordering::Relaxed),
            current_queue_len: self.queue_len(),
        }
    }
}

/// Executor statistics (atomic counters)
#[derive(Debug, Default)]
struct ExecutorStats {
    /// Tasks submitted
    tasks_submitted: AtomicU64,
    /// Tasks completed successfully
    tasks_completed: AtomicU64,
    /// Tasks rejected (queue full)
    tasks_rejected: AtomicU64,
    /// Tasks that panicked
    task_panics: AtomicU64,
    /// Batches processed
    batches_processed: AtomicU64,
    /// Total task execution time (nanoseconds)
    task_time_ns: AtomicU64,
    /// Total idle time (nanoseconds)
    idle_time_ns: AtomicU64,
}

/// Snapshot of executor statistics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ExecutorStatsSnapshot {
    /// Tasks submitted
    pub tasks_submitted: u64,
    /// Tasks completed
    pub tasks_completed: u64,
    /// Tasks rejected
    pub tasks_rejected: u64,
    /// Task panics
    pub task_panics: u64,
    /// Batches processed
    pub batches_processed: u64,
    /// Total task time in nanoseconds
    pub task_time_ns: u64,
    /// Total idle time in nanoseconds
    pub idle_time_ns: u64,
    /// Current queue length
    pub current_queue_len: usize,
}

impl ExecutorStatsSnapshot {
    /// Calculate average task execution time in microseconds
    pub fn avg_task_time_us(&self) -> f64 {
        if self.tasks_completed == 0 {
            0.0
        } else {
            (self.task_time_ns as f64 / self.tasks_completed as f64) / 1000.0
        }
    }

    /// Calculate utilization percentage
    pub fn utilization(&self) -> f64 {
        let total = self.task_time_ns + self.idle_time_ns;
        if total == 0 {
            0.0
        } else {
            (self.task_time_ns as f64 / total as f64) * 100.0
        }
    }
}

/// Work-stealing executor for load balancing
///
/// This executor can steal tasks from other executors when idle,
/// providing better load balancing at the cost of some cache locality.
pub struct WorkStealingExecutor {
    /// Local executor
    local: Executor,

    /// Other executors to steal from
    neighbors: Vec<Arc<Executor>>,

    /// Tasks stolen
    tasks_stolen: AtomicU64,
}

impl WorkStealingExecutor {
    /// Create a new work-stealing executor
    pub fn new(
        id: usize,
        capacity: usize,
        enable_polling: bool,
        polling_interval_us: u64,
        neighbors: Vec<Arc<Executor>>,
    ) -> Self {
        Self {
            local: Executor::new(id, capacity, enable_polling, polling_interval_us),
            neighbors,
            tasks_stolen: AtomicU64::new(0),
        }
    }

    /// Submit a task locally
    pub fn submit<F>(&self, task: F) -> RuntimeResult<()>
    where
        F: FnOnce() + Send + 'static,
    {
        self.local.submit(task)
    }

    /// Run the work-stealing executor
    pub fn run(&self) {
        self.local.running.store(true, Ordering::SeqCst);
        debug!(
            executor_id = self.local.id,
            "Work-stealing executor started"
        );

        while self.local.running.load(Ordering::SeqCst) {
            // First, try local tasks
            let local_processed = self.local.process_batch();

            // If no local work, try stealing
            if local_processed == 0 {
                let stolen = self.try_steal();
                if stolen == 0 {
                    // No work anywhere, wait
                    if self.local.enable_polling {
                        std::hint::spin_loop();
                    } else {
                        std::thread::sleep(self.local.polling_interval);
                    }
                }
            }
        }

        debug!(
            executor_id = self.local.id,
            "Work-stealing executor stopped"
        );
    }

    /// Try to steal tasks from neighbors
    fn try_steal(&self) -> usize {
        for neighbor in &self.neighbors {
            // Try to steal half of neighbor's tasks
            let neighbor_len = neighbor.queue_len();
            if neighbor_len > 1 {
                let steal_count = neighbor_len / 2;
                let mut stolen = 0;

                // Note: This is a simplified implementation.
                // A real work-stealing queue would use deque stealing.
                let mut tasks: Vec<Task> = {
                    let mut queue = neighbor.task_queue.lock();
                    let count = queue.len().min(steal_count);
                    queue.drain(..count).collect()
                };

                for task in tasks.drain(..) {
                    if self.local.submit(task).is_ok() {
                        stolen += 1;
                    }
                }

                if stolen > 0 {
                    self.tasks_stolen
                        .fetch_add(stolen as u64, Ordering::Relaxed);
                    trace!(
                        executor_id = self.local.id,
                        from = neighbor.id,
                        count = stolen,
                        "Stole tasks"
                    );
                    return stolen;
                }
            }
        }

        0
    }

    /// Stop the executor
    pub fn stop(&self) {
        self.local.stop();
    }

    /// Get tasks stolen count
    pub fn tasks_stolen(&self) -> u64 {
        self.tasks_stolen.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    #[test]
    fn test_executor_submit() {
        let executor = Executor::new(0, 100, false, 1000);

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        executor
            .submit(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();

        assert_eq!(executor.queue_len(), 1);
    }

    #[test]
    fn test_executor_capacity() {
        let executor = Executor::new(0, 2, false, 1000);

        executor.submit(|| {}).unwrap();
        executor.submit(|| {}).unwrap();
        assert!(executor.submit(|| {}).is_err()); // Queue full
    }

    #[test]
    fn test_executor_stats() {
        let executor = Executor::new(0, 100, false, 1000);

        executor.submit(|| {}).unwrap();
        executor.submit(|| {}).unwrap();

        let stats = executor.stats();
        assert_eq!(stats.tasks_submitted, 2);
        assert_eq!(stats.current_queue_len, 2);
    }

    #[test]
    fn test_executor_stats_snapshot() {
        let stats = ExecutorStatsSnapshot {
            tasks_submitted: 100,
            tasks_completed: 90,
            tasks_rejected: 10,
            task_panics: 0,
            batches_processed: 10,
            task_time_ns: 9_000_000, // 9ms
            idle_time_ns: 1_000_000, // 1ms
            current_queue_len: 5,
        };

        assert_eq!(stats.avg_task_time_us(), 100.0); // 9ms / 90 = 100us
        assert_eq!(stats.utilization(), 90.0); // 9ms / 10ms = 90%
    }
}
