//! Data Transitions and Migrations
//!
//! Handles the actual movement of data between storage tiers.

use super::storage_tiers::StorageTier;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

/// Migration job status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationStatus {
    /// Job is queued
    Pending,
    /// Job is in progress
    InProgress,
    /// Job completed successfully
    Completed,
    /// Job failed
    Failed,
    /// Job was cancelled
    Cancelled,
}

/// Migration priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub enum MigrationPriority {
    /// Low priority (background)
    Low = 0,
    /// Normal priority
    #[default]
    Normal = 1,
    /// High priority
    High = 2,
    /// Critical (compliance/capacity)
    Critical = 3,
}

/// A migration job
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationJob {
    /// Job ID
    pub id: String,
    /// Segment ID to migrate
    pub segment_id: String,
    /// Source tier
    pub from_tier: StorageTier,
    /// Target tier
    pub to_tier: StorageTier,
    /// Reason for migration
    pub reason: String,
    /// Job priority
    pub priority: MigrationPriority,
    /// Job status
    pub status: MigrationStatus,
    /// Created timestamp
    pub created_at: i64,
    /// Started timestamp
    pub started_at: Option<i64>,
    /// Completed timestamp
    pub completed_at: Option<i64>,
    /// Bytes to transfer
    pub bytes_to_transfer: u64,
    /// Bytes transferred so far
    pub bytes_transferred: u64,
    /// Error message if failed
    pub error: Option<String>,
    /// Retry count
    pub retry_count: u32,
    /// Maximum retries
    pub max_retries: u32,
}

impl MigrationJob {
    /// Create a new migration job
    pub fn new(
        segment_id: impl Into<String>,
        from_tier: StorageTier,
        to_tier: StorageTier,
        reason: impl Into<String>,
    ) -> Self {
        let id = format!(
            "mig-{}-{}",
            chrono::Utc::now().timestamp_millis(),
            rand::random::<u32>() % 10000
        );

        Self {
            id,
            segment_id: segment_id.into(),
            from_tier,
            to_tier,
            reason: reason.into(),
            priority: MigrationPriority::Normal,
            status: MigrationStatus::Pending,
            created_at: chrono::Utc::now().timestamp_millis(),
            started_at: None,
            completed_at: None,
            bytes_to_transfer: 0,
            bytes_transferred: 0,
            error: None,
            retry_count: 0,
            max_retries: 3,
        }
    }

    /// Set priority
    pub fn with_priority(mut self, priority: MigrationPriority) -> Self {
        self.priority = priority;
        self
    }

    /// Set bytes to transfer
    pub fn with_size(mut self, bytes: u64) -> Self {
        self.bytes_to_transfer = bytes;
        self
    }

    /// Get progress percentage
    pub fn progress(&self) -> f64 {
        if self.bytes_to_transfer == 0 {
            0.0
        } else {
            (self.bytes_transferred as f64 / self.bytes_to_transfer as f64) * 100.0
        }
    }

    /// Mark as in progress
    pub fn start(&mut self) {
        self.status = MigrationStatus::InProgress;
        self.started_at = Some(chrono::Utc::now().timestamp_millis());
    }

    /// Mark as completed
    pub fn complete(&mut self) {
        self.status = MigrationStatus::Completed;
        self.completed_at = Some(chrono::Utc::now().timestamp_millis());
        self.bytes_transferred = self.bytes_to_transfer;
    }

    /// Mark as failed
    pub fn fail(&mut self, error: impl Into<String>) {
        self.error = Some(error.into());
        self.retry_count += 1;

        if self.retry_count >= self.max_retries {
            self.status = MigrationStatus::Failed;
            self.completed_at = Some(chrono::Utc::now().timestamp_millis());
        } else {
            self.status = MigrationStatus::Pending; // Retry
        }
    }

    /// Cancel the job
    pub fn cancel(&mut self) {
        self.status = MigrationStatus::Cancelled;
        self.completed_at = Some(chrono::Utc::now().timestamp_millis());
    }

    /// Can this job be retried?
    pub fn can_retry(&self) -> bool {
        self.retry_count < self.max_retries
    }

    /// Get duration in milliseconds
    pub fn duration_ms(&self) -> Option<i64> {
        match (self.started_at, self.completed_at) {
            (Some(start), Some(end)) => Some(end - start),
            (Some(start), None) => Some(chrono::Utc::now().timestamp_millis() - start),
            _ => None,
        }
    }
}

/// Migration statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MigrationStats {
    /// Total jobs submitted
    pub total_jobs: u64,
    /// Jobs completed successfully
    pub completed_jobs: u64,
    /// Jobs failed
    pub failed_jobs: u64,
    /// Jobs cancelled
    pub cancelled_jobs: u64,
    /// Jobs currently in progress
    pub in_progress_jobs: u64,
    /// Jobs pending
    pub pending_jobs: u64,
    /// Total bytes migrated
    pub bytes_migrated: u64,
    /// Average migration time (ms)
    pub avg_migration_time_ms: u64,
    /// Migrations by tier transition
    pub migrations_by_transition: HashMap<String, u64>,
}

impl MigrationStats {
    /// Record a completed migration
    pub fn record_completion(&mut self, job: &MigrationJob) {
        self.completed_jobs += 1;
        self.bytes_migrated += job.bytes_transferred;

        // Update average time
        if let Some(duration) = job.duration_ms() {
            let total_time =
                self.avg_migration_time_ms * (self.completed_jobs - 1) + duration as u64;
            self.avg_migration_time_ms = total_time / self.completed_jobs;
        }

        // Record transition
        let key = format!("{:?}->{:?}", job.from_tier, job.to_tier);
        *self.migrations_by_transition.entry(key).or_default() += 1;
    }

    /// Record a failed migration
    pub fn record_failure(&mut self) {
        self.failed_jobs += 1;
    }
}

/// Transition rule for automatic tier migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitionRule {
    /// Rule name
    pub name: String,
    /// Source tier
    pub from_tier: StorageTier,
    /// Target tier
    pub to_tier: StorageTier,
    /// Minimum age in seconds before transition
    pub min_age_seconds: u64,
    /// Minimum idle time (since last access) in seconds
    pub min_idle_seconds: u64,
    /// Maximum access count to be eligible
    pub max_access_count: Option<u64>,
    /// Priority for migrations triggered by this rule
    pub priority: MigrationPriority,
    /// Is this rule enabled?
    pub enabled: bool,
}

impl TransitionRule {
    /// Create a new transition rule
    pub fn new(name: impl Into<String>, from_tier: StorageTier, to_tier: StorageTier) -> Self {
        Self {
            name: name.into(),
            from_tier,
            to_tier,
            min_age_seconds: 0,
            min_idle_seconds: 0,
            max_access_count: None,
            priority: MigrationPriority::Normal,
            enabled: true,
        }
    }

    /// Set minimum age
    pub fn with_min_age(mut self, seconds: u64) -> Self {
        self.min_age_seconds = seconds;
        self
    }

    /// Set minimum idle time
    pub fn with_min_idle(mut self, seconds: u64) -> Self {
        self.min_idle_seconds = seconds;
        self
    }

    /// Set maximum access count
    pub fn with_max_access_count(mut self, count: u64) -> Self {
        self.max_access_count = Some(count);
        self
    }

    /// Set priority
    pub fn with_priority(mut self, priority: MigrationPriority) -> Self {
        self.priority = priority;
        self
    }
}

/// Transition plan - a set of rules for automatic tier transitions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitionPlan {
    /// Plan name
    pub name: String,
    /// Transition rules
    pub rules: Vec<TransitionRule>,
}

impl TransitionPlan {
    /// Create a new transition plan
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            rules: Vec::new(),
        }
    }

    /// Add a rule
    pub fn add_rule(mut self, rule: TransitionRule) -> Self {
        self.rules.push(rule);
        self
    }

    /// Create a default transition plan
    pub fn default_plan() -> Self {
        Self::new("default")
            // Memory -> SSD after 1 hour
            .add_rule(
                TransitionRule::new("memory-to-ssd", StorageTier::Memory, StorageTier::Ssd)
                    .with_min_age(3600)
                    .with_priority(MigrationPriority::High),
            )
            // SSD -> HDD after 1 day, idle for 12 hours
            .add_rule(
                TransitionRule::new("ssd-to-hdd", StorageTier::Ssd, StorageTier::Hdd)
                    .with_min_age(86400)
                    .with_min_idle(43200),
            )
            // HDD -> Object Storage after 30 days, idle for 7 days
            .add_rule(
                TransitionRule::new(
                    "hdd-to-object",
                    StorageTier::Hdd,
                    StorageTier::ObjectStorage,
                )
                .with_min_age(30 * 86400)
                .with_min_idle(7 * 86400),
            )
            // Object Storage -> Cold Archive after 1 year
            .add_rule(
                TransitionRule::new(
                    "object-to-cold",
                    StorageTier::ObjectStorage,
                    StorageTier::ColdArchive,
                )
                .with_min_age(365 * 86400)
                .with_priority(MigrationPriority::Low),
            )
    }

    /// Create an aggressive cost-saving plan
    pub fn aggressive_cost_saving() -> Self {
        Self::new("aggressive-cost")
            // Memory -> SSD after 30 minutes
            .add_rule(
                TransitionRule::new("memory-to-ssd", StorageTier::Memory, StorageTier::Ssd)
                    .with_min_age(1800),
            )
            // SSD -> HDD after 6 hours
            .add_rule(
                TransitionRule::new("ssd-to-hdd", StorageTier::Ssd, StorageTier::Hdd)
                    .with_min_age(21600),
            )
            // HDD -> Object Storage after 7 days
            .add_rule(
                TransitionRule::new(
                    "hdd-to-object",
                    StorageTier::Hdd,
                    StorageTier::ObjectStorage,
                )
                .with_min_age(7 * 86400),
            )
    }

    /// Create a performance-focused plan
    pub fn performance_focused() -> Self {
        Self::new("performance")
            // Memory -> SSD after 1 day
            .add_rule(
                TransitionRule::new("memory-to-ssd", StorageTier::Memory, StorageTier::Ssd)
                    .with_min_age(86400),
            )
            // SSD -> HDD after 7 days, very idle
            .add_rule(
                TransitionRule::new("ssd-to-hdd", StorageTier::Ssd, StorageTier::Hdd)
                    .with_min_age(7 * 86400)
                    .with_min_idle(3 * 86400)
                    .with_max_access_count(10),
            )
    }
}

/// Migration manager for handling migration jobs
pub struct MigrationManager {
    /// Pending job queue (sorted by priority)
    pending_queue: VecDeque<MigrationJob>,
    /// Jobs currently in progress
    in_progress: HashMap<String, MigrationJob>,
    /// Completed jobs (recent history)
    completed: VecDeque<MigrationJob>,
    /// Maximum concurrent migrations
    max_concurrent: usize,
    /// Statistics
    stats: MigrationStats,
    /// Maximum history size
    max_history: usize,
}

impl MigrationManager {
    /// Create a new migration manager
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            pending_queue: VecDeque::new(),
            in_progress: HashMap::new(),
            completed: VecDeque::new(),
            max_concurrent,
            stats: MigrationStats::default(),
            max_history: 1000,
        }
    }

    /// Submit a new migration job
    pub fn submit(&mut self, job: MigrationJob) -> String {
        let id = job.id.clone();
        self.stats.total_jobs += 1;
        self.stats.pending_jobs += 1;

        // Insert maintaining priority order
        let pos = self
            .pending_queue
            .iter()
            .position(|j| j.priority < job.priority)
            .unwrap_or(self.pending_queue.len());
        self.pending_queue.insert(pos, job);

        id
    }

    /// Get the next job to process
    pub fn next_job(&mut self) -> Option<MigrationJob> {
        if self.in_progress.len() >= self.max_concurrent {
            return None;
        }

        if let Some(mut job) = self.pending_queue.pop_front() {
            job.start();
            self.stats.pending_jobs -= 1;
            self.stats.in_progress_jobs += 1;

            let id = job.id.clone();
            self.in_progress.insert(id, job.clone());
            Some(job)
        } else {
            None
        }
    }

    /// Mark a job as completed
    pub fn complete_job(&mut self, job_id: &str) {
        if let Some(mut job) = self.in_progress.remove(job_id) {
            job.complete();
            self.stats.in_progress_jobs -= 1;
            self.stats.record_completion(&job);

            self.add_to_history(job);
        }
    }

    /// Mark a job as failed
    pub fn fail_job(&mut self, job_id: &str, error: impl Into<String>) {
        if let Some(mut job) = self.in_progress.remove(job_id) {
            job.fail(error);
            self.stats.in_progress_jobs -= 1;

            if job.status == MigrationStatus::Failed {
                self.stats.record_failure();
                self.add_to_history(job);
            } else {
                // Re-queue for retry
                self.stats.pending_jobs += 1;
                self.pending_queue.push_back(job);
            }
        }
    }

    /// Cancel a job
    pub fn cancel_job(&mut self, job_id: &str) -> bool {
        // Check pending queue
        if let Some(pos) = self.pending_queue.iter().position(|j| j.id == job_id) {
            let Some(mut job) = self.pending_queue.remove(pos) else {
                return false;
            };
            job.cancel();
            self.stats.pending_jobs -= 1;
            self.stats.cancelled_jobs += 1;
            self.add_to_history(job);
            return true;
        }

        // Check in-progress
        if let Some(mut job) = self.in_progress.remove(job_id) {
            job.cancel();
            self.stats.in_progress_jobs -= 1;
            self.stats.cancelled_jobs += 1;
            self.add_to_history(job);
            return true;
        }

        false
    }

    /// Update job progress
    pub fn update_progress(&mut self, job_id: &str, bytes_transferred: u64) {
        if let Some(job) = self.in_progress.get_mut(job_id) {
            job.bytes_transferred = bytes_transferred;
        }
    }

    /// Get job by ID
    pub fn get_job(&self, job_id: &str) -> Option<&MigrationJob> {
        self.in_progress
            .get(job_id)
            .or_else(|| self.pending_queue.iter().find(|j| j.id == job_id))
            .or_else(|| self.completed.iter().find(|j| j.id == job_id))
    }

    /// Get all pending jobs
    pub fn pending_jobs(&self) -> impl Iterator<Item = &MigrationJob> {
        self.pending_queue.iter()
    }

    /// Get all in-progress jobs
    pub fn in_progress_jobs(&self) -> impl Iterator<Item = &MigrationJob> {
        self.in_progress.values()
    }

    /// Get statistics
    pub fn stats(&self) -> &MigrationStats {
        &self.stats
    }

    /// Add job to history
    fn add_to_history(&mut self, job: MigrationJob) {
        self.completed.push_back(job);
        while self.completed.len() > self.max_history {
            self.completed.pop_front();
        }
    }

    /// Get queue depth
    pub fn queue_depth(&self) -> usize {
        self.pending_queue.len()
    }

    /// Get current concurrent migrations
    pub fn concurrent_migrations(&self) -> usize {
        self.in_progress.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_job_creation() {
        let job = MigrationJob::new(
            "seg-001",
            StorageTier::Memory,
            StorageTier::Ssd,
            "Age-based migration",
        );

        assert_eq!(job.segment_id, "seg-001");
        assert_eq!(job.from_tier, StorageTier::Memory);
        assert_eq!(job.to_tier, StorageTier::Ssd);
        assert_eq!(job.status, MigrationStatus::Pending);
        assert_eq!(job.priority, MigrationPriority::Normal);
    }

    #[test]
    fn test_migration_job_lifecycle() {
        let mut job = MigrationJob::new("seg-001", StorageTier::Ssd, StorageTier::Hdd, "Test")
            .with_size(1024 * 1024);

        assert_eq!(job.progress(), 0.0);

        job.start();
        assert_eq!(job.status, MigrationStatus::InProgress);
        assert!(job.started_at.is_some());

        job.bytes_transferred = 512 * 1024;
        assert_eq!(job.progress(), 50.0);

        job.complete();
        assert_eq!(job.status, MigrationStatus::Completed);
        assert!(job.completed_at.is_some());
        assert_eq!(job.progress(), 100.0);
    }

    #[test]
    fn test_migration_job_failure_and_retry() {
        let mut job = MigrationJob::new(
            "seg-001",
            StorageTier::Ssd,
            StorageTier::ObjectStorage,
            "Test",
        );

        job.start();
        job.fail("Network error");

        assert_eq!(job.status, MigrationStatus::Pending); // Should retry
        assert_eq!(job.retry_count, 1);
        assert!(job.can_retry());

        // Fail again
        job.fail("Network error");
        job.fail("Network error");

        assert_eq!(job.status, MigrationStatus::Failed); // Max retries reached
        assert!(!job.can_retry());
    }

    #[test]
    fn test_migration_priority() {
        assert!(MigrationPriority::Critical > MigrationPriority::High);
        assert!(MigrationPriority::High > MigrationPriority::Normal);
        assert!(MigrationPriority::Normal > MigrationPriority::Low);
    }

    #[test]
    fn test_transition_rule() {
        let rule = TransitionRule::new("test", StorageTier::Memory, StorageTier::Ssd)
            .with_min_age(3600)
            .with_min_idle(1800)
            .with_max_access_count(100);

        assert_eq!(rule.min_age_seconds, 3600);
        assert_eq!(rule.min_idle_seconds, 1800);
        assert_eq!(rule.max_access_count, Some(100));
    }

    #[test]
    fn test_transition_plan_default() {
        let plan = TransitionPlan::default_plan();

        assert_eq!(plan.name, "default");
        assert!(!plan.rules.is_empty());

        // Should have memory->ssd rule
        let memory_to_ssd = plan
            .rules
            .iter()
            .find(|r| r.from_tier == StorageTier::Memory && r.to_tier == StorageTier::Ssd);
        assert!(memory_to_ssd.is_some());
    }

    #[test]
    fn test_migration_manager_submit() {
        let mut manager = MigrationManager::new(4);

        let job1 = MigrationJob::new("seg-001", StorageTier::Memory, StorageTier::Ssd, "Test");
        let job2 = MigrationJob::new("seg-002", StorageTier::Ssd, StorageTier::Hdd, "Test")
            .with_priority(MigrationPriority::High);

        manager.submit(job1);
        manager.submit(job2);

        // High priority should be first
        let next = manager.next_job().unwrap();
        assert_eq!(next.segment_id, "seg-002");
    }

    #[test]
    fn test_migration_manager_concurrency_limit() {
        let mut manager = MigrationManager::new(2);

        // Submit 3 jobs
        for i in 0..3 {
            let job = MigrationJob::new(
                format!("seg-{:03}", i),
                StorageTier::Memory,
                StorageTier::Ssd,
                "Test",
            );
            manager.submit(job);
        }

        // Should only get 2 jobs (concurrency limit)
        assert!(manager.next_job().is_some());
        assert!(manager.next_job().is_some());
        assert!(manager.next_job().is_none());

        assert_eq!(manager.concurrent_migrations(), 2);
        assert_eq!(manager.queue_depth(), 1);
    }

    #[test]
    fn test_migration_manager_complete() {
        let mut manager = MigrationManager::new(4);

        let job = MigrationJob::new("seg-001", StorageTier::Memory, StorageTier::Ssd, "Test")
            .with_size(1024);
        let id = manager.submit(job);

        let _ = manager.next_job();
        manager.complete_job(&id);

        assert_eq!(manager.concurrent_migrations(), 0);
        assert_eq!(manager.stats().completed_jobs, 1);
    }

    #[test]
    fn test_migration_manager_cancel() {
        let mut manager = MigrationManager::new(4);

        let job = MigrationJob::new("seg-001", StorageTier::Memory, StorageTier::Ssd, "Test");
        let id = manager.submit(job);

        assert!(manager.cancel_job(&id));
        assert_eq!(manager.stats().cancelled_jobs, 1);
    }
}
