//! Resource quota management for tenants

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Quota configuration for rate limiting
#[derive(Debug, Clone)]
pub struct QuotaConfig {
    /// Window size for rate calculations
    pub window_size: Duration,
    /// Whether to enforce quotas
    pub enforce: bool,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            window_size: Duration::from_secs(1),
            enforce: true,
        }
    }
}

/// Current resource usage for a tenant
#[derive(Debug)]
pub struct ResourceUsage {
    /// Current topic count
    pub topic_count: AtomicU64,
    /// Current partition count
    pub partition_count: AtomicU64,
    /// Current storage bytes
    pub storage_bytes: AtomicU64,
    /// Producer bytes in current window
    pub producer_bytes_window: AtomicU64,
    /// Consumer bytes in current window
    pub consumer_bytes_window: AtomicU64,
    /// Window start time
    pub window_start: std::sync::Mutex<Instant>,
}

impl Default for ResourceUsage {
    fn default() -> Self {
        Self {
            topic_count: AtomicU64::new(0),
            partition_count: AtomicU64::new(0),
            storage_bytes: AtomicU64::new(0),
            producer_bytes_window: AtomicU64::new(0),
            consumer_bytes_window: AtomicU64::new(0),
            window_start: std::sync::Mutex::new(Instant::now()),
        }
    }
}

impl ResourceUsage {
    /// Reset rate window if expired
    fn maybe_reset_window(&self, window_size: Duration) {
        let mut start = self.window_start.lock().unwrap_or_else(|e| e.into_inner());
        if start.elapsed() >= window_size {
            self.producer_bytes_window.store(0, Ordering::Relaxed);
            self.consumer_bytes_window.store(0, Ordering::Relaxed);
            *start = Instant::now();
        }
    }
}

/// Result of a quota check
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotaCheckResult {
    /// Within quota
    Allowed,
    /// Quota exceeded
    Exceeded {
        resource: String,
        limit: u64,
        current: u64,
    },
    /// Tenant not found
    TenantNotFound,
}

/// Manages resource quotas across all tenants
pub struct QuotaManager {
    /// Per-tenant resource usage
    usage: DashMap<String, Arc<ResourceUsage>>,
    /// Quota configuration
    config: QuotaConfig,
}

impl QuotaManager {
    /// Create a new quota manager
    pub fn new(config: QuotaConfig) -> Self {
        Self {
            usage: DashMap::new(),
            config,
        }
    }

    /// Register a new tenant
    pub fn register_tenant(&self, tenant_id: &str) {
        self.usage
            .entry(tenant_id.to_string())
            .or_insert_with(|| Arc::new(ResourceUsage::default()));
    }

    /// Remove a tenant
    pub fn remove_tenant(&self, tenant_id: &str) {
        self.usage.remove(tenant_id);
    }

    /// Check if a topic creation is within quota
    pub fn check_topic_creation(
        &self,
        tenant_id: &str,
        max_topics: u32,
        partitions: u32,
        max_partitions: u32,
    ) -> QuotaCheckResult {
        if !self.config.enforce {
            return QuotaCheckResult::Allowed;
        }

        match self.usage.get(tenant_id) {
            Some(usage) => {
                let current_topics = usage.topic_count.load(Ordering::Relaxed);
                if current_topics >= max_topics as u64 {
                    return QuotaCheckResult::Exceeded {
                        resource: "topics".to_string(),
                        limit: max_topics as u64,
                        current: current_topics,
                    };
                }

                let current_partitions = usage.partition_count.load(Ordering::Relaxed);
                if current_partitions + partitions as u64 > max_partitions as u64 {
                    return QuotaCheckResult::Exceeded {
                        resource: "partitions".to_string(),
                        limit: max_partitions as u64,
                        current: current_partitions,
                    };
                }

                QuotaCheckResult::Allowed
            }
            None => QuotaCheckResult::TenantNotFound,
        }
    }

    /// Check produce rate quota
    pub fn check_produce_rate(
        &self,
        tenant_id: &str,
        bytes: u64,
        max_bytes_per_sec: u64,
    ) -> QuotaCheckResult {
        if !self.config.enforce {
            return QuotaCheckResult::Allowed;
        }

        match self.usage.get(tenant_id) {
            Some(usage) => {
                usage.maybe_reset_window(self.config.window_size);

                let current = usage.producer_bytes_window.load(Ordering::Relaxed);
                if current + bytes > max_bytes_per_sec {
                    return QuotaCheckResult::Exceeded {
                        resource: "producer_bytes_per_sec".to_string(),
                        limit: max_bytes_per_sec,
                        current,
                    };
                }

                QuotaCheckResult::Allowed
            }
            None => QuotaCheckResult::TenantNotFound,
        }
    }

    /// Check consume rate quota
    pub fn check_consume_rate(
        &self,
        tenant_id: &str,
        bytes: u64,
        max_bytes_per_sec: u64,
    ) -> QuotaCheckResult {
        if !self.config.enforce {
            return QuotaCheckResult::Allowed;
        }

        match self.usage.get(tenant_id) {
            Some(usage) => {
                usage.maybe_reset_window(self.config.window_size);

                let current = usage.consumer_bytes_window.load(Ordering::Relaxed);
                if current + bytes > max_bytes_per_sec {
                    return QuotaCheckResult::Exceeded {
                        resource: "consumer_bytes_per_sec".to_string(),
                        limit: max_bytes_per_sec,
                        current,
                    };
                }

                QuotaCheckResult::Allowed
            }
            None => QuotaCheckResult::TenantNotFound,
        }
    }

    /// Check storage quota
    pub fn check_storage(
        &self,
        tenant_id: &str,
        additional_bytes: u64,
        max_storage_bytes: u64,
    ) -> QuotaCheckResult {
        if !self.config.enforce {
            return QuotaCheckResult::Allowed;
        }

        match self.usage.get(tenant_id) {
            Some(usage) => {
                let current = usage.storage_bytes.load(Ordering::Relaxed);
                if current + additional_bytes > max_storage_bytes {
                    return QuotaCheckResult::Exceeded {
                        resource: "storage_bytes".to_string(),
                        limit: max_storage_bytes,
                        current,
                    };
                }

                QuotaCheckResult::Allowed
            }
            None => QuotaCheckResult::TenantNotFound,
        }
    }

    /// Record produced bytes
    pub fn record_produce(&self, tenant_id: &str, bytes: u64) {
        if let Some(usage) = self.usage.get(tenant_id) {
            usage
                .producer_bytes_window
                .fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Record consumed bytes
    pub fn record_consume(&self, tenant_id: &str, bytes: u64) {
        if let Some(usage) = self.usage.get(tenant_id) {
            usage
                .consumer_bytes_window
                .fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Record topic creation
    pub fn record_topic_created(&self, tenant_id: &str, partitions: u32) {
        if let Some(usage) = self.usage.get(tenant_id) {
            usage.topic_count.fetch_add(1, Ordering::Relaxed);
            usage
                .partition_count
                .fetch_add(partitions as u64, Ordering::Relaxed);
        }
    }

    /// Record topic deletion
    pub fn record_topic_deleted(&self, tenant_id: &str, partitions: u32) {
        if let Some(usage) = self.usage.get(tenant_id) {
            usage.topic_count.fetch_sub(1, Ordering::Relaxed);
            usage
                .partition_count
                .fetch_sub(partitions as u64, Ordering::Relaxed);
        }
    }

    /// Get resource usage for a tenant
    pub fn get_usage(&self, tenant_id: &str) -> Option<Arc<ResourceUsage>> {
        self.usage.get(tenant_id).map(|e| e.value().clone())
    }

    /// Get number of tracked tenants
    pub fn tenant_count(&self) -> usize {
        self.usage.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quota_check_topic_creation_allowed() {
        let mgr = QuotaManager::new(QuotaConfig::default());
        mgr.register_tenant("t1");

        let result = mgr.check_topic_creation("t1", 100, 3, 500);
        assert_eq!(result, QuotaCheckResult::Allowed);
    }

    #[test]
    fn test_quota_check_topic_creation_exceeded() {
        let mgr = QuotaManager::new(QuotaConfig::default());
        mgr.register_tenant("t1");

        let usage = mgr.get_usage("t1").unwrap();
        usage.topic_count.store(100, Ordering::Relaxed);

        let result = mgr.check_topic_creation("t1", 100, 3, 500);
        assert!(matches!(result, QuotaCheckResult::Exceeded { .. }));
    }

    #[test]
    fn test_quota_check_produce_rate() {
        let mgr = QuotaManager::new(QuotaConfig::default());
        mgr.register_tenant("t1");

        let max_rate = 1024 * 1024; // 1 MB/s
        let result = mgr.check_produce_rate("t1", 512, max_rate);
        assert_eq!(result, QuotaCheckResult::Allowed);
    }

    #[test]
    fn test_quota_produce_rate_exceeded() {
        let mgr = QuotaManager::new(QuotaConfig::default());
        mgr.register_tenant("t1");

        let max_rate = 1000;
        mgr.record_produce("t1", 900);

        let result = mgr.check_produce_rate("t1", 200, max_rate);
        assert!(matches!(result, QuotaCheckResult::Exceeded { .. }));
    }

    #[test]
    fn test_quota_storage_check() {
        let mgr = QuotaManager::new(QuotaConfig::default());
        mgr.register_tenant("t1");

        let max_storage = 1024 * 1024;
        let result = mgr.check_storage("t1", 512, max_storage);
        assert_eq!(result, QuotaCheckResult::Allowed);
    }

    #[test]
    fn test_quota_tenant_not_found() {
        let mgr = QuotaManager::new(QuotaConfig::default());
        let result = mgr.check_topic_creation("unknown", 100, 3, 500);
        assert_eq!(result, QuotaCheckResult::TenantNotFound);
    }

    #[test]
    fn test_quota_enforcement_disabled() {
        let mgr = QuotaManager::new(QuotaConfig {
            enforce: false,
            ..Default::default()
        });
        let result = mgr.check_topic_creation("t1", 0, 100, 0);
        assert_eq!(result, QuotaCheckResult::Allowed);
    }

    #[test]
    fn test_record_topic_lifecycle() {
        let mgr = QuotaManager::new(QuotaConfig::default());
        mgr.register_tenant("t1");

        mgr.record_topic_created("t1", 3);
        let usage = mgr.get_usage("t1").unwrap();
        assert_eq!(usage.topic_count.load(Ordering::Relaxed), 1);
        assert_eq!(usage.partition_count.load(Ordering::Relaxed), 3);

        mgr.record_topic_deleted("t1", 3);
        assert_eq!(usage.topic_count.load(Ordering::Relaxed), 0);
        assert_eq!(usage.partition_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_remove_tenant() {
        let mgr = QuotaManager::new(QuotaConfig::default());
        mgr.register_tenant("t1");
        assert_eq!(mgr.tenant_count(), 1);

        mgr.remove_tenant("t1");
        assert_eq!(mgr.tenant_count(), 0);
    }

    #[test]
    fn test_consume_rate_tracking() {
        let mgr = QuotaManager::new(QuotaConfig::default());
        mgr.register_tenant("t1");

        mgr.record_consume("t1", 500);
        let usage = mgr.get_usage("t1").unwrap();
        assert_eq!(usage.consumer_bytes_window.load(Ordering::Relaxed), 500);
    }
}
