//! Usage Tracking
//!
//! Provides usage tracking and metering for multi-tenant deployments.

use chrono::{Datelike, Timelike};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

/// Usage metric type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UsageMetric {
    /// Messages produced
    MessagesProduced,
    /// Messages consumed
    MessagesConsumed,
    /// Bytes produced
    BytesProduced,
    /// Bytes consumed
    BytesConsumed,
    /// API requests
    ApiRequests,
    /// Active connections
    ActiveConnections,
    /// Topics created
    TopicsCreated,
    /// Partitions created
    PartitionsCreated,
    /// Storage used (bytes)
    StorageUsed,
    /// Network egress (bytes)
    NetworkEgress,
    /// Network ingress (bytes)
    NetworkIngress,
    /// Compute time (milliseconds)
    ComputeTime,
    /// Custom metric
    Custom(u32),
}

impl UsageMetric {
    /// Get metric name
    pub fn name(&self) -> &'static str {
        match self {
            UsageMetric::MessagesProduced => "messages_produced",
            UsageMetric::MessagesConsumed => "messages_consumed",
            UsageMetric::BytesProduced => "bytes_produced",
            UsageMetric::BytesConsumed => "bytes_consumed",
            UsageMetric::ApiRequests => "api_requests",
            UsageMetric::ActiveConnections => "active_connections",
            UsageMetric::TopicsCreated => "topics_created",
            UsageMetric::PartitionsCreated => "partitions_created",
            UsageMetric::StorageUsed => "storage_used",
            UsageMetric::NetworkEgress => "network_egress",
            UsageMetric::NetworkIngress => "network_ingress",
            UsageMetric::ComputeTime => "compute_time",
            UsageMetric::Custom(_) => "custom",
        }
    }

    /// Get unit for metric
    pub fn unit(&self) -> &'static str {
        match self {
            UsageMetric::MessagesProduced | UsageMetric::MessagesConsumed => "count",
            UsageMetric::BytesProduced
            | UsageMetric::BytesConsumed
            | UsageMetric::StorageUsed
            | UsageMetric::NetworkEgress
            | UsageMetric::NetworkIngress => "bytes",
            UsageMetric::ApiRequests
            | UsageMetric::TopicsCreated
            | UsageMetric::PartitionsCreated
            | UsageMetric::ActiveConnections => "count",
            UsageMetric::ComputeTime => "ms",
            UsageMetric::Custom(_) => "unknown",
        }
    }

    /// Is this a cumulative metric (vs point-in-time)?
    pub fn is_cumulative(&self) -> bool {
        !matches!(
            self,
            UsageMetric::ActiveConnections | UsageMetric::StorageUsed
        )
    }
}

/// Billing period
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BillingPeriod {
    /// Current hour
    CurrentHour,
    /// Current day
    CurrentDay,
    /// Current week
    CurrentWeek,
    /// Current month
    CurrentMonth,
    /// Custom period (start_ts, end_ts)
    Custom(i64, i64),
}

impl BillingPeriod {
    /// Get start and end timestamps for this period
    pub fn timestamps(&self) -> (i64, i64) {
        let now = chrono::Utc::now();

        match self {
            BillingPeriod::CurrentHour => {
                let start = now
                    .date_naive()
                    .and_hms_opt(now.hour(), 0, 0)
                    .map(|dt| dt.and_utc().timestamp_millis())
                    .unwrap_or(0);
                let end = now.timestamp_millis();
                (start, end)
            }
            BillingPeriod::CurrentDay => {
                let start = now
                    .date_naive()
                    .and_hms_opt(0, 0, 0)
                    .map(|dt| dt.and_utc().timestamp_millis())
                    .unwrap_or(0);
                let end = now.timestamp_millis();
                (start, end)
            }
            BillingPeriod::CurrentWeek => {
                let days_since_monday = now.weekday().num_days_from_monday() as i64;
                let start = (now - chrono::Duration::days(days_since_monday))
                    .date_naive()
                    .and_hms_opt(0, 0, 0)
                    .map(|dt| dt.and_utc().timestamp_millis())
                    .unwrap_or(0);
                let end = now.timestamp_millis();
                (start, end)
            }
            BillingPeriod::CurrentMonth => {
                let start = now
                    .date_naive()
                    .with_day(1)
                    .and_then(|d| d.and_hms_opt(0, 0, 0))
                    .map(|dt| dt.and_utc().timestamp_millis())
                    .unwrap_or(0);
                let end = now.timestamp_millis();
                (start, end)
            }
            BillingPeriod::Custom(start, end) => (*start, *end),
        }
    }
}

/// Single usage record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageRecord {
    /// Tenant ID
    pub tenant_id: String,
    /// Metric type
    pub metric: UsageMetric,
    /// Value
    pub value: u64,
    /// Timestamp
    pub timestamp: i64,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl UsageRecord {
    /// Create a new usage record
    pub fn new(tenant_id: impl Into<String>, metric: UsageMetric, value: u64) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            metric,
            value,
            timestamp: chrono::Utc::now().timestamp_millis(),
            metadata: HashMap::new(),
        }
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Aggregated usage for a period
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UsageAggregate {
    /// Total value
    pub total: u64,
    /// Maximum value
    pub max: u64,
    /// Minimum value
    pub min: u64,
    /// Average value
    pub avg: f64,
    /// Count of records
    pub count: u64,
    /// First timestamp
    pub first_ts: i64,
    /// Last timestamp
    pub last_ts: i64,
}

impl UsageAggregate {
    /// Create from records
    pub fn from_records(records: &[UsageRecord]) -> Self {
        if records.is_empty() {
            return Self::default();
        }

        let total: u64 = records.iter().map(|r| r.value).sum();
        let max = records.iter().map(|r| r.value).max().unwrap_or(0);
        let min = records.iter().map(|r| r.value).min().unwrap_or(0);
        let count = records.len() as u64;
        let avg = if count > 0 {
            total as f64 / count as f64
        } else {
            0.0
        };
        let first_ts = records.iter().map(|r| r.timestamp).min().unwrap_or(0);
        let last_ts = records.iter().map(|r| r.timestamp).max().unwrap_or(0);

        Self {
            total,
            max,
            min,
            avg,
            count,
            first_ts,
            last_ts,
        }
    }
}

/// Tenant usage summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageSummary {
    /// Tenant ID
    pub tenant_id: String,
    /// Period
    pub period: BillingPeriod,
    /// Usage by metric
    pub metrics: HashMap<UsageMetric, UsageAggregate>,
    /// Generated at
    pub generated_at: i64,
}

impl UsageSummary {
    /// Create a new summary
    pub fn new(tenant_id: impl Into<String>, period: BillingPeriod) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            period,
            metrics: HashMap::new(),
            generated_at: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Add metric aggregate
    pub fn add_metric(&mut self, metric: UsageMetric, aggregate: UsageAggregate) {
        self.metrics.insert(metric, aggregate);
    }

    /// Get total for a metric
    pub fn total(&self, metric: UsageMetric) -> u64 {
        self.metrics.get(&metric).map(|a| a.total).unwrap_or(0)
    }
}

/// Usage tracker for a tenant
#[derive(Debug)]
struct TenantUsage {
    /// Recent records (ring buffer)
    records: VecDeque<UsageRecord>,
    /// Current period totals by metric
    current_totals: HashMap<UsageMetric, u64>,
    /// Maximum records to keep
    max_records: usize,
}

impl TenantUsage {
    fn new(max_records: usize) -> Self {
        Self {
            records: VecDeque::new(),
            current_totals: HashMap::new(),
            max_records,
        }
    }

    fn add_record(&mut self, record: UsageRecord) {
        let metric = record.metric;
        let value = record.value;

        // Update totals
        if metric.is_cumulative() {
            *self.current_totals.entry(metric).or_default() += value;
        } else {
            self.current_totals.insert(metric, value);
        }

        // Add to records
        self.records.push_back(record);

        // Trim if needed
        while self.records.len() > self.max_records {
            self.records.pop_front();
        }
    }

    fn get_records(&self, period: BillingPeriod) -> Vec<UsageRecord> {
        let (start, end) = period.timestamps();

        self.records
            .iter()
            .filter(|r| r.timestamp >= start && r.timestamp <= end)
            .cloned()
            .collect()
    }

    fn get_records_by_metric(
        &self,
        metric: UsageMetric,
        period: BillingPeriod,
    ) -> Vec<UsageRecord> {
        let (start, end) = period.timestamps();

        self.records
            .iter()
            .filter(|r| r.metric == metric && r.timestamp >= start && r.timestamp <= end)
            .cloned()
            .collect()
    }

    fn current_total(&self, metric: UsageMetric) -> u64 {
        self.current_totals.get(&metric).copied().unwrap_or(0)
    }

    fn reset_totals(&mut self) {
        self.current_totals.clear();
    }
}

/// Usage tracker
pub struct UsageTracker {
    /// Usage by tenant
    tenants: HashMap<String, TenantUsage>,
    /// Maximum records per tenant
    max_records_per_tenant: usize,
    /// Aggregation interval (reserved for future use)
    _aggregation_interval_ms: i64,
}

impl UsageTracker {
    /// Create a new usage tracker
    pub fn new() -> Self {
        Self {
            tenants: HashMap::new(),
            max_records_per_tenant: 10000,
            _aggregation_interval_ms: 60000, // 1 minute
        }
    }

    /// Initialize tracking for a tenant
    pub fn init_tenant(&mut self, tenant_id: &str) {
        self.tenants.insert(
            tenant_id.to_string(),
            TenantUsage::new(self.max_records_per_tenant),
        );
    }

    /// Remove tracking for a tenant
    pub fn remove_tenant(&mut self, tenant_id: &str) {
        self.tenants.remove(tenant_id);
    }

    /// Record usage
    pub fn record(&mut self, tenant_id: &str, metric: UsageMetric, value: u64) {
        let usage = self
            .tenants
            .entry(tenant_id.to_string())
            .or_insert_with(|| TenantUsage::new(self.max_records_per_tenant));

        let record = UsageRecord::new(tenant_id, metric, value);
        usage.add_record(record);
    }

    /// Record with metadata
    pub fn record_with_metadata(
        &mut self,
        tenant_id: &str,
        metric: UsageMetric,
        value: u64,
        metadata: HashMap<String, String>,
    ) {
        let usage = self
            .tenants
            .entry(tenant_id.to_string())
            .or_insert_with(|| TenantUsage::new(self.max_records_per_tenant));

        let mut record = UsageRecord::new(tenant_id, metric, value);
        record.metadata = metadata;
        usage.add_record(record);
    }

    /// Get usage records for a tenant
    pub fn get_usage(&self, tenant_id: &str, period: BillingPeriod) -> Option<Vec<UsageRecord>> {
        self.tenants.get(tenant_id).map(|u| u.get_records(period))
    }

    /// Get usage for a specific metric
    pub fn get_metric_usage(
        &self,
        tenant_id: &str,
        metric: UsageMetric,
        period: BillingPeriod,
    ) -> Option<Vec<UsageRecord>> {
        self.tenants
            .get(tenant_id)
            .map(|u| u.get_records_by_metric(metric, period))
    }

    /// Get current total for a metric
    pub fn current_total(&self, tenant_id: &str, metric: UsageMetric) -> u64 {
        self.tenants
            .get(tenant_id)
            .map(|u| u.current_total(metric))
            .unwrap_or(0)
    }

    /// Get usage summary for a tenant
    pub fn get_summary(&self, tenant_id: &str, period: BillingPeriod) -> Option<UsageSummary> {
        let usage = self.tenants.get(tenant_id)?;

        let mut summary = UsageSummary::new(tenant_id, period);

        // Group records by metric
        let records = usage.get_records(period);
        let mut by_metric: HashMap<UsageMetric, Vec<UsageRecord>> = HashMap::new();

        for record in records {
            by_metric.entry(record.metric).or_default().push(record);
        }

        // Create aggregates
        for (metric, records) in by_metric {
            let aggregate = UsageAggregate::from_records(&records);
            summary.add_metric(metric, aggregate);
        }

        Some(summary)
    }

    /// Reset all period totals
    pub fn reset_all_totals(&mut self) {
        for usage in self.tenants.values_mut() {
            usage.reset_totals();
        }
    }

    /// Get tenant count
    pub fn tenant_count(&self) -> usize {
        self.tenants.len()
    }

    /// Get total record count across all tenants
    pub fn total_records(&self) -> usize {
        self.tenants.values().map(|u| u.records.len()).sum()
    }
}

impl Default for UsageTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_usage_metric() {
        let metric = UsageMetric::MessagesProduced;
        assert_eq!(metric.name(), "messages_produced");
        assert_eq!(metric.unit(), "count");
        assert!(metric.is_cumulative());

        let storage = UsageMetric::StorageUsed;
        assert!(!storage.is_cumulative());
    }

    #[test]
    fn test_usage_record() {
        let record = UsageRecord::new("tenant1", UsageMetric::MessagesProduced, 100)
            .with_metadata("topic", "events");

        assert_eq!(record.tenant_id, "tenant1");
        assert_eq!(record.value, 100);
        assert_eq!(record.metadata.get("topic"), Some(&"events".to_string()));
    }

    #[test]
    fn test_usage_aggregate() {
        let records = vec![
            UsageRecord::new("t1", UsageMetric::MessagesProduced, 10),
            UsageRecord::new("t1", UsageMetric::MessagesProduced, 20),
            UsageRecord::new("t1", UsageMetric::MessagesProduced, 30),
        ];

        let aggregate = UsageAggregate::from_records(&records);

        assert_eq!(aggregate.total, 60);
        assert_eq!(aggregate.max, 30);
        assert_eq!(aggregate.min, 10);
        assert_eq!(aggregate.count, 3);
        assert_eq!(aggregate.avg, 20.0);
    }

    #[test]
    fn test_usage_tracker() {
        let mut tracker = UsageTracker::new();

        tracker.init_tenant("tenant1");

        tracker.record("tenant1", UsageMetric::MessagesProduced, 100);
        tracker.record("tenant1", UsageMetric::MessagesProduced, 200);
        tracker.record("tenant1", UsageMetric::BytesProduced, 1024);

        assert_eq!(
            tracker.current_total("tenant1", UsageMetric::MessagesProduced),
            300
        );
        assert_eq!(
            tracker.current_total("tenant1", UsageMetric::BytesProduced),
            1024
        );
    }

    #[test]
    fn test_usage_tracker_get_usage() {
        let mut tracker = UsageTracker::new();

        tracker.init_tenant("tenant1");
        tracker.record("tenant1", UsageMetric::MessagesProduced, 100);
        tracker.record("tenant1", UsageMetric::MessagesProduced, 200);

        let records = tracker
            .get_usage("tenant1", BillingPeriod::CurrentDay)
            .unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_usage_summary() {
        let mut tracker = UsageTracker::new();

        tracker.init_tenant("tenant1");
        tracker.record("tenant1", UsageMetric::MessagesProduced, 100);
        tracker.record("tenant1", UsageMetric::MessagesProduced, 200);
        tracker.record("tenant1", UsageMetric::BytesProduced, 1024);

        let summary = tracker
            .get_summary("tenant1", BillingPeriod::CurrentDay)
            .unwrap();

        assert_eq!(summary.total(UsageMetric::MessagesProduced), 300);
        assert_eq!(summary.total(UsageMetric::BytesProduced), 1024);
    }

    #[test]
    fn test_billing_period_timestamps() {
        let (start, end) = BillingPeriod::CurrentDay.timestamps();
        assert!(start < end);
        assert!(end <= chrono::Utc::now().timestamp_millis());
    }

    #[test]
    fn test_usage_tracker_remove_tenant() {
        let mut tracker = UsageTracker::new();

        tracker.init_tenant("tenant1");
        tracker.record("tenant1", UsageMetric::MessagesProduced, 100);

        assert_eq!(tracker.tenant_count(), 1);

        tracker.remove_tenant("tenant1");
        assert_eq!(tracker.tenant_count(), 0);
    }

    #[test]
    fn test_non_cumulative_metric() {
        let mut tracker = UsageTracker::new();

        tracker.init_tenant("tenant1");
        tracker.record("tenant1", UsageMetric::ActiveConnections, 10);
        tracker.record("tenant1", UsageMetric::ActiveConnections, 15);

        // Non-cumulative metrics replace rather than sum
        assert_eq!(
            tracker.current_total("tenant1", UsageMetric::ActiveConnections),
            15
        );
    }
}
