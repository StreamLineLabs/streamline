//! Usage metering for Streamline Cloud
//!
//! Tracks per-tenant resource consumption and provides aggregation
//! over configurable time windows (hourly, daily) for billing and
//! capacity planning.
//!
//! # Stability: Experimental

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A point-in-time snapshot of a tenant's resource usage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeteringRecord {
    /// Tenant this record belongs to
    pub tenant_id: String,
    /// Timestamp of the measurement
    pub timestamp: DateTime<Utc>,
    /// Bytes ingested (produced) during the measurement window
    pub bytes_in: u64,
    /// Bytes egressed (consumed) during the measurement window
    pub bytes_out: u64,
    /// Messages produced during the measurement window
    pub messages_in: u64,
    /// Messages consumed during the measurement window
    pub messages_out: u64,
    /// Total storage bytes in use at measurement time
    pub storage_bytes: u64,
    /// Number of active client connections at measurement time
    pub active_connections: u32,
}

/// Time window for usage aggregation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregationWindow {
    /// Aggregate per hour
    Hourly,
    /// Aggregate per day
    Daily,
}

/// Aggregated usage over a time window
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedUsage {
    /// Tenant ID
    pub tenant_id: String,
    /// Aggregation window
    pub window: AggregationWindow,
    /// Start of the window
    pub window_start: DateTime<Utc>,
    /// End of the window
    pub window_end: DateTime<Utc>,
    /// Total bytes ingested
    pub total_bytes_in: u64,
    /// Total bytes egressed
    pub total_bytes_out: u64,
    /// Total messages produced
    pub total_messages_in: u64,
    /// Total messages consumed
    pub total_messages_out: u64,
    /// Peak storage bytes observed
    pub peak_storage_bytes: u64,
    /// Peak active connections observed
    pub peak_active_connections: u32,
    /// Number of samples aggregated
    pub sample_count: usize,
}

/// Aggregates raw usage records into time-windowed summaries
pub struct UsageAggregator;

impl UsageAggregator {
    /// Aggregate a set of records into a single summary for the given window.
    pub fn aggregate(
        tenant_id: &str,
        records: &[MeteringRecord],
        window: AggregationWindow,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> AggregatedUsage {
        let in_window: Vec<&MeteringRecord> = records
            .iter()
            .filter(|r| r.timestamp >= window_start && r.timestamp < window_end)
            .collect();

        AggregatedUsage {
            tenant_id: tenant_id.to_string(),
            window,
            window_start,
            window_end,
            total_bytes_in: in_window.iter().map(|r| r.bytes_in).sum(),
            total_bytes_out: in_window.iter().map(|r| r.bytes_out).sum(),
            total_messages_in: in_window.iter().map(|r| r.messages_in).sum(),
            total_messages_out: in_window.iter().map(|r| r.messages_out).sum(),
            peak_storage_bytes: in_window.iter().map(|r| r.storage_bytes).max().unwrap_or(0),
            peak_active_connections: in_window
                .iter()
                .map(|r| r.active_connections)
                .max()
                .unwrap_or(0),
            sample_count: in_window.len(),
        }
    }
}

/// Service that records and queries per-tenant usage metrics
pub struct MeteringService {
    /// Raw records keyed by tenant ID
    records: Arc<RwLock<HashMap<String, Vec<MeteringRecord>>>>,
}

impl MeteringService {
    /// Create a new metering service
    pub fn new() -> Self {
        Self {
            records: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Record a usage measurement for a tenant
    pub async fn record_usage(&self, record: MeteringRecord) -> Result<()> {
        let mut records = self.records.write().await;
        records
            .entry(record.tenant_id.clone())
            .or_default()
            .push(record);
        Ok(())
    }

    /// Get a summary of a tenant's usage over a time range
    pub async fn get_usage_summary(
        &self,
        tenant_id: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<AggregatedUsage> {
        let records = self.records.read().await;
        let tenant_records = records.get(tenant_id).ok_or_else(|| {
            StreamlineError::Config(format!("No metering data for tenant: {}", tenant_id))
        })?;

        Ok(UsageAggregator::aggregate(
            tenant_id,
            tenant_records,
            AggregationWindow::Daily,
            from,
            to,
        ))
    }

    /// Export a usage report as a list of aggregated windows
    pub async fn export_usage_report(
        &self,
        tenant_id: &str,
        window: AggregationWindow,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<AggregatedUsage>> {
        let records = self.records.read().await;
        let tenant_records = records.get(tenant_id).ok_or_else(|| {
            StreamlineError::Config(format!("No metering data for tenant: {}", tenant_id))
        })?;

        let duration = match window {
            AggregationWindow::Hourly => chrono::Duration::hours(1),
            AggregationWindow::Daily => chrono::Duration::days(1),
        };

        let mut results = Vec::new();
        let mut cursor = from;
        while cursor < to {
            let window_end = (cursor + duration).min(to);
            let agg = UsageAggregator::aggregate(
                tenant_id,
                tenant_records,
                window,
                cursor,
                window_end,
            );
            if agg.sample_count > 0 {
                results.push(agg);
            }
            cursor = window_end;
        }

        Ok(results)
    }
}

impl Default for MeteringService {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(tenant_id: &str, ts: DateTime<Utc>, bytes_in: u64) -> MeteringRecord {
        MeteringRecord {
            tenant_id: tenant_id.to_string(),
            timestamp: ts,
            bytes_in,
            bytes_out: bytes_in / 2,
            messages_in: 100,
            messages_out: 80,
            storage_bytes: 1_000_000,
            active_connections: 5,
        }
    }

    #[tokio::test]
    async fn test_record_and_summarize() {
        let svc = MeteringService::new();
        let now = Utc::now();

        svc.record_usage(make_record("t1", now, 1024)).await.unwrap();
        svc.record_usage(make_record("t1", now, 2048)).await.unwrap();

        let summary = svc
            .get_usage_summary("t1", now - chrono::Duration::hours(1), now + chrono::Duration::hours(1))
            .await
            .unwrap();

        assert_eq!(summary.total_bytes_in, 3072);
        assert_eq!(summary.total_messages_in, 200);
        assert_eq!(summary.sample_count, 2);
    }

    #[tokio::test]
    async fn test_no_data_returns_error() {
        let svc = MeteringService::new();
        let now = Utc::now();
        let result = svc
            .get_usage_summary("nonexistent", now - chrono::Duration::hours(1), now)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_export_usage_report() {
        let svc = MeteringService::new();
        let base = Utc::now();

        for i in 0..3 {
            let ts = base + chrono::Duration::hours(i);
            svc.record_usage(make_record("t1", ts, 512)).await.unwrap();
        }

        let report = svc
            .export_usage_report(
                "t1",
                AggregationWindow::Hourly,
                base - chrono::Duration::minutes(1),
                base + chrono::Duration::hours(3),
            )
            .await
            .unwrap();

        assert!(!report.is_empty());
        assert!(report.iter().all(|a| a.window == AggregationWindow::Hourly));
    }

    #[test]
    fn test_aggregator_empty_window() {
        let agg = UsageAggregator::aggregate(
            "t1",
            &[],
            AggregationWindow::Daily,
            Utc::now(),
            Utc::now() + chrono::Duration::days(1),
        );
        assert_eq!(agg.sample_count, 0);
        assert_eq!(agg.total_bytes_in, 0);
    }
}
