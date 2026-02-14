//! Time-Based Partitioning
//!
//! Provides time-based partitioning strategies for efficient data organization.

use super::{DataPoint, DataPointBatch};
use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Datelike, NaiveDateTime, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Time partition granularity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TimePartition {
    /// Hourly partitions (YYYY-MM-DD-HH)
    #[default]
    Hourly,
    /// Daily partitions (YYYY-MM-DD)
    Daily,
    /// Weekly partitions (YYYY-WW)
    Weekly,
    /// Monthly partitions (YYYY-MM)
    Monthly,
    /// Yearly partitions (YYYY)
    Yearly,
}

impl TimePartition {
    /// Get partition key for a timestamp
    pub fn key_for(&self, timestamp_ms: i64) -> String {
        let secs = timestamp_ms / 1000;
        let dt = DateTime::<Utc>::from_timestamp(secs, 0).unwrap_or_default();

        match self {
            TimePartition::Hourly => format!(
                "{:04}-{:02}-{:02}-{:02}",
                dt.year(),
                dt.month(),
                dt.day(),
                dt.hour()
            ),
            TimePartition::Daily => {
                format!("{:04}-{:02}-{:02}", dt.year(), dt.month(), dt.day())
            }
            TimePartition::Weekly => {
                format!("{:04}-W{:02}", dt.iso_week().year(), dt.iso_week().week())
            }
            TimePartition::Monthly => format!("{:04}-{:02}", dt.year(), dt.month()),
            TimePartition::Yearly => format!("{:04}", dt.year()),
        }
    }

    /// Get start timestamp for a partition key
    pub fn start_time(&self, key: &str) -> Option<i64> {
        let parts: Vec<&str> = key.split('-').collect();

        match self {
            TimePartition::Hourly => {
                if parts.len() != 4 {
                    return None;
                }
                let year: i32 = parts[0].parse().ok()?;
                let month: u32 = parts[1].parse().ok()?;
                let day: u32 = parts[2].parse().ok()?;
                let hour: u32 = parts[3].parse().ok()?;

                let naive = NaiveDateTime::new(
                    chrono::NaiveDate::from_ymd_opt(year, month, day)?,
                    chrono::NaiveTime::from_hms_opt(hour, 0, 0)?,
                );
                Some(naive.and_utc().timestamp_millis())
            }
            TimePartition::Daily => {
                if parts.len() != 3 {
                    return None;
                }
                let year: i32 = parts[0].parse().ok()?;
                let month: u32 = parts[1].parse().ok()?;
                let day: u32 = parts[2].parse().ok()?;

                let naive = NaiveDateTime::new(
                    chrono::NaiveDate::from_ymd_opt(year, month, day)?,
                    chrono::NaiveTime::from_hms_opt(0, 0, 0)?,
                );
                Some(naive.and_utc().timestamp_millis())
            }
            TimePartition::Weekly => {
                // Format: YYYY-Www
                if parts.len() != 2 {
                    return None;
                }
                let year: i32 = parts[0].parse().ok()?;
                let week_str = parts[1].strip_prefix('W')?;
                let week: u32 = week_str.parse().ok()?;

                let date = chrono::NaiveDate::from_isoywd_opt(year, week, chrono::Weekday::Mon)?;
                let naive = NaiveDateTime::new(date, chrono::NaiveTime::from_hms_opt(0, 0, 0)?);
                Some(naive.and_utc().timestamp_millis())
            }
            TimePartition::Monthly => {
                if parts.len() != 2 {
                    return None;
                }
                let year: i32 = parts[0].parse().ok()?;
                let month: u32 = parts[1].parse().ok()?;

                let naive = NaiveDateTime::new(
                    chrono::NaiveDate::from_ymd_opt(year, month, 1)?,
                    chrono::NaiveTime::from_hms_opt(0, 0, 0)?,
                );
                Some(naive.and_utc().timestamp_millis())
            }
            TimePartition::Yearly => {
                if parts.len() != 1 {
                    return None;
                }
                let year: i32 = parts[0].parse().ok()?;

                let naive = NaiveDateTime::new(
                    chrono::NaiveDate::from_ymd_opt(year, 1, 1)?,
                    chrono::NaiveTime::from_hms_opt(0, 0, 0)?,
                );
                Some(naive.and_utc().timestamp_millis())
            }
        }
    }

    /// Get end timestamp for a partition key
    pub fn end_time(&self, key: &str) -> Option<i64> {
        let start = self.start_time(key)?;
        let duration_ms = match self {
            TimePartition::Hourly => 3600 * 1000,
            TimePartition::Daily => 86400 * 1000,
            TimePartition::Weekly => 7 * 86400 * 1000,
            TimePartition::Monthly => {
                // Approximate - use 31 days
                31 * 86400 * 1000
            }
            TimePartition::Yearly => {
                // Approximate - use 366 days
                366 * 86400 * 1000
            }
        };
        Some(start + duration_ms)
    }

    /// Get duration in milliseconds
    pub fn duration_ms(&self) -> i64 {
        match self {
            TimePartition::Hourly => 3600 * 1000,
            TimePartition::Daily => 86400 * 1000,
            TimePartition::Weekly => 7 * 86400 * 1000,
            TimePartition::Monthly => 30 * 86400 * 1000,
            TimePartition::Yearly => 365 * 86400 * 1000,
        }
    }
}

/// Partition state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionState {
    /// Partition is active and accepting writes
    Active,
    /// Partition is sealed (read-only)
    Sealed,
    /// Partition is being compacted
    Compacting,
    /// Partition has been archived
    Archived,
    /// Partition is marked for deletion
    Deleting,
}

/// Partition information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    /// Partition key
    pub key: String,
    /// Partition strategy
    pub strategy: TimePartition,
    /// Partition state
    pub state: PartitionState,
    /// Start timestamp
    pub start_time: i64,
    /// End timestamp
    pub end_time: i64,
    /// Number of points in partition
    pub point_count: u64,
    /// Size in bytes
    pub size_bytes: u64,
    /// File path
    pub path: PathBuf,
    /// Creation time
    pub created_at: i64,
    /// Last modified time
    pub modified_at: i64,
}

impl PartitionInfo {
    /// Create new partition info
    pub fn new(key: &str, strategy: TimePartition, path: PathBuf) -> Self {
        let start_time = strategy.start_time(key).unwrap_or(0);
        let end_time = strategy.end_time(key).unwrap_or(0);
        let now = chrono::Utc::now().timestamp_millis();

        Self {
            key: key.to_string(),
            strategy,
            state: PartitionState::Active,
            start_time,
            end_time,
            point_count: 0,
            size_bytes: 0,
            path,
            created_at: now,
            modified_at: now,
        }
    }

    /// Check if timestamp falls within this partition
    pub fn contains(&self, timestamp: i64) -> bool {
        timestamp >= self.start_time && timestamp < self.end_time
    }

    /// Check if partition is writable
    pub fn is_writable(&self) -> bool {
        self.state == PartitionState::Active
    }
}

/// Partition statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PartitionStats {
    /// Total partitions
    pub total_partitions: u32,
    /// Active partitions
    pub active_partitions: u32,
    /// Sealed partitions
    pub sealed_partitions: u32,
    /// Archived partitions
    pub archived_partitions: u32,
    /// Total points across all partitions
    pub total_points: u64,
    /// Total size in bytes
    pub total_bytes: u64,
}

/// Partition manager
pub struct PartitionManager {
    strategy: TimePartition,
    base_path: PathBuf,
    partitions: HashMap<String, PartitionInfo>,
    active_partition: Option<String>,
}

impl PartitionManager {
    /// Create a new partition manager
    pub fn new(strategy: TimePartition, base_path: PathBuf) -> Self {
        Self {
            strategy,
            base_path,
            partitions: HashMap::new(),
            active_partition: None,
        }
    }

    /// Get or create partition for a timestamp
    pub fn get_or_create_partition(&mut self, timestamp: i64) -> Result<&mut PartitionInfo> {
        let key = self.strategy.key_for(timestamp);

        if !self.partitions.contains_key(&key) {
            let path = self.base_path.join(&key);
            let partition = PartitionInfo::new(&key, self.strategy, path);
            self.partitions.insert(key.clone(), partition);
            self.active_partition = Some(key.clone());
        }

        self.partitions
            .get_mut(&key)
            .ok_or_else(|| StreamlineError::storage_msg("Failed to get partition".into()))
    }

    /// Get partition for a key
    pub fn get_partition(&self, key: &str) -> Option<&PartitionInfo> {
        self.partitions.get(key)
    }

    /// Get partitions in a time range
    pub fn partitions_in_range(&self, start: i64, end: i64) -> Vec<&PartitionInfo> {
        self.partitions
            .values()
            .filter(|p| p.end_time >= start && p.start_time <= end)
            .collect()
    }

    /// Route batch to partitions
    pub fn route_batch(
        &mut self,
        batch: &DataPointBatch,
    ) -> Result<HashMap<String, Vec<DataPoint>>> {
        let mut routed: HashMap<String, Vec<DataPoint>> = HashMap::new();

        for point in &batch.points {
            let key = self.strategy.key_for(point.timestamp);
            routed.entry(key).or_default().push(point.clone());
        }

        // Ensure partitions exist
        for key in routed.keys() {
            if !self.partitions.contains_key(key) {
                let path = self.base_path.join(key);
                let partition = PartitionInfo::new(key, self.strategy, path);
                self.partitions.insert(key.clone(), partition);
            }
        }

        Ok(routed)
    }

    /// Seal a partition (make it read-only)
    pub fn seal_partition(&mut self, key: &str) -> Result<()> {
        if let Some(partition) = self.partitions.get_mut(key) {
            partition.state = PartitionState::Sealed;
            Ok(())
        } else {
            Err(StreamlineError::storage_msg(format!(
                "Partition not found: {}",
                key
            )))
        }
    }

    /// Delete a partition
    pub fn delete_partition(&mut self, key: &str) -> Result<PartitionInfo> {
        self.partitions
            .remove(key)
            .ok_or_else(|| StreamlineError::storage_msg(format!("Partition not found: {}", key)))
    }

    /// Get statistics
    pub fn stats(&self) -> PartitionStats {
        let mut stats = PartitionStats::default();

        for partition in self.partitions.values() {
            stats.total_partitions += 1;
            stats.total_points += partition.point_count;
            stats.total_bytes += partition.size_bytes;

            match partition.state {
                PartitionState::Active => stats.active_partitions += 1,
                PartitionState::Sealed => stats.sealed_partitions += 1,
                PartitionState::Archived => stats.archived_partitions += 1,
                _ => {}
            }
        }

        stats
    }

    /// List all partition keys
    pub fn list_partitions(&self) -> Vec<String> {
        self.partitions.keys().cloned().collect()
    }

    /// Get partitions older than a certain age
    pub fn expired_partitions(&self, max_age_ms: i64) -> Vec<String> {
        let cutoff = chrono::Utc::now().timestamp_millis() - max_age_ms;

        self.partitions
            .iter()
            .filter(|(_, p)| p.end_time < cutoff)
            .map(|(k, _)| k.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_partition_hourly() {
        let partition = TimePartition::Hourly;

        // 2024-01-15 14:30:00 UTC
        let ts = 1705329000000i64;
        let key = partition.key_for(ts);

        assert_eq!(key, "2024-01-15-14");
    }

    #[test]
    fn test_time_partition_daily() {
        let partition = TimePartition::Daily;

        let ts = 1705329000000i64;
        let key = partition.key_for(ts);

        assert_eq!(key, "2024-01-15");
    }

    #[test]
    fn test_time_partition_monthly() {
        let partition = TimePartition::Monthly;

        let ts = 1705329000000i64;
        let key = partition.key_for(ts);

        assert_eq!(key, "2024-01");
    }

    #[test]
    fn test_partition_start_end_time() {
        let partition = TimePartition::Daily;

        let key = "2024-01-15";
        let start = partition.start_time(key).unwrap();
        let end = partition.end_time(key).unwrap();

        assert!(end > start);
        assert_eq!(end - start, 86400 * 1000);
    }

    #[test]
    fn test_partition_manager() {
        let mut manager = PartitionManager::new(TimePartition::Hourly, PathBuf::from("/tmp/ts"));

        let ts1 = 1705329000000i64; // 2024-01-15 14:xx
        let ts2 = 1705332600000i64; // 2024-01-15 15:xx

        let _ = manager.get_or_create_partition(ts1).unwrap();
        let _ = manager.get_or_create_partition(ts2).unwrap();

        let partitions = manager.list_partitions();
        assert_eq!(partitions.len(), 2);
    }

    #[test]
    fn test_route_batch() {
        let mut manager = PartitionManager::new(TimePartition::Hourly, PathBuf::from("/tmp/ts"));

        let mut batch = DataPointBatch::new();
        batch.add(DataPoint::new("metric", 1705329000000, 1.0)); // Hour 14
        batch.add(DataPoint::new("metric", 1705332600000, 2.0)); // Hour 15
        batch.add(DataPoint::new("metric", 1705329100000, 3.0)); // Hour 14

        let routed = manager.route_batch(&batch).unwrap();

        assert_eq!(routed.len(), 2);
        assert_eq!(routed.get("2024-01-15-14").unwrap().len(), 2);
        assert_eq!(routed.get("2024-01-15-15").unwrap().len(), 1);
    }

    #[test]
    fn test_partition_info_contains() {
        let info = PartitionInfo::new(
            "2024-01-15",
            TimePartition::Daily,
            PathBuf::from("/tmp/ts/2024-01-15"),
        );

        // Timestamp within Jan 15, 2024
        let ts = 1705329000000i64;
        assert!(info.contains(ts));

        // Timestamp on Jan 16, 2024
        let ts2 = 1705420800000i64;
        assert!(!info.contains(ts2));
    }
}
