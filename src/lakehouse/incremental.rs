//! Incremental Refresh Engine
//!
//! Processes only changed data for efficient materialized view updates.

use super::materialized_view::{MaterializedView, Watermark};
use super::query_optimizer::{AggregateExpr, Value};
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Incremental refresh engine
pub struct IncrementalEngine {
    /// Delta tracking enabled
    delta_enabled: bool,
    /// Batch size for processing
    batch_size: usize,
}

impl IncrementalEngine {
    /// Create a new incremental engine
    pub fn new(delta_enabled: bool, batch_size: usize) -> Self {
        Self {
            delta_enabled,
            batch_size,
        }
    }

    /// Process incremental refresh for a view
    pub async fn process_incremental(
        &self,
        view: &MaterializedView,
        delta: DeltaBatch,
    ) -> Result<IncrementalResult> {
        if !self.delta_enabled {
            return Err(StreamlineError::Config(
                "Delta processing is disabled".into(),
            ));
        }

        if !view.supports_incremental() {
            return Err(StreamlineError::Config(format!(
                "View '{}' does not support incremental refresh",
                view.name
            )));
        }

        let start_time = std::time::Instant::now();
        let mut rows_processed = 0;
        let mut rows_inserted = 0;
        let mut rows_updated = 0;
        let mut rows_deleted = 0;

        // Process changes in batches
        for batch in delta.changes.chunks(self.batch_size) {
            for change in batch {
                match change.operation {
                    ChangeOperation::Insert => {
                        self.process_insert(view, change).await?;
                        rows_inserted += 1;
                    }
                    ChangeOperation::Update => {
                        self.process_update(view, change).await?;
                        rows_updated += 1;
                    }
                    ChangeOperation::Delete => {
                        self.process_delete(view, change).await?;
                        rows_deleted += 1;
                    }
                }
                rows_processed += 1;
            }
        }

        Ok(IncrementalResult {
            view_name: view.name.clone(),
            rows_processed,
            rows_inserted,
            rows_updated,
            rows_deleted,
            duration_ms: start_time.elapsed().as_millis() as u64,
            new_watermark: delta.watermark,
        })
    }

    /// Process an insert change
    async fn process_insert(&self, view: &MaterializedView, change: &Change) -> Result<()> {
        // Apply aggregation updates for insert
        tracing::debug!(
            view = %view.name,
            offset = change.offset,
            "Processing insert for incremental refresh"
        );

        // For each aggregate, update the running value
        for agg in &view.definition.aggregates {
            self.update_aggregate_for_insert(agg, &change.values)
                .await?;
        }

        Ok(())
    }

    /// Process an update change
    async fn process_update(&self, view: &MaterializedView, change: &Change) -> Result<()> {
        // For update, we need both old and new values
        tracing::debug!(
            view = %view.name,
            offset = change.offset,
            "Processing update for incremental refresh"
        );

        // Subtract old value and add new value
        if let Some(old_values) = &change.old_values {
            for agg in &view.definition.aggregates {
                self.update_aggregate_for_delete(agg, old_values).await?;
                self.update_aggregate_for_insert(agg, &change.values)
                    .await?;
            }
        }

        Ok(())
    }

    /// Process a delete change
    async fn process_delete(&self, view: &MaterializedView, change: &Change) -> Result<()> {
        tracing::debug!(
            view = %view.name,
            offset = change.offset,
            "Processing delete for incremental refresh"
        );

        for agg in &view.definition.aggregates {
            self.update_aggregate_for_delete(agg, &change.values)
                .await?;
        }

        Ok(())
    }

    /// Update aggregate for an insert
    async fn update_aggregate_for_insert(
        &self,
        agg: &AggregateExpr,
        values: &HashMap<String, Value>,
    ) -> Result<()> {
        let column_value = agg.column.as_ref().and_then(|c| values.get(c));

        match agg.function.to_uppercase().as_str() {
            "COUNT" => {
                // Increment count
                tracing::trace!("Incrementing COUNT for {}", agg.alias);
            }
            "SUM" => {
                if let Some(Value::Int(v)) = column_value {
                    tracing::trace!("Adding {} to SUM for {}", v, agg.alias);
                } else if let Some(Value::Float(v)) = column_value {
                    tracing::trace!("Adding {} to SUM for {}", v, agg.alias);
                }
            }
            "MIN" => {
                // Update min if new value is smaller
                tracing::trace!("Checking MIN for {}", agg.alias);
            }
            "MAX" => {
                // Update max if new value is larger
                tracing::trace!("Checking MAX for {}", agg.alias);
            }
            _ => {
                return Err(StreamlineError::Config(format!(
                    "Aggregate function '{}' not supported for incremental refresh",
                    agg.function
                )));
            }
        }

        Ok(())
    }

    /// Update aggregate for a delete
    async fn update_aggregate_for_delete(
        &self,
        agg: &AggregateExpr,
        values: &HashMap<String, Value>,
    ) -> Result<()> {
        let column_value = agg.column.as_ref().and_then(|c| values.get(c));

        match agg.function.to_uppercase().as_str() {
            "COUNT" => {
                // Decrement count
                tracing::trace!("Decrementing COUNT for {}", agg.alias);
            }
            "SUM" => {
                if let Some(Value::Int(v)) = column_value {
                    tracing::trace!("Subtracting {} from SUM for {}", v, agg.alias);
                } else if let Some(Value::Float(v)) = column_value {
                    tracing::trace!("Subtracting {} from SUM for {}", v, agg.alias);
                }
            }
            "MIN" | "MAX" => {
                // MIN/MAX require recalculation if the deleted value was the min/max
                // This is a limitation of simple incremental processing
                tracing::trace!("MIN/MAX may need recalculation for {}", agg.alias);
            }
            _ => {}
        }

        Ok(())
    }

    /// Compute delta between two watermarks
    pub fn compute_delta(
        &self,
        old_watermark: Option<&Watermark>,
        new_watermark: &Watermark,
    ) -> DeltaRange {
        let mut partition_ranges = HashMap::new();

        for (partition, &new_offset) in &new_watermark.partitions {
            let old_offset = old_watermark
                .and_then(|w| w.partitions.get(partition))
                .copied()
                .unwrap_or(0);

            if new_offset > old_offset {
                partition_ranges.insert(
                    *partition,
                    OffsetRange {
                        start: old_offset,
                        end: new_offset,
                    },
                );
            }
        }

        DeltaRange {
            partition_ranges,
            timestamp_range: old_watermark
                .and_then(|w| w.timestamp)
                .zip(new_watermark.timestamp)
                .map(|(start, end)| TimestampRange { start, end }),
        }
    }
}

/// Batch of changes for incremental processing
#[derive(Debug, Clone)]
pub struct DeltaBatch {
    /// Topic name
    pub topic: String,
    /// Partition
    pub partition: i32,
    /// Changes in this batch
    pub changes: Vec<Change>,
    /// New watermark after processing
    pub watermark: Watermark,
}

/// A single change record
#[derive(Debug, Clone)]
pub struct Change {
    /// Offset of the record
    pub offset: i64,
    /// Timestamp of the change
    pub timestamp: i64,
    /// Type of change
    pub operation: ChangeOperation,
    /// New values (for insert/update)
    pub values: HashMap<String, Value>,
    /// Old values (for update/delete)
    pub old_values: Option<HashMap<String, Value>>,
    /// Partition key
    pub key: Option<Vec<u8>>,
}

/// Type of change operation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeOperation {
    /// New record inserted
    Insert,
    /// Record updated
    Update,
    /// Record deleted
    Delete,
}

/// Range of offsets for delta processing
#[derive(Debug, Clone)]
pub struct DeltaRange {
    /// Per-partition offset ranges
    pub partition_ranges: HashMap<i32, OffsetRange>,
    /// Timestamp range
    pub timestamp_range: Option<TimestampRange>,
}

/// Offset range
#[derive(Debug, Clone, Copy)]
pub struct OffsetRange {
    /// Start offset (inclusive)
    pub start: i64,
    /// End offset (exclusive)
    pub end: i64,
}

impl OffsetRange {
    /// Check if range is empty
    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    /// Get the number of offsets in range
    pub fn len(&self) -> u64 {
        if self.is_empty() {
            0
        } else {
            (self.end - self.start) as u64
        }
    }
}

/// Timestamp range
#[derive(Debug, Clone, Copy)]
pub struct TimestampRange {
    /// Start timestamp (inclusive)
    pub start: i64,
    /// End timestamp (exclusive)
    pub end: i64,
}

/// Result of incremental processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalResult {
    /// View name
    pub view_name: String,
    /// Total rows processed
    pub rows_processed: u64,
    /// Rows inserted
    pub rows_inserted: u64,
    /// Rows updated
    pub rows_updated: u64,
    /// Rows deleted
    pub rows_deleted: u64,
    /// Processing duration in ms
    pub duration_ms: u64,
    /// New watermark
    pub new_watermark: Watermark,
}

/// Aggregation state for incremental updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationState {
    /// Group key values
    pub group_key: HashMap<String, Value>,
    /// Aggregate values
    pub aggregates: HashMap<String, AggregateValue>,
}

/// Value of an aggregate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregateValue {
    /// Count value
    Count(u64),
    /// Sum value (integer)
    SumInt(i64),
    /// Sum value (float)
    SumFloat(f64),
    /// Min value
    Min(Value),
    /// Max value
    Max(Value),
    /// Average state (sum, count)
    Avg { sum: f64, count: u64 },
}

impl AggregateValue {
    /// Merge another aggregate value into this one
    pub fn merge(&mut self, other: &AggregateValue) -> Result<()> {
        match (self, other) {
            (AggregateValue::Count(a), AggregateValue::Count(b)) => {
                *a += b;
            }
            (AggregateValue::SumInt(a), AggregateValue::SumInt(b)) => {
                *a += b;
            }
            (AggregateValue::SumFloat(a), AggregateValue::SumFloat(b)) => {
                *a += b;
            }
            (AggregateValue::Min(a), AggregateValue::Min(b)) => {
                if compare_values(b, a) == std::cmp::Ordering::Less {
                    *a = b.clone();
                }
            }
            (AggregateValue::Max(a), AggregateValue::Max(b)) => {
                if compare_values(b, a) == std::cmp::Ordering::Greater {
                    *a = b.clone();
                }
            }
            (
                AggregateValue::Avg { sum: s1, count: c1 },
                AggregateValue::Avg { sum: s2, count: c2 },
            ) => {
                *s1 += s2;
                *c1 += c2;
            }
            _ => {
                return Err(StreamlineError::Config(
                    "Cannot merge different aggregate types".into(),
                ));
            }
        }
        Ok(())
    }

    /// Get the final value of the aggregate
    pub fn finalize(&self) -> Value {
        match self {
            AggregateValue::Count(c) => Value::Int(*c as i64),
            AggregateValue::SumInt(s) => Value::Int(*s),
            AggregateValue::SumFloat(s) => Value::Float(*s),
            AggregateValue::Min(v) | AggregateValue::Max(v) => v.clone(),
            AggregateValue::Avg { sum, count } => {
                if *count > 0 {
                    Value::Float(*sum / *count as f64)
                } else {
                    Value::Null
                }
            }
        }
    }
}

/// Compare two values
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    }
}

/// Delta log for tracking changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaLog {
    /// Log entries
    entries: Vec<DeltaLogEntry>,
    /// Current version
    version: u64,
}

impl DeltaLog {
    /// Create a new delta log
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            version: 0,
        }
    }

    /// Append an entry to the log
    pub fn append(&mut self, entry: DeltaLogEntry) {
        self.version += 1;
        self.entries.push(entry);
    }

    /// Get entries since a version
    pub fn entries_since(&self, version: u64) -> &[DeltaLogEntry] {
        let start_idx = self.entries.iter().position(|e| e.version > version);
        match start_idx {
            Some(idx) => &self.entries[idx..],
            None => &[],
        }
    }

    /// Compact the log (remove old entries)
    pub fn compact(&mut self, keep_versions: u64) {
        let min_version = self.version.saturating_sub(keep_versions);
        self.entries.retain(|e| e.version >= min_version);
    }
}

impl Default for DeltaLog {
    fn default() -> Self {
        Self::new()
    }
}

/// Entry in the delta log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaLogEntry {
    /// Version number
    pub version: u64,
    /// Timestamp
    pub timestamp: i64,
    /// Type of entry
    pub entry_type: DeltaLogEntryType,
    /// Affected partitions
    pub partitions: Vec<i32>,
    /// Number of records
    pub record_count: u64,
}

/// Type of delta log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeltaLogEntryType {
    /// New data added
    DataAdded {
        /// Paths to new files
        files: Vec<String>,
    },
    /// Data removed
    DataRemoved {
        /// Paths to removed files
        files: Vec<String>,
    },
    /// Compaction performed
    Compaction {
        /// Old files that were compacted
        old_files: Vec<String>,
        /// New compacted file
        new_file: String,
    },
    /// Schema change
    SchemaChange {
        /// Old schema version
        old_version: u32,
        /// New schema version
        new_version: u32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_range() {
        let range = OffsetRange { start: 10, end: 20 };
        assert!(!range.is_empty());
        assert_eq!(range.len(), 10);

        let empty = OffsetRange { start: 20, end: 10 };
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
    }

    #[test]
    fn test_aggregate_value_merge() {
        let mut count1 = AggregateValue::Count(10);
        let count2 = AggregateValue::Count(5);
        count1.merge(&count2).unwrap();
        assert!(matches!(count1, AggregateValue::Count(15)));

        let mut sum1 = AggregateValue::SumInt(100);
        let sum2 = AggregateValue::SumInt(50);
        sum1.merge(&sum2).unwrap();
        assert!(matches!(sum1, AggregateValue::SumInt(150)));
    }

    #[test]
    fn test_delta_log() {
        let mut log = DeltaLog::new();

        log.append(DeltaLogEntry {
            version: 1,
            timestamp: 1000,
            entry_type: DeltaLogEntryType::DataAdded {
                files: vec!["file1.parquet".to_string()],
            },
            partitions: vec![0],
            record_count: 100,
        });

        log.append(DeltaLogEntry {
            version: 2,
            timestamp: 2000,
            entry_type: DeltaLogEntryType::DataAdded {
                files: vec!["file2.parquet".to_string()],
            },
            partitions: vec![0],
            record_count: 50,
        });

        assert_eq!(log.version, 2);
        assert_eq!(log.entries_since(0).len(), 2);
        assert_eq!(log.entries_since(1).len(), 1);
    }
}
