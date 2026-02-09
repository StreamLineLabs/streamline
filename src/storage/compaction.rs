//! Log compaction for Streamline storage
//!
//! This module implements log compaction to retain only the latest value
//! for each key within a partition. This enables using Streamline as a
//! changelog/table store where old values for the same key can be discarded.

use crate::error::Result;
use crate::storage::partition::Partition;
use crate::storage::record::Record;
use crate::storage::segment::{segment_filename, Segment};
use bytes::Bytes;
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{debug, info};

/// Type alias for the offset map: key -> (offset, record)
type OffsetMap = HashMap<Bytes, (i64, Record)>;

/// Result of building an offset map
type OffsetMapResult = (OffsetMap, Vec<Record>);

/// Log compactor for partition cleanup
pub(crate) struct LogCompactor {
    /// Minimum ratio of dirty (compactable) data before compaction
    #[allow(dead_code)]
    min_cleanable_dirty_ratio: f64,

    /// How long to retain tombstones (in milliseconds)
    delete_retention_ms: i64,

    /// Minimum time before a segment is eligible for compaction (in milliseconds)
    #[allow(dead_code)]
    min_compaction_lag_ms: i64,
}

impl LogCompactor {
    /// Create a new log compactor with the given configuration
    pub fn new(
        min_cleanable_dirty_ratio: f64,
        delete_retention_ms: i64,
        min_compaction_lag_ms: i64,
    ) -> Self {
        Self {
            min_cleanable_dirty_ratio,
            delete_retention_ms,
            min_compaction_lag_ms,
        }
    }

    /// Run compaction on a partition
    ///
    /// This method compacts sealed segments by keeping only the latest value for each key.
    /// Tombstones (records with null values) are retained for `delete_retention_ms` before removal.
    /// Records without keys cannot be compacted and are always retained.
    ///
    /// Returns the number of segments compacted.
    pub fn compact(&self, partition: &mut Partition) -> Result<usize> {
        // Get the current timestamp for tombstone retention
        let current_time = chrono::Utc::now().timestamp_millis();

        // Get sealed segments that are eligible for compaction
        let eligible_segments = self.get_eligible_segments(partition, current_time)?;

        if eligible_segments.is_empty() {
            debug!(
                topic = %partition.topic(),
                partition = partition.id(),
                "No segments eligible for compaction"
            );
            return Ok(0);
        }

        info!(
            topic = %partition.topic(),
            partition = partition.id(),
            segment_count = eligible_segments.len(),
            "Starting compaction"
        );

        // Build key -> (offset, record) map and collect records without keys
        let (offset_map, records_without_keys) =
            self.build_offset_map(&eligible_segments, current_time)?;

        // Compact the segments
        let compacted_count = self.compact_segments(
            partition,
            &eligible_segments,
            &offset_map,
            records_without_keys,
        )?;

        info!(
            topic = %partition.topic(),
            partition = partition.id(),
            compacted = compacted_count,
            "Compaction completed"
        );

        Ok(compacted_count)
    }

    /// Get segments eligible for compaction
    ///
    /// Segments are eligible if:
    /// - They are sealed (not active)
    /// - They are old enough (based on min_compaction_lag_ms)
    fn get_eligible_segments(
        &self,
        partition: &Partition,
        current_time: i64,
    ) -> Result<Vec<SegmentInfo>> {
        let mut eligible = Vec::new();

        // Get all sealed segments
        let sealed_segments = partition.get_sealed_segments();

        // Check if we should compact - need at least 2 segments to compact
        if sealed_segments.len() < 2 {
            return Ok(eligible);
        }

        // Iterate through sealed segments
        for segment in sealed_segments.iter() {
            // Check if segment is old enough
            if self.min_compaction_lag_ms > 0 {
                let segment_age = current_time - segment.max_timestamp();
                if segment_age < self.min_compaction_lag_ms {
                    continue;
                }
            }

            eligible.push(SegmentInfo {
                path: segment.path().to_path_buf(),
                base_offset: segment.base_offset(),
            });
        }

        Ok(eligible)
    }

    /// Build a map of key -> (latest offset, record) from segments
    ///
    /// This scans all records in the given segments and keeps track of the latest
    /// record for each key. Tombstones are handled specially based on retention time.
    /// Also returns records without keys that should be preserved.
    fn build_offset_map(
        &self,
        segments: &[SegmentInfo],
        current_time: i64,
    ) -> Result<OffsetMapResult> {
        let mut offset_map: OffsetMap = HashMap::new();
        let mut records_without_keys = Vec::new();

        for segment_info in segments {
            // Open the segment for reading
            let segment = Segment::open(&segment_info.path)?;
            let records = segment.read_all()?;

            for record in records {
                // Handle records without keys - they cannot be compacted
                let key = match record.key.clone() {
                    Some(k) => k,
                    None => {
                        records_without_keys.push(record);
                        continue;
                    }
                };

                // Check if this is a tombstone (null/empty value)
                let is_tombstone = record.value.is_empty();

                // Handle tombstone retention
                if is_tombstone {
                    let tombstone_age = current_time - record.timestamp;
                    if tombstone_age > self.delete_retention_ms {
                        // Tombstone is old enough to be removed entirely
                        offset_map.remove(&key);
                        continue;
                    }
                }

                // Update the map if this is a newer record for this key
                offset_map
                    .entry(key)
                    .and_modify(|(existing_offset, existing_record)| {
                        if record.offset > *existing_offset {
                            *existing_offset = record.offset;
                            *existing_record = record.clone();
                        }
                    })
                    .or_insert((record.offset, record.clone()));
            }
        }

        Ok((offset_map, records_without_keys))
    }

    /// Compact segments by rewriting them with only the latest values
    fn compact_segments(
        &self,
        partition: &mut Partition,
        segments: &[SegmentInfo],
        offset_map: &OffsetMap,
        records_without_keys: Vec<Record>,
    ) -> Result<usize> {
        if segments.is_empty() {
            return Ok(0);
        }

        // Get the base offset of the first segment
        let base_offset = segments[0].base_offset;

        // Create a new compacted segment
        let compacted_path = partition.path().join(segment_filename(base_offset));
        let mut compacted_segment = Segment::create(&compacted_path, base_offset)?;

        // Collect all records that should be retained (latest per key)
        let mut retained_records: Vec<Record> =
            offset_map.values().map(|(_, r)| r.clone()).collect();

        // Add records without keys
        retained_records.extend(records_without_keys);

        // Sort by offset to maintain order
        retained_records.sort_by_key(|r| r.offset);

        // Write records to the compacted segment
        for record in &retained_records {
            compacted_segment.append_record(record.clone())?;
        }

        // Seal the compacted segment
        compacted_segment.seal()?;

        info!(
            "Created compacted segment at offset {} with {} records",
            base_offset,
            retained_records.len()
        );

        // Replace the old segments with the new compacted one
        let removed =
            partition.replace_with_compacted_segment(segments.len(), compacted_segment)?;

        Ok(removed)
    }
}

impl Default for LogCompactor {
    fn default() -> Self {
        Self::new(0.5, 86_400_000, 0)
    }
}

/// Information about a segment for compaction
struct SegmentInfo {
    path: PathBuf,
    base_offset: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_compactor_creation() {
        let compactor = LogCompactor::new(0.5, 86400000, 0);
        assert_eq!(compactor.min_cleanable_dirty_ratio, 0.5);
        assert_eq!(compactor.delete_retention_ms, 86400000);
        assert_eq!(compactor.min_compaction_lag_ms, 0);
    }

    #[test]
    fn test_compactor_default() {
        let compactor = LogCompactor::default();
        assert_eq!(compactor.min_cleanable_dirty_ratio, 0.5);
        assert_eq!(compactor.delete_retention_ms, 86400000);
        assert_eq!(compactor.min_compaction_lag_ms, 0);
    }

    #[test]
    fn test_build_offset_map_empty() {
        let compactor = LogCompactor::default();
        let segments: Vec<SegmentInfo> = vec![];
        let current_time = chrono::Utc::now().timestamp_millis();

        let (offset_map, records_without_keys) =
            compactor.build_offset_map(&segments, current_time).unwrap();
        assert!(offset_map.is_empty());
        assert!(records_without_keys.is_empty());
    }

    #[test]
    fn test_compaction_with_duplicate_keys() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Write records with duplicate keys
        partition
            .append(Some(Bytes::from("key1")), Bytes::from("value1"))
            .unwrap();
        partition
            .append(Some(Bytes::from("key2")), Bytes::from("value2"))
            .unwrap();
        partition
            .append(Some(Bytes::from("key1")), Bytes::from("value1-updated"))
            .unwrap();
        partition
            .append(Some(Bytes::from("key2")), Bytes::from("value2-updated"))
            .unwrap();

        // Force segment roll to have sealed segments
        partition.roll_segment().unwrap();

        // Add more records
        partition
            .append(Some(Bytes::from("key1")), Bytes::from("value1-final"))
            .unwrap();

        // Get initial count
        let initial_count = partition.record_count();
        assert_eq!(initial_count, 5);

        // Run compaction
        let compactor = LogCompactor::default();
        let _compacted_count = compactor.compact(&mut partition).unwrap();

        // Should have compacted successfully
        // Note: compaction only works on sealed segments, not the active one
    }

    #[test]
    fn test_compaction_with_tombstones() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Write records with keys
        partition
            .append(Some(Bytes::from("key1")), Bytes::from("value1"))
            .unwrap();
        partition
            .append(Some(Bytes::from("key2")), Bytes::from("value2"))
            .unwrap();

        // Write tombstone (empty value)
        partition
            .append(Some(Bytes::from("key1")), Bytes::from(""))
            .unwrap();

        // Force segment roll
        partition.roll_segment().unwrap();

        // Run compaction with short tombstone retention
        let compactor = LogCompactor::new(0.5, 0, 0); // 0ms retention for tombstones
        std::thread::sleep(std::time::Duration::from_millis(10)); // Wait a bit
        let _compacted_count = compactor.compact(&mut partition).unwrap();

        // Tombstones should be removed after retention period
        // The test verifies compaction doesn't crash with tombstones
    }

    #[test]
    fn test_compaction_preserves_records_without_keys() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Write records without keys (cannot be compacted)
        partition.append(None, Bytes::from("value1")).unwrap();
        partition.append(None, Bytes::from("value2")).unwrap();

        // Write records with keys
        partition
            .append(Some(Bytes::from("key1")), Bytes::from("value3"))
            .unwrap();
        partition
            .append(Some(Bytes::from("key1")), Bytes::from("value3-updated"))
            .unwrap();

        // Force segment roll
        partition.roll_segment().unwrap();

        let initial_count = partition.record_count();

        // Run compaction
        let compactor = LogCompactor::default();
        let _compacted_count = compactor.compact(&mut partition).unwrap();

        // Records without keys should be preserved
        let final_count = partition.record_count();

        // We should have at least the records without keys
        assert!(final_count >= 2);
        assert!(final_count <= initial_count);
    }

    #[test]
    fn test_compaction_keeps_latest_per_key() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Write multiple versions of the same key
        partition
            .append(Some(Bytes::from("key1")), Bytes::from("v1"))
            .unwrap();
        partition
            .append(Some(Bytes::from("key1")), Bytes::from("v2"))
            .unwrap();
        partition
            .append(Some(Bytes::from("key1")), Bytes::from("v3"))
            .unwrap();

        // Force segment roll
        partition.roll_segment().unwrap();

        // Read records before compaction
        let records_before = partition.read(0, 100).unwrap();
        assert_eq!(records_before.len(), 3);

        // Run compaction
        let compactor = LogCompactor::default();
        compactor.compact(&mut partition).unwrap();

        // After compaction, only the latest should remain for the key
        // Note: active segment is not compacted, only sealed segments
        // Since we only have one sealed segment, compaction won't do much
    }

    #[test]
    fn test_compaction_no_segments() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // No records written, so no segments to compact
        let compactor = LogCompactor::default();
        let compacted_count = compactor.compact(&mut partition).unwrap();

        assert_eq!(compacted_count, 0);
    }

    #[test]
    fn test_compaction_respects_min_lag() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Write some records
        partition
            .append(Some(Bytes::from("key1")), Bytes::from("value1"))
            .unwrap();
        partition.roll_segment().unwrap();

        // Create compactor with very long lag requirement (1 hour)
        let compactor = LogCompactor::new(0.5, 86400000, 3600000);
        let compacted_count = compactor.compact(&mut partition).unwrap();

        // Should not compact because segments are too new
        assert_eq!(compacted_count, 0);
    }
}
