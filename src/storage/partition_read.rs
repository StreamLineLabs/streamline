//! Read operations for Partition
//!
//! Contains read, fetch-at-offset, and timestamp-based offset lookup methods.

use crate::error::Result;
use crate::storage::partition::Partition;
use crate::storage::record::Record;

impl Partition {
    /// Read records from this partition starting at the given offset
    /// This reads all records including uncommitted ones (used for replica fetching)
    pub fn read(&self, start_offset: i64, max_records: usize) -> Result<Vec<Record>> {
        self.read_internal(start_offset, max_records, None)
    }

    /// Read committed records up to the high watermark
    /// This is used for consumer reads - consumers should only see committed data
    pub fn read_committed(&self, start_offset: i64, max_records: usize) -> Result<Vec<Record>> {
        let hwm = self.high_watermark();
        self.read_internal(start_offset, max_records, Some(hwm))
    }

    /// Read committed records up to the last stable offset (LSO).
    /// Used for READ_COMMITTED isolation level in transactional consumers.
    ///
    /// The LSO is the first offset of any ongoing transaction, or HWM if none.
    /// Records beyond the LSO are not returned because they may belong to
    /// in-progress transactions. The aborted transaction list is returned
    /// separately in the fetch response for client-side filtering.
    pub fn read_committed_with_lso(
        &self,
        start_offset: i64,
        max_records: usize,
        last_stable_offset: i64,
    ) -> Result<Vec<Record>> {
        let limit = last_stable_offset.min(self.high_watermark());
        self.read_internal(start_offset, max_records, Some(limit))
    }

    /// Internal read method with optional max offset limit
    fn read_internal(
        &self,
        start_offset: i64,
        max_records: usize,
        max_offset: Option<i64>,
    ) -> Result<Vec<Record>> {
        let mut records = Vec::new();

        // In-memory mode: read directly from memory
        if self.in_memory {
            for record in &self.in_memory_records {
                if records.len() >= max_records {
                    break;
                }
                if record.offset < start_offset {
                    continue;
                }
                if let Some(limit) = max_offset {
                    if record.offset >= limit {
                        break;
                    }
                }
                records.push(record.clone());
            }
        } else {
            // Disk-based mode: read from segment files
            let mut remaining = max_records;

            // Read from sealed segments
            for segment in &self.sealed_segments {
                if remaining == 0 {
                    break;
                }

                // Skip segments that are entirely before start_offset
                if segment.max_offset() < start_offset {
                    continue;
                }

                // Skip segments that are entirely after max_offset
                if let Some(limit) = max_offset {
                    if segment.base_offset() >= limit {
                        break;
                    }
                }

                let segment_records = segment.read_from_offset(start_offset, remaining)?;

                // Filter records by max_offset if specified
                let filtered: Vec<_> = if let Some(limit) = max_offset {
                    segment_records
                        .into_iter()
                        .filter(|r| r.offset < limit)
                        .collect()
                } else {
                    segment_records
                };

                remaining -= filtered.len();
                records.extend(filtered);
            }

            // Read from active segment
            if remaining > 0 {
                if let Some(ref segment) = self.active_segment {
                    if segment.max_offset() >= start_offset {
                        let segment_records = segment.read_from_offset(start_offset, remaining)?;

                        // Filter records by max_offset if specified
                        let filtered: Vec<_> = if let Some(limit) = max_offset {
                            segment_records
                                .into_iter()
                                .filter(|r| r.offset < limit)
                                .collect()
                        } else {
                            segment_records
                        };

                        records.extend(filtered);
                    }
                }
            }
        }

        // Record bytes out
        let bytes_out: u64 = records.iter().map(|r| r.value.len() as u64).sum();
        crate::metrics::record_bytes_out(&self.topic, bytes_out);

        Ok(records)
    }

    /// Find the first offset with timestamp >= target_timestamp
    ///
    /// This is used by the ListOffsets API to support timestamp-based offset lookup.
    /// Returns `Ok(None)` if no records match (empty partition or all records older than target).
    ///
    /// # Algorithm
    /// 1. Use segment header timestamps to skip segments that can't contain the target
    /// 2. For candidate segments, scan records to find first offset >= target_timestamp
    /// 3. Return the first matching offset
    pub fn find_offset_by_timestamp(&self, target_timestamp: i64) -> Result<Option<i64>> {
        // Handle in-memory mode
        if self.in_memory {
            for record in &self.in_memory_records {
                if record.timestamp >= target_timestamp {
                    return Ok(Some(record.offset));
                }
            }
            return Ok(None);
        }

        // Disk-based mode: check sealed segments first
        for segment in &self.sealed_segments {
            // Skip segments whose max_timestamp is less than target
            // (all records in this segment are older than what we're looking for)
            if segment.max_timestamp() < target_timestamp {
                continue;
            }

            // This segment might contain our target timestamp
            // Read records and find the first one >= target_timestamp
            // Start from base offset since we need to check timestamps
            let records = segment.read_from_offset(segment.base_offset(), usize::MAX)?;
            for record in records {
                if record.timestamp >= target_timestamp {
                    return Ok(Some(record.offset));
                }
            }
        }

        // Check active segment
        if let Some(ref segment) = self.active_segment {
            // Only check if there are records and max_timestamp >= target
            if segment.record_count() > 0 && segment.max_timestamp() >= target_timestamp {
                let records = segment.read_from_offset(segment.base_offset(), usize::MAX)?;
                for record in records {
                    if record.timestamp >= target_timestamp {
                        return Ok(Some(record.offset));
                    }
                }
            }
        }

        // No matching offset found
        Ok(None)
    }
}
