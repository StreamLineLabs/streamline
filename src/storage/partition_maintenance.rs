//! Maintenance operations for Partition
//!
//! Contains retention enforcement, truncation, compaction support,
//! and segment lifecycle management methods.

use crate::error::{Result, StreamlineError};
use crate::storage::partition::Partition;
use crate::storage::segment::{segment_filename, Segment};
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::Ordering;
use tracing::info;

impl Partition {
    /// Set the maximum segment size
    pub fn set_max_segment_bytes(&mut self, max_bytes: u64) {
        self.max_segment_bytes = max_bytes;
    }

    /// Delete segments whose max_timestamp is older than the cutoff time
    ///
    /// Returns the number of segments deleted.
    /// Never deletes the active segment or the last sealed segment.
    pub fn delete_segments_before_timestamp(&mut self, cutoff_timestamp: i64) -> Result<usize> {
        let mut deleted_count = 0;

        // We need to keep at least one segment (or the active segment if no sealed segments)
        while self.sealed_segments.len() > 1 {
            if let Some(segment) = self.sealed_segments.front() {
                if segment.max_timestamp() < cutoff_timestamp {
                    // Remove and delete this segment
                    if let Some(segment) = self.sealed_segments.pop_front() {
                        info!(
                            topic = %self.topic,
                            partition = self.id,
                            base_offset = segment.base_offset(),
                            max_offset = segment.max_offset(),
                            max_timestamp = segment.max_timestamp(),
                            "Deleting segment (time-based retention)"
                        );
                        segment.delete()?;
                        deleted_count += 1;
                    }
                } else {
                    // Segments are ordered by base offset (and timestamps generally
                    // increase with offsets), so if this segment is recent enough,
                    // all subsequent ones will likely be too
                    break;
                }
            } else {
                break;
            }
        }

        Ok(deleted_count)
    }

    /// Delete oldest segments until total size is <= max_size
    ///
    /// Returns the number of segments deleted.
    /// Never deletes the active segment or the last sealed segment.
    pub fn delete_segments_over_size(&mut self, max_size: u64) -> Result<usize> {
        let mut deleted_count = 0;
        let mut current_size = self.size();

        // We need to keep at least one segment (or the active segment if no sealed segments)
        while current_size > max_size && self.sealed_segments.len() > 1 {
            if let Some(segment) = self.sealed_segments.pop_front() {
                let segment_size = segment.size();
                info!(
                    topic = %self.topic,
                    partition = self.id,
                    base_offset = segment.base_offset(),
                    max_offset = segment.max_offset(),
                    size = segment_size,
                    "Deleting segment (size-based retention)"
                );
                segment.delete()?;
                deleted_count += 1;
                current_size -= segment_size;
            } else {
                break;
            }
        }

        Ok(deleted_count)
    }

    /// Get the path to the partition directory
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Truncate the log to the given offset
    ///
    /// This is used when a follower needs to resync after a leader change.
    /// All records at or after the given offset will be removed.
    /// The HWM will also be adjusted if it's above the truncation point.
    ///
    /// Returns the number of records truncated.
    pub fn truncate_to(&mut self, target_offset: i64) -> Result<u64> {
        let current_leo = self.log_end_offset();

        if target_offset >= current_leo {
            // Nothing to truncate
            return Ok(0);
        }

        if target_offset < 0 {
            return Err(StreamlineError::storage_msg(
                "Cannot truncate to negative offset".to_string(),
            ));
        }

        info!(
            topic = %self.topic,
            partition = self.id,
            current_leo,
            target_offset,
            "Truncating partition log"
        );

        let records_truncated = (current_leo - target_offset) as u64;

        // First, handle the active segment
        if let Some(ref segment) = self.active_segment {
            if segment.base_offset() >= target_offset {
                // Entire active segment needs to be removed
                if let Some(segment) = self.active_segment.take() {
                    segment.force_delete()?; // Use force_delete for active (unsealed) segments
                }
            } else if segment.max_offset() >= target_offset {
                // Active segment needs partial truncation
                // For now, we'll recreate it (simple approach)
                // A more sophisticated approach would truncate in place
                let base_offset = segment.base_offset();
                let records_to_keep = segment.read_from_offset(base_offset, usize::MAX)?;
                let records_to_keep: Vec<_> = records_to_keep
                    .into_iter()
                    .filter(|r| r.offset < target_offset)
                    .collect();

                // Delete old segment
                let old_path = segment.path().to_path_buf();
                if let Some(segment) = self.active_segment.take() {
                    segment.force_delete()?; // Use force_delete for active (unsealed) segments
                }

                // Create new segment with kept records
                let mut new_segment = Segment::create(&old_path, base_offset)?;
                for record in records_to_keep {
                    new_segment.append_record(record)?;
                }
                self.active_segment = Some(new_segment);
            }
        }

        // Handle sealed segments (remove those entirely past target_offset)
        loop {
            let should_remove_entirely = self
                .sealed_segments
                .back()
                .map(|s| s.base_offset() >= target_offset)
                .unwrap_or(false);

            let should_partial_truncate = self
                .sealed_segments
                .back()
                .map(|s| s.base_offset() < target_offset && s.max_offset() >= target_offset)
                .unwrap_or(false);

            if should_remove_entirely {
                // This entire segment is past the truncation point
                if let Some(segment) = self.sealed_segments.pop_back() {
                    info!(
                        topic = %self.topic,
                        partition = self.id,
                        base_offset = segment.base_offset(),
                        "Removing sealed segment during truncation"
                    );
                    segment.delete()?;
                }
            } else if should_partial_truncate {
                // This segment needs partial truncation
                // For simplicity, we'll just unseal it and make it active
                // (in practice, you might truncate in place)
                if let Some(segment) = self.sealed_segments.pop_back() {
                    let base_offset = segment.base_offset();
                    let records_to_keep = segment.read_from_offset(base_offset, usize::MAX)?;
                    let records_to_keep: Vec<_> = records_to_keep
                        .into_iter()
                        .filter(|r| r.offset < target_offset)
                        .collect();

                    let old_path = segment.path().to_path_buf();
                    segment.delete()?;

                    // Create new active segment with kept records
                    let mut new_segment = Segment::create(&old_path, base_offset)?;
                    for record in records_to_keep {
                        new_segment.append_record(record)?;
                    }
                    self.active_segment = Some(new_segment);
                }
                break;
            } else {
                // This segment is entirely before the truncation point, or no more segments
                break;
            }
        }

        // Update LEO to the target offset
        self.next_offset.store(target_offset, Ordering::SeqCst);

        // Adjust HWM if it's above the new LEO
        let hwm = self.high_watermark();
        if hwm > target_offset {
            self.high_watermark.store(target_offset, Ordering::SeqCst);
        }

        // If we have no active segment, create one
        if self.active_segment.is_none() {
            self.create_new_active_segment()?;
        }

        info!(
            topic = %self.topic,
            partition = self.id,
            new_leo = target_offset,
            records_truncated,
            "Partition truncation complete"
        );

        Ok(records_truncated)
    }

    /// Delete all records before the specified offset (DeleteRecords API)
    ///
    /// This is used by the Kafka DeleteRecords API for GDPR compliance and
    /// log management. Records before the specified offset will be deleted,
    /// effectively raising the low watermark (earliest offset).
    ///
    /// Returns the new low watermark (earliest_offset) after deletion.
    pub fn delete_records_before(&mut self, target_offset: i64) -> Result<i64> {
        let current_earliest = self.earliest_offset();
        let current_leo = self.log_end_offset();

        // Validate offset
        if target_offset < 0 {
            return Err(StreamlineError::storage_msg(
                "Cannot delete to negative offset".to_string(),
            ));
        }

        // If target is at or before current earliest, nothing to delete
        if target_offset <= current_earliest {
            return Ok(current_earliest);
        }

        // If target is beyond LEO, treat it as deleting up to LEO
        let effective_target = target_offset.min(current_leo);

        info!(
            topic = %self.topic,
            partition = self.id,
            current_earliest,
            target_offset = effective_target,
            "Deleting records before offset"
        );

        if self.in_memory {
            // In-memory mode: remove records from the front of the deque
            while let Some(record) = self.in_memory_records.front() {
                if record.offset < effective_target {
                    self.in_memory_records.pop_front();
                } else {
                    break;
                }
            }
            return Ok(self.earliest_offset());
        }

        // Remove sealed segments entirely before the target offset
        loop {
            let should_remove_entirely = self
                .sealed_segments
                .front()
                .map(|s| s.max_offset() < effective_target)
                .unwrap_or(false);

            let should_partial_delete = self
                .sealed_segments
                .front()
                .map(|s| s.max_offset() >= effective_target && s.base_offset() < effective_target)
                .unwrap_or(false);

            if should_remove_entirely {
                // This segment is entirely before the target, remove it
                if let Some(segment) = self.sealed_segments.pop_front() {
                    info!(
                        topic = %self.topic,
                        partition = self.id,
                        base_offset = segment.base_offset(),
                        max_offset = segment.max_offset(),
                        "Removing segment during delete_records"
                    );
                    segment.delete()?;
                }
            } else if should_partial_delete {
                // This segment contains the target offset - need partial deletion
                // Read records at/after target, delete old segment, create new one
                let records_to_keep: Vec<_> = self
                    .sealed_segments
                    .front()
                    .map(|s| s.read_from_offset(effective_target, usize::MAX))
                    .transpose()?
                    .unwrap_or_default();

                if let Some(segment) = self.sealed_segments.pop_front() {
                    segment.delete()?;

                    if !records_to_keep.is_empty() {
                        // Create new segment starting at the effective target
                        let new_base_offset = records_to_keep[0].offset;
                        let new_path = self.path.join(segment_filename(new_base_offset));
                        let mut new_segment = Segment::create(&new_path, new_base_offset)?;
                        for record in records_to_keep {
                            new_segment.append_record(record)?;
                        }
                        new_segment.seal()?;
                        self.sealed_segments.push_front(new_segment);
                    }
                }
                break;
            } else {
                // This segment starts at or after target - keep it, or no more segments
                break;
            }
        }

        // Check active segment if no sealed segments remain
        if self.sealed_segments.is_empty() {
            // Check if active segment needs partial deletion
            let needs_partial_delete = self
                .active_segment
                .as_ref()
                .map(|s| s.base_offset() < effective_target)
                .unwrap_or(false);

            if needs_partial_delete {
                // Read records to keep before taking the segment
                let records_to_keep: Vec<_> = self
                    .active_segment
                    .as_ref()
                    .map(|s| s.read_from_offset(effective_target, usize::MAX))
                    .transpose()?
                    .unwrap_or_default();

                // Now take and delete the segment
                if let Some(segment) = self.active_segment.take() {
                    segment.force_delete()?;

                    if !records_to_keep.is_empty() {
                        let new_base_offset = records_to_keep[0].offset;
                        let new_path = self.path.join(segment_filename(new_base_offset));
                        let mut new_segment = Segment::create(&new_path, new_base_offset)?;
                        for record in records_to_keep {
                            new_segment.append_record(record)?;
                        }
                        self.active_segment = Some(new_segment);
                    }
                }
            }
        }

        // Ensure we have an active segment
        if self.active_segment.is_none() {
            self.create_new_active_segment()?;
        }

        let new_earliest = self.earliest_offset();
        info!(
            topic = %self.topic,
            partition = self.id,
            old_earliest = current_earliest,
            new_earliest,
            "Delete records complete"
        );

        Ok(new_earliest)
    }

    /// Get the last offset that matches the given leader epoch
    ///
    /// This is used for log divergence detection. When a follower connects
    /// to a new leader, it needs to find the point where their logs diverge.
    ///
    /// Currently returns the LEO as a simple implementation.
    /// A full implementation would track leader epochs per record.
    pub fn last_offset_for_epoch(&self, _leader_epoch: i32) -> i64 {
        // Simple implementation: return LEO
        // A full implementation would track epochs and return the last
        // offset written under the given epoch
        self.log_end_offset()
    }

    /// Get a reference to sealed segments for compaction
    ///
    /// This is used by the compaction logic to read segments that can be compacted.
    /// The active segment is not included as it's still being written to.
    pub fn get_sealed_segments(&self) -> &VecDeque<Segment> {
        &self.sealed_segments
    }

    /// Replace compacted segments with new compacted segment
    ///
    /// This method removes the specified old segments and adds the new compacted segment.
    /// Returns the number of segments removed.
    pub fn replace_with_compacted_segment(
        &mut self,
        old_segment_count: usize,
        new_segment: Segment,
    ) -> Result<usize> {
        // Remove the old segments from the front
        let mut removed = 0;
        for _ in 0..old_segment_count {
            if let Some(old_segment) = self.sealed_segments.pop_front() {
                // Delete the old segment file
                old_segment.delete()?;
                removed += 1;
            } else {
                break;
            }
        }

        // Add the new compacted segment at the front
        self.sealed_segments.push_front(new_segment);

        Ok(removed)
    }
}
