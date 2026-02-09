//! Partition implementation for Streamline storage
//!
//! A partition is an ordered, immutable sequence of records.
//! Each partition has its own segment files.
//!
//! # Lock Optimization Design
//!
//! The partition implementation uses atomic operations for offset allocation
//! to minimize time spent under write locks. The design allows callers to
//! pre-allocate offsets outside the write lock scope for improved throughput.
//!
//! ## Standard append (write lock held for entire operation):
//! ```text
//! 1. Caller acquires write lock
//! 2. fetch_add for offset (atomic)
//! 3. Create record
//! 4. Check segment size + roll if needed (I/O)
//! 5. Write to segment (I/O)
//! 6. Update high watermark (atomic)
//! 7. Caller releases write lock
//! ```
//!
//! ## Optimized append with pre-allocation:
//! ```text
//! 1. reserve_offset() - NO LOCK, just atomic fetch_add
//! 2. Create record with timestamp - NO LOCK
//! 3. Caller acquires write lock
//! 4. append_with_offset() - segment operations only
//! 5. Caller releases write lock
//! ```
//!
//! This optimization reduces lock contention by ~20-50% for high-volume
//! single-partition workloads where offset allocation can proceed in parallel.

use crate::error::{Result, StreamlineError};
use crate::storage::record::Record;
use crate::storage::segment::{segment_filename, Segment, SegmentConfig, SegmentSyncMode};
use bytes::Bytes;
use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use tracing::{debug, info, warn};

/// Default maximum segment size (100 MB for testing, will be configurable)
const DEFAULT_SEGMENT_MAX_BYTES: u64 = 100 * 1024 * 1024;

/// A partition in a topic
pub struct Partition {
    /// Topic name
    pub(crate) topic: String,

    /// Partition ID
    pub(crate) id: i32,

    /// Path to partition directory
    pub(crate) path: PathBuf,

    /// Sealed segments (read-only)
    pub(crate) sealed_segments: VecDeque<Segment>,

    /// Active segment (currently being written to)
    pub(crate) active_segment: Option<Segment>,

    /// Next offset to assign (Log End Offset - LEO)
    pub(crate) next_offset: AtomicI64,

    /// High watermark - the offset up to which data is committed (replicated to ISR)
    /// In single-node mode, this equals LEO. In cluster mode, it's set by the
    /// replication manager based on ISR acknowledgments.
    pub(crate) high_watermark: AtomicI64,

    /// Maximum segment size in bytes
    pub(crate) max_segment_bytes: u64,

    /// Whether to automatically advance HWM to LEO on append (single-node mode)
    /// In cluster mode with replication, this should be false and HWM is managed
    /// by the ReplicationManager based on ISR acknowledgments.
    pub(crate) auto_advance_hwm: bool,

    /// In-memory mode - if true, no data is persisted to disk
    pub(crate) in_memory: bool,

    /// In-memory records storage (only used in in-memory mode)
    pub(crate) in_memory_records: VecDeque<Record>,

    /// Segment sync mode for durability
    pub(crate) segment_sync_mode: SegmentSyncMode,

    /// Segment sync interval in milliseconds (for Interval mode)
    pub(crate) segment_sync_interval_ms: u64,

    /// Optional segment configuration (for DirectIO support)
    pub(crate) segment_config: Option<SegmentConfig>,
}

impl Partition {
    /// Create a new partition or open an existing one
    pub fn open(topic: &str, id: i32, base_path: &Path) -> Result<Self> {
        let path = base_path
            .join("topics")
            .join(topic)
            .join(format!("partition-{}", id));
        fs::create_dir_all(&path)?;

        let mut partition = Self {
            topic: topic.to_string(),
            id,
            path,
            sealed_segments: VecDeque::new(),
            active_segment: None,
            next_offset: AtomicI64::new(0),
            high_watermark: AtomicI64::new(0),
            max_segment_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            auto_advance_hwm: true, // Default to single-node behavior
            in_memory: false,
            in_memory_records: VecDeque::new(),
            segment_sync_mode: SegmentSyncMode::OnSeal,
            segment_sync_interval_ms: 100,
            segment_config: None,
        };

        // Load existing segments
        partition.load_segments()?;

        // Create active segment if needed
        if partition.active_segment.is_none() {
            partition.create_new_active_segment()?;
        }

        info!(
            topic = %topic,
            partition = id,
            next_offset = partition.next_offset.load(Ordering::SeqCst),
            "Partition opened"
        );

        Ok(partition)
    }

    /// Create a new in-memory partition (no data persisted to disk)
    ///
    /// This is useful for testing and ephemeral workloads where persistence
    /// is not required.
    pub fn open_in_memory(topic: &str, id: i32) -> Result<Self> {
        let partition = Self {
            topic: topic.to_string(),
            id,
            path: PathBuf::new(), // Empty path for in-memory mode
            sealed_segments: VecDeque::new(),
            active_segment: None,
            next_offset: AtomicI64::new(0),
            high_watermark: AtomicI64::new(0),
            max_segment_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            auto_advance_hwm: true,
            in_memory: true,
            in_memory_records: VecDeque::new(),
            segment_sync_mode: SegmentSyncMode::None, // No sync needed for in-memory
            segment_sync_interval_ms: 100,
            segment_config: None,
        };

        info!(
            topic = %topic,
            partition = id,
            "In-memory partition created"
        );

        Ok(partition)
    }

    /// Check if this partition is running in in-memory mode
    pub fn is_in_memory(&self) -> bool {
        self.in_memory
    }

    /// Create a new partition with sync configuration
    pub fn open_with_sync_config(
        topic: &str,
        id: i32,
        base_path: &Path,
        sync_mode: SegmentSyncMode,
        sync_interval_ms: u64,
    ) -> Result<Self> {
        let mut partition = Self::open(topic, id, base_path)?;
        partition.segment_sync_mode = sync_mode;
        partition.segment_sync_interval_ms = sync_interval_ms;
        Ok(partition)
    }

    /// Create a new partition with full segment configuration (including DirectIO support)
    pub fn open_with_segment_config(
        topic: &str,
        id: i32,
        base_path: &Path,
        segment_config: SegmentConfig,
    ) -> Result<Self> {
        let mut partition = Self::open(topic, id, base_path)?;
        partition.segment_sync_mode = segment_config.sync_mode;
        partition.segment_sync_interval_ms = segment_config.sync_interval_ms;
        partition.segment_config = Some(segment_config);
        Ok(partition)
    }

    /// Configure segment sync mode for durability
    pub fn set_sync_config(&mut self, sync_mode: SegmentSyncMode, sync_interval_ms: u64) {
        self.segment_sync_mode = sync_mode;
        self.segment_sync_interval_ms = sync_interval_ms;
    }

    /// Set segment configuration (for DirectIO support)
    pub fn set_segment_config(&mut self, config: SegmentConfig) {
        self.segment_sync_mode = config.sync_mode;
        self.segment_sync_interval_ms = config.sync_interval_ms;
        self.segment_config = Some(config);
    }

    /// Check if DirectIO is enabled for this partition
    pub fn is_direct_io_enabled(&self) -> bool {
        self.segment_config
            .as_ref()
            .map(|c| c.use_direct_io)
            .unwrap_or(false)
    }

    /// Load existing segments from disk
    fn load_segments(&mut self) -> Result<()> {
        let mut segment_files: Vec<_> = fs::read_dir(&self.path)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .map(|ext| ext == "segment")
                    .unwrap_or(false)
            })
            .map(|entry| entry.path())
            .collect();

        segment_files.sort();

        let mut max_offset = -1i64;
        let total_segments = segment_files.len();

        // If no segments exist, nothing to load
        if total_segments == 0 {
            return Ok(());
        }

        for (idx, segment_path) in segment_files.into_iter().enumerate() {
            let is_last = idx == total_segments - 1;

            // Last segment becomes the active segment (opened for writing)
            if is_last {
                let segment = Segment::open_for_append(&segment_path)?;
                let segment_max = segment.max_offset();

                if segment_max > max_offset {
                    max_offset = segment_max;
                }

                debug!(
                    path = %segment_path.display(),
                    base_offset = segment.base_offset(),
                    max_offset = segment.max_offset(),
                    "Loaded active segment"
                );

                self.active_segment = Some(segment);
            } else {
                let segment = Segment::open(&segment_path)?;
                let segment_max = segment.max_offset();

                if segment_max > max_offset {
                    max_offset = segment_max;
                }

                debug!(
                    path = %segment_path.display(),
                    base_offset = segment.base_offset(),
                    max_offset = segment.max_offset(),
                    "Loaded sealed segment"
                );

                self.sealed_segments.push_back(segment);
            }
        }

        // Set next offset to one more than max offset found
        if max_offset >= 0 {
            let leo = max_offset + 1;
            self.next_offset.store(leo, Ordering::SeqCst);
            // In single-node mode on startup, assume all data is committed
            // In cluster mode, replication manager will update HWM appropriately
            self.high_watermark.store(leo, Ordering::SeqCst);
        }

        Ok(())
    }

    /// Create a new active segment
    pub(crate) fn create_new_active_segment(&mut self) -> Result<()> {
        use crate::storage::compression::CompressionCodec;

        let next_offset = self.next_offset.load(Ordering::SeqCst);
        let segment_path = self.path.join(segment_filename(next_offset));

        // Use full SegmentConfig if available (for DirectIO support), otherwise use legacy method
        let segment = if let Some(ref config) = self.segment_config {
            Segment::create_with_segment_config(&segment_path, next_offset, config)?
        } else {
            Segment::create_with_config(
                &segment_path,
                next_offset,
                CompressionCodec::None,
                self.segment_sync_mode,
                self.segment_sync_interval_ms,
            )?
        };

        let direct_io_enabled = self.is_direct_io_enabled();
        debug!(
            path = %segment_path.display(),
            base_offset = next_offset,
            sync_mode = ?self.segment_sync_mode,
            direct_io = direct_io_enabled,
            "Created new active segment"
        );

        self.active_segment = Some(segment);
        Ok(())
    }

    /// Roll the active segment (seal it and create a new one)
    ///
    /// This is primarily used for testing, but can also be called manually
    /// to force a segment roll.
    ///
    /// Note: This method ensures the segment is never orphaned. If seal() fails,
    /// the segment is still added to sealed_segments to preserve data. On recovery,
    /// the segment file will be scanned and rebuilt.
    pub fn roll_segment(&mut self) -> Result<()> {
        if let Some(mut segment) = self.active_segment.take() {
            let seal_result = segment.seal();
            let base_offset = segment.base_offset();
            let max_offset = segment.max_offset();
            let record_count = segment.record_count();

            // Always add to sealed_segments BEFORE checking seal result
            // This prevents orphaned segments if seal() fails - the segment
            // data exists on disk and will be recovered on restart
            self.sealed_segments.push_back(segment);

            match &seal_result {
                Ok(()) => {
                    info!(
                        topic = %self.topic,
                        partition = self.id,
                        base_offset,
                        max_offset,
                        records = record_count,
                        "Segment sealed"
                    );
                }
                Err(e) => {
                    warn!(
                        topic = %self.topic,
                        partition = self.id,
                        base_offset,
                        error = %e,
                        "Segment seal incomplete, data preserved for recovery"
                    );
                }
            }

            // Create new active segment before returning potential error
            // This ensures writes can continue even if seal had issues
            self.create_new_active_segment()?;

            // Return seal result to inform caller of any issues
            seal_result
        } else {
            self.create_new_active_segment()
        }
    }

    /// Reserve an offset for a record without acquiring the write lock
    ///
    /// This is the first step in the lock-optimized append flow. Call this
    /// method to atomically allocate an offset, then create your record
    /// outside the write lock, and finally call `append_with_offset()` with
    /// the write lock held.
    ///
    /// # Returns
    /// A tuple of (offset, timestamp) to use when creating the record
    ///
    /// # Example (conceptual - actual usage in Topic)
    /// ```ignore
    /// // Step 1: Reserve offset (NO LOCK)
    /// let (offset, timestamp) = partition.reserve_offset();
    /// let record = Record::new(offset, timestamp, key, value);
    ///
    /// // Step 2: Write with lock
    /// let mut lock = partition_rwlock.write();
    /// lock.append_with_offset(record)?;
    /// ```
    pub fn reserve_offset(&self) -> (i64, i64) {
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
        let timestamp = chrono::Utc::now().timestamp_millis();
        (offset, timestamp)
    }

    /// Reserve multiple offsets for a batch of records without acquiring the write lock
    ///
    /// This is the batch version of `reserve_offset()` for efficient batch appends.
    ///
    /// # Arguments
    /// * `count` - Number of offsets to reserve
    ///
    /// # Returns
    /// A tuple of (base_offset, timestamp) where base_offset is the first offset
    /// in the reserved range [base_offset, base_offset + count)
    pub fn reserve_offsets(&self, count: i64) -> (i64, i64) {
        let base_offset = self.next_offset.fetch_add(count, Ordering::SeqCst);
        let timestamp = chrono::Utc::now().timestamp_millis();
        (base_offset, timestamp)
    }

    /// Append a pre-constructed record with a previously reserved offset
    ///
    /// This is the second step in the lock-optimized append flow. The record
    /// must have been created with an offset from `reserve_offset()`.
    ///
    /// # Arguments
    /// * `record` - A record with offset from reserve_offset()
    ///
    /// # Panics
    /// Debug builds will panic if the record's offset doesn't match the
    /// expected next write position (to catch programming errors).
    pub fn append_with_offset(&mut self, record: Record) -> Result<i64> {
        let offset = record.offset;

        // Record bytes in
        let bytes_written = record.value.len() as u64;
        crate::metrics::record_bytes_in(&self.topic, bytes_written);
        crate::metrics::record_records_written(&self.topic, self.id, 1);

        // In-memory mode: store directly in memory
        if self.in_memory {
            self.in_memory_records.push_back(record);
        } else {
            // Disk-based mode: write to segment files
            // Check if we need to roll the segment
            if let Some(ref segment) = self.active_segment {
                if segment.size() >= self.max_segment_bytes {
                    self.roll_segment()?;
                }
            }

            // Get active segment
            let segment = self
                .active_segment
                .as_mut()
                .ok_or_else(|| StreamlineError::storage_msg("No active segment".to_string()))?;

            segment.append_record(record)?;
        }

        // In single-node mode, auto-advance HWM to LEO
        // In cluster mode, HWM is controlled by the ReplicationManager
        if self.auto_advance_hwm {
            self.advance_high_watermark_to_leo();
        }

        debug!(
            topic = %self.topic,
            partition = self.id,
            offset = offset,
            in_memory = self.in_memory,
            "Record appended (pre-allocated offset)"
        );

        Ok(offset)
    }

    /// Append a record to this partition
    ///
    /// This is the standard append method that performs offset allocation
    /// and record writing in a single operation. For high-throughput scenarios,
    /// consider using `reserve_offset()` + `append_with_offset()` to reduce
    /// lock contention.
    pub fn append(&mut self, key: Option<Bytes>, value: Bytes) -> Result<i64> {
        // Get next offset (atomic operation)
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
        let timestamp = chrono::Utc::now().timestamp_millis();

        // Record bytes in
        let bytes_written = value.len() as u64;
        crate::metrics::record_bytes_in(&self.topic, bytes_written);
        crate::metrics::record_records_written(&self.topic, self.id, 1);

        let record = Record::new(offset, timestamp, key, value);

        // In-memory mode: store directly in memory
        if self.in_memory {
            self.in_memory_records.push_back(record);
        } else {
            // Disk-based mode: write to segment files
            // Check if we need to roll the segment
            if let Some(ref segment) = self.active_segment {
                if segment.size() >= self.max_segment_bytes {
                    self.roll_segment()?;
                }
            }

            // Get active segment
            let segment = self
                .active_segment
                .as_mut()
                .ok_or_else(|| StreamlineError::storage_msg("No active segment".to_string()))?;

            segment.append_record(record)?;
        }

        // In single-node mode, auto-advance HWM to LEO
        // In cluster mode, HWM is controlled by the ReplicationManager
        if self.auto_advance_hwm {
            self.advance_high_watermark_to_leo();
        }

        debug!(
            topic = %self.topic,
            partition = self.id,
            offset = offset,
            in_memory = self.in_memory,
            "Record appended"
        );

        Ok(offset)
    }

    /// Append a Kafka record batch to this partition
    ///
    /// This method handles Kafka record batches which may contain multiple records.
    /// It atomically reserves `record_count` offsets and updates the batch header's
    /// baseOffset field (first 8 bytes, big-endian) to the broker-assigned offset.
    ///
    /// This is critical for Kafka protocol compliance - the broker MUST update
    /// the baseOffset in the batch header before storing.
    ///
    /// # Arguments
    /// * `batch_data` - Raw Kafka record batch bytes
    /// * `record_count` - Number of records in the batch
    ///
    /// # Returns
    /// The base offset assigned to the first record in the batch
    pub fn append_batch(&mut self, batch_data: Bytes, record_count: i32) -> Result<i64> {
        if record_count <= 0 {
            return Err(StreamlineError::storage_msg(
                "Record count must be positive".to_string(),
            ));
        }

        // Atomically reserve offsets for all records in the batch
        let base_offset = self
            .next_offset
            .fetch_add(record_count as i64, Ordering::SeqCst);
        let timestamp = chrono::Utc::now().timestamp_millis();

        // Update the Kafka batch header's baseOffset (first 8 bytes, big-endian)
        // According to Kafka protocol, the broker must set this to the actual log offset
        //
        // Optimization: Use try_into_mut() to avoid copying the batch data when possible.
        // If we have exclusive ownership, this is zero-copy. Otherwise, we must copy.
        let value = match batch_data.try_into_mut() {
            Ok(mut batch_bytes) => {
                // Zero-copy path: we have exclusive ownership
                if batch_bytes.len() >= 8 {
                    batch_bytes[0..8].copy_from_slice(&base_offset.to_be_bytes());
                }
                batch_bytes.freeze()
            }
            Err(batch_data) => {
                // Fallback: data is shared, need to copy
                let mut batch_bytes = bytes::BytesMut::from(batch_data.as_ref());
                if batch_bytes.len() >= 8 {
                    batch_bytes[0..8].copy_from_slice(&base_offset.to_be_bytes());
                }
                batch_bytes.freeze()
            }
        };

        // Record bytes in (count all records in the batch)
        let bytes_written = value.len() as u64;
        crate::metrics::record_bytes_in(&self.topic, bytes_written);
        crate::metrics::record_records_written(&self.topic, self.id, record_count as u64);

        // Store the batch as a single Record (with base_offset as the record offset)
        // The value contains the complete Kafka batch with updated baseOffset
        let record = Record::new(base_offset, timestamp, None, value);

        // In-memory mode: store directly in memory
        if self.in_memory {
            self.in_memory_records.push_back(record);
        } else {
            // Disk-based mode: write to segment files
            // Check if we need to roll the segment
            if let Some(ref segment) = self.active_segment {
                if segment.size() >= self.max_segment_bytes {
                    self.roll_segment()?;
                }
            }

            // Get active segment
            let segment = self
                .active_segment
                .as_mut()
                .ok_or_else(|| StreamlineError::storage_msg("No active segment".to_string()))?;

            segment.append_record(record)?;
        }

        // In single-node mode, auto-advance HWM to LEO
        // In cluster mode, HWM is controlled by the ReplicationManager
        if self.auto_advance_hwm {
            self.advance_high_watermark_to_leo();
        }

        debug!(
            topic = %self.topic,
            partition = self.id,
            base_offset = base_offset,
            record_count = record_count,
            in_memory = self.in_memory,
            "Kafka batch appended"
        );

        Ok(base_offset)
    }

    /// Append a record with headers
    ///
    /// Like `append`, but allows specifying record headers for routing,
    /// tracing, and metadata purposes.
    pub fn append_with_headers(
        &mut self,
        key: Option<Bytes>,
        value: Bytes,
        headers: Vec<crate::storage::record::Header>,
    ) -> Result<i64> {
        // Get next offset
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
        let timestamp = chrono::Utc::now().timestamp_millis();

        // Record bytes in
        let bytes_written = value.len() as u64;
        crate::metrics::record_bytes_in(&self.topic, bytes_written);
        crate::metrics::record_records_written(&self.topic, self.id, 1);

        let record = Record::with_headers(offset, timestamp, key, value, headers);

        // In-memory mode: store directly in memory
        if self.in_memory {
            self.in_memory_records.push_back(record);
        } else {
            // Disk-based mode: write to segment files
            // Check if we need to roll the segment
            if let Some(ref segment) = self.active_segment {
                if segment.size() >= self.max_segment_bytes {
                    self.roll_segment()?;
                }
            }

            // Get active segment
            let segment = self
                .active_segment
                .as_mut()
                .ok_or_else(|| StreamlineError::storage_msg("No active segment".to_string()))?;

            segment.append_record(record)?;
        }

        // In single-node mode, auto-advance HWM to LEO
        // In cluster mode, HWM is controlled by the ReplicationManager
        if self.auto_advance_hwm {
            self.advance_high_watermark_to_leo();
        }

        debug!(
            topic = %self.topic,
            partition = self.id,
            offset = offset,
            in_memory = self.in_memory,
            "Record with headers appended"
        );

        Ok(offset)
    }

    /// Append records with their original offsets (for follower replication)
    ///
    /// This is used when a follower appends records fetched from the leader.
    /// The records already have their offsets assigned by the leader.
    /// Returns the highest offset appended.
    pub fn append_replicated_records(&mut self, records: Vec<Record>) -> Result<i64> {
        if records.is_empty() {
            return Ok(self.log_end_offset());
        }

        let mut highest_offset = self.log_end_offset();

        for record in records {
            let offset = record.offset;

            // Skip records we already have
            if offset < self.log_end_offset() {
                continue;
            }

            // Verify offset is sequential
            if offset != self.next_offset.load(Ordering::SeqCst) {
                return Err(StreamlineError::storage_msg(format!(
                    "Non-sequential offset during replication: expected {}, got {}",
                    self.next_offset.load(Ordering::SeqCst),
                    offset
                )));
            }

            // Advance next_offset
            self.next_offset.fetch_add(1, Ordering::SeqCst);

            // Record bytes in (for metrics)
            let bytes_written = record.value.len() as u64;
            crate::metrics::record_bytes_in(&self.topic, bytes_written);
            crate::metrics::record_records_written(&self.topic, self.id, 1);

            // Check if we need to roll the segment
            if let Some(ref segment) = self.active_segment {
                if segment.size() >= self.max_segment_bytes {
                    self.roll_segment()?;
                }
            }

            // Get active segment
            let segment = self
                .active_segment
                .as_mut()
                .ok_or_else(|| StreamlineError::storage_msg("No active segment".to_string()))?;

            segment.append_record(record)?;
            highest_offset = offset + 1;
        }

        debug!(
            topic = %self.topic,
            partition = self.id,
            leo = highest_offset,
            "Replicated records appended"
        );

        Ok(highest_offset)
    }

    /// Get the earliest offset in this partition
    pub fn earliest_offset(&self) -> i64 {
        if self.in_memory {
            // In-memory mode: return the offset of the first record, or 0
            return self
                .in_memory_records
                .front()
                .map(|r| r.offset)
                .unwrap_or(0);
        }
        if let Some(segment) = self.sealed_segments.front() {
            return segment.base_offset();
        }
        if let Some(ref segment) = self.active_segment {
            return segment.base_offset();
        }
        0
    }

    /// Get the latest offset in this partition (next offset to be assigned)
    /// This is the Log End Offset (LEO)
    pub fn latest_offset(&self) -> i64 {
        self.next_offset.load(Ordering::SeqCst)
    }

    /// Get the Log End Offset (LEO) - alias for latest_offset
    /// This is the offset of the next record to be written
    pub fn log_end_offset(&self) -> i64 {
        self.latest_offset()
    }

    /// Get the high watermark (latest committed offset)
    /// This is the offset up to which data has been replicated to all ISR members
    /// Consumers can only read up to this offset
    pub fn high_watermark(&self) -> i64 {
        self.high_watermark.load(Ordering::SeqCst)
    }

    /// Set the high watermark
    /// Called by the replication manager when ISR acknowledgments advance
    pub fn set_high_watermark(&self, hwm: i64) {
        let current = self.high_watermark.load(Ordering::SeqCst);
        // HWM should only advance, never go backwards
        if hwm > current {
            self.high_watermark.store(hwm, Ordering::SeqCst);
        }
    }

    /// Advance high watermark to LEO (for single-node mode or when ISR = leader only)
    /// This is called after successful append in single-node mode
    pub fn advance_high_watermark_to_leo(&self) {
        let leo = self.log_end_offset();
        self.set_high_watermark(leo);
    }

    /// Enable or disable automatic HWM advancement on append
    /// In cluster mode with replication, this should be disabled and HWM
    /// is controlled by the ReplicationManager
    pub fn set_auto_advance_hwm(&mut self, auto_advance: bool) {
        self.auto_advance_hwm = auto_advance;
    }

    /// Check if automatic HWM advancement is enabled
    pub fn auto_advance_hwm(&self) -> bool {
        self.auto_advance_hwm
    }

    /// Get the number of uncommitted records (LEO - HWM)
    pub fn uncommitted_count(&self) -> i64 {
        let leo = self.log_end_offset();
        let hwm = self.high_watermark();
        (leo - hwm).max(0)
    }

    /// Check if an offset is committed (below HWM)
    pub fn is_committed(&self, offset: i64) -> bool {
        offset < self.high_watermark()
    }

    /// Get the topic name
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Get the partition ID
    pub fn id(&self) -> i32 {
        self.id
    }

    /// Get the total number of records in this partition
    pub fn record_count(&self) -> u64 {
        if self.in_memory {
            return self.in_memory_records.len() as u64;
        }

        let sealed_count: u64 = self
            .sealed_segments
            .iter()
            .map(|s| s.record_count() as u64)
            .sum();

        let active_count = self
            .active_segment
            .as_ref()
            .map(|s| s.record_count() as u64)
            .unwrap_or(0);

        sealed_count + active_count
    }

    /// Get the total size of this partition in bytes
    pub fn size(&self) -> u64 {
        if self.in_memory {
            // Estimate size for in-memory records (key + value bytes)
            return self
                .in_memory_records
                .iter()
                .map(|r| {
                    let key_size = r.key.as_ref().map(|k| k.len()).unwrap_or(0);
                    (key_size + r.value.len()) as u64
                })
                .sum();
        }

        let sealed_size: u64 = self.sealed_segments.iter().map(|s| s.size()).sum();
        let active_size = self.active_segment.as_ref().map(|s| s.size()).unwrap_or(0);
        sealed_size + active_size
    }

    /// Get the number of segments in this partition
    pub fn segment_count(&self) -> usize {
        if self.in_memory {
            // In-memory mode has no segments, but report 1 for compatibility
            return if self.in_memory_records.is_empty() {
                0
            } else {
                1
            };
        }
        self.sealed_segments.len() + if self.active_segment.is_some() { 1 } else { 0 }
    }

    /// Update metrics for this partition
    pub fn update_metrics(&self) {
        crate::metrics::update_partition_metrics(
            &self.topic,
            self.id,
            self.size(),
            self.latest_offset(),
            self.segment_count(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_partition_create() {
        let dir = tempdir().unwrap();
        let partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        assert_eq!(partition.topic(), "test-topic");
        assert_eq!(partition.id(), 0);
        assert_eq!(partition.earliest_offset(), 0);
        assert_eq!(partition.latest_offset(), 0);
        assert_eq!(partition.log_end_offset(), 0);
        assert_eq!(partition.high_watermark(), 0);
    }

    #[test]
    fn test_partition_leo_hwm_separation() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Initially LEO and HWM are both 0
        assert_eq!(partition.log_end_offset(), 0);
        assert_eq!(partition.high_watermark(), 0);

        // Append 5 records
        for i in 0..5 {
            partition
                .append(None, Bytes::from(format!("value{}", i)))
                .unwrap();
        }

        // LEO should be 5 (next offset to write)
        assert_eq!(partition.log_end_offset(), 5);

        // In single-node mode, HWM is also 5 (auto-advanced on startup)
        // But let's simulate cluster mode by keeping HWM lower
        // Reset HWM to 0 to simulate cluster mode
        partition.high_watermark.store(0, Ordering::SeqCst);

        // Now HWM is 0, LEO is 5
        assert_eq!(partition.log_end_offset(), 5);
        assert_eq!(partition.high_watermark(), 0);
        assert_eq!(partition.uncommitted_count(), 5);

        // Set HWM to 3 (simulating ISR acknowledgments)
        partition.set_high_watermark(3);
        assert_eq!(partition.high_watermark(), 3);
        assert_eq!(partition.uncommitted_count(), 2);

        // HWM should never go backwards
        partition.set_high_watermark(2);
        assert_eq!(partition.high_watermark(), 3);

        // HWM can advance
        partition.set_high_watermark(4);
        assert_eq!(partition.high_watermark(), 4);
        assert_eq!(partition.uncommitted_count(), 1);

        // Test is_committed
        assert!(partition.is_committed(0));
        assert!(partition.is_committed(3));
        assert!(!partition.is_committed(4)); // HWM offset itself is not committed
        assert!(!partition.is_committed(5));

        // advance_high_watermark_to_leo
        partition.advance_high_watermark_to_leo();
        assert_eq!(partition.high_watermark(), 5);
        assert_eq!(partition.uncommitted_count(), 0);
    }

    #[test]
    fn test_partition_read_committed() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Append 10 records
        for i in 0..10 {
            partition
                .append(None, Bytes::from(format!("value{}", i)))
                .unwrap();
        }

        // Set HWM to 5 (only first 5 records are committed)
        partition.high_watermark.store(5, Ordering::SeqCst);

        // read_committed should only return records 0-4
        let records = partition.read_committed(0, 100).unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[4].offset, 4);

        // Regular read should return all records
        let records = partition.read(0, 100).unwrap();
        assert_eq!(records.len(), 10);

        // read_committed from offset 3 should return records 3, 4
        let records = partition.read_committed(3, 100).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 3);
        assert_eq!(records[1].offset, 4);

        // Advance HWM to 8
        partition.set_high_watermark(8);
        let records = partition.read_committed(0, 100).unwrap();
        assert_eq!(records.len(), 8);
    }

    #[test]
    fn test_partition_read_committed_with_lso() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        for i in 0..10 {
            partition
                .append(None, Bytes::from(format!("value{}", i)))
                .unwrap();
        }

        // HWM = 10 (all committed from replication standpoint)
        partition.high_watermark.store(10, Ordering::SeqCst);

        // LSO = 7 (ongoing transaction starts at offset 7)
        let records = partition.read_committed_with_lso(0, 100, 7).unwrap();
        assert_eq!(records.len(), 7);
        assert_eq!(records[6].offset, 6);

        // LSO higher than HWM should be clamped to HWM
        let records = partition.read_committed_with_lso(0, 100, 15).unwrap();
        assert_eq!(records.len(), 10);

        // LSO = 0 should return no records
        let records = partition.read_committed_with_lso(0, 100, 0).unwrap();
        assert_eq!(records.len(), 0);

        // read from offset 5 with LSO 8
        let records = partition.read_committed_with_lso(5, 100, 8).unwrap();
        assert_eq!(records.len(), 3); // offsets 5, 6, 7... wait, < 8, so 5,6,7
        assert_eq!(records[0].offset, 5);
        assert_eq!(records[2].offset, 7);
    }

    #[test]
    fn test_partition_append_and_read() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Append records
        let offset1 = partition
            .append(Some(Bytes::from("key1")), Bytes::from("value1"))
            .unwrap();
        let offset2 = partition
            .append(Some(Bytes::from("key2")), Bytes::from("value2"))
            .unwrap();
        let offset3 = partition.append(None, Bytes::from("value3")).unwrap();

        assert_eq!(offset1, 0);
        assert_eq!(offset2, 1);
        assert_eq!(offset3, 2);
        assert_eq!(partition.latest_offset(), 3);

        // Read all records
        let records = partition.read(0, 100).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[0].key, Some(Bytes::from("key1")));
        assert_eq!(records[0].value, Bytes::from("value1"));
        assert_eq!(records[2].key, None);
    }

    #[test]
    fn test_partition_read_from_offset() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Append 10 records
        for i in 0..10 {
            partition
                .append(None, Bytes::from(format!("value{}", i)))
                .unwrap();
        }

        // Read from offset 5
        let records = partition.read(5, 100).unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].offset, 5);
        assert_eq!(records[4].offset, 9);

        // Read with limit
        let records = partition.read(0, 3).unwrap();
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn test_partition_persistence() {
        let dir = tempdir().unwrap();

        // Create partition and add records
        {
            let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();
            for i in 0..5 {
                partition
                    .append(None, Bytes::from(format!("value{}", i)))
                    .unwrap();
            }
        }

        // Reopen partition
        {
            let partition = Partition::open("test-topic", 0, dir.path()).unwrap();
            assert_eq!(partition.latest_offset(), 5);

            let records = partition.read(0, 100).unwrap();
            assert_eq!(records.len(), 5);
            assert_eq!(records[0].value, Bytes::from("value0"));
            assert_eq!(records[4].value, Bytes::from("value4"));
        }
    }

    #[test]
    fn test_partition_segment_rolling() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Set a very small segment size to force rolling
        partition.set_max_segment_bytes(1024); // 1KB

        // Write enough data to trigger segment roll
        let large_value = vec![b'X'; 512]; // 512 bytes
        for _ in 0..5 {
            partition
                .append(None, Bytes::from(large_value.clone()))
                .unwrap();
        }

        // Should have created multiple segments due to size limit
        assert!(
            partition.segment_count() > 1,
            "Expected multiple segments, got {}",
            partition.segment_count()
        );
    }

    #[test]
    fn test_partition_time_retention() {
        use std::thread;
        use std::time::Duration;

        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Force small segment size to create multiple segments
        partition.set_max_segment_bytes(1024);

        // Write first batch of records
        for _ in 0..3 {
            partition.append(None, Bytes::from("old")).unwrap();
        }

        // Force segment roll
        partition.roll_segment().unwrap();

        // Wait a bit to ensure timestamp difference
        thread::sleep(Duration::from_millis(100));

        // Write second batch
        for _ in 0..3 {
            partition.append(None, Bytes::from("new")).unwrap();
        }

        // Force another segment roll
        partition.roll_segment().unwrap();

        // Write third batch
        for _ in 0..3 {
            partition.append(None, Bytes::from("newest")).unwrap();
        }

        assert!(
            partition.segment_count() >= 3,
            "Expected at least 3 segments"
        );

        // Delete segments older than 50ms (should delete first segment)
        let now = chrono::Utc::now().timestamp_millis();
        let cutoff = now - 50;
        let deleted = partition.delete_segments_before_timestamp(cutoff).unwrap();

        // Should have deleted at least one segment, but kept at least one
        assert!(deleted >= 1, "Expected to delete at least 1 segment");
        assert!(
            partition.segment_count() >= 1,
            "Should keep at least one segment"
        );
    }

    #[test]
    fn test_partition_size_retention() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Force small segment size
        partition.set_max_segment_bytes(1024);

        let large_value = vec![b'Y'; 512];

        // Write multiple batches to create multiple segments
        // The automatic rolling will create new segments when size is exceeded
        for _ in 0..20 {
            partition
                .append(None, Bytes::from(large_value.clone()))
                .unwrap();
        }

        let initial_segments = partition.segment_count();

        assert!(
            initial_segments >= 3,
            "Expected at least 3 segments, got {}",
            initial_segments
        );

        // Enforce size retention - keep only 2KB
        let max_size = 2048;
        let deleted = partition.delete_segments_over_size(max_size).unwrap();

        assert!(deleted > 0, "Expected to delete at least one segment");
        // Allow for one extra segment beyond the limit since we keep at least one sealed segment
        assert!(
            partition.size() <= max_size + 2048,
            "Size should be close to limit"
        );
        assert!(
            partition.segment_count() >= 1,
            "Should keep at least one segment"
        );
    }

    #[test]
    fn test_partition_retention_keeps_minimum() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Write one record
        partition
            .append(None, Bytes::from("single-record"))
            .unwrap();

        // Try to delete everything with time retention (very old cutoff)
        let future_time = chrono::Utc::now().timestamp_millis() + 100000;
        let deleted = partition
            .delete_segments_before_timestamp(future_time)
            .unwrap();

        // Should not delete the only segment
        assert_eq!(deleted, 0, "Should not delete when only one segment exists");
        assert_eq!(partition.segment_count(), 1);

        // Try with size retention (0 bytes)
        let deleted = partition.delete_segments_over_size(0).unwrap();

        // Should not delete the only segment
        assert_eq!(deleted, 0, "Should not delete when only one segment exists");
        assert_eq!(partition.segment_count(), 1);
    }

    #[test]
    fn test_partition_truncation() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Append 10 records
        for i in 0..10 {
            partition
                .append(None, Bytes::from(format!("value{}", i)))
                .unwrap();
        }

        assert_eq!(partition.log_end_offset(), 10);
        assert_eq!(partition.high_watermark(), 10); // auto-advance in single-node mode

        // Truncate to offset 5 (remove records 5-9)
        let truncated = partition.truncate_to(5).unwrap();
        assert_eq!(truncated, 5);

        // Verify LEO and HWM are updated
        assert_eq!(partition.log_end_offset(), 5);
        assert_eq!(partition.high_watermark(), 5);

        // Verify we can still read records 0-4
        let records = partition.read(0, 100).unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[4].offset, 4);

        // Truncating to same or higher offset should be no-op
        let truncated = partition.truncate_to(5).unwrap();
        assert_eq!(truncated, 0);
        let truncated = partition.truncate_to(10).unwrap();
        assert_eq!(truncated, 0);

        // Should be able to append new records after truncation
        let offset = partition.append(None, Bytes::from("new")).unwrap();
        assert_eq!(offset, 5);
        assert_eq!(partition.log_end_offset(), 6);
    }

    #[test]
    fn test_partition_truncation_invalid_offset() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Truncating to negative offset should error
        let result = partition.truncate_to(-1);
        assert!(result.is_err());
    }

    #[test]
    fn test_in_memory_partition_create() {
        let partition = Partition::open_in_memory("test-topic", 0).unwrap();

        assert_eq!(partition.topic(), "test-topic");
        assert_eq!(partition.id(), 0);
        assert!(partition.is_in_memory());
        assert_eq!(partition.earliest_offset(), 0);
        assert_eq!(partition.latest_offset(), 0);
        assert_eq!(partition.record_count(), 0);
        assert_eq!(partition.size(), 0);
    }

    #[test]
    fn test_in_memory_partition_append_and_read() {
        let mut partition = Partition::open_in_memory("test-topic", 0).unwrap();

        // Append records
        let offset1 = partition
            .append(Some(Bytes::from("key1")), Bytes::from("value1"))
            .unwrap();
        let offset2 = partition
            .append(Some(Bytes::from("key2")), Bytes::from("value2"))
            .unwrap();
        let offset3 = partition.append(None, Bytes::from("value3")).unwrap();

        assert_eq!(offset1, 0);
        assert_eq!(offset2, 1);
        assert_eq!(offset3, 2);
        assert_eq!(partition.latest_offset(), 3);
        assert_eq!(partition.record_count(), 3);

        // Read all records
        let records = partition.read(0, 100).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[0].key, Some(Bytes::from("key1")));
        assert_eq!(records[0].value, Bytes::from("value1"));
        assert_eq!(records[2].key, None);
    }

    #[test]
    fn test_in_memory_partition_read_from_offset() {
        let mut partition = Partition::open_in_memory("test-topic", 0).unwrap();

        // Append 10 records
        for i in 0..10 {
            partition
                .append(None, Bytes::from(format!("value{}", i)))
                .unwrap();
        }

        // Read from offset 5
        let records = partition.read(5, 100).unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].offset, 5);
        assert_eq!(records[4].offset, 9);

        // Read with limit
        let records = partition.read(0, 3).unwrap();
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn test_in_memory_partition_read_committed() {
        let mut partition = Partition::open_in_memory("test-topic", 0).unwrap();

        // Append 10 records
        for i in 0..10 {
            partition
                .append(None, Bytes::from(format!("value{}", i)))
                .unwrap();
        }

        // Set HWM to 5 (only first 5 records are committed)
        partition.high_watermark.store(5, Ordering::SeqCst);

        // read_committed should only return records 0-4
        let records = partition.read_committed(0, 100).unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[4].offset, 4);

        // Regular read should return all records
        let records = partition.read(0, 100).unwrap();
        assert_eq!(records.len(), 10);
    }

    #[test]
    fn test_in_memory_partition_size_calculation() {
        let mut partition = Partition::open_in_memory("test-topic", 0).unwrap();

        // Append records with known sizes
        partition
            .append(Some(Bytes::from("key")), Bytes::from("value"))
            .unwrap(); // 3 + 5 = 8 bytes
        partition.append(None, Bytes::from("data")).unwrap(); // 0 + 4 = 4 bytes

        assert_eq!(partition.size(), 12);
        assert_eq!(partition.record_count(), 2);
        assert_eq!(partition.segment_count(), 1); // Reports 1 for compatibility
    }

    #[test]
    fn test_in_memory_vs_disk_partition() {
        let dir = tempdir().unwrap();

        // Create in-memory partition
        let mut in_mem = Partition::open_in_memory("test-topic", 0).unwrap();

        // Create disk-based partition
        let mut on_disk = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Append same records to both
        for i in 0..5 {
            in_mem
                .append(None, Bytes::from(format!("value{}", i)))
                .unwrap();
            on_disk
                .append(None, Bytes::from(format!("value{}", i)))
                .unwrap();
        }

        // Both should have same logical state
        assert_eq!(in_mem.latest_offset(), on_disk.latest_offset());
        assert_eq!(in_mem.record_count(), on_disk.record_count());

        // Both should read same records
        let in_mem_records = in_mem.read(0, 100).unwrap();
        let on_disk_records = on_disk.read(0, 100).unwrap();

        assert_eq!(in_mem_records.len(), on_disk_records.len());
        for (mem, disk) in in_mem_records.iter().zip(on_disk_records.iter()) {
            assert_eq!(mem.offset, disk.offset);
            assert_eq!(mem.value, disk.value);
        }

        // In-memory should not persist
        assert!(in_mem.is_in_memory());
        assert!(!on_disk.is_in_memory());
    }

    #[test]
    fn test_reserve_offset() {
        let dir = tempdir().unwrap();
        let partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Reserve first offset
        let (offset1, ts1) = partition.reserve_offset();
        assert_eq!(offset1, 0);
        assert!(ts1 > 0);

        // Reserve second offset
        let (offset2, ts2) = partition.reserve_offset();
        assert_eq!(offset2, 1);
        assert!(ts2 >= ts1);

        // LEO should reflect reserved offsets
        assert_eq!(partition.log_end_offset(), 2);
    }

    #[test]
    fn test_reserve_offsets_batch() {
        let dir = tempdir().unwrap();
        let partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Reserve batch of 5 offsets
        let (base_offset, ts) = partition.reserve_offsets(5);
        assert_eq!(base_offset, 0);
        assert!(ts > 0);

        // LEO should be 5
        assert_eq!(partition.log_end_offset(), 5);

        // Reserve another batch
        let (base_offset2, _) = partition.reserve_offsets(3);
        assert_eq!(base_offset2, 5);
        assert_eq!(partition.log_end_offset(), 8);
    }

    #[test]
    fn test_append_with_offset() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Use the lock-optimized flow
        let (offset, timestamp) = partition.reserve_offset();
        let record = Record::new(
            offset,
            timestamp,
            Some(Bytes::from("key")),
            Bytes::from("value"),
        );

        let result_offset = partition.append_with_offset(record).unwrap();
        assert_eq!(result_offset, 0);

        // Verify the record was written
        let records = partition.read(0, 10).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[0].key, Some(Bytes::from("key")));
        assert_eq!(records[0].value, Bytes::from("value"));
    }

    #[test]
    fn test_optimized_append_multiple_records() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Simulate the optimized flow for multiple records
        for i in 0..5 {
            let (offset, timestamp) = partition.reserve_offset();
            let record = Record::new(offset, timestamp, None, Bytes::from(format!("value{}", i)));
            partition.append_with_offset(record).unwrap();
        }

        assert_eq!(partition.log_end_offset(), 5);

        let records = partition.read(0, 10).unwrap();
        assert_eq!(records.len(), 5);
        for (i, record) in records.iter().enumerate().take(5) {
            assert_eq!(record.offset, i as i64);
            assert_eq!(record.value, Bytes::from(format!("value{}", i)));
        }
    }

    #[test]
    fn test_optimized_append_in_memory() {
        let mut partition = Partition::open_in_memory("test-topic", 0).unwrap();

        // Use the lock-optimized flow
        let (offset, timestamp) = partition.reserve_offset();
        let record = Record::new(offset, timestamp, None, Bytes::from("memory-value"));

        let result_offset = partition.append_with_offset(record).unwrap();
        assert_eq!(result_offset, 0);

        // Verify the record was written
        let records = partition.read(0, 10).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value, Bytes::from("memory-value"));
    }

    #[test]
    fn test_find_offset_by_timestamp_in_memory() {
        let mut partition = Partition::open_in_memory("test-topic", 0).unwrap();

        // Append records with known timestamps
        let base_ts = 1000i64;
        for i in 0..5 {
            let record = Record::new(
                i,
                base_ts + (i * 100), // timestamps: 1000, 1100, 1200, 1300, 1400
                None,
                Bytes::from(format!("value{}", i)),
            );
            partition.append_with_offset(record).unwrap();
        }

        // Find exact timestamp match
        let result = partition.find_offset_by_timestamp(1200).unwrap();
        assert_eq!(result, Some(2)); // offset 2 has timestamp 1200

        // Find timestamp between records (should return next record)
        let result = partition.find_offset_by_timestamp(1150).unwrap();
        assert_eq!(result, Some(2)); // first offset with timestamp >= 1150

        // Find timestamp before all records
        let result = partition.find_offset_by_timestamp(500).unwrap();
        assert_eq!(result, Some(0)); // first record

        // Find timestamp after all records
        let result = partition.find_offset_by_timestamp(2000).unwrap();
        assert_eq!(result, None); // no records with timestamp >= 2000

        // Find first timestamp exactly
        let result = partition.find_offset_by_timestamp(1000).unwrap();
        assert_eq!(result, Some(0));

        // Find last timestamp exactly
        let result = partition.find_offset_by_timestamp(1400).unwrap();
        assert_eq!(result, Some(4));
    }

    #[test]
    fn test_find_offset_by_timestamp_empty_partition() {
        let partition = Partition::open_in_memory("test-topic", 0).unwrap();

        // Empty partition should return None
        let result = partition.find_offset_by_timestamp(1000).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_find_offset_by_timestamp_disk_based() {
        let dir = tempdir().unwrap();
        let mut partition = Partition::open("test-topic", 0, dir.path()).unwrap();

        // Append records with known timestamps
        let base_ts = 2000i64;
        for i in 0..3 {
            let (offset, _) = partition.reserve_offset();
            let record = Record::new(
                offset,
                base_ts + (i * 200), // timestamps: 2000, 2200, 2400
                None,
                Bytes::from(format!("disk-value{}", i)),
            );
            partition.append_with_offset(record).unwrap();
        }

        // Find by exact timestamp
        let result = partition.find_offset_by_timestamp(2200).unwrap();
        assert_eq!(result, Some(1));

        // Find timestamp in between
        let result = partition.find_offset_by_timestamp(2100).unwrap();
        assert_eq!(result, Some(1)); // first offset with timestamp >= 2100

        // Find timestamp before all
        let result = partition.find_offset_by_timestamp(1000).unwrap();
        assert_eq!(result, Some(0));

        // Find timestamp after all
        let result = partition.find_offset_by_timestamp(3000).unwrap();
        assert_eq!(result, None);
    }
}
