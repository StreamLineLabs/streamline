//! Sendfile-based zero-copy transfer for fetch responses
//!
//! This module provides sendfile() integration for efficient zero-copy
//! data transfer from segment files directly to network sockets,
//! bypassing user-space buffering.
//!
//! ## When sendfile() is used
//!
//! Sendfile is only used when ALL of the following conditions are met:
//! - Connection is plain TCP (not TLS)
//! - Socket file descriptor is available
//! - Segment is sealed (immutable)
//! - Platform is Unix (Linux/macOS)
//! - Zero-copy is enabled in configuration
//!
//! ## Benefits
//!
//! - **Zero-copy**: Data goes directly from disk page cache to socket buffer
//! - **No deserialization**: Segments are sent as raw bytes
//! - **Reduced CPU**: No memcpy, decompression, or parsing overhead
//! - **Better throughput**: Kernel handles the transfer efficiently
//!
//! ## Limitations
//!
//! - Only works with sealed (immutable) segments
//! - Cannot filter by offset within a batch (sends entire segment range)
//! - TLS connections always fall back to regular read+write
//! - Requires complete segments (partial batch transfers not supported)

use crate::error::{Result, StreamlineError};
use crate::protocol::connection::RawFd;
use crate::storage::segment::Segment;
use crate::storage::zerocopy::sendfile_async;
use std::fs::File;
use tracing::{debug, warn};

/// Information about a segment suitable for sendfile transfer
#[derive(Debug, Clone)]
pub struct SendfileSegmentInfo {
    /// Segment file path
    pub path: std::path::PathBuf,
    /// Starting byte offset in the file
    pub offset: u64,
    /// Number of bytes to transfer
    pub length: usize,
    /// Base offset of the segment (for logging/metrics)
    pub base_offset: i64,
}

/// Try to extract sendfile information from a sealed segment
///
/// This function checks if a segment can be transferred via sendfile
/// and returns the necessary file descriptor and byte range information.
///
/// # Arguments
/// * `segment` - The segment to read from (must be sealed)
/// * `start_offset` - Logical offset to start reading from
/// * `max_bytes` - Maximum number of bytes to transfer
///
/// # Returns
/// `Some(SendfileSegmentInfo)` if sendfile can be used, None otherwise
#[cfg(unix)]
pub fn extract_sendfile_info(
    segment: &Segment,
    start_offset: i64,
    max_bytes: usize,
) -> Option<SendfileSegmentInfo> {
    use std::os::unix::fs::MetadataExt;

    // Only sealed segments are immutable and safe for sendfile
    if !segment.is_sealed() {
        return None;
    }

    // Check if offset is in this segment's range
    if start_offset < segment.base_offset() || start_offset > segment.max_offset() {
        return None;
    }

    // For sendfile, we'll start from the beginning of the segment data (after header)
    // This is a simplification - we transfer the entire segment range
    // The Kafka protocol layer will filter records by offset
    let file_offset = 64u64; // HEADER_SIZE

    // Get segment file size to determine available bytes
    let segment_size = std::fs::metadata(segment.path()).ok()?.size();
    let available_bytes = (segment_size.saturating_sub(file_offset)) as usize;

    if available_bytes == 0 {
        return None;
    }

    let transfer_bytes = available_bytes.min(max_bytes);

    Some(SendfileSegmentInfo {
        path: segment.path().to_path_buf(),
        offset: file_offset,
        length: transfer_bytes,
        base_offset: segment.base_offset(),
    })
}

/// Non-Unix fallback (always returns None)
#[cfg(not(unix))]
pub fn extract_sendfile_info(
    _segment: &Segment,
    _start_offset: i64,
    _max_bytes: usize,
) -> Option<SendfileSegmentInfo> {
    None
}

/// Transfer segment data to a socket using sendfile()
///
/// This performs a zero-copy transfer of segment data directly to the
/// socket, bypassing user-space buffering.
///
/// # Arguments
/// * `socket_fd` - Raw file descriptor of the TCP socket
/// * `info` - Sendfile information (path, offset, length)
///
/// # Returns
/// Number of bytes transferred, or error
///
/// # Errors
/// Returns error if:
/// - File cannot be opened
/// - Sendfile syscall fails
/// - Socket is closed or invalid
#[cfg(unix)]
pub async fn sendfile_segment(socket_fd: RawFd, info: &SendfileSegmentInfo) -> Result<usize> {
    // Open the segment file for reading
    let file = File::open(&info.path)?;

    debug!(
        path = %info.path.display(),
        offset = info.offset,
        length = info.length,
        socket_fd = socket_fd,
        "Starting sendfile transfer"
    );

    // Perform sendfile in blocking task (sendfile is a blocking syscall)
    match sendfile_async(socket_fd, file, info.offset, info.length).await {
        Ok(result) => {
            debug!(
                bytes_sent = result.bytes_sent,
                requested = info.length,
                "Sendfile transfer completed"
            );

            // Record metrics
            #[cfg(feature = "metrics")]
            {
                metrics::counter!("streamline_fetch_sendfile_total").increment(1);
                metrics::counter!("streamline_fetch_sendfile_bytes")
                    .increment(result.bytes_sent as u64);
            }

            Ok(result.bytes_sent)
        }
        Err(e) => {
            warn!(
                error = %e,
                path = %info.path.display(),
                offset = info.offset,
                length = info.length,
                "Sendfile transfer failed"
            );

            // Record failure metric
            #[cfg(feature = "metrics")]
            {
                metrics::counter!("streamline_fetch_sendfile_errors").increment(1);
            }

            Err(StreamlineError::Io(e))
        }
    }
}

/// Non-Unix fallback (always errors)
#[cfg(not(unix))]
pub async fn sendfile_segment(_socket_fd: RawFd, _info: &SendfileSegmentInfo) -> Result<usize> {
    Err(StreamlineError::storage_msg(
        "sendfile not supported on this platform".to_string(),
    ))
}

/// Try to sendfile an entire segment range
///
/// This is a high-level helper that attempts to use sendfile for a
/// segment read operation. If sendfile cannot be used, it returns None
/// and the caller should fall back to regular read.
///
/// # Arguments
/// * `segment` - The segment to read from
/// * `socket_fd` - Raw socket file descriptor (if available)
/// * `start_offset` - Logical offset to start reading from
/// * `max_bytes` - Maximum bytes to transfer
///
/// # Returns
/// - `Some(Ok(bytes_sent))` if sendfile succeeded
/// - `Some(Err(error))` if sendfile was attempted but failed
/// - `None` if sendfile cannot be used (caller should use regular read)
#[cfg(unix)]
pub async fn try_sendfile_segment(
    segment: &Segment,
    socket_fd: Option<RawFd>,
    start_offset: i64,
    max_bytes: usize,
) -> Option<Result<usize>> {
    // Check if we have a socket fd
    let fd = socket_fd?;

    // Extract sendfile info from segment
    let info = extract_sendfile_info(segment, start_offset, max_bytes)?;

    // Attempt sendfile
    Some(sendfile_segment(fd, &info).await)
}

/// Non-Unix fallback (always returns None)
#[cfg(not(unix))]
pub async fn try_sendfile_segment(
    _segment: &Segment,
    _socket_fd: Option<RawFd>,
    _start_offset: i64,
    _max_bytes: usize,
) -> Option<Result<usize>> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::{Record, RecordBatch};
    use bytes::Bytes;
    use tempfile::tempdir;

    #[test]
    fn test_extract_sendfile_info_unsealed_segment() {
        let temp_dir = tempdir().unwrap();
        let segment_path = temp_dir.path().join("00000000000000000000.segment");

        // Create an unsealed (active) segment
        let mut segment = Segment::create(&segment_path, 0).unwrap();

        let timestamp = chrono::Utc::now().timestamp_millis();
        let mut batch = RecordBatch::new(0, timestamp);
        batch.add_record(Record::new(
            0,
            timestamp,
            Some(Bytes::from("key")),
            Bytes::from("value"),
        ));
        segment.append_batch(&batch).unwrap();

        // Unsealed segments should not support sendfile
        #[cfg(unix)]
        assert!(extract_sendfile_info(&segment, 0, 1024).is_none());
    }

    #[test]
    fn test_extract_sendfile_info_sealed_segment() {
        let temp_dir = tempdir().unwrap();
        let segment_path = temp_dir.path().join("00000000000000000000.segment");

        // Create and seal a segment
        let mut segment = Segment::create(&segment_path, 0).unwrap();

        let timestamp = chrono::Utc::now().timestamp_millis();
        let mut batch = RecordBatch::new(0, timestamp);
        for i in 0..10 {
            batch.add_record(Record::new(
                i,
                timestamp,
                Some(Bytes::from(format!("key-{}", i))),
                Bytes::from(format!("value-{}", i)),
            ));
        }
        segment.append_batch(&batch).unwrap();
        segment.seal().unwrap();

        // Sealed segments should support sendfile
        #[cfg(unix)]
        {
            let info = extract_sendfile_info(&segment, 0, 1024);
            assert!(info.is_some());

            if let Some(info) = info {
                assert_eq!(info.base_offset, 0);
                assert!(info.length > 0);
                assert!(info.offset >= 64); // Should be after header
            }
        }
    }

    #[test]
    fn test_extract_sendfile_info_out_of_range() {
        let temp_dir = tempdir().unwrap();
        let segment_path = temp_dir.path().join("00000000000000000000.segment");

        let mut segment = Segment::create(&segment_path, 100).unwrap();

        let timestamp = chrono::Utc::now().timestamp_millis();
        let mut batch = RecordBatch::new(100, timestamp);
        batch.add_record(Record::new(
            100,
            timestamp,
            Some(Bytes::from("key")),
            Bytes::from("value"),
        ));
        segment.append_batch(&batch).unwrap();
        segment.seal().unwrap();

        // Offset 0 is outside the segment's range (100+)
        #[cfg(unix)]
        assert!(extract_sendfile_info(&segment, 0, 1024).is_none());

        // Offset 100 should work
        #[cfg(unix)]
        assert!(extract_sendfile_info(&segment, 100, 1024).is_some());
    }
}
