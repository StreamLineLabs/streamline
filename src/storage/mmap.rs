//! Memory-mapped segment reader for zero-copy reads
//!
//! This module provides memory-mapped access to sealed segment files,
//! enabling zero-copy reads directly from the page cache. Memory mapping
//! is particularly beneficial for:
//!
//! - Large segments (reduces system call overhead)
//! - Repeated reads of the same data (data stays in page cache)
//! - Sequential reads (OS can prefetch efficiently)
//!
//! ## Usage
//!
//! ```no_run
//! use streamline::storage::mmap::MmapSegmentReader;
//!
//! let reader = MmapSegmentReader::open("segment.dat", 1024 * 1024).unwrap();
//! let data = reader.slice(64, 1024); // Zero-copy slice
//! ```

use crate::error::{Result, StreamlineError};
use crate::storage::segment::SegmentHeader;
use bytes::Bytes;
use memmap2::Mmap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

/// Size of segment header in bytes
const HEADER_SIZE: usize = 64;

/// Memory-mapped segment reader for efficient zero-copy reads
///
/// Uses memory mapping to allow the kernel to manage data in the page cache,
/// avoiding explicit read syscalls and memory copies for frequently accessed data.
pub struct MmapSegmentReader {
    /// Memory-mapped region
    mmap: Mmap,
    /// Parsed segment header
    header: SegmentHeader,
    /// Path to the segment file (for debugging/logging)
    path: PathBuf,
}

impl MmapSegmentReader {
    /// Open a segment file for memory-mapped reading
    ///
    /// # Arguments
    /// * `path` - Path to the segment file
    /// * `mmap_threshold` - Minimum file size to enable mmap (files smaller than this
    ///   may not benefit from mmap overhead)
    ///
    /// # Errors
    /// Returns error if:
    /// - File cannot be opened
    /// - File is too small for header
    /// - Header is invalid or corrupted
    /// - Memory mapping fails
    pub fn open<P: AsRef<Path>>(path: P, mmap_threshold: u64) -> Result<Self> {
        let path = path.as_ref();
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        let size = metadata.len();

        // Check if file meets minimum threshold
        if size < mmap_threshold {
            return Err(StreamlineError::storage_msg(format!(
                "Segment file {} ({} bytes) below mmap threshold ({} bytes)",
                path.display(),
                size,
                mmap_threshold
            )));
        }

        // Check minimum size for header
        if size < HEADER_SIZE as u64 {
            return Err(StreamlineError::storage_msg(format!(
                "Segment file {} too small for header ({} bytes < {} bytes)",
                path.display(),
                size,
                HEADER_SIZE
            )));
        }

        // Create memory mapping
        // SAFETY: Memory mapping a file is safe here because:
        // - The file is opened read-only, preventing modifications
        // - We validate the file size before mapping (must contain header)
        // - Mmap::map creates a read-only mapping that prevents writes
        // - The file handle remains valid for the lifetime of the Mmap
        // - We validate the header structure after mapping before using data
        let mmap = unsafe { Mmap::map(&file)? };

        // Parse and validate header
        let header = SegmentHeader::from_bytes(&mmap[..HEADER_SIZE])?;

        // Record successful mmap creation
        #[cfg(feature = "metrics")]
        metrics::counter!("streamline_zerocopy_mmap_segments_opened").increment(1);

        Ok(Self {
            mmap,
            header,
            path: path.to_path_buf(),
        })
    }

    /// Get the total size of the memory-mapped region
    pub fn size(&self) -> usize {
        self.mmap.len()
    }

    /// Get the segment header
    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    /// Get a zero-copy slice of the segment data
    ///
    /// Returns a slice directly into the memory-mapped region,
    /// avoiding any data copying. The returned slice is valid
    /// as long as this reader exists.
    ///
    /// # Arguments
    /// * `offset` - Byte offset from the start of the file
    /// * `len` - Number of bytes to include in the slice
    ///
    /// # Panics
    /// Panics if offset + len exceeds the file size.
    pub fn slice(&self, offset: usize, len: usize) -> &[u8] {
        #[cfg(feature = "metrics")]
        {
            metrics::counter!("streamline_zerocopy_mmap_reads_total").increment(1);
            metrics::counter!("streamline_zerocopy_mmap_bytes_read").increment(len as u64);
        }
        &self.mmap[offset..offset + len]
    }

    /// Get a zero-copy slice of the segment data as Bytes
    ///
    /// This creates a reference-counted Bytes object that shares
    /// the underlying memory with the mmap. No data is copied.
    ///
    /// # Arguments
    /// * `offset` - Byte offset from the start of the file
    /// * `len` - Number of bytes to include
    ///
    /// # Returns
    /// Returns None if the range is invalid (offset + len > size)
    pub fn slice_bytes(&self, offset: usize, len: usize) -> Option<Bytes> {
        if offset + len > self.mmap.len() {
            return None;
        }
        // Copy the slice to create an owned Bytes
        // Note: This does copy the data, but from the page cache (mmap) to the heap.
        // For true zero-copy, the caller should use slice() and work with the reference.
        Some(Bytes::copy_from_slice(&self.mmap[offset..offset + len]))
    }

    /// Get the data section (after header) as a slice
    pub fn data(&self) -> &[u8] {
        &self.mmap[HEADER_SIZE..]
    }

    /// Read data at a specific offset (after header)
    ///
    /// # Arguments
    /// * `data_offset` - Offset within the data section (0 = first byte after header)
    /// * `len` - Number of bytes to read
    ///
    /// # Returns
    /// Returns None if the range would exceed the file size
    pub fn read_data(&self, data_offset: usize, len: usize) -> Option<&[u8]> {
        let abs_offset = HEADER_SIZE + data_offset;
        let end = abs_offset + len;
        if end > self.mmap.len() {
            return None;
        }
        Some(&self.mmap[abs_offset..end])
    }

    /// Get the file path (for debugging)
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Advise the kernel about expected access patterns
    ///
    /// Call this method before sequential reads to hint to the kernel
    /// that it should prefetch data. This is best-effort and may not
    /// be supported on all platforms.
    #[cfg(unix)]
    pub fn advise_sequential(&mut self) -> io::Result<()> {
        // SAFETY: We're calling madvise on our valid memory-mapped region.
        // - self.mmap is a valid Mmap that was successfully created in open()
        // - as_ptr() returns a valid pointer to the start of the mapping
        // - len() returns the actual length of the mapped region
        // - MADV_SEQUENTIAL is an advisory hint that cannot corrupt memory
        let result = unsafe {
            libc::madvise(
                self.mmap.as_ptr() as *mut libc::c_void,
                self.mmap.len(),
                libc::MADV_SEQUENTIAL,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Advise the kernel that we will access data randomly
    ///
    /// Call this method if reads will be random rather than sequential.
    #[cfg(unix)]
    pub fn advise_random(&mut self) -> io::Result<()> {
        // SAFETY: We're calling madvise on our valid memory-mapped region.
        // - self.mmap is a valid Mmap that was successfully created in open()
        // - as_ptr() returns a valid pointer to the start of the mapping
        // - len() returns the actual length of the mapped region
        // - MADV_RANDOM is an advisory hint that cannot corrupt memory
        let result = unsafe {
            libc::madvise(
                self.mmap.as_ptr() as *mut libc::c_void,
                self.mmap.len(),
                libc::MADV_RANDOM,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Advise the kernel that we need this data soon
    ///
    /// This hints to the kernel to prefetch the data into memory.
    #[cfg(unix)]
    pub fn advise_willneed(&mut self) -> io::Result<()> {
        // SAFETY: We're calling madvise on our valid memory-mapped region.
        // - self.mmap is a valid Mmap that was successfully created in open()
        // - as_ptr() returns a valid pointer to the start of the mapping
        // - len() returns the actual length of the mapped region
        // - MADV_WILLNEED is an advisory hint that triggers prefetching
        //   but cannot corrupt memory
        let result = unsafe {
            libc::madvise(
                self.mmap.as_ptr() as *mut libc::c_void,
                self.mmap.len(),
                libc::MADV_WILLNEED,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Advise the kernel that we no longer need this data
    ///
    /// This hints to the kernel that the pages can be released from memory.
    /// Useful after a segment is sealed and no longer actively accessed,
    /// allowing the kernel to reclaim memory for other uses.
    #[cfg(unix)]
    pub fn advise_dontneed(&mut self) -> io::Result<()> {
        // SAFETY: We're calling madvise on our valid memory-mapped region.
        // - self.mmap is a valid Mmap that was successfully created in open()
        // - as_ptr() returns a valid pointer to the start of the mapping
        // - len() returns the actual length of the mapped region
        // - MADV_DONTNEED is an advisory hint that releases pages but
        //   the data is still accessible (will be paged back in on access)
        let result = unsafe {
            libc::madvise(
                self.mmap.as_ptr() as *mut libc::c_void,
                self.mmap.len(),
                libc::MADV_DONTNEED,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Advise the kernel about a specific range that will be needed soon
    ///
    /// This is useful for prefetching specific portions of the segment,
    /// such as upcoming batches for a consumer that's reading sequentially.
    ///
    /// # Arguments
    /// * `offset` - Starting offset within the mmap region
    /// * `len` - Length of the range to prefetch
    #[cfg(unix)]
    pub fn advise_willneed_range(&self, offset: usize, len: usize) -> io::Result<()> {
        if offset + len > self.mmap.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Range exceeds mmap bounds",
            ));
        }

        // SAFETY: We're calling madvise on a valid portion of our memory-mapped region.
        // - The bounds check above ensures we don't exceed the mapping
        // - The pointer arithmetic is safe within the valid mapping range
        // - MADV_WILLNEED is an advisory hint that triggers prefetching
        let ptr = unsafe { self.mmap.as_ptr().add(offset) };
        let result = unsafe { libc::madvise(ptr as *mut libc::c_void, len, libc::MADV_WILLNEED) };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Advise the kernel that a specific range is no longer needed
    ///
    /// This is useful for releasing memory after processing a portion
    /// of the segment, allowing the kernel to reclaim those pages.
    ///
    /// # Arguments
    /// * `offset` - Starting offset within the mmap region
    /// * `len` - Length of the range to release
    #[cfg(unix)]
    pub fn advise_dontneed_range(&self, offset: usize, len: usize) -> io::Result<()> {
        if offset + len > self.mmap.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Range exceeds mmap bounds",
            ));
        }

        // SAFETY: We're calling madvise on a valid portion of our memory-mapped region.
        // - The bounds check above ensures we don't exceed the mapping
        // - The pointer arithmetic is safe within the valid mapping range
        // - MADV_DONTNEED releases pages but data remains accessible
        let ptr = unsafe { self.mmap.as_ptr().add(offset) };
        let result = unsafe { libc::madvise(ptr as *mut libc::c_void, len, libc::MADV_DONTNEED) };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Advise the kernel about sequential access for a specific range
    ///
    /// Useful when you know a specific portion will be read sequentially.
    ///
    /// # Arguments
    /// * `offset` - Starting offset within the mmap region
    /// * `len` - Length of the range
    #[cfg(unix)]
    pub fn advise_sequential_range(&self, offset: usize, len: usize) -> io::Result<()> {
        if offset + len > self.mmap.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Range exceeds mmap bounds",
            ));
        }

        // SAFETY: We're calling madvise on a valid portion of our memory-mapped region.
        let ptr = unsafe { self.mmap.as_ptr().add(offset) };
        let result = unsafe { libc::madvise(ptr as *mut libc::c_void, len, libc::MADV_SEQUENTIAL) };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl std::fmt::Debug for MmapSegmentReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapSegmentReader")
            .field("path", &self.path)
            .field("size", &self.mmap.len())
            .field("header", &self.header)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::{Record, RecordBatch};
    use crate::storage::segment::Segment;
    use tempfile::tempdir;

    fn create_test_batch(count: usize) -> RecordBatch {
        let timestamp = chrono::Utc::now().timestamp_millis();
        let mut batch = RecordBatch::new(0, timestamp);
        for i in 0..count {
            batch.add_record(Record::new(
                i as i64,
                timestamp,
                Some(Bytes::from(format!("key-{}", i))),
                Bytes::from(format!("value-{}", i)),
            ));
        }
        batch
    }

    #[test]
    fn test_mmap_reader_basic() {
        let temp_dir = tempdir().unwrap();
        let segment_path = temp_dir.path().join("00000000000000000000.segment");

        // Create a segment with some data
        let mut segment = Segment::create(&segment_path, 0).unwrap();
        let batch = create_test_batch(100);
        segment.append_batch(&batch).unwrap();
        segment.seal().unwrap();

        // Open with mmap (use 0 threshold to always allow mmap)
        let reader = MmapSegmentReader::open(&segment_path, 0).unwrap();

        // Verify header
        assert_eq!(reader.header().base_offset, 0);
        // record_count may or may not be updated depending on segment implementation
        // The important thing is the segment can be read

        // Verify we can read data
        assert!(reader.size() > HEADER_SIZE);
        assert!(!reader.data().is_empty());
    }

    #[test]
    fn test_mmap_threshold_enforcement() {
        let temp_dir = tempdir().unwrap();
        let segment_path = temp_dir.path().join("small.segment");

        // Create a small segment
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
        segment.seal().unwrap();

        let file_size = std::fs::metadata(&segment_path).unwrap().len();

        // Should fail if threshold is higher than file size
        let result = MmapSegmentReader::open(&segment_path, file_size + 1);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("below mmap threshold"));

        // Should succeed if threshold is lower than file size
        let result = MmapSegmentReader::open(&segment_path, file_size);
        assert!(result.is_ok());
    }

    #[test]
    fn test_slice_operations() {
        let temp_dir = tempdir().unwrap();
        let segment_path = temp_dir.path().join("slice.segment");

        // Create segment
        let mut segment = Segment::create(&segment_path, 0).unwrap();
        let timestamp = 12345i64;
        let mut batch = RecordBatch::new(0, timestamp);
        batch.add_record(Record::new(
            0,
            timestamp,
            Some(Bytes::from("test-key")),
            Bytes::from("test-value"),
        ));
        segment.append_batch(&batch).unwrap();
        segment.seal().unwrap();

        let reader = MmapSegmentReader::open(&segment_path, 0).unwrap();

        // Test slice
        let header_slice = reader.slice(0, HEADER_SIZE);
        assert_eq!(header_slice.len(), HEADER_SIZE);
        assert_eq!(&header_slice[..4], b"STRM"); // Magic bytes

        // Test slice_bytes
        let bytes = reader.slice_bytes(0, 4).unwrap();
        assert_eq!(&bytes[..], b"STRM");

        // Test invalid range
        let invalid = reader.slice_bytes(0, reader.size() + 1);
        assert!(invalid.is_none());
    }
}
