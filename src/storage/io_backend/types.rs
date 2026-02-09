//! Type definitions for I/O backend abstraction.
//!
//! This module defines the core types used across all I/O backend implementations:
//!
//! - [`FileId`]: Unique identifier for open files in the I/O system
//! - [`OpenMode`]: File open modes (read, read-write, create, append)
//! - [`IoResult`]: Result type that returns both the result and the buffer
//! - [`IoStats`]: Statistics for tracking I/O operations
//!
//! # Buffer Ownership Model
//!
//! The [`IoResult`] type uses an ownership-based model where buffers are moved
//! to I/O operations and returned with the result. This is required for io_uring
//! compatibility where the kernel owns the buffer during the operation.
//!
//! ```text
//! let buf = vec![0u8; 4096];
//! let (result, buf) = file.read_at(buf, offset).await;
//! // buf is returned even if result is Err
//! ```

use crate::error::Result;

/// Unique identifier for an open file in the I/O backend
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileId(pub u64);

impl FileId {
    /// Create a new FileId
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the raw ID value
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// File open mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    /// Open for reading only
    Read,
    /// Open for reading and writing
    ReadWrite,
    /// Create new file (truncate if exists)
    Create,
    /// Open for appending
    Append,
}

/// Result type for I/O operations that returns both the result and the buffer
///
/// This ownership model is required for io_uring compatibility, where buffers
/// must be moved to the kernel and returned after the operation completes.
pub type IoResult<T> = (Result<T>, Vec<u8>);

/// Statistics for I/O operations
#[derive(Debug, Clone, Default)]
pub struct IoStats {
    /// Total number of read operations
    pub reads: u64,
    /// Total bytes read
    pub bytes_read: u64,
    /// Total number of write operations
    pub writes: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Total number of sync operations
    pub syncs: u64,
    /// Total number of failed operations
    pub errors: u64,
}

impl IoStats {
    /// Create a new IoStats
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a read operation
    pub fn record_read(&mut self, bytes: u64) {
        self.reads += 1;
        self.bytes_read += bytes;
    }

    /// Record a write operation
    pub fn record_write(&mut self, bytes: u64) {
        self.writes += 1;
        self.bytes_written += bytes;
    }

    /// Record a sync operation
    pub fn record_sync(&mut self) {
        self.syncs += 1;
    }

    /// Record an error
    pub fn record_error(&mut self) {
        self.errors += 1;
    }
}
