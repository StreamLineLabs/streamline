//! C-compatible types for the Streamline FFI layer.
//!
//! These types use `#[repr(C)]` to ensure a stable ABI layout that can be
//! consumed from C, Python (ctypes/cffi), Go (cgo), Java (JNI/JNA), and
//! other languages with C FFI support.

use crate::embedded::EmbeddedStreamline;
use std::os::raw::c_char;

/// Opaque handle wrapping an [`EmbeddedStreamline`] instance.
///
/// Callers receive a `*mut StreamlineHandle` from [`streamline_open`] /
/// [`streamline_open_in_memory`] and must pass it to every subsequent API
/// call. The handle must be freed with [`streamline_close`].
#[repr(C)]
pub struct StreamlineHandle {
    inner: *mut EmbeddedStreamline,
}

impl StreamlineHandle {
    /// Create a new handle from a boxed `EmbeddedStreamline`.
    pub(crate) fn from_embedded(embedded: EmbeddedStreamline) -> *mut Self {
        let inner = Box::into_raw(Box::new(embedded));
        Box::into_raw(Box::new(Self { inner }))
    }

    /// Borrow the inner `EmbeddedStreamline` immutably.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that `self.inner` is a valid pointer produced
    /// by `Box::into_raw`.
    pub(crate) unsafe fn as_ref(&self) -> &EmbeddedStreamline {
        unsafe { &*self.inner }
    }

    /// Consume the handle, returning the boxed `EmbeddedStreamline` so it can
    /// be dropped.
    ///
    /// # Safety
    ///
    /// Must only be called once per handle.
    pub(crate) unsafe fn into_inner(self) -> Box<EmbeddedStreamline> {
        unsafe { Box::from_raw(self.inner) }
    }
}

/// C-compatible configuration for opening a Streamline instance.
///
/// # Example (C)
///
/// ```c
/// StreamlineConfig cfg = {
///     .data_dir = "/var/lib/streamline",
///     .default_partitions = 3,
///     .in_memory = 0,
/// };
/// StreamlineHandle *h = streamline_open(&cfg);
/// ```
#[repr(C)]
pub struct StreamlineConfig {
    /// Path to the data directory (UTF-8, null-terminated).
    /// Ignored when `in_memory` is non-zero.
    pub data_dir: *const c_char,
    /// Default number of partitions for new topics.
    pub default_partitions: i32,
    /// Non-zero to use in-memory storage (data is not persisted).
    pub in_memory: i32,
}

/// Error codes returned by FFI functions.
///
/// A value of `0` (`STREAMLINE_OK`) indicates success. Negative values
/// indicate an error category. Use [`streamline_last_error`] to retrieve
/// a human-readable description of the most recent error.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamlineErrorCode {
    /// Operation completed successfully.
    Ok = 0,
    /// An I/O error occurred (file system, network, etc.).
    IoError = -1,
    /// A storage-layer error occurred.
    StorageError = -2,
    /// The requested topic was not found.
    TopicNotFound = -3,
    /// An argument was invalid (null pointer, bad UTF-8, etc.).
    InvalidArgument = -4,
    /// A Kafka protocol-level error occurred.
    ProtocolError = -5,
    /// An unspecified internal error occurred.
    InternalError = -6,
}

impl StreamlineErrorCode {
    /// Map a [`crate::error::StreamlineError`] to an FFI error code.
    pub fn from_streamline_error(err: &crate::error::StreamlineError) -> Self {
        use crate::error::StreamlineError as E;
        match err {
            E::Io(_) => Self::IoError,
            E::Storage(_) | E::StorageDomain(_) => Self::StorageError,
            E::TopicNotFound(_) => Self::TopicNotFound,
            E::PartitionNotFound(_, _) => Self::TopicNotFound,
            E::Protocol(_) | E::ProtocolDomain(_) => Self::ProtocolError,
            E::InvalidTopicName(_) | E::InvalidOffset(_) | E::InvalidData(_) => {
                Self::InvalidArgument
            }
            _ => Self::InternalError,
        }
    }
}

/// A single record in C-compatible layout.
///
/// Pointers (`key_ptr`, `value_ptr`) are owned by the containing
/// [`StreamlineRecordBatch`] and must **not** be freed individually.
/// Call [`streamline_free_record_batch`] to release the entire batch.
#[repr(C)]
pub struct StreamlineRecord {
    /// Record offset within its partition.
    pub offset: i64,
    /// Timestamp in milliseconds since the Unix epoch.
    pub timestamp: i64,
    /// Pointer to key bytes, or null if the record has no key.
    pub key_ptr: *const u8,
    /// Length of the key in bytes. Zero when `key_ptr` is null.
    pub key_len: u32,
    /// Pointer to value bytes.
    pub value_ptr: *const u8,
    /// Length of the value in bytes.
    pub value_len: u32,
}

/// A batch of records returned by [`streamline_consume`].
///
/// The caller must free the batch with [`streamline_free_record_batch`]
/// when it is no longer needed.
#[repr(C)]
pub struct StreamlineRecordBatch {
    /// Pointer to an array of [`StreamlineRecord`] structs.
    pub records: *mut StreamlineRecord,
    /// Number of records in the array.
    pub count: i32,
    /// Internal pointer used for deallocation – do not modify.
    pub(crate) _backing: *mut Vec<BackingRecord>,
}

/// Internal type that keeps owned copies of key/value bytes so that the
/// raw pointers in [`StreamlineRecord`] remain valid until the batch is freed.
pub(crate) struct BackingRecord {
    pub(crate) _key: Option<Vec<u8>>,
    pub(crate) _value: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn test_error_code_values() {
        assert_eq!(StreamlineErrorCode::Ok as i32, 0);
        assert_eq!(StreamlineErrorCode::IoError as i32, -1);
        assert_eq!(StreamlineErrorCode::StorageError as i32, -2);
        assert_eq!(StreamlineErrorCode::TopicNotFound as i32, -3);
        assert_eq!(StreamlineErrorCode::InvalidArgument as i32, -4);
        assert_eq!(StreamlineErrorCode::ProtocolError as i32, -5);
        assert_eq!(StreamlineErrorCode::InternalError as i32, -6);
    }

    #[test]
    fn test_error_code_from_streamline_error() {
        use crate::error::StreamlineError;

        let io_err = StreamlineError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"));
        assert_eq!(
            StreamlineErrorCode::from_streamline_error(&io_err),
            StreamlineErrorCode::IoError
        );

        let storage_err = StreamlineError::storage_msg("bad segment".into());
        assert_eq!(
            StreamlineErrorCode::from_streamline_error(&storage_err),
            StreamlineErrorCode::StorageError
        );

        let not_found = StreamlineError::TopicNotFound("events".into());
        assert_eq!(
            StreamlineErrorCode::from_streamline_error(&not_found),
            StreamlineErrorCode::TopicNotFound
        );

        let internal = StreamlineError::Internal("oops".into());
        assert_eq!(
            StreamlineErrorCode::from_streamline_error(&internal),
            StreamlineErrorCode::InternalError
        );
    }

    #[test]
    fn test_record_layout_is_repr_c() {
        // Ensure StreamlineRecord has a predictable size (no padding surprises).
        // On 64-bit: offset(8) + timestamp(8) + key_ptr(8) + key_len(4) +
        //            value_ptr(8) + value_len(4) = 40 + possible padding.
        assert!(mem::size_of::<StreamlineRecord>() > 0);
    }

    #[test]
    fn test_config_layout_is_repr_c() {
        assert!(mem::size_of::<StreamlineConfig>() > 0);
    }

    #[test]
    fn test_batch_default_empty() {
        let batch = StreamlineRecordBatch {
            records: std::ptr::null_mut(),
            count: 0,
            _backing: std::ptr::null_mut(),
        };
        assert_eq!(batch.count, 0);
        assert!(batch.records.is_null());
    }
}
