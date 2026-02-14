//! Core FFI functions that operate on a [`StreamlineHandle`].
//!
//! Every public function in this module is `extern "C"` with `#[no_mangle]`
//! so it can be called from C and any language with C FFI support.
//!
//! # Safety contract
//!
//! * All pointer parameters are checked for null before dereference.
//! * Panics are caught with [`std::panic::catch_unwind`] so they never
//!   cross the FFI boundary.
//! * The last error message is stored in thread-local storage and can be
//!   retrieved with [`streamline_last_error`].

use super::types::{
    BackingRecord, StreamlineConfig, StreamlineErrorCode, StreamlineHandle, StreamlineRecord,
    StreamlineRecordBatch,
};
use crate::embedded::{EmbeddedConfig, EmbeddedStreamline};
use bytes::Bytes;
use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;

// ---------------------------------------------------------------------------
// Thread-local last-error storage
// ---------------------------------------------------------------------------

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/// Store an error message in thread-local storage so the caller can
/// retrieve it with [`streamline_last_error`].
fn set_last_error(msg: &str) {
    let c =
        CString::new(msg).unwrap_or_else(|_| CString::new("(error contained null byte)").unwrap_or_default());
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = Some(c);
    });
}

/// Map a `StreamlineError` to an FFI error code and store its description.
fn map_err(err: &crate::error::StreamlineError) -> i32 {
    set_last_error(&err.to_string());
    StreamlineErrorCode::from_streamline_error(err) as i32
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Safely convert a `*const c_char` to `&str`. Returns `Err` if null or
/// not valid UTF-8, and sets the last error accordingly.
unsafe fn cstr_to_str<'a>(ptr: *const c_char) -> Result<&'a str, i32> {
    if ptr.is_null() {
        set_last_error("null string pointer");
        return Err(StreamlineErrorCode::InvalidArgument as i32);
    }
    unsafe { CStr::from_ptr(ptr) }.to_str().map_err(|_| {
        set_last_error("invalid UTF-8 in string argument");
        StreamlineErrorCode::InvalidArgument as i32
    })
}

/// Dereference a handle pointer, returning `&EmbeddedStreamline`.
unsafe fn deref_handle<'a>(handle: *mut StreamlineHandle) -> Result<&'a EmbeddedStreamline, i32> {
    if handle.is_null() {
        set_last_error("null handle");
        return Err(StreamlineErrorCode::InvalidArgument as i32);
    }
    Ok(unsafe { (*handle).as_ref() })
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Open a Streamline instance with the given configuration.
///
/// Returns a handle on success, or null on failure. Call
/// [`streamline_last_error`] for details when null is returned.
///
/// # Safety
///
/// `config` must be a valid pointer to a [`StreamlineConfig`].
#[no_mangle]
pub extern "C" fn streamline_open(config: *const StreamlineConfig) -> *mut StreamlineHandle {
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        if config.is_null() {
            set_last_error("null config pointer");
            return std::ptr::null_mut();
        }
        let cfg = unsafe { &*config };

        let embedded_cfg = if cfg.in_memory != 0 {
            EmbeddedConfig::in_memory().with_partitions(cfg.default_partitions.max(1) as u32)
        } else {
            let data_dir = match unsafe { cstr_to_str(cfg.data_dir) } {
                Ok(s) => s,
                Err(_) => return std::ptr::null_mut(),
            };
            EmbeddedConfig::persistent(data_dir)
                .with_partitions(cfg.default_partitions.max(1) as u32)
        };

        match EmbeddedStreamline::new(embedded_cfg) {
            Ok(instance) => StreamlineHandle::from_embedded(instance),
            Err(e) => {
                map_err(&e);
                std::ptr::null_mut()
            }
        }
    }))
    .unwrap_or_else(|_| {
        set_last_error("panic during streamline_open");
        std::ptr::null_mut()
    })
}

/// Open an in-memory Streamline instance with default settings.
///
/// Equivalent to calling [`streamline_open`] with `in_memory = 1`.
#[no_mangle]
pub extern "C" fn streamline_open_in_memory() -> *mut StreamlineHandle {
    std::panic::catch_unwind(AssertUnwindSafe(|| match EmbeddedStreamline::in_memory() {
        Ok(instance) => StreamlineHandle::from_embedded(instance),
        Err(e) => {
            map_err(&e);
            std::ptr::null_mut()
        }
    }))
    .unwrap_or_else(|_| {
        set_last_error("panic during streamline_open_in_memory");
        std::ptr::null_mut()
    })
}

/// Close a Streamline handle, freeing all associated resources.
///
/// After this call the handle pointer is invalid and must not be used.
///
/// # Safety
///
/// `handle` must be a valid pointer returned by [`streamline_open`] or
/// [`streamline_open_in_memory`], and must not have been closed already.
#[no_mangle]
pub extern "C" fn streamline_close(handle: *mut StreamlineHandle) {
    if handle.is_null() {
        return;
    }
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let outer = unsafe { Box::from_raw(handle) };
        // Drop the inner EmbeddedStreamline.
        let _ = unsafe { outer.into_inner() };
    }));
}

// ---------------------------------------------------------------------------
// Topic management
// ---------------------------------------------------------------------------

/// Create a topic with the specified number of partitions.
///
/// Returns `0` on success or a negative error code.
///
/// # Safety
///
/// `handle` and `name` must be valid, non-null pointers.
#[no_mangle]
pub extern "C" fn streamline_create_topic(
    handle: *mut StreamlineHandle,
    name: *const c_char,
    partitions: i32,
) -> i32 {
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let embedded = match unsafe { deref_handle(handle) } {
            Ok(e) => e,
            Err(code) => return code,
        };
        let topic_name = match unsafe { cstr_to_str(name) } {
            Ok(s) => s,
            Err(code) => return code,
        };
        match embedded.create_topic(topic_name, partitions) {
            Ok(()) => StreamlineErrorCode::Ok as i32,
            Err(e) => map_err(&e),
        }
    }))
    .unwrap_or_else(|_| {
        set_last_error("panic during streamline_create_topic");
        StreamlineErrorCode::InternalError as i32
    })
}

/// Delete a topic.
///
/// Returns `0` on success or a negative error code.
///
/// # Safety
///
/// `handle` and `name` must be valid, non-null pointers.
#[no_mangle]
pub extern "C" fn streamline_delete_topic(
    handle: *mut StreamlineHandle,
    name: *const c_char,
) -> i32 {
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let embedded = match unsafe { deref_handle(handle) } {
            Ok(e) => e,
            Err(code) => return code,
        };
        let topic_name = match unsafe { cstr_to_str(name) } {
            Ok(s) => s,
            Err(code) => return code,
        };
        match embedded.delete_topic(topic_name) {
            Ok(()) => StreamlineErrorCode::Ok as i32,
            Err(e) => map_err(&e),
        }
    }))
    .unwrap_or_else(|_| {
        set_last_error("panic during streamline_delete_topic");
        StreamlineErrorCode::InternalError as i32
    })
}

// ---------------------------------------------------------------------------
// Produce
// ---------------------------------------------------------------------------

/// Produce a message to a topic partition.
///
/// Returns the offset on success (>= 0) or a negative error code.
///
/// `key_ptr` may be null for a keyless message; in that case `key_len`
/// is ignored.
///
/// # Safety
///
/// All pointer parameters must be valid for their indicated lengths.
#[no_mangle]
pub extern "C" fn streamline_produce(
    handle: *mut StreamlineHandle,
    topic: *const c_char,
    partition: i32,
    key_ptr: *const u8,
    key_len: u32,
    value_ptr: *const u8,
    value_len: u32,
) -> i64 {
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let embedded = match unsafe { deref_handle(handle) } {
            Ok(e) => e,
            Err(code) => return code as i64,
        };
        let topic_name = match unsafe { cstr_to_str(topic) } {
            Ok(s) => s,
            Err(code) => return code as i64,
        };
        if value_ptr.is_null() && value_len > 0 {
            set_last_error("null value pointer with non-zero length");
            return StreamlineErrorCode::InvalidArgument as i32 as i64;
        }

        let key = if key_ptr.is_null() {
            None
        } else {
            let slice = unsafe { std::slice::from_raw_parts(key_ptr, key_len as usize) };
            Some(Bytes::copy_from_slice(slice))
        };

        let value = if value_ptr.is_null() {
            Bytes::new()
        } else {
            let slice = unsafe { std::slice::from_raw_parts(value_ptr, value_len as usize) };
            Bytes::copy_from_slice(slice)
        };

        match embedded.produce(topic_name, partition, key, value) {
            Ok(offset) => offset,
            Err(e) => map_err(&e) as i64,
        }
    }))
    .unwrap_or_else(|_| {
        set_last_error("panic during streamline_produce");
        StreamlineErrorCode::InternalError as i32 as i64
    })
}

// ---------------------------------------------------------------------------
// Consume
// ---------------------------------------------------------------------------

/// Consume records from a topic partition.
///
/// Up to `max_records` records starting from `offset` are written into
/// `out_batch`. Returns `0` on success or a negative error code.
///
/// The caller **must** free the batch with [`streamline_free_record_batch`].
///
/// # Safety
///
/// `handle`, `topic`, and `out_batch` must be valid, non-null pointers.
#[no_mangle]
pub extern "C" fn streamline_consume(
    handle: *mut StreamlineHandle,
    topic: *const c_char,
    partition: i32,
    offset: i64,
    max_records: i32,
    out_batch: *mut StreamlineRecordBatch,
) -> i32 {
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        if out_batch.is_null() {
            set_last_error("null out_batch pointer");
            return StreamlineErrorCode::InvalidArgument as i32;
        }

        let embedded = match unsafe { deref_handle(handle) } {
            Ok(e) => e,
            Err(code) => return code,
        };
        let topic_name = match unsafe { cstr_to_str(topic) } {
            Ok(s) => s,
            Err(code) => return code,
        };

        let records =
            match embedded.consume(topic_name, partition, offset, max_records.max(0) as usize) {
                Ok(r) => r,
                Err(e) => return map_err(&e),
            };

        // Build backing storage that owns the byte data.
        let mut backing: Vec<BackingRecord> = Vec::with_capacity(records.len());
        let mut c_records: Vec<StreamlineRecord> = Vec::with_capacity(records.len());

        for rec in &records {
            let key_owned = rec.key.as_ref().map(|k| k.to_vec());
            let value_owned = rec.value.to_vec();
            backing.push(BackingRecord {
                _key: key_owned,
                _value: value_owned,
            });
        }

        // Now create C records pointing into the backing data.
        for (i, rec) in records.iter().enumerate() {
            let back = &backing[i];
            let (kp, kl) = match &back._key {
                Some(k) => (k.as_ptr(), k.len() as u32),
                None => (std::ptr::null(), 0u32),
            };
            c_records.push(StreamlineRecord {
                offset: rec.offset,
                timestamp: rec.timestamp,
                key_ptr: kp,
                key_len: kl,
                value_ptr: back._value.as_ptr(),
                value_len: back._value.len() as u32,
            });
        }

        let count = c_records.len() as i32;

        // Heap-allocate so the pointers remain stable.
        let backing_box = Box::new(backing);
        let records_box = c_records.into_boxed_slice();

        let batch = unsafe { &mut *out_batch };
        batch.count = count;
        batch._backing = Box::into_raw(backing_box);
        batch.records = Box::into_raw(records_box) as *mut StreamlineRecord;

        StreamlineErrorCode::Ok as i32
    }))
    .unwrap_or_else(|_| {
        set_last_error("panic during streamline_consume");
        StreamlineErrorCode::InternalError as i32
    })
}

/// Free a record batch previously returned by [`streamline_consume`].
///
/// After this call the batch pointer contents are invalid.
///
/// # Safety
///
/// `batch` must point to a [`StreamlineRecordBatch`] that was populated
/// by [`streamline_consume`] and has not been freed yet.
#[no_mangle]
pub extern "C" fn streamline_free_record_batch(batch: *mut StreamlineRecordBatch) {
    if batch.is_null() {
        return;
    }
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let b = unsafe { &mut *batch };
        if !b._backing.is_null() {
            let _ = unsafe { Box::from_raw(b._backing) };
            b._backing = std::ptr::null_mut();
        }
        if !b.records.is_null() && b.count > 0 {
            let len = b.count as usize;
            let slice = unsafe { std::slice::from_raw_parts_mut(b.records, len) };
            let _ = unsafe { Box::from_raw(slice as *mut [StreamlineRecord]) };
            b.records = std::ptr::null_mut();
        }
        b.count = 0;
    }));
}

// ---------------------------------------------------------------------------
// Topic listing
// ---------------------------------------------------------------------------

/// List all topics.
///
/// On success, `*out_topics` is set to a heap-allocated array of
/// null-terminated C strings and `*out_count` is set to the number of
/// topics. Returns `0` on success or a negative error code.
///
/// The caller **must** free the list with [`streamline_free_topics`].
///
/// # Safety
///
/// `handle`, `out_topics`, and `out_count` must be valid, non-null pointers.
#[no_mangle]
pub extern "C" fn streamline_list_topics(
    handle: *mut StreamlineHandle,
    out_topics: *mut *mut *mut c_char,
    out_count: *mut i32,
) -> i32 {
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        if out_topics.is_null() || out_count.is_null() {
            set_last_error("null output pointer");
            return StreamlineErrorCode::InvalidArgument as i32;
        }

        let embedded = match unsafe { deref_handle(handle) } {
            Ok(e) => e,
            Err(code) => return code,
        };

        let topics = match embedded.list_topics() {
            Ok(t) => t,
            Err(e) => return map_err(&e),
        };

        let mut c_strings: Vec<*mut c_char> = Vec::with_capacity(topics.len());
        for name in &topics {
            match CString::new(name.as_str()) {
                Ok(cs) => c_strings.push(cs.into_raw()),
                Err(_) => {
                    // Free already-allocated strings on failure.
                    for ptr in &c_strings {
                        let _ = unsafe { CString::from_raw(*ptr) };
                    }
                    set_last_error("topic name contains null byte");
                    return StreamlineErrorCode::InternalError as i32;
                }
            }
        }

        let count = c_strings.len() as i32;
        let array = c_strings.into_boxed_slice();
        unsafe {
            *out_count = count;
            *out_topics = Box::into_raw(array) as *mut *mut c_char;
        }
        StreamlineErrorCode::Ok as i32
    }))
    .unwrap_or_else(|_| {
        set_last_error("panic during streamline_list_topics");
        StreamlineErrorCode::InternalError as i32
    })
}

/// Free a topic list previously returned by [`streamline_list_topics`].
///
/// # Safety
///
/// `topics` must have been returned by [`streamline_list_topics`].
#[no_mangle]
pub extern "C" fn streamline_free_topics(topics: *mut *mut c_char, count: i32) {
    if topics.is_null() || count <= 0 {
        return;
    }
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let slice = unsafe { std::slice::from_raw_parts(topics, count as usize) };
        for &ptr in slice {
            if !ptr.is_null() {
                let _ = unsafe { CString::from_raw(ptr) };
            }
        }
        // Free the outer array.
        let _ =
            unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(topics, count as usize)) };
    }));
}

// ---------------------------------------------------------------------------
// Offsets & flush
// ---------------------------------------------------------------------------

/// Get the latest offset (next offset to be written) for a partition.
///
/// Returns the offset (>= 0) on success or a negative error code.
///
/// # Safety
///
/// `handle` and `topic` must be valid, non-null pointers.
#[no_mangle]
pub extern "C" fn streamline_latest_offset(
    handle: *mut StreamlineHandle,
    topic: *const c_char,
    partition: i32,
) -> i64 {
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let embedded = match unsafe { deref_handle(handle) } {
            Ok(e) => e,
            Err(code) => return code as i64,
        };
        let topic_name = match unsafe { cstr_to_str(topic) } {
            Ok(s) => s,
            Err(code) => return code as i64,
        };
        match embedded.latest_offset(topic_name, partition) {
            Ok(off) => off,
            Err(e) => map_err(&e) as i64,
        }
    }))
    .unwrap_or_else(|_| {
        set_last_error("panic during streamline_latest_offset");
        StreamlineErrorCode::InternalError as i32 as i64
    })
}

/// Flush all pending writes to disk.
///
/// Returns `0` on success or a negative error code.
///
/// # Safety
///
/// `handle` must be a valid, non-null pointer.
#[no_mangle]
pub extern "C" fn streamline_flush(handle: *mut StreamlineHandle) -> i32 {
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let embedded = match unsafe { deref_handle(handle) } {
            Ok(e) => e,
            Err(code) => return code,
        };
        match embedded.flush() {
            Ok(()) => StreamlineErrorCode::Ok as i32,
            Err(e) => map_err(&e),
        }
    }))
    .unwrap_or_else(|_| {
        set_last_error("panic during streamline_flush");
        StreamlineErrorCode::InternalError as i32
    })
}

// ---------------------------------------------------------------------------
// Error & version queries
// ---------------------------------------------------------------------------

/// Return a pointer to the last error message for the current thread.
///
/// The returned pointer is valid until the next FFI call on the same
/// thread. Returns null if no error has occurred.
#[no_mangle]
pub extern "C" fn streamline_last_error() -> *const c_char {
    LAST_ERROR.with(|cell| {
        let borrow = cell.borrow();
        match borrow.as_ref() {
            Some(cs) => cs.as_ptr(),
            None => std::ptr::null(),
        }
    })
}

/// Return the Streamline library version as a static null-terminated string.
#[no_mangle]
pub extern "C" fn streamline_version() -> *const c_char {
    // Include a trailing NUL in the byte literal.
    static VERSION: &[u8] = concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes();
    VERSION.as_ptr() as *const c_char
}

// ---------------------------------------------------------------------------
// Extended SDK FFI functions
// ---------------------------------------------------------------------------

/// C-compatible topic metadata
#[repr(C)]
pub struct StreamlineTopicInfo {
    /// Topic name (null-terminated)
    pub name: *mut c_char,
    /// Number of partitions
    pub partition_count: i32,
    /// Total number of records across all partitions
    pub total_records: i64,
    /// Total size in bytes
    pub size_bytes: i64,
}

/// C-compatible instance statistics
#[repr(C)]
pub struct StreamlineStats {
    /// Total topics
    pub topic_count: i32,
    /// Total partitions
    pub partition_count: i32,
    /// Total records
    pub total_records: i64,
    /// Total storage bytes
    pub total_bytes: i64,
}

/// Get topic metadata. Returns 0 on success, negative on error.
///
/// # Safety
///
/// `handle`, `topic_name`, and `info_out` must be valid pointers.
#[no_mangle]
pub extern "C" fn streamline_topic_info(
    handle: *mut StreamlineHandle,
    topic_name: *const c_char,
    info_out: *mut StreamlineTopicInfo,
) -> i32 {
    if handle.is_null() || topic_name.is_null() || info_out.is_null() {
        set_last_error("null pointer argument");
        return StreamlineErrorCode::InvalidArgument as i32;
    }

    let embedded = unsafe { (*handle).as_ref() };
    let name = match unsafe { CStr::from_ptr(topic_name) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_last_error("invalid UTF-8 in topic name");
            return StreamlineErrorCode::InvalidArgument as i32;
        }
    };

    match embedded.topic_info(name) {
        Ok(meta) => {
            let c_name = match CString::new(name) {
                Ok(s) => s,
                Err(_) => {
                    set_last_error("topic name contains null byte");
                    return StreamlineErrorCode::InvalidArgument as i32;
                }
            };
            unsafe {
                (*info_out).name = c_name.into_raw();
                (*info_out).partition_count = meta.partition_count as i32;
                (*info_out).total_records = meta.total_records;
                (*info_out).size_bytes = meta.total_size_bytes;
            }
            0
        }
        Err(e) => {
            set_last_error(&e.to_string());
            StreamlineErrorCode::StorageError as i32
        }
    }
}

/// Free a topic info's name string.
///
/// # Safety
///
/// The `name` pointer must have been produced by `streamline_topic_info`.
#[no_mangle]
pub extern "C" fn streamline_free_topic_info(info: *mut StreamlineTopicInfo) {
    if info.is_null() {
        return;
    }
    let name = unsafe { (*info).name };
    if !name.is_null() {
        unsafe {
            drop(CString::from_raw(name));
            (*info).name = std::ptr::null_mut();
        }
    }
}

/// Get overall instance statistics. Returns 0 on success, negative on error.
///
/// # Safety
///
/// `handle` and `stats_out` must be valid pointers.
#[no_mangle]
pub extern "C" fn streamline_stats(
    handle: *mut StreamlineHandle,
    stats_out: *mut StreamlineStats,
) -> i32 {
    if handle.is_null() || stats_out.is_null() {
        set_last_error("null pointer argument");
        return StreamlineErrorCode::InvalidArgument as i32;
    }

    let embedded = unsafe { (*handle).as_ref() };
    let stats = embedded.stats();

    unsafe {
        (*stats_out).topic_count = stats.topics as i32;
        (*stats_out).partition_count = stats.partitions as i32;
        (*stats_out).total_records = stats.estimated_records as i64;
        (*stats_out).total_bytes = 0; // size info available per-topic via topic_info
    }
    0
}

/// Produce multiple messages in a batch. Returns number of messages produced
/// on success, or a negative error code on failure.
///
/// # Safety
///
/// `handle`, `topic_name`, `values`, and `value_lens` must be valid pointers.
/// `values` and `value_lens` must have at least `count` elements.
#[no_mangle]
pub extern "C" fn streamline_produce_batch(
    handle: *mut StreamlineHandle,
    topic_name: *const c_char,
    partition: i32,
    values: *const *const u8,
    value_lens: *const u32,
    count: i32,
) -> i64 {
    if handle.is_null() || topic_name.is_null() || values.is_null() || value_lens.is_null() {
        set_last_error("null pointer argument");
        return StreamlineErrorCode::InvalidArgument as i64;
    }
    if count <= 0 {
        return 0;
    }

    let embedded = unsafe { (*handle).as_ref() };
    let topic = match unsafe { CStr::from_ptr(topic_name) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_last_error("invalid UTF-8 in topic name");
            return StreamlineErrorCode::InvalidArgument as i64;
        }
    };

    let mut produced = 0i64;
    for i in 0..count as usize {
        let val_ptr = unsafe { *values.add(i) };
        let val_len = unsafe { *value_lens.add(i) } as usize;
        if val_ptr.is_null() {
            continue;
        }
        let value = unsafe { std::slice::from_raw_parts(val_ptr, val_len) };

        match embedded.produce(topic, partition, None, bytes::Bytes::copy_from_slice(value)) {
            Ok(_offset) => produced += 1,
            Err(e) => {
                set_last_error(&e.to_string());
                if produced == 0 {
                    return StreamlineErrorCode::StorageError as i64;
                }
                return produced;
            }
        }
    }

    produced
}

/// Get the partition count for a topic. Returns partition count on success,
/// negative error code on failure.
///
/// # Safety
///
/// `handle` and `topic_name` must be valid pointers.
#[no_mangle]
pub extern "C" fn streamline_partition_count(
    handle: *mut StreamlineHandle,
    topic_name: *const c_char,
) -> i32 {
    if handle.is_null() || topic_name.is_null() {
        set_last_error("null pointer argument");
        return StreamlineErrorCode::InvalidArgument as i32;
    }

    let embedded = unsafe { (*handle).as_ref() };
    let name = match unsafe { CStr::from_ptr(topic_name) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_last_error("invalid UTF-8 in topic name");
            return StreamlineErrorCode::InvalidArgument as i32;
        }
    };

    match embedded.topic_info(name) {
        Ok(meta) => meta.partition_count as i32,
        Err(e) => {
            set_last_error(&e.to_string());
            StreamlineErrorCode::TopicNotFound as i32
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn test_open_in_memory_and_close() {
        let handle = streamline_open_in_memory();
        assert!(!handle.is_null());
        streamline_close(handle);
    }

    #[test]
    fn test_open_with_config() {
        let dir = CString::new("/tmp/streamline_ffi_test").unwrap();
        let cfg = StreamlineConfig {
            data_dir: dir.as_ptr(),
            default_partitions: 2,
            in_memory: 1, // use in-memory so we don't need a real dir
        };
        let handle = streamline_open(&cfg);
        assert!(!handle.is_null());
        streamline_close(handle);
    }

    #[test]
    fn test_open_null_config() {
        let handle = streamline_open(std::ptr::null());
        assert!(handle.is_null());
    }

    #[test]
    fn test_close_null_is_safe() {
        streamline_close(std::ptr::null_mut());
    }

    #[test]
    fn test_create_and_list_topics() {
        let handle = streamline_open_in_memory();
        assert!(!handle.is_null());

        let name = CString::new("ffi-test-topic").unwrap();
        let rc = streamline_create_topic(handle, name.as_ptr(), 2);
        assert_eq!(rc, 0);

        let mut topics_ptr: *mut *mut c_char = std::ptr::null_mut();
        let mut count: i32 = 0;
        let rc = streamline_list_topics(handle, &mut topics_ptr, &mut count);
        assert_eq!(rc, 0);
        assert!(count >= 1);

        // Find our topic in the list.
        let found = (0..count).any(|i| {
            let cs = unsafe { CStr::from_ptr(*topics_ptr.add(i as usize)) };
            cs.to_str().unwrap() == "ffi-test-topic"
        });
        assert!(found);

        streamline_free_topics(topics_ptr, count);
        streamline_close(handle);
    }

    #[test]
    fn test_produce_and_consume() {
        let handle = streamline_open_in_memory();
        assert!(!handle.is_null());

        let topic = CString::new("produce-test").unwrap();
        let rc = streamline_create_topic(handle, topic.as_ptr(), 1);
        assert_eq!(rc, 0);

        // Produce a message.
        let value = b"hello ffi";
        let offset = streamline_produce(
            handle,
            topic.as_ptr(),
            0,
            std::ptr::null(),
            0,
            value.as_ptr(),
            value.len() as u32,
        );
        assert_eq!(offset, 0);

        // Consume it back.
        let mut batch = StreamlineRecordBatch {
            records: std::ptr::null_mut(),
            count: 0,
            _backing: std::ptr::null_mut(),
        };
        let rc = streamline_consume(handle, topic.as_ptr(), 0, 0, 10, &mut batch);
        assert_eq!(rc, 0);
        assert_eq!(batch.count, 1);

        let rec = unsafe { &*batch.records };
        assert_eq!(rec.offset, 0);
        let val = unsafe { std::slice::from_raw_parts(rec.value_ptr, rec.value_len as usize) };
        assert_eq!(val, b"hello ffi");
        assert!(rec.key_ptr.is_null());
        assert_eq!(rec.key_len, 0);

        streamline_free_record_batch(&mut batch);
        streamline_close(handle);
    }

    #[test]
    fn test_produce_with_key() {
        let handle = streamline_open_in_memory();
        let topic = CString::new("key-test").unwrap();
        streamline_create_topic(handle, topic.as_ptr(), 1);

        let key = b"my-key";
        let value = b"my-value";
        let offset = streamline_produce(
            handle,
            topic.as_ptr(),
            0,
            key.as_ptr(),
            key.len() as u32,
            value.as_ptr(),
            value.len() as u32,
        );
        assert_eq!(offset, 0);

        let mut batch = StreamlineRecordBatch {
            records: std::ptr::null_mut(),
            count: 0,
            _backing: std::ptr::null_mut(),
        };
        streamline_consume(handle, topic.as_ptr(), 0, 0, 10, &mut batch);
        assert_eq!(batch.count, 1);

        let rec = unsafe { &*batch.records };
        let k = unsafe { std::slice::from_raw_parts(rec.key_ptr, rec.key_len as usize) };
        assert_eq!(k, b"my-key");

        streamline_free_record_batch(&mut batch);
        streamline_close(handle);
    }

    #[test]
    fn test_delete_topic() {
        let handle = streamline_open_in_memory();
        let name = CString::new("del-topic").unwrap();
        streamline_create_topic(handle, name.as_ptr(), 1);

        let rc = streamline_delete_topic(handle, name.as_ptr());
        assert_eq!(rc, 0);

        streamline_close(handle);
    }

    #[test]
    fn test_latest_offset() {
        let handle = streamline_open_in_memory();
        let topic = CString::new("offset-test").unwrap();
        streamline_create_topic(handle, topic.as_ptr(), 1);

        let off = streamline_latest_offset(handle, topic.as_ptr(), 0);
        assert_eq!(off, 0);

        let value = b"data";
        streamline_produce(
            handle,
            topic.as_ptr(),
            0,
            std::ptr::null(),
            0,
            value.as_ptr(),
            value.len() as u32,
        );
        let off = streamline_latest_offset(handle, topic.as_ptr(), 0);
        assert_eq!(off, 1);

        streamline_close(handle);
    }

    #[test]
    fn test_flush() {
        let handle = streamline_open_in_memory();
        let rc = streamline_flush(handle);
        assert_eq!(rc, 0);
        streamline_close(handle);
    }

    #[test]
    fn test_last_error_initially_null() {
        // Clear any previous error.
        LAST_ERROR.with(|cell| *cell.borrow_mut() = None);
        let ptr = streamline_last_error();
        assert!(ptr.is_null());
    }

    #[test]
    fn test_version_is_not_null() {
        let ptr = streamline_version();
        assert!(!ptr.is_null());
        let version = unsafe { CStr::from_ptr(ptr) }.to_str().unwrap();
        assert!(!version.is_empty());
    }

    #[test]
    fn test_null_handle_returns_error() {
        let topic = CString::new("t").unwrap();
        let rc = streamline_create_topic(std::ptr::null_mut(), topic.as_ptr(), 1);
        assert!(rc < 0);
    }

    #[test]
    fn test_null_topic_returns_error() {
        let handle = streamline_open_in_memory();
        let rc = streamline_create_topic(handle, std::ptr::null(), 1);
        assert!(rc < 0);
        streamline_close(handle);
    }

    #[test]
    fn test_free_record_batch_null_is_safe() {
        streamline_free_record_batch(std::ptr::null_mut());
    }

    #[test]
    fn test_free_topics_null_is_safe() {
        streamline_free_topics(std::ptr::null_mut(), 0);
    }
}
