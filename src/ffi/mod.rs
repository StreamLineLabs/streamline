//! C Foreign Function Interface (FFI) for Streamline.
//!
//! This module exposes a **stable C ABI** so that Streamline can be used as an
//! embedded library from C, Python (ctypes / cffi), Go (cgo), Java (JNI / JNA),
//! Ruby (fiddle), and any other language with C FFI support.
//!
//! # Quick start (C)
//!
//! ```c
//! #include "streamline.h"
//!
//! StreamlineHandle *h = streamline_open_in_memory();
//! streamline_create_topic(h, "events", 3);
//!
//! const char *msg = "hello";
//! int64_t offset = streamline_produce(h, "events", 0,
//!                                      NULL, 0,
//!                                      (const uint8_t *)msg, 5);
//!
//! StreamlineRecordBatch batch = {0};
//! streamline_consume(h, "events", 0, 0, 100, &batch);
//!
//! for (int i = 0; i < batch.count; i++) {
//!     printf("offset=%lld value=%.*s\n",
//!            batch.records[i].offset,
//!            batch.records[i].value_len,
//!            batch.records[i].value_ptr);
//! }
//!
//! streamline_free_record_batch(&batch);
//! streamline_close(h);
//! ```
//!
//! # Error handling
//!
//! Functions that can fail return either a null pointer (for handle-returning
//! functions) or a negative [`StreamlineErrorCode`] value. In both cases the
//! caller can retrieve a human-readable error message via
//! [`streamline_last_error`], which uses thread-local storage.
//!
//! # Memory management
//!
//! * Handles returned by [`streamline_open`] / [`streamline_open_in_memory`]
//!   must be freed with [`streamline_close`].
//! * Record batches from [`streamline_consume`] must be freed with
//!   [`streamline_free_record_batch`].
//! * Topic lists from [`streamline_list_topics`] must be freed with
//!   [`streamline_free_topics`].
//!
//! # Thread safety
//!
//! A single [`StreamlineHandle`] may be shared across threads – the
//! underlying storage uses `Arc` and concurrent data structures.
//!
//! # Stability
//!
//! This module is **Experimental**. The ABI may change without notice in
//! minor versions until promoted to Beta.

pub mod handle;
pub mod types;

// Re-export the C-facing types at the `ffi` module level for convenience.
