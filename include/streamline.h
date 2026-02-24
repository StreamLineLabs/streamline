/**
 * @file streamline.h
 * @brief C API for Embedded Streamline — The Redis of Streaming
 *
 * This header provides the public C API for embedding Streamline as a
 * library in any language with C FFI support (Python/ctypes, Go/cgo,
 * Java/JNI, Ruby/FFI, Node.js/ffi-napi, etc.).
 *
 * ## Quick Start (C)
 *
 * ```c
 * #include "streamline.h"
 *
 * int main() {
 *     StreamlineHandle *h = streamline_open_in_memory();
 *     streamline_create_topic(h, "events", 3);
 *     streamline_produce(h, "events", 0, "key", 3, "hello", 5);
 *
 *     StreamlineRecordBatch *batch = NULL;
 *     int count = streamline_consume(h, "events", 0, 0, 100, &batch);
 *     // ... process records ...
 *     streamline_free_record_batch(batch);
 *     streamline_close(h);
 * }
 * ```
 *
 * ## Linking
 *
 * Link against `libstreamline.so` (Linux), `libstreamline.dylib` (macOS),
 * or `streamline.dll` (Windows). Build with:
 *
 *     cargo build --release --features embedded
 *
 * @version 0.2.0
 * @license Apache-2.0
 * @see https://github.com/streamlinelabs/streamline
 */

#ifndef STREAMLINE_H
#define STREAMLINE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Opaque handle ────────────────────────────────────────────────── */

/** Opaque handle to a Streamline instance. */
typedef struct StreamlineHandle StreamlineHandle;

/* ── Configuration ────────────────────────────────────────────────── */

/** Configuration for opening a persistent Streamline instance. */
typedef struct {
    /** Directory for data storage. Must be a valid path. */
    const char *data_dir;
    /** Default number of partitions for new topics (default: 1). */
    int32_t default_partitions;
    /** If non-zero, store data in memory only (no disk persistence). */
    int32_t in_memory;
} StreamlineConfig;

/* ── Record types ─────────────────────────────────────────────────── */

/** A single record returned by consume operations. */
typedef struct {
    int64_t offset;
    int64_t timestamp;
    const uint8_t *key;
    int32_t key_len;
    const uint8_t *value;
    int32_t value_len;
} StreamlineRecord;

/** A batch of records returned by consume operations. */
typedef struct {
    StreamlineRecord *records;
    int32_t count;
} StreamlineRecordBatch;

/** Topic metadata. */
typedef struct {
    const char *name;
    int32_t partitions;
    int64_t total_messages;
    int64_t size_bytes;
} StreamlineTopicInfo;

/* ── Error codes ──────────────────────────────────────────────────── */

/** Error codes returned by Streamline FFI functions. */
enum StreamlineErrorCode {
    STREAMLINE_OK = 0,
    STREAMLINE_ERR_NULL_POINTER = -1,
    STREAMLINE_ERR_INVALID_UTF8 = -2,
    STREAMLINE_ERR_TOPIC_NOT_FOUND = -3,
    STREAMLINE_ERR_PARTITION_NOT_FOUND = -4,
    STREAMLINE_ERR_STORAGE = -5,
    STREAMLINE_ERR_CONFIG = -6,
    STREAMLINE_ERR_INTERNAL = -99,
};

/* ── Lifecycle ────────────────────────────────────────────────────── */

/**
 * Open a Streamline instance with the given configuration.
 *
 * @param config  Configuration struct. Must not be NULL.
 * @return Handle on success, NULL on failure (call streamline_last_error()).
 */
StreamlineHandle *streamline_open(const StreamlineConfig *config);

/**
 * Open an in-memory Streamline instance (no persistence).
 * Ideal for testing.
 *
 * @return Handle on success, NULL on failure.
 */
StreamlineHandle *streamline_open_in_memory(void);

/**
 * Close a Streamline instance and free all resources.
 *
 * @param handle  Handle to close. Safe to pass NULL (no-op).
 */
void streamline_close(StreamlineHandle *handle);

/* ── Topic management ─────────────────────────────────────────────── */

/**
 * Create a topic.
 *
 * @param handle      Active handle.
 * @param topic       Topic name (null-terminated).
 * @param partitions  Number of partitions (>= 1).
 * @return 0 on success, negative error code on failure.
 */
int32_t streamline_create_topic(StreamlineHandle *handle, const char *topic, int32_t partitions);

/**
 * Delete a topic.
 *
 * @param handle  Active handle.
 * @param topic   Topic name (null-terminated).
 * @return 0 on success, negative error code on failure.
 */
int32_t streamline_delete_topic(StreamlineHandle *handle, const char *topic);

/**
 * List all topics.
 *
 * @param handle  Active handle.
 * @param out     Receives an array of topic name strings.
 * @param count   Receives the number of topics.
 * @return 0 on success, negative error code on failure.
 *
 * @note Caller must free the returned array with streamline_free_topics().
 */
int32_t streamline_list_topics(StreamlineHandle *handle, char ***out, int32_t *count);

/** Free the topic name array returned by streamline_list_topics(). */
void streamline_free_topics(char **topics, int32_t count);

/**
 * Get topic metadata.
 *
 * @param handle  Active handle.
 * @param topic   Topic name.
 * @param out     Receives topic info.
 * @return 0 on success, negative error code on failure.
 *
 * @note Caller must free with streamline_free_topic_info().
 */
int32_t streamline_topic_info(StreamlineHandle *handle, const char *topic, StreamlineTopicInfo **out);

/** Free topic info returned by streamline_topic_info(). */
void streamline_free_topic_info(StreamlineTopicInfo *info);

/** Get the partition count for a topic. Returns negative on error. */
int32_t streamline_partition_count(StreamlineHandle *handle, const char *topic);

/* ── Produce ──────────────────────────────────────────────────────── */

/**
 * Produce a single record.
 *
 * @param handle     Active handle.
 * @param topic      Topic name.
 * @param partition  Target partition (0-based).
 * @param key        Key bytes (may be NULL).
 * @param key_len    Key length in bytes.
 * @param value      Value bytes (may be NULL).
 * @param value_len  Value length in bytes.
 * @return Assigned offset on success (>= 0), negative error code on failure.
 */
int64_t streamline_produce(
    StreamlineHandle *handle,
    const char *topic,
    int32_t partition,
    const uint8_t *key,
    int32_t key_len,
    const uint8_t *value,
    int32_t value_len
);

/**
 * Produce a batch of records atomically.
 *
 * @param handle     Active handle.
 * @param topic      Topic name.
 * @param partition  Target partition.
 * @param keys       Array of key pointers.
 * @param key_lens   Array of key lengths.
 * @param values     Array of value pointers.
 * @param value_lens Array of value lengths.
 * @param count      Number of records in the batch.
 * @return Base offset on success, negative error code on failure.
 */
int64_t streamline_produce_batch(
    StreamlineHandle *handle,
    const char *topic,
    int32_t partition,
    const uint8_t **keys,
    const int32_t *key_lens,
    const uint8_t **values,
    const int32_t *value_lens,
    int32_t count
);

/** Flush all pending writes to disk. */
int32_t streamline_flush(StreamlineHandle *handle);

/* ── Consume ──────────────────────────────────────────────────────── */

/**
 * Consume records from a partition.
 *
 * @param handle     Active handle.
 * @param topic      Topic name.
 * @param partition  Partition to read from.
 * @param offset     Starting offset.
 * @param max_records Maximum number of records to return.
 * @param out        Receives a pointer to the record batch.
 * @return Number of records returned (>= 0), or negative error code.
 *
 * @note Caller must free the batch with streamline_free_record_batch().
 */
int32_t streamline_consume(
    StreamlineHandle *handle,
    const char *topic,
    int32_t partition,
    int64_t offset,
    int32_t max_records,
    StreamlineRecordBatch **out
);

/** Free a record batch returned by streamline_consume(). */
void streamline_free_record_batch(StreamlineRecordBatch *batch);

/** Get the latest offset for a partition. Returns negative on error. */
int64_t streamline_latest_offset(StreamlineHandle *handle, const char *topic, int32_t partition);

/* ── Utility ──────────────────────────────────────────────────────── */

/**
 * Get the last error message.
 *
 * @return Null-terminated error string, or NULL if no error.
 *         The returned pointer is valid until the next FFI call on the same thread.
 */
const char *streamline_last_error(void);

/**
 * Get the Streamline version string.
 *
 * @return Static null-terminated version string (e.g., "0.2.0").
 */
const char *streamline_version(void);

/**
 * Get server statistics as a JSON string.
 *
 * @param handle  Active handle.
 * @return JSON string (caller must free with standard free()).
 */
char *streamline_stats(StreamlineHandle *handle);

#ifdef __cplusplus
}
#endif

#endif /* STREAMLINE_H */
