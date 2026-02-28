/**
 * Streamline C API
 *
 * Provides a C-compatible interface for embedding Streamline
 * in Go, Node.js, .NET, Python, and other languages via FFI.
 *
 * Usage:
 *   StreamlineHandle *h = streamline_open_in_memory();
 *   streamline_create_topic(h, "events", 3);
 *
 *   const char *msg = "hello";
 *   int64_t offset = streamline_produce(h, "events", 0,
 *                                       NULL, 0,
 *                                       (const uint8_t *)msg, 5);
 *
 *   StreamlineRecordBatch batch = {0};
 *   streamline_consume(h, "events", 0, 0, 100, &batch);
 *
 *   for (int i = 0; i < batch.count; i++) {
 *       printf("offset=%lld value=%.*s\n",
 *              batch.records[i].offset,
 *              (int)batch.records[i].value_len,
 *              batch.records[i].value_ptr);
 *   }
 *
 *   streamline_free_record_batch(&batch);
 *   streamline_close(h);
 */

#ifndef STREAMLINE_H
#define STREAMLINE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================= */
/* Types                                                                     */
/* ========================================================================= */

/** Opaque handle to a Streamline instance. */
typedef struct StreamlineHandle StreamlineHandle;

/** C-compatible configuration for opening a Streamline instance. */
typedef struct StreamlineConfig {
    const char *data_dir;          /**< Path to data directory (NULL for in-memory) */
    int32_t     default_partitions; /**< Default partitions for new topics */
    int32_t     in_memory;          /**< Non-zero for in-memory mode */
} StreamlineConfig;

/** A single record returned by streamline_consume. */
typedef struct StreamlineRecord {
    int64_t        offset;     /**< Record offset within partition */
    int64_t        timestamp;  /**< Timestamp (ms since epoch) */
    const uint8_t *key_ptr;    /**< Key bytes (NULL if no key) */
    uint32_t       key_len;    /**< Key length in bytes */
    const uint8_t *value_ptr;  /**< Value bytes */
    uint32_t       value_len;  /**< Value length in bytes */
} StreamlineRecord;

/** A batch of records returned by streamline_consume. */
typedef struct StreamlineRecordBatch {
    StreamlineRecord *records; /**< Array of records */
    int32_t           count;   /**< Number of records */
    void             *_backing; /**< Internal — do not modify */
} StreamlineRecordBatch;

/* ========================================================================= */
/* Error Codes                                                               */
/* ========================================================================= */

#define STREAMLINE_OK              0
#define STREAMLINE_ERR_IO         -1
#define STREAMLINE_ERR_STORAGE    -2
#define STREAMLINE_ERR_NOT_FOUND  -3
#define STREAMLINE_ERR_INVALID    -4
#define STREAMLINE_ERR_PROTOCOL   -5
#define STREAMLINE_ERR_INTERNAL   -6

/* ========================================================================= */
/* Lifecycle                                                                 */
/* ========================================================================= */

/**
 * Open a Streamline instance with the given configuration.
 * @return Handle on success, NULL on failure (call streamline_last_error).
 */
StreamlineHandle *streamline_open(const StreamlineConfig *config);

/**
 * Open an in-memory Streamline instance with default settings.
 * @return Handle on success, NULL on failure.
 */
StreamlineHandle *streamline_open_in_memory(void);

/**
 * Close a Streamline handle and free all resources.
 */
void streamline_close(StreamlineHandle *handle);

/* ========================================================================= */
/* Topic Management                                                          */
/* ========================================================================= */

/**
 * Create a topic with the specified number of partitions.
 * @return STREAMLINE_OK on success, negative error code on failure.
 */
int streamline_create_topic(StreamlineHandle *handle, const char *name, int32_t partitions);

/**
 * Delete a topic.
 * @return STREAMLINE_OK on success, negative error code on failure.
 */
int streamline_delete_topic(StreamlineHandle *handle, const char *name);

/**
 * Get the number of partitions for a topic.
 * @return Partition count on success, negative error code on failure.
 */
int32_t streamline_partition_count(StreamlineHandle *handle, const char *topic);

/* ========================================================================= */
/* Produce                                                                   */
/* ========================================================================= */

/**
 * Produce a single record to a topic partition.
 * @param partition Target partition number.
 * @param key       Key bytes (NULL for no key).
 * @param key_len   Key length.
 * @param value     Value bytes.
 * @param value_len Value length.
 * @return Assigned offset on success, negative error code on failure.
 */
int64_t streamline_produce(
    StreamlineHandle *handle,
    const char *topic,
    int32_t partition,
    const uint8_t *key, uint32_t key_len,
    const uint8_t *value, uint32_t value_len
);

/**
 * Flush pending writes to storage.
 * @return STREAMLINE_OK on success.
 */
int streamline_flush(StreamlineHandle *handle);

/* ========================================================================= */
/* Consume                                                                   */
/* ========================================================================= */

/**
 * Consume records from a topic partition.
 * @param partition  Partition number.
 * @param offset     Starting offset.
 * @param max_records Maximum records to return.
 * @param[out] batch Output record batch (caller must free with streamline_free_record_batch).
 * @return Number of records on success, negative error code on failure.
 */
int streamline_consume(
    StreamlineHandle *handle,
    const char *topic,
    int32_t partition,
    int64_t offset,
    int32_t max_records,
    StreamlineRecordBatch *batch
);

/**
 * Get the latest (next) offset for a topic partition.
 * @return Latest offset on success, negative error code on failure.
 */
int64_t streamline_latest_offset(StreamlineHandle *handle, const char *topic, int32_t partition);

/**
 * Free a record batch previously returned by streamline_consume.
 */
void streamline_free_record_batch(StreamlineRecordBatch *batch);

/* ========================================================================= */
/* Introspection                                                             */
/* ========================================================================= */

/**
 * Get the last error message (thread-local).
 * @return Error string, or NULL if no error. Valid until next API call.
 */
const char *streamline_last_error(void);

/**
 * Get the Streamline version string.
 * @return Version string (e.g., "0.2.0").
 */
const char *streamline_version(void);

#ifdef __cplusplus
}
#endif

#endif /* STREAMLINE_H */
