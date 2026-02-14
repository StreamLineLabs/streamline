//! Virtual table mapping from Streamline topics to SQLite tables
//!
//! Each Streamline topic is materialized as a regular SQLite table with the
//! following fixed schema:
//!
//! ```sql
//! CREATE TABLE "<topic_name>" (
//!     "offset"    INTEGER NOT NULL,
//!     partition   INTEGER NOT NULL,
//!     timestamp   TEXT NOT NULL,         -- ISO-8601 datetime
//!     key         TEXT,                  -- UTF-8 or Base64 for binary keys
//!     value       TEXT,                  -- UTF-8 or Base64 for binary values
//!     headers     TEXT                   -- JSON object of headers
//! );
//! ```
//!
//! Because `value` is stored as TEXT, SQLite's built-in `json_extract` function
//! works out of the box for JSON-encoded messages.

use crate::storage::TopicManager;
use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// Maximum records to read per partition in a single sync batch.
const SYNC_BATCH_SIZE: usize = 10_000;

/// Maps Streamline topics to SQLite tables.
pub struct TopicTableMapper<'a> {
    topic_manager: &'a Arc<TopicManager>,
}

impl<'a> TopicTableMapper<'a> {
    /// Create a new mapper referencing the given topic manager.
    pub fn new(topic_manager: &'a Arc<TopicManager>) -> Self {
        Self { topic_manager }
    }

    /// Ensure the SQLite table for the given topic exists, then load any new
    /// records since the last sync.
    ///
    /// Returns the number of new records inserted.
    pub fn sync_topic_to_table(
        &self,
        conn: &Connection,
        topic: &str,
        sync_state: &mut HashMap<String, HashMap<i32, i64>>,
    ) -> crate::Result<usize> {
        // Escape the topic name for use in SQL identifiers.
        let escaped = topic.replace('"', "\"\"");

        // Create the table if it does not already exist.
        let create_sql = format!(
            r#"CREATE TABLE IF NOT EXISTS "{escaped}" (
                "offset"    INTEGER NOT NULL,
                partition   INTEGER NOT NULL,
                timestamp   INTEGER NOT NULL,
                key         TEXT,
                value       TEXT,
                headers     TEXT
            )"#
        );
        conn.execute_batch(&create_sql).map_err(|e| {
            crate::error::StreamlineError::Storage(format!(
                "Failed to create SQLite table for topic '{}': {}",
                topic, e
            ))
        })?;

        // Create index for common query patterns (idempotent).
        let idx_sql = format!(
            r#"CREATE INDEX IF NOT EXISTS "idx_{escaped}_offset" ON "{escaped}" ("offset")"#
        );
        conn.execute_batch(&idx_sql).ok(); // best-effort

        let idx_ts_sql = format!(
            r#"CREATE INDEX IF NOT EXISTS "idx_{escaped}_timestamp" ON "{escaped}" (timestamp)"#
        );
        conn.execute_batch(&idx_ts_sql).ok();

        // Determine partition count.
        let metadata = self.topic_manager.get_topic_metadata(topic)?;
        let num_partitions = metadata.num_partitions;

        let topic_offsets = sync_state.entry(topic.to_string()).or_default();
        let mut total_inserted = 0usize;

        for partition in 0..num_partitions {
            let last_synced = topic_offsets.get(&partition).copied().unwrap_or(0);
            let latest = self.topic_manager.latest_offset(topic, partition)?;

            if latest <= last_synced {
                continue; // nothing new
            }

            // Read records from the topic.
            let records = self
                .topic_manager
                .read(topic, partition, last_synced, SYNC_BATCH_SIZE)?;

            if records.is_empty() {
                continue;
            }

            let insert_sql = format!(
                r#"INSERT INTO "{escaped}" ("offset", partition, timestamp, key, value, headers) VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#
            );
            let mut stmt = conn.prepare_cached(&insert_sql).map_err(|e| {
                crate::error::StreamlineError::Storage(format!(
                    "Failed to prepare INSERT for topic '{}': {}",
                    topic, e
                ))
            })?;

            let mut max_offset = last_synced;

            for record in &records {
                let key_str = record.key.as_ref().map(bytes_to_string);
                let value_str = bytes_to_string(&record.value);
                let headers_json = headers_to_json(&record.headers);

                stmt.execute(rusqlite::params![
                    record.offset,
                    partition,
                    record.timestamp,
                    key_str,
                    value_str,
                    headers_json,
                ])
                .map_err(|e| {
                    crate::error::StreamlineError::Storage(format!(
                        "Failed to insert record into SQLite table '{}': {}",
                        topic, e
                    ))
                })?;

                if record.offset + 1 > max_offset {
                    max_offset = record.offset + 1;
                }
            }

            total_inserted += records.len();
            topic_offsets.insert(partition, max_offset);

            debug!(
                topic = %topic,
                partition = partition,
                records = records.len(),
                next_offset = max_offset,
                "Synced partition to SQLite"
            );
        }

        Ok(total_inserted)
    }
}

/// Convert Bytes to a UTF-8 string, falling back to Base64 for non-UTF-8 data.
fn bytes_to_string(b: &bytes::Bytes) -> String {
    match std::str::from_utf8(b) {
        Ok(s) => s.to_string(),
        Err(_) => base64::engine::general_purpose::STANDARD.encode(b),
    }
}

/// Serialize record headers to a JSON object string.
fn headers_to_json(headers: &[crate::storage::record::Header]) -> String {
    use serde_json::json;

    if headers.is_empty() {
        return "{}".to_string();
    }

    let mut map = serde_json::Map::new();
    for h in headers {
        let val = match std::str::from_utf8(&h.value) {
            Ok(s) => json!(s),
            Err(_) => json!(base64::engine::general_purpose::STANDARD.encode(&h.value)),
        };
        map.insert(h.key.clone(), val);
    }
    serde_json::Value::Object(map).to_string()
}

use base64::Engine;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_to_string_utf8() {
        let b = bytes::Bytes::from("hello world");
        assert_eq!(bytes_to_string(&b), "hello world");
    }

    #[test]
    fn test_bytes_to_string_binary() {
        let b = bytes::Bytes::from(vec![0xFF, 0xFE, 0x00]);
        let result = bytes_to_string(&b);
        // Should be Base64 encoded.
        assert!(!result.is_empty());
        assert!(result.chars().all(|c| c.is_ascii()));
    }

    #[test]
    fn test_headers_to_json_empty() {
        let headers: Vec<crate::storage::record::Header> = vec![];
        assert_eq!(headers_to_json(&headers), "{}");
    }

    #[test]
    fn test_headers_to_json_with_values() {
        use crate::storage::record::Header;
        let headers = vec![
            Header {
                key: "content-type".to_string(),
                value: bytes::Bytes::from("application/json"),
            },
            Header {
                key: "trace-id".to_string(),
                value: bytes::Bytes::from("abc-123"),
            },
        ];
        let json_str = headers_to_json(&headers);
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["content-type"], "application/json");
        assert_eq!(parsed["trace-id"], "abc-123");
    }
}
