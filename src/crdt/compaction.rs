//! CRDT-Aware Log Compaction
//!
//! Extends standard log compaction to merge CRDT records rather than
//! just keeping the latest value. This enables efficient storage while
//! preserving CRDT semantics.
//!
//! # How It Works
//!
//! Standard compaction: Keep only the latest record per key
//! CRDT compaction: Merge all records per key using CRDT merge semantics
//!
//! ```text
//! Before Compaction:
//! ┌─────────────────────────────────────────────────────────┐
//! │ Offset 1: key="counter", node1 increments 10            │
//! │ Offset 2: key="counter", node2 increments 20            │
//! │ Offset 3: key="counter", node1 increments 5             │
//! └─────────────────────────────────────────────────────────┘
//!
//! After Standard Compaction (WRONG for CRDTs):
//! ┌─────────────────────────────────────────────────────────┐
//! │ Offset 3: key="counter", node1 increments 5 (value=5)   │
//! └─────────────────────────────────────────────────────────┘
//!
//! After CRDT Compaction (CORRECT):
//! ┌─────────────────────────────────────────────────────────┐
//! │ Merged: key="counter", merged value=35 (10+20+5)        │
//! └─────────────────────────────────────────────────────────┘
//! ```

use super::record::{has_crdt_header, map_to_headers, CrdtRecord};
use super::types::{CrdtOperation, CrdtValue};
use crate::error::Result;
use crate::storage::record::{Header, Record};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Configuration for CRDT-aware compaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrdtCompactionConfig {
    /// Enable CRDT-aware compaction
    pub enabled: bool,
    /// Node ID for merged records
    pub node_id: String,
    /// Maximum records to merge in a single batch
    pub max_merge_batch_size: usize,
    /// Whether to preserve original timestamps (use max)
    pub preserve_timestamps: bool,
}

impl Default for CrdtCompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            node_id: "compactor".to_string(),
            max_merge_batch_size: 10000,
            preserve_timestamps: true,
        }
    }
}

/// Statistics from CRDT compaction
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CrdtCompactionStats {
    /// Number of CRDT records processed
    pub crdt_records_processed: u64,
    /// Number of non-CRDT records (passed through)
    pub non_crdt_records: u64,
    /// Number of successful merges
    pub merges_performed: u64,
    /// Number of merge errors (type mismatches, etc.)
    pub merge_errors: u64,
    /// Number of unique keys after compaction
    pub unique_keys: u64,
    /// Bytes saved by compaction
    pub bytes_saved: u64,
}

/// CRDT-aware log compactor
///
/// Extends standard compaction to merge CRDT records instead of
/// just keeping the latest value.
pub struct CrdtCompactor {
    config: CrdtCompactionConfig,
    stats: CrdtCompactionStats,
}

impl CrdtCompactor {
    /// Create a new CRDT compactor
    pub fn new(config: CrdtCompactionConfig) -> Self {
        Self {
            config,
            stats: CrdtCompactionStats::default(),
        }
    }

    /// Create with default configuration
    pub fn default_compactor() -> Self {
        Self::new(CrdtCompactionConfig::default())
    }

    /// Check if a record is a CRDT record
    pub fn is_crdt_record(record: &Record) -> bool {
        has_crdt_header(&record.headers)
    }

    /// Compact a batch of records
    ///
    /// This is the main entry point for CRDT-aware compaction.
    /// It groups records by key and merges CRDT records.
    pub fn compact_records(&mut self, records: Vec<Record>) -> Result<Vec<Record>> {
        if !self.config.enabled {
            return Ok(records);
        }

        let original_len = records.len();
        let original_bytes: u64 = records.iter().map(|r| r.value.len() as u64).sum();

        // Separate CRDT and non-CRDT records
        let (crdt_records, non_crdt_records): (Vec<_>, Vec<_>) =
            records.into_iter().partition(Self::is_crdt_record);

        self.stats.non_crdt_records += non_crdt_records.len() as u64;
        self.stats.crdt_records_processed += crdt_records.len() as u64;

        // Group CRDT records by key
        let mut key_groups: HashMap<Option<Bytes>, Vec<Record>> = HashMap::new();
        for record in crdt_records {
            key_groups
                .entry(record.key.clone())
                .or_default()
                .push(record);
        }

        // Merge each group
        let mut merged_records = Vec::new();
        for (key, group) in key_groups {
            match self.merge_crdt_group(&key, group) {
                Ok(Some(merged)) => {
                    merged_records.push(merged);
                    self.stats.merges_performed += 1;
                }
                Ok(None) => {
                    // All records in group were invalid
                    self.stats.merge_errors += 1;
                }
                Err(e) => {
                    warn!("CRDT merge error for key {:?}: {}", key, e);
                    self.stats.merge_errors += 1;
                }
            }
        }

        // Combine merged CRDT records with non-CRDT records
        let mut result = non_crdt_records;
        result.extend(merged_records);

        // Sort by offset to maintain order
        result.sort_by_key(|r| r.offset);

        self.stats.unique_keys = result.len() as u64;

        let final_bytes: u64 = result.iter().map(|r| r.value.len() as u64).sum();
        if original_bytes > final_bytes {
            self.stats.bytes_saved += original_bytes - final_bytes;
        }

        info!(
            original_records = original_len,
            compacted_records = result.len(),
            crdt_merges = self.stats.merges_performed,
            bytes_saved = self.stats.bytes_saved,
            "CRDT compaction completed"
        );

        Ok(result)
    }

    /// Merge a group of CRDT records with the same key
    fn merge_crdt_group(
        &self,
        key: &Option<Bytes>,
        records: Vec<Record>,
    ) -> Result<Option<Record>> {
        if records.is_empty() {
            return Ok(None);
        }

        // If only one record, return as-is
        if records.len() == 1 {
            return Ok(records.into_iter().next());
        }

        // Parse and merge CRDT values
        let mut merged_value: Option<CrdtValue> = None;
        let mut max_offset = i64::MIN;
        let mut max_timestamp = i64::MIN;
        let mut latest_headers: Vec<Header> = Vec::new();

        for record in records {
            // Parse CRDT record using Vec<Header>
            let crdt_record = match CrdtRecord::from_raw_vec(
                record.key.clone(),
                record.value.clone(),
                &record.headers,
            )? {
                Some(cr) => cr,
                None => {
                    debug!(
                        "Record at offset {} is not a valid CRDT record",
                        record.offset
                    );
                    continue;
                }
            };

            // Merge value
            match &mut merged_value {
                Some(existing) => {
                    if let Err(e) = existing.merge(&crdt_record.value) {
                        warn!("Failed to merge CRDT values: {}", e);
                        continue;
                    }
                }
                None => {
                    merged_value = Some(crdt_record.value);
                }
            }

            // Track max offset and timestamp
            if record.offset > max_offset {
                max_offset = record.offset;
                latest_headers = record.headers.clone();
            }
            if record.timestamp > max_timestamp {
                max_timestamp = record.timestamp;
            }
        }

        // Create merged record
        let merged_value = match merged_value {
            Some(v) => v,
            None => return Ok(None),
        };

        // Create new CRDT record with merged state
        let merged_crdt = CrdtRecord::new(
            key.clone(),
            merged_value,
            self.config.node_id.clone(),
            CrdtOperation::State, // Compacted records are always state
        );

        let value_bytes = merged_crdt.value_bytes()?;

        // Build headers - start with CRDT headers
        let crdt_headers_map = merged_crdt.all_headers();
        let mut final_headers_map: HashMap<String, Bytes> = crdt_headers_map;

        // Preserve any non-CRDT headers from the latest record
        for header in &latest_headers {
            if !header.key.starts_with("x-crdt-") {
                final_headers_map.insert(header.key.clone(), header.value.clone());
            }
        }

        // Convert back to Vec<Header>
        let headers = map_to_headers(&final_headers_map);

        Ok(Some(Record {
            offset: max_offset,
            timestamp: if self.config.preserve_timestamps {
                max_timestamp
            } else {
                chrono::Utc::now().timestamp_millis()
            },
            key: key.clone(),
            value: value_bytes,
            headers,
            crc: None, // CRC will be computed when writing
        }))
    }

    /// Get compaction statistics
    pub fn stats(&self) -> &CrdtCompactionStats {
        &self.stats
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = CrdtCompactionStats::default();
    }

    /// Get configuration
    pub fn config(&self) -> &CrdtCompactionConfig {
        &self.config
    }
}

impl Default for CrdtCompactor {
    fn default() -> Self {
        Self::default_compactor()
    }
}

/// Helper function to check if a record is a CRDT record
pub fn is_crdt_record(record: &Record) -> bool {
    CrdtCompactor::is_crdt_record(record)
}

/// Helper function to compact a batch of records
pub fn compact_crdt_records(
    records: Vec<Record>,
    node_id: impl Into<String>,
) -> Result<Vec<Record>> {
    let config = CrdtCompactionConfig {
        enabled: true,
        node_id: node_id.into(),
        ..Default::default()
    };
    let mut compactor = CrdtCompactor::new(config);
    compactor.compact_records(records)
}

#[cfg(test)]
mod tests {
    use super::super::record::CrdtRecordBuilder;
    use super::super::types::GCounter;
    use super::*;

    fn create_test_record(offset: i64, key: &str, node_id: &str, increment: u64) -> Record {
        let mut counter = GCounter::new(node_id);
        counter.increment(increment);

        // Convert key to owned String to avoid lifetime issues
        let key_owned = key.to_string();
        let crdt_record = CrdtRecordBuilder::new(node_id)
            .key(key_owned.clone())
            .g_counter(counter);

        let value_bytes = crdt_record.value_bytes().unwrap();
        let headers = crdt_record.all_headers_vec();

        Record {
            offset,
            timestamp: chrono::Utc::now().timestamp_millis(),
            key: Some(Bytes::from(key_owned)),
            value: value_bytes,
            headers,
            crc: None,
        }
    }

    #[test]
    fn test_crdt_compaction_basic() {
        let records = vec![
            create_test_record(1, "counter1", "node1", 10),
            create_test_record(2, "counter1", "node2", 20),
            create_test_record(3, "counter1", "node1", 5),
        ];

        let mut compactor = CrdtCompactor::default_compactor();
        let result = compactor.compact_records(records).unwrap();

        // Should have one merged record
        assert_eq!(result.len(), 1);

        // Parse and check merged value
        let merged = CrdtRecord::from_raw_vec(
            result[0].key.clone(),
            result[0].value.clone(),
            &result[0].headers,
        )
        .unwrap()
        .unwrap();

        match merged.value {
            // GCounter merge uses max-per-node: node1=max(10,5)=10, node2=20 → total=30
            CrdtValue::GCounter(c) => assert_eq!(c.value(), 30),
            _ => panic!("Expected GCounter"),
        }
    }

    #[test]
    fn test_crdt_compaction_multiple_keys() {
        let records = vec![
            create_test_record(1, "counter1", "node1", 10),
            create_test_record(2, "counter2", "node1", 100),
            create_test_record(3, "counter1", "node2", 20),
            create_test_record(4, "counter2", "node2", 200),
        ];

        let mut compactor = CrdtCompactor::default_compactor();
        let result = compactor.compact_records(records).unwrap();

        // Should have two merged records (one per key)
        assert_eq!(result.len(), 2);

        // Find counter1 and counter2
        let counter1 = result
            .iter()
            .find(|r| r.key.as_ref().map(|k| k.as_ref()) == Some(b"counter1".as_ref()))
            .unwrap();
        let counter2 = result
            .iter()
            .find(|r| r.key.as_ref().map(|k| k.as_ref()) == Some(b"counter2".as_ref()))
            .unwrap();

        // Check counter1 merged value
        let merged1 = CrdtRecord::from_raw_vec(
            counter1.key.clone(),
            counter1.value.clone(),
            &counter1.headers,
        )
        .unwrap()
        .unwrap();

        match merged1.value {
            CrdtValue::GCounter(c) => assert_eq!(c.value(), 30), // 10 + 20
            _ => panic!("Expected GCounter"),
        }

        // Check counter2 merged value
        let merged2 = CrdtRecord::from_raw_vec(
            counter2.key.clone(),
            counter2.value.clone(),
            &counter2.headers,
        )
        .unwrap()
        .unwrap();

        match merged2.value {
            CrdtValue::GCounter(c) => assert_eq!(c.value(), 300), // 100 + 200
            _ => panic!("Expected GCounter"),
        }
    }

    #[test]
    fn test_crdt_compaction_mixed_records() {
        let crdt_record = create_test_record(1, "counter", "node1", 10);

        let non_crdt_record = Record {
            offset: 2,
            timestamp: chrono::Utc::now().timestamp_millis(),
            key: Some(Bytes::from("regular-key")),
            value: Bytes::from("regular-value"),
            headers: Vec::new(),
            crc: None,
        };

        let records = vec![crdt_record, non_crdt_record];

        let mut compactor = CrdtCompactor::default_compactor();
        let result = compactor.compact_records(records).unwrap();

        // Should have both records preserved
        assert_eq!(result.len(), 2);

        // Non-CRDT record should be unchanged
        let regular = result
            .iter()
            .find(|r| r.key.as_ref().map(|k| k.as_ref()) == Some(b"regular-key".as_ref()))
            .unwrap();
        assert_eq!(regular.value, Bytes::from("regular-value"));
    }

    #[test]
    fn test_crdt_compaction_disabled() {
        let records = vec![
            create_test_record(1, "counter", "node1", 10),
            create_test_record(2, "counter", "node2", 20),
        ];

        let config = CrdtCompactionConfig {
            enabled: false,
            ..Default::default()
        };
        let mut compactor = CrdtCompactor::new(config);
        let result = compactor.compact_records(records).unwrap();

        // Should have both records (no compaction)
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_crdt_compaction_stats() {
        let records = vec![
            create_test_record(1, "counter1", "node1", 10),
            create_test_record(2, "counter1", "node2", 20),
            create_test_record(3, "counter2", "node1", 100),
        ];

        let mut compactor = CrdtCompactor::default_compactor();
        let _result = compactor.compact_records(records).unwrap();

        let stats = compactor.stats();
        assert_eq!(stats.crdt_records_processed, 3);
        assert_eq!(stats.merges_performed, 2); // Two keys = two merge operations
        assert_eq!(stats.unique_keys, 2);
    }

    #[test]
    fn test_is_crdt_record() {
        let crdt_record = create_test_record(1, "counter", "node1", 10);
        assert!(CrdtCompactor::is_crdt_record(&crdt_record));

        let non_crdt_record = Record {
            offset: 2,
            timestamp: chrono::Utc::now().timestamp_millis(),
            key: Some(Bytes::from("key")),
            value: Bytes::from("value"),
            headers: Vec::new(),
            crc: None,
        };
        assert!(!CrdtCompactor::is_crdt_record(&non_crdt_record));
    }

    #[test]
    fn test_compact_helper_function() {
        let records = vec![
            create_test_record(1, "counter", "node1", 10),
            create_test_record(2, "counter", "node2", 20),
        ];

        let result = compact_crdt_records(records, "compactor").unwrap();
        assert_eq!(result.len(), 1);
    }
}
