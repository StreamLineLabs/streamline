//! CRDT Record Wire Format
//!
//! Extends Kafka records with CRDT metadata for automatic convergence.
//! CRDT metadata is stored in record headers for backward compatibility.

use super::clock::{CausalTimestamp, HybridLogicalClock, VectorClock};
use super::types::{CrdtOperation, CrdtType, CrdtValue};
use crate::error::{Result, StreamlineError};
use crate::storage::record::Header;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Convert Vec<Header> to HashMap<String, Bytes> for easier lookup
pub fn headers_to_map(headers: &[Header]) -> HashMap<String, Bytes> {
    headers
        .iter()
        .map(|h| (h.key.clone(), h.value.clone()))
        .collect()
}

/// Convert HashMap<String, Bytes> to Vec<Header>
pub fn map_to_headers(map: &HashMap<String, Bytes>) -> Vec<Header> {
    map.iter()
        .map(|(k, v)| Header {
            key: k.clone(),
            value: v.clone(),
        })
        .collect()
}

/// Check if headers contain a CRDT type header
pub fn has_crdt_header(headers: &[Header]) -> bool {
    headers.iter().any(|h| h.key == CRDT_TYPE_HEADER)
}

/// Header key for CRDT type
pub const CRDT_TYPE_HEADER: &str = "x-crdt-type";

/// Header key for HLC timestamp (physical:logical:node_id)
pub const CRDT_HLC_HEADER: &str = "x-crdt-hlc";

/// Header key for vector clock (JSON-encoded)
pub const CRDT_VCLOCK_HEADER: &str = "x-crdt-vclock";

/// Header key for operation type (state/delta)
pub const CRDT_OPERATION_HEADER: &str = "x-crdt-op";

/// Header key for node ID
pub const CRDT_NODE_HEADER: &str = "x-crdt-node";

/// Header key for CRDT version
pub const CRDT_VERSION_HEADER: &str = "x-crdt-version";

/// Current CRDT wire format version
pub const CRDT_FORMAT_VERSION: &str = "1";

/// CRDT Record Metadata
///
/// Metadata attached to records that contain CRDT data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrdtMetadata {
    /// CRDT type
    pub crdt_type: CrdtType,
    /// Causal timestamp
    pub timestamp: CausalTimestamp,
    /// Node that created this update
    pub node_id: String,
    /// Operation type (state or delta)
    pub operation: CrdtOperation,
    /// Wire format version
    pub version: String,
}

impl CrdtMetadata {
    /// Create new CRDT metadata
    pub fn new(crdt_type: CrdtType, node_id: impl Into<String>, operation: CrdtOperation) -> Self {
        let node = node_id.into();
        Self {
            crdt_type,
            timestamp: CausalTimestamp::new(node.clone()),
            node_id: node,
            operation,
            version: CRDT_FORMAT_VERSION.to_string(),
        }
    }

    /// Create metadata with existing timestamp
    pub fn with_timestamp(
        crdt_type: CrdtType,
        timestamp: CausalTimestamp,
        node_id: impl Into<String>,
        operation: CrdtOperation,
    ) -> Self {
        Self {
            crdt_type,
            timestamp,
            node_id: node_id.into(),
            operation,
            version: CRDT_FORMAT_VERSION.to_string(),
        }
    }

    /// Convert to record headers (HashMap format)
    pub fn to_headers(&self) -> HashMap<String, Bytes> {
        let mut headers = HashMap::new();

        // CRDT type
        headers.insert(
            CRDT_TYPE_HEADER.to_string(),
            Bytes::from(self.crdt_type.to_string()),
        );

        // HLC timestamp (physical:logical:node_id)
        let hlc = &self.timestamp.hlc;
        let hlc_str = format!("{}:{}:{}", hlc.physical, hlc.logical, hlc.node_id);
        headers.insert(CRDT_HLC_HEADER.to_string(), Bytes::from(hlc_str));

        // Vector clock (JSON)
        if let Ok(vclock_json) = serde_json::to_string(&self.timestamp.vector_clock.clocks) {
            headers.insert(CRDT_VCLOCK_HEADER.to_string(), Bytes::from(vclock_json));
        }

        // Operation type
        let op_str = match self.operation {
            CrdtOperation::State => "state",
            CrdtOperation::Delta => "delta",
        };
        headers.insert(CRDT_OPERATION_HEADER.to_string(), Bytes::from(op_str));

        // Node ID
        headers.insert(
            CRDT_NODE_HEADER.to_string(),
            Bytes::from(self.node_id.clone()),
        );

        // Version
        headers.insert(
            CRDT_VERSION_HEADER.to_string(),
            Bytes::from(self.version.clone()),
        );

        headers
    }

    /// Convert to record headers (Vec<Header> format for storage)
    pub fn to_header_vec(&self) -> Vec<Header> {
        map_to_headers(&self.to_headers())
    }

    /// Parse from Vec<Header>
    pub fn from_header_vec(headers: &[Header]) -> Result<Option<Self>> {
        let map = headers_to_map(headers);
        Self::from_headers(&map)
    }

    /// Parse from record headers
    pub fn from_headers(headers: &HashMap<String, Bytes>) -> Result<Option<Self>> {
        // Check if this is a CRDT record
        let crdt_type = match headers.get(CRDT_TYPE_HEADER) {
            Some(bytes) => {
                let type_str = std::str::from_utf8(bytes).map_err(|e| {
                    StreamlineError::Crdt(format!("Invalid UTF-8 in CRDT type: {}", e))
                })?;
                type_str.parse::<CrdtType>()?
            }
            None => return Ok(None), // Not a CRDT record
        };

        // Parse HLC
        let hlc = if let Some(bytes) = headers.get(CRDT_HLC_HEADER) {
            let hlc_str = std::str::from_utf8(bytes)
                .map_err(|e| StreamlineError::Crdt(format!("Invalid UTF-8 in HLC: {}", e)))?;
            parse_hlc(hlc_str)?
        } else {
            return Err(StreamlineError::Crdt(
                "Missing HLC header in CRDT record".to_string(),
            ));
        };

        // Parse vector clock
        let vector_clock = if let Some(bytes) = headers.get(CRDT_VCLOCK_HEADER) {
            let vclock_str = std::str::from_utf8(bytes).map_err(|e| {
                StreamlineError::Crdt(format!("Invalid UTF-8 in vector clock: {}", e))
            })?;
            let clocks: HashMap<String, u64> = serde_json::from_str(vclock_str)?;
            VectorClock { clocks }
        } else {
            VectorClock::new()
        };

        // Parse operation
        let operation = if let Some(bytes) = headers.get(CRDT_OPERATION_HEADER) {
            let op_str = std::str::from_utf8(bytes)
                .map_err(|e| StreamlineError::Crdt(format!("Invalid UTF-8 in operation: {}", e)))?;
            match op_str {
                "state" => CrdtOperation::State,
                "delta" => CrdtOperation::Delta,
                _ => CrdtOperation::State, // Default to state
            }
        } else {
            CrdtOperation::State
        };

        // Parse node ID
        let node_id = if let Some(bytes) = headers.get(CRDT_NODE_HEADER) {
            std::str::from_utf8(bytes)
                .map_err(|e| StreamlineError::Crdt(format!("Invalid UTF-8 in node ID: {}", e)))?
                .to_string()
        } else {
            hlc.node_id.clone()
        };

        // Parse version
        let version = if let Some(bytes) = headers.get(CRDT_VERSION_HEADER) {
            std::str::from_utf8(bytes)
                .map_err(|e| StreamlineError::Crdt(format!("Invalid UTF-8 in version: {}", e)))?
                .to_string()
        } else {
            CRDT_FORMAT_VERSION.to_string()
        };

        Ok(Some(Self {
            crdt_type,
            timestamp: CausalTimestamp { hlc, vector_clock },
            node_id,
            operation,
            version,
        }))
    }
}

/// Parse HLC from string format "physical:logical:node_id"
fn parse_hlc(s: &str) -> Result<HybridLogicalClock> {
    let parts: Vec<&str> = s.splitn(3, ':').collect();
    if parts.len() != 3 {
        return Err(StreamlineError::Crdt(format!("Invalid HLC format: {}", s)));
    }

    let physical = parts[0]
        .parse()
        .map_err(|e| StreamlineError::Crdt(format!("Invalid HLC physical: {}", e)))?;
    let logical = parts[1]
        .parse()
        .map_err(|e| StreamlineError::Crdt(format!("Invalid HLC logical: {}", e)))?;
    let node_id = parts[2].to_string();

    Ok(HybridLogicalClock {
        physical,
        logical,
        node_id,
    })
}

/// CRDT Record
///
/// A record with CRDT data and metadata for automatic merge.
#[derive(Debug, Clone)]
pub struct CrdtRecord {
    /// Record key
    pub key: Option<Bytes>,
    /// CRDT value
    pub value: CrdtValue,
    /// CRDT metadata
    pub metadata: CrdtMetadata,
    /// Additional headers
    pub headers: HashMap<String, Bytes>,
}

impl CrdtRecord {
    /// Create a new CRDT record
    pub fn new(
        key: Option<Bytes>,
        value: CrdtValue,
        node_id: impl Into<String>,
        operation: CrdtOperation,
    ) -> Self {
        let crdt_type = value.crdt_type();
        Self {
            key,
            value,
            metadata: CrdtMetadata::new(crdt_type, node_id, operation),
            headers: HashMap::new(),
        }
    }

    /// Create with existing metadata
    pub fn with_metadata(key: Option<Bytes>, value: CrdtValue, metadata: CrdtMetadata) -> Self {
        Self {
            key,
            value,
            metadata,
            headers: HashMap::new(),
        }
    }

    /// Add a custom header
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Get all headers (including CRDT metadata headers) as HashMap
    pub fn all_headers(&self) -> HashMap<String, Bytes> {
        let mut headers = self.metadata.to_headers();
        headers.extend(self.headers.clone());
        headers
    }

    /// Get all headers as Vec<Header> for storage
    pub fn all_headers_vec(&self) -> Vec<Header> {
        map_to_headers(&self.all_headers())
    }

    /// Serialize the CRDT value to bytes
    pub fn value_bytes(&self) -> Result<Bytes> {
        let json = serde_json::to_vec(&self.value)?;
        Ok(Bytes::from(json))
    }

    /// Deserialize a CRDT record from raw record data (HashMap headers)
    pub fn from_raw(
        key: Option<Bytes>,
        value: Bytes,
        headers: &HashMap<String, Bytes>,
    ) -> Result<Option<Self>> {
        // Check if this is a CRDT record
        let metadata = match CrdtMetadata::from_headers(headers)? {
            Some(m) => m,
            None => return Ok(None),
        };

        // Parse CRDT value
        let crdt_value: CrdtValue = serde_json::from_slice(&value)?;

        // Verify type matches
        if crdt_value.crdt_type() != metadata.crdt_type {
            return Err(StreamlineError::Crdt(format!(
                "CRDT type mismatch: header says {:?}, value is {:?}",
                metadata.crdt_type,
                crdt_value.crdt_type()
            )));
        }

        // Extract non-CRDT headers
        let mut custom_headers = headers.clone();
        custom_headers.remove(CRDT_TYPE_HEADER);
        custom_headers.remove(CRDT_HLC_HEADER);
        custom_headers.remove(CRDT_VCLOCK_HEADER);
        custom_headers.remove(CRDT_OPERATION_HEADER);
        custom_headers.remove(CRDT_NODE_HEADER);
        custom_headers.remove(CRDT_VERSION_HEADER);

        Ok(Some(Self {
            key,
            value: crdt_value,
            metadata,
            headers: custom_headers,
        }))
    }

    /// Deserialize a CRDT record from raw record data (Vec<Header> format)
    pub fn from_raw_vec(
        key: Option<Bytes>,
        value: Bytes,
        headers: &[Header],
    ) -> Result<Option<Self>> {
        let map = headers_to_map(headers);
        Self::from_raw(key, value, &map)
    }
}

/// Builder for CRDT records
pub struct CrdtRecordBuilder {
    key: Option<Bytes>,
    node_id: String,
    operation: CrdtOperation,
    headers: HashMap<String, Bytes>,
}

impl CrdtRecordBuilder {
    /// Create a new builder
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            key: None,
            node_id: node_id.into(),
            operation: CrdtOperation::State,
            headers: HashMap::new(),
        }
    }

    /// Set the record key
    pub fn key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set the operation type
    pub fn operation(mut self, operation: CrdtOperation) -> Self {
        self.operation = operation;
        self
    }

    /// Add a custom header
    pub fn header(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Build a G-Counter record
    pub fn g_counter(self, value: super::types::GCounter) -> CrdtRecord {
        let mut record = CrdtRecord::new(
            self.key,
            CrdtValue::GCounter(value),
            self.node_id,
            self.operation,
        );
        record.headers = self.headers;
        record
    }

    /// Build a PN-Counter record
    pub fn pn_counter(self, value: super::types::PNCounter) -> CrdtRecord {
        let mut record = CrdtRecord::new(
            self.key,
            CrdtValue::PNCounter(value),
            self.node_id,
            self.operation,
        );
        record.headers = self.headers;
        record
    }

    /// Build an LWW-Register record with string value
    pub fn lww_register_string(self, value: super::types::LWWRegister<String>) -> CrdtRecord {
        let mut record = CrdtRecord::new(
            self.key,
            CrdtValue::LWWRegisterString(value),
            self.node_id,
            self.operation,
        );
        record.headers = self.headers;
        record
    }

    /// Build an OR-Set record with string elements
    pub fn or_set_string(self, value: super::types::ORSet<String>) -> CrdtRecord {
        let mut record = CrdtRecord::new(
            self.key,
            CrdtValue::ORSetString(value),
            self.node_id,
            self.operation,
        );
        record.headers = self.headers;
        record
    }

    /// Build an RGA-Sequence record with string elements
    pub fn rga_sequence_string(self, value: super::types::RGASequence<String>) -> CrdtRecord {
        let mut record = CrdtRecord::new(
            self.key,
            CrdtValue::RGASequenceString(value),
            self.node_id,
            self.operation,
        );
        record.headers = self.headers;
        record
    }
}

/// Merge context for CRDT records
pub struct CrdtMergeContext {
    /// Node ID for this context
    pub node_id: String,
    /// Accumulated states by key
    states: HashMap<Bytes, CrdtValue>,
}

impl CrdtMergeContext {
    /// Create a new merge context
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            states: HashMap::new(),
        }
    }

    /// Merge a CRDT record into the context
    pub fn merge(&mut self, record: &CrdtRecord) -> Result<()> {
        let key = record
            .key
            .clone()
            .unwrap_or_else(|| Bytes::from_static(b""));

        if let Some(existing) = self.states.get_mut(&key) {
            existing.merge(&record.value)?;
        } else {
            self.states.insert(key, record.value.clone());
        }

        Ok(())
    }

    /// Get the merged state for a key
    pub fn get(&self, key: &Bytes) -> Option<&CrdtValue> {
        self.states.get(key)
    }

    /// Get all merged states
    pub fn states(&self) -> &HashMap<Bytes, CrdtValue> {
        &self.states
    }

    /// Convert merged states back to records
    pub fn to_records(&self) -> Vec<CrdtRecord> {
        self.states
            .iter()
            .map(|(key, value)| {
                CrdtRecord::new(
                    Some(key.clone()),
                    value.clone(),
                    self.node_id.clone(),
                    CrdtOperation::State,
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::GCounter;
    use super::*;

    #[test]
    fn test_metadata_roundtrip() {
        let metadata = CrdtMetadata::new(CrdtType::GCounter, "node1", CrdtOperation::State);
        let headers = metadata.to_headers();

        let parsed = CrdtMetadata::from_headers(&headers).unwrap().unwrap();
        assert_eq!(parsed.crdt_type, CrdtType::GCounter);
        assert_eq!(parsed.node_id, "node1");
        assert_eq!(parsed.operation, CrdtOperation::State);
    }

    #[test]
    fn test_crdt_record_serialization() {
        let mut counter = GCounter::new("node1");
        counter.increment(42);

        let record = CrdtRecordBuilder::new("node1")
            .key("my-counter")
            .g_counter(counter);

        let value_bytes = record.value_bytes().unwrap();
        let headers = record.all_headers();

        // Parse back
        let parsed = CrdtRecord::from_raw(Some(Bytes::from("my-counter")), value_bytes, &headers)
            .unwrap()
            .unwrap();

        match parsed.value {
            CrdtValue::GCounter(c) => assert_eq!(c.value(), 42),
            _ => panic!("Expected GCounter"),
        }
    }

    #[test]
    fn test_merge_context() {
        let mut ctx = CrdtMergeContext::new("merger");

        // Create two counter records for the same key
        let mut counter1 = GCounter::new("node1");
        counter1.increment(10);
        let record1 = CrdtRecordBuilder::new("node1")
            .key("counter")
            .g_counter(counter1);

        let mut counter2 = GCounter::new("node2");
        counter2.increment(20);
        let record2 = CrdtRecordBuilder::new("node2")
            .key("counter")
            .g_counter(counter2);

        ctx.merge(&record1).unwrap();
        ctx.merge(&record2).unwrap();

        // Check merged value
        let merged = ctx.get(&Bytes::from("counter")).unwrap();
        match merged {
            CrdtValue::GCounter(c) => assert_eq!(c.value(), 30),
            _ => panic!("Expected GCounter"),
        }
    }

    #[test]
    fn test_hlc_parsing() {
        let hlc = parse_hlc("1234567890:42:node1").unwrap();
        assert_eq!(hlc.physical, 1234567890);
        assert_eq!(hlc.logical, 42);
        assert_eq!(hlc.node_id, "node1");
    }

    #[test]
    fn test_non_crdt_record() {
        let headers: HashMap<String, Bytes> = HashMap::new();
        let result = CrdtMetadata::from_headers(&headers).unwrap();
        assert!(result.is_none());
    }
}
