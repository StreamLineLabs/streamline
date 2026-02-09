//! Record types for Streamline storage

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// A single record in a topic partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    /// Offset of this record within the partition
    pub offset: i64,

    /// Timestamp of the record (milliseconds since epoch)
    pub timestamp: i64,

    /// Optional key for the record
    pub key: Option<Bytes>,

    /// Value of the record
    pub value: Bytes,

    /// Optional headers
    pub headers: Vec<Header>,

    /// Optional CRC32 checksum for data integrity verification (v2 format only)
    /// When present, enables per-record corruption detection in addition to batch-level CRCs.
    /// Missing CRC (None) indicates v1 format records - verify_crc() returns true for these.
    ///
    /// Note: We use `#[serde(default)]` for backward compatibility with JSON deserializaton,
    /// but NOT `skip_serializing_if` because bincode requires consistent struct layout.
    /// Option<u32> serializes efficiently: 1 byte tag + optional 4 bytes.
    #[serde(default)]
    pub crc: Option<u32>,
}

/// A record header (key-value pair)
///
/// # Performance Note
///
/// `Record` uses `Bytes` (Arc-based) for `key` and `value` fields, making cloning O(1).
/// However, `Header.key` uses `String`, which requires memory allocation on clone.
/// This is acceptable because:
/// - Most Kafka records have zero or few headers
/// - Header keys are typically short (< 50 bytes)
/// - The `value` field uses `Bytes` for cheap cloning of potentially large values
///
/// If header cloning becomes a bottleneck in profiling, consider changing `key` to
/// `Bytes` or `Arc<str>` in a future version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Header {
    /// Header key
    pub key: String,

    /// Header value
    pub value: Bytes,
}

impl Record {
    /// Create a new record
    pub fn new(offset: i64, timestamp: i64, key: Option<Bytes>, value: Bytes) -> Self {
        Self {
            offset,
            timestamp,
            key,
            value,
            headers: Vec::new(),
            crc: None,
        }
    }

    /// Create a new record with headers
    pub fn with_headers(
        offset: i64,
        timestamp: i64,
        key: Option<Bytes>,
        value: Bytes,
        headers: Vec<Header>,
    ) -> Self {
        Self {
            offset,
            timestamp,
            key,
            value,
            headers,
            crc: None,
        }
    }

    /// Calculate CRC32 checksum for this record's data
    ///
    /// The checksum covers: offset, timestamp, key (if present), value, and all headers.
    /// This provides per-record integrity verification to complement batch-level CRCs.
    pub fn calculate_crc(&self) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.offset.to_le_bytes());
        hasher.update(&self.timestamp.to_le_bytes());
        if let Some(ref key) = self.key {
            hasher.update(key);
        }
        hasher.update(&self.value);
        for header in &self.headers {
            hasher.update(header.key.as_bytes());
            hasher.update(&header.value);
        }
        hasher.finalize()
    }

    /// Verify the CRC32 checksum if present
    ///
    /// Returns `true` if:
    /// - CRC is present and matches the calculated value
    /// - CRC is absent (v1 format records without per-record checksums)
    ///
    /// Returns `false` only if CRC is present but doesn't match (corruption detected).
    pub fn verify_crc(&self) -> bool {
        match self.crc {
            Some(expected) => self.calculate_crc() == expected,
            None => true, // v1 records don't have per-record CRC
        }
    }

    /// Populate the CRC field with the calculated checksum
    ///
    /// Call this when creating new records that should have per-record checksums.
    pub fn with_crc(mut self) -> Self {
        self.crc = Some(self.calculate_crc());
        self
    }

    /// Get the size of this record in bytes (approximate)
    pub fn size(&self) -> usize {
        let key_size = self.key.as_ref().map(|k| k.len()).unwrap_or(0);
        let headers_size: usize = self
            .headers
            .iter()
            .map(|h| h.key.len() + h.value.len())
            .sum();

        // 8 bytes for offset + 8 bytes for timestamp + key + value + headers
        8 + 8 + key_size + self.value.len() + headers_size
    }

    /// Get the per-message TTL from headers, if set
    /// Returns the TTL in milliseconds, or None if not set
    pub fn get_ttl_ms(&self) -> Option<i64> {
        self.headers
            .iter()
            .find(|h| h.key == super::topic::MESSAGE_TTL_HEADER)
            .and_then(|h| std::str::from_utf8(&h.value).ok())
            .and_then(|s| s.parse::<i64>().ok())
    }

    /// Check if this record has expired based on TTL
    ///
    /// # Arguments
    /// * `topic_ttl_ms` - The topic-level TTL in milliseconds (-1 for infinite)
    /// * `current_time_ms` - The current time in milliseconds since epoch
    ///
    /// # Returns
    /// `true` if the record has expired, `false` otherwise
    ///
    /// # Overflow Handling
    /// This function uses `saturating_add` when calculating the expiry time
    /// (`timestamp + ttl`). If the sum would overflow `i64::MAX`, it saturates
    /// at `i64::MAX` instead of wrapping. This is intentional:
    /// - It prevents unexpected early expiry from overflow wrap-around
    /// - Records with very large timestamps or TTLs effectively never expire
    /// - This is safe because `i64::MAX` milliseconds is ~292 million years
    ///
    /// # Example
    /// ```ignore
    /// // Overflow case: timestamp near MAX + TTL saturates to MAX
    /// let record = Record { timestamp: i64::MAX - 1000, ... };
    /// // With TTL of 10000ms, expiry would overflow without saturating_add
    /// // Instead, expiry_time = i64::MAX, so record won't expire unexpectedly
    /// ```
    pub fn is_expired(&self, topic_ttl_ms: i64, current_time_ms: i64) -> bool {
        // Get effective TTL: per-message overrides topic-level
        let effective_ttl = self.get_ttl_ms().unwrap_or(topic_ttl_ms);

        // -1 means infinite TTL
        if effective_ttl < 0 {
            return false;
        }

        // Record is expired if current time > timestamp + ttl
        // Uses saturating_add to prevent overflow wrap-around (see doc comment above)
        let expiry_time = self.timestamp.saturating_add(effective_ttl);
        current_time_ms > expiry_time
    }
}

/// A batch of records for efficient storage and transmission
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordBatch {
    /// Base offset of the first record in this batch
    pub base_offset: i64,

    /// Batch timestamp
    pub timestamp: i64,

    /// Records in this batch
    pub records: Vec<Record>,
}

impl RecordBatch {
    /// Create a new empty record batch
    pub fn new(base_offset: i64, timestamp: i64) -> Self {
        Self {
            base_offset,
            timestamp,
            records: Vec::new(),
        }
    }

    /// Add a record to this batch
    pub fn add_record(&mut self, record: Record) {
        self.records.push(record);
    }

    /// Get the number of records in this batch
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Get the first offset in this batch
    pub fn first_offset(&self) -> Option<i64> {
        self.records.first().map(|r| r.offset)
    }

    /// Get the last offset in this batch
    pub fn last_offset(&self) -> Option<i64> {
        self.records.last().map(|r| r.offset)
    }

    /// Get the size of this batch in bytes (approximate)
    pub fn size(&self) -> usize {
        // 8 bytes for base_offset + 8 bytes for timestamp + records
        16 + self.records.iter().map(|r| r.size()).sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_new() {
        let record = Record::new(0, 1234567890, None, Bytes::from("test value"));
        assert_eq!(record.offset, 0);
        assert_eq!(record.timestamp, 1234567890);
        assert!(record.key.is_none());
        assert_eq!(record.value, Bytes::from("test value"));
        assert!(record.headers.is_empty());
    }

    #[test]
    fn test_record_with_key() {
        let record = Record::new(
            1,
            1234567890,
            Some(Bytes::from("key")),
            Bytes::from("value"),
        );
        assert_eq!(record.offset, 1);
        assert!(record.key.is_some());
        assert_eq!(record.key.unwrap(), Bytes::from("key"));
    }

    #[test]
    fn test_record_size() {
        let record = Record::new(0, 0, Some(Bytes::from("key")), Bytes::from("value"));
        // 8 (offset) + 8 (timestamp) + 3 (key) + 5 (value) = 24
        assert_eq!(record.size(), 24);
    }

    #[test]
    fn test_record_batch() {
        let mut batch = RecordBatch::new(0, 1234567890);
        assert!(batch.is_empty());

        batch.add_record(Record::new(0, 1234567890, None, Bytes::from("record 1")));
        batch.add_record(Record::new(1, 1234567891, None, Bytes::from("record 2")));

        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
        assert_eq!(batch.last_offset(), Some(1));
    }

    #[test]
    fn test_record_get_ttl_ms() {
        // Record without TTL header
        let record = Record::new(0, 1000, None, Bytes::from("value"));
        assert_eq!(record.get_ttl_ms(), None);

        // Record with valid TTL header
        let record_with_ttl = Record::with_headers(
            0,
            1000,
            None,
            Bytes::from("value"),
            vec![Header {
                key: "x-message-ttl-ms".to_string(),
                value: Bytes::from("5000"),
            }],
        );
        assert_eq!(record_with_ttl.get_ttl_ms(), Some(5000));

        // Record with invalid TTL header (non-numeric)
        let record_invalid = Record::with_headers(
            0,
            1000,
            None,
            Bytes::from("value"),
            vec![Header {
                key: "x-message-ttl-ms".to_string(),
                value: Bytes::from("invalid"),
            }],
        );
        assert_eq!(record_invalid.get_ttl_ms(), None);
    }

    #[test]
    fn test_record_is_expired() {
        let now_ms = 10000i64;
        let record_time = 5000i64;

        // Create record at timestamp 5000
        let record = Record::new(0, record_time, None, Bytes::from("value"));

        // Test with infinite topic TTL (-1)
        assert!(!record.is_expired(-1, now_ms));

        // Test with TTL that hasn't expired (TTL=10000, expires at 15000)
        assert!(!record.is_expired(10000, now_ms));

        // Test with TTL that has expired (TTL=1000, expired at 6000)
        assert!(record.is_expired(1000, now_ms));

        // Test exact boundary (TTL=5000, expires exactly at now_ms=10000)
        // Should NOT be expired since we use > not >=
        assert!(!record.is_expired(5000, now_ms));

        // Test just past boundary
        assert!(record.is_expired(4999, now_ms));
    }

    #[test]
    fn test_record_is_expired_with_header_override() {
        let now_ms = 10000i64;
        let record_time = 5000i64;

        // Record with per-message TTL header (1000ms, expires at 6000)
        let record = Record::with_headers(
            0,
            record_time,
            None,
            Bytes::from("value"),
            vec![Header {
                key: "x-message-ttl-ms".to_string(),
                value: Bytes::from("1000"), // Short TTL
            }],
        );

        // Even with long topic TTL, per-message TTL takes precedence
        assert!(record.is_expired(100000, now_ms)); // Would not expire with topic TTL

        // Negative per-message TTL means infinite
        let record_infinite = Record::with_headers(
            0,
            record_time,
            None,
            Bytes::from("value"),
            vec![Header {
                key: "x-message-ttl-ms".to_string(),
                value: Bytes::from("-1"),
            }],
        );
        assert!(!record_infinite.is_expired(1000, now_ms)); // Would expire with topic TTL
    }

    #[test]
    fn test_record_is_expired_overflow_handling() {
        // Test that saturating_add prevents unexpected early expiry from overflow
        // Without saturating_add, timestamp + ttl would wrap around to a negative value

        // Create record with timestamp near i64::MAX
        let high_timestamp = i64::MAX - 1000;
        let record = Record::new(0, high_timestamp, None, Bytes::from("value"));

        // TTL that would cause overflow without saturating_add
        let ttl_ms = 10000i64;

        // Current time is after the timestamp but before what would be the "wrapped" expiry
        let current_time = high_timestamp + 500; // Still within TTL

        // Without saturating_add: expiry = (MAX - 1000) + 10000 = overflow -> negative
        // With saturating_add: expiry = MAX (saturated)
        // The record should NOT be expired since expiry saturates to MAX
        assert!(
            !record.is_expired(ttl_ms, current_time),
            "Record should not expire when timestamp + TTL would overflow (saturates to MAX)"
        );

        // Even with current_time at a very high value, should not expire
        // because expiry_time saturates to i64::MAX
        let very_high_current = i64::MAX - 1;
        assert!(
            !record.is_expired(ttl_ms, very_high_current),
            "Record should not expire even with very high current time when expiry saturates"
        );
    }

    #[test]
    fn test_record_crc_calculation() {
        let record = Record::new(
            0,
            1234567890,
            Some(Bytes::from("key")),
            Bytes::from("value"),
        );

        // CRC should be deterministic
        let crc1 = record.calculate_crc();
        let crc2 = record.calculate_crc();
        assert_eq!(crc1, crc2, "CRC calculation should be deterministic");

        // Different data should produce different CRC
        let record2 = Record::new(
            0,
            1234567890,
            Some(Bytes::from("key")),
            Bytes::from("different"),
        );
        assert_ne!(
            record.calculate_crc(),
            record2.calculate_crc(),
            "Different values should produce different CRCs"
        );
    }

    #[test]
    fn test_record_crc_verification() {
        // Record without CRC (v1 format) should verify as true
        let record_v1 = Record::new(0, 1234567890, None, Bytes::from("value"));
        assert!(
            record_v1.verify_crc(),
            "v1 records without CRC should verify"
        );

        // Record with correct CRC should verify
        let record_v2 = Record::new(0, 1234567890, None, Bytes::from("value")).with_crc();
        assert!(
            record_v2.crc.is_some(),
            "with_crc should populate CRC field"
        );
        assert!(
            record_v2.verify_crc(),
            "v2 records with correct CRC should verify"
        );

        // Record with incorrect CRC should fail verification
        let mut corrupted = record_v2.clone();
        corrupted.crc = Some(0xDEADBEEF); // Wrong CRC
        assert!(
            !corrupted.verify_crc(),
            "Corrupted CRC should fail verification"
        );
    }

    #[test]
    fn test_record_crc_with_headers() {
        let record = Record::with_headers(
            0,
            1234567890,
            Some(Bytes::from("key")),
            Bytes::from("value"),
            vec![Header {
                key: "header-key".to_string(),
                value: Bytes::from("header-value"),
            }],
        )
        .with_crc();

        assert!(record.verify_crc(), "Record with headers should verify");

        // Changing header should change CRC
        let mut modified = record.clone();
        modified.headers[0].value = Bytes::from("modified");
        assert!(
            !modified.verify_crc(),
            "Modified header should fail verification"
        );
    }
}
