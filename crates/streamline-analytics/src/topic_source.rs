//! Abstraction layer for reading topic data in the analytics engine.
//!
//! The main Streamline crate provides a concrete implementation of
//! [`TopicDataSource`] that wraps `TopicManager`, allowing the analytics
//! workspace crate to read topic data without depending on the main crate's
//! concrete types.
//!
//! # Stability
//!
//! **Stable** -- Breaking changes only in major versions.

/// Trait for reading topic data, abstracting away the concrete TopicManager.
///
/// The main crate provides an implementation wrapping `TopicManager` so that
/// the analytics engine can query stream data without a direct dependency on
/// storage internals.
///
/// # Example (mock implementation for testing)
///
/// ```
/// use streamline_analytics::topic_source::{AnalyticsRecord, TopicDataSource};
/// use streamline_analytics::error::Result;
///
/// struct MockSource;
///
/// impl TopicDataSource for MockSource {
///     fn num_partitions(&self, _topic: &str) -> Result<i32> { Ok(1) }
///     fn earliest_offset(&self, _t: &str, _p: i32) -> Result<i64> { Ok(0) }
///     fn latest_offset(&self, _t: &str, _p: i32) -> Result<i64> { Ok(0) }
///     fn read_records(&self, _t: &str, _p: i32, _o: i64, _m: usize) -> Result<Vec<AnalyticsRecord>> {
///         Ok(vec![])
///     }
/// }
/// ```
pub trait TopicDataSource: Send + Sync {
    /// Get the number of partitions for a topic.
    ///
    /// Returns an error if the topic does not exist.
    fn num_partitions(&self, topic: &str) -> crate::error::Result<i32>;

    /// Get the earliest available offset for a partition.
    fn earliest_offset(&self, topic: &str, partition: i32) -> crate::error::Result<i64>;

    /// Get the latest (next-to-be-written) offset for a partition.
    fn latest_offset(&self, topic: &str, partition: i32) -> crate::error::Result<i64>;

    /// Read records from a topic partition starting at `offset`.
    ///
    /// Returns up to `max_records` records. An empty `Vec` is returned when
    /// the partition is empty or the offset is at the end.
    fn read_records(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_records: usize,
    ) -> crate::error::Result<Vec<AnalyticsRecord>>;
}

/// A record as seen by the analytics engine.
///
/// This is a simplified projection of `storage::Record` that contains only
/// the fields needed for SQL query materialisation.
#[derive(Debug, Clone)]
pub struct AnalyticsRecord {
    /// The offset of this record within its partition.
    pub offset: i64,
    /// Timestamp in milliseconds since epoch.
    pub timestamp: i64,
    /// Optional message key (binary).
    pub key: Option<Vec<u8>>,
    /// Message payload (binary).
    pub value: Vec<u8>,
    /// Header key-value pairs.
    pub headers: Vec<(String, Vec<u8>)>,
}
