//! Bloom filter implementation for efficient key lookups
//!
//! This module provides probabilistic data structures for fast key existence checks
//! at the segment and partition level. Bloom filters enable O(1) negative lookups,
//! meaning we can quickly determine that a key is definitely NOT present without
//! reading from disk.
//!
//! ## Use Cases
//!
//! 1. **Segment-level filtering**: Skip segments that definitely don't contain a key
//! 2. **Key-based routing**: Quick membership checks for topic routing
//! 3. **Compaction optimization**: Identify segments with overlapping keys
//! 4. **Consumer group membership**: Track which keys have been consumed
//!
//! ## Guarantees
//!
//! - **No false negatives**: If the filter says a key is NOT present, it's definitely not
//! - **Possible false positives**: The filter may say a key exists when it doesn't
//! - **Configurable accuracy**: Trade memory for lower false positive rates
//!
//! ## File Format
//!
//! Bloom filter files use `.bloom` extension alongside segment files:
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │ Header (32 bytes)                       │
//! │   Magic: "SBLM" (4 bytes)               │
//! │   Version: u16                          │
//! │   Flags: u16                            │
//! │   Bitmap Size: u64                      │
//! │   Num Hash Functions: u32               │
//! │   Item Count: u32                       │
//! │   CRC32: u32                            │
//! │   Reserved: 4 bytes                     │
//! ├─────────────────────────────────────────┤
//! │ Bitmap Data                             │
//! │   [u8; bitmap_size]                     │
//! └─────────────────────────────────────────┘
//! ```
//!
//! ## Example
//!
//! ```rust,ignore
//! use streamline::storage::bloom::KeyBloomFilter;
//!
//! // Create a filter for 10,000 items with 1% false positive rate
//! let mut filter = KeyBloomFilter::new(10_000, 0.01);
//!
//! // Add keys
//! filter.add(b"user:123");
//! filter.add(b"user:456");
//!
//! // Check membership
//! assert!(filter.might_contain(b"user:123")); // Probably true
//! assert!(!filter.might_contain(b"user:999")); // Definitely false (no false negatives)
//! ```

use crate::error::{Result, StreamlineError};
use bloomfilter::Bloom;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Magic bytes for bloom filter file header
const BLOOM_MAGIC: &[u8; 4] = b"SBLM";

/// Current bloom filter format version
const BLOOM_VERSION: u16 = 1;

/// Size of bloom filter header in bytes (extended to include sip keys)
/// Used for documentation; actual value is embedded in save/load
#[allow(dead_code)]
const BLOOM_HEADER_SIZE: usize = 64;

/// Default false positive rate (1%)
pub const DEFAULT_FALSE_POSITIVE_RATE: f64 = 0.01;

/// Default expected items per filter
pub const DEFAULT_EXPECTED_ITEMS: usize = 100_000;

/// Configuration for bloom filters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilterConfig {
    /// Expected number of items to store
    ///
    /// This affects the size of the filter. If you insert more items
    /// than expected, the false positive rate will increase.
    pub expected_items: usize,

    /// Target false positive rate (0.0 to 1.0)
    ///
    /// Lower values require more memory. Common values:
    /// - 0.01 (1%): Good balance of accuracy and size
    /// - 0.001 (0.1%): Higher accuracy, more memory
    /// - 0.1 (10%): Lower accuracy, less memory
    pub false_positive_rate: f64,

    /// Enable bloom filters for segments
    pub enabled: bool,
}

impl Default for BloomFilterConfig {
    fn default() -> Self {
        Self {
            expected_items: DEFAULT_EXPECTED_ITEMS,
            false_positive_rate: DEFAULT_FALSE_POSITIVE_RATE,
            enabled: true,
        }
    }
}

impl BloomFilterConfig {
    /// Create config for high-accuracy filtering (0.1% FPR)
    pub fn high_accuracy() -> Self {
        Self {
            expected_items: DEFAULT_EXPECTED_ITEMS,
            false_positive_rate: 0.001,
            enabled: true,
        }
    }

    /// Create config for memory-efficient filtering (5% FPR)
    pub fn memory_efficient() -> Self {
        Self {
            expected_items: DEFAULT_EXPECTED_ITEMS,
            false_positive_rate: 0.05,
            enabled: true,
        }
    }

    /// Calculate approximate memory usage in bytes
    pub fn estimated_memory_bytes(&self) -> usize {
        // Bloom filter optimal size formula: m = -n * ln(p) / (ln(2)^2)
        // where n = expected items, p = false positive rate
        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let bits = -(self.expected_items as f64) * self.false_positive_rate.ln() / ln2_squared;
        (bits / 8.0).ceil() as usize
    }
}

/// A bloom filter for efficient key lookups
///
/// This is a probabilistic data structure that can tell you:
/// - Definitely NOT present (no false negatives)
/// - Probably present (possible false positives)
#[derive(Debug)]
pub struct KeyBloomFilter {
    /// The underlying bloom filter
    filter: Bloom<[u8]>,

    /// Number of items added to the filter
    item_count: usize,

    /// Expected items (for sizing calculations)
    /// Used when merging filters to compute combined capacity
    #[allow(dead_code)]
    expected_items: usize,

    /// Target false positive rate
    /// Used for diagnostics and config reconstruction on load
    #[allow(dead_code)]
    false_positive_rate: f64,
}

impl KeyBloomFilter {
    /// Create a new bloom filter with the given capacity and false positive rate
    ///
    /// # Arguments
    ///
    /// * `expected_items` - Expected number of items to insert
    /// * `false_positive_rate` - Target false positive rate (0.0 to 1.0)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Filter for 10,000 items with 1% false positive rate
    /// let filter = KeyBloomFilter::new(10_000, 0.01);
    /// ```
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        // Ensure valid parameters
        let expected_items = expected_items.max(1);
        let false_positive_rate = false_positive_rate.clamp(0.0001, 0.5);

        // Calculate optimal number of bits
        // Bloom filter optimal size formula: m = -n * ln(p) / (ln(2)^2)
        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let bits =
            (-(expected_items as f64) * false_positive_rate.ln() / ln2_squared).ceil() as usize;

        // Bloom::new(bitmap_size, items_count) - it calculates optimal hash functions internally
        let filter = Bloom::new(bits, expected_items);

        Self {
            filter,
            item_count: 0,
            expected_items,
            false_positive_rate,
        }
    }

    /// Create from config
    pub fn from_config(config: &BloomFilterConfig) -> Self {
        Self::new(config.expected_items, config.false_positive_rate)
    }

    /// Create a new bloom filter with specific SIP keys
    ///
    /// This is used for creating filters that can be merged together.
    /// Filters must have the same SIP keys for merging to work correctly.
    pub fn with_sip_keys(
        expected_items: usize,
        false_positive_rate: f64,
        sip_keys: [(u64, u64); 2],
    ) -> Self {
        let expected_items = expected_items.max(1);
        let false_positive_rate = false_positive_rate.clamp(0.0001, 0.5);

        // Calculate optimal number of bits
        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let bits =
            (-(expected_items as f64) * false_positive_rate.ln() / ln2_squared).ceil() as usize;
        let num_bits = bits as u64;

        // Calculate optimal hash functions: k = (m/n) * ln(2)
        let k = ((bits as f64 / expected_items as f64) * std::f64::consts::LN_2).ceil() as u32;

        // Create with specified SIP keys for merge compatibility
        let bitmap = vec![0u8; bits / 8];
        let filter = Bloom::from_existing(&bitmap, num_bits, k, sip_keys);

        Self {
            filter,
            item_count: 0,
            expected_items,
            false_positive_rate,
        }
    }

    /// Get the SIP keys used by this filter
    ///
    /// Use these keys to create compatible filters for merging.
    pub fn sip_keys(&self) -> [(u64, u64); 2] {
        self.filter.sip_keys()
    }

    /// Create an empty filter compatible with this one (for merging)
    ///
    /// The new filter will have the same bitmap size, hash functions, and SIP keys,
    /// making it safe to merge with the original filter.
    pub fn create_compatible(&self) -> Self {
        let num_bits = self.filter.number_of_bits();
        let num_hashes = self.filter.number_of_hash_functions();
        let sip_keys = self.filter.sip_keys();
        let bitmap_size = (num_bits / 8) as usize;

        let bitmap = vec![0u8; bitmap_size];
        let filter = Bloom::from_existing(&bitmap, num_bits, num_hashes, sip_keys);

        Self {
            filter,
            item_count: 0,
            expected_items: self.expected_items,
            false_positive_rate: self.false_positive_rate,
        }
    }

    /// Add a key to the filter
    ///
    /// After adding, `might_contain(key)` will return `true` for this key.
    pub fn add(&mut self, key: &[u8]) {
        self.filter.set(key);
        self.item_count += 1;
    }

    /// Add multiple keys to the filter
    pub fn add_all(&mut self, keys: impl IntoIterator<Item = impl AsRef<[u8]>>) {
        for key in keys {
            self.add(key.as_ref());
        }
    }

    /// Check if a key might be in the filter
    ///
    /// Returns:
    /// - `false`: Key is definitely NOT in the filter (no false negatives)
    /// - `true`: Key is probably in the filter (possible false positive)
    pub fn might_contain(&self, key: &[u8]) -> bool {
        self.filter.check(key)
    }

    /// Check multiple keys and return those that might exist
    pub fn filter_keys<'a>(&self, keys: &'a [&'a [u8]]) -> Vec<&'a [u8]> {
        keys.iter()
            .filter(|key| self.might_contain(key))
            .copied()
            .collect()
    }

    /// Get the number of items added to the filter
    pub fn item_count(&self) -> usize {
        self.item_count
    }

    /// Check if the filter is empty
    pub fn is_empty(&self) -> bool {
        self.item_count == 0
    }

    /// Get the estimated current false positive rate
    ///
    /// This may be higher than the target if more items than expected
    /// have been inserted.
    pub fn estimated_fpr(&self) -> f64 {
        if self.item_count == 0 {
            return 0.0;
        }

        // FPR formula: (1 - e^(-k*n/m))^k
        // where k = num hashes, n = items, m = bits
        let bits = self.filter.number_of_bits() as f64;
        let hashes = self.filter.number_of_hash_functions() as f64;
        let items = self.item_count as f64;

        let exponent = -hashes * items / bits;
        (1.0 - exponent.exp()).powf(hashes)
    }

    /// Get the size of the filter in bytes
    pub fn size_bytes(&self) -> usize {
        (self.filter.number_of_bits() / 8) as usize
    }

    /// Merge another bloom filter into this one
    ///
    /// The filters must have the same size. After merging, this filter
    /// will return `true` for any key that either filter would have
    /// returned `true` for.
    pub fn merge(&mut self, other: &KeyBloomFilter) -> Result<()> {
        if self.filter.number_of_bits() != other.filter.number_of_bits() {
            return Err(StreamlineError::storage_msg(
                "Cannot merge bloom filters of different sizes".to_string(),
            ));
        }

        if self.filter.number_of_hash_functions() != other.filter.number_of_hash_functions() {
            return Err(StreamlineError::storage_msg(
                "Cannot merge bloom filters with different hash functions".to_string(),
            ));
        }

        // Get bitmaps and OR them together
        let self_bytes = self.filter.bitmap();
        let other_bytes = other.filter.bitmap();

        let merged_bytes: Vec<u8> = self_bytes
            .iter()
            .zip(other_bytes.iter())
            .map(|(a, b)| a | b)
            .collect();

        // Reconstruct filter with merged bitmap using the same sip keys
        let sip_keys = self.filter.sip_keys();
        self.filter = Bloom::from_existing(
            &merged_bytes,
            self.filter.number_of_bits(),
            self.filter.number_of_hash_functions(),
            sip_keys,
        );
        self.item_count += other.item_count;

        Ok(())
    }

    /// Clear the filter
    pub fn clear(&mut self) {
        self.filter.clear();
        self.item_count = 0;
    }

    /// Save the bloom filter to a file
    pub fn save(&self, path: &Path) -> Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        let mut writer = BufWriter::new(file);

        // Get sip keys for proper reconstruction
        let sip_keys = self.filter.sip_keys();

        // Write header (extended to 64 bytes to include sip keys)
        let mut header = [0u8; 64];
        header[0..4].copy_from_slice(BLOOM_MAGIC);
        header[4..6].copy_from_slice(&BLOOM_VERSION.to_le_bytes());
        // Flags at [6..8] - reserved
        let bitmap_size = self.filter.number_of_bits() / 8;
        header[8..16].copy_from_slice(&bitmap_size.to_le_bytes());
        let num_hashes = self.filter.number_of_hash_functions();
        header[16..20].copy_from_slice(&num_hashes.to_le_bytes());
        header[20..24].copy_from_slice(&(self.item_count as u32).to_le_bytes());
        // Sip keys (4 x u64 = 32 bytes)
        header[24..32].copy_from_slice(&sip_keys[0].0.to_le_bytes());
        header[32..40].copy_from_slice(&sip_keys[0].1.to_le_bytes());
        header[40..48].copy_from_slice(&sip_keys[1].0.to_le_bytes());
        header[48..56].copy_from_slice(&sip_keys[1].1.to_le_bytes());
        // CRC at [56..60]
        let crc = crc32fast::hash(&header[0..56]);
        header[56..60].copy_from_slice(&crc.to_le_bytes());
        // Reserved [60..64]

        writer.write_all(&header)?;

        // Write bitmap
        let bitmap = self.filter.bitmap();
        writer.write_all(&bitmap)?;

        writer.flush()?;
        writer.get_ref().sync_all()?;

        debug!(path = ?path, items = self.item_count, size = bitmap.len(), "Saved bloom filter");

        Ok(())
    }

    /// Load a bloom filter from a file
    pub fn load(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read header (64 bytes)
        let mut header = [0u8; 64];
        reader.read_exact(&mut header)?;

        // Verify magic
        if &header[0..4] != BLOOM_MAGIC {
            return Err(StreamlineError::CorruptedData(
                "Invalid bloom filter magic".to_string(),
            ));
        }

        // Check version
        let version = u16::from_le_bytes([header[4], header[5]]);
        if version != BLOOM_VERSION {
            return Err(StreamlineError::CorruptedData(format!(
                "Unsupported bloom filter version: {}",
                version
            )));
        }

        // Read metadata
        let bitmap_size = u64::from_le_bytes(
            header[8..16]
                .try_into()
                .map_err(|_| StreamlineError::CorruptedData("Invalid bloom filter bitmap size".to_string()))?,
        ) as usize;
        let num_hashes = u32::from_le_bytes(
            header[16..20]
                .try_into()
                .map_err(|_| StreamlineError::CorruptedData("Invalid bloom filter hash count".to_string()))?,
        );
        let item_count = u32::from_le_bytes(
            header[20..24]
                .try_into()
                .map_err(|_| StreamlineError::CorruptedData("Invalid bloom filter item count".to_string()))?,
        ) as usize;

        // Read sip keys
        let sip_key0_0 = u64::from_le_bytes(
            header[24..32]
                .try_into()
                .map_err(|_| StreamlineError::CorruptedData("Invalid bloom filter sip key".to_string()))?,
        );
        let sip_key0_1 = u64::from_le_bytes(
            header[32..40]
                .try_into()
                .map_err(|_| StreamlineError::CorruptedData("Invalid bloom filter sip key".to_string()))?,
        );
        let sip_key1_0 = u64::from_le_bytes(
            header[40..48]
                .try_into()
                .map_err(|_| StreamlineError::CorruptedData("Invalid bloom filter sip key".to_string()))?,
        );
        let sip_key1_1 = u64::from_le_bytes(
            header[48..56]
                .try_into()
                .map_err(|_| StreamlineError::CorruptedData("Invalid bloom filter sip key".to_string()))?,
        );
        let sip_keys = [(sip_key0_0, sip_key0_1), (sip_key1_0, sip_key1_1)];

        // Verify CRC
        let stored_crc = u32::from_le_bytes(
            header[56..60]
                .try_into()
                .map_err(|_| StreamlineError::CorruptedData("Invalid bloom filter CRC".to_string()))?,
        );
        let computed_crc = crc32fast::hash(&header[0..56]);
        if stored_crc != computed_crc {
            return Err(StreamlineError::CorruptedData(
                "Bloom filter header CRC mismatch".to_string(),
            ));
        }

        // Read bitmap
        let mut bitmap = vec![0u8; bitmap_size];
        reader.read_exact(&mut bitmap)?;

        // Reconstruct filter with proper sip keys
        let filter = Bloom::from_existing(&bitmap, (bitmap_size * 8) as u64, num_hashes, sip_keys);

        // Estimate original config (we can't recover exact values)
        let expected_items = item_count.max(1);
        let false_positive_rate = DEFAULT_FALSE_POSITIVE_RATE;

        debug!(path = ?path, items = item_count, size = bitmap_size, "Loaded bloom filter");

        Ok(Self {
            filter,
            item_count,
            expected_items,
            false_positive_rate,
        })
    }
}

/// Manages bloom filters for all segments in a partition
pub struct BloomFilterIndex {
    /// Map of segment base offset to bloom filter
    filters: Arc<RwLock<std::collections::HashMap<i64, KeyBloomFilter>>>,

    /// Configuration
    config: BloomFilterConfig,

    /// Base directory for bloom filter files
    base_dir: PathBuf,
}

impl BloomFilterIndex {
    /// Create a new bloom filter index
    pub fn new(base_dir: &Path, config: BloomFilterConfig) -> Self {
        Self {
            filters: Arc::new(RwLock::new(std::collections::HashMap::new())),
            config,
            base_dir: base_dir.to_path_buf(),
        }
    }

    /// Get the bloom filter file path for a segment
    pub fn bloom_path(&self, base_offset: i64) -> PathBuf {
        self.base_dir.join(format!("{:020}.bloom", base_offset))
    }

    /// Load all bloom filters from disk
    pub async fn load_all(&self) -> Result<usize> {
        let mut loaded = 0;
        let mut filters = self.filters.write().await;

        // Find all .bloom files
        if let Ok(entries) = std::fs::read_dir(&self.base_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().map(|e| e == "bloom").unwrap_or(false) {
                    // Parse base offset from filename
                    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                        if let Ok(base_offset) = stem.parse::<i64>() {
                            match KeyBloomFilter::load(&path) {
                                Ok(filter) => {
                                    filters.insert(base_offset, filter);
                                    loaded += 1;
                                }
                                Err(e) => {
                                    warn!(path = ?path, error = %e, "Failed to load bloom filter");
                                }
                            }
                        }
                    }
                }
            }
        }

        info!(count = loaded, "Loaded bloom filters");
        Ok(loaded)
    }

    /// Create or get the bloom filter for a segment
    pub async fn get_or_create(&self, base_offset: i64) -> KeyBloomFilter {
        let mut filters = self.filters.write().await;

        // Remove existing or create new
        filters
            .remove(&base_offset)
            .unwrap_or_else(|| KeyBloomFilter::from_config(&self.config))
    }

    /// Save a bloom filter for a segment
    pub async fn save(&self, base_offset: i64, filter: KeyBloomFilter) -> Result<()> {
        let path = self.bloom_path(base_offset);
        filter.save(&path)?;

        let mut filters = self.filters.write().await;
        filters.insert(base_offset, filter);

        Ok(())
    }

    /// Add a key to a segment's bloom filter
    pub async fn add_key(&self, base_offset: i64, key: &[u8]) {
        let mut filters = self.filters.write().await;

        if let Some(filter) = filters.get_mut(&base_offset) {
            filter.add(key);
        } else {
            let mut filter = KeyBloomFilter::from_config(&self.config);
            filter.add(key);
            filters.insert(base_offset, filter);
        }
    }

    /// Check if a key might exist in any segment
    ///
    /// Returns a list of segment offsets that might contain the key.
    pub async fn find_segments_with_key(&self, key: &[u8]) -> Vec<i64> {
        let filters = self.filters.read().await;

        filters
            .iter()
            .filter(|(_, filter)| filter.might_contain(key))
            .map(|(offset, _)| *offset)
            .collect()
    }

    /// Check if a key might exist in a specific segment
    pub async fn might_contain(&self, base_offset: i64, key: &[u8]) -> bool {
        let filters = self.filters.read().await;

        filters
            .get(&base_offset)
            .map(|f| f.might_contain(key))
            .unwrap_or(true) // Assume true if no filter exists
    }

    /// Remove the bloom filter for a segment
    pub async fn remove(&self, base_offset: i64) -> Result<()> {
        let mut filters = self.filters.write().await;
        filters.remove(&base_offset);

        let path = self.bloom_path(base_offset);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }

        Ok(())
    }

    /// Persist all dirty filters to disk
    pub async fn flush(&self) -> Result<()> {
        let filters = self.filters.read().await;

        for (base_offset, filter) in filters.iter() {
            let path = self.bloom_path(*base_offset);
            filter.save(&path)?;
        }

        Ok(())
    }

    /// Get statistics about the bloom filters
    pub async fn stats(&self) -> BloomFilterStats {
        let filters = self.filters.read().await;

        let mut total_items = 0;
        let mut total_bytes = 0;
        let mut avg_fpr = 0.0;

        for filter in filters.values() {
            total_items += filter.item_count();
            total_bytes += filter.size_bytes();
            avg_fpr += filter.estimated_fpr();
        }

        let count = filters.len();
        if count > 0 {
            avg_fpr /= count as f64;
        }

        BloomFilterStats {
            filter_count: count,
            total_items,
            total_bytes,
            average_fpr: avg_fpr,
        }
    }
}

/// Statistics about bloom filters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilterStats {
    /// Number of bloom filters
    pub filter_count: usize,
    /// Total items across all filters
    pub total_items: usize,
    /// Total memory usage in bytes
    pub total_bytes: usize,
    /// Average false positive rate
    pub average_fpr: f64,
}

/// Bloom filter for consumer group membership tracking
///
/// This specialized filter tracks which message keys have been consumed
/// by a consumer group, enabling efficient redelivery detection.
pub struct ConsumerBloomFilter {
    /// The underlying filter
    filter: KeyBloomFilter,

    /// Consumer group ID
    group_id: String,

    /// Topic name
    topic: String,

    /// Partition ID
    partition: i32,
}

impl ConsumerBloomFilter {
    /// Create a new consumer bloom filter
    pub fn new(
        group_id: &str,
        topic: &str,
        partition: i32,
        expected_items: usize,
        fpr: f64,
    ) -> Self {
        Self {
            filter: KeyBloomFilter::new(expected_items, fpr),
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
        }
    }

    /// Record a consumed key
    pub fn mark_consumed(&mut self, key: &[u8]) {
        self.filter.add(key);
    }

    /// Check if a key was probably consumed
    pub fn was_consumed(&self, key: &[u8]) -> bool {
        self.filter.might_contain(key)
    }

    /// Get the consumer group ID
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Get the topic
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Get the partition
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Get the number of consumed keys tracked
    pub fn consumed_count(&self) -> usize {
        self.filter.item_count()
    }

    /// Save to a file
    pub fn save(&self, dir: &Path) -> Result<()> {
        let filename = format!(
            "{}_{}_{}_.consumer_bloom",
            self.group_id, self.topic, self.partition
        );
        let path = dir.join(filename);
        self.filter.save(&path)
    }

    /// Load from a file
    pub fn load(dir: &Path, group_id: &str, topic: &str, partition: i32) -> Result<Self> {
        let filename = format!("{}_{}_{}.consumer_bloom", group_id, topic, partition);
        let path = dir.join(filename);
        let filter = KeyBloomFilter::load(&path)?;

        Ok(Self {
            filter,
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
        })
    }
}

/// Builder for creating bloom filters with optional key extraction
pub struct BloomFilterBuilder {
    filter: KeyBloomFilter,
}

impl BloomFilterBuilder {
    /// Create a new builder with the given capacity
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        Self {
            filter: KeyBloomFilter::new(expected_items, false_positive_rate),
        }
    }

    /// Add a key (takes ownership and returns self for chaining)
    pub fn with_key(mut self, key: &[u8]) -> Self {
        self.filter.add(key);
        self
    }

    /// Add keys from an iterator (takes ownership and returns self for chaining)
    pub fn with_keys(mut self, keys: impl IntoIterator<Item = impl AsRef<[u8]>>) -> Self {
        self.filter.add_all(keys);
        self
    }

    /// Build the filter
    pub fn build(self) -> KeyBloomFilter {
        self.filter
    }
}

/// Generate bloom filter filename from segment base offset
pub fn bloom_filename(base_offset: i64) -> String {
    format!("{:020}.bloom", base_offset)
}

/// Get the bloom filter path for a segment path
pub fn bloom_path_for_segment(segment_path: &Path) -> PathBuf {
    segment_path.with_extension("bloom")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_bloom_filter_basic() {
        let mut filter = KeyBloomFilter::new(1000, 0.01);

        // Empty filter
        assert!(filter.is_empty());
        assert!(!filter.might_contain(b"test"));

        // Add keys
        filter.add(b"key1");
        filter.add(b"key2");
        filter.add(b"key3");

        assert_eq!(filter.item_count(), 3);
        assert!(!filter.is_empty());

        // Check membership - added keys should be found
        assert!(filter.might_contain(b"key1"));
        assert!(filter.might_contain(b"key2"));
        assert!(filter.might_contain(b"key3"));

        // Keys not added should probably not be found (low FPR)
        // Note: This could occasionally fail due to false positives
        let mut not_found = 0;
        for i in 0..100 {
            if !filter.might_contain(format!("nonexistent{}", i).as_bytes()) {
                not_found += 1;
            }
        }
        // With 1% FPR, we expect at least 90% to be correctly identified as not present
        assert!(
            not_found > 90,
            "Too many false positives: {}",
            100 - not_found
        );
    }

    #[test]
    fn test_bloom_filter_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bloom");

        // Create and save filter
        {
            let mut filter = KeyBloomFilter::new(100, 0.01);
            filter.add(b"persistent_key1");
            filter.add(b"persistent_key2");
            filter.save(&path).unwrap();
        }

        // Load and verify
        {
            let filter = KeyBloomFilter::load(&path).unwrap();
            assert_eq!(filter.item_count(), 2);
            assert!(filter.might_contain(b"persistent_key1"));
            assert!(filter.might_contain(b"persistent_key2"));
        }
    }

    #[test]
    fn test_bloom_filter_merge() {
        // Create first filter
        let mut filter1 = KeyBloomFilter::new(100, 0.01);
        filter1.add(b"key1");
        filter1.add(b"key2");

        // Create second filter compatible with the first (same size, hash funcs, sip keys)
        let mut filter2 = filter1.create_compatible();
        filter2.add(b"key3");
        filter2.add(b"key4");

        filter1.merge(&filter2).unwrap();

        assert_eq!(filter1.item_count(), 4);
        assert!(filter1.might_contain(b"key1"));
        assert!(filter1.might_contain(b"key2"));
        assert!(filter1.might_contain(b"key3"));
        assert!(filter1.might_contain(b"key4"));
    }

    #[test]
    fn test_bloom_filter_clear() {
        let mut filter = KeyBloomFilter::new(100, 0.01);
        filter.add(b"key1");
        filter.add(b"key2");

        assert_eq!(filter.item_count(), 2);

        filter.clear();

        assert!(filter.is_empty());
        assert_eq!(filter.item_count(), 0);
        assert!(!filter.might_contain(b"key1"));
    }

    #[test]
    fn test_bloom_filter_config() {
        let config = BloomFilterConfig::default();
        assert_eq!(config.expected_items, 100_000);
        assert_eq!(config.false_positive_rate, 0.01);
        assert!(config.enabled);

        let high_accuracy = BloomFilterConfig::high_accuracy();
        assert_eq!(high_accuracy.false_positive_rate, 0.001);

        let memory_efficient = BloomFilterConfig::memory_efficient();
        assert_eq!(memory_efficient.false_positive_rate, 0.05);
    }

    #[test]
    fn test_bloom_filter_size_estimation() {
        let config = BloomFilterConfig {
            expected_items: 10_000,
            false_positive_rate: 0.01,
            enabled: true,
        };

        let estimated_bytes = config.estimated_memory_bytes();
        // For 10,000 items at 1% FPR, expected ~12KB
        assert!(estimated_bytes > 10_000);
        assert!(estimated_bytes < 20_000);
    }

    #[tokio::test]
    async fn test_bloom_filter_index() {
        let dir = tempdir().unwrap();
        let config = BloomFilterConfig::default();
        let index = BloomFilterIndex::new(dir.path(), config);

        // Add keys to different segments
        index.add_key(0, b"segment0_key1").await;
        index.add_key(0, b"segment0_key2").await;
        index.add_key(1000, b"segment1000_key1").await;

        // Find segments containing keys
        let segments = index.find_segments_with_key(b"segment0_key1").await;
        assert!(segments.contains(&0));

        let segments = index.find_segments_with_key(b"segment1000_key1").await;
        assert!(segments.contains(&1000));

        // Key not in any segment
        let segments = index.find_segments_with_key(b"nonexistent").await;
        assert!(segments.is_empty());
    }

    #[test]
    fn test_bloom_filename_generation() {
        assert_eq!(bloom_filename(0), "00000000000000000000.bloom");
        assert_eq!(bloom_filename(12345), "00000000000000012345.bloom");

        let segment_path = Path::new("/data/partition-0/00000000000000000000.segment");
        let bloom_path = bloom_path_for_segment(segment_path);
        assert_eq!(
            bloom_path,
            Path::new("/data/partition-0/00000000000000000000.bloom")
        );
    }

    #[test]
    fn test_consumer_bloom_filter() {
        let mut filter = ConsumerBloomFilter::new("my-group", "my-topic", 0, 1000, 0.01);

        assert_eq!(filter.group_id(), "my-group");
        assert_eq!(filter.topic(), "my-topic");
        assert_eq!(filter.partition(), 0);

        filter.mark_consumed(b"message_key_1");
        filter.mark_consumed(b"message_key_2");

        assert!(filter.was_consumed(b"message_key_1"));
        assert!(filter.was_consumed(b"message_key_2"));
        assert!(!filter.was_consumed(b"message_key_3"));

        assert_eq!(filter.consumed_count(), 2);
    }

    #[test]
    fn test_bloom_filter_builder() {
        let filter = BloomFilterBuilder::new(100, 0.01)
            .with_key(b"key1")
            .with_key(b"key2")
            .with_keys(vec![b"key3".to_vec(), b"key4".to_vec()])
            .build();

        assert_eq!(filter.item_count(), 4);
        assert!(filter.might_contain(b"key1"));
        assert!(filter.might_contain(b"key4"));
    }

    #[test]
    fn test_estimated_fpr() {
        let mut filter = KeyBloomFilter::new(100, 0.01);

        // Empty filter has 0 FPR
        assert_eq!(filter.estimated_fpr(), 0.0);

        // Add some items
        for i in 0..100 {
            filter.add(format!("key{}", i).as_bytes());
        }

        // FPR should be positive and reasonable
        let fpr = filter.estimated_fpr();
        assert!(fpr > 0.0, "FPR should be positive, got {}", fpr);
        // At 100 items with 1% target, actual FPR should be in reasonable range
        assert!(fpr < 0.5, "FPR should be reasonable, got {}", fpr);
    }

    #[test]
    fn test_filter_keys() {
        let mut filter = KeyBloomFilter::new(100, 0.01);
        filter.add(b"key1");
        filter.add(b"key2");

        let keys: Vec<&[u8]> = vec![b"key1", b"key2", b"key3", b"key4"];
        let matches = filter.filter_keys(&keys);

        assert!(matches.contains(&b"key1".as_slice()));
        assert!(matches.contains(&b"key2".as_slice()));
        // key3 and key4 might or might not be in the result (false positives)
    }
}
