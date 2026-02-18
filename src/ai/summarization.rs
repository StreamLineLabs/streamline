//! Smart Summarization for Stream Data
//!
//! Provides aggregate statistics per time window, automatic JSON schema
//! inference, and cardinality estimation via HyperLogLog.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

// ─── Aggregate Statistics ────────────────────────────────────────────

/// Aggregate statistics for a window of numeric values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateStats {
    pub count: u64,
    pub sum: f64,
    pub avg: f64,
    pub min: f64,
    pub max: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub std_dev: f64,
}

impl AggregateStats {
    /// Compute stats from a slice of values.
    pub fn from_values(values: &[f64]) -> Self {
        if values.is_empty() {
            return Self {
                count: 0,
                sum: 0.0,
                avg: 0.0,
                min: 0.0,
                max: 0.0,
                p50: 0.0,
                p95: 0.0,
                p99: 0.0,
                std_dev: 0.0,
            };
        }

        let count = values.len() as u64;
        let sum: f64 = values.iter().sum();
        let avg = sum / count as f64;
        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        let variance = values.iter().map(|v| (v - avg).powi(2)).sum::<f64>() / count as f64;
        let std_dev = variance.sqrt();

        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let p50 = percentile(&sorted, 0.50);
        let p95 = percentile(&sorted, 0.95);
        let p99 = percentile(&sorted, 0.99);

        Self {
            count,
            sum,
            avg,
            min,
            max,
            p50,
            p95,
            p99,
            std_dev,
        }
    }
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (pct * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// ─── Schema Inference ────────────────────────────────────────────────

/// Inferred JSON field type.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InferredType {
    String,
    Number,
    Boolean,
    Null,
    Array,
    Object,
    Mixed,
}

/// A single inferred field in a JSON schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferredField {
    pub name: String,
    pub field_type: InferredType,
    /// How many messages contained this field
    pub occurrence_count: u64,
    /// Percentage of messages that had this field
    pub occurrence_pct: f64,
}

/// Inferred schema from a collection of JSON messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferredSchema {
    pub fields: Vec<InferredField>,
    pub messages_analysed: u64,
}

/// Infer a schema from a set of JSON message strings.
pub fn infer_schema(messages: &[&str]) -> InferredSchema {
    let total = messages.len() as u64;
    if total == 0 {
        return InferredSchema {
            fields: Vec::new(),
            messages_analysed: 0,
        };
    }

    // Track (field_name → type counts)
    let mut field_types: HashMap<String, HashMap<InferredType, u64>> = HashMap::new();

    for msg in messages {
        if let Ok(serde_json::Value::Object(map)) = serde_json::from_str::<serde_json::Value>(msg)
        {
            for (key, val) in &map {
                let ty = json_value_type(val);
                *field_types
                    .entry(key.clone())
                    .or_default()
                    .entry(ty)
                    .or_insert(0) += 1;
            }
        }
    }

    let mut fields: Vec<InferredField> = field_types
        .into_iter()
        .map(|(name, types)| {
            let occurrence_count: u64 = types.values().sum();
            let occurrence_pct = occurrence_count as f64 / total as f64 * 100.0;

            // Determine dominant type
            let field_type = if types.len() == 1 {
                types.into_keys().next().unwrap_or(InferredType::Mixed)
            } else {
                // Pick the most common, fall back to Mixed
                let mut entries: Vec<_> = types.into_iter().collect();
                entries.sort_by(|a, b| b.1.cmp(&a.1));
                if entries[0].1 as f64 / occurrence_count as f64 > 0.8 {
                    entries[0].0.clone()
                } else {
                    InferredType::Mixed
                }
            };

            InferredField {
                name,
                field_type,
                occurrence_count,
                occurrence_pct,
            }
        })
        .collect();

    fields.sort_by(|a, b| b.occurrence_count.cmp(&a.occurrence_count));

    InferredSchema {
        fields,
        messages_analysed: total,
    }
}

fn json_value_type(val: &serde_json::Value) -> InferredType {
    match val {
        serde_json::Value::Null => InferredType::Null,
        serde_json::Value::Bool(_) => InferredType::Boolean,
        serde_json::Value::Number(_) => InferredType::Number,
        serde_json::Value::String(_) => InferredType::String,
        serde_json::Value::Array(_) => InferredType::Array,
        serde_json::Value::Object(_) => InferredType::Object,
    }
}

// ─── HyperLogLog Cardinality Estimation ──────────────────────────────

/// A simple HyperLogLog implementation for cardinality estimation.
#[derive(Debug, Clone)]
pub struct HyperLogLog {
    /// Register array (each register stores the max leading-zeros count)
    registers: Vec<u8>,
    /// Number of registers (m = 2^p)
    m: usize,
    /// Precision parameter
    p: u8,
}

impl HyperLogLog {
    /// Create a new HyperLogLog with the given precision (4..=16).
    /// Higher precision = more memory but better accuracy.
    pub fn new(precision: u8) -> Self {
        let p = precision.clamp(4, 16);
        let m = 1usize << p;
        Self {
            registers: vec![0u8; m],
            m,
            p,
        }
    }

    /// Add an item by hashing it with a simple hash function.
    pub fn add(&mut self, item: &str) {
        let hash = Self::hash(item);
        let idx = (hash as usize) & (self.m - 1);
        let w = hash >> self.p;
        // Count leading zeros of the remaining bits, plus 1
        let leading_zeros = if w == 0 {
            (64 - self.p) as u8 + 1
        } else {
            w.leading_zeros() as u8 - self.p + 1
        };
        if leading_zeros > self.registers[idx] {
            self.registers[idx] = leading_zeros;
        }
    }

    /// Estimate the cardinality.
    pub fn estimate(&self) -> u64 {
        let m = self.m as f64;
        // Alpha constant for bias correction
        let alpha = match self.m {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };

        let raw: f64 = self
            .registers
            .iter()
            .map(|&r| 2.0_f64.powi(-(r as i32)))
            .sum();
        let estimate = alpha * m * m / raw;

        // Small range correction
        if estimate <= 2.5 * m {
            let zeros = self.registers.iter().filter(|&&r| r == 0).count() as f64;
            if zeros > 0.0 {
                return (m * (m / zeros).ln()) as u64;
            }
        }

        estimate as u64
    }

    /// Merge another HyperLogLog into this one.
    pub fn merge(&mut self, other: &HyperLogLog) {
        assert_eq!(self.m, other.m, "HLL precision must match for merge");
        for (r, &o) in self.registers.iter_mut().zip(other.registers.iter()) {
            *r = (*r).max(o);
        }
    }

    fn hash(item: &str) -> u64 {
        // FNV-1a 64-bit hash
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in item.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }
}

// ─── Stream Summarizer ───────────────────────────────────────────────

/// Configuration for stream summarization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummarizationConfig {
    /// Enable summarization
    pub enabled: bool,
    /// Window size for numeric aggregation
    pub window_size: usize,
    /// HyperLogLog precision (4–16)
    pub hll_precision: u8,
    /// Maximum messages to keep for schema inference
    pub max_schema_samples: usize,
}

impl Default for SummarizationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            window_size: 4096,
            hll_precision: 12,
            max_schema_samples: 200,
        }
    }
}

/// Per-topic summarizer state.
struct TopicSummarizer {
    /// Numeric values for aggregation
    values: VecDeque<f64>,
    max_values: usize,
    /// Recent raw messages for schema inference
    recent_messages: VecDeque<String>,
    max_messages: usize,
    /// HyperLogLog for cardinality estimation
    hll: HyperLogLog,
    /// Total messages ingested
    total_messages: u64,
}

impl TopicSummarizer {
    fn new(config: &SummarizationConfig) -> Self {
        Self {
            values: VecDeque::with_capacity(config.window_size),
            max_values: config.window_size,
            recent_messages: VecDeque::with_capacity(config.max_schema_samples),
            max_messages: config.max_schema_samples,
            hll: HyperLogLog::new(config.hll_precision),
            total_messages: 0,
        }
    }

    fn add_message(&mut self, message: &str) {
        self.total_messages += 1;
        self.hll.add(message);

        // Try to extract a numeric value from the message
        if let Ok(v) = message.trim().parse::<f64>() {
            if self.values.len() >= self.max_values {
                self.values.pop_front();
            }
            self.values.push_back(v);
        } else if let Ok(serde_json::Value::Object(map)) = serde_json::from_str::<serde_json::Value>(message) {
            // Extract numeric fields from JSON
            for val in map.values() {
                if let Some(n) = val.as_f64() {
                    if self.values.len() >= self.max_values {
                        self.values.pop_front();
                    }
                    self.values.push_back(n);
                }
            }
        }

        if self.recent_messages.len() >= self.max_messages {
            self.recent_messages.pop_front();
        }
        self.recent_messages.push_back(message.to_string());
    }
}

/// Summary of a topic's data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicSummary {
    /// Topic name
    pub topic: String,
    /// Total messages ingested
    pub total_messages: u64,
    /// Estimated unique messages (HyperLogLog)
    pub estimated_cardinality: u64,
    /// Aggregate statistics over numeric values in the window
    pub stats: Option<AggregateStats>,
    /// Inferred schema from recent JSON messages
    pub schema: Option<InferredSchema>,
}

/// Multi-topic stream summarizer.
pub struct StreamSummarizer {
    config: SummarizationConfig,
    topics: Arc<RwLock<HashMap<String, TopicSummarizer>>>,
}

impl StreamSummarizer {
    /// Create a new stream summarizer.
    pub fn new(config: SummarizationConfig) -> Self {
        Self {
            config,
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Ingest a message for a topic.
    pub async fn add_message(&self, topic: &str, message: &str) {
        let mut topics = self.topics.write().await;
        let summarizer = topics
            .entry(topic.to_string())
            .or_insert_with(|| TopicSummarizer::new(&self.config));
        summarizer.add_message(message);
    }

    /// Get a summary for a topic.
    pub async fn summarize(&self, topic: &str) -> Option<TopicSummary> {
        let topics = self.topics.read().await;
        let s = topics.get(topic)?;

        let stats = if s.values.is_empty() {
            None
        } else {
            let vals: Vec<f64> = s.values.iter().copied().collect();
            Some(AggregateStats::from_values(&vals))
        };

        let schema = if s.recent_messages.is_empty() {
            None
        } else {
            let msgs: Vec<&str> = s.recent_messages.iter().map(|s| s.as_str()).collect();
            let inferred = infer_schema(&msgs);
            if inferred.fields.is_empty() {
                None
            } else {
                Some(inferred)
            }
        };

        Some(TopicSummary {
            topic: topic.to_string(),
            total_messages: s.total_messages,
            estimated_cardinality: s.hll.estimate(),
            stats,
            schema,
        })
    }

    /// List tracked topics.
    pub async fn topics(&self) -> Vec<String> {
        self.topics.read().await.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_stats() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let stats = AggregateStats::from_values(&values);
        assert_eq!(stats.count, 10);
        assert!((stats.avg - 5.5).abs() < 0.01);
        assert!((stats.min - 1.0).abs() < f64::EPSILON);
        assert!((stats.max - 10.0).abs() < f64::EPSILON);
        assert!((stats.p50 - 5.0).abs() < 1.1); // Approximate
    }

    #[test]
    fn test_aggregate_stats_empty() {
        let stats = AggregateStats::from_values(&[]);
        assert_eq!(stats.count, 0);
    }

    #[test]
    fn test_schema_inference() {
        let messages = vec![
            r#"{"name": "Alice", "age": 30, "active": true}"#,
            r#"{"name": "Bob", "age": 25, "active": false}"#,
            r#"{"name": "Charlie", "age": 35}"#,
        ];
        let schema = infer_schema(&messages.iter().map(|s| *s).collect::<Vec<_>>());
        assert_eq!(schema.messages_analysed, 3);
        assert!(schema.fields.iter().any(|f| f.name == "name" && f.field_type == InferredType::String));
        assert!(schema.fields.iter().any(|f| f.name == "age" && f.field_type == InferredType::Number));
    }

    #[test]
    fn test_hyperloglog_basic() {
        let mut hll = HyperLogLog::new(12);
        for i in 0..1000 {
            hll.add(&format!("item-{}", i));
        }
        let est = hll.estimate();
        // Should be within ~10% of 1000
        assert!(est > 800 && est < 1200, "estimate {} not in range", est);
    }

    #[test]
    fn test_hyperloglog_merge() {
        let mut hll1 = HyperLogLog::new(10);
        let mut hll2 = HyperLogLog::new(10);
        for i in 0..500 {
            hll1.add(&format!("item-{}", i));
        }
        for i in 500..1000 {
            hll2.add(&format!("item-{}", i));
        }
        hll1.merge(&hll2);
        let est = hll1.estimate();
        assert!(est > 800 && est < 1200, "merged estimate {} not in range", est);
    }

    #[tokio::test]
    async fn test_stream_summarizer() {
        let summarizer = StreamSummarizer::new(SummarizationConfig::default());

        for i in 0..50 {
            summarizer.add_message("test-topic", &format!(r#"{{"value": {}, "name": "sensor-{}"}}"#, i * 10, i % 5)).await;
        }

        let summary = summarizer.summarize("test-topic").await.unwrap();
        assert_eq!(summary.total_messages, 50);
        assert!(summary.stats.is_some());
        assert!(summary.schema.is_some());

        let schema = summary.schema.unwrap();
        assert!(schema.fields.iter().any(|f| f.name == "value"));
    }
}
