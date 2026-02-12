//! Semantic Partitioner
//!
//! Routes messages to partitions based on semantic similarity using K-means clustering.
//! Messages with similar content are routed to the same partition, improving locality
//! for semantic queries.

use crate::ai::providers::EmbeddingProvider;
use crate::error::{Result, StreamlineError};
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Configuration for semantic partitioning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticPartitionerConfig {
    /// Number of partitions (clusters)
    pub num_partitions: usize,
    /// Embedding dimension
    pub dimension: usize,
    /// Number of K-means iterations for initialization
    pub init_iterations: usize,
    /// Online learning rate for centroid updates
    pub learning_rate: f32,
    /// Minimum samples before enabling online learning
    pub min_samples_for_learning: usize,
    /// Enable adaptive learning rate
    pub adaptive_learning: bool,
    /// Fallback to hash partitioning if embedding fails
    pub fallback_to_hash: bool,
}

impl Default for SemanticPartitionerConfig {
    fn default() -> Self {
        Self {
            num_partitions: 8,
            dimension: 384,
            init_iterations: 10,
            learning_rate: 0.01,
            min_samples_for_learning: 100,
            adaptive_learning: true,
            fallback_to_hash: true,
        }
    }
}

impl SemanticPartitionerConfig {
    /// Create configuration for a specific number of partitions
    pub fn with_partitions(num_partitions: usize, dimension: usize) -> Self {
        Self {
            num_partitions,
            dimension,
            ..Default::default()
        }
    }
}

/// A cluster centroid
#[derive(Debug, Clone)]
struct Centroid {
    /// Centroid vector
    vector: Vec<f32>,
    /// Number of samples assigned to this centroid
    sample_count: u64,
    /// Sum of squared distances (for variance tracking)
    sum_squared_distance: f64,
}

impl Centroid {
    fn new(dimension: usize) -> Self {
        Self {
            vector: vec![0.0; dimension],
            sample_count: 0,
            sum_squared_distance: 0.0,
        }
    }

    fn from_vector(vector: Vec<f32>) -> Self {
        Self {
            vector,
            sample_count: 0,
            sum_squared_distance: 0.0,
        }
    }

    /// Update centroid with online learning
    fn update(&mut self, sample: &[f32], learning_rate: f32) {
        for (c, &s) in self.vector.iter_mut().zip(sample.iter()) {
            *c += learning_rate * (s - *c);
        }
        self.sample_count += 1;
    }

    /// Compute distance to a sample
    fn distance(&self, sample: &[f32]) -> f32 {
        // Cosine distance
        let dot: f32 = self
            .vector
            .iter()
            .zip(sample.iter())
            .map(|(a, b)| a * b)
            .sum();
        let norm_a: f32 = self.vector.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = sample.iter().map(|x| x * x).sum::<f32>().sqrt();

        if norm_a == 0.0 || norm_b == 0.0 {
            1.0
        } else {
            1.0 - (dot / (norm_a * norm_b))
        }
    }
}

/// Semantic partitioner using K-means clustering
pub struct SemanticPartitioner {
    config: SemanticPartitionerConfig,
    /// Embedding provider
    provider: Arc<dyn EmbeddingProvider>,
    /// Cluster centroids
    centroids: Arc<RwLock<Vec<Centroid>>>,
    /// Whether centroids are initialized
    initialized: Arc<RwLock<bool>>,
    /// Initialization samples buffer
    init_buffer: Arc<RwLock<Vec<Vec<f32>>>>,
    /// Statistics
    stats: PartitionerStats,
}

impl SemanticPartitioner {
    /// Create a new semantic partitioner
    pub fn new(config: SemanticPartitionerConfig, provider: Arc<dyn EmbeddingProvider>) -> Self {
        let centroids = (0..config.num_partitions)
            .map(|_| Centroid::new(config.dimension))
            .collect();

        Self {
            config,
            provider,
            centroids: Arc::new(RwLock::new(centroids)),
            initialized: Arc::new(RwLock::new(false)),
            init_buffer: Arc::new(RwLock::new(Vec::new())),
            stats: PartitionerStats::default(),
        }
    }

    /// Partition a message by its text content
    pub async fn partition(&self, text: &str, key: Option<&str>) -> Result<i32> {
        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);

        // Generate embedding
        let embedding = match self.provider.embed(text).await {
            Ok(emb) => emb,
            Err(e) => {
                self.stats.embedding_errors.fetch_add(1, Ordering::Relaxed);
                if self.config.fallback_to_hash {
                    // Fallback to hash-based partitioning
                    let hash_input = key.unwrap_or(text);
                    let hash = simple_hash(hash_input);
                    return Ok((hash % self.config.num_partitions as u32) as i32);
                } else {
                    return Err(StreamlineError::AI(format!("Embedding failed: {}", e)));
                }
            }
        };

        self.partition_by_vector(&embedding).await
    }

    /// Partition by pre-computed vector
    pub async fn partition_by_vector(&self, vector: &[f32]) -> Result<i32> {
        if vector.len() != self.config.dimension {
            return Err(StreamlineError::InvalidData(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.config.dimension,
                vector.len()
            )));
        }

        // Check if initialized
        let is_initialized = *self.initialized.read().await;

        if !is_initialized {
            // Add to initialization buffer
            let mut buffer = self.init_buffer.write().await;
            buffer.push(vector.to_vec());

            // Check if we have enough samples
            if buffer.len() >= self.config.min_samples_for_learning {
                drop(buffer);
                self.initialize_centroids().await?;
            } else {
                // Use round-robin until initialized
                let partition = (buffer.len() % self.config.num_partitions) as i32;
                return Ok(partition);
            }
        }

        // Find nearest centroid
        let centroids = self.centroids.read().await;
        let mut best_partition = 0;
        let mut best_distance = f32::MAX;

        for (i, centroid) in centroids.iter().enumerate() {
            let dist = centroid.distance(vector);
            if dist < best_distance {
                best_distance = dist;
                best_partition = i;
            }
        }

        drop(centroids);

        // Online learning update
        if self.config.learning_rate > 0.0 {
            let mut centroids = self.centroids.write().await;
            let learning_rate = if self.config.adaptive_learning {
                // Adaptive learning rate: decreases with sample count
                let count = centroids[best_partition].sample_count as f32;
                self.config.learning_rate / (1.0 + count * 0.001)
            } else {
                self.config.learning_rate
            };

            centroids[best_partition].update(vector, learning_rate);
            centroids[best_partition].sum_squared_distance += best_distance as f64;
        }

        self.stats
            .partitions_assigned
            .fetch_add(1, Ordering::Relaxed);

        Ok(best_partition as i32)
    }

    /// Initialize centroids using K-means++
    async fn initialize_centroids(&self) -> Result<()> {
        let mut init = self.initialized.write().await;
        if *init {
            return Ok(());
        }

        info!("Initializing semantic partitioner centroids");

        let buffer = self.init_buffer.read().await;
        if buffer.is_empty() {
            return Ok(());
        }

        let samples: Vec<_> = buffer.iter().cloned().collect();
        drop(buffer);

        // K-means++ initialization
        let mut rng = rand::thread_rng();
        let mut centroid_vectors = Vec::with_capacity(self.config.num_partitions);

        // First centroid: random sample
        let first_idx = rng.gen_range(0..samples.len());
        centroid_vectors.push(samples[first_idx].clone());

        // Remaining centroids: probability proportional to squared distance
        while centroid_vectors.len() < self.config.num_partitions {
            let mut distances: Vec<f32> = samples
                .iter()
                .map(|sample| {
                    centroid_vectors
                        .iter()
                        .map(|c| cosine_distance(sample, c))
                        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                        .unwrap_or(f32::MAX)
                })
                .collect();

            // Square distances for probability
            for d in &mut distances {
                *d = *d * *d;
            }

            let total: f32 = distances.iter().sum();
            if total == 0.0 {
                // All samples are identical, just pick random
                let idx = rng.gen_range(0..samples.len());
                centroid_vectors.push(samples[idx].clone());
            } else {
                // Weighted random selection
                let threshold = rng.gen::<f32>() * total;
                let mut cumsum = 0.0;
                for (i, &d) in distances.iter().enumerate() {
                    cumsum += d;
                    if cumsum >= threshold {
                        centroid_vectors.push(samples[i].clone());
                        break;
                    }
                }
            }
        }

        // Run K-means iterations
        let mut assignments = vec![0usize; samples.len()];
        for _ in 0..self.config.init_iterations {
            // Assign samples to nearest centroid
            for (i, sample) in samples.iter().enumerate() {
                let mut best_cluster = 0;
                let mut best_dist = f32::MAX;
                for (j, centroid) in centroid_vectors.iter().enumerate() {
                    let dist = cosine_distance(sample, centroid);
                    if dist < best_dist {
                        best_dist = dist;
                        best_cluster = j;
                    }
                }
                assignments[i] = best_cluster;
            }

            // Update centroids
            for (j, centroid) in centroid_vectors.iter_mut().enumerate() {
                let assigned: Vec<_> = samples
                    .iter()
                    .zip(assignments.iter())
                    .filter(|(_, &a)| a == j)
                    .map(|(s, _)| s)
                    .collect();

                if !assigned.is_empty() {
                    // Average of assigned samples
                    let dim = centroid.len();
                    for d in 0..dim {
                        centroid[d] =
                            assigned.iter().map(|s| s[d]).sum::<f32>() / assigned.len() as f32;
                    }
                }
            }
        }

        // Update centroids
        let mut centroids = self.centroids.write().await;
        for (i, vec) in centroid_vectors.into_iter().enumerate() {
            centroids[i] = Centroid::from_vector(vec);
        }

        *init = true;
        info!(
            "Initialized {} centroids from {} samples",
            self.config.num_partitions,
            samples.len()
        );

        // Clear init buffer
        self.init_buffer.write().await.clear();

        Ok(())
    }

    /// Get partition distribution statistics
    pub async fn partition_stats(&self) -> Vec<PartitionInfo> {
        let centroids = self.centroids.read().await;
        centroids
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let variance = if c.sample_count > 0 {
                    c.sum_squared_distance / c.sample_count as f64
                } else {
                    0.0
                };

                PartitionInfo {
                    partition: i as i32,
                    sample_count: c.sample_count,
                    variance,
                }
            })
            .collect()
    }

    /// Get overall statistics
    pub fn stats(&self) -> PartitionerStatsSnapshot {
        PartitionerStatsSnapshot {
            total_requests: self.stats.total_requests.load(Ordering::Relaxed),
            partitions_assigned: self.stats.partitions_assigned.load(Ordering::Relaxed),
            embedding_errors: self.stats.embedding_errors.load(Ordering::Relaxed),
        }
    }

    /// Check if partitioner is initialized
    pub async fn is_initialized(&self) -> bool {
        *self.initialized.read().await
    }

    /// Get number of partitions
    pub fn num_partitions(&self) -> usize {
        self.config.num_partitions
    }

    /// Force re-initialization with new samples
    pub async fn reset(&self) {
        let mut init = self.initialized.write().await;
        *init = false;

        let mut centroids = self.centroids.write().await;
        for centroid in centroids.iter_mut() {
            *centroid = Centroid::new(self.config.dimension);
        }

        self.init_buffer.write().await.clear();

        info!("Reset semantic partitioner");
    }
}

/// Per-partition statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    /// Partition number
    pub partition: i32,
    /// Number of samples assigned
    pub sample_count: u64,
    /// Variance of distances to centroid
    pub variance: f64,
}

/// Internal statistics
#[derive(Debug, Default)]
struct PartitionerStats {
    total_requests: AtomicU64,
    partitions_assigned: AtomicU64,
    embedding_errors: AtomicU64,
}

/// Statistics snapshot
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PartitionerStatsSnapshot {
    /// Total partitioning requests
    pub total_requests: u64,
    /// Total partitions assigned (after initialization)
    pub partitions_assigned: u64,
    /// Total embedding errors
    pub embedding_errors: u64,
}

/// Simple hash function for fallback partitioning
fn simple_hash(s: &str) -> u32 {
    let mut hash: u32 = 5381;
    for c in s.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(c as u32);
    }
    hash
}

/// Compute cosine distance between two vectors
fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        1.0
    } else {
        1.0 - (dot / (norm_a * norm_b))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::providers::MockProvider;

    #[tokio::test]
    async fn test_partition_before_init() {
        let provider = Arc::new(MockProvider::new(64));
        let config = SemanticPartitionerConfig::with_partitions(4, 64);
        let partitioner = SemanticPartitioner::new(config, provider);

        // Should use round-robin before initialization
        let p1 = partitioner.partition("hello", None).await.unwrap();
        let p2 = partitioner.partition("world", None).await.unwrap();
        let p3 = partitioner.partition("test", None).await.unwrap();

        assert!((0..4).contains(&p1));
        assert!((0..4).contains(&p2));
        assert!((0..4).contains(&p3));
    }

    #[tokio::test]
    async fn test_partition_after_init() {
        let provider = Arc::new(MockProvider::new(32));
        let mut config = SemanticPartitionerConfig::with_partitions(4, 32);
        config.min_samples_for_learning = 10;
        let partitioner = SemanticPartitioner::new(config, provider);

        // Add enough samples to trigger initialization
        for i in 0..15 {
            let text = format!("sample text number {}", i);
            let _ = partitioner.partition(&text, None).await;
        }

        assert!(partitioner.is_initialized().await);

        // Now partitioning should work with centroids
        let p = partitioner.partition("another sample", None).await.unwrap();
        assert!((0..4).contains(&p));
    }

    #[tokio::test]
    async fn test_partition_consistency() {
        let provider = Arc::new(MockProvider::new(32));
        let mut config = SemanticPartitionerConfig::with_partitions(4, 32);
        config.learning_rate = 0.0; // Disable online learning for consistency
        config.min_samples_for_learning = 5;
        let partitioner = SemanticPartitioner::new(config, provider);

        // Initialize
        for i in 0..10 {
            let _ = partitioner.partition(&format!("init {}", i), None).await;
        }

        // Same text should go to same partition
        let p1 = partitioner
            .partition("consistent text", None)
            .await
            .unwrap();
        let p2 = partitioner
            .partition("consistent text", None)
            .await
            .unwrap();
        assert_eq!(p1, p2);
    }

    #[tokio::test]
    async fn test_partition_stats() {
        let provider = Arc::new(MockProvider::new(32));
        let config = SemanticPartitionerConfig::with_partitions(4, 32);
        let partitioner = SemanticPartitioner::new(config, provider);

        for i in 0..5 {
            let _ = partitioner.partition(&format!("text {}", i), None).await;
        }

        let stats = partitioner.stats();
        assert_eq!(stats.total_requests, 5);
    }

    #[tokio::test]
    async fn test_reset() {
        let provider = Arc::new(MockProvider::new(32));
        let mut config = SemanticPartitionerConfig::with_partitions(4, 32);
        config.min_samples_for_learning = 5;
        let partitioner = SemanticPartitioner::new(config, provider);

        // Initialize
        for i in 0..10 {
            let _ = partitioner.partition(&format!("text {}", i), None).await;
        }
        assert!(partitioner.is_initialized().await);

        // Reset
        partitioner.reset().await;
        assert!(!partitioner.is_initialized().await);
    }

    #[test]
    fn test_cosine_distance() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let c = vec![0.0, 1.0, 0.0];

        assert!((cosine_distance(&a, &b) - 0.0).abs() < 0.001);
        assert!((cosine_distance(&a, &c) - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_simple_hash() {
        let h1 = simple_hash("hello");
        let h2 = simple_hash("hello");
        let h3 = simple_hash("world");

        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
    }
}
