//! Vector Streaming Module
//!
//! Provides native vector types for streaming with efficient storage,
//! transmission, and querying of high-dimensional vectors.
//!
//! # Features
//!
//! - **Native Vector Types**: First-class vector support in record schema
//! - **Efficient Encoding**: Quantized vectors for reduced storage
//! - **Vector Indexes**: HNSW and IVF indexes for fast similarity search
//! - **RAG Support**: Retrieval-Augmented Generation pipeline integration
//! - **Real-time Updates**: Streaming vector index updates

use crate::error::{Result, StreamlineError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Vector dimension limits
pub const MIN_VECTOR_DIM: usize = 2;
pub const MAX_VECTOR_DIM: usize = 8192;

/// Default vector dimension
pub const DEFAULT_VECTOR_DIM: usize = 1536;

/// Vector encoding types
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorEncoding {
    /// Full precision float32
    #[default]
    Float32,
    /// Half precision float16
    Float16,
    /// 8-bit integer quantization
    Int8,
    /// 4-bit quantization (2 values per byte)
    Int4,
    /// Binary quantization (32 dims per 4 bytes)
    Binary,
}

impl VectorEncoding {
    /// Bytes per dimension for this encoding
    pub fn bytes_per_dim(&self) -> f64 {
        match self {
            VectorEncoding::Float32 => 4.0,
            VectorEncoding::Float16 => 2.0,
            VectorEncoding::Int8 => 1.0,
            VectorEncoding::Int4 => 0.5,
            VectorEncoding::Binary => 0.03125, // 1/32
        }
    }

    /// Calculate storage size for vector
    pub fn storage_size(&self, dims: usize) -> usize {
        match self {
            VectorEncoding::Float32 => dims * 4,
            VectorEncoding::Float16 => dims * 2,
            VectorEncoding::Int8 => dims,
            VectorEncoding::Int4 => dims.div_ceil(2),
            VectorEncoding::Binary => dims.div_ceil(32) * 4,
        }
    }
}

/// Native vector type for streaming
#[derive(Debug, Clone)]
pub struct StreamVector {
    /// Vector ID
    pub id: String,
    /// Dimension count
    pub dimensions: usize,
    /// Encoding type
    pub encoding: VectorEncoding,
    /// Raw vector data
    data: Bytes,
    /// Optional metadata
    pub metadata: HashMap<String, String>,
    /// Source topic
    pub source_topic: Option<String>,
    /// Source offset
    pub source_offset: Option<i64>,
    /// Timestamp
    pub timestamp: i64,
}

impl StreamVector {
    /// Create a new vector from float32 values
    pub fn from_f32(id: impl Into<String>, values: &[f32]) -> Result<Self> {
        if values.len() < MIN_VECTOR_DIM || values.len() > MAX_VECTOR_DIM {
            return Err(StreamlineError::Validation(format!(
                "Vector dimension {} out of range [{}, {}]",
                values.len(),
                MIN_VECTOR_DIM,
                MAX_VECTOR_DIM
            )));
        }

        let mut data = BytesMut::with_capacity(values.len() * 4);
        for &v in values {
            data.put_f32_le(v);
        }

        Ok(Self {
            id: id.into(),
            dimensions: values.len(),
            encoding: VectorEncoding::Float32,
            data: data.freeze(),
            metadata: HashMap::new(),
            source_topic: None,
            source_offset: None,
            timestamp: chrono::Utc::now().timestamp_millis(),
        })
    }

    /// Create a vector from bytes with specified encoding
    pub fn from_bytes(
        id: impl Into<String>,
        dimensions: usize,
        encoding: VectorEncoding,
        data: Bytes,
    ) -> Result<Self> {
        let expected_size = encoding.storage_size(dimensions);
        if data.len() != expected_size {
            return Err(StreamlineError::Validation(format!(
                "Vector data size {} doesn't match expected {} for {} dimensions with {:?} encoding",
                data.len(),
                expected_size,
                dimensions,
                encoding
            )));
        }

        Ok(Self {
            id: id.into(),
            dimensions,
            encoding,
            data,
            metadata: HashMap::new(),
            source_topic: None,
            source_offset: None,
            timestamp: chrono::Utc::now().timestamp_millis(),
        })
    }

    /// Get vector as float32 values
    pub fn to_f32(&self) -> Result<Vec<f32>> {
        match self.encoding {
            VectorEncoding::Float32 => {
                let mut result = Vec::with_capacity(self.dimensions);
                let mut cursor = self.data.clone();
                while cursor.remaining() >= 4 {
                    result.push(cursor.get_f32_le());
                }
                Ok(result)
            }
            VectorEncoding::Float16 => {
                // Convert from half precision
                let mut result = Vec::with_capacity(self.dimensions);
                let mut cursor = self.data.clone();
                while cursor.remaining() >= 2 {
                    let bits = cursor.get_u16_le();
                    result.push(half_to_float(bits));
                }
                Ok(result)
            }
            VectorEncoding::Int8 => {
                // Dequantize from int8 (assuming range [-1, 1])
                let mut result = Vec::with_capacity(self.dimensions);
                for &byte in self.data.iter() {
                    let signed = byte as i8;
                    result.push(signed as f32 / 127.0);
                }
                Ok(result)
            }
            VectorEncoding::Int4 => {
                // Dequantize from int4
                let mut result = Vec::with_capacity(self.dimensions);
                for &byte in self.data.iter() {
                    let lo = (byte & 0x0F) as i8 - 8;
                    let hi = ((byte >> 4) & 0x0F) as i8 - 8;
                    result.push(lo as f32 / 7.0);
                    if result.len() < self.dimensions {
                        result.push(hi as f32 / 7.0);
                    }
                }
                Ok(result)
            }
            VectorEncoding::Binary => {
                // Binary to sparse float
                let mut result = Vec::with_capacity(self.dimensions);
                let mut cursor = self.data.clone();
                let mut dim = 0;
                while cursor.remaining() >= 4 && dim < self.dimensions {
                    let bits = cursor.get_u32_le();
                    for i in 0..32 {
                        if dim >= self.dimensions {
                            break;
                        }
                        result.push(if bits & (1 << i) != 0 { 1.0 } else { -1.0 });
                        dim += 1;
                    }
                }
                Ok(result)
            }
        }
    }

    /// Quantize to a different encoding
    pub fn quantize(&self, target_encoding: VectorEncoding) -> Result<Self> {
        if self.encoding == target_encoding {
            return Ok(self.clone());
        }

        let values = self.to_f32()?;
        Self::from_f32_with_encoding(&self.id, &values, target_encoding)
    }

    /// Create from f32 with specific encoding
    pub fn from_f32_with_encoding(
        id: impl Into<String>,
        values: &[f32],
        encoding: VectorEncoding,
    ) -> Result<Self> {
        let dimensions = values.len();
        if !(MIN_VECTOR_DIM..=MAX_VECTOR_DIM).contains(&dimensions) {
            return Err(StreamlineError::Validation(format!(
                "Vector dimension {} out of range",
                dimensions
            )));
        }

        let data = match encoding {
            VectorEncoding::Float32 => {
                let mut buf = BytesMut::with_capacity(dimensions * 4);
                for &v in values {
                    buf.put_f32_le(v);
                }
                buf.freeze()
            }
            VectorEncoding::Float16 => {
                let mut buf = BytesMut::with_capacity(dimensions * 2);
                for &v in values {
                    buf.put_u16_le(float_to_half(v));
                }
                buf.freeze()
            }
            VectorEncoding::Int8 => {
                let mut buf = BytesMut::with_capacity(dimensions);
                for &v in values {
                    let clamped = v.clamp(-1.0, 1.0);
                    let quantized = (clamped * 127.0).round() as i8;
                    buf.put_i8(quantized);
                }
                buf.freeze()
            }
            VectorEncoding::Int4 => {
                let mut buf = BytesMut::with_capacity(dimensions.div_ceil(2));
                for chunk in values.chunks(2) {
                    let lo = ((chunk[0].clamp(-1.0, 1.0) * 7.0).round() as i8 + 8) as u8;
                    let hi = if chunk.len() > 1 {
                        ((chunk[1].clamp(-1.0, 1.0) * 7.0).round() as i8 + 8) as u8
                    } else {
                        8
                    };
                    buf.put_u8((lo & 0x0F) | ((hi & 0x0F) << 4));
                }
                buf.freeze()
            }
            VectorEncoding::Binary => {
                let num_words = dimensions.div_ceil(32);
                let mut buf = BytesMut::with_capacity(num_words * 4);
                for chunk in values.chunks(32) {
                    let mut bits: u32 = 0;
                    for (i, &v) in chunk.iter().enumerate() {
                        if v > 0.0 {
                            bits |= 1 << i;
                        }
                    }
                    buf.put_u32_le(bits);
                }
                buf.freeze()
            }
        };

        Ok(Self {
            id: id.into(),
            dimensions,
            encoding,
            data,
            metadata: HashMap::new(),
            source_topic: None,
            source_offset: None,
            timestamp: chrono::Utc::now().timestamp_millis(),
        })
    }

    /// Get raw bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Calculate L2 norm
    pub fn l2_norm(&self) -> Result<f32> {
        let values = self.to_f32()?;
        Ok(values.iter().map(|x| x * x).sum::<f32>().sqrt())
    }

    /// Normalize to unit length
    pub fn normalize(&self) -> Result<Self> {
        let values = self.to_f32()?;
        let norm = values.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm < 1e-10 {
            return Err(StreamlineError::Validation("Vector has zero norm".into()));
        }
        let normalized: Vec<f32> = values.iter().map(|x| x / norm).collect();
        Self::from_f32_with_encoding(&self.id, &normalized, self.encoding)
    }

    /// Compute cosine similarity with another vector
    pub fn cosine_similarity(&self, other: &Self) -> Result<f32> {
        if self.dimensions != other.dimensions {
            return Err(StreamlineError::Validation(
                "Vector dimensions don't match".into(),
            ));
        }

        let a = self.to_f32()?;
        let b = other.to_f32()?;

        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

        if norm_a < 1e-10 || norm_b < 1e-10 {
            return Ok(0.0);
        }

        Ok(dot / (norm_a * norm_b))
    }

    /// Compute Euclidean distance with another vector
    pub fn euclidean_distance(&self, other: &Self) -> Result<f32> {
        if self.dimensions != other.dimensions {
            return Err(StreamlineError::Validation(
                "Vector dimensions don't match".into(),
            ));
        }

        let a = self.to_f32()?;
        let b = other.to_f32()?;

        let sum_sq: f32 = a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum();
        Ok(sum_sq.sqrt())
    }

    /// Set metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Set source info
    pub fn with_source(mut self, topic: impl Into<String>, offset: i64) -> Self {
        self.source_topic = Some(topic.into());
        self.source_offset = Some(offset);
        self
    }

    /// Storage size in bytes
    pub fn storage_size(&self) -> usize {
        self.data.len()
    }

    /// Compression ratio vs float32
    pub fn compression_ratio(&self) -> f32 {
        let full_size = self.dimensions * 4;
        full_size as f32 / self.data.len() as f32
    }
}

/// Helper to convert float32 to float16 bits
fn float_to_half(f: f32) -> u16 {
    let bits = f.to_bits();
    let sign = (bits >> 31) & 1;
    let exp = ((bits >> 23) & 0xFF) as i32;
    let frac = bits & 0x7FFFFF;

    if exp == 255 {
        // Inf or NaN
        ((sign << 15) | 0x7C00 | (frac >> 13)) as u16
    } else if exp > 142 {
        // Overflow to inf
        ((sign << 15) | 0x7C00) as u16
    } else if exp < 113 {
        // Underflow to zero
        (sign << 15) as u16
    } else {
        let new_exp = (exp - 127 + 15) as u16;
        let new_frac = (frac >> 13) as u16;
        ((sign << 15) as u16) | (new_exp << 10) | new_frac
    }
}

/// Helper to convert float16 bits to float32
fn half_to_float(h: u16) -> f32 {
    let sign = ((h >> 15) & 1) as u32;
    let exp = ((h >> 10) & 0x1F) as i32;
    let frac = (h & 0x3FF) as u32;

    let bits = if exp == 31 {
        // Inf or NaN
        (sign << 31) | 0x7F800000 | (frac << 13)
    } else if exp == 0 {
        if frac == 0 {
            sign << 31
        } else {
            // Denormalized
            let mut f = frac;
            let mut e = 0i32;
            while f & 0x400 == 0 {
                f <<= 1;
                e += 1;
            }
            f &= 0x3FF;
            let new_exp = (127 - 15 - e) as u32;
            (sign << 31) | (new_exp << 23) | (f << 13)
        }
    } else {
        let new_exp = (exp + 127 - 15) as u32;
        (sign << 31) | (new_exp << 23) | (frac << 13)
    };

    f32::from_bits(bits)
}

/// Vector index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexConfig {
    /// Index name
    pub name: String,
    /// Vector dimensions
    pub dimensions: usize,
    /// Storage encoding
    pub encoding: VectorEncoding,
    /// Index type
    pub index_type: VectorIndexType,
    /// HNSW parameters (if applicable)
    pub hnsw: Option<HnswParams>,
    /// IVF parameters (if applicable)
    pub ivf: Option<IvfParams>,
}

impl Default for VectorIndexConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            dimensions: DEFAULT_VECTOR_DIM,
            encoding: VectorEncoding::Float32,
            index_type: VectorIndexType::Hnsw,
            hnsw: Some(HnswParams::default()),
            ivf: None,
        }
    }
}

/// Vector index types
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorIndexType {
    /// Flat (brute force) - exact but slow for large datasets
    Flat,
    /// HNSW - fast approximate nearest neighbor
    #[default]
    Hnsw,
    /// IVF - inverted file index
    Ivf,
    /// Hybrid HNSW+IVF
    HnswIvf,
}

/// HNSW index parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswParams {
    /// Max connections per node
    pub m: usize,
    /// Max connections for layer 0
    pub m0: usize,
    /// Construction search depth
    pub ef_construction: usize,
    /// Query search depth
    pub ef_search: usize,
}

impl Default for HnswParams {
    fn default() -> Self {
        Self {
            m: 16,
            m0: 32,
            ef_construction: 200,
            ef_search: 100,
        }
    }
}

/// IVF index parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IvfParams {
    /// Number of clusters
    pub n_clusters: usize,
    /// Number of probes during search
    pub n_probe: usize,
}

impl Default for IvfParams {
    fn default() -> Self {
        Self {
            n_clusters: 256,
            n_probe: 8,
        }
    }
}

/// Vector store for topic-level vector indexes
pub struct TopicVectorStore {
    /// Configuration
    config: VectorIndexConfig,
    /// Vectors by ID
    vectors: Arc<RwLock<HashMap<String, StreamVector>>>,
    /// Index statistics
    stats: Arc<RwLock<VectorStoreStats>>,
}

/// Vector store statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VectorStoreStats {
    /// Total vectors
    pub vector_count: u64,
    /// Total bytes stored
    pub bytes_stored: u64,
    /// Total searches
    pub search_count: u64,
    /// Average search latency (ms)
    pub avg_search_latency_ms: f64,
    /// Index build time (ms)
    pub index_build_time_ms: u64,
}

impl TopicVectorStore {
    /// Create a new topic vector store
    pub fn new(config: VectorIndexConfig) -> Self {
        Self {
            config,
            vectors: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(VectorStoreStats::default())),
        }
    }

    /// Add a vector to the store
    pub async fn add(&self, vector: StreamVector) -> Result<()> {
        if vector.dimensions != self.config.dimensions {
            return Err(StreamlineError::Validation(format!(
                "Vector dimension {} doesn't match index dimension {}",
                vector.dimensions, self.config.dimensions
            )));
        }

        let size = vector.storage_size() as u64;
        let id = vector.id.clone();

        let mut vectors = self.vectors.write().await;
        vectors.insert(id, vector);

        let mut stats = self.stats.write().await;
        stats.vector_count = vectors.len() as u64;
        stats.bytes_stored += size;

        Ok(())
    }

    /// Get a vector by ID
    pub async fn get(&self, id: &str) -> Option<StreamVector> {
        let vectors = self.vectors.read().await;
        vectors.get(id).cloned()
    }

    /// Remove a vector
    pub async fn remove(&self, id: &str) -> Option<StreamVector> {
        let mut vectors = self.vectors.write().await;
        let removed = vectors.remove(id);

        if let Some(ref v) = removed {
            let mut stats = self.stats.write().await;
            stats.vector_count = vectors.len() as u64;
            stats.bytes_stored = stats.bytes_stored.saturating_sub(v.storage_size() as u64);
        }

        removed
    }

    /// Search for nearest neighbors
    pub async fn search(&self, query: &StreamVector, k: usize) -> Result<Vec<VectorSearchResult>> {
        let start = std::time::Instant::now();

        if query.dimensions != self.config.dimensions {
            return Err(StreamlineError::Validation(format!(
                "Query dimension {} doesn't match index dimension {}",
                query.dimensions, self.config.dimensions
            )));
        }

        let vectors = self.vectors.read().await;

        // For now, use brute force search (HNSW would be used in production)
        let mut results: Vec<VectorSearchResult> = vectors
            .values()
            .filter_map(|v| {
                query
                    .cosine_similarity(v)
                    .ok()
                    .map(|similarity| VectorSearchResult {
                        id: v.id.clone(),
                        score: similarity,
                        metadata: v.metadata.clone(),
                        source_topic: v.source_topic.clone(),
                        source_offset: v.source_offset,
                    })
            })
            .collect();

        // Sort by similarity (descending)
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(k);

        // Update stats
        let elapsed = start.elapsed().as_millis() as f64;
        let mut stats = self.stats.write().await;
        stats.search_count += 1;
        stats.avg_search_latency_ms =
            (stats.avg_search_latency_ms * (stats.search_count - 1) as f64 + elapsed)
                / stats.search_count as f64;

        Ok(results)
    }

    /// Get store statistics
    pub async fn stats(&self) -> VectorStoreStats {
        self.stats.read().await.clone()
    }

    /// Get configuration
    pub fn config(&self) -> &VectorIndexConfig {
        &self.config
    }
}

/// Vector search result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchResult {
    /// Vector ID
    pub id: String,
    /// Similarity score
    pub score: f32,
    /// Vector metadata
    pub metadata: HashMap<String, String>,
    /// Source topic (if tracked)
    pub source_topic: Option<String>,
    /// Source offset (if tracked)
    pub source_offset: Option<i64>,
}

/// RAG (Retrieval-Augmented Generation) pipeline
pub struct RagPipeline {
    /// Vector store
    vector_store: Arc<TopicVectorStore>,
    /// Configuration
    config: RagConfig,
}

/// RAG configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagConfig {
    /// Number of context chunks to retrieve
    pub context_chunks: usize,
    /// Minimum similarity threshold
    pub min_similarity: f32,
    /// Maximum context length (tokens)
    pub max_context_tokens: usize,
    /// Include metadata in context
    pub include_metadata: bool,
}

impl Default for RagConfig {
    fn default() -> Self {
        Self {
            context_chunks: 5,
            min_similarity: 0.7,
            max_context_tokens: 4096,
            include_metadata: true,
        }
    }
}

impl RagPipeline {
    /// Create a new RAG pipeline
    pub fn new(vector_store: Arc<TopicVectorStore>, config: RagConfig) -> Self {
        Self {
            vector_store,
            config,
        }
    }

    /// Retrieve context for a query
    pub async fn retrieve_context(&self, query_vector: &StreamVector) -> Result<RagContext> {
        let results = self
            .vector_store
            .search(query_vector, self.config.context_chunks)
            .await?;

        let chunks: Vec<RagChunk> = results
            .into_iter()
            .filter(|r| r.score >= self.config.min_similarity)
            .map(|r| RagChunk {
                id: r.id,
                score: r.score,
                metadata: r.metadata,
                source_topic: r.source_topic,
                source_offset: r.source_offset,
            })
            .collect();

        let total_retrieved = chunks.len();
        Ok(RagContext {
            chunks,
            total_retrieved,
        })
    }

    /// Get configuration
    pub fn config(&self) -> &RagConfig {
        &self.config
    }
}

/// RAG context retrieved for a query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagContext {
    /// Retrieved chunks
    pub chunks: Vec<RagChunk>,
    /// Total chunks retrieved
    pub total_retrieved: usize,
}

/// A chunk of context for RAG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagChunk {
    /// Chunk ID
    pub id: String,
    /// Relevance score
    pub score: f32,
    /// Chunk metadata
    pub metadata: HashMap<String, String>,
    /// Source topic
    pub source_topic: Option<String>,
    /// Source offset
    pub source_offset: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_creation() {
        let values = vec![0.1, 0.2, 0.3, 0.4];
        let vector = StreamVector::from_f32("test", &values).unwrap();

        assert_eq!(vector.dimensions, 4);
        assert_eq!(vector.encoding, VectorEncoding::Float32);

        let recovered = vector.to_f32().unwrap();
        for (a, b) in values.iter().zip(recovered.iter()) {
            assert!((a - b).abs() < 1e-6);
        }
    }

    #[test]
    fn test_vector_quantization() {
        let values: Vec<f32> = (0..128).map(|i| (i as f32 / 127.0) * 2.0 - 1.0).collect();
        let vector = StreamVector::from_f32("test", &values).unwrap();

        // Quantize to int8
        let quantized = vector.quantize(VectorEncoding::Int8).unwrap();
        assert_eq!(quantized.storage_size(), 128); // 1 byte per dim

        // Compression ratio
        assert_eq!(quantized.compression_ratio(), 4.0);

        // Verify approximate recovery
        let recovered = quantized.to_f32().unwrap();
        for (a, b) in values.iter().zip(recovered.iter()) {
            assert!((a - b).abs() < 0.02); // Allow small quantization error
        }
    }

    #[test]
    fn test_cosine_similarity() {
        let a = StreamVector::from_f32("a", &[1.0, 0.0, 0.0]).unwrap();
        let b = StreamVector::from_f32("b", &[1.0, 0.0, 0.0]).unwrap();
        let c = StreamVector::from_f32("c", &[0.0, 1.0, 0.0]).unwrap();

        // Same vector = similarity 1
        let sim_ab = a.cosine_similarity(&b).unwrap();
        assert!((sim_ab - 1.0).abs() < 1e-6);

        // Orthogonal vectors = similarity 0
        let sim_ac = a.cosine_similarity(&c).unwrap();
        assert!(sim_ac.abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_vector_store() {
        let config = VectorIndexConfig {
            dimensions: 4,
            ..Default::default()
        };
        let store = TopicVectorStore::new(config);

        // Add vectors
        let v1 = StreamVector::from_f32("v1", &[1.0, 0.0, 0.0, 0.0]).unwrap();
        let v2 = StreamVector::from_f32("v2", &[0.9, 0.1, 0.0, 0.0]).unwrap();
        let v3 = StreamVector::from_f32("v3", &[0.0, 1.0, 0.0, 0.0]).unwrap();

        store.add(v1).await.unwrap();
        store.add(v2).await.unwrap();
        store.add(v3).await.unwrap();

        // Search
        let query = StreamVector::from_f32("q", &[1.0, 0.0, 0.0, 0.0]).unwrap();
        let results = store.search(&query, 2).await.unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "v1"); // Exact match
        assert_eq!(results[1].id, "v2"); // Close match
    }
}
