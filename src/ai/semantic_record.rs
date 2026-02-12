//! Semantic Record Wire Format
//!
//! Extends the standard record format with embedding metadata for semantic streaming.
//! Supports quantization for efficient storage and transmission.

use crate::ai::providers::QuantizationType;
use crate::error::{Result, StreamlineError};
use crate::storage::record::{Header, Record};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

/// Header keys for semantic record metadata
pub const SEMANTIC_TYPE_HEADER: &str = "x-semantic-type";
pub const SEMANTIC_MODEL_HEADER: &str = "x-semantic-model";
pub const SEMANTIC_DIM_HEADER: &str = "x-semantic-dim";
pub const SEMANTIC_QUANT_HEADER: &str = "x-semantic-quant";
pub const SEMANTIC_NORM_HEADER: &str = "x-semantic-norm";

/// Semantic record type indicator
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[repr(u8)]
pub enum SemanticType {
    /// No embedding
    #[default]
    None = 0,
    /// Text content with embedding
    Text = 1,
    /// Image content with embedding
    Image = 2,
    /// Audio content with embedding
    Audio = 3,
    /// Multimodal content
    Multimodal = 4,
}

impl From<u8> for SemanticType {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::Text,
            2 => Self::Image,
            3 => Self::Audio,
            4 => Self::Multimodal,
            _ => Self::None,
        }
    }
}

/// Embedding metadata attached to records
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingMetadata {
    /// Model identifier used for embedding
    pub model_id: String,
    /// Embedding dimensions
    pub dimensions: u16,
    /// Quantization type
    pub quantization: QuantizationType,
    /// L2 norm of original vector (for de-normalization)
    pub norm: Option<f32>,
    /// Semantic content type
    pub semantic_type: SemanticType,
}

impl Default for EmbeddingMetadata {
    fn default() -> Self {
        Self {
            model_id: String::new(),
            dimensions: 0,
            quantization: QuantizationType::Float32,
            norm: None,
            semantic_type: SemanticType::None,
        }
    }
}

impl EmbeddingMetadata {
    /// Create new metadata for a text embedding
    pub fn text(model_id: impl Into<String>, dimensions: u16) -> Self {
        Self {
            model_id: model_id.into(),
            dimensions,
            quantization: QuantizationType::Float32,
            norm: None,
            semantic_type: SemanticType::Text,
        }
    }

    /// Set quantization type
    pub fn with_quantization(mut self, quantization: QuantizationType) -> Self {
        self.quantization = quantization;
        self
    }

    /// Set norm value
    pub fn with_norm(mut self, norm: f32) -> Self {
        self.norm = Some(norm);
        self
    }

    /// Convert to record headers
    pub fn to_headers(&self) -> Vec<Header> {
        let mut headers = vec![
            Header {
                key: SEMANTIC_TYPE_HEADER.to_string(),
                value: Bytes::copy_from_slice(&[self.semantic_type as u8]),
            },
            Header {
                key: SEMANTIC_MODEL_HEADER.to_string(),
                value: Bytes::copy_from_slice(self.model_id.as_bytes()),
            },
            Header {
                key: SEMANTIC_DIM_HEADER.to_string(),
                value: Bytes::copy_from_slice(&self.dimensions.to_le_bytes()),
            },
            Header {
                key: SEMANTIC_QUANT_HEADER.to_string(),
                value: Bytes::copy_from_slice(&[self.quantization as u8]),
            },
        ];

        if let Some(norm) = self.norm {
            headers.push(Header {
                key: SEMANTIC_NORM_HEADER.to_string(),
                value: Bytes::copy_from_slice(&norm.to_le_bytes()),
            });
        }

        headers
    }

    /// Parse from record headers
    pub fn from_headers(headers: &[Header]) -> Option<Self> {
        let mut metadata = Self::default();
        let mut found = false;

        for header in headers {
            match header.key.as_str() {
                SEMANTIC_TYPE_HEADER if !header.value.is_empty() => {
                    metadata.semantic_type = SemanticType::from(header.value[0]);
                    found = true;
                }
                SEMANTIC_MODEL_HEADER => {
                    metadata.model_id = std::str::from_utf8(&header.value).ok()?.to_string();
                }
                SEMANTIC_DIM_HEADER if header.value.len() >= 2 => {
                    metadata.dimensions = u16::from_le_bytes([header.value[0], header.value[1]]);
                }
                SEMANTIC_QUANT_HEADER if !header.value.is_empty() => {
                    metadata.quantization = match header.value[0] {
                        0 => QuantizationType::Float32,
                        1 => QuantizationType::Int8,
                        2 => QuantizationType::Binary,
                        _ => QuantizationType::Float32,
                    };
                }
                SEMANTIC_NORM_HEADER if header.value.len() >= 4 => {
                    let bytes = [
                        header.value[0],
                        header.value[1],
                        header.value[2],
                        header.value[3],
                    ];
                    metadata.norm = Some(f32::from_le_bytes(bytes));
                }
                _ => {}
            }
        }

        if found {
            Some(metadata)
        } else {
            None
        }
    }
}

/// A record with semantic embedding data
#[derive(Debug, Clone)]
pub struct SemanticRecord {
    /// Base record
    pub record: Record,
    /// Embedding metadata
    pub metadata: EmbeddingMetadata,
    /// Embedding vector (quantized or full precision)
    pub vector_data: Bytes,
}

impl SemanticRecord {
    /// Create a new semantic record from a base record and embedding
    pub fn new(mut record: Record, metadata: EmbeddingMetadata, vector: Vec<f32>) -> Self {
        // Add semantic headers to record
        let mut headers: Vec<Header> = record.headers.to_vec();
        headers.extend(metadata.to_headers());
        record.headers = headers;

        // Serialize vector based on quantization
        let vector_data = match metadata.quantization {
            QuantizationType::Float32 => serialize_float32(&vector),
            QuantizationType::Int8 => serialize_int8(&vector),
            QuantizationType::Binary => serialize_binary(&vector),
        };

        Self {
            record,
            metadata,
            vector_data,
        }
    }

    /// Create from an existing record (extracts embedding from value suffix)
    pub fn from_record(record: Record) -> Option<Self> {
        let metadata = EmbeddingMetadata::from_headers(&record.headers)?;

        // Vector data is stored at the end of the value
        let vector_size = match metadata.quantization {
            QuantizationType::Float32 => metadata.dimensions as usize * 4,
            QuantizationType::Int8 => metadata.dimensions as usize,
            QuantizationType::Binary => (metadata.dimensions as usize).div_ceil(8),
        };

        if record.value.len() < vector_size {
            return None;
        }

        let vector_start = record.value.len() - vector_size;
        let vector_data = record.value.slice(vector_start..);

        Some(Self {
            record,
            metadata,
            vector_data,
        })
    }

    /// Get the embedding vector as f32
    pub fn get_vector(&self) -> Result<Vec<f32>> {
        match self.metadata.quantization {
            QuantizationType::Float32 => deserialize_float32(&self.vector_data),
            QuantizationType::Int8 => deserialize_int8(&self.vector_data, self.metadata.norm),
            QuantizationType::Binary => {
                deserialize_binary(&self.vector_data, self.metadata.dimensions)
            }
        }
    }

    /// Get the original content (value without vector data)
    pub fn get_content(&self) -> Bytes {
        let vector_size = match self.metadata.quantization {
            QuantizationType::Float32 => self.metadata.dimensions as usize * 4,
            QuantizationType::Int8 => self.metadata.dimensions as usize,
            QuantizationType::Binary => (self.metadata.dimensions as usize).div_ceil(8),
        };

        if self.record.value.len() > vector_size {
            self.record
                .value
                .slice(..self.record.value.len() - vector_size)
        } else {
            Bytes::new()
        }
    }

    /// Convert to a record with embedded vector in value
    pub fn to_record(&self) -> Record {
        let mut value = BytesMut::new();
        value.put(self.get_content());
        value.put(self.vector_data.clone());

        Record {
            offset: self.record.offset,
            timestamp: self.record.timestamp,
            key: self.record.key.clone(),
            value: value.freeze(),
            headers: self.record.headers.clone(),
            crc: None,
        }
    }

    /// Compute cosine similarity with another semantic record
    pub fn similarity(&self, other: &SemanticRecord) -> Result<f32> {
        let v1 = self.get_vector()?;
        let v2 = other.get_vector()?;

        if v1.len() != v2.len() {
            return Err(StreamlineError::InvalidData(
                "Vector dimension mismatch".into(),
            ));
        }

        Ok(cosine_similarity(&v1, &v2))
    }
}

/// Serialize vector as float32
fn serialize_float32(vector: &[f32]) -> Bytes {
    let mut buf = BytesMut::with_capacity(vector.len() * 4);
    for &v in vector {
        buf.put_f32_le(v);
    }
    buf.freeze()
}

/// Deserialize float32 vector
fn deserialize_float32(data: &[u8]) -> Result<Vec<f32>> {
    if data.len() % 4 != 0 {
        return Err(StreamlineError::InvalidData(
            "Invalid float32 vector data".into(),
        ));
    }

    let mut cursor = std::io::Cursor::new(data);
    let mut vector = Vec::with_capacity(data.len() / 4);
    while cursor.has_remaining() {
        vector.push(cursor.get_f32_le());
    }
    Ok(vector)
}

/// Serialize vector as int8 (quantized)
fn serialize_int8(vector: &[f32]) -> Bytes {
    let mut buf = BytesMut::with_capacity(vector.len());

    // Find min/max for scaling
    let (min, max) = vector.iter().fold((f32::MAX, f32::MIN), |(min, max), &v| {
        (min.min(v), max.max(v))
    });

    let scale = if max > min { 255.0 / (max - min) } else { 1.0 };

    for &v in vector {
        let quantized = ((v - min) * scale).round() as u8;
        buf.put_u8(quantized);
    }

    buf.freeze()
}

/// Deserialize int8 quantized vector
fn deserialize_int8(data: &[u8], norm: Option<f32>) -> Result<Vec<f32>> {
    let vector: Vec<f32> = data.iter().map(|&v| v as f32 / 255.0).collect();

    // Apply norm scaling if available
    if let Some(n) = norm {
        Ok(vector.iter().map(|&v| v * n).collect())
    } else {
        Ok(vector)
    }
}

/// Serialize vector as binary (1-bit quantization)
fn serialize_binary(vector: &[f32]) -> Bytes {
    let byte_count = vector.len().div_ceil(8);
    let mut buf = BytesMut::with_capacity(byte_count);

    for chunk in vector.chunks(8) {
        let mut byte = 0u8;
        for (i, &v) in chunk.iter().enumerate() {
            if v > 0.0 {
                byte |= 1 << i;
            }
        }
        buf.put_u8(byte);
    }

    buf.freeze()
}

/// Deserialize binary quantized vector
fn deserialize_binary(data: &[u8], dimensions: u16) -> Result<Vec<f32>> {
    let mut vector = Vec::with_capacity(dimensions as usize);

    for (byte_idx, &byte) in data.iter().enumerate() {
        for bit in 0..8 {
            let idx = byte_idx * 8 + bit;
            if idx >= dimensions as usize {
                break;
            }
            let value = if byte & (1 << bit) != 0 { 1.0 } else { -1.0 };
            vector.push(value);
        }
    }

    Ok(vector)
}

/// Compute cosine similarity between two vectors
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}

/// Builder for creating semantic records
#[derive(Debug, Default)]
pub struct SemanticRecordBuilder {
    offset: i64,
    timestamp: i64,
    key: Option<Bytes>,
    value: Bytes,
    headers: Vec<Header>,
    model_id: String,
    dimensions: u16,
    quantization: QuantizationType,
    vector: Option<Vec<f32>>,
}

impl SemanticRecordBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set offset
    pub fn offset(mut self, offset: i64) -> Self {
        self.offset = offset;
        self
    }

    /// Set timestamp
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Set key
    pub fn key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set value
    pub fn value(mut self, value: impl Into<Bytes>) -> Self {
        self.value = value.into();
        self
    }

    /// Add header
    pub fn header(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push(Header {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    /// Set embedding model
    pub fn model(mut self, model_id: impl Into<String>, dimensions: u16) -> Self {
        self.model_id = model_id.into();
        self.dimensions = dimensions;
        self
    }

    /// Set quantization
    pub fn quantization(mut self, quantization: QuantizationType) -> Self {
        self.quantization = quantization;
        self
    }

    /// Set embedding vector
    pub fn vector(mut self, vector: Vec<f32>) -> Self {
        self.vector = Some(vector);
        self
    }

    /// Build the semantic record
    pub fn build(self) -> Result<SemanticRecord> {
        let vector = self
            .vector
            .ok_or_else(|| StreamlineError::InvalidData("Embedding vector is required".into()))?;

        if vector.len() != self.dimensions as usize {
            return Err(StreamlineError::InvalidData(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.dimensions,
                vector.len()
            )));
        }

        let record = Record {
            offset: self.offset,
            timestamp: self.timestamp,
            key: self.key,
            value: self.value,
            headers: self.headers,
            crc: None,
        };

        let metadata = EmbeddingMetadata::text(&self.model_id, self.dimensions)
            .with_quantization(self.quantization);

        Ok(SemanticRecord::new(record, metadata, vector))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embedding_metadata_to_headers() {
        let metadata = EmbeddingMetadata::text("test-model", 384)
            .with_quantization(QuantizationType::Float32)
            .with_norm(1.5);

        let headers = metadata.to_headers();
        assert_eq!(headers.len(), 5); // type, model, dim, quant, norm
    }

    #[test]
    fn test_embedding_metadata_roundtrip() {
        let original = EmbeddingMetadata::text("my-model", 1024)
            .with_quantization(QuantizationType::Int8)
            .with_norm(2.0);

        let headers = original.to_headers();
        let parsed = EmbeddingMetadata::from_headers(&headers).unwrap();

        assert_eq!(parsed.model_id, "my-model");
        assert_eq!(parsed.dimensions, 1024);
        assert_eq!(parsed.quantization, QuantizationType::Int8);
        assert_eq!(parsed.norm, Some(2.0));
        assert_eq!(parsed.semantic_type, SemanticType::Text);
    }

    #[test]
    fn test_serialize_deserialize_float32() {
        let vector = vec![1.0, 2.0, 3.0, -1.0, 0.5];
        let serialized = serialize_float32(&vector);
        let deserialized = deserialize_float32(&serialized).unwrap();

        assert_eq!(vector.len(), deserialized.len());
        for (a, b) in vector.iter().zip(deserialized.iter()) {
            assert!((a - b).abs() < 0.0001);
        }
    }

    #[test]
    fn test_serialize_deserialize_int8() {
        let vector = vec![0.0, 0.5, 1.0, 0.25, 0.75];
        let serialized = serialize_int8(&vector);
        let deserialized = deserialize_int8(&serialized, None).unwrap();

        assert_eq!(vector.len(), deserialized.len());
    }

    #[test]
    fn test_serialize_deserialize_binary() {
        let vector = vec![1.0, -1.0, 1.0, 1.0, -1.0, -1.0, 1.0, -1.0, 1.0];
        let serialized = serialize_binary(&vector);
        let deserialized = deserialize_binary(&serialized, 9).unwrap();

        assert_eq!(vector.len(), deserialized.len());
        for (a, b) in vector.iter().zip(deserialized.iter()) {
            assert_eq!(a.signum(), b.signum());
        }
    }

    #[test]
    fn test_semantic_record_builder() {
        let vector = vec![0.1, 0.2, 0.3, 0.4];
        let record = SemanticRecordBuilder::new()
            .offset(100)
            .timestamp(1234567890)
            .key(Bytes::from("test-key"))
            .value(Bytes::from("hello world"))
            .model("test-model", 4)
            .vector(vector.clone())
            .build()
            .unwrap();

        assert_eq!(record.record.offset, 100);
        assert_eq!(record.metadata.dimensions, 4);
        assert_eq!(record.get_vector().unwrap(), vector);
    }

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let c = vec![0.0, 1.0, 0.0];

        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 0.001);
        assert!((cosine_similarity(&a, &c) - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_semantic_type_from_u8() {
        assert_eq!(SemanticType::from(0), SemanticType::None);
        assert_eq!(SemanticType::from(1), SemanticType::Text);
        assert_eq!(SemanticType::from(2), SemanticType::Image);
        assert_eq!(SemanticType::from(255), SemanticType::None);
    }
}
