//! Compression support for Streamline storage
//!
//! This module provides compression and decompression utilities for
//! record batches stored in segments.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

/// Default compression level for Zstd (range: 1-22, default: 3)
pub const DEFAULT_ZSTD_LEVEL: i32 = 3;

/// Default compression level for Gzip (range: 0-9, default: 6)
pub const DEFAULT_GZIP_LEVEL: u32 = 6;

/// Configuration for compression levels per codec
///
/// This allows tuning the trade-off between compression speed and ratio:
/// - Higher levels: better compression ratio, slower
/// - Lower levels: faster compression, larger output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    /// Zstd compression level (1-22, default: 3)
    /// Level 1: fastest, Level 22: best compression
    pub zstd_level: i32,

    /// Gzip compression level (0-9, default: 6)
    /// Level 0: no compression, Level 9: best compression
    pub gzip_level: u32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            zstd_level: DEFAULT_ZSTD_LEVEL,
            gzip_level: DEFAULT_GZIP_LEVEL,
        }
    }
}

impl CompressionConfig {
    /// Create a new compression config with specified levels
    pub fn new(zstd_level: i32, gzip_level: u32) -> Self {
        Self {
            // Clamp zstd level to valid range (1-22)
            zstd_level: zstd_level.clamp(1, 22),
            // Clamp gzip level to valid range (0-9)
            gzip_level: gzip_level.clamp(0, 9),
        }
    }
}

/// Compression codec types supported by Streamline
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum CompressionCodec {
    /// No compression
    #[default]
    None = 0,
    /// LZ4 compression - fast compression/decompression
    Lz4 = 1,
    /// Zstandard compression - good balance of speed and ratio
    Zstd = 2,
    /// Snappy compression - very fast, moderate ratio
    Snappy = 3,
    /// Gzip compression - widely compatible, good ratio
    Gzip = 4,
}

impl CompressionCodec {
    /// Create a codec from a byte value
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::None),
            1 => Some(Self::Lz4),
            2 => Some(Self::Zstd),
            3 => Some(Self::Snappy),
            4 => Some(Self::Gzip),
            _ => None,
        }
    }

    /// Get the byte value for this codec
    pub fn as_byte(&self) -> u8 {
        *self as u8
    }

    /// Get codec name for logging
    pub fn name(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
            Self::Snappy => "snappy",
            Self::Gzip => "gzip",
        }
    }

    /// Parse codec from string name
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "none" => Some(Self::None),
            "lz4" => Some(Self::Lz4),
            "zstd" | "zstandard" => Some(Self::Zstd),
            "snappy" => Some(Self::Snappy),
            "gzip" | "gz" => Some(Self::Gzip),
            _ => None,
        }
    }
}

/// Compress data using the specified codec with default compression levels
pub fn compress(data: &[u8], codec: CompressionCodec) -> Result<Vec<u8>> {
    compress_with_config(data, codec, &CompressionConfig::default())
}

/// Compress data using the specified codec and compression config
///
/// This allows configuring compression levels for codecs that support them:
/// - Zstd: uses `config.zstd_level` (1-22)
/// - Gzip: uses `config.gzip_level` (0-9)
/// - LZ4 and Snappy: compression level is not configurable
pub fn compress_with_config(
    data: &[u8],
    codec: CompressionCodec,
    config: &CompressionConfig,
) -> Result<Vec<u8>> {
    match codec {
        CompressionCodec::None => Ok(data.to_vec()),
        CompressionCodec::Lz4 => compress_lz4(data),
        CompressionCodec::Zstd => compress_zstd(data, config.zstd_level),
        CompressionCodec::Snappy => compress_snappy(data),
        CompressionCodec::Gzip => compress_gzip(data, config.gzip_level),
    }
}

/// Decompress data using the specified codec
pub fn decompress(data: &[u8], codec: CompressionCodec) -> Result<Vec<u8>> {
    match codec {
        CompressionCodec::None => Ok(data.to_vec()),
        CompressionCodec::Lz4 => decompress_lz4(data),
        CompressionCodec::Zstd => decompress_zstd(data),
        CompressionCodec::Snappy => decompress_snappy(data),
        CompressionCodec::Gzip => decompress_gzip(data),
    }
}

/// Compress data using LZ4
fn compress_lz4(data: &[u8]) -> Result<Vec<u8>> {
    Ok(lz4_flex::compress_prepend_size(data))
}

/// Decompress LZ4 data
fn decompress_lz4(data: &[u8]) -> Result<Vec<u8>> {
    lz4_flex::decompress_size_prepended(data)
        .map_err(|e| StreamlineError::storage_msg(format!("LZ4 decompression failed: {}", e)))
}

/// Compress data using Zstd with configurable level
fn compress_zstd(data: &[u8], level: i32) -> Result<Vec<u8>> {
    let mut encoder = zstd::Encoder::new(Vec::new(), level).map_err(|e| {
        StreamlineError::storage_msg(format!("Zstd encoder creation failed: {}", e))
    })?;

    encoder
        .write_all(data)
        .map_err(|e| StreamlineError::storage_msg(format!("Zstd compression failed: {}", e)))?;

    encoder
        .finish()
        .map_err(|e| StreamlineError::storage_msg(format!("Zstd finish failed: {}", e)))
}

/// Decompress Zstd data
fn decompress_zstd(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = zstd::Decoder::new(data).map_err(|e| {
        StreamlineError::storage_msg(format!("Zstd decoder creation failed: {}", e))
    })?;

    let mut output = Vec::new();
    decoder
        .read_to_end(&mut output)
        .map_err(|e| StreamlineError::storage_msg(format!("Zstd decompression failed: {}", e)))?;

    Ok(output)
}

/// Compress data using Snappy
fn compress_snappy(data: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = snap::raw::Encoder::new();
    encoder
        .compress_vec(data)
        .map_err(|e| StreamlineError::storage_msg(format!("Snappy compression failed: {}", e)))
}

/// Decompress Snappy data
fn decompress_snappy(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = snap::raw::Decoder::new();
    decoder
        .decompress_vec(data)
        .map_err(|e| StreamlineError::storage_msg(format!("Snappy decompression failed: {}", e)))
}

/// Compress data using Gzip with configurable level
fn compress_gzip(data: &[u8], level: u32) -> Result<Vec<u8>> {
    use flate2::write::GzEncoder;
    use flate2::Compression;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(level));
    encoder
        .write_all(data)
        .map_err(|e| StreamlineError::storage_msg(format!("Gzip compression failed: {}", e)))?;

    encoder
        .finish()
        .map_err(|e| StreamlineError::storage_msg(format!("Gzip finish failed: {}", e)))
}

/// Decompress Gzip data
fn decompress_gzip(data: &[u8]) -> Result<Vec<u8>> {
    use flate2::read::GzDecoder;

    let mut decoder = GzDecoder::new(data);
    let mut output = Vec::new();
    decoder
        .read_to_end(&mut output)
        .map_err(|e| StreamlineError::storage_msg(format!("Gzip decompression failed: {}", e)))?;

    Ok(output)
}

/// Estimate the compressed size for a given codec
/// Useful for deciding whether compression is worthwhile
pub fn estimate_compression_ratio(codec: CompressionCodec) -> f64 {
    match codec {
        CompressionCodec::None => 1.0,
        CompressionCodec::Lz4 => 0.5, // ~50% compression typical for JSON
        CompressionCodec::Zstd => 0.35, // ~35% compression typical
        CompressionCodec::Snappy => 0.55, // ~55% compression typical
        CompressionCodec::Gzip => 0.4, // ~40% compression typical
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codec_from_byte() {
        assert_eq!(CompressionCodec::from_byte(0), Some(CompressionCodec::None));
        assert_eq!(CompressionCodec::from_byte(1), Some(CompressionCodec::Lz4));
        assert_eq!(CompressionCodec::from_byte(2), Some(CompressionCodec::Zstd));
        assert_eq!(
            CompressionCodec::from_byte(3),
            Some(CompressionCodec::Snappy)
        );
        assert_eq!(CompressionCodec::from_byte(4), Some(CompressionCodec::Gzip));
        assert_eq!(CompressionCodec::from_byte(99), None);
    }

    #[test]
    fn test_codec_from_name() {
        assert_eq!(
            CompressionCodec::from_name("none"),
            Some(CompressionCodec::None)
        );
        assert_eq!(
            CompressionCodec::from_name("lz4"),
            Some(CompressionCodec::Lz4)
        );
        assert_eq!(
            CompressionCodec::from_name("zstd"),
            Some(CompressionCodec::Zstd)
        );
        assert_eq!(
            CompressionCodec::from_name("ZSTD"),
            Some(CompressionCodec::Zstd)
        );
        assert_eq!(
            CompressionCodec::from_name("snappy"),
            Some(CompressionCodec::Snappy)
        );
        assert_eq!(
            CompressionCodec::from_name("gzip"),
            Some(CompressionCodec::Gzip)
        );
        assert_eq!(
            CompressionCodec::from_name("gz"),
            Some(CompressionCodec::Gzip)
        );
        assert_eq!(CompressionCodec::from_name("unknown"), None);
    }

    #[test]
    fn test_compress_decompress_none() {
        let data = b"Hello, World!";
        let compressed = compress(data, CompressionCodec::None).unwrap();
        let decompressed = decompress(&compressed, CompressionCodec::None).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_compress_decompress_lz4() {
        let data = b"Hello, World! This is some test data for compression.";
        let compressed = compress(data, CompressionCodec::Lz4).unwrap();
        let decompressed = decompress(&compressed, CompressionCodec::Lz4).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_compress_decompress_zstd() {
        let data = b"Hello, World! This is some test data for compression.";
        let compressed = compress(data, CompressionCodec::Zstd).unwrap();
        let decompressed = decompress(&compressed, CompressionCodec::Zstd).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_compress_decompress_snappy() {
        let data = b"Hello, World! This is some test data for compression.";
        let compressed = compress(data, CompressionCodec::Snappy).unwrap();
        let decompressed = decompress(&compressed, CompressionCodec::Snappy).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_compress_decompress_gzip() {
        let data = b"Hello, World! This is some test data for compression.";
        let compressed = compress(data, CompressionCodec::Gzip).unwrap();
        let decompressed = decompress(&compressed, CompressionCodec::Gzip).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_compression_reduces_size() {
        // Create highly compressible data
        let data: Vec<u8> = (0..1000).map(|_| b'A').collect();

        let lz4_compressed = compress(&data, CompressionCodec::Lz4).unwrap();
        let zstd_compressed = compress(&data, CompressionCodec::Zstd).unwrap();
        let snappy_compressed = compress(&data, CompressionCodec::Snappy).unwrap();
        let gzip_compressed = compress(&data, CompressionCodec::Gzip).unwrap();

        assert!(lz4_compressed.len() < data.len());
        assert!(zstd_compressed.len() < data.len());
        assert!(snappy_compressed.len() < data.len());
        assert!(gzip_compressed.len() < data.len());
    }

    #[test]
    fn test_empty_data() {
        let data: &[u8] = &[];

        for codec in [
            CompressionCodec::None,
            CompressionCodec::Lz4,
            CompressionCodec::Zstd,
            CompressionCodec::Snappy,
            CompressionCodec::Gzip,
        ] {
            let compressed = compress(data, codec).unwrap();
            let decompressed = decompress(&compressed, codec).unwrap();
            assert_eq!(data, decompressed.as_slice());
        }
    }

    #[test]
    fn test_large_data() {
        // Test with 1MB of data
        let data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

        for codec in [
            CompressionCodec::Lz4,
            CompressionCodec::Zstd,
            CompressionCodec::Snappy,
            CompressionCodec::Gzip,
        ] {
            let compressed = compress(&data, codec).unwrap();
            let decompressed = decompress(&compressed, codec).unwrap();
            assert_eq!(data, decompressed);
        }
    }

    #[test]
    fn test_compression_config_default() {
        let config = CompressionConfig::default();
        assert_eq!(config.zstd_level, DEFAULT_ZSTD_LEVEL);
        assert_eq!(config.gzip_level, DEFAULT_GZIP_LEVEL);
    }

    #[test]
    fn test_compression_config_new_clamping() {
        // Test that values are clamped to valid ranges
        let config = CompressionConfig::new(0, 15);
        assert_eq!(config.zstd_level, 1); // Clamped from 0 to 1
        assert_eq!(config.gzip_level, 9); // Clamped from 15 to 9

        let config = CompressionConfig::new(30, 5);
        assert_eq!(config.zstd_level, 22); // Clamped from 30 to 22
        assert_eq!(config.gzip_level, 5); // Within range

        let config = CompressionConfig::new(10, 0);
        assert_eq!(config.zstd_level, 10); // Within range
        assert_eq!(config.gzip_level, 0); // Within range (no compression)
    }

    #[test]
    fn test_compress_with_config_zstd() {
        let data = b"Hello, World! This is some test data for compression.";

        // Test with different compression levels
        let config_low = CompressionConfig::new(1, 6);
        let config_high = CompressionConfig::new(19, 6);

        let compressed_low =
            compress_with_config(data, CompressionCodec::Zstd, &config_low).unwrap();
        let compressed_high =
            compress_with_config(data, CompressionCodec::Zstd, &config_high).unwrap();

        // Both should decompress correctly
        let decompressed_low = decompress(&compressed_low, CompressionCodec::Zstd).unwrap();
        let decompressed_high = decompress(&compressed_high, CompressionCodec::Zstd).unwrap();

        assert_eq!(data.as_slice(), decompressed_low.as_slice());
        assert_eq!(data.as_slice(), decompressed_high.as_slice());
    }

    #[test]
    fn test_compress_with_config_gzip() {
        let data = b"Hello, World! This is some test data for compression.";

        // Test with different compression levels
        let config_low = CompressionConfig::new(3, 1);
        let config_high = CompressionConfig::new(3, 9);

        let compressed_low =
            compress_with_config(data, CompressionCodec::Gzip, &config_low).unwrap();
        let compressed_high =
            compress_with_config(data, CompressionCodec::Gzip, &config_high).unwrap();

        // Both should decompress correctly
        let decompressed_low = decompress(&compressed_low, CompressionCodec::Gzip).unwrap();
        let decompressed_high = decompress(&compressed_high, CompressionCodec::Gzip).unwrap();

        assert_eq!(data.as_slice(), decompressed_low.as_slice());
        assert_eq!(data.as_slice(), decompressed_high.as_slice());
    }
}
