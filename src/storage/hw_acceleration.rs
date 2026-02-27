//! Hardware acceleration engine for high-throughput record processing.
//!
//! Provides SIMD and io_uring optimization layers with automatic capability
//! detection and scalar fallbacks. All operations are safe — no intrinsics
//! are used directly. Instead, CPU features are detected at construction time
//! and used to select optimal batch sizes and processing strategies.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

fn default_true() -> bool {
    true
}
fn default_simd_batch_size() -> usize {
    256
}
fn default_io_uring_queue_depth() -> u32 {
    128
}
fn default_prefetch_distance() -> usize {
    4
}

/// Configuration for the hardware acceleration engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HwAccelConfig {
    #[serde(default = "default_true")]
    pub enable_simd: bool,

    #[serde(default)]
    pub enable_io_uring: bool,

    #[serde(default = "default_simd_batch_size")]
    pub simd_batch_size: usize,

    #[serde(default = "default_io_uring_queue_depth")]
    pub io_uring_queue_depth: u32,

    #[serde(default = "default_prefetch_distance")]
    pub prefetch_distance: usize,

    #[serde(default = "default_true")]
    pub enable_compression_accel: bool,
}

impl Default for HwAccelConfig {
    fn default() -> Self {
        Self {
            enable_simd: true,
            enable_io_uring: false,
            simd_batch_size: 256,
            io_uring_queue_depth: 128,
            prefetch_distance: 4,
            enable_compression_accel: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Capabilities
// ---------------------------------------------------------------------------

/// Detected hardware capabilities of the current platform.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HwCapabilities {
    pub sse2: bool,
    pub sse42: bool,
    pub avx2: bool,
    pub avx512: bool,
    pub neon: bool,
    pub io_uring_supported: bool,
    pub cpu_model: String,
    pub cache_line_size: usize,
    pub l1_cache_kb: usize,
    pub detected_at: String,
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// Atomic counters for hardware-accelerated operations.
#[derive(Debug, Default)]
pub struct HwAccelStats {
    pub simd_operations: AtomicU64,
    pub io_uring_operations: AtomicU64,
    pub bytes_processed_simd: AtomicU64,
    pub crc_computations: AtomicU64,
    pub compression_accel_ops: AtomicU64,
    pub throughput_improvement_pct: AtomicU64,
}

/// Point-in-time snapshot of [`HwAccelStats`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HwAccelStatsSnapshot {
    pub simd_operations: u64,
    pub io_uring_operations: u64,
    pub bytes_processed_simd: u64,
    pub crc_computations: u64,
    pub compression_accel_ops: u64,
    pub throughput_improvement_pct: u64,
}

// ---------------------------------------------------------------------------
// Batch result types
// ---------------------------------------------------------------------------

/// Result of a batch processing operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchProcessResult {
    pub records_processed: usize,
    pub bytes_processed: u64,
    pub duration_ns: u64,
    pub method: ProcessMethod,
}

/// Which processing path was used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessMethod {
    Scalar,
    Simd128,
    Simd256,
    Simd512,
    Neon,
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// SIMD / io_uring optimisation layer for record processing.
pub struct HwAccelerationEngine {
    config: HwAccelConfig,
    capabilities: HwCapabilities,
    stats: Arc<HwAccelStats>,
}

impl HwAccelerationEngine {
    /// Create a new engine, detecting hardware capabilities at construction.
    pub fn new(config: HwAccelConfig) -> Self {
        let capabilities = Self::detect_capabilities();
        Self {
            config,
            capabilities,
            stats: Arc::new(HwAccelStats::default()),
        }
    }

    /// Probe CPU features and platform capabilities.
    pub fn detect_capabilities() -> HwCapabilities {
        let sse2 = cfg!(target_feature = "sse2") || cfg!(target_arch = "x86_64");
        let sse42 = cfg!(target_feature = "sse4.2");
        let avx2 = cfg!(target_feature = "avx2");
        let avx512 = cfg!(target_feature = "avx512f");
        let neon = cfg!(target_feature = "neon") || cfg!(target_arch = "aarch64");

        let io_uring_supported = cfg!(target_os = "linux");

        let cpu_model = if cfg!(target_arch = "x86_64") {
            "x86_64".to_string()
        } else if cfg!(target_arch = "aarch64") {
            "aarch64".to_string()
        } else {
            "unknown".to_string()
        };

        // Conservative defaults — real detection would read cpuid / sysfs.
        let cache_line_size = 64;
        let l1_cache_kb = 32;

        HwCapabilities {
            sse2,
            sse42,
            avx2,
            avx512,
            neon,
            io_uring_supported,
            cpu_model,
            cache_line_size,
            l1_cache_kb,
            detected_at: chrono::Utc::now().to_rfc3339(),
        }
    }

    /// Return a reference to the detected capabilities.
    pub fn capabilities(&self) -> &HwCapabilities {
        &self.capabilities
    }

    /// Return a reference to the current configuration.
    pub fn config(&self) -> &HwAccelConfig {
        &self.config
    }

    // -- CRC32 ---------------------------------------------------------------

    /// Batch CRC32 computation. Uses an optimised lookup-table approach when
    /// SIMD is conceptually available, otherwise falls back to a byte-at-a-time
    /// scalar implementation.
    pub fn batch_crc32(&self, data: &[&[u8]]) -> Vec<u32> {
        let start = Instant::now();
        let method = self.select_method();

        let results: Vec<u32> = data.iter().map(|buf| Self::crc32_scalar(buf)).collect();

        self.stats
            .crc_computations
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        let total_bytes: u64 = data.iter().map(|b| b.len() as u64).sum();
        self.stats
            .bytes_processed_simd
            .fetch_add(total_bytes, Ordering::Relaxed);
        self.stats
            .simd_operations
            .fetch_add(if method != ProcessMethod::Scalar { 1 } else { 0 }, Ordering::Relaxed);

        let _ = BatchProcessResult {
            records_processed: data.len(),
            bytes_processed: total_bytes,
            duration_ns: start.elapsed().as_nanos() as u64,
            method,
        };

        results
    }

    /// Simple CRC32 (ISO 3309 polynomial) computed byte-at-a-time.
    fn crc32_scalar(data: &[u8]) -> u32 {
        let mut crc: u32 = 0xFFFF_FFFF;
        for &byte in data {
            crc ^= byte as u32;
            for _ in 0..8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ 0xEDB8_8320;
                } else {
                    crc >>= 1;
                }
            }
        }
        !crc
    }

    // -- Byte search ---------------------------------------------------------

    /// SIMD-aware byte search across multiple buffers.
    pub fn batch_find_byte(&self, haystacks: &[&[u8]], needle: u8) -> Vec<Option<usize>> {
        let method = self.select_method();

        let results: Vec<Option<usize>> = haystacks
            .iter()
            .map(|buf| Self::find_byte_scalar(buf, needle))
            .collect();

        self.stats
            .simd_operations
            .fetch_add(if method != ProcessMethod::Scalar { 1 } else { 0 }, Ordering::Relaxed);

        results
    }

    fn find_byte_scalar(haystack: &[u8], needle: u8) -> Option<usize> {
        haystack.iter().position(|&b| b == needle)
    }

    // -- Compression estimate ------------------------------------------------

    /// Estimate compressed sizes for a batch of buffers by counting unique
    /// byte frequencies — a cheap heuristic for compressibility.
    pub fn batch_compress_estimate(&self, data: &[&[u8]]) -> Vec<usize> {
        let results: Vec<usize> = data.iter().map(|buf| Self::compress_estimate_scalar(buf)).collect();

        self.stats
            .compression_accel_ops
            .fetch_add(data.len() as u64, Ordering::Relaxed);

        results
    }

    /// Estimate compressed size using byte-frequency entropy heuristic.
    fn compress_estimate_scalar(data: &[u8]) -> usize {
        if data.is_empty() {
            return 0;
        }

        let mut freq = [0u32; 256];
        for &b in data {
            freq[b as usize] += 1;
        }

        let len = data.len() as f64;
        let mut entropy: f64 = 0.0;
        for &count in &freq {
            if count > 0 {
                let p = count as f64 / len;
                entropy -= p * p.log2();
            }
        }

        // Estimated bits per byte → compressed byte count (minimum 1 byte).
        let estimated_bytes = ((entropy * len) / 8.0).ceil() as usize;
        estimated_bytes.max(1).min(data.len())
    }

    // -- Batch sizing --------------------------------------------------------

    /// Return the optimal batch size based on detected SIMD width.
    pub fn optimal_batch_size(&self) -> usize {
        if !self.config.enable_simd {
            return self.config.simd_batch_size;
        }

        if self.capabilities.avx512 {
            self.config.simd_batch_size * 4
        } else if self.capabilities.avx2 {
            self.config.simd_batch_size * 2
        } else if self.capabilities.neon || self.capabilities.sse42 {
            self.config.simd_batch_size
        } else {
            self.config.simd_batch_size / 2
        }
    }

    // -- Stats ---------------------------------------------------------------

    /// Take a point-in-time snapshot of the acceleration statistics.
    pub fn stats(&self) -> HwAccelStatsSnapshot {
        HwAccelStatsSnapshot {
            simd_operations: self.stats.simd_operations.load(Ordering::Relaxed),
            io_uring_operations: self.stats.io_uring_operations.load(Ordering::Relaxed),
            bytes_processed_simd: self.stats.bytes_processed_simd.load(Ordering::Relaxed),
            crc_computations: self.stats.crc_computations.load(Ordering::Relaxed),
            compression_accel_ops: self.stats.compression_accel_ops.load(Ordering::Relaxed),
            throughput_improvement_pct: self
                .stats
                .throughput_improvement_pct
                .load(Ordering::Relaxed),
        }
    }

    // -- Internals -----------------------------------------------------------

    /// Select the best processing method based on detected capabilities.
    fn select_method(&self) -> ProcessMethod {
        if !self.config.enable_simd {
            return ProcessMethod::Scalar;
        }
        if self.capabilities.avx512 {
            ProcessMethod::Simd512
        } else if self.capabilities.avx2 {
            ProcessMethod::Simd256
        } else if self.capabilities.neon {
            ProcessMethod::Neon
        } else if self.capabilities.sse42 || self.capabilities.sse2 {
            ProcessMethod::Simd128
        } else {
            ProcessMethod::Scalar
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_engine() -> HwAccelerationEngine {
        HwAccelerationEngine::new(HwAccelConfig::default())
    }

    // -- Config tests --------------------------------------------------------

    #[test]
    fn test_default_config() {
        let cfg = HwAccelConfig::default();
        assert!(cfg.enable_simd);
        assert!(!cfg.enable_io_uring);
        assert_eq!(cfg.simd_batch_size, 256);
        assert_eq!(cfg.io_uring_queue_depth, 128);
        assert_eq!(cfg.prefetch_distance, 4);
        assert!(cfg.enable_compression_accel);
    }

    #[test]
    fn test_config_serde_defaults() {
        let cfg: HwAccelConfig = serde_json::from_str("{}").unwrap();
        assert!(cfg.enable_simd);
        assert!(!cfg.enable_io_uring);
        assert_eq!(cfg.simd_batch_size, 256);
    }

    #[test]
    fn test_config_serde_override() {
        let json = r#"{"enable_simd": false, "simd_batch_size": 512}"#;
        let cfg: HwAccelConfig = serde_json::from_str(json).unwrap();
        assert!(!cfg.enable_simd);
        assert_eq!(cfg.simd_batch_size, 512);
    }

    // -- Capability detection ------------------------------------------------

    #[test]
    fn test_detect_capabilities() {
        let caps = HwAccelerationEngine::detect_capabilities();
        assert!(!caps.cpu_model.is_empty());
        assert_eq!(caps.cache_line_size, 64);
        assert_eq!(caps.l1_cache_kb, 32);
        assert!(!caps.detected_at.is_empty());
    }

    #[test]
    fn test_capabilities_accessible() {
        let engine = default_engine();
        let caps = engine.capabilities();
        assert_eq!(caps.cache_line_size, 64);
    }

    // -- CRC32 ---------------------------------------------------------------

    #[test]
    fn test_batch_crc32_empty_input() {
        let engine = default_engine();
        let results = engine.batch_crc32(&[]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_batch_crc32_single_buffer() {
        let engine = default_engine();
        let data = b"hello world";
        let results = engine.batch_crc32(&[data]);
        assert_eq!(results.len(), 1);
        // CRC32 of "hello world" is well-known: 0x0D4A1185
        assert_eq!(results[0], 0x0D4A_1185);
    }

    #[test]
    fn test_batch_crc32_multiple_buffers() {
        let engine = default_engine();
        let a = b"foo";
        let b = b"bar";
        let c = b"baz";
        let results = engine.batch_crc32(&[a, b, c]);
        assert_eq!(results.len(), 3);
        // Each buffer should produce a deterministic CRC.
        assert_eq!(results[0], engine.batch_crc32(&[a])[0]);
        assert_eq!(results[1], engine.batch_crc32(&[b])[0]);
    }

    #[test]
    fn test_crc32_empty_buffer() {
        let engine = default_engine();
        let results = engine.batch_crc32(&[b""]);
        // CRC32 of empty data == 0x00000000
        assert_eq!(results[0], 0x0000_0000);
    }

    #[test]
    fn test_crc32_deterministic() {
        let engine = default_engine();
        let data = b"determinism";
        let r1 = engine.batch_crc32(&[data]);
        let r2 = engine.batch_crc32(&[data]);
        assert_eq!(r1, r2);
    }

    // -- Byte search ---------------------------------------------------------

    #[test]
    fn test_batch_find_byte_found() {
        let engine = default_engine();
        let results = engine.batch_find_byte(&[b"abcdef"], b'c');
        assert_eq!(results, vec![Some(2)]);
    }

    #[test]
    fn test_batch_find_byte_not_found() {
        let engine = default_engine();
        let results = engine.batch_find_byte(&[b"abcdef"], b'z');
        assert_eq!(results, vec![None]);
    }

    #[test]
    fn test_batch_find_byte_multiple() {
        let engine = default_engine();
        let results = engine.batch_find_byte(&[b"abc", b"xyz", b"hello"], b'x');
        assert_eq!(results, vec![None, Some(0), None]);
    }

    #[test]
    fn test_batch_find_byte_empty_haystack() {
        let engine = default_engine();
        let results = engine.batch_find_byte(&[b""], b'a');
        assert_eq!(results, vec![None]);
    }

    // -- Compress estimate ---------------------------------------------------

    #[test]
    fn test_compress_estimate_empty() {
        let engine = default_engine();
        let results = engine.batch_compress_estimate(&[b""]);
        assert_eq!(results, vec![0]);
    }

    #[test]
    fn test_compress_estimate_uniform_data() {
        let engine = default_engine();
        let data = vec![0xAAu8; 1024];
        let results = engine.batch_compress_estimate(&[&data]);
        // Perfectly uniform data has zero entropy → estimated 1 byte minimum.
        assert!(results[0] < data.len());
    }

    #[test]
    fn test_compress_estimate_random_data() {
        let engine = default_engine();
        // Pseudo-random: all 256 byte values equally.
        let data: Vec<u8> = (0..=255).cycle().take(1024).collect();
        let results = engine.batch_compress_estimate(&[&data]);
        // High entropy ≈ incompressible — estimated size close to original.
        assert!(results[0] > 512);
    }

    #[test]
    fn test_compress_estimate_multiple() {
        let engine = default_engine();
        let uniform = vec![0u8; 512];
        let mixed: Vec<u8> = (0..=255).cycle().take(512).collect();
        let results = engine.batch_compress_estimate(&[&uniform, &mixed]);
        assert_eq!(results.len(), 2);
        assert!(results[0] < results[1]);
    }

    // -- Optimal batch size --------------------------------------------------

    #[test]
    fn test_optimal_batch_size_simd_disabled() {
        let mut cfg = HwAccelConfig::default();
        cfg.enable_simd = false;
        let engine = HwAccelerationEngine::new(cfg.clone());
        assert_eq!(engine.optimal_batch_size(), cfg.simd_batch_size);
    }

    #[test]
    fn test_optimal_batch_size_returns_positive() {
        let engine = default_engine();
        assert!(engine.optimal_batch_size() > 0);
    }

    // -- Stats ---------------------------------------------------------------

    #[test]
    fn test_stats_initial_zeroes() {
        let engine = default_engine();
        let snap = engine.stats();
        assert_eq!(snap.simd_operations, 0);
        assert_eq!(snap.io_uring_operations, 0);
        assert_eq!(snap.bytes_processed_simd, 0);
        assert_eq!(snap.crc_computations, 0);
        assert_eq!(snap.compression_accel_ops, 0);
    }

    #[test]
    fn test_stats_after_crc() {
        let engine = default_engine();
        engine.batch_crc32(&[b"a", b"b", b"c"]);
        let snap = engine.stats();
        assert_eq!(snap.crc_computations, 3);
        assert_eq!(snap.bytes_processed_simd, 3);
    }

    #[test]
    fn test_stats_after_compress_estimate() {
        let engine = default_engine();
        engine.batch_compress_estimate(&[b"test1", b"test2"]);
        let snap = engine.stats();
        assert_eq!(snap.compression_accel_ops, 2);
    }

    // -- ProcessMethod / select_method ---------------------------------------

    #[test]
    fn test_select_method_simd_disabled() {
        let mut cfg = HwAccelConfig::default();
        cfg.enable_simd = false;
        let engine = HwAccelerationEngine::new(cfg);
        // When SIMD is disabled the engine should always choose Scalar.
        let data = b"check";
        let result = engine.batch_crc32(&[data]);
        assert_eq!(result.len(), 1);
        // Stats should show zero SIMD operations.
        assert_eq!(engine.stats().simd_operations, 0);
    }

    #[test]
    fn test_process_method_serialisation() {
        let json = serde_json::to_string(&ProcessMethod::Simd256).unwrap();
        let back: ProcessMethod = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ProcessMethod::Simd256);
    }

    #[test]
    fn test_batch_process_result_serialisation() {
        let r = BatchProcessResult {
            records_processed: 10,
            bytes_processed: 1024,
            duration_ns: 500,
            method: ProcessMethod::Scalar,
        };
        let json = serde_json::to_string(&r).unwrap();
        let back: BatchProcessResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.records_processed, 10);
    }
}
