//! Time-Series Compression Algorithms
//!
//! Provides specialized compression for time-series data including:
//! - Delta encoding for timestamps
//! - Double-delta (delta-of-delta) for regular intervals
//! - XOR-based compression for floating point values (Gorilla)
//! - Run-length encoding for repeated values

use super::{DataPoint, DataPointBatch};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

/// Compression codec types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum CompressionCodec {
    /// No compression
    None,
    /// Delta encoding (good for timestamps)
    Delta,
    /// Double-delta encoding (excellent for regular intervals)
    DoubleDelta,
    /// Gorilla-style XOR compression (for floating point)
    Gorilla,
    /// Run-length encoding (for repeated values)
    RunLength,
    /// Combined: DoubleDelta for timestamps, Gorilla for values
    #[default]
    TimeSeriesOptimal,
}

/// Time-series compressor
pub struct TimeSeriesCompressor {
    codec: CompressionCodec,
}

impl TimeSeriesCompressor {
    /// Create a new compressor with the specified codec
    pub fn new(codec: CompressionCodec) -> Self {
        Self { codec }
    }

    /// Compress a batch of data points
    pub fn compress(&self, batch: &DataPointBatch) -> Bytes {
        match self.codec {
            CompressionCodec::None => self.compress_none(batch),
            CompressionCodec::Delta => self.compress_delta(batch),
            CompressionCodec::DoubleDelta => self.compress_double_delta(batch),
            CompressionCodec::Gorilla => self.compress_gorilla(batch),
            CompressionCodec::RunLength => self.compress_rle(batch),
            CompressionCodec::TimeSeriesOptimal => self.compress_optimal(batch),
        }
    }

    /// Decompress bytes back to data points
    pub fn decompress(&self, data: &[u8]) -> Vec<DataPoint> {
        if data.is_empty() {
            return Vec::new();
        }

        match self.codec {
            CompressionCodec::None => self.decompress_none(data),
            CompressionCodec::Delta => self.decompress_delta(data),
            CompressionCodec::DoubleDelta => self.decompress_double_delta(data),
            CompressionCodec::Gorilla => self.decompress_gorilla(data),
            CompressionCodec::RunLength => self.decompress_rle(data),
            CompressionCodec::TimeSeriesOptimal => self.decompress_optimal(data),
        }
    }

    fn compress_none(&self, batch: &DataPointBatch) -> Bytes {
        // Simple JSON serialization for no compression
        serde_json::to_vec(batch)
            .map(Bytes::from)
            .unwrap_or_default()
    }

    fn decompress_none(&self, data: &[u8]) -> Vec<DataPoint> {
        serde_json::from_slice::<DataPointBatch>(data)
            .map(|b| b.points)
            .unwrap_or_default()
    }

    fn compress_delta(&self, batch: &DataPointBatch) -> Bytes {
        if batch.is_empty() {
            return Bytes::new();
        }

        let mut buf = BytesMut::new();

        // Header: codec (1 byte) + count (4 bytes)
        buf.put_u8(CompressionCodec::Delta as u8);
        buf.put_u32(batch.len() as u32);

        // First timestamp as absolute value
        let first = &batch.points[0];
        buf.put_i64(first.timestamp);

        // Delta-encode remaining timestamps
        let mut prev_ts = first.timestamp;
        for point in &batch.points[1..] {
            let delta = point.timestamp - prev_ts;
            buf.put_i64(delta);
            prev_ts = point.timestamp;
        }

        // Values as f64
        for point in &batch.points {
            buf.put_f64(point.value);
        }

        // Metric name (assume all same metric for simplicity)
        let metric_bytes = first.metric.as_bytes();
        buf.put_u16(metric_bytes.len() as u16);
        buf.put_slice(metric_bytes);

        buf.freeze()
    }

    fn decompress_delta(&self, data: &[u8]) -> Vec<DataPoint> {
        if data.len() < 5 {
            return Vec::new();
        }

        let mut buf = data;

        let _codec = buf.get_u8();
        let count = buf.get_u32() as usize;

        if count == 0 {
            return Vec::new();
        }

        // Read timestamps
        let mut timestamps = Vec::with_capacity(count);
        let first_ts = buf.get_i64();
        timestamps.push(first_ts);

        let mut prev_ts = first_ts;
        for _ in 1..count {
            let delta = buf.get_i64();
            prev_ts += delta;
            timestamps.push(prev_ts);
        }

        // Read values
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(buf.get_f64());
        }

        // Read metric name
        let metric_len = buf.get_u16() as usize;
        let metric = String::from_utf8_lossy(&buf[..metric_len]).to_string();

        // Reconstruct points
        timestamps
            .into_iter()
            .zip(values)
            .map(|(ts, val)| DataPoint::new(&metric, ts, val))
            .collect()
    }

    fn compress_double_delta(&self, batch: &DataPointBatch) -> Bytes {
        if batch.is_empty() {
            return Bytes::new();
        }

        let mut buf = BytesMut::new();

        // Header
        buf.put_u8(CompressionCodec::DoubleDelta as u8);
        buf.put_u32(batch.len() as u32);

        // First two timestamps as absolute
        let first = &batch.points[0];
        buf.put_i64(first.timestamp);

        if batch.len() > 1 {
            let second = &batch.points[1];
            let first_delta = second.timestamp - first.timestamp;
            buf.put_i64(first_delta);

            // Double-delta encode remaining
            let mut prev_ts = second.timestamp;
            let mut prev_delta = first_delta;

            for point in &batch.points[2..] {
                let delta = point.timestamp - prev_ts;
                let double_delta = delta - prev_delta;

                // Variable-length encoding of double-delta
                buf.put_i64(double_delta);

                prev_ts = point.timestamp;
                prev_delta = delta;
            }
        }

        // Values using XOR encoding
        self.encode_values_xor(&batch.points, &mut buf);

        // Metric name
        let metric_bytes = first.metric.as_bytes();
        buf.put_u16(metric_bytes.len() as u16);
        buf.put_slice(metric_bytes);

        buf.freeze()
    }

    fn decompress_double_delta(&self, data: &[u8]) -> Vec<DataPoint> {
        if data.len() < 5 {
            return Vec::new();
        }

        let mut buf = data;

        let _codec = buf.get_u8();
        let count = buf.get_u32() as usize;

        if count == 0 {
            return Vec::new();
        }

        // Read timestamps
        let mut timestamps = Vec::with_capacity(count);
        let first_ts = buf.get_i64();
        timestamps.push(first_ts);

        if count > 1 {
            let first_delta = buf.get_i64();
            let second_ts = first_ts + first_delta;
            timestamps.push(second_ts);

            let mut prev_ts = second_ts;
            let mut prev_delta = first_delta;

            for _ in 2..count {
                let double_delta = buf.get_i64();
                let delta = prev_delta + double_delta;
                prev_ts += delta;
                timestamps.push(prev_ts);
                prev_delta = delta;
            }
        }

        // Decode values
        let values = self.decode_values_xor(&mut buf, count);

        // Read metric name
        let metric_len = buf.get_u16() as usize;
        let metric = String::from_utf8_lossy(&buf[..metric_len]).to_string();

        // Reconstruct points
        timestamps
            .into_iter()
            .zip(values)
            .map(|(ts, val)| DataPoint::new(&metric, ts, val))
            .collect()
    }

    fn compress_gorilla(&self, batch: &DataPointBatch) -> Bytes {
        // Gorilla compression focuses on XOR encoding of floating point values
        let mut buf = BytesMut::new();

        buf.put_u8(CompressionCodec::Gorilla as u8);
        buf.put_u32(batch.len() as u32);

        if batch.is_empty() {
            return buf.freeze();
        }

        // Store timestamps normally
        for point in &batch.points {
            buf.put_i64(point.timestamp);
        }

        // XOR encode values
        self.encode_values_xor(&batch.points, &mut buf);

        // Metric name
        let metric_bytes = batch.points[0].metric.as_bytes();
        buf.put_u16(metric_bytes.len() as u16);
        buf.put_slice(metric_bytes);

        buf.freeze()
    }

    fn decompress_gorilla(&self, data: &[u8]) -> Vec<DataPoint> {
        if data.len() < 5 {
            return Vec::new();
        }

        let mut buf = data;

        let _codec = buf.get_u8();
        let count = buf.get_u32() as usize;

        if count == 0 {
            return Vec::new();
        }

        // Read timestamps
        let mut timestamps = Vec::with_capacity(count);
        for _ in 0..count {
            timestamps.push(buf.get_i64());
        }

        // Decode values
        let values = self.decode_values_xor(&mut buf, count);

        // Read metric name
        let metric_len = buf.get_u16() as usize;
        let metric = String::from_utf8_lossy(&buf[..metric_len]).to_string();

        timestamps
            .into_iter()
            .zip(values)
            .map(|(ts, val)| DataPoint::new(&metric, ts, val))
            .collect()
    }

    fn compress_rle(&self, batch: &DataPointBatch) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_u8(CompressionCodec::RunLength as u8);
        buf.put_u32(batch.len() as u32);

        if batch.is_empty() {
            return buf.freeze();
        }

        // Timestamps with delta
        let first = &batch.points[0];
        buf.put_i64(first.timestamp);

        let mut prev_ts = first.timestamp;
        for point in &batch.points[1..] {
            buf.put_i64(point.timestamp - prev_ts);
            prev_ts = point.timestamp;
        }

        // Run-length encode values
        let mut runs: Vec<(f64, u32)> = Vec::new();
        let mut current_value = first.value;
        let mut run_length = 1u32;

        for point in &batch.points[1..] {
            if (point.value - current_value).abs() < f64::EPSILON {
                run_length += 1;
            } else {
                runs.push((current_value, run_length));
                current_value = point.value;
                run_length = 1;
            }
        }
        runs.push((current_value, run_length));

        // Write runs
        buf.put_u32(runs.len() as u32);
        for (value, count) in runs {
            buf.put_f64(value);
            buf.put_u32(count);
        }

        // Metric name
        let metric_bytes = first.metric.as_bytes();
        buf.put_u16(metric_bytes.len() as u16);
        buf.put_slice(metric_bytes);

        buf.freeze()
    }

    fn decompress_rle(&self, data: &[u8]) -> Vec<DataPoint> {
        if data.len() < 5 {
            return Vec::new();
        }

        let mut buf = data;

        let _codec = buf.get_u8();
        let count = buf.get_u32() as usize;

        if count == 0 {
            return Vec::new();
        }

        // Read timestamps
        let mut timestamps = Vec::with_capacity(count);
        let first_ts = buf.get_i64();
        timestamps.push(first_ts);

        let mut prev_ts = first_ts;
        for _ in 1..count {
            let delta = buf.get_i64();
            prev_ts += delta;
            timestamps.push(prev_ts);
        }

        // Read and expand runs
        let num_runs = buf.get_u32() as usize;
        let mut values = Vec::with_capacity(count);

        for _ in 0..num_runs {
            let value = buf.get_f64();
            let run_count = buf.get_u32() as usize;
            for _ in 0..run_count {
                values.push(value);
            }
        }

        // Read metric name
        let metric_len = buf.get_u16() as usize;
        let metric = String::from_utf8_lossy(&buf[..metric_len]).to_string();

        timestamps
            .into_iter()
            .zip(values)
            .map(|(ts, val)| DataPoint::new(&metric, ts, val))
            .collect()
    }

    fn compress_optimal(&self, batch: &DataPointBatch) -> Bytes {
        // Use double-delta for timestamps, XOR for values
        self.compress_double_delta(batch)
    }

    fn decompress_optimal(&self, data: &[u8]) -> Vec<DataPoint> {
        self.decompress_double_delta(data)
    }

    fn encode_values_xor(&self, points: &[DataPoint], buf: &mut BytesMut) {
        if points.is_empty() {
            return;
        }

        // First value stored as-is
        let first_bits = points[0].value.to_bits();
        buf.put_u64(first_bits);

        // XOR subsequent values
        let mut prev_bits = first_bits;
        for point in &points[1..] {
            let curr_bits = point.value.to_bits();
            let xor = curr_bits ^ prev_bits;
            buf.put_u64(xor);
            prev_bits = curr_bits;
        }
    }

    fn decode_values_xor(&self, buf: &mut &[u8], count: usize) -> Vec<f64> {
        if count == 0 {
            return Vec::new();
        }

        let mut values = Vec::with_capacity(count);

        // First value
        let first_bits = buf.get_u64();
        values.push(f64::from_bits(first_bits));

        // XOR decode remaining
        let mut prev_bits = first_bits;
        for _ in 1..count {
            let xor = buf.get_u64();
            let curr_bits = xor ^ prev_bits;
            values.push(f64::from_bits(curr_bits));
            prev_bits = curr_bits;
        }

        values
    }
}

impl Default for TimeSeriesCompressor {
    fn default() -> Self {
        Self::new(CompressionCodec::TimeSeriesOptimal)
    }
}

/// Delta encoder for integers
pub struct DeltaEncoder {
    prev: i64,
}

impl DeltaEncoder {
    pub fn new() -> Self {
        Self { prev: 0 }
    }

    pub fn encode(&mut self, value: i64) -> i64 {
        let delta = value - self.prev;
        self.prev = value;
        delta
    }

    pub fn decode(&mut self, delta: i64) -> i64 {
        self.prev += delta;
        self.prev
    }

    pub fn reset(&mut self) {
        self.prev = 0;
    }
}

impl Default for DeltaEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Double-delta encoder (delta-of-delta)
pub struct DoubleDeltaEncoder {
    prev_value: i64,
    prev_delta: i64,
    first: bool,
    second: bool,
}

impl DoubleDeltaEncoder {
    pub fn new() -> Self {
        Self {
            prev_value: 0,
            prev_delta: 0,
            first: true,
            second: false,
        }
    }

    pub fn encode(&mut self, value: i64) -> i64 {
        if self.first {
            self.first = false;
            self.second = true;
            self.prev_value = value;
            return value;
        }

        if self.second {
            self.second = false;
            let delta = value - self.prev_value;
            self.prev_delta = delta;
            self.prev_value = value;
            return delta;
        }

        let delta = value - self.prev_value;
        let double_delta = delta - self.prev_delta;
        self.prev_delta = delta;
        self.prev_value = value;
        double_delta
    }

    pub fn reset(&mut self) {
        self.prev_value = 0;
        self.prev_delta = 0;
        self.first = true;
        self.second = false;
    }
}

impl Default for DoubleDeltaEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Gorilla-style bit packer for floating point values
pub struct GorillaBitPacker {
    prev_bits: u64,
    prev_leading: u32,
    prev_trailing: u32,
}

impl GorillaBitPacker {
    pub fn new() -> Self {
        Self {
            prev_bits: 0,
            prev_leading: 0,
            prev_trailing: 0,
        }
    }

    pub fn encode(&mut self, value: f64) -> (u64, u32, u32) {
        let bits = value.to_bits();
        let xor = bits ^ self.prev_bits;

        let leading = xor.leading_zeros();
        let trailing = xor.trailing_zeros();

        self.prev_bits = bits;
        self.prev_leading = leading;
        self.prev_trailing = trailing;

        (xor, leading, trailing)
    }

    pub fn decode(&mut self, xor: u64) -> f64 {
        let bits = xor ^ self.prev_bits;
        self.prev_bits = bits;
        f64::from_bits(bits)
    }
}

impl Default for GorillaBitPacker {
    fn default() -> Self {
        Self::new()
    }
}

/// Run-length encoder
pub struct RunLengthEncoder<T: PartialEq + Clone> {
    current: Option<T>,
    count: u32,
}

impl<T: PartialEq + Clone> RunLengthEncoder<T> {
    pub fn new() -> Self {
        Self {
            current: None,
            count: 0,
        }
    }

    pub fn add(&mut self, value: T) -> Option<(T, u32)> {
        match &self.current {
            Some(curr) if *curr == value => {
                self.count += 1;
                None
            }
            Some(_) => {
                let prev = self.current.take();
                self.current = Some(value);
                let old_count = self.count;
                self.count = 1;
                prev.map(|v| (v, old_count))
            }
            None => {
                self.current = Some(value);
                self.count = 1;
                None
            }
        }
    }

    pub fn flush(&mut self) -> Option<(T, u32)> {
        self.current.take().map(|v| (v, self.count))
    }
}

impl<T: PartialEq + Clone> Default for RunLengthEncoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_batch() -> DataPointBatch {
        let mut batch = DataPointBatch::new();
        batch.add(DataPoint::new("test.metric", 1000, 10.5));
        batch.add(DataPoint::new("test.metric", 2000, 11.2));
        batch.add(DataPoint::new("test.metric", 3000, 10.8));
        batch.add(DataPoint::new("test.metric", 4000, 12.1));
        batch
    }

    #[test]
    fn test_compress_decompress_none() {
        let compressor = TimeSeriesCompressor::new(CompressionCodec::None);
        let batch = create_test_batch();

        let compressed = compressor.compress(&batch);
        let decompressed = compressor.decompress(&compressed);

        assert_eq!(decompressed.len(), batch.len());
        assert_eq!(decompressed[0].timestamp, 1000);
        assert_eq!(decompressed[0].value, 10.5);
    }

    #[test]
    fn test_compress_decompress_delta() {
        let compressor = TimeSeriesCompressor::new(CompressionCodec::Delta);
        let batch = create_test_batch();

        let compressed = compressor.compress(&batch);
        let decompressed = compressor.decompress(&compressed);

        assert_eq!(decompressed.len(), batch.len());
        for (orig, dec) in batch.points.iter().zip(decompressed.iter()) {
            assert_eq!(orig.timestamp, dec.timestamp);
            assert!((orig.value - dec.value).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_compress_decompress_double_delta() {
        let compressor = TimeSeriesCompressor::new(CompressionCodec::DoubleDelta);
        let batch = create_test_batch();

        let compressed = compressor.compress(&batch);
        let decompressed = compressor.decompress(&compressed);

        assert_eq!(decompressed.len(), batch.len());
        for (orig, dec) in batch.points.iter().zip(decompressed.iter()) {
            assert_eq!(orig.timestamp, dec.timestamp);
        }
    }

    #[test]
    fn test_compress_decompress_rle() {
        let compressor = TimeSeriesCompressor::new(CompressionCodec::RunLength);

        // Batch with repeated values
        let mut batch = DataPointBatch::new();
        batch.add(DataPoint::new("test", 1000, 10.0));
        batch.add(DataPoint::new("test", 2000, 10.0));
        batch.add(DataPoint::new("test", 3000, 10.0));
        batch.add(DataPoint::new("test", 4000, 20.0));

        let compressed = compressor.compress(&batch);
        let decompressed = compressor.decompress(&compressed);

        assert_eq!(decompressed.len(), 4);
        assert_eq!(decompressed[0].value, 10.0);
        assert_eq!(decompressed[3].value, 20.0);
    }

    #[test]
    fn test_delta_encoder() {
        let mut encoder = DeltaEncoder::new();

        let values = vec![100, 110, 115, 120, 135];
        let encoded: Vec<i64> = values.iter().map(|&v| encoder.encode(v)).collect();

        assert_eq!(encoded, vec![100, 10, 5, 5, 15]);

        let mut decoder = DeltaEncoder::new();
        let decoded: Vec<i64> = encoded.iter().map(|&d| decoder.decode(d)).collect();

        assert_eq!(decoded, values);
    }

    #[test]
    fn test_double_delta_encoder() {
        let mut encoder = DoubleDeltaEncoder::new();

        // Regular interval timestamps (1000ms apart)
        let values = [1000, 2000, 3000, 4000, 5000];
        let encoded: Vec<i64> = values.iter().map(|&v| encoder.encode(v)).collect();

        // First value, first delta, then double-deltas (should be 0 for regular intervals)
        assert_eq!(encoded[0], 1000); // First value
        assert_eq!(encoded[1], 1000); // First delta
        assert_eq!(encoded[2], 0); // Double delta (1000 - 1000)
        assert_eq!(encoded[3], 0);
        assert_eq!(encoded[4], 0);
    }

    #[test]
    fn test_run_length_encoder() {
        let mut encoder = RunLengthEncoder::<i32>::new();

        assert!(encoder.add(1).is_none());
        assert!(encoder.add(1).is_none());
        assert!(encoder.add(1).is_none());

        let result = encoder.add(2);
        assert_eq!(result, Some((1, 3)));

        assert!(encoder.add(2).is_none());

        let flush = encoder.flush();
        assert_eq!(flush, Some((2, 2)));
    }
}
