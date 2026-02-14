//! Benchmarks for zero-copy networking optimizations
//!
//! Run with: cargo bench --bench zerocopy_benchmarks
//!
//! These benchmarks measure the performance of:
//! - Buffer pool allocation vs fresh allocation
//! - Memory-mapped reads vs pread
//! - Zero-copy reader performance

use bytes::{Bytes, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::io::Write;
use std::sync::Arc;
use streamline::storage::record::{Record, RecordBatch};
use streamline::storage::segment::Segment;
use streamline::storage::{BufferPool, MmapSegmentReader, ZeroCopyReader};
use tempfile::{tempdir, NamedTempFile};

/// Benchmark buffer pool get/put cycle vs fresh allocation
fn bench_buffer_pool(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = Arc::new(BufferPool::new(16, 64 * 1024));

    let mut group = c.benchmark_group("buffer_allocation");
    group.throughput(Throughput::Elements(1));

    // Benchmark fresh allocation
    group.bench_function("fresh_alloc_64kb", |b| {
        b.iter(|| {
            let buf = BytesMut::with_capacity(64 * 1024);
            black_box(buf);
        });
    });

    // Benchmark pooled allocation
    group.bench_function("pooled_alloc_64kb", |b| {
        b.to_async(&rt).iter(|| {
            let pool = pool.clone();
            async move {
                let buf = pool.get().await;
                pool.put(buf).await;
            }
        });
    });

    // Benchmark capacity-based allocation
    for size in [4096, 16384, 65536, 262144] {
        group.bench_with_input(
            BenchmarkId::new("get_with_capacity", size),
            &size,
            |b, &size| {
                b.to_async(&rt).iter(|| {
                    let pool = pool.clone();
                    async move {
                        let buf = pool.get_with_capacity(size).await;
                        black_box(buf);
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark zero-copy reader vs standard file read
fn bench_file_read(c: &mut Criterion) {
    // Create test file with 1MB of data
    let mut temp_file = NamedTempFile::new().unwrap();
    let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
    temp_file.write_all(&data).unwrap();
    temp_file.flush().unwrap();

    let path = temp_file.path().to_path_buf();
    let reader = ZeroCopyReader::open(&path).unwrap();

    let mut group = c.benchmark_group("file_read");

    // Benchmark different read sizes
    for size in [4096, 16384, 65536, 262144] {
        group.throughput(Throughput::Bytes(size as u64));

        // Standard file read
        group.bench_with_input(BenchmarkId::new("std_read", size), &size, |b, &size| {
            use std::fs::File;
            use std::io::{Read, Seek, SeekFrom};

            let mut file = File::open(&path).unwrap();
            let mut buf = vec![0u8; size];

            b.iter(|| {
                file.seek(SeekFrom::Start(0)).unwrap();
                file.read_exact(&mut buf).unwrap();
                black_box(&buf);
            });
        });

        // Zero-copy pread
        group.bench_with_input(
            BenchmarkId::new("zerocopy_pread", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    let result = reader.read_range(0, size).unwrap();
                    black_box(result);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark mmap reader vs pread for segment reads
fn bench_segment_read(c: &mut Criterion) {
    let temp_dir = tempdir().unwrap();
    let segment_path = temp_dir.path().join("benchmark.segment");

    // Create segment with 1000 records
    {
        let mut segment = Segment::create(&segment_path, 0).unwrap();
        let timestamp = chrono::Utc::now().timestamp_millis();

        for batch_idx in 0..10 {
            let mut batch = RecordBatch::new(batch_idx * 100, timestamp);
            for i in 0..100 {
                let offset = batch_idx * 100 + i;
                batch.add_record(Record::new(
                    offset,
                    timestamp,
                    Some(Bytes::from(format!("key-{:08}", offset))),
                    Bytes::from(format!("value-{:08}-{}", offset, "x".repeat(100))),
                ));
            }
            segment.append_batch(&batch).unwrap();
        }
        segment.seal().unwrap();
    }

    let file_size = std::fs::metadata(&segment_path).unwrap().len();

    let mut group = c.benchmark_group("segment_read");
    group.throughput(Throughput::Bytes(file_size));

    // Benchmark mmap reader
    group.bench_function("mmap_read", |b| {
        let mmap_reader = MmapSegmentReader::open(&segment_path, 0).unwrap();
        b.iter(|| {
            let data = mmap_reader.data();
            black_box(data);
        });
    });

    // Benchmark mmap slice access
    group.bench_function("mmap_slice", |b| {
        let mmap_reader = MmapSegmentReader::open(&segment_path, 0).unwrap();
        b.iter(|| {
            // Simulate random access reads
            for offset in (64..file_size as usize).step_by(1000) {
                let slice = mmap_reader.slice(offset, 100.min(file_size as usize - offset));
                black_box(slice);
            }
        });
    });

    // Benchmark pread for comparison
    group.bench_function("pread", |b| {
        let reader = ZeroCopyReader::open(&segment_path).unwrap();
        b.iter(|| {
            // Simulate random access reads
            for offset in (64..file_size as u64).step_by(1000) {
                let data = reader.read_range(offset, 100).unwrap();
                black_box(data);
            }
        });
    });

    group.finish();
}

/// Benchmark record batch aggregation with and without pre-allocation
fn bench_record_aggregation(c: &mut Criterion) {
    // Create sample records
    let records: Vec<Record> = (0..100)
        .map(|i| {
            Record::new(
                i,
                1234567890,
                Some(Bytes::from(format!("key-{}", i))),
                Bytes::from(format!("value-{}-{}", i, "x".repeat(1000))),
            )
        })
        .collect();

    let mut group = c.benchmark_group("record_aggregation");

    let total_size: usize = records.iter().map(|r| r.value.len()).sum();
    group.throughput(Throughput::Bytes(total_size as u64));

    // Without pre-allocation (old method)
    group.bench_function("no_prealloc", |b| {
        b.iter(|| {
            let mut buf = BytesMut::new();
            for record in &records {
                buf.extend_from_slice(&record.value);
            }
            black_box(buf.freeze());
        });
    });

    // With pre-allocation (new method)
    group.bench_function("with_prealloc", |b| {
        b.iter(|| {
            let total_len: usize = records.iter().map(|r| r.value.len()).sum();
            let mut buf = BytesMut::with_capacity(total_len);
            for record in &records {
                buf.extend_from_slice(&record.value);
            }
            black_box(buf.freeze());
        });
    });

    group.finish();
}

/// Benchmark cache key hashing
fn bench_cache_key(c: &mut Criterion) {
    use std::collections::HashMap;
    use streamline::protocol::response_cache::CacheKey;

    let mut group = c.benchmark_group("cache_key");

    // Benchmark key creation
    group.bench_function("create", |b| {
        b.iter(|| {
            let key = CacheKey::new("my-topic".to_string(), 0, 12345, 65536);
            black_box(key);
        });
    });

    // Benchmark hashmap lookup
    group.bench_function("hashmap_lookup", |b| {
        let mut map = HashMap::new();
        for i in 0..1000 {
            let key = CacheKey::new(format!("topic-{}", i % 10), i % 5, (i * 100) as i64, 65536);
            map.insert(key, i);
        }

        let lookup_key = CacheKey::new("topic-5".to_string(), 2, 50000, 65536);

        b.iter(|| {
            let result = map.get(&lookup_key);
            black_box(result);
        });
    });

    group.finish();
}

/// Benchmark response cache operations
fn bench_response_cache(c: &mut Criterion) {
    use streamline::protocol::response_cache::{CacheKey, ResponseCache};

    let temp_dir = tempdir().unwrap();
    let cache = ResponseCache::new(temp_dir.path().to_path_buf(), 64 * 1024 * 1024, 60).unwrap();

    // Pre-populate cache with entries
    let test_data = vec![0u8; 64 * 1024]; // 64KB response
    for i in 0..100 {
        let key = CacheKey::new(format!("topic-{}", i % 10), i % 5, (i * 1000) as i64, 65536);
        cache.put(key, &test_data).unwrap();
    }

    let mut group = c.benchmark_group("response_cache");

    // Benchmark cache hit
    group.bench_function("cache_hit", |b| {
        let key = CacheKey::new("topic-5".to_string(), 2, 50000, 65536);
        b.iter(|| {
            let result = cache.get(&key);
            black_box(result);
        });
    });

    // Benchmark cache miss
    group.bench_function("cache_miss", |b| {
        let key = CacheKey::new("nonexistent".to_string(), 99, 999999, 65536);
        b.iter(|| {
            let result = cache.get(&key);
            black_box(result);
        });
    });

    // Benchmark cache put
    group.bench_function("cache_put_64kb", |b| {
        let mut counter = 1000;
        b.iter(|| {
            let key = CacheKey::new(format!("bench-{}", counter), 0, counter, 65536);
            counter += 1;
            let result = cache.put(key, &test_data).unwrap();
            black_box(result);
        });
    });

    group.finish();
}

/// Benchmark zero-copy path vs regular path for fetch-like operations
fn bench_zerocopy_vs_regular(c: &mut Criterion) {
    let temp_dir = tempdir().unwrap();
    let segment_path = temp_dir.path().join("zerocopy_bench.segment");

    // Create a larger segment for realistic benchmarking
    {
        let mut segment = Segment::create(&segment_path, 0).unwrap();
        let timestamp = chrono::Utc::now().timestamp_millis();

        // Create 100 batches of 100 records each = 10,000 records
        for batch_idx in 0..100 {
            let mut batch = RecordBatch::new(batch_idx * 100, timestamp);
            for i in 0..100 {
                let offset = batch_idx * 100 + i;
                batch.add_record(Record::new(
                    offset,
                    timestamp,
                    Some(Bytes::from(format!("key-{:08}", offset))),
                    Bytes::from(format!("value-{:08}-{}", offset, "x".repeat(500))),
                ));
            }
            segment.append_batch(&batch).unwrap();
        }
        segment.seal().unwrap();
    }

    let file_size = std::fs::metadata(&segment_path).unwrap().len();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = Arc::new(BufferPool::new(16, 64 * 1024));

    let mut group = c.benchmark_group("zerocopy_vs_regular");
    group.throughput(Throughput::Bytes(file_size));

    // Regular path: std::fs::read + fresh allocation
    group.bench_function("regular_path", |b| {
        b.iter(|| {
            // Simulate regular fetch: read file, allocate buffer, copy data
            let data = std::fs::read(&segment_path).unwrap();
            let mut response = BytesMut::new();
            response.extend_from_slice(&data);
            black_box(response.freeze());
        });
    });

    // Zero-copy path: mmap + buffer pool
    group.bench_function("zerocopy_mmap_bufferpool", |b| {
        let mmap_reader = MmapSegmentReader::open(&segment_path, 0).unwrap();
        b.to_async(&rt).iter(|| {
            let pool = pool.clone();
            let data = mmap_reader.data();
            async move {
                // Simulate zero-copy fetch: mmap read + pooled buffer
                let mut response = pool.get_with_capacity(data.len()).await;
                response.extend_from_slice(data);
                black_box(response.freeze());
            }
        });
    });

    // Zero-copy path: mmap only (direct slice)
    group.bench_function("zerocopy_mmap_direct", |b| {
        let mmap_reader = MmapSegmentReader::open(&segment_path, 0).unwrap();
        b.iter(|| {
            // Simulate zero-copy fetch: direct mmap access (no copy)
            let data = mmap_reader.data();
            black_box(data);
        });
    });

    // Zero-copy path: pread + buffer pool
    group.bench_function("zerocopy_pread_bufferpool", |b| {
        let reader = ZeroCopyReader::open(&segment_path).unwrap();
        b.to_async(&rt).iter(|| {
            let pool = pool.clone();
            // Read synchronously, then use async buffer pool
            let data = reader.read_range(0, file_size as usize).unwrap();
            async move {
                let mut response = pool.get_with_capacity(data.len()).await;
                response.extend_from_slice(&data);
                black_box(response.freeze());
            }
        });
    });

    group.finish();
}

/// Benchmark simulated fetch response building
fn bench_fetch_response_building(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = Arc::new(BufferPool::new(16, 64 * 1024));

    // Create sample records like a real fetch would return
    let records: Vec<Record> = (0..500)
        .map(|i| {
            Record::new(
                i,
                1234567890,
                Some(Bytes::from(format!("key-{:08}", i))),
                Bytes::from(format!("value-{:08}-{}", i, "x".repeat(200))),
            )
        })
        .collect();

    let total_size: usize = records.iter().map(|r| r.value.len()).sum();

    let mut group = c.benchmark_group("fetch_response_building");
    group.throughput(Throughput::Bytes(total_size as u64));

    // Old method: no pre-allocation, no buffer pool
    group.bench_function("old_method_no_prealloc", |b| {
        b.iter(|| {
            let mut buf = BytesMut::new();
            for record in &records {
                buf.extend_from_slice(&record.value);
            }
            black_box(buf.freeze());
        });
    });

    // Improved method: pre-allocation only
    group.bench_function("prealloc_only", |b| {
        b.iter(|| {
            let total_len: usize = records.iter().map(|r| r.value.len()).sum();
            let mut buf = BytesMut::with_capacity(total_len);
            for record in &records {
                buf.extend_from_slice(&record.value);
            }
            black_box(buf.freeze());
        });
    });

    // New method: buffer pool with capacity
    group.bench_function("buffer_pool_with_capacity", |b| {
        b.to_async(&rt).iter(|| {
            let pool = pool.clone();
            let records = &records;
            async move {
                let total_len: usize = records.iter().map(|r| r.value.len()).sum();
                let mut buf = pool.get_with_capacity(total_len).await;
                for record in records {
                    buf.extend_from_slice(&record.value);
                }
                black_box(buf.freeze());
            }
        });
    });

    group.finish();
}

/// Benchmark TLS vs plain TCP context overhead
fn bench_connection_context(c: &mut Criterion) {
    use streamline::protocol::ConnectionContext;
    use streamline::storage::ZeroCopyConfig;

    let mut group = c.benchmark_group("connection_context");

    // Benchmark context creation
    group.bench_function("create_plain_tcp", |b| {
        let config = ZeroCopyConfig::default();
        b.iter(|| {
            let ctx = ConnectionContext::plain_tcp(None, config.clone());
            black_box(ctx);
        });
    });

    group.bench_function("create_tls", |b| {
        let config = ZeroCopyConfig::default();
        b.iter(|| {
            let ctx = ConnectionContext::tls(None, config.clone());
            black_box(ctx);
        });
    });

    // Benchmark capability checks
    group.bench_function("can_sendfile_check", |b| {
        let config = ZeroCopyConfig::default();
        let ctx = ConnectionContext::plain_tcp(None, config);
        b.iter(|| {
            let result = ctx.can_sendfile();
            black_box(result);
        });
    });

    group.bench_function("can_mmap_check", |b| {
        let config = ZeroCopyConfig::default();
        let ctx = ConnectionContext::plain_tcp(None, config);
        b.iter(|| {
            let result = ctx.can_mmap();
            black_box(result);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_buffer_pool,
    bench_file_read,
    bench_segment_read,
    bench_record_aggregation,
    bench_cache_key,
    bench_response_cache,
    bench_zerocopy_vs_regular,
    bench_fetch_response_building,
    bench_connection_context,
);

criterion_main!(benches);
