//! I/O Backend benchmarks for Streamline
//!
//! These benchmarks measure the performance of the io_backend layer,
//! comparing standard async I/O with io_uring (when available on Linux).
//!
//! Run with:
//!   cargo bench --bench io_backend_benchmarks
//!
//! Run with io_uring (Linux only):
//!   cargo bench --bench io_backend_benchmarks --features io-uring

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use streamline::storage::{get_standard_backend, AsyncFile, AsyncFileSystem, IoBufferPool};
use tempfile::tempdir;
use tokio::runtime::Runtime;

/// Helper to run async benchmarks
fn run_async<F: std::future::Future>(f: F) -> F::Output {
    let rt = Runtime::new().unwrap();
    rt.block_on(f)
}

/// Benchmark sequential writes with different sizes
fn bench_write_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("io_backend_write");

    for size in [4096, 16384, 65536, 262144, 1048576].iter() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.dat");

        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::new("standard", size), size, |b, &sz| {
            b.iter(|| {
                run_async(async {
                    let fs = get_standard_backend();
                    let file = fs.create(&file_path).await.unwrap();
                    let data = vec![0xABu8; sz];
                    let (result, _) = file.write_at(black_box(data), 0).await;
                    result.unwrap();
                    file.sync_data().await.unwrap();
                })
            })
        });
    }

    group.finish();
}

/// Benchmark sequential reads with different sizes
fn bench_read_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("io_backend_read");

    for size in [4096, 16384, 65536, 262144, 1048576].iter() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.dat");

        // Pre-create file with data
        run_async(async {
            let fs = get_standard_backend();
            let file = fs.create(&file_path).await.unwrap();
            let data = vec![0xABu8; *size];
            let (result, _) = file.write_at(data, 0).await;
            result.unwrap();
            file.sync_data().await.unwrap();
        });

        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::new("standard", size), size, |b, &sz| {
            b.iter(|| {
                run_async(async {
                    let fs = get_standard_backend();
                    let file = fs.open(&file_path).await.unwrap();
                    let buf = vec![0u8; sz];
                    let (result, data) = file.read_at(buf, 0).await;
                    result.unwrap();
                    black_box(data)
                })
            })
        });
    }

    group.finish();
}

/// Benchmark random read patterns (simulating segment reads)
fn bench_random_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("io_backend_random_read");

    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.dat");
    let file_size = 10 * 1024 * 1024; // 10 MB file
    let read_size = 4096; // 4 KB reads

    // Pre-create file with data
    run_async(async {
        let fs = get_standard_backend();
        let file = fs.create(&file_path).await.unwrap();
        let data = vec![0xABu8; file_size];
        let (result, _) = file.write_at(data, 0).await;
        result.unwrap();
        file.sync_data().await.unwrap();
    });

    // Generate random offsets
    let offsets: Vec<u64> = (0..100)
        .map(|i| (i * 97 % (file_size / read_size)) as u64 * read_size as u64)
        .collect();

    group.throughput(Throughput::Elements(offsets.len() as u64));
    group.bench_function("standard_random_4k", |b| {
        b.iter(|| {
            run_async(async {
                let fs = get_standard_backend();
                let file = fs.open(&file_path).await.unwrap();
                for &offset in &offsets {
                    let buf = vec![0u8; read_size];
                    let (result, data) = file.read_at(buf, offset).await;
                    result.unwrap();
                    black_box(data);
                }
            })
        })
    });

    group.finish();
}

/// Benchmark append operations (simulating WAL writes)
fn bench_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("io_backend_append");

    for batch_size in [1, 10, 100].iter() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.dat");
        let record_size = 128; // 128 byte records

        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("standard", batch_size),
            batch_size,
            |b, &count| {
                b.iter(|| {
                    run_async(async {
                        let fs = get_standard_backend();
                        let file = fs.open_append(&file_path).await.unwrap();
                        let mut offset = file.size().await.unwrap_or(0);

                        for _ in 0..count {
                            let data = vec![0xABu8; record_size];
                            let (result, data) = file.write_at(data, offset).await;
                            result.unwrap();
                            offset += data.len() as u64;
                        }
                        file.sync_data().await.unwrap();
                    })
                })
            },
        );
    }

    group.finish();
}

/// Benchmark sync operations
fn bench_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("io_backend_sync");

    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.dat");

    // Pre-create file
    run_async(async {
        let fs = get_standard_backend();
        let file = fs.create(&file_path).await.unwrap();
        let data = vec![0xABu8; 4096];
        let (result, _) = file.write_at(data, 0).await;
        result.unwrap();
    });

    group.bench_function("sync_data", |b| {
        b.iter(|| {
            run_async(async {
                let fs = get_standard_backend();
                let file = fs.open_rw(&file_path).await.unwrap();
                let data = vec![0xABu8; 4096];
                let (result, _) = file.write_at(data, 0).await;
                result.unwrap();
                file.sync_data().await.unwrap();
            })
        })
    });

    group.bench_function("sync_all", |b| {
        b.iter(|| {
            run_async(async {
                let fs = get_standard_backend();
                let file = fs.open_rw(&file_path).await.unwrap();
                let data = vec![0xABu8; 4096];
                let (result, _) = file.write_at(data, 0).await;
                result.unwrap();
                file.sync_all().await.unwrap();
            })
        })
    });

    group.finish();
}

/// Benchmark buffer pool performance
fn bench_buffer_pool(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_pool");

    // Without buffer pool
    group.bench_function("allocate_4k", |b| {
        b.iter(|| {
            let buf: Vec<u8> = vec![0u8; 4096];
            black_box(buf)
        })
    });

    // With buffer pool
    let pool = IoBufferPool::default_pool();
    group.bench_function("pool_acquire_release_4k", |b| {
        b.iter(|| {
            let buf = pool.acquire(4096);
            black_box(&buf);
            pool.release(buf);
        })
    });

    // Measure allocation vs pool reuse for larger sizes
    for size in [4096, 65536, 1048576].iter() {
        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::new("allocate", size), size, |b, &sz| {
            b.iter(|| {
                let buf: Vec<u8> = vec![0u8; sz];
                black_box(buf)
            })
        });

        let pool = IoBufferPool::default_pool();
        group.bench_with_input(BenchmarkId::new("pool_cycle", size), size, |b, &sz| {
            b.iter(|| {
                let buf = pool.acquire(sz);
                black_box(&buf);
                pool.release(buf);
            })
        });
    }

    group.finish();
}

/// Benchmark concurrent file operations (simulating multiple partition writes)
fn bench_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("io_backend_concurrent");

    for num_files in [1, 4, 8].iter() {
        let dir = tempdir().unwrap();
        let files: Vec<_> = (0..*num_files)
            .map(|i| dir.path().join(format!("file_{}.dat", i)))
            .collect();

        group.throughput(Throughput::Elements(*num_files as u64));
        group.bench_with_input(
            BenchmarkId::new("standard", num_files),
            &files,
            |b, files| {
                b.iter(|| {
                    run_async(async {
                        let data = vec![0xABu8; 4096];

                        // Write to all files concurrently using JoinSet
                        let mut join_set = tokio::task::JoinSet::new();
                        for path in files.iter() {
                            let path = path.clone();
                            let data = data.clone();
                            join_set.spawn(async move {
                                let fs = get_standard_backend();
                                let file = fs.open_append(&path).await.unwrap();
                                let (result, _) = file.write_at(data, 0).await;
                                result.unwrap();
                                file.sync_data().await.unwrap();
                            });
                        }

                        // Wait for all tasks
                        while join_set.join_next().await.is_some() {}
                    })
                })
            },
        );
    }

    group.finish();
}

/// Benchmark AsyncSegment operations
fn bench_async_segment(c: &mut Criterion) {
    use streamline::storage::{AsyncSegment, Record, RecordBatch};

    let mut group = c.benchmark_group("async_segment");

    for record_count in [1, 10, 100].iter() {
        let dir = tempdir().unwrap();
        let segment_path = dir.path().join("segment");
        std::fs::create_dir_all(&segment_path).unwrap();

        group.throughput(Throughput::Elements(*record_count as u64));
        group.bench_with_input(
            BenchmarkId::new("append_records", record_count),
            record_count,
            |b, &count| {
                b.iter(|| {
                    run_async(async {
                        let fs = get_standard_backend();
                        let segment = AsyncSegment::create(&fs, &segment_path.join("0.segment"), 0)
                            .await
                            .unwrap();

                        for i in 0..count {
                            let mut batch = RecordBatch::new(i, 1000 + i);
                            batch.add_record(Record::new(
                                i,
                                1000 + i,
                                None,
                                bytes::Bytes::from(format!("value-{}", i)),
                            ));
                            segment.append_batch(&batch).await.unwrap();
                        }
                        segment.sync().await.unwrap();
                    })
                })
            },
        );
    }

    group.finish();
}

/// Benchmark AsyncWalWriter operations
fn bench_async_wal(c: &mut Criterion) {
    use streamline::storage::AsyncWalWriter;
    use streamline::WalConfig;

    let mut group = c.benchmark_group("async_wal");

    for entry_count in [1, 10, 100].iter() {
        let dir = tempdir().unwrap();

        group.throughput(Throughput::Elements(*entry_count as u64));
        group.bench_with_input(
            BenchmarkId::new("append_entries", entry_count),
            entry_count,
            |b, &count| {
                b.iter(|| {
                    run_async(async {
                        let fs = get_standard_backend();
                        let config = WalConfig {
                            enabled: true,
                            ..Default::default()
                        };
                        let wal = AsyncWalWriter::new(&fs, dir.path(), config).await.unwrap();

                        for i in 0..count {
                            wal.append_record(
                                "test-topic",
                                0,
                                None,
                                &bytes::Bytes::from(format!("value-{}", i)),
                            )
                            .await
                            .unwrap();
                        }
                        wal.sync().await.unwrap();
                        wal.close().await.unwrap();
                    })
                })
            },
        );
    }

    group.finish();
}

/// Benchmark AsyncSegmentIndex operations
fn bench_async_index(c: &mut Criterion) {
    use streamline::storage::{AsyncIndexBuilder, AsyncSegmentIndex};

    let mut group = c.benchmark_group("async_index");

    // Benchmark index building
    for entry_count in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*entry_count as u64));
        group.bench_with_input(
            BenchmarkId::new("build_index", entry_count),
            entry_count,
            |b, &count| {
                let dir = tempdir().unwrap();
                let index_path = dir.path().join("test.index");

                b.iter(|| {
                    let mut builder = AsyncIndexBuilder::new(4096);
                    for i in 0..count {
                        builder.record_batch(i, i as u64 * 100, 100);
                    }
                    black_box(builder.build(&index_path))
                })
            },
        );
    }

    // Benchmark index lookups
    let dir = tempdir().unwrap();
    let index_path = dir.path().join("test.index");

    let index = {
        let mut builder = AsyncIndexBuilder::new(1024);
        for i in 0..10000 {
            builder.record_batch(i * 10, i as u64 * 100, 100);
        }
        builder.build(&index_path)
    };

    group.bench_function("lookup", |b| {
        let mut offset = 0i64;
        b.iter(|| {
            let result = index.lookup(offset);
            offset = (offset + 73) % 100000; // Pseudo-random access
            black_box(result)
        })
    });

    // Benchmark save/load cycle
    group.bench_function("save_load_1000", |b| {
        let dir = tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        let mut index = AsyncSegmentIndex::new(&index_path);
        for i in 0..1000 {
            index.add_entry(i * 10, i as u64 * 100);
        }

        b.iter(|| {
            run_async(async {
                let fs = get_standard_backend();
                index.save(&fs).await.unwrap();
                let loaded = AsyncSegmentIndex::load(&fs, &index_path).await.unwrap();
                black_box(loaded)
            })
        })
    });

    group.finish();
}

// Conditional io_uring benchmarks (Linux only)
#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod uring_benchmarks {
    use super::*;
    use streamline::storage::{get_uring_backend, is_uring_available};

    pub fn bench_uring_vs_standard(c: &mut Criterion) {
        if !is_uring_available() {
            eprintln!("io_uring not available, skipping uring benchmarks");
            return;
        }

        let mut group = c.benchmark_group("uring_vs_standard");

        for size in [4096, 65536, 262144].iter() {
            let dir = tempdir().unwrap();
            let file_path = dir.path().join("test.dat");

            group.throughput(Throughput::Bytes(*size as u64));

            // Standard backend write
            group.bench_with_input(BenchmarkId::new("standard_write", size), size, |b, &sz| {
                b.iter(|| {
                    run_async(async {
                        let fs = get_standard_backend();
                        let file = fs.create(&file_path).await.unwrap();
                        let data = vec![0xABu8; sz];
                        let (result, _) = file.write_at(black_box(data), 0).await;
                        result.unwrap();
                        file.sync_data().await.unwrap();
                    })
                })
            });

            // io_uring backend write
            group.bench_with_input(BenchmarkId::new("uring_write", size), size, |b, &sz| {
                b.iter(|| {
                    run_async(async {
                        let fs = get_uring_backend();
                        let file = fs.create(&file_path).await.unwrap();
                        let data = vec![0xABu8; sz];
                        let (result, _) = file.write_at(black_box(data), 0).await;
                        result.unwrap();
                        file.sync_data().await.unwrap();
                    })
                })
            });
        }

        group.finish();
    }
}

criterion_group!(
    benches,
    bench_write_sizes,
    bench_read_sizes,
    bench_random_reads,
    bench_append,
    bench_sync,
    bench_buffer_pool,
    bench_concurrent_writes,
    bench_async_segment,
    bench_async_wal,
    bench_async_index,
);

#[cfg(all(target_os = "linux", feature = "io-uring"))]
criterion_group!(uring_benches, uring_benchmarks::bench_uring_vs_standard,);

#[cfg(all(target_os = "linux", feature = "io-uring"))]
criterion_main!(benches, uring_benches);

#[cfg(not(all(target_os = "linux", feature = "io-uring")))]
criterion_main!(benches);
