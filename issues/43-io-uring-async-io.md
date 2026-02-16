# Implement io_uring for Async Disk I/O

## Summary

Implement io_uring-based async disk I/O to replace the current synchronous file operations. This enables true kernel-level async I/O for WAL writes and segment reads, improving latency and throughput while aligning with the PRD's single-threaded + async I/O architecture.

Referenced in PRD at `streamline-prd.md:255-256`, `streamline-prd.md:282-283`, `streamline-prd.md:1198`, `streamline-prd.md:1213`.

## Current State

Streamline uses **synchronous blocking I/O** throughout the storage layer:

- `src/storage/segment.rs:12-14` - Uses `std::fs::{File, OpenOptions}`, `BufReader`, `BufWriter`
- `src/storage/wal.rs:72-73` - Uses synchronous `std::fs` for WAL writes
- `src/storage/zerocopy.rs:34` - Uses `std::fs::File` with `pread` via `FileExt`
- `src/storage/index.rs` - Synchronous index file operations
- `Cargo.toml:29` - Only standard tokio, no tokio-uring

### Problems with Current Implementation

1. **Blocking I/O in async context**: File operations block the tokio runtime thread
2. **Thread pool overhead**: `tokio::fs` uses spawn_blocking internally, adding latency
3. **Syscall overhead**: Each I/O operation requires separate syscalls
4. **Not aligned with PRD**: PRD explicitly calls for io_uring (`streamline-prd.md:282`)

## Requirements

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     STREAMLINE SERVER                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Main Thread (tokio runtime)                 │   │
│  │                                                          │   │
│  │   Network I/O ◄──► Protocol Handler ◄──► Storage API    │   │
│  │     (epoll)                                │              │   │
│  └────────────────────────────────────────────┼──────────────┘   │
│                                               │                  │
│                                               ▼                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │           I/O Abstraction Layer                          │   │
│  │                                                          │   │
│  │   IoBackend::read()  IoBackend::write()  IoBackend::sync│   │
│  └────────────────────────────────────────────┬─────────────┘   │
│                                               │                  │
│              ┌────────────────────────────────┴───────┐         │
│              │                                        │         │
│              ▼                                        ▼         │
│  ┌───────────────────────┐            ┌───────────────────────┐│
│  │  Standard Backend     │            │  io_uring Backend     ││
│  │  (macOS, Windows,     │            │  (Linux 5.11+)        ││
│  │   old Linux)          │            │                       ││
│  │                       │            │  ┌─────────────────┐  ││
│  │  spawn_blocking +     │            │  │ tokio-uring     │  ││
│  │  std::fs              │            │  │ runtime         │  ││
│  └───────────────────────┘            │  └─────────────────┘  ││
│                                       └───────────────────────┘│
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### I/O Abstraction Trait

```rust
// src/storage/io_backend/mod.rs

use bytes::Bytes;
use std::path::Path;

/// Result type for I/O operations with buffer return
pub type IoResult<T> = (crate::error::Result<T>, Vec<u8>);

/// Async file operations trait
#[async_trait::async_trait]
pub trait AsyncFile: Send + Sync {
    /// Read bytes at a specific offset
    ///
    /// The buffer is moved to the kernel and returned after the operation.
    /// This ownership model is required for io_uring compatibility.
    async fn read_at(&self, buf: Vec<u8>, offset: u64) -> IoResult<usize>;

    /// Write bytes at a specific offset
    async fn write_at(&self, buf: Vec<u8>, offset: u64) -> IoResult<usize>;

    /// Append bytes to the end of the file
    async fn append(&self, buf: Vec<u8>) -> IoResult<usize>;

    /// Sync file data to disk (fdatasync)
    async fn sync_data(&self) -> crate::error::Result<()>;

    /// Sync file data and metadata to disk (fsync)
    async fn sync_all(&self) -> crate::error::Result<()>;

    /// Get the current file size
    async fn size(&self) -> crate::error::Result<u64>;

    /// Pre-allocate space for the file (fallocate)
    async fn allocate(&self, len: u64) -> crate::error::Result<()>;
}

/// File opener trait
#[async_trait::async_trait]
pub trait AsyncFileSystem: Send + Sync {
    type File: AsyncFile;

    /// Open a file for reading
    async fn open(&self, path: &Path) -> crate::error::Result<Self::File>;

    /// Open a file for reading and writing
    async fn open_rw(&self, path: &Path) -> crate::error::Result<Self::File>;

    /// Create a new file (truncate if exists)
    async fn create(&self, path: &Path) -> crate::error::Result<Self::File>;

    /// Open or create a file for appending
    async fn open_append(&self, path: &Path) -> crate::error::Result<Self::File>;
}
```

### Buffer Ownership Model

io_uring requires ownership transfer of buffers to the kernel:

```rust
// Current pattern (borrowed buffer)
let mut buf = [0u8; 4096];
file.read(&mut buf)?;  // Buffer borrowed

// io_uring pattern (owned buffer)
let buf = vec![0u8; 4096];
let (result, buf) = file.read_at(buf, 0).await;  // Buffer moved, then returned
let bytes_read = result?;
```

This ownership model is reflected in the trait design to ensure compatibility with both backends.

### Buffer Pool for Efficiency

```rust
// src/storage/io_backend/buffer_pool.rs

/// Pre-allocated buffer pool to reduce allocation overhead
pub struct IoBufferPool {
    /// Available buffers by size class
    pools: DashMap<usize, Vec<Vec<u8>>>,
    /// Size classes (e.g., 4KB, 64KB, 1MB)
    size_classes: Vec<usize>,
    /// Maximum buffers per size class
    max_per_class: usize,
}

impl IoBufferPool {
    /// Get a buffer of at least the specified size
    pub fn acquire(&self, min_size: usize) -> Vec<u8> {
        let size_class = self.round_up_to_class(min_size);

        if let Some(mut pool) = self.pools.get_mut(&size_class) {
            if let Some(buf) = pool.pop() {
                return buf;
            }
        }

        vec![0u8; size_class]
    }

    /// Return a buffer to the pool
    pub fn release(&self, mut buf: Vec<u8>) {
        let size_class = self.round_down_to_class(buf.capacity());

        if let Some(mut pool) = self.pools.get_mut(&size_class) {
            if pool.len() < self.max_per_class {
                buf.clear();
                pool.push(buf);
            }
        }
    }
}
```

### Standard Backend Implementation

```rust
// src/storage/io_backend/standard.rs

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct StandardFile {
    file: Arc<Mutex<File>>,
    path: PathBuf,
}

#[async_trait::async_trait]
impl AsyncFile for StandardFile {
    async fn read_at(&self, mut buf: Vec<u8>, offset: u64) -> IoResult<usize> {
        let file = self.file.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut file = file.blocking_lock();
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                file.read_at(&mut buf, offset).map(|n| (n, buf))
            }
            #[cfg(not(unix))]
            {
                file.seek(SeekFrom::Start(offset))?;
                let n = file.read(&mut buf)?;
                Ok((n, buf))
            }
        })
        .await
        .unwrap();

        match result {
            Ok((n, buf)) => (Ok(n), buf),
            Err(e) => (Err(e.into()), buf),
        }
    }

    async fn write_at(&self, buf: Vec<u8>, offset: u64) -> IoResult<usize> {
        let file = self.file.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut file = file.blocking_lock();
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                file.write_at(&buf, offset).map(|n| (n, buf))
            }
            #[cfg(not(unix))]
            {
                file.seek(SeekFrom::Start(offset))?;
                let n = file.write(&buf)?;
                Ok((n, buf))
            }
        })
        .await
        .unwrap();

        match result {
            Ok((n, buf)) => (Ok(n), buf),
            Err(e) => (Err(e.into()), buf),
        }
    }

    async fn sync_data(&self) -> crate::error::Result<()> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || {
            file.blocking_lock().sync_data()
        })
        .await
        .unwrap()?;
        Ok(())
    }

    async fn sync_all(&self) -> crate::error::Result<()> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || {
            file.blocking_lock().sync_all()
        })
        .await
        .unwrap()?;
        Ok(())
    }
}
```

### io_uring Backend Implementation

```rust
// src/storage/io_backend/uring.rs

#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod uring_impl {
    use tokio_uring::fs::File;

    pub struct UringFile {
        file: File,
    }

    impl UringFile {
        pub async fn open(path: &Path) -> crate::error::Result<Self> {
            let file = File::open(path).await?;
            Ok(Self { file })
        }

        pub async fn create(path: &Path) -> crate::error::Result<Self> {
            let file = File::create(path).await?;
            Ok(Self { file })
        }
    }

    #[async_trait::async_trait]
    impl AsyncFile for UringFile {
        async fn read_at(&self, buf: Vec<u8>, offset: u64) -> IoResult<usize> {
            let (result, buf) = self.file.read_at(buf, offset).await;
            match result {
                Ok(n) => (Ok(n), buf),
                Err(e) => (Err(e.into()), buf),
            }
        }

        async fn write_at(&self, buf: Vec<u8>, offset: u64) -> IoResult<usize> {
            let (result, buf) = self.file.write_at(buf, offset).await;
            match result {
                Ok(n) => (Ok(n), buf),
                Err(e) => (Err(e.into()), buf),
            }
        }

        async fn sync_data(&self) -> crate::error::Result<()> {
            self.file.sync_data().await?;
            Ok(())
        }

        async fn sync_all(&self) -> crate::error::Result<()> {
            self.file.sync_all().await?;
            Ok(())
        }
    }
}
```

### I/O Worker Thread Model

Since tokio-uring requires its own runtime (and is `!Sync`), use a dedicated worker thread:

```rust
// src/storage/io_backend/worker.rs

use tokio::sync::{mpsc, oneshot};

pub enum IoRequest {
    Read {
        file_id: FileId,
        offset: u64,
        len: usize,
        response: oneshot::Sender<IoResult<Bytes>>,
    },
    Write {
        file_id: FileId,
        offset: u64,
        data: Bytes,
        response: oneshot::Sender<IoResult<usize>>,
    },
    Sync {
        file_id: FileId,
        response: oneshot::Sender<crate::error::Result<()>>,
    },
    OpenFile {
        path: PathBuf,
        mode: OpenMode,
        response: oneshot::Sender<crate::error::Result<FileId>>,
    },
    CloseFile {
        file_id: FileId,
    },
    Shutdown,
}

pub struct IoWorker {
    tx: mpsc::Sender<IoRequest>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl IoWorker {
    /// Start the io_uring worker thread
    pub fn start(config: IoConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.queue_depth);

        let handle = std::thread::Builder::new()
            .name("streamline-io".to_string())
            .spawn(move || {
                tokio_uring::start(async move {
                    Self::run_loop(rx, config).await;
                });
            })
            .expect("Failed to spawn I/O worker thread");

        Self {
            tx,
            handle: Some(handle),
        }
    }

    async fn run_loop(mut rx: mpsc::Receiver<IoRequest>, config: IoConfig) {
        let mut files: HashMap<FileId, UringFile> = HashMap::new();
        let mut next_id = FileId(0);
        let buffer_pool = IoBufferPool::new(config.buffer_pool_size);

        while let Some(request) = rx.recv().await {
            match request {
                IoRequest::Read { file_id, offset, len, response } => {
                    if let Some(file) = files.get(&file_id) {
                        let buf = buffer_pool.acquire(len);
                        let (result, buf) = file.read_at(buf, offset).await;
                        buffer_pool.release(buf);
                        let _ = response.send(result.map(|n| Bytes::from(&buf[..n])));
                    }
                }
                IoRequest::Write { file_id, offset, data, response } => {
                    if let Some(file) = files.get(&file_id) {
                        let (result, _) = file.write_at(data.to_vec(), offset).await;
                        let _ = response.send(result);
                    }
                }
                IoRequest::Shutdown => break,
                // ... other handlers
            }
        }
    }

    /// Send a read request
    pub async fn read(&self, file_id: FileId, offset: u64, len: usize) -> IoResult<Bytes> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(IoRequest::Read { file_id, offset, len, response: tx }).await?;
        rx.await?
    }

    /// Shutdown the worker
    pub async fn shutdown(&mut self) {
        let _ = self.tx.send(IoRequest::Shutdown).await;
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
```

### Configuration

```rust
// src/config/mod.rs

/// I/O backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoConfig {
    /// Enable io_uring backend on Linux (default: true)
    #[serde(default = "default_enable_io_uring")]
    pub enable_io_uring: bool,

    /// io_uring submission queue depth (default: 256)
    #[serde(default = "default_sq_depth")]
    pub sq_depth: u32,

    /// Request queue depth for I/O worker (default: 1024)
    #[serde(default = "default_queue_depth")]
    pub queue_depth: usize,

    /// Number of I/O worker threads (default: 1)
    /// Note: Each worker has its own io_uring instance
    #[serde(default = "default_io_workers")]
    pub io_workers: usize,

    /// Buffer pool size per size class (default: 64)
    #[serde(default = "default_buffer_pool_size")]
    pub buffer_pool_size: usize,

    /// Pre-allocate WAL files (fallocate) for better performance
    #[serde(default = "default_preallocate")]
    pub preallocate_wal: bool,

    /// Pre-allocation size for WAL files (default: 64MB)
    #[serde(default = "default_preallocate_size")]
    pub preallocate_size: u64,
}

fn default_enable_io_uring() -> bool { true }
fn default_sq_depth() -> u32 { 256 }
fn default_queue_depth() -> usize { 1024 }
fn default_io_workers() -> usize { 1 }
fn default_buffer_pool_size() -> usize { 64 }
fn default_preallocate() -> bool { true }
fn default_preallocate_size() -> u64 { 64 * 1024 * 1024 }
```

CLI arguments:
```bash
streamline \
  --enable-io-uring true \
  --io-uring-sq-depth 256 \
  --io-workers 2 \
  --io-buffer-pool-size 128
```

## Implementation Tasks

### Phase 1: I/O Abstraction Layer

1. Create `src/storage/io_backend/mod.rs`:
   - Define `AsyncFile` trait
   - Define `AsyncFileSystem` trait
   - Export appropriate backend based on features

2. Create `src/storage/io_backend/types.rs`:
   - `FileId` wrapper type
   - `OpenMode` enum
   - `IoResult` type alias

3. Create `src/storage/io_backend/buffer_pool.rs`:
   - `IoBufferPool` implementation
   - Size class management
   - Acquire/release operations

### Phase 2: Standard Backend

1. Create `src/storage/io_backend/standard.rs`:
   - `StandardFile` implementation
   - `StandardFileSystem` implementation
   - Uses `spawn_blocking` for I/O operations

2. Add unit tests for standard backend

### Phase 3: io_uring Backend

1. Update `Cargo.toml`:
   ```toml
   [target.'cfg(target_os = "linux")'.dependencies]
   tokio-uring = { version = "0.5", optional = true }

   [features]
   io-uring = ["tokio-uring"]
   ```

2. Create `src/storage/io_backend/uring.rs`:
   - `UringFile` implementation
   - Kernel version detection
   - Graceful fallback if io_uring unavailable

3. Create `src/storage/io_backend/worker.rs`:
   - `IoWorker` thread management
   - Request/response channel handling
   - File handle management

### Phase 4: Integration with Storage Layer

1. Update `src/storage/segment.rs`:
   - Replace `std::fs::File` with `AsyncFile`
   - Update read/write methods for ownership model
   - Add async versions of key methods

2. Update `src/storage/wal.rs`:
   - Replace `BufWriter<File>` with `AsyncFile`
   - Update `WalWriter` for async operations
   - Maintain sync mode semantics

3. Update `src/storage/zerocopy.rs`:
   - Integrate with io_uring for true zero-copy
   - Update `ZeroCopyReader` to use backend

4. Update `src/storage/index.rs`:
   - Convert to async file operations

### Phase 5: Runtime Integration

1. Update `src/server/mod.rs`:
   - Start I/O worker(s) on server startup
   - Graceful shutdown of workers
   - Pass worker handle to storage components

2. Update `src/config/mod.rs`:
   - Add `IoConfig` structure
   - CLI argument parsing
   - Environment variable support

### Phase 6: Benchmarking and Optimization

1. Create `benches/io_benchmarks.rs`:
   - WAL write latency comparison
   - Segment read throughput comparison
   - Standard vs io_uring comparison

2. Add metrics for I/O operations:
   - Latency histograms
   - Throughput counters
   - Queue depth gauges

## Acceptance Criteria

### Functionality
- [ ] I/O abstraction trait works with both backends
- [ ] Standard backend works on all platforms
- [ ] io_uring backend works on Linux 5.11+
- [ ] Graceful fallback on unsupported kernels
- [ ] Feature flag `io-uring` controls backend selection
- [ ] I/O worker thread starts and stops correctly

### Performance
- [ ] WAL write latency reduced by 30%+ with io_uring
- [ ] Segment read throughput improved by 20%+ with io_uring
- [ ] No regression on standard backend
- [ ] Buffer pool reduces allocation overhead

### Compatibility
- [ ] macOS builds and runs with standard backend
- [ ] Windows builds and runs with standard backend
- [ ] Linux <5.11 falls back to standard backend
- [ ] Existing tests pass with both backends

### Operations
- [ ] `--enable-io-uring` flag documented
- [ ] Kernel version logged on startup
- [ ] Metrics exported for I/O latency
- [ ] Graceful degradation on io_uring errors

## Testing

### Unit Tests
- Buffer pool allocation/release
- File trait implementations
- Ownership transfer semantics
- Error handling

### Integration Tests
- WAL write/read cycle with io_uring
- Segment operations with io_uring
- Mixed workload (reads + writes)
- Crash recovery with io_uring

### Performance Tests
- `benches/io_benchmarks.rs`:
  - Sequential write throughput
  - Random read latency
  - Fsync latency
  - Comparison: standard vs io_uring

### Platform Tests
- Linux 5.11+ with io_uring feature
- Linux <5.11 fallback
- macOS standard backend
- CI matrix for all platforms

## Risk Assessment

### Breaking Changes
- **None** - Feature-gated, additive change
- Existing code paths unchanged when feature disabled

### Performance Risks
- **Low** - io_uring is proven technology
- Worst case: falls back to standard I/O

### Complexity
- **Medium** - New abstraction layer
- Isolated to storage module

### Platform Considerations

| Platform | io_uring Support | Backend Used |
|----------|------------------|--------------|
| Linux 5.11+ | ✅ Full | io_uring |
| Linux 5.1-5.10 | ⚠️ Partial | Standard (fallback) |
| Linux <5.1 | ❌ None | Standard |
| macOS | ❌ N/A | Standard |
| Windows | ❌ N/A | Standard |

## Related Files

### New Files to Create
| File | Purpose |
|------|---------|
| `src/storage/io_backend/mod.rs` | Module organization and trait definitions |
| `src/storage/io_backend/types.rs` | Common types (FileId, OpenMode, etc.) |
| `src/storage/io_backend/buffer_pool.rs` | Buffer pool for reducing allocations |
| `src/storage/io_backend/standard.rs` | Standard I/O backend |
| `src/storage/io_backend/uring.rs` | io_uring backend (Linux only) |
| `src/storage/io_backend/worker.rs` | I/O worker thread management |
| `benches/io_benchmarks.rs` | Performance benchmarks |

### Existing Files to Modify
| File | Changes |
|------|---------|
| `Cargo.toml` | Add `tokio-uring` dependency with feature flag |
| `src/storage/mod.rs` | Export `io_backend` module |
| `src/storage/segment.rs` | Use `AsyncFile` trait |
| `src/storage/wal.rs` | Use `AsyncFile` trait |
| `src/storage/zerocopy.rs` | Integrate with io_uring |
| `src/storage/index.rs` | Use `AsyncFile` trait |
| `src/config/mod.rs` | Add `IoConfig` |
| `src/server/mod.rs` | Start/stop I/O workers |

## Future Enhancements

### Phase 2 Opportunities
- **Registered buffers**: Pre-register buffers with kernel for even lower overhead
- **Fixed files**: Register file descriptors to avoid fd lookup overhead
- **Linked operations**: Chain dependent I/O operations
- **Multishot reads**: Continuous reads for streaming workloads

### Network I/O
- Investigate io_uring for network operations (lower priority)
- Current epoll/kqueue is performant for network I/O

## References

- [Efficient IO with io_uring (Kernel Documentation)](https://kernel.dk/io_uring.pdf)
- [tokio-uring GitHub Repository](https://github.com/tokio-rs/tokio-uring)
- [tokio-uring Documentation](https://docs.rs/tokio-uring/)
- [Announcing tokio-uring (Tokio Blog)](https://tokio.rs/blog/2021-07-tokio-uring)
- [Linux io_uring and tokio-uring exploration](https://developerlife.com/2024/05/25/tokio-uring-exploration-rust/)
- [io_uring in Qdrant](https://qdrant.tech/articles/io_uring/)
- [Performance: Tokio vs Tokio-Uring (Dec 2024)](https://shbhmrzd.github.io/2024/12/19/async_rt_benchmark.html)

## Labels

`enhancement`, `performance`, `storage`, `linux`, `medium`, `phase-2`
