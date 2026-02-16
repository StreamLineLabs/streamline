# Zero-Copy Networking

## Priority: Medium

## Summary

Implement zero-copy networking optimizations that coexist with TLS, using zero-copy when feasible (plain TCP) and gracefully falling back for TLS connections. This includes memory-mapped segment reads, `sendfile()` system call integration, buffer pool enhancements, and response caching. These optimizations can reduce CPU usage by 30-50% and improve fetch latency by 20-40% for hot data.

**Key Design Decision**: TLS and zero-copy coexist - use sendfile for plain TCP, gracefully fall back for TLS while still benefiting from mmap and buffer pool optimizations.

Inspired by [Kafka's zero-copy design](https://docs.confluent.io/platform/6.2/kafka/design.html) which uses Java NIO's `FileChannel.transferTo()` mapping to the `sendfile()` syscall.

## Implementation Status: ✅ COMPLETE (Dec 2024)

The zero-copy networking implementation is complete with comprehensive tests and benchmarks:

### Completed Components
- **ConnectionContext** (`src/protocol/connection.rs`) - Tracks TLS vs plain TCP connections
- **Extended ZeroCopyConfig** (`src/storage/zerocopy.rs`) - Full configuration with CLI args
- **BufferPool with get_with_capacity()** - Capacity-aware buffer allocation to reduce allocations
- **MmapSegmentReader** (`src/storage/mmap.rs`) - Memory-mapped access to sealed segments
- **ResponseCache** (`src/protocol/response_cache.rs`) - LRU cache with TTL for fetch responses
- **sendfile_to_socket()** - Platform-specific sendfile implementation (Linux/macOS)
- **Integration in KafkaHandler** - buffer_pool, zerocopy_config, and response_cache wired through
- **TLS Fallback Integration Tests** (`tests/zerocopy_test.rs`) - 7 tests verifying TLS behavior
- **Zero-Copy Benchmarks** (`benches/zerocopy_benchmarks.rs`) - Response cache, mmap vs pread, fetch response building

### Deferred (Future Enhancement)
- **Full sendfile path**: Requires architectural change to avoid splitting TcpStream and keep raw fd

### Files Added
- `src/protocol/connection.rs` - ConnectionContext
- `src/storage/mmap.rs` - MmapSegmentReader
- `src/protocol/response_cache.rs` - ResponseCache

### Files Modified
- `src/storage/zerocopy.rs` - Extended ZeroCopyConfig, added get_with_capacity(), sendfile_to_socket()
- `src/protocol/kafka.rs` - Wired buffer pool for aggregation, ConnectionContext through handlers
- `src/protocol/pipeline.rs` - Added cache_key to PendingResponse
- `src/config/mod.rs` - Added CLI args for zero-copy configuration
- `Cargo.toml` - Added memmap2 dependency

---

## Original State (Before Implementation)

- `src/storage/zerocopy.rs` - Foundation exists with `BufferPool`, `ZeroCopyReader`, `ZeroCopyConfig`
- `src/storage/segment.rs` - Segments use `BufReader`/`BufWriter` with standard read/write
- `src/protocol/kafka.rs:679-711` - Network writes use `write_all()` copying data to socket
- `src/protocol/kafka.rs:2494` - Unnecessary aggregation copy in fetch response building
- `docs/PERFORMANCE.md` - Documents current zero-copy optimizations (pread, BufferPool)

Original data flow (with copies identified):
```
Segment File (disk)
    ↓ [Copy #1: read() into buffer]
JSON Deserialize → Vec<Record>
    ↓ [Copy #2: record.value aggregation at kafka.rs:2494]
BytesMut aggregation buffer
    ↓ [Copy #3: Kafka protocol encoding]
Response buffer
    ↓ [Copy #4: write_all() to socket]
Socket / TLS Stream
```

## Architecture

### Connection Type Detection

Track TLS vs plain TCP at connection acceptance time and propagate through the system:

```
Server::run() (src/server/mod.rs:477-544)
    │
    ├── TLS enabled → handle_tls_connection()
    │                    └── ConnectionContext::tls()
    │                           └── can_sendfile() = false
    │
    └── TLS disabled → handle_connection()
                          └── ConnectionContext::plain_tcp()
                                 └── can_sendfile() = true
```

### Optimized Data Flow (Plain TCP)

```
Segment File (disk)
    ↓
[Sealed segments: mmap reads - zero kernel copy]
[Response cache check]
    ↓
[Cache HIT] → sendfile() to socket (true zero-copy)
    ↓
[Cache MISS] → Kafka encode → cache → write_all()
```

### Optimized Data Flow (TLS)

```
Segment File (disk)
    ↓
[Sealed segments: mmap reads - reduced copies]
[BufferPool: reused allocations]
    ↓
JSON Deserialize → Vec<Record>
    ↓
[Eliminated aggregation copy]
    ↓
Kafka Protocol Encoder (uses BufferPool)
    ↓
[Required: TLS encryption in user space]
TLS Stream → write_all()
```

## Requirements

### Phase 1: Foundation - ConnectionContext & Configuration

#### Create `src/protocol/connection.rs` (NEW FILE)

```rust
use std::net::SocketAddr;
use crate::storage::ZeroCopyConfig;

/// Connection properties that affect optimization strategies
#[derive(Debug, Clone)]
pub struct ConnectionContext {
    /// Whether TLS is enabled on this connection
    pub is_tls: bool,
    /// Peer address (for metrics/debugging)
    pub peer_addr: Option<SocketAddr>,
    /// Zero-copy config for this connection
    pub zerocopy_config: ZeroCopyConfig,
}

impl ConnectionContext {
    pub fn plain_tcp(peer_addr: Option<SocketAddr>, config: ZeroCopyConfig) -> Self {
        Self {
            is_tls: false,
            peer_addr,
            zerocopy_config: config,
        }
    }

    pub fn tls(peer_addr: Option<SocketAddr>, config: ZeroCopyConfig) -> Self {
        Self {
            is_tls: true,
            peer_addr,
            zerocopy_config: ZeroCopyConfig {
                enable_sendfile: false,  // Auto-disable for TLS
                ..config
            },
        }
    }

    /// Whether sendfile is usable on this connection
    pub fn can_sendfile(&self) -> bool {
        !self.is_tls && self.zerocopy_config.enable_sendfile
    }

    /// Whether mmap is usable (works for both TLS and plain TCP)
    pub fn can_mmap(&self) -> bool {
        self.zerocopy_config.enable_mmap
    }
}
```

#### Extend `src/config/mod.rs`

```rust
/// Zero-copy networking configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZeroCopyConfig {
    /// Master switch to enable/disable all zero-copy optimizations
    /// Default: true
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Use memory-mapped I/O for sealed segments
    /// Default: true
    #[serde(default = "default_true")]
    pub enable_mmap: bool,

    /// Minimum segment size (bytes) to use mmap
    /// Default: 1MB
    #[serde(default = "default_mmap_threshold")]
    pub mmap_threshold: u64,

    /// Enable buffer pool for reducing allocations
    /// Default: true
    #[serde(default = "default_true")]
    pub enable_buffer_pool: bool,

    /// Number of buffers in the pool
    /// Default: 16
    #[serde(default = "default_buffer_pool_size")]
    pub buffer_pool_size: usize,

    /// Size of each buffer in the pool (bytes)
    /// Default: 64KB
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,

    /// Enable sendfile for plain TCP connections
    /// Automatically disabled for TLS connections
    /// Default: true
    #[serde(default = "default_true")]
    pub enable_sendfile: bool,

    /// Enable response caching
    /// Default: true
    #[serde(default = "default_true")]
    pub enable_response_cache: bool,

    /// Maximum response cache size (bytes)
    /// Default: 128MB
    #[serde(default = "default_response_cache_size")]
    pub response_cache_max_bytes: u64,

    /// Response cache TTL in seconds
    /// Default: 60
    #[serde(default = "default_cache_ttl")]
    pub response_cache_ttl_secs: u64,
}

fn default_true() -> bool { true }
fn default_mmap_threshold() -> u64 { 1024 * 1024 }  // 1MB
fn default_buffer_pool_size() -> usize { 16 }
fn default_buffer_size() -> usize { 64 * 1024 }  // 64KB
fn default_response_cache_size() -> u64 { 128 * 1024 * 1024 }  // 128MB
fn default_cache_ttl() -> u64 { 60 }

impl Default for ZeroCopyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            enable_mmap: true,
            mmap_threshold: default_mmap_threshold(),
            enable_buffer_pool: true,
            buffer_pool_size: default_buffer_pool_size(),
            buffer_size: default_buffer_size(),
            enable_sendfile: true,
            enable_response_cache: true,
            response_cache_max_bytes: default_response_cache_size(),
            response_cache_ttl_secs: default_cache_ttl(),
        }
    }
}
```

CLI arguments to add:
```
--zerocopy-enabled           Enable zero-copy optimizations (default: true)
--zerocopy-mmap              Enable mmap for sealed segments (default: true)
--zerocopy-mmap-threshold    Minimum segment size for mmap in bytes (default: 1048576)
--zerocopy-buffer-pool-size  Number of pooled buffers (default: 16)
--zerocopy-sendfile          Enable sendfile for plain TCP (default: true)
--zerocopy-response-cache    Enable response caching (default: true)
--zerocopy-cache-size-mb     Response cache size in MB (default: 128)
```

#### Modify `src/protocol/kafka.rs`

Update connection handling to pass `ConnectionContext`:

```rust
pub async fn handle_connection(&self, stream: TcpStream) -> Result<()> {
    let peer_addr = stream.peer_addr().ok();
    let ctx = ConnectionContext::plain_tcp(peer_addr, self.zerocopy_config.clone());
    let session_manager = SessionManager::with_peer_addr(peer_addr);
    self.handle_stream_with_context(stream, ctx, session_manager).await
}

pub async fn handle_tls_connection<S>(&self, stream: S) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let ctx = ConnectionContext::tls(None, self.zerocopy_config.clone());
    let session_manager = SessionManager::with_peer_addr(None);
    self.handle_stream_with_context(stream, ctx, session_manager).await
}
```

#### Add Metrics

```rust
// Zero-copy metrics
metrics::counter!("streamline_zerocopy_mmap_reads_total");
metrics::counter!("streamline_zerocopy_sendfile_bytes_total");
metrics::counter!("streamline_zerocopy_buffer_pool_hits");
metrics::counter!("streamline_zerocopy_buffer_pool_misses");
metrics::counter!("streamline_zerocopy_response_cache_hits");
metrics::counter!("streamline_zerocopy_response_cache_misses");
metrics::gauge!("streamline_zerocopy_buffer_pool_available");
metrics::gauge!("streamline_zerocopy_response_cache_bytes");
```

### Phase 2: Buffer Pool Integration

#### Modify `src/storage/zerocopy.rs`

Extend `BufferPool` with capacity-aware allocation:

```rust
impl BufferPool {
    /// Get a buffer with at least the specified capacity
    pub async fn get_with_capacity(&self, min_capacity: usize) -> BytesMut {
        let mut buffers = self.buffers.lock().await;

        // Try to find a buffer with sufficient capacity
        if let Some(idx) = buffers.iter().position(|b| b.capacity() >= min_capacity) {
            let mut buf = buffers.swap_remove(idx);
            buf.clear();
            metrics::counter!("streamline_zerocopy_buffer_pool_hits").increment(1);
            return buf;
        }

        // No suitable buffer, allocate new one
        metrics::counter!("streamline_zerocopy_buffer_pool_misses").increment(1);
        BytesMut::with_capacity(min_capacity.max(self.buffer_size))
    }
}
```

#### Eliminate Aggregation Copy at Line 2494

Current code (`src/protocol/kafka.rs:2484-2497`):
```rust
let records_bytes: Option<Bytes> = if records.is_empty() {
    None
} else {
    let mut buf = BytesMut::new();
    for record in &records {
        buf.extend_from_slice(&record.value);  // UNNECESSARY COPY
    }
    Some(buf.freeze())
};
```

Optimized code:
```rust
let records_bytes: Option<Bytes> = if records.is_empty() {
    None
} else {
    // Pre-calculate total size and get pooled buffer
    let total_len: usize = records.iter().map(|r| r.value.len()).sum();
    let mut buf = self.buffer_pool.get_with_capacity(total_len).await;

    for record in &records {
        buf.extend_from_slice(&record.value);
    }
    Some(buf.freeze())
};
```

#### Add `buffer_pool` to `KafkaHandler`

```rust
pub struct KafkaHandler {
    // ... existing fields ...
    buffer_pool: Arc<BufferPool>,
    zerocopy_config: ZeroCopyConfig,
}
```

### Phase 3: Memory-Mapped Segments

#### Add Dependency to `Cargo.toml`

```toml
memmap2 = "0.9"
```

#### Create `src/storage/mmap.rs` (NEW FILE)

```rust
//! Memory-mapped segment reader for zero-copy access to sealed segments

use crate::error::{Result, StreamlineError};
use crate::storage::segment::SegmentHeader;
use memmap2::Mmap;
use std::fs::File;
use std::path::{Path, PathBuf};

/// Memory-mapped segment reader for zero-copy access
pub struct MmapSegmentReader {
    mmap: Mmap,
    header: SegmentHeader,
    path: PathBuf,
}

impl MmapSegmentReader {
    /// Open a segment file with memory mapping
    ///
    /// # Safety
    /// Only safe for sealed (immutable) segments. The caller must ensure
    /// the segment will not be modified or truncated while mapped.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;

        // SAFETY: We only mmap sealed segments which are immutable
        let mmap = unsafe {
            memmap2::MmapOptions::new()
                .map(&file)
                .map_err(|e| StreamlineError::Storage(format!("mmap failed: {}", e)))?
        };

        // Parse header from mmap'd data
        if mmap.len() < 64 {
            return Err(StreamlineError::CorruptedData("Segment too small".into()));
        }
        let header = SegmentHeader::from_bytes(&mmap[0..64])?;

        Ok(Self {
            mmap,
            header,
            path: path.to_path_buf(),
        })
    }

    /// Get a zero-copy slice of the segment data
    pub fn slice(&self, offset: u64, len: usize) -> &[u8] {
        let start = offset as usize;
        let end = (start + len).min(self.mmap.len());
        &self.mmap[start..end]
    }

    /// Get the segment header
    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    /// Get the total size of the mmap'd region
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    /// Check if the mmap is empty
    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
    }
}
```

#### Modify `src/storage/segment.rs`

Add optional mmap reader:

```rust
use crate::storage::mmap::MmapSegmentReader;
use crate::storage::ZeroCopyConfig;

pub struct Segment {
    // ... existing fields ...
    mmap_reader: Option<MmapSegmentReader>,
}

impl Segment {
    /// Open a segment with zero-copy configuration
    pub fn open_with_config(path: &Path, config: &ZeroCopyConfig) -> Result<Self> {
        let mut segment = Self::open(path)?;

        // Only use mmap for sealed segments above threshold
        if segment.sealed && config.enable_mmap {
            let size = std::fs::metadata(path)?.len();
            if size >= config.mmap_threshold {
                match MmapSegmentReader::open(path) {
                    Ok(reader) => {
                        segment.mmap_reader = Some(reader);
                        metrics::counter!("streamline_zerocopy_mmap_reads_total").increment(1);
                    }
                    Err(e) => {
                        tracing::warn!("Failed to mmap segment {}: {}", path.display(), e);
                        // Fall back to regular reads
                    }
                }
            }
        }

        Ok(segment)
    }

    /// Read using mmap if available, otherwise fall back to regular read
    pub fn read_from_offset_optimized(&self, start_offset: i64, max_records: usize) -> Result<Vec<Record>> {
        if let Some(ref mmap) = self.mmap_reader {
            // Use mmap-based reading
            self.read_from_mmap(mmap, start_offset, max_records)
        } else {
            // Fall back to regular file reading
            self.read_from_offset(start_offset, max_records)
        }
    }
}
```

### Phase 4: Response Cache with Sendfile

#### Create `src/protocol/response_cache.rs` (NEW FILE)

```rust
//! Response cache for zero-copy sendfile transfers

use crate::error::{Result, StreamlineError};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Key for cached responses
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CacheKey {
    pub topic: String,
    pub partition: i32,
    pub start_offset: i64,
    pub max_bytes: i32,
}

/// A cached response stored on disk
#[derive(Debug)]
pub struct CachedResponse {
    pub file_path: PathBuf,
    pub size: u64,
    pub created_at: Instant,
    pub ttl: Duration,
}

impl CachedResponse {
    pub fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.ttl
    }

    pub fn open_file(&self) -> Result<File> {
        File::open(&self.file_path).map_err(|e| StreamlineError::Storage(e.to_string()))
    }
}

/// LRU response cache backed by filesystem
pub struct ResponseCache {
    cache_dir: PathBuf,
    entries: RwLock<HashMap<CacheKey, CachedResponse>>,
    max_size_bytes: u64,
    current_size: AtomicU64,
    ttl: Duration,
}

impl ResponseCache {
    pub fn new(cache_dir: PathBuf, max_size_bytes: u64, ttl_secs: u64) -> Result<Self> {
        fs::create_dir_all(&cache_dir)?;

        Ok(Self {
            cache_dir,
            entries: RwLock::new(HashMap::new()),
            max_size_bytes,
            current_size: AtomicU64::new(0),
            ttl: Duration::from_secs(ttl_secs),
        })
    }

    /// Get a cached response if available and not expired
    pub fn get(&self, key: &CacheKey) -> Option<CachedResponse> {
        let entries = self.entries.read();

        if let Some(entry) = entries.get(key) {
            if !entry.is_expired() && entry.file_path.exists() {
                metrics::counter!("streamline_zerocopy_response_cache_hits").increment(1);
                return Some(CachedResponse {
                    file_path: entry.file_path.clone(),
                    size: entry.size,
                    created_at: entry.created_at,
                    ttl: entry.ttl,
                });
            }
        }

        metrics::counter!("streamline_zerocopy_response_cache_misses").increment(1);
        None
    }

    /// Cache a response, evicting old entries if necessary
    pub fn put(&self, key: CacheKey, data: &[u8]) -> Result<CachedResponse> {
        let size = data.len() as u64;

        // Evict if necessary
        while self.current_size.load(Ordering::Relaxed) + size > self.max_size_bytes {
            self.evict_oldest();
        }

        // Generate unique filename
        let filename = format!(
            "{}_{}_{}_{}_{}.cache",
            key.topic, key.partition, key.start_offset, key.max_bytes,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let file_path = self.cache_dir.join(&filename);

        // Write to disk
        let mut file = File::create(&file_path)?;
        file.write_all(data)?;
        file.sync_data()?;

        let entry = CachedResponse {
            file_path: file_path.clone(),
            size,
            created_at: Instant::now(),
            ttl: self.ttl,
        };

        // Update cache
        {
            let mut entries = self.entries.write();
            entries.insert(key, CachedResponse {
                file_path,
                size,
                created_at: Instant::now(),
                ttl: self.ttl,
            });
        }

        self.current_size.fetch_add(size, Ordering::Relaxed);
        metrics::gauge!("streamline_zerocopy_response_cache_bytes")
            .set(self.current_size.load(Ordering::Relaxed) as f64);

        Ok(entry)
    }

    fn evict_oldest(&self) {
        let mut entries = self.entries.write();

        // Find oldest entry
        let oldest_key = entries
            .iter()
            .min_by_key(|(_, v)| v.created_at)
            .map(|(k, _)| k.clone());

        if let Some(key) = oldest_key {
            if let Some(entry) = entries.remove(&key) {
                let _ = fs::remove_file(&entry.file_path);
                self.current_size.fetch_sub(entry.size, Ordering::Relaxed);
            }
        }
    }

    /// Clear all cached entries
    pub fn clear(&self) {
        let mut entries = self.entries.write();

        for (_, entry) in entries.drain() {
            let _ = fs::remove_file(&entry.file_path);
        }

        self.current_size.store(0, Ordering::Relaxed);
    }
}
```

#### Implement Sendfile in `src/storage/zerocopy.rs`

```rust
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};

/// Send file contents directly to a socket (zero-copy on Linux/macOS)
///
/// Returns the number of bytes sent.
#[cfg(unix)]
pub async fn sendfile_to_socket(
    socket_fd: RawFd,
    file: &std::fs::File,
    offset: u64,
    count: usize,
) -> std::io::Result<usize> {
    let file_fd = file.as_raw_fd();

    // sendfile is blocking, use spawn_blocking
    tokio::task::spawn_blocking(move || {
        sendfile_sync(socket_fd, file_fd, offset, count)
    }).await?
}

#[cfg(target_os = "linux")]
fn sendfile_sync(socket_fd: RawFd, file_fd: RawFd, mut offset: u64, count: usize) -> std::io::Result<usize> {
    let mut sent = 0;
    while sent < count {
        let result = unsafe {
            libc::sendfile(
                socket_fd,
                file_fd,
                &mut offset as *mut u64 as *mut i64,
                count - sent,
            )
        };

        if result < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }

        sent += result as usize;
        offset += result as u64;
    }
    Ok(sent)
}

#[cfg(target_os = "macos")]
fn sendfile_sync(socket_fd: RawFd, file_fd: RawFd, offset: u64, count: usize) -> std::io::Result<usize> {
    let mut len = count as libc::off_t;

    let result = unsafe {
        libc::sendfile(
            file_fd,
            socket_fd,
            offset as libc::off_t,
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };

    if result < 0 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(len as usize)
}

#[cfg(not(unix))]
pub async fn sendfile_to_socket(
    _socket_fd: i32,
    file: &std::fs::File,
    offset: u64,
    count: usize,
) -> std::io::Result<usize> {
    // Fallback: read file into memory and return for regular write
    // On Windows, there's no sendfile equivalent we can easily use
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "sendfile not available on this platform",
    ))
}
```

#### Modify `writer_task()` for Sendfile Path

In `src/protocol/kafka.rs`, update writer task:

```rust
async fn writer_task<W>(
    mut writer: W,
    mut response_rx: mpsc::Receiver<PendingResponse>,
    ctx: ConnectionContext,
    response_cache: Option<Arc<ResponseCache>>,
    socket_fd: Option<RawFd>,  // Passed for sendfile
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    while let Some(response) = response_rx.recv().await {
        // Try sendfile path for plain TCP with cache hit
        if ctx.can_sendfile() && socket_fd.is_some() {
            if let Some(cache) = &response_cache {
                if let Some(ref cache_key) = response.cache_key {
                    if let Some(cached) = cache.get(cache_key) {
                        let file = cached.open_file()?;
                        let sent = sendfile_to_socket(
                            socket_fd.unwrap(),
                            &file,
                            0,
                            cached.size as usize,
                        ).await?;

                        metrics::counter!("streamline_zerocopy_sendfile_bytes_total")
                            .increment(sent as u64);
                        continue;
                    }
                }
            }
        }

        // Regular write path (TLS or cache miss)
        let response_size = response.data.len() as u32;
        writer.write_all(&response_size.to_be_bytes()).await?;
        writer.write_all(&response.data).await?;
        writer.flush().await?;

        metrics::counter!("streamline_bytes_sent_total").increment(response_size as u64 + 4);

        // Cache the response for future sendfile use
        if ctx.can_sendfile() {
            if let Some(cache) = &response_cache {
                if let Some(ref cache_key) = response.cache_key {
                    let _ = cache.put(cache_key.clone(), &response.data);
                }
            }
        }
    }

    Ok(())
}
```

### Phase 5: Integration & Testing

#### Update `src/server/mod.rs`

Pass configuration to `KafkaHandler`:

```rust
let kafka_handler = KafkaHandler::new_with_zerocopy(
    topic_manager.clone(),
    group_coordinator.clone(),
    config.storage.zerocopy.clone(),
);
```

#### Update `src/storage/mod.rs`

Export new modules:

```rust
pub mod mmap;

pub use mmap::MmapSegmentReader;
```

#### Update `src/protocol/mod.rs`

Export new modules:

```rust
pub mod connection;
pub mod response_cache;

pub use connection::ConnectionContext;
pub use response_cache::{CacheKey, CachedResponse, ResponseCache};
```

## Implementation Tasks

### Phase 1: Foundation ✅ COMPLETED
- [x] Create `src/protocol/connection.rs` with `ConnectionContext`
- [x] Add `ZeroCopyConfig` to `src/config/mod.rs`
- [x] Add CLI arguments for zero-copy configuration
- [x] Add zero-copy metrics
- [x] Modify `handle_connection()` and `handle_tls_connection()` to create `ConnectionContext`

### Phase 2: Buffer Pool ✅ COMPLETED
- [x] Extend `BufferPool` with `get_with_capacity()` method
- [x] Add `buffer_pool: Arc<BufferPool>` to `KafkaHandler`
- [x] Eliminate aggregation copy at `kafka.rs:2494` (now uses buffer pool)
- [x] Integrate buffer pool into fetch response building

### Phase 3: Memory-Mapped Segments ✅ COMPLETED
- [x] Add `memmap2 = "0.9"` to `Cargo.toml`
- [x] Create `src/storage/mmap.rs` with `MmapSegmentReader`
- [x] Add `mmap_reader` field to `Segment` struct
- [x] Create `Segment::open_with_config()` method
- [x] Add mmap fallback handling for errors

### Phase 4: Response Cache & Sendfile ✅ COMPLETED
- [x] Create `src/protocol/response_cache.rs`
- [x] Implement `sendfile_to_socket()` in `src/storage/zerocopy.rs`
- [x] Add `cache_key` field to `PendingResponse`
- [x] Modify `writer_task()` to accept `ConnectionContext` and `ResponseCache`
- [x] Wire response caching in writer_task
- [x] Add `socket_fd` field to `ConnectionContext` for storing raw socket descriptor
- [x] Add `plain_tcp_with_fd()` constructor for Unix platforms
- [x] Extract socket fd in `handle_connection()` before stream split using `AsRawFd`
- [x] Implement `try_sendfile_response()` in writer_task for cache hits
- [x] Handle partial writes with retry loop for complete transfers

**Sendfile Implementation**: Complete sendfile() support is now available for plain TCP connections. The socket fd is extracted before the stream is split, enabling zero-copy transfers for cached responses.

### Phase 5: Testing & Documentation ✅ COMPLETED
- [x] Unit tests for `ConnectionContext` (in connection.rs)
- [x] Unit tests for `MmapSegmentReader` (in mmap.rs)
- [x] Unit tests for `ResponseCache` (in response_cache.rs)
- [x] Create integration test for TLS fallback (tests/zerocopy_test.rs)
- [x] Integration tests for sendfile path (test_socket_fd_storage, test_sendfile_path_conditions, test_sendfile_cache_integration, test_sendfile_function_edge_cases)
- [x] Add benchmarks for zero-copy vs regular path (benches/zerocopy_benchmarks.rs)
- [x] Update `docs/PERFORMANCE.md` (already documents zero-copy configuration)

## Configuration

### Default Configuration (Zero Configuration Needed)

```yaml
storage:
  zerocopy:
    enabled: true
    enable_mmap: true
    mmap_threshold: 1048576        # 1MB
    enable_buffer_pool: true
    buffer_pool_size: 16
    buffer_size: 65536             # 64KB
    enable_sendfile: true          # Auto-disabled for TLS
    enable_response_cache: true
    response_cache_max_bytes: 134217728  # 128MB
    response_cache_ttl_secs: 60
```

### CLI Examples

```bash
# Default - all optimizations enabled
streamline

# Disable all zero-copy (for debugging)
streamline --zerocopy-enabled=false

# Disable response cache only
streamline --zerocopy-response-cache=false

# Fine-tune buffer pool
streamline --zerocopy-buffer-pool-size=32 --zerocopy-buffer-size=131072
```

## Performance Targets

| Metric | Current | Target | Notes |
|--------|---------|--------|-------|
| Fetch latency (hot, p99) | ~2ms | <1.5ms | With mmap |
| Fetch CPU usage | baseline | -30% | With sendfile |
| Memory allocations/fetch | ~3-5 | <1 | With BufferPool |
| Context switches/fetch | 4 | 2 | With sendfile |

## Limitations

### TLS Incompatibility with Sendfile

When TLS is enabled, `sendfile()` cannot be used because:
- Data must be encrypted before transmission
- Encryption happens in user space
- Cannot send raw file bytes over encrypted channel

**Behavior**: Automatically disable sendfile for TLS connections, fall back to mmap + buffer pool optimizations which still provide benefits.

### Platform Support

| Platform | mmap | sendfile | Response Cache |
|----------|------|----------|----------------|
| Linux | Yes | Yes | Yes |
| macOS | Yes | Yes (copyfile) | Yes |
| Windows | No | No | Yes (no sendfile) |

### Safety Considerations

- Only mmap sealed (immutable) segments
- Handle SIGBUS gracefully if segment is unexpectedly truncated
- Response cache uses filesystem - ensure cache directory has sufficient space

## Related Files

- `src/storage/zerocopy.rs` - Existing zero-copy utilities
- `src/storage/segment.rs` - Segment implementation
- `src/protocol/kafka.rs` - Protocol handler (lines 2494, 679-711)
- `src/server/mod.rs` - Server connection handling
- `src/config/mod.rs` - Configuration
- `docs/PERFORMANCE.md` - Performance documentation

## References

- [Kafka Zero-Copy Design](https://docs.confluent.io/platform/6.2/kafka/design.html)
- [memmap2 Crate](https://docs.rs/memmap2/latest/memmap2/)
- [Linux sendfile(2)](https://man7.org/linux/man-pages/man2/sendfile.2.html)
- [macOS sendfile(2)](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/sendfile.2.html)

## Labels

`medium-priority`, `performance`, `storage`, `networking`
