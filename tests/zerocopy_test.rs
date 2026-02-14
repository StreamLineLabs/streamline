//! Integration tests for zero-copy networking optimizations
//!
//! These tests verify that zero-copy features work correctly in various
//! scenarios including TLS fallback and plain TCP optimization paths.

use streamline::protocol::response_cache::{CacheKey, ResponseCache};
use streamline::protocol::ConnectionContext;
#[cfg(unix)]
use streamline::protocol::RawFd;
use streamline::storage::{BufferPool, MmapSegmentReader, ZeroCopyConfig, ZeroCopyReader};
use tempfile::tempdir;

/// Test that ConnectionContext correctly identifies TLS vs plain TCP
#[test]
fn test_connection_context_tls_detection() {
    let config = ZeroCopyConfig::default();

    // Plain TCP with socket fd should allow sendfile on Unix
    #[cfg(unix)]
    {
        let dummy_fd: RawFd = 42; // Fake fd for testing
        let plain_ctx = ConnectionContext::plain_tcp_with_fd(None, config.clone(), dummy_fd);
        assert!(!plain_ctx.is_tls);
        assert!(plain_ctx.can_sendfile());
        assert!(plain_ctx.can_mmap());
    }

    // Plain TCP without socket fd should not allow sendfile
    #[cfg(not(unix))]
    {
        let plain_ctx = ConnectionContext::plain_tcp(None, config.clone());
        assert!(!plain_ctx.is_tls);
        assert!(!plain_ctx.can_sendfile()); // No socket fd available
        assert!(plain_ctx.can_mmap());
    }

    // TLS should disable sendfile automatically
    let tls_ctx = ConnectionContext::tls(None, config);
    assert!(tls_ctx.is_tls);
    assert!(!tls_ctx.can_sendfile());
    assert!(tls_ctx.can_mmap()); // mmap still works with TLS
}

/// Test that ZeroCopyConfig can be disabled
#[test]
fn test_zerocopy_config_disabled() {
    let config = ZeroCopyConfig {
        enabled: false,
        enable_sendfile: false,
        enable_mmap: false,
        ..Default::default()
    };

    let ctx = ConnectionContext::plain_tcp(None, config);
    assert!(!ctx.can_sendfile());
    assert!(!ctx.can_mmap());
}

/// Test buffer pool allocation and reuse
#[tokio::test]
async fn test_buffer_pool_reuse() {
    let pool = BufferPool::new(4, 4096);

    // Get a buffer and use it
    let mut buf = pool.get().await;
    buf.extend_from_slice(b"test data");
    let cap = buf.capacity();

    // Return buffer to pool
    pool.put(buf).await;

    // Get buffer again - should be reused
    let buf2 = pool.get().await;
    assert_eq!(buf2.capacity(), cap);

    // Check stats
    let stats = pool.stats().await;
    assert_eq!(stats.max_buffers, 4);
    assert_eq!(stats.buffer_size, 4096);
}

/// Test buffer pool with capacity request
#[tokio::test]
async fn test_buffer_pool_capacity() {
    let pool = BufferPool::new(4, 1024);

    // Request buffer with specific capacity
    let buf = pool.get_with_capacity(8192).await;
    assert!(buf.capacity() >= 8192);

    // Return it
    pool.put(buf).await;

    // Request smaller buffer - should get the large one back
    let buf2 = pool.get_with_capacity(1024).await;
    assert!(buf2.capacity() >= 8192);
}

/// Test response cache basic operations
#[test]
fn test_response_cache_basic() {
    let temp_dir = tempdir().unwrap();
    let cache = ResponseCache::new(temp_dir.path().to_path_buf(), 1024 * 1024, 60).unwrap();

    let key = CacheKey::new("test-topic".to_string(), 0, 100, 4096);
    let data = b"cached response data for testing";

    // Initially cache miss
    assert!(cache.get(&key).is_none());

    // Add to cache
    let entry = cache.put(key.clone(), data).unwrap();
    assert_eq!(entry.size, data.len() as u64);

    // Now should hit
    let cached = cache.get(&key).unwrap();
    assert_eq!(cached.size, data.len() as u64);

    // Verify file contents
    let file_data = std::fs::read(&cached.file_path).unwrap();
    assert_eq!(&file_data[..], data);
}

/// Test response cache eviction
#[test]
fn test_response_cache_eviction() {
    let temp_dir = tempdir().unwrap();
    // Very small cache - only 100 bytes
    let cache = ResponseCache::new(temp_dir.path().to_path_buf(), 100, 60).unwrap();

    // Add entries that exceed cache size
    for i in 0..5 {
        let key = CacheKey::new(format!("topic-{}", i), 0, 0, 1024);
        let data = vec![0u8; 50]; // 50 bytes each
        cache.put(key, &data).unwrap();
    }

    // Should have evicted some entries
    assert!(
        cache
            .stats()
            .evictions
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );
    assert!(cache.current_size() <= 100);
}

/// Test response cache clear
#[test]
fn test_response_cache_clear() {
    let temp_dir = tempdir().unwrap();
    let cache = ResponseCache::new(temp_dir.path().to_path_buf(), 1024 * 1024, 60).unwrap();

    // Add some entries
    for i in 0..3 {
        let key = CacheKey::new(format!("topic-{}", i), 0, 0, 1024);
        cache.put(key, b"test data").unwrap();
    }

    assert_eq!(cache.entry_count(), 3);

    // Clear cache
    cache.clear();

    assert_eq!(cache.entry_count(), 0);
    assert_eq!(cache.current_size(), 0);
}

/// Test ZeroCopyReader with real file
#[test]
fn test_zerocopy_reader() {
    use std::io::Write;

    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("test_data.bin");

    // Create test file
    let data = b"Hello, zero-copy world! This is test data for reading.";
    {
        let mut file = std::fs::File::create(&file_path).unwrap();
        file.write_all(data).unwrap();
    }

    // Open with zero-copy reader
    let reader = ZeroCopyReader::open(&file_path).unwrap();
    assert_eq!(reader.size(), data.len() as u64);

    // Read entire file
    let content = reader.read_range(0, data.len()).unwrap();
    assert_eq!(&content[..], data);

    // Read partial range
    let partial = reader.read_range(7, 9).unwrap();
    assert_eq!(&partial[..], b"zero-copy");
}

/// Test ZeroCopyReader with multiple ranges
#[test]
fn test_zerocopy_reader_ranges() {
    use std::io::Write;

    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("ranges_test.bin");

    let data = b"0123456789ABCDEFGHIJ";
    {
        let mut file = std::fs::File::create(&file_path).unwrap();
        file.write_all(data).unwrap();
    }

    let reader = ZeroCopyReader::open(&file_path).unwrap();

    let ranges = &[(0, 5), (5, 5), (10, 5)];
    let results = reader.read_ranges(ranges).unwrap();

    assert_eq!(&results[0][..], b"01234");
    assert_eq!(&results[1][..], b"56789");
    assert_eq!(&results[2][..], b"ABCDE");
}

/// Test MmapSegmentReader with segment file
#[test]
fn test_mmap_segment_reader() {
    use bytes::Bytes;
    use streamline::storage::record::{Record, RecordBatch};
    use streamline::storage::segment::Segment;

    let temp_dir = tempdir().unwrap();
    let segment_path = temp_dir.path().join("00000000000000000000.segment");

    // Create a segment with test data
    let mut segment = Segment::create(&segment_path, 0).unwrap();
    let timestamp = chrono::Utc::now().timestamp_millis();
    let mut batch = RecordBatch::new(0, timestamp);

    for i in 0..10 {
        batch.add_record(Record::new(
            i as i64,
            timestamp,
            Some(Bytes::from(format!("key-{}", i))),
            Bytes::from(format!("value-{}", i)),
        ));
    }

    segment.append_batch(&batch).unwrap();
    segment.seal().unwrap();

    // Open with mmap reader (threshold 0 to always use mmap)
    let reader = MmapSegmentReader::open(&segment_path, 0).unwrap();

    // Verify header is readable
    assert_eq!(reader.header().base_offset, 0);

    // Verify we can read data
    assert!(reader.size() > 64); // At least header size
    assert!(!reader.data().is_empty());

    // Verify magic bytes via slice
    let header_slice = reader.slice(0, 4);
    assert_eq!(header_slice, b"STRM");
}

/// Test mmap threshold enforcement
#[test]
fn test_mmap_threshold() {
    use bytes::Bytes;
    use streamline::storage::record::{Record, RecordBatch};
    use streamline::storage::segment::Segment;

    let temp_dir = tempdir().unwrap();
    let segment_path = temp_dir.path().join("small.segment");

    // Create a small segment
    let mut segment = Segment::create(&segment_path, 0).unwrap();
    let timestamp = chrono::Utc::now().timestamp_millis();
    let mut batch = RecordBatch::new(0, timestamp);
    batch.add_record(Record::new(
        0,
        timestamp,
        Some(Bytes::from("key")),
        Bytes::from("value"),
    ));
    segment.append_batch(&batch).unwrap();
    segment.seal().unwrap();

    let file_size = std::fs::metadata(&segment_path).unwrap().len();

    // High threshold should prevent mmap
    let result = MmapSegmentReader::open(&segment_path, file_size + 1);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("below mmap threshold"));

    // Threshold at or below file size should work
    let result = MmapSegmentReader::open(&segment_path, file_size);
    assert!(result.is_ok());
}

/// Test ZeroCopyConfig serialization
#[test]
fn test_zerocopy_config_serialization() {
    let config = ZeroCopyConfig::default();

    // Serialize to JSON
    let json = serde_json::to_string(&config).unwrap();
    assert!(json.contains("\"enabled\":true"));
    assert!(json.contains("\"enable_mmap\":true"));
    assert!(json.contains("\"enable_sendfile\":true"));

    // Deserialize back
    let parsed: ZeroCopyConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.enabled, config.enabled);
    assert_eq!(parsed.enable_mmap, config.enable_mmap);
    assert_eq!(parsed.mmap_threshold, config.mmap_threshold);
}

/// Test CacheKey filename generation with special characters
#[test]
fn test_cache_key_special_chars() {
    let key = CacheKey::new("my/topic:name\\test".to_string(), 1, 200, 2048);

    // Verify topic with special characters generates valid filename
    // (CacheKey::to_filename is private, but we can test via cache operations)
    let temp_dir = tempdir().unwrap();
    let cache = ResponseCache::new(temp_dir.path().to_path_buf(), 1024, 60).unwrap();

    // This should work without errors
    let result = cache.put(key.clone(), b"test data");
    assert!(result.is_ok());

    // Should be retrievable
    assert!(cache.get(&key).is_some());
}

/// Test connection context with peer address
#[test]
fn test_connection_context_peer_addr() {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    let config = ZeroCopyConfig::default();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 9092);

    let ctx = ConnectionContext::plain_tcp(Some(addr), config);
    assert_eq!(ctx.peer_addr, Some(addr));
}

// =============================================================================
// TLS Fallback Integration Tests
// =============================================================================

/// Test that TLS context properly disables sendfile while keeping other optimizations
#[test]
fn test_tls_fallback_sendfile_disabled() {
    let config = ZeroCopyConfig {
        enabled: true,
        enable_sendfile: true, // Explicitly enabled
        enable_mmap: true,
        enable_buffer_pool: true,
        enable_response_cache: true,
        ..Default::default()
    };

    // TLS should override enable_sendfile to false
    let tls_ctx = ConnectionContext::tls(None, config.clone());
    assert!(tls_ctx.is_tls);
    assert!(!tls_ctx.can_sendfile(), "TLS should disable sendfile");
    assert!(tls_ctx.can_mmap(), "TLS should still allow mmap");
    assert!(
        tls_ctx.zerocopy_config.enable_buffer_pool,
        "TLS should still allow buffer pool"
    );
    assert!(
        tls_ctx.zerocopy_config.enable_response_cache,
        "TLS should still allow response cache"
    );

    // Plain TCP with socket fd should keep sendfile enabled
    #[cfg(unix)]
    {
        let dummy_fd: RawFd = 42;
        let plain_ctx = ConnectionContext::plain_tcp_with_fd(None, config, dummy_fd);
        assert!(!plain_ctx.is_tls);
        assert!(
            plain_ctx.can_sendfile(),
            "Plain TCP with fd should allow sendfile"
        );
    }

    // Plain TCP without socket fd - sendfile not available
    #[cfg(not(unix))]
    {
        let plain_ctx = ConnectionContext::plain_tcp(None, config);
        assert!(!plain_ctx.is_tls);
        assert!(
            !plain_ctx.can_sendfile(),
            "Plain TCP without fd cannot use sendfile"
        );
    }
}

/// Test TLS fallback with buffer pool - ensures buffer pool works regardless of TLS
#[tokio::test]
async fn test_tls_fallback_buffer_pool_works() {
    let pool = BufferPool::new(4, 4096);

    // Simulate TLS connection using buffer pool for allocation
    let tls_config = ZeroCopyConfig::default();
    let tls_ctx = ConnectionContext::tls(None, tls_config);

    // Buffer pool should work even with TLS
    assert!(tls_ctx.zerocopy_config.enable_buffer_pool);

    // Allocate and use buffers as TLS connection would
    let mut buf = pool.get_with_capacity(8192).await;
    buf.extend_from_slice(b"simulated TLS encrypted data that would be written");

    // Verify allocation worked
    assert!(buf.capacity() >= 8192);
    assert!(!buf.is_empty());

    // Return to pool
    pool.put(buf).await;

    // Pool should still function
    let stats = pool.stats().await;
    assert!(stats.available > 0);
}

/// Test TLS fallback with mmap - ensures mmap works regardless of TLS
#[test]
fn test_tls_fallback_mmap_works() {
    use bytes::Bytes;
    use streamline::storage::record::{Record, RecordBatch};
    use streamline::storage::segment::Segment;

    let temp_dir = tempdir().unwrap();
    let segment_path = temp_dir.path().join("tls_mmap_test.segment");

    // Create a segment
    let mut segment = Segment::create(&segment_path, 0).unwrap();
    let timestamp = chrono::Utc::now().timestamp_millis();
    let mut batch = RecordBatch::new(0, timestamp);

    for i in 0..50 {
        batch.add_record(Record::new(
            i as i64,
            timestamp,
            Some(Bytes::from(format!("key-{}", i))),
            Bytes::from(format!("value-{}-{}", i, "x".repeat(200))),
        ));
    }

    segment.append_batch(&batch).unwrap();
    segment.seal().unwrap();

    // Simulate TLS context
    let tls_config = ZeroCopyConfig::default();
    let tls_ctx = ConnectionContext::tls(None, tls_config);

    // Mmap should still be enabled for TLS
    assert!(tls_ctx.can_mmap());

    // Open with mmap reader - should work for TLS connections too
    let reader = MmapSegmentReader::open(&segment_path, 0).unwrap();

    // Read data via mmap (this would then be encrypted in user space for TLS)
    let data = reader.data();
    assert!(!data.is_empty());

    // Verify header
    let header_slice = reader.slice(0, 4);
    assert_eq!(header_slice, b"STRM");
}

/// Test TLS fallback with response cache - cache works but sendfile path is skipped
#[test]
fn test_tls_fallback_response_cache_works() {
    let temp_dir = tempdir().unwrap();
    let cache = ResponseCache::new(temp_dir.path().to_path_buf(), 1024 * 1024, 60).unwrap();

    let tls_config = ZeroCopyConfig::default();
    let tls_ctx = ConnectionContext::tls(None, tls_config);

    // Response cache should be enabled
    assert!(tls_ctx.zerocopy_config.enable_response_cache);

    // Cache a response (would normally be for sendfile, but TLS uses regular write)
    let key = CacheKey::new("tls-topic".to_string(), 0, 100, 4096);
    let data = b"cached response that would be encrypted before sending";

    cache.put(key.clone(), data).unwrap();

    // Cache lookup should work
    let cached = cache.get(&key).unwrap();
    assert_eq!(cached.size, data.len() as u64);

    // For TLS, we'd read from cache file and encrypt, not use sendfile
    // Verify we can read the cached data
    let file_contents = std::fs::read(&cached.file_path).unwrap();
    assert_eq!(&file_contents[..], data);
}

/// Test complete TLS fallback scenario - all optimizations that work with TLS
#[tokio::test]
async fn test_tls_fallback_complete_scenario() {
    use bytes::Bytes;
    use streamline::storage::record::{Record, RecordBatch};
    use streamline::storage::segment::Segment;

    let temp_dir = tempdir().unwrap();
    let segment_path = temp_dir.path().join("complete_tls_test.segment");
    let cache_dir = temp_dir.path().join("cache");

    // Setup: Create segment with data
    {
        let mut segment = Segment::create(&segment_path, 0).unwrap();
        let timestamp = chrono::Utc::now().timestamp_millis();
        let mut batch = RecordBatch::new(0, timestamp);

        for i in 0..20 {
            batch.add_record(Record::new(
                i as i64,
                timestamp,
                Some(Bytes::from(format!("key-{}", i))),
                Bytes::from(format!("value-{}", i)),
            ));
        }
        segment.append_batch(&batch).unwrap();
        segment.seal().unwrap();
    }

    // Create TLS connection context
    let tls_config = ZeroCopyConfig {
        enabled: true,
        enable_sendfile: true,       // Will be disabled by TLS context
        enable_mmap: true,           // Should work
        enable_buffer_pool: true,    // Should work
        enable_response_cache: true, // Should work (but no sendfile)
        ..Default::default()
    };
    let tls_ctx = ConnectionContext::tls(None, tls_config);

    // Verify TLS disables only sendfile
    assert!(!tls_ctx.can_sendfile());
    assert!(tls_ctx.can_mmap());

    // Step 1: Use mmap to read segment (zero-copy from disk)
    let mmap_reader = MmapSegmentReader::open(&segment_path, 0).unwrap();
    let segment_data = mmap_reader.data();
    assert!(!segment_data.is_empty());

    // Step 2: Use buffer pool for response building
    let pool = BufferPool::new(4, 4096);
    let mut response_buf = pool.get_with_capacity(segment_data.len()).await;
    response_buf.extend_from_slice(segment_data);

    // Step 3: Cache the response (for potential future use)
    let cache = ResponseCache::new(cache_dir, 1024 * 1024, 60).unwrap();
    let cache_key = CacheKey::new("test-topic".to_string(), 0, 0, 4096);
    cache.put(cache_key.clone(), &response_buf).unwrap();

    // Step 4: For TLS, we'd encrypt and write (simulated here)
    // In real code: tls_stream.write_all(&encrypted_data)
    let response_to_send = response_buf.freeze();
    assert!(!response_to_send.is_empty());

    // Verify cache works for subsequent requests
    let cached = cache.get(&cache_key).unwrap();
    assert!(cached.size > 0);

    // Return buffer to pool for reuse
    // (In this test, buffer was frozen, so we get a new one)
    let _ = pool.get().await;
}

/// Test that plain TCP and TLS contexts have correct capabilities
#[test]
fn test_connection_context_capabilities_matrix() {
    let config = ZeroCopyConfig::default();

    // Plain TCP with socket fd - full capabilities on Unix
    #[cfg(unix)]
    {
        let dummy_fd: RawFd = 42;
        let plain_ctx = ConnectionContext::plain_tcp_with_fd(None, config.clone(), dummy_fd);
        assert!(!plain_ctx.is_tls);
        assert!(plain_ctx.can_sendfile());
        assert!(plain_ctx.can_mmap());
        assert!(plain_ctx.zerocopy_config.enable_buffer_pool);
        assert!(plain_ctx.zerocopy_config.enable_response_cache);
    }

    // Plain TCP without socket fd - sendfile not available
    #[cfg(not(unix))]
    {
        let plain_ctx = ConnectionContext::plain_tcp(None, config.clone());
        assert!(!plain_ctx.is_tls);
        assert!(!plain_ctx.can_sendfile()); // No socket fd
        assert!(plain_ctx.can_mmap());
        assert!(plain_ctx.zerocopy_config.enable_buffer_pool);
        assert!(plain_ctx.zerocopy_config.enable_response_cache);
    }

    // TLS - sendfile disabled, others enabled
    let tls_ctx = ConnectionContext::tls(None, config.clone());
    assert!(tls_ctx.is_tls);
    assert!(!tls_ctx.can_sendfile());
    assert!(tls_ctx.can_mmap());
    assert!(tls_ctx.zerocopy_config.enable_buffer_pool);
    assert!(tls_ctx.zerocopy_config.enable_response_cache);

    // Disabled config - nothing works
    let disabled_config = ZeroCopyConfig {
        enabled: false,
        enable_sendfile: false,
        enable_mmap: false,
        enable_buffer_pool: false,
        enable_response_cache: false,
        ..Default::default()
    };
    let disabled_ctx = ConnectionContext::plain_tcp(None, disabled_config);
    assert!(!disabled_ctx.can_sendfile());
    assert!(!disabled_ctx.can_mmap());
}

/// Test TLS context preserves peer address
#[test]
fn test_tls_context_preserves_peer_addr() {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    let config = ZeroCopyConfig::default();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 443);

    let tls_ctx = ConnectionContext::tls(Some(addr), config);
    assert_eq!(tls_ctx.peer_addr, Some(addr));
    assert!(tls_ctx.is_tls);
}

/// Test socket_fd storage and retrieval in ConnectionContext (Unix only)
#[cfg(unix)]
#[test]
fn test_socket_fd_storage() {
    let config = ZeroCopyConfig::default();
    let dummy_fd: RawFd = 123;

    // Create context with socket fd
    let ctx = ConnectionContext::plain_tcp_with_fd(None, config, dummy_fd);

    // Verify fd is stored correctly
    assert_eq!(ctx.get_socket_fd(), Some(123));
    assert!(ctx.can_sendfile());

    // Verify socket_fd is None for plain_tcp without fd
    let ctx_no_fd = ConnectionContext::plain_tcp(None, ZeroCopyConfig::default());
    assert_eq!(ctx_no_fd.get_socket_fd(), None);
    assert!(!ctx_no_fd.can_sendfile());
}

/// Test sendfile path conditions are correctly evaluated
#[cfg(unix)]
#[test]
fn test_sendfile_path_conditions() {
    // Condition 1: Must have socket_fd
    let config_enabled = ZeroCopyConfig::default();
    let ctx_no_fd = ConnectionContext::plain_tcp(None, config_enabled.clone());
    assert!(!ctx_no_fd.can_sendfile(), "No fd means no sendfile");

    // Condition 2: Must not be TLS
    let tls_ctx = ConnectionContext::tls(None, config_enabled.clone());
    assert!(!tls_ctx.can_sendfile(), "TLS means no sendfile");

    // Condition 3: Must have sendfile enabled in config
    let mut config_disabled = config_enabled.clone();
    config_disabled.enable_sendfile = false;
    let dummy_fd: RawFd = 42;
    let ctx_disabled = ConnectionContext::plain_tcp_with_fd(None, config_disabled, dummy_fd);
    assert!(
        !ctx_disabled.can_sendfile(),
        "Disabled config means no sendfile"
    );

    // Condition 4: Master switch must be on
    let mut config_master_off = config_enabled.clone();
    config_master_off.enabled = false;
    let ctx_master_off = ConnectionContext::plain_tcp_with_fd(None, config_master_off, dummy_fd);
    assert!(
        !ctx_master_off.can_sendfile(),
        "Master switch off means no sendfile"
    );

    // All conditions met
    let ctx_all_good = ConnectionContext::plain_tcp_with_fd(None, config_enabled, dummy_fd);
    assert!(
        ctx_all_good.can_sendfile(),
        "All conditions met should enable sendfile"
    );
}

/// Test sendfile with response cache integration (Unix only)
#[cfg(unix)]
#[test]
fn test_sendfile_cache_integration() {
    use std::fs;

    let temp_dir = tempdir().unwrap();
    let cache = ResponseCache::new(temp_dir.path().to_path_buf(), 1024 * 1024, 60).unwrap();

    // Create a cache key and some data
    let cache_key = CacheKey::new("sendfile-test-topic".to_string(), 0, 0, 4096);
    let test_data = b"This is test data that would be sent via sendfile";

    // Put data in cache
    let entry = cache.put(cache_key.clone(), test_data).unwrap();
    assert_eq!(entry.size, test_data.len() as u64);

    // Verify cache file exists and has correct content
    let cached = cache.get(&cache_key).unwrap();
    assert_eq!(cached.size, test_data.len() as u64);

    // Open the cached file (this is what sendfile would do)
    let file = cached.open().unwrap();
    let metadata = file.metadata().unwrap();
    assert_eq!(metadata.len(), test_data.len() as u64);

    // Verify file content
    let file_content = fs::read(&cached.file_path).unwrap();
    assert_eq!(file_content, test_data);

    // Create a context that would enable sendfile
    let config = ZeroCopyConfig::default();
    let dummy_fd: RawFd = 42;
    let ctx = ConnectionContext::plain_tcp_with_fd(None, config, dummy_fd);

    // Verify all sendfile conditions are met
    assert!(ctx.can_sendfile());
    assert!(ctx.zerocopy_config.enable_response_cache);
    assert!(ctx.get_socket_fd().is_some());
}

/// Test that sendfile_to_socket function signature is correct (Unix only)
/// This test verifies the sendfile function exists and handles edge cases
#[cfg(unix)]
#[test]
fn test_sendfile_function_edge_cases() {
    use std::io::Write;
    use streamline::storage::zerocopy::sendfile_to_socket;
    use tempfile::NamedTempFile;

    // Create a temp file with some content
    let mut temp_file = NamedTempFile::new().unwrap();
    let content = b"Test content for sendfile";
    temp_file.write_all(content).unwrap();
    temp_file.flush().unwrap();

    // Open the file for reading
    let file = std::fs::File::open(temp_file.path()).unwrap();

    // Try sendfile with an invalid socket fd - should fail
    let result = sendfile_to_socket(-1, &file, 0, content.len());
    assert!(result.is_err(), "Invalid fd should fail");

    // Verify error is a reasonable OS error
    let err = result.unwrap_err();
    assert!(
        err.raw_os_error().is_some(),
        "Should be an OS error: {}",
        err
    );
}

// =============================================================================
// io_backend Integration Tests
// =============================================================================

/// Test StandardFileSystem basic operations
#[tokio::test]
async fn test_io_backend_standard_create_and_read() {
    use streamline::storage::{get_standard_backend, AsyncFile, AsyncFileSystem};

    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().join("io_backend_test.txt");
    let fs = get_standard_backend();

    // Create file and write
    let file = fs.create(&path).await.unwrap();
    let data = b"hello io_backend world".to_vec();
    let (result, _) = file.write_at(data.clone(), 0).await;
    result.unwrap();
    file.sync_all().await.unwrap();

    // Read back
    let file = fs.open(&path).await.unwrap();
    let buf = vec![0u8; 100];
    let (result, buf) = file.read_at(buf, 0).await;
    let bytes_read = result.unwrap();
    assert_eq!(bytes_read, data.len());
    assert_eq!(&buf[..bytes_read], data.as_slice());
}

/// Test StandardFileSystem append operations
#[tokio::test]
async fn test_io_backend_standard_append() {
    use streamline::storage::{get_standard_backend, AsyncFile, AsyncFileSystem};

    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().join("io_backend_append.txt");
    let fs = get_standard_backend();

    // Create and append
    let file = fs.open_append(&path).await.unwrap();
    let (result, _) = file.append(b"first ".to_vec()).await;
    result.unwrap();
    let (result, _) = file.append(b"second".to_vec()).await;
    result.unwrap();
    file.sync_all().await.unwrap();

    // Verify size
    let size = file.size().await.unwrap();
    assert_eq!(size, 12);

    // Read back
    let buf = vec![0u8; 12];
    let (result, buf) = file.read_at(buf, 0).await;
    let bytes_read = result.unwrap();
    assert_eq!(&buf[..bytes_read], b"first second");
}

/// Test io_backend should_use_uring detection (platform-specific)
#[test]
fn test_io_backend_uring_detection() {
    use streamline::storage::should_use_uring;

    // This test verifies the function works and returns consistent results
    let use_uring = should_use_uring();

    // On non-Linux or without io-uring feature, should always be false
    #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
    {
        assert!(
            !use_uring,
            "io_uring should not be available on this platform"
        );
    }

    // On Linux with io-uring feature, depends on kernel version
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        // Just verify it doesn't crash - actual availability depends on kernel
        println!("io_uring available: {}", use_uring);
    }
}

/// Test IoBufferPool allocation and reuse
#[test]
fn test_io_buffer_pool() {
    use streamline::storage::IoBufferPool;

    let pool = IoBufferPool::default_pool();

    // Acquire buffer of specific size class
    let buf = pool.acquire(4096);
    assert!(buf.capacity() >= 4096);

    // Release and acquire again - should be reused
    pool.release(buf);
    let buf2 = pool.acquire(4096);
    assert!(buf2.capacity() >= 4096);

    // Check stats
    let stats = pool.stats();
    assert!(stats.allocations > 0 || stats.reuses > 0);
}

/// Test IoBufferPool size classes
#[test]
fn test_io_buffer_pool_size_classes() {
    use streamline::storage::IoBufferPool;

    let pool = IoBufferPool::default_pool();

    // Request small buffer (rounds up to 4KB class)
    let small = pool.acquire(1000);
    assert!(small.capacity() >= 1000);

    // Request medium buffer (rounds up to 16KB class)
    let medium = pool.acquire(10000);
    assert!(medium.capacity() >= 10000);

    // Request large buffer (rounds up to 256KB class)
    let large = pool.acquire(100000);
    assert!(large.capacity() >= 100000);

    // Release all
    pool.release(small);
    pool.release(medium);
    pool.release(large);

    // Verify stats
    let stats = pool.stats();
    assert_eq!(stats.allocations, 3);
}

// =============================================================================
// AsyncSegment Integration Tests
// =============================================================================

/// Test AsyncSegment with StandardFileSystem
#[tokio::test]
async fn test_async_segment_basic_operations() {
    use bytes::Bytes;
    use streamline::storage::record::Record;
    use streamline::storage::{get_standard_backend, AsyncSegment};

    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().join("async_segment_basic.segment");
    let fs = get_standard_backend();

    // Create segment
    let segment = AsyncSegment::create(&fs, &path, 0).await.unwrap();

    // Write records
    for i in 0..10 {
        let record = Record::new(
            i,
            chrono::Utc::now().timestamp_millis() + i,
            Some(Bytes::from(format!("key-{}", i))),
            Bytes::from(format!("value-{}", i)),
        );
        segment.append_record(record).await.unwrap();
    }

    // Verify stats
    assert_eq!(segment.record_count().await, 10);
    assert_eq!(segment.max_offset().await, 9);
    assert!(!segment.is_sealed().await);

    // Read back
    let records = segment.read_all().await.unwrap();
    assert_eq!(records.len(), 10);
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.offset, i as i64);
        assert_eq!(record.value, Bytes::from(format!("value-{}", i)));
    }
}

/// Test AsyncSegment batch write performance
#[tokio::test]
async fn test_async_segment_batch_performance() {
    use bytes::Bytes;
    use streamline::storage::record::{Record, RecordBatch};
    use streamline::storage::{get_standard_backend, AsyncSegment};

    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().join("async_segment_batch.segment");
    let fs = get_standard_backend();

    let segment = AsyncSegment::create(&fs, &path, 0).await.unwrap();

    // Write batch of records
    let timestamp = chrono::Utc::now().timestamp_millis();
    let mut batch = RecordBatch::new(0, timestamp);
    for i in 0i64..1000 {
        batch.add_record(Record::new(
            i,
            timestamp + i,
            Some(Bytes::from(format!("key-{}", i))),
            Bytes::from(format!("value-{}-{}", i, "x".repeat(100))),
        ));
    }

    segment.append_batch(&batch).await.unwrap();

    // Verify
    assert_eq!(segment.record_count().await, 1000);

    // Read partial range
    let records = segment.read_from_offset(500, 100).await.unwrap();
    assert_eq!(records.len(), 100);
    assert_eq!(records[0].offset, 500);
    assert_eq!(records[99].offset, 599);
}

/// Test AsyncSegment seal and reopen
#[tokio::test]
async fn test_async_segment_seal_and_reopen() {
    use bytes::Bytes;
    use streamline::storage::record::Record;
    use streamline::storage::{get_standard_backend, AsyncSegment};

    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().join("async_segment_seal.segment");
    let fs = get_standard_backend();

    // Create and write
    {
        let segment = AsyncSegment::create(&fs, &path, 100).await.unwrap();

        for i in 0..5 {
            let record = Record::new(
                100 + i,
                chrono::Utc::now().timestamp_millis(),
                None,
                Bytes::from(format!("data-{}", i)),
            );
            segment.append_record(record).await.unwrap();
        }

        // Seal
        segment.seal().await.unwrap();
        assert!(segment.is_sealed().await);

        // Write should fail on sealed segment
        let result = segment
            .append_record(Record::new(105, 0, None, Bytes::from("should fail")))
            .await;
        assert!(result.is_err());
    }

    // Reopen and verify
    {
        let segment: AsyncSegment<_> = AsyncSegment::open(&fs, &path).await.unwrap();
        assert!(segment.is_sealed().await);

        let records = segment.read_all().await.unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].offset, 100);
        assert_eq!(records[4].offset, 104);
    }
}

/// Test AsyncSegment with compression
#[tokio::test]
async fn test_async_segment_with_compression() {
    use bytes::Bytes;
    use streamline::storage::record::Record;
    use streamline::storage::{get_standard_backend, AsyncSegment, CompressionCodec};

    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().join("async_segment_compressed.segment");
    let fs = get_standard_backend();

    // Create with LZ4 compression
    let segment = AsyncSegment::create_with_compression(&fs, &path, 0, CompressionCodec::Lz4)
        .await
        .unwrap();

    // Write some compressible data
    for i in 0..20 {
        let record = Record::new(
            i,
            chrono::Utc::now().timestamp_millis(),
            None,
            Bytes::from("a".repeat(1000)), // Highly compressible
        );
        segment.append_record(record).await.unwrap();
    }

    // Verify
    let records = segment.read_all().await.unwrap();
    assert_eq!(records.len(), 20);
    for record in &records {
        assert_eq!(record.value.len(), 1000);
    }

    // Size should be smaller than uncompressed (20 * ~1000 bytes)
    let size = segment.size().await;
    assert!(size < 20000, "Compressed size {} should be < 20000", size);
}

/// Test AsyncSegment open_for_append
#[tokio::test]
async fn test_async_segment_open_for_append() {
    use bytes::Bytes;
    use streamline::storage::record::Record;
    use streamline::storage::{get_standard_backend, AsyncSegment};

    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().join("async_segment_append.segment");
    let fs = get_standard_backend();

    // Create and write initial data
    {
        let segment = AsyncSegment::create(&fs, &path, 0).await.unwrap();
        for i in 0..5 {
            segment
                .append_record(Record::new(
                    i,
                    chrono::Utc::now().timestamp_millis(),
                    None,
                    Bytes::from(format!("initial-{}", i)),
                ))
                .await
                .unwrap();
        }
        segment.sync().await.unwrap();
    }

    // Reopen for append and add more
    {
        let segment: AsyncSegment<_> = AsyncSegment::open_for_append(&fs, &path).await.unwrap();
        assert!(!segment.is_sealed().await);

        for i in 5..10 {
            segment
                .append_record(Record::new(
                    i,
                    chrono::Utc::now().timestamp_millis(),
                    None,
                    Bytes::from(format!("appended-{}", i)),
                ))
                .await
                .unwrap();
        }

        // Verify all records
        let records = segment.read_all().await.unwrap();
        assert_eq!(records.len(), 10);
        assert_eq!(records[4].value, Bytes::from("initial-4"));
        assert_eq!(records[5].value, Bytes::from("appended-5"));
    }
}

/// Test concurrent reads on AsyncSegment
#[tokio::test]
async fn test_async_segment_concurrent_reads() {
    use bytes::Bytes;
    use std::sync::Arc;
    use streamline::storage::record::{Record, RecordBatch};
    use streamline::storage::{get_standard_backend, AsyncSegment};

    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().join("async_segment_concurrent.segment");
    let fs = get_standard_backend();

    // Create segment with data
    let segment = Arc::new(AsyncSegment::create(&fs, &path, 0).await.unwrap());

    let timestamp = chrono::Utc::now().timestamp_millis();
    let mut batch = RecordBatch::new(0, timestamp);
    for i in 0..100 {
        batch.add_record(Record::new(
            i as i64,
            timestamp,
            None,
            Bytes::from(format!("value-{}", i)),
        ));
    }
    segment.append_batch(&batch).await.unwrap();
    segment.seal().await.unwrap();

    // Spawn concurrent readers
    let mut handles = vec![];
    for _ in 0..10 {
        let seg = segment.clone();
        handles.push(tokio::spawn(async move {
            let records = seg.read_all().await.unwrap();
            assert_eq!(records.len(), 100);
            records
        }));
    }

    // Wait for all and verify
    for handle in handles {
        let records = handle.await.unwrap();
        assert_eq!(records.len(), 100);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[99].offset, 99);
    }
}
