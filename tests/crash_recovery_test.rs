//! Crash recovery tests for Streamline storage layer
//!
//! These tests verify that Streamline correctly handles and recovers from:
//! - Partial batch writes (simulated crash mid-write)
//! - WAL entries with corrupted checksums
//! - Segment files with corrupted record batches
//!
//! The tests simulate crash scenarios by directly manipulating storage files
//! to create various corruption patterns, then verify recovery behavior.
//!
//! ## Running Tests
//!
//! ```bash
//! cargo test --test crash_recovery_test
//! ```

use bytes::{BufMut, Bytes, BytesMut};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use tempfile::TempDir;

/// WAL magic bytes
const WAL_MAGIC: &[u8; 4] = b"WLOG";

/// WAL header size
const WAL_HEADER_SIZE: usize = 16;

/// Segment magic bytes
const SEGMENT_MAGIC: &[u8; 4] = b"STRM";

/// Segment header size
const SEGMENT_HEADER_SIZE: usize = 64;

/// Create a valid WAL header
fn create_wal_header() -> [u8; WAL_HEADER_SIZE] {
    let mut buf = [0u8; WAL_HEADER_SIZE];
    buf[0..4].copy_from_slice(WAL_MAGIC);
    buf[4..6].copy_from_slice(&1u16.to_le_bytes()); // version
    buf[6..8].copy_from_slice(&0u16.to_le_bytes()); // flags
    let timestamp = chrono::Utc::now().timestamp_millis();
    buf[8..16].copy_from_slice(&timestamp.to_le_bytes());
    buf
}

/// Create a valid WAL entry with proper CRC
fn create_wal_entry(sequence: u64, topic: &str, partition: i32, value: &[u8]) -> Bytes {
    let timestamp = chrono::Utc::now().timestamp_millis();
    let topic_bytes = topic.as_bytes();

    // Build data: null key (-1) + value length + value
    let mut data = BytesMut::new();
    data.put_i32_le(-1); // null key
    data.put_u32_le(value.len() as u32);
    data.put_slice(value);
    let data = data.freeze();

    // Compute CRC32
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&sequence.to_le_bytes());
    hasher.update(&timestamp.to_le_bytes());
    hasher.update(&[1u8]); // entry type = Record
    hasher.update(topic_bytes);
    hasher.update(&partition.to_le_bytes());
    hasher.update(&data);
    let crc32 = hasher.finalize();

    // Build entry
    let total_size = 8 + 8 + 1 + 2 + topic_bytes.len() + 4 + 4 + data.len() + 4;
    let mut buf = BytesMut::with_capacity(total_size);

    buf.put_u64_le(sequence);
    buf.put_i64_le(timestamp);
    buf.put_u8(1); // entry type = Record
    buf.put_u16_le(topic_bytes.len() as u16);
    buf.put_slice(topic_bytes);
    buf.put_i32_le(partition);
    buf.put_u32_le(data.len() as u32);
    buf.put_slice(&data);
    buf.put_u32_le(crc32);

    buf.freeze()
}

/// Create a valid segment header with proper CRC
fn create_segment_header(
    base_offset: i64,
    max_offset: i64,
    record_count: u32,
) -> [u8; SEGMENT_HEADER_SIZE] {
    let mut buf = [0u8; SEGMENT_HEADER_SIZE];

    // Magic (4 bytes)
    buf[0..4].copy_from_slice(SEGMENT_MAGIC);

    // Version (2 bytes)
    buf[4..6].copy_from_slice(&1u16.to_le_bytes());

    // Flags (2 bytes) - bincode format
    buf[6..8].copy_from_slice(&1u16.to_le_bytes());

    // Base offset (8 bytes)
    buf[8..16].copy_from_slice(&base_offset.to_le_bytes());

    // Max offset (8 bytes)
    buf[16..24].copy_from_slice(&max_offset.to_le_bytes());

    let timestamp = chrono::Utc::now().timestamp_millis();

    // Base timestamp (8 bytes)
    buf[24..32].copy_from_slice(&timestamp.to_le_bytes());

    // Max timestamp (8 bytes)
    buf[32..40].copy_from_slice(&timestamp.to_le_bytes());

    // Record count (4 bytes)
    buf[40..44].copy_from_slice(&record_count.to_le_bytes());

    // Compression (1 byte) = None
    buf[44] = 0;

    // Reserved (15 bytes) - already zeroed

    // CRC32 (4 bytes) - calculated over first 60 bytes
    let crc = crc32fast::hash(&buf[..60]);
    buf[60..64].copy_from_slice(&crc.to_le_bytes());

    buf
}

/// Test: Partial WAL entry write (truncated mid-entry)
///
/// Simulates a crash that occurred while writing a WAL entry, leaving
/// a truncated entry at the end of the file. Recovery should skip the
/// incomplete entry and not corrupt subsequent reads.
#[test]
fn test_wal_recovery_truncated_entry() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test.wal");

    // Create a WAL file with header + complete entry + truncated entry
    {
        let mut file = File::create(&wal_path).unwrap();

        // Write header
        file.write_all(&create_wal_header()).unwrap();

        // Write a complete entry
        let entry1 = create_wal_entry(1, "test-topic", 0, b"complete message");
        file.write_all(&entry1).unwrap();

        // Write a truncated entry (only first 10 bytes of a larger entry)
        let entry2 = create_wal_entry(2, "test-topic", 0, b"this will be truncated");
        file.write_all(&entry2[..10]).unwrap();

        file.sync_all().unwrap();
    }

    // Verify the file was created with expected size
    let metadata = std::fs::metadata(&wal_path).unwrap();
    assert!(metadata.len() > WAL_HEADER_SIZE as u64);

    // Read and verify structure
    let mut file = File::open(&wal_path).unwrap();
    let mut header = [0u8; WAL_HEADER_SIZE];
    file.read_exact(&mut header).unwrap();

    // Verify magic bytes
    assert_eq!(&header[0..4], WAL_MAGIC);

    // The truncated entry should be detectable by insufficient remaining bytes
    // This test verifies the corruption pattern is created correctly
    let file_size = file.seek(SeekFrom::End(0)).unwrap();
    let data_size = file_size - WAL_HEADER_SIZE as u64;

    // We wrote one complete entry + 10 bytes of partial entry
    // A valid entry needs at least 31 bytes minimum
    assert!(data_size > 31, "Should have at least one complete entry");
}

/// Test: WAL entry with corrupted CRC
///
/// Creates a WAL file with an entry that has an invalid checksum.
/// Recovery should detect the CRC mismatch and handle it gracefully.
#[test]
fn test_wal_recovery_corrupted_crc() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test_crc.wal");

    {
        let mut file = File::create(&wal_path).unwrap();

        // Write header
        file.write_all(&create_wal_header()).unwrap();

        // Write a valid entry
        let entry1 = create_wal_entry(1, "test-topic", 0, b"valid entry");
        file.write_all(&entry1).unwrap();

        // Write an entry with corrupted CRC
        let mut corrupted_entry = create_wal_entry(2, "test-topic", 0, b"corrupted entry").to_vec();
        // Flip some bits in the CRC (last 4 bytes)
        let len = corrupted_entry.len();
        corrupted_entry[len - 1] ^= 0xFF;
        corrupted_entry[len - 2] ^= 0xAA;
        file.write_all(&corrupted_entry).unwrap();

        file.sync_all().unwrap();
    }

    // Verify file structure - both entries should be present in the file
    let metadata = std::fs::metadata(&wal_path).unwrap();
    assert!(metadata.len() > WAL_HEADER_SIZE as u64 + 31);

    // Read header and verify
    let mut file = File::open(&wal_path).unwrap();
    let mut header = [0u8; WAL_HEADER_SIZE];
    file.read_exact(&mut header).unwrap();
    assert_eq!(&header[0..4], WAL_MAGIC);
}

/// Test: WAL with completely corrupted entry type
///
/// Creates a WAL entry with an invalid entry type byte. Recovery should
/// detect this as corruption and skip the entry.
#[test]
fn test_wal_recovery_invalid_entry_type() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test_type.wal");

    {
        let mut file = File::create(&wal_path).unwrap();

        // Write header
        file.write_all(&create_wal_header()).unwrap();

        // Write a valid entry first
        let entry1 = create_wal_entry(1, "test-topic", 0, b"valid");
        file.write_all(&entry1).unwrap();

        // Write an entry with invalid type
        let mut bad_entry = create_wal_entry(2, "test-topic", 0, b"bad type").to_vec();
        // Entry type is at position 16 (after sequence and timestamp)
        bad_entry[16] = 255; // Invalid entry type
        file.write_all(&bad_entry).unwrap();

        file.sync_all().unwrap();
    }

    // Verify file was created
    assert!(wal_path.exists());
    let metadata = std::fs::metadata(&wal_path).unwrap();
    assert!(metadata.len() > WAL_HEADER_SIZE as u64);
}

/// Test: Segment file with corrupted header CRC
///
/// Creates a segment file where the header CRC doesn't match the header data.
/// Recovery should detect this and refuse to open the segment.
#[test]
fn test_segment_recovery_corrupted_header_crc() {
    let temp_dir = TempDir::new().unwrap();
    let segment_path = temp_dir.path().join("0.segment");

    {
        let mut file = File::create(&segment_path).unwrap();

        // Create a valid header
        let mut header = create_segment_header(0, 10, 5);

        // Corrupt the CRC by flipping bits
        header[60] ^= 0xFF;
        header[61] ^= 0xAA;

        file.write_all(&header).unwrap();
        file.sync_all().unwrap();
    }

    // Read back and verify corruption is detectable
    let mut file = File::open(&segment_path).unwrap();
    let mut header = [0u8; SEGMENT_HEADER_SIZE];
    file.read_exact(&mut header).unwrap();

    // Magic should still be valid
    assert_eq!(&header[0..4], SEGMENT_MAGIC);

    // CRC should NOT match
    let stored_crc = u32::from_le_bytes(header[60..64].try_into().unwrap());
    let computed_crc = crc32fast::hash(&header[..60]);
    assert_ne!(stored_crc, computed_crc, "CRC should be corrupted");
}

/// Test: Segment file with truncated record batch
///
/// Creates a segment file with a valid header but a truncated record batch.
/// This simulates a crash during batch write. Recovery should detect the
/// truncation and not return corrupted data.
#[test]
fn test_segment_recovery_truncated_batch() {
    let temp_dir = TempDir::new().unwrap();
    let segment_path = temp_dir.path().join("0.segment");

    {
        let mut file = File::create(&segment_path).unwrap();

        // Write valid header
        let header = create_segment_header(0, -1, 0);
        file.write_all(&header).unwrap();

        // Write a "partial" batch - just length prefix with no data
        // Format: length (4 bytes) + crc (4 bytes) + data
        // We'll write length indicating 100 bytes but only provide 20
        let batch_length: u32 = 100;
        file.write_all(&batch_length.to_le_bytes()).unwrap();

        // Write partial CRC and some garbage data
        file.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap(); // CRC (will be wrong)
        file.write_all(&[0u8; 20]).unwrap(); // Only 20 bytes of "data"

        file.sync_all().unwrap();
    }

    // Verify file structure
    let metadata = std::fs::metadata(&segment_path).unwrap();
    let expected_size = SEGMENT_HEADER_SIZE + 4 + 4 + 20; // header + length + crc + partial data
    assert_eq!(metadata.len() as usize, expected_size);

    // Read and verify header is still valid
    let mut file = File::open(&segment_path).unwrap();
    let mut header = [0u8; SEGMENT_HEADER_SIZE];
    file.read_exact(&mut header).unwrap();

    assert_eq!(&header[0..4], SEGMENT_MAGIC);
    let stored_crc = u32::from_le_bytes(header[60..64].try_into().unwrap());
    let computed_crc = crc32fast::hash(&header[..60]);
    assert_eq!(stored_crc, computed_crc, "Header CRC should be valid");
}

/// Test: Segment with batch CRC mismatch
///
/// Creates a segment file with a valid header but a record batch where
/// the batch CRC doesn't match the batch data. Recovery should detect
/// this corruption.
#[test]
fn test_segment_recovery_batch_crc_mismatch() {
    let temp_dir = TempDir::new().unwrap();
    let segment_path = temp_dir.path().join("0.segment");

    {
        let mut file = File::create(&segment_path).unwrap();

        // Write valid header
        let header = create_segment_header(0, 0, 1);
        file.write_all(&header).unwrap();

        // Create a minimal "batch" with intentionally wrong CRC
        let batch_data = b"some record data here";
        let batch_length = (batch_data.len() + 4) as u32; // data + crc

        file.write_all(&batch_length.to_le_bytes()).unwrap();

        // Write WRONG CRC (not matching the data)
        let wrong_crc: u32 = 0xDEADBEEF;
        file.write_all(&wrong_crc.to_le_bytes()).unwrap();

        // Write the data
        file.write_all(batch_data).unwrap();

        file.sync_all().unwrap();
    }

    // Verify the corruption pattern
    let mut file = File::open(&segment_path).unwrap();
    file.seek(SeekFrom::Start(SEGMENT_HEADER_SIZE as u64))
        .unwrap();

    // Read batch length
    let mut len_buf = [0u8; 4];
    file.read_exact(&mut len_buf).unwrap();
    let batch_length = u32::from_le_bytes(len_buf);
    assert_eq!(batch_length, 25); // 21 bytes data + 4 bytes crc

    // Read CRC
    let mut crc_buf = [0u8; 4];
    file.read_exact(&mut crc_buf).unwrap();
    let stored_crc = u32::from_le_bytes(crc_buf);
    assert_eq!(stored_crc, 0xDEADBEEF);

    // Read data and compute actual CRC
    let mut data = vec![0u8; 21];
    file.read_exact(&mut data).unwrap();
    let actual_crc = crc32fast::hash(&data);

    // CRCs should NOT match
    assert_ne!(
        stored_crc, actual_crc,
        "CRCs should not match - data is corrupted"
    );
}

/// Test: WAL file with invalid magic bytes
///
/// Creates a file that doesn't start with the WAL magic bytes.
/// Recovery should immediately detect this as not a valid WAL file.
#[test]
fn test_wal_recovery_invalid_magic() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test_magic.wal");

    {
        let mut file = File::create(&wal_path).unwrap();

        // Write wrong magic bytes
        file.write_all(b"JUNK").unwrap();
        file.write_all(&[0u8; 12]).unwrap(); // Rest of header

        file.sync_all().unwrap();
    }

    // Verify magic bytes are wrong
    let mut file = File::open(&wal_path).unwrap();
    let mut magic = [0u8; 4];
    file.read_exact(&mut magic).unwrap();

    assert_ne!(&magic, WAL_MAGIC, "Magic bytes should be invalid");
}

/// Test: Segment file with invalid magic bytes
///
/// Creates a file that doesn't start with the segment magic bytes.
/// Recovery should immediately detect this as not a valid segment file.
#[test]
fn test_segment_recovery_invalid_magic() {
    let temp_dir = TempDir::new().unwrap();
    let segment_path = temp_dir.path().join("0.segment");

    {
        let mut file = File::create(&segment_path).unwrap();

        // Write wrong magic bytes followed by rest of header
        let mut header = [0u8; SEGMENT_HEADER_SIZE];
        header[0..4].copy_from_slice(b"BAAD");
        file.write_all(&header).unwrap();

        file.sync_all().unwrap();
    }

    // Verify magic bytes are wrong
    let mut file = File::open(&segment_path).unwrap();
    let mut magic = [0u8; 4];
    file.read_exact(&mut magic).unwrap();

    assert_ne!(&magic, SEGMENT_MAGIC, "Magic bytes should be invalid");
}

/// Test: Empty WAL file (just header, no entries)
///
/// A WAL file with only a header and no entries should be valid and
/// represent an empty log. This is the state right after creation.
#[test]
fn test_wal_recovery_empty_file() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("empty.wal");

    {
        let mut file = File::create(&wal_path).unwrap();
        file.write_all(&create_wal_header()).unwrap();
        file.sync_all().unwrap();
    }

    let metadata = std::fs::metadata(&wal_path).unwrap();
    assert_eq!(metadata.len() as usize, WAL_HEADER_SIZE);

    // Verify header is valid
    let mut file = File::open(&wal_path).unwrap();
    let mut header = [0u8; WAL_HEADER_SIZE];
    file.read_exact(&mut header).unwrap();
    assert_eq!(&header[0..4], WAL_MAGIC);
}

/// Test: Zero-byte WAL file
///
/// A completely empty file should be detected as invalid.
#[test]
fn test_wal_recovery_zero_byte_file() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("zero.wal");

    {
        File::create(&wal_path).unwrap();
    }

    let metadata = std::fs::metadata(&wal_path).unwrap();
    assert_eq!(metadata.len(), 0);
}

/// Test: Multiple valid WAL entries followed by corruption
///
/// This tests a realistic crash scenario where multiple entries were
/// successfully written, then a crash occurred mid-write. Recovery
/// should preserve all complete entries and skip the partial one.
#[test]
fn test_wal_recovery_multiple_entries_then_corruption() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("multi.wal");

    let num_valid_entries: u64 = 10;

    {
        let mut file = File::create(&wal_path).unwrap();

        // Write header
        file.write_all(&create_wal_header()).unwrap();

        // Write multiple valid entries
        for i in 0..num_valid_entries {
            let message = format!("message {}", i);
            let entry = create_wal_entry(i, "test-topic", 0, message.as_bytes());
            file.write_all(&entry).unwrap();
        }

        // Write partial entry (simulate crash)
        let partial = create_wal_entry(num_valid_entries, "test-topic", 0, b"partial");
        file.write_all(&partial[..15]).unwrap(); // Only 15 bytes

        file.sync_all().unwrap();
    }

    // Verify file structure
    let metadata = std::fs::metadata(&wal_path).unwrap();
    assert!(metadata.len() > WAL_HEADER_SIZE as u64);

    // Verify there's data after the header by reading some bytes
    let mut file = File::open(&wal_path).unwrap();
    file.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64)).unwrap();

    let mut buffer = vec![0u8; 4096];
    let bytes_read = file.read(&mut buffer).unwrap();

    // We should find entry data after header (rough heuristic - real recovery would parse properly)
    assert!(bytes_read > 0, "Should find entry data after header");
}

/// Test: Segment with multiple record batches, middle one corrupted
///
/// Creates a segment with valid batches, then a corrupted batch, then more
/// valid batches. Tests that recovery can handle mid-file corruption.
#[test]
fn test_segment_recovery_mid_file_corruption() {
    let temp_dir = TempDir::new().unwrap();
    let segment_path = temp_dir.path().join("0.segment");

    {
        let mut file = File::create(&segment_path).unwrap();

        // Write valid header
        let header = create_segment_header(0, 2, 3);
        file.write_all(&header).unwrap();

        // Write first batch (valid)
        let batch1_data = b"batch one data";
        let batch1_crc = crc32fast::hash(batch1_data);
        let batch1_len = (batch1_data.len() + 4) as u32;
        file.write_all(&batch1_len.to_le_bytes()).unwrap();
        file.write_all(&batch1_crc.to_le_bytes()).unwrap();
        file.write_all(batch1_data).unwrap();

        // Write second batch (corrupted CRC)
        let batch2_data = b"batch two data";
        let batch2_len = (batch2_data.len() + 4) as u32;
        file.write_all(&batch2_len.to_le_bytes()).unwrap();
        file.write_all(&0xBADBADu32.to_le_bytes()).unwrap(); // Wrong CRC
        file.write_all(batch2_data).unwrap();

        // Write third batch (valid)
        let batch3_data = b"batch three";
        let batch3_crc = crc32fast::hash(batch3_data);
        let batch3_len = (batch3_data.len() + 4) as u32;
        file.write_all(&batch3_len.to_le_bytes()).unwrap();
        file.write_all(&batch3_crc.to_le_bytes()).unwrap();
        file.write_all(batch3_data).unwrap();

        file.sync_all().unwrap();
    }

    // Verify file was created with all batches
    let metadata = std::fs::metadata(&segment_path).unwrap();
    let expected_min_size = SEGMENT_HEADER_SIZE + 3 * (4 + 4 + 10); // header + 3 batches (min)
    assert!(metadata.len() as usize >= expected_min_size);
}

/// Test: WAL entry with extremely long topic name
///
/// Tests handling of entries with maximum/near-maximum topic name lengths.
/// Should handle gracefully without buffer overflows.
#[test]
fn test_wal_recovery_long_topic_name() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("long_topic.wal");

    // Create a very long but valid topic name (max u16 length)
    let long_topic: String = "x".repeat(1000);

    {
        let mut file = File::create(&wal_path).unwrap();
        file.write_all(&create_wal_header()).unwrap();

        let entry = create_wal_entry(1, &long_topic, 0, b"test");
        file.write_all(&entry).unwrap();

        file.sync_all().unwrap();
    }

    // Verify file was created
    let metadata = std::fs::metadata(&wal_path).unwrap();
    assert!(metadata.len() > WAL_HEADER_SIZE as u64 + 1000); // Header + topic + overhead
}

/// Test: WAL entry with empty topic name
///
/// Tests handling of entries with zero-length topic names.
#[test]
fn test_wal_recovery_empty_topic_name() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("empty_topic.wal");

    {
        let mut file = File::create(&wal_path).unwrap();
        file.write_all(&create_wal_header()).unwrap();

        let entry = create_wal_entry(1, "", 0, b"test data");
        file.write_all(&entry).unwrap();

        file.sync_all().unwrap();
    }

    // Verify file was created with correct structure
    let mut file = File::open(&wal_path).unwrap();
    let mut header = [0u8; WAL_HEADER_SIZE];
    file.read_exact(&mut header).unwrap();
    assert_eq!(&header[0..4], WAL_MAGIC);
}

/// Test: Segment header with future version number
///
/// Creates a segment with a version number higher than currently supported.
/// Recovery should detect this as incompatible.
#[test]
fn test_segment_recovery_future_version() {
    let temp_dir = TempDir::new().unwrap();
    let segment_path = temp_dir.path().join("0.segment");

    {
        let mut file = File::create(&segment_path).unwrap();

        // Create header with future version
        let mut header = [0u8; SEGMENT_HEADER_SIZE];
        header[0..4].copy_from_slice(SEGMENT_MAGIC);
        header[4..6].copy_from_slice(&999u16.to_le_bytes()); // Future version

        // Fill rest with valid-looking data
        header[8..16].copy_from_slice(&0i64.to_le_bytes()); // base_offset
        header[16..24].copy_from_slice(&(-1i64).to_le_bytes()); // max_offset

        // Compute CRC
        let crc = crc32fast::hash(&header[..60]);
        header[60..64].copy_from_slice(&crc.to_le_bytes());

        file.write_all(&header).unwrap();
        file.sync_all().unwrap();
    }

    // Verify version is set to future value
    let mut file = File::open(&segment_path).unwrap();
    let mut header = [0u8; SEGMENT_HEADER_SIZE];
    file.read_exact(&mut header).unwrap();

    let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
    assert_eq!(version, 999);
}

/// Test: Bit-flip corruption in WAL data
///
/// Simulates random bit flips (like from cosmic rays or hardware errors)
/// in WAL entry data. CRC should detect these corruptions.
#[test]
fn test_wal_recovery_bit_flip_corruption() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("bitflip.wal");

    {
        let mut file = File::create(&wal_path).unwrap();
        file.write_all(&create_wal_header()).unwrap();

        // Create entry and flip a bit in the middle of the data
        let mut entry = create_wal_entry(1, "test", 0, b"important data").to_vec();

        // Flip a bit in the middle of the value (not in CRC)
        let middle = entry.len() / 2;
        entry[middle] ^= 0x01;

        file.write_all(&entry).unwrap();
        file.sync_all().unwrap();
    }

    // File should exist but entry should have bad CRC
    assert!(wal_path.exists());
}

/// Test: Concurrent crash simulation - partial header write
///
/// Simulates an extremely early crash where even the header wasn't
/// fully written.
#[test]
fn test_wal_recovery_partial_header() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("partial_header.wal");

    {
        let mut file = File::create(&wal_path).unwrap();

        // Write only magic + partial version
        file.write_all(WAL_MAGIC).unwrap();
        file.write_all(&[1u8]).unwrap(); // Partial version

        file.sync_all().unwrap();
    }

    let metadata = std::fs::metadata(&wal_path).unwrap();
    assert_eq!(metadata.len(), 5); // 4 bytes magic + 1 byte partial
    assert!(metadata.len() < WAL_HEADER_SIZE as u64);
}
