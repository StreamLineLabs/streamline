//! Recording storage format for traffic replay

use crate::error::{Result, StreamlineError};
use crate::storage::Record;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

/// Magic bytes for recording file format
#[allow(dead_code)]
pub const RECORDING_MAGIC: &[u8; 4] = b"SREC";

/// Recording file format version
pub const RECORDING_VERSION: u8 = 1;

/// Metadata about a recording
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordingMetadata {
    /// Recording ID
    pub id: String,
    /// Name/description
    pub name: Option<String>,
    /// When recording started (ms since epoch)
    pub start_time: i64,
    /// When recording ended (ms since epoch)
    pub end_time: Option<i64>,
    /// Topics captured
    pub topics: Vec<String>,
    /// Total records captured
    pub record_count: u64,
    /// Total bytes captured
    pub byte_count: u64,
    /// Duration in milliseconds
    pub duration_ms: Option<i64>,
    /// Environment metadata (production, staging, etc.)
    pub environment: Option<String>,
    /// Custom labels
    pub labels: HashMap<String, String>,
    /// Recording format version
    pub version: u8,
}

impl RecordingMetadata {
    /// Create new metadata for a recording
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: None,
            start_time: chrono::Utc::now().timestamp_millis(),
            end_time: None,
            topics: Vec::new(),
            record_count: 0,
            byte_count: 0,
            duration_ms: None,
            environment: None,
            labels: HashMap::new(),
            version: RECORDING_VERSION,
        }
    }

    /// Set recording name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set topics
    pub fn with_topics(mut self, topics: Vec<String>) -> Self {
        self.topics = topics;
        self
    }

    /// Set environment
    pub fn with_environment(mut self, env: impl Into<String>) -> Self {
        self.environment = Some(env.into());
        self
    }

    /// Add a label
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Mark recording as complete
    pub fn complete(&mut self) {
        self.end_time = Some(chrono::Utc::now().timestamp_millis());
        if let Some(end) = self.end_time {
            self.duration_ms = Some(end - self.start_time);
        }
    }

    /// Load metadata from a file
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path).map_err(|e| {
            StreamlineError::storage_msg(format!(
                "Failed to read metadata {}: {}",
                path.display(),
                e
            ))
        })?;
        serde_json::from_str(&content).map_err(|e| {
            StreamlineError::storage_msg(format!(
                "Failed to parse metadata {}: {}",
                path.display(),
                e
            ))
        })
    }

    /// Save metadata to a file
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let content = serde_json::to_string_pretty(self).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to serialize metadata: {}", e))
        })?;
        std::fs::write(path, content).map_err(|e| {
            StreamlineError::storage_msg(format!(
                "Failed to write metadata {}: {}",
                path.display(),
                e
            ))
        })
    }
}

/// A segment of recorded data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordingSegment {
    /// Segment index
    pub index: u32,
    /// File path
    pub path: PathBuf,
    /// Number of records in this segment
    pub record_count: u64,
    /// Byte size of this segment
    pub byte_size: u64,
    /// First record timestamp
    pub first_timestamp: i64,
    /// Last record timestamp
    pub last_timestamp: i64,
}

/// A recorded message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordedMessage {
    /// Capture timestamp (when recorded)
    pub capture_time: i64,
    /// Original topic
    pub topic: String,
    /// Original partition
    pub partition: i32,
    /// Original offset
    pub offset: i64,
    /// Original timestamp
    pub timestamp: i64,
    /// Message key (base64 encoded for binary safety)
    pub key: Option<String>,
    /// Message value (base64 encoded)
    pub value: String,
    /// Headers
    pub headers: Vec<RecordedHeader>,
}

/// A recorded header
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordedHeader {
    pub key: String,
    pub value: String, // base64 encoded
}

impl RecordedMessage {
    /// Create from a Record
    pub fn from_record(record: &Record, topic: &str, partition: i32) -> Self {
        Self {
            capture_time: chrono::Utc::now().timestamp_millis(),
            topic: topic.to_string(),
            partition,
            offset: record.offset,
            timestamp: record.timestamp,
            key: record
                .key
                .as_ref()
                .map(|k| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, k)),
            value: base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                &record.value,
            ),
            headers: record
                .headers
                .iter()
                .map(|h| RecordedHeader {
                    key: h.key.clone(),
                    value: base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        &h.value,
                    ),
                })
                .collect(),
        }
    }

    /// Convert to a Record
    pub fn to_record(&self) -> Result<Record> {
        let key = self
            .key
            .as_ref()
            .map(|k| {
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, k)
                    .map(Bytes::from)
            })
            .transpose()
            .map_err(|e| StreamlineError::storage_msg(format!("Invalid base64 key: {}", e)))?;

        let value = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &self.value)
            .map(Bytes::from)
            .map_err(|e| StreamlineError::storage_msg(format!("Invalid base64 value: {}", e)))?;

        let headers: Result<Vec<_>> = self
            .headers
            .iter()
            .map(|h| {
                let value =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &h.value)
                        .map(Bytes::from)
                        .map_err(|e| {
                            StreamlineError::storage_msg(format!("Invalid base64 header: {}", e))
                        })?;
                Ok(crate::storage::record::Header {
                    key: h.key.clone(),
                    value,
                })
            })
            .collect();

        Ok(Record {
            offset: self.offset,
            timestamp: self.timestamp,
            key,
            value,
            headers: headers?,
            crc: None,
        })
    }
}

/// A complete recording
pub struct Recording {
    /// Recording metadata
    pub metadata: RecordingMetadata,
    /// Base directory
    base_dir: PathBuf,
    /// Segments
    segments: Vec<RecordingSegment>,
    /// Current segment writer
    current_writer: Option<BufWriter<std::fs::File>>,
    /// Current segment index
    current_segment: u32,
    /// Records in current segment
    current_segment_records: u64,
    /// Bytes in current segment
    current_segment_bytes: u64,
    /// Maximum records per segment
    max_records_per_segment: u64,
}

impl Recording {
    /// Create a new recording
    pub fn create(base_dir: impl AsRef<Path>, metadata: RecordingMetadata) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_dir).map_err(|e| {
            StreamlineError::storage_msg(format!(
                "Failed to create recording directory {}: {}",
                base_dir.display(),
                e
            ))
        })?;

        // Save metadata
        let metadata_path = base_dir.join("metadata.json");
        metadata.save(&metadata_path)?;

        Ok(Self {
            metadata,
            base_dir,
            segments: Vec::new(),
            current_writer: None,
            current_segment: 0,
            current_segment_records: 0,
            current_segment_bytes: 0,
            max_records_per_segment: 100_000,
        })
    }

    /// Open an existing recording
    pub fn open(base_dir: impl AsRef<Path>) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();

        if !base_dir.exists() {
            return Err(StreamlineError::storage_msg(format!(
                "Recording not found: {}",
                base_dir.display()
            )));
        }

        let metadata_path = base_dir.join("metadata.json");
        let metadata = RecordingMetadata::load(&metadata_path)?;

        // Find all segment files
        let mut segments = Vec::new();
        for entry in std::fs::read_dir(&base_dir).map_err(|e| {
            StreamlineError::storage_msg(format!(
                "Failed to read recording directory {}: {}",
                base_dir.display(),
                e
            ))
        })? {
            let entry = entry.map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to read directory entry: {}", e))
            })?;
            let path = entry.path();
            if path.extension().map(|e| e == "ndjson").unwrap_or(false) {
                // Parse segment info from filename
                if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(index) = filename.parse::<u32>() {
                        let file_meta = std::fs::metadata(&path).ok();
                        segments.push(RecordingSegment {
                            index,
                            path: path.clone(),
                            record_count: 0, // Will be populated on read
                            byte_size: file_meta.map(|m| m.len()).unwrap_or(0),
                            first_timestamp: 0,
                            last_timestamp: 0,
                        });
                    }
                }
            }
        }

        segments.sort_by_key(|s| s.index);

        let current_segment = segments.last().map(|s| s.index + 1).unwrap_or(0);

        Ok(Self {
            metadata,
            base_dir,
            segments,
            current_writer: None,
            current_segment,
            current_segment_records: 0,
            current_segment_bytes: 0,
            max_records_per_segment: 100_000,
        })
    }

    /// Write a recorded message
    pub fn write(&mut self, message: &RecordedMessage) -> Result<()> {
        // Rotate segment if needed
        if self.current_segment_records >= self.max_records_per_segment {
            self.rotate_segment()?;
        }

        // Ensure we have a writer
        if self.current_writer.is_none() {
            self.open_segment()?;
        }

        let writer = self.current_writer.as_mut().ok_or_else(|| {
            StreamlineError::storage_msg("Segment writer not initialized".to_string())
        })?;

        // Write as NDJSON
        let json = serde_json::to_string(message).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to serialize message: {}", e))
        })?;
        writeln!(writer, "{}", json)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to write message: {}", e)))?;

        let bytes_written = json.len() as u64 + 1; // +1 for newline
        self.current_segment_records += 1;
        self.current_segment_bytes += bytes_written;
        self.metadata.record_count += 1;
        self.metadata.byte_count += bytes_written;

        Ok(())
    }

    /// Open a new segment for writing
    fn open_segment(&mut self) -> Result<()> {
        let segment_path = self
            .base_dir
            .join(format!("{:08}.ndjson", self.current_segment));
        let file = std::fs::File::create(&segment_path).map_err(|e| {
            StreamlineError::storage_msg(format!(
                "Failed to create segment file {}: {}",
                segment_path.display(),
                e
            ))
        })?;
        self.current_writer = Some(BufWriter::new(file));
        Ok(())
    }

    /// Rotate to a new segment
    fn rotate_segment(&mut self) -> Result<()> {
        if let Some(mut writer) = self.current_writer.take() {
            writer.flush().map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to flush segment: {}", e))
            })?;

            // Record segment metadata
            let segment_path = self
                .base_dir
                .join(format!("{:08}.ndjson", self.current_segment));
            self.segments.push(RecordingSegment {
                index: self.current_segment,
                path: segment_path,
                record_count: self.current_segment_records,
                byte_size: self.current_segment_bytes,
                first_timestamp: 0, // Could track this if needed
                last_timestamp: 0,
            });
        }

        self.current_segment += 1;
        self.current_segment_records = 0;
        self.current_segment_bytes = 0;

        Ok(())
    }

    /// Flush and close the recording
    pub fn close(&mut self) -> Result<()> {
        // Flush current segment
        if self.current_writer.is_some() {
            self.rotate_segment()?;
        }

        // Update and save metadata
        self.metadata.complete();
        let metadata_path = self.base_dir.join("metadata.json");
        self.metadata.save(&metadata_path)?;

        Ok(())
    }

    /// Get an iterator over all recorded messages
    pub fn messages(&self) -> Result<RecordingIterator> {
        RecordingIterator::new(&self.segments)
    }

    /// Get the recording path
    pub fn path(&self) -> &Path {
        &self.base_dir
    }
}

/// Iterator over recorded messages
pub struct RecordingIterator {
    segments: Vec<RecordingSegment>,
    current_segment_index: usize,
    current_reader: Option<BufReader<std::fs::File>>,
    line_buffer: String,
}

impl RecordingIterator {
    fn new(segments: &[RecordingSegment]) -> Result<Self> {
        Ok(Self {
            segments: segments.to_vec(),
            current_segment_index: 0,
            current_reader: None,
            line_buffer: String::new(),
        })
    }

    fn open_next_segment(&mut self) -> Result<bool> {
        if self.current_segment_index >= self.segments.len() {
            return Ok(false);
        }

        let segment = &self.segments[self.current_segment_index];
        let file = std::fs::File::open(&segment.path).map_err(|e| {
            StreamlineError::storage_msg(format!(
                "Failed to open segment {}: {}",
                segment.path.display(),
                e
            ))
        })?;
        self.current_reader = Some(BufReader::new(file));
        self.current_segment_index += 1;
        Ok(true)
    }
}

impl Iterator for RecordingIterator {
    type Item = Result<RecordedMessage>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Ensure we have a reader
            if self.current_reader.is_none() {
                match self.open_next_segment() {
                    Ok(true) => {}
                    Ok(false) => return None,
                    Err(e) => return Some(Err(e)),
                }
            }

            let reader = match self.current_reader.as_mut() {
                Some(r) => r,
                None => {
                    return Some(Err(StreamlineError::storage_msg(
                        "Segment reader not initialized".to_string(),
                    )))
                }
            };
            self.line_buffer.clear();

            match reader.read_line(&mut self.line_buffer) {
                Ok(0) => {
                    // End of segment, try next
                    self.current_reader = None;
                    continue;
                }
                Ok(_) => {
                    let line = self.line_buffer.trim();
                    if line.is_empty() {
                        continue;
                    }

                    match serde_json::from_str::<RecordedMessage>(line) {
                        Ok(msg) => return Some(Ok(msg)),
                        Err(e) => {
                            return Some(Err(StreamlineError::storage_msg(format!(
                                "Failed to parse recorded message: {}",
                                e
                            ))))
                        }
                    }
                }
                Err(e) => {
                    return Some(Err(StreamlineError::storage_msg(format!(
                        "Failed to read from segment: {}",
                        e
                    ))))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_recording_metadata() {
        let metadata = RecordingMetadata::new("test-recording")
            .with_name("Test Recording")
            .with_topics(vec!["events".to_string()])
            .with_environment("test")
            .with_label("version", "1.0");

        assert_eq!(metadata.id, "test-recording");
        assert_eq!(metadata.name, Some("Test Recording".to_string()));
        assert_eq!(metadata.topics, vec!["events"]);
    }

    #[test]
    fn test_recorded_message_roundtrip() {
        let record = Record::new(
            42,
            1234567890,
            Some(Bytes::from("key")),
            Bytes::from("value"),
        );

        let recorded = RecordedMessage::from_record(&record, "test-topic", 0);
        assert_eq!(recorded.topic, "test-topic");
        assert_eq!(recorded.offset, 42);

        let restored = recorded.to_record().unwrap();
        assert_eq!(restored.offset, 42);
        assert_eq!(restored.value, Bytes::from("value"));
        assert_eq!(restored.key, Some(Bytes::from("key")));
    }

    #[test]
    fn test_recording_create_and_write() {
        let temp_dir = TempDir::new().unwrap();
        let recording_dir = temp_dir.path().join("recording-001");

        let metadata = RecordingMetadata::new("test").with_topics(vec!["events".to_string()]);

        let mut recording = Recording::create(&recording_dir, metadata).unwrap();

        // Write some messages
        for i in 0..5 {
            let record = Record::new(
                i,
                chrono::Utc::now().timestamp_millis(),
                None,
                Bytes::from(format!("value-{}", i)),
            );
            let msg = RecordedMessage::from_record(&record, "events", 0);
            recording.write(&msg).unwrap();
        }

        recording.close().unwrap();

        // Verify metadata was updated
        assert_eq!(recording.metadata.record_count, 5);
        assert!(recording.metadata.end_time.is_some());
    }

    #[test]
    fn test_recording_open_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let recording_dir = temp_dir.path().join("recording-002");

        // Create a recording
        let metadata = RecordingMetadata::new("test").with_topics(vec!["events".to_string()]);
        let mut recording = Recording::create(&recording_dir, metadata).unwrap();

        for i in 0..10 {
            let record = Record::new(i, 1234567890 + i, None, Bytes::from(format!("value-{}", i)));
            let msg = RecordedMessage::from_record(&record, "events", 0);
            recording.write(&msg).unwrap();
        }
        recording.close().unwrap();

        // Open and read
        let recording = Recording::open(&recording_dir).unwrap();
        let messages: Vec<_> = recording.messages().unwrap().collect();

        assert_eq!(messages.len(), 10);
        for (i, msg_result) in messages.into_iter().enumerate() {
            let msg = msg_result.unwrap();
            assert_eq!(msg.offset, i as i64);
        }
    }
}
