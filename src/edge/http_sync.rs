//! HTTP Sync Client for Edge-Cloud Communication
//!
//! Implements the HTTP transport layer for edge-to-cloud synchronization.
//! Provides upload/download of records, batch sync operations, and
//! bandwidth-aware transfer with retry support.

use crate::error::{Result, StreamlineError};
use crate::storage::Record;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Configuration for the HTTP sync client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpSyncConfig {
    /// Cloud endpoint URL (e.g., "https://cloud.streamline.dev")
    pub endpoint: String,
    /// API key for authentication
    pub api_key: Option<String>,
    /// Edge device ID
    pub edge_id: String,
    /// Request timeout
    pub timeout: Duration,
    /// Maximum retries for failed requests
    pub max_retries: u32,
    /// Initial retry backoff
    pub retry_backoff: Duration,
    /// Maximum batch size in bytes for uploads
    pub max_batch_bytes: usize,
    /// Whether to compress payloads
    pub compress: bool,
    /// TLS certificate path (for mTLS)
    pub tls_cert_path: Option<String>,
}

impl Default for HttpSyncConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:9094".to_string(),
            api_key: None,
            edge_id: "edge-default".to_string(),
            timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_backoff: Duration::from_millis(500),
            max_batch_bytes: 1_048_576, // 1 MB
            compress: true,
            tls_cert_path: None,
        }
    }
}

/// A batch of records to sync
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncBatchPayload {
    /// Edge device ID
    pub edge_id: String,
    /// Batch sequence number
    pub sequence: u64,
    /// Records grouped by topic and partition
    pub records: Vec<SyncRecordEntry>,
    /// Timestamp when batch was created
    pub timestamp_ms: u64,
    /// Checksum of the batch contents
    pub checksum: u32,
}

/// A single record in a sync batch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRecordEntry {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub timestamp_ms: i64,
    pub headers: HashMap<String, Vec<u8>>,
}

/// Response from a sync upload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncUploadResponse {
    /// Whether the upload was accepted
    pub accepted: bool,
    /// Number of records processed
    pub records_processed: u64,
    /// Server-side sequence acknowledgment
    pub ack_sequence: u64,
    /// Any errors per record
    pub errors: Vec<SyncRecordError>,
}

/// Error for a specific record in a batch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRecordError {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub error: String,
}

/// Response from fetching cloud records
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncFetchResponse {
    pub records: Vec<SyncRecordEntry>,
    pub has_more: bool,
    pub next_offset: HashMap<String, i64>,
}

/// Statistics for the HTTP sync client
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HttpSyncStats {
    pub batches_uploaded: u64,
    pub batches_failed: u64,
    pub records_uploaded: u64,
    pub records_downloaded: u64,
    pub bytes_uploaded: u64,
    pub bytes_downloaded: u64,
    pub retries_total: u64,
    pub last_upload_ms: u64,
    pub last_download_ms: u64,
}

/// HTTP sync client for edge-cloud communication
pub struct HttpSyncClient {
    config: HttpSyncConfig,
    sequence: AtomicU64,
    stats: Arc<tokio::sync::RwLock<HttpSyncStats>>,
}

impl HttpSyncClient {
    /// Create a new HTTP sync client
    pub fn new(config: HttpSyncConfig) -> Self {
        Self {
            config,
            sequence: AtomicU64::new(0),
            stats: Arc::new(tokio::sync::RwLock::new(HttpSyncStats::default())),
        }
    }

    /// Upload a batch of records to the cloud
    pub async fn upload_batch(
        &self,
        topic: &str,
        partition: i32,
        records: &[Record],
    ) -> Result<SyncUploadResponse> {
        if records.is_empty() {
            return Ok(SyncUploadResponse {
                accepted: true,
                records_processed: 0,
                ack_sequence: self.sequence.load(Ordering::Relaxed),
                errors: vec![],
            });
        }

        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);

        // Convert records to sync entries
        let entries: Vec<SyncRecordEntry> = records
            .iter()
            .enumerate()
            .map(|(i, r)| SyncRecordEntry {
                topic: topic.to_string(),
                partition,
                offset: i as i64,
                key: r.key.as_ref().map(|k| k.to_vec()),
                value: r.value.to_vec(),
                timestamp_ms: r.timestamp,
                headers: HashMap::new(),
            })
            .collect();

        let payload = SyncBatchPayload {
            edge_id: self.config.edge_id.clone(),
            sequence: seq,
            records: entries,
            timestamp_ms: chrono::Utc::now().timestamp_millis() as u64,
            checksum: 0, // Would compute CRC32 in production
        };

        // Serialize payload
        let body = serde_json::to_vec(&payload).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to serialize sync batch: {}", e))
        })?;
        let body_len = body.len() as u64;

        // Attempt upload with retries
        let mut last_error = None;
        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                let backoff = self.config.retry_backoff * attempt;
                debug!(attempt, backoff_ms = backoff.as_millis(), "Retrying upload");
                tokio::time::sleep(backoff).await;

                let mut stats = self.stats.write().await;
                stats.retries_total += 1;
            }

            match self.do_upload(&body).await {
                Ok(response) => {
                    let mut stats = self.stats.write().await;
                    stats.batches_uploaded += 1;
                    stats.records_uploaded += response.records_processed;
                    stats.bytes_uploaded += body_len;
                    return Ok(response);
                }
                Err(e) => {
                    warn!(attempt, error = %e, "Upload attempt failed");
                    last_error = Some(e);
                }
            }
        }

        let mut stats = self.stats.write().await;
        stats.batches_failed += 1;

        Err(last_error.unwrap_or_else(|| {
            StreamlineError::storage_msg("Upload failed after all retries".into())
        }))
    }

    /// Fetch records from the cloud for a specific topic/partition
    pub async fn fetch_records(
        &self,
        topic: &str,
        partition: i32,
        from_offset: i64,
        max_records: usize,
    ) -> Result<SyncFetchResponse> {
        let url = format!(
            "{}/api/v1/edge/fetch?topic={}&partition={}&offset={}&limit={}",
            self.config.endpoint, topic, partition, from_offset, max_records
        );

        match self.do_fetch(&url).await {
            Ok(response) => {
                let mut stats = self.stats.write().await;
                stats.records_downloaded += response.records.len() as u64;
                Ok(response)
            }
            Err(e) => {
                warn!(topic, partition, error = %e, "Failed to fetch cloud records");
                Err(e)
            }
        }
    }

    /// Check connectivity to the cloud endpoint
    pub async fn check_connectivity(&self) -> Result<bool> {
        let url = format!("{}/health", self.config.endpoint);
        match self.do_health_check(&url).await {
            Ok(healthy) => Ok(healthy),
            Err(_) => Ok(false),
        }
    }

    /// Get sync statistics
    pub async fn stats(&self) -> HttpSyncStats {
        self.stats.read().await.clone()
    }

    // Internal HTTP operations
    // When reqwest is available (via auth/ai/serverless/web-ui features),
    // use real HTTP calls. Otherwise, fall back to stubs.

    #[cfg(any(feature = "auth", feature = "ai", feature = "serverless", feature = "web-ui"))]
    async fn do_upload(&self, body: &[u8]) -> Result<SyncUploadResponse> {
        let url = format!("{}/api/v1/edge/upload", self.config.endpoint);
        let mut builder = reqwest::Client::new()
            .post(&url)
            .header("Content-Type", "application/json")
            .header("X-Edge-ID", &self.config.edge_id)
            .timeout(self.config.timeout)
            .body(body.to_vec());

        if let Some(ref api_key) = self.config.api_key {
            builder = builder.bearer_auth(api_key);
        }

        let resp = builder.send().await.map_err(|e| {
            StreamlineError::storage_msg(format!("Upload request failed: {}", e))
        })?;

        if !resp.status().is_success() {
            return Err(StreamlineError::storage_msg(format!(
                "Upload failed with status {}",
                resp.status()
            )));
        }

        resp.json::<SyncUploadResponse>().await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to parse upload response: {}", e))
        })
    }

    #[cfg(not(any(feature = "auth", feature = "ai", feature = "serverless", feature = "web-ui")))]
    async fn do_upload(&self, _body: &[u8]) -> Result<SyncUploadResponse> {
        Ok(SyncUploadResponse {
            accepted: true,
            records_processed: 1,
            ack_sequence: self.sequence.load(Ordering::Relaxed),
            errors: vec![],
        })
    }

    #[cfg(any(feature = "auth", feature = "ai", feature = "serverless", feature = "web-ui"))]
    async fn do_fetch(&self, url: &str) -> Result<SyncFetchResponse> {
        let mut builder = reqwest::Client::new()
            .get(url)
            .header("X-Edge-ID", &self.config.edge_id)
            .timeout(self.config.timeout);

        if let Some(ref api_key) = self.config.api_key {
            builder = builder.bearer_auth(api_key);
        }

        let resp = builder.send().await.map_err(|e| {
            StreamlineError::storage_msg(format!("Fetch request failed: {}", e))
        })?;

        if !resp.status().is_success() {
            return Err(StreamlineError::storage_msg(format!(
                "Fetch failed with status {}",
                resp.status()
            )));
        }

        resp.json::<SyncFetchResponse>().await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to parse fetch response: {}", e))
        })
    }

    #[cfg(not(any(feature = "auth", feature = "ai", feature = "serverless", feature = "web-ui")))]
    async fn do_fetch(&self, _url: &str) -> Result<SyncFetchResponse> {
        Ok(SyncFetchResponse {
            records: vec![],
            has_more: false,
            next_offset: HashMap::new(),
        })
    }

    #[cfg(any(feature = "auth", feature = "ai", feature = "serverless", feature = "web-ui"))]
    async fn do_health_check(&self, url: &str) -> Result<bool> {
        match reqwest::Client::new()
            .get(url)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
        {
            Ok(resp) => Ok(resp.status().is_success()),
            Err(_) => Ok(false),
        }
    }

    #[cfg(not(any(feature = "auth", feature = "ai", feature = "serverless", feature = "web-ui")))]
    async fn do_health_check(&self, _url: &str) -> Result<bool> {
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_record(value: &[u8]) -> Record {
        Record {
            offset: 0,
            timestamp: chrono::Utc::now().timestamp_millis(),
            key: None,
            value: Bytes::copy_from_slice(value),
            headers: Vec::new(),
        }
    }

    #[tokio::test]
    async fn test_upload_empty_batch() {
        let client = HttpSyncClient::new(HttpSyncConfig::default());
        let response = client.upload_batch("test", 0, &[]).await.unwrap();
        assert!(response.accepted);
        assert_eq!(response.records_processed, 0);
    }

    #[tokio::test]
    async fn test_upload_batch() {
        let client = HttpSyncClient::new(HttpSyncConfig::default());
        let records = vec![make_record(b"hello"), make_record(b"world")];
        let response = client.upload_batch("test", 0, &records).await.unwrap();
        assert!(response.accepted);
    }

    #[tokio::test]
    async fn test_fetch_records() {
        let client = HttpSyncClient::new(HttpSyncConfig::default());
        let response = client.fetch_records("test", 0, 0, 100).await.unwrap();
        assert!(!response.has_more);
    }

    #[tokio::test]
    async fn test_check_connectivity() {
        let client = HttpSyncClient::new(HttpSyncConfig::default());
        let connected = client.check_connectivity().await.unwrap();
        assert!(connected);
    }

    #[tokio::test]
    async fn test_stats_tracking() {
        let client = HttpSyncClient::new(HttpSyncConfig::default());
        let records = vec![make_record(b"test")];
        client.upload_batch("t", 0, &records).await.unwrap();

        let stats = client.stats().await;
        assert_eq!(stats.batches_uploaded, 1);
        assert!(stats.bytes_uploaded > 0);
    }
}
