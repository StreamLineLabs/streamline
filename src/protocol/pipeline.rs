//! Request pipelining for Kafka protocol connections
//!
//! This module implements connection pipelining to allow multiple in-flight
//! requests per connection, significantly improving throughput for clients
//! that can send requests without waiting for responses.
//!
//! The pipeline manager tracks requests by correlation ID and ensures responses
//! are sent back in the correct order (matching correlation IDs).

// Request pipelining scaffolding (not yet fully integrated)
#![allow(dead_code)]
#![allow(clippy::type_complexity)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Semaphore};

use crate::error::{Result, StreamlineError};

/// Configuration for connection pipelining
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Maximum number of in-flight requests per connection
    pub max_in_flight: usize,
    /// Size of the response buffer channel
    pub response_buffer_size: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 10,
            response_buffer_size: 100,
        }
    }
}

/// A pending request being processed
#[derive(Debug)]
pub(crate) struct PendingRequest {
    /// The correlation ID from the request header
    pub correlation_id: i32,
    /// The raw request data
    pub data: Vec<u8>,
}

/// A completed response ready to be sent
#[derive(Debug)]
pub(crate) struct PendingResponse {
    /// The correlation ID this response is for
    pub correlation_id: i32,
    /// The serialized response data (including size prefix)
    pub data: Vec<u8>,
    /// Optional cache key for response caching (used for fetch responses)
    pub cache_key: Option<super::response_cache::CacheKey>,
}

/// Statistics for a pipeline
#[derive(Debug, Default)]
pub(crate) struct PipelineStats {
    /// Total requests processed
    pub requests_processed: AtomicU64,
    /// Total responses sent
    pub responses_sent: AtomicU64,
    /// Current number of in-flight requests
    pub in_flight: AtomicU64,
    /// Maximum concurrent requests seen
    pub max_concurrent: AtomicU64,
}

impl PipelineStats {
    /// Create new empty stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a request starting processing
    pub fn request_started(&self) {
        self.requests_processed.fetch_add(1, Ordering::Relaxed);
        let current = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;

        // Update max concurrent if needed
        let mut max = self.max_concurrent.load(Ordering::Relaxed);
        while current > max {
            match self.max_concurrent.compare_exchange_weak(
                max,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(m) => max = m,
            }
        }
    }

    /// Record a request completing
    pub fn request_completed(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a response being sent
    pub fn response_sent(&self) {
        self.responses_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current in-flight count
    pub fn current_in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }
}

/// Manages request pipelining for a single connection
///
/// The pipeline manager:
/// - Tracks pending responses by sequence number (order received)
/// - Ensures responses are sent in the same order requests were received
/// - Provides backpressure via semaphore when too many requests are in flight
///
/// Note: Kafka protocol requires responses to be sent in the same order as
/// requests were received. We use sequence numbers for ordering, not correlation
/// IDs, because correlation IDs may not be sequential (clients can skip values).
pub(crate) struct PipelineManager {
    /// Configuration
    config: PipelineConfig,

    /// Semaphore for limiting in-flight requests
    in_flight_semaphore: Arc<Semaphore>,

    /// Channel for sending completed responses to the writer task
    response_tx: mpsc::Sender<PendingResponse>,

    /// Buffer for out-of-order responses, keyed by sequence number.
    /// The value is (correlation_id, data).
    ///
    /// Design note: BTreeMap is used instead of HashMap because:
    /// 1. We need to iterate in sequence order when flushing buffered responses
    /// 2. For small numbers of entries (max_in_flight is typically 10), BTreeMap
    ///    has better cache locality than HashMap
    /// 3. BTreeMap's O(log n) operations are negligible for small n
    ///
    /// The tuple stores: (correlation_id, data, optional_cache_key)
    response_buffer:
        Arc<Mutex<BTreeMap<u64, (i32, Vec<u8>, Option<super::response_cache::CacheKey>)>>>,

    /// Next sequence number to assign to incoming requests.
    /// Uses AtomicU64 for lock-free increment in the hot path.
    next_request_seq: AtomicU64,

    /// Next sequence number expected for sending responses.
    /// Kept as Mutex because it must be coordinated with response_buffer.
    next_send_seq: Arc<Mutex<u64>>,

    /// Statistics
    stats: Arc<PipelineStats>,
}

impl PipelineManager {
    /// Create a new pipeline manager with the given configuration
    pub fn new(config: PipelineConfig) -> (Self, mpsc::Receiver<PendingResponse>) {
        let (response_tx, response_rx) = mpsc::channel(config.response_buffer_size);

        let manager = Self {
            in_flight_semaphore: Arc::new(Semaphore::new(config.max_in_flight)),
            config,
            response_tx,
            response_buffer: Arc::new(Mutex::new(BTreeMap::new())),
            next_request_seq: AtomicU64::new(0),
            next_send_seq: Arc::new(Mutex::new(0)),
            stats: Arc::new(PipelineStats::new()),
        };

        (manager, response_rx)
    }

    /// Create with default configuration
    pub fn with_defaults() -> (Self, mpsc::Receiver<PendingResponse>) {
        Self::new(PipelineConfig::default())
    }

    /// Acquire a permit for an in-flight request and get the sequence number
    ///
    /// This provides backpressure - if too many requests are in flight,
    /// this will wait until a slot is available. Returns the sequence number
    /// that must be used when submitting the response.
    ///
    /// Returns an error if the server is shutting down (semaphore closed).
    pub async fn acquire_permit(&self) -> Result<(InFlightPermit, u64)> {
        let permit = self
            .in_flight_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StreamlineError::ShuttingDown)?;

        self.stats.request_started();

        // Assign sequence number atomically - lock-free for better performance
        let seq = self.next_request_seq.fetch_add(1, Ordering::Relaxed);

        Ok((
            InFlightPermit {
                _permit: permit,
                stats: self.stats.clone(),
            },
            seq,
        ))
    }

    /// Try to acquire a permit without waiting
    pub async fn try_acquire_permit(&self) -> Option<(InFlightPermit, u64)> {
        self.in_flight_semaphore
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| {
                self.stats.request_started();
                // Futures can't be used in map, so we need to handle this differently
                // For simplicity, we'll just return None if try_acquire succeeds
                // but we can't get the sequence number synchronously
                (
                    InFlightPermit {
                        _permit: permit,
                        stats: self.stats.clone(),
                    },
                    0, // Placeholder - need to get actual seq
                )
            })
    }

    /// Assign a sequence number for a request (call before spawning task)
    /// This is now synchronous since we use AtomicU64 for lock-free operation.
    pub fn assign_sequence(&self) -> u64 {
        self.next_request_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Submit a completed response with its sequence number
    ///
    /// Responses are buffered and sent in sequence order (order received).
    /// The lock is released before sending to avoid holding it across await points,
    /// which could cause contention and potential priority inversion issues.
    pub async fn submit_response(&self, seq: u64, correlation_id: i32, data: Vec<u8>) {
        self.submit_response_with_cache_key(seq, correlation_id, data, None)
            .await;
    }

    /// Submit a completed response with its sequence number and optional cache key
    ///
    /// The cache key is used for sendfile optimization. When present, the response
    /// will be cached and can be sent via zero-copy sendfile on subsequent requests.
    pub async fn submit_response_with_cache_key(
        &self,
        seq: u64,
        correlation_id: i32,
        data: Vec<u8>,
        cache_key: Option<super::response_cache::CacheKey>,
    ) {
        // Collect responses to send while holding the lock, then release before sending.
        // This avoids holding the mutex across await points which could cause contention.
        let responses_to_send = {
            let mut buffer = self.response_buffer.lock().await;
            let mut next_send = self.next_send_seq.lock().await;

            let mut responses = Vec::new();

            // Check if this is the next expected response
            if seq == *next_send {
                // This response is ready to send immediately
                responses.push(PendingResponse {
                    correlation_id,
                    data,
                    cache_key,
                });
                *next_send += 1;

                // Collect any buffered responses that are now in order
                while let Some((buffered_corr_id, buffered_data, buffered_cache_key)) =
                    buffer.remove(&*next_send)
                {
                    responses.push(PendingResponse {
                        correlation_id: buffered_corr_id,
                        data: buffered_data,
                        cache_key: buffered_cache_key,
                    });
                    *next_send += 1;
                }
            } else {
                // Buffer for later - store correlation_id, data, and cache_key
                buffer.insert(seq, (correlation_id, data, cache_key));
            }

            responses
            // Locks are released here at end of scope
        };

        // Send responses without holding any locks
        for response in responses_to_send {
            if self.response_tx.send(response).await.is_ok() {
                self.stats.response_sent();
            }
        }
    }

    /// Get the number of currently in-flight requests
    pub fn in_flight_count(&self) -> usize {
        self.config.max_in_flight - self.in_flight_semaphore.available_permits()
    }

    /// Get pipeline statistics
    pub fn stats(&self) -> &PipelineStats {
        &self.stats
    }

    /// Get the configuration
    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    /// Reset the sequence numbers (for connection reset/reconnect)
    pub async fn reset(&self) {
        let mut buffer = self.response_buffer.lock().await;
        let mut next_send = self.next_send_seq.lock().await;

        buffer.clear();
        self.next_request_seq.store(0, Ordering::Relaxed);
        *next_send = 0;
    }
}

/// RAII guard for an in-flight request
///
/// Automatically releases the semaphore permit and updates stats when dropped.
pub(crate) struct InFlightPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    stats: Arc<PipelineStats>,
}

impl Drop for InFlightPermit {
    fn drop(&mut self) {
        self.stats.request_completed();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pipeline_config_default() {
        let config = PipelineConfig::default();
        assert_eq!(config.max_in_flight, 10);
        assert_eq!(config.response_buffer_size, 100);
    }

    #[tokio::test]
    async fn test_pipeline_manager_creation() {
        let (manager, _rx) = PipelineManager::with_defaults();
        assert_eq!(manager.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn test_pipeline_permit_acquisition() {
        let config = PipelineConfig {
            max_in_flight: 2,
            response_buffer_size: 10,
        };
        let (manager, _rx) = PipelineManager::new(config);

        // Acquire first permit - should get seq 0
        let (_permit1, seq1) = manager.acquire_permit().await.unwrap();
        assert_eq!(manager.in_flight_count(), 1);
        assert_eq!(seq1, 0);

        // Acquire second permit - should get seq 1
        let (_permit2, seq2) = manager.acquire_permit().await.unwrap();
        assert_eq!(manager.in_flight_count(), 2);
        assert_eq!(seq2, 1);

        // Third permit should not be immediately available (semaphore full)
        assert!(manager.try_acquire_permit().await.is_none());

        // Drop first permit
        drop(_permit1);
        assert_eq!(manager.in_flight_count(), 1);

        // Now we can acquire another - should get seq 2
        let result = manager.try_acquire_permit().await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_pipeline_response_ordering() {
        let config = PipelineConfig {
            max_in_flight: 10,
            response_buffer_size: 10,
        };
        let (manager, mut rx) = PipelineManager::new(config);

        // Submit responses out of order by sequence number
        // Correlation IDs can be any values (not necessarily sequential)
        // seq=0 -> corr_id=100, seq=1 -> corr_id=50, seq=2 -> corr_id=200
        manager.submit_response(0, 100, vec![100]).await; // First - seq 0 sent immediately
        manager.submit_response(2, 200, vec![200]).await; // Buffered (waiting for seq 1)
        manager.submit_response(1, 50, vec![50]).await; // Triggers sending seq 1 then 2

        // Responses should come back in sequence order (0, 1, 2)
        // with their original correlation IDs preserved
        let resp0 = rx.recv().await.unwrap();
        assert_eq!(resp0.correlation_id, 100);
        assert_eq!(resp0.data, vec![100]);

        let resp1 = rx.recv().await.unwrap();
        assert_eq!(resp1.correlation_id, 50);
        assert_eq!(resp1.data, vec![50]);

        let resp2 = rx.recv().await.unwrap();
        assert_eq!(resp2.correlation_id, 200);
        assert_eq!(resp2.data, vec![200]);
    }

    #[tokio::test]
    async fn test_pipeline_stats() {
        let config = PipelineConfig {
            max_in_flight: 5,
            response_buffer_size: 10,
        };
        let (manager, _rx) = PipelineManager::new(config);

        // Acquire some permits
        let (permit1, _seq1) = manager.acquire_permit().await.unwrap();
        let (permit2, _seq2) = manager.acquire_permit().await.unwrap();

        assert_eq!(manager.stats().current_in_flight(), 2);
        assert_eq!(
            manager.stats().requests_processed.load(Ordering::Relaxed),
            2
        );

        // Release one
        drop(permit1);
        assert_eq!(manager.stats().current_in_flight(), 1);

        // Max concurrent should still be 2
        assert_eq!(manager.stats().max_concurrent.load(Ordering::Relaxed), 2);

        drop(permit2);
    }

    #[tokio::test]
    async fn test_pipeline_reset() {
        let config = PipelineConfig {
            max_in_flight: 10,
            response_buffer_size: 10,
        };
        let (manager, mut rx) = PipelineManager::new(config);

        // Submit first response with seq=0 and corr_id=5
        manager.submit_response(0, 5, vec![5]).await;

        // Receive it
        let resp = rx.recv().await.unwrap();
        assert_eq!(resp.correlation_id, 5);

        // Reset - sequence numbers go back to 0
        manager.reset().await;

        // Now submit again with seq=0 (reset) and corr_id=100
        manager.submit_response(0, 100, vec![100]).await;

        let resp = rx.recv().await.unwrap();
        assert_eq!(resp.correlation_id, 100);
    }
}
