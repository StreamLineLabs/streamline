//! Connection handling for the Kafka protocol handler
//!
//! Contains methods for accepting and managing client connections,
//! including TLS support, pipelined request processing, and zero-copy
//! sendfile optimization.

use crate::error::{Result, StreamlineError};
use crate::protocol::pipeline::PipelineManager;
use crate::protocol::ConnectionContext;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::{KafkaHandler, SessionManager, HARD_MAX_MESSAGE_BYTES};

/// Result of attempting to send a response via sendfile
#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SendfileAttemptResult {
    /// Successfully sent the response via sendfile
    Success,
    /// Sendfile not applicable (no cache hit, TLS, etc.) - use regular write path
    NotApplicable,
    /// Sendfile failed after writing size header - connection is corrupted, must close
    ConnectionCorrupted,
}

impl KafkaHandler {
    /// Handle a client connection
    #[tracing::instrument(skip(self, stream), fields(peer_addr = ?stream.peer_addr().ok()))]
    pub async fn handle_connection(&self, stream: TcpStream) -> Result<()> {
        let peer_addr = stream.peer_addr().ok();
        info!(peer = ?peer_addr, "New Kafka client connection");

        // Extract raw socket fd BEFORE splitting the stream (required for sendfile)
        #[cfg(unix)]
        let ctx = {
            use std::os::unix::io::AsRawFd;
            let socket_fd = stream.as_raw_fd();
            debug!(peer = ?peer_addr, fd = socket_fd, "Extracted socket fd for sendfile");
            ConnectionContext::plain_tcp_with_fd(
                peer_addr,
                self.zerocopy_config.clone(),
                socket_fd,
            )
        };

        #[cfg(not(unix))]
        let ctx = ConnectionContext::plain_tcp(peer_addr, self.zerocopy_config.clone());

        // Create a new session manager for this connection with peer address for ACL checks
        // is_tls=false for plain TCP connections
        let session_manager = SessionManager::with_peer_addr_and_tls(peer_addr, false);

        self.handle_stream(stream, peer_addr, session_manager, ctx)
            .await
    }

    /// Handle a TLS client connection
    #[tracing::instrument(skip(self, stream))]
    pub async fn handle_tls_connection<S>(&self, stream: S) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        info!("New TLS Kafka client connection");
        // Create a new session manager for this connection (no peer address for TLS streams)
        // is_tls=true for TLS-encrypted connections
        let session_manager = SessionManager::with_peer_addr_and_tls(None, true);
        // TLS connection - sendfile disabled automatically
        let ctx = ConnectionContext::tls(None, self.zerocopy_config.clone());
        self.handle_stream(stream, None, session_manager, ctx).await
    }

    /// Handle a stream (generic over TcpStream and TLS stream)
    ///
    /// This implementation supports connection pipelining, allowing multiple
    /// in-flight requests per connection. Responses are sent in correlation ID order.
    #[tracing::instrument(skip(self, stream, session_manager, ctx), fields(peer_addr = ?peer_addr))]
    pub(in crate::protocol) async fn handle_stream<S>(
        &self,
        stream: S,
        peer_addr: Option<std::net::SocketAddr>,
        session_manager: SessionManager,
        ctx: ConnectionContext,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        // Track active connection
        #[cfg(feature = "metrics")]
        ::metrics::gauge!("streamline_active_connections").increment(1.0);

        // Split stream into read and write halves
        let (reader, writer) = tokio::io::split(stream);

        // Create pipeline manager for this connection
        let (pipeline, response_rx) = PipelineManager::new(self.pipeline_config.clone());
        let pipeline = Arc::new(pipeline);

        // Share session manager between tasks
        let session_manager = Arc::new(session_manager);

        // Clone response cache for writer task
        let response_cache = self.response_cache.clone();

        // Spawn writer task with connection context for zero-copy optimizations
        let writer_handle = tokio::spawn(Self::writer_task(
            writer,
            response_rx,
            peer_addr,
            ctx,
            response_cache,
        ));

        // Run reader task (processes requests)
        let reader_result = self
            .reader_task(reader, pipeline.clone(), session_manager, peer_addr)
            .await;

        // Wait for writer to finish
        let writer_result = writer_handle.await;

        // Decrement connection counter when connection closes
        #[cfg(feature = "metrics")]
        ::metrics::gauge!("streamline_active_connections").decrement(1.0);

        // Record pipeline stats
        let stats = pipeline.stats();
        #[cfg(feature = "metrics")]
        ::metrics::gauge!("streamline_pipeline_max_concurrent").set(
            stats
                .max_concurrent
                .load(std::sync::atomic::Ordering::Relaxed) as f64,
        );
        let _ = stats; // Avoid unused warning when metrics disabled

        // Return the first error if any
        reader_result?;
        writer_result
            .map_err(|e| StreamlineError::Server(format!("Writer task panicked: {}", e)))?
    }

    /// Writer task that sends responses in order
    ///
    /// Supports zero-copy optimizations based on connection context:
    /// - Plain TCP: Uses sendfile() for cached responses (zero-copy transfer)
    /// - TLS: Uses regular writes (encryption must happen in user space)
    ///
    /// The sendfile path is used when:
    /// 1. Connection is plain TCP (not TLS)
    /// 2. Response cache is enabled and we have a cache hit
    /// 3. Socket fd was extracted before stream split
    async fn writer_task<W>(
        mut writer: W,
        mut response_rx: mpsc::Receiver<crate::protocol::pipeline::PendingResponse>,
        peer_addr: Option<std::net::SocketAddr>,
        ctx: ConnectionContext,
        response_cache: Option<Arc<crate::protocol::response_cache::ResponseCache>>,
    ) -> Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        // Log connection capabilities
        debug!(
            peer = ?peer_addr,
            is_tls = ctx.is_tls,
            can_sendfile = ctx.can_sendfile(),
            can_mmap = ctx.can_mmap(),
            socket_fd = ?ctx.get_socket_fd(),
            "Writer task started with zero-copy context"
        );

        while let Some(response) = response_rx.recv().await {
            // Try sendfile path for cache hits on plain TCP connections
            #[cfg(unix)]
            let sendfile_result = if ctx.can_sendfile() {
                Self::try_sendfile_response(
                    &mut writer,
                    &response,
                    &ctx,
                    &response_cache,
                    peer_addr,
                )
                .await
            } else {
                SendfileAttemptResult::NotApplicable
            };

            #[cfg(not(unix))]
            let sendfile_result = SendfileAttemptResult::NotApplicable;

            // Dummy enum for non-unix to satisfy the match
            #[cfg(not(unix))]
            #[derive(PartialEq)]
            enum SendfileAttemptResult {
                NotApplicable,
                Success,
                ConnectionCorrupted,
            }

            match sendfile_result {
                SendfileAttemptResult::Success => continue,
                SendfileAttemptResult::ConnectionCorrupted => {
                    // Connection is in a bad state - we wrote the size header but couldn't
                    // complete the sendfile. The client will see a truncated response.
                    // We must close the connection to avoid further corruption.
                    warn!(peer = ?peer_addr, "Closing connection due to sendfile failure");
                    break;
                }
                SendfileAttemptResult::NotApplicable => {
                    // Fall through to regular write path
                }
            }

            // Regular write path (TLS, cache miss, or sendfile not available)
            let response_size = response.data.len() as u32;
            if let Err(e) = writer.write_all(&response_size.to_be_bytes()).await {
                debug!(peer = ?peer_addr, error = %e, "Failed to write response size");
                break;
            }

            // Write response body
            if let Err(e) = writer.write_all(&response.data).await {
                debug!(peer = ?peer_addr, error = %e, "Failed to write response body");
                break;
            }

            if let Err(e) = writer.flush().await {
                debug!(peer = ?peer_addr, error = %e, "Failed to flush response");
                break;
            }

            #[cfg(feature = "metrics")]
            ::metrics::counter!("streamline_bytes_sent_total").increment(response_size as u64 + 4);

            // Cache the response for future sendfile use (if cache is enabled and has key)
            if let (Some(cache), Some(cache_key)) = (&response_cache, response.cache_key) {
                if ctx.zerocopy_config.enable_response_cache {
                    if let Err(e) = cache.put(cache_key, &response.data) {
                        debug!(peer = ?peer_addr, error = %e, "Failed to cache response");
                    }
                }
            }
        }

        Ok(())
    }

    /// Try to send a cached response using sendfile (Unix only)
    ///
    /// Returns `SendfileAttemptResult` indicating the outcome:
    /// - `Success`: Response was sent via sendfile, continue to next response
    /// - `NotApplicable`: Sendfile not used, fall back to regular write path
    /// - `ConnectionCorrupted`: Sendfile failed after writing size header, connection must close
    #[cfg(unix)]
    async fn try_sendfile_response<W>(
        writer: &mut W,
        response: &crate::protocol::pipeline::PendingResponse,
        ctx: &ConnectionContext,
        response_cache: &Option<Arc<crate::protocol::response_cache::ResponseCache>>,
        peer_addr: Option<std::net::SocketAddr>,
    ) -> SendfileAttemptResult
    where
        W: AsyncWrite + Unpin,
    {
        use crate::storage::zerocopy::sendfile_async;

        // Need a cache key and cache to check for hit
        let cache_key = match &response.cache_key {
            Some(key) => key,
            None => return SendfileAttemptResult::NotApplicable,
        };

        let cache = match response_cache {
            Some(c) => c,
            None => return SendfileAttemptResult::NotApplicable,
        };

        // Check for cache hit
        let cached = match cache.get(cache_key) {
            Some(c) => c,
            None => return SendfileAttemptResult::NotApplicable,
        };

        // Get socket fd
        let socket_fd = match ctx.get_socket_fd() {
            Some(fd) => fd,
            None => return SendfileAttemptResult::NotApplicable,
        };

        // Verify we can open the cached file before committing to sendfile path
        if let Err(e) = cached.open() {
            debug!(peer = ?peer_addr, error = %e, "Failed to open cached file for sendfile");
            return SendfileAttemptResult::NotApplicable;
        }

        // === POINT OF NO RETURN ===
        // After this point, we've committed to sendfile. Any failure means the
        // connection is corrupted because we've written the size header but won't
        // be able to send the full body.

        // Write response size header first (4 bytes, big-endian)
        // We must do this before sendfile since Kafka protocol requires size prefix
        let response_size = cached.size as u32;
        if let Err(e) = writer.write_all(&response_size.to_be_bytes()).await {
            debug!(peer = ?peer_addr, error = %e, "Failed to write response size for sendfile");
            // Size header write failed, connection may still be okay for fallback
            return SendfileAttemptResult::NotApplicable;
        }

        // Flush the writer to ensure size header is sent before sendfile
        if let Err(e) = writer.flush().await {
            warn!(
                peer = ?peer_addr,
                error = %e,
                "Sendfile flush failed after writing size header - connection corrupted"
            );
            #[cfg(feature = "metrics")]
            ::metrics::counter!("streamline_zerocopy_sendfile_connection_corrupted_total")
                .increment(1);
            return SendfileAttemptResult::ConnectionCorrupted;
        }

        // Use sendfile to transfer the cached response body
        let total_size = cached.size as usize;
        let mut bytes_sent: usize = 0;

        // Loop to handle partial writes (sendfile may not send all bytes at once)
        while bytes_sent < total_size {
            // Need to reopen file for each sendfile call since sendfile_async takes ownership
            let file = match cached.open() {
                Ok(f) => f,
                Err(e) => {
                    warn!(
                        peer = ?peer_addr,
                        error = %e,
                        bytes_sent = bytes_sent,
                        total_size = total_size,
                        "Failed to reopen cached file mid-sendfile - connection corrupted"
                    );
                    #[cfg(feature = "metrics")]
                    ::metrics::counter!("streamline_zerocopy_sendfile_connection_corrupted_total")
                        .increment(1);
                    return SendfileAttemptResult::ConnectionCorrupted;
                }
            };

            match sendfile_async(socket_fd, file, bytes_sent as u64, total_size - bytes_sent).await
            {
                Ok(result) => {
                    if result.bytes_sent == 0 {
                        // No progress made, avoid infinite loop
                        warn!(
                            peer = ?peer_addr,
                            bytes_sent = bytes_sent,
                            total_size = total_size,
                            "Sendfile returned 0 bytes mid-transfer - connection corrupted"
                        );
                        #[cfg(feature = "metrics")]
                        ::metrics::counter!(
                            "streamline_zerocopy_sendfile_connection_corrupted_total"
                        )
                        .increment(1);
                        return SendfileAttemptResult::ConnectionCorrupted;
                    }
                    bytes_sent += result.bytes_sent;
                    debug!(
                        peer = ?peer_addr,
                        sent = result.bytes_sent,
                        total_sent = bytes_sent,
                        total_size = total_size,
                        "Sendfile progress"
                    );
                }
                Err(e) => {
                    warn!(
                        peer = ?peer_addr,
                        error = %e,
                        bytes_sent = bytes_sent,
                        total_size = total_size,
                        "Sendfile failed mid-transfer - connection corrupted"
                    );
                    #[cfg(feature = "metrics")]
                    ::metrics::counter!("streamline_zerocopy_sendfile_connection_corrupted_total")
                        .increment(1);
                    return SendfileAttemptResult::ConnectionCorrupted;
                }
            }
        }

        // Successfully sent entire response via sendfile
        debug!(
            peer = ?peer_addr,
            bytes = total_size,
            "Sent cached response via sendfile (zero-copy)"
        );
        #[cfg(feature = "metrics")]
        {
            ::metrics::counter!("streamline_bytes_sent_total").increment(response_size as u64 + 4);
            ::metrics::counter!("streamline_zerocopy_sendfile_responses_total").increment(1);
        }

        SendfileAttemptResult::Success
    }

    /// Reader task that reads requests and spawns processing tasks
    async fn reader_task<R>(
        &self,
        mut reader: R,
        pipeline: Arc<PipelineManager>,
        session_manager: Arc<SessionManager>,
        peer_addr: Option<std::net::SocketAddr>,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin,
    {
        use kafka_protocol::messages::ApiKey;

        // Track correlation IDs for ordering
        let mut next_correlation_id: i32 = 0;

        loop {
            // Read message size (4 bytes, big-endian)
            let mut size_buf = [0u8; 4];
            match reader.read_exact(&mut size_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    debug!(peer = ?peer_addr, "Client disconnected");
                    break;
                }
                Err(e) => {
                    error!(peer = ?peer_addr, error = %e, "Failed to read message size");
                    return Err(e.into());
                }
            }

            let message_size = u32::from_be_bytes(size_buf) as usize;
            #[cfg(feature = "metrics")]
            ::metrics::counter!("streamline_bytes_received_total")
                .increment(message_size as u64 + 4);

            // Enforce hard cap regardless of configuration to prevent DoS
            if message_size as u64 > HARD_MAX_MESSAGE_BYTES {
                error!(
                    peer = ?peer_addr,
                    size = message_size,
                    hard_cap = HARD_MAX_MESSAGE_BYTES,
                    "Message exceeds hard size limit (potential DoS attempt)"
                );
                return Err(StreamlineError::protocol_msg(format!(
                    "Message size {} exceeds hard limit {}",
                    message_size, HARD_MAX_MESSAGE_BYTES
                )));
            }

            if message_size as u64 > self.max_message_bytes {
                error!(
                    peer = ?peer_addr,
                    size = message_size,
                    max = self.max_message_bytes,
                    "Message too large"
                );
                return Err(StreamlineError::protocol_msg(format!(
                    "Message size {} exceeds maximum {}",
                    message_size, self.max_message_bytes
                )));
            }

            // Check against resource limiter if configured
            if let Some(ref limiter) = self.resource_limiter {
                if let Err(e) = limiter.check_request_size(message_size as u64) {
                    warn!(
                        peer = ?peer_addr,
                        size = message_size,
                        "Request rejected by resource limiter: {:?}",
                        e
                    );
                    return Err(StreamlineError::protocol_msg(format!(
                        "Request rejected: {:?}",
                        e
                    )));
                }
            }

            // Read message body using try_reserve to handle allocation failure gracefully
            // This prevents panic on OOM and allows us to return a proper error
            let mut message_buf = Vec::new();
            if let Err(e) = message_buf.try_reserve(message_size) {
                error!(
                    peer = ?peer_addr,
                    size = message_size,
                    error = %e,
                    "Failed to allocate buffer for message (OOM protection)"
                );
                return Err(StreamlineError::protocol_msg(format!(
                    "Failed to allocate {} bytes for message: {}",
                    message_size, e
                )));
            }
            message_buf.resize(message_size, 0);
            reader.read_exact(&mut message_buf).await?;

            // Extract correlation ID from the request header for ordering
            // Correlation ID is at bytes 4-8 (after api_key and api_version)
            let correlation_id = if message_buf.len() >= 8 {
                i32::from_be_bytes([
                    message_buf[4],
                    message_buf[5],
                    message_buf[6],
                    message_buf[7],
                ])
            } else {
                // Fallback to sequential IDs if header is malformed
                let id = next_correlation_id;
                next_correlation_id = next_correlation_id.wrapping_add(1);
                id
            };

            // Acquire pipeline permit (provides backpressure) and get sequence number
            // The sequence number preserves the order in which requests were received
            // Returns early if the server is shutting down
            let (permit, seq) = pipeline.acquire_permit().await?;

            // Clone references for the processing task
            let handler = self.clone_for_task();
            let session_manager = session_manager.clone();
            let pipeline = pipeline.clone();

            // Spawn task to process this request
            tokio::spawn(async move {
                let _permit = permit; // Hold permit until task completes

                let start = std::time::Instant::now();
                let response = handler
                    .process_message(&message_buf, &session_manager)
                    .await;
                let duration = start.elapsed();

                #[cfg(feature = "metrics")]
                {
                    ::metrics::histogram!("streamline_request_duration_seconds")
                        .record(duration.as_secs_f64());
                    ::metrics::counter!("streamline_requests_total").increment(1);
                }
                let _ = duration; // Avoid unused warning when metrics disabled

                // Submit response (will be sent in sequence order, not correlation ID order)
                // This ensures responses are sent in the same order requests were received
                match response {
                    Ok(resp) => {
                        // Use submit_response_with_cache_key to enable sendfile optimization
                        // for fetch responses that have a cache key
                        pipeline
                            .submit_response_with_cache_key(
                                seq,
                                correlation_id,
                                resp.data,
                                resp.cache_key,
                            )
                            .await;
                    }
                    Err(e) => {
                        // For errors, we still need to send something to maintain ordering
                        // Create a proper error response so clients can understand what happened
                        warn!(correlation_id, error = %e, "Request processing failed");

                        // Extract API key and version from the message to create proper error response
                        let (api_key, api_version) = if message_buf.len() >= 4 {
                            let api_key = i16::from_be_bytes([message_buf[0], message_buf[1]]);
                            let api_version = i16::from_be_bytes([message_buf[2], message_buf[3]]);
                            (api_key, api_version)
                        } else {
                            // Malformed message - use ApiVersions as fallback
                            // SECURITY: Log this to detect potential protocol attacks or buggy clients
                            warn!(
                                correlation_id,
                                message_len = message_buf.len(),
                                "Malformed message too short to extract API key/version, using ApiVersions v0 fallback"
                            );
                            (ApiKey::ApiVersions as i16, 0)
                        };

                        // Map error to appropriate Kafka protocol error code
                        let error_code = e.kafka_error_code().as_i16();

                        // Create a proper Kafka error response
                        let error_response = match KafkaHandler::create_error_response(
                            api_key,
                            api_version,
                            correlation_id,
                            error_code,
                        ) {
                            Ok(response) => response,
                            Err(err) => {
                                warn!(
                                    correlation_id,
                                    error = %err,
                                    "Failed to encode Kafka error response"
                                );
                                Vec::new()
                            }
                        };

                        pipeline
                            .submit_response(seq, correlation_id, error_response)
                            .await;
                    }
                }
            });
        }

        Ok(())
    }
}
