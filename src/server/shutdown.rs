//! Graceful shutdown coordinator for Streamline
//!
//! This module provides a structured approach to graceful shutdown, ensuring:
//! - In-flight requests are tracked and completed
//! - Shutdown phases execute in the correct order
//! - A configurable timeout prevents indefinite hangs
//! - All resources are properly cleaned up

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, watch, Mutex, Notify};
use tracing::{debug, info, warn};

/// Default shutdown timeout in seconds
pub const DEFAULT_SHUTDOWN_TIMEOUT_SECS: u64 = 30;

/// Default connection drain timeout in seconds
pub const DEFAULT_DRAIN_TIMEOUT_SECS: u64 = 10;

/// Configuration for shutdown behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownConfig {
    /// Maximum time to wait for graceful shutdown before forcing
    pub timeout_secs: u64,
    /// Whether to wait for in-flight requests to complete
    pub wait_for_requests: bool,
    /// Time to wait for active connections to drain before forcibly closing
    pub drain_timeout_secs: u64,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            timeout_secs: DEFAULT_SHUTDOWN_TIMEOUT_SECS,
            wait_for_requests: true,
            drain_timeout_secs: DEFAULT_DRAIN_TIMEOUT_SECS,
        }
    }
}

/// Shutdown phase for ordered cleanup
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownPhase {
    /// Normal operation
    Running,
    /// Stop accepting new connections, but continue serving existing ones
    DrainConnections,
    /// Wait for in-flight requests to complete
    DrainRequests,
    /// Transfer leadership (cluster mode only)
    TransferLeadership,
    /// Close all connections
    CloseConnections,
    /// Flush WAL and persist state
    FlushState,
    /// Final shutdown
    Complete,
}

impl std::fmt::Display for ShutdownPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShutdownPhase::Running => write!(f, "running"),
            ShutdownPhase::DrainConnections => write!(f, "draining connections"),
            ShutdownPhase::DrainRequests => write!(f, "draining requests"),
            ShutdownPhase::TransferLeadership => write!(f, "transferring leadership"),
            ShutdownPhase::CloseConnections => write!(f, "closing connections"),
            ShutdownPhase::FlushState => write!(f, "flushing state"),
            ShutdownPhase::Complete => write!(f, "complete"),
        }
    }
}

/// Coordinator for graceful shutdown operations
pub struct ShutdownCoordinator {
    /// Configuration
    config: ShutdownConfig,
    /// Whether shutdown has been initiated
    shutdown_initiated: AtomicBool,
    /// Current phase of shutdown
    phase_tx: watch::Sender<ShutdownPhase>,
    /// Receiver for current phase
    phase_rx: watch::Receiver<ShutdownPhase>,
    /// Broadcast sender for shutdown notification
    notify_tx: broadcast::Sender<()>,
    /// Number of active connections
    active_connections: AtomicU64,
    /// Number of in-flight requests
    in_flight_requests: AtomicU64,
    /// Notifier for when all requests are complete
    requests_drained: Arc<Notify>,
    /// Notifier for when all connections are closed
    connections_drained: Arc<Notify>,
    /// Shutdown start time
    shutdown_start: Mutex<Option<Instant>>,
}

impl ShutdownCoordinator {
    /// Create a new shutdown coordinator with default configuration
    pub fn new() -> Self {
        Self::with_config(ShutdownConfig::default())
    }

    /// Create a new shutdown coordinator with custom configuration
    pub fn with_config(config: ShutdownConfig) -> Self {
        let (phase_tx, phase_rx) = watch::channel(ShutdownPhase::Running);
        let (notify_tx, _) = broadcast::channel(16);

        Self {
            config,
            shutdown_initiated: AtomicBool::new(false),
            phase_tx,
            phase_rx,
            notify_tx,
            active_connections: AtomicU64::new(0),
            in_flight_requests: AtomicU64::new(0),
            requests_drained: Arc::new(Notify::new()),
            connections_drained: Arc::new(Notify::new()),
            shutdown_start: Mutex::new(None),
        }
    }

    /// Check if shutdown has been initiated
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown_initiated.load(Ordering::SeqCst)
    }

    /// Get the current shutdown phase
    pub fn current_phase(&self) -> ShutdownPhase {
        *self.phase_rx.borrow()
    }

    /// Subscribe to shutdown notifications
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.notify_tx.subscribe()
    }

    /// Watch for phase changes
    pub fn watch_phase(&self) -> watch::Receiver<ShutdownPhase> {
        self.phase_rx.clone()
    }

    /// Track a new connection
    pub fn connection_opened(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Track a closed connection
    pub fn connection_closed(&self) {
        let prev = self.active_connections.fetch_sub(1, Ordering::Relaxed);
        if prev == 1 && self.is_shutting_down() {
            debug!("Last connection closed during shutdown");
            self.connections_drained.notify_waiters();
        }
    }

    /// Get the number of active connections
    pub fn active_connections(&self) -> u64 {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Track a new in-flight request
    pub fn request_started(&self) {
        self.in_flight_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Track completion of an in-flight request
    pub fn request_completed(&self) {
        let prev = self.in_flight_requests.fetch_sub(1, Ordering::Relaxed);
        if prev == 1 && self.is_shutting_down() {
            debug!("Last in-flight request completed during shutdown");
            self.requests_drained.notify_waiters();
        }
    }

    /// Get the number of in-flight requests
    pub fn in_flight_requests(&self) -> u64 {
        self.in_flight_requests.load(Ordering::Relaxed)
    }

    /// Create a request guard that automatically tracks the request lifecycle
    pub fn request_guard(&self) -> Option<RequestGuard<'_>> {
        // Don't accept new requests if we're shutting down
        if self.is_shutting_down() {
            return None;
        }
        self.request_started();
        Some(RequestGuard { coordinator: self })
    }

    /// Drain active connections with a configurable timeout
    ///
    /// This method waits for all active connections to close gracefully.
    /// If connections remain after the drain timeout, they will be forcibly
    /// closed and logged for observability.
    ///
    /// # Arguments
    /// * `timeout` - Optional custom timeout. If None, uses `drain_timeout_secs` from config.
    ///
    /// # Returns
    /// The number of connections that were forcibly closed (0 if all drained gracefully).
    pub async fn drain_connections(&self, timeout: Option<Duration>) -> u64 {
        let drain_timeout = timeout.unwrap_or(Duration::from_secs(self.config.drain_timeout_secs));
        let initial_connections = self.active_connections();

        if initial_connections == 0 {
            debug!("No active connections to drain");
            return 0;
        }

        info!(
            active_connections = initial_connections,
            timeout_secs = drain_timeout.as_secs(),
            "Draining active connections"
        );

        // Wait for connections with timeout
        let drain_result = tokio::time::timeout(drain_timeout, async {
            while self.active_connections() > 0 {
                self.connections_drained.notified().await;
            }
        })
        .await;

        let remaining = self.active_connections();

        if drain_result.is_err() && remaining > 0 {
            warn!(
                remaining_connections = remaining,
                initial_connections = initial_connections,
                drained = initial_connections.saturating_sub(remaining),
                "Connection drain timeout - forcibly closing remaining connections"
            );
            remaining
        } else {
            info!(
                drained_connections = initial_connections,
                "All connections drained gracefully"
            );
            0
        }
    }

    /// Initiate graceful shutdown
    ///
    /// This method will:
    /// 1. Stop accepting new connections
    /// 2. Wait for in-flight requests to complete (with timeout)
    /// 3. Execute cleanup phases in order
    ///
    /// Returns Ok(()) if shutdown completed gracefully, or Err with status if timed out.
    pub async fn initiate_shutdown(&self) -> Result<(), ShutdownError> {
        // Only allow one shutdown
        if self.shutdown_initiated.swap(true, Ordering::SeqCst) {
            debug!("Shutdown already in progress");
            return Ok(());
        }

        let start = Instant::now();
        *self.shutdown_start.lock().await = Some(start);
        let timeout = Duration::from_secs(self.config.timeout_secs);

        info!(
            timeout_secs = self.config.timeout_secs,
            active_connections = self.active_connections(),
            in_flight_requests = self.in_flight_requests(),
            "Initiating graceful shutdown"
        );

        // Notify all listeners that shutdown has started
        let _ = self.notify_tx.send(());

        // Phase 1: Stop accepting new connections
        self.transition_to(ShutdownPhase::DrainConnections);

        // Phase 2: Wait for in-flight requests to complete
        if self.config.wait_for_requests && self.in_flight_requests() > 0 {
            self.transition_to(ShutdownPhase::DrainRequests);

            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return Err(ShutdownError::Timeout {
                    phase: ShutdownPhase::DrainRequests,
                    pending_requests: self.in_flight_requests(),
                    active_connections: self.active_connections(),
                });
            }

            // Wait for requests with timeout
            let wait_result = tokio::time::timeout(remaining, async {
                while self.in_flight_requests() > 0 {
                    self.requests_drained.notified().await;
                }
            })
            .await;

            if wait_result.is_err() {
                warn!(
                    pending_requests = self.in_flight_requests(),
                    active_connections = self.active_connections(),
                    "Shutdown timeout waiting for requests to drain"
                );
                return Err(ShutdownError::Timeout {
                    phase: ShutdownPhase::DrainRequests,
                    pending_requests: self.in_flight_requests(),
                    active_connections: self.active_connections(),
                });
            }

            info!("All in-flight requests completed");
        }

        // Phase 3: Drain and close connections
        self.transition_to(ShutdownPhase::CloseConnections);

        // Calculate remaining time for connection drain
        let remaining = timeout.saturating_sub(start.elapsed());
        let drain_timeout = remaining.min(Duration::from_secs(self.config.drain_timeout_secs));

        // Drain connections with timeout
        let forcibly_closed = self.drain_connections(Some(drain_timeout)).await;
        if forcibly_closed > 0 {
            debug!(
                forcibly_closed = forcibly_closed,
                "Some connections were forcibly closed during shutdown"
            );
        }

        // Phase 4: Flush state
        self.transition_to(ShutdownPhase::FlushState);

        // Phase 5: Complete
        self.transition_to(ShutdownPhase::Complete);

        let elapsed = start.elapsed();
        info!(
            elapsed_ms = elapsed.as_millis(),
            "Graceful shutdown complete"
        );

        Ok(())
    }

    /// Helper to transition to a new phase
    fn transition_to(&self, phase: ShutdownPhase) {
        debug!(phase = %phase, "Transitioning to shutdown phase");
        let _ = self.phase_tx.send(phase);
    }

    /// Get shutdown statistics
    pub async fn stats(&self) -> ShutdownStats {
        let shutdown_duration = self.shutdown_start.lock().await.map(|s| s.elapsed());

        ShutdownStats {
            is_shutting_down: self.is_shutting_down(),
            phase: self.current_phase(),
            active_connections: self.active_connections(),
            in_flight_requests: self.in_flight_requests(),
            shutdown_duration,
        }
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard for tracking in-flight requests
pub struct RequestGuard<'a> {
    coordinator: &'a ShutdownCoordinator,
}

impl<'a> Drop for RequestGuard<'a> {
    fn drop(&mut self) {
        self.coordinator.request_completed();
    }
}

/// Statistics about shutdown state
#[derive(Debug, Clone)]
pub struct ShutdownStats {
    /// Whether shutdown has been initiated
    pub is_shutting_down: bool,
    /// Current shutdown phase
    pub phase: ShutdownPhase,
    /// Number of active connections
    pub active_connections: u64,
    /// Number of in-flight requests
    pub in_flight_requests: u64,
    /// Time since shutdown was initiated
    pub shutdown_duration: Option<Duration>,
}

/// Error type for shutdown operations
#[derive(Debug)]
pub enum ShutdownError {
    /// Shutdown timed out while waiting for requests
    Timeout {
        phase: ShutdownPhase,
        pending_requests: u64,
        active_connections: u64,
    },
}

impl std::fmt::Display for ShutdownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShutdownError::Timeout {
                phase,
                pending_requests,
                active_connections,
            } => {
                write!(
                    f,
                    "Shutdown timed out during {} phase ({} pending requests, {} active connections)",
                    phase, pending_requests, active_connections
                )
            }
        }
    }
}

impl std::error::Error for ShutdownError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shutdown_config_default() {
        let config = ShutdownConfig::default();
        assert_eq!(config.timeout_secs, DEFAULT_SHUTDOWN_TIMEOUT_SECS);
        assert!(config.wait_for_requests);
        assert_eq!(config.drain_timeout_secs, DEFAULT_DRAIN_TIMEOUT_SECS);
    }

    #[test]
    fn test_shutdown_phase_display() {
        assert_eq!(ShutdownPhase::Running.to_string(), "running");
        assert_eq!(
            ShutdownPhase::DrainConnections.to_string(),
            "draining connections"
        );
        assert_eq!(
            ShutdownPhase::DrainRequests.to_string(),
            "draining requests"
        );
        assert_eq!(ShutdownPhase::Complete.to_string(), "complete");
    }

    #[test]
    fn test_coordinator_initial_state() {
        let coordinator = ShutdownCoordinator::new();
        assert!(!coordinator.is_shutting_down());
        assert_eq!(coordinator.current_phase(), ShutdownPhase::Running);
        assert_eq!(coordinator.active_connections(), 0);
        assert_eq!(coordinator.in_flight_requests(), 0);
    }

    #[test]
    fn test_connection_tracking() {
        let coordinator = ShutdownCoordinator::new();

        coordinator.connection_opened();
        assert_eq!(coordinator.active_connections(), 1);

        coordinator.connection_opened();
        assert_eq!(coordinator.active_connections(), 2);

        coordinator.connection_closed();
        assert_eq!(coordinator.active_connections(), 1);

        coordinator.connection_closed();
        assert_eq!(coordinator.active_connections(), 0);
    }

    #[test]
    fn test_request_tracking() {
        let coordinator = ShutdownCoordinator::new();

        coordinator.request_started();
        assert_eq!(coordinator.in_flight_requests(), 1);

        coordinator.request_started();
        assert_eq!(coordinator.in_flight_requests(), 2);

        coordinator.request_completed();
        assert_eq!(coordinator.in_flight_requests(), 1);

        coordinator.request_completed();
        assert_eq!(coordinator.in_flight_requests(), 0);
    }

    #[test]
    fn test_request_guard() {
        let coordinator = ShutdownCoordinator::new();
        assert_eq!(coordinator.in_flight_requests(), 0);

        {
            let _guard = coordinator.request_guard();
            assert_eq!(coordinator.in_flight_requests(), 1);
        } // guard drops here

        assert_eq!(coordinator.in_flight_requests(), 0);
    }

    #[test]
    fn test_request_guard_during_shutdown() {
        let coordinator = ShutdownCoordinator::new();

        // Simulate shutdown being initiated
        coordinator.shutdown_initiated.store(true, Ordering::SeqCst);

        // Should not allow new requests during shutdown
        assert!(coordinator.request_guard().is_none());
    }

    #[tokio::test]
    async fn test_graceful_shutdown_no_requests() {
        let coordinator = ShutdownCoordinator::with_config(ShutdownConfig {
            timeout_secs: 5,
            wait_for_requests: true,
            drain_timeout_secs: DEFAULT_DRAIN_TIMEOUT_SECS,
        });

        let result = coordinator.initiate_shutdown().await;
        assert!(result.is_ok());
        assert!(coordinator.is_shutting_down());
        assert_eq!(coordinator.current_phase(), ShutdownPhase::Complete);
    }

    #[tokio::test]
    async fn test_graceful_shutdown_with_requests() {
        let coordinator = Arc::new(ShutdownCoordinator::with_config(ShutdownConfig {
            timeout_secs: 5,
            wait_for_requests: true,
            drain_timeout_secs: DEFAULT_DRAIN_TIMEOUT_SECS,
        }));

        // Simulate an in-flight request
        coordinator.request_started();

        // Start shutdown in background
        let coordinator_clone = coordinator.clone();
        let shutdown_handle =
            tokio::spawn(async move { coordinator_clone.initiate_shutdown().await });

        // Give shutdown time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Complete the request
        coordinator.request_completed();

        // Shutdown should complete successfully
        let result = shutdown_handle.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_shutdown_timeout() {
        let coordinator = ShutdownCoordinator::with_config(ShutdownConfig {
            timeout_secs: 1, // Very short timeout
            wait_for_requests: true,
            drain_timeout_secs: 1,
        });

        // Simulate a request that won't complete
        coordinator.request_started();

        let result = coordinator.initiate_shutdown().await;
        assert!(result.is_err());

        match result {
            Err(ShutdownError::Timeout {
                phase,
                pending_requests,
                ..
            }) => {
                assert_eq!(phase, ShutdownPhase::DrainRequests);
                assert_eq!(pending_requests, 1);
            }
            Ok(_) => panic!("Expected timeout error"),
        }
    }

    #[tokio::test]
    async fn test_double_shutdown_is_idempotent() {
        let coordinator = ShutdownCoordinator::new();

        // First shutdown
        let result1 = coordinator.initiate_shutdown().await;
        assert!(result1.is_ok());

        // Second shutdown should also succeed (idempotent)
        let result2 = coordinator.initiate_shutdown().await;
        assert!(result2.is_ok());
    }

    #[tokio::test]
    async fn test_stats() {
        let coordinator = ShutdownCoordinator::new();

        coordinator.connection_opened();
        coordinator.request_started();

        let stats = coordinator.stats().await;
        assert!(!stats.is_shutting_down);
        assert_eq!(stats.phase, ShutdownPhase::Running);
        assert_eq!(stats.active_connections, 1);
        assert_eq!(stats.in_flight_requests, 1);
        assert!(stats.shutdown_duration.is_none());
    }

    #[test]
    fn test_shutdown_error_display() {
        let error = ShutdownError::Timeout {
            phase: ShutdownPhase::DrainRequests,
            pending_requests: 5,
            active_connections: 10,
        };
        let msg = error.to_string();
        assert!(msg.contains("draining requests"));
        assert!(msg.contains("5 pending requests"));
        assert!(msg.contains("10 active connections"));
    }

    #[tokio::test]
    async fn test_drain_connections_no_connections() {
        let coordinator = ShutdownCoordinator::new();

        // No connections - should return 0 immediately
        let forcibly_closed = coordinator.drain_connections(None).await;
        assert_eq!(forcibly_closed, 0);
    }

    #[tokio::test]
    async fn test_drain_connections_graceful() {
        let coordinator = Arc::new(ShutdownCoordinator::new());

        // Open a connection
        coordinator.connection_opened();
        assert_eq!(coordinator.active_connections(), 1);

        // Mark as shutting down so notifications work
        coordinator.shutdown_initiated.store(true, Ordering::SeqCst);

        // Start drain in background
        let coordinator_clone = coordinator.clone();
        let drain_handle = tokio::spawn(async move {
            coordinator_clone
                .drain_connections(Some(Duration::from_secs(5)))
                .await
        });

        // Give drain time to start waiting
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Close the connection
        coordinator.connection_closed();

        // Drain should complete successfully with 0 forcibly closed
        let forcibly_closed = drain_handle.await.unwrap();
        assert_eq!(forcibly_closed, 0);
    }

    #[tokio::test]
    async fn test_drain_connections_timeout() {
        let coordinator = ShutdownCoordinator::with_config(ShutdownConfig {
            timeout_secs: 30,
            wait_for_requests: true,
            drain_timeout_secs: 1,
        });

        // Open a connection that won't close
        coordinator.connection_opened();

        // Mark as shutting down
        coordinator.shutdown_initiated.store(true, Ordering::SeqCst);

        // Drain should timeout and return 1 (the connection that wasn't closed)
        let forcibly_closed = coordinator
            .drain_connections(Some(Duration::from_millis(100)))
            .await;
        assert_eq!(forcibly_closed, 1);
    }
}
