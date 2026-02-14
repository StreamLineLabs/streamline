//! Fault injection framework for testing system resilience
//!
//! This module provides comprehensive fault injection capabilities for testing:
//! - Storage faults (write failures, corruption, delays)
//! - Network faults (latency, packet loss, connection failures)
//! - Protocol faults (malformed messages, invalid sequences)
//! - Resource faults (memory pressure, file descriptor exhaustion)
//!
//! ## Usage
//!
//! Fault injection tests verify the system handles failure conditions gracefully
//! without data loss or corruption.

// Allow unused code in this module - these are test utilities designed for
// comprehensive fault injection testing, and not all capabilities are used yet
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Types of faults that can be injected
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultType {
    /// Fail write operations with an error
    WriteFailure,
    /// Fail read operations with an error
    ReadFailure,
    /// Corrupt data during write (bit flip simulation)
    DataCorruption,
    /// Add latency to operations
    Latency(Duration),
    /// Fail every Nth operation
    IntermittentFailure(u64),
    /// Simulate disk full condition
    DiskFull,
    /// Simulate connection timeout
    ConnectionTimeout,
    /// Simulate connection reset
    ConnectionReset,
    /// Simulate slow network (high latency)
    SlowNetwork(Duration),
    /// Simulate packet loss (percentage 0-100)
    PacketLoss(u8),
    /// Simulate out-of-order delivery
    OutOfOrder,
    /// Simulate partial write
    PartialWrite,
}

/// Configuration for fault injection
#[derive(Clone)]
pub struct FaultConfig {
    /// Whether fault injection is enabled
    pub enabled: bool,
    /// Probability of fault occurring (0-100)
    pub probability: u8,
    /// Maximum number of faults to inject (0 = unlimited)
    pub max_faults: u64,
    /// Delay before fault takes effect
    pub delay: Duration,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            probability: 100, // Always trigger when enabled
            max_faults: 0,
            delay: Duration::from_millis(0),
        }
    }
}

/// Fault injection controller
///
/// Thread-safe controller for injecting various fault types into the system.
/// Typically used in test environments to verify error handling.
#[derive(Default)]
pub struct FaultInjector {
    /// Currently active faults
    active_faults: RwLock<Vec<(FaultType, FaultConfig)>>,
    /// Total faults injected
    fault_count: AtomicU64,
    /// Whether fault injection is globally enabled
    enabled: AtomicBool,
    /// Recorded fault events for verification
    fault_log: RwLock<Vec<FaultEvent>>,
}

/// A recorded fault event
#[derive(Debug, Clone)]
pub struct FaultEvent {
    /// Type of fault that occurred
    pub fault_type: FaultType,
    /// Timestamp when fault was injected
    pub timestamp: std::time::Instant,
    /// Context information about the fault
    pub context: String,
}

impl FaultInjector {
    /// Create a new fault injector
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable fault injection globally
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    /// Disable fault injection globally
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
    }

    /// Check if fault injection is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    /// Register a fault to be injected
    pub async fn register_fault(&self, fault_type: FaultType, config: FaultConfig) {
        let mut faults = self.active_faults.write().await;
        faults.push((fault_type, config));
    }

    /// Clear all registered faults
    pub async fn clear_faults(&self) {
        let mut faults = self.active_faults.write().await;
        faults.clear();
    }

    /// Check if a specific fault type should be triggered
    ///
    /// Returns true if the fault should be applied based on configuration.
    pub async fn should_inject(&self, fault_type: &FaultType) -> bool {
        if !self.is_enabled() {
            return false;
        }

        let faults = self.active_faults.read().await;
        for (registered_type, config) in faults.iter() {
            if registered_type == fault_type && config.enabled {
                // Check max faults limit
                if config.max_faults > 0 {
                    let count = self.fault_count.load(Ordering::SeqCst);
                    if count >= config.max_faults {
                        return false;
                    }
                }

                // Check probability
                if config.probability < 100 {
                    use std::hash::{Hash, Hasher};
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    std::time::SystemTime::now().hash(&mut hasher);
                    let random = (hasher.finish() % 100) as u8;
                    if random >= config.probability {
                        return false;
                    }
                }

                return true;
            }
        }
        false
    }

    /// Inject a fault and record the event
    pub async fn inject(&self, fault_type: FaultType, context: &str) {
        let event = FaultEvent {
            fault_type: fault_type.clone(),
            timestamp: std::time::Instant::now(),
            context: context.to_string(),
        };

        self.fault_count.fetch_add(1, Ordering::SeqCst);
        let mut log = self.fault_log.write().await;
        log.push(event);

        tracing::debug!(
            fault = ?fault_type,
            context = context,
            "Injected fault"
        );
    }

    /// Get the total number of faults injected
    pub fn fault_count(&self) -> u64 {
        self.fault_count.load(Ordering::SeqCst)
    }

    /// Get the fault event log
    pub async fn get_fault_log(&self) -> Vec<FaultEvent> {
        self.fault_log.read().await.clone()
    }

    /// Reset fault count and log
    pub async fn reset(&self) {
        self.fault_count.store(0, Ordering::SeqCst);
        let mut log = self.fault_log.write().await;
        log.clear();
    }
}

/// Storage fault simulator
///
/// Simulates various storage-layer failures for testing durability.
pub struct StorageFaultSimulator {
    injector: Arc<FaultInjector>,
}

impl StorageFaultSimulator {
    pub fn new(injector: Arc<FaultInjector>) -> Self {
        Self { injector }
    }

    /// Simulate write failure
    pub async fn fail_writes(&self) {
        let config = FaultConfig {
            enabled: true,
            ..Default::default()
        };
        self.injector
            .register_fault(FaultType::WriteFailure, config)
            .await;
    }

    /// Simulate disk full condition
    pub async fn simulate_disk_full(&self) {
        let config = FaultConfig {
            enabled: true,
            ..Default::default()
        };
        self.injector
            .register_fault(FaultType::DiskFull, config)
            .await;
    }

    /// Simulate intermittent write failures (every Nth write)
    pub async fn intermittent_failures(&self, every_n: u64) {
        let config = FaultConfig {
            enabled: true,
            ..Default::default()
        };
        self.injector
            .register_fault(FaultType::IntermittentFailure(every_n), config)
            .await;
    }

    /// Simulate data corruption
    pub async fn corrupt_data(&self) {
        let config = FaultConfig {
            enabled: true,
            ..Default::default()
        };
        self.injector
            .register_fault(FaultType::DataCorruption, config)
            .await;
    }

    /// Simulate partial writes
    pub async fn partial_writes(&self) {
        let config = FaultConfig {
            enabled: true,
            ..Default::default()
        };
        self.injector
            .register_fault(FaultType::PartialWrite, config)
            .await;
    }
}

/// Network fault simulator
///
/// Simulates various network-layer failures for testing reliability.
pub struct NetworkFaultSimulator {
    injector: Arc<FaultInjector>,
}

impl NetworkFaultSimulator {
    pub fn new(injector: Arc<FaultInjector>) -> Self {
        Self { injector }
    }

    /// Simulate connection timeouts
    pub async fn timeout_connections(&self) {
        let config = FaultConfig {
            enabled: true,
            ..Default::default()
        };
        self.injector
            .register_fault(FaultType::ConnectionTimeout, config)
            .await;
    }

    /// Simulate connection resets
    pub async fn reset_connections(&self) {
        let config = FaultConfig {
            enabled: true,
            ..Default::default()
        };
        self.injector
            .register_fault(FaultType::ConnectionReset, config)
            .await;
    }

    /// Simulate slow network with specified latency
    pub async fn slow_network(&self, latency: Duration) {
        let config = FaultConfig {
            enabled: true,
            ..Default::default()
        };
        self.injector
            .register_fault(FaultType::SlowNetwork(latency), config)
            .await;
    }

    /// Simulate packet loss at specified percentage
    pub async fn packet_loss(&self, percentage: u8) {
        let config = FaultConfig {
            enabled: true,
            ..Default::default()
        };
        self.injector
            .register_fault(FaultType::PacketLoss(percentage.min(100)), config)
            .await;
    }

    /// Simulate out-of-order message delivery
    pub async fn out_of_order_delivery(&self) {
        let config = FaultConfig {
            enabled: true,
            ..Default::default()
        };
        self.injector
            .register_fault(FaultType::OutOfOrder, config)
            .await;
    }
}

/// Latency injector for simulating slow operations
pub struct LatencyInjector {
    /// Base latency to add
    base_latency: Duration,
    /// Random jitter range (0 to jitter)
    jitter: Duration,
    /// Whether latency injection is enabled
    enabled: AtomicBool,
}

impl LatencyInjector {
    pub fn new(base_latency: Duration, jitter: Duration) -> Self {
        Self {
            base_latency,
            jitter,
            enabled: AtomicBool::new(false),
        }
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
    }

    /// Inject latency if enabled
    pub async fn maybe_delay(&self) {
        if self.enabled.load(Ordering::SeqCst) {
            let jitter_nanos = if self.jitter.as_nanos() > 0 {
                use std::hash::{Hash, Hasher};
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                std::time::SystemTime::now().hash(&mut hasher);
                (hasher.finish() as u128 % self.jitter.as_nanos()) as u64
            } else {
                0
            };
            let total = self.base_latency + Duration::from_nanos(jitter_nanos);
            tokio::time::sleep(total).await;
        }
    }
}

/// Chaos monkey for random fault injection
///
/// Randomly injects faults during operation to test system resilience.
pub struct ChaosMonkey {
    injector: Arc<FaultInjector>,
    /// Mean time between faults
    mtbf: Duration,
    /// Available fault types to inject
    fault_pool: Vec<FaultType>,
    /// Whether chaos monkey is running
    running: Arc<AtomicBool>,
}

impl ChaosMonkey {
    pub fn new(injector: Arc<FaultInjector>, mtbf: Duration) -> Self {
        Self {
            injector,
            mtbf,
            fault_pool: vec![
                FaultType::WriteFailure,
                FaultType::ReadFailure,
                FaultType::ConnectionTimeout,
                FaultType::SlowNetwork(Duration::from_millis(500)),
                FaultType::PacketLoss(10),
            ],
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Add a fault type to the pool
    pub fn add_fault(&mut self, fault_type: FaultType) {
        self.fault_pool.push(fault_type);
    }

    /// Start the chaos monkey in background
    pub fn start(&self) -> tokio::task::JoinHandle<()> {
        let injector = self.injector.clone();
        let mtbf = self.mtbf;
        let fault_pool = self.fault_pool.clone();
        let running = self.running.clone();

        running.store(true, Ordering::SeqCst);

        tokio::spawn(async move {
            while running.load(Ordering::SeqCst) {
                // Sleep for random time based on MTBF
                use std::hash::{Hash, Hasher};
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                std::time::SystemTime::now().hash(&mut hasher);
                let delay_factor = (hasher.finish() % 200) as f64 / 100.0; // 0-2x
                let delay = mtbf.mul_f64(delay_factor);
                tokio::time::sleep(delay).await;

                if !running.load(Ordering::SeqCst) {
                    break;
                }

                // Pick random fault
                if !fault_pool.is_empty() {
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    std::time::SystemTime::now().hash(&mut hasher);
                    let idx = (hasher.finish() as usize) % fault_pool.len();
                    let fault = fault_pool[idx].clone();

                    // Inject fault briefly
                    let config = FaultConfig {
                        enabled: true,
                        max_faults: 1,
                        ..Default::default()
                    };
                    injector.register_fault(fault.clone(), config).await;
                    injector.inject(fault, "chaos_monkey").await;

                    // Brief active period
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    injector.clear_faults().await;
                }
            }
        })
    }

    /// Stop the chaos monkey
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fault_injector_basic() {
        let injector = FaultInjector::new();

        // Initially disabled
        assert!(!injector.is_enabled());
        assert!(!injector.should_inject(&FaultType::WriteFailure).await);

        // Enable and register fault
        injector.enable();
        let config = FaultConfig {
            enabled: true,
            ..Default::default()
        };
        injector
            .register_fault(FaultType::WriteFailure, config)
            .await;

        // Should now trigger
        assert!(injector.should_inject(&FaultType::WriteFailure).await);
        assert!(!injector.should_inject(&FaultType::ReadFailure).await);
    }

    #[tokio::test]
    async fn test_fault_injector_max_faults() {
        let injector = FaultInjector::new();
        injector.enable();

        let config = FaultConfig {
            enabled: true,
            max_faults: 2,
            ..Default::default()
        };
        injector
            .register_fault(FaultType::WriteFailure, config)
            .await;

        // First two should trigger
        assert!(injector.should_inject(&FaultType::WriteFailure).await);
        injector.inject(FaultType::WriteFailure, "test1").await;
        assert!(injector.should_inject(&FaultType::WriteFailure).await);
        injector.inject(FaultType::WriteFailure, "test2").await;

        // Third should not (max reached)
        assert!(!injector.should_inject(&FaultType::WriteFailure).await);
        assert_eq!(injector.fault_count(), 2);
    }

    #[tokio::test]
    async fn test_fault_log() {
        let injector = FaultInjector::new();
        injector.enable();

        injector.inject(FaultType::WriteFailure, "ctx1").await;
        injector.inject(FaultType::ReadFailure, "ctx2").await;

        let log = injector.get_fault_log().await;
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].context, "ctx1");
        assert_eq!(log[1].context, "ctx2");
    }

    #[tokio::test]
    async fn test_storage_fault_simulator() {
        let injector = Arc::new(FaultInjector::new());
        let simulator = StorageFaultSimulator::new(injector.clone());

        injector.enable();
        simulator.fail_writes().await;

        assert!(injector.should_inject(&FaultType::WriteFailure).await);
    }

    #[tokio::test]
    async fn test_network_fault_simulator() {
        let injector = Arc::new(FaultInjector::new());
        let simulator = NetworkFaultSimulator::new(injector.clone());

        injector.enable();
        simulator.packet_loss(25).await;

        assert!(injector.should_inject(&FaultType::PacketLoss(25)).await);
    }

    #[tokio::test]
    async fn test_latency_injector() {
        let latency = LatencyInjector::new(Duration::from_millis(10), Duration::from_millis(5));

        // Disabled - should not delay
        let start = std::time::Instant::now();
        latency.maybe_delay().await;
        assert!(start.elapsed() < Duration::from_millis(5));

        // Enabled - should delay
        latency.enable();
        let start = std::time::Instant::now();
        latency.maybe_delay().await;
        assert!(start.elapsed() >= Duration::from_millis(10));
    }
}
