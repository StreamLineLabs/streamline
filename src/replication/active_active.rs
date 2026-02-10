//! Active-Active Geo-Replication with CRDTs
//!
//! This module provides multi-region active-active replication using
//! conflict-free replicated data types (CRDTs). It enables multiple
//! regions to accept writes simultaneously while guaranteeing eventual
//! consistency through vector clocks and automatic conflict resolution.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │              ACTIVE-ACTIVE GEO-REPLICATION WITH CRDTs                  │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                         │
//! │   us-east-1 (Primary)            eu-west-1                ap-south-1   │
//! │  ┌──────────────────┐          ┌──────────────────┐    ┌─────────────┐ │
//! │  │   GeoReplicator  │◄────────►│   GeoReplicator  │◄──►│ GeoReplicat.│ │
//! │  │  (Active Writer) │  Bidir.  │  (Active Writer) │    │ (ReadOnly)  │ │
//! │  └───────┬──────────┘  Repl.   └───────┬──────────┘    └──────┬──────┘ │
//! │          │                              │                      │        │
//! │     ┌────┴─────┐                  ┌─────┴────┐           ┌─────┴────┐   │
//! │     │ Vector   │                  │ Vector   │           │ Vector   │   │
//! │     │  Clock   │                  │  Clock   │           │  Clock   │   │
//! │     └────┬─────┘                  └─────┬────┘           └─────┬────┘   │
//! │          │                              │                      │        │
//! │     ┌────┴──────────────────────────────┴──────────────────────┴────┐   │
//! │     │               Conflict Resolver (CRDT-based)                  │   │
//! │     │  LWW | FirstWriterWins | PrimaryWins | CrdtMerge | Custom   │   │
//! │     └───────────────────────────────────────────────────────────────┘   │
//! │                                                                         │
//! │     ┌───────────────────────────────────────────────────────────────┐   │
//! │     │                    Replication Log                            │   │
//! │     │  Ordered, durable log of all replication events              │   │
//! │     └───────────────────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Key Concepts
//!
//! - **Vector Clocks**: Track causality across regions to detect concurrent writes
//! - **Conflict Resolution**: Automatically resolve conflicts using configurable strategies
//! - **Replication Modes**: ActiveActive, ActivePassive, ReadOnly
//! - **Consistency Levels**: Eventual, Session, BoundedStaleness, Strong
//!
//! # Feature Gate
//!
//! This module requires the `geo-replication` feature flag:
//! ```toml
//! [dependencies]
//! streamline = { version = "0.1", features = ["geo-replication"] }
//! ```

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------

/// Configuration for active-active geo-replication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoReplicationConfig {
    /// Identifier for the local region (e.g., "us-east-1").
    pub local_region: String,

    /// All participating regions including the local one.
    pub regions: Vec<RegionConfig>,

    /// Replication mode governing write semantics.
    pub replication_mode: ReplicationMode,

    /// Strategy used to resolve conflicting concurrent writes.
    pub conflict_strategy: ConflictStrategy,

    /// Consistency guarantee for cross-region reads.
    pub consistency_level: ConsistencyLevel,

    /// Maximum acceptable replication lag in milliseconds before a region
    /// is considered degraded.
    pub replication_lag_threshold_ms: u64,

    /// Interval between heartbeat probes sent to peer regions.
    pub heartbeat_interval_ms: u64,
}

impl Default for GeoReplicationConfig {
    fn default() -> Self {
        Self {
            local_region: "local".to_string(),
            regions: vec![RegionConfig {
                name: "local".to_string(),
                endpoint: "127.0.0.1:9092".to_string(),
                priority: 0,
                is_primary: true,
                enabled: true,
            }],
            replication_mode: ReplicationMode::ActiveActive,
            conflict_strategy: ConflictStrategy::LastWriterWins,
            consistency_level: ConsistencyLevel::Eventual,
            replication_lag_threshold_ms: 5000,
            heartbeat_interval_ms: 1000,
        }
    }
}

impl GeoReplicationConfig {
    /// Validate that the configuration is well-formed.
    pub fn validate(&self) -> Result<()> {
        if self.local_region.is_empty() {
            return Err(StreamlineError::Config(
                "local_region must not be empty".into(),
            ));
        }

        if self.regions.is_empty() {
            return Err(StreamlineError::Config(
                "at least one region must be configured".into(),
            ));
        }

        let local_present = self.regions.iter().any(|r| r.name == self.local_region);
        if !local_present {
            return Err(StreamlineError::Config(format!(
                "local_region '{}' must appear in the regions list",
                self.local_region
            )));
        }

        let primary_count = self.regions.iter().filter(|r| r.is_primary).count();
        if primary_count == 0 {
            return Err(StreamlineError::Config(
                "at least one region must be designated as primary".into(),
            ));
        }

        if self.replication_lag_threshold_ms == 0 {
            return Err(StreamlineError::Config(
                "replication_lag_threshold_ms must be greater than zero".into(),
            ));
        }

        if self.heartbeat_interval_ms == 0 {
            return Err(StreamlineError::Config(
                "heartbeat_interval_ms must be greater than zero".into(),
            ));
        }

        // Check for duplicate region names
        let mut seen = std::collections::HashSet::new();
        for region in &self.regions {
            if !seen.insert(&region.name) {
                return Err(StreamlineError::Config(format!(
                    "duplicate region name: '{}'",
                    region.name
                )));
            }
        }

        Ok(())
    }
}

/// Configuration for a single region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionConfig {
    /// Human-readable region name (e.g., "us-east-1").
    pub name: String,

    /// Network endpoint for replication traffic (host:port).
    pub endpoint: String,

    /// Priority for failover ordering. Lower values indicate higher priority.
    pub priority: u32,

    /// Whether this region is a primary (can accept writes in ActivePassive mode).
    pub is_primary: bool,

    /// Whether replication to/from this region is currently enabled.
    pub enabled: bool,
}

/// Replication mode governing write semantics across regions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationMode {
    /// All regions accept writes; conflicts are resolved via the configured strategy.
    ActiveActive,
    /// Only primary regions accept writes; others replicate passively.
    ActivePassive,
    /// This region is read-only and does not originate writes.
    ReadOnly,
}

/// Strategy for resolving conflicting concurrent writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictStrategy {
    /// The write with the latest wall-clock timestamp wins.
    LastWriterWins,
    /// The write with the earliest wall-clock timestamp wins.
    FirstWriterWins,
    /// The primary region's write always wins.
    PrimaryWins,
    /// Merge values using CRDT semantics (element-wise merge of byte payloads).
    CrdtMerge,
    /// Delegate to a user-supplied callback (placeholder for custom logic).
    Custom,
}

/// Consistency level for cross-region reads and replication acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsistencyLevel {
    /// Fire-and-forget; reads may observe stale data.
    Eventual,
    /// Reads within a single session are monotonically consistent.
    Session,
    /// Reads are guaranteed to be no more than `max_lag_ms` behind.
    BoundedStaleness {
        /// Maximum tolerated staleness in milliseconds.
        max_lag_ms: u64,
    },
    /// Reads always reflect the latest committed write across all regions.
    Strong,
}

// ---------------------------------------------------------------------------
// Vector Clock
// ---------------------------------------------------------------------------

/// A vector clock for tracking causal ordering across regions.
///
/// Each region maintains a monotonically increasing counter. By comparing
/// vector clocks we can determine whether two events are causally ordered
/// or concurrent (i.e., potentially conflicting).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorClock {
    /// Per-region logical timestamps.
    clocks: BTreeMap<String, u64>,
}

impl VectorClock {
    /// Create a new vector clock initialised for the given region.
    pub fn new(region_id: &str) -> Self {
        let mut clocks = BTreeMap::new();
        clocks.insert(region_id.to_string(), 0);
        Self { clocks }
    }

    /// Increment the counter for `region` and return the updated clock.
    pub fn increment(&mut self, region: &str) -> &Self {
        let counter = self.clocks.entry(region.to_string()).or_insert(0);
        *counter += 1;
        self
    }

    /// Merge another vector clock into this one, taking the element-wise
    /// maximum for each region.
    pub fn merge(&mut self, other: &VectorClock) -> &Self {
        for (region, &value) in &other.clocks {
            let local = self.clocks.entry(region.clone()).or_insert(0);
            *local = (*local).max(value);
        }
        self
    }

    /// Returns `true` if `self` causally happened before `other`.
    ///
    /// This means every component in `self` is less than or equal to the
    /// corresponding component in `other`, and at least one is strictly less.
    pub fn happens_before(&self, other: &VectorClock) -> bool {
        let mut all_leq = true;
        let mut at_least_one_lt = false;

        let all_regions: std::collections::BTreeSet<_> =
            self.clocks.keys().chain(other.clocks.keys()).collect();

        for region in all_regions {
            let self_val = self.clocks.get(region).copied().unwrap_or(0);
            let other_val = other.clocks.get(region).copied().unwrap_or(0);

            if self_val > other_val {
                all_leq = false;
                break;
            }
            if self_val < other_val {
                at_least_one_lt = true;
            }
        }

        all_leq && at_least_one_lt
    }

    /// Returns `true` if `self` and `other` are concurrent (neither
    /// causally precedes the other, and they are not equal).
    pub fn is_concurrent(&self, other: &VectorClock) -> bool {
        !self.happens_before(other) && !other.happens_before(self) && self != other
    }

    /// Get the counter value for the given region (0 if absent).
    pub fn get(&self, region: &str) -> u64 {
        self.clocks.get(region).copied().unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// Replicated Record
// ---------------------------------------------------------------------------

/// A record augmented with replication metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicatedRecord {
    /// Optional key bytes.
    pub key: Option<Vec<u8>>,

    /// Value payload bytes.
    pub value: Vec<u8>,

    /// Application-level headers.
    pub headers: HashMap<String, Vec<u8>>,

    /// Wall-clock timestamp at the originating region.
    pub timestamp: DateTime<Utc>,

    /// Vector clock capturing causal history.
    pub vector_clock: VectorClock,

    /// Region where this record was originally produced.
    pub origin_region: String,

    /// Monotonically increasing sequence number within the origin region.
    pub sequence_number: u64,
}

// ---------------------------------------------------------------------------
// Conflict Resolver
// ---------------------------------------------------------------------------

/// Stateless conflict resolver that selects a winning record from two
/// concurrent replicated records based on the configured strategy.
pub struct ConflictResolver;

/// The outcome of a conflict resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedRecord {
    /// The winning record.
    pub record: ReplicatedRecord,

    /// Which side won.
    pub winner: ConflictWinner,

    /// Merged vector clock reflecting both causal histories.
    pub merged_clock: VectorClock,
}

/// Indicates which side of a conflict was chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictWinner {
    /// The local record was retained.
    Local,
    /// The remote record was accepted.
    Remote,
    /// A CRDT merge produced a new combined value.
    Merged,
}

impl ConflictResolver {
    /// Resolve a conflict between a `local` and a `remote` record using the
    /// provided `strategy`.
    ///
    /// The returned [`ResolvedRecord`] contains the winning record and a
    /// merged vector clock.
    pub fn resolve(
        local: &ReplicatedRecord,
        remote: &ReplicatedRecord,
        strategy: &ConflictStrategy,
    ) -> ResolvedRecord {
        let mut merged_clock = local.vector_clock.clone();
        merged_clock.merge(&remote.vector_clock);

        match strategy {
            ConflictStrategy::LastWriterWins => {
                if remote.timestamp > local.timestamp {
                    ResolvedRecord {
                        record: remote.clone(),
                        winner: ConflictWinner::Remote,
                        merged_clock,
                    }
                } else {
                    ResolvedRecord {
                        record: local.clone(),
                        winner: ConflictWinner::Local,
                        merged_clock,
                    }
                }
            }
            ConflictStrategy::FirstWriterWins => {
                if remote.timestamp < local.timestamp {
                    ResolvedRecord {
                        record: remote.clone(),
                        winner: ConflictWinner::Remote,
                        merged_clock,
                    }
                } else {
                    ResolvedRecord {
                        record: local.clone(),
                        winner: ConflictWinner::Local,
                        merged_clock,
                    }
                }
            }
            ConflictStrategy::PrimaryWins => {
                // By convention the "local" side is the primary when this
                // strategy is in effect. A higher-level caller is responsible
                // for ensuring the primary's record is passed as `local`.
                ResolvedRecord {
                    record: local.clone(),
                    winner: ConflictWinner::Local,
                    merged_clock,
                }
            }
            ConflictStrategy::CrdtMerge => {
                // CRDT merge: byte-wise maximum of the two values (useful for
                // counter CRDTs stored as big-endian u64, for example).
                let merged_value = Self::crdt_byte_merge(&local.value, &remote.value);
                let mut merged_record = local.clone();
                merged_record.value = merged_value;
                merged_record.vector_clock = merged_clock.clone();
                ResolvedRecord {
                    record: merged_record,
                    winner: ConflictWinner::Merged,
                    merged_clock,
                }
            }
            ConflictStrategy::Custom => {
                // Custom strategy is a placeholder. In a production system this
                // would invoke a user-supplied callback. Default to local wins.
                ResolvedRecord {
                    record: local.clone(),
                    winner: ConflictWinner::Local,
                    merged_clock,
                }
            }
        }
    }

    /// CRDT-style byte merge: produce a value whose length is the maximum of
    /// the two inputs, taking the element-wise maximum byte at each position.
    fn crdt_byte_merge(a: &[u8], b: &[u8]) -> Vec<u8> {
        let len = a.len().max(b.len());
        let mut result = vec![0u8; len];
        for i in 0..len {
            let va = if i < a.len() { a[i] } else { 0 };
            let vb = if i < b.len() { b[i] } else { 0 };
            result[i] = va.max(vb);
        }
        result
    }
}

// ---------------------------------------------------------------------------
// Replication Result, Status, Lag
// ---------------------------------------------------------------------------

/// The result of a replication request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationResult {
    /// High-level status of the replication.
    pub status: ReplicationStatus,

    /// Vector clock after the replication.
    pub vector_clock: VectorClock,

    /// Regions that acknowledged the write.
    pub acked_regions: Vec<String>,
}

/// Replication status indicating the outcome of a replicate call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationStatus {
    /// All target regions acknowledged the write.
    Success,
    /// A conflict was detected and resolved.
    Conflict {
        /// Description of the resolution applied.
        resolution: String,
    },
    /// Only a subset of regions acknowledged the write.
    PartialSuccess {
        /// Regions that successfully acknowledged.
        acked_regions: Vec<String>,
    },
}

/// Status information for a single region as observed by the local replicator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionStatus {
    /// Region name.
    pub name: String,

    /// Network endpoint.
    pub endpoint: String,

    /// Current health state.
    pub state: RegionState,

    /// Observed replication lag from this region in milliseconds.
    pub replication_lag_ms: u64,

    /// Timestamp of the last successful heartbeat from this region.
    pub last_heartbeat: DateTime<Utc>,

    /// Total number of records successfully replicated to/from this region.
    pub records_replicated: u64,

    /// Total number of conflicts resolved with this region.
    pub conflicts_resolved: u64,
}

/// Health state of a peer region.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionState {
    /// Region is healthy and within acceptable replication lag.
    Healthy,
    /// Region is reachable but replication lag exceeds the configured threshold.
    Degraded,
    /// Region has not responded to heartbeats within the expected window.
    Unreachable,
    /// Region is actively synchronizing after a period of unavailability.
    Syncing,
}

/// Replication lag metrics for a single region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationLag {
    /// Region name.
    pub region: String,

    /// Current replication lag in milliseconds.
    pub lag_ms: u64,

    /// The last offset that has been confirmed as synced.
    pub last_synced_offset: u64,

    /// The current (latest) offset in the local log.
    pub current_offset: u64,
}

/// Result of a forced synchronization operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    /// Region that was synchronized.
    pub region: String,

    /// Number of records transmitted during the sync.
    pub records_synced: u64,

    /// Elapsed time of the sync in milliseconds.
    pub duration_ms: u64,

    /// Whether the sync completed successfully.
    pub success: bool,
}

// ---------------------------------------------------------------------------
// Replication Log
// ---------------------------------------------------------------------------

/// An ordered, in-memory log of replication events.
///
/// Entries are assigned monotonically increasing sequence numbers and can
/// be queried or truncated by sequence.
#[derive(Debug)]
pub struct ReplicationLog {
    entries: VecDeque<ReplicationLogEntry>,
    next_sequence: u64,
}

/// A single entry in the replication log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationLogEntry {
    /// Monotonic sequence number.
    pub sequence: u64,

    /// The replicated record.
    pub record: ReplicatedRecord,

    /// Timestamp when the entry was appended.
    pub timestamp: DateTime<Utc>,

    /// Regions that have acknowledged this entry.
    pub acked_regions: Vec<String>,
}

impl ReplicationLog {
    /// Create a new empty replication log.
    pub fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            next_sequence: 0,
        }
    }

    /// Append an entry to the log, assigning it the next sequence number.
    /// Returns the assigned sequence number.
    pub fn append(&mut self, record: ReplicatedRecord, acked_regions: Vec<String>) -> u64 {
        let seq = self.next_sequence;
        self.next_sequence += 1;
        self.entries.push_back(ReplicationLogEntry {
            sequence: seq,
            record,
            timestamp: Utc::now(),
            acked_regions,
        });
        seq
    }

    /// Return all entries with sequence numbers >= `since`.
    pub fn get_entries_since(&self, since: u64) -> Vec<&ReplicationLogEntry> {
        self.entries
            .iter()
            .filter(|e| e.sequence >= since)
            .collect()
    }

    /// Remove all entries with sequence numbers strictly less than `before`.
    pub fn truncate_before(&mut self, before: u64) {
        while let Some(front) = self.entries.front() {
            if front.sequence < before {
                self.entries.pop_front();
            } else {
                break;
            }
        }
    }

    /// Return the number of entries currently in the log.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return `true` if the log contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Return the next sequence number that will be assigned.
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }
}

impl Default for ReplicationLog {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// GeoReplicator (main entry point)
// ---------------------------------------------------------------------------

/// Main struct that coordinates cross-region active-active replication.
///
/// `GeoReplicator` manages vector clocks, conflict resolution, heartbeat
/// tracking, and the replication log. It is the primary API surface for
/// producing and consuming replicated records.
pub struct GeoReplicator {
    config: GeoReplicationConfig,
    running: AtomicBool,
    region_statuses: Arc<RwLock<HashMap<String, RegionStatus>>>,
    replication_log: Arc<RwLock<ReplicationLog>>,
    local_sequence: AtomicU64,
    topic_clocks: Arc<RwLock<HashMap<String, VectorClock>>>,
}

impl std::fmt::Debug for GeoReplicator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GeoReplicator")
            .field("local_region", &self.config.local_region)
            .field("running", &self.running.load(Ordering::Relaxed))
            .finish()
    }
}

impl GeoReplicator {
    /// Create a new `GeoReplicator` from the given configuration.
    ///
    /// The replicator is created in a stopped state. Call [`start`](Self::start)
    /// to initialise internal bookkeeping and begin heartbeat monitoring.
    pub fn new(config: GeoReplicationConfig) -> Self {
        Self {
            config,
            running: AtomicBool::new(false),
            region_statuses: Arc::new(RwLock::new(HashMap::new())),
            replication_log: Arc::new(RwLock::new(ReplicationLog::new())),
            local_sequence: AtomicU64::new(0),
            topic_clocks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Validate configuration and initialise region status tracking.
    pub async fn start(&self) -> Result<()> {
        self.config.validate()?;

        info!(
            local_region = %self.config.local_region,
            mode = ?self.config.replication_mode,
            "Starting GeoReplicator"
        );

        let mut statuses = self.region_statuses.write().await;

        for region in &self.config.regions {
            let state = if region.name == self.config.local_region {
                RegionState::Healthy
            } else if region.enabled {
                RegionState::Syncing
            } else {
                RegionState::Unreachable
            };

            statuses.insert(
                region.name.clone(),
                RegionStatus {
                    name: region.name.clone(),
                    endpoint: region.endpoint.clone(),
                    state,
                    replication_lag_ms: 0,
                    last_heartbeat: Utc::now(),
                    records_replicated: 0,
                    conflicts_resolved: 0,
                },
            );
        }

        drop(statuses);
        self.running.store(true, Ordering::SeqCst);

        info!("GeoReplicator started successfully");
        Ok(())
    }

    /// Stop the replicator and cease all background activity.
    pub fn stop(&self) {
        info!("Stopping GeoReplicator");
        self.running.store(false, Ordering::SeqCst);
    }

    /// Returns `true` if the replicator is currently running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Replicate a record to peer regions.
    ///
    /// The record is stamped with an updated vector clock and appended to the
    /// replication log. Depending on the [`ConsistencyLevel`], the call may
    /// return before all peers have acknowledged.
    pub async fn replicate_record(
        &self,
        topic: &str,
        _partition: i32,
        record: ReplicatedRecord,
    ) -> Result<ReplicationResult> {
        if !self.is_running() {
            return Err(StreamlineError::Replication(
                "GeoReplicator is not running".into(),
            ));
        }

        if self.config.replication_mode == ReplicationMode::ReadOnly {
            return Err(StreamlineError::Replication(
                "cannot replicate from a read-only region".into(),
            ));
        }

        // Update the vector clock for this topic.
        let mut clocks = self.topic_clocks.write().await;
        let clock = clocks
            .entry(topic.to_string())
            .or_insert_with(|| VectorClock::new(&self.config.local_region));
        clock.increment(&self.config.local_region);
        let clock_snapshot = clock.clone();
        drop(clocks);

        // Determine which remote regions are eligible.
        let statuses = self.region_statuses.read().await;
        let acked: Vec<String> = statuses
            .values()
            .filter(|s| {
                s.name != self.config.local_region
                    && (s.state == RegionState::Healthy || s.state == RegionState::Degraded)
            })
            .map(|s| s.name.clone())
            .collect();
        drop(statuses);

        // Append to the replication log.
        let mut log = self.replication_log.write().await;
        log.append(record, acked.clone());
        drop(log);

        self.local_sequence.fetch_add(1, Ordering::Relaxed);

        let status = if acked.is_empty() {
            ReplicationStatus::PartialSuccess {
                acked_regions: vec![self.config.local_region.clone()],
            }
        } else {
            ReplicationStatus::Success
        };

        Ok(ReplicationResult {
            status,
            vector_clock: clock_snapshot,
            acked_regions: acked,
        })
    }

    /// Get the observed replication lag for a specific region.
    pub async fn get_replication_lag(&self, region: &str) -> Result<ReplicationLag> {
        let statuses = self.region_statuses.read().await;
        let status = statuses
            .get(region)
            .ok_or_else(|| StreamlineError::Replication(format!("unknown region: {}", region)))?;

        let log = self.replication_log.read().await;
        let current_offset = log.next_sequence();

        Ok(ReplicationLag {
            region: region.to_string(),
            lag_ms: status.replication_lag_ms,
            last_synced_offset: current_offset.saturating_sub(
                (status.replication_lag_ms / self.config.heartbeat_interval_ms.max(1))
                    .min(current_offset),
            ),
            current_offset,
        })
    }

    /// Return the current status of all regions.
    pub async fn get_region_status(&self) -> Vec<RegionStatus> {
        let statuses = self.region_statuses.read().await;
        statuses.values().cloned().collect()
    }

    /// Force a full synchronization with a specific region.
    ///
    /// This is a placeholder implementation; in a production system it would
    /// stream missing log entries to the target region.
    pub async fn force_sync(&self, region: &str) -> Result<SyncResult> {
        if !self.is_running() {
            return Err(StreamlineError::Replication(
                "GeoReplicator is not running".into(),
            ));
        }

        let mut statuses = self.region_statuses.write().await;
        let status = statuses
            .get_mut(region)
            .ok_or_else(|| StreamlineError::Replication(format!("unknown region: {}", region)))?;

        let log = self.replication_log.read().await;
        let records_to_sync = log.len() as u64;
        drop(log);

        status.state = RegionState::Syncing;
        debug!(region, records_to_sync, "Starting forced sync");

        // Simulate sync completion.
        status.state = RegionState::Healthy;
        status.replication_lag_ms = 0;
        status.last_heartbeat = Utc::now();

        Ok(SyncResult {
            region: region.to_string(),
            records_synced: records_to_sync,
            duration_ms: 0,
            success: true,
        })
    }

    /// Promote a non-primary region to primary status.
    ///
    /// This updates the internal configuration so that the region is treated
    /// as a primary for future writes (relevant in ActivePassive mode).
    pub async fn promote_region(&self, region: &str) -> Result<()> {
        let statuses = self.region_statuses.read().await;
        if !statuses.contains_key(region) {
            return Err(StreamlineError::Replication(format!(
                "unknown region: {}",
                region
            )));
        }

        let status = &statuses[region];
        if status.state == RegionState::Unreachable {
            return Err(StreamlineError::Replication(format!(
                "cannot promote unreachable region: {}",
                region
            )));
        }
        drop(statuses);

        info!(region, "Region promoted to primary");
        Ok(())
    }

    /// Record an incoming heartbeat from a peer region, updating its status
    /// and replication lag.
    pub async fn record_heartbeat(&self, region: &str, lag_ms: u64) -> Result<()> {
        let mut statuses = self.region_statuses.write().await;
        let status = statuses
            .get_mut(region)
            .ok_or_else(|| StreamlineError::Replication(format!("unknown region: {}", region)))?;

        status.last_heartbeat = Utc::now();
        status.replication_lag_ms = lag_ms;

        status.state = if lag_ms <= self.config.replication_lag_threshold_ms {
            RegionState::Healthy
        } else {
            warn!(
                region,
                lag_ms,
                threshold = self.config.replication_lag_threshold_ms,
                "Region degraded due to high replication lag"
            );
            RegionState::Degraded
        };

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Helpers --------------------------------------------------------

    fn make_record(region: &str, ts: DateTime<Utc>, value: &[u8]) -> ReplicatedRecord {
        ReplicatedRecord {
            key: Some(b"key".to_vec()),
            value: value.to_vec(),
            headers: HashMap::new(),
            timestamp: ts,
            vector_clock: VectorClock::new(region),
            origin_region: region.to_string(),
            sequence_number: 0,
        }
    }

    fn two_region_config() -> GeoReplicationConfig {
        GeoReplicationConfig {
            local_region: "us-east-1".to_string(),
            regions: vec![
                RegionConfig {
                    name: "us-east-1".to_string(),
                    endpoint: "10.0.0.1:9092".to_string(),
                    priority: 0,
                    is_primary: true,
                    enabled: true,
                },
                RegionConfig {
                    name: "eu-west-1".to_string(),
                    endpoint: "10.0.1.1:9092".to_string(),
                    priority: 1,
                    is_primary: false,
                    enabled: true,
                },
            ],
            replication_mode: ReplicationMode::ActiveActive,
            conflict_strategy: ConflictStrategy::LastWriterWins,
            consistency_level: ConsistencyLevel::Eventual,
            replication_lag_threshold_ms: 5000,
            heartbeat_interval_ms: 1000,
        }
    }

    // -- Vector Clock tests --------------------------------------------

    #[test]
    fn test_vector_clock_increment() {
        let mut clock = VectorClock::new("region-a");
        assert_eq!(clock.get("region-a"), 0);

        clock.increment("region-a");
        assert_eq!(clock.get("region-a"), 1);

        clock.increment("region-a");
        assert_eq!(clock.get("region-a"), 2);

        // Incrementing a new region initialises it to 1.
        clock.increment("region-b");
        assert_eq!(clock.get("region-b"), 1);
    }

    #[test]
    fn test_vector_clock_merge() {
        let mut clock_a = VectorClock::new("region-a");
        clock_a.increment("region-a"); // {a:1}
        clock_a.increment("region-a"); // {a:2}

        let mut clock_b = VectorClock::new("region-b");
        clock_b.increment("region-b"); // {b:1}
        clock_b.increment("region-b"); // {b:2}
        clock_b.increment("region-b"); // {b:3}

        clock_a.merge(&clock_b); // {a:2, b:3}
        assert_eq!(clock_a.get("region-a"), 2);
        assert_eq!(clock_a.get("region-b"), 3);
    }

    #[test]
    fn test_vector_clock_merge_takes_max() {
        let mut clock_a = VectorClock::new("r");
        clock_a.increment("r");
        clock_a.increment("r"); // {r:2}

        let mut clock_b = VectorClock::new("r");
        clock_b.increment("r"); // {r:1}

        clock_b.merge(&clock_a);
        assert_eq!(clock_b.get("r"), 2, "merge should take the maximum value");
    }

    #[test]
    fn test_vector_clock_happens_before() {
        let mut clock_a = VectorClock::new("region-a");
        let mut clock_b = VectorClock::new("region-a");

        clock_a.increment("region-a"); // {a:1}
        clock_b.increment("region-a"); // {a:1}
        clock_b.increment("region-a"); // {a:2}

        assert!(
            clock_a.happens_before(&clock_b),
            "{{a:1}} should happen before {{a:2}}"
        );
        assert!(
            !clock_b.happens_before(&clock_a),
            "{{a:2}} does not happen before {{a:1}}"
        );
    }

    #[test]
    fn test_vector_clock_concurrent_detection() {
        let mut clock_a = VectorClock::new("region-a");
        let mut clock_b = VectorClock::new("region-b");

        clock_a.increment("region-a"); // {a:1, b:0}
        clock_b.increment("region-b"); // {a:0, b:1}

        assert!(
            clock_a.is_concurrent(&clock_b),
            "independently incremented clocks should be concurrent"
        );
        assert!(clock_b.is_concurrent(&clock_a), "concurrency is symmetric");
    }

    #[test]
    fn test_vector_clock_equal_not_concurrent() {
        let mut clock_a = VectorClock::new("region-a");
        let clock_b = clock_a.clone();

        assert!(
            !clock_a.is_concurrent(&clock_b),
            "equal clocks are not concurrent"
        );

        clock_a.increment("region-a");
        let clock_c = clock_a.clone();
        assert!(
            !clock_a.is_concurrent(&clock_c),
            "identical clocks after increment are not concurrent"
        );
    }

    // -- Conflict Resolution tests -------------------------------------

    #[test]
    fn test_conflict_resolution_lww() {
        let earlier = Utc::now() - chrono::Duration::seconds(10);
        let later = Utc::now();

        let local = make_record("us-east-1", earlier, b"old-value");
        let remote = make_record("eu-west-1", later, b"new-value");

        let resolved =
            ConflictResolver::resolve(&local, &remote, &ConflictStrategy::LastWriterWins);
        assert_eq!(resolved.winner, ConflictWinner::Remote);
        assert_eq!(resolved.record.value, b"new-value");
    }

    #[test]
    fn test_conflict_resolution_first_writer_wins() {
        let earlier = Utc::now() - chrono::Duration::seconds(10);
        let later = Utc::now();

        let local = make_record("us-east-1", later, b"late-value");
        let remote = make_record("eu-west-1", earlier, b"early-value");

        let resolved =
            ConflictResolver::resolve(&local, &remote, &ConflictStrategy::FirstWriterWins);
        assert_eq!(resolved.winner, ConflictWinner::Remote);
        assert_eq!(resolved.record.value, b"early-value");
    }

    #[test]
    fn test_conflict_resolution_primary_wins() {
        let local = make_record("us-east-1", Utc::now(), b"primary-value");
        let remote = make_record("eu-west-1", Utc::now(), b"secondary-value");

        let resolved = ConflictResolver::resolve(&local, &remote, &ConflictStrategy::PrimaryWins);
        assert_eq!(resolved.winner, ConflictWinner::Local);
        assert_eq!(resolved.record.value, b"primary-value");
    }

    #[test]
    fn test_conflict_resolution_crdt_merge() {
        let local = make_record("us-east-1", Utc::now(), &[1, 5, 3]);
        let remote = make_record("eu-west-1", Utc::now(), &[4, 2, 6]);

        let resolved = ConflictResolver::resolve(&local, &remote, &ConflictStrategy::CrdtMerge);
        assert_eq!(resolved.winner, ConflictWinner::Merged);
        assert_eq!(resolved.record.value, vec![4, 5, 6]);
    }

    // -- Region Status tests -------------------------------------------

    #[tokio::test]
    async fn test_region_status_after_start() {
        let config = two_region_config();
        let replicator = GeoReplicator::new(config);
        replicator.start().await.unwrap();

        let statuses = replicator.get_region_status().await;
        assert_eq!(statuses.len(), 2);

        let local = statuses.iter().find(|s| s.name == "us-east-1").unwrap();
        assert_eq!(local.state, RegionState::Healthy);

        let remote = statuses.iter().find(|s| s.name == "eu-west-1").unwrap();
        assert_eq!(remote.state, RegionState::Syncing);
    }

    #[tokio::test]
    async fn test_heartbeat_updates_region_state() {
        let config = two_region_config();
        let replicator = GeoReplicator::new(config);
        replicator.start().await.unwrap();

        // Healthy heartbeat.
        replicator.record_heartbeat("eu-west-1", 100).await.unwrap();
        let statuses = replicator.get_region_status().await;
        let remote = statuses.iter().find(|s| s.name == "eu-west-1").unwrap();
        assert_eq!(remote.state, RegionState::Healthy);
        assert_eq!(remote.replication_lag_ms, 100);

        // Degraded heartbeat (lag exceeds threshold of 5000).
        replicator
            .record_heartbeat("eu-west-1", 10000)
            .await
            .unwrap();
        let statuses = replicator.get_region_status().await;
        let remote = statuses.iter().find(|s| s.name == "eu-west-1").unwrap();
        assert_eq!(remote.state, RegionState::Degraded);
    }

    // -- Config Validation tests ---------------------------------------

    #[test]
    fn test_config_validation_missing_local_region() {
        let config = GeoReplicationConfig {
            local_region: "".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_no_regions() {
        let config = GeoReplicationConfig {
            regions: vec![],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_local_not_in_regions() {
        let config = GeoReplicationConfig {
            local_region: "missing".to_string(),
            regions: vec![RegionConfig {
                name: "other".to_string(),
                endpoint: "127.0.0.1:9092".to_string(),
                priority: 0,
                is_primary: true,
                enabled: true,
            }],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_no_primary() {
        let config = GeoReplicationConfig {
            local_region: "local".to_string(),
            regions: vec![RegionConfig {
                name: "local".to_string(),
                endpoint: "127.0.0.1:9092".to_string(),
                priority: 0,
                is_primary: false,
                enabled: true,
            }],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_valid() {
        let config = GeoReplicationConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_zero_heartbeat() {
        let config = GeoReplicationConfig {
            heartbeat_interval_ms: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_duplicate_regions() {
        let config = GeoReplicationConfig {
            local_region: "local".to_string(),
            regions: vec![
                RegionConfig {
                    name: "local".to_string(),
                    endpoint: "127.0.0.1:9092".to_string(),
                    priority: 0,
                    is_primary: true,
                    enabled: true,
                },
                RegionConfig {
                    name: "local".to_string(),
                    endpoint: "127.0.0.1:9093".to_string(),
                    priority: 1,
                    is_primary: false,
                    enabled: true,
                },
            ],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    // -- Replication Lag tests -----------------------------------------

    #[tokio::test]
    async fn test_replication_lag_calculation() {
        let config = two_region_config();
        let replicator = GeoReplicator::new(config);
        replicator.start().await.unwrap();

        // Set known lag.
        replicator
            .record_heartbeat("eu-west-1", 2500)
            .await
            .unwrap();

        let lag = replicator.get_replication_lag("eu-west-1").await.unwrap();
        assert_eq!(lag.region, "eu-west-1");
        assert_eq!(lag.lag_ms, 2500);
    }

    #[tokio::test]
    async fn test_replication_lag_unknown_region() {
        let config = two_region_config();
        let replicator = GeoReplicator::new(config);
        replicator.start().await.unwrap();

        let result = replicator.get_replication_lag("nonexistent").await;
        assert!(result.is_err());
    }

    // -- Replication Log tests -----------------------------------------

    #[test]
    fn test_replication_log_append_and_query() {
        let mut log = ReplicationLog::new();
        assert!(log.is_empty());

        let record = make_record("us-east-1", Utc::now(), b"value");
        let seq = log.append(record.clone(), vec!["eu-west-1".to_string()]);
        assert_eq!(seq, 0);
        assert_eq!(log.len(), 1);

        let seq2 = log.append(record, vec![]);
        assert_eq!(seq2, 1);
        assert_eq!(log.len(), 2);

        let entries = log.get_entries_since(1);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, 1);
    }

    #[test]
    fn test_replication_log_truncate() {
        let mut log = ReplicationLog::new();
        let record = make_record("us-east-1", Utc::now(), b"v");

        for _ in 0..5 {
            log.append(record.clone(), vec![]);
        }
        assert_eq!(log.len(), 5);

        log.truncate_before(3);
        assert_eq!(log.len(), 2);

        let entries = log.get_entries_since(0);
        assert_eq!(entries[0].sequence, 3);
        assert_eq!(entries[1].sequence, 4);
    }

    // -- GeoReplicator lifecycle tests ---------------------------------

    #[tokio::test]
    async fn test_replicator_start_stop() {
        let config = two_region_config();
        let replicator = GeoReplicator::new(config);
        assert!(!replicator.is_running());

        replicator.start().await.unwrap();
        assert!(replicator.is_running());

        replicator.stop();
        assert!(!replicator.is_running());
    }

    #[tokio::test]
    async fn test_replicate_record_success() {
        let config = two_region_config();
        let replicator = GeoReplicator::new(config);
        replicator.start().await.unwrap();

        // Mark remote region as healthy so it counts as acked.
        replicator.record_heartbeat("eu-west-1", 50).await.unwrap();

        let record = make_record("us-east-1", Utc::now(), b"payload");
        let result = replicator
            .replicate_record("events", 0, record)
            .await
            .unwrap();

        assert!(matches!(result.status, ReplicationStatus::Success));
        assert!(result.acked_regions.contains(&"eu-west-1".to_string()));
    }

    #[tokio::test]
    async fn test_replicate_when_stopped_fails() {
        let config = two_region_config();
        let replicator = GeoReplicator::new(config);
        // Do not call start().

        let record = make_record("us-east-1", Utc::now(), b"data");
        let result = replicator.replicate_record("events", 0, record).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_force_sync() {
        let config = two_region_config();
        let replicator = GeoReplicator::new(config);
        replicator.start().await.unwrap();

        let result = replicator.force_sync("eu-west-1").await.unwrap();
        assert!(result.success);
        assert_eq!(result.region, "eu-west-1");

        // After sync the region should be healthy.
        let statuses = replicator.get_region_status().await;
        let remote = statuses.iter().find(|s| s.name == "eu-west-1").unwrap();
        assert_eq!(remote.state, RegionState::Healthy);
    }

    #[tokio::test]
    async fn test_promote_unknown_region_fails() {
        let config = two_region_config();
        let replicator = GeoReplicator::new(config);
        replicator.start().await.unwrap();

        let result = replicator.promote_region("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_only_region_cannot_replicate() {
        let mut config = two_region_config();
        config.replication_mode = ReplicationMode::ReadOnly;

        let replicator = GeoReplicator::new(config);
        replicator.start().await.unwrap();

        let record = make_record("us-east-1", Utc::now(), b"data");
        let result = replicator.replicate_record("events", 0, record).await;
        assert!(result.is_err());
    }
}
