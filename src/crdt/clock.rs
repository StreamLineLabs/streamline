//! Clock Infrastructure for CRDTs
//!
//! Provides Hybrid Logical Clocks (HLC) and Vector Clocks for causality tracking
//! in distributed systems.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Hybrid Logical Clock (HLC)
///
/// Combines physical time with a logical counter to provide a monotonically
/// increasing timestamp that respects causality.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HybridLogicalClock {
    /// Physical time component (milliseconds since epoch)
    pub physical: i64,
    /// Logical counter for events at the same physical time
    pub logical: u32,
    /// Node identifier
    pub node_id: String,
}

impl PartialOrd for HybridLogicalClock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HybridLogicalClock {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.physical.cmp(&other.physical) {
            Ordering::Equal => match self.logical.cmp(&other.logical) {
                Ordering::Equal => self.node_id.cmp(&other.node_id),
                other => other,
            },
            other => other,
        }
    }
}

impl Default for HybridLogicalClock {
    fn default() -> Self {
        Self::new("")
    }
}

impl HybridLogicalClock {
    /// Create a new HLC for a node
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            physical: current_time_ms(),
            logical: 0,
            node_id: node_id.into(),
        }
    }

    /// Generate a new timestamp (local event)
    pub fn tick(&mut self) -> HybridLogicalClock {
        let now = current_time_ms();

        if now > self.physical {
            self.physical = now;
            self.logical = 0;
        } else {
            self.logical += 1;
        }

        self.clone()
    }

    /// Update from a received timestamp (receive event)
    pub fn receive(&mut self, other: &HybridLogicalClock) -> HybridLogicalClock {
        let now = current_time_ms();

        if now > self.physical && now > other.physical {
            self.physical = now;
            self.logical = 0;
        } else if self.physical > other.physical {
            self.logical += 1;
        } else if other.physical > self.physical {
            self.physical = other.physical;
            self.logical = other.logical + 1;
        } else {
            // Equal physical times
            self.logical = self.logical.max(other.logical) + 1;
        }

        self.clone()
    }

    /// Compare two HLC timestamps
    pub fn compare(&self, other: &HybridLogicalClock) -> Ordering {
        match self.physical.cmp(&other.physical) {
            Ordering::Equal => match self.logical.cmp(&other.logical) {
                Ordering::Equal => self.node_id.cmp(&other.node_id),
                other => other,
            },
            other => other,
        }
    }

    /// Check if this timestamp happens before another
    pub fn happens_before(&self, other: &HybridLogicalClock) -> bool {
        self.compare(other) == Ordering::Less
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(12 + self.node_id.len() + 1);
        bytes.extend_from_slice(&self.physical.to_le_bytes());
        bytes.extend_from_slice(&self.logical.to_le_bytes());
        bytes.push(self.node_id.len() as u8);
        bytes.extend_from_slice(self.node_id.as_bytes());
        bytes
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 13 {
            return None;
        }

        let physical = i64::from_le_bytes(bytes[0..8].try_into().ok()?);
        let logical = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
        let node_id_len = bytes[12] as usize;

        if bytes.len() < 13 + node_id_len {
            return None;
        }

        let node_id = String::from_utf8(bytes[13..13 + node_id_len].to_vec()).ok()?;

        Some(Self {
            physical,
            logical,
            node_id,
        })
    }
}

/// Vector Clock for tracking causality across multiple nodes
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct VectorClock {
    /// Clock values per node
    pub clocks: HashMap<String, u64>,
}

impl VectorClock {
    /// Create a new empty vector clock
    pub fn new() -> Self {
        Self {
            clocks: HashMap::new(),
        }
    }

    /// Create a vector clock with initial nodes
    pub fn with_nodes(nodes: &[&str]) -> Self {
        let clocks = nodes.iter().map(|&n| (n.to_string(), 0)).collect();
        Self { clocks }
    }

    /// Increment the counter for a node
    pub fn increment(&mut self, node_id: &str) {
        *self.clocks.entry(node_id.to_string()).or_insert(0) += 1;
    }

    /// Get the counter for a node
    pub fn get(&self, node_id: &str) -> u64 {
        self.clocks.get(node_id).copied().unwrap_or(0)
    }

    /// Set the counter for a node
    pub fn set(&mut self, node_id: &str, value: u64) {
        self.clocks.insert(node_id.to_string(), value);
    }

    /// Merge with another vector clock (take max of each component)
    pub fn merge(&mut self, other: &VectorClock) {
        for (node, &value) in &other.clocks {
            let current = self.clocks.entry(node.clone()).or_insert(0);
            *current = (*current).max(value);
        }
    }

    /// Check if this clock happens before another
    pub fn happens_before(&self, other: &VectorClock) -> bool {
        let mut dominated = false;

        // Check all entries in self
        for (node, &value) in &self.clocks {
            let other_value = other.clocks.get(node).copied().unwrap_or(0);
            if value > other_value {
                return false;
            }
            if value < other_value {
                dominated = true;
            }
        }

        // Check entries only in other
        for (node, &value) in &other.clocks {
            if !self.clocks.contains_key(node) && value > 0 {
                dominated = true;
            }
        }

        dominated
    }

    /// Check if two clocks are concurrent (neither happens before the other)
    pub fn is_concurrent(&self, other: &VectorClock) -> bool {
        !self.happens_before(other) && !other.happens_before(self) && self != other
    }

    /// Check if this clock dominates another (happens after or equal)
    pub fn dominates(&self, other: &VectorClock) -> bool {
        for (node, &value) in &other.clocks {
            if self.get(node) < value {
                return false;
            }
        }
        true
    }

    /// Check if this clock satisfies the dependencies in another clock
    /// (i.e., this clock includes all updates that the other clock depends on)
    pub fn satisfies(&self, dependencies: &VectorClock) -> bool {
        self.dominates(dependencies)
    }

    /// Compare two vector clocks
    pub fn compare(&self, other: &VectorClock) -> VectorClockOrdering {
        if self == other {
            VectorClockOrdering::Equal
        } else if self.happens_before(other) {
            VectorClockOrdering::Before
        } else if other.happens_before(self) {
            VectorClockOrdering::After
        } else {
            VectorClockOrdering::Concurrent
        }
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(self.clocks.len() as u32).to_le_bytes());

        for (node, &value) in &self.clocks {
            bytes.push(node.len() as u8);
            bytes.extend_from_slice(node.as_bytes());
            bytes.extend_from_slice(&value.to_le_bytes());
        }

        bytes
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 4 {
            return None;
        }

        let count = u32::from_le_bytes(bytes[0..4].try_into().ok()?) as usize;
        let mut clocks = HashMap::with_capacity(count);
        let mut offset = 4;

        for _ in 0..count {
            if offset >= bytes.len() {
                return None;
            }

            let node_len = bytes[offset] as usize;
            offset += 1;

            if offset + node_len + 8 > bytes.len() {
                return None;
            }

            let node = String::from_utf8(bytes[offset..offset + node_len].to_vec()).ok()?;
            offset += node_len;

            let value = u64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
            offset += 8;

            clocks.insert(node, value);
        }

        Some(Self { clocks })
    }
}

/// Ordering result for vector clocks
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorClockOrdering {
    /// This clock happened before the other
    Before,
    /// This clock happened after the other
    After,
    /// The clocks are equal
    Equal,
    /// The clocks are concurrent (neither happened before the other)
    Concurrent,
}

/// Type alias for clock comparison (same as VectorClockOrdering)
pub type ClockComparison = VectorClockOrdering;

/// Thread-safe HLC clock source
pub struct HlcClockSource {
    physical: AtomicU64,
    logical: AtomicU64,
    node_id: String,
}

impl HlcClockSource {
    /// Create a new clock source for a node
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            physical: AtomicU64::new(current_time_ms() as u64),
            logical: AtomicU64::new(0),
            node_id: node_id.into(),
        }
    }

    /// Generate a new timestamp
    pub fn now(&self) -> HybridLogicalClock {
        let now = current_time_ms() as u64;
        let mut physical = self.physical.load(AtomicOrdering::Acquire);
        let logical;

        loop {
            if now > physical {
                // Try to update physical time
                match self.physical.compare_exchange_weak(
                    physical,
                    now,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                ) {
                    Ok(_) => {
                        self.logical.store(0, AtomicOrdering::Release);
                        logical = 0;
                        physical = now;
                        break;
                    }
                    Err(p) => {
                        physical = p;
                    }
                }
            } else {
                // Increment logical counter
                logical = self.logical.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                break;
            }
        }

        HybridLogicalClock {
            physical: physical as i64,
            logical: logical as u32,
            node_id: self.node_id.clone(),
        }
    }

    /// Get the node ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

/// Get current time in milliseconds since epoch
fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Causal timestamp combining HLC and vector clock
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CausalTimestamp {
    /// Hybrid logical clock for total ordering
    pub hlc: HybridLogicalClock,
    /// Vector clock for causality
    pub vector_clock: VectorClock,
}

impl PartialOrd for CausalTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CausalTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        // Use HLC for total ordering
        self.hlc.cmp(&other.hlc)
    }
}

impl CausalTimestamp {
    /// Create a new causal timestamp
    pub fn new(node_id: impl Into<String>) -> Self {
        let node = node_id.into();
        let mut vc = VectorClock::new();
        vc.increment(&node);

        Self {
            hlc: HybridLogicalClock::new(&node),
            vector_clock: vc,
        }
    }

    /// Create from components
    pub fn from_parts(hlc: HybridLogicalClock, vector_clock: VectorClock) -> Self {
        Self { hlc, vector_clock }
    }

    /// Advance the timestamp (local event)
    pub fn tick(&mut self) {
        self.hlc.tick();
        self.vector_clock.increment(&self.hlc.node_id);
    }

    /// Merge with a received timestamp
    pub fn merge(&mut self, other: &CausalTimestamp) {
        self.hlc.receive(&other.hlc);
        self.vector_clock.merge(&other.vector_clock);
        self.vector_clock.increment(&self.hlc.node_id);
    }

    /// Check if this timestamp causally precedes another
    pub fn causally_precedes(&self, other: &CausalTimestamp) -> bool {
        self.vector_clock.happens_before(&other.vector_clock)
    }

    /// Check if timestamps are concurrent
    pub fn is_concurrent(&self, other: &CausalTimestamp) -> bool {
        self.vector_clock.is_concurrent(&other.vector_clock)
    }
}

impl Default for CausalTimestamp {
    fn default() -> Self {
        Self::new("")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hlc_tick() {
        let mut hlc = HybridLogicalClock::new("node1");
        let t1 = hlc.tick();
        let t2 = hlc.tick();

        assert!(t1.happens_before(&t2));
    }

    #[test]
    fn test_hlc_receive() {
        let mut hlc1 = HybridLogicalClock::new("node1");
        let mut hlc2 = HybridLogicalClock::new("node2");

        let t1 = hlc1.tick();
        hlc2.receive(&t1);
        let t2 = hlc2.tick();

        assert!(t1.happens_before(&t2));
    }

    #[test]
    fn test_hlc_serialization() {
        let hlc = HybridLogicalClock {
            physical: 1234567890,
            logical: 42,
            node_id: "node1".to_string(),
        };

        let bytes = hlc.to_bytes();
        let restored = HybridLogicalClock::from_bytes(&bytes).unwrap();

        assert_eq!(hlc, restored);
    }

    #[test]
    fn test_vector_clock_increment() {
        let mut vc = VectorClock::new();
        vc.increment("node1");
        vc.increment("node1");
        vc.increment("node2");

        assert_eq!(vc.get("node1"), 2);
        assert_eq!(vc.get("node2"), 1);
        assert_eq!(vc.get("node3"), 0);
    }

    #[test]
    fn test_vector_clock_merge() {
        let mut vc1 = VectorClock::new();
        vc1.set("node1", 5);
        vc1.set("node2", 3);

        let mut vc2 = VectorClock::new();
        vc2.set("node1", 3);
        vc2.set("node2", 7);
        vc2.set("node3", 2);

        vc1.merge(&vc2);

        assert_eq!(vc1.get("node1"), 5);
        assert_eq!(vc1.get("node2"), 7);
        assert_eq!(vc1.get("node3"), 2);
    }

    #[test]
    fn test_vector_clock_happens_before() {
        let mut vc1 = VectorClock::new();
        vc1.set("node1", 1);
        vc1.set("node2", 2);

        let mut vc2 = VectorClock::new();
        vc2.set("node1", 2);
        vc2.set("node2", 3);

        assert!(vc1.happens_before(&vc2));
        assert!(!vc2.happens_before(&vc1));
    }

    #[test]
    fn test_vector_clock_concurrent() {
        let mut vc1 = VectorClock::new();
        vc1.set("node1", 2);
        vc1.set("node2", 1);

        let mut vc2 = VectorClock::new();
        vc2.set("node1", 1);
        vc2.set("node2", 2);

        assert!(vc1.is_concurrent(&vc2));
        assert!(vc2.is_concurrent(&vc1));
    }

    #[test]
    fn test_vector_clock_serialization() {
        let mut vc = VectorClock::new();
        vc.set("node1", 5);
        vc.set("node2", 10);

        let bytes = vc.to_bytes();
        let restored = VectorClock::from_bytes(&bytes).unwrap();

        assert_eq!(vc, restored);
    }

    #[test]
    fn test_vector_clock_ordering() {
        let mut vc1 = VectorClock::new();
        vc1.set("a", 1);

        let mut vc2 = VectorClock::new();
        vc2.set("a", 2);

        assert_eq!(vc1.compare(&vc2), VectorClockOrdering::Before);
        assert_eq!(vc2.compare(&vc1), VectorClockOrdering::After);
        assert_eq!(vc1.compare(&vc1), VectorClockOrdering::Equal);
    }

    #[test]
    fn test_causal_timestamp() {
        let mut ts1 = CausalTimestamp::new("node1");
        ts1.tick();

        let mut ts2 = CausalTimestamp::new("node2");
        ts2.merge(&ts1);
        ts2.tick();

        assert!(ts1.causally_precedes(&ts2));
        assert!(!ts2.causally_precedes(&ts1));
    }

    #[test]
    fn test_hlc_clock_source() {
        let source = HlcClockSource::new("node1");

        let t1 = source.now();
        let t2 = source.now();
        let t3 = source.now();

        assert!(t1.happens_before(&t2));
        assert!(t2.happens_before(&t3));
    }
}
