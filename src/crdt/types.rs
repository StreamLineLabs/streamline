//! CRDT Data Types
//!
//! Conflict-free Replicated Data Types for eventually consistent distributed systems.
//! These types automatically converge to the same state regardless of update order.
//!
//! # Implemented Types
//!
//! - **GCounter**: Grow-only counter (increment only)
//! - **PNCounter**: Positive-Negative counter (increment and decrement)
//! - **LWWRegister**: Last-Writer-Wins Register (single value with timestamp)
//! - **ORSet**: Observed-Remove Set (add and remove with unique tags)
//! - **RGASequence**: Replicated Growable Array (ordered sequence)

use super::clock::HybridLogicalClock;
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

/// CRDT type identifier for wire format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrdtType {
    /// Grow-only counter
    GCounter,
    /// Positive-Negative counter
    PNCounter,
    /// Last-Writer-Wins register
    LWWRegister,
    /// Observed-Remove set
    ORSet,
    /// Replicated Growable Array
    RGASequence,
}

impl std::fmt::Display for CrdtType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CrdtType::GCounter => write!(f, "g_counter"),
            CrdtType::PNCounter => write!(f, "pn_counter"),
            CrdtType::LWWRegister => write!(f, "lww_register"),
            CrdtType::ORSet => write!(f, "or_set"),
            CrdtType::RGASequence => write!(f, "rga_sequence"),
        }
    }
}

impl std::str::FromStr for CrdtType {
    type Err = StreamlineError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "g_counter" | "gcounter" | "GCounter" => Ok(CrdtType::GCounter),
            "pn_counter" | "pncounter" | "PNCounter" => Ok(CrdtType::PNCounter),
            "lww_register" | "lwwregister" | "LWWRegister" => Ok(CrdtType::LWWRegister),
            "or_set" | "orset" | "ORSet" => Ok(CrdtType::ORSet),
            "rga_sequence" | "rgasequence" | "RGASequence" => Ok(CrdtType::RGASequence),
            _ => Err(StreamlineError::Config(format!("Unknown CRDT type: {}", s))),
        }
    }
}

/// CRDT operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrdtOperation {
    /// Full state transfer
    State,
    /// Delta (incremental) update
    Delta,
}

/// Trait for all CRDT types
pub trait Crdt: Clone + Send + Sync {
    /// Merge another CRDT state into this one
    fn merge(&mut self, other: &Self);

    /// Get the CRDT type identifier
    fn crdt_type(&self) -> CrdtType;

    /// Check if this CRDT is empty/default
    fn is_empty(&self) -> bool;

    /// Serialize to bytes
    fn to_bytes(&self) -> Result<Vec<u8>>
    where
        Self: Serialize,
    {
        Ok(serde_json::to_vec(self)?)
    }

    /// Deserialize from bytes
    fn from_bytes(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized + for<'de> Deserialize<'de>,
    {
        Ok(serde_json::from_slice(bytes)?)
    }
}

// ============================================================================
// GCounter - Grow-only Counter
// ============================================================================

/// Grow-only Counter (G-Counter)
///
/// A counter that can only be incremented. Each node maintains its own count,
/// and the total value is the sum of all node counts.
///
/// # Example
///
/// ```rust,ignore
/// use streamline::crdt::GCounter;
///
/// let mut counter1 = GCounter::new("node1");
/// counter1.increment(5);
///
/// let mut counter2 = GCounter::new("node2");
/// counter2.increment(3);
///
/// counter1.merge(&counter2);
/// assert_eq!(counter1.value(), 8);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GCounter {
    /// Per-node counts
    counts: HashMap<String, u64>,
    /// Local node ID for increment operations
    #[serde(skip)]
    local_node: Option<String>,
}

impl GCounter {
    /// Create a new G-Counter for a specific node
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            counts: HashMap::new(),
            local_node: Some(node_id.into()),
        }
    }

    /// Create a G-Counter without a local node (for merge-only operations)
    pub fn empty() -> Self {
        Self {
            counts: HashMap::new(),
            local_node: None,
        }
    }

    /// Increment the counter by a value
    pub fn increment(&mut self, amount: u64) {
        if let Some(ref node_id) = self.local_node {
            *self.counts.entry(node_id.clone()).or_insert(0) += amount;
        }
    }

    /// Increment by 1
    pub fn inc(&mut self) {
        self.increment(1);
    }

    /// Get the total counter value
    pub fn value(&self) -> u64 {
        self.counts.values().sum()
    }

    /// Get the count for a specific node
    pub fn node_value(&self, node_id: &str) -> u64 {
        self.counts.get(node_id).copied().unwrap_or(0)
    }

    /// Get all node counts
    pub fn counts(&self) -> &HashMap<String, u64> {
        &self.counts
    }

    /// Set the local node ID
    pub fn set_local_node(&mut self, node_id: impl Into<String>) {
        self.local_node = Some(node_id.into());
    }
}

impl Default for GCounter {
    fn default() -> Self {
        Self::empty()
    }
}

impl Crdt for GCounter {
    fn merge(&mut self, other: &Self) {
        for (node_id, &count) in &other.counts {
            let entry = self.counts.entry(node_id.clone()).or_insert(0);
            *entry = (*entry).max(count);
        }
    }

    fn crdt_type(&self) -> CrdtType {
        CrdtType::GCounter
    }

    fn is_empty(&self) -> bool {
        self.counts.is_empty() || self.value() == 0
    }
}

// ============================================================================
// PNCounter - Positive-Negative Counter
// ============================================================================

/// Positive-Negative Counter (PN-Counter)
///
/// A counter that supports both increment and decrement operations.
/// Internally uses two G-Counters: one for increments, one for decrements.
///
/// # Example
///
/// ```rust,ignore
/// use streamline::crdt::PNCounter;
///
/// let mut counter = PNCounter::new("node1");
/// counter.increment(10);
/// counter.decrement(3);
/// assert_eq!(counter.value(), 7);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PNCounter {
    /// Positive (increment) counter
    positive: GCounter,
    /// Negative (decrement) counter
    negative: GCounter,
}

impl PNCounter {
    /// Create a new PN-Counter for a specific node
    pub fn new(node_id: impl Into<String>) -> Self {
        let node = node_id.into();
        Self {
            positive: GCounter::new(node.clone()),
            negative: GCounter::new(node),
        }
    }

    /// Create a PN-Counter without a local node
    pub fn empty() -> Self {
        Self {
            positive: GCounter::empty(),
            negative: GCounter::empty(),
        }
    }

    /// Increment the counter
    pub fn increment(&mut self, amount: u64) {
        self.positive.increment(amount);
    }

    /// Increment by 1
    pub fn inc(&mut self) {
        self.increment(1);
    }

    /// Decrement the counter
    pub fn decrement(&mut self, amount: u64) {
        self.negative.increment(amount);
    }

    /// Decrement by 1
    pub fn dec(&mut self) {
        self.decrement(1);
    }

    /// Get the counter value (can be negative)
    pub fn value(&self) -> i64 {
        self.positive.value() as i64 - self.negative.value() as i64
    }

    /// Get the positive count
    pub fn positive_value(&self) -> u64 {
        self.positive.value()
    }

    /// Get the negative count
    pub fn negative_value(&self) -> u64 {
        self.negative.value()
    }

    /// Set the local node ID
    pub fn set_local_node(&mut self, node_id: impl Into<String>) {
        let node = node_id.into();
        self.positive.set_local_node(node.clone());
        self.negative.set_local_node(node);
    }
}

impl Default for PNCounter {
    fn default() -> Self {
        Self::empty()
    }
}

impl Crdt for PNCounter {
    fn merge(&mut self, other: &Self) {
        self.positive.merge(&other.positive);
        self.negative.merge(&other.negative);
    }

    fn crdt_type(&self) -> CrdtType {
        CrdtType::PNCounter
    }

    fn is_empty(&self) -> bool {
        self.positive.is_empty() && self.negative.is_empty()
    }
}

// ============================================================================
// LWWRegister - Last-Writer-Wins Register
// ============================================================================

/// Last-Writer-Wins Register (LWW-Register)
///
/// A register that stores a single value. Concurrent writes are resolved
/// by keeping the value with the highest timestamp.
///
/// # Example
///
/// ```rust,ignore
/// use streamline::crdt::LWWRegister;
///
/// let mut reg1 = LWWRegister::new("node1", "initial");
/// reg1.set("value1");
///
/// let mut reg2 = LWWRegister::new("node2", "initial");
/// reg2.set("value2");
///
/// // After merge, the register with the later timestamp wins
/// reg1.merge(&reg2);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LWWRegister<T>
where
    T: Clone + Send + Sync,
{
    /// Current value
    value: T,
    /// Timestamp when the value was set
    timestamp: HybridLogicalClock,
    /// Whether this register has been set
    initialized: bool,
}

impl<T> LWWRegister<T>
where
    T: Clone + Send + Sync,
{
    /// Create a new LWW-Register with an initial value
    pub fn new(node_id: impl Into<String>, value: T) -> Self {
        Self {
            value,
            timestamp: HybridLogicalClock::new(node_id),
            initialized: true,
        }
    }

    /// Create an uninitialized register
    pub fn uninitialized(node_id: impl Into<String>, default: T) -> Self {
        Self {
            value: default,
            timestamp: HybridLogicalClock::new(node_id),
            initialized: false,
        }
    }

    /// Set the register value
    pub fn set(&mut self, value: T) {
        self.value = value;
        self.timestamp = self.timestamp.tick();
        self.initialized = true;
    }

    /// Set the register value with an external timestamp
    pub fn set_with_timestamp(&mut self, value: T, timestamp: HybridLogicalClock) {
        if !self.initialized || timestamp > self.timestamp {
            self.value = value;
            self.timestamp = timestamp;
            self.initialized = true;
        }
    }

    /// Get the current value
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Get the current value if initialized
    pub fn get(&self) -> Option<&T> {
        if self.initialized {
            Some(&self.value)
        } else {
            None
        }
    }

    /// Get the timestamp
    pub fn timestamp(&self) -> &HybridLogicalClock {
        &self.timestamp
    }

    /// Check if the register is initialized
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }
}

impl<T> Crdt for LWWRegister<T>
where
    T: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
    fn merge(&mut self, other: &Self) {
        if !other.initialized {
            return;
        }
        if !self.initialized || other.timestamp > self.timestamp {
            self.value = other.value.clone();
            self.timestamp = other.timestamp.clone();
            self.initialized = true;
        }
    }

    fn crdt_type(&self) -> CrdtType {
        CrdtType::LWWRegister
    }

    fn is_empty(&self) -> bool {
        !self.initialized
    }
}

impl<T> PartialEq for LWWRegister<T>
where
    T: Clone + Send + Sync + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.timestamp == other.timestamp
    }
}

// ============================================================================
// ORSet - Observed-Remove Set
// ============================================================================

/// Unique tag for OR-Set elements
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UniqueTag {
    /// Node that created the tag
    pub node_id: String,
    /// Sequence number within the node
    pub sequence: u64,
}

impl UniqueTag {
    /// Create a new unique tag
    pub fn new(node_id: impl Into<String>, sequence: u64) -> Self {
        Self {
            node_id: node_id.into(),
            sequence,
        }
    }
}

/// Observed-Remove Set (OR-Set)
///
/// A set that supports both add and remove operations without conflicts.
/// Each add creates a unique tag, and remove only removes observed tags.
///
/// # Example
///
/// ```rust,ignore
/// use streamline::crdt::ORSet;
///
/// let mut set1: ORSet<String> = ORSet::new("node1");
/// set1.add("apple".to_string());
/// set1.add("banana".to_string());
///
/// let mut set2 = set1.clone();
/// set2.set_local_node("node2");
///
/// set1.remove(&"apple".to_string());  // Remove on set1
/// set2.add("cherry".to_string());     // Add on set2
///
/// set1.merge(&set2);
/// // set1 now contains: banana, cherry (apple was removed)
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ORSet<T>
where
    T: Clone + Eq + Hash + Send + Sync,
{
    /// Elements with their unique tags
    elements: HashMap<T, HashSet<UniqueTag>>,
    /// Tombstones (removed tags)
    tombstones: HashSet<UniqueTag>,
    /// Local node ID
    #[serde(skip)]
    local_node: Option<String>,
    /// Sequence counter for generating unique tags
    #[serde(skip)]
    sequence: u64,
}

impl<T> ORSet<T>
where
    T: Clone + Eq + Hash + Send + Sync,
{
    /// Create a new OR-Set for a specific node
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            elements: HashMap::new(),
            tombstones: HashSet::new(),
            local_node: Some(node_id.into()),
            sequence: 0,
        }
    }

    /// Create an OR-Set without a local node
    pub fn empty() -> Self {
        Self {
            elements: HashMap::new(),
            tombstones: HashSet::new(),
            local_node: None,
            sequence: 0,
        }
    }

    /// Set the local node ID
    pub fn set_local_node(&mut self, node_id: impl Into<String>) {
        self.local_node = Some(node_id.into());
    }

    /// Generate a new unique tag
    fn new_tag(&mut self) -> Option<UniqueTag> {
        self.local_node.as_ref().map(|node_id| {
            self.sequence += 1;
            UniqueTag::new(node_id.clone(), self.sequence)
        })
    }

    /// Add an element to the set
    pub fn add(&mut self, element: T) -> bool {
        if let Some(tag) = self.new_tag() {
            self.elements.entry(element).or_default().insert(tag);
            true
        } else {
            false
        }
    }

    /// Remove an element from the set
    pub fn remove(&mut self, element: &T) -> bool {
        if let Some(tags) = self.elements.remove(element) {
            for tag in tags {
                self.tombstones.insert(tag);
            }
            true
        } else {
            false
        }
    }

    /// Check if the set contains an element
    pub fn contains(&self, element: &T) -> bool {
        self.elements
            .get(element)
            .map(|tags| !tags.is_empty())
            .unwrap_or(false)
    }

    /// Get all elements in the set
    pub fn elements(&self) -> Vec<&T> {
        self.elements
            .iter()
            .filter(|(_, tags)| !tags.is_empty())
            .map(|(elem, _)| elem)
            .collect()
    }

    /// Get the number of elements
    pub fn len(&self) -> usize {
        self.elements
            .iter()
            .filter(|(_, tags)| !tags.is_empty())
            .count()
    }

    /// Check if the set is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all elements
    pub fn clear(&mut self) {
        for (_, tags) in self.elements.drain() {
            for tag in tags {
                self.tombstones.insert(tag);
            }
        }
    }

    /// Get the raw elements map (for debugging)
    pub fn raw_elements(&self) -> &HashMap<T, HashSet<UniqueTag>> {
        &self.elements
    }

    /// Get the tombstones (for debugging)
    pub fn raw_tombstones(&self) -> &HashSet<UniqueTag> {
        &self.tombstones
    }
}

impl<T> Default for ORSet<T>
where
    T: Clone + Eq + Hash + Send + Sync,
{
    fn default() -> Self {
        Self::empty()
    }
}

impl<T> Crdt for ORSet<T>
where
    T: Clone + Eq + Hash + Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
    fn merge(&mut self, other: &Self) {
        // Merge tombstones
        self.tombstones.extend(other.tombstones.iter().cloned());

        // Merge elements, filtering out tombstoned tags
        for (element, other_tags) in &other.elements {
            let entry = self.elements.entry(element.clone()).or_default();
            for tag in other_tags {
                if !self.tombstones.contains(tag) {
                    entry.insert(tag.clone());
                }
            }
        }

        // Remove tombstoned tags from existing elements
        for tags in self.elements.values_mut() {
            tags.retain(|tag| !self.tombstones.contains(tag));
        }

        // Remove empty entries
        self.elements.retain(|_, tags| !tags.is_empty());
    }

    fn crdt_type(&self) -> CrdtType {
        CrdtType::ORSet
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> PartialEq for ORSet<T>
where
    T: Clone + Eq + Hash + Send + Sync,
{
    fn eq(&self, other: &Self) -> bool {
        self.elements == other.elements && self.tombstones == other.tombstones
    }
}

// ============================================================================
// RGASequence - Replicated Growable Array
// ============================================================================

/// Unique identifier for RGA elements
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RgaId {
    /// Timestamp of insertion
    pub timestamp: HybridLogicalClock,
    /// Position hint for ordering
    pub position: u64,
}

impl RgaId {
    /// Create a new RGA ID
    pub fn new(timestamp: HybridLogicalClock, position: u64) -> Self {
        Self {
            timestamp,
            position,
        }
    }
}

impl PartialOrd for RgaId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RgaId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.timestamp.cmp(&other.timestamp) {
            std::cmp::Ordering::Equal => self.position.cmp(&other.position),
            other => other,
        }
    }
}

/// RGA Element
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RgaElement<T>
where
    T: Clone + Send + Sync,
{
    /// Element value
    pub value: T,
    /// Unique identifier
    pub id: RgaId,
    /// ID of the element this comes after (None for first element)
    pub after: Option<RgaId>,
    /// Whether this element is deleted (tombstone)
    pub deleted: bool,
}

impl<T> RgaElement<T>
where
    T: Clone + Send + Sync,
{
    /// Create a new RGA element
    pub fn new(value: T, id: RgaId, after: Option<RgaId>) -> Self {
        Self {
            value,
            id,
            after,
            deleted: false,
        }
    }
}

/// Replicated Growable Array (RGA)
///
/// An ordered sequence that supports insert and delete operations.
/// Concurrent inserts at the same position are resolved using timestamps.
///
/// # Example
///
/// ```rust,ignore
/// use streamline::crdt::RGASequence;
///
/// let mut seq: RGASequence<char> = RGASequence::new("node1");
/// seq.insert(0, 'a');
/// seq.insert(1, 'b');
/// seq.insert(2, 'c');
///
/// assert_eq!(seq.to_vec(), vec!['a', 'b', 'c']);
///
/// seq.delete(1);  // Remove 'b'
/// assert_eq!(seq.to_vec(), vec!['a', 'c']);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RGASequence<T>
where
    T: Clone + Send + Sync,
{
    /// All elements (including tombstones)
    elements: Vec<RgaElement<T>>,
    /// Local node ID
    #[serde(skip)]
    local_node: Option<String>,
    /// Position counter for IDs
    #[serde(skip)]
    position: u64,
}

impl<T> RGASequence<T>
where
    T: Clone + Send + Sync,
{
    /// Create a new RGA sequence for a specific node
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            elements: Vec::new(),
            local_node: Some(node_id.into()),
            position: 0,
        }
    }

    /// Create an RGA sequence without a local node
    pub fn empty() -> Self {
        Self {
            elements: Vec::new(),
            local_node: None,
            position: 0,
        }
    }

    /// Set the local node ID
    pub fn set_local_node(&mut self, node_id: impl Into<String>) {
        self.local_node = Some(node_id.into());
    }

    /// Generate a new RGA ID
    fn new_id(&mut self) -> Option<RgaId> {
        self.local_node.as_ref().map(|node_id| {
            self.position += 1;
            RgaId::new(HybridLogicalClock::new(node_id.clone()), self.position)
        })
    }

    /// Get the visible index for an internal index
    #[allow(dead_code)]
    fn visible_index(&self, internal_idx: usize) -> usize {
        self.elements[..internal_idx]
            .iter()
            .filter(|e| !e.deleted)
            .count()
    }

    /// Get the internal index for a visible index
    fn internal_index(&self, visible_idx: usize) -> Option<usize> {
        let mut visible_count = 0;
        for (i, elem) in self.elements.iter().enumerate() {
            if !elem.deleted {
                if visible_count == visible_idx {
                    return Some(i);
                }
                visible_count += 1;
            }
        }
        if visible_count == visible_idx {
            Some(self.elements.len())
        } else {
            None
        }
    }

    /// Find the position to insert a new element
    fn find_insert_position(&self, after: Option<&RgaId>, new_id: &RgaId) -> usize {
        let start_pos = if let Some(after_id) = after {
            // Find the element with this ID
            self.elements
                .iter()
                .position(|e| &e.id == after_id)
                .map(|i| i + 1)
                .unwrap_or(0)
        } else {
            0
        };

        // Find the correct position among elements with the same 'after'
        let mut pos = start_pos;
        while pos < self.elements.len() {
            let elem = &self.elements[pos];
            // If this element has a different 'after', stop
            if elem.after.as_ref() != after {
                break;
            }
            // If new_id should come before this element, stop
            if new_id > &elem.id {
                break;
            }
            pos += 1;
        }

        pos
    }

    /// Insert an element at the given visible index
    pub fn insert(&mut self, index: usize, value: T) -> bool {
        let Some(id) = self.new_id() else {
            return false;
        };

        let after = if index == 0 {
            None
        } else {
            self.internal_index(index - 1)
                .map(|i| self.elements[i].id.clone())
        };

        let element = RgaElement::new(value, id.clone(), after.clone());
        let pos = self.find_insert_position(after.as_ref(), &id);
        self.elements.insert(pos, element);
        true
    }

    /// Insert an element with a specific RGA element (for merge)
    fn insert_element(&mut self, element: RgaElement<T>) {
        let pos = self.find_insert_position(element.after.as_ref(), &element.id);

        // Check if element already exists
        if self.elements.iter().any(|e| e.id == element.id) {
            return;
        }

        self.elements.insert(pos, element);
    }

    /// Append an element to the end
    pub fn push(&mut self, value: T) -> bool {
        let len = self.len();
        self.insert(len, value)
    }

    /// Delete the element at the given visible index
    pub fn delete(&mut self, index: usize) -> bool {
        if let Some(internal_idx) = self.internal_index(index) {
            if internal_idx < self.elements.len() {
                self.elements[internal_idx].deleted = true;
                return true;
            }
        }
        false
    }

    /// Get the element at the given visible index
    pub fn get(&self, index: usize) -> Option<&T> {
        self.internal_index(index)
            .filter(|&i| i < self.elements.len())
            .map(|i| &self.elements[i].value)
    }

    /// Get the number of visible elements
    pub fn len(&self) -> usize {
        self.elements.iter().filter(|e| !e.deleted).count()
    }

    /// Check if the sequence is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Convert to a vector of visible elements
    pub fn to_vec(&self) -> Vec<T> {
        self.elements
            .iter()
            .filter(|e| !e.deleted)
            .map(|e| e.value.clone())
            .collect()
    }

    /// Iterate over visible elements
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.elements
            .iter()
            .filter(|e| !e.deleted)
            .map(|e| &e.value)
    }

    /// Get raw elements (for debugging)
    pub fn raw_elements(&self) -> &[RgaElement<T>] {
        &self.elements
    }
}

impl<T> Default for RGASequence<T>
where
    T: Clone + Send + Sync,
{
    fn default() -> Self {
        Self::empty()
    }
}

impl<T> Crdt for RGASequence<T>
where
    T: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
    fn merge(&mut self, other: &Self) {
        // Merge all elements from other
        for element in &other.elements {
            // Check if we have this element
            if let Some(our_elem) = self.elements.iter_mut().find(|e| e.id == element.id) {
                // Merge deletion status (OR - if either deleted, it's deleted)
                our_elem.deleted = our_elem.deleted || element.deleted;
            } else {
                // Insert the element
                self.insert_element(element.clone());
            }
        }
    }

    fn crdt_type(&self) -> CrdtType {
        CrdtType::RGASequence
    }

    fn is_empty(&self) -> bool {
        RGASequence::is_empty(self)
    }
}

impl<T> PartialEq for RGASequence<T>
where
    T: Clone + Send + Sync + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        // Compare visible elements only
        self.to_vec() == other.to_vec()
    }
}

// ============================================================================
// Helper types for wire format
// ============================================================================

/// Container for any CRDT type (for serialization)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum CrdtValue {
    /// G-Counter
    GCounter(GCounter),
    /// PN-Counter
    PNCounter(PNCounter),
    /// LWW-Register with string value
    LWWRegisterString(LWWRegister<String>),
    /// LWW-Register with bytes value
    LWWRegisterBytes(LWWRegister<Vec<u8>>),
    /// OR-Set with string elements
    ORSetString(ORSet<String>),
    /// OR-Set with i64 elements
    ORSetI64(ORSet<i64>),
    /// RGA-Sequence with string elements
    RGASequenceString(RGASequence<String>),
    /// RGA-Sequence with char elements
    RGASequenceChar(RGASequence<char>),
}

impl CrdtValue {
    /// Get the CRDT type
    pub fn crdt_type(&self) -> CrdtType {
        match self {
            CrdtValue::GCounter(_) => CrdtType::GCounter,
            CrdtValue::PNCounter(_) => CrdtType::PNCounter,
            CrdtValue::LWWRegisterString(_) | CrdtValue::LWWRegisterBytes(_) => {
                CrdtType::LWWRegister
            }
            CrdtValue::ORSetString(_) | CrdtValue::ORSetI64(_) => CrdtType::ORSet,
            CrdtValue::RGASequenceString(_) | CrdtValue::RGASequenceChar(_) => {
                CrdtType::RGASequence
            }
        }
    }

    /// Merge with another CRDT value
    pub fn merge(&mut self, other: &CrdtValue) -> Result<()> {
        match (self, other) {
            (CrdtValue::GCounter(a), CrdtValue::GCounter(b)) => {
                a.merge(b);
                Ok(())
            }
            (CrdtValue::PNCounter(a), CrdtValue::PNCounter(b)) => {
                a.merge(b);
                Ok(())
            }
            (CrdtValue::LWWRegisterString(a), CrdtValue::LWWRegisterString(b)) => {
                a.merge(b);
                Ok(())
            }
            (CrdtValue::LWWRegisterBytes(a), CrdtValue::LWWRegisterBytes(b)) => {
                a.merge(b);
                Ok(())
            }
            (CrdtValue::ORSetString(a), CrdtValue::ORSetString(b)) => {
                a.merge(b);
                Ok(())
            }
            (CrdtValue::ORSetI64(a), CrdtValue::ORSetI64(b)) => {
                a.merge(b);
                Ok(())
            }
            (CrdtValue::RGASequenceString(a), CrdtValue::RGASequenceString(b)) => {
                a.merge(b);
                Ok(())
            }
            (CrdtValue::RGASequenceChar(a), CrdtValue::RGASequenceChar(b)) => {
                a.merge(b);
                Ok(())
            }
            _ => Err(StreamlineError::Config(
                "Cannot merge different CRDT types".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gcounter_basic() {
        let mut counter = GCounter::new("node1");
        counter.increment(5);
        counter.increment(3);
        assert_eq!(counter.value(), 8);
    }

    #[test]
    fn test_gcounter_merge() {
        let mut counter1 = GCounter::new("node1");
        counter1.increment(5);

        let mut counter2 = GCounter::new("node2");
        counter2.increment(3);

        counter1.merge(&counter2);
        assert_eq!(counter1.value(), 8);

        // Merge is idempotent
        counter1.merge(&counter2);
        assert_eq!(counter1.value(), 8);
    }

    #[test]
    fn test_pncounter_basic() {
        let mut counter = PNCounter::new("node1");
        counter.increment(10);
        counter.decrement(3);
        assert_eq!(counter.value(), 7);
    }

    #[test]
    fn test_pncounter_negative() {
        let mut counter = PNCounter::new("node1");
        counter.decrement(5);
        assert_eq!(counter.value(), -5);
    }

    #[test]
    fn test_pncounter_merge() {
        let mut counter1 = PNCounter::new("node1");
        counter1.increment(10);

        let mut counter2 = PNCounter::new("node2");
        counter2.decrement(3);

        counter1.merge(&counter2);
        assert_eq!(counter1.value(), 7);
    }

    #[test]
    fn test_lwwregister_basic() {
        let mut reg = LWWRegister::new("node1", "initial");
        assert_eq!(reg.value(), &"initial");

        reg.set("updated");
        assert_eq!(reg.value(), &"updated");
    }

    #[test]
    fn test_lwwregister_merge() {
        let mut reg1 = LWWRegister::new("node1", "value1".to_string());
        std::thread::sleep(std::time::Duration::from_millis(1));
        let reg2 = LWWRegister::new("node2", "value2".to_string());

        // reg2 has later timestamp, so it wins
        reg1.merge(&reg2);
        assert_eq!(reg1.value(), "value2");
    }

    #[test]
    fn test_orset_add_remove() {
        let mut set: ORSet<String> = ORSet::new("node1");
        set.add("apple".to_string());
        set.add("banana".to_string());

        assert!(set.contains(&"apple".to_string()));
        assert!(set.contains(&"banana".to_string()));
        assert_eq!(set.len(), 2);

        set.remove(&"apple".to_string());
        assert!(!set.contains(&"apple".to_string()));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_orset_merge() {
        let mut set1: ORSet<String> = ORSet::new("node1");
        set1.add("apple".to_string());

        let mut set2: ORSet<String> = ORSet::new("node2");
        set2.add("banana".to_string());

        set1.merge(&set2);
        assert!(set1.contains(&"apple".to_string()));
        assert!(set1.contains(&"banana".to_string()));
        assert_eq!(set1.len(), 2);
    }

    #[test]
    fn test_orset_concurrent_add_remove() {
        let mut set1: ORSet<String> = ORSet::new("node1");
        set1.add("apple".to_string());

        let mut set2 = set1.clone();
        set2.set_local_node("node2");

        // node1 removes apple
        set1.remove(&"apple".to_string());

        // node2 adds apple again (concurrent with remove)
        set2.add("apple".to_string());

        // After merge, apple should exist (add wins over concurrent remove)
        set1.merge(&set2);
        assert!(set1.contains(&"apple".to_string()));
    }

    #[test]
    fn test_rga_sequence_basic() {
        let mut seq: RGASequence<char> = RGASequence::new("node1");
        seq.push('a');
        seq.push('b');
        seq.push('c');

        assert_eq!(seq.to_vec(), vec!['a', 'b', 'c']);
        assert_eq!(seq.len(), 3);
    }

    #[test]
    fn test_rga_sequence_insert() {
        let mut seq: RGASequence<char> = RGASequence::new("node1");
        seq.insert(0, 'a');
        seq.insert(1, 'c');
        seq.insert(1, 'b'); // Insert between a and c

        assert_eq!(seq.to_vec(), vec!['a', 'b', 'c']);
    }

    #[test]
    fn test_rga_sequence_delete() {
        let mut seq: RGASequence<char> = RGASequence::new("node1");
        seq.push('a');
        seq.push('b');
        seq.push('c');

        seq.delete(1); // Delete 'b'
        assert_eq!(seq.to_vec(), vec!['a', 'c']);
    }

    #[test]
    fn test_rga_sequence_merge() {
        let mut seq1: RGASequence<char> = RGASequence::new("node1");
        seq1.push('a');
        seq1.push('b');

        let mut seq2: RGASequence<char> = RGASequence::new("node2");
        seq2.push('x');
        seq2.push('y');

        seq1.merge(&seq2);
        // Both sequences merged - order depends on timestamps
        assert_eq!(seq1.len(), 4);
    }

    #[test]
    fn test_crdt_type_parse() {
        assert_eq!("g_counter".parse::<CrdtType>().unwrap(), CrdtType::GCounter);
        assert_eq!(
            "pn_counter".parse::<CrdtType>().unwrap(),
            CrdtType::PNCounter
        );
        assert_eq!(
            "lww_register".parse::<CrdtType>().unwrap(),
            CrdtType::LWWRegister
        );
        assert_eq!("or_set".parse::<CrdtType>().unwrap(), CrdtType::ORSet);
        assert_eq!(
            "rga_sequence".parse::<CrdtType>().unwrap(),
            CrdtType::RGASequence
        );
    }

    #[test]
    fn test_crdt_serialization() {
        let mut counter = GCounter::new("node1");
        counter.increment(42);

        let bytes = counter.to_bytes().unwrap();
        let restored = GCounter::from_bytes(&bytes).unwrap();

        assert_eq!(counter.value(), restored.value());
    }
}
