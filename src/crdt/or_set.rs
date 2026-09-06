//! Observed-Remove Set (ORSet) CRDT.
//!
//! An ORSet supports both `add` and `remove` with unique tags. Each add
//! operation assigns a globally unique tag to the element. Remove deletes
//! only the tags that the removing replica has *observed*. A concurrent add
//! from another replica introduces a fresh tag that survives the remove,
//! implementing **add-wins** semantics.
//!
//! # Properties
//!
//! - **Add-wins**: Concurrent add + remove → the element remains.
//! - **Convergent**: Replicas always converge regardless of merge order.
//! - **Unique tags**: Each add generates a unique tag to distinguish
//!   concurrent additions.
//!
//! # Use Cases
//!
//! - Shopping carts (add/remove items concurrently).
//! - Tag sets where items can be added and removed.
//! - Distributed membership with join/leave semantics.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

/// A unique tag assigned to each add operation.
///
/// Composed of a node identifier and a monotonic sequence number to
/// ensure global uniqueness without coordination.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Tag {
    /// Node that generated this tag.
    pub node: String,
    /// Monotonic counter local to the node.
    pub seq: u64,
}

/// An Observed-Remove Set CRDT with add-wins semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservedRemoveSet<T: Eq + Hash + Clone> {
    /// Mapping from element to the set of tags that "support" its presence.
    entries: HashMap<T, HashSet<Tag>>,
    /// Local node identifier.
    local_node: String,
    /// Monotonic counter for generating unique tags.
    counter: u64,
}

impl<T: Eq + Hash + Clone> ObservedRemoveSet<T> {
    /// Creates a new ORSet for the given node.
    pub fn new(node: impl Into<String>) -> Self {
        Self {
            entries: HashMap::new(),
            local_node: node.into(),
            counter: 0,
        }
    }

    /// Adds an element, generating a fresh unique tag.
    pub fn add(&mut self, value: T) {
        self.counter += 1;
        let tag = Tag {
            node: self.local_node.clone(),
            seq: self.counter,
        };
        self.entries.entry(value).or_default().insert(tag);
    }

    /// Removes an element by clearing all *observed* tags.
    ///
    /// If another replica concurrently adds the same element, its fresh
    /// tag will survive this remove (add-wins semantics).
    pub fn remove(&mut self, value: &T) {
        self.entries.remove(value);
    }

    /// Returns true if the element is present (has at least one supporting tag).
    pub fn contains(&self, value: &T) -> bool {
        self.entries
            .get(value)
            .map(|tags| !tags.is_empty())
            .unwrap_or(false)
    }

    /// Returns the number of distinct elements in the set.
    pub fn len(&self) -> usize {
        self.entries
            .values()
            .filter(|tags| !tags.is_empty())
            .count()
    }

    /// Returns true if the set contains no elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns an iterator over the elements currently in the set.
    pub fn elements(&self) -> Vec<&T> {
        self.entries
            .iter()
            .filter(|(_, tags)| !tags.is_empty())
            .map(|(elem, _)| elem)
            .collect()
    }

    /// Sets the local node identifier (for use after cloning a replica).
    pub fn set_local_node(&mut self, node: impl Into<String>) {
        self.local_node = node.into();
    }

    /// Merges another ORSet into this one.
    ///
    /// For each element, the merged tag set is the union of both replicas'
    /// tags. Tags that were removed on one replica but not observed by the
    /// other are handled by the add-wins property.
    pub fn merge(&mut self, other: &ObservedRemoveSet<T>) {
        for (elem, other_tags) in &other.entries {
            let entry = self.entries.entry(elem.clone()).or_default();
            for tag in other_tags {
                entry.insert(tag.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_set() {
        let set: ObservedRemoveSet<String> = ObservedRemoveSet::new("node1");
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn add_and_contains() {
        let mut set = ObservedRemoveSet::new("node1");
        set.add("apple".to_string());
        assert!(set.contains(&"apple".to_string()));
        assert!(!set.contains(&"banana".to_string()));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn remove_element() {
        let mut set = ObservedRemoveSet::new("node1");
        set.add("apple".to_string());
        assert!(set.contains(&"apple".to_string()));
        set.remove(&"apple".to_string());
        assert!(!set.contains(&"apple".to_string()));
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn add_wins_on_concurrent_add_remove() {
        // Node 1 has the element
        let mut node1 = ObservedRemoveSet::new("node1");
        node1.add("item".to_string());

        // Node 2 gets a copy
        let mut node2 = node1.clone();
        node2.set_local_node("node2");

        // Concurrent: node1 removes, node2 re-adds
        node1.remove(&"item".to_string());
        node2.add("item".to_string());

        // Merge: node2's fresh tag survives node1's remove
        node1.merge(&node2);
        assert!(
            node1.contains(&"item".to_string()),
            "add should win over concurrent remove"
        );
    }

    #[test]
    fn merge_union_of_elements() {
        let mut a = ObservedRemoveSet::new("node1");
        a.add(1);
        a.add(2);

        let mut b = ObservedRemoveSet::new("node2");
        b.add(2);
        b.add(3);

        a.merge(&b);
        assert!(a.contains(&1));
        assert!(a.contains(&2));
        assert!(a.contains(&3));
        assert_eq!(a.len(), 3);
    }

    #[test]
    fn merge_is_commutative() {
        let mut a = ObservedRemoveSet::new("node1");
        a.add(1);
        let mut b = ObservedRemoveSet::new("node2");
        b.add(2);

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.len(), ba.len());
        assert!(ab.contains(&1) && ab.contains(&2));
        assert!(ba.contains(&1) && ba.contains(&2));
    }

    #[test]
    fn merge_is_idempotent() {
        let mut a = ObservedRemoveSet::new("node1");
        a.add(1);
        a.add(2);

        let snapshot = a.clone();
        a.merge(&snapshot);
        assert_eq!(a.len(), 2);
    }

    #[test]
    fn unique_tags_per_add() {
        let mut set = ObservedRemoveSet::new("node1");
        set.add("x".to_string());
        set.add("x".to_string()); // second add of same element
                                  // Both adds generate unique tags; element is present with 2 tags
        let tags = set.entries.get("x").unwrap();
        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn elements_iterator() {
        let mut set = ObservedRemoveSet::new("node1");
        set.add(1);
        set.add(2);
        set.add(3);
        set.remove(&2);
        let mut elems: Vec<&i32> = set.elements();
        elems.sort();
        assert_eq!(elems, vec![&1, &3]);
    }
}
