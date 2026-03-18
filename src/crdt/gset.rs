//! Grow-only Set (GSet) CRDT.
//!
//! A GSet supports only `add` — elements can never be removed. The merge
//! operation is set union, which is commutative, associative, and idempotent.
//!
//! # Properties
//!
//! - **Monotonic**: The set only ever grows.
//! - **Convergent**: Merge is set union — replicas always converge.
//! - **Simple**: No tombstones, no metadata per element.
//!
//! # Use Cases
//!
//! - Tracking "seen" events or IDs across distributed nodes.
//! - Collecting unique tags or labels.
//! - Membership lists where removal is not needed.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::hash::Hash;

/// A grow-only set CRDT.
///
/// Elements can only be added; there is no remove operation. Merge is
/// the set union of both replicas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GSet<T: Eq + Hash + Clone> {
    elements: HashSet<T>,
}

impl<T: Eq + Hash + Clone> GSet<T> {
    /// Creates an empty GSet.
    pub fn new() -> Self {
        Self {
            elements: HashSet::new(),
        }
    }

    /// Adds an element to the set. Returns true if the element was newly inserted.
    pub fn add(&mut self, value: T) -> bool {
        self.elements.insert(value)
    }

    /// Returns true if the set contains the given element.
    pub fn contains(&self, value: &T) -> bool {
        self.elements.contains(value)
    }

    /// Returns the number of elements in the set.
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Returns true if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Returns an iterator over the elements.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.elements.iter()
    }

    /// Returns the elements as a reference to the inner HashSet.
    pub fn elements(&self) -> &HashSet<T> {
        &self.elements
    }

    /// Merges another GSet into this one (set union).
    ///
    /// After merge, this set contains all elements from both sets.
    pub fn merge(&mut self, other: &GSet<T>) {
        for item in &other.elements {
            self.elements.insert(item.clone());
        }
    }
}

impl<T: Eq + Hash + Clone> Default for GSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Eq + Hash + Clone> FromIterator<T> for GSet<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut set = GSet::new();
        for item in iter {
            set.add(item);
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_set() {
        let set: GSet<String> = GSet::new();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn add_and_contains() {
        let mut set = GSet::new();
        assert!(set.add("hello".to_string()));
        assert!(!set.add("hello".to_string())); // duplicate
        assert!(set.contains(&"hello".to_string()));
        assert!(!set.contains(&"world".to_string()));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn merge_is_union() {
        let mut a = GSet::new();
        a.add(1);
        a.add(2);

        let mut b = GSet::new();
        b.add(2);
        b.add(3);

        a.merge(&b);
        assert_eq!(a.len(), 3);
        assert!(a.contains(&1));
        assert!(a.contains(&2));
        assert!(a.contains(&3));
    }

    #[test]
    fn merge_is_commutative() {
        let mut a = GSet::new();
        a.add(1);
        let mut b = GSet::new();
        b.add(2);

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.elements(), ba.elements());
    }

    #[test]
    fn merge_is_idempotent() {
        let mut a = GSet::new();
        a.add(1);
        a.add(2);

        let before = a.clone();
        a.merge(&before);
        assert_eq!(a.elements(), before.elements());
    }

    #[test]
    fn merge_is_associative() {
        let mut a = GSet::new();
        a.add(1);
        let mut b = GSet::new();
        b.add(2);
        let mut c = GSet::new();
        c.add(3);

        // (a ∪ b) ∪ c
        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        // a ∪ (b ∪ c)
        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        assert_eq!(ab_c.elements(), a_bc.elements());
    }

    #[test]
    fn from_iterator() {
        let set: GSet<i32> = vec![1, 2, 3, 2, 1].into_iter().collect();
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn iterate_elements() {
        let mut set = GSet::new();
        set.add(1);
        set.add(2);
        let collected: HashSet<&i32> = set.iter().collect();
        assert_eq!(collected.len(), 2);
    }
}
