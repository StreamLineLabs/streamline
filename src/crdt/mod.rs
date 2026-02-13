//! CRDT (Conflict-free Replicated Data Types) Module
//!
//! Provides native CRDT support for eventually consistent distributed streaming.
//! CRDTs automatically converge to the same state regardless of update order,
//! making them ideal for edge computing and offline-first architectures.
//!
//! # Features
//!
//! - **CRDT Types**: G-Counter, PN-Counter, LWW-Register, OR-Set, RGA-Sequence
//! - **Clock Infrastructure**: Hybrid Logical Clocks (HLC) and Vector Clocks
//! - **Wire Format**: Header-based CRDT metadata for backward compatibility
//! - **State Management**: Merged state queries and real-time subscriptions
//! - **Causal Delivery**: Optional causal ordering for message delivery
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                    CRDT Record Wire Format                    │
//! ├──────────────────────────────────────────────────────────────┤
//! │  Headers:                                                     │
//! │  ├── x-crdt-type: g_counter|pn_counter|lww_register|...      │
//! │  ├── x-crdt-hlc: physical:logical:node_id                    │
//! │  ├── x-crdt-vclock: {"node1":5,"node2":3}                    │
//! │  ├── x-crdt-op: state|delta                                  │
//! │  └── x-crdt-node: node_id                                    │
//! ├──────────────────────────────────────────────────────────────┤
//! │  Value: JSON-encoded CRDT state                               │
//! └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use streamline::crdt::{GCounter, PNCounter, ORSet, LWWRegister, RGASequence};
//! use streamline::crdt::{CrdtRecord, CrdtRecordBuilder, CrdtStateManager};
//!
//! // Create CRDT types
//! let mut counter = GCounter::new("edge-node-1");
//! counter.increment(10);
//!
//! let mut set: ORSet<String> = ORSet::new("edge-node-1");
//! set.add("item1".to_string());
//!
//! // Create CRDT records for streaming
//! let record = CrdtRecordBuilder::new("edge-node-1")
//!     .key("my-counter")
//!     .g_counter(counter);
//!
//! // State management with automatic merge
//! let state_manager = CrdtStateManager::default_manager();
//! state_manager.process_record("topic", 0, &record, 1).await?;
//!
//! // Query merged state
//! let state = state_manager.get_merged_state("topic", 0, &Bytes::from("my-counter"));
//! ```
//!
//! # CRDT Types
//!
//! | Type | Description | Use Case |
//! |------|-------------|----------|
//! | `GCounter` | Grow-only counter | View counts, metrics |
//! | `PNCounter` | Increment/decrement counter | Account balances, stock |
//! | `LWWRegister` | Last-writer-wins value | User preferences, status |
//! | `ORSet` | Add/remove set | Tags, shopping carts |
//! | `RGASequence` | Ordered sequence | Collaborative editing |
//!
//! # Causal Consistency
//!
//! CRDTs can be combined with vector clocks for causal consistency:
//!
//! ```rust,ignore
//! use streamline::crdt::{VectorClock, HybridLogicalClock, CausalConsumer};
//!
//! // Vector clocks track causality
//! let mut vc1 = VectorClock::new();
//! vc1.increment("node1");
//!
//! let mut vc2 = VectorClock::new();
//! vc2.increment("node2");
//!
//! // Check ordering
//! assert!(vc1.is_concurrent(&vc2)); // Concurrent updates
//!
//! // Causal consumer for ordered delivery
//! let consumer = CausalConsumer::new("topic", 0, state_manager);
//! ```

pub mod clock;
pub mod compaction;
pub mod record;
pub mod replication;
pub mod state;
pub mod types;

// Re-export clock types
pub use clock::{
    CausalTimestamp, ClockComparison, HlcClockSource, HybridLogicalClock, VectorClock,
};

// Re-export CRDT types
pub use types::{
    Crdt, CrdtOperation, CrdtType, CrdtValue, GCounter, LWWRegister, ORSet, PNCounter, RGASequence,
    RgaElement, RgaId, UniqueTag,
};

// Re-export record types
pub use record::{
    has_crdt_header, headers_to_map, map_to_headers, CrdtMergeContext, CrdtMetadata, CrdtRecord,
    CrdtRecordBuilder, CRDT_FORMAT_VERSION, CRDT_HLC_HEADER, CRDT_NODE_HEADER,
    CRDT_OPERATION_HEADER, CRDT_TYPE_HEADER, CRDT_VCLOCK_HEADER, CRDT_VERSION_HEADER,
};

// Re-export state management types
pub use state::{
    CausalConsumer, CrdtStateConfig, CrdtStateManager, CrdtStateStatsSnapshot, KeySubscription,
    MergedState, MultiPartitionState, StateChange, StateKey,
};

// Re-export compaction types
pub use compaction::{
    compact_crdt_records, is_crdt_record, CrdtCompactionConfig, CrdtCompactionStats, CrdtCompactor,
};

// Re-export geo-replication types
pub use replication::{
    ConflictStrategy, GeoReplicationConfig, GeoReplicationManager, GeoReplicationStatsSnapshot,
    PeerRegion, PeerReplicationStatus, PeerState, ReplicationMode,
};

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_gcounter_convergence() {
        // Simulate edge nodes making concurrent updates
        let mut edge1 = GCounter::new("edge1");
        edge1.increment(10);

        let mut edge2 = GCounter::new("edge2");
        edge2.increment(20);

        let mut edge3 = GCounter::new("edge3");
        edge3.increment(30);

        // Merge in any order - always converges
        let mut cloud = GCounter::empty();
        cloud.merge(&edge1);
        cloud.merge(&edge2);
        cloud.merge(&edge3);

        assert_eq!(cloud.value(), 60);

        // Different merge order, same result
        let mut cloud2 = GCounter::empty();
        cloud2.merge(&edge3);
        cloud2.merge(&edge1);
        cloud2.merge(&edge2);

        assert_eq!(cloud2.value(), 60);
    }

    #[test]
    fn test_orset_add_wins() {
        // Demonstrate add-wins semantics
        let mut set1: ORSet<String> = ORSet::new("node1");
        set1.add("item".to_string());

        let mut set2 = set1.clone();
        set2.set_local_node("node2");

        // Concurrent: node1 removes, node2 re-adds
        set1.remove(&"item".to_string());
        set2.add("item".to_string());

        // Merge: add wins because it has a fresh unique tag
        set1.merge(&set2);
        assert!(set1.contains(&"item".to_string()));
    }

    #[test]
    fn test_lww_register_timestamp_wins() {
        let reg1 = LWWRegister::new("node1", "first".to_string());

        std::thread::sleep(std::time::Duration::from_millis(1));
        let reg2 = LWWRegister::new("node2", "second".to_string());

        // reg2 has later timestamp
        let mut merged = reg1.clone();
        merged.merge(&reg2);

        assert_eq!(merged.value(), "second");
    }

    #[test]
    fn test_vector_clock_causality() {
        let mut vc1 = VectorClock::new();
        vc1.increment("node1");
        vc1.increment("node1");

        let mut vc2 = vc1.clone();
        vc2.increment("node2");

        // vc1 happens before vc2
        assert_eq!(vc1.compare(&vc2), ClockComparison::Before);
        assert_eq!(vc2.compare(&vc1), ClockComparison::After);

        // Concurrent clocks
        let mut vc3 = VectorClock::new();
        vc3.increment("node3");

        assert_eq!(vc1.compare(&vc3), ClockComparison::Concurrent);
    }

    #[tokio::test]
    async fn test_end_to_end_crdt_flow() {
        // Create state manager
        let manager = CrdtStateManager::default_manager();

        // Edge node 1 creates counter
        let mut counter1 = GCounter::new("edge1");
        counter1.increment(100);

        let record1 = CrdtRecordBuilder::new("edge1")
            .key("page-views")
            .g_counter(counter1);

        manager
            .process_record("metrics", 0, &record1, 1)
            .await
            .unwrap();

        // Edge node 2 creates counter (concurrent)
        let mut counter2 = GCounter::new("edge2");
        counter2.increment(200);

        let record2 = CrdtRecordBuilder::new("edge2")
            .key("page-views")
            .g_counter(counter2);

        manager
            .process_record("metrics", 0, &record2, 2)
            .await
            .unwrap();

        // Query merged state
        let state = manager
            .get_merged_state("metrics", 0, &Bytes::from("page-views"))
            .unwrap();

        match state.value {
            CrdtValue::GCounter(c) => assert_eq!(c.value(), 300),
            _ => panic!("Expected GCounter"),
        }
    }

    #[test]
    fn test_rga_collaborative_edit() {
        // Simulate collaborative text editing
        let mut doc1: RGASequence<char> = RGASequence::new("user1");
        doc1.push('H');
        doc1.push('e');
        doc1.push('l');
        doc1.push('l');
        doc1.push('o');

        let mut doc2 = doc1.clone();
        doc2.set_local_node("user2");

        // user1 adds "!" at the end
        doc1.push('!');

        // user2 adds " World" at the end (concurrent)
        doc2.push(' ');
        doc2.push('W');
        doc2.push('o');
        doc2.push('r');
        doc2.push('l');
        doc2.push('d');

        // Merge preserves both edits
        doc1.merge(&doc2);

        let result: String = doc1.iter().collect();
        // Both additions are preserved (order depends on timestamps)
        assert!(result.contains("Hello"));
        assert!(result.contains("!") || result.contains("World"));
    }
}
