//! Rack-aware replica assignment for partition placement
//!
//! This module implements rack-aware replica placement that spreads replicas
//! across different failure domains (racks) to maximize fault tolerance.
//!
//! ## Algorithm
//!
//! The rack-aware assigner attempts to place replicas in different racks:
//! 1. Sort brokers by rack, then by node_id for determinism
//! 2. For each partition, select replicas from different racks when possible
//! 3. Fall back to same-rack placement when not enough racks are available
//!
//! ## Example
//!
//! With brokers in 3 racks (rack-a: \[1,2\], rack-b: \[3,4\], rack-c: \[5,6\]):
//! - Partition 0 with RF=3: replicas could be [1, 3, 5] (one from each rack)
//! - This ensures partition survives any single rack failure

use crate::cluster::node::{BrokerInfo, NodeId};
use std::collections::HashMap;
use tracing::debug;

/// Rack-aware replica assigner
///
/// Assigns partition replicas across different racks to maximize fault tolerance.
#[derive(Debug, Default)]
pub struct RackAwareAssigner {
    /// Whether rack-aware assignment is enabled
    enabled: bool,
}

impl RackAwareAssigner {
    /// Create a new rack-aware assigner
    pub fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    /// Assign replicas for a partition
    ///
    /// Returns a list of broker IDs that should hold replicas for this partition.
    /// The first broker in the list is the preferred leader.
    ///
    /// # Arguments
    /// * `partition_id` - The partition ID (used for load balancing across partitions)
    /// * `replication_factor` - Number of replicas to create
    /// * `brokers` - Available brokers with their rack information
    pub fn assign_replicas(
        &self,
        partition_id: i32,
        replication_factor: i16,
        brokers: &[BrokerInfo],
    ) -> Vec<NodeId> {
        if brokers.is_empty() {
            return Vec::new();
        }

        let rf = replication_factor as usize;
        if rf > brokers.len() {
            // Not enough brokers for the requested replication factor
            // Return all brokers (caller should handle this case)
            return brokers.iter().map(|b| b.node_id).collect();
        }

        if !self.enabled {
            // Fall back to round-robin assignment
            return self.round_robin_assign(partition_id, rf, brokers);
        }

        // Group brokers by rack
        let brokers_by_rack = self.group_by_rack(brokers);
        let rack_count = brokers_by_rack.len();

        debug!(
            partition_id,
            replication_factor,
            rack_count,
            broker_count = brokers.len(),
            "Assigning replicas with rack awareness"
        );

        // If all brokers are in the same rack (or no rack info), use round-robin
        if rack_count <= 1 {
            return self.round_robin_assign(partition_id, rf, brokers);
        }

        // Perform rack-aware assignment
        self.rack_aware_assign(partition_id, rf, brokers_by_rack)
    }

    /// Group brokers by their rack
    fn group_by_rack(&self, brokers: &[BrokerInfo]) -> HashMap<String, Vec<NodeId>> {
        let mut by_rack: HashMap<String, Vec<NodeId>> = HashMap::new();

        for broker in brokers {
            let rack = broker.rack.clone().unwrap_or_else(|| "default".to_string());
            by_rack.entry(rack).or_default().push(broker.node_id);
        }

        // Sort broker IDs within each rack for determinism
        for brokers in by_rack.values_mut() {
            brokers.sort();
        }

        by_rack
    }

    /// Perform rack-aware replica assignment
    fn rack_aware_assign(
        &self,
        partition_id: i32,
        replication_factor: usize,
        brokers_by_rack: HashMap<String, Vec<NodeId>>,
    ) -> Vec<NodeId> {
        // Sort racks for deterministic ordering
        let mut racks: Vec<_> = brokers_by_rack.keys().cloned().collect();
        racks.sort();

        let rack_count = racks.len();
        let mut result = Vec::with_capacity(replication_factor);
        let mut rack_indices: HashMap<String, usize> = HashMap::new();

        // Initialize rack indices based on partition_id for load balancing
        for (i, rack) in racks.iter().enumerate() {
            let brokers = &brokers_by_rack[rack];
            let start_idx = (partition_id as usize + i) % brokers.len();
            rack_indices.insert(rack.clone(), start_idx);
        }

        // Start with a different rack for each partition to balance leaders
        let starting_rack_idx = partition_id as usize % rack_count;

        // Assign replicas in a round-robin fashion across racks
        let mut replica_num = 0;
        while result.len() < replication_factor {
            // Select rack (rotate through racks)
            let rack_idx = (starting_rack_idx + replica_num) % rack_count;
            let rack = &racks[rack_idx];
            let brokers = &brokers_by_rack[rack];

            // Select broker within rack (safe: rack_indices populated from racks above)
            let Some(broker_idx) = rack_indices.get_mut(rack) else {
                replica_num += 1;
                continue;
            };
            let broker_id = brokers[*broker_idx % brokers.len()];

            // Only add if not already in the result
            if !result.contains(&broker_id) {
                result.push(broker_id);
            }

            // Move to next broker in this rack for next time
            *broker_idx += 1;
            replica_num += 1;

            // Safety valve: if we've gone through all combinations, break
            if replica_num > replication_factor * rack_count * 2 {
                break;
            }
        }

        // If we couldn't get enough unique replicas (edge case),
        // fill remaining with any available brokers
        if result.len() < replication_factor {
            for rack in &racks {
                for broker_id in &brokers_by_rack[rack] {
                    if !result.contains(broker_id) {
                        result.push(*broker_id);
                        if result.len() >= replication_factor {
                            break;
                        }
                    }
                }
                if result.len() >= replication_factor {
                    break;
                }
            }
        }

        debug!(partition_id, ?result, "Rack-aware assignment complete");

        result
    }

    /// Simple round-robin assignment (fallback when rack-awareness is disabled or not applicable)
    fn round_robin_assign(
        &self,
        partition_id: i32,
        replication_factor: usize,
        brokers: &[BrokerInfo],
    ) -> Vec<NodeId> {
        // Sort broker IDs for determinism
        let mut broker_ids: Vec<_> = brokers.iter().map(|b| b.node_id).collect();
        broker_ids.sort();

        let broker_count = broker_ids.len();
        let mut result = Vec::with_capacity(replication_factor);

        for i in 0..replication_factor {
            let idx = (partition_id as usize + i) % broker_count;
            result.push(broker_ids[idx]);
        }

        result
    }

    /// Get rack distribution for a set of replicas
    ///
    /// Returns a map of rack -> count of replicas in that rack.
    /// Useful for analyzing or validating assignments.
    pub fn get_rack_distribution(
        replicas: &[NodeId],
        brokers: &[BrokerInfo],
    ) -> HashMap<String, usize> {
        let broker_map: HashMap<NodeId, &BrokerInfo> =
            brokers.iter().map(|b| (b.node_id, b)).collect();

        let mut distribution: HashMap<String, usize> = HashMap::new();

        for replica_id in replicas {
            if let Some(broker) = broker_map.get(replica_id) {
                let rack = broker.rack.clone().unwrap_or_else(|| "default".to_string());
                *distribution.entry(rack).or_default() += 1;
            }
        }

        distribution
    }

    /// Calculate the rack diversity score for a replica assignment
    ///
    /// Returns a value between 0.0 and 1.0 where:
    /// - 1.0 means all replicas are in different racks
    /// - 0.0 means all replicas are in the same rack
    pub fn rack_diversity_score(replicas: &[NodeId], brokers: &[BrokerInfo]) -> f64 {
        if replicas.len() <= 1 {
            return 1.0;
        }

        let distribution = Self::get_rack_distribution(replicas, brokers);
        let unique_racks = distribution.len() as f64;
        let total_replicas = replicas.len() as f64;

        // Score is the ratio of unique racks to total replicas
        // Capped at 1.0 (perfect distribution)
        (unique_racks / total_replicas).min(1.0)
    }
}

/// Configuration for rack-aware assignment
#[derive(Debug, Clone, Default)]
pub struct RackAwareConfig {
    /// Enable rack-aware replica placement
    pub enabled: bool,

    /// Prefer rack diversity even if it means unbalanced broker load
    pub prefer_rack_diversity: bool,
}

impl RackAwareConfig {
    /// Create a new config with rack-awareness enabled
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            prefer_rack_diversity: true,
        }
    }

    /// Create a new config with rack-awareness disabled
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            prefer_rack_diversity: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::node::NodeState;

    fn make_broker(node_id: NodeId, rack: Option<&str>) -> BrokerInfo {
        BrokerInfo {
            node_id,
            advertised_addr: format!("127.0.0.{}:9092", node_id).parse().unwrap(),
            inter_broker_addr: format!("127.0.0.{}:9093", node_id).parse().unwrap(),
            rack: rack.map(|s| s.to_string()),
            datacenter: None,
            state: NodeState::Running,
        }
    }

    #[test]
    fn test_round_robin_without_racks() {
        let assigner = RackAwareAssigner::new(false);
        let brokers = vec![
            make_broker(1, None),
            make_broker(2, None),
            make_broker(3, None),
        ];

        // Partition 0, RF=3
        let replicas = assigner.assign_replicas(0, 3, &brokers);
        assert_eq!(replicas.len(), 3);
        assert_eq!(replicas, vec![1, 2, 3]);

        // Partition 1, RF=3 - should rotate
        let replicas = assigner.assign_replicas(1, 3, &brokers);
        assert_eq!(replicas, vec![2, 3, 1]);
    }

    #[test]
    fn test_rack_aware_assignment_three_racks() {
        let assigner = RackAwareAssigner::new(true);
        let brokers = vec![
            make_broker(1, Some("rack-a")),
            make_broker(2, Some("rack-a")),
            make_broker(3, Some("rack-b")),
            make_broker(4, Some("rack-b")),
            make_broker(5, Some("rack-c")),
            make_broker(6, Some("rack-c")),
        ];

        // Partition 0, RF=3 - should get one from each rack
        let replicas = assigner.assign_replicas(0, 3, &brokers);
        assert_eq!(replicas.len(), 3);

        // Verify we have replicas in different racks
        let distribution = RackAwareAssigner::get_rack_distribution(&replicas, &brokers);
        assert_eq!(distribution.len(), 3); // 3 different racks
    }

    #[test]
    fn test_rack_aware_assignment_two_racks() {
        let assigner = RackAwareAssigner::new(true);
        let brokers = vec![
            make_broker(1, Some("rack-a")),
            make_broker(2, Some("rack-a")),
            make_broker(3, Some("rack-b")),
            make_broker(4, Some("rack-b")),
        ];

        // Partition 0, RF=3 - should get at least 2 racks
        let replicas = assigner.assign_replicas(0, 3, &brokers);
        assert_eq!(replicas.len(), 3);

        let distribution = RackAwareAssigner::get_rack_distribution(&replicas, &brokers);
        assert_eq!(distribution.len(), 2); // 2 different racks
    }

    #[test]
    fn test_rack_aware_assignment_single_rack() {
        let assigner = RackAwareAssigner::new(true);
        let brokers = vec![
            make_broker(1, Some("rack-a")),
            make_broker(2, Some("rack-a")),
            make_broker(3, Some("rack-a")),
        ];

        // All in same rack - should still work
        let replicas = assigner.assign_replicas(0, 3, &brokers);
        assert_eq!(replicas.len(), 3);
        assert_eq!(replicas, vec![1, 2, 3]);
    }

    #[test]
    fn test_rack_aware_assignment_more_rf_than_racks() {
        let assigner = RackAwareAssigner::new(true);
        let brokers = vec![
            make_broker(1, Some("rack-a")),
            make_broker(2, Some("rack-a")),
            make_broker(3, Some("rack-b")),
            make_broker(4, Some("rack-b")),
        ];

        // RF=4 with 2 racks - should still assign all 4
        let replicas = assigner.assign_replicas(0, 4, &brokers);
        assert_eq!(replicas.len(), 4);

        // All unique brokers
        let mut sorted = replicas.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 4);
    }

    #[test]
    fn test_rack_aware_load_balancing() {
        let assigner = RackAwareAssigner::new(true);
        let brokers = vec![
            make_broker(1, Some("rack-a")),
            make_broker(2, Some("rack-b")),
            make_broker(3, Some("rack-c")),
        ];

        // Create multiple partitions and check that leaders are distributed
        let mut leader_counts: HashMap<NodeId, usize> = HashMap::new();
        for partition_id in 0..6 {
            let replicas = assigner.assign_replicas(partition_id, 3, &brokers);
            *leader_counts.entry(replicas[0]).or_default() += 1;
        }

        // Each broker should be leader approximately equal times
        for count in leader_counts.values() {
            assert!(*count >= 1); // At least one leadership each
        }
    }

    #[test]
    fn test_empty_brokers() {
        let assigner = RackAwareAssigner::new(true);
        let replicas = assigner.assign_replicas(0, 3, &[]);
        assert!(replicas.is_empty());
    }

    #[test]
    fn test_insufficient_brokers() {
        let assigner = RackAwareAssigner::new(true);
        let brokers = vec![
            make_broker(1, Some("rack-a")),
            make_broker(2, Some("rack-b")),
        ];

        // RF=3 but only 2 brokers
        let replicas = assigner.assign_replicas(0, 3, &brokers);
        assert_eq!(replicas.len(), 2); // Returns all available
    }

    #[test]
    fn test_rack_diversity_score() {
        let brokers = vec![
            make_broker(1, Some("rack-a")),
            make_broker(2, Some("rack-b")),
            make_broker(3, Some("rack-c")),
            make_broker(4, Some("rack-a")),
        ];

        // All different racks
        let score = RackAwareAssigner::rack_diversity_score(&[1, 2, 3], &brokers);
        assert!((score - 1.0).abs() < 0.01);

        // Two in same rack
        let score = RackAwareAssigner::rack_diversity_score(&[1, 4, 2], &brokers);
        assert!(score < 1.0);
        assert!(score > 0.5);

        // All in same rack
        let score = RackAwareAssigner::rack_diversity_score(&[1, 4], &brokers);
        assert!((score - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_get_rack_distribution() {
        let brokers = vec![
            make_broker(1, Some("rack-a")),
            make_broker(2, Some("rack-a")),
            make_broker(3, Some("rack-b")),
        ];

        let dist = RackAwareAssigner::get_rack_distribution(&[1, 2, 3], &brokers);
        assert_eq!(dist.get("rack-a"), Some(&2));
        assert_eq!(dist.get("rack-b"), Some(&1));
    }

    #[test]
    fn test_deterministic_assignment() {
        let assigner = RackAwareAssigner::new(true);
        let brokers = vec![
            make_broker(1, Some("rack-a")),
            make_broker(2, Some("rack-b")),
            make_broker(3, Some("rack-c")),
        ];

        // Same input should always produce same output
        let replicas1 = assigner.assign_replicas(5, 3, &brokers);
        let replicas2 = assigner.assign_replicas(5, 3, &brokers);
        assert_eq!(replicas1, replicas2);
    }

    #[test]
    fn test_mixed_rack_and_no_rack() {
        let assigner = RackAwareAssigner::new(true);
        let brokers = vec![
            make_broker(1, Some("rack-a")),
            make_broker(2, None), // No rack info
            make_broker(3, Some("rack-b")),
        ];

        // Should still work - broker 2 goes to "default" rack
        let replicas = assigner.assign_replicas(0, 3, &brokers);
        assert_eq!(replicas.len(), 3);

        let distribution = RackAwareAssigner::get_rack_distribution(&replicas, &brokers);
        assert!(distribution.contains_key("default"));
    }
}
