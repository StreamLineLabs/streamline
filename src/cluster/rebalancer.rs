//! Partition rebalancer for production clustering
//!
//! Automatically redistributes partition replicas when brokers join, leave,
//! or fail. Supports multiple rebalancing strategies to minimize data movement
//! while maintaining even load distribution across the cluster.

use crate::cluster::node::{BrokerInfo, NodeId};
use crate::cluster::rack::RackAwareAssigner;
use crate::cluster::raft::state_machine::ClusterMetadata;
use crate::cluster::raft::types::{PartitionAssignment, TopicAssignment};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Configuration for the partition rebalancer
#[derive(Debug, Clone)]
pub struct RebalancerConfig {
    /// Maximum number of partition movements per rebalance operation
    pub max_movements_per_rebalance: usize,

    /// Maximum number of concurrent partition movements
    pub max_concurrent_movements: usize,

    /// Imbalance threshold (0.0-1.0) before triggering rebalance
    /// e.g., 0.1 means rebalance when any broker has 10% more/fewer partitions than average
    pub imbalance_threshold: f64,

    /// Whether to use rack-aware assignment during rebalancing
    pub rack_aware: bool,

    /// Minimum time between rebalance operations (prevents thrashing)
    pub cooldown_ms: u64,
}

impl Default for RebalancerConfig {
    fn default() -> Self {
        Self {
            max_movements_per_rebalance: 50,
            max_concurrent_movements: 5,
            imbalance_threshold: 0.1,
            rack_aware: true,
            cooldown_ms: 60_000,
        }
    }
}

/// Strategy for rebalancing partitions
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RebalanceStrategy {
    /// Minimize data movement while achieving balance
    MinimalMovement,
    /// Achieve perfect balance regardless of movement cost
    FullRebalance,
    /// Only fix under-replicated partitions (no load balancing)
    RepairOnly,
}

/// A planned partition movement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMovement {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition_id: i32,
    /// Current replica set
    pub old_replicas: Vec<NodeId>,
    /// New replica set
    pub new_replicas: Vec<NodeId>,
    /// New leader (first replica in new set)
    pub new_leader: NodeId,
    /// Reason for the movement
    pub reason: MovementReason,
}

/// Reason a partition needs to be moved
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MovementReason {
    /// Replica is on a dead broker
    DeadBrokerEviction { dead_node: NodeId },
    /// Load imbalance across brokers
    LoadBalancing {
        overloaded_node: NodeId,
        underloaded_node: NodeId,
    },
    /// Under-replicated partition needs additional replicas
    UnderReplicated {
        current_replicas: usize,
        desired_replicas: usize,
    },
    /// New broker joined and needs partitions
    NewBrokerAssignment { new_node: NodeId },
}

/// Result of a rebalance plan
#[derive(Debug, Clone)]
pub struct RebalancePlan {
    /// Planned movements
    pub movements: Vec<PartitionMovement>,
    /// Number of partitions that are under-replicated
    pub under_replicated_count: usize,
    /// Imbalance score before rebalance (0.0 = perfectly balanced)
    pub imbalance_before: f64,
    /// Projected imbalance after rebalance
    pub imbalance_after: f64,
}

/// Partition rebalancer that computes optimal replica assignments
pub struct PartitionRebalancer {
    config: RebalancerConfig,
}

impl PartitionRebalancer {
    pub fn new(config: RebalancerConfig) -> Self {
        Self { config }
    }

    /// Compute a rebalance plan based on current cluster state
    pub fn plan_rebalance(
        &self,
        metadata: &ClusterMetadata,
        strategy: RebalanceStrategy,
    ) -> RebalancePlan {
        let alive_brokers: Vec<&BrokerInfo> = metadata.alive_brokers();
        let alive_ids: Vec<NodeId> = alive_brokers.iter().map(|b| b.node_id).collect();

        if alive_ids.is_empty() {
            return RebalancePlan {
                movements: vec![],
                under_replicated_count: 0,
                imbalance_before: 0.0,
                imbalance_after: 0.0,
            };
        }

        let mut movements = Vec::new();

        // Phase 1: Fix partitions with dead broker replicas
        let dead_broker_movements = self.plan_dead_broker_evictions(metadata, &alive_ids);
        let under_replicated_count = dead_broker_movements.len();
        movements.extend(dead_broker_movements);

        // Phase 2: Fix under-replicated partitions
        let under_rep_movements =
            self.plan_under_replicated_repairs(metadata, &alive_ids, &movements);
        movements.extend(under_rep_movements);

        if strategy == RebalanceStrategy::RepairOnly {
            let imbalance = self.compute_imbalance(metadata, &alive_ids);
            return RebalancePlan {
                movements,
                under_replicated_count,
                imbalance_before: imbalance,
                imbalance_after: imbalance,
            };
        }

        // Phase 3: Load balancing
        let imbalance_before = self.compute_imbalance(metadata, &alive_ids);

        if imbalance_before > self.config.imbalance_threshold
            || strategy == RebalanceStrategy::FullRebalance
        {
            let balance_movements = self.plan_load_balancing(metadata, &alive_ids, &movements);
            movements.extend(balance_movements);
        }

        // Limit total movements
        movements.truncate(self.config.max_movements_per_rebalance);

        let imbalance_after = self.simulate_imbalance(metadata, &alive_ids, &movements);

        info!(
            movements = movements.len(),
            under_replicated_count,
            imbalance_before,
            imbalance_after,
            "Rebalance plan computed"
        );

        RebalancePlan {
            movements,
            under_replicated_count,
            imbalance_before,
            imbalance_after,
        }
    }

    /// Plan movements to evict replicas from dead brokers
    fn plan_dead_broker_evictions(
        &self,
        metadata: &ClusterMetadata,
        alive_ids: &[NodeId],
    ) -> Vec<PartitionMovement> {
        let mut movements = Vec::new();

        for (topic_name, topic) in &metadata.topics {
            for (partition_id, partition) in &topic.partitions {
                let dead_replicas: Vec<NodeId> = partition
                    .replicas
                    .iter()
                    .filter(|r| !alive_ids.contains(r))
                    .copied()
                    .collect();

                if dead_replicas.is_empty() {
                    continue;
                }

                // Build new replica set: keep alive replicas, replace dead ones
                let mut new_replicas: Vec<NodeId> = partition
                    .replicas
                    .iter()
                    .filter(|r| alive_ids.contains(r))
                    .copied()
                    .collect();

                // Find replacement brokers (least loaded first)
                let load_map = self.compute_load_map(metadata, alive_ids);
                let mut candidates: Vec<(NodeId, usize)> = load_map
                    .iter()
                    .filter(|(id, _)| !new_replicas.contains(id))
                    .map(|(&id, &load)| (id, load))
                    .collect();
                candidates.sort_by_key(|(_, load)| *load);

                for (candidate, _) in candidates {
                    if new_replicas.len() >= partition.replicas.len() {
                        break;
                    }
                    new_replicas.push(candidate);
                }

                if new_replicas != partition.replicas && !new_replicas.is_empty() {
                    let new_leader = if new_replicas.contains(
                        &partition.leader.unwrap_or(0),
                    ) {
                        partition.leader.unwrap_or(new_replicas[0])
                    } else {
                        new_replicas[0]
                    };

                    movements.push(PartitionMovement {
                        topic: topic_name.clone(),
                        partition_id: *partition_id,
                        old_replicas: partition.replicas.clone(),
                        new_replicas,
                        new_leader,
                        reason: MovementReason::DeadBrokerEviction {
                            dead_node: dead_replicas[0],
                        },
                    });
                }
            }
        }

        movements
    }

    /// Plan movements to repair under-replicated partitions
    fn plan_under_replicated_repairs(
        &self,
        metadata: &ClusterMetadata,
        alive_ids: &[NodeId],
        existing_movements: &[PartitionMovement],
    ) -> Vec<PartitionMovement> {
        let mut movements = Vec::new();
        let already_moved: Vec<(String, i32)> = existing_movements
            .iter()
            .map(|m| (m.topic.clone(), m.partition_id))
            .collect();

        for (topic_name, topic) in &metadata.topics {
            for (partition_id, partition) in &topic.partitions {
                if already_moved.contains(&(topic_name.clone(), *partition_id)) {
                    continue;
                }

                let alive_replicas: Vec<NodeId> = partition
                    .replicas
                    .iter()
                    .filter(|r| alive_ids.contains(r))
                    .copied()
                    .collect();

                let desired = topic.replication_factor as usize;
                if alive_replicas.len() >= desired {
                    continue;
                }

                let load_map = self.compute_load_map(metadata, alive_ids);
                let mut new_replicas = alive_replicas.clone();
                let mut candidates: Vec<(NodeId, usize)> = load_map
                    .iter()
                    .filter(|(id, _)| !new_replicas.contains(id))
                    .map(|(&id, &load)| (id, load))
                    .collect();
                candidates.sort_by_key(|(_, load)| *load);

                for (candidate, _) in candidates {
                    if new_replicas.len() >= desired {
                        break;
                    }
                    new_replicas.push(candidate);
                }

                if new_replicas.len() > alive_replicas.len() {
                    let new_leader = new_replicas[0];
                    movements.push(PartitionMovement {
                        topic: topic_name.clone(),
                        partition_id: *partition_id,
                        old_replicas: partition.replicas.clone(),
                        new_replicas,
                        new_leader,
                        reason: MovementReason::UnderReplicated {
                            current_replicas: alive_replicas.len(),
                            desired_replicas: desired,
                        },
                    });
                }
            }
        }

        movements
    }

    /// Plan movements to balance load across brokers
    fn plan_load_balancing(
        &self,
        metadata: &ClusterMetadata,
        alive_ids: &[NodeId],
        existing_movements: &[PartitionMovement],
    ) -> Vec<PartitionMovement> {
        let mut movements = Vec::new();
        let mut load_map = self.compute_load_map(metadata, alive_ids);

        // Apply existing movements to load map
        for m in existing_movements {
            for old in &m.old_replicas {
                if let Some(load) = load_map.get_mut(old) {
                    *load = load.saturating_sub(1);
                }
            }
            for new in &m.new_replicas {
                *load_map.entry(*new).or_insert(0) += 1;
            }
        }

        let total_replicas: usize = load_map.values().sum();
        let avg_load = total_replicas as f64 / alive_ids.len().max(1) as f64;
        let max_budget = self
            .config
            .max_movements_per_rebalance
            .saturating_sub(existing_movements.len());

        for _ in 0..max_budget {
            // Find most overloaded and underloaded brokers
            let (overloaded, over_count) = match load_map
                .iter()
                .max_by_key(|(_, &load)| load)
            {
                Some((&id, &load)) => (id, load),
                None => break,
            };

            let (underloaded, under_count) = match load_map
                .iter()
                .min_by_key(|(_, &load)| load)
            {
                Some((&id, &load)) => (id, load),
                None => break,
            };

            if (over_count as f64 - avg_load) < 1.5 || over_count <= under_count + 1 {
                break; // Close enough to balanced
            }

            // Find a partition to move from overloaded to underloaded
            let mut moved = false;
            for (topic_name, topic) in &metadata.topics {
                if moved {
                    break;
                }
                for (partition_id, partition) in &topic.partitions {
                    if !partition.replicas.contains(&overloaded) {
                        continue;
                    }
                    if partition.replicas.contains(&underloaded) {
                        continue;
                    }

                    let mut new_replicas = partition.replicas.clone();
                    if let Some(pos) = new_replicas.iter().position(|&r| r == overloaded) {
                        new_replicas[pos] = underloaded;
                    }

                    let new_leader = if partition.leader == Some(overloaded) {
                        underloaded
                    } else {
                        partition.leader.unwrap_or(new_replicas[0])
                    };

                    movements.push(PartitionMovement {
                        topic: topic_name.clone(),
                        partition_id: *partition_id,
                        old_replicas: partition.replicas.clone(),
                        new_replicas,
                        new_leader,
                        reason: MovementReason::LoadBalancing {
                            overloaded_node: overloaded,
                            underloaded_node: underloaded,
                        },
                    });

                    *load_map.get_mut(&overloaded).unwrap() -= 1;
                    *load_map.get_mut(&underloaded).unwrap() += 1;
                    moved = true;
                    break;
                }
            }

            if !moved {
                break;
            }
        }

        movements
    }

    /// Compute per-broker partition replica count
    fn compute_load_map(
        &self,
        metadata: &ClusterMetadata,
        alive_ids: &[NodeId],
    ) -> HashMap<NodeId, usize> {
        let mut load: HashMap<NodeId, usize> =
            alive_ids.iter().map(|&id| (id, 0)).collect();

        for topic in metadata.topics.values() {
            for partition in topic.partitions.values() {
                for replica in &partition.replicas {
                    if let Some(count) = load.get_mut(replica) {
                        *count += 1;
                    }
                }
            }
        }

        load
    }

    /// Compute the imbalance score (0.0 = perfectly balanced)
    fn compute_imbalance(
        &self,
        metadata: &ClusterMetadata,
        alive_ids: &[NodeId],
    ) -> f64 {
        let load_map = self.compute_load_map(metadata, alive_ids);
        if load_map.is_empty() {
            return 0.0;
        }

        let total: usize = load_map.values().sum();
        let avg = total as f64 / load_map.len() as f64;

        if avg == 0.0 {
            return 0.0;
        }

        let max_deviation = load_map
            .values()
            .map(|&load| (load as f64 - avg).abs())
            .fold(0.0_f64, f64::max);

        max_deviation / avg
    }

    /// Simulate imbalance after applying movements
    fn simulate_imbalance(
        &self,
        metadata: &ClusterMetadata,
        alive_ids: &[NodeId],
        movements: &[PartitionMovement],
    ) -> f64 {
        let mut load_map = self.compute_load_map(metadata, alive_ids);

        for m in movements {
            for old in &m.old_replicas {
                if let Some(load) = load_map.get_mut(old) {
                    *load = load.saturating_sub(1);
                }
            }
            for new in &m.new_replicas {
                *load_map.entry(*new).or_insert(0) += 1;
            }
        }

        if load_map.is_empty() {
            return 0.0;
        }

        let total: usize = load_map.values().sum();
        let avg = total as f64 / load_map.len() as f64;

        if avg == 0.0 {
            return 0.0;
        }

        load_map
            .values()
            .map(|&load| (load as f64 - avg).abs())
            .fold(0.0_f64, f64::max)
            / avg
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::node::NodeState;
    use std::net::SocketAddr;

    fn make_broker(id: NodeId, state: NodeState) -> BrokerInfo {
        BrokerInfo {
            node_id: id,
            advertised_addr: format!("127.0.0.1:{}", 9092 + id).parse().unwrap(),
            inter_broker_addr: format!("127.0.0.1:{}", 9192 + id).parse().unwrap(),
            rack: None,
            datacenter: None,
            state,
        }
    }

    fn make_metadata_with_topics(
        brokers: Vec<BrokerInfo>,
        topics: Vec<(&str, Vec<Vec<NodeId>>)>,
    ) -> ClusterMetadata {
        let mut metadata = ClusterMetadata::new("test-cluster".to_string());
        for broker in brokers {
            metadata.register_broker(broker);
        }
        for (name, partitions) in topics {
            let mut assignment =
                TopicAssignment::new(name.to_string(), partitions.len() as i32, 3);
            for (i, replicas) in partitions.iter().enumerate() {
                let pa = PartitionAssignment::new(i as i32, replicas.clone())
                    .with_leader(replicas[0]);
                assignment = assignment.with_partition(pa);
            }
            metadata.topics.insert(name.to_string(), assignment);
        }
        metadata
    }

    #[test]
    fn test_rebalancer_no_movements_when_balanced() {
        let rebalancer = PartitionRebalancer::new(RebalancerConfig::default());
        let metadata = make_metadata_with_topics(
            vec![
                make_broker(1, NodeState::Running),
                make_broker(2, NodeState::Running),
                make_broker(3, NodeState::Running),
            ],
            vec![("topic1", vec![vec![1, 2, 3], vec![2, 3, 1], vec![3, 1, 2]])],
        );

        let plan = rebalancer.plan_rebalance(&metadata, RebalanceStrategy::MinimalMovement);
        assert!(plan.movements.is_empty());
        assert_eq!(plan.under_replicated_count, 0);
    }

    #[test]
    fn test_rebalancer_evicts_dead_broker() {
        let rebalancer = PartitionRebalancer::new(RebalancerConfig::default());
        let metadata = make_metadata_with_topics(
            vec![
                make_broker(1, NodeState::Running),
                make_broker(2, NodeState::Dead),
                make_broker(3, NodeState::Running),
                make_broker(4, NodeState::Running),
            ],
            vec![("topic1", vec![vec![1, 2, 3]])],
        );

        let plan = rebalancer.plan_rebalance(&metadata, RebalanceStrategy::RepairOnly);
        assert!(!plan.movements.is_empty());

        let movement = &plan.movements[0];
        assert!(!movement.new_replicas.contains(&2)); // Dead broker evicted
        assert!(movement.new_replicas.contains(&4)); // Replacement added
    }

    #[test]
    fn test_rebalancer_load_balancing() {
        let rebalancer = PartitionRebalancer::new(RebalancerConfig {
            imbalance_threshold: 0.0, // Force rebalance
            ..Default::default()
        });

        // Broker 1 has 6 replicas, brokers 2-3 have 0
        let metadata = make_metadata_with_topics(
            vec![
                make_broker(1, NodeState::Running),
                make_broker(2, NodeState::Running),
                make_broker(3, NodeState::Running),
            ],
            vec![(
                "topic1",
                vec![vec![1], vec![1], vec![1], vec![1], vec![1], vec![1]],
            )],
        );

        let plan = rebalancer.plan_rebalance(&metadata, RebalanceStrategy::FullRebalance);
        assert!(!plan.movements.is_empty());
        assert!(plan.imbalance_after < plan.imbalance_before);
    }

    #[test]
    fn test_rebalancer_empty_cluster() {
        let rebalancer = PartitionRebalancer::new(RebalancerConfig::default());
        let metadata = ClusterMetadata::new("test".to_string());

        let plan = rebalancer.plan_rebalance(&metadata, RebalanceStrategy::MinimalMovement);
        assert!(plan.movements.is_empty());
    }

    #[test]
    fn test_compute_imbalance_perfect() {
        let rebalancer = PartitionRebalancer::new(RebalancerConfig::default());
        let metadata = make_metadata_with_topics(
            vec![
                make_broker(1, NodeState::Running),
                make_broker(2, NodeState::Running),
            ],
            vec![("t1", vec![vec![1], vec![2]])],
        );
        let alive = vec![1, 2];
        let imbalance = rebalancer.compute_imbalance(&metadata, &alive);
        assert!(imbalance < 0.01);
    }
}
