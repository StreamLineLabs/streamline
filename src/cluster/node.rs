//! Node types for cluster membership
//!
//! This module defines the structures representing nodes (brokers) in the cluster.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Instant;

/// Unique identifier for a node in the cluster
pub type NodeId = u64;

/// Information about a broker node in the cluster
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrokerInfo {
    /// Unique node ID
    pub node_id: NodeId,

    /// Address for client connections (Kafka protocol)
    pub advertised_addr: SocketAddr,

    /// Address for inter-broker communication
    pub inter_broker_addr: SocketAddr,

    /// Optional rack identifier for rack-aware replica placement
    pub rack: Option<String>,

    /// Optional datacenter identifier for multi-DC deployment
    pub datacenter: Option<String>,

    /// Node state
    pub state: NodeState,
}

impl BrokerInfo {
    /// Create a new broker info
    pub fn new(
        node_id: NodeId,
        advertised_addr: SocketAddr,
        inter_broker_addr: SocketAddr,
    ) -> Self {
        Self {
            node_id,
            advertised_addr,
            inter_broker_addr,
            rack: None,
            datacenter: None,
            state: NodeState::Starting,
        }
    }

    /// Set the rack identifier
    pub fn with_rack(mut self, rack: String) -> Self {
        self.rack = Some(rack);
        self
    }

    /// Set the datacenter identifier
    pub fn with_datacenter(mut self, datacenter: String) -> Self {
        self.datacenter = Some(datacenter);
        self
    }

    /// Check if the node is alive (Running state)
    pub fn is_alive(&self) -> bool {
        matches!(self.state, NodeState::Running)
    }

    /// Check if the node can accept client requests
    pub fn can_accept_requests(&self) -> bool {
        matches!(self.state, NodeState::Running)
    }
}

/// State of a node in the cluster
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum NodeState {
    /// Node is starting up and joining the cluster
    Starting,

    /// Node is running and healthy
    Running,

    /// Node is shutting down gracefully
    ShuttingDown,

    /// Node is considered dead (missed heartbeats)
    Dead,

    /// Node is in an unknown state
    #[default]
    Unknown,
}

impl std::fmt::Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeState::Starting => write!(f, "Starting"),
            NodeState::Running => write!(f, "Running"),
            NodeState::ShuttingDown => write!(f, "ShuttingDown"),
            NodeState::Dead => write!(f, "Dead"),
            NodeState::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Runtime information about a peer node (not persisted)
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Broker information
    pub broker: BrokerInfo,

    /// Last time we received a heartbeat from this peer
    pub last_heartbeat: Instant,

    /// Number of consecutive failed connection attempts
    pub failed_attempts: u32,

    /// Whether we have an active connection to this peer
    pub connected: bool,
}

impl PeerInfo {
    /// Create a new peer info
    pub fn new(broker: BrokerInfo) -> Self {
        Self {
            broker,
            last_heartbeat: Instant::now(),
            failed_attempts: 0,
            connected: false,
        }
    }

    /// Record a successful heartbeat
    pub fn record_heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
        self.failed_attempts = 0;
    }

    /// Record a failed connection attempt
    pub fn record_failure(&mut self) {
        self.failed_attempts += 1;
    }

    /// Check if the peer is considered alive based on heartbeat timeout
    pub fn is_alive(&self, timeout_ms: u64) -> bool {
        self.last_heartbeat.elapsed().as_millis() < timeout_ms as u128
    }

    /// Get time since last heartbeat in milliseconds
    pub fn ms_since_heartbeat(&self) -> u64 {
        self.last_heartbeat.elapsed().as_millis() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_info_creation() {
        let broker = BrokerInfo::new(
            1,
            "192.168.1.1:9092".parse().unwrap(),
            "192.168.1.1:9093".parse().unwrap(),
        );

        assert_eq!(broker.node_id, 1);
        assert_eq!(broker.state, NodeState::Starting);
        assert!(!broker.is_alive());
    }

    #[test]
    fn test_broker_with_rack() {
        let broker = BrokerInfo::new(
            1,
            "192.168.1.1:9092".parse().unwrap(),
            "192.168.1.1:9093".parse().unwrap(),
        )
        .with_rack("us-east-1a".to_string());

        assert_eq!(broker.rack, Some("us-east-1a".to_string()));
    }

    #[test]
    fn test_broker_with_datacenter() {
        let broker = BrokerInfo::new(
            1,
            "192.168.1.1:9092".parse().unwrap(),
            "192.168.1.1:9093".parse().unwrap(),
        )
        .with_datacenter("dc1".to_string())
        .with_rack("us-east-1a".to_string());

        assert_eq!(broker.datacenter, Some("dc1".to_string()));
        assert_eq!(broker.rack, Some("us-east-1a".to_string()));
    }

    #[test]
    fn test_node_state_display() {
        assert_eq!(NodeState::Running.to_string(), "Running");
        assert_eq!(NodeState::Dead.to_string(), "Dead");
    }

    #[test]
    fn test_peer_info_heartbeat() {
        let broker = BrokerInfo::new(
            1,
            "192.168.1.1:9092".parse().unwrap(),
            "192.168.1.1:9093".parse().unwrap(),
        );
        let mut peer = PeerInfo::new(broker);

        assert!(peer.is_alive(10000)); // Should be alive within 10 seconds
        peer.record_heartbeat();
        assert_eq!(peer.failed_attempts, 0);

        peer.record_failure();
        peer.record_failure();
        assert_eq!(peer.failed_attempts, 2);

        peer.record_heartbeat();
        assert_eq!(peer.failed_attempts, 0); // Reset on successful heartbeat
    }
}
