//! Acknowledgment policies for produce requests
//!
//! This module defines how producers wait for acknowledgment of writes:
//! - acks=0: No acknowledgment (fire and forget)
//! - acks=1: Leader acknowledgment only
//! - acks=all/-1: All in-sync replicas must acknowledge

use std::fmt;

/// Acknowledgment policy for produce requests
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AcksPolicy {
    /// No acknowledgment required (acks=0)
    /// Producer doesn't wait for any confirmation
    /// Fastest but least durable
    None,

    /// Leader acknowledgment only (acks=1)
    /// Producer waits for leader to write to local log
    /// Default behavior, good balance of speed and durability
    #[default]
    Leader,

    /// All in-sync replicas must acknowledge (acks=all or acks=-1)
    /// Producer waits for all ISR members to confirm
    /// Most durable but slowest
    All,
}

impl AcksPolicy {
    /// Create AcksPolicy from Kafka protocol acks value
    pub fn from_kafka_acks(acks: i16) -> Self {
        match acks {
            0 => AcksPolicy::None,
            1 => AcksPolicy::Leader,
            -1 => AcksPolicy::All,
            // Treat any other value as "all" for safety
            _ => AcksPolicy::All,
        }
    }

    /// Convert to Kafka protocol acks value
    pub fn to_kafka_acks(self) -> i16 {
        match self {
            AcksPolicy::None => 0,
            AcksPolicy::Leader => 1,
            AcksPolicy::All => -1,
        }
    }

    /// Check if this policy requires waiting for any acknowledgment
    pub fn requires_ack(&self) -> bool {
        !matches!(self, AcksPolicy::None)
    }

    /// Check if this policy requires waiting for all ISR replicas
    pub fn requires_all_isr(&self) -> bool {
        matches!(self, AcksPolicy::All)
    }
}

impl fmt::Display for AcksPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AcksPolicy::None => write!(f, "acks=0"),
            AcksPolicy::Leader => write!(f, "acks=1"),
            AcksPolicy::All => write!(f, "acks=all"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acks_from_kafka() {
        assert_eq!(AcksPolicy::from_kafka_acks(0), AcksPolicy::None);
        assert_eq!(AcksPolicy::from_kafka_acks(1), AcksPolicy::Leader);
        assert_eq!(AcksPolicy::from_kafka_acks(-1), AcksPolicy::All);
        // Unknown values default to All for safety
        assert_eq!(AcksPolicy::from_kafka_acks(99), AcksPolicy::All);
    }

    #[test]
    fn test_acks_to_kafka() {
        assert_eq!(AcksPolicy::None.to_kafka_acks(), 0);
        assert_eq!(AcksPolicy::Leader.to_kafka_acks(), 1);
        assert_eq!(AcksPolicy::All.to_kafka_acks(), -1);
    }

    #[test]
    fn test_requires_ack() {
        assert!(!AcksPolicy::None.requires_ack());
        assert!(AcksPolicy::Leader.requires_ack());
        assert!(AcksPolicy::All.requires_ack());
    }

    #[test]
    fn test_requires_all_isr() {
        assert!(!AcksPolicy::None.requires_all_isr());
        assert!(!AcksPolicy::Leader.requires_all_isr());
        assert!(AcksPolicy::All.requires_all_isr());
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", AcksPolicy::None), "acks=0");
        assert_eq!(format!("{}", AcksPolicy::Leader), "acks=1");
        assert_eq!(format!("{}", AcksPolicy::All), "acks=all");
    }

    #[test]
    fn test_default() {
        assert_eq!(AcksPolicy::default(), AcksPolicy::Leader);
    }
}
