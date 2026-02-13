//! Conflict resolution for edge-cloud synchronization
//!
//! Handles merge conflicts when edge and cloud data diverge.

use crate::storage::Record;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::config::ConflictResolution;

/// Result of a conflict resolution attempt
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionResult {
    /// Use the edge record
    UseEdge,
    /// Use the cloud record
    UseCloud,
    /// Use a merged record
    UseMerged,
    /// Skip both records
    Skip,
    /// Conflict could not be resolved automatically
    Unresolved,
}

/// Details about a detected conflict
#[derive(Debug, Clone)]
pub struct ConflictInfo {
    /// Topic where conflict occurred
    pub topic: String,
    /// Partition where conflict occurred
    pub partition: i32,
    /// Edge record offset
    pub edge_offset: i64,
    /// Cloud record offset
    pub cloud_offset: i64,
    /// Edge record timestamp
    pub edge_timestamp: i64,
    /// Cloud record timestamp
    pub cloud_timestamp: i64,
    /// Optional key if records have keys
    pub key: Option<Vec<u8>>,
    /// Resolution applied
    pub resolution: ResolutionResult,
    /// Timestamp when conflict was detected
    pub detected_at: i64,
}

/// Statistics for conflict resolution
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConflictStats {
    /// Total conflicts detected
    pub total_conflicts: u64,
    /// Conflicts resolved by edge winning
    pub edge_wins: u64,
    /// Conflicts resolved by cloud winning
    pub cloud_wins: u64,
    /// Conflicts resolved by merge
    pub merged: u64,
    /// Conflicts skipped
    pub skipped: u64,
    /// Unresolved conflicts requiring manual intervention
    pub unresolved: u64,
    /// Last conflict timestamp
    pub last_conflict_at: Option<i64>,
}

/// Conflict resolver for edge-cloud synchronization
pub struct ConflictResolver {
    strategy: ConflictResolution,
    stats: parking_lot::RwLock<ConflictStats>,
    /// Custom resolution callback (if strategy is Custom)
    #[allow(dead_code, clippy::type_complexity)]
    custom_resolver: Option<Box<dyn Fn(&Record, &Record) -> ResolutionResult + Send + Sync>>,
    /// Conflict history for debugging
    conflict_history: parking_lot::RwLock<Vec<ConflictInfo>>,
    /// Max history entries to keep
    max_history: usize,
}

impl ConflictResolver {
    /// Create a new conflict resolver with the given strategy
    pub fn new(strategy: ConflictResolution) -> Self {
        Self {
            strategy,
            stats: parking_lot::RwLock::new(ConflictStats::default()),
            custom_resolver: None,
            conflict_history: parking_lot::RwLock::new(Vec::new()),
            max_history: 1000,
        }
    }

    /// Create a conflict resolver with a custom resolution function
    pub fn with_custom_resolver<F>(resolver: F) -> Self
    where
        F: Fn(&Record, &Record) -> ResolutionResult + Send + Sync + 'static,
    {
        Self {
            strategy: ConflictResolution::Custom,
            stats: parking_lot::RwLock::new(ConflictStats::default()),
            custom_resolver: Some(Box::new(resolver)),
            conflict_history: parking_lot::RwLock::new(Vec::new()),
            max_history: 1000,
        }
    }

    /// Resolve a conflict between edge and cloud records
    pub fn resolve(&self, edge: &Record, cloud: &Record) -> ResolutionResult {
        let result = match self.strategy {
            ConflictResolution::LastWriteWins => self.resolve_last_write_wins(edge, cloud),
            ConflictResolution::CloudWins => ResolutionResult::UseCloud,
            ConflictResolution::EdgeWins => ResolutionResult::UseEdge,
            ConflictResolution::HigherOffsetWins => self.resolve_higher_offset_wins(edge, cloud),
            ConflictResolution::Custom => {
                if let Some(ref resolver) = self.custom_resolver {
                    resolver(edge, cloud)
                } else {
                    ResolutionResult::Unresolved
                }
            }
        };

        // Update statistics
        self.update_stats(&result, edge.timestamp);

        result
    }

    /// Resolve using last-write-wins strategy
    fn resolve_last_write_wins(&self, edge: &Record, cloud: &Record) -> ResolutionResult {
        if edge.timestamp > cloud.timestamp {
            ResolutionResult::UseEdge
        } else if cloud.timestamp > edge.timestamp {
            ResolutionResult::UseCloud
        } else {
            // Same timestamp - prefer cloud as tie-breaker
            ResolutionResult::UseCloud
        }
    }

    /// Resolve using higher-offset-wins strategy
    fn resolve_higher_offset_wins(&self, edge: &Record, cloud: &Record) -> ResolutionResult {
        if edge.offset > cloud.offset {
            ResolutionResult::UseEdge
        } else if cloud.offset > edge.offset {
            ResolutionResult::UseCloud
        } else {
            // Same offset - fall back to timestamp
            self.resolve_last_write_wins(edge, cloud)
        }
    }

    /// Detect conflicts between edge and cloud record sets
    pub fn detect_conflicts(
        &self,
        edge_records: &[Record],
        cloud_records: &[Record],
        topic: &str,
        partition: i32,
    ) -> Vec<ConflictInfo> {
        let mut conflicts = Vec::new();

        // Build a map of cloud records by key (or offset if no key)
        let mut cloud_map: HashMap<Vec<u8>, &Record> = HashMap::new();
        for record in cloud_records {
            let key = record
                .key
                .as_ref()
                .map(|k| k.to_vec())
                .unwrap_or_else(|| record.offset.to_le_bytes().to_vec());
            cloud_map.insert(key, record);
        }

        // Check each edge record for conflicts
        for edge_record in edge_records {
            let key = edge_record
                .key
                .as_ref()
                .map(|k| k.to_vec())
                .unwrap_or_else(|| edge_record.offset.to_le_bytes().to_vec());

            if let Some(cloud_record) = cloud_map.get(&key) {
                // Potential conflict - same key exists in both
                if edge_record.offset != cloud_record.offset
                    || edge_record.timestamp != cloud_record.timestamp
                {
                    let resolution = self.resolve(edge_record, cloud_record);

                    let conflict = ConflictInfo {
                        topic: topic.to_string(),
                        partition,
                        edge_offset: edge_record.offset,
                        cloud_offset: cloud_record.offset,
                        edge_timestamp: edge_record.timestamp,
                        cloud_timestamp: cloud_record.timestamp,
                        key: edge_record.key.as_ref().map(|k| k.to_vec()),
                        resolution,
                        detected_at: chrono::Utc::now().timestamp_millis(),
                    };

                    conflicts.push(conflict);
                }
            }
        }

        // Store conflict history
        self.add_to_history(&conflicts);

        conflicts
    }

    /// Update statistics based on resolution result
    fn update_stats(&self, result: &ResolutionResult, timestamp: i64) {
        let mut stats = self.stats.write();
        stats.total_conflicts += 1;
        stats.last_conflict_at = Some(timestamp);

        match result {
            ResolutionResult::UseEdge => stats.edge_wins += 1,
            ResolutionResult::UseCloud => stats.cloud_wins += 1,
            ResolutionResult::UseMerged => stats.merged += 1,
            ResolutionResult::Skip => stats.skipped += 1,
            ResolutionResult::Unresolved => stats.unresolved += 1,
        }
    }

    /// Add conflicts to history
    fn add_to_history(&self, conflicts: &[ConflictInfo]) {
        let mut history = self.conflict_history.write();
        for conflict in conflicts {
            history.push(conflict.clone());
        }

        // Trim history if needed
        if history.len() > self.max_history {
            let excess = history.len() - self.max_history;
            history.drain(0..excess);
        }
    }

    /// Get current statistics
    pub fn stats(&self) -> ConflictStats {
        self.stats.read().clone()
    }

    /// Get conflict history
    pub fn history(&self) -> Vec<ConflictInfo> {
        self.conflict_history.read().clone()
    }

    /// Clear statistics and history
    pub fn reset(&self) {
        *self.stats.write() = ConflictStats::default();
        self.conflict_history.write().clear();
    }

    /// Get the current resolution strategy
    pub fn strategy(&self) -> ConflictResolution {
        self.strategy
    }
}

/// Three-way merge for record values
#[allow(dead_code)]
pub struct ThreeWayMerge;

#[allow(dead_code)]
impl ThreeWayMerge {
    /// Attempt to merge two record values with a common ancestor
    /// Returns None if merge is not possible (binary data or no common structure)
    pub fn merge(base: &[u8], edge: &[u8], cloud: &[u8]) -> Option<Vec<u8>> {
        // Try JSON merge first
        if let Some(merged) = Self::merge_json(base, edge, cloud) {
            return Some(merged);
        }

        // For binary data, we can't merge - return None
        None
    }

    /// Attempt to merge JSON values
    fn merge_json(base: &[u8], edge: &[u8], cloud: &[u8]) -> Option<Vec<u8>> {
        let base_json: serde_json::Value = serde_json::from_slice(base).ok()?;
        let edge_json: serde_json::Value = serde_json::from_slice(edge).ok()?;
        let cloud_json: serde_json::Value = serde_json::from_slice(cloud).ok()?;

        // Only merge objects
        let base_obj = base_json.as_object()?;
        let edge_obj = edge_json.as_object()?;
        let cloud_obj = cloud_json.as_object()?;

        let mut merged = base_obj.clone();

        // Apply edge changes
        for (key, edge_value) in edge_obj {
            let base_value = base_obj.get(key);
            let cloud_value = cloud_obj.get(key);

            match (base_value, cloud_value) {
                (Some(bv), Some(cv)) if bv == edge_value && cv != edge_value => {
                    // Edge changed from base, cloud didn't change - use edge
                    merged.insert(key.clone(), edge_value.clone());
                }
                (Some(bv), Some(cv)) if bv != edge_value && cv == bv => {
                    // Edge changed, cloud same as base - use edge
                    merged.insert(key.clone(), edge_value.clone());
                }
                (Some(_bv), Some(cv)) if cv == edge_value => {
                    // Both changed to same value - no conflict
                    merged.insert(key.clone(), edge_value.clone());
                }
                (None, None) => {
                    // New key added by edge
                    merged.insert(key.clone(), edge_value.clone());
                }
                _ => {
                    // Complex conflict - can't auto-merge
                    // For now, prefer edge value
                    merged.insert(key.clone(), edge_value.clone());
                }
            }
        }

        // Apply cloud-only additions
        for (key, cloud_value) in cloud_obj {
            if !edge_obj.contains_key(key) && !base_obj.contains_key(key) {
                merged.insert(key.clone(), cloud_value.clone());
            }
        }

        serde_json::to_vec(&serde_json::Value::Object(merged)).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn create_test_record(offset: i64, timestamp: i64, key: Option<&[u8]>) -> Record {
        Record {
            offset,
            timestamp,
            key: key.map(Bytes::copy_from_slice),
            value: Bytes::from("test value"),
            headers: Vec::new(),
            crc: None,
        }
    }

    #[test]
    fn test_last_write_wins_edge() {
        let resolver = ConflictResolver::new(ConflictResolution::LastWriteWins);

        let edge = create_test_record(1, 1000, None);
        let cloud = create_test_record(1, 900, None);

        assert_eq!(resolver.resolve(&edge, &cloud), ResolutionResult::UseEdge);
    }

    #[test]
    fn test_last_write_wins_cloud() {
        let resolver = ConflictResolver::new(ConflictResolution::LastWriteWins);

        let edge = create_test_record(1, 900, None);
        let cloud = create_test_record(1, 1000, None);

        assert_eq!(resolver.resolve(&edge, &cloud), ResolutionResult::UseCloud);
    }

    #[test]
    fn test_cloud_wins() {
        let resolver = ConflictResolver::new(ConflictResolution::CloudWins);

        let edge = create_test_record(1, 2000, None);
        let cloud = create_test_record(1, 1000, None);

        assert_eq!(resolver.resolve(&edge, &cloud), ResolutionResult::UseCloud);
    }

    #[test]
    fn test_edge_wins() {
        let resolver = ConflictResolver::new(ConflictResolution::EdgeWins);

        let edge = create_test_record(1, 1000, None);
        let cloud = create_test_record(1, 2000, None);

        assert_eq!(resolver.resolve(&edge, &cloud), ResolutionResult::UseEdge);
    }

    #[test]
    fn test_higher_offset_wins() {
        let resolver = ConflictResolver::new(ConflictResolution::HigherOffsetWins);

        let edge = create_test_record(10, 1000, None);
        let cloud = create_test_record(5, 2000, None);

        assert_eq!(resolver.resolve(&edge, &cloud), ResolutionResult::UseEdge);
    }

    #[test]
    fn test_custom_resolver() {
        let resolver =
            ConflictResolver::with_custom_resolver(|_edge, _cloud| ResolutionResult::UseMerged);

        let edge = create_test_record(1, 1000, None);
        let cloud = create_test_record(1, 1000, None);

        assert_eq!(resolver.resolve(&edge, &cloud), ResolutionResult::UseMerged);
    }

    #[test]
    fn test_conflict_stats() {
        let resolver = ConflictResolver::new(ConflictResolution::LastWriteWins);

        let edge1 = create_test_record(1, 1000, None);
        let cloud1 = create_test_record(1, 900, None);
        resolver.resolve(&edge1, &cloud1);

        let edge2 = create_test_record(2, 800, None);
        let cloud2 = create_test_record(2, 1000, None);
        resolver.resolve(&edge2, &cloud2);

        let stats = resolver.stats();
        assert_eq!(stats.total_conflicts, 2);
        assert_eq!(stats.edge_wins, 1);
        assert_eq!(stats.cloud_wins, 1);
    }

    #[test]
    fn test_detect_conflicts() {
        let resolver = ConflictResolver::new(ConflictResolution::LastWriteWins);

        let edge_records = vec![
            create_test_record(1, 1000, Some(b"key1")),
            create_test_record(2, 2000, Some(b"key2")),
        ];

        let cloud_records = vec![
            create_test_record(1, 900, Some(b"key1")), // Conflict
            create_test_record(3, 1500, Some(b"key3")),
        ];

        let conflicts = resolver.detect_conflicts(&edge_records, &cloud_records, "test-topic", 0);

        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].edge_offset, 1);
        assert_eq!(conflicts[0].cloud_offset, 1);
    }

    #[test]
    fn test_json_merge() {
        let base = r#"{"a": 1, "b": 2}"#.as_bytes();
        let edge = r#"{"a": 1, "b": 3}"#.as_bytes(); // Changed b
        let cloud = r#"{"a": 1, "b": 2, "c": 4}"#.as_bytes(); // Added c

        let merged = ThreeWayMerge::merge(base, edge, cloud).unwrap();
        let merged_json: serde_json::Value = serde_json::from_slice(&merged).unwrap();

        assert_eq!(merged_json["a"], 1);
        assert_eq!(merged_json["b"], 3); // Edge change
        assert_eq!(merged_json["c"], 4); // Cloud addition
    }
}
