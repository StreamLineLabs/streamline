//! Offset storage for consumer groups

use crate::consumer::group::{CommittedOffset, ConsumerGroup};
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{debug, warn};

/// Offset storage for consumer groups
pub struct OffsetStore {
    /// Base directory for offset storage
    base_path: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
struct OffsetEntry {
    topic: String,
    partition: i32,
    offset: i64,
    metadata: String,
    commit_timestamp: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct OffsetData {
    offsets: Vec<OffsetEntry>,
}

impl OffsetStore {
    /// Create a new offset store
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path)?;
        Ok(Self { base_path })
    }

    /// Get the offset file path for a group
    fn group_offsets_path(&self, group_id: &str) -> PathBuf {
        self.base_path.join(group_id).join("offsets.json")
    }

    /// Load offsets for a group
    pub fn load_offsets(&self, group_id: &str) -> Result<HashMap<(String, i32), CommittedOffset>> {
        let path = self.group_offsets_path(group_id);

        if !path.exists() {
            return Ok(HashMap::new());
        }

        let content = fs::read_to_string(&path)?;
        let data: OffsetData = serde_json::from_str(&content)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to parse offsets: {}", e)))?;

        let mut offsets = HashMap::new();
        for entry in data.offsets {
            offsets.insert(
                (entry.topic, entry.partition),
                CommittedOffset {
                    offset: entry.offset,
                    metadata: entry.metadata,
                    commit_timestamp: entry.commit_timestamp,
                },
            );
        }

        debug!(group_id = %group_id, num_offsets = offsets.len(), "Loaded offsets");
        Ok(offsets)
    }

    /// Save offsets for a group
    pub fn save_offsets(
        &self,
        group_id: &str,
        offsets: &HashMap<(String, i32), CommittedOffset>,
    ) -> Result<()> {
        let path = self.group_offsets_path(group_id);

        // Create parent directory
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let entries: Vec<OffsetEntry> = offsets
            .iter()
            .map(|((topic, partition), co)| OffsetEntry {
                topic: topic.clone(),
                partition: *partition,
                offset: co.offset,
                metadata: co.metadata.clone(),
                commit_timestamp: co.commit_timestamp,
            })
            .collect();

        let data = OffsetData { offsets: entries };

        let content = serde_json::to_string_pretty(&data)?;
        fs::write(&path, content)?;

        debug!(group_id = %group_id, num_offsets = offsets.len(), "Saved offsets");
        Ok(())
    }

    /// Load group metadata
    pub fn load_group(&self, group_id: &str) -> Result<Option<ConsumerGroup>> {
        let path = self.base_path.join(group_id).join("metadata.json");

        if !path.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(&path)?;
        let mut group: ConsumerGroup = serde_json::from_str(&content)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to parse group: {}", e)))?;

        // Load offsets
        group.offsets = self.load_offsets(group_id)?;

        debug!(group_id = %group_id, "Loaded group metadata");
        Ok(Some(group))
    }

    /// Save group metadata
    pub fn save_group(&self, group: &ConsumerGroup) -> Result<()> {
        let path = self.base_path.join(&group.group_id).join("metadata.json");

        // Create parent directory
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let content = serde_json::to_string_pretty(group)?;
        fs::write(&path, content)?;

        // Save offsets separately
        self.save_offsets(&group.group_id, &group.offsets)?;

        debug!(group_id = %group.group_id, "Saved group metadata");
        Ok(())
    }

    /// Delete group data
    pub fn delete_group(&self, group_id: &str) -> Result<()> {
        let path = self.base_path.join(group_id);

        if path.exists() {
            fs::remove_dir_all(&path)?;
            debug!(group_id = %group_id, "Deleted group data");
        } else {
            warn!(group_id = %group_id, "Group data not found");
        }

        Ok(())
    }

    /// List all groups
    pub fn list_groups(&self) -> Result<Vec<String>> {
        if !self.base_path.exists() {
            return Ok(Vec::new());
        }

        let mut groups = Vec::new();

        for entry in fs::read_dir(&self.base_path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    groups.push(name.to_string());
                }
            }
        }

        Ok(groups)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_offset_store_save_and_load() {
        let dir = tempdir().unwrap();
        let store = OffsetStore::new(dir.path()).unwrap();

        let mut offsets = HashMap::new();
        offsets.insert(
            ("topic1".to_string(), 0),
            CommittedOffset {
                offset: 100,
                metadata: "test".to_string(),
                commit_timestamp: 123456789,
            },
        );

        store.save_offsets("test-group", &offsets).unwrap();
        let loaded = store.load_offsets("test-group").unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded.get(&("topic1".to_string(), 0)).unwrap().offset, 100);
    }

    #[test]
    fn test_group_save_and_load() {
        let dir = tempdir().unwrap();
        let store = OffsetStore::new(dir.path()).unwrap();

        let group = ConsumerGroup::new("test-group".to_string());
        store.save_group(&group).unwrap();

        let loaded = store.load_group("test-group").unwrap();
        assert!(loaded.is_some());

        let loaded_group = loaded.unwrap();
        assert_eq!(loaded_group.group_id, "test-group");
    }

    #[test]
    fn test_list_groups() {
        let dir = tempdir().unwrap();
        let store = OffsetStore::new(dir.path()).unwrap();

        let group1 = ConsumerGroup::new("group1".to_string());
        let group2 = ConsumerGroup::new("group2".to_string());

        store.save_group(&group1).unwrap();
        store.save_group(&group2).unwrap();

        let groups = store.list_groups().unwrap();
        assert_eq!(groups.len(), 2);
        assert!(groups.contains(&"group1".to_string()));
        assert!(groups.contains(&"group2".to_string()));
    }
}
