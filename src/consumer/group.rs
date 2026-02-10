//! Consumer group data structures

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::time::Instant;

/// Custom serialization for HashMap with (String, i32) tuple keys
/// JSON requires string keys, so we serialize as "topic:partition"
mod offset_map_serde {
    use super::*;

    pub fn serialize<S>(
        map: &HashMap<(String, i32), CommittedOffset>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;
        let mut ser_map = serializer.serialize_map(Some(map.len()))?;
        for ((topic, partition), value) in map {
            let key = format!("{}:{}", topic, partition);
            ser_map.serialize_entry(&key, value)?;
        }
        ser_map.end()
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<(String, i32), CommittedOffset>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string_map: HashMap<String, CommittedOffset> = HashMap::deserialize(deserializer)?;
        let mut result = HashMap::new();
        for (key, value) in string_map {
            // Parse "topic:partition" format
            if let Some((topic, partition_str)) = key.rsplit_once(':') {
                if let Ok(partition) = partition_str.parse::<i32>() {
                    result.insert((topic.to_string(), partition), value);
                }
            }
        }
        Ok(result)
    }
}

/// Consumer group state
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupState {
    /// Group has no members
    Empty,
    /// Group is preparing to rebalance
    PreparingRebalance,
    /// Group is completing rebalance (waiting for SyncGroup from all members)
    CompletingRebalance,
    /// Group is stable and consuming
    Stable,
    /// Group is dead (marked for deletion)
    Dead,
}

/// Consumer group member
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMember {
    /// Member ID (assigned by coordinator)
    pub member_id: String,

    /// Client ID (from client)
    pub client_id: String,

    /// Client host
    pub client_host: String,

    /// Session timeout in milliseconds
    pub session_timeout_ms: i32,

    /// Rebalance timeout in milliseconds
    pub rebalance_timeout_ms: i32,

    /// Subscribed topics
    pub subscriptions: Vec<String>,

    /// Assigned partitions (topic, partition)
    pub assignment: Vec<(String, i32)>,

    /// Last heartbeat timestamp
    #[serde(skip, default = "Instant::now")]
    pub last_heartbeat: Instant,

    /// Protocol metadata (client-provided subscription info)
    pub protocol_metadata: Vec<u8>,
}

/// Committed offset information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommittedOffset {
    /// Offset value
    pub offset: i64,

    /// Optional metadata
    pub metadata: String,

    /// Commit timestamp (milliseconds since epoch)
    pub commit_timestamp: i64,
}

/// Consumer group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroup {
    /// Group ID
    pub group_id: String,

    /// Generation ID (incremented on each rebalance)
    pub generation_id: i32,

    /// Protocol type (typically "consumer")
    pub protocol_type: String,

    /// Protocol name (e.g., "range", "roundrobin")
    pub protocol_name: String,

    /// Leader member ID (chosen during join)
    pub leader: Option<String>,

    /// Group members
    pub members: HashMap<String, GroupMember>,

    /// Group state
    pub state: GroupState,

    /// Committed offsets by (topic, partition)
    #[serde(with = "offset_map_serde")]
    pub offsets: HashMap<(String, i32), CommittedOffset>,

    /// Timestamp when the current rebalance started
    #[serde(skip, default = "Instant::now")]
    pub rebalance_start: Instant,

    /// Set of members who have joined the current rebalance
    #[serde(skip, default)]
    pub pending_members: HashMap<String, Instant>,
}

impl ConsumerGroup {
    /// Create a new consumer group
    pub fn new(group_id: String) -> Self {
        Self {
            group_id,
            generation_id: 0,
            protocol_type: "consumer".to_string(),
            protocol_name: "range".to_string(),
            leader: None,
            members: HashMap::new(),
            state: GroupState::Empty,
            offsets: HashMap::new(),
            rebalance_start: Instant::now(),
            pending_members: HashMap::new(),
        }
    }

    /// Start a new rebalance
    pub fn start_rebalance(&mut self) {
        self.state = GroupState::PreparingRebalance;
        self.generation_id += 1;
        self.rebalance_start = Instant::now();
        self.pending_members.clear();
    }

    /// Mark a member as having joined the current rebalance
    pub fn member_joined(&mut self, member_id: &str) {
        self.pending_members
            .insert(member_id.to_string(), Instant::now());
    }

    /// Get the maximum rebalance timeout from all members
    pub fn max_rebalance_timeout_ms(&self) -> i32 {
        self.members
            .values()
            .map(|m| m.rebalance_timeout_ms)
            .max()
            .unwrap_or(300000) // Default to 5 minutes if no members
    }

    /// Check if a member has exceeded the rebalance timeout
    pub fn has_member_exceeded_rebalance_timeout(&self, member_id: &str, now: Instant) -> bool {
        if let Some(member) = self.members.get(member_id) {
            let timeout = std::time::Duration::from_millis(member.rebalance_timeout_ms as u64);
            now.duration_since(self.rebalance_start) > timeout
        } else {
            false
        }
    }

    /// Check if a member has joined the current rebalance
    pub fn has_member_joined(&self, member_id: &str) -> bool {
        self.pending_members.contains_key(member_id)
    }

    /// Get all subscribed topics from all members
    pub fn get_all_subscribed_topics(&self) -> Vec<String> {
        let mut topics = std::collections::HashSet::new();
        for member in self.members.values() {
            for topic in &member.subscriptions {
                topics.insert(topic.clone());
            }
        }
        topics.into_iter().collect()
    }

    /// Check if a member has expired based on session timeout
    pub fn has_member_expired(&self, member_id: &str, now: Instant) -> bool {
        if let Some(member) = self.members.get(member_id) {
            let timeout = std::time::Duration::from_millis(member.session_timeout_ms as u64);
            now.duration_since(member.last_heartbeat) > timeout
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_group() {
        let group = ConsumerGroup::new("test-group".to_string());
        assert_eq!(group.group_id, "test-group");
        assert_eq!(group.generation_id, 0);
        assert_eq!(group.state, GroupState::Empty);
        assert!(group.members.is_empty());
    }

    #[test]
    fn test_get_all_subscribed_topics() {
        let mut group = ConsumerGroup::new("test-group".to_string());

        let member1 = GroupMember {
            member_id: "member1".to_string(),
            client_id: "client1".to_string(),
            client_host: "localhost".to_string(),
            session_timeout_ms: 45000,
            rebalance_timeout_ms: 300000,
            subscriptions: vec!["topic1".to_string(), "topic2".to_string()],
            assignment: vec![],
            last_heartbeat: Instant::now(),
            protocol_metadata: vec![],
        };

        let member2 = GroupMember {
            member_id: "member2".to_string(),
            client_id: "client2".to_string(),
            client_host: "localhost".to_string(),
            session_timeout_ms: 45000,
            rebalance_timeout_ms: 300000,
            subscriptions: vec!["topic2".to_string(), "topic3".to_string()],
            assignment: vec![],
            last_heartbeat: Instant::now(),
            protocol_metadata: vec![],
        };

        group.members.insert("member1".to_string(), member1);
        group.members.insert("member2".to_string(), member2);

        let topics = group.get_all_subscribed_topics();
        assert_eq!(topics.len(), 3);
        assert!(topics.contains(&"topic1".to_string()));
        assert!(topics.contains(&"topic2".to_string()));
        assert!(topics.contains(&"topic3".to_string()));
    }
}
