//! Branch metadata: stored in the `__branches` system topic, signed via M4
//! attestation (so branches cannot be silently rewritten).

use serde::{Deserialize, Serialize};

/// Globally unique branch identifier. Format: `<base-topic>:<name>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BranchId(pub String);

impl BranchId {
    pub fn new(base_topic: &str, name: &str) -> Self {
        Self(format!("{base_topic}:{name}"))
    }

    pub fn parts(&self) -> Option<(&str, &str)> {
        self.0.split_once(':')
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BranchState {
    Active,
    Discarded,
    Merged,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchMeta {
    pub id: BranchId,
    /// Base topic this branch forks from.
    pub base_topic: String,
    /// Per-partition base offsets the branch starts from. `Vec<i64>` indexed
    /// by partition number; entries beyond the base topic's partition count
    /// are invalid.
    pub base_offsets: Vec<i64>,
    pub created_by: String,
    pub created_at_ms: u64,
    pub state: BranchState,
}

impl BranchMeta {
    pub fn new(base_topic: &str, name: &str, base_offsets: Vec<i64>, created_by: &str) -> Self {
        Self {
            id: BranchId::new(base_topic, name),
            base_topic: base_topic.into(),
            base_offsets,
            created_by: created_by.into(),
            created_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            state: BranchState::Active,
        }
    }

    /// Topic name where the branch's own writes live.
    pub fn write_topic(&self) -> String {
        format!(
            "__branch.{}.{}",
            self.base_topic,
            self.id.parts().map(|(_, n)| n).unwrap_or("anon")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_id_roundtrip() {
        let id = BranchId::new("orders", "experiment-a");
        assert_eq!(id.parts(), Some(("orders", "experiment-a")));
    }

    #[test]
    fn write_topic_includes_branch_name() {
        let m = BranchMeta::new("orders", "exp-a", vec![10, 20], "alice");
        assert_eq!(m.write_topic(), "__branch.orders.exp-a");
        assert_eq!(m.state, BranchState::Active);
    }
}
