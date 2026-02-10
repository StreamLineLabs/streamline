//! Policy audit trail for tracking policy changes

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Maximum number of audit entries to keep
const MAX_AUDIT_ENTRIES: usize = 10000;

/// Type of policy change
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PolicyChangeType {
    /// Resource was created
    Create,
    /// Resource was updated
    Update,
    /// Resource was deleted
    Delete,
    /// Policy was loaded
    Load,
    /// Policy was validated
    Validate,
    /// Policy was applied
    Apply,
}

impl std::fmt::Display for PolicyChangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicyChangeType::Create => write!(f, "CREATE"),
            PolicyChangeType::Update => write!(f, "UPDATE"),
            PolicyChangeType::Delete => write!(f, "DELETE"),
            PolicyChangeType::Load => write!(f, "LOAD"),
            PolicyChangeType::Validate => write!(f, "VALIDATE"),
            PolicyChangeType::Apply => write!(f, "APPLY"),
        }
    }
}

/// A single audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyAuditEntry {
    /// Timestamp (ms since epoch)
    pub timestamp: i64,
    /// Type of change
    pub change_type: PolicyChangeType,
    /// Resource type (topic, acl, quota, schema, document)
    pub resource_type: String,
    /// Resource name
    pub resource_name: String,
    /// User/principal who made the change
    pub principal: Option<String>,
    /// Source of the change (file path, API, etc.)
    pub source: Option<String>,
    /// Additional details
    pub details: Option<String>,
    /// Previous value (for updates)
    pub previous_value: Option<String>,
    /// New value
    pub new_value: Option<String>,
}

impl PolicyAuditEntry {
    /// Create a new audit entry
    pub fn new(
        change_type: PolicyChangeType,
        resource_type: impl Into<String>,
        resource_name: impl Into<String>,
    ) -> Self {
        Self {
            timestamp: chrono::Utc::now().timestamp_millis(),
            change_type,
            resource_type: resource_type.into(),
            resource_name: resource_name.into(),
            principal: None,
            source: None,
            details: None,
            previous_value: None,
            new_value: None,
        }
    }

    /// Set the principal
    pub fn with_principal(mut self, principal: impl Into<String>) -> Self {
        self.principal = Some(principal.into());
        self
    }

    /// Set the source
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// Set details
    pub fn with_detail(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }

    /// Set previous value
    pub fn with_previous(mut self, value: impl Into<String>) -> Self {
        self.previous_value = Some(value.into());
        self
    }

    /// Set new value
    pub fn with_new(mut self, value: impl Into<String>) -> Self {
        self.new_value = Some(value.into());
        self
    }
}

/// Audit trail for policy changes
pub struct PolicyAuditTrail {
    /// Audit entries (newest first)
    entries: VecDeque<PolicyAuditEntry>,
    /// Maximum entries to keep
    max_entries: usize,
}

impl PolicyAuditTrail {
    /// Create a new audit trail
    pub fn new() -> Self {
        Self {
            entries: VecDeque::with_capacity(MAX_AUDIT_ENTRIES),
            max_entries: MAX_AUDIT_ENTRIES,
        }
    }

    /// Create with custom max entries
    pub fn with_max_entries(max: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(max),
            max_entries: max,
        }
    }

    /// Add an audit entry
    pub fn add(&mut self, entry: PolicyAuditEntry) {
        self.entries.push_front(entry);
        while self.entries.len() > self.max_entries {
            self.entries.pop_back();
        }
    }

    /// Get all entries
    pub fn entries(&self) -> impl Iterator<Item = &PolicyAuditEntry> {
        self.entries.iter()
    }

    /// Get entries by resource type
    pub fn entries_for_type(&self, resource_type: &str) -> Vec<&PolicyAuditEntry> {
        self.entries
            .iter()
            .filter(|e| e.resource_type == resource_type)
            .collect()
    }

    /// Get entries by resource name
    pub fn entries_for_resource(&self, name: &str) -> Vec<&PolicyAuditEntry> {
        self.entries
            .iter()
            .filter(|e| e.resource_name == name)
            .collect()
    }

    /// Get entries since a timestamp
    pub fn entries_since(&self, timestamp: i64) -> Vec<&PolicyAuditEntry> {
        self.entries
            .iter()
            .filter(|e| e.timestamp >= timestamp)
            .collect()
    }

    /// Get the most recent entries
    pub fn recent(&self, count: usize) -> Vec<&PolicyAuditEntry> {
        self.entries.iter().take(count).collect()
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Clear all entries
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Convert to a serializable log
    pub fn to_log(&self) -> PolicyAuditLog {
        PolicyAuditLog {
            entries: self.entries.iter().cloned().collect(),
            total: self.entries.len(),
        }
    }
}

impl Default for PolicyAuditTrail {
    fn default() -> Self {
        Self::new()
    }
}

/// Serializable audit log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyAuditLog {
    /// Audit entries
    pub entries: Vec<PolicyAuditEntry>,
    /// Total number of entries
    pub total: usize,
}

impl PolicyAuditLog {
    /// Filter entries by change type
    pub fn filter_by_type(&self, change_type: PolicyChangeType) -> Vec<&PolicyAuditEntry> {
        self.entries
            .iter()
            .filter(|e| e.change_type == change_type)
            .collect()
    }

    /// Filter entries by resource type
    pub fn filter_by_resource_type(&self, resource_type: &str) -> Vec<&PolicyAuditEntry> {
        self.entries
            .iter()
            .filter(|e| e.resource_type == resource_type)
            .collect()
    }

    /// Export to NDJSON format
    pub fn to_ndjson(&self) -> String {
        self.entries
            .iter()
            .filter_map(|e| serde_json::to_string(e).ok())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_entry_creation() {
        let entry = PolicyAuditEntry::new(PolicyChangeType::Create, "topic", "events")
            .with_principal("admin")
            .with_source("policy.yaml")
            .with_detail("Created topic with 3 partitions");

        assert_eq!(entry.change_type, PolicyChangeType::Create);
        assert_eq!(entry.resource_type, "topic");
        assert_eq!(entry.resource_name, "events");
        assert_eq!(entry.principal, Some("admin".to_string()));
    }

    #[test]
    fn test_audit_trail() {
        let mut trail = PolicyAuditTrail::new();

        trail.add(PolicyAuditEntry::new(
            PolicyChangeType::Create,
            "topic",
            "events",
        ));
        trail.add(PolicyAuditEntry::new(
            PolicyChangeType::Create,
            "topic",
            "logs",
        ));
        trail.add(PolicyAuditEntry::new(
            PolicyChangeType::Create,
            "acl",
            "User:alice",
        ));

        assert_eq!(trail.len(), 3);
        assert_eq!(trail.entries_for_type("topic").len(), 2);
        assert_eq!(trail.entries_for_type("acl").len(), 1);
    }

    #[test]
    fn test_audit_trail_max_entries() {
        let mut trail = PolicyAuditTrail::with_max_entries(5);

        for i in 0..10 {
            trail.add(PolicyAuditEntry::new(
                PolicyChangeType::Create,
                "topic",
                format!("topic-{}", i),
            ));
        }

        assert_eq!(trail.len(), 5);
        // Should have topics 5-9 (newest first)
        let entries: Vec<_> = trail.entries().collect();
        assert_eq!(entries[0].resource_name, "topic-9");
        assert_eq!(entries[4].resource_name, "topic-5");
    }

    #[test]
    fn test_audit_trail_recent() {
        let mut trail = PolicyAuditTrail::new();

        for i in 0..5 {
            trail.add(PolicyAuditEntry::new(
                PolicyChangeType::Create,
                "topic",
                format!("topic-{}", i),
            ));
        }

        let recent = trail.recent(3);
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0].resource_name, "topic-4");
    }

    #[test]
    fn test_audit_log_serialization() {
        let mut trail = PolicyAuditTrail::new();
        trail.add(PolicyAuditEntry::new(
            PolicyChangeType::Create,
            "topic",
            "events",
        ));

        let log = trail.to_log();
        let json = serde_json::to_string(&log).unwrap();

        assert!(json.contains("events"));
        assert!(json.contains("CREATE") || json.contains("create"));
    }

    #[test]
    fn test_audit_log_ndjson_export() {
        let mut trail = PolicyAuditTrail::new();
        trail.add(PolicyAuditEntry::new(
            PolicyChangeType::Create,
            "topic",
            "events",
        ));
        trail.add(PolicyAuditEntry::new(
            PolicyChangeType::Update,
            "acl",
            "User:bob",
        ));

        let log = trail.to_log();
        let ndjson = log.to_ndjson();
        let lines: Vec<_> = ndjson.lines().collect();

        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn test_change_type_display() {
        assert_eq!(format!("{}", PolicyChangeType::Create), "CREATE");
        assert_eq!(format!("{}", PolicyChangeType::Update), "UPDATE");
        assert_eq!(format!("{}", PolicyChangeType::Delete), "DELETE");
    }
}
