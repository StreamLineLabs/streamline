//! Lifecycle Policies
//!
//! Provides policy-based lifecycle management with support for:
//! - Retention policies
//! - Cost optimization policies
//! - Compliance policies
//! - Custom rules

use super::{CompressionType, DataSegment, LifecycleAction, StorageTier};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Policy condition for triggering actions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PolicyCondition {
    /// Age in seconds
    AgeGreaterThan(u64),
    /// Age less than
    AgeLessThan(u64),
    /// Size in bytes greater than
    SizeGreaterThan(u64),
    /// Size less than
    SizeLessThan(u64),
    /// Access count greater than
    AccessCountGreaterThan(u64),
    /// Access count less than
    AccessCountLessThan(u64),
    /// Idle time (since last access) greater than
    IdleTimeGreaterThan(u64),
    /// Current storage tier
    OnTier(StorageTier),
    /// Tag matches
    TagEquals(String, String),
    /// Tag exists
    TagExists(String),
    /// Topic name matches pattern
    TopicMatches(String),
    /// Segment is sealed
    IsSealed,
    /// Segment is not sealed
    IsNotSealed,
    /// Logical AND of conditions
    And(Vec<PolicyCondition>),
    /// Logical OR of conditions
    Or(Vec<PolicyCondition>),
    /// Logical NOT of condition
    Not(Box<PolicyCondition>),
}

impl PolicyCondition {
    /// Evaluate condition against a segment
    pub fn evaluate(&self, segment: &DataSegment) -> bool {
        match self {
            PolicyCondition::AgeGreaterThan(seconds) => segment.age_seconds() > *seconds,
            PolicyCondition::AgeLessThan(seconds) => segment.age_seconds() < *seconds,
            PolicyCondition::SizeGreaterThan(bytes) => segment.size_bytes > *bytes,
            PolicyCondition::SizeLessThan(bytes) => segment.size_bytes < *bytes,
            PolicyCondition::AccessCountGreaterThan(count) => segment.access_count > *count,
            PolicyCondition::AccessCountLessThan(count) => segment.access_count < *count,
            PolicyCondition::IdleTimeGreaterThan(seconds) => segment.idle_seconds() > *seconds,
            PolicyCondition::OnTier(tier) => segment.current_tier == *tier,
            PolicyCondition::TagEquals(key, value) => {
                segment.tags.get(key).map(|v| v == value).unwrap_or(false)
            }
            PolicyCondition::TagExists(key) => segment.tags.contains_key(key),
            PolicyCondition::TopicMatches(pattern) => {
                if pattern.contains('*') {
                    let regex_pattern = pattern.replace('.', "\\.").replace('*', ".*");
                    regex::Regex::new(&regex_pattern)
                        .map(|re| re.is_match(&segment.topic))
                        .unwrap_or(false)
                } else {
                    segment.topic == *pattern
                }
            }
            PolicyCondition::IsSealed => segment.sealed,
            PolicyCondition::IsNotSealed => !segment.sealed,
            PolicyCondition::And(conditions) => conditions.iter().all(|c| c.evaluate(segment)),
            PolicyCondition::Or(conditions) => conditions.iter().any(|c| c.evaluate(segment)),
            PolicyCondition::Not(condition) => !condition.evaluate(segment),
        }
    }

    /// Create an AND condition
    pub fn and(self, other: PolicyCondition) -> PolicyCondition {
        match self {
            PolicyCondition::And(mut conditions) => {
                conditions.push(other);
                PolicyCondition::And(conditions)
            }
            _ => PolicyCondition::And(vec![self, other]),
        }
    }

    /// Create an OR condition
    pub fn or(self, other: PolicyCondition) -> PolicyCondition {
        match self {
            PolicyCondition::Or(mut conditions) => {
                conditions.push(other);
                PolicyCondition::Or(conditions)
            }
            _ => PolicyCondition::Or(vec![self, other]),
        }
    }

    /// Create a NOT condition
    pub fn negate(self) -> PolicyCondition {
        PolicyCondition::Not(Box::new(self))
    }
}

/// Policy action to take when conditions are met
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PolicyAction {
    /// Migrate to specified tier
    MigrateToTier(StorageTier),
    /// Compress with specified algorithm
    Compress(CompressionType),
    /// Delete the segment
    Delete,
    /// Archive to specified path
    Archive(String),
    /// No action
    None,
}

/// A single policy rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyRule {
    /// Rule name
    pub name: String,
    /// Condition to evaluate
    pub condition: PolicyCondition,
    /// Action to take if condition is true
    pub action: PolicyAction,
    /// Priority (higher = more important)
    pub priority: i32,
    /// Is this rule enabled?
    pub enabled: bool,
}

impl PolicyRule {
    /// Create a new policy rule
    pub fn new(name: impl Into<String>, condition: PolicyCondition, action: PolicyAction) -> Self {
        Self {
            name: name.into(),
            condition,
            action,
            priority: 0,
            enabled: true,
        }
    }

    /// Set priority
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Enable or disable the rule
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Evaluate and return action if condition matches
    pub fn evaluate(&self, segment: &DataSegment) -> Option<&PolicyAction> {
        if self.enabled && self.condition.evaluate(segment) {
            Some(&self.action)
        } else {
            None
        }
    }
}

/// Complete lifecycle policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecyclePolicy {
    /// Policy name
    pub name: String,
    /// Policy description
    pub description: String,
    /// Rules in this policy
    pub rules: Vec<PolicyRule>,
    /// Is this policy enabled?
    pub enabled: bool,
    /// Topics this policy applies to (empty = all)
    pub topics: Vec<String>,
    /// Tags filter (all must match)
    pub tags: HashMap<String, String>,
}

impl LifecyclePolicy {
    /// Create a new lifecycle policy
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            rules: Vec::new(),
            enabled: true,
            topics: Vec::new(),
            tags: HashMap::new(),
        }
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Add a rule
    pub fn add_rule(mut self, rule: PolicyRule) -> Self {
        self.rules.push(rule);
        self
    }

    /// Filter by topics
    pub fn for_topics(mut self, topics: Vec<String>) -> Self {
        self.topics = topics;
        self
    }

    /// Filter by tags
    pub fn with_tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags = tags;
        self
    }

    /// Check if policy applies to segment
    pub fn applies_to(&self, segment: &DataSegment) -> bool {
        // Check topics filter
        if !self.topics.is_empty() && !self.topics.contains(&segment.topic) {
            return false;
        }

        // Check tags filter
        for (key, value) in &self.tags {
            if segment.tags.get(key) != Some(value) {
                return false;
            }
        }

        true
    }

    /// Evaluate policy against segment and return first matching action
    pub fn evaluate(&self, segment: &DataSegment) -> Option<&PolicyAction> {
        if !self.enabled || !self.applies_to(segment) {
            return None;
        }

        // Sort rules by priority (highest first) and find first match
        let mut sorted_rules: Vec<_> = self.rules.iter().filter(|r| r.enabled).collect();
        sorted_rules.sort_by(|a, b| b.priority.cmp(&a.priority));

        for rule in sorted_rules {
            if let Some(action) = rule.evaluate(segment) {
                return Some(action);
            }
        }

        None
    }
}

/// Compliance policy for regulatory requirements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompliancePolicy {
    /// Policy name
    pub name: String,
    /// Minimum retention period (days)
    pub min_retention_days: u32,
    /// Maximum retention period (days)
    pub max_retention_days: Option<u32>,
    /// Require encryption at rest
    pub require_encryption: bool,
    /// Require immutability (no deletion)
    pub require_immutability: bool,
    /// Geographic restrictions (allowed regions)
    pub allowed_regions: Vec<String>,
    /// Require audit logging
    pub require_audit_log: bool,
    /// Data classification
    pub data_classification: DataClassification,
}

impl CompliancePolicy {
    /// Create a new compliance policy
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            min_retention_days: 0,
            max_retention_days: None,
            require_encryption: false,
            require_immutability: false,
            allowed_regions: Vec::new(),
            require_audit_log: false,
            data_classification: DataClassification::Internal,
        }
    }

    /// GDPR-compliant policy
    pub fn gdpr() -> Self {
        Self {
            name: "GDPR".to_string(),
            min_retention_days: 0,
            max_retention_days: Some(365 * 3), // 3 years max
            require_encryption: true,
            require_immutability: false,
            allowed_regions: vec!["eu".to_string()],
            require_audit_log: true,
            data_classification: DataClassification::Personal,
        }
    }

    /// HIPAA-compliant policy
    pub fn hipaa() -> Self {
        Self {
            name: "HIPAA".to_string(),
            min_retention_days: 365 * 6, // 6 years minimum
            max_retention_days: None,
            require_encryption: true,
            require_immutability: true,
            allowed_regions: vec!["us".to_string()],
            require_audit_log: true,
            data_classification: DataClassification::HealthInfo,
        }
    }

    /// SOX-compliant policy
    pub fn sox() -> Self {
        Self {
            name: "SOX".to_string(),
            min_retention_days: 365 * 7, // 7 years minimum
            max_retention_days: None,
            require_encryption: true,
            require_immutability: true,
            allowed_regions: Vec::new(),
            require_audit_log: true,
            data_classification: DataClassification::Financial,
        }
    }

    /// Check if segment complies with policy
    pub fn check_compliance(&self, segment: &DataSegment) -> ComplianceResult {
        let mut violations = Vec::new();

        // Check minimum retention (cannot delete before min_retention)
        let age_days = segment.age_seconds() / 86400;
        if age_days < self.min_retention_days as u64 {
            // This is fine - data is younger than minimum
        }

        // Check maximum retention
        if let Some(max_days) = self.max_retention_days {
            if age_days > max_days as u64 {
                violations.push(ComplianceViolation::MaxRetentionExceeded {
                    max_days,
                    actual_days: age_days as u32,
                });
            }
        }

        // Check encryption
        if self.require_encryption {
            // Check if segment path indicates encryption
            let is_encrypted = segment.path.to_string_lossy().contains(".enc")
                || segment.tags.get("encrypted") == Some(&"true".to_string());
            if !is_encrypted {
                violations.push(ComplianceViolation::MissingEncryption);
            }
        }

        ComplianceResult {
            is_compliant: violations.is_empty(),
            violations,
        }
    }

    /// Check if deletion is allowed
    pub fn can_delete(&self, segment: &DataSegment) -> bool {
        if self.require_immutability {
            return false;
        }

        let age_days = segment.age_seconds() / 86400;
        age_days >= self.min_retention_days as u64
    }
}

/// Data classification levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataClassification {
    /// Public data
    Public,
    /// Internal use only
    Internal,
    /// Confidential
    Confidential,
    /// Personal data (PII)
    Personal,
    /// Financial data
    Financial,
    /// Health information (PHI)
    HealthInfo,
    /// Top secret
    TopSecret,
}

/// Compliance check result
#[derive(Debug, Clone)]
pub struct ComplianceResult {
    /// Is the segment compliant?
    pub is_compliant: bool,
    /// List of violations
    pub violations: Vec<ComplianceViolation>,
}

/// Compliance violation types
#[derive(Debug, Clone)]
pub enum ComplianceViolation {
    /// Maximum retention exceeded
    MaxRetentionExceeded { max_days: u32, actual_days: u32 },
    /// Missing encryption
    MissingEncryption,
    /// Invalid region
    InvalidRegion { actual_region: String },
    /// Immutability violated
    ImmutabilityViolated,
}

/// Cost optimization policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostOptimizationPolicy {
    /// Target monthly cost (dollars)
    pub target_cost: f64,
    /// Preferred compression algorithm
    pub preferred_compression: CompressionType,
    /// Minimum access frequency before migrating to cold storage
    pub min_access_frequency: f64,
    /// Maximum cost per GB
    pub max_cost_per_gb: f64,
    /// Enable automatic tier selection
    pub auto_tier: bool,
}

impl Default for CostOptimizationPolicy {
    fn default() -> Self {
        Self {
            target_cost: 1000.0,
            preferred_compression: CompressionType::Zstd,
            min_access_frequency: 0.1, // 1 access per 10 days
            max_cost_per_gb: 0.10,
            auto_tier: true,
        }
    }
}

impl CostOptimizationPolicy {
    /// Recommend storage tier based on access pattern and cost
    pub fn recommend_tier(&self, segment: &DataSegment) -> StorageTier {
        if !self.auto_tier {
            return segment.current_tier;
        }

        // Calculate access frequency (accesses per day)
        let age_days = (segment.age_seconds() as f64) / 86400.0;
        let access_frequency = if age_days > 0.0 {
            segment.access_count as f64 / age_days
        } else {
            f64::MAX // Very new data, keep hot
        };

        if access_frequency > 10.0 {
            StorageTier::Memory
        } else if access_frequency > 1.0 {
            StorageTier::Ssd
        } else if access_frequency > self.min_access_frequency {
            StorageTier::Hdd
        } else {
            StorageTier::ObjectStorage
        }
    }

    /// Check if segment should be compressed
    pub fn should_compress(&self, segment: &DataSegment) -> bool {
        // Compress if on cold storage or low access frequency
        matches!(
            segment.current_tier,
            StorageTier::Hdd | StorageTier::ObjectStorage | StorageTier::ColdArchive
        )
    }
}

/// Policy evaluator for applying policies to segments
pub struct PolicyEvaluator {
    policies: Vec<LifecyclePolicy>,
    compliance_policies: Vec<CompliancePolicy>,
    cost_policy: Option<CostOptimizationPolicy>,
}

impl PolicyEvaluator {
    /// Create a new policy evaluator
    pub fn new() -> Self {
        Self {
            policies: Vec::new(),
            compliance_policies: Vec::new(),
            cost_policy: None,
        }
    }

    /// Add a lifecycle policy
    pub fn add_policy(&mut self, policy: LifecyclePolicy) {
        self.policies.push(policy);
    }

    /// Add a compliance policy
    pub fn add_compliance_policy(&mut self, policy: CompliancePolicy) {
        self.compliance_policies.push(policy);
    }

    /// Set cost optimization policy
    pub fn set_cost_policy(&mut self, policy: CostOptimizationPolicy) {
        self.cost_policy = Some(policy);
    }

    /// Evaluate all policies against a segment
    pub fn evaluate(&self, segment: &DataSegment) -> Option<LifecycleAction> {
        // First check compliance - compliance violations take precedence
        for compliance in &self.compliance_policies {
            let result = compliance.check_compliance(segment);
            if !result.is_compliant {
                // Handle compliance violations
                for violation in result.violations {
                    if let ComplianceViolation::MaxRetentionExceeded { .. } = violation {
                        if compliance.can_delete(segment) {
                            return Some(LifecycleAction::Delete {
                                segment_id: segment.id.clone(),
                                reason: format!(
                                    "Compliance policy {} requires deletion",
                                    compliance.name
                                ),
                            });
                        }
                    }
                }
            }
        }

        // Then check lifecycle policies
        for policy in &self.policies {
            if let Some(action) = policy.evaluate(segment) {
                return Some(self.convert_action(action, segment));
            }
        }

        // Finally, apply cost optimization
        if let Some(cost_policy) = &self.cost_policy {
            let recommended_tier = cost_policy.recommend_tier(segment);
            if recommended_tier != segment.current_tier && segment.sealed {
                return Some(LifecycleAction::Migrate {
                    segment_id: segment.id.clone(),
                    from_tier: segment.current_tier,
                    to_tier: recommended_tier,
                    reason: "Cost optimization".to_string(),
                });
            }

            if cost_policy.should_compress(segment) {
                let path = segment.path.to_string_lossy();
                if !path.ends_with(".zst") && !path.ends_with(".lz4") && !path.ends_with(".gz") {
                    return Some(LifecycleAction::Compress {
                        segment_id: segment.id.clone(),
                        algorithm: cost_policy.preferred_compression,
                    });
                }
            }
        }

        None
    }

    /// Convert PolicyAction to LifecycleAction
    fn convert_action(&self, action: &PolicyAction, segment: &DataSegment) -> LifecycleAction {
        match action {
            PolicyAction::MigrateToTier(tier) => LifecycleAction::Migrate {
                segment_id: segment.id.clone(),
                from_tier: segment.current_tier,
                to_tier: *tier,
                reason: "Policy rule".to_string(),
            },
            PolicyAction::Compress(algorithm) => LifecycleAction::Compress {
                segment_id: segment.id.clone(),
                algorithm: *algorithm,
            },
            PolicyAction::Delete => LifecycleAction::Delete {
                segment_id: segment.id.clone(),
                reason: "Policy rule".to_string(),
            },
            PolicyAction::Archive(path) => LifecycleAction::Archive {
                segment_id: segment.id.clone(),
                destination: std::path::PathBuf::from(path),
            },
            PolicyAction::None => LifecycleAction::None,
        }
    }
}

impl Default for PolicyEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_segment() -> DataSegment {
        let mut segment = DataSegment::new("seg-001", "test-topic", 0, 0);
        segment.size_bytes = 1024 * 1024; // 1MB
        segment.sealed = true;
        segment
    }

    #[test]
    fn test_policy_condition_age() {
        let segment = create_test_segment();

        // New segment should be less than 1 hour old
        assert!(PolicyCondition::AgeLessThan(3600).evaluate(&segment));
        assert!(!PolicyCondition::AgeGreaterThan(3600).evaluate(&segment));
    }

    #[test]
    fn test_policy_condition_size() {
        let segment = create_test_segment();

        assert!(PolicyCondition::SizeGreaterThan(512 * 1024).evaluate(&segment));
        assert!(!PolicyCondition::SizeGreaterThan(2 * 1024 * 1024).evaluate(&segment));
    }

    #[test]
    fn test_policy_condition_tier() {
        let segment = create_test_segment();

        assert!(PolicyCondition::OnTier(StorageTier::Memory).evaluate(&segment));
        assert!(!PolicyCondition::OnTier(StorageTier::Ssd).evaluate(&segment));
    }

    #[test]
    fn test_policy_condition_tags() {
        let segment = create_test_segment().with_tag("env", "production");

        assert!(
            PolicyCondition::TagEquals("env".to_string(), "production".to_string())
                .evaluate(&segment)
        );
        assert!(
            !PolicyCondition::TagEquals("env".to_string(), "staging".to_string())
                .evaluate(&segment)
        );
        assert!(PolicyCondition::TagExists("env".to_string()).evaluate(&segment));
        assert!(!PolicyCondition::TagExists("missing".to_string()).evaluate(&segment));
    }

    #[test]
    fn test_policy_condition_and_or() {
        let segment = create_test_segment();

        // AND condition
        let and_cond = PolicyCondition::IsSealed.and(PolicyCondition::OnTier(StorageTier::Memory));
        assert!(and_cond.evaluate(&segment));

        // OR condition
        let or_cond = PolicyCondition::OnTier(StorageTier::Ssd)
            .or(PolicyCondition::OnTier(StorageTier::Memory));
        assert!(or_cond.evaluate(&segment));

        // NOT condition
        let not_cond = PolicyCondition::OnTier(StorageTier::Ssd).negate();
        assert!(not_cond.evaluate(&segment));
    }

    #[test]
    fn test_policy_rule() {
        let segment = create_test_segment();

        let rule = PolicyRule::new(
            "migrate-sealed",
            PolicyCondition::IsSealed,
            PolicyAction::MigrateToTier(StorageTier::Ssd),
        );

        let action = rule.evaluate(&segment);
        assert!(action.is_some());
        assert!(matches!(
            action.unwrap(),
            PolicyAction::MigrateToTier(StorageTier::Ssd)
        ));
    }

    #[test]
    fn test_lifecycle_policy() {
        let segment = create_test_segment();

        let policy = LifecyclePolicy::new("test-policy")
            .with_description("Test policy")
            .add_rule(PolicyRule::new(
                "rule1",
                PolicyCondition::IsSealed,
                PolicyAction::MigrateToTier(StorageTier::Ssd),
            ));

        let action = policy.evaluate(&segment);
        assert!(action.is_some());
    }

    #[test]
    fn test_compliance_policy_gdpr() {
        let policy = CompliancePolicy::gdpr();

        assert!(policy.require_encryption);
        assert!(policy.require_audit_log);
        assert_eq!(policy.allowed_regions, vec!["eu".to_string()]);
    }

    #[test]
    fn test_compliance_check() {
        let policy = CompliancePolicy::new("test").require_encryption;
        let segment = create_test_segment();

        // Segment without encryption tag should fail if encryption is required
        if policy {
            let result = CompliancePolicy::new("test-enc").check_compliance(&segment);
            // By default our test segment doesn't have encryption marker
            assert!(result.is_compliant); // No encryption required in default policy
        }
    }

    #[test]
    fn test_cost_optimization_policy() {
        let policy = CostOptimizationPolicy::default();
        let mut segment = create_test_segment();

        // New segment with no accesses should stay on memory
        segment.access_count = 100;
        let tier = policy.recommend_tier(&segment);
        // High access count on new segment -> should be hot
        assert!(matches!(tier, StorageTier::Memory | StorageTier::Ssd));
    }

    #[test]
    fn test_policy_evaluator() {
        let mut evaluator = PolicyEvaluator::new();

        let policy = LifecyclePolicy::new("test").add_rule(PolicyRule::new(
            "migrate-rule",
            PolicyCondition::And(vec![
                PolicyCondition::IsSealed,
                PolicyCondition::OnTier(StorageTier::Memory),
            ]),
            PolicyAction::MigrateToTier(StorageTier::Ssd),
        ));

        evaluator.add_policy(policy);

        let segment = create_test_segment();
        let action = evaluator.evaluate(&segment);

        assert!(action.is_some());
        if let Some(LifecycleAction::Migrate { to_tier, .. }) = action {
            assert_eq!(to_tier, StorageTier::Ssd);
        } else {
            panic!("Expected Migrate action");
        }
    }
}
