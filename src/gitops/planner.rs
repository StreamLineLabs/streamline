//! GitOps Execution Planner
//!
//! Provides a "plan" mode for the GitOps system, showing what would change
//! before applying — similar to `terraform plan`.

use super::config::{AclSpec, ConfigManifest, TopicSpec};
use super::reconciler::{DriftResult, DriftType};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Type of resource being managed
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceType {
    Topic,
    Acl,
    Schema,
    Sink,
    Config,
}

impl fmt::Display for ResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResourceType::Topic => write!(f, "topic"),
            ResourceType::Acl => write!(f, "acl"),
            ResourceType::Schema => write!(f, "schema"),
            ResourceType::Sink => write!(f, "sink"),
            ResourceType::Config => write!(f, "config"),
        }
    }
}

/// Action to be taken on a resource
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeAction {
    Create,
    Update,
    Delete,
    NoChange,
}

impl fmt::Display for ChangeAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChangeAction::Create => write!(f, "CREATE"),
            ChangeAction::Update => write!(f, "UPDATE"),
            ChangeAction::Delete => write!(f, "DELETE"),
            ChangeAction::NoChange => write!(f, "NO CHANGE"),
        }
    }
}

/// Impact level of a change
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeImpact {
    Breaking,
    NonBreaking,
    Cosmetic,
}

/// Difference in a single field between current and desired state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDiff {
    /// Name of the field that differs
    pub field_name: String,
    /// Current value (None if resource is being created)
    pub current_value: Option<String>,
    /// Desired value (None if resource is being deleted)
    pub desired_value: Option<String>,
}

/// A single planned change to a resource
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedChange {
    /// Type of the resource
    pub resource_type: ResourceType,
    /// Name of the resource
    pub resource_name: String,
    /// Action to perform
    pub action: ChangeAction,
    /// Field-level differences
    pub details: Vec<FieldDiff>,
    /// Impact assessment
    pub impact: ChangeImpact,
}

/// Summary statistics for a plan
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlanSummary {
    pub total_changes: usize,
    pub creates: usize,
    pub updates: usize,
    pub deletes: usize,
    pub no_changes: usize,
    pub has_breaking_changes: bool,
}

/// Complete execution plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPlan {
    /// Unique identifier
    pub id: String,
    /// Creation timestamp (unix millis)
    pub created_at: i64,
    /// Name of the manifest this plan was generated from
    pub manifest_name: String,
    /// Ordered list of planned changes
    pub changes: Vec<PlannedChange>,
    /// Aggregate summary
    pub summary: PlanSummary,
    /// Warnings for the operator
    pub warnings: Vec<String>,
}

/// Current state of a topic for plan comparison
#[derive(Debug, Clone)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: u32,
    pub replication_factor: u16,
    pub config: HashMap<String, String>,
}

/// Current state of an ACL for plan comparison
#[derive(Debug, Clone)]
pub struct AclInfo {
    pub principal: String,
    pub resource_type: String,
    pub resource_name: String,
    pub operation: String,
    pub permission: String,
}

/// Generates execution plans by comparing desired and current state
pub struct GitOpsPlanner;

impl GitOpsPlanner {
    pub fn new() -> Self {
        Self
    }

    /// Build an execution plan from a manifest and the current cluster state.
    pub fn plan(
        &self,
        manifest: &ConfigManifest,
        current_topics: &[TopicInfo],
        current_acls: &[AclInfo],
    ) -> Result<ExecutionPlan> {
        let manifest_name = format!("{}/{}", manifest.metadata.namespace, manifest.metadata.name);
        let mut changes = Vec::new();
        let mut warnings = Vec::new();

        let topic_map: HashMap<&str, &TopicInfo> = current_topics
            .iter()
            .map(|t| (t.name.as_str(), t))
            .collect();

        if let Some(spec) = &manifest.spec {
            // --- Topics ---
            for desired in &spec.topics {
                if let Some(current) = topic_map.get(desired.name.as_str()) {
                    let (diffs, impact) = Self::diff_topic(desired, current);
                    if diffs.is_empty() {
                        changes.push(PlannedChange {
                            resource_type: ResourceType::Topic,
                            resource_name: desired.name.clone(),
                            action: ChangeAction::NoChange,
                            details: vec![],
                            impact: ChangeImpact::Cosmetic,
                        });
                    } else {
                        if impact == ChangeImpact::Breaking {
                            warnings.push(format!(
                                "Breaking change on topic \"{}\": partition count change cannot be reversed",
                                desired.name
                            ));
                        }
                        changes.push(PlannedChange {
                            resource_type: ResourceType::Topic,
                            resource_name: desired.name.clone(),
                            action: ChangeAction::Update,
                            details: diffs,
                            impact,
                        });
                    }
                } else {
                    changes.push(Self::plan_topic_create(desired));
                }
            }

            // Topics in cluster but not in manifest -> Delete
            let desired_names: std::collections::HashSet<&str> =
                spec.topics.iter().map(|t| t.name.as_str()).collect();
            for current in current_topics {
                if !desired_names.contains(current.name.as_str()) {
                    changes.push(PlannedChange {
                        resource_type: ResourceType::Topic,
                        resource_name: current.name.clone(),
                        action: ChangeAction::Delete,
                        details: vec![],
                        impact: ChangeImpact::Breaking,
                    });
                    warnings.push(format!(
                        "Topic \"{}\" will be deleted — all data will be lost",
                        current.name
                    ));
                }
            }

            // --- ACLs ---
            let acl_key = |a: &AclSpec| {
                format!(
                    "{}:{}:{}:{}",
                    a.principal, a.resource_type, a.resource_name, a.operation
                )
            };
            let acl_info_key = |a: &AclInfo| {
                format!(
                    "{}:{}:{}:{}",
                    a.principal, a.resource_type, a.resource_name, a.operation
                )
            };

            let current_acl_map: HashMap<String, &AclInfo> =
                current_acls.iter().map(|a| (acl_info_key(a), a)).collect();

            for desired in &spec.acls {
                let key = acl_key(desired);
                if current_acl_map.contains_key(&key) {
                    changes.push(PlannedChange {
                        resource_type: ResourceType::Acl,
                        resource_name: key,
                        action: ChangeAction::NoChange,
                        details: vec![],
                        impact: ChangeImpact::Cosmetic,
                    });
                } else {
                    changes.push(PlannedChange {
                        resource_type: ResourceType::Acl,
                        resource_name: key,
                        action: ChangeAction::Create,
                        details: vec![FieldDiff {
                            field_name: "permission".to_string(),
                            current_value: None,
                            desired_value: Some(format!(
                                "{} {} on {}:{}",
                                desired.permission,
                                desired.operation,
                                desired.resource_type,
                                desired.resource_name
                            )),
                        }],
                        impact: ChangeImpact::NonBreaking,
                    });
                }
            }
        }

        let summary = Self::build_summary(&changes);

        Ok(ExecutionPlan {
            id: Self::generate_id(),
            created_at: chrono::Utc::now().timestamp_millis(),
            manifest_name,
            changes,
            summary,
            warnings,
        })
    }

    /// Build an execution plan directly from drift detection results.
    pub fn plan_from_drift(drifts: &[DriftResult]) -> ExecutionPlan {
        let mut changes = Vec::new();
        let mut warnings = Vec::new();

        for drift in drifts {
            let resource_type = match drift.resource_type.as_str() {
                "Topic" => ResourceType::Topic,
                "ACL" => ResourceType::Acl,
                "Schema" => ResourceType::Schema,
                "Sink" => ResourceType::Sink,
                _ => ResourceType::Config,
            };

            let (action, impact) = match drift.drift_type {
                DriftType::Missing => (ChangeAction::Create, ChangeImpact::NonBreaking),
                DriftType::Modified => {
                    let impact = if drift.details.contains("partition") {
                        ChangeImpact::Breaking
                    } else {
                        ChangeImpact::NonBreaking
                    };
                    (ChangeAction::Update, impact)
                }
                DriftType::Extra => (ChangeAction::Delete, ChangeImpact::Breaking),
            };

            if impact == ChangeImpact::Breaking {
                warnings.push(format!(
                    "Breaking change on {} \"{}\"",
                    resource_type, drift.resource_name
                ));
            }

            let details = vec![FieldDiff {
                field_name: drift.details.clone(),
                current_value: drift.actual.clone(),
                desired_value: drift.expected.clone(),
            }];

            changes.push(PlannedChange {
                resource_type,
                resource_name: drift.resource_name.clone(),
                action,
                details,
                impact,
            });
        }

        let summary = Self::build_summary(&changes);

        ExecutionPlan {
            id: Self::generate_id(),
            created_at: chrono::Utc::now().timestamp_millis(),
            manifest_name: String::new(),
            changes,
            summary,
            warnings,
        }
    }

    // --- private helpers ---

    fn diff_topic(desired: &TopicSpec, current: &TopicInfo) -> (Vec<FieldDiff>, ChangeImpact) {
        let mut diffs = Vec::new();
        let mut impact = ChangeImpact::Cosmetic;

        if desired.partitions != current.partitions {
            diffs.push(FieldDiff {
                field_name: "partitions".to_string(),
                current_value: Some(current.partitions.to_string()),
                desired_value: Some(desired.partitions.to_string()),
            });
            // Partition count changes are irreversible
            impact = ChangeImpact::Breaking;
        }

        if desired.replication_factor != current.replication_factor {
            diffs.push(FieldDiff {
                field_name: "replication_factor".to_string(),
                current_value: Some(current.replication_factor.to_string()),
                desired_value: Some(desired.replication_factor.to_string()),
            });
            if impact != ChangeImpact::Breaking {
                impact = ChangeImpact::NonBreaking;
            }
        }

        // Config key diff
        for (key, desired_val) in &desired.config {
            match current.config.get(key) {
                Some(cur_val) if cur_val != desired_val => {
                    diffs.push(FieldDiff {
                        field_name: format!("config.{}", key),
                        current_value: Some(cur_val.clone()),
                        desired_value: Some(desired_val.clone()),
                    });
                    if impact == ChangeImpact::Cosmetic {
                        impact = ChangeImpact::NonBreaking;
                    }
                }
                None => {
                    diffs.push(FieldDiff {
                        field_name: format!("config.{}", key),
                        current_value: None,
                        desired_value: Some(desired_val.clone()),
                    });
                    if impact == ChangeImpact::Cosmetic {
                        impact = ChangeImpact::NonBreaking;
                    }
                }
                _ => {}
            }
        }

        (diffs, impact)
    }

    fn plan_topic_create(desired: &TopicSpec) -> PlannedChange {
        let mut details = vec![
            FieldDiff {
                field_name: "partitions".to_string(),
                current_value: None,
                desired_value: Some(desired.partitions.to_string()),
            },
            FieldDiff {
                field_name: "replication_factor".to_string(),
                current_value: None,
                desired_value: Some(desired.replication_factor.to_string()),
            },
        ];

        for (key, val) in &desired.config {
            details.push(FieldDiff {
                field_name: format!("config.{}", key),
                current_value: None,
                desired_value: Some(val.clone()),
            });
        }

        PlannedChange {
            resource_type: ResourceType::Topic,
            resource_name: desired.name.clone(),
            action: ChangeAction::Create,
            details,
            impact: ChangeImpact::NonBreaking,
        }
    }

    fn build_summary(changes: &[PlannedChange]) -> PlanSummary {
        let mut summary = PlanSummary::default();
        for c in changes {
            match c.action {
                ChangeAction::Create => summary.creates += 1,
                ChangeAction::Update => summary.updates += 1,
                ChangeAction::Delete => summary.deletes += 1,
                ChangeAction::NoChange => summary.no_changes += 1,
            }
            if c.impact == ChangeImpact::Breaking {
                summary.has_breaking_changes = true;
            }
        }
        summary.total_changes = summary.creates + summary.updates + summary.deletes;
        summary
    }

    fn generate_id() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("plan-{:016x}", nanos)
    }
}

impl Default for GitOpsPlanner {
    fn default() -> Self {
        Self::new()
    }
}

/// Formats an [`ExecutionPlan`] for display
pub struct PlanFormatter;

impl PlanFormatter {
    /// Terraform-style text output
    pub fn format_text(plan: &ExecutionPlan) -> String {
        let mut out = String::new();

        out.push_str("Streamline GitOps Plan\n");
        out.push_str("======================\n\n");

        for change in &plan.changes {
            let symbol = match change.action {
                ChangeAction::Create => "+",
                ChangeAction::Update => "~",
                ChangeAction::Delete => "-",
                ChangeAction::NoChange => "=",
            };

            let detail = match change.action {
                ChangeAction::Create => {
                    let parts: Vec<String> = change
                        .details
                        .iter()
                        .filter_map(|d| {
                            d.desired_value
                                .as_ref()
                                .map(|v| format!("{}={}", d.field_name, v))
                        })
                        .collect();
                    if parts.is_empty() {
                        String::new()
                    } else {
                        parts.join(", ")
                    }
                }
                ChangeAction::Update => {
                    let parts: Vec<String> = change
                        .details
                        .iter()
                        .map(|d| {
                            format!(
                                "{}: {} -> {}",
                                d.field_name,
                                d.current_value.as_deref().unwrap_or("(none)"),
                                d.desired_value.as_deref().unwrap_or("(none)")
                            )
                        })
                        .collect();
                    parts.join(", ")
                }
                ChangeAction::Delete | ChangeAction::NoChange => String::new(),
            };

            let action_label = format!("({})", change.action);
            let line = if detail.is_empty() {
                format!(
                    "  {} {} {:20} {}\n",
                    symbol,
                    change.resource_type,
                    format!("\"{}\"", change.resource_name),
                    action_label,
                )
            } else {
                format!(
                    "  {} {} {:20} {} {}\n",
                    symbol,
                    change.resource_type,
                    format!("\"{}\"", change.resource_name),
                    action_label,
                    detail,
                )
            };
            out.push_str(&line);
        }

        out.push('\n');
        out.push_str(&Self::format_summary(plan));

        for warning in &plan.warnings {
            out.push_str(&format!("\u{26a0} WARNING: {}\n", warning));
        }

        out
    }

    /// JSON representation
    pub fn format_json(plan: &ExecutionPlan) -> String {
        serde_json::to_string_pretty(plan).unwrap_or_else(|_| "{}".to_string())
    }

    /// One-line summary
    pub fn format_summary(plan: &ExecutionPlan) -> String {
        format!(
            "Summary: {} to create, {} to update, {} to delete, {} unchanged\n",
            plan.summary.creates,
            plan.summary.updates,
            plan.summary.deletes,
            plan.summary.no_changes,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gitops::config::{
        AclSpec, ManifestKind, ManifestMetadata, StreamlineConfigSpec, TopicSpec,
    };

    fn sample_manifest() -> ConfigManifest {
        ConfigManifest {
            api_version: "streamline.io/v1".to_string(),
            kind: ManifestKind::StreamlineConfig,
            metadata: ManifestMetadata {
                name: "prod".to_string(),
                namespace: "default".to_string(),
                ..Default::default()
            },
            spec: Some(StreamlineConfigSpec {
                topics: vec![
                    TopicSpec {
                        name: "events".to_string(),
                        partitions: 3,
                        replication_factor: 1,
                        config: {
                            let mut m = HashMap::new();
                            m.insert("retention.ms".to_string(), "604800000".to_string());
                            m
                        },
                        ..Default::default()
                    },
                    TopicSpec {
                        name: "users".to_string(),
                        partitions: 6,
                        replication_factor: 1,
                        config: {
                            let mut m = HashMap::new();
                            m.insert("retention.ms".to_string(), "2592000000".to_string());
                            m
                        },
                        ..Default::default()
                    },
                ],
                acls: vec![AclSpec {
                    principal: "User:app".to_string(),
                    resource_type: "topic".to_string(),
                    resource_name: "events".to_string(),
                    operation: "write".to_string(),
                    permission: "allow".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
        }
    }

    fn current_topics() -> Vec<TopicInfo> {
        vec![
            TopicInfo {
                name: "users".to_string(),
                partitions: 3,
                replication_factor: 1,
                config: {
                    let mut m = HashMap::new();
                    m.insert("retention.ms".to_string(), "604800000".to_string());
                    m
                },
            },
            TopicInfo {
                name: "temp-logs".to_string(),
                partitions: 1,
                replication_factor: 1,
                config: HashMap::new(),
            },
        ]
    }

    #[test]
    fn test_plan_create_update_delete() {
        let planner = GitOpsPlanner::new();
        let plan = planner
            .plan(&sample_manifest(), &current_topics(), &[])
            .unwrap();

        // "events" is new -> Create
        assert!(plan
            .changes
            .iter()
            .any(|c| c.resource_name == "events" && c.action == ChangeAction::Create));
        // "users" exists with different partitions -> Update
        assert!(plan
            .changes
            .iter()
            .any(|c| c.resource_name == "users" && c.action == ChangeAction::Update));
        // "temp-logs" not in manifest -> Delete
        assert!(plan
            .changes
            .iter()
            .any(|c| c.resource_name == "temp-logs" && c.action == ChangeAction::Delete));

        assert_eq!(plan.summary.creates, 2); // events topic + ACL
        assert_eq!(plan.summary.updates, 1);
        assert_eq!(plan.summary.deletes, 1);
        assert!(plan.summary.has_breaking_changes);
    }

    #[test]
    fn test_plan_no_change() {
        let manifest = ConfigManifest {
            api_version: "streamline.io/v1".to_string(),
            kind: ManifestKind::StreamlineConfig,
            metadata: ManifestMetadata {
                name: "stable".to_string(),
                namespace: "default".to_string(),
                ..Default::default()
            },
            spec: Some(StreamlineConfigSpec {
                topics: vec![TopicSpec {
                    name: "orders".to_string(),
                    partitions: 3,
                    replication_factor: 1,
                    ..Default::default()
                }],
                ..Default::default()
            }),
        };

        let current = vec![TopicInfo {
            name: "orders".to_string(),
            partitions: 3,
            replication_factor: 1,
            config: HashMap::new(),
        }];

        let planner = GitOpsPlanner::new();
        let plan = planner.plan(&manifest, &current, &[]).unwrap();

        assert_eq!(plan.summary.total_changes, 0);
        assert_eq!(plan.summary.no_changes, 1);
        assert!(!plan.summary.has_breaking_changes);
    }

    #[test]
    fn test_plan_from_drift() {
        let drifts = vec![
            DriftResult {
                resource_type: "Topic".to_string(),
                resource_name: "events".to_string(),
                drift_type: DriftType::Missing,
                details: "Topic does not exist".to_string(),
                expected: Some("partitions=3".to_string()),
                actual: None,
            },
            DriftResult {
                resource_type: "Topic".to_string(),
                resource_name: "users".to_string(),
                drift_type: DriftType::Modified,
                details: "Partition count differs".to_string(),
                expected: Some("6".to_string()),
                actual: Some("3".to_string()),
            },
        ];

        let plan = GitOpsPlanner::plan_from_drift(&drifts);

        assert_eq!(plan.changes.len(), 2);
        assert_eq!(plan.summary.creates, 1);
        assert_eq!(plan.summary.updates, 1);
    }

    #[test]
    fn test_format_text_output() {
        let planner = GitOpsPlanner::new();
        let plan = planner
            .plan(&sample_manifest(), &current_topics(), &[])
            .unwrap();

        let text = PlanFormatter::format_text(&plan);

        assert!(text.contains("Streamline GitOps Plan"));
        assert!(text.contains("Summary:"));
        assert!(text.contains("+ topic"));
        assert!(text.contains("~ topic"));
        assert!(text.contains("- topic"));
        assert!(text.contains("WARNING"));
    }
}
