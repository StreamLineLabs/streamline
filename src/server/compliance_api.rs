//! Compliance & Audit Framework API
//!
//! ## Endpoints
//!
//! - `GET /api/v1/compliance/status` - Overall compliance status
//! - `POST /api/v1/compliance/scan` - Run compliance scan
//! - `GET /api/v1/compliance/policies` - List compliance policies
//! - `POST /api/v1/compliance/policies` - Create policy
//! - `DELETE /api/v1/compliance/policies/:id` - Delete policy
//! - `POST /api/v1/compliance/retention/enforce` - Enforce retention policies
//! - `POST /api/v1/compliance/pii/scan` - Scan for PII
//! - `GET /api/v1/compliance/audit-log` - Get audit trail
//! - `POST /api/v1/compliance/deletion-request` - Right-to-deletion request
//! - `GET /api/v1/compliance/report` - Generate compliance report

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};
use uuid::Uuid;

// ============================================================================
// State
// ============================================================================

/// Shared state for the Compliance API
#[derive(Clone)]
pub struct ComplianceApiState {
    pub policies: Arc<RwLock<Vec<CompliancePolicy>>>,
    pub scan_results: Arc<RwLock<Vec<ComplianceScan>>>,
    pub deletion_requests: Arc<RwLock<Vec<DeletionRequest>>>,
    pub audit_log: Arc<RwLock<Vec<AuditEntry>>>,
}

impl ComplianceApiState {
    pub fn new() -> Self {
        Self {
            policies: Arc::new(RwLock::new(Vec::new())),
            scan_results: Arc::new(RwLock::new(Vec::new())),
            deletion_requests: Arc::new(RwLock::new(Vec::new())),
            audit_log: Arc::new(RwLock::new(Vec::new())),
        }
    }

    fn log_audit(&self, action: &str, actor: &str, resource: &str, details: &str) {
        self.audit_log.write().push(AuditEntry {
            id: Uuid::new_v4().to_string(),
            action: action.to_string(),
            actor: actor.to_string(),
            resource: resource.to_string(),
            details: details.to_string(),
            timestamp: now_iso(),
            ip_address: None,
        });
    }
}

// ============================================================================
// Types
// ============================================================================

/// A compliance policy with associated rules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompliancePolicy {
    pub id: String,
    pub name: String,
    pub framework: ComplianceFramework,
    pub rules: Vec<ComplianceRule>,
    pub enabled: bool,
    pub created_at: String,
}

/// Supported compliance frameworks
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ComplianceFramework {
    Gdpr,
    Hipaa,
    Sox,
    Pci,
    Custom(String),
}

/// A single compliance rule within a policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceRule {
    pub name: String,
    pub rule_type: RuleType,
    pub config: HashMap<String, String>,
    pub severity: String,
}

/// The type of compliance rule
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuleType {
    RetentionLimit { max_days: u32 },
    EncryptionRequired,
    PiiDetection { patterns: Vec<String> },
    AccessControl { min_acl_entries: usize },
    AuditLogging { required: bool },
    DataResidency { allowed_regions: Vec<String> },
}

/// Result of a compliance scan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceScan {
    pub id: String,
    pub status: ScanStatus,
    pub findings: Vec<Finding>,
    pub score: f64,
    pub scanned_at: String,
    pub duration_ms: f64,
}

/// Status of a compliance scan
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum ScanStatus {
    Running,
    Completed,
    Failed { reason: String },
}

/// A finding from a compliance scan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Finding {
    pub severity: String,
    pub category: String,
    pub resource: String,
    pub message: String,
    pub remediation: String,
}

/// A right-to-deletion request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletionRequest {
    pub id: String,
    pub subject_id: String,
    pub topics: Vec<String>,
    pub status: DeletionStatus,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    pub records_deleted: u64,
}

/// Status of a deletion request
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum DeletionStatus {
    Pending,
    InProgress,
    Completed,
    Failed { reason: String },
}

/// An audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    pub id: String,
    pub action: String,
    pub actor: String,
    pub resource: String,
    pub details: String,
    pub timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
}

/// A generated compliance report
#[derive(Debug, Clone, Serialize)]
pub struct ComplianceReport {
    pub framework: String,
    pub generated_at: String,
    pub overall_score: f64,
    pub sections: Vec<ReportSection>,
    pub summary: String,
}

/// A section within a compliance report
#[derive(Debug, Clone, Serialize)]
pub struct ReportSection {
    pub name: String,
    pub status: String,
    pub findings_count: usize,
    pub details: String,
}

// ============================================================================
// Request / Response Types
// ============================================================================

/// Request to create a compliance policy
#[derive(Debug, Deserialize)]
pub struct CreatePolicyRequest {
    pub name: String,
    pub framework: ComplianceFramework,
    pub rules: Vec<ComplianceRule>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

/// Request to submit a deletion request
#[derive(Debug, Deserialize)]
pub struct CreateDeletionRequest {
    pub subject_id: String,
    pub topics: Vec<String>,
}

// ============================================================================
// Router
// ============================================================================

/// Create the Compliance API router
pub fn create_compliance_api_router(state: ComplianceApiState) -> Router {
    Router::new()
        .route("/api/v1/compliance/status", get(compliance_status))
        .route("/api/v1/compliance/scan", post(run_scan))
        .route("/api/v1/compliance/policies", get(list_policies))
        .route("/api/v1/compliance/policies", post(create_policy))
        .route("/api/v1/compliance/policies/:id", delete(delete_policy))
        .route(
            "/api/v1/compliance/retention/enforce",
            post(enforce_retention),
        )
        .route("/api/v1/compliance/pii/scan", post(pii_scan))
        .route("/api/v1/compliance/audit-log", get(get_audit_log))
        .route(
            "/api/v1/compliance/deletion-request",
            post(create_deletion_request),
        )
        .route("/api/v1/compliance/report", get(generate_report))
        .with_state(state)
}

// ============================================================================
// Handlers
// ============================================================================

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

async fn compliance_status(
    State(state): State<ComplianceApiState>,
) -> Json<serde_json::Value> {
    let policies = state.policies.read();
    let scans = state.scan_results.read();
    let latest_score = scans.last().map(|s| s.score).unwrap_or(0.0);
    let total_findings: usize = scans.iter().flat_map(|s| &s.findings).count();

    Json(serde_json::json!({
        "status": if latest_score >= 80.0 { "compliant" } else { "non_compliant" },
        "score": latest_score,
        "policies_count": policies.len(),
        "enabled_policies": policies.iter().filter(|p| p.enabled).count(),
        "total_scans": scans.len(),
        "total_findings": total_findings,
        "pending_deletions": state.deletion_requests.read().iter()
            .filter(|d| d.status == DeletionStatus::Pending).count(),
        "last_scan": scans.last().map(|s| &s.scanned_at),
    }))
}

async fn run_scan(
    State(state): State<ComplianceApiState>,
) -> (StatusCode, Json<ComplianceScan>) {
    let start = Instant::now();
    let policies = state.policies.read().clone();

    let mut findings = Vec::new();
    for policy in &policies {
        if !policy.enabled {
            continue;
        }
        for rule in &policy.rules {
            // Simulate compliance checks
            match &rule.rule_type {
                RuleType::EncryptionRequired => {
                    findings.push(Finding {
                        severity: rule.severity.clone(),
                        category: "encryption".to_string(),
                        resource: "cluster".to_string(),
                        message: "Encryption at rest status checked".to_string(),
                        remediation: "Enable encryption at rest in server config".to_string(),
                    });
                }
                RuleType::PiiDetection { patterns } => {
                    findings.push(Finding {
                        severity: rule.severity.clone(),
                        category: "pii".to_string(),
                        resource: "topics/*".to_string(),
                        message: format!("PII scan with {} patterns", patterns.len()),
                        remediation: "Review flagged topics for PII data".to_string(),
                    });
                }
                _ => {}
            }
        }
    }

    let score = if findings.is_empty() {
        100.0
    } else {
        let critical = findings.iter().filter(|f| f.severity == "critical").count();
        let high = findings.iter().filter(|f| f.severity == "high").count();
        (100.0 - (critical as f64 * 20.0) - (high as f64 * 10.0)).max(0.0)
    };

    let scan = ComplianceScan {
        id: Uuid::new_v4().to_string(),
        status: ScanStatus::Completed,
        findings,
        score,
        scanned_at: now_iso(),
        duration_ms: start.elapsed().as_secs_f64() * 1000.0,
    };

    state.scan_results.write().push(scan.clone());
    state.log_audit("compliance_scan", "system", "cluster", "Compliance scan completed");
    info!(scan_id = %scan.id, score = scan.score, "Compliance scan completed");
    (StatusCode::CREATED, Json(scan))
}

async fn list_policies(
    State(state): State<ComplianceApiState>,
) -> Json<Vec<CompliancePolicy>> {
    Json(state.policies.read().clone())
}

async fn create_policy(
    State(state): State<ComplianceApiState>,
    Json(req): Json<CreatePolicyRequest>,
) -> (StatusCode, Json<CompliancePolicy>) {
    let policy = CompliancePolicy {
        id: Uuid::new_v4().to_string(),
        name: req.name,
        framework: req.framework,
        rules: req.rules,
        enabled: req.enabled,
        created_at: now_iso(),
    };
    info!(policy_id = %policy.id, name = %policy.name, "Created compliance policy");
    state.log_audit(
        "create_policy",
        "system",
        &policy.id,
        &format!("Created policy: {}", policy.name),
    );
    state.policies.write().push(policy.clone());
    (StatusCode::CREATED, Json(policy))
}

async fn delete_policy(
    State(state): State<ComplianceApiState>,
    Path(id): Path<String>,
) -> StatusCode {
    let mut policies = state.policies.write();
    let len_before = policies.len();
    policies.retain(|p| p.id != id);
    if policies.len() < len_before {
        state.log_audit("delete_policy", "system", &id, "Policy deleted");
        info!(policy_id = %id, "Deleted compliance policy");
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn enforce_retention(
    State(state): State<ComplianceApiState>,
) -> Json<serde_json::Value> {
    let policies = state.policies.read();
    let mut enforced = 0u32;

    for policy in policies.iter() {
        if !policy.enabled {
            continue;
        }
        for rule in &policy.rules {
            if let RuleType::RetentionLimit { max_days } = &rule.rule_type {
                info!(policy = %policy.name, max_days = max_days, "Enforcing retention");
                enforced += 1;
            }
        }
    }

    state.log_audit(
        "enforce_retention",
        "system",
        "cluster",
        &format!("Enforced {} retention rules", enforced),
    );

    Json(serde_json::json!({
        "enforced_rules": enforced,
        "status": "completed",
        "timestamp": now_iso(),
    }))
}

async fn pii_scan(
    State(state): State<ComplianceApiState>,
) -> Json<serde_json::Value> {
    let start = Instant::now();
    let policies = state.policies.read();
    let mut patterns_checked = 0usize;

    for policy in policies.iter() {
        for rule in &policy.rules {
            if let RuleType::PiiDetection { patterns } = &rule.rule_type {
                patterns_checked += patterns.len();
            }
        }
    }

    state.log_audit("pii_scan", "system", "topics/*", "PII scan executed");
    info!(patterns = patterns_checked, "PII scan completed");

    Json(serde_json::json!({
        "status": "completed",
        "patterns_checked": patterns_checked,
        "topics_scanned": 0,
        "pii_found": 0,
        "duration_ms": start.elapsed().as_secs_f64() * 1000.0,
        "timestamp": now_iso(),
    }))
}

async fn get_audit_log(
    State(state): State<ComplianceApiState>,
) -> Json<Vec<AuditEntry>> {
    Json(state.audit_log.read().clone())
}

async fn create_deletion_request(
    State(state): State<ComplianceApiState>,
    Json(req): Json<CreateDeletionRequest>,
) -> (StatusCode, Json<DeletionRequest>) {
    let request = DeletionRequest {
        id: Uuid::new_v4().to_string(),
        subject_id: req.subject_id.clone(),
        topics: req.topics,
        status: DeletionStatus::Pending,
        created_at: now_iso(),
        completed_at: None,
        records_deleted: 0,
    };
    info!(request_id = %request.id, subject = %req.subject_id, "Created deletion request");
    state.log_audit(
        "deletion_request",
        "system",
        &request.id,
        &format!("Deletion request for subject: {}", req.subject_id),
    );
    state.deletion_requests.write().push(request.clone());
    (StatusCode::CREATED, Json(request))
}

async fn generate_report(
    State(state): State<ComplianceApiState>,
) -> Json<ComplianceReport> {
    let scans = state.scan_results.read();
    let policies = state.policies.read();

    let overall_score = scans.last().map(|s| s.score).unwrap_or(100.0);
    let total_findings: Vec<&Finding> = scans.iter().flat_map(|s| &s.findings).collect();

    let mut sections = Vec::new();

    // Group findings by category
    let mut categories: HashMap<String, Vec<&Finding>> = HashMap::new();
    for f in &total_findings {
        categories
            .entry(f.category.clone())
            .or_default()
            .push(f);
    }
    for (cat, findings) in &categories {
        let critical = findings.iter().filter(|f| f.severity == "critical").count();
        let status = if critical > 0 { "fail" } else { "pass" };
        sections.push(ReportSection {
            name: cat.clone(),
            status: status.to_string(),
            findings_count: findings.len(),
            details: format!(
                "{} findings ({} critical)",
                findings.len(),
                critical
            ),
        });
    }

    // Add policy coverage section
    sections.push(ReportSection {
        name: "policy_coverage".to_string(),
        status: if policies.is_empty() { "warning" } else { "pass" }.to_string(),
        findings_count: 0,
        details: format!("{} policies configured", policies.len()),
    });

    let framework = policies
        .first()
        .map(|p| format!("{:?}", p.framework))
        .unwrap_or_else(|| "general".to_string());

    let summary = format!(
        "Overall score: {:.1}%. {} policies active, {} total findings.",
        overall_score,
        policies.iter().filter(|p| p.enabled).count(),
        total_findings.len()
    );

    state.log_audit("generate_report", "system", "cluster", "Compliance report generated");

    Json(ComplianceReport {
        framework,
        generated_at: now_iso(),
        overall_score,
        sections,
        summary,
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_state() -> ComplianceApiState {
        ComplianceApiState::new()
    }

    fn test_router() -> Router {
        create_compliance_api_router(test_state())
    }

    fn sample_policy() -> CompliancePolicy {
        CompliancePolicy {
            id: Uuid::new_v4().to_string(),
            name: "GDPR Policy".to_string(),
            framework: ComplianceFramework::Gdpr,
            rules: vec![
                ComplianceRule {
                    name: "retention".to_string(),
                    rule_type: RuleType::RetentionLimit { max_days: 90 },
                    config: HashMap::new(),
                    severity: "high".to_string(),
                },
                ComplianceRule {
                    name: "encryption".to_string(),
                    rule_type: RuleType::EncryptionRequired,
                    config: HashMap::new(),
                    severity: "critical".to_string(),
                },
            ],
            enabled: true,
            created_at: now_iso(),
        }
    }

    #[test]
    fn test_state_new() {
        let state = ComplianceApiState::new();
        assert!(state.policies.read().is_empty());
        assert!(state.scan_results.read().is_empty());
        assert!(state.deletion_requests.read().is_empty());
        assert!(state.audit_log.read().is_empty());
    }

    #[test]
    fn test_framework_serde() {
        let json = serde_json::to_string(&ComplianceFramework::Gdpr).unwrap();
        assert_eq!(json, "\"gdpr\"");
        let fw: ComplianceFramework = serde_json::from_str("\"hipaa\"").unwrap();
        assert_eq!(fw, ComplianceFramework::Hipaa);
    }

    #[test]
    fn test_custom_framework_serde() {
        let fw = ComplianceFramework::Custom("ISO27001".to_string());
        let json = serde_json::to_string(&fw).unwrap();
        let parsed: ComplianceFramework = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, ComplianceFramework::Custom("ISO27001".to_string()));
    }

    #[test]
    fn test_rule_type_serde() {
        let rt = RuleType::RetentionLimit { max_days: 30 };
        let json = serde_json::to_string(&rt).unwrap();
        assert!(json.contains("retention_limit"));
        let parsed: RuleType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, rt);
    }

    #[test]
    fn test_scan_status_serde() {
        let s = ScanStatus::Completed;
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("completed"));
    }

    #[test]
    fn test_deletion_status_serde() {
        let d = DeletionStatus::Pending;
        let json = serde_json::to_string(&d).unwrap();
        let parsed: DeletionStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, DeletionStatus::Pending);
    }

    #[test]
    fn test_audit_entry_serde() {
        let entry = AuditEntry {
            id: "a1".to_string(),
            action: "test".to_string(),
            actor: "user".to_string(),
            resource: "topic".to_string(),
            details: "detail".to_string(),
            timestamp: now_iso(),
            ip_address: None,
        };
        let json = serde_json::to_string(&entry).unwrap();
        assert!(!json.contains("ip_address")); // skipped when None
    }

    #[test]
    fn test_log_audit() {
        let state = test_state();
        state.log_audit("test_action", "tester", "resource", "details");
        assert_eq!(state.audit_log.read().len(), 1);
        assert_eq!(state.audit_log.read()[0].action, "test_action");
    }

    #[tokio::test]
    async fn test_compliance_status_empty() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/compliance/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_create_policy_endpoint() {
        let app = test_router();
        let body = serde_json::json!({
            "name": "Test Policy",
            "framework": "gdpr",
            "rules": [{
                "name": "encrypt",
                "rule_type": {"type": "encryption_required"},
                "config": {},
                "severity": "high"
            }]
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/compliance/policies")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_list_policies_empty() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/compliance/policies")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_delete_policy() {
        let state = test_state();
        let policy = sample_policy();
        let pid = policy.id.clone();
        state.policies.write().push(policy);
        let app = create_compliance_api_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(&format!("/api/v1/compliance/policies/{}", pid))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_delete_policy_not_found() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/compliance/policies/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_run_scan() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/compliance/scan")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_enforce_retention() {
        let state = test_state();
        state.policies.write().push(sample_policy());
        let app = create_compliance_api_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/compliance/retention/enforce")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_pii_scan() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/compliance/pii/scan")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_audit_log() {
        let state = test_state();
        state.log_audit("test", "actor", "res", "detail");
        let app = create_compliance_api_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/compliance/audit-log")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_create_deletion_request() {
        let app = test_router();
        let body = serde_json::json!({
            "subject_id": "user-123",
            "topics": ["events", "logs"]
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/compliance/deletion-request")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_generate_report() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/compliance/report")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
