//! Zero-Downtime Schema Migration Engine
//!
//! Orchestrates live schema migrations with dual-write support, batched record
//! transformation, sampling-based verification, and automatic rollback.
//!
//! ## Lifecycle
//!
//! 1. **Plan** – compute diff, estimate work, generate rollback steps.
//! 2. **Dual-Write** – new writes go to both old & new schemas.
//! 3. **Migrate** – transform historical records in batches.
//! 4. **Verify** – sample-check migrated data.
//! 5. **Cutover** – switch consumers to new schema, stop dual-write.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Tuning knobs for the migration engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationConfig {
    /// Maximum number of migrations executing concurrently.
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent_migrations: usize,
    /// Records transformed per batch.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Percentage of records sampled during verification (0.0–100.0).
    #[serde(default = "default_verification_sample_pct")]
    pub verification_sample_pct: f64,
    /// Automatically rollback if an error is encountered.
    #[serde(default = "default_auto_rollback")]
    pub auto_rollback_on_error: bool,
    /// How long dual-write phase lasts (seconds).
    #[serde(default = "default_dual_write_duration")]
    pub dual_write_duration_secs: u64,
}

fn default_max_concurrent() -> usize {
    3
}
fn default_batch_size() -> usize {
    1000
}
fn default_verification_sample_pct() -> f64 {
    1.0
}
fn default_auto_rollback() -> bool {
    true
}
fn default_dual_write_duration() -> u64 {
    300
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            max_concurrent_migrations: default_max_concurrent(),
            batch_size: default_batch_size(),
            verification_sample_pct: default_verification_sample_pct(),
            auto_rollback_on_error: default_auto_rollback(),
            dual_write_duration_secs: default_dual_write_duration(),
        }
    }
}

// ---------------------------------------------------------------------------
// Risk & Diff types
// ---------------------------------------------------------------------------

/// How risky a schema change is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RiskLevel {
    Safe,
    Low,
    Medium,
    High,
    Critical,
}

/// A single added or removed field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldChange {
    pub name: String,
    pub field_type: String,
    pub default_value: Option<serde_json::Value>,
}

/// A field whose type changed between versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldModification {
    pub name: String,
    pub old_type: String,
    pub new_type: String,
    pub compatible: bool,
}

/// Structural difference between two schema versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDiff {
    pub fields_added: Vec<FieldChange>,
    pub fields_removed: Vec<FieldChange>,
    pub fields_modified: Vec<FieldModification>,
    pub breaking: bool,
    pub risk_level: RiskLevel,
}

// ---------------------------------------------------------------------------
// Migration status & steps
// ---------------------------------------------------------------------------

/// Current state of a migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationStatus {
    Planned,
    DualWriting,
    Migrating,
    Verifying,
    Completed,
    RolledBack,
    Failed(String),
}

/// An individual step in a migration or rollback plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationStep {
    StartDualWrite,
    TransformRecords { batch_size: usize },
    VerifySample { sample_pct: f64 },
    CutoverConsumers,
    StopDualWrite,
    Cleanup,
}

/// Ordered steps + estimates for a migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationPlan {
    pub steps: Vec<MigrationStep>,
    pub estimated_duration_secs: u64,
    pub estimated_records: u64,
    pub rollback_plan: Vec<MigrationStep>,
}

// ---------------------------------------------------------------------------
// Progress tracking
// ---------------------------------------------------------------------------

/// Live progress of a running migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationProgress {
    pub records_processed: u64,
    pub records_total: u64,
    pub percent_complete: f64,
    pub errors: u64,
    pub verification_passed: Option<bool>,
}

impl Default for MigrationProgress {
    fn default() -> Self {
        Self {
            records_processed: 0,
            records_total: 0,
            percent_complete: 0.0,
            errors: 0,
            verification_passed: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Migration aggregate
// ---------------------------------------------------------------------------

/// A full migration record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Migration {
    pub id: String,
    pub subject: String,
    pub from_version: u32,
    pub to_version: u32,
    pub schema_diff: SchemaDiff,
    pub status: MigrationStatus,
    pub plan: MigrationPlan,
    pub progress: MigrationProgress,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
}

// ---------------------------------------------------------------------------
// Engine statistics (lock-free)
// ---------------------------------------------------------------------------

/// Atomic counters for observability.
#[derive(Debug, Default)]
pub struct MigrationStats {
    pub migrations_planned: AtomicU64,
    pub migrations_started: AtomicU64,
    pub migrations_completed: AtomicU64,
    pub migrations_failed: AtomicU64,
    pub migrations_rolled_back: AtomicU64,
    pub records_processed: AtomicU64,
    pub verification_runs: AtomicU64,
}

/// Serialisable snapshot of [`MigrationStats`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationStatsSnapshot {
    pub migrations_planned: u64,
    pub migrations_started: u64,
    pub migrations_completed: u64,
    pub migrations_failed: u64,
    pub migrations_rolled_back: u64,
    pub records_processed: u64,
    pub verification_runs: u64,
}

impl MigrationStats {
    pub fn snapshot(&self) -> MigrationStatsSnapshot {
        MigrationStatsSnapshot {
            migrations_planned: self.migrations_planned.load(Ordering::Relaxed),
            migrations_started: self.migrations_started.load(Ordering::Relaxed),
            migrations_completed: self.migrations_completed.load(Ordering::Relaxed),
            migrations_failed: self.migrations_failed.load(Ordering::Relaxed),
            migrations_rolled_back: self.migrations_rolled_back.load(Ordering::Relaxed),
            records_processed: self.records_processed.load(Ordering::Relaxed),
            verification_runs: self.verification_runs.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Migration error
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, thiserror::Error)]
pub enum MigrationError {
    #[error("Migration not found: {0}")]
    NotFound(String),
    #[error("Invalid state transition from {from:?} for operation {op}")]
    InvalidState { from: MigrationStatus, op: String },
    #[error("Concurrent migration limit ({0}) reached")]
    ConcurrencyLimit(usize),
    #[error("Verification failed for migration {0}")]
    VerificationFailed(String),
    #[error("Internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// Orchestrates zero-downtime schema migrations.
pub struct SchemaMigrationEngine {
    migrations: Arc<RwLock<HashMap<String, Migration>>>,
    config: MigrationConfig,
    stats: Arc<MigrationStats>,
}

impl SchemaMigrationEngine {
    /// Create a new engine with the supplied configuration.
    pub fn new(config: MigrationConfig) -> Self {
        info!(
            max_concurrent = config.max_concurrent_migrations,
            batch_size = config.batch_size,
            "Schema migration engine initialised"
        );
        Self {
            migrations: Arc::new(RwLock::new(HashMap::new())),
            config,
            stats: Arc::new(MigrationStats::default()),
        }
    }

    /// Compute the structural diff between an old and new schema definition.
    pub fn compute_diff(
        &self,
        old_schema: &serde_json::Value,
        new_schema: &serde_json::Value,
    ) -> SchemaDiff {
        let old_fields = extract_fields(old_schema);
        let new_fields = extract_fields(new_schema);

        let fields_added: Vec<FieldChange> = new_fields
            .iter()
            .filter(|(k, _)| !old_fields.contains_key(*k))
            .map(|(k, v)| FieldChange {
                name: k.clone(),
                field_type: v.clone(),
                default_value: None,
            })
            .collect();

        let fields_removed: Vec<FieldChange> = old_fields
            .iter()
            .filter(|(k, _)| !new_fields.contains_key(*k))
            .map(|(k, v)| FieldChange {
                name: k.clone(),
                field_type: v.clone(),
                default_value: None,
            })
            .collect();

        let fields_modified: Vec<FieldModification> = old_fields
            .iter()
            .filter_map(|(k, old_t)| {
                new_fields.get(k).and_then(|new_t| {
                    if old_t != new_t {
                        Some(FieldModification {
                            name: k.clone(),
                            old_type: old_t.clone(),
                            new_type: new_t.clone(),
                            compatible: is_compatible_type_change(old_t, new_t),
                        })
                    } else {
                        None
                    }
                })
            })
            .collect();

        let breaking =
            !fields_removed.is_empty() || fields_modified.iter().any(|m| !m.compatible);

        let risk_level = compute_risk(&fields_added, &fields_removed, &fields_modified);

        debug!(
            added = fields_added.len(),
            removed = fields_removed.len(),
            modified = fields_modified.len(),
            breaking,
            ?risk_level,
            "Schema diff computed"
        );

        SchemaDiff {
            fields_added,
            fields_removed,
            fields_modified,
            breaking,
            risk_level,
        }
    }

    /// Plan a migration without starting it.
    pub async fn plan_migration(
        &self,
        subject: String,
        from_version: u32,
        to_version: u32,
        diff: SchemaDiff,
    ) -> Result<Migration, MigrationError> {
        let id = Uuid::new_v4().to_string();
        let estimated_records = 10_000; // placeholder estimate

        let plan = MigrationPlan {
            steps: build_steps(&self.config, &diff),
            estimated_duration_secs: estimate_duration(&self.config, estimated_records),
            estimated_records,
            rollback_plan: build_rollback_steps(),
        };

        let migration = Migration {
            id: id.clone(),
            subject,
            from_version,
            to_version,
            schema_diff: diff,
            status: MigrationStatus::Planned,
            plan,
            progress: MigrationProgress::default(),
            created_at: chrono::Utc::now().to_rfc3339(),
            started_at: None,
            completed_at: None,
        };

        self.migrations
            .write()
            .await
            .insert(id.clone(), migration.clone());
        self.stats
            .migrations_planned
            .fetch_add(1, Ordering::Relaxed);

        info!(migration_id = %id, "Migration planned");
        Ok(migration)
    }

    /// Begin executing a planned migration.
    pub async fn start_migration(&self, id: &str) -> Result<Migration, MigrationError> {
        let mut map = self.migrations.write().await;
        let active = map
            .values()
            .filter(|m| {
                matches!(
                    m.status,
                    MigrationStatus::DualWriting | MigrationStatus::Migrating
                )
            })
            .count();

        if active >= self.config.max_concurrent_migrations {
            return Err(MigrationError::ConcurrencyLimit(
                self.config.max_concurrent_migrations,
            ));
        }

        let migration = map
            .get_mut(id)
            .ok_or_else(|| MigrationError::NotFound(id.to_string()))?;

        if migration.status != MigrationStatus::Planned {
            return Err(MigrationError::InvalidState {
                from: migration.status.clone(),
                op: "start".into(),
            });
        }

        migration.status = MigrationStatus::DualWriting;
        migration.started_at = Some(chrono::Utc::now().to_rfc3339());
        self.stats
            .migrations_started
            .fetch_add(1, Ordering::Relaxed);

        info!(migration_id = %id, "Migration started – entering dual-write phase");
        Ok(migration.clone())
    }

    /// Pause a running migration (returns to Planned).
    pub async fn pause_migration(&self, id: &str) -> Result<Migration, MigrationError> {
        let mut map = self.migrations.write().await;
        let migration = map
            .get_mut(id)
            .ok_or_else(|| MigrationError::NotFound(id.to_string()))?;

        match &migration.status {
            MigrationStatus::DualWriting | MigrationStatus::Migrating => {
                warn!(migration_id = %id, "Pausing migration");
                migration.status = MigrationStatus::Planned;
                Ok(migration.clone())
            }
            other => Err(MigrationError::InvalidState {
                from: other.clone(),
                op: "pause".into(),
            }),
        }
    }

    /// Rollback a migration.
    pub async fn rollback_migration(&self, id: &str) -> Result<Migration, MigrationError> {
        let mut map = self.migrations.write().await;
        let migration = map
            .get_mut(id)
            .ok_or_else(|| MigrationError::NotFound(id.to_string()))?;

        match &migration.status {
            MigrationStatus::Completed | MigrationStatus::RolledBack => {
                Err(MigrationError::InvalidState {
                    from: migration.status.clone(),
                    op: "rollback".into(),
                })
            }
            _ => {
                warn!(migration_id = %id, "Rolling back migration");
                migration.status = MigrationStatus::RolledBack;
                migration.completed_at = Some(chrono::Utc::now().to_rfc3339());
                self.stats
                    .migrations_rolled_back
                    .fetch_add(1, Ordering::Relaxed);
                Ok(migration.clone())
            }
        }
    }

    /// Retrieve a single migration by ID.
    pub async fn get_migration(&self, id: &str) -> Result<Migration, MigrationError> {
        self.migrations
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| MigrationError::NotFound(id.to_string()))
    }

    /// List all tracked migrations.
    pub async fn list_migrations(&self) -> Vec<Migration> {
        self.migrations.read().await.values().cloned().collect()
    }

    /// Run verification on a migration that is in the Verifying or Migrating state.
    pub async fn verify_migration(&self, id: &str) -> Result<bool, MigrationError> {
        let mut map = self.migrations.write().await;
        let migration = map
            .get_mut(id)
            .ok_or_else(|| MigrationError::NotFound(id.to_string()))?;

        match &migration.status {
            MigrationStatus::Migrating
            | MigrationStatus::Verifying
            | MigrationStatus::DualWriting => {}
            other => {
                return Err(MigrationError::InvalidState {
                    from: other.clone(),
                    op: "verify".into(),
                });
            }
        }

        migration.status = MigrationStatus::Verifying;
        self.stats
            .verification_runs
            .fetch_add(1, Ordering::Relaxed);

        // Simulated verification: pass if error rate < 1%
        let passed = if migration.progress.records_total > 0 {
            let error_rate =
                migration.progress.errors as f64 / migration.progress.records_total as f64;
            error_rate < 0.01
        } else {
            true
        };

        migration.progress.verification_passed = Some(passed);

        if passed {
            migration.status = MigrationStatus::Completed;
            migration.completed_at = Some(chrono::Utc::now().to_rfc3339());
            self.stats
                .migrations_completed
                .fetch_add(1, Ordering::Relaxed);
            info!(migration_id = %id, "Migration verified and completed");
        } else if self.config.auto_rollback_on_error {
            error!(migration_id = %id, "Verification failed – auto-rolling back");
            migration.status = MigrationStatus::RolledBack;
            migration.completed_at = Some(chrono::Utc::now().to_rfc3339());
            self.stats
                .migrations_rolled_back
                .fetch_add(1, Ordering::Relaxed);
        } else {
            migration.status = MigrationStatus::Failed("Verification failed".into());
            self.stats
                .migrations_failed
                .fetch_add(1, Ordering::Relaxed);
        }

        Ok(passed)
    }

    /// Return a snapshot of engine statistics.
    pub fn stats(&self) -> &Arc<MigrationStats> {
        &self.stats
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_fields(schema: &serde_json::Value) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Some(fields) = schema.get("fields").and_then(|f| f.as_array()) {
        for field in fields {
            if let (Some(name), Some(ft)) = (
                field.get("name").and_then(|n| n.as_str()),
                field.get("type").and_then(|t| t.as_str()),
            ) {
                map.insert(name.to_string(), ft.to_string());
            }
        }
    }
    map
}

fn is_compatible_type_change(old: &str, new: &str) -> bool {
    matches!(
        (old, new),
        ("int", "long")
            | ("int", "float")
            | ("int", "double")
            | ("long", "double")
            | ("float", "double")
            | ("string", "string")
    )
}

fn compute_risk(
    added: &[FieldChange],
    removed: &[FieldChange],
    modified: &[FieldModification],
) -> RiskLevel {
    if !removed.is_empty() && !modified.is_empty() {
        RiskLevel::Critical
    } else if modified.iter().any(|m| !m.compatible) {
        RiskLevel::High
    } else if !removed.is_empty() {
        RiskLevel::Medium
    } else if !modified.is_empty() {
        RiskLevel::Low
    } else if added.is_empty() {
        RiskLevel::Safe
    } else {
        RiskLevel::Safe
    }
}

fn build_steps(config: &MigrationConfig, diff: &SchemaDiff) -> Vec<MigrationStep> {
    let mut steps = vec![MigrationStep::StartDualWrite];

    if diff.breaking || !diff.fields_modified.is_empty() || !diff.fields_removed.is_empty() {
        steps.push(MigrationStep::TransformRecords {
            batch_size: config.batch_size,
        });
    }

    steps.push(MigrationStep::VerifySample {
        sample_pct: config.verification_sample_pct,
    });
    steps.push(MigrationStep::CutoverConsumers);
    steps.push(MigrationStep::StopDualWrite);
    steps.push(MigrationStep::Cleanup);
    steps
}

fn build_rollback_steps() -> Vec<MigrationStep> {
    vec![
        MigrationStep::StopDualWrite,
        MigrationStep::CutoverConsumers,
        MigrationStep::Cleanup,
    ]
}

fn estimate_duration(config: &MigrationConfig, records: u64) -> u64 {
    let batches = (records + config.batch_size as u64 - 1) / config.batch_size as u64;
    // ~100ms per batch + dual-write window
    config.dual_write_duration_secs + batches / 10
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn default_engine() -> SchemaMigrationEngine {
        SchemaMigrationEngine::new(MigrationConfig::default())
    }

    fn simple_diff() -> SchemaDiff {
        SchemaDiff {
            fields_added: vec![FieldChange {
                name: "email".into(),
                field_type: "string".into(),
                default_value: None,
            }],
            fields_removed: vec![],
            fields_modified: vec![],
            breaking: false,
            risk_level: RiskLevel::Safe,
        }
    }

    // -- config defaults ---------------------------------------------------

    #[test]
    fn test_migration_config_defaults() {
        let cfg = MigrationConfig::default();
        assert_eq!(cfg.max_concurrent_migrations, 3);
        assert_eq!(cfg.batch_size, 1000);
        assert!((cfg.verification_sample_pct - 1.0).abs() < f64::EPSILON);
        assert!(cfg.auto_rollback_on_error);
        assert_eq!(cfg.dual_write_duration_secs, 300);
    }

    // -- plan --------------------------------------------------------------

    #[tokio::test]
    async fn test_plan_migration() {
        let engine = default_engine();
        let m = engine
            .plan_migration("orders-value".into(), 1, 2, simple_diff())
            .await
            .unwrap();
        assert_eq!(m.status, MigrationStatus::Planned);
        assert_eq!(m.from_version, 1);
        assert_eq!(m.to_version, 2);
        assert!(!m.id.is_empty());
    }

    #[tokio::test]
    async fn test_plan_sets_timestamps() {
        let engine = default_engine();
        let m = engine
            .plan_migration("t".into(), 1, 2, simple_diff())
            .await
            .unwrap();
        assert!(!m.created_at.is_empty());
        assert!(m.started_at.is_none());
        assert!(m.completed_at.is_none());
    }

    // -- start -------------------------------------------------------------

    #[tokio::test]
    async fn test_start_migration() {
        let engine = default_engine();
        let m = engine
            .plan_migration("t".into(), 1, 2, simple_diff())
            .await
            .unwrap();
        let started = engine.start_migration(&m.id).await.unwrap();
        assert_eq!(started.status, MigrationStatus::DualWriting);
        assert!(started.started_at.is_some());
    }

    #[tokio::test]
    async fn test_start_already_started_fails() {
        let engine = default_engine();
        let m = engine
            .plan_migration("t".into(), 1, 2, simple_diff())
            .await
            .unwrap();
        engine.start_migration(&m.id).await.unwrap();
        let err = engine.start_migration(&m.id).await.unwrap_err();
        assert!(matches!(err, MigrationError::InvalidState { .. }));
    }

    #[tokio::test]
    async fn test_concurrency_limit() {
        let cfg = MigrationConfig {
            max_concurrent_migrations: 1,
            ..Default::default()
        };
        let engine = SchemaMigrationEngine::new(cfg);
        let m1 = engine
            .plan_migration("a".into(), 1, 2, simple_diff())
            .await
            .unwrap();
        engine.start_migration(&m1.id).await.unwrap();

        let m2 = engine
            .plan_migration("b".into(), 1, 2, simple_diff())
            .await
            .unwrap();
        let err = engine.start_migration(&m2.id).await.unwrap_err();
        assert!(matches!(err, MigrationError::ConcurrencyLimit(1)));
    }

    // -- pause -------------------------------------------------------------

    #[tokio::test]
    async fn test_pause_migration() {
        let engine = default_engine();
        let m = engine
            .plan_migration("t".into(), 1, 2, simple_diff())
            .await
            .unwrap();
        engine.start_migration(&m.id).await.unwrap();
        let paused = engine.pause_migration(&m.id).await.unwrap();
        assert_eq!(paused.status, MigrationStatus::Planned);
    }

    #[tokio::test]
    async fn test_pause_planned_fails() {
        let engine = default_engine();
        let m = engine
            .plan_migration("t".into(), 1, 2, simple_diff())
            .await
            .unwrap();
        let err = engine.pause_migration(&m.id).await.unwrap_err();
        assert!(matches!(err, MigrationError::InvalidState { .. }));
    }

    // -- rollback ----------------------------------------------------------

    #[tokio::test]
    async fn test_rollback_migration() {
        let engine = default_engine();
        let m = engine
            .plan_migration("t".into(), 1, 2, simple_diff())
            .await
            .unwrap();
        engine.start_migration(&m.id).await.unwrap();
        let rolled = engine.rollback_migration(&m.id).await.unwrap();
        assert_eq!(rolled.status, MigrationStatus::RolledBack);
        assert!(rolled.completed_at.is_some());
    }

    #[tokio::test]
    async fn test_rollback_completed_fails() {
        let engine = default_engine();
        let m = engine
            .plan_migration("t".into(), 1, 2, simple_diff())
            .await
            .unwrap();
        engine.start_migration(&m.id).await.unwrap();
        engine.verify_migration(&m.id).await.unwrap();
        let err = engine.rollback_migration(&m.id).await.unwrap_err();
        assert!(matches!(err, MigrationError::InvalidState { .. }));
    }

    // -- get / list --------------------------------------------------------

    #[tokio::test]
    async fn test_get_migration() {
        let engine = default_engine();
        let m = engine
            .plan_migration("t".into(), 1, 2, simple_diff())
            .await
            .unwrap();
        let fetched = engine.get_migration(&m.id).await.unwrap();
        assert_eq!(fetched.id, m.id);
    }

    #[tokio::test]
    async fn test_get_missing_returns_not_found() {
        let engine = default_engine();
        let err = engine.get_migration("nonexistent").await.unwrap_err();
        assert!(matches!(err, MigrationError::NotFound(_)));
    }

    #[tokio::test]
    async fn test_list_migrations() {
        let engine = default_engine();
        engine
            .plan_migration("a".into(), 1, 2, simple_diff())
            .await
            .unwrap();
        engine
            .plan_migration("b".into(), 2, 3, simple_diff())
            .await
            .unwrap();
        let list = engine.list_migrations().await;
        assert_eq!(list.len(), 2);
    }

    // -- verify ------------------------------------------------------------

    #[tokio::test]
    async fn test_verify_passes_no_errors() {
        let engine = default_engine();
        let m = engine
            .plan_migration("t".into(), 1, 2, simple_diff())
            .await
            .unwrap();
        engine.start_migration(&m.id).await.unwrap();
        let passed = engine.verify_migration(&m.id).await.unwrap();
        assert!(passed);
        let updated = engine.get_migration(&m.id).await.unwrap();
        assert_eq!(updated.status, MigrationStatus::Completed);
    }

    #[tokio::test]
    async fn test_verify_planned_fails() {
        let engine = default_engine();
        let m = engine
            .plan_migration("t".into(), 1, 2, simple_diff())
            .await
            .unwrap();
        let err = engine.verify_migration(&m.id).await.unwrap_err();
        assert!(matches!(err, MigrationError::InvalidState { .. }));
    }

    // -- compute_diff ------------------------------------------------------

    #[test]
    fn test_compute_diff_add_field() {
        let engine = default_engine();
        let old = json!({"fields": [{"name": "id", "type": "int"}]});
        let new = json!({"fields": [{"name": "id", "type": "int"}, {"name": "email", "type": "string"}]});
        let diff = engine.compute_diff(&old, &new);
        assert_eq!(diff.fields_added.len(), 1);
        assert_eq!(diff.fields_added[0].name, "email");
        assert!(!diff.breaking);
    }

    #[test]
    fn test_compute_diff_remove_field_is_breaking() {
        let engine = default_engine();
        let old = json!({"fields": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}]});
        let new = json!({"fields": [{"name": "id", "type": "int"}]});
        let diff = engine.compute_diff(&old, &new);
        assert_eq!(diff.fields_removed.len(), 1);
        assert!(diff.breaking);
    }

    #[test]
    fn test_compute_diff_modify_field() {
        let engine = default_engine();
        let old = json!({"fields": [{"name": "count", "type": "int"}]});
        let new = json!({"fields": [{"name": "count", "type": "long"}]});
        let diff = engine.compute_diff(&old, &new);
        assert_eq!(diff.fields_modified.len(), 1);
        assert!(diff.fields_modified[0].compatible);
        assert!(!diff.breaking);
    }

    #[test]
    fn test_compute_diff_incompatible_modify() {
        let engine = default_engine();
        let old = json!({"fields": [{"name": "value", "type": "string"}]});
        let new = json!({"fields": [{"name": "value", "type": "int"}]});
        let diff = engine.compute_diff(&old, &new);
        assert!(diff.breaking);
        assert!(!diff.fields_modified[0].compatible);
    }

    // -- stats -------------------------------------------------------------

    #[tokio::test]
    async fn test_stats_increment() {
        let engine = default_engine();
        engine
            .plan_migration("t".into(), 1, 2, simple_diff())
            .await
            .unwrap();
        let snap = engine.stats().snapshot();
        assert_eq!(snap.migrations_planned, 1);
        assert_eq!(snap.migrations_started, 0);
    }

    // -- risk level --------------------------------------------------------

    #[test]
    fn test_risk_level_critical() {
        let risk = compute_risk(
            &[],
            &[FieldChange {
                name: "x".into(),
                field_type: "int".into(),
                default_value: None,
            }],
            &[FieldModification {
                name: "y".into(),
                old_type: "string".into(),
                new_type: "int".into(),
                compatible: false,
            }],
        );
        assert_eq!(risk, RiskLevel::Critical);
    }

    #[test]
    fn test_risk_level_safe() {
        let risk = compute_risk(
            &[FieldChange {
                name: "a".into(),
                field_type: "string".into(),
                default_value: None,
            }],
            &[],
            &[],
        );
        assert_eq!(risk, RiskLevel::Safe);
    }

    // -- serialisation roundtrip -------------------------------------------

    #[test]
    fn test_migration_status_serde_roundtrip() {
        let statuses = vec![
            MigrationStatus::Planned,
            MigrationStatus::DualWriting,
            MigrationStatus::Completed,
            MigrationStatus::Failed("oops".into()),
        ];
        for s in &statuses {
            let json = serde_json::to_string(s).unwrap();
            let back: MigrationStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(&back, s);
        }
    }

    #[test]
    fn test_migration_config_serde_roundtrip() {
        let cfg = MigrationConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let back: MigrationConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.batch_size, cfg.batch_size);
    }
}
