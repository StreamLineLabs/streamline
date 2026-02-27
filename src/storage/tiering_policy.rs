//! Tiered Storage Policy Engine
//!
//! Policy engine for automatic hot/warm/cold data tiering with transparent read-through.
//! Supports per-topic policy overrides, multiple tier conditions, and cloud storage backends.

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Storage tier
// ---------------------------------------------------------------------------

/// Represents the storage tier a segment resides in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageTier {
    Hot,
    Warm,
    Cold,
    Frozen,
    Delete,
}

impl StorageTier {
    /// Returns a numeric coldness rank (higher = colder).
    fn coldness(&self) -> u8 {
        match self {
            StorageTier::Hot => 0,
            StorageTier::Warm => 1,
            StorageTier::Cold => 2,
            StorageTier::Frozen => 3,
            StorageTier::Delete => 4,
        }
    }
}

impl std::fmt::Display for StorageTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageTier::Hot => write!(f, "hot"),
            StorageTier::Warm => write!(f, "warm"),
            StorageTier::Cold => write!(f, "cold"),
            StorageTier::Frozen => write!(f, "frozen"),
            StorageTier::Delete => write!(f, "delete"),
        }
    }
}

// ---------------------------------------------------------------------------
// Tier condition & backend
// ---------------------------------------------------------------------------

/// Condition that triggers a tier transition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TierCondition {
    /// Segment age exceeds the given number of hours.
    AgeHours(u64),
    /// Segment size exceeds the given number of bytes.
    SizeBytes(u64),
    /// Read frequency drops below threshold.
    AccessFrequencyBelow { reads_per_hour: f64 },
    /// Both age and size conditions must be met.
    Combined { age_hours: u64, size_bytes: u64 },
}

/// Backend target for a tier.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TierBackend {
    Local { path: String },
    S3 { bucket: String, prefix: String, region: String },
    Azure { container: String, prefix: String },
    Gcs { bucket: String, prefix: String },
}

// ---------------------------------------------------------------------------
// Tier rule
// ---------------------------------------------------------------------------

/// A single rule mapping a condition to a target tier and backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierRule {
    pub tier: StorageTier,
    pub condition: TierCondition,
    pub backend: TierBackend,
}

// ---------------------------------------------------------------------------
// Topic-level policy
// ---------------------------------------------------------------------------

/// Per-topic tiering policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicTieringPolicy {
    pub topic: String,
    pub tiers: Vec<TierRule>,
    pub override_global: bool,
    pub created_at: u64,
    pub last_evaluated_at: Option<u64>,
}

// ---------------------------------------------------------------------------
// Global policy
// ---------------------------------------------------------------------------

/// Global tiering defaults applied when no topic-specific override exists.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalTieringPolicy {
    pub default_hot_retention_hours: u64,
    pub default_warm_retention_hours: u64,
    pub default_cold_retention_hours: u64,
    pub enable_auto_tiering: bool,
    pub compaction_on_tier_transition: bool,
    pub read_through_cache_size_mb: u64,
    pub max_concurrent_uploads: usize,
    pub max_concurrent_downloads: usize,
}

impl Default for GlobalTieringPolicy {
    fn default() -> Self {
        Self {
            default_hot_retention_hours: 24,
            default_warm_retention_hours: 168,
            default_cold_retention_hours: 8760,
            enable_auto_tiering: true,
            compaction_on_tier_transition: true,
            read_through_cache_size_mb: 256,
            max_concurrent_uploads: 4,
            max_concurrent_downloads: 8,
        }
    }
}

// ---------------------------------------------------------------------------
// Segment info & decision
// ---------------------------------------------------------------------------

/// Lightweight segment metadata used for batch evaluation.
#[derive(Debug, Clone)]
pub struct SegmentInfo {
    pub segment_id: String,
    pub partition: i32,
    pub age_hours: f64,
    pub size_bytes: u64,
    pub current_tier: StorageTier,
    pub reads_per_hour: f64,
}

/// A tiering decision produced by the policy engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentTieringDecision {
    pub segment_id: String,
    pub topic: String,
    pub partition: i32,
    pub current_tier: StorageTier,
    pub target_tier: StorageTier,
    pub reason: String,
    pub segment_age_hours: f64,
    pub segment_size_bytes: u64,
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Atomic counters for tiering operations.
pub struct TieringStats {
    pub segments_evaluated: AtomicU64,
    pub segments_moved: AtomicU64,
    pub bytes_uploaded: AtomicU64,
    pub bytes_downloaded: AtomicU64,
    pub read_through_hits: AtomicU64,
    pub read_through_misses: AtomicU64,
    pub errors: AtomicU64,
}

impl TieringStats {
    fn new() -> Self {
        Self {
            segments_evaluated: AtomicU64::new(0),
            segments_moved: AtomicU64::new(0),
            bytes_uploaded: AtomicU64::new(0),
            bytes_downloaded: AtomicU64::new(0),
            read_through_hits: AtomicU64::new(0),
            read_through_misses: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> TieringStatsSnapshot {
        TieringStatsSnapshot {
            segments_evaluated: self.segments_evaluated.load(Ordering::Relaxed),
            segments_moved: self.segments_moved.load(Ordering::Relaxed),
            bytes_uploaded: self.bytes_uploaded.load(Ordering::Relaxed),
            bytes_downloaded: self.bytes_downloaded.load(Ordering::Relaxed),
            read_through_hits: self.read_through_hits.load(Ordering::Relaxed),
            read_through_misses: self.read_through_misses.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

/// Serialisable snapshot of tiering statistics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TieringStatsSnapshot {
    pub segments_evaluated: u64,
    pub segments_moved: u64,
    pub bytes_uploaded: u64,
    pub bytes_downloaded: u64,
    pub read_through_hits: u64,
    pub read_through_misses: u64,
    pub errors: u64,
}

// ---------------------------------------------------------------------------
// Policy engine
// ---------------------------------------------------------------------------

/// Core policy engine for tiered storage decisions.
pub struct TieringPolicyEngine {
    policies: Arc<RwLock<HashMap<String, TopicTieringPolicy>>>,
    global_policy: Arc<RwLock<GlobalTieringPolicy>>,
    stats: Arc<TieringStats>,
}

impl TieringPolicyEngine {
    /// Create a new policy engine with the given global defaults.
    pub fn new(global: GlobalTieringPolicy) -> Self {
        info!(
            hot_hours = global.default_hot_retention_hours,
            warm_hours = global.default_warm_retention_hours,
            cold_hours = global.default_cold_retention_hours,
            "Tiering policy engine initialized"
        );
        Self {
            policies: Arc::new(RwLock::new(HashMap::new())),
            global_policy: Arc::new(RwLock::new(global)),
            stats: Arc::new(TieringStats::new()),
        }
    }

    // -- Topic policy CRUD --------------------------------------------------

    /// Register or update a per-topic tiering policy.
    pub fn set_topic_policy(&self, policy: TopicTieringPolicy) -> Result<()> {
        if policy.topic.is_empty() {
            return Err(StreamlineError::Config(
                "Topic name must not be empty".into(),
            ));
        }
        if policy.tiers.is_empty() {
            return Err(StreamlineError::Config(
                "Policy must contain at least one tier rule".into(),
            ));
        }
        let topic = policy.topic.clone();
        let mut map = self.policies.write().map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to acquire policy lock: {e}"))
        })?;
        debug!(topic = %topic, rules = policy.tiers.len(), "Setting topic tiering policy");
        map.insert(topic, policy);
        Ok(())
    }

    /// Retrieve the policy for a specific topic, if any.
    pub fn get_topic_policy(&self, topic: &str) -> Option<TopicTieringPolicy> {
        let map = self.policies.read().ok()?;
        map.get(topic).cloned()
    }

    /// Remove a per-topic policy.
    pub fn remove_topic_policy(&self, topic: &str) -> Result<()> {
        let mut map = self.policies.write().map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to acquire policy lock: {e}"))
        })?;
        if map.remove(topic).is_none() {
            warn!(topic = %topic, "Attempted to remove non-existent topic policy");
        }
        Ok(())
    }

    /// List all registered per-topic policies.
    pub fn list_policies(&self) -> Vec<TopicTieringPolicy> {
        let map = self.policies.read().unwrap_or_else(|e| e.into_inner());
        map.values().cloned().collect()
    }

    // -- Global policy ------------------------------------------------------

    /// Replace the global tiering policy.
    pub fn update_global_policy(&self, policy: GlobalTieringPolicy) {
        info!(
            hot_hours = policy.default_hot_retention_hours,
            warm_hours = policy.default_warm_retention_hours,
            cold_hours = policy.default_cold_retention_hours,
            "Updating global tiering policy"
        );
        let mut g = self
            .global_policy
            .write()
            .unwrap_or_else(|e| e.into_inner());
        *g = policy;
    }

    /// Return a clone of the current global policy.
    pub fn get_global_policy(&self) -> GlobalTieringPolicy {
        self.global_policy
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    // -- Evaluation ---------------------------------------------------------

    /// Evaluate a single segment and return a tiering decision if a transition is warranted.
    pub fn evaluate_segment(
        &self,
        topic: &str,
        partition: i32,
        segment_id: &str,
        age_hours: f64,
        size_bytes: u64,
        current_tier: StorageTier,
        reads_per_hour: f64,
    ) -> Option<SegmentTieringDecision> {
        self.stats.segments_evaluated.fetch_add(1, Ordering::Relaxed);

        let rules = self.resolve_rules(topic);
        self.evaluate_against_rules(
            &rules,
            topic,
            partition,
            segment_id,
            age_hours,
            size_bytes,
            current_tier,
            reads_per_hour,
        )
    }

    /// Evaluate all segments belonging to a topic and return decisions for those that need moving.
    pub fn evaluate_topic(
        &self,
        topic: &str,
        segments: &[SegmentInfo],
    ) -> Vec<SegmentTieringDecision> {
        let rules = self.resolve_rules(topic);
        let mut decisions = Vec::new();

        for seg in segments {
            self.stats.segments_evaluated.fetch_add(1, Ordering::Relaxed);
            if let Some(d) = self.evaluate_against_rules(
                &rules,
                topic,
                seg.partition,
                &seg.segment_id,
                seg.age_hours,
                seg.size_bytes,
                seg.current_tier,
                seg.reads_per_hour,
            ) {
                decisions.push(d);
            }
        }

        decisions
    }

    /// Return a snapshot of accumulated statistics.
    pub fn stats(&self) -> TieringStatsSnapshot {
        self.stats.snapshot()
    }

    // -- Internal helpers ---------------------------------------------------

    /// Resolve the effective tier rules for a topic.
    ///
    /// If the topic has a policy with `override_global == true`, use its rules.
    /// Otherwise fall back to rules synthesised from the global policy.
    fn resolve_rules(&self, topic: &str) -> Vec<TierRule> {
        if let Some(tp) = self.get_topic_policy(topic) {
            if tp.override_global {
                return tp.tiers;
            }
        }
        self.global_default_rules()
    }

    /// Build tier rules from the global policy defaults.
    fn global_default_rules(&self) -> Vec<TierRule> {
        let gp = self.get_global_policy();
        vec![
            TierRule {
                tier: StorageTier::Delete,
                condition: TierCondition::AgeHours(gp.default_cold_retention_hours),
                backend: TierBackend::Local {
                    path: String::new(),
                },
            },
            TierRule {
                tier: StorageTier::Cold,
                condition: TierCondition::AgeHours(gp.default_warm_retention_hours),
                backend: TierBackend::Local {
                    path: "/tmp/cold".into(),
                },
            },
            TierRule {
                tier: StorageTier::Warm,
                condition: TierCondition::AgeHours(gp.default_hot_retention_hours),
                backend: TierBackend::Local {
                    path: "/tmp/warm".into(),
                },
            },
        ]
    }

    /// Evaluate a segment against a set of rules (coldest-first).
    ///
    /// Rules are sorted by descending tier coldness; the first matching condition wins.
    /// A decision is only returned when the target tier is colder than the current tier.
    fn evaluate_against_rules(
        &self,
        rules: &[TierRule],
        topic: &str,
        partition: i32,
        segment_id: &str,
        age_hours: f64,
        size_bytes: u64,
        current_tier: StorageTier,
        reads_per_hour: f64,
    ) -> Option<SegmentTieringDecision> {
        let mut sorted: Vec<&TierRule> = rules.iter().collect();
        sorted.sort_by(|a, b| b.tier.coldness().cmp(&a.tier.coldness()));

        for rule in sorted {
            if rule.tier.coldness() <= current_tier.coldness() {
                continue;
            }
            if Self::condition_matches(&rule.condition, age_hours, size_bytes, reads_per_hour) {
                return Some(SegmentTieringDecision {
                    segment_id: segment_id.to_string(),
                    topic: topic.to_string(),
                    partition,
                    current_tier,
                    target_tier: rule.tier,
                    reason: Self::format_reason(&rule.condition, &rule.tier),
                    segment_age_hours: age_hours,
                    segment_size_bytes: size_bytes,
                });
            }
        }
        None
    }

    /// Check whether a condition is satisfied.
    fn condition_matches(
        cond: &TierCondition,
        age_hours: f64,
        size_bytes: u64,
        reads_per_hour: f64,
    ) -> bool {
        match cond {
            TierCondition::AgeHours(h) => age_hours >= *h as f64,
            TierCondition::SizeBytes(s) => size_bytes >= *s,
            TierCondition::AccessFrequencyBelow { reads_per_hour: threshold } => {
                reads_per_hour < *threshold
            }
            TierCondition::Combined {
                age_hours: h,
                size_bytes: s,
            } => age_hours >= *h as f64 && size_bytes >= *s,
        }
    }

    fn format_reason(cond: &TierCondition, tier: &StorageTier) -> String {
        match cond {
            TierCondition::AgeHours(h) => {
                format!("Segment age exceeds {h} hours; moving to {tier}")
            }
            TierCondition::SizeBytes(s) => {
                format!("Segment size exceeds {s} bytes; moving to {tier}")
            }
            TierCondition::AccessFrequencyBelow { reads_per_hour } => {
                format!(
                    "Read frequency below {reads_per_hour} reads/hour; moving to {tier}"
                )
            }
            TierCondition::Combined { age_hours, size_bytes } => {
                format!(
                    "Segment age >= {age_hours}h and size >= {size_bytes}B; moving to {tier}"
                )
            }
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- Helpers ------------------------------------------------------------

    fn default_engine() -> TieringPolicyEngine {
        TieringPolicyEngine::new(GlobalTieringPolicy::default())
    }

    fn sample_topic_policy(topic: &str) -> TopicTieringPolicy {
        TopicTieringPolicy {
            topic: topic.to_string(),
            tiers: vec![
                TierRule {
                    tier: StorageTier::Warm,
                    condition: TierCondition::AgeHours(12),
                    backend: TierBackend::Local {
                        path: "/warm".into(),
                    },
                },
                TierRule {
                    tier: StorageTier::Cold,
                    condition: TierCondition::AgeHours(48),
                    backend: TierBackend::S3 {
                        bucket: "cold-bucket".into(),
                        prefix: "data/".into(),
                        region: "us-east-1".into(),
                    },
                },
            ],
            override_global: true,
            created_at: 1000,
            last_evaluated_at: None,
        }
    }

    // -- GlobalTieringPolicy defaults ---------------------------------------

    #[test]
    fn test_global_policy_defaults() {
        let gp = GlobalTieringPolicy::default();
        assert_eq!(gp.default_hot_retention_hours, 24);
        assert_eq!(gp.default_warm_retention_hours, 168);
        assert_eq!(gp.default_cold_retention_hours, 8760);
        assert!(gp.enable_auto_tiering);
        assert!(gp.compaction_on_tier_transition);
        assert_eq!(gp.read_through_cache_size_mb, 256);
        assert_eq!(gp.max_concurrent_uploads, 4);
        assert_eq!(gp.max_concurrent_downloads, 8);
    }

    // -- StorageTier ordering -----------------------------------------------

    #[test]
    fn test_storage_tier_coldness_ordering() {
        assert!(StorageTier::Hot.coldness() < StorageTier::Warm.coldness());
        assert!(StorageTier::Warm.coldness() < StorageTier::Cold.coldness());
        assert!(StorageTier::Cold.coldness() < StorageTier::Frozen.coldness());
        assert!(StorageTier::Frozen.coldness() < StorageTier::Delete.coldness());
    }

    #[test]
    fn test_storage_tier_display() {
        assert_eq!(StorageTier::Hot.to_string(), "hot");
        assert_eq!(StorageTier::Warm.to_string(), "warm");
        assert_eq!(StorageTier::Cold.to_string(), "cold");
        assert_eq!(StorageTier::Frozen.to_string(), "frozen");
        assert_eq!(StorageTier::Delete.to_string(), "delete");
    }

    // -- Topic policy CRUD --------------------------------------------------

    #[test]
    fn test_set_and_get_topic_policy() {
        let engine = default_engine();
        let policy = sample_topic_policy("orders");
        engine.set_topic_policy(policy.clone()).unwrap();

        let retrieved = engine.get_topic_policy("orders").unwrap();
        assert_eq!(retrieved.topic, "orders");
        assert_eq!(retrieved.tiers.len(), 2);
        assert!(retrieved.override_global);
    }

    #[test]
    fn test_get_nonexistent_topic_policy() {
        let engine = default_engine();
        assert!(engine.get_topic_policy("nope").is_none());
    }

    #[test]
    fn test_remove_topic_policy() {
        let engine = default_engine();
        engine
            .set_topic_policy(sample_topic_policy("orders"))
            .unwrap();
        engine.remove_topic_policy("orders").unwrap();
        assert!(engine.get_topic_policy("orders").is_none());
    }

    #[test]
    fn test_remove_nonexistent_policy_ok() {
        let engine = default_engine();
        assert!(engine.remove_topic_policy("ghost").is_ok());
    }

    #[test]
    fn test_list_policies() {
        let engine = default_engine();
        engine
            .set_topic_policy(sample_topic_policy("a"))
            .unwrap();
        engine
            .set_topic_policy(sample_topic_policy("b"))
            .unwrap();
        let list = engine.list_policies();
        assert_eq!(list.len(), 2);
        let topics: Vec<&str> = list.iter().map(|p| p.topic.as_str()).collect();
        assert!(topics.contains(&"a"));
        assert!(topics.contains(&"b"));
    }

    #[test]
    fn test_set_policy_empty_topic_name_errors() {
        let engine = default_engine();
        let mut policy = sample_topic_policy("");
        policy.topic = String::new();
        assert!(engine.set_topic_policy(policy).is_err());
    }

    #[test]
    fn test_set_policy_empty_rules_errors() {
        let engine = default_engine();
        let policy = TopicTieringPolicy {
            topic: "bad".into(),
            tiers: vec![],
            override_global: false,
            created_at: 0,
            last_evaluated_at: None,
        };
        assert!(engine.set_topic_policy(policy).is_err());
    }

    // -- Global policy update -----------------------------------------------

    #[test]
    fn test_update_and_get_global_policy() {
        let engine = default_engine();
        let mut gp = engine.get_global_policy();
        gp.default_hot_retention_hours = 48;
        engine.update_global_policy(gp);
        assert_eq!(engine.get_global_policy().default_hot_retention_hours, 48);
    }

    // -- Condition matching -------------------------------------------------

    #[test]
    fn test_condition_age_hours() {
        assert!(TieringPolicyEngine::condition_matches(
            &TierCondition::AgeHours(24),
            25.0,
            0,
            0.0,
        ));
        assert!(!TieringPolicyEngine::condition_matches(
            &TierCondition::AgeHours(24),
            23.0,
            0,
            0.0,
        ));
        // Exact boundary
        assert!(TieringPolicyEngine::condition_matches(
            &TierCondition::AgeHours(24),
            24.0,
            0,
            0.0,
        ));
    }

    #[test]
    fn test_condition_size_bytes() {
        assert!(TieringPolicyEngine::condition_matches(
            &TierCondition::SizeBytes(1000),
            0.0,
            1500,
            0.0,
        ));
        assert!(!TieringPolicyEngine::condition_matches(
            &TierCondition::SizeBytes(1000),
            0.0,
            500,
            0.0,
        ));
    }

    #[test]
    fn test_condition_access_frequency() {
        assert!(TieringPolicyEngine::condition_matches(
            &TierCondition::AccessFrequencyBelow {
                reads_per_hour: 5.0
            },
            0.0,
            0,
            3.0,
        ));
        assert!(!TieringPolicyEngine::condition_matches(
            &TierCondition::AccessFrequencyBelow {
                reads_per_hour: 5.0
            },
            0.0,
            0,
            10.0,
        ));
    }

    #[test]
    fn test_condition_combined() {
        let cond = TierCondition::Combined {
            age_hours: 24,
            size_bytes: 1000,
        };
        // Both met
        assert!(TieringPolicyEngine::condition_matches(&cond, 25.0, 1500, 0.0));
        // Only age met
        assert!(!TieringPolicyEngine::condition_matches(&cond, 25.0, 500, 0.0));
        // Only size met
        assert!(!TieringPolicyEngine::condition_matches(&cond, 12.0, 1500, 0.0));
        // Neither met
        assert!(!TieringPolicyEngine::condition_matches(&cond, 12.0, 500, 0.0));
    }

    // -- Segment evaluation (global defaults) --------------------------------

    #[test]
    fn test_evaluate_segment_global_hot_to_warm() {
        let engine = default_engine();
        // 25-hour-old segment should move Hot -> Warm (default hot = 24h)
        let decision = engine.evaluate_segment("t", 0, "seg1", 25.0, 100, StorageTier::Hot, 50.0);
        assert!(decision.is_some());
        let d = decision.unwrap();
        assert_eq!(d.current_tier, StorageTier::Hot);
        assert_eq!(d.target_tier, StorageTier::Warm);
    }

    #[test]
    fn test_evaluate_segment_no_transition_when_young() {
        let engine = default_engine();
        let decision = engine.evaluate_segment("t", 0, "seg1", 5.0, 100, StorageTier::Hot, 100.0);
        assert!(decision.is_none());
    }

    #[test]
    fn test_evaluate_segment_warm_to_cold() {
        let engine = default_engine();
        // 200-hour-old segment in warm tier; global warm retention = 168h
        let decision =
            engine.evaluate_segment("t", 0, "seg1", 200.0, 100, StorageTier::Warm, 1.0);
        assert!(decision.is_some());
        let d = decision.unwrap();
        assert_eq!(d.target_tier, StorageTier::Cold);
    }

    #[test]
    fn test_evaluate_segment_cold_to_delete() {
        let engine = default_engine();
        // very old segment in cold tier; global cold retention = 8760h
        let decision =
            engine.evaluate_segment("t", 0, "seg1", 9000.0, 100, StorageTier::Cold, 0.0);
        assert!(decision.is_some());
        let d = decision.unwrap();
        assert_eq!(d.target_tier, StorageTier::Delete);
    }

    // -- Segment evaluation (topic-specific) --------------------------------

    #[test]
    fn test_evaluate_segment_topic_override() {
        let engine = default_engine();
        engine
            .set_topic_policy(sample_topic_policy("orders"))
            .unwrap();
        // sample_topic_policy: warm after 12h, cold after 48h
        let decision =
            engine.evaluate_segment("orders", 0, "seg1", 15.0, 100, StorageTier::Hot, 50.0);
        let d = decision.unwrap();
        assert_eq!(d.target_tier, StorageTier::Warm);
    }

    #[test]
    fn test_evaluate_segment_topic_override_cold() {
        let engine = default_engine();
        engine
            .set_topic_policy(sample_topic_policy("orders"))
            .unwrap();
        let decision =
            engine.evaluate_segment("orders", 0, "seg1", 50.0, 100, StorageTier::Warm, 0.0);
        let d = decision.unwrap();
        assert_eq!(d.target_tier, StorageTier::Cold);
    }

    // -- Batch evaluation ---------------------------------------------------

    #[test]
    fn test_evaluate_topic_batch() {
        let engine = default_engine();
        let segments = vec![
            SegmentInfo {
                segment_id: "s1".into(),
                partition: 0,
                age_hours: 5.0,
                size_bytes: 100,
                current_tier: StorageTier::Hot,
                reads_per_hour: 100.0,
            },
            SegmentInfo {
                segment_id: "s2".into(),
                partition: 0,
                age_hours: 30.0,
                size_bytes: 200,
                current_tier: StorageTier::Hot,
                reads_per_hour: 10.0,
            },
            SegmentInfo {
                segment_id: "s3".into(),
                partition: 1,
                age_hours: 200.0,
                size_bytes: 300,
                current_tier: StorageTier::Warm,
                reads_per_hour: 0.1,
            },
        ];
        let decisions = engine.evaluate_topic("t", &segments);
        // s1: too young → no move. s2: 30h > 24h → warm. s3: 200h > 168h → cold.
        assert_eq!(decisions.len(), 2);
        assert!(decisions.iter().any(|d| d.segment_id == "s2"));
        assert!(decisions.iter().any(|d| d.segment_id == "s3"));
    }

    #[test]
    fn test_evaluate_topic_empty_segments() {
        let engine = default_engine();
        let decisions = engine.evaluate_topic("t", &[]);
        assert!(decisions.is_empty());
    }

    // -- Stats tracking -----------------------------------------------------

    #[test]
    fn test_stats_initial_zero() {
        let engine = default_engine();
        let s = engine.stats();
        assert_eq!(s.segments_evaluated, 0);
        assert_eq!(s.segments_moved, 0);
    }

    #[test]
    fn test_stats_incremented_on_evaluate() {
        let engine = default_engine();
        engine.evaluate_segment("t", 0, "s1", 1.0, 100, StorageTier::Hot, 100.0);
        engine.evaluate_segment("t", 0, "s2", 25.0, 100, StorageTier::Hot, 100.0);
        assert_eq!(engine.stats().segments_evaluated, 2);
    }

    #[test]
    fn test_stats_snapshot_serializable() {
        let snap = TieringStatsSnapshot {
            segments_evaluated: 10,
            segments_moved: 2,
            bytes_uploaded: 5000,
            bytes_downloaded: 3000,
            read_through_hits: 100,
            read_through_misses: 5,
            errors: 1,
        };
        let json = serde_json::to_string(&snap).unwrap();
        let deser: TieringStatsSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(snap, deser);
    }

    // -- Serde round-trips --------------------------------------------------

    #[test]
    fn test_storage_tier_serde() {
        let json = serde_json::to_string(&StorageTier::Frozen).unwrap();
        assert_eq!(json, "\"frozen\"");
        let deser: StorageTier = serde_json::from_str(&json).unwrap();
        assert_eq!(deser, StorageTier::Frozen);
    }

    #[test]
    fn test_tier_backend_serde_s3() {
        let backend = TierBackend::S3 {
            bucket: "b".into(),
            prefix: "p/".into(),
            region: "eu-west-1".into(),
        };
        let json = serde_json::to_string(&backend).unwrap();
        let deser: TierBackend = serde_json::from_str(&json).unwrap();
        match deser {
            TierBackend::S3 { bucket, .. } => assert_eq!(bucket, "b"),
            _ => panic!("expected S3"),
        }
    }

    #[test]
    fn test_tier_condition_serde() {
        let cond = TierCondition::Combined {
            age_hours: 24,
            size_bytes: 1_000_000,
        };
        let json = serde_json::to_string(&cond).unwrap();
        let deser: TierCondition = serde_json::from_str(&json).unwrap();
        match deser {
            TierCondition::Combined { age_hours, size_bytes } => {
                assert_eq!(age_hours, 24);
                assert_eq!(size_bytes, 1_000_000);
            }
            _ => panic!("expected Combined"),
        }
    }

    #[test]
    fn test_segment_tiering_decision_serde() {
        let d = SegmentTieringDecision {
            segment_id: "seg-1".into(),
            topic: "orders".into(),
            partition: 3,
            current_tier: StorageTier::Hot,
            target_tier: StorageTier::Cold,
            reason: "test".into(),
            segment_age_hours: 100.5,
            segment_size_bytes: 999,
        };
        let json = serde_json::to_string(&d).unwrap();
        let deser: SegmentTieringDecision = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.segment_id, "seg-1");
        assert_eq!(deser.target_tier, StorageTier::Cold);
    }

    // -- Edge cases ---------------------------------------------------------

    #[test]
    fn test_no_downgrade_same_tier() {
        let engine = default_engine();
        // Already warm, and age only qualifies for warm → no move
        let decision =
            engine.evaluate_segment("t", 0, "seg1", 25.0, 100, StorageTier::Warm, 100.0);
        // 25h > 24h qualifies for warm, but segment is already warm.
        // It doesn't qualify for cold (168h), so no decision.
        assert!(decision.is_none());
    }

    #[test]
    fn test_skip_warmer_tier_when_already_cold() {
        let engine = default_engine();
        // Segment is already Cold. Age qualifies for warm but that's warmer → skip.
        let decision =
            engine.evaluate_segment("t", 0, "seg1", 50.0, 100, StorageTier::Cold, 0.0);
        assert!(decision.is_none());
    }

    #[test]
    fn test_global_policy_fallback_when_no_override() {
        let engine = default_engine();
        let policy = TopicTieringPolicy {
            topic: "logs".into(),
            tiers: vec![TierRule {
                tier: StorageTier::Frozen,
                condition: TierCondition::AgeHours(1),
                backend: TierBackend::Local {
                    path: "/f".into(),
                },
            }],
            override_global: false,
            created_at: 0,
            last_evaluated_at: None,
        };
        engine.set_topic_policy(policy).unwrap();
        // override_global = false → global rules apply, not the topic's Frozen-after-1h rule
        let decision =
            engine.evaluate_segment("logs", 0, "seg1", 5.0, 100, StorageTier::Hot, 100.0);
        // 5h < 24h (global hot threshold) → no transition
        assert!(decision.is_none());
    }

    #[test]
    fn test_format_reason_messages() {
        let r1 = TieringPolicyEngine::format_reason(
            &TierCondition::AgeHours(24),
            &StorageTier::Warm,
        );
        assert!(r1.contains("24 hours"));
        assert!(r1.contains("warm"));

        let r2 = TieringPolicyEngine::format_reason(
            &TierCondition::SizeBytes(500),
            &StorageTier::Cold,
        );
        assert!(r2.contains("500 bytes"));

        let r3 = TieringPolicyEngine::format_reason(
            &TierCondition::AccessFrequencyBelow {
                reads_per_hour: 2.0,
            },
            &StorageTier::Frozen,
        );
        assert!(r3.contains("2"));

        let r4 = TieringPolicyEngine::format_reason(
            &TierCondition::Combined {
                age_hours: 10,
                size_bytes: 200,
            },
            &StorageTier::Delete,
        );
        assert!(r4.contains("10"));
        assert!(r4.contains("200"));
    }

    #[test]
    fn test_policy_overwrite() {
        let engine = default_engine();
        engine
            .set_topic_policy(sample_topic_policy("x"))
            .unwrap();
        assert_eq!(engine.get_topic_policy("x").unwrap().tiers.len(), 2);

        let mut p2 = sample_topic_policy("x");
        p2.tiers.push(TierRule {
            tier: StorageTier::Frozen,
            condition: TierCondition::AgeHours(720),
            backend: TierBackend::Local {
                path: "/frozen".into(),
            },
        });
        engine.set_topic_policy(p2).unwrap();
        assert_eq!(engine.get_topic_policy("x").unwrap().tiers.len(), 3);
    }

    #[test]
    fn test_access_frequency_based_tiering() {
        let engine = default_engine();
        let policy = TopicTieringPolicy {
            topic: "metrics".into(),
            tiers: vec![TierRule {
                tier: StorageTier::Cold,
                condition: TierCondition::AccessFrequencyBelow {
                    reads_per_hour: 1.0,
                },
                backend: TierBackend::Gcs {
                    bucket: "archive".into(),
                    prefix: "metrics/".into(),
                },
            }],
            override_global: true,
            created_at: 0,
            last_evaluated_at: None,
        };
        engine.set_topic_policy(policy).unwrap();
        let d = engine
            .evaluate_segment("metrics", 0, "seg1", 1.0, 100, StorageTier::Hot, 0.5)
            .unwrap();
        assert_eq!(d.target_tier, StorageTier::Cold);
    }

    #[test]
    fn test_decision_contains_correct_metadata() {
        let engine = default_engine();
        let d = engine
            .evaluate_segment("my-topic", 7, "seg-42", 30.0, 4096, StorageTier::Hot, 2.0)
            .unwrap();
        assert_eq!(d.topic, "my-topic");
        assert_eq!(d.partition, 7);
        assert_eq!(d.segment_id, "seg-42");
        assert_eq!(d.segment_age_hours, 30.0);
        assert_eq!(d.segment_size_bytes, 4096);
    }
}
