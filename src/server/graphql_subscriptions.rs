//! GraphQL Subscription Engine for Streamline
//!
//! Provides a subscription push engine that maps Streamline topics to GraphQL
//! subscriptions, enabling real-time data consumption via GraphQL.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::error::{Result, StreamlineError};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the subscription engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionConfig {
    /// Maximum subscriptions a single client may hold.
    pub max_subscriptions_per_client: usize,
    /// Maximum total subscriptions across all clients.
    pub max_total_subscriptions: usize,
    /// Heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: u64,
    /// Per-subscription message buffer size.
    pub message_buffer_size: usize,
    /// Maximum batch size for delivery.
    pub max_batch_size: usize,
    /// Maximum JSON path depth for filters.
    pub filter_max_depth: usize,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            max_subscriptions_per_client: 10,
            max_total_subscriptions: 10_000,
            heartbeat_interval_ms: 30_000,
            message_buffer_size: 1_000,
            max_batch_size: 100,
            filter_max_depth: 5,
        }
    }
}

// ---------------------------------------------------------------------------
// Subscription types
// ---------------------------------------------------------------------------

/// Status of an active subscription.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SubscriptionStatus {
    Active,
    Paused,
    Error(String),
}

/// Starting offset for a subscription.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SubscriptionOffset {
    Latest,
    Earliest,
    Specific(i64),
    Timestamp(u64),
}

/// Comparison operator used in subscription filters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FilterOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
    Regex,
}

/// A filter applied to incoming events before delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionFilter {
    /// JSONPath-like field selector (e.g. `"$.user.id"`).
    pub field: String,
    pub operator: FilterOp,
    pub value: serde_json::Value,
}

/// Represents a currently registered subscription.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveSubscription {
    pub id: String,
    pub client_id: String,
    pub topic: String,
    pub partition: Option<i32>,
    pub filter: Option<SubscriptionFilter>,
    pub from_offset: SubscriptionOffset,
    pub created_at: u64,
    pub messages_delivered: u64,
    pub last_delivered_at: Option<u64>,
    pub status: SubscriptionStatus,
}

/// An event to be delivered to matching subscriptions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionEvent {
    pub subscription_id: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<String>,
    pub value: serde_json::Value,
    pub timestamp: u64,
    pub headers: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// Atomic counters for subscription statistics.
#[derive(Debug, Default)]
pub struct SubscriptionStats {
    pub total_created: AtomicU64,
    pub active: AtomicU64,
    pub paused: AtomicU64,
    pub messages_delivered: AtomicU64,
    pub errors: AtomicU64,
}

/// Serializable snapshot of [`SubscriptionStats`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SubscriptionStatsSnapshot {
    pub total_created: u64,
    pub active: u64,
    pub paused: u64,
    pub messages_delivered: u64,
    pub errors: u64,
}

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

/// Core subscription manager – thread-safe, clone-friendly.
#[derive(Debug, Clone)]
pub struct SubscriptionManager {
    subscriptions: Arc<RwLock<HashMap<String, ActiveSubscription>>>,
    config: SubscriptionConfig,
    stats: Arc<SubscriptionStats>,
}

impl SubscriptionManager {
    /// Create a new manager with the given configuration.
    pub fn new(config: SubscriptionConfig) -> Self {
        Self {
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            config,
            stats: Arc::new(SubscriptionStats::default()),
        }
    }

    /// Register a new subscription and return its ID.
    pub fn subscribe(
        &self,
        client_id: &str,
        topic: &str,
        partition: Option<i32>,
        filter: Option<SubscriptionFilter>,
        from: SubscriptionOffset,
    ) -> Result<String> {
        if let Some(ref f) = filter {
            self.validate_filter(f)?;
        }

        let mut subs = self.subscriptions.write().map_err(|e| {
            StreamlineError::Internal(format!("subscription lock poisoned: {e}"))
        })?;

        // Enforce total limit
        if subs.len() >= self.config.max_total_subscriptions {
            return Err(StreamlineError::ResourceExhausted(
                "max total subscriptions reached".into(),
            ));
        }

        // Enforce per-client limit
        let client_count = subs
            .values()
            .filter(|s| s.client_id == client_id)
            .count();
        if client_count >= self.config.max_subscriptions_per_client {
            return Err(StreamlineError::ResourceExhausted(format!(
                "client {client_id} has reached the maximum of {} subscriptions",
                self.config.max_subscriptions_per_client,
            )));
        }

        let id = Uuid::new_v4().to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let sub = ActiveSubscription {
            id: id.clone(),
            client_id: client_id.to_string(),
            topic: topic.to_string(),
            partition,
            filter,
            from_offset: from,
            created_at: now,
            messages_delivered: 0,
            last_delivered_at: None,
            status: SubscriptionStatus::Active,
        };

        debug!(subscription_id = %id, %client_id, %topic, "new subscription created");
        subs.insert(id.clone(), sub);

        self.stats.total_created.fetch_add(1, Ordering::Relaxed);
        self.stats.active.fetch_add(1, Ordering::Relaxed);

        Ok(id)
    }

    /// Remove a subscription.
    pub fn unsubscribe(&self, subscription_id: &str) -> Result<()> {
        let mut subs = self.subscriptions.write().map_err(|e| {
            StreamlineError::Internal(format!("subscription lock poisoned: {e}"))
        })?;

        let removed = subs.remove(subscription_id).ok_or_else(|| {
            StreamlineError::Validation(format!(
                "subscription not found: {subscription_id}"
            ))
        })?;

        match removed.status {
            SubscriptionStatus::Active => {
                self.stats.active.fetch_sub(1, Ordering::Relaxed);
            }
            SubscriptionStatus::Paused => {
                self.stats.paused.fetch_sub(1, Ordering::Relaxed);
            }
            SubscriptionStatus::Error(_) => {}
        }

        debug!(subscription_id, "subscription removed");
        Ok(())
    }

    /// Pause a subscription so it stops receiving events.
    pub fn pause(&self, subscription_id: &str) -> Result<()> {
        let mut subs = self.subscriptions.write().map_err(|e| {
            StreamlineError::Internal(format!("subscription lock poisoned: {e}"))
        })?;

        let sub = subs.get_mut(subscription_id).ok_or_else(|| {
            StreamlineError::Validation(format!(
                "subscription not found: {subscription_id}"
            ))
        })?;

        if sub.status != SubscriptionStatus::Active {
            return Err(StreamlineError::Validation(format!(
                "cannot pause subscription in {:?} state",
                sub.status,
            )));
        }

        sub.status = SubscriptionStatus::Paused;
        self.stats.active.fetch_sub(1, Ordering::Relaxed);
        self.stats.paused.fetch_add(1, Ordering::Relaxed);

        debug!(subscription_id, "subscription paused");
        Ok(())
    }

    /// Resume a previously paused subscription.
    pub fn resume(&self, subscription_id: &str) -> Result<()> {
        let mut subs = self.subscriptions.write().map_err(|e| {
            StreamlineError::Internal(format!("subscription lock poisoned: {e}"))
        })?;

        let sub = subs.get_mut(subscription_id).ok_or_else(|| {
            StreamlineError::Validation(format!(
                "subscription not found: {subscription_id}"
            ))
        })?;

        if sub.status != SubscriptionStatus::Paused {
            return Err(StreamlineError::Validation(format!(
                "cannot resume subscription in {:?} state",
                sub.status,
            )));
        }

        sub.status = SubscriptionStatus::Active;
        self.stats.paused.fetch_sub(1, Ordering::Relaxed);
        self.stats.active.fetch_add(1, Ordering::Relaxed);

        debug!(subscription_id, "subscription resumed");
        Ok(())
    }

    /// List subscriptions, optionally filtered by client.
    pub fn list_subscriptions(&self, client_id: Option<&str>) -> Vec<ActiveSubscription> {
        let subs = self.subscriptions.read().unwrap_or_else(|e| e.into_inner());
        subs.values()
            .filter(|s| client_id.map_or(true, |cid| s.client_id == cid))
            .cloned()
            .collect()
    }

    /// Deliver an event to all matching active subscriptions.
    ///
    /// Returns the number of subscriptions the event was delivered to.
    pub fn deliver_event(&self, event: SubscriptionEvent) -> Result<usize> {
        let mut subs = self.subscriptions.write().map_err(|e| {
            StreamlineError::Internal(format!("subscription lock poisoned: {e}"))
        })?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut matched = 0usize;

        for sub in subs.values_mut() {
            if sub.status != SubscriptionStatus::Active {
                continue;
            }
            if sub.topic != event.topic {
                continue;
            }
            if let Some(p) = sub.partition {
                if p != event.partition {
                    continue;
                }
            }
            if let Some(ref filter) = sub.filter {
                if !Self::matches_filter(filter, &event.value) {
                    continue;
                }
            }

            sub.messages_delivered += 1;
            sub.last_delivered_at = Some(now);
            matched += 1;
        }

        self.stats
            .messages_delivered
            .fetch_add(matched as u64, Ordering::Relaxed);
        Ok(matched)
    }

    /// Evaluate a [`SubscriptionFilter`] against a JSON value.
    pub fn matches_filter(filter: &SubscriptionFilter, value: &serde_json::Value) -> bool {
        let resolved = resolve_field(&filter.field, value);
        let resolved = match resolved {
            Some(v) => v,
            None => return false,
        };

        match &filter.operator {
            FilterOp::Eq => resolved == &filter.value,
            FilterOp::Neq => resolved != &filter.value,
            FilterOp::Gt => compare_json(resolved, &filter.value) == Some(std::cmp::Ordering::Greater),
            FilterOp::Gte => matches!(
                compare_json(resolved, &filter.value),
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
            ),
            FilterOp::Lt => compare_json(resolved, &filter.value) == Some(std::cmp::Ordering::Less),
            FilterOp::Lte => matches!(
                compare_json(resolved, &filter.value),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            ),
            FilterOp::Contains => match (resolved.as_str(), filter.value.as_str()) {
                (Some(hay), Some(needle)) => hay.contains(needle),
                _ => {
                    if let Some(arr) = resolved.as_array() {
                        arr.contains(&filter.value)
                    } else {
                        false
                    }
                }
            },
            FilterOp::Regex => match (resolved.as_str(), filter.value.as_str()) {
                (Some(hay), Some(pattern)) => {
                    regex::Regex::new(pattern)
                        .map(|re| re.is_match(hay))
                        .unwrap_or_else(|e| {
                            warn!(pattern, %e, "invalid regex in subscription filter");
                            false
                        })
                }
                _ => false,
            },
        }
    }

    /// Return a point-in-time snapshot of subscription statistics.
    pub fn stats(&self) -> SubscriptionStatsSnapshot {
        SubscriptionStatsSnapshot {
            total_created: self.stats.total_created.load(Ordering::Relaxed),
            active: self.stats.active.load(Ordering::Relaxed),
            paused: self.stats.paused.load(Ordering::Relaxed),
            messages_delivered: self.stats.messages_delivered.load(Ordering::Relaxed),
            errors: self.stats.errors.load(Ordering::Relaxed),
        }
    }

    // -- internal helpers ---------------------------------------------------

    fn validate_filter(&self, filter: &SubscriptionFilter) -> Result<()> {
        let depth = parse_field_segments(&filter.field).len();
        if depth > self.config.filter_max_depth {
            return Err(StreamlineError::Validation(format!(
                "filter field depth {depth} exceeds maximum of {}",
                self.config.filter_max_depth,
            )));
        }
        if filter.operator == FilterOp::Regex {
            if let Some(pattern) = filter.value.as_str() {
                regex::Regex::new(pattern).map_err(|e| {
                    StreamlineError::Validation(format!("invalid regex pattern: {e}"))
                })?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// JSON field resolution helpers
// ---------------------------------------------------------------------------

/// Parse a JSONPath-like field string into path segments.
///
/// Supports `"$.a.b.c"` and plain `"a.b.c"` forms.
fn parse_field_segments(field: &str) -> Vec<&str> {
    let trimmed = field.strip_prefix("$.").unwrap_or(field);
    trimmed.split('.').filter(|s| !s.is_empty()).collect()
}

/// Resolve a dotted field path against a [`serde_json::Value`].
fn resolve_field<'a>(field: &str, value: &'a serde_json::Value) -> Option<&'a serde_json::Value> {
    let segments = parse_field_segments(field);
    let mut current = value;
    for seg in segments {
        current = current.get(seg)?;
    }
    Some(current)
}

/// Compare two JSON values numerically, returning [`None`] for incompatible types.
fn compare_json(a: &serde_json::Value, b: &serde_json::Value) -> Option<std::cmp::Ordering> {
    match (a.as_f64(), b.as_f64()) {
        (Some(av), Some(bv)) => av.partial_cmp(&bv),
        _ => match (a.as_str(), b.as_str()) {
            (Some(av), Some(bv)) => Some(av.cmp(bv)),
            _ => None,
        },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn default_manager() -> SubscriptionManager {
        SubscriptionManager::new(SubscriptionConfig::default())
    }

    fn small_manager() -> SubscriptionManager {
        SubscriptionManager::new(SubscriptionConfig {
            max_subscriptions_per_client: 2,
            max_total_subscriptions: 5,
            ..Default::default()
        })
    }

    // -- subscribe / unsubscribe -------------------------------------------

    #[test]
    fn test_subscribe_returns_uuid() {
        let mgr = default_manager();
        let id = mgr
            .subscribe("c1", "topic-a", None, None, SubscriptionOffset::Latest)
            .unwrap();
        assert!(Uuid::parse_str(&id).is_ok());
    }

    #[test]
    fn test_subscribe_creates_active_subscription() {
        let mgr = default_manager();
        let id = mgr
            .subscribe("c1", "topic-a", Some(0), None, SubscriptionOffset::Earliest)
            .unwrap();
        let subs = mgr.list_subscriptions(Some("c1"));
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].id, id);
        assert_eq!(subs[0].topic, "topic-a");
        assert_eq!(subs[0].partition, Some(0));
        assert_eq!(subs[0].status, SubscriptionStatus::Active);
    }

    #[test]
    fn test_unsubscribe_removes_subscription() {
        let mgr = default_manager();
        let id = mgr
            .subscribe("c1", "t", None, None, SubscriptionOffset::Latest)
            .unwrap();
        mgr.unsubscribe(&id).unwrap();
        assert!(mgr.list_subscriptions(None).is_empty());
    }

    #[test]
    fn test_unsubscribe_not_found() {
        let mgr = default_manager();
        let err = mgr.unsubscribe("nonexistent").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    // -- per-client limit --------------------------------------------------

    #[test]
    fn test_per_client_limit_enforced() {
        let mgr = small_manager();
        mgr.subscribe("c1", "t1", None, None, SubscriptionOffset::Latest)
            .unwrap();
        mgr.subscribe("c1", "t2", None, None, SubscriptionOffset::Latest)
            .unwrap();
        let err = mgr
            .subscribe("c1", "t3", None, None, SubscriptionOffset::Latest)
            .unwrap_err();
        assert!(err.to_string().contains("maximum"));
    }

    // -- total limit -------------------------------------------------------

    #[test]
    fn test_total_limit_enforced() {
        let mgr = small_manager();
        for i in 0..5 {
            mgr.subscribe(&format!("c{i}"), "t", None, None, SubscriptionOffset::Latest)
                .unwrap();
        }
        let err = mgr
            .subscribe("c99", "t", None, None, SubscriptionOffset::Latest)
            .unwrap_err();
        assert!(err.to_string().contains("max total"));
    }

    // -- pause / resume ----------------------------------------------------

    #[test]
    fn test_pause_and_resume() {
        let mgr = default_manager();
        let id = mgr
            .subscribe("c1", "t", None, None, SubscriptionOffset::Latest)
            .unwrap();
        mgr.pause(&id).unwrap();

        let sub = &mgr.list_subscriptions(None)[0];
        assert_eq!(sub.status, SubscriptionStatus::Paused);

        mgr.resume(&id).unwrap();
        let sub = &mgr.list_subscriptions(None)[0];
        assert_eq!(sub.status, SubscriptionStatus::Active);
    }

    #[test]
    fn test_pause_non_active_fails() {
        let mgr = default_manager();
        let id = mgr
            .subscribe("c1", "t", None, None, SubscriptionOffset::Latest)
            .unwrap();
        mgr.pause(&id).unwrap();
        let err = mgr.pause(&id).unwrap_err();
        assert!(err.to_string().contains("cannot pause"));
    }

    #[test]
    fn test_resume_non_paused_fails() {
        let mgr = default_manager();
        let id = mgr
            .subscribe("c1", "t", None, None, SubscriptionOffset::Latest)
            .unwrap();
        let err = mgr.resume(&id).unwrap_err();
        assert!(err.to_string().contains("cannot resume"));
    }

    #[test]
    fn test_pause_not_found() {
        let mgr = default_manager();
        assert!(mgr.pause("nope").is_err());
    }

    #[test]
    fn test_resume_not_found() {
        let mgr = default_manager();
        assert!(mgr.resume("nope").is_err());
    }

    // -- deliver_event / filtering -----------------------------------------

    fn make_event(topic: &str, partition: i32, value: serde_json::Value) -> SubscriptionEvent {
        SubscriptionEvent {
            subscription_id: String::new(),
            topic: topic.to_string(),
            partition,
            offset: 0,
            key: None,
            value,
            timestamp: 0,
            headers: HashMap::new(),
        }
    }

    #[test]
    fn test_deliver_event_matches_topic() {
        let mgr = default_manager();
        mgr.subscribe("c1", "orders", None, None, SubscriptionOffset::Latest)
            .unwrap();
        let matched = mgr
            .deliver_event(make_event("orders", 0, json!({})))
            .unwrap();
        assert_eq!(matched, 1);
    }

    #[test]
    fn test_deliver_event_skips_wrong_topic() {
        let mgr = default_manager();
        mgr.subscribe("c1", "orders", None, None, SubscriptionOffset::Latest)
            .unwrap();
        let matched = mgr
            .deliver_event(make_event("users", 0, json!({})))
            .unwrap();
        assert_eq!(matched, 0);
    }

    #[test]
    fn test_deliver_event_partition_filter() {
        let mgr = default_manager();
        mgr.subscribe("c1", "t", Some(1), None, SubscriptionOffset::Latest)
            .unwrap();
        assert_eq!(mgr.deliver_event(make_event("t", 0, json!({}))).unwrap(), 0);
        assert_eq!(mgr.deliver_event(make_event("t", 1, json!({}))).unwrap(), 1);
    }

    #[test]
    fn test_deliver_event_skips_paused() {
        let mgr = default_manager();
        let id = mgr
            .subscribe("c1", "t", None, None, SubscriptionOffset::Latest)
            .unwrap();
        mgr.pause(&id).unwrap();
        assert_eq!(mgr.deliver_event(make_event("t", 0, json!({}))).unwrap(), 0);
    }

    // -- filter operators --------------------------------------------------

    #[test]
    fn test_filter_eq() {
        let f = SubscriptionFilter {
            field: "$.status".into(),
            operator: FilterOp::Eq,
            value: json!("active"),
        };
        assert!(SubscriptionManager::matches_filter(&f, &json!({"status": "active"})));
        assert!(!SubscriptionManager::matches_filter(&f, &json!({"status": "inactive"})));
    }

    #[test]
    fn test_filter_neq() {
        let f = SubscriptionFilter {
            field: "$.status".into(),
            operator: FilterOp::Neq,
            value: json!("active"),
        };
        assert!(!SubscriptionManager::matches_filter(&f, &json!({"status": "active"})));
        assert!(SubscriptionManager::matches_filter(&f, &json!({"status": "inactive"})));
    }

    #[test]
    fn test_filter_gt_lt() {
        let gt = SubscriptionFilter {
            field: "$.amount".into(),
            operator: FilterOp::Gt,
            value: json!(100),
        };
        assert!(SubscriptionManager::matches_filter(&gt, &json!({"amount": 150})));
        assert!(!SubscriptionManager::matches_filter(&gt, &json!({"amount": 100})));
        assert!(!SubscriptionManager::matches_filter(&gt, &json!({"amount": 50})));

        let lt = SubscriptionFilter {
            field: "$.amount".into(),
            operator: FilterOp::Lt,
            value: json!(100),
        };
        assert!(SubscriptionManager::matches_filter(&lt, &json!({"amount": 50})));
        assert!(!SubscriptionManager::matches_filter(&lt, &json!({"amount": 100})));
    }

    #[test]
    fn test_filter_gte_lte() {
        let gte = SubscriptionFilter {
            field: "$.amount".into(),
            operator: FilterOp::Gte,
            value: json!(100),
        };
        assert!(SubscriptionManager::matches_filter(&gte, &json!({"amount": 100})));
        assert!(SubscriptionManager::matches_filter(&gte, &json!({"amount": 200})));
        assert!(!SubscriptionManager::matches_filter(&gte, &json!({"amount": 99})));

        let lte = SubscriptionFilter {
            field: "$.amount".into(),
            operator: FilterOp::Lte,
            value: json!(100),
        };
        assert!(SubscriptionManager::matches_filter(&lte, &json!({"amount": 100})));
        assert!(SubscriptionManager::matches_filter(&lte, &json!({"amount": 50})));
        assert!(!SubscriptionManager::matches_filter(&lte, &json!({"amount": 101})));
    }

    #[test]
    fn test_filter_contains_string() {
        let f = SubscriptionFilter {
            field: "$.name".into(),
            operator: FilterOp::Contains,
            value: json!("stream"),
        };
        assert!(SubscriptionManager::matches_filter(&f, &json!({"name": "streamline"})));
        assert!(!SubscriptionManager::matches_filter(&f, &json!({"name": "redis"})));
    }

    #[test]
    fn test_filter_contains_array() {
        let f = SubscriptionFilter {
            field: "$.tags".into(),
            operator: FilterOp::Contains,
            value: json!("urgent"),
        };
        assert!(SubscriptionManager::matches_filter(
            &f,
            &json!({"tags": ["urgent", "low"]})
        ));
        assert!(!SubscriptionManager::matches_filter(
            &f,
            &json!({"tags": ["low"]})
        ));
    }

    #[test]
    fn test_filter_regex() {
        let f = SubscriptionFilter {
            field: "$.email".into(),
            operator: FilterOp::Regex,
            value: json!(r"^[a-z]+@example\.com$"),
        };
        assert!(SubscriptionManager::matches_filter(
            &f,
            &json!({"email": "alice@example.com"})
        ));
        assert!(!SubscriptionManager::matches_filter(
            &f,
            &json!({"email": "ALICE@example.com"})
        ));
    }

    #[test]
    fn test_filter_nested_field() {
        let f = SubscriptionFilter {
            field: "$.user.address.city".into(),
            operator: FilterOp::Eq,
            value: json!("Berlin"),
        };
        assert!(SubscriptionManager::matches_filter(
            &f,
            &json!({"user": {"address": {"city": "Berlin"}}})
        ));
        assert!(!SubscriptionManager::matches_filter(
            &f,
            &json!({"user": {"address": {"city": "Paris"}}})
        ));
    }

    #[test]
    fn test_filter_missing_field_returns_false() {
        let f = SubscriptionFilter {
            field: "$.nonexistent".into(),
            operator: FilterOp::Eq,
            value: json!(1),
        };
        assert!(!SubscriptionManager::matches_filter(&f, &json!({"a": 1})));
    }

    // -- filter depth validation -------------------------------------------

    #[test]
    fn test_filter_depth_limit() {
        let mgr = SubscriptionManager::new(SubscriptionConfig {
            filter_max_depth: 2,
            ..Default::default()
        });
        let deep_filter = SubscriptionFilter {
            field: "$.a.b.c".into(),
            operator: FilterOp::Eq,
            value: json!(1),
        };
        let err = mgr
            .subscribe("c1", "t", None, Some(deep_filter), SubscriptionOffset::Latest)
            .unwrap_err();
        assert!(err.to_string().contains("depth"));
    }

    #[test]
    fn test_invalid_regex_rejected_at_subscribe() {
        let mgr = default_manager();
        let filter = SubscriptionFilter {
            field: "$.x".into(),
            operator: FilterOp::Regex,
            value: json!("[invalid"),
        };
        let err = mgr
            .subscribe("c1", "t", None, Some(filter), SubscriptionOffset::Latest)
            .unwrap_err();
        assert!(err.to_string().contains("regex"));
    }

    // -- stats -------------------------------------------------------------

    #[test]
    fn test_stats_after_subscribe() {
        let mgr = default_manager();
        mgr.subscribe("c1", "t", None, None, SubscriptionOffset::Latest)
            .unwrap();
        let s = mgr.stats();
        assert_eq!(s.total_created, 1);
        assert_eq!(s.active, 1);
        assert_eq!(s.paused, 0);
    }

    #[test]
    fn test_stats_after_pause_resume() {
        let mgr = default_manager();
        let id = mgr
            .subscribe("c1", "t", None, None, SubscriptionOffset::Latest)
            .unwrap();

        mgr.pause(&id).unwrap();
        let s = mgr.stats();
        assert_eq!(s.active, 0);
        assert_eq!(s.paused, 1);

        mgr.resume(&id).unwrap();
        let s = mgr.stats();
        assert_eq!(s.active, 1);
        assert_eq!(s.paused, 0);
    }

    #[test]
    fn test_stats_after_unsubscribe() {
        let mgr = default_manager();
        let id = mgr
            .subscribe("c1", "t", None, None, SubscriptionOffset::Latest)
            .unwrap();
        mgr.unsubscribe(&id).unwrap();
        let s = mgr.stats();
        assert_eq!(s.total_created, 1);
        assert_eq!(s.active, 0);
    }

    #[test]
    fn test_stats_messages_delivered() {
        let mgr = default_manager();
        mgr.subscribe("c1", "t", None, None, SubscriptionOffset::Latest)
            .unwrap();
        mgr.deliver_event(make_event("t", 0, json!({}))).unwrap();
        mgr.deliver_event(make_event("t", 0, json!({}))).unwrap();
        assert_eq!(mgr.stats().messages_delivered, 2);
    }

    // -- offset modes stored correctly -------------------------------------

    #[test]
    fn test_offset_modes_stored() {
        let mgr = default_manager();

        let id1 = mgr
            .subscribe("c1", "t", None, None, SubscriptionOffset::Earliest)
            .unwrap();
        let id2 = mgr
            .subscribe("c1", "t", None, None, SubscriptionOffset::Specific(42))
            .unwrap();
        let id3 = mgr
            .subscribe("c1", "t", None, None, SubscriptionOffset::Timestamp(1234567890))
            .unwrap();

        let subs = mgr.list_subscriptions(Some("c1"));
        let find = |id: &str| subs.iter().find(|s| s.id == id).unwrap();

        assert_eq!(find(&id1).from_offset, SubscriptionOffset::Earliest);
        assert_eq!(find(&id2).from_offset, SubscriptionOffset::Specific(42));
        assert_eq!(find(&id3).from_offset, SubscriptionOffset::Timestamp(1234567890));
    }

    // -- list_subscriptions ------------------------------------------------

    #[test]
    fn test_list_subscriptions_all_vs_filtered() {
        let mgr = default_manager();
        mgr.subscribe("c1", "t1", None, None, SubscriptionOffset::Latest)
            .unwrap();
        mgr.subscribe("c2", "t2", None, None, SubscriptionOffset::Latest)
            .unwrap();

        assert_eq!(mgr.list_subscriptions(None).len(), 2);
        assert_eq!(mgr.list_subscriptions(Some("c1")).len(), 1);
        assert_eq!(mgr.list_subscriptions(Some("c2")).len(), 1);
        assert_eq!(mgr.list_subscriptions(Some("c3")).len(), 0);
    }

    // -- deliver with filter integration -----------------------------------

    #[test]
    fn test_deliver_with_eq_filter() {
        let mgr = default_manager();
        let filter = SubscriptionFilter {
            field: "$.level".into(),
            operator: FilterOp::Eq,
            value: json!("error"),
        };
        mgr.subscribe("c1", "logs", None, Some(filter), SubscriptionOffset::Latest)
            .unwrap();

        assert_eq!(
            mgr.deliver_event(make_event("logs", 0, json!({"level": "error"})))
                .unwrap(),
            1
        );
        assert_eq!(
            mgr.deliver_event(make_event("logs", 0, json!({"level": "info"})))
                .unwrap(),
            0
        );
    }

    // -- field path without $. prefix --------------------------------------

    #[test]
    fn test_filter_field_without_dollar_prefix() {
        let f = SubscriptionFilter {
            field: "status".into(),
            operator: FilterOp::Eq,
            value: json!("ok"),
        };
        assert!(SubscriptionManager::matches_filter(&f, &json!({"status": "ok"})));
    }

    // -- multiple subscribers match same event -----------------------------

    #[test]
    fn test_multiple_subscribers_match() {
        let mgr = default_manager();
        mgr.subscribe("c1", "t", None, None, SubscriptionOffset::Latest)
            .unwrap();
        mgr.subscribe("c2", "t", None, None, SubscriptionOffset::Latest)
            .unwrap();
        mgr.subscribe("c3", "t", None, None, SubscriptionOffset::Latest)
            .unwrap();
        assert_eq!(mgr.deliver_event(make_event("t", 0, json!({}))).unwrap(), 3);
    }
}
