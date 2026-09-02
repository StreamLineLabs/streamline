//! Memory decay engine for agent memory (M1 P2).
//!
//! Implements half-life decay on importance scores. Memories below a
//! threshold are candidates for summarization and tier-down.

/// Configuration knobs for the decay engine.
#[derive(Debug, Clone)]
pub struct DecayConfig {
    /// Half-life in days — importance halves every `half_life_days`.
    pub half_life_days: f64,
    /// Importance below this after decay → candidate for tier-down.
    pub threshold: f32,
    /// Hard cap — memories older than this are always decayed.
    pub max_age_days: f64,
    /// How often the background sweep runs (seconds).
    pub run_interval_secs: u64,
}

impl Default for DecayConfig {
    fn default() -> Self {
        Self {
            half_life_days: 30.0,
            threshold: 0.3,
            max_age_days: 90.0,
            run_interval_secs: 21_600, // 6 hours
        }
    }
}

/// Statistics returned after a decay sweep.
#[derive(Debug, Clone, Default)]
pub struct DecayResult {
    pub memories_scanned: u64,
    pub memories_decayed: u64,
    pub memories_tiered_down: u64,
}

/// Compute the decayed importance of a memory.
///
/// Formula: `original × 2^(−age / half_life)`.
pub fn compute_decayed_importance(original: f32, age_days: f64, half_life_days: f64) -> f32 {
    if half_life_days <= 0.0 {
        return 0.0;
    }
    original * (2.0_f64.powf(-age_days / half_life_days) as f32)
}

/// Returns `true` when a memory should be decayed (importance dropped below
/// threshold or the memory exceeded the maximum age).
pub fn should_decay(importance: f32, age_days: f64, config: &DecayConfig) -> bool {
    if age_days >= config.max_age_days {
        return true;
    }
    let decayed = compute_decayed_importance(importance, age_days, config.half_life_days);
    decayed < config.threshold
}

/// Sweep an agent's memories and mark decayed entries.
///
/// This is a placeholder that walks the in-memory content store. A real
/// implementation would stream from the topic log and compact in place.
pub fn run_decay(agent_id: &str, config: &DecayConfig) -> DecayResult {
    use super::tier_router;

    let store = tier_router::content_store_snapshot(agent_id);
    let mut result = DecayResult {
        memories_scanned: store.len() as u64,
        ..Default::default()
    };

    for (importance, age_days) in &store {
        if should_decay(*importance, *age_days, config) {
            result.memories_decayed += 1;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_decay_at_age_zero() {
        let imp = compute_decayed_importance(1.0, 0.0, 30.0);
        assert!((imp - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn halves_at_half_life() {
        let imp = compute_decayed_importance(1.0, 30.0, 30.0);
        assert!((imp - 0.5).abs() < 0.001);
    }

    #[test]
    fn quarters_at_two_half_lives() {
        let imp = compute_decayed_importance(1.0, 60.0, 30.0);
        assert!((imp - 0.25).abs() < 0.001);
    }

    #[test]
    fn zero_half_life_returns_zero() {
        assert_eq!(compute_decayed_importance(1.0, 10.0, 0.0), 0.0);
    }

    #[test]
    fn should_decay_below_threshold() {
        let cfg = DecayConfig::default();
        // After 60 days with half-life 30, importance 0.5 → 0.125 < 0.3
        assert!(should_decay(0.5, 60.0, &cfg));
    }

    #[test]
    fn should_not_decay_high_importance_young() {
        let cfg = DecayConfig::default();
        // After 1 day with importance 0.9 → ~0.88 > 0.3
        assert!(!should_decay(0.9, 1.0, &cfg));
    }

    #[test]
    fn should_decay_past_max_age() {
        let cfg = DecayConfig::default();
        // Even with importance 1.0, past max_age_days it should decay
        assert!(should_decay(1.0, 91.0, &cfg));
    }

    #[test]
    fn run_decay_on_nonexistent_agent() {
        let cfg = DecayConfig::default();
        let r = run_decay("no-such-agent", &cfg);
        assert_eq!(r.memories_scanned, 0);
        assert_eq!(r.memories_decayed, 0);
    }

    #[test]
    fn default_config_values() {
        let cfg = DecayConfig::default();
        assert!((cfg.half_life_days - 30.0).abs() < f64::EPSILON);
        assert!((cfg.threshold - 0.3).abs() < f32::EPSILON);
        assert!((cfg.max_age_days - 90.0).abs() < f64::EPSILON);
        assert_eq!(cfg.run_interval_secs, 21_600);
    }
}
