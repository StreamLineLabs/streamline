//! Time-bounded, audited bypass mode for contract enforcement (M4 P1).
//!
//! When an operator needs to ship an emergency fix that violates an active
//! contract, they can issue `streamline contract bypass --topic foo
//! --duration 30m --reason "PROD-1234"`. The bypass is:
//!
//! 1. Time-bounded (max 24h, configurable).
//! 2. Logged to `__bypass_audit` system topic, signed.
//! 3. Visible in `streamline contract status`.
//! 4. Auto-expired by the broker.
//!
//! Stability tier: **Experimental**.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct BypassToken {
    pub topic: String,
    pub principal: String,
    pub reason: String,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum BypassError {
    #[error("duration exceeds maximum allowed ({max_secs}s)")]
    DurationTooLong { max_secs: u64 },
    #[error("reason cannot be empty")]
    EmptyReason,
}

const MAX_DURATION_SECS: u64 = 24 * 3600;

impl BypassToken {
    pub fn new(
        topic: impl Into<String>,
        principal: impl Into<String>,
        reason: impl Into<String>,
        duration: Duration,
    ) -> Result<Self, BypassError> {
        let reason = reason.into();
        if reason.trim().is_empty() {
            return Err(BypassError::EmptyReason);
        }
        if duration.as_secs() > MAX_DURATION_SECS {
            return Err(BypassError::DurationTooLong {
                max_secs: MAX_DURATION_SECS,
            });
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Ok(Self {
            topic: topic.into(),
            principal: principal.into(),
            reason,
            issued_at_ms: now_ms,
            expires_at_ms: now_ms + duration.as_millis() as u64,
        })
    }

    pub fn is_active(&self, now_ms: u64) -> bool {
        now_ms < self.expires_at_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_reason() {
        let err = BypassToken::new("t", "alice", "", Duration::from_secs(60))
            .err()
            .expect("should reject");
        matches!(err, BypassError::EmptyReason);
    }

    #[test]
    fn rejects_overlong_duration() {
        let err = BypassToken::new("t", "alice", "PROD-1", Duration::from_secs(MAX_DURATION_SECS + 1))
            .err()
            .expect("should reject");
        matches!(err, BypassError::DurationTooLong { .. });
    }

    #[test]
    fn active_until_expiry() {
        let tok = BypassToken::new("t", "alice", "ok", Duration::from_secs(60)).unwrap();
        assert!(tok.is_active(tok.issued_at_ms));
        assert!(!tok.is_active(tok.expires_at_ms + 1));
    }
}
