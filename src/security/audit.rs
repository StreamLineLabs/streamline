//! Audit-log helpers for KMS operations.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::security::kms::Algorithm;

#[derive(Debug, Clone)]
pub enum KmsOp {
    Sign,
    Verify,
    Encrypt,
    Decrypt,
}

impl KmsOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sign => "sign",
            Self::Verify => "verify",
            Self::Encrypt => "encrypt",
            Self::Decrypt => "decrypt",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuditEvent {
    pub ts_ms: u64,
    pub backend: &'static str,
    pub op: KmsOp,
    pub key_id: String,
    pub algorithm: Option<Algorithm>,
    pub principal: Option<String>,
    pub success: bool,
    pub reason: Option<String>,
}

impl AuditEvent {
    pub fn now(
        backend: &'static str,
        op: KmsOp,
        key_id: impl Into<String>,
        success: bool,
    ) -> Self {
        Self {
            ts_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            backend,
            op,
            key_id: key_id.into(),
            algorithm: None,
            principal: None,
            success,
            reason: None,
        }
    }

    pub fn with_algorithm(mut self, alg: Algorithm) -> Self {
        self.algorithm = Some(alg);
        self
    }

    pub fn with_principal(mut self, principal: impl Into<String>) -> Self {
        self.principal = Some(principal.into());
        self
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_event_records_timestamp() {
        let ev = AuditEvent::now("local-age", KmsOp::Sign, "k1", true);
        assert!(ev.ts_ms > 0);
        assert_eq!(ev.op.as_str(), "sign");
    }

    #[test]
    fn audit_event_chainable_builders() {
        let ev = AuditEvent::now("vault", KmsOp::Verify, "k2", false)
            .with_algorithm(Algorithm::Ed25519)
            .with_principal("alice")
            .with_reason("signature mismatch");
        assert_eq!(ev.algorithm, Some(Algorithm::Ed25519));
        assert_eq!(ev.principal.as_deref(), Some("alice"));
    }
}
